mod error;
mod features;
mod merge;
mod schema;
mod utils;

use chrono::{DateTime, Duration, FixedOffset, Utc};
use delta_kernel::schema::StructField;
use delta_kernel::table_properties::DataSkippingNumIndexedCols;
use deltalake::arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use deltalake::arrow::record_batch::RecordBatchIterator;
use deltalake::checkpoints::{cleanup_metadata, create_checkpoint};
use deltalake::datafusion::catalog::TableProvider;
use deltalake::datafusion::prelude::SessionContext;
use deltalake::delta_datafusion::DeltaCdfTableProvider;
use deltalake::errors::DeltaTableError;
use deltalake::kernel::transaction::{CommitProperties, TableReference};
use deltalake::kernel::{scalars::ScalarExt, Transaction};
use deltalake::kernel::{EagerSnapshot, StructDataExt};
use deltalake::logstore::IORuntime;
use deltalake::logstore::LogStoreRef;
use deltalake::operations::collect_sendable_stream;
use deltalake::operations::optimize::{create_session_state_for_optimize, OptimizeType};
use deltalake::parquet::basic::Compression;
use deltalake::parquet::errors::ParquetError;
use deltalake::parquet::file::properties::WriterProperties;
use deltalake::partitions::PartitionFilter;
use deltalake::table::config::TablePropertiesExt;
use deltalake::table::state::DeltaTableState;
use deltalake::{DeltaResult, DeltaTable};
use error::DeltaError;
use futures::future::join_all;
use futures::TryStreamExt;
use magnus::{
    function, method, prelude::*, try_convert::TryConvertOwned, typed_data::Obj, Error as RbErr,
    Integer, Module, RArray, Ruby, TryConvert, Value,
};
use serde_json::Map;
use std::collections::{HashMap, HashSet};
use std::future::IntoFuture;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time;
use uuid::Uuid;

use crate::error::{to_rt_err, RbRuntimeError, RbValueError, RubyError};
use crate::features::TableFeatures;
use crate::merge::RbMergeBuilder;
use crate::schema::{schema_to_rbobject, Field};
use crate::utils::rt;

type RbResult<T> = Result<T, RbErr>;

enum PartitionFilterValue {
    Single(String),
    Multiple(Vec<String>),
}

impl TryConvert for PartitionFilterValue {
    fn try_convert(val: Value) -> RbResult<Self> {
        if let Ok(v) = Vec::<String>::try_convert(val) {
            Ok(PartitionFilterValue::Multiple(v))
        } else {
            Ok(PartitionFilterValue::Single(String::try_convert(val)?))
        }
    }
}

unsafe impl TryConvertOwned for PartitionFilterValue {}

#[magnus::wrap(class = "DeltaLake::RawDeltaTable")]
struct RawDeltaTable {
    _table: Arc<Mutex<deltalake::DeltaTable>>,
}

#[magnus::wrap(class = "DeltaLake::RawDeltaTableMetaData")]
struct RawDeltaTableMetaData {
    id: String,
    name: Option<String>,
    description: Option<String>,
    partition_columns: Vec<String>,
    created_time: Option<i64>,
    configuration: HashMap<String, String>,
}

impl RawDeltaTableMetaData {
    fn id(&self) -> String {
        self.id.clone()
    }

    fn name(&self) -> Option<String> {
        self.name.clone()
    }

    fn description(&self) -> Option<String> {
        self.description.clone()
    }

    fn partition_columns(&self) -> Vec<String> {
        self.partition_columns.clone()
    }

    fn created_time(&self) -> Option<i64> {
        self.created_time
    }

    fn configuration(&self) -> HashMap<String, String> {
        self.configuration.clone()
    }
}

type StringVec = Vec<String>;

impl RawDeltaTable {
    fn with_table<T>(&self, func: impl Fn(&deltalake::DeltaTable) -> RbResult<T>) -> RbResult<T> {
        match self._table.lock() {
            Ok(table) => func(&table),
            Err(e) => Err(RbRuntimeError::new_err(e.to_string())),
        }
    }

    fn cloned_state(&self) -> RbResult<EagerSnapshot> {
        self.with_table(|t| {
            t.snapshot()
                .map(|snapshot| snapshot.snapshot())
                .cloned()
                .map_err(RubyError::from)
                .map_err(RbErr::from)
        })
    }

    fn log_store(&self) -> RbResult<LogStoreRef> {
        self.with_table(|t| Ok(t.log_store().clone()))
    }

    fn set_state(&self, state: Option<DeltaTableState>) -> RbResult<()> {
        let mut original = self
            ._table
            .lock()
            .map_err(|e| RbRuntimeError::new_err(e.to_string()))?;
        original.state = state;
        Ok(())
    }
}

impl RawDeltaTable {
    pub fn new(
        table_uri: String,
        version: Option<i64>,
        storage_options: Option<HashMap<String, String>>,
        without_files: bool,
        log_buffer_size: Option<usize>,
    ) -> RbResult<Self> {
        let table_url = deltalake::table::builder::parse_table_uri(table_uri)
            .map_err(error::RubyError::from)?;
        let mut builder = deltalake::DeltaTableBuilder::from_url(table_url)
            .map_err(error::RubyError::from)?
            .with_io_runtime(IORuntime::default());

        if let Some(storage_options) = storage_options {
            builder = builder.with_storage_options(storage_options)
        }
        if let Some(version) = version {
            builder = builder.with_version(version)
        }
        if without_files {
            builder = builder.without_files()
        }
        if let Some(buf_size) = log_buffer_size {
            builder = builder
                .with_log_buffer_size(buf_size)
                .map_err(RubyError::from)?;
        }

        let table = rt().block_on(builder.load()).map_err(RubyError::from)?;
        Ok(RawDeltaTable {
            _table: Arc::new(Mutex::new(table)),
        })
    }

    pub fn is_deltatable(
        table_uri: String,
        storage_options: Option<HashMap<String, String>>,
    ) -> RbResult<bool> {
        let table_url = deltalake::table::builder::ensure_table_uri(table_uri)
            .map_err(|_| RbValueError::new_err("Invalid table URI"))?;
        let mut builder = deltalake::DeltaTableBuilder::from_url(table_url)
            .map_err(|_| RbValueError::new_err("Failed to create table builder"))?;
        if let Some(storage_options) = storage_options {
            builder = builder.with_storage_options(storage_options)
        }
        Ok(rt()
            .block_on(async {
                match builder.build() {
                    Ok(table) => table.verify_deltatable_existence().await,
                    Err(err) => Err(err),
                }
            })
            .map_err(RubyError::from)?)
    }

    pub fn table_uri(&self) -> RbResult<String> {
        self.with_table(|t| Ok(t.table_url().to_string()))
    }

    pub fn version(&self) -> RbResult<Option<i64>> {
        self.with_table(|t| Ok(t.version()))
    }

    pub fn has_files(&self) -> RbResult<bool> {
        self.with_table(|t| Ok(t.config.require_files))
    }

    pub fn metadata(&self) -> RbResult<RawDeltaTableMetaData> {
        let metadata = self.with_table(|t| {
            let snapshot = t.snapshot().map_err(RubyError::from).map_err(RbErr::from)?;
            Ok(snapshot.metadata().clone())
        })?;
        Ok(RawDeltaTableMetaData {
            id: metadata.id().to_string(),
            name: metadata.name().map(String::from),
            description: metadata.description().map(String::from),
            partition_columns: metadata.partition_columns().clone(),
            created_time: metadata.created_time(),
            configuration: metadata.configuration().clone(),
        })
    }

    pub fn protocol_versions(&self) -> RbResult<(i32, i32, Option<StringVec>, Option<StringVec>)> {
        let table_protocol = self.with_table(|t| {
            let snapshot = t.snapshot().map_err(RubyError::from).map_err(RbErr::from)?;
            Ok(snapshot.protocol().clone())
        })?;
        Ok((
            table_protocol.min_reader_version(),
            table_protocol.min_writer_version(),
            table_protocol.writer_features().and_then(|features| {
                let empty_set = !features.is_empty();
                empty_set.then(|| {
                    features
                        .iter()
                        .map(|v| v.to_string())
                        .collect::<Vec<String>>()
                })
            }),
            table_protocol.reader_features().and_then(|features| {
                let empty_set = !features.is_empty();
                empty_set.then(|| {
                    features
                        .iter()
                        .map(|v| v.to_string())
                        .collect::<Vec<String>>()
                })
            }),
        ))
    }

    pub fn load_version(&self, version: i64) -> RbResult<()> {
        #[allow(clippy::await_holding_lock)]
        rt().block_on(async {
            let mut table = self
                ._table
                .lock()
                .map_err(|e| RbRuntimeError::new_err(e.to_string()))?;
            (*table)
                .load_version(version)
                .await
                .map_err(RubyError::from)
                .map_err(RbErr::from)
        })
    }

    pub fn get_latest_version(&self) -> RbResult<i64> {
        #[allow(clippy::await_holding_lock)]
        rt().block_on(async {
            match self._table.lock() {
                Ok(table) => table
                    .get_latest_version()
                    .await
                    .map_err(RubyError::from)
                    .map_err(RbErr::from),
                Err(e) => Err(RbRuntimeError::new_err(e.to_string())),
            }
        })
    }

    pub fn get_num_index_cols(&self) -> RbResult<i32> {
        self.with_table(|t| {
            let n_cols = t
                .snapshot()
                .map_err(RubyError::from)?
                .config()
                .num_indexed_cols();
            Ok(match n_cols {
                DataSkippingNumIndexedCols::NumColumns(n_cols) => n_cols as i32,
                DataSkippingNumIndexedCols::AllColumns => -1,
            })
        })
    }

    pub fn get_stats_columns(&self) -> RbResult<Option<Vec<String>>> {
        self.with_table(|t| {
            Ok(t.snapshot()
                .map_err(RubyError::from)?
                .config()
                .data_skipping_stats_columns
                .as_ref()
                .map(|v| v.iter().map(|s| s.to_string()).collect::<Vec<String>>()))
        })
    }

    pub fn load_with_datetime(&self, ds: String) -> RbResult<()> {
        let datetime =
            DateTime::<Utc>::from(DateTime::<FixedOffset>::parse_from_rfc3339(&ds).map_err(
                |err| RbValueError::new_err(format!("Failed to parse datetime string: {err}")),
            )?);
        #[allow(clippy::await_holding_lock)]
        rt().block_on(async {
            let mut table = self
                ._table
                .lock()
                .map_err(|e| RbRuntimeError::new_err(e.to_string()))?;
            (*table)
                .load_with_datetime(datetime)
                .await
                .map_err(RubyError::from)
                .map_err(RbErr::from)
        })
    }

    pub fn files(
        &self,
        partition_filters: Option<Vec<(String, String, PartitionFilterValue)>>,
    ) -> RbResult<Vec<String>> {
        if !self.has_files()? {
            return Err(DeltaError::new_err("Table is instantiated without files."));
        }

        if let Some(filters) = partition_filters {
            let filters = convert_partition_filters(filters).map_err(RubyError::from)?;
            Ok(self
                .with_table(|t| {
                    rt().block_on(async {
                        t.get_files_by_partitions(&filters)
                            .await
                            .map_err(RubyError::from)
                            .map_err(RbErr::from)
                    })
                })?
                .into_iter()
                .map(|p| p.to_string())
                .collect())
        } else {
            match self._table.lock() {
                Ok(table) => Ok(table
                    .get_file_uris()
                    .map_err(RubyError::from)?
                    .map(|f| f.to_string())
                    .collect()),
                Err(e) => Err(RbRuntimeError::new_err(e.to_string())),
            }
        }
    }

    pub fn file_uris(
        &self,
        partition_filters: Option<Vec<(String, String, PartitionFilterValue)>>,
    ) -> RbResult<Vec<String>> {
        if !self.with_table(|t| Ok(t.config.require_files))? {
            return Err(DeltaError::new_err("Table is initiated without files."));
        }

        if let Some(filters) = partition_filters {
            let filters = convert_partition_filters(filters).map_err(RubyError::from)?;
            self.with_table(|t| {
                rt().block_on(async {
                    t.get_file_uris_by_partitions(&filters)
                        .await
                        .map_err(RubyError::from)
                        .map_err(RbErr::from)
                })
            })
        } else {
            self.with_table(|t| {
                Ok(t.get_file_uris()
                    .map_err(RubyError::from)
                    .map_err(RbErr::from)?
                    .collect::<Vec<String>>())
            })
        }
    }

    pub fn schema(ruby: &Ruby, rb_self: &Self) -> RbResult<Value> {
        let schema = rb_self.with_table(|t| {
            let snapshot = t.snapshot().map_err(RubyError::from).map_err(RbErr::from)?;
            Ok(snapshot.schema().clone())
        })?;
        schema_to_rbobject(schema, ruby)
    }

    pub fn vacuum(
        &self,
        dry_run: bool,
        retention_hours: Option<u64>,
        enforce_retention_duration: bool,
        commit_properties: Option<RbCommitProperties>,
        post_commithook_properties: Option<RbPostCommitHookProperties>,
    ) -> RbResult<Vec<String>> {
        let table = self._table.lock().map_err(to_rt_err)?.clone();
        let mut cmd = table
            .vacuum()
            .with_enforce_retention_duration(enforce_retention_duration)
            .with_dry_run(dry_run);

        if let Some(retention_period) = retention_hours {
            cmd = cmd.with_retention_period(Duration::hours(retention_period as i64));
        }

        if let Some(commit_properties) =
            maybe_create_commit_properties(commit_properties, post_commithook_properties)
        {
            cmd = cmd.with_commit_properties(commit_properties);
        }
        let (table, metrics) = rt().block_on(cmd.into_future()).map_err(RubyError::from)?;
        self.set_state(table.state)?;
        Ok(metrics.files_deleted)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn compact_optimize(
        &self,
        partition_filters: Option<Vec<(String, String, PartitionFilterValue)>>,
        target_size: Option<u64>,
        max_concurrent_tasks: Option<usize>,
        min_commit_interval: Option<u64>,
        writer_properties: Option<RbWriterProperties>,
        commit_properties: Option<RbCommitProperties>,
        post_commithook_properties: Option<RbPostCommitHookProperties>,
    ) -> RbResult<String> {
        let table = self._table.lock().map_err(to_rt_err)?.clone();
        let mut cmd = table
            .optimize()
            .with_max_concurrent_tasks(max_concurrent_tasks.unwrap_or_else(num_cpus::get));

        if let Some(size) = target_size {
            cmd = cmd.with_target_size(size);
        }
        if let Some(commit_interval) = min_commit_interval {
            cmd = cmd.with_min_commit_interval(time::Duration::from_secs(commit_interval));
        }

        if let Some(writer_props) = writer_properties {
            cmd = cmd.with_writer_properties(
                set_writer_properties(writer_props).map_err(RubyError::from)?,
            );
        }

        if let Some(commit_properties) =
            maybe_create_commit_properties(commit_properties, post_commithook_properties)
        {
            cmd = cmd.with_commit_properties(commit_properties);
        }

        let converted_filters = convert_partition_filters(partition_filters.unwrap_or_default())
            .map_err(RubyError::from)?;
        cmd = cmd.with_filters(&converted_filters);

        let (table, metrics) = rt().block_on(cmd.into_future()).map_err(RubyError::from)?;
        self.set_state(table.state)?;
        Ok(serde_json::to_string(&metrics).unwrap())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn z_order_optimize(
        &self,
        z_order_columns: Vec<String>,
        partition_filters: Option<Vec<(String, String, PartitionFilterValue)>>,
        target_size: Option<u64>,
        max_concurrent_tasks: Option<usize>,
        max_spill_size: Option<usize>,
        max_temp_directory_size: Option<u64>,
        min_commit_interval: Option<u64>,
        writer_properties: Option<RbWriterProperties>,
        commit_properties: Option<RbCommitProperties>,
        post_commithook_properties: Option<RbPostCommitHookProperties>,
    ) -> RbResult<String> {
        let table = self._table.lock().map_err(to_rt_err)?.clone();
        let mut cmd = table
            .clone()
            .optimize()
            .with_max_concurrent_tasks(max_concurrent_tasks.unwrap_or_else(num_cpus::get))
            .with_type(OptimizeType::ZOrder(z_order_columns));

        if max_spill_size.is_some() || max_temp_directory_size.is_some() {
            let session =
                create_session_state_for_optimize(max_spill_size, max_temp_directory_size);
            cmd = cmd.with_session_state(Arc::new(session));
        }

        if let Some(size) = target_size {
            cmd = cmd.with_target_size(size);
        }
        if let Some(commit_interval) = min_commit_interval {
            cmd = cmd.with_min_commit_interval(time::Duration::from_secs(commit_interval));
        }

        if let Some(writer_props) = writer_properties {
            cmd = cmd.with_writer_properties(
                set_writer_properties(writer_props).map_err(RubyError::from)?,
            );
        }

        if let Some(commit_properties) =
            maybe_create_commit_properties(commit_properties, post_commithook_properties)
        {
            cmd = cmd.with_commit_properties(commit_properties);
        }

        let converted_filters = convert_partition_filters(partition_filters.unwrap_or_default())
            .map_err(RubyError::from)?;
        cmd = cmd.with_filters(&converted_filters);

        let (table, metrics) = rt().block_on(cmd.into_future()).map_err(RubyError::from)?;
        self.set_state(table.state)?;
        Ok(serde_json::to_string(&metrics).unwrap())
    }

    pub fn add_columns(&self, fields: RArray) -> RbResult<()> {
        let fields = fields.typecheck::<Obj<Field>>()?;

        let table = self._table.lock().map_err(to_rt_err)?.clone();
        let mut cmd = table.add_columns();

        let new_fields = fields
            .iter()
            .map(|v| v.inner.clone())
            .collect::<Vec<StructField>>();

        cmd = cmd.with_fields(new_fields);

        let table = rt().block_on(cmd.into_future()).map_err(RubyError::from)?;
        self.set_state(table.state)?;
        Ok(())
    }

    pub fn add_feature(
        &self,
        feature: RArray,
        allow_protocol_versions_increase: bool,
    ) -> RbResult<()> {
        let feature = feature
            .into_iter()
            .map(TableFeatures::try_convert)
            .collect::<RbResult<Vec<_>>>()?;

        let table = self._table.lock().map_err(to_rt_err)?.clone();
        let cmd = table
            .add_feature()
            .with_features(feature)
            .with_allow_protocol_versions_increase(allow_protocol_versions_increase);

        let table = rt().block_on(cmd.into_future()).map_err(RubyError::from)?;
        self.set_state(table.state)?;
        Ok(())
    }

    pub fn add_constraints(&self, constraints: HashMap<String, String>) -> RbResult<()> {
        let table = self._table.lock().map_err(to_rt_err)?.clone();
        let mut cmd = table.add_constraint();

        for (col_name, expression) in constraints {
            cmd = cmd.with_constraint(col_name.clone(), expression.clone());
        }

        let table = rt().block_on(cmd.into_future()).map_err(RubyError::from)?;
        self.set_state(table.state)?;
        Ok(())
    }

    pub fn drop_constraints(&self, name: String, raise_if_not_exists: bool) -> RbResult<()> {
        let table = self._table.lock().map_err(to_rt_err)?.clone();
        let cmd = table
            .drop_constraints()
            .with_constraint(name)
            .with_raise_if_not_exists(raise_if_not_exists);

        let table = rt().block_on(cmd.into_future()).map_err(RubyError::from)?;
        self.set_state(table.state)?;
        Ok(())
    }

    pub fn load_cdf(
        &self,
        starting_version: Option<i64>,
        ending_version: Option<i64>,
        starting_timestamp: Option<String>,
        ending_timestamp: Option<String>,
        columns: Option<Vec<String>>,
    ) -> RbResult<ArrowArrayStream> {
        let ctx = SessionContext::new();
        let table = self._table.lock().map_err(to_rt_err)?.clone();
        let mut cmd = table.scan_cdf();

        if let Some(sv) = starting_version {
            cmd = cmd.with_starting_version(sv);
        }
        if let Some(ev) = ending_version {
            cmd = cmd.with_ending_version(ev);
        }
        if let Some(st) = starting_timestamp {
            let starting_ts: DateTime<Utc> = DateTime::<Utc>::from_str(&st)
                .map_err(|pe| RbValueError::new_err(pe.to_string()))?
                .to_utc();
            cmd = cmd.with_starting_timestamp(starting_ts);
        }
        if let Some(et) = ending_timestamp {
            let ending_ts = DateTime::<Utc>::from_str(&et)
                .map_err(|pe| RbValueError::new_err(pe.to_string()))?
                .to_utc();
            cmd = cmd.with_starting_timestamp(ending_ts);
        }

        let table_provider: Arc<dyn TableProvider> =
            Arc::new(DeltaCdfTableProvider::try_new(cmd).map_err(RubyError::from)?);

        let plan = rt()
            .block_on(async {
                let mut df = ctx.read_table(table_provider)?;
                if let Some(columns) = columns {
                    let cols: Vec<_> = columns.iter().map(|c| c.as_ref()).collect();
                    df = df.select_columns(&cols)?;
                }
                df.create_physical_plan().await
            })
            .map_err(RubyError::from)?;

        let mut tasks = vec![];
        for p in 0..plan.properties().output_partitioning().partition_count() {
            let inner_plan = plan.clone();
            let partition_batch = inner_plan.execute(p, ctx.task_ctx()).unwrap();
            let handle = rt().spawn(collect_sendable_stream(partition_batch));
            tasks.push(handle);
        }

        // This is unfortunate.
        let batches = rt()
            .block_on(join_all(tasks))
            .into_iter()
            .flatten()
            .collect::<Result<Vec<Vec<_>>, _>>()
            .unwrap()
            .into_iter()
            .flatten()
            .map(Ok);
        let batch_iter = RecordBatchIterator::new(batches, plan.schema());
        let ffi_stream = FFI_ArrowArrayStream::new(Box::new(batch_iter));
        Ok(ArrowArrayStream { stream: ffi_stream })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_merge_builder(
        &self,
        source: RbArrowType<ArrowArrayStreamReader>,
        predicate: String,
        source_alias: Option<String>,
        target_alias: Option<String>,
        safe_cast: bool,
        writer_properties: Option<RbWriterProperties>,
        post_commithook_properties: Option<RbPostCommitHookProperties>,
        commit_properties: Option<RbCommitProperties>,
    ) -> RbResult<RbMergeBuilder> {
        Ok(RbMergeBuilder::new(
            self.log_store()?,
            self.cloned_state()?,
            source.0,
            predicate,
            source_alias,
            target_alias,
            safe_cast,
            writer_properties,
            post_commithook_properties,
            commit_properties,
        )
        .map_err(RubyError::from)?)
    }

    pub fn merge_execute(&self, merge_builder: &RbMergeBuilder) -> RbResult<String> {
        let (table, metrics) = merge_builder.execute().map_err(RubyError::from)?;
        self.set_state(table.state)?;
        Ok(metrics)
    }

    pub fn restore(
        &self,
        target: Option<Value>,
        ignore_missing_files: bool,
        protocol_downgrade_allowed: bool,
        commit_properties: Option<RbCommitProperties>,
    ) -> RbResult<String> {
        let table = self._table.lock().map_err(to_rt_err)?.clone();
        let mut cmd = table.restore();
        if let Some(val) = target {
            if let Some(version) = Integer::from_value(val) {
                cmd = cmd.with_version_to_restore(version.to_i64()?)
            }
            if let Ok(ds) = String::try_convert(val) {
                let datetime = DateTime::<Utc>::from(
                    DateTime::<FixedOffset>::parse_from_rfc3339(ds.as_ref()).map_err(|err| {
                        RbValueError::new_err(format!("Failed to parse datetime string: {err}"))
                    })?,
                );
                cmd = cmd.with_datetime_to_restore(datetime)
            }
        }
        cmd = cmd.with_ignore_missing_files(ignore_missing_files);
        cmd = cmd.with_protocol_downgrade_allowed(protocol_downgrade_allowed);

        if let Some(commit_properties) = maybe_create_commit_properties(commit_properties, None) {
            cmd = cmd.with_commit_properties(commit_properties);
        }

        let (table, metrics) = rt().block_on(cmd.into_future()).map_err(RubyError::from)?;
        self.set_state(table.state)?;
        Ok(serde_json::to_string(&metrics).unwrap())
    }

    pub fn history(&self, limit: Option<usize>) -> RbResult<Vec<String>> {
        #[allow(clippy::await_holding_lock)]
        let history = rt().block_on(async {
            match self._table.lock() {
                Ok(table) => table
                    .history(limit)
                    .await
                    .map_err(RubyError::from)
                    .map_err(RbErr::from),
                Err(e) => Err(RbRuntimeError::new_err(e.to_string())),
            }
        })?;
        Ok(history
            .map(|c| serde_json::to_string(&c).unwrap())
            .collect())
    }

    pub fn update_incremental(&self) -> RbResult<()> {
        #[allow(clippy::await_holding_lock)]
        Ok(rt()
            .block_on(async {
                let mut table = self
                    ._table
                    .lock()
                    .map_err(|e| DeltaTableError::Generic(e.to_string()))?;
                (*table).update_incremental(None).await
            })
            .map_err(RubyError::from)?)
    }

    fn get_active_partitions(ruby: &Ruby, rb_self: &Self) -> RbResult<RArray> {
        let schema = rb_self.with_table(|t| {
            let snapshot = t.snapshot().map_err(RubyError::from).map_err(RbErr::from)?;
            Ok(snapshot.schema().clone())
        })?;
        let metadata = rb_self.with_table(|t| {
            let snapshot = t.snapshot().map_err(RubyError::from).map_err(RbErr::from)?;
            Ok(snapshot.metadata().clone())
        })?;
        let _column_names: HashSet<&str> =
            schema.fields().map(|field| field.name().as_str()).collect();
        let partition_columns: HashSet<&str> = metadata
            .partition_columns()
            .iter()
            .map(|col| col.as_str())
            .collect();

        let converted_filters = Vec::new();

        let partition_columns: Vec<&str> = partition_columns.into_iter().collect();

        let state = rb_self.cloned_state()?;
        let log_store = rb_self.log_store()?;
        let adds: Vec<_> = rt()
            .block_on(async {
                state
                    .file_views_by_partitions(&log_store, &converted_filters)
                    .try_collect()
                    .await
            })
            .map_err(RubyError::from)?;
        let active_partitions: HashSet<Vec<(&str, Option<String>)>> = adds
            .iter()
            .flat_map(|add| {
                Ok::<_, RubyError>(
                    partition_columns
                        .iter()
                        .map(|col| {
                            (
                                *col,
                                add.partition_values()
                                    .and_then(|v| {
                                        v.index_of(col).and_then(|idx| v.value(idx).cloned())
                                    })
                                    .map(|v| v.serialize()),
                            )
                        })
                        .collect(),
                )
            })
            .collect();

        Ok(ruby.ary_from_iter(active_partitions))
    }

    pub fn create_checkpoint(&self) -> RbResult<()> {
        let operation_id = Uuid::new_v4();

        #[allow(clippy::await_holding_lock)]
        let _result = rt().block_on(async {
            match self._table.lock() {
                Ok(table) => create_checkpoint(&table, Some(operation_id))
                    .await
                    .map_err(RubyError::from)
                    .map_err(RbErr::from),
                Err(e) => Err(RbRuntimeError::new_err(e.to_string())),
            }
        });

        Ok(())
    }

    pub fn cleanup_metadata(&self) -> RbResult<()> {
        let operation_id = Uuid::new_v4();

        #[allow(clippy::await_holding_lock)]
        let result = rt().block_on(async {
            match self._table.lock() {
                Ok(table) => {
                    let result = cleanup_metadata(&table, Some(operation_id))
                        .await
                        .map_err(RubyError::from)
                        .map_err(RbErr::from)?;

                    let new_state = if result > 0 {
                        Some(
                            DeltaTableState::try_new(
                                &table.log_store(),
                                table.config.clone(),
                                table.version(),
                            )
                            .await
                            .map_err(RubyError::from)?,
                        )
                    } else {
                        None
                    };

                    Ok((result, new_state))
                }
                Err(e) => Err(RbRuntimeError::new_err(e.to_string())),
            }
        });

        let (_result, new_state) = result?;

        if new_state.is_some() {
            self.set_state(new_state)?;
        }

        Ok(())
    }

    pub fn get_add_file_sizes(&self) -> RbResult<HashMap<String, i64>> {
        self.with_table(|t| {
            let log_store = t.log_store();
            let sizes: HashMap<String, i64> = rt()
                .block_on(async {
                    t.snapshot()?
                        .snapshot()
                        .file_views(&log_store, None)
                        .map_ok(|f| (f.path().to_string(), f.size()))
                        .try_collect()
                        .await
                })
                .map_err(RubyError::from)?;
            Ok(sizes)
        })
    }

    pub fn delete(
        &self,
        predicate: Option<String>,
        writer_properties: Option<RbWriterProperties>,
        commit_properties: Option<RbCommitProperties>,
        post_commithook_properties: Option<RbPostCommitHookProperties>,
    ) -> RbResult<String> {
        let table = self._table.lock().map_err(to_rt_err)?.clone();
        let mut cmd = table.delete();
        if let Some(predicate) = predicate {
            cmd = cmd.with_predicate(predicate);
        }
        if let Some(writer_props) = writer_properties {
            cmd = cmd.with_writer_properties(
                set_writer_properties(writer_props).map_err(RubyError::from)?,
            );
        }
        if let Some(commit_properties) =
            maybe_create_commit_properties(commit_properties, post_commithook_properties)
        {
            cmd = cmd.with_commit_properties(commit_properties);
        }

        let (table, metrics) = rt().block_on(cmd.into_future()).map_err(RubyError::from)?;
        self.set_state(table.state)?;
        Ok(serde_json::to_string(&metrics).unwrap())
    }

    pub fn set_table_properties(
        &self,
        properties: HashMap<String, String>,
        raise_if_not_exists: bool,
    ) -> RbResult<()> {
        let table = self._table.lock().map_err(to_rt_err)?.clone();
        let cmd = table
            .set_tbl_properties()
            .with_properties(properties)
            .with_raise_if_not_exists(raise_if_not_exists);

        let table = rt().block_on(cmd.into_future()).map_err(RubyError::from)?;
        self.set_state(table.state)?;
        Ok(())
    }

    pub fn repair(
        &self,
        dry_run: bool,
        commit_properties: Option<RbCommitProperties>,
        post_commithook_properties: Option<RbPostCommitHookProperties>,
    ) -> RbResult<String> {
        let table = self._table.lock().map_err(to_rt_err)?.clone();
        let mut cmd = table.filesystem_check().with_dry_run(dry_run);

        if let Some(commit_properties) =
            maybe_create_commit_properties(commit_properties, post_commithook_properties)
        {
            cmd = cmd.with_commit_properties(commit_properties);
        }

        let (table, metrics) = rt().block_on(cmd.into_future()).map_err(RubyError::from)?;
        self.set_state(table.state)?;
        Ok(serde_json::to_string(&metrics).unwrap())
    }

    pub fn transaction_version(&self, app_id: String) -> RbResult<Option<i64>> {
        // NOTE: this will simplify once we have moved logstore onto state.
        let log_store = self.log_store()?;
        let snapshot = self.with_table(|t| Ok(t.snapshot().map_err(RubyError::from)?.clone()))?;
        Ok(rt()
            .block_on(snapshot.transaction_version(log_store.as_ref(), app_id))
            .map_err(RubyError::from)?)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn write(
        &self,
        data: RbArrowType<ArrowArrayStreamReader>,
        mode: String,
        schema_mode: Option<String>,
        partition_by: Option<Vec<String>>,
        predicate: Option<String>,
        target_file_size: Option<usize>,
        name: Option<String>,
        description: Option<String>,
        configuration: Option<HashMap<String, Option<String>>>,
        writer_properties: Option<RbWriterProperties>,
        commit_properties: Option<RbCommitProperties>,
        post_commithook_properties: Option<RbPostCommitHookProperties>,
    ) -> RbResult<()> {
        let table = {
            let table = self._table.lock().map_err(to_rt_err)?.clone();
            let batches = data.0.map(|batch| batch.unwrap()).collect::<Vec<_>>();

            let save_mode = mode.parse().map_err(RubyError::from)?;
            let mut builder = table.write(batches).with_save_mode(save_mode);

            if let Some(schema_mode) = schema_mode {
                builder = builder.with_schema_mode(schema_mode.parse().map_err(RubyError::from)?);
            }
            if let Some(partition_columns) = partition_by {
                builder = builder.with_partition_columns(partition_columns);
            }

            if let Some(writer_props) = writer_properties {
                builder = builder.with_writer_properties(
                    set_writer_properties(writer_props).map_err(RubyError::from)?,
                );
            }

            if let Some(name) = &name {
                builder = builder.with_table_name(name);
            };

            if let Some(description) = &description {
                builder = builder.with_description(description);
            };

            if let Some(predicate) = predicate {
                builder = builder.with_replace_where(predicate);
            };

            if let Some(target_file_size) = target_file_size {
                builder = builder.with_target_file_size(target_file_size)
            };

            if let Some(config) = configuration {
                builder = builder.with_configuration(config);
            };

            if let Some(commit_properties) =
                maybe_create_commit_properties(commit_properties, post_commithook_properties)
            {
                builder = builder.with_commit_properties(commit_properties);
            };

            rt().block_on(builder.into_future())
                .map_err(RubyError::from)
                .map_err(RbErr::from)
        }?;

        self.set_state(table.state)?;
        Ok(())
    }
}

fn set_post_commithook_properties(
    mut commit_properties: CommitProperties,
    post_commithook_properties: RbPostCommitHookProperties,
) -> CommitProperties {
    commit_properties =
        commit_properties.with_create_checkpoint(post_commithook_properties.create_checkpoint);
    commit_properties = commit_properties
        .with_cleanup_expired_logs(post_commithook_properties.cleanup_expired_logs);
    commit_properties
}

fn set_writer_properties(writer_properties: RbWriterProperties) -> DeltaResult<WriterProperties> {
    let mut properties = WriterProperties::builder();
    let data_page_size_limit = writer_properties.data_page_size_limit;
    let dictionary_page_size_limit = writer_properties.dictionary_page_size_limit;
    let data_page_row_count_limit = writer_properties.data_page_row_count_limit;
    let write_batch_size = writer_properties.write_batch_size;
    let max_row_group_size = writer_properties.max_row_group_size;
    let compression = writer_properties.compression;
    let statistics_truncate_length = writer_properties.statistics_truncate_length;
    let default_column_properties = writer_properties.default_column_properties;
    let column_properties = writer_properties.column_properties;

    if let Some(data_page_size) = data_page_size_limit {
        properties = properties.set_data_page_size_limit(data_page_size);
    }
    if let Some(dictionary_page_size) = dictionary_page_size_limit {
        properties = properties.set_dictionary_page_size_limit(dictionary_page_size);
    }
    if let Some(data_page_row_count) = data_page_row_count_limit {
        properties = properties.set_data_page_row_count_limit(data_page_row_count);
    }
    if let Some(batch_size) = write_batch_size {
        properties = properties.set_write_batch_size(batch_size);
    }
    if let Some(row_group_size) = max_row_group_size {
        properties = properties.set_max_row_group_size(row_group_size);
    }
    properties = properties.set_statistics_truncate_length(statistics_truncate_length);

    if let Some(compression) = compression {
        let compress: Compression = compression
            .parse()
            .map_err(|err: ParquetError| DeltaTableError::Generic(err.to_string()))?;

        properties = properties.set_compression(compress);
    }

    if let Some(default_column_properties) = default_column_properties {
        if let Some(dictionary_enabled) = default_column_properties.dictionary_enabled {
            properties = properties.set_dictionary_enabled(dictionary_enabled);
        }
        if let Some(bloom_filter_properties) = default_column_properties.bloom_filter_properties {
            if let Some(set_bloom_filter_enabled) = bloom_filter_properties.set_bloom_filter_enabled
            {
                properties = properties.set_bloom_filter_enabled(set_bloom_filter_enabled);
            }
            if let Some(bloom_filter_fpp) = bloom_filter_properties.fpp {
                properties = properties.set_bloom_filter_fpp(bloom_filter_fpp);
            }
            if let Some(bloom_filter_ndv) = bloom_filter_properties.ndv {
                properties = properties.set_bloom_filter_ndv(bloom_filter_ndv);
            }
        }
    }
    if let Some(column_properties) = column_properties {
        for (column_name, column_prop) in column_properties {
            if let Some(column_prop) = column_prop {
                if let Some(dictionary_enabled) = column_prop.dictionary_enabled {
                    properties = properties.set_column_dictionary_enabled(
                        column_name.clone().into(),
                        dictionary_enabled,
                    );
                }
                if let Some(bloom_filter_properties) = column_prop.bloom_filter_properties {
                    if let Some(set_bloom_filter_enabled) =
                        bloom_filter_properties.set_bloom_filter_enabled
                    {
                        properties = properties.set_column_bloom_filter_enabled(
                            column_name.clone().into(),
                            set_bloom_filter_enabled,
                        );
                    }
                    if let Some(bloom_filter_fpp) = bloom_filter_properties.fpp {
                        properties = properties.set_column_bloom_filter_fpp(
                            column_name.clone().into(),
                            bloom_filter_fpp,
                        );
                    }
                    if let Some(bloom_filter_ndv) = bloom_filter_properties.ndv {
                        properties = properties
                            .set_column_bloom_filter_ndv(column_name.into(), bloom_filter_ndv);
                    }
                }
            }
        }
    }
    Ok(properties.build())
}

fn convert_partition_filters(
    partitions_filters: Vec<(String, String, PartitionFilterValue)>,
) -> Result<Vec<PartitionFilter>, DeltaTableError> {
    partitions_filters
        .into_iter()
        .map(|filter| match filter {
            (key, op, PartitionFilterValue::Single(v)) => {
                let key: &'_ str = key.as_ref();
                let op: &'_ str = op.as_ref();
                let v: &'_ str = v.as_ref();
                PartitionFilter::try_from((key, op, v))
            }
            (key, op, PartitionFilterValue::Multiple(v)) => {
                let key: &'_ str = key.as_ref();
                let op: &'_ str = op.as_ref();
                let v: Vec<&'_ str> = v.iter().map(|v| v.as_ref()).collect();
                PartitionFilter::try_from((key, op, v.as_slice()))
            }
        })
        .collect()
}

fn maybe_create_commit_properties(
    maybe_commit_properties: Option<RbCommitProperties>,
    post_commithook_properties: Option<RbPostCommitHookProperties>,
) -> Option<CommitProperties> {
    if maybe_commit_properties.is_none() && post_commithook_properties.is_none() {
        return None;
    }
    let mut commit_properties = CommitProperties::default();

    if let Some(commit_props) = maybe_commit_properties {
        if let Some(metadata) = commit_props.custom_metadata {
            let json_metadata: Map<String, serde_json::Value> =
                metadata.into_iter().map(|(k, v)| (k, v.into())).collect();
            commit_properties = commit_properties.with_metadata(json_metadata);
        };

        if let Some(max_retries) = commit_props.max_commit_retries {
            commit_properties = commit_properties.with_max_retries(max_retries);
        };

        if let Some(app_transactions) = commit_props.app_transactions {
            let app_transactions = app_transactions.iter().map(Transaction::from).collect();
            commit_properties = commit_properties.with_application_transactions(app_transactions);
        }
    }

    if let Some(post_commit_hook_props) = post_commithook_properties {
        commit_properties =
            set_post_commithook_properties(commit_properties, post_commit_hook_props)
    }
    Some(commit_properties)
}

fn rust_core_version() -> String {
    deltalake::crate_version().to_string()
}

pub struct BloomFilterProperties {
    pub set_bloom_filter_enabled: Option<bool>,
    pub fpp: Option<f64>,
    pub ndv: Option<u64>,
}

impl TryConvert for BloomFilterProperties {
    fn try_convert(val: Value) -> RbResult<Self> {
        Ok(BloomFilterProperties {
            set_bloom_filter_enabled: val.funcall("set_bloom_filter_enabled", ())?,
            fpp: val.funcall("fpp", ())?,
            ndv: val.funcall("ndv", ())?,
        })
    }
}

pub struct ColumnProperties {
    pub dictionary_enabled: Option<bool>,
    pub max_statistics_size: Option<usize>,
    pub bloom_filter_properties: Option<BloomFilterProperties>,
}

impl TryConvert for ColumnProperties {
    fn try_convert(val: Value) -> RbResult<Self> {
        Ok(ColumnProperties {
            dictionary_enabled: val.funcall("dictionary_enabled", ())?,
            max_statistics_size: val.funcall("max_statistics_size", ())?,
            bloom_filter_properties: val.funcall("bloom_filter_properties", ())?,
        })
    }
}

pub struct RbWriterProperties {
    data_page_size_limit: Option<usize>,
    dictionary_page_size_limit: Option<usize>,
    data_page_row_count_limit: Option<usize>,
    write_batch_size: Option<usize>,
    max_row_group_size: Option<usize>,
    statistics_truncate_length: Option<usize>,
    compression: Option<String>,
    default_column_properties: Option<ColumnProperties>,
    column_properties: Option<HashMap<String, Option<ColumnProperties>>>,
}

impl TryConvert for RbWriterProperties {
    fn try_convert(val: Value) -> RbResult<Self> {
        Ok(RbWriterProperties {
            data_page_size_limit: val.funcall("data_page_size_limit", ())?,
            dictionary_page_size_limit: val.funcall("dictionary_page_size_limit", ())?,
            data_page_row_count_limit: val.funcall("data_page_row_count_limit", ())?,
            write_batch_size: val.funcall("write_batch_size", ())?,
            max_row_group_size: val.funcall("max_row_group_size", ())?,
            statistics_truncate_length: val.funcall("statistics_truncate_length", ())?,
            compression: val.funcall("compression", ())?,
            default_column_properties: val.funcall("default_column_properties", ())?,
            // TODO fix
            column_properties: None,
        })
    }
}

pub struct RbPostCommitHookProperties {
    create_checkpoint: bool,
    cleanup_expired_logs: Option<bool>,
}

impl TryConvert for RbPostCommitHookProperties {
    fn try_convert(val: Value) -> RbResult<Self> {
        Ok(RbPostCommitHookProperties {
            create_checkpoint: val.funcall("create_checkpoint", ())?,
            cleanup_expired_logs: val.funcall("cleanup_expired_logs", ())?,
        })
    }
}

#[magnus::wrap(class = "DeltaLake::Transaction")]
pub struct RbTransaction {
    pub app_id: String,
    pub version: i64,
    pub last_updated: Option<i64>,
}

impl From<Transaction> for RbTransaction {
    fn from(value: Transaction) -> Self {
        RbTransaction {
            app_id: value.app_id,
            version: value.version,
            last_updated: value.last_updated,
        }
    }
}

impl From<&RbTransaction> for Transaction {
    fn from(value: &RbTransaction) -> Self {
        Transaction {
            app_id: value.app_id.clone(),
            version: value.version,
            last_updated: value.last_updated,
        }
    }
}

pub struct RbCommitProperties {
    custom_metadata: Option<HashMap<String, String>>,
    max_commit_retries: Option<usize>,
    app_transactions: Option<Vec<RbTransaction>>,
}

impl TryConvert for RbCommitProperties {
    fn try_convert(val: Value) -> RbResult<Self> {
        Ok(RbCommitProperties {
            custom_metadata: val.funcall("custom_metadata", ())?,
            max_commit_retries: val.funcall("max_commit_retries", ())?,
            // TODO fix
            app_transactions: None,
        })
    }
}

#[allow(clippy::too_many_arguments)]
fn write_to_deltalake(
    table_uri: String,
    data: RbArrowType<ArrowArrayStreamReader>,
    mode: String,
    schema_mode: Option<String>,
    partition_by: Option<Vec<String>>,
    predicate: Option<String>,
    target_file_size: Option<usize>,
    name: Option<String>,
    description: Option<String>,
    configuration: Option<HashMap<String, Option<String>>>,
    storage_options: Option<HashMap<String, String>>,
    writer_properties: Option<RbWriterProperties>,
    commit_properties: Option<RbCommitProperties>,
    post_commithook_properties: Option<RbPostCommitHookProperties>,
) -> RbResult<()> {
    let batches = data.0.map(|batch| batch.unwrap()).collect::<Vec<_>>();
    let save_mode = mode.parse().map_err(RubyError::from)?;

    let options = storage_options.clone().unwrap_or_default();
    let table_url =
        deltalake::table::builder::ensure_table_uri(&table_uri).map_err(RubyError::from)?;
    let table = rt()
        .block_on(DeltaTable::try_from_url_with_storage_options(
            table_url.clone(),
            options.clone(),
        ))
        .map_err(RubyError::from)?;

    let mut builder = table.write(batches).with_save_mode(save_mode);
    if let Some(schema_mode) = schema_mode {
        builder = builder.with_schema_mode(schema_mode.parse().map_err(RubyError::from)?);
    }
    if let Some(partition_columns) = partition_by {
        builder = builder.with_partition_columns(partition_columns);
    }

    if let Some(writer_props) = writer_properties {
        builder = builder
            .with_writer_properties(set_writer_properties(writer_props).map_err(RubyError::from)?);
    }

    if let Some(name) = &name {
        builder = builder.with_table_name(name);
    };

    if let Some(description) = &description {
        builder = builder.with_description(description);
    };

    if let Some(predicate) = predicate {
        builder = builder.with_replace_where(predicate);
    };

    if let Some(target_file_size) = target_file_size {
        builder = builder.with_target_file_size(target_file_size)
    };

    if let Some(config) = configuration {
        builder = builder.with_configuration(config);
    };

    if let Some(commit_properties) =
        maybe_create_commit_properties(commit_properties, post_commithook_properties)
    {
        builder = builder.with_commit_properties(commit_properties);
    };

    rt().block_on(builder.into_future())
        .map_err(RubyError::from)?;

    Ok(())
}

pub struct RbArrowType<T>(pub T);

impl TryConvert for RbArrowType<ArrowArrayStreamReader> {
    fn try_convert(val: Value) -> RbResult<Self> {
        let addr: usize = val.funcall("to_i", ())?;

        // use similar approach as Polars to consume pointer and avoid copy
        let stream_ptr =
            Box::new(unsafe { std::ptr::replace(addr as _, FFI_ArrowArrayStream::empty()) });

        Ok(RbArrowType(
            ArrowArrayStreamReader::try_new(*stream_ptr)
                .map_err(|err| DeltaError::new_err(err.to_string()))?,
        ))
    }
}

#[magnus::wrap(class = "DeltaLake::ArrowArrayStream")]
pub struct ArrowArrayStream {
    stream: FFI_ArrowArrayStream,
}

impl ArrowArrayStream {
    pub fn to_i(&self) -> usize {
        (&self.stream as *const _) as usize
    }
}

#[magnus::init(name = "deltalake")]
fn init(ruby: &Ruby) -> RbResult<()> {
    deltalake::aws::register_handlers(None);
    deltalake::azure::register_handlers(None);
    deltalake::gcp::register_handlers(None);

    let module = ruby.define_module("DeltaLake")?;
    module.define_singleton_method("write_deltalake_rust", function!(write_to_deltalake, 14))?;
    module.define_singleton_method("rust_core_version", function!(rust_core_version, 0))?;

    let class = module.define_class("RawDeltaTable", ruby.class_object())?;
    class.define_singleton_method("new", function!(RawDeltaTable::new, 5))?;
    class.define_singleton_method("is_deltatable", function!(RawDeltaTable::is_deltatable, 2))?;
    class.define_method("table_uri", method!(RawDeltaTable::table_uri, 0))?;
    class.define_method("version", method!(RawDeltaTable::version, 0))?;
    class.define_method("has_files", method!(RawDeltaTable::has_files, 0))?;
    class.define_method("metadata", method!(RawDeltaTable::metadata, 0))?;
    class.define_method(
        "protocol_versions",
        method!(RawDeltaTable::protocol_versions, 0),
    )?;
    class.define_method("load_version", method!(RawDeltaTable::load_version, 1))?;
    class.define_method(
        "get_latest_version",
        method!(RawDeltaTable::get_latest_version, 0),
    )?;
    class.define_method(
        "get_num_index_cols",
        method!(RawDeltaTable::get_num_index_cols, 0),
    )?;
    class.define_method(
        "get_stats_columns",
        method!(RawDeltaTable::get_stats_columns, 0),
    )?;
    class.define_method(
        "load_with_datetime",
        method!(RawDeltaTable::load_with_datetime, 1),
    )?;
    class.define_method("files", method!(RawDeltaTable::files, 1))?;
    class.define_method("file_uris", method!(RawDeltaTable::file_uris, 1))?;
    class.define_method("schema", method!(RawDeltaTable::schema, 0))?;
    class.define_method("vacuum", method!(RawDeltaTable::vacuum, 5))?;
    class.define_method(
        "compact_optimize",
        method!(RawDeltaTable::compact_optimize, 7),
    )?;
    class.define_method(
        "z_order_optimize",
        method!(RawDeltaTable::z_order_optimize, 10),
    )?;
    class.define_method("add_columns", method!(RawDeltaTable::add_columns, 1))?;
    class.define_method("add_feature", method!(RawDeltaTable::add_feature, 2))?;
    class.define_method(
        "add_constraints",
        method!(RawDeltaTable::add_constraints, 1),
    )?;
    class.define_method(
        "drop_constraints",
        method!(RawDeltaTable::drop_constraints, 2),
    )?;
    class.define_method("load_cdf", method!(RawDeltaTable::load_cdf, 5))?;
    class.define_method(
        "create_merge_builder",
        method!(RawDeltaTable::create_merge_builder, 8),
    )?;
    class.define_method("merge_execute", method!(RawDeltaTable::merge_execute, 1))?;
    class.define_method("restore", method!(RawDeltaTable::restore, 4))?;
    class.define_method("history", method!(RawDeltaTable::history, 1))?;
    class.define_method(
        "update_incremental",
        method!(RawDeltaTable::update_incremental, 0),
    )?;
    class.define_method(
        "get_active_partitions",
        method!(RawDeltaTable::get_active_partitions, 0),
    )?;
    class.define_method(
        "create_checkpoint",
        method!(RawDeltaTable::create_checkpoint, 0),
    )?;
    class.define_method(
        "cleanup_metadata",
        method!(RawDeltaTable::cleanup_metadata, 0),
    )?;
    class.define_method(
        "get_add_file_sizes",
        method!(RawDeltaTable::get_add_file_sizes, 0),
    )?;
    class.define_method("delete", method!(RawDeltaTable::delete, 4))?;
    class.define_method(
        "set_table_properties",
        method!(RawDeltaTable::set_table_properties, 2),
    )?;
    class.define_method("repair", method!(RawDeltaTable::repair, 3))?;
    class.define_method(
        "transaction_version",
        method!(RawDeltaTable::transaction_version, 1),
    )?;
    class.define_method("write", method!(RawDeltaTable::write, 12))?;

    let class = module.define_class("RawDeltaTableMetaData", ruby.class_object())?;
    class.define_method("id", method!(RawDeltaTableMetaData::id, 0))?;
    class.define_method("name", method!(RawDeltaTableMetaData::name, 0))?;
    class.define_method(
        "description",
        method!(RawDeltaTableMetaData::description, 0),
    )?;
    class.define_method(
        "partition_columns",
        method!(RawDeltaTableMetaData::partition_columns, 0),
    )?;
    class.define_method(
        "created_time",
        method!(RawDeltaTableMetaData::created_time, 0),
    )?;
    class.define_method(
        "configuration",
        method!(RawDeltaTableMetaData::configuration, 0),
    )?;

    let class = module.define_class("ArrowArrayStream", ruby.class_object())?;
    class.define_method("to_i", method!(ArrowArrayStream::to_i, 0))?;

    let class = module.define_class("Field", ruby.class_object())?;
    class.define_singleton_method("new", function!(Field::new, 2))?;
    class.define_method("name", method!(Field::name, 0))?;
    class.define_method("type", method!(Field::get_type, 0))?;
    class.define_method("nullable", method!(Field::nullable, 0))?;

    let class = module.define_class("RbMergeBuilder", ruby.class_object())?;
    class.define_method("source_alias", method!(RbMergeBuilder::source_alias, 0))?;
    class.define_method("target_alias", method!(RbMergeBuilder::target_alias, 0))?;
    class.define_method(
        "when_matched_update",
        method!(RbMergeBuilder::when_matched_update, 2),
    )?;
    class.define_method(
        "when_matched_delete",
        method!(RbMergeBuilder::when_matched_delete, 1),
    )?;
    class.define_method(
        "when_not_matched_insert",
        method!(RbMergeBuilder::when_not_matched_insert, 2),
    )?;
    class.define_method(
        "when_not_matched_by_source_update",
        method!(RbMergeBuilder::when_not_matched_by_source_update, 2),
    )?;
    class.define_method(
        "when_not_matched_by_source_delete",
        method!(RbMergeBuilder::when_not_matched_by_source_delete, 1),
    )?;

    Ok(())
}
