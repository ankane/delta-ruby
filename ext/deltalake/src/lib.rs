mod error;
mod schema;
mod utils;

use std::cell::RefCell;
use std::collections::HashMap;
use std::future::IntoFuture;

use chrono::Duration;
use deltalake::arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use deltalake::kernel::StructType;
use deltalake::operations::delete::DeleteBuilder;
use deltalake::operations::vacuum::VacuumBuilder;
use deltalake::storage::IORuntime;
use deltalake::DeltaOps;
use error::DeltaError;

use magnus::{function, method, prelude::*, Error, Module, Ruby, Value};

use crate::error::RubyError;
use crate::schema::{schema_to_rbobject, Field};
use crate::utils::rt;

type RbResult<T> = Result<T, Error>;
type StringVec = Vec<String>;

#[magnus::wrap(class = "DeltaLake::RawDeltaTable")]
struct RawDeltaTable {
    _table: RefCell<deltalake::DeltaTable>,
}

#[magnus::wrap(class = "DeltaLake::RawDeltaTableMetaData")]
struct RawDeltaTableMetaData {
    id: String,
    name: Option<String>,
    description: Option<String>,
    partition_columns: Vec<String>,
    created_time: Option<i64>,
    configuration: HashMap<String, Option<String>>,
}

impl RawDeltaTable {
    pub fn new(
        table_uri: String,
        version: Option<i64>,
        storage_options: Option<HashMap<String, String>>,
        without_files: bool,
        log_buffer_size: Option<usize>,
    ) -> RbResult<Self> {
        let mut builder = deltalake::DeltaTableBuilder::from_uri(&table_uri)
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
            _table: RefCell::new(table),
        })
    }

    pub fn is_deltatable(
        table_uri: String,
        storage_options: Option<HashMap<String, String>>,
    ) -> RbResult<bool> {
        let mut builder = deltalake::DeltaTableBuilder::from_uri(&table_uri);
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
        Ok(self._table.borrow().table_uri())
    }

    pub fn version(&self) -> RbResult<i64> {
        Ok(self._table.borrow().version())
    }

    pub fn has_files(&self) -> RbResult<bool> {
        Ok(self._table.borrow().config.require_files)
    }

    pub fn metadata(&self) -> RbResult<RawDeltaTableMetaData> {
        let binding = self._table.borrow();
        let metadata = binding.metadata().map_err(RubyError::from)?;
        Ok(RawDeltaTableMetaData {
            id: metadata.id.clone(),
            name: metadata.name.clone(),
            description: metadata.description.clone(),
            partition_columns: metadata.partition_columns.clone(),
            created_time: metadata.created_time,
            configuration: metadata.configuration.clone(),
        })
    }

    pub fn protocol_versions(&self) -> RbResult<(i32, i32, Option<StringVec>, Option<StringVec>)> {
        let binding = self._table.borrow();
        let table_protocol = binding.protocol().map_err(RubyError::from)?;
        Ok((
            table_protocol.min_reader_version,
            table_protocol.min_writer_version,
            table_protocol
                .writer_features
                .as_ref()
                .and_then(|features| {
                    let empty_set = !features.is_empty();
                    empty_set.then(|| {
                        features
                            .iter()
                            .map(|v| v.to_string())
                            .collect::<Vec<String>>()
                    })
                }),
            table_protocol
                .reader_features
                .as_ref()
                .and_then(|features| {
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
        Ok(rt()
            .block_on(self._table.borrow_mut().load_version(version))
            .map_err(RubyError::from)?)
    }

    pub fn files(&self) -> RbResult<Vec<String>> {
        if !self.has_files()? {
            return Err(DeltaError::new_err("Table is instantiated without files."));
        }

        Ok(self
            ._table
            .borrow()
            .get_files_iter()
            .map_err(RubyError::from)?
            .map(|f| f.to_string())
            .collect())
    }

    pub fn file_uris(&self) -> RbResult<Vec<String>> {
        if !self._table.borrow().config.require_files {
            return Err(DeltaError::new_err("Table is initiated without files."));
        }

        Ok(self
            ._table
            .borrow()
            .get_file_uris()
            .map_err(RubyError::from)?
            .collect())
    }

    pub fn schema(&self) -> RbResult<Value> {
        let binding = self._table.borrow();
        let schema: &StructType = binding.get_schema().map_err(RubyError::from)?;
        schema_to_rbobject(schema.to_owned())
    }

    pub fn vacuum(
        &self,
        dry_run: bool,
        retention_hours: Option<u64>,
        enforce_retention_duration: bool,
    ) -> RbResult<Vec<String>> {
        let mut cmd = VacuumBuilder::new(
            self._table.borrow().log_store(),
            self._table
                .borrow()
                .snapshot()
                .map_err(RubyError::from)?
                .clone(),
        )
        .with_enforce_retention_duration(enforce_retention_duration)
        .with_dry_run(dry_run);
        if let Some(retention_period) = retention_hours {
            cmd = cmd.with_retention_period(Duration::hours(retention_period as i64));
        }

        let (table, metrics) = rt().block_on(cmd.into_future()).map_err(RubyError::from)?;
        self._table.borrow_mut().state = table.state;
        Ok(metrics.files_deleted)
    }

    pub fn update_incremental(&self) -> RbResult<()> {
        #[allow(deprecated)]
        Ok(rt()
            .block_on(self._table.borrow_mut().update_incremental(None))
            .map_err(RubyError::from)?)
    }

    pub fn delete(&self, predicate: Option<String>) -> RbResult<String> {
        let mut cmd = DeleteBuilder::new(
            self._table.borrow().log_store(),
            self._table
                .borrow()
                .snapshot()
                .map_err(RubyError::from)?
                .clone(),
        );
        if let Some(predicate) = predicate {
            cmd = cmd.with_predicate(predicate);
        }

        let (table, metrics) = rt().block_on(cmd.into_future()).map_err(RubyError::from)?;
        self._table.borrow_mut().state = table.state;
        Ok(serde_json::to_string(&metrics).unwrap())
    }
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

    fn configuration(&self) -> HashMap<String, Option<String>> {
        self.configuration.clone()
    }
}

#[allow(clippy::too_many_arguments)]
fn write_to_deltalake(
    table_uri: String,
    data: Value,
    mode: String,
    table: Option<&RawDeltaTable>,
    schema_mode: Option<String>,
    partition_by: Option<Vec<String>>,
    predicate: Option<String>,
    target_file_size: Option<usize>,
    name: Option<String>,
    description: Option<String>,
    configuration: Option<HashMap<String, Option<String>>>,
    storage_options: Option<HashMap<String, String>>,
) -> RbResult<()> {
    let capsule_pointer: usize = data.funcall("to_i", ())?;

    // use similar approach as Polars to avoid copy
    let stream_ptr =
        Box::new(unsafe { std::ptr::replace(capsule_pointer as _, FFI_ArrowArrayStream::empty()) });
    let stream = ArrowArrayStreamReader::try_new(*stream_ptr)
        .map_err(|err| DeltaError::new_err(err.to_string()))?;

    let batches = stream.map(|batch| batch.unwrap()).collect::<Vec<_>>();
    let save_mode = mode.parse().map_err(RubyError::from)?;

    let options = storage_options.clone().unwrap_or_default();
    let table = if let Some(table) = table {
        DeltaOps(table._table.borrow().clone())
    } else {
        rt().block_on(DeltaOps::try_from_uri_with_storage_options(
            &table_uri, options,
        ))
        .map_err(RubyError::from)?
    };

    let mut builder = table.write(batches).with_save_mode(save_mode);
    if let Some(schema_mode) = schema_mode {
        builder = builder.with_schema_mode(schema_mode.parse().map_err(RubyError::from)?);
    }
    if let Some(partition_columns) = partition_by {
        builder = builder.with_partition_columns(partition_columns);
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

    rt().block_on(builder.into_future())
        .map_err(RubyError::from)?;

    Ok(())
}

#[magnus::init]
fn init(ruby: &Ruby) -> RbResult<()> {
    deltalake::aws::register_handlers(None);

    let module = ruby.define_module("DeltaLake")?;
    module.define_singleton_method("write_deltalake_rust", function!(write_to_deltalake, 12))?;

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
    class.define_method("files", method!(RawDeltaTable::files, 0))?;
    class.define_method("file_uris", method!(RawDeltaTable::file_uris, 0))?;
    class.define_method("schema", method!(RawDeltaTable::schema, 0))?;
    class.define_method("vacuum", method!(RawDeltaTable::vacuum, 3))?;
    class.define_method(
        "update_incremental",
        method!(RawDeltaTable::update_incremental, 0),
    )?;
    class.define_method("delete", method!(RawDeltaTable::delete, 1))?;

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

    let class = module.define_class("Field", ruby.class_object())?;
    class.define_method("name", method!(Field::name, 0))?;
    class.define_method("type", method!(Field::get_type, 0))?;
    class.define_method("nullable", method!(Field::nullable, 0))?;

    Ok(())
}
