use deltalake::arrow::array::RecordBatchReader;
use deltalake::arrow::datatypes::Schema as ArrowSchema;
use deltalake::arrow::ffi_stream::ArrowArrayStreamReader;
use deltalake::datafusion::catalog::TableProvider;
use deltalake::datafusion::datasource::MemTable;
use deltalake::datafusion::prelude::SessionContext;
use deltalake::kernel::EagerSnapshot;
use deltalake::logstore::LogStoreRef;
use deltalake::operations::merge::MergeBuilder;
use deltalake::{DeltaResult, DeltaTable};
use std::cell::RefCell;
use std::collections::HashMap;
use std::future::IntoFuture;
use std::sync::Arc;

use crate::error::RubyError;
use crate::utils::rt;
use crate::RbResult;
use crate::{
    maybe_create_commit_properties, set_writer_properties, RbCommitProperties,
    RbPostCommitHookProperties, RbWriterProperties,
};

#[magnus::wrap(class = "DeltaLake::RbMergeBuilder")]
pub(crate) struct RbMergeBuilder {
    _builder: RefCell<Option<MergeBuilder>>,
    source_alias: Option<String>,
    target_alias: Option<String>,
    #[allow(dead_code)]
    arrow_schema: Arc<ArrowSchema>,
}

// getters
impl RbMergeBuilder {
    pub fn source_alias(&self) -> Option<String> {
        self.source_alias.clone()
    }

    pub fn target_alias(&self) -> Option<String> {
        self.target_alias.clone()
    }
}

impl RbMergeBuilder {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        log_store: LogStoreRef,
        snapshot: EagerSnapshot,
        source: ArrowArrayStreamReader,
        predicate: String,
        source_alias: Option<String>,
        target_alias: Option<String>,
        safe_cast: bool,
        writer_properties: Option<RbWriterProperties>,
        post_commithook_properties: Option<RbPostCommitHookProperties>,
        commit_properties: Option<RbCommitProperties>,
    ) -> DeltaResult<Self> {
        let ctx = SessionContext::new();
        let schema = source.schema();
        let batches = vec![source.map(|batch| batch.unwrap()).collect::<Vec<_>>()];
        let table_provider: Arc<dyn TableProvider> =
            Arc::new(MemTable::try_new(schema.clone(), batches).unwrap());
        let source_df = ctx.read_table(table_provider).unwrap();

        let mut cmd =
            MergeBuilder::new(log_store, snapshot, predicate, source_df).with_safe_cast(safe_cast);

        if let Some(src_alias) = &source_alias {
            cmd = cmd.with_source_alias(src_alias);
        }

        if let Some(trgt_alias) = &target_alias {
            cmd = cmd.with_target_alias(trgt_alias);
        }

        if let Some(writer_props) = writer_properties {
            cmd = cmd.with_writer_properties(set_writer_properties(writer_props)?);
        }

        if let Some(commit_properties) =
            maybe_create_commit_properties(commit_properties, post_commithook_properties)
        {
            cmd = cmd.with_commit_properties(commit_properties);
        }

        Ok(Self {
            _builder: RefCell::new(Some(cmd)),
            source_alias,
            target_alias,
            arrow_schema: schema,
        })
    }

    pub fn execute(&self) -> DeltaResult<(DeltaTable, String)> {
        let (table, metrics) = rt().block_on(self._builder.take().unwrap().into_future())?;
        Ok((table, serde_json::to_string(&metrics).unwrap()))
    }
}

impl RbMergeBuilder {
    pub fn when_matched_update(
        &self,
        updates: HashMap<String, String>,
        predicate: Option<String>,
    ) -> RbResult<()> {
        let mut binding = self._builder.borrow_mut();
        *binding = match binding.take() {
            Some(cmd) => Some(
                cmd.when_matched_update(|mut update| {
                    for (column, expression) in updates {
                        update = update.update(column, expression)
                    }
                    if let Some(predicate) = predicate {
                        update = update.predicate(predicate)
                    };
                    update
                })
                .map_err(RubyError::from)?,
            ),
            None => unreachable!(),
        };
        Ok(())
    }

    pub fn when_matched_delete(&self, predicate: Option<String>) -> RbResult<()> {
        let mut binding = self._builder.borrow_mut();
        *binding = match binding.take() {
            Some(cmd) => Some(
                cmd.when_matched_delete(|mut delete| {
                    if let Some(predicate) = predicate {
                        delete = delete.predicate(predicate)
                    };
                    delete
                })
                .map_err(RubyError::from)?,
            ),
            None => unreachable!(),
        };
        Ok(())
    }

    pub fn when_not_matched_insert(
        &self,
        updates: HashMap<String, String>,
        predicate: Option<String>,
    ) -> RbResult<()> {
        let mut binding = self._builder.borrow_mut();
        *binding = match binding.take() {
            Some(cmd) => Some(
                cmd.when_not_matched_insert(|mut insert| {
                    for (column, expression) in updates {
                        insert = insert.set(column, expression)
                    }
                    if let Some(predicate) = predicate {
                        insert = insert.predicate(predicate)
                    };
                    insert
                })
                .map_err(RubyError::from)?,
            ),
            None => unreachable!(),
        };
        Ok(())
    }

    pub fn when_not_matched_by_source_update(
        &self,
        updates: HashMap<String, String>,
        predicate: Option<String>,
    ) -> RbResult<()> {
        let mut binding = self._builder.borrow_mut();
        *binding = match binding.take() {
            Some(cmd) => Some(
                cmd.when_not_matched_by_source_update(|mut update| {
                    for (column, expression) in updates {
                        update = update.update(column, expression)
                    }
                    if let Some(predicate) = predicate {
                        update = update.predicate(predicate)
                    };
                    update
                })
                .map_err(RubyError::from)?,
            ),
            None => unreachable!(),
        };
        Ok(())
    }

    pub fn when_not_matched_by_source_delete(&self, predicate: Option<String>) -> RbResult<()> {
        let mut binding = self._builder.borrow_mut();
        *binding = match binding.take() {
            Some(cmd) => Some(
                cmd.when_not_matched_by_source_delete(|mut delete| {
                    if let Some(predicate) = predicate {
                        delete = delete.predicate(predicate)
                    };
                    delete
                })
                .map_err(RubyError::from)?,
            ),
            None => unreachable!(),
        };
        Ok(())
    }
}
