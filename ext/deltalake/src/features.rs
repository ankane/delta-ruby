use crate::{RbResult, RbValueError};
use deltalake::kernel::TableFeatures as KernelTableFeatures;
use magnus::{TryConvert, Value, prelude::*};

/// High level table features
#[derive(Clone)]
pub enum TableFeatures {
    /// Mapping of one column to another
    ColumnMapping,
    /// Deletion vectors for merge, update, delete
    DeletionVectors,
    /// timestamps without timezone support
    TimestampWithoutTimezone,
    /// version 2 of checkpointing
    V2Checkpoint,
    /// Append Only Tables
    AppendOnly,
    /// Table invariants
    Invariants,
    /// Check constraints on columns
    CheckConstraints,
    /// CDF on a table
    ChangeDataFeed,
    /// Columns with generated values
    GeneratedColumns,
    /// ID Columns
    IdentityColumns,
    /// Row tracking on tables
    RowTracking,
    /// domain specific metadata
    DomainMetadata,
    /// Iceberg compatibility support
    IcebergCompatV1,
}

impl From<TableFeatures> for KernelTableFeatures {
    fn from(value: TableFeatures) -> Self {
        match value {
            TableFeatures::ColumnMapping => KernelTableFeatures::ColumnMapping,
            TableFeatures::DeletionVectors => KernelTableFeatures::DeletionVectors,
            TableFeatures::TimestampWithoutTimezone => {
                KernelTableFeatures::TimestampWithoutTimezone
            }
            TableFeatures::V2Checkpoint => KernelTableFeatures::V2Checkpoint,
            TableFeatures::AppendOnly => KernelTableFeatures::AppendOnly,
            TableFeatures::Invariants => KernelTableFeatures::Invariants,
            TableFeatures::CheckConstraints => KernelTableFeatures::CheckConstraints,
            TableFeatures::ChangeDataFeed => KernelTableFeatures::ChangeDataFeed,
            TableFeatures::GeneratedColumns => KernelTableFeatures::GeneratedColumns,
            TableFeatures::IdentityColumns => KernelTableFeatures::IdentityColumns,
            TableFeatures::RowTracking => KernelTableFeatures::RowTracking,
            TableFeatures::DomainMetadata => KernelTableFeatures::DomainMetadata,
            TableFeatures::IcebergCompatV1 => KernelTableFeatures::IcebergCompatV1,
        }
    }
}

impl TryConvert for TableFeatures {
    fn try_convert(val: Value) -> RbResult<Self> {
        // TODO add more features
        let s = val.to_r_string()?;
        let feature = match unsafe { s.as_str()? } {
            "append_only" => TableFeatures::AppendOnly,
            _ => return Err(RbValueError::new_err("Invalid feature")),
        };
        Ok(feature)
    }
}
