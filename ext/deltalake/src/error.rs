use arrow_schema::ArrowError;
use deltalake::datafusion::error::DataFusionError;
use deltalake::{errors::DeltaTableError, ObjectStoreError};
use magnus::{Error as RbErr, Module, RModule, Ruby};
use std::borrow::Cow;

use crate::ruby::{RbException, RbIOError, RbNotImplementedError, RbRuntimeError, RbValueError};

macro_rules! create_exception {
    ($type:ident, $name:expr) => {
        pub struct $type {}

        impl $type {
            pub fn new_err<T>(message: T) -> RbErr
            where
                T: Into<Cow<'static, str>>,
            {
                let class = Ruby::get()
                    .unwrap()
                    .class_object()
                    .const_get::<_, RModule>("DeltaLake")
                    .unwrap()
                    .const_get($name)
                    .unwrap();
                RbErr::new(class, message)
            }
        }
    };
}

create_exception!(DeltaError, "Error");
create_exception!(TableNotFoundError, "TableNotFoundError");
create_exception!(DeltaProtocolError, "DeltaProtocolError");
create_exception!(CommitFailedError, "CommitFailedError");
create_exception!(SchemaMismatchError, "SchemaMismatchError");

pub(crate) fn to_rt_err(msg: impl ToString) -> RbErr {
    RbRuntimeError::new_err(msg.to_string())
}

pub(crate) fn to_rt_err2(msg: impl ToString) -> RubyError {
    RubyError::RuntimeError(msg.to_string())
}

fn inner_to_rb_err(err: DeltaTableError) -> RbErr {
    match err {
        DeltaTableError::NotATable(msg) => TableNotFoundError::new_err(msg),
        DeltaTableError::InvalidTableLocation(msg) => TableNotFoundError::new_err(msg),

        // protocol errors
        DeltaTableError::InvalidJsonLog { .. } => DeltaProtocolError::new_err(err.to_string()),
        DeltaTableError::InvalidStatsJson { .. } => DeltaProtocolError::new_err(err.to_string()),
        DeltaTableError::InvalidData { message } => DeltaProtocolError::new_err(message),

        // commit errors
        DeltaTableError::Transaction { source } => CommitFailedError::new_err(source.to_string()),

        // ruby exceptions
        DeltaTableError::ObjectStore { source } => object_store_to_rb(source),
        DeltaTableError::Io { source } => RbIOError::new_err(source.to_string()),

        DeltaTableError::Arrow { source } => arrow_to_rb(source),

        _ => DeltaError::new_err(err.to_string()),
    }
}

fn object_store_to_rb(err: ObjectStoreError) -> RbErr {
    match err {
        ObjectStoreError::NotFound { .. } => RbIOError::new_err(err.to_string()),
        ObjectStoreError::Generic { source, .. }
            if source.to_string().contains("AWS_S3_ALLOW_UNSAFE_RENAME") =>
        {
            DeltaProtocolError::new_err(source.to_string())
        }
        _ => RbIOError::new_err(err.to_string()),
    }
}

fn arrow_to_rb(err: ArrowError) -> RbErr {
    match err {
        ArrowError::IoError(msg, _) => RbIOError::new_err(msg),
        ArrowError::DivideByZero => RbValueError::new_err("division by zero"),
        ArrowError::InvalidArgumentError(msg) => RbValueError::new_err(msg),
        ArrowError::NotYetImplemented(msg) => RbNotImplementedError::new_err(msg),
        ArrowError::SchemaError(msg) => SchemaMismatchError::new_err(msg),
        other => RbException::new_err(other.to_string()),
    }
}

fn datafusion_to_rb(err: DataFusionError) -> RbErr {
    DeltaError::new_err(err.to_string())
}

pub enum RubyError {
    DeltaTable(DeltaTableError),
    DataFusion(DataFusionError),
    ThreadingError(String),
    // Ruby-specific errors for when GVL released
    RuntimeError(String),
    ValueError(&'static str),
}

impl From<DeltaTableError> for RubyError {
    fn from(err: DeltaTableError) -> Self {
        RubyError::DeltaTable(err)
    }
}

impl From<DataFusionError> for RubyError {
    fn from(err: DataFusionError) -> Self {
        RubyError::DataFusion(err)
    }
}

impl From<RubyError> for RbErr {
    fn from(value: RubyError) -> Self {
        match value {
            RubyError::DeltaTable(err) => inner_to_rb_err(err),
            RubyError::DataFusion(err) => datafusion_to_rb(err),
            RubyError::ThreadingError(err) => RbRuntimeError::new_err(err),
            RubyError::RuntimeError(err) => RbValueError::new_err(err),
            RubyError::ValueError(err) => RbValueError::new_err(err),
        }
    }
}
