use arrow_schema::ArrowError;
use deltalake::protocol::ProtocolError;
use deltalake::{errors::DeltaTableError, ObjectStoreError};
use magnus::{exception, Error as RbErr, Module, RModule, Ruby};
use std::borrow::Cow;

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

fn inner_to_rb_err(err: DeltaTableError) -> RbErr {
    match err {
        DeltaTableError::NotATable(msg) => TableNotFoundError::new_err(msg),
        DeltaTableError::InvalidTableLocation(msg) => TableNotFoundError::new_err(msg),

        // protocol errors
        DeltaTableError::InvalidJsonLog { .. } => DeltaProtocolError::new_err(err.to_string()),
        DeltaTableError::InvalidStatsJson { .. } => DeltaProtocolError::new_err(err.to_string()),
        DeltaTableError::InvalidData { violations } => {
            DeltaProtocolError::new_err(format!("Invariant violations: {:?}", violations))
        }

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

fn checkpoint_to_rb(err: ProtocolError) -> RbErr {
    match err {
        ProtocolError::Arrow { source } => arrow_to_rb(source),
        ProtocolError::ObjectStore { source } => object_store_to_rb(source),
        ProtocolError::EndOfLog => DeltaProtocolError::new_err("End of log"),
        ProtocolError::NoMetaData => DeltaProtocolError::new_err("Table metadata missing"),
        ProtocolError::CheckpointNotFound => DeltaProtocolError::new_err(err.to_string()),
        ProtocolError::InvalidField(err) => RbValueError::new_err(err),
        ProtocolError::InvalidRow(err) => RbValueError::new_err(err),
        ProtocolError::InvalidDeletionVectorStorageType(err) => RbValueError::new_err(err),
        ProtocolError::SerializeOperation { source } => RbValueError::new_err(source.to_string()),
        ProtocolError::ParquetParseError { source } => RbIOError::new_err(source.to_string()),
        ProtocolError::IO { source } => RbIOError::new_err(source.to_string()),
        ProtocolError::Generic(msg) => DeltaError::new_err(msg),
        ProtocolError::Kernel { source } => DeltaError::new_err(source.to_string()),
    }
}

pub enum RubyError {
    DeltaTable(DeltaTableError),
    Protocol(ProtocolError),
}

impl From<DeltaTableError> for RubyError {
    fn from(err: DeltaTableError) -> Self {
        RubyError::DeltaTable(err)
    }
}

impl From<ProtocolError> for RubyError {
    fn from(err: ProtocolError) -> Self {
        RubyError::Protocol(err)
    }
}

impl From<RubyError> for RbErr {
    fn from(value: RubyError) -> Self {
        match value {
            RubyError::DeltaTable(err) => inner_to_rb_err(err),
            RubyError::Protocol(err) => checkpoint_to_rb(err),
        }
    }
}

macro_rules! create_builtin_exception {
    ($type:ident, $class:expr) => {
        pub struct $type {}

        impl $type {
            pub fn new_err<T>(message: T) -> RbErr
            where
                T: Into<Cow<'static, str>>,
            {
                RbErr::new($class, message)
            }
        }
    };
}

create_builtin_exception!(RbException, exception::runtime_error());
create_builtin_exception!(RbIOError, exception::io_error());
create_builtin_exception!(RbNotImplementedError, exception::not_imp_error());
create_builtin_exception!(RbValueError, exception::arg_error());
