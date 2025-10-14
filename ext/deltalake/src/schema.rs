use deltalake::kernel::{
    DataType, PrimitiveType as DeltaPrimitive, StructField, StructType as DeltaStructType,
};
use magnus::{value::ReprValue, Module, RModule, Ruby, TryConvert, Value};
use std::sync::Arc;

use crate::{RbResult, RbValueError};

pub fn schema_to_rbobject(schema: Arc<DeltaStructType>, ruby: &Ruby) -> RbResult<Value> {
    let fields = schema.fields().map(|field| Field {
        inner: field.clone(),
    });

    let rb_schema: Value = ruby
        .class_object()
        .const_get::<_, RModule>("DeltaLake")?
        .const_get("Schema")?;

    rb_schema.funcall("new", (ruby.ary_from_iter(fields),))
}

fn ruby_type_to_schema(ob: Value) -> RbResult<DataType> {
    if let Ok(raw_primitive) = String::try_convert(ob) {
        // Pass through PrimitiveType::new() to do validation
        return PrimitiveType::new(raw_primitive)
            .map(|data_type| DataType::Primitive(data_type.inner_type));
    }
    Err(RbValueError::new_err("Invalid data type"))
}

pub struct PrimitiveType {
    inner_type: DeltaPrimitive,
}

impl PrimitiveType {
    fn new(data_type: String) -> RbResult<Self> {
        let data_type: DeltaPrimitive =
            serde_json::from_str(&format!("\"{data_type}\"")).map_err(|_| {
                if data_type.starts_with("decimal") {
                    RbValueError::new_err(format!(
                        "invalid type string: {data_type}, precision/scale can't be larger than 38"
                    ))
                } else {
                    RbValueError::new_err(format!("invalid type string: {data_type}"))
                }
            })?;

        Ok(Self {
            inner_type: data_type,
        })
    }
}

#[magnus::wrap(class = "DeltaLake::Field")]
pub struct Field {
    pub inner: StructField,
}

impl Field {
    pub fn new(name: String, r#type: Value) -> RbResult<Self> {
        let ty = ruby_type_to_schema(r#type)?;
        Ok(Self {
            inner: StructField::new(name, ty, true),
        })
    }

    pub fn name(&self) -> String {
        self.inner.name().to_string()
    }

    pub fn get_type(&self) -> String {
        self.inner.data_type().to_string()
    }

    pub fn nullable(&self) -> bool {
        self.inner.is_nullable()
    }
}
