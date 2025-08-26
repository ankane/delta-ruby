use deltalake::kernel::{StructField, StructType as DeltaStructType};
use magnus::{value::ReprValue, Module, RModule, Ruby, Value};

use crate::RbResult;

pub fn schema_to_rbobject(schema: DeltaStructType) -> RbResult<Value> {
    let ruby = Ruby::get().unwrap();

    let fields = schema.fields().map(|field| Field {
        inner: field.clone(),
    });

    let rb_schema: Value = ruby
        .class_object()
        .const_get::<_, RModule>("DeltaLake")?
        .const_get("Schema")?;

    rb_schema.funcall("new", (ruby.ary_from_iter(fields),))
}

#[magnus::wrap(class = "DeltaLake::Field")]
pub struct Field {
    pub inner: StructField,
}

impl Field {
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
