use std::sync::Arc;

use anyhow::Result;
use arrow::datatypes::Schema as ArrowSchema;
use crate::sink::Sink;

pub struct DeltaSink {
    table_uri: String,
    schema: Arc<ArrowSchema>,
}

impl DeltaSink {
    pub fn new(table_uri: String) -> Result<Self> {
        todo!()
    }
}

impl Sink for DeltaSink {

}

#[cfg(test)]
mod tests {
}