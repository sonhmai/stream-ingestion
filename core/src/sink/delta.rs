use std::sync::Arc;

use anyhow::Result;
use arrow::datatypes::Schema as ArrowSchema;
use crate::sink::Sink;
use crate::source::MessageBatch;

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
    async fn add(&self, message_batch: MessageBatch) -> Result<()> {
        todo!()
    }

    async fn flush(&self) -> Result<()> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
}