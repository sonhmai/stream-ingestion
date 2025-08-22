use std::sync::Arc;

use anyhow::Result;
use arrow::datatypes::{Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::sink::Sink;

/// Delta Lake sink buffers messages and clears them on flush.
pub struct DeltaSink {
    table_uri: String,
    schema: Arc<ArrowSchema>,
    buffer: Mutex<Vec<RecordBatch>>, // buffered micro-batches waiting to be flushed
}

impl DeltaSink {
    pub fn new(table_uri: String) -> Result<Self> {
        Ok(Self {
            table_uri,
            schema: Arc::new(ArrowSchema::new(Vec::<Field>::new())),
            buffer: Mutex::new(Vec::new()),
        })
    }
}

#[async_trait]
impl Sink for DeltaSink {
    async fn add(&self, record_batch: RecordBatch) -> Result<()> {
        let mut guard = self.buffer.lock().await;
        guard.push(record_batch);
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        use deltalake::DeltaOps;

        let batches: Vec<RecordBatch> = {
            let guard = self.buffer.lock().await;
            if guard.is_empty() {
                return Ok(());
            }
            guard.clone()
        };

        // Write using DeltaOps; this will create the table if missing.
        let _table = DeltaOps::try_from_uri(self.table_uri.as_str())
            .await?
            .write(batches)
            .await?;

        let mut guard = self.buffer.lock().await;
        guard.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_delta_sink_add_flush_and_verify_table() -> Result<()> {
        let tmpdir = TempDir::new()?;
        let table_uri = tmpdir.path().to_str().unwrap();
        let sink = DeltaSink::new(table_uri.into())?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let ids = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let names = Arc::new(StringArray::from(vec!["a", "b", "c"]));
        let batch = RecordBatch::try_new(schema, vec![ids, names]).expect("batch");

        sink.add(batch).await?;
        assert_eq!(sink.buffer.lock().await.len(), 1);

        sink.flush().await?;
        assert_eq!(sink.buffer.lock().await.len(), 0);

        // Verify we can open the Delta table and a snapshot exists
        let table = deltalake::open_table(table_uri).await?;
        let snapshot = table.snapshot().expect("snapshot available");
        assert!(snapshot.version() >= 0);

        Ok(())
    }
}