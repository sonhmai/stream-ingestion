use std::collections::HashMap;
use std::sync::Arc;
use anyhow::Result;
use arrow::datatypes::{DataType, Field, Schema};
use tempfile::TempDir;

use ingest_core::ingestor::Ingestor;
use ingest_core::sink::delta::DeltaSink;
use ingest_core::source::memory::MemorySource;
use ingest_core::source::SourceMessage;
use futures::stream::TryStreamExt;

#[tokio::test]
async fn test_ingestor_run_and_shutdown() -> Result<()> {
    // setup
    let tmp_dir = TempDir::new().unwrap();
    let table_uri = tmp_dir.path().to_str().unwrap().to_string();

    let messages = vec![
        SourceMessage {
            payload: br#"{"id": 1, "name": "foo"}"#.to_vec(),
            topic: "test".to_string(),
            partition: 0,
            headers: HashMap::new(),
        },
        SourceMessage {
            payload: br#"{"id": 2, "name": "bar"}"#.to_vec(),
            topic: "test".to_string(),
            partition: 0,
            headers: HashMap::new(),
        },
    ];

    let source = Arc::new(MemorySource::new(messages, 10));
    let sink = Arc::new(DeltaSink::new(table_uri.clone()).unwrap());

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
    ]));

    // act
    let ingestor = Ingestor::new(source, sink, schema);
    ingestor.run_once().await.unwrap();
    ingestor.shutdown().await.unwrap();

    // verify
    let table = deltalake::open_table(&table_uri).await.unwrap();
    assert_eq!(table.version(), 0);
    let files: Vec<_> = table.get_file_uris()?.collect();
    println!("{files:?}");

    // snapshot currently held by table instance
    // let state_snapshot = table.snapshot()?;
    // let stream = state_snapshot.scan(None, &[], None).await.unwrap();
    // let batches: Vec<_> = stream.try_collect().await.unwrap();
    //
    // let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    // assert_eq!(total_rows, 2);
    Ok(())
}
