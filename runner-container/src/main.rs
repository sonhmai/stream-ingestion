use anyhow::{Context, Result};
use std::time::Duration;
use ingest_core::config::IngestConfig;
use ingest_core::errors::SourceError;
use ingest_core::ingestor::Ingestor;

/// This is the entry point for running a long-running ingestion job on container
/// hosting services like AWS EKS, ECS, etc.
///
/// What it does at a high-level:
///     Load config.
///     Main loop runs forever until a shutdown signal is received:
///         - consume from kafka topic partition(s)
///         - write to delta file(s)
#[tokio::main]
async fn main() -> Result<()> {
    // this is only sample code
    let config = IngestConfig::from_env()?;
    let engine = Ingestor::new(config).await?; // Assuming `new` can't fail with SourceError

    loop {
        match engine.run_once().await.context("Ingestion cycle failed") {
            Ok(_) => {
                // todo: logging, telemetry, etc.
            }
            Err(e) => {
                if let Some(source_err) = e.downcast_ref::<SourceError>() {
                    match source_err {
                        SourceError::Transient { .. } => {
                            eprintln!("A transient error occurred, will retry: {:?}", e);
                            tokio::time::sleep(Duration::from_secs(5)).await;
                            continue; // Retry the loop
                        }
                        _ => {
                            // fatal error
                            eprintln!("A fatal error occurred: {:?}", e);
                            return Err(e); // Propagate anyhow::Error
                        }
                    }
                } else {
                    return Err(e); // other unexpected errors
                }
            }
        }
    }
}
