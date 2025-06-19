mod config;
mod kafka;
// mod delta;  // Temporarily disabled for testing
mod s3;
mod errors;

use anyhow::Result;
use lambda_runtime::{service_fn, Error, LambdaEvent};
use serde::{Deserialize, Serialize};
use std::env;
use std::time::{Duration, Instant};
use tokio::time::timeout;
use tracing::{error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::config::IngestConfig;
// use crate::delta::DeltaWriter;  // Temporarily disabled for testing
use crate::errors::{Result as StreamResult, StreamIngestError};
use crate::kafka::KafkaConsumerClient;
use crate::s3::S3Client;

#[derive(Debug, Deserialize)]
struct LambdaRequest {
    #[serde(default)]
    pub config_path: Option<String>,
    #[serde(default)]
    pub max_runtime_seconds: Option<u64>,
    #[serde(default)]
    pub dry_run: Option<bool>,
}

#[derive(Debug, Serialize)]
struct LambdaResponse {
    pub status: String,
    pub messages_processed: usize,
    pub batches_processed: usize,
    pub runtime_seconds: f64,
    pub errors: Vec<String>,
    pub metrics: IngestMetrics,
}

#[derive(Debug, Default, Serialize)]
struct IngestMetrics {
    pub total_messages: usize,
    pub successful_messages: usize,
    pub failed_messages: usize,
    pub bytes_processed: u64,
    pub avg_batch_size: f64,
    pub delta_writes: usize,
    pub kafka_commits: usize,
}

struct StreamIngestProcessor {
    config: IngestConfig,
    kafka_client: KafkaConsumerClient,
    // delta_writer: DeltaWriter,  // Temporarily disabled for testing
    s3_client: S3Client,
    metrics: IngestMetrics,
}

impl StreamIngestProcessor {
    pub async fn new(config: IngestConfig) -> StreamResult<Self> {
        config.validate()
            .map_err(|e| StreamIngestError::Config(crate::errors::ConfigError::ValidationFailed { 
                reason: e.to_string() 
            }))?;

        let kafka_client = KafkaConsumerClient::new(&config)
            .map_err(|e| StreamIngestError::Kafka(crate::errors::KafkaError::ConsumerCreation { 
                reason: e.to_string() 
            }))?;

        let s3_client = S3Client::new(config.s3.clone()).await
            .map_err(|e| StreamIngestError::S3(crate::errors::S3Error::ClientCreation { 
                reason: e.to_string() 
            }))?;

        // let table_uri = s3_client.get_table_uri(&config.delta.table_name);
        // let delta_writer = DeltaWriter::new(config.delta.clone(), table_uri)
        //     .map_err(|e| StreamIngestError::Delta(crate::errors::DeltaError::TableCreation { 
        //         reason: e.to_string() 
        //     }))?;

        Ok(Self {
            config,
            kafka_client,
            // delta_writer,  // Temporarily disabled for testing
            s3_client,
            metrics: IngestMetrics::default(),
        })
    }

    pub async fn run(&mut self, max_runtime: Duration, dry_run: bool) -> StreamResult<()> {
        let start_time = Instant::now();
        
        info!("Starting stream ingestion process");
        info!("Max runtime: {:?}, Dry run: {}", max_runtime, dry_run);

        self.kafka_client.subscribe().await
            .map_err(|e| StreamIngestError::Kafka(crate::errors::KafkaError::Subscription { 
                topic: self.config.kafka.topic.clone(),
                reason: e.to_string() 
            }))?;

        if !dry_run {
            self.s3_client.ensure_bucket_exists().await
                .map_err(|e| StreamIngestError::S3(crate::errors::S3Error::BucketAccess { 
                    bucket: self.config.s3.bucket.clone(),
                    reason: e.to_string() 
                }))?;

            // self.delta_writer.ensure_table_exists().await
            //     .map_err(|e| StreamIngestError::Delta(crate::errors::DeltaError::TableCreation { 
            //         reason: e.to_string() 
            //     }))?;
        }

        let mut retry_count = 0;
        let max_retries = self.config.processing.max_retries;

        while start_time.elapsed() < max_runtime {
            match self.process_batch(dry_run).await {
                Ok(processed_count) => {
                    if processed_count == 0 {
                        info!("No messages available, waiting...");
                        tokio::time::sleep(Duration::from_millis(1000)).await;
                        continue;
                    }
                    retry_count = 0;
                }
                Err(e) => {
                    error!("Batch processing failed: {}", e);
                    
                    if e.is_retryable() && retry_count < max_retries {
                        retry_count += 1;
                        let delay = Duration::from_millis(
                            e.get_retry_delay_ms() * retry_count as u64
                        );
                        warn!("Retrying in {:?} (attempt {}/{})", delay, retry_count, max_retries);
                        tokio::time::sleep(delay).await;
                        continue;
                    } else {
                        error!("Max retries exceeded or non-retryable error");
                        return Err(e);
                    }
                }
            }

            if let Some(checkpoint_interval) = self.config.processing.checkpoint_interval_ms {
                if start_time.elapsed().as_millis() as u64 % checkpoint_interval == 0 {
                    info!("Checkpoint - Metrics: {:?}", self.metrics);
                }
            }
        }

        info!("Stream ingestion completed. Final metrics: {:?}", self.metrics);
        Ok(())
    }

    async fn process_batch(&mut self, dry_run: bool) -> StreamResult<usize> {
        let messages = self.kafka_client
            .consume_batch(
                self.config.processing.batch_size,
                self.config.processing.batch_timeout_ms,
            )
            .await
            .map_err(|e| StreamIngestError::Kafka(crate::errors::KafkaError::Consumption { 
                reason: e.to_string() 
            }))?;

        if messages.is_empty() {
            return Ok(0);
        }

        self.metrics.total_messages += messages.len();
        let batch_size = messages.len();

        if dry_run {
            info!("DRY RUN: Would process {} messages", batch_size);
            self.metrics.successful_messages += batch_size;
            return Ok(batch_size);
        }

        // Temporarily disabled delta writing for testing
        // match self.delta_writer.write_batch(messages.clone()).await {
        //     Ok(_) => {
                self.metrics.successful_messages += batch_size;
                self.metrics.delta_writes += 1;
                
                self.kafka_client.commit_offsets(&messages).await
                    .map_err(|e| StreamIngestError::Kafka(crate::errors::KafkaError::OffsetCommit { 
                        reason: e.to_string() 
                    }))?;
                
                self.metrics.kafka_commits += 1;
                info!("Successfully processed batch of {} messages", batch_size);
        //     }
        //     Err(e) => {
        //         error!("Failed to write batch to Delta table: {}", e);
        //         self.metrics.failed_messages += batch_size;
        //         return Err(StreamIngestError::Delta(crate::errors::DeltaError::Write { 
        //             reason: e.to_string() 
        //         }));
        //     }
        // }

        self.metrics.batches_processed += 1;
        self.metrics.avg_batch_size = 
            self.metrics.total_messages as f64 / self.metrics.batches_processed as f64;

        Ok(batch_size)
    }

    pub fn get_metrics(&self) -> &IngestMetrics {
        &self.metrics
    }
}

async fn lambda_handler(event: LambdaEvent<LambdaRequest>) -> Result<LambdaResponse, Error> {
    let start_time = Instant::now();
    let mut errors = Vec::new();
    let request = event.payload;

    let config = match load_config(request.config_path.as_deref()).await {
        Ok(config) => config,
        Err(e) => {
            let error_msg = format!("Failed to load configuration: {}", e);
            error!("{}", error_msg);
            return Ok(LambdaResponse {
                status: "error".to_string(),
                messages_processed: 0,
                batches_processed: 0,
                runtime_seconds: start_time.elapsed().as_secs_f64(),
                errors: vec![error_msg],
                metrics: IngestMetrics::default(),
            });
        }
    };

    let max_runtime = Duration::from_secs(request.max_runtime_seconds.unwrap_or(840)); // 14 minutes default
    let dry_run = request.dry_run.unwrap_or(false);

    let mut processor = match StreamIngestProcessor::new(config).await {
        Ok(processor) => processor,
        Err(e) => {
            let error_msg = format!("Failed to initialize processor: {}", e);
            error!("{}", error_msg);
            return Ok(LambdaResponse {
                status: "error".to_string(),
                messages_processed: 0,
                batches_processed: 0,
                runtime_seconds: start_time.elapsed().as_secs_f64(),
                errors: vec![error_msg],
                metrics: IngestMetrics::default(),
            });
        }
    };

    let processing_result = timeout(max_runtime, processor.run(max_runtime, dry_run)).await;

    let (status, final_errors) = match processing_result {
        Ok(Ok(_)) => ("success".to_string(), errors),
        Ok(Err(e)) => {
            let error_msg = format!("Processing failed: {}", e);
            error!("{}", error_msg);
            errors.push(error_msg);
            ("error".to_string(), errors)
        }
        Err(_) => {
            let error_msg = "Lambda timeout exceeded".to_string();
            error!("{}", error_msg);
            errors.push(error_msg);
            ("timeout".to_string(), errors)
        }
    };

    let metrics = processor.get_metrics().clone();

    Ok(LambdaResponse {
        status,
        messages_processed: metrics.successful_messages,
        batches_processed: metrics.batches_processed,
        runtime_seconds: start_time.elapsed().as_secs_f64(),
        errors: final_errors,
        metrics,
    })
}

async fn load_config(config_path: Option<&str>) -> Result<IngestConfig> {
    match config_path {
        Some(path) => IngestConfig::from_file(path),
        None => IngestConfig::from_env(),
    }
}

fn init_tracing() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "stream_ingest=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    init_tracing();

    if env::var("AWS_LAMBDA_RUNTIME_API").is_ok() {
        info!("Running in AWS Lambda environment");
        lambda_runtime::run(service_fn(lambda_handler)).await
    } else {
        info!("Running in local development mode");
        
        let config = load_config(None).await
            .map_err(|e| format!("Failed to load configuration: {}", e))?;

        let mut processor = StreamIngestProcessor::new(config).await
            .map_err(|e| format!("Failed to initialize processor: {}", e))?;

        let max_runtime = Duration::from_secs(60); // 1 minute for local testing
        let dry_run = env::var("DRY_RUN").map(|v| v == "true").unwrap_or(false);

        processor.run(max_runtime, dry_run).await
            .map_err(|e| format!("Processing failed: {}", e))?;

        info!("Local processing completed successfully");
        Ok(())
    }
}
