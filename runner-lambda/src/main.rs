use std::time::{Duration, Instant};
use anyhow::Result;
use std::env;
use lambda_runtime::LambdaEvent;
use lambda_runtime::tracing::{error, info};
use serde::{Deserialize, Serialize};

pub async fn lambda_handler(event: LambdaEvent<LambdaRequest>) -> Result<LambdaResponse, Error> {
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

#[tokio::main]
async fn main() -> Result<(), Error> {
    init_tracing();

    if env::var("AWS_LAMBDA_RUNTIME_API").is_ok() {
        info!("Running in AWS Lambda environment");
        lambda_runtime::run(service_fn(crate::lambda_handler)).await
    } else {
        info!("Running in local development mode");

        let config = core::load_config(None)
            .await
            .map_err(|e| format!("Failed to load configuration: {}", e))?;

        let mut processor = StreamIngestProcessor::new(config)
            .await
            .map_err(|e| format!("Failed to initialize processor: {}", e))?;

        let max_runtime = Duration::from_secs(60); // 1 minute for local testing
        let dry_run = env::var("DRY_RUN").map(|v| v == "true").unwrap_or(false);

        processor
            .run(max_runtime, dry_run)
            .await
            .map_err(|e| format!("Processing failed: {}", e))?;

        info!("Local processing completed successfully");
        Ok(())
    }
}

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