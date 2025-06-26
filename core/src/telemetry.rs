use serde::Serialize;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

pub fn init_tracing() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "stream_ingest=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct IngestMetrics {
    pub total_messages: usize,
    pub successful_messages: usize,
    pub failed_messages: usize,
    pub bytes_processed: u64,
    pub avg_batch_size: f64,
    pub delta_writes: usize,
    pub kafka_commits: usize,
    pub batches_processed: usize,
}