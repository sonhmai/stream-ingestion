use thiserror::Error;

#[derive(Error, Debug)]
pub enum IngestionError {
    #[error("Configuration error: {0}")]
    Config(#[from] ConfigError),

    #[error("Data source error")]
    SourceError(#[from] SourceError),

    #[error("Kafka error: {0}")]
    Kafka(#[from] KafkaError),

    #[error("Delta Lake error: {0}")]
    Delta(#[from] DeltaError),

    #[error("Serialization error: {0}")]
    Serialization(#[from] SerializationError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Generic error: {0}")]
    Generic(#[from] anyhow::Error),
}

#[derive(Error, Debug)]
pub enum SourceError {
    #[error("Failed to connect or communicate with the source")]
    Connection {
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("A transient error occurred, the operation can be retried")]
    Transient {
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("Unrecoverable error from the source")]
    Unrecoverable(#[source] Box<dyn std::error::Error + Send + Sync>),
}

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Invalid configuration: {message}")]
    Invalid { message: String },

    #[error("Missing required field: {field}")]
    MissingField { field: String },

    #[error("Configuration validation failed: {reason}")]
    ValidationFailed { reason: String },

    #[error("Failed to load configuration from {source}: {error}")]
    LoadFailed {
        source: String,
        #[source]
        error: Box<dyn std::error::Error + Send + Sync>,
    },
}

#[derive(Error, Debug)]
pub enum KafkaError {
    #[error("Failed to create consumer: {reason}")]
    ConsumerCreation { reason: String },

    #[error("Failed to subscribe to topic {topic}: {reason}")]
    Subscription { topic: String, reason: String },

    #[error("Failed to consume messages: {reason}")]
    Consumption { reason: String },

    #[error("Failed to commit offsets: {reason}")]
    OffsetCommit { reason: String },

    #[error("Message parsing failed: {reason}")]
    MessageParsing { reason: String },

    #[error("Consumer health check failed: {reason}")]
    HealthCheck { reason: String },
}

#[derive(Error, Debug)]
pub enum DeltaError {
    #[error("Failed to create Delta table: {reason}")]
    TableCreation { reason: String },

    #[error("Failed to write to Delta table: {reason}")]
    Write { reason: String },

    #[error("Schema conversion failed: {reason}")]
    SchemaConversion { reason: String },

    #[error("Failed to load Delta table from {path}: {reason}")]
    TableLoad { path: String, reason: String },

    #[error("Table optimization failed: {reason}")]
    Optimization { reason: String },

    #[error("Table vacuum failed: {reason}")]
    Vacuum { reason: String },
}

#[derive(Error, Debug)]
pub enum LambdaError {
    #[error("Lambda initialization failed: {reason}")]
    Initialization { reason: String },

    #[error("Lambda invocation failed: {reason}")]
    Invocation { reason: String },

    #[error("Lambda timeout exceeded")]
    Timeout,

    #[error("Lambda memory limit exceeded")]
    MemoryLimit,

    #[error("Lambda cold start detected")]
    ColdStart,
}

#[derive(Error, Debug)]
pub enum SerializationError {
    #[error("JSON serialization failed: {reason}")]
    Json { reason: String },

    #[error("YAML serialization failed: {reason}")]
    Yaml { reason: String },

    #[error("Arrow serialization failed: {reason}")]
    Arrow { reason: String },

    #[error("Data type conversion failed from {from_type} to {to_type}: {reason}")]
    TypeConversion {
        from_type: String,
        to_type: String,
        reason: String,
    },
}

pub type Result<T> = std::result::Result<T, IngestionError>;

impl From<serde_json::Error> for IngestionError {
    fn from(err: serde_json::Error) -> Self {
        IngestionError::Serialization(SerializationError::Json {
            reason: err.to_string(),
        })
    }
}

impl From<serde_yaml::Error> for IngestionError {
    fn from(err: serde_yaml::Error) -> Self {
        IngestionError::Serialization(SerializationError::Yaml {
            reason: err.to_string(),
        })
    }
}

impl IngestionError {
    pub fn is_retryable(&self) -> bool {
        match self {
            IngestionError::Kafka(KafkaError::Consumption { .. }) => true,
            IngestionError::Kafka(KafkaError::OffsetCommit { .. }) => true,
            IngestionError::Delta(DeltaError::Write { .. }) => true,
            IngestionError::Config(_) => false,
            IngestionError::Serialization(_) => false,
            _ => true,
        }
    }

    pub fn should_skip_message(&self) -> bool {
        match self {
            IngestionError::Serialization(_) => true,
            IngestionError::Kafka(KafkaError::MessageParsing { .. }) => true,
            IngestionError::Delta(DeltaError::SchemaConversion { .. }) => true,
            _ => false,
        }
    }

    pub fn get_retry_delay_ms(&self) -> u64 {
        match self {
            IngestionError::Kafka(_) => 1000,
            IngestionError::Delta(_) => 2000,
            _ => 5000,
        }
    }
}

