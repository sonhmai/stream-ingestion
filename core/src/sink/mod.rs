pub mod delta;

/// The `DataSink` trait defines the contract for any destination system.
/// It is responsible for taking a batch of data in Arrow format and persisting it.
pub trait Sink: Send + Sync {
    // todo what should be the interface for the sink? buffering, flush, etc.
}