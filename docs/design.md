# Design notes

<!-- TOC -->
* [Design notes](#design-notes)
  * [Rust](#rust)
  * [Fault Tolerance](#fault-tolerance)
    * [Checkpointing](#checkpointing)
<!-- TOC -->

## Rust

Why Rust?
- AWS Lambda cold start under 150ms with SDK init.
- Memory efficiency <40 MB for simple functions, cost saving >90% compared to JVM.

Rust integration with Kafka (`rust-rdkafka`)
- delivers `1M msg/s` for small msgs and `234 MB/s` sustained throughput
- full async support (with Tokio)
- transaction support for exactly-once semantics
- automatic connection management with broker failover

## Fault Tolerance

Failure modes:
- lambda failed while consuming Kafka
- lambda failed while writing S3

### Checkpointing

Compute backend only commit offset back to Kafka after writing consumed data successfully.