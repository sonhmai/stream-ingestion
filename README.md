# Stream Ingest Framework

A Rust framework for ingesting Kafka topics into AWS S3 as Delta Lake tables, designed to run on AWS Lambda.

## Overview

This framework provides a configuration-driven approach to streaming data ingestion, converting Kafka messages into queryable Delta Lake format stored in S3. It's optimized for AWS Lambda deployment with comprehensive error handling and retry logic.

## Development Commands

- **Build**: `cargo build --release`
- **Test**: `cargo test --lib` (unit tests)
- **Check**: `cargo check`
- **Format**: `cargo fmt`
- **Lint**: `cargo clippy`

```shell
# deploy lambda to an AWS env. make sure AWS profile was activated or AWS envs were exported
# export AWS_ACCESS_KEY_ID=example
# export AWS_SECRET_ACCESS_KEY=example
# export AWS_SESSION_TOKEN=example
ingest deploy --config configs/source_kafka_sasl.yaml
```