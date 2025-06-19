# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Rust framework for ingesting Kafka topics into AWS S3 as Delta Lake tables, designed to run on AWS Lambda. The system processes streaming data in batches and stores it in a structured, queryable format.

## Development Commands

- **Build**: `cargo build --release`
- **Run locally**: `cargo run` (requires INGEST_CONFIG env var or config file)
- **Test**: `cargo test`
- **Check**: `cargo check`
- **Format**: `cargo fmt`
- **Lint**: `cargo clippy`
- **Build for Lambda**: Use AWS SAM or cargo lambda for deployment

## Architecture

### Core Components

- **config.rs** - Configuration management using YAML format
- **kafka.rs** - Kafka consumer client with batch processing
- **delta.rs** - Delta Lake writer with Arrow schema conversion
- **s3.rs** - S3 client integration with health checks and metrics
- **errors.rs** - Comprehensive error handling with retry logic
- **main.rs** - Lambda handler and local execution orchestration

### Data Flow

1. **Kafka Consumption**: Connects to Kafka cluster and consumes messages in configurable batches
2. **Schema Validation**: Validates incoming JSON against configured Delta schema
3. **Delta Conversion**: Converts messages to Arrow format with automatic Kafka metadata
4. **S3 Storage**: Writes Delta Lake files to S3 with optional partitioning
5. **Offset Management**: Commits Kafka offsets after successful writes

### Configuration

Uses YAML configuration files (see `examples/` directory):
- Kafka connection and consumer settings
- S3 bucket and credentials configuration  
- Delta Lake schema definition with data types
- Processing parameters (batch size, retries, timeouts)

### Lambda Integration

- Automatically detects Lambda environment via AWS_LAMBDA_RUNTIME_API
- Supports configurable runtime limits (default 14 minutes)
- Returns detailed metrics and error information
- Handles timeouts and memory limits gracefully

### Key Features

- **Automatic Schema Evolution**: Optional schema merging for evolving data
- **Partitioning Support**: Configurable partition columns for optimized queries
- **Retry Logic**: Intelligent retry with exponential backoff
- **Health Checks**: Built-in health checks for Kafka, S3, and Delta table
- **Metrics Collection**: Comprehensive processing metrics and monitoring
- **Dry Run Mode**: Testing mode that validates configuration without writing data