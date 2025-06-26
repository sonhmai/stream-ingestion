# Design notes

<!-- TOC -->
* [Design notes](#design-notes)
  * [Background](#background)
  * [Design Requirements](#design-requirements)
  * [Design choices](#design-choices)
    * [API](#api)
  * [Fault Tolerance](#fault-tolerance)
    * [Checkpointing](#checkpointing)
<!-- TOC -->

## Background
Spark/ Databricks Structured Streaming jobs are commonly used in data platform for streaming workloads.
There are cases where this can be not cost-effective.

## Design Requirements

Functional
- pluggable compute backend: can switch between running a scheduled polling Lambda and long-running container workload e.g. on ECS or EKS later.
- system can be more cost-effective than running Databricks/ Spark Structured Streaming jobs in some workloads.

Non-functional
- exactly once consuming message from Kafka and write to storage.

## Design choices

## CLI

```shell
# deploy lambda to an AWS env. make sure AWS profile was activated or AWS envs were exported
# export AWS_ACCESS_KEY_ID=example
# export AWS_SECRET_ACCESS_KEY=example
# export AWS_SESSION_TOKEN=example
ingest deploy --config configs/source_kafka_sasl.yaml
```

### API
todo

## Fault Tolerance

Failure modes:
- lambda failed while consuming Kafka
- lambda failed while writing S3

### Checkpointing

Compute backend only commit offset back to Kafka after writing consumed data successfully.

