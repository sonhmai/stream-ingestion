# FAQ

<!-- TOC -->
* [FAQ](#faq)
    * [When to use what?](#when-to-use-what)
    * [How about cost comparison?](#how-about-cost-comparison)
    * [Why we don't want to write to object store for every RecordBatch/ micro-batch?](#why-we-dont-want-to-write-to-object-store-for-every-recordbatch-micro-batch)
    * [Lambda triggering](#lambda-triggering)
<!-- TOC -->

Note that the decisions documented here can be changed in the future when the assumption when we made them no longer holds or there is new constraint, requirement.

### When to use what?
- Lambda: <100 msg/sec
- Databricks/ Spark: >= 100 msg/sec

These numbers are not yet verified by performance test yet.

### How about cost comparison?
The cost can be estimated.
We plan to build a working version then do some benchmark to see the cost.

### Why we don't want to write to object store for every RecordBatch/ micro-batch?
- Writing every single micro-batch from Kafka directly to S3 is inefficient.
- Aggregating them in memory reduces I/O, creates larger and more optimally-sized files for the data lake, and lowers API costs.

### Why Rust?
Why Rust?
- AWS Lambda cold start under 150ms with SDK init.
- Memory efficiency <40 MB for simple functions, cost saving >90% compared to JVM.

Rust integration with Kafka (`rust-rdkafka`)
- delivers `1M msg/s` for small msgs and `234 MB/s` sustained throughput
- full async support (with Tokio)
- transaction support for exactly-once semantics
- automatic connection management with broker failover

### Delta sink
- should delta sink make sure that s3 bucket and delta table already exist first?

### Lambda triggering

AWS Lambda triggering by Event Source Mapping (ESM) has a lot of benefits. However, we chose scheduling (polling) for now because
1. low level control of what happens.
2. no need to have system coupling when using lambda with cross-account AWS MSK or Kafka clusters where Event Source Mapping is not possible.

We do realize the challenges of this choice.

<table>
  <thead>
    <tr>
      <th>Feature</th>
      <th>Option A: Event Source Mapping (ESM)</th>
      <th>Option B: Scheduled Lambda (Polling)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><strong>Organizational Coupling</strong></td>
      <td><strong>High</strong>. Requires cross-team coordination for IAM, VPC, and policies. Brittle to changes in the source account.</td>
      <td><strong>Low</strong>. Requires only credentials. Promotes team autonomy and independent deployment. <strong>(Strongly Preferred)</strong></td>
    </tr>
    <tr>
      <td><strong>Code Complexity</strong></td>
      <td><strong>Low</strong>. The framework handles polling, scaling, and offset commits. The team focuses on business logic.</td>
      <td><strong>High</strong>. The team is responsible for the entire consumer lifecycle: client config, polling loop, state management, and manual offset commits.</td>
    </tr>
    <tr>
      <td><strong>Data Latency</strong></td>
      <td><strong>Low</strong>. Near real-time processing as messages arrive.</td>
      <td><strong>High</strong>. Latency is determined by the schedule interval (e.g., 5-15 minutes).</td>
    </tr>
    <tr>
      <td><strong>Scalability</strong></td>
      <td><strong>Automatic & Elastic</strong>. Scales concurrently up to the number of Kafka partitions without code changes.</td>
      <td><strong>Manual</strong>. Scaling requires vertical sizing (more memory/vCPU) or a significantly more complex manual partitioning strategy.</td>
    </tr>
    <tr>
      <td><strong>Error Handling</strong></td>
      <td><strong>Managed</strong>. Built-in retry policies and DLQ integration for failed batches.</td>
      <td><strong>DIY</strong>. Self-implement all retry and dead-lettering logic within the Lambda code.</td>
    </tr>
  </tbody>
</table>

### Why not use datafusion to write data to object store instead of delta-rs?
- `datafusion` uses delta under the hood to write Delta table, so there is no using `datafusion` alone, we need to import `delta-rs` anyway.
- `delta-rs` gives a more low-level abstraction and finer-grained control. This is what we likely need for controlling the writes and commits. For example
  - how to make the write idempotent?
  - how to make the ingestion exactly-once?
  - do we need to manually manage checkpoint to make the above happen?

```rust
use datafusion::prelude::*;
use deltalake::{DeltaTable, DeltaOps, DeltaTableError};
use arrow_array::RecordBatch;

let ctx = SessionContext::new();

// create and register delta source
let source = DeltaTable::open(source_table).await?;
ctx.register_table("source", Arc::new(source))?;

// transformation
let df = ctx.sql(query).await?;
let batches: Vec<RecordBatch> = df.collect().await?;

// Write to target Delta table
let _table = DeltaOps::try_from_uri(target_path)
  .await?
  .write(batches)
  .with_save_mode(SaveMode::Overwrite)
  .await?;
```

### thiserror vs anyhow

Use thiserror
- for library (like `core`)
- callers need to react differently to different kinds of errors for example
  - retry on ThisError
  - fail on ThatError

Use anyhow
- in `main.rs` or top-level app logic.
- need to propagate an error upwards without inspecting its type.
- add contextual info to errors as they bubble up.