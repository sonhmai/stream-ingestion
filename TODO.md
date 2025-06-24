# TODO

POC
- [ ] basic kafka integration
- [ ] local testing
- [ ] lambda entry point

design
- do we use Kafka trigger or Lambda schedule?
- does Kafka trigger work with AWS MSK in other accounts or on-prem Kafka?

features
- [ ] kafka consumer
  - consume(config) -> subscribe and poll messages
- [ ] delta sink
  - do we need to use write locking e.g. in DynamoDB for concurrent writes? Probably not, we can limit concurrency
  or lock through S3 files. It's simpler.
- [ ] checkpointing
- [ ] cicd
  - how to version this lib? 
  - how to deploy it to lambda?

hardening
- [ ] perf test
  - how much (TPS, throughput) can lambda sustain?
  - how to map TPS, throughput into lambda memory provision?
- [ ] observability
  - can use Kafka consumer-group metrics for things like consumer lag, etc.
  - alerts can be set on lambda failures.


nice to have
- [ ] compaction: maybe taken care by other platform jobs.