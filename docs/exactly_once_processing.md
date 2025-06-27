# Exactly Once Processing

EOP achieved by storing Kafka partition offsets as txn action in Delta Lake transaction log.

Delta Lake has `txn` action
```
{
  "txn": {
    "appId":"3ba13872-2d47-4e17-86a0-21afd2a22395", // (String) uid for app
    "version":364475, // (Long) app-specific identifier for this txn
    "lastUpdated": 123456789  // Option[Long] ms since Unix epoch, time txn created
  }
}
```

this is tracked per partition
```rust
let txn_app_id = format!("{}-{}", app_id, partition);
```