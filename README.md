# Stream Ingest Framework

A Rust framework for ingesting Kafka topics into AWS S3 as Delta Lake tables, designed to run on AWS Lambda.

## Overview

This framework provides a configuration-driven approach to streaming data ingestion, converting Kafka messages into queryable Delta Lake format stored in S3. It's optimized for AWS Lambda deployment with comprehensive error handling and retry logic.

```shell

# test
cargo test -p ingest-core

# build release for Linux x86 (Lambda)
cargo build --release --target=x86_64-unknown-linux-musl
```

## Build issues

If has error similar to this on MacOS
``` 
 --- stderr
  librdkafka will be linked dynamically
  librdkafka 2.3.0 cannot be found on the system: Could not run `PKG_CONFIG_ALLOW_SYSTEM_LIBS=1 PKG_CONFIG_ALLOW_SYSTEM_CFLAGS=1 pkg-config --libs --cflags rdkafka 'rdkafka >= 2.3.0'`
  The pkg-config command could not be found.
```
install packages
``` 
brew install pkg-config cmake pkg-config-wrapper
```

- https://www.oliverlooney.com/blogs/How-to-cross-compile-rust-lambda-for-Amazon-Linux-2-Lambda-Runtime
- https://loige.co/building_x86_rust-containers-from-mac-silicon/