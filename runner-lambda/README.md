# Lambda Runner

Runner for the Rust lambda.

## Prerequisites

- [AWS CLI](https://aws.amazon.com/cli/) configured with appropriate credentials
- [Cargo Lambda](https://github.com/cargo-lambda/cargo-lambda) for building Rust Lambda functions
- [SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html) (optional, for local testing)

## Installation

Install Cargo Lambda:
```bash
cargo install cargo-lambda
```

## Build

Build the Lambda function for deployment:
```bash
cargo lambda build --release
```

## Deploy

### Option 1: Using AWS CLI

1. Create a deployment package:
```bash
cargo lambda build --release
```

2. Create the Lambda function:
```bash
aws lambda create-function \
  --function-name hello-world-rust \
  --runtime provided.al2023 \
  --role arn:aws:iam::YOUR_ACCOUNT_ID:role/lambda-execution-role \
  --handler bootstrap \
  --zip-file fileb://target/lambda/runner-lambda/bootstrap.zip
```

3. Test the function:
```bash
aws lambda invoke \
  --function-name hello-world-rust \
  --payload '{"name": "AWS"}' \
  response.json
```

### Option 2: Using Cargo Lambda Deploy

```bash
cargo lambda deploy --iam-role arn:aws:iam::YOUR_ACCOUNT_ID:role/lambda-execution-role
```

## Local Testing

Test locally using Cargo Lambda:
```bash
cargo lambda watch
```

In another terminal:
```bash
cargo lambda invoke --data-ascii '{"name": "Local"}'
```

## Input/Output

**Input:**
```json
{
  "name": "World"
}
```

**Output:**
```json
{
  "message": "Hello, World!"
}
```
