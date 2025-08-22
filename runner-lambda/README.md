# Lambda Runner

Runner for the Rust lambda.

- [AWS CLI](https://aws.amazon.com/cli/) configured with appropriate credentials
- [Cargo Lambda](https://github.com/cargo-lambda/cargo-lambda) for building Rust Lambda functions
- [SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html) (optional, for local testing)

Using [Cargo Lambda](https://www.cargo-lambda.info/guide/getting-started.html)

```bash
# install via brew
brew install cargo-lambda/tap/cargo-lambda
# or install via cargo
cargo install cargo-lambda

# build the lambda function
cargo lambda build --release

# deploy with new default execution role created
cargo lambda deploy
# deploy with existing role
cargo lambda deploy --iam-role arn:aws:iam::YOUR_ACCOUNT_ID:role/lambda-execution-role

# run locally for testing
cargo lambda watch
cargo lambda invoke --data-ascii '{"name": "Local"}'
```

In case we want to use aws cli instead of cargo-lambda
```bash
cargo lambda build --release

aws lambda create-function \
  --function-name hello-world-rust \
  --runtime provided.al2023 \
  --role arn:aws:iam::YOUR_ACCOUNT_ID:role/lambda-execution-role \
  --handler bootstrap \
  --zip-file fileb://target/lambda/runner-lambda/bootstrap.zip

# run
aws lambda invoke \
  --function-name hello-world-rust \
  --payload '{"name": "AWS"}' \
  response.json
```
