use lambda_runtime::{service_fn, Error, LambdaEvent};
use serde::{Deserialize, Serialize};

pub async fn lambda_handler(event: LambdaEvent<HelloRequest>) -> Result<HelloResponse, Error> {
    let request = event.payload;
    let name = request.name.unwrap_or_else(|| "World".to_string());
    
    Ok(HelloResponse {
        message: format!("Hello, {}!", name),
    })
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    lambda_runtime::run(service_fn(lambda_handler)).await
}

#[derive(Debug, Deserialize)]
struct HelloRequest {
    #[serde(default)]
    pub name: Option<String>,
}

#[derive(Debug, Serialize)]
struct HelloResponse {
    pub message: String,
}