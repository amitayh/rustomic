use server::query_service_server::QueryServiceServer;
use tonic::{transport::Server, Request, Response, Status};

use server::query_service_server::QueryService;
use server::QueryRequest;
use server::QueryResponse;

pub mod server {
    tonic::include_proto!("rustomic.server");
}

#[derive(Debug, Default)]
pub struct MyGreeter {}

#[tonic::async_trait]
impl QueryService for MyGreeter {
    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<QueryResponse>, Status> {
        println!("Got a request: {:?}", request);
        Ok(Response::new(QueryResponse {}))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let greeter = MyGreeter::default();

    Server::builder()
        .add_service(QueryServiceServer::new(greeter))
        .serve(addr)
        .await?;

    Ok(())
}
