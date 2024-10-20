use rustomic::clock::Instant;
use rustomic::query::database::Database;
use rustomic::schema::attribute::*;
use rustomic::schema::default::default_datoms;
use rustomic::storage::attribute_resolver::AttributeResolver;
use rustomic::storage::disk::DiskStorage;
use rustomic::storage::disk::ReadOnly;
use rustomic::storage::ReadStorage;
use rustomic::storage::WriteStorage;
use rustomic::tx::transactor;
use rustomic::tx::Transaction;
use server::query_service_server::QueryServiceServer;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::Mutex;
use tonic::{transport::Server, Request, Response, Status};

use server::query_service_server::QueryService;
use server::QueryRequest;
use server::QueryResponse;

mod edn;
mod parser;

const DB_PATH: &str = "/tmp/foo";

pub mod server {
    tonic::include_proto!("rustomic.server");
}

pub struct QueryServiceImpl {
    storage: DiskStorage<ReadOnly>,
    resolver: Arc<Mutex<AttributeResolver>>,
}

#[tonic::async_trait]
impl QueryService for QueryServiceImpl {
    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<QueryResponse>, Status> {
        let mut resolver = self.resolver.lock().await;
        let basis_tx = self
            .storage
            .latest_entity_id()
            .map_err(|err| Status::unknown(err.to_string()))?;
        let mut db = Database::new(basis_tx);
        let request = request.into_inner();
        let query = parser::parse(&request.query).map_err(Status::invalid_argument)?;
        let results: Vec<_> = db
            .query(&self.storage, &mut resolver, query)
            .map_err(|err| Status::unknown(err.to_string()))?
            .collect();
        println!("Got a request: {:?} {:?}", request, results);
        Ok(Response::new(QueryResponse {}))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut resolver = AttributeResolver::new();

    init_db(&mut resolver)?;

    let addr = "[::1]:50051".parse()?;
    let storage = DiskStorage::read_only(DB_PATH)?;
    let greeter = QueryServiceImpl {
        storage,
        resolver: Arc::new(Mutex::new(resolver)),
    };

    println!("Starting server on {:?}...", &addr);

    Server::builder()
        .add_service(QueryServiceServer::new(greeter))
        .serve(addr)
        .await?;

    Ok(())
}

fn init_db(resolver: &mut AttributeResolver) -> Result<(), Box<dyn std::error::Error>> {
    let mut storage = DiskStorage::read_write(DB_PATH)?;
    if storage.latest_entity_id()? > 0 {
        // Looks like the DB already has some datoms saved, no need to re-create the schema.
        return Ok(());
    }

    storage.save(&default_datoms())?;

    let now = Instant(
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs(),
    );

    let tx = Transaction::new()
        .with(AttributeDefinition::new("movie/name", ValueType::Str))
        .with(AttributeDefinition::new("movie/year", ValueType::U64))
        .with(AttributeDefinition::new("movie/director", ValueType::Ref).many())
        .with(AttributeDefinition::new("movie/cast", ValueType::Ref).many())
        .with(AttributeDefinition::new("actor/name", ValueType::Str))
        .with(AttributeDefinition::new("artist/name", ValueType::Str))
        .with(AttributeDefinition::new("release/name", ValueType::Str))
        .with(AttributeDefinition::new("release/artists", ValueType::Ref).many());

    let tx_result = transactor::transact(&storage, resolver, now, tx)?;
    storage.save(&tx_result.tx_data)?;

    Ok(())
}
