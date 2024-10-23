use rustomic::clock::Instant;
use rustomic::datom::Value;
use rustomic::query::database::Database;
use rustomic::query::QueryError;
use rustomic::schema::attribute::*;
use rustomic::schema::default::default_datoms;
use rustomic::storage::attribute_resolver::AttributeResolver;
use rustomic::storage::disk::DiskStorage;
use rustomic::storage::disk::DiskStorageError;
use rustomic::storage::disk::ReadOnly;
use rustomic::storage::ReadStorage;
use rustomic::storage::WriteStorage;
use rustomic::tx::transactor;
use rustomic::tx::EntityOperation;
use rustomic::tx::Transaction;
use server::query_service_server::QueryServiceServer;
use std::collections::HashSet;
use std::time::SystemTime;
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
    resolver: AttributeResolver,
}

impl QueryServiceImpl {
    async fn query_impl(
        &self,
        request: QueryRequest,
    ) -> Result<HashSet<Vec<Value>>, QueryError<DiskStorageError>> {
        let basis_tx = self.storage.latest_entity_id()?;
        let query = parser::parse(&request.query).map_err(|_| QueryError::Error)?;
        println!("@@@ request: {:?}", &request);
        println!("@@@ parsed query: {:?}", &query);
        let results = Database::new(basis_tx)
            .query(&self.storage, &self.resolver, query)
            .await?;
        results.collect()
    }
}

#[tonic::async_trait]
impl QueryService for QueryServiceImpl {
    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<QueryResponse>, Status> {
        let results = self
            .query_impl(request.into_inner())
            .await
            .map_err(|err| Status::unknown(err.to_string()))?;
        println!("@@@ results: {:?}", &results);
        Ok(Response::new(QueryResponse {
            assignments: results
                .iter()
                .map(|result| format!("{:?}", result))
                .collect(),
        }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let resolver = AttributeResolver::new();
    init_db(&resolver).await?;

    let storage = DiskStorage::read_only(DB_PATH)?;
    let query_service = QueryServiceImpl { storage, resolver };

    let addr = "[::1]:50051".parse()?;
    println!("Starting server on {:?}...", &addr);

    Server::builder()
        .add_service(QueryServiceServer::new(query_service))
        .serve(addr)
        .await?;

    Ok(())
}

async fn init_db(resolver: &AttributeResolver) -> Result<(), Box<dyn std::error::Error>> {
    let mut storage = DiskStorage::read_write(DB_PATH)?;
    if storage.latest_entity_id()? > 0 {
        // Looks like the DB already has some datoms saved, no need to re-create the schema.
        return Ok(());
    }

    storage.save(&default_datoms())?;

    {
        let tx = Transaction::new()
            .with(AttributeDefinition::new("movie/name", ValueType::Str))
            .with(AttributeDefinition::new("movie/year", ValueType::U64))
            .with(AttributeDefinition::new("movie/director", ValueType::Ref).many())
            .with(AttributeDefinition::new("movie/cast", ValueType::Ref).many())
            .with(AttributeDefinition::new("actor/name", ValueType::Str))
            .with(AttributeDefinition::new("artist/name", ValueType::Str))
            .with(AttributeDefinition::new("release/name", ValueType::Str))
            .with(AttributeDefinition::new("release/artists", ValueType::Ref).many());

        let tx_result = transactor::transact(&storage, resolver, now(), tx).await?;
        storage.save(&tx_result.tx_data)?;
    }

    {
        let tx = Transaction::new()
            .with(EntityOperation::on_temp_id("john").assert("artist/name", "John Lenon"))
            .with(EntityOperation::on_temp_id("paul").assert("artist/name", "Paul McCartney"))
            .with(
                EntityOperation::on_new()
                    .assert("release/name", "Abbey Road")
                    .set_reference("release/artists", "john")
                    .set_reference("release/artists", "paul"),
            );

        let tx_result = transactor::transact(&storage, resolver, now(), tx).await?;
        storage.save(&tx_result.tx_data)?;
    }

    Ok(())
}

fn now() -> Instant {
    Instant(
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs(),
    )
}
