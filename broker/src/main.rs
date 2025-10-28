//! This crate provides all the logic and binaries necessary to run a broker on a node.

use actix_web::error::BlockingError;
use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder};
use coordinator::client::{Client, ClientImpl as CoordinatorClient};
use std::sync::Arc;

use crate::broker::Broker;
use crate::persistence::LogManager;
use argh::FromArgs;
use ::broker::model::PublishResponse;
use broker::Error as BrokerError;
use coordinator::model::RegisterBrokerRequest;
use protocol::record::RecordBatch;
use serde::Serialize;

mod broker;
mod persistence;

#[derive(FromArgs)]
/// Starts a broker server on the given port, with the given hostname.
struct Args {
    /// hostname to use for the server
    #[argh(option, default = "String::from(\"localhost\")")]
    host: String,

    /// port to use for the server
    #[argh(option)]
    port: u16,

    /// endpoint of the coordinator service
    #[argh(option)]
    coordinator_endpoint: String,

    /// root path on the disk under which all the data will be written
    #[argh(option)]
    root_path: String,
}

#[post("/publish/{topic}/{partition}")]
async fn publish(
    broker: web::Data<Arc<Broker>>,
    record_batch: web::Json<RecordBatch>,
    path: web::Path<(String, u32)>,
) -> impl Responder {
    build_http_response(
        web::block(move || { broker.publish(&path.0, path.1, record_batch.into_inner()) }).await,
        |base_offset| PublishResponse { base_offset }
    )
}

#[get("/read-next-batch/{topic}/{partition}/{consumer_group}")]
async fn read_next_batch(
    broker: web::Data<Arc<Broker>>,
    path: web::Path<(String, u32, String)>,
) -> impl Responder {
    let (topic, partition, consumer_group) = path.into_inner();
    println!("topic: {}, partition: {}, consumer_group: {}", topic, partition, consumer_group);
    build_http_response(
        web::block(move || broker.read_next_batch(&topic, partition, consumer_group)).await,
        |record_batch| record_batch
    )
}

fn build_http_response<T, R: Serialize, F>(result: Result<Result<T, BrokerError>, BlockingError>, converter: F) -> HttpResponse
where F: Fn(T) -> R {
    match result {
        Ok(Ok(t)) => HttpResponse::Ok().json(converter(t)),
        Ok(Err(e)) => convert_broker_error(e),
        Err(_) => HttpResponse::InternalServerError().body("Internal failure with unknown cause"),
    }
}

fn convert_broker_error(e: BrokerError) -> HttpResponse {
    match e {
        BrokerError::Coordinator(msg) => HttpResponse::BadGateway().body(msg),
        BrokerError::Io(e) => HttpResponse::InternalServerError().body(e.to_string()),
        BrokerError::Internal(msg) => HttpResponse::InternalServerError().body(msg),
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let args: Args = argh::from_env();

    let coordinator_client = CoordinatorClient::new(args.coordinator_endpoint);
    coordinator_client.register_broker(RegisterBrokerRequest { host: args.host.clone(), port: args.port })
        .expect("Failed to register broker to the coordinator service");

    let broker = Broker::new(Arc::new(LogManager::new(args.root_path)), Arc::new(coordinator_client));
    let broker = web::Data::new(Arc::new(broker));

    let server = HttpServer::new(move || App::new()
        .app_data(broker.clone())
        .service(publish)
        .service(read_next_batch)
    );
    server
        // seems more than enough since these threads take the request asynchronously, and there are other threads taking care of the read and write
        // operations (one per partition)
        .workers(10)
        .bind((args.host, args.port))?
        .run()
        .await
}