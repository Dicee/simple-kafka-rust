//! This crate provides all the logic and binaries necessary to run a broker on a node.

use actix_web::error::BlockingError;
use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder};
use std::sync::Arc;
use crate::broker::Broker;
use crate::persistence::LogManager;
use argh::FromArgs;
use ::broker::model::{PublishRawRequest, PublishResponse, ReadNextBatchRequest, TopicPartition};
use broker::Error as BrokerError;
use coordinator::model::RegisterBrokerRequest;
use protocol::record::RecordBatch;
use serde::Serialize;
use coordinator::Client;

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

#[get("/ping")]
async fn ping() -> impl Responder { "{\"ping\": \"pong\"}" }

#[post("/publish")]
async fn publish(
    broker: web::Data<Arc<Broker>>,
    query: web::Query<TopicPartition>,
    record_batch: web::Json<RecordBatch>,
) -> impl Responder {
    build_http_response(
        web::block(move || { broker.publish(&query.topic, query.partition, record_batch.into_inner()) }).await,
        |base_offset| PublishResponse { base_offset }
    )
}

#[post("/publish-raw")]
async fn publish_raw(
    broker: web::Data<Arc<Broker>>,
    query: web::Query<PublishRawRequest>,
    body: web::Bytes,
) -> impl Responder {
    build_http_response(
        web::block(move || { broker.publish_raw(&query.topic, query.partition, body.to_vec(), query.record_count) }).await,
        |base_offset| PublishResponse { base_offset }
    )
}

// Note this is a post because it modifies some state on the server. Ideally the client should just send an offset to read from, but for now we
// do not have an operation capable of reinitializing the state of the reader on the broker side if we notice that the request offset is not the 
// one the reader is currently tracking. For simplicity's sake, we'll keep the stateful API for now (I want to get things to work end-to-end before
// working on smaller improvements).
#[post("/read-next-batch")]
async fn read_next_batch(
    broker: web::Data<Arc<Broker>>,
    request: web::Json<ReadNextBatchRequest>,
) -> impl Responder {
    let ReadNextBatchRequest { topic, partition, consumer_group } = request.into_inner();
    build_http_response(
        web::block(move || broker.read_next_batch(&topic, partition, consumer_group)).await,
        |record_batch| record_batch
    )
}

fn build_http_response<T: Clone, R: Serialize, F>(result: Result<Result<T, BrokerError>, BlockingError>, converter: F) -> HttpResponse
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

    let coordinator_client = coordinator::ClientImpl::new(args.coordinator_endpoint);
    coordinator_client.register_broker(RegisterBrokerRequest { host: args.host.clone(), port: args.port })
        .expect("Failed to register broker to the coordinator service");

    let broker = Arc::new(Broker::new(LogManager::new(args.root_path), Arc::new(coordinator_client)));
    let broker_clone = Arc::clone(&broker);
    let broker_data = web::Data::new(broker);

    let server = HttpServer::new(move || App::new()
        .app_data(broker_data.clone())
        .service(ping)
        .service(publish).service(publish_raw)
        .service(read_next_batch)
    );

    println!("Starting server on {}:{}...", args.host, args.port);
    server
        // seems more than enough since these threads take the request asynchronously, and there are other threads taking care of the read and write
        // operations (one per partition)
        .workers(10)
        .bind((args.host, args.port))?
        .run()
        .await?;

    // the server implements graceful shutdown, so if we run after server.await all server references and web::Data references should have been cleaned up,
    // and we're safe to clean up the state of the application itself
    match Arc::into_inner(broker_clone) {
        None => panic!("Unable to shut down the broker, there are still references to it"),
        Some(broker) => broker.shutdown().expect("Unable to shut down the broker cleanly"),
    };

    Ok(())
}