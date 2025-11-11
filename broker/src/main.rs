//! This crate provides all the logic and binaries necessary to run a broker on a node.

use crate::broker::Broker;
use crate::persistence::LogManager;
use actix_web::error::BlockingError;
use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder};
use argh::FromArgs;
use ::broker::model::BrokerApiErrorKind::{BadRequest, CoordinatorFailure, Internal};
use ::broker::model::{PollBatchesRequest, PublishRawRequest, PublishResponse, TopicPartition, READ_OFFSET_HEADER};
use broker::Error as BrokerError;
use client_utils::ApiError;
use coordinator::model::CoordinatorApiErrorKind;
use coordinator::Client;
use protocol::record::RecordBatch;
use serde::Serialize;
use std::sync::Arc;

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
    build_json_http_response(
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
    build_json_http_response(
        web::block(move || { broker.publish_raw(&query.topic, query.partition, body.to_vec(), query.record_count) }).await,
        |base_offset| PublishResponse { base_offset }
    )
}

// Note this is a post because it modifies some state on the server (to be efficient, we obviously cannot afford creating a new reader and finding
// the right offset every time, so we reuse the same BufReader most of the time, and that's a stateful struct).
#[post("/poll-batches-raw")]
async fn poll_batches_raw(
    broker: web::Data<Arc<Broker>>,
    request: web::Json<PollBatchesRequest>,
) -> impl Responder {
    let PollBatchesRequest { topic, partition, consumer_group, offset, poll_config } = request.into_inner();
    match web::block(move || { broker.poll_batches_raw(&topic, partition, consumer_group, offset, &poll_config) }).await {
        Ok(Ok(poll_response)) => {
            let mut response = HttpResponse::Ok();
            if let Some(ack_read_offset) = poll_response.ack_read_offset {
                response.append_header((READ_OFFSET_HEADER, ack_read_offset));
            }
            response.body(poll_response.bytes)
        }
        Ok(Err(e)) => convert_broker_error(e),
        Err(e) => new_internal_failure_response(e.to_string()),
    }
}

fn build_json_http_response<T: Clone, R: Serialize, F>(result: Result<Result<T, BrokerError>, BlockingError>, converter: F) -> HttpResponse
where F: Fn(T) -> R {
    match result {
        Ok(Ok(t)) => HttpResponse::Ok().json(converter(t)),
        Ok(Err(e)) => convert_broker_error(e),
        Err(e) => new_internal_failure_response(e.to_string()),
    }
}

fn convert_broker_error(e: BrokerError) -> HttpResponse {
    match e {
        BrokerError::InvalidReadOffset(cause) => new_bad_request_response(format!("{cause}")),
        BrokerError::UnexpectedCoordinatorError(msg) => new_coordinator_failure_response(msg),
        BrokerError::CoordinatorApi(e) => match e.kind {
            CoordinatorApiErrorKind::Internal => new_coordinator_failure_response(e.message),
            _ => new_bad_request_response(format!("{e:?}")),
        },
        BrokerError::Io(e) => new_internal_failure_response(e.to_string()),
        BrokerError::Internal(msg) => new_internal_failure_response(msg),
    }
}

fn new_bad_request_response(message: String) -> HttpResponse {
    HttpResponse::BadRequest().json(ApiError { kind: BadRequest, message })
}

fn new_coordinator_failure_response(message: String) -> HttpResponse {
    HttpResponse::BadGateway().json(ApiError { kind: CoordinatorFailure, message })
}

fn new_internal_failure_response(message: String) -> HttpResponse {
    HttpResponse::InternalServerError().json(ApiError { kind: Internal, message })
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let args: Args = argh::from_env();

    let coordinator_client = coordinator::ClientImpl::new(args.coordinator_endpoint);
    coordinator_client.register_broker(&args.host, args.port)
        .expect("Failed to register broker to the coordinator service");

    let broker = Arc::new(Broker::new(LogManager::new(args.root_path), Arc::new(coordinator_client)));
    let broker_clone = Arc::clone(&broker);
    let broker_data = web::Data::new(broker);

    let server = HttpServer::new(move || App::new()
        .app_data(broker_data.clone())
        .service(ping)
        .service(publish).service(publish_raw)
        .service(poll_batches_raw)
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