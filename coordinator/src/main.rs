use crate::dao::Error::*;
use crate::dao::{Error, MetadataAndStateDao};
use crate::registry::BrokerRegistry;
use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder};
use argh::FromArgs;
use coordinator::model::*;
use std::path::Path;
use std::sync::Arc;

mod dao;
mod registry;

#[derive(FromArgs)]
/// Starts a coordinator server on the given port, with the given hostname.
struct Args {
    /// hostname to use for the server
    #[argh(option, default = "String::from(\"localhost\")")]
    host: String,

    /// port to use for the server
    #[argh(option)]
    port: u16,

    /// path of the SQLite file used as database
    #[argh(option)]
    db_path: String,
}

// used for the health check to start the Docker container of broker instances only when the
// coordinator is ready to receive registration requests
#[get("/ping")]
async fn ping() -> impl Responder { "{\"ping\": \"pong\"}" }

#[post("/create-topic")]
async fn create_topic(
    dao: web::Data<Arc<MetadataAndStateDao>>,
    request: web::Json<CreateTopicRequest>,
) -> impl Responder {
    match dao.create_topic(&request.name, request.partition_count) {
        Ok(_) => HttpResponse::NoContent().body(""),
        Err(e) => convert_dao_error(e)
    }
}

#[get("/topics/{topic}")]
async fn get_topic(
    dao: web::Data<Arc<MetadataAndStateDao>>,
    path: web::Path<String>,
) -> impl Responder {
    match dao.get_topic(&path) {
        Ok(topic) => HttpResponse::Ok().json(GetTopicResponse { name: topic.name, partition_count: topic.partition_count }),
        Err(e) => convert_dao_error(e)
    }
}

#[post("/increment-write-offset")]
async fn inc_write_offset(
    dao: web::Data<Arc<MetadataAndStateDao>>,
    request: web::Json<IncrementWriteOffsetRequest>,
) -> impl Responder {
    match dao.inc_write_offset_by(&request.topic, request.partition, request.inc) {
        Ok(_) => HttpResponse::NoContent().body(""),
        Err(e) => convert_dao_error(e)
    }
}

#[get("/topics/{topic}/partitions/{partition}/write-offset")]
async fn get_write_offset(
    dao: web::Data<Arc<MetadataAndStateDao>>,
    path: web::Path<TopicPartition>,
) -> impl Responder {
    match dao.get_write_offset(&path.topic, path.partition) {
        Ok(offset) => HttpResponse::Ok().json(GetWriteOffsetResponse { offset }),
        Err(e) => convert_dao_error(e)
    }
}

#[post("/ack-read-offset")]
async fn ack_read_offset(
    dao: web::Data<Arc<MetadataAndStateDao>>,
    request: web::Json<AckReadOffsetRequest>,
) -> impl Responder {
    let AckReadOffsetRequest { topic, partition, consumer_group, offset } = request.into_inner();
    match dao.ack_read_offset(&topic, partition, &consumer_group, offset) {
        Ok(_) => HttpResponse::NoContent().body(""),
        Err(e) => convert_dao_error(e)
    }
}

#[get("/topics/{topic}/partitions/{partition}/consumer-groups/{consumer_group}/read-offset")]
async fn get_read_offset(
    dao: web::Data<Arc<MetadataAndStateDao>>,
    path: web::Path<TopicPartitionConsumer>,
) -> impl Responder {
    match dao.get_read_offset(&path.topic, path.partition, &path.consumer_group) {
        Ok(offset) => HttpResponse::Ok().json(GetReadOffsetResponse { offset }),
        Err(e) => convert_dao_error(e)
    }
}

#[post("/register-broker")]
async fn register_broker(
    broker_registry: web::Data<Arc<BrokerRegistry>>,
    request: web::Json<RegisterBrokerRequest>,
) -> impl Responder {
    let RegisterBrokerRequest { host, port } = request.into_inner();
    broker_registry.register_broker(HostAndPort { host, port });
    HttpResponse::NoContent().body("")
}

#[get("/brokers")]
async fn list_brokers(
    broker_registry: web::Data<Arc<BrokerRegistry>>,
) -> impl Responder {
    // regrettable but it's a mock API anyway so no need to be fancy, cloning will be alright
    let brokers = broker_registry.brokers().iter().map(HostAndPort::clone).collect();
    HttpResponse::Ok().json(ListBrokersResponse { brokers })
}

fn convert_dao_error(e: Error) -> HttpResponse {
    match e {
        // I prefer explicitly listing all 4xx rather than using _ so that we can visually confirm that indeed, all of them are 4xx
        TopicAlreadyExists(_) | TopicNotFound(_) | OutOfRangePartition { .. } | InvalidReadOffset(_) => HttpResponse::BadRequest().body(e.to_string()),
        Internal(msg) => HttpResponse::InternalServerError().body(msg),
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let args: Args = argh::from_env();
    let dao = web::Data::new(Arc::new(MetadataAndStateDao::new(Path::new(&args.db_path)).unwrap()));
    let broker_registry = web::Data::new(Arc::new(BrokerRegistry::new()));

    let server = HttpServer::new(move || App::new()
        .app_data(dao.clone())
        .app_data(broker_registry.clone())
        .service(ping)
        .service(create_topic).service(get_topic)
        .service(inc_write_offset).service(get_write_offset)
        .service(ack_read_offset).service(get_read_offset)
        .service(register_broker).service(list_brokers)
    );
    server
        .workers(5)
        .bind((args.host, args.port))?
        .run()
        .await
}
