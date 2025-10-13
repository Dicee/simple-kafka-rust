use argh::FromArgs;
use simple_server::{Builder, Request, ResponseBuilder, ResponseResult, Server, StatusCode};
use std::fmt::Display;
use std::path::Path;

use coordinator::dao::Error::{Internal, TopicNotFound};
use coordinator::dao::{self, Dao};
use serde::{Deserialize, Serialize};

#[cfg(test)]
mod main_test;

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

fn main() {
    let args: Args = argh::from_env();
    let dao = Dao::new(Path::new(&args.db_path)).unwrap();

    let server = Server::new(move |request, mut response| {
        let uri = request.uri();

        let response: ResponseResult = match uri.path() {
            "/create-topic" => 
                handle(&request, &mut response, |r: CreateTopicRequest| dao.create_topic(&r.name, r.partition_count)),
            "/get-topic" =>
                handle(&request, &mut response, |r: GetTopicRequest| dao.get_topic(&r.name)
                    .map(|topic| GetTopicResponse { name: topic.name, partition_count: topic.partition_count})
                ),
            "/increment-write-offset" =>
                handle(&request, &mut response, |r: IncrementWriteOffsetRequest| dao.inc_write_offset_by(&r.topic, r.partition, r.inc)),
            "/get-write-offset" =>
                handle(&request, &mut response, |r: GetWriteOffsetRequest| dao.get_write_offset(&r.topic, r.partition)
                    .map(|offset| GetWriteOffsetResponse { offset })
                ),
            "/ack-read-offset" =>
                handle(&request, &mut response, |r: AckReadOffsetRequest| dao.ack_read_offset(&r.topic, r.partition, &r.consumer_group, r.offset)),
            "/get-read-offset" =>
                handle(&request, &mut response, |r: GetReadOffsetRequest| dao.get_read_offset(&r.topic, r.partition, &r.consumer_group)
                    .map(|offset| GetReadOffsetResponse { offset })
                ),
            _ => {
                handle_error(&mut response, format!("Unknown API {uri}"), StatusCode::NOT_FOUND)
            },
        };
        response
    });
    server.listen(&args.host, &args.port.to_string());
}

fn handle<Req, Res, H>(request: &Request<Vec<u8>>, response: &mut ResponseBuilder, do_handle: H) -> ResponseResult
where
    Req: for<'de> Deserialize<'de>,
    Res: Serialize,
    H: FnOnce(Req) -> dao::Result<Res>,
{
    let request = match serde_json::from_slice::<Req>(request.body()) {
        Ok(request) => request,
        Err(e) => {
            return handle_error(response, e.to_string(), StatusCode::BAD_REQUEST);
        }
    };

    let result = match do_handle(request) {
        Ok(r) => r,
        Err(e) => return handle_dao_error(response, e)
    };
    let response_bytes = match serde_json::to_vec(&result) {
        Ok(rb) => rb,
        Err(e) => return handle_unexpected_error(response, e),
    };

    response.status(StatusCode::OK);
    Ok(response.body(response_bytes)?)
}

fn handle_dao_error(response: &mut Builder, error: dao::Error,) -> ResponseResult {
    match error {
        Internal(message) => handle_unexpected_error(response, message),
        TopicNotFound(_) => handle_error(response, error.to_string(), StatusCode::NOT_FOUND),
        _ => handle_error(response, error.to_string(), StatusCode::BAD_REQUEST),
    }
}

fn handle_unexpected_error<E : Display>(response: &mut Builder, error: E) -> ResponseResult {
    handle_error(response, error.to_string(), StatusCode::INTERNAL_SERVER_ERROR)
}

fn handle_error(response: &mut Builder, error_msg: String, status_code: StatusCode) -> ResponseResult {
    let response_bytes = serde_json::to_vec(&ErrorResponse {
        status_code: status_code.as_u16(),
        message: error_msg,
    }).unwrap_or_else(|_| b"Failed to serialize error response".to_vec());

    response.status(status_code);
    Ok(response.body(response_bytes)?)
}

#[derive(Serialize, Deserialize)]
#[derive(Debug, PartialEq, Eq)]
struct CreateTopicRequest {
    name: String,
    partition_count: u32,
}

#[derive(Serialize, Deserialize)]
#[derive(Debug, PartialEq, Eq)]
struct GetTopicRequest {
    name: String,
}

#[derive(Serialize, Deserialize)]
#[derive(Debug, PartialEq, Eq)]
struct GetTopicResponse {
    name: String,
    partition_count: u32,
}

#[derive(Serialize, Deserialize)]
#[derive(Debug, PartialEq, Eq)]
struct IncrementWriteOffsetRequest {
    topic: String,
    partition: u32,
    inc: u32,
}

#[derive(Serialize, Deserialize)]
#[derive(Debug, PartialEq, Eq)]
struct GetWriteOffsetRequest {
    topic: String,
    partition: u32,
}

#[derive(Serialize, Deserialize)]
#[derive(Debug, PartialEq, Eq)]
struct GetWriteOffsetResponse {
    offset: Option<u64>,
}

#[derive(Serialize, Deserialize)]
#[derive(Debug, PartialEq, Eq)]
struct AckReadOffsetRequest {
    topic: String,
    partition: u32,
    consumer_group: String,
    offset: u64,
}

#[derive(Serialize, Deserialize)]
#[derive(Debug, PartialEq, Eq)]
struct GetReadOffsetRequest {
    topic: String,
    partition: u32,
    consumer_group: String,
}

#[derive(Serialize, Deserialize)]
#[derive(Debug, PartialEq, Eq)]
struct GetReadOffsetResponse {
    offset: Option<u64>,
}

#[derive(Serialize, Deserialize)]
#[derive(Debug, PartialEq, Eq)]
struct ErrorResponse {
    status_code: u16,
    message: String,
}
