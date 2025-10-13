use super::handle;
use crate::dao;
use assertor::{assert_that, EqualityAssertion};
use coordinator::dao::{InvalidOffset::*, Topic};
use serde::{Deserialize, Serialize};
use simple_server::{Request, ResponseBuilder, ResponseResult, StatusCode};

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct TestRequest {
    value: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct TestResponse {
    value: String,
}

#[test]
fn test_handle_success() {
    let request_body = serde_json::to_vec(&TestRequest { value: String::from("test") }).unwrap();
    let request = Request::new(request_body);
    let mut response_builder = ResponseBuilder::new();

    let result = handle(&request, &mut response_builder, |req: TestRequest| {
        Ok(TestResponse { value: req.value })
    });

    assert_that_response_is(result, StatusCode::OK, "{\"value\":\"test\"}")
}

#[test]
fn test_handle_request_deserialization_error() {
    let request_body = "{}".as_bytes().to_vec();
    let request = Request::new(request_body);
    let mut response_builder = ResponseBuilder::new();

    let result = handle(&request, &mut response_builder, |_: TestRequest| {
        Ok(TestResponse { value: "".to_string() })
    });

    assert_that_response_is(result, StatusCode::BAD_REQUEST, "{\"status_code\":400,\"message\":\"missing field `value` at line 1 column 2\"}");
}

#[test]
fn test_handle_dao_error_topic_not_found() {
    let result = test_handle_dao_error_base(dao::Error::TopicNotFound(String::from("topic")));
    assert_that_response_is(result, StatusCode::NOT_FOUND, "{\"status_code\":404,\"message\":\"Topic not found: topic\"}");
}

#[test]
fn test_handle_dao_error_topic_already_exists() {
    let result = test_handle_dao_error_base(dao::Error::TopicAlreadyExists(String::from("topic")));
    assert_that_response_is(result, StatusCode::BAD_REQUEST, "{\"status_code\":400,\"message\":\"Topic already exists: topic\"}");
}

#[test]
fn test_handle_dao_error_out_of_range_partition() {
    let result = test_handle_dao_error_base(dao::Error::OutOfRangePartition {
        topic: Topic { name: String::from("topic"), partition_count: 128 },
        invalid_partition: 200,
    });
    assert_that_response_is(result, StatusCode::BAD_REQUEST, "{\"status_code\":400,\"message\":\"Invalid partition 200 for topic topic, cannot exceed 128\"}");
}

#[test]
fn test_handle_dao_error_invalid_read_offset_too_large() {
    let result = test_handle_dao_error_base(dao::Error::InvalidReadOffset(TooLarge { max_offset: 125, invalid_offset: 200 }));
    assert_that_response_is(result, StatusCode::BAD_REQUEST, "{\"status_code\":400,\"message\":\"Invalid read offset 200: it exceeds the maximum written offset of 125.\"}");
}

#[test]
fn test_handle_dao_error_invalid_read_offset_no_data_written() {
    let result = test_handle_dao_error_base(dao::Error::InvalidReadOffset(NoDataWritten));
    assert_that_response_is(result, StatusCode::BAD_REQUEST, "{\"status_code\":400,\"message\":\"Invalid read offset: no data written has been written yet\"}");
}

#[test]
fn test_handle_dao_error_internal() {
    let result = test_handle_dao_error_base(dao::Error::Internal(String::from("test error")));
    assert_that_response_is(result, StatusCode::INTERNAL_SERVER_ERROR, "{\"status_code\":500,\"message\":\"test error\"}");
}

fn test_handle_dao_error_base(error: dao::Error) -> ResponseResult {
    let request_body = serde_json::to_vec(&TestRequest { value: "test".to_string() }).unwrap();
    let request = Request::new(request_body);
    let mut response_builder = ResponseBuilder::new();

    handle(&request, &mut response_builder, |_: TestRequest| {
        Err::<String, dao::Error>(error)
    })
}

fn assert_that_response_is(result: ResponseResult, status_code: StatusCode, expected_body: &str) {
    let response = result.unwrap();
    let response_body = String::from_utf8(response.body().clone()).unwrap();

    assert_that!(response.status()).is_equal_to(status_code);
    assert_that!(response_body).is_equal_to(String::from(expected_body));
}