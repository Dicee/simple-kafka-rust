use crate::client::Error::*;
use crate::client::{Client, MockHttpClient};
use crate::model::*;
use assertor::{assert_that, EqualityAssertion, ResultAssertion};
use mockall::predicate;
use ureq::http::Response;
use ureq::Body;

const DOMAIN: &str = "localhost:5000";
const USE_TLS: bool = false;

#[test]
fn test_client_post_success() {
    let mut http_client_mock = MockHttpClient::new();

    http_client_mock
        .expect_post()
        .with(predicate::eq("http://localhost:5000/create-topic"), predicate::eq("{\"name\":\"topic\",\"partition_count\":128}"))
        .times(1)
        .returning(move |_, _| Ok(Response::builder()
            .status(200)
            .body(Body::builder().data(Vec::new()))
            .unwrap()
        ));

    let client = new_client(http_client_mock);
    assert_that!(client.create_topic(CreateTopicRequest { name: String::from("topic"), partition_count: 128 })).has_ok(());
}

#[test]
fn test_client_post_success_use_tls() {
    let mut http_client_mock = MockHttpClient::new();

    http_client_mock
        .expect_post()
        .with(predicate::eq("https://localhost:5000/create-topic"), predicate::eq("{\"name\":\"topic\",\"partition_count\":128}"))
        .times(1)
        .returning(move |_, _| Ok(Response::builder()
            .status(200)
            .body(Body::builder().data(Vec::new()))
            .unwrap()
        ));

    let client = Client::new_with_http_client(DOMAIN.to_string(), true, Box::new(http_client_mock));
    assert_that!(client.create_topic(CreateTopicRequest { name: String::from("topic"), partition_count: 128 })).has_ok(());
}

#[test]
fn test_client_post_api_error() {
    let mut http_client_mock = MockHttpClient::new();
    let api_error_msg = "Some error occurred";

    http_client_mock
        .expect_post()
        .with(predicate::eq("http://localhost:5000/create-topic"), predicate::eq("{\"name\":\"topic\",\"partition_count\":128}"))
        .times(1)
        .returning(move |_, _| Ok(Response::builder()
            .status(400)
            .body(Body::builder().data(api_error_msg))
            .unwrap()
        ));

    let client = new_client(http_client_mock);
    let response = client.create_topic(CreateTopicRequest { name: String::from("topic"), partition_count: 128 });

    match response.unwrap_err() {
        Api(msg) => assert_that!(msg).is_equal_to(api_error_msg.to_string()),
        _ => unreachable!(),
    }
}

#[test]
fn test_client_post_ureq_error() {
    let mut http_client_mock = MockHttpClient::new();

    http_client_mock
        .expect_post()
        .with(predicate::eq("http://localhost:5000/create-topic"), predicate::eq("{\"name\":\"topic\",\"partition_count\":128}"))
        .times(1)
        .returning(move |_, _| Err(ureq::Error::HostNotFound));

    let client = new_client(http_client_mock);
    let response = client.create_topic(CreateTopicRequest { name: String::from("topic"), partition_count: 128 });

    match response.unwrap_err() {
        Ureq(ureq::Error::HostNotFound) => {},
        _ => unreachable!(),
    }
}

#[test]
fn test_client_post_with_response_body_success() {
    let mut http_client_mock = MockHttpClient::new();

    http_client_mock
        .expect_post()
        .with(predicate::eq("http://localhost:5000/get-topic"), predicate::eq("{\"name\":\"topic\"}"))
        .times(1)
        .returning(move |_, _| Ok(Response::builder()
            .status(200)
            .body(Body::builder().data("{\"name\":\"topic\",\"partition_count\":128}"))
            .unwrap()
        ));

    let client = new_client(http_client_mock);
    assert_that!(client.get_topic(GetTopicRequest { name: String::from("topic") }))
        .has_ok(GetTopicResponse {
            name: String::from("topic"),
            partition_count: 128,
        });
}

#[test]
fn test_client_post_with_response_body_invalid_body() {
    let mut http_client_mock = MockHttpClient::new();

    http_client_mock
        .expect_post()
        .with(predicate::eq("http://localhost:5000/get-topic"), predicate::eq("{\"name\":\"topic\"}"))
        .times(1)
        .returning(move |_, _| Ok(Response::builder()
            .status(200)
            .body(Body::builder().data("{\"name\":\"topic\"}"))
            .unwrap()
        ));

    let client = new_client(http_client_mock);
    let response = client.get_topic(GetTopicRequest { name: String::from("topic") });

    match response.unwrap_err() {
        Serde(e) => assert_that!(e.to_string()).is_equal_to(String::from("missing field `partition_count` at line 1 column 16")),
        _ => unreachable!(),
    }
}

fn new_client(http_client_mock: MockHttpClient) -> Client {
    Client::new_with_http_client(DOMAIN.to_string(), USE_TLS, Box::new(http_client_mock))
}