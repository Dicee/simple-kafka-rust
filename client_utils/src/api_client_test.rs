use super::{ApiClient, MockHttpClient};
use crate::Error::{Api, Ureq};
use assertor::{assert_that, EqualityAssertion, ResultAssertion};
use mockall::predicate;
use serde::{Deserialize, Serialize};
use ureq::http::Response;
use ureq::Body;

const DOMAIN: &str = "localhost:5000";
const USE_TLS: bool = false;
const URI: &str = "http://localhost:5000/dummy";
const API: &str = "/dummy";
const DUMMY_REQUEST: DummyRequest = DummyRequest { some_int: 5 };

#[test]
fn test_client_post_success() {
    let mut http_client_mock = MockHttpClient::new();

    http_client_mock
        .expect_post()
        // I tried writing a predicate for SendBody, it wasn't possible because I cannot get ownership of it, and the only method allowing
        // to get the contents requires ownership
        .with(predicate::eq(URI), predicate::always())
        .times(1)
        .returning(move |_, _| Ok(Response::builder()
            .status(200)
            .body(Body::builder().data(Vec::new()))
            .unwrap()
        ));

    let client = new_client(http_client_mock);
    assert_that!(client.post(API, DUMMY_REQUEST)).has_ok(());
}

#[test]
fn test_client_post_success_use_tls() {
    let mut http_client_mock = MockHttpClient::new();

    http_client_mock
        .expect_post()
        // I tried writing a predicate for SendBody, it wasn't possible because I cannot get ownership of it, and the only method allowing
        // to get the contents requires ownership
        .with(predicate::eq("https://localhost:5000/dummy"), predicate::always())
        .times(1)
        .returning(move |_, _| Ok(Response::builder()
            .status(200)
            .body(Body::builder().data(Vec::new()))
            .unwrap()
        ));

    let client = ApiClient::new_with_http_client(DOMAIN.to_string(), true, Box::new(http_client_mock));
    assert_that!(client.post(API, DUMMY_REQUEST)).has_ok(());
}

#[test]
fn test_client_post_api_error() {
    let mut http_client_mock = MockHttpClient::new();
    let api_error_msg = "Some error occurred";

    http_client_mock
        .expect_post()
        // I tried writing a predicate for SendBody, it wasn't possible because I cannot get ownership of it, and the only method allowing
        // to get the contents requires ownership
        .with(predicate::eq(URI), predicate::always())
        .times(1)
        .returning(move |_, _| Ok(Response::builder()
            .status(400)
            .body(Body::builder().data(api_error_msg))
            .unwrap()
        ));

    let client = new_client(http_client_mock);
    match client.post(API, DUMMY_REQUEST).unwrap_err() {
        Api(msg) => assert_that!(msg).is_equal_to(api_error_msg.to_string()),
        _ => unreachable!(),
    }
}

#[test]
fn test_client_post_ureq_error() {
    let mut http_client_mock = MockHttpClient::new();

    http_client_mock
        .expect_post()
        // I tried writing a predicate for SendBody, it wasn't possible because I cannot get ownership of it, and the only method allowing
        // to get the contents requires ownership
        .with(predicate::eq(URI), predicate::always())
        .times(1)
        .returning(move |_, _| Err(ureq::Error::HostNotFound));

    let client = new_client(http_client_mock);
    match client.post(API, DUMMY_REQUEST).unwrap_err() {
        Ureq(ureq::Error::HostNotFound) => {},
        _ => unreachable!(),
    }
}

#[test]
fn test_client_get_with_response_body_success() {
    let mut http_client_mock = MockHttpClient::new();

    http_client_mock
        .expect_get()
        .with(predicate::eq(URI))
        .times(1)
        .returning(move |_| Ok(Response::builder()
            .status(200)
            .body(Body::builder().data("{\"some_short\":1278}"))
            .unwrap()
        ));

    let client = new_client(http_client_mock);
    assert_that!(client.get(API)).has_ok(DummyResponse { some_short: 1278 });
}

#[test]
fn test_client_get_with_response_body_invalid_body() {
    let mut http_client_mock = MockHttpClient::new();

    http_client_mock
        .expect_get()
        .with(predicate::eq(URI))
        .times(1)
        .returning(move |_| Ok(Response::builder()
            .status(200)
            .body(Body::builder().data("{\"name\":\"topic\"}"))
            .unwrap()
        ));

    let client = new_client(http_client_mock);

    match client.get::<DummyResponse>(API).unwrap_err() {
        Ureq(e) => assert_that!(e.to_string()).is_equal_to(String::from("json: missing field `some_short` at line 1 column 16")),
        _ => unreachable!(),
    }
}

fn new_client(http_client_mock: MockHttpClient) -> ApiClient {
    ApiClient::new_with_http_client(DOMAIN.to_string(), USE_TLS, Box::new(http_client_mock))
}

#[derive(Serialize)]
#[derive(Eq, PartialEq, Debug)]
struct DummyRequest {
    some_int: u32,
}

#[derive(Deserialize)]
#[derive(Eq, PartialEq, Debug)]
struct DummyResponse {
    some_short: u16,
}