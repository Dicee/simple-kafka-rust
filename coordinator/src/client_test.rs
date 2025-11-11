use crate::model::GetTopicResponse;
use crate::{Client, ClientImpl};
use assertor::{assert_that, ResultAssertion};
use client_utils::{ApiClient, HttpClient, MockHttpClient};
use mockall::predicate::eq;
use std::collections::HashMap;
use std::sync::Mutex;
use ureq::http::Response;
use ureq::Body;

const TOPIC: &str = "topic";

#[test]
fn test_get_topic_is_cached() {
    let mut http_client = MockHttpClient::new();

    http_client.expect_get()
        .with(eq("http://domain/topics/topic"))
        .times(1)
        .returning(move |_| Ok(Response::builder()
            .status(200)
            .body(Body::builder().data("{\"name\":\"topic\",\"partition_count\":103}"))
            .unwrap()
        ));

    let http_client: Box<dyn HttpClient> = Box::new(http_client);
    let api_client = ApiClient::new_with_http_client("domain".to_owned(), false, false, http_client);
    let coordinator_client = ClientImpl {
        topics: Mutex::new(HashMap::new()),
        api_client,
    };

    let expected_response = GetTopicResponse { name: TOPIC.into(), partition_count: 103 };

    // called 4 times but mock HTTP client only called once
    assert_that!(coordinator_client.get_topic(TOPIC)).has_ok(expected_response.clone());
    assert_that!(coordinator_client.get_topic(TOPIC)).has_ok(expected_response.clone());
    assert_that!(coordinator_client.get_topic(TOPIC)).has_ok(expected_response.clone());
    assert_that!(coordinator_client.get_topic(TOPIC)).has_ok(expected_response);
}