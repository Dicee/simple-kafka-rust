use crate::{Client, ClientImpl};
use client_utils::{ApiClient, HttpClient, MockHttpClient};
use std::collections::HashMap;
use std::sync::Mutex;
use actix_web::HttpResponse;
use assertor::{assert_that, ResultAssertion};
use mockall::predicate::eq;
use ureq::Body;
use ureq::http::Response;
use crate::model::{GetTopicRequest, GetTopicResponse};

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

    let expected_response = GetTopicResponse { name: "topic".to_owned(), partition_count: 103 };

    // called 4 times but mock HTTP client only called once
    assert_that!(coordinator_client.get_topic(GetTopicRequest { name: "topic".to_owned() })).has_ok(expected_response.clone());
    assert_that!(coordinator_client.get_topic(GetTopicRequest { name: "topic".to_owned() })).has_ok(expected_response.clone());
    assert_that!(coordinator_client.get_topic(GetTopicRequest { name: "topic".to_owned() })).has_ok(expected_response.clone());
    assert_that!(coordinator_client.get_topic(GetTopicRequest { name: "topic".to_owned() })).has_ok(expected_response);
}