use super::{
    fixture::{Fixture, REPLACEMENT},
    process,
    provider::{reply, stream},
};
use axum::{body::Bytes, http::StatusCode};
use serde_json::json;
use tokio::task::JoinSet;

const FIRST: &[u8] = b"event: content_block_delta\ndata: {\"delta\":{\"partial_json\":\"{\"}}\n\n";

#[tokio::test]
async fn provisioning_rotation_restart_preserve_native_tools_and_sse() {
    // Given CLI-provisioned people and two real replicas in one namespace.
    let mut f = Fixture::new("lifecycle").await;
    let a = f.binary("").await;
    let b = f.binary("").await;
    let mut clients = JoinSet::new();
    // When JSON and SSE requests traverse the two independent replicas.
    clients.spawn(f.request(&a, 0).send());
    let hit = f.hit().await;
    assert!(hit.body.as_ref() == super::BODY.as_bytes());
    reply(
        hit,
        StatusCode::OK,
        r#"{"content":[{"type":"tool_use","id":"t1"}],"usage":{"input_tokens":7}}"#,
    );
    let response = clients.join_next().await.unwrap().unwrap().unwrap();
    assert_eq!(response.headers().get("x-proxy-route").unwrap(), "claude");
    let body: serde_json::Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    assert_eq!(body.pointer("/usage/input_tokens").unwrap(), 7);
    clients.spawn(f.request(&b, 1).send());
    let send = stream(f.hit().await);
    send.send(Bytes::from_static(FIRST)).await.unwrap();
    let mut response = clients.join_next().await.unwrap().unwrap().unwrap();
    assert_eq!(response.chunk().await.unwrap().unwrap(), FIRST);
    drop(send);
    assert!(response.chunk().await.unwrap().is_none());
    // Then replacement and re-encryption survive removing the old key and restarting.
    let replaced = process::ok(
        &f.config,
        &[
            "auth",
            "claude",
            "set",
            f.people.first().unwrap(),
            "--credential-stdin",
        ],
        Some(REPLACEMENT),
    );
    assert_eq!(replaced.get("generation").unwrap(), 2);
    let rotated = process::ok(
        &f.config,
        &["auth", "credentials", "reencrypt", "--to", "new"],
        None,
    );
    assert_eq!(rotated.get("rewritten").unwrap(), 2);
    let config = a.config.clone();
    drop(a);
    let text = std::fs::read_to_string(&config)
        .unwrap()
        .replace("active_key_id='old'", "active_key_id='new'");
    let old_entry = format!("old='{}',", f.db.directory.join("old").display());
    std::fs::write(&config, text.replace(&old_entry, "")).unwrap();
    std::fs::remove_file(f.db.directory.join("old")).unwrap();
    let restarted = process::Replica::start(&config).await;
    clients.spawn(f.request(&restarted, 0).send());
    reply(f.hit().await, StatusCode::OK, "rotated");
    assert_eq!(
        clients
            .join_next()
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .text()
            .await
            .unwrap(),
        "rotated"
    );
    f.static_ok(&restarted).await;
    assert_eq!(f.order, ["alice", "bob", "replacement"]);
    f.record(json!({"json_status":200,"sse_first_before_close":true,"rotation_rewritten":2,"restart_status":200})).await;
}

#[tokio::test]
async fn provider_errors_invalid_credentials_retry_and_replacement_are_isolated() {
    // Given separate Alice and Bob credentials on two real replicas.
    let mut f = Fixture::new("provider-errors").await;
    let a = f.binary("").await;
    let b = f.binary("").await;
    let mut clients = JoinSet::new();
    // When Alice's old generation fails after replacement, Bob retries a 429.
    clients.spawn(f.request(&a, 0).send());
    let old = f.hit().await;
    process::ok(
        &f.config,
        &[
            "auth",
            "claude",
            "set",
            f.people.first().unwrap(),
            "--credential-stdin",
        ],
        Some(REPLACEMENT),
    );
    reply(old, StatusCode::UNAUTHORIZED, "old-key-invalid");
    assert_eq!(
        clients
            .join_next()
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    clients.spawn(f.request(&b, 1).send());
    reply(f.hit().await, StatusCode::TOO_MANY_REQUESTS, "busy");
    reply(f.hit().await, StatusCode::OK, "retried");
    let response = clients.join_next().await.unwrap().unwrap().unwrap();
    assert_eq!(response.headers().get("x-proxy-retries").unwrap(), "1");
    assert_eq!(response.text().await.unwrap(), "retried");
    clients.spawn(f.request(&a, 0).send());
    reply(f.hit().await, StatusCode::UNAUTHORIZED, "new-key-invalid");
    assert_eq!(
        clients
            .join_next()
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    // Then invalid current credentials fail before provider and Bob remains available.
    assert_eq!(
        f.request(&b, 0).send().await.unwrap().status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    clients.spawn(f.request(&b, 1).send());
    reply(f.hit().await, StatusCode::FORBIDDEN, "forbidden");
    assert_eq!(
        clients
            .join_next()
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    clients.spawn(f.request(&a, 1).send());
    reply(f.hit().await, StatusCode::OK, "bob-still-active");
    assert_eq!(
        clients
            .join_next()
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert!(f.provider.hits.try_recv().is_err());
    f.record(json!({"old_401_replacement_survived":true,"retry_count":1,"invalid_current_status":503,"bob_after_403":200})).await;
}
