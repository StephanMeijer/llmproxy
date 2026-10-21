use super::support::*;
use axum::{
    body::{Body, Bytes},
    http::StatusCode,
    response::Response,
};
use std::{
    io::{Read, Write},
    process::{Command, Stdio},
};

#[tokio::test]
async fn upstream_connection_failure_returns_502_and_releases_leases() {
    // Given a configured provider listener that has stopped.
    let mut f = Fixture::new().await;
    let a = f
        .binary(
            "[[limit]]\nname='native'\nprefixes=['/claude/']\nmax_inflight=1\n",
            "",
        )
        .await;
    f.stop_upstream().await;
    // When Claude attempts the upstream connection.
    let response = f.request(&a, false).send().await.unwrap();
    // Then the proxy returns502 and releases its admitted concurrency.
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    f.state_count("concurrency_leases", 0).await;
    artifact(
        "upstream-failure.txt",
        "PASS stopped provider listener returns502; admitted lease released; sanitized response/log\n",
    );
    f.close(vec![a]).await;
}

#[tokio::test]
async fn retry_beyond_deadline_preserves_provider_429_and_headers() {
    // Given a provider429 whose Retry-After exceeds the remaining request budget.
    let mut f = Fixture::new().await;
    let a = f.binary("", "").await;
    let request = tokio::spawn(f.request(&a, false).send());
    let hit = f.hit().await;
    let mut response = Response::new(Body::from("{\"error\":{\"type\":\"rate_limit_error\"}}"));
    *response.status_mut() = StatusCode::TOO_MANY_REQUESTS;
    response
        .headers_mut()
        .insert("retry-after", "60".parse().unwrap());
    // When the provider responds.
    assert!(hit.reply.send(response).is_ok());
    let response = request.await.unwrap().unwrap();
    // Then its status, error, and retry hint are preserved without retrying.
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(response.headers().get("retry-after").unwrap(), "60");
    assert_eq!(response.headers().get("x-proxy-retries").unwrap(), "0");
    assert_eq!(
        response.text().await.unwrap(),
        "{\"error\":{\"type\":\"rate_limit_error\"}}"
    );
    assert!(f.hits.try_recv().is_err());
    artifact(
        "429-native.txt",
        "PASS provider429 exact error and Retry-After60 preserved, zero retries\n",
    );
    f.close(vec![a]).await;
}

#[tokio::test]
async fn two_replicas_preserve_bytes_tools_headers_and_person_credentials() {
    // Given two real replicas sharing encrypted Alice/Bob credentials.
    let mut f = Fixture::new().await;
    let a = f.binary("", "").await;
    let b = f.binary("", "").await;
    // When each sends a native tool request with repeated beta headers and a hop header.
    let mut outgoing = f
        .request(&a, false)
        .header("anthropic-beta", "first")
        .header("anthropic-beta", "second")
        .header("connection", "x-private-hop")
        .header("x-private-hop", "remove-me")
        .build()
        .unwrap();
    outgoing.url_mut().set_query(Some("beta=one+two"));
    let alice = tokio::spawn(reqwest::Client::new().execute(outgoing));
    let bob = tokio::spawn(f.request(&b, true).send());
    for _ in 0..2 {
        let hit = f.hit().await;
        // Then the wire bytes and native fields survive, while proxy auth and hops do not.
        assert_eq!(hit.body.as_ref(), BODY.as_bytes());
        assert!(!hit.headers.contains_key("authorization"));
        assert!(!hit.headers.contains_key("x-private-hop"));
        assert_eq!(hit.headers.get("anthropic-version").unwrap(), "2023-06-01");
        assert!(hit.path.starts_with("/native/v1/messages"));
        let key = hit.headers.get("x-api-key").unwrap().as_bytes();
        if key == ALICE_KEY {
            assert_eq!(hit.path, "/native/v1/messages?beta=one+two");
            assert_eq!(hit.headers.get_all("anthropic-beta").iter().count(), 2);
        } else {
            assert!(key == BOB_KEY);
        }
        let mut response = Response::new(Body::from(
            "{\"usage\":{\"input_tokens\":7},\"content\":[{\"type\":\"tool_use\",\"id\":\"t1\"}]}",
        ));
        *response.status_mut() = StatusCode::CREATED;
        response
            .headers_mut()
            .insert("request-id", "native-id".parse().unwrap());
        response
            .headers_mut()
            .insert("connection", "x-response-hop".parse().unwrap());
        response
            .headers_mut()
            .insert("x-response-hop", "remove".parse().unwrap());
        response.headers_mut().insert(
            "anthropic-ratelimit-requests-remaining",
            "42".parse().unwrap(),
        );
        assert!(hit.reply.send(response).is_ok());
    }
    for response in [alice.await.unwrap().unwrap(), bob.await.unwrap().unwrap()] {
        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(response.headers()["x-proxy-route"], "claude");
        assert_eq!(response.headers()["request-id"], "native-id");
        assert_eq!(
            response.headers()["anthropic-ratelimit-requests-remaining"],
            "42"
        );
        assert!(!response.headers().contains_key("x-response-hop"));
        assert_eq!(
            response.text().await.unwrap(),
            "{\"usage\":{\"input_tokens\":7},\"content\":[{\"type\":\"tool_use\",\"id\":\"t1\"}]}"
        );
    }
    artifact(
        "native.txt",
        "PASS two binary replicas, Alice/Bob key isolation, byte-exact tool JSON, native repeated beta/version headers, query, 201/status/usage/rate headers, hop and auth stripping\n",
    );
    let url = reqwest::Url::parse(&f.db.config.url).unwrap();
    let dump = Command::new("docker")
        .args([
            "exec",
            &std::env::var("LLMPROXY_CREDENTIAL_CONTAINER").unwrap(),
            "pg_dump",
            "-U",
            "postgres",
            url.path().trim_start_matches('/'),
        ])
        .output()
        .unwrap();
    assert!(dump.status.success());
    for secret in [
        ALICE_KEY,
        BOB_KEY,
        f.alice.expose().as_bytes(),
        f.bob.expose().as_bytes(),
    ] {
        assert!(
            !dump
                .stdout
                .windows(secret.len())
                .any(|window| window == secret)
        );
    }
    artifact(
        "dump-scan.txt",
        "PASS pg_dump exit0; both fake provider keys and complete proxy tokens absent from database dump after live requests\n",
    );
    f.close(vec![a, b]).await;
}

#[tokio::test]
async fn curl_no_buffer_observes_sse_before_provider_completion() {
    const FIRST: &[u8] = b"event: content_block_delta\ndata: {\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"city\\\":\"}}\n\n";
    const LAST: &[u8] = b"event: future_event\ndata: {\"usage\":{\"output_tokens\":3}}\n\nevent: error\ndata: {\"error\":{\"type\":\"overloaded_error\"}}\n\n";
    // Given a real binary and a provider that waits between native SSE chunks.
    let mut f = Fixture::new().await;
    let a = f.binary("", "").await;
    let mut child = Command::new("curl")
        .args([
            "--silent",
            "--show-error",
            "--no-buffer",
            "--config",
            "-",
            "--data-binary",
            BODY,
            &format!("{}/claude/v1/messages", a.base),
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    writeln!(
        child.stdin.take().unwrap(),
        "header = \"Authorization: Bearer {}\"",
        f.alice.expose()
    )
    .unwrap();
    let hit = f.hit().await;
    let (send, _) = stream(hit);
    // When only the first chunk is available and the provider connection remains open.
    send.send(Bytes::from_static(FIRST)).await.unwrap();
    let mut stdout = child.stdout.take().unwrap();
    let read = tokio::task::spawn_blocking(move || {
        let mut first = vec![0; FIRST.len()];
        stdout.read_exact(&mut first).unwrap();
        (stdout, first)
    });
    let (mut stdout, first) = tokio::time::timeout(WAIT, read).await.unwrap().unwrap();
    // Then curl has already received byte-exact native tool deltas.
    assert_eq!(first, FIRST);
    send.send(Bytes::from_static(LAST)).await.unwrap();
    drop(send);
    let result = tokio::task::spawn_blocking(move || {
        let mut rest = Vec::new();
        stdout.read_to_end(&mut rest).unwrap();
        let status = child.wait().unwrap();
        (rest, status)
    })
    .await
    .unwrap();
    assert_eq!(result.0, LAST);
    assert!(result.1.success());
    artifact(
        "curl-sse.txt",
        &format!(
            "invocation: curl --silent --show-error --no-buffer --config - --data-binary <native body> <replica>/claude/v1/messages\nPASS first chunk received while provider open; curl exit 0\n{}{}",
            String::from_utf8_lossy(FIRST),
            String::from_utf8_lossy(LAST)
        ),
    );
    f.close(vec![a]).await;
}

#[tokio::test]
async fn retry_429_charges_model_only_shared_budget_once() {
    // Given a model-only request budget of one and two replicas.
    let mut f = Fixture::new().await;
    let limits = "[[limit]]\nname='model'\nmodel='claude-test'\nrpm=1\nburst=1\n";
    let a = f.binary(limits, "").await;
    // When the provider asks for one retry.
    let request = tokio::spawn(f.request(&a, false).send());
    let hit = f.hit().await;
    let mut response = Response::new(Body::from("busy"));
    *response.status_mut() = StatusCode::TOO_MANY_REQUESTS;
    response
        .headers_mut()
        .insert("retry-after", "0".parse().unwrap());
    assert!(hit.reply.send(response).is_ok());
    let hit = f.hit().await;
    reply(hit, StatusCode::OK, "done");
    let response = request.await.unwrap().unwrap();
    // Then the retry succeeds without another admission or charge.
    assert_eq!(response.headers()["x-proxy-retries"], "1");
    assert_eq!(response.text().await.unwrap(), "done");
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM rate_state")
        .fetch_one(&f.db.pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
    let queued: i64 = sqlx::query_scalar("SELECT next_enqueue FROM limit_definitions")
        .fetch_one(&f.db.pool)
        .await
        .unwrap();
    assert_eq!(queued, 1);
    artifact(
        "retry.txt",
        "PASS native 429 retried once, x-proxy-retries=1, one model-only shared rate row and next_enqueue=1\n",
    );
    f.close(vec![a]).await;
}
