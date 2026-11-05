#![allow(clippy::unwrap_used, clippy::expect_used)]

#[path = "auth/binary.rs"]
mod binary;
#[path = "common/database.rs"]
mod database;
#[path = "common/database_close.rs"]
mod database_close;
#[path = "auth/http.rs"]
mod http;
#[path = "auth/support.rs"]
mod support;

use axum::http::{HeaderMap, HeaderValue};
use llmproxy::{
    auth::{AuthError, Authenticator, Principal, RouteKind},
    secret::ProxyToken,
    storage::Storage,
};
use std::time::Duration;
use support::{Fixture, LEGACY};

const WAIT: Duration = Duration::from_secs(30);

#[tokio::test]
async fn route_token_database_matrix_preserves_person_identity() {
    let fixture = Fixture::new().await;
    let unavailable = fixture.unavailable_config().await;
    for (available, postgres) in [(true, &fixture.db.config), (false, &unavailable)] {
        let auth = Authenticator::new(LEGACY.into(), Some(Storage::new(postgres).unwrap()), WAIT);
        let (base, _servers) = http::start(postgres).await;
        let client = reqwest::Client::new();
        for route in [
            RouteKind::Health,
            RouteKind::Static,
            RouteKind::Claude,
            RouteKind::Unknown,
        ] {
            for (name, token, identity) in [
                ("missing", None, None),
                ("legacy", Some(LEGACY), Some(Principal::LegacyStatic)),
                (
                    "alice",
                    Some(fixture.alice.expose()),
                    Some(Principal::Person(fixture.alice_id)),
                ),
                (
                    "bob",
                    Some(fixture.bob.expose()),
                    Some(Principal::Person(fixture.bob_id)),
                ),
                ("invalid", Some(fixture.invalid.expose()), None),
                ("revoked", Some(fixture.revoked.expose()), None),
                ("disabled", Some(fixture.disabled.expose()), None),
                ("malformed", Some("malformed"), None),
                ("conflicting", Some(LEGACY), None),
            ] {
                let mut headers = HeaderMap::new();
                if let Some(token) = token {
                    headers.insert(
                        "authorization",
                        HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
                    );
                }
                if name == "conflicting" {
                    headers.insert("x-api-key", HeaderValue::from_static("other"));
                }
                headers.insert(
                    "x-person-id",
                    HeaderValue::from_str(&fixture.bob_id.0.to_string()).unwrap(),
                );
                let expected = if route == RouteKind::Health {
                    Ok(None)
                } else if ["missing", "conflicting", "malformed"].contains(&name) {
                    Err(AuthError::Unauthorized)
                } else if name == "legacy" {
                    if route == RouteKind::Claude {
                        Err(AuthError::Forbidden)
                    } else {
                        Ok(identity)
                    }
                } else if !available {
                    Err(AuthError::Unavailable)
                } else if identity.is_some() {
                    Ok(identity)
                } else {
                    Err(AuthError::Unauthorized)
                };
                let actual = auth.authenticate(route, &headers).await;
                assert_eq!(
                    actual, expected,
                    "database={available} route={route:?} token={name}"
                );
                let (path, success) = match route {
                    RouteKind::Health => ("/healthz", 200),
                    RouteKind::Static => ("/static/models", 200),
                    RouteKind::Claude => ("/claude/v1/messages", 503),
                    RouteKind::Unknown => ("/unknown", 404),
                };
                let status = expected.map_or_else(|error| error.status().as_u16(), |_| success);
                let response = client
                    .request(
                        if route == RouteKind::Claude {
                            reqwest::Method::POST
                        } else {
                            reqwest::Method::GET
                        },
                        format!("{base}{path}"),
                    )
                    .headers(headers)
                    .send()
                    .await
                    .unwrap();
                assert_eq!(
                    response.status().as_u16(),
                    status,
                    "database={available} route={route:?} token={name}"
                );
                println!(
                    "matrix database={available} route={route:?} token={name} status={status} result={actual:?}"
                );
            }
        }
    }
    fixture.close().await;
}

#[tokio::test]
async fn ambiguous_headers_fail_before_database_access() {
    let auth = Authenticator::new(LEGACY.into(), None, WAIT);
    for headers in [
        vec![("authorization", "Bearer legacy"), ("x-api-key", "legacy")],
        vec![
            ("authorization", "Bearer legacy"),
            ("authorization", "Bearer legacy"),
        ],
        vec![("authorization", "Basic legacy"), ("x-api-key", "legacy")],
        vec![("x-api-key", "legacy"), ("x-api-key", "legacy")],
        vec![("authorization", "Bearer ")],
        vec![("authorization", "Bearer legacy ")],
    ] {
        let expected =
            if headers == vec![("authorization", "Bearer legacy"), ("x-api-key", "legacy")] {
                Ok(Some(Principal::LegacyStatic))
            } else {
                Err(AuthError::Unauthorized)
            };
        let mut map = HeaderMap::new();
        for (name, value) in headers {
            map.append(name, HeaderValue::from_str(value).unwrap());
        }
        assert_eq!(auth.authenticate(RouteKind::Static, &map).await, expected);
    }
}

#[tokio::test]
async fn wrong_secret_and_unusable_hash_fail_closed() {
    let fixture = Fixture::new().await;
    let auth = Authenticator::new(LEGACY.into(), Some(fixture.storage.clone()), WAIT);
    let wrong = ProxyToken::parse(&format!("{}.{}", fixture.alice.id(), "0".repeat(64))).unwrap();
    let mut headers = HeaderMap::new();
    headers.insert("x-api-key", HeaderValue::from_str(wrong.expose()).unwrap());
    assert_eq!(
        auth.authenticate(RouteKind::Static, &headers).await,
        Err(AuthError::Unauthorized)
    );
    sqlx::query("UPDATE proxy_tokens SET secret_hash='$argon2id$invalid' WHERE id=$1")
        .bind(fixture.alice.id())
        .execute(&fixture.db.pool)
        .await
        .unwrap();
    headers.insert(
        "x-api-key",
        HeaderValue::from_str(fixture.alice.expose()).unwrap(),
    );
    assert_eq!(
        auth.authenticate(RouteKind::Static, &headers).await,
        Err(AuthError::Unavailable)
    );
    sqlx::query("DROP TABLE proxy_tokens")
        .execute(&fixture.db.pool)
        .await
        .unwrap();
    assert_eq!(
        auth.authenticate(RouteKind::Static, &headers).await,
        Err(AuthError::Unavailable)
    );
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn overlapping_person_tokens_wait_for_a_verifier_instead_of_failing() {
    // Given two valid person tokens and more simultaneous requests than verifier slots.
    let fixture = Fixture::new().await;
    let auth = Authenticator::new(LEGACY.into(), Some(fixture.storage.clone()), WAIT);
    let tokens = [
        (fixture.alice.expose(), fixture.alice_id),
        (fixture.bob.expose(), fixture.bob_id),
    ];
    // When eight requests authenticate at once.
    let requests: Vec<_> = tokens.iter().cycle().take(8).collect();
    let results = futures_util::future::join_all(requests.iter().map(|(token, _)| {
        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", HeaderValue::from_str(token).unwrap());
        let auth = &auth;
        async move { auth.authenticate(RouteKind::Static, &headers).await }
    }))
    .await;
    // Then every one is authenticated as its own person; none is told the database is down.
    for (n, (result, (_, person))) in results.into_iter().zip(requests).enumerate() {
        assert_eq!(result, Ok(Some(Principal::Person(*person))), "request {n}");
    }
    fixture.close().await;
}

#[tokio::test]
async fn real_binary_routes_authentication_and_keeps_legacy_local() {
    binary::binary_scenario().await;
}
