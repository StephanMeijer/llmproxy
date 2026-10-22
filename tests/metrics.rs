#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use axum::Router;
use llmproxy::{
    config::Config,
    metrics::{CredentialCounts, CredentialOperation, CredentialOutcome, CredentialProvider},
    proxy::{Metrics, Proxy, metrics_router, router},
};
use tokio::{net::TcpListener, task::JoinSet};

const PROVIDER: CredentialProvider = CredentialProvider::Anthropic;
const CANARIES: [&str; 8] = [
    "person.[alice]*",
    "token.+^$[secret]",
    "namespace.(private)",
    "host.[private].invalid",
    "key-id.[secret]",
    "error.(private)*",
    "model.[private]+",
    "mail+[private]@example.invalid",
];
const TIMEOUT: Duration = Duration::from_secs(10);
const ACTIVE: CredentialCounts = CredentialCounts {
    active: 1,
    invalid: 0,
    removed: 0,
    unavailable: 0,
};
const INVALID: CredentialCounts = CredentialCounts {
    active: 0,
    invalid: 1,
    ..ACTIVE
};
const REMOVED: CredentialCounts = CredentialCounts {
    active: 0,
    removed: 1,
    ..ACTIVE
};

#[test]
fn exposes_metadata_without_inventing_unobserved_counts() {
    // Given an unobserved database and credential store.
    let metrics = Metrics::default();
    // When rendered.
    let text = metrics.render();
    // Then metadata is present, availability is zero and unknown counts are absent.
    for (name, kind) in [
        ("llm3_postgres_available", "gauge"),
        ("llm3_provider_credentials", "gauge"),
        ("llm3_credential_operations_total", "counter"),
    ] {
        assert!(text.contains(&format!("# HELP {name} ")));
        assert!(text.contains(&format!("# TYPE {name} {kind}\n")));
    }
    assert!(text.contains("llm3_postgres_available 0\n"));
    assert!(!text.contains("llm3_provider_credentials{"));
    assert!(!text.contains("llm3_credential_operations_total{"));
}

#[test]
fn snapshots_replace_counts_and_include_unavailable_credentials() {
    // Given a previous larger snapshot.
    let metrics = Metrics::default();
    let credentials = metrics.credentials();
    credentials.set_credentials(
        PROVIDER,
        CredentialCounts {
            active: 42,
            ..Default::default()
        },
    );
    // When the same complete newer snapshot is applied twice.
    for _ in 0..2 {
        credentials.set_credentials(
            PROVIDER,
            CredentialCounts {
                active: 2,
                invalid: 3,
                removed: 4,
                unavailable: 5,
            },
        );
    }
    // Then each state has its exact count, with no accumulated duplicates.
    let text = metrics.render();
    for (state, count) in [
        ("active", 2),
        ("invalid", 3),
        ("removed", 4),
        ("unavailable", 5),
    ] {
        assert!(text.contains(&format!(
            "llm3_provider_credentials{{provider=\"anthropic\",state=\"{state}\"}} {count}\n"
        )));
    }
    assert_eq!(
        text.lines()
            .filter(|line| line.starts_with("llm3_provider_credentials{"))
            .count(),
        4
    );
}

#[test]
fn operation_labels_remain_bounded_and_counters_accumulate() {
    // Given every supported lifecycle operation and bounded outcome.
    let metrics = Metrics::default();
    let operations = [
        (CredentialOperation::Install, "install"),
        (CredentialOperation::Replace, "replace"),
        (CredentialOperation::Remove, "remove"),
        (CredentialOperation::Invalidate, "invalidate"),
        (CredentialOperation::Reencrypt, "reencrypt"),
    ];
    let outcomes = [
        (CredentialOutcome::Success, "success"),
        (CredentialOutcome::Conflict, "conflict"),
        (CredentialOutcome::NotFound, "not_found"),
        (CredentialOutcome::Unavailable, "unavailable"),
    ];
    // When each combination completes twice.
    for (operation, _) in operations {
        for (outcome, _) in outcomes {
            for _ in 0..2 {
                metrics
                    .credentials()
                    .record_operation(PROVIDER, operation, outcome);
            }
        }
    }
    // Then the exact allowlist is emitted, with one series per combination.
    let text = metrics.render();
    let actual: BTreeSet<_> = text
        .lines()
        .filter(|line| line.starts_with("llm3_credential_operations_total{"))
        .map(str::to_owned)
        .collect();
    let expected: BTreeSet<_> = operations.into_iter().flat_map(|(_, operation)| {
        outcomes.into_iter().map(move |(_, outcome)| format!(
            "llm3_credential_operations_total{{provider=\"anthropic\",operation=\"{operation}\",outcome=\"{outcome}\"}} 2"
        ))
    }).collect();
    assert_eq!(actual, expected);
}

async fn serve(servers: &mut JoinSet<()>, app: Router) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    servers.spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

#[tokio::test]
async fn scrapes_lifecycle_outage_recovery_without_identity_or_secret_leaks() {
    // Given actual proxy/metrics servers with canaries in configuration and requests.
    let mut servers = JoinSet::new();
    let upstream = serve(&mut servers, Router::new().fallback(|| async { "ok" })).await;
    let token = CANARIES.get(1).unwrap();
    let config = Config::parse(&format!(
        "listen = '127.0.0.1:0'\nproxy_token = '{token}'\n[[route]]\nname = 'claude'\nupstream = '{upstream}'\nheaders = {{ x-api-key = '{token}' }}\n"
    ), |_| None).unwrap();
    let proxy = Arc::new(Proxy::new(config).unwrap());
    let base = serve(&mut servers, router(Arc::clone(&proxy))).await;
    let scrape_base = serve(&mut servers, metrics_router(Arc::clone(&proxy))).await;
    let client = reqwest::Client::builder().timeout(TIMEOUT).build().unwrap();
    let response = client
        .post(format!("{base}/claude/v1/messages"))
        .bearer_auth(token)
        .header("x-private-context", CANARIES.join(";"))
        .body(CANARIES.join("\n"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.text().await.unwrap(), "ok");
    let credentials = proxy.metrics().credentials();
    credentials.set_postgres_available(true);
    // When authoritative lifecycle snapshots and database observations change.
    for (phase, counts, operation, available) in [
        (
            "installed",
            ACTIVE,
            Some(CredentialOperation::Install),
            true,
        ),
        (
            "invalid",
            INVALID,
            Some(CredentialOperation::Invalidate),
            true,
        ),
        ("removed", REMOVED, Some(CredentialOperation::Remove), true),
        ("outage", REMOVED, None, false),
        (
            "recovered",
            ACTIVE,
            Some(CredentialOperation::Replace),
            true,
        ),
    ] {
        if available {
            credentials.set_credentials(PROVIDER, counts);
        }
        if let Some(operation) = operation {
            credentials.record_operation(PROVIDER, operation, CredentialOutcome::Success);
        }
        credentials.set_postgres_available(available);
        let response = client
            .get(format!("{scrape_base}/metrics"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        let text = response.text().await.unwrap();
        // Then each real HTTP scrape reflects state, retains request labels and hides canaries.
        assert!(text.contains(&format!(
            "llm3_postgres_available {}\n",
            u8::from(available)
        )));
        for (state, count) in [
            ("active", counts.active),
            ("invalid", counts.invalid),
            ("removed", counts.removed),
            ("unavailable", counts.unavailable),
        ] {
            assert!(text.contains(&format!(
                "llm3_provider_credentials{{provider=\"anthropic\",state=\"{state}\"}} {count}\n"
            )));
        }
        assert!(text.contains("llm3_http_requests_total{route=\"claude\",code=\"200\"} 1\n"));
        assert!(
            text.contains("llm3_http_request_header_duration_seconds_count{route=\"claude\"} 1\n")
        );
        assert!(text.contains("llm3_credential_operations_total{provider=\"anthropic\",operation=\"install\",outcome=\"success\"} 1\n"));
        for canary in CANARIES {
            assert!(!text.contains(canary), "scrape contained private data");
        }
        println!("SCRAPE {phase}\n{text}");
    }
    servers.shutdown().await;
    println!("SERVER_TASKS_SHUT_DOWN {}", servers.is_empty());
}
