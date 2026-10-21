use super::{
    runtime_metrics::{connections, scrape},
    support::*,
};
use axum::http::StatusCode;
use std::time::{Duration, Instant};

const LIMIT: &str =
    "[[limit]]\nname='limit-private-canary'\nprefixes=['/claude/']\nrpm=60\nburst=10\n";
const CONFLICTING: &str =
    "[[limit]]\nname='limit-private-canary'\nprefixes=['/claude/']\nrpm=60\nburst=11\n";
const QUEUED: &str =
    "[[limit]]\nname='queue-private-canary'\nprefixes=['/claude/']\nrpm=1\nburst=0\n";
const SAMPLE: &str = "llm3_shared_limit_conflicts_total{provider=\"anthropic\"}";
const QUEUE_RESPONSE_LIMIT: Duration = Duration::from_secs(17);

async fn conflicts(f: &Fixture, binary: &Binary, phase: &str, expected: u64) {
    let text = scrape(f, binary, phase).await;
    let samples: Vec<_> = text
        .lines()
        .filter(|line| line.starts_with("llm3_shared_limit_conflicts_total{"))
        .collect();
    assert_eq!(samples, [format!("{SAMPLE} {expected}")]);
    for canary in ["limit-private-canary", "queue-private-canary", BODY] {
        assert!(!text.contains(canary));
    }
    for sample in text.lines().filter(|line| !line.starts_with('#')) {
        assert!(!sample.contains(&f.db.config.namespace));
    }
    println!("CONFLICT_SAMPLE phase={phase} expected={expected} bounded=true");
}

#[tokio::test]
async fn divergent_replica_conflicts_increment_only_its_bounded_counter() {
    // Given real replicas sharing a namespace, with matching and divergent definitions.
    let mut f = Fixture::new().await;
    let original = f.binary(LIMIT, "").await;
    let matching = f.binary(LIMIT, "").await;
    let divergent = f.binary(CONFLICTING, "").await;
    for binary in [&original, &matching] {
        let request = tokio::spawn(f.request(binary, false).send());
        reply(f.hit().await, StatusCode::OK, "matching");
        assert_eq!(request.await.unwrap().unwrap().status(), StatusCode::OK);
    }
    // When the divergent replica receives two Claude requests and one static request.
    for _ in 0..2 {
        let response = f.request(&divergent, false).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = response.text().await.unwrap();
        assert!(body.contains("adapter_unavailable"));
        assert!(!body.contains("limit-private-canary"));
        println!("DIVERGENT_CLAUDE status=503 body={body}");
    }
    assert!(f.hits.try_recv().is_err());
    let request = tokio::spawn(
        reqwest::Client::new()
            .post(format!("{}/static/v1/messages", divergent.base))
            .bearer_auth("legacy")
            .send(),
    );
    reply(f.hit().await, StatusCode::OK, "static");
    assert_eq!(request.await.unwrap().unwrap().status(), StatusCode::OK);
    // Then only the divergent process increments and static forwarding remains available.
    conflicts(&f, &original, "original-matching", 0).await;
    conflicts(&f, &matching, "replica-matching", 0).await;
    conflicts(&f, &divergent, "replica-conflict", 2).await;
    let fingerprints: Vec<String> = sqlx::query_scalar(
        "SELECT encode(fingerprint, 'hex') FROM provider_credentials WHERE fingerprint IS NOT NULL",
    )
    .fetch_all(&f.db.pool)
    .await
    .unwrap();
    let text = scrape(&f, &divergent, "conflict-redaction").await;
    for fingerprint in fingerprints {
        assert!(!text.contains(&fingerprint));
    }
    artifact(
        "conflict-replicas.txt",
        &format!(
            "PASS matching Claude=200, divergent Claude=503 twice, static=200; counters=0,0,2; fingerprints and identities absent\nnamespace=test\noriginal and matching:\n{LIMIT}\ndivergent:\n{CONFLICTING}"
        ),
    );
    f.close(vec![original, matching, divergent]).await;
}

#[tokio::test]
async fn fingerprint_free_admission_and_database_outage_do_not_count_as_conflicts() {
    // Given a real replica without shared definitions and a warmed credential cache.
    let mut f = Fixture::new().await;
    let binary = f.binary("", "").await;
    let request = tokio::spawn(f.request(&binary, false).send());
    reply(f.hit().await, StatusCode::OK, "unlimited");
    assert_eq!(request.await.unwrap().unwrap().status(), StatusCode::OK);
    f.state_count("limit_definitions", 0).await;
    conflicts(&f, &binary, "fingerprint-free", 0).await;
    // When a subsequent request encounters an actual database outage.
    connections(&f, false).await;
    let response = f.request(&binary, false).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    // Then availability fails closed without recording a definition conflict.
    conflicts(&f, &binary, "conflict-database-outage", 0).await;
    connections(&f, true).await;
    artifact(
        "conflict-outage.txt",
        "PASS fingerprint-free Claude=200 with no definitions, outage Claude=503, conflict counter=0\n",
    );
    f.close(vec![binary]).await;
}

#[tokio::test]
async fn admission_schema_failure_does_not_count_as_conflict() {
    // Given a real replica whose shared definition table is unavailable.
    let f = Fixture::new().await;
    let binary = f.binary(LIMIT, "").await;
    sqlx::query("ALTER TABLE limit_definitions RENAME TO hidden_limit_definitions")
        .execute(&f.db.pool)
        .await
        .unwrap();
    // When Claude admission tries to register its definition.
    let response = f.request(&binary, false).send().await.unwrap();
    // Then a schema failure is unavailable, not a configuration conflict.
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    conflicts(&f, &binary, "conflict-schema-failure", 0).await;
    artifact(
        "conflict-schema.txt",
        "PASS missing definition table Claude=503, conflict counter=0\n",
    );
    f.close(vec![binary]).await;
}

#[tokio::test]
async fn queue_timeout_does_not_count_as_conflict() {
    // Given a shared request budget exhausted by one completed request.
    let mut f = Fixture::new().await;
    let binary = f.binary(QUEUED, "").await;
    let request = tokio::spawn(f.request(&binary, false).send());
    reply(f.hit().await, StatusCode::OK, "exhausted");
    assert_eq!(request.await.unwrap().unwrap().status(), StatusCode::OK);
    // When the next request cannot obtain capacity within the queue deadline.
    let started = Instant::now();
    let response = f.request(&binary, false).send().await.unwrap();
    // Then queue exhaustion returns429 without counting a definition conflict.
    let elapsed = started.elapsed();
    assert!(elapsed <= QUEUE_RESPONSE_LIMIT, "{elapsed:?}");
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert!(response.text().await.unwrap().contains("queue_timeout"));
    assert!(f.hits.try_recv().is_err());
    conflicts(&f, &binary, "conflict-queue-timeout", 0).await;
    artifact(
        "conflict-queue.txt",
        &format!(
            "PASS exhausted queue Claude=429 queue_timeout, no second upstream hit, conflict counter=0; elapsed={elapsed:?}, max_wait=15s, response_bound={QUEUE_RESPONSE_LIMIT:?}\n"
        ),
    );
    f.close(vec![binary]).await;
}
