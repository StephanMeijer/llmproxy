use super::support::*;
use axum::http::StatusCode;
use llmproxy::secret::SecretBytes;

pub async fn scrape(f: &Fixture, binary: &Binary, phase: &str) -> String {
    let text = reqwest::get(&binary.metrics)
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    for secret in [
        ALICE_KEY,
        BOB_KEY,
        f.alice.expose().as_bytes(),
        f.bob.expose().as_bytes(),
    ] {
        assert!(
            !text
                .as_bytes()
                .windows(secret.len())
                .any(|bytes| bytes == secret)
        );
    }
    for identity in [
        f.alice_version.person.0.to_string(),
        f.bob_version.person.0.to_string(),
        f.db.config.url.clone(),
    ] {
        assert!(!text.contains(&identity));
    }
    artifact(&format!("runtime-metrics-{phase}.txt"), &text);
    println!("SCRAPE {phase}\n{text}");
    text
}

pub fn counts(text: &str, expected: [u64; 4]) {
    for (state, count) in ["active", "invalid", "removed", "unavailable"]
        .into_iter()
        .zip(expected)
    {
        assert!(
            text.contains(&format!(
                "llm3_provider_credentials{{provider=\"anthropic\",state=\"{state}\"}} {count}\n"
            )),
            "{text}"
        );
    }
}

pub async fn connections(f: &Fixture, enabled: bool) {
    let name = reqwest::Url::parse(&f.db.config.url)
        .unwrap()
        .path()
        .trim_start_matches('/')
        .to_owned();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ALTER DATABASE {name} ALLOW_CONNECTIONS {enabled}"
    )))
    .execute(&f.db.admin)
    .await
    .unwrap();
    if !enabled {
        sqlx::query("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=$1")
            .bind(name)
            .execute(&f.db.admin)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn real_binary_metrics_follow_durable_state_outage_and_recovery() {
    // Given a binary serving two installed credentials.
    let mut f = Fixture::new().await;
    let binary = f.binary("", "").await;
    // When real requests observe mutations and an outage in the same process.
    let request = tokio::spawn(f.request(&binary, false).send());
    reply(f.hit().await, StatusCode::OK, "ok");
    assert_eq!(request.await.unwrap().unwrap().status(), StatusCode::OK);
    let text = scrape(&f, &binary, "installed").await;
    counts(&text, [2, 0, 0, 0]);
    assert!(text.contains("llm3_postgres_available 1\n"));
    assert!(!text.contains("operation=\"install\""));

    let request = tokio::spawn(f.request(&binary, false).send());
    reply(f.hit().await, StatusCode::UNAUTHORIZED, "rejected");
    assert_eq!(
        request.await.unwrap().unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    let text = scrape(&f, &binary, "invalid").await;
    counts(&text, [1, 1, 0, 0]);
    assert!(text.contains("operation=\"invalidate\",outcome=\"success\"} 1\n"));

    f.credentials.remove(f.bob_version).await.unwrap();
    assert_eq!(
        f.request(&binary, true).send().await.unwrap().status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    counts(&scrape(&f, &binary, "removed").await, [0, 1, 1, 0]);
    connections(&f, false).await;
    assert_eq!(
        f.request(&binary, false).send().await.unwrap().status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    let text = scrape(&f, &binary, "outage").await;
    counts(&text, [0, 1, 1, 0]);
    assert!(text.contains("llm3_postgres_available 0\n"));
    connections(&f, true).await;
    assert_eq!(
        f.request(&binary, false).send().await.unwrap().status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    let text = scrape(&f, &binary, "recovery").await;
    // Then counts remain authoritative even though invalid credentials cannot forward.
    counts(&text, [0, 1, 1, 0]);
    assert!(text.contains("llm3_postgres_available 1\n"));
    f.close(vec![binary]).await;
}

#[tokio::test]
async fn real_binary_records_invalidation_conflict_and_database_failure() {
    // Given an admitted generation that is replaced while upstream is held.
    let mut f = Fixture::new().await;
    let binary = f.binary("", "").await;
    let request = tokio::spawn(f.request(&binary, false).send());
    let hit = f.hit().await;
    f.credentials
        .replace(
            f.alice_version,
            &SecretBytes::new(b"runtime-replacement-canary".to_vec()),
        )
        .await
        .unwrap();
    // When the provider rejects the old generation, then a second rejection meets an outage.
    reply(hit, StatusCode::UNAUTHORIZED, "old");
    assert_eq!(
        request.await.unwrap().unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    let text = scrape(&f, &binary, "conflict").await;
    counts(&text, [2, 0, 0, 0]);
    assert!(text.contains("operation=\"invalidate\",outcome=\"conflict\"} 1\n"));
    assert!(!text.contains("runtime-replacement-canary"));
    let request = tokio::spawn(f.request(&binary, true).send());
    let hit = f.hit().await;
    connections(&f, false).await;
    reply(hit, StatusCode::UNAUTHORIZED, "unavailable");
    assert_eq!(
        request.await.unwrap().unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    let text = scrape(&f, &binary, "invalidation-unavailable").await;
    // Then counters distinguish durable conflict and failure, retaining the last counts.
    counts(&text, [2, 0, 0, 0]);
    assert!(text.contains("llm3_postgres_available 0\n"));
    assert!(text.contains("operation=\"invalidate\",outcome=\"unavailable\"} 1\n"));
    connections(&f, true).await;
    f.close(vec![binary]).await;
}

#[tokio::test]
async fn real_binary_counts_corrupt_and_missing_key_envelopes_as_unavailable() {
    // Given one ciphertext with a failed authentication tag and one missing key identifier.
    let f = Fixture::new().await;
    sqlx::query("UPDATE provider_credentials SET ciphertext=set_byte(ciphertext,0,get_byte(ciphertext,0) # 1) WHERE person_id=$1")
        .bind(f.alice_version.person).execute(&f.db.pool).await.unwrap();
    sqlx::query("UPDATE provider_credentials SET key_id='missing-key-canary' WHERE person_id=$1")
        .bind(f.bob_version.person)
        .execute(&f.db.pool)
        .await
        .unwrap();
    let binary = f.binary("", "").await;
    // When authenticated requests trigger a complete authoritative refresh.
    for bob in [false, true] {
        assert_eq!(
            f.request(&binary, bob).send().await.unwrap().status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }
    let text = scrape(&f, &binary, "undecryptable").await;
    // Then local decryption failures are not reported as database outages or active credentials.
    counts(&text, [0, 0, 0, 2]);
    assert!(text.contains("llm3_postgres_available 1\n"));
    assert!(!text.contains("missing-key-canary"));
    f.close(vec![binary]).await;
}
