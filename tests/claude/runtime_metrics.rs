use super::support::*;
use axum::http::StatusCode;
use llmproxy::secret::SecretBytes;
use std::time::Duration;

/// Credential counts refresh in the background, so poll until `expected` counts appear.
pub async fn scrape(
    f: &Fixture,
    binary: &Binary,
    phase: &str,
    expected: Option<[u64; 4]>,
) -> String {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let text = loop {
        let text = reqwest::get(&binary.metrics)
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        if expected.is_none_or(|expected| lines(expected).all(|line| text.contains(&line)))
            || tokio::time::Instant::now() >= deadline
        {
            break text;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
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

fn lines(expected: [u64; 4]) -> impl Iterator<Item = String> {
    ["active", "invalid", "removed", "unavailable"]
        .into_iter()
        .zip(expected)
        .map(|(state, count)| {
            format!(
                "llm3_provider_credentials{{provider=\"anthropic\",state=\"{state}\"}} {count}\n"
            )
        })
}

pub fn counts(text: &str, expected: [u64; 4]) {
    for line in lines(expected) {
        assert!(text.contains(&line), "{text}");
    }
}

pub async fn connections(f: &Fixture, enabled: bool) {
    connections_except(f, enabled, None).await;
}

/// Like `connections`, but disabling spares the backend `keep`.
async fn connections_except(f: &Fixture, enabled: bool, keep: Option<i32>) {
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
        sqlx::query("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=$1 AND pid IS DISTINCT FROM $2")
            .bind(name)
            .bind(keep)
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
    let text = scrape(&f, &binary, "installed", Some([2, 0, 0, 0])).await;
    counts(&text, [2, 0, 0, 0]);
    assert!(text.contains("llm3_postgres_available 1\n"));
    assert!(!text.contains("operation=\"install\""));

    let request = tokio::spawn(f.request(&binary, false).send());
    reply(f.hit().await, StatusCode::UNAUTHORIZED, "rejected");
    assert_eq!(
        request.await.unwrap().unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    let text = scrape(&f, &binary, "invalid", Some([1, 1, 0, 0])).await;
    counts(&text, [1, 1, 0, 0]);
    assert!(text.contains("operation=\"invalidate\",outcome=\"success\"} 1\n"));

    f.credentials.remove(f.bob_version).await.unwrap();
    assert_eq!(
        f.request(&binary, true).send().await.unwrap().status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    counts(
        &scrape(&f, &binary, "removed", Some([0, 1, 1, 0])).await,
        [0, 1, 1, 0],
    );
    // The test keeps one connection so it can write while the binary is cut off.
    let mut writer = f.db.pool.acquire().await.unwrap();
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *writer)
        .await
        .unwrap();
    connections_except(&f, false, Some(pid)).await;
    assert_eq!(
        f.request(&binary, false).send().await.unwrap().status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    let text = scrape(&f, &binary, "outage", Some([0, 1, 1, 0])).await;
    counts(&text, [0, 1, 1, 0]);
    assert!(text.contains("llm3_postgres_available 0\n"));
    // A change the listener cannot hear: only the reconnect refresh can report it.
    sqlx::query("UPDATE provider_credentials SET state='active' WHERE person_id=$1")
        .bind(f.alice_version.person)
        .execute(&mut *writer)
        .await
        .unwrap();
    drop(writer);
    connections(&f, true).await;
    let text = scrape(&f, &binary, "recovery", Some([1, 0, 1, 0])).await;
    // Then counts follow durable state changed during the outage, with no request in between.
    counts(&text, [1, 0, 1, 0]);
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
    let text = scrape(&f, &binary, "conflict", Some([2, 0, 0, 0])).await;
    counts(&text, [2, 0, 0, 0]);
    assert!(text.contains("operation=\"invalidate\",outcome=\"conflict\"} 1\n"));
    assert!(text.contains("llm3_shared_limit_conflicts_total{provider=\"anthropic\"} 0\n"));
    assert!(!text.contains("runtime-replacement-canary"));
    let request = tokio::spawn(f.request(&binary, true).send());
    let hit = f.hit().await;
    connections(&f, false).await;
    reply(hit, StatusCode::UNAUTHORIZED, "unavailable");
    assert_eq!(
        request.await.unwrap().unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    let text = scrape(&f, &binary, "invalidation-unavailable", Some([2, 0, 0, 0])).await;
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
    let text = scrape(&f, &binary, "undecryptable", Some([0, 0, 0, 2])).await;
    // Then local decryption failures are not reported as database outages or active credentials.
    counts(&text, [0, 0, 0, 2]);
    assert!(text.contains("llm3_postgres_available 1\n"));
    assert!(!text.contains("missing-key-canary"));
    f.close(vec![binary]).await;
}

#[tokio::test]
async fn real_binary_retries_a_failed_count_refresh_without_a_new_change() {
    // Given a binary whose credential table is locked by a change that sends no notification.
    let mut f = Fixture::new().await;
    let binary = f.binary("", "").await;
    let request = tokio::spawn(f.request(&binary, false).send());
    reply(f.hit().await, StatusCode::OK, "ok");
    assert_eq!(request.await.unwrap().unwrap().status(), StatusCode::OK);
    counts(
        &scrape(&f, &binary, "retry-before", Some([2, 0, 0, 0])).await,
        [2, 0, 0, 0],
    );
    let mut tx = f.db.pool.begin().await.unwrap();
    // DISABLE TRIGGER alone takes a lock that still lets reads through.
    for statement in [
        "LOCK TABLE provider_credentials IN ACCESS EXCLUSIVE MODE",
        "ALTER TABLE provider_credentials DISABLE TRIGGER credential_change",
    ] {
        sqlx::query(statement).execute(&mut *tx).await.unwrap();
    }
    sqlx::query("UPDATE provider_credentials SET state='invalid' WHERE person_id=$1")
        .bind(f.alice_version.person)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("ALTER TABLE provider_credentials ENABLE TRIGGER credential_change")
        .execute(&mut *tx)
        .await
        .unwrap();
    // When a wake-up makes the count scan hit the lock timeout once.
    sqlx::query("SELECT pg_notify('llmproxy_credentials', '')")
        .execute(&f.db.pool)
        .await
        .unwrap();
    let waiting = "SELECT query_start::text FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE 'SELECT person_id,state,%'";
    let started: String = loop {
        if let Some(started) = sqlx::query_scalar(waiting)
            .fetch_optional(&f.db.pool)
            .await
            .unwrap()
        {
            break started;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    while sqlx::query_scalar::<_, String>(waiting)
        .fetch_all(&f.db.pool)
        .await
        .unwrap()
        .contains(&started)
    {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tx.commit().await.unwrap();
    // Then a retry, not a notification, reports the committed change.
    counts(
        &scrape(&f, &binary, "retry-after", Some([1, 1, 0, 0])).await,
        [1, 1, 0, 0],
    );
    f.close(vec![binary]).await;
}
