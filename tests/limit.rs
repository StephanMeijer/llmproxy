#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use llmproxy::{config::LimitConfig, limit::Limits};
use tokio::time::{Instant, timeout};

fn cfg(toml: &str) -> LimitConfig {
    toml::from_str(toml).unwrap()
}

#[tokio::test(start_paused = true)]
async fn holds_until_the_bucket_refills() {
    let limits = Limits::new(&[cfg("name='a'\nprefixes=['/a/']\nrpm=60")]).unwrap();
    for _ in 0..60 {
        limits.acquire("/a/x", None, 0).await;
    }
    let start = Instant::now();
    limits.acquire("/a/x", None, 0).await;
    assert_eq!(start.elapsed(), Duration::from_secs(1));
    // A different path is not matched, so it never waits.
    limits.acquire("/b/x", None, 0).await;
    assert_eq!(start.elapsed(), Duration::from_secs(1));
}

#[tokio::test(start_paused = true)]
async fn token_budget_and_model_matching() {
    let limits = Limits::new(&[cfg("name='m'\nmodel='big'\ntpm=600")]).unwrap();
    assert!(limits.by_model());
    limits.acquire("/x", Some("big"), 600).await;
    let start = Instant::now();
    limits.acquire("/x", Some("big"), 10).await;
    assert_eq!(start.elapsed(), Duration::from_secs(1)); // 10 tokens at 10/s
    limits.acquire("/x", Some("other"), 600).await; // other models are unlimited
    assert_eq!(start.elapsed(), Duration::from_secs(1));
}

#[tokio::test(start_paused = true)]
async fn model_and_prefixes_must_both_match() {
    let limits = Limits::new(&[cfg("name='b'\nmodel='big'\nprefixes=['/a/']\nrpm=1")]).unwrap();
    limits.acquire("/a/x", Some("big"), 0).await; // takes the only request
    let start = Instant::now();
    limits.acquire("/b/x", Some("big"), 0).await; // wrong path
    limits.acquire("/a/x", Some("other"), 0).await; // wrong model
    assert_eq!(start.elapsed(), Duration::ZERO);
}

#[tokio::test(start_paused = true)]
async fn oversized_request_waits_for_a_full_bucket_not_forever() {
    let limits = Limits::new(&[cfg("name='m'\nmodel='big'\ntpm=60")]).unwrap();
    limits.acquire("/x", Some("big"), 60).await;
    let start = Instant::now();
    limits.acquire("/x", Some("big"), 1_000_000).await;
    assert_eq!(start.elapsed(), Duration::from_mins(1));
}

#[tokio::test(start_paused = true)]
async fn inflight_cap_releases_with_the_permit() {
    let limits = Limits::new(&[cfg("name='c'\nprefixes=['/']\nmax_inflight=1")]).unwrap();
    let permits = limits.acquire("/x", None, 0).await;
    let held = timeout(Duration::from_secs(5), limits.acquire("/x", None, 0));
    assert!(held.await.is_err());
    drop(permits);
    limits.acquire("/x", None, 0).await;
}

#[tokio::test(start_paused = true)]
async fn waiters_are_served_in_arrival_order() {
    let limits = Arc::new(Limits::new(&[cfg("name='a'\nprefixes=['/']\nrpm=60")]).unwrap());
    for _ in 0..60 {
        limits.acquire("/x", None, 0).await;
    }
    let served = Arc::new(Mutex::new(Vec::new()));
    let mut tasks = Vec::new();
    for id in 0..5 {
        let (limits, served) = (limits.clone(), served.clone());
        tasks.push(tokio::spawn(async move {
            limits.acquire("/x", None, 0).await;
            served.lock().unwrap().push(id);
        }));
        tokio::task::yield_now().await; // let it queue before the next one arrives
    }
    for task in tasks {
        task.await.unwrap();
    }
    assert_eq!(*served.lock().unwrap(), [0, 1, 2, 3, 4]);
}

#[test]
fn rejects_useless_limits() {
    for (bad, why) in [
        ("name='a'\nrpm=1", "set `model` or `prefixes`"),
        (
            "name='a'\nprefixes=['/']",
            "set `rpm`, `tpm` or `max_inflight`",
        ),
        ("name='a'\nprefixes=['/']\nrpm=0", "must be above 0"),
        ("name='a'\nprefixes=['/']\ntpm=0", "must be above 0"),
        (
            "name='a'\nprefixes=['/']\nmax_inflight=0",
            "must be above 0",
        ),
    ] {
        let err = Limits::new(&[cfg(bad)])
            .err()
            .expect("limit should be refused");
        assert!(err.0.contains(why), "{bad}: {err}");
    }
}
