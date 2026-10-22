#![allow(clippy::unwrap_used, clippy::expect_used)]

use llmproxy::{
    config::LimitConfig,
    limit::shared::{
        AccountingError, CompiledLimits, FixedBucket, PendingAdmission, UNITS_PER_TOKEN,
    },
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::time::Instant;
use uuid::Uuid;

#[path = "shared_limit/support.rs"]
mod support;
use support::{Replica, State, request};

fn configs(text: &str) -> Vec<LimitConfig> {
    text.split("\n---\n")
        .map(|s| toml::from_str(s).unwrap())
        .collect()
}

#[test]
fn fixed_point_refills_exactly_at_microsecond_boundaries() {
    for rate in [1, 7, 60, 1001, u32::MAX] {
        for burst in [0, 9, u32::MAX] {
            let mut bucket = FixedBucket::new(rate, burst).unwrap();
            bucket.charge(usize::MAX).unwrap();
            let wait = bucket.wait(1);
            bucket.refill(wait.checked_sub(Duration::from_micros(1)).unwrap());
            assert_eq!(bucket.charge(1), Err(AccountingError::InsufficientBudget));
            bucket.refill(Duration::from_micros(1));
            bucket.charge(1).unwrap();
            assert!(bucket.available_units() < u64::from(rate));
        }
    }
}

#[test]
fn refill_partition_has_no_rounding_drift_and_clamps_at_capacity() {
    for rate in [1, 7, 60, 1001, u32::MAX] {
        let mut whole = FixedBucket::new(rate, 11).unwrap();
        whole.charge(usize::MAX).unwrap();
        let mut parts = whole;
        whole.refill(Duration::from_micros(999));
        for _ in 0..999 {
            parts.refill(Duration::from_micros(1));
        }
        assert_eq!(parts.available_units(), whole.available_units());
        whole.refill(Duration::MAX);
        assert_eq!(
            whole.available_units(),
            (u64::from(rate) + 11) * UNITS_PER_TOKEN
        );
        whole.charge(usize::MAX).unwrap();
        assert_eq!(whole.available_units(), 0);
    }
}

#[test]
fn invalid_durable_balance_is_rejected_without_mutation() {
    let mut bucket = FixedBucket::new(1, 0).unwrap();
    for balance in [-1, i64::MAX] {
        assert_eq!(
            bucket.restore(balance),
            Err(AccountingError::InvalidBalance)
        );
        assert_eq!(bucket.available_units(), UNITS_PER_TOKEN);
    }
    assert!(FixedBucket::new(0, 0).is_err());
}

#[test]
fn fingerprints_detect_every_behavior_change_and_order() {
    let original = configs("name='a'\nprefixes=['/']\nrpm=1\ntpm=10\nmax_inflight=1");
    let first = CompiledLimits::new(&original, "ns").unwrap();
    let hash = first.shared.first().unwrap().fingerprint().unwrap();
    let identical = CompiledLimits::new(&original, "ns").unwrap();
    identical.shared.first().unwrap().verify(&hash).unwrap();
    for field in ["burst=1", "tpm_burst=1", "model='large'"] {
        let changed = configs(&format!(
            "name='a'\nprefixes=['/']\nrpm=1\ntpm=10\nmax_inflight=1\n{field}"
        ));
        assert!(
            CompiledLimits::new(&changed, "ns")
                .unwrap()
                .shared
                .first()
                .unwrap()
                .verify(&hash)
                .is_err()
        );
    }
    for changed in [
        "name='a'\nprefixes=['/claude/']\nrpm=1\ntpm=10\nmax_inflight=1",
        "name='a'\nprefixes=['/']\nrpm=2\ntpm=10\nmax_inflight=1",
        "name='a'\nprefixes=['/']\nrpm=1\ntpm=11\nmax_inflight=1",
        "name='a'\nprefixes=['/']\nrpm=1\ntpm=10\nmax_inflight=2",
        "name='b'\nprefixes=['/']\nrpm=1\n---\nname='a'\nprefixes=['/']\nrpm=1\ntpm=10\nmax_inflight=1",
    ] {
        assert!(
            CompiledLimits::new(&configs(changed), "ns")
                .unwrap()
                .shared
                .last()
                .unwrap()
                .verify(&hash)
                .is_err()
        );
    }
}

#[tokio::test(start_paused = true)]
async fn compilation_keeps_static_pools_local_and_claude_shared() {
    let compiled = CompiledLimits::new(
        &configs(
            "name='all'\nprefixes=['/']\nrpm=1\n---\nname='static'\nprefixes=['/static/']\nrpm=1",
        ),
        "ns",
    )
    .unwrap();
    assert_eq!(compiled.shared.len(), 1);
    let start = Instant::now();
    compiled.local.acquire("/claude/v1/messages", None, 0).await;
    compiled.local.acquire("/static/x", None, 0).await;
    assert_eq!(start.elapsed(), Duration::ZERO);
    compiled.local.acquire("/static/x", None, 0).await;
    assert_eq!(start.elapsed(), Duration::from_mins(1));
}

#[test]
fn shared_matching_preserves_model_and_path_intersection() {
    let compiled = CompiledLimits::new(
        &configs("name='a'\nmodel='big'\nprefixes=['/claude/v1/']\nrpm=1"),
        "ns",
    )
    .unwrap();
    let definition = compiled.shared.first().unwrap();
    assert!(definition.matches("/claude/v1/messages", Some("big")));
    assert!(!definition.matches("/claude/v1/messages", Some("small")));
    assert!(!definition.matches("/claude/v2/messages", Some("big")));
    assert!(!definition.matches("/static/v1/messages", Some("big")));
    assert!(
        CompiledLimits::new(
            &configs("name='a'\nprefixes=['/']\nrpm=1\n---\nname='a'\nprefixes=['/']\nrpm=1"),
            "ns"
        )
        .is_err()
    );
}

#[tokio::test]
async fn two_replicas_accept_identical_definitions_and_reject_conflicting_burst() {
    let state = Arc::new(Mutex::new(State::default()));
    let a = Replica {
        state: state.clone(),
        fail: None,
    };
    let b = Replica {
        state: state.clone(),
        fail: None,
    };
    let config = configs("name='a'\nprefixes=['/']\nrpm=1");
    let first = CompiledLimits::new(&config, "ns").unwrap();
    let second = CompiledLimits::new(&config, "ns").unwrap();
    let alice = Uuid::new_v4();
    drop(
        PendingAdmission::new(&first.shared, request(alice))
            .acquire(&a)
            .await
            .unwrap(),
    );
    drop(
        PendingAdmission::new(&second.shared, request(alice))
            .acquire(&b)
            .await
            .unwrap(),
    );
    let conflict =
        CompiledLimits::new(&configs("name='a'\nprefixes=['/']\nrpm=1\nburst=1"), "ns").unwrap();
    assert!(
        PendingAdmission::new(&conflict.shared, request(Uuid::new_v4()))
            .acquire(&b)
            .await
            .is_err()
    );
    assert_eq!(state.lock().unwrap().definitions.len(), 1);
    assert_eq!(
        state.lock().unwrap().charged.get(&(alice, "a".into())),
        Some(&20)
    );
    assert_eq!(state.lock().unwrap().leases, 0);
}

#[tokio::test]
async fn later_failure_releases_earlier_leases_without_refunding_rate_charge() {
    let state = Arc::new(Mutex::new(State::default()));
    let store = Replica {
        state: state.clone(),
        fail: Some("later".into()),
    };
    let compiled = CompiledLimits::new(
        &configs("name='first'\nprefixes=['/']\nrpm=1\n---\nname='later'\nprefixes=['/']\nrpm=1"),
        "ns",
    )
    .unwrap();
    let person = Uuid::new_v4();
    assert!(
        PendingAdmission::new(&compiled.shared, request(person))
            .acquire(&store)
            .await
            .is_err()
    );
    let state = state.lock().unwrap();
    assert_eq!(state.leases, 0);
    assert_eq!(state.order, ["first"]);
    assert_eq!(state.charged.get(&(person, "first".into())), Some(&10));
    drop(state);
}

#[tokio::test]
async fn retries_hold_admission_without_recharge_and_people_have_separate_keys() {
    let state = Arc::new(Mutex::new(State::default()));
    let store = Replica {
        state: state.clone(),
        fail: None,
    };
    let compiled = CompiledLimits::new(
        &configs("name='a'\nprefixes=['/']\nrpm=1\n---\nname='b'\nprefixes=['/']\nrpm=1"),
        "ns",
    )
    .unwrap();
    let alice = Uuid::new_v4();
    let bob = Uuid::new_v4();
    let admission = PendingAdmission::new(&compiled.shared, request(alice))
        .acquire(&store)
        .await
        .unwrap();
    for _ in 0..3 {
        tokio::task::yield_now().await;
    }
    assert_eq!(state.lock().unwrap().order, ["a", "b"]);
    assert_eq!(state.lock().unwrap().leases, 2);
    drop(admission);
    drop(
        PendingAdmission::new(&compiled.shared, request(bob))
            .acquire(&store)
            .await
            .unwrap(),
    );
    assert_eq!(
        state.lock().unwrap().charged.get(&(alice, "a".into())),
        Some(&10)
    );
    assert_eq!(
        state.lock().unwrap().charged.get(&(bob, "a".into())),
        Some(&10)
    );
    assert_ne!(
        compiled.shared.first().unwrap().key(alice),
        compiled.shared.first().unwrap().key(bob)
    );
    assert_eq!(state.lock().unwrap().leases, 0);
}

#[test]
fn definitions_expose_inflight_and_initial_buckets_and_errors_render() {
    let limits = CompiledLimits::new(
        &configs("name='a'\nprefixes=['/claude/']\nrpm=2\ntpm=10\nmax_inflight=3\n---\nname='b'\nprefixes=['/claude/']\nrpm=1"),
        "ns",
    )
    .unwrap();
    let a = limits.shared.first().unwrap();
    let b = limits.shared.get(1).unwrap();
    assert_eq!(a.max_inflight(), Some(3));
    assert_eq!(b.max_inflight(), None);
    let (requests, tokens) = a.buckets().unwrap();
    assert!(requests.is_some() && tokens.is_some());
    let (requests, tokens) = b.buckets().unwrap();
    assert!(requests.is_some() && tokens.is_none());
    let mut bucket = FixedBucket::new(1, 0).unwrap();
    bucket.restore(0).unwrap();
    let messages: std::collections::BTreeSet<String> = [
        AccountingError::ZeroRate,
        AccountingError::InvalidBalance,
        AccountingError::InsufficientBudget,
    ]
    .iter()
    .map(ToString::to_string)
    .collect();
    assert_eq!(messages.len(), 3);
}
