#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "unit test assertions"
)]

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{Router, extract::State, routing::get};
use ring::{
    rand::SystemRandom,
    signature::{RSA_PKCS1_SHA256, RsaKeyPair, RsaPublicKeyComponents},
};
use serde_json::{Value, json};

use super::{
    Cache, Discovery, Keys, MAX_DOCUMENT_BYTES, OidcError, Pkce, PublicKey, challenge, discover,
    encode, fetch_keys, random_token, verify_id_token,
};

/// TEST-ONLY RSA 2048 key (PKCS#8 DER), generated for these tests and trusted nowhere else.
const TEST_ONLY_KEY: &[u8] = include_bytes!("test_only_rsa_2048.pk8");
const ISSUER: &str = "https://auth.example";
const AUDIENCE: &str = "oaiapp_test";
const NONCE: &str = "nonce-1";
const KID: &str = "test-kid";
const NOW: u64 = 1_800_000_000;

fn key_pair() -> RsaKeyPair {
    RsaKeyPair::from_pkcs8(TEST_ONLY_KEY).unwrap()
}

fn keys() -> Keys {
    let public: RsaPublicKeyComponents<Vec<u8>> = key_pair().public().into();
    Keys::from([(
        KID.to_owned(),
        PublicKey {
            n: public.n,
            e: public.e,
        },
    )])
}

fn jwks() -> Value {
    json!({"keys": [
        {"kty": "EC", "kid": "ec", "crv": "P-256", "x": "a", "y": "b"},
        rsa_jwk(KID),
    ]})
}

fn rsa_jwk(kid: &str) -> Value {
    let key = &keys()[KID];
    json!({"kty": "RSA", "kid": kid, "n": encode(&key.n), "e": encode(&key.e), "alg": "RS256", "use": "sig"})
}

/// Signs `header.payload` (both already base64url) with the test key.
fn sign_raw(header: &str, payload: &str) -> String {
    let pair = key_pair();
    let input = format!("{header}.{payload}");
    let mut signature = vec![0; pair.public().modulus_len()];
    pair.sign(
        &RSA_PKCS1_SHA256,
        &SystemRandom::new(),
        input.as_bytes(),
        &mut signature,
    )
    .unwrap();
    format!("{input}.{}", encode(&signature))
}

fn sign(header: &Value, claims: &Value) -> String {
    sign_raw(
        &encode(header.to_string().as_bytes()),
        &encode(claims.to_string().as_bytes()),
    )
}

fn header() -> Value {
    json!({"alg": "RS256", "kid": KID, "typ": "JWT"})
}

fn claims() -> Value {
    json!({
        "iss": ISSUER, "aud": AUDIENCE, "exp": NOW + 600, "nonce": NONCE,
        "sub": "user-1", "email": "a@example.com"
    })
}

fn with(field: &str, value: Value) -> Value {
    let mut claims = claims();
    claims[field] = value;
    claims
}

fn verify(token: &str) -> Result<super::IdClaims, OidcError> {
    verify_id_token(token, &keys(), ISSUER, AUDIENCE, NONCE, NOW)
}

#[test]
fn a_valid_token_yields_its_identity() {
    let claims = verify(&sign(&header(), &claims())).unwrap();
    assert_eq!(claims.sub, "user-1");
    assert_eq!(claims.email.as_deref(), Some("a@example.com"));
    assert_eq!(claims.exp, NOW + 600);
    let mut array = with("aud", json!(["other", AUDIENCE]));
    array["azp"] = json!(AUDIENCE);
    assert_eq!(verify(&sign(&header(), &array)).unwrap().sub, "user-1");
    let single = with("aud", json!([AUDIENCE]));
    assert!(verify(&sign(&header(), &single)).is_ok());
    let within_leeway = with("exp", json!(NOW - 30));
    assert!(verify(&sign(&header(), &within_leeway)).is_ok());
}

#[test]
fn claim_mismatches_are_refused() {
    for (claims, expected) in [
        (
            with("iss", json!("https://evil.example")),
            OidcError::Issuer,
        ),
        (with("aud", json!("other")), OidcError::Audience),
        (with("aud", json!(["other"])), OidcError::Audience),
        (with("aud", json!(["other", AUDIENCE])), OidcError::Audience),
        (with("azp", json!("other")), OidcError::Audience),
        (with("exp", json!(NOW - 61)), OidcError::Expired),
        (with("nonce", json!("other")), OidcError::Nonce),
        (with("nonce", Value::Null), OidcError::Nonce),
        (with("sub", json!("")), OidcError::Subject),
    ] {
        assert_eq!(verify(&sign(&header(), &claims)).unwrap_err(), expected);
    }
}

#[test]
fn signature_algorithm_and_key_are_enforced() {
    let token = sign(&header(), &claims());
    let mut parts: Vec<&str> = token.split('.').collect();
    let tampered = encode(with("sub", json!("user-2")).to_string().as_bytes());
    parts[1] = &tampered;
    assert_eq!(verify(&parts.join(".")).unwrap_err(), OidcError::Signature);

    for (header, expected) in [
        (
            json!({"alg": "RS256", "kid": "unknown"}),
            OidcError::UnknownKey,
        ),
        (json!({"alg": "RS256"}), OidcError::UnknownKey),
        (json!({"alg": "none", "kid": KID}), OidcError::Algorithm),
        (json!({"alg": "HS256", "kid": KID}), OidcError::Algorithm),
    ] {
        assert_eq!(verify(&sign(&header, &claims())).unwrap_err(), expected);
    }
}

#[test]
fn malformed_tokens_are_refused_without_echoing_them() {
    let header = encode(header().to_string().as_bytes());
    let token = sign(&json!({"alg": "RS256", "kid": KID}), &claims());
    let two_parts = token.rsplit_once('.').unwrap().0.to_owned();
    for token in [
        two_parts,
        format!("{token}.extra"),
        "!!!.e30.e30".to_owned(),
        sign_raw(&encode(b"not json"), "e30"),
        sign_raw(&header, &encode(b"not json")),
        sign_raw(&header, "!!!"),
        format!("{}.!!!", token.rsplit_once('.').unwrap().0),
    ] {
        let error = verify(&token).unwrap_err();
        assert_eq!(error, OidcError::Malformed, "{token}");
        assert!(!error.to_string().contains(&token));
    }
}

#[test]
fn pkce_matches_rfc_7636_and_random_values_differ() {
    // RFC 7636 Appendix B.
    assert_eq!(
        challenge("dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"),
        "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
    );
    let pkce = Pkce::new().unwrap();
    assert_eq!(pkce.verifier.len(), 43);
    assert_eq!(pkce.challenge, challenge(&pkce.verifier));
    let (a, b) = (random_token().unwrap(), random_token().unwrap());
    assert_eq!(a.len(), 43);
    assert_ne!(a, b);
    assert_ne!(*pkce.verifier, a);
}

#[test]
fn errors_are_fixed_strings() {
    for error in [
        OidcError::Randomness,
        OidcError::Fetch,
        OidcError::TooLarge,
        OidcError::IssuerMismatch,
        OidcError::Malformed,
        OidcError::Algorithm,
        OidcError::UnknownKey,
        OidcError::Signature,
        OidcError::Issuer,
        OidcError::Audience,
        OidcError::Expired,
        OidcError::Nonce,
        OidcError::Subject,
    ] {
        assert!(!error.to_string().is_empty());
    }
}

/// A fake issuer: discovery naming `issuer`, a JWKS whose hits are counted (it adds the `rotated`
/// kid from its second hit on), and an oversized body.
async fn fake(issuer: Option<&str>) -> (String, Arc<AtomicUsize>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let issuer = issuer.unwrap_or(&base).to_owned();
    let document = json!({
        "issuer": issuer,
        "jwks_uri": format!("{base}/jwks"),
        "revocation_endpoint": format!("{base}/revoke"),
        "token_endpoint": format!("{base}/token"),
    })
    .to_string();
    let hits = Arc::new(AtomicUsize::new(0));
    let app = Router::new()
        .route(
            "/.well-known/openid-configuration",
            get(move || async move { document }),
        )
        .route(
            "/jwks",
            get(|State(hits): State<Arc<AtomicUsize>>| async move {
                let mut jwks = jwks();
                if hits.fetch_add(1, Ordering::SeqCst) > 0 {
                    jwks["keys"]
                        .as_array_mut()
                        .unwrap()
                        .push(rsa_jwk("rotated"));
                }
                jwks.to_string()
            }),
        )
        .route("/big", get(|| async { "x".repeat(MAX_DOCUMENT_BYTES + 1) }))
        .route(
            "/missing",
            get(|| async { (axum::http::StatusCode::NOT_FOUND, "{}") }),
        )
        .with_state(Arc::clone(&hits));
    tokio::spawn(async move { axum::serve(listener, app).await });
    (base, hits)
}

#[tokio::test]
async fn discovery_and_keys_are_fetched_and_checked() {
    let client = reqwest::Client::new();
    let (base, _) = fake(None).await;
    let discovery: Discovery = discover(&client, &base).await.unwrap();
    assert_eq!(discovery.issuer, base);
    assert_eq!(discovery.revocation_endpoint, format!("{base}/revoke"));
    assert_eq!(discovery.token_endpoint, Some(format!("{base}/token")));
    assert_eq!(discovery.authorization_endpoint, None);
    let fetched = fetch_keys(&client, &discovery.jwks_uri).await.unwrap();
    assert_eq!(fetched, keys());

    let (other, _) = fake(Some("https://elsewhere.example")).await;
    assert_eq!(
        discover(&client, &other).await.unwrap_err(),
        OidcError::IssuerMismatch
    );
    for (path, expected) in [
        ("/big", OidcError::TooLarge),
        ("/missing", OidcError::Fetch),
        ("/.well-known/openid-configuration", OidcError::Malformed),
    ] {
        assert_eq!(
            fetch_keys(&client, &format!("{base}{path}"))
                .await
                .unwrap_err(),
            expected
        );
    }
    assert_eq!(
        fetch_keys(&client, "http://127.0.0.1:1/jwks")
            .await
            .unwrap_err(),
        OidcError::Fetch
    );
}

#[tokio::test]
async fn an_unknown_key_refetches_at_most_once_a_minute() {
    let client = reqwest::Client::new();
    let (base, hits) = fake(None).await;
    let mut cache = Cache::load(&client, &base).await.unwrap();
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    let claims = with("iss", json!(base));
    let token = sign(&header(), &claims);
    assert!(
        cache
            .verify(&client, &token, AUDIENCE, NONCE, NOW)
            .await
            .is_ok()
    );

    let rotated = sign(&json!({"alg": "RS256", "kid": "rotated"}), &claims);
    let unknown = cache.verify(&client, &rotated, AUDIENCE, NONCE, NOW).await;
    assert_eq!(unknown.unwrap_err(), OidcError::UnknownKey);
    assert_eq!(hits.load(Ordering::SeqCst), 1, "refetch is rate-limited");

    cache.keys_fetched_at = cache
        .keys_fetched_at
        .checked_sub(Duration::from_secs(61))
        .unwrap();
    let refetched = cache.verify(&client, &rotated, AUDIENCE, NONCE, NOW).await;
    assert_eq!(refetched.unwrap().sub, "user-1", "the rotated key is used");
    assert_eq!(
        hits.load(Ordering::SeqCst),
        2,
        "one refetch after the interval"
    );
    let unknown = sign(&json!({"alg": "RS256", "kid": "gone"}), &claims);
    let unknown = cache.verify(&client, &unknown, AUDIENCE, NONCE, NOW).await;
    assert_eq!(unknown.unwrap_err(), OidcError::UnknownKey);
    assert_eq!(hits.load(Ordering::SeqCst), 2, "refetch stays rate-limited");
    assert_eq!(cache.discovery().issuer, base);
}
