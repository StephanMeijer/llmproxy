use super::support::*;
use axum::http::StatusCode;
use llmproxy::secret::SecretBytes;

#[tokio::test]
async fn invalid_fingerprint_file_fails_closed_before_provider_dispatch() {
    let mut f = Fixture::new().await;
    std::fs::write(f.db.path.with_extension("pepper"), [2; 31]).unwrap();
    let a = f.binary("", "").await;
    assert_eq!(
        f.request(&a, false).send().await.unwrap().status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(f.hits.try_recv().is_err());
    f.close(vec![a]).await;
}

#[tokio::test]
async fn stored_credential_with_header_control_bytes_never_reaches_provider() {
    let mut f = Fixture::new().await;
    f.credentials
        .replace(
            f.alice_version,
            &SecretBytes::new(b"invalid\nheader".to_vec()),
        )
        .await
        .unwrap();
    let a = f.binary("", "").await;
    assert_eq!(
        f.request(&a, false).send().await.unwrap().status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(f.hits.try_recv().is_err());
    f.close(vec![a]).await;
}
