use llmproxy::secret::{
    CredentialContext, KeyRing, ProxyToken, SecretBytes, SecretError, TokenHasher,
    credential_fingerprint, lookup_tag,
};
use std::{collections::BTreeMap, num::NonZeroUsize};
use uuid::Uuid;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn ring(active: &str, keys: &[(&str, u8)]) -> Result<KeyRing, SecretError> {
    KeyRing::new(
        active,
        keys.iter()
            .map(|(id, byte)| ((*id).to_owned(), SecretBytes::new(vec![*byte; 32])))
            .collect(),
    )
}

const fn context() -> CredentialContext<'static> {
    CredentialContext {
        namespace: "test",
        person: Uuid::nil(),
        provider: "anthropic",
    }
}

#[tokio::test]
async fn secret_token_hash_roundtrip_and_unique_salts() -> TestResult {
    let token = ProxyToken::generate()?;
    let parsed = ProxyToken::parse(token.expose())?;
    let hasher = TokenHasher::new(NonZeroUsize::MIN);
    let first = hasher.hash(&token).await?;
    let second = hasher.hash(&token).await?;
    assert_eq!(parsed.id(), token.id());
    assert!(first != second);
    assert!(first.starts_with("$argon2id$v=19$"));
    assert!(hasher.verify(&parsed, &first).await?);
    assert!(!hasher.verify(&ProxyToken::generate()?, &first).await?);
    Ok(())
}

#[test]
fn secret_rejects_malformed_tokens() {
    for token in [
        "",
        "not-a-token",
        "00000000-0000-0000-0000-000000000000.short",
        "00000000-0000-0000-0000-000000000000.~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~",
    ] {
        assert!(matches!(
            ProxyToken::parse(token),
            Err(SecretError::InvalidToken)
        ));
    }
}

#[tokio::test]
async fn secret_rejects_malformed_or_unbounded_hash_parameters() -> TestResult {
    let token = ProxyToken::generate()?;
    let hasher = TokenHasher::new(NonZeroUsize::MIN);
    let hash = hasher.hash(&token).await?;
    for invalid in [
        "bad".to_owned(),
        "$argon2id$v=19$m=19456,t=2,p=1$c2FsdA$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
            .to_owned(),
        hash.replace("m=19456", "m=4294967295"),
        hash.replace("t=2", "t=4294967295"),
        hash.replace("p=1", "p=4294967295"),
        hash.replace("argon2id", "argon2i"),
        hash.replace("v=19", "v=16"),
    ] {
        assert_eq!(
            hasher.verify(&token, &invalid).await,
            Err(SecretError::InvalidHash)
        );
    }
    Ok(())
}

#[test]
fn secret_saturation_survives_caller_cancellation() -> TestResult {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(1)
        .build()?;
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    runtime.block_on(async {
        let hasher = TokenHasher::new(NonZeroUsize::MIN);
        let token = ProxyToken::generate()?;
        let hash = hasher.hash(&token).await?;
        let blocker = tokio::task::spawn_blocking(move || release_rx.recv());
        let mut hashing = Box::pin(hasher.hash(&token));
        assert!(futures_util::poll!(&mut hashing).is_pending());
        drop(hashing);
        assert_eq!(hasher.hash(&token).await, Err(SecretError::Busy));
        assert_eq!(hasher.verify(&token, &hash).await, Err(SecretError::Busy));
        release_tx.send(())?;
        blocker.await??;
        Ok::<(), Box<dyn std::error::Error>>(())
    })?;
    runtime.shutdown_timeout(std::time::Duration::from_secs(10));
    Ok(())
}

#[tokio::test]
async fn secret_verifies_stored_nondefault_parameters() -> TestResult {
    use argon2::{Algorithm, Argon2, Params, Version, password_hash::PasswordHasher};
    let token = ProxyToken::generate()?;
    let algorithm = Argon2::new(
        Algorithm::Argon2id,
        Version::V0x13,
        Params::new(32768, 3, 1, None)?,
    );
    let hash = algorithm
        .hash_password_with_salt(token.expose().as_bytes(), b"test-only-salt-16")?
        .to_string();
    assert!(
        TokenHasher::new(NonZeroUsize::MIN)
            .verify(&token, &hash)
            .await?
    );
    Ok(())
}

#[path = "secret/encryption.rs"]
mod encryption;

#[test]
fn key_ring_loads_exact_32_byte_files_and_every_error_renders_redacted() -> TestResult {
    let dir = std::env::temp_dir().join(format!("llmproxy-keys-{}", Uuid::new_v4().simple()));
    std::fs::create_dir_all(&dir)?;
    let good = dir.join("good.key");
    let short = dir.join("short.key");
    let long = dir.join("long.key");
    std::fs::write(&good, [7u8; 32])?;
    std::fs::write(&short, [7u8; 31])?;
    std::fs::write(&long, [7u8; 33])?;
    let paths = |pairs: &[(&str, &std::path::Path)]| -> BTreeMap<String, std::path::PathBuf> {
        pairs
            .iter()
            .map(|(id, path)| ((*id).to_owned(), path.to_path_buf()))
            .collect()
    };
    assert!(KeyRing::load("good", &paths(&[("good", &good)])).is_ok());
    for (active, files, expected) in [
        (
            "good",
            vec![("good", good.as_path()), ("short", short.as_path())],
            SecretError::InvalidKey,
        ),
        (
            "good",
            vec![("good", good.as_path()), ("long", long.as_path())],
            SecretError::InvalidKey,
        ),
        (
            "good",
            vec![("good", dir.join("missing.key").as_path())],
            SecretError::KeyFile,
        ),
        (
            "other",
            vec![("good", good.as_path())],
            SecretError::UnknownKey,
        ),
    ] {
        assert_eq!(KeyRing::load(active, &paths(&files)).err(), Some(expected));
    }
    std::fs::remove_dir_all(&dir)?;
    let errors = [
        SecretError::Randomness,
        SecretError::InvalidToken,
        SecretError::InvalidHash,
        SecretError::Busy,
        SecretError::Worker,
        SecretError::InvalidKey,
        SecretError::KeyFile,
        SecretError::UnknownKey,
        SecretError::UnsupportedVersion,
        SecretError::Authentication,
    ];
    let messages: std::collections::BTreeSet<String> =
        errors.iter().map(ToString::to_string).collect();
    assert_eq!(messages.len(), errors.len());
    assert!(
        messages
            .iter()
            .all(|m| !m.is_empty() && !m.contains(".key"))
    );
    Ok(())
}
