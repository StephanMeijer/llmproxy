use super::{
    BTreeMap, CredentialContext, KeyRing, ProxyToken, SecretBytes, SecretError, TestResult, Uuid,
    context, credential_fingerprint, lookup_tag, ring,
};

#[test]
fn secret_roundtrip_and_nonce_uniqueness() -> TestResult {
    let ring = ring("first", &[("first", 1)])?;
    let plaintext = SecretBytes::new(b"test-only-provider-credential".to_vec());
    let first = ring.encrypt(&context(), &plaintext)?;
    let second = ring.encrypt(&context(), &plaintext)?;
    assert!(first.nonce != second.nonce);
    assert!(first.ciphertext != second.ciphertext);
    assert!(ring.decrypt(&context(), &first)?.expose() == plaintext.expose());
    let stored = serde_json::to_string(&first)?;
    assert!(!stored.contains(std::str::from_utf8(plaintext.expose())?));
    Ok(())
}

#[test]
fn secret_context_fields_are_authenticated() -> TestResult {
    let ring = ring("first", &[("first", 1)])?;
    let envelope = ring.encrypt(&context(), &SecretBytes::new(vec![42]))?;
    for wrong in [
        CredentialContext {
            namespace: "other",
            ..context()
        },
        CredentialContext {
            person: Uuid::new_v4(),
            ..context()
        },
        CredentialContext {
            provider: "other",
            ..context()
        },
    ] {
        assert!(matches!(
            ring.decrypt(&wrong, &envelope),
            Err(SecretError::Authentication)
        ));
    }
    Ok(())
}

#[test]
fn secret_tampering_wrong_keys_and_versions_fail_closed() -> TestResult {
    let ring = ring("first", &[("first", 1)])?;
    let envelope = ring.encrypt(&context(), &SecretBytes::new(vec![42]))?;
    let mut tampered = envelope.clone();
    tampered.ciphertext.push(0);
    assert!(matches!(
        ring.decrypt(&context(), &tampered),
        Err(SecretError::Authentication)
    ));
    tampered = envelope.clone();
    tampered.nonce = [0; 12];
    assert!(matches!(
        ring.decrypt(&context(), &tampered),
        Err(SecretError::Authentication)
    ));
    tampered = envelope.clone();
    tampered.version = 2;
    assert!(matches!(
        ring.decrypt(&context(), &tampered),
        Err(SecretError::UnsupportedVersion)
    ));
    tampered = envelope.clone();
    tampered.key_id = "missing".to_owned();
    assert!(matches!(
        ring.decrypt(&context(), &tampered),
        Err(SecretError::UnknownKey)
    ));
    let wrong = KeyRing::new(
        "first",
        BTreeMap::from([("first".to_owned(), SecretBytes::new(vec![2; 32]))]),
    )?;
    assert!(matches!(
        wrong.decrypt(&context(), &envelope),
        Err(SecretError::Authentication)
    ));
    Ok(())
}

#[test]
fn secret_rotation_keeps_old_readable_and_reencrypts_to_active_key() -> TestResult {
    let old = ring("first", &[("first", 1)])?;
    let rolling = ring("second", &[("first", 1), ("second", 2)])?;
    let retired = ring("second", &[("second", 2)])?;
    let plaintext = SecretBytes::new(vec![42]);
    let envelope = old.encrypt(&context(), &plaintext)?;
    let migrated = rolling.reencrypt(&context(), &envelope)?;
    assert_eq!(migrated.key_id, "second");
    assert!(retired.decrypt(&context(), &migrated)?.expose() == plaintext.expose());
    assert!(matches!(
        retired.decrypt(&context(), &envelope),
        Err(SecretError::UnknownKey)
    ));
    Ok(())
}

#[test]
fn secret_keyed_fingerprints_and_lookup_tags_are_stable_and_pepper_bound() -> TestResult {
    let pepper = SecretBytes::new(vec![1; 32]);
    let other = SecretBytes::new(vec![2; 32]);
    let plaintext = SecretBytes::new(vec![42]);
    let token = ProxyToken::generate()?;
    assert_eq!(
        lookup_tag(&pepper, &token)?,
        lookup_tag(&pepper, &ProxyToken::parse(token.expose())?)?
    );
    assert_ne!(lookup_tag(&pepper, &token)?, lookup_tag(&other, &token)?);
    assert_eq!(
        credential_fingerprint(&pepper, &plaintext)?,
        credential_fingerprint(&pepper, &SecretBytes::new(vec![42]))?
    );
    assert_ne!(
        credential_fingerprint(&pepper, &plaintext)?,
        credential_fingerprint(&other, &plaintext)?
    );
    assert_ne!(
        credential_fingerprint(
            &pepper,
            &SecretBytes::new(token.expose().as_bytes().to_vec())
        )?,
        lookup_tag(&pepper, &token)?
    );
    assert_eq!(
        credential_fingerprint(&SecretBytes::new(vec![1]), &plaintext),
        Err(SecretError::InvalidKey)
    );
    Ok(())
}

#[test]
fn secret_redacts_debug_errors_and_serialized_envelopes() -> TestResult {
    let secret = SecretBytes::new(b"test-only-provider-credential".to_vec());
    let ring = ring("first", &[("first", 1)])?;
    let token = ProxyToken::generate()?;
    let envelope = ring.encrypt(&context(), &secret)?;
    let output = format!(
        "{secret:?} {ring:?} {token:?} {envelope:?} {} {:?} {}",
        SecretError::Authentication,
        SecretError::KeyFile,
        serde_json::to_string(&envelope)?
    );
    assert!(!output.contains(std::str::from_utf8(secret.expose())?));
    assert!(!output.contains(token.expose()));
    Ok(())
}

#[test]
fn secret_rejects_invalid_key_rings_and_unreadable_files() {
    assert!(matches!(
        KeyRing::new("missing", BTreeMap::new()),
        Err(SecretError::UnknownKey)
    ));
    assert!(matches!(
        KeyRing::new(
            "bad/id",
            BTreeMap::from([("bad/id".to_owned(), SecretBytes::new(vec![1; 32]))])
        ),
        Err(SecretError::InvalidKey)
    ));
    assert!(matches!(
        KeyRing::new(
            "first",
            BTreeMap::from([("first".to_owned(), SecretBytes::new(vec![1]))])
        ),
        Err(SecretError::InvalidKey)
    ));
    let missing = std::env::temp_dir().join(Uuid::new_v4().to_string());
    assert!(matches!(
        KeyRing::load("first", &BTreeMap::from([("first".to_owned(), missing)])),
        Err(SecretError::KeyFile)
    ));
}
