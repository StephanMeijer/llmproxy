use super::*;
use std::{
    process::{Command, Stdio},
    time::Duration,
};

#[tokio::test]
async fn killed_rotation_restarts_idempotently_without_losing_credentials() {
    // Given two old-key rows, with the second locked to stop the CLI between commits.
    let (db, store, alice, bob) = setup().await;
    let credentials = service(&store, ring("old", &[("old", 1)]));
    credentials
        .install(alice, &SecretBytes::new(ALICE.to_vec()))
        .await
        .unwrap();
    credentials
        .install(bob, &SecretBytes::new(BOB.to_vec()))
        .await
        .unwrap();
    assert_eq!(key_references(&db, "old").await, 2);
    let config = encryption(&db);
    let text = format!(
        "{}\n[credential_encryption]\nfingerprint_key_file = '{}'\nactive_key_id='new'\n[credential_encryption.keys]\nold='{}'\nnew='{}'\n",
        fs::read_to_string(&db.path).unwrap(),
        config.fingerprint_key_file.display(),
        config.keys.get("old").unwrap().display(),
        config.keys.get("new").unwrap().display()
    );
    fs::write(&db.path, text).unwrap();
    let mut lock = db.pool.begin().await.unwrap();
    sqlx::query("SELECT person_id FROM provider_credentials WHERE person_id=$1 FOR UPDATE")
        .bind(bob)
        .fetch_one(&mut *lock)
        .await
        .unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_llmproxy"))
        .args(["auth", "credentials", "reencrypt", "--to", "new"])
        .env("LLMPROXY_CONFIG", &db.path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    super::eventually::count_eventually(
        &db.pool,
        "SELECT count(*) FROM provider_credentials WHERE key_id='new'",
        1,
        Duration::from_secs(10),
    )
    .await;
    let argv = fs::read(format!("/proc/{}/cmdline", child.id())).unwrap();
    assert!(!argv.is_empty());
    // When killed after one committed row and restarted with the same command.
    child.kill().unwrap();
    let killed = child.wait_with_output().unwrap();
    assert!(!killed.status.success());
    lock.rollback().await.unwrap();
    let restarted = Command::new(env!("CARGO_BIN_EXE_llmproxy"))
        .args(["auth", "credentials", "reencrypt", "--to", "new"])
        .env("LLMPROXY_CONFIG", &db.path)
        .output()
        .unwrap();
    assert!(restarted.status.success());
    assert_eq!(restarted.stdout, b"{\"rewritten\":1,\"remaining\":0}\n");
    // Then only the new key is referenced, both secrets survive, and another run is a no-op.
    assert_eq!(key_references(&db, "old").await, 0);
    assert_eq!(key_references(&db, "new").await, 2);
    let repeated = Credentials::open(store.clone(), &config)
        .unwrap()
        .reencrypt()
        .await
        .unwrap();
    assert_eq!(repeated.rewritten, 0);
    assert_eq!(repeated.remaining, 0);
    let new_only = service(&store, ring("new", &[("new", 2)]));
    assert_eq!(
        new_only.snapshot(alice).await.unwrap().secret.expose(),
        ALICE
    );
    assert_eq!(new_only.snapshot(bob).await.unwrap().secret.expose(), BOB);
    for bytes in [
        &argv,
        &killed.stdout,
        &killed.stderr,
        &restarted.stdout,
        &restarted.stderr,
    ] {
        assert!(!bytes.windows(ALICE.len()).any(|v| v == ALICE));
        assert!(!bytes.windows(BOB.len()).any(|v| v == BOB));
    }
    artifact("rotation-argv.bin", &argv);
    artifact("rotation-stdout.txt", &restarted.stdout);
    artifact("rotation.txt", b"PASS: killed real CLI after one commit with second row locked; restarted rewrites one remaining row; repeat rewrites zero; old references zero; new-only ring decrypts both rows; argv/stdout/stderr contain no seeded credential\n");
    for path in config.keys.values().chain([&config.fingerprint_key_file]) {
        fs::remove_file(path).unwrap();
    }
    store.close().await;
    db.close().await;
}
