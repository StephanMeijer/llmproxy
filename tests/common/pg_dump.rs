use std::{env, process::Command};

/// Reads the container up front so a missing variable fails before the test's scenario runs.
pub fn credential_container() -> String {
    env::var("LLMPROXY_CREDENTIAL_CONTAINER").expect(
        "set LLMPROXY_CREDENTIAL_CONTAINER to the container ID that serves LLMPROXY_TEST_DATABASE_URL",
    )
}

/// Dumps the database named in `db_url` from inside `container`.
pub fn pg_dump(container: &str, db_url: &str) -> Vec<u8> {
    let url = reqwest::Url::parse(db_url).unwrap();
    let dump = Command::new("docker")
        .args([
            "exec",
            container,
            "pg_dump",
            "-U",
            "postgres",
            url.path().trim_start_matches('/'),
        ])
        .output()
        .unwrap();
    assert!(dump.status.success(), "pg_dump failed: {dump:?}");
    dump.stdout
}
