#![allow(clippy::unwrap_used, clippy::expect_used)]

#[path = "operator_cli/concurrency.rs"]
mod concurrency;
#[path = "migrations/support.rs"]
mod database;
#[path = "operator_cli/failure.rs"]
mod failure;
#[path = "operator_cli/input.rs"]
mod input;
#[path = "operator_cli/lifecycle.rs"]
mod lifecycle;

use serde_json::Value;
use std::{
    fs,
    io::Write,
    path::PathBuf,
    process::{Command, Output, Stdio},
};

const BIN: &str = env!("CARGO_BIN_EXE_llmproxy");
const ALICE_KEY: &str = "fake-anthropic-cli-alice-9";
const BOB_KEY: &str = "fake-anthropic-cli-bob-9";

struct Operator {
    db: database::Database,
    old: PathBuf,
    new: PathBuf,
    fingerprint: PathBuf,
}

impl Operator {
    async fn new() -> Self {
        let db = database::Database::new().await;
        let old = db.path.with_extension("old.key");
        let new = db.path.with_extension("new.key");
        let fingerprint = db.path.with_extension("fingerprint.key");
        for (path, byte) in [(&old, 1), (&new, 2), (&fingerprint, 3)] {
            fs::write(path, [byte; 32]).unwrap();
        }
        let text = format!(
            "{}\n[credential_encryption]\nactive_key_id='old'\nfingerprint_key_file='{}'\n[credential_encryption.keys]\nold='{}'\nnew='{}'\n",
            fs::read_to_string(&db.path).unwrap(),
            fingerprint.display(),
            old.display(),
            new.display()
        );
        fs::write(&db.path, text).unwrap();
        let operator = Self {
            db,
            old,
            new,
            fingerprint,
        };
        let output = operator.run(&["db", "migrate"], None);
        assert!(output.status.success(), "{output:?}");
        operator
    }

    fn command(&self, args: &[&str]) -> Command {
        let mut command = Command::new(BIN);
        command
            .args(args)
            .env("LLMPROXY_CONFIG", &self.db.path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        command
    }

    fn run(&self, args: &[&str], input: Option<&str>) -> Output {
        let mut child = self.command(args).spawn().unwrap();
        if let Some(input) = input {
            child
                .stdin
                .take()
                .unwrap()
                .write_all(input.as_bytes())
                .unwrap();
        }
        let output = child.wait_with_output().unwrap();
        for key in [ALICE_KEY, BOB_KEY] {
            for bytes in [&output.stdout, &output.stderr] {
                assert!(!String::from_utf8_lossy(bytes).contains(key));
            }
        }
        output
    }

    fn ok(&self, args: &[&str], input: Option<&str>) -> Value {
        let output = self.run(args, input);
        assert!(output.status.success(), "{output:?}");
        assert!(output.stderr.is_empty(), "{output:?}");
        serde_json::from_slice(&output.stdout).unwrap()
    }

    fn person(&self, label: &str) -> String {
        self.ok(&["auth", "person", "create", label], None)
            .get("person_id")
            .unwrap()
            .as_str()
            .unwrap()
            .to_owned()
    }

    async fn close(self) {
        for path in [&self.old, &self.new, &self.fingerprint] {
            fs::remove_file(path).unwrap();
        }
        self.db.close().await;
    }
}

fn artifact(name: &str, bytes: &[u8]) {
    if let Ok(dir) = std::env::var("LLMPROXY_OPERATOR_EVIDENCE") {
        fs::write(PathBuf::from(dir).join(name), bytes).unwrap();
    }
}
