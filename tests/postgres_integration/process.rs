use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
};

pub struct Replica {
    child: Child,
    pub base: String,
    pub config: PathBuf,
}

impl Replica {
    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    pub async fn start(config: &Path) -> Self {
        let text = fs::read_to_string(config).unwrap();
        let parsed: toml::Value = toml::from_str(&text).unwrap();
        let address = parsed.get("listen").unwrap().as_str().unwrap();
        let log = fs::File::create(config.with_extension("log")).unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_llmproxy"))
            .arg(config)
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap();
        let mut replica = Self {
            child,
            base: format!("http://{address}"),
            config: config.into(),
        };
        tokio::time::timeout(super::WAIT, async {
            let client = reqwest::Client::new();
            loop {
                assert!(
                    replica.child.try_wait().unwrap().is_none(),
                    "replica exited before healthy"
                );
                if client
                    .get(format!("{}/healthz", replica.base))
                    .send()
                    .await
                    .is_ok_and(|r| r.status().is_success())
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        replica
    }

    pub fn kill(&mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
    }
}

impl Drop for Replica {
    fn drop(&mut self) {
        if self.child.try_wait().unwrap().is_none() {
            self.kill();
        }
    }
}

pub fn cli(config: &Path, args: &[&str], input: Option<&str>) -> std::process::Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_llmproxy"))
        .args(args)
        .env("LLMPROXY_CONFIG", config)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    if let Some(input) = input {
        child
            .stdin
            .take()
            .unwrap()
            .write_all(input.as_bytes())
            .unwrap();
    }
    child.wait_with_output().unwrap()
}

pub fn ok(config: &Path, args: &[&str], input: Option<&str>) -> serde_json::Value {
    let output = cli(config, args, input);
    assert!(
        output.status.success(),
        "operator command failed (output withheld)"
    );
    assert!(output.stderr.is_empty(), "operator stderr must be empty");
    serde_json::from_slice(&output.stdout).unwrap()
}
