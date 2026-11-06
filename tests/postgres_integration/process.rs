#[path = "../common/healthy.rs"]
mod healthy;
#[path = "../common/in_process.rs"]
mod in_process;
#[path = "../common/shutdown.rs"]
mod shutdown;
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
};

/// One proxy replica: a spawned binary, or a `Proxy` served inside the test process.
pub struct Replica {
    pub base: String,
    run: Run,
}

enum Run {
    Binary { child: Child, config: PathBuf },
    // Dropping it stops the proxy's runtime.
    InProcess(#[expect(dead_code, reason = "held only to be dropped")] in_process::InProcess),
}

impl Replica {
    pub fn pid(&self) -> u32 {
        match &self.run {
            Run::Binary { child, .. } => child.id(),
            Run::InProcess(_) => unreachable!("an in-process replica has no child process"),
        }
    }

    pub fn config(&self) -> PathBuf {
        match &self.run {
            Run::Binary { config, .. } => config.clone(),
            Run::InProcess(_) => unreachable!("an in-process replica has no config file"),
        }
    }

    /// Serves a `Proxy` parsed from `text` inside the test process.
    pub fn in_process(text: &str) -> Self {
        let served = in_process::InProcess::start(text);
        Self {
            base: served.base.clone(),
            run: Run::InProcess(served),
        }
    }

    pub async fn start(config: &Path) -> Self {
        let text = fs::read_to_string(config).unwrap();
        let parsed: toml::Value = toml::from_str(&text).unwrap();
        let address = parsed.get("listen").unwrap().as_str().unwrap();
        // Append so a restarted replica keeps the earlier run's output for the leak scan.
        let log = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(config.with_extension("log"))
            .unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_llmproxy"))
            .arg(config)
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap();
        // Built first so a failed health check still stops the child on drop.
        let mut replica = Self {
            base: format!("http://{address}"),
            run: Run::Binary {
                child,
                config: config.into(),
            },
        };
        if let Run::Binary { child, .. } = &mut replica.run {
            healthy::wait_healthy(&replica.base, child, super::WAIT).await;
        }
        replica
    }

    pub fn kill(&mut self) {
        let Run::Binary { child, .. } = &mut self.run else {
            unreachable!("an in-process replica has no child process")
        };
        child.kill().unwrap();
        child.wait().unwrap();
    }
}

impl Drop for Replica {
    fn drop(&mut self) {
        if let Run::Binary { child, .. } = &mut self.run {
            shutdown::stop(child);
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
