use std::{fs, path::PathBuf};

/// Writes a debug artifact under `$LLMPROXY_TEST_EVIDENCE/<test crate>/` only when that variable is set.
pub fn artifact(name: &str, bytes: impl AsRef<[u8]>) {
    if let Some(dir) = std::env::var_os("LLMPROXY_TEST_EVIDENCE") {
        let dir = PathBuf::from(dir).join(env!("CARGO_CRATE_NAME"));
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join(name), bytes).unwrap();
    }
}
