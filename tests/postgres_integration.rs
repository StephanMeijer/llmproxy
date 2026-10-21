#![allow(clippy::unwrap_used, clippy::expect_used)]

#[path = "postgres_integration/acknowledgement.rs"]
mod acknowledgement;
#[path = "postgres_integration/admission.rs"]
mod admission;
#[path = "postgres_integration/database.rs"]
mod database;
#[path = "postgres_integration/faults.rs"]
mod faults;
#[path = "postgres_integration/fixture.rs"]
mod fixture;
#[path = "postgres_integration/lifecycle.rs"]
mod lifecycle;
#[path = "postgres_integration/process.rs"]
mod process;
#[path = "postgres_integration/provider.rs"]
mod provider;
#[path = "postgres_integration/server_crash.rs"]
mod server_crash;

const WAIT: std::time::Duration = std::time::Duration::from_secs(45);
const BODY: &str = r#"{"model":"claude-test","max_tokens":25,"messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","content":"ok"}]}],"tools":[{"name":"weather","input_schema":{"type":"object"}}]}"#;
const LIMIT: &str = "[[limit]]\nname='shared'\nprefixes=['/claude/']\nmax_inflight=1\n";
