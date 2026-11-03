#![allow(clippy::unwrap_used, clippy::expect_used)]

#[path = "claude/cancellation.rs"]
mod cancellation;
#[path = "migrations/support.rs"]
mod database;
#[path = "claude/edges.rs"]
mod edges;
#[path = "claude/heartbeat_metrics.rs"]
mod heartbeat_metrics;
#[path = "claude/lifecycle.rs"]
mod lifecycle;
#[path = "claude/native.rs"]
mod native;
#[path = "claude/runtime_metrics.rs"]
mod runtime_metrics;
#[path = "claude/support.rs"]
mod support;
