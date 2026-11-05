#![allow(clippy::unwrap_used, clippy::expect_used)]

#[path = "claude/cancellation.rs"]
mod cancellation;
#[path = "claude/conflict_metrics.rs"]
mod conflict_metrics;
#[path = "common/database.rs"]
mod database;
#[path = "common/database_close.rs"]
mod database_close;
#[path = "claude/edges.rs"]
mod edges;
#[path = "claude/heartbeat_metrics.rs"]
mod heartbeat_metrics;
#[path = "claude/lifecycle.rs"]
mod lifecycle;
#[path = "claude/native.rs"]
mod native;
#[path = "common/pg_dump.rs"]
mod pg_dump;
#[path = "claude/runtime_metrics.rs"]
mod runtime_metrics;
#[path = "claude/support.rs"]
mod support;
