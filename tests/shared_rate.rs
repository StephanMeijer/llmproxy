#![allow(clippy::unwrap_used, clippy::expect_used)]

#[path = "shared_rate/budgets.rs"]
mod budgets;
#[path = "migrations/support.rs"]
mod database;
#[path = "shared_rate/deadline.rs"]
mod deadline;
#[path = "shared_rate/deadline_bounds.rs"]
mod deadline_bounds;
#[path = "shared_rate/fifo.rs"]
mod fifo;
#[path = "shared_rate/manual.rs"]
mod manual;
#[path = "shared_rate/support.rs"]
mod support;

use support::*;
