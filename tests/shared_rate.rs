#![allow(clippy::unwrap_used, clippy::expect_used)]

#[path = "shared_rate/budgets.rs"]
mod budgets;
#[path = "common/database.rs"]
mod database;
#[path = "common/database_close.rs"]
mod database_close;
#[path = "shared_rate/deadline.rs"]
mod deadline;
#[path = "shared_rate/deadline_bounds.rs"]
mod deadline_bounds;
#[path = "common/eventually.rs"]
mod eventually;
#[path = "shared_rate/expiry.rs"]
mod expiry;
#[path = "shared_rate/fifo.rs"]
mod fifo;
#[path = "shared_rate/manual.rs"]
mod manual;
#[path = "shared_rate/support.rs"]
mod support;
#[path = "shared_rate/try_admit.rs"]
mod try_admit;

use support::*;
