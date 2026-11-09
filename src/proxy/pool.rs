//! Free-model pools: one endpoint that fails over across models proven free by their catalog.

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "wired up by the catalog refresh in the next PR of the stack"
    )
)]
mod proof;
