ALTER TABLE concurrency_leases ADD COLUMN ownership_generation bigint NOT NULL DEFAULT 1
    CHECK (ownership_generation > 0);
