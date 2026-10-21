CREATE TABLE people (
    namespace text NOT NULL,
    id uuid NOT NULL,
    display_name text NOT NULL,
    enabled boolean NOT NULL DEFAULT true,
    version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (namespace, id)
);

CREATE TABLE proxy_tokens (
    namespace text NOT NULL,
    id uuid NOT NULL,
    person_id uuid NOT NULL,
    secret_hash text NOT NULL CHECK (secret_hash LIKE '$argon2id$%'),
    revoked_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (namespace, id),
    FOREIGN KEY (namespace, person_id) REFERENCES people(namespace, id)
);
CREATE INDEX proxy_tokens_person ON proxy_tokens(namespace, person_id);

CREATE TABLE provider_credentials (
    namespace text NOT NULL,
    person_id uuid NOT NULL,
    provider text NOT NULL CHECK (provider = 'anthropic'),
    generation bigint NOT NULL DEFAULT 1 CHECK (generation > 0),
    state text NOT NULL CHECK (state IN ('active', 'invalid', 'removed')),
    format_version integer,
    key_id text,
    nonce bytea,
    ciphertext bytea,
    fingerprint bytea,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (namespace, person_id, provider),
    UNIQUE (namespace, provider, fingerprint),
    FOREIGN KEY (namespace, person_id) REFERENCES people(namespace, id),
    CHECK ((state = 'removed' AND format_version IS NULL AND key_id IS NULL AND nonce IS NULL
                AND ciphertext IS NULL AND fingerprint IS NULL)
        OR (state IN ('active', 'invalid') AND format_version IS NOT NULL AND format_version = 1 AND key_id IS NOT NULL
                AND nonce IS NOT NULL AND octet_length(nonce) = 12
                AND ciphertext IS NOT NULL AND octet_length(ciphertext) >= 16
                AND fingerprint IS NOT NULL AND octet_length(fingerprint) = 32))
);

CREATE FUNCTION advance_credential_generation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.generation := OLD.generation + 1;
    NEW.updated_at := clock_timestamp();
    RETURN NEW;
END;
$$;
CREATE TRIGGER credential_generation BEFORE UPDATE ON provider_credentials
    FOR EACH ROW EXECUTE FUNCTION advance_credential_generation();

CREATE TABLE limit_definitions (
    namespace text NOT NULL,
    person_id uuid NOT NULL,
    provider text NOT NULL CHECK (provider = 'anthropic'),
    name text NOT NULL,
    config jsonb NOT NULL CHECK (jsonb_typeof(config) = 'object'),
    version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
    next_enqueue bigint NOT NULL DEFAULT 0 CHECK (next_enqueue >= 0),
    PRIMARY KEY (namespace, person_id, provider, name),
    FOREIGN KEY (namespace, person_id) REFERENCES people(namespace, id)
);

CREATE TABLE rate_state (
    namespace text NOT NULL,
    person_id uuid NOT NULL,
    provider text NOT NULL,
    limit_name text NOT NULL,
    kind text NOT NULL CHECK (kind IN ('requests', 'tokens')),
    available double precision NOT NULL CHECK (available >= 0 AND available < 'Infinity'::double precision),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (namespace, person_id, provider, limit_name, kind),
    FOREIGN KEY (namespace, person_id, provider, limit_name)
        REFERENCES limit_definitions(namespace, person_id, provider, name)
);

CREATE TABLE fifo_waiters (
    namespace text NOT NULL,
    person_id uuid NOT NULL,
    provider text NOT NULL,
    limit_name text NOT NULL,
    id uuid NOT NULL,
    owner uuid NOT NULL,
    enqueue_order bigint NOT NULL CHECK (enqueue_order > 0),
    expires_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (namespace, id),
    UNIQUE (namespace, person_id, provider, limit_name, enqueue_order),
    FOREIGN KEY (namespace, person_id, provider, limit_name)
        REFERENCES limit_definitions(namespace, person_id, provider, name)
);
CREATE INDEX fifo_waiters_cleanup ON fifo_waiters(expires_at);

-- UPDATE locks the definition until transaction end: successful enqueue order is commit-serialized.
CREATE FUNCTION assign_enqueue_order() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE limit_definitions SET next_enqueue = next_enqueue + 1
        WHERE namespace = NEW.namespace AND person_id = NEW.person_id
            AND provider = NEW.provider AND name = NEW.limit_name
        RETURNING next_enqueue INTO NEW.enqueue_order;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'limit definition missing' USING ERRCODE = '23503';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER fifo_enqueue BEFORE INSERT ON fifo_waiters
    FOR EACH ROW EXECUTE FUNCTION assign_enqueue_order();

CREATE TABLE concurrency_leases (
    namespace text NOT NULL,
    person_id uuid NOT NULL,
    provider text NOT NULL,
    limit_name text NOT NULL,
    id uuid NOT NULL,
    owner uuid NOT NULL,
    expires_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (namespace, id),
    FOREIGN KEY (namespace, person_id, provider, limit_name)
        REFERENCES limit_definitions(namespace, person_id, provider, name)
);
CREATE INDEX concurrency_leases_count ON concurrency_leases(namespace, person_id, provider, limit_name, expires_at);
CREATE INDEX concurrency_leases_cleanup ON concurrency_leases(expires_at);

-- Notifications carry only identifiers and versions; consumers re-read durable state after gaps.
CREATE FUNCTION notify_credential_change() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('llmproxy_credentials', json_build_object(
        'namespace', NEW.namespace, 'person_id', NEW.person_id,
        'provider', NEW.provider, 'generation', NEW.generation)::text);
    RETURN NEW;
END;
$$;
CREATE TRIGGER credential_change AFTER INSERT OR UPDATE ON provider_credentials
    FOR EACH ROW EXECUTE FUNCTION notify_credential_change();
