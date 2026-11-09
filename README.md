# llmproxy

An authenticated, rate-limited reverse proxy between LLM clients and provider APIs. Static routes use server-configured provider keys. The optional Claude adapter gives each person a proxy token and an encrypted, operator-installed Anthropic API key, with PostgreSQL-backed limits shared across replicas.

- **Credential isolation:** the client's `Authorization` and `x-api-key` are dropped and replaced by the provider credential you configure.
- **Upstream protection:** requests above a limit are held in arrival order until budget frees, instead of being rejected. The client sees `429` only after `max_wait`.
- **Compatibility shim:** optionally adds `tool_choice: "auto"` for providers that need it.
- **Streaming:** server-sent events pass through unbuffered.

It speaks plain HTTP. Put a TLS-terminating reverse proxy or ingress in front of it.

## Quick start

```sh
cargo build --release

export PROXY_TOKEN="$(openssl rand -hex 32)"
export MINIMAX_API_KEY=...            # only the variables your config references
cp config.example.toml config.toml    # edit routes and limits

target/release/llmproxy --check-config config.toml   # validate, do not serve
target/release/llmproxy config.toml
```

For serving and `--check-config`, the optional config path overrides `$LLMPROXY_CONFIG`, which defaults to `/etc/llmproxy/config.toml`. Operator commands use the environment/default path.

Then call a provider through the proxy:

```sh
curl http://localhost:8788/minimax/chat/completions \
  -H "Authorization: Bearer $PROXY_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"model": "...", "messages": [{"role": "user", "content": "hi"}]}'
```

`x-api-key: $PROXY_TOKEN` works as well. A request to `/<name>/<rest>` is forwarded to the route `<name>`'s upstream as `<upstream>/<rest>`, query string included.

## Configuration

See [`config.example.toml`](config.example.toml) for a complete, commented file. Unknown keys are rejected, so a typo fails at startup instead of being ignored.

Any string value may contain `${VAR}`, filled in from the environment. An unset variable stops startup, and an empty `proxy_token` is refused. To write a literal `${`, write `$${`; a lone `$` or `$$` needs no escape.

### Top level

| Key | Default | Meaning |
|---|---|---|
| `listen` | required | Address for client traffic, e.g. `"0.0.0.0:8788"`. |
| `metrics_listen` | none | Address for Prometheus metrics. No metrics listener when unset. |
| `proxy_token` | required | Legacy shared token for static routes; cannot access Claude. |
| `max_wait` | `"120s"` | Longest a request is held by a limit before `429`. Whole seconds, written like `"120s"`. Waiting for a personal-token verifier is separate and capped at the shorter of `max_wait` and 10 s. |
| `connect_timeout` | `"10s"` | Upstream dial timeout. |
| `shutdown_timeout` | `"30s"` | Longest a `SIGTERM` or `SIGINT` drain waits for open requests before exiting anyway. Under Kubernetes, set `terminationGracePeriodSeconds` a few seconds longer than this, or the pod is killed mid-drain. |
| `upstream_idle_timeout` | `"600s"` | Longest the upstream may send nothing, while waiting for the response headers or between body chunks. A stall before the headers gets `502`; a stall mid-body ends the response. It resets on every chunk, so a long stream that keeps sending is never cut off. Raise it for non-streaming requests that generate for longer than this. |
| `max_body` | `16777216` | Largest request body in bytes. Larger gets `413`. |
| `tool_choice_prefix` | none | See [tool_choice](#tool_choice-shim). |

### `[[route]]`

| Key | Meaning |
|---|---|
| `name` | Path prefix `/<name>/`. Letters, digits, `-` and `_` only; must be unique. |
| `upstream` | `http(s)` base URL the rest of the path is appended to. |
| `headers` | Headers set on every forwarded request. An empty value removes the header. |
| `retry_429` | Default `true`. Hold and retry an upstream `429` until `max_wait` runs out. Set `false` where a `429` means the quota is spent, because retrying then only delays the same answer. |

### `[[limit]]`

A limit applies to a request when it matches. Set `model` (the `model` field of the JSON body), `prefixes` (request path prefixes), or both, in which case both must match. A request may match several limits; it waits for each in config order.

| Key | Meaning |
|---|---|
| `name` | Used in error messages. |
| `rpm`, `burst` | Requests per minute. The bucket holds `rpm + burst` and refills at `rpm / 60` per second. |
| `tpm`, `tpm_burst` | Estimated tokens per minute, same shape. Tokens are estimated as body bytes / 4. |
| `max_inflight` | Concurrent requests. The slot is held until the response body ends. |

A limit needs a matcher and at least one of `rpm`, `tpm`, `max_inflight`.

With the Claude adapter enabled, limits matching `/claude/` use PostgreSQL admission per person/provider across replicas; static limits remain local. Model-only limits are also compiled for Claude. Shared definitions must agree within a namespace, including matching behavior, rates, bursts and concurrency. Keep the configured order consistent on all replicas.

Definitions persist in PostgreSQL. Restarting all replicas with a changed definition under the same name does not overwrite a conflicting stored definition. There is no online limit migration command; retain compatible definitions until an explicit drained deployment/state migration has been planned.

### Free-model pools: `[[pool]]`, `[[pool_provider]]`, `[[quota_domain]]`

A pool fails over across free models and closes any route whose price is not proven zero by the provider's live catalog; see [`docs/pools.md`](docs/pools.md). With any of these sections present, the static route name `pools` is reserved.

| Section | Key | Meaning |
|---|---|---|
| `[[pool_provider]]` | `name` | Unique identifier, referenced by routes and quota domains. |
| | `upstream` | `http(s)` base URL without credentials, query or fragment. The catalog is `<upstream>/models`. |
| | `headers` | As for `[[route]]`; also sent on catalog fetches. |
| | `catalog` | How the catalog states prices: `openrouter` or `opencode`. |
| `[[quota_domain]]` | `name`, `provider` | A budget shared by the routes on one provider account. |
| | `rpm` | Requests per minute, positive. |
| `[[pool]]` | `name` | Path `/pools/<name>/`. Needs at least one `[[pool.route]]`. |
| `[[pool.route]]` | `priority` | Unique within the pool; the lowest is tried first. |
| | `provider`, `model` | The provider and its model id. On an `openrouter` catalog provider, models under `openrouter/` (OpenRouter's own routers, such as `openrouter/free`) are refused. |
| | `context`, `max_output` | Token limits, positive. |
| | `capabilities` | Any of `tools`, `vision`, `schema`. |
| | `endpoints` | At least one of `chat`, `responses`. |
| | `quota` | A quota domain of the same provider. |

Each provider's catalog is fetched at startup and every 10 minutes (plus up to 30 s of jitter), with the provider's headers, a 10 s timeout and an 8 MiB cap. Proof stays valid for 30 minutes after the fetch that produced it; a failed fetch keeps the old proof until then. `GET /pools/<pool>/models` (legacy `proxy_token`) shows each route's state:

```json
{"pool": "free", "routes": [{"priority": 1, "provider": "openrouter", "model": "…:free", "endpoints": ["chat"], "status": "eligible", "proof_valid_for_secs": 1712}]}
```

`status` is `eligible` or one of `closed: no proof` (no successful fetch yet), `closed: expired`, `closed: not free`, `closed: missing from catalog`, `closed: endpoint not in catalog` (opencode lists API kinds, and none of the route's `endpoints`). `proof_valid_for_secs` is `null` without valid proof. An unknown pool gets `404`.

`POST /pools/<pool>/chat/completions` and `POST /pools/<pool>/responses` (legacy `proxy_token`) are served; every other `/pools/` path gets `404`. The body must be a JSON object of at most `max_body` bytes (`413`). Before any upstream call, a body with `plugins`, `web_search_options`, `provider`, `models` or `route`, a `model` ending in `:online`, a `tools` entry whose `type` is not `function` (on `/responses`: `function` or `custom`), such as `openrouter:web_search` or `web_search_preview`, or a `tool_choice` other than `auto`, `none`, `required` or one of those types, gets `400` `bad_request`. Otherwise the body's `model` is replaced with the chosen route's model.

Routes are tried in ascending priority. A route is a candidate when it declares the endpoint, its status is `eligible` for that endpoint, it is not cooling down, and its quota domain has a request left this minute (it is taken then). Each attempt is sent once, with the provider's headers instead of the client's credentials:

| Upstream outcome | What happens |
|---|---|
| Connect error or timeout before headers, or `5xx` | Try the next route. |
| `429` | Cool the quota domain down for `Retry-After` (whole seconds), default 60 s, at most 1 h, then try the next route. |
| `401` or `403` | Cool the route down for 5 min, then try the next route. |
| Any other status | Relay it unchanged, streaming included, with `x-llmproxy-pool-route: <provider>/<model>` and `x-llmproxy-pool-priority: <priority>`. |

A response is relayed before its body is read, so a stream that fails midway cannot fail over. When no candidate is left, the answer is `503` with `{"error": {"type": "pool_exhausted"}}` and `Retry-After` in whole seconds until the earliest quota refill, cooldown end or catalog refresh, at least 1.

Send `x-llmproxy-session: <1–128 visible ASCII characters>` to keep a conversation on one route: the route that last served the session is tried first while it is a candidate. Any other value is ignored. The map is per process, holds at most 10 000 sessions (the oldest is dropped) and forgets a session after an hour.

## Behaviour

**Authentication.** `Authorization: Bearer <token>` or `x-api-key: <token>`. Legacy tokens are compared without short-circuiting on the first differing byte; personal tokens are verified against stored hashes, at most 4 at a time (one per CPU on smaller hosts); further personal-token requests queue in arrival order for up to 10 s (or `max_wait`, if shorter), then get `429`. Authentication runs before forwarding, so an unknown path without a token gets `401`, not `404`. Duplicate or conflicting authentication headers get `401`. `/healthz` needs no token.

**Header hygiene.** Not forwarded: the client's `Authorization`, `x-api-key`, `Host`, `Forwarded`, `X-Forwarded-*`, `X-Real-Ip`, and hop-by-hop headers. Upstream redirects are not followed, so a redirect cannot carry your credential elsewhere.

**Path safety.** The forwarded URL must stay under the route's upstream. Paths that resolve outside it with `..` (or `%2e%2e`) get `400`. Dot segments that stay inside, like `a/../b`, are fine.

**Holding.** Held requests queue first-in-first-out. A request that gives up or times out while waiting on a limit takes nothing from that limit, and nothing is sent upstream. With several matching limits, budget already taken from an earlier one is not given back.

**Upstream 429s.** When the provider answers `429` and the route has `retry_429` on, the proxy waits and sends the same request again instead of passing the error on. It waits for the provider's `Retry-After` (whole seconds) or, without one, 1 s doubling up to 10 s, and never less than 250 ms. Waiting shares the `max_wait` budget with the time spent held on limits. When the next wait would not fit in what is left, the latest `429` is returned as it came from the provider, with its own `Retry-After` and body, so a client is never held for nothing. Retries are cancelled if the client disconnects. A `Retry-After` given as a date is ignored, and a retry does not draw on the local `rpm` / `tpm` budget again.

### `tool_choice` shim

With `tool_choice_prefix = "/albert/"`, a `POST` to `/albert/.../chat/completions` whose JSON body has a non-empty `tools` array and no `tool_choice` key gets `"tool_choice": "auto"` added at the end. Everything else is forwarded byte for byte: other paths, non-JSON bodies, empty `tools`, and any present `tool_choice` (even `null`). Key order is preserved.

### Responses

Provider responses are passed through with their status and headers, plus:

| Header | Meaning |
|---|---|
| `X-Proxy-Route` | Name of the route that served the request. |
| `X-Proxy-Queue-Wait-Ms` | Time spent held on limits. |
| `X-Proxy-Retries` | How many times the request was resent after an upstream `429` (`0` if never). |

Errors the proxy produces itself are JSON: `{"error": {"type": "...", "message": "..."}}`.

| Status | `type` | When |
|---|---|---|
| 400 | `bad_request` | Path escapes the upstream. |
| 401 | `unauthorized` | Missing or wrong token. |
| 403 | `forbidden` | The legacy shared token attempted a Claude request. |
| 404 | `not_found` | No route matches. |
| 413 | `body_too_large` | Body over `max_body` (or unreadable). |
| 429 | `queue_timeout` | Held for `max_wait`, or no personal-token verifier freed up within the shorter of `max_wait` and 10 s. `Retry-After` is that wait in seconds. |
| 502 | `upstream_error` | The upstream could not be reached, or sent no response headers within `upstream_idle_timeout`. |
| 503 | `authentication_unavailable`, `adapter_unavailable` | PostgreSQL/schema/limit configuration unavailable, or Claude credential missing, invalid, removed or undecryptable. |

Upstream statuses pass through, including Anthropic `400` validation errors; an Anthropic `401` invalidates that credential generation, so subsequent requests get `503` until the operator replaces it. An upstream `403` is passed through. Local queue exhaustion returns `429`; provider `429` follows the configured retry policy. A stream whose lease is lost after headers ends abruptly rather than changing its already-sent HTTP status.

## Per-person Claude operations

Only direct Anthropic API keys are supported today. This is not Claude subscription/OAuth access, Bedrock/Vertex cloud signing, or ChatGPT subscription support. Clients use `POST /claude/v1/messages` with Anthropic Messages JSON, including tools and `stream: true`; no OpenAI protocol translation is performed. Other Claude paths or methods return `404` after successful personal authentication.

| Client credential | Static routes | Claude Messages |
|---|---|---|
| Legacy `proxy_token` | Allowed, including during PostgreSQL outages | `403` |
| Active person's issued token | Allowed; database authentication required | Own active API key only |
| Missing, revoked, disabled or malformed token | `401` | `401` |
| Conflicting Authorization and x-api-key values | `401` | `401` |

Equal values in both authentication headers are accepted. `GET /healthz` is unauthenticated liveness, not database readiness. Metrics use a separate, unauthenticated listener; restrict it to your monitoring network. Database-backed authentication fails closed with `503` when authority cannot be established. Legacy static traffic remains independent of PostgreSQL and encryption files.

### Install and provision

Use PostgreSQL 16 or newer (CI and crash tests use 18). Configure `[postgres]`, `[credential_encryption]` and `[claude]` as shown in the example. `--check-config` is offline: it neither connects nor reads key files, and cannot certify readiness. `target/release/llmproxy --help` lists the operator interface without loading configuration. Auth commands read `LLMPROXY_CONFIG`; `db migrate` also accepts a config path.

Use a migration role that owns the schema and can create/alter tables, indexes and sequences. Run migrations explicitly before serving; startup never migrates. Concurrent `db migrate` runs serialize on an advisory lock; a run that waits longer than `acquire_timeout` for it exits non-zero, naming the held lock. The runtime/operator role needs CONNECT on the database, USAGE on the schema, SELECT/INSERT/UPDATE/DELETE on application tables, sequence USAGE/SELECT, and SELECT on `_sqlx_migrations`, but no schema CREATE or DDL ownership. Apply grants after migrations and renew them for new objects. Tests additionally require CREATEDB and administrative privileges for fault injection; do not give those privileges to production replicas.

A non-local PostgreSQL host must use `sslmode=verify-full` with a trusted server CA (the URL's or `PGSSLMODE`'s); any other mode is refused at startup and by `--check-config`. `localhost`, loopback addresses and Unix sockets are exempt. For a private network without TLS, set `insecure_tls = true` under `[postgres]` to allow it anyway; serving then logs a warning. Encryption protects stored provider credentials, not all database metadata. Encrypt database volumes and backups separately; removed or re-encrypted secrets may remain in backups and WAL until their retention expires. Keep recoverable key backups under separate access control. Losing encryption keys loses credential access; losing the fingerprint key breaks stable credential fingerprints. Either loss also blocks `auth claude remove` and `auth credentials reencrypt`.

Create separate random 32-byte binary files for the fingerprint key and each encryption master key, readable only by the service/operator. Do not use hex strings or put key bytes in TOML, argv, logs or Git. For example, on the operator host:

```sh
umask 077
mkdir -p secrets
openssl rand -out secrets/fingerprint.key 32
openssl rand -out secrets/primary.key 32
# Set the absolute file paths in config.toml, and set its runtime DATABASE_URL.
export LLMPROXY_CONFIG="$PWD/config.toml"
target/release/llmproxy --check-config "$LLMPROXY_CONFIG"
# migration.toml uses the migration role's connection URL.
target/release/llmproxy db migrate migration.toml
target/release/llmproxy auth person create Alice
# Copy person_id from the JSON output:
export PERSON_ID='<person_id>'
target/release/llmproxy auth token issue "$PERSON_ID"
# Save the one-time token in the client's secret store; do not capture it in logs.
target/release/llmproxy auth claude set "$PERSON_ID"
target/release/llmproxy auth claude status "$PERSON_ID"
target/release/llmproxy auth person status "$PERSON_ID"
```

`auth claude set` prompts without echo on a terminal and installs or replaces the key. For automation, `target/release/llmproxy auth claude set "$PERSON_ID" --credential-stdin < /run/secrets/anthropic-api-key` reads the secret from stdin. Status output is redacted stored metadata, not a decryption/readiness check. Create Bob and issue/install his credentials using the same sequence; tokens never select another person's key. To revoke one token use `auth token revoke <token_id>`; to disable all access for a person use `auth person disable <person_id>`; to remove a provider key use `auth claude remove <person_id>`. `auth claude remove` and `auth credentials reencrypt` load the configured key files and the fingerprint key file, so they fail if any of them is unreadable.

### Two replicas and requests

Give both replicas the same database, namespace, key files/key ring, and Claude limit definitions. Change only their client and metrics listen addresses (for example 8788/9180 and 8789/9181). A namespace isolates tenants' database state; it is not a substitute for database access control. Conflicting shared limit definitions fail Claude closed with `503`; coordinate configuration changes across replicas. Static route limits remain process-local. Budget `max_connections` plus one notification connection per replica, plus operator commands; reserve database capacity for migrations and administration.

```sh
target/release/llmproxy config.toml &
target/release/llmproxy replica-b.toml &
curl -fsS http://127.0.0.1:8788/healthz
curl -fsS http://127.0.0.1:8789/healthz
# PROXY_TOKEN is the legacy token; PERSON_TOKEN is the issued person's token.
curl -sS http://127.0.0.1:8788/minimax/chat/completions \
  -H "Authorization: Bearer $PROXY_TOKEN" -H 'Content-Type: application/json' \
  -d '{"model":"your-static-model","messages":[{"role":"user","content":"hi"}]}'
curl -sS http://127.0.0.1:8788/claude/v1/messages \
  -H "Authorization: Bearer $PERSON_TOKEN" -H 'Content-Type: application/json' \
  -H 'anthropic-version: 2023-06-01' \
  -d '{"model":"your-claude-model","max_tokens":64,"messages":[{"role":"user","content":"hi"}]}'
curl -N -sS http://127.0.0.1:8789/claude/v1/messages \
  -H "Authorization: Bearer $PERSON_TOKEN" -H 'Content-Type: application/json' \
  -H 'anthropic-version: 2023-06-01' \
  -d '{"model":"your-claude-model","max_tokens":64,"stream":true,"messages":[{"role":"user","content":"hi"}]}'
curl -fsS http://127.0.0.1:9180/metrics
target/release/llmproxy auth claude set "$PERSON_ID"
```

Replacement takes effect on later requests on both replicas; it does not rewrite an already running upstream request. Shared concurrency leases renew every 10 seconds and expire after 30 seconds. The proxy cancels transport before an unrenewed lease expires. This cannot prove that a provider has stopped processing a request already received: provider-side work or billing may continue after cancellation.

### Encryption rotation and recovery

First distribute a new 32-byte key file under a new key ID to every replica, retaining the old key and unchanged fingerprint key. Restart all replicas with both keys readable. Then change `active_key_id` to the new ID everywhere and restart every writer before running:

```sh
target/release/llmproxy auth credentials reencrypt --to next
```

The target ID must exist in the configured key ring. Re-run after interruption; the command reports `rewritten` and `remaining`. Once remaining is zero, all writers use the new active key, and old-backup recovery has been addressed, remove the old ID/file from replicas and restart them. Retain protected old keys for backups that still need them. Do not rotate the fingerprint key with this procedure: it must stay stable across replicas and encryption rotations.

Exercise outages before rollout: with PostgreSQL stopped, legacy static requests must still work, while issued-token requests return `503`; after database recovery they resume. With a wrong encryption key or incompatible schema, Claude returns `503` and legacy static continues. Repair the schema using the matching migration release; never edit migration checksums to suppress a failure. A key ring that loaded successfully is kept until restart, so after replacing a key that loaded but is wrong, or changing or rotating key files, restart the affected replicas. A key file that is missing, unreadable or malformed is retried on every Claude request, so repairing it needs no restart. `/healthz` remains `200` in these cases.

## Observability

Logs are JSON on stdout at `INFO`, one line per request: route, status, method, path, milliseconds. Bodies, header values and query strings are never logged.

`RUST_LOG` changes the level, for example `RUST_LOG=warn` to drop the per-request lines or `RUST_LOG=info,llmproxy=debug` for more detail from the proxy alone. Unset, empty or invalid means `info`. Below `info`, dependencies such as `sqlx` and `hyper` add their own debug output, and the promise above holds only at `info` and higher.

When `metrics_listen` is set, `GET /metrics` serves:

```
llm3_http_requests_total{route,code}              counter
llm3_http_requests_in_flight                      gauge
llm3_http_requests_held                           gauge
llm3_http_request_header_duration_seconds{route}  histogram
llm3_http_request_duration_seconds{route}         histogram
llm3_postgres_available                          gauge
llm3_provider_credentials{provider,state}         gauge
llm3_credential_operations_total{provider,operation,outcome} counter
```

`route` is a configured route name, `healthz`, or `other` (rejected before routing).

Credential telemetry accepts only fixed labels: provider `anthropic`; states `active`, `invalid`, `removed`, `unavailable`; operations `install`, `replace`, `remove`, `invalidate`, `reencrypt`; outcomes `success`, `conflict`, `not_found`, `unavailable`. No identity, token, namespace, host, key ID, error text or model becomes a credential metric label. Request metrics retain their existing route/status labels, including a configured `claude` route.

The lifecycle integration API is `Proxy::metrics().storage()`: `set_postgres_available` records the latest observed database result, `set_credentials` replaces a complete authoritative provider count snapshot, and `record_operation` counts a completed durable operation. Availability starts at zero until observed available. Credential samples are absent until a successful snapshot and retain their last known values during outages; use the availability gauge to detect stale counts. Snapshot refreshes must be serialized by their caller. Undecryptable active credentials count only as `unavailable`. Operation counters are process-local and start over on restart.

Runtime observations refresh database availability and credential counts during requests; they are not an active readiness probe. Operator commands are separate processes, so their counters do not appear in a serving replica. For PostgreSQL-backed deployments, alert on `llm3_postgres_available == 0`, increases in Claude `503` responses and unavailable/invalid credentials; also alert on persistent held requests. Scrape each replica, and send a controlled authenticated readiness request when validating rollout or recovery.

Personal authentication and active concurrency-lease renewal observe database availability. Heartbeat storage failures set availability to zero even without new requests; successful renewals restore it, while loss of lease ownership alone does not imply a database outage. Authenticated Claude credential lookups refresh a serialized, namespace-wide credential snapshot, including failed lookups; upstream `401` invalidation refreshes it again and records the serving process's `invalidate` outcome. These counters do not count operator installs, replacements or removals. Restarted processes begin unobserved until requests exercise these paths.

`llm3_http_requests_in_flight` counts a request from arrival until its response body ends, so a long stream stays counted for as long as it runs, and one the client abandons stops being counted. `llm3_http_requests_held` counts only the requests currently waiting on a limit, whether ours or the provider's (waiting to retry after an upstream `429`). Alert on a backlog with `held`, not `in_flight`, which also includes healthy long streams.

Both histograms start the clock when the request arrives. `llm3_http_request_header_duration_seconds` stops it when the response headers are ready, so it covers time spent held on a limit, upstream retries and the upstream's time to respond; alert on a slowdown with this one. `llm3_http_request_duration_seconds` stops it when the response body ends, so a long stream lands in a high bucket however healthy it is, and a client that leaves mid-stream records the time until it left. A request the client abandons before the headers are ready is not observed, as with the counters. The buckets are fixed: 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120 and 300 seconds.

On `SIGTERM` or `SIGINT` the proxy stops accepting and lets in-flight and held requests finish, for at most `shutdown_timeout`. After that it logs a warning and exits, closing the connections still open, such as long streams.

## Not yet implemented

Tracked as issues: a zero-cost model pool ([#1](https://github.com/StephanMeijer/llmproxy/issues/1), design in [`docs/pools.md`](docs/pools.md)), a per-person ChatGPT subscription adapter using Responses ([#19](https://github.com/StephanMeijer/llmproxy/issues/19)), and Chat Completions compatibility for that adapter ([#20](https://github.com/StephanMeijer/llmproxy/issues/20)). Static limit state remains per-process; only Claude limits use shared PostgreSQL admission.

## Development

CI ([`ci.yml`](.github/workflows/ci.yml)) runs these on every pull request and every push to `main`, and all must pass:

```sh
cargo fmt --all --check
cargo clippy --locked --all-targets -- -D warnings
cargo audit --deny warnings
export LLMPROXY_TEST_DATABASE_URL='postgres://postgres:local-test-only@127.0.0.1:5432/postgres'
export LLMPROXY_CREDENTIAL_CONTAINER='<owned-postgres-container-id>'
export RUST_TEST_THREADS=2   # bound concurrent test pools against a small test database
cargo test --locked
CARGO_TARGET_DIR=target/cov cargo llvm-cov --locked --fail-under-lines 99
```

- **Strict clippy.** `pedantic` and `nursery` are denied, as are `unwrap_used`, `expect_used`, `panic`, `todo`, `unimplemented`, `dbg_macro` and `indexing_slicing` (see `[lints.clippy]` in `Cargo.toml`).
- **Coverage.** Line coverage must stay at or above 99%. Tests live in `tests/`, which coverage ignores, so the number measures production code only. It needs [`cargo-llvm-cov`](https://github.com/taiki-e/cargo-llvm-cov) (`cargo install cargo-llvm-cov --locked`) and the `llvm-tools` component, which `rustup toolchain install` in this directory adds.
- **Advisories.** `cargo audit --deny warnings` fails on any RustSec advisory in `Cargo.lock`, including unmaintained and yanked crates. Install it with `cargo install cargo-audit --locked`.
- **Toolchain.** Pinned in `rust-toolchain.toml`. Bump it on purpose: a new Rust release can add lints.
- **Real database tests.** `LLMPROXY_TEST_DATABASE_URL` is mandatory for database tests and coverage, pointing at an isolated PostgreSQL 18 instance with CREATEDB and administrative fault-injection privileges. Tests create/drop isolated databases and use fake local upstreams, with no provider credentials. Docker must be available: `postgres_integration` owns an additional PostgreSQL 18 container for real stop/start crash tests. CLI tests run the built binary (Unix only). CI checks readiness and explicitly migrates before tests.
- **Test evidence.** Set `LLMPROXY_TEST_EVIDENCE` to a directory to have tests write debug artifacts (request transcripts, database snapshots) under `<dir>/<test target>/`. Unset, tests write none.

If your `CARGO_TARGET_DIR` is shared with other projects, give coverage its own, for example `CARGO_TARGET_DIR=target/cov cargo llvm-cov --locked`. Leftover profile files from other projects make `cargo llvm-cov report` fail.
