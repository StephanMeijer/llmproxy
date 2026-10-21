# llmproxy

An authenticated, rate-limited reverse proxy that sits between your LLM clients and provider APIs. Clients hold one proxy token; the provider keys stay on the server. Everything is set in a single TOML file.

- **Credential isolation:** the client's `Authorization` and `x-api-key` are dropped and replaced by the provider credential you configure.
- **Upstream protection:** requests above a limit are held in arrival order until budget frees, instead of being rejected. The client sees `429` only after `max_wait`.
- **Compatibility shim:** optionally adds `tool_choice: "auto"` for providers that need it.
- **Streaming:** server-sent events pass through unbuffered.

It speaks plain HTTP. Put a TLS-terminating reverse proxy or ingress in front of it.

## Quick start

```sh
cargo build --release

export PROXY_TOKEN=change-me
export MINIMAX_API_KEY=...            # only the variables your config references
cp config.example.toml config.toml    # edit routes and limits

target/release/llmproxy --check-config config.toml   # validate, do not serve
target/release/llmproxy config.toml
```

The config path is the first non-flag argument, else `$LLMPROXY_CONFIG`, else `/etc/llmproxy/config.toml`.

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
| `proxy_token` | required | The token clients must present. |
| `max_wait` | `"120s"` | Longest a request is held by a limit before `429`. Whole seconds, written like `"120s"`. |
| `connect_timeout` | `"10s"` | Upstream dial timeout. Reading a response is never cut off, so long streams are safe. |
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

## Behaviour

**Authentication.** `Authorization: Bearer <token>` or `x-api-key: <token>`, compared without short-circuiting on the first differing byte. This runs before routing, so an unknown path without a token gets `401`, not `404`. `GET /healthz` needs no token.

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
| 404 | `not_found` | No route matches. |
| 413 | `body_too_large` | Body over `max_body` (or unreadable). |
| 429 | `queue_timeout` | Held for `max_wait`. `Retry-After` is `max_wait` in seconds. |
| 502 | `upstream_error` | The upstream could not be reached. |

## Observability

Logs are JSON on stdout at `INFO`, one line per request: route, status, method, path, milliseconds. Bodies, header values and query strings are never logged.

When `metrics_listen` is set, `GET /metrics` serves:

```
llm3_http_requests_total{route,code}              counter
llm3_http_requests_in_flight                      gauge
llm3_http_requests_held                           gauge
llm3_http_request_header_duration_seconds{route}  histogram
llm3_http_request_duration_seconds{route}         histogram
```

`route` is a configured route name, `healthz`, or `other` (rejected before routing).

`llm3_http_requests_in_flight` counts a request from arrival until its response body ends, so a long stream stays counted for as long as it runs, and one the client abandons stops being counted. `llm3_http_requests_held` counts only the requests currently waiting on a limit, whether ours or the provider's (waiting to retry after an upstream `429`). Alert on a backlog with `held`, not `in_flight`, which also includes healthy long streams.

Both histograms start the clock when the request arrives. `llm3_http_request_header_duration_seconds` stops it when the response headers are ready, so it covers time spent held on a limit, upstream retries and the upstream's time to respond; alert on a slowdown with this one. `llm3_http_request_duration_seconds` stops it when the response body ends, so a long stream lands in a high bucket however healthy it is, and a client that leaves mid-stream records the time until it left. A request the client abandons before the headers are ready is not observed, as with the counters. The buckets are fixed: 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120 and 300 seconds.

On `SIGTERM` or `SIGINT` the proxy stops accepting and lets in-flight and held requests finish.

## Not yet implemented

Tracked as issues: a zero-cost model pool ([#1](https://github.com/StephanMeijer/llmproxy/issues/1)) and a Claude subscription adapter ([#2](https://github.com/StephanMeijer/llmproxy/issues/2)).

Also by design: one shared token for all clients, and a single process, since limit state is held in memory.

## Development

CI ([`ci.yml`](.github/workflows/ci.yml)) runs these on every pull request and every push to `main`, and all must pass:

```sh
cargo fmt --all --check
cargo clippy --locked --all-targets -- -D warnings
cargo llvm-cov --locked --fail-under-lines 99     # also runs the tests
```

- **Strict clippy.** `pedantic` and `nursery` are denied, as are `unwrap_used`, `expect_used`, `panic`, `todo`, `unimplemented`, `dbg_macro` and `indexing_slicing` (see `[lints.clippy]` in `Cargo.toml`).
- **Coverage.** Line coverage must stay at or above 99%. Tests live in `tests/`, which coverage ignores, so the number measures production code only. It needs [`cargo-llvm-cov`](https://github.com/taiki-e/cargo-llvm-cov) (`cargo install cargo-llvm-cov --locked`) and the `llvm-tools` component, which `rustup toolchain install` in this directory adds.
- **Toolchain.** Pinned in `rust-toolchain.toml`. Bump it on purpose: a new Rust release can add lints.
- **Tests need no network.** They run a fake upstream and the real proxy on ephemeral ports, and `tests/cli.rs` runs the real binary (start-up, flags, bind errors, `SIGTERM` and `SIGINT`; Unix only).

If your `CARGO_TARGET_DIR` is shared with other projects, give coverage its own, for example `CARGO_TARGET_DIR=target/cov cargo llvm-cov --locked`. Leftover profile files from other projects make `cargo llvm-cov report` fail.
