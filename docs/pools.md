# Free-model pools: design

Status: implemented for [#1](https://github.com/StephanMeijer/llmproxy/issues/1); answers
[#26](https://github.com/StephanMeijer/llmproxy/issues/26). Config
([#27](https://github.com/StephanMeijer/llmproxy/issues/27)), routing and failover
([#28](https://github.com/StephanMeijer/llmproxy/issues/28)), free proof
([#29](https://github.com/StephanMeijer/llmproxy/issues/29)) and metrics with a charge alarm
([#30](https://github.com/StephanMeijer/llmproxy/issues/30)) are in place; the README documents
the behaviour and the metrics.

A pool is one endpoint that fails over across a reviewed list of free models. Its one hard rule:
**the proxy never sends a pool request that could cost money.** Every rule below closes a route
rather than guess.

## Where proof of "free" comes from

Proof comes only from a provider's live model catalog. A provider without a machine-readable
catalog that states prices cannot be in a pool; there is no hand-written "trust me, it is free"
list. The catalog is fetched from `<upstream>/models` with the provider's configured headers.

| Catalog kind | Fetched from | A model is free when |
|---|---|---|
| `openrouter` | `https://openrouter.ai/api/v1/models` | `pricing.prompt` and `pricing.completion` are present, and every price in `pricing`, including `overrides` entries, parses as exactly zero |
| `opencode` | `https://opencode.ai/zen/v1/models` | `cost.text.input.price` and `cost.text.output.price` are present, and every price in `cost` (`price`, `cache.read`, `cache.write` at every level, including `overrides` entries) parses as exactly zero |

Both catalogs were checked on 2026-11-09:

- **OpenRouter** lists 462 models, each with a `pricing` object of decimal strings. Keys seen
  include `prompt`, `completion`, `image`, `audio`, `web_search`, `internal_reasoning`,
  `input_cache_read`, `input_cache_write` and `overrides`. Free models such as
  `liquid/lfm-3-1.7b:free` carry `{"prompt": "0", "completion": "0"}`.
- **opencode zen** lists 82 models, each with a nested `cost` object of numbers (for example
  `{"text": {"input": {"price": 0, "cache": {"read": 0}}, "output": {"price": 0}}}`), and an `api`
  object naming the API kinds it serves (`chat-completions`, `responses`, ...).

Fail-closed rules, the same for every catalog kind:

- "Parses as exactly zero" means the value is a decimal (an OpenRouter string such as `"0"` or
  `"0.0"`, an opencode number such as `0`) whose value is exactly zero. Anything else, including
  `"-1"`, closes the route. An opencode number must be the integer `0`: a float such as `0.0` closes
  the route, because a JSON parser underflows `1e-400` to it.
- A model id the catalog lists more than once is free only when every listing is free.
- A missing price object, or a missing required key from the table above, closes the route. An
  empty or partial price object is not proof.
- Every other price key that is present must also parse as exactly zero. A key the proxy does not
  recognise counts as a price. Only thresholds and factors are not prices: OpenRouter
  `overrides[].min_prompt_tokens`, opencode `when` conditions (such as `context.gt`), and opencode
  `multipliers[].factor` (any factor times zero stays zero).
- A model missing from the catalog closes its route.
- A fetch that fails, times out, returns non-2xx or does not parse keeps the previous proof only
  until that proof expires.
- Every model in OpenRouter's own `openrouter/` namespace (`openrouter/free`, `openrouter/auto`
  and any router added later) is refused in config: these routers pick the model themselves, and
  the proof must be for the model that actually answers.

Albert and MiniMax, the other providers in `config.example.toml`, answer `401` on `/models`
without a key, and neither publishes prices in its catalog. They stay out of pools until they do.

## Refresh and validity

- Each provider's catalog is refreshed every **10 minutes**, plus up to 30 seconds of jitter so
  replicas do not fetch in step. A refresh covers all of that provider's routes.
- Proof is valid for **30 minutes** after the fetch that produced it. Once it expires, the route
  closes before the next request, not after.
- On startup, every route is closed until its first successful fetch.
- Both values are constants, not config, in the first version. Make them configurable only when
  a provider's terms need it.

## Endpoints

A pool serves both `POST /pools/<pool>/chat/completions` and `POST /pools/<pool>/responses`, and
`GET /pools/<pool>/models` for route status (#29).

- Each route declares the endpoint kinds it supports (`endpoints = ["chat", "responses"]`).
- A request only tries routes that declare its kind. When none is eligible, the request gets
  `pool_exhausted` (#30).
- For the `opencode` catalog, the kind must also appear in the model's `api` object. Most free
  zen models list only `chat-completions`. OpenRouter's catalog does not list API kinds, so the
  route's declaration is taken as given.

## Requests that could cost money anyway

A zero price list does not cover these, so a request carrying any of them is rejected with `400`
before any upstream call (#29):

- OpenRouter extras: `plugins`, `web_search_options`.
- A body `model` ending in `:online`. This is checked on the request as received, before the
  model is replaced.
- Caller routing fields: `provider`, `models`, `route`. The pool picks the route, never the body
  (#28).
- Tools that run at the provider. Every `tools` entry must be an object whose `type` is
  `function` (on `/responses`, `function` or `custom`): the caller runs those itself. Anything
  else, such as OpenRouter server tools (`openrouter:web_search`, `openrouter:web_fetch`, ...) or
  Responses built-in tools (`web_search_preview`, `file_search`, `code_interpreter`,
  `image_generation`, `computer_use_preview`, `mcp`, ...), may be billed and is refused, as is a
  `tool_choice` other than `auto`, `none`, `required` or an object of an allowed type.

Otherwise the body's `model` is not rejected but replaced with the selected route's model. The
caller names the pool, not a model.

## Quota domains

A quota domain is a requests-per-minute budget shared by the routes that draw on one provider
account, for example all OpenRouter `:free` models. A route whose domain is exhausted, locally or
because the upstream answered `429`, is skipped until the window refills (#28).

## Config sketch for #27

```toml
[[pool_provider]]
name = "openrouter"
upstream = "https://openrouter.ai/api/v1"
headers = { Authorization = "Bearer ${OPENROUTER_API_KEY}" }
catalog = "openrouter"            # proof source; only the kinds in the table above

[[quota_domain]]
name = "openrouter-free"
provider = "openrouter"
rpm = 20

[[pool]]
name = "free"

[[pool.route]]
priority = 1                      # unique within the pool; lowest is tried first
provider = "openrouter"
model = "liquid/lfm-3-1.7b:free"
context = 262144                  # tokens
max_output = 8192
capabilities = ["tools", "schema"] # of: tools, vision, schema
endpoints = ["chat"]               # of: chat, responses
quota = "openrouter-free"         # required; must belong to the route's provider
```

`--check-config` validation (#27) refuses, offline:

- duplicate pool, provider or quota-domain names;
- an empty pool, or duplicate priorities within a pool;
- a route without a quota domain, a route naming an unknown provider or quota domain, or a quota
  domain of another provider;
- zero `rpm`, `context` or `max_output`, and empty `endpoints`;
- a static `[[route]]` named `pools` while any `[[pool]]`, `[[pool_provider]]` or
  `[[quota_domain]]` section is present;
- a route on an `openrouter` catalog provider whose model starts with `openrouter/`.
