pub const USAGE: &str = "llmproxy — authenticated LLM egress proxy

Usage:
  llmproxy [config]
  llmproxy --check-config [config]
  llmproxy --help | -h
  llmproxy db migrate [config]
  llmproxy auth person create <label>
  llmproxy auth person status <person-id>
  llmproxy auth person disable <person-id>
  llmproxy auth token issue <person-id>
  llmproxy auth token revoke <token-id>
  llmproxy auth claude set <person-id> [--credential-stdin]
  llmproxy auth claude status <person-id>
  llmproxy auth claude remove <person-id>
  llmproxy auth credentials reencrypt --to <key-id>
  llmproxy credentials reencrypt --to <key-id>

Configuration defaults to LLMPROXY_CONFIG, then /etc/llmproxy/config.toml.
Auth and reencrypt commands use that configuration; they take no config argument.
--check-config validates offline without opening database connections or key files.
Claude set reads a hidden terminal prompt, or stdin with --credential-stdin.
Never pass credentials as command arguments. Token issue emits a secret once on
stdout: protect its destination. Person/token identifiers are UUIDs.
";
