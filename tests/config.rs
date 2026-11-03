#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{env, error::Error, fs, path::PathBuf, process::Command, time::Duration};

use llmproxy::{
    config::{Config, ConfigError},
    proxy::Proxy,
};

const MINIMAL: &str = "listen = \"127.0.0.1:0\"\nproxy_token = \"${TOKEN}\"\n";
const DEFAULT_MAX_BODY: usize = 16 * 1024 * 1024;
const UNSET_VAR: &str = "LLMPROXY_TEST_SURELY_UNSET_VAR";
const POSTGRES: &str = "[postgres]\nurl = \"postgres://127.0.0.1:1/x\"\nnamespace = \"test_1-a\"\n";
const ENCRYPTION: &str = "[credential_encryption]\nfingerprint_key_file = '/nonexistent/fingerprint'\nactive_key_id = \"key_1-a\"\nkeys = { key_1-a = \"/nonexistent/llmproxy-master-key\" }\n";
const CLAUDE: &str = "[claude]\n";

fn lookup(name: &str) -> Option<String> {
    (name == "TOKEN").then(|| "s3\"cret".to_owned())
}

/// Builds the proxy from `extra` appended to a valid minimal config and returns why it was refused.
fn refused(extra: &str) -> String {
    let text = format!("listen = \"127.0.0.1:0\"\nproxy_token = \"t\"\n{extra}");
    Config::parse(&text, |_| None)
        .and_then(Proxy::new)
        .err()
        .expect("config should be refused")
        .to_string()
}

fn temp_config(name: &str, text: &str) -> PathBuf {
    let path = env::temp_dir().join(format!("llmproxy-{}-{name}.toml", std::process::id()));
    fs::write(&path, text).unwrap();
    path
}

#[test]
fn expands_env_into_strings_and_applies_defaults() {
    let config = Config::parse(MINIMAL, lookup).unwrap();
    assert_eq!(config.proxy_token, "s3\"cret");
    assert_eq!(config.max_wait, Duration::from_mins(2));
    assert_eq!(config.connect_timeout, Duration::from_secs(10));
    assert_eq!(config.max_body, DEFAULT_MAX_BODY);
}

#[test]
fn unset_and_unterminated_references_are_errors() {
    let err = Config::parse(MINIMAL, |_| None).unwrap_err();
    assert!(err.0.contains("environment variable is not set"), "{err}");
    let err = Config::parse("proxy_token = \"${TOKEN\"", lookup).unwrap_err();
    assert!(err.0.contains("unterminated"), "{err}");
}

#[test]
fn unset_references_do_not_disclose_placeholder_contents() -> Result<(), Box<dyn Error>> {
    for placeholder in [
        "LLMPROXY_UNSET_CONFIG_REFERENCE",
        "postgres://user:closed-reference-canary@127.0.0.1:1/db",
    ] {
        let text =
            format!("{MINIMAL}[postgres]\nurl = \"${{{placeholder}}}\"\nnamespace = \"test\"\n");
        let error = Config::parse(&text, lookup)
            .err()
            .ok_or("unset reference accepted")?;
        assert!(!error.to_string().contains(placeholder));
        assert!(!format!("{error:?}").contains(placeholder));
        assert!(
            error
                .to_string()
                .contains("environment variable is not set")
        );
    }
    Ok(())
}

/// What `proxy_token = "<value>"` expands to, with `X` set to "val".
fn expanded(value: &str) -> Result<String, ConfigError> {
    let text = format!("listen = \"127.0.0.1:0\"\nproxy_token = \"{value}\"\n");
    Config::parse(&text, |name| (name == "X").then(|| "val".to_owned()))
        .map(|config| config.proxy_token)
}

#[test]
fn a_dollar_dollar_brace_is_a_literal_brace() {
    for (value, want) in [
        ("$${X}", "${X}"),
        ("a$${b", "a${b"), // an escape needs no closing brace
        ("$${X}${X}", "${X}val"),
        ("$$${X}", "$${X}"),
        ("$$$${X}", "$$${X}"),
    ] {
        assert_eq!(expanded(value).unwrap(), want, "{value}");
    }
}

#[test]
fn other_dollars_are_left_alone() {
    for (value, want) in [
        ("$", "$"),
        ("$$", "$$"),
        ("$X", "$X"),
        ("a$ b", "a$ b"),
        ("${X}", "val"),
    ] {
        assert_eq!(expanded(value).unwrap(), want, "{value}");
    }
}

#[test]
fn references_still_fail_next_to_an_escape() {
    let err = expanded("$${X}${NOPE}").unwrap_err();
    assert!(err.0.contains("environment variable is not set"), "{err}");
    let err = expanded("$${X} ${").unwrap_err();
    assert!(err.0.contains("unterminated"), "{err}");
}

#[test]
fn literal_braces_work_in_header_values() {
    let text = format!(
        "{MINIMAL}[[route]]\nname = \"a\"\nupstream = \"http://127.0.0.1:1/v1\"\n\
         headers = {{ X-Template = \"$${{NOT_A_VAR}}\" }}\n"
    );
    let config = Config::parse(&text, lookup).unwrap();
    let route = config.routes.first().unwrap();
    assert_eq!(route.headers.get("X-Template").unwrap(), "${NOT_A_VAR}");
    Proxy::new(config).unwrap();
}

#[test]
fn empty_token_is_rejected() {
    let err = Config::parse(MINIMAL, |_| Some(String::new())).unwrap_err();
    assert!(err.0.contains("proxy_token"), "{err}");
}

#[test]
fn unknown_keys_bad_durations_and_bad_toml_are_rejected() {
    let typo = format!("{MINIMAL}max_wiat = \"1s\"\n");
    assert!(Config::parse(&typo, lookup).is_err());
    let bad = format!("{MINIMAL}max_wait = \"2m\"\n");
    let err = Config::parse(&bad, lookup).unwrap_err();
    assert!(err.0.contains("whole seconds"), "{err}");
    let err = Config::parse("listen = ", lookup).unwrap_err();
    assert!(err.0.contains("parse config"), "{err}");
}

#[test]
fn non_string_durations_and_unset_variables_inside_arrays_are_rejected() {
    let number = format!("{MINIMAL}max_wait = 120\n");
    assert!(Config::parse(&number, lookup).is_err());
    let nested = format!("{MINIMAL}[[limit]]\nname = \"a\"\nprefixes = [\"${{NOPE}}\"]\nrpm = 1\n");
    let err = Config::parse(&nested, lookup).unwrap_err();
    assert!(err.0.contains("environment variable is not set"), "{err}");
}

#[test]
fn retry_429_defaults_to_on_and_can_be_turned_off() {
    let parse = |extra: &str| {
        let text = format!("{MINIMAL}[[route]]\nname = \"a\"\nupstream = \"http://h\"\n{extra}");
        Config::parse(&text, lookup)
    };
    let retries = |config: Config| config.routes.first().unwrap().retry_429;
    assert!(retries(parse("").unwrap()));
    assert!(!retries(parse("retry_429 = false\n").unwrap()));
    assert!(parse("retry_429 = \"no\"\n").is_err());
}

#[test]
fn errors_display_their_message() {
    assert_eq!(ConfigError("boom".into()).to_string(), "boom");
}

#[test]
fn example_config_parses() {
    let text = include_str!("../config.example.toml");
    let config = Config::parse(text, |_| Some("x".to_owned())).unwrap();
    assert_eq!(config.routes.len(), 5);
    assert_eq!(config.limits.len(), 3);
    Proxy::new(config).unwrap();
}

#[test]
fn load_reads_a_file_and_names_problems() {
    let ok = temp_config(
        "ok",
        "listen = \"127.0.0.1:0\"\nproxy_token = \"literal\"\n",
    );
    assert_eq!(Config::load(&ok).unwrap().proxy_token, "literal");

    let unset = temp_config(
        "unset",
        &format!("listen = \"127.0.0.1:0\"\nproxy_token = \"${{{UNSET_VAR}}}\"\n"),
    );
    let err = Config::load(&unset).unwrap_err();
    assert!(err.0.contains("environment variable is not set"), "{err}");

    let missing = env::temp_dir().join("llmproxy-no-such-file.toml");
    let err = Config::load(&missing).unwrap_err();
    assert!(err.0.contains("llmproxy-no-such-file.toml"), "{err}");

    for path in [ok, unset] {
        fs::remove_file(path).unwrap();
    }
}

#[test]
fn accepts_a_valid_route() {
    let text = format!(
        "{MINIMAL}[[route]]\nname = \"a-b_1\"\nupstream = \"http://127.0.0.1:1/v1\"\n\
         headers = {{ Authorization = \"Bearer k\", X-Api-Key = \"\" }}\n"
    );
    Proxy::new(Config::parse(&text, lookup).unwrap()).unwrap();
}

#[test]
fn refuses_bad_route_names() {
    for name in ["", "a/b", "a b", "a.b"] {
        let why = refused(&format!(
            "[[route]]\nname = \"{name}\"\nupstream = \"http://h\"\n"
        ));
        assert!(why.contains("name must be"), "{name:?}: {why}");
    }
}

#[test]
fn refuses_duplicate_routes() {
    let route = "[[route]]\nname = \"a\"\nupstream = \"http://h\"\n";
    let why = refused(&format!("{route}{route}"));
    assert!(why.contains("route a: defined twice"), "{why}");
}

#[test]
fn refuses_upstreams_that_are_not_http_urls() {
    for upstream in ["ftp://h", "not a url", "http://"] {
        let why = refused(&format!(
            "[[route]]\nname = \"a\"\nupstream = \"{upstream}\"\n"
        ));
        assert!(why.contains("http(s) URL"), "{upstream:?}: {why}");
    }
}

#[test]
fn refuses_invalid_header_names_and_values() {
    let name = refused(
        "[[route]]\nname = \"a\"\nupstream = \"http://h\"\nheaders = { \"bad name\" = \"v\" }\n",
    );
    assert!(name.contains("invalid name"), "{name}");
    let value =
        refused("[[route]]\nname = \"a\"\nupstream = \"http://h\"\nheaders = { X = \"a\\nb\" }\n");
    assert!(value.contains("invalid value"), "{value}");
}

#[test]
fn refuses_limits_that_match_or_limit_nothing() {
    let why = refused("[[limit]]\nname = \"x\"\nrpm = 1\n");
    assert!(why.contains("limit x: set `model` or `prefixes`"), "{why}");
}

#[test]
fn adapter_sections_are_optional_and_independently_validated() -> Result<(), Box<dyn Error>> {
    for extra in [String::new(), POSTGRES.into(), ENCRYPTION.into()] {
        let config = Config::parse(&format!("{MINIMAL}{extra}"), lookup)?;
        assert!(config.claude.is_none());
        Proxy::new(config)?;
    }
    Ok(())
}

#[test]
fn adapter_defaults_and_explicit_settings_are_offline() -> Result<(), Box<dyn Error>> {
    let config = Config::parse(&format!("{MINIMAL}{POSTGRES}{ENCRYPTION}{CLAUDE}"), lookup)?;
    let postgres = config.postgres.as_ref().ok_or("missing postgres")?;
    assert_eq!(postgres.max_connections, 10);
    assert_eq!(postgres.connect_timeout, Duration::from_secs(10));
    assert_eq!(postgres.acquire_timeout, Duration::from_secs(10));
    let claude = config.claude.as_ref().ok_or("missing claude")?;
    assert_eq!(claude.upstream, "https://api.anthropic.com/");
    assert!(claude.retry_429);
    Proxy::new(config)?;

    let text = format!(
        "{MINIMAL}{POSTGRES}max_connections = 4\nconnect_timeout = \"3s\"\nacquire_timeout = \"5s\"\n\
         {ENCRYPTION}{CLAUDE}upstream = \"http://localhost:1234/api\"\nretry_429 = false\n"
    );
    let config = Config::parse(&text, lookup)?;
    let postgres = config.postgres.as_ref().ok_or("missing postgres")?;
    assert_eq!(postgres.max_connections, 4);
    assert_eq!(postgres.connect_timeout, Duration::from_secs(3));
    assert_eq!(postgres.acquire_timeout, Duration::from_secs(5));
    let claude = config.claude.as_ref().ok_or("missing claude")?;
    assert_eq!(claude.upstream, "http://localhost:1234/api/");
    assert!(!claude.retry_429);
    Proxy::new(config)?;
    Ok(())
}

#[test]
fn adapter_environment_expansion_preserves_value_boundaries() -> Result<(), Box<dyn Error>> {
    let text = format!(
        "{MINIMAL}[postgres]\nurl = \"${{DB_URL}}\"\nnamespace = \"${{NAMESPACE}}\"\n\
         [credential_encryption]\nfingerprint_key_file = '/nonexistent/fingerprint'\nactive_key_id = \"key_1-a\"\nkeys = {{ key_1-a = \"${{KEY_FILE}}\" }}\n\
         [claude]\nupstream = \"${{UPSTREAM}}\"\n"
    );
    let config = Config::parse(&text, |name| match name {
        "DB_URL" => Some("postgresql://127.0.0.1:1/x?sslmode=require".into()),
        "NAMESPACE" => Some("people".into()),
        "KEY_FILE" => Some("/nonexistent/key-\"quoted\"".into()),
        "UPSTREAM" => Some("http://localhost:1234/api/".into()),
        other => lookup(other),
    })?;
    let encryption = config
        .credential_encryption
        .as_ref()
        .ok_or("missing keys")?;
    assert_eq!(
        encryption.keys.get("key_1-a"),
        Some(&PathBuf::from("/nonexistent/key-\"quoted\""))
    );
    Proxy::new(config)?;
    Ok(())
}

#[test]
fn claude_requires_both_storage_and_encryption() {
    for sections in [
        CLAUDE.to_owned(),
        format!("{POSTGRES}{CLAUDE}"),
        format!("{ENCRYPTION}{CLAUDE}"),
    ] {
        assert!(
            refused(&sections).contains("claude requires [postgres] and [credential_encryption]")
        );
    }
}

#[test]
fn claude_reserves_only_its_enabled_static_route_name() -> Result<(), Box<dyn Error>> {
    let route = "[[route]]\nname = \"claude\"\nupstream = \"http://localhost\"\n";
    assert!(
        refused(&format!("{POSTGRES}{ENCRYPTION}{CLAUDE}{route}"))
            .contains("prefix /claude is reserved")
    );
    Proxy::new(Config::parse(&format!("{MINIMAL}{route}"), lookup)?)?;
    let allowed = route.replace("name = \"claude\"", "name = \"claude-test\"");
    Proxy::new(Config::parse(
        &format!("{MINIMAL}{POSTGRES}{ENCRYPTION}{CLAUDE}{allowed}"),
        lookup,
    )?)?;
    Ok(())
}

#[test]
fn proxy_revalidates_adapter_settings_after_public_config_changes() -> Result<(), Box<dyn Error>> {
    let mut config = Config::parse(&format!("{MINIMAL}{POSTGRES}{ENCRYPTION}{CLAUDE}"), lookup)?;
    config.postgres = None;
    assert!(
        Proxy::new(config)
            .err()
            .ok_or("missing validation")?
            .to_string()
            .contains("claude requires")
    );
    Ok(())
}

#[test]
fn adapter_unknown_fields_and_bad_types_are_rejected() {
    for extra in [
        format!("{POSTGRES}max_connection = 1\n"),
        format!("{ENCRYPTION}active_key = \"a\"\n"),
        format!("{POSTGRES}{ENCRYPTION}{CLAUDE}upsteam = \"http://h\"\n"),
        format!("{POSTGRES}max_connections = \"bad\"\n"),
        format!("{POSTGRES}{ENCRYPTION}{CLAUDE}retry_429 = \"yes\"\n"),
        "[credential_encryption]\nfingerprint_key_file = '/nonexistent/fingerprint'\nactive_key_id = \"a\"\nkeys = { a = 42 }\n".into(),
    ] {
        assert!(refused(&extra).contains("invalid config"));
    }
}

#[test]
fn namespace_and_key_identifiers_are_bounded_and_path_independent() -> Result<(), Box<dyn Error>> {
    for id in [
        String::new(),
        "has space".into(),
        "a.b".into(),
        "a/b".into(),
        "é".into(),
        "a".repeat(64),
    ] {
        assert!(refused(&POSTGRES.replace("test_1-a", &id)).contains("postgres.namespace"));
        let encryption = format!(
            "[credential_encryption]\nfingerprint_key_file = '/nonexistent/fingerprint'\nactive_key_id = \"{id}\"\nkeys = {{ \"{id}\" = \"/missing\" }}\n"
        );
        assert!(refused(&encryption).contains("key IDs"));
    }
    let max_id = "a".repeat(63);
    Proxy::new(Config::parse(
        &format!("{MINIMAL}{}", POSTGRES.replace("test_1-a", &max_id)),
        lookup,
    )?)?;
    let encryption = format!(
        "[credential_encryption]\nfingerprint_key_file = '/nonexistent/fingerprint'\nactive_key_id = \"{max_id}\"\nkeys = {{ {max_id} = \"/missing\" }}\n"
    );
    Proxy::new(Config::parse(&format!("{MINIMAL}{encryption}"), lookup)?)?;
    Ok(())
}

#[test]
fn all_key_ids_paths_and_active_key_membership_are_validated() {
    for keys in ["{}", "{ other = \"/missing\" }"] {
        assert!(
            refused(&format!(
                "[credential_encryption]\nfingerprint_key_file = '/nonexistent/fingerprint'\nactive_key_id = \"a\"\nkeys = {keys}\n"
            ))
            .contains("must name an entry")
        );
    }
    for keys in ["{ a = \"\" }", "{ a = \"/missing\", b = \"\" }"] {
        assert!(
            refused(&format!(
                "[credential_encryption]\nfingerprint_key_file = '/nonexistent/fingerprint'\nactive_key_id = \"a\"\nkeys = {keys}\n"
            ))
            .contains("paths must not be empty")
        );
    }
    assert!(refused("[credential_encryption]\nfingerprint_key_file = '/nonexistent/fingerprint'\nactive_key_id = \"a\"\nkeys = { a = \"/missing\", \"bad.id\" = \"/missing\" }\n").contains("key IDs"));
}

#[test]
fn postgres_rejects_bad_urls_pool_sizes_and_timeouts() {
    for url in [
        "http://localhost/db",
        "postgres://",
        "not a url",
        "postgres://localhost/db#fragment",
        "postgres://localhost:bad/db",
    ] {
        assert!(
            refused(&POSTGRES.replace("postgres://127.0.0.1:1/x", url)).contains("postgres.url")
        );
    }
    assert!(refused(&format!("{POSTGRES}max_connections = 0\n")).contains("must be positive"));
    for field in ["connect_timeout", "acquire_timeout"] {
        for seconds in ["0s", "18446744073709551615s"] {
            assert!(
                refused(&format!("{POSTGRES}{field} = \"{seconds}\"\n"))
                    .contains("positive, representable duration")
            );
        }
        for seconds in ["2m", "-1s", "1.5s", "18446744073709551616s"] {
            assert!(
                refused(&format!("{POSTGRES}{field} = \"{seconds}\"\n")).contains("whole seconds")
            );
        }
        assert!(refused(&format!("{POSTGRES}{field} = 10\n")).contains("invalid config"));
    }
}

#[test]
fn claude_rejects_unsafe_or_malformed_upstreams() {
    for url in [
        "ftp://localhost",
        "not a url",
        "http://",
        "https://name:password@localhost",
        "https://localhost?key=secret",
        "https://localhost/#fragment",
    ] {
        assert!(
            refused(&format!(
                "{POSTGRES}{ENCRYPTION}{CLAUDE}upstream = \"{url}\"\n"
            ))
            .contains("claude.upstream must be an http(s) URL")
        );
    }
}

#[test]
fn adapter_errors_and_postgres_debug_do_not_disclose_config_values() -> Result<(), Box<dyn Error>> {
    const CANARY: &str = "config-redaction-canary";
    let url = format!("postgres://user:{CANARY}@localhost/db");
    for text in [
        format!(
            "{MINIMAL}[postgres]\nurl = \"{url}\"\nnamespace = \"ok\"\nmax_connections = \"{CANARY}\"\n"
        ),
        format!("{MINIMAL}[postgres]\nurl = \"{url}\" trailing\n"),
        format!("{MINIMAL}[postgres]\nurl = \"{url}${{UNFINISHED\"\n"),
        format!("{MINIMAL}[postgres]\nurl = \"{CANARY}\"\nnamespace = \"ok\"\n"),
    ] {
        let error = Config::parse(&text, lookup)
            .err()
            .ok_or("invalid config accepted")?;
        assert!(!error.to_string().contains(CANARY));
        assert!(!format!("{error:?}").contains(CANARY));
    }
    let config = Config::parse(
        &format!(
            "{MINIMAL}{}",
            POSTGRES.replace("postgres://127.0.0.1:1/x", &url)
        ),
        lookup,
    )?;
    assert!(!format!("{:?}", config.postgres).contains(CANARY));
    Ok(())
}

#[test]
fn check_config_accepts_offline_adapter_and_rejects_reserved_prefix() -> Result<(), Box<dyn Error>>
{
    let text = format!(
        "listen = \"127.0.0.1:0\"\nproxy_token = \"literal\"\n{POSTGRES}{ENCRYPTION}{CLAUDE}"
    );
    for (name, extra, code, expected) in [
        ("offline-adapter", "", 0, ": ok"),
        (
            "offline-conflict",
            "[[route]]\nname = \"claude\"\nupstream = \"http://localhost\"\n",
            1,
            "prefix /claude is reserved",
        ),
    ] {
        let path = temp_config(name, &format!("{text}{extra}"));
        let output = Command::new(env!("CARGO_BIN_EXE_llmproxy"))
            .arg("--check-config")
            .arg(&path)
            .output()?;
        fs::remove_file(path)?;
        assert_eq!(output.status.code(), Some(code));
        let observed = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(observed.contains(expected));
        assert!(!observed.contains("postgres://"));
        assert!(!observed.contains("/nonexistent/llmproxy-master-key"));
    }
    Ok(())
}
