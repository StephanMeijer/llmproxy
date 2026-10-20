#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{env, fs, path::PathBuf, time::Duration};

use llmproxy::{
    config::{Config, ConfigError},
    proxy::Proxy,
};

const MINIMAL: &str = "listen = \"127.0.0.1:0\"\nproxy_token = \"${TOKEN}\"\n";
const DEFAULT_MAX_BODY: usize = 16 * 1024 * 1024;
const UNSET_VAR: &str = "LLMPROXY_TEST_SURELY_UNSET_VAR";

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
    assert!(err.0.contains("TOKEN is not set"), "{err}");
    let err = Config::parse("proxy_token = \"${TOKEN\"", lookup).unwrap_err();
    assert!(err.0.contains("unterminated"), "{err}");
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
    assert!(err.0.contains("NOPE is not set"), "{err}");
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
    assert!(err.0.contains("NOPE is not set"), "{err}");
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
    assert!(err.0.contains(&format!("{UNSET_VAR} is not set")), "{err}");

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
