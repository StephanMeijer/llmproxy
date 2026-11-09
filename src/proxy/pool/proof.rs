//! Reads a provider's `/models` catalog into proof of which models are free, per `docs/pools.md`.

use std::collections::BTreeMap;

use serde_json::Value;

use crate::config::{Catalog, Endpoint};

/// What a catalog proves about one model.
#[derive(Debug, PartialEq, Eq)]
pub struct ModelProof {
    pub free: bool,
    /// The endpoint kinds the catalog lists; `None` when the catalog does not list them.
    pub kinds: Option<Vec<Endpoint>>,
}

/// The catalog body as a whole is unusable; the message never echoes the body.
#[derive(Debug, PartialEq, Eq)]
pub struct ProofError(pub &'static str);

/// Whether `(parent, key)` names a threshold or factor rather than a price.
type Exempt = fn(&str, &str) -> bool;

/// Proof per model id. Every rule fails closed: anything unexpected means not free.
pub fn parse(catalog: Catalog, body: &[u8]) -> Result<BTreeMap<String, ModelProof>, ProofError> {
    let body: Value =
        serde_json::from_slice(body).map_err(|_| ProofError("catalog is not JSON"))?;
    let models = body
        .get("data")
        .and_then(Value::as_array)
        .ok_or(ProofError("catalog has no data array"))?;
    let (root, required, exempt): (_, [&str; 2], Exempt) = match catalog {
        Catalog::Openrouter => (
            "pricing",
            ["/pricing/prompt", "/pricing/completion"],
            |parent, key| parent == "overrides" && key == "min_prompt_tokens",
        ),
        Catalog::Opencode => (
            "cost",
            ["/cost/text/input/price", "/cost/text/output/price"],
            |parent, key| key == "when" || (parent == "multipliers" && key == "factor"),
        ),
    };
    Ok(models
        .iter()
        .filter_map(|model| {
            let id = model.get("id")?.as_str()?;
            let free = required
                .iter()
                .all(|path| model.pointer(path).is_some_and(is_zero))
                && model
                    .get(root)
                    .is_some_and(|prices| all_zero(prices, "", exempt));
            let kinds = (catalog == Catalog::Opencode).then(|| {
                model
                    .get("api")
                    .and_then(Value::as_object)
                    .into_iter()
                    .flat_map(|api| api.keys())
                    .filter_map(|kind| match kind.as_str() {
                        "chat-completions" => Some(Endpoint::Chat),
                        "responses" => Some(Endpoint::Responses),
                        _ => None,
                    })
                    .collect()
            });
            Some((id.to_owned(), ModelProof { free, kinds }))
        })
        .collect())
}

/// Every value under `value` is exactly zero, except keys `exempt` names; array entries inherit
/// the array's key as their parent.
fn all_zero(value: &Value, parent: &str, exempt: Exempt) -> bool {
    match value {
        Value::Object(map) => map
            .iter()
            .all(|(key, value)| exempt(parent, key) || all_zero(value, key, exempt)),
        Value::Array(items) => items.iter().all(|item| all_zero(item, parent, exempt)),
        _ => is_zero(value),
    }
}

/// A decimal string or number whose digits are all zero, such as `"0"`, `"0.000"` or `0`.
/// Parsing as a float would wrongly accept `"-0"` or underflow `"1e-400"` to zero.
fn is_zero(value: &Value) -> bool {
    let text = match value {
        Value::String(text) => text.clone(),
        Value::Number(number) => number.to_string(),
        _ => return false,
    };
    let (whole, fraction) = text.split_once('.').unwrap_or((&text, ""));
    !whole.is_empty()
        && whole
            .bytes()
            .chain(fraction.bytes())
            .all(|digit| digit == b'0')
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    reason = "unit test assertions"
)]
mod tests {
    use super::{Catalog, Endpoint, ModelProof, ProofError, parse};

    fn openrouter(pricing: &str) -> bool {
        let body = format!(r#"{{"data":[{{"id":"liquid/lfm-3-1.7b:free","pricing":{pricing}}}]}}"#);
        parse(Catalog::Openrouter, body.as_bytes()).unwrap()["liquid/lfm-3-1.7b:free"].free
    }

    fn opencode(entry: &str) -> ModelProof {
        let body = format!(r#"{{"data":[{{"id":"big-pickle",{entry}}}]}}"#);
        parse(Catalog::Opencode, body.as_bytes())
            .unwrap()
            .remove("big-pickle")
            .unwrap()
    }

    #[test]
    fn openrouter_is_free_only_when_every_price_is_exactly_zero() {
        assert!(openrouter(r#"{"prompt":"0","completion":"0"}"#));
        assert!(openrouter(
            r#"{"prompt":"0.0","completion":"0.000","image":"0"}"#
        ));
        // A key the proxy does not know counts as a price.
        assert!(openrouter(
            r#"{"prompt":"0","completion":"0","web_search":"0"}"#
        ));
        assert!(!openrouter(
            r#"{"prompt":"0","completion":"0","web_search":"0.0001"}"#
        ));
        assert!(!openrouter(
            r#"{"prompt":"0.00000015","completion":"0.0000006"}"#
        ));
        // OpenRouter's own routers price themselves at -1.
        assert!(!openrouter(r#"{"prompt":"-1","completion":"-1"}"#));
    }

    #[test]
    fn openrouter_partial_or_odd_price_objects_are_not_proof() {
        assert!(!openrouter(r#"{"prompt":"0"}"#));
        assert!(!openrouter("{}"));
        assert!(!openrouter(r#""0""#));
        for odd in [
            r#""free""#,
            r#""-0""#,
            r#"".0""#,
            r#""1e-400""#,
            "null",
            "false",
            "[]",
        ] {
            assert!(
                !openrouter(&format!(r#"{{"prompt":{odd},"completion":"0"}}"#)),
                "{odd}"
            );
        }
    }

    #[test]
    fn openrouter_overrides_must_be_zero_but_their_threshold_is_not_a_price() {
        let free = r#"{"prompt":"0","completion":"0",
            "overrides":[{"min_prompt_tokens":200000,"prompt":"0","completion":"0"}]}"#;
        assert!(openrouter(free));
        let paid = r#"{"prompt":"0","completion":"0",
            "overrides":[{"min_prompt_tokens":200000,"prompt":"0.000003","completion":"0"}]}"#;
        assert!(!openrouter(paid));
        // The threshold is only exempt inside an override.
        assert!(!openrouter(
            r#"{"prompt":"0","completion":"0","min_prompt_tokens":200000}"#
        ));
    }

    #[test]
    fn openrouter_lists_no_endpoint_kinds() {
        let body = br#"{"data":[{"id":"a","pricing":{"prompt":"0","completion":"0"}}]}"#;
        assert_eq!(parse(Catalog::Openrouter, body).unwrap()["a"].kinds, None);
    }

    #[test]
    fn opencode_is_free_only_when_every_cost_is_zero() {
        let free = r#""cost":{"text":{"input":{"price":0,"cache":{"read":0,"write":0}},
            "output":{"price":0}}}"#;
        assert!(opencode(free).free);
        let paid = r#""cost":{"text":{"input":{"price":1.25,"cache":{"read":0.125}},
            "output":{"price":10}}}"#;
        assert!(!opencode(paid).free);
        assert!(!opencode(r#""cost":{"text":{"input":{"price":0}}}"#).free);
        assert!(!opencode(r#""api":{}"#).free);
    }

    #[test]
    fn opencode_conditions_and_factors_are_not_prices() {
        let long_context = r#""cost":{"text":{"input":{"price":0},"output":{"price":0}},
            "overrides":[{"when":{"context":{"gt":200000}},
                "text":{"input":{"price":0},"output":{"price":0}}}]}"#;
        assert!(opencode(long_context).free);
        let multiplied = r#""cost":{"text":{"input":{"price":0},"output":{"price":0}},
            "multipliers":[{"when":{"tier":"priority"},"factor":2}]}"#;
        assert!(opencode(multiplied).free);
        let paid_override = r#""cost":{"text":{"input":{"price":0},"output":{"price":0}},
            "overrides":[{"when":{"context":{"gt":200000}},"text":{"input":{"price":2}}}]}"#;
        assert!(!opencode(paid_override).free);
    }

    #[test]
    fn opencode_api_keys_map_to_endpoint_kinds() {
        let entry = r#""api":{"chat-completions":{},"responses":{},"messages":{}}"#;
        assert_eq!(
            opencode(entry).kinds,
            Some(vec![Endpoint::Chat, Endpoint::Responses])
        );
        assert_eq!(opencode(r#""cost":{}"#).kinds, Some(vec![]));
    }

    #[test]
    fn models_without_a_string_id_are_skipped() {
        let body = br#"{"data":[{"id":7},{"pricing":{}},"x",{"id":"a"}]}"#;
        let proof = parse(Catalog::Openrouter, body).unwrap();
        assert_eq!(proof.keys().collect::<Vec<_>>(), ["a"]);
    }

    #[test]
    fn an_unusable_body_is_an_error_that_does_not_echo_it() {
        assert_eq!(
            parse(Catalog::Openrouter, b"secret <html>"),
            Err(ProofError("catalog is not JSON"))
        );
        assert_eq!(
            parse(Catalog::Opencode, br#"{"models":[]}"#),
            Err(ProofError("catalog has no data array"))
        );
        assert_eq!(
            parse(Catalog::Opencode, br#"{"data":{}}"#),
            Err(ProofError("catalog has no data array"))
        );
    }
}
