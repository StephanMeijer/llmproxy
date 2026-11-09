//! Spots a positive cost a provider reports in a relayed pool response, without changing it.

use std::mem;

use serde_json::Value;

/// A body, or an SSE line, longer than this is not inspected, so memory stays bounded.
const MAX_PENDING: usize = 1 << 20;

/// Reads a JSON body whole, or an SSE stream line by line, as its chunks pass.
pub struct Charge {
    sse: bool,
    pending: Vec<u8>,
    /// The current body or line outgrew `MAX_PENDING`; skip it.
    skipping: bool,
    charged: bool,
}

impl Charge {
    pub const fn new(sse: bool) -> Self {
        Self {
            sse,
            pending: Vec::new(),
            skipping: false,
            charged: false,
        }
    }

    /// Reads one chunk; true the first time the response reports a positive cost.
    pub fn feed(&mut self, mut chunk: &[u8]) -> bool {
        let before = self.charged;
        if self.sse {
            while let Some(at) = chunk.iter().position(|b| *b == b'\n') {
                let (line, rest) = chunk.split_at(at);
                self.push(line);
                self.check();
                chunk = rest.get(1..).unwrap_or_default();
            }
        }
        self.push(chunk);
        !before && self.charged
    }

    /// Reads what is left at the end of the body; true if that first reports a positive cost.
    pub fn finish(&mut self) -> bool {
        let before = self.charged;
        self.check();
        !before && self.charged
    }

    fn push(&mut self, bytes: &[u8]) {
        if self.pending.len() + bytes.len() > MAX_PENDING {
            self.skipping = true;
            self.pending = Vec::new();
        } else if !self.skipping {
            self.pending.extend_from_slice(bytes);
        }
    }

    fn check(&mut self) {
        let pending = mem::take(&mut self.pending);
        if !mem::take(&mut self.skipping) {
            self.charged |= costs(&pending);
        }
    }
}

/// Whether a JSON body or SSE `data:` line has a `cost`, top level or in `usage`, above zero.
fn costs(line: &[u8]) -> bool {
    let line = line.trim_ascii();
    let line = line.strip_prefix(b"data:").unwrap_or(line);
    let Ok(value) = serde_json::from_slice::<Value>(line) else {
        return false;
    };
    [value.pointer("/usage/cost"), value.get("cost")]
        .into_iter()
        .flatten()
        .any(|cost| match cost {
            Value::Number(n) => n.as_f64().is_some_and(|n| n > 0.0),
            Value::String(s) => s.trim().parse::<f64>().is_ok_and(|n| n > 0.0),
            _ => false,
        })
}

#[cfg(test)]
mod tests {
    use super::{Charge, MAX_PENDING};

    /// Feeds `chunks`, then finishes; returns how many times a charge was first reported.
    fn hits(sse: bool, chunks: &[&[u8]]) -> usize {
        let mut charge = Charge::new(sse);
        let fed = chunks.iter().filter(|c| charge.feed(c)).count();
        fed + usize::from(charge.finish())
    }

    #[test]
    fn a_positive_cost_is_found_once_in_json_or_sse() {
        assert_eq!(hits(false, &[br#"{"usage":{"co"#, br#"st":0.0002}}"#]), 1);
        assert_eq!(hits(false, &[b"{\n  \"cost\": \"0.1\"\n}\n"]), 1);
        let sse: &[&[u8]] = &[
            b"data: {\"n\":1}\n\ndata: {\"usage\":{\"cost\":0.0",
            b"1}}\r\n\ndata: {\"cost\":2}\n",
            b"data: [DONE]",
        ];
        assert_eq!(hits(true, sse), 1);
        // A last line without its newline is read at the end.
        assert_eq!(hits(true, &[br#"data: {"cost":"3"}"#]), 1);
    }

    #[test]
    fn zero_missing_or_odd_costs_are_not_charges() {
        for body in [
            br#"{"usage":{"cost":0}}"#.as_slice(),
            br#"{"usage":{"cost":"0.000"}}"#,
            br#"{"usage":{"cost":null},"cost":"free"}"#,
            br#"{"choices":[{"cost":1}]}"#,
            b"not json",
        ] {
            assert_eq!(hits(false, &[body]), 0, "{}", String::from_utf8_lossy(body));
        }
        // A JSON body is read whole, not by line.
        assert_eq!(hits(true, &[b"{\n\"cost\": 1\n}"]), 0);
    }

    #[test]
    fn an_oversized_line_is_skipped_but_the_next_is_read() {
        let big = vec![b'x'; MAX_PENDING];
        let chunks: &[&[u8]] = &[b"data: {\"cost\":", &big, b"1}\ndata: {\"cost\":1}\n"];
        assert_eq!(hits(true, chunks), 1);
        assert_eq!(hits(false, &[b"{\"cost\":", &big, b"1}"]), 0);
    }
}
