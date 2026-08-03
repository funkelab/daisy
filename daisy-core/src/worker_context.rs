//! Encoding for the `DAISY_CONTEXT` handoff.
//!
//! A worker learns where to call home from a single environment variable
//! holding `key=value:key=value` pairs. That framing has no escape
//! mechanism, so any value containing `:` or `=` silently corrupts the
//! context — and daisy puts filesystem paths (the log directory) and
//! user-chosen task ids in there. A Windows path (`C:\logs`) or a task named
//! `stage=1` would otherwise arrive as garbage, or fail to parse at all, at
//! the far end of an `srun`.
//!
//! The values are therefore percent-encoded, and only for the three
//! characters that matter: `%` (the escape itself), `:` (pair separator) and
//! `=` (key/value separator). Everything else, including non-ASCII, passes
//! through untouched, so a context stays readable in a log or a `ps` listing:
//!
//! ```text
//! hostname=node07:port=41567:task_id=extract%3Dfrags:logdir=/nrs/my%3Alogs
//! ```
//!
//! Both ends of the handoff must agree, so the server (which formats the
//! string) and the Python `Context` (which parses it, and re-emits it for
//! workers that re-exec themselves) share these two functions.

/// Percent-encode the characters that the `key=value:key=value` framing
/// reserves. Cheap: returns the input unchanged when there is nothing to
/// escape, which is the overwhelmingly common case.
pub fn encode_value(value: &str) -> String {
    if !value.contains(['%', ':', '=']) {
        return value.to_string();
    }
    let mut out = String::with_capacity(value.len() + 8);
    for c in value.chars() {
        match c {
            '%' => out.push_str("%25"),
            ':' => out.push_str("%3A"),
            '=' => out.push_str("%3D"),
            _ => out.push(c),
        }
    }
    out
}

/// Reverse `encode_value`.
///
/// Unrecognized escapes are left alone rather than rejected: a context
/// written by an older daisy (or by hand) may contain a bare `%`, and
/// refusing to start a worker over it would be worse than passing it
/// through.
pub fn decode_value(value: &str) -> String {
    if !value.contains('%') {
        return value.to_string();
    }
    let mut out = String::with_capacity(value.len());
    let bytes = value.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            match &value[i + 1..i + 3] {
                "25" => {
                    out.push('%');
                    i += 3;
                    continue;
                }
                "3A" | "3a" => {
                    out.push(':');
                    i += 3;
                    continue;
                }
                "3D" | "3d" => {
                    out.push('=');
                    i += 3;
                    continue;
                }
                _ => {}
            }
        }
        // Not an escape we produce: copy this character verbatim. Indexing
        // by char boundary keeps multi-byte input intact.
        let c = value[i..].chars().next().expect("index is a char boundary");
        out.push(c);
        i += c.len_utf8();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leaves_ordinary_values_alone() {
        for v in ["node07", "41567", "extract-frags", "/nrs/logs", "über"] {
            assert_eq!(encode_value(v), v, "should not have escaped {v}");
            assert_eq!(decode_value(v), v);
        }
    }

    #[test]
    fn round_trips_the_reserved_characters() {
        for v in [
            "C:\\logs", "stage=1", "100%", "a:b=c%d", ":", "=", "%",
            "%3A", // literal text that looks like an escape
        ] {
            let encoded = encode_value(v);
            assert!(
                !encoded.contains(':') && !encoded.contains('='),
                "{v} encoded to {encoded}, which still breaks the framing"
            );
            assert_eq!(decode_value(&encoded), v, "round trip failed for {v}");
        }
    }

    #[test]
    fn a_path_with_a_colon_survives_the_full_framing() {
        let logdir = "/nrs/lab/my:logs";
        let encoded = format!("logdir={}", encode_value(logdir));
        // The framing splits on ':' first, then on the first '='.
        let tokens: Vec<&str> = encoded.split(':').collect();
        assert_eq!(tokens.len(), 1, "value leaked a separator: {encoded}");
        let (k, v) = tokens[0].split_once('=').unwrap();
        assert_eq!(k, "logdir");
        assert_eq!(decode_value(v), logdir);
    }

    #[test]
    fn passes_through_escapes_it_did_not_write() {
        // A bare '%' from an older daisy or a hand-built context.
        assert_eq!(decode_value("50%off"), "50%off");
        assert_eq!(decode_value("%"), "%");
        assert_eq!(decode_value("%ZZ"), "%ZZ");
        assert_eq!(decode_value("trailing%"), "trailing%");
    }

    #[test]
    fn decoding_is_a_no_op_without_escapes() {
        assert_eq!(decode_value("hostname"), "hostname");
    }
}
