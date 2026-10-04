//! Prepared native-artifact record admission. This module advertises no capability.
use serde_json::Value;
use std::collections::BTreeSet;

#[path = "note_artifact/canonical.rs"]
pub mod canonical;
#[path = "note_artifact/request.rs"]
pub mod request;

pub const RECORD_BYTES: usize = 16_384;
pub const RECORD_DEPTH: usize = 32;
pub const RECORD_TOKENS: usize = 2_048;
pub const RECORD_MEMBERS: usize = 128;
pub const RECORD_KEY_BYTES: usize = 1_024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordError {
    Budget,
    Syntax,
    Text,
}

#[derive(Debug)]
pub struct ArtifactRecord {
    pub kind: String,
    pub value: Value,
}

#[cfg(test)]
mod tests {
    use super::{decode_record, preflight_record, RecordError, RECORD_BYTES};
    use serde_json::json;

    #[test]
    fn accepts_exact_bounded_record_and_preserves_native_text() {
        let record = r#"{"kind":"diff.fragment","value":{"text":"😀 &amp;\r\n中"}}"#;
        let decoded = decode_record(record).unwrap();
        assert_eq!(decoded.kind, "diff.fragment");
        assert_eq!(decoded.value["text"], "😀 &amp;\r\n中");
    }

    #[test]
    fn rejects_depth_before_recursive_deserialization() {
        let at_limit = format!("{}0{}", "[".repeat(32), "]".repeat(32));
        assert!(preflight_record(&at_limit).is_ok());
        let over = format!("{}0{}", "[".repeat(33), "]".repeat(33));
        assert_eq!(preflight_record(&over), Err(RecordError::Budget));
    }

    #[test]
    fn bounds_array_members_and_object_keys_independently() {
        assert!(preflight_record(&serde_json::to_string(&vec![0; 128]).unwrap()).is_ok());
        assert_eq!(
            preflight_record(&serde_json::to_string(&vec![0; 129]).unwrap()),
            Err(RecordError::Budget)
        );
        let value = json!({"x".repeat(1024):0});
        assert!(preflight_record(&value.to_string()).is_ok());
        assert_eq!(
            preflight_record(&json!({"x".repeat(1025):0}).to_string()),
            Err(RecordError::Budget)
        );
        let keys = (0..129)
            .map(|i| (format!("key{i}"), json!(i)))
            .collect::<serde_json::Map<_, _>>();
        assert_eq!(
            preflight_record(&Value::Object(keys).to_string()),
            Err(RecordError::Budget)
        );
    }

    #[test]
    fn bounds_tokens_and_bytes_before_json_tree_allocation() {
        let nodes = vec![vec![0; 127]; 17];
        assert_eq!(
            preflight_record(&serde_json::to_string(&nodes).unwrap()),
            Err(RecordError::Budget)
        );
        assert_eq!(
            preflight_record(&" ".repeat(RECORD_BYTES + 1)),
            Err(RecordError::Budget)
        );
    }

    #[test]
    fn escaped_surrogates_and_nul_are_checked_before_tree_parse() {
        assert!(preflight_record(r#"["\ud83d\ude00"]"#).is_ok());
        for source in [
            r#"["\ud83d"]"#,
            r#"["\ude00"]"#,
            r#"["\ud83d\u0041"]"#,
            r#"["\u0000"]"#,
        ] {
            assert_eq!(preflight_record(source), Err(RecordError::Text), "{source}");
        }
    }

    #[test]
    fn duplicate_keys_and_invalid_envelopes_are_not_renderer_records() {
        assert_eq!(
            preflight_record(r#"{"code":1,"\u0063ode":2}"#),
            Err(RecordError::Syntax)
        );
        for source in [
            r#"[]"#,
            r#"{"kind":"diff.row"}"#,
            r#"{"kind":"diff.row","value":{},"extra":0}"#,
            r#"{"kind":"diff.row","value":null}"#,
        ] {
            assert!(decode_record(source).is_err(), "{source}");
        }
    }

    use serde_json::Value;
}

#[derive(Default)]
struct Container {
    opener: u8,
    members: usize,
    started: bool,
    keys: BTreeSet<String>,
}

fn hex_unit(bytes: &[u8], at: usize) -> Result<u16, RecordError> {
    let mut value = 0u16;
    for byte in bytes.get(at..at + 4).ok_or(RecordError::Syntax)? {
        let digit = match byte {
            b'0'..=b'9' => byte - b'0',
            b'a'..=b'f' => byte - b'a' + 10,
            b'A'..=b'F' => byte - b'A' + 10,
            _ => return Err(RecordError::Syntax),
        };
        value = (value << 4) | u16::from(digit);
    }
    Ok(value)
}

/// Scan one JSON scalar without allocating its decoded contents.
fn string_end(source: &str, mut at: usize) -> Result<(usize, usize), RecordError> {
    let bytes = source.as_bytes();
    at += 1;
    let mut decoded = 0;
    loop {
        let byte = *bytes.get(at).ok_or(RecordError::Syntax)?;
        if byte == b'"' {
            return Ok((at + 1, decoded));
        }
        if byte == b'\\' {
            at += 1;
            let escape = *bytes.get(at).ok_or(RecordError::Syntax)?;
            at += 1;
            match escape {
                b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't' => decoded += 1,
                b'u' => {
                    let first = hex_unit(bytes, at)?;
                    at += 4;
                    let scalar = if (0xd800..=0xdbff).contains(&first) {
                        if bytes.get(at..at + 2) != Some(b"\\u") {
                            return Err(RecordError::Text);
                        }
                        let second = hex_unit(bytes, at + 2)?;
                        if !(0xdc00..=0xdfff).contains(&second) {
                            return Err(RecordError::Text);
                        }
                        at += 6;
                        0x1_0000
                            + ((u32::from(first) - 0xd800) << 10)
                            + (u32::from(second) - 0xdc00)
                    } else {
                        u32::from(first)
                    };
                    if scalar == 0 {
                        return Err(RecordError::Text);
                    }
                    decoded += char::from_u32(scalar).ok_or(RecordError::Text)?.len_utf8();
                }
                _ => return Err(RecordError::Syntax),
            }
        } else {
            if byte < 32 {
                return Err(RecordError::Text);
            }
            let size = source[at..]
                .chars()
                .next()
                .ok_or(RecordError::Syntax)?
                .len_utf8();
            at += size;
            decoded += size;
        }
        if decoded > RECORD_BYTES {
            return Err(RecordError::Budget);
        }
    }
}

/// Enforce structural budgets before constructing any recursive JSON value.
/// This is framing validation only; the registered profile validates semantics.
pub fn preflight_record(source: &str) -> Result<(), RecordError> {
    preflight(source, RECORD_BYTES)
}

fn preflight(source: &str, byte_limit: usize) -> Result<(), RecordError> {
    if source.len() > byte_limit {
        return Err(RecordError::Budget);
    }
    let bytes = source.as_bytes();
    let mut stack = Vec::<Container>::with_capacity(RECORD_DEPTH);
    let mut tokens = 0;
    let mut at = 0;
    while at < bytes.len() {
        let byte = bytes[at];
        if byte.is_ascii_whitespace() {
            at += 1;
            continue;
        }
        match byte {
            b'[' | b'{' => {
                if let Some(parent) = stack.last_mut() {
                    if parent.opener == b'[' && !parent.started {
                        parent.members = 1;
                        parent.started = true;
                    }
                }
                if stack.len() == RECORD_DEPTH {
                    return Err(RecordError::Budget);
                }
                stack.push(Container {
                    opener: byte,
                    ..Container::default()
                });
                tokens += 1;
                at += 1;
            }
            b']' | b'}' => {
                let container = stack.pop().ok_or(RecordError::Syntax)?;
                if (byte == b']') != (container.opener == b'[') {
                    return Err(RecordError::Syntax);
                }
                at += 1;
            }
            b':' => {
                let container = stack
                    .last_mut()
                    .filter(|c| c.opener == b'{')
                    .ok_or(RecordError::Syntax)?;
                container.members += 1;
                if container.members > RECORD_MEMBERS {
                    return Err(RecordError::Budget);
                }
                at += 1;
            }
            b',' => {
                let container = stack.last_mut().ok_or(RecordError::Syntax)?;
                if container.opener == b'[' {
                    container.members += 1;
                    if container.members > RECORD_MEMBERS {
                        return Err(RecordError::Budget);
                    }
                }
                at += 1;
            }
            b'"' => {
                let (end, size) = string_end(source, at)?;
                let next = bytes[end..]
                    .iter()
                    .position(|b| !b.is_ascii_whitespace())
                    .map(|offset| end + offset);
                if next.is_some_and(|next| bytes[next] == b':') {
                    if size > RECORD_KEY_BYTES {
                        return Err(RecordError::Budget);
                    }
                    let container = stack
                        .last_mut()
                        .filter(|c| c.opener == b'{')
                        .ok_or(RecordError::Syntax)?;
                    let key = serde_json::from_str::<String>(&source[at..end])
                        .map_err(|_| RecordError::Syntax)?;
                    if !container.keys.insert(key) {
                        return Err(RecordError::Syntax);
                    }
                } else if let Some(parent) = stack.last_mut() {
                    if parent.opener == b'[' && !parent.started {
                        parent.members = 1;
                        parent.started = true;
                    }
                }
                tokens += 1;
                at = end;
            }
            _ => {
                if let Some(parent) = stack.last_mut() {
                    if parent.opener == b'[' && !parent.started {
                        parent.members = 1;
                        parent.started = true;
                    }
                }
                let start = at;
                while at < bytes.len()
                    && !bytes[at].is_ascii_whitespace()
                    && !b"[]{}:,\"".contains(&bytes[at])
                {
                    at += 1;
                }
                if start == at {
                    return Err(RecordError::Syntax);
                }
                tokens += 1;
            }
        }
        if tokens > RECORD_TOKENS {
            return Err(RecordError::Budget);
        }
    }
    if !stack.is_empty() {
        return Err(RecordError::Syntax);
    }
    Ok(())
}

/// Decode only a preflighted record. Profile-specific validation is mandatory
/// before its contents can be staged as an indexed native resource.
pub fn decode_record(source: &str) -> Result<ArtifactRecord, RecordError> {
    preflight_record(source)?;
    let value: Value = serde_json::from_str(source).map_err(|_| RecordError::Syntax)?;
    let Value::Object(mut object) = value else {
        return Err(RecordError::Syntax);
    };
    if object.len() != 2 {
        return Err(RecordError::Syntax);
    }
    let Some(Value::String(kind)) = object.remove("kind") else {
        return Err(RecordError::Syntax);
    };
    if kind.is_empty() || kind.len() > 128 {
        return Err(RecordError::Syntax);
    }
    let value = object
        .remove("value")
        .filter(Value::is_object)
        .ok_or(RecordError::Syntax)?;
    Ok(ArtifactRecord { kind, value })
}
