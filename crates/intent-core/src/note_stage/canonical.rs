//! Stage integrity envelopes use the request byte budget, not the unrelated
//! artifact record token budget. Transport still bounds the complete raw frame
//! before parsing and rejects duplicate keys/invalid Unicode at that boundary.
use super::{NoteMutationError, Result};
use serde_json::Value;
use sha2::{Digest, Sha256};

const BYTES: usize = 65536;

pub(super) fn digest(value: &Value) -> Result<String> {
    Ok(format!("{:x}", Sha256::digest(bytes(value)?.as_bytes())))
}

fn push(out: &mut String, text: &str) -> Result<()> {
    if out.len().saturating_add(text.len()) > BYTES {
        return Err(NoteMutationError::Budget);
    }
    out.push_str(text);
    Ok(())
}

fn string(value: &str, out: &mut String) -> Result<()> {
    push(out, "\"")?;
    for ch in value.chars() {
        let mut buffer = [0; 4];
        match ch {
            '"' => push(out, "\\\"")?,
            '\\' => push(out, "\\\\")?,
            '\u{08}' => push(out, "\\b")?,
            '\t' => push(out, "\\t")?,
            '\n' => push(out, "\\n")?,
            '\u{0c}' => push(out, "\\f")?,
            '\r' => push(out, "\\r")?,
            ch if ch <= '\u{1f}' => push(out, &format!("\\u{:04x}", u32::from(ch)))?,
            ch => push(out, ch.encode_utf8(&mut buffer))?,
        }
    }
    push(out, "\"")
}

pub(super) fn bytes(value: &Value) -> Result<String> {
    let mut out = String::new();
    write(value, &mut out, 0)?;
    Ok(out)
}

fn write(value: &Value, out: &mut String, depth: usize) -> Result<()> {
    // All admitted staged shapes are much shallower. This also bounds traversal
    // when computed_digest is invoked directly on an unvalidated request.
    if depth > 32 {
        return Err(NoteMutationError::Budget);
    }
    match value {
        Value::Null => push(out, "null"),
        Value::Bool(v) => push(out, if *v { "true" } else { "false" }),
        Value::Number(v) => {
            let number = v
                .as_f64()
                .filter(|v| v.is_finite())
                .ok_or(NoteMutationError::Invalid)?;
            push(out, ryu_js::Buffer::new().format_finite(number))
        }
        Value::String(v) => string(v, out),
        Value::Array(values) => {
            if values.len() > BYTES {
                return Err(NoteMutationError::Budget);
            }
            push(out, "[")?;
            for (i, value) in values.iter().enumerate() {
                if i > 0 {
                    push(out, ",")?;
                }
                write(value, out, depth + 1)?;
            }
            push(out, "]")
        }
        Value::Object(values) => {
            if values.len() > BYTES {
                return Err(NoteMutationError::Budget);
            }
            let mut fields: Vec<_> = values.iter().collect();
            fields.sort_unstable_by(|(a, _), (b, _)| a.encode_utf16().cmp(b.encode_utf16()));
            push(out, "{")?;
            for (i, (key, value)) in fields.into_iter().enumerate() {
                if i > 0 {
                    push(out, ",")?;
                }
                string(key, out)?;
                push(out, ":")?;
                write(value, out, depth + 1)?;
            }
            push(out, "}")
        }
    }
}
