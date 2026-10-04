//! Bounded raw lifecycle framing, separate from transport admission and authentication.
use super::{
    validate_id, Control, Operation, Read, Result, SessionError, CONTROL_BYTES, REQUEST_BYTES,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{collections::HashSet, io::Write};

#[derive(Debug)]
pub enum Method {
    Open(Operation),
    Read(Read),
    Close(Operation),
}
#[derive(Debug)]
pub struct Request {
    pub id: Value,
    pub method: Method,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    jsonrpc: String,
    id: Value,
    method: String,
    params: Value,
}

/// Reject duplicate decoded keys and total nesting above32 before recursive JSON allocation.
/// # Errors
/// Rejects oversize, ambiguous or malformed JSON; scratch remains input-bounded.
pub fn strict(raw: &str, limit: usize) -> Result<Value> {
    if raw.len() > limit {
        return Err(SessionError::Budget);
    }
    let b = raw.as_bytes();
    let mut i = 0;
    let mut stack: Vec<Option<HashSet<String>>> = Vec::new();
    while i < b.len() {
        match b[i] {
            b'"' => {
                let start = i;
                i += 1;
                while i < b.len() {
                    if b[i] == b'\\' {
                        i += 2;
                    } else if b[i] == b'"' {
                        break;
                    } else {
                        i += 1;
                    }
                }
                if i >= b.len() {
                    return Err(SessionError::Invalid);
                }
                let s: String =
                    serde_json::from_str(&raw[start..=i]).map_err(|_| SessionError::Invalid)?;
                let mut after = i + 1;
                while after < b.len() && b[after].is_ascii_whitespace() {
                    after += 1;
                }
                if b.get(after) == Some(&b':') {
                    let Some(Some(keys)) = stack.last_mut() else {
                        return Err(SessionError::Invalid);
                    };
                    if !keys.insert(s) {
                        return Err(SessionError::Invalid);
                    }
                }
            }
            b'{' | b'[' => {
                stack.push((b[i] == b'{').then(HashSet::new));
                if stack.len() > 32 {
                    return Err(SessionError::Invalid);
                }
            }
            b'}' | b']' if stack.pop().is_none() => return Err(SessionError::Invalid),
            _ => {}
        }
        i += 1;
    }
    if !stack.is_empty() {
        return Err(SessionError::Invalid);
    }
    serde_json::from_str(raw).map_err(|_| SessionError::Invalid)
}
/// Decode the exact three prepared methods without routing or allocating an owner.
/// # Errors
/// Rejects invalid framing, field sets, ID, descriptor, or read sequence/ref budget.
pub fn parse(raw: &str) -> Result<Request> {
    let f: Envelope =
        serde_json::from_value(strict(raw, REQUEST_BYTES)?).map_err(|_| SessionError::Invalid)?;
    if f.jsonrpc != "2.0" {
        return Err(SessionError::Invalid);
    }
    validate_id(&f.id)?;
    let method = match f.method.as_str() {
        "note.sourceSession.open" | "note.sourceSession.close" => {
            let op: Operation =
                serde_json::from_value(f.params).map_err(|_| SessionError::Invalid)?;
            op.validate()?;
            if f.method.ends_with("open") {
                Method::Open(op)
            } else {
                Method::Close(op)
            }
        }
        "note.sourceSession.read" => {
            let r: Read = serde_json::from_value(f.params).map_err(|_| SessionError::Invalid)?;
            super::token(&r.workspace_id)?;
            super::hash(&r.operation_id, 64)?;
            if r.sequence > crate::note_artifact::request::SAFE_INTEGER {
                return Err(SessionError::Invalid);
            }
            r.request.validate()?;
            Method::Read(r)
        }
        _ => return Err(SessionError::Unavailable),
    };
    Ok(Request { id: f.id, method })
}
struct Frame {
    bytes: Vec<u8>,
    limit: usize,
}
impl Write for Frame {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        if b.len() > self.limit - self.bytes.len() {
            return Err(std::io::Error::other("source frame budget"));
        }
        self.bytes.extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
/// Serialize within a preselected encoded byte budget. The caller owns the frame admission.
/// # Errors
/// Rejects envelopes exceeding the complete escaped UTF-8 limit.
pub fn bounded(value: &Value, limit: usize) -> Result<String> {
    let mut f = Frame {
        bytes: Vec::with_capacity(limit),
        limit,
    };
    serde_json::to_writer(&mut f, value).map_err(|_| SessionError::Budget)?;
    String::from_utf8(f.bytes).map_err(|_| SessionError::Invalid)
}
/// Build a source-free result envelope using the current actual RPC ID.
/// # Errors
/// Rejects invalid controls/IDs or a complete control frame above4096 bytes.
pub fn control(c: &Control, id: &Value) -> Result<String> {
    c.validate()?;
    validate_id(id)?;
    bounded(&json!({"jsonrpc":"2.0","id":id,"result":c}), CONTROL_BYTES)
}
/// Bound error data to its public code, never propagating underlying error strings.
/// # Errors
/// Rejects invalid IDs or a complete error frame above4096 bytes.
pub fn error(e: SessionError, id: &Value) -> Result<String> {
    validate_id(id)?;
    bounded(
        &json!({"jsonrpc":"2.0","id":id,"error":{"code":e.number(),"message":e.code(),"data":{"code":e.code()}}}),
        CONTROL_BYTES,
    )
}
