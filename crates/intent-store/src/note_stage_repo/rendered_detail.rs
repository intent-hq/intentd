//! Bounded metadata context for already verified rendered identity captures.
//! Only the accepted-hit matcher calls `hit_ref`. The owner supplies the original
//! authenticated Context and rechecks authorization/view expiry before publication.
//! These fixed-subset resources are bounded independently of each output budget;
//! this synchronous adapter does not establish native capture or storage ownership.
use super::search_output::{hit_id, invalid, sign, verify, Context};
use intent_core::{
    note_mutation::NoteMutationError,
    note_receipt_detail::{ReceiptDetailKind, ReceiptDetailQuery},
    Error, Result,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

const REF: &str = "nrd1.";
const CURSOR: &str = "nrc1.";
const SAFE: u64 = 9_007_199_254_740_991;

fn budget() -> Error {
    Error::NoteMutation(NoteMutationError::Budget)
}
fn units(text: &str) -> u64 {
    u64::try_from(text.encode_utf16().count()).expect("bounded text length fits")
}
fn byte_at(text: &str, offset: u64) -> Result<usize> {
    let mut at = 0;
    for (byte, scalar) in text.char_indices() {
        if at == offset {
            return Ok(byte);
        }
        at += u64::try_from(scalar.len_utf16()).expect("scalar width fits");
        if at > offset {
            return Err(invalid());
        }
    }
    if at == offset {
        Ok(text.len())
    } else {
        Err(invalid())
    }
}
fn matched(context: &Context, start: u64, end: u64) -> Result<(u64, u64)> {
    let capture = context.rendered.as_ref().ok_or_else(invalid)?;
    let range = &capture.source_range;
    if context.length > SAFE
        || context.generation > SAFE
        || range.end > context.length
        || range.start > range.end
        || capture.text.is_empty()
        || capture.text.len() > 16384
        || units(&capture.text) > 4096
        || units(&capture.text) != range.end - range.start
        || capture.selected_range.start > capture.selected_range.end
        || capture.selected_range.end > range.end - range.start
        || start < range.start
        || start >= end
        || end > range.end
    {
        return Err(invalid());
    }
    let (first, last) = (start - range.start, end - range.start);
    if first < capture.selected_range.start || last > capture.selected_range.end {
        return Err(invalid());
    }
    for position in [
        first,
        last,
        capture.selected_range.start,
        capture.selected_range.end,
    ] {
        byte_at(&capture.text, position)?;
    }
    Ok((first, last))
}

/// `start/end` must come from an actual accepted rendered hit, not caller ranges.
pub(super) fn hit_ref(context: &Context, start: u64, end: u64) -> Result<String> {
    matched(context, start, end)?;
    sign(REF, &context.key, &context.binding, &[start, end, 0, 0, 0])
}

struct Node<'a> {
    value: &'a Value,
    parent: Option<usize>,
    key: Option<&'a str>,
    index: Option<usize>,
    children: Vec<usize>,
}
fn tree<'a>(
    value: &'a Value,
    nodes: &mut Vec<Node<'a>>,
    parent: Option<usize>,
    key: Option<&'a str>,
    index: Option<usize>,
    depth: usize,
) -> Result<usize> {
    // Fixed descriptor schema has a small constant tree. Guards also contain an
    // accidental caller violation without accepting arbitrary uploaded graphs.
    if depth > 12 || nodes.len() >= 256 || key.is_some_and(|key| key.len() > 1024) {
        return Err(budget());
    }
    let id = nodes.len();
    nodes.push(Node {
        value,
        parent,
        key,
        index,
        children: Vec::new(),
    });
    match value {
        Value::Object(object) => {
            let mut keys: Vec<_> = object.keys().collect();
            keys.sort();
            for key in keys {
                let child = tree(&object[key], nodes, Some(id), Some(key), None, depth + 1)?;
                nodes[id].children.push(child);
            }
        }
        Value::Array(array) => {
            for (index, value) in array.iter().enumerate() {
                let child = tree(value, nodes, Some(id), None, Some(index), depth + 1)?;
                nodes[id].children.push(child);
            }
        }
        _ => {}
    }
    Ok(id)
}
// The native rendered leaf is always externally addressed, including tiny text.
// Other strings retain the existing canonical metadata inline threshold. Checking
// the full fixed path avoids treating descriptor data as field authority.
fn scalar_resource(nodes: &[Node<'_>], id: usize) -> bool {
    let node = &nodes[id];
    let Some(text) = node.value.as_str() else {
        return false;
    };
    text.len() > 1024
        || (node.key == Some("renderedText")
            && node.parent.is_some_and(|parent| {
                nodes[parent].key == Some("leaf") && nodes[parent].parent == Some(0)
            }))
}

struct Resources<'a> {
    context: &'a Context,
    start: u64,
    end: u64,
    identity: String,
}
impl Resources<'_> {
    fn reference(&self, node: usize, kind: u64, offset: u64) -> Result<String> {
        sign(
            REF,
            &self.context.key,
            &self.context.binding,
            &[
                self.start,
                self.end,
                u64::try_from(node).map_err(|_| invalid())?,
                kind,
                offset,
            ],
        )
    }
    fn id(&self, node: usize) -> String {
        format!("{}:{node}", self.identity)
    }
    fn entry(&self, nodes: &[Node<'_>], id: usize) -> Result<Value> {
        let node = &nodes[id];
        let mut entry = json!({"id":self.id(id),"parentId":node.parent.map(|n|self.id(n))});
        if let Some(key) = node.key {
            entry["key"] = json!(key);
        }
        if let Some(index) = node.index {
            entry["index"] = json!(index);
        }
        let kind = match node.value {
            Value::Object(_) => "object",
            Value::Array(_) => "array",
            Value::String(_) => "string",
            Value::Number(_) => "number",
            Value::Bool(_) => "boolean",
            Value::Null => "null",
        };
        entry["type"] = json!(kind);
        match node.value {
            Value::Object(_) | Value::Array(_) => {
                entry["childrenRef"] = json!(self.reference(id, 1, 0)?)
            }
            Value::String(_) if scalar_resource(nodes, id) => {
                entry["valueRef"] = json!(self.reference(id, 2, 0)?);
            }
            value => entry["value"] = value.clone(),
        }
        Ok(entry)
    }
}
fn envelope(context: &Context, query: &ReceiptDetailQuery) -> Value {
    json!({"kind":"noteOperationPage","scope":query.scope,
        "operationId":query.operation_id,"headerDigest":query.header_digest,
        "payloadDigest":context.payload,"viewId":context.view,"outputKind":"detail",
        "sourceLength":context.length,"items":[],"nextCursor":null,"expiresAt":context.expires})
}
fn fits(page: &Value, rpc_id: &Value, limit: usize) -> Result<bool> {
    Ok(
        serde_json::to_vec(&json!({"jsonrpc":"2.0","id":rpc_id,"result":page}))
            .map_err(|_| invalid())?
            .len()
            <= limit,
    )
}
fn cursor_binding(context: &Context, query: &ReceiptDetailQuery) -> Result<[u8; 32]> {
    let mut hash = Sha256::new();
    hash.update(context.binding);
    hash.update(serde_json::to_vec(query).map_err(|_| invalid())?);
    Ok(hash.finalize().into())
}

/// Read only references minted under this exact original capture binding. The
/// caller owns authenticated Context construction, snapshot settlement and the
/// final liveness fence. No source/receipt fallback is performed here.
pub(super) fn read(context: &Context, query: &ReceiptDetailQuery, rpc_id: &Value) -> Result<Value> {
    query.validate().map_err(Error::NoteMutation)?;
    if query.kind != ReceiptDetailKind::Detail
        || !query.operation_envelope
        || query.context_envelope
        || query.header_digest.is_none()
        || query.payload_digest.is_some()
        || query.text_id.is_some()
    {
        return Err(invalid());
    }
    let claims = verify(&query.reference, REF, &context.key, &context.binding, 5)?;
    let [start, end, node, kind, position]: [u64; 5] = claims.try_into().map_err(|_| invalid())?;
    if sign(
        REF,
        &context.key,
        &context.binding,
        &[start, end, node, kind, position],
    )? != query.reference
    {
        return Err(invalid());
    }
    let (first, last) = matched(context, start, end)?;
    let capture = context.rendered.as_ref().ok_or_else(invalid)?;
    let whole = json!({"start":capture.source_range.start,"end":capture.source_range.end});
    let logical = json!({"kind":"stagedRenderedHit","mapping":"identity",
        "sourceRange":{"start":start,"end":end},"renderedRange":{"start":first,"end":last},
        "parent":{"ordinal":0,"sourceRange":whole,"descriptor":capture.parent,"attributes":{}},
        "leaf":{"ordinal":1,"sourceRange":whole,"descriptor":capture.leaf,"attributes":{},"renderedText":capture.text}});
    if serde_json::to_vec(&logical).map_err(|_| invalid())?.len() > 65536 {
        return Err(budget());
    }
    let mut nodes = Vec::new();
    tree(&logical, &mut nodes, None, None, None, 0)?;
    let index = usize::try_from(node).map_err(|_| invalid())?;
    let item = nodes.get(index).ok_or_else(invalid)?;
    let resources = Resources {
        context,
        start,
        end,
        identity: hit_id(&context.binding, start, end),
    };
    let mut page = envelope(context, query);
    if kind == 2 {
        if query.cursor.is_some() || !scalar_resource(&nodes, index) {
            return Err(invalid());
        }
        let text = item.value.as_str().ok_or_else(invalid)?;
        // A continuation ref identifies the same whole scalar field and carries
        // its default next position. An explicit seek may override that position;
        // unlike source-hit fragments it is not a position-restricted resource.
        byte_at(text, position)?;
        if position >= units(text) && !text.is_empty() {
            return Err(invalid());
        }
        let offset = query.offset.unwrap_or(position);
        let byte = byte_at(text, offset)?;
        if offset >= units(text) && !text.is_empty() {
            return Err(invalid());
        }
        let mut end_byte = byte;
        for scalar in text[byte..].chars() {
            if end_byte - byte + scalar.len_utf8() > query.max_source_bytes {
                break;
            }
            end_byte += scalar.len_utf8();
        }
        loop {
            let fragment = &text[byte..end_byte];
            if fragment.is_empty() && !text.is_empty() {
                return Err(budget());
            }
            let next = offset + units(fragment);
            let next_ref = if next == units(text) {
                None
            } else {
                Some(resources.reference(index, 2, next)?)
            };
            page["items"] = json!([{"kind":"fragment","id":resources.id(index),"field":item.key.unwrap_or("value"),"offset":offset,"text":fragment,"nextRef":next_ref}]);
            if fits(&page, rpc_id, query.max_wire_bytes)? {
                return Ok(page);
            }
            if end_byte == byte {
                return Err(budget());
            }
            let first = text[byte..end_byte]
                .chars()
                .next()
                .ok_or_else(invalid)?
                .len_utf8();
            if end_byte - byte <= first {
                return Err(budget());
            }
            end_byte = byte + ((end_byte - byte) / 2).max(first);
            while !text.is_char_boundary(end_byte) {
                end_byte -= 1;
            }
        }
    }
    if position != 0
        || query.offset.is_some()
        || (kind != 0 && kind != 1)
        || (kind == 0 && index != 0)
        || (kind == 1 && !item.value.is_object() && !item.value.is_array())
    {
        return Err(invalid());
    }
    let children = if kind == 0 {
        vec![0]
    } else {
        item.children.clone()
    };
    let binding = cursor_binding(context, query)?;
    let at = match &query.cursor {
        None => 0,
        Some(cursor) => {
            let decoded = verify(cursor, CURSOR, &context.key, &binding, 1)?;
            if sign(CURSOR, &context.key, &binding, &decoded)? != *cursor {
                return Err(invalid());
            }
            let at = usize::try_from(decoded[0]).map_err(|_| invalid())?;
            if at == 0 || at >= children.len() {
                return Err(invalid());
            }
            at
        }
    };
    let mut count = (children.len() - at).min(query.max_items);
    loop {
        page["items"] = Value::Array(
            children[at..at + count]
                .iter()
                .map(|id| resources.entry(&nodes, *id))
                .collect::<Result<Vec<_>>>()?,
        );
        let next = at + count;
        page["nextCursor"] = if next == children.len() {
            Value::Null
        } else {
            json!(sign(
                CURSOR,
                &context.key,
                &binding,
                &[u64::try_from(next).map_err(|_| invalid())?]
            )?)
        };
        if fits(&page, rpc_id, query.max_wire_bytes)? {
            return Ok(page);
        }
        if count <= 1 {
            return Err(budget());
        }
        count -= 1;
    }
}

#[cfg(test)]
#[path = "rendered_detail_tests.rs"]
mod tests;
