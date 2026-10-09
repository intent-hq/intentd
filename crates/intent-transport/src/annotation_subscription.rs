//! Strict admission and wire framing for the bounded note-state projection.
//!
//! These helpers only inspect persisted scalar state. Subscription ownership,
//! authorization, incarnation binding, coalescing and re-reads belong to the
//! connection forwarder; this module never loads notes or comment collections.
use serde_json::{json, Map, Value};

const MAX_TOKEN_BYTES: usize = 256;
const MAX_PUSH_BYTES: usize = 4096;
const MAX_SAFE_SEQUENCE: u64 = 9_007_199_254_740_991;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PageStateChannel {
    Note,
    Comment,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct PageStateSubscription {
    pub workspace_id: String,
    pub note_id: String,
    pub replace_group: Option<String>,
}

fn invalid() -> String {
    "Invalid pageState subscription".into()
}

fn token(value: Option<&Value>) -> Result<&str, String> {
    value
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty() && text.len() <= MAX_TOKEN_BYTES)
        .ok_or_else(invalid)
}

/// Return `None` only for a projection handled by the existing legacy channel.
/// In particular, a malformed opt-in never falls back to a full snapshot.
pub(crate) fn parse_page_state_subscription(
    channel: PageStateChannel,
    params: &Value,
) -> Result<Option<PageStateSubscription>, String> {
    let params = params.as_object().ok_or_else(invalid)?;
    match params.get("projection") {
        None => return Ok(None),
        Some(Value::Null) if channel == PageStateChannel::Note => return Ok(None),
        Some(Value::String(value))
            if channel == PageStateChannel::Note && matches!(value.as_str(), "full" | "slim") =>
        {
            return Ok(None);
        }
        Some(Value::String(value)) if value == "pageState" => {}
        Some(_) => return Err(invalid()),
    }
    if params.keys().any(|key| {
        !matches!(
            key.as_str(),
            "workspaceId" | "noteId" | "projection" | "replaceGroup"
        )
    }) {
        return Err(invalid());
    }
    Ok(Some(PageStateSubscription {
        workspace_id: token(params.get("workspaceId"))?.into(),
        note_id: token(params.get("noteId"))?.into(),
        replace_group: params
            .get("replaceGroup")
            .map(|value| token(Some(value)).map(str::to_owned))
            .transpose()?,
    }))
}

fn exact_object<'a>(value: &'a Value, fields: &[&str]) -> Result<&'a Map<String, Value>, String> {
    let object = value.as_object().ok_or_else(invalid)?;
    if object.len() != fields.len() || fields.iter().any(|field| !object.contains_key(*field)) {
        return Err(invalid());
    }
    Ok(object)
}

fn validate_state(state: &Value) -> Result<(), String> {
    let state = exact_object(
        state,
        &[
            "kind",
            "scope",
            "stateGeneration",
            "sourceRevision",
            "attributionGeneration",
            "attributionState",
            "commentRevision",
            "deleted",
            "invalidation",
        ],
    )?;
    if state["kind"] != "notePageState"
        || state["invalidation"] != "all"
        || !state["deleted"].is_boolean()
        || !matches!(
            state["attributionState"].as_str(),
            Some("pending" | "ready")
        )
    {
        return Err(invalid());
    }
    let scope = exact_object(
        &state["scope"],
        &["backendId", "workspaceId", "noteId", "noteInstanceId"],
    )?;
    for field in ["backendId", "workspaceId", "noteId", "noteInstanceId"] {
        token(scope.get(field))?;
    }
    for field in ["sourceRevision", "attributionGeneration", "commentRevision"] {
        token(state.get(field))?;
    }
    let generation = token(state.get("stateGeneration"))?;
    let parsed = generation.parse::<u64>().map_err(|_| invalid())?;
    if parsed.to_string() != generation {
        return Err(invalid());
    }
    Ok(())
}

/// Build the entire escaped UTF-8 JSON-RPC push, or refuse it. Validate every
/// scalar and reject extra fields before serialization bounds allocation work.
/// Never truncate the tuple or substitute a legacy collection on failure.
pub(crate) fn build_page_state_push(
    subscription_id: &str,
    seq: u64,
    state: &Value,
) -> Result<String, String> {
    if subscription_id.is_empty()
        || subscription_id.len() > MAX_TOKEN_BYTES
        || seq > MAX_SAFE_SEQUENCE
    {
        return Err(invalid());
    }
    validate_state(state)?;
    let frame = json!({
        "jsonrpc":"2.0", "method":"subscription.push",
        "params": {"subscriptionId":subscription_id, "kind":"snapshot", "seq":seq, "snapshot":state}
    });
    let encoded = serde_json::to_string(&frame).map_err(|_| invalid())?;
    if encoded.len() > MAX_PUSH_BYTES {
        return Err("pageState push exceeds its wire budget".into());
    }
    Ok(encoded)
}

#[cfg(test)]
mod tests;
