//! Catalog list pages for `specialist.list`, `rules.list` and `skill.list`
//! (PROTOCOL §5.11 "Catalog list pages").
//!
//! Every reply is one page, sized so the serialized result stays within
//! [`CATALOG_PAGE_BYTES`] (`server.capabilities.boundedReads`). Without
//! paging params a caller gets the first page in the method's existing
//! shape; `nextToken` continues it.

use serde_json::{json, Map, Value};

use super::{invalid_params, opt_int, opt_str, RpcErr};

/// Default and hard maximum rows per page.
pub(super) const CATALOG_PAGE_MAX_ROWS: usize = 500;

/// Serialized bytes one paged result may use, so the frame stays under the
/// 512,000-byte read budget.
pub(super) const CATALOG_PAGE_BYTES: usize = 480_000;

/// Whether the request opts into paging with `limit` or `nextToken`.
fn paging_requested(params: &Map<String, Value>) -> bool {
    ["limit", "nextToken"]
        .iter()
        .any(|k| params.get(*k).is_some_and(|v| !v.is_null()))
}

/// Versioned cursor scoped to one method and resource.
fn encode_token(scope: &str, offset: usize) -> String {
    json!({ "version": 1, "scope": scope, "offset": offset }).to_string()
}

/// Decode `nextToken` (absent → offset 0). A malformed token or one minted
/// for another method or resource is `-32602`.
fn decode_token(params: &Map<String, Value>, scope: &str) -> Result<usize, RpcErr> {
    let Some(raw) = opt_str(params, "nextToken") else {
        return Ok(0);
    };
    let invalid = || invalid_params(format!("invalid or mismatched nextToken for {scope}"));
    let cursor: Value = serde_json::from_str(&raw).map_err(|_| invalid())?;
    if cursor.get("version").and_then(Value::as_u64) != Some(1)
        || cursor.get("scope").and_then(Value::as_str) != Some(scope)
    {
        return Err(invalid());
    }
    cursor
        .get("offset")
        .and_then(Value::as_u64)
        .and_then(|o| usize::try_from(o).ok())
        .ok_or_else(invalid)
}

/// One page of rows plus the token for the rest (`None` at the end).
#[derive(Debug)]
struct Page {
    rows: Vec<Value>,
    next_token: Option<String>,
}

fn serialized_len(value: &Value) -> usize {
    serde_json::to_vec(value).map_or(0, |bytes| bytes.len())
}

/// Page `rows` by count (`limit`, clamped into `[1, 500]`, default 500) and
/// by `row_budget` serialized bytes. Every page holds at least one row; a row
/// that alone exceeds the budget is passed to `slim` first.
fn page_rows(
    rows: Vec<Value>,
    params: &Map<String, Value>,
    scope: &str,
    row_budget: usize,
    slim: impl Fn(&mut Value),
) -> Result<Page, RpcErr> {
    let start = decode_token(params, scope)?;
    let max = i64::try_from(CATALOG_PAGE_MAX_ROWS).unwrap_or(i64::MAX);
    let limit = usize::try_from(opt_int(params, "limit").unwrap_or(max).clamp(1, max))
        .unwrap_or(CATALOG_PAGE_MAX_ROWS);
    let len = rows.len();
    let start = start.min(len);
    let mut used = 0usize;
    let mut page = Vec::new();
    for mut row in rows.into_iter().skip(start).take(limit) {
        let mut size = serialized_len(&row);
        if size > row_budget {
            slim(&mut row);
            size = serialized_len(&row);
        }
        // One byte for the separating comma.
        if !page.is_empty() && used + size + 1 > row_budget {
            break;
        }
        used += size + 1;
        page.push(row);
    }
    let end = start + page.len();
    Ok(Page {
        rows: page,
        next_token: (end < len).then(|| encode_token(scope, end)),
    })
}

/// Rows budget once the rest of the result (`envelope`, serialized with an
/// empty rows array) is counted.
fn row_budget(envelope: &Value) -> usize {
    CATALOG_PAGE_BYTES.saturating_sub(serialized_len(envelope))
}

/// Replace a row's large text field with `<field>Omitted: true` and its size.
fn omit_body(row: &mut Value, field: &str) {
    if let Some(obj) = row.as_object_mut() {
        if let Some(body) = obj.remove(field) {
            let bytes = body.as_str().map_or(0, str::len);
            obj.insert(format!("{field}Omitted"), Value::Bool(true));
            obj.insert(format!("{field}Bytes"), Value::from(bytes));
        }
    }
}

fn token_value(token: Option<String>) -> Value {
    token.map_or(Value::Null, Value::String)
}

/// `specialist.list` → `{ specialists, importDiagnostics?, nextToken }`;
/// `importDiagnostics` is catalog-wide and rides the first page only.
pub(super) fn page_specialist_list(
    mut listed: Value,
    params: &Map<String, Value>,
) -> Result<Value, RpcErr> {
    let Some(obj) = listed.as_object_mut() else {
        return Ok(listed);
    };
    // A reply without the catalog array is not a catalog; pass it through.
    let Some(Value::Array(rows)) = obj.remove("specialists") else {
        return Ok(listed);
    };
    if params.get("nextToken").is_some_and(|t| !t.is_null()) {
        obj.remove("importDiagnostics");
    }
    obj.insert("specialists".into(), json!([]));
    obj.insert("nextToken".into(), json!("x".repeat(96)));
    let budget = row_budget(&Value::Object(obj.clone()));
    let page = page_rows(rows, params, "specialist.list", budget, |row| {
        omit_body(row, "behaviorPrompt");
        omit_body(row, "prompt");
        if let Some(obj) = row.as_object_mut() {
            if obj.remove("bundled").is_some() {
                obj.insert("bundledOmitted".into(), Value::Bool(true));
            }
        }
    })?;
    obj.insert("specialists".into(), Value::Array(page.rows));
    obj.insert("nextToken".into(), token_value(page.next_token));
    Ok(listed)
}

/// `rules.list` → `{ rules: RuleSet, nextToken }`; the page applies to
/// `rules.rules`.
pub(super) fn page_rules_list(
    mut listed: Value,
    params: &Map<String, Value>,
) -> Result<Value, RpcErr> {
    let scope = format!(
        "rules.list:{}",
        listed
            .pointer("/rules/workspaceId")
            .and_then(Value::as_str)
            .unwrap_or("global")
    );
    let Some(set) = listed.get_mut("rules").and_then(Value::as_object_mut) else {
        return Ok(listed);
    };
    let Some(Value::Array(rows)) = set.remove("rules") else {
        return Ok(listed);
    };
    set.insert("rules".into(), json!([]));
    let envelope = json!({ "rules": Value::Object(set.clone()), "nextToken": "x".repeat(128) });
    let page = page_rows(rows, params, &scope, row_budget(&envelope), |row| {
        omit_body(row, "content");
    })?;
    set.insert("rules".into(), Value::Array(page.rows));
    if let Some(obj) = listed.as_object_mut() {
        obj.insert("nextToken".into(), token_value(page.next_token));
    }
    Ok(listed)
}

/// `skill.list` keeps its bare-array reply (the first page) without paging
/// params; `limit` or `nextToken` selects the `{ skills, nextToken }`
/// envelope, which carries the continuation token.
pub(super) fn page_skill_list(
    listed: Value,
    params: &Map<String, Value>,
    workspace_id: &intent_core::WorkspaceId,
) -> Result<Value, RpcErr> {
    let workspace_id = workspace_id.as_str();
    let paged = paging_requested(params);
    let rows = match listed {
        Value::Array(rows) => rows,
        other => return Ok(other),
    };
    let scope = format!("skill.list:{workspace_id}");
    let envelope = json!({ "skills": [], "nextToken": "x".repeat(128 + workspace_id.len()) });
    let page = page_rows(rows, params, &scope, row_budget(&envelope), |row| {
        omit_body(row, "description");
    })?;
    Ok(if paged {
        json!({ "skills": page.rows, "nextToken": token_value(page.next_token) })
    } else {
        Value::Array(page.rows)
    })
}

/// `skill.list` with `agentId`: always the envelope, paged like
/// [`page_skill_list`], keeping the reply's `stale` / `unavailable`.
pub(super) fn page_agent_skill_list(
    listed: Value,
    params: &Map<String, Value>,
    workspace_id: &intent_core::WorkspaceId,
    agent_id: &intent_core::AgentId,
) -> Result<Value, RpcErr> {
    let Value::Object(mut listed) = listed else {
        return Ok(listed);
    };
    let rows = match listed.remove("skills") {
        Some(Value::Array(rows)) => rows,
        _ => Vec::new(),
    };
    let scope = format!("skill.list:{}:{}", workspace_id.as_str(), agent_id.as_str());
    let envelope = json!({
        "skills": [],
        "nextToken": "x".repeat(128 + scope.len()),
        "stale": true,
        "unavailable": "machine-update-required",
    });
    let page = page_rows(rows, params, &scope, row_budget(&envelope), |row| {
        omit_body(row, "description");
    })?;
    listed.insert("skills".into(), Value::Array(page.rows));
    listed.insert("nextToken".into(), token_value(page.next_token));
    Ok(Value::Object(listed))
}

#[cfg(test)]
mod tests;
