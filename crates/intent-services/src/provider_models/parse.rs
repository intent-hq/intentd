//! Pure parsers turning adapter payloads into PROTOCOL §5.30 wire rows
//! `{ id, name, provider, description? }`.
//!
//! Ports the FE payload normalization: the model list may live under
//! `models.availableModels`, `availableModels`, `models.available`, a bare
//! `models` array, or a `configOptions` select option with `id == "model"`
//! (claude-agent-acp ≥ 0.60 / codex-acp ≥ 0.16 report the catalog this way),
//! and the payload may be wrapped under `update` / `sessionUpdate`
//! (session-update notifications). Codex additionally expands effort-variant
//! base models into `{model}/{effort}` rows.

use serde_json::{json, Map, Value};

/// Reasoning effort levels codex effort-variant models expand into (parity
/// with `supportedReasoningEfforts` in the FE static catalog).
const CODEX_EFFORTS: [(&str, &str); 4] = [
    ("low", "Faster responses with less deliberation"),
    ("medium", "Balanced speed and reasoning depth"),
    ("high", "Deeper reasoning for complex problems"),
    ("xhigh", "Maximum reasoning depth for the hardest problems"),
];

/// Codex base models that expand into reasoning-effort variants (parity with
/// `EFFORT_VARIANT_MODELS` in the FE).
const CODEX_EFFORT_VARIANT_MODELS: [&str; 3] =
    ["gpt-5.3-codex", "gpt-5.2-codex", "gpt-5.1-codex-max"];

/// Extract the raw model-entry array from any ACP payload shape.
///
/// Accepts the bare update or one wrapped under `update` / `sessionUpdate`,
/// with the list under `models.availableModels`, `availableModels`,
/// `models.available`, a bare `models` array, or the `configOptions` select
/// option whose `id` is `"model"`.
fn extract_available_models(payload: &Value) -> Option<&Vec<Value>> {
    let update = payload
        .get("update")
        .or_else(|| payload.get("sessionUpdate"))
        .unwrap_or(payload);
    let models = update.get("models");
    // Emptiness is filtered per branch so an empty array in one shape (e.g. a
    // transitional adapter emitting `availableModels: []` alongside a
    // populated configOptions catalog) still lets a later shape win.
    let non_empty = |a: &&Vec<Value>| !a.is_empty();
    models
        .and_then(|m| m.get("availableModels"))
        .and_then(Value::as_array)
        .filter(non_empty)
        .or_else(|| {
            update
                .get("availableModels")
                .and_then(Value::as_array)
                .filter(non_empty)
        })
        .or_else(|| {
            models
                .and_then(|m| m.get("available"))
                .and_then(Value::as_array)
                .filter(non_empty)
        })
        .or_else(|| models.and_then(Value::as_array).filter(non_empty))
        .or_else(|| extract_config_options_models(update))
}

/// Extract the model rows from a `configOptions` payload: the select option
/// with `id == "model"` (falling back to `category == "model"`) carries the
/// catalog as `options: [{ value, name, description? }]`. Sibling select
/// options (`mode`, `effort`, `fast`, …) are ignored. Values are preserved
/// verbatim as model ids (including effort-suffixed ids like `opus[1m]`),
/// matching the retired FE probe's behavior.
///
/// Each candidate must carry a non-empty `options` array: an `id == "model"`
/// entry without usable options falls through to a `category == "model"`
/// sibling instead of aborting the extraction.
fn extract_config_options_models(update: &Value) -> Option<&Vec<Value>> {
    let options = update.get("configOptions").and_then(Value::as_array)?;
    let by_key = |key: &str| {
        options
            .iter()
            .filter(|o| o.get(key).and_then(Value::as_str) == Some("model"))
            .find_map(|o| {
                o.get("options")
                    .and_then(Value::as_array)
                    .filter(|a| !a.is_empty())
            })
    };
    by_key("id").or_else(|| by_key("category"))
}

/// Pull `(id, name, description)` out of one raw model entry, tolerating the
/// field aliases the adapters use (`modelId`/`id`/`value`,
/// `name`/`displayName`/`label`).
fn entry_fields(entry: &Value) -> Option<(String, String, Option<String>)> {
    let id = ["modelId", "id", "value"]
        .iter()
        .find_map(|k| entry.get(*k).and_then(Value::as_str))
        .map(str::trim)
        .filter(|s| !s.is_empty())?
        .to_string();
    let name = ["name", "displayName", "label"]
        .iter()
        .find_map(|k| entry.get(*k).and_then(Value::as_str))
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or(&id)
        .to_string();
    let description = entry
        .get("description")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(String::from);
    Some((id, name, description))
}

/// Build one wire row `{ id, name, provider, description? }`.
fn wire_row(id: &str, name: &str, provider: &str, description: Option<&str>) -> Value {
    let mut row = Map::new();
    row.insert("id".to_string(), json!(id));
    row.insert("name".to_string(), json!(name));
    row.insert("provider".to_string(), json!(provider));
    if let Some(desc) = description {
        row.insert("description".to_string(), json!(desc));
    }
    Value::Object(row)
}

/// Parse an ACP payload (session/new result or session-update notification)
/// into wire rows for `provider`. Used by claude-code, pi, and droid.
pub(super) fn parse_acp_models(payload: &Value, provider: &str) -> Vec<Value> {
    let Some(candidates) = extract_available_models(payload) else {
        return Vec::new();
    };
    candidates
        .iter()
        .filter_map(entry_fields)
        .map(|(id, name, desc)| wire_row(&id, &name, provider, desc.as_deref()))
        .collect()
}

/// Codex variant of [`parse_acp_models`]: base models in
/// [`CODEX_EFFORT_VARIANT_MODELS`] expand into `{model}/{effort}` rows with
/// effort-suffixed names (parity with the FE `parseModelsFromAcpResponse`).
pub(super) fn parse_codex_acp_models(payload: &Value) -> Vec<Value> {
    let Some(candidates) = extract_available_models(payload) else {
        return Vec::new();
    };
    let mut rows = Vec::new();
    for (id, name, desc) in candidates.iter().filter_map(entry_fields) {
        if CODEX_EFFORT_VARIANT_MODELS.contains(&id.as_str()) {
            for (effort, effort_desc) in CODEX_EFFORTS {
                let effort_label = capitalize(effort);
                let description = match &desc {
                    Some(d) => format!("{d} — {}", effort_desc.to_lowercase()),
                    None => effort_desc.to_string(),
                };
                rows.push(wire_row(
                    &format!("{id}/{effort}"),
                    &format!("{name} ({effort_label})"),
                    "codex",
                    Some(&description),
                ));
            }
        } else {
            rows.push(wire_row(&id, &name, "codex", desc.as_deref()));
        }
    }
    rows
}

fn capitalize(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
        None => String::new(),
    }
}

/// Parse `opencode models` stdout: one `provider/model` per line; lines
/// without a `/` (log noise, headers) are skipped. Display names follow the
/// FE's `formatModelLabel` ("Provider Model Name").
pub(super) fn parse_opencode_models(stdout: &str) -> Vec<Value> {
    let mut rows = Vec::new();
    for line in stdout.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || !trimmed.contains('/') {
            continue;
        }
        let (model_provider, model_id) = trimmed.split_once('/').expect("checked contains '/'");
        if model_provider.is_empty() || model_id.is_empty() {
            continue;
        }
        let name = format!(
            "{} {}",
            capitalize(model_provider),
            title_case_model(model_id)
        );
        rows.push(wire_row(trimmed, &name, "opencode", None));
    }
    rows
}

/// Map parsed [`intent_providers::GrokModel`] rows onto the PROTOCOL §5.30
/// wire shape (the id/name/description work is already done by the shared
/// grok parser in `intent-providers`).
pub(super) fn grok_wire_rows(models: &[intent_providers::GrokModel]) -> Vec<Value> {
    models
        .iter()
        .map(|m| wire_row(&m.model_id, &m.name, "grok", m.description.as_deref()))
        .collect()
}

/// FE `formatModelLabel` parity: hyphens become spaces and each word is
/// capitalized (`claude-sonnet-4` → `Claude Sonnet 4`).
fn title_case_model(model_id: &str) -> String {
    model_id
        .replace('-', " ")
        .split_whitespace()
        .map(capitalize)
        .collect::<Vec<_>>()
        .join(" ")
}

/// Whether a JSON-RPC error from an adapter signals "authentication
/// required" (parity with the FE droid probe's `isAuthRequiredError`).
pub(super) fn is_auth_required_error(code: i64, message: &str) -> bool {
    if code == 401 {
        return true;
    }
    let lower = message.to_lowercase();
    let normalized: String = lower
        .chars()
        .map(|c| if c == '_' || c == '-' { ' ' } else { c })
        .collect();
    normalized.contains("auth required")
        || normalized.contains("authentication required")
        || normalized.contains("not logged in")
        || normalized.contains("not authenticated")
        || normalized.contains("unauthorized")
        || normalized.contains("please login")
        || normalized.contains("please log in")
        || normalized.contains("please signin")
        || normalized.contains("please sign in")
}
