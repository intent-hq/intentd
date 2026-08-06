//! Pure parsers turning adapter payloads into PROTOCOL §5.30 wire rows
//! `{ id, name, provider, description? }`.
//!
//! Ports the FE payload normalization: the model list may live under
//! `models.availableModels`, `availableModels`, `models.available`, a bare
//! `models` array, or a `configOptions` select option with `id == "model"`
//! (claude-agent-acp ≥ 0.60 / codex-acp ≥ 0.16 report the catalog this way),
//! and the payload may be wrapped under `update` / `sessionUpdate`
//! (session-update notifications). Effort-capable models carry the PROTOCOL
//! §5.30 `effortLevels` list on a single base row — codex from its known
//! level set, other providers from the adapter-advertised
//! `supportedEffortLevels`.

use serde_json::{json, Map, Value};

/// Reasoning effort levels codex effort-capable models advertise (parity with
/// `supportedReasoningEfforts` in the FE static catalog).
const CODEX_EFFORTS: [&str; 4] = ["low", "medium", "high", "xhigh"];

/// Codex base models that support reasoning effort (parity with
/// `EFFORT_CAPABLE_MODELS` in the FE).
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

/// Adapter-advertised reasoning-effort levels for one raw model entry
/// (`supportedEffortLevels`, as reported by claude-agent-acp). Non-string and
/// blank entries are dropped; an absent or empty list yields `None`.
fn entry_effort_levels(entry: &Value) -> Option<Vec<String>> {
    let levels: Vec<String> = entry
        .get("supportedEffortLevels")
        .and_then(Value::as_array)?
        .iter()
        .filter_map(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect();
    (!levels.is_empty()).then_some(levels)
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

/// Attach the PROTOCOL §5.30 `effortLevels` list to a wire row. Omitted when
/// the model advertises no levels, so effort-less models stay unchanged.
fn with_effort_levels(mut row: Value, levels: Option<Vec<String>>) -> Value {
    if let (Some(levels), Some(obj)) = (levels, row.as_object_mut()) {
        obj.insert("effortLevels".to_string(), json!(levels));
    }
    row
}

/// Parse an ACP payload (session/new result or session-update notification)
/// into wire rows for `provider`. Used by claude-code, pi, and droid. Models
/// advertising `supportedEffortLevels` carry them as `effortLevels`.
pub(super) fn parse_acp_models(payload: &Value, provider: &str) -> Vec<Value> {
    let Some(candidates) = extract_available_models(payload) else {
        return Vec::new();
    };
    candidates
        .iter()
        .filter_map(|entry| {
            let (id, name, desc) = entry_fields(entry)?;
            Some(with_effort_levels(
                wire_row(&id, &name, provider, desc.as_deref()),
                entry_effort_levels(entry),
            ))
        })
        .collect()
}

/// Codex variant of [`parse_acp_models`]: one base row per model, with the
/// models in [`CODEX_EFFORT_VARIANT_MODELS`] carrying [`CODEX_EFFORTS`] as
/// `effortLevels` instead of expanding into `{model}/{effort}` rows —
/// reasoning effort is a first-class session field (PROTOCOL §5.2/§5.30).
pub(super) fn parse_codex_acp_models(payload: &Value) -> Vec<Value> {
    let Some(candidates) = extract_available_models(payload) else {
        return Vec::new();
    };
    candidates
        .iter()
        .filter_map(|entry| {
            let (id, name, desc) = entry_fields(entry)?;
            let levels = entry_effort_levels(entry).or_else(|| {
                CODEX_EFFORT_VARIANT_MODELS
                    .contains(&id.as_str())
                    .then(|| CODEX_EFFORTS.iter().map(|s| s.to_string()).collect())
            });
            Some(with_effort_levels(
                wire_row(&id, &name, "codex", desc.as_deref()),
                levels,
            ))
        })
        .collect()
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

/// One public (non-private) repo entry from the Hugging Face `models` list
/// API, trimmed to the fields the unsloth catalog source needs.
pub(super) struct HfRepo {
    /// Full HF repo id, e.g. `unsloth/Qwen3.6-35B-A3B-GGUF`.
    pub id: String,
    pub downloads: u64,
    pub trending_score: f64,
}

/// Parse the raw JSON array returned by
/// `https://huggingface.co/api/models?author=unsloth&filter=gguf&limit=1000`
/// into [`HfRepo`] rows. Private repos are dropped (the API should never
/// return them for an unauthenticated request, but the check is cheap
/// insurance); entries missing a non-empty `id` are skipped. Malformed JSON
/// (not an array) yields an empty list rather than an error — the caller
/// treats an empty result as "no models reported".
pub(super) fn parse_hf_unsloth_response(body: &str) -> Vec<HfRepo> {
    let Ok(value) = serde_json::from_str::<Value>(body) else {
        return Vec::new();
    };
    let Some(entries) = value.as_array() else {
        return Vec::new();
    };
    entries
        .iter()
        .filter(|entry| entry.get("private").and_then(Value::as_bool) != Some(true))
        .filter_map(|entry| {
            let id = entry
                .get("id")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())?
                .to_string();
            let downloads = entry.get("downloads").and_then(Value::as_u64).unwrap_or(0);
            let trending_score = entry
                .get("trendingScore")
                .and_then(Value::as_f64)
                .unwrap_or(0.0);
            Some(HfRepo {
                id,
                downloads,
                trending_score,
            })
        })
        .collect()
}

/// Bytes-per-parameter estimate for a Q4-class GGUF quant (the middle of the
/// unsloth catalog's typical quant range): ~0.6 bytes/param, close to Q4_K_M.
const BYTES_PER_PARAM_Q4: f64 = 0.6;

/// Fixed headroom (KV cache, context, runtime overhead) added on top of the
/// raw weights estimate. 1 GiB is a conservative single value across model
/// sizes — exact KV-cache cost scales with context length and is not worth
/// modeling precisely here.
const FIT_HEADROOM_BYTES: f64 = 1024.0 * 1024.0 * 1024.0;

/// Fraction of total system RAM a model's estimated footprint must fit
/// within, leaving room for the OS, the daemon, and other agent processes.
const RAM_FIT_FRACTION: f64 = 0.7;

/// Estimate a model's resident-memory footprint in bytes from its total
/// parameter count (in billions): raw Q4-class weights plus a fixed
/// headroom for KV cache / runtime overhead.
pub(super) fn estimate_model_bytes(params_billion: f64) -> f64 {
    params_billion * 1_000_000_000_f64 * BYTES_PER_PARAM_Q4 + FIT_HEADROOM_BYTES
}

/// Whether a model with `params_billion` total parameters is estimated to
/// fit within [`RAM_FIT_FRACTION`] of `total_ram_bytes`.
pub(super) fn fits_within_ram(params_billion: f64, total_ram_bytes: u64) -> bool {
    estimate_model_bytes(params_billion) <= (total_ram_bytes as f64) * RAM_FIT_FRACTION
}

/// Whether a model whose weight files total `model_bytes` on disk fits
/// within [`RAM_FIT_FRACTION`] of `total_ram_bytes`, with the same
/// [`FIT_HEADROOM_BYTES`] KV-cache/runtime allowance the catalog's
/// param-count estimate bakes in. Shared with the spawn-time quant-variant
/// selection ([`crate::unsloth_server`]) so both fit checks apply one
/// consistent headroom policy.
pub(crate) fn gguf_bytes_fit_within_ram(model_bytes: u64, total_ram_bytes: u64) -> bool {
    (model_bytes as f64) + FIT_HEADROOM_BYTES <= (total_ram_bytes as f64) * RAM_FIT_FRACTION
}

/// Parse the total parameter count (in billions) out of an HF repo id's model
/// name, tolerating both dense names (`27B`) and MoE names that also carry an
/// active-parameter suffix (`35B-A3B` — the total is `35B`; `A3B` is the
/// active count and is skipped). Returns `None` when no size token is found
/// (e.g. `grok-2-GGUF`, `Qwen3-Coder-Next-GGUF`) — the catalog treats an
/// unparseable size as unknown and excludes it from the fit-filtered list
/// rather than guessing.
pub(super) fn parse_param_count_billions(repo_id: &str) -> Option<f64> {
    let name = repo_id.rsplit('/').next().unwrap_or(repo_id);
    name.split(|c: char| !c.is_ascii_alphanumeric() && c != '.')
        .filter(|t| !t.is_empty())
        .find_map(parse_size_token)
}

/// Parse one `-`/`_`-delimited name token as a total-parameter size
/// (`27B`, `0.8B`, `270M`), skipping MoE active-parameter markers (`A3B`).
fn parse_size_token(token: &str) -> Option<f64> {
    let lower = token.to_ascii_lowercase();
    let mut chars = lower.chars();
    let unit = match chars.next_back()? {
        'b' => 1_000_000_000_f64,
        'm' => 1_000_000_f64,
        _ => return None,
    };
    let digits: String = chars.collect();
    if digits.is_empty() {
        return None;
    }
    if let Some(rest) = digits.strip_prefix('a') {
        if rest.starts_with(|c: char| c.is_ascii_digit()) {
            // Active-parameter marker (e.g. "a3b" in "35B-A3B"): not the
            // total, so let the dense/total token win instead.
            return None;
        }
    }
    if digits.matches('.').count() > 1 || !digits.chars().all(|c| c.is_ascii_digit() || c == '.') {
        return None;
    }
    let value: f64 = digits.parse().ok()?;
    Some(value * unit / 1_000_000_000_f64)
}

/// Strip a trailing `-GGUF` (case-insensitive) from a bare repo name. The
/// char-boundary guard keeps the byte slice panic-free on (unexpected)
/// non-ASCII repo names from the network.
fn strip_gguf_suffix(name: &str) -> &str {
    if name.len() >= 5
        && name.is_char_boundary(name.len() - 5)
        && name[name.len() - 5..].eq_ignore_ascii_case("-gguf")
    {
        &name[..name.len() - 5]
    } else {
        name
    }
}

/// Display name for an unsloth repo: the bare repo name (author prefix
/// dropped) with the trailing `-GGUF` marker stripped.
fn unsloth_display_name(repo_id: &str) -> String {
    let name = repo_id.rsplit('/').next().unwrap_or(repo_id);
    strip_gguf_suffix(name).to_string()
}

/// Group a non-negative integer's digits with `,` thousands separators
/// (`811019` -> `811,019`).
fn format_thousands(n: u64) -> String {
    let digits = n.to_string();
    let mut grouped = String::with_capacity(digits.len() + digits.len() / 3);
    for (i, ch) in digits.chars().rev().enumerate() {
        if i != 0 && i % 3 == 0 {
            grouped.push(',');
        }
        grouped.push(ch);
    }
    grouped.chars().rev().collect()
}

/// Build one unsloth wire row: `id` is the full HF repo id (never per-quant),
/// `name` is the bare repo name, and `description` reports the HF download
/// count (the ranking signal used to sort the catalog).
fn unsloth_wire_row(repo: &HfRepo) -> Value {
    let name = unsloth_display_name(&repo.id);
    let description = format!("{} downloads", format_thousands(repo.downloads));
    wire_row(&repo.id, &name, "unsloth", Some(&description))
}

/// Build the unsloth catalog's wire rows from parsed HF repos: sorted by
/// downloads (ties broken by `trendingScore`, both descending) and filtered
/// to repos whose estimated footprint fits [`RAM_FIT_FRACTION`] of
/// `total_ram_bytes`. `total_ram_bytes: None` (RAM detection unsupported on
/// this platform) skips the fit filter entirely — every repo is included.
/// Returns the wire rows plus how many repos were hidden by the fit filter
/// (too large, or of unknown/unparseable size).
pub(super) fn build_unsloth_rows(
    repos: &[HfRepo],
    total_ram_bytes: Option<u64>,
) -> (Vec<Value>, usize) {
    let mut sorted: Vec<&HfRepo> = repos.iter().collect();
    sorted.sort_by(|a, b| {
        b.downloads.cmp(&a.downloads).then_with(|| {
            b.trending_score
                .partial_cmp(&a.trending_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
    });

    let Some(total_ram_bytes) = total_ram_bytes else {
        return (sorted.iter().map(|r| unsloth_wire_row(r)).collect(), 0);
    };

    let mut rows = Vec::with_capacity(sorted.len());
    let mut hidden = 0usize;
    for repo in sorted {
        match parse_param_count_billions(&repo.id) {
            Some(params_billion) if fits_within_ram(params_billion, total_ram_bytes) => {
                rows.push(unsloth_wire_row(repo));
            }
            _ => hidden += 1,
        }
    }
    (rows, hidden)
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
