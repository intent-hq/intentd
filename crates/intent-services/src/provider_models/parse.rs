//! Pure parsers turning adapter payloads into PROTOCOL §5.30 wire rows
//! `{ id, name, provider, description? }`.
//!
//! Ports the FE payload normalization: the model list may live under
//! `models.availableModels`, `availableModels`, `models.available`, a bare
//! `models` array, or a `configOptions` select option with `id == "model"`
//! (claude-agent-acp ≥ 0.60 / codex-acp ≥ 0.16 report the catalog this way),
//! and the payload may be wrapped under `update` / `sessionUpdate`
//! (session-update notifications). Effort-capable models carry the PROTOCOL
//! §5.30 `effortLevels` list on a single base row — codex from adapter evidence
//! and collapsed variants, other providers from adapter-advertised per-model
//! `supportedEffortLevels` / `effortLevels`, falling back to the session's
//! global `thought_level` select. The adapter's `default` pseudo-row is
//! resolved to the real model it stands for — that row is marked
//! `isDefault: true` and the pseudo-row is dropped; an unresolvable
//! pseudo-row is kept as-is (fail-soft).

use serde_json::{json, Map, Value};

use crate::usage_stats;

/// Reasoning effort levels used by codex model variants.
const CODEX_EFFORTS: [&str; 6] = ["low", "medium", "high", "xhigh", "max", "ultra"];

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

/// Reasoning-effort levels advertised for the whole ACP session by the first
/// `thought_level` select. This mirrors live-session discovery and drops the
/// provider-default sentinel, which is a clear-selection affordance rather
/// than a real effort level.
fn extract_thought_level_values(payload: &Value) -> Option<Vec<String>> {
    let update = payload
        .get("update")
        .or_else(|| payload.get("sessionUpdate"))
        .unwrap_or(payload);
    let option = update
        .get("configOptions")
        .and_then(Value::as_array)?
        .iter()
        .find(|option| {
            option.get("category").and_then(Value::as_str) == Some("thought_level")
                && option.get("type").and_then(Value::as_str) == Some("select")
        })?;
    let values: Vec<String> = option
        .get("options")
        .and_then(Value::as_array)?
        .iter()
        .flat_map(|entry| {
            entry.get("options").and_then(Value::as_array).map_or_else(
                || std::slice::from_ref(entry).iter(),
                |options| options.iter(),
            )
        })
        .filter_map(|entry| entry.get("value").and_then(Value::as_str))
        .map(str::trim)
        .filter(|value| !value.is_empty() && !value.eq_ignore_ascii_case("default"))
        .map(String::from)
        .collect();
    (!values.is_empty()).then_some(values)
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

/// Adapter-advertised reasoning-effort levels for one raw model entry.
/// `supportedEffortLevels` (as reported by claude-agent-acp) wins over the
/// wire-shaped `effortLevels` alias when both are populated. Non-string and
/// blank entries are dropped; absent or empty lists yield `None`.
fn entry_effort_levels(entry: &Value) -> Option<Vec<String>> {
    ["supportedEffortLevels", "effortLevels"]
        .iter()
        .find_map(|key| {
            let levels: Vec<String> = entry
                .get(*key)
                .and_then(Value::as_array)?
                .iter()
                .filter_map(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect();
            (!levels.is_empty()).then_some(levels)
        })
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

fn codex_effort(value: &str) -> Option<&'static str> {
    CODEX_EFFORTS
        .iter()
        .copied()
        .find(|effort| value.eq_ignore_ascii_case(effort))
        .or_else(|| value.eq_ignore_ascii_case("none").then_some("none"))
}

fn strip_parenthesized_effort(value: &str) -> (String, Option<&'static str>) {
    let trimmed = value.trim();
    let Some(open) = trimmed.rfind('(') else {
        return (trimmed.to_string(), None);
    };
    let Some(level) = trimmed
        .strip_suffix(')')
        .and_then(|without_close| without_close.get(open + 1..))
        .and_then(codex_effort)
    else {
        return (trimmed.to_string(), None);
    };
    let base = trimmed[..open].trim_end();
    if base.is_empty() {
        return (trimmed.to_string(), None);
    }
    (base.to_string(), Some(level))
}

fn strip_codex_id_effort(
    id: &str,
    name_effort: Option<&'static str>,
) -> (String, Option<&'static str>) {
    if let Some(open) = id.rfind('[') {
        if let Some(level) = id
            .strip_suffix(']')
            .and_then(|without_close| without_close.get(open + 1..))
            .and_then(codex_effort)
        {
            let base = id[..open].trim_end();
            if !base.is_empty() {
                return (base.to_string(), Some(level));
            }
        }
    }

    let (base, parenthesized) = strip_parenthesized_effort(id);
    if parenthesized.is_some() {
        return (base, parenthesized);
    }

    let lower = id.to_ascii_lowercase();
    for effort in CODEX_EFFORTS {
        for separator in ['/', ':'] {
            let suffix = format!("{separator}{effort}");
            if lower.ends_with(&suffix) && id.len() > suffix.len() {
                return (id[..id.len() - suffix.len()].to_string(), Some(effort));
            }
        }
    }

    // A hyphen/underscore suffix is ambiguous with real model names such as
    // `gpt-5.1-codex-max`; only strip it when the display name confirms that
    // the row is an effort variant.
    if let Some(effort) = name_effort {
        for separator in ['-', '_'] {
            let suffix = format!("{separator}{effort}");
            if lower.ends_with(&suffix) && id.len() > suffix.len() {
                return (id[..id.len() - suffix.len()].to_string(), Some(effort));
            }
        }
    }
    (id.to_string(), None)
}

fn push_unique(values: &mut Vec<String>, additions: impl IntoIterator<Item = String>) {
    for value in additions {
        if !values.contains(&value) {
            values.push(value);
        }
    }
}

struct CodexModelGroup {
    id: String,
    name: String,
    description: Option<String>,
    adapter_levels: Vec<String>,
    inferred_levels: Vec<String>,
}

/// Parse an ACP payload (session/new result or session-update notification)
/// into wire rows for `provider`. Used by claude-code, pi, and droid. Models
/// carry per-model effort metadata when present, else levels from the session's
/// global `thought_level` select. The catalog's default is resolved via
/// [`resolve_default_row`]: the real default row is marked `isDefault: true`
/// and the adapter's `default` pseudo-row is dropped whenever a real row
/// exists (kept only when it is the catalog's sole row).
pub(super) fn parse_acp_models(payload: &Value, provider: &str) -> Vec<Value> {
    let Some(candidates) = extract_available_models(payload) else {
        return Vec::new();
    };
    let session_effort_levels = extract_thought_level_values(payload);
    let mut rows: Vec<Value> = candidates
        .iter()
        .filter_map(|entry| {
            let (id, name, desc) = entry_fields(entry)?;
            Some(with_effort_levels(
                wire_row(&id, &name, provider, desc.as_deref()),
                entry_effort_levels(entry).or_else(|| session_effort_levels.clone()),
            ))
        })
        .collect();
    resolve_default_row(&mut rows, extract_model_current_value(payload));
    rows
}

/// The model select's `currentValue` from the probed payload's
/// `configOptions`: the same select [`extract_config_options_models`] reads
/// the catalog from (`id == "model"` with non-empty options, falling back to
/// `category == "model"`) — and the same select the D13 effective-model
/// resolution in `agent_session` uses. `None` when absent or blank.
fn extract_model_current_value(payload: &Value) -> Option<&str> {
    let update = payload
        .get("update")
        .or_else(|| payload.get("sessionUpdate"))
        .unwrap_or(payload);
    let options = update.get("configOptions").and_then(Value::as_array)?;
    let by_key = |key: &str| {
        options
            .iter()
            .filter(|o| o.get(key).and_then(Value::as_str) == Some("model"))
            .find(|o| {
                o.get("options")
                    .and_then(Value::as_array)
                    .is_some_and(|a| !a.is_empty())
            })
    };
    by_key("id")
        .or_else(|| by_key("category"))?
        .get("currentValue")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
}

/// Whether a wire row is the adapter's `default` pseudo-row (id `"default"`,
/// case-insensitive) — a stand-in for a real sibling model, not a model.
/// Crate-visible so the model-catalog cache can drop stale persisted
/// pseudo-rows on load (entries fetched by a pre-resolution daemon).
pub(crate) fn is_default_pseudo_row(row: &Value) -> bool {
    row.get("id")
        .and_then(Value::as_str)
        .is_some_and(|id| id.eq_ignore_ascii_case("default"))
}

/// A wire row's version-bearing model-family display, from its name then
/// description (e.g. name "Default (recommended)" is family-less, description
/// "Opus 4.8 with 1M context · …" → `"Opus 4.8"`). Same heuristic as the D13
/// effective-model resolution.
fn row_family_display(row: &Value) -> Option<String> {
    usage_stats::version_bearing_display(
        ["name", "description"]
            .into_iter()
            .filter_map(|key| row.get(key).and_then(Value::as_str)),
    )
}

/// Resolve which sibling real row the `default` pseudo-row at `pseudo` stands
/// for: the row whose version-bearing family display matches the pseudo-row's
/// own. `None` unless exactly one sibling matches — an ambiguous family match
/// must not guess a default.
fn resolve_pseudo_default_sibling(rows: &[Value], pseudo: usize) -> Option<usize> {
    let family = row_family_display(&rows[pseudo])?;
    let mut matches = rows.iter().enumerate().filter(|(index, row)| {
        *index != pseudo && row_family_display(row).as_deref() == Some(family.as_str())
    });
    let (index, _) = matches.next()?;
    matches.next().is_none().then_some(index)
}

/// Resolve the catalog's default row: mark the real default `isDefault: true`
/// and drop the adapter's `default` pseudo-row. The real default is the model
/// select's `currentValue` when it names a real (non-`default`) row; else the
/// pseudo-row is resolved to the unique sibling sharing its version-bearing
/// model family ([`resolve_pseudo_default_sibling`]). The pseudo-row is
/// dropped **unconditionally** whenever at least one real row exists —
/// resolved or not; when the resolution fails nothing is marked `isDefault`
/// (no guessing). The single exception: a catalog whose only row is the
/// pseudo-row keeps it, so this logic never empties a catalog.
fn resolve_default_row(rows: &mut Vec<Value>, current_value: Option<&str>) {
    let pseudo = rows.iter().position(is_default_pseudo_row);
    let target = current_value
        .filter(|value| !value.eq_ignore_ascii_case("default"))
        .and_then(|value| {
            rows.iter()
                .position(|row| row.get("id").and_then(Value::as_str) == Some(value))
        })
        .or_else(|| pseudo.and_then(|pseudo| resolve_pseudo_default_sibling(rows, pseudo)));
    if let Some(target) = target {
        if let Some(row) = rows[target].as_object_mut() {
            row.insert("isDefault".to_string(), json!(true));
        }
    }
    if let Some(pseudo) = pseudo.filter(|_| rows.len() > 1) {
        rows.remove(pseudo);
    }
}

/// Codex variant of [`parse_acp_models`]: adapter-expanded effort variants are
/// grouped into one base row. Adapter-advertised levels are merged with levels
/// inferred from variant ids/names.
pub(super) fn parse_codex_acp_models(payload: &Value) -> Vec<Value> {
    let Some(candidates) = extract_available_models(payload) else {
        return Vec::new();
    };
    let mut groups: Vec<CodexModelGroup> = Vec::new();
    for entry in candidates {
        let Some((raw_id, raw_name, description)) = entry_fields(entry) else {
            continue;
        };
        let (name, name_effort) = strip_parenthesized_effort(&raw_name);
        let (id, id_effort) = strip_codex_id_effort(&raw_id, name_effort);
        let inferred_effort = id_effort.or(name_effort);
        let name = if name == raw_name && raw_name == raw_id && id != raw_id {
            id.clone()
        } else {
            name
        };

        let index = groups
            .iter()
            .position(|group| group.id.eq_ignore_ascii_case(&id));
        let group = if let Some(index) = index {
            &mut groups[index]
        } else {
            groups.push(CodexModelGroup {
                id,
                name,
                description: None,
                adapter_levels: Vec::new(),
                inferred_levels: Vec::new(),
            });
            groups.last_mut().expect("just pushed")
        };
        if inferred_effort.is_none() && group.description.is_none() {
            group.description = description;
        }
        if let Some(levels) = entry_effort_levels(entry) {
            push_unique(&mut group.adapter_levels, levels);
        }
        if let Some(effort) = inferred_effort {
            push_unique(&mut group.inferred_levels, [effort.to_string()]);
        }
    }

    groups
        .into_iter()
        .map(|group| {
            let levels = if !group.adapter_levels.is_empty() || !group.inferred_levels.is_empty() {
                let mut levels: Vec<String> = CODEX_EFFORTS
                    .iter()
                    .filter(|effort| {
                        group
                            .adapter_levels
                            .iter()
                            .chain(&group.inferred_levels)
                            .any(|level| level.eq_ignore_ascii_case(effort))
                    })
                    .map(std::string::ToString::to_string)
                    .collect();
                push_unique(
                    &mut levels,
                    group
                        .adapter_levels
                        .into_iter()
                        .filter(|level| codex_effort(level).is_none()),
                );
                (!levels.is_empty()).then_some(levels)
            } else {
                None
            };
            with_effort_levels(
                wire_row(
                    &group.id,
                    &group.name,
                    "codex",
                    group.description.as_deref(),
                ),
                levels,
            )
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
/// unsloth catalog's typical quant range): ~0.6 bytes/param, close to `Q4_K_M`.
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
// RAM sizes above 2^53 bytes (8 PiB) do not occur; loss-free in f64.
#[allow(clippy::cast_precision_loss)]
pub(super) fn fits_within_ram(params_billion: f64, total_ram_bytes: u64) -> bool {
    estimate_model_bytes(params_billion) <= (total_ram_bytes as f64) * RAM_FIT_FRACTION
}

/// Whether a model whose weight files total `model_bytes` on disk fits
/// within [`RAM_FIT_FRACTION`] of `total_ram_bytes`, with the same
/// [`FIT_HEADROOM_BYTES`] KV-cache/runtime allowance the catalog's
/// param-count estimate bakes in. Shared with the spawn-time quant-variant
/// selection ([`crate::unsloth_server`]) so both fit checks apply one
/// consistent headroom policy.
// RAM/model sizes above 2^53 bytes (8 PiB) do not occur; loss-free in f64.
#[allow(clippy::cast_precision_loss)]
pub(crate) fn gguf_bytes_fit_within_ram(model_bytes: u64, total_ram_bytes: u64) -> bool {
    (model_bytes as f64) + FIT_HEADROOM_BYTES <= (total_ram_bytes as f64) * RAM_FIT_FRACTION
}

/// Parse the total parameter count (in billions) out of an HF repo id's model
/// name, tolerating both dense names (`27B`) and `MoE` names that also carry an
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
/// (`27B`, `0.8B`, `270M`), skipping `MoE` active-parameter markers (`A3B`).
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
