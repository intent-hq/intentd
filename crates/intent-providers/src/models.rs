//! Model id + capability-tier helpers (§6.9).
//!
//! Ports the model helpers from `provider-config.ts`: compound model ids,
//! `PROVIDER_MODEL_TIERS`, and fuzzy/tier resolution. Providers with dynamic
//! model lists (opencode, droid, grok) are intentionally absent from the tier
//! table. Grok's dynamic-model parsers (`grok models` stdout and the ACP
//! `initialize` response) are ported from `grok-acp-probe.ts`.

use serde_json::Value;

use crate::config::default_provider_id;

/// Capability tiers for a provider. Mirrors the TS `{ fast, balanced, smart }`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ModelTiers {
    /// Quick, cheap models for background tasks.
    pub fast: &'static str,
    /// General-purpose models for most tasks.
    pub balanced: &'static str,
    /// High-capability models for complex reasoning.
    pub smart: &'static str,
}

/// Model capability tier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelTier {
    /// Quick, cheap tier.
    Fast,
    /// General-purpose tier.
    Balanced,
    /// High-capability tier.
    Smart,
}

impl ModelTier {
    /// All tiers in resolution order (`fast`, `balanced`, `smart`).
    pub const ALL: [ModelTier; 3] = [ModelTier::Fast, ModelTier::Balanced, ModelTier::Smart];

    fn pick(self, tiers: &ModelTiers) -> &'static str {
        match self {
            ModelTier::Fast => tiers.fast,
            ModelTier::Balanced => tiers.balanced,
            ModelTier::Smart => tiers.smart,
        }
    }
}

/// Per-provider capability tiers, in definition order. Port of
/// `PROVIDER_MODEL_TIERS`. opencode/droid are intentionally omitted (dynamic
/// model lists fetched from the CLI at runtime).
pub static PROVIDER_MODEL_TIERS: &[(&str, ModelTiers)] = &[
    (
        "auggie",
        ModelTiers {
            fast: "haiku4.5",
            balanced: "sonnet4.5",
            smart: "opus4.7",
        },
    ),
    (
        "claude-code",
        ModelTiers {
            fast: "haiku",
            balanced: "sonnet",
            smart: "default",
        },
    ),
    (
        "codex",
        ModelTiers {
            fast: "gpt-5.3-codex/medium",
            balanced: "gpt-5.3-codex/high",
            smart: "gpt-5.3-codex/xhigh",
        },
    ),
    (
        "cortex",
        ModelTiers {
            fast: "claude-sonnet-4-5",
            balanced: "claude-opus-4-5",
            smart: "claude-opus-4-5",
        },
    ),
];

/// Tier table for a provider, or `None` for dynamic-model providers.
pub fn tiers_for(provider_id: &str) -> Option<&'static ModelTiers> {
    PROVIDER_MODEL_TIERS
        .iter()
        .find(|(id, _)| *id == provider_id)
        .map(|(_, t)| t)
}

/// Parse a compound model id `{provider}:{model}` into its parts. A bare id
/// (no `:`) belongs to the default provider. Port of `parseCompoundModelId`.
pub fn parse_compound_model_id(compound: &str) -> (String, String) {
    match compound.split_once(':') {
        Some((provider, model)) => (provider.to_string(), model.to_string()),
        None => (default_provider_id().to_string(), compound.to_string()),
    }
}

/// Create a compound model id from provider + model. Port of `createCompoundModelId`.
pub fn create_compound_model_id(provider_id: &str, model_id: &str) -> String {
    format!("{provider_id}:{model_id}")
}

/// Default model for a provider at a tier, falling back to auggie's tier when
/// the provider has no tier mappings. Port of `getDefaultModelForProvider`.
pub fn default_model_for_provider(provider_id: &str, tier: ModelTier) -> &'static str {
    tiers_for(provider_id)
        .or_else(|| tiers_for("auggie"))
        .map(|t| tier.pick(t))
        .expect("auggie tier mappings must exist")
}

/// Reverse-map a concrete model id to its tier, checking the preferred provider
/// first, then all providers. Port of `getModelTierFromModel`.
pub fn model_tier_from_model(
    model_id: &str,
    preferred_provider_id: Option<&str>,
) -> Option<ModelTier> {
    if let Some(provider) = preferred_provider_id {
        if let Some(tiers) = tiers_for(provider) {
            if let Some(t) = ModelTier::ALL.iter().find(|t| t.pick(tiers) == model_id) {
                return Some(*t);
            }
        }
    }
    for (_, tiers) in PROVIDER_MODEL_TIERS {
        if let Some(t) = ModelTier::ALL.iter().find(|t| t.pick(tiers) == model_id) {
            return Some(*t);
        }
    }
    None
}

/// Whether a model (bare or compound) targets `target_provider_id`. Port of
/// `isModelValidForProvider`.
pub fn is_model_valid_for_provider(model: &str, target_provider_id: &str) -> bool {
    parse_compound_model_id(model).0 == target_provider_id
}

/// Normalize a model id for fuzzy comparison: lowercase, strip a leading
/// `claude-` brand prefix, and drop all non-alphanumeric characters.
fn normalize_for_fuzzy_match(id: &str) -> String {
    let lower = id.to_lowercase();
    let stripped = lower.strip_prefix("claude-").unwrap_or(&lower);
    stripped
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect()
}

/// Apply the shared fuzzy-match rules (exact CI, normalized exact, normalized
/// longest-prefix) of `candidate` against `pool`, returning the matched entry.
fn fuzzy_pick<'a>(candidate: &str, pool: &[&'a str]) -> Option<&'a str> {
    let normalized = normalize_for_fuzzy_match(candidate);
    if normalized.is_empty() {
        return None;
    }
    let lower_candidate = candidate.to_lowercase();
    if let Some(m) = pool.iter().find(|m| m.to_lowercase() == lower_candidate) {
        return Some(m);
    }
    if let Some(m) = pool
        .iter()
        .find(|m| normalize_for_fuzzy_match(m) == normalized)
    {
        return Some(m);
    }
    pool.iter()
        .filter(|m| normalize_for_fuzzy_match(m).starts_with(&normalized))
        .max_by_key(|m| normalize_for_fuzzy_match(m).len())
        .copied()
}

/// Normalize a bare/fuzzy model name to the qualified `provider:alias` form for
/// `target_provider_id`, using that provider's tier models as the pool. Returns
/// the qualified compound id, or `None` if no reasonable match. Candidates that
/// already contain `:` are returned unchanged. Port of `normalizeModelOverride`.
pub fn normalize_model_override(candidate: &str, target_provider_id: &str) -> Option<String> {
    if candidate.is_empty() {
        return None;
    }
    if candidate.contains(':') {
        return Some(candidate.to_string());
    }
    let tiers = tiers_for(target_provider_id)?;
    let mut pool: Vec<&str> = Vec::new();
    for v in [tiers.fast, tiers.balanced, tiers.smart] {
        if !pool.contains(&v) {
            pool.push(v);
        }
    }
    fuzzy_pick(candidate, &pool).map(|m| create_compound_model_id(target_provider_id, m))
}

/// Fuzzy-match a candidate against an explicit pool of model ids (typically the
/// provider's live CLI list). Returns the matched bare pool entry. Port of
/// `fuzzyMatchModelInPool`.
pub fn fuzzy_match_model_in_pool(candidate: &str, pool: &[&str]) -> Option<String> {
    if candidate.is_empty() || pool.is_empty() {
        return None;
    }
    fuzzy_pick(candidate, pool).map(|m| m.to_string())
}

/// Parse a Codex model id into its base model and optional reasoning effort.
///
/// A `{base}/{effort}` id (e.g. `gpt-5.3-codex/high`) splits on the first `/`;
/// a bare id has no effort. Port of `parseCodexReasoningEffort`
/// (`open-ai-codex-models.ts`).
pub fn parse_codex_reasoning_effort(model_id: &str) -> (String, Option<String>) {
    match model_id.split_once('/') {
        Some((base, effort)) => (base.to_string(), Some(effort.to_string())),
        None => (model_id.to_string(), None),
    }
}

/// Walk `preference_list` in order and return the first id present in
/// `available_values`. Port of `resolvePreferredModel`.
pub fn resolve_preferred_model(
    preference_list: &[&str],
    available_values: &[&str],
) -> Option<String> {
    preference_list
        .iter()
        .find(|p| available_values.contains(p))
        .map(|p| p.to_string())
}

/// One model advertised by the Grok CLI. Port of `GrokAcpModel`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrokModel {
    /// Model identifier (e.g. `grok-4.5`).
    pub model_id: String,
    /// Human-readable label (derived from the id when the CLI gives none).
    pub name: String,
    /// Optional description (or `agentType · N token context` synthesis).
    pub description: Option<String>,
}

/// Models + current-model extracted from a Grok ACP `initialize`/`session/new`
/// result or a JSON `grok models` payload.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct GrokParsedModels {
    /// Deduplicated model list, in advertised order.
    pub models: Vec<GrokModel>,
    /// The currently-selected model id, when advertised.
    pub current_model_id: Option<String>,
}

/// Parse result of `grok models` stdout. Port of `GrokModelsCommandResult`.
/// `authenticated` is `None` when the output carries no explicit auth marker
/// (the command exits 0 in both auth states, so the exit code is not trusted).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct GrokModelsCommandOutput {
    /// `Some(true)` on a "you are logged in" marker, `Some(false)` on an
    /// explicit logged-out marker, `None` when the output is inconclusive.
    pub authenticated: Option<bool>,
    /// Parsed model list (may be empty when logged out).
    pub models: Vec<GrokModel>,
    /// Model carrying a `(default)` / `(current)` marker, when present.
    pub current_model_id: Option<String>,
}

/// JS-style string coercion for a model field: strings pass through, numbers
/// stringify; other shapes yield `None`.
fn value_to_string(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// Format an integer with `,` thousands separators (JS `toLocaleString`).
fn thousands_separated(n: i64) -> String {
    let digits = n.unsigned_abs().to_string();
    let mut out = String::new();
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    if n < 0 {
        format!("-{out}")
    } else {
        out
    }
}

/// Derive a model description: an explicit `description` wins; otherwise
/// synthesize from `agentType` and the context-window field. Port of
/// `toDescription`.
fn grok_description(model: &Value) -> Option<String> {
    if let Some(desc) = model.get("description").and_then(Value::as_str) {
        let trimmed = desc.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }
    let mut parts: Vec<String> = Vec::new();
    if let Some(agent_type) = model.get("agentType").and_then(Value::as_str) {
        let trimmed = agent_type.trim();
        if !trimmed.is_empty() {
            parts.push(trimmed.to_string());
        }
    }
    let context_window = model
        .get("contextWindow")
        .or_else(|| model.get("contextWindowTokens"))
        .or_else(|| model.get("maxContextTokens"));
    if let Some(n) = context_window.and_then(Value::as_i64) {
        parts.push(format!("{} token context", thousands_separated(n)));
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join(" · "))
    }
}

/// Extract one model from an untyped JSON candidate. Port of `modelFromUnknown`.
fn grok_model_from_value(value: &Value) -> Option<GrokModel> {
    let model_id = value_to_string(value.get("modelId"))
        .or_else(|| value_to_string(value.get("id")))
        .or_else(|| value_to_string(value.get("value")))?
        .trim()
        .to_string();
    if model_id.is_empty() {
        return None;
    }
    // Upstream (`modelFromUnknown`) falls back to the raw id — JSON payloads
    // carry authoritative labels, unlike the text parser which synthesizes
    // one. The extra empty-check guards `name: ""` payloads.
    let name = value_to_string(value.get("name"))
        .or_else(|| value_to_string(value.get("label")))
        .or_else(|| value_to_string(value.get("displayName")))
        .map(|n| n.trim().to_string())
        .filter(|n| !n.is_empty())
        .unwrap_or_else(|| model_id.clone());
    Some(GrokModel {
        description: grok_description(value),
        model_id,
        name,
    })
}

/// Extract a deduplicated model list from the known Grok payload shapes
/// (bare array, `availableModels`, `models`, `data`,
/// `modelState.availableModels`, `models.availableModels`). Port of
/// `modelsFromUnknown`.
fn grok_models_from_value(value: &Value) -> Vec<GrokModel> {
    let candidates: &[Value] = if let Some(arr) = value.as_array() {
        arr
    } else {
        [
            value.get("availableModels"),
            value.get("models"),
            value.get("data"),
            value
                .get("modelState")
                .and_then(|s| s.get("availableModels")),
            value.get("models").and_then(|m| m.get("availableModels")),
        ]
        .iter()
        .find_map(|v| v.and_then(Value::as_array))
        .map(|v| v.as_slice())
        .unwrap_or(&[])
    };

    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut models = Vec::new();
    for candidate in candidates {
        let Some(model) = grok_model_from_value(candidate) else {
            continue;
        };
        if seen.insert(model.model_id.clone()) {
            models.push(model);
        }
    }
    models
}

/// Parse the model list + current model out of a Grok ACP `initialize` (or
/// `session/new`) result. Port of `parseGrokInitializeModels`.
pub fn parse_grok_initialize_models(result: &Value) -> GrokParsedModels {
    // Explicit JSON nulls fall through like the TS nullish-coalescing chain
    // (`res?.modelState ?? res?.models ?? res`).
    let model_state = result
        .get("modelState")
        .filter(|v| !v.is_null())
        .or_else(|| result.get("models").filter(|v| !v.is_null()))
        .unwrap_or(result);
    let current_model_id = model_state
        .get("currentModelId")
        .and_then(Value::as_str)
        .or_else(|| result.get("currentModelId").and_then(Value::as_str))
        .map(|s| s.to_string());
    GrokParsedModels {
        models: grok_models_from_value(model_state),
        current_model_id,
    }
}

/// Find the JSON-RPC response with id `response_id` in raw `grok agent stdio`
/// stdout (tolerating non-JSON preamble lines like update banners) and parse
/// its models. Port of `parseGrokInitializeResponseFromStdout`.
pub fn parse_grok_initialize_response_from_stdout(
    stdout: &str,
    response_id: i64,
) -> Option<GrokParsedModels> {
    for line in stdout.lines() {
        let Ok(msg) = serde_json::from_str::<Value>(line.trim()) else {
            continue;
        };
        // A response has a numeric id and no method; requests/notifications
        // and unrelated ids are skipped, as are error responses.
        if !msg.is_object()
            || msg.get("method").is_some()
            || msg.get("id").and_then(Value::as_i64) != Some(response_id)
            || msg.get("error").is_some()
        {
            continue;
        }
        let result = msg.get("result").cloned().unwrap_or(Value::Null);
        return Some(parse_grok_initialize_models(&result));
    }
    None
}

/// Drop blank lines and `Update available …` banners. Port of
/// `stripGrokPreambleLines`.
fn strip_grok_preamble_lines(stdout: &str) -> Vec<&str> {
    stdout
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.to_lowercase().starts_with("update available"))
        .collect()
}

/// Title-case a model id into a label (`opus-4-6` → `Opus 4 6`). Port of
/// `labelFromModelId`.
fn label_from_model_id(model_id: &str) -> String {
    let spaced: String = model_id
        .chars()
        .map(|c| if c == '-' || c == '_' { ' ' } else { c })
        .collect();
    let mut out = String::with_capacity(spaced.len());
    let mut at_word_start = true;
    for c in spaced.chars() {
        if at_word_start && c.is_ascii_alphanumeric() {
            out.extend(c.to_uppercase());
            at_word_start = false;
        } else {
            out.push(c);
            if !(c.is_ascii_alphanumeric() || c == '_') {
                at_word_start = true;
            }
        }
    }
    out
}

/// Whether a token looks like a model id: `^[a-z][a-z0-9._:/-]*$` (case-
/// insensitive) and containing at least one of `-/:.`. Port of `isModelIdLike`.
fn is_model_id_like(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !first.is_ascii_alphabetic() {
        return false;
    }
    if !chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | ':' | '/' | '-')) {
        return false;
    }
    value.contains(['-', '/', ':', '.'])
}

/// Strip a trailing ` (default)` / ` (current)` marker, reporting whether one
/// was present. Port of `stripTrailingModelMarker`.
fn strip_trailing_model_marker(value: &str) -> (&str, bool) {
    let trimmed_end = value.trim_end();
    for marker in ["(default)", "(current)"] {
        if trimmed_end.len() >= marker.len() {
            let split = trimmed_end.len() - marker.len();
            if trimmed_end.is_char_boundary(split)
                && trimmed_end[split..].eq_ignore_ascii_case(marker)
            {
                let head = trimmed_end[..split].trim_end();
                // The marker must be preceded by whitespace (`\s+\(…\)` upstream).
                if head.len() < split {
                    return (head, true);
                }
            }
        }
    }
    (trimmed_end, false)
}

/// True for header/status lines that must not be parsed as model rows
/// (`logged in`, `authenticated`, `Available models:`, `Model`/`Model ID`
/// column headers, and `---`/`===` rules). Port of the skip pattern in
/// `parseTextModelLine`.
fn is_grok_header_or_status_line(line: &str) -> bool {
    let lower = line.to_lowercase();
    if lower.contains("logged in")
        || lower.contains("authenticated")
        || lower.contains("available model")
    {
        return true;
    }
    // `^model\s*(id)?\b`: "model" followed by a word boundary — directly, or
    // after an optional "id" suffix ("models" has no boundary and passes).
    if let Some(rest) = lower.strip_prefix("model") {
        let boundary = |s: &str| {
            s.chars()
                .next()
                .map_or(true, |c| !(c.is_ascii_alphanumeric() || c == '_'))
        };
        if boundary(rest) || rest.strip_prefix("id").is_some_and(boundary) {
            return true;
        }
    }
    !line.is_empty() && (line.chars().all(|c| c == '-') || line.chars().all(|c| c == '='))
}

/// Parse one text row of `grok models` output into a model (+ whether it
/// carried a current/default marker). Port of `parseTextModelLine`.
fn parse_grok_text_model_line(line: &str) -> Option<(GrokModel, bool)> {
    if is_grok_header_or_status_line(line) {
        return None;
    }
    let cleaned_with_marker =
        line.trim_start_matches(|c: char| c.is_whitespace() || matches!(c, '•' | '*' | '-'));
    let (cleaned, has_current_marker) = strip_trailing_model_marker(cleaned_with_marker);
    if cleaned.is_empty() {
        return None;
    }

    let columns: Vec<&str> = if cleaned.contains('|') {
        cleaned
            .split('|')
            .map(str::trim)
            .filter(|c| !c.is_empty())
            .collect()
    } else {
        split_on_column_gaps(cleaned)
    };
    let (model_id, label, description_parts) = if columns.len() > 1 {
        (columns[0], columns.get(1).copied(), &columns[2..])
    } else {
        let mut ws = cleaned.split_whitespace();
        (ws.next().unwrap_or(""), ws.next(), &[] as &[&str])
    };
    if model_id.is_empty() || !is_model_id_like(model_id) {
        return None;
    }

    let description = if description_parts.is_empty() {
        None
    } else {
        Some(description_parts.join(" "))
    };
    let name = match label {
        Some(l) if l != model_id => l.to_string(),
        _ => label_from_model_id(model_id),
    };
    Some((
        GrokModel {
            model_id: model_id.to_string(),
            name,
            description,
        },
        has_current_marker,
    ))
}

/// Split a row into columns on runs of 2+ spaces or tabs (`\s{2,}|\t+`).
fn split_on_column_gaps(line: &str) -> Vec<&str> {
    let mut columns = Vec::new();
    let mut start = 0;
    let bytes = line.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let is_gap =
            bytes[i] == b'\t' || (bytes[i] == b' ' && i + 1 < bytes.len() && bytes[i + 1] == b' ');
        if is_gap {
            if start < i {
                columns.push(line[start..i].trim());
            }
            while i < bytes.len() && (bytes[i] == b' ' || bytes[i] == b'\t') {
                i += 1;
            }
            start = i;
        } else {
            i += 1;
        }
    }
    if start < bytes.len() {
        columns.push(line[start..].trim());
    }
    columns.retain(|c| !c.is_empty());
    columns
}

/// Parse `grok models` stdout: explicit auth markers, then a JSON payload if
/// one is present, then text rows (with `(default)`/`(current)` markers). The
/// exit code is never trusted — the command exits 0 in both auth states.
/// Port of `parseGrokModelsCommandOutput`.
pub fn parse_grok_models_command_output(stdout: &str) -> GrokModelsCommandOutput {
    let lines = strip_grok_preamble_lines(stdout);
    let text = lines.join("\n");
    let lower = text.to_lowercase();
    let authenticated = if lower.contains("you are logged in") {
        Some(true)
    } else if lower.contains("you are not authenticated")
        || lower.contains("not logged in")
        || lower.contains("login required")
        || lower.contains("please login")
        || lower.contains("please log in")
    {
        Some(false)
    } else {
        None
    };

    // A JSON payload (whole output or one line) that yields a non-empty
    // model list wins over text-row parsing; empty/irrelevant JSON falls
    // through to the text parser (parity with the TS implementation).
    for candidate in std::iter::once(text.as_str()).chain(lines.iter().copied()) {
        let Ok(parsed) = serde_json::from_str::<Value>(candidate) else {
            continue;
        };
        let parsed = parse_grok_initialize_models(&parsed);
        if !parsed.models.is_empty() {
            return GrokModelsCommandOutput {
                authenticated,
                models: parsed.models,
                current_model_id: parsed.current_model_id,
            };
        }
    }

    let mut models = Vec::new();
    let mut current_model_id = None;
    for line in &lines {
        let Some((model, has_current_marker)) = parse_grok_text_model_line(line) else {
            continue;
        };
        if has_current_marker {
            current_model_id = Some(model.model_id.clone());
        }
        models.push(model);
    }
    GrokModelsCommandOutput {
        authenticated,
        models,
        current_model_id,
    }
}
