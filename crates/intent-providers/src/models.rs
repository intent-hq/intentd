//! Model id + capability-tier helpers (§6.9).
//!
//! Ports the model helpers from `provider-config.ts`: compound model ids,
//! `PROVIDER_MODEL_TIERS`, and fuzzy/tier resolution. Providers with dynamic
//! model lists (opencode, droid) are intentionally absent from the tier table.

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
