//! intent-providers — provider registry + launch arg/env assembly + model
//! resolution (§3.1, §6.9).
//!
//! Depends on `intent-core` only (§3.2). Provider quirks are data, not code:
//! the [`ProviderConfig`] registry ([`ACP_PROVIDERS`]) ports
//! `provider-config.ts` so adding a provider is a config change. Arg/env
//! assembly and PATH enrichment ([`args`]) are pure helpers (no spawning/IO —
//! that is M3.3). Model id + capability-tier helpers live in [`models`].

pub use intent_core::Result;

pub mod args;
pub mod config;
pub mod models;

pub use args::{
    apply_codex_config_args, build_provider_args, build_provider_env, enhanced_path,
    upsert_codex_config_args, ArgInputs,
};
pub use config::{
    all_provider_ids, always_enabled_providers, auth_error_message, default_provider_config,
    default_provider_id, disableable_providers, find_provider, is_provider_authentication_error,
    provider_config, ProviderConfig, ACP_PROVIDERS,
};
pub use models::{
    create_compound_model_id, default_model_for_provider, fuzzy_match_model_in_pool,
    is_model_valid_for_provider, model_tier_from_model, normalize_model_override,
    parse_codex_reasoning_effort, parse_compound_model_id, resolve_preferred_model, tiers_for,
    ModelTier, ModelTiers, PROVIDER_MODEL_TIERS,
};

#[cfg(test)]
mod tests;
