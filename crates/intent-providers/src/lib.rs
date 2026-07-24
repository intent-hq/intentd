//! intent-providers — provider registry + launch arg/env assembly + model
//! resolution (§3.1, §6.9).
//!
//! Depends on `intent-core` plus `tracing` (§3.2; `tracing` is needed only to
//! WARN when the `INTENTD_ACP_NODE_MAX_OLD_SPACE_MB` override fails to parse).
//! Provider quirks are data, not code:
//! the [`ProviderConfig`] registry ([`ACP_PROVIDERS`]) ports
//! `provider-config.ts` so adding a provider is a config change. Arg/env
//! assembly and PATH enrichment ([`args`]) are pure helpers (no spawning/IO —
//! that is M3.3). Model id + capability-tier helpers live in [`models`].

pub use intent_core::Result;

pub mod args;
pub mod config;
pub mod discover;
pub mod models;

pub use args::{
    apply_codex_config_args, build_provider_args, build_provider_env, enhanced_path,
    upsert_codex_config_args, ArgInputs,
};
pub use config::{
    all_provider_ids, always_enabled_providers, auth_error_message, default_provider_config,
    default_provider_id, disableable_providers, find_provider, is_provider_authentication_error,
    provider_config, InjectionMechanism, ProviderConfig, ProviderRuntime, ACP_PROVIDERS,
    CLAUDE_AGENT_ACP_NODE_REQUIREMENT, CLAUDE_AGENT_ACP_NPX_PACKAGE, CLAUDE_AGENT_ACP_VERSION,
    PI_ACP_NPX_PACKAGE,
};
pub use discover::{
    discover_providers, find_npx, find_provider_binary, probe_npx, resolve_on_path, NpxStatus,
    ProviderAvailability,
};
pub use models::{
    create_compound_model_id, default_model_for_provider, fuzzy_match_model_in_pool,
    is_model_valid_for_provider, model_tier_from_model, normalize_model_override,
    parse_codex_reasoning_effort, parse_compound_model_id, parse_grok_initialize_models,
    parse_grok_initialize_response_from_stdout, parse_grok_models_command_output,
    providers_claiming_model, resolve_preferred_model, tiers_for, GrokModel,
    GrokModelsCommandOutput, GrokParsedModels, ModelTier, ModelTiers, PROVIDER_MODEL_TIERS,
};

#[cfg(test)]
mod tests;
