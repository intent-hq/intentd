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

/// Test-binary-wide guard: export `NODE_DISABLE_COMPILE_CACHE=1` before any
/// test runs. `enhanced_path_dirs()` triggers `intent_core`'s login-shell PATH
/// capture, whose rc files may run node CLIs (nvm/npm, ng completion); those
/// inherit this and skip `module.enableCompileCache()`, which would otherwise
/// leave a `node-compile-cache/` residue at the TMPDIR root.
#[cfg(test)]
#[ctor::ctor(unsafe)]
fn disable_node_compile_cache() {
    std::env::set_var("NODE_DISABLE_COMPILE_CACHE", "1");
}

pub mod args;
pub mod config;
pub mod discover;
pub mod models;
pub mod version_gate;

#[cfg(test)]
pub(crate) use args::upsert_codex_config_args;
pub use args::{
    apply_codex_config_args, build_provider_args, build_provider_env, build_provider_env_for_spawn,
    build_provider_env_with_unsloth, enhanced_path, ArgInputs, UnslothEndpoint, UnslothModelLimit,
};
pub use config::{
    all_provider_ids, auth_error_message, find_provider, first_provider_id,
    is_provider_authentication_error, provider_config, InjectionMechanism, ProviderConfig,
    ACP_PROVIDERS, AUGGIE_CLI_MIN_VERSION, AUGGIE_CLI_REQUIREMENT,
    CLAUDE_AGENT_ACP_NODE_REQUIREMENT, CLAUDE_AGENT_ACP_NPX_PACKAGE, CLAUDE_AGENT_ACP_VERSION,
    PI_ACP_NPX_PACKAGE, PI_CLI_MIN_VERSION, PI_CLI_REQUIREMENT,
};
#[cfg(test)]
pub(crate) use config::{
    always_enabled_providers, disableable_providers, first_provider_config, ProviderRuntime,
};
pub use discover::{
    discover_providers_with_overrides, find_auggie_candidates, find_npx, find_pi_cli,
    find_provider_binary, gated_reason, gated_reason_with_env, not_installed_detail, probe_npx,
    provider_availability_for, resolve_on_path, ProviderAvailability,
};
#[cfg(test)]
pub(crate) use models::{
    create_compound_model_id, fuzzy_match_model_in_pool, is_model_valid_for_provider,
    parse_codex_reasoning_effort, parse_grok_initialize_models,
    parse_grok_initialize_response_from_stdout, resolve_preferred_model,
};
pub use models::{parse_compound_model_id, parse_grok_models_command_output, GrokModel};
pub use version_gate::{
    auggie_cli_gate, auggie_gate_reason, pi_cli_gate, pi_gate_reason, PiCliGate, PiCliProbe,
};

#[cfg(test)]
mod tests;
