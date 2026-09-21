//! `providers.catalog` payload builder (monorepo#928; documented in the
//! monorepo `docs/protocol/` as of protocol 2.6).
//!
//! Serves the static `intent-providers` registry over the wire so clients no
//! longer need a local copy of `provider-config.ts`. Every registered
//! provider is served, registry order; the daemon evaluates the gating
//! fields (`requires_env_var` / `requires_feature_code`) into a `visible`
//! boolean while also passing the raw fields through, so clients can either
//! trust the evaluation or re-derive it. The data is static — no cache/TTL.

use serde_json::{json, Value};

/// Build the full `providers.catalog` result: `{ providers: [...] }`. No
/// provider carries a default designation — clients derive an effective
/// default from settings (`model.defaultProvider`).
/// Gating is evaluated against the daemon's process environment (see
/// [`provider_visible`]).
pub(crate) fn build_providers_catalog() -> Value {
    build_providers_catalog_with_env(&|var| std::env::var_os(var).is_some())
}

/// [`build_providers_catalog`] with an injectable env-var presence probe, so
/// unit tests can exercise both sides of the `requires_env_var` gate without
/// mutating the (process-global) real environment.
fn build_providers_catalog_with_env(env_has: &dyn Fn(&str) -> bool) -> Value {
    let providers: Vec<Value> = intent_providers::ACP_PROVIDERS
        .iter()
        .map(|p| provider_row(p, env_has))
        .collect();
    json!({ "providers": providers })
}

/// Whether a provider passes the daemon-side visibility gate. Derived from
/// [`intent_providers::gated_reason_with_env`] — the single env-var/
/// feature-code gate shared with discovery's `gatedOff` and the
/// `models.list` cortex/droid sources — so the surfaces can never drift.
fn provider_visible(p: &intent_providers::ProviderConfig, env_has: &dyn Fn(&str) -> bool) -> bool {
    intent_providers::gated_reason_with_env(p, env_has).is_none()
}

/// One catalog row (camelCase on the wire). Optional fields are omitted when
/// unset, never null. Model discovery is fully dynamic (`models.list`) — the
/// row carries no model metadata.
fn provider_row(p: &intent_providers::ProviderConfig, env_has: &dyn Fn(&str) -> bool) -> Value {
    let mut row = serde_json::Map::new();
    row.insert("id".into(), json!(p.id));
    row.insert("displayName".into(), json!(p.display_name));
    row.insert("shortName".into(), json!(p.short_name));
    row.insert("command".into(), json!(p.command));
    row.insert("canBeDisabled".into(), json!(p.can_be_disabled));
    row.insert("supportsTestPrompt".into(), json!(p.supports_test_prompt));
    if let Some(hint) = p.login_command_hint {
        row.insert("loginCommandHint".into(), json!(hint));
    }
    if let Some(url) = p.login_docs_url {
        row.insert("loginDocsUrl".into(), json!(url));
    }
    if let Some(patterns) = p.auth_error_patterns {
        row.insert("authErrorPatterns".into(), json!(patterns));
    }
    if let Some(var) = p.requires_env_var {
        row.insert("requiresEnvVar".into(), json!(var));
    }
    if let Some(code) = p.requires_feature_code {
        row.insert("requiresFeatureCode".into(), json!(code));
    }
    row.insert("visible".into(), json!(provider_visible(p, env_has)));
    Value::Object(row)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn catalog(env_has: &dyn Fn(&str) -> bool) -> Value {
        build_providers_catalog_with_env(env_has)
    }

    #[test]
    fn serves_all_providers_in_registry_order_without_default_id() {
        let v = catalog(&|_| false);
        let ids: Vec<&str> = v["providers"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p["id"].as_str().unwrap())
            .collect();
        let expected: Vec<&str> = intent_providers::all_provider_ids();
        assert_eq!(ids, expected, "one row per registry entry, same order");
        // No privileged provider: the payload carries no defaultProviderId.
        assert!(
            v.get("defaultProviderId").is_none(),
            "catalog must not carry defaultProviderId"
        );
    }

    #[test]
    fn row_shape_matches_registry_entry() {
        let v = catalog(&|_| false);
        let auggie = &v["providers"][0];
        assert_eq!(auggie["id"], "auggie");
        assert_eq!(auggie["displayName"], "Augment Auggie");
        assert_eq!(auggie["shortName"], "Auggie");
        assert_eq!(auggie["command"], "auggie");
        assert_eq!(auggie["canBeDisabled"], true);
        assert_eq!(auggie["supportsTestPrompt"], true);
        assert_eq!(auggie["loginCommandHint"], "auggie login");
        // The generic provider row's login CTA reads `loginDocsUrl`.
        assert!(auggie["loginDocsUrl"].is_string());
        assert!(auggie["authErrorPatterns"].is_array());
        // Unset optionals are omitted, never null.
        assert!(auggie.get("requiresEnvVar").is_none());
        assert!(auggie.get("requiresFeatureCode").is_none());
        assert!(auggie.get("npxOnlyPackage").is_none());
        // No per-row default designation.
        assert!(
            auggie.get("isDefault").is_none(),
            "rows must not carry isDefault"
        );
    }

    #[test]
    fn short_names_match_fe_config() {
        let v = catalog(&|_| false);
        let by_id = |id: &str| -> String {
            v["providers"]
                .as_array()
                .unwrap()
                .iter()
                .find(|p| p["id"] == id)
                .unwrap()["shortName"]
                .as_str()
                .unwrap()
                .to_string()
        };
        assert_eq!(by_id("auggie"), "Auggie");
        assert_eq!(by_id("claude-code"), "Claude Code");
        assert_eq!(by_id("codex"), "Codex");
        assert_eq!(by_id("cortex"), "Cortex");
        assert_eq!(by_id("opencode"), "OpenCode");
        assert_eq!(by_id("unsloth"), "Unsloth");
        assert_eq!(by_id("pi"), "Pi");
        assert_eq!(by_id("droid"), "Droid");
        assert_eq!(by_id("grok"), "Grok");
        assert_eq!(by_id("mock"), "Mock");
    }

    fn row<'a>(v: &'a Value, id: &str) -> &'a Value {
        v["providers"]
            .as_array()
            .unwrap()
            .iter()
            .find(|p| p["id"] == id)
            .unwrap()
    }

    #[test]
    fn supports_test_prompt_present_on_every_row_and_respects_opt_outs() {
        let v = catalog(&|_| false);
        for p in v["providers"].as_array().unwrap() {
            let expected = p["id"] != "unsloth" && p["id"] != "antigravity";
            assert_eq!(
                p["supportsTestPrompt"], expected,
                "supportsTestPrompt mismatch for {}",
                p["id"]
            );
        }
    }

    #[test]
    fn env_var_gate_evaluates_visible_and_passes_raw_field_through() {
        // mock requires MOCK_AGENT_SCRIPT_PATH: hidden when unset…
        let v = catalog(&|_| false);
        let mock = row(&v, "mock");
        assert_eq!(mock["visible"], false);
        assert_eq!(mock["requiresEnvVar"], "MOCK_AGENT_SCRIPT_PATH");

        // …and visible when the env var is present. The raw field is still
        // passed through so clients can re-derive the gate.
        let v = catalog(&|var| var == "MOCK_AGENT_SCRIPT_PATH");
        let mock = row(&v, "mock");
        assert_eq!(mock["visible"], true);
        assert_eq!(mock["requiresEnvVar"], "MOCK_AGENT_SCRIPT_PATH");
    }

    #[test]
    fn feature_code_gate_always_hides_and_passes_raw_field_through() {
        // The daemon stores no feature-code enablement, so a configured code
        // always gates (default-deny). No registry provider carries a feature
        // code today (cortex was un-gated, monorepo#1902), so the mechanism
        // is covered with a synthetic config.
        let synthetic = intent_providers::ProviderConfig {
            requires_feature_code: Some("test-code"),
            ..*intent_providers::find_provider("auggie").unwrap()
        };
        let r = provider_row(&synthetic, &|_| true);
        assert_eq!(r["visible"], false);
        assert_eq!(r["requiresFeatureCode"], "test-code");
    }

    /// cortex and droid are hidden by default: `visible: false` with the
    /// raw `requiresEnvVar` passed through when the enable vars are unset,
    /// and visible again when each var is present.
    #[test]
    fn cortex_and_droid_are_gated_by_default_and_visible_when_enabled() {
        let v = catalog(&|_| false);
        for (id, var) in [
            ("cortex", "INTENTD_ENABLE_CORTEX"),
            ("droid", "INTENTD_ENABLE_DROID"),
        ] {
            let p = row(&v, id);
            assert_eq!(p["visible"], false, "{id} hidden by default");
            assert_eq!(p["requiresEnvVar"], var);
            assert!(p.get("requiresFeatureCode").is_none());

            let enabled = catalog(&|v| v == var);
            let p = row(&enabled, id);
            assert_eq!(p["visible"], true, "{id} visible when {var} is set");
            assert_eq!(p["requiresEnvVar"], var, "raw field still passed through");
        }
    }

    #[test]
    fn ungated_providers_are_visible() {
        let v = catalog(&|_| false);
        for id in [
            "auggie",
            "claude-code",
            "codex",
            "opencode",
            "unsloth",
            "pi",
            "grok",
        ] {
            assert_eq!(row(&v, id)["visible"], true, "{id} should be visible");
        }
    }

    /// Regression (tier removal): no row carries `modelTiers` or `isDefault`
    /// for any provider — model discovery is fully dynamic and no provider
    /// has a default designation.
    #[test]
    fn no_row_carries_model_tiers_or_is_default() {
        let v = catalog(&|_| false);
        for id in intent_providers::all_provider_ids() {
            assert!(
                row(&v, id).get("modelTiers").is_none(),
                "{id} must omit modelTiers"
            );
            assert!(
                row(&v, id).get("isDefault").is_none(),
                "{id} must omit isDefault"
            );
        }
    }
}
