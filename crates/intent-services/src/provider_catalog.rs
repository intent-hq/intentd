//! `providers.catalog` payload builder (§6.9, monorepo#928).
//!
//! Serves the static `intent-providers` registry over the wire so clients no
//! longer need a local copy of `provider-config.ts`. Every registered
//! provider is served, registry order; the daemon evaluates the gating
//! fields (`requires_env_var` / `requires_feature_code`) into a `visible`
//! boolean while also passing the raw fields through, so clients can either
//! trust the evaluation or re-derive it. The data is static — no cache/TTL.

use serde_json::{json, Value};

/// Build the full `providers.catalog` result:
/// `{ providers: [...], defaultProviderId }`. Gating is evaluated against the
/// daemon's process environment (see [`provider_visible`]).
pub fn build_providers_catalog() -> Value {
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
    json!({
        "providers": providers,
        "defaultProviderId": intent_providers::default_provider_id(),
    })
}

/// Whether a provider passes the daemon-side visibility gate: a configured
/// `requires_env_var` must be present in the environment, and a configured
/// `requires_feature_code` always gates (the daemon stores no feature-code
/// enablement — the same default-deny `models.list` applies to cortex in
/// `model_catalog::cortex_fetch`).
fn provider_visible(p: &intent_providers::ProviderConfig, env_has: &dyn Fn(&str) -> bool) -> bool {
    if p.requires_env_var.is_some_and(|var| !env_has(var)) {
        return false;
    }
    p.requires_feature_code.is_none()
}

/// One catalog row (camelCase on the wire). Optional fields are omitted when
/// unset, never null; `modelTiers` is present only for providers with a
/// static tier table (`PROVIDER_MODEL_TIERS` — dynamic-model providers like
/// opencode/droid/grok are intentionally absent from it).
fn provider_row(p: &intent_providers::ProviderConfig, env_has: &dyn Fn(&str) -> bool) -> Value {
    let mut row = serde_json::Map::new();
    row.insert("id".into(), json!(p.id));
    row.insert("displayName".into(), json!(p.display_name));
    row.insert("shortName".into(), json!(p.short_name));
    row.insert("command".into(), json!(p.command));
    row.insert("isDefault".into(), json!(p.is_default));
    row.insert("canBeDisabled".into(), json!(p.can_be_disabled));
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
    if let Some(tiers) = intent_providers::tiers_for(p.id) {
        row.insert(
            "modelTiers".into(),
            json!({
                "fast": tiers.fast,
                "balanced": tiers.balanced,
                "smart": tiers.smart,
            }),
        );
    }
    Value::Object(row)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn catalog(env_has: &dyn Fn(&str) -> bool) -> Value {
        build_providers_catalog_with_env(env_has)
    }

    #[test]
    fn serves_all_providers_in_registry_order_with_default_id() {
        let v = catalog(&|_| false);
        let ids: Vec<&str> = v["providers"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p["id"].as_str().unwrap())
            .collect();
        let expected: Vec<&str> = intent_providers::all_provider_ids();
        assert_eq!(ids, expected, "one row per registry entry, same order");
        assert_eq!(
            v["defaultProviderId"].as_str().unwrap(),
            intent_providers::default_provider_id()
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
        assert_eq!(auggie["isDefault"], true);
        assert_eq!(auggie["canBeDisabled"], true);
        assert_eq!(auggie["loginCommandHint"], "auggie login");
        assert!(auggie["authErrorPatterns"].is_array());
        // Unset optionals are omitted, never null.
        assert!(auggie.get("requiresEnvVar").is_none());
        assert!(auggie.get("requiresFeatureCode").is_none());
        assert!(auggie.get("loginDocsUrl").is_none());
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
        // always gates (default-deny, same as models.list for cortex).
        let v = catalog(&|_| true);
        let cortex = row(&v, "cortex");
        assert_eq!(cortex["visible"], false);
        assert_eq!(cortex["requiresFeatureCode"], "cortex");
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
            "droid",
            "grok",
        ] {
            assert_eq!(row(&v, id)["visible"], true, "{id} should be visible");
        }
    }

    #[test]
    fn model_tiers_present_only_for_static_tier_providers() {
        let v = catalog(&|_| false);
        for id in ["auggie", "claude-code", "codex", "cortex"] {
            let tiers = &row(&v, id)["modelTiers"];
            assert!(tiers.is_object(), "{id} should carry modelTiers");
            for tier in ["fast", "balanced", "smart"] {
                assert!(tiers[tier].is_string(), "{id} modelTiers.{tier}");
            }
        }
        for id in ["opencode", "unsloth", "pi", "droid", "grok", "mock"] {
            assert!(
                row(&v, id).get("modelTiers").is_none(),
                "{id} (dynamic models) must omit modelTiers"
            );
        }
    }

    #[test]
    fn tier_values_match_registry_table() {
        let v = catalog(&|_| false);
        let auggie_tiers = &row(&v, "auggie")["modelTiers"];
        let expected = intent_providers::tiers_for("auggie").unwrap();
        assert_eq!(auggie_tiers["fast"], expected.fast);
        assert_eq!(auggie_tiers["balanced"], expected.balanced);
        assert_eq!(auggie_tiers["smart"], expected.smart);
    }
}
