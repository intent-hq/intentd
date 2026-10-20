//! Treat a provider as installed in a test without touching the real machine.
//!
//! # When to use it
//!
//! Any test that reaches the provider availability gate
//! ([`crate::agent_ops::ensure_provider_available`]: `agent.create`,
//! `agent.delegate`, `agent.setModel`, wake-or-create, …) or spawn resolution
//! (`agent_manager::resolve_spawn`) for an npx-only provider must mark that
//! provider installed with this module. Otherwise the test passes only on a
//! developer machine that happens to have the tools and fails on CI.
//!
//! # Why `providers.paths` overrides do not work
//!
//! Codex and Claude Code are npx-only: they need `npx` (with Node) for the
//! pinned ACP adapter AND the real `codex` / `claude` CLI
//! (`requires_secondary_binary`), and they ignore `providers.paths`
//! overrides (`npx_only_honors_path_override` is false, and the installed
//! CLI is never resolved through an override). CI runners install Node but
//! not those CLIs, so an override such as `{"codex": "/bin/sh"}` looks like a
//! fix, passes locally, and fails on CI (intentd#2462, CI run 39115304490).
//! Providers that do honor overrides (auggie, grok, opencode) can keep using
//! an executable override.
//!
//! # What the marker does and does not do
//!
//! The marker is a `providers.paths` value. Being settings data, it reaches
//! every call site (including the `spawn_blocking` delegate resolver) per
//! `Services` instance, without mutating process-global `PATH`. When a base
//! provider is marked:
//!
//! - the availability gate treats it (and every profile of it) as installed;
//! - spawn resolution uses a placeholder `npx` (pure resolution: nothing is
//!   executed).
//!
//! The disabled gate, the cached-auth gate and feature gating
//! (`requires_env_var` / `requires_feature_code`) still run for real. The
//! auth gate on a marked provider is covered by
//! `provider_profiles::routing_tests::explicit_delegation_checks_the_profiles_own_auth_verdict`,
//! which seeds a per-profile verdict instead of the process-wide one.
//!
//! Only those two seams honor the marker. Discovery surfaces
//! (`host.providerDiscovery`, the discovery cache, default-provider healing)
//! still report the host's real installation: a test driving them must not
//! depend on a provider being installed. Providers that honor
//! `providers.paths` (auggie, grok, opencode, …) are not markable; point them
//! at an executable (e.g. `/bin/sh`) instead.
//!
//! # Guard
//!
//! `scripts/test-without-provider-clis.sh <command>` runs tests with every
//! provider CLI hidden, the way CI sees the machine; see that script.

use std::collections::BTreeMap;

/// The `providers.paths` value that marks a base provider installed.
const MARKER: &str = "intentd-test:runnable";

/// Whether `provider_id` (a base id or a profile reference) belongs to a
/// base provider marked installed in `paths`.
pub(crate) fn is_marked(paths: &BTreeMap<String, String>, provider_id: &str) -> bool {
    paths
        .get(intent_providers::base_provider_id(provider_id))
        .is_some_and(|path| path.trim() == MARKER)
}

/// Mark each base provider installed in a `providers.paths` map.
///
/// # Panics
/// When a name is a profile reference (mark the base provider, which covers
/// all of its profiles), not a registered provider, or a provider that
/// honors `providers.paths` (use an executable override for it).
pub(crate) fn mark(paths: &mut BTreeMap<String, String>, bases: &[&str]) {
    for base in bases {
        assert_eq!(
            intent_providers::base_provider_id(base),
            *base,
            "mark the base provider; it covers every profile of it"
        );
        let provider = intent_providers::find_provider(base)
            .unwrap_or_else(|| panic!("{base} is not a registered provider"));
        assert!(
            provider.npx_only_package.is_some() && !provider.npx_only_honors_path_override,
            "{base} honors providers.paths: point it at an executable instead"
        );
        paths.insert((*base).to_string(), MARKER.to_string());
    }
}

/// A `providers.paths` JSON value marking `bases` installed, for fixtures
/// that write the setting directly.
pub(crate) fn paths_json(bases: &[&str]) -> serde_json::Value {
    let mut paths = BTreeMap::new();
    mark(&mut paths, bases);
    serde_json::json!(paths)
}

/// Mark `bases` installed on a live `Services`, keeping its other
/// `providers.paths` entries.
///
/// # Panics
/// When `svc` has no settings registry or the setting cannot be applied.
pub(crate) fn mark_on(svc: &crate::Services, bases: &[&str]) {
    let mut paths = svc.effective_settings().providers.paths;
    mark(&mut paths, bases);
    svc.settings_registry()
        .expect("settings registry")
        .apply(&[("providers.paths".into(), serde_json::json!(paths))])
        .expect("apply providers.paths");
}

#[cfg(test)]
mod tests {
    use super::*;
    use intent_core::settings_file::ProvidersSettings;

    fn marked(bases: &[&str]) -> ProvidersSettings {
        let mut providers = ProvidersSettings::default();
        mark(&mut providers.paths, bases);
        providers
    }

    #[test]
    fn marks_the_base_and_every_profile_of_it() {
        let providers = marked(&["codex"]);
        assert!(is_marked(&providers.paths, "codex"));
        assert!(is_marked(&providers.paths, "codex@work"));
        assert!(!is_marked(&providers.paths, "claude-code"));
        assert_eq!(
            paths_json(&["codex", "claude-code"]),
            serde_json::json!({ "codex": MARKER, "claude-code": MARKER })
        );
    }

    #[test]
    #[should_panic(expected = "mark the base provider")]
    fn rejects_a_profile_reference() {
        marked(&["codex@work"]);
    }

    #[test]
    #[should_panic(expected = "not a registered provider")]
    fn rejects_an_unknown_provider() {
        marked(&["no-such-provider"]);
    }

    #[test]
    #[should_panic(expected = "honors providers.paths")]
    fn rejects_a_provider_that_honors_path_overrides() {
        marked(&["auggie"]);
    }

    #[test]
    fn a_marked_provider_counts_as_installed() {
        let providers = marked(&["codex", "claude-code"]);
        for id in ["codex", "codex@work", "claude-code"] {
            crate::agent_ops::ensure_provider_available("agent.create", id, &providers)
                .unwrap_or_else(|e| panic!("{id} marked installed: {e}"));
        }
    }

    #[test]
    fn the_disabled_gate_still_applies() {
        let mut providers = marked(&["codex"]);
        providers.enabled = Some(BTreeMap::from([("codex".to_string(), false)]));
        let err = crate::agent_ops::ensure_provider_available("agent.create", "codex", &providers)
            .expect_err("disabled provider rejected");
        assert!(err.to_string().contains("not enabled"), "{err}");
    }

    #[test]
    fn feature_gating_still_applies() {
        // Cortex is gated off unless INTENTD_ENABLE_CORTEX is set; the
        // marker replaces only the installation check.
        assert!(std::env::var_os("INTENTD_ENABLE_CORTEX").is_none());
        let mut providers = ProvidersSettings::default();
        providers
            .paths
            .insert("cortex".to_string(), MARKER.to_string());
        let err = crate::agent_ops::ensure_provider_available("agent.create", "cortex", &providers)
            .expect_err("gated provider rejected");
        assert!(err.to_string().contains("INTENTD_ENABLE_CORTEX"), "{err}");
    }

    #[test]
    fn an_unmarked_provider_uses_real_discovery() {
        // Whatever this host has installed, an unmarked provider's verdict
        // is exactly the real discovery result.
        let providers = marked(&["claude-code"]);
        let real =
            intent_providers::provider_availability_for("codex", &|_| None).expect("registered");
        assert!(real.gated_off.is_none() && !real.has_npx_fallback);
        let result =
            crate::agent_ops::ensure_provider_available("agent.create", "codex", &providers);
        assert_eq!(result.is_ok(), real.installed, "{result:?}");
    }
}
