//! Regression tests for spec Decision D2: `agent.delegate` provider
//! resolution when the caller supplies no explicit `model`.
//!
//! `agent.delegate` has no `provider` param on the wire (PROTOCOL §5.5), so
//! before these tests the daemon left `AgentCreateExtra.provider` unset and
//! the spawn path fell through to the then-hardcoded default provider
//! (Auggie) regardless of the user's actual configured default. Covers the
//! resolution order:
//! 1. specialist frontmatter `codingAgent` (or a compound `model` prefix)
//! 2. the settings-derived default
//!    ([`crate::agent_session::derived_default_provider`]: registered
//!    `model.default` compound prefix, else registered `providers.active`)
//! 3. neither resolvable/available → a clear caller-surfaceable error, never
//!    a hardcoded/positional default provider
//!
//! The `mock` provider ([`intent_providers::ACP_PROVIDERS`]) is used as the
//! resolution target throughout: its availability is gated purely on the
//! `MOCK_AGENT_SCRIPT_PATH` env var (`requires_env_var`), so
//! [`crate::agent_manager::tests::EnvGuard`] makes "available" / "not
//! available" fully deterministic without depending on any real ACP provider
//! binary being installed on the test host.

use std::sync::Arc;

use intent_core::{AgentDelegateInput, WorkspaceId};
use intent_store::Store;
use serde_json::json;
use tempfile::TempDir;

use super::tests::{workspace, TempDb};
use crate::agent_manager::tests::EnvGuard;
use crate::Services;

async fn setup() -> (TempDb, Services, WorkspaceId, TempDir, TempDir) {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.expect("ws");

    let specialists_dir = TempDir::new().expect("temp specialists dir");
    let config_dir = TempDir::new().expect("temp config dir");
    let registry = Arc::new(
        crate::SettingsRegistry::load(config_dir.path().join("config.toml"))
            .expect("load registry"),
    );
    let services = Services::new(store)
        .with_settings_registry(registry)
        .with_specialist_dirs(
            Some(specialists_dir.path().to_path_buf()),
            Some(specialists_dir.path().to_path_buf()),
        );
    (tmp, services, ws, specialists_dir, config_dir)
}

fn set(svc: &Services, path: &str, value: serde_json::Value) {
    svc.settings_registry()
        .expect("registry wired")
        .apply(&[(path.to_string(), value)])
        .expect("apply setting");
}

fn create_specialist_with_coding_agent(dir: &std::path::Path, id: &str, coding_agent: &str) {
    let content = format!(
        "---\nname: \"{id}\"\ndescription: \"Test specialist\"\ncodingAgent: \"{coding_agent}\"\n---\n\nTest prompt"
    );
    std::fs::write(dir.join(format!("{id}.md")), content).expect("write specialist");
}

/// No explicit `model`, no specialist: the configured default
/// (`providers.active`) is resolved onto the created session's `provider`
/// when it is available — never left to fall through to the hardcoded
/// default provider (Auggie) at spawn time.
#[tokio::test]
async fn delegate_with_no_explicit_model_resolves_configured_default_provider() {
    let (_t, svc, ws, _specialists, _cfg) = setup().await;
    set(&svc, "providers.active", json!("mock"));
    let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", "/tmp/does-not-need-to-exist.js")]);

    let resp = svc
        .agent_delegate_op(
            ws.clone(),
            AgentDelegateInput {
                agent_instructions: Some("do the thing".into()),
                ..Default::default()
            },
            None,
        )
        .await
        .expect("delegate resolves the configured default");
    let agent_id = intent_core::AgentId::from(resp["agentId"].as_str().expect("agentId"));
    assert_eq!(
        resp["provider"].as_str(),
        Some("mock"),
        "delegate result surfaces the resolved provider (PROTOCOL §5.5): {resp}"
    );

    let got = svc.agent_get_op(agent_id, None).await.expect("get");
    assert_eq!(
        got.provider.as_deref(),
        Some("mock"),
        "configured default provider persisted on the delegated session, not left unset/Auggie"
    );
}

/// Within D2 step 2, the `model.default` compound prefix outranks
/// `providers.active` — the derived default is the prefix's provider even
/// when a different (also registered) `providers.active` is set.
#[tokio::test]
async fn delegate_model_default_prefix_outranks_providers_active() {
    let (_t, svc, ws, _specialists, _cfg) = setup().await;
    set(&svc, "model.default", json!("mock:test-model"));
    // providers.active names a different registered provider; if the prefix
    // did not outrank it, resolution would target codex (and fail on
    // availability) rather than the available mock provider.
    set(&svc, "providers.active", json!("codex"));
    let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", "/tmp/does-not-need-to-exist.js")]);

    let resp = svc
        .agent_delegate_op(
            ws.clone(),
            AgentDelegateInput {
                agent_instructions: Some("do the thing".into()),
                ..Default::default()
            },
            None,
        )
        .await
        .expect("delegate resolves the model.default prefix provider");
    let agent_id = intent_core::AgentId::from(resp["agentId"].as_str().expect("agentId"));

    let got = svc.agent_get_op(agent_id, None).await.expect("get");
    assert_eq!(
        got.provider.as_deref(),
        Some("mock"),
        "model.default compound prefix outranks providers.active in the derived default"
    );
}

/// An unknown `model.default` compound prefix is not trusted: it falls
/// through to `providers.active` instead of hard-failing delegate with an
/// unknown-provider error (or shadowing a valid configured default).
#[tokio::test]
async fn delegate_unknown_model_default_prefix_falls_through_to_active() {
    let (_t, svc, ws, _specialists, _cfg) = setup().await;
    set(&svc, "model.default", json!("typo:foo"));
    set(&svc, "providers.active", json!("mock"));
    let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", "/tmp/does-not-need-to-exist.js")]);

    let resp = svc
        .agent_delegate_op(
            ws.clone(),
            AgentDelegateInput {
                agent_instructions: Some("do the thing".into()),
                ..Default::default()
            },
            None,
        )
        .await
        .expect("unknown model.default prefix falls through instead of failing delegate");
    let agent_id = intent_core::AgentId::from(resp["agentId"].as_str().expect("agentId"));

    let got = svc.agent_get_op(agent_id, None).await.expect("get");
    assert_eq!(
        got.provider.as_deref(),
        Some("mock"),
        "unknown model.default prefix falls through to providers.active"
    );
}

/// A specialist's frontmatter `codingAgent` takes precedence over the
/// configured default (D2 step 1 beats step 2).
#[tokio::test]
async fn delegate_specialist_explicit_coding_agent_beats_configured_default() {
    let (_t, svc, ws, specialists_dir, _cfg) = setup().await;
    create_specialist_with_coding_agent(specialists_dir.path(), "mock-specialist", "mock");
    // Configured default names a DIFFERENT (unavailable) provider — if the
    // specialist's explicit codingAgent were not honored first, this would
    // either resolve to "codex" or error, not "mock".
    set(&svc, "providers.active", json!("codex"));
    let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", "/tmp/does-not-need-to-exist.js")]);

    let resp = svc
        .agent_delegate_op(
            ws.clone(),
            AgentDelegateInput {
                agent_instructions: Some("do the thing".into()),
                specialist: Some("mock-specialist".into()),
                ..Default::default()
            },
            None,
        )
        .await
        .expect("delegate resolves the specialist's explicit coding agent");
    let agent_id = intent_core::AgentId::from(resp["agentId"].as_str().expect("agentId"));

    let got = svc.agent_get_op(agent_id, None).await.expect("get");
    assert_eq!(
        got.provider.as_deref(),
        Some("mock"),
        "specialist's explicit codingAgent wins over the configured default"
    );
}

/// The configured default provider is unavailable (not installed / gated
/// off): `agent.delegate` fails with a clear, caller-surfaceable error
/// instead of silently persisting/spawning the hardcoded default provider
/// (Auggie).
#[tokio::test]
async fn delegate_configured_default_unavailable_errors_instead_of_auggie() {
    let (_t, svc, ws, _specialists, _cfg) = setup().await;
    set(&svc, "providers.active", json!("mock"));
    let _env = EnvGuard::apply(&[("MOCK_AGENT_SCRIPT_PATH", None)]);

    let err = svc
        .agent_delegate_op(
            ws.clone(),
            AgentDelegateInput {
                agent_instructions: Some("do the thing".into()),
                ..Default::default()
            },
            None,
        )
        .await
        .expect_err("unavailable configured default must fail, not silently spawn Auggie");
    let message = err.to_string();
    assert!(
        message.contains("mock"),
        "error names the unavailable configured provider: {message}"
    );
    assert!(
        !message.to_ascii_lowercase().contains("auggie"),
        "error must never silently point at the hardcoded default provider: {message}"
    );
}

/// A specialist's unavailable explicit `codingAgent` also fails clearly
/// rather than falling through to the configured default or Auggie.
#[tokio::test]
async fn delegate_specialist_coding_agent_unavailable_errors_instead_of_auggie() {
    let (_t, svc, ws, specialists_dir, _cfg) = setup().await;
    create_specialist_with_coding_agent(specialists_dir.path(), "mock-specialist", "mock");
    set(&svc, "providers.active", json!("mock"));
    let _env = EnvGuard::apply(&[("MOCK_AGENT_SCRIPT_PATH", None)]);

    let err = svc
        .agent_delegate_op(
            ws.clone(),
            AgentDelegateInput {
                agent_instructions: Some("do the thing".into()),
                specialist: Some("mock-specialist".into()),
                ..Default::default()
            },
            None,
        )
        .await
        .expect_err("unavailable specialist coding agent must fail, not silently spawn Auggie");
    let message = err.to_string();
    assert!(
        !message.to_ascii_lowercase().contains("auggie"),
        "error must never silently point at the hardcoded default provider: {message}"
    );
}

/// Nothing configured (no specialist `codingAgent`, no `providers.active`):
/// `agent.delegate` still succeeds and leaves `provider` unset, exactly the
/// pre-existing "no configured default" model-resolution behavior — this is
/// NOT the "unavailable" error case (D2 step 3's `Ok(None)` branch), and
/// must not regress the large existing test suite that delegates without
/// ever configuring a default provider.
#[tokio::test]
async fn delegate_with_nothing_configured_leaves_provider_unset() {
    let (_t, svc, ws, _specialists, _cfg) = setup().await;

    let resp = svc
        .agent_delegate_op(
            ws.clone(),
            AgentDelegateInput {
                agent_instructions: Some("do the thing".into()),
                ..Default::default()
            },
            None,
        )
        .await
        .expect("delegate succeeds with nothing configured");
    let agent_id = intent_core::AgentId::from(resp["agentId"].as_str().expect("agentId"));
    assert!(
        resp.get("provider").is_none(),
        "delegate result omits `provider` when the session has none (never null): {resp}"
    );

    let got = svc.agent_get_op(agent_id, None).await.expect("get");
    assert_eq!(got.provider, None, "no configured default to resolve to");
}

/// An explicit `model` param (even a bare, non-compound one) opts out of D2
/// resolution entirely — the caller made its own provider-adjacent choice by
/// supplying a model.
#[tokio::test]
async fn delegate_with_explicit_model_skips_d2_resolution() {
    let (_t, svc, ws, _specialists, _cfg) = setup().await;
    // A configured default that would error if D2 ran (unavailable "mock").
    set(&svc, "providers.active", json!("mock"));
    let _env = EnvGuard::apply(&[("MOCK_AGENT_SCRIPT_PATH", None)]);

    let resp = svc
        .agent_delegate_op(
            ws.clone(),
            AgentDelegateInput {
                agent_instructions: Some("do the thing".into()),
                model: Some("opencode:kimi-k3".into()),
                ..Default::default()
            },
            None,
        )
        .await
        .expect("explicit model bypasses D2 entirely, so the unavailable default never errors");
    let agent_id = intent_core::AgentId::from(resp["agentId"].as_str().expect("agentId"));
    assert_eq!(
        resp["provider"].as_str(),
        Some("opencode"),
        "delegate result surfaces the compound-model provider prefix: {resp}"
    );

    let got = svc.agent_get_op(agent_id, None).await.expect("get");
    assert_eq!(
        got.provider.as_deref(),
        Some("opencode"),
        "explicit compound model's provider prefix wins, untouched by D2"
    );
}
