//! Regression tests for spec Decision D2: `agent.delegate` provider
//! resolution when the caller supplies no explicit `model`.
//!
//! `agent.delegate` has no `provider` param on the wire (PROTOCOL §5.5), so
//! before these tests the daemon left `AgentCreateExtra.provider` unset and
//! the spawn path fell through to the then-hardcoded default provider
//! (Auggie) regardless of the user's actual configured default. Covers the
//! resolution order:
//! 1. specialist frontmatter `codingAgent`
//! 2. the settings-derived default
//!    ([`crate::agent_session::derived_default_provider`]: registered
//!    `model.defaultProvider`)
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

use intent_core::{
    AgentDelegateInput, BatchTaskEntry, BatchTaskOptions, NoteCreate, NoteId, WorkspaceApi,
    WorkspaceId,
};
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
/// (`model.defaultProvider`) is resolved onto the created session's
/// `provider` when it is available — never left to fall through to the
/// hardcoded default provider (Auggie) at spawn time.
#[tokio::test]
async fn delegate_with_no_explicit_model_resolves_configured_default_provider() {
    let (_t, svc, ws, _specialists, _cfg) = setup().await;
    set(&svc, "model.defaultProvider", json!("mock"));
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

/// `model.default` never carries a provider (it is a bare model id): with an
/// unregistered `model.defaultProvider` the derived default is undecidable
/// and delegate fails loudly instead of trusting the stale id.
#[tokio::test]
async fn delegate_unknown_default_provider_fails_loudly() {
    let (_t, svc, ws, _specialists, _cfg) = setup().await;
    set(&svc, "model.default", json!("foo"));
    set(&svc, "model.defaultProvider", json!("typo"));
    let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", "/tmp/does-not-need-to-exist.js")]);

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
        .expect_err("unregistered model.defaultProvider must not be trusted");
    assert!(
        matches!(&err, intent_core::Error::InvalidParams(m)
            if m.contains("no default provider/model is configured")),
        "clear no-default error, got: {err:?}"
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
    set(&svc, "model.defaultProvider", json!("codex"));
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
    set(&svc, "model.defaultProvider", json!("mock"));
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
    set(&svc, "model.defaultProvider", json!("mock"));
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

/// Nothing configured (no specialist `codingAgent`, no `providers.active`,
/// no compound `model.default`): `agent.delegate` fails loudly with the
/// clear no-default `-32602` (monorepo#3044) — the former residual behavior
/// left `provider` unset and the spawn silently bottomed out at the
/// positional first registered provider (auggie), installed or not.
#[tokio::test]
async fn delegate_with_nothing_configured_fails_loudly() {
    let (_t, svc, ws, _specialists, _cfg) = setup().await;

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
        .expect_err("delegate with nothing configured must fail loudly");
    assert!(
        matches!(&err, intent_core::Error::InvalidParams(m)
            if m.contains("no default provider/model is configured")),
        "clear no-default error, got: {err:?}"
    );
}

/// An explicit `model` param opts out of D2 resolution entirely — the caller
/// made its own provider-adjacent choice by supplying a model — so the child
/// runs on the settings-derived default provider with the caller's model,
/// never on a specialist's `codingAgent` rung.
#[tokio::test]
async fn delegate_with_explicit_model_skips_d2_resolution() {
    let (_t, svc, ws, _specialists, _cfg) = setup().await;
    set(&svc, "model.defaultProvider", json!("mock"));
    let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", "/tmp/does-not-need-to-exist.js")]);

    let resp = svc
        .agent_delegate_op(
            ws.clone(),
            AgentDelegateInput {
                agent_instructions: Some("do the thing".into()),
                model: Some("test-model".into()),
                ..Default::default()
            },
            None,
        )
        .await
        .expect("explicit model bypasses D2; the settings default carries the provider");
    let agent_id = intent_core::AgentId::from(resp["agentId"].as_str().expect("agentId"));

    let got = svc.agent_get_op(agent_id, None).await.expect("get");
    assert_eq!(
        got.model.as_deref(),
        Some("test-model"),
        "the caller's bare model persists unchanged"
    );
}

/// An explicit `provider` param pins the child's provider, outranking the
/// settings-derived default (PROTOCOL §5.5: param > specialist frontmatter >
/// settings default).
#[tokio::test]
async fn delegate_explicit_provider_param_beats_configured_default() {
    let (_t, svc, ws, _specialists, _cfg) = setup().await;
    // The configured default names a DIFFERENT provider — the explicit
    // param must win over it.
    set(&svc, "model.defaultProvider", json!("codex"));
    let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", "/tmp/does-not-need-to-exist.js")]);

    let resp = svc
        .agent_delegate_op(
            ws.clone(),
            AgentDelegateInput {
                agent_instructions: Some("do the thing".into()),
                provider: Some("mock".into()),
                ..Default::default()
            },
            None,
        )
        .await
        .expect("explicit provider param pins the provider");
    let agent_id = intent_core::AgentId::from(resp["agentId"].as_str().expect("agentId"));
    assert_eq!(
        resp["provider"].as_str(),
        Some("mock"),
        "delegate result surfaces the explicit provider param: {resp}"
    );

    let got = svc.agent_get_op(agent_id, None).await.expect("get");
    assert_eq!(
        got.provider.as_deref(),
        Some("mock"),
        "explicit provider param wins over the configured default"
    );
}

/// The explicit `provider` param also outranks the specialist's frontmatter
/// `codingAgent` (D2 step 1) — the caller's word is final.
#[tokio::test]
async fn delegate_explicit_provider_param_beats_specialist_coding_agent() {
    let (_t, svc, ws, specialists_dir, _cfg) = setup().await;
    // The specialist pins a DIFFERENT (unavailable) provider — if the param
    // did not win, resolution would target codex and fail on availability.
    create_specialist_with_coding_agent(specialists_dir.path(), "codex-specialist", "codex");
    let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", "/tmp/does-not-need-to-exist.js")]);

    let resp = svc
        .agent_delegate_op(
            ws.clone(),
            AgentDelegateInput {
                agent_instructions: Some("do the thing".into()),
                specialist: Some("codex-specialist".into()),
                provider: Some("mock".into()),
                ..Default::default()
            },
            None,
        )
        .await
        .expect("explicit provider param outranks the specialist's codingAgent");
    let agent_id = intent_core::AgentId::from(resp["agentId"].as_str().expect("agentId"));

    let got = svc.agent_get_op(agent_id, None).await.expect("get");
    assert_eq!(
        got.provider.as_deref(),
        Some("mock"),
        "explicit provider param wins over specialist frontmatter"
    );
}

/// An explicit `provider` alongside a BARE `model` disambiguates which
/// provider serves the model — the exact multi-provider-model use case the
/// param exists for (monorepo#3044).
#[tokio::test]
async fn delegate_explicit_provider_with_bare_model_pins_provider() {
    let (_t, svc, ws, _specialists, _cfg) = setup().await;
    set(&svc, "model.defaultProvider", json!("codex"));
    let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", "/tmp/does-not-need-to-exist.js")]);

    let resp = svc
        .agent_delegate_op(
            ws.clone(),
            AgentDelegateInput {
                agent_instructions: Some("do the thing".into()),
                model: Some("shared-model".into()),
                provider: Some("mock".into()),
                ..Default::default()
            },
            None,
        )
        .await
        .expect("explicit provider disambiguates the bare model");
    let agent_id = intent_core::AgentId::from(resp["agentId"].as_str().expect("agentId"));

    let got = svc.agent_get_op(agent_id, None).await.expect("get");
    assert_eq!(
        got.provider.as_deref(),
        Some("mock"),
        "bare model runs on the explicitly named provider"
    );
    assert_eq!(
        got.model.as_deref(),
        Some("shared-model"),
        "the bare model id persists unchanged"
    );
}

/// An unknown explicit `provider` is rejected with `-32602` naming the known
/// providers, before any side effect.
#[tokio::test]
async fn delegate_unknown_explicit_provider_errors() {
    let (_t, svc, ws, _specialists, _cfg) = setup().await;

    let err = svc
        .agent_delegate_op(
            ws.clone(),
            AgentDelegateInput {
                agent_instructions: Some("do the thing".into()),
                provider: Some("typo-provider".into()),
                ..Default::default()
            },
            None,
        )
        .await
        .expect_err("unknown provider must be rejected");
    assert!(
        matches!(err, intent_core::Error::InvalidParams(_)),
        "unknown provider is InvalidParams (-32602): {err}"
    );
    let message = err.to_string();
    assert!(
        message.contains("unknown provider: typo-provider"),
        "error names the bad id: {message}"
    );
}

/// A known-but-unavailable explicit `provider` fails with a clear error
/// (same availability contract as the derived-resolution rungs).
#[tokio::test]
async fn delegate_unavailable_explicit_provider_errors() {
    let (_t, svc, ws, _specialists, _cfg) = setup().await;
    let _env = EnvGuard::apply(&[("MOCK_AGENT_SCRIPT_PATH", None)]);

    let err = svc
        .agent_delegate_op(
            ws.clone(),
            AgentDelegateInput {
                agent_instructions: Some("do the thing".into()),
                provider: Some("mock".into()),
                ..Default::default()
            },
            None,
        )
        .await
        .expect_err("unavailable explicit provider must fail clearly");
    let message = err.to_string();
    assert!(
        message.contains("mock"),
        "error names the unavailable provider: {message}"
    );
}

// ── Batch form ──────────────────────────────────────────────────────────────

async fn seed_task(svc: &Services, ws: &WorkspaceId, title: &str) -> NoteId {
    let note = svc
        .create_note(
            ws.clone(),
            NoteCreate {
                title: title.into(),
                content: Some(format!("{title} body")),
                tags: None,
                parent_id: None,
            },
            None,
            None,
        )
        .await
        .expect("create note")
        .note;
    svc.mark_as_task(
        ws.clone(),
        note.id.clone(),
        "not_started".into(),
        vec![],
        None,
        None,
        None,
        None,
    )
    .await
    .expect("markAsTask");
    note.id
}

/// A bad TOP-LEVEL batch `provider` (the default shared by every entry that
/// doesn't override it) is validated up front: the whole call rejects with
/// `-32602` before the classification loop can start ANY task — never a
/// partial batch where earlier rows spawned before a later row surfaced the
/// same shared failure.
#[tokio::test]
async fn batch_delegate_bad_top_level_provider_rejects_before_any_start() {
    let (_t, svc, ws, _specialists, _cfg) = setup().await;
    let t1 = seed_task(&svc, &ws, "First").await;
    let t2 = seed_task(&svc, &ws, "Second").await;
    // "mock" is known but unavailable (env gate off); also cover unknown.
    let _env = EnvGuard::apply(&[("MOCK_AGENT_SCRIPT_PATH", None)]);

    for provider in ["typo-provider", "mock"] {
        let err = svc
            .agent_delegate_op(
                ws.clone(),
                AgentDelegateInput {
                    tasks: Some(vec![
                        BatchTaskEntry::Id(t1.clone()),
                        BatchTaskEntry::Id(t2.clone()),
                    ]),
                    provider: Some(provider.into()),
                    ..Default::default()
                },
                None,
            )
            .await
            .expect_err("bad top-level batch provider rejects the whole call");
        assert!(
            matches!(err, intent_core::Error::InvalidParams(_)),
            "up-front rejection is InvalidParams (-32602): {err}"
        );
        assert!(err.to_string().contains(provider), "{err}");
    }
    let agents = svc.agent_list_op(ws.clone()).await.expect("list");
    assert!(
        agents.is_empty(),
        "no task started before the up-front rejection: {agents:?}"
    );
}

/// Batch `provider` semantics: an entry without an override inherits the
/// top-level default (and starts on it), while a per-entry override beats it
/// — and, being per-entry, a bad override surfaces as that row's `error`
/// disposition without failing rows that already started (the documented
/// non-transactional batch contract, same as `model`/`specialist`).
#[tokio::test]
async fn batch_delegate_provider_top_level_inherited_and_per_entry_override_wins() {
    let (_t, svc, ws, _specialists, _cfg) = setup().await;
    let t1 = seed_task(&svc, &ws, "Inherits").await;
    let t2 = seed_task(&svc, &ws, "Overrides").await;
    let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", "/tmp/does-not-need-to-exist.js")]);

    let resp = svc
        .agent_delegate_op(
            ws.clone(),
            AgentDelegateInput {
                tasks: Some(vec![
                    BatchTaskEntry::Id(t1.clone()),
                    BatchTaskEntry::Options(BatchTaskOptions {
                        task_note_id: t2.clone(),
                        specialist: None,
                        model: None,
                        // Deterministically invalid (unknown id) — proves
                        // the override is applied to THIS row (the valid
                        // top-level "mock" would have started it) and stays
                        // a per-row error.
                        provider: Some("typo-provider".into()),
                        reasoning_effort: None,
                        agent_instructions: None,
                    }),
                ]),
                provider: Some("mock".into()),
                ..Default::default()
            },
            None,
        )
        .await
        .expect("valid top-level provider: the batch call itself succeeds");

    let rows = resp["tasks"].as_array().expect("tasks array");
    let row = |id: &NoteId| {
        rows.iter()
            .find(|r| r["taskNoteId"] == json!(id.0))
            .unwrap_or_else(|| panic!("row for {} in {resp}", id.0))
    };

    let r1 = row(&t1);
    assert_eq!(r1["disposition"], "started", "{resp}");
    let agent_id = intent_core::AgentId::from(r1["agentId"].as_str().expect("agentId"));
    let got = svc.agent_get_op(agent_id, None).await.expect("get");
    assert_eq!(
        got.provider.as_deref(),
        Some("mock"),
        "entry without an override inherits the top-level batch provider"
    );

    let r2 = row(&t2);
    assert_eq!(
        r2["disposition"], "error",
        "per-entry override beats the top-level default and fails per-row: {resp}"
    );
    assert!(
        r2["reason"]
            .as_str()
            .unwrap_or_default()
            .contains("typo-provider"),
        "error row names the overriding provider: {resp}"
    );
}

// ── Disabled / unrunnable providers (monorepo#3178) ─────────────────────────

/// Build a [`intent_providers::ProviderAvailability`] for the injectable
/// runnability gate tests (deterministic — no host PATH probing).
fn availability(
    installed: bool,
    has_npx_fallback: bool,
    gated_off: Option<String>,
) -> intent_providers::ProviderAvailability {
    intent_providers::ProviderAvailability {
        id: "codex",
        display_name: "OpenAI Codex",
        command: "codex-acp",
        installed,
        resolved_path: None,
        gated_off,
        auth_check_args: None,
        has_npx_fallback,
        npx_only_package: None,
        secondary_binary: None,
    }
}

/// A provider whose local binary is missing but that declares an npx
/// fallback (codex) counts as runnable when npx resolves — aligned with what
/// `resolve_spawn` can actually run — and stays rejected when npx is absent.
#[test]
fn runnable_gate_codex_accepted_via_npx_fallback() {
    super::ensure_provider_runnable(
        "agent.delegate",
        "codex",
        Some(availability(false, true, None)),
        &|| true,
    )
    .expect("npx fallback makes codex runnable");

    let err = super::ensure_provider_runnable(
        "agent.delegate",
        "codex",
        Some(availability(false, true, None)),
        &|| false,
    )
    .expect_err("no binary and no npx: not runnable");
    assert!(
        matches!(&err, intent_core::Error::InvalidParams(m) if m.contains("not installed")),
        "not-installed rejection: {err:?}"
    );

    // Installed providers never consult the npx probe.
    super::ensure_provider_runnable(
        "agent.delegate",
        "codex",
        Some(availability(true, false, None)),
        &|| false,
    )
    .expect("installed provider is runnable regardless of npx");
}

/// A gated-off provider (missing env var / feature code) is rejected with
/// the gate reason rather than a generic not-installed message.
#[test]
fn runnable_gate_reports_gate_reason() {
    let err = super::ensure_provider_runnable(
        "agent.delegate",
        "codex",
        Some(availability(
            false,
            true,
            Some("requires env var FOO".into()),
        )),
        &|| true,
    )
    .expect_err("gated-off provider must be rejected");
    assert!(
        matches!(&err, intent_core::Error::InvalidParams(m) if m.contains("requires env var FOO")),
        "gate reason surfaced: {err:?}"
    );
}

/// `providers.enabled[id] == false` rejects with the distinct "not enabled"
/// message — including npx-only providers (claude-code), whose npx-based
/// `installed` status must not bypass the disabled check. Absent maps,
/// absent entries, and `true` entries stay enabled.
#[test]
fn enabled_gate_rejects_explicitly_disabled_providers() {
    let disabled: std::collections::BTreeMap<String, bool> = [
        ("codex".to_string(), false),
        ("claude-code".to_string(), false),
    ]
    .into();
    for id in ["codex", "claude-code"] {
        let err = super::ensure_provider_enabled("agent.delegate", id, Some(&disabled))
            .expect_err("disabled provider must be rejected");
        assert!(
            matches!(&err, intent_core::Error::InvalidParams(m)
                if m.contains("not enabled") && m.contains("Settings > Agents")),
            "distinct not-enabled rejection for {id}: {err:?}"
        );
    }

    let enabled: std::collections::BTreeMap<String, bool> = [("codex".to_string(), true)].into();
    super::ensure_provider_enabled("agent.delegate", "codex", Some(&enabled))
        .expect("explicitly enabled");
    super::ensure_provider_enabled(
        "agent.delegate",
        "codex",
        Some(&std::collections::BTreeMap::default()),
    )
    .expect("absent entry means enabled");
    super::ensure_provider_enabled("agent.delegate", "codex", None)
        .expect("absent map means enabled");
}

/// End to end through `agent.delegate`: a provider that is installed and
/// available but explicitly disabled in `providers.enabled` fails fast with
/// the "not enabled" `-32602` — on the explicit `provider` param and on the
/// settings-derived default alike — and never persists a session row.
#[tokio::test]
async fn delegate_disabled_provider_rejected_with_not_enabled() {
    let (_t, svc, ws, _specialists, _cfg) = setup().await;
    // mock is fully available (env gate satisfied) — only `enabled` blocks it.
    let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", "/tmp/does-not-need-to-exist.js")]);
    set(&svc, "providers.enabled", json!({ "mock": false }));

    // Explicit `provider` param.
    let err = svc
        .agent_delegate_op(
            ws.clone(),
            AgentDelegateInput {
                agent_instructions: Some("do the thing".into()),
                provider: Some("mock".into()),
                ..Default::default()
            },
            None,
        )
        .await
        .expect_err("disabled explicit provider must fail fast");
    assert!(
        matches!(&err, intent_core::Error::InvalidParams(m) if m.contains("not enabled")),
        "distinct not-enabled rejection: {err:?}"
    );

    // Settings-derived default (D2 step 2).
    set(&svc, "model.defaultProvider", json!("mock"));
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
        .expect_err("disabled derived default must fail fast");
    assert!(
        matches!(&err, intent_core::Error::InvalidParams(m) if m.contains("not enabled")),
        "distinct not-enabled rejection: {err:?}"
    );

    let agents = svc.agent_list_op(ws.clone()).await.expect("list");
    assert!(agents.is_empty(), "no session row persisted: {agents:?}");
}

/// End to end through `agent.create` (the shared seam `agent.wakeOrCreate`
/// and delegate's child creation also funnel through): a disabled provider —
/// explicit param or settings-derived default — is rejected with the "not
/// enabled" `-32602` before any session row is persisted.
#[tokio::test]
async fn create_disabled_provider_rejected_with_not_enabled() {
    let (_t, svc, ws, _specialists, _cfg) = setup().await;
    let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", "/tmp/does-not-need-to-exist.js")]);
    set(&svc, "providers.enabled", json!({ "mock": false }));

    // Explicit provider on the create payload.
    let extra = intent_core::AgentCreateExtra {
        provider: Some("mock".into()),
        ..Default::default()
    };
    let err = svc
        .agent_create_op(
            ws.clone(),
            Some("Blocked".into()),
            None,
            None,
            None,
            None,
            false,
            extra,
        )
        .await
        .expect_err("disabled explicit provider must fail fast");
    assert!(
        matches!(&err, intent_core::Error::InvalidParams(m) if m.contains("not enabled")),
        "distinct not-enabled rejection: {err:?}"
    );

    // Settings-derived default provider.
    set(&svc, "model.defaultProvider", json!("mock"));
    let err = svc
        .agent_create_op(
            ws.clone(),
            Some("Blocked2".into()),
            None,
            None,
            None,
            None,
            false,
            intent_core::AgentCreateExtra::default(),
        )
        .await
        .expect_err("disabled derived default must fail fast");
    assert!(
        matches!(&err, intent_core::Error::InvalidParams(m) if m.contains("not enabled")),
        "distinct not-enabled rejection: {err:?}"
    );

    let agents = svc.agent_list_op(ws.clone()).await.expect("list");
    assert!(agents.is_empty(), "no session row persisted: {agents:?}");

    // Re-enabling unblocks creation on the same provider.
    set(&svc, "providers.enabled", json!({ "mock": true }));
    svc.agent_create_op(
        ws.clone(),
        Some("OK".into()),
        None,
        None,
        None,
        None,
        false,
        intent_core::AgentCreateExtra::default(),
    )
    .await
    .expect("re-enabled provider creates normally");
}

// ── Hard-false auth-verdict gate ─────────────────────────────────────────────

/// Restores the seeded auth verdict to cached-unknown (permissive) on drop,
/// so a panicking test cannot leave a hard-false verdict in the process-wide
/// cache (60s TTL) for other tests to trip over. The seeding tests hold the
/// crate-wide [`EnvGuard`] lock, which already serializes them against every
/// other mock-delegating test.
struct AuthSeedReset(&'static str);
impl Drop for AuthSeedReset {
    fn drop(&mut self) {
        crate::provider_auth::seed_auth_verdict_for_tests(self.0, None);
    }
}

/// The auth gate rejects ONLY a hard-false verdict: `true` and unknown both
/// pass (inconclusive probes must never block creates). The claude-code
/// rejection names the catalog login hint AND the desktop-app caveat — a
/// Claude desktop-app sign-in does not carry over to the CLI credential
/// chain, so the message must spell out the CLI login steps.
#[test]
fn auth_gate_rejects_hard_false_and_names_remedy() {
    super::ensure_provider_authenticated("agent.delegate", "claude-code", Some(true))
        .expect("authenticated verdict passes");
    super::ensure_provider_authenticated("agent.delegate", "claude-code", None)
        .expect("unknown verdict stays permissive");

    let err = super::ensure_provider_authenticated("agent.delegate", "claude-code", Some(false))
        .expect_err("hard-false verdict must be rejected");
    let intent_core::Error::InvalidParams(m) = &err else {
        panic!("user-facing InvalidParams (not Internal, which is masked): {err:?}");
    };
    assert!(
        m.contains("claude-code") && m.contains("Anthropic Claude Code"),
        "names the provider: {m}"
    );
    assert!(
        m.contains("claude auth login"),
        "names the catalog loginCommandHint: {m}"
    );
    assert!(
        m.contains("Claude desktop app") && m.contains("does not carry over"),
        "desktop-app caveat: {m}"
    );
    assert!(
        m.contains("\"claude\"") && m.contains("\"/login\""),
        "spells out the CLI login steps: {m}"
    );
}

/// Every other provider's rejection carries its own catalog login hint
/// (`login_command_hint`, else the `{command} login` fallback) — and never
/// the claude-only desktop-app caveat.
#[test]
fn auth_gate_names_each_providers_catalog_login_hint() {
    for (id, hint) in [
        ("auggie", "auggie login"),
        ("grok", "grok login"),
        // codex's hint names the real `codex` CLI (the probe target), not
        // the non-runnable adapter fallback "codex-acp login".
        ("codex", "codex login"),
        // No catalog hint: falls back to `{command} login`.
        ("opencode", "opencode login"),
    ] {
        let err = super::ensure_provider_authenticated("agent.delegate", id, Some(false))
            .expect_err("hard-false verdict must be rejected");
        let intent_core::Error::InvalidParams(m) = &err else {
            panic!("user-facing InvalidParams: {err:?}");
        };
        assert!(m.contains(hint), "catalog login hint for {id}: {m}");
        assert!(
            !m.contains("desktop app"),
            "desktop-app caveat is claude-code-only: {m}"
        );
    }
}

/// End to end through `agent.delegate`: a hard-false cached auth verdict on
/// the resolved provider fails fast with the actionable `-32602` — before
/// any session row is persisted — and flipping the cache back to unknown
/// lets the same delegate proceed.
#[tokio::test]
async fn delegate_hard_false_auth_verdict_rejected_before_session_row() {
    let (_t, svc, ws, _specialists, _cfg) = setup().await;
    let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", "/tmp/does-not-need-to-exist.js")]);
    let _reset = AuthSeedReset("mock");
    set(&svc, "model.defaultProvider", json!("mock"));
    crate::provider_auth::seed_auth_verdict_for_tests("mock", Some(false));

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
        .expect_err("hard-false auth verdict must fail fast at delegate");
    assert!(
        matches!(&err, intent_core::Error::InvalidParams(m)
            if m.contains("not authenticated") && m.contains("Mock (E2E)")),
        "actionable not-authenticated rejection: {err:?}"
    );
    let agents = svc.agent_list_op(ws.clone()).await.expect("list");
    assert!(agents.is_empty(), "no session row persisted: {agents:?}");

    // Unknown verdict (inconclusive probe) proceeds.
    crate::provider_auth::seed_auth_verdict_for_tests("mock", None);
    svc.agent_delegate_op(
        ws.clone(),
        AgentDelegateInput {
            agent_instructions: Some("do the thing".into()),
            ..Default::default()
        },
        None,
    )
    .await
    .expect("unknown verdict stays permissive");
}

/// End to end through `agent.create` (the shared seam `agent.wakeOrCreate`
/// and delegate's child creation also funnel through): a hard-false cached
/// auth verdict — explicit provider param or settings-derived default — is
/// rejected before any session row is persisted; a `true` verdict proceeds.
#[tokio::test]
async fn create_hard_false_auth_verdict_rejected() {
    let (_t, svc, ws, _specialists, _cfg) = setup().await;
    let _env = EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", "/tmp/does-not-need-to-exist.js")]);
    let _reset = AuthSeedReset("mock");
    crate::provider_auth::seed_auth_verdict_for_tests("mock", Some(false));

    // Explicit provider on the create payload.
    let extra = intent_core::AgentCreateExtra {
        provider: Some("mock".into()),
        ..Default::default()
    };
    let err = svc
        .agent_create_op(
            ws.clone(),
            Some("Blocked".into()),
            None,
            None,
            None,
            None,
            false,
            extra,
        )
        .await
        .expect_err("hard-false explicit provider must fail fast");
    assert!(
        matches!(&err, intent_core::Error::InvalidParams(m) if m.contains("not authenticated")),
        "actionable not-authenticated rejection: {err:?}"
    );

    // Settings-derived default provider.
    set(&svc, "model.defaultProvider", json!("mock"));
    let err = svc
        .agent_create_op(
            ws.clone(),
            Some("Blocked2".into()),
            None,
            None,
            None,
            None,
            false,
            intent_core::AgentCreateExtra::default(),
        )
        .await
        .expect_err("hard-false derived default must fail fast");
    assert!(
        matches!(&err, intent_core::Error::InvalidParams(m) if m.contains("not authenticated")),
        "actionable not-authenticated rejection: {err:?}"
    );

    let agents = svc.agent_list_op(ws.clone()).await.expect("list");
    assert!(agents.is_empty(), "no session row persisted: {agents:?}");

    // A logged-in verdict creates normally.
    crate::provider_auth::seed_auth_verdict_for_tests("mock", Some(true));
    svc.agent_create_op(
        ws.clone(),
        Some("OK".into()),
        None,
        None,
        None,
        None,
        false,
        intent_core::AgentCreateExtra::default(),
    )
    .await
    .expect("authenticated provider creates normally");
}
