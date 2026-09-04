//! Tests for the settings rung of the creation-time reasoning-effort
//! resolution: `model.defaultReasoningEffort`.
//!
//! Full precedence at creation:
//! explicit `reasoningEffort` param (including an explicit clear) >
//! specialist model-option effort > specialist frontmatter `reasoningEffort` >
//! settings `model.defaultReasoningEffort` > unset.
//!
//! The settings rung is a strict companion to the settings default *model*:
//! it applies only when the session's model itself resolved from the settings
//! chain ([`crate::agent_ops::DefaultModelSource::Settings`]). A
//! caller-supplied model, a specialist pin, or a fall-through to the provider
//! CLI default all leave the session effort unset.
//!
//! Settings-chain leniency: a level the resolved model's cached
//! `effortLevels` provably does not list is dropped (session effort unset)
//! rather than rejected with `-32602` — only caller-supplied efforts reject.

use std::sync::Arc;

use intent_core::{AgentCreateExtra, AgentDelegateInput, AgentId, WorkspaceId};
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
    // monorepo#3044: creation requires a resolvable provider (no positional
    // fallback) — seed the effective default provider explicitly. The
    // `providers.paths` override points auggie at a deterministic executable
    // so the delegate path's availability check passes without the real
    // binary on the test host (CI has none).
    registry
        .apply(&[
            ("model.defaultProvider".into(), serde_json::json!("auggie")),
            (
                "providers.paths".into(),
                serde_json::json!({ "auggie": "/bin/sh" }),
            ),
        ])
        .expect("seed default provider");
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

/// Seed a provider catalog so `fable-5` has effort evidence and `sonnet5`
/// has none.
///
/// Both `auggie` and `mock` are seeded: the create-path tests never spawn, so
/// they can name `auggie`, while the delegate-path tests resolve a *provider*
/// and pass its availability check — `mock` is gated purely on
/// `MOCK_AGENT_SCRIPT_PATH` ([`EnvGuard`]), and delegate tests that resolve
/// the settings default `auggie` rely on the `providers.paths` override in
/// [`setup`], so neither depends on a real ACP provider binary being
/// installed on the test host (CI has none).
fn seed_catalog(svc: &Services) {
    for provider in ["auggie", "mock"] {
        let version_key = crate::model_catalog::source_for(provider)
            .map(|source| (source.version_key)())
            .unwrap_or_default();
        svc.models_catalog.store_for_test(
            provider,
            &version_key,
            vec![
                json!({ "id": "fable-5", "name": "Fable 5", "provider": provider,
                        "effortLevels": ["low", "high"] }),
                json!({ "id": "sonnet5", "name": "Sonnet 5", "provider": provider }),
            ],
        );
    }
}

/// Make the `mock` provider available for delegate-path tests, which resolve
/// and validate a provider before creating the session.
fn mock_provider_env() -> EnvGuard {
    EnvGuard::set_all(&[("MOCK_AGENT_SCRIPT_PATH", "/tmp/does-not-need-to-exist.js")])
}

fn write_specialist(dir: &std::path::Path, id: &str, extra_frontmatter: &str) {
    let content =
        format!("---\nname: \"{id}\"\ndescription: \"Test specialist\"\n{extra_frontmatter}---\n\nTest prompt");
    std::fs::write(dir.join(format!("{id}.md")), content).expect("write specialist");
}

async fn create(
    svc: &Services,
    ws: &WorkspaceId,
    model: Option<&str>,
    specialist: Option<&str>,
    extra: AgentCreateExtra,
) -> AgentId {
    let created = svc
        .agent_create_op(
            ws.clone(),
            Some("Agent".into()),
            model.map(str::to_string),
            specialist.map(str::to_string),
            None,
            None,
            false,
            extra,
        )
        .await
        .expect("create");
    AgentId::from(created["agent"]["id"].as_str().expect("agent id"))
}

async fn effort_of(svc: &Services, id: AgentId) -> Option<String> {
    svc.agent_get_session_op(id)
        .await
        .expect("get session")
        .reasoning_effort
}

/// The settings default effort is pinned when nothing more specific decided
/// it and the model itself came from the settings chain.
#[tokio::test]
async fn settings_default_effort_applies_to_a_settings_resolved_model() {
    let (_t, svc, ws, _spec, _cfg) = setup().await;
    seed_catalog(&svc);
    set(&svc, "model.default", json!("fable-5"));
    set(&svc, "model.defaultReasoningEffort", json!("high"));

    let id = create(&svc, &ws, None, None, AgentCreateExtra::default()).await;
    let session = svc.agent_get_session_op(id).await.expect("get session");
    assert_eq!(session.model.as_deref(), Some("fable-5"));
    assert_eq!(session.reasoning_effort.as_deref(), Some("high"));
}

/// An explicitly supplied model pins nothing from the settings effort: the
/// setting is a companion to the settings *default model* only.
#[tokio::test]
async fn explicit_model_suppresses_the_settings_default_effort() {
    let (_t, svc, ws, _spec, _cfg) = setup().await;
    seed_catalog(&svc);
    set(&svc, "model.default", json!("fable-5"));
    set(&svc, "model.defaultReasoningEffort", json!("high"));

    let id = create(
        &svc,
        &ws,
        Some("fable-5"),
        None,
        AgentCreateExtra::default(),
    )
    .await;
    assert_eq!(effort_of(&svc, id).await, None);
}

/// A specialist frontmatter `model` pin also suppresses the settings effort —
/// the model did not come from the settings chain.
#[tokio::test]
async fn specialist_model_pin_suppresses_the_settings_default_effort() {
    let (_t, svc, ws, spec_dir, _cfg) = setup().await;
    seed_catalog(&svc);
    set(&svc, "model.default", json!("sonnet5"));
    set(&svc, "model.defaultReasoningEffort", json!("high"));
    write_specialist(spec_dir.path(), "pinned", "model: \"fable-5\"\n");

    let id = create(&svc, &ws, None, Some("pinned"), AgentCreateExtra::default()).await;
    let session = svc.agent_get_session_op(id).await.expect("get session");
    assert_eq!(session.model.as_deref(), Some("fable-5"));
    assert_eq!(session.reasoning_effort, None);
}

/// No settings default model (the CLI-default rung) → no settings effort.
#[tokio::test]
async fn cli_default_model_suppresses_the_settings_default_effort() {
    let (_t, svc, ws, _spec, _cfg) = setup().await;
    seed_catalog(&svc);
    set(&svc, "model.defaultReasoningEffort", json!("high"));

    let id = create(&svc, &ws, None, None, AgentCreateExtra::default()).await;
    let session = svc.agent_get_session_op(id).await.expect("get session");
    assert_eq!(session.model, None);
    assert_eq!(session.reasoning_effort, None);
}

/// An explicit `reasoningEffort` param outranks the settings default, and an
/// explicit *clear* (blank param) is honored as a decision rather than
/// falling through to the setting.
#[tokio::test]
async fn explicit_effort_param_and_explicit_clear_outrank_the_setting() {
    let (_t, svc, ws, _spec, _cfg) = setup().await;
    seed_catalog(&svc);
    set(&svc, "model.default", json!("fable-5"));
    set(&svc, "model.defaultReasoningEffort", json!("high"));

    let id = create(
        &svc,
        &ws,
        None,
        None,
        AgentCreateExtra {
            reasoning_effort: Some("low".into()),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(effort_of(&svc, id).await.as_deref(), Some("low"));

    let id = create(
        &svc,
        &ws,
        None,
        None,
        AgentCreateExtra {
            reasoning_effort: Some(String::new()),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(
        effort_of(&svc, id).await,
        None,
        "an explicit clear must not fall through to the settings default"
    );
}

/// Specialist frontmatter `reasoningEffort` and the chosen model option's
/// effort both outrank the settings default, even when the model itself came
/// from the settings chain.
#[tokio::test]
async fn specialist_efforts_outrank_the_settings_default() {
    let (_t, svc, ws, spec_dir, _cfg) = setup().await;
    seed_catalog(&svc);
    set(&svc, "model.default", json!("fable-5"));
    set(&svc, "model.defaultReasoningEffort", json!("high"));
    let _env = mock_provider_env();
    write_specialist(spec_dir.path(), "fm", "reasoningEffort: \"low\"\n");
    write_specialist(
        spec_dir.path(),
        "opt",
        "model: \"fable-5\"\nmodelOptions:\n  - model: \"fable-5\"\n    reasoningEffort: \"low\"\n",
    );

    for spec in ["fm", "opt"] {
        let resp = svc
            .agent_delegate_op(
                ws.clone(),
                AgentDelegateInput {
                    agent_instructions: Some("do the thing".into()),
                    specialist: Some(spec.into()),
                    ..Default::default()
                },
                None,
            )
            .await
            .expect("delegate");
        let id = AgentId::from(resp["agentId"].as_str().expect("agentId"));
        assert_eq!(
            effort_of(&svc, id).await.as_deref(),
            Some("low"),
            "specialist effort must outrank the settings default ({spec})"
        );
    }
}

/// A specialist whose `modelOptions` entry keys on the *settings default*
/// model (it pins no `model` of its own) still gets that option's effort:
/// the delegate/wakeOrCreate effort seam resolves the effective model through
/// the full default-model chain, not just the specialist's own pin.
#[tokio::test]
async fn model_option_keyed_on_the_settings_default_model_is_selected() {
    let (_t, svc, ws, spec_dir, _cfg) = setup().await;
    seed_catalog(&svc);
    set(&svc, "model.default", json!("fable-5"));
    set(&svc, "model.defaultReasoningEffort", json!("high"));
    let _env = mock_provider_env();
    write_specialist(
        spec_dir.path(),
        "unpinned",
        "modelOptions:\n  - model: \"fable-5\"\n    reasoningEffort: \"low\"\n",
    );

    let resp = svc
        .agent_delegate_op(
            ws.clone(),
            AgentDelegateInput {
                agent_instructions: Some("do the thing".into()),
                specialist: Some("unpinned".into()),
                ..Default::default()
            },
            None,
        )
        .await
        .expect("delegate");
    let id = AgentId::from(resp["agentId"].as_str().expect("agentId"));
    assert_eq!(
        effort_of(&svc, id).await.as_deref(),
        Some("low"),
        "the option keyed on the settings default model must win over the settings effort"
    );
}

/// A *direct* `agent.create` naming a specialist consults the same specialist
/// rungs the delegate seam does, so frontmatter `reasoningEffort` (and a
/// `modelOptions` entry keyed on the settings default model) still outranks
/// the settings default even though the model resolved from the settings
/// chain.
#[tokio::test]
async fn direct_create_specialist_efforts_outrank_the_settings_default() {
    let (_t, svc, ws, spec_dir, _cfg) = setup().await;
    seed_catalog(&svc);
    set(&svc, "model.default", json!("fable-5"));
    set(&svc, "model.defaultReasoningEffort", json!("high"));
    write_specialist(spec_dir.path(), "fm", "reasoningEffort: \"low\"\n");
    write_specialist(
        spec_dir.path(),
        "opt",
        "modelOptions:\n  - model: \"fable-5\"\n    reasoningEffort: \"low\"\n",
    );

    for spec in ["fm", "opt"] {
        let id = create(&svc, &ws, None, Some(spec), AgentCreateExtra::default()).await;
        let session = svc.agent_get_session_op(id).await.expect("get session");
        assert_eq!(session.model.as_deref(), Some("fable-5"));
        assert_eq!(
            session.reasoning_effort.as_deref(),
            Some("low"),
            "specialist effort must outrank the settings default on direct create ({spec})"
        );
    }
}

/// A specialist that declares no effort at all still falls through to the
/// settings default, so wiring the specialist rungs into the create seam does
/// not shadow the new rung.
#[tokio::test]
async fn direct_create_specialist_without_effort_falls_through_to_the_setting() {
    let (_t, svc, ws, spec_dir, _cfg) = setup().await;
    seed_catalog(&svc);
    set(&svc, "model.default", json!("fable-5"));
    set(&svc, "model.defaultReasoningEffort", json!("high"));
    write_specialist(spec_dir.path(), "plain", "");

    let id = create(&svc, &ws, None, Some("plain"), AgentCreateExtra::default()).await;
    assert_eq!(effort_of(&svc, id).await.as_deref(), Some("high"));
}

/// A specialist effort the resolved model provably does not support is a
/// caller-side decision, not the settings chain, so it still rejects with
/// `-32602` rather than being dropped.
#[tokio::test]
async fn direct_create_unsupported_specialist_effort_is_rejected() {
    let (_t, svc, ws, spec_dir, _cfg) = setup().await;
    seed_catalog(&svc);
    set(&svc, "model.default", json!("fable-5"));
    write_specialist(spec_dir.path(), "bad", "reasoningEffort: \"xhigh\"\n");

    let err = svc
        .agent_create_op(
            ws.clone(),
            Some("Agent".into()),
            None,
            Some("bad".into()),
            None,
            None,
            false,
            AgentCreateExtra::default(),
        )
        .await
        .expect_err("unsupported specialist effort must reject");
    assert!(
        format!("{err}").contains("reasoningEffort"),
        "expected an invalid-params rejection naming reasoningEffort, got: {err}"
    );
}

/// Settings-chain leniency: a level the resolved model's cached
/// `effortLevels` does not list is dropped (unset), not `-32602`.
#[tokio::test]
async fn unsupported_settings_default_effort_is_dropped_not_rejected() {
    let (_t, svc, ws, _spec, _cfg) = setup().await;
    seed_catalog(&svc);
    set(&svc, "model.default", json!("fable-5"));
    set(&svc, "model.defaultReasoningEffort", json!("xhigh"));

    let id = create(&svc, &ws, None, None, AgentCreateExtra::default()).await;
    let session = svc.agent_get_session_op(id).await.expect("get session");
    assert_eq!(session.model.as_deref(), Some("fable-5"));
    assert_eq!(
        session.reasoning_effort, None,
        "an unsupported settings level is dropped with a warn, never a -32602"
    );
}

/// With no cached `effortLevels` evidence the settings level passes through
/// verbatim, matching `ensure_effort_supported_by_model`'s evidence-only rule.
#[tokio::test]
async fn settings_default_effort_passes_through_without_catalog_evidence() {
    let (_t, svc, ws, _spec, _cfg) = setup().await;
    seed_catalog(&svc);
    set(&svc, "model.default", json!("sonnet5"));
    set(&svc, "model.defaultReasoningEffort", json!("xhigh"));

    let id = create(&svc, &ws, None, None, AgentCreateExtra::default()).await;
    assert_eq!(effort_of(&svc, id).await.as_deref(), Some("xhigh"));
}

/// A blank/whitespace-only setting value never becomes a session effort.
#[tokio::test]
async fn blank_settings_default_effort_leaves_the_session_unset() {
    let (_t, svc, ws, _spec, _cfg) = setup().await;
    seed_catalog(&svc);
    set(&svc, "model.default", json!("fable-5"));
    set(&svc, "model.defaultReasoningEffort", json!("   "));

    let id = create(&svc, &ws, None, None, AgentCreateExtra::default()).await;
    assert_eq!(effort_of(&svc, id).await, None);
}

/// The model-option effort rung matches on the effective `{ provider, model }`
/// pair: two options sharing a bare model id under different providers apply
/// their own efforts, keyed by the provider the delegate actually resolves.
#[tokio::test]
async fn delegate_model_option_effort_matches_the_effective_provider() {
    let (_t, svc, ws, spec_dir, _cfg) = setup().await;
    seed_catalog(&svc);
    set(&svc, "model.default", json!("fable-5"));
    let _env = mock_provider_env();
    write_specialist(
        spec_dir.path(),
        "paired",
        "modelOptions: [{\"provider\":\"other\",\"model\":\"fable-5\",\"reasoningEffort\":\"high\"},\
         {\"provider\":\"mock\",\"model\":\"fable-5\",\"reasoningEffort\":\"low\"}]\n",
    );

    let resp = svc
        .agent_delegate_op(
            ws.clone(),
            AgentDelegateInput {
                agent_instructions: Some("do the thing".into()),
                specialist: Some("paired".into()),
                provider: Some("mock".into()),
                ..Default::default()
            },
            None,
        )
        .await
        .expect("delegate");
    let id = AgentId::from(resp["agentId"].as_str().expect("agentId"));
    assert_eq!(
        effort_of(&svc, id).await.as_deref(),
        Some("low"),
        "the option pinned to the effective provider wins over one sharing the bare model id"
    );
}

/// Explicit `model` + no `provider` (spawn doctrine corner): the child spawns
/// on the settings-derived default — the specialist's `codingAgent` never
/// participates in that spawn chain — so the pair match must key on the
/// settings default too, not on `codingAgent`.
#[tokio::test]
async fn delegate_explicit_model_pair_match_ignores_the_specialist_coding_agent() {
    let (_t, svc, ws, spec_dir, _cfg) = setup().await;
    seed_catalog(&svc);
    let _env = mock_provider_env();
    // Settings default provider is `auggie` (seeded in setup); the specialist
    // pins `codingAgent: mock` and carries one option per provider.
    write_specialist(
        spec_dir.path(),
        "pinned",
        "codingAgent: \"mock\"\n\
         modelOptions: [{\"provider\":\"mock\",\"model\":\"fable-5\",\"reasoningEffort\":\"high\"},\
         {\"provider\":\"auggie\",\"model\":\"fable-5\",\"reasoningEffort\":\"low\"}]\n",
    );

    let resp = svc
        .agent_delegate_op(
            ws.clone(),
            AgentDelegateInput {
                agent_instructions: Some("do the thing".into()),
                specialist: Some("pinned".into()),
                model: Some("fable-5".into()),
                ..Default::default()
            },
            None,
        )
        .await
        .expect("delegate");
    let id = AgentId::from(resp["agentId"].as_str().expect("agentId"));
    assert_eq!(
        effort_of(&svc, id).await.as_deref(),
        Some("low"),
        "the pair match keys on the settings-derived default the child actually spawns on, \
         not the specialist's codingAgent"
    );
}

/// The settings rung also applies through `agent.delegate` (which routes into
/// `agent_create_op`) when neither the caller nor a specialist decided it.
#[tokio::test]
async fn delegate_applies_the_settings_default_effort() {
    let (_t, svc, ws, _spec, _cfg) = setup().await;
    seed_catalog(&svc);
    set(&svc, "model.default", json!("fable-5"));
    set(&svc, "model.defaultReasoningEffort", json!("high"));
    let _env = mock_provider_env();

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
        .expect("delegate");
    let id = AgentId::from(resp["agentId"].as_str().expect("agentId"));
    assert_eq!(effort_of(&svc, id).await.as_deref(), Some("high"));
}
