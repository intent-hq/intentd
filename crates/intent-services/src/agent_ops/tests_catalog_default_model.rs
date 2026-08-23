//! Tests for the catalog-default rung of the creation-time default-model
//! resolution (PROTOCOL §5.5): when neither a specialist frontmatter pin nor
//! the settings chain resolves a model, the **cached** provider catalog's
//! `isDefault` row is pinned to `session.model` — frozen even if the provider
//! later changes its default. Cache-only: a cold cache or a catalog without a
//! marked row falls through to the provider CLI default, byte-identical to
//! the pre-rung behavior.
//!
//! Full precedence:
//! explicit model > specialist frontmatter model > settings chain > catalog
//! `isDefault` row > CLI default
//!
//! The settings default reasoning effort remains a strict companion to the
//! *settings* default model: a catalog-default-resolved model never pins it.

use std::sync::Arc;

use intent_core::{AgentCreateExtra, AgentId, WorkspaceId};
use intent_store::Store;
use serde_json::json;
use tempfile::TempDir;

use super::tests::{workspace, TempDb};
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
    // fallback) — seed the effective default provider explicitly; the tests
    // here exercise the MODEL rungs, which sit below it.
    registry
        .apply(&[("providers.active".into(), json!("auggie"))])
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

/// Seed the auggie catalog with `sonnet5` marked as the provider default.
fn seed_catalog_with_default(svc: &Services) {
    svc.models_catalog.store_for_test(
        "auggie",
        crate::model_catalog::AUGGIE_CATALOG_VERSION,
        vec![
            json!({ "id": "fable-5", "name": "Fable 5", "provider": "auggie",
                    "effortLevels": ["low", "high"] }),
            json!({ "id": "sonnet5", "name": "Sonnet 5", "provider": "auggie",
                    "isDefault": true }),
        ],
    );
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

fn write_specialist(dir: &std::path::Path, id: &str, extra_frontmatter: &str) {
    let content = format!(
        "---\nname: \"{id}\"\ndescription: \"Test specialist\"\n{extra_frontmatter}---\n\nTest prompt"
    );
    std::fs::write(dir.join(format!("{id}.md")), content).expect("write specialist");
}

/// Warm cache: a no-model create pins the catalog's `isDefault` row to
/// session.model.
#[tokio::test]
async fn warm_catalog_default_is_pinned_at_create() {
    let (_t, svc, ws, _spec, _cfg) = setup().await;
    seed_catalog_with_default(&svc);

    let id = create(&svc, &ws, None, None, AgentCreateExtra::default()).await;
    let got = svc.agent_get_op(id, None).await.expect("get");
    assert_eq!(got.model.as_deref(), Some("sonnet5"));
}

/// Cold cache: behavior is byte-identical to before the rung — session.model
/// stays unset (provider CLI default).
#[tokio::test]
async fn cold_cache_falls_through_to_cli_default() {
    let (_t, svc, ws, _spec, _cfg) = setup().await;

    let id = create(&svc, &ws, None, None, AgentCreateExtra::default()).await;
    let got = svc.agent_get_op(id, None).await.expect("get");
    assert_eq!(got.model, None);
}

/// Warm cache without an `isDefault` row: no pin, CLI default.
#[tokio::test]
async fn catalog_without_default_row_falls_through() {
    let (_t, svc, ws, _spec, _cfg) = setup().await;
    svc.models_catalog.store_for_test(
        "auggie",
        crate::model_catalog::AUGGIE_CATALOG_VERSION,
        vec![
            json!({ "id": "fable-5", "name": "Fable 5", "provider": "auggie" }),
            json!({ "id": "sonnet5", "name": "Sonnet 5", "provider": "auggie" }),
        ],
    );

    let id = create(&svc, &ws, None, None, AgentCreateExtra::default()).await;
    let got = svc.agent_get_op(id, None).await.expect("get");
    assert_eq!(got.model, None);
}

/// The settings chain outranks the catalog default.
#[tokio::test]
async fn settings_chain_outranks_catalog_default() {
    let (_t, svc, ws, _spec, _cfg) = setup().await;
    seed_catalog_with_default(&svc);
    set(&svc, "model.default", json!("auggie:fable-5"));

    let id = create(&svc, &ws, None, None, AgentCreateExtra::default()).await;
    let got = svc.agent_get_op(id, None).await.expect("get");
    assert_eq!(got.model.as_deref(), Some("auggie:fable-5"));
}

/// A specialist frontmatter pin outranks the catalog default.
#[tokio::test]
async fn specialist_pin_outranks_catalog_default() {
    let (_t, svc, ws, spec_dir, _cfg) = setup().await;
    seed_catalog_with_default(&svc);
    write_specialist(spec_dir.path(), "pinned", "model: \"auggie:fable-5\"\n");

    let id = create(&svc, &ws, None, Some("pinned"), AgentCreateExtra::default()).await;
    let got = svc.agent_get_op(id, None).await.expect("get");
    assert_eq!(got.model.as_deref(), Some("auggie:fable-5"));
}

/// An explicit client model outranks everything.
#[tokio::test]
async fn explicit_model_outranks_catalog_default() {
    let (_t, svc, ws, _spec, _cfg) = setup().await;
    seed_catalog_with_default(&svc);

    let id = create(
        &svc,
        &ws,
        Some("fable-5"),
        None,
        AgentCreateExtra::default(),
    )
    .await;
    let got = svc.agent_get_op(id, None).await.expect("get");
    assert_eq!(got.model.as_deref(), Some("fable-5"));
}

/// A settings default owned by another provider drops through to the catalog
/// default instead of the CLI default (the rung sits between them).
#[tokio::test]
async fn foreign_settings_default_drops_to_catalog_default() {
    let (_t, svc, ws, _spec, _cfg) = setup().await;
    seed_catalog_with_default(&svc);
    // Also seed grok so `grok-4-fast` provably belongs to grok, making the
    // settings default a provable foreign model for auggie.
    svc.models_catalog.store_for_test(
        "grok",
        "",
        vec![json!({ "id": "grok-4-fast", "name": "Grok", "provider": "grok" })],
    );
    set(&svc, "model.default", json!("grok-4-fast"));

    let id = create(
        &svc,
        &ws,
        None,
        None,
        AgentCreateExtra {
            provider: Some("auggie".into()),
            ..Default::default()
        },
    )
    .await;
    let got = svc.agent_get_op(id, None).await.expect("get");
    assert_eq!(
        got.model.as_deref(),
        Some("sonnet5"),
        "a provider-guarded settings drop must land on the catalog default"
    );
}

/// The settings default reasoning effort does NOT apply to a
/// catalog-default-resolved model: it is a strict companion to the settings
/// default *model* (PROTOCOL §5.5).
#[tokio::test]
async fn catalog_default_model_suppresses_the_settings_default_effort() {
    let (_t, svc, ws, _spec, _cfg) = setup().await;
    svc.models_catalog.store_for_test(
        "auggie",
        crate::model_catalog::AUGGIE_CATALOG_VERSION,
        vec![
            json!({ "id": "fable-5", "name": "Fable 5", "provider": "auggie",
                     "effortLevels": ["low", "high"], "isDefault": true }),
        ],
    );
    set(&svc, "model.defaultReasoningEffort", json!("high"));

    let id = create(&svc, &ws, None, None, AgentCreateExtra::default()).await;
    let session = svc.agent_get_session_op(id).await.expect("get session");
    assert_eq!(session.model.as_deref(), Some("fable-5"));
    assert_eq!(
        session.reasoning_effort, None,
        "settings default effort must not companion a catalog-default model"
    );
}

/// `specialist.list` previews the catalog default through the same resolver
/// (`resolvedModel`), so the preview matches what a no-model create pins.
#[tokio::test]
async fn specialist_preview_reports_the_catalog_default() {
    use intent_core::WorkspaceApi;
    let (_t, svc, _ws, spec_dir, _cfg) = setup().await;
    seed_catalog_with_default(&svc);
    write_specialist(spec_dir.path(), "plain", "");

    let listed = svc.specialist_list(None, None).await.expect("list");
    let specs = listed["specialists"].as_array().expect("specialists");
    let plain = specs
        .iter()
        .find(|d| d["id"] == "plain")
        .expect("plain listed");
    assert_eq!(plain["resolvedModel"], json!("sonnet5"));
    assert_eq!(plain["resolvedProvider"], json!("auggie"));
}
