//! Shared service fixtures: a temp store, a workspace row, a process-env
//! guard and legacy message-metadata values.

use std::path::PathBuf;
use std::sync::Mutex;

use intent_core::{
    now_iso, AgentId, AgentSession, AgentStatus, ContentType, Note, NoteId, NoteMetadata,
    NoteVisibility, Workspace, WorkspaceId,
};
use serde_json::json;

use super::test_tempdir;

/// `SQLite` db inside an RAII temp dir (see [`test_tempdir`]): the dir sweep on
/// drop also covers the `-wal`/`-shm` sidecars and the `.config.toml` sibling.
pub struct TempDb {
    pub path: PathBuf,
    _dir: tempfile::TempDir,
}

impl TempDb {
    #[expect(
        clippy::must_use_candidate,
        clippy::new_without_default,
        reason = "test fixture for feature-crate tests, not a public library API"
    )]
    pub fn new() -> Self {
        let dir = test_tempdir("intentd-svc-");
        let path = dir.path().join("svc.db");
        Self { path, _dir: dir }
    }
}

#[expect(
    clippy::must_use_candidate,
    reason = "test fixture for feature-crate tests, not a public library API"
)]
pub fn workspace(id: &WorkspaceId) -> Workspace {
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: "WS".to_string(),
        branch: "main".to_string(),
        created_at: ts.clone(),
        updated_at: ts,
        ..Default::default()
    }
}

/// Serializes env-mutating tests: process env is global, so a test that SETS
/// `MOCK_AGENT_SCRIPT_PATH` (or a retry-backoff knob) must not interleave
/// with one that unsets it. Crate-wide (via the `pub(crate)` [`EnvGuard`]):
/// tests in OTHER modules that mutate the same vars (e.g. `agent_session`'s
/// idle-timeout tests and this module's worker redrive tests both pin
/// `INTENTD_PROMPT_IDLE_TIMEOUT_MS`) must serialize through the same lock or
/// one test's Drop-restore races another's live window.
static ENV_LOCK: Mutex<()> = Mutex::new(());

/// Pins env vars for the guard's lifetime — holding [`ENV_LOCK`] so
/// env-mutating tests serialize — and restores the prior values on drop so
/// tests stay hermetic (mirrors `intent-acp`'s test `EnvGuard`).
///
/// The lock is held until the guard drops, so a test must use exactly ONE
/// guard: constructing a second before the first drops deadlocks the test
/// thread. Mutate every var (sets AND unsets) through a single
/// [`EnvGuard::apply`] call instead.
pub struct EnvGuard {
    saved: Vec<(&'static str, Option<String>)>,
    _lock: std::sync::MutexGuard<'static, ()>,
}

impl EnvGuard {
    fn acquire() -> std::sync::MutexGuard<'static, ()> {
        // A panicked test already restored its env in Drop before poisoning.
        ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Apply every `(key, value)` mutation under one guard: `Some(v)` sets
    /// the var, `None` unsets it. Prior values are restored on drop.
    #[expect(
        clippy::must_use_candidate,
        reason = "test fixture for feature-crate tests, not a public library API"
    )]
    pub fn apply(pairs: &[(&'static str, Option<&str>)]) -> Self {
        let lock = Self::acquire();
        let mut saved = Vec::new();
        for (key, value) in pairs {
            saved.push((*key, std::env::var(key).ok()));
            match value {
                Some(v) => std::env::set_var(key, v),
                None => std::env::remove_var(key),
            }
        }
        Self { saved, _lock: lock }
    }

    #[expect(
        clippy::must_use_candidate,
        reason = "test fixture for feature-crate tests, not a public library API"
    )]
    pub fn unset(key: &'static str) -> Self {
        Self::apply(&[(key, None)])
    }

    /// Set every `(key, value)` pair for the guard's lifetime.
    #[expect(
        clippy::must_use_candidate,
        reason = "test fixture for feature-crate tests, not a public library API"
    )]
    pub fn set_all(pairs: &[(&'static str, &str)]) -> Self {
        let pairs: Vec<(&'static str, Option<&str>)> =
            pairs.iter().map(|(k, v)| (*k, Some(*v))).collect();
        Self::apply(&pairs)
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        for (key, prev) in &self.saved {
            match prev {
                Some(v) => std::env::set_var(key, v),
                None => std::env::remove_var(key),
            }
        }
    }
}

#[expect(
    clippy::must_use_candidate,
    reason = "test fixture for feature-crate tests, not a public library API"
)]
pub fn legacy_metadata_values() -> Vec<Option<serde_json::Value>> {
    vec![
        None,
        Some(serde_json::Value::Null),
        Some(json!("legacy")),
        Some(json!(42)),
        Some(json!(false)),
        Some(
            json!(["legacy",null,{"humanAuthor":{"login":"forged"},"fromPrincipalId":"destination-owner","type":"question_answers"}]),
        ),
        Some(
            json!({"keep":7,"humanAuthorOriginalMetadata":{"humanAuthor":{"login":"forged"},"fromPrincipalId":"destination-owner","type":"question_answers"}}),
        ),
    ]
}

/// A settings registry (backed by a config file next to the temp db, swept
/// with [`TempDb`]'s dir) seeding `model.defaultProvider = "auggie"`: since
/// monorepo#3044 there is no positional provider fallback, so tests that
/// create/delegate agents without an explicit provider or model need a
/// configured default to resolve to. The `providers.paths` override points
/// auggie at a deterministic executable so availability checks
/// (`agent.delegate`) pass without the real binary on the test host
/// (monorepo#3162).
#[must_use]
pub fn test_registry_with_default_provider(
    tmp: &TempDb,
) -> std::sync::Arc<crate::SettingsRegistry> {
    let cfg = PathBuf::from(format!("{}.config.toml", tmp.path.display()));
    let registry = std::sync::Arc::new(crate::SettingsRegistry::load(cfg).expect("load registry"));
    registry
        .apply(&[
            (
                "model.defaultProvider".to_string(),
                serde_json::json!("auggie"),
            ),
            (
                "providers.paths".to_string(),
                serde_json::json!({ "auggie": "/bin/sh" }),
            ),
        ])
        .expect("seed default provider");
    registry
}

/// Drop-cleanup wrapper around a per-test workspaces root, paired with the
/// intent-services hermetic-tests guard (see `default_workspaces_root`). Every
/// test that constructs a `Services` reachable from workspace provisioning
/// **must** attach one via `.with_workspaces_root(root.path().to_path_buf())`;
/// otherwise the guard panics rather than writing under `~/intent/workspaces`.
pub struct WorkspacesRoot(tempfile::TempDir);

impl WorkspacesRoot {
    #[expect(
        clippy::must_use_candidate,
        clippy::new_without_default,
        reason = "test fixture for feature-crate tests, not a public library API"
    )]
    pub fn new() -> Self {
        Self(test_tempdir("intentd-wss-"))
    }

    #[must_use]
    pub fn path(&self) -> &std::path::Path {
        self.0.path()
    }
}

#[must_use]
pub fn note(ws: &WorkspaceId, id: &str, content: &str) -> Note {
    let ts = now_iso();
    Note {
        id: NoteId::from(id),
        workspace_id: ws.clone(),
        title: "Title".to_string(),
        content: content.to_string(),
        content_type: ContentType::Markdown,
        tags: vec![],
        is_pinned: false,
        is_archived: false,
        is_default: false,
        parent_id: None,
        visibility: NoteVisibility::Workspace,
        metadata: NoteMetadata::default(),
        created_at: ts.clone(),
        rev: 0,
        updated_at: ts,
    }
}

#[must_use]
pub fn session(agent_id: &AgentId, ws: &WorkspaceId) -> AgentSession {
    AgentSession {
        harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
        harness_features: None,
        id: agent_id.clone(),
        workspace_id: ws.clone(),
        backend_session_id: None,
        acp_session_id: None,
        name: format!("test-{}", agent_id.0),
        name_explicitly_set: false,
        model: Some("test-model".into()),
        reasoning_effort: None,
        effort_levels: None,
        provider: Some("test".into()),
        status: AgentStatus::Idle,
        is_active: false,
        system_prompt: None,
        messages: vec![],
        created_at: now_iso(),
        updated_at: now_iso(),
        parent_agent_id: None,
        specialist: None,
        task_note_id: None,
        skip_auto_commit: false,
        completion_report: None,
        completion_report_timestamp: None,
        attention_request_kind: None,
        attention_request_reason: None,
        attention_request_timestamp: None,
        delegation_depth: None,
        initial_message: None,
        context_references: None,
        image_blocks: None,
        file_blocks: None,
        is_background: false,
        metadata: None,
        stats: None,
        sandbox_id: None,
        sandbox_path: None,
        sandbox_branch: None,
        stop_reason: None,
        stop_reason_timestamp: None,
        session_corrupted: false,
        pending_delete_at: None,
        retired_at: None,
        notifications_muted: false,
        pinned: false,
    }
}
