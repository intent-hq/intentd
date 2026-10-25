//! Shared service fixtures: a temp store, a workspace row, a process-env
//! guard and legacy message-metadata values.

use std::path::PathBuf;
use std::sync::Mutex;

use intent_core::{
    now_iso, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceId, WorkspaceStatus,
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
        base_ref: None,
        base_commit_sha: None,
        status: WorkspaceStatus::Active,
        status_message: None,
        status_image_asset_id: None,
        activity: WorkspaceActivity::Idle,
        attention: WorkspaceAttention::None,
        attention_reminder: None,
        created_at: ts.clone(),
        updated_at: ts,
        last_activity: None,
        last_content_activity: None,
        tags: vec![],
        path: None,
        repository_path: None,
        repository_owner: None,
        repository_name: None,
        worktree_path: None,
        scope: None,
        skip_worktree: false,
        setup_script: None,
        is_remote: false,
        default_model: None,
        pr_number: None,
        pr_url: None,
        pr_status: None,
        active_pull_request: None,
        pull_requests: None,
        context_links: None,
        archived: false,
        archived_at: None,
        retired_at: None,
        task_stats: None,
        agent_summary: None,
        diff_summary: None,
        token_usage: None,
        cow_supported: None,
        browser_client_id: None,
        pull_requests_total: None,
        display_status: None,
        waiting: false,
        scripts_running: false,
        checkout_mode: None,
        disk_usage: None,
        pending_delete_at: None,
        membership: None,
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
