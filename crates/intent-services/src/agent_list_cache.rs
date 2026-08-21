//! Cached `agent.list` message projections with event-driven invalidation.
//!
//! ## Why
//!
//! Focus/menu refresh fans out `workspace.list` → `agent.list` per workspace.
//! Each `agent.list` re-runs the per-workspace message projection
//! ([`Store::get_agent_session_message_projections`]): a bounded aggregate
//! `COUNT` over `agent_message` joined to every session in the workspace,
//! plus the persisted last-user/assistant preview columns (monorepo#958,
//! 0066). Bounded, but still a full-workspace aggregate repeated for every
//! workspace on each focus burst; on large workspaces it dominates a
//! post-focus `sample(1)` of intentd.
//!
//! Session metadata (`list_agent_session_summaries`) stays uncached so
//! status/name edits stay live. Only the message projection map is cached.
//!
//! ## Coherency
//!
//! - **Invalidate on transcript write** (primary): every services path that
//!   writes `agent_message` (append, replace/truncate rewrite) bumps the
//!   workspace epoch and drops the cached map.
//! - **Invalidate on session create/delete**: new empty agents and removals
//!   must appear/disappear even with no messages.
//! - **Epoch guard**: in-flight loads capture the epoch at start and only
//!   write if it still matches, so a late completion cannot repopulate after
//!   invalidate (same pattern as [`crate::workspace_aggregates`]).
//! - **Best-effort single-flight** per workspace: one loader claims the slot
//!   and fills the cache; concurrent losers fall through to their own direct
//!   load (never blocking behind the winner), so a fully cold cache can still
//!   run a handful of identical queries once. The claim bounds cache-fill
//!   churn rather than hard-deduplicating queries.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use intent_core::{Result, WorkspaceId};
use intent_store::{SessionMessageProjection, Store};

/// Per-workspace cache state: the invalidation epoch and the cached
/// projection map live in ONE entry so they cannot desynchronize.
///
/// Invalidation clears `projections` but never removes the entry: the epoch
/// must outlive the cached data, because a state that were removed and later
/// recreated would restart at epoch 0 — a previously observable value — and
/// a stale in-flight load that captured that epoch could then pass the
/// [`AgentListProjectionCache::store_if_current`] guard and repopulate the
/// cache with pre-invalidate data. The residual cost is one `(String, u64)`
/// per workspace ever invalidated, bounded by the workspace count.
struct WorkspaceState {
    epoch: u64,
    projections: Option<HashMap<String, SessionMessageProjection>>,
}

/// RAII single-flight slot: removes the key on drop (panic / cancel safe).
struct InFlightGuard {
    set: Arc<Mutex<HashSet<String>>>,
    key: String,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.set.lock().unwrap().remove(&self.key);
    }
}

/// Shared projection cache held as an `Arc` field on [`crate::Services`].
pub(crate) struct AgentListProjectionCache {
    states: Mutex<HashMap<String, WorkspaceState>>,
    in_flight: Arc<Mutex<HashSet<String>>>,
}

impl AgentListProjectionCache {
    pub(crate) fn new() -> Self {
        Self {
            states: Mutex::new(HashMap::new()),
            in_flight: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// Drop the cached projection map for `workspace_id` and bump its epoch
    /// so any in-flight load is discarded on completion. The state entry
    /// (and its epoch) is retained — see [`WorkspaceState`] for why removal
    /// would break the epoch guard.
    pub(crate) fn invalidate(&self, workspace_id: &str) {
        let mut states = self.states.lock().unwrap();
        let state = states
            .entry(workspace_id.to_string())
            .or_insert(WorkspaceState {
                epoch: 0,
                projections: None,
            });
        state.epoch = state.epoch.wrapping_add(1);
        state.projections = None;
    }

    fn current_epoch(&self, workspace_id: &str) -> u64 {
        self.states
            .lock()
            .unwrap()
            .get(workspace_id)
            .map_or(0, |s| s.epoch)
    }

    fn lookup(
        &self,
        workspace_id: &str,
        epoch: u64,
    ) -> Option<HashMap<String, SessionMessageProjection>> {
        let guard = self.states.lock().unwrap();
        let state = guard.get(workspace_id)?;
        if state.epoch != epoch {
            return None;
        }
        state.projections.clone()
    }

    fn store_if_current(
        &self,
        workspace_id: &str,
        epoch: u64,
        projections: HashMap<String, SessionMessageProjection>,
    ) {
        let mut states = self.states.lock().unwrap();
        match states.get_mut(workspace_id) {
            Some(state) => {
                if state.epoch != epoch {
                    return;
                }
                state.projections = Some(projections);
            }
            None => {
                // No state yet means the workspace was never invalidated, so
                // only the implicit epoch 0 may publish.
                if epoch != 0 {
                    return;
                }
                states.insert(
                    workspace_id.to_string(),
                    WorkspaceState {
                        epoch: 0,
                        projections: Some(projections),
                    },
                );
            }
        }
    }

    /// Serve the projection map from cache or load it once (single-flight).
    /// Concurrent waiters that lose the race fall through to a direct load so
    /// they never block behind a slow winner; the winner still fills the cache
    /// for subsequent calls.
    pub(crate) async fn get_or_load(
        &self,
        store: &Store,
        workspace_id: &WorkspaceId,
    ) -> Result<HashMap<String, SessionMessageProjection>> {
        let key = workspace_id.0.as_str();
        let epoch = self.current_epoch(key);
        if let Some(hit) = self.lookup(key, epoch) {
            return Ok(hit);
        }

        let claimed = {
            let mut set = self.in_flight.lock().unwrap();
            if set.insert(key.to_string()) {
                Some(InFlightGuard {
                    set: Arc::clone(&self.in_flight),
                    key: key.to_string(),
                })
            } else {
                None
            }
        };

        // Re-check after claiming — another task may have finished between the
        // first lookup and the single-flight insert.
        let epoch = self.current_epoch(key);
        if let Some(hit) = self.lookup(key, epoch) {
            drop(claimed);
            return Ok(hit);
        }

        let loaded = store
            .get_agent_session_message_projections(workspace_id)
            .await?;

        if claimed.is_some() {
            self.store_if_current(key, epoch, loaded.clone());
        } else if let Some(hit) = self.lookup(key, self.current_epoch(key)) {
            // Loser: prefer a freshly published cache entry if the winner won.
            return Ok(hit);
        }

        drop(claimed);
        Ok(loaded)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use intent_core::{
        now_iso, AgentId, AgentSession, AgentStatus, Workspace, WorkspaceActivity,
        WorkspaceAttention, WorkspaceId, WorkspaceStatus,
    };
    use intent_store::Store;

    struct TempDb {
        _dir: tempfile::TempDir,
        path: std::path::PathBuf,
    }

    impl TempDb {
        fn new() -> Self {
            let dir = tempfile::TempDir::new().expect("tempdir");
            let path = dir.path().join("test.db");
            Self { _dir: dir, path }
        }
    }

    fn workspace(id: &WorkspaceId) -> Workspace {
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
            created_at: ts.clone(),
            updated_at: ts,
            last_activity: None,
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
            archived: false,
            archived_at: None,
            task_stats: None,
            agent_summary: None,
            diff_summary: None,
            token_usage: None,
            cow_supported: None,
            display_status: None,
            waiting: false,
            checkout_mode: None,
            disk_usage: None,
            pending_delete_at: None,
        }
    }

    async fn open_store() -> (Store, TempDb) {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open");
        (store, tmp)
    }

    fn session(ws: &WorkspaceId, id: &str) -> AgentSession {
        let ts = now_iso();
        AgentSession {
            harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
            harness_features: None,
            id: AgentId(id.to_string()),
            workspace_id: ws.clone(),
            backend_session_id: None,
            acp_session_id: None,
            name: id.to_string(),
            name_explicitly_set: false,
            model: None,
            reasoning_effort: None,
            effort_levels: None,
            provider: None,
            status: AgentStatus::Idle,
            is_active: false,
            system_prompt: None,
            created_at: ts.clone(),
            updated_at: ts,
            messages: vec![],
            parent_agent_id: None,
            specialist: None,
            task_note_id: None,
            skip_auto_commit: false,
            stats: None,
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
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
            stop_reason: None,
            stop_reason_timestamp: None,
            session_corrupted: false,
            pending_delete_at: None,
        }
    }

    async fn seed_agent(store: &Store, ws: &WorkspaceId, id: &str) {
        store.insert_workspace(&workspace(ws)).await.ok(); // may already exist
        store
            .insert_agent_session(&session(ws, id))
            .await
            .expect("create agent");
    }

    #[tokio::test]
    async fn cache_hit_retains_entry_after_warm() {
        let (store, _tmp) = open_store().await;
        let ws = WorkspaceId("ws-cache-1".into());
        seed_agent(&store, &ws, "a1").await;
        let content = serde_json::json!([{ "type": "text", "text": "hello world" }]);
        store
            .append_agent_message(&AgentId("a1".into()), "user", &content, &now_iso())
            .await
            .expect("append");

        let cache = AgentListProjectionCache::new();
        let first = cache.get_or_load(&store, &ws).await.expect("load1");
        assert_eq!(first.get("a1").map(|p| p.message_count), Some(1));

        let second = cache.get_or_load(&store, &ws).await.expect("load2");
        assert_eq!(first, second);
        assert!(
            cache.lookup(&ws.0, 0).is_some(),
            "entry retained at epoch 0"
        );
    }

    #[tokio::test]
    async fn invalidate_forces_reload_of_new_message() {
        let (store, _tmp) = open_store().await;
        let ws = WorkspaceId("ws-cache-2".into());
        seed_agent(&store, &ws, "a1").await;
        let cache = AgentListProjectionCache::new();
        let empty = cache.get_or_load(&store, &ws).await.expect("load empty");
        assert_eq!(empty.get("a1").map(|p| p.message_count), Some(0));

        let content = serde_json::json!([{ "type": "text", "text": "after" }]);
        store
            .append_agent_message(&AgentId("a1".into()), "user", &content, &now_iso())
            .await
            .expect("append");
        // Without invalidate, cache would still show message_count=0.
        assert_eq!(
            cache
                .get_or_load(&store, &ws)
                .await
                .expect("stale hit")
                .get("a1")
                .map(|p| p.message_count),
            Some(0)
        );

        cache.invalidate(&ws.0);
        let fresh = cache.get_or_load(&store, &ws).await.expect("reload");
        assert_eq!(fresh.get("a1").map(|p| p.message_count), Some(1));
        let blocks = fresh
            .get("a1")
            .and_then(|p| p.last_user_text_blocks.as_ref());
        assert_eq!(blocks, Some(&vec!["after".to_string()]));
    }

    #[tokio::test]
    async fn epoch_guard_drops_stale_in_flight_write() {
        let cache = AgentListProjectionCache::new();
        let ws = "ws-epoch";
        assert_eq!(cache.current_epoch(ws), 0);
        cache.invalidate(ws);
        assert_eq!(cache.current_epoch(ws), 1);
        let mut map = HashMap::new();
        map.insert(
            "a".into(),
            SessionMessageProjection {
                message_count: 9,
                ..Default::default()
            },
        );
        cache.store_if_current(ws, 0, map);
        assert!(
            cache.lookup(ws, 1).is_none(),
            "stale epoch-0 write must not land"
        );
    }
}
