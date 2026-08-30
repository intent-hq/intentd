//! Functional end-to-end round-trip test for the workspace transfer relay:
//! two in-process daemon service stacks (two Stores, two temp roots) relay a
//! seeded workspace exactly the way the FE will — `transfer.plan` →
//! `export.start` → chunked `export.read` on the source, piped into
//! `import.begin`/`chunk`/`commit` on the target, then `export.finalize`
//! back on the source — plus a mid-relay abort case proving both sides
//! clean up.

use std::path::{Path, PathBuf};

use intent_core::transfer::TransferManifest;
use intent_core::{
    now_iso, AgentId, AgentSession, AgentStatus, ClientId, Error, Hook, HookId, HookState, Note,
    NoteId, PrMonitor, PrMonitorId, PrMonitorState, Script, ScriptMode, WorkspaceId,
    WorkspaceStatus,
};
use intent_store::{AgentQueueRow, PersistedEventSubscription, Sandbox, SandboxStatus, Store};

use crate::transfer_export::ExportState;
use crate::Services;

struct TempDir(PathBuf);
impl TempDir {
    fn new(prefix: &str) -> Self {
        let p = std::env::temp_dir().join(format!("{prefix}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&p).expect("mkdir");
        Self(p)
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// One in-process daemon stack: its own `SQLite` store, workspaces root, and
/// assets root — the same wiring the export/import unit suites use.
async fn fresh_services(workspaces_root: &Path, assets_root: &Path) -> Services {
    let db = std::env::temp_dir().join(format!("roundtrip-test-{}.db", uuid::Uuid::new_v4()));
    let store = Store::open(&db).await.expect("open store");
    Services::new(store)
        .with_workspaces_root(workspaces_root.to_path_buf())
        .with_assets_root(assets_root.to_path_buf())
}

fn session(agent_id: &AgentId, ws: &WorkspaceId, status: AgentStatus) -> AgentSession {
    AgentSession {
        harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
        harness_features: None,
        id: agent_id.clone(),
        workspace_id: ws.clone(),
        backend_session_id: None,
        acp_session_id: None,
        name: "a".to_string(),
        name_explicitly_set: false,
        model: None,
        reasoning_effort: None,
        effort_levels: None,
        provider: None,
        status,
        is_active: matches!(status, AgentStatus::Active),
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
    }
}

fn note(ws: &WorkspaceId, id: &str, content: &str) -> Note {
    Note {
        id: NoteId::from(id),
        workspace_id: ws.clone(),
        title: "N".to_string(),
        content: content.to_string(),
        content_type: intent_core::ContentType::Markdown,
        tags: vec![],
        is_pinned: false,
        is_archived: false,
        is_default: false,
        parent_id: None,
        visibility: intent_core::NoteVisibility::Workspace,
        metadata: intent_core::NoteMetadata::default(),
        created_at: now_iso(),
        rev: 0,
        updated_at: now_iso(),
    }
}

// ---- git fixtures ---------------------------------------------------------

fn init_repo(repo_path: &Path) {
    std::fs::create_dir_all(repo_path).unwrap();
    let repo = git2::Repository::init_opts(
        repo_path,
        git2::RepositoryInitOptions::new().initial_head("main"),
    )
    .unwrap();
    std::fs::write(repo_path.join("README.md"), "hello\n").unwrap();
    let sig = git2::Signature::now("Test", "test@example.com").unwrap();
    let tree_id = {
        let mut index = repo.index().unwrap();
        index.add_path(Path::new("README.md")).unwrap();
        index.write().unwrap();
        index.write_tree().unwrap()
    };
    let tree = repo.find_tree(tree_id).unwrap();
    repo.commit(Some("HEAD"), &sig, &sig, "Initial commit", &tree, &[])
        .unwrap();
}

fn commit_file(repo_path: &Path, file: &str, content: &str, message: &str) -> String {
    let repo = git2::Repository::open(repo_path).unwrap();
    std::fs::write(repo_path.join(file), content).unwrap();
    let mut index = repo.index().unwrap();
    index.add_path(Path::new(file)).unwrap();
    index.write().unwrap();
    let tree_oid = index.write_tree().unwrap();
    let tree = repo.find_tree(tree_oid).unwrap();
    let sig = git2::Signature::now("Test", "test@example.com").unwrap();
    let parent = repo.head().unwrap().peel_to_commit().unwrap();
    repo.commit(Some("HEAD"), &sig, &sig, message, &tree, &[&parent])
        .unwrap()
        .to_string()
}

fn repo_head(repo_path: &Path) -> String {
    let repo = git2::Repository::open(repo_path).unwrap();
    let head = repo.head().unwrap().target().unwrap().to_string();
    head
}

fn is_untracked(repo_path: &Path, file: &str) -> bool {
    let repo = git2::Repository::open(repo_path).unwrap();
    let statuses = repo
        .statuses(Some(git2::StatusOptions::new().include_untracked(true)))
        .unwrap();
    statuses
        .iter()
        .any(|s| s.path().unwrap_or_default() == file && s.status() == git2::Status::WT_NEW)
}

/// Wait until the background export build settles (Ready) or the session
/// disappears (failed build). Returns true when Ready.
async fn wait_ready(svc: &Services, export_id: &str) -> bool {
    for _ in 0..400 {
        {
            let exports = svc.transfer_exports.lock().unwrap();
            match exports.get(export_id) {
                Some(s) if matches!(s.state, ExportState::Ready(_)) => return true,
                Some(_) => {}
                None => return false,
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    false
}

/// Pull the sealed archive metadata off a Ready session: (size, sha, manifest).
fn ready_meta(svc: &Services, export_id: &str) -> (u64, String, TransferManifest) {
    let exports = svc.transfer_exports.lock().unwrap();
    match &exports.get(export_id).expect("session").state {
        ExportState::Ready(r) => (r.size_bytes, r.sha256.clone(), r.manifest.clone()),
        ExportState::Building { .. } => panic!("session not ready"),
    }
}

/// What the seed produced (paths the assertions need).
struct Seeded {
    repo: PathBuf,
    sandbox: PathBuf,
    main_tip: String,
    sandbox_tip: String,
}

const AGENT_LIVE: &str = "agent-live";
const AGENT_IDLE: &str = "agent-idle";
const AGENT_SB: &str = "agent-sb";

/// Seed the source stack with the full transfer inventory: two notes, three
/// agents (one in-flight with nulled-on-import session ids, one with message
/// history + a queued message, one owning the sandbox), a hook, an event
/// subscription, a PR monitor, a script with an absolute cwd, a draft, an
/// asset, some events, a dirty git worktree, and one dirty sandbox clone.
async fn seed_source(
    svc: &Services,
    ws_root: &Path,
    assets_root: &Path,
    id: &WorkspaceId,
) -> Seeded {
    let t = now_iso();

    // Dirty workspace repo on `main`: one extra commit, one staged file, one
    // untracked file.
    let repo = ws_root.join(&id.0).join("repo");
    init_repo(&repo);
    let main_tip = commit_file(&repo, "second.txt", "second\n", "feat: second commit");
    std::fs::write(repo.join("staged.txt"), "staged\n").unwrap();
    {
        let r = git2::Repository::open(&repo).unwrap();
        let mut index = r.index().unwrap();
        index.add_path(Path::new("staged.txt")).unwrap();
        index.write().unwrap();
    }
    std::fs::write(repo.join("dirty.txt"), "uncommitted\n").unwrap();

    // Sandbox clone on `sb/agent-sb` with one commit and one dirty file.
    let sb_branch = format!("sb/{AGENT_SB}");
    let sandbox = ws_root
        .join(&id.0)
        .join("sandboxes")
        .join(AGENT_SB)
        .join("sb-clone");
    std::fs::create_dir_all(sandbox.parent().unwrap()).unwrap();
    let out = std::process::Command::new("git")
        .arg("clone")
        .arg("--quiet")
        .arg(&repo)
        .arg(&sandbox)
        .output()
        .unwrap();
    assert!(out.status.success(), "sandbox clone failed");
    {
        let r = git2::Repository::open(&sandbox).unwrap();
        let head = r.head().unwrap().peel_to_commit().unwrap();
        r.branch(&sb_branch, &head, false).unwrap();
        r.set_head(&format!("refs/heads/{sb_branch}")).unwrap();
    }
    let sandbox_tip = commit_file(&sandbox, "sb.txt", "sandbox work\n", "feat: sandbox commit");
    std::fs::write(sandbox.join("sb-dirty.txt"), "sandbox wip\n").unwrap();

    let mut ws = crate::tests::workspace(id);
    ws.repository_path = Some(repo.to_string_lossy().into_owned());
    ws.repository_name = Some("test-repo".to_string());
    svc.store.insert_workspace(&ws).await.expect("workspace");

    svc.store
        .insert_note(&note(id, "note-spec", "the spec"))
        .await
        .expect("note 1");
    svc.store
        .insert_note(&note(id, "note-progress", "progress log"))
        .await
        .expect("note 2");

    // Agents: in-flight (with live session ids), idle with history + queue,
    // and the sandbox owner.
    let live = AgentId::from(AGENT_LIVE);
    let mut live_session = session(&live, id, AgentStatus::Active);
    live_session.acp_session_id = Some("acp-live".to_string());
    live_session.backend_session_id = Some(AgentId::from("backend-live"));
    svc.store
        .insert_agent_session(&live_session)
        .await
        .expect("live session");
    let idle = AgentId::from(AGENT_IDLE);
    svc.store
        .insert_agent_session(&session(&idle, id, AgentStatus::RuntimeIdle))
        .await
        .expect("idle session");
    let sb_agent = AgentId::from(AGENT_SB);
    svc.store
        .insert_agent_session(&session(&sb_agent, id, AgentStatus::RuntimeIdle))
        .await
        .expect("sb session");

    // Message history + a queued message (the queue rides the archive and
    // rehydrates on the target).
    for (agent, role, text) in [
        (&idle, "user", "hello"),
        (&idle, "assistant", "hi there"),
        (&live, "user", "do the thing"),
    ] {
        svc.store
            .append_agent_message(
                agent,
                role,
                &serde_json::json!([{ "type": "text", "text": text }]),
                &t,
            )
            .await
            .expect("message");
    }
    svc.store
        .replace_agent_queue(
            &live,
            &[AgentQueueRow {
                id: "qm-1".to_string(),
                agent_id: live.clone(),
                position: 0,
                payload: serde_json::json!({
                    "id": "qm-1", "turnId": "qm-1",
                    "content": "queued while busy", "queuedAt": t
                }),
                created_at: t.clone(),
                turn_id: "qm-1".to_string(),
            }],
        )
        .await
        .expect("queue");

    // Hook (scheduled, far-future expiry so target rehydration resumes it).
    svc.store
        .insert_hook(&Hook {
            hook_id: HookId::from("hook-rt"),
            workspace_id: id.clone(),
            agent_id: idle.clone(),
            name: "watcher".to_string(),
            code: "return { dispatch: false }".to_string(),
            delay_ms: 10_000,
            cron: None,
            run_at: None,
            state: HookState::Scheduled,
            created_at: t.clone(),
            last_run_at: None,
            next_run_at: Some("2999-01-01T00:00:00Z".to_string()),
            run_count: 0,
            last_error: None,
            last_logs: None,
            last_state: None,
            expires_at: Some("2999-01-01T00:30:00Z".to_string()),
            perpetual: false,
            dispatch_count: 0,
        })
        .await
        .expect("hook");

    // Event subscription + PR monitor + script (absolute cwd → rewritten).
    svc.store
        .upsert_event_subscription(&PersistedEventSubscription {
            id: "sub-rt".to_string(),
            workspace_id: id.clone(),
            subscriber_agent_id: idle.clone(),
            event_types: vec!["note:*".to_string()],
            exclude_self: true,
            batch_window_ms: 500,
            created_at: t.clone(),
        })
        .await
        .expect("subscription");
    svc.store
        .insert_pr_monitor(&PrMonitor {
            monitor_id: PrMonitorId::from("prmon-rt"),
            workspace_id: id.clone(),
            agent_id: idle.clone(),
            repo_owner: "intent-hq".to_string(),
            repo_name: "intentd".to_string(),
            pr_number: 42,
            state: PrMonitorState::Active,
            last_snapshot: None,
            baseline_snapshot: None,
            pending_changes: vec![],
            pending_since: None,
            last_change_at: None,
            last_polled_at: None,
            last_error: None,
            created_at: t.clone(),
            updated_at: t.clone(),
        })
        .await
        .expect("pr monitor");
    svc.store
        .upsert_script(&Script {
            id: "script-rt".to_string(),
            workspace_id: id.0.clone(),
            name: "build".to_string(),
            command: "true".to_string(),
            cwd: Some(repo.to_string_lossy().into_owned()),
            env: None,
            mode: ScriptMode::Command,
            category: None,
            source: "user".to_string(),
            auto_start: None,
            created_at: t.clone(),
            updated_at: None,
        })
        .await
        .expect("script");

    // Sandbox row for the clone above.
    svc.store
        .insert_sandbox(&Sandbox {
            id: "sb-rt".to_string(),
            workspace_id: id.clone(),
            agent_id: sb_agent.clone(),
            path: sandbox.to_string_lossy().into_owned(),
            branch: sb_branch,
            base_commit_sha: main_tip.clone(),
            snapshot_commit_sha: None,
            status: SandboxStatus::Created,
            retry_count: 0,
            created_at: t.clone(),
            updated_at: t.clone(),
        })
        .await
        .expect("sandbox");

    // Draft (FKs onto client; dropped by the import transform).
    let client = ClientId::from("client-rt");
    svc.store
        .upsert_client(&client, None, None)
        .await
        .expect("client");
    svc.store
        .upsert_draft(id, &live, &client, "half-typed message", None)
        .await
        .expect("draft");

    // One asset + events (events must NOT transfer).
    let dir = assets_root.join(&id.0);
    std::fs::create_dir_all(&dir).expect("assets dir");
    std::fs::write(dir.join("img.png"), b"asset-bytes").expect("asset");

    // Two attachment-registry rows: one with its stored file present in the
    // git-ignored `.intent/attachments/` store, one whose file was deleted
    // out-of-band (deleted-is-deleted — the row transfers, no file rides).
    let att_dir = repo.join(".intent/attachments");
    std::fs::create_dir_all(&att_dir).expect("attachments dir");
    std::fs::write(att_dir.join(".gitignore"), "*\n").expect("marker");
    std::fs::write(att_dir.join("doc.pdf"), b"attachment-bytes").expect("attachment");
    for (att_id, name) in [("att-live", "doc.pdf"), ("att-gone", "gone.txt")] {
        svc.store
            .insert_attachment(&intent_store::AttachmentRecord {
                id: att_id.to_string(),
                workspace_id: id.clone(),
                file_name: name.to_string(),
                mime_type: None,
                size: 16,
                uploaded_at: t.clone(),
                stored_path: format!(".intent/attachments/{name}"),
            })
            .await
            .expect("attachment row");
    }
    for i in 0..3 {
        svc.store
            .insert_event(&intent_store::NewEvent {
                workspace_id: id.clone(),
                timestamp: t.clone(),
                event_type: format!("note:updated:{i}"),
                actor: crate::system_actor(),
                session_id: None,
                correlation_id: None,
                parent_event_id: None,
                metadata: None,
                data: serde_json::json!({ "i": i }),
            })
            .await
            .expect("event");
    }

    Seeded {
        repo,
        sandbox,
        main_tip,
        sandbox_tip,
    }
}

/// Relay one sealed export archive from `source` to `target` the way the FE
/// does: read every chunk off `workspace.export.read` and pipe it straight
/// into `workspace.import.chunk`. Returns the target's commit result.
async fn relay(
    source: &Services,
    target: &Services,
    export_id: &str,
    manifest: &TransferManifest,
    size: u64,
    sha: &str,
) -> serde_json::Value {
    let begin = target
        .workspace_import_begin_op(
            serde_json::to_value(manifest).expect("manifest json"),
            size,
            sha.to_string(),
        )
        .await
        .expect("import begin");
    let import_id = begin["importId"].as_str().expect("importId").to_string();

    let first = source
        .workspace_export_read_op(export_id.to_string(), 0)
        .await
        .expect("read 0");
    let total_chunks = first["totalChunks"].as_u64().expect("totalChunks");
    for seq in 0..total_chunks {
        let chunk = if seq == 0 {
            first.clone()
        } else {
            source
                .workspace_export_read_op(export_id.to_string(), seq)
                .await
                .expect("read chunk")
        };
        target
            .workspace_import_chunk_op(
                import_id.clone(),
                seq,
                chunk["data"].as_str().expect("chunk data").to_string(),
            )
            .await
            .expect("import chunk");
    }
    target
        .workspace_import_commit_op(import_id)
        .await
        .expect("import commit")
}

/// The full FE-shaped relay over two in-process stacks: plan on the source,
/// export → chunked read → staged import → commit on the target →
/// finalize on the source. Asserts the plan's size estimate is a sane
/// predictor of the actual archive, the target's per-table row counts
/// (events zero, drafts dropped), path rewrites, nulled ACP session ids,
/// interrupted-agent capture, git worktree + sandbox dirty state, the
/// rehydration counts, and the finalized (archived) source.
#[tokio::test]
async fn transfer_round_trip_between_two_stacks() {
    let src_ws_root = TempDir::new("rt-src-ws");
    let src_assets_root = TempDir::new("rt-src-assets");
    let dst_ws_root = TempDir::new("rt-dst-ws");
    let dst_assets_root = TempDir::new("rt-dst-assets");
    let source = fresh_services(&src_ws_root.0, &src_assets_root.0).await;
    let target = fresh_services(&dst_ws_root.0, &dst_assets_root.0).await;

    let id = WorkspaceId("ws-roundtrip".to_string());
    let seeded = seed_source(&source, &src_ws_root.0, &src_assets_root.0, &id).await;

    // ---- plan (source): the FE preview before starting -------------------
    let plan = source
        .workspace_transfer_plan_op(id.clone())
        .await
        .expect("plan");
    assert!(plan.manifest.git.has_repository);
    assert_eq!(plan.manifest.git.branch.as_deref(), Some("main"));
    assert!(plan
        .manifest
        .git
        .sandbox_branches
        .contains(&format!("sb/{AGENT_SB}")));
    assert!(!plan.manifest.git.dirty_files.is_empty());
    let plan_rows = |n: &str| {
        plan.manifest
            .tables
            .iter()
            .find(|t| t.name == n)
            .map_or(-1, |t| t.row_count)
    };
    assert_eq!(plan_rows("agent_session"), 3);
    assert_eq!(plan_rows("agent_message"), 3);
    assert_eq!(plan_rows("agent_queue"), 1);
    assert!(plan.total_size_bytes > 0);

    // ---- export (source) --------------------------------------------------
    let started = source
        .workspace_export_start_op(id.clone())
        .await
        .expect("export start");
    let export_id = started["exportId"].as_str().expect("exportId").to_string();
    assert!(wait_ready(&source, &export_id).await, "build must succeed");

    // Multi-chunk relay with a tiny archive: shrink the chunk budget.
    {
        let mut exports = source.transfer_exports.lock().unwrap();
        exports.get_mut(&export_id).unwrap().max_chunk_bytes = 4096;
    }
    let (size, sha, manifest) = ready_meta(&source, &export_id);
    assert!(size > 4096, "archive should span multiple chunks");

    // Plan estimate vs. actual archive size: within a sane factor. The
    // estimate skips zip compression and WIP snapshot deltas, so allow a
    // generous band rather than pinning a ratio.
    let estimate = plan.total_size_bytes;
    assert!(
        estimate >= size / 10 && estimate <= size.saturating_mul(10),
        "plan estimate {estimate} should be within 10x of actual archive {size}"
    );

    // ---- relay + commit (target) -------------------------------------------
    let committed = relay(&source, &target, &export_id, &manifest, size, &sha).await;

    // ---- target row counts --------------------------------------------------
    let stats = target.store.transfer_table_stats(&id).await.expect("stats");
    let rows = |n: &str| {
        stats
            .iter()
            .find(|s| s.name == n)
            .map_or(-1, |s| s.row_count)
    };
    assert_eq!(rows("workspace"), 1);
    assert_eq!(rows("note"), 2);
    assert_eq!(rows("agent_session"), 3);
    assert_eq!(rows("agent_message"), 3);
    assert_eq!(rows("agent_queue"), 1);
    assert_eq!(rows("hook"), 1);
    assert_eq!(rows("event_subscription"), 1);
    assert_eq!(rows("pr_monitor"), 1);
    assert_eq!(rows("script"), 1);
    assert_eq!(rows("sandbox"), 1);
    assert_eq!(rows("draft"), 0, "drafts never transfer");
    // Event history stays on the source.
    let events = target
        .store
        .query_events(&intent_store::EventQuery {
            workspace_id: Some(id.clone()),
            event_type_prefix: Some("note:".to_string()),
            ..Default::default()
        })
        .await
        .expect("target events");
    assert!(events.is_empty(), "events must not transfer: {events:?}");

    // ---- interrupted agents + session scrub ---------------------------------
    assert_eq!(
        committed["interruptedAgents"],
        serde_json::json!([AGENT_LIVE])
    );
    let live = target
        .store
        .get_agent_session(&AgentId::from(AGENT_LIVE))
        .await
        .expect("live session on target");
    assert_eq!(live.status, AgentStatus::RuntimeIdle);
    assert!(live.acp_session_id.is_none(), "acp session id nulled");
    assert!(
        live.backend_session_id.is_none(),
        "backend session id nulled"
    );
    assert!(!live.is_active);
    assert!(live
        .stop_reason
        .as_deref()
        .unwrap_or_default()
        .contains("transferred"));
    let interrupted = target
        .store
        .list_interrupted_agents()
        .await
        .expect("interrupted");
    assert_eq!(interrupted.len(), 1);
    assert_eq!(interrupted[0].agent_id.0, AGENT_LIVE);
    assert_eq!(interrupted[0].prev_status, "active");

    // ---- path rewrites + git materialization ---------------------------------
    let imported = target.store.get_workspace(&id).await.expect("workspace");
    let checkout = dst_ws_root.0.join(&id.0).join("test-repo");
    assert_eq!(
        imported.repository_path.as_deref(),
        Some(checkout.to_str().unwrap()),
        "repository re-rooted under the target workspaces root"
    );
    assert_eq!(imported.worktree_path, None);
    assert_eq!(imported.branch, "main");
    // Worktree: right tip, dirty state restored (WIP snapshot unwound).
    assert_eq!(repo_head(&checkout), seeded.main_tip);
    assert_eq!(
        std::fs::read_to_string(checkout.join("dirty.txt")).unwrap(),
        "uncommitted\n"
    );
    assert_eq!(
        std::fs::read_to_string(checkout.join("staged.txt")).unwrap(),
        "staged\n"
    );
    assert!(
        is_untracked(&checkout, "dirty.txt"),
        "dirty.txt untracked again"
    );

    // Sandbox: row rewritten, clone materialized with its commit + dirty file.
    let sandboxes = target.store.list_sandboxes(&id).await.expect("sandboxes");
    assert_eq!(sandboxes.len(), 1);
    let expected_sb = dst_ws_root
        .0
        .join(&id.0)
        .join("sandboxes")
        .join(AGENT_SB)
        .join("test-repo");
    assert_eq!(sandboxes[0].path, expected_sb.to_string_lossy());
    assert_eq!(repo_head(&expected_sb), seeded.sandbox_tip);
    assert_eq!(
        std::fs::read_to_string(expected_sb.join("sb-dirty.txt")).unwrap(),
        "sandbox wip\n"
    );

    // Script cwd re-rooted under the target workspace dir.
    let scripts = target.store.list_all_scripts().await.expect("scripts");
    let script = scripts
        .iter()
        .find(|s| s.id == "script-rt")
        .expect("script");
    let expected_cwd = dst_ws_root.0.join(&id.0).join("repo");
    assert_eq!(script.cwd.as_deref(), Some(expected_cwd.to_str().unwrap()));

    // Asset placed.
    assert_eq!(
        std::fs::read(dst_assets_root.0.join(&id.0).join("img.png")).expect("asset"),
        b"asset-bytes"
    );

    // Attachments: both registry rows landed; the live file materialized in
    // the re-rooted checkout's git-ignored store (with the ignore-all
    // marker), the deleted one imported as a row without a file.
    let atts = target
        .store
        .list_attachments(&id)
        .await
        .expect("attachments");
    assert_eq!(atts.len(), 2);
    let att_dir = checkout.join(".intent/attachments");
    assert_eq!(
        std::fs::read(att_dir.join("doc.pdf")).expect("attachment file"),
        b"attachment-bytes"
    );
    assert_eq!(
        std::fs::read_to_string(att_dir.join(".gitignore")).expect("marker"),
        "*\n"
    );
    assert!(!att_dir.join("gone.txt").exists(), "deleted-is-deleted");

    // ---- rehydration counts ---------------------------------------------------
    assert_eq!(committed["rehydrated"]["hooks"], 1);
    assert_eq!(committed["rehydrated"]["eventSubscriptions"], 1);
    assert_eq!(committed["rehydrated"]["prMonitors"], 1);
    assert_eq!(committed["rehydrated"]["agentQueues"], 1);

    // ---- finalize (source) ------------------------------------------------------
    let finalized = source
        .workspace_export_finalize_op(
            export_id.clone(),
            true,
            Some("Transferred to another machine".to_string()),
        )
        .await
        .expect("finalize");
    assert_eq!(finalized["finalized"], true);
    let src_ws = source.store.get_workspace(&id).await.expect("source ws");
    assert_eq!(src_ws.status, WorkspaceStatus::Archived);
    assert_eq!(
        src_ws.status_message.as_deref(),
        Some("Transferred to another machine")
    );
    assert!(
        !src_ws_root
            .0
            .join(".export-staging")
            .join(&export_id)
            .exists(),
        "export staging cleaned"
    );
    // Finalize unwound the source WIP snapshots: dirty state restored.
    assert_eq!(repo_head(&seeded.repo), seeded.main_tip);
    assert!(seeded.repo.join("dirty.txt").exists());
    assert!(is_untracked(&seeded.repo, "dirty.txt"));
    assert_eq!(repo_head(&seeded.sandbox), seeded.sandbox_tip);
    assert!(seeded.sandbox.join("sb-dirty.txt").exists());
}

/// Failure injection: abort the relay mid-chunk. The source stays intact
/// (its export abort unwinds WIP snapshots and cleans staging), and the
/// target's import abort cleans its staging with nothing committed.
#[tokio::test]
async fn transfer_abort_mid_relay_cleans_both_sides() {
    let src_ws_root = TempDir::new("rt-abort-src-ws");
    let src_assets_root = TempDir::new("rt-abort-src-assets");
    let dst_ws_root = TempDir::new("rt-abort-dst-ws");
    let dst_assets_root = TempDir::new("rt-abort-dst-assets");
    let source = fresh_services(&src_ws_root.0, &src_assets_root.0).await;
    let target = fresh_services(&dst_ws_root.0, &dst_assets_root.0).await;

    let id = WorkspaceId("ws-roundtrip-abort".to_string());
    let seeded = seed_source(&source, &src_ws_root.0, &src_assets_root.0, &id).await;

    let started = source
        .workspace_export_start_op(id.clone())
        .await
        .expect("export start");
    let export_id = started["exportId"].as_str().expect("exportId").to_string();
    assert!(wait_ready(&source, &export_id).await, "build must succeed");
    {
        let mut exports = source.transfer_exports.lock().unwrap();
        exports.get_mut(&export_id).unwrap().max_chunk_bytes = 4096;
    }
    let (size, sha, manifest) = ready_meta(&source, &export_id);

    // Begin the import and deliver only the FIRST chunk, then abort.
    let begin = target
        .workspace_import_begin_op(
            serde_json::to_value(&manifest).expect("manifest json"),
            size,
            sha.clone(),
        )
        .await
        .expect("import begin");
    let import_id = begin["importId"].as_str().expect("importId").to_string();
    let chunk0 = source
        .workspace_export_read_op(export_id.clone(), 0)
        .await
        .expect("read 0");
    assert!(
        chunk0["totalChunks"].as_u64().unwrap() > 1,
        "mid-relay abort needs >1 chunk"
    );
    target
        .workspace_import_chunk_op(
            import_id.clone(),
            0,
            chunk0["data"].as_str().unwrap().to_string(),
        )
        .await
        .expect("chunk 0");

    // Abort both sides.
    let target_staging = dst_ws_root.0.join(".import-staging").join(&import_id);
    assert!(target_staging.exists());
    let aborted = target
        .workspace_import_abort_op(import_id.clone())
        .await
        .expect("import abort");
    assert_eq!(aborted["aborted"], true);
    assert!(!target_staging.exists(), "target staging cleaned");
    assert!(matches!(
        target.store.get_workspace(&id).await,
        Err(Error::NotFound(_))
    ));

    let source_staging = src_ws_root.0.join(".export-staging").join(&export_id);
    assert!(source_staging.exists());
    let aborted = source
        .workspace_export_abort_op(export_id.clone())
        .await
        .expect("export abort");
    assert_eq!(aborted["aborted"], true);
    assert!(!source_staging.exists(), "source staging cleaned");

    // Source intact: workspace still active, WIP snapshots unwound so the
    // dirty state is exactly as seeded.
    let src_ws = source.store.get_workspace(&id).await.expect("source ws");
    assert_eq!(src_ws.status, WorkspaceStatus::Active);
    assert_eq!(repo_head(&seeded.repo), seeded.main_tip);
    assert!(is_untracked(&seeded.repo, "dirty.txt"));
    assert_eq!(
        std::fs::read_to_string(seeded.repo.join("staged.txt")).unwrap(),
        "staged\n"
    );
    assert_eq!(repo_head(&seeded.sandbox), seeded.sandbox_tip);
    assert_eq!(
        std::fs::read_to_string(seeded.sandbox.join("sb-dirty.txt")).unwrap(),
        "sandbox wip\n"
    );
}
