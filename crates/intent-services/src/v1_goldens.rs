//! v1 golden fixtures (harness-versioning H0, intent-hq/monorepo#2459).
//!
//! Byte-exact pins of every system-generated string that reaches an agent
//! conversation, captured against TODAY'S output as the baseline for the
//! byte-neutral harness refactor (H5/H6). These tests intentionally assert
//! full literal bytes (or a SHA-256 pin for the large bundled doctrine
//! layers): any wording/whitespace change MUST fail here and force a
//! deliberate golden update alongside a harness-version decision.
//!
//! Surface inventory: spec note "System-message audit (H0 final inventory)".
//! The composed turn-envelope goldens (decorations around a user message)
//! live in `agent_manager::v1_turn_envelope_goldens`, next to the manager's
//! test helpers. The delegation preamble is already byte-pinned by
//! `agent_ops::tests::delegate_appends_task_note_preamble_to_first_message`.

use std::path::PathBuf;
use std::sync::Arc;

use intent_core::events::{AGENT_DELETED, AGENT_FAILED, AGENT_IDLE};
use intent_core::{
    now_iso, ActorType, AgentId, Event, EventActor, Workspace, WorkspaceActivity,
    WorkspaceAttention, WorkspaceId, WorkspaceStatus,
};
use intent_store::Store;
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::Services;

fn sha256_hex(s: &str) -> String {
    let mut h = Sha256::new();
    h.update(s.as_bytes());
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

/// Minimal workspace row for store-backed goldens.
fn workspace(id: &WorkspaceId) -> Workspace {
    let ts = now_iso();
    Workspace {
        execution_environment: None,
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

struct TempDb {
    path: PathBuf,
}

impl TempDb {
    fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("intentd-goldens-{}.db", uuid::Uuid::new_v4()));
        Self { path }
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(PathBuf::from(format!("{}{suffix}", self.path.display())));
        }
    }
}

async fn setup() -> (TempDb, Services, WorkspaceId) {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.expect("ws");
    let services = Services::new(store);
    (tmp, services, ws)
}

/// A child-completion event as `handle_completion_event` sees it.
fn completion_event(event_type: &str, child: &AgentId, data: serde_json::Value) -> Event {
    Event {
        id: "ev-1".to_string(),
        workspace_id: WorkspaceId::from("ws-1"),
        timestamp: "2026-01-02T03:04:05Z".to_string(),
        event_type: event_type.to_string(),
        actor: EventActor {
            actor_type: ActorType::Agent,
            id: Some(child.0.clone()),
            name: Some("Builder".to_string()),
            email: None,
            model: None,
            metadata: None,
        },
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data,
    }
}

// ---------------------------------------------------------------------------
// Queue notes and warnings (`agent_manager.rs`)
// ---------------------------------------------------------------------------

#[test]
fn golden_stale_redrive_note() {
    assert_eq!(
        crate::agent_manager::stale_redrive_note("2026-01-02T03:04:05Z"),
        "[SYSTEM NOTE] This message was queued before you completed; your completion report \
         was already delivered to your parent at 2026-01-02T03:04:05Z. Only call reportToParent \
         again if this message materially changes the outcome — do not re-send the same report."
    );
}

#[test]
fn golden_dequeue_wait_note() {
    assert_eq!(
        crate::agent_manager::dequeue_wait_note("2026-01-02T03:04:05Z", "2m 10s"),
        "[SYSTEM NOTE] This message was queued at 2026-01-02T03:04:05Z and waited 2m 10s \
         before delivery."
    );
}

// Deliberately redundant with the byte-exact table in
// `agent_manager::tests` (queue-note context there); kept here so the
// H0 baseline is self-contained. Update both together.
#[test]
fn golden_format_wait_duration() {
    let f = crate::agent_manager::format_wait_duration;
    assert_eq!(f(0), "0s");
    assert_eq!(f(-5), "0s");
    assert_eq!(f(59), "59s");
    assert_eq!(f(60), "1m 0s");
    assert_eq!(f(130), "2m 10s");
    assert_eq!(f(3600), "1h 0m");
    assert_eq!(f(3720), "1h 2m");
}

#[test]
fn golden_idle_timeout_warning() {
    assert_eq!(
        crate::agent_manager::idle_timeout_warning_text(std::time::Duration::from_secs(1800)),
        "[SYSTEM WARNING] Your turn exceeded the inactivity timeout (1800s of silence) \
         and was interrupted. If you were waiting on something external, schedule a \
         `ws.hook.schedule` background hook to watch the condition and end your turn instead \
         of blocking — the hook's wake message resumes you. Assess where you left off and \
         continue the work."
    );
    // Fractional windows render the float form.
    assert_eq!(
        crate::agent_manager::idle_timeout_warning_text(std::time::Duration::from_millis(1500)),
        "[SYSTEM WARNING] Your turn exceeded the inactivity timeout (1.5s of silence) \
         and was interrupted. If you were waiting on something external, schedule a \
         `ws.hook.schedule` background hook to watch the condition and end your turn instead \
         of blocking — the hook's wake message resumes you. Assess where you left off and \
         continue the work."
    );
}

// ---------------------------------------------------------------------------
// Turn-envelope literal fragments
// ---------------------------------------------------------------------------

#[test]
fn golden_naming_tool_references() {
    assert_eq!(
        crate::agent_manager::workspace_naming_tool_reference("auggie"),
        "the `set_workspace_title_workspace-mcp` tool"
    );
    assert_eq!(
        crate::agent_manager::workspace_naming_tool_reference("opencode"),
        "the `workspace-mcp_set_workspace_title` tool"
    );
    assert_eq!(
        crate::agent_manager::workspace_naming_tool_reference("codex"),
        crate::agent_manager::GENERIC_NAMING_TOOL_REFERENCE
    );
    assert_eq!(
        crate::agent_manager::GENERIC_NAMING_TOOL_REFERENCE,
        "the `set_workspace_title` tool from the workspace MCP server"
    );
}

#[test]
fn golden_supervisor_history_wrapper() {
    use intent_core::AgentMessage;
    let msg = |role: &str, text: &str| AgentMessage {
        id: format!("m-{role}-{}", text.len()),
        agent_id: AgentId::from("agent-h"),
        seq: 0,
        role: role.to_string(),
        content: json!([{ "type": "text", "text": text }]),
        metadata: None,
        app_message_id: None,
        created_at: "2026-01-02T03:04:05Z".to_string(),
    };
    let xml = crate::history_xml::format_history_as_xml(
        &[msg("user", "hi <&>"), msg("assistant", "done")],
        crate::history_xml::MAX_HISTORY_CHARS,
    );
    assert_eq!(
        xml,
        "<supervisor>\n\
         The previous ACP session was lost. Below is the full conversation history from the prior session so you can continue seamlessly.\n\
         Do NOT mention session recovery to the user. Just continue naturally as if nothing happened.\n\
         \n\
         <exchange>\n\
         \x20 <user_request_or_tool_results>\n\
         \x20   <text>hi &lt;&amp;&gt;</text>\n\
         \x20 </user_request_or_tool_results>\n\
         \x20 <agent_response_or_tool_uses>\n\
         \x20   <text>done</text>\n\
         \x20 </agent_response_or_tool_uses>\n\
         </exchange>\n\
         Continue the conversation from this point. Do not mention session recovery or interruption.\n\
         </supervisor>"
    );
}

// ---------------------------------------------------------------------------
// Completion / group wakes (`lib.rs`)
// ---------------------------------------------------------------------------

#[test]
fn golden_completion_wake_variants() {
    let child = AgentId::from("agent-c1");
    // Bare idle completion, watch retained (grouped-failure path shape).
    let ev = completion_event(AGENT_IDLE, &child, json!({}));
    assert_eq!(
        crate::format_completion_wake(&child, &ev, None, false),
        "[WORKSPACE EVENTS] Child agent Builder (agent-c1) completed."
    );
    // Report + retired watch note.
    let ev = completion_event(
        AGENT_IDLE,
        &child,
        json!({ "agentName": "Builder", "completionReport": "All done." }),
    );
    assert_eq!(
        crate::format_completion_wake(&child, &ev, None, true),
        "[WORKSPACE EVENTS] Child agent Builder (agent-c1) completed. Report: All done. \
         NOTE: this wake consumed your one-shot watch on this agent — the watch is now \
         retired. Call ws.agent.watch(\"agent-c1\") again to be woken at its next completion."
    );
    // Failure with error + summary.
    let ev = completion_event(
        AGENT_FAILED,
        &child,
        json!({ "lastResponseSummary": "was compiling", "error": "exit 1" }),
    );
    assert_eq!(
        crate::format_completion_wake(&child, &ev, None, false),
        "[WORKSPACE EVENTS] Child agent Builder (agent-c1) failed. Summary: was compiling \
         Error: exit 1"
    );
    // Deleted: no re-arm pointer.
    let ev = completion_event(AGENT_DELETED, &child, json!({}));
    assert_eq!(
        crate::format_completion_wake(&child, &ev, None, true),
        "[WORKSPACE EVENTS] Child agent Builder (agent-c1) was deleted. NOTE: this wake \
         consumed your one-shot watch on this agent — the watch is now retired. The agent \
         was deleted, so it cannot be re-watched."
    );
    // Stall suspicion appended only without a rendered report.
    let stall = crate::StallSuspicion {
        task_title: "Port frobnicator".to_string(),
        task_status: "in_progress".to_string(),
    };
    let ev = completion_event(AGENT_IDLE, &child, json!({}));
    assert_eq!(
        crate::format_completion_wake(&child, &ev, Some(&stall), false),
        "[WORKSPACE EVENTS] Child agent Builder (agent-c1) completed. No completion report \
         and assigned task \"Port frobnicator\" is still in_progress — the agent may have \
         stalled rather than finished (monorepo#1016). Consider ws.agent.wakeOrCreate to \
         resume it."
    );
}

#[test]
fn golden_group_child_line_variants() {
    let child = AgentId::from("agent-c2");
    let ev = completion_event(
        AGENT_IDLE,
        &child,
        json!({ "agentName": "Builder", "completionReport": "Shipped." }),
    );
    assert_eq!(
        crate::format_group_child_line(&child, &ev, None, None),
        "- Builder (agent-c2) completed. Report: Shipped."
    );
    // Explicit completion_report wins over event data.
    assert_eq!(
        crate::format_group_child_line(&child, &ev, Some("Persisted report."), None),
        "- Builder (agent-c2) completed. Report: Persisted report."
    );
    // Attention fold (blocker) on a failed member.
    let ev = completion_event(
        AGENT_FAILED,
        &child,
        json!({
            "attentionRequestKind": "blocker",
            "attentionRequestReason": "sandbox exploded",
        }),
    );
    assert_eq!(
        crate::format_group_child_line(&child, &ev, None, None),
        "- Builder (agent-c2) failed. Reported a blocker: sandbox exploded"
    );
    // Discussion fold.
    let ev = completion_event(
        AGENT_IDLE,
        &child,
        json!({
            "attentionRequestKind": "discussion",
            "attentionRequestReason": "which schema?",
        }),
    );
    assert_eq!(
        crate::format_group_child_line(&child, &ev, None, None),
        "- Builder (agent-c2) completed. Requested a discussion: which schema?"
    );
}

#[test]
fn golden_group_wake_header() {
    let child_a = AgentId::from("agent-a");
    let child_b = AgentId::from("agent-b");
    let mut group = crate::agent_subscriptions::DelegationGroup {
        group_id: "g-1".to_string(),
        workspace_id: WorkspaceId::from("ws-1"),
        parent_agent_id: AgentId::from("agent-p"),
        await_mode: "after_all".to_string(),
        expected_agent_ids: vec![child_a.clone(), child_b.clone()],
        completed_agent_ids: vec![child_a.clone(), child_b.clone()],
        deleted_agent_ids: vec![],
        subscription_id: None,
        sealed: true,
        delivered: false,
        event_summaries: vec![
            "- Builder (agent-a) completed. Report: Done A.".to_string(),
            "- Builder (agent-b) completed. Report: Done B.".to_string(),
        ],
        raw_events: vec![
            Arc::new(completion_event(AGENT_IDLE, &child_a, json!({}))),
            Arc::new(completion_event(AGENT_IDLE, &child_b, json!({}))),
        ],
    };
    assert_eq!(
        crate::format_group_wake(&group),
        "[WORKSPACE EVENTS] All 2 delegated child agent(s) settled (completionStatus: completed).\n\
         - Builder (agent-a) completed. Report: Done A.\n\
         - Builder (agent-b) completed. Report: Done B."
    );
    // A failed member flips the status to partial (STAB-160); the child
    // lines are unchanged.
    group.raw_events[1] = Arc::new(completion_event(AGENT_FAILED, &child_b, json!({})));
    assert_eq!(
        crate::format_group_wake(&group),
        "[WORKSPACE EVENTS] All 2 delegated child agent(s) settled (completionStatus: partial).\n\
         - Builder (agent-a) completed. Report: Done A.\n\
         - Builder (agent-b) completed. Report: Done B."
    );
}

#[test]
fn golden_event_subscription_wake() {
    let child = AgentId::from("agent-e");
    let a = completion_event("file:updated", &child, json!({}));
    let b = completion_event("task:updated", &child, json!({}));
    let c = completion_event("file:updated", &child, json!({}));
    assert_eq!(
        crate::event_subscriptions::format_event_subscription_wake(&[&a, &b, &c]),
        "[WORKSPACE EVENTS] 3 event(s) matched your subscription: file:updated, task:updated."
    );
}

// ---------------------------------------------------------------------------
// Ready-set delta section (`agent_ops/ready_delta.rs`)
// ---------------------------------------------------------------------------

// Deliberately redundant with `ready_delta`'s own byte-exact tests
// (`render_section_links_tasks_and_flips_plural_framing`,
// `render_section_annotates_attention_statuses_instead_of_dropping`);
// kept here so the H0 baseline is self-contained. Update both together.
#[test]
fn golden_unblocked_section() {
    use crate::agent_ops::ready_delta::{render_unblocked_section, UnblockedReason, UnblockedTask};
    let tasks = vec![
        UnblockedTask {
            note_id: "n-1".to_string(),
            title: "Wire the adapter".to_string(),
            reason: UnblockedReason::DepsSatisfied,
            attention: None,
        },
        UnblockedTask {
            note_id: "n-2".to_string(),
            title: "Ship docs".to_string(),
            reason: UnblockedReason::ConflictCleared,
            attention: Some(intent_core::TaskStatus::Blocked),
        },
    ];
    assert_eq!(
        render_unblocked_section(&tasks, false),
        "Tasks now unblocked by this completion: \
         [Wire the adapter](intent://local/task/n-1) (deps satisfied), \
         [Ship docs](intent://local/task/n-2) (conflict cleared; currently blocked — needs attention)."
    );
    assert_eq!(
        render_unblocked_section(&tasks[..1], true),
        "Tasks now unblocked by these completions: \
         [Wire the adapter](intent://local/task/n-1) (deps satisfied)."
    );
}

// ---------------------------------------------------------------------------
// Hook wake framing and notices (`hook_manager.rs`)
// ---------------------------------------------------------------------------

#[test]
fn golden_hook_wake_logs_section() {
    assert_eq!(crate::hook_manager::with_wake_logs("msg", None), "msg");
    assert_eq!(crate::hook_manager::with_wake_logs("msg", Some("")), "msg");
    assert_eq!(
        crate::hook_manager::with_wake_logs("msg", Some("line1\nline2")),
        "msg\n\n[hook logs]\nline1\nline2"
    );
    // Over-cap logs are head-truncated with the marker line: exactly the
    // LAST 2048 chars survive, nothing more. Positionally distinct bytes
    // pin which end is retained and the exact cap.
    let logs: String = (0..3000)
        .map(|i| char::from(b'a' + (i % 26) as u8))
        .collect();
    assert_eq!(
        crate::hook_manager::with_wake_logs("msg", Some(&logs)),
        format!(
            "msg\n\n[hook logs]\n[earlier log lines truncated]\n{}",
            &logs[3000 - 2048..]
        )
    );
}

/// End-to-end hook wake bytes: schedule a one-shot hook whose validation run
/// dispatches, and pin the exact framed message (framing + dispatch body +
/// retired-state note) delivered to the owner.
#[tokio::test]
async fn golden_hook_dispatch_wake_bytes() {
    let (_t, svc, ws) = setup().await;
    let bus = crate::EventBus::new(svc.store().clone());
    let svc = svc.with_event_bus(bus);
    let owner = AgentId::from("agent-hooks");
    seed_agent(&svc, &ws, &owner).await;
    let out = svc
        .hook_schedule_op(
            &ws,
            &owner,
            &json!({
                "name": "ci-watch",
                "code": "return { dispatch: true, message: 'CI is green' };",
                "delayMs": 10_000,
            }),
        )
        .await
        .expect("schedule");
    assert_eq!(out["dispatched"], json!(true));
    let text = wake_texts_when(&svc, &owner, 1).await;
    assert_eq!(
        text,
        vec!["[Background hook \"ci-watch\"] CI is green\n\
             \n\
             [This hook is now retired and will not run again — reschedule via \
             ws.hook.schedule if still needed.]"
            .to_string()]
    );
}

/// Perpetual dispatch wake: the active-state note names the TTL deadline.
#[tokio::test]
async fn golden_hook_perpetual_dispatch_wake_bytes() {
    let (_t, svc, ws) = setup().await;
    let bus = crate::EventBus::new(svc.store().clone());
    let svc = svc.with_event_bus(bus);
    let owner = AgentId::from("agent-hooks");
    seed_agent(&svc, &ws, &owner).await;
    let out = svc
        .hook_schedule_op(
            &ws,
            &owner,
            &json!({
                "name": "pr-watch",
                "code": "return { dispatch: true, message: 'PR moved' };",
                "delayMs": 10_000,
                "perpetual": true,
            }),
        )
        .await
        .expect("schedule");
    assert_eq!(out["dispatched"], json!(true));
    let expires_at = out["hook"]["expiresAt"]
        .as_str()
        .expect("expiresAt")
        .to_string();
    let text = wake_texts_when(&svc, &owner, 1).await;
    assert_eq!(
        text,
        vec![format!(
            "[Background hook \"pr-watch\"] PR moved\n\
             \n\
             [This hook remains active until {expires_at} — cancel via ws.hook.cancel \
             when no longer needed.]"
        )]
    );
}

// ---------------------------------------------------------------------------
// PR monitor wakes (`pr_monitor.rs`)
// ---------------------------------------------------------------------------

fn merge_requirements(
    state: &str,
    approvals_have: i64,
    unresolved: i64,
) -> crate::pr_ops::MergeRequirements {
    crate::pr_ops::MergeRequirements {
        state: state.to_string(),
        is_draft: false,
        has_conflicts: false,
        is_behind: false,
        mergeable: Some(true),
        checks: crate::pr_ops::MergeRequirementsChecks {
            total: 3,
            passed: 2,
            failed: 0,
            pending: 1,
            items: vec![],
            failing_required: vec![],
            pending_required: vec!["build".to_string()],
            required_known: true,
        },
        approvals: crate::pr_ops::MergeRequirementsApprovals {
            decision: "review_required".to_string(),
            have: approvals_have,
            needed: Some(1),
            changes_requested: 0,
        },
        threads: crate::pr_ops::MergeRequirementsThreads {
            unresolved,
            resolution_required: Some(true),
        },
        merge_state_status: None,
        merge_blocked_reason: None,
        rules_known: true,
    }
}

fn pr_snapshot(state: &str) -> crate::pr_monitor::PrMonitorSnapshot {
    crate::pr_monitor::PrMonitorSnapshot {
        title: "feat: add adapter".to_string(),
        url: "https://github.com/o/r/pull/42".to_string(),
        head_sha: Some("abcdef1234567890".to_string()),
        conversation_count: 2,
        review_comment_count: 1,
        requirements: merge_requirements(state, 0, 1),
    }
}

fn pr_monitor_row() -> intent_core::PrMonitor {
    intent_core::PrMonitor {
        monitor_id: intent_core::PrMonitorId::from("prmon-1"),
        workspace_id: WorkspaceId::from("ws-1"),
        agent_id: AgentId::from("agent-pr"),
        repo_owner: "o".to_string(),
        repo_name: "r".to_string(),
        pr_number: 42,
        state: intent_core::PrMonitorState::Active,
        last_snapshot: None,
        baseline_snapshot: None,
        pending_changes: vec![],
        pending_since: None,
        last_change_at: None,
        last_polled_at: None,
        last_error: None,
        created_at: "2026-01-02T03:04:05Z".to_string(),
        updated_at: "2026-01-02T03:04:05Z".to_string(),
    }
}

#[test]
fn golden_pr_monitor_checklist_and_change_wake() {
    let m = pr_monitor_row();
    let snapshot = pr_snapshot("open");
    let changes = vec![
        "new approval (0 → 1 approving)".to_string(),
        "+1 conversation comment (3 total)".to_string(),
    ];
    assert_eq!(
        crate::pr_monitor::render_change_wake(&m, &changes, &snapshot),
        "[PR monitor o/r#42] 2 changes detected on \"feat: add adapter\" \
         (https://github.com/o/r/pull/42):\n\
         - new approval (0 → 1 approving)\n\
         - +1 conversation comment (3 total)\n\
         \n\
         Where the PR stands now:\n\
         - state: open\n\
         - approvals: review_required (0/1 required)\n\
         - checks: 2 passed, 0 failed, 1 pending (of 3); pending required: build\n\
         - unresolved threads: 1 (resolution required to merge)"
    );
}

#[test]
fn golden_pr_monitor_terminal_wake() {
    let m = pr_monitor_row();
    let snapshot = pr_snapshot("merged");
    assert_eq!(
        crate::pr_monitor::render_terminal_wake(&m, &[], &snapshot),
        "[PR monitor o/r#42] \"feat: add adapter\" was MERGED \
         (https://github.com/o/r/pull/42).\n\
         \n\
         Monitoring has STOPPED — this monitor is retired and will not report again."
    );
    let snapshot = pr_snapshot("closed");
    let changes = vec!["state: open → closed".to_string()];
    assert_eq!(
        crate::pr_monitor::render_terminal_wake(&m, &changes, &snapshot),
        "[PR monitor o/r#42] \"feat: add adapter\" was CLOSED without merging \
         (https://github.com/o/r/pull/42).\n\
         \n\
         Monitoring has STOPPED — this monitor is retired and will not report again.\n\
         \n\
         Changes since the last report:\n\
         - state: open → closed"
    );
}

#[test]
fn golden_pr_monitor_diff_lines() {
    let old = pr_snapshot("open");
    let mut new = pr_snapshot("open");
    new.requirements.approvals.have = 1;
    new.conversation_count = 3;
    new.head_sha = Some("1234567890abcdef".to_string());
    let changes = crate::pr_monitor::diff_snapshots(&old, &new);
    assert_eq!(
        changes,
        vec![
            "new commits pushed (head is now 12345678)".to_string(),
            "new approval (0 → 1 approving)".to_string(),
            "+1 conversation comment (3 total)".to_string(),
        ]
    );
}

/// Checklist branch lines beyond the happy path: draft, conflicts, behind,
/// failing required checks, changes-requested, blocked reason, and the two
/// rules-unknown renderings. (The per-field diff LINES beyond
/// `golden_pr_monitor_diff_lines` are byte-pinned by `pr_monitor::tests::
/// diff_detects_each_field_class` and companions — exact `assert_eq` on each
/// line — so they are not re-pinned here.)
#[test]
fn golden_pr_monitor_checklist_branch_lines() {
    let mut s = pr_snapshot("open");
    let r = &mut s.requirements;
    r.is_draft = true;
    r.has_conflicts = true;
    r.is_behind = true;
    r.mergeable = Some(false);
    r.checks.failed = 1;
    r.checks.passed = 1;
    r.checks.failing_required = vec!["build".to_string()];
    r.checks.pending_required = vec![];
    r.approvals.changes_requested = 1;
    r.merge_blocked_reason = Some("merge conflicts".to_string());
    assert_eq!(
        crate::pr_monitor::render_checklist(&s),
        "- state: open\n\
         - approvals: review_required (0/1 required)\n\
         - changes requested by 1 reviewer\n\
         - checks: 1 passed, 1 failed, 1 pending (of 3); failing required: build\n\
         - unresolved threads: 1 (resolution required to merge)\n\
         - merge conflicts present\n\
         - branch is behind its base\n\
         - blocked: merge conflicts"
    );
    // Rules-unknown: approvals fall back to the bare count, threads drop the
    // resolution tail, and the trailer line appears; unknown required-check
    // flags add the availability note.
    let mut s = pr_snapshot("open");
    let r = &mut s.requirements;
    r.rules_known = false;
    r.approvals.needed = None;
    r.threads.resolution_required = None;
    r.checks.required_known = false;
    r.checks.pending_required = vec![];
    assert_eq!(
        crate::pr_monitor::render_checklist(&s),
        "- state: open\n\
         - approvals: review_required (0 approving)\n\
         - checks: 2 passed, 0 failed, 1 pending (of 3) (required-check flags unavailable)\n\
         - unresolved threads: 1\n\
         - (branch rules unreadable — approval/thread requirements unknown)"
    );
}

/// FE-cancel and archive-sweep notices for hooks and PR monitors: exact
/// wake bytes delivered to the owning agent.
#[tokio::test]
async fn golden_hook_and_pr_monitor_cancel_notice_bytes() {
    let (_t, svc, ws) = setup().await;
    let bus = crate::EventBus::new(svc.store().clone());
    let svc = svc.with_event_bus(bus);
    let owner = AgentId::from("agent-cancel");
    seed_agent(&svc, &ws, &owner).await;
    // Hook FE-cancel (caller = None).
    let out = svc
        .hook_schedule_op(
            &ws,
            &owner,
            &json!({
                "name": "watcher",
                "code": "return { dispatch: false };",
                "delayMs": 10_000,
            }),
        )
        .await
        .expect("schedule");
    let hook_id = intent_core::HookId::from(out["hook"]["hookId"].as_str().expect("hookId"));
    svc.hook_cancel_op(&ws, &hook_id, None)
        .await
        .expect("cancel");
    let texts = wake_texts_when(&svc, &owner, 1).await;
    assert_eq!(
        texts,
        vec!["[Background hook \"watcher\"] This hook was cancelled from the app.".to_string()]
    );
    // PR monitor FE-cancel + archive-sweep notices.
    let mut monitor = pr_monitor_row();
    monitor.workspace_id = ws.clone();
    monitor.agent_id = owner.clone();
    assert!(svc
        .store()
        .insert_pr_monitor(&monitor)
        .await
        .expect("insert"));
    svc.pr_monitor_cancel(&ws, &monitor.monitor_id, None)
        .await
        .expect("cancel monitor");
    let texts = wake_texts_when(&svc, &owner, 2).await;
    assert_eq!(
        texts[1],
        "[PR monitor o/r#42] This monitor was cancelled from the app — it will not \
         report again."
    );
    let mut monitor2 = pr_monitor_row();
    monitor2.monitor_id = intent_core::PrMonitorId::from("prmon-2");
    monitor2.workspace_id = ws.clone();
    monitor2.agent_id = owner.clone();
    assert!(svc
        .store()
        .insert_pr_monitor(&monitor2)
        .await
        .expect("insert 2"));
    svc.cancel_workspace_pr_monitors(&ws).await;
    let texts = wake_texts_when(&svc, &owner, 3).await;
    assert_eq!(
        texts[2],
        "[PR monitor o/r#42] This monitor was cancelled because its workspace was \
         archived — it will not report again."
    );
}

/// Archive-sweep hook cancel notice: exact wake bytes (framed like every
/// hook wake).
#[tokio::test]
async fn golden_hook_archive_cancel_notice_bytes() {
    let (_t, svc, ws) = setup().await;
    let bus = crate::EventBus::new(svc.store().clone());
    let svc = svc.with_event_bus(bus);
    let owner = AgentId::from("agent-arch");
    seed_agent(&svc, &ws, &owner).await;
    svc.hook_schedule_op(
        &ws,
        &owner,
        &json!({
            "name": "sweeper",
            "code": "return { dispatch: false };",
            "delayMs": 10_000,
        }),
    )
    .await
    .expect("schedule");
    svc.cancel_workspace_hooks(&ws).await;
    let texts = wake_texts_when(&svc, &owner, 1).await;
    assert_eq!(
        texts,
        vec![
            "[Background hook \"sweeper\"] This hook was cancelled because its workspace \
             was archived."
                .to_string()
        ]
    );
}

/// Hook expiry notices: exact bytes for the one-shot ("without a dispatch")
/// and perpetual ("N runs, N dispatches") tallies, including singular and
/// plural forms.
#[tokio::test]
async fn golden_hook_expiry_notice_bytes() {
    let (_t, svc, ws) = setup().await;
    let bus = crate::EventBus::new(svc.store().clone());
    let svc = svc.with_event_bus(bus);
    let owner = AgentId::from("agent-exp");
    seed_agent(&svc, &ws, &owner).await;
    let past = "2026-01-02T03:04:05Z".to_string();
    let hook =
        |id: &str, name: &str, perpetual: bool, runs: i64, dispatches: i64| intent_core::Hook {
            hook_id: intent_core::HookId::from(id),
            workspace_id: ws.clone(),
            agent_id: owner.clone(),
            name: name.to_string(),
            code: "return { dispatch: false };".to_string(),
            delay_ms: 600_000,
            state: intent_core::HookState::Scheduled,
            created_at: past.clone(),
            last_run_at: None,
            next_run_at: None,
            run_count: runs,
            last_error: None,
            last_logs: None,
            last_state: None,
            expires_at: Some(past.clone()),
            perpetual,
            dispatch_count: dispatches,
        };
    // One-shot, plural runs; rehydration expires already-past hooks at boot.
    svc.store()
        .insert_hook(&hook("hook-exp-1", "one-shot", false, 2, 0))
        .await
        .expect("insert 1");
    // Perpetual, singular tallies.
    svc.store()
        .insert_hook(&hook("hook-exp-2", "perpetual", true, 1, 1))
        .await
        .expect("insert 2");
    svc.rehydrate_hooks().await.expect("rehydrate");
    let mut texts = wake_texts_when(&svc, &owner, 2).await;
    texts.sort();
    assert_eq!(
        texts,
        vec![
            "[Background hook \"one-shot\"] Your background hook \"one-shot\" expired after \
             reaching its TTL (2 runs completed without a dispatch). Schedule a new hook via \
             ws.hook.schedule if the condition is still worth watching."
                .to_string(),
            "[Background hook \"perpetual\"] Your background hook \"perpetual\" expired after \
             reaching its TTL (1 run, 1 dispatch). Schedule a new hook via ws.hook.schedule \
             if the condition is still worth watching."
                .to_string(),
        ]
    );
}

/// Hook eviction notice: exact bytes for a throwing run (failed-run wording
/// + terminal note).
#[tokio::test]
async fn golden_hook_eviction_notice_bytes() {
    let (_t, svc, ws) = setup().await;
    let bus = crate::EventBus::new(svc.store().clone());
    let svc = svc.with_event_bus(bus);
    let owner = AgentId::from("agent-evict");
    seed_agent(&svc, &ws, &owner).await;
    // Seed a scheduled row directly (a throwing script would fail the
    // schedule-time validation run) and drive one run via runNow.
    let hook = intent_core::Hook {
        hook_id: intent_core::HookId::from("hook-evict-1"),
        workspace_id: ws.clone(),
        agent_id: owner.clone(),
        name: "will-throw".to_string(),
        code: "throw new Error('kaput');".to_string(),
        delay_ms: 600_000,
        state: intent_core::HookState::Scheduled,
        created_at: now_iso(),
        last_run_at: None,
        next_run_at: None,
        run_count: 0,
        last_error: None,
        last_logs: None,
        last_state: None,
        expires_at: None,
        perpetual: false,
        dispatch_count: 0,
    };
    svc.store().insert_hook(&hook).await.expect("insert");
    svc.rehydrate_hooks().await.expect("rehydrate");
    svc.hook_run_now_op(&ws, &hook.hook_id)
        .await
        .expect("runNow");
    let texts = wake_texts_when(&svc, &owner, 1).await;
    assert_eq!(
        texts,
        vec![
            "[Background hook \"will-throw\"] Your background hook \"will-throw\" was \
             evicted after a failed run: Error: kaput\n\
             \n\
             [This hook will not run again. Schedule a new hook via ws.hook.schedule \
             if the condition is still worth watching.]"
                .to_string()
        ]
    );
}

/// Hook eviction notice, internal-error variant: exact bytes when the
/// scheduler evicts after a store error (`evict_hook_after_store_error`).
#[tokio::test]
async fn golden_hook_eviction_internal_error_notice_bytes() {
    let (_t, svc, ws) = setup().await;
    let bus = crate::EventBus::new(svc.store().clone());
    let svc = svc.with_event_bus(bus);
    let owner = AgentId::from("agent-evict-2");
    seed_agent(&svc, &ws, &owner).await;
    let mut hook = intent_core::Hook {
        hook_id: intent_core::HookId::from("hook-evict-2"),
        workspace_id: ws.clone(),
        agent_id: owner.clone(),
        name: "store-victim".to_string(),
        code: "return { dispatch: false };".to_string(),
        delay_ms: 600_000,
        state: intent_core::HookState::Scheduled,
        created_at: now_iso(),
        last_run_at: None,
        next_run_at: None,
        run_count: 0,
        last_error: None,
        last_logs: None,
        last_state: None,
        expires_at: None,
        perpetual: false,
        dispatch_count: 0,
    };
    svc.store().insert_hook(&hook).await.expect("insert");
    let cause = intent_core::Error::Internal("db locked".to_string());
    svc.evict_hook_after_store_error(&mut hook, &cause).await;
    let texts = wake_texts_when(&svc, &owner, 1).await;
    assert_eq!(
        texts,
        vec![
            "[Background hook \"store-victim\"] Your background hook \"store-victim\" was \
             evicted after an internal error: scheduler stopped after a store error: \
             internal error: db locked\n\
             \n\
             [This hook will not run again. Schedule a new hook via ws.hook.schedule \
             if the condition is still worth watching.]"
                .to_string()
        ]
    );
}

// ---------------------------------------------------------------------------
// Delegation preamble + notices (`agent_ops.rs`)
// ---------------------------------------------------------------------------

/// Seed a bare agent session row owned by `ws` (mirrors the hook tests).
async fn seed_agent(svc: &Services, ws: &WorkspaceId, id: &AgentId) {
    let ts = now_iso();
    let session = intent_core::AgentSession {
        harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
        harness_features: None,
        id: id.clone(),
        workspace_id: ws.clone(),
        parent_agent_id: None,
        backend_session_id: None,
        acp_session_id: None,
        name: "Builder".to_string(),
        name_explicitly_set: false,
        model: None,
        reasoning_effort: None,
        effort_levels: None,
        provider: None,
        system_prompt: None,
        specialist: None,
        status: intent_core::AgentStatus::Active,
        is_active: false,
        messages: vec![],
        stats: None,
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
        created_at: ts.clone(),
        updated_at: ts,
        sandbox_id: None,
        sandbox_path: None,
        sandbox_branch: None,
        stop_reason: None,
        stop_reason_timestamp: None,
        session_corrupted: false,
        pending_delete_at: None,
    };
    svc.store()
        .insert_agent_session(&session)
        .await
        .expect("seed agent");
}

/// All persisted message texts for an agent (each message's text blocks
/// joined), oldest-first — the wake-delivery capture used by the goldens.
/// Wake persistence can lag the op return, so poll until at least `expected`
/// messages are present (generous deadline, monorepo#1358 precedent); on
/// timeout return whatever was seen and let the caller's assert report it.
async fn wake_texts_when(svc: &Services, id: &AgentId, expected: usize) -> Vec<String> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let session = svc.store().get_agent_session(id).await.expect("session");
        let texts: Vec<String> = session
            .messages
            .iter()
            .map(|m| {
                m.content
                    .as_array()
                    .map(|blocks| {
                        blocks
                            .iter()
                            .filter_map(|b| b.get("text").and_then(|t| t.as_str()))
                            .collect::<Vec<_>>()
                            .join("\n")
                    })
                    .unwrap_or_default()
            })
            .collect();
        if texts.len() >= expected || std::time::Instant::now() >= deadline {
            return texts;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

/// Questions-dismissed notice: exact bytes for the zero-question fallback
/// noun ("questions" — the count is 0 for an unknown message id).
#[tokio::test]
async fn golden_questions_dismissed_notice_bytes() {
    let (_t, svc, ws) = setup().await;
    let owner = AgentId::from("agent-q");
    seed_agent(&svc, &ws, &owner).await;
    svc.agent_dismiss_questions_op(ws.clone(), owner.clone(), "msg-1".to_string())
        .await
        .expect("dismiss");
    let text = wake_texts_when(&svc, &owner, 1).await;
    assert_eq!(
        text,
        vec![
            "User dismissed your questions without answering. This is an informative \
             notice only — do not re-ask and do not proceed with any work; end \
             your turn and wait for the user's next message."
                .to_string()
        ]
    );
}

/// Report-to-parent wake: exact bytes of the ungrouped immediate parent wake.
#[tokio::test]
async fn golden_report_to_parent_wake_bytes() {
    let (_t, svc, ws) = setup().await;
    let parent = AgentId::from("agent-parent");
    seed_agent(&svc, &ws, &parent).await;
    let resp = svc
        .agent_delegate_op(
            ws.clone(),
            intent_core::AgentDelegateInput {
                agent_instructions: Some("do the thing".into()),
                ..Default::default()
            },
            Some(parent.clone()),
        )
        .await
        .expect("delegate");
    let child = AgentId::from(resp["agentId"].as_str().expect("agentId"));
    let child_name = svc
        .store()
        .get_agent_session(&child)
        .await
        .expect("child")
        .name;
    svc.agent_report_to_parent_op(ws.clone(), json!("Task finished."), Some(child.clone()))
        .await
        .expect("report");
    let texts = wake_texts_when(&svc, &parent, 1).await;
    assert_eq!(
        texts,
        vec![format!(
            "[WORKSPACE EVENTS] Child agent {child_name} ({id}) reported. Report: Task \
             finished. NOTE: this report consumed your one-shot watch on this agent — it \
             will NOT fire again on completion (failure/deletion still deliver). Call \
             ws.agent.watch(\"{id}\") again to be woken at its next completion.",
            id = child.0
        )]
    );
}

/// Attention-request wakes: exact bytes for the blocker and discussion verbs.
#[tokio::test]
async fn golden_attention_request_wake_bytes() {
    let (_t, svc, ws) = setup().await;
    let parent = AgentId::from("agent-parent");
    seed_agent(&svc, &ws, &parent).await;
    let resp = svc
        .agent_delegate_op(
            ws.clone(),
            intent_core::AgentDelegateInput {
                agent_instructions: Some("do the thing".into()),
                ..Default::default()
            },
            Some(parent.clone()),
        )
        .await
        .expect("delegate");
    let child = AgentId::from(resp["agentId"].as_str().expect("agentId"));
    let child_name = svc
        .store()
        .get_agent_session(&child)
        .await
        .expect("child")
        .name;
    svc.agent_request_attention_op(
        ws.clone(),
        "blocker".into(),
        "sandbox exploded".into(),
        Some(child.clone()),
    )
    .await
    .expect("blocker");
    svc.agent_request_attention_op(
        ws.clone(),
        "discussion".into(),
        "which schema?".into(),
        Some(child.clone()),
    )
    .await
    .expect("discussion");
    let texts = wake_texts_when(&svc, &parent, 2).await;
    assert_eq!(
        texts,
        vec![
            format!(
                "[WORKSPACE EVENTS] Child agent {child_name} ({}) reports a blocker: sandbox exploded",
                child.0
            ),
            format!(
                "[WORKSPACE EVENTS] Child agent {child_name} ({}) requests a discussion: which schema?",
                child.0
            ),
        ]
    );
}

/// Watcher fan-out attention wake (monorepo#1229/#2051): an explicit
/// non-parent `ws.agent.watch` watcher gets the remains-armed variant, with
/// the ungrouped completion promise.
#[tokio::test]
async fn golden_watcher_attention_wake_bytes() {
    let (_t, svc, ws) = setup().await;
    let parent = AgentId::from("agent-parent");
    let watcher = AgentId::from("agent-watcher");
    seed_agent(&svc, &ws, &parent).await;
    seed_agent(&svc, &ws, &watcher).await;
    let resp = svc
        .agent_delegate_op(
            ws.clone(),
            intent_core::AgentDelegateInput {
                agent_instructions: Some("do the thing".into()),
                ..Default::default()
            },
            Some(parent.clone()),
        )
        .await
        .expect("delegate");
    let child = AgentId::from(resp["agentId"].as_str().expect("agentId"));
    let child_name = svc
        .store()
        .get_agent_session(&child)
        .await
        .expect("child")
        .name;
    svc.agent_watch_op(ws.clone(), watcher.clone(), child.clone())
        .await
        .expect("watch");
    svc.agent_request_attention_op(
        ws.clone(),
        "blocker".into(),
        "sandbox exploded".into(),
        Some(child.clone()),
    )
    .await
    .expect("blocker");
    let texts = wake_texts_when(&svc, &watcher, 1).await;
    assert_eq!(
        texts,
        vec![format!(
            "[WORKSPACE EVENTS] Watched agent {child_name} ({}) reports a blocker: sandbox \
             exploded (Your watch on this agent remains armed; you will still be woken at \
             its completion.)",
            child.0
        )]
    );
}

// ---------------------------------------------------------------------------
// Prompt notices (`agent_manager.rs`)
// ---------------------------------------------------------------------------

/// Note-image and attachment-reference notices: exact bytes, including the
/// optional mime/size fragments.
#[test]
fn golden_image_and_attachment_notice_bytes() {
    assert_eq!(
        crate::agent_manager::note_images_notice(2),
        "[System: 2 image(s) from the referenced note(s) are attached to this message.]"
    );
    assert_eq!(
        crate::agent_manager::attachment_reference_notice(
            "report.pdf",
            Some("application/pdf"),
            Some(1234),
            "att-1"
        ),
        "[Attachment: \"report.pdf\", type application/pdf, 1234 bytes — attachmentId: att-1. \
         The file is NOT inlined in this message. Call ws.file.getAttachment(\"att-1\") to \
         copy it into your working directory, then read it from the returned path.]"
    );
    assert_eq!(
        crate::agent_manager::attachment_reference_notice("notes.txt", None, None, "att-2"),
        "[Attachment: \"notes.txt\" — attachmentId: att-2. The file is NOT inlined in this \
         message. Call ws.file.getAttachment(\"att-2\") to copy it into your working \
         directory, then read it from the returned path.]"
    );
}

/// Supervisor-history truncation markers: the omitted-exchanges comment
/// (exact bytes inside the wrapper) and the middle-truncation marker line
/// inside an oversized tool_result.
#[test]
fn golden_supervisor_history_truncation_markers() {
    use intent_core::AgentMessage;
    let msg = |id: &str, role: &str, blocks: serde_json::Value| AgentMessage {
        id: id.to_string(),
        agent_id: AgentId::from("agent-h"),
        seq: 0,
        role: role.to_string(),
        content: blocks,
        metadata: None,
        app_message_id: None,
        created_at: "2026-01-02T03:04:05Z".to_string(),
    };
    // Two exchanges with a budget that only fits the newest: the omission
    // comment names the count and sits right after the preamble.
    let messages = vec![
        msg(
            "m1",
            "user",
            json!([{ "type": "text", "text": "first question" }]),
        ),
        msg(
            "m2",
            "assistant",
            json!([{ "type": "text", "text": "first answer" }]),
        ),
        msg("m3", "user", json!([{ "type": "text", "text": "second" }])),
        msg(
            "m4",
            "assistant",
            json!([{ "type": "text", "text": "reply" }]),
        ),
    ];
    let preamble_len = "<supervisor>\nThe previous ACP session was lost. Below is the full \
                        conversation history from the prior session so you can continue \
                        seamlessly.\nDo NOT mention session recovery to the user. Just \
                        continue naturally as if nothing happened.\n\n"
        .len();
    let closing_len = "Continue the conversation from this point. Do not mention session \
                       recovery or interruption.\n</supervisor>"
        .len();
    let newest_exchange = "<exchange>\n\
                           \x20 <user_request_or_tool_results>\n\
                           \x20   <text>second</text>\n\
                           \x20 </user_request_or_tool_results>\n\
                           \x20 <agent_response_or_tool_uses>\n\
                           \x20   <text>reply</text>\n\
                           \x20 </agent_response_or_tool_uses>\n\
                           </exchange>\n";
    let max_omission = "<!-- 2 earlier exchanges omitted due to size limits -->\n".len();
    let budget = preamble_len + closing_len + max_omission + newest_exchange.len();
    let xml = crate::history_xml::format_history_as_xml(&messages, budget);
    assert_eq!(
        xml,
        format!(
            "<supervisor>\n\
             The previous ACP session was lost. Below is the full conversation history from \
             the prior session so you can continue seamlessly.\n\
             Do NOT mention session recovery to the user. Just continue naturally as if \
             nothing happened.\n\
             \n\
             <!-- 1 earlier exchanges omitted due to size limits -->\n\
             {newest_exchange}\
             Continue the conversation from this point. Do not mention session recovery or \
             interruption.\n\
             </supervisor>"
        )
    );
    // Middle-truncation marker: an oversized tool_result is head+tail kept
    // with the exact `... [N characters truncated] ...` line between. Cap is
    // 4000 chars with 60 reserved for the marker (half-budget 1970).
    let big = "y".repeat(5000);
    let messages = vec![msg(
        "m5",
        "user",
        json!([{ "type": "tool_result", "tool_use_id": "t1", "content": big }]),
    )];
    let xml =
        crate::history_xml::format_history_as_xml(&messages, crate::history_xml::MAX_HISTORY_CHARS);
    let expected_block = format!(
        "    <tool_result tool_use_id=\"t1\" is_error=\"false\">\n\
         \x20     {}\n... [1060 characters truncated] ...\n{}\n\
         \x20   </tool_result>\n",
        "y".repeat(1970),
        "y".repeat(1970),
    );
    assert!(xml.contains(&expected_block), "{xml}");
}

// ---------------------------------------------------------------------------
// System prompt assembly (`rules.rs`)
// ---------------------------------------------------------------------------

/// Exact bytes of the compact assembled-prompt layers (commit policy,
/// questions, next-steps footer, specialist role wrapper, role-reminder
/// footer, user-rules wrapper) plus the `\n\n---\n\n` layer separator, pinned
/// via a hermetic assembly with no workspace path (no rule files, no skills,
/// no RTK — only the always-on layers).
#[tokio::test]
async fn golden_assembled_prompt_static_layers() {
    let (_t, svc, _ws) = setup().await;
    let specialist = crate::rules::SpecialistPromptInjection {
        behavior_prompt: Some("Implement the task.".to_string()),
        specialist_name: Some("Implementor".to_string()),
        role_reminder: Some("Stay in scope.".to_string()),
    };
    let prompt = crate::rules::assemble_system_prompt(
        svc.store(),
        None,
        "task-loop",
        Some(&specialist),
        false,
        false,
        false,
        &intent_core::settings_file::AgentFeaturesSettings::default(),
        None,
        None,
    )
    .await
    .expect("assembled prompt");
    // The specialization-rules layer is itself composed with the same
    // `\n\n---\n\n` separator (common → workspace → specific), so skip the
    // leading doctrine segments (pinned by hash in
    // `golden_bundled_doctrine_hashes`) up to the specialist role layer.
    let mut layers = prompt
        .split("\n\n---\n\n")
        .skip_while(|l| !l.starts_with("# Your Specialist Role"));
    assert_eq!(
        layers.next().expect("specialist role layer"),
        "# Your Specialist Role\n\n\
         <specialist_role>\n\
         Implement the task.\n\
         </specialist_role>\n\n\
         The instructions in <specialist_role> define your primary function. \
         Prioritize them above general guidance."
    );
    assert_eq!(
        layers.next().expect("commit policy layer"),
        "## Commit Policy\n\n\
         Commit through `ws.git.commit` — never run `git commit` yourself \
         unless the user explicitly asks for a git workflow that \
         `ws.git.commit` cannot express (e.g. multiple scoped commits on a \
         branch). You may commit when it makes sense for the work; the system \
         may also automatically commit any remaining changes when your turn \
         ends."
    );
    assert_eq!(
        layers.next().expect("role reminder layer"),
        "## Role Reminder\n\nYou are a Implementor. Stay in scope."
    );
    assert_eq!(
        layers.next().expect("questions layer"),
        "## Asking the User Questions\n\n\
         When requirements are ambiguous or a decision needs user input, ask \
         structured clarifying questions with `ws.app.question.ask` via the \
         `workspace_api` tool instead of burying questions in prose. Call it once \
         per question with 2-4 options; do not add an \"Other\" option — a \
         free-form answer is always offered automatically. Ask all your \
         questions, then end the turn: questions are presented when your turn \
         ends, and the answers arrive in the next user message."
    );
    assert_eq!(
        layers.next().expect("next steps layer"),
        "## Suggested Next Steps\n\n\
         At the end of your response, offer the user clear next actions as a \
         `<!-- suggested-prompts ... -->` HTML comment block:\n\n\
         ```\n\
         <!-- suggested-prompts\n\
         Run the tests to verify the implementation.\n\
         Review changes before committing.\n\
         -->\n\
         ```\n\n\
         Write 2–4 prompts, each a short directive sentence phrased as \
         something the user might say next."
    );
    assert!(layers.next().is_none(), "no unexpected trailing layers");
}

/// Auto-commit flips the next-steps example line and appends the
/// auto-commit clause; sub-agents skip questions + next-steps entirely.
#[tokio::test]
async fn golden_assembled_prompt_auto_commit_and_sub_agent_variants() {
    let (_t, svc, _ws) = setup().await;
    let features = intent_core::settings_file::AgentFeaturesSettings::default();
    let auto_on = crate::rules::assemble_system_prompt(
        svc.store(),
        None,
        "task-loop",
        None,
        false,
        true,
        false,
        &features,
        None,
        None,
    )
    .await
    .expect("prompt");
    let footer = auto_on
        .split("\n\n---\n\n")
        .find(|l| l.starts_with("## Suggested Next Steps"))
        .expect("next steps layer");
    assert_eq!(
        footer,
        "## Suggested Next Steps\n\n\
         At the end of your response, offer the user clear next actions as a \
         `<!-- suggested-prompts ... -->` HTML comment block:\n\n\
         ```\n\
         <!-- suggested-prompts\n\
         Run the tests to verify the implementation.\n\
         Check the changes in the diff view.\n\
         -->\n\
         ```\n\n\
         Write 2–4 prompts, each a short directive sentence phrased as \
         something the user might say next. Auto-commit is enabled; do not \
         include prompts about committing or reviewing changes before committing."
    );
    let sub_agent = crate::rules::assemble_system_prompt(
        svc.store(),
        None,
        "task-loop",
        None,
        true,
        false,
        false,
        &features,
        None,
        None,
    )
    .await
    .expect("prompt");
    assert!(!sub_agent.contains("## Asking the User Questions"));
    assert!(!sub_agent.contains("## Suggested Next Steps"));
    assert!(sub_agent.contains("## Commit Policy"));
}

/// H2 regression: a session stamped `harnessVersion: "1.0"` (every session
/// H1 has created) resolves the v1 doctrine set and assembles the exact
/// bytes the pre-versioned layout produced — pinned here as equality with
/// the session-less (latest) assembly, whose layers are themselves
/// byte-pinned by the goldens above and the doctrine hashes below. An
/// unknown/corrupt stamp falls back to the latest instead of failing.
#[tokio::test]
async fn golden_v1_session_assembles_identical_to_latest() {
    let (_t, svc, ws) = setup().await;
    let owner = AgentId::from("agent-h2-pin");
    seed_agent(&svc, &ws, &owner).await;
    let mut session = svc
        .store()
        .get_agent_session(&owner)
        .await
        .expect("session");
    assert_eq!(session.harness_version, "1.0", "H1 stamps 1.0");
    let features = intent_core::settings_file::AgentFeaturesSettings::default();
    let specialist = crate::rules::SpecialistPromptInjection {
        behavior_prompt: Some("Implement the task.".to_string()),
        specialist_name: Some("Implementor".to_string()),
        role_reminder: Some("Stay in scope.".to_string()),
    };
    let assemble = |session: Option<intent_core::AgentSession>| {
        let store = svc.store();
        let specialist = specialist.clone();
        let features = features.clone();
        async move {
            crate::rules::assemble_system_prompt(
                store,
                None,
                "task-loop",
                Some(&specialist),
                false,
                false,
                false,
                &features,
                None,
                session.as_ref(),
            )
            .await
            .expect("assembled prompt")
        }
    };
    let latest = assemble(None).await;
    let pinned_v1 = assemble(Some(session.clone())).await;
    assert_eq!(pinned_v1, latest, "1.0 session == pre-change assembly");
    // A stale/corrupt stamp falls back to the latest (never fails a spawn).
    session.harness_version = "9.9".to_string();
    let unknown = assemble(Some(session)).await;
    assert_eq!(unknown, latest, "unknown stamp falls back to latest");
}

/// The bundled doctrine layers are large; pin them by SHA-256 so any change
/// to the shipped instruction markdown (or the feature-gating composition)
/// fails here and forces a harness-version decision. The hashes are of
/// `get_instruction_with_common` output with all-default agent features.
#[test]
fn golden_bundled_doctrine_hashes() {
    let features = intent_core::settings_file::AgentFeaturesSettings::default();
    let pins = [
        ("task-loop", "GOLDEN_TASK_LOOP"),
        ("interactive", "GOLDEN_INTERACTIVE"),
        ("workspace-agent", "GOLDEN_WORKSPACE_AGENT"),
        ("task-breakdown", "GOLDEN_TASK_BREAKDOWN"),
        ("common", "GOLDEN_COMMON"),
        ("workspace", "GOLDEN_WORKSPACE"),
    ];
    let actual: Vec<String> = pins
        .iter()
        .map(|(agent_type, _)| {
            format!(
                "{}: {}",
                agent_type,
                sha256_hex(&crate::instructions::get_instruction_with_common(
                    agent_type, &features
                ))
            )
        })
        .collect();
    let expected = vec![
        "task-loop: 3866e429706b36113628653bb88ea86319b52bad6b2d0b1a583a998e704d965a".to_string(),
        "interactive: 0e01a055468ab2631cb642e799b0daa51846c3ad2b6c11b088e604e412a30716".to_string(),
        "workspace-agent: f69092bc2a4c4a5fc62b98dfc35b96f5f0115c6a8758a961a91d842fa3e43073"
            .to_string(),
        "task-breakdown: d3795b3bd8d08737c7df792913ecd61a9d6bc961d7660acf4ab40b509cbf6410"
            .to_string(),
        "common: 8810905ce42af859fe87a2e11d596e0ce6d495b6d82cc69422e4447bd6ea0065".to_string(),
        "workspace: 8044a353bbbc846cf95a33bc0cf2dc72ceacc672f99e009e3959b777f49f4c27".to_string(),
    ];
    assert_eq!(actual, expected);
}

/// User-rules wrapper bytes (workspace rule files + repo-config instructions
/// both route through this).
#[test]
fn golden_user_rules_wrapper() {
    assert_eq!(
        crate::rules::format_user_rules_for_context("Be careful.", "/repo/AGENTS.md"),
        "## User Rules & Guidelines\n\n\
         The following rules and guidelines have been configured for this project. \
         Please follow these conventions and best practices:\n\n\
         ```\nBe careful.\n```\n\n\
         These rules are loaded from: /repo/AGENTS.md"
    );
}

/// RTK instruction line: exact bytes for the subcommand-joined variant
/// (mirrors `cloudlands-fe rtk-detector.ts getRtkPromptInstruction()`).
#[test]
fn golden_rtk_instruction_line() {
    assert_eq!(
        crate::rules::rtk_instruction_line(
            crate::harness::latest(),
            &["git".to_string(), "cargo".to_string()]
        ),
        "Prefix these commands with rtk for compressed, LLM-friendly output: git, cargo"
    );
}

/// Skills catalog wrapper: exact bytes of the prompt-injection layer,
/// including XML escaping of metadata fields and the empty-catalog fast path.
#[test]
fn golden_skills_catalog_wrapper() {
    let skill = |name: &str, description: &str, location: &str| crate::skills::SkillMetadata {
        name: name.to_string(),
        description: description.to_string(),
        location: location.to_string(),
        scope: "project".to_string(),
        allowed_tools: None,
        compatibility: None,
    };
    assert_eq!(crate::skills::build_skills_catalog(&[]), "");
    assert_eq!(
        crate::skills::build_skills_catalog(&[
            skill("deploy", "Deploy the app", "/repo/.skills/deploy/SKILL.md"),
            skill("a<b", "uses & \"quotes\"", "/repo/x/SKILL.md"),
        ]),
        "The following skills provide specialized instructions for specific tasks.\n\
         When a task matches a skill's description, use your file-read tool to load\n\
         the SKILL.md at the listed location before proceeding.\n\
         When a skill references relative paths, resolve them against the skill's\n\
         directory (the parent of SKILL.md) and use absolute paths in tool calls.\n\
         \n\
         <available_skills>\n\
         \x20 <skill>\n\
         \x20   <name>deploy</name>\n\
         \x20   <description>Deploy the app</description>\n\
         \x20   <location>/repo/.skills/deploy/SKILL.md</location>\n\
         \x20 </skill>\n\
         \x20 <skill>\n\
         \x20   <name>a&lt;b</name>\n\
         \x20   <description>uses &amp; &quot;quotes&quot;</description>\n\
         \x20   <location>/repo/x/SKILL.md</location>\n\
         \x20 </skill>\n\
         </available_skills>"
    );
}

/// Isolation hints: the sandboxed-implementor and CoW-coordinator layers.
#[test]
fn golden_isolation_hints() {
    let mut session = intent_core::AgentSession {
        harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
        harness_features: None,
        id: AgentId::from("agent-iso"),
        workspace_id: WorkspaceId::from("ws-1"),
        parent_agent_id: None,
        backend_session_id: None,
        acp_session_id: None,
        name: "Builder".to_string(),
        name_explicitly_set: false,
        model: None,
        reasoning_effort: None,
        effort_levels: None,
        provider: None,
        system_prompt: None,
        specialist: Some("implementor".to_string()),
        status: intent_core::AgentStatus::Active,
        is_active: false,
        messages: vec![],
        stats: None,
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
        created_at: now_iso(),
        updated_at: now_iso(),
        sandbox_id: None,
        sandbox_path: Some("/sandboxes/sb-1".to_string()),
        sandbox_branch: Some("sb/one".to_string()),
        stop_reason: None,
        stop_reason_timestamp: None,
        session_corrupted: false,
        pending_delete_at: None,
    };
    let specialist = crate::rules::SpecialistPromptInjection {
        behavior_prompt: None,
        specialist_name: Some("implementor".to_string()),
        role_reminder: None,
    };
    let hint = crate::rules::build_isolation_hint(None, Some(&session), Some(&specialist))
        .expect("sandboxed implementor hint");
    assert_eq!(
        hint,
        "## Workspace Isolation\n\n\
         You are working in an **isolated CoW (copy-on-write) sandbox** at `/sandboxes/sb-1` \
         on branch `sb/one` (base commit tracked in sandbox metadata). Your workspace is **isolated at the \
         filesystem level from other agents**: each agent works in its own copy-on-write \
         clone, so your file reads and writes cannot see or affect other agents' concurrent \
         changes (or the canonical checkout) until merge-back. Your dependency caches (node_modules, \
         target/, .venv, etc.) are warm — you inherited them from the canonical workspace.\n\n\
         **Critical constraints:**\n\
         - Do NOT switch branches or checkout other refs in your sandbox.\n\
         - On completion, the system automatically merges your branch back to the canonical workspace.\n\
         - If your changes conflict with canonical, you will be **woken with the conflicting paths** \
         and a ref to reconcile against. When that happens, resolve the conflicts **in your sandbox only** \
         (rebase or merge onto the fetched canonical ref), then end your turn again. The system will \
         retry the merge. Do NOT attempt to touch other checkouts or the canonical workspace directly.\n\
         - You have up to 2 conflict-resolution attempts before the merge is deferred to manual intervention."
    );
    // Coordinator variant: direct-mode CoW-supported workspace. Direct-mode
    // eligibility additionally requires a repository path (the same predicate
    // the delegate provisioning path resolves isolation from).
    session.sandbox_path = None;
    let mut ws = workspace(&WorkspaceId::from("ws-1"));
    ws.skip_worktree = true;
    ws.repository_path = Some("/repo".to_string());
    ws.cow_supported = Some(true);
    let specialist = crate::rules::SpecialistPromptInjection {
        behavior_prompt: None,
        specialist_name: Some("spec-writer".to_string()),
        role_reminder: None,
    };
    let hint = crate::rules::build_isolation_hint(Some(&ws), Some(&session), Some(&specialist))
        .expect("coordinator hint");
    assert!(hint.starts_with("## Agent Delegation & Isolation\n\n"));
    assert_eq!(
        sha256_hex(&hint),
        "a8efe4d8c64633f524b3fe5846883b84d769857bf39c604ba3c54e74af0dda2a"
    );
}

// ---------------------------------------------------------------------------
// Snapshot line + role reminder (`agent_ops.rs` / `lib.rs`)
// ---------------------------------------------------------------------------

/// The per-turn snapshot line: prefix + single-line camelCase JSON with
/// zero counts omitted. `time` is live, so pin the exact prefix and the
/// serialized field set separately.
#[tokio::test]
async fn golden_snapshot_line_shape() {
    let (_t, svc, ws) = setup().await;
    let owner = AgentId::from("agent-snap");
    seed_agent(&svc, &ws, &owner).await;
    svc.enqueue_message(&owner, "pending".into(), None, None, None, None, false);
    let line = svc
        .agent_state_snapshot_line(&owner)
        .await
        .expect("snapshot line");
    let json_part = line
        .strip_prefix("current ws.agent.snapshot() => ")
        .expect("prefix bytes");
    let v: serde_json::Value = serde_json::from_str(json_part).expect("valid JSON");
    let obj = v.as_object().expect("object");
    assert_eq!(
        obj.keys().collect::<Vec<_>>(),
        vec!["time", "queuedMessages"],
        "field set + order pin: {line}"
    );
    assert_eq!(obj["queuedMessages"], json!(1));
}

/// The full snapshot field catalog: every optional field populated, pinned
/// as exact serialized bytes so a rename, reorder, or serde-attribute change
/// on any field fails the golden (the live-line test above only exercises
/// `queuedMessages`).
#[test]
fn golden_snapshot_full_field_serialization() {
    let snap = crate::agent_ops::AgentSnapshot {
        time: "2026-01-02T03:04:05Z".to_string(),
        hooks: 1,
        agent_watches: 2,
        queued_messages: 3,
        event_subscriptions: 4,
        running_sub_agents: 5,
        num_questions_asked: 6,
        pr_monitors: vec![
            "intent-hq/intentd#7".to_string(),
            "intent-hq/monorepo#8 (changes pending)".to_string(),
        ],
        pending_attention: Some("blocker".to_string()),
    };
    assert_eq!(
        serde_json::to_string(&snap).unwrap(),
        "{\"time\":\"2026-01-02T03:04:05Z\",\"hooks\":1,\"agentWatches\":2,\
         \"queuedMessages\":3,\"eventSubscriptions\":4,\"runningSubAgents\":5,\
         \"numQuestionsAsked\":6,\"prMonitors\":[\"intent-hq/intentd#7\",\
         \"intent-hq/monorepo#8 (changes pending)\"],\"pendingAttention\":\"blocker\"}"
    );
}
