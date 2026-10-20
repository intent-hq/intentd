//! Unit coverage for the §5.5b worker projection. Cases mirror the monorepo's
//! executable model (`scripts/agent-worker-contract.test.mjs`).

use super::*;

fn scope(provider: &str) -> Scope {
    Scope {
        workspace_id: WorkspaceId::from("ws-A"),
        agent_id: AgentId::from("agent-A"),
        provider: provider.into(),
        acp_session_id: Some("session-A".into()),
        connection_generation: Some("generation-A".into()),
    }
}

fn identity(provider: &str) -> AdapterIdentity {
    let a = adapter_for(provider).expect("allowlisted");
    AdapterIdentity {
        name: Some(a.name.into()),
        version: Some(a.version.into()),
    }
}

fn codex() -> Projection {
    Projection::new(scope("codex"), &identity("codex"), true)
}

fn claude() -> Projection {
    Projection::new(scope("claude-code"), &identity("claude-code"), true)
}

fn frame(update: Value, at: u64) -> WorkerFrame {
    WorkerFrame {
        session_id: Some("session-A".into()),
        update,
        received_at_ms: at,
    }
}

fn spawn(id: &str) -> Value {
    json!({ "sessionUpdate": "async_task_spawned", "asyncTaskId": id, "taskType": "shell",
            "name": "synthetic command", "toolCallId": "call-A", "canStop": true, "showInTranscript": false })
}

fn with(mut update: Value, key: &str, value: Value) -> Value {
    update[key] = value;
    update
}

fn state(id: &str, s: &str) -> Value {
    json!({ "sessionUpdate": "async_task_state_update", "asyncTaskId": id, "state": s, "toolCallId": "call-A" })
}

fn rows(p: &mut Projection, now: u64) -> Vec<Value> {
    p.snapshot(now)["workers"].as_array().unwrap().clone()
}

fn ids(p: &mut Projection) -> Vec<String> {
    rows(p, 0)
        .iter()
        .map(|r| r["identity"]["asyncTaskId"].as_str().unwrap().to_owned())
        .collect()
}

#[test]
fn snapshot_shape_matches_the_contract() {
    let mut p = codex();
    assert!(p.ingest(&frame(spawn("worker-A"), 1000)));
    assert_eq!(
        p.snapshot(1000),
        json!({
            "workspaceId": "ws-A", "agentId": "agent-A", "provider": "codex",
            "acpSessionId": "session-A", "connectionGeneration": "generation-A",
            "support": { "status": "supported" }, "continuity": "continuous",
            "asOf": 1000, "truncated": false,
            "workers": [{
                "identity": { "workspaceId": "ws-A", "agentId": "agent-A", "provider": "codex",
                              "acpSessionId": "session-A", "connectionGeneration": "generation-A",
                              "asyncTaskId": "worker-A" },
                "toolCallId": "call-A", "name": "synthetic command", "taskType": "shell",
                "canStop": true, "firstObservedAt": 1000, "observedAt": 1000,
                "providerState": "running", "source": "codex-air", "cause": "unavailable",
                "exitEvidence": { "kind": "unknown" }, "status": "running"
            }]
        })
    );
}

#[test]
fn running_persists_until_an_end_report_and_age_never_demotes() {
    let mut p = codex();
    p.ingest(&frame(spawn("w"), 1000));
    for now in [1000, 61_000, 86_400_000] {
        let row = &rows(&mut p, now)[0];
        assert_eq!(row["status"], "running");
        assert!(row.get("unknownReason").is_none());
        assert_eq!(row["observedAt"], 1000);
    }
    assert!(p.ingest(&frame(state("w", "completed"), 86_400_001)));
    assert_eq!(rows(&mut p, 0)[0]["status"], "completed");
}

#[test]
fn disconnect_demotes_running_rows_and_refuses_later_frames() {
    let mut p = claude();
    p.ingest(&frame(spawn("w"), 1000));
    p.ingest(&frame(spawn("done"), 1000));
    p.ingest(&frame(state("done", "failed"), 2000));
    assert!(p.disconnect());
    assert!(!p.disconnect());
    let r = rows(&mut p, 3000);
    assert_eq!(r[0]["status"], "unknown");
    assert_eq!(r[0]["unknownReason"], "disconnected");
    assert_eq!(r[0]["providerState"], "running");
    assert_eq!(r[1]["status"], "failed", "ended rows keep their report");
    assert!(!p.ingest(&frame(state("w", "completed"), 4000)));
    assert_eq!(p.snapshot(4000)["continuity"], "disconnected");
}

#[test]
fn first_terminal_wins_and_late_or_duplicate_frames_change_nothing() {
    let mut p = codex();
    assert!(
        p.ingest(&frame(state("w", "failed"), 1000)),
        "terminal before spawn"
    );
    assert!(!p.ingest(&frame(spawn("w"), 2000)));
    assert!(!p.ingest(&frame(state("w", "completed"), 3000)));
    let row = &rows(&mut p, 0)[0];
    assert_eq!(row["providerState"], "failed");
    assert_eq!(row["observedAt"], 1000);
    assert_eq!(
        row["canStop"], false,
        "terminal-only row was never stoppable"
    );
    assert!(row.get("taskType").is_none());

    let mut dup = codex();
    dup.ingest(&frame(spawn("w"), 1000));
    assert!(
        !dup.ingest(&frame(spawn("w"), 70_000)),
        "duplicate spawn is no heartbeat"
    );
    assert_eq!(rows(&mut dup, 0)[0]["observedAt"], 1000);
    assert!(
        !dup.ingest(&frame(state("w", "failed"), 500)),
        "older ingress rejected"
    );
}

#[test]
fn non_terminal_states_progress_and_unknown_updates_are_ignored() {
    let mut p = claude();
    p.ingest(&frame(spawn("w"), 1000));
    for update in [
        state("w", "running"),
        state("w", "paused"),
        json!({ "sessionUpdate": "async_task_progress", "asyncTaskId": "w", "summary": "x" }),
        json!({ "sessionUpdate": "tool_call_update", "asyncTaskId": "w", "status": "completed" }),
    ] {
        assert!(!p.ingest(&frame(update, 2000)));
    }
    assert_eq!(rows(&mut p, 0)[0]["observedAt"], 1000);
}

#[test]
fn ownership_and_malformed_frames_reject_before_allocation() {
    let mut p = codex();
    let mut foreign = frame(spawn("w"), 1000);
    foreign.session_id = Some("child-session".into());
    assert!(!p.ingest(&foreign));
    let mut none = frame(spawn("w"), 1000);
    none.session_id = None;
    assert!(!p.ingest(&none));
    for update in [
        with(spawn("w"), "asyncTaskId", json!("")),
        with(spawn("w"), "asyncTaskId", json!("x".repeat(257))),
        with(spawn("w"), "asyncTaskId", json!("bad\n")),
        with(spawn("w"), "toolCallId", json!(4)),
        with(spawn("w"), "toolCallId", Value::Null),
        with(spawn("w"), "taskType", json!("agent")),
        state("w", "paused"),
    ] {
        assert!(!p.ingest(&frame(update, 1000)));
    }
    assert!(!p.ingest(&frame(spawn("w"), MAX_SAFE_INTEGER + 1)));
    assert!(p.seen.is_empty());
    let mut absent = spawn("w");
    absent.as_object_mut().unwrap().remove("toolCallId");
    assert!(p.ingest(&frame(absent, 1000)));
    assert!(rows(&mut p, 0)[0].get("toolCallId").is_none());
}

#[test]
fn names_are_sanitized_and_bounded() {
    let mut p = codex();
    p.ingest(&frame(
        with(
            spawn("w"),
            "name",
            json!(format!("a\tb{}", "é".repeat(300))),
        ),
        1000,
    ));
    let name = rows(&mut p, 0)[0]["name"].as_str().unwrap().to_owned();
    assert!(name.starts_with("a b"));
    assert!(name.len() <= 512);
}

#[test]
fn claude_reports_non_subagent_tasks_after_sanitizing_types() {
    let mut p = claude();
    for (id, t) in [
        ("a", " shell\n"),
        ("b", "workflow"),
        ("c", "monitor"),
        ("d", "task"),
    ] {
        assert!(
            p.ingest(&frame(with(spawn(id), "taskType", json!(t)), 1000)),
            "{t}"
        );
    }
    for (id, t) in [
        ("e", json!("local_agent")),
        ("f", json!("agent\n")),
        ("g", json!(" local_agent")),
        ("h", json!("\u{1}")),
        ("i", json!("")),
        ("j", json!(7)),
        ("k", Value::Null),
    ] {
        assert!(
            !p.ingest(&frame(with(spawn(id), "taskType", t.clone()), 1000)),
            "{t}"
        );
    }
    let types: Vec<Value> = rows(&mut p, 0)
        .iter()
        .map(|r| r["taskType"].clone())
        .collect();
    assert_eq!(
        types,
        vec![
            json!("shell"),
            json!("workflow"),
            json!("monitor"),
            json!("task")
        ]
    );
    assert!(rows(&mut p, 0).iter().all(|r| r["source"] == "claude-air"));
}

#[test]
fn codex_admits_only_shells() {
    for t in ["workflow", "monitor", "task"] {
        assert!(!codex().ingest(&frame(with(spawn("w"), "taskType", json!(t)), 1000)));
    }
}

#[test]
fn can_stop_requires_an_explicit_true() {
    for (value, expected) in [
        (json!(true), true),
        (json!(false), false),
        (json!("true"), false),
        (Value::Null, false),
    ] {
        let mut p = codex();
        p.ingest(&frame(with(spawn("w"), "canStop", value), 1000));
        assert_eq!(rows(&mut p, 0)[0]["canStop"], expected);
    }
}

#[test]
fn eviction_prefers_ended_rows_and_tombstones_block_recreation() {
    let mut p = Projection::with_bounds(scope("codex"), &identity("codex"), true, 2, 10);
    p.ingest(&frame(spawn("dev-server"), 1000));
    p.ingest(&frame(spawn("short"), 2000));
    p.ingest(&frame(state("short", "completed"), 3000));
    p.ingest(&frame(spawn("next"), 4000));
    assert_eq!(ids(&mut p), ["dev-server", "next"]);
    assert_eq!(p.snapshot(0)["truncated"], true);
    assert!(
        !p.ingest(&frame(spawn("short"), 5000)),
        "evicted ID stays fenced"
    );
    p.ingest(&frame(spawn("third"), 6000));
    assert_eq!(
        ids(&mut p),
        ["next", "third"],
        "only running rows left: oldest goes"
    );
}

#[test]
fn a_full_ledger_rejects_unseen_ids_but_retained_rows_can_still_end() {
    let mut p = Projection::with_bounds(scope("codex"), &identity("codex"), true, 2, 2);
    p.ingest(&frame(spawn("a"), 1000));
    p.ingest(&frame(spawn("b"), 1000));
    assert!(
        p.ingest(&frame(spawn("c"), 1000)),
        "latching truncated is a change"
    );
    assert!(!p.ingest(&frame(spawn("d"), 1000)));
    assert_eq!(ids(&mut p), ["a", "b"]);
    assert!(p.ingest(&frame(state("a", "completed"), 2000)));
    assert_eq!(p.seen.len(), 2);
}

#[test]
fn support_reasons_follow_the_documented_order() {
    let bad_version = AdapterIdentity {
        name: Some("@agentclientprotocol/codex-acp".into()),
        version: Some("1.13.1".into()),
    };
    let wrong_name = AdapterIdentity {
        name: Some("@agentclientprotocol/claude-agent-acp".into()),
        version: Some("2.2.2".into()),
    };
    for (provider, ident, negotiated, reason) in [
        ("auggie", identity("codex"), true, "provider"),
        ("claude", identity("claude-code"), true, "provider"),
        ("codex", bad_version, true, "adapter-version"),
        ("codex", wrong_name, true, "adapter-version"),
        ("codex", AdapterIdentity::default(), true, "adapter-version"),
        ("codex", identity("codex"), false, "not-negotiated"),
        (
            "claude-code",
            identity("claude-code"),
            false,
            "not-negotiated",
        ),
    ] {
        let mut p = Projection::new(scope(provider), &ident, negotiated);
        assert_eq!(
            p.snapshot(0)["support"],
            json!({ "status": "unsupported", "reason": reason }),
            "{provider}"
        );
        assert!(!p.ingest(&frame(spawn("w"), 1000)));
    }
}

#[test]
fn provider_profiles_use_their_base_provider() {
    let mut s = scope("claude-code@work");
    let mut p = Projection::new(s.clone(), &identity("claude-code"), true);
    assert_eq!(p.snapshot(0)["support"]["status"], "supported");
    assert!(p.ingest(&frame(
        with(spawn("w"), "taskType", json!("workflow")),
        1000
    )));
    let row = &rows(&mut p, 0)[0];
    assert_eq!(row["source"], "claude-air");
    assert_eq!(row["identity"]["provider"], "claude-code@work");
    s.provider = "codex@p2".into();
    let mut x = Projection::new(s, &identity("codex"), true);
    assert!(!x.ingest(&frame(
        with(spawn("w"), "taskType", json!("workflow")),
        1000
    )));
    assert!(x.ingest(&frame(spawn("w"), 1000)));
}

#[test]
fn no_session_snapshots_are_unavailable_and_empty() {
    let mut p = Projection::no_session(
        WorkspaceId::from("ws-A"),
        AgentId::from("agent-A"),
        "codex".into(),
    );
    let s = p.snapshot(5);
    assert_eq!(
        s["support"],
        json!({ "status": "unavailable", "reason": "no-session" })
    );
    assert_eq!(s["continuity"], "lost");
    assert_eq!(s["acpSessionId"], Value::Null);
    assert_eq!(s["connectionGeneration"], Value::Null);
    assert_eq!(s["workers"], json!([]));

    let mut unbound = Projection::new(
        Scope {
            acp_session_id: None,
            ..scope("codex")
        },
        &identity("codex"),
        true,
    );
    assert_eq!(unbound.snapshot(0)["support"]["reason"], "no-session");
    assert!(!unbound.ingest(&frame(spawn("w"), 1000)));
    assert!(unbound.bind_session("session-A"));
    assert!(!unbound.bind_session("session-A"));
    assert!(unbound.ingest(&frame(spawn("w"), 1000)));
}

#[test]
fn a_new_session_on_the_same_connection_clears_rows_and_tombstones() {
    let mut p = codex();
    p.ingest(&frame(spawn("w"), 1000));
    assert!(p.bind_session("session-B"));
    assert_eq!(rows(&mut p, 0).len(), 0);
    let mut f = frame(spawn("w"), 2000);
    f.session_id = Some("session-B".into());
    assert!(p.ingest(&f), "IDs are scoped by session");
}

#[test]
fn backward_time_cannot_rewind_the_projection_clock() {
    let mut p = codex();
    p.ingest(&frame(spawn("w"), 1000));
    assert_eq!(p.snapshot(70_000)["asOf"], 70_000);
    assert_eq!(p.snapshot(2000)["asOf"], 70_000);
    assert_eq!(rows(&mut p, 2000)[0]["observedAt"], 1000);
}

#[test]
fn stop_forwards_only_for_a_connected_running_stoppable_worker_of_this_generation() {
    let mut p = codex();
    p.ingest(&frame(spawn("run"), 1000));
    p.ingest(&frame(with(spawn("locked"), "canStop", json!(false)), 1000));
    p.ingest(&frame(spawn("done"), 1000));
    p.ingest(&frame(state("done", "completed"), 2000));
    assert_eq!(
        p.stop_decision("generation-A", "run"),
        Ok(StopDecision::Forward {
            session_id: "session-A".into()
        })
    );
    assert_eq!(
        p.stop_decision("generation-A", "locked"),
        Ok(StopDecision::Decline)
    );
    assert_eq!(
        p.stop_decision("generation-A", "done"),
        Ok(StopDecision::Decline)
    );
    assert_eq!(p.stop_decision("generation-A", "missing"), Err(()));
    assert_eq!(
        p.stop_decision("generation-old", "run"),
        Err(()),
        "stale generation is not-found"
    );
    p.disconnect();
    assert_eq!(
        p.stop_decision("generation-A", "run"),
        Ok(StopDecision::Decline)
    );
}

#[test]
fn registry_fences_every_operation_to_the_current_generation() {
    let workers = AgentWorkers::default();
    let agent = AgentId::from("agent-A");
    let unbound = Scope {
        acp_session_id: None,
        ..scope("codex")
    };
    let first = workers.begin(unbound.clone(), &identity("codex"), true, Weak::new());
    assert_eq!(first["support"]["reason"], "no-session");
    assert!(workers
        .bind_session(&agent, "generation-A", "session-A")
        .is_some());
    assert!(workers
        .ingest(&agent, "generation-A", &frame(spawn("w"), 1000))
        .is_some());
    assert!(
        workers
            .ingest(&agent, "generation-A", &frame(spawn("w"), 1000))
            .is_none(),
        "no change, no snapshot"
    );
    assert!(workers
        .ingest(&agent, "generation-old", &frame(spawn("x"), 1000))
        .is_none());
    assert!(matches!(
        workers.stop_target(&agent, "generation-A", "w"),
        Some(StopTarget::ConnectionGone)
    ));
    assert!(workers.stop_target(&agent, "generation-old", "w").is_none());

    let next = Scope {
        connection_generation: Some("generation-B".into()),
        ..unbound
    };
    let snapshot = workers.begin(next, &identity("codex"), true, Weak::new());
    assert_eq!(
        snapshot["workers"],
        json!([]),
        "a new generation starts empty"
    );
    assert!(
        workers.disconnect(&agent, "generation-A").is_none(),
        "old generation cannot disconnect the new one"
    );
    assert!(workers.disconnect(&agent, "generation-B").is_some());
    let released = workers.release(&agent).expect("held");
    assert_eq!(released["support"]["reason"], "no-session");
    assert_eq!(released["workers"], json!([]));
    assert!(workers.release(&agent).is_none());
    assert!(workers.snapshot(&agent).is_none());
}

#[test]
fn claude_allowlist_tracks_the_pinned_adapter() {
    // Moving the Claude adapter pin requires reviewing its worker reports
    // (PROTOCOL §5.5b) and updating SUPPORTED_ADAPTERS with it.
    let claude = adapter_for("claude-code").unwrap();
    assert_eq!(
        claude.version,
        intent_providers::CLAUDE_AGENT_ACP_VERSION,
        "claude-agent-acp pin moved: review its async-task reports, then update the §5.5b allowlist"
    );
    assert_eq!(
        format!("{}@{}", claude.name, claude.version),
        intent_providers::CLAUDE_AGENT_ACP_NPX_PACKAGE
    );
    let codex = adapter_for("codex").unwrap();
    assert_eq!(
        format!("{}@{}", codex.name, codex.version),
        intent_providers::CODEX_ACP_NPX_PACKAGE,
        "codex-acp pin moved: review its async-task reports, then update the §5.5b allowlist"
    );
}

#[test]
fn negotiation_after_spawn_sets_support_without_rows() {
    let workers = AgentWorkers::default();
    let agent = AgentId::from("agent-A");
    let unbound = Scope {
        acp_session_id: None,
        ..scope("claude-code")
    };
    workers.begin(unbound, &AdapterIdentity::default(), false, Weak::new());
    assert_eq!(
        workers.current_generation(&agent).as_deref(),
        Some("generation-A")
    );
    assert!(workers
        .negotiate(&agent, "generation-old", &identity("claude-code"), true)
        .is_none());
    let s = workers
        .negotiate(&agent, "generation-A", &identity("claude-code"), true)
        .unwrap();
    assert_eq!(s["support"]["reason"], "no-session", "still unbound");
    let bound = workers
        .bind_session(&agent, "generation-A", "session-A")
        .unwrap();
    assert_eq!(bound["support"], json!({ "status": "supported" }));
    let wrong = AdapterIdentity {
        version: Some("0.0.1".into()),
        ..AdapterIdentity::default()
    };
    workers.negotiate(&agent, "generation-A", &wrong, true);
    let rebound = workers
        .bind_session(&agent, "generation-A", "session-A")
        .unwrap();
    assert_eq!(rebound["support"]["reason"], "adapter-version");
}
