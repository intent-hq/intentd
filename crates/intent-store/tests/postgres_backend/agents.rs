//! Agent sessions, child and interrupted agents, persisted queues, task
//! links, flipped completions, wake markers and review-context enrollment on
//! both backends. Bodies use fixed ids, leave their rows behind and assert
//! exact values from an empty start, so a Postgres statement without its
//! tenant filter fails the subject run. Writes use tenant-specific values
//! (or reset decoy-only state) so a leaking write changes a decoy row.

use intent_core::prompt_breakdown::PromptBreakdown;
use intent_core::{
    AgentId, AgentListRowScope, AgentMessage, AgentSession, AgentStatus, Error, NoteId,
    TaskAgentLink, TokenUsageTotals, WorkspaceId,
};
use intent_store::{AgentQueueRow, AgentTurnEffort, Store};
use serde_json::{json, Value};

use crate::harness::{open, parity_test, sqlite_store, tenant_rows, test_db};

const WS: &str = "__chief__";

fn ws() -> WorkspaceId {
    WorkspaceId(WS.into())
}

fn aid(id: &str) -> AgentId {
    AgentId(id.into())
}

fn session(id: &str, tenant: &str, created_at: &str) -> AgentSession {
    AgentSession {
        id: aid(id),
        workspace_id: ws(),
        name: format!("{id} of {tenant}"),
        model: Some("opus".into()),
        provider: Some("claude".into()),
        status: AgentStatus::Idle,
        system_prompt: Some("prompt".into()),
        initial_message: Some("hello".into()),
        specialist: Some(format!("spec-{tenant}")),
        metadata: Some(json!({ "tenant": tenant })),
        context_references: Some(json!([{ "kind": "note" }])),
        created_at: created_at.into(),
        updated_at: created_at.into(),
        ..AgentSession::default()
    }
}

fn names<T: std::ops::Deref<Target = AgentSession>>(rows: &[T]) -> Vec<&str> {
    rows.iter().map(|s| s.id.as_str()).collect()
}

fn not_found<T: std::fmt::Debug>(r: &intent_core::Result<T>) -> bool {
    matches!(r, Err(Error::NotFound(_)))
}

async fn metadata(store: &Store, id: &str) -> Option<Value> {
    store
        .get_agent_session_without_messages(&aid(id))
        .await
        .unwrap()
        .metadata
}

/// Five sessions: `a` top-level, `b` its running child, `c` background,
/// `d` a child of a missing parent, `e` retired.
async fn seed_sessions(store: &Store, tenant: &str) {
    let top = session("agent-a", tenant, "2026-01-01T00:00:01Z");
    store
        .insert_agent_session_with_preferences(&top, true, true)
        .await
        .unwrap();
    let mut child = session("agent-b", tenant, "2026-01-01T00:00:02Z");
    child.parent_agent_id = Some(aid("agent-a"));
    child.status = AgentStatus::Active;
    child.is_active = true;
    child.completion_report = Some("done".into());
    child.provider = Some("codex".into());
    store.insert_agent_session(&child).await.unwrap();
    let mut background = session("agent-c", tenant, "2026-01-01T00:00:03Z");
    background.is_background = true;
    background.model = None;
    background.provider = None;
    store
        .insert_agent_session_with_task_graph(&background, true)
        .await
        .unwrap();
    let mut orphan = session("agent-d", tenant, "2026-01-01T00:00:04Z");
    orphan.parent_agent_id = Some(aid("agent-gone"));
    orphan.status = AgentStatus::Completed;
    store.insert_agent_session(&orphan).await.unwrap();
    let mut retired = session("agent-e", tenant, "2026-01-01T00:00:05Z");
    retired.retired_at = Some("2026-01-02T00:00:00Z".into());
    store.insert_agent_session(&retired).await.unwrap();
}

/// Decoy-only sessions with the ids the subject probes as missing
/// (`agent-zz`) or as a missing parent (`agent-gone`): a lookup without its
/// tenant filter finds them on the subject run.
async fn decoy_only_sessions(store: &Store, tenant: &str) {
    if tenant == "decoy" {
        for id in ["agent-zz", "agent-gone"] {
            store
                .insert_agent_session(&session(id, tenant, "2026-01-01T00:00:09Z"))
                .await
                .unwrap();
        }
    }
}

parity_test!(agent_sessions_insert_and_read, |store, tenant| {
    assert!(store
        .list_agent_session_summaries(&ws())
        .await
        .unwrap()
        .is_empty());
    assert!(not_found(
        &store.get_agent_session_summary(&aid("agent-a")).await
    ));
    seed_sessions(&store, tenant).await;
    let dup = store
        .insert_agent_session(&session("agent-a", tenant, "x"))
        .await
        .unwrap_err()
        .to_string();
    assert!(dup.contains("collided"), "{dup}");

    let a = session("agent-a", tenant, "2026-01-01T00:00:01Z");
    assert_eq!(
        store
            .get_agent_session_without_messages(&aid("agent-a"))
            .await
            .unwrap(),
        a
    );
    let summary = store
        .get_agent_session_summary(&aid("agent-a"))
        .await
        .unwrap();
    assert_eq!(
        (summary.name.as_str(), summary.system_prompt.as_deref()),
        (a.name.as_str(), None)
    );
    assert_eq!(
        store.get_agent_creation_preferences(&ws()).await.unwrap(),
        json!({ "specialistId": format!("spec-{tenant}") })
    );
    assert_eq!(
        store
            .get_agent_statuses(&[aid("agent-b"), aid("agent-a"), aid("agent-zz")])
            .await
            .unwrap(),
        [
            (aid("agent-a"), AgentStatus::Idle),
            (aid("agent-b"), AgentStatus::Active)
        ]
    );
    assert!(store
        .get_agent_session_task_graph_enabled(&aid("agent-a"))
        .await
        .unwrap());
    assert!(!store
        .get_agent_session_task_graph_enabled(&aid("agent-b"))
        .await
        .unwrap());
    assert_eq!(
        store
            .materialize_agent_session_harness_features(
                &aid("agent-b"),
                &json!({"taskGraph": true})
            )
            .await
            .unwrap(),
        Some(json!({"taskGraph": true}))
    );
    assert_eq!(
        store
            .materialize_agent_session_harness_features(
                &aid("agent-b"),
                &json!({"taskGraph": false})
            )
            .await
            .unwrap(),
        Some(json!({"taskGraph": true}))
    );
    assert!(store
        .get_agent_session_task_graph_enabled(&aid("agent-b"))
        .await
        .unwrap());
    assert!(not_found(
        &store
            .materialize_agent_session_harness_features(&aid("agent-zz"), &json!({}))
            .await
    ));
    // The decoy's agent-e keeps no snapshot for the subject's write to reach.
    if tenant != "decoy" {
        let snapshot = json!({ "taskGraph": false, "tenant": tenant });
        assert_eq!(
            store
                .materialize_agent_session_harness_features(&aid("agent-e"), &snapshot)
                .await
                .unwrap(),
            Some(snapshot)
        );
    }
    assert_eq!(
        store
            .get_agent_session_status(&aid("agent-b"))
            .await
            .unwrap(),
        AgentStatus::Active
    );
    assert_eq!(
        store
            .get_agent_session_updated_at(&aid("agent-c"))
            .await
            .unwrap(),
        "2026-01-01T00:00:03Z"
    );
    let stamp = format!("2026-01-01T00:00:01Z {tenant}");
    store
        .refresh_agent_session_timestamp(&ws(), &aid("agent-a"), &stamp)
        .await
        .unwrap();
    let batch = store
        .get_agent_session_updated_at_batch(&[aid("agent-a"), aid("agent-zz")])
        .await
        .unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[&aid("agent-a")], stamp);
    assert_eq!(
        store.get_agent_session_name(&aid("agent-a")).await.unwrap(),
        Some(format!("agent-a of {tenant}"))
    );
    assert_eq!(
        store
            .get_agent_session_name(&aid("agent-zz"))
            .await
            .unwrap(),
        None
    );

    let all = ["agent-a", "agent-b", "agent-c", "agent-d", "agent-e"];
    assert_eq!(
        names(&store.list_agent_session_summaries(&ws()).await.unwrap()),
        all
    );
    let every: Vec<String> = store
        .list_all_agent_sessions()
        .await
        .unwrap()
        .into_iter()
        .map(|s| s.id.0)
        .collect();
    assert_eq!(every, all);
    assert_eq!(
        names(
            &store
                .list_active_agent_session_summaries(&ws())
                .await
                .unwrap()
        ),
        ["agent-a", "agent-b", "agent-c", "agent-d"]
    );
    assert_eq!(
        names(
            &store
                .list_retired_agent_session_summaries(&ws())
                .await
                .unwrap()
        ),
        ["agent-e"]
    );
    assert_eq!(store.count_retired_agent_sessions(&ws()).await.unwrap(), 1);
    let by_ws = store
        .list_agent_session_summaries_by_workspace(&[ws(), WorkspaceId("ws-none".into())])
        .await
        .unwrap();
    assert_eq!(by_ws.len(), 1);
    assert_eq!(names(&by_ws[&ws()]), all);

    let scoped = |scope: AgentListRowScope| {
        let store = &store;
        async move {
            store
                .list_scoped_agent_session_summaries(&ws(), &scope)
                .await
                .unwrap()
                .iter()
                .map(|s| s.id.0.clone())
                .collect::<Vec<_>>()
        }
    };
    assert_eq!(scoped(AgentListRowScope::TopLevel).await, ["agent-a"]);
    assert_eq!(scoped(AgentListRowScope::Background).await, ["agent-c"]);
    assert_eq!(
        scoped(AgentListRowScope::Delegated {
            parent_agent_id: None,
            orphaned_only: false
        })
        .await,
        ["agent-b", "agent-d"]
    );
    assert_eq!(
        scoped(AgentListRowScope::Delegated {
            parent_agent_id: None,
            orphaned_only: true
        })
        .await,
        ["agent-d"]
    );
    assert_eq!(
        scoped(AgentListRowScope::Delegated {
            parent_agent_id: Some(aid("agent-a")),
            orphaned_only: false
        })
        .await,
        ["agent-b"]
    );
    let counts = store.count_agent_sessions_by_scope(&ws()).await.unwrap();
    assert_eq!(
        (counts.top_level, counts.delegated, counts.background),
        (1, 2, 1)
    );
    let delegated = store
        .count_delegated_agent_sessions_by_parent(&ws())
        .await
        .unwrap();
    assert_eq!(delegated.running, 1);
    assert_eq!(
        (delegated.orphaned.total, delegated.orphaned.running),
        (1, 0)
    );
    assert_eq!(delegated.by_parent.len(), 2);
    assert_eq!(delegated.by_parent[&aid("agent-a")].total, 1);
    assert_eq!(delegated.by_parent[&aid("agent-a")].running, 1);
    assert_eq!(delegated.by_parent[&aid("agent-gone")].total, 1);

    let children = store.count_child_agents(&aid("agent-a")).await.unwrap();
    assert_eq!(
        (children.active, children.unsettled, children.running),
        (1, 1, 1)
    );
    assert_eq!(
        store
            .count_unsettled_child_agents(&aid("agent-gone"))
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        names(
            &store
                .list_child_agent_summaries(&aid("agent-a"))
                .await
                .unwrap()
        ),
        ["agent-b"]
    );
    decoy_only_sessions(&store, tenant).await;
});

parity_test!(
    agent_session_full_row_and_metadata_writers,
    |store, tenant| {
        Box::pin(full_row_and_metadata_writers(&store, tenant)).await;
    }
);

async fn full_row_and_metadata_writers(store: &Store, tenant: &str) {
    seed_sessions(store, tenant).await;
    let at = |n: u32| format!("2026-02-01T00:00:{n:02}Z {tenant}");
    let (w, a) = (ws(), aid("agent-a"));

    // A manual choice is a daemon-owned key: whole-document writes keep it.
    store
        .set_agent_session_model_choice(&w, &a, "opus", Some("claude"), Some("tok"), &at(1))
        .await
        .unwrap();
    assert_eq!(
        metadata(store, "agent-a").await,
        Some(json!({ "tenant": tenant, "manualChoice": "tok" }))
    );
    let mut row = store.get_agent_session_without_messages(&a).await.unwrap();
    row.name = format!("renamed {tenant}");
    row.status = AgentStatus::Waiting;
    row.metadata = Some(json!({ "x": tenant, "manualChoice": "forged" }));
    row.updated_at = at(2);
    store
        .update_agent_session_with_preferences(&w, &row, true, Some(true), Some(true), false)
        .await
        .unwrap();
    let got = store.get_agent_session_without_messages(&a).await.unwrap();
    assert_eq!(got.name, format!("renamed {tenant}"));
    assert_eq!(got.status, AgentStatus::Waiting);
    assert!(got.notifications_muted && got.pinned);
    assert_eq!(
        got.metadata,
        Some(json!({ "x": tenant, "manualChoice": "tok" }))
    );
    assert!(not_found(
        &store
            .update_agent_session(&WorkspaceId("ws-none".into()), &row)
            .await
    ));
    assert!(!store
        .take_agent_session_manual_choice(&w, &a, "wrong")
        .await
        .unwrap());
    assert!(store
        .take_agent_session_manual_choice(&w, &a, "tok")
        .await
        .unwrap());
    assert_eq!(
        metadata(store, "agent-a").await,
        Some(json!({ "x": tenant }))
    );

    // Selection changes reset the manual choice; whole writes after it keep the rest.
    store
        .set_agent_session_model_choice(&w, &a, "opus", Some("claude"), Some("t2"), &at(3))
        .await
        .unwrap();
    store
        .set_agent_session_model(&w, &a, "opus", Some("claude"), &at(4))
        .await
        .unwrap();
    assert_eq!(
        metadata(store, "agent-a").await,
        Some(json!({ "x": tenant }))
    );

    store
        .update_agent_session_metadata(&w, &a, Some(&json!({ "y": tenant })), &at(5))
        .await
        .unwrap();
    assert_eq!(
        metadata(store, "agent-a").await,
        Some(json!({ "y": tenant }))
    );
    assert!(not_found(
        &store
            .update_agent_session_metadata(&w, &aid("agent-zz"), None, &at(5))
            .await
    ));

    let key = "lastSeenMessageId";
    assert!(store
        .set_agent_session_metadata_key(&w, &a, key, "m1", None, Some(&at(6)))
        .await
        .unwrap());
    assert!(!store
        .set_agent_session_metadata_key(&w, &a, key, "m2", Some(Some("m0")), None)
        .await
        .unwrap());
    assert!(store
        .set_agent_session_metadata_key(&w, &a, key, "m2", Some(Some("m1")), None)
        .await
        .unwrap());
    assert!(not_found(
        &store
            .set_agent_session_metadata_key(&w, &aid("agent-zz"), key, "m", Some(None), None)
            .await
    ));
    assert_eq!(
        store
            .take_agent_session_metadata_key(&w, &a, key)
            .await
            .unwrap(),
        Some("m2".into())
    );
    assert_eq!(
        store
            .take_agent_session_metadata_key(&w, &a, key)
            .await
            .unwrap(),
        None
    );
    store
        .set_agent_session_metadata_key_json(&w, &a, "obj", r#"{"n":1}"#, &at(7))
        .await
        .unwrap();
    assert_eq!(
        metadata(store, "agent-a").await,
        Some(json!({ "y": tenant, "obj": { "n": 1 } }))
    );
    let got = store.get_agent_session_without_messages(&a).await.unwrap();
    assert_eq!(got.updated_at, at(7));

    // Provider moves.
    store.set_acp_session_id(&w, &a, "acp-1").await.unwrap();
    store.set_acp_session_id(&w, &a, "acp-1").await.unwrap();
    assert!(store.set_acp_session_id(&w, &a, "acp-x").await.is_err());
    let mut moved = store.get_agent_session_without_messages(&a).await.unwrap();
    moved.provider = Some("codex".into());
    assert!(store.update_agent_session(&w, &moved).await.is_err());
    assert!(store
        .rehome_agent_session_provider(&w, &a, Some("claude"), "codex", Some("gpt"), &at(8))
        .await
        .unwrap());
    assert!(!store
        .rehome_agent_session_provider(&w, &a, Some("claude"), "codex", Some("gpt"), &at(8))
        .await
        .unwrap());
    assert!(not_found(
        &store
            .rehome_agent_session_provider(&w, &aid("agent-zz"), None, "codex", None, &at(8))
            .await
    ));
    assert!(store
        .failover_agent_session_provider(
            &w,
            &a,
            "codex",
            Some("gpt"),
            "claude",
            Some(("providerReturn", r#"{"to":"codex"}"#)),
            true,
            &at(9),
        )
        .await
        .unwrap());
    assert!(!store
        .failover_agent_session_provider(
            &w,
            &a,
            "codex",
            Some("gpt"),
            "claude",
            None,
            false,
            &at(9)
        )
        .await
        .unwrap());
    assert!(store
        .set_agent_session_metadata_key_json_if_identity(
            &w,
            &a,
            "claude",
            Some("gpt"),
            "k",
            "[1,2]",
            true,
            &at(10),
        )
        .await
        .unwrap());
    assert!(!store
        .set_agent_session_metadata_key_json_if_identity(
            &w,
            &a,
            "codex",
            Some("gpt"),
            "k",
            "[3]",
            false,
            &at(10),
        )
        .await
        .unwrap());
    assert_eq!(
        metadata(store, "agent-a").await,
        Some(
            json!({ "y": tenant, "obj": { "n": 1 }, "providerReturn": { "to": "codex" }, "k": [1, 2] })
        )
    );
    // The return state is daemon-owned too.
    store
        .update_agent_session_metadata(&w, &a, Some(&json!(["not", "an", "object"])), &at(11))
        .await
        .unwrap();
    assert_eq!(
        metadata(store, "agent-a").await,
        Some(
            json!({ "priorNonObjectMetadata": ["not", "an", "object"], "providerReturn": { "to": "codex" } })
        )
    );
    let got = store.get_agent_session_without_messages(&a).await.unwrap();
    assert_eq!(
        (got.provider.as_deref(), got.model.as_deref()),
        (Some("claude"), Some("gpt"))
    );

    let c = aid("agent-c");
    assert!(store
        .pin_agent_session_provider_if_null(&w, &c, "claude", &at(12))
        .await
        .unwrap());
    assert!(!store
        .pin_agent_session_provider_if_null(&w, &c, "codex", &at(12))
        .await
        .unwrap());
    assert!(not_found(
        &store
            .pin_agent_session_provider_if_null(&WorkspaceId("ws-none".into()), &c, "x", &at(12))
            .await
    ));
    assert!(store
        .fill_agent_session_identity_if_null(&w, &c, Some("sonnet"), Some("high"), &at(13))
        .await
        .unwrap());
    assert!(!store
        .fill_agent_session_identity_if_null(&w, &c, Some("haiku"), Some("low"), &at(13))
        .await
        .unwrap());
    assert!(store
        .fill_agent_session_effort_if_identity(
            &w,
            &aid("agent-b"),
            "low",
            Some("codex"),
            Some("opus"),
            &at(14)
        )
        .await
        .unwrap());
    let got = store.get_agent_session_without_messages(&c).await.unwrap();
    assert_eq!(
        (
            got.model.as_deref(),
            got.reasoning_effort.as_deref(),
            got.provider.as_deref()
        ),
        (Some("sonnet"), Some("high"), Some("claude"))
    );
    // A model change drops an effort the daemon pinned from Auto.
    store
        .set_agent_session_model(&w, &c, "haiku", Some("claude"), &at(15))
        .await
        .unwrap();
    let got = store.get_agent_session_without_messages(&c).await.unwrap();
    assert_eq!(got.reasoning_effort, None);
    assert!(not_found(
        &store
            .set_agent_session_model(&w, &aid("agent-zz"), "m", None, &at(15))
            .await
    ));
    assert!(not_found(
        &store
            .pin_agent_session_provider_if_null(&w, &aid("agent-zz"), "x", &at(16))
            .await
    ));
    if tenant == "decoy" {
        // Back to the state the subject's guarded writes start from.
        let mut row = store.get_agent_session_without_messages(&c).await.unwrap();
        (row.provider, row.model, row.reasoning_effort) = (None, None, None);
        store.update_agent_session(&w, &row).await.unwrap();
        let mut row = store
            .get_agent_session_without_messages(&aid("agent-b"))
            .await
            .unwrap();
        row.reasoning_effort = None;
        store.update_agent_session(&w, &row).await.unwrap();
    }
    decoy_only_sessions(store, tenant).await;
}

parity_test!(agent_session_state_writers, |store, tenant| {
    Box::pin(state_writers(&store, tenant)).await;
});

async fn state_writers(store: &Store, tenant: &str) {
    seed_sessions(store, tenant).await;
    let at = |n: u32| format!("2026-03-01T00:00:{n:02}Z {tenant}");
    let (w, a, b) = (ws(), aid("agent-a"), aid("agent-b"));
    let missing = aid("agent-zz");

    assert!(store
        .set_agent_notifications_muted(&w, &a, true)
        .await
        .unwrap());
    assert!(!store
        .set_agent_notifications_muted(&w, &a, true)
        .await
        .unwrap());
    assert!(not_found(
        &store
            .set_agent_notifications_muted(&w, &missing, true)
            .await
    ));
    assert!(store.set_agent_pinned(&w, &a, true).await.unwrap());
    assert!(!store.set_agent_pinned(&w, &a, true).await.unwrap());
    let policy = format!("ask-{tenant}");
    assert!(store
        .set_agent_permission_policy(&w, &a, Some(&policy))
        .await
        .unwrap());
    assert!(!store
        .set_agent_permission_policy(&w, &a, Some(&policy))
        .await
        .unwrap());
    assert!(not_found(
        &store.set_agent_permission_policy(&w, &missing, None).await
    ));

    let breakdown: PromptBreakdown = serde_json::from_value(json!({
        "harnessVersion": "1.0", "recordedAt": tenant, "systemPromptChars": 3,
        "toolDescriptionChars": 0, "totalChars": 3, "estTokens": 1,
        "sections": [], "hiddenSkills": []
    }))
    .unwrap();
    store
        .set_agent_session_system_prompt_with_breakdown(&w, &a, "new", Some(&breakdown))
        .await
        .unwrap();
    assert_eq!(
        store
            .get_agent_session_prompt_breakdown(&a, Some("new"))
            .await
            .unwrap(),
        Some(breakdown)
    );
    assert_eq!(
        store
            .get_agent_session_prompt_breakdown(&a, Some("other"))
            .await
            .unwrap(),
        None
    );
    // A full-row write with the same prompt keeps the breakdown; a new prompt drops it.
    let mut row = store.get_agent_session_without_messages(&a).await.unwrap();
    store.update_agent_session(&w, &row).await.unwrap();
    assert!(store
        .get_agent_session_prompt_breakdown(&a, Some("new"))
        .await
        .unwrap()
        .is_some());
    row.system_prompt = Some("newer".into());
    store.update_agent_session(&w, &row).await.unwrap();
    assert_eq!(
        store
            .get_agent_session_prompt_breakdown(&a, Some("newer"))
            .await
            .unwrap(),
        None
    );
    store
        .set_agent_session_system_prompt(&w, &a, &format!("p-{tenant}"))
        .await
        .unwrap();
    assert!(not_found(
        &store
            .set_agent_session_system_prompt(&w, &missing, "p")
            .await
    ));
    assert!(not_found(
        &store
            .get_agent_session_prompt_breakdown(&missing, None)
            .await
    ));

    store
        .set_agent_session_status(
            &w,
            &b,
            AgentStatus::Waiting,
            false,
            &at(1),
            Some(Some("done".into())),
        )
        .await
        .unwrap();
    let got = store.get_agent_session_summary(&b).await.unwrap();
    assert_eq!(
        (
            got.status,
            got.is_active,
            got.stop_reason.as_deref(),
            got.stop_reason_timestamp.clone()
        ),
        (AgentStatus::Waiting, false, Some("done"), Some(at(1)))
    );
    store
        .set_agent_session_status(&w, &b, AgentStatus::Active, true, &at(2), None)
        .await
        .unwrap();
    assert_eq!(
        store
            .get_agent_session_summary(&b)
            .await
            .unwrap()
            .stop_reason
            .as_deref(),
        Some("done")
    );
    assert!(not_found(
        &store
            .set_agent_session_status(&w, &missing, AgentStatus::Idle, false, &at(2), None)
            .await
    ));
    store
        .refresh_agent_session_timestamp(&w, &b, &at(3))
        .await
        .unwrap();
    assert_eq!(store.get_agent_session_updated_at(&b).await.unwrap(), at(3));
    assert!(not_found(
        &store
            .refresh_agent_session_timestamp(&w, &missing, &at(3))
            .await
    ));

    assert!(store
        .set_agent_session_retired_at(&w, &a, Some(&at(4)), &at(4))
        .await
        .unwrap());
    assert!(!store
        .set_agent_session_retired_at(&w, &a, Some(&at(5)), &at(5))
        .await
        .unwrap());
    assert_eq!(
        store.get_agent_session_retired_at(&a).await.unwrap(),
        Some(at(4))
    );
    assert!(store
        .set_agent_session_retired_at(&w, &a, None, &at(6))
        .await
        .unwrap());
    assert_eq!(store.get_agent_session_retired_at(&a).await.unwrap(), None);
    assert!(not_found(
        &store.get_agent_session_retired_at(&missing).await
    ));

    assert!(store.clear_completion_report(&w, &b, &at(7)).await.unwrap());
    assert!(!store.clear_completion_report(&w, &b, &at(7)).await.unwrap());
    assert!(not_found(
        &store.clear_completion_report(&w, &missing, &at(7)).await
    ));
    store
        .set_attention_request(&w, &a, "question", &format!("why {tenant}"), &at(8))
        .await
        .unwrap();
    let got = store.get_agent_session_summary(&a).await.unwrap();
    assert_eq!(got.attention_request_kind.as_deref(), Some("question"));
    assert!(store.clear_attention_request(&w, &a, &at(9)).await.unwrap());
    assert!(!store.clear_attention_request(&w, &a, &at(9)).await.unwrap());
    assert!(not_found(
        &store
            .set_attention_request(&w, &missing, "k", "r", "t")
            .await
    ));
    store
        .set_attention_request(&w, &a, "blocker", "stuck", &at(10))
        .await
        .unwrap();
    assert!(!store
        .clear_blocker_request_if_matches(&w, &a, "stuck", None, &at(11))
        .await
        .unwrap());
    assert!(store
        .clear_blocker_request_if_matches(&w, &a, "stuck", Some(&at(10)), &at(11))
        .await
        .unwrap());

    let levels = vec!["low".to_owned(), tenant.to_owned()];
    assert!(store
        .set_agent_effort_levels(&w, &a, Some(&levels), &at(12))
        .await
        .unwrap());
    assert!(!store
        .set_agent_effort_levels(&w, &a, Some(&levels), &at(12))
        .await
        .unwrap());
    assert_eq!(
        store
            .get_agent_session_summary(&a)
            .await
            .unwrap()
            .effort_levels,
        Some(levels)
    );
    assert!(store
        .set_agent_effort_levels(&w, &a, None, &at(13))
        .await
        .unwrap());

    assert_eq!(
        store
            .get_agent_session_last_turn_model(&w, &a)
            .await
            .unwrap(),
        (None, None)
    );
    store
        .set_agent_session_last_turn_model(&w, &a, Some("claude:opus"), tenant)
        .await
        .unwrap();
    assert_eq!(
        store
            .get_agent_session_last_turn_model(&w, &a)
            .await
            .unwrap(),
        (Some("opus".into()), Some("claude".into()))
    );
    assert!(not_found(
        &store.get_agent_session_last_turn_model(&w, &missing).await
    ));
    assert_eq!(
        store
            .get_agent_session_last_turn_effort(&w, &a)
            .await
            .unwrap(),
        None
    );
    let effort = AgentTurnEffort {
        effort: Some("high".into()),
        default_value: tenant.into(),
        provider: "claude".into(),
        model: Some("opus".into()),
    };
    store
        .set_agent_session_last_turn_effort(&w, &a, &effort)
        .await
        .unwrap();
    assert_eq!(
        store
            .get_agent_session_last_turn_effort(&w, &a)
            .await
            .unwrap(),
        Some(effort)
    );

    assert_eq!(store.reset_all_active_flags().await.unwrap(), 1);
    assert_eq!(store.reset_all_active_flags().await.unwrap(), 0);
    if tenant == "decoy" {
        // Decoy-only state the subject's writes must leave alone.
        store.set_agent_pinned(&w, &a, false).await.unwrap();
        store
            .set_agent_notifications_muted(&w, &a, false)
            .await
            .unwrap();
        store
            .set_agent_permission_policy(&w, &a, None)
            .await
            .unwrap();
        store
            .set_agent_session_status(&w, &b, AgentStatus::Active, true, &at(14), None)
            .await
            .unwrap();
        let mut row = store.get_agent_session_without_messages(&b).await.unwrap();
        row.completion_report = Some("decoy report".into());
        store.update_agent_session(&w, &row).await.unwrap();
        // The subject's timestamp, so its blocker clear would match this row.
        store
            .set_attention_request(&w, &a, "blocker", "stuck", "2026-03-01T00:00:10Z subject")
            .await
            .unwrap();
        store
            .set_agent_session_retired_at(&w, &a, Some("decoy retired"), "decoy")
            .await
            .unwrap();
    }
    decoy_only_sessions(store, tenant).await;
}

parity_test!(agent_session_token_usage_and_acp_fold, |store, tenant| {
    seed_sessions(&store, tenant).await;
    let (w, a) = (ws(), aid("agent-a"));
    let usage = |input: u64, output: u64| TokenUsageTotals {
        input_tokens: input,
        output_tokens: output,
        ..TokenUsageTotals::default()
    };
    assert_eq!(
        store.get_agent_session_token_usage(&w, &a).await.unwrap(),
        (Some("opus".into()), None, Some("claude".into()), None)
    );
    store
        .set_agent_session_token_usage(&w, &a, &usage(10, 5))
        .await
        .unwrap();
    store
        .set_agent_session_token_usage(&w, &a, &usage(4, 5))
        .await
        .unwrap();
    assert!(not_found(
        &store
            .set_agent_session_token_usage(&w, &aid("agent-zz"), &usage(1, 1))
            .await
    ));
    assert_eq!(
        store.get_agent_session_token_usage(&w, &a).await.unwrap().3,
        Some(usage(4, 5))
    );
    let rows = store.get_workspace_agent_usage_data(&w).await.unwrap();
    let mut ids: Vec<&str> = rows.iter().map(|r| r.0.as_str()).collect();
    ids.sort_unstable();
    assert_eq!(ids, ["agent-a", "agent-b", "agent-c", "agent-d", "agent-e"]);
    let row_a = rows.iter().find(|r| r.0 == "agent-a").unwrap();
    assert_eq!(row_a.6, format!("agent-a of {tenant}"));
    assert_eq!(row_a.5.len(), 1);
    assert_eq!(row_a.5[0].model, "opus");
    assert_eq!(row_a.5[0].reported_totals, usage(4, 5));

    assert!(store
        .set_agent_session_resolved_model(&w, &a, Some("opus"), Some(&format!("Opus {tenant}")))
        .await
        .unwrap());
    assert!(!store
        .set_agent_session_resolved_model(&w, &a, Some("sonnet"), Some("x"))
        .await
        .unwrap());
    assert_eq!(
        store.get_agent_session_token_usage(&w, &a).await.unwrap().1,
        Some(format!("Opus {tenant}"))
    );
    store
        .clear_agent_session_resolved_model(&w, &a)
        .await
        .unwrap();
    assert_eq!(
        store.get_agent_session_token_usage(&w, &a).await.unwrap().1,
        None
    );

    store.set_acp_session_id(&w, &a, "acp-1").await.unwrap();
    assert_eq!(
        store
            .replace_acp_session_id(&w, &a, "acp-1", &format!("acp-{tenant}"))
            .await
            .unwrap(),
        format!("acp-{tenant}")
    );
    assert_eq!(
        store
            .replace_acp_session_id(&w, &a, "acp-1", "acp-3")
            .await
            .unwrap(),
        format!("acp-{tenant}")
    );
    assert!(not_found(
        &store
            .replace_acp_session_id(&WorkspaceId("ws-none".into()), &a, "x", "y")
            .await
    ));
    // The replace banked the snapshot into the baseline and cleared it.
    assert_eq!(
        store.get_agent_session_token_usage(&w, &a).await.unwrap().3,
        None
    );
    let rows = store.get_workspace_agent_usage_data(&w).await.unwrap();
    let row_a = rows.iter().find(|r| r.0 == "agent-a").unwrap();
    assert_eq!(
        (row_a.2.clone(), row_a.3.clone()),
        (None, Some(usage(4, 5)))
    );
    if tenant == "decoy" {
        assert!(store
            .set_agent_session_resolved_model(&w, &a, Some("opus"), Some("decoy resolved"))
            .await
            .unwrap());
    }
    decoy_only_sessions(&store, tenant).await;
});

parity_test!(interrupted_agents_round_trip, |store, tenant| {
    assert!(store.list_interrupted_agents().await.unwrap().is_empty());
    seed_sessions(&store, tenant).await;
    let (w, a, b) = (ws(), aid("agent-a"), aid("agent-b"));
    assert!(store
        .insert_interrupted_agent(&a, &w, "active", &format!("t1 {tenant}"))
        .await
        .unwrap());
    assert!(store
        .insert_interrupted_agent_with_reason(&b, &w, "processing", "t2", Some("system_suspend"))
        .await
        .unwrap());
    // A session that no longer exists is not listed.
    assert!(store
        .insert_interrupted_agent(&aid("agent-gone"), &w, "active", "t3")
        .await
        .unwrap());
    let listed = store.list_interrupted_agents().await.unwrap();
    let ids: Vec<&str> = listed.iter().map(|i| i.agent_id.as_str()).collect();
    assert_eq!(ids, ["agent-a", "agent-b"]);
    assert_eq!(listed[0].agent_name, Some(format!("agent-a of {tenant}")));
    assert!(listed[0].workspace_name.is_some());
    assert_eq!(
        (
            listed[0].prev_status.as_str(),
            listed[0].interrupted_at.clone(),
            listed[0].reason.clone()
        ),
        ("active", format!("t1 {tenant}"), None)
    );
    assert_eq!(listed[1].reason.as_deref(), Some("system_suspend"));
    assert!(store.get_interrupted_agent(&a).await.unwrap().is_some());
    assert!(store
        .set_interrupted_resolution(&a, "resumed", &format!("r {tenant}"))
        .await
        .unwrap());
    assert!(!store
        .set_interrupted_resolution(&a, "resumed", "r")
        .await
        .unwrap());
    assert!(store.get_interrupted_agent(&a).await.unwrap().is_none());
    assert!(store.reset_interrupted_resolution(&a).await.unwrap());
    assert!(!store
        .reset_interrupted_resolution(&aid("agent-zz"))
        .await
        .unwrap());
    assert_eq!(store.list_interrupted_agents().await.unwrap().len(), 2);
    if tenant == "decoy" {
        store
            .set_interrupted_resolution(&b, "abandoned", "decoy-only")
            .await
            .unwrap();
        store
            .set_interrupted_resolution(&a, "resumed", "decoy-only")
            .await
            .unwrap();
    }
    decoy_only_sessions(&store, tenant).await;
});

fn queue_row(id: &str, agent: &str, position: i64, tenant: &str) -> AgentQueueRow {
    AgentQueueRow {
        id: id.into(),
        agent_id: aid(agent),
        position,
        payload: json!({ "text": format!("{id} {tenant}") }),
        created_at: format!("2026-04-01T00:00:0{position}Z"),
        turn_id: format!("turn-{id}"),
    }
}

parity_test!(agent_queues_round_trip, |store, tenant| {
    assert!(store.load_all_agent_queues().await.unwrap().is_empty());
    seed_sessions(&store, tenant).await;
    let (a, b) = (aid("agent-a"), aid("agent-b"));
    store
        .replace_agent_queue(
            &a,
            &[
                queue_row("q2", "agent-a", 1, tenant),
                queue_row("q1", "agent-a", 0, tenant),
            ],
        )
        .await
        .unwrap();
    store
        .replace_agent_queue(&b, &[queue_row("q3", "agent-b", 0, tenant)])
        .await
        .unwrap();
    let loaded = store.load_all_agent_queues().await.unwrap();
    let got: Vec<(&str, &str, i64)> = loaded
        .iter()
        .map(|r| (r.agent_id.as_str(), r.id.as_str(), r.position))
        .collect();
    assert_eq!(
        got,
        [
            ("agent-a", "q1", 0),
            ("agent-a", "q2", 1),
            ("agent-b", "q3", 0)
        ]
    );
    assert_eq!(loaded[0].payload, json!({ "text": format!("q1 {tenant}") }));
    assert_eq!(loaded[0].turn_id, "turn-q1");
    assert!(store
        .replace_agent_queue(&a, &[queue_row("qx", "agent-b", 0, tenant)])
        .await
        .is_err());
    store
        .move_agent_queue(&a, &b, &[queue_row("q1", "agent-b", 0, tenant)])
        .await
        .unwrap();
    let got: Vec<(String, String)> = store
        .load_all_agent_queues()
        .await
        .unwrap()
        .into_iter()
        .map(|r| (r.agent_id.0, r.id))
        .collect();
    assert_eq!(got, [("agent-b".to_owned(), "q1".to_owned())]);
    assert!(store
        .move_agent_queue(&a, &b, &[queue_row("qy", "agent-a", 0, tenant)])
        .await
        .is_err());
    store.replace_agent_queue(&a, &[]).await.unwrap();
    if tenant == "decoy" {
        store
            .replace_agent_queue(&a, &[queue_row("q9", "agent-a", 0, tenant)])
            .await
            .unwrap();
    }
});

parity_test!(task_agent_links_round_trip, |store, tenant| {
    assert!(store.list_task_agent_links(&ws()).await.unwrap().is_empty());
    let link = |note: &str, key: &str, created_at: i64| TaskAgentLink {
        workspace_id: ws(),
        note_id: NoteId::from(note.to_owned()),
        task_key: key.into(),
        task_text: format!("{key} {tenant}"),
        agent_id: format!("agent-{tenant}"),
        created_at,
    };
    let first = link("n1", "k2", 2);
    assert_eq!(store.upsert_task_agent_link(&first).await.unwrap(), first);
    store
        .upsert_task_agent_link(&link("n1", "k1", 2))
        .await
        .unwrap();
    store
        .upsert_task_agent_link(&link("n0", "k9", 1))
        .await
        .unwrap();
    let mut replaced = link("n1", "k2", 3);
    replaced.task_text = format!("replaced {tenant}");
    store.upsert_task_agent_link(&replaced).await.unwrap();
    let listed = store.list_task_agent_links(&ws()).await.unwrap();
    let keys: Vec<&str> = listed.iter().map(|l| l.task_key.as_str()).collect();
    assert_eq!(keys, ["k9", "k1", "k2"]);
    assert_eq!(listed[2], replaced);
    let n1 = NoteId::from("n1".to_owned());
    assert!(store
        .delete_task_agent_link(&ws(), &n1, "k1")
        .await
        .unwrap());
    assert!(!store
        .delete_task_agent_link(&ws(), &n1, "k1")
        .await
        .unwrap());
    assert_eq!(store.list_task_agent_links(&ws()).await.unwrap().len(), 2);
    if tenant == "decoy" {
        store
            .upsert_task_agent_link(&link("n1", "k1", 5))
            .await
            .unwrap();
    }
});

parity_test!(agent_flipped_completions_round_trip, |store, tenant| {
    seed_sessions(&store, tenant).await;
    let (w, a) = (ws(), aid("agent-a"));
    let note = |n: &str| NoteId::from(n.to_owned());
    assert!(store
        .list_agent_flipped_completions(&a)
        .await
        .unwrap()
        .is_empty());
    store
        .record_agent_flipped_completion(&a, &w, &note("t2"), "2026-05-01T00:00:02Z")
        .await
        .unwrap();
    store
        .record_agent_flipped_completion(&a, &w, &note("t1"), "2026-05-01T00:00:01Z")
        .await
        .unwrap();
    store
        .record_agent_flipped_completion(&a, &w, &note("t1"), "2026-05-01T00:00:03Z")
        .await
        .unwrap();
    let list = store.list_agent_flipped_completions(&a).await.unwrap();
    assert_eq!(list, [(w.clone(), note("t2")), (w.clone(), note("t1"))]);
    // The cap evicts the oldest records.
    for i in 0..50 {
        store
            .record_agent_flipped_completion(
                &a,
                &w,
                &note(&format!("c{i:02}")),
                &format!("2026-06-01T00:00:{i:02}Z"),
            )
            .await
            .unwrap();
    }
    let list = store.list_agent_flipped_completions(&a).await.unwrap();
    assert_eq!(list.len(), 50);
    assert_eq!(list[0].1, note("c00"));
    store
        .remove_agent_flipped_completions_for_task(&w, &note("c00"))
        .await
        .unwrap();
    let taken = store.take_agent_flipped_completions(&a).await.unwrap();
    assert_eq!(taken.len(), 49);
    assert_eq!(taken[0].1, note("c01"));
    assert!(store
        .take_agent_flipped_completions(&a)
        .await
        .unwrap()
        .is_empty());
    let tag = format!("2026-07-01T00:00:00Z {tenant}");
    store
        .record_agent_flipped_completion(&a, &w, &note("kept"), &tag)
        .await
        .unwrap();
    if tenant == "decoy" {
        store
            .record_agent_flipped_completion(&a, &w, &note("decoy-only"), &tag)
            .await
            .unwrap();
        store
            .record_agent_flipped_completion(&a, &w, &note("c00"), &tag)
            .await
            .unwrap();
    }
});

parity_test!(wake_delivery_markers_round_trip, |store, tenant| {
    seed_sessions(&store, tenant).await;
    let (a, b, c) = (aid("agent-a"), aid("agent-b"), aid("agent-c"));
    assert!(!store.has_advisory_wake_delivery(&a, &b).await.unwrap());
    store
        .record_advisory_wake_delivery(&a, &b, tenant)
        .await
        .unwrap();
    store
        .record_advisory_wake_delivery(&a, &b, "again")
        .await
        .unwrap();
    store
        .record_advisory_wake_delivery(&a, &c, tenant)
        .await
        .unwrap();
    store
        .record_advisory_wake_delivery(&c, &b, tenant)
        .await
        .unwrap();
    assert!(store.has_advisory_wake_delivery(&a, &b).await.unwrap());
    assert!(!store.has_advisory_wake_delivery(&b, &a).await.unwrap());
    store.clear_advisory_wake_delivery(&a, &c).await.unwrap();
    assert!(!store.has_advisory_wake_delivery(&a, &c).await.unwrap());
    store
        .clear_advisory_wake_deliveries_for_child(&b)
        .await
        .unwrap();
    assert!(!store.has_advisory_wake_delivery(&a, &b).await.unwrap());
    assert!(!store.has_advisory_wake_delivery(&c, &b).await.unwrap());
    store
        .record_advisory_wake_delivery(&a, &b, tenant)
        .await
        .unwrap();
    store
        .record_advisory_wake_delivery(&a, &c, tenant)
        .await
        .unwrap();
    store
        .clear_advisory_wake_deliveries_for_children(&[b.clone(), c.clone()])
        .await
        .unwrap();
    store
        .clear_advisory_wake_deliveries_for_children(&[])
        .await
        .unwrap();
    assert!(!store.has_advisory_wake_delivery(&a, &b).await.unwrap());
    assert!(!store.has_advisory_wake_delivery(&a, &c).await.unwrap());
    if tenant == "decoy" {
        for (parent, child) in [(&a, &b), (&a, &c), (&c, &b)] {
            store
                .record_advisory_wake_delivery(parent, child, "decoy")
                .await
                .unwrap();
        }
    }

    assert_eq!(
        store.get_completion_wake_delivery(&a, &b).await.unwrap(),
        None
    );
    store
        .record_completion_wake_delivery(&a, &b, "id-1", "t")
        .await
        .unwrap();
    store
        .record_completion_wake_delivery(&a, &b, &format!("id-{tenant}"), "t")
        .await
        .unwrap();
    assert_eq!(
        store.get_completion_wake_delivery(&a, &b).await.unwrap(),
        Some(format!("id-{tenant}"))
    );
    assert_eq!(
        store.get_completion_wake_delivery(&b, &a).await.unwrap(),
        None
    );
});

parity_test!(review_context_enrollment_without_rows, |store| {
    seed_sessions(&store, "any").await;
    let (w, a) = (ws(), aid("agent-a"));
    assert!(store
        .get_review_context_enrollment(&w, &a, " ")
        .await
        .is_err());
    assert!(store
        .get_review_context_enrollment(&w, &a, "acp-1")
        .await
        .unwrap()
        .is_none());
});

/// Enrollment rows are written by the repository lifecycle, which the
/// Postgres store does not run; the Postgres tenants seed them directly.
#[tokio::test]
async fn review_context_enrollment_reads_the_tenants_rows() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let key = "6f1c2a0e-8a6b-4c1d-9e2f-3a4b5c6d7e8f";
    let (w, a) = (ws(), aid("agent-a"));
    let a_store = open(&db, "tenant-a").await;
    let b_store = open(&db, "tenant-b").await;
    for (tenant, store) in [("tenant-a", &a_store), ("tenant-b", &b_store)] {
        seed_sessions(store, tenant).await;
        store.set_acp_session_id(&w, &a, "acp-1").await.unwrap();
    }
    sqlx::query(
        "INSERT INTO review_context_enrollment \
         (tenant_id, agent_id, workspace_id, acp_session_id, owner_key, version) \
         VALUES ('tenant-a', 'agent-a', $1, 'acp-1', $2, 1)",
    )
    .bind(WS)
    .bind(key)
    .execute(&db.admin)
    .await
    .unwrap();
    let found = a_store
        .get_review_context_enrollment(&w, &a, "acp-1")
        .await
        .unwrap()
        .expect("tenant A's enrollment");
    assert_eq!(found.owner_key(), key);
    assert!(b_store
        .get_review_context_enrollment(&w, &a, "acp-1")
        .await
        .unwrap()
        .is_none());
    sqlx::query(
        "INSERT INTO review_context_private_family (tenant_id, acp_session_id, version) \
         VALUES ('tenant-a', 'acp-2', 1)",
    )
    .execute(&db.admin)
    .await
    .unwrap();
    assert!(a_store
        .get_review_context_enrollment(&w, &a, "acp-2")
        .await
        .is_err());
    assert!(b_store
        .get_review_context_enrollment(&w, &a, "acp-2")
        .await
        .unwrap()
        .is_none());
}

// ---- methods that read agent messages ------------------------------------------

/// Append a message: through the store on `SQLite`, and with SQL on Postgres
/// (message writes are another domain's port), with the same provenance
/// columns, session pointers and usage-cell counts the store writes.
async fn append(
    store: &Store,
    admin: Option<&sqlx::PgPool>,
    tenant: &str,
    agent: &str,
    role: &str,
    content: Value,
    metadata: Option<Value>,
) -> AgentMessage {
    let created_at = "2026-08-01T00:00:00Z";
    let Some(admin) = admin else {
        return store
            .append_agent_message_with_metadata(
                &aid(agent),
                role,
                &content,
                metadata.as_ref(),
                created_at,
            )
            .await
            .unwrap();
    };
    let origin = match role {
        "assistant" => "agent",
        "user" => "human",
        _ => "excluded",
    };
    let (id, seq): (String, i64) = sqlx::query_as(
        "INSERT INTO agent_message (tenant_id, id, agent_id, seq, role, content, created_at, \
         metadata, usage_model, usage_origin) \
         SELECT $1, $2 || '-' || n, $2, n, $3, $4, $5, $6, \
             (SELECT COALESCE(NULLIF(resolved_model, ''), NULLIF(model, ''), 'unknown') \
              FROM agent_session WHERE tenant_id = $1 AND id = $2), $7 \
         FROM (SELECT COALESCE(MAX(seq) + 1, 0) AS n FROM agent_message \
               WHERE tenant_id = $1 AND agent_id = $2) s \
         RETURNING id, seq",
    )
    .bind(tenant)
    .bind(agent)
    .bind(role)
    .bind(content.to_string())
    .bind(created_at)
    .bind(metadata.as_ref().map(Value::to_string))
    .bind(origin)
    .fetch_one(admin)
    .await
    .unwrap();
    if origin != "excluded" {
        sqlx::query(
            "UPDATE agent_session SET last_message_role = $3, last_message_id = $4 \
             WHERE tenant_id = $1 AND id = $2",
        )
        .bind(tenant)
        .bind(agent)
        .bind(role)
        .bind(&id)
        .execute(admin)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO agent_usage_cell (tenant_id, agent_id, model, human_messages, agent_messages) \
             SELECT $1, $2, usage_model, CAST(usage_origin = 'human' AS INT), \
                 CAST(usage_origin = 'agent' AS INT) \
             FROM agent_message WHERE tenant_id = $1 AND id = $3 \
             ON CONFLICT (tenant_id, agent_id, model) DO UPDATE SET \
             human_messages = agent_usage_cell.human_messages + excluded.human_messages, \
             agent_messages = agent_usage_cell.agent_messages + excluded.agent_messages",
        )
        .bind(tenant)
        .bind(agent)
        .bind(&id)
        .execute(admin)
        .await
        .unwrap();
    }
    AgentMessage {
        id,
        agent_id: aid(agent),
        seq,
        role: role.into(),
        content,
        metadata,
        app_message_id: None,
        author: None,
        created_at: created_at.into(),
    }
}

/// [`parity_test!`] for bodies that also seed messages: binds the admin
/// pool (`None` on `SQLite`) for [`append`].
macro_rules! message_parity_test {
    ($name:ident, |$store:ident, $tenant:ident, $admin:ident| $body:block) => {
        #[tokio::test]
        async fn $name() {
            {
                let (store, _dir) = sqlite_store().await;
                let $store = store;
                let $tenant: &str = "sqlite";
                let $admin: Option<&sqlx::PgPool> = None;
                $body
            }
            if let Some(db) = test_db(false).await {
                {
                    let $store = open(&db, "decoy").await;
                    let $tenant: &str = "decoy";
                    let $admin = Some(&db.admin);
                    $body
                }
                let before = tenant_rows(&db.admin, "decoy").await;
                {
                    let $store = open(&db, "subject").await;
                    let $tenant: &str = "subject";
                    let $admin = Some(&db.admin);
                    $body
                }
                assert!(
                    before == tenant_rows(&db.admin, "decoy").await,
                    "the subject tenant's run changed the decoy tenant's rows"
                );
            }
        }
    };
}

message_parity_test!(
    identity_notices_and_question_markers,
    |store, tenant, admin| {
        seed_sessions(&store, tenant).await;
        let (w, a) = (ws(), aid("agent-a"));
        assert!(store
            .recent_agent_identity_notices(&a, 5)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(store.latest_agent_identity_notice(&a).await.unwrap(), None);
        let notice = |kind: &str| json!({ "type": kind, "tenant": tenant });
        append(
            &store,
            admin,
            tenant,
            "agent-a",
            "system",
            json!([]),
            Some(notice("model_changed")),
        )
        .await;
        append(
            &store,
            admin,
            tenant,
            "agent-a",
            "system",
            json!([]),
            Some(notice("other")),
        )
        .await;
        append(
            &store,
            admin,
            tenant,
            "agent-a",
            "system",
            json!([]),
            Some(notice("provider_failover")),
        )
        .await;
        assert_eq!(
            store.recent_agent_identity_notices(&a, 5).await.unwrap(),
            [notice("provider_failover"), notice("model_changed")]
        );
        assert_eq!(
            store.recent_agent_identity_notices(&a, 1).await.unwrap(),
            [notice("provider_failover")]
        );
        assert_eq!(
            store.latest_agent_identity_notice(&a).await.unwrap(),
            Some(notice("provider_failover"))
        );

        let key = intent_core::PENDING_QUESTIONS_MESSAGE_ID_KEY;
        let question = append(
            &store,
            admin,
            tenant,
            "agent-a",
            "assistant",
            json!([]),
            None,
        )
        .await;
        let at = format!("2026-08-02T00:00:00Z {tenant}");
        assert!(store
            .set_pending_questions_marker_if_unanswered(&w, &a, Some(None), &question, &at)
            .await
            .unwrap());
        assert_eq!(
            metadata(&store, "agent-a").await.unwrap()[key],
            json!(question.id)
        );
        assert!(!store
            .set_pending_questions_marker_if_unanswered(&w, &a, Some(None), &question, &at)
            .await
            .unwrap());
        append(
            &store,
            admin,
            tenant,
            "agent-a",
            "user",
            json!([]),
            Some(json!({ "type": "question_answers", "answeredQuestionsMessageId": question.id })),
        )
        .await;
        assert!(!store
            .set_pending_questions_marker_if_unanswered(&w, &a, None, &question, &at)
            .await
            .unwrap());
        assert!(not_found(
            &store
                .set_pending_questions_marker_if_unanswered(
                    &w,
                    &aid("agent-zz"),
                    None,
                    &question,
                    &at
                )
                .await
        ));
        decoy_only_sessions(&store, tenant).await;
    }
);

message_parity_test!(
    resolved_model_moves_the_prompt_and_usage_falls_back_to_messages,
    |store, tenant, admin| {
        seed_sessions(&store, tenant).await;
        let (w, b) = (ws(), aid("agent-b"));
        append(
            &store,
            admin,
            tenant,
            "agent-b",
            "user",
            json!([{ "type": "text", "text": "hi" }]),
            None,
        )
        .await;
        let resolved = format!("Opus {tenant}");
        assert!(store
            .set_agent_session_resolved_model(&w, &b, Some("opus"), Some(&resolved))
            .await
            .unwrap());
        let rows = store.get_workspace_agent_usage_data(&w).await.unwrap();
        let cells: Vec<(String, u64)> = rows
            .iter()
            .find(|r| r.0 == "agent-b")
            .unwrap()
            .5
            .iter()
            .map(|c| (c.model.clone(), c.human_messages))
            .collect();
        assert_eq!(cells, [(resolved.clone(), 1)]);
        if let (Some(admin), "decoy") = (admin, tenant) {
            // An old-model cell the subject's decrement and prune must not reach.
            sqlx::query(
                "INSERT INTO agent_usage_cell (tenant_id, agent_id, model, human_messages) \
                 VALUES ('decoy', 'agent-b', 'opus', 1)",
            )
            .execute(admin)
            .await
            .unwrap();
        }

        // No reported usage: the tally reads per-message usage instead.
        let usage = json!({ "inputTokens": 7, "tenant": tenant });
        append(
            &store,
            admin,
            tenant,
            "agent-c",
            "assistant",
            json!({ "usage": usage }),
            None,
        )
        .await;
        append(
            &store,
            admin,
            tenant,
            "agent-c",
            "assistant",
            json!({ "_meta": { "usage": usage } }),
            None,
        )
        .await;
        append(
            &store,
            admin,
            tenant,
            "agent-c",
            "assistant",
            json!({ "usage": 3 }),
            None,
        )
        .await;
        let rows = store.get_workspace_agent_usage_data(&w).await.unwrap();
        let row_c = rows.iter().find(|r| r.0 == "agent-c").unwrap();
        assert_eq!(
            row_c.4,
            [json!({ "usage": usage }), json!({ "usage": usage })]
        );
        assert_eq!(row_c.5.len(), 1);
        assert_eq!(row_c.5[0].model, "unknown");
        assert_eq!(row_c.5[0].agent_messages, 3);
        assert_eq!(row_c.5[0].message_usage.len(), 2);
    }
);

/// A session whose stored `metadata` is not JSON: the writers that read it
/// with `json_extract` fail on both backends, as `SQLite` raises.
async fn malformed_metadata_writes_fail(
    store: &Store,
    corrupt: impl std::future::Future<Output = ()>,
) {
    seed_sessions(store, "malformed").await;
    let (w, a) = (ws(), aid("agent-a"));
    let row = store.get_agent_session_without_messages(&a).await.unwrap();
    corrupt.await;
    assert!(store.update_agent_session(&w, &row).await.is_err());
    assert!(store
        .set_agent_session_metadata_key(&w, &a, "k", "v", Some(None), None)
        .await
        .is_err());
    let question = AgentMessage {
        id: "q".into(),
        agent_id: a.clone(),
        seq: 0,
        role: "assistant".into(),
        content: json!([]),
        metadata: None,
        app_message_id: None,
        author: None,
        created_at: "t".into(),
    };
    assert!(store
        .set_pending_questions_marker_if_unanswered(&w, &a, Some(None), &question, "t")
        .await
        .is_err());
}

#[tokio::test]
async fn malformed_session_metadata_fails_guarded_writes_on_both_backends() {
    let (sqlite, dir) = sqlite_store().await;
    let path = dir.path().join("intent.db");
    malformed_metadata_writes_fail(&sqlite, async {
        use sqlx::Connection as _;
        let mut conn = sqlx::SqliteConnection::connect(&format!("sqlite://{}", path.display()))
            .await
            .unwrap();
        sqlx::query("UPDATE agent_session SET metadata = '{bad' WHERE id = 'agent-a'")
            .execute(&mut conn)
            .await
            .unwrap();
    })
    .await;
    let Some(db) = test_db(false).await else {
        return;
    };
    let store = open(&db, "subject").await;
    malformed_metadata_writes_fail(&store, async {
        sqlx::query(
            "UPDATE agent_session SET metadata = '{bad' \
             WHERE tenant_id = 'subject' AND id = 'agent-a'",
        )
        .execute(&db.admin)
        .await
        .unwrap();
    })
    .await;
}

/// A tenant with a reported snapshot but no usage cells seeds its legacy
/// cell, even when another tenant has cells for the same agent id.
#[tokio::test]
async fn token_usage_seeds_legacy_cells_per_tenant() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let (w, a) = (ws(), aid("agent-a"));
    let usage = |input: u64| TokenUsageTotals {
        input_tokens: input,
        ..TokenUsageTotals::default()
    };
    let a_store = open(&db, "tenant-a").await;
    let b_store = open(&db, "tenant-b").await;
    for (tenant, store) in [("tenant-a", &a_store), ("tenant-b", &b_store)] {
        store
            .insert_agent_session(&session("agent-a", tenant, "2026-01-01T00:00:01Z"))
            .await
            .unwrap();
    }
    b_store
        .set_agent_session_token_usage(&w, &a, &usage(3))
        .await
        .unwrap();
    sqlx::query(
        "UPDATE agent_session SET token_usage = $1 WHERE tenant_id = 'tenant-a' AND id = 'agent-a'",
    )
    .bind(serde_json::to_string(&usage(10)).unwrap())
    .execute(&db.admin)
    .await
    .unwrap();
    a_store
        .set_agent_session_token_usage(&w, &a, &usage(15))
        .await
        .unwrap();
    let rows = a_store.get_workspace_agent_usage_data(&w).await.unwrap();
    assert_eq!(rows[0].5[0].reported_totals, usage(15));
}
