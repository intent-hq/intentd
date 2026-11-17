//! Hook store methods on both backends: inserts, the full-row and `hook.list`
//! reads, active counts and metadata, and the per-field run updates. Tie
//! hooks share a `created_at` and have ids whose order depends on collation,
//! so the tie-breaks and `COLLATE "C"` decide results.
//!
//! Decoy rule: reads run while the decoy owns the same hook ids (plus a
//! decoy-only workspace, agent and hook the subject probes as absent), and
//! names, code, logs and state carry the tenant, so a leaked row never looks
//! right. The decoy's hooks are reset to their inserted state after its own
//! writes, so a write without its tenant filter changes a decoy row.

use std::collections::HashSet;

use intent_core::{
    AgentId, AgentSession, AgentStatus, Error, Hook, HookId, HookListRow, HookState, HookSummary,
    Workspace, WorkspaceId,
};
use intent_store::Store;

use crate::harness::parity_test;

const CHIEF: &str = "__chief__";
const T0: &str = "2026-01-01T00:00:00.000Z";
const T1: &str = "2026-01-02T00:00:00.000Z";
const T2: &str = "2026-01-03T00:00:00.000Z";
const T3: &str = "2026-01-04T00:00:00.000Z";
const T4: &str = "2026-01-05T00:00:00.000Z";
const T5: &str = "2026-01-06T00:00:00.000Z";
/// Shared `created_at` of the tie hooks (see [`seed_ties`]).
const TIE: &str = "2026-01-07T00:00:00.000Z";
const NEXT: &str = "2026-02-01T00:00:00.000Z";
const EXPIRES: &str = "2026-03-01T00:00:00.000Z";

fn wid(id: &str) -> WorkspaceId {
    WorkspaceId(id.into())
}

fn aid(id: &str) -> AgentId {
    AgentId(id.into())
}

fn hid(id: &str) -> HookId {
    HookId(id.into())
}

fn ids(hooks: &[Hook]) -> Vec<&str> {
    hooks.iter().map(|h| h.hook_id.0.as_str()).collect()
}

fn not_found<T: std::fmt::Debug>(r: &intent_core::Result<T>) -> bool {
    matches!(r, Err(Error::NotFound(_)))
}

fn hook(id: &str, ws: &str, agent: &str, created_at: &str, state: HookState, tenant: &str) -> Hook {
    Hook {
        hook_id: hid(id),
        workspace_id: wid(ws),
        agent_id: aid(agent),
        name: format!("{tenant} {id}"),
        code: format!("return '{tenant}'"),
        delay_ms: 1000,
        cron: None,
        run_at: None,
        state,
        created_at: created_at.into(),
        last_run_at: None,
        next_run_at: Some(NEXT.into()),
        run_count: 0,
        last_error: None,
        last_logs: Some(format!("{tenant} logs")),
        last_state: Some(format!("{{\"tenant\":\"{tenant}\"}}")),
        expires_at: Some(EXPIRES.into()),
        perpetual: false,
        dispatch_count: 0,
    }
}

fn summary(h: &Hook) -> HookSummary {
    HookSummary {
        hook_id: h.hook_id.clone(),
        workspace_id: h.workspace_id.clone(),
        agent_id: h.agent_id.clone(),
        name: h.name.clone(),
        delay_ms: h.delay_ms,
        cron: h.cron.clone(),
        run_at: h.run_at.clone(),
        state: h.state,
        created_at: h.created_at.clone(),
        last_run_at: h.last_run_at.clone(),
        next_run_at: h.next_run_at.clone(),
        run_count: h.run_count,
        last_error: h.last_error.clone(),
        expires_at: h.expires_at.clone(),
        perpetual: h.perpetual,
        dispatch_count: h.dispatch_count,
    }
}

fn workspace(id: &str) -> Workspace {
    Workspace {
        id: wid(id),
        title: format!("title {id}"),
        branch: "main".into(),
        created_at: T0.into(),
        updated_at: T0.into(),
        ..Default::default()
    }
}

fn session(id: &str, ws: &str, tenant: &str) -> AgentSession {
    AgentSession {
        id: aid(id),
        workspace_id: wid(ws),
        name: format!("{id} of {tenant}"),
        status: AgentStatus::Idle,
        created_at: T0.into(),
        updated_at: T0.into(),
        ..AgentSession::default()
    }
}

/// `h1` active (a1), `h2` retired cron hook (a1), `h3` running one-shot in
/// `ws-b` (a2), `h4` cancelled (a1), `h5` active (a2) in the chief workspace.
fn seed_hooks(tenant: &str) -> Vec<Hook> {
    let h1 = hook("h1", CHIEF, "a1", T1, HookState::Scheduled, tenant);
    let mut h2 = hook("h2", CHIEF, "a1", T2, HookState::Dispatched, tenant);
    h2.cron = Some("*/5 * * * *".into());
    h2.delay_ms = 0;
    h2.last_run_at = Some(T2.into());
    h2.run_count = 4;
    h2.dispatch_count = 1;
    h2.last_error = Some(format!("{tenant} failed"));
    let mut h3 = hook("h3", "ws-b", "a2", T3, HookState::Running, tenant);
    h3.run_at = Some(NEXT.into());
    h3.delay_ms = 0;
    h3.perpetual = true;
    h3.next_run_at = None;
    let h4 = hook("h4", CHIEF, "a1", T4, HookState::Cancelled, tenant);
    let h5 = hook("h5", CHIEF, "a2", T5, HookState::Scheduled, tenant);
    vec![h1, h2, h3, h4, h5]
}

/// Parents and [`seed_hooks`], with `prefix` on every hook id.
async fn seed(store: &Store, tenant: &str, prefix: &str) -> Vec<Hook> {
    store.insert_workspace(&workspace("ws-b")).await.unwrap();
    store
        .insert_agent_session(&session("a1", CHIEF, tenant))
        .await
        .unwrap();
    store
        .insert_agent_session(&session("a2", "ws-b", tenant))
        .await
        .unwrap();
    let mut hooks = seed_hooks(tenant);
    for h in &mut hooks {
        h.hook_id = hid(&format!("{prefix}{}", h.hook_id.0));
        store.insert_hook(h).await.unwrap();
    }
    hooks
}

/// Three hooks in `ws-t` (agent `a3`) sharing [`TIE`], inserted as `hc`
/// (active), `hB` (cancelled), `ha` (active). Insertion order, byte order
/// (`hB` < `ha` < `hc`) and a locale collation (`ha` < `hB` < `hc`) all
/// differ, so the `row_seq` tie-break and `COLLATE "C"` each decide a result.
async fn seed_ties(store: &Store, tenant: &str) -> Vec<Hook> {
    store.insert_workspace(&workspace("ws-t")).await.unwrap();
    store
        .insert_agent_session(&session("a3", "ws-t", tenant))
        .await
        .unwrap();
    let hooks = vec![
        hook("hc", "ws-t", "a3", TIE, HookState::Scheduled, tenant),
        hook("hB", "ws-t", "a3", TIE, HookState::Cancelled, tenant),
        hook("ha", "ws-t", "a3", TIE, HookState::Scheduled, tenant),
    ];
    for h in &hooks {
        store.insert_hook(h).await.unwrap();
    }
    hooks
}

/// Decoy-only rows: a workspace, agent and active hook the subject never
/// creates and probes as absent.
async fn seed_decoy_only(store: &Store, tenant: &str) {
    store.insert_workspace(&workspace("ws-c")).await.unwrap();
    store
        .insert_agent_session(&session("a-decoy", "ws-c", tenant))
        .await
        .unwrap();
    store
        .insert_hook(&hook(
            "h-decoy",
            "ws-c",
            "a-decoy",
            T0,
            HookState::Scheduled,
            tenant,
        ))
        .await
        .unwrap();
}

fn metadata(rows: Vec<intent_store::ActiveHookMetadata>) -> Vec<(String, String, String)> {
    rows.into_iter()
        .map(|m| {
            assert_eq!(m.next_run_at.as_deref(), Some(NEXT));
            assert_eq!(m.expires_at.as_deref(), Some(EXPIRES));
            (m.agent_id.0, m.hook_id.0, m.name)
        })
        .collect()
}

parity_test!(hook_reads_round_trip, |store, tenant| {
    let (chief, wb, wc) = (wid(CHIEF), wid("ws-b"), wid("ws-c"));
    let (a1, a2, ad) = (aid("a1"), aid("a2"), aid("a-decoy"));

    assert!(not_found(&store.get_hook(&hid("h1")).await));
    assert!(store
        .list_hooks_by_workspace(&chief)
        .await
        .unwrap()
        .is_empty());
    assert!(store.list_hooks_by_agent(&a1).await.unwrap().is_empty());
    assert!(store
        .active_hook_metadata_by_agent(&a1)
        .await
        .unwrap()
        .is_empty());
    assert!(store
        .active_hook_metadata_by_workspace(&chief)
        .await
        .unwrap()
        .is_empty());
    assert!(store
        .list_hook_rows(&chief, None, true)
        .await
        .unwrap()
        .is_empty());
    assert!(store
        .list_hook_rows_page(&chief, true, None, 10)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(store.count_active_hooks_by_agent(&a1).await.unwrap(), 0);
    assert_eq!(
        store.count_active_hooks_by_workspace(&chief).await.unwrap(),
        0
    );
    assert!(store
        .workspaces_with_active_hooks(&[chief.clone(), wb.clone()])
        .await
        .unwrap()
        .is_empty());
    assert!(store.load_active_hooks().await.unwrap().is_empty());

    let hooks = seed(&store, tenant, "").await;
    let [h1, h2, h3, h4, h5] = <[Hook; 5]>::try_from(hooks).unwrap();
    assert!(store.insert_hook(&h1).await.is_err());
    let ties = seed_ties(&store, tenant).await;
    let [hc, hb, ha] = <[Hook; 3]>::try_from(ties).unwrap();
    let (wt, a3) = (wid("ws-t"), aid("a3"));

    assert_eq!(store.get_hook(&hid("h2")).await.unwrap(), h2);
    assert_eq!(store.get_hook(&hid("h3")).await.unwrap(), h3);
    assert_eq!(
        store.list_hooks_by_workspace(&chief).await.unwrap(),
        [h1.clone(), h2.clone(), h4.clone(), h5.clone()]
    );
    assert_eq!(
        store.list_hooks_by_workspace(&wb).await.unwrap(),
        std::slice::from_ref(&h3)
    );
    assert_eq!(
        store.list_hooks_by_agent(&a1).await.unwrap(),
        [h1.clone(), h2.clone(), h4.clone()]
    );
    assert_eq!(
        ids(&store.list_hooks_by_agent(&a2).await.unwrap()),
        ["h3", "h5"]
    );

    let name = |id: &str| format!("{tenant} {id}");
    assert_eq!(
        metadata(store.active_hook_metadata_by_agent(&a1).await.unwrap()),
        [("a1".into(), "h1".into(), name("h1"))]
    );
    assert_eq!(
        metadata(
            store
                .active_hook_metadata_by_workspace(&chief)
                .await
                .unwrap()
        ),
        [
            ("a1".into(), "h1".into(), name("h1")),
            ("a2".into(), "h5".into(), name("h5"))
        ]
    );
    let by_a2 = store.active_hook_metadata_by_agent(&a2).await.unwrap();
    assert_eq!(by_a2.len(), 2);
    assert_eq!(by_a2[0].hook_id.0, "h3");
    assert_eq!(by_a2[0].next_run_at, None);

    assert_eq!(
        store.list_hook_rows(&chief, None, false).await.unwrap(),
        [
            HookListRow::Active(h1.clone()),
            HookListRow::Active(h5.clone())
        ]
    );
    assert_eq!(
        store.list_hook_rows(&chief, None, true).await.unwrap(),
        [
            HookListRow::Active(h1.clone()),
            HookListRow::Retired(summary(&h2)),
            HookListRow::Retired(summary(&h4)),
            HookListRow::Active(h5.clone()),
        ]
    );
    assert_eq!(
        store.list_hook_rows(&chief, Some(&a2), true).await.unwrap(),
        [HookListRow::Active(h5.clone())]
    );
    assert!(store
        .list_hook_rows(&wb, Some(&a1), true)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        store
            .list_hook_rows(&chief, Some(&a1), false)
            .await
            .unwrap(),
        [HookListRow::Active(h1.clone())]
    );

    // Ties on `created_at`: row-ordered reads keep insertion order.
    assert_eq!(
        store.list_hooks_by_workspace(&wt).await.unwrap(),
        [hc.clone(), hb.clone(), ha.clone()]
    );
    assert_eq!(
        store.list_hooks_by_agent(&a3).await.unwrap(),
        [hc.clone(), hb.clone(), ha.clone()]
    );
    assert_eq!(
        metadata(store.active_hook_metadata_by_workspace(&wt).await.unwrap()),
        [
            ("a3".into(), "hc".into(), name("hc")),
            ("a3".into(), "ha".into(), name("ha"))
        ]
    );
    assert_eq!(
        metadata(store.active_hook_metadata_by_agent(&a3).await.unwrap()),
        [
            ("a3".into(), "hc".into(), name("hc")),
            ("a3".into(), "ha".into(), name("ha"))
        ]
    );
    assert_eq!(
        store.list_hook_rows(&wt, None, true).await.unwrap(),
        [
            HookListRow::Active(hc.clone()),
            HookListRow::Retired(summary(&hb)),
            HookListRow::Active(ha.clone()),
        ]
    );
    assert_eq!(
        store.list_hook_rows(&wt, Some(&a3), false).await.unwrap(),
        [
            HookListRow::Active(hc.clone()),
            HookListRow::Active(ha.clone())
        ]
    );
    // The page breaks ties on `hook_id` in byte order: `hB`, `ha`, `hc`.
    assert_eq!(
        store.list_hook_rows_page(&wt, true, None, 2).await.unwrap(),
        [
            (TIE.to_owned(), HookListRow::Retired(summary(&hb))),
            (TIE.to_owned(), HookListRow::Active(ha.clone())),
        ]
    );
    assert_eq!(
        store
            .list_hook_rows_page(&wt, true, Some((TIE, "hB")), 10)
            .await
            .unwrap(),
        [
            (TIE.to_owned(), HookListRow::Active(ha.clone())),
            (TIE.to_owned(), HookListRow::Active(hc.clone())),
        ]
    );
    assert_eq!(
        store
            .list_hook_rows_page(&wt, false, Some((TIE, "hB")), 1)
            .await
            .unwrap(),
        [(TIE.to_owned(), HookListRow::Active(ha.clone()))]
    );
    assert_eq!(
        store
            .list_hook_rows_page(&wt, false, Some((TIE, "ha")), 10)
            .await
            .unwrap(),
        [(TIE.to_owned(), HookListRow::Active(hc.clone()))]
    );

    let page = store
        .list_hook_rows_page(&chief, true, None, 2)
        .await
        .unwrap();
    assert_eq!(
        page,
        [
            (T1.to_owned(), HookListRow::Active(h1.clone())),
            (T2.to_owned(), HookListRow::Retired(summary(&h2))),
        ]
    );
    assert_eq!(
        store
            .list_hook_rows_page(&chief, true, Some((T2, "h2")), 10)
            .await
            .unwrap(),
        [
            (T4.to_owned(), HookListRow::Retired(summary(&h4))),
            (T5.to_owned(), HookListRow::Active(h5.clone())),
        ]
    );
    assert_eq!(
        store
            .list_hook_rows_page(&chief, false, Some((T1, "h1")), 10)
            .await
            .unwrap(),
        [(T5.to_owned(), HookListRow::Active(h5.clone()))]
    );
    assert_eq!(
        store
            .list_hook_rows_page(&chief, true, Some((T1, "h0")), 1)
            .await
            .unwrap(),
        [(T1.to_owned(), HookListRow::Active(h1.clone()))]
    );

    assert_eq!(store.count_active_hooks_by_agent(&a1).await.unwrap(), 1);
    assert_eq!(store.count_active_hooks_by_agent(&a2).await.unwrap(), 2);
    assert_eq!(
        store.count_active_hooks_by_workspace(&chief).await.unwrap(),
        2
    );
    assert_eq!(store.count_active_hooks_by_workspace(&wb).await.unwrap(), 1);
    assert_eq!(
        store
            .workspaces_with_active_hooks(&[chief.clone(), wb.clone(), wc.clone()])
            .await
            .unwrap(),
        HashSet::from([chief.clone(), wb.clone()])
    );
    assert!(store
        .workspaces_with_active_hooks(&[])
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        store.load_active_hooks().await.unwrap(),
        [h1, h3, h5, hc, ha]
    );

    assert!(not_found(&store.get_hook(&hid("h-decoy")).await));
    assert!(store.list_hooks_by_workspace(&wc).await.unwrap().is_empty());
    assert!(store.list_hooks_by_agent(&ad).await.unwrap().is_empty());
    assert!(store
        .active_hook_metadata_by_agent(&ad)
        .await
        .unwrap()
        .is_empty());
    assert!(store
        .active_hook_metadata_by_workspace(&wc)
        .await
        .unwrap()
        .is_empty());
    assert!(store
        .list_hook_rows(&wc, None, true)
        .await
        .unwrap()
        .is_empty());
    assert!(store
        .list_hook_rows_page(&wc, true, None, 10)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(store.count_active_hooks_by_agent(&ad).await.unwrap(), 0);
    assert_eq!(store.count_active_hooks_by_workspace(&wc).await.unwrap(), 0);

    if tenant == "decoy" {
        seed_decoy_only(&store, tenant).await;
    }
});

parity_test!(hook_writes_update_one_row, |store, tenant| {
    // The decoy writes its own `d-` hooks, then leaves the subject's ids in
    // their pre-write state.
    let p = if tenant == "decoy" { "d-" } else { "" };
    for missing in ["h1", "h-decoy"] {
        let id = hid(missing);
        assert!(not_found(
            &store.update_hook_state(&id, HookState::Running).await
        ));
        assert!(not_found(&store.expire_hook(&id).await));
        assert!(not_found(&store.update_hook_run(&id, T3, Some(NEXT)).await));
        assert!(not_found(&store.record_hook_dispatch(&id, 1).await));
        assert!(not_found(&store.update_hook_next_run(&id, None).await));
        assert!(not_found(
            &store.update_hook_last_error(&id, Some("x")).await
        ));
        assert!(not_found(&store.update_hook_last_logs(&id, None).await));
        assert!(not_found(&store.update_hook_last_state(&id, None).await));
    }

    let hooks = seed(&store, tenant, p).await;
    let id = |n: &str| hid(&format!("{p}{n}"));
    let (h1, h2, h3) = (id("h1"), id("h2"), id("h3"));

    store
        .update_hook_state(&h1, HookState::Running)
        .await
        .unwrap();
    let mut want = hooks[0].clone();
    want.state = HookState::Running;
    assert_eq!(store.get_hook(&h1).await.unwrap(), want);

    let run_at = format!("2026-01-10T00:00:00.000Z {tenant}");
    store.update_hook_run(&h1, &run_at, Some(T5)).await.unwrap();
    store.update_hook_run(&h1, &run_at, Some(T5)).await.unwrap();
    want.run_count = 1;
    want.last_run_at = Some(run_at);
    want.next_run_at = Some(T5.into());
    assert_eq!(store.get_hook(&h1).await.unwrap(), want);
    store.update_hook_run(&h1, T4, None).await.unwrap();
    want.run_count = 2;
    want.last_run_at = Some(T4.into());
    want.next_run_at = None;
    assert_eq!(store.get_hook(&h1).await.unwrap(), want);

    store.record_hook_dispatch(&h1, 3).await.unwrap();
    store.record_hook_dispatch(&h1, 2).await.unwrap();
    want.dispatch_count = 3;
    store.update_hook_next_run(&h1, Some(NEXT)).await.unwrap();
    want.next_run_at = Some(NEXT.into());
    let error = format!("{tenant} boom");
    store
        .update_hook_last_error(&h1, Some(&error))
        .await
        .unwrap();
    want.last_error = Some(error);
    let logs = format!("{tenant} new logs");
    store.update_hook_last_logs(&h1, Some(&logs)).await.unwrap();
    want.last_logs = Some(logs);
    store.update_hook_last_state(&h1, None).await.unwrap();
    want.last_state = None;
    assert_eq!(store.get_hook(&h1).await.unwrap(), want);

    store.update_hook_last_error(&h2, None).await.unwrap();
    assert_eq!(hooks[1].next_run_at.as_deref(), Some(NEXT));
    store.expire_hook(&h2).await.unwrap();
    store.update_hook_last_logs(&h3, None).await.unwrap();
    store.update_hook_next_run(&h3, None).await.unwrap();
    store.expire_hook(&h3).await.unwrap();
    let mut want2 = hooks[1].clone();
    want2.last_error = None;
    want2.state = HookState::Expired;
    want2.next_run_at = None;
    let mut want3 = hooks[2].clone();
    want3.last_logs = None;
    want3.state = HookState::Expired;
    assert_eq!(store.get_hook(&h2).await.unwrap(), want2);
    assert_eq!(store.get_hook(&h3).await.unwrap(), want3);

    store
        .update_hook_state(&id("h5"), HookState::Evicted)
        .await
        .unwrap();
    assert_eq!(
        ids(&store.load_active_hooks().await.unwrap()),
        [h1.0.as_str()],
        "only h1 stays active"
    );
    assert_eq!(store.get_hook(&id("h4")).await.unwrap(), hooks[3]);

    if tenant == "decoy" {
        for h in seed_hooks(tenant) {
            store.insert_hook(&h).await.unwrap();
        }
        store
            .insert_hook(&hook(
                "h-decoy",
                CHIEF,
                "a1",
                T0,
                HookState::Scheduled,
                tenant,
            ))
            .await
            .unwrap();
    }
});
