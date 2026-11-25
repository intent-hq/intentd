//! Event log and persisted event-subscription store methods on both
//! backends: inserts, every `query_events` filter (including the caller-bound
//! desktop visibility), the chunked retention sweeps, and subscription CRUD.
//!
//! Event types, timestamps and paths mix case so `COLLATE "C"` and `SQLite`'s
//! ASCII-only case-insensitive `LIKE` decide results; JSON filters probe
//! non-string values. Decoy rule: the decoy runs the same body, so it owns a
//! row in every state each filter selects; at the end of its run it puts back
//! the rows the subject deletes and adds rows behind the ids the subject
//! probes as absent.

use intent_core::desktop::{with_connection, DesktopConnection};
use intent_core::{
    with_caller, ActorType, AgentId, Caller, ClientId, Event, EventActor, HostRole, PrincipalId,
    Workspace, WorkspaceId,
};
use intent_store::{EventQuery, NewEvent, PersistedEventSubscription, Store};
use serde_json::{json, Value};

use crate::harness::parity_test;

const CUTOFF: &str = "2026-01-10T00:00:00Z";
const OLD: &str = "2026-01-01T00:00:00Z";

fn wid(id: &str) -> WorkspaceId {
    WorkspaceId(id.into())
}

fn aid(id: &str) -> AgentId {
    AgentId(id.into())
}

fn actor(actor_type: ActorType, id: Option<&str>) -> EventActor {
    EventActor {
        actor_type,
        id: id.map(Into::into),
        ..Default::default()
    }
}

/// An event labelled `n` (in `data.n`), by the system actor.
fn ev(ws: &str, n: &str, event_type: &str, timestamp: &str, tenant: &str) -> NewEvent {
    NewEvent {
        workspace_id: wid(ws),
        timestamp: timestamp.into(),
        event_type: event_type.into(),
        actor: actor(ActorType::System, None),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: json!({ "n": n, "tenant": tenant }),
    }
}

#[track_caller]
fn with_data(mut e: NewEvent, extra: Value) -> NewEvent {
    let Value::Object(extra) = extra else {
        panic!("extra data must be an object");
    };
    for (k, v) in extra {
        e.data[k] = v;
    }
    e
}

fn labels(events: &[Event]) -> Vec<String> {
    events
        .iter()
        .map(|e| e.data["n"].as_str().unwrap().to_owned())
        .collect()
}

/// Labels `n<to>` down to `n<from>`, skipping `except`.
fn desc(to: u32, from: u32, except: &[u32]) -> Vec<String> {
    (from..=to)
        .rev()
        .filter(|i| !except.contains(i))
        .map(|i| format!("n{i}"))
        .collect()
}

fn ts(i: u32) -> String {
    format!("2026-01-01T00:00:{i:02}Z")
}

async fn query(store: &Store, q: EventQuery) -> Vec<String> {
    labels(&store.query_events(&q).await.unwrap())
}

fn in_ws(ws: &str) -> EventQuery {
    EventQuery {
        workspace_id: Some(wid(ws)),
        ..Default::default()
    }
}

fn wire(principal: &str) -> Caller {
    Caller::Wire {
        principal_id: PrincipalId(principal.into()),
        host_role: HostRole::Guest,
    }
}

/// The 23 labelled events (`n1`..`n23`, timestamps increasing): `ws-a` holds
/// `n1`-`n21`, `ws-own` the permission request `n22`, `ws-b` `n23`.
fn seed_events(tenant: &str) -> Vec<NewEvent> {
    let e = |i: u32, ty: &str| ev("ws-a", &format!("n{i}"), ty, &ts(i), tenant);
    let md = |mut x: NewEvent, m: Value| {
        x.metadata = Some(m);
        x
    };
    let mut n1 = with_data(e(1, "note:updated"), json!({"path": "/Repo/SRC/a.rs"}));
    n1.actor = actor(ActorType::Agent, Some("agent-1"));
    n1.session_id = Some("s1".into());
    n1.correlation_id = Some("c1".into());
    n1.parent_event_id = Some("p1".into());
    let mut n2 = with_data(
        e(2, "note:deleted"),
        json!({"relativePath": "/repo/src/b.rs"}),
    );
    n2.actor = actor(ActorType::User, Some("u1"));
    n2.session_id = Some("s1".into());
    let mut n3 = e(3, "NOTE:updated");
    n3.actor = actor(ActorType::Agent, Some("agent-2"));
    let desk = "desktop:frame";
    vec![
        n1,
        n2,
        n3,
        e(4, "Note:x"),
        e(5, "no%e:y"),
        e(6, "no_e:x"),
        e(7, "émoji:added"),
        e(8, "discussion:message"),
        with_data(
            e(9, "client:connected"),
            json!({"principalId": "guest-g", "note": "a\u{0}b"}),
        ),
        with_data(
            e(10, "client:disconnected"),
            json!({"principalId": "other"}),
        ),
        with_data(e(11, "client:updated"), json!({"principalId": 7})),
        md(e(12, desk), json!({"desktopPrincipalId": "desk-p"})),
        md(
            e(13, desk),
            json!({"desktopPrincipalId": "desk-p", "desktopConnectionEpoch": "E1", "x": "\u{0}"}),
        ),
        md(
            e(14, desk),
            json!({"desktopPrincipalId": "desk-p", "desktopConnectionEpoch": "E2"}),
        ),
        md(
            e(15, "DESKTOP:frame"),
            json!({"desktopPrincipalId": "desk-p"}),
        ),
        md(e(16, desk), json!({"desktopPrincipalId": "other-p"})),
        e(17, desk),
        e(18, intent_core::events::AGENT_PERMISSION_RESOLVED),
        with_data(e(19, "file:changed"), json!({"path": "/repo/a%b/x"})),
        with_data(e(20, "file:changed"), json!({"path": "/repo/aXb/x"})),
        with_data(e(21, "file:changed"), json!({"path": "/É/x"})),
        ev(
            "ws-own",
            "n22",
            intent_core::events::AGENT_PERMISSION_REQUEST,
            &ts(22),
            tenant,
        ),
        ev("ws-b", "n23", "note:updated", &ts(23), tenant),
    ]
}

/// `ws-own`, owned by the primary principal; returns that principal.
async fn owned_workspace(store: &Store, tenant: &str) -> PrincipalId {
    store
        .insert_workspace(&Workspace {
            id: wid("ws-own"),
            title: format!("ws-own of {tenant}"),
            branch: "main".into(),
            created_at: OLD.into(),
            updated_at: OLD.into(),
            ..Default::default()
        })
        .await
        .unwrap();
    store
        .get_workspace_owner_principal_id(&wid("ws-own"))
        .await
        .unwrap()
        .expect("ws-own has an owner")
}

parity_test!(event_inserts_and_queries, |store, tenant| {
    assert!(store
        .query_events(&EventQuery::default())
        .await
        .unwrap()
        .is_empty());
    assert!(store.insert_events(&[]).await.unwrap().is_empty());
    assert!(store
        .events_by_workspace(&wid("ws-ghost"), 10)
        .await
        .unwrap()
        .is_empty());
    let owner = owned_workspace(&store, tenant).await;

    let seeds = seed_events(tenant);
    let batch = store.insert_events(&seeds[..11]).await.unwrap();
    assert_eq!(
        labels(&batch),
        desc(11, 1, &[]).into_iter().rev().collect::<Vec<_>>()
    );
    let mut ids: Vec<&str> = batch.iter().map(|e| e.id.as_str()).collect();
    assert!(ids.iter().all(|id| uuid::Uuid::parse_str(id).is_ok()));
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), 11);
    for seed in &seeds[11..] {
        let one = store.insert_event(seed).await.unwrap();
        assert_eq!(one.data, seed.data);
        assert_eq!(one.metadata, seed.metadata);
    }
    // The persisted row round-trips every column.
    assert_eq!(
        store
            .events_by_type(&wid("ws-a"), "note:updated", 5)
            .await
            .unwrap(),
        [batch[0].clone()]
    );
    assert_eq!(batch[0].parent_event_id.as_deref(), Some("p1"));

    // Ties on `timestamp` fall back to `id DESC`; `t` sorts after `T` in
    // byte order (a locale collation puts it first).
    let tie_a = store
        .insert_event(&ev(
            "ws-t",
            "tie-a",
            "tie:x",
            "2026-01-03T00:00:00Z",
            tenant,
        ))
        .await
        .unwrap();
    let tie_b = store
        .insert_event(&ev(
            "ws-t",
            "tie-b",
            "tie:x",
            "2026-01-03T00:00:00Z",
            tenant,
        ))
        .await
        .unwrap();
    for (n, t) in [
        ("c-upper", "2026-01-02T00:00:00Z"),
        ("c-lower", "2026-01-02t00:00:00Z"),
    ] {
        store
            .insert_event(&ev("ws-t", n, "c:x", t, tenant))
            .await
            .unwrap();
    }
    let mut ties = [tie_a, tie_b];
    ties.sort_by(|x, y| y.id.as_bytes().cmp(x.id.as_bytes()));
    let mut ws_t = labels(&ties);
    ws_t.extend(["c-lower".into(), "c-upper".into()]);
    assert_eq!(query(&store, in_ws("ws-t")).await, ws_t);
    assert_eq!(
        query(
            &store,
            EventQuery {
                until: Some("2026-01-02T00:00:00Z".into()),
                ..in_ws("ws-t")
            }
        )
        .await,
        ["c-upper"]
    );
    assert_eq!(
        query(
            &store,
            EventQuery {
                since: Some("2026-01-02t00:00:00Z".into()),
                ..in_ws("ws-t")
            }
        )
        .await,
        ws_t[..3]
    );

    // No caller: desktop events (any case) are hidden.
    let ws_a = desc(21, 1, &[12, 13, 14, 15, 16, 17]);
    let mut all = ws_t.clone();
    all.extend(desc(23, 22, &[]));
    all.extend(ws_a.clone());
    assert_eq!(query(&store, EventQuery::default()).await, all);
    assert_eq!(query(&store, in_ws("ws-a")).await, ws_a);
    assert_eq!(
        query(
            &store,
            EventQuery {
                exclude_discussion_events: true,
                ..in_ws("ws-a")
            }
        )
        .await,
        desc(21, 1, &[8, 12, 13, 14, 15, 16, 17])
    );

    let types = |t: &[&str]| EventQuery {
        event_types: t.iter().map(|s| (*s).to_owned()).collect(),
        ..Default::default()
    };
    assert_eq!(query(&store, types(&["note:updated"])).await, ["n23", "n1"]);
    assert_eq!(
        query(
            &store,
            EventQuery {
                workspace_id: Some(wid("ws-a")),
                ..types(&["note:updated", "note:deleted", "nope"])
            }
        )
        .await,
        ["n2", "n1"]
    );
    for (prefix, want) in [
        ("note:", vec!["n23", "n2", "n1"]),
        ("NOTE:", vec!["n3"]),
        ("Note:", vec!["n4"]),
        ("no_e:", vec!["n6"]),
        ("no%e:", vec!["n5"]),
        ("émoji:", vec!["n7"]),
        ("desktop:", vec![]),
    ] {
        let got = query(
            &store,
            EventQuery {
                event_type_prefix: Some(prefix.into()),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(got, want, "prefix {prefix}");
    }

    let by_actor = |at: Option<ActorType>, id: Option<&str>| EventQuery {
        actor_type: at,
        actor_id: id.map(Into::into),
        ..in_ws("ws-a")
    };
    assert_eq!(
        query(&store, by_actor(Some(ActorType::Agent), None)).await,
        ["n3", "n1"]
    );
    assert_eq!(
        query(&store, by_actor(Some(ActorType::User), None)).await,
        ["n2"]
    );
    assert_eq!(
        query(&store, by_actor(Some(ActorType::Agent), Some("agent-1"))).await,
        ["n1"]
    );
    assert_eq!(query(&store, by_actor(None, Some("u1"))).await, ["n2"]);
    assert!(query(&store, by_actor(Some(ActorType::Tool), None))
        .await
        .is_empty());
    assert_eq!(
        query(
            &store,
            EventQuery {
                session_id: Some("s1".into()),
                ..Default::default()
            }
        )
        .await,
        ["n2", "n1"]
    );
    assert_eq!(
        query(
            &store,
            EventQuery {
                session_id: Some("s1".into()),
                correlation_id: Some("c1".into()),
                ..Default::default()
            }
        )
        .await,
        ["n1"]
    );
    assert_eq!(
        query(
            &store,
            EventQuery {
                since: Some(ts(9)),
                until: Some(ts(11)),
                ..in_ws("ws-a")
            }
        )
        .await,
        ["n11", "n10", "n9"]
    );

    // `SQLite` LIKE folds ASCII case only; `%` and `_` in the prefix are literal.
    for (prefix, want) in [
        ("/repo/src", vec!["n2", "n1"]),
        ("/REPO/a%b", vec!["n19"]),
        ("/repo/a_b", vec![]),
        ("/É", vec!["n21"]),
        ("/é", vec![]),
    ] {
        let got = query(
            &store,
            EventQuery {
                path_prefix: Some(prefix.into()),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(got, want, "path prefix {prefix}");
    }

    // Client events: only the principal's own, and only as a JSON string.
    let client = |p: &str| EventQuery {
        client_principal_id: Some(PrincipalId(p.into())),
        ..in_ws("ws-a")
    };
    assert_eq!(
        query(&store, client("guest-g")).await,
        desc(21, 1, &[10, 11, 12, 13, 14, 15, 16, 17])
    );
    assert_eq!(
        query(&store, client("7")).await,
        desc(21, 1, &[9, 10, 11, 12, 13, 14, 15, 16, 17])
    );

    // Permission events need an explicit owner membership of their workspace.
    let guest = |p: &PrincipalId| EventQuery {
        guest_permission_principal_id: Some(p.clone()),
        ..Default::default()
    };
    let without = |drop: &[&str]| -> Vec<String> {
        all.iter()
            .filter(|n| !drop.contains(&n.as_str()))
            .cloned()
            .collect()
    };
    assert_eq!(query(&store, guest(&owner)).await, without(&["n18"]));
    assert_eq!(
        query(&store, guest(&PrincipalId("nobody".into()))).await,
        without(&["n18", "n22"])
    );

    let page = |limit: Option<i64>, offset: Option<i64>| EventQuery {
        limit,
        offset,
        ..in_ws("ws-a")
    };
    assert_eq!(
        query(&store, page(Some(3), None)).await,
        ["n21", "n20", "n19"]
    );
    assert_eq!(query(&store, page(Some(2), Some(1))).await, ["n20", "n19"]);
    assert_eq!(query(&store, page(Some(-1), None)).await, ws_a);
    assert_eq!(query(&store, page(Some(-1), Some(14))).await, ["n1"]);
    assert_eq!(query(&store, page(Some(2), Some(-5))).await, ["n21", "n20"]);
    assert!(query(&store, page(Some(0), None)).await.is_empty());
    assert_eq!(query(&store, page(None, Some(3))).await, ws_a);
    assert_eq!(
        labels(&store.events_by_workspace(&wid("ws-a"), 2).await.unwrap()),
        ["n21", "n20"]
    );
    assert_eq!(
        labels(
            &store
                .events_by_type(&wid("ws-b"), "note:updated", 5)
                .await
                .unwrap()
        ),
        ["n23"]
    );
    assert!(store
        .events_by_type(&wid("ws-a"), "note:", 5)
        .await
        .unwrap()
        .is_empty());

    // Desktop events: the wire caller's own; an epoch must match the
    // connection, as a JSON string.
    let visible = |extra: &[u32]| -> Vec<String> {
        desc(21, 1, &[12, 13, 14, 15, 16, 17])
            .into_iter()
            .chain(extra.iter().map(|i| format!("n{i}")))
            .collect::<Vec<_>>()
    };
    let sorted = |mut v: Vec<String>| {
        v.sort_by_key(|n| std::cmp::Reverse(n[1..].parse::<u32>().unwrap()));
        v
    };
    let as_caller = |principal: &str, epoch: Option<&str>| {
        let store = store.clone();
        let caller = wire(principal);
        let connection = epoch.map(|e| DesktopConnection {
            client_id: ClientId::from("client-1"),
            principal_id: PrincipalId(principal.into()),
            connection_epoch: e.into(),
        });
        async move {
            with_caller(
                caller,
                with_connection(connection, async { query(&store, in_ws("ws-a")).await }),
            )
            .await
        }
    };
    assert_eq!(as_caller("desk-p", None).await, sorted(visible(&[12, 15])));
    assert_eq!(
        as_caller("desk-p", Some("E1")).await,
        sorted(visible(&[12, 13, 15]))
    );
    assert_eq!(
        as_caller("other-p", Some("E2")).await,
        sorted(visible(&[16]))
    );

    if tenant == "decoy" {
        store
            .insert_event(&ev("ws-ghost", "ghost", "note:updated", OLD, tenant))
            .await
            .unwrap();
    }
});

/// Old rows of every swept family, survivors around them, and `extra` more
/// old `file:changed` rows (enough to need a second delete chunk).
fn retention_rows(tenant: &str, extra: usize) -> Vec<NewEvent> {
    use intent_core::events as e;
    let mut rows: Vec<NewEvent> = [
        "agent:stream:chunk",
        "file:changed",
        "host:exec:stdout",
        e::TERMINAL_DATA,
        e::SCRIPT_OUTPUT,
        e::WORKSPACE_UPDATED,
        e::DRAFT_CHANGED,
        e::AGENT_STATUS_CHANGED,
        e::AGENT_IDLE,
        e::AGENT_SUBSCRIPTIONS_CHANGED,
        e::SETTINGS_CHANGED,
        e::WORKSPACE_TOKEN_USAGE_CHANGED,
        e::AGENT_QUEUE_UPDATED,
        e::AGENT_TOOL_CALL,
    ]
    .iter()
    .map(|t| ev("ws-r", &format!("old {t}"), t, OLD, tenant))
    .collect();
    rows.extend((0..extra).map(|i| ev("ws-r", &format!("bulk {i}"), "file:changed", OLD, tenant)));
    rows
}

/// Rows every sweep keeps: lifecycle types, case and prefix near-misses, and
/// swept types at exactly the cutoff.
fn kept_rows(tenant: &str) -> Vec<NewEvent> {
    [
        ("keep script:state", "script:state", OLD),
        ("keep File:changed", "File:changed", OLD),
        ("keep agent:streamy", "agent:streamy", OLD),
        ("keep file;x", "file;x", OLD),
        ("keep workspace:created", "workspace:created", OLD),
        ("keep at cutoff", "file:changed", CUTOFF),
        (
            "keep tool at cutoff",
            intent_core::events::AGENT_TOOL_CALL,
            CUTOFF,
        ),
    ]
    .iter()
    .map(|(n, t, at)| ev("ws-r", n, t, at, tenant))
    .collect()
}

parity_test!(event_retention_sweeps, |store, tenant| {
    assert_eq!(
        store.delete_tool_call_events_before(CUTOFF).await.unwrap(),
        0
    );
    let bulk = 1000;
    store
        .insert_events(&retention_rows(tenant, bulk))
        .await
        .unwrap();
    store.insert_events(&kept_rows(tenant)).await.unwrap();

    assert_eq!(
        store.delete_tool_call_events_before(CUTOFF).await.unwrap(),
        1
    );
    assert_eq!(
        store.delete_tool_call_events_before(CUTOFF).await.unwrap(),
        0
    );
    assert_eq!(
        store.delete_ephemeral_events_before(CUTOFF).await.unwrap(),
        13 + bulk as u64
    );
    assert_eq!(
        store.delete_ephemeral_events_before(CUTOFF).await.unwrap(),
        0
    );
    let mut left = query(&store, in_ws("ws-r")).await;
    left.sort();
    assert_eq!(
        left,
        [
            "keep File:changed",
            "keep agent:streamy",
            "keep at cutoff",
            "keep file;x",
            "keep script:state",
            "keep tool at cutoff",
            "keep workspace:created",
        ]
    );

    if tenant == "decoy" {
        store
            .insert_events(&retention_rows(tenant, bulk))
            .await
            .unwrap();
    }
});

fn sub(
    id: &str,
    ws: &str,
    agent: &str,
    created_at: &str,
    tenant: &str,
) -> PersistedEventSubscription {
    PersistedEventSubscription {
        id: id.into(),
        workspace_id: wid(ws),
        subscriber_agent_id: aid(agent),
        event_types: vec!["agent:*".into(), format!("{tenant}:x")],
        exclude_self: true,
        batch_window_ms: 500,
        created_at: created_at.into(),
    }
}

fn sub_ids(subs: &[PersistedEventSubscription]) -> Vec<&str> {
    subs.iter().map(|s| s.id.as_str()).collect()
}

const S0: &str = "2026-01-01T00:00:00.000Z";
const TIE: &str = "2026-01-02T00:00:00.000Z";
const S2: &str = "2026-01-03T00:00:00.000Z";
const S3: &str = "2026-01-04T00:00:00.000Z";

/// The rows the subject's deletes remove.
fn deleted_subs(tenant: &str) -> [PersistedEventSubscription; 3] {
    [
        sub("sB", "ws-1", "a1", TIE, tenant),
        sub("s-a2", "ws-1", "a2", S2, tenant),
        sub("s-w2", "ws-2", "a1", S3, tenant),
    ]
}

parity_test!(event_subscriptions_round_trip, |store, tenant| {
    assert!(store.list_event_subscriptions().await.unwrap().is_empty());
    store.delete_event_subscription("s-ghost").await.unwrap();
    store
        .delete_event_subscriptions_for_agent(&aid("a-ghost"))
        .await
        .unwrap();
    store
        .delete_event_subscriptions_for_workspace(&wid("ws-ghost"))
        .await
        .unwrap();

    // `sc`, `sB`, `sa` tie on `created_at`: insertion order, byte order and
    // a locale collation all differ.
    let early = sub("s-early", "ws-1", "a1", S0, tenant);
    let sc = sub("sc", "ws-1", "a1", TIE, tenant);
    let sa = sub("sa", "ws-1", "a1", TIE, tenant);
    let [sb, by_a2, in_w2] = deleted_subs(tenant);
    for s in [&in_w2, &sc, &sb, &sa, &by_a2, &early] {
        store.upsert_event_subscription(s).await.unwrap();
    }
    let listed = store.list_event_subscriptions().await.unwrap();
    assert_eq!(
        sub_ids(&listed),
        ["s-early", "sc", "sB", "sa", "s-a2", "s-w2"]
    );
    let got = &listed[1];
    assert_eq!(
        (
            &got.workspace_id,
            &got.subscriber_agent_id,
            &got.event_types
        ),
        (&sc.workspace_id, &sc.subscriber_agent_id, &sc.event_types)
    );
    assert!(got.exclude_self);
    assert_eq!((got.batch_window_ms, got.created_at.as_str()), (500, TIE));

    // An upsert changes only the mutable columns and keeps the row's place.
    let changed = PersistedEventSubscription {
        workspace_id: wid("ws-9"),
        subscriber_agent_id: aid("a9"),
        event_types: vec![],
        exclude_self: false,
        batch_window_ms: 0,
        created_at: S3.into(),
        ..sc.clone()
    };
    store.upsert_event_subscription(&changed).await.unwrap();
    let listed = store.list_event_subscriptions().await.unwrap();
    assert_eq!(
        sub_ids(&listed),
        ["s-early", "sc", "sB", "sa", "s-a2", "s-w2"]
    );
    let got = &listed[1];
    assert_eq!(
        (
            got.workspace_id.0.as_str(),
            got.subscriber_agent_id.0.as_str()
        ),
        ("ws-1", "a1")
    );
    assert!(got.event_types.is_empty() && !got.exclude_self);
    assert_eq!((got.batch_window_ms, got.created_at.as_str()), (0, TIE));

    store.delete_event_subscription("sB").await.unwrap();
    store.delete_event_subscription("sB").await.unwrap();
    store
        .delete_event_subscriptions_for_agent(&aid("a2"))
        .await
        .unwrap();
    store
        .delete_event_subscriptions_for_workspace(&wid("ws-2"))
        .await
        .unwrap();
    assert_eq!(
        sub_ids(&store.list_event_subscriptions().await.unwrap()),
        ["s-early", "sc", "sa"]
    );

    if tenant == "decoy" {
        for s in deleted_subs(tenant) {
            store.upsert_event_subscription(&s).await.unwrap();
        }
        store
            .upsert_event_subscription(&sub("s-ghost", "ws-ghost", "a-ghost", S0, tenant))
            .await
            .unwrap();
    }
});

/// Seqs of `pairs`, asserting each returned event is the tenant's.
#[track_caller]
fn seqs_of(pairs: &[(Event, u64)], tenant: &str) -> Vec<u64> {
    for (e, _) in pairs {
        assert_eq!(e.data["tenant"], tenant, "another tenant's event: {e:?}");
    }
    pairs.iter().map(|(_, s)| *s).collect()
}

#[track_caller]
fn assert_strictly_increasing(seqs: &[u64]) {
    assert!(
        seqs.windows(2).all(|w| w[0] < w[1]),
        "seqs must strictly increase: {seqs:?}"
    );
}

parity_test!(event_seq_cursor_pages_in_commit_order, |store, tenant| {
    assert_eq!(store.max_event_seq().await.unwrap(), 0);
    assert!(store.events_after(0, -1).await.unwrap().is_empty());
    assert!(store.insert_events_seq(&[]).await.unwrap().is_empty());

    let e = |i: u32| ev("ws-a", &format!("n{i}"), "note:updated", &ts(i), tenant);
    let (first, s1) = store.insert_event_seq(&e(1)).await.unwrap();
    assert!(s1 > 0);
    assert_eq!(store.max_event_seq().await.unwrap(), s1);
    // A write to another table between batches: on Postgres it takes a
    // number from the same tenant counter, so the event seqs have a gap.
    store.set_setting("seq-gap", tenant).await.unwrap();
    let batch = store.insert_events_seq(&[e(2), e(3), e(4)]).await.unwrap();
    // Input order, and the timestamp does not decide the seq.
    let (late, s5) = store
        .insert_event_seq(&ev("ws-b", "n5", "note:updated", OLD, tenant))
        .await
        .unwrap();
    let mut inserted = vec![(first, s1)];
    inserted.extend(batch);
    inserted.push((late, s5));
    assert_eq!(
        labels(&inserted.iter().map(|(e, _)| e.clone()).collect::<Vec<_>>()),
        ["n1", "n2", "n3", "n4", "n5"]
    );
    let seqs = seqs_of(&inserted, tenant);
    assert_strictly_increasing(&seqs);
    assert_eq!(store.max_event_seq().await.unwrap(), s5);

    // The full read matches what the inserts returned, ids and all.
    let all = store.events_after(0, -1).await.unwrap();
    assert_eq!(seqs_of(&all, tenant), seqs);
    let ids = |v: &[(Event, u64)]| v.iter().map(|(e, _)| e.id.clone()).collect::<Vec<_>>();
    assert_eq!(ids(&all), ids(&inserted));
    assert_eq!(all[0].0, inserted[0].0);

    // Paging by the last seen seq reassembles the log.
    let mut paged = Vec::new();
    let mut cursor = 0;
    loop {
        let page = store.events_after(cursor, 2).await.unwrap();
        assert!(page.len() <= 2);
        let Some((_, last)) = page.last() else { break };
        cursor = *last;
        paged.extend(page);
    }
    assert_eq!(seqs_of(&paged, tenant), seqs);
    assert_eq!(cursor, s5);

    // A cursor in a gap or mid-log starts right after it.
    assert_eq!(
        seqs_of(&store.events_after(seqs[1] - 1, 1).await.unwrap(), tenant),
        [seqs[1]]
    );
    assert_eq!(
        seqs_of(&store.events_after(seqs[2], -1).await.unwrap(), tenant),
        seqs[3..]
    );
    assert!(store.events_after(0, 0).await.unwrap().is_empty());

    // Nothing past the watermark, however far.
    assert!(store.events_after(s5, 10).await.unwrap().is_empty());
    assert!(store.events_after(s5 + 1000, -1).await.unwrap().is_empty());
    assert!(store.events_after(u64::MAX, -1).await.unwrap().is_empty());

    // The plain inserts take seqs too.
    store.insert_event(&e(6)).await.unwrap();
    store.insert_events(&[e(7)]).await.unwrap();
    let tail = store.events_after(s5, -1).await.unwrap();
    assert_eq!(
        labels(&tail.iter().map(|(e, _)| e.clone()).collect::<Vec<_>>()),
        ["n6", "n7"]
    );
    assert_strictly_increasing(&[vec![s5], seqs_of(&tail, tenant)].concat());
    assert_eq!(store.max_event_seq().await.unwrap(), tail[1].1);
});

parity_test!(
    event_seq_is_unique_and_ordered_across_concurrent_writers,
    |store, tenant| {
        const WRITERS: usize = 8;
        const BATCHES: usize = 5;
        const PER_BATCH: usize = 4;
        let start = store.max_event_seq().await.unwrap();
        let mut writers = tokio::task::JoinSet::new();
        for w in 0..WRITERS {
            let store = store.clone();
            let tenant = tenant.to_owned();
            writers.spawn(async move {
                let mut mine = Vec::new();
                for b in 0..BATCHES {
                    let batch: Vec<NewEvent> = (0..PER_BATCH)
                        .map(|i| {
                            ev(
                                "ws-a",
                                &format!("w{w}-b{b}-{i}"),
                                "note:updated",
                                OLD,
                                &tenant,
                            )
                        })
                        .collect();
                    // Each writer's own later commits get later seqs.
                    mine.extend(store.insert_events_seq(&batch).await.unwrap());
                }
                mine
            });
        }
        let mut returned = Vec::new();
        while let Some(mine) = writers.join_next().await {
            let mine = mine.unwrap();
            assert_strictly_increasing(&seqs_of(&mine, tenant));
            returned.extend(mine);
        }
        assert_eq!(returned.len(), WRITERS * BATCHES * PER_BATCH);

        // The log holds exactly the returned (id, seq) pairs, in seq order, and
        // a batch's rows are numbered together (no other writer interleaves).
        let log = store.events_after(start, -1).await.unwrap();
        let seqs = seqs_of(&log, tenant);
        assert_strictly_increasing(&seqs);
        let mut expected: Vec<(String, u64)> =
            returned.iter().map(|(e, s)| (e.id.clone(), *s)).collect();
        expected.sort_by_key(|(_, s)| *s);
        let got: Vec<(String, u64)> = log.iter().map(|(e, s)| (e.id.clone(), *s)).collect();
        assert_eq!(got, expected);
        for chunk in log.chunks(PER_BATCH) {
            let batch = chunk[0].0.data["n"]
                .as_str()
                .unwrap()
                .rsplit_once('-')
                .unwrap()
                .0
                .to_owned();
            for (i, (e, _)) in chunk.iter().enumerate() {
                assert_eq!(
                    e.data["n"],
                    format!("{batch}-{i}"),
                    "batch split: {chunk:?}"
                );
            }
        }
        assert_eq!(store.max_event_seq().await.unwrap(), *seqs.last().unwrap());
    }
);
