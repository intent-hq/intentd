//! Home final stops, cloud home rebuilds and home moves on both backends.
//!
//! Bodies use fixed ids and run the same on every tenant. At the end of the
//! decoy run, the rows the subject's writes and probes reach are put back in
//! their pre-write state, so a statement without its tenant filter reaches
//! a decoy row on the subject run.

use intent_core::machines::{
    CloudMachine, CloudMachineSize, CloudSandboxState, CloudStartedBy, ExposedMachine,
    RemoteMachineState,
};
use intent_core::node_link::{HomeStopReason, HomeStopRecord};
use intent_core::{ExecutorCredentialId, HomeId, HomeState, Workspace, WorkspaceHome, WorkspaceId};
use intent_store::{HomeAutoStart, HomeMove, Store};

use crate::harness::{exec, parity_test, TestDb};

const T0: &str = "2026-01-01T00:00:00.000Z";
const T1: &str = "2026-01-02T00:00:00.000Z";
const T2: &str = "2026-01-03T00:00:00.000Z";

fn wid(id: &str) -> WorkspaceId {
    WorkspaceId(id.into())
}

fn hid(id: &str) -> HomeId {
    HomeId(id.into())
}

async fn workspace(store: &Store, id: &str) {
    store
        .insert_workspace(&Workspace {
            id: wid(id),
            title: format!("title {id}"),
            branch: "main".into(),
            created_at: T0.into(),
            updated_at: T0.into(),
            ..Default::default()
        })
        .await
        .unwrap();
}

fn home(home_id: &str, node: Option<&str>, cloud: bool) -> WorkspaceHome {
    WorkspaceHome {
        home_id: hid(home_id),
        node_id: node.map(str::to_owned),
        cloud_machine_id: cloud.then(|| "e2b".to_owned()),
        name: if cloud { "E2B" } else { "Mac" }.into(),
        state: HomeState::Attached,
        since: T0.into(),
        last_backup_at: Some(T0.into()),
        backup_warning: false,
        setup: None,
    }
}

fn person(id: &str) -> CloudStartedBy {
    CloudStartedBy {
        principal_id: id.into(),
        display_name: None,
    }
}

fn machine(tenant: &str, id: &str, sandbox_of: Option<&str>) -> ExposedMachine {
    ExposedMachine {
        id: id.into(),
        name: format!("{tenant} {id}"),
        hosts: vec!["127.0.0.1".into()],
        port: 7443,
        tc_address: None,
        fingerprint: "ab".repeat(32),
        credential_id: ExecutorCredentialId::from(format!("cred-{id}")),
        platform: None,
        capabilities: None,
        observed_at: None,
        state: RemoteMachineState::Disconnected,
        added_at: T0.into(),
        last_connected_at: None,
        node_protocol: None,
        cloud: sandbox_of.map(|workspace_id| CloudMachine {
            provider: "e2b".into(),
            cloud_machine_id: "e2b".into(),
            sandbox_id: format!("sb-{id}"),
            volume_id: None,
            state: CloudSandboxState::Running,
            template: "t".into(),
            size: CloudMachineSize::S4x8,
            started_by: person(&format!("{tenant}-starter")),
            agent_id: "agent-1".into(),
            workspace_id: workspace_id.into(),
            started_at: T0.into(),
            running_seconds: 0,
            running_since: None,
            paused_at: None,
            relay_url: None,
        }),
    }
}

fn stop(home_id: &str, stop_id: &str, node: &str, at: &str) -> HomeStopRecord {
    HomeStopRecord {
        workspace_id: wid("ws-s"),
        home_id: hid(home_id),
        node_id: node.into(),
        stop_id: stop_id.into(),
        reason: HomeStopReason::Retired,
        orphaned_at: None,
        recorded_at: at.into(),
    }
}

fn moving(workspace: &str, from: &str, target: WorkspaceHome) -> HomeMove {
    HomeMove {
        workspace_id: wid(workspace),
        target: WorkspaceHome {
            since: T1.into(),
            last_backup_at: None,
            ..target
        },
        from_home_id: hid(from),
        from_state: HomeState::Attached,
        from_since: T0.into(),
        pushed: false,
        started_at: T1.into(),
    }
}

fn home_ids(stops: &[HomeStopRecord]) -> Vec<&str> {
    stops.iter().map(|s| s.home_id.as_str()).collect()
}

parity_test!(home_stops_switch_record_list_and_finish, |store, tenant| {
    workspace(&store, "ws-s").await;
    let ws = wid("ws-s");
    store
        .set_workspace_home(&ws, &home("h-a", Some("n1"), false))
        .await
        .unwrap();

    let moved = store
        .switch_workspace_home(&ws, &home("h-b", Some("n2"), false), T1)
        .await
        .unwrap()
        .expect("the old machine home gets a stop");
    assert_eq!(
        (moved.home_id.as_str(), moved.node_id.as_str(), moved.reason),
        ("h-a", "n1", HomeStopReason::Moved)
    );
    assert!(moved.orphaned_at.is_some());
    assert_eq!(
        store
            .get_workspace_home(&ws)
            .await
            .unwrap()
            .unwrap()
            .home_id,
        hid("h-b")
    );
    assert_eq!(
        store.get_home_stop(&hid("h-a")).await.unwrap(),
        Some(moved.clone())
    );
    assert_eq!(
        store
            .switch_workspace_home(&ws, &home("h-b", Some("n2"), false), T1)
            .await
            .unwrap(),
        None,
        "the same home replaces nothing"
    );
    assert!(store
        .switch_workspace_home(&wid("ws-ghost"), &home("h-g", None, false), T1)
        .await
        .is_err());
    assert_eq!(
        store
            .finish_home_stop(&hid("h-a"), &moved.stop_id)
            .await
            .unwrap(),
        Some(moved)
    );
    assert_eq!(store.get_home_stop(&hid("h-a")).await.unwrap(), None);

    let c = stop("h-c", "stop-c", "n1", T0);
    assert_eq!(store.record_home_stop(&c).await.unwrap(), c);
    assert_eq!(
        store
            .record_home_stop(&stop("h-c", "stop-c2", "n1", T2))
            .await
            .unwrap(),
        c,
        "a home keeps its first stop"
    );
    let d = stop("h-d", "stop-d", "n1", T2);
    store.record_home_stop(&d).await.unwrap();
    let e = stop("h-e", "stop-e", "n2", T1);
    store.record_home_stop(&e).await.unwrap();
    assert_eq!(
        home_ids(&store.list_home_stops("n1").await.unwrap()),
        ["h-c", "h-d"]
    );
    assert!(store.list_home_stops("n9").await.unwrap().is_empty());
    assert_eq!(
        home_ids(&store.list_workspace_home_stops(&ws).await.unwrap()),
        ["h-c", "h-e", "h-d"]
    );
    assert!(!store.home_stop_discards_push(&hid("h-c")).await.unwrap());
    assert!(!store.home_stop_discards_push(&hid("h-none")).await.unwrap());
    assert_eq!(store.get_home_stop(&hid("h-none")).await.unwrap(), None);

    assert_eq!(
        store.finish_home_stop(&hid("h-c"), "stop-x").await.unwrap(),
        None
    );
    assert_eq!(
        store.finish_home_stop(&hid("h-c"), "stop-c").await.unwrap(),
        Some(c.clone())
    );
    assert_eq!(store.delete_home_stops_on_machine("n1").await.unwrap(), 1);
    assert_eq!(store.delete_home_stops_on_machine("n1").await.unwrap(), 0);
    assert_eq!(
        home_ids(&store.list_workspace_home_stops(&ws).await.unwrap()),
        ["h-e"]
    );

    if tenant == "decoy" {
        store.record_home_stop(&c).await.unwrap();
        store.record_home_stop(&d).await.unwrap();
        store
            .record_home_stop(&stop("h-none", "stop-none", "n-decoy", T0))
            .await
            .unwrap();
        store
            .record_home_stop(&stop("h-a", "stop-a", "n1", T0))
            .await
            .unwrap();
        store
            .set_workspace_home(&ws, &home("h-a", Some("n1"), false))
            .await
            .unwrap();
        workspace(&store, "ws-ghost").await;
    }
});

/// On the subject run, give the decoy's `home_rebuild` row of `ws-r` the
/// subject's new `homeId`, so a probe of it without its tenant filter
/// reaches the decoy row. Point it back to `h-decoy` afterwards.
async fn point_decoy_rebuild(db: Option<&TestDb>, tenant: &str, home_id: &str) {
    if let (Some(db), "subject") = (db, tenant) {
        exec(
            &db.admin,
            format!(
                "UPDATE home_rebuild SET home_id = '{home_id}' \
                 WHERE tenant_id = 'decoy' AND workspace_id = 'ws-r'"
            ),
        )
        .await;
    }
}

async fn seed_cloud_homes(store: &Store, tenant: &str) {
    for id in ["sb-1", "sb-2"] {
        store
            .save_exposed_machine(&machine(tenant, id, Some("ws-r")))
            .await
            .unwrap();
    }
    store
        .set_workspace_home(&wid("ws-r"), &home("h-r", Some("sb-1"), true))
        .await
        .unwrap();
    store
        .set_workspace_home(&wid("ws-q"), &home("h-q", Some("sb-2"), true))
        .await
        .unwrap();
    let mut owed = stop("h-x", "stop-x", "sb-1", T0);
    owed.workspace_id = wid("ws-r");
    store.record_home_stop(&owed).await.unwrap();
}

parity_test!(
    cloud_machine_removal_rebuilds_its_homes,
    |store, tenant, db| {
        workspace(&store, "ws-r").await;
        workspace(&store, "ws-q").await;
        let (r, q) = (wid("ws-r"), wid("ws-q"));
        seed_cloud_homes(&store, tenant).await;

        assert!(store
            .remove_cloud_machine("sb-2", false, T1)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(store.get_exposed_machine("sb-2").await.unwrap(), None);
        assert_eq!(
            store.get_workspace_home(&q).await.unwrap(),
            Some(home("h-q", Some("sb-2"), true)),
            "without rebuild_homes a home is left as is"
        );

        let rebuilt = store.remove_cloud_machine("sb-1", true, T1).await.unwrap();
        assert_eq!(rebuilt.len(), 1);
        let new_home = rebuilt[0].home_id.clone();
        assert_eq!(
            (&rebuilt[0].workspace_id, rebuilt[0].from_home_id.as_str()),
            (&r, "h-r")
        );
        assert_ne!(new_home, hid("h-r"));
        let renewed = store.get_workspace_home(&r).await.unwrap().unwrap();
        assert_eq!(
            renewed,
            WorkspaceHome {
                home_id: new_home.clone(),
                node_id: None,
                since: T1.into(),
                ..home("h-r", None, true)
            }
        );
        assert!(store.list_home_stops("sb-1").await.unwrap().is_empty());
        assert_eq!(store.get_exposed_machine("sb-1").await.unwrap(), None);

        point_decoy_rebuild(db, tenant, new_home.as_str()).await;
        assert_eq!(
            store.home_rebuild_of(&r, &new_home).await.unwrap(),
            Some(hid("h-r"))
        );
        assert_eq!(store.home_rebuild_of(&r, &hid("h-r")).await.unwrap(), None);
        assert!(store.finish_home_rebuild(&r, &new_home).await.unwrap());
        assert!(!store.finish_home_rebuild(&r, &new_home).await.unwrap());
        assert_eq!(store.home_rebuild_of(&r, &new_home).await.unwrap(), None);
        point_decoy_rebuild(db, tenant, "h-decoy").await;

        if let (Some(db), "decoy") = (db, tenant) {
            seed_cloud_homes(&store, tenant).await;
            exec(
                &db.admin,
                format!(
                    "INSERT INTO home_rebuild (tenant_id, workspace_id, home_id, from_home_id, \
                 recorded_at) VALUES ('decoy', 'ws-r', 'h-decoy', 'h-r', '{T1}')"
                ),
            )
            .await;
        }
    }
);

parity_test!(home_moves_begin_progress_and_switch, |store, tenant| {
    workspace(&store, "ws-m").await;
    let ws = wid("ws-m");
    for (id, sandbox) in [
        ("mac-1", None),
        ("sb-m", Some("ws-m")),
        ("sb-left", Some("ws-m")),
    ] {
        store
            .save_exposed_machine(&machine(tenant, id, sandbox))
            .await
            .unwrap();
    }
    store
        .set_workspace_home(&ws, &home("h-old", Some("mac-1"), false))
        .await
        .unwrap();
    assert_eq!(store.get_home_move(&ws).await.unwrap(), None);
    assert!(store.list_home_moves().await.unwrap().is_empty());

    let mv = moving("ws-m", "h-old", home("h-new", None, true));
    assert!(store.begin_home_move(&mv).await.unwrap());
    assert!(!store.begin_home_move(&mv).await.unwrap(), "already moving");
    let shown_before = store.get_workspace_home(&ws).await.unwrap().unwrap();
    assert_eq!(
        (shown_before.state, shown_before.since.as_str()),
        (HomeState::Moving, T1)
    );
    assert_eq!(store.get_home_move(&ws).await.unwrap(), Some(mv.clone()));
    assert_eq!(
        store.list_home_moves().await.unwrap(),
        std::slice::from_ref(&mv)
    );
    assert_eq!(
        store.get_shown_workspace_home(&ws).await.unwrap(),
        Some(shown_before.clone()),
        "not pushed yet: shown as is"
    );

    assert!(store
        .set_home_move_machine(&ws, &hid("h-new"), "sb-m")
        .await
        .unwrap());
    assert!(!store
        .set_home_move_machine(&ws, &hid("h-new"), "sb-other")
        .await
        .unwrap());
    let to_sb = store.home_moves_to_machine("sb-m").await.unwrap();
    assert_eq!(to_sb.len(), 1);
    assert_eq!(to_sb[0].target.node_id.as_deref(), Some("sb-m"));
    assert_eq!(
        store
            .unclaimed_home_sandboxes(&ws, "e2b", "agent-1")
            .await
            .unwrap()
            .into_iter()
            .map(|m| m.id)
            .collect::<Vec<_>>(),
        ["sb-left"]
    );

    assert!(store
        .mark_home_move_pushed(&ws, &hid("h-new"))
        .await
        .unwrap());
    assert!(!store
        .mark_home_move_pushed(&ws, &hid("h-wrong"))
        .await
        .unwrap());
    let shown = store.get_shown_workspace_home(&ws).await.unwrap().unwrap();
    assert_eq!(
        (shown.home_id.as_str(), shown.state),
        ("h-new", HomeState::Starting)
    );
    assert_eq!(
        store.list_shown_workspace_homes().await.unwrap()[&ws],
        shown
    );

    assert_eq!(
        store
            .claim_home_auto_start_or_record_move(&ws, &hid("h-new"), "sb-m", "lease", 1)
            .await
            .unwrap(),
        HomeAutoStart::AtSwitch
    );
    assert_eq!(
        store
            .claim_home_auto_start_or_record_move(&ws, &hid("h-old"), "mac-1", "lease", 1)
            .await
            .unwrap(),
        HomeAutoStart::Claimed
    );
    assert_eq!(
        store
            .claim_home_auto_start_or_record_move(&ws, &hid("h-zzz"), "mac-1", "lease", 1)
            .await
            .unwrap(),
        HomeAutoStart::No
    );

    let owner = person("owner");
    assert_eq!(
        store
            .complete_home_move(&ws, &hid("h-wrong"), &owner, T2)
            .await
            .unwrap(),
        None
    );
    let switch = store
        .complete_home_move(&ws, &hid("h-new"), &owner, T2)
        .await
        .unwrap()
        .expect("the move switches");
    assert!(switch.auto_start);
    let old_stop = switch.stop.expect("the old machine home stops");
    assert_eq!(
        (
            old_stop.home_id.as_str(),
            old_stop.node_id.as_str(),
            old_stop.reason
        ),
        ("h-old", "mac-1", HomeStopReason::Moved)
    );
    assert_eq!(
        store.get_home_stop(&hid("h-old")).await.unwrap(),
        Some(old_stop)
    );
    let switched = store.get_workspace_home(&ws).await.unwrap().unwrap();
    assert_eq!(
        switched,
        WorkspaceHome {
            node_id: Some("sb-m".into()),
            since: T2.into(),
            last_backup_at: Some(T0.into()),
            ..home("h-new", None, true)
        }
    );
    assert_eq!(
        store
            .get_exposed_machine("sb-m")
            .await
            .unwrap()
            .unwrap()
            .cloud
            .unwrap()
            .started_by,
        owner
    );
    assert_eq!(store.get_home_move(&ws).await.unwrap(), None);
    assert!(store
        .home_moves_to_machine("sb-m")
        .await
        .unwrap()
        .is_empty());

    if tenant == "decoy" {
        store
            .set_workspace_home(&ws, &home("h-old", Some("mac-1"), false))
            .await
            .unwrap();
        assert!(store.begin_home_move(&mv).await.unwrap());
        store
            .set_workspace_home(&ws, &home("h-old", Some("mac-1"), false))
            .await
            .unwrap();
    }
});

async fn begin_move_to_mac(store: &Store) {
    store
        .set_workspace_home(&wid("ws-a"), &home("h-a1", Some("mac-1"), false))
        .await
        .unwrap();
    assert!(store
        .begin_home_move(&moving("ws-a", "h-a1", home("h-a2", Some("mac-2"), false)))
        .await
        .unwrap());
}

parity_test!(home_moves_roll_back, |store, tenant| {
    for id in ["ws-a", "ws-g", "ws-x"] {
        workspace(&store, id).await;
    }
    store
        .save_exposed_machine(&machine(tenant, "mac-1", None))
        .await
        .unwrap();
    let (a, g, x) = (wid("ws-a"), wid("ws-g"), wid("ws-x"));
    begin_move_to_mac(&store).await;
    assert!(!store
        .abort_home_move(&a, &hid("h-wrong"), T2)
        .await
        .unwrap());
    assert!(store.abort_home_move(&a, &hid("h-a2"), T2).await.unwrap());
    assert!(!store.abort_home_move(&a, &hid("h-a2"), T2).await.unwrap());
    assert_eq!(
        store.get_workspace_home(&a).await.unwrap(),
        Some(home("h-a1", Some("mac-1"), false)),
        "the old home gets its state and since back"
    );
    let target_stop = store.get_home_stop(&hid("h-a2")).await.unwrap().unwrap();
    assert_eq!(
        (target_stop.node_id.as_str(), target_stop.reason),
        ("mac-2", HomeStopReason::Retired)
    );
    assert!(store.home_stop_discards_push(&hid("h-a2")).await.unwrap());

    // An old cloud home whose sandbox went while it moved is renewed.
    store
        .set_workspace_home(&g, &home("h-g1", Some("sb-gone"), true))
        .await
        .unwrap();
    assert!(store
        .begin_home_move(&moving("ws-g", "h-g1", home("h-g2", None, true)))
        .await
        .unwrap());
    assert!(store.abort_home_move(&g, &hid("h-g2"), T2).await.unwrap());
    let renewed = store.get_workspace_home(&g).await.unwrap().unwrap();
    assert_eq!(
        (
            renewed.node_id.as_deref(),
            renewed.state,
            renewed.since.as_str()
        ),
        (None, HomeState::Attached, T2)
    );
    assert_eq!(
        store.home_rebuild_of(&g, &renewed.home_id).await.unwrap(),
        Some(hid("h-g1"))
    );
    assert_eq!(store.get_home_stop(&hid("h-g2")).await.unwrap(), None);

    // A switch onto a sandbox that is gone fails and keeps the move.
    store
        .set_workspace_home(&x, &home("h-x1", Some("mac-1"), false))
        .await
        .unwrap();
    let onto_gone = moving("ws-x", "h-x1", home("h-x2", None, true));
    assert!(store.begin_home_move(&onto_gone).await.unwrap());
    assert!(store
        .set_home_move_machine(&x, &hid("h-x2"), "sb-missing")
        .await
        .unwrap());
    assert!(store
        .complete_home_move(&x, &hid("h-x2"), &person("owner"), T2)
        .await
        .is_err());
    assert!(store.get_home_move(&x).await.unwrap().is_some());
    assert_eq!(store.get_home_stop(&hid("h-x1")).await.unwrap(), None);

    if tenant == "decoy" {
        begin_move_to_mac(&store).await;
        store
            .set_workspace_home(&g, &home("h-g1", Some("sb-gone"), true))
            .await
            .unwrap();
    }
});

/// Removing a machine as an orchestrator detaches its homes in the removal
/// itself: an attached home at once, and a move away from it gets
/// `detached` as the state its rollback restores. A restart or a store
/// error between steps can no longer leave a home attached to a machine
/// the backend forgot (nodes.md "An orchestrator leaves its backends").
parity_test!(
    an_orchestrator_removal_detaches_its_homes,
    |store, tenant| {
        for id in ["ws-a", "ws-m", "ws-o"] {
            workspace(&store, id).await;
        }
        for id in ["mac-1", "mac-3"] {
            store
                .save_exposed_machine(&machine(tenant, id, None))
                .await
                .unwrap();
        }
        let (a, m, o) = (wid("ws-a"), wid("ws-m"), wid("ws-o"));
        store
            .set_workspace_home(&a, &home("h-a1", Some("mac-1"), false))
            .await
            .unwrap();
        store
            .set_workspace_home(&m, &home("h-m1", Some("mac-1"), false))
            .await
            .unwrap();
        assert!(store
            .begin_home_move(&moving("ws-m", "h-m1", home("h-m2", Some("mac-2"), false)))
            .await
            .unwrap());
        store
            .set_workspace_home(&o, &home("h-o1", Some("mac-3"), false))
            .await
            .unwrap();

        assert_eq!(
            store
                .remove_orchestrator_machine("mac-1", T2)
                .await
                .unwrap(),
            Some(ExecutorCredentialId::from("cred-mac-1"))
        );
        assert_eq!(store.get_exposed_machine("mac-1").await.unwrap(), None);
        let attached = store.get_workspace_home(&a).await.unwrap().unwrap();
        assert_eq!(
            (attached.state, attached.since.as_str()),
            (HomeState::Detached, T2),
            "an attached home on the removed machine is detached at once"
        );
        assert_eq!(
            store.get_workspace_home(&m).await.unwrap().unwrap().state,
            HomeState::Moving,
            "a moving home stays moving"
        );
        assert_eq!(
            store.get_workspace_home(&o).await.unwrap().unwrap().state,
            HomeState::Attached,
            "another machine's home is untouched"
        );

        // The move fails after the removal: its rollback restores detached.
        assert!(store.abort_home_move(&m, &hid("h-m2"), T2).await.unwrap());
        let back = store.get_workspace_home(&m).await.unwrap().unwrap();
        assert_eq!(
            (back.home_id, back.state, back.since.as_str()),
            (hid("h-m1"), HomeState::Detached, T2)
        );
        assert_eq!(
            store
                .remove_orchestrator_machine("mac-1", T2)
                .await
                .unwrap(),
            None,
            "an unknown machine is a no-op"
        );

        if tenant == "decoy" {
            store
                .save_exposed_machine(&machine(tenant, "mac-1", None))
                .await
                .unwrap();
            store
                .set_workspace_home(&a, &home("h-a1", Some("mac-1"), false))
                .await
                .unwrap();
            store
                .set_workspace_home(&m, &home("h-m1", Some("mac-1"), false))
                .await
                .unwrap();
            assert!(store
                .begin_home_move(&moving("ws-m", "h-m1", home("h-m2", Some("mac-2"), false)))
                .await
                .unwrap());
        }
    }
);
