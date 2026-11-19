use super::*;
use intent_core::machines::{
    CloudMachine, CloudMachineSize, CloudSandboxState, CloudStartedBy, ExposedMachine,
    RemoteMachineState,
};
use intent_core::{now_iso, ExecutorCredentialId, Workspace};

async fn open() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("move.db")).await.unwrap();
    store
        .insert_workspace(&Workspace {
            id: WorkspaceId::from("ws-1"),
            title: "ws-1".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    (dir, store)
}

fn person(id: &str) -> CloudStartedBy {
    CloudStartedBy {
        principal_id: id.into(),
        display_name: None,
    }
}

fn sandbox(id: &str, started_by: &str) -> ExposedMachine {
    ExposedMachine {
        id: id.into(),
        name: "E2B · Agent".into(),
        hosts: vec!["127.0.0.1".into()],
        port: 7443,
        tc_address: None,
        fingerprint: "ab".repeat(32),
        credential_id: ExecutorCredentialId::from(format!("cred-{id}")),
        platform: None,
        capabilities: None,
        observed_at: None,
        state: RemoteMachineState::Disconnected,
        added_at: now_iso(),
        last_connected_at: None,
        node_protocol: Some(19),
        cloud: Some(CloudMachine {
            provider: "e2b".into(),
            cloud_machine_id: "e2b".into(),
            sandbox_id: format!("sb-{id}"),
            volume_id: None,
            state: CloudSandboxState::Running,
            template: "t".into(),
            size: CloudMachineSize::S4x8,
            started_by: person(started_by),
            agent_id: "agent-1".into(),
            workspace_id: "ws-1".into(),
            started_at: now_iso(),
            running_seconds: 0,
            running_since: None,
            paused_at: None,
        }),
    }
}

fn home(id: &str, node: Option<&str>, cloud: bool) -> WorkspaceHome {
    WorkspaceHome {
        home_id: HomeId::from(id),
        node_id: node.map(str::to_owned),
        cloud_machine_id: cloud.then(|| "e2b".to_owned()),
        name: if cloud { "E2B" } else { "Mac" }.into(),
        state: HomeState::Attached,
        since: now_iso(),
        last_backup_at: Some("2026-11-15T10:00:00Z".into()),
        backup_warning: false,
        setup: None,
    }
}

fn moving_to(target: WorkspaceHome) -> HomeMove {
    HomeMove {
        workspace_id: WorkspaceId::from("ws-1"),
        target,
        from_home_id: HomeId::from("h-old"),
        from_state: HomeState::Attached,
        from_since: now_iso(),
        pushed: false,
        started_at: now_iso(),
    }
}

/// A move onto an agent's sandbox makes the workspace owner the sandbox's
/// `startedBy` in the switch itself, so a restart right after the switch
/// cannot leave the agent's starter in control of the home (nodes.md
/// "Cloud homes" 2).
#[tokio::test]
async fn the_switch_onto_a_sandbox_makes_the_owner_its_starter() {
    let (_dir, store) = open().await;
    let ws = WorkspaceId::from("ws-1");
    store
        .set_workspace_home(&ws, &home("h-old", Some("m-mac"), false))
        .await
        .unwrap();
    store
        .save_exposed_machine(&sandbox("m-sb", "starter"))
        .await
        .unwrap();
    let home_move = moving_to(home("h-new", Some("m-sb"), true));
    assert!(store.begin_home_move(&home_move).await.unwrap());

    let switch = store
        .complete_home_move(&ws, &HomeId::from("h-new"), &person("owner"), &now_iso())
        .await
        .unwrap()
        .expect("the switch");
    assert_eq!(
        switch.stop.map(|stop| stop.node_id).as_deref(),
        Some("m-mac")
    );
    let row = store.get_exposed_machine("m-sb").await.unwrap().unwrap();
    assert_eq!(row.cloud.unwrap().started_by, person("owner"));
}

/// Clients see a move to a cloud home `starting`, under the new home's
/// name, once the old home's last backup landed, while the store keeps the
/// old home `moving`; a move to a machine stays `moving`. The move's new
/// sandbox is recorded once.
#[tokio::test]
async fn a_move_to_a_cloud_home_shows_starting_after_its_last_backup() {
    let (_dir, store) = open().await;
    let ws = WorkspaceId::from("ws-1");
    store
        .set_workspace_home(&ws, &home("h-old", Some("m-mac"), false))
        .await
        .unwrap();
    assert!(store
        .begin_home_move(&moving_to(home("h-new", None, true)))
        .await
        .unwrap());
    let shown = store.get_shown_workspace_home(&ws).await.unwrap().unwrap();
    assert_eq!(shown.state, HomeState::Moving);
    assert_eq!(shown.home_id, HomeId::from("h-old"));

    assert!(store
        .mark_home_move_pushed(&ws, &HomeId::from("h-new"))
        .await
        .unwrap());
    let shown = store.get_shown_workspace_home(&ws).await.unwrap().unwrap();
    assert_eq!(shown.state, HomeState::Starting);
    assert_eq!(shown.home_id, HomeId::from("h-new"));
    assert_eq!(shown.name, "E2B");
    assert_eq!(
        shown.last_backup_at.as_deref(),
        Some("2026-11-15T10:00:00Z")
    );
    let listed = store.list_shown_workspace_homes().await.unwrap();
    assert_eq!(listed[&ws], shown);
    let stored = store.get_workspace_home(&ws).await.unwrap().unwrap();
    assert_eq!(stored.state, HomeState::Moving);

    assert!(store
        .set_home_move_machine(&ws, &HomeId::from("h-new"), "m-sb")
        .await
        .unwrap());
    assert!(!store
        .set_home_move_machine(&ws, &HomeId::from("h-new"), "m-other")
        .await
        .unwrap());
    assert_eq!(
        store
            .get_shown_workspace_home(&ws)
            .await
            .unwrap()
            .unwrap()
            .node_id
            .as_deref(),
        Some("m-sb")
    );

    assert!(store
        .abort_home_move(&ws, &HomeId::from("h-new"), &now_iso())
        .await
        .unwrap());
    let machine_move = HomeMove {
        pushed: true,
        ..moving_to(home("h-mac2", Some("m-mac2"), false))
    };
    assert!(store.begin_home_move(&machine_move).await.unwrap());
    let shown = store.get_shown_workspace_home(&ws).await.unwrap().unwrap();
    assert_eq!(shown.state, HomeState::Moving);
}

/// A move whose new home's sandbox went (lost, killed or deleted) cannot
/// switch onto it; its rollback owes the gone machine no stop.
#[tokio::test]
async fn a_move_onto_a_gone_sandbox_rolls_back_without_a_stop() {
    let (_dir, store) = open().await;
    let ws = WorkspaceId::from("ws-1");
    store
        .set_workspace_home(&ws, &home("h-old", Some("m-mac"), false))
        .await
        .unwrap();
    assert!(store
        .begin_home_move(&moving_to(home("h-new", Some("m-gone"), true)))
        .await
        .unwrap());
    assert!(store
        .complete_home_move(&ws, &HomeId::from("h-new"), &person("owner"), &now_iso())
        .await
        .is_err());
    assert!(store
        .abort_home_move(&ws, &HomeId::from("h-new"), &now_iso())
        .await
        .unwrap());
    assert!(store.list_home_stops("m-gone").await.unwrap().is_empty());
    let back = store.get_workspace_home(&ws).await.unwrap().unwrap();
    assert_eq!(back.home_id, HomeId::from("h-old"));
    assert_eq!(back.state, HomeState::Attached);
}

/// A restart between a move's sandbox row and its record on the move
/// leaves a home-only sandbox nothing holds: the resumed move finds it (so
/// it takes it instead of starting another), and once the move records it,
/// or for another workspace, entry or starter, it is not found.
#[tokio::test]
async fn a_home_only_sandbox_a_cut_start_left_is_found_for_the_resumed_move() {
    let (_dir, store) = open().await;
    let ws = WorkspaceId::from("ws-1");
    store
        .set_workspace_home(&ws, &home("h-old", Some("m-mac"), false))
        .await
        .unwrap();
    let mut left = sandbox("m-left", "owner");
    left.cloud.as_mut().unwrap().agent_id = "home-ws-1".into();
    store.save_exposed_machine(&left).await.unwrap();
    let mut agents = sandbox("m-agent", "owner");
    agents.cloud.as_mut().unwrap().agent_id = "agent-1".into();
    store.save_exposed_machine(&agents).await.unwrap();
    assert!(store
        .begin_home_move(&HomeMove {
            pushed: true,
            ..moving_to(home("h-new", None, true))
        })
        .await
        .unwrap());

    let found = store
        .unclaimed_home_sandboxes(&ws, "e2b", "home-ws-1")
        .await
        .unwrap();
    assert_eq!(
        found.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(),
        ["m-left"]
    );
    assert!(store
        .unclaimed_home_sandboxes(&ws, "other-entry", "home-ws-1")
        .await
        .unwrap()
        .is_empty());

    assert!(store
        .set_home_move_machine(&ws, &HomeId::from("h-new"), "m-left")
        .await
        .unwrap());
    assert!(store
        .unclaimed_home_sandboxes(&ws, "e2b", "home-ws-1")
        .await
        .unwrap()
        .is_empty());
}
