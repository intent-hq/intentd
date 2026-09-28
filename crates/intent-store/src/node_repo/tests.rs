use super::*;
use intent_core::nodes::{LeaseFenceKind, NodeArch, NodeCapacity, NodeOs, NodePath};
use std::borrow::Cow;

fn records() -> (NodeRecord, NodeLease) {
    let node = NodeRecord {
        id: "node-test".into(),
        head_id: "head-test".into(),
        node_identity: "identity-test".into(),
        name: "Build host".into(),
        kind: NodeKind::Static,
        os: NodeOs::Linux,
        arch: NodeArch::X86_64,
        state: NodeState::Ready,
        draining: false,
        capacity: NodeCapacity {
            max_agents: 4,
            reserved_agents: 0,
            memory_budget_bytes: 1 << 30,
            used_memory_bytes: 0,
            exclusive_reserved: false,
        },
        version: "test".into(),
        endpoint: Some("wss://node.example:443/node".into()),
        certificate_sha256: Some("a".repeat(64)),
        last_seen_at: None,
    };
    let lease = NodeLease {
        owner: LeaseOwner {
            head_id: node.head_id.clone(),
            node_id: node.id.clone(),
            node_identity: node.node_identity.clone(),
            lease_id: "lease-test".into(),
            incarnation: uuid::Uuid::new_v4().to_string(),
        },
        state: LeaseState::Acquiring,
        created_at: now_iso(),
        last_seen_at: None,
        ack_seq: NodeCounter(0),
        link_generation: NodeCounter(0),
        release_requested: false,
        fence: None,
    };
    (node, lease)
}
async fn seed_agent(store: &Store, id: &str, ws: &str) {
    sqlx::query("INSERT OR IGNORE INTO workspace (id, title, branch, created_at, updated_at) VALUES (?, 'test', 'test', ?, ?)")
        .bind(ws).bind(now_iso()).bind(now_iso()).execute(store.write_pool()).await.unwrap();
    sqlx::query("INSERT INTO agent_session (id, workspace_id, name, status, created_at, updated_at) VALUES (?, ?, 'test', 'idle', ?, ?)")
        .bind(id).bind(ws).bind(now_iso()).bind(now_iso()).execute(store.write_pool()).await.unwrap();
}
async fn ready(store: &Store) -> (NodeRecord, NodeLease, NodeAssignment) {
    let (node, initial) = records();
    store.enroll_node(&node, &initial).await.unwrap();
    let lease = store
        .transition_node_lease(&initial.owner, LeaseState::Ready)
        .await
        .unwrap();
    seed_agent(store, "agent", "workspace").await;
    let template = NodeAssignment {
        agent_id: "agent".into(),
        workspace_id: "workspace".into(),
        owner: lease.owner.clone(),
        run_id: uuid::Uuid::new_v4().to_string(),
        assignment_epoch: NodeCounter(999),
        repo_keys: vec!["repo".into()],
        node_path: Some(NodePath("/node-only/checkout".into())),
        active: true,
        tombstoned: false,
        inherited_checkpoint_id: None,
        merge_target_agent_id: None,
    };
    let assignment = store.assign_node_run(&template).await.unwrap();
    (node, lease, assignment)
}
fn checkpoint(assignment: &NodeAssignment, revision: u64) -> NodeCheckpoint {
    NodeCheckpoint {
        id: uuid::Uuid::new_v4().to_string(),
        agent_id: assignment.agent_id.clone(),
        workspace_id: assignment.workspace_id.clone(),
        owner: assignment.owner.clone(),
        run_id: assignment.run_id.clone(),
        assignment_epoch: assignment.assignment_epoch,
        capture_revision: NodeCounter(revision),
        journal_seq: NodeCounter(0),
        manifest_sha256: "b".repeat(64),
        captured_at: now_iso(),
        committed_at: String::new(),
    }
}
fn fence() -> LeaseFence {
    LeaseFence {
        kind: LeaseFenceKind::ProcessesStopped,
        recorded_at: now_iso(),
    }
}

#[tokio::test]
async fn release_is_durable_idempotent_and_does_not_free_unconfirmed_hosts() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("nodes.db");
    let store = Store::open(&path).await.unwrap();
    let (node, lease, assignment) = ready(&store).await;
    assert_eq!(assignment.assignment_epoch, NodeCounter(1));
    assert_eq!(
        store.assign_node_run(&assignment).await.unwrap(),
        assignment
    );
    store
        .transition_node_lease(&lease.owner, LeaseState::Busy)
        .await
        .unwrap();
    store
        .transition_node_lease(&lease.owner, LeaseState::Idle)
        .await
        .unwrap();
    assert!(store
        .finish_node_lease(&lease.owner, LeaseState::Released, fence())
        .await
        .is_err());
    let requested = store
        .request_node_lease_release(&lease.owner)
        .await
        .unwrap();
    assert!(requested.release_requested);
    assert_eq!(requested.state, LeaseState::Idle);
    assert_eq!(
        store
            .request_node_lease_release(&lease.owner)
            .await
            .unwrap(),
        requested
    );
    assert!(
        store
            .get_node(&node.id, &node.head_id)
            .await
            .unwrap()
            .draining
    );
    assert!(
        store
            .get_node_assignment(&assignment.agent_id, &assignment.workspace_id)
            .await
            .unwrap()
            .active
    );
    assert!(store
        .authorize_node_repo(&assignment, "repo")
        .await
        .is_err());
    assert!(store.assign_node_run(&assignment).await.is_err());
    store.close().await;
    let store = Store::open(&path).await.unwrap();
    assert_eq!(store.get_node_lease(&lease.owner).await.unwrap(), requested);
    let released = store
        .finish_node_lease(&lease.owner, LeaseState::Released, fence())
        .await
        .unwrap();
    assert_eq!(released.state, LeaseState::Released);
    assert!(released.fence.is_some());
    assert_eq!(
        store
            .finish_node_lease(&lease.owner, LeaseState::Released, fence())
            .await
            .unwrap(),
        released
    );
    assert_eq!(
        store
            .request_node_lease_release(&lease.owner)
            .await
            .unwrap(),
        released
    );
    assert!(
        !store
            .get_node_assignment(&assignment.agent_id, &assignment.workspace_id)
            .await
            .unwrap()
            .active
    );
    assert!(store
        .transition_node_lease(&lease.owner, LeaseState::Ready)
        .await
        .is_err());
    assert!(store.next_node_link_generation(&lease.owner).await.is_err());
    assert!(store
        .get_node(&node.id, &node.head_id)
        .await
        .unwrap()
        .endpoint
        .is_some());
}

#[tokio::test]
async fn restart_preserves_identity_generation_ack_assignment_and_capture_counter() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("nodes.db");
    let store = Store::open(&path).await.unwrap();
    let (node, lease, assignment) = ready(&store).await;
    assert_eq!(
        store.next_node_link_generation(&lease.owner).await.unwrap(),
        NodeCounter(1)
    );
    assert_eq!(
        store.next_node_capture_revision(&assignment).await.unwrap(),
        NodeCounter(1)
    );
    // Simulate the watermark column committed by the future journal transaction.
    // This test does not claim transcript ingestion/contiguous ack behavior.
    sqlx::query("UPDATE node_lease SET ack_seq = ? WHERE id = ?")
        .bind(u64::MAX.to_string())
        .bind(&lease.owner.lease_id)
        .execute(store.write_pool())
        .await
        .unwrap();
    store.close().await;
    let store = Store::open(&path).await.unwrap();
    assert_eq!(store.get_node(&node.id, &node.head_id).await.unwrap(), node);
    assert_eq!(
        store.list_owned_nodes(&node.head_id).await.unwrap(),
        vec![node.clone()]
    );
    assert!(store
        .list_owned_nodes("other-head")
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        store.list_owned_node_leases(&node.head_id).await.unwrap()[0].ack_seq,
        NodeCounter(u64::MAX)
    );
    assert!(store
        .list_owned_node_leases("other-head")
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        store
            .get_node_assignment(&assignment.agent_id, &assignment.workspace_id)
            .await
            .unwrap(),
        assignment
    );
    assert_eq!(
        store.get_node_lease(&lease.owner).await.unwrap().ack_seq,
        NodeCounter(u64::MAX)
    );
    assert_eq!(
        store.next_node_link_generation(&lease.owner).await.unwrap(),
        NodeCounter(2)
    );
    assert_eq!(
        store.next_node_capture_revision(&assignment).await.unwrap(),
        NodeCounter(2)
    );
    assert_eq!(
        store.get_node_lease(&lease.owner).await.unwrap().ack_seq,
        NodeCounter(u64::MAX)
    );
}

#[tokio::test]
async fn owner_scope_and_tombstones_fail_closed() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("nodes.db")).await.unwrap();
    let (node, lease, assignment) = ready(&store).await;
    assert!(store.get_node(&node.id, "other-head").await.is_err());
    for field in 0..5 {
        let mut wrong = lease.owner.clone();
        match field {
            0 => wrong.head_id = "other".into(),
            1 => wrong.node_id = "other".into(),
            2 => wrong.node_identity = "other".into(),
            3 => wrong.incarnation = uuid::Uuid::new_v4().to_string(),
            _ => wrong.lease_id = "other".into(),
        }
        assert!(store.get_node_lease(&wrong).await.is_err());
        assert!(store.request_node_lease_release(&wrong).await.is_err());
    }
    store
        .authorize_node_repo(&assignment, "repo")
        .await
        .unwrap();
    assert!(store
        .authorize_node_repo(&assignment, "sibling-repo")
        .await
        .is_err());
    let mut wrong = assignment.clone();
    wrong.workspace_id = "sibling-workspace".into();
    assert!(store.assign_node_run(&wrong).await.is_err());
    assert!(store.authorize_node_repo(&wrong, "repo").await.is_err());
    wrong = assignment.clone();
    wrong.run_id = uuid::Uuid::new_v4().to_string();
    assert!(store.assign_node_run(&wrong).await.is_err());
    assert!(store.stop_node_assignment(&wrong, false).await.is_err());
    store.stop_node_assignment(&assignment, true).await.unwrap();
    assert!(store
        .authorize_node_repo(&assignment, "repo")
        .await
        .is_err());
    assert!(store.assign_node_run(&wrong).await.is_err());
    assert!(store
        .commit_node_checkpoint(&checkpoint(&assignment, 1))
        .await
        .is_err());
}

#[tokio::test]
async fn checkpoints_order_numerically_and_retries_keep_original_receipts() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("nodes.db");
    let store = Store::open(&path).await.unwrap();
    let (_, _, assignment) = ready(&store).await;
    let ten = checkpoint(&assignment, 10);
    let nine = checkpoint(&assignment, 9);
    let first = store.commit_node_checkpoint(&ten).await.unwrap();
    assert_eq!(first.outcome, CheckpointOutcome::Advanced);
    assert_eq!(
        store.commit_node_checkpoint(&nine).await.unwrap().outcome,
        CheckpointOutcome::Historical
    );
    let newer = checkpoint(&assignment, 11);
    store.commit_node_checkpoint(&newer).await.unwrap();
    assert_eq!(store.commit_node_checkpoint(&ten).await.unwrap(), first);
    let mut conflict = ten.clone();
    conflict.id = uuid::Uuid::new_v4().to_string();
    assert!(store.commit_node_checkpoint(&conflict).await.is_err());
    conflict = ten.clone();
    conflict.manifest_sha256 = "c".repeat(64);
    assert!(store.commit_node_checkpoint(&conflict).await.is_err());
    let mut unacked = checkpoint(&assignment, 12);
    unacked.journal_seq = NodeCounter(1);
    assert!(store.commit_node_checkpoint(&unacked).await.is_err());
    store.close().await;
    let store = Store::open(&path).await.unwrap();
    assert_eq!(
        store
            .current_node_checkpoint(&assignment.agent_id, &assignment.workspace_id)
            .await
            .unwrap()
            .unwrap()
            .id,
        newer.id
    );
    assert_eq!(store.commit_node_checkpoint(&ten).await.unwrap(), first);
    let mut new_run = assignment.clone();
    new_run.run_id = uuid::Uuid::new_v4().to_string();
    store
        .stop_node_assignment(&assignment, false)
        .await
        .unwrap();
    let new_run = store.assign_node_run(&new_run).await.unwrap();
    assert_eq!(new_run.assignment_epoch, NodeCounter(2));
    assert!(store
        .commit_node_checkpoint(&checkpoint(&assignment, 13))
        .await
        .is_err());
    assert_eq!(
        store
            .current_node_checkpoint(&assignment.agent_id, &assignment.workspace_id)
            .await
            .unwrap()
            .unwrap()
            .id,
        newer.id
    );
    let epoch_two = checkpoint(&new_run, 1);
    assert_eq!(
        store
            .commit_node_checkpoint(&epoch_two)
            .await
            .unwrap()
            .outcome,
        CheckpointOutcome::Advanced
    );
    assert_eq!(
        store.next_node_capture_revision(&new_run).await.unwrap(),
        NodeCounter(1)
    );
}

#[tokio::test]
async fn concurrent_checkpoint_commits_cannot_regress_pointer() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("nodes.db")).await.unwrap();
    let (_, _, assignment) = ready(&store).await;
    let nine = checkpoint(&assignment, 9);
    let ten = checkpoint(&assignment, 10);
    let (a, b) = tokio::join!(
        store.commit_node_checkpoint(&ten),
        store.commit_node_checkpoint(&nine)
    );
    a.unwrap();
    b.unwrap();
    assert_eq!(
        store
            .current_node_checkpoint(&assignment.agent_id, &assignment.workspace_id)
            .await
            .unwrap()
            .unwrap()
            .id,
        ten.id
    );
}

#[tokio::test]
async fn local_registration_has_no_remote_credentials_and_cannot_be_released() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("nodes.db")).await.unwrap();
    let (mut node, lease) = records();
    node.kind = NodeKind::Local;
    assert!(store.enroll_node(&node, &lease).await.is_err());
    node.endpoint = None;
    node.certificate_sha256 = None;
    store.enroll_node(&node, &lease).await.unwrap();
    assert!(store
        .request_node_lease_release(&lease.owner)
        .await
        .is_err());
    assert!(store
        .finish_node_lease(&lease.owner, LeaseState::Lost, fence())
        .await
        .is_err());
}

#[tokio::test]
async fn lost_lease_requires_fence_and_preserves_recovery_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("nodes.db")).await.unwrap();
    let (_, lease, assignment) = ready(&store).await;
    let captured = checkpoint(&assignment, 1);
    store.commit_node_checkpoint(&captured).await.unwrap();
    assert!(store
        .transition_node_lease(&lease.owner, LeaseState::Lost)
        .await
        .is_err());
    store
        .finish_node_lease(
            &lease.owner,
            LeaseState::Lost,
            LeaseFence {
                kind: LeaseFenceKind::OfflineBudgetElapsed,
                recorded_at: now_iso(),
            },
        )
        .await
        .unwrap();
    assert!(
        !store
            .get_node_assignment(&assignment.agent_id, &assignment.workspace_id)
            .await
            .unwrap()
            .active
    );
    assert_eq!(
        store
            .current_node_checkpoint(&assignment.agent_id, &assignment.workspace_id)
            .await
            .unwrap()
            .unwrap()
            .id,
        captured.id
    );
}

#[tokio::test]
async fn inherited_baseline_is_persisted_and_workspace_scoped() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("nodes.db")).await.unwrap();
    let (_, _, parent) = ready(&store).await;
    let source = checkpoint(&parent, 1);
    store.commit_node_checkpoint(&source).await.unwrap();
    seed_agent(&store, "child", "workspace").await;
    let mut child = parent.clone();
    child.agent_id = "child".into();
    child.run_id = uuid::Uuid::new_v4().to_string();
    child.inherited_checkpoint_id = Some(source.id);
    assert!(store.assign_node_run(&child).await.is_err());
    child.merge_target_agent_id = Some(parent.agent_id);
    let admitted = store.assign_node_run(&child).await.unwrap();
    assert_eq!(
        store
            .get_node_assignment(&child.agent_id, &child.workspace_id)
            .await
            .unwrap(),
        admitted
    );
    seed_agent(&store, "other-child", "other-workspace").await;
    child.agent_id = "other-child".into();
    child.workspace_id = "other-workspace".into();
    assert!(store.assign_node_run(&child).await.is_err());
}

#[tokio::test]
async fn upgrades_pinned_schema_without_changing_existing_rows_or_migrations() {
    // Both recorded monorepo pin f0fcfce7b and foundation base 890b49187
    // contain published migrations through 0135. Apply that exact prefix first.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("nodes.db");
    let pool = crate::connect_write(&path).await.unwrap();
    let mut pinned = sqlx::migrate::Migrator {
        migrations: Cow::Owned(
            crate::MIGRATOR
                .iter()
                .filter(|m| m.version <= 135)
                .cloned()
                .collect(),
        ),
        ..sqlx::migrate::Migrator::DEFAULT
    };
    pinned.set_locking(false);
    pinned.run(&pool).await.unwrap();
    sqlx::query("INSERT INTO workspace (id, title, branch, created_at, updated_at) VALUES ('retained', 'retained title', 'main', 'time', 'time')").execute(&pool).await.unwrap();
    let before: Vec<(i64, Vec<u8>)> =
        sqlx::query_as("SELECT version, checksum FROM _sqlx_migrations ORDER BY version")
            .fetch_all(&pool)
            .await
            .unwrap();
    pool.close().await;
    let store = Store::open(&path).await.unwrap();
    let after: Vec<(i64, Vec<u8>)> = sqlx::query_as(
        "SELECT version, checksum FROM _sqlx_migrations WHERE version <= 135 ORDER BY version",
    )
    .fetch_all(store.read_pool())
    .await
    .unwrap();
    assert_eq!(before, after);
    assert_eq!(before.last().unwrap().0, 135);
    let title: String = sqlx::query_scalar("SELECT title FROM workspace WHERE id = 'retained'")
        .fetch_one(store.read_pool())
        .await
        .unwrap();
    assert_eq!(title, "retained title");
    assert!(store.migration_status().await.unwrap().is_current());
    ready(&store).await;
}

#[tokio::test]
async fn deleted_agent_cannot_commit_or_authorize_and_retains_tombstone() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("nodes.db")).await.unwrap();
    let (_, _, assignment) = ready(&store).await;
    sqlx::query("DELETE FROM agent_session WHERE id = ?")
        .bind(assignment.agent_id.as_str())
        .execute(store.write_pool())
        .await
        .unwrap();
    assert!(store
        .authorize_node_repo(&assignment, "repo")
        .await
        .is_err());
    assert!(store
        .commit_node_checkpoint(&checkpoint(&assignment, 1))
        .await
        .is_err());
    let retained = store
        .get_node_assignment(&assignment.agent_id, &assignment.workspace_id)
        .await
        .unwrap();
    assert!(retained.tombstoned);
    assert!(!retained.active);
}

#[tokio::test]
async fn deleting_workspace_tombstones_node_assignments_atomically() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("nodes.db")).await.unwrap();
    let (_, _, assignment) = ready(&store).await;
    sqlx::query("DELETE FROM workspace WHERE id = ?")
        .bind(assignment.workspace_id.as_str())
        .execute(store.write_pool())
        .await
        .unwrap();
    assert!(store
        .authorize_node_repo(&assignment, "repo")
        .await
        .is_err());
    let retained = store
        .get_node_assignment(&assignment.agent_id, &assignment.workspace_id)
        .await
        .unwrap();
    assert!(retained.tombstoned);
}
