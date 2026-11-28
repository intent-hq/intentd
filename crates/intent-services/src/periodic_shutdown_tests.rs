use std::time::Duration;

use intent_core::WorkspaceId;
use intent_store::Store;
use intentd_test_support::PausePoint;
use std::sync::Arc;
use tokio::sync::oneshot;

use crate::{events::EventBus, Services};

pub(crate) struct CommitHold {
    kind: &'static str,
    pause: Arc<PausePoint>,
}

impl Services {
    pub(crate) async fn hold_periodic_commit(&self, kind: &'static str) {
        let hold = {
            let mut slot = self.periodic_commit_hold.lock().unwrap();
            if slot.as_ref().is_some_and(|hold| hold.kind == kind) {
                slot.take()
            } else {
                None
            }
        };
        if let Some(hold) = hold {
            hold.pause.pause().await;
        }
    }
}

/// Hold the next periodic `kind` commit at its pause point.
pub(crate) fn hold(svc: &Services, kind: &'static str) -> Arc<PausePoint> {
    let pause = PausePoint::new(format!("periodic {kind} commit"));
    *svc.periodic_commit_hold.lock().unwrap() = Some(CommitHold {
        kind,
        pause: pause.clone(),
    });
    pause
}

pub(crate) async fn entered(commit: &PausePoint) {
    commit.wait_reached().await;
}

pub(crate) async fn drain_held(svc: &Services, commit: &PausePoint) {
    let (tx, rx) = oneshot::channel();
    *svc.secrets.writer_drain_pending.lock().unwrap() = Some(tx);
    let service = svc.clone();
    let drain = tokio::spawn(async move { service.shutdown_store_writers().await });
    assert_eq!(
        tokio::time::timeout(
            intentd_test_support::test_timeout(Duration::from_secs(5)),
            rx
        )
        .await
        .expect("finite drain must retain committed publication")
        .unwrap(),
        "store-tasks"
    );
    commit.release();
    tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(10)),
        drain,
    )
    .await
    .unwrap()
    .unwrap();
}

#[tokio::test]
async fn token_scan_committed_tally_survives_loop_abort() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.db");
    let store = Store::open(&path).await.unwrap();
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&crate::tests::workspace(&ws))
        .await
        .unwrap();
    let bus = EventBus::new(store.clone());
    let svc = Services::new_with_file_secrets(
        store.clone(),
        intent_core::FileSecretStore::with_path(dir.path().join("secrets.json")),
    )
    .with_event_bus(bus.clone());
    let commit = hold(&svc, "token");
    let task = svc.spawn_token_usage_scan_loop(Duration::from_millis(1));
    entered(&commit).await;
    assert!(store
        .get_workspace(&ws)
        .await
        .unwrap()
        .token_usage
        .is_some());
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    drain_held(&svc, &commit).await;
    bus.shutdown().await.unwrap();
    store.close().await;
    let reopened = Store::open(&path).await.unwrap();
    assert!(reopened
        .get_workspace(&ws)
        .await
        .unwrap()
        .token_usage
        .is_some());
    let events = reopened
        .query_events(&intent_store::EventQuery {
            workspace_id: Some(ws),
            event_types: vec!["workspace:tokenUsage-changed".into()],
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(events.len(), 1);
    reopened.close().await;
}
