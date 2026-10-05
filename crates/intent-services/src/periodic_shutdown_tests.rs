use std::time::Duration;

use intent_core::WorkspaceId;
use intent_store::Store;
use tokio::sync::oneshot;

use crate::{events::EventBus, Services};

pub(crate) struct CommitHold {
    kind: &'static str,
    entered: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
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
            let _ = hold.entered.send(());
            let _ = hold.release.await;
        }
    }
}

pub(crate) fn hold(
    svc: &Services,
    kind: &'static str,
) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
    let (entered, rx) = oneshot::channel();
    let (tx, release) = oneshot::channel();
    *svc.periodic_commit_hold.lock().unwrap() = Some(CommitHold {
        kind,
        entered,
        release,
    });
    (rx, tx)
}

pub(crate) async fn entered(rx: oneshot::Receiver<()>) {
    tokio::time::timeout(Duration::from_secs(10), rx)
        .await
        .unwrap()
        .unwrap();
}

pub(crate) async fn drain_held(svc: &Services, release: oneshot::Sender<()>) {
    let (tx, rx) = oneshot::channel();
    *svc.secrets.writer_drain_pending.lock().unwrap() = Some(tx);
    let service = svc.clone();
    let drain = tokio::spawn(async move { service.shutdown_store_writers().await });
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), rx)
            .await
            .expect("finite drain must retain committed publication")
            .unwrap(),
        "store-tasks"
    );
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(10), drain)
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
    let (rx, release) = hold(&svc, "token");
    let task = svc.spawn_token_usage_scan_loop(Duration::from_millis(1));
    entered(rx).await;
    assert!(store
        .get_workspace(&ws)
        .await
        .unwrap()
        .token_usage
        .is_some());
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    drain_held(&svc, release).await;
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
