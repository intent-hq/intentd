//! [`intent_app::tenant`]: the startup sweeps and background loops one
//! backend runs, started under its admissions and stopped as a set.

use intent_app::tenant::{self, Admission};
use intent_services::{EventBus, Services};
use intent_store::Store;

/// An admission fixed at construction.
struct Fixed(bool);

impl Admission for Fixed {
    fn allowed(&self) -> bool {
        self.0
    }
}

async fn composed(dir: &tempfile::TempDir) -> Services {
    let store = Store::open(&dir.path().join("svc.db"))
        .await
        .expect("open store");
    // The daemon's backend has an event bus: without one, the event-driven
    // loops end at once.
    let bus = EventBus::new(store.clone());
    intent_app::compose(
        Services::new_isolated(store)
            .with_event_bus(bus)
            .with_workspaces_root(dir.path().join("workspaces"))
            .with_assets_root(dir.path().join("assets")),
    )
}

async fn settled() {
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn refused_admissions_start_no_loop() {
    let dir = tempfile::tempdir().unwrap();
    let services = composed(&dir).await;
    tenant::restore_registries(&services, &Fixed(false), &Fixed(false)).await;
    let mut background =
        tenant::start_background(&services, dir.path(), &Fixed(false), &Fixed(false));
    settled().await;
    assert_eq!(background.loops_running(), 0);
    background.stop_loops().await;
    background
        .attachment_retention
        .await
        .expect("a refused sweep settles");
}

#[tokio::test]
async fn admitted_loops_run_until_stopped() {
    let dir = tempfile::tempdir().unwrap();
    let services = composed(&dir).await;
    tenant::restore_registries(&services, &Fixed(true), &Fixed(true)).await;
    let mut background =
        tenant::start_background(&services, dir.path(), &Fixed(true), &Fixed(true));
    settled().await;
    assert_eq!(background.loops_running(), tenant::LOOPS);
    background.stop_loops().await;
    assert_eq!(background.loops_running(), 0);
}

#[tokio::test]
async fn node_mode_runs_only_the_backends_own_upkeep() {
    let dir = tempfile::tempdir().unwrap();
    let services = composed(&dir).await;
    let mut background =
        tenant::start_background(&services, dir.path(), &Fixed(true), &Fixed(false));
    settled().await;
    assert_eq!(background.loops_running(), tenant::UPKEEP_LOOPS);
    background.stop_loops().await;
    assert_eq!(background.loops_running(), 0);
}
