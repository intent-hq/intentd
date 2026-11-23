//! Two tenants in one process, each with its own [`TenantRuntime`] and
//! self-fence: each runs its own restores, sweeps and loops, and firing one
//! tenant's fence stops only that tenant's connections, links and tasks.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use intent_app::remote::RemoteServicesExt as _;
use intent_app::tenant::{self, Admission, TenantRuntime};
use intent_core::tenant_fence::{FenceStep, SelfFenceReport, TenantFence};
use intent_services::{EventBus, Services};
use intent_store::Store;

struct Open;

impl Admission for Open {
    fn allowed(&self) -> bool {
        true
    }
}

struct Tenant {
    _dir: tempfile::TempDir,
    services: Services,
    fence: TenantFence,
    runtime: TenantRuntime,
    report: tokio::task::JoinHandle<SelfFenceReport>,
    /// How many of this tenant's client connections were closed.
    closed: Arc<AtomicUsize>,
}

async fn tenant() -> Tenant {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("svc.db")).await.unwrap();
    let bus = EventBus::new(store.clone());
    let services = intent_app::compose(
        Services::new_isolated(store)
            .with_event_bus(bus)
            .with_workspaces_root(dir.path().join("workspaces"))
            .with_assets_root(dir.path().join("assets")),
    );
    let fence = TenantFence::new();
    let mut runner = intent_services::self_fence::self_fence_with(fence.clone(), services.clone());
    let runtime = TenantRuntime::start(&services, dir.path(), &Open, &Open, &mut runner).await;
    let closed = Arc::new(AtomicUsize::new(0));
    for _ in 0..2 {
        let closed = closed.clone();
        runtime
            .connections()
            .register(move || {
                closed.fetch_add(1, Ordering::SeqCst);
            })
            .expect("an open tenant accepts connections");
    }
    let report = runner.spawn();
    Tenant {
        _dir: dir,
        services,
        fence,
        runtime,
        report,
        closed,
    }
}

async fn settled() {
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }
}

fn wait() -> Duration {
    intentd_test_support::test_timeout(Duration::from_secs(10))
}

#[tokio::test]
async fn each_tenant_runs_its_own_loops_and_connections() {
    let a = tenant().await;
    let b = tenant().await;
    settled().await;

    assert_eq!(a.runtime.loops_running(), tenant::TENANT_LOOPS);
    assert_eq!(b.runtime.loops_running(), tenant::TENANT_LOOPS);
    assert_eq!(a.runtime.connections().len(), 2);
    assert_eq!(b.runtime.connections().len(), 2);
    assert_ne!(
        a.services.remote_feature().unwrap().head_session(),
        b.services.remote_feature().unwrap().head_session(),
    );
}

#[tokio::test]
async fn fencing_one_tenant_stops_only_its_work() {
    let a = tenant().await;
    let b = tenant().await;
    settled().await;

    a.fence.fire("lease lost");
    let report = tokio::time::timeout(wait(), a.report)
        .await
        .expect("the fence finishes")
        .unwrap();

    assert!(report.overran.is_empty(), "{:?}", report.overran);
    for step in [
        FenceStep::ClientConnections,
        FenceStep::MachineLinks,
        FenceStep::Tasks,
        FenceStep::McpServers,
        FenceStep::WritesSettled,
    ] {
        assert!(report.finished(step).is_some(), "{step:?} ran");
    }
    assert_eq!(a.closed.load(Ordering::SeqCst), 2, "A's connections closed");
    assert!(a.runtime.connections().is_closed());
    assert_eq!(a.runtime.tasks_running(), 0, "A's tasks are cancelled");
    assert!(a.services.mcp_hub().background_stopping());
    assert!(
        a.services.remote_feature().unwrap().links_closed(),
        "A's machine links are closed"
    );
    let late = Arc::new(AtomicUsize::new(0));
    let counted = late.clone();
    assert!(a
        .runtime
        .connections()
        .register(move || {
            counted.fetch_add(1, Ordering::SeqCst);
        })
        .is_none());
    assert_eq!(
        late.load(Ordering::SeqCst),
        1,
        "a late connection is closed at once"
    );

    assert!(!b.fence.is_fenced());
    assert_eq!(
        b.closed.load(Ordering::SeqCst),
        0,
        "B's connections stay up"
    );
    assert_eq!(b.runtime.connections().len(), 2);
    assert_eq!(
        b.runtime.loops_running(),
        tenant::TENANT_LOOPS,
        "B's loops keep running"
    );
    assert!(!b.services.mcp_hub().background_stopping());
    assert!(!b.services.remote_feature().unwrap().links_closed());
}
