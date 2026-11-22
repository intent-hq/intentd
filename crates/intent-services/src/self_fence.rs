//! The services side of a tenant's self-fence (see
//! [`intent_core::tenant_fence`]): stop the tenant's MCP servers and release
//! the runner's [`Services`] handle when its fence fires. Other clones of
//! the `Services` live on with their owners (listeners, loops), which the
//! composition stops through the runner's other steps.
//!
//! [`tenant_self_fence`] is the library constructor the per-tenant
//! composition calls. It returns the runner with those two consumers
//! registered; the composition then adds the tenant's client connections
//! (`SelfFence::on_client_connections`), machine links
//! (`SelfFence::on_machine_links`) and `startup_work` / orchestration tasks
//! (`SelfFence::adopt_task`), and spawns it. `SQLite` daemons have no
//! fence, so they never build one.

use intent_core::tenant_fence::{SelfFence, TenantFence};

use crate::Services;

/// The self-fence of the tenant `services` serves: on the store's current
/// claim's fence, it stops the services' MCP servers (background starts and
/// health ticks first, then every running server) and drops this handle
/// to `services`; the runner keeps it alive until then.
/// `None` when the store has no fence (`SQLite`, or a Postgres store whose
/// process never claimed the tenant).
#[must_use]
pub fn tenant_self_fence(services: Services) -> Option<SelfFence> {
    let fence = services.store().tenant_fence()?;
    Some(self_fence_with(fence, services))
}

/// [`tenant_self_fence`] on an explicit fence.
#[must_use]
pub fn self_fence_with(fence: TenantFence, services: Services) -> SelfFence {
    let mut runner = SelfFence::new(fence);
    let mcp = services.mcp_hub();
    runner.on_mcp_servers(move || async move {
        mcp.begin_background_shutdown();
        mcp.shutdown().await;
    });
    runner.own(services);
    runner
}

#[cfg(test)]
mod tests {
    use super::{self_fence_with, tenant_self_fence};
    use crate::Services;
    use intent_core::tenant_fence::{FenceStep, TenantFence};
    use std::time::Duration;

    async fn services() -> (tempfile::TempDir, Services) {
        let dir = tempfile::tempdir().unwrap();
        let store = intent_store::Store::open(&dir.path().join("test.db"))
            .await
            .unwrap();
        (dir, Services::new(store))
    }

    #[tokio::test]
    async fn a_sqlite_daemon_has_no_self_fence() {
        let (_dir, services) = services().await;
        assert!(tenant_self_fence(services).is_none());
    }

    #[tokio::test]
    async fn the_fence_stops_mcp_and_drops_the_services() {
        let (_dir, services) = services().await;
        let store = services.store().clone();
        let mcp = services.mcp_hub();
        let fence = TenantFence::new();
        let running = self_fence_with(fence.clone(), services).spawn();
        assert!(!mcp.background_stopping());
        fence.fire("lease lost");
        let report = tokio::time::timeout(
            intentd_test_support::test_timeout(Duration::from_secs(10)),
            running,
        )
        .await
        .expect("the self-fence finishes")
        .unwrap();
        assert!(mcp.background_stopping(), "MCP background work is stopped");
        assert!(report.finished(FenceStep::McpServers).is_some());
        assert!(report.finished(FenceStep::Services).is_some());
        assert!(report.overran.is_empty());
        drop(store);
    }
}
