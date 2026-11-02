//! Node mode on the settings surface: only the `remoteAgents.*`, `daemon.*`
//! and `server.*` keys are served, and `daemon.mode` changes only over the
//! local socket (fail-closed when the transport is unknown).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use intent_core::{Error, Result, ServerControl, WorkspaceApi};
use intent_store::Store;
use serde_json::json;

use crate::{InMemorySecretStore, Services, SettingsRegistry};

struct Transport {
    tcp: bool,
}

impl ServerControl for Transport {
    fn start_ws_listener(&self) -> Pin<Box<dyn Future<Output = Result<u16>> + Send + '_>> {
        Box::pin(async { Ok(0) })
    }
    fn stop_ws_listener(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async {})
    }
    fn ws_listener_port(&self) -> Pin<Box<dyn Future<Output = Option<u16>> + Send + '_>> {
        Box::pin(async { None })
    }
    fn is_tcp_connection(&self) -> bool {
        self.tcp
    }
}

async fn services(
    dir: &tempfile::TempDir,
    node_mode: bool,
    tcp: Option<bool>,
) -> (Arc<Services>, Arc<SettingsRegistry>) {
    let store = Store::open(&dir.path().join("state.db")).await.unwrap();
    let registry = Arc::new(SettingsRegistry::load(dir.path().join("config.toml")).unwrap());
    let services = Arc::new(
        Services::new(store)
            .with_settings_registry(registry.clone())
            .with_secret_store(Arc::new(InMemorySecretStore::default()))
            .with_node_mode(node_mode),
    );
    if let Some(tcp) = tcp {
        services.attach_server_control(Arc::new(Transport { tcp }));
    }
    (services, registry)
}

/// Run `f` as the daemon caller (an administrator), like a request task.
async fn as_daemon<T: Send + 'static>(f: impl Future<Output = T> + Send + 'static) -> T {
    intent_core::spawn_daemon(f).await.unwrap()
}

fn is_node_mode(result: &Result<serde_json::Value>) -> bool {
    matches!(result, Err(Error::NodeMode(_)))
}

#[tokio::test]
async fn node_mode_serves_only_the_three_setting_families() {
    let dir = crate::test_support::test_tempdir("node-mode-settings");
    let (svc, registry) = services(&dir, true, Some(false)).await;
    assert!(svc.node_mode());
    assert!(WorkspaceApi::node_mode(svc.as_ref()));

    let s = svc.clone();
    let got =
        as_daemon(async move { s.settings_get("remoteAgents.permission".into()).await }).await;
    assert_eq!(got.unwrap()["value"], json!("allowAll"));
    let s = svc.clone();
    let got = as_daemon(async move { s.settings_get("agents.maxConcurrent".into()).await }).await;
    assert!(is_node_mode(&got), "{got:?}");
    assert_eq!(got.unwrap_err().code(), -32003);

    let s = svc.clone();
    let applied = as_daemon(async move {
        s.settings_update(json!([{ "path": "remoteAgents.permission", "value": "autoByRisk" }]))
            .await
    })
    .await;
    assert!(applied.is_ok(), "{applied:?}");
    assert_eq!(
        registry.snapshot().effective.remote_agents.permission,
        intent_core::settings_file::RemoteAgentPermission::AutoByRisk
    );

    // One refused key refuses the whole batch; nothing is written.
    let s = svc.clone();
    let mixed = as_daemon(async move {
        s.settings_update(json!([
            { "path": "remoteAgents.permission", "value": "denyAll" },
            { "path": "git.autoCommit", "value": false },
        ]))
        .await
    })
    .await;
    assert!(is_node_mode(&mixed), "{mixed:?}");
    let effective = registry.snapshot().effective.clone();
    assert!(effective.git.auto_commit);
    assert_eq!(
        effective.remote_agents.permission,
        intent_core::settings_file::RemoteAgentPermission::AutoByRisk
    );
    let s = svc.clone();
    let reset = as_daemon(async move { s.settings_reset("git.autoCommit".into()).await }).await;
    assert!(is_node_mode(&reset), "{reset:?}");
}

#[tokio::test]
async fn regular_mode_keeps_every_setting() {
    let dir = crate::test_support::test_tempdir("node-mode-regular-settings");
    let (svc, _registry) = services(&dir, false, Some(false)).await;
    assert!(!WorkspaceApi::node_mode(svc.as_ref()));
    let s = svc.clone();
    let got = as_daemon(async move { s.settings_get("agents.maxConcurrent".into()).await }).await;
    assert!(got.is_ok(), "{got:?}");
}

#[tokio::test]
async fn daemon_mode_changes_only_over_the_local_socket() {
    for (node_mode, tcp, allowed) in [
        (false, Some(false), true),
        (true, Some(false), true),
        (false, Some(true), false),
        (true, Some(true), false),
        (false, None, false),
    ] {
        let dir = crate::test_support::test_tempdir("node-mode-daemon-mode");
        let (svc, registry) = services(&dir, node_mode, tcp).await;
        let s = svc.clone();
        let result = as_daemon(async move {
            s.settings_update(json!([{ "path": "daemon.mode", "value": "node" }]))
                .await
        })
        .await;
        let mode = registry.snapshot().effective.daemon.mode;
        if allowed {
            assert!(result.is_ok(), "{node_mode} {tcp:?}: {result:?}");
            assert_eq!(mode, intent_core::settings_file::DaemonMode::Node);
        } else {
            assert!(
                matches!(result, Err(Error::Forbidden(_))),
                "{node_mode} {tcp:?}: {result:?}"
            );
            assert_eq!(mode, intent_core::settings_file::DaemonMode::Regular);
        }
        let s = svc.clone();
        let reset = as_daemon(async move { s.settings_reset("daemon.mode".into()).await }).await;
        assert_eq!(
            reset.is_ok(),
            allowed,
            "reset {node_mode} {tcp:?}: {reset:?}"
        );
        let s = svc.clone();
        let read = as_daemon(async move { s.settings_get("daemon.mode".into()).await }).await;
        assert!(read.is_ok(), "reads stay open: {read:?}");
    }
}

/// A remote-launch port that places agents on exposed machines; every
/// other method is the unavailable port's.
struct MachinePort(crate::feature_ports::UnavailableRemoteLaunch);

impl crate::feature_ports::RemoteLaunchPort for MachinePort {
    fn placements<'a>(
        &'a self,
        svc: &'a Services,
    ) -> intent_core::BoxFuture<'a, Result<intent_core::placement::PlatformAvailability>> {
        self.0.placements(svc)
    }
    fn placement_help<'a>(
        &'a self,
        svc: &'a Services,
    ) -> intent_core::BoxFuture<'a, Option<String>> {
        self.0.placement_help(svc)
    }
    fn admit<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: &'a intent_core::WorkspaceId,
        placement: &'a intent_core::placement::AgentPlacement,
        needs: &'a intent_core::remote_launch::RemoteLaunchNeeds,
    ) -> intent_core::BoxFuture<'a, Result<intent_core::remote_launch::RemotePlacementView>> {
        self.0.admit(svc, workspace_id, placement, needs)
    }
    fn launch<'a>(
        &'a self,
        svc: &'a Services,
        request: intent_core::remote_launch::RemoteLaunchRequest,
    ) -> intent_core::BoxFuture<'a, Result<()>> {
        self.0.launch(svc, request)
    }
    fn runtime<'a>(
        &'a self,
        svc: &'a Services,
        agent_id: &'a intent_core::AgentId,
    ) -> intent_core::BoxFuture<'a, Result<Arc<crate::feature_ports::RemoteRuntime>>> {
        self.0.runtime(svc, agent_id)
    }
    fn manages_machines(&self) -> bool {
        true
    }
}

/// `remoteMachines: 1` is advertised by every backend with the remote
/// feature, never in node mode. No setting gates it.
#[tokio::test]
async fn remote_machines_capability_needs_only_the_feature_outside_node_mode() {
    for node_mode in [false, true] {
        let dir = crate::test_support::test_tempdir("remote-machines-capability");
        let (svc, _registry) = services(&dir, node_mode, Some(false)).await;
        assert!(
            !svc.remote_machines_capability(),
            "no remote feature: the backend methods are missing"
        );
        let svc = Arc::try_unwrap(svc)
            .ok()
            .expect("sole owner")
            .with_remote_launch_port(Arc::new(MachinePort(
                crate::feature_ports::UnavailableRemoteLaunch,
            )));
        assert_eq!(
            svc.remote_machines_capability(),
            !node_mode,
            "node mode {node_mode}"
        );
    }
}
