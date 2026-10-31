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
    let got = as_daemon(async move { s.settings_get("remoteAgents.enabled".into()).await }).await;
    assert_eq!(got.unwrap()["value"], json!(false));
    let s = svc.clone();
    let got = as_daemon(async move { s.settings_get("agents.maxConcurrent".into()).await }).await;
    assert!(is_node_mode(&got), "{got:?}");
    assert_eq!(got.unwrap_err().code(), -32003);

    let s = svc.clone();
    let applied = as_daemon(async move {
        s.settings_update(json!([{ "path": "remoteAgents.enabled", "value": true }]))
            .await
    })
    .await;
    assert!(applied.is_ok(), "{applied:?}");
    assert!(registry.snapshot().effective.remote_agents.enabled);

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
        intent_core::settings_file::RemoteAgentPermission::AllowAll
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
