//! Lifecycle ownership for listeners that exist before the Store.
use intent_core::startup::{StartupPhase, StartupStatus};
use intent_transport::{startup::StartupDispatch, WsApiServer};
use std::sync::Arc;

pub struct Bootstrap {
    pub dispatch: StartupDispatch,
    pub ws: Option<WsApiServer>,
    pub port: Option<u16>,
    pub local: Option<tokio::task::JoinHandle<std::io::Result<()>>>,
    pub stop_local: Option<tokio::sync::oneshot::Sender<()>>,
    pub tunnel: Arc<crate::tunnel::TunnelSupervisor>,
}

impl Bootstrap {
    pub fn new(config: &intent_core::Config) -> Self {
        Self {
            dispatch: StartupDispatch::new(StartupStatus::new()),
            ws: None,
            port: None,
            local: None,
            stop_local: None,
            tunnel: Arc::new(crate::tunnel::TunnelSupervisor::new(
                crate::tunnel::resolve_tailcat_bin(),
                &config.data_dir.join("tunnel"),
            )),
        }
    }

    pub async fn stop(&mut self) {
        if self.dispatch.status().snapshot().phase != StartupPhase::Ready {
            self.dispatch.status().fail();
        }
        if let Some(stop) = self.stop_local.take() {
            let _ = stop.send(());
        }
        if let Some(local) = self.local.take() {
            let _ = local.await;
        }
        self.tunnel.stop().await;
        if let Some(ws) = self.ws.take() {
            ws.stop().await;
        }
    }
}

/// A file edit affecting admission invalidates this bootstrap attempt. Other
/// settings keep their ordinary post-start live-reload behavior.
pub fn admission_unchanged(
    registry: &intent_services::SettingsRegistry,
    path: &std::path::Path,
    insecure: bool,
) -> bool {
    let Ok(fresh) = intent_services::SettingsRegistry::load(path) else {
        return false;
    };
    if crate::apply_startup_pins(&fresh, insecure).is_err() {
        return false;
    }
    let fresh = fresh.snapshot();
    let expected = registry.snapshot();
    [
        "server.wsApi.enabled",
        "server.wsApi.port",
        "server.bindAddress",
        "server.tunnel.only",
        "server.tunnel.enabled",
        "server.tunnel.derpUrl",
    ]
    .iter()
    .all(|key| fresh.get(key) == expected.get(key))
}
