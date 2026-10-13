//! Lifecycle ownership for listeners that exist before the Store.
use intent_core::startup::{StartupPhase, StartupStatus};
use intent_transport::{startup::StartupDispatch, WsApiServer};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

pub struct Bootstrap {
    pub dispatch: StartupDispatch,
    pub cancellation: Cancellation,
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
            cancellation: Cancellation::new(),
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
        self.stop_local_listener().await;
        self.tunnel.stop().await;
        if let Some(ws) = self.ws.take() {
            ws.stop().await;
        }
    }

    pub async fn stop_local_listener(&mut self) {
        if let Some(stop) = self.stop_local.take() {
            let _ = stop.send(());
        }
        if let Some(local) = self.local.take() {
            let _ = local.await;
        }
    }
}

/// A latched stop request shared with the initialization owner. Only the
/// pre-Store future may be dropped; service initialization owns its teardown.
#[derive(Clone)]
pub struct Cancellation {
    services_owned: Arc<AtomicBool>,
    stop: tokio::sync::watch::Sender<bool>,
}

impl Cancellation {
    fn new() -> Self {
        let (stop, _) = tokio::sync::watch::channel(false);
        Self {
            services_owned: Arc::new(AtomicBool::new(false)),
            stop,
        }
    }

    pub fn own_services(&self) {
        self.services_owned.store(true, Ordering::Release);
    }

    pub fn services_owned(&self) -> bool {
        self.services_owned.load(Ordering::Acquire)
    }

    pub fn request_stop(&self) {
        self.stop.send_replace(true);
    }

    pub fn stopping(&self) -> bool {
        *self.stop.borrow()
    }

    pub async fn stopped(&self) {
        let mut stop = self.stop.subscribe();
        let _ = stop.wait_for(|value| *value).await;
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
