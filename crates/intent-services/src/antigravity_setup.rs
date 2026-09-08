//! Connection-owned, explicit setup. No settings writes or shared events.

use std::future::Future;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use intent_providers::{antigravity as runtime, find_provider_binary};
use serde::Serialize;

use crate::antigravity_install::{self, Cancellation, InstallError, InstallProgress};

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(tag = "phase", rename_all = "camelCase")]
pub enum Phase {
    Idle,
    Checking,
    Downloading {
        received: u64,
        total: u64,
    },
    Verifying,
    SignInRequired,
    SigningIn,
    Connected {
        #[serde(rename = "modelCount")]
        model_count: usize,
    },
    Cancelled,
    Failed {
        code: Failure,
    },
}

impl Phase {
    fn busy(&self) -> bool {
        matches!(
            self,
            Self::Checking | Self::Downloading { .. } | Self::Verifying | Self::SigningIn
        )
    }
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum Failure {
    Install(InstallError),
    Setup(SetupError),
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum SetupError {
    InvalidCustomPath,
    AuthenticationCheckFailed,
    SignInFailed,
    BrowserUnavailable,
    ModelsUnavailable,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Status {
    pub operation_id: Option<String>,
    pub supported: bool,
    pub cli_detected: bool,
    pub runtime_installed: bool,
    #[serde(flatten)]
    pub phase: Phase,
}

/// Metadata only: no login, download, credential access, or preference changes.
#[must_use]
pub fn status(explicit: Option<&str>) -> Status {
    Status {
        operation_id: None,
        supported: runtime::supported_host(),
        cli_detected: intent_providers::discover::find_antigravity_cli().is_some(),
        runtime_installed: find_provider_binary("antigravity", "antigravity-acp", explicit)
            .is_some(),
        phase: Phase::Idle,
    }
}

struct Shared {
    status: Status,
    binary: Option<PathBuf>,
}

/// The transport retains this handle only for the initiating connection.
/// Dropping it cancels children and any installer staging work.
pub struct Operation {
    shared: Arc<Mutex<Shared>>,
    cancel: Cancellation,
}

impl Drop for Operation {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

impl Operation {
    #[must_use]
    pub fn start(home: PathBuf, explicit: Option<String>) -> Self {
        let mut initial = status(explicit.as_deref());
        initial.operation_id = Some(uuid::Uuid::new_v4().to_string());
        initial.phase = Phase::Checking;
        let operation = Self {
            shared: Arc::new(Mutex::new(Shared {
                status: initial,
                binary: None,
            })),
            cancel: Cancellation::default(),
        };
        let shared = Arc::clone(&operation.shared);
        let cancel = operation.cancel.clone();
        tokio::spawn(async move {
            let result = connect(&shared, &cancel, home, explicit).await;
            finish(&shared, &cancel, result);
        });
        operation
    }

    #[must_use]
    pub fn status(&self) -> Status {
        self.shared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .status
            .clone()
    }

    #[must_use]
    pub fn reusable(&self) -> bool {
        let phase = self.status().phase;
        phase.busy() || matches!(phase, Phase::SignInRequired)
    }

    pub fn cancel(&self) {
        self.cancel.cancel();
        self.shared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .status
            .phase = Phase::Cancelled;
    }

    /// The callback receives an ephemeral, validated URL. It must not log or
    /// persist the URL. Repeat clicks cannot spawn another login process.
    pub fn login<F, C>(&self, open_url: F) -> bool
    where
        F: FnOnce(String) -> C + Send + 'static,
        C: Future<Output = bool> + Send + 'static,
    {
        let binary = {
            let mut shared = self
                .shared
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if shared.status.phase == Phase::SigningIn {
                return true;
            }
            if shared.status.phase != Phase::SignInRequired || self.cancel.is_cancelled() {
                return false;
            }
            let Some(binary) = shared.binary.clone() else {
                return false;
            };
            shared.status.phase = Phase::SigningIn;
            binary
        };
        let shared = Arc::clone(&self.shared);
        let cancel = self.cancel.clone();
        tokio::spawn(async move {
            let result = login(&binary, &cancel, open_url).await;
            let result = match result {
                Ok(()) => checked_models(&binary, &cancel).await,
                Err(error) => Err(error),
            };
            finish(&shared, &cancel, result);
        });
        true
    }
}

fn set_phase(shared: &Mutex<Shared>, cancel: &Cancellation, phase: Phase) {
    let mut shared = shared
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if !cancel.is_cancelled() {
        shared.status.phase = phase;
    }
}

fn finish(shared: &Mutex<Shared>, cancel: &Cancellation, result: Result<Phase, Failure>) {
    if !cancel.is_cancelled() && matches!(&result, Ok(Phase::Connected { .. })) {
        // A fresh guarded session supersedes an older cached auth failure.
        // The existing promotion epoch also fences in-flight stale probes.
        crate::provider_auth::promote_auth_verdict("antigravity");
    }
    set_phase(
        shared,
        cancel,
        result.unwrap_or_else(|code| Phase::Failed { code }),
    );
}

async fn connect(
    shared: &Arc<Mutex<Shared>>,
    cancel: &Cancellation,
    home: PathBuf,
    explicit: Option<String>,
) -> Result<Phase, Failure> {
    if !runtime::supported_host() {
        return Err(Failure::Install(InstallError::UnsupportedHost));
    }
    let binary = if let Some(path) = explicit.as_deref().filter(|s| !s.trim().is_empty()) {
        // A broken explicit override is a repair action, not permission to
        // silently install or select a different runtime.
        intent_providers::discover::resolve_explicit_path("antigravity", path)
            .ok_or(Failure::Setup(SetupError::InvalidCustomPath))?
    } else if let Some(binary) = find_provider_binary("antigravity", "antigravity-acp", None) {
        binary
    } else {
        let state = Arc::clone(shared);
        let cancellation = cancel.clone();
        antigravity_install::install(
            runtime::install_root(&home),
            cancel.clone(),
            Arc::new(move |progress| {
                let phase = match progress {
                    InstallProgress::Downloading { received, total } => {
                        Phase::Downloading { received, total }
                    }
                    InstallProgress::Verifying => Phase::Verifying,
                };
                set_phase(&state, &cancellation, phase);
            }),
        )
        .await
        .map_err(Failure::Install)?
    };
    {
        let mut state = shared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.binary = Some(binary.clone());
        state.status.runtime_installed = true;
    }
    set_phase(shared, cancel, Phase::Checking);
    let authenticated = tokio::select! {
        biased;
        () = cancel.cancelled() => return Err(Failure::Install(InstallError::Cancelled)),
        result = crate::provider_models::probe_antigravity_auth(binary.clone()) => result,
    };
    match authenticated {
        Some(true) => checked_models(&binary, cancel).await,
        Some(false) => Ok(Phase::SignInRequired),
        None => Err(Failure::Setup(SetupError::AuthenticationCheckFailed)),
    }
}

async fn checked_models(binary: &std::path::Path, cancel: &Cancellation) -> Result<Phase, Failure> {
    let result = tokio::select! {
        biased;
        () = cancel.cancelled() => return Err(Failure::Install(InstallError::Cancelled)),
        result = crate::provider_models::fetch_antigravity_models_at(Some(binary.to_owned())) => result,
    };
    result
        .models
        .filter(|rows| !rows.is_empty())
        .map(|rows| Phase::Connected {
            model_count: rows.len(),
        })
        .ok_or(Failure::Setup(SetupError::ModelsUnavailable))
}

async fn login<F, C>(
    binary: &std::path::Path,
    cancel: &Cancellation,
    open_url: F,
) -> Result<(), Failure>
where
    F: FnOnce(String) -> C,
    C: Future<Output = bool>,
{
    // One URL per explicit click. A second notification cannot open extra tabs.
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let mut sender = Some(sender);
    let child_cancel = Cancellation::default();
    let authentication = crate::antigravity::login(
        binary.to_owned(),
        move |url| {
            if let Some(sender) = sender.take() {
                let _ = sender.send(url.to_owned());
            }
        },
        child_cancel.cancelled(),
    );
    tokio::pin!(authentication);
    let open = async {
        match receiver.await {
            Ok(url) if crate::antigravity::valid_login_url(&url) => open_url(url).await,
            _ => false,
        }
    };
    tokio::pin!(open);
    let mut opened = false;
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => {
                child_cancel.cancel();
                let _ = authentication.await;
                return Err(Failure::Install(InstallError::Cancelled));
            }
            result = &mut authentication => return result.map_err(|_| Failure::Setup(SetupError::SignInFailed)),
            success = &mut open, if !opened => {
                if !success {
                    child_cancel.cancel();
                    let _ = authentication.await;
                    return Err(Failure::Setup(SetupError::BrowserUnavailable));
                }
                opened = true;
            }
        }
    }
}

#[cfg(test)]
mod tests;
