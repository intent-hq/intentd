//! The backend's GitHub token on its machines (nodeProtocol 15, monorepo
//! `docs/protocol/node-link.md` § Forge token on the machine).
//!
//! One resolved value serves every linked machine. Each connector sends it
//! in a `forgeToken` frame right after a bind, before anything else can
//! launch on that link, and again whenever [`ForgeTokens::refresh`] sees a
//! different value. Only a SHA-256 digest is compared, and nothing here logs
//! or renders the value.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use intent_core::node_link::{
    ForgeToken, LinkFrame, LinkId, SecretValue, FORGE_HOST_GITHUB,
};
use intent_core::BoxFuture;
use intent_services::feature_api::Services;
use intent_transport::node_link::NodeLink;
use sha2::Digest as _;
use tokio::sync::watch;
use tokio::time::Instant;
use zeroize::Zeroizing;

/// Resolves the token to hand out now: `None` when there is none or the
/// `sourceControl.github.exposeGitCredentialToChildren` gate is off.
pub(crate) type Resolver =
    Arc<dyn Fn() -> BoxFuture<'static, Option<Zeroizing<String>>> + Send + Sync>;

/// The value the machines hold, with its digest.
#[derive(Clone, Default)]
pub(crate) struct Held {
    token: Option<SecretValue>,
    digest: Option<[u8; 32]>,
}

impl Held {
    /// The frame that hands this value (or its absence) to a machine.
    pub(crate) fn frame(&self) -> LinkFrame {
        LinkFrame::ForgeToken(ForgeToken {
            request_id: LinkId::new_v4(),
            host: FORGE_HOST_GITHUB.to_owned(),
            token: self.token.clone(),
        })
    }
}

impl std::fmt::Debug for Held {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Held")
            .field("held", &self.token.is_some())
            .finish()
    }
}

/// See the module docs.
pub(crate) struct ForgeTokens {
    resolver: Resolver,
    held: watch::Sender<Held>,
    /// One resolution at a time, so an older one never overwrites a newer.
    refreshing: tokio::sync::Mutex<()>,
    watching: AtomicBool,
}

impl ForgeTokens {
    pub(crate) fn new(resolver: Resolver) -> Self {
        Self {
            resolver,
            held: watch::channel(Held::default()).0,
            refreshing: tokio::sync::Mutex::new(()),
            watching: AtomicBool::new(false),
        }
    }

    /// The daemon's own GitHub credential, behind its gate.
    pub(crate) fn for_services(svc: &Services) -> Self {
        let registry = svc.settings_registry();
        Self::new(Arc::new(move || {
            let registry = registry.clone();
            Box::pin(async move {
                intent_services::github_git_credential(registry.as_deref())
                    .await
                    .map(|(_, token)| Zeroizing::new(token))
            })
        }))
    }

    /// Follow the held value: a connector's serving loop sends each change.
    pub(crate) fn subscribe(&self) -> watch::Receiver<Held> {
        self.held.subscribe()
    }

    /// Resolve the token again; `true` when the held value changed.
    pub(crate) async fn refresh(&self) -> bool {
        let _one = self.refreshing.lock().await;
        let token = (self.resolver)().await;
        let digest: Option<[u8; 32]> = token
            .as_ref()
            .map(|token| sha2::Sha256::digest(token.as_bytes()).into());
        self.held.send_if_modified(|held| {
            if held.digest == digest {
                return false;
            }
            *held = Held {
                token: token.map(SecretValue::from),
                digest,
            };
            true
        })
    }

    /// Start the rotation watcher once: while any machine is linked, it
    /// resolves again every `every`, and at once (within `check`) after a
    /// GitHub connect, revoke or token edit, or a change of the gate.
    pub(crate) fn watch(self: &Arc<Self>, svc: &Services, every: Duration, check: Duration) {
        if self.watching.swap(true, Ordering::SeqCst) {
            return;
        }
        let tokens = Arc::downgrade(self);
        let svc = svc.clone();
        intent_core::spawn_daemon(async move {
            let state = || {
                let gate = intent_services::feature_api::execution_call::expose_git_credential(
                    svc.settings_registry().as_deref(),
                );
                (intent_services::github_credential_generation(), gate)
            };
            let mut seen = state();
            let mut last = Instant::now();
            loop {
                tokio::time::sleep(check).await;
                let Some(tokens) = tokens.upgrade() else {
                    return;
                };
                let now = state();
                if tokens.held.receiver_count() == 0 {
                    // No linked machine: each bind resolves afresh.
                    seen = now;
                    continue;
                }
                if now != seen || last.elapsed() >= every {
                    seen = now;
                    last = Instant::now();
                    if tokens.refresh().await {
                        tracing::info!("remote machines: the GitHub token changed; sending it again");
                    }
                }
            }
        });
    }
}

/// Send `held` to the machine on `link`. Never logs the value.
pub(crate) async fn send(link: &NodeLink, held: &Held) {
    if let Err(error) = link.send(&held.frame()).await {
        tracing::warn!(%error, "remote machine: the GitHub token was not sent");
    }
}

/// The next change of `forge`; never resolves without one (`None`, or its
/// sender gone).
pub(crate) async fn changed(forge: &mut Option<watch::Receiver<Held>>) -> Held {
    loop {
        match forge {
            Some(rx) => {
                if rx.changed().await.is_ok() {
                    return rx.borrow_and_update().clone();
                }
                *forge = None;
            }
            None => std::future::pending::<()>().await,
        }
    }
}
