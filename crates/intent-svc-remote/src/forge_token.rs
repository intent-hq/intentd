//! The backend's GitHub token on its machines (nodeProtocol 15, monorepo
//! `docs/protocol/node-link.md` § Forge token on the machine).
//!
//! One resolved value serves every linked machine. Each connector sends it
//! in a `forgeToken` frame right after a bind, before anything else can
//! launch on that link, and again whenever [`ForgeTokens::refresh`] sees a
//! different value. Only a SHA-256 digest is compared, and nothing here logs
//! or renders the value.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use intent_core::node_link::{ForgeToken, LinkFrame, LinkId, SecretValue, FORGE_HOST_GITHUB};
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

/// The SHA-256 of a token, or of its absence (`None`).
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Digest(Option<[u8; 32]>);

/// The value the machines hold, with its digest.
#[derive(Clone, Default)]
pub(crate) struct Held {
    token: Option<SecretValue>,
    digest: Digest,
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

#[cfg(test)]
impl Held {
    /// The held token.
    pub(crate) fn frame_token(&self) -> Option<&SecretValue> {
        self.token.as_ref()
    }
}

impl std::fmt::Debug for Held {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Held")
            .field("held", &self.token.is_some())
            .finish_non_exhaustive()
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

    /// Resolve the token again; `true` when the held value changed. Every
    /// resolution wakes the linked connectors, so one whose machine has not
    /// confirmed the value sends it again.
    pub(crate) async fn refresh(&self) -> bool {
        let _one = self.refreshing.lock().await;
        let token = (self.resolver)().await;
        let digest = Digest(
            token
                .as_ref()
                .map(|token| sha2::Sha256::digest(token.as_bytes()).into()),
        );
        let mut changed = false;
        self.held.send_modify(|held| {
            if held.digest != digest {
                changed = true;
                *held = Held {
                    token: token.map(SecretValue::from),
                    digest,
                };
            }
        });
        changed
    }

    /// [`Self::refresh`] on its own task, so a caller that stops waiting
    /// does not cancel it: its result still reaches every linked machine.
    pub(crate) fn refresh_in_background(self: &Arc<Self>) -> tokio::task::JoinHandle<bool> {
        let tokens = self.clone();
        intent_core::spawn_daemon(async move { tokens.refresh().await })
    }

    /// Start the rotation watcher once: while any machine is linked, it
    /// resolves again every `every`, and at once (within `check`) after a
    /// GitHub connect, revoke or token edit, or a change of the gate.
    pub(crate) fn watch(self: &Arc<Self>, svc: &Services, every: Duration, check: Duration) {
        let svc = svc.clone();
        self.watch_with(
            move || {
                let gate = intent_services::feature_api::execution_call::expose_git_credential(
                    svc.settings_registry().as_deref(),
                );
                (intent_services::github_credential_generation(), gate)
            },
            every,
            check,
        );
    }

    /// [`Self::watch`] over any `state` whose change calls for a fresh
    /// resolution.
    pub(crate) fn watch_with<S: PartialEq + Send + 'static>(
        self: &Arc<Self>,
        state: impl Fn() -> S + Send + 'static,
        every: Duration,
        check: Duration,
    ) {
        if self.watching.swap(true, Ordering::SeqCst) {
            return;
        }
        let tokens = Arc::downgrade(self);
        intent_core::spawn_daemon(async move {
            let mut seen = state();
            let mut last = Instant::now();
            loop {
                tokio::time::sleep(check).await;
                let Some(tokens) = tokens.upgrade() else {
                    return;
                };
                if tokens.held.receiver_count() == 0 {
                    // No linked machine: each bind resolves afresh, and the
                    // period starts again at the next link.
                    seen = state();
                    last = Instant::now();
                    continue;
                }
                let now = state();
                if now != seen || last.elapsed() >= every {
                    seen = now;
                    last = Instant::now();
                    if tokens.refresh().await {
                        tracing::info!(
                            "remote machines: the GitHub token changed; sending it again"
                        );
                    }
                }
            }
        });
    }
}

/// The most frames a link waits for answers to.
const MAX_PENDING: usize = 8;

/// What one link's machine confirmed (node-link.md § Forge token on the
/// machine, rule 3): a bind starts with nothing confirmed.
#[derive(Default)]
pub(crate) struct Confirmed {
    /// The digest the machine last answered `ok` for (`Some(None)`: the
    /// confirmed absence of a token).
    value: Option<Digest>,
    /// The newest frames sent and not yet answered, with the digest each
    /// carried; at most [`MAX_PENDING`], so a machine that never answers
    /// holds nothing.
    pending: VecDeque<(LinkId, Digest)>,
    /// The first frame sent on this bind, and whether it was answered.
    bind: Option<(LinkId, bool)>,
}

impl Confirmed {
    /// Whether the machine confirmed a token (not `null`) on this link.
    pub(crate) fn token(&self) -> bool {
        matches!(self.value, Some(Digest(Some(_))))
    }

    /// Whether this bind's first frame was answered, or the machine
    /// confirmed a value since: a launch that would carry `forge` stops
    /// waiting.
    pub(crate) fn settled(&self) -> bool {
        self.value.is_some() || self.bind.is_some_and(|(_, answered)| answered)
    }

    /// Record the machine's answer to frame `request_id`.
    pub(crate) fn answer(&mut self, request_id: LinkId, ok: bool) {
        if let Some((id, answered)) = &mut self.bind {
            if *id == request_id {
                *answered = true;
            }
        }
        if let Some(at) = self.pending.iter().position(|(id, _)| *id == request_id) {
            if let Some((_, digest)) = self.pending.remove(at) {
                if ok {
                    self.value = Some(digest);
                }
            }
        }
    }
}

/// Send `held` to the machine on `link` unless it already confirmed that
/// value. Never logs the value.
pub(crate) async fn send(link: &NodeLink, held: &Held, confirmed: &Mutex<Confirmed>) {
    let frame = {
        let mut confirmed = crate::lock(confirmed);
        if confirmed.value == Some(held.digest) {
            return;
        }
        let frame = held.frame();
        if let LinkFrame::ForgeToken(forge) = &frame {
            if confirmed.pending.len() == MAX_PENDING {
                confirmed.pending.pop_front();
            }
            confirmed.pending.push_back((forge.request_id, held.digest));
            if confirmed.bind.is_none() {
                confirmed.bind = Some((forge.request_id, false));
            }
        }
        frame
    };
    if let Err(error) = link.send(&frame).await {
        tracing::warn!(%error, "remote machine: the GitHub token was not sent");
    }
}

/// The next resolution of `forge`; never resolves without one (`None`, or
/// its sender gone).
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
