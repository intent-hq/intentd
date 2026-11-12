//! Local code pairing, host side (§5.58): the in-memory code slot, the
//! persisted local pairing credentials and their two events.
//!
//! The code lives only in memory as `HMAC-SHA256(key, context | code)` under a
//! key drawn at daemon start, so a restart forgets every code. Attempts are
//! serialized on one async mutex: the first matching attempt moves the code to
//! `redeemed` and mints the credential before any other attempt observes the
//! slot. A terminal state is remembered (without the hash) until the code's
//! original expiry so late attempts get the matching refusal.
//!
//! Locality is the transport's job: it calls the owner methods only for local
//! connections and [`LocalPairing::redeem`] only from the loopback endpoint.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

use hmac::{Hmac, KeyInit, Mac};
use intent_core::local_pairing::{
    display_code, normalize_code, validate_app_name, CreatedLocalCode, LocalPairingCancelReason,
    LocalPairingCode, LocalPairingCodeState, LocalPairingCredential, LocalPairingError,
    LocalPairingService, RedeemedLocalCode, CODE_ALPHABET, CODE_HMAC_CONTEXT, CODE_LENGTH,
    CODE_LIFETIME, MAX_WRONG_TRIES, SUSPEND_ALL_SIGNAL,
};
use intent_core::{events, BoxFuture, WorkspaceId};
use intent_store::{LocalPairingCredentialRow, LocalPairingRevocation, NewEvent, Store};
use serde_json::{json, Value};
use sha2::{Digest as _, Sha256};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tokio::sync::broadcast;
use tokio::time::Instant;

use crate::EventBus;

/// Revocation fan-out capacity. A lagging receiver fails closed (the
/// transport closes its connection), so this only needs to cover bursts.
const REVOCATION_CAPACITY: usize = 64;

/// The one code slot: the current or most recent code until its expiry.
struct Slot {
    code: LocalPairingCode,
    /// The keyed hash; dropped as soon as the code leaves `active`.
    hash: Option<Vec<u8>>,
    deadline: Instant,
    /// [`LocalPairing::tunnel_starts`] when the code was created; a later
    /// tunnel start cancels it (`tunnel-active`).
    tunnel_epoch: u64,
}

/// Host-side local pairing service. Construct with [`LocalPairing::new`].
pub struct LocalPairing {
    store: Store,
    bus: Option<EventBus>,
    key: [u8; 32],
    slot: tokio::sync::Mutex<Option<Slot>>,
    connections: Mutex<HashMap<String, usize>>,
    revocations: broadcast::Sender<String>,
    lifetime: std::time::Duration,
    /// Intent's remote-access tunnel is running (see
    /// [`LocalPairingService::set_tunnel_active`]).
    tunnel_active: std::sync::atomic::AtomicBool,
    /// Tunnel starts so far; bumped after `tunnel_active` is set.
    tunnel_starts: std::sync::atomic::AtomicU64,
    me: Weak<Self>,
}

fn internal(error: impl std::fmt::Display) -> LocalPairingError {
    LocalPairingError::Internal(error.to_string())
}

fn rfc3339(at: OffsetDateTime) -> String {
    at.format(&Rfc3339).unwrap_or_default()
}

fn random_bytes<const N: usize>() -> Result<[u8; N], LocalPairingError> {
    let mut bytes = [0u8; N];
    getrandom::fill(&mut bytes).map_err(|e| internal(format!("random source failed: {e}")))?;
    Ok(bytes)
}

/// Draw a normalized code uniformly from the alphabet (rejection sampling
/// keeps the distribution uniform over 31 symbols).
fn draw_code() -> Result<String, LocalPairingError> {
    let alphabet = CODE_ALPHABET.len();
    let limit = u8::try_from(256 - 256 % alphabet).expect("limit fits in u8");
    let mut code = String::with_capacity(CODE_LENGTH);
    while code.len() < CODE_LENGTH {
        for byte in random_bytes::<16>()? {
            if byte < limit && code.len() < CODE_LENGTH {
                code.push(char::from(CODE_ALPHABET[usize::from(byte) % alphabet]));
            }
        }
    }
    Ok(code)
}

fn credential_json(row: &LocalPairingCredentialRow, connected: bool) -> LocalPairingCredential {
    LocalPairingCredential {
        credential_id: row.id.clone(),
        app_name: row.app_name.clone(),
        created_at: row.created_at.clone(),
        last_used_at: row.last_used_at.clone(),
        connected,
    }
}

/// Test seam: a code lifetime in milliseconds that can only *shorten* the
/// 10-minute [`CODE_LIFETIME`] (end-to-end expiry tests). Values that do not
/// parse, are below 100 ms or exceed the contract lifetime are ignored.
pub const CODE_LIFETIME_ENV: &str = "INTENTD_LOCAL_PAIRING_CODE_LIFETIME_MS";

fn code_lifetime_from(env: Option<&str>) -> std::time::Duration {
    env.and_then(|value| value.trim().parse::<u64>().ok())
        .map(std::time::Duration::from_millis)
        .filter(|lifetime| {
            *lifetime >= std::time::Duration::from_millis(100) && *lifetime < CODE_LIFETIME
        })
        .unwrap_or(CODE_LIFETIME)
}

impl LocalPairing {
    /// A new service with a fresh in-memory HMAC key.
    ///
    /// # Errors
    ///
    /// When the OS random source fails.
    pub fn new(store: Store, bus: Option<EventBus>) -> Result<Arc<Self>, LocalPairingError> {
        let lifetime = code_lifetime_from(std::env::var(CODE_LIFETIME_ENV).ok().as_deref());
        Self::with_lifetime(store, bus, lifetime)
    }

    fn with_lifetime(
        store: Store,
        bus: Option<EventBus>,
        lifetime: std::time::Duration,
    ) -> Result<Arc<Self>, LocalPairingError> {
        let key = random_bytes::<32>()?;
        Ok(Arc::new_cyclic(|me| Self {
            store,
            bus,
            key,
            slot: tokio::sync::Mutex::new(None),
            connections: Mutex::new(HashMap::new()),
            revocations: broadcast::channel(REVOCATION_CAPACITY).0,
            lifetime,
            tunnel_active: std::sync::atomic::AtomicBool::new(false),
            tunnel_starts: std::sync::atomic::AtomicU64::new(0),
            me: me.clone(),
        }))
    }

    fn keyed_hash(&self, normalized: &str) -> Hmac<Sha256> {
        let mut mac = <Hmac<Sha256> as KeyInit>::new_from_slice(&self.key)
            .expect("HMAC accepts any key length");
        mac.update(CODE_HMAC_CONTEXT.as_bytes());
        mac.update(normalized.as_bytes());
        mac
    }

    fn publish(&self, event_type: &str, data: Value) {
        let Some(bus) = &self.bus else { return };
        let _ = bus.publish_transient(&NewEvent {
            workspace_id: WorkspaceId::from_string(String::new()),
            timestamp: intent_core::now_iso(),
            event_type: event_type.to_owned(),
            actor: crate::system_actor(),
            session_id: None,
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            data,
        });
    }

    fn publish_code(&self, code: &LocalPairingCode) {
        self.publish(
            events::PAIRING_LOCAL_CODE_CHANGED,
            json!({ "localCode": code }),
        );
    }

    fn publish_credential(&self, credential: &LocalPairingCredential) {
        self.publish(
            events::PAIRING_LOCAL_CREDENTIALS_CHANGED,
            json!({ "credential": credential }),
        );
    }

    fn connected(&self, credential_id: &str) -> bool {
        self.connections
            .lock()
            .expect("local pairing connections poisoned")
            .get(credential_id)
            .is_some_and(|n| *n > 0)
    }

    /// Apply expiry: an active code past its deadline becomes `expired`
    /// (emitted), and any code past its deadline leaves the slot.
    fn settle(&self, slot: &mut Option<Slot>) {
        // A tunnel start after the code was created cancels it, whether or
        // not the start's own cleanup task has run yet.
        let starts = self.tunnel_starts.load(std::sync::atomic::Ordering::SeqCst);
        if let Some(current) = slot.as_mut() {
            if current.code.state == LocalPairingCodeState::Active && current.tunnel_epoch != starts
            {
                current.code.state = LocalPairingCodeState::Cancelled;
                current.code.cancel_reason = Some(LocalPairingCancelReason::TunnelActive);
                current.hash = None;
                self.publish_code(&current.code);
            }
        }
        if slot.as_ref().is_some_and(|s| Instant::now() >= s.deadline) {
            let mut done = slot.take().expect("checked above");
            if done.code.state == LocalPairingCodeState::Active {
                done.code.state = LocalPairingCodeState::Expired;
                self.publish_code(&done.code);
            }
        }
    }

    fn arm_expiry(&self, code_id: String, deadline: Instant) {
        let me = self.me.clone();
        intent_core::spawn_daemon(async move {
            tokio::time::sleep_until(deadline).await;
            let Some(me) = me.upgrade() else { return };
            let mut slot = me.slot.lock().await;
            if slot.as_ref().is_some_and(|s| s.code.code_id == code_id) {
                me.settle(&mut slot);
            }
        });
    }

    async fn create(&self) -> Result<CreatedLocalCode, LocalPairingError> {
        let normalized = draw_code()?;
        let hash = self
            .keyed_hash(&normalized)
            .finalize()
            .into_bytes()
            .to_vec();
        let created = OffsetDateTime::now_utc();
        let lifetime = time::Duration::try_from(self.lifetime).map_err(internal)?;
        let code = LocalPairingCode {
            code_id: uuid::Uuid::new_v4().to_string(),
            state: LocalPairingCodeState::Active,
            created_at: rfc3339(created),
            expires_at: rfc3339(created + lifetime),
            attempts_remaining: MAX_WRONG_TRIES,
            cancel_reason: None,
            credential_id: None,
            app_name: None,
        };
        let deadline = Instant::now() + self.lifetime;
        let mut slot = self.slot.lock().await;
        // Read the start count before the flag (see `set_tunnel_active`).
        let tunnel_epoch = self.tunnel_starts.load(std::sync::atomic::Ordering::SeqCst);
        if self.tunnel_is_active() {
            return Err(LocalPairingError::TunnelActive);
        }
        self.settle(&mut slot);
        if let Some(old) = slot.as_mut() {
            if old.code.state == LocalPairingCodeState::Active {
                old.code.state = LocalPairingCodeState::Cancelled;
                old.code.cancel_reason = Some(LocalPairingCancelReason::Replaced);
                old.hash = None;
                self.publish_code(&old.code);
            }
        }
        *slot = Some(Slot {
            code: code.clone(),
            hash: Some(hash),
            deadline,
            tunnel_epoch,
        });
        self.publish_code(&code);
        drop(slot);
        self.arm_expiry(code.code_id.clone(), deadline);
        Ok(CreatedLocalCode {
            code: display_code(&normalized),
            local_code: code,
        })
    }

    async fn cancel(&self, code_id: Option<String>) -> bool {
        let mut slot = self.slot.lock().await;
        self.settle(&mut slot);
        let Some(current) = slot.as_mut() else {
            return false;
        };
        if current.code.state != LocalPairingCodeState::Active
            || code_id.is_some_and(|id| id != current.code.code_id)
        {
            return false;
        }
        current.code.state = LocalPairingCodeState::Cancelled;
        current.code.cancel_reason = Some(LocalPairingCancelReason::User);
        current.hash = None;
        self.publish_code(&current.code);
        true
    }

    /// Spend the active code and mint a credential (contract check order:
    /// params, then the code's state, then the comparison).
    ///
    /// # Errors
    ///
    /// The §5.58 redemption errors; `Internal` when minting fails (the code
    /// then stays active).
    pub async fn redeem(
        &self,
        code: &str,
        app_name: &str,
    ) -> Result<RedeemedLocalCode, LocalPairingError> {
        let normalized = normalize_code(code).ok_or_else(|| {
            LocalPairingError::InvalidParams(
                "code must be 6 characters from the pairing alphabet".into(),
            )
        })?;
        let app_name = validate_app_name(app_name)?;
        let mut slot = self.slot.lock().await;
        // Checked under the slot lock, before the code's state: while the
        // tunnel runs there is no redeemable code (§5.58: the active one is
        // cancelled), so a tunnelled attempt never counts or spends anything.
        if self.tunnel_is_active() {
            return Err(LocalPairingError::Expired);
        }
        self.settle(&mut slot);
        let Some(current) = slot.as_mut() else {
            return Err(LocalPairingError::Expired);
        };
        match current.code.state {
            LocalPairingCodeState::Active => {}
            LocalPairingCodeState::Locked => return Err(LocalPairingError::Locked),
            _ => return Err(LocalPairingError::Expired),
        }
        let stored = current.hash.as_deref().ok_or(LocalPairingError::Expired)?;
        if self.keyed_hash(&normalized).verify_slice(stored).is_err() {
            current.code.attempts_remaining = current.code.attempts_remaining.saturating_sub(1);
            if current.code.attempts_remaining == 0 {
                current.code.state = LocalPairingCodeState::Locked;
                current.hash = None;
                self.publish_code(&current.code);
                return Err(LocalPairingError::Locked);
            }
            self.publish_code(&current.code);
            return Err(LocalPairingError::Invalid {
                attempts_remaining: current.code.attempts_remaining,
            });
        }
        let token = hex::encode(random_bytes::<32>()?);
        let token_hash = hex::encode(Sha256::digest(token.as_bytes()));
        let credential_id = uuid::Uuid::new_v4().to_string();
        let row = self
            .store
            .insert_local_pairing_credential(&credential_id, &token_hash, &app_name)
            .await
            .map_err(internal)?;
        current.code.state = LocalPairingCodeState::Redeemed;
        current.code.credential_id = Some(credential_id.clone());
        current.code.app_name = Some(app_name.clone());
        current.hash = None;
        self.publish_code(&current.code);
        drop(slot);
        self.publish_credential(&credential_json(&row, false));
        Ok(RedeemedLocalCode {
            credential_id,
            token,
            app_name,
        })
    }

    async fn list(&self) -> Result<Vec<LocalPairingCredential>, LocalPairingError> {
        let rows = self
            .store
            .list_local_pairing_credentials()
            .await
            .map_err(internal)?;
        Ok(rows
            .iter()
            .map(|row| credential_json(row, self.connected(&row.id)))
            .collect())
    }

    async fn revoke(&self, credential_id: String) -> Result<bool, LocalPairingError> {
        if credential_id.trim().is_empty() {
            return Err(LocalPairingError::InvalidParams(
                "credentialId is required".into(),
            ));
        }
        match self
            .store
            .revoke_local_pairing_credential(&credential_id)
            .await
            .map_err(internal)?
        {
            LocalPairingRevocation::Revoked => {
                let _ = self.revocations.send(credential_id.clone());
                self.publish(
                    events::PAIRING_LOCAL_CREDENTIALS_CHANGED,
                    json!({ "credentialId": credential_id, "revoked": true }),
                );
                Ok(true)
            }
            LocalPairingRevocation::AlreadyRevoked => Ok(false),
            LocalPairingRevocation::NotFound => Err(LocalPairingError::NotFound),
        }
    }

    fn tunnel_is_active(&self) -> bool {
        self.tunnel_active.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Emit the tunnel cancellation promptly ([`Self::settle`] applies it).
    async fn cancel_for_tunnel(&self) {
        let mut slot = self.slot.lock().await;
        self.settle(&mut slot);
    }

    /// Adjust the live-connection count and emit on a `connected` flip.
    fn connection_delta(&self, credential_id: &str, opened: bool) {
        let flipped = {
            let mut connections = self
                .connections
                .lock()
                .expect("local pairing connections poisoned");
            let count = connections.entry(credential_id.to_owned()).or_insert(0);
            let before = *count > 0;
            if opened {
                *count += 1;
            } else {
                *count = count.saturating_sub(1);
            }
            let after = *count > 0;
            if *count == 0 {
                connections.remove(credential_id);
            }
            before != after
        };
        if !flipped || self.bus.is_none() {
            return;
        }
        let me = self.me.clone();
        let credential_id = credential_id.to_owned();
        intent_core::spawn_daemon(async move {
            let Some(me) = me.upgrade() else { return };
            if let Ok(Some(row)) = me.store.get_local_pairing_credential(&credential_id).await {
                if row.revoked_at.is_none() {
                    me.publish_credential(&credential_json(&row, me.connected(&credential_id)));
                }
            }
        });
    }
}

impl LocalPairingService for LocalPairing {
    fn create_code(&self) -> BoxFuture<'_, Result<CreatedLocalCode, LocalPairingError>> {
        Box::pin(self.create())
    }

    fn cancel_code(&self, code_id: Option<String>) -> BoxFuture<'_, bool> {
        Box::pin(self.cancel(code_id))
    }

    fn redeem(
        &self,
        code: String,
        app_name: String,
    ) -> BoxFuture<'_, Result<RedeemedLocalCode, LocalPairingError>> {
        Box::pin(async move { LocalPairing::redeem(self, &code, &app_name).await })
    }

    fn list_credentials(
        &self,
    ) -> BoxFuture<'_, Result<Vec<LocalPairingCredential>, LocalPairingError>> {
        Box::pin(self.list())
    }

    fn revoke_credential(
        &self,
        credential_id: String,
    ) -> BoxFuture<'_, Result<bool, LocalPairingError>> {
        Box::pin(self.revoke(credential_id))
    }

    fn resolve_credential(&self, token_hash: String) -> BoxFuture<'_, Option<String>> {
        Box::pin(async move {
            if self.tunnel_is_active() {
                return None;
            }
            self.store
                .resolve_local_pairing_credential(&token_hash)
                .await
                .unwrap_or_else(|e| {
                    tracing::warn!(error = %e, "local pairing credential lookup failed; rejecting");
                    None
                })
        })
    }

    fn credential_active(&self, credential_id: String) -> BoxFuture<'_, bool> {
        Box::pin(async move {
            !self.tunnel_is_active()
                && matches!(
                    self.store.get_local_pairing_credential(&credential_id).await,
                    Ok(Some(row)) if row.revoked_at.is_none()
                )
        })
    }

    fn subscribe_revocations(&self) -> broadcast::Receiver<String> {
        self.revocations.subscribe()
    }

    fn connection_opened(&self, credential_id: &str) {
        self.connection_delta(credential_id, true);
    }

    fn connection_closed(&self, credential_id: &str) {
        self.connection_delta(credential_id, false);
    }

    fn set_tunnel_active(&self, active: bool) {
        let was = self
            .tunnel_active
            .swap(active, std::sync::atomic::Ordering::SeqCst);
        if active {
            // After the flag: a create that read the old count before this
            // bump is cancelled by it; one that reads the new count has
            // already seen the flag and refused (see `create`).
            self.tunnel_starts
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        if !active || was {
            return;
        }
        // Credentials stay valid; their live connections close (1008) and
        // admission refuses them until the tunnel stops.
        let _ = self.revocations.send(SUSPEND_ALL_SIGNAL.to_owned());
        let me = self.me.clone();
        intent_core::spawn_daemon(async move {
            if let Some(me) = me.upgrade() {
                me.cancel_for_tunnel().await;
            }
        });
    }

    fn tunnel_active(&self) -> bool {
        self.tunnel_is_active()
    }
}

#[cfg(test)]
mod tests;
