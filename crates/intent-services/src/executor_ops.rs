//! Machine-side executor credentials for daemon-to-daemon remote agents
//! (monorepo `docs/protocol/methods/nodes.md`, "Machine methods: executor
//! credentials"): owner-only `executor.mint`, `executor.list` and
//! `executor.revoke`.
//!
//! The methods are on neither the collaborator nor the member allowlist, so
//! the transport refuses members and guests first; [`Services::require_machine_owner`]
//! rechecks in depth against the durable host role. The minted token is
//! returned once: only its SHA-256 hash is stored and it is never logged.

use intent_core::machines::{ExecutorCredential, ExecutorLiveStatus};
use intent_core::placement::AgentPlatform;
use intent_core::{current_caller, now_iso, Caller, Error, ExecutorCredentialId, HostRole, Result};
use intent_store::ExecutorRevocation;
use serde_json::{json, Map, Value};

use crate::Services;

/// The setting that gates `executor.mint`.
const ENABLED_SETTING: &str = "remoteAgents.enabled";

/// Longest `backendId`, `backendName` and `backendFingerprint`, in UTF-8 bytes.
const MAX_PARAM_BYTES: usize = 128;

/// Bytes of randomness in a minted token (hex-encoded on the wire).
const TOKEN_BYTES: usize = 32;

fn forbidden(what: &str) -> Error {
    Error::Forbidden(format!("{what} requires the machine owner"))
}

fn text_param<'a>(params: &'a Value, name: &str, required: bool) -> Result<Option<&'a str>> {
    let value = match params.get(name) {
        None | Some(Value::Null) if !required => return Ok(None),
        None | Some(Value::Null) => {
            return Err(Error::InvalidParams(format!("{name} is required")));
        }
        Some(value) => value,
    };
    let text = value
        .as_str()
        .ok_or_else(|| Error::InvalidParams(format!("{name} must be a string")))?;
    if text.trim().is_empty() || text.len() > MAX_PARAM_BYTES {
        return Err(Error::InvalidParams(format!(
            "{name} must be 1-{MAX_PARAM_BYTES} UTF-8 bytes and not blank"
        )));
    }
    Ok(Some(text))
}

fn random_token() -> Result<String> {
    use std::fmt::Write as _;
    let mut bytes = [0_u8; TOKEN_BYTES];
    getrandom::fill(&mut bytes)
        .map_err(|_| Error::Internal("executor token randomness unavailable".into()))?;
    Ok(bytes
        .iter()
        .fold(String::with_capacity(TOKEN_BYTES * 2), |mut text, byte| {
            let _ = write!(text, "{byte:02x}");
            text
        }))
}

fn backend_row(credential: &ExecutorCredential, live: ExecutorLiveStatus) -> Value {
    let mut row = Map::new();
    row.insert("credentialId".into(), json!(credential.id));
    row.insert("backendId".into(), json!(credential.backend_id));
    row.insert("backendName".into(), json!(credential.backend_name));
    if let Some(fingerprint) = &credential.backend_fingerprint {
        row.insert("backendFingerprint".into(), json!(fingerprint));
    }
    row.insert("createdAt".into(), json!(credential.created_at));
    if let Some(last_used_at) = &credential.last_used_at {
        row.insert("lastUsedAt".into(), json!(last_used_at));
    }
    row.insert("connected".into(), json!(live.connected));
    row.insert("runningAgents".into(), json!(live.running_agents));
    Value::Object(row)
}

impl Services {
    /// Owner-only gate for the `executor.*` methods: the daemon itself, or a
    /// wire caller admitted as the owner whose durable host role is still
    /// owner. Members, guests, agents and executor credentials are refused.
    async fn require_machine_owner(&self, what: &str) -> Result<()> {
        match current_caller() {
            Some(Caller::Daemon) => Ok(()),
            Some(Caller::Wire {
                principal_id,
                host_role: HostRole::Owner,
            }) => match self.store.get_host_role(&principal_id).await {
                Ok(HostRole::Owner) => Ok(()),
                Ok(_) | Err(Error::NotFound(_)) => Err(forbidden(what)),
                Err(error) => Err(error),
            },
            Some(Caller::Wire { .. } | Caller::Agent { .. } | Caller::Executor { .. }) => {
                Err(forbidden(what))
            }
            None => {
                // Logs the unbound gate (and aborts under the e2e assert).
                crate::capability::gated_collaborator_caller(what)?;
                Err(forbidden(what))
            }
        }
    }

    fn remote_agents_enabled(&self) -> bool {
        self.settings_registry()
            .is_some_and(|registry| registry.snapshot().effective.remote_agents.enabled)
    }

    fn executor_live_status(&self, id: &ExecutorCredentialId) -> ExecutorLiveStatus {
        self.executor_liveness
            .get()
            .map(|liveness| liveness.status(id))
            .unwrap_or_default()
    }

    /// `executor.mint`: see [`intent_core::WorkspaceApi::executor_mint`].
    pub(crate) async fn executor_mint_op(&self, params: Value) -> Result<Value> {
        self.require_machine_owner("executor.mint").await?;
        let backend_id = text_param(&params, "backendId", true)?.unwrap_or_default();
        let backend_name = text_param(&params, "backendName", true)?.unwrap_or_default();
        let backend_fingerprint = text_param(&params, "backendFingerprint", false)?;
        if !self.remote_agents_enabled() {
            return Err(Error::RemoteAgentsDisabled {
                setting: ENABLED_SETTING,
            });
        }
        let platform = AgentPlatform::host().ok_or_else(|| {
            Error::Unsupported("remote agents run on Linux and macOS only".into())
        })?;
        let routes = self
            .machine_routes
            .get()
            .ok_or_else(|| Error::Unsupported("this daemon has no executor listener".into()))?
            .machine_routes()
            .await?;
        let daemon_id = self.store.daemon_id().await?;
        let token = random_token()?;
        let credential = ExecutorCredential {
            id: ExecutorCredentialId::new(),
            backend_id: backend_id.to_owned(),
            backend_name: backend_name.to_owned(),
            backend_fingerprint: backend_fingerprint.map(str::to_owned),
            created_at: now_iso(),
            last_used_at: None,
            revoked_at: None,
        };
        self.store
            .insert_executor_credential(&credential, &crate::invite_ops::hash_secret(&token))
            .await?;
        let mut machine = json!({
            "daemonId": daemon_id,
            "name": routes.name,
            "platform": platform,
            "port": routes.port,
            "hosts": routes.hosts,
            "fingerprint": routes.fingerprint,
        });
        if let Some(tc_address) = routes.tc_address {
            machine["tcAddress"] = json!(tc_address);
        }
        Ok(json!({ "credentialId": credential.id, "token": token, "machine": machine }))
    }

    /// `executor.list`: see [`intent_core::WorkspaceApi::executor_list`].
    pub(crate) async fn executor_list_op(&self) -> Result<Value> {
        self.require_machine_owner("executor.list").await?;
        let backends: Vec<Value> = self
            .store
            .list_active_executor_credentials()
            .await?
            .iter()
            .map(|credential| backend_row(credential, self.executor_live_status(&credential.id)))
            .collect();
        Ok(json!({ "backends": backends }))
    }

    /// `executor.revoke`: see [`intent_core::WorkspaceApi::executor_revoke`].
    /// Store-level: the token stops admitting at once. Closing the
    /// credential's live `/node/*` connections is wired by the executor
    /// link (it notifies on [`ExecutorRevocation::Revoked`]).
    pub(crate) async fn executor_revoke_op(&self, credential_id: &str) -> Result<Value> {
        self.require_machine_owner("executor.revoke").await?;
        if credential_id.is_empty() {
            return Err(Error::InvalidParams("credentialId is required".into()));
        }
        let id = ExecutorCredentialId::from(credential_id.to_owned());
        match self.store.revoke_executor_credential(&id).await? {
            ExecutorRevocation::NotFound => {
                return Err(Error::NotFound(format!(
                    "executor credential {credential_id}"
                )));
            }
            // Close the credential's live `/node/*` connections with `1008`.
            // Repeated on an already revoked credential so a retry after a
            // missed notice still closes them.
            ExecutorRevocation::Revoked | ExecutorRevocation::AlreadyRevoked => {
                self.notify_executor_revoked(&id);
            }
        }
        Ok(json!({ "ok": true }))
    }
}

#[cfg(test)]
mod tests;
