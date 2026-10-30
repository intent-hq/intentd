//! Daemon-to-daemon remote agents persistence (migration 0176).
//!
//! Machine side: the executor credentials this daemon minted for backends
//! (`executor_credential`, token stored only as a SHA-256 hash) and one link
//! binding per credential (`executor_binding`). Backend side: the machines
//! exposed to this daemon (`exposed_machine`); their tokens live in the
//! secret store, never here.
//!
//! This is not a wire authorization boundary: callers enforce the owner-only
//! method rules and the `/node/*` admission order.
use crate::Store;
use intent_core::{
    machines::{ExecutorCredential, ExposedMachine, RemoteMachineState},
    node_link::{BindDecision, LeaseBinding, MachineCapabilities},
    nodes::NodeCounter,
    now_iso,
    placement::AgentPlatform,
    Error, ExecutorCredentialId, Result,
};
use sqlx::{sqlite::SqliteRow, Row};

const CREDENTIAL_COLUMNS: &str =
    "id, backend_id, backend_name, backend_fingerprint, created_at, last_used_at, revoked_at";
const MACHINE_COLUMNS: &str = "id, name, hosts_json, port, tc_address, fingerprint, \
     credential_id, platform_json, capabilities_json, observed_at, state, added_at, \
     last_connected_at";

fn db(error: impl std::fmt::Display) -> Error {
    Error::Internal(format!("remote machine persistence: {error}"))
}
fn invalid(message: &str) -> Error {
    Error::InvalidParams(message.into())
}
fn encode(value: &impl serde::Serialize) -> Result<String> {
    serde_json::to_string(value).map_err(db)
}

/// What [`Store::revoke_executor_credential`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutorRevocation {
    /// The credential was active and is now revoked.
    Revoked,
    /// The credential was already revoked; nothing changed.
    AlreadyRevoked,
    /// No credential has this id.
    NotFound,
}

fn credential_row(row: &SqliteRow) -> ExecutorCredential {
    ExecutorCredential {
        id: ExecutorCredentialId::from(row.get::<String, _>("id")),
        backend_id: row.get("backend_id"),
        backend_name: row.get("backend_name"),
        backend_fingerprint: row.get("backend_fingerprint"),
        created_at: row.get("created_at"),
        last_used_at: row.get("last_used_at"),
        revoked_at: row.get("revoked_at"),
    }
}

fn machine_row(row: &SqliteRow) -> Result<ExposedMachine> {
    let hosts: Vec<String> = serde_json::from_str(row.get("hosts_json")).map_err(db)?;
    let platform: Option<AgentPlatform> = row
        .get::<Option<String>, _>("platform_json")
        .map(|json| serde_json::from_str(&json))
        .transpose()
        .map_err(db)?;
    let capabilities: Option<MachineCapabilities> = row
        .get::<Option<String>, _>("capabilities_json")
        .map(|json| serde_json::from_str(&json))
        .transpose()
        .map_err(db)?;
    let state = RemoteMachineState::parse(row.get("state"))
        .ok_or_else(|| db("unknown exposed machine state"))?;
    let port = u16::try_from(row.get::<i64, _>("port")).map_err(db)?;
    Ok(ExposedMachine {
        id: row.get("id"),
        name: row.get("name"),
        hosts,
        port,
        tc_address: row.get("tc_address"),
        fingerprint: row.get("fingerprint"),
        credential_id: ExecutorCredentialId::from(row.get::<String, _>("credential_id")),
        platform,
        capabilities,
        observed_at: row.get("observed_at"),
        state,
        added_at: row.get("added_at"),
        last_connected_at: row.get("last_connected_at"),
    })
}

fn resting(state: RemoteMachineState) -> Result<()> {
    if state == RemoteMachineState::Connected {
        return Err(invalid("a connected state is live and never persisted"));
    }
    Ok(())
}

impl Store {
    /// Record a newly minted executor credential, keyed by `token_hash`
    /// (hex SHA-256 of the token, never the token itself). Minting again for
    /// the same backend adds another credential.
    ///
    /// # Errors
    ///
    /// `InvalidParams` for an empty id, hash, backend id or name, or a
    /// credential that is already revoked; `Internal` on database failure
    /// (including a duplicate id or hash).
    pub async fn insert_executor_credential(
        &self,
        credential: &ExecutorCredential,
        token_hash: &str,
    ) -> Result<()> {
        if credential.id.as_str().is_empty()
            || token_hash.is_empty()
            || credential.backend_id.is_empty()
            || credential.backend_name.is_empty()
            || credential.revoked_at.is_some()
        {
            return Err(invalid("invalid executor credential"));
        }
        sqlx::query(
            "INSERT INTO executor_credential (id, token_hash, backend_id, backend_name, \
             backend_fingerprint, created_at, last_used_at, revoked_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, NULL)",
        )
        .bind(credential.id.as_str())
        .bind(token_hash)
        .bind(&credential.backend_id)
        .bind(&credential.backend_name)
        .bind(&credential.backend_fingerprint)
        .bind(&credential.created_at)
        .bind(&credential.last_used_at)
        .execute(self.write_pool())
        .await
        .map_err(db)?;
        Ok(())
    }

    /// Resolve an **active** credential by token hash and bump its
    /// `last_used_at` in one statement, so a concurrent revoke can never
    /// still admit it. `None` for an unknown or revoked hash.
    ///
    /// # Errors
    ///
    /// `Internal` on database failure.
    pub async fn resolve_active_executor_credential(
        &self,
        token_hash: &str,
    ) -> Result<Option<ExecutorCredential>> {
        let sql = format!(
            "UPDATE executor_credential SET last_used_at = ? \
             WHERE token_hash = ? AND revoked_at IS NULL RETURNING {CREDENTIAL_COLUMNS}"
        );
        let row = sqlx::query(&sql)
            .bind(now_iso())
            .bind(token_hash)
            .fetch_optional(self.write_pool())
            .await
            .map_err(db)?;
        Ok(row.as_ref().map(credential_row))
    }

    /// One credential by id, revoked or not.
    ///
    /// # Errors
    ///
    /// `Internal` on database failure.
    pub async fn get_executor_credential(
        &self,
        id: &ExecutorCredentialId,
    ) -> Result<Option<ExecutorCredential>> {
        let sql = format!("SELECT {CREDENTIAL_COLUMNS} FROM executor_credential WHERE id = ?");
        let row = sqlx::query(&sql)
            .bind(id.as_str())
            .fetch_optional(self.read_pool())
            .await
            .map_err(db)?;
        Ok(row.as_ref().map(credential_row))
    }

    /// The unrevoked credentials, oldest first (`executor.list`).
    ///
    /// # Errors
    ///
    /// `Internal` on database failure.
    pub async fn list_active_executor_credentials(&self) -> Result<Vec<ExecutorCredential>> {
        let sql = format!(
            "SELECT {CREDENTIAL_COLUMNS} FROM executor_credential WHERE revoked_at IS NULL \
             ORDER BY created_at, id"
        );
        let rows = sqlx::query(&sql)
            .fetch_all(self.read_pool())
            .await
            .map_err(db)?;
        Ok(rows.iter().map(credential_row).collect())
    }

    /// Revoke a credential. Idempotent; its binding stays so the revoked
    /// backend can never bind again with an older generation.
    ///
    /// # Errors
    ///
    /// `Internal` on database failure.
    pub async fn revoke_executor_credential(
        &self,
        id: &ExecutorCredentialId,
    ) -> Result<ExecutorRevocation> {
        let mut tx = self
            .write_pool()
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(db)?;
        let revoked: Option<Option<String>> =
            sqlx::query_scalar("SELECT revoked_at FROM executor_credential WHERE id = ?")
                .bind(id.as_str())
                .fetch_optional(&mut *tx)
                .await
                .map_err(db)?;
        let outcome = match revoked {
            None => ExecutorRevocation::NotFound,
            Some(Some(_)) => ExecutorRevocation::AlreadyRevoked,
            Some(None) => {
                sqlx::query("UPDATE executor_credential SET revoked_at = ? WHERE id = ?")
                    .bind(now_iso())
                    .bind(id.as_str())
                    .execute(&mut *tx)
                    .await
                    .map_err(db)?;
                ExecutorRevocation::Revoked
            }
        };
        tx.commit().await.map_err(db)?;
        Ok(outcome)
    }

    /// The current link binding of a credential, `None` before its first
    /// accepted bind.
    ///
    /// # Errors
    ///
    /// `Internal` on database failure or a corrupt row.
    pub async fn executor_binding(
        &self,
        credential_id: &ExecutorCredentialId,
    ) -> Result<Option<LeaseBinding>> {
        let row = sqlx::query(
            "SELECT head_id, node_id, lease_id, generation FROM executor_binding \
             WHERE credential_id = ?",
        )
        .bind(credential_id.as_str())
        .fetch_optional(self.read_pool())
        .await
        .map_err(db)?;
        row.map(|row| {
            Ok(LeaseBinding {
                head_id: row.get("head_id"),
                node_id: row.get("node_id"),
                lease_id: row.get("lease_id"),
                generation: NodeCounter(row.get::<String, _>("generation").parse().map_err(db)?),
            })
        })
        .transpose()
    }

    /// Decide a bind offer on the machine `node_id` for one credential and,
    /// when accepted, durably replace that credential's binding before
    /// returning. The offer's `headId` must be the backend the credential was
    /// minted for ([`BindDecision::OtherHead`] otherwise), then the
    /// single-node rule applies per credential: only a strictly higher
    /// generation replaces the current binding.
    ///
    /// # Errors
    ///
    /// `NotFound` when the credential is unknown or revoked (the caller
    /// refuses the link as unauthenticated); `Internal` on database failure.
    pub async fn bind_executor(
        &self,
        credential_id: &ExecutorCredentialId,
        node_id: &str,
        offered: &LeaseBinding,
    ) -> Result<BindDecision> {
        let mut tx = self
            .write_pool()
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(db)?;
        let backend_id: Option<String> = sqlx::query_scalar(
            "SELECT backend_id FROM executor_credential WHERE id = ? AND revoked_at IS NULL",
        )
        .bind(credential_id.as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        let backend_id = backend_id.ok_or_else(|| Error::NotFound("executor credential".into()))?;
        if offered.node_id == node_id && offered.head_id != backend_id {
            return Ok(BindDecision::OtherHead);
        }
        let current = sqlx::query(
            "SELECT head_id, node_id, lease_id, generation FROM executor_binding \
             WHERE credential_id = ?",
        )
        .bind(credential_id.as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?
        .map(|row| {
            Ok::<_, Error>(LeaseBinding {
                head_id: row.get("head_id"),
                node_id: row.get("node_id"),
                lease_id: row.get("lease_id"),
                generation: NodeCounter(row.get::<String, _>("generation").parse().map_err(db)?),
            })
        })
        .transpose()?;
        let decision = LeaseBinding::decide(node_id, current.as_ref(), offered);
        if decision == BindDecision::Accept {
            sqlx::query(
                "INSERT INTO executor_binding (credential_id, head_id, node_id, lease_id, \
                 generation, bound_at) VALUES (?, ?, ?, ?, ?, ?) \
                 ON CONFLICT(credential_id) DO UPDATE SET head_id = excluded.head_id, \
                 node_id = excluded.node_id, lease_id = excluded.lease_id, \
                 generation = excluded.generation, bound_at = excluded.bound_at",
            )
            .bind(credential_id.as_str())
            .bind(&offered.head_id)
            .bind(&offered.node_id)
            .bind(&offered.lease_id)
            .bind(offered.generation.0.to_string())
            .bind(now_iso())
            .execute(&mut *tx)
            .await
            .map_err(db)?;
            tx.commit().await.map_err(db)?;
        }
        Ok(decision)
    }

    /// Save an exposed machine after a successful probe, replacing any row
    /// with the same id. Returns the replaced row's credential when it
    /// differs from the new one, for the caller to revoke on the machine.
    ///
    /// # Errors
    ///
    /// `InvalidParams` for an empty id, name, fingerprint or credential, no
    /// hosts, port 0 or a `connected` state; `Internal` on database failure.
    pub async fn save_exposed_machine(
        &self,
        machine: &ExposedMachine,
    ) -> Result<Option<ExecutorCredentialId>> {
        if machine.id.is_empty()
            || machine.name.is_empty()
            || machine.fingerprint.is_empty()
            || machine.credential_id.as_str().is_empty()
            || machine.hosts.is_empty()
            || machine.port == 0
        {
            return Err(invalid("invalid exposed machine"));
        }
        resting(machine.state)?;
        let mut tx = self
            .write_pool()
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(db)?;
        let previous: Option<String> =
            sqlx::query_scalar("SELECT credential_id FROM exposed_machine WHERE id = ?")
                .bind(&machine.id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(db)?;
        sqlx::query(
            "INSERT OR REPLACE INTO exposed_machine (id, name, hosts_json, port, tc_address, \
             fingerprint, credential_id, platform_json, capabilities_json, observed_at, state, \
             added_at, last_connected_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&machine.id)
        .bind(&machine.name)
        .bind(encode(&machine.hosts)?)
        .bind(i64::from(machine.port))
        .bind(&machine.tc_address)
        .bind(&machine.fingerprint)
        .bind(machine.credential_id.as_str())
        .bind(machine.platform.as_ref().map(encode).transpose()?)
        .bind(machine.capabilities.as_ref().map(encode).transpose()?)
        .bind(&machine.observed_at)
        .bind(machine.state.as_str())
        .bind(&machine.added_at)
        .bind(&machine.last_connected_at)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(previous
            .filter(|previous| previous != machine.credential_id.as_str())
            .map(ExecutorCredentialId::from))
    }

    /// One exposed machine by id.
    ///
    /// # Errors
    ///
    /// `Internal` on database failure or a corrupt row.
    pub async fn get_exposed_machine(&self, id: &str) -> Result<Option<ExposedMachine>> {
        let sql = format!("SELECT {MACHINE_COLUMNS} FROM exposed_machine WHERE id = ?");
        let row = sqlx::query(&sql)
            .bind(id)
            .fetch_optional(self.read_pool())
            .await
            .map_err(db)?;
        row.as_ref().map(machine_row).transpose()
    }

    /// Every exposed machine, oldest first.
    ///
    /// # Errors
    ///
    /// `Internal` on database failure or a corrupt row.
    pub async fn list_exposed_machines(&self) -> Result<Vec<ExposedMachine>> {
        let sql = format!("SELECT {MACHINE_COLUMNS} FROM exposed_machine ORDER BY added_at, id");
        let rows = sqlx::query(&sql)
            .fetch_all(self.read_pool())
            .await
            .map_err(db)?;
        rows.iter().map(machine_row).collect()
    }

    /// Remove an exposed machine. Returns its credential for the caller to
    /// revoke on the machine; `None` for an unknown id (a no-op).
    ///
    /// # Errors
    ///
    /// `Internal` on database failure.
    pub async fn remove_exposed_machine(&self, id: &str) -> Result<Option<ExecutorCredentialId>> {
        let credential: Option<String> =
            sqlx::query_scalar("DELETE FROM exposed_machine WHERE id = ? RETURNING credential_id")
                .bind(id)
                .fetch_optional(self.write_pool())
                .await
                .map_err(db)?;
        Ok(credential.map(ExecutorCredentialId::from))
    }

    /// Persist a machine's resting state. Returns whether the machine exists.
    ///
    /// # Errors
    ///
    /// `InvalidParams` for `connected`; `Internal` on database failure.
    pub async fn set_exposed_machine_state(
        &self,
        id: &str,
        state: RemoteMachineState,
    ) -> Result<bool> {
        resting(state)?;
        let done = sqlx::query("UPDATE exposed_machine SET state = ? WHERE id = ?")
            .bind(state.as_str())
            .bind(id)
            .execute(self.write_pool())
            .await
            .map_err(db)?;
        Ok(done.rows_affected() > 0)
    }

    /// After an accepted bind: replace the stored routes with the ones the
    /// machine reported, and stamp `last_connected_at`. The state is not
    /// touched; `connected` is a live overlay. Returns whether the machine
    /// exists.
    ///
    /// # Errors
    ///
    /// `InvalidParams` for no hosts; `Internal` on database failure.
    pub async fn record_exposed_machine_connection(
        &self,
        id: &str,
        hosts: &[String],
        tc_address: Option<&str>,
        connected_at: &str,
    ) -> Result<bool> {
        if hosts.is_empty() {
            return Err(invalid("an exposed machine needs at least one host"));
        }
        let done = sqlx::query(
            "UPDATE exposed_machine SET hosts_json = ?, tc_address = ?, last_connected_at = ? \
             WHERE id = ?",
        )
        .bind(encode(&hosts)?)
        .bind(tc_address)
        .bind(connected_at)
        .bind(id)
        .execute(self.write_pool())
        .await
        .map_err(db)?;
        Ok(done.rows_affected() > 0)
    }

    /// Cache a machine's capabilities report, its platform and `observedAt`,
    /// so a disconnected machine still shows what it had. Returns whether the
    /// machine exists.
    ///
    /// # Errors
    ///
    /// `Internal` on database failure.
    pub async fn record_exposed_machine_capabilities(
        &self,
        id: &str,
        capabilities: &MachineCapabilities,
    ) -> Result<bool> {
        let done = sqlx::query(
            "UPDATE exposed_machine SET capabilities_json = ?, platform_json = ?, \
             observed_at = ? WHERE id = ?",
        )
        .bind(encode(capabilities)?)
        .bind(encode(&capabilities.platform)?)
        .bind(&capabilities.observed_at)
        .bind(id)
        .execute(self.write_pool())
        .await
        .map_err(db)?;
        Ok(done.rows_affected() > 0)
    }
}

#[cfg(test)]
mod tests;
