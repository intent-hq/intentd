//! Internal node persistence. All mutations serialize on the write pool and
//! recheck the durable lease owner. This is not a wire authorization boundary;
//! callers must still apply existing principal/agent/workspace authorization.
//! Journal ingestion owns ack advancement in its transcript transaction. There
//! is deliberately no standalone watermark setter here.
use crate::Store;
use intent_core::{
    nodes::{
        CheckpointOutcome, CheckpointReceipt, LeaseFence, LeaseOwner, LeaseState, NodeAssignment,
        NodeCheckpoint, NodeCounter, NodeKind, NodeLease, NodeRecord, NodeState,
    },
    now_iso, AgentId, Error, Result, WorkspaceId,
};
use serde::{de::DeserializeOwned, Serialize};
use sqlx::{Row, SqliteConnection};

const MAX_SAFE_INTEGER: u64 = (1 << 53) - 1;

fn db(error: impl std::fmt::Display) -> Error {
    Error::Internal(format!("node persistence: {error}"))
}
fn invalid(message: &str) -> Error {
    Error::InvalidParams(message.into())
}
fn encode(value: &impl Serialize) -> Result<String> {
    serde_json::to_string(value).map_err(db)
}
fn decode<T: DeserializeOwned>(json: &str) -> Result<T> {
    serde_json::from_str(json).map_err(db)
}
fn increment(counter: NodeCounter) -> Result<NodeCounter> {
    counter
        .0
        .checked_add(1)
        .map(NodeCounter)
        .ok_or_else(|| invalid("node counter exhausted"))
}

async fn lease(conn: &mut SqliteConnection, owner: &LeaseOwner) -> Result<NodeLease> {
    let row = sqlx::query("SELECT record_json, ack_seq FROM node_lease WHERE id = ?")
        .bind(&owner.lease_id)
        .fetch_optional(conn)
        .await
        .map_err(db)?
        .ok_or_else(|| Error::NotFound("lease".into()))?;
    let mut record: NodeLease = decode(row.get("record_json"))?;
    if record.owner != *owner {
        return Err(Error::NotFound("lease owner".into()));
    }
    record.ack_seq = NodeCounter(row.get::<&str, _>("ack_seq").parse().map_err(db)?);
    Ok(record)
}
async fn save_lease(conn: &mut SqliteConnection, record: &NodeLease) -> Result<()> {
    sqlx::query("UPDATE node_lease SET record_json = ?, terminal = ? WHERE id = ?")
        .bind(encode(record)?)
        .bind(record.state.is_terminal())
        .bind(&record.owner.lease_id)
        .execute(conn)
        .await
        .map_err(db)?;
    Ok(())
}
async fn node(conn: &mut SqliteConnection, id: &str, head_id: &str) -> Result<NodeRecord> {
    let json: String =
        sqlx::query_scalar("SELECT record_json FROM execution_node WHERE id = ? AND head_id = ?")
            .bind(id)
            .bind(head_id)
            .fetch_optional(conn)
            .await
            .map_err(db)?
            .ok_or_else(|| Error::NotFound("node".into()))?;
    decode(&json)
}
async fn save_node(conn: &mut SqliteConnection, record: &NodeRecord) -> Result<()> {
    sqlx::query("UPDATE execution_node SET record_json = ? WHERE id = ?")
        .bind(encode(record)?)
        .bind(&record.id)
        .execute(conn)
        .await
        .map_err(db)?;
    Ok(())
}
async fn assignment(conn: &mut SqliteConnection, id: &AgentId) -> Result<Option<NodeAssignment>> {
    let json: Option<String> =
        sqlx::query_scalar("SELECT record_json FROM node_assignment WHERE agent_id = ?")
            .bind(id.as_str())
            .fetch_optional(conn)
            .await
            .map_err(db)?;
    json.as_deref().map(decode).transpose()
}
fn dispatchable(lease: &NodeLease) -> Result<()> {
    if lease.release_requested
        || matches!(
            lease.state,
            LeaseState::Acquiring | LeaseState::Released | LeaseState::Lost
        )
    {
        return Err(invalid("lease has no dispatch authority"));
    }
    Ok(())
}
fn static_only(node: &NodeRecord) -> Result<()> {
    if node.kind != NodeKind::Static {
        return Err(invalid("built-in local node cannot be released"));
    }
    Ok(())
}

impl Store {
    /// Atomically persist an enrollment binding after authenticated negotiation.
    /// Tokens belong in the secret store, not this repository. Enrollment
    /// request receipts are the responsibility of method/link integration.
    /// # Errors
    /// Rejects malformed/mismatched records, reused identity or database errors.
    pub async fn enroll_node(&self, record: &NodeRecord, initial: &NodeLease) -> Result<()> {
        let owner = &initial.owner;
        if record.id.is_empty()
            || record.head_id.is_empty()
            || record.node_identity.is_empty()
            || owner.node_id != record.id
            || owner.head_id != record.head_id
            || owner.node_identity != record.node_identity
            || owner.lease_id.is_empty()
            || uuid::Uuid::parse_str(&owner.incarnation).is_err()
            || initial.state != LeaseState::Acquiring
            || initial.release_requested
            || initial.fence.is_some()
            || initial.ack_seq != NodeCounter(0)
            || initial.link_generation != NodeCounter(0)
        {
            return Err(invalid("invalid initial node/lease binding"));
        }
        if record.capacity.memory_budget_bytes > MAX_SAFE_INTEGER
            || record.capacity.used_memory_bytes > MAX_SAFE_INTEGER
        {
            return Err(invalid("capacity exceeds safe JSON integer"));
        }
        match record.kind {
            NodeKind::Static => {
                if !record
                    .endpoint
                    .as_ref()
                    .is_some_and(|s| s.starts_with("wss://") && s.ends_with("/node"))
                    || !record.certificate_sha256.as_ref().is_some_and(|s| {
                        s.len() == 64
                            && s.bytes()
                                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                    })
                {
                    return Err(invalid("static node requires pinned WSS endpoint"));
                }
            }
            NodeKind::Local => {
                if record.endpoint.is_some() || record.certificate_sha256.is_some() {
                    return Err(invalid("local node has no network credential"));
                }
            }
        }
        let mut tx = self.write_pool().begin().await.map_err(db)?;
        sqlx::query("INSERT INTO execution_node (id, head_id, node_identity, record_json) VALUES (?, ?, ?, ?)")
            .bind(&record.id).bind(&record.head_id).bind(&record.node_identity).bind(encode(record)?)
            .execute(&mut *tx).await.map_err(db)?;
        sqlx::query("INSERT INTO node_lease (id, node_id, record_json) VALUES (?, ?, ?)")
            .bind(&owner.lease_id)
            .bind(&record.id)
            .bind(encode(initial)?)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)
    }

    /// Enumerate durable registrations for one head installation on restart.
    /// These private records must be projected before public inventory delivery.
    /// # Errors
    /// Returns database or decoding errors.
    pub async fn list_owned_nodes(&self, head_id: &str) -> Result<Vec<NodeRecord>> {
        let rows: Vec<String> = sqlx::query_scalar(
            "SELECT record_json FROM execution_node WHERE head_id = ? ORDER BY id",
        )
        .bind(head_id)
        .fetch_all(self.read_pool())
        .await
        .map_err(db)?;
        rows.iter().map(|json| decode(json)).collect()
    }

    /// Enumerate all leases, including terminal history, for the bound head.
    /// # Errors
    /// Returns database or decoding errors.
    pub async fn list_owned_node_leases(&self, head_id: &str) -> Result<Vec<NodeLease>> {
        let rows = sqlx::query("SELECT l.record_json, l.ack_seq FROM node_lease l JOIN execution_node n ON n.id = l.node_id WHERE n.head_id = ? ORDER BY l.id")
            .bind(head_id).fetch_all(self.read_pool()).await.map_err(db)?;
        rows.iter()
            .map(|row| {
                let mut record: NodeLease = decode(row.get("record_json"))?;
                record.ack_seq = NodeCounter(row.get::<&str, _>("ack_seq").parse().map_err(db)?);
                Ok(record)
            })
            .collect()
    }

    /// Read private inventory for the bound head installation.
    /// # Errors
    /// Returns not-found for a different owner or a database failure.
    pub async fn get_node(&self, id: &str, head_id: &str) -> Result<NodeRecord> {
        node(
            &mut *self.read_pool().acquire().await.map_err(db)?,
            id,
            head_id,
        )
        .await
    }

    /// Read a lease only for its exact durable ownership tuple.
    /// # Errors
    /// Returns not-found for stale ownership or a database failure.
    pub async fn get_node_lease(&self, owner: &LeaseOwner) -> Result<NodeLease> {
        lease(&mut *self.read_pool().acquire().await.map_err(db)?, owner).await
    }

    /// Apply a reconciled nonterminal state. Terminal states require a fence.
    /// # Errors
    /// Rejects invalid transitions, released leases or stale ownership.
    pub async fn transition_node_lease(
        &self,
        owner: &LeaseOwner,
        next: LeaseState,
    ) -> Result<NodeLease> {
        let mut tx = self.write_pool().begin().await.map_err(db)?;
        let mut record = lease(&mut tx, owner).await?;
        if next.is_terminal() || record.release_requested || !record.state.can_transition_to(next) {
            return Err(invalid("invalid lease transition"));
        }
        record.state = next;
        save_lease(&mut tx, &record).await?;
        tx.commit().await.map_err(db)?;
        Ok(record)
    }

    /// Persist a greater generation before dialing. Releasing leases can still
    /// reconnect to confirm stops, but cannot dispatch new work.
    /// # Errors
    /// Rejects terminal/stale leases, exhausted counters or database errors.
    pub async fn next_node_link_generation(&self, owner: &LeaseOwner) -> Result<NodeCounter> {
        let mut tx = self.write_pool().begin().await.map_err(db)?;
        let mut record = lease(&mut tx, owner).await?;
        if record.state.is_terminal() {
            return Err(invalid("terminal lease"));
        }
        record.link_generation = increment(record.link_generation)?;
        save_lease(&mut tx, &record).await?;
        tx.commit().await.map_err(db)?;
        Ok(record.link_generation)
    }

    /// Drain the static registration and revoke new dispatch, retaining every
    /// assignment/reservation until confirmed stop or offline-budget fencing.
    /// # Errors
    /// Rejects local/stale ownership or database errors.
    pub async fn request_node_lease_release(&self, owner: &LeaseOwner) -> Result<NodeLease> {
        let mut tx = self.write_pool().begin().await.map_err(db)?;
        let mut record = lease(&mut tx, owner).await?;
        let mut host = node(&mut tx, &owner.node_id, &owner.head_id).await?;
        static_only(&host)?;
        if !record.state.is_terminal() {
            record.release_requested = true;
            host.draining = true;
            save_node(&mut tx, &host).await?;
            save_lease(&mut tx, &record).await?;
        }
        tx.commit().await.map_err(db)?;
        Ok(record)
    }

    /// Commit externally verified stop/fence evidence, then relinquish managed
    /// assignment authority. Never invokes a host shutdown or deletes files.
    /// # Errors
    /// Rejects local/stale leases, invalid terminal transitions or DB errors.
    pub async fn finish_node_lease(
        &self,
        owner: &LeaseOwner,
        terminal: LeaseState,
        fence: LeaseFence,
    ) -> Result<NodeLease> {
        if !terminal.is_terminal() || fence.recorded_at.is_empty() {
            return Err(invalid("terminal lease requires fence evidence"));
        }
        let mut tx = self.write_pool().begin().await.map_err(db)?;
        let mut record = lease(&mut tx, owner).await?;
        let mut host = node(&mut tx, &owner.node_id, &owner.head_id).await?;
        static_only(&host)?;
        if record.state.is_terminal() {
            return Ok(record);
        }
        if !record.state.can_transition_to(terminal)
            || (terminal == LeaseState::Released && !record.release_requested)
        {
            return Err(invalid("release must be requested before completion"));
        }
        record.state = terminal;
        record.fence = Some(fence);
        host.draining = true;
        let rows = sqlx::query("SELECT record_json FROM node_assignment WHERE lease_id = ?")
            .bind(&owner.lease_id)
            .fetch_all(&mut *tx)
            .await
            .map_err(db)?;
        for row in rows {
            let mut placed: NodeAssignment = decode(row.get("record_json"))?;
            placed.active = false;
            sqlx::query("UPDATE node_assignment SET record_json = ? WHERE agent_id = ?")
                .bind(encode(&placed)?)
                .bind(placed.agent_id.as_str())
                .execute(&mut *tx)
                .await
                .map_err(db)?;
        }
        host.capacity.reserved_agents = 0;
        host.capacity.exclusive_reserved = false;
        save_node(&mut tx, &host).await?;
        save_lease(&mut tx, &record).await?;
        tx.commit().await.map_err(db)?;
        Ok(record)
    }

    /// Admit a new run with a store-allocated epoch. The template's epoch is
    /// ignored. Repeating the same run is a no-op only for an identical binding.
    /// Admission/capacity and public caller authorization belong to services.
    /// # Errors
    /// Rejects stale/unfenced ownership, changed workspace, deleted agents and DB errors.
    pub async fn assign_node_run(&self, template: &NodeAssignment) -> Result<NodeAssignment> {
        let mut tx = self.write_pool().begin().await.map_err(db)?;
        let current_lease = lease(&mut tx, &template.owner).await?;
        dispatchable(&current_lease)?;
        let host = node(&mut tx, &template.owner.node_id, &template.owner.head_id).await?;
        if host.draining
            || host.state != NodeState::Ready
            || uuid::Uuid::parse_str(&template.run_id).is_err()
        {
            return Err(invalid("node unavailable or invalid run"));
        }
        let workspace: Option<String> =
            sqlx::query_scalar("SELECT workspace_id FROM agent_session WHERE id = ?")
                .bind(template.agent_id.as_str())
                .fetch_optional(&mut *tx)
                .await
                .map_err(db)?;
        if workspace.as_deref() != Some(template.workspace_id.as_str()) {
            return Err(Error::NotFound("agent workspace".into()));
        }
        let old = assignment(&mut tx, &template.agent_id).await?;
        let mut admitted = template.clone();
        admitted.active = true;
        admitted.tombstoned = false;
        admitted.assignment_epoch = NodeCounter(1);
        if let Some(old) = old {
            if old.tombstoned
                || old.workspace_id != template.workspace_id
                || old.owner.head_id != template.owner.head_id
            {
                return Err(invalid("assignment ownership is tombstoned or mismatched"));
            }
            if old.run_id == template.run_id {
                admitted.assignment_epoch = old.assignment_epoch;
                if old != admitted {
                    return Err(invalid("run identity reused"));
                }
                return Ok(old);
            }
            if old.active {
                return Err(invalid("previous run must be stopped before reassignment"));
            }
            admitted.assignment_epoch = increment(old.assignment_epoch)?;
        }
        if let Some(baseline) = &admitted.inherited_checkpoint_id {
            let json: Option<String> =
                sqlx::query_scalar("SELECT record_json FROM node_checkpoint WHERE id = ?")
                    .bind(baseline)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(db)?;
            let checkpoint: NodeCheckpoint =
                decode(&json.ok_or_else(|| Error::NotFound("inherited checkpoint".into()))?)?;
            if checkpoint.workspace_id != admitted.workspace_id
                || Some(&checkpoint.agent_id) != admitted.merge_target_agent_id.as_ref()
            {
                return Err(Error::NotFound("inherited checkpoint owner".into()));
            }
        }
        sqlx::query("INSERT INTO node_assignment (agent_id, workspace_id, lease_id, record_json) VALUES (?, ?, ?, ?) ON CONFLICT(agent_id) DO UPDATE SET lease_id = excluded.lease_id, record_json = excluded.record_json, capture_revision = '0'")
            .bind(admitted.agent_id.as_str()).bind(admitted.workspace_id.as_str()).bind(&admitted.owner.lease_id).bind(encode(&admitted)?)
            .execute(&mut *tx).await.map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(admitted)
    }

    /// Resolve only the persisted workspace/agent scope, including tombstones.
    /// # Errors
    /// Returns not-found for scope mismatch or a database failure.
    pub async fn get_node_assignment(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
    ) -> Result<NodeAssignment> {
        let record = assignment(
            &mut *self.read_pool().acquire().await.map_err(db)?,
            agent_id,
        )
        .await?
        .ok_or_else(|| Error::NotFound("assignment".into()))?;
        if record.workspace_id != *workspace_id {
            return Err(Error::NotFound("assignment workspace".into()));
        }
        Ok(record)
    }

    /// Revoke one run after confirmed termination, or permanently tombstone its
    /// ref ownership on discard/deletion. The epoch and checkpoint stay durable.
    /// # Errors
    /// Rejects stale run identity or DB errors.
    pub async fn stop_node_assignment(
        &self,
        expected: &NodeAssignment,
        tombstone: bool,
    ) -> Result<()> {
        let mut tx = self.write_pool().begin().await.map_err(db)?;
        lease(&mut tx, &expected.owner).await?;
        let mut record = assignment(&mut tx, &expected.agent_id)
            .await?
            .ok_or_else(|| Error::NotFound("assignment".into()))?;
        check_assignment(&record, expected)?;
        record.active = false;
        record.tombstoned |= tombstone;
        sqlx::query("UPDATE node_assignment SET record_json = ? WHERE agent_id = ?")
            .bind(encode(&record)?)
            .bind(record.agent_id.as_str())
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)
    }

    /// Check a repository grant without upgrading the caller to daemon authority.
    /// # Errors
    /// Rejects stale/stopped/tombstoned execution or out-of-scope repositories.
    pub async fn authorize_node_repo(
        &self,
        expected: &NodeAssignment,
        repo_key: &str,
    ) -> Result<()> {
        let mut conn = self.read_pool().acquire().await.map_err(db)?;
        let current_lease = lease(&mut conn, &expected.owner).await?;
        dispatchable(&current_lease)?;
        let current = assignment(&mut conn, &expected.agent_id)
            .await?
            .ok_or_else(|| Error::NotFound("assignment".into()))?;
        check_assignment(&current, expected)?;
        if !current.active
            || current.tombstoned
            || !current.repo_keys.iter().any(|key| key == repo_key)
        {
            return Err(Error::NotFound("repository grant".into()));
        }
        Ok(())
    }

    /// Allocate a revision before capture on a store-owning local node. Remote
    /// node persistence must provide the same operation in its private store.
    /// # Errors
    /// Rejects stale/stopped ownership, counter exhaustion or DB errors.
    pub async fn next_node_capture_revision(
        &self,
        expected: &NodeAssignment,
    ) -> Result<NodeCounter> {
        let mut tx = self.write_pool().begin().await.map_err(db)?;
        dispatchable(&lease(&mut tx, &expected.owner).await?)?;
        let current = assignment(&mut tx, &expected.agent_id)
            .await?
            .ok_or_else(|| Error::NotFound("assignment".into()))?;
        check_assignment(&current, expected)?;
        if !current.active || current.tombstoned {
            return Err(invalid("inactive assignment"));
        }
        let value: String =
            sqlx::query_scalar("SELECT capture_revision FROM node_assignment WHERE agent_id = ?")
                .bind(expected.agent_id.as_str())
                .fetch_one(&mut *tx)
                .await
                .map_err(db)?;
        let next = increment(NodeCounter(value.parse().map_err(db)?))?;
        sqlx::query("UPDATE node_assignment SET capture_revision = ? WHERE agent_id = ?")
            .bind(next.0.to_string())
            .bind(expected.agent_id.as_str())
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(next)
    }

    /// Persist metadata ONLY after the caller validates and fsyncs all immutable
    /// objects/manifest anchors. The transaction revalidates current ownership,
    /// ack cut and numeric freshness. It emits no event and updates no Git ref.
    /// # Errors
    /// Rejects stale owners, unacked cuts, reused revision/UUID and DB errors.
    pub async fn commit_node_checkpoint(
        &self,
        checkpoint: &NodeCheckpoint,
    ) -> Result<CheckpointReceipt> {
        if checkpoint.capture_revision == NodeCounter(0)
            || uuid::Uuid::parse_str(&checkpoint.id).is_err()
            || checkpoint.manifest_sha256.len() != 64
            || !checkpoint
                .manifest_sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(invalid("checkpoint-invalid"));
        }
        let mut tx = self.write_pool().begin().await.map_err(db)?;
        let owned_lease = lease(&mut tx, &checkpoint.owner).await?;
        let existing =
            sqlx::query("SELECT record_json, receipt_json FROM node_checkpoint WHERE id = ?")
                .bind(&checkpoint.id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(db)?;
        if let Some(row) = existing {
            let stored: NodeCheckpoint = decode(row.get("record_json"))?;
            let mut retry = checkpoint.clone();
            retry.committed_at.clone_from(&stored.committed_at);
            if stored != retry {
                return Err(invalid("checkpoint-invalid: UUID reused"));
            }
            return decode(row.get("receipt_json"));
        }
        dispatchable(&owned_lease)?;
        let assigned = assignment(&mut tx, &checkpoint.agent_id)
            .await?
            .ok_or_else(|| invalid("stale-checkpoint-owner"))?;
        if !assigned.active
            || assigned.tombstoned
            || assigned.owner != checkpoint.owner
            || assigned.workspace_id != checkpoint.workspace_id
            || assigned.run_id != checkpoint.run_id
            || assigned.assignment_epoch != checkpoint.assignment_epoch
        {
            return Err(invalid("stale-checkpoint-owner"));
        }
        if checkpoint.journal_seq > owned_lease.ack_seq {
            return Err(invalid("checkpoint cut is not acknowledged"));
        }
        let pair_used: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM node_checkpoint WHERE agent_id = ? AND assignment_epoch = ? AND capture_revision = ?)")
            .bind(checkpoint.agent_id.as_str()).bind(checkpoint.assignment_epoch.0.to_string()).bind(checkpoint.capture_revision.0.to_string())
            .fetch_one(&mut *tx).await.map_err(db)?;
        if pair_used {
            return Err(invalid("checkpoint-invalid: capture revision reused"));
        }
        let previous: Option<String> = sqlx::query_scalar("SELECT c.record_json FROM node_assignment a JOIN node_checkpoint c ON c.id = a.current_checkpoint_id WHERE a.agent_id = ?")
            .bind(checkpoint.agent_id.as_str()).fetch_optional(&mut *tx).await.map_err(db)?;
        let previous: Option<NodeCheckpoint> = previous.as_deref().map(decode).transpose()?;
        let advanced = previous.as_ref().is_none_or(|p| {
            (checkpoint.assignment_epoch, checkpoint.capture_revision)
                > (p.assignment_epoch, p.capture_revision)
        });
        let receipt = CheckpointReceipt {
            checkpoint_id: checkpoint.id.clone(),
            outcome: if advanced {
                CheckpointOutcome::Advanced
            } else {
                CheckpointOutcome::Historical
            },
            current_checkpoint_id: if advanced {
                checkpoint.id.clone()
            } else {
                previous.map_or_else(|| checkpoint.id.clone(), |p| p.id)
            },
        };
        let mut stored = checkpoint.clone();
        stored.committed_at = now_iso();
        sqlx::query("INSERT INTO node_checkpoint (id, agent_id, assignment_epoch, capture_revision, record_json, receipt_json) VALUES (?, ?, ?, ?, ?, ?)")
            .bind(&stored.id).bind(stored.agent_id.as_str()).bind(stored.assignment_epoch.0.to_string()).bind(stored.capture_revision.0.to_string())
            .bind(encode(&stored)?).bind(encode(&receipt)?).execute(&mut *tx).await.map_err(db)?;
        if advanced {
            sqlx::query("UPDATE node_assignment SET current_checkpoint_id = ? WHERE agent_id = ?")
                .bind(&stored.id)
                .bind(stored.agent_id.as_str())
                .execute(&mut *tx)
                .await
                .map_err(db)?;
        }
        tx.commit().await.map_err(db)?;
        Ok(receipt)
    }

    /// Read the last successful checkpoint; it may belong to the previous run
    /// until the new epoch successfully captures its state.
    /// # Errors
    /// Returns not-found on workspace mismatch or a database failure.
    pub async fn current_node_checkpoint(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
    ) -> Result<Option<NodeCheckpoint>> {
        self.get_node_assignment(agent_id, workspace_id).await?;
        let json: Option<String> = sqlx::query_scalar("SELECT c.record_json FROM node_assignment a JOIN node_checkpoint c ON c.id = a.current_checkpoint_id WHERE a.agent_id = ? AND a.workspace_id = ?")
            .bind(agent_id.as_str()).bind(workspace_id.as_str()).fetch_optional(self.read_pool()).await.map_err(db)?;
        json.as_deref().map(decode).transpose()
    }
}

fn check_assignment(current: &NodeAssignment, expected: &NodeAssignment) -> Result<()> {
    if current.agent_id != expected.agent_id
        || current.workspace_id != expected.workspace_id
        || current.owner != expected.owner
        || current.run_id != expected.run_id
        || current.assignment_epoch != expected.assignment_epoch
    {
        return Err(Error::NotFound("assignment owner".into()));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
