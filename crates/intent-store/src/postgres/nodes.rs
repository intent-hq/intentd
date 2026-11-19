//! Execution nodes, leases, assignments and checkpoints, remote agents,
//! final stops, node invites and executor
//! credentials/bindings/exposed machines (Postgres). Mirrors `node_repo.rs`,
//! `node_invite_repo.rs` and `executor_repo.rs`; every row belongs to the
//! store's tenant.

use intent_core::{
    machines::{CloudMachine, ExecutorCredential, ExposedMachine, RemoteMachineState},
    node_link::{BindDecision, LeaseBinding, MachineCapabilities},
    nodes::{
        CheckpointOutcome, CheckpointReceipt, LeaseFence, LeaseFenceKind, LeaseOwner, LeaseState,
        NodeArch, NodeAssignment, NodeCheckpoint, NodeCounter, NodeLease, NodeOs, NodeRecord,
        NodeState,
    },
    now_iso,
    placement::AgentPlatform,
    remote_launch::{FinalStopReason, RemoteAgentRecord, RemoteFinalStopRecord},
    AgentId, Error, ExecutorCredentialId, Result, WorkspaceId,
};
use sqlx::{postgres::PgRow, PgConnection, Row};

use super::{db, PgStore, WriteTx};
use crate::executor_repo::{has_route, resting};
use crate::node_invite_repo::constant_time_eq;
use crate::node_repo::{
    check_assignment, decode, dispatchable, encode, increment, static_only, validate_enrollment,
};
use crate::{ExecutorRevocation, NodeInvite, NodeInviteRefusal, NodeInviteRevocation};

fn col<'r, T>(row: &'r PgRow, name: &str) -> Result<T>
where
    T: sqlx::Decode<'r, sqlx::Postgres> + sqlx::Type<sqlx::Postgres>,
{
    row.try_get::<T, _>(name)
        .map_err(|e| Error::Internal(format!("column {name}: {e}")))
}

fn invalid(message: &str) -> Error {
    Error::InvalidParams(message.into())
}

fn counter(row: &PgRow, name: &str) -> Result<NodeCounter> {
    Ok(NodeCounter(col::<String>(row, name)?.parse().map_err(db)?))
}

fn lease_row(row: &PgRow) -> Result<NodeLease> {
    let mut record: NodeLease = decode(&col::<String>(row, "record_json")?)?;
    record.ack_seq = counter(row, "ack_seq")?;
    Ok(record)
}

impl PgStore {
    async fn lease(&self, conn: &mut PgConnection, owner: &LeaseOwner) -> Result<NodeLease> {
        let row = sqlx::query(
            "SELECT record_json, ack_seq FROM node_lease WHERE tenant_id = $1 AND id = $2",
        )
        .bind(self.tenant())
        .bind(&owner.lease_id)
        .fetch_optional(conn)
        .await
        .map_err(db)?
        .ok_or_else(|| Error::NotFound("lease".into()))?;
        let record = lease_row(&row)?;
        if record.owner != *owner {
            return Err(Error::NotFound("lease owner".into()));
        }
        Ok(record)
    }

    async fn save_lease(&self, tx: &mut WriteTx, record: &NodeLease) -> Result<()> {
        sqlx::query(
            "UPDATE node_lease SET record_json = $2, terminal = $3 WHERE tenant_id = $1 AND id = $4",
        )
        .bind(self.tenant())
        .bind(encode(record)?)
        .bind(i64::from(record.state.is_terminal()))
        .bind(&record.owner.lease_id)
        .execute(&mut **tx)
        .await
        .map_err(db)?;
        Ok(())
    }

    async fn node(&self, conn: &mut PgConnection, id: &str, head_id: &str) -> Result<NodeRecord> {
        let json: String = sqlx::query_scalar(
            "SELECT record_json FROM execution_node WHERE tenant_id = $1 AND id = $2 AND head_id = $3",
        )
        .bind(self.tenant())
        .bind(id)
        .bind(head_id)
        .fetch_optional(conn)
        .await
        .map_err(db)?
        .ok_or_else(|| Error::NotFound("node".into()))?;
        decode(&json)
    }

    async fn save_node(&self, tx: &mut WriteTx, record: &NodeRecord) -> Result<()> {
        sqlx::query("UPDATE execution_node SET record_json = $2 WHERE tenant_id = $1 AND id = $3")
            .bind(self.tenant())
            .bind(encode(record)?)
            .bind(&record.id)
            .execute(&mut **tx)
            .await
            .map_err(db)?;
        Ok(())
    }

    async fn insert_node(&self, tx: &mut WriteTx, record: &NodeRecord) -> Result<()> {
        sqlx::query(
            "INSERT INTO execution_node (tenant_id, id, head_id, node_identity, record_json) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(self.tenant())
        .bind(&record.id)
        .bind(&record.head_id)
        .bind(&record.node_identity)
        .bind(encode(record)?)
        .execute(&mut **tx)
        .await
        .map_err(db)?;
        Ok(())
    }

    async fn insert_lease(&self, tx: &mut WriteTx, node_id: &str, lease: &NodeLease) -> Result<()> {
        sqlx::query(
            "INSERT INTO node_lease (tenant_id, id, node_id, record_json) VALUES ($1, $2, $3, $4)",
        )
        .bind(self.tenant())
        .bind(&lease.owner.lease_id)
        .bind(node_id)
        .bind(encode(lease)?)
        .execute(&mut **tx)
        .await
        .map_err(db)?;
        Ok(())
    }

    /// End every live lease of `node_id` as `lost`, fenced now.
    async fn end_live_leases(&self, tx: &mut WriteTx, node_id: &str) -> Result<usize> {
        let rows: Vec<String> = sqlx::query_scalar(
            "SELECT record_json FROM node_lease WHERE tenant_id = $1 AND node_id = $2 \
             AND terminal = 0 ORDER BY row_seq",
        )
        .bind(self.tenant())
        .bind(node_id)
        .fetch_all(&mut **tx)
        .await
        .map_err(db)?;
        let mut ended = 0;
        for json in rows {
            let mut record: NodeLease = decode(&json)?;
            if record.state.is_terminal() {
                continue;
            }
            record.state = LeaseState::Lost;
            record.fence = Some(LeaseFence {
                kind: LeaseFenceKind::ProcessesStopped,
                recorded_at: now_iso(),
            });
            self.save_lease(tx, &record).await?;
            ended += 1;
        }
        Ok(ended)
    }

    pub async fn enroll_node(&self, record: &NodeRecord, initial: &NodeLease) -> Result<()> {
        validate_enrollment(record, initial)?;
        let mut tx = self.begin_write().await?;
        self.insert_node(&mut tx, record).await?;
        self.insert_lease(&mut tx, &record.id, initial).await?;
        tx.commit().await.map_err(db)
    }

    pub async fn renew_machine_lease(
        &self,
        record: &NodeRecord,
        initial: &NodeLease,
        generation: NodeCounter,
    ) -> Result<()> {
        validate_enrollment(record, initial)?;
        let mut tx = self.begin_write().await?;
        let head: Option<String> = sqlx::query_scalar(
            "SELECT head_id FROM execution_node WHERE tenant_id = $1 AND id = $2",
        )
        .bind(self.tenant())
        .bind(&record.id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        match head {
            None => self.insert_node(&mut tx, record).await?,
            Some(head) if head == record.head_id => {
                self.end_live_leases(&mut tx, &record.id).await?;
                sqlx::query(
                    "UPDATE execution_node SET node_identity = $2, record_json = $3 \
                     WHERE tenant_id = $1 AND id = $4",
                )
                .bind(self.tenant())
                .bind(&record.node_identity)
                .bind(encode(record)?)
                .bind(&record.id)
                .execute(&mut *tx)
                .await
                .map_err(db)?;
            }
            Some(_) => return Err(invalid("node is registered by another head")),
        }
        let mut lease = initial.clone();
        lease.link_generation = generation;
        self.insert_lease(&mut tx, &record.id, &lease).await?;
        tx.commit().await.map_err(db)
    }

    pub async fn end_machine_leases(&self, node_id: &str, head_id: &str) -> Result<usize> {
        let mut tx = self.begin_write().await?;
        let owned: Option<String> = sqlx::query_scalar(
            "SELECT id FROM execution_node WHERE tenant_id = $1 AND id = $2 AND head_id = $3",
        )
        .bind(self.tenant())
        .bind(node_id)
        .bind(head_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        if owned.is_none() {
            return Ok(0);
        }
        let ended = self.end_live_leases(&mut tx, node_id).await?;
        tx.commit().await.map_err(db)?;
        Ok(ended)
    }

    pub async fn list_owned_nodes(&self, head_id: &str) -> Result<Vec<NodeRecord>> {
        let mut tx = self.begin().await?;
        let rows: Vec<String> = sqlx::query_scalar(
            "SELECT record_json FROM execution_node WHERE tenant_id = $1 AND head_id = $2 \
             ORDER BY id COLLATE \"C\"",
        )
        .bind(self.tenant())
        .bind(head_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        rows.iter().map(|json| decode(json)).collect()
    }

    pub async fn list_owned_node_leases(&self, head_id: &str) -> Result<Vec<NodeLease>> {
        let mut tx = self.begin().await?;
        let rows = sqlx::query(
            "SELECT l.record_json, l.ack_seq FROM node_lease l \
             JOIN execution_node n ON n.tenant_id = $1 AND n.id = l.node_id \
             WHERE l.tenant_id = $1 AND n.head_id = $2 ORDER BY l.id COLLATE \"C\"",
        )
        .bind(self.tenant())
        .bind(head_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        rows.iter().map(lease_row).collect()
    }

    pub async fn get_node(&self, id: &str, head_id: &str) -> Result<NodeRecord> {
        let mut tx = self.begin().await?;
        let record = self.node(&mut tx, id, head_id).await?;
        tx.commit().await.map_err(db)?;
        Ok(record)
    }

    pub async fn get_node_lease(&self, owner: &LeaseOwner) -> Result<NodeLease> {
        let mut tx = self.begin().await?;
        let record = self.lease(&mut tx, owner).await?;
        tx.commit().await.map_err(db)?;
        Ok(record)
    }

    pub async fn transition_node_lease(
        &self,
        owner: &LeaseOwner,
        next: LeaseState,
    ) -> Result<NodeLease> {
        let mut tx = self.begin_write().await?;
        let mut record = self.lease(&mut tx, owner).await?;
        if next.is_terminal() || record.release_requested || !record.state.can_transition_to(next) {
            return Err(invalid("invalid lease transition"));
        }
        record.state = next;
        self.save_lease(&mut tx, &record).await?;
        tx.commit().await.map_err(db)?;
        Ok(record)
    }

    pub async fn next_node_link_generation(&self, owner: &LeaseOwner) -> Result<NodeCounter> {
        let mut tx = self.begin_write().await?;
        let mut record = self.lease(&mut tx, owner).await?;
        if record.state.is_terminal() {
            return Err(invalid("terminal lease"));
        }
        record.link_generation = increment(record.link_generation)?;
        self.save_lease(&mut tx, &record).await?;
        tx.commit().await.map_err(db)?;
        Ok(record.link_generation)
    }

    pub async fn request_node_lease_release(&self, owner: &LeaseOwner) -> Result<NodeLease> {
        let mut tx = self.begin_write().await?;
        let mut record = self.lease(&mut tx, owner).await?;
        let mut host = self.node(&mut tx, &owner.node_id, &owner.head_id).await?;
        static_only(&host)?;
        if !record.state.is_terminal() {
            record.release_requested = true;
            host.draining = true;
            self.save_node(&mut tx, &host).await?;
            self.save_lease(&mut tx, &record).await?;
        }
        tx.commit().await.map_err(db)?;
        Ok(record)
    }

    pub async fn find_execution_node(&self, id: &str) -> Result<Option<NodeRecord>> {
        let mut tx = self.begin().await?;
        let json: Option<String> = sqlx::query_scalar(
            "SELECT record_json FROM execution_node WHERE tenant_id = $1 AND id = $2",
        )
        .bind(self.tenant())
        .bind(id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        json.as_deref().map(decode).transpose()
    }

    pub async fn node_head_id(&self) -> Result<Option<String>> {
        let mut tx = self.begin().await?;
        let head = sqlx::query_scalar(
            "SELECT head_id FROM execution_node WHERE tenant_id = $1 ORDER BY row_seq LIMIT 1",
        )
        .bind(self.tenant())
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(head)
    }

    pub async fn record_node_welcome(
        &self,
        owner: &LeaseOwner,
        os: NodeOs,
        arch: NodeArch,
        max_agents: u32,
        version: &str,
    ) -> Result<NodeLease> {
        let mut tx = self.begin_write().await?;
        let mut record = self.lease(&mut tx, owner).await?;
        if record.state.is_terminal() {
            return Err(invalid("terminal lease"));
        }
        let mut host = self.node(&mut tx, &owner.node_id, &owner.head_id).await?;
        let seen = now_iso();
        host.os = os;
        host.arch = arch;
        host.capacity.max_agents = max_agents;
        version.clone_into(&mut host.version);
        host.state = NodeState::Ready;
        host.last_seen_at = Some(seen.clone());
        if record.state == LeaseState::Acquiring {
            record.state = LeaseState::Ready;
        }
        record.last_seen_at = Some(seen);
        self.save_node(&mut tx, &host).await?;
        self.save_lease(&mut tx, &record).await?;
        tx.commit().await.map_err(db)?;
        Ok(record)
    }

    pub async fn advance_node_lease_ack(
        &self,
        owner: &LeaseOwner,
        seq: NodeCounter,
    ) -> Result<NodeCounter> {
        let mut tx = self.begin_write().await?;
        let record = self.lease(&mut tx, owner).await?;
        if record.state.is_terminal() {
            return Err(invalid("terminal lease"));
        }
        if seq <= record.ack_seq {
            return Ok(record.ack_seq);
        }
        sqlx::query("UPDATE node_lease SET ack_seq = $2 WHERE tenant_id = $1 AND id = $3")
            .bind(self.tenant())
            .bind(seq.0.to_string())
            .bind(&owner.lease_id)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(seq)
    }
}

const FINAL_STOP_COLUMNS: &str =
    "agent_id, workspace_id, machine_id, lease_id, stop_id, reason, repo_path, requested_at, submodules";

fn final_stop_row(row: &PgRow) -> Result<RemoteFinalStopRecord> {
    let reason: String = col(row, "reason")?;
    Ok(RemoteFinalStopRecord {
        agent_id: AgentId::from(col::<String>(row, "agent_id")?),
        workspace_id: WorkspaceId::from(col::<String>(row, "workspace_id")?),
        machine_id: col(row, "machine_id")?,
        lease_id: col(row, "lease_id")?,
        stop_id: col(row, "stop_id")?,
        reason: FinalStopReason::parse(&reason)
            .ok_or_else(|| db(format!("unknown final stop reason {reason}")))?,
        repo_path: col(row, "repo_path")?,
        requested_at: col(row, "requested_at")?,
        submodules: serde_json::from_str(&col::<String>(row, "submodules")?)
            .map_err(|e| db(format!("final stop submodules: {e}")))?,
    })
}

impl PgStore {
    pub async fn record_final_stop(
        &self,
        stop: &RemoteFinalStopRecord,
    ) -> Result<RemoteFinalStopRecord> {
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "INSERT INTO remote_final_stop (tenant_id, agent_id, workspace_id, machine_id, \
             lease_id, stop_id, reason, repo_path, requested_at, submodules) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) \
             ON CONFLICT (tenant_id, agent_id) DO NOTHING",
        )
        .bind(self.tenant())
        .bind(stop.agent_id.as_str())
        .bind(stop.workspace_id.as_str())
        .bind(&stop.machine_id)
        .bind(&stop.lease_id)
        .bind(&stop.stop_id)
        .bind(stop.reason.as_str())
        .bind(&stop.repo_path)
        .bind(&stop.requested_at)
        .bind(serde_json::to_string(&stop.submodules).map_err(db)?)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        let row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {FINAL_STOP_COLUMNS} FROM remote_final_stop \
             WHERE tenant_id = $1 AND agent_id = $2"
        )))
        .bind(self.tenant())
        .bind(stop.agent_id.as_str())
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        let recorded = final_stop_row(&row)?;
        tx.commit().await.map_err(db)?;
        Ok(recorded)
    }

    pub async fn list_final_stops(&self, lease_id: &str) -> Result<Vec<RemoteFinalStopRecord>> {
        let mut tx = self.begin().await?;
        let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {FINAL_STOP_COLUMNS} FROM remote_final_stop \
             WHERE tenant_id = $1 AND lease_id = $2 \
             ORDER BY requested_at COLLATE \"C\", agent_id COLLATE \"C\""
        )))
        .bind(self.tenant())
        .bind(lease_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        rows.iter().map(final_stop_row).collect()
    }

    pub async fn get_final_stop(
        &self,
        agent_id: &AgentId,
    ) -> Result<Option<RemoteFinalStopRecord>> {
        let mut tx = self.begin().await?;
        let row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {FINAL_STOP_COLUMNS} FROM remote_final_stop \
             WHERE tenant_id = $1 AND agent_id = $2"
        )))
        .bind(self.tenant())
        .bind(agent_id.as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        row.as_ref().map(final_stop_row).transpose()
    }
}

const INVITE_COLUMNS: &str =
    "id, label, created_at, expires_at, used_at, credential_id, revoked_at";

fn invite_row(row: &PgRow) -> Result<NodeInvite> {
    Ok(NodeInvite {
        id: col(row, "id")?,
        label: col(row, "label")?,
        created_at: col(row, "created_at")?,
        expires_at: col(row, "expires_at")?,
        used_at: col(row, "used_at")?,
        credential_id: col(row, "credential_id")?,
        revoked_at: col(row, "revoked_at")?,
    })
}

impl PgStore {
    pub async fn insert_node_invite(&self, invite: &NodeInvite, secret_hash: &str) -> Result<()> {
        if invite.id.is_empty()
            || secret_hash.is_empty()
            || invite.used_at.is_some()
            || invite.revoked_at.is_some()
        {
            return Err(invalid("invalid node invite"));
        }
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "INSERT INTO node_invite (tenant_id, id, secret_hash, label, created_at, expires_at) \
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(self.tenant())
        .bind(&invite.id)
        .bind(secret_hash)
        .bind(&invite.label)
        .bind(&invite.created_at)
        .bind(&invite.expires_at)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)
    }

    pub async fn list_open_node_invites(&self) -> Result<Vec<NodeInvite>> {
        let mut tx = self.begin().await?;
        let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {INVITE_COLUMNS} FROM node_invite \
             WHERE tenant_id = $1 AND used_at IS NULL AND revoked_at IS NULL \
             ORDER BY created_at COLLATE \"C\", id COLLATE \"C\""
        )))
        .bind(self.tenant())
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        let now = now_iso();
        let mut open = Vec::with_capacity(rows.len());
        for row in &rows {
            let invite = invite_row(row)?;
            if !invite.expired_at(&now) {
                open.push(invite);
            }
        }
        Ok(open)
    }

    pub async fn claimed_node_invites(&self) -> Result<Vec<(String, String)>> {
        let mut tx = self.begin().await?;
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT credential_id, id FROM node_invite \
             WHERE tenant_id = $1 AND credential_id IS NOT NULL ORDER BY row_seq",
        )
        .bind(self.tenant())
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(rows)
    }

    pub async fn revoke_node_invite(&self, id: &str) -> Result<NodeInviteRevocation> {
        let mut tx = self.begin_write().await?;
        let updated = sqlx::query(
            "UPDATE node_invite SET revoked_at = $2 \
             WHERE tenant_id = $1 AND id = $3 AND used_at IS NULL AND revoked_at IS NULL",
        )
        .bind(self.tenant())
        .bind(now_iso())
        .bind(id)
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected();
        let outcome = if updated > 0 {
            NodeInviteRevocation::Revoked
        } else {
            let known: Option<String> =
                sqlx::query_scalar("SELECT id FROM node_invite WHERE tenant_id = $1 AND id = $2")
                    .bind(self.tenant())
                    .bind(id)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(db)?;
            match known {
                Some(_) => NodeInviteRevocation::Closed,
                None => NodeInviteRevocation::NotFound,
            }
        };
        tx.commit().await.map_err(db)?;
        Ok(outcome)
    }

    pub async fn claim_node_invite(
        &self,
        id: &str,
        secret_hash: &str,
        backend_id: &str,
        credential: &ExecutorCredential,
        token_hash: &str,
        late: Option<&'static str>,
    ) -> Result<std::result::Result<(), NodeInviteRefusal>> {
        if credential.id.as_str().is_empty()
            || token_hash.is_empty()
            || credential.backend_id.is_empty()
            || credential.backend_name.is_empty()
            || credential.revoked_at.is_some()
        {
            return Err(invalid("invalid executor credential"));
        }
        let mut tx = self.begin_write().await?;
        let row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {INVITE_COLUMNS}, secret_hash FROM node_invite WHERE tenant_id = $1 AND id = $2"
        )))
        .bind(self.tenant())
        .bind(id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        let Some(row) = row else {
            return Ok(Err(NodeInviteRefusal::Invalid));
        };
        let stored: String = col(&row, "secret_hash")?;
        if !constant_time_eq(stored.as_bytes(), secret_hash.as_bytes()) {
            return Ok(Err(NodeInviteRefusal::Invalid));
        }
        let invite = invite_row(&row)?;
        let now = now_iso();
        if invite.revoked_at.is_some() {
            return Ok(Err(NodeInviteRefusal::Revoked));
        }
        if invite.used_at.is_some() {
            return Ok(Err(NodeInviteRefusal::Used));
        }
        if invite.expired_at(&now) {
            return Ok(Err(NodeInviteRefusal::Expired {
                expires_at: invite.expires_at,
            }));
        }
        if let Some(code) = late {
            return Ok(Err(NodeInviteRefusal::Late(code)));
        }
        sqlx::query(
            "INSERT INTO executor_credential (tenant_id, id, token_hash, backend_id, \
             backend_name, backend_fingerprint, created_at, last_used_at, revoked_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, NULL, NULL)",
        )
        .bind(self.tenant())
        .bind(credential.id.as_str())
        .bind(token_hash)
        .bind(&credential.backend_id)
        .bind(&credential.backend_name)
        .bind(&credential.backend_fingerprint)
        .bind(&credential.created_at)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        let spent = sqlx::query(
            "UPDATE node_invite SET used_at = $2, used_by_backend_id = $3, credential_id = $4 \
             WHERE tenant_id = $1 AND id = $5 AND used_at IS NULL AND revoked_at IS NULL",
        )
        .bind(self.tenant())
        .bind(&now)
        .bind(backend_id)
        .bind(credential.id.as_str())
        .bind(id)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        if spent.rows_affected() != 1 {
            return Ok(Err(NodeInviteRefusal::Used));
        }
        tx.commit().await.map_err(db)?;
        Ok(Ok(()))
    }
}

const CREDENTIAL_COLUMNS: &str =
    "id, backend_id, backend_name, backend_fingerprint, created_at, last_used_at, revoked_at";
const MACHINE_COLUMNS: &str = "id, name, hosts_json, port, tc_address, fingerprint, \
     credential_id, platform_json, capabilities_json, observed_at, state, added_at, \
     last_connected_at, node_protocol, cloud_json";

fn credential_row(row: &PgRow) -> Result<ExecutorCredential> {
    Ok(ExecutorCredential {
        id: ExecutorCredentialId::from(col::<String>(row, "id")?),
        backend_id: col(row, "backend_id")?,
        backend_name: col(row, "backend_name")?,
        backend_fingerprint: col(row, "backend_fingerprint")?,
        created_at: col(row, "created_at")?,
        last_used_at: col(row, "last_used_at")?,
        revoked_at: col(row, "revoked_at")?,
    })
}

fn json_opt<T: serde::de::DeserializeOwned>(row: &PgRow, name: &str) -> Result<Option<T>> {
    col::<Option<String>>(row, name)?
        .map(|json| serde_json::from_str(&json))
        .transpose()
        .map_err(db)
}

fn machine_row(row: &PgRow) -> Result<ExposedMachine> {
    let hosts: Vec<String> =
        serde_json::from_str(&col::<String>(row, "hosts_json")?).map_err(db)?;
    let platform: Option<AgentPlatform> = json_opt(row, "platform_json")?;
    let capabilities: Option<MachineCapabilities> = json_opt(row, "capabilities_json")?;
    let state = RemoteMachineState::parse(&col::<String>(row, "state")?)
        .ok_or_else(|| db("unknown exposed machine state"))?;
    Ok(ExposedMachine {
        id: col(row, "id")?,
        name: col(row, "name")?,
        hosts,
        port: u16::try_from(col::<i64>(row, "port")?).map_err(db)?,
        tc_address: col(row, "tc_address")?,
        fingerprint: col(row, "fingerprint")?,
        credential_id: ExecutorCredentialId::from(col::<String>(row, "credential_id")?),
        platform,
        capabilities,
        observed_at: col(row, "observed_at")?,
        state,
        added_at: col(row, "added_at")?,
        last_connected_at: col(row, "last_connected_at")?,
        node_protocol: col::<Option<i64>>(row, "node_protocol")?
            .map(u32::try_from)
            .transpose()
            .map_err(db)?,
        cloud: json_opt(row, "cloud_json")?,
    })
}

fn binding_row(row: &PgRow) -> Result<LeaseBinding> {
    Ok(LeaseBinding {
        head_id: col(row, "head_id")?,
        node_id: col(row, "node_id")?,
        lease_id: col(row, "lease_id")?,
        generation: counter(row, "generation")?,
    })
}

impl PgStore {
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
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "INSERT INTO executor_credential (tenant_id, id, token_hash, backend_id, \
             backend_name, backend_fingerprint, created_at, last_used_at, revoked_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NULL)",
        )
        .bind(self.tenant())
        .bind(credential.id.as_str())
        .bind(token_hash)
        .bind(&credential.backend_id)
        .bind(&credential.backend_name)
        .bind(&credential.backend_fingerprint)
        .bind(&credential.created_at)
        .bind(&credential.last_used_at)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)
    }

    pub async fn resolve_active_executor_credential(
        &self,
        token_hash: &str,
    ) -> Result<Option<ExecutorCredential>> {
        let mut tx = self.begin_write().await?;
        let row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE executor_credential SET last_used_at = $2 \
             WHERE tenant_id = $1 AND token_hash = $3 AND revoked_at IS NULL \
             RETURNING {CREDENTIAL_COLUMNS}"
        )))
        .bind(self.tenant())
        .bind(now_iso())
        .bind(token_hash)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        row.as_ref().map(credential_row).transpose()
    }

    pub async fn get_executor_credential(
        &self,
        id: &ExecutorCredentialId,
    ) -> Result<Option<ExecutorCredential>> {
        let mut tx = self.begin().await?;
        let row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {CREDENTIAL_COLUMNS} FROM executor_credential WHERE tenant_id = $1 AND id = $2"
        )))
        .bind(self.tenant())
        .bind(id.as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        row.as_ref().map(credential_row).transpose()
    }

    pub async fn list_active_executor_credentials(&self) -> Result<Vec<ExecutorCredential>> {
        let mut tx = self.begin().await?;
        let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {CREDENTIAL_COLUMNS} FROM executor_credential \
             WHERE tenant_id = $1 AND revoked_at IS NULL \
             ORDER BY created_at COLLATE \"C\", id COLLATE \"C\""
        )))
        .bind(self.tenant())
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        rows.iter().map(credential_row).collect()
    }

    pub async fn revoke_executor_credential(
        &self,
        id: &ExecutorCredentialId,
    ) -> Result<ExecutorRevocation> {
        let mut tx = self.begin_write().await?;
        let revoked: Option<Option<String>> = sqlx::query_scalar(
            "SELECT revoked_at FROM executor_credential WHERE tenant_id = $1 AND id = $2",
        )
        .bind(self.tenant())
        .bind(id.as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        let outcome = match revoked {
            None => ExecutorRevocation::NotFound,
            Some(Some(_)) => ExecutorRevocation::AlreadyRevoked,
            Some(None) => {
                sqlx::query(
                    "UPDATE executor_credential SET revoked_at = $2 WHERE tenant_id = $1 AND id = $3",
                )
                .bind(self.tenant())
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

    async fn binding(
        &self,
        conn: &mut PgConnection,
        credential_id: &ExecutorCredentialId,
    ) -> Result<Option<LeaseBinding>> {
        sqlx::query(
            "SELECT head_id, node_id, lease_id, generation FROM executor_binding \
             WHERE tenant_id = $1 AND credential_id = $2",
        )
        .bind(self.tenant())
        .bind(credential_id.as_str())
        .fetch_optional(conn)
        .await
        .map_err(db)?
        .as_ref()
        .map(binding_row)
        .transpose()
    }

    pub async fn executor_binding(
        &self,
        credential_id: &ExecutorCredentialId,
    ) -> Result<Option<LeaseBinding>> {
        let mut tx = self.begin().await?;
        let binding = self.binding(&mut tx, credential_id).await?;
        tx.commit().await.map_err(db)?;
        Ok(binding)
    }

    pub async fn bind_executor(
        &self,
        credential_id: &ExecutorCredentialId,
        node_id: &str,
        offered: &LeaseBinding,
    ) -> Result<BindDecision> {
        let mut tx = self.begin_write().await?;
        let backend_id: Option<String> = sqlx::query_scalar(
            "SELECT backend_id FROM executor_credential \
             WHERE tenant_id = $1 AND id = $2 AND revoked_at IS NULL",
        )
        .bind(self.tenant())
        .bind(credential_id.as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        let backend_id = backend_id.ok_or_else(|| Error::NotFound("executor credential".into()))?;
        if offered.node_id == node_id && offered.head_id != backend_id {
            return Ok(BindDecision::OtherHead);
        }
        let current = self.binding(&mut tx, credential_id).await?;
        let decision = LeaseBinding::decide(node_id, current.as_ref(), offered);
        if decision == BindDecision::Accept {
            sqlx::query(
                "INSERT INTO executor_binding (tenant_id, credential_id, head_id, node_id, \
                 lease_id, generation, bound_at) VALUES ($1, $2, $3, $4, $5, $6, $7) \
                 ON CONFLICT (tenant_id, credential_id) DO UPDATE SET head_id = excluded.head_id, \
                 node_id = excluded.node_id, lease_id = excluded.lease_id, \
                 generation = excluded.generation, bound_at = excluded.bound_at",
            )
            .bind(self.tenant())
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

    pub async fn save_exposed_machine(
        &self,
        machine: &ExposedMachine,
    ) -> Result<Option<ExecutorCredentialId>> {
        if machine.id.is_empty()
            || machine.name.is_empty()
            || machine.fingerprint.is_empty()
            || machine.credential_id.as_str().is_empty()
            || !has_route(&machine.hosts, machine.tc_address.as_deref())
            || machine.port == 0
        {
            return Err(invalid("invalid exposed machine"));
        }
        resting(machine.state)?;
        let mut tx = self.begin_write().await?;
        let previous: Option<String> = sqlx::query_scalar(
            "SELECT credential_id FROM exposed_machine WHERE tenant_id = $1 AND id = $2",
        )
        .bind(self.tenant())
        .bind(&machine.id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        sqlx::query(
            "INSERT INTO exposed_machine (tenant_id, id, name, hosts_json, port, tc_address, \
             fingerprint, credential_id, platform_json, capabilities_json, observed_at, state, \
             added_at, last_connected_at, node_protocol, cloud_json) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16) \
             ON CONFLICT (tenant_id, id) DO UPDATE SET name = excluded.name, \
             hosts_json = excluded.hosts_json, port = excluded.port, \
             tc_address = excluded.tc_address, fingerprint = excluded.fingerprint, \
             credential_id = excluded.credential_id, platform_json = excluded.platform_json, \
             capabilities_json = excluded.capabilities_json, observed_at = excluded.observed_at, \
             state = excluded.state, added_at = excluded.added_at, \
             last_connected_at = excluded.last_connected_at, \
             node_protocol = excluded.node_protocol, cloud_json = excluded.cloud_json",
        )
        .bind(self.tenant())
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
        .bind(machine.node_protocol.map(i64::from))
        .bind(machine.cloud.as_ref().map(encode).transpose()?)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(previous
            .filter(|previous| previous != machine.credential_id.as_str())
            .map(ExecutorCredentialId::from))
    }

    pub async fn get_exposed_machine(&self, id: &str) -> Result<Option<ExposedMachine>> {
        let mut tx = self.begin().await?;
        let row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {MACHINE_COLUMNS} FROM exposed_machine WHERE tenant_id = $1 AND id = $2"
        )))
        .bind(self.tenant())
        .bind(id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        row.as_ref().map(machine_row).transpose()
    }

    async fn machines(&self, cloud: bool) -> Result<Vec<ExposedMachine>> {
        let mut tx = self.begin().await?;
        let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {MACHINE_COLUMNS} FROM exposed_machine \
             WHERE tenant_id = $1 AND (cloud_json IS NOT NULL) = $2 \
             ORDER BY added_at COLLATE \"C\", id COLLATE \"C\""
        )))
        .bind(self.tenant())
        .bind(cloud)
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        rows.iter().map(machine_row).collect()
    }

    pub async fn list_exposed_machines(&self) -> Result<Vec<ExposedMachine>> {
        self.machines(false).await
    }

    pub async fn list_cloud_machines(&self) -> Result<Vec<ExposedMachine>> {
        self.machines(true).await
    }

    pub async fn remove_exposed_machine(&self, id: &str) -> Result<Option<ExecutorCredentialId>> {
        let mut tx = self.begin_write().await?;
        let credential: Option<String> = sqlx::query_scalar(
            "DELETE FROM exposed_machine WHERE tenant_id = $1 AND id = $2 RETURNING credential_id",
        )
        .bind(self.tenant())
        .bind(id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(credential.map(ExecutorCredentialId::from))
    }

    pub async fn update_cloud_machine(&self, id: &str, cloud: &CloudMachine) -> Result<bool> {
        let mut tx = self.begin_write().await?;
        let done = sqlx::query(
            "UPDATE exposed_machine SET cloud_json = $2 \
             WHERE tenant_id = $1 AND id = $3 AND cloud_json IS NOT NULL",
        )
        .bind(self.tenant())
        .bind(encode(cloud)?)
        .bind(id)
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected();
        tx.commit().await.map_err(db)?;
        Ok(done > 0)
    }

    pub async fn rename_cloud_machine(&self, id: &str, name: &str) -> Result<bool> {
        if name.is_empty() {
            return Err(invalid("invalid exposed machine"));
        }
        let mut tx = self.begin_write().await?;
        let done = sqlx::query(
            "UPDATE exposed_machine SET name = $2 \
             WHERE tenant_id = $1 AND id = $3 AND cloud_json IS NOT NULL",
        )
        .bind(self.tenant())
        .bind(name)
        .bind(id)
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected();
        tx.commit().await.map_err(db)?;
        Ok(done > 0)
    }

    pub async fn set_exposed_machine_state(
        &self,
        id: &str,
        state: RemoteMachineState,
    ) -> Result<bool> {
        resting(state)?;
        let mut tx = self.begin_write().await?;
        let done =
            sqlx::query("UPDATE exposed_machine SET state = $2 WHERE tenant_id = $1 AND id = $3")
                .bind(self.tenant())
                .bind(state.as_str())
                .bind(id)
                .execute(&mut *tx)
                .await
                .map_err(db)?
                .rows_affected();
        tx.commit().await.map_err(db)?;
        Ok(done > 0)
    }

    pub async fn record_exposed_machine_connection(
        &self,
        id: &str,
        hosts: &[String],
        tc_address: Option<&str>,
        connected_at: &str,
    ) -> Result<bool> {
        if !has_route(hosts, tc_address) {
            return Err(invalid(
                "an exposed machine needs a host or a tailcat address",
            ));
        }
        let mut tx = self.begin_write().await?;
        let done = sqlx::query(
            "UPDATE exposed_machine SET hosts_json = $2, tc_address = $3, last_connected_at = $4 \
             WHERE tenant_id = $1 AND id = $5",
        )
        .bind(self.tenant())
        .bind(encode(&hosts)?)
        .bind(tc_address)
        .bind(connected_at)
        .bind(id)
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected();
        tx.commit().await.map_err(db)?;
        Ok(done > 0)
    }

    pub async fn record_exposed_machine_protocol(&self, id: &str, protocol: u32) -> Result<bool> {
        let mut tx = self.begin_write().await?;
        let done = sqlx::query(
            "UPDATE exposed_machine SET node_protocol = $2 WHERE tenant_id = $1 AND id = $3",
        )
        .bind(self.tenant())
        .bind(i64::from(protocol))
        .bind(id)
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected();
        tx.commit().await.map_err(db)?;
        Ok(done > 0)
    }

    pub async fn record_exposed_machine_capabilities(
        &self,
        id: &str,
        capabilities: &MachineCapabilities,
    ) -> Result<bool> {
        let mut tx = self.begin_write().await?;
        let done = sqlx::query(
            "UPDATE exposed_machine SET capabilities_json = $2, platform_json = $3, \
             observed_at = $4 WHERE tenant_id = $1 AND id = $5",
        )
        .bind(self.tenant())
        .bind(encode(capabilities)?)
        .bind(encode(&capabilities.platform)?)
        .bind(&capabilities.observed_at)
        .bind(id)
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected();
        tx.commit().await.map_err(db)?;
        Ok(done > 0)
    }

    pub async fn get_cloud_machine_provider(&self, id: &str) -> Result<Option<String>> {
        let mut tx = self.begin().await?;
        let config = sqlx::query_scalar(
            "SELECT config_json FROM cloud_machine_provider WHERE tenant_id = $1 AND id = $2",
        )
        .bind(self.tenant())
        .bind(id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(config)
    }

    pub async fn list_cloud_machine_providers(&self) -> Result<Vec<(String, String)>> {
        let mut tx = self.begin().await?;
        let rows = sqlx::query_as(
            "SELECT id, config_json FROM cloud_machine_provider WHERE tenant_id = $1 \
             ORDER BY row_seq",
        )
        .bind(self.tenant())
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(rows)
    }

    pub async fn save_cloud_machine_provider(&self, id: &str, config_json: &str) -> Result<()> {
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "INSERT INTO cloud_machine_provider (tenant_id, id, config_json, updated_at) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (tenant_id, id) DO UPDATE SET config_json = excluded.config_json, \
             updated_at = excluded.updated_at",
        )
        .bind(self.tenant())
        .bind(id)
        .bind(config_json)
        .bind(now_iso())
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)
    }

    pub async fn delete_cloud_machine_provider(&self, id: &str) -> Result<bool> {
        let mut tx = self.begin_write().await?;
        let done =
            sqlx::query("DELETE FROM cloud_machine_provider WHERE tenant_id = $1 AND id = $2")
                .bind(self.tenant())
                .bind(id)
                .execute(&mut *tx)
                .await
                .map_err(db)?
                .rows_affected();
        tx.commit().await.map_err(db)?;
        Ok(done > 0)
    }
}

fn remote_agent_row(row: &PgRow) -> Result<RemoteAgentRecord> {
    Ok(RemoteAgentRecord {
        agent_id: AgentId::from(col::<String>(row, "agent_id")?),
        workspace_id: WorkspaceId::from(col::<String>(row, "workspace_id")?),
        node_id: col(row, "node_id")?,
        lease_id: col(row, "lease_id")?,
        created_at: col(row, "created_at")?,
    })
}

impl PgStore {
    async fn assignment(
        &self,
        conn: &mut PgConnection,
        agent_id: &AgentId,
    ) -> Result<Option<NodeAssignment>> {
        let json: Option<String> = sqlx::query_scalar(
            "SELECT record_json FROM node_assignment WHERE tenant_id = $1 AND agent_id = $2",
        )
        .bind(self.tenant())
        .bind(agent_id.as_str())
        .fetch_optional(conn)
        .await
        .map_err(db)?;
        json.as_deref().map(decode).transpose()
    }

    async fn save_assignment(&self, tx: &mut WriteTx, record: &NodeAssignment) -> Result<()> {
        sqlx::query(
            "UPDATE node_assignment SET record_json = $2 WHERE tenant_id = $1 AND agent_id = $3",
        )
        .bind(self.tenant())
        .bind(encode(record)?)
        .bind(record.agent_id.as_str())
        .execute(&mut **tx)
        .await
        .map_err(db)?;
        Ok(())
    }

    async fn current_checkpoint(
        &self,
        conn: &mut PgConnection,
        agent_id: &AgentId,
    ) -> Result<Option<NodeCheckpoint>> {
        let json: Option<String> = sqlx::query_scalar(
            "SELECT c.record_json FROM node_assignment a \
             JOIN node_checkpoint c ON c.tenant_id = $1 AND c.id = a.current_checkpoint_id \
             WHERE a.tenant_id = $1 AND a.agent_id = $2",
        )
        .bind(self.tenant())
        .bind(agent_id.as_str())
        .fetch_optional(conn)
        .await
        .map_err(db)?;
        json.as_deref().map(decode).transpose()
    }

    pub async fn finish_node_lease(
        &self,
        owner: &LeaseOwner,
        terminal: LeaseState,
        fence: LeaseFence,
    ) -> Result<NodeLease> {
        if !terminal.is_terminal() || fence.recorded_at.is_empty() {
            return Err(invalid("terminal lease requires fence evidence"));
        }
        let mut tx = self.begin_write().await?;
        let mut record = self.lease(&mut tx, owner).await?;
        let mut host = self.node(&mut tx, &owner.node_id, &owner.head_id).await?;
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
        let rows: Vec<String> = sqlx::query_scalar(
            "SELECT record_json FROM node_assignment WHERE tenant_id = $1 AND lease_id = $2 \
             ORDER BY row_seq",
        )
        .bind(self.tenant())
        .bind(&owner.lease_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        for json in rows {
            let mut placed: NodeAssignment = decode(&json)?;
            placed.active = false;
            self.save_assignment(&mut tx, &placed).await?;
        }
        host.capacity.reserved_agents = 0;
        host.capacity.exclusive_reserved = false;
        self.save_node(&mut tx, &host).await?;
        self.save_lease(&mut tx, &record).await?;
        tx.commit().await.map_err(db)?;
        Ok(record)
    }

    pub async fn assign_node_run(&self, template: &NodeAssignment) -> Result<NodeAssignment> {
        let mut tx = self.begin_write().await?;
        let current_lease = self.lease(&mut tx, &template.owner).await?;
        dispatchable(&current_lease)?;
        let host = self
            .node(&mut tx, &template.owner.node_id, &template.owner.head_id)
            .await?;
        if host.draining
            || host.state != NodeState::Ready
            || uuid::Uuid::parse_str(&template.run_id).is_err()
        {
            return Err(invalid("node unavailable or invalid run"));
        }
        let workspace: Option<String> = sqlx::query_scalar(
            "SELECT workspace_id FROM agent_session WHERE tenant_id = $1 AND id = $2",
        )
        .bind(self.tenant())
        .bind(template.agent_id.as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        if workspace.as_deref() != Some(template.workspace_id.as_str()) {
            return Err(Error::NotFound("agent workspace".into()));
        }
        let old = self.assignment(&mut tx, &template.agent_id).await?;
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
            if old.merge_target_agent_id != template.merge_target_agent_id
                || old.inherited_checkpoint_id != template.inherited_checkpoint_id
            {
                return Err(invalid(
                    "reassignment cannot replace inherited baseline or merge target",
                ));
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
            let json: Option<String> = sqlx::query_scalar(
                "SELECT record_json FROM node_checkpoint WHERE tenant_id = $1 AND id = $2",
            )
            .bind(self.tenant())
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
        sqlx::query(
            "INSERT INTO node_assignment (tenant_id, agent_id, workspace_id, lease_id, record_json) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (tenant_id, agent_id) DO UPDATE SET lease_id = excluded.lease_id, \
             record_json = excluded.record_json, capture_revision = '0'",
        )
        .bind(self.tenant())
        .bind(admitted.agent_id.as_str())
        .bind(admitted.workspace_id.as_str())
        .bind(&admitted.owner.lease_id)
        .bind(encode(&admitted)?)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(admitted)
    }

    pub async fn get_node_assignment(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
    ) -> Result<NodeAssignment> {
        let mut tx = self.begin().await?;
        let record = self.assignment(&mut tx, agent_id).await?;
        tx.commit().await.map_err(db)?;
        let record = record.ok_or_else(|| Error::NotFound("assignment".into()))?;
        if record.workspace_id != *workspace_id {
            return Err(Error::NotFound("assignment workspace".into()));
        }
        Ok(record)
    }

    pub async fn stop_node_assignment(
        &self,
        expected: &NodeAssignment,
        tombstone: bool,
    ) -> Result<()> {
        let mut tx = self.begin_write().await?;
        self.lease(&mut tx, &expected.owner).await?;
        let mut record = self
            .assignment(&mut tx, &expected.agent_id)
            .await?
            .ok_or_else(|| Error::NotFound("assignment".into()))?;
        check_assignment(&record, expected)?;
        record.active = false;
        record.tombstoned |= tombstone;
        self.save_assignment(&mut tx, &record).await?;
        tx.commit().await.map_err(db)
    }

    pub async fn authorize_node_repo(
        &self,
        expected: &NodeAssignment,
        repo_key: &str,
    ) -> Result<()> {
        // The lease and the assignment come from one snapshot.
        let mut tx = self.begin_read_snapshot().await?;
        let current_lease = self.lease(&mut tx, &expected.owner).await?;
        dispatchable(&current_lease)?;
        super::read_seam::probe::between(self.tenant(), "authorize_node_repo").await;
        let current = self
            .assignment(&mut tx, &expected.agent_id)
            .await?
            .ok_or_else(|| Error::NotFound("assignment".into()))?;
        tx.commit().await.map_err(db)?;
        check_assignment(&current, expected)?;
        if !current.active
            || current.tombstoned
            || !current.repo_keys.iter().any(|key| key == repo_key)
        {
            return Err(Error::NotFound("repository grant".into()));
        }
        Ok(())
    }

    pub async fn next_node_capture_revision(
        &self,
        expected: &NodeAssignment,
    ) -> Result<NodeCounter> {
        let mut tx = self.begin_write().await?;
        dispatchable(&self.lease(&mut tx, &expected.owner).await?)?;
        let current = self
            .assignment(&mut tx, &expected.agent_id)
            .await?
            .ok_or_else(|| Error::NotFound("assignment".into()))?;
        check_assignment(&current, expected)?;
        if !current.active || current.tombstoned {
            return Err(invalid("inactive assignment"));
        }
        let value: String = sqlx::query_scalar(
            "SELECT capture_revision FROM node_assignment WHERE tenant_id = $1 AND agent_id = $2",
        )
        .bind(self.tenant())
        .bind(expected.agent_id.as_str())
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        let next = increment(NodeCounter(value.parse().map_err(db)?))?;
        sqlx::query(
            "UPDATE node_assignment SET capture_revision = $2 WHERE tenant_id = $1 AND agent_id = $3",
        )
        .bind(self.tenant())
        .bind(next.0.to_string())
        .bind(expected.agent_id.as_str())
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(next)
    }

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
        let mut tx = self.begin_write().await?;
        let owned_lease = self.lease(&mut tx, &checkpoint.owner).await?;
        let existing = sqlx::query(
            "SELECT record_json, receipt_json FROM node_checkpoint WHERE tenant_id = $1 AND id = $2",
        )
        .bind(self.tenant())
        .bind(&checkpoint.id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        if let Some(row) = existing {
            let stored: NodeCheckpoint = decode(&col::<String>(&row, "record_json")?)?;
            let mut retry = checkpoint.clone();
            retry.committed_at.clone_from(&stored.committed_at);
            if stored != retry {
                return Err(invalid("checkpoint-invalid: UUID reused"));
            }
            return decode(&col::<String>(&row, "receipt_json")?);
        }
        dispatchable(&owned_lease)?;
        let assigned = self
            .assignment(&mut tx, &checkpoint.agent_id)
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
        let pair_used: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM node_checkpoint WHERE tenant_id = $1 AND agent_id = $2 \
             AND assignment_epoch = $3 AND capture_revision = $4)",
        )
        .bind(self.tenant())
        .bind(checkpoint.agent_id.as_str())
        .bind(checkpoint.assignment_epoch.0.to_string())
        .bind(checkpoint.capture_revision.0.to_string())
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        if pair_used {
            return Err(invalid("checkpoint-invalid: capture revision reused"));
        }
        let previous = self
            .current_checkpoint(&mut tx, &checkpoint.agent_id)
            .await?;
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
        sqlx::query(
            "INSERT INTO node_checkpoint (tenant_id, id, agent_id, assignment_epoch, \
             capture_revision, record_json, receipt_json) VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(self.tenant())
        .bind(&stored.id)
        .bind(stored.agent_id.as_str())
        .bind(stored.assignment_epoch.0.to_string())
        .bind(stored.capture_revision.0.to_string())
        .bind(encode(&stored)?)
        .bind(encode(&receipt)?)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        if advanced {
            sqlx::query(
                "UPDATE node_assignment SET current_checkpoint_id = $2 \
                 WHERE tenant_id = $1 AND agent_id = $3",
            )
            .bind(self.tenant())
            .bind(&stored.id)
            .bind(stored.agent_id.as_str())
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        }
        tx.commit().await.map_err(db)?;
        Ok(receipt)
    }

    pub async fn current_node_checkpoint(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
    ) -> Result<Option<NodeCheckpoint>> {
        // The assignment and its checkpoint come from one snapshot.
        let mut tx = self.begin_read_snapshot().await?;
        let record = self
            .assignment(&mut tx, agent_id)
            .await?
            .ok_or_else(|| Error::NotFound("assignment".into()))?;
        if record.workspace_id != *workspace_id {
            return Err(Error::NotFound("assignment workspace".into()));
        }
        super::read_seam::probe::between(self.tenant(), "current_node_checkpoint").await;
        let current = self.current_checkpoint(&mut tx, agent_id).await?;
        tx.commit().await.map_err(db)?;
        Ok(current)
    }
}

const REMOTE_AGENT_COLUMNS: &str = "agent_id, workspace_id, node_id, lease_id, created_at";

impl PgStore {
    pub async fn record_remote_agent(&self, agent: &RemoteAgentRecord) -> Result<()> {
        let mut tx = self.begin_write().await?;
        let row = sqlx::query(
            "SELECT node_id, terminal FROM node_lease WHERE tenant_id = $1 AND id = $2",
        )
        .bind(self.tenant())
        .bind(&agent.lease_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?
        .ok_or_else(|| Error::NotFound("lease".into()))?;
        if col::<String>(&row, "node_id")? != agent.node_id || col::<i64>(&row, "terminal")? != 0 {
            return Err(invalid("lease does not admit agents on this node"));
        }
        let existing = sqlx::query(
            "SELECT node_id, lease_id FROM remote_agent WHERE tenant_id = $1 AND agent_id = $2",
        )
        .bind(self.tenant())
        .bind(agent.agent_id.as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        if let Some(existing) = existing {
            if col::<String>(&existing, "node_id")? == agent.node_id
                && col::<String>(&existing, "lease_id")? == agent.lease_id
            {
                return Ok(());
            }
            return Err(invalid("agent already runs on another lease"));
        }
        sqlx::query(
            "INSERT INTO remote_agent (tenant_id, agent_id, workspace_id, node_id, lease_id, \
             created_at) VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(self.tenant())
        .bind(agent.agent_id.as_str())
        .bind(agent.workspace_id.as_str())
        .bind(&agent.node_id)
        .bind(&agent.lease_id)
        .bind(&agent.created_at)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)
    }

    pub async fn list_remote_agents(&self, lease_id: &str) -> Result<Vec<RemoteAgentRecord>> {
        let mut tx = self.begin().await?;
        let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {REMOTE_AGENT_COLUMNS} FROM remote_agent \
             WHERE tenant_id = $1 AND lease_id = $2 \
             ORDER BY created_at COLLATE \"C\", agent_id COLLATE \"C\""
        )))
        .bind(self.tenant())
        .bind(lease_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        rows.iter().map(remote_agent_row).collect()
    }

    pub async fn get_remote_agent(&self, agent_id: &AgentId) -> Result<Option<RemoteAgentRecord>> {
        let mut tx = self.begin().await?;
        let row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {REMOTE_AGENT_COLUMNS} FROM remote_agent WHERE tenant_id = $1 AND agent_id = $2"
        )))
        .bind(self.tenant())
        .bind(agent_id.as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        row.as_ref().map(remote_agent_row).transpose()
    }

    pub async fn get_remote_agents(&self, agent_ids: &[AgentId]) -> Result<Vec<RemoteAgentRecord>> {
        if agent_ids.is_empty() {
            return Ok(Vec::new());
        }
        let ids: Vec<&str> = agent_ids.iter().map(AgentId::as_str).collect();
        let mut tx = self.begin().await?;
        let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {REMOTE_AGENT_COLUMNS} FROM remote_agent \
             WHERE tenant_id = $1 AND agent_id = ANY($2) \
             ORDER BY created_at COLLATE \"C\", agent_id COLLATE \"C\""
        )))
        .bind(self.tenant())
        .bind(ids)
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        rows.iter().map(remote_agent_row).collect()
    }

    pub async fn remove_remote_agent(&self, agent_id: &AgentId) -> Result<bool> {
        let mut tx = self.begin_write().await?;
        let done = sqlx::query("DELETE FROM remote_agent WHERE tenant_id = $1 AND agent_id = $2")
            .bind(self.tenant())
            .bind(agent_id.as_str())
            .execute(&mut *tx)
            .await
            .map_err(db)?
            .rows_affected();
        tx.commit().await.map_err(db)?;
        Ok(done > 0)
    }

    pub async fn workspace_has_remote_agent_work(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<bool> {
        let mut tx = self.begin().await?;
        let pending: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM remote_agent WHERE tenant_id = $1 AND workspace_id = $2) \
             OR EXISTS (SELECT 1 FROM remote_final_stop WHERE tenant_id = $1 AND workspace_id = $2)",
        )
        .bind(self.tenant())
        .bind(workspace_id.as_str())
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(pending)
    }

    pub async fn finish_final_stop(&self, agent_id: &AgentId, stop_id: &str) -> Result<bool> {
        let mut tx = self.begin_write().await?;
        let reason: Option<String> = sqlx::query_scalar(
            "DELETE FROM remote_final_stop WHERE tenant_id = $1 AND agent_id = $2 AND stop_id = $3 \
             RETURNING reason",
        )
        .bind(self.tenant())
        .bind(agent_id.as_str())
        .bind(stop_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        let Some(reason) = reason else {
            return Ok(false);
        };
        if !FinalStopReason::parse(&reason).is_some_and(FinalStopReason::keeps_placement) {
            sqlx::query("DELETE FROM remote_agent WHERE tenant_id = $1 AND agent_id = $2")
                .bind(self.tenant())
                .bind(agent_id.as_str())
                .execute(&mut *tx)
                .await
                .map_err(db)?;
        }
        tx.commit().await.map_err(db)?;
        Ok(true)
    }

    pub async fn list_cloud_placed_agents(&self) -> Result<Vec<(WorkspaceId, AgentId)>> {
        // SQLite has no ORDER BY and scans in rowid order; `row_seq` is the
        // same insertion order, made explicit because Postgres has no default.
        let mut tx = self.begin().await?;
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT workspace_id, id FROM agent_session WHERE tenant_id = $1 \
             AND sqlite_json_extract(metadata, '$.remotePlacement.placement.target') = 'e2b' \
             ORDER BY row_seq",
        )
        .bind(self.tenant())
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(rows
            .into_iter()
            .map(|(workspace, agent)| (WorkspaceId::from(workspace), AgentId::from(agent)))
            .collect())
    }
}
