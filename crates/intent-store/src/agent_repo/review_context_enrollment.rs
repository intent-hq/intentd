//! Private native-family correlation. No serialization, public projection or authority.
use super::{AgentId, Error, Result, Row, Store, WorkspaceId};
use crate::{RepositoryAcpInitialization, RepositoryInitializationBinding};

/// Random candidate retained by the original startup before calling the peer.
/// Deliberately neither deserializable nor constructible from public identity.
pub struct ReviewContextOwnerKey(String);

impl ReviewContextOwnerKey {
    #[must_use]
    pub fn generate() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }

    /// Only for the approved private ACP startup context, never a public DTO.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Exact original persisted family. A read is only a constraint on fresh admission.
pub struct ReviewContextEnrollment {
    workspace: WorkspaceId,
    agent: AgentId,
    session: String,
    owner: ReviewContextOwnerKey,
}

impl ReviewContextEnrollment {
    #[must_use]
    pub fn owner_key(&self) -> &str {
        self.owner.as_str()
    }
}

/// Creation may insert only with its winning ID write; a load is read-only.
pub enum ReviewContextEnrollmentIntent {
    Created(ReviewContextOwnerKey),
    Loaded(ReviewContextEnrollment),
}

fn unavailable() -> Error {
    Error::Internal("private native enrollment unavailable".into())
}
fn valid_session(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= 256 && !value.contains('\0')
}

impl Store {
    /// Select only a matching local enrollment. Missing/invalid evidence is not
    /// backfilled and cannot authorize ordinary fallback or native replay.
    ///
    /// # Errors
    /// Returns a generic error for unreadable or invalid present enrollment,
    /// without exposing private values or downgrading it to absence.
    pub async fn get_review_context_enrollment(
        &self,
        workspace: &WorkspaceId,
        agent: &AgentId,
        session: &str,
    ) -> Result<Option<ReviewContextEnrollment>> {
        if !valid_session(session) {
            return Err(unavailable());
        }
        let row = sqlx::query("SELECT e.owner_key,e.version FROM review_context_enrollment e JOIN agent_session s ON s.id=e.agent_id AND s.workspace_id=e.workspace_id AND s.acp_session_id=e.acp_session_id WHERE e.agent_id=? AND e.workspace_id=? AND e.acp_session_id=?")
            .bind(&agent.0).bind(&workspace.0).bind(session).fetch_optional(self.read_pool()).await
            .map_err(|_| unavailable())?;
        let Some(row) = row else {
            // Either durable family denial or surviving original provenance
            // disproves ordinary lineage. Neither recovers a native owner or
            // authorizes replay; an unmarked unknown family is not proven safe.
            let private: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM review_context_private_family WHERE acp_session_id=?) OR EXISTS(SELECT 1 FROM pr_monitor_message_provenance WHERE agent_id=?)")
                .bind(session).bind(&agent.0).fetch_one(self.read_pool()).await.map_err(|_| unavailable())?;
            if private {
                return Err(unavailable());
            }
            return Ok(None);
        };
        let key: String = row.try_get("owner_key").map_err(|_| unavailable())?;
        let uuid = uuid::Uuid::parse_str(&key).map_err(|_| unavailable())?;
        let version: i64 = row.try_get("version").map_err(|_| unavailable())?;
        if version != 1 || uuid.get_version_num() != 4 || uuid.to_string() != key {
            return Err(unavailable());
        }
        Ok(Some(ReviewContextEnrollment {
            workspace: workspace.clone(),
            agent: agent.clone(),
            session: session.into(),
            owner: ReviewContextOwnerKey(key),
        }))
    }

    pub(crate) async fn apply_review_context_enrollment(
        conn: &mut sqlx::SqliteConnection,
        binding: &RepositoryInitializationBinding,
        intent: &ReviewContextEnrollmentIntent,
    ) -> Result<()> {
        match (intent, &binding.action) {
            (
                ReviewContextEnrollmentIntent::Created(owner),
                RepositoryAcpInitialization::FirstSet { session_id }
                | RepositoryAcpInitialization::Replace { session_id, .. },
            ) => {
                if !valid_session(session_id) {
                    return Err(unavailable());
                }
                sqlx::query("INSERT INTO review_context_enrollment(agent_id,workspace_id,acp_session_id,owner_key,version) VALUES (?,?,?,?,1)")
                    .bind(&binding.agent_id.0).bind(&binding.workspace_id.0).bind(session_id)
                    .bind(owner.as_str()).execute(conn).await.map_err(|_| unavailable())?;
            }
            (
                ReviewContextEnrollmentIntent::Loaded(original),
                RepositoryAcpInitialization::Loaded { session_id },
            ) => {
                if original.workspace != binding.workspace_id
                    || original.agent != binding.agent_id
                    || &original.session != session_id
                {
                    return Err(unavailable());
                }
                let matches: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM review_context_enrollment WHERE agent_id=? AND workspace_id=? AND acp_session_id=? AND owner_key=? AND version=1)")
                    .bind(&original.agent.0).bind(&original.workspace.0).bind(&original.session)
                    .bind(original.owner_key()).fetch_one(conn).await.map_err(|_| unavailable())?;
                if !matches {
                    return Err(unavailable());
                }
            }
            _ => return Err(unavailable()),
        }
        Ok(())
    }
}
