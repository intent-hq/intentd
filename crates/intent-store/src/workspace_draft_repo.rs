//! Durable workspace-draft repository with optimistic revisions.

use intent_core::{
    now_iso, AgentId, ContextLink, DraftDelivery, DraftPhase, DraftSource, Error, Result,
    WorkspaceDraft, WorkspaceDraftId, WorkspaceId,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use sqlx::sqlite::SqliteRow;
use sqlx::Row;

use crate::{enum_from_db, enum_to_db, Store};

const COLUMNS: &str = "id, owner_client_id, revision, phase, title, intent_text, source, \
    context_links, attachments, config, operation_key, promoted_workspace_id, initial_agent_id, \
    delivery, last_error, created_at, updated_at";

/// Editable fields accepted by an optimistic workspace-draft update.
#[derive(Debug, Clone, Default)]
pub struct WorkspaceDraftPatch {
    pub title: Option<Option<String>>,
    pub intent_text: Option<String>,
    pub source: Option<Option<DraftSource>>,
    pub context_links: Option<Vec<ContextLink>>,
    pub attachments: Option<Vec<serde_json::Value>>,
    pub config: Option<serde_json::Map<String, serde_json::Value>>,
    pub last_error: Option<Option<String>>,
}

impl Store {
    /// Insert and return a durable workspace draft.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if serialization, insertion, or reloading fails.
    pub async fn create_workspace_draft(&self, draft: &WorkspaceDraft) -> Result<WorkspaceDraft> {
        let sql = format!(
            "INSERT INTO workspace_draft ({COLUMNS}) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        );
        sqlx::query(&sql)
            .bind(&draft.id.0)
            .bind(&draft.owner_client_id.0)
            .bind(u64_to_i64(draft.revision, "revision")?)
            .bind(enum_to_db(&draft.phase)?)
            .bind(&draft.title)
            .bind(&draft.intent_text)
            .bind(optional_json_to_db(draft.source.as_ref(), "source")?)
            .bind(json_to_db(&draft.context_links, "context_links")?)
            .bind(json_to_db(&draft.attachments, "attachments")?)
            .bind(json_to_db(&draft.config, "config")?)
            .bind(&draft.operation_key)
            .bind(draft.promoted_workspace_id.as_ref().map(|id| &id.0))
            .bind(draft.initial_agent_id.as_ref().map(|id| &id.0))
            .bind(json_to_db(&draft.delivery, "delivery")?)
            .bind(&draft.last_error)
            .bind(&draft.created_at)
            .bind(&draft.updated_at)
            .execute(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("create workspace draft failed: {e}")))?;
        self.get_workspace_draft(&draft.id).await
    }

    /// Fetch a workspace draft by id.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` when absent, or `Error::Internal` on read/decode failure.
    pub async fn get_workspace_draft(&self, id: &WorkspaceDraftId) -> Result<WorkspaceDraft> {
        let sql = format!("SELECT {COLUMNS} FROM workspace_draft WHERE id = ?");
        let row = sqlx::query(&sql)
            .bind(&id.0)
            .fetch_optional(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("get workspace draft failed: {e}")))?;
        row.as_ref()
            .map(map_workspace_draft_row)
            .transpose()?
            .ok_or_else(|| Error::NotFound(format!("workspace draft {id}")))
    }

    /// List every non-promoted draft, oldest first.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if reading or decoding rows fails.
    pub async fn list_open_workspace_drafts(&self) -> Result<Vec<WorkspaceDraft>> {
        let sql = format!(
            "SELECT {COLUMNS} FROM workspace_draft WHERE phase <> 'promoted' ORDER BY created_at, id"
        );
        let rows = sqlx::query(&sql)
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("list workspace drafts failed: {e}")))?;
        rows.iter().map(map_workspace_draft_row).collect()
    }

    /// Apply an editable patch only when `expected_revision` still matches.
    ///
    /// # Errors
    ///
    /// Returns `Error::Conflict` on a stale revision, `Error::NotFound` when
    /// absent, or `Error::Internal` on persistence/serialization failure.
    pub async fn update_workspace_draft_with_revision(
        &self,
        id: &WorkspaceDraftId,
        expected_revision: u64,
        patch: WorkspaceDraftPatch,
    ) -> Result<WorkspaceDraft> {
        let mut current = self.get_workspace_draft(id).await?;
        if current.revision != expected_revision {
            return Err(conflict(&current)?);
        }
        if let Some(title) = patch.title {
            current.title = title;
        }
        if let Some(intent_text) = patch.intent_text {
            current.intent_text = intent_text;
        }
        if let Some(source) = patch.source {
            current.source = source;
        }
        if let Some(context_links) = patch.context_links {
            current.context_links = context_links;
        }
        if let Some(attachments) = patch.attachments {
            current.attachments = attachments;
        }
        if let Some(config) = patch.config {
            current.config = config;
        }
        if let Some(last_error) = patch.last_error {
            current.last_error = last_error;
        }
        let updated_at = now_iso();
        let res = sqlx::query(
            "UPDATE workspace_draft SET title=?, intent_text=?, source=?, context_links=?, \
             attachments=?, config=?, last_error=?, revision=revision+1, updated_at=? \
             WHERE id=? AND revision=?",
        )
        .bind(&current.title)
        .bind(&current.intent_text)
        .bind(optional_json_to_db(current.source.as_ref(), "source")?)
        .bind(json_to_db(&current.context_links, "context_links")?)
        .bind(json_to_db(&current.attachments, "attachments")?)
        .bind(json_to_db(&current.config, "config")?)
        .bind(&current.last_error)
        .bind(updated_at)
        .bind(&id.0)
        .bind(u64_to_i64(expected_revision, "expected_revision")?)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("update workspace draft failed: {e}")))?;
        if res.rows_affected() == 0 {
            let latest = self.get_workspace_draft(id).await?;
            return Err(conflict(&latest)?);
        }
        self.get_workspace_draft(id).await
    }

    /// Set a draft phase and optional failure detail, incrementing its revision.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` when absent, or `Error::Internal` on write failure.
    pub async fn set_workspace_draft_phase(
        &self,
        id: &WorkspaceDraftId,
        phase: DraftPhase,
        last_error: Option<&str>,
    ) -> Result<WorkspaceDraft> {
        let res = sqlx::query(
            "UPDATE workspace_draft SET phase=?, last_error=?, revision=revision+1, updated_at=? \
             WHERE id=?",
        )
        .bind(enum_to_db(&phase)?)
        .bind(last_error)
        .bind(now_iso())
        .bind(&id.0)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("set workspace draft phase failed: {e}")))?;
        require_updated(res.rows_affected(), id)?;
        self.get_workspace_draft(id).await
    }

    /// Record the exactly-once promotion mapping and mark the draft promoted.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` when absent, or `Error::Internal` on write failure.
    pub async fn set_workspace_draft_promotion(
        &self,
        id: &WorkspaceDraftId,
        workspace_id: &WorkspaceId,
        agent_id: Option<&AgentId>,
    ) -> Result<WorkspaceDraft> {
        let res = sqlx::query(
            "UPDATE workspace_draft SET phase='promoted', promoted_workspace_id=?, \
             initial_agent_id=?, last_error=NULL, revision=revision+1, updated_at=? WHERE id=?",
        )
        .bind(&workspace_id.0)
        .bind(agent_id.map(|id| &id.0))
        .bind(now_iso())
        .bind(&id.0)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("set workspace draft promotion failed: {e}")))?;
        require_updated(res.rows_affected(), id)?;
        self.get_workspace_draft(id).await
    }

    /// Replace delivery reconciliation state and increment the draft revision.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` when absent, or `Error::Internal` on write failure.
    pub async fn set_workspace_draft_delivery(
        &self,
        id: &WorkspaceDraftId,
        delivery: &DraftDelivery,
    ) -> Result<WorkspaceDraft> {
        let res = sqlx::query(
            "UPDATE workspace_draft SET delivery=?, revision=revision+1, updated_at=? WHERE id=?",
        )
        .bind(json_to_db(delivery, "delivery")?)
        .bind(now_iso())
        .bind(&id.0)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("set workspace draft delivery failed: {e}")))?;
        require_updated(res.rows_affected(), id)?;
        self.get_workspace_draft(id).await
    }

    /// Delete a draft, returning whether a row existed.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the delete fails.
    pub async fn delete_workspace_draft(&self, id: &WorkspaceDraftId) -> Result<bool> {
        let result = sqlx::query("DELETE FROM workspace_draft WHERE id = ?")
            .bind(&id.0)
            .execute(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("delete workspace draft failed: {e}")))?;
        Ok(result.rows_affected() > 0)
    }
}

fn require_updated(rows_affected: u64, id: &WorkspaceDraftId) -> Result<()> {
    if rows_affected == 0 {
        Err(Error::NotFound(format!("workspace draft {id}")))
    } else {
        Ok(())
    }
}

fn conflict(current: &WorkspaceDraft) -> Result<Error> {
    serde_json::to_value(current)
        .map(|current| Error::Conflict { current })
        .map_err(|e| Error::Internal(format!("encode workspace draft conflict failed: {e}")))
}

fn u64_to_i64(value: u64, name: &str) -> Result<i64> {
    i64::try_from(value).map_err(|_| Error::InvalidParams(format!("{name} exceeds SQLite range")))
}

fn json_to_db<T: Serialize + ?Sized>(value: &T, name: &str) -> Result<String> {
    serde_json::to_string(value).map_err(|e| Error::Internal(format!("encode {name} failed: {e}")))
}

fn optional_json_to_db<T: Serialize>(value: Option<&T>, name: &str) -> Result<Option<String>> {
    value.map(|value| json_to_db(value, name)).transpose()
}

fn json_from_db<T: DeserializeOwned>(value: &str, name: &str) -> Result<T> {
    serde_json::from_str(value).map_err(|e| Error::Internal(format!("decode {name} failed: {e}")))
}

fn map_workspace_draft_row(row: &SqliteRow) -> Result<WorkspaceDraft> {
    let revision =
        u64::try_from(row.try_get::<i64, _>("revision").map_err(|e| {
            Error::Internal(format!("workspace draft column revision failed: {e}"))
        })?)
        .map_err(|_| Error::Internal("workspace draft revision is negative".to_string()))?;
    let source = row
        .try_get::<Option<String>, _>("source")
        .map_err(|e| Error::Internal(format!("workspace draft column source failed: {e}")))?
        .map(|value| json_from_db(&value, "source"))
        .transpose()?;
    Ok(WorkspaceDraft {
        id: WorkspaceDraftId(row.get("id")),
        owner_client_id: intent_core::ClientId(row.get("owner_client_id")),
        revision,
        phase: enum_from_db(&row.get::<String, _>("phase"))?,
        title: row.get("title"),
        intent_text: row.get("intent_text"),
        source,
        context_links: json_from_db(&row.get::<String, _>("context_links"), "context_links")?,
        attachments: json_from_db(&row.get::<String, _>("attachments"), "attachments")?,
        config: json_from_db(&row.get::<String, _>("config"), "config")?,
        operation_key: row.get("operation_key"),
        promoted_workspace_id: row
            .get::<Option<String>, _>("promoted_workspace_id")
            .map(WorkspaceId),
        initial_agent_id: row
            .get::<Option<String>, _>("initial_agent_id")
            .map(AgentId),
        delivery: json_from_db(&row.get::<String, _>("delivery"), "delivery")?,
        last_error: row.get("last_error"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use intent_core::{
        ClientId, ContextLinkKind, DraftDeliveryState, DraftIsolation, WorkspaceDraftId,
    };
    use serde_json::json;
    use uuid::Uuid;

    struct TempDb(std::path::PathBuf);

    impl TempDb {
        fn new() -> Self {
            Self(std::env::temp_dir().join(format!("workspace-draft-{}.db", Uuid::new_v4())))
        }
    }

    impl Drop for TempDb {
        fn drop(&mut self) {
            for suffix in ["", "-wal", "-shm"] {
                let _ = std::fs::remove_file(format!("{}{suffix}", self.0.display()));
            }
        }
    }

    fn draft(operation_key: &str) -> WorkspaceDraft {
        let now = now_iso();
        WorkspaceDraft {
            id: WorkspaceDraftId::new(),
            owner_client_id: ClientId::new(),
            revision: 0,
            phase: DraftPhase::Editing,
            title: Some("Draft title".to_string()),
            intent_text: "Build it".to_string(),
            source: Some(DraftSource::Local {
                path: "/tmp/project".to_string(),
                branch: Some("main".to_string()),
                isolation: DraftIsolation::Worktree,
            }),
            context_links: vec![ContextLink {
                kind: ContextLinkKind::Issue,
                url: "https://github.com/intent-hq/intent/issues/1".to_string(),
                owner: "intent-hq".to_string(),
                repo: "intent".to_string(),
                number: 1,
            }],
            attachments: vec![json!({ "id": "attachment-1" })],
            config: serde_json::from_value(json!({ "model": "test" })).unwrap(),
            operation_key: operation_key.to_string(),
            promoted_workspace_id: None,
            initial_agent_id: None,
            delivery: DraftDelivery::default(),
            last_error: None,
            created_at: now.clone(),
            updated_at: now,
        }
    }

    #[tokio::test]
    async fn create_get_list_update_conflict_and_delete() {
        let db = TempDb::new();
        let store = Store::open(&db.0).await.unwrap();
        let original = draft("operation-1");
        let created = store.create_workspace_draft(&original).await.unwrap();
        assert_eq!(created, original);
        assert_eq!(
            store.get_workspace_draft(&original.id).await.unwrap(),
            original
        );
        assert_eq!(
            store.list_open_workspace_drafts().await.unwrap(),
            vec![original.clone()]
        );

        let updated = store
            .update_workspace_draft_with_revision(
                &original.id,
                0,
                WorkspaceDraftPatch {
                    intent_text: Some("Changed".to_string()),
                    title: Some(None),
                    ..WorkspaceDraftPatch::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(updated.revision, 1);
        assert_eq!(updated.intent_text, "Changed");
        assert_eq!(updated.title, None);

        let error = store
            .update_workspace_draft_with_revision(&original.id, 0, WorkspaceDraftPatch::default())
            .await
            .unwrap_err();
        let Error::Conflict { current } = error else {
            panic!("expected conflict")
        };
        assert_eq!(current["revision"], 1);

        assert!(store.delete_workspace_draft(&original.id).await.unwrap());
        assert!(!store.delete_workspace_draft(&original.id).await.unwrap());
        assert!(matches!(
            store.get_workspace_draft(&original.id).await,
            Err(Error::NotFound(_))
        ));
    }

    #[tokio::test]
    async fn operation_key_is_unique() {
        let db = TempDb::new();
        let store = Store::open(&db.0).await.unwrap();
        store
            .create_workspace_draft(&draft("same-key"))
            .await
            .unwrap();
        assert!(store
            .create_workspace_draft(&draft("same-key"))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn phase_promotion_and_delivery_are_revisioned() {
        let db = TempDb::new();
        let store = Store::open(&db.0).await.unwrap();
        let original = draft("operation-2");
        store.create_workspace_draft(&original).await.unwrap();
        let failed = store
            .set_workspace_draft_phase(&original.id, DraftPhase::Failed, Some("clone failed"))
            .await
            .unwrap();
        assert_eq!(failed.revision, 1);
        assert_eq!(failed.last_error.as_deref(), Some("clone failed"));

        let workspace_id = WorkspaceId::new();
        let agent_id = AgentId::new();
        let promoted = store
            .set_workspace_draft_promotion(&original.id, &workspace_id, Some(&agent_id))
            .await
            .unwrap();
        assert_eq!(promoted.phase, DraftPhase::Promoted);
        assert_eq!(promoted.promoted_workspace_id, Some(workspace_id));
        assert_eq!(promoted.initial_agent_id, Some(agent_id));
        assert_eq!(promoted.revision, 2);
        assert!(store.list_open_workspace_drafts().await.unwrap().is_empty());

        let delivery = DraftDelivery {
            state: DraftDeliveryState::Sent,
            message_id: Some("message-1".to_string()),
            error: None,
        };
        let delivered = store
            .set_workspace_draft_delivery(&original.id, &delivery)
            .await
            .unwrap();
        assert_eq!(delivered.delivery, delivery);
        assert_eq!(delivered.revision, 3);
    }
}
