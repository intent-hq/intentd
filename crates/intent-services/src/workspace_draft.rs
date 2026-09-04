//! Durable pre-workspace draft operations and promotion orchestration.

use std::path::PathBuf;
use std::sync::Arc;

use intent_core::{
    now_iso, ClientId, ContextLink, DraftDelivery, DraftIsolation, DraftPhase, DraftSource, Error,
    WorkspaceApi, WorkspaceCreate, WorkspaceCreateInitialAgent, WorkspaceDraft, WorkspaceDraftId,
    WorkspaceId,
};
use intent_store::{NewEvent, WorkspaceDraftPatch};
use serde_json::{json, Map, Value};

use crate::{publish_event, system_actor, Result, Services};

impl Services {
    pub(crate) async fn workspace_draft_create_op(&self, input: Value) -> Result<Value> {
        let input = object(input, "workspaceDraft.create params")?;
        let now = now_iso();
        let draft = WorkspaceDraft {
            id: WorkspaceDraftId::new(),
            owner_client_id: optional::<ClientId>(&input, "ownerClientId")?
                .unwrap_or_else(ClientId::new),
            revision: 0,
            phase: DraftPhase::Editing,
            title: optional(&input, "title")?,
            intent_text: optional(&input, "intentText")?.unwrap_or_default(),
            source: optional(&input, "source")?,
            context_links: optional::<Vec<ContextLink>>(&input, "contextLinks")?
                .unwrap_or_default(),
            attachments: optional(&input, "attachments")?.unwrap_or_default(),
            config: optional(&input, "config")?.unwrap_or_default(),
            operation_key: uuid::Uuid::new_v4().to_string(),
            promoted_workspace_id: None,
            initial_agent_id: None,
            delivery: DraftDelivery::default(),
            last_error: None,
            created_at: now.clone(),
            updated_at: now,
        };
        let draft = self.store.create_workspace_draft(&draft).await?;
        self.publish_workspace_draft_updated(&draft).await;
        encode(draft)
    }

    pub(crate) async fn workspace_draft_get_op(&self, id: WorkspaceDraftId) -> Result<Value> {
        encode(self.store.get_workspace_draft(&id).await?)
    }

    pub(crate) async fn workspace_draft_list_op(&self) -> Result<Value> {
        encode(self.store.list_open_workspace_drafts().await?)
    }

    pub(crate) async fn workspace_draft_update_op(
        &self,
        id: WorkspaceDraftId,
        expected_revision: u64,
        patch: Value,
    ) -> Result<Value> {
        let patch = parse_patch(object(patch, "workspaceDraft.update patch")?)?;
        let draft = self
            .store
            .update_workspace_draft_with_revision(&id, expected_revision, patch)
            .await?;
        self.publish_workspace_draft_updated(&draft).await;
        encode(draft)
    }

    pub(crate) async fn workspace_draft_promote_op(
        &self,
        id: WorkspaceDraftId,
        expected_revision: u64,
        initial_agent: Option<Value>,
    ) -> Result<Value> {
        let gate = {
            let mut locks = self
                .workspace_draft_promotion_locks
                .lock()
                .map_err(|_| Error::Internal("workspace draft promotion lock poisoned".into()))?;
            locks
                .entry(id.clone())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                .clone()
        };
        let _guard = gate.lock().await;
        let draft = self.store.get_workspace_draft(&id).await?;

        if let Some(workspace_id) = draft.promoted_workspace_id.clone() {
            if draft.phase == DraftPhase::Promoted {
                return self.promotion_result(draft, workspace_id).await;
            }
            match self.store.get_workspace(&workspace_id).await {
                Ok(workspace) => {
                    let initial_agent = self.recover_initial_agent(&workspace_id).await?;
                    let agent_id = initial_agent.as_ref().map(|agent| agent.id.clone());
                    let promoted = self
                        .store
                        .set_workspace_draft_promotion(&id, &workspace_id, agent_id.as_ref())
                        .await?;
                    self.publish_workspace_draft_promoted(&promoted).await;
                    let mut result = json!({ "draft": promoted, "workspace": workspace });
                    if let Some(agent) = initial_agent {
                        result["initialAgent"] = encode(agent)?;
                    }
                    return Ok(result);
                }
                Err(Error::NotFound(_)) => {}
                Err(error) => return Err(error),
            }
        }
        if !matches!(draft.phase, DraftPhase::Promoting) && draft.revision != expected_revision {
            return Err(Error::Conflict {
                current: serde_json::to_value(&draft).map_err(|e| {
                    Error::Internal(format!("encode workspace draft conflict failed: {e}"))
                })?,
            });
        }

        let initial_agent = initial_agent
            .map(|value| {
                serde_json::from_value::<WorkspaceCreateInitialAgent>(value)
                    .map_err(|e| Error::InvalidParams(format!("invalid initialAgent: {e}")))
            })
            .transpose()?;
        let input = promotion_input(&draft, initial_agent);
        let promoting = if draft.phase == DraftPhase::Promoting {
            draft
        } else {
            self.store
                .set_workspace_draft_phase(&id, DraftPhase::Promoting, None)
                .await?
        };
        self.publish_workspace_draft_updated(&promoting).await;

        let created = match async {
            validate_new_folder_target(&promoting).await?;
            WorkspaceApi::create_workspace(self, input, Some(promoting.operation_key.clone())).await
        }
        .await
        {
            Ok(created) => created,
            Err(error) => {
                let message = error.to_string();
                if let Ok(failed) = self
                    .store
                    .set_workspace_draft_phase(&id, DraftPhase::Failed, Some(&message))
                    .await
                {
                    self.publish_workspace_draft_updated(&failed).await;
                }
                return Err(error);
            }
        };
        let agent_id = created
            .initial_agent
            .as_ref()
            .and_then(|agent| agent.get("id"))
            .and_then(Value::as_str)
            .map(intent_core::AgentId::from);
        let promoted = self
            .store
            .set_workspace_draft_promotion(&id, &created.workspace.id, agent_id.as_ref())
            .await?;
        self.publish_workspace_draft_promoted(&promoted).await;
        let mut result = json!({ "draft": promoted, "workspace": created.workspace });
        if let Some(agent) = created.initial_agent {
            result["initialAgent"] = agent;
        }
        Ok(result)
    }

    pub(crate) async fn workspace_draft_mark_delivery_op(
        &self,
        id: WorkspaceDraftId,
        delivery: Value,
    ) -> Result<Value> {
        let delivery: DraftDelivery = serde_json::from_value(delivery)
            .map_err(|e| Error::InvalidParams(format!("invalid delivery: {e}")))?;
        let draft = self
            .store
            .set_workspace_draft_delivery(&id, &delivery)
            .await?;
        self.publish_workspace_draft_updated(&draft).await;
        encode(draft)
    }

    pub(crate) async fn workspace_draft_delete_op(&self, id: WorkspaceDraftId) -> Result<Value> {
        let deleted = self.store.delete_workspace_draft(&id).await?;
        if deleted {
            publish_event(
                self.event_bus.as_ref(),
                draft_event("workspace-draft:deleted", json!({ "draftId": id.as_str() })),
            )
            .await;
        }
        Ok(json!({ "deleted": deleted }))
    }

    async fn promotion_result(
        &self,
        draft: WorkspaceDraft,
        workspace_id: WorkspaceId,
    ) -> Result<Value> {
        let workspace = self.store.get_workspace(&workspace_id).await?;
        let mut result = json!({ "draft": draft, "workspace": workspace });
        if let Some(agent_id) = draft.initial_agent_id {
            let agent = WorkspaceApi::agent_get(self, agent_id, Some(workspace_id)).await?;
            result["initialAgent"] = encode(agent)?;
        }
        Ok(result)
    }

    async fn recover_initial_agent(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Option<intent_core::AgentLite>> {
        let sessions = self
            .store
            .list_agent_session_summaries(workspace_id)
            .await?;
        let initial = sessions.into_iter().find(|session| {
            session
                .metadata
                .as_ref()
                .and_then(Value::as_object)
                .and_then(|metadata| metadata.get("isInitialAgent"))
                .and_then(Value::as_bool)
                == Some(true)
        });
        match initial {
            Some(session) => WorkspaceApi::agent_get(self, session.id, Some(workspace_id.clone()))
                .await
                .map(Some),
            None => Ok(None),
        }
    }

    async fn publish_workspace_draft_updated(&self, draft: &WorkspaceDraft) {
        publish_event(
            self.event_bus.as_ref(),
            draft_event("workspace-draft:updated", json!({ "draft": draft })),
        )
        .await;
    }

    async fn publish_workspace_draft_promoted(&self, draft: &WorkspaceDraft) {
        let mut data = json!({
            "draftId": draft.id.as_str(),
            "workspaceId": draft.promoted_workspace_id,
        });
        if let Some(agent_id) = &draft.initial_agent_id {
            data["initialAgentId"] = json!(agent_id.as_str());
        }
        publish_event(
            self.event_bus.as_ref(),
            draft_event("workspace-draft:promoted", data),
        )
        .await;
    }
}

fn promotion_input(
    draft: &WorkspaceDraft,
    initial_agent: Option<WorkspaceCreateInitialAgent>,
) -> WorkspaceCreate {
    let mut input = WorkspaceCreate {
        title: draft.title.clone(),
        context_links: Some(draft.context_links.clone()),
        setup_script: draft
            .config
            .get("setupScript")
            .and_then(Value::as_str)
            .map(str::to_string),
        is_remote: draft.config.get("isRemote").and_then(Value::as_bool),
        default_model: draft
            .config
            .get("model")
            .and_then(Value::as_str)
            .map(str::to_string),
        initial_agent,
        workspace_draft_id: Some(draft.id.clone()),
        ..WorkspaceCreate::default()
    };
    match &draft.source {
        Some(DraftSource::Local {
            path,
            branch,
            isolation,
        }) => {
            input.repository_path = Some(path.clone());
            input.base_ref.clone_from(branch);
            input.skip_isolation = Some(*isolation == DraftIsolation::InPlace);
        }
        Some(DraftSource::Github {
            url,
            owner,
            name,
            branch,
        }) => {
            input.github_url = Some(url.clone());
            input.repository_owner = Some(owner.clone());
            input.repository_name = Some(name.clone());
            input.base_ref.clone_from(branch);
        }
        Some(DraftSource::NewFolder { parent_path, name }) => {
            input.repository_path = Some(
                PathBuf::from(parent_path)
                    .join(name)
                    .to_string_lossy()
                    .into(),
            );
            input.is_new_repo = Some(true);
            input.skip_isolation = Some(true);
        }
        None => {}
    }
    input
}

async fn validate_new_folder_target(draft: &WorkspaceDraft) -> Result<()> {
    let Some(DraftSource::NewFolder { parent_path, name }) = &draft.source else {
        return Ok(());
    };
    let path = PathBuf::from(parent_path).join(name);
    let metadata = match tokio::fs::metadata(&path).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(Error::Internal(format!(
                "workspaceDraft.promote: inspect new project path {} failed: {error}",
                path.display()
            )));
        }
    };
    if !metadata.is_dir() {
        return Err(Error::InvalidParams(format!(
            "new project path already exists and is not a directory: {}",
            path.display()
        )));
    }
    let mut entries = tokio::fs::read_dir(&path).await.map_err(|error| {
        Error::Internal(format!(
            "workspaceDraft.promote: read new project directory {} failed: {error}",
            path.display()
        ))
    })?;
    if entries
        .next_entry()
        .await
        .map_err(|error| {
            Error::Internal(format!(
                "workspaceDraft.promote: read new project directory {} failed: {error}",
                path.display()
            ))
        })?
        .is_some()
    {
        return Err(Error::InvalidParams(format!(
            "new project directory already exists and is not empty: {}",
            path.display()
        )));
    }
    Ok(())
}

fn parse_patch(mut patch: Map<String, Value>) -> Result<WorkspaceDraftPatch> {
    let title = clearable(&mut patch, "title")?;
    let source = clearable(&mut patch, "source")?;
    let parsed = WorkspaceDraftPatch {
        title,
        intent_text: take(&mut patch, "intentText")?,
        source,
        context_links: take(&mut patch, "contextLinks")?,
        attachments: take(&mut patch, "attachments")?,
        config: take(&mut patch, "config")?,
        last_error: None,
    };
    if !patch.is_empty() {
        return Err(Error::InvalidParams(format!(
            "unsupported workspace draft patch field: {}",
            patch.keys().next().expect("not empty")
        )));
    }
    Ok(parsed)
}

fn object(value: Value, name: &str) -> Result<Map<String, Value>> {
    match value {
        Value::Object(map) => Ok(map),
        _ => Err(Error::InvalidParams(format!("{name} must be an object"))),
    }
}

fn optional<T: serde::de::DeserializeOwned>(
    map: &Map<String, Value>,
    key: &str,
) -> Result<Option<T>> {
    map.get(key)
        .filter(|value| !value.is_null())
        .cloned()
        .map(|value| {
            serde_json::from_value(value)
                .map_err(|e| Error::InvalidParams(format!("invalid {key}: {e}")))
        })
        .transpose()
}

fn take<T: serde::de::DeserializeOwned>(
    map: &mut Map<String, Value>,
    key: &str,
) -> Result<Option<T>> {
    map.remove(key)
        .map(|value| {
            serde_json::from_value(value)
                .map_err(|e| Error::InvalidParams(format!("invalid {key}: {e}")))
        })
        .transpose()
}

#[allow(clippy::option_option)] // Outer None = omitted; inner None = explicit null.
fn clearable<T: serde::de::DeserializeOwned>(
    map: &mut Map<String, Value>,
    key: &str,
) -> Result<Option<Option<T>>> {
    match map.remove(key) {
        None => Ok(None),
        Some(Value::Null) => Ok(Some(None)),
        Some(value) => serde_json::from_value(value)
            .map(Some)
            .map(Some)
            .map_err(|e| Error::InvalidParams(format!("invalid {key}: {e}"))),
    }
}

fn encode<T: serde::Serialize>(value: T) -> Result<Value> {
    serde_json::to_value(value)
        .map_err(|e| Error::Internal(format!("encode workspace draft result failed: {e}")))
}

fn draft_event(event_type: &str, data: Value) -> NewEvent {
    NewEvent {
        workspace_id: WorkspaceId::from(""),
        timestamp: now_iso(),
        event_type: event_type.to_string(),
        actor: system_actor(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data,
    }
}
