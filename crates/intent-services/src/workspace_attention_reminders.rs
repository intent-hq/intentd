//! Person-specific acknowledgement leaves canonical work and question state intact.
use std::collections::{BTreeSet, HashMap};
use std::fmt::Write as _;

use intent_core::{
    current_caller, AgentId, AgentSession, AgentStatus, AttentionReminderReason, Caller, Error,
    Result, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceAttentionReminder,
    WorkspaceDisplayStatus, WorkspaceId,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::{publish_event, workspace_updated_event, Services};

impl Services {
    pub(crate) async fn attention_reminder_reasons(
        &self,
        workspace: &Workspace,
        sessions: &[AgentSession],
        state: &HashMap<String, Value>,
        legacy_questions: Option<&HashMap<AgentId, String>>,
    ) -> Result<Vec<AttentionReminderReason>> {
        for (key, value) in state {
            if (key == "review" || key.starts_with("discussion:"))
                && value.as_str().is_none_or(str::is_empty)
            {
                return Err(Error::Internal("Malformed reminder generation".into()));
            }
        }
        let mut reasons = Vec::new();
        if workspace.attention == WorkspaceAttention::ReviewRequired {
            reasons.push(AttentionReminderReason {
                id: "review".into(),
                revision: state
                    .get("review")
                    .and_then(Value::as_str)
                    .unwrap_or("legacy-review")
                    .into(),
            });
        }
        for session in sessions.iter().filter(|s| {
            s.parent_agent_id.is_none()
                && !s.is_background
                && s.status != AgentStatus::Deleted
                && s.retired_at.is_none()
                && !s.notifications_muted
        }) {
            if !self.attention_surfacing_deferred(&session.id)
                && session
                    .attention_request_kind
                    .as_deref()
                    .is_some_and(|kind| kind != "blocker")
            {
                let key = format!("discussion:{}", session.id.as_str());
                reasons.push(AttentionReminderReason {
                    id: key.clone(),
                    revision: state.get(&key).and_then(Value::as_str).map_or_else(
                        || {
                            Sha256::digest(
                                json!([
                                    session.attention_request_kind.as_deref(),
                                    session.attention_request_timestamp.as_deref(),
                                    session.attention_request_reason.as_deref()
                                ])
                                .to_string()
                                .as_bytes(),
                            )
                            .iter()
                            .fold(
                                "legacy:".to_string(),
                                |mut hex, byte| {
                                    let _ = write!(hex, "{byte:02x}");
                                    hex
                                },
                            )
                        },
                        str::to_string,
                    ),
                });
            }
            if !session.pending_questions_marker_written() {
                if let Some(legacy_questions) = legacy_questions {
                    if let Some(message) = legacy_questions.get(&session.id) {
                        reasons.push(AttentionReminderReason {
                            id: format!("questions:{}", session.id.as_str()),
                            revision: message.clone(),
                        });
                    }
                    continue;
                }
            }
            if !session.pending_questions_marker_written() {
                if let Some(message) = self.try_pending_questions_from_tail(&session.id).await? {
                    reasons.push(AttentionReminderReason {
                        id: format!("questions:{}", session.id.as_str()),
                        revision: message,
                    });
                }
                continue;
            }
            if let Some(message) = session
                .pending_questions_message_id()
                .filter(|pending| session.dismissed_questions_message_id() != Some(*pending))
            {
                reasons.push(AttentionReminderReason {
                    id: format!("questions:{}", session.id.as_str()),
                    revision: message.into(),
                });
            }
        }
        reasons.sort();
        reasons.dedup();
        Ok(reasons)
    }

    /// A read failure leaves the projection absent, so it can never hide attention.
    pub(crate) async fn enrich_attention_reminder(
        &self,
        workspace: &mut Workspace,
        sessions: Option<&[AgentSession]>,
        state: Option<&HashMap<String, Value>>,
        legacy_questions: Option<&HashMap<AgentId, String>>,
    ) {
        workspace.attention_reminder = None;
        let Some(Caller::Wire { principal_id, .. }) = current_caller() else {
            return;
        };
        let Some(raw) = workspace.display_status else {
            return;
        };
        if workspace.id.is_chief() || workspace.archived {
            return;
        }
        let fetched_state;
        let empty_state = HashMap::new();
        let state = if let Some(state) = state {
            state
        } else {
            fetched_state = match self
                .store
                .workspace_reminder_state(std::slice::from_ref(&workspace.id), Some(&principal_id))
                .await
            {
                Ok(state) => state,
                Err(_) => return,
            };
            fetched_state.get(&workspace.id).unwrap_or(&empty_state)
        };
        let fetched_sessions;
        let sessions = if let Some(sessions) = sessions {
            sessions
        } else {
            fetched_sessions = match self.store.list_agent_session_summaries(&workspace.id).await {
                Ok(sessions) => sessions,
                Err(_) => return,
            };
            &fetched_sessions
        };
        let Ok(reasons) = self
            .attention_reminder_reasons(workspace, sessions, state, legacy_questions)
            .await
        else {
            return;
        };
        let receipts: BTreeSet<AttentionReminderReason> =
            match state.get(&format!("receipt:{}", principal_id.as_str())) {
                None => BTreeSet::new(),
                Some(value) => match serde_json::from_value(value.clone()) {
                    Ok(receipts) => receipts,
                    Err(_) => return,
                },
            };
        let dismissed =
            !reasons.is_empty() && reasons.iter().all(|reason| receipts.contains(reason));
        let status = if dismissed && raw == WorkspaceDisplayStatus::NeedsAttention {
            if workspace.activity == WorkspaceActivity::AgentRunning {
                "in_progress".to_string()
            } else {
                "waiting".to_string()
            }
        } else {
            serde_json::to_value(raw)
                .ok()
                .and_then(|v| v.as_str().map(str::to_string))
                .unwrap_or_else(|| "needs_attention".into())
        };
        if let Ok(mut cache) = self.last_attention_reminder_reasons.lock() {
            cache
                .entry(workspace.id.clone())
                .or_insert_with(|| reasons.clone());
        }
        workspace.attention_reminder = Some(WorkspaceAttentionReminder {
            reasons,
            dismissed,
            display_status: status,
        });
    }

    pub(crate) async fn maybe_emit_attention_reminder_changed(&self, workspace: &Workspace) {
        self.refresh_attention_reminder_reasons(workspace, false)
            .await;
    }

    pub(crate) async fn emit_attention_reminder_raise(&self, workspace: &WorkspaceId) {
        if let Ok(workspace) = self.store.get_workspace(workspace).await {
            self.refresh_attention_reminder_reasons(&workspace, true)
                .await;
        }
    }

    async fn refresh_attention_reminder_reasons(&self, workspace: &Workspace, raised: bool) {
        if workspace.id.is_chief() {
            return;
        }
        let Ok(state) = self
            .store
            .workspace_reminder_state(std::slice::from_ref(&workspace.id), None)
            .await
        else {
            return;
        };
        let Ok(sessions) = self.store.list_agent_session_summaries(&workspace.id).await else {
            return;
        };
        let empty = HashMap::new();
        let Ok(reasons) = self
            .attention_reminder_reasons(
                workspace,
                &sessions,
                state.get(&workspace.id).unwrap_or(&empty),
                None,
            )
            .await
        else {
            return;
        };
        let changed = self
            .last_attention_reminder_reasons
            .lock()
            .ok()
            .is_some_and(|mut cache| {
                cache
                    .insert(workspace.id.clone(), reasons.clone())
                    .map_or(!reasons.is_empty(), |old| old != reasons)
            });
        if changed || (raised && !reasons.is_empty()) {
            self.emit_attention_reminder_invalidation(&workspace.id)
                .await;
        }
    }

    pub(crate) async fn emit_attention_reminder_invalidation(&self, workspace: &WorkspaceId) {
        publish_event(
            self.event_bus.as_ref(),
            workspace_updated_event(workspace, &json!({"attentionReminder":true})),
        )
        .await;
    }

    pub(crate) async fn dismiss_attention_reasons_op(
        &self,
        id: WorkspaceId,
        observed: Vec<AttentionReminderReason>,
    ) -> Result<Workspace> {
        let Some(Caller::Wire { principal_id, .. }) = current_caller() else {
            return Err(Error::Forbidden(
                "reminder acknowledgement requires a person".into(),
            ));
        };
        self.require_member(&id).await?;
        let workspace = self.store.get_workspace(&id).await?;
        let sessions = self.store.list_agent_session_summaries(&id).await?;
        let state = self
            .store
            .workspace_reminder_state(std::slice::from_ref(&id), Some(&principal_id))
            .await?;
        let empty = HashMap::new();
        let current: BTreeSet<_> = self
            .attention_reminder_reasons(
                &workspace,
                &sessions,
                state.get(&id).unwrap_or(&empty),
                None,
            )
            .await?
            .into_iter()
            .collect();
        let accepted: Vec<_> = observed
            .into_iter()
            .filter(|reason| current.contains(reason))
            .collect();
        if !id.is_chief()
            && !workspace.archived
            && self
                .store
                .acknowledge_workspace_reminders(&id, &principal_id, &accepted)
                .await?
        {
            self.emit_attention_reminder_invalidation(&id).await;
        }
        // Recompute after persistence; a new concurrent reason remains unacknowledged.
        intent_core::WorkspaceApi::get_workspace(self, id).await
    }
}
