//! Publish narrow committed projections once, independent of client count.
use super::EventBus;
use intent_core::{
    events::{NOTE_DELETED, TASK_UPDATED, WORKSPACE_TASK_STATS_CHANGED},
    Error, Event, NoteId, Result,
};
use intent_store::NewEvent;
use std::collections::HashMap;

impl EventBus {
    pub(super) async fn publish_note_and_task_changes(&self, source: &NewEvent) -> Result<Event> {
        // Hold through reads AND publications. Concurrent mutation notifications
        // can coalesce into current state, but an older read cannot publish last.
        let mut baseline = self.task_event_baselines.lock(&source.workspace_id).await;
        let mut event = source.clone();
        let mut is_task = false;
        if source.event_type != NOTE_DELETED {
            if let Some(id) = source
                .data
                .get("noteId")
                .and_then(serde_json::Value::as_str)
            {
                match self
                    .store
                    .get_note_metadata(&source.workspace_id, &NoteId::from(id))
                    .await
                {
                    Ok(note) => {
                        is_task = note.metadata.task.is_some();
                        let mut metadata = serde_json::to_value(&note)
                            .map_err(|e| Error::Internal(format!("encode note metadata: {e}")))?;
                        metadata
                            .as_object_mut()
                            .expect("note object")
                            .remove("content");
                        // Explicit empty metadata clears a former task marker.
                        metadata["metadata"] = serde_json::to_value(&note.metadata)
                            .map_err(|e| Error::Internal(format!("encode task metadata: {e}")))?;
                        event.data["note"] = metadata;
                    }
                    Err(Error::NotFound(_)) => {}
                    Err(error) => tracing::warn!(%error, "note event metadata unavailable"),
                }
            }
        }
        let id = source
            .data
            .get("noteId")
            .and_then(serde_json::Value::as_str);
        let previous = baseline.as_ref();
        let affects_tasks = is_task
            || id == Some("spec")
            || previous.is_some_and(|p| {
                p.tasks.iter().any(|task| {
                    Some(task.id.as_str()) == id
                        // Deleting an ordinary parent detaches task children
                        // through the store trigger, without child note events.
                        || (source.event_type == NOTE_DELETED
                            && task
                                .parent_id
                                .as_ref()
                                .is_some_and(|parent| Some(parent.as_str()) == id))
                })
            });
        let current = if affects_tasks {
            match self
                .store
                .list_workspace_tasks(&source.workspace_id, None)
                .await
            {
                Ok(snapshot) => Some(snapshot),
                Err(error) => {
                    tracing::warn!(%error, "task event projection unavailable");
                    None
                }
            }
        } else {
            None
        };
        if let Some(current) = &current {
            if let Some(task) = current.tasks.iter().find(|t| Some(t.id.as_str()) == id) {
                if let Some(note) = event.data.get_mut("note") {
                    note["metadata"]["task"]["unmetDependsOn"] =
                        serde_json::json!(task.unmet_depends_on);
                }
            }
        }
        let published = self.publish_raw(&event).await?;
        let Some(current) = current else {
            return Ok(published);
        };
        let old_tasks: HashMap<_, _> = previous
            .into_iter()
            .flat_map(|p| &p.tasks)
            .map(|t| (&t.id, t))
            .collect();
        let current_ids: std::collections::HashSet<_> =
            current.tasks.iter().map(|t| &t.id).collect();
        let tasks: Vec<_> = current
            .tasks
            .iter()
            .filter(|t| old_tasks.get(&t.id).copied() != Some(*t))
            .collect();
        let removed: Vec<_> = previous
            .into_iter()
            .flat_map(|p| &p.tasks)
            .filter(|t| !current_ids.contains(&t.id))
            .map(|t| &t.id)
            .collect();
        if !tasks.is_empty() || !removed.is_empty() {
            self.publish_raw(&projection_event(
                source,
                TASK_UPDATED,
                serde_json::json!({
                    "workspaceId": source.workspace_id,
                    "tasks": tasks,
                    "removedIds": removed,
                }),
            ))
            .await?;
        }
        if previous.map(|p| &p.stats) != Some(&current.stats) {
            self.publish_raw(&projection_event(
                source,
                WORKSPACE_TASK_STATS_CHANGED,
                serde_json::json!({
                    "workspaceId": source.workspace_id,
                    "taskStats": current.stats,
                }),
            ))
            .await?;
        }
        *baseline = Some(current);
        Ok(published)
    }
}

fn projection_event(source: &NewEvent, event_type: &str, data: serde_json::Value) -> NewEvent {
    NewEvent {
        event_type: event_type.into(),
        timestamp: intent_core::now_iso(),
        data,
        ..source.clone()
    }
}
