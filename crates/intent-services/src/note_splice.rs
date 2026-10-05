//! Public exact-CAS note writes. Source, canonical phases, children, versions and
//! receipt share one writer; publication happens only after its consuming commit.
use crate::{note_conversion_plan, note_ops, Services};
use intent_core::{
    note_mutation::NoteApplySplices, AgentId, Caller, Comment, Error, Note, NoteVersionAuthor,
    Result, TaskStatus, WorkspaceId,
};
use intent_store::{
    note_annotation_repo::{AnchorOccurrence, SourceRange},
    NoteMutationAdmission, NoteMutationWrite,
};
use serde_json::Value;

struct ConversionPublication {
    children: Vec<Note>,
    relations: Vec<(Note, Option<Vec<String>>)>,
}

async fn reanchor(write: &mut NoteMutationWrite, author: &NoteVersionAuthor) -> Result<()> {
    let comments = write.comments().await?;
    let plan = note_ops::canonical::plan_anchor_changes(write.source(), &comments);
    for phase in &plan.phases {
        write
            .apply_recorded_phase(phase.reason, &phase.edits)
            .await?;
    }
    if write.source() != plan.content {
        return Err(Error::Internal(
            "Canonical source composition mismatch".into(),
        ));
    }
    write
        .mark_comments_orphaned(&plan.orphaned, &intent_core::now_iso())
        .await?;
    write.persist_source(author, &intent_core::now_iso()).await
}

async fn convert(
    write: &mut NoteMutationWrite,
    author: &NoteVersionAuthor,
) -> Result<ConversionPublication> {
    let mut metadata = write.workspace_note_metadata().await?;
    let plan = note_conversion_plan::plan_conversion(write.note(), write.source(), &metadata);
    for child in &plan.children {
        write.insert_conversion_child(child, author).await?;
    }
    // Fetch metadata after insertion, never clone the children's source bodies.
    if !plan.children.is_empty() {
        metadata = write.workspace_note_metadata().await?;
    }
    let mut relations = Vec::new();
    for update in &plan.relations {
        if let Some(note) = write
            .persist_conversion_relations(
                &update.note_id,
                &update.depends_on,
                &update.conflicts_with,
                &intent_core::now_iso(),
            )
            .await?
        {
            if let Some(current) = metadata.iter_mut().find(|n| n.id == note.id) {
                *current = note.clone();
            }
            let ready = update
                .depends_on_changed
                .then(|| crate::compute_ready_task_ids(&metadata));
            debug_assert!(update.depends_on_changed || update.conflicts_with_changed);
            relations.push((note, ready));
        }
    }
    for phase in &plan.phases {
        write
            .apply_recorded_phase(phase.reason, &phase.edits)
            .await?;
    }
    if write.source() != plan.content {
        return Err(Error::Internal(
            "Conversion source composition mismatch".into(),
        ));
    }
    for warning in &plan.warnings {
        write.record_warning("task-conversion", warning).await?;
    }
    // Creation descriptors retain authored block keys independently of title reuse.
    debug_assert_eq!(plan.created_tasks.len(), plan.children.len());
    if !plan.phases.is_empty() || !plan.children.is_empty() {
        reanchor(write, author).await?;
    }
    Ok(ConversionPublication {
        children: plan.children,
        relations,
    })
}

/// Resolve every surviving canonical marker occurrence from the final source.
/// Source-sized scanning is part of write preparation, never a paged read.
fn anchor_occurrences(source: &str, comments: &[Comment]) -> Result<Vec<AnchorOccurrence>> {
    let mut occurrences = Vec::new();
    for comment in comments
        .iter()
        .filter(|c| c.parent_id.is_none() && c.is_orphaned != Some(true))
    {
        let open = format!("<!--anchor:{}:start-->", comment.id);
        let close = format!("<!--anchor:{}:end-->", comment.id);
        for (at, _) in source.match_indices(&open) {
            let start = at + open.len();
            if let Some(relative) = source[start..].find(&close) {
                let end = start + relative;
                occurrences.push(AnchorOccurrence {
                    comment_id: comment.id.clone(),
                    occurrence_id: format!("range:{at}"),
                    source_range: SourceRange {
                        start: i64::try_from(source[..start].encode_utf16().count())
                            .map_err(|_| Error::Internal("Source offset overflow".into()))?,
                        end: i64::try_from(source[..end].encode_utf16().count())
                            .map_err(|_| Error::Internal("Source offset overflow".into()))?,
                    },
                });
            }
        }
        let point = format!("<!--anchor:{}:point-->", comment.id);
        for (at, _) in source.match_indices(&point) {
            let position = i64::try_from(source[..at].encode_utf16().count())
                .map_err(|_| Error::Internal("Source offset overflow".into()))?;
            occurrences.push(AnchorOccurrence {
                comment_id: comment.id.clone(),
                occurrence_id: format!("point:{at}"),
                source_range: SourceRange {
                    start: position,
                    end: position,
                },
            });
        }
    }
    Ok(occurrences)
}

impl Services {
    pub(crate) async fn apply_note_splices(&self, request: NoteApplySplices) -> Result<Value> {
        request.validate().map_err(Error::NoteMutation)?;
        let workspace = WorkspaceId(request.workspace_id.clone());
        let _mutation = self.workspace_mutations.enter(&workspace)?;
        self.require_member(&workspace).await?;
        self.store.get_workspace(&workspace).await?;
        let (principal, agent) = match intent_core::current_caller() {
            Some(Caller::Wire { principal_id, .. }) => {
                (format!("principal:{}", principal_id.0), None)
            }
            Some(Caller::Agent { agent_id }) => (format!("agent:{}", agent_id.0), Some(agent_id)),
            Some(Caller::Daemon) => ("daemon".into(), None),
            None => return Err(Error::Forbidden("Caller required".into())),
        };
        let author = crate::resolve_note_version_author(&self.store, agent.as_ref()).await;
        // Preserve the inherited per-logical-replacement write guard. Defer
        // rejection until replay lookup, so historical receipts remain exact.
        let replacements_valid = request
            .splices
            .iter()
            .try_for_each(|splice| note_ops::reject_numbered_read_presentation(&splice.text));
        let admission = self
            .store
            .begin_note_mutation(&principal, request, &intent_core::now_iso())
            .await?;
        self.require_member(&workspace).await?;
        let write = match admission {
            NoteMutationAdmission::Replay(receipt) => return Ok(receipt),
            NoteMutationAdmission::Write(write) => write,
        };
        replacements_valid?;
        self.finish_note_mutation(write, workspace, agent, author)
            .await
    }

    pub(crate) async fn finish_note_mutation(
        &self,
        mut write: Box<NoteMutationWrite>,
        workspace: WorkspaceId,
        agent: Option<AgentId>,
        author: NoteVersionAuthor,
    ) -> Result<Value> {
        write.retain_source_state("callerResult").await?;
        reanchor(&mut write, &author).await?;
        write.retain_source_state("preConversionCanonical").await?;
        let publication = if note_ops::has_task_blocks(write.source()) {
            write.begin_conversion().await?;
            match convert(&mut write, &author).await {
                Ok(publication) => {
                    // Failure to release must drop the entire outer transaction.
                    write.finish_conversion().await?;
                    Some(publication)
                }
                Err(error) => {
                    // A phase Err alone never poisons SQLite. Fallback is legal
                    // ONLY after both ROLLBACK TO and RELEASE have succeeded.
                    write.rollback_conversion().await?;
                    tracing::warn!(%error, "note splice conversion rolled back");
                    write.record_warning("task-conversion-failed", "Task conversion could not be completed; the canonical caller edit was retained.").await?;
                    None
                }
            }
        } else {
            None
        };
        let comments = write.comments().await?;
        let anchors = anchor_occurrences(write.source(), &comments)?;
        write.publish_annotation_anchors(&anchors).await?;
        // Retain only event metadata across commit, not another full source copy.
        let note_id = write.note().id.clone();
        let title = write.note().title.clone();
        let receipt = write.commit().await?;
        if let Some(publication) = publication {
            for child in &publication.children {
                self.emit_child_task_created(child, TaskStatus::NotStarted, agent.as_ref())
                    .await;
            }
            for (note, ready) in publication.relations {
                crate::publish_event(
                    self.event_bus.as_ref(),
                    crate::note_change_event(
                        &workspace,
                        &note.id,
                        &note.title,
                        intent_core::events::NOTE_UPDATED,
                        "update",
                    ),
                )
                .await;
                if let Some(ready) = ready {
                    crate::publish_event(
                        self.event_bus.as_ref(),
                        crate::ready_tasks_changed_reason_event(
                            &workspace,
                            &ready,
                            &note.id,
                            "relations-changed",
                            &intent_core::now_iso(),
                        ),
                    )
                    .await;
                }
            }
            if !publication.children.is_empty() {
                self.maybe_emit_display_status_changed(&workspace).await;
            }
        }
        self.schedule_line_attribution_recompute(&workspace, &note_id);
        crate::publish_event(
            self.event_bus.as_ref(),
            crate::note_change_event(
                &workspace,
                &note_id,
                &title,
                intent_core::events::NOTE_UPDATED,
                "update",
            ),
        )
        .await;
        self.maybe_emit_display_status_for_spec_write(&workspace, &note_id)
            .await;
        Ok(receipt)
    }
}

#[cfg(test)]
mod tests;
