//! Transaction-local conversion planning. The caller supplies the workspace
//! snapshot from its writer and persists this plan inside its conversion savepoint.
//! This module performs no writes or event publication.
use std::collections::HashMap;

use intent_core::{CreatedTaskEntry, Error, Note, NoteId, TaskStatus};

use crate::note_ops::{self, canonical::CanonicalPhase};

pub(crate) struct ConversionPlan {
    pub content: String,
    pub phases: Vec<CanonicalPhase>,
    pub children: Vec<Note>,
    pub created_tasks: Vec<CreatedTaskEntry>,
    /// Ordered metadata updates, including updates to children created above.
    pub relations: Vec<RelationUpdate>,
    pub warnings: Vec<String>,
}

pub(crate) struct RelationUpdate {
    pub note_id: NoteId,
    pub depends_on: Vec<NoteId>,
    pub conflicts_with: Vec<NoteId>,
    /// The transactional writer stamps `updated_at` when applying each update.
    /// Publish readiness only for a dependency change, after outer commit.
    pub depends_on_changed: bool,
    pub conflicts_with_changed: bool,
}

/// The validators need the existing Note metadata shape, but no source bytes.
/// Copy fields explicitly so even a new child's body is never cloned here.
fn metadata_only(note: &Note) -> Note {
    Note {
        id: note.id.clone(),
        workspace_id: note.workspace_id.clone(),
        title: note.title.clone(),
        content: String::new(),
        content_type: note.content_type,
        tags: note.tags.clone(),
        is_pinned: note.is_pinned,
        is_archived: note.is_archived,
        is_default: note.is_default,
        parent_id: note.parent_id.clone(),
        visibility: note.visibility,
        metadata: note.metadata.clone(),
        created_at: note.created_at.clone(),
        rev: note.rev,
        updated_at: note.updated_at.clone(),
    }
}

/// Preserve declaration-order relation validation and title reuse using only
/// the writer's snapshot. Each accepted update feeds subsequent cycle checks.
/// The writer must project empty content in SQL; unrelated note bodies are not
/// inputs. This still retains workspace-sized metadata, not a bounded read page.
pub(crate) fn plan_conversion(parent: &Note, source: &str, snapshot: &[Note]) -> ConversionPlan {
    let parsed = if note_ops::has_task_blocks(source) {
        note_ops::extract_task_blocks(source)
    } else {
        note_ops::TaskBlocksResult {
            tasks: Vec::new(),
            source_change: note_ops::canonical::CanonicalSourceChange::unchanged(source.into()),
        }
    };
    let mut plan = ConversionPlan {
        content: parsed.source_change.content,
        phases: parsed
            .source_change
            .phases
            .into_iter()
            .map(|edits| CanonicalPhase {
                reason: "task-conversion",
                edits,
            })
            .collect(),
        children: Vec::new(),
        created_tasks: Vec::new(),
        relations: Vec::new(),
        warnings: Vec::new(),
    };
    let mut existing: HashMap<String, NoteId> = snapshot
        .iter()
        .filter(|note| note.parent_id.as_ref() == Some(&parent.id))
        .map(|note| (note.title.trim().to_lowercase(), note.id.clone()))
        .collect();
    let mut block_ids = Vec::with_capacity(parsed.tasks.len());
    let mut peer_order = 100;
    for (index, task) in parsed.tasks.iter().enumerate() {
        let normalized = task.title.trim().to_lowercase();
        let id = if let Some(id) = existing.get(&normalized) {
            if task.effort.is_some() {
                plan.warnings.push(format!(
                    "task block {}: effort= ignored — a task note with this \
                     title already exists and its estimate is preserved",
                    crate::task_block_label(task)
                ));
            }
            id.clone()
        } else {
            let body = if task.content.is_empty() {
                format!("# {}\n\nCreated as a prerequisite task.", task.title)
            } else {
                format!("# {}\n\n{}", task.title, task.content)
            };
            let child = crate::build_child_task_note(
                &parent.workspace_id,
                &parent.id,
                &task.title,
                body,
                TaskStatus::NotStarted,
                Some(peer_order),
                task.effort.clone(),
            );
            existing.insert(normalized, child.id.clone());
            let id = child.id.clone();
            plan.created_tasks.push(CreatedTaskEntry {
                key: task.key.clone(),
                title: task.title.clone(),
                note_id: id.0.clone(),
            });
            plan.children.push(child);
            id
        };
        let placeholder = format!("<!-- task-block-placeholder-{index} -->");
        let link = format!("- [ ] [{}](intent://local/task/{})", task.title, id.0);
        let change = note_ops::canonical::replace_all(&plan.content, &placeholder, &link);
        plan.content = change.content;
        plan.phases
            .extend(change.phases.into_iter().map(|edits| CanonicalPhase {
                reason: "task-marker-projection",
                edits,
            }));
        block_ids.push(id);
        peer_order += 100;
    }
    for task in &parsed.tasks {
        for issue in &task.issues {
            plan.warnings.push(format!(
                "task block {}: {issue}",
                crate::task_block_label(task)
            ));
        }
    }
    let mut keys = HashMap::new();
    let mut titles = HashMap::new();
    for (task, id) in parsed.tasks.iter().zip(&block_ids) {
        if let Some(key) = &task.key {
            keys.entry(key.clone())
                .and_modify(|slot| *slot = None)
                .or_insert_with(|| Some(id.clone()));
        }
        titles
            .entry(task.title.trim().to_string())
            .and_modify(|slot| *slot = None)
            .or_insert_with(|| Some(id.clone()));
    }
    let mut all: Vec<Note> = snapshot.iter().map(metadata_only).collect();
    all.extend(plan.children.iter().map(metadata_only));
    for (task, id) in parsed.tasks.iter().zip(&block_ids) {
        let label = crate::task_block_label(task);
        let mut dependencies = Vec::new();
        let mut conflicts = Vec::new();
        for (field, values, accepted) in [
            ("dependsOn", &task.depends_on, &mut dependencies),
            ("conflictsWith", &task.conflicts_with, &mut conflicts),
        ] {
            for value in values {
                let Some(target) = crate::resolve_block_relation_value(
                    value,
                    &keys,
                    &titles,
                    &all,
                    field,
                    &label,
                    &mut plan.warnings,
                ) else {
                    continue;
                };
                let edge = std::slice::from_ref(&target);
                let checked = crate::validate_relation_ids(id, edge, &all, field).and_then(|()| {
                    if field == "dependsOn" {
                        if let Some(cycle) = crate::find_dependency_cycle(id, edge, &all) {
                            return Err(Error::Internal(format!(
                                "dependsOn would create a cycle: {}",
                                cycle.join(" -> ")
                            )));
                        }
                        crate::ensure_no_tree_relative_dependency(id, edge, &all)?;
                    }
                    Ok(())
                });
                match checked {
                    Ok(()) => accepted.push(target),
                    Err(error) => plan.warnings.push(format!(
                        "task block {label}: {field} reference \"{value}\" rejected: {}; edge skipped",
                        crate::relation_error_text(&error)
                    )),
                }
            }
        }
        if dependencies.is_empty() && conflicts.is_empty() {
            continue;
        }
        // Legacy title reuse can select a non-task child. Preserve its warning
        // outcome rather than silently promoting it into a task.
        let Some(note) = all.iter_mut().find(|note| &note.id == id) else {
            unreachable!("every block id comes from the snapshot or a new child");
        };
        let Some(metadata) = note.metadata.task.as_mut() else {
            plan.warnings.push(format!(
                "task block {label}: failed to seed relations: Note is not a task"
            ));
            continue;
        };
        let mut depends_on_changed = false;
        let mut conflicts_with_changed = false;
        if !dependencies.is_empty() {
            let dependencies = crate::normalize_relation_ids(dependencies);
            depends_on_changed = metadata.depends_on != dependencies;
            metadata.depends_on = dependencies;
        }
        if !conflicts.is_empty() {
            let conflicts = crate::normalize_relation_ids(conflicts);
            conflicts_with_changed = metadata.conflicts_with != conflicts;
            metadata.conflicts_with = conflicts;
        }
        if depends_on_changed || conflicts_with_changed {
            plan.relations.push(RelationUpdate {
                note_id: note.id.clone(),
                depends_on: metadata.depends_on.clone(),
                conflicts_with: metadata.conflicts_with.clone(),
                depends_on_changed,
                conflicts_with_changed,
            });
        }
    }
    plan
}

#[cfg(test)]
mod tests {
    use super::*;
    use intent_core::{note_mutation::NoteSourceHistory, WorkspaceId};

    fn parent(source: &str) -> Note {
        crate::build_child_task_note(
            &WorkspaceId::new(),
            &NoteId::new(),
            "Parent",
            source.into(),
            TaskStatus::NotStarted,
            None,
            None,
        )
    }

    #[test]
    fn conversion_plan_conserves_unicode_and_literal_placeholder_provenance() {
        let source = "same😀\r\n<!-- task-block-placeholder-0 -->\r\n@@@task key=a\r\n# A\r\nbody\r\n@@@\r\nsame😀";
        let parent = parent(source);
        let plan = plan_conversion(&parent, source, std::slice::from_ref(&parent));
        assert_eq!(plan.children.len(), 1);
        assert!(plan.content.starts_with("same😀\r\n- [ ] [A]"));
        assert!(plan.content.ends_with("\r\nsame😀"));
        let link = format!("intent://local/task/{}", plan.children[0].id.0);
        assert_eq!(plan.content.matches(&link).count(), 2);
        let mut history = NoteSourceHistory::new(source.into());
        for phase in &plan.phases {
            history.apply_phase(&phase.edits).unwrap();
        }
        assert_eq!(history.source(), plan.content);
        assert_eq!(plan.phases[0].reason, "task-conversion");
        assert_eq!(plan.phases[1].reason, "task-marker-projection");
    }

    #[test]
    fn conversion_plan_validates_cycles_against_prior_planned_relations() {
        let source = "@@@task key=a dependsOn=b\n# A\n@@@\n@@@task key=b dependsOn=a\n# B\n@@@";
        let parent = parent(source);
        let plan = plan_conversion(&parent, source, std::slice::from_ref(&parent));
        assert_eq!(plan.children.len(), 2);
        assert_eq!(plan.relations.len(), 1);
        let first = &plan.relations[0];
        assert_eq!(first.note_id, plan.children[0].id);
        assert_eq!(first.depends_on, vec![plan.children[1].id.clone()]);
        assert!(first.depends_on_changed);
        assert!(!first.conflicts_with_changed);
        assert_eq!(plan.created_tasks[0].key.as_deref(), Some("a"));
        assert_eq!(plan.created_tasks[1].key.as_deref(), Some("b"));
        assert_eq!(plan.warnings.len(), 1);
        assert!(plan.warnings[0].contains("would create a cycle"));
        assert!(plan.children.iter().all(|child| child
            .metadata
            .task
            .as_ref()
            .unwrap()
            .depends_on
            .is_empty()));
    }

    #[test]
    fn conversion_plan_reuses_title_without_clearing_existing_relations_or_effort() {
        let source = "@@@task key=duplicate effort=1h dependsOn=missing\n# A\n@@@\n@@@task key=duplicate\n# A\n@@@\n@@@task dependsOn=duplicate\n# B\n@@@";
        let parent = parent(source);
        let mut existing = crate::build_child_task_note(
            &parent.workspace_id,
            &parent.id,
            "A",
            "Existing content".into(),
            TaskStatus::NotStarted,
            None,
            Some("2d".into()),
        );
        let old_dependency = NoteId::new();
        existing.metadata.task.as_mut().unwrap().depends_on = vec![old_dependency];
        let plan = plan_conversion(&parent, source, &[parent.clone(), existing.clone()]);
        assert_eq!(plan.children.len(), 1);
        assert_eq!(plan.children[0].title, "B");
        assert!(plan.relations.is_empty());
        assert_eq!(plan.warnings.len(), 3);
        assert!(plan
            .warnings
            .iter()
            .any(|warning| warning.contains("estimate is preserved")));
        assert!(plan
            .warnings
            .iter()
            .any(|warning| warning.contains("is ambiguous")));
        assert_eq!(
            plan.content
                .matches(&format!("intent://local/task/{}", existing.id.0))
                .count(),
            2
        );
    }

    #[test]
    fn conversion_plan_keeps_each_reused_child_update_and_original_creation_key() {
        let source = "@@@task key=original dependsOn=c\n# A\n@@@\n@@@task key=reused conflictsWith=b\n# A\n@@@\n@@@task dependsOn=b\n# A\n@@@\n@@@task key=b\n# B\n@@@\n@@@task key=c\n# C\n@@@";
        let parent = parent(source);
        let plan = plan_conversion(&parent, source, std::slice::from_ref(&parent));
        assert_eq!(plan.children.len(), 3);
        assert_eq!(plan.created_tasks.len(), 3);
        assert_eq!(plan.created_tasks[0].key.as_deref(), Some("original"));
        assert_eq!(plan.relations.len(), 3);
        assert!(plan
            .relations
            .iter()
            .all(|update| update.note_id == plan.children[0].id));
        let first = &plan.relations[0];
        let second = &plan.relations[1];
        let third = &plan.relations[2];
        assert!(first.depends_on_changed);
        assert!(!first.conflicts_with_changed);
        assert!(!second.depends_on_changed);
        assert!(second.conflicts_with_changed);
        assert_eq!(first.depends_on, second.depends_on);
        assert!(third.depends_on_changed);
        assert!(!third.conflicts_with_changed);
        assert_eq!(second.conflicts_with, third.conflicts_with);
        assert_eq!(third.depends_on, vec![plan.children[1].id.clone()]);
        assert!(metadata_only(&plan.children[0]).content.is_empty());
        assert!(!plan.children[0].content.is_empty());
    }
}
