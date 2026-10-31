//! `ws.crossWorkspace.search`: find sibling workspaces by free text and/or a
//! pull-request reference.
//!
//! Scope is exactly the `crossWorkspace.*` sibling set (same repository,
//! membership-narrowed, self excluded), so every hit is a workspace the
//! agent can already open with `crossWorkspace.listNotes` / `readNote`.
//! Three indexes feed it:
//!
//! - the PR <-> workspace link index (migration 0161), for `pr`;
//! - workspace metadata (title, branch, status message, label, tags, PR
//!   titles), for `query`;
//! - the note FTS index restricted to the siblings, for `query`.

use std::collections::{HashMap, HashSet};

use intent_core::{
    pull_request_key, CrossWorkspaceSearch, Error, PullRequestInfo, Result, Workspace, WorkspaceId,
};
use intent_store::{NoteFtsMatch, NoteFtsOptions, PullRequestLink};
use serde_json::{json, Value};

use crate::{has_repository_identity, workspaces_share_repository, Services};

/// Note hits kept per workspace in a result row. Capped in SQL per
/// workspace, so one sibling's many strong hits never crowd out another.
const NOTES_PER_WORKSPACE: usize = 3;
/// `NOTES_PER_WORKSPACE` as the store's SQL limit type; keep them equal.
const NOTES_PER_WORKSPACE_SQL: i64 = 3;

impl Services {
    /// The caller's sibling workspaces (`crossWorkspace.listSiblings` scope):
    /// same repository, membership-narrowed, self excluded, archived
    /// included, retired only with `include_retired`.
    pub(crate) async fn cross_workspace_siblings(
        &self,
        workspace_id: &WorkspaceId,
        include_retired: bool,
    ) -> Result<Vec<Workspace>> {
        self.require_member(workspace_id).await?;
        let current = match self.store.get_workspace(workspace_id).await {
            Ok(w) => w,
            Err(Error::NotFound(_)) => {
                return Err(Error::Internal("Current workspace not found".to_string()));
            }
            Err(e) => return Err(e),
        };
        if !has_repository_identity(&current) {
            return Err(Error::Internal(
                "Current workspace is not associated with a repository".to_string(),
            ));
        }
        let mut all = self
            .store
            .list_workspaces(intent_core::WorkspaceListFilter::new(true, include_retired))
            .await?;
        self.retain_member_workspaces(&mut all).await?;
        all.retain(|w| w.id != *workspace_id && workspaces_share_repository(&current, w));
        Ok(all)
    }

    pub(crate) async fn cross_workspace_search_op(
        &self,
        workspace_id: WorkspaceId,
        search: CrossWorkspaceSearch,
    ) -> Result<Value> {
        let query = search
            .query
            .as_deref()
            .map(str::trim)
            .filter(|q| !q.is_empty());
        if query.is_none() && search.pr.is_none() {
            return Err(Error::InvalidParams(
                "Provide a non-empty query, a pr, or both".to_string(),
            ));
        }
        let siblings = self
            .cross_workspace_siblings(&workspace_id, search.include_retired)
            .await?;
        if siblings.is_empty() {
            return Ok(json!({ "matches": [], "truncated": false }));
        }
        let ids: Vec<WorkspaceId> = siblings.iter().map(|w| w.id.clone()).collect();
        let links = self
            .store
            .list_pull_request_links_for_workspaces(&ids)
            .await?;
        // With a PR selector, only PR-matching siblings can survive ranking,
        // so note search is restricted to them.
        let note_scope: Vec<WorkspaceId> = match &search.pr {
            Some(selector) => {
                let matched: HashSet<&str> = links
                    .iter()
                    .filter(|l| selector.matches(&l.pr_key))
                    .map(|l| l.workspace_id.as_str())
                    .collect();
                ids.iter()
                    .filter(|id| matched.contains(id.as_str()))
                    .cloned()
                    .collect()
            }
            None => ids.clone(),
        };
        let note_hits = match query.and_then(intent_search::fts_match_expr) {
            Some(expr) if !note_scope.is_empty() => {
                // The per-workspace cap bounds the result to
                // `note_scope.len() * NOTES_PER_WORKSPACE` rows, so no global
                // limit is needed and every eligible sibling is represented.
                let options = NoteFtsOptions {
                    allowed_workspace_ids: Some(&note_scope),
                    include_archived: true,
                    per_workspace_limit: Some(NOTES_PER_WORKSPACE_SQL),
                    ..NoteFtsOptions::default()
                };
                self.store.search_notes_fts(&expr, &options).await?
            }
            _ => Vec::new(),
        };
        Ok(rank_matches(
            siblings,
            &CrossWorkspaceSearch {
                query: query.map(str::to_string),
                ..search
            },
            &links,
            note_hits,
        ))
    }
}

/// Score, filter and shape sibling rows (pure, so the ranking rules are
/// unit-tested without a store). Order: PR hits, then metadata matches
/// (title phrase above other fields), then note-only hits by FTS rank; ties
/// by most recently updated, then id.
pub(crate) fn rank_matches(
    siblings: Vec<Workspace>,
    search: &CrossWorkspaceSearch,
    links: &[PullRequestLink],
    note_hits: Vec<NoteFtsMatch>,
) -> Value {
    let query = search.query.as_deref().unwrap_or_default();
    let terms: Vec<String> = query.split_whitespace().map(str::to_lowercase).collect();
    let phrase = query.to_lowercase();

    let mut links_by_ws: HashMap<&str, Vec<&PullRequestLink>> = HashMap::new();
    for link in links {
        links_by_ws
            .entry(link.workspace_id.as_str())
            .or_default()
            .push(link);
    }
    let mut notes_by_ws: HashMap<String, Vec<NoteFtsMatch>> = HashMap::new();
    for hit in note_hits {
        notes_by_ws
            .entry(hit.workspace_id.clone())
            .or_default()
            .push(hit);
    }

    let mut scored: Vec<(f64, Workspace, Value)> = Vec::new();
    for w in siblings {
        let ws_links = links_by_ws
            .get(w.id.as_str())
            .map_or(&[][..], Vec::as_slice);
        let pr_infos = pull_request_infos(&w);
        let mut matched_on: Vec<&str> = Vec::new();
        let mut score = 0.0;

        let pr_rows: Vec<Value> = match &search.pr {
            Some(selector) => ws_links
                .iter()
                .filter(|l| selector.matches(&l.pr_key))
                .map(|l| pr_row(l, pr_infos.get(l.pr_key.as_str()).copied()))
                .collect(),
            None => Vec::new(),
        };
        if search.pr.is_some() {
            if pr_rows.is_empty() {
                continue;
            }
            matched_on.push("pullRequest");
            score += 1000.0;
        }

        let mut notes = Vec::new();
        if !terms.is_empty() {
            let pr_text: Vec<String> = pr_infos
                .values()
                .map(|pr| pr.title.to_lowercase())
                .chain(ws_links.iter().map(|l| l.pr_key.clone()))
                .collect();
            let fields: [(&str, String); 5] = [
                ("title", w.title.to_lowercase()),
                ("branch", w.branch.to_lowercase()),
                (
                    "statusMessage",
                    w.status_message
                        .as_deref()
                        .unwrap_or_default()
                        .to_lowercase(),
                ),
                (
                    "tags",
                    w.label
                        .iter()
                        .chain(w.tags.iter())
                        .map(|t| t.to_lowercase())
                        .collect::<Vec<_>>()
                        .join(" "),
                ),
                ("pullRequestTitle", pr_text.join(" ")),
            ];
            let meta_match = terms
                .iter()
                .all(|t| fields.iter().any(|(_, text)| text.contains(t.as_str())));
            if meta_match {
                for (name, text) in &fields {
                    if terms.iter().any(|t| text.contains(t.as_str())) {
                        matched_on.push(*name);
                        score += 100.0;
                    }
                }
                if fields[0].1.contains(phrase.as_str()) {
                    score += 300.0;
                }
            }
            if let Some(hits) = notes_by_ws.remove(w.id.as_str()) {
                if let Some(best) = hits.first() {
                    score += (-best.rank).clamp(0.0, 99.0);
                }
                notes = hits
                    .into_iter()
                    .take(NOTES_PER_WORKSPACE)
                    .map(|hit| {
                        let text = format!("{} {} {}", hit.title, hit.content, hit.tags.join(" "));
                        json!({
                            "noteId": hit.note_id,
                            "title": hit.title,
                            "preview": intent_search::fts_preview(&text, query),
                        })
                    })
                    .collect();
                matched_on.push("notes");
                score += 1.0;
            }
            if !meta_match && notes.is_empty() {
                continue;
            }
        }

        let mut row = json!({
            "id": w.id,
            "title": if w.title.is_empty() { "Untitled" } else { w.title.as_str() },
            "branch": w.branch,
            "status": w.status,
            "archived": w.archived,
            "updatedAt": w.updated_at,
            "matchedOn": matched_on,
        });
        if !pr_rows.is_empty() {
            row["pullRequests"] = Value::Array(pr_rows);
        }
        if !notes.is_empty() {
            row["notes"] = Value::Array(notes);
        }
        scored.push((score, w, row));
    }

    scored.sort_by(|(sa, wa, _), (sb, wb, _)| {
        sb.total_cmp(sa)
            .then_with(|| wb.updated_at.cmp(&wa.updated_at))
            .then_with(|| wa.id.as_str().cmp(wb.id.as_str()))
    });
    let truncated = scored.len() > search.limit;
    let matches: Vec<Value> = scored
        .into_iter()
        .take(search.limit)
        .map(|(_, _, row)| row)
        .collect();
    json!({ "matches": matches, "truncated": truncated })
}

/// A workspace's persisted PR snapshots keyed by canonical PR key, so a link
/// hit can carry the PR's title and status when the workspace still has it.
fn pull_request_infos(w: &Workspace) -> HashMap<String, &PullRequestInfo> {
    w.pull_requests
        .iter()
        .flatten()
        .chain(w.active_pull_request.iter())
        .filter_map(|pr| pull_request_key(&pr.url).map(|key| (key, pr)))
        .collect()
}

fn pr_row(link: &PullRequestLink, info: Option<&PullRequestInfo>) -> Value {
    let number = link
        .pr_key
        .rsplit('/')
        .next()
        .and_then(|n| n.parse::<u64>().ok());
    let mut row = json!({
        "key": link.pr_key,
        "url": info.map_or_else(|| format!("https://{}", link.pr_key), |pr| pr.url.clone()),
        "linkedAt": link.linked_at,
    });
    if let Some(number) = number {
        row["number"] = json!(number);
    }
    if let Some(pr) = info {
        row["title"] = json!(pr.title);
        row["status"] = json!(pr.status);
    }
    row
}
