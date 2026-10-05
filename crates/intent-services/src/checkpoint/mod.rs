//! Checkpoint v1 data and local codec helpers. No scheduler, transport, ownership
//! CAS or durable promotion lives here. The caller holds the runtime capture
//! barrier and resolves repository identities from its grants, never Git URLs.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use intent_core::{Error, Result};
use intent_git::checkpoint::{self as git, CaptureOptions, Snapshot};
use serde::{Deserialize, Serialize};

pub mod session;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Manifest {
    pub format_version: u32,
    pub checkpoint_id: String,
    pub workspace_id: String,
    pub agent_id: String,
    pub lease_id: String,
    pub incarnation: String,
    pub run_id: String,
    pub assignment_epoch: String,
    pub capture_revision: String,
    pub captured_at: String,
    pub journal_seq: String,
    pub repos: Vec<RepositoryEntry>,
    pub session: session::Session,
    pub attachments: Vec<Attachment>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RepositoryEntry {
    pub repo_key: String,
    pub path: String,
    pub fork_base: String,
    #[serde(flatten)]
    pub snapshot: Snapshot,
    pub submodules: Vec<Submodule>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inherited: Option<Inherited>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Submodule {
    /// Relative to the containing repository (not the workspace).
    pub path: String,
    pub repo_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Inherited {
    pub source_agent_id: String,
    pub checkpoint_id: String,
    pub execution_base: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Attachment {
    pub attachment_id: String,
    pub sha256: String,
}

fn invalid(message: impl Into<String>) -> Error {
    Error::Internal(format!("checkpoint: {}", message.into()))
}

fn full_oid(value: &str) -> bool {
    value.len() == 40
        && value
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}

fn counter(value: &str) -> Result<u64> {
    value
        .parse::<u64>()
        .ok()
        .filter(|n| n.to_string() == value)
        .ok_or_else(|| invalid("noncanonical u64 counter"))
}

impl Manifest {
    /// Structural validation only. The receiver must additionally prove grants,
    /// object closure, attachment hashes, transcript ack and assignment ownership.
    ///
    /// # Errors
    /// Rejects unsupported versions, unsafe identities/paths and inconsistent graphs or sessions.
    pub fn validate(&self) -> Result<()> {
        if self.format_version != 1 {
            return Err(invalid("unsupported format version"));
        }
        for value in [&self.checkpoint_id, &self.incarnation, &self.run_id] {
            uuid::Uuid::parse_str(value).map_err(|_| invalid("invalid UUID"))?;
        }
        if [&self.workspace_id, &self.agent_id, &self.lease_id]
            .iter()
            .any(|s| s.is_empty())
        {
            return Err(invalid("empty owner identity"));
        }
        counter(&self.assignment_epoch)?;
        counter(&self.capture_revision)?;
        counter(&self.journal_seq)?;
        time::OffsetDateTime::parse(
            &self.captured_at,
            &time::format_description::well_known::Rfc3339,
        )
        .map_err(|_| invalid("invalid capture time"))?;
        validate_repositories(&self.repos)?;
        self.session.validate(&self.journal_seq)?;
        let mut ids = HashSet::new();
        for attachment in &self.attachments {
            if attachment.attachment_id.is_empty()
                || !ids.insert(&attachment.attachment_id)
                || !session::is_sha256(&attachment.sha256)
            {
                return Err(invalid("invalid attachment"));
            }
        }
        Ok(())
    }
}

fn validate_repositories(repos: &[RepositoryEntry]) -> Result<()> {
    if repos.first().is_none_or(|r| r.path != ".") {
        return Err(invalid("root repository must be first"));
    }
    let mut paths = HashMap::new();
    let mut keys = HashSet::new();
    let mut edges = HashMap::new();
    for (position, repo) in repos.iter().enumerate() {
        if (repo.path != "." && !git::safe_relative_path(&repo.path))
            || paths.insert(repo.path.clone(), position).is_some()
            || repo.repo_key.is_empty()
            || !keys.insert(&repo.repo_key)
        {
            return Err(invalid("invalid or duplicate repository"));
        }
        if repo.snapshot.object_format != "sha1"
            || [&repo.snapshot.head, &repo.snapshot.index, &repo.fork_base]
                .iter()
                .any(|s| !full_oid(s))
            || repo.snapshot.wip.as_ref().is_some_and(|s| !full_oid(s))
        {
            return Err(invalid("invalid repository object identity"));
        }
        if repo
            .snapshot
            .branch
            .as_ref()
            .is_some_and(|b| !git2::Reference::is_valid_name(&format!("refs/heads/{b}")))
        {
            return Err(invalid("invalid branch"));
        }
        if let Some(inherited) = &repo.inherited {
            if inherited.source_agent_id.is_empty()
                || uuid::Uuid::parse_str(&inherited.checkpoint_id).is_err()
                || !full_oid(&inherited.execution_base)
            {
                return Err(invalid("invalid inherited baseline"));
            }
        }
        for edge in &repo.submodules {
            if !git::safe_relative_path(&edge.path) {
                return Err(invalid("invalid submodule path"));
            }
            let path = if repo.path == "." {
                edge.path.clone()
            } else {
                format!("{}/{}", repo.path, edge.path)
            };
            if edges.insert(path, (position, &edge.repo_key)).is_some() {
                return Err(invalid("duplicate submodule edge"));
            }
        }
    }
    for (position, repo) in repos.iter().enumerate().skip(1) {
        if !edges
            .get(&repo.path)
            .is_some_and(|(parent, key)| *parent < position && **key == repo.repo_key)
        {
            return Err(invalid("missing or unordered submodule edge"));
        }
    }
    if edges.len() != repos.len() - 1 || edges.keys().any(|p| !paths.contains_key(p)) {
        return Err(invalid("dangling submodule edge"));
    }
    Ok(())
}

/// Caller-owned identities and ancestry; keys must come from repository grants.
/// Source paths are workspace-relative and never serialized as absolute paths.
#[derive(Debug)]
pub struct RepositoryInput {
    pub repo_key: String,
    pub path: String,
    pub fork_base: String,
    pub inherited: Option<Inherited>,
    pub exclusions: CaptureOptions,
}

/// Capture all initialized tracked submodules, including dirty and unpublished
/// nested checkouts. Exact input coverage prevents silently losing child work.
///
/// # Errors
/// Rejects incomplete grants, unsafe submodules, unsupported state and source changes.
pub fn capture_repositories(
    root: &Path,
    inputs: &[RepositoryInput],
) -> Result<Vec<RepositoryEntry>> {
    let discovered = crate::transfer_submodules::checkpoint_submodules(root)?;
    let mut paths = vec![".".to_string()];
    paths.extend(discovered.iter().map(|s| s.path.clone()));
    if inputs.len() != paths.len() || inputs.iter().zip(&paths).any(|(i, p)| &i.path != p) {
        return Err(invalid(
            "repository grants must cover root and initialized submodules in order",
        ));
    }
    let index_before: Vec<_> = inputs
        .iter()
        .map(|i| index_bytes(&root.join(&i.path)))
        .collect::<Result<_>>()?;
    let mut entries = Vec::new();
    for input in inputs {
        let mut submodules = Vec::new();
        for child in discovered.iter().filter(|c| c.parent == input.path) {
            let key = inputs
                .iter()
                .find(|i| i.path == child.path)
                .ok_or_else(|| invalid("missing submodule grant"))?;
            submodules.push(Submodule {
                path: child.relative_path.clone(),
                repo_key: key.repo_key.clone(),
            });
        }
        entries.push(RepositoryEntry {
            repo_key: input.repo_key.clone(),
            path: input.path.clone(),
            fork_base: input.fork_base.clone(),
            snapshot: git::capture(&root.join(&input.path), &input.exclusions)?,
            submodules,
            inherited: input.inherited.clone(),
        });
    }
    validate_repositories(&entries)?;
    if crate::transfer_submodules::checkpoint_submodules(root)? != discovered {
        return Err(invalid("submodules changed during capture"));
    }
    for ((input, entry), before) in inputs.iter().zip(&entries).zip(index_before) {
        if index_bytes(&root.join(&input.path))? != before {
            return Err(invalid("index changed during capture"));
        }
        git::verify_source(&root.join(&input.path), &entry.snapshot, &input.exclusions)?;
    }
    Ok(entries)
}

fn index_bytes(path: &Path) -> Result<Option<Vec<u8>>> {
    let repo = git2::Repository::open(path).map_err(|e| invalid(e.to_string()))?;
    let index = repo.index().map_err(|e| invalid(e.to_string()))?;
    match std::fs::read(index.path().ok_or_else(|| invalid("missing index path"))?) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(invalid(e.to_string())),
    }
}

/// Restore into a new private directory. A failure removes the entire partial
/// result; callers publish only after provider/attachment validation also passes.
///
/// # Errors
/// Rejects invalid manifests, missing objects, existing targets and filesystem failures.
pub fn restore_repositories<S: std::hash::BuildHasher>(
    repos: &[RepositoryEntry],
    sources: &HashMap<String, PathBuf, S>,
    destination: &Path,
) -> Result<()> {
    validate_repositories(repos)?;
    match destination.symlink_metadata() {
        Ok(_) => return Err(invalid("restore destination already exists")),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(invalid(e.to_string())),
    }
    let mut owns_destination = false;
    let result = (|| {
        for repo in repos {
            let source = sources
                .get(&repo.repo_key)
                .ok_or_else(|| invalid("missing repository objects"))?;
            let target = if repo.path == "." {
                destination.to_path_buf()
            } else {
                destination.join(&repo.path)
            };
            if repo.path != "." {
                // Gitlink checkout creates empty directories. Only remove that
                // exact empty directory; never follow a symlink or delete files.
                if let Some(parent) = target.parent() {
                    let relative = parent
                        .strip_prefix(destination)
                        .map_err(|_| invalid("submodule escape"))?;
                    let mut check = destination.to_path_buf();
                    for part in relative.components() {
                        check.push(part);
                        if check
                            .symlink_metadata()
                            .is_ok_and(|m| m.file_type().is_symlink())
                        {
                            return Err(invalid("submodule symlink escape"));
                        }
                    }
                    std::fs::create_dir_all(parent).map_err(|e| invalid(e.to_string()))?;
                }
                if target
                    .symlink_metadata()
                    .is_ok_and(|m| m.file_type().is_symlink())
                {
                    return Err(invalid("submodule symlink escape"));
                }
                if target.exists() {
                    std::fs::remove_dir(&target).map_err(|e| invalid(e.to_string()))?;
                }
            }
            git::restore(source, &repo.snapshot, &target)?;
            owns_destination = true;
        }
        Ok(())
    })();
    if result.is_err() && owns_destination {
        let _ = std::fs::remove_dir_all(destination);
    }
    result
}

#[cfg(test)]
mod tests;
