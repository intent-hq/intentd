//! Head-owned bare repositories and scoped ref admission. No transport or forge push.
//!
//! Callers supply authenticated and current persisted assignments independently;
//! a request body is never authority. Hold the service's assignment/checkpoint lock
//! from admission through the durable checkpoint decision and alias repair. These
//! primitives do not replace lease admission, quarantine, object-closure grants,
//! capture-revision ordering, or the successful-checkpoint database transaction.

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use git2::{ErrorCode, Oid, Reference, Repository};
use intent_core::{Error, GitRemoteUrl, RepoRef, Result};
use sha2::{Digest, Sha256};

use crate::{map_git_err, repo_cache};

/// Canonical forge identity, independent of workspace and agent placement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HubIdentity {
    provider: String,
    host: String,
    owner: String,
    repository: String,
}

impl HubIdentity {
    /// Construct an identity from trusted forge metadata (not an agent URL).
    ///
    /// # Errors
    /// Rejects unsafe or empty identity components.
    pub fn new(provider: &str, host: &str, owner: &str, repository: &str) -> Result<Self> {
        for value in [provider, host, owner, repository] {
            component(value)?;
        }
        let (owner, repository) = RepoRef::new(owner, repository).identity_parts();
        let provider = provider.to_ascii_lowercase();
        let host = host.to_ascii_lowercase();
        Ok(Self {
            provider,
            host,
            owner,
            repository,
        })
    }

    /// An opaque stable disk/ref key; length-delimited fields prevent ambiguity.
    #[must_use]
    pub fn key(&self) -> String {
        let mut hash = Sha256::new();
        for part in [&self.provider, &self.host, &self.owner, &self.repository] {
            hash.update((part.len() as u64).to_be_bytes());
            hash.update(part.as_bytes());
        }
        let mut key = String::with_capacity(64);
        for byte in hash.finalize() {
            write!(key, "{byte:02x}").expect("writing to String cannot fail");
        }
        key
    }

    fn matches_origin(&self, url: &str) -> bool {
        GitRemoteUrl::parse(url).is_some_and(|remote| {
            let host_matches = remote.host().eq_ignore_ascii_case(&self.host);
            host_matches
                && remote.repo_slug() == Some(RepoRef::new(&self.owner, &self.repository))
                && remote.path().trim_start_matches('/').split('/').count() == 2
        })
    }
}

/// Scope resolved from authenticated link identity and persisted assignment.
/// All fields participate in comparison, including incarnation and reconnect fencing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HubAssignment {
    pub workspace_id: String,
    pub agent_id: String,
    pub repo_key: String,
    pub lease_id: String,
    pub incarnation_id: String,
    pub connection_generation: u64,
    pub assignment_epoch: u64,
}

/// Agent-owned mutable aliases. Index and checkpoint anchors are head-only.
#[derive(Clone, Copy, Debug)]
pub enum AgentRef {
    Head,
    Wip,
}

/// Immutable checkpoint anchor kinds.
#[derive(Clone, Copy, Debug)]
pub enum CheckpointRef {
    Head,
    Wip,
    Index,
}

/// An exact compare-and-swap; `None` means absent (old) or delete (new).
#[derive(Clone, Debug)]
pub struct RefUpdate {
    pub name: String,
    pub expected: Option<Oid>,
    pub new: Option<Oid>,
}

/// Ref name constructors validate IDs as single components, never path prefixes.
///
/// # Errors
/// Rejects invalid workspace/agent IDs.
pub fn agent_ref(workspace: &str, agent: &str, kind: AgentRef) -> Result<String> {
    component(workspace)?;
    component(agent)?;
    let suffix = match kind {
        AgentRef::Head => "head",
        AgentRef::Wip => "wip",
    };
    Ok(format!(
        "refs/intent/ws/{workspace}/agents/{agent}/{suffix}"
    ))
}

/// # Errors
/// Rejects invalid checkpoint or repository keys.
pub fn checkpoint_ref(checkpoint: &str, repo_key: &str, kind: CheckpointRef) -> Result<String> {
    component(checkpoint)?;
    component(repo_key)?;
    let suffix = match kind {
        CheckpointRef::Head => "head",
        CheckpointRef::Wip => "wip",
        CheckpointRef::Index => "index",
    };
    Ok(format!(
        "refs/intent/checkpoints/{checkpoint}/{repo_key}/{suffix}"
    ))
}

/// # Errors
/// Rejects invalid branch names, including refspecs and traversal.
pub fn publish_ref(branch: &str) -> Result<String> {
    let name = format!("refs/intent/publish/{branch}");
    if branch.is_empty() || branch.starts_with('-') || !Reference::is_valid_name(&name) {
        return Err(Error::InvalidParams("invalid hub publish branch".into()));
    }
    Ok(name)
}

fn component(value: &str) -> Result<()> {
    if value.is_empty()
        || value.starts_with(['.', '-'])
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
        || !Reference::is_valid_name(&format!("refs/intent/{value}"))
    {
        return Err(Error::InvalidParams(
            "invalid hub identity/ref component".into(),
        ));
    }
    Ok(())
}

/// A hub is self-contained before it becomes visible to callers. It has no
/// configured forge remote, so checkpoint ref operations cannot push to a forge.
#[derive(Clone, Debug)]
pub struct Hub {
    path: PathBuf,
    identity: HubIdentity,
}

impl Hub {
    /// Idempotently create one bare hub per identity using the existing cache
    /// path helpers and lock. Cache alternates are temporary: copy their reachable
    /// objects before atomically publishing the directory. Cache GC/eviction can
    /// then proceed freely, even across daemon restarts.
    ///
    /// # Errors
    /// Rejects wrong-origin cache slots, incompatible existing hubs and Git/I/O failures.
    pub async fn ensure(cache_root: &Path, hub_root: &Path, identity: HubIdentity) -> Result<Self> {
        let cache = repo_cache::cache_path_for(cache_root, &identity.owner, &identity.repository);
        let hub = Self {
            path: hub_root.join(format!("{}.git", identity.key())),
            identity,
        };
        let result = hub.clone();
        let locked_cache = cache.clone();
        repo_cache::with_cache_lock_blocking(&cache, move || {
            if hub.path.exists() {
                hub.open()?;
                return Ok(());
            }
            hub.check_cache(&locked_cache)?;
            let staging = hub.path.with_extension("initializing");
            if staging.exists() {
                std::fs::remove_dir_all(&staging).map_err(io_error)?;
            }
            std::fs::create_dir_all(&staging).map_err(io_error)?;
            let repo = Repository::init_bare(&staging).map_err(map_git_err)?;
            repo.config()
                .map_err(map_git_err)?
                .set_str("intent.hubIdentity", &hub.identity.key())
                .map_err(map_git_err)?;
            repo_cache::seed_detached_bare(&locked_cache, &staging)?;
            std::fs::rename(&staging, &hub.path).map_err(io_error)
        })
        .await?;
        Ok(result)
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Refresh only head-owned base refs from the already-refreshed local cache.
    /// Does not refresh the forge itself or touch agent/checkpoint/publication refs.
    ///
    /// # Errors
    /// Rejects a changed cache origin and propagates Git/I/O failures.
    pub async fn sync_base(&self, cache_root: &Path) -> Result<()> {
        let cache =
            repo_cache::cache_path_for(cache_root, &self.identity.owner, &self.identity.repository);
        let hub = self.clone();
        let locked_cache = cache.clone();
        repo_cache::with_cache_lock_blocking(&cache, move || {
            hub.open()?;
            hub.check_cache(&locked_cache)?;
            repo_cache::sync_bare_base(&locked_cache, &hub.path)
        })
        .await
    }

    fn check_cache(&self, cache: &Path) -> Result<()> {
        let repo = Repository::open(cache).map_err(map_git_err)?;
        let remote = repo.find_remote("origin").map_err(map_git_err)?;
        if !remote
            .url()
            .ok()
            .is_some_and(|url| self.identity.matches_origin(url))
        {
            return Err(Error::Forbidden(
                "hub cache origin does not match repository".into(),
            ));
        }
        Ok(())
    }

    fn open(&self) -> Result<Repository> {
        let repo = Repository::open_bare(&self.path).map_err(map_git_err)?;
        let key = repo
            .config()
            .map_err(map_git_err)?
            .get_string("intent.hubIdentity")
            .map_err(map_git_err)?;
        if key != self.identity.key() || repo.path().join("objects/info/alternates").exists() {
            return Err(Error::Forbidden(
                "hub identity or retention invariant violated".into(),
            ));
        }
        Ok(repo)
    }

    /// Validate an agent's proposed receive without moving any refs. The caller
    /// stages it until the successful-checkpoint transaction commits. Objects must
    /// already have passed quarantine/closure validation in the receiving service.
    ///
    /// # Errors
    /// Rejects stale/forged assignments, other namespaces, symbolic refs, stale OIDs,
    /// duplicate refs, missing objects, and non-commit targets.
    pub fn validate_receive(
        &self,
        authenticated: &HubAssignment,
        current: &HubAssignment,
        updates: &[RefUpdate],
    ) -> Result<()> {
        self.authorize(authenticated, current, updates)?;
        let repo = self.open()?;
        for update in updates {
            check_update(&repo, update)?;
        }
        Ok(())
    }

    /// Head-only alias repair, using the CURRENT successful DB pointer, never an
    /// upload callback's saved manifest. Caller holds assignment/checkpoint lock;
    /// authenticated/current equality is rechecked and each exact ref is locked
    /// before CAS validation. A Git I/O failure may require replaying alias repair:
    /// Git and `SQLite` (and individual libgit2 ref commits) are not one transaction.
    ///
    /// # Errors
    /// Same admission errors as `validate_receive`, plus ref lock/write failures.
    pub fn finalize_agent_refs(
        &self,
        authenticated: &HubAssignment,
        current: &HubAssignment,
        updates: &[RefUpdate],
    ) -> Result<()> {
        self.authorize(authenticated, current, updates)?;
        self.apply(updates)
    }

    fn authorize(
        &self,
        authenticated: &HubAssignment,
        current: &HubAssignment,
        updates: &[RefUpdate],
    ) -> Result<()> {
        if authenticated != current || current.repo_key != self.identity.key() {
            return Err(Error::Forbidden(
                "hub assignment is stale or outside repository".into(),
            ));
        }
        for id in [&current.lease_id, &current.incarnation_id] {
            component(id)?;
        }
        let head = agent_ref(&current.workspace_id, &current.agent_id, AgentRef::Head)?;
        let wip = agent_ref(&current.workspace_id, &current.agent_id, AgentRef::Wip)?;
        let mut names = BTreeSet::new();
        for update in updates {
            if (update.name != head && update.name != wip) || !names.insert(&update.name) {
                return Err(Error::Forbidden(
                    "hub receive ref is outside agent scope or duplicated".into(),
                ));
            }
        }
        Ok(())
    }

    /// Head-only immutable recovery anchor. Retrying the same target is harmless;
    /// replacing or deleting an existing anchor is not allowed.
    ///
    /// # Errors
    /// Rejects invalid IDs, missing/wrong-type objects and attempts to move anchors.
    pub fn anchor(&self, checkpoint: &str, kind: CheckpointRef, target: Oid) -> Result<()> {
        let name = checkpoint_ref(checkpoint, &self.identity.key(), kind)?;
        let repo = self.open()?;
        let existing = direct_target(&repo, &name)?;
        if existing.is_some_and(|old| old != target) {
            return Err(Error::Forbidden("checkpoint anchor is immutable".into()));
        }
        self.apply(&[RefUpdate {
            name,
            expected: existing,
            new: Some(target),
        }])
    }

    fn apply(&self, updates: &[RefUpdate]) -> Result<()> {
        let repo = self.open()?;
        let signature = git2::Signature::now("Intent", "hub@intent.local").map_err(map_git_err)?;
        let mut transaction = repo.transaction().map_err(map_git_err)?;
        let mut ordered: Vec<_> = updates.iter().collect();
        ordered.sort_by(|a, b| a.name.cmp(&b.name));
        for update in &ordered {
            transaction.lock_ref(&update.name).map_err(map_git_err)?;
        }
        for update in &ordered {
            check_update(&repo, update)?;
        }
        for update in &ordered {
            if let Some(target) = update.new {
                transaction
                    .set_target(
                        &update.name,
                        target,
                        Some(&signature),
                        "head hub ref update",
                    )
                    .map_err(map_git_err)?;
            } else if update.expected.is_some() {
                transaction.remove(&update.name).map_err(map_git_err)?;
            }
        }
        transaction.commit().map_err(map_git_err)
    }
}

fn direct_target(repo: &Repository, name: &str) -> Result<Option<Oid>> {
    match repo.find_reference(name) {
        Ok(reference) => reference
            .target()
            .map(Some)
            .ok_or_else(|| Error::Forbidden("symbolic hub refs are forbidden".into())),
        Err(e) if e.code() == ErrorCode::NotFound => Ok(None),
        Err(e) => Err(map_git_err(e)),
    }
}

fn check_update(repo: &Repository, update: &RefUpdate) -> Result<()> {
    if direct_target(repo, &update.name)? != update.expected {
        return Err(Error::Forbidden("hub ref expected old OID mismatch".into()));
    }
    if let Some(target) = update.new {
        repo.find_commit(target).map_err(map_git_err)?;
    }
    Ok(())
}

// By-value so filesystem operations can use this directly with map_err.
#[expect(clippy::needless_pass_by_value)]
fn io_error(e: std::io::Error) -> Error {
    Error::Internal(format!("hub filesystem operation failed: {e}"))
}

#[cfg(test)]
mod tests;
