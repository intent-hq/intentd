//! Concrete local root reads with the existing worktree-lock lifetime.
//!
//! Root records and context inputs are observations, never caller authority.
//! The service entry must supply its shared `WorktreeLocks`, not a new registry.
//! Every escaped source/operation is retired before that lock is released.

use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;

use base64::Engine as _;
use intent_core::{NativeReviewTransport, RepositoryRootId, RepositoryRootKind};
use intent_git::worktree::WorktreeLocks;
use intent_sourcecontrol::remote_project::CanonicalRemoteResolver;
use intent_store::Store;
use sha2::{Digest, Sha256};

use crate::repository_context_reader::{
    read_repository_context_with_resolver, AdmittedRepositoryRoot, GitConfigEnvironment,
    RepositoryContextInput, RepositoryContextRead,
};

use crate::repository_admission::{
    AdmissionError, AdmissionResult, RepositoryOperationFacts, RepositoryRetirement,
};

/// Store identity used to detect changed records around waits and Git reads.
/// Creation timestamps are compared as facts, not claimed to be ABA counters.
#[derive(Clone, PartialEq, Eq)]
pub(super) struct RootRecord {
    root: RepositoryRootId,
    lock_path: PathBuf,
    canonical_path: PathBuf,
    workspace_created_at: String,
    workspace_branch: String,
    repository_path: Option<String>,
    worktree_path: Option<String>,
    registered_created_at: Option<String>,
}

fn local_error(error: &intent_core::Error) -> AdmissionError {
    match error {
        intent_core::Error::NotFound(_) | intent_core::Error::Forbidden(_) => {
            AdmissionError::Denied
        }
        _ => AdmissionError::Unavailable,
    }
}

impl RootRecord {
    pub(super) async fn read(store: &Store, root: &RepositoryRootId) -> AdmissionResult<Self> {
        let workspace = store
            .get_workspace(&root.workspace_id)
            .await
            .map_err(|error| local_error(&error))?;
        // Current native execution is local to the admitted authoritative host.
        // A local mirror of a remote workspace is never an execution fallback.
        if workspace.is_remote || workspace.archived || workspace.pending_delete_at.is_some() {
            return Err(AdmissionError::Denied);
        }
        let (path, registered_created_at) = match &root.kind {
            RepositoryRootKind::Primary => {
                // Same stored key as ac_execute/git_ops::worktree_path. The
                // workspace metadata path is deliberately not a Git fallback.
                let path = workspace
                    .worktree_path
                    .as_ref()
                    .or(workspace.repository_path.as_ref())
                    .filter(|path| !path.is_empty())
                    .ok_or(AdmissionError::Unavailable)?;
                (path.clone(), None)
            }
            RepositoryRootKind::Registered { git_root_id } => {
                let registered = store
                    .get_workspace_git_root(git_root_id)
                    .await
                    .map_err(|error| local_error(&error))?;
                if registered.workspace_id != root.workspace_id {
                    return Err(AdmissionError::Denied);
                }
                (registered.path, Some(registered.created_at))
            }
        };
        let lock_path = PathBuf::from(path);
        if !lock_path.is_absolute() {
            return Err(AdmissionError::Unavailable);
        }
        let canonical_path = tokio::fs::canonicalize(&lock_path)
            .await
            .map_err(|_| AdmissionError::Unavailable)?;
        Ok(Self {
            root: root.clone(),
            lock_path,
            canonical_path,
            workspace_created_at: workspace.created_at,
            workspace_branch: workspace.branch,
            repository_path: workspace.repository_path,
            worktree_path: workspace.worktree_path,
            registered_created_at,
        })
    }
}

struct RetireOnDrop(RepositoryRetirement);

impl Drop for RetireOnDrop {
    fn drop(&mut self) {
        self.0.end_scope();
    }
}

/// Constructed only inside the real worktree-lock closure. Holding an Arc does
/// not extend the lock: all later reads reject the permanently retired lifetime.
pub(super) struct RepositoryGitSource {
    store: Store,
    record: RootRecord,
    retirement: RepositoryRetirement,
}

impl RepositoryGitSource {
    pub(super) async fn with_locked<T, F, Fut>(
        store: &Store,
        locks: &WorktreeLocks,
        record: RootRecord,
        retirement: RepositoryRetirement,
        action: F,
    ) -> AdmissionResult<T>
    where
        F: FnOnce(Arc<Self>) -> Fut,
        Fut: Future<Output = AdmissionResult<T>>,
    {
        // Also retire on an error/cancellation before the lock is acquired.
        let pending_lifetime = RetireOnDrop(retirement.clone());
        retirement.check_current()?;
        let lock_path = record.lock_path.clone();
        let result = locks
            .with_lock(&lock_path, || async move {
                // This guard drops before with_lock releases its actual lock.
                let _locked_lifetime = RetireOnDrop(retirement.clone());
                retirement.check_current()?;
                if RootRecord::read(store, &record.root).await? != record {
                    return Err(AdmissionError::BindingChanged);
                }
                let source = Arc::new(Self {
                    store: store.clone(),
                    record,
                    retirement,
                });
                action(source).await
            })
            .await;
        drop(pending_lifetime);
        result
    }

    pub(super) async fn check_root(&self) -> AdmissionResult<()> {
        self.retirement.check_current()?;
        let current = RootRecord::read(&self.store, &self.record.root)
            .await
            .inspect_err(|error| {
                if matches!(error, AdmissionError::Denied | AdmissionError::Retired) {
                    self.retirement.retire();
                }
            })?;
        if current != self.record {
            self.retirement.retire();
            return Err(AdmissionError::BindingChanged);
        }
        self.retirement.check_current()
    }

    /// Fresh effective Git/config read. The inventory authority, connection and
    /// historical selection in input remain explicitly supplied by their owners.
    pub(super) async fn read_context(
        &self,
        mut input: RepositoryContextInput,
        resolver: CanonicalRemoteResolver,
        environment: GitConfigEnvironment,
    ) -> AdmissionResult<RepositoryContextRead> {
        self.check_root().await?;
        let [AdmittedRepositoryRoot { root, path, .. }] = input.roots.as_mut_slice() else {
            return Err(AdmissionError::BindingChanged);
        };
        if root != &self.record.root || path != &self.record.canonical_path {
            return Err(AdmissionError::BindingChanged);
        }
        let output = tokio::task::spawn_blocking(move || {
            read_repository_context_with_resolver(&input, &resolver, &environment)
        })
        .await
        .map_err(|_| AdmissionError::Unavailable)?
        .map_err(|error| local_error(&error))?;
        self.check_root().await?;
        Ok(output)
    }

    /// Re-read actual local facts while retaining the explicitly admitted
    /// provider/account/target facts. No source account is discovered here.
    pub(super) async fn observe_operation(
        &self,
        original: &RepositoryOperationFacts,
        input: RepositoryContextInput,
        resolver: CanonicalRemoteResolver,
        environment: GitConfigEnvironment,
    ) -> AdmissionResult<RepositoryOperationFacts> {
        if original.preparation.root != self.record.root
            || original.worktree_path != self.record.canonical_path
            || input.scope != original.preparation.scope
        {
            return Err(AdmissionError::BindingChanged);
        }
        let staged = self
            .staging_fingerprint(original.staging_fingerprint.is_some())
            .await?;
        let output = self.read_context(input, resolver, environment).await?;
        let [root] = output.context.roots.as_slice() else {
            return Err(AdmissionError::Unavailable);
        };
        let [changes] = output.change_inputs.as_slice() else {
            return Err(AdmissionError::Unavailable);
        };
        let [private] = output.private_roots.as_slice() else {
            return Err(AdmissionError::Unavailable);
        };
        if private.root != self.record.root || changes.root != self.record.root {
            return Err(AdmissionError::BindingChanged);
        }
        let mut observed = original.clone();
        observed.git_dir = changes.git_dir.clone();
        observed.common_dir = changes.common_dir.clone();
        observed.preparation.context_revision = output.context.revision;
        observed.preparation.local_head_sha = root.head_sha.clone();
        let source_ref = private
            .source_ref
            .as_deref()
            .ok_or(AdmissionError::BindingChanged)?;
        let branch = source_ref
            .strip_prefix("refs/heads/")
            .filter(|name| !name.is_empty())
            .ok_or(AdmissionError::BindingChanged)?;
        observed.preparation.source.branch = branch.to_owned();
        observed.source_ref = source_ref.to_owned();
        match &original.preparation.transport {
            Some(selected) => {
                let remote = private
                    .remotes
                    .iter()
                    .find(|remote| remote.name == selected.remote_name)
                    .ok_or(AdmissionError::BindingChanged)?;
                let display = root
                    .remotes
                    .iter()
                    .find(|remote| remote.name == selected.remote_name)
                    .ok_or(AdmissionError::BindingChanged)?;
                observed.fetch_destinations.clone_from(&remote.fetch);
                observed.push_destinations.clone_from(&remote.push);
                observed.preparation.transport = Some(NativeReviewTransport {
                    remote_name: selected.remote_name.clone(),
                    fetch_urls: display
                        .fetch
                        .iter()
                        .map(|endpoint| endpoint.url.clone())
                        .collect(),
                    push_urls: display
                        .push
                        .iter()
                        .map(|endpoint| endpoint.url.clone())
                        .collect(),
                });
            }
            None if private.remotes.is_empty() => {
                observed.fetch_destinations.clear();
                observed.push_destinations.clear();
            }
            None => return Err(AdmissionError::BindingChanged),
        }
        let after = self
            .staging_fingerprint(original.staging_fingerprint.is_some())
            .await?;
        if staged != after {
            return Err(AdmissionError::BindingChanged);
        }
        observed.staging_fingerprint = after;
        self.check_root().await?;
        Ok(observed)
    }

    async fn staging_fingerprint(&self, required: bool) -> AdmissionResult<Option<String>> {
        if !required {
            return Ok(None);
        }
        self.retirement.check_current()?;
        let path = self.record.canonical_path.clone();
        let fingerprint = tokio::task::spawn_blocking(move || {
            let repository =
                git2::Repository::open(path).map_err(|_| AdmissionError::Unavailable)?;
            let mut index = repository
                .index()
                .map_err(|_| AdmissionError::Unavailable)?;
            index.read(true).map_err(|_| AdmissionError::Unavailable)?;
            let mut digest = Sha256::new();
            for entry in index.iter() {
                digest.update(entry.mode.to_le_bytes());
                digest.update(entry.flags.to_le_bytes());
                digest.update(entry.flags_extended.to_le_bytes());
                digest.update(entry.id.as_bytes());
                digest.update(entry.path.len().to_le_bytes());
                digest.update(&entry.path);
            }
            Ok(Some(
                base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(digest.finalize()),
            ))
        })
        .await
        .map_err(|_| AdmissionError::Unavailable)??;
        self.retirement.check_current()?;
        Ok(fingerprint)
    }
}
