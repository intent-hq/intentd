//! Process-local invalidation for managed repository mutations, never permission.
//!
//! The service observer owns subscriptions and retirement. Store only brackets
//! actual writers. Raw pool users, external SQL, import and physical filesystem
//! writers are not covered by this boundary and cannot enable repository admission.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use intent_core::{AgentId, WorkspaceGitRootId, WorkspaceId};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

use crate::{Error, Result, Store};

pub(crate) mod initialization;
pub use initialization::{
    RepositoryAcpCompatibilityEffect, RepositoryAcpCompatibilityOutcome,
    RepositoryAcpCompatibilityPersistence, RepositoryAcpCompatibilityResult,
    RepositoryAcpInitialization, RepositoryInitializationBinding, RepositoryInitializationClaim,
    RepositoryInitializationConfirmation, RepositoryInitializationObservation,
    RepositoryInitializationOutcome, RepositoryInitializationPersistence,
    RepositoryInitializationTicket,
};

/// Invalidation coordinates, not identities supplied as permission claims.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum RepositoryLifecycleKey {
    /// Every subscription includes this domain-wide retirement coordinate.
    Database,
    /// Human authority changes; only original Wire read requests subscribe.
    WireAuthority,
    /// A saved choice only; physical owners do not subscribe to this key.
    Selection {
        workspace_id: WorkspaceId,
        git_root_id: Option<WorkspaceGitRootId>,
    },
    /// The originally captured workspace.
    Workspace(WorkspaceId),
    /// The originally captured agent row.
    Agent(AgentId),
    /// The originally captured registered root.
    GitRoot(WorkspaceGitRootId),
    /// A path already qualified by the physical-root owner, not by Store.
    Worktree(PathBuf),
}

/// One original mutation owner. Dropping this ticket MUST NOT settle a barrier.
pub trait RepositoryLifecycleMutationTicket: Send {
    /// Consume only after the original owner confirmed commit or no effect.
    fn settle_confirmed(self: Box<Self>);
}

/// Invalidation only. No database, network or asynchronous work may run here.
pub trait RepositoryLifecycleObserver: Send + Sync {
    /// Atomically block ALL keys and detach matching subscriptions, then retire
    /// their existing leaves outside the registry lock before returning.
    ///
    /// # Errors
    /// Returns an error when the invalidation domain cannot safely begin.
    fn begin_mutation(
        &self,
        keys: &[RepositoryLifecycleKey],
    ) -> Result<Box<dyn RepositoryLifecycleMutationTicket>>;

    /// Block matching requests before a reversible pending-delete marker changes.
    /// Retire request leaves, not their still-live physical owners; actual Store
    /// deletion continues to use `begin_mutation`. No metadata lock may cross
    /// retirement waits. Settlement belongs only to the original operation.
    ///
    /// # Errors
    /// Unavailable by default. Ordinary mutation retirement is not a substitute.
    fn begin_pending_delete(
        &self,
        _keys: &[RepositoryLifecycleKey],
    ) -> Result<Box<dyn RepositoryLifecycleMutationTicket>> {
        Err(lifecycle_error("pending deletion is unavailable"))
    }

    /// Authenticate and consume an original pending physical owner's proof.
    /// Atomically block its keys, retire every prior live origin and competing
    /// pending attempt, and preserve only the original attempt as installing.
    /// This also applies to an original successful load without a row change.
    /// No registry lock may be retained across asynchronous Store work.
    ///
    /// # Errors
    /// Denies by default; implementations must reject forged, stale or rebound
    /// proof. IDs and a canonical session string never establish ownership.
    fn begin_initialization(
        &self,
        _original_owner: Box<dyn std::any::Any + Send>,
        _binding: &RepositoryInitializationBinding,
    ) -> Result<Box<dyn RepositoryInitializationTicket>> {
        Err(lifecycle_error("original initialization is unavailable"))
    }
}

#[derive(Default)]
struct DomainState {
    observer: Option<Arc<dyn RepositoryLifecycleObserver>>,
    active_writers: usize,
    unconfirmed_without_observer: bool,
    invalidated: bool,
}

pub(crate) struct LifecycleDomain {
    identity: DatabaseIdentity,
    state: Mutex<DomainState>,
    writers: Arc<AsyncMutex<()>>,
    // Keep the file incarnation alive even after one Store closes its pools;
    // device/inode reuse cannot silently attach a replacement database.
    _database_file: std::fs::File,
}

#[derive(Clone, Hash, PartialEq, Eq)]
enum DatabaseIdentity {
    #[cfg(unix)]
    File(u64, u64),
    #[cfg(not(unix))]
    Path(PathBuf),
}

#[derive(Default)]
struct Domains {
    files: HashMap<DatabaseIdentity, Weak<LifecycleDomain>>,
    paths: HashMap<PathBuf, (DatabaseIdentity, Weak<LifecycleDomain>)>,
    // An installed observer, unresolved writer or retired incarnation must not
    // disappear when the last Store drops: its SQLite worker may still be live.
    retained: HashMap<DatabaseIdentity, Arc<LifecycleDomain>>,
}

static DOMAINS: OnceLock<Mutex<Domains>> = OnceLock::new();

fn lifecycle_error(message: &str) -> Error {
    Error::Internal(format!("repository lifecycle: {message}"))
}

/// All managed opens of the same file share an observer, including opens before
/// installation. Only confirmed, unobserved domains may be reclaimed; protected
/// domains retain their actual file incarnation until this process exits.
pub(crate) fn domain_for(path: &Path) -> Result<Arc<LifecycleDomain>> {
    let database_file = std::fs::File::open(path)
        .map_err(|e| lifecycle_error(&format!("database handle unavailable: {e}")))?;
    let canonical = std::fs::canonicalize(path)
        .map_err(|e| lifecycle_error(&format!("database path unavailable: {e}")))?;
    #[cfg(unix)]
    let identity = {
        use std::os::unix::fs::MetadataExt;
        let metadata = database_file
            .metadata()
            .map_err(|e| lifecycle_error(&format!("database identity unavailable: {e}")))?;
        DatabaseIdentity::File(metadata.dev(), metadata.ino())
    };
    #[cfg(not(unix))]
    let identity = DatabaseIdentity::Path(canonical.clone());
    let mut domains = DOMAINS
        .get_or_init(Mutex::default)
        .lock()
        .map_err(|_| lifecycle_error("managed database registry poisoned"))?;
    domains.files.retain(|_, domain| domain.strong_count() > 0);
    domains
        .paths
        .retain(|_, (_, domain)| domain.strong_count() > 0);
    if let Some((previous, domain)) = domains.paths.get(&canonical) {
        if *previous != identity {
            if let Some(domain) = domain.upgrade() {
                drop(domains);
                let observer = {
                    let mut state = domain
                        .state
                        .lock()
                        .map_err(|_| lifecycle_error("database domain poisoned"))?;
                    state.invalidated = true;
                    state.observer.clone()
                };
                domain.retain_for_process();
                if let Some(observer) = observer {
                    // The original database has been replaced outside Store.
                    // Never settle this retirement or hand out a fresh domain.
                    drop(observer.begin_mutation(&[RepositoryLifecycleKey::Database])?);
                }
                return Err(lifecycle_error(
                    "database path changed its live file incarnation",
                ));
            }
        }
    }
    if let Some(domain) = domains.files.get(&identity).and_then(Weak::upgrade) {
        domains
            .paths
            .insert(canonical, (identity, Arc::downgrade(&domain)));
        return Ok(domain);
    }
    let domain = Arc::new(LifecycleDomain {
        identity: identity.clone(),
        state: Mutex::default(),
        writers: Arc::default(),
        _database_file: database_file,
    });
    domains
        .paths
        .insert(canonical, (identity.clone(), Arc::downgrade(&domain)));
    domains.files.insert(identity, Arc::downgrade(&domain));
    Ok(domain)
}

impl LifecycleDomain {
    fn retain_for_process(self: &Arc<Self>) {
        // domain_for releases the registry lock before taking any domain lock.
        // Preserve protection even if the registry was poisoned; ordinary
        // lookup still rejects that poisoned registry instead of admitting work.
        let mut domains = DOMAINS
            .get_or_init(Mutex::default)
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        domains
            .retained
            .entry(self.identity.clone())
            .or_insert_with(|| self.clone());
    }

    pub(crate) async fn write(self: &Arc<Self>) -> Result<LifecycleWrite> {
        let serial = self.writers.clone().lock_owned().await;
        let mut state = self
            .state
            .lock()
            .map_err(|_| lifecycle_error("database domain poisoned"))?;
        if state.invalidated {
            return Err(lifecycle_error("database incarnation was retired"));
        }
        state.active_writers = state
            .active_writers
            .checked_add(1)
            .ok_or_else(|| lifecycle_error("writer count exhausted"))?;
        drop(state);
        Ok(LifecycleWrite {
            domain: self.clone(),
            serial: Some(serial),
            ticket: None,
            begun: false,
            confirmed: false,
        })
    }
}

/// The async serialization lock belongs to Store, not the service registry or
/// retirement leaf. Nested bounded deletion drops it but retains its barrier.
pub(crate) struct LifecycleWrite {
    domain: Arc<LifecycleDomain>,
    serial: Option<OwnedMutexGuard<()>>,
    ticket: Option<Box<dyn RepositoryLifecycleMutationTicket>>,
    begun: bool,
    confirmed: bool,
}

impl LifecycleWrite {
    pub(crate) fn begin(&mut self, keys: &[RepositoryLifecycleKey]) -> Result<()> {
        if self.begun {
            return Err(lifecycle_error("mutation owner already began"));
        }
        let observer = self
            .domain
            .state
            .lock()
            .map_err(|_| lifecycle_error("database domain poisoned"))?
            .observer
            .clone();
        self.begun = true;
        if let Some(observer) = observer {
            self.ticket = Some(observer.begin_mutation(keys)?);
        }
        Ok(())
    }

    /// Capture the same installed observer under install serialization, then
    /// release that serialization before joining detached selection consumers.
    /// Active original ownership remains registered even without an observer.
    pub(crate) fn begin_selection_change(
        &mut self,
        root: &intent_core::RepositoryRootId,
    ) -> Result<()> {
        if self.begun || self.serial.is_none() {
            return Err(lifecycle_error(
                "selection owner is not original and serialized",
            ));
        }
        let observer = {
            let state = self
                .domain
                .state
                .lock()
                .map_err(|_| lifecycle_error("database domain poisoned"))?;
            if state.invalidated {
                return Err(lifecycle_error("database incarnation was retired"));
            }
            state.observer.clone()
        };
        let git_root_id = match &root.kind {
            intent_core::RepositoryRootKind::Primary => None,
            intent_core::RepositoryRootKind::Registered { git_root_id } => {
                Some(git_root_id.clone())
            }
        };
        self.begun = true;
        self.release_serialization();
        if let Some(observer) = observer {
            self.ticket = Some(
                observer.begin_mutation(&[RepositoryLifecycleKey::Selection {
                    workspace_id: root.workspace_id.clone(),
                    git_root_id,
                }])?,
            );
        }
        Ok(())
    }

    /// Reacquire only this already-begun owner's domain; never begin or repair a
    /// second ticket. The caller must compare again inside its write transaction.
    pub(crate) async fn resume_serialization(&mut self) -> Result<()> {
        if !self.begun || self.serial.is_some() {
            return Err(lifecycle_error(
                "selection owner cannot resume serialization",
            ));
        }
        let serial = self.domain.writers.clone().lock_owned().await;
        if self
            .domain
            .state
            .lock()
            .map_err(|_| lifecycle_error("database domain poisoned"))?
            .invalidated
        {
            return Err(lifecycle_error("database incarnation was retired"));
        }
        self.serial = Some(serial);
        Ok(())
    }

    pub(crate) fn release_serialization(&mut self) {
        self.serial.take();
    }

    pub(crate) fn settle(mut self) {
        if let Some(ticket) = self.ticket.take() {
            ticket.settle_confirmed();
        }
        self.confirmed = true;
    }

    pub(crate) fn finish<T>(self, result: Result<T>) -> Result<T> {
        if result.is_ok() {
            self.settle();
        }
        result
    }
}

impl Drop for LifecycleWrite {
    fn drop(&mut self) {
        if let Ok(mut state) = self.domain.state.lock() {
            state.active_writers -= 1;
            if self.begun && !self.confirmed && state.observer.is_none() {
                // A sqlx worker may outlive the dropped future. Do not install
                // an observer later and mislabel that pending write as covered.
                state.unconfirmed_without_observer = true;
            }
        }
        if self.begun && !self.confirmed {
            self.domain.retain_for_process();
        }
        // A service ticket deliberately drops without confirmation here.
    }
}

/// Original pending-deletion ownership in one managed database domain.
///
/// This is not admission or proof of complete deletion. It holds no writer lock;
/// its owner may carry it through a timer or asynchronous cleanup. Dropping it
/// without original confirmed completion retains uncertainty, including before
/// the first observer is installed and across same-process managed reopen.
#[must_use = "retain through original completion; dropping does not settle"]
pub struct RepositoryPendingDeleteGuard {
    lifecycle: LifecycleWrite,
}

impl RepositoryPendingDeleteGuard {
    /// Confirm only this operation's positively established terminal work or
    /// no effect. A public success response, current row or another operation's
    /// completion cannot establish this fact. Consumes the sole original guard.
    pub fn settle_confirmed(self) {
        self.lifecycle.settle();
    }
}

impl Store {
    /// Compare retained database-domain allocation identity only.
    ///
    /// Clones and independent managed opens can share this allocation, including
    /// after it is invalidated. This does not establish observer installation,
    /// settlement, permission or current usability; those checks remain separate.
    #[must_use]
    pub fn shares_repository_lifecycle_domain(&self, other: &Store) -> bool {
        Arc::ptr_eq(&self.repository_lifecycle, &other.repository_lifecycle)
    }

    /// Retain original ownership before publishing or claiming pending deletion.
    ///
    /// Uses the sole retained Store domain and its actual installed observer.
    /// Without an observer, the active owner still blocks first installation;
    /// dropping an unconfirmed owner preserves that protection on managed reopen.
    /// No writer/install serialization is held during the observer callback.
    /// This does not enable repository admission on unsupported platforms.
    ///
    /// # Errors
    /// Rejects empty keys, a retired/invalid domain, exhausted ownership, or an
    /// unavailable observer. An observer error never confirms an uncertain begin.
    pub async fn begin_repository_pending_delete(
        &self,
        keys: &[RepositoryLifecycleKey],
    ) -> Result<RepositoryPendingDeleteGuard> {
        if keys.is_empty() {
            return Err(lifecycle_error("pending deletion requires original keys"));
        }
        let mut lifecycle = self.repository_lifecycle_write().await?;
        let observer = {
            let state = lifecycle
                .domain
                .state
                .lock()
                .map_err(|_| lifecycle_error("database domain poisoned"))?;
            if state.invalidated {
                return Err(lifecycle_error("database incarnation was retired"));
            }
            state.observer.clone()
        };
        // This incremented owner and captured observer are protected by the
        // existing writer/install serialization. A first install now sees an
        // active owner even if this begin captured no observer.
        lifecycle.begun = true;
        lifecycle.release_serialization();
        if let Some(observer) = observer {
            lifecycle.ticket = Some(observer.begin_pending_delete(keys)?);
        }
        Ok(RepositoryPendingDeleteGuard { lifecycle })
    }

    /// Install the sole invalidation owner for this managed database lifetime.
    /// Clones and independent managed opens observe the SAME installed Arc.
    /// Installation is not a permission grant or proof of complete writer coverage.
    ///
    /// # Errors
    /// Refuses replacement, uncertain earlier writes or an unsupported file identity.
    pub async fn install_repository_lifecycle_observer(
        &self,
        observer: Arc<dyn RepositoryLifecycleObserver>,
    ) -> Result<()> {
        // Canonical paths alone cannot establish hard-link identity on these
        // platforms. Preserve ordinary Store behavior but leave B unavailable.
        if !cfg!(unix) {
            return Err(lifecycle_error("managed file identity is unavailable"));
        }
        let _serial = self.repository_lifecycle.writers.lock().await;
        let mut state = self
            .repository_lifecycle
            .state
            .lock()
            .map_err(|_| lifecycle_error("database domain poisoned"))?;
        if state.invalidated {
            return Err(lifecycle_error("database incarnation was retired"));
        }
        if let Some(installed) = &state.observer {
            return if Arc::ptr_eq(installed, &observer) {
                Ok(())
            } else {
                Err(lifecycle_error(
                    "a different observer already owns this database",
                ))
            };
        }
        if state.active_writers != 0 || state.unconfirmed_without_observer {
            return Err(lifecycle_error(
                "an earlier writer has not confirmed settlement",
            ));
        }
        self.repository_lifecycle.retain_for_process();
        state.observer = Some(observer);
        Ok(())
    }

    /// Match the actual installed owner; this is evidence, never permission.
    #[must_use]
    pub fn has_repository_lifecycle_observer(
        &self,
        observer: &Arc<dyn RepositoryLifecycleObserver>,
    ) -> bool {
        self.repository_lifecycle.state.lock().is_ok_and(|state| {
            !state.invalidated
                && state
                    .observer
                    .as_ref()
                    .is_some_and(|installed| Arc::ptr_eq(installed, observer))
        })
    }

    pub(crate) async fn repository_lifecycle_write(&self) -> Result<LifecycleWrite> {
        self.repository_lifecycle.write().await
    }
}

#[cfg(all(test, unix))]
mod tests;

#[cfg(all(test, unix))]
mod wire_authority_tests;

#[cfg(all(test, unix))]
mod pending_delete_tests;

#[cfg(all(test, unix))]
mod selection_tests;
