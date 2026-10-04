//! Private repository cache. Source equality and freshness are data checks;
//! every use separately retains and checks its original caller/connection owner.
use std::path::{Path, PathBuf};
use std::sync::Arc;

use intent_core::{Error, Result};
use sha2::{Digest, Sha256};

use super::{CacheEnsureEvent, CacheEnsureProgress};
use crate::native_checkout::{
    self, NativeCheckoutCredentials, NativeCheckoutSelection, NativeCheckoutSource,
};

/// The caller's original lifetime/credential/project guard. Exactly one pure
/// transfer under its existing locks, never a network/file operation or await.
pub trait NativeCacheAuthority: Send + Sync {
    fn with_current(&self, transfer: &mut (dyn FnMut() -> Result<()> + Send)) -> Result<()>;
}

/// Exact refs observed in this qualified cache, not inferred defaults.
pub struct QualifiedCachedBranches {
    pub branches: Vec<NativeCheckoutSelection>,
    pub default_branch: Option<String>,
}

struct CreatedCheckout {
    destination: Option<PathBuf>,
    selection: NativeCheckoutSelection,
}
impl CreatedCheckout {
    fn publish(mut self) -> NativeCheckoutSelection {
        self.destination = None;
        self.selection.clone()
    }
}
impl Drop for CreatedCheckout {
    fn drop(&mut self) {
        if let Some(path) = &self.destination {
            let _ = std::fs::remove_dir_all(path);
        }
    }
}

/// Original connection namespace + full exact source + original authority.
/// No constructor accepts an arbitrary cache path, token or renderer grant.
pub struct QualifiedRepositoryCache {
    source: NativeCheckoutSource,
    path: PathBuf,
    authority: Arc<dyn NativeCacheAuthority>,
}

fn unavailable() -> Error {
    Error::Internal("Qualified repository cache unavailable".into())
}

fn current<T: Send>(
    authority: &dyn NativeCacheAuthority,
    transfer: impl FnOnce() -> T + Send,
) -> Result<T> {
    let mut transfer = Some(transfer);
    let mut value = None;
    authority.with_current(&mut || {
        value = Some(transfer.take().ok_or_else(unavailable)?());
        Ok(())
    })?;
    value.ok_or_else(unavailable)
}

impl QualifiedRepositoryCache {
    /// The namespace must come from the captured connection/authority owner.
    /// It is cache separation, not authority; every hit still calls the guard.
    /// # Errors
    /// Refuses an empty namespace and a retired original authority.
    pub fn new(
        cache_root: &Path,
        source: NativeCheckoutSource,
        connection_key: &str,
        authority: Arc<dyn NativeCacheAuthority>,
    ) -> Result<Self> {
        if connection_key.is_empty() {
            return Err(unavailable());
        }
        current(authority.as_ref(), || ())?;
        let mut digest = Sha256::new();
        digest.update(b"intent-qualified-repository-cache-v1");
        for value in [connection_key, source.url()] {
            digest.update((value.len() as u64).to_be_bytes());
            digest.update(value.as_bytes());
        }
        let mut name = String::with_capacity(64);
        for byte in digest.finalize() {
            use std::fmt::Write;
            write!(&mut name, "{byte:02x}").expect("write digest to string");
        }
        Ok(Self {
            source,
            path: cache_root.join("qualified").join(name),
            authority,
        })
    }

    /// Warm/refresh an exact selection. No freshness or origin check skips the
    /// original authority, and a late completion cannot mark a retired request fresh.
    /// # Errors
    /// Preserves original authority refusals and rejects changed source/branch/auth.
    pub async fn ensure(
        &self,
        selection: NativeCheckoutSelection,
        mut credential: Box<dyn NativeCheckoutCredentials>,
        progress: Option<CacheEnsureProgress>,
    ) -> Result<()> {
        current(self.authority.as_ref(), || ())?;
        let path = self.path.clone();
        let source = self.source.clone();
        let authority = self.authority.clone();
        super::with_cache_lock_blocking(&self.path, move || {
            current(authority.as_ref(), || ())?;
            let valid = git2::Repository::open(&path).ok().is_some_and(|repo| {
                native_checkout::verify_remote(&repo, &source, &selection).is_ok()
            });
            if valid && super::is_fresh(&path, super::fresh_ttl()) {
                current(authority.as_ref(), || ())?;
                super::emit(progress.as_ref(), CacheEnsureEvent::Step("fresh"));
                return Ok(());
            }
            if path.exists() {
                let repo = git2::Repository::open(&path).ok();
                if repo
                    .as_ref()
                    .is_some_and(|repo| native_checkout::source_matches(repo, &source))
                {
                    drop(repo);
                    super::emit(progress.as_ref(), CacheEnsureEvent::Step("fetch"));
                    native_checkout::fetch_exact(&source, &path, &selection, credential.as_mut())?;
                } else {
                    return Err(unavailable());
                }
            } else {
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent).map_err(|_| unavailable())?;
                }
                super::emit(progress.as_ref(), CacheEnsureEvent::Step("clone"));
                native_checkout::clone_exact(&source, &path, &selection, credential.as_mut())?;
            }
            current(authority.as_ref(), || super::mark_fresh(&path))?;
            Ok(())
        })
        .await?;
        current(self.authority.as_ref(), || ())
    }

    /// Read refs only after source/authority checks. Busy is a cache miss; known
    /// denial is an error, never an empty success or fallback to origin freshness.
    /// # Errors
    /// Preserves the original authority refusal or malformed repository error.
    pub async fn branches(&self) -> Result<Option<QualifiedCachedBranches>> {
        current(self.authority.as_ref(), || ())?;
        let lock = super::lock_for(&self.path);
        let Ok(guard) = lock.try_lock_owned() else {
            return Ok(None);
        };
        let path = self.path.clone();
        let source = self.source.clone();
        let authority = self.authority.clone();
        let branches = tokio::task::spawn_blocking(move || {
            let _guard = guard;
            current(authority.as_ref(), || ())?;
            let Ok(repo) = git2::Repository::open(&path) else {
                return Ok(None);
            };
            if !native_checkout::source_matches(&repo, &source) {
                return Err(unavailable());
            }
            let mut branches = Vec::new();
            for branch in repo
                .branches(Some(git2::BranchType::Remote))
                .map_err(|_| unavailable())?
            {
                let (branch, _) = branch.map_err(|_| unavailable())?;
                if let Some(name) = branch
                    .name()
                    .map_err(|_| unavailable())?
                    .and_then(|name| name.strip_prefix("origin/"))
                {
                    if name != "HEAD" {
                        let oid = branch.get().target().ok_or_else(unavailable)?;
                        branches.push(NativeCheckoutSelection::new(name, &oid.to_string())?);
                    }
                }
            }
            branches.sort_unstable_by(|a, b| a.branch.cmp(&b.branch));
            let result = QualifiedCachedBranches {
                branches,
                default_branch: super::default_branch(&repo).ok(),
            };
            current(authority.as_ref(), || Some(result))
        })
        .await
        .map_err(|_| unavailable())??;
        current(self.authority.as_ref(), || branches)
    }

    /// Provision an exact standalone checkout from this original cache. R owns
    /// destination reservation/workspace publication and its final reply fence.
    /// # Errors
    /// Refuses stale authority/source/selection; cleans only its new destination.
    pub async fn checkout(
        &self,
        destination: PathBuf,
        selection: NativeCheckoutSelection,
    ) -> Result<NativeCheckoutSelection> {
        current(self.authority.as_ref(), || ())?;
        let path = self.path.clone();
        let source = self.source.clone();
        let authority = self.authority.clone();
        let result = super::with_cache_lock_blocking(&self.path, move || {
            current(authority.as_ref(), || ())?;
            let actual = native_checkout::from_cache(&source, &path, &destination, &selection)?;
            let created = CreatedCheckout {
                destination: Some(destination),
                selection: actual,
            };
            current(authority.as_ref(), || created)
        })
        .await?;
        Ok(current(self.authority.as_ref(), || result)?.publish())
    }
}

#[cfg(test)]
mod tests;
