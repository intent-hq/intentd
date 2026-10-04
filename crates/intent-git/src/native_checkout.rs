//! HTTPS checkout primitives for an already qualified original connection.
//! No credential helper, token environment, URL rewrite or redirect fallback.
use std::path::Path;

use git2::{build::RepoBuilder, Cred, Repository};
#[cfg(test)]
use git2::{FetchOptions, RemoteCallbacks, RemoteRedirect};
use intent_core::{Error, GitRemoteUrl, Result};

mod http;

/// Exact source supplied by the admitted provider/caller. Not execution authority.
#[derive(Clone)]
pub struct NativeCheckoutSource {
    url: String,
}
impl NativeCheckoutSource {
    /// # Errors
    /// Refuses non-HTTPS, credentials, ambiguous path components and URL suffixes.
    pub fn https(url: &str) -> Result<Self> {
        let parsed = GitRemoteUrl::parse(url).ok_or_else(unavailable)?;
        if !url.starts_with("https://")
            || url
                .chars()
                .any(|c| c.is_control() || c.is_whitespace() || matches!(c, '@' | '?' | '#' | '\\'))
            || parsed
                .path()
                .split('/')
                .skip(1)
                .any(|p| p.is_empty() || p == "." || p == "..")
            || ["%2e", "%2f", "%5c", "%25", "%00"]
                .iter()
                .any(|p| url.to_ascii_lowercase().contains(p))
        {
            return Err(unavailable());
        }
        Ok(Self { url: url.into() })
    }

    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }
}

/// Immutable observed remote branch and object, never a fabricated default.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeCheckoutSelection {
    pub branch: String,
    pub commit_sha: String,
}
impl NativeCheckoutSelection {
    /// # Errors
    /// Requires a valid branch and full object ID from the selected provider result.
    pub fn new(branch: &str, commit_sha: &str) -> Result<Self> {
        if branch.is_empty()
            || !git2::Reference::is_valid_name(&format!("refs/heads/{branch}"))
            || git2::Oid::from_str(commit_sha).is_err()
            || commit_sha.len() != 40
        {
            return Err(Error::InvalidParams(
                "An observed branch and full commit SHA are required".into(),
            ));
        }
        Ok(Self {
            branch: branch.into(),
            commit_sha: commit_sha.into(),
        })
    }
    fn check(&self) -> Result<()> {
        Self::new(&self.branch, &self.commit_sha).map(|_| ())
    }
}

/// Operation-local credential and original response attribution. No raw token API.
pub trait NativeCheckoutCredentials: Send {
    fn credential(&mut self, _url: &str) -> std::result::Result<Cred, git2::Error> {
        Err(git2::Error::from_str(
            "Native HTTPS requires request admission",
        ))
    }
    /// Admit one local ref/publication effect, without holding metadata locks
    /// during filesystem IO. Already admitted effects cannot be recalled.
    fn with_current(&self, _transfer: &mut (dyn FnMut() -> Result<()> + Send)) -> Result<()> {
        Err(unavailable())
    }
    /// Borrow credentials only inside the original consuming admission. The
    /// callback prepares one request; it must not perform IO or retain a borrow.
    /// An implementation without this admission cannot use HTTPS checkout.
    fn with_basic_auth(
        &mut self,
        _url: &str,
        _prepare: &mut (dyn FnMut(&str, &str) -> Result<()> + Send),
    ) -> Result<()> {
        Err(unavailable())
    }
    /// A real transport authentication refusal belongs only to this original source.
    fn rejected(&self);
    /// Observe the actual response before decoding or optional result handling.
    fn observe(&self, status: u16, _backoff_until: Option<std::time::Instant>) {
        if matches!(status, 401 | 403 | 404) {
            self.rejected();
        }
    }
}

fn unavailable() -> Error {
    Error::Internal("Qualified native HTTPS checkout unavailable".into())
}
#[cfg(test)]
fn transport_error(error: git2::Error) -> Error {
    if error.code() == git2::ErrorCode::Auth {
        Error::GitAuthorization("The selected repository connection was refused".into())
    } else {
        unavailable()
    }
}

fn reject_transport_overrides(config: &git2::Config) -> Result<()> {
    let mut entries = config.entries(None).map_err(|_| unavailable())?;
    while let Some(entry) = entries.next() {
        let entry = entry.map_err(|_| unavailable())?;
        let name = entry
            .name()
            .map_err(|_| unavailable())?
            .to_ascii_lowercase();
        if (name.starts_with("url.")
            && (name.ends_with(".insteadof") || name.ends_with(".pushinsteadof")))
            || (name.starts_with("http.")
                && (name.ends_with("extraheader")
                    || name.ends_with("proxy")
                    || name.ends_with("sslverify")))
        {
            return Err(unavailable());
        }
    }
    Ok(())
}

#[cfg(test)]
fn options<'a>(
    source: &'a NativeCheckoutSource,
    credential: &'a mut dyn NativeCheckoutCredentials,
) -> FetchOptions<'a> {
    let mut callbacks = RemoteCallbacks::new();
    let mut attempts = 0;
    callbacks.credentials(move |url, _, allowed| {
        if url != source.url
            || !allowed.contains(git2::CredentialType::USER_PASS_PLAINTEXT)
            || attempts >= 3
        {
            return Err(git2::Error::from_str(
                "Original HTTPS repository credential only",
            ));
        }
        attempts += 1;
        credential.credential(url)
    });
    let mut options = FetchOptions::new();
    options.remote_callbacks(callbacks);
    options.follow_redirects(RemoteRedirect::None);
    options.proxy_options(git2::ProxyOptions::new());
    options.prune(git2::FetchPrune::On);
    options
}

/// Clone only the observed remote branch; refuse a moved branch instead of
/// silently checking out a newer or unrelated commit. Call on an owned worker.
/// # Errors
/// Refuses existing paths, transport/config overrides, authentication and changed HEAD.
pub fn clone_exact(
    source: &NativeCheckoutSource,
    destination: &Path,
    selection: &NativeCheckoutSelection,
    credential: &mut dyn NativeCheckoutCredentials,
) -> Result<NativeCheckoutSelection> {
    selection.check()?;
    if destination.exists() {
        return Err(unavailable());
    }
    reject_transport_overrides(&git2::Config::open_default().map_err(|_| unavailable())?)?;
    std::fs::create_dir(destination).map_err(|_| unavailable())?;
    let result = (|| {
        if source.url.starts_with("https://") {
            let repo = Repository::init(destination).map_err(|_| unavailable())?;
            repo.remote("origin", source.url())
                .map_err(|_| unavailable())?;
            http::fetch(
                &repo,
                source,
                &selection.branch,
                Some(&selection.commit_sha),
                credential,
            )?;
            let reference = format!("refs/heads/{}", selection.branch);
            let oid = git2::Oid::from_str(&selection.commit_sha).map_err(|_| unavailable())?;
            repo.reference(&reference, oid, false, "qualified checkout")
                .map_err(|_| unavailable())?;
            repo.set_head(&reference).map_err(|_| unavailable())?;
            repo.checkout_head(Some(git2::build::CheckoutBuilder::new().safe()))
                .map_err(|_| unavailable())?;
            return Ok(selection.clone());
        }
        #[cfg(test)]
        {
            let mut builder = RepoBuilder::new();
            builder.branch(&selection.branch);
            builder.fetch_options(options(source, credential));
            let cloned = builder.clone(source.url(), destination);
            drop(builder);
            let repo = cloned.map_err(|error| {
                if error.code() == git2::ErrorCode::Auth {
                    credential.rejected();
                }
                transport_error(error)
            })?;
            verify_remote(&repo, source, selection)?;
            let actual = repo.head().map_err(|_| unavailable())?;
            if actual.name().ok() != Some(format!("refs/heads/{}", selection.branch).as_str())
                || actual.target().map(|oid| oid.to_string()).as_deref()
                    != Some(&selection.commit_sha)
            {
                return Err(unavailable());
            }
            Ok(selection.clone())
        }
        #[cfg(not(test))]
        Err(unavailable())
    })();
    if result.is_err() {
        let _ = std::fs::remove_dir_all(destination);
    }
    result
}

/// Fetch into an existing qualified repository without modifying its working tree.
/// # Errors
/// Refuses replaced origin/config, auth/redirect failures and a moved selected branch.
pub fn fetch_exact(
    source: &NativeCheckoutSource,
    path: &Path,
    selection: &NativeCheckoutSelection,
    credential: &mut dyn NativeCheckoutCredentials,
) -> Result<NativeCheckoutSelection> {
    selection.check()?;
    let repo = Repository::open(path).map_err(|_| unavailable())?;
    reject_transport_overrides(&repo.config().map_err(|_| unavailable())?)?;
    if !source_matches(&repo, source) {
        return Err(unavailable());
    }
    if source.url.starts_with("https://") {
        return http::fetch(
            &repo,
            source,
            &selection.branch,
            Some(&selection.commit_sha),
            credential,
        );
    }
    #[cfg(test)]
    {
        let mut remote = repo
            .remote_anonymous(source.url())
            .map_err(|_| unavailable())?;
        let fetched = remote.fetch(
            &["+refs/heads/*:refs/remotes/origin/*"],
            Some(&mut options(source, credential)),
            None,
        );
        fetched.map_err(|error| {
            if error.code() == git2::ErrorCode::Auth {
                credential.rejected();
            }
            transport_error(error)
        })?;
        verify_remote(&repo, source, selection)?;
        Ok(selection.clone())
    }
    #[cfg(not(test))]
    Err(unavailable())
}

/// Fetch the current remote branch into remote-tracking refs, never local HEAD.
/// # Errors
/// Refuses source replacement, admission/transport denial and missing branches.
pub fn fetch_original(
    path: &Path,
    source: &NativeCheckoutSource,
    branch: &str,
    credential: &mut dyn NativeCheckoutCredentials,
) -> Result<NativeCheckoutSelection> {
    let repo = open_original(path, source)?;
    http::fetch(&repo, source, branch, None, credential)
}

/// Push the original local branch HEAD to exactly the selected origin/branch.
/// # Errors
/// Refuses a moved local HEAD, non-fast-forward without force, redirects, or
/// server rejection. A lost response after send is reported as uncertain.
pub fn push_original(
    path: &Path,
    source: &NativeCheckoutSource,
    branch: &str,
    force: bool,
    credential: &mut dyn NativeCheckoutCredentials,
) -> Result<NativeCheckoutSelection> {
    let repo = open_original(path, source)?;
    http::push(&repo, source, branch, force, credential)
}

fn open_original(path: &Path, source: &NativeCheckoutSource) -> Result<Repository> {
    let repo = Repository::open(path).map_err(|_| unavailable())?;
    reject_transport_overrides(&repo.config().map_err(|_| unavailable())?)?;
    if !source.url.starts_with("https://") || !source_matches(&repo, source) {
        return Err(unavailable());
    }
    Ok(repo)
}

pub(crate) fn source_matches(repo: &Repository, source: &NativeCheckoutSource) -> bool {
    repo.find_remote("origin")
        .ok()
        .and_then(|remote| remote.url().ok().map(String::from))
        .as_deref()
        == Some(source.url())
}

pub(crate) fn verify_remote(
    repo: &Repository,
    source: &NativeCheckoutSource,
    selection: &NativeCheckoutSelection,
) -> Result<()> {
    if !source_matches(repo, source)
        || repo
            .refname_to_id(&format!("refs/remotes/origin/{}", selection.branch))
            .ok()
            .map(|oid| oid.to_string())
            .as_deref()
            != Some(&selection.commit_sha)
    {
        return Err(unavailable());
    }
    Ok(())
}

/// All-local standalone checkout from a qualified cache. Caller holds that
/// cache's original lock and checks authority before and after IO.
/// # Errors
/// Refuses stale/mismatched source or selection and removes only its new destination.
pub(crate) fn from_cache(
    source: &NativeCheckoutSource,
    cache: &Path,
    destination: &Path,
    selection: &NativeCheckoutSelection,
) -> Result<NativeCheckoutSelection> {
    selection.check()?;
    if destination.exists() {
        return Err(unavailable());
    }
    let original = Repository::open(cache).map_err(|_| unavailable())?;
    verify_remote(&original, source, selection)?;
    reject_transport_overrides(&git2::Config::open_default().map_err(|_| unavailable())?)?;
    std::fs::create_dir(destination).map_err(|_| unavailable())?;
    let result = (|| {
        let mut builder = RepoBuilder::new();
        builder.clone_local(git2::build::CloneLocal::Local);
        let repo = builder
            .clone(cache.to_str().ok_or_else(unavailable)?, destination)
            .map_err(|_| unavailable())?;
        let oid = git2::Oid::from_str(&selection.commit_sha).map_err(|_| unavailable())?;
        let object = repo
            .find_object(oid, Some(git2::ObjectType::Commit))
            .map_err(|_| unavailable())?;
        repo.reference(
            &format!("refs/heads/{}", selection.branch),
            oid,
            true,
            "qualified checkout",
        )
        .map_err(|_| unavailable())?;
        repo.set_head(&format!("refs/heads/{}", selection.branch))
            .map_err(|_| unavailable())?;
        repo.reset(&object, git2::ResetType::Hard, None)
            .map_err(|_| unavailable())?;
        for branch in original
            .branches(Some(git2::BranchType::Remote))
            .map_err(|_| unavailable())?
        {
            let (branch, _) = branch.map_err(|_| unavailable())?;
            if let (Ok(Some(name)), Some(target)) = (branch.name(), branch.get().target()) {
                repo.reference(
                    &format!("refs/remotes/{name}"),
                    target,
                    true,
                    "qualified cached refs",
                )
                .map_err(|_| unavailable())?;
            }
        }
        repo.remote_set_url("origin", source.url())
            .map_err(|_| unavailable())?;
        verify_remote(&repo, source, selection)?;
        Ok(selection.clone())
    })();
    if result.is_err() {
        let _ = std::fs::remove_dir_all(destination);
    }
    result
}

#[cfg(test)]
pub(crate) mod tests;
