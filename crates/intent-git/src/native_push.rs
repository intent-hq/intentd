//! A prepared HTTPS-only native push. The caller owns its original worktree lock,
//! credential and final admission; this adapter never consults a credential helper.
use git2::{Cred, PushOptions, RemoteCallbacks, RemoteRedirect, Repository};
use intent_core::{Error, Result};
use std::path::{Path, PathBuf};

fn unavailable() -> Error {
    Error::Internal("Native HTTPS push unavailable".into())
}

/// Immutable destinations/ref/SHA checked before the original consuming action.
/// Neither this value nor a path is execution authority.
pub struct PreparedNativePush {
    path: PathBuf,
    remote: String,
    reference: String,
    sha: String,
    fetch: Vec<String>,
    destinations: Vec<String>,
}
impl PreparedNativePush {
    /// Prepare only an existing local branch, all original HTTPS destinations and
    /// a non-force refspec. Rewrites, extra headers and disabled TLS are refused.
    /// # Errors
    /// Refuses unsupported transport/configuration or changed local observations.
    pub fn prepare(
        path: &Path,
        remote: &str,
        reference: &str,
        sha: &str,
        fetch: &[String],
        destinations: &[String],
    ) -> Result<Self> {
        if !reference.starts_with("refs/heads/")
            || !git2::Reference::is_valid_name(reference)
            || git2::Oid::from_str(sha).is_err()
            || destinations.is_empty()
            || destinations.len() > 16
            || fetch.len() > 16
            || destinations.iter().chain(fetch).any(|s| !https(s))
        {
            return Err(unavailable());
        }
        let value = Self {
            path: path.to_path_buf(),
            remote: remote.into(),
            reference: reference.into(),
            sha: sha.into(),
            fetch: fetch.to_vec(),
            destinations: destinations.to_vec(),
        };
        value.check()?;
        Ok(value)
    }
    fn check(&self) -> Result<Repository> {
        let repo = Repository::open(&self.path).map_err(|_| unavailable())?;
        if repo
            .head()
            .ok()
            .and_then(|r| r.name().ok().map(str::to_owned))
            .as_deref()
            != Some(&self.reference)
            || repo
                .refname_to_id(&self.reference)
                .map_err(|_| unavailable())?
                .to_string()
                != self.sha
        {
            return Err(unavailable());
        }
        let config = repo.config().map_err(|_| unavailable())?;
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
        let urls = |key: &str| -> Result<Vec<String>> {
            match config.multivar(key, None) {
                Ok(mut entries) => {
                    let mut values = Vec::new();
                    while let Some(entry) = entries.next() {
                        values.push(
                            entry
                                .map_err(|_| unavailable())?
                                .value()
                                .map_err(|_| unavailable())?
                                .to_string(),
                        );
                    }
                    Ok(values)
                }
                Err(e) if e.code() == git2::ErrorCode::NotFound => Ok(Vec::new()),
                Err(_) => Err(unavailable()),
            }
        };
        let fetch = urls(&format!("remote.{}.url", self.remote))?;
        let mut push = urls(&format!("remote.{}.pushurl", self.remote))?;
        if push.is_empty() {
            push.clone_from(&fetch);
        }
        if fetch != self.fetch || push != self.destinations {
            return Err(unavailable());
        }
        drop(entries);
        drop(config);
        Ok(repo)
    }
    /// Start immediately on the already acquired worker after final admission.
    /// `observed` records each primitive destination success before the next I/O
    /// or tracking-ref update. A later error does not erase those observations.
    /// # Errors
    /// Refuses changed facts, authentication, redirection or a non-fast-forward push.
    pub fn execute(
        self,
        mut credential: impl FnMut() -> std::result::Result<Cred, git2::Error>,
        mut observed: impl FnMut(&str),
    ) -> Result<String> {
        let repo = self.check()?;
        for destination in &self.destinations {
            let mut remote = repo
                .remote_anonymous(destination)
                .map_err(|_| unavailable())?;
            let mut callbacks = RemoteCallbacks::new();
            callbacks.credentials(|url, _, allowed| {
                if url != destination
                    || !allowed.contains(git2::CredentialType::USER_PASS_PLAINTEXT)
                {
                    return Err(git2::Error::from_str("Original HTTPS credential only"));
                }
                credential()
            });
            callbacks.push_negotiation(|updates| {
                if updates.len() != 1
                    || updates[0].dst() != git2::Oid::from_str(&self.sha)?
                    || updates[0].dst_refname()? != self.reference
                {
                    return Err(git2::Error::from_str("Original ref and object only"));
                }
                Ok(())
            });
            let mut confirmed = false;
            callbacks.push_update_reference(|reference, status| {
                if reference != self.reference {
                    return Err(git2::Error::from_str("Original reference only"));
                }
                if status.is_some() {
                    return Err(git2::Error::from_str("Remote reference refused"));
                }
                confirmed = true;
                observed(&self.sha);
                Ok(())
            });
            let mut options = PushOptions::new();
            options.remote_callbacks(callbacks);
            options.follow_redirects(RemoteRedirect::None);
            let proxy = git2::ProxyOptions::new();
            options.proxy_options(proxy);
            remote
                .push(
                    &[&format!("{}:{}", self.reference, self.reference)],
                    Some(&mut options),
                )
                .map_err(|_| unavailable())?;
            drop(options);
            if !confirmed {
                return Err(unavailable());
            }
        }
        let branch = self
            .reference
            .strip_prefix("refs/heads/")
            .ok_or_else(unavailable)?;
        let _ = repo.reference(
            &format!("refs/remotes/{}/{branch}", self.remote),
            git2::Oid::from_str(&self.sha).map_err(|_| unavailable())?,
            true,
            "native push observation",
        );
        Ok(self.sha)
    }
}
fn https(value: &str) -> bool {
    value.strip_prefix("https://").is_some_and(|rest| {
        rest.split_once('/')
            .is_some_and(|(host, path)| !host.is_empty() && !path.is_empty())
    }) && !value
        .chars()
        .any(|c| c.is_control() || c.is_whitespace() || matches!(c, '@' | '?' | '#' | '\\'))
}

#[cfg(test)]
mod tests;
