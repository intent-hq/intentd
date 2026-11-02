//! Canonical pull-request key shared by PR <-> workspace link recording and
//! `workspace.findByPullRequest` (PROTOCOL §5.1 "Pull request workspace
//! lookup").
//!
//! Links are recorded in SQL (migration `0161_pull_request_workspace.sql`,
//! trigger `pull_request_workspace_candidate_record`) from the persisted PR
//! URLs. Both sides produce `<host>/<owner>/<repo>/pull/<number>`: scheme
//! dropped (so `http` and `https` links match), lowercased, trailing slashes
//! and a leading `www.` host prefix removed, port kept. [`pull_request_key`]
//! additionally ignores anything after the number (a `/files` tab, a query,
//! a fragment) because it handles clicked links; stored forge URLs never carry
//! those. The intent-store parity test asserts that every key the trigger
//! records is the key this function produces for the same URL.

/// The canonical key for a GitHub-form pull-request URL, or `None` when
/// `url` is not one.
///
/// Accepts `http` / `https`. Host, owner and repository must be non-empty and
/// free of `@`; the number must be decimal digits without a leading zero.
#[must_use]
pub fn pull_request_key(url: &str) -> Option<String> {
    let lower = url.trim().to_ascii_lowercase();
    let rest = lower
        .strip_prefix("https://")
        .or_else(|| lower.strip_prefix("http://"))?;
    let rest = rest.split(['?', '#']).next().unwrap_or_default();
    let mut segments = rest.split('/');
    let host = segments.next()?;
    let host = host.strip_prefix("www.").unwrap_or(host);
    let owner = segments.next()?;
    let repo = segments.next()?;
    if [host, owner, repo]
        .iter()
        .any(|s| s.is_empty() || s.contains('@'))
    {
        return None;
    }
    if segments.next()? != "pull" {
        return None;
    }
    let number = segments.next()?;
    if number.is_empty() || number.starts_with('0') || !number.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    Some(format!("{host}/{owner}/{repo}/pull/{number}"))
}

/// A pull-request reference an agent typed, resolved against canonical
/// [`pull_request_key`] keys (`ws.crossWorkspace.search({ pr })`).
///
/// - a pull-request URL matches exactly its canonical key;
/// - `owner/repo#N` matches `N` in that repository on any host;
/// - `N` or `#N` matches `N` in any repository.
///
/// Matching is anchored on `/` boundaries, so `#12` never matches `#112` and
/// `o/r#1` never matches `xo/r#1`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PullRequestSelector {
    /// An exact canonical key, from a pull-request URL.
    Key(String),
    /// A `/`-anchored key suffix: `/<owner>/<repo>/pull/<n>` or `/pull/<n>`.
    Suffix(String),
}

impl PullRequestSelector {
    /// Parse an agent-supplied reference; `None` when it is none of the
    /// accepted forms (see the type docs).
    #[must_use]
    pub fn parse(input: &str) -> Option<Self> {
        let trimmed = input.trim();
        let lower = trimmed.to_ascii_lowercase();
        if lower.starts_with("https://") || lower.starts_with("http://") {
            return pull_request_key(trimmed).map(Self::Key);
        }
        let (repo, number) = match lower.rsplit_once('#') {
            Some((repo, number)) => (repo.trim(), number.trim()),
            None => ("", lower.as_str()),
        };
        if number.is_empty()
            || number.starts_with('0')
            || !number.bytes().all(|b| b.is_ascii_digit())
        {
            return None;
        }
        if repo.is_empty() {
            return Some(Self::Suffix(format!("/pull/{number}")));
        }
        let (owner, name) = repo.split_once('/')?;
        if [owner, name]
            .iter()
            .any(|s| s.is_empty() || s.contains(['/', '@']) || s.contains(char::is_whitespace))
        {
            return None;
        }
        Some(Self::Suffix(format!("/{owner}/{name}/pull/{number}")))
    }

    /// Whether the canonical `key` (as stored in the PR link index) is a
    /// pull request this selector names.
    #[must_use]
    pub fn matches(&self, key: &str) -> bool {
        match self {
            Self::Key(want) => key == want,
            Self::Suffix(suffix) => key.ends_with(suffix.as_str()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{pull_request_key, PullRequestSelector};

    #[test]
    fn canonical_github_url_maps_to_a_scheme_free_key() {
        assert_eq!(
            pull_request_key("https://github.com/intent-hq/intentd/pull/2100").as_deref(),
            Some("github.com/intent-hq/intentd/pull/2100")
        );
    }

    #[test]
    fn scheme_case_www_and_trailing_parts_normalize() {
        let want = Some("github.com/intent-hq/intentd/pull/7".to_string());
        for url in [
            "http://github.com/intent-hq/intentd/pull/7",
            "https://GitHub.com/Intent-HQ/IntentD/pull/7",
            "https://www.github.com/intent-hq/intentd/pull/7",
            "  https://github.com/intent-hq/intentd/pull/7/  ",
            "https://github.com/intent-hq/intentd/pull/7/files",
            "https://github.com/intent-hq/intentd/pull/7/commits/abc123",
            "https://github.com/intent-hq/intentd/pull/7?w=1",
            "https://github.com/intent-hq/intentd/pull/7#discussion_r1",
        ] {
            assert_eq!(pull_request_key(url), want, "{url}");
        }
    }

    #[test]
    fn enterprise_host_and_port_are_kept() {
        assert_eq!(
            pull_request_key("http://GHE.example.com:8443/Org/Repo/pull/3").as_deref(),
            Some("ghe.example.com:8443/org/repo/pull/3")
        );
    }

    #[test]
    fn non_pull_request_urls_are_rejected() {
        for url in [
            "",
            "github.com/o/r/pull/1",
            "ftp://github.com/o/r/pull/1",
            "https://github.com/o/r/issues/1",
            "https://github.com/o/r/pull/",
            "https://github.com/o/r/pull/0",
            "https://github.com/o/r/pull/007",
            "https://github.com/o/r/pull/1a",
            "https://github.com/o/r/pulls",
            "https://github.com/o/pull/1",
            "https://github.com//r/pull/1",
            "https:///o/r/pull/1",
            "https://www./o/r/pull/1",
            "https://user@github.com/o/r/pull/1",
            "https://github.com/o@x/r/pull/1",
            "https://gitlab.com/g/p/-/merge_requests/1",
        ] {
            assert_eq!(pull_request_key(url), None, "{url}");
        }
    }

    #[test]
    fn large_numbers_are_kept_verbatim() {
        assert_eq!(
            pull_request_key("https://github.com/o/r/pull/99999999999999999999999").as_deref(),
            Some("github.com/o/r/pull/99999999999999999999999")
        );
    }

    #[test]
    fn selector_parses_urls_numbers_and_owner_repo_refs() {
        assert_eq!(
            PullRequestSelector::parse("https://github.com/Intent-HQ/IntentD/pull/7/files"),
            Some(PullRequestSelector::Key(
                "github.com/intent-hq/intentd/pull/7".to_string()
            ))
        );
        for input in ["7", "#7", " #7 "] {
            assert_eq!(
                PullRequestSelector::parse(input),
                Some(PullRequestSelector::Suffix("/pull/7".to_string())),
                "{input}"
            );
        }
        assert_eq!(
            PullRequestSelector::parse("Intent-HQ/intentd#2100"),
            Some(PullRequestSelector::Suffix(
                "/intent-hq/intentd/pull/2100".to_string()
            ))
        );
        for bad in [
            "",
            "#",
            "0",
            "#012",
            "7a",
            "intentd#7",
            "a/b/c#7",
            "/r#7",
            "o/#7",
            "o@x/r#7",
            "https://github.com/o/r/issues/7",
            "github.com/o/r/pull/7",
        ] {
            assert_eq!(PullRequestSelector::parse(bad), None, "{bad}");
        }
    }

    #[test]
    fn selector_matches_on_path_boundaries_only() {
        let key = "github.com/intent-hq/intentd/pull/112";
        let parse = |s: &str| PullRequestSelector::parse(s).expect(s);
        assert!(parse("112").matches(key));
        assert!(parse("intent-hq/intentd#112").matches(key));
        assert!(parse("https://github.com/intent-hq/intentd/pull/112").matches(key));
        assert!(!parse("12").matches(key));
        assert!(!parse("hq/intentd#112").matches(key));
        assert!(!parse("intent-hq/intent#112").matches(key));
        assert!(!parse("https://github.com/intent-hq/intent/pull/112").matches(key));
    }
}
