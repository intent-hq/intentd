//! Canonical pull-request key shared by PR <-> workspace link recording and
//! `workspace.findByPullRequest` (PROTOCOL §5.1 "Pull request workspace
//! lookup").
//!
//! Links are recorded in SQL (migration `0161_pull_request_workspace.sql`)
//! from forge-supplied `html_url`s, which are already canonical
//! `https://<host>/<owner>/<repo>/pull/<number>`; the triggers lowercase them,
//! drop trailing slashes and a leading `www.` host prefix, and keep only that
//! exact shape. [`pull_request_key`] maps any user-facing PR URL (a clicked
//! link, possibly with a `/files` tab, a query or a fragment) onto the same
//! key so the lookup and the recorder always agree.

/// The canonical key for a GitHub-form pull-request URL, or `None` when
/// `url` is not one.
///
/// Accepts `http` / `https`; lowercases the host (port kept), strips a
/// leading `www.`, lowercases owner and repository, and ignores anything
/// after the number (extra path segments, query, fragment). The number must
/// be a positive decimal integer and is rendered without leading zeros.
#[must_use]
pub fn pull_request_key(url: &str) -> Option<String> {
    let url = url.trim();
    let lower = url.to_ascii_lowercase();
    let (scheme, rest) = if let Some(rest) = lower.strip_prefix("https://") {
        ("https", rest)
    } else if let Some(rest) = lower.strip_prefix("http://") {
        ("http", rest)
    } else {
        return None;
    };
    let rest = rest.split(['?', '#']).next().unwrap_or_default();
    let mut segments = rest.split('/');
    let host = segments
        .next()
        .filter(|h| !h.is_empty() && !h.contains('@'))?;
    let host = host.strip_prefix("www.").unwrap_or(host);
    if host.is_empty() {
        return None;
    }
    let owner = segments.next().filter(|s| !s.is_empty())?;
    let repo = segments.next().filter(|s| !s.is_empty())?;
    if segments.next()? != "pull" {
        return None;
    }
    let number = segments.next()?;
    if number.is_empty() || !number.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let number: u64 = number.parse().ok().filter(|n| *n > 0)?;
    Some(format!("{scheme}://{host}/{owner}/{repo}/pull/{number}"))
}

#[cfg(test)]
mod tests {
    use super::pull_request_key;

    #[test]
    fn canonical_github_url_is_its_own_key() {
        assert_eq!(
            pull_request_key("https://github.com/intent-hq/intentd/pull/2100").as_deref(),
            Some("https://github.com/intent-hq/intentd/pull/2100")
        );
    }

    #[test]
    fn case_www_and_trailing_parts_normalize() {
        let want = Some("https://github.com/intent-hq/intentd/pull/7".to_string());
        for url in [
            "https://GitHub.com/Intent-HQ/IntentD/pull/7",
            "https://www.github.com/intent-hq/intentd/pull/7",
            "  https://github.com/intent-hq/intentd/pull/7/  ",
            "https://github.com/intent-hq/intentd/pull/7/files",
            "https://github.com/intent-hq/intentd/pull/7/commits/abc123",
            "https://github.com/intent-hq/intentd/pull/7?w=1",
            "https://github.com/intent-hq/intentd/pull/7#discussion_r1",
            "https://github.com/intent-hq/intentd/pull/007",
        ] {
            assert_eq!(pull_request_key(url), want, "{url}");
        }
    }

    #[test]
    fn enterprise_host_and_port_are_kept() {
        assert_eq!(
            pull_request_key("http://GHE.example.com:8443/Org/Repo/pull/3").as_deref(),
            Some("http://ghe.example.com:8443/org/repo/pull/3")
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
            "https://github.com/o/r/pull/1a",
            "https://github.com/o/r/pulls",
            "https://github.com/o/pull/1",
            "https://github.com//r/pull/1",
            "https://user@github.com/o/r/pull/1",
            "https://gitlab.com/g/p/-/merge_requests/1",
            "https://github.com/o/r/pull/99999999999999999999999",
        ] {
            assert_eq!(pull_request_key(url), None, "{url}");
        }
    }
}
