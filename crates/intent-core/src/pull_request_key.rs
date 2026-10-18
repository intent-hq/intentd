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

#[cfg(test)]
mod tests {
    use super::pull_request_key;

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
}
