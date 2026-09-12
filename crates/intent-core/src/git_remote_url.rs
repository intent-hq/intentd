//! The one owned git remote-URL parser. Every crate that derives a
//! repository slug from a remote URL goes through [`GitRemoteUrl`] instead of
//! a hand-rolled prefix strip, so the authority-isolation rule below is
//! enforced in exactly one place.

use crate::RepoRef;

const KNOWN_SCHEMES: [&str; 4] = ["https", "http", "ssh", "git"];

/// A parsed git remote URL: the authority's host plus the repository path.
///
/// Three syntaxes are accepted — a scheme URL (`https://`, `http://`, `ssh://`,
/// `git://`, compared ASCII-case-insensitively), the scp-like
/// `[user@]host:path` form, and `file://` with an empty or `localhost`
/// authority (host `""` / `localhost`, path kept verbatim — never GitHub, but
/// [`Self::repo_slug`] still keys a cache slot from it). Bare local paths
/// (`/`, `./`, `../`, `C:\`), unknown schemes, and any input whose authority
/// cannot be isolated parse to `None`.
///
/// # Authority isolation
///
/// The authority is isolated *before* userinfo is stripped: for a scheme URL
/// it is the span between `://` and the first `/`, `?` or `#`; for the
/// scp-like form it is the span before the first `:`, which must contain no
/// `/` (git itself treats anything with a `/` before the first `:` as a local
/// path). Only then is userinfo removed with `rsplit_once('@')` on that span,
/// and only then is a numeric port stripped. Stripping up to the last `@` of
/// the whole string instead let `https://example.invalid/a@github.com/acme/widget.git`
/// and `/tmp/a@github.com:acme/widget.git` masquerade as GitHub remotes and
/// serve a foreign or local clone as the requested repository — see
/// intent-hq/intentd#1815 (review thread r3996201879). An `@` in the path can
/// therefore never promote a foreign or local source to GitHub.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitRemoteUrl {
    host: String,
    path: String,
}

impl GitRemoteUrl {
    /// Parse a remote URL. Returns `None` for bare local paths, unknown
    /// schemes, and any input without an isolable authority and a path.
    /// `file://` parses only with an empty or `localhost` authority; its path
    /// is kept as-is (no userinfo, port, query or fragment handling).
    #[must_use]
    pub fn parse(url: &str) -> Option<Self> {
        let trimmed = url.trim();
        let (authority, path) = if let Some((scheme, rest)) = trimmed.split_once("://") {
            if scheme.eq_ignore_ascii_case("file") {
                let (host, path) = rest.split_at(rest.find('/')?);
                if !(host.is_empty() || host.eq_ignore_ascii_case("localhost")) {
                    return None;
                }
                let path = path.trim_end_matches('/');
                if path.is_empty() {
                    return None;
                }
                return Some(Self {
                    host: host.to_string(),
                    path: path.to_string(),
                });
            }
            if !KNOWN_SCHEMES.iter().any(|s| scheme.eq_ignore_ascii_case(s)) {
                return None;
            }
            let end = rest.find(['/', '?', '#'])?;
            let (authority, path) = rest.split_at(end);
            if !path.starts_with('/') {
                return None;
            }
            let path = path.split(['?', '#']).next().unwrap_or(path);
            let host_port = strip_userinfo(authority)?;
            (strip_numeric_port(host_port), path)
        } else {
            // No scheme: only the scp-like `[user@]host:path` form qualifies.
            // A `/` or `\` before the first `:` marks a local path, and a
            // single-letter authority followed by a path separator is a DOS
            // drive prefix (`C:\repos\...`, `c:/repos/...`). A single-letter
            // host with a relative path (`g:acme/widget.git`) stays scp-like:
            // that is a valid SSH host alias, and git treats it as a remote.
            let (authority, path) = trimmed.split_once(':')?;
            let dos_drive = authority.len() == 1
                && authority.bytes().all(|b| b.is_ascii_alphabetic())
                && path.starts_with(['/', '\\']);
            if authority.contains(['/', '\\']) || dos_drive {
                return None;
            }
            (strip_userinfo(authority)?, path)
        };
        let path = path.trim_end_matches('/');
        if authority.is_empty() || path.is_empty() {
            return None;
        }
        Some(Self {
            host: authority.to_string(),
            path: path.to_string(),
        })
    }

    /// The authority's host with userinfo and any numeric port stripped.
    /// Casing is preserved; compare with `eq_ignore_ascii_case`.
    #[must_use]
    pub fn host(&self) -> &str {
        &self.host
    }

    /// The repository path with query, fragment and trailing `/` removed.
    /// Scheme URLs keep their leading `/`; the scp-like form yields the span
    /// after the first `:` verbatim.
    #[must_use]
    pub fn path(&self) -> &str {
        &self.path
    }

    /// The GitHub repository this remote names, if the host is exactly
    /// `github.com` or `www.github.com` (ASCII-case-insensitive) and the path
    /// has exactly two non-empty segments. The name goes through
    /// [`repo_ref_from_segments`]: a `.git` suffix is stripped and a name that
    /// is empty afterwards is rejected; owner and name keep the caller's casing.
    #[must_use]
    pub fn github_repo(&self) -> Option<RepoRef> {
        let is_github = ["github.com", "www.github.com"]
            .iter()
            .any(|h| self.host.eq_ignore_ascii_case(h));
        if !is_github {
            return None;
        }
        let mut segments = self.segments();
        let (owner, name) = (segments.next()?, segments.next()?);
        if segments.next().is_some() {
            return None;
        }
        repo_ref_from_segments(owner, name)
    }

    /// Host-agnostic `owner/name` from the last two non-empty path segments,
    /// with the same [`repo_ref_from_segments`] suffix rule as
    /// [`Self::github_repo`]. Suitable for cache-slot keys where the forge
    /// does not matter.
    #[must_use]
    pub fn repo_slug(&self) -> Option<RepoRef> {
        let mut segments: Vec<&str> = self.segments().collect();
        let name = segments.pop()?;
        let owner = segments.pop()?;
        repo_ref_from_segments(owner, name)
    }

    fn segments(&self) -> impl Iterator<Item = &str> {
        self.path.split('/').filter(|s| !s.is_empty())
    }
}

/// The one segment→[`RepoRef`] step shared by [`GitRemoteUrl::github_repo`]
/// and [`GitRemoteUrl::repo_slug`], so the two answers cannot diverge. A
/// trailing `.git` is stripped from the name ASCII-case-insensitively (`.GIT`,
/// `.Git` — GitHub reserves the suffix regardless of case, and the former ACP
/// parser already accepted it); a name that is empty afterwards
/// (`https://github.com/o/.git`) yields `None` rather than a half-formed ref.
/// The owner is never rewritten, but a bare `.git` owner is rejected by the
/// same emptiness test.
fn repo_ref_from_segments(owner: &str, name: &str) -> Option<RepoRef> {
    let name = strip_git_suffix(name);
    if strip_git_suffix(owner).is_empty() || name.is_empty() {
        return None;
    }
    Some(RepoRef::new(owner, name))
}

/// Drop `userinfo@` from an already-isolated authority. `None` when nothing
/// is left for the host.
fn strip_userinfo(authority: &str) -> Option<&str> {
    let host_port = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
    (!host_port.is_empty()).then_some(host_port)
}

/// Strip a trailing `:<digits>` port. A non-numeric or empty suffix stays part
/// of the host so it fails a strict host comparison rather than passing it.
fn strip_numeric_port(host_port: &str) -> &str {
    match host_port.rsplit_once(':') {
        Some((host, port)) if !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit()) => host,
        _ => host_port,
    }
}

/// Strip a trailing `.git` ASCII-case-insensitively. The suffix is pure ASCII,
/// so a case-insensitive byte match guarantees the cut lands on a char boundary.
fn strip_git_suffix(name: &str) -> &str {
    match name.len().checked_sub(".git".len()) {
        Some(cut) if name.is_char_boundary(cut) && name[cut..].eq_ignore_ascii_case(".git") => {
            &name[..cut]
        }
        _ => name,
    }
}

#[cfg(test)]
mod tests {
    use super::GitRemoteUrl;
    use crate::RepoRef;

    fn github(url: &str) -> Option<RepoRef> {
        GitRemoteUrl::parse(url).and_then(|u| u.github_repo())
    }

    fn slug(url: &str) -> Option<RepoRef> {
        GitRemoteUrl::parse(url).and_then(|u| u.repo_slug())
    }

    /// Golden corpus from the spec's Verification Plan: every GitHub form
    /// (https, www, scp-like, ssh with port, userinfo, query/fragment,
    /// uppercase, trailing slash) yields the same `RepoRef`.
    #[test]
    fn golden_github_positives() {
        assert_eq!(
            github("https://github.com/Acme/Widget.git"),
            Some(RepoRef::new("Acme", "Widget"))
        );
        let widget = RepoRef::new("acme", "widget");
        for url in [
            "https://www.github.com/acme/widget",
            "git@github.com:acme/widget.git",
            "ssh://git@github.com:22/acme/widget.git",
            "https://oauth2:tok@github.com/acme/widget.git",
            "https://x@github.com/acme/widget.git?ref=main#frag",
            "https://user@example.invalid@github.com/acme/widget",
            "HTTPS://GITHUB.COM/acme/widget/",
            "http://github.com/acme/widget",
            "git://github.com/acme/widget.git",
            "ssh://github.com/acme/widget.git",
            "  https://github.com/acme/widget.git  ",
            "https://github.com/acme/widget.Git",
            "https://github.com/acme/widget.GIT",
        ] {
            assert_eq!(github(url), Some(widget.clone()), "{url}");
        }
        assert_eq!(
            github("git@GitHub.com:Acme/Widget.GIT"),
            Some(RepoRef::new("Acme", "Widget"))
        );
    }

    /// A name that is empty once `.git` is stripped is not a repository:
    /// `https://github.com/o/.git` must never yield `RepoRef { o, "" }` from
    /// either accessor (the former `accept_changes` / `clone_ops` parsers rejected
    /// it, and the slug feeds persisted identity and cache-slot keys).
    #[test]
    fn empty_name_after_git_strip_is_rejected() {
        for url in [
            "https://github.com/o/.git",
            "https://github.com/o/.GIT",
            "git@github.com:o/.git",
            "ssh://git@github.com/o/.git",
            "https://github.com/.git/widget",
            "https://github.com/.git",
            "https://github.com/o/.git/",
        ] {
            assert_eq!(github(url), None, "github_repo {url}");
            assert_eq!(slug(url), None, "repo_slug {url}");
        }
        assert_eq!(slug("file:///tmp/o/.git"), None);
        assert_eq!(slug("https://gitlab.com/group/o/.git"), None);
    }

    /// Accepted deltas against the deleted per-crate parsers (intentd#1836).
    /// Each row is a URL the old `accept_changes::parse_owner_repo` /
    /// `clone_ops::parse_owner_repo` mis-derived; the shared parser returns
    /// the corrected value, and this test pins that correction so the change
    /// is explicit rather than incidental.
    #[test]
    fn accepted_deltas_against_deleted_parsers() {
        let widget = RepoRef::new("acme", "widget");
        // Old: owner `22`, name `acme/widget` (port taken as the owner).
        assert_eq!(
            github("ssh://git@github.com:22/acme/widget.git"),
            Some(widget.clone())
        );
        // Old: name `widget/extra`; a GitHub identity is exactly two segments.
        // The host-agnostic cache slug still keys the last two, as before.
        assert_eq!(github("https://github.com/acme/widget/extra"), None);
        assert_eq!(
            slug("https://github.com/acme/widget/extra"),
            Some(RepoRef::new("widget", "extra"))
        );
        // Old: name `widget.git?ref=main` (query kept, `.git` not stripped).
        assert_eq!(
            github("https://github.com/acme/widget.git?ref=main"),
            Some(widget.clone())
        );
        assert_eq!(
            slug("https://github.com/acme/widget.git?ref=main"),
            Some(widget)
        );
        // Unchanged: dotted repository names keep every dot but the suffix.
        assert_eq!(
            github("https://github.com/octo/molecules.gg.git"),
            Some(RepoRef::new("octo", "molecules.gg"))
        );
    }

    /// The two probe URLs from intent-hq/intentd#1815 review thread
    /// r3996201879 plus every other foreign-host / local-path corpus entry:
    /// an `@` in the path never promotes the source to GitHub.
    #[test]
    fn golden_github_negatives() {
        for url in [
            "https://example.invalid/a@github.com/acme/widget.git",
            "/tmp/a@github.com:acme/widget.git",
            "https://github.com.evil.example/acme/widget",
            "./a@github.com:acme/widget.git",
            "../a@github.com:acme/widget",
            "C:\\repos\\a@github.com:acme\\widget",
            "file:///tmp/a@github.com:acme/widget.git",
            "https://github.com?x=a@github.com/acme/widget",
            "https://github.com/acme",
            "https://github.com/acme/widget/extra",
            "https://gitlab.com/acme/widget.git",
            "ssh://git@github.com:evil/acme/widget.git",
            "ssh://git@github.com.evil/acme/widget.git",
            "git@github.com.evil:acme/widget.git",
            "ssh://git@github.com",
            "https://github.com",
            "https://github.com/",
            "https://github.com//",
            "git@github.com:",
            "",
        ] {
            assert_eq!(github(url), None, "{url}");
        }
    }

    /// Bare local paths, unknown schemes and inputs without an isolable
    /// authority do not parse at all.
    #[test]
    fn local_paths_and_unknown_schemes_parse_to_none() {
        for url in [
            "/tmp/a@github.com:acme/widget.git",
            "/tmp/foo/bar",
            "./a@github.com:acme/widget.git",
            "../a@github.com:acme/widget",
            "C:\\repos\\a@github.com:acme\\widget",
            "c:/repos/acme/widget",
            "C:\\",
            "file://example.invalid/tmp/acme/widget.git",
            "file://",
            "file:///",
            "file://localhost",
            "ftp://github.com/acme/widget",
            "https://github.com?x=a@github.com/acme/widget",
            "https://github.com",
            "https://github.com/",
            "https://github.com//",
            "git@github.com:",
            "acme/widget",
            ":acme/widget",
            "@:acme/widget",
            "",
        ] {
            assert_eq!(GitRemoteUrl::parse(url), None, "{url}");
        }
    }

    /// `repo_slug` is host-agnostic: the gitlab corpus entry and a nested
    /// group path both yield the last two segments.
    #[test]
    fn repo_slug_is_host_agnostic() {
        let widget = RepoRef::new("acme", "widget");
        assert_eq!(
            slug("https://gitlab.com/acme/widget.git"),
            Some(widget.clone())
        );
        assert_eq!(
            slug("https://gitlab.com/group/acme/widget.git"),
            Some(widget.clone())
        );
        assert_eq!(slug("git@gitlab.com:acme/widget.git"), Some(widget.clone()));
        assert_eq!(
            slug("ssh://git@gitlab.com:2222/acme/widget"),
            Some(widget.clone())
        );
        // A single-letter SSH host alias is a remote, not a DOS drive: the
        // drive form needs a `\` or `/` right after the colon (see the
        // `local_paths_and_unknown_schemes_parse_to_none` rows).
        assert_eq!(slug("g:acme/widget.git"), Some(widget.clone()));
        assert_eq!(github("g:acme/widget.git"), None);
        assert_eq!(slug("https://github.com/acme/widget.git"), Some(widget));
        assert_eq!(slug("https://gitlab.com/widget.git"), None);
        assert_eq!(slug("https://gitlab.com/"), None);
    }

    /// `file://` remotes parse (empty or `localhost` authority, path kept
    /// verbatim) so the host-agnostic slug can key a cache slot, while the
    /// strict GitHub identity stays `None` even when the path carries an
    /// `@github.com:` lookalike.
    #[test]
    fn file_urls_yield_repo_slug_but_never_github() {
        let widget = RepoRef::new("acme", "widget");
        for url in [
            "file:///tmp/acme/widget.git",
            "file:///tmp/acme/widget/",
            "file://localhost/tmp/acme/widget.git",
            "FILE:///tmp/acme/widget",
        ] {
            assert_eq!(slug(url), Some(widget.clone()), "{url}");
            assert_eq!(github(url), None, "{url}");
        }

        let u = GitRemoteUrl::parse("file:///tmp/a@github.com:acme/widget.git").unwrap();
        assert_eq!(u.host(), "");
        assert_eq!(u.path(), "/tmp/a@github.com:acme/widget.git");
        assert_eq!(u.github_repo(), None);
        assert_eq!(
            u.repo_slug(),
            Some(RepoRef::new("a@github.com:acme", "widget"))
        );

        let u = GitRemoteUrl::parse("file://localhost/tmp/acme/widget").unwrap();
        assert_eq!(u.host(), "localhost");
        assert_eq!(u.path(), "/tmp/acme/widget");
        assert_eq!(slug("file:///widget.git"), None);
    }

    #[test]
    fn host_and_path_strip_userinfo_port_query_and_fragment() {
        let u =
            GitRemoteUrl::parse("ssh://oauth2:tok@GitHub.com:22/acme/widget.git/?ref=x#f").unwrap();
        assert_eq!(u.host(), "GitHub.com");
        assert_eq!(u.path(), "/acme/widget.git");

        let u = GitRemoteUrl::parse("git@github.com:acme/widget.git").unwrap();
        assert_eq!(u.host(), "github.com");
        assert_eq!(u.path(), "acme/widget.git");

        let u = GitRemoteUrl::parse("ssh://git@github.com:evil/acme/widget.git").unwrap();
        assert_eq!(u.host(), "github.com:evil");

        let u = GitRemoteUrl::parse("https://user@example.invalid@github.com/acme/widget").unwrap();
        assert_eq!(u.host(), "github.com");
    }

    #[test]
    fn github_repo_preserves_casing_and_compares_under_repo_ref_identity() {
        let parsed = github("https://github.com/Acme/Widget.git").unwrap();
        assert_eq!(parsed.owner, "Acme");
        assert_eq!(parsed.name, "Widget");
        assert_eq!(parsed, RepoRef::new("acme", "widget"));
    }
}
