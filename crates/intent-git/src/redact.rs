//! Credential redaction for git stderr / URL-bearing error text.
//!
//! Shelled-out git failures echo the remote URL (`fatal: unable to access
//! 'https://user:token@github.com/...'`), so any surface that logs, streams,
//! or returns such text must mask the userinfo first. Shared by the service
//! layer's clone pipeline and the repo-cache module's own logging.

/// Redact a `user[:pass]@` credential fragment from any URL-like substring in
/// `text`. Best-effort; used for terminal `error` payloads and log lines.
///
/// Two passes: an authority pass anchored on `://`, then a scheme-less pass
/// masking bare `user[:pass]@host` fragments — a front-truncated stderr tail
/// or an scp-like remote carries no `://` anchor to find (monorepo#836).
#[must_use]
pub fn redact_credentials(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(scheme_end) = rest.find("://") {
        out.push_str(&rest[..scheme_end + 3]);
        rest = &rest[scheme_end + 3..];
        let end_authority = rest.find(['/', ' ', '\t', '\n']).unwrap_or(rest.len());
        let authority = &rest[..end_authority];
        if let Some(at) = authority.rfind('@') {
            out.push_str("***@");
            out.push_str(&authority[at + 1..]);
        } else {
            out.push_str(authority);
        }
        rest = &rest[end_authority..];
    }
    out.push_str(rest);
    redact_bare_userinfo(&out)
}

/// Scheme-less pass of [`redact_credentials`]: mask the userinfo of any bare
/// `user[:pass]@host` fragment. Deliberately over-eager — it also masks the
/// `git@` of scp-like remotes — because a mangled username in an error
/// message is harmless while a leaked password or token is not, and tokens
/// often travel as the username with no `:pass` (e.g. `ghp_…@github.com`).
///
/// Known best-effort limitation: `'`/`"` sit in the delimiter set to bound
/// quoted contexts, yet RFC 3986 permits them (sub-delims) unencoded in
/// userinfo — a bare `user:pa'ss@host` fragment therefore masks only from
/// the quote onward. The `://`-anchored first pass fully handles the quoted
/// URLs git actually emits, and the line-boundary tail trim makes bare
/// fragments rare, so this trade-off is deliberate.
fn redact_bare_userinfo(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(at) = rest.find('@') {
        let before = &rest[..at];
        // The userinfo starts after the last delimiter before the `@`.
        let start = before
            .rfind([' ', '\t', '\r', '\n', '/', '\'', '"'])
            .map_or(0, |i| i + 1);
        out.push_str(&before[..start]);
        if start < at {
            out.push_str("***");
        }
        out.push('@');
        rest = &rest[at + 1..];
    }
    out.push_str(rest);
    out
}
