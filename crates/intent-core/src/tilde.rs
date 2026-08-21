//! Home-directory tilde expansion for caller-supplied paths.
//!
//! IPC clients send paths like `~/Developer/repo` (the FE onboarding default),
//! but neither Rust nor git expands `~` — git treats it as the literal
//! relative directory `./~`, which fails on the packaged sidecar's read-only
//! cwd (intent-hq/monorepo#822). Path-typed params are expanded at the daemon
//! boundary with these helpers: a leading `~` / `~/` resolves to the user's
//! home directory; `~user` forms and non-tilde paths pass through unchanged.
//!
//! Expansion is `/`-separated only: a Windows-style `~\` prefix is not
//! recognized and passes through verbatim (the daemon's supported targets are
//! unix-first). When no home directory can be resolved from the environment,
//! inputs also pass through unchanged.

use std::path::{Path, PathBuf};

/// Expand a leading `~` / `~/` in `input` against `home`. Anything else —
/// including `~user` forms — passes through verbatim.
#[must_use]
pub fn expand_tilde_with(input: &str, home: &Path) -> PathBuf {
    if input == "~" {
        return home.to_path_buf();
    }
    if let Some(rest) = input.strip_prefix("~/") {
        // Trim any extra leading separators (`~//repo`): `Path::join` with an
        // absolute right-hand side would replace `home` entirely, silently
        // escaping it.
        let rest = rest.trim_start_matches('/');
        if rest.is_empty() {
            return home.to_path_buf();
        }
        return home.join(rest);
    }
    PathBuf::from(input)
}

/// Expand a leading `~` / `~/` against the process environment's home
/// directory (`$HOME`, with `USERPROFILE` as the Windows fallback). When no
/// home can be resolved the input passes through unchanged.
#[must_use]
pub fn expand_tilde(input: &str) -> PathBuf {
    match env_home_dir() {
        Some(home) => expand_tilde_with(input, &home),
        None => PathBuf::from(input),
    }
}

/// String-typed variant of [`expand_tilde`] for callers that persist the path
/// as UTF-8 text. When the expanded path is not valid UTF-8 (a non-UTF-8
/// `$HOME`), the input passes through unchanged rather than being lossily
/// rewritten to a path that does not exist on disk.
#[must_use]
pub fn expand_tilde_string(input: &str) -> String {
    match expand_tilde(input).to_str() {
        Some(expanded) => expanded.to_owned(),
        None => input.to_owned(),
    }
}

/// Resolve the home directory from the environment. `None` when unset so
/// callers degrade to passthrough instead of guessing.
fn env_home_dir() -> Option<PathBuf> {
    if let Some(home) = std::env::var_os("HOME") {
        if !home.is_empty() {
            return Some(PathBuf::from(home));
        }
    }
    #[cfg(windows)]
    if let Some(profile) = std::env::var_os("USERPROFILE") {
        if !profile.is_empty() {
            return Some(PathBuf::from(profile));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bare_tilde_expands_to_home() {
        assert_eq!(
            expand_tilde_with("~", Path::new("/home/u")),
            PathBuf::from("/home/u")
        );
    }

    #[test]
    fn tilde_slash_prefix_expands_under_home() {
        assert_eq!(
            expand_tilde_with("~/Developer/repo", Path::new("/home/u")),
            PathBuf::from("/home/u/Developer/repo")
        );
    }

    #[test]
    fn tilde_user_form_passes_through() {
        assert_eq!(
            expand_tilde_with("~alice/repo", Path::new("/home/u")),
            PathBuf::from("~alice/repo")
        );
    }

    #[test]
    fn non_tilde_paths_pass_through() {
        assert_eq!(
            expand_tilde_with("/abs/path", Path::new("/home/u")),
            PathBuf::from("/abs/path")
        );
        assert_eq!(
            expand_tilde_with("rel/path", Path::new("/home/u")),
            PathBuf::from("rel/path")
        );
        assert_eq!(expand_tilde_with("", Path::new("/home/u")), PathBuf::new());
    }

    #[test]
    fn interior_tilde_passes_through() {
        assert_eq!(
            expand_tilde_with("/data/~backup", Path::new("/home/u")),
            PathBuf::from("/data/~backup")
        );
    }

    #[test]
    fn extra_leading_separators_stay_under_home() {
        // `~//repo` must not escape `home`: a bare `Path::join("/repo")`
        // would discard the left-hand side.
        assert_eq!(
            expand_tilde_with("~//repo", Path::new("/home/u")),
            PathBuf::from("/home/u/repo")
        );
        assert_eq!(
            expand_tilde_with("~//", Path::new("/home/u")),
            PathBuf::from("/home/u")
        );
    }

    #[test]
    fn env_variant_expands_against_process_home() {
        let Some(home) = std::env::var_os("HOME").map(PathBuf::from) else {
            eprintln!("skipping env tilde test: HOME not set");
            return;
        };
        assert_eq!(expand_tilde("~/x"), home.join("x"));
        assert_eq!(expand_tilde("/abs/x"), PathBuf::from("/abs/x"));
    }

    #[test]
    fn string_variant_expands_and_preserves_non_tilde() {
        let Some(home) = std::env::var_os("HOME").map(PathBuf::from) else {
            eprintln!("skipping string tilde test: HOME not set");
            return;
        };
        assert_eq!(expand_tilde_string("~/x"), home.join("x").to_str().unwrap());
        assert_eq!(expand_tilde_string("/abs/x"), "/abs/x");
    }
}
