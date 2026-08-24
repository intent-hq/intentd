//! GitHub token resolution (§7.3).
//!
//! Tokens are resolved per `sourceControl.github.tokenSource`:
//!
//! 1. `explicit` — stored in the file-backed secrets store
//!    ([`intent_core::FileSecretStore`], `~/intent/secrets.json`) under
//!    account `sourceControl.github.token` (never in plaintext config or logs).
//! 2. `env` — `GITHUB_TOKEN` / `GH_TOKEN`.
//! 3. `gh-cli` — `gh auth token` (shell out to the GitHub CLI). The binary is
//!    located via the enriched directory list
//!    ([`intent_core::path_utils::enhanced_path_dirs`]: inherited PATH plus
//!    common install locations like `/usr/local/bin`, `/opt/homebrew/bin`,
//!    `~/.local/bin` and login-shell PATH dirs), because a daemon launched by
//!    launchd/systemd inherits a minimal PATH that typically lacks `gh`
//!    (monorepo#3321). A successful `gh` token is cached briefly
//!    ([`GH_TOKEN_CACHE_TTL`]) so bursts of `pr.*` calls do not spawn one
//!    subprocess each.
//!
//! `auto` (the default) tries the three in order and uses the first hit. A
//! missing token is *not* an error here — [`resolve`] returns `None`, and the
//! registry turns that into a graceful `NotConfigured` (§7.4).
//! [`resolve_detailed`] additionally reports *why* each attempted source
//! yielded nothing, so that `NotConfigured` error can name every source tried.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::time::timeout;

/// Secrets-store account/key for the GitHub token (`sourceControl.github.token`).
/// Shared with [`crate::device_flow`], which writes/deletes this exact entry.
pub(crate) const SECRET_ACCOUNT: &str = "sourceControl.github.token";
/// Bounded wait for a secrets-store read before treating the entry as absent.
/// A stalled backing store (e.g. a wedged filesystem) would otherwise block
/// the caller indefinitely. Mirrors the read budget used by
/// `intent-services::AsyncSecretStore`.
const SECRET_LOAD_TIMEOUT: Duration = Duration::from_secs(3);
/// Bounded wait for the `gh` CLI lookup: binary discovery (which can pay a
/// one-time cold login-shell PATH probe when the daemon did not prewarm it)
/// plus the `gh auth token` subprocess. Shelling out to `gh` can stall on
/// flaky network / OS state, so cap it so the async runtime is never blocked
/// waiting on the child.
const GH_CLI_TIMEOUT: Duration = Duration::from_secs(5);

/// How long a successful `gh auth token` result is reused before shelling out
/// again. Keeps bursts of `pr.*` calls from spawning one subprocess each,
/// while staying short enough that a `gh auth logout` (or token rotation) is
/// picked up quickly. Failures are never cached, so a fresh `gh auth login`
/// is honored on the very next call. The revoke path also invalidates this
/// cache explicitly (see [`invalidate_gh_cli_cache`]).
const GH_TOKEN_CACHE_TTL: Duration = Duration::from_secs(60);

/// Positive-only cache for the `gh` CLI token. 🔒 In-memory only — the value
/// must never reach logs, errors, or argv (no `Debug` derive on the state).
///
/// The generation counter closes the invalidate-during-lookup race: a lookup
/// snapshots the generation before spawning `gh`, and its result is stored
/// only when the generation is unchanged — an [`invalidate`](Self::invalidate)
/// that lands mid-lookup (e.g. the post-revoke `gh` logout) bumps it, so the
/// now-revoked token is discarded instead of re-populating the cache.
struct GhTokenCache {
    state: Mutex<GhTokenCacheState>,
}

#[derive(Default)]
struct GhTokenCacheState {
    generation: u64,
    entry: Option<(Instant, String)>,
}

impl GhTokenCache {
    const fn new() -> Self {
        Self {
            state: Mutex::new(GhTokenCacheState {
                generation: 0,
                entry: None,
            }),
        }
    }

    /// The cached token when it is younger than `ttl`, else `None`.
    fn fresh(&self, ttl: Duration) -> Option<String> {
        let guard = self.state.lock().ok()?;
        let (at, token) = guard.entry.as_ref()?;
        (at.elapsed() < ttl).then(|| token.clone())
    }

    /// Snapshot the generation before starting a lookup (see
    /// [`store_if_current`](Self::store_if_current)).
    fn generation(&self) -> u64 {
        self.state.lock().map_or(0, |g| g.generation)
    }

    /// Store a lookup's token unless an invalidation raced it: a no-op when
    /// `generation` no longer matches (the looked-up token may be the very
    /// one that was just revoked).
    fn store_if_current(&self, generation: u64, token: &str) {
        if let Ok(mut guard) = self.state.lock() {
            if guard.generation == generation {
                guard.entry = Some((Instant::now(), token.to_string()));
            }
        }
    }

    /// Drop the cached token and bump the generation so any in-flight lookup
    /// discards its (possibly stale) result.
    fn invalidate(&self) {
        if let Ok(mut guard) = self.state.lock() {
            guard.generation = guard.generation.wrapping_add(1);
            guard.entry = None;
        }
    }
}

/// Process-wide [`GhTokenCache`] used by [`gh_cli_token`].
static GH_TOKEN_CACHE: GhTokenCache = GhTokenCache::new();

/// Drop the cached `gh` CLI token (e.g. after the daemon logs `gh` out on
/// revoke) so the next resolution re-probes instead of serving a stale token.
/// Also discards the result of any lookup already in flight.
pub(crate) fn invalidate_gh_cli_cache() {
    GH_TOKEN_CACHE.invalidate();
}

/// Strategy used to resolve the GitHub token.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TokenSource {
    /// Try the secrets store, then env, then `gh` CLI (the default).
    #[default]
    Auto,
    /// Read from the file-backed secrets store only.
    Explicit,
    /// Read from `GITHUB_TOKEN` / `GH_TOKEN` only.
    Env,
    /// Shell out to `gh auth token` only.
    GhCli,
}

/// Outcome of a detailed resolution: the token (when any attempted source
/// produced one) plus a human-readable skip reason for every source that was
/// tried and yielded nothing, in attempt order. Reasons never carry token
/// material.
pub struct TokenResolution {
    /// The first token the attempted sources produced, if any.
    pub token: Option<String>,
    /// Why each attempted source yielded nothing (empty when `token` is
    /// `Some` and the first source hit).
    pub skipped: Vec<String>,
}

/// 🔒 Manual impl: the token is redacted so a stray `{:?}` (logs, `expect`
/// messages) can never leak credential material.
impl std::fmt::Debug for TokenResolution {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokenResolution")
            .field("token", &self.token.as_ref().map(|_| "<redacted>"))
            .field("skipped", &self.skipped)
            .finish()
    }
}

/// One source's attempt: the token, or a reason it yielded nothing.
type SourceResult = std::result::Result<String, String>;

/// Resolve a token for the given strategy, or `None` if none is available.
/// Secrets-store and `gh` subprocess reads run on the blocking pool with
/// bounded timeouts so a stalled backing store or hung child never blocks the
/// async runtime.
pub async fn resolve(source: &TokenSource) -> Option<String> {
    resolve_detailed(source).await.token
}

/// Like [`resolve`], but reports why each attempted source yielded nothing so
/// callers can surface an actionable "not configured" error (monorepo#3321).
pub async fn resolve_detailed(source: &TokenSource) -> TokenResolution {
    resolve_detailed_with(source, file_store_token, env_token, gh_cli_token).await
}

/// The resolution order over injectable source probes (test seam: the mocks
/// never touch the real secrets store or spawn a real `gh`). Sources are
/// attempted lazily — a hit short-circuits the rest.
async fn resolve_detailed_with<SFut, GFut>(
    source: &TokenSource,
    secrets: impl FnOnce() -> SFut,
    env: impl FnOnce() -> SourceResult,
    gh: impl FnOnce() -> GFut,
) -> TokenResolution
where
    SFut: Future<Output = SourceResult>,
    GFut: Future<Output = SourceResult>,
{
    let mut skipped = Vec::new();
    let attempt = |result: SourceResult, skipped: &mut Vec<String>| match result {
        Ok(token) => Some(token),
        Err(reason) => {
            skipped.push(reason);
            None
        }
    };
    let token = match source {
        TokenSource::Explicit => attempt(secrets().await, &mut skipped),
        TokenSource::Env => attempt(env(), &mut skipped),
        TokenSource::GhCli => attempt(gh().await, &mut skipped),
        TokenSource::Auto => {
            let mut token = attempt(secrets().await, &mut skipped);
            if token.is_none() {
                token = attempt(env(), &mut skipped);
            }
            if token.is_none() {
                token = attempt(gh().await, &mut skipped);
            }
            token
        }
    };
    TokenResolution { token, skipped }
}

/// Read the token from the file-backed secrets store
/// ([`intent_core::FileSecretStore`]). A missing or unreadable entry resolves
/// to a skip reason so resolution can fall through. Runs on the blocking pool
/// with a bounded timeout so a stalled backing store cannot wedge a tokio
/// worker.
async fn file_store_token() -> SourceResult {
    let handle =
        tokio::task::spawn_blocking(|| intent_core::FileSecretStore::new().load(SECRET_ACCOUNT));
    match timeout(SECRET_LOAD_TIMEOUT, handle).await {
        Ok(Ok(Ok(Some(v)))) => {
            non_empty(&v).ok_or_else(|| format!("secrets store: `{SECRET_ACCOUNT}` entry is empty"))
        }
        Ok(Ok(Ok(None))) => Err(format!("secrets store: no `{SECRET_ACCOUNT}` entry")),
        Ok(Err(_)) => Err("secrets store: lookup task failed".to_string()),
        Ok(Ok(Err(e))) => {
            tracing::warn!(
                account = %SECRET_ACCOUNT,
                error = %e,
                "secrets-store load failed for github token (corrupt/unreadable file)"
            );
            Err("secrets store: load failed (corrupt/unreadable file)".to_string())
        }
        Err(_) => {
            tracing::warn!(
                account = %SECRET_ACCOUNT,
                "secrets-store load timed out for github token"
            );
            Err("secrets store: load timed out".to_string())
        }
    }
}

/// Read `GITHUB_TOKEN`, falling back to `GH_TOKEN`.
fn env_token() -> SourceResult {
    pick_env_token(
        std::env::var("GITHUB_TOKEN").ok().as_deref(),
        std::env::var("GH_TOKEN").ok().as_deref(),
    )
    .ok_or_else(|| "env: GITHUB_TOKEN/GH_TOKEN unset or empty".to_string())
}

/// Pure selection of the env token (testable): prefer `GITHUB_TOKEN`, then
/// `GH_TOKEN`, ignoring empty values.
pub(crate) fn pick_env_token(github: Option<&str>, gh: Option<&str>) -> Option<String> {
    github
        .and_then(non_empty)
        .or_else(|| gh.and_then(non_empty))
}

/// Shell out to `gh auth token`, locating `gh` via [`find_gh_binary`] first.
/// Every failure mode yields a distinct skip reason (binary missing, spawn
/// failure, non-zero exit, empty token, timeout). Runs on the blocking pool
/// with a bounded timeout so a wedged child can't block a tokio worker. A
/// success is cached for [`GH_TOKEN_CACHE_TTL`].
async fn gh_cli_token() -> SourceResult {
    if let Some(token) = GH_TOKEN_CACHE.fresh(GH_TOKEN_CACHE_TTL) {
        return Ok(token);
    }
    let generation = GH_TOKEN_CACHE.generation();
    let handle = tokio::task::spawn_blocking(|| {
        let Some(gh) = find_gh_binary() else {
            return Err(
                "gh CLI: `gh` not found on the daemon's PATH or common install locations"
                    .to_string(),
            );
        };
        let output = std::process::Command::new(&gh)
            .args(["auth", "token"])
            .output();
        interpret_gh_output(&gh, output)
    });
    match timeout(GH_CLI_TIMEOUT, handle).await {
        Ok(Ok(Ok(v))) => {
            GH_TOKEN_CACHE.store_if_current(generation, &v);
            Ok(v)
        }
        Ok(Ok(Err(reason))) => Err(reason),
        Ok(Err(_)) => Err("gh CLI: token lookup task failed".to_string()),
        Err(_) => {
            tracing::warn!("`gh auth token` timed out");
            Err(format!(
                "gh CLI: `gh auth token` timed out after {}s",
                GH_CLI_TIMEOUT.as_secs()
            ))
        }
    }
}

/// Map a spawned `gh auth token` outcome onto a token or a skip reason
/// (testable without spawning). Reasons carry at most the first non-empty
/// stderr line, truncated — `gh auth token` failure output is a plain
/// diagnostic (e.g. "not logged in to any hosts"), never token material.
fn interpret_gh_output(gh: &Path, output: std::io::Result<std::process::Output>) -> SourceResult {
    let gh = gh.display();
    match output {
        Err(e) => Err(format!("gh CLI ({gh}): failed to run: {e}")),
        Ok(out) if !out.status.success() => {
            let stderr = String::from_utf8_lossy(&out.stderr);
            let detail = stderr
                .lines()
                .map(str::trim)
                .find(|l| !l.is_empty())
                .unwrap_or_default()
                .chars()
                .take(200)
                .collect::<String>();
            if detail.is_empty() {
                Err(format!(
                    "gh CLI ({gh}): `gh auth token` failed ({})",
                    out.status
                ))
            } else {
                Err(format!(
                    "gh CLI ({gh}): `gh auth token` failed ({}): {detail}",
                    out.status
                ))
            }
        }
        Ok(out) => {
            let token = String::from_utf8_lossy(&out.stdout).trim().to_string();
            if token.is_empty() {
                Err(format!(
                    "gh CLI ({gh}): `gh auth token` printed an empty token"
                ))
            } else {
                Ok(token)
            }
        }
    }
}

/// Locate the `gh` binary. A daemon launched by launchd/systemd inherits a
/// minimal PATH that often lacks `gh` even though interactive shells find it
/// (monorepo#3321). Two phases, cheapest first:
///
/// 1. Inherited PATH plus well-known install directories ([`fast_gh_dirs`]) —
///    never blocks, and covers virtually every real install.
/// 2. The full [`intent_core::path_utils::enhanced_path_dirs`] list — this
///    can pay the one-time cold login-shell PATH capture (up to ~5s per shell
///    attempt when the startup prewarm has not finished), so it only runs
///    when phase 1 misses; a cold daemon still resolves `/usr/bin`-style
///    installs instantly, inside [`GH_CLI_TIMEOUT`].
///
/// Shared with [`crate::gh_sync`] so every `gh` lookup in this crate agrees.
pub(crate) fn find_gh_binary() -> Option<PathBuf> {
    let is_windows = cfg!(windows);
    find_gh_in_dirs_for(&fast_gh_dirs(), is_windows)
        .or_else(|| find_gh_in_dirs_for(&intent_core::path_utils::enhanced_path_dirs(), is_windows))
}

/// Phase-1 directory list: inherited PATH entries, then the well-known unix
/// install locations from monorepo#3321 (`/usr/local/bin`, `/opt/homebrew/bin`,
/// `/usr/bin`, `~/.local/bin`), deduplicated. Pure env/filesystem — never
/// spawns the login-shell probe.
fn fast_gh_dirs() -> Vec<PathBuf> {
    use intent_core::path_utils::push_dir;
    use std::collections::HashSet;
    let mut dirs: Vec<PathBuf> = Vec::new();
    let mut seen: HashSet<PathBuf> = HashSet::new();
    if let Some(path) = std::env::var_os("PATH") {
        for dir in std::env::split_paths(&path) {
            push_dir(&mut dirs, &mut seen, dir);
        }
    }
    if !cfg!(windows) {
        for dir in ["/usr/local/bin", "/opt/homebrew/bin", "/usr/bin"] {
            push_dir(&mut dirs, &mut seen, PathBuf::from(dir));
        }
        if let Some(home) = std::env::var_os("HOME").filter(|h| !h.is_empty()) {
            push_dir(&mut dirs, &mut seen, PathBuf::from(home).join(".local/bin"));
        }
    }
    dirs
}

/// [`find_gh_binary`] parametrized on the directory list and platform (test
/// seam — Windows CI is disabled, so the Windows arm is unit-tested on POSIX).
fn find_gh_in_dirs_for(dirs: &[PathBuf], is_windows: bool) -> Option<PathBuf> {
    use intent_core::path_utils::{is_executable_file_for, WINDOWS_EXEC_EXTENSIONS};
    for dir in dirs {
        if is_windows {
            for ext in WINDOWS_EXEC_EXTENSIONS {
                let candidate = dir.join(format!("gh.{ext}"));
                if is_executable_file_for(&candidate, true) {
                    return Some(candidate);
                }
            }
        } else {
            let candidate = dir.join("gh");
            if is_executable_file_for(&candidate, false) {
                return Some(candidate);
            }
        }
    }
    None
}

/// `Some(s)` only when `s` is non-empty after trimming.
fn non_empty(s: &str) -> Option<String> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefers_github_token_over_gh_token() {
        let picked = pick_env_token(Some("gho_primary"), Some("gho_fallback"));
        assert_eq!(picked.as_deref(), Some("gho_primary"));
    }

    #[test]
    fn falls_back_to_gh_token() {
        let picked = pick_env_token(None, Some("gho_fallback"));
        assert_eq!(picked.as_deref(), Some("gho_fallback"));
    }

    #[test]
    fn ignores_empty_values() {
        assert_eq!(pick_env_token(Some("   "), Some("")), None);
        assert_eq!(pick_env_token(None, None), None);
    }

    #[test]
    fn token_source_deserializes_kebab_case() {
        let s: TokenSource = serde_json::from_str("\"gh-cli\"").unwrap();
        assert_eq!(s, TokenSource::GhCli);
        assert_eq!(TokenSource::default(), TokenSource::Auto);
    }

    /// Mock probes for [`resolve_detailed_with`] — never touch the real
    /// secrets store, env, or spawn `gh`.
    async fn secrets_hit() -> SourceResult {
        Ok("tok_secrets".to_string())
    }
    async fn secrets_miss() -> SourceResult {
        Err("secrets store: no entry".to_string())
    }
    fn env_miss() -> SourceResult {
        Err("env: unset".to_string())
    }
    async fn gh_hit() -> SourceResult {
        Ok("tok_gh".to_string())
    }
    async fn gh_miss() -> SourceResult {
        Err("gh CLI: not found".to_string())
    }

    #[tokio::test]
    async fn auto_falls_through_to_gh_cli() {
        let res = resolve_detailed_with(&TokenSource::Auto, secrets_miss, env_miss, gh_hit).await;
        assert_eq!(res.token.as_deref(), Some("tok_gh"));
        assert_eq!(res.skipped, vec!["secrets store: no entry", "env: unset"]);
    }

    #[tokio::test]
    async fn auto_short_circuits_on_first_hit() {
        let res = resolve_detailed_with(&TokenSource::Auto, secrets_hit, env_miss, gh_hit).await;
        assert_eq!(res.token.as_deref(), Some("tok_secrets"));
        assert!(res.skipped.is_empty());
    }

    #[tokio::test]
    async fn auto_reports_every_source_when_all_miss() {
        let res = resolve_detailed_with(&TokenSource::Auto, secrets_miss, env_miss, gh_miss).await;
        assert_eq!(res.token, None);
        assert_eq!(
            res.skipped,
            vec!["secrets store: no entry", "env: unset", "gh CLI: not found"]
        );
    }

    #[tokio::test]
    async fn single_source_reports_only_its_own_miss() {
        let res = resolve_detailed_with(&TokenSource::GhCli, secrets_hit, env_miss, gh_miss).await;
        assert_eq!(res.token, None);
        assert_eq!(res.skipped, vec!["gh CLI: not found"]);
    }

    fn output(status_code: i32, stdout: &str, stderr: &str) -> std::process::Output {
        #[cfg(unix)]
        use std::os::unix::process::ExitStatusExt;
        #[cfg(windows)]
        use std::os::windows::process::ExitStatusExt;
        use std::process::ExitStatus;
        std::process::Output {
            #[cfg(unix)]
            status: ExitStatus::from_raw(status_code << 8),
            #[cfg(windows)]
            status: ExitStatus::from_raw(status_code as u32),
            stdout: stdout.as_bytes().to_vec(),
            stderr: stderr.as_bytes().to_vec(),
        }
    }

    #[test]
    fn gh_output_success_yields_trimmed_token() {
        let res = interpret_gh_output(Path::new("/usr/bin/gh"), Ok(output(0, "gho_x\n", "")));
        assert_eq!(res, Ok("gho_x".to_string()));
    }

    #[test]
    fn gh_output_failure_carries_first_stderr_line() {
        let res = interpret_gh_output(
            Path::new("/usr/bin/gh"),
            Ok(output(1, "", "\nno oauth token found\ndetail\n")),
        );
        let reason = res.unwrap_err();
        assert!(reason.contains("/usr/bin/gh"), "{reason}");
        assert!(reason.contains("no oauth token found"), "{reason}");
        assert!(!reason.contains("detail"), "{reason}");
    }

    #[test]
    fn gh_output_empty_token_is_a_miss() {
        let res = interpret_gh_output(Path::new("/usr/bin/gh"), Ok(output(0, "  \n", "")));
        assert!(res.unwrap_err().contains("empty token"));
    }

    #[test]
    fn gh_output_spawn_error_is_a_miss() {
        let err = std::io::Error::new(std::io::ErrorKind::NotFound, "enoent");
        let res = interpret_gh_output(Path::new("/usr/bin/gh"), Err(err));
        assert!(res.unwrap_err().contains("failed to run"));
    }

    #[cfg(unix)]
    #[test]
    fn find_gh_scans_dirs_in_order() {
        use std::os::unix::fs::PermissionsExt;
        let empty = tempfile::tempdir().unwrap();
        let hit = tempfile::tempdir().unwrap();
        let gh = hit.path().join("gh");
        std::fs::write(&gh, "#!/bin/sh\n").unwrap();
        std::fs::set_permissions(&gh, std::fs::Permissions::from_mode(0o755)).unwrap();
        let dirs = vec![empty.path().to_path_buf(), hit.path().to_path_buf()];
        assert_eq!(find_gh_in_dirs_for(&dirs, false), Some(gh));
    }

    #[cfg(unix)]
    #[test]
    fn find_gh_skips_non_executable_files() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let gh = dir.path().join("gh");
        std::fs::write(&gh, "").unwrap();
        std::fs::set_permissions(&gh, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert_eq!(
            find_gh_in_dirs_for(&[dir.path().to_path_buf()], false),
            None
        );
    }

    #[cfg(unix)]
    #[test]
    fn find_gh_windows_arm_requires_runnable_extension() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let bare = dir.path().join("gh");
        std::fs::write(&bare, "").unwrap();
        std::fs::set_permissions(&bare, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert_eq!(find_gh_in_dirs_for(&[dir.path().to_path_buf()], true), None);
        let exe = dir.path().join("gh.exe");
        std::fs::write(&exe, "").unwrap();
        assert_eq!(
            find_gh_in_dirs_for(&[dir.path().to_path_buf()], true),
            Some(exe)
        );
    }

    #[test]
    fn cache_serves_fresh_token_within_ttl() {
        let cache = GhTokenCache::new();
        assert_eq!(cache.fresh(GH_TOKEN_CACHE_TTL), None);
        let generation = cache.generation();
        cache.store_if_current(generation, "gho_cached");
        assert_eq!(
            cache.fresh(GH_TOKEN_CACHE_TTL).as_deref(),
            Some("gho_cached")
        );
    }

    #[test]
    fn cache_expires_after_ttl() {
        let cache = GhTokenCache::new();
        cache.store_if_current(cache.generation(), "gho_cached");
        assert_eq!(cache.fresh(Duration::ZERO), None);
    }

    #[test]
    fn cache_invalidate_drops_entry() {
        let cache = GhTokenCache::new();
        cache.store_if_current(cache.generation(), "gho_cached");
        cache.invalidate();
        assert_eq!(cache.fresh(GH_TOKEN_CACHE_TTL), None);
    }

    #[test]
    fn cache_rejects_store_from_a_lookup_that_raced_an_invalidation() {
        let cache = GhTokenCache::new();
        let generation = cache.generation();
        cache.invalidate();
        cache.store_if_current(generation, "gho_revoked");
        assert_eq!(cache.fresh(GH_TOKEN_CACHE_TTL), None);
        cache.store_if_current(cache.generation(), "gho_fresh");
        assert_eq!(
            cache.fresh(GH_TOKEN_CACHE_TTL).as_deref(),
            Some("gho_fresh")
        );
    }

    #[test]
    fn token_resolution_debug_redacts_the_token() {
        let res = TokenResolution {
            token: Some("gho_secret_material".to_string()),
            skipped: vec!["env: unset".to_string()],
        };
        let debug = format!("{res:?}");
        assert!(!debug.contains("gho_secret_material"), "{debug}");
        assert!(debug.contains("<redacted>"), "{debug}");
        assert!(debug.contains("env: unset"), "{debug}");
    }
}
