//! Provider discovery: which configured providers are *installed* (resolvable on
//! `PATH`) and which are gated off by a missing env var / feature code (§6.9).
//!
//! Pure detection only — no process spawning (that would pull a runtime into a
//! leaf crate, §3.2). It ports the "is this provider available?" intent of
//! `provider-availability.service.ts` to a `PATH` probe; the optional
//! authentication probe (which must spawn `auth status`) lives in the daemon
//! layer that already owns a tokio runtime.

use std::path::PathBuf;

use intent_core::path_utils::{
    has_windows_exec_extension, is_executable_file, is_executable_file_for, WINDOWS_EXEC_EXTENSIONS,
};

use crate::config::{ProviderConfig, ACP_PROVIDERS};

/// Availability of one configured provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderAvailability {
    /// Provider id (`auggie`, `codex`, …).
    pub id: &'static str,
    /// Human-readable provider name.
    pub display_name: &'static str,
    /// The CLI command probed on `PATH`.
    pub command: &'static str,
    /// Whether the provider would actually spawn: the command resolved to an
    /// executable (auto-detection) OR a valid `providers.paths` override
    /// targets it (when the caller supplied overrides via
    /// [`discover_providers_with_overrides`]). Override-aware so it matches
    /// `resolve_spawn` / the managed-server lifecycle (monorepo#1065).
    pub installed: bool,
    /// The auto-detected executable path, when found. Always override-free —
    /// a `providers.paths` override never appears here, so `installed` can be
    /// `true` while this is `None` (valid override, nothing auto-detected).
    pub resolved_path: Option<PathBuf>,
    /// `Some(reason)` when the provider is gated off (env var / feature code not
    /// present), in which case it is skipped rather than probed.
    pub gated_off: Option<String>,
    /// The provider's auth-status check args (`Some` ⇒ a daemon-side probe is
    /// possible), surfaced so the caller can run it without re-reading config.
    pub auth_check_args: Option<&'static [&'static str]>,
    /// Whether this provider supports npx fallback when binary is unresolved.
    pub has_npx_fallback: bool,
    /// When set, the provider is spawned exclusively via `npx -y <package>`
    /// (pinned spec); `installed`/`resolved_path` then reflect npx itself
    /// rather than a local provider binary.
    pub npx_only_package: Option<&'static str>,
    /// For providers with [`ProviderConfig::requires_secondary_binary`]
    /// (unsloth: `opencode` + `unsloth`): the required secondary's status, so
    /// callers (doctor, the discovery wire payload) can attribute
    /// unavailability to the actually-missing binary and surface where the
    /// secondary lives. `None` when the provider has no secondary requirement
    /// or was gated off (never probed).
    pub secondary_binary: Option<SecondaryBinary>,
}

/// Status of a provider's required secondary binary
/// ([`ProviderConfig::requires_secondary_binary`]; unsloth: the `unsloth` CLI
/// the managed-server lifecycle shells out to).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecondaryBinary {
    /// The secondary command probed.
    pub command: &'static str,
    /// Whether the secondary would actually resolve at spawn time: the
    /// auto-detected path resolved OR a valid `providers.paths` override
    /// targets it (override-aware, like [`ProviderAvailability::installed`]).
    pub resolved: bool,
    /// The auto-detected path, when found. Always override-free — `resolved`
    /// can be `true` while this is `None` (valid override, nothing
    /// auto-detected).
    pub resolved_path: Option<PathBuf>,
}

/// Status of npx availability for provider fallback spawning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NpxStatus {
    /// Resolved absolute path to npx, when found.
    pub resolved_path: Option<PathBuf>,
    /// Version string from `npx --version`, when successfully probed.
    pub version: Option<String>,
    /// Whether the version meets the minimum requirement (major >= 7).
    pub version_ok: bool,
}

/// Platform `PATH` list separator.
const PATH_SEP: char = if cfg!(windows) { ';' } else { ':' };

/// Candidate filenames to try when resolving a command in a directory.
/// POSIX uses the bare name. Windows probes only runnable entry points
/// (`.exe`/`.cmd`/`.bat`) and never the bare extensionless name —
/// `CreateProcess` cannot run it, and npm shim pairs (`auggie` next to
/// `auggie.cmd`) must resolve the `.cmd` shim — unless the command itself
/// already carries an executable extension.
fn name_candidates(command: &str) -> Vec<String> {
    name_candidates_for(command, cfg!(windows))
}

/// [`name_candidates`] parametrized on the platform (test seam — Windows CI
/// is disabled, so both arms are unit-tested on POSIX).
fn name_candidates_for(command: &str, is_windows: bool) -> Vec<String> {
    if !is_windows || has_windows_exec_extension(std::path::Path::new(command)) {
        return vec![command.to_string()];
    }
    WINDOWS_EXEC_EXTENSIONS
        .iter()
        .map(|ext| format!("{command}.{ext}"))
        .collect()
}

/// Resolve `command` to an executable path by scanning `PATH`, or `None`.
#[must_use]
pub fn resolve_on_path(command: &str) -> Option<PathBuf> {
    // An explicit path (rare in the registry) is honored directly.
    let as_path = PathBuf::from(command);
    if as_path.is_absolute() {
        return as_path.is_file().then_some(as_path);
    }
    let path = std::env::var_os("PATH")?;
    for dir in path.to_string_lossy().split(PATH_SEP) {
        let dir = dir.trim();
        if dir.is_empty() {
            continue;
        }
        for candidate in name_candidates(command) {
            let full = PathBuf::from(dir).join(&candidate);
            if full.is_file() {
                return Some(full);
            }
        }
    }
    None
}

/// Why a provider is gated off, or `None` when it is eligible for probing.
/// This is the single source of the env-var/feature-code gate shared by
/// discovery (`gatedOff`), `providers.catalog` (`visible`), and the
/// `models.list` cortex/droid sources.
#[must_use]
pub fn gated_reason(provider: &ProviderConfig) -> Option<String> {
    gated_reason_with_env(provider, &|var| std::env::var_os(var).is_some())
}

/// [`gated_reason`] with an injectable env-var presence probe, so callers'
/// unit tests can exercise both sides of the `requires_env_var` gate without
/// mutating the (process-global) real environment.
pub fn gated_reason_with_env(
    provider: &ProviderConfig,
    env_has: &dyn Fn(&str) -> bool,
) -> Option<String> {
    if let Some(var) = provider.requires_env_var {
        if !env_has(var) {
            return Some(format!("requires env var {var}"));
        }
    }
    if let Some(code) = provider.requires_feature_code {
        return Some(format!("requires feature code {code}"));
    }
    None
}

/// Discover availability for every configured provider (§6.9), in registry
/// order. Gated providers report `gated_off` and are not probed on `PATH`.
/// npx-only providers (claude-code) are probed for `npx` availability instead
/// of a local provider binary — there is no local-binary path for them.
/// All other providers resolve through [`find_provider_binary`] so this
/// aggregate surface and `host.providerAuthStatus` share one resolution
/// precedence: native installer locations (grok `~/.grok/bin`, opencode
/// `~/.opencode/bin`), then `~/.augment/bin`, then the enhanced PATH scan
/// (inherited PATH + enriched tool dirs + login-shell capture). Providers with
/// [`ProviderConfig::requires_secondary_binary`] set (unsloth: `opencode` +
/// `unsloth`) additionally require that second binary to resolve — the
/// `resolved_path` still reports the primary `command`'s path (the ACP spawn
/// target), but `installed` is `false` unless both binaries are present.
///
/// This entry point passes no `providers.paths` overrides (they live in
/// settings above this leaf crate); callers with settings access should use
/// [`discover_providers_with_overrides`] so `installed` matches what
/// `resolve_spawn` would actually spawn (monorepo#1065).
#[cfg(test)]
pub(crate) fn discover_providers() -> Vec<ProviderAvailability> {
    discover_providers_with_overrides(&|_| None)
}

/// [`discover_providers`] with `providers.paths` overrides supplied by the
/// caller (the settings live above this leaf crate; the daemon's transport
/// layer reads them and threads the values through — monorepo#1065).
/// `override_path` looks up the raw setting for a `providers.paths` key.
///
/// Overrides affect ONLY the `installed` / secondary `resolved` determination
/// (so both match what `resolve_spawn` / the managed-server lifecycle would
/// actually spawn); `resolved_path` / the secondary's `resolved_path` stay
/// auto-detected. An invalid override (relative / missing / non-executable)
/// contributes nothing, exactly like `find_provider_binary`'s explicit tier.
pub fn discover_providers_with_overrides(
    override_path: &dyn Fn(&str) -> Option<String>,
) -> Vec<ProviderAvailability> {
    discover_providers_with_overrides_and_resolver(override_path, &|id, cmd| {
        find_provider_binary(id, cmd, None)
    })
}

fn discover_providers_with_overrides_and_resolver(
    override_path: &dyn Fn(&str) -> Option<String>,
    resolve_auto: &dyn Fn(&str, &str) -> Option<PathBuf>,
) -> Vec<ProviderAvailability> {
    ACP_PROVIDERS
        .iter()
        .map(|provider| {
            availability_for(
                provider,
                gated_reason(provider),
                resolve_auto,
                override_path,
            )
        })
        .collect()
}

/// Resolve a single registered provider's availability by id, applying
/// `providers.paths` overrides the same way [`discover_providers_with_overrides`]
/// does. Returns `None` when `provider_id` is not registered. Exposed so
/// callers that cache per-provider results (the daemon's discovery cache,
/// keyed by provider id) can resolve one provider without re-walking and
/// re-resolving the entire registry.
pub fn provider_availability_for(
    provider_id: &str,
    override_path: &dyn Fn(&str) -> Option<String>,
) -> Option<ProviderAvailability> {
    let provider = ACP_PROVIDERS.iter().find(|p| p.id == provider_id)?;
    Some(availability_for(
        provider,
        gated_reason(provider),
        &|id, cmd| find_provider_binary(id, cmd, None),
        override_path,
    ))
}

/// Compute one provider's availability from injected resolvers (test seam —
/// lets unit tests exercise the override/auto combinations without touching
/// the real filesystem or `PATH`). `resolve_auto` performs the override-free
/// auto-detection for a `(provider_id, command)` pair; `override_path` looks
/// up the raw `providers.paths` value for a key.
fn availability_for(
    provider: &ProviderConfig,
    gated_off: Option<String>,
    resolve_auto: &dyn Fn(&str, &str) -> Option<PathBuf>,
    override_path: &dyn Fn(&str) -> Option<String>,
) -> ProviderAvailability {
    let resolved_path = if gated_off.is_some() {
        None
    } else if provider.npx_only_package.is_some() {
        find_npx()
    } else {
        resolve_auto(provider.id, provider.command)
    };
    // A valid override for the primary binary, keyed by the provider that
    // OWNS it ([`ProviderConfig::primary_binary_provider_id`], matching
    // `resolve_spawn`: unsloth's opencode primary honors the `opencode`
    // key). npx-only providers only honor it when they opt in
    // (`npx_only_honors_path_override`; claude-code) — `resolve_spawn` then
    // exec's a valid override in place of the pinned npx spawn
    // (monorepo#4352); pi keeps npx-only semantics, so an override never
    // flips its `installed`.
    let primary_override = if gated_off.is_some()
        || (provider.npx_only_package.is_some() && !provider.npx_only_honors_path_override)
    {
        None
    } else {
        let key = provider.primary_binary_provider_id();
        override_path(key).and_then(|p| resolve_explicit_path(key, &p))
    };
    // The secondary binary (unsloth: the `unsloth` CLI itself) is resolved
    // with the same provider_id/command pair the daemon's managed-server
    // lifecycle uses, plus the provider's OWN `providers.paths` key (the
    // `unsloth` key targets the unsloth CLI, `ensure_endpoint`'s spawn gate)
    // for the override-aware `resolved` flag.
    let secondary_binary = if gated_off.is_some() {
        None
    } else {
        provider.requires_secondary_binary.map(|s| {
            let auto = resolve_auto(s, s);
            let secondary_override =
                override_path(provider.id).and_then(|p| resolve_explicit_path(provider.id, &p));
            SecondaryBinary {
                command: s,
                resolved: auto.is_some() || secondary_override.is_some(),
                resolved_path: auto,
            }
        })
    };
    let installed = gated_off.is_none()
        && installed_with_secondary(
            resolved_path.is_some() || primary_override.is_some(),
            provider.requires_secondary_binary,
            |_| secondary_binary.as_ref().is_some_and(|s| s.resolved),
        );
    ProviderAvailability {
        id: provider.id,
        display_name: provider.display_name,
        command: provider.command,
        installed,
        resolved_path,
        gated_off,
        auth_check_args: provider.auth_check_args,
        has_npx_fallback: provider.fallback_npx_package.is_some(),
        npx_only_package: provider.npx_only_package,
        secondary_binary,
    }
}

/// Combine a provider's primary-binary resolution with its optional secondary
/// requirement ([`ProviderConfig::requires_secondary_binary`]): `true` only
/// when the primary resolved AND (no secondary is required OR the secondary
/// resolves too). Pure/injectable so the four presence combinations are unit
/// tested without touching the real filesystem or `PATH`.
fn installed_with_secondary(
    primary_resolved: bool,
    requires_secondary: Option<&str>,
    secondary_resolved: impl Fn(&str) -> bool,
) -> bool {
    primary_resolved && requires_secondary.is_none_or(secondary_resolved)
}

/// Human-readable detail for a not-installed provider, naming the
/// actually-missing binary. For dual-binary providers
/// ([`ProviderConfig::requires_secondary_binary`]) the message attributes
/// unavailability to whichever binary failed to resolve — e.g. unsloth with
/// opencode present but the `unsloth` CLI absent reports the unsloth CLI,
/// not opencode (monorepo#935). Pure, so every combination is unit-testable.
#[must_use]
pub fn not_installed_detail(
    command: &str,
    primary_resolved: bool,
    secondary_binary: Option<(&str, bool)>,
) -> String {
    match secondary_binary {
        Some((secondary, secondary_resolved)) => {
            match (primary_resolved, secondary_resolved) {
                (true, false) => format!("{secondary} not on PATH; {command} found"),
                (false, true) => format!("{command} not on PATH; {secondary} found"),
                (false, false) => format!("{command} and {secondary} not on PATH"),
                // Inconsistent input — a not-installed provider never has
                // both binaries resolved. Handled explicitly (this is a pub
                // helper) so a future caller can't print a false "not on
                // PATH" diagnosis.
                (true, true) => {
                    format!("{command} and {secondary} found, but provider reported not installed")
                }
            }
        }
        None => format!("{command} not on PATH"),
    }
}

/// Probe npx availability (path only, no spawning). Returns the resolved path
/// when npx is found on PATH. Version probing requires spawning `npx --version`
/// and is handled at the transport layer where a tokio runtime is available.
#[must_use]
pub fn probe_npx() -> NpxStatus {
    let resolved_path = find_npx();
    NpxStatus {
        resolved_path,
        version: None,
        version_ok: false,
    }
}

/// Resolve a provider binary to an absolute path using the precedence order:
/// 1. Explicit path from `providers.paths` map (keyed by the provider that
///    OWNS the binary, [`ProviderConfig::primary_binary_provider_id`]:
///    unsloth's opencode primary resolves under the `opencode` key, while
///    the `unsloth` key targets the unsloth CLI itself)
/// 2. Native installer location (grok: `~/.grok/bin`, opencode: `~/.opencode/bin`)
/// 3. `~/.augment/bin/<command>` (auggie-specific, not a generic managed tier)
/// 4. `~/.augment/auggie-path` marker (auggie-only, monorepo#939 parity) — the
///    authoritative install record, so it beats an arbitrary PATH-scan hit
/// 5. Scan enhanced PATH directories (`intent_core::path_utils`: inherited
///    PATH + enriched tool dirs + login-shell PATH capture)
///
/// Returns the first resolving tier, or `None` when the binary cannot be
/// resolved. The `provider_id` is used for logging when an explicit path is
/// invalid and to gate the auggie-only marker tier. Callers that need to
/// version-gate the result (the auggie ACP spawn path) use
/// [`find_auggie_candidates`] instead, which returns every tier in precedence
/// order so an incompatible hit can be skipped.
#[must_use]
pub fn find_provider_binary(
    provider_id: &str,
    command: &str,
    explicit_path: Option<&str>,
) -> Option<PathBuf> {
    let home = home_dir();
    find_provider_binary_with_home_and_dirs(
        provider_id,
        command,
        explicit_path,
        home.as_deref(),
        &intent_core::path_utils::enhanced_path_dirs(),
    )
}

/// Every resolved auggie binary candidate, in discovery-precedence order
/// (explicit `providers.paths["auggie"]` → `~/.augment/bin/auggie` →
/// `~/.augment/auggie-path` marker → each enhanced-PATH hit). Each entry is an
/// existing executable; the list is de-duplicated preserving first-seen order.
///
/// This is the version-gate-aware companion to [`find_provider_binary`]: the
/// auggie ACP spawn path probes `--version` on each candidate in order and
/// launches the first one new enough, so a stale nvm auggie earlier on PATH is
/// skipped rather than launched with flags it does not understand
/// (monorepo#1045 regression). `find_provider_binary("auggie", …)` returns the
/// first element of this list.
#[must_use]
pub fn find_auggie_candidates(explicit_path: Option<&str>) -> Vec<PathBuf> {
    let home = home_dir();
    find_auggie_candidates_with_home_and_dirs(
        explicit_path,
        home.as_deref(),
        &intent_core::path_utils::enhanced_path_dirs(),
    )
}

/// [`find_auggie_candidates`] with `home` and the enhanced dirs injected
/// (test seam — avoids mutating process-global `HOME`/`PATH` in parallel
/// tests). Builds the ordered, de-duplicated candidate list; the precedence
/// mirrors [`find_provider_binary_with_home_and_dirs`] for auggie exactly, so
/// the first element always equals what `find_provider_binary` would return.
fn find_auggie_candidates_with_home_and_dirs(
    explicit_path: Option<&str>,
    home: Option<&std::path::Path>,
    enhanced_dirs: &[PathBuf],
) -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = Vec::new();
    let push = |p: PathBuf, out: &mut Vec<PathBuf>| {
        if !out.contains(&p) {
            out.push(p);
        }
    };

    // 1. Explicit `providers.paths["auggie"]`.
    if let Some(path) = explicit_path {
        if let Some(pb) = resolve_explicit_path("auggie", path) {
            push(pb, &mut out);
        }
    }
    // 2. `~/.augment/bin/auggie` (auggie's own install location).
    if let Some(managed) = managed_binary_path_with_home("auggie", home) {
        if is_executable_file(&managed) {
            push(managed, &mut out);
        }
    }
    // 3. `~/.augment/auggie-path` marker (authoritative install record).
    if let Some(marked) = auggie_marker_path_with_home(home) {
        push(marked, &mut out);
    }
    // 4. Every enhanced-PATH hit, in order — so a too-old earlier hit can be
    // skipped in favor of a newer later one.
    for dir in enhanced_dirs {
        for candidate in &name_candidates("auggie") {
            let full = dir.join(candidate);
            if is_executable_file(&full) {
                push(full, &mut out);
            }
        }
    }
    out
}

/// [`find_provider_binary`] with an explicit `home` for every user-local tier
/// (test seam — avoids mutating process-global `HOME` in parallel tests).
#[cfg(test)]
fn find_provider_binary_with_home(
    provider_id: &str,
    command: &str,
    explicit_path: Option<&str>,
    home: Option<&std::path::Path>,
) -> Option<PathBuf> {
    find_provider_binary_with_home_and_dirs(
        provider_id,
        command,
        explicit_path,
        home,
        &intent_core::path_utils::enhanced_path_dirs_with_home(home),
    )
}

fn find_provider_binary_with_home_and_dirs(
    provider_id: &str,
    command: &str,
    explicit_path: Option<&str>,
    home: Option<&std::path::Path>,
    enhanced_dirs: &[PathBuf],
) -> Option<PathBuf> {
    // 1. Explicit setting wins (must be executable and absolute)
    if let Some(path) = explicit_path {
        // An Antigravity custom path is an explicit choice, including when its
        // official bundle is incomplete. Do not silently replace that choice.
        if provider_id == "antigravity" && !path.trim().is_empty() {
            return resolve_explicit_path(provider_id, path);
        }
        if let Some(pb) = resolve_explicit_path(provider_id, path) {
            return Some(pb);
        }
    }

    // 2. Native installer locations (grok: `~/.grok/bin/grok`, opencode:
    // `~/.opencode/bin/opencode`) are preferred over any PATH-resolved
    // npm-global wrapper (parity with `grok-resolver.ts` / `opencode-resolver.ts`:
    // wrappers can emit update banners before real stdout).
    if let Some(home) = home {
        if let Some(native) = find_provider_native_binary_in(provider_id, command, home) {
            return Some(native);
        }
    }

    // 3. ~/.augment/bin (auggie's install location; kept for auggie back-compat)
    if let Some(managed) = managed_binary_path_with_home(command, home) {
        if is_executable_file(&managed) {
            return Some(managed);
        }
    }

    // 4. ~/.augment/auggie-path marker (auggie-only): auggie's authoritative
    // record of where it installed itself. A daemon-launched process inherits
    // a minimal PATH that often misses that dir, so the marker must beat the
    // PATH scan below — otherwise an arbitrary nvm auggie earlier on PATH
    // wins over the install the user actually updated (monorepo#1045, and
    // parity with `intent_context::discovery::find_auggie`).
    if provider_id == "auggie" {
        if let Some(marked) = auggie_marker_path_with_home(home) {
            return Some(marked);
        }
    }

    // 5. Preserve complete existing installations before our managed bridge.
    // A stale launcher must not hide a later PATH hit or the recovered runtime.
    if provider_id == "antigravity" {
        return enhanced_dirs
            .iter()
            .flat_map(|dir| {
                name_candidates(command)
                    .into_iter()
                    .map(|name| dir.join(name))
            })
            .find(|path| crate::antigravity::is_complete_candidate(path))
            .or_else(|| {
                crate::antigravity::supported_host()
                    .then(|| home.and_then(crate::antigravity::managed_binary))
                    .flatten()
            });
    }
    find_in_dirs(enhanced_dirs, command)
}

/// The explicit-override tier ALONE for an npx-only provider
/// ([`ProviderConfig::npx_only_package`]) that opts in via
/// [`ProviderConfig::npx_only_honors_path_override`]: a valid
/// `providers.paths[id]` value (absolute, executable; `id` is the
/// [`ProviderConfig::primary_binary_provider_id`]) resolves to the adapter
/// binary to exec in place of the pinned `npx -y <package>` spawn; an
/// absent, blank, or invalid value — or a provider that does not opt in (pi)
/// — yields `None` and the caller keeps the pinned npx spawn. There is
/// deliberately no auto-discovery fallthrough (managed bin / PATH scan) —
/// that is what makes the provider npx-only. Shared by the ACP spawn,
/// discovery's `installed`, the one-shot / test-prompt launches, and the
/// claude-code ACP auth fallback probe so every launch surface runs the same
/// adapter (intent-hq/monorepo#4352). The model-catalog fetch is NOT on this
/// path: it always runs the pinned package (the catalog registry has no
/// settings access, and the list is not an auth signal).
#[must_use]
pub fn resolve_npx_only_override(
    provider: &ProviderConfig,
    explicit_path: Option<&str>,
) -> Option<PathBuf> {
    if !provider.npx_only_honors_path_override {
        return None;
    }
    let key = provider.primary_binary_provider_id();
    explicit_path.and_then(|p| resolve_explicit_path(key, p))
}

/// Validate + resolve an explicit `providers.paths` value: trimmed, absolute,
/// and executable, or `None` (with a warning naming the provider key — blank
/// values are treated as unset without warning). Shared by
/// [`find_provider_binary`]'s explicit tier, [`resolve_npx_only_override`],
/// and the override-aware `installed` determination in
/// [`discover_providers_with_overrides`], so every side accepts exactly the
/// same overrides.
#[must_use]
pub fn resolve_explicit_path(provider_id: &str, path: &str) -> Option<PathBuf> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return None;
    }
    let pb = PathBuf::from(trimmed);
    if pb.is_absolute() && is_executable_file(&pb) {
        if provider_id == "antigravity" && !crate::antigravity::is_complete_candidate(&pb) {
            tracing::warn!(configured_path = trimmed, "Antigravity custom path has missing or non-executable runtime files; repair the custom installation");
            return None;
        }
        return Some(pb);
    }
    if provider_id == "antigravity" {
        tracing::warn!(configured_path = trimmed, "Antigravity custom path must be absolute and executable; repair or clear the custom path before setup can continue");
        return None;
    }
    // Other providers retain their existing fallback policy for invalid paths.
    tracing::warn!(
        provider_id = provider_id,
        configured_path = trimmed,
        "providers.paths[\"{}\"] must be absolute and executable; ignoring the override (falling back to native install dir / managed bin / PATH scan, or the pinned npx package)",
        provider_id
    );
    None
}

/// The `$HOME`-relative directory a provider's native installer places its
/// binary under (`~/<dot_dir>/bin/<command>`), or `None` for providers without
/// a native-installer tier.
fn native_install_dir(provider_id: &str) -> Option<&'static str> {
    match provider_id {
        "grok" => Some(".grok"),
        // unsloth rides the opencode binary, so it shares opencode's
        // native-installer tier (`~/.opencode/bin/opencode`).
        "opencode" | "unsloth" => Some(".opencode"),
        _ => None,
    }
}

/// Candidate paths for a provider's native installer location under `home`
/// (`~/<dot_dir>/bin/<command>`; Windows probes only the runnable
/// `.exe`/`.cmd`/`.bat` variants, same preference as [`name_candidates`]).
/// Port of `GROK_NATIVE_PATHS` / `OPENCODE_NATIVE_PATHS` from the FE resolvers.
fn native_install_candidates(home: &std::path::Path, dot_dir: &str, command: &str) -> Vec<PathBuf> {
    native_install_candidates_for(home, dot_dir, command, cfg!(windows))
}

/// [`native_install_candidates`] parametrized on the platform (test seam —
/// Windows CI is disabled, so both arms are unit-tested on POSIX).
fn native_install_candidates_for(
    home: &std::path::Path,
    dot_dir: &str,
    command: &str,
    is_windows: bool,
) -> Vec<PathBuf> {
    let bin = home.join(dot_dir).join("bin");
    name_candidates_for(command, is_windows)
        .into_iter()
        .map(|name| bin.join(name))
        .collect()
}

/// Resolve a provider's native installer binary under an explicit `home`
/// (test seam — avoids mutating the process-global `HOME` in parallel tests).
fn find_provider_native_binary_in(
    provider_id: &str,
    command: &str,
    home: &std::path::Path,
) -> Option<PathBuf> {
    let dot_dir = native_install_dir(provider_id)?;
    native_install_candidates(home, dot_dir, command)
        .into_iter()
        .find(|p| is_executable_file(p))
}

/// The auggie binary path (`~/.augment/bin/<command>[.exe]`). This is auggie's
/// own install location, not a generic Intent-managed binary tier.
#[cfg(test)]
fn managed_binary_path(command: &str) -> Option<PathBuf> {
    let home = home_dir();
    managed_binary_path_with_home(command, home.as_deref())
}

fn managed_binary_path_with_home(command: &str, home: Option<&std::path::Path>) -> Option<PathBuf> {
    let home = home?;
    let name = if cfg!(windows) {
        format!("{command}.exe")
    } else {
        command.to_string()
    };
    Some(home.join(".augment").join("bin").join(name))
}

/// Read the auggie binary path recorded by auggie's own installer in
/// `~/.augment/auggie-path` (a single line holding an absolute path). The
/// first non-blank line is used, so a marker that grows extra lines still
/// resolves. This is the authoritative record of where auggie last installed
/// itself — daemon-launched processes inherit a minimal PATH that often misses
/// that directory, so the marker must win over an arbitrary PATH-scan hit
/// (parity with `intent_context::discovery`, monorepo#939).
///
/// Returns `None` — silently — when the marker is missing, unreadable, empty,
/// relative, or stale (points at something that is no longer an executable
/// file), so a leftover marker never shadows a working install.
fn auggie_marker_path_with_home(home: Option<&std::path::Path>) -> Option<PathBuf> {
    auggie_marker_path_with_home_for(home, cfg!(windows))
}

/// [`auggie_marker_path_with_home`] parametrized on the platform (test seam —
/// Windows CI is disabled, so the Windows arm is unit-tested on POSIX).
fn auggie_marker_path_with_home_for(
    home: Option<&std::path::Path>,
    is_windows: bool,
) -> Option<PathBuf> {
    let marker = home?.join(".augment").join("auggie-path");
    let contents = std::fs::read_to_string(&marker).ok()?;
    let recorded = PathBuf::from(contents.lines().map(str::trim).find(|l| !l.is_empty())?);
    if !recorded.is_absolute() {
        return None;
    }
    resolve_marker_runnable(&recorded, is_windows)
}

/// Resolve a marker-recorded path to a runnable executable under the platform
/// policy. An already-executable path is returned as-is. On Windows a bare
/// extensionless record (auggie's installer writes `…\npm\auggie` even though
/// only `auggie.cmd` is runnable) is resolved to its runnable sibling by
/// probing `.exe`/`.cmd`/`.bat` in the same directory. Ported from
/// `intent_context::discovery::resolve_runnable`.
fn resolve_marker_runnable(recorded: &std::path::Path, is_windows: bool) -> Option<PathBuf> {
    if is_executable_file_for(recorded, is_windows) {
        return Some(recorded.to_path_buf());
    }
    if is_windows && !has_windows_exec_extension(recorded) {
        for ext in WINDOWS_EXEC_EXTENSIONS {
            let candidate = recorded.with_extension(ext);
            if is_executable_file_for(&candidate, is_windows) {
                return Some(candidate);
            }
        }
    }
    None
}

/// Resolve the user's home directory from environment, cross-platform.
fn home_dir() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
}

/// Find the first executable for `command` by scanning enhanced PATH directories
/// from `intent_core::path_utils::enhanced_path_dirs()`: inherited PATH plus
/// enriched tool dirs (node/npm/nvm/homebrew/volta/asdf, …) plus the cached
/// login-shell PATH capture.
///
/// Blocking: this scans the filesystem, and on Unix the *first* per-process
/// call can spawn `$SHELL -ilc` (up to 5s, then cached). Latency-sensitive
/// async callers should prewarm or wrap in `spawn_blocking`.
fn find_in_enhanced_dirs(command: &str) -> Option<PathBuf> {
    find_in_dirs(&intent_core::path_utils::enhanced_path_dirs(), command)
}

/// Detect the user's CLI separately from the ACP runtime. Never spawn it.
#[must_use]
pub fn find_antigravity_cli() -> Option<PathBuf> {
    find_in_enhanced_dirs("agy")
}

/// Find the first executable for `command` in `dirs`, in order (test seam —
/// lets tests scan a controlled dir list without spawning a login shell).
fn find_in_dirs(dirs: &[PathBuf], command: &str) -> Option<PathBuf> {
    find_in_dirs_for(dirs, command, cfg!(windows))
}

/// [`find_in_dirs`] parametrized on the platform (test seam — Windows CI is
/// disabled, so the Windows candidate/executability arm is unit-tested on
/// POSIX).
fn find_in_dirs_for(dirs: &[PathBuf], command: &str, is_windows: bool) -> Option<PathBuf> {
    let candidates = name_candidates_for(command, is_windows);
    for dir in dirs {
        for candidate in &candidates {
            let full = dir.join(candidate);
            if is_executable_file_for(&full, is_windows) {
                return Some(full);
            }
        }
    }
    None
}

/// Resolve `npx` to an absolute path using the same enhanced PATH scanning that
/// `find_provider_binary` uses. Returns `None` when npx cannot be found.
#[must_use]
pub fn find_npx() -> Option<PathBuf> {
    find_in_enhanced_dirs("npx")
}

/// Resolve the real `pi` CLI — the binary pi-acp spawns (and the generated
/// wrapper script execs) — to an absolute path. A command carrying a path
/// separator (the `PI_ACP_PI_COMMAND` override shape) is validated directly
/// as an executable file; a bare name scans the SPAWN-TIME enhanced PATH
/// ([`crate::args::enhanced_path`] over the resolved npx binary — pi is
/// npx-only, so the pi-acp child's PATH prepends npx's parent dir and
/// `~/.augment/bin` ahead of the enriched/inherited dirs), so the probe
/// resolves the same `pi` the wrapper will actually exec, not merely one
/// visible to the daemon.
#[must_use]
pub fn find_pi_cli(command: &str) -> Option<PathBuf> {
    let as_path = PathBuf::from(command);
    if as_path.is_absolute() || command.contains(std::path::MAIN_SEPARATOR) {
        return is_executable_file(&as_path).then_some(as_path);
    }
    let npx = find_npx();
    find_in_dirs(
        &crate::args::enhanced_path_spawn_dirs(npx.as_deref()),
        command,
    )
}

#[cfg(test)]
mod find_provider_binary_tests {
    use super::*;
    use std::fs;

    /// A fresh RAII temp directory for `tag` under the system temp root. The
    /// returned guard removes the dir on drop (including on panic); set
    /// `INTENTD_TEST_KEEP_TMP` (non-empty) to keep it around for debugging.
    fn unique_temp_dir(tag: &str) -> tempfile::TempDir {
        let mut dir = tempfile::Builder::new()
            .prefix(&format!("intent-providers-{tag}-"))
            .tempdir()
            .expect("create test temp dir");
        if std::env::var_os("INTENTD_TEST_KEEP_TMP").is_some_and(|v| !v.is_empty()) {
            dir.disable_cleanup(true);
        }
        dir
    }

    #[cfg(unix)]
    fn make_executable(path: &std::path::Path) {
        use std::os::unix::fs::PermissionsExt;
        fs::write(path, "#!/bin/sh\nexit 0\n").unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
    }

    #[cfg(not(unix))]
    fn make_executable(path: &std::path::Path) {
        fs::write(path, "exit 0").unwrap();
    }

    #[cfg(unix)]
    fn make_antigravity_bundle(home: &std::path::Path) -> PathBuf {
        use crate::antigravity::{install_root, ARCHIVE_SHA256, FILES, VERSION};
        let version = install_root(home).join(VERSION);
        fs::create_dir_all(&version).unwrap();
        for (name, bytes, _) in FILES {
            let path = version.join(name);
            make_executable(&path);
            fs::OpenOptions::new()
                .write(true)
                .open(path)
                .unwrap()
                .set_len(bytes)
                .unwrap();
        }
        fs::write(version.join("ready"), ARCHIVE_SHA256).unwrap();
        version
    }

    #[test]
    fn find_provider_binary_returns_none_when_absent() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let unique_cmd = format!("intent-test-absent-{nanos}");
        let result = find_provider_binary("nonexistent", &unique_cmd, None);
        assert_eq!(result, None);
    }

    #[test]
    fn find_provider_binary_prefers_explicit_setting() {
        let dir = unique_temp_dir("explicit");
        let bin = dir.path().join("my-provider");
        make_executable(&bin);
        let result = find_provider_binary("test", "my-provider", Some(bin.to_str().unwrap()));
        assert_eq!(result, Some(bin));
    }

    #[cfg(unix)]
    #[test]
    fn antigravity_managed_discovery_requires_a_complete_activated_bundle() {
        use crate::antigravity::{managed_binary, ARCHIVE_SHA256, HARNESS, SERVER};
        let home = unique_temp_dir("antigravity-managed");
        let version = make_antigravity_bundle(home.path());
        fs::remove_file(version.join("ready")).unwrap();
        assert_eq!(
            managed_binary(home.path()),
            None,
            "staging is not discoverable"
        );
        fs::write(version.join("ready"), ARCHIVE_SHA256).unwrap();
        assert_eq!(managed_binary(home.path()), Some(version.join(SERVER)));
        fs::remove_file(version.join(HARNESS)).unwrap();
        assert_eq!(
            managed_binary(home.path()),
            None,
            "missing companion is not installed"
        );
    }

    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    #[test]
    fn antigravity_cli_is_not_acp_and_custom_installations_keep_precedence() {
        use crate::antigravity::SERVER;
        let home = unique_temp_dir("antigravity-precedence");
        let bin = home.path().join("bin");
        fs::create_dir(&bin).unwrap();
        make_executable(&bin.join("agy"));
        let find = |explicit: Option<&str>| {
            find_provider_binary_with_home_and_dirs(
                "antigravity",
                "antigravity-acp",
                explicit,
                Some(home.path()),
                std::slice::from_ref(&bin),
            )
        };
        assert_eq!(find(None), None);
        let version = make_antigravity_bundle(home.path());
        assert_eq!(find(None), Some(version.join(SERVER)));
        let on_path = bin.join("antigravity-acp");
        make_executable(&on_path);
        assert_eq!(find(None), Some(on_path));
        let custom = home.path().join("custom");
        make_executable(&custom);
        assert_eq!(find(custom.to_str()), Some(custom));
    }

    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    #[test]
    fn antigravity_deleted_runtime_does_not_shadow_recovered_managed_bundle() {
        use crate::antigravity::{HARNESS, SERVER};
        let home = unique_temp_dir("antigravity-deleted-runtime");
        let bin = home.path().join("bin");
        let old = home.path().join("old install");
        fs::create_dir(&bin).unwrap();
        fs::create_dir(&old).unwrap();
        let server = old.join(SERVER);
        make_executable(&server);
        make_executable(&old.join(HARNESS));
        let wrapper = bin.join("antigravity-acp");
        let script = format!("#!/bin/sh\nexec \"{}\" \"$@\"\n", server.display());
        make_executable(&wrapper);
        fs::write(&wrapper, &script).unwrap();
        let find = |explicit| {
            find_provider_binary_with_home_and_dirs(
                "antigravity",
                "antigravity-acp",
                explicit,
                Some(home.path()),
                std::slice::from_ref(&bin),
            )
        };
        assert_eq!(find(None), Some(wrapper.clone()));
        fs::remove_file(&server).unwrap();
        assert_eq!(
            find(None),
            None,
            "Connect must enter the missing-runtime installer branch"
        );
        let managed = make_antigravity_bundle(home.path()).join(SERVER);
        assert_eq!(
            find(None),
            Some(managed),
            "every shared resolver caller must use the recovered bundle"
        );
        assert_eq!(
            find(wrapper.to_str()),
            None,
            "a broken explicit override must not fall through"
        );
        assert_eq!(fs::read_to_string(&wrapper).unwrap(), script);
        assert!(
            !server.exists(),
            "recovery must not rewrite the user's old bundle"
        );
    }

    #[cfg(unix)]
    #[test]
    fn antigravity_skips_incomplete_path_launchers_but_keeps_later_candidates() {
        use crate::antigravity::{HARNESS, SERVER};
        let home = unique_temp_dir("antigravity-path-candidates");
        let first = home.path().join("first");
        let second = home.path().join("second");
        fs::create_dir(&first).unwrap();
        fs::create_dir(&second).unwrap();
        let server = home.path().join(SERVER);
        make_executable(&server);
        let broken = first.join("antigravity-acp");
        make_executable(&broken);
        fs::write(
            &broken,
            format!("#!/bin/sh\nexec {} \"$@\"\n", server.display()),
        )
        .unwrap();
        let later = second.join("antigravity-acp");
        make_executable(&later);
        let dirs = [first, second];
        let find = || {
            find_provider_binary_with_home_and_dirs(
                "antigravity",
                "antigravity-acp",
                None,
                Some(home.path()),
                &dirs,
            )
        };
        assert_eq!(
            find(),
            Some(later),
            "missing companion must also skip a wrapper"
        );
        make_executable(&home.path().join(HARNESS));
        assert_eq!(
            find(),
            Some(broken),
            "healthy existing installation keeps precedence"
        );
    }

    #[cfg(unix)]
    #[test]
    fn antigravity_direct_and_symlinked_servers_require_the_companion() {
        use crate::antigravity::{is_complete_candidate, HARNESS, SERVER};
        let home = unique_temp_dir("antigravity-symlink");
        let server = home.path().join(SERVER);
        make_executable(&server);
        let link = home.path().join("antigravity-acp");
        std::os::unix::fs::symlink(&server, &link).unwrap();
        assert!(!is_complete_candidate(&server));
        assert!(!is_complete_candidate(&link));
        make_executable(&home.path().join(HARNESS));
        assert!(is_complete_candidate(&link));
        fs::remove_file(server).unwrap();
        assert!(!is_complete_candidate(&link));
    }

    #[cfg(unix)]
    #[test]
    fn antigravity_literal_wrapper_checks_are_bounded_and_never_execute_scripts() {
        use crate::antigravity::{is_complete_candidate, SERVER};
        let home = unique_temp_dir("antigravity-literal-launcher");
        let wrapper = home.path().join("antigravity-acp");
        make_executable(&wrapper);
        let missing = home.path().join(SERVER);
        for target in [
            missing.display().to_string(),
            format!("'{}'", missing.display()),
            format!("\"{}\"", missing.display()),
        ] {
            fs::write(&wrapper, format!("#!/bin/sh\nexec {target} \"$@\"\n")).unwrap();
            assert!(!is_complete_candidate(&wrapper));
        }
        // Unknown custom commands are not classified as missing runtimes, even
        // if they would fail authentication or need their own environment.
        let marker = home.path().join("must-not-run");
        for body in [
            format!("touch '{}'\nexit 42", marker.display()),
            "exec \"$HOME/agy_acp_server.par\" \"$@\"".into(),
            "exec \"$(resolve_runtime)/agy_acp_server.par\" \"$@\"".into(),
            format!("exec {} \"$@\"\nexit 42", missing.display()),
            format!("exec {} \"$@\"\n#{}", missing.display(), "x".repeat(4096)),
        ] {
            fs::write(&wrapper, format!("#!/bin/sh\n{body}\n")).unwrap();
            assert!(
                is_complete_candidate(&wrapper),
                "opaque command must retain custom-adapter semantics"
            );
        }
        assert!(
            !marker.exists(),
            "discovery must not execute even a small script"
        );
    }

    #[test]
    fn find_provider_binary_ignores_empty_explicit_setting() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let unique_cmd = format!("intent-test-cmd-{nanos}");
        let result = find_provider_binary("test", &unique_cmd, Some(""));
        assert_eq!(result, None);
    }

    #[test]
    fn find_provider_binary_ignores_whitespace_only_explicit_setting() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let unique_cmd = format!("intent-test-cmd-{nanos}");
        let result = find_provider_binary("test", &unique_cmd, Some("   "));
        assert_eq!(result, None);
    }

    #[test]
    fn find_provider_binary_falls_through_when_explicit_path_missing() {
        // When providers.paths.<id> points to a missing file, resolution should
        // fall through to managed bin / PATH scan (and warn)
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let unique_cmd = format!("intent-test-cmd-{nanos}");
        let result = find_provider_binary("test", &unique_cmd, Some("/nonexistent/path/binary"));
        // Should fall through and return None since we don't have managed bin or PATH match
        assert_eq!(result, None);
    }

    #[cfg(unix)]
    #[test]
    fn find_provider_binary_returns_none_when_no_candidates_found() {
        // Verify function returns None when binary is not in any of the search locations
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let unique_cmd = format!("intent-test-nocand-{nanos}");
        let result = find_provider_binary("test", &unique_cmd, None);
        assert_eq!(result, None);
    }

    /// Write `~/.augment/auggie-path` under `home` with the given contents.
    fn write_marker(home: &std::path::Path, contents: &str) {
        let augment = home.join(".augment");
        fs::create_dir_all(&augment).unwrap();
        fs::write(augment.join("auggie-path"), contents).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn auggie_marker_resolves_recorded_executable() {
        let dir = unique_temp_dir("marker-ok");
        let bin = dir.path().join("marked-auggie");
        make_executable(&bin);
        write_marker(dir.path(), bin.to_str().unwrap());
        assert_eq!(auggie_marker_path_with_home(Some(dir.path())), Some(bin));
    }

    #[test]
    fn auggie_marker_stale_or_relative_returns_none() {
        let dir = unique_temp_dir("marker-stale");
        // Stale: points at a nonexistent file.
        write_marker(dir.path(), dir.path().join("gone").to_str().unwrap());
        assert_eq!(auggie_marker_path_with_home(Some(dir.path())), None);
        // Relative: rejected.
        write_marker(dir.path(), "auggie");
        assert_eq!(auggie_marker_path_with_home(Some(dir.path())), None);
        // Missing marker / no home.
        assert_eq!(auggie_marker_path_with_home(None), None);
    }

    #[cfg(unix)]
    #[test]
    fn auggie_marker_beats_path_scan_but_loses_to_managed_bin() {
        // Marker wins over an arbitrary PATH hit (the monorepo#1045 fix)...
        let dir = unique_temp_dir("marker-vs-path");
        let marked = dir.path().join("marked-auggie");
        make_executable(&marked);
        write_marker(dir.path(), marked.to_str().unwrap());
        let path_dir = unique_temp_dir("marker-vs-path-scan");
        make_executable(&path_dir.path().join("auggie"));
        assert_eq!(
            find_provider_binary_with_home_and_dirs(
                "auggie",
                "auggie",
                None,
                Some(dir.path()),
                &[path_dir.path().to_path_buf()],
            ),
            Some(marked),
        );
        // ...but ~/.augment/bin still outranks the marker.
        let managed = dir.path().join(".augment").join("bin").join("auggie");
        fs::create_dir_all(managed.parent().unwrap()).unwrap();
        make_executable(&managed);
        assert_eq!(
            find_provider_binary_with_home_and_dirs(
                "auggie",
                "auggie",
                None,
                Some(dir.path()),
                &[path_dir.path().to_path_buf()],
            ),
            Some(managed),
        );
    }

    #[cfg(unix)]
    #[test]
    fn marker_tier_is_auggie_only() {
        // A non-auggie provider never consults the auggie marker, even if the
        // recorded path happens to be executable.
        let dir = unique_temp_dir("marker-other-provider");
        let marked = dir.path().join("marked-auggie");
        make_executable(&marked);
        write_marker(dir.path(), marked.to_str().unwrap());
        assert_eq!(
            find_provider_binary_with_home_and_dirs("grok", "grok", None, Some(dir.path()), &[],),
            None,
        );
    }

    #[cfg(unix)]
    #[test]
    fn auggie_candidates_ordered_and_deduped_across_all_tiers() {
        let dir = unique_temp_dir("candidates");
        // Managed bin.
        let managed = dir.path().join(".augment").join("bin").join("auggie");
        fs::create_dir_all(managed.parent().unwrap()).unwrap();
        make_executable(&managed);
        // Marker.
        let marked = dir.path().join("marked-auggie");
        make_executable(&marked);
        write_marker(dir.path(), marked.to_str().unwrap());
        // Two PATH dirs, the second duplicating the managed bin dir so dedup
        // is exercised.
        let path1 = unique_temp_dir("candidates-p1");
        let p1_bin = path1.path().join("auggie");
        make_executable(&p1_bin);
        let managed_dir = managed.parent().unwrap().to_path_buf();

        let candidates = find_auggie_candidates_with_home_and_dirs(
            None,
            Some(dir.path()),
            &[path1.path().to_path_buf(), managed_dir],
        );
        // Order: managed bin, marker, first PATH hit — managed bin appears once
        // even though its dir is also in the enhanced dirs list.
        assert_eq!(candidates, vec![managed.clone(), marked, p1_bin]);
        // The first candidate always equals find_provider_binary's result.
        assert_eq!(
            find_provider_binary_with_home_and_dirs(
                "auggie",
                "auggie",
                None,
                Some(dir.path()),
                &[path1.path().to_path_buf()],
            ),
            Some(managed),
        );
    }

    #[cfg(unix)]
    #[test]
    fn auggie_candidates_explicit_path_leads() {
        let dir = unique_temp_dir("candidates-explicit");
        let explicit = dir.path().join("explicit-auggie");
        make_executable(&explicit);
        let path_dir = unique_temp_dir("candidates-explicit-path");
        make_executable(&path_dir.path().join("auggie"));
        let candidates = find_auggie_candidates_with_home_and_dirs(
            Some(explicit.to_str().unwrap()),
            Some(dir.path()),
            &[path_dir.path().to_path_buf()],
        );
        assert_eq!(candidates.first(), Some(&explicit));
    }

    #[test]
    fn discover_providers_reports_claude_code_as_npx_only() {
        let providers = discover_providers();
        let cc = providers.iter().find(|p| p.id == "claude-code").unwrap();
        assert_eq!(
            cc.npx_only_package,
            Some(crate::config::CLAUDE_AGENT_ACP_NPX_PACKAGE),
            "claude-code availability must carry the pinned npx package"
        );
        // Assert against the single discovery snapshot rather than re-resolving
        // npx (no test mutates process-global PATH anymore — monorepo#628 —
        // but the snapshot assertion stays robust regardless).
        assert_eq!(cc.installed, cc.resolved_path.is_some());
        if let Some(path) = &cc.resolved_path {
            assert!(path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("npx"));
        }
    }

    #[test]
    fn discover_providers_reports_pi_as_npx_only() {
        let providers = discover_providers();
        let pi = providers.iter().find(|p| p.id == "pi").unwrap();
        assert_eq!(
            pi.npx_only_package,
            Some(crate::config::PI_ACP_NPX_PACKAGE),
            "pi availability must carry the pinned npx package"
        );
        // Assert against the same discovery snapshot rather than re-resolving
        // npx (see the claude-code test above; monorepo#628).
        assert_eq!(pi.installed, pi.resolved_path.is_some());
        if let Some(path) = &pi.resolved_path {
            assert!(path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("npx"));
        }
    }

    #[test]
    fn discover_providers_non_npx_only_providers_unchanged() {
        let providers = discover_providers();
        for p in providers
            .iter()
            .filter(|p| p.id != "claude-code" && p.id != "pi")
        {
            assert_eq!(p.npx_only_package, None, "{} must not be npx-only", p.id);
        }
    }

    #[test]
    fn grok_native_candidates_prefer_home_grok_bin() {
        let home = PathBuf::from("/home/tester");
        let bin = home.join(".grok").join("bin");
        assert_eq!(
            native_install_candidates_for(&home, ".grok", "grok", false),
            vec![bin.join("grok")],
            "POSIX probes only the bare native installer path"
        );
        assert_eq!(
            native_install_candidates_for(&home, ".grok", "grok", true),
            vec![
                bin.join("grok.exe"),
                bin.join("grok.cmd"),
                bin.join("grok.bat")
            ],
            "Windows probes runnable entry points, never the bare name"
        );
    }

    #[test]
    fn opencode_native_candidates_prefer_home_opencode_bin() {
        let home = PathBuf::from("/home/tester");
        let bin = home.join(".opencode").join("bin");
        assert_eq!(
            native_install_candidates_for(&home, ".opencode", "opencode", false),
            vec![bin.join("opencode")],
            "POSIX probes only the bare native installer path"
        );
        assert_eq!(
            native_install_candidates_for(&home, ".opencode", "opencode", true),
            vec![
                bin.join("opencode.exe"),
                bin.join("opencode.cmd"),
                bin.join("opencode.bat")
            ],
            "Windows probes runnable entry points, never the bare name"
        );
    }

    #[cfg(unix)]
    #[test]
    fn find_grok_native_binary_in_requires_executable_at_native_path() {
        // End-to-end against a fake home: `<home>/.grok/bin/grok` resolves
        // only once it is executable (non-executable files must not resolve).
        let home = unique_temp_dir("grok-home");
        let bin_dir = home.path().join(".grok").join("bin");
        fs::create_dir_all(&bin_dir).unwrap();
        assert_eq!(
            find_provider_native_binary_in("grok", "grok", home.path()),
            None
        );

        let bin = bin_dir.join("grok");
        fs::write(&bin, "not executable").unwrap();
        assert_eq!(
            find_provider_native_binary_in("grok", "grok", home.path()),
            None
        );

        make_executable(&bin);
        assert_eq!(
            find_provider_native_binary_in("grok", "grok", home.path()),
            Some(bin)
        );
    }

    #[cfg(unix)]
    #[test]
    fn find_opencode_native_binary_in_requires_executable_at_native_path() {
        // Regression for opencode installed only via its native installer
        // (`<home>/.opencode/bin/opencode`, no PATH entry): resolution must
        // find it, and only once it is executable.
        let home = unique_temp_dir("opencode-home");
        let bin_dir = home.path().join(".opencode").join("bin");
        fs::create_dir_all(&bin_dir).unwrap();
        assert_eq!(
            find_provider_native_binary_in("opencode", "opencode", home.path()),
            None
        );

        let bin = bin_dir.join("opencode");
        fs::write(&bin, "not executable").unwrap();
        assert_eq!(
            find_provider_native_binary_in("opencode", "opencode", home.path()),
            None
        );

        make_executable(&bin);
        assert_eq!(
            find_provider_native_binary_in("opencode", "opencode", home.path()),
            Some(bin)
        );
    }

    #[cfg(unix)]
    #[test]
    fn find_provider_native_binary_in_ignores_providers_without_native_installs() {
        // Only grok/opencode have native-installer tiers; other providers must
        // not resolve from a lookalike dot-dir layout.
        let home = unique_temp_dir("native-other");
        let bin_dir = home.path().join(".auggie").join("bin");
        fs::create_dir_all(&bin_dir).unwrap();
        let bin = bin_dir.join("auggie");
        make_executable(&bin);
        assert_eq!(
            find_provider_native_binary_in("auggie", "auggie", home.path()),
            None
        );
    }

    #[cfg(unix)]
    #[test]
    fn find_provider_binary_explicit_setting_wins_over_native_for_opencode() {
        // Precedence: with a native binary present in the fake home, the
        // explicit `providers.paths` setting still wins; without the explicit
        // setting, the native tier resolves.
        let home = unique_temp_dir("opencode-precedence-home");
        let native_dir = home.path().join(".opencode").join("bin");
        fs::create_dir_all(&native_dir).unwrap();
        let native = native_dir.join("opencode");
        make_executable(&native);

        let explicit_dir = unique_temp_dir("opencode-explicit");
        let explicit = explicit_dir.path().join("opencode");
        make_executable(&explicit);

        let result = find_provider_binary_with_home(
            "opencode",
            "opencode",
            Some(explicit.to_str().unwrap()),
            Some(home.path()),
        );
        assert_eq!(result, Some(explicit), "explicit setting must beat native");

        let result =
            find_provider_binary_with_home("opencode", "opencode", None, Some(home.path()));
        assert_eq!(
            result,
            Some(native),
            "native tier must resolve without an explicit setting"
        );
    }

    #[cfg(unix)]
    #[test]
    fn find_in_dirs_scans_login_shell_style_dirs() {
        // The enhanced scan must find binaries in dirs that only appear via
        // the login-shell PATH capture (injected here as a controlled dir
        // list — no real login shell is spawned, same seam pattern as
        // `intent_core::path_utils` tests).
        let login_dir = unique_temp_dir("login-shell-bin");
        let bin = login_dir.path().join("opencode");
        make_executable(&bin);

        let dirs = vec![
            PathBuf::from("/nonexistent/first"),
            login_dir.path().to_path_buf(),
        ];
        assert_eq!(find_in_dirs(&dirs, "opencode"), Some(bin));
        assert_eq!(find_in_dirs(&dirs, "intent-test-absent-cmd"), None);
    }

    #[cfg(unix)]
    #[test]
    fn find_provider_binary_scans_non_default_nvm_version() {
        let home = unique_temp_dir("nvm-provider-home");
        let v20_bin = home.path().join(".nvm/versions/node/v20.19.0/bin");
        let v24_bin = home.path().join(".nvm/versions/node/v24.5.0/bin");
        fs::create_dir_all(&v20_bin).unwrap();
        fs::create_dir_all(&v24_bin).unwrap();
        make_executable(&v20_bin.join("node"));

        let command = format!(
            "intent-nvm-provider-{}",
            home.path().file_name().unwrap().to_string_lossy()
        );
        let binary = v24_bin.join(&command);
        make_executable(&binary);

        assert_eq!(
            find_provider_binary_with_home("test", &command, None, Some(home.path())),
            Some(binary)
        );
    }

    #[cfg(unix)]
    #[test]
    fn discovery_reports_provider_from_non_default_nvm_version() {
        let home = unique_temp_dir("nvm-discovery-home");
        let v20_bin = home.path().join(".nvm/versions/node/v20.19.0/bin");
        let v24_bin = home.path().join(".nvm/versions/node/v24.5.0/bin");
        fs::create_dir_all(&v20_bin).unwrap();
        fs::create_dir_all(&v24_bin).unwrap();
        make_executable(&v20_bin.join("node"));
        let codex = v24_bin.join("codex-acp");
        make_executable(&codex);
        let dirs = vec![v20_bin, v24_bin];

        let providers = discover_providers_with_overrides_and_resolver(&|_| None, &|_, command| {
            find_in_dirs(&dirs, command)
        });
        let availability = providers.iter().find(|p| p.id == "codex").unwrap();

        assert!(availability.installed);
        assert_eq!(availability.resolved_path.as_deref(), Some(codex.as_path()));
    }

    #[test]
    fn installed_with_secondary_no_requirement_mirrors_primary() {
        assert!(installed_with_secondary(true, None, |_| false));
        assert!(!installed_with_secondary(false, None, |_| true));
    }

    #[test]
    fn installed_with_secondary_requires_both_present() {
        // Both present.
        assert!(installed_with_secondary(true, Some("unsloth"), |_| true));
        // Only primary (opencode) present, secondary (unsloth CLI) missing.
        assert!(!installed_with_secondary(true, Some("unsloth"), |_| false));
        // Only secondary present, primary missing.
        assert!(!installed_with_secondary(false, Some("unsloth"), |_| true));
        // Neither present.
        assert!(!installed_with_secondary(false, Some("unsloth"), |_| false));
    }

    #[test]
    fn not_installed_detail_without_secondary_names_the_primary() {
        assert_eq!(
            not_installed_detail("codex", false, None),
            "codex not on PATH"
        );
    }

    #[test]
    fn not_installed_detail_attributes_the_actually_missing_binary() {
        // Secondary (unsloth CLI) missing, primary (opencode) found — must
        // name the unsloth CLI, not opencode (monorepo#935).
        assert_eq!(
            not_installed_detail("opencode", true, Some(("unsloth", false))),
            "unsloth not on PATH; opencode found"
        );
        // Primary missing, secondary found.
        assert_eq!(
            not_installed_detail("opencode", false, Some(("unsloth", true))),
            "opencode not on PATH; unsloth found"
        );
        // Both missing.
        assert_eq!(
            not_installed_detail("opencode", false, Some(("unsloth", false))),
            "opencode and unsloth not on PATH"
        );
        // Inconsistent input (both resolved) must never claim "not on PATH".
        assert_eq!(
            not_installed_detail("opencode", true, Some(("unsloth", true))),
            "opencode and unsloth found, but provider reported not installed"
        );
    }

    #[test]
    fn unsloth_registry_entry_requires_the_unsloth_cli_secondary_binary() {
        let unsloth = crate::config::ACP_PROVIDERS
            .iter()
            .find(|p| p.id == "unsloth")
            .expect("unsloth must be registered");
        assert_eq!(
            unsloth.command, "opencode",
            "unsloth rides opencode's ACP runtime"
        );
        assert_eq!(
            unsloth.requires_secondary_binary,
            Some("unsloth"),
            "unsloth availability must additionally require the unsloth CLI"
        );
    }

    /// The `providers.paths` override key for a provider's primary binary is
    /// the provider that OWNS it: unsloth rides opencode, so its primary
    /// resolves under the `opencode` key; every other provider owns its own.
    #[test]
    fn primary_binary_provider_id_retargets_only_unsloth() {
        for provider in crate::config::ACP_PROVIDERS {
            let expected = if provider.id == "unsloth" {
                "opencode"
            } else {
                provider.id
            };
            assert_eq!(provider.primary_binary_provider_id(), expected);
        }
    }

    #[test]
    fn discover_providers_unsloth_installed_iff_command_field_present() {
        // Regardless of the real host's installed binaries, the discovery
        // snapshot's `installed` flag for unsloth must be consistent with
        // `installed_with_secondary`'s semantics: it can only be `true` when
        // both the primary (opencode) and secondary (unsloth CLI) resolved.
        let providers = discover_providers();
        let unsloth = providers
            .iter()
            .find(|p| p.id == "unsloth")
            .expect("unsloth must be in the discovery snapshot");
        if unsloth.installed {
            assert!(
                unsloth.resolved_path.is_some(),
                "installed=true requires a resolved opencode path"
            );
            assert!(
                find_provider_binary("unsloth", "unsloth", None).is_some(),
                "installed=true requires the unsloth CLI to also resolve"
            );
        }
        // The snapshot must always carry the secondary-binary status for
        // unsloth (doctor's attribution input), consistent with a direct
        // resolution of the unsloth CLI — including the resolved path itself.
        let secondary = unsloth
            .secondary_binary
            .as_ref()
            .expect("unsloth must report its secondary-binary status");
        assert_eq!(secondary.command, "unsloth");
        assert_eq!(
            secondary.resolved_path,
            find_provider_binary("unsloth", "unsloth", None)
        );
        // Override-free discovery: `resolved` mirrors the auto-detection.
        assert_eq!(secondary.resolved, secondary.resolved_path.is_some());
    }

    #[test]
    fn managed_binary_path_returns_expected_location() {
        if let Some(home) = home_dir() {
            let result = managed_binary_path("auggie");
            let expected = if cfg!(windows) {
                home.join(".augment").join("bin").join("auggie.exe")
            } else {
                home.join(".augment").join("bin").join("auggie")
            };
            assert_eq!(result, Some(expected));
        }
    }
}

/// Regression tests for override-aware `installed` (monorepo#1065): a valid
/// `providers.paths` override must flip `installed` / the secondary's
/// `resolved` to match what `resolve_spawn` would actually spawn, WITHOUT
/// touching the auto-detected `resolved_path` fields; an invalid override
/// must contribute nothing. Driven through [`availability_for`]'s injected
/// resolvers so every override/auto combination is deterministic (no real
/// filesystem / `PATH` / `HOME` dependence).
#[cfg(test)]
mod override_aware_discovery_tests {
    use super::*;
    use std::fs;

    /// A fresh RAII temp directory for `tag` under the system temp root. The
    /// returned guard removes the dir on drop (including on panic); set
    /// `INTENTD_TEST_KEEP_TMP` (non-empty) to keep it around for debugging.
    fn unique_temp_dir(tag: &str) -> tempfile::TempDir {
        let mut dir = tempfile::Builder::new()
            .prefix(&format!("intent-providers-{tag}-"))
            .tempdir()
            .expect("create test temp dir");
        if std::env::var_os("INTENTD_TEST_KEEP_TMP").is_some_and(|v| !v.is_empty()) {
            dir.disable_cleanup(true);
        }
        dir
    }

    #[cfg(unix)]
    fn make_executable(path: &std::path::Path) {
        use std::os::unix::fs::PermissionsExt;
        fs::write(path, "#!/bin/sh\nexit 0\n").unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
    }

    #[cfg(not(unix))]
    fn make_executable(path: &std::path::Path) {
        fs::write(path, "exit 0").unwrap();
    }

    fn unsloth_config() -> &'static ProviderConfig {
        crate::config::ACP_PROVIDERS
            .iter()
            .find(|p| p.id == "unsloth")
            .expect("unsloth must be registered")
    }

    fn auggie_config() -> &'static ProviderConfig {
        crate::config::ACP_PROVIDERS
            .iter()
            .find(|p| p.id == "auggie")
            .expect("auggie must be registered")
    }

    fn claude_code_config() -> &'static ProviderConfig {
        crate::config::ACP_PROVIDERS
            .iter()
            .find(|p| p.id == "claude-code")
            .expect("claude-code must be registered")
    }

    fn pi_config() -> &'static ProviderConfig {
        crate::config::ACP_PROVIDERS
            .iter()
            .find(|p| p.id == "pi")
            .expect("pi must be registered")
    }

    /// monorepo#4352: a valid `providers.paths["claude-code"]` override
    /// resolves to the adapter binary the spawn will exec in place of the
    /// pinned npx package; absent / blank / relative / missing /
    /// non-executable values contribute nothing (pinned npx applies).
    #[test]
    fn resolve_npx_only_override_accepts_only_absolute_executables() {
        let dir = unique_temp_dir("npx-only-override");
        let adapter = dir.path().join("claude-agent-acp");
        make_executable(&adapter);
        let claude = claude_code_config();
        assert_eq!(
            resolve_npx_only_override(claude, Some(adapter.to_str().unwrap())),
            Some(adapter.clone()),
            "valid absolute executable resolves"
        );
        assert_eq!(resolve_npx_only_override(claude, None), None);
        assert_eq!(resolve_npx_only_override(claude, Some("   ")), None);
        assert_eq!(
            resolve_npx_only_override(claude, Some("relative/claude-agent-acp")),
            None,
            "relative paths are rejected"
        );
        let missing = dir.path().join("missing");
        assert_eq!(
            resolve_npx_only_override(claude, Some(missing.to_str().unwrap())),
            None,
            "missing files are rejected"
        );
        #[cfg(unix)]
        {
            let not_exec = dir.path().join("not-exec");
            fs::write(&not_exec, "x").unwrap();
            assert_eq!(
                resolve_npx_only_override(claude, Some(not_exec.to_str().unwrap())),
                None,
                "non-executable files are rejected"
            );
        }
    }

    /// pi does not opt in (`npx_only_honors_path_override` is false): even a
    /// valid override resolves to nothing, so every surface keeps the pinned
    /// npx spawn and discovery never advertises an `installed` pi that the
    /// real `pi` CLI gate would still reject.
    #[test]
    fn pi_override_is_ignored_on_every_surface() {
        let dir = unique_temp_dir("npx-only-override-pi");
        let adapter = dir.path().join("pi-acp");
        make_executable(&adapter);
        assert!(!pi_config().npx_only_honors_path_override);
        assert_eq!(
            resolve_npx_only_override(pi_config(), Some(adapter.to_str().unwrap())),
            None,
            "pi keeps pinned-npx-only semantics"
        );
        let overrides = |key: &str| (key == "pi").then(|| adapter.display().to_string());
        let availability = availability_for(pi_config(), None, &|_, _| None, &overrides);
        assert_eq!(
            availability.installed,
            availability.resolved_path.is_some(),
            "a pi override must not flip installed"
        );
    }

    /// monorepo#4352: discovery honors the claude-code override for the
    /// `installed` flag (so the FE sees the provider available whenever the
    /// spawn would succeed) while `resolved_path` stays the auto-detected
    /// npx — never the override path.
    #[test]
    fn valid_claude_code_override_flips_installed_without_touching_paths() {
        let dir = unique_temp_dir("override-claude-code");
        let adapter = dir.path().join("claude-agent-acp");
        make_executable(&adapter);
        let overrides = |key: &str| (key == "claude-code").then(|| adapter.display().to_string());
        let availability = availability_for(claude_code_config(), None, &|_, _| None, &overrides);
        assert!(availability.installed, "valid override must flip installed");
        assert_ne!(availability.resolved_path.as_ref(), Some(&adapter));
        assert_eq!(
            availability.npx_only_package,
            Some(crate::config::CLAUDE_AGENT_ACP_NPX_PACKAGE)
        );
    }

    /// An invalid claude-code override contributes nothing: `installed`
    /// tracks npx auto-detection exactly as before.
    #[test]
    fn invalid_claude_code_override_does_not_flip_installed() {
        let dir = unique_temp_dir("override-claude-code-invalid");
        let missing = dir.path().join("missing");
        let overrides = |key: &str| (key == "claude-code").then(|| missing.display().to_string());
        let availability = availability_for(claude_code_config(), None, &|_, _| None, &overrides);
        assert_eq!(availability.installed, availability.resolved_path.is_some());
    }

    /// monorepo#1065: with nothing auto-detected, valid overrides for both
    /// unsloth keys (`opencode` → primary, `unsloth` → the CLI) make
    /// `installed` / `resolved` true while both path fields stay absent.
    #[test]
    fn valid_unsloth_override_flips_installed_without_touching_paths() {
        let dir = unique_temp_dir("override-valid");
        let opencode = dir.path().join("opencode");
        let unsloth = dir.path().join("unsloth");
        make_executable(&opencode);
        make_executable(&unsloth);
        let overrides = |key: &str| match key {
            "opencode" => Some(opencode.display().to_string()),
            "unsloth" => Some(unsloth.display().to_string()),
            _ => None,
        };
        let availability = availability_for(unsloth_config(), None, &|_, _| None, &overrides);
        assert!(
            availability.installed,
            "valid overrides must flip installed"
        );
        assert_eq!(
            availability.resolved_path, None,
            "resolvedPath must stay auto-detected (absent here)"
        );
        let secondary = availability.secondary_binary.expect("secondary status");
        assert_eq!(secondary.command, "unsloth");
        assert!(secondary.resolved, "valid override must flip resolved");
        assert_eq!(
            secondary.resolved_path, None,
            "secondaryResolvedPath must stay auto-detected (absent here)"
        );
    }

    /// An invalid override (missing file / relative path) must not flip
    /// `installed` — the managed server would not start from it either.
    #[test]
    fn invalid_unsloth_override_does_not_flip_installed() {
        for bad in ["/nonexistent/override/unsloth", "relative/unsloth"] {
            let overrides = |key: &str| match key {
                "opencode" | "unsloth" => Some(bad.to_string()),
                _ => None,
            };
            let availability = availability_for(unsloth_config(), None, &|_, _| None, &overrides);
            assert!(
                !availability.installed,
                "invalid override {bad} must not flip installed"
            );
            let secondary = availability.secondary_binary.expect("secondary status");
            assert!(
                !secondary.resolved,
                "invalid override {bad} must not flip resolved"
            );
        }
    }

    /// Override + auto-detection coexistence: the auto-detected paths keep
    /// reporting on the wire-visible path fields (never the override path),
    /// and `installed` stays true.
    #[test]
    fn override_and_auto_detection_coexist_with_auto_paths_reported() {
        let dir = unique_temp_dir("override-coexist");
        let override_bin = dir.path().join("unsloth-override");
        make_executable(&override_bin);
        let auto_opencode = dir.path().join("auto").join("opencode");
        let auto_unsloth = dir.path().join("auto").join("unsloth");
        let resolve_auto = |_: &str, cmd: &str| match cmd {
            "opencode" => Some(auto_opencode.clone()),
            "unsloth" => Some(auto_unsloth.clone()),
            _ => None,
        };
        let overrides = |key: &str| match key {
            "opencode" | "unsloth" => Some(override_bin.display().to_string()),
            _ => None,
        };
        let availability = availability_for(unsloth_config(), None, &resolve_auto, &overrides);
        assert!(availability.installed);
        assert_eq!(
            availability.resolved_path,
            Some(auto_opencode),
            "resolvedPath must report the auto-detected path, not the override"
        );
        let secondary = availability.secondary_binary.expect("secondary status");
        assert!(secondary.resolved);
        assert_eq!(
            secondary.resolved_path,
            Some(auto_unsloth),
            "secondaryResolvedPath must report the auto-detected path, not the override"
        );
    }

    /// The same mechanism applies to single-binary providers: a valid
    /// `providers.paths["auggie"]` override with the binary not auto-detected
    /// reports installed, and the primary key follows
    /// `primary_binary_provider_id` (matching `resolve_spawn`).
    #[test]
    fn valid_primary_override_flips_installed_for_single_binary_provider() {
        let dir = unique_temp_dir("override-auggie");
        let auggie = dir.path().join("auggie");
        make_executable(&auggie);
        let overrides = |key: &str| (key == "auggie").then(|| auggie.display().to_string());
        let availability = availability_for(auggie_config(), None, &|_, _| None, &overrides);
        assert!(availability.installed);
        assert_eq!(availability.resolved_path, None);
        assert_eq!(availability.secondary_binary, None);
    }

    /// Gated providers ignore overrides entirely (never probed).
    #[test]
    fn gated_provider_ignores_overrides() {
        let dir = unique_temp_dir("override-gated");
        let bin = dir.path().join("unsloth");
        make_executable(&bin);
        let overrides = |_: &str| Some(bin.display().to_string());
        let availability = availability_for(
            unsloth_config(),
            Some("requires env var TEST".to_string()),
            &|_, _| None,
            &overrides,
        );
        assert!(!availability.installed);
        assert_eq!(availability.secondary_binary, None);
    }

    /// The public entry point threads the lookup through: with valid
    /// overrides for both unsloth keys, the full-registry discovery reports
    /// unsloth installed regardless of the host's real binaries.
    #[test]
    fn discover_providers_with_overrides_reports_unsloth_installed() {
        let dir = unique_temp_dir("override-e2e");
        let opencode = dir.path().join("opencode");
        let unsloth = dir.path().join("unsloth");
        make_executable(&opencode);
        make_executable(&unsloth);
        let overrides = |key: &str| match key {
            "opencode" => Some(opencode.display().to_string()),
            "unsloth" => Some(unsloth.display().to_string()),
            _ => None,
        };
        let providers = discover_providers_with_overrides(&overrides);
        let u = providers
            .iter()
            .find(|p| p.id == "unsloth")
            .expect("unsloth in snapshot");
        assert!(u.installed);
        let secondary = u.secondary_binary.as_ref().expect("secondary status");
        assert!(secondary.resolved);
        // Path fields stay auto-detected: never the override paths.
        assert_ne!(u.resolved_path.as_ref(), Some(&opencode));
        assert_ne!(secondary.resolved_path.as_ref(), Some(&unsloth));
    }
}

/// Windows executable-resolution semantics (monorepo#1054): Windows must
/// prefer runnable entry points (`.exe`/`.cmd`/`.bat`) and never resolve a
/// bare extensionless file (`CreateProcess` cannot run it) — the npm shim
/// pair `auggie` + `auggie.cmd` must resolve the `.cmd` shim. Windows CI is
/// disabled, so both platform arms are driven through the `_for` seams on
/// POSIX; POSIX behavior stays byte-identical.
#[cfg(test)]
mod windows_resolution_tests {
    use super::*;
    use std::fs;

    /// A fresh RAII temp directory for `tag` under the system temp root. The
    /// returned guard removes the dir on drop (including on panic); set
    /// `INTENTD_TEST_KEEP_TMP` (non-empty) to keep it around for debugging.
    fn unique_temp_dir(tag: &str) -> tempfile::TempDir {
        let mut dir = tempfile::Builder::new()
            .prefix(&format!("intent-providers-{tag}-"))
            .tempdir()
            .expect("create test temp dir");
        if std::env::var_os("INTENTD_TEST_KEEP_TMP").is_some_and(|v| !v.is_empty()) {
            dir.disable_cleanup(true);
        }
        dir
    }

    #[test]
    fn name_candidates_posix_is_bare_name_only() {
        assert_eq!(name_candidates_for("auggie", false), vec!["auggie"]);
        assert_eq!(name_candidates_for("auggie.exe", false), vec!["auggie.exe"]);
    }

    #[test]
    fn name_candidates_windows_prefers_executable_extensions_over_bare_name() {
        assert_eq!(
            name_candidates_for("auggie", true),
            vec!["auggie.exe", "auggie.cmd", "auggie.bat"],
            "the bare extensionless name must not be a candidate on Windows"
        );
    }

    #[test]
    fn name_candidates_windows_keeps_command_carrying_executable_extension() {
        assert_eq!(name_candidates_for("auggie.cmd", true), vec!["auggie.cmd"]);
        // Case-insensitive, matching Windows filename semantics.
        assert_eq!(name_candidates_for("AUGGIE.EXE", true), vec!["AUGGIE.EXE"]);
    }

    #[test]
    fn name_candidates_windows_suffixes_non_executable_extension() {
        // A non-runnable extension is not an entry point; it still gets the
        // runnable suffixes appended like an extensionless command.
        assert_eq!(
            name_candidates_for("foo.py", true),
            vec!["foo.py.exe", "foo.py.cmd", "foo.py.bat"]
        );
    }

    #[test]
    fn is_executable_file_windows_requires_runnable_extension() {
        let dir = unique_temp_dir("win-exec");
        let bare = dir.path().join("auggie");
        let cmd = dir.path().join("auggie.cmd");
        let exe_upper = dir.path().join("tool.EXE");
        fs::write(&bare, "#!/bin/sh\nexit 0\n").unwrap();
        fs::write(&cmd, "@echo off\r\n").unwrap();
        fs::write(&exe_upper, "MZ").unwrap();
        assert!(
            !is_executable_file_for(&bare, true),
            "an extensionless file is not runnable on Windows"
        );
        assert!(is_executable_file_for(&cmd, true));
        assert!(
            is_executable_file_for(&exe_upper, true),
            "extension matching must be case-insensitive"
        );
        assert!(!is_executable_file_for(
            &dir.path().join("missing.exe"),
            true
        ));
        assert!(
            !is_executable_file_for(dir.path(), true),
            "directories never resolve"
        );
    }

    #[test]
    fn find_in_dirs_windows_npm_shim_pair_resolves_the_cmd_shim() {
        // Regression: npm installs the extensionless POSIX script `auggie`
        // next to the runnable `auggie.cmd` shim in the same dir; the .cmd
        // shim must win (the bare file used to be resolved first).
        let dir = unique_temp_dir("win-shim-pair");
        fs::write(dir.path().join("auggie"), "#!/bin/sh\nexit 0\n").unwrap();
        let cmd = dir.path().join("auggie.cmd");
        fs::write(&cmd, "@echo off\r\n").unwrap();
        assert_eq!(
            find_in_dirs_for(&[dir.path().to_path_buf()], "auggie", true),
            Some(cmd)
        );
    }

    #[test]
    fn find_in_dirs_windows_extensionless_file_alone_does_not_resolve() {
        let dir = unique_temp_dir("win-bare-only");
        fs::write(dir.path().join("auggie"), "#!/bin/sh\nexit 0\n").unwrap();
        assert_eq!(
            find_in_dirs_for(&[dir.path().to_path_buf()], "auggie", true),
            None,
            "CreateProcess cannot run a bare extensionless file"
        );
    }

    #[test]
    fn find_in_dirs_windows_resolves_command_carrying_executable_extension() {
        let dir = unique_temp_dir("win-explicit-ext");
        let cmd = dir.path().join("auggie.cmd");
        fs::write(&cmd, "@echo off\r\n").unwrap();
        assert_eq!(
            find_in_dirs_for(&[dir.path().to_path_buf()], "auggie.cmd", true),
            Some(cmd)
        );
    }

    #[cfg(unix)]
    #[test]
    fn find_in_dirs_posix_still_resolves_the_bare_executable() {
        // POSIX behavior stays byte-identical: the bare name resolves once
        // executable, and no extension variants are probed.
        use std::os::unix::fs::PermissionsExt;
        let dir = unique_temp_dir("posix-bare");
        let bin = dir.path().join("auggie");
        fs::write(&bin, "#!/bin/sh\nexit 0\n").unwrap();
        fs::set_permissions(&bin, fs::Permissions::from_mode(0o755)).unwrap();
        fs::write(dir.path().join("auggie.cmd"), "@echo off\r\n").unwrap();
        assert_eq!(
            find_in_dirs_for(&[dir.path().to_path_buf()], "auggie", false),
            Some(bin)
        );
    }
}
