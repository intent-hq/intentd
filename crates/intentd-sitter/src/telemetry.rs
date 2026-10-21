//! Anonymous update-check telemetry: the `updates.intentapp.dev` endpoint.
//!
//! When telemetry is on, the manifest fetch goes through
//! `https://updates.intentapp.dev/channel/<ch>.json?p=&v=&src=&k=&d=` first.
//! The endpoint counts the check and answers with a 302 to the fixed GitHub
//! channel asset; GitHub stays the only source of manifests:
//! [`check_redirect`] accepts only the official asset URL for the same
//! channel, then GitHub's own asset hosts, and a response that was not
//! redirected at all is refused. Any failure falls back to the GitHub bases.
//!
//! Only non-identifying values are sent: the build's target triple, the
//! sitter version, the install source, the check trigger, and whether this
//! is the first check of the UTC day (from a date in `state.json`, never an
//! identifier).
//!
//! Opt-out ([`opted_out`]): `DO_NOT_TRACK=1`, `INTENTD_DISABLE_TELEMETRY=1`,
//! or `telemetry = false` in `<data_dir>/sitter/config.toml`. The
//! `INTENTD_SITTER_MANIFEST_BASE_URL` override also skips the endpoint.

use std::ffi::OsString;
use std::fmt;
use std::path::Path;
use std::time::Duration;

use reqwest::Url;
use time::OffsetDateTime;

use crate::cli::Channel;
use crate::config;

/// Production update-check endpoint (its own Vercel project).
pub const UPDATE_ENDPOINT_BASE_URL: &str = "https://updates.intentapp.dev";

/// Total timeout for the endpoint hop plus the GitHub redirect it hands
/// back. Shorter than the plain manifest timeout: a slow endpoint must not
/// delay the GitHub fallback for long.
pub const UPDATE_ENDPOINT_TIMEOUT: Duration = Duration::from_secs(10);

/// Most redirects a manifest fetch through the endpoint may follow.
pub const MAX_REDIRECT_HOPS: usize = 5;

/// Repos whose `channel-<ch>` release may host the manifest.
const MANIFEST_REPOS: &[&str] = &["intentd-releases", "intentd"];

/// Env opt-outs (any truthy value: `1`, `true`, `yes`, `on`).
pub const DO_NOT_TRACK_ENV: &str = "DO_NOT_TRACK";
pub const DISABLE_TELEMETRY_ENV: &str = "INTENTD_DISABLE_TELEMETRY";

/// This sitter's version, sent as `v`.
pub const SITTER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// `User-Agent` for every sitter request: `intentd-sitter/<ver> (<triple>)`.
#[must_use]
pub fn user_agent() -> String {
    format!(
        "intentd-sitter/{SITTER_VERSION} ({})",
        crate::manifest::TARGET_TRIPLE
    )
}

/// What started an update check (`k`). Kept an enum so the trigger set can
/// change in one place.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckTrigger {
    /// The randomized periodic timer (also the startup and failed-start
    /// re-checks the supervisor runs on its own).
    Periodic,
    /// SIGUSR2: the daemon's idle-time check.
    Idle,
    /// `intentd update`, `intentd sitter channel --redownload`, or SIGUSR1
    /// (`system.requestUpdate`).
    Manual,
}

impl CheckTrigger {
    /// Wire value for `k`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Periodic => "periodic",
            Self::Idle => "idle",
            Self::Manual => "manual",
        }
    }
}

impl fmt::Display for CheckTrigger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// How this sitter was installed (`src`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstallSource {
    Brew,
    Deb,
    Script,
    Windows,
    Unknown,
}

impl InstallSource {
    /// Wire value for `src`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Brew => "brew",
            Self::Deb => "deb",
            Self::Script => "script",
            Self::Windows => "windows",
            Self::Unknown => "unknown",
        }
    }

    /// Detect the install source of the running executable (symlinks
    /// resolved, so a `/usr/local/bin` link into the Cellar reads as brew).
    #[must_use]
    pub fn detect() -> Self {
        let exe = std::env::current_exe()
            .ok()
            .map(|exe| std::fs::canonicalize(&exe).unwrap_or(exe));
        let Some(exe) = exe else {
            return Self::Unknown;
        };
        let home = std::env::var_os("HOME").map(std::path::PathBuf::from);
        detect_install_source(
            &exe,
            home.as_deref(),
            cfg!(windows),
            Path::new(DPKG_MARKER).exists(),
        )
    }
}

impl fmt::Display for InstallSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// dpkg's file list for the `intentd` package: present when the `.deb`
/// owns `/usr/bin/intentd`.
pub const DPKG_MARKER: &str = "/var/lib/dpkg/info/intentd.list";

/// Classify `exe` (already canonicalized). Pure, for tests.
#[must_use]
pub fn detect_install_source(
    exe: &Path,
    home: Option<&Path>,
    windows: bool,
    dpkg_marker: bool,
) -> InstallSource {
    if windows {
        return InstallSource::Windows;
    }
    let s = exe.to_string_lossy();
    if s.contains("/Cellar/")
        || s.starts_with("/opt/homebrew/")
        || s.starts_with("/home/linuxbrew/.linuxbrew/")
    {
        return InstallSource::Brew;
    }
    if exe == Path::new("/usr/bin/intentd") {
        return if dpkg_marker {
            InstallSource::Deb
        } else {
            InstallSource::Unknown
        };
    }
    if exe == Path::new("/usr/local/bin/intentd")
        || home.is_some_and(|home| exe == home.join(".local/bin/intentd"))
    {
        return InstallSource::Script;
    }
    InstallSource::Unknown
}

/// Whether `value` reads as an enabled boolean flag.
fn truthy(value: &OsString) -> bool {
    matches!(
        value.to_string_lossy().trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// Whether telemetry is switched off by env (`env` looks a variable up) or
/// by `telemetry = false` in `config_path`.
pub fn opted_out(env: impl Fn(&str) -> Option<OsString>, config_path: &Path) -> bool {
    [DO_NOT_TRACK_ENV, DISABLE_TELEMETRY_ENV]
        .iter()
        .any(|name| env(name).is_some_and(|v| truthy(&v)))
        || config::load_telemetry(config_path) == Some(false)
}

/// UTC calendar day of `now` as `YYYY-MM-DD` (the `last_check_day` value).
#[must_use]
pub fn utc_day(now: OffsetDateTime) -> String {
    let date = now.to_offset(time::UtcOffset::UTC).date();
    format!(
        "{:04}-{:02}-{:02}",
        date.year(),
        u8::from(date.month()),
        date.day()
    )
}

/// Whether a check at `now` is the first of its UTC day (`d=1`).
#[must_use]
pub fn is_first_check_of_day(last_check_day: Option<&str>, now: OffsetDateTime) -> bool {
    last_check_day != Some(utc_day(now).as_str())
}

/// Query values for one update check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckParams<'a> {
    pub platform: &'a str,
    pub version: &'a str,
    pub source: InstallSource,
    pub trigger: CheckTrigger,
    pub first_of_day: bool,
}

/// `<base>/channel/<ch>.json?p=&v=&src=&k=&d=`, query values URL-encoded.
///
/// # Errors
/// Returns the parse error when `base_url` is not a valid URL.
pub fn update_check_url(
    base_url: &str,
    channel: Channel,
    params: &CheckParams<'_>,
) -> Result<Url, UrlParseError> {
    let mut url = Url::parse(&format!(
        "{}/channel/{channel}.json",
        base_url.trim_end_matches('/')
    ))?;
    url.query_pairs_mut()
        .append_pair("p", params.platform)
        .append_pair("v", params.version)
        .append_pair("src", params.source.as_str())
        .append_pair("k", params.trigger.as_str())
        .append_pair("d", if params.first_of_day { "1" } else { "0" });
    Ok(url)
}

/// `Url::parse` error (`reqwest` re-exports `Url` but not this type).
pub type UrlParseError = <Url as std::str::FromStr>::Err;

/// Why a redirect from the update endpoint was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RedirectRejected {
    #[error("more than {MAX_REDIRECT_HOPS} redirects")]
    TooManyHops,
    #[error("update endpoint redirected to {0}, not the official {1} channel manifest")]
    NotChannelManifest(String, Channel),
    #[error("redirect to {0} is not a GitHub release asset host")]
    NotAssetHost(String),
}

/// No userinfo, default port, https.
fn plain_https(url: &Url) -> bool {
    url.scheme() == "https"
        && url.username().is_empty()
        && url.password().is_none()
        && url.port().is_none()
}

/// Exactly `https://github.com/intent-hq/<repo>/releases/download/channel-<ch>/<ch>.json`.
#[must_use]
pub fn is_channel_manifest_url(url: &Url, channel: Channel) -> bool {
    plain_https(url)
        && url.host_str() == Some("github.com")
        && url.query().is_none()
        && url.fragment().is_none()
        && MANIFEST_REPOS.iter().any(|repo| {
            url.path()
                == format!("/intent-hq/{repo}/releases/download/channel-{channel}/{channel}.json")
        })
}

/// An https URL on a `*.githubusercontent.com` host.
#[must_use]
pub fn is_github_asset_url(url: &Url) -> bool {
    plain_https(url)
        && url.domain().is_some_and(|domain| {
            domain
                .strip_suffix(".githubusercontent.com")
                .is_some_and(|label| !label.is_empty())
        })
}

/// Redirect allowlist for a fetch through the update endpoint. `previous`
/// is every URL visited so far (the endpoint URL first); `next` is where
/// the server wants to send us. The first hop must be the official channel
/// asset for `channel`; later hops must stay on GitHub's asset hosts.
///
/// # Errors
/// Returns why `next` is refused.
pub fn check_redirect(
    channel: Channel,
    previous: &[Url],
    next: &Url,
) -> Result<(), RedirectRejected> {
    let hop = previous.len();
    if hop > MAX_REDIRECT_HOPS {
        return Err(RedirectRejected::TooManyHops);
    }
    if hop <= 1 {
        if is_channel_manifest_url(next, channel) {
            return Ok(());
        }
        return Err(RedirectRejected::NotChannelManifest(
            next.to_string(),
            channel,
        ));
    }
    if is_github_asset_url(next) {
        Ok(())
    } else {
        Err(RedirectRejected::NotAssetHost(next.to_string()))
    }
}

/// `reqwest` redirect policy wrapping [`check_redirect`].
#[must_use]
pub fn redirect_policy(channel: Channel) -> reqwest::redirect::Policy {
    reqwest::redirect::Policy::custom(move |attempt| {
        match check_redirect(channel, attempt.previous(), attempt.url()) {
            Ok(()) => attempt.follow(),
            Err(rejected) => attempt.error(rejected),
        }
    })
}

/// Whether the final URL of an endpoint fetch is an allowed GitHub source:
/// a response that never left the endpoint is refused.
#[must_use]
pub fn is_allowed_final_url(url: &Url, channel: Channel) -> bool {
    is_channel_manifest_url(url, channel) || is_github_asset_url(url)
}

/// Where and how the updater reaches the update-check endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateEndpoint {
    /// Endpoint base URL ([`UPDATE_ENDPOINT_BASE_URL`] in production).
    pub base_url: String,
    /// Sent as `src`.
    pub install_source: InstallSource,
    /// Total timeout for the endpoint fetch.
    pub timeout: Duration,
}

impl UpdateEndpoint {
    /// The production endpoint with the detected install source.
    #[must_use]
    pub fn production() -> Self {
        Self {
            base_url: UPDATE_ENDPOINT_BASE_URL.to_string(),
            install_source: InstallSource::detect(),
            timeout: UPDATE_ENDPOINT_TIMEOUT,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use time::macros::datetime;

    fn url(s: &str) -> Url {
        Url::parse(s).unwrap()
    }

    const ENDPOINT: &str =
        "https://updates.intentapp.dev/channel/stable.json?p=x&v=1&src=brew&k=periodic&d=0";
    const STABLE_RELEASES: &str =
        "https://github.com/intent-hq/intentd-releases/releases/download/channel-stable/stable.json";
    const STABLE_INTENTD: &str =
        "https://github.com/intent-hq/intentd/releases/download/channel-stable/stable.json";
    const ASSET: &str =
        "https://release-assets.githubusercontent.com/github-production-release-asset/1/2?sp=r&sig=x";

    // ---- redirect allowlist (security-critical) ----

    #[test]
    fn allowed_chain_passes() {
        let mut previous = vec![url(ENDPOINT)];
        assert_eq!(
            check_redirect(Channel::Stable, &previous, &url(STABLE_RELEASES)),
            Ok(())
        );
        previous.push(url(STABLE_RELEASES));
        assert_eq!(
            check_redirect(Channel::Stable, &previous, &url(ASSET)),
            Ok(())
        );
        previous.push(url(ASSET));
        assert_eq!(
            check_redirect(
                Channel::Stable,
                &previous,
                &url("https://objects.githubusercontent.com/x")
            ),
            Ok(())
        );
    }

    #[test]
    fn both_manifest_repos_are_allowed_for_every_channel() {
        for channel in [Channel::Stable, Channel::Beta, Channel::Alpha] {
            for repo in ["intentd-releases", "intentd"] {
                let target = url(&format!(
                    "https://github.com/intent-hq/{repo}/releases/download/channel-{channel}/{channel}.json"
                ));
                assert_eq!(
                    check_redirect(channel, &[url(ENDPOINT)], &target),
                    Ok(()),
                    "{target}"
                );
            }
        }
        assert_eq!(
            check_redirect(Channel::Stable, &[url(ENDPOINT)], &url(STABLE_INTENTD)),
            Ok(())
        );
    }

    #[test]
    fn first_hop_to_another_repo_is_rejected() {
        for target in [
            "https://github.com/intent-hq/cloudlands-releases/releases/download/channel-stable/stable.json",
            "https://github.com/evil/intentd-releases/releases/download/channel-stable/stable.json",
            "https://github.com/intent-hq/intentd-releases-evil/releases/download/channel-stable/stable.json",
            "https://github.com/intent-hq/intentd/releases/download/v9.9.9/stable.json",
        ] {
            assert!(
                matches!(
                    check_redirect(Channel::Stable, &[url(ENDPOINT)], &url(target)),
                    Err(RedirectRejected::NotChannelManifest(..))
                ),
                "{target}"
            );
        }
    }

    #[test]
    fn first_hop_to_another_channel_is_rejected() {
        for target in [
            "https://github.com/intent-hq/intentd-releases/releases/download/channel-alpha/alpha.json",
            "https://github.com/intent-hq/intentd-releases/releases/download/channel-stable/alpha.json",
            "https://github.com/intent-hq/intentd-releases/releases/download/channel-alpha/stable.json",
        ] {
            assert!(
                check_redirect(Channel::Stable, &[url(ENDPOINT)], &url(target)).is_err(),
                "{target}"
            );
        }
    }

    #[test]
    fn first_hop_to_another_host_or_shape_is_rejected() {
        for target in [
            "https://evil.example/intent-hq/intentd-releases/releases/download/channel-stable/stable.json",
            "https://github.com.evil.example/intent-hq/intentd-releases/releases/download/channel-stable/stable.json",
            "http://github.com/intent-hq/intentd-releases/releases/download/channel-stable/stable.json",
            "https://github.com:8443/intent-hq/intentd-releases/releases/download/channel-stable/stable.json",
            "https://user@github.com/intent-hq/intentd-releases/releases/download/channel-stable/stable.json",
            "https://github.com/intent-hq/intentd-releases/releases/download/channel-stable/stable.json?x=1",
            "https://github.com/intent-hq/intentd-releases/releases/download/channel-stable/stable.json#f",
            "https://updates.intentapp.dev/elsewhere.json",
            // An asset host is only allowed after the GitHub hop.
            ASSET,
        ] {
            assert!(
                check_redirect(Channel::Stable, &[url(ENDPOINT)], &url(target)).is_err(),
                "{target}"
            );
        }
    }

    #[test]
    fn later_hops_must_stay_on_github_asset_hosts() {
        let previous = [url(ENDPOINT), url(STABLE_RELEASES)];
        for target in [
            "https://evil.example/x",
            "https://githubusercontent.com/x",
            "https://githubusercontent.com.evil.example/x",
            "https://evilgithubusercontent.com/x",
            "http://objects.githubusercontent.com/x",
            "https://objects.githubusercontent.com:444/x",
            "https://github.com/intent-hq/intentd-releases/releases/download/channel-stable/stable.json",
            "https://updates.intentapp.dev/channel/stable.json",
        ] {
            assert!(
                matches!(
                    check_redirect(Channel::Stable, &previous, &url(target)),
                    Err(RedirectRejected::NotAssetHost(_))
                ),
                "{target}"
            );
        }
    }

    #[test]
    fn too_many_hops_are_rejected() {
        let mut previous = vec![url(ENDPOINT), url(STABLE_RELEASES)];
        while previous.len() < MAX_REDIRECT_HOPS {
            previous.push(url(ASSET));
        }
        assert_eq!(
            check_redirect(Channel::Stable, &previous, &url(ASSET)),
            Ok(())
        );
        previous.push(url(ASSET));
        assert_eq!(
            check_redirect(Channel::Stable, &previous, &url(ASSET)),
            Err(RedirectRejected::TooManyHops)
        );
    }

    #[test]
    fn final_url_must_have_left_the_endpoint() {
        assert!(!is_allowed_final_url(&url(ENDPOINT), Channel::Stable));
        assert!(is_allowed_final_url(&url(STABLE_RELEASES), Channel::Stable));
        assert!(is_allowed_final_url(&url(ASSET), Channel::Stable));
        assert!(!is_allowed_final_url(&url(STABLE_RELEASES), Channel::Beta));
    }

    // ---- URL + query ----

    #[test]
    fn builds_url_and_query() {
        let params = CheckParams {
            platform: "aarch64-apple-darwin",
            version: "0.1.28",
            source: InstallSource::Brew,
            trigger: CheckTrigger::Periodic,
            first_of_day: true,
        };
        assert_eq!(
            update_check_url("https://updates.intentapp.dev/", Channel::Stable, &params)
                .unwrap()
                .as_str(),
            "https://updates.intentapp.dev/channel/stable.json?p=aarch64-apple-darwin&v=0.1.28&src=brew&k=periodic&d=1"
        );
        let params = CheckParams {
            platform: "x86_64-unknown-linux-gnu",
            version: "1.0.0-beta.1+build.5",
            source: InstallSource::Deb,
            trigger: CheckTrigger::Idle,
            first_of_day: false,
        };
        assert_eq!(
            update_check_url(UPDATE_ENDPOINT_BASE_URL, Channel::Beta, &params)
                .unwrap()
                .as_str(),
            "https://updates.intentapp.dev/channel/beta.json?p=x86_64-unknown-linux-gnu&v=1.0.0-beta.1%2Bbuild.5&src=deb&k=idle&d=0"
        );
    }

    #[test]
    fn trigger_and_source_wire_values() {
        assert_eq!(CheckTrigger::Periodic.as_str(), "periodic");
        assert_eq!(CheckTrigger::Idle.as_str(), "idle");
        assert_eq!(CheckTrigger::Manual.as_str(), "manual");
        for (source, wire) in [
            (InstallSource::Brew, "brew"),
            (InstallSource::Deb, "deb"),
            (InstallSource::Script, "script"),
            (InstallSource::Windows, "windows"),
            (InstallSource::Unknown, "unknown"),
        ] {
            assert_eq!(source.to_string(), wire);
        }
    }

    #[test]
    fn user_agent_names_version_and_triple() {
        assert_eq!(
            user_agent(),
            format!(
                "intentd-sitter/{} ({})",
                env!("CARGO_PKG_VERSION"),
                crate::manifest::TARGET_TRIPLE
            )
        );
    }

    // ---- opt-out ----

    fn env_of(vars: &[(&str, &str)]) -> impl Fn(&str) -> Option<OsString> {
        let vars: HashMap<String, OsString> = vars
            .iter()
            .map(|(k, v)| ((*k).to_string(), OsString::from(v)))
            .collect();
        move |name| vars.get(name).cloned()
    }

    #[test]
    fn telemetry_on_by_default() {
        let dir = tempfile::tempdir().unwrap();
        assert!(!opted_out(env_of(&[]), &dir.path().join("config.toml")));
    }

    #[test]
    fn do_not_track_opts_out() {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("config.toml");
        assert!(opted_out(env_of(&[("DO_NOT_TRACK", "1")]), &config));
        assert!(opted_out(env_of(&[("DO_NOT_TRACK", "true")]), &config));
        assert!(!opted_out(env_of(&[("DO_NOT_TRACK", "0")]), &config));
        assert!(!opted_out(env_of(&[("DO_NOT_TRACK", "")]), &config));
    }

    #[test]
    fn intentd_disable_telemetry_opts_out() {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("config.toml");
        assert!(opted_out(
            env_of(&[("INTENTD_DISABLE_TELEMETRY", "1")]),
            &config
        ));
        assert!(!opted_out(
            env_of(&[("INTENTD_DISABLE_TELEMETRY", "0")]),
            &config
        ));
    }

    #[test]
    fn config_telemetry_false_opts_out() {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("config.toml");
        std::fs::write(&config, "channel = \"beta\"\ntelemetry = false\n").unwrap();
        assert!(opted_out(env_of(&[]), &config));
        std::fs::write(&config, "telemetry = true\n").unwrap();
        assert!(!opted_out(env_of(&[]), &config));
        // An invalid channel must not hide the opt-out.
        std::fs::write(&config, "channel = \"nightly\"\ntelemetry = false\n").unwrap();
        assert!(opted_out(env_of(&[]), &config));
    }

    // ---- daily flag ----

    #[test]
    fn daily_flag_across_utc_day_boundary() {
        let before = datetime!(2026-10-20 23:59:59 UTC);
        let after = datetime!(2026-10-21 00:00:00 UTC);
        assert_eq!(utc_day(before), "2026-10-20");
        assert_eq!(utc_day(after), "2026-10-21");
        assert!(is_first_check_of_day(None, before));
        assert!(!is_first_check_of_day(Some("2026-10-20"), before));
        assert!(is_first_check_of_day(Some("2026-10-20"), after));
        assert!(!is_first_check_of_day(Some("2026-10-21"), after));
    }

    #[test]
    fn daily_flag_uses_utc_not_local_offset() {
        // 23:30 at UTC-5 is already the next UTC day.
        let now = datetime!(2026-10-20 23:30:00 -5);
        assert_eq!(utc_day(now), "2026-10-21");
        assert!(is_first_check_of_day(Some("2026-10-20"), now));
    }

    // ---- install source ----

    #[test]
    fn install_source_for_each_path_shape() {
        let home = Path::new("/home/ada");
        let cases: &[(&str, bool, bool, InstallSource)] = &[
            (
                "/opt/homebrew/Cellar/intentd/0.1.28/bin/intentd",
                false,
                false,
                InstallSource::Brew,
            ),
            (
                "/usr/local/Cellar/intentd/0.1.28/bin/intentd",
                false,
                false,
                InstallSource::Brew,
            ),
            (
                "/opt/homebrew/bin/intentd",
                false,
                false,
                InstallSource::Brew,
            ),
            (
                "/home/linuxbrew/.linuxbrew/Cellar/intentd/0.1.28/bin/intentd",
                false,
                false,
                InstallSource::Brew,
            ),
            ("/usr/bin/intentd", false, true, InstallSource::Deb),
            ("/usr/bin/intentd", false, false, InstallSource::Unknown),
            (
                "/usr/local/bin/intentd",
                false,
                false,
                InstallSource::Script,
            ),
            (
                "/home/ada/.local/bin/intentd",
                false,
                false,
                InstallSource::Script,
            ),
            (
                "/home/bob/.local/bin/intentd",
                false,
                false,
                InstallSource::Unknown,
            ),
            ("/srv/custom/intentd", false, true, InstallSource::Unknown),
            (
                r"C:\Users\ada\AppData\Local\intentd\intentd.exe",
                true,
                false,
                InstallSource::Windows,
            ),
        ];
        for (exe, windows, dpkg, expected) in cases {
            assert_eq!(
                detect_install_source(Path::new(exe), Some(home), *windows, *dpkg),
                *expected,
                "{exe}"
            );
        }
        assert_eq!(
            detect_install_source(
                Path::new("/home/ada/.local/bin/intentd"),
                None,
                false,
                false
            ),
            InstallSource::Unknown
        );
    }
}
