//! Which store `intentd serve` uses, decided before it writes anything.
//!
//! Postgres, and with it more than one daemon per tenant, exists only in
//! orchestrator mode. [`INTENTD_POSTGRES_URL`](POSTGRES_URL_ENV) is the one
//! switch for it. With the URL set, [`choose_for_serve`] reads the daemon mode
//! the same way `serve` does (the `--node` / `--orchestrator` flag, else
//! `daemon.mode` in `config.toml`) without creating `config.toml`, and
//! refuses any mode but orchestrator. `main` runs it before tracing (which
//! creates the log file), the data-dir lock, the pidfile and the store.
//!
//! In orchestrator mode it mints the [`OrchestratorOnly`] token that
//! `Store::open_postgres` takes; the private [`choose`] is the only place
//! that mints it. No shipped build links the Postgres backend yet (feature
//! `postgres` stays out of the release graph), so `serve` then stops at the
//! open with [`PostgresBackend::unavailable`].
//!
//! A set but empty, blank or non-UTF-8 URL is an error, never "unset": a
//! daemon meant for Postgres must not quietly open the `SQLite` store.
//!
//! The mode is checked at each start, so a later change of `daemon.mode` is
//! covered by the next start's check. `cmd_serve_owned` resolves the mode
//! again from the settings registry; today the Postgres choice returns before
//! it. When the open is wired (per-tenant composition), `cmd_serve_owned`
//! must run in [`PostgresBackend::mode`], not re-resolve it, so an edit of
//! `config.toml` between the two reads cannot pair Postgres with another
//! mode.
//!
//! Removing the URL from the environment keeps it from children; on Linux
//! `/proc/<pid>/environ` still shows the process's original environment to
//! same-user processes. A secret reference instead of a plain URL is for the
//! per-tenant composition step.

use intent_core::settings_file::{DaemonMode, SettingsFile};
use intent_store::OrchestratorOnly;

/// The env var that selects the Postgres store; the URL may carry a password.
pub const POSTGRES_URL_ENV: &str = "INTENTD_POSTGRES_URL";

/// The store this `serve` opens.
#[derive(Debug)]
pub enum StoreBackend {
    /// The data dir's `SQLite` database (every shipped daemon).
    Sqlite,
    /// The Postgres store; only ever chosen in orchestrator mode.
    Postgres(PostgresBackend),
}

/// A Postgres store choice: the URL plus the orchestrator-mode proof the
/// opener requires.
pub struct PostgresBackend {
    url: String,
    mode: DaemonMode,
    token: OrchestratorOnly,
}

impl std::fmt::Debug for PostgresBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The URL may carry a password: never print it.
        f.debug_struct("PostgresBackend")
            .field("url", &"<redacted>")
            .field("mode", &self.mode)
            .field("token", &self.token)
            .finish()
    }
}

impl PostgresBackend {
    /// The mode this choice was made in (always orchestrator). The daemon
    /// must run in this mode rather than resolve it again.
    pub fn mode(&self) -> DaemonMode {
        self.mode
    }

    /// The open this build cannot do yet: the Postgres backend is not in any
    /// shipped build. Per-tenant composition will take `url` and the token
    /// here instead.
    pub fn unavailable(self) -> anyhow::Error {
        let mode = mode_name(self.mode());
        let Self { url, token, .. } = self;
        drop((url, token));
        anyhow::anyhow!(
            "{POSTGRES_URL_ENV} is set and daemon.mode is {mode}, but this intentd build has \
             no Postgres store backend; unset {POSTGRES_URL_ENV} to use the SQLite store"
        )
    }
}

/// Where the daemon mode for this start came from, for the refusal message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModeSource {
    /// The `--node` / `--orchestrator` flag.
    Flag,
    /// `config.toml` exists: its `daemon.mode`, or the default when it does
    /// not set one.
    ConfigFile,
    /// No flag and no `config.toml`: the default mode.
    NoConfigFile,
}

/// Wire name of a mode, as written in `config.toml`.
fn mode_name(mode: DaemonMode) -> &'static str {
    match mode {
        DaemonMode::Regular => "regular",
        DaemonMode::Node => "node",
        DaemonMode::Orchestrator => "orchestrator",
    }
}

/// The Postgres URL from the raw env value: `None` when unset. A value that
/// is set but blank or not UTF-8 is an error, never a fallback to `SQLite`.
fn postgres_url(raw: Option<std::ffi::OsString>) -> anyhow::Result<Option<String>> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let Ok(url) = raw.into_string() else {
        anyhow::bail!("{POSTGRES_URL_ENV} is set but is not valid UTF-8; fix or unset it");
    };
    if url.trim().is_empty() {
        anyhow::bail!(
            "{POSTGRES_URL_ENV} is set but empty; set a Postgres URL or unset it to use the \
             SQLite store"
        );
    }
    Ok(Some(url))
}

/// The Postgres store for a daemon in `mode`. Only orchestrator mode may use
/// Postgres, and this is the one place that mints the opener's token: keep
/// it private, so no other code can turn an arbitrary `DaemonMode` into one.
///
/// # Errors
/// Any mode but orchestrator. The message names both settings.
fn choose(mode: DaemonMode, source: ModeSource, url: String) -> anyhow::Result<StoreBackend> {
    match mode {
        DaemonMode::Orchestrator => Ok(StoreBackend::Postgres(PostgresBackend {
            url,
            mode,
            token: OrchestratorOnly::mint_for_intentd_serve(),
        })),
        DaemonMode::Regular | DaemonMode::Node => {
            let name = mode_name(mode);
            let source = match source {
                ModeSource::Flag => format!("the --{name} flag"),
                ModeSource::ConfigFile => "config.toml, or its default when unset".to_owned(),
                ModeSource::NoConfigFile => "the default; there is no config.toml".to_owned(),
            };
            anyhow::bail!(
                "{POSTGRES_URL_ENV} is set, but daemon.mode is {name} (from {source}): the \
                 Postgres store runs only in orchestrator mode. Set daemon.mode = \
                 \"orchestrator\" (or pass --orchestrator), or unset {POSTGRES_URL_ENV}"
            )
        }
    }
}

/// The store for this `serve`: reads [`POSTGRES_URL_ENV`] and removes it from
/// the environment (children never inherit the URL), then, only when it is
/// set, the daemon mode as `serve` resolves it, reading `config.toml` without
/// creating it.
///
/// Call it while the process is single-threaded (`main`, before the runtime):
/// it changes the environment.
///
/// # Errors
/// A set but blank or non-UTF-8 URL; any mode but orchestrator with the URL
/// set; an unreadable or malformed `config.toml` while the URL is set.
pub fn choose_for_serve(flag: Option<DaemonMode>) -> anyhow::Result<StoreBackend> {
    let raw = std::env::var_os(POSTGRES_URL_ENV);
    std::env::remove_var(POSTGRES_URL_ENV);
    let Some(url) = postgres_url(raw)? else {
        return Ok(StoreBackend::Sqlite);
    };
    let (mode, source) = match flag {
        Some(mode) => (mode, ModeSource::Flag),
        None => persisted_mode()?,
    };
    choose(mode, source, url)
}

/// `daemon.mode` from `config.toml`, read-only: a missing file is the
/// default mode, and nothing is created.
fn persisted_mode() -> anyhow::Result<(DaemonMode, ModeSource)> {
    let path =
        intent_core::config::resolve_config_path().map_err(|e| anyhow::anyhow!(e.to_string()))?;
    let settings =
        SettingsFile::load_existing(&path).map_err(|e| anyhow::anyhow!(e.to_string()))?;
    Ok(match settings {
        Some(settings) => (settings.daemon.mode, ModeSource::ConfigFile),
        None => (DaemonMode::default(), ModeSource::NoConfigFile),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const URL: &str = "postgres://intent:secret-pw@db/intent";

    fn refusal(mode: DaemonMode, source: ModeSource) -> String {
        choose(mode, source, URL.to_owned())
            .expect_err("a non-orchestrator mode must refuse Postgres")
            .to_string()
    }

    #[test]
    fn an_unset_url_keeps_sqlite() {
        assert_eq!(postgres_url(None).unwrap(), None);
        assert_eq!(
            postgres_url(Some(URL.into())).unwrap().as_deref(),
            Some(URL)
        );
    }

    #[test]
    fn a_set_but_blank_url_is_an_error_not_sqlite() {
        for blank in ["", "  "] {
            let err = postgres_url(Some(blank.into())).unwrap_err().to_string();
            assert!(err.contains(POSTGRES_URL_ENV), "{err}");
            assert!(err.contains("empty"), "{err}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn a_non_utf8_url_is_an_error_not_sqlite() {
        use std::os::unix::ffi::OsStringExt as _;
        let raw = std::ffi::OsString::from_vec(b"postgres://\xff".to_vec());
        let err = postgres_url(Some(raw)).unwrap_err().to_string();
        assert!(err.contains("UTF-8"), "{err}");
    }

    #[test]
    fn regular_mode_refuses_a_postgres_url_naming_both_settings() {
        let err = refusal(DaemonMode::Regular, ModeSource::ConfigFile);
        for needle in [
            POSTGRES_URL_ENV,
            "daemon.mode",
            "regular",
            "config.toml",
            "orchestrator",
        ] {
            assert!(err.contains(needle), "`{needle}` missing: {err}");
        }
        assert!(!err.contains("secret-pw"), "{err}");
    }

    #[test]
    fn node_mode_refuses_a_postgres_url_naming_the_flag() {
        let err = refusal(DaemonMode::Node, ModeSource::Flag);
        for needle in [POSTGRES_URL_ENV, "daemon.mode", "--node", "orchestrator"] {
            assert!(err.contains(needle), "`{needle}` missing: {err}");
        }
    }

    #[test]
    fn orchestrator_mode_chooses_postgres_and_never_prints_the_url() {
        let backend = choose(DaemonMode::Orchestrator, ModeSource::Flag, URL.to_owned()).unwrap();
        let StoreBackend::Postgres(postgres) = backend else {
            panic!("orchestrator mode must choose Postgres");
        };
        assert_eq!(postgres.url, URL);
        assert_eq!(postgres.mode(), DaemonMode::Orchestrator);
        assert!(!format!("{postgres:?}").contains("secret-pw"));
        let err = postgres.unavailable().to_string();
        assert!(err.contains("no Postgres store backend"), "{err}");
        assert!(!err.contains("secret-pw"), "{err}");
    }

    #[test]
    fn the_default_mode_says_where_it_came_from() {
        let err = refusal(DaemonMode::Regular, ModeSource::NoConfigFile);
        assert!(err.contains("there is no config.toml"), "{err}");
        let err = refusal(DaemonMode::Regular, ModeSource::ConfigFile);
        assert!(err.contains("or its default when unset"), "{err}");
    }
}
