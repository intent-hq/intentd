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
//! `Store::open_postgres` takes. No shipped build links the Postgres backend
//! yet (feature `postgres` stays out of the release graph), so `serve` then
//! stops at the open with [`PostgresBackend::unavailable`].
//!
//! `daemon.mode` is read once per start, so a later change of the setting is
//! covered by the next start's check.

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
    token: OrchestratorOnly,
}

impl std::fmt::Debug for PostgresBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The URL may carry a password: never print it.
        f.debug_struct("PostgresBackend")
            .field("url", &"<redacted>")
            .field("token", &self.token)
            .finish()
    }
}

impl PostgresBackend {
    /// The open this build cannot do yet: the Postgres backend is not in any
    /// shipped build. Per-tenant composition will take `url` and the token
    /// here instead.
    pub fn unavailable(self) -> anyhow::Error {
        let Self { url, token } = self;
        drop((url, token));
        anyhow::anyhow!(
            "{POSTGRES_URL_ENV} is set and daemon.mode is orchestrator, but this intentd build \
             has no Postgres store backend; unset {POSTGRES_URL_ENV} to use the SQLite store"
        )
    }
}

/// Where the daemon mode for this start came from, for the refusal message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModeSource {
    /// The `--node` / `--orchestrator` flag.
    Flag,
    /// `daemon.mode` in `config.toml`.
    ConfigFile,
    /// No flag and no `config.toml`: the default mode.
    Default,
}

/// Wire name of a mode, as written in `config.toml`.
fn mode_name(mode: DaemonMode) -> &'static str {
    match mode {
        DaemonMode::Regular => "regular",
        DaemonMode::Node => "node",
        DaemonMode::Orchestrator => "orchestrator",
    }
}

/// The store for a daemon in `mode`, given the configured Postgres URL. Only
/// orchestrator mode may use Postgres; it alone mints the opener's token.
///
/// # Errors
/// A Postgres URL in any other mode. The message names both settings.
pub fn choose(
    mode: DaemonMode,
    source: ModeSource,
    postgres_url: Option<String>,
) -> anyhow::Result<StoreBackend> {
    let Some(url) = postgres_url.filter(|url| !url.trim().is_empty()) else {
        return Ok(StoreBackend::Sqlite);
    };
    match mode {
        DaemonMode::Orchestrator => Ok(StoreBackend::Postgres(PostgresBackend {
            url,
            token: OrchestratorOnly::mint_for_intentd_serve(),
        })),
        DaemonMode::Regular | DaemonMode::Node => {
            let name = mode_name(mode);
            let source = match source {
                ModeSource::Flag => format!("the --{name} flag"),
                ModeSource::ConfigFile => "config.toml".to_owned(),
                ModeSource::Default => "the default; config.toml does not set it".to_owned(),
            };
            anyhow::bail!(
                "{POSTGRES_URL_ENV} is set, but daemon.mode is {name} (from {source}): the \
                 Postgres store runs only in orchestrator mode. Set daemon.mode = \
                 \"orchestrator\" (or pass --orchestrator), or unset {POSTGRES_URL_ENV}"
            )
        }
    }
}

/// [`choose`] for this `serve`: reads [`POSTGRES_URL_ENV`] and removes it from
/// the environment (children never inherit the URL), then, only when it is
/// set, the daemon mode as `serve` resolves it, reading `config.toml` without
/// creating it.
///
/// Call it while the process is single-threaded (`main`, before the runtime):
/// it changes the environment.
///
/// # Errors
/// As [`choose`]; also an unreadable or malformed `config.toml` while the URL
/// is set.
pub fn choose_for_serve(flag: Option<DaemonMode>) -> anyhow::Result<StoreBackend> {
    let url = std::env::var(POSTGRES_URL_ENV).ok();
    std::env::remove_var(POSTGRES_URL_ENV);
    if url.as_deref().is_none_or(|url| url.trim().is_empty()) {
        return Ok(StoreBackend::Sqlite);
    }
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
        None => (DaemonMode::default(), ModeSource::Default),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const URL: &str = "postgres://intent:secret-pw@db/intent";

    fn refusal(mode: DaemonMode, source: ModeSource) -> String {
        choose(mode, source, Some(URL.to_owned()))
            .expect_err("a non-orchestrator mode must refuse Postgres")
            .to_string()
    }

    #[test]
    fn no_url_keeps_sqlite_in_every_mode() {
        for mode in [
            DaemonMode::Regular,
            DaemonMode::Node,
            DaemonMode::Orchestrator,
        ] {
            assert!(matches!(
                choose(mode, ModeSource::Flag, None).unwrap(),
                StoreBackend::Sqlite
            ));
            assert!(matches!(
                choose(mode, ModeSource::Flag, Some("  ".to_owned())).unwrap(),
                StoreBackend::Sqlite
            ));
        }
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
        let backend = choose(
            DaemonMode::Orchestrator,
            ModeSource::Flag,
            Some(URL.to_owned()),
        )
        .unwrap();
        let StoreBackend::Postgres(postgres) = backend else {
            panic!("orchestrator mode must choose Postgres");
        };
        assert_eq!(postgres.url, URL);
        assert!(!format!("{postgres:?}").contains("secret-pw"));
        let err = postgres.unavailable().to_string();
        assert!(err.contains("no Postgres store backend"), "{err}");
        assert!(!err.contains("secret-pw"), "{err}");
    }

    #[test]
    fn the_default_mode_says_config_toml_does_not_set_it() {
        let err = refusal(DaemonMode::Regular, ModeSource::Default);
        assert!(err.contains("the default"), "{err}");
    }
}
