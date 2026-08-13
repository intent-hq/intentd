//! intentd-sitter binary entry point.
//!
//! Parses the sitter-owned `--sitter-*` flags, resolves the data-dir/state
//! layout, and hands off to [`intentd_sitter::supervisor`]. For `serve`:
//! startup update check, spawn the installed daemon with all forwarded args
//! verbatim, keep it updated on the randomized 12–24h cadence, and babysit
//! crashes. One-shot subcommands run the installed daemon exactly once with
//! no updater activity. The intercepted `intentd sitter channel`,
//! `intentd restart`, and `intentd update` commands are handled entirely
//! here — they never spawn the daemon.

use intentd_sitter::cli::{self, SitterArgs, SitterCommand};
use intentd_sitter::config;
use intentd_sitter::manifest;
use intentd_sitter::paths::SitterPaths;
use intentd_sitter::supervisor::{self, SupervisorConfig, MANIFEST_BASE_URL_ENV};
use intentd_sitter::updater::{UpdateCheck, UpdateOutcome, Updater};

fn main() {
    std::process::exit(run());
}

fn run() -> i32 {
    let args = match SitterArgs::parse_from(
        std::env::args_os().skip(1),
        std::env::var_os(cli::CHANNEL_ENV),
    ) {
        Ok(args) => args,
        Err(e) => {
            eprintln!("intentd-sitter: {e}");
            return 2;
        }
    };

    if args.print_version {
        println!("intentd-sitter {}", env!("CARGO_PKG_VERSION"));
        return 0;
    }

    let paths = match SitterPaths::resolve() {
        Ok(paths) => paths,
        Err(e) => {
            eprintln!("intentd-sitter: {e}");
            return 1;
        }
    };

    // The env override pins exactly one base (no fallback) so tests and
    // overrides stay deterministic; the default is the ordered fallback list.
    let base_urls = match std::env::var(MANIFEST_BASE_URL_ENV)
        .ok()
        .filter(|url| !url.is_empty())
    {
        Some(url) => vec![url],
        None => manifest::DEFAULT_MANIFEST_BASE_URLS
            .iter()
            .map(|base| base.to_string())
            .collect(),
    };

    match args.sitter_command() {
        Some(Ok(command)) => return run_sitter_command(command, &args, &paths, &base_urls),
        Some(Err(e)) => {
            eprintln!("intentd-sitter: {e}");
            return 2;
        }
        None => {}
    }

    let channel = config::resolve_channel(args.channel, config::load_channel(&paths.config_path));

    supervisor::run(
        paths,
        channel,
        args.passthrough,
        SupervisorConfig::from_env(),
        base_urls,
    )
}

/// How to apply a new channel pin / freshly installed binary right away.
const RESTART_HINT: &str = "apply it now with `intentd restart` (fallback: \
     `brew services restart intentd` / `systemctl --user restart intentd`)";

/// Execute an intercepted sitter-owned command; returns the exit code.
/// Never spawns the daemon.
fn run_sitter_command(
    command: SitterCommand,
    args: &SitterArgs,
    paths: &SitterPaths,
    base_urls: &[String],
) -> i32 {
    match command {
        SitterCommand::Channel { set, redownload } => {
            run_channel_command(set, redownload, args, paths, base_urls)
        }
        SitterCommand::Restart => run_restart(paths),
        SitterCommand::Update { check } => run_update_command(check, args, paths, base_urls),
    }
}

/// `intentd sitter channel [stable|beta|alpha] [--redownload]` — never
/// touches a running daemon.
fn run_channel_command(
    set: Option<cli::Channel>,
    redownload: bool,
    args: &SitterArgs,
    paths: &SitterPaths,
    base_urls: &[String],
) -> i32 {
    let Some(channel) = set else {
        let resolved =
            config::resolve_channel(args.channel, config::load_channel(&paths.config_path));
        println!("{} (from {})", resolved.channel, resolved.origin);
        return 0;
    };

    if let Err(e) = config::save_channel(&paths.config_path, channel) {
        eprintln!(
            "intentd-sitter: failed to write {}: {e}",
            paths.config_path.display()
        );
        return 1;
    }
    println!(
        "channel {channel} pinned in {}",
        paths.config_path.display()
    );

    if redownload {
        let updater = match Updater::with_base_urls(paths.clone(), base_urls.iter().cloned()) {
            Ok(updater) => updater,
            Err(e) => {
                eprintln!("intentd-sitter: {e} (the channel pin was still written)");
                return 1;
            }
        };
        match updater.force_install(channel) {
            // `AlreadyCurrent` is defensive/unreachable here: force_install
            // bypasses the newer-only comparison and always reinstalls, but
            // the arm keeps the match exhaustive (and prints something
            // sensible) if updater internals ever change.
            Ok(
                UpdateOutcome::Installed { version, .. }
                | UpdateOutcome::AlreadyCurrent { version },
            ) => {
                println!(
                    "installed intentd {version} from channel {channel}; \
                     the new binary becomes active after a restart"
                );
                println!("{RESTART_HINT}");
            }
            Err(e) => {
                eprintln!(
                    "intentd-sitter: failed to install from channel {channel}: {e} \
                     (the channel pin was still written)"
                );
                return 1;
            }
        }
    } else {
        println!(
            "{RESTART_HINT}; a running service otherwise picks it up \
             at its next periodic update check"
        );
    }
    0
}

/// `intentd update [--check]` — force an update check on the effective
/// channel now instead of waiting for the periodic serve-mode check.
/// `--check` only reports installed vs latest and exits 0 whenever the
/// check itself succeeded, update available or not (scripts parse stdout
/// to tell the two apart); the full form installs a
/// newer version (newer-only, never a downgrade) and, when a supervised
/// serve-mode sitter is running, restarts its daemon via SIGHUP so the new
/// binary takes effect immediately. The automatic restart is skipped when
/// the channel came from this invocation's flag/env override and differs
/// from the channel the running service follows (config pin > stable
/// default — service definitions pass no flag or env): silently restarting
/// a stable-pinned service onto a beta binary would put it on the wrong
/// channel.
fn run_update_command(
    check: bool,
    args: &SitterArgs,
    paths: &SitterPaths,
    base_urls: &[String],
) -> i32 {
    let resolved = config::resolve_channel(args.channel, config::load_channel(&paths.config_path));
    let channel = resolved.channel;
    let updater = match Updater::with_base_urls(paths.clone(), base_urls.iter().cloned()) {
        Ok(updater) => updater,
        Err(e) => {
            eprintln!("intentd-sitter: {e}");
            return 1;
        }
    };

    if check {
        return match updater.check_only(channel) {
            Ok(UpdateCheck {
                installed,
                latest,
                update_available,
            }) => {
                match installed {
                    Some(installed) => println!("installed: intentd {installed}"),
                    None => println!("installed: none"),
                }
                println!("latest on channel {channel}: intentd {latest}");
                if update_available {
                    println!("update available; run `intentd update` to install it");
                } else {
                    println!("already up to date");
                }
                0
            }
            Err(e) => {
                eprintln!("intentd-sitter: update check on channel {channel} failed: {e}");
                1
            }
        };
    }

    match updater.check_and_install(channel) {
        Ok(UpdateOutcome::AlreadyCurrent { version }) => {
            println!("already up to date: intentd {version} (channel {channel})");
            0
        }
        Ok(UpdateOutcome::Installed { version, previous }) => {
            match previous {
                Some(previous) => println!(
                    "installed intentd {version} from channel {channel} \
                     (was {previous})"
                ),
                None => println!("installed intentd {version} from channel {channel}"),
            }
            let service = config::resolve_channel(None, config::load_channel(&paths.config_path));
            if channel != service.channel {
                println!(
                    "not restarting the running service: it follows channel {} \
                     (from {}), not {channel}; to switch it, pin the channel with \
                     `intentd sitter channel {channel}` and run `intentd restart`",
                    service.channel, service.origin
                );
                return 0;
            }
            apply_installed_update(paths)
        }
        Err(e) => {
            eprintln!("intentd-sitter: update on channel {channel} failed: {e}");
            1
        }
    }
}

/// Restart the supervised daemon: SIGHUP the serve-mode sitter so it
/// gracefully stops the daemon and respawns it on the currently installed
/// version. Shared by `intentd restart` and `intentd update`.
#[cfg(unix)]
fn send_sighup_to_sitter(pid: nix::unistd::Pid) -> i32 {
    if let Err(e) = nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGHUP) {
        eprintln!("intentd-sitter: failed to signal the supervised sitter (pid {pid}): {e}");
        return 1;
    }
    println!("restarting intentd: sent SIGHUP to the supervised sitter (pid {pid})");
    0
}

/// Activate a freshly installed daemon: when a supervised serve-mode sitter
/// is running, SIGHUP it so it respawns on the new version now; otherwise
/// the new binary simply takes effect on the next start.
#[cfg(unix)]
fn apply_installed_update(paths: &SitterPaths) -> i32 {
    let Some(pid) = supervisor::read_live_pid(&paths.pid_path) else {
        println!(
            "no running supervised intentd found; the new version takes \
             effect on the next start"
        );
        return 0;
    };
    send_sighup_to_sitter(pid)
}

/// No SIGHUP on windows: the new binary takes effect on the next (service)
/// restart instead.
#[cfg(not(unix))]
fn apply_installed_update(_paths: &SitterPaths) -> i32 {
    println!("restart the intentd service to start using the new version");
    0
}

/// `intentd restart` — restart the supervised daemon in place by sending
/// SIGHUP to the serve-mode sitter found via its pidfile. Stale pidfiles
/// (dead pid) are treated as no running service.
#[cfg(unix)]
fn run_restart(paths: &SitterPaths) -> i32 {
    let Some(pid) = supervisor::read_live_pid(&paths.pid_path) else {
        eprintln!(
            "intentd-sitter: no running supervised intentd found (no live pid in {}); \
             start the service first (`intentd serve`, `brew services start intentd`, \
             or `systemctl --user start intentd`)",
            paths.pid_path.display()
        );
        return 1;
    };
    send_sighup_to_sitter(pid)
}

/// No SIGHUP on windows: point at the service manager instead.
#[cfg(not(unix))]
fn run_restart(_paths: &SitterPaths) -> i32 {
    eprintln!(
        "intentd-sitter: `intentd restart` is not supported on Windows; \
         restart the service instead"
    );
    1
}
