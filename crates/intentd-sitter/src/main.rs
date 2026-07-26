//! intentd-sitter binary entry point.
//!
//! Parses the sitter-owned `--sitter-*` flags, resolves the data-dir/state
//! layout, and hands off to [`intentd_sitter::supervisor`]. For `serve`:
//! startup update check, spawn the installed daemon with all forwarded args
//! verbatim, keep it updated on the randomized 12–24h cadence, and babysit
//! crashes. One-shot subcommands run the installed daemon exactly once with
//! no updater activity. The intercepted `intentd sitter channel` command is
//! handled entirely here — it never spawns the daemon.

use intentd_sitter::cli::{self, SitterArgs, SitterCommand};
use intentd_sitter::config;
use intentd_sitter::manifest;
use intentd_sitter::paths::SitterPaths;
use intentd_sitter::supervisor::{self, SupervisorConfig, MANIFEST_BASE_URL_ENV};
use intentd_sitter::updater::{UpdateOutcome, Updater};

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

    let base_url = std::env::var(MANIFEST_BASE_URL_ENV)
        .ok()
        .filter(|url| !url.is_empty())
        .unwrap_or_else(|| manifest::DEFAULT_MANIFEST_BASE_URL.to_string());

    match args.sitter_command() {
        Some(Ok(command)) => return run_sitter_command(command, &args, &paths, &base_url),
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
        base_url,
    )
}

/// How to apply a new channel pin / freshly installed binary right away.
const RESTART_HINT: &str = "apply it now with `intentd restart` (fallback: \
     `brew services restart intentd` / `systemctl --user restart intentd`)";

/// Execute an intercepted `intentd sitter …` command; returns the exit code.
/// Never spawns the daemon and never touches a running one.
fn run_sitter_command(
    command: SitterCommand,
    args: &SitterArgs,
    paths: &SitterPaths,
    base_url: &str,
) -> i32 {
    let SitterCommand::Channel { set, redownload } = command;
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
        let updater = match Updater::with_base_url(paths.clone(), base_url) {
            Ok(updater) => updater,
            Err(e) => {
                eprintln!("intentd-sitter: {e} (the channel pin was still written)");
                return 1;
            }
        };
        match updater.force_install(channel) {
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
