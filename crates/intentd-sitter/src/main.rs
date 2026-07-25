//! intentd-sitter binary entry point.
//!
//! Parses the sitter-owned `--sitter-*` flags, resolves the data-dir/state
//! layout, and hands off to [`intentd_sitter::supervisor`]. For `serve`:
//! startup update check, spawn the installed daemon with all forwarded args
//! verbatim, keep it updated on the randomized 12–24h cadence, and babysit
//! crashes. One-shot subcommands run the installed daemon exactly once with
//! no updater activity.

use intentd_sitter::cli::{self, SitterArgs};
use intentd_sitter::manifest;
use intentd_sitter::paths::SitterPaths;
use intentd_sitter::supervisor::{self, SupervisorConfig, MANIFEST_BASE_URL_ENV};

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

    supervisor::run(
        paths,
        args.channel,
        args.passthrough,
        SupervisorConfig::from_env(),
        base_url,
    )
}
