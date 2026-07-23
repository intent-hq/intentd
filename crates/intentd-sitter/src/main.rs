//! intentd-sitter binary entry point.
//!
//! Skeleton only: parses the sitter-owned `--sitter-*` flags, resolves the
//! data-dir/state layout, and reports what is (not yet) installed. The
//! update engine lives in [`intentd_sitter::updater`]; wiring it up plus
//! daemon supervision land in follow-up changes.

use intentd_sitter::cli::{self, SitterArgs};
use intentd_sitter::paths::SitterPaths;
use intentd_sitter::state;

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

    let state = state::load(&paths.state_path);
    let installed = state
        .current_version
        .as_deref()
        .map(|v| paths.daemon_binary(v))
        .filter(|bin| bin.exists());

    match installed {
        Some(bin) => {
            eprintln!(
                "intentd-sitter: daemon {} is installed at {} but launch/supervision is not implemented yet",
                state.current_version.as_deref().unwrap_or_default(),
                bin.display()
            );
            1
        }
        None => {
            eprintln!(
                "intentd-sitter: no intentd daemon is installed for channel {} (update support is not implemented yet)",
                args.channel
            );
            1
        }
    }
}
