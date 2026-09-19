//! intentd-microvm-helper — small signed fork/exec helper that boots a libkrun
//! Linux aarch64 microVM. intentd spawns and supervises this binary; on
//! success the helper process becomes the VM and exits with the guest
//! command's exit status.
//!
//! Platform support: real boot path on macOS (Apple Silicon; requires the
//! `com.apple.security.hypervisor` entitlement — see
//! `scripts/sign-microvm-helper.sh`). On all other platforms the same CLI
//! parses and validates, then exits `EXIT_UNAVAILABLE` (69).
//!
//! `--probe` runs the boot path's dylib resolution + dlopen + symbol lookup
//! without creating a VM and exits 0 when libkrun is loadable, printing one
//! JSON line (`{"status":"ok","libkrun_dir":…,"libkrun":…,"libkrun_version":…}`)
//! on stdout; intentd uses the exit status to decide
//! `system.capabilities.microvmSupported` and logs the line at startup.

mod cli;
#[cfg(target_os = "macos")]
mod krun;

use clap::Parser;

/// Invalid configuration (semantic validation failed before boot).
pub const EXIT_USAGE: i32 = 64;
/// microVM unavailable: unsupported platform, or libkrun/libkrunfw dylibs
/// missing or unloadable.
pub const EXIT_UNAVAILABLE: i32 = 69;
/// A libkrun API call failed while configuring or starting the VM.
pub const EXIT_KRUN_API: i32 = 70;

fn main() {
    let mode = match cli::Cli::parse().into_mode() {
        Ok(mode) => mode,
        Err(msg) => {
            eprintln!("intentd-microvm-helper: {msg}");
            std::process::exit(EXIT_USAGE);
        }
    };

    #[cfg(target_os = "macos")]
    {
        let err = match mode {
            cli::Mode::Probe(plan) => match krun::probe(&plan) {
                Ok(report) => {
                    println!("{}", report.to_json_line());
                    std::process::exit(0)
                }
                Err(err) => err,
            },
            // On success krun_start_enter never returns: the process becomes
            // the VM and later exits with the guest command's exit status.
            cli::Mode::Boot(plan) => krun::boot(&plan),
        };
        eprintln!("intentd-microvm-helper: {}", err.message);
        std::process::exit(err.exit_code);
    }

    #[cfg(not(target_os = "macos"))]
    {
        let _ = &mode;
        eprintln!(
            "intentd-microvm-helper: microVM execution is only supported on macOS \
             (Apple Silicon) in v1; this platform has no libkrun backend"
        );
        std::process::exit(EXIT_UNAVAILABLE);
    }
}
