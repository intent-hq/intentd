//! CLI contract for the microVM helper (consumed by the intentd orchestrator).
//!
//! The helper boots a libkrun Linux aarch64 microVM whose root filesystem is a
//! host directory exposed over virtio-fs, then execs a guest command. On
//! success the helper process *becomes* the VM (`krun_start_enter` does not
//! return) and its exit status is the guest command's exit status.
//!
//! Hard constraints encoded here (from the spike findings / monorepo#1120):
//! - Guest exec path/args/env ride the kernel cmdline, which is ASCII-only and
//!   length-limited. Anything non-trivial must be delivered as a script file
//!   over virtio-fs (or via vsock exec) and invoked as e.g.
//!   `/bin/sh /ctl/run.sh`. Validation rejects non-ASCII and over-budget
//!   command material outright.
//! - Guest env is an explicit allowlist: only `--env` / `--env-passthrough`
//!   values are injected. The host environment is never forwarded wholesale.

use std::path::PathBuf;

use clap::Parser;

/// Total byte budget for exec path + args + env riding the kernel cmdline.
/// aarch64 COMMAND_LINE_SIZE is 2048; leave headroom for libkrun's own
/// additions (init=, console=, virtio-fs tags, ...).
pub const CMDLINE_BUDGET: usize = 1024;

/// Maximum number of vCPUs accepted (libkrun takes a u8; keep a sane cap).
pub const MAX_VCPUS: u8 = 16;

/// Minimum guest RAM in MiB.
pub const MIN_MEM_MIB: u32 = 128;

/// Unix socket paths must fit `sockaddr_un.sun_path`: 104 bytes on macOS
/// (SUN_LEN), 108 on Linux. Reject over-long vsock socket paths during
/// validation (exit 64) instead of failing deep in a libkrun API call.
pub const MAX_SOCKET_PATH_BYTES: usize = 103;

#[derive(Debug, Parser)]
#[command(
    name = "intentd-microvm-helper",
    about = "Boots a libkrun microVM (macOS aarch64) and execs a guest command; \
             exits with the guest command's exit status",
    after_help = "GUEST COMMAND\n  Everything after `--` is the guest command: the first \
                  token is the executable path inside the guest, the rest are its \
                  arguments. Exec path, args and env ride the kernel cmdline (ASCII-only, \
                  length-limited) — deliver real workloads as a script file over virtio-fs, \
                  e.g. `-- /bin/sh /ctl/run.sh`.\n\nEXIT CODES\n  0-255 guest command exit \
                  status (VM booted and ran to completion)\n  2     CLI parse error\n  64    \
                  invalid configuration (validation failed before boot)\n  69    microVM \
                  unavailable (unsupported platform, or libkrun/libkrunfw dylibs not \
                  found/loadable)\n  70    libkrun API error while configuring or starting \
                  the VM"
)]
pub struct Cli {
    /// Host directory exposed as the guest root filesystem (virtio-fs).
    #[arg(long, value_name = "DIR")]
    pub root_fs: PathBuf,

    /// Number of vCPUs for the guest.
    #[arg(long, default_value_t = 2, value_name = "N")]
    pub vcpus: u8,

    /// Guest RAM in MiB.
    #[arg(long, default_value_t = 2048, value_name = "MIB")]
    pub mem_mib: u32,

    /// Extra virtio-fs share, format TAG=HOST_DIR (repeatable). The guest
    /// mounts it with `mount -t virtiofs TAG <mountpoint>`.
    #[arg(long = "virtiofs", value_name = "TAG=HOST_DIR")]
    pub virtiofs: Vec<String>,

    /// Guest-initiated vsock port, format PORT=HOST_UNIX_SOCKET (repeatable).
    /// When the guest connects to vsock PORT, libkrun connects to the given
    /// host unix socket (a host process must already be listening there).
    #[arg(long = "vsock-connect", value_name = "PORT=SOCKET")]
    pub vsock_connect: Vec<String>,

    /// Host-initiated vsock port, format PORT=HOST_UNIX_SOCKET (repeatable).
    /// libkrun listens on the host unix socket; connections made to it are
    /// forwarded to the guest's vsock PORT.
    #[arg(long = "vsock-listen", value_name = "PORT=SOCKET")]
    pub vsock_listen: Vec<String>,

    /// Working directory inside the guest (path within the root filesystem).
    #[arg(long, value_name = "GUEST_DIR")]
    pub workdir: Option<String>,

    /// Guest environment variable, format KEY=VALUE (repeatable). Explicit
    /// allowlist — nothing else from the host environment is forwarded.
    #[arg(long = "env", value_name = "KEY=VALUE")]
    pub env: Vec<String>,

    /// Forward a variable from the helper's own environment into the guest by
    /// name (repeatable). Silently skipped when unset on the host.
    #[arg(long = "env-passthrough", value_name = "KEY")]
    pub env_passthrough: Vec<String>,

    /// Redirect the guest console to a host file instead of the helper's
    /// stdout/stderr.
    #[arg(long, value_name = "HOST_FILE")]
    pub console_log: Option<PathBuf>,

    /// Directory containing libkrun.dylib + libkrunfw.5.dylib. Defaults to
    /// $INTENTD_LIBKRUN_DIR, then the helper's own directory, then
    /// <helper dir>/../lib, then /opt/homebrew/lib (dev).
    #[arg(long, value_name = "DIR")]
    pub libkrun_dir: Option<PathBuf>,

    /// libkrun log level (0=off .. 5=trace).
    #[arg(long, default_value_t = 1, value_name = "LEVEL")]
    pub krun_log_level: u32,

    /// Guest command: executable path followed by its arguments.
    #[arg(last = true, required = true, value_name = "EXEC [ARGS]...")]
    pub guest_command: Vec<String>,
}

/// Fully validated boot configuration handed to the libkrun boot path.
#[derive(Debug, PartialEq)]
pub struct BootPlan {
    pub root_fs: PathBuf,
    pub vcpus: u8,
    pub mem_mib: u32,
    /// (tag, host_dir)
    pub virtiofs: Vec<(String, PathBuf)>,
    pub vsock: Vec<VsockPort>,
    pub workdir: Option<String>,
    /// KEY=VALUE pairs, already merged from --env and --env-passthrough.
    pub env: Vec<String>,
    pub exec_path: String,
    pub exec_args: Vec<String>,
    pub console_log: Option<PathBuf>,
    pub libkrun_dir: Option<PathBuf>,
    pub krun_log_level: u32,
}

#[derive(Debug, PartialEq)]
pub struct VsockPort {
    pub port: u32,
    pub socket: PathBuf,
    /// true = host-initiated (libkrun listens on the unix socket and forwards
    /// into the guest port); false = guest-initiated.
    pub host_initiated: bool,
}

impl Cli {
    /// Validates the parsed arguments into a [`BootPlan`], enforcing the
    /// kernel-cmdline and env-allowlist constraints. Filesystem existence
    /// checks live here too so the boot path can assume a sane plan.
    pub fn into_plan(self) -> Result<BootPlan, String> {
        if self.vcpus == 0 || self.vcpus > MAX_VCPUS {
            return Err(format!(
                "--vcpus must be 1..={MAX_VCPUS}, got {}",
                self.vcpus
            ));
        }
        if self.mem_mib < MIN_MEM_MIB {
            return Err(format!(
                "--mem-mib must be >= {MIN_MEM_MIB}, got {}",
                self.mem_mib
            ));
        }
        if !self.root_fs.is_dir() {
            return Err(format!(
                "--root-fs is not a directory: {}",
                self.root_fs.display()
            ));
        }

        let mut virtiofs = Vec::new();
        for spec in &self.virtiofs {
            let (tag, dir) = spec
                .split_once('=')
                .ok_or_else(|| format!("--virtiofs expects TAG=HOST_DIR, got '{spec}'"))?;
            validate_virtiofs_tag(tag)?;
            let dir = PathBuf::from(dir);
            if !dir.is_dir() {
                return Err(format!(
                    "--virtiofs {tag}: host path is not a directory: {}",
                    dir.display()
                ));
            }
            virtiofs.push((tag.to_string(), dir));
        }

        let mut vsock = Vec::new();
        for (specs, host_initiated) in [(&self.vsock_connect, false), (&self.vsock_listen, true)] {
            for spec in specs {
                vsock.push(parse_vsock_spec(spec, host_initiated)?);
            }
        }
        let mut seen_ports: Vec<u32> = Vec::new();
        for p in &vsock {
            if seen_ports.contains(&p.port) {
                return Err(format!("duplicate vsock port {}", p.port));
            }
            seen_ports.push(p.port);
        }

        if let Some(workdir) = &self.workdir {
            if !workdir.starts_with('/') {
                return Err(format!(
                    "--workdir must be an absolute guest path, got '{workdir}'"
                ));
            }
            ensure_cmdline_safe("--workdir", workdir)?;
        }

        let mut env = Vec::new();
        for pair in &self.env {
            let (key, _value) = pair
                .split_once('=')
                .ok_or_else(|| format!("--env expects KEY=VALUE, got '{pair}'"))?;
            validate_env_key(key)?;
            ensure_cmdline_safe("--env", pair)?;
            env.push(pair.clone());
        }
        for key in &self.env_passthrough {
            validate_env_key(key)?;
            if let Ok(value) = std::env::var(key) {
                let pair = format!("{key}={value}");
                ensure_cmdline_safe("--env-passthrough", &pair)?;
                env.push(pair);
            }
        }

        let mut guest_command = self.guest_command.into_iter();
        let exec_path = guest_command.next().ok_or("guest command is required")?;
        let exec_args: Vec<String> = guest_command.collect();
        if !exec_path.starts_with('/') {
            return Err(format!(
                "guest executable must be an absolute guest path, got '{exec_path}'"
            ));
        }
        ensure_cmdline_safe("guest executable", &exec_path)?;
        for arg in &exec_args {
            ensure_cmdline_safe("guest argument", arg)?;
        }

        let budget: usize = exec_path.len()
            + exec_args.iter().map(|a| a.len() + 1).sum::<usize>()
            + env.iter().map(|e| e.len() + 1).sum::<usize>();
        if budget > CMDLINE_BUDGET {
            return Err(format!(
                "guest command + env is {budget} bytes; the kernel-cmdline budget is \
                 {CMDLINE_BUDGET}. Deliver the workload as a script file over virtio-fs \
                 (e.g. `-- /bin/sh /ctl/run.sh`) instead of inline arguments"
            ));
        }

        Ok(BootPlan {
            root_fs: self.root_fs,
            vcpus: self.vcpus,
            mem_mib: self.mem_mib,
            virtiofs,
            vsock,
            workdir: self.workdir,
            env,
            exec_path,
            exec_args,
            console_log: self.console_log,
            libkrun_dir: self.libkrun_dir,
            krun_log_level: self.krun_log_level,
        })
    }
}

/// Exec path, args and env ride the kernel cmdline: printable ASCII only.
fn ensure_cmdline_safe(what: &str, value: &str) -> Result<(), String> {
    if value.bytes().all(|b| (0x20..=0x7e).contains(&b)) {
        Ok(())
    } else {
        Err(format!(
            "{what} contains non-printable or non-ASCII bytes ('{value}'); guest command \
             material rides the kernel cmdline — deliver it as a script file over virtio-fs"
        ))
    }
}

fn validate_virtiofs_tag(tag: &str) -> Result<(), String> {
    let ok = !tag.is_empty()
        && tag.len() <= 36
        && tag
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.');
    if ok {
        Ok(())
    } else {
        Err(format!(
            "invalid virtio-fs tag '{tag}' (1-36 chars from [A-Za-z0-9._-])"
        ))
    }
}

fn validate_env_key(key: &str) -> Result<(), String> {
    let mut bytes = key.bytes();
    let head_ok = bytes
        .next()
        .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_');
    if head_ok
        && key
            .bytes()
            .skip(1)
            .all(|b| b.is_ascii_alphanumeric() || b == b'_')
    {
        Ok(())
    } else {
        Err(format!("invalid environment variable name '{key}'"))
    }
}

fn parse_vsock_spec(spec: &str, host_initiated: bool) -> Result<VsockPort, String> {
    let flag = if host_initiated {
        "--vsock-listen"
    } else {
        "--vsock-connect"
    };
    let (port, socket) = spec
        .split_once('=')
        .ok_or_else(|| format!("{flag} expects PORT=SOCKET, got '{spec}'"))?;
    let port: u32 = port
        .parse()
        .map_err(|_| format!("{flag}: invalid port '{port}'"))?;
    if port == 0 {
        return Err(format!("{flag}: port must be > 0"));
    }
    if socket.is_empty() {
        return Err(format!("{flag}: socket path is empty"));
    }
    if socket.len() > MAX_SOCKET_PATH_BYTES {
        return Err(format!(
            "{flag}: socket path is {} bytes; unix socket paths must be at most \
             {MAX_SOCKET_PATH_BYTES} bytes (SUN_LEN): {socket}",
            socket.len()
        ));
    }
    Ok(VsockPort {
        port,
        socket: PathBuf::from(socket),
        host_initiated,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(std::iter::once("intentd-microvm-helper").chain(args.iter().copied()))
            .expect("args should parse")
    }

    fn base_args(root: &std::path::Path) -> Vec<String> {
        vec![
            "--root-fs".into(),
            root.display().to_string(),
            "--".into(),
            "/bin/sh".into(),
            "/ctl/run.sh".into(),
        ]
    }

    fn plan_with(root: &std::path::Path, extra: &[&str]) -> Result<BootPlan, String> {
        let mut args: Vec<String> = vec!["--root-fs".into(), root.display().to_string()];
        args.extend(extra.iter().map(|s| s.to_string()));
        args.extend(["--".into(), "/bin/sh".into(), "/ctl/run.sh".into()]);
        let refs: Vec<&str> = args.iter().map(String::as_str).collect();
        parse(&refs).into_plan()
    }

    #[test]
    fn minimal_plan_defaults() {
        let root = tempfile::tempdir().unwrap();
        let args = base_args(root.path());
        let refs: Vec<&str> = args.iter().map(String::as_str).collect();
        let plan = parse(&refs).into_plan().unwrap();
        assert_eq!(plan.vcpus, 2);
        assert_eq!(plan.mem_mib, 2048);
        assert_eq!(plan.exec_path, "/bin/sh");
        assert_eq!(plan.exec_args, vec!["/ctl/run.sh"]);
        assert!(plan.env.is_empty());
        assert!(plan.virtiofs.is_empty());
        assert!(plan.vsock.is_empty());
    }

    #[test]
    fn missing_guest_command_fails_parse() {
        let root = tempfile::tempdir().unwrap();
        let res = Cli::try_parse_from([
            "intentd-microvm-helper",
            "--root-fs",
            &root.path().display().to_string(),
        ]);
        assert!(res.is_err());
    }

    #[test]
    fn rejects_zero_vcpus_and_small_mem() {
        let root = tempfile::tempdir().unwrap();
        assert!(plan_with(root.path(), &["--vcpus", "0"])
            .unwrap_err()
            .contains("--vcpus"));
        assert!(plan_with(root.path(), &["--mem-mib", "64"])
            .unwrap_err()
            .contains("--mem-mib"));
    }

    #[test]
    fn rejects_missing_root_fs() {
        let err = plan_with(std::path::Path::new("/nonexistent/rootfs"), &[]).unwrap_err();
        assert!(err.contains("--root-fs"));
    }

    #[test]
    fn virtiofs_spec_parsing() {
        let root = tempfile::tempdir().unwrap();
        let share = tempfile::tempdir().unwrap();
        let spec = format!("work={}", share.path().display());
        let plan = plan_with(root.path(), &["--virtiofs", &spec]).unwrap();
        assert_eq!(
            plan.virtiofs,
            vec![("work".to_string(), share.path().to_path_buf())]
        );

        assert!(plan_with(root.path(), &["--virtiofs", "noequals"]).is_err());
        let bad_tag = format!("bad tag={}", share.path().display());
        assert!(plan_with(root.path(), &["--virtiofs", &bad_tag]).is_err());
        assert!(plan_with(root.path(), &["--virtiofs", "work=/nonexistent/dir"]).is_err());
    }

    #[test]
    fn vsock_spec_parsing() {
        let root = tempfile::tempdir().unwrap();
        let plan = plan_with(
            root.path(),
            &[
                "--vsock-connect",
                "1024=/tmp/a.sock",
                "--vsock-listen",
                "1025=/tmp/b.sock",
            ],
        )
        .unwrap();
        assert_eq!(
            plan.vsock,
            vec![
                VsockPort {
                    port: 1024,
                    socket: PathBuf::from("/tmp/a.sock"),
                    host_initiated: false
                },
                VsockPort {
                    port: 1025,
                    socket: PathBuf::from("/tmp/b.sock"),
                    host_initiated: true
                },
            ]
        );

        assert!(plan_with(root.path(), &["--vsock-connect", "0=/tmp/a.sock"]).is_err());
        assert!(plan_with(root.path(), &["--vsock-connect", "abc=/tmp/a.sock"]).is_err());
        assert!(plan_with(root.path(), &["--vsock-connect", "1024="]).is_err());
        let long_sock = format!("1026=/tmp/{}.sock", "x".repeat(MAX_SOCKET_PATH_BYTES));
        let err = plan_with(root.path(), &["--vsock-listen", &long_sock]).unwrap_err();
        assert!(err.contains("SUN_LEN"));
        let err = plan_with(
            root.path(),
            &[
                "--vsock-connect",
                "1024=/tmp/a.sock",
                "--vsock-listen",
                "1024=/tmp/b.sock",
            ],
        )
        .unwrap_err();
        assert!(err.contains("duplicate vsock port"));
    }

    #[test]
    fn env_allowlist_and_passthrough() {
        let root = tempfile::tempdir().unwrap();
        let plan = plan_with(root.path(), &["--env", "FOO=bar", "--env", "BAZ="]).unwrap();
        assert_eq!(plan.env, vec!["FOO=bar", "BAZ="]);

        assert!(plan_with(root.path(), &["--env", "NOVALUE"]).is_err());
        assert!(plan_with(root.path(), &["--env", "1BAD=x"]).is_err());
        assert!(plan_with(root.path(), &["--env-passthrough", "BAD-NAME"]).is_err());

        // Passthrough: present var is forwarded, absent var is skipped.
        std::env::set_var("MICROVM_HELPER_TEST_VAR", "present");
        let plan = plan_with(
            root.path(),
            &[
                "--env-passthrough",
                "MICROVM_HELPER_TEST_VAR",
                "--env-passthrough",
                "MICROVM_HELPER_ABSENT_VAR",
            ],
        )
        .unwrap();
        std::env::remove_var("MICROVM_HELPER_TEST_VAR");
        assert_eq!(plan.env, vec!["MICROVM_HELPER_TEST_VAR=present"]);
    }

    #[test]
    fn guest_command_constraints() {
        let root = tempfile::tempdir().unwrap();
        // Relative exec path rejected.
        let mut args: Vec<String> = vec!["--root-fs".into(), root.path().display().to_string()];
        args.extend(["--".into(), "sh".into()]);
        let refs: Vec<&str> = args.iter().map(String::as_str).collect();
        assert!(parse(&refs).into_plan().is_err());

        // Non-ASCII argument rejected with a script-file hint.
        let mut args: Vec<String> = vec!["--root-fs".into(), root.path().display().to_string()];
        args.extend([
            "--".into(),
            "/bin/sh".into(),
            "-c".into(),
            "echo héllo".into(),
        ]);
        let refs: Vec<&str> = args.iter().map(String::as_str).collect();
        let err = parse(&refs).into_plan().unwrap_err();
        assert!(err.contains("script file"));
    }

    #[test]
    fn cmdline_budget_enforced() {
        let root = tempfile::tempdir().unwrap();
        let big = "x".repeat(CMDLINE_BUDGET);
        let mut args: Vec<String> = vec!["--root-fs".into(), root.path().display().to_string()];
        args.extend(["--".into(), "/bin/sh".into(), "-c".into(), big]);
        let refs: Vec<&str> = args.iter().map(String::as_str).collect();
        let err = parse(&refs).into_plan().unwrap_err();
        assert!(err.contains("kernel-cmdline budget"));
    }

    #[test]
    fn workdir_must_be_absolute() {
        let root = tempfile::tempdir().unwrap();
        assert!(plan_with(root.path(), &["--workdir", "relative/path"]).is_err());
        let plan = plan_with(root.path(), &["--workdir", "/work"]).unwrap();
        assert_eq!(plan.workdir.as_deref(), Some("/work"));
    }
}
