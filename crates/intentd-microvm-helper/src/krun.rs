//! macOS libkrun boot path.
//!
//! libkrun is loaded at runtime with `dlopen` rather than linked at build
//! time, so the workspace builds everywhere (CI runners, hosts without
//! Homebrew libkrun) and the dylib location is a runtime concern:
//!
//! - The dylib directory is resolved from `--libkrun-dir`, then
//!   `$INTENTD_LIBKRUN_DIR`, then the helper's own directory, then
//!   `<helper dir>/../lib`, then `/opt/homebrew/lib` (dev install).
//! - libkrun itself dlopens `libkrunfw.5.dylib` by bare leaf name at boot.
//!   Pre-loading libkrunfw by absolute path does NOT satisfy that lookup:
//!   dyld matches already-loaded images by install name (the packaging
//!   prefix's full path), not leaf name, and DYLD_* env vars are captured at
//!   process launch so setenv can't help either. What dyld's leaf-name search
//!   does consult is the current working directory, so we chdir into the
//!   resolved dylib directory before booting (all host paths in the plan are
//!   made absolute first). No install_name/DYLD_* surgery is needed on
//!   bundled dylibs.

use std::ffi::{c_char, c_void, CString};
use std::path::{Path, PathBuf};

use crate::cli::BootPlan;
use crate::{EXIT_KRUN_API, EXIT_UNAVAILABLE};

const LIBKRUN_NAMES: [&str; 2] = ["libkrun.dylib", "libkrun.1.dylib"];
const LIBKRUNFW_NAMES: [&str; 2] = ["libkrunfw.5.dylib", "libkrunfw.dylib"];

pub struct BootError {
    pub exit_code: i32,
    pub message: String,
}

fn unavailable(message: String) -> BootError {
    BootError {
        exit_code: EXIT_UNAVAILABLE,
        message,
    }
}

fn api_error(message: String) -> BootError {
    BootError {
        exit_code: EXIT_KRUN_API,
        message,
    }
}

type FnCreateCtx = unsafe extern "C" fn() -> i32;
type FnSetLogLevel = unsafe extern "C" fn(u32) -> i32;
type FnSetVmConfig = unsafe extern "C" fn(u32, u8, u32) -> i32;
type FnSetPath = unsafe extern "C" fn(u32, *const c_char) -> i32;
type FnAddVirtiofs = unsafe extern "C" fn(u32, *const c_char, *const c_char) -> i32;
type FnAddVsockPort = unsafe extern "C" fn(u32, u32, *const c_char) -> i32;
type FnAddVsockPort2 = unsafe extern "C" fn(u32, u32, *const c_char, bool) -> i32;
type FnSetExec =
    unsafe extern "C" fn(u32, *const c_char, *const *const c_char, *const *const c_char) -> i32;
type FnStartEnter = unsafe extern "C" fn(u32) -> i32;

/// Configures and enters the microVM. Only returns on failure.
pub fn boot(plan: &BootPlan) -> BootError {
    match try_boot(plan) {
        Ok(never) => match never {},
        Err(err) => err,
    }
}

enum Never {}

fn try_boot(plan: &BootPlan) -> Result<Never, BootError> {
    let plan = absolutize_plan(plan)?;
    let plan = &plan;
    let dir = resolve_libkrun_dir(plan)?;
    enter_dylib_dir(&dir)?;
    let lib = load_libkrun(&dir)?;

    let set_log_level: FnSetLogLevel = sym(lib, "krun_set_log_level")?;
    let create_ctx: FnCreateCtx = sym(lib, "krun_create_ctx")?;
    let set_vm_config: FnSetVmConfig = sym(lib, "krun_set_vm_config")?;
    let set_root: FnSetPath = sym(lib, "krun_set_root")?;
    let add_virtiofs: FnAddVirtiofs = sym(lib, "krun_add_virtiofs")?;
    let set_exec: FnSetExec = sym(lib, "krun_set_exec")?;
    let start_enter: FnStartEnter = sym(lib, "krun_start_enter")?;

    unsafe {
        // Best-effort; RUST_LOG-style env can override inside libkrun.
        let _ = set_log_level(plan.krun_log_level);
    }

    let ctx = unsafe { create_ctx() };
    if ctx < 0 {
        return Err(api_error(format!("krun_create_ctx failed: {ctx}")));
    }
    let ctx = ctx as u32;

    check("krun_set_vm_config", unsafe {
        set_vm_config(ctx, plan.vcpus, plan.mem_mib)
    })?;

    let root = cstring_from_path("--root-fs", &plan.root_fs)?;
    check("krun_set_root", unsafe { set_root(ctx, root.as_ptr()) })?;

    for (tag, dir) in &plan.virtiofs {
        let c_tag = cstring("virtio-fs tag", tag)?;
        let c_dir = cstring_from_path("virtio-fs path", dir)?;
        check("krun_add_virtiofs", unsafe {
            add_virtiofs(ctx, c_tag.as_ptr(), c_dir.as_ptr())
        })?;
    }

    for port in &plan.vsock {
        let c_sock = cstring_from_path("vsock socket", &port.socket)?;
        // krun_add_vsock_port2 (with the listen flag) needs libkrun >= 1.10;
        // fall back to krun_add_vsock_port for guest-initiated ports.
        match sym::<FnAddVsockPort2>(lib, "krun_add_vsock_port2") {
            Ok(add2) => check("krun_add_vsock_port2", unsafe {
                add2(ctx, port.port, c_sock.as_ptr(), port.host_initiated)
            })?,
            Err(_) if !port.host_initiated => {
                let add: FnAddVsockPort = sym(lib, "krun_add_vsock_port")?;
                check("krun_add_vsock_port", unsafe {
                    add(ctx, port.port, c_sock.as_ptr())
                })?;
            }
            Err(err) => return Err(err),
        }
    }

    if let Some(workdir) = &plan.workdir {
        let set_workdir: FnSetPath = sym(lib, "krun_set_workdir")?;
        let c_workdir = cstring("--workdir", workdir)?;
        check("krun_set_workdir", unsafe {
            set_workdir(ctx, c_workdir.as_ptr())
        })?;
    }

    if let Some(console) = &plan.console_log {
        let set_console: FnSetPath = sym(lib, "krun_set_console_output")?;
        let c_console = cstring_from_path("--console-log", console)?;
        check("krun_set_console_output", unsafe {
            set_console(ctx, c_console.as_ptr())
        })?;
    }

    let exec_path = cstring("guest executable", &plan.exec_path)?;
    let args: Vec<CString> = plan
        .exec_args
        .iter()
        .map(|a| cstring("guest argument", a))
        .collect::<Result<_, _>>()?;
    let env: Vec<CString> = plan
        .env
        .iter()
        .map(|e| cstring("guest env", e))
        .collect::<Result<_, _>>()?;
    let mut argv: Vec<*const c_char> = args.iter().map(|a| a.as_ptr()).collect();
    argv.push(std::ptr::null());
    // envp must be non-NULL: a NULL envp makes libkrun forward the entire
    // host environment, defeating the allowlist.
    let mut envp: Vec<*const c_char> = env.iter().map(|e| e.as_ptr()).collect();
    envp.push(std::ptr::null());

    check("krun_set_exec", unsafe {
        set_exec(ctx, exec_path.as_ptr(), argv.as_ptr(), envp.as_ptr())
    })?;

    // On success this does not return: the helper process becomes the VM and
    // exits with the guest command's exit status.
    let err = unsafe { start_enter(ctx) };
    Err(api_error(format!(
        "krun_start_enter failed: {err} (is the binary signed with the \
         com.apple.security.hypervisor entitlement? see scripts/sign-microvm-helper.sh)"
    )))
}

/// Picks the first candidate directory that contains a libkrun dylib.
fn resolve_libkrun_dir(plan: &BootPlan) -> Result<PathBuf, BootError> {
    let mut candidates: Vec<PathBuf> = Vec::new();
    if let Some(dir) = &plan.libkrun_dir {
        candidates.push(dir.clone());
    }
    if let Ok(dir) = std::env::var("INTENTD_LIBKRUN_DIR") {
        if !dir.is_empty() {
            candidates.push(PathBuf::from(dir));
        }
    }
    if let Ok(exe) = std::env::current_exe() {
        if let Some(exe_dir) = exe.parent() {
            candidates.push(exe_dir.to_path_buf());
            candidates.push(exe_dir.join("../lib"));
        }
    }
    candidates.push(PathBuf::from("/opt/homebrew/lib"));

    for dir in &candidates {
        if LIBKRUN_NAMES.iter().any(|name| dir.join(name).is_file()) {
            return Ok(dir.clone());
        }
    }
    Err(unavailable(format!(
        "libkrun.dylib not found (searched: {}); install libkrun or pass --libkrun-dir",
        candidates
            .iter()
            .map(|d| d.display().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    )))
}

/// Rewrites every host path in the plan to an absolute path so the process
/// can chdir into the dylib directory without breaking them.
fn absolutize_plan(plan: &BootPlan) -> Result<BootPlan, BootError> {
    let cwd = std::env::current_dir()
        .map_err(|e| unavailable(format!("cannot resolve current directory: {e}")))?;
    let abs = |p: &Path| -> PathBuf {
        if p.is_absolute() {
            p.to_path_buf()
        } else {
            cwd.join(p)
        }
    };
    Ok(BootPlan {
        root_fs: abs(&plan.root_fs),
        vcpus: plan.vcpus,
        mem_mib: plan.mem_mib,
        virtiofs: plan
            .virtiofs
            .iter()
            .map(|(tag, dir)| (tag.clone(), abs(dir)))
            .collect(),
        vsock: plan
            .vsock
            .iter()
            .map(|p| crate::cli::VsockPort {
                port: p.port,
                socket: abs(&p.socket),
                host_initiated: p.host_initiated,
            })
            .collect(),
        workdir: plan.workdir.clone(),
        env: plan.env.clone(),
        exec_path: plan.exec_path.clone(),
        exec_args: plan.exec_args.clone(),
        console_log: plan.console_log.as_deref().map(&abs),
        libkrun_dir: plan.libkrun_dir.as_deref().map(&abs),
        krun_log_level: plan.krun_log_level,
    })
}

/// chdir into the dylib directory: libkrun dlopens `libkrunfw.5.dylib` by
/// bare leaf name at boot, and the current working directory is the one
/// reliable place dyld's leaf-name search finds a bundled copy (see module
/// docs).
fn enter_dylib_dir(dir: &Path) -> Result<(), BootError> {
    std::env::set_current_dir(dir).map_err(|e| {
        unavailable(format!(
            "cannot chdir into libkrun dir {}: {e}",
            dir.display()
        ))
    })
}

/// dlopens libkrun, first verifying libkrunfw is present in the same
/// directory (libkrun needs it at boot; see [`enter_dylib_dir`]).
fn load_libkrun(dir: &Path) -> Result<*mut c_void, BootError> {
    if !LIBKRUNFW_NAMES.iter().any(|n| dir.join(n).is_file()) {
        return Err(unavailable(format!(
            "libkrunfw.5.dylib not found next to libkrun in {} (libkrun dlopens it \
             by bare name at boot)",
            dir.display()
        )));
    }

    let krun = LIBKRUN_NAMES
        .iter()
        .map(|n| dir.join(n))
        .find(|p| p.is_file())
        .expect("resolve_libkrun_dir guarantees a libkrun dylib exists");
    dlopen(&krun)
}

fn dlopen(path: &Path) -> Result<*mut c_void, BootError> {
    let c_path = cstring_from_path("dylib path", path).map_err(|e| unavailable(e.message))?;
    let handle = unsafe { libc::dlopen(c_path.as_ptr(), libc::RTLD_NOW | libc::RTLD_GLOBAL) };
    if handle.is_null() {
        Err(unavailable(format!(
            "failed to load {}: {}",
            path.display(),
            dlerror_message()
        )))
    } else {
        Ok(handle)
    }
}

fn sym<T: Copy>(lib: *mut c_void, name: &str) -> Result<T, BootError> {
    assert_eq!(std::mem::size_of::<T>(), std::mem::size_of::<*mut c_void>());
    let c_name = cstring("symbol name", name).map_err(|e| unavailable(e.message))?;
    let ptr = unsafe { libc::dlsym(lib, c_name.as_ptr()) };
    if ptr.is_null() {
        Err(unavailable(format!(
            "symbol {name} not found in libkrun: {}",
            dlerror_message()
        )))
    } else {
        Ok(unsafe { std::mem::transmute_copy::<*mut c_void, T>(&ptr) })
    }
}

fn dlerror_message() -> String {
    let err = unsafe { libc::dlerror() };
    if err.is_null() {
        "unknown dlerror".to_string()
    } else {
        unsafe { std::ffi::CStr::from_ptr(err) }
            .to_string_lossy()
            .into_owned()
    }
}

fn check(what: &str, ret: i32) -> Result<(), BootError> {
    if ret < 0 {
        Err(api_error(format!("{what} failed: {ret}")))
    } else {
        Ok(())
    }
}

fn cstring(what: &str, value: &str) -> Result<CString, BootError> {
    CString::new(value).map_err(|_| api_error(format!("{what} contains a NUL byte")))
}

fn cstring_from_path(what: &str, path: &Path) -> Result<CString, BootError> {
    cstring(
        what,
        path.to_str()
            .ok_or_else(|| api_error(format!("{what} is not valid UTF-8: {}", path.display())))?,
    )
}
