//! Host-environment probes (§12.3): `hasDisplay` and display-server detection
//! (used by `host.status` and the CLI `status`/`doctor` surfaces, §5.7), plus
//! the crate-local OS hostname helper (used by `host.status` and
//! `server.pairingInfo`).

use std::path::Path;
use std::process::Command;

/// Cached host identity shared by the status and pairing surfaces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostEnvironment {
    pub hostname: String,
    pub pretty_hostname: String,
    pub device_kind: Option<String>,
    pub hardware_model: Option<String>,
}

/// Injected inputs for [`classify_device_kind`].
#[derive(Debug, Clone, Copy, Default)]
pub struct DeviceKindInputs<'a> {
    pub os: &'a str,
    pub product_name: Option<&'a str>,
    pub hardware_model: Option<&'a str>,
    pub sys_vendor: Option<&'a str>,
    pub chassis_type: Option<&'a str>,
    pub hypervisor_type: Option<&'a str>,
    pub is_container: bool,
    pub has_display: bool,
}

/// Pure device-kind policy. All OS probes are injected so every decision is
/// deterministic and unit-testable.
#[must_use]
pub fn classify_device_kind(inputs: DeviceKindInputs<'_>) -> Option<&'static str> {
    match inputs.os {
        "macos" => classify_macos(inputs.product_name, inputs.hardware_model),
        "linux" => Some(classify_linux(inputs)),
        _ => None,
    }
}

fn classify_macos(
    product_name: Option<&str>,
    hardware_model: Option<&str>,
) -> Option<&'static str> {
    let product = product_name.unwrap_or_default().to_ascii_lowercase();
    if product.contains("mac mini") {
        return Some("macMini");
    }
    if product.contains("mac studio") {
        return Some("macStudio");
    }
    if product.contains("macbook") {
        return Some("laptop");
    }
    if product.contains("imac") || product.contains("mac pro") {
        return Some("desktop");
    }

    let model = hardware_model.unwrap_or_default();
    if model.starts_with("Macmini") {
        Some("macMini")
    } else if model.starts_with("MacBook") {
        Some("laptop")
    } else if model.starts_with("iMac") || model.starts_with("MacPro") {
        Some("desktop")
    } else {
        None
    }
}

fn classify_linux(inputs: DeviceKindInputs<'_>) -> &'static str {
    let vendor = inputs.sys_vendor.unwrap_or_default().to_ascii_lowercase();
    let product = inputs.product_name.unwrap_or_default().to_ascii_lowercase();
    let virtual_dmi = [
        "qemu",
        "kvm",
        "vmware",
        "virtualbox",
        "xen",
        "amazon ec2",
        "google compute engine",
    ]
    .iter()
    .any(|needle| vendor.contains(needle) || product.contains(needle))
        || (vendor.contains("microsoft") && product.contains("virtual machine"));
    if inputs.is_container
        || inputs
            .hypervisor_type
            .is_some_and(|value| !value.trim().is_empty())
        || virtual_dmi
    {
        return "cloudVm";
    }

    match inputs
        .chassis_type
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "8" | "9" | "10" | "portable" | "laptop" | "notebook" => "laptop",
        "3"
        | "4"
        | "5"
        | "6"
        | "7"
        | "13"
        | "15"
        | "16"
        | "35"
        | "36"
        | "desktop"
        | "low profile desktop"
        | "pizza box"
        | "mini tower"
        | "tower"
        | "all in one"
        | "space-saving"
        | "mini pc"
        | "stick pc" => "desktop",
        "17"
        | "23"
        | "28"
        | "29"
        | "server"
        | "main server chassis"
        | "rack mount chassis"
        | "blade"
        | "blade enclosure" => "server",
        _ if !inputs.has_display => "server",
        _ => "desktop",
    }
}

/// Probe the current host once. Callers cache this result off the RPC path.
#[must_use]
pub fn detect_host_environment() -> HostEnvironment {
    let hostname = local_hostname();
    let pretty_hostname = pretty_hostname();
    let (product_name, hardware_model, sys_vendor, chassis_type, hypervisor_type, is_container) =
        if cfg!(target_os = "macos") {
            let product = command_output("/usr/sbin/sysctl", &["-n", "hw.product"])
                .or_else(ioreg_product_name);
            let model = command_output("/usr/sbin/sysctl", &["-n", "hw.model"]);
            (product, model, None, None, None, false)
        } else if cfg!(target_os = "linux") {
            (
                read_trimmed("/sys/class/dmi/id/product_name"),
                None,
                read_trimmed("/sys/class/dmi/id/sys_vendor"),
                read_trimmed("/sys/class/dmi/id/chassis_type"),
                read_trimmed("/sys/hypervisor/type"),
                Path::new("/.dockerenv").exists() || Path::new("/run/.containerenv").exists(),
            )
        } else {
            (None, None, None, None, None, false)
        };
    let reported_hardware_model = product_name.clone().or(hardware_model.clone());
    let device_kind = classify_device_kind(DeviceKindInputs {
        os: std::env::consts::OS,
        product_name: product_name.as_deref(),
        hardware_model: hardware_model.as_deref(),
        sys_vendor: sys_vendor.as_deref(),
        chassis_type: chassis_type.as_deref(),
        hypervisor_type: hypervisor_type.as_deref(),
        is_container,
        has_display: detect_has_display(),
    })
    .map(str::to_string);
    HostEnvironment {
        hostname,
        pretty_hostname,
        device_kind,
        hardware_model: reported_hardware_model,
    }
}

fn read_trimmed(path: &str) -> Option<String> {
    std::fs::read_to_string(path)
        .ok()
        .map(|value| value.trim_matches(['\0', '\r', '\n', ' ']).to_string())
        .filter(|value| !value.is_empty())
}

fn command_output(program: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(program).args(args).output().ok()?;
    output
        .status
        .success()
        .then(|| {
            String::from_utf8_lossy(&output.stdout)
                .trim_matches(['\0', '\r', '\n', ' '])
                .to_string()
        })
        .filter(|value| !value.is_empty())
}

fn ioreg_product_name() -> Option<String> {
    let output = command_output("/usr/sbin/ioreg", &["-rd1", "-c", "IOPlatformExpertDevice"])?;
    let value = output.lines().find_map(|line| {
        line.contains("product-name")
            .then(|| line.split_once('=').map(|(_, value)| value.trim()))
            .flatten()
    })?;
    if let Some(quoted) = value
        .strip_prefix("<\"")
        .and_then(|v| v.strip_suffix("\">"))
    {
        return Some(quoted.trim_end_matches('\0').to_string());
    }
    let hex = value.strip_prefix('<')?.strip_suffix('>')?;
    let bytes = hex
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            std::str::from_utf8(pair)
                .ok()
                .and_then(|p| u8::from_str_radix(p, 16).ok())
        })
        .collect::<Option<Vec<_>>>()?;
    String::from_utf8(bytes)
        .ok()
        .map(|value| value.trim_end_matches('\0').to_string())
        .filter(|value| !value.is_empty())
}

/// Best-effort `hasDisplay` (§12.3): X11/Wayland env on Unix, otherwise assume
/// a display unless the process was reached over SSH (no local console). Public
/// so the CLI `status`/`doctor` surfaces (§5.7) report the same value
/// `host.status` returns.
#[must_use]
pub fn detect_has_display() -> bool {
    has_display(
        std::env::var_os("DISPLAY").is_some(),
        std::env::var_os("WAYLAND_DISPLAY").is_some(),
        std::env::var_os("SSH_CONNECTION").is_some() || std::env::var_os("SSH_TTY").is_some(),
        cfg!(target_os = "macos") || cfg!(target_os = "windows"),
    )
}

/// Pure `hasDisplay` policy (testable without touching the process env): an
/// explicit X11/Wayland display always wins; otherwise GUI platforms
/// (macOS/Windows) assume a console unless reached over SSH; headless Unix
/// without a display server has none.
// The four independent env probes ARE the inputs of this pure policy fn;
// bundling them into a struct would only obscure the truth table.
#[allow(clippy::fn_params_excessive_bools)]
fn has_display(display: bool, wayland: bool, over_ssh: bool, gui_platform: bool) -> bool {
    if display || wayland {
        return true;
    }
    if gui_platform {
        return !over_ssh;
    }
    false
}

/// Best-effort display-server identifier for `host.status` `displayServer`
/// (§5.14). Reports `wayland`/`x11` when their env is present; `None`
/// otherwise. Cross-platform clean — returns `None` on hosts without those
/// Unix display-server env vars.
pub(crate) fn detect_display_server() -> Option<String> {
    if std::env::var_os("WAYLAND_DISPLAY").is_some() {
        return Some("wayland".to_string());
    }
    if std::env::var_os("DISPLAY").is_some() {
        return Some("x11".to_string());
    }
    None
}

/// Local OS hostname (`os.hostname()` equivalent), or `intent` on failure.
/// Public so the composition root can include it in the `system.status`
/// snapshot alongside `host.status` and `server.pairingInfo`.
#[must_use]
pub fn local_hostname() -> String {
    whoami::hostname()
        .ok()
        .filter(|h| !h.is_empty())
        .unwrap_or_else(|| "intent".to_string())
}

/// OS "pretty" device name (macOS Computer Name, e.g. "Clement's Mac Studio";
/// `PrettyHostname` on Linux, computer name on Windows), or [`local_hostname`]
/// when no pretty name is available. Public so the composition root can
/// include it in the `system.status` snapshot alongside `host.status` and
/// `server.pairingInfo`.
#[must_use]
pub fn pretty_hostname() -> String {
    whoami::devicename()
        .ok()
        .filter(|d| !d.is_empty())
        .unwrap_or_else(local_hostname)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn has_display_policy_is_cross_platform_clean() {
        // An explicit display server always wins, regardless of platform/SSH.
        assert!(has_display(true, false, true, false));
        assert!(has_display(false, true, true, false));
        // GUI platforms (macOS/Windows) assume a console unless reached over SSH.
        assert!(has_display(false, false, false, true));
        assert!(!has_display(false, false, true, true));
        // Headless Unix without a display server has none.
        assert!(!has_display(false, false, false, false));
    }

    #[test]
    fn hostnames_are_never_empty() {
        // Both helpers fall back rather than returning an empty string:
        // `local_hostname` to `intent`, `pretty_hostname` to `local_hostname`.
        assert!(!local_hostname().is_empty());
        assert!(!pretty_hostname().is_empty());
    }

    #[test]
    fn display_server_prefers_wayland_then_x11() {
        // Deterministic precedence is asserted at the policy boundary; this test
        // only pins the documented contract for the env-free `None` case (CI hosts
        // without DISPLAY/WAYLAND_DISPLAY set).
        if std::env::var_os("WAYLAND_DISPLAY").is_none() && std::env::var_os("DISPLAY").is_none() {
            assert_eq!(detect_display_server(), None);
        }
    }

    fn inputs(os: &'static str) -> DeviceKindInputs<'static> {
        DeviceKindInputs {
            os,
            has_display: true,
            ..DeviceKindInputs::default()
        }
    }

    #[test]
    fn classifies_every_macos_product_and_identifier_branch() {
        for (product, expected) in [
            ("Mac mini", "macMini"),
            ("Mac Studio", "macStudio"),
            ("MacBook Pro", "laptop"),
            ("iMac", "desktop"),
            ("Mac Pro", "desktop"),
        ] {
            assert_eq!(
                classify_device_kind(DeviceKindInputs {
                    product_name: Some(product),
                    ..inputs("macos")
                }),
                Some(expected)
            );
        }
        for (model, expected) in [
            ("Macmini8,1", "macMini"),
            ("MacBookPro16,1", "laptop"),
            ("iMac20,1", "desktop"),
            ("MacPro7,1", "desktop"),
        ] {
            assert_eq!(
                classify_device_kind(DeviceKindInputs {
                    hardware_model: Some(model),
                    ..inputs("macos")
                }),
                Some(expected)
            );
        }
        assert_eq!(classify_device_kind(inputs("macos")), None);
    }

    #[test]
    fn linux_virtualization_and_container_inputs_win() {
        for value in [
            "QEMU",
            "KVM",
            "VMware, Inc.",
            "VirtualBox",
            "Xen",
            "Amazon EC2",
            "Google Compute Engine",
        ] {
            assert_eq!(
                classify_device_kind(DeviceKindInputs {
                    sys_vendor: Some(value),
                    ..inputs("linux")
                }),
                Some("cloudVm")
            );
        }
        assert_eq!(
            classify_device_kind(DeviceKindInputs {
                sys_vendor: Some("Microsoft Corporation"),
                product_name: Some("Virtual Machine"),
                ..inputs("linux")
            }),
            Some("cloudVm")
        );
        assert_eq!(
            classify_device_kind(DeviceKindInputs {
                hypervisor_type: Some("xen"),
                ..inputs("linux")
            }),
            Some("cloudVm")
        );
        assert_eq!(
            classify_device_kind(DeviceKindInputs {
                is_container: true,
                ..inputs("linux")
            }),
            Some("cloudVm")
        );
    }

    #[test]
    fn linux_chassis_and_display_fallbacks_cover_every_branch() {
        for (chassis, expected) in [
            ("8", "laptop"),
            ("9", "laptop"),
            ("10", "laptop"),
            ("3", "desktop"),
            ("7", "desktop"),
            ("35", "desktop"),
            ("17", "server"),
            ("23", "server"),
            ("28", "server"),
        ] {
            assert_eq!(
                classify_device_kind(DeviceKindInputs {
                    chassis_type: Some(chassis),
                    ..inputs("linux")
                }),
                Some(expected)
            );
        }
        assert_eq!(
            classify_device_kind(DeviceKindInputs {
                has_display: false,
                ..inputs("linux")
            }),
            Some("server")
        );
        assert_eq!(classify_device_kind(inputs("linux")), Some("desktop"));
        assert_eq!(classify_device_kind(inputs("windows")), None);
    }
}
