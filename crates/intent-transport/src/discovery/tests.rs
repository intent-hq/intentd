//! Tests for `transport::discovery`: the deterministic name/TXT builders, the
//! disabled-flag gate (nothing published), and a hermetic advertise→browse
//! round-trip resolved via an `mdns-sd` browse channel.

use std::time::{Duration, Instant};

use mdns_sd::{ServiceDaemon, ServiceEvent};

use super::*;

fn test_caps() -> HostCapabilities {
    HostCapabilities {
        os: "macos".to_string(),
        arch: "aarch64".to_string(),
        has_display: true,
        locality: "local",
    }
}

#[test]
fn instance_name_strips_trailing_local() {
    assert_eq!(instance_name("studio.local"), "Intent on studio");
    assert_eq!(instance_name("studio"), "Intent on studio");
    // Only a trailing `.local` is stripped, mirroring the TS regex.
    assert_eq!(instance_name("my.local.host"), "Intent on my.local.host");
}

#[test]
fn txt_records_match_ts_keys_plus_host_capabilities() {
    let caps = test_caps();
    let records = txt_records("AB:CD:EF", "studio.local", &caps);
    // Order is deterministic: TS keys first, then the §12.3 fields.
    let keys: Vec<&str> = records.iter().map(|(k, _)| k.as_str()).collect();
    assert_eq!(
        keys,
        vec![
            "version",
            "path",
            "hostname",
            "fp",
            "os",
            "arch",
            "hasDisplay",
            "locality"
        ]
    );
    let get = |k: &str| {
        records
            .iter()
            .find(|(rk, _)| rk == k)
            .map(|(_, v)| v.as_str())
    };
    assert_eq!(get("version"), Some("1"));
    assert_eq!(get("path"), Some("/ws"));
    assert_eq!(get("hostname"), Some("studio.local"));
    assert_eq!(get("fp"), Some("AB:CD:EF"));
    assert_eq!(get("os"), Some("macos"));
    assert_eq!(get("arch"), Some("aarch64"));
    assert_eq!(get("hasDisplay"), Some("true"));
    assert_eq!(get("locality"), Some("local"));
}

#[test]
fn disabled_flag_publishes_nothing() {
    // The gate must not even construct a daemon when discovery is disabled.
    assert!(advertise_if_enabled(false, 5180, "AB:CD:EF", false).is_none());
}

#[test]
fn detect_sets_locality_from_resolved_flag() {
    // mDNS advertises the resolved locality (§5.14): TCP/WSS is remote by
    // default, forced-local listeners advertise local.
    assert_eq!(HostCapabilities::detect(true).locality, "local");
    assert_eq!(HostCapabilities::detect(false).locality, "remote");
}

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
fn display_server_prefers_wayland_then_x11() {
    // Deterministic precedence is asserted at the policy boundary; this test
    // only pins the documented contract for the env-free `None` case (CI hosts
    // without DISPLAY/WAYLAND_DISPLAY set).
    if std::env::var_os("WAYLAND_DISPLAY").is_none() && std::env::var_os("DISPLAY").is_none() {
        assert_eq!(detect_display_server(), None);
    }
}

/// Hermetic advertise→browse round-trip: a single `mdns-sd` daemon publishes
/// the service (via `addr_auto`, the production code path) and a browse channel
/// on the same daemon resolves it. This mirrors `mdns-sd`'s own integration
/// tests; a loopback-only interface cannot do this reliably (e.g. macOS `lo0`
/// is not multicast-capable), so the test uses the host's real interfaces.
#[test]
fn advertises_and_resolves_via_browser() {
    const PORT: u16 = 51999;
    let caps = test_caps();
    let info = build_service_info(PORT, "AB:CD:EF:01:23", "studio.local", &caps, None)
        .expect("build service info");
    let fullname = info.get_fullname().to_string();

    let daemon = ServiceDaemon::new().expect("create mdns daemon");
    daemon.register(info).expect("register service");
    let rx = daemon.browse(SERVICE_TYPE).expect("browse service type");

    let deadline = Instant::now() + Duration::from_secs(15);
    let resolved = loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "timed out waiting for mDNS resolution"
        );
        match rx.recv_timeout(remaining) {
            Ok(ServiceEvent::ServiceResolved(found)) if found.get_port() == PORT => break found,
            Ok(_) => continue,
            Err(_) => panic!("timed out waiting for mDNS resolution"),
        }
    };

    assert_eq!(resolved.get_type(), SERVICE_TYPE);
    assert!(
        resolved.get_fullname().starts_with("Intent on studio."),
        "unexpected fullname: {}",
        resolved.get_fullname()
    );
    assert_eq!(resolved.get_fullname(), fullname);
    assert_eq!(resolved.get_port(), PORT);
    assert_eq!(resolved.get_property_val_str("path"), Some("/ws"));
    assert_eq!(resolved.get_property_val_str("version"), Some("1"));
    assert_eq!(resolved.get_property_val_str("fp"), Some("AB:CD:EF:01:23"));
    assert_eq!(
        resolved.get_property_val_str("hostname"),
        Some("studio.local")
    );
    assert_eq!(resolved.get_property_val_str("hasDisplay"), Some("true"));
    assert_eq!(resolved.get_property_val_str("locality"), Some("local"));

    daemon.shutdown().expect("shutdown daemon");
}
