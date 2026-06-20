//! Golden-string tests for the launchd plist + systemd unit (§5.8).

use super::*;

#[test]
fn launchd_plist_matches_golden() {
    let plist = launchd_plist(
        "/usr/local/bin/intentd",
        "/data/logs/intentd.out.log",
        "/data/logs/intentd.err.log",
    );
    let expected = r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>ai.intent.intentd</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/local/bin/intentd</string>
        <string>serve</string>
        <string>--listen</string>
        <string>uds</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <dict>
        <key>Crashed</key>
        <true/>
        <key>SuccessfulExit</key>
        <false/>
    </dict>
    <key>StandardOutPath</key>
    <string>/data/logs/intentd.out.log</string>
    <key>StandardErrorPath</key>
    <string>/data/logs/intentd.err.log</string>
</dict>
</plist>
"#;
    assert_eq!(plist, expected);
}

#[test]
fn systemd_unit_matches_golden() {
    let unit = systemd_unit("/usr/local/bin/intentd");
    let expected = "[Unit]\n\
Description=Intent backend daemon (intentd)\n\
After=network.target\n\
\n\
[Service]\n\
Type=simple\n\
ExecStart=/usr/local/bin/intentd serve\n\
ExecStop=/usr/local/bin/intentd stop\n\
Restart=on-failure\n\
\n\
[Install]\n\
WantedBy=default.target\n";
    assert_eq!(unit, expected);
}

#[test]
fn launchd_keepalive_does_not_relaunch_clean_stop() {
    // §5.8: KeepAlive must gate on Crashed=true + SuccessfulExit=false so a
    // graceful `intentd stop` (clean exit) is NOT relaunched by launchd.
    let plist = launchd_plist("intentd", "/o", "/e");
    assert!(plist.contains("<key>SuccessfulExit</key>\n        <false/>"));
    assert!(plist.contains("<key>Crashed</key>\n        <true/>"));
}

#[test]
fn systemd_unit_stops_via_cli_and_restarts_on_failure() {
    let unit = systemd_unit("intentd");
    assert!(unit.contains("ExecStop=intentd stop"));
    assert!(unit.contains("Restart=on-failure"));
    assert!(unit.contains("Type=simple"));
    assert!(unit.contains("WantedBy=default.target"));
}
