use std::collections::HashMap;

use super::*;

fn config(vars: &[(&str, &str)]) -> Result<ListenConfig, String> {
    let vars: HashMap<String, String> = vars
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect();
    ListenConfig::from_env(|name| vars.get(name).cloned(), Path::new("/data"))
}

#[test]
fn unset_listens_on_the_loopback_with_auth() {
    let config = config(&[]).unwrap();
    assert_eq!(
        config,
        ListenConfig {
            socket_path: PathBuf::from("/data/orchestrator.sock"),
            bind_addresses: vec![IpAddr::from([127, 0, 0, 1])],
            ws_port: DEFAULT_WS_PORT,
            tls_dir: PathBuf::from("/data"),
            insecure: false,
        }
    );
    assert_eq!(DEFAULT_WS_PORT, 5191, "next to intentd's 5181");
}

#[test]
fn blank_values_are_unset() {
    assert_eq!(
        config(&[
            (SOCKET_ENV, " "),
            (BIND_ENV, ""),
            (WS_PORT_ENV, " "),
            (TLS_DIR_ENV, ""),
            (INSECURE_ENV, "")
        ])
        .unwrap(),
        config(&[]).unwrap()
    );
}

#[test]
fn every_value_can_be_set() {
    let config = config(&[
        (SOCKET_ENV, "/run/o.sock"),
        (BIND_ENV, "10.0.0.5, 127.0.0.1"),
        (WS_PORT_ENV, "0"),
        (TLS_DIR_ENV, "/shared/tls"),
        (INSECURE_ENV, "1"),
    ])
    .unwrap();
    assert_eq!(config.socket_path, PathBuf::from("/run/o.sock"));
    assert_eq!(
        config.bind_addresses,
        [IpAddr::from([10, 0, 0, 5]), IpAddr::from([127, 0, 0, 1])]
    );
    assert_eq!(config.ws_port, 0);
    assert_eq!(config.tls_dir, PathBuf::from("/shared/tls"));
    assert!(config.insecure);
}

#[test]
fn the_loopback_is_always_bound_unless_a_wildcard_covers_it() {
    assert_eq!(
        config(&[(BIND_ENV, "10.0.0.5")]).unwrap().bind_addresses,
        [IpAddr::from([10, 0, 0, 5]), IpAddr::from([127, 0, 0, 1])]
    );
    for wide in ["0.0.0.0", "::"] {
        assert_eq!(
            config(&[(BIND_ENV, wide)]).unwrap().bind_addresses,
            [wide.parse::<IpAddr>().unwrap()]
        );
    }
}

#[test]
fn invalid_values_are_errors_never_defaults() {
    for (name, value) in [
        (BIND_ENV, "not-an-ip"),
        (BIND_ENV, "10.0.0.5,10.0.0.5"),
        (BIND_ENV, "0.0.0.0,10.0.0.5"),
        (WS_PORT_ENV, "70000"),
        (WS_PORT_ENV, "http"),
        (INSECURE_ENV, "yes"),
    ] {
        let error = config(&[(name, value)]).unwrap_err();
        assert!(error.contains(name), "{name}={value}: {error}");
    }
    assert!(!config(&[(INSECURE_ENV, "false")]).unwrap().insecure);
    assert!(!config(&[(INSECURE_ENV, "0")]).unwrap().insecure);
    assert!(config(&[(INSECURE_ENV, "true")]).unwrap().insecure);
}
