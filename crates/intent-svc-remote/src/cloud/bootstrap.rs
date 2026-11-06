//! The work inside a new sandbox, over E2B's authenticated process API: start
//! `intentd serve` with its WSS listener and tailcat tunnel, wait until it
//! answers on its local socket (and has its tunnel address), then mint the
//! executor credential for this backend there. The local socket is the
//! sandbox daemon's owner seat; the token goes straight into the machine
//! row's secret, never into a log or an error.

use std::collections::BTreeMap;
use std::time::Duration;

use serde_json::{json, Value};

use intent_core::machines::cloud_machine_unavailable;
use intent_core::{Error, Result};
use intent_nodes::e2b::{E2bClient, ProcessSpec, Sandbox, SecretString};

/// Test seam: dial the sandbox daemon at this host (its WSS port) instead of
/// its tailcat address, for a fake E2B that runs the sandbox locally.
const DIRECT_HOST_ENV: &str = "INTENTD_E2B_DIRECT_HOST";

/// Longest wait for the sandbox daemon (and its tunnel) before the mint.
const DAEMON_WAIT_SECS: u32 = 180;

/// Write the daemon's first settings (external listener and tunnel on)
/// unless the sandbox has some, then start it in the background unless it
/// already answers. The template never runs intentd, so this first start
/// makes the sandbox's own daemon id, certificate and tailcat key.
const START_SCRIPT: &str = r#"set -eu
dir="${INTENTD_DATA_DIR:-$HOME/.intent}"
mkdir -p "$dir"
if [ ! -f "$dir/config.toml" ]; then
  printf '[server.wsApi]\nenabled = true\n\n[server.tunnel]\nenabled = true\n' > "$dir/config.toml"
fi
if ! intentd call system.status >/dev/null 2>&1; then
  if command -v setsid >/dev/null 2>&1; then
    setsid nohup intentd serve >"$dir/serve.log" 2>&1 </dev/null &
  else
    nohup intentd serve >"$dir/serve.log" 2>&1 </dev/null &
  fi
fi
"#;

/// Wait for the daemon (and its tunnel address unless a direct route is
/// used), then mint. A mint that fails (listener not up yet) is retried; a
/// failed mint saves no credential.
const MINT_SCRIPT: &str = r#"set -u
i=0
while :; do
  if status=$(intentd call system.status 2>/dev/null); then
    if [ -n "${INTENT_DIRECT_ROUTE:-}" ] || printf '%s' "$status" | grep -q '"tcAddress"'; then
      if out=$(intentd call executor.mint --params "$INTENT_MINT_PARAMS" 2>/dev/null); then
        printf '%s\n' "$out"
        exit 0
      fi
    fi
  fi
  i=$((i + 1))
  if [ "$i" -ge "$INTENT_WAIT_SECS" ]; then
    echo "intentd or its tunnel did not start in the sandbox" >&2
    exit 3
  fi
  sleep 1
done
"#;

/// The sandbox daemon's credential and routes, as minted. `Debug` hides the
/// token.
pub(crate) struct Minted {
    pub(crate) machine_id: String,
    pub(crate) credential_id: String,
    pub(crate) token: String,
    pub(crate) port: u16,
    pub(crate) hosts: Vec<String>,
    pub(crate) tc_address: Option<String>,
    pub(crate) fingerprint: String,
}

impl std::fmt::Debug for Minted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Minted")
            .field("machine_id", &self.machine_id)
            .field("credential_id", &self.credential_id)
            .field("token", &"<redacted>")
            .finish_non_exhaustive()
    }
}

fn failed(message: impl Into<String>) -> Error {
    cloud_machine_unavailable(
        "bootstrap-failed",
        true,
        None,
        format!(
            "{} The sandbox was deleted; no machine was added.",
            message.into()
        ),
    )
}

/// Process stderr made safe for an error message: at most 200 characters,
/// without control characters. It never holds the token (only a successful
/// mint prints one, on stdout).
fn tail(stderr: &[u8]) -> String {
    let text = String::from_utf8_lossy(stderr);
    let text: String = text
        .trim()
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect();
    let start = text.chars().count().saturating_sub(200);
    text.chars().skip(start).collect()
}

/// Start intentd in `sandbox` and mint its executor credential for this
/// backend.
///
/// # Errors
///
/// `cloud-machine-unavailable` `bootstrap-failed` when a step fails, or the
/// E2B error of a failed call.
pub(crate) async fn run(
    client: &E2bClient,
    sandbox: &Sandbox,
    backend: &Value,
    mut on_daemon_started: impl FnMut(),
) -> Result<Minted> {
    let mut start = ProcessSpec::new("/bin/sh");
    start.args = vec!["-c".into(), START_SCRIPT.into()];
    start.timeout = Some(Duration::from_secs(60));
    let started = client
        .run_process(sandbox, &start)
        .await
        .map_err(|error| super::config::to_error(&error, super::config::Phase::Other))?;
    if !started.success() {
        return Err(failed(format!(
            "intentd could not start in the sandbox (exit {}): {}",
            started.exit.exit_code,
            tail(&started.stderr)
        )));
    }
    on_daemon_started();

    let direct = std::env::var(DIRECT_HOST_ENV)
        .ok()
        .filter(|host| !host.is_empty());
    let mut mint = ProcessSpec::new("/bin/sh");
    mint.args = vec!["-c".into(), MINT_SCRIPT.into()];
    mint.timeout = Some(Duration::from_secs(u64::from(DAEMON_WAIT_SECS) + 60));
    let mut envs = BTreeMap::new();
    envs.insert(
        "INTENT_MINT_PARAMS".to_owned(),
        SecretString::new(backend.to_string()),
    );
    envs.insert(
        "INTENT_WAIT_SECS".to_owned(),
        SecretString::new(DAEMON_WAIT_SECS.to_string()),
    );
    if direct.is_some() {
        envs.insert("INTENT_DIRECT_ROUTE".to_owned(), SecretString::new("1"));
    }
    mint.envs = envs;
    let minted = client
        .run_process(sandbox, &mint)
        .await
        .map_err(|error| super::config::to_error(&error, super::config::Phase::Other))?;
    if !minted.success() {
        return Err(failed(format!(
            "the sandbox's intentd did not mint a credential (exit {}): {}",
            minted.exit.exit_code,
            tail(&minted.stderr)
        )));
    }
    parse(&minted.stdout, direct)
}

/// Read the mint result (`{ credentialId, token, machine }`).
fn parse(stdout: &[u8], direct: Option<String>) -> Result<Minted> {
    let value: Value = serde_json::from_slice(stdout)
        .map_err(|_| failed("the sandbox's intentd printed an unreadable mint result."))?;
    let text = |value: &Value| value.as_str().filter(|s| !s.is_empty()).map(str::to_owned);
    let machine = &value["machine"];
    let (Some(machine_id), Some(credential_id), Some(token), Some(fingerprint)) = (
        text(&machine["daemonId"]),
        text(&value["credentialId"]),
        text(&value["token"]),
        text(&machine["fingerprint"]),
    ) else {
        return Err(failed("the sandbox's mint result is incomplete."));
    };
    let port = machine["port"]
        .as_u64()
        .and_then(|port| u16::try_from(port).ok())
        .filter(|port| *port != 0)
        .ok_or_else(|| failed("the sandbox's mint result has no port."))?;
    let tc_address = text(&machine["tcAddress"]);
    // Only the tailcat route: E2B's public URL ends TLS at its proxy, so the
    // certificate pin would fail there.
    let hosts = match direct {
        Some(host) => vec![host],
        None if tc_address.is_some() => Vec::new(),
        None => return Err(failed("the sandbox's tunnel has no address.")),
    };
    Ok(Minted {
        machine_id,
        credential_id,
        token,
        port,
        hosts,
        tc_address,
        fingerprint,
    })
}

/// The `executor.mint` params for this backend.
pub(crate) fn mint_params(backend_id: &str, backend_name: &str, fingerprint: Option<&str>) -> Value {
    let mut params = json!({ "backendId": backend_id, "backendName": backend_name });
    if let Some(fingerprint) = fingerprint {
        params["backendFingerprint"] = json!(fingerprint);
    }
    params
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mint_results_need_every_field_and_a_route() {
        let full = json!({
            "credentialId": "cred-1",
            "token": "secret-token-value",
            "machine": { "daemonId": "d-1", "port": 5181, "fingerprint": "AA", "tcAddress": "tc1" },
        });
        let minted = parse(full.to_string().as_bytes(), None).unwrap();
        assert!(minted.hosts.is_empty());
        assert_eq!(minted.tc_address.as_deref(), Some("tc1"));
        assert!(!format!("{minted:?}").contains("secret-token-value"));

        let mut no_tunnel = full.clone();
        no_tunnel["machine"]
            .as_object_mut()
            .unwrap()
            .remove("tcAddress");
        let error = parse(no_tunnel.to_string().as_bytes(), None).unwrap_err();
        assert!(!error.to_string().contains("secret-token-value"));
        let direct = parse(no_tunnel.to_string().as_bytes(), Some("127.0.0.1".into())).unwrap();
        assert_eq!(direct.hosts, ["127.0.0.1"]);

        let mut no_token = full;
        no_token.as_object_mut().unwrap().remove("token");
        assert!(parse(no_token.to_string().as_bytes(), None).is_err());
        assert!(parse(b"not json secret-token-value", None)
            .unwrap_err()
            .to_string()
            .find("secret-token-value")
            .is_none());
    }
}
