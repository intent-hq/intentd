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
use intent_nodes::cloud::{CloudInstance, CloudProvider, ProcessSpec, SecretString};

/// Test seam: dial the sandbox daemon at this host (its WSS port) instead of
/// its tailcat address, for a fake E2B that runs the sandbox locally.
const DIRECT_HOST_ENV: &str = "INTENTD_E2B_DIRECT_HOST";

/// Longest wait for the sandbox daemon (and its tunnel) before the mint. A
/// sandbox daemon has its listener and tunnel address within seconds.
const DAEMON_WAIT_SECS: u32 = 120;

/// Test seam: a shorter daemon wait, in seconds.
const DAEMON_WAIT_ENV: &str = "INTENTD_E2B_DAEMON_WAIT_SECS";

/// Longest bootstrap error kept, in characters.
const MAX_TAIL_CHARS: usize = 600;

/// Write the daemon's first settings (external listener and tunnel on)
/// unless the sandbox has some, then start it in the background unless it
/// already answers, printing [`STARTED`] when it did and waiting (up to
/// 50 s) until it answers. The template never
/// runs intentd, so this first start makes the sandbox's own daemon id,
/// certificate and tailcat key; on a machine that keeps only its disk, a
/// later start finds them on the disk.
///
/// Every `intentd` here and in [`MINT_SCRIPT`] runs with the same explicit
/// data dir: without it the daemon uses its platform default
/// (`~/.local/share/intentd`) and never reads the settings written here, so
/// it starts with no listener and no tunnel.
const START_SCRIPT: &str = r#"set -eu
export INTENTD_DATA_DIR="${INTENTD_DATA_DIR:-$HOME/.intent}"
dir="$INTENTD_DATA_DIR"
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
  echo started
  # A dial right after must find it listening; a start that runs meanwhile
  # must find it answering, not start a second one.
  i=0
  until intentd call system.status >/dev/null 2>&1; do
    i=$((i + 1))
    [ "$i" -ge 50 ] && break
    sleep 1
  done
fi
"#;

/// What [`START_SCRIPT`] prints when it started intentd.
const STARTED: &str = "started";

/// Wait for the daemon (and its tunnel address unless a direct route is
/// used), then mint. A mint that fails (listener not up yet) is retried; a
/// failed mint saves no credential. When the wait runs out, stderr says
/// what was missing, whether tailcat runs, and the daemon's last warnings
/// and errors (it logs no tokens; the backend scrubs and caps them again).
const MINT_SCRIPT: &str = r#"set -u
export INTENTD_DATA_DIR="${INTENTD_DATA_DIR:-$HOME/.intent}"
i=0
status=""
mint_error=""
errf=$(mktemp)
while :; do
  if status=$(intentd call system.status 2>/dev/null); then
    if [ -n "${INTENT_DIRECT_ROUTE:-}" ] || printf '%s' "$status" | grep -q '"tcAddress"'; then
      if out=$(intentd call executor.mint --params "$INTENT_MINT_PARAMS" 2>"$errf"); then
        printf '%s\n' "$out"
        exit 0
      fi
      mint_error=$(grep -o '"message": *"[^"]*"' "$errf" | head -n 1)
    fi
  else
    status=""
  fi
  i=$((i + 1))
  if [ "$i" -ge "$INTENT_WAIT_SECS" ]; then
    if [ -z "$status" ]; then
      echo "intentd did not answer on its local socket within ${INTENT_WAIT_SECS}s." >&2
    elif printf '%s' "$status" | grep -q '"port": *null'; then
      echo "intentd started with no external listener (its settings did not turn it on), so it has no tunnel." >&2
    elif ! printf '%s' "$status" | grep -q '"tcAddress"'; then
      echo "intentd has no tunnel address within ${INTENT_WAIT_SECS}s." >&2
    else
      echo "the credential mint failed: ${mint_error:-no reason given}." >&2
    fi
    if pgrep -x tailcat >/dev/null 2>&1; then echo "tailcat is running." >&2; else echo "tailcat is not running." >&2; fi
    if [ -f "$INTENTD_DATA_DIR/serve.log" ]; then
      grep -E ' (WARN|ERROR) ' "$INTENTD_DATA_DIR/serve.log" | tail -n 4 >&2
    fi
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

/// Process stderr made safe for an error message: no control characters,
/// every token-like run (32 or more letters, digits, `-` or `_`) replaced,
/// and at most [`MAX_TAIL_CHARS`] characters, the end kept. Only a
/// successful mint prints a credential, on stdout; this is a second guard
/// for daemon log lines.
fn tail(stderr: &[u8]) -> String {
    let text = String::from_utf8_lossy(stderr);
    let mut cleaned = String::with_capacity(text.len());
    let mut run = String::new();
    let flush = |run: &mut String, cleaned: &mut String| {
        if run.chars().count() >= 32 {
            cleaned.push_str("<redacted>");
        } else {
            cleaned.push_str(run);
        }
        run.clear();
    };
    for c in text.trim().chars() {
        if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
            run.push(c);
            continue;
        }
        flush(&mut run, &mut cleaned);
        cleaned.push(if c.is_control() { ' ' } else { c });
    }
    flush(&mut run, &mut cleaned);
    let start = cleaned.chars().count().saturating_sub(MAX_TAIL_CHARS);
    cleaned.chars().skip(start).collect()
}

/// Start intentd in `sandbox` and mint its executor credential for this
/// backend.
///
/// # Errors
///
/// `cloud-machine-unavailable` `bootstrap-failed` when a step fails, or the
/// E2B error of a failed call.
pub(crate) async fn run(
    client: &dyn CloudProvider,
    sandbox: &CloudInstance,
    backend: &Value,
) -> Result<Minted> {
    start_daemon(client, sandbox).await?;

    let direct = std::env::var(DIRECT_HOST_ENV)
        .ok()
        .filter(|host| !host.is_empty());
    let mut mint = ProcessSpec::new("/bin/sh");
    mint.args = vec!["-c".into(), MINT_SCRIPT.into()];
    let wait = std::env::var(DAEMON_WAIT_ENV)
        .ok()
        .and_then(|secs| secs.parse::<u32>().ok())
        .filter(|secs| *secs > 0)
        .unwrap_or(DAEMON_WAIT_SECS);
    mint.timeout = Some(Duration::from_secs(u64::from(wait) + 60));
    let mut envs = BTreeMap::new();
    envs.insert(
        "INTENT_MINT_PARAMS".to_owned(),
        SecretString::new(backend.to_string()),
    );
    envs.insert(
        "INTENT_WAIT_SECS".to_owned(),
        SecretString::new(wait.to_string()),
    );
    if direct.is_some() {
        envs.insert("INTENT_DIRECT_ROUTE".to_owned(), SecretString::new("1"));
    }
    mint.envs = envs;
    let minted = client.run_command(sandbox, &mint).await.map_err(|error| {
        super::config::to_error(&error, super::config::Phase::Other, client.kind())
    })?;
    if !minted.success() {
        return Err(failed(format!(
            "the sandbox's intentd did not mint a credential (exit {}): {}",
            minted.exit.exit_code,
            tail(&minted.stderr)
        )));
    }
    parse(&minted.stdout, direct)
}

/// Start intentd in `sandbox` unless it already answers. Returns whether
/// it started it.
///
/// # Errors
///
/// `cloud-machine-unavailable` `bootstrap-failed` when the start fails, or
/// the provider error of a failed call.
pub(crate) async fn start_daemon(
    client: &dyn CloudProvider,
    sandbox: &CloudInstance,
) -> Result<bool> {
    let mut start = ProcessSpec::new("/bin/sh");
    start.args = vec!["-c".into(), START_SCRIPT.into()];
    start.timeout = Some(Duration::from_secs(60));
    let started = client.run_command(sandbox, &start).await.map_err(|error| {
        super::config::to_error(&error, super::config::Phase::Other, client.kind())
    })?;
    if !started.success() {
        return Err(failed(format!(
            "intentd could not start in the sandbox (exit {}): {}",
            started.exit.exit_code,
            tail(&started.stderr)
        )));
    }
    Ok(String::from_utf8_lossy(&started.stdout)
        .lines()
        .any(|line| line.trim() == STARTED))
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
pub(crate) fn mint_params(
    backend_id: &str,
    backend_name: &str,
    fingerprint: Option<&str>,
) -> Value {
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
    fn bootstrap_errors_keep_the_end_without_tokens_or_control_characters() {
        let token = "Q".repeat(40);
        let stderr = format!(
            "{}intentd started with no external listener.\n\u{1b}[31mWARN token={token} tunnel off",
            "x ".repeat(400)
        );
        let kept = tail(stderr.as_bytes());
        assert!(kept.chars().count() <= MAX_TAIL_CHARS);
        assert!(kept.ends_with("tunnel off"), "{kept}");
        assert!(kept.contains("token=<redacted>"), "{kept}");
        assert!(!kept.contains(&token));
        assert!(!kept.chars().any(char::is_control), "{kept}");
        assert_eq!(tail(b"short-id is kept"), "short-id is kept");
    }

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
