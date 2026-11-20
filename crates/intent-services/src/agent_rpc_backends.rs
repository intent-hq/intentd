//! Remote backends for the Assistant's `ws.rpc` (D3): owner desktop apps
//! report the backends they hold credentials for (`backends.report`), and
//! the daemon relays calls to those backends through an online reporter.
//! In memory, keyed by the reporter's `clientId`, with no expiry: a report
//! survives its app disconnecting and is replaced by that app's next one.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use intent_core::agent_rpc::{
    carries_credential, AgentRpcClass, AgentRpcError, AgentRpcSurface, ReportedBackend,
    AGENT_RPC_INTERNAL_CODE, LOCAL_BACKEND_ID,
};
use intent_core::{ClientId, Error, Result};
use serde_json::{json, Value};

use crate::Services;

/// Most backends one app may report.
const MAX_REPORTED_BACKENDS: usize = 200;
/// Most reporting apps remembered (oldest report dropped past it).
const MAX_REPORTERS: usize = 64;
/// Bound on the remote `agent.rpcMethods` read used to classify a method
/// this daemon does not know.
const REMOTE_CLASS_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
struct Report {
    device: String,
    entries: Vec<ReportedBackend>,
    reported_at: String,
    seq: u64,
}

#[derive(Debug, Default)]
struct State {
    seq: u64,
    reports: HashMap<String, Report>,
}

/// Every app's latest report.
#[derive(Debug, Default)]
pub(crate) struct BackendReports {
    state: Mutex<State>,
}

/// One app that reported a backend.
#[derive(Debug, Clone)]
pub(crate) struct Reporter {
    pub client_id: String,
    pub device: String,
    pub reported_at: String,
}

/// One merged remote backend.
#[derive(Debug, Clone)]
pub(crate) struct KnownBackend {
    pub id: String,
    pub name: String,
    pub daemon_version: Option<String>,
    /// Newest report first.
    pub reporters: Vec<Reporter>,
}

/// Where a `ws.rpc` call goes.
#[derive(Debug, Clone)]
pub(crate) enum Target {
    Local,
    Remote(KnownBackend),
}

impl Target {
    pub(crate) fn id(&self) -> &str {
        match self {
            Self::Local => LOCAL_BACKEND_ID,
            Self::Remote(b) => &b.id,
        }
    }
}

fn normalized_fingerprint(fp: &str) -> String {
    format!("fp:{}", fp.trim().to_uppercase())
}

impl BackendReports {
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn replace(&self, client_id: &str, device: String, entries: Vec<ReportedBackend>) {
        let mut state = self.lock();
        state.seq += 1;
        let seq = state.seq;
        state.reports.insert(
            client_id.to_string(),
            Report {
                device,
                entries,
                reported_at: intent_core::now_iso(),
                seq,
            },
        );
        while state.reports.len() > MAX_REPORTERS {
            let Some(oldest) = state
                .reports
                .iter()
                .min_by_key(|(_, r)| r.seq)
                .map(|(id, _)| id.clone())
            else {
                break;
            };
            state.reports.remove(&oldest);
        }
    }

    /// Remote backends merged across reports by identity, sorted by name.
    /// The entry carrying `local_fingerprint` is the local backend and is
    /// left out.
    fn merged(&self, local_fingerprint: Option<&str>) -> Vec<KnownBackend> {
        let local_id = local_fingerprint
            .map(str::trim)
            .filter(|f| !f.is_empty())
            .map(normalized_fingerprint);
        let state = self.lock();
        let mut reports: Vec<(&String, &Report)> = state.reports.iter().collect();
        reports.sort_by_key(|(_, r)| std::cmp::Reverse(r.seq));
        let mut merged: Vec<KnownBackend> = Vec::new();
        for (client_id, report) in reports {
            for entry in &report.entries {
                let id = entry.identity();
                if local_id.as_deref() == Some(id.as_str()) {
                    continue;
                }
                let reporter = Reporter {
                    client_id: client_id.clone(),
                    device: report.device.clone(),
                    reported_at: report.reported_at.clone(),
                };
                match merged.iter_mut().find(|b| b.id == id) {
                    Some(known) => {
                        if !known.reporters.iter().any(|r| r.client_id == *client_id) {
                            known.reporters.push(reporter);
                        }
                    }
                    None => merged.push(KnownBackend {
                        id,
                        name: if entry.name.trim().is_empty() {
                            entry.host.clone()
                        } else {
                            entry.name.clone()
                        },
                        daemon_version: entry.daemon_version.clone(),
                        reporters: vec![reporter],
                    }),
                }
            }
        }
        merged.sort_by_key(|b| b.name.to_lowercase());
        merged
    }
}

fn relay_failure(code: &str, message: impl Into<String>) -> AgentRpcError {
    AgentRpcError::coded(AGENT_RPC_INTERNAL_CODE, code, message)
}

/// Whether `error` means a relayed call may have run without a reply.
pub(crate) fn relay_outcome_unknown(error: &AgentRpcError) -> bool {
    matches!(
        error.data_code(),
        Some("relay-disconnected" | "relay-timeout" | "relay-outcome-unknown")
    )
}

impl Services {
    /// `backends.report` (transport fast path, owner connections only).
    pub(crate) fn backends_report_op(
        &self,
        client_id: &ClientId,
        device: String,
        params: &Value,
    ) -> Result<Value> {
        if carries_credential(params) {
            return Err(Error::InvalidParams(
                "backends.report must not carry tokens or other credentials".into(),
            ));
        }
        let raw = params
            .get("backends")
            .and_then(Value::as_array)
            .ok_or_else(|| Error::InvalidParams("backends must be an array".into()))?;
        if raw.len() > MAX_REPORTED_BACKENDS {
            return Err(Error::InvalidParams(format!(
                "a report holds at most {MAX_REPORTED_BACKENDS} backends"
            )));
        }
        let entries = raw
            .iter()
            .enumerate()
            .map(|(i, entry)| {
                serde_json::from_value::<ReportedBackend>(entry.clone())
                    .map_err(|e| Error::InvalidParams(format!("backends[{i}]: {e}")))
            })
            .collect::<Result<Vec<_>>>()?;
        let accepted = entries.len();
        self.backend_reports
            .replace(client_id.as_str(), device, entries);
        Ok(json!({ "accepted": accepted }))
    }

    pub(crate) fn known_backends(&self, surface: &dyn AgentRpcSurface) -> Vec<KnownBackend> {
        self.backend_reports
            .merged(surface.local_fingerprint().as_deref())
    }

    /// `ws.rpc.backends()`: the local backend, then every remote one with its
    /// reporters and whether each is online now. No client round trip.
    pub(crate) fn backends_view(&self, surface: &dyn AgentRpcSurface) -> Value {
        let mut rows = vec![json!({
            "id": LOCAL_BACKEND_ID,
            "name": surface.local_backend_name(),
            "local": true,
            "daemonVersion": surface.daemon_version(),
        })];
        for backend in self.known_backends(surface) {
            let reporters: Vec<Value> = backend
                .reporters
                .iter()
                .map(|r| {
                    json!({
                        "clientId": r.client_id,
                        "device": r.device,
                        "online": surface.relay_online(&r.client_id),
                        "lastSeenAt": r.reported_at,
                    })
                })
                .collect();
            let reachable = reporters.iter().any(|r| r["online"] == true);
            let mut row = json!({
                "id": backend.id,
                "name": backend.name,
                "local": false,
                "reachable": reachable,
                "reporters": reporters,
            });
            if let Some(version) = backend.daemon_version {
                row["daemonVersion"] = json!(version);
            }
            rows.push(row);
        }
        json!({ "backends": rows })
    }

    /// Resolve a backend id from `ws.rpc` (`None`, `"local"` or a
    /// `backends()` id).
    pub(crate) fn resolve_backend(
        &self,
        surface: &dyn AgentRpcSurface,
        backend: Option<&str>,
    ) -> std::result::Result<Target, AgentRpcError> {
        let Some(id) = backend.map(str::trim).filter(|b| !b.is_empty()) else {
            return Ok(Target::Local);
        };
        if id == LOCAL_BACKEND_ID {
            return Ok(Target::Local);
        }
        if surface
            .local_fingerprint()
            .is_some_and(|fp| normalized_fingerprint(&fp) == id)
        {
            return Ok(Target::Local);
        }
        self.known_backends(surface)
            .into_iter()
            .find(|b| b.id == id)
            .map(Target::Remote)
            .ok_or_else(|| {
                AgentRpcError::coded(
                    -32602,
                    "unknown-backend",
                    format!(
                        "unknown backend `{id}`: list the reachable ones with ws.rpc.backends()"
                    ),
                )
            })
    }

    /// The online reporter to relay through (newest report first), or the
    /// error that names the machines to open.
    pub(crate) fn pick_relay(
        surface: &dyn AgentRpcSurface,
        backend: &KnownBackend,
    ) -> std::result::Result<String, AgentRpcError> {
        if let Some(reporter) = backend
            .reporters
            .iter()
            .find(|r| surface.relay_online(&r.client_id))
        {
            return Ok(reporter.client_id.clone());
        }
        let mut devices: Vec<&str> = backend
            .reporters
            .iter()
            .map(|r| r.device.as_str())
            .collect();
        devices.dedup();
        Err(relay_failure(
            "no-relay-online",
            format!(
                "Open Intent on {} to reach '{}'",
                devices.join(" or "),
                backend.name
            ),
        ))
    }

    /// Relay one call to a remote backend within `timeout`.
    pub(crate) async fn relay(
        surface: &dyn AgentRpcSurface,
        backend: &KnownBackend,
        method: &str,
        params: Value,
        timeout: Duration,
    ) -> std::result::Result<Value, AgentRpcError> {
        let client_id = Self::pick_relay(surface, backend)?;
        surface
            .relay_call(
                client_id,
                backend.id.clone(),
                method.to_string(),
                params,
                timeout,
            )
            .await
    }

    /// D6: the local class when this daemon knows `method`; otherwise the
    /// remote daemon's `agent.rpcMethods` class; otherwise mutating. Each
    /// remote's classes are fetched at most once per request (`cache`, keyed
    /// by backend id; `None` records a failed fetch), within `deadline`.
    pub(crate) async fn class_for(
        surface: &dyn AgentRpcSurface,
        target: &Target,
        method: &str,
        deadline: tokio::time::Instant,
        cache: &mut HashMap<String, Option<Value>>,
    ) -> AgentRpcClass {
        let Target::Remote(backend) = target else {
            return surface.classify(method);
        };
        if surface.summary(method).is_some() {
            return surface.classify(method);
        }
        if !cache.contains_key(&backend.id) {
            let wait = deadline
                .saturating_duration_since(tokio::time::Instant::now())
                .min(REMOTE_CLASS_TIMEOUT);
            let reply = if wait.is_zero() {
                None
            } else {
                Self::relay(surface, backend, "agent.rpcMethods", json!({}), wait)
                    .await
                    .ok()
            };
            cache.insert(backend.id.clone(), reply);
        }
        let class = cache
            .get(&backend.id)
            .and_then(Option::as_ref)
            .and_then(|reply| reply.get("methods"))
            .and_then(Value::as_array)
            .and_then(|rows| rows.iter().find(|r| r["method"] == method))
            .and_then(|r| r["class"].as_str());
        match class {
            Some("read") => AgentRpcClass::Read,
            Some("forbidden-to-agent") => AgentRpcClass::ForbiddenToAgent,
            Some("unsupported-on-agent-path") => AgentRpcClass::UnsupportedOnAgentPath,
            _ => AgentRpcClass::Mutating,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(name: &str, fp: Option<&str>, host: &str) -> ReportedBackend {
        ReportedBackend {
            name: name.into(),
            host: host.into(),
            hosts: vec![],
            port: 8843,
            fingerprint: fp.map(str::to_string),
            tc_address: None,
            connected: None,
            daemon_version: None,
        }
    }

    #[test]
    fn reports_merge_by_identity_and_drop_the_local_backend() {
        let reports = BackendReports::default();
        reports.replace(
            "mac-a",
            "MacBook".into(),
            vec![
                entry("Studio", Some("aa:bb"), "studio.local"),
                entry("Self", Some("ff:ff"), "self.local"),
            ],
        );
        reports.replace(
            "mac-b",
            "Mac Studio".into(),
            vec![
                entry("Studio again", Some(" AA:BB "), "10.0.0.2"),
                entry("Home server", None, "Home.Example"),
            ],
        );
        let merged = reports.merged(Some("FF:FF"));
        let ids: Vec<&str> = merged.iter().map(|b| b.id.as_str()).collect();
        assert_eq!(ids, ["addr:home.example:8843", "fp:AA:BB"]);
        let studio = &merged[1];
        let reporters: Vec<&str> = studio
            .reporters
            .iter()
            .map(|r| r.client_id.as_str())
            .collect();
        assert_eq!(reporters, ["mac-b", "mac-a"], "newest report first");
    }

    #[test]
    fn a_new_report_replaces_the_previous_one() {
        let reports = BackendReports::default();
        reports.replace(
            "mac-a",
            "MacBook".into(),
            vec![entry("Old", Some("01"), "o")],
        );
        reports.replace(
            "mac-a",
            "MacBook".into(),
            vec![entry("New", Some("02"), "n")],
        );
        let ids: Vec<String> = reports.merged(None).into_iter().map(|b| b.id).collect();
        assert_eq!(ids, ["fp:02"]);
    }

    #[test]
    fn a_relay_that_lost_its_reply_is_an_unknown_outcome() {
        let coded = |code: &str| AgentRpcError::coded(-32603, code, "x");
        for code in [
            "relay-disconnected",
            "relay-timeout",
            "relay-outcome-unknown",
        ] {
            assert!(relay_outcome_unknown(&coded(code)), "{code}");
        }
        for code in ["relay-unreachable", "no-relay-online", "unknown-backend"] {
            assert!(!relay_outcome_unknown(&coded(code)), "{code}");
        }
    }
}
