//! `forward.*` port-forwarding fast-path (§5.14, §12.4).
//!
//! When a script/terminal URL-detection hook (§5.8) finds a dev-server port on a
//! REMOTE daemon, the client requests a forwarded tunnel so the service is
//! reachable locally. Like `host.status`/`events.`/`system.*`, these methods are
//! a transport concern and are intercepted before the JSON-RPC dispatcher.
//!
//! On a remote connection `forward.create` binds a loopback TCP listener that
//! splices each accepted connection to `127.0.0.1:remotePort` on the daemon
//! host; on a local (UDS) connection forwarding is unnecessary (§5.14) so a
//! metadata-only entry is recorded (`localPort = remotePort`, no listener). The
//! registry is per-connection: dropping it on disconnect aborts every tunnel.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::{json, Value};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

use crate::events::{error_frame, success_frame};

/// Global, monotonic forward id counter (`fwd-<n>`).
static FORWARD_COUNTER: AtomicU64 = AtomicU64::new(0);

fn next_forward_id() -> String {
    let n = FORWARD_COUNTER.fetch_add(1, Ordering::Relaxed) + 1;
    format!("fwd-{n}")
}

/// One active forward: ports + optional detected URL, plus the accept-loop task
/// (aborted on `Drop`). A local metadata-only entry has `task = None`.
struct Forward {
    local_port: u16,
    remote_port: u16,
    url: Option<String>,
    task: Option<JoinHandle<()>>,
}

impl Drop for Forward {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

/// Per-connection registry of active port forwards (§5.14). Dropping it aborts
/// every tunnel — disconnect cleanup mirrors the subscription registry.
#[derive(Default)]
pub(crate) struct ForwardRegistry {
    forwards: HashMap<String, Forward>,
}

impl ForwardRegistry {
    /// Open a forward. Remote: bind `127.0.0.1:local_port` (ephemeral when
    /// `None`) and splice accepted connections to `127.0.0.1:remote_port` on the
    /// daemon host. Local: record a metadata-only entry with no listener.
    pub(crate) async fn create(
        &mut self,
        remote_port: u16,
        local_port: Option<u16>,
        url: Option<String>,
        is_local: bool,
    ) -> Result<(String, u16, u16), String> {
        let id = next_forward_id();
        if is_local {
            self.forwards.insert(
                id.clone(),
                Forward {
                    local_port: remote_port,
                    remote_port,
                    url,
                    task: None,
                },
            );
            return Ok((id, remote_port, remote_port));
        }
        let listener = TcpListener::bind(("127.0.0.1", local_port.unwrap_or(0)))
            .await
            .map_err(|e| format!("bind local forward port: {e}"))?;
        let bound = listener
            .local_addr()
            .map_err(|e| format!("resolve local forward port: {e}"))?
            .port();
        let task = tokio::spawn(accept_loop(listener, remote_port));
        self.forwards.insert(
            id.clone(),
            Forward {
                local_port: bound,
                remote_port,
                url,
                task: Some(task),
            },
        );
        Ok((id, bound, remote_port))
    }

    /// Render the active forwards as the `forward.list` result.
    pub(crate) fn list_json(&self) -> Value {
        let forwards: Vec<Value> = self
            .forwards
            .iter()
            .map(|(id, f)| {
                let mut entry = json!({
                    "forwardId": id,
                    "localPort": f.local_port,
                    "remotePort": f.remote_port,
                });
                if let Some(url) = &f.url {
                    entry["url"] = json!(url);
                }
                entry
            })
            .collect();
        json!({ "forwards": forwards })
    }

    /// Close a forward by id; `true` if it existed (its `Drop` aborts the tunnel).
    pub(crate) fn close(&mut self, id: &str) -> bool {
        self.forwards.remove(id).is_some()
    }
}

/// Accept connections on the loopback listener and splice each to the remote
/// port on the daemon host until the listener is dropped/aborted.
async fn accept_loop(listener: TcpListener, remote_port: u16) {
    loop {
        match listener.accept().await {
            Ok((mut inbound, _)) => {
                tokio::spawn(async move {
                    match TcpStream::connect(("127.0.0.1", remote_port)).await {
                        Ok(mut outbound) => {
                            let _ =
                                tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                        }
                        Err(e) => {
                            tracing::debug!(error = %e, port = remote_port, "forward dial failed");
                        }
                    }
                });
            }
            Err(e) => {
                tracing::debug!(error = %e, "forward accept failed");
                break;
            }
        }
    }
}

/// The three `forward.*` methods, once classified, with their parsed params.
pub(crate) enum ForwardMethod {
    Create {
        remote_port: Option<u64>,
        local_port: Option<u64>,
        url: Option<String>,
    },
    List,
    Close {
        forward_id: Option<String>,
    },
}

/// A classified `forward.*` request awaiting handling by the connection task.
pub(crate) struct ForwardRequest {
    pub method: ForwardMethod,
    pub id_present: bool,
    pub id_echo: Value,
}

/// Classify a parsed frame as a `forward.*` request, or `None` to fall through.
/// Mirrors the `control`/`host` fast-path pre-check: a JSON-RPC 2.0 object with
/// a string `method` and an `id` (if present) that is a string, number, or null.
pub(crate) fn classify(value: &Value) -> Option<ForwardRequest> {
    let obj = value.as_object()?;
    if obj.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return None;
    }
    let method_name = obj.get("method").and_then(Value::as_str)?;
    let id_member = obj.get("id");
    if let Some(v) = id_member {
        if !v.is_null() && !v.is_string() && !v.is_number() {
            return None;
        }
    }
    let params = obj.get("params").and_then(Value::as_object);
    let opt_u64 = |name: &str| params.and_then(|p| p.get(name)).and_then(Value::as_u64);
    let opt_str = |name: &str| {
        params
            .and_then(|p| p.get(name))
            .and_then(Value::as_str)
            .map(str::to_string)
    };
    let method = match method_name {
        "forward.create" => ForwardMethod::Create {
            remote_port: opt_u64("remotePort"),
            local_port: opt_u64("localPort"),
            url: opt_str("url"),
        },
        "forward.list" => ForwardMethod::List,
        "forward.close" => ForwardMethod::Close {
            forward_id: opt_str("forwardId"),
        },
        _ => return None,
    };
    Some(ForwardRequest {
        method,
        id_present: id_member.is_some(),
        id_echo: id_member.cloned().unwrap_or(Value::Null),
    })
}

/// Coerce a JSON port number to a valid `u16` TCP port (1..=65535).
fn as_port(value: u64) -> Option<u16> {
    if (1..=u64::from(u16::MAX)).contains(&value) {
        Some(value as u16)
    } else {
        None
    }
}

/// Handle a classified `forward.*` request against the connection's registry,
/// returning the response frame (or `None` for a notification). Invalid params
/// surface as `-32602`; a failed listener bind as `-32603` (PROTOCOL §9).
pub(crate) async fn handle(
    req: ForwardRequest,
    registry: &mut ForwardRegistry,
    is_local: bool,
) -> Option<String> {
    let result: Result<Value, (i32, String)> = match req.method {
        ForwardMethod::Create {
            remote_port,
            local_port,
            url,
        } => match remote_port.and_then(as_port) {
            None => Err((-32602, "Missing required parameter: remotePort".to_string())),
            Some(remote) => {
                let local = match local_port {
                    Some(p) => match as_port(p) {
                        Some(p) => Some(p),
                        None => {
                            return frame(
                                req.id_present,
                                req.id_echo,
                                Err((-32602, "localPort must be a valid TCP port".to_string())),
                            )
                        }
                    },
                    None => None,
                };
                match registry.create(remote, local, url, is_local).await {
                    Ok((id, local_port, remote_port)) => Ok(json!({
                        "forwardId": id,
                        "localPort": local_port,
                        "remotePort": remote_port,
                    })),
                    Err(e) => Err((-32603, e)),
                }
            }
        },
        ForwardMethod::List => Ok(registry.list_json()),
        ForwardMethod::Close { forward_id } => match forward_id {
            Some(id) if !id.is_empty() => {
                registry.close(&id);
                Ok(json!({ "ok": true }))
            }
            _ => Err((-32602, "Missing required parameter: forwardId".to_string())),
        },
    };
    frame(req.id_present, req.id_echo, result)
}

/// Build the response frame for a `forward.*` result, or `None` for a
/// notification (no `id`).
fn frame(id_present: bool, id_echo: Value, result: Result<Value, (i32, String)>) -> Option<String> {
    if !id_present {
        return None;
    }
    Some(match result {
        Ok(value) => success_frame(id_echo, value),
        Err((code, message)) => error_frame(id_echo, code, &message),
    })
}

#[cfg(test)]
mod tests;
