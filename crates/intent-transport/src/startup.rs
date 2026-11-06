//! Restricted startup dispatch. No application handle exists until handoff.
use intent_core::{
    startup::{StartupPhase, StartupStatus},
    WorkspaceApi,
};
use intent_services::EventBus;
use serde_json::{json, Value};
use std::sync::{Arc, OnceLock};

/// Complete application dispatch installed exactly once after initialization.
#[derive(Clone)]
pub struct ReadyServices {
    pub api: Arc<dyn WorkspaceApi>,
    pub bus: EventBus,
    pub rpc_limiter: crate::RpcLimiter,
    pub reverse_registry: Arc<crate::PrimaryReverseRegistry>,
    pub server_pairing_info: Option<Arc<dyn crate::ServerPairingInfo>>,
    pub control: Option<Arc<dyn crate::SystemControl>>,
}

struct Inner {
    status: StartupStatus,
    services: OnceLock<ReadyServices>,
    changed: tokio::sync::watch::Sender<bool>,
    policy_check: OnceLock<Arc<dyn Fn() -> bool + Send + Sync>>,
}

/// Shared handoff for all bound endpoints. Bootstrap seats never acquire services.
#[derive(Clone)]
pub struct StartupDispatch(Arc<Inner>);

impl StartupDispatch {
    #[must_use]
    pub fn new(status: StartupStatus) -> Self {
        let (changed, _) = tokio::sync::watch::channel(false);
        Self(Arc::new(Inner {
            status,
            services: OnceLock::new(),
            changed,
            policy_check: OnceLock::new(),
        }))
    }

    /// Install a strict, file-backed admission recheck before exposing listeners.
    /// It runs off the async runtime and never consults application services.
    #[must_use]
    pub fn set_policy_check(&self, check: Arc<dyn Fn() -> bool + Send + Sync>) -> bool {
        self.0.policy_check.set(check).is_ok()
    }

    pub(crate) async fn admit_bootstrap(&self) -> bool {
        let Some(check) = self.0.policy_check.get().cloned() else {
            return true;
        };
        let allowed = tokio::task::spawn_blocking(move || check())
            .await
            .unwrap_or(false);
        if !allowed {
            self.0.status.fail();
        }
        allowed
    }

    pub(crate) fn normal(services: ReadyServices) -> Self {
        let dispatch = Self::new(StartupStatus::new());
        let _ = dispatch.0.services.set(services);
        assert!(dispatch.0.status.ready());
        dispatch
    }

    /// Publish full admission and retire every bootstrap seat. Fails if this
    /// attempt failed already or another caller installed the services.
    #[must_use]
    pub fn install(&self, services: ReadyServices) -> bool {
        if self.0.status.phase() == StartupPhase::Failed {
            return false;
        }
        if self.0.services.set(services).is_err() {
            return false;
        }
        let Some(ready) = self.0.services.get() else {
            return false;
        };
        ready
            .reverse_registry
            .spawn_client_event_publisher(ready.api.clone());
        if !self.0.status.ready() {
            return false;
        }
        self.0.changed.send_replace(true);
        true
    }

    #[must_use]
    pub fn status(&self) -> &StartupStatus {
        &self.0.status
    }

    pub(crate) fn ready(&self) -> Option<&ReadyServices> {
        (self.0.status.phase() == StartupPhase::Ready)
            .then(|| self.0.services.get())
            .flatten()
    }

    pub(crate) fn services_mut(&mut self) -> &mut ReadyServices {
        Arc::get_mut(&mut self.0)
            .expect("unshared builder")
            .services
            .get_mut()
            .expect("normal builder")
    }

    /// Wait until bootstrap seats must reconnect to ordinary authenticated admission.
    pub async fn retired(&self) {
        let mut changed = self.0.changed.subscribe();
        let _ = changed.wait_for(|v| *v).await;
    }
}

pub(crate) fn response(status: &StartupStatus, raw: &str) -> Option<String> {
    use crate::router::EnvelopeCheck;
    let value: Value = match serde_json::from_str(raw) {
        Ok(value) => value,
        Err(_) => return Some(error(&Value::Null, -32700, "Parse error", None)),
    };
    // No reverse call was issued. Ignore unsolicited response frames.
    if value.get("method").is_none()
        && (value.get("result").is_some() || value.get("error").is_some())
    {
        return None;
    }
    let (id, method) = match crate::router::check_envelope(&value) {
        EnvelopeCheck::Valid {
            is_notification: true,
            ..
        } => return None,
        EnvelopeCheck::Valid {
            echo_id, method, ..
        } => (echo_id, method),
        EnvelopeCheck::BadJsonRpc { echo_id } => {
            return Some(error(
                &echo_id,
                -32600,
                "Invalid Request: jsonrpc must be \"2.0\"",
                None,
            ))
        }
        EnvelopeCheck::BadMethod { echo_id } => {
            return Some(error(
                &echo_id,
                -32600,
                "Invalid Request: method must be a non-empty string",
                None,
            ))
        }
        EnvelopeCheck::NotObject => {
            return Some(error(
                &Value::Null,
                -32600,
                "Invalid Request: expected an object",
                None,
            ))
        }
        EnvelopeCheck::BadId => {
            return Some(error(
                &Value::Null,
                -32600,
                "Invalid Request: id must be a string, number, or null",
                None,
            ))
        }
    };
    if method == "system.startupStatus" {
        return Some(status_response(status, &id, value.get("params")));
    }
    let snapshot = status.snapshot();
    let failed = snapshot.phase == StartupPhase::Failed;
    let mut data = json!({"code":if failed { "daemon-startup-failed" } else { "daemon-not-ready" }, "retryable":!failed, "startup":snapshot});
    if !failed {
        data["retryAfterMs"] = json!(snapshot.retry_after_ms.unwrap_or(1000));
    }
    Some(error(
        &id,
        -32012,
        if failed {
            "Daemon startup failed"
        } else {
            "Daemon not ready"
        },
        Some(data),
    ))
}

pub(crate) fn status_response(
    status: &StartupStatus,
    id: &Value,
    params: Option<&Value>,
) -> String {
    if params.is_some_and(|p| !p.as_object().is_some_and(serde_json::Map::is_empty)) {
        return error(id, -32602, "Invalid params", None);
    }
    json!({"jsonrpc":"2.0","id":id,"result":status.snapshot()}).to_string()
}

fn error(id: &Value, code: i32, message: &str, data: Option<Value>) -> String {
    let mut error = json!({"code":code,"message":message});
    if let Some(data) = data {
        error["data"] = data;
    }
    json!({"jsonrpc":"2.0","id":id,"error":error}).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn only_status_is_admitted_and_notifications_never_execute() {
        let status = StartupStatus::new();
        for method in [
            "client.hello",
            "system.status",
            "workspace.list",
            "note.add",
            "events.subscribe",
            "unknown",
        ] {
            let frame = json!({"jsonrpc":"2.0","id":1,"method":method}).to_string();
            let value: Value = serde_json::from_str(&response(&status, &frame).unwrap()).unwrap();
            assert_eq!(value["error"]["code"], -32012);
            assert_eq!(value["error"]["data"]["retryable"], true);
            assert!(response(
                &status,
                &json!({"jsonrpc":"2.0","method":method}).to_string()
            )
            .is_none());
        }
        for params in [Value::Null, json!([]), json!({"extra":true})] {
            let value: Value = serde_json::from_str(&response(&status, &json!({"jsonrpc":"2.0","id":1,"method":"system.startupStatus","params":params}).to_string()).unwrap()).unwrap();
            assert_eq!(value["error"]["code"], -32602);
        }
        status.fail();
        let value: Value = serde_json::from_str(
            &response(
                &status,
                r#"{"jsonrpc":"2.0","id":1,"method":"client.hello"}"#,
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(value["error"]["data"]["retryable"], false);
        assert!(value["error"]["data"].get("retryAfterMs").is_none());
    }
}
