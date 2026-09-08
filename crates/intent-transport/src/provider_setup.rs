//! App-only Antigravity setup. Owned by a local UDS connection, not the
//! workspace API, agent tools, or the shared event bus.

use std::path::PathBuf;
use std::time::Duration;

use intent_core::WorkspaceApi;
use intent_services::antigravity_setup::{self, Operation};
use serde_json::{json, Value};

use crate::events::{error_frame, success_frame};
use crate::reverse::ReverseChannel;

#[derive(Default)]
pub(crate) struct Connection {
    pub(crate) authorized: bool,
    operation: Option<Operation>,
}

pub(crate) struct Request {
    action: Action,
    params: Value,
    id: Option<Value>,
}

#[derive(Clone, Copy)]
enum Action {
    Status,
    Start,
    Login,
    Cancel,
}

pub(crate) fn classify(value: &Value) -> Option<Request> {
    if value.get("jsonrpc")?.as_str()? != "2.0" {
        return None;
    }
    let action = match value.get("method")?.as_str()? {
        "providers.setup.status" => Action::Status,
        "providers.setup.start" => Action::Start,
        "providers.setup.login" => Action::Login,
        "providers.setup.cancel" => Action::Cancel,
        _ => return None,
    };
    let id = value.get("id").cloned();
    if id
        .as_ref()
        .is_some_and(|v| !v.is_null() && !v.is_string() && !v.is_number())
    {
        return None;
    }
    Some(Request {
        action,
        params: value.get("params").cloned().unwrap_or(Value::Null),
        id,
    })
}

impl Connection {
    pub(crate) async fn handle(
        &mut self,
        req: Request,
        api: &dyn WorkspaceApi,
        reverse: &ReverseChannel,
    ) -> Option<String> {
        // Notifications cannot represent deliberate setup actions.
        let id = req.id?;
        if crate::context::is_tcp_connection() || !self.authorized {
            return Some(error_frame(
                &id,
                -32001,
                "Antigravity setup requires an authorized local app connection",
            ));
        }
        if req.params.get("providerId").and_then(Value::as_str) != Some("antigravity") {
            return Some(error_frame(&id, -32602, "providerId must be antigravity"));
        }
        match req.action {
            Action::Status | Action::Start => {
                if self
                    .operation
                    .as_ref()
                    .is_none_or(|op| matches!(req.action, Action::Start) && !op.reusable())
                {
                    let Ok(settings) = api.settings_get("providers.paths".into()).await else {
                        return Some(error_frame(
                            &id,
                            -32603,
                            "Cannot read provider configuration",
                        ));
                    };
                    let explicit = settings
                        .get("value")
                        .and_then(|v| v.get("antigravity"))
                        .and_then(Value::as_str)
                        .map(str::to_owned);
                    if matches!(req.action, Action::Status) {
                        return Some(success_frame(
                            &id,
                            &json!(antigravity_setup::status(explicit.as_deref())),
                        ));
                    }
                    let Some(home) = std::env::var_os("HOME")
                        .map(PathBuf::from)
                        .and_then(|path| path.canonicalize().ok())
                    else {
                        return Some(error_frame(
                            &id,
                            -32603,
                            "Cannot locate provider installation directory",
                        ));
                    };
                    self.operation = Some(Operation::start(home, explicit));
                }
            }
            Action::Login | Action::Cancel => {
                let Some(operation) = self.operation.as_ref().filter(|op| {
                    req.params
                        .get("operationId")
                        .and_then(Value::as_str)
                        .is_some_and(|id| op.status().operation_id.as_deref() == Some(id))
                }) else {
                    return Some(error_frame(
                        &id,
                        -32602,
                        "Unknown setup operation on this connection",
                    ));
                };
                if matches!(req.action, Action::Cancel) {
                    operation.cancel();
                } else {
                    let reverse = reverse.clone();
                    let operation_id = operation.status().operation_id;
                    if !operation.login(move |url| async move {
                        reverse
                            .request(
                                "providers.setup.openLogin",
                                json!({"operationId":operation_id,"url":url}),
                                Duration::from_secs(30),
                            )
                            .await
                            .is_ok_and(|reply| {
                                reply.get("opened").and_then(Value::as_bool) == Some(true)
                            })
                    }) {
                        return Some(error_frame(&id, -32602, "Setup is not waiting for sign-in"));
                    }
                }
            }
        }
        self.operation
            .as_ref()
            .map(|op| success_frame(&id, &json!(op.status())))
    }
}

#[cfg(test)]
mod tests;
