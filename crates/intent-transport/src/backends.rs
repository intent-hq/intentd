//! `backends.report` fast path (Assistant remote backends): an owner desktop
//! app reports the backends it holds credentials for, keyed by the
//! connection's hello'd `clientId`. Like `drafts.*`, it consumes connection
//! state (the hello identity), so it is intercepted before the router.

use intent_core::WorkspaceApi;
use serde_json::{json, Value};

use crate::events::{error_frame, error_frame_with_data, success_frame};
use crate::reverse::ReverseClientIdentity;

/// A classified `backends.report` request.
pub(crate) struct BackendsReport {
    pub params: Value,
    pub id_present: bool,
    pub id_echo: Value,
}

/// Classify a parsed frame as `backends.report`, or `None` to fall through.
pub(crate) fn classify(value: &Value) -> Option<BackendsReport> {
    let obj = value.as_object()?;
    if obj.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
        || obj.get("method").and_then(Value::as_str) != Some("backends.report")
    {
        return None;
    }
    let id_member = obj.get("id");
    if let Some(v) = id_member {
        if !v.is_null() && !v.is_string() && !v.is_number() {
            return None;
        }
    }
    Some(BackendsReport {
        params: obj.get("params").cloned().unwrap_or(Value::Null),
        id_present: id_member.is_some(),
        id_echo: id_member.cloned().unwrap_or(Value::Null),
    })
}

/// Store the report under the connection's hello identity. Owner only; a
/// connection that never said hello is refused.
pub(crate) async fn handle(
    req: BackendsReport,
    api: &dyn WorkspaceApi,
    identity: Option<ReverseClientIdentity>,
) -> Option<String> {
    let BackendsReport {
        params,
        id_present,
        id_echo,
    } = req;
    let frame = if crate::context::is_non_administrator_caller() {
        error_frame(
            &id_echo,
            crate::catalog::FORBIDDEN_ERROR_CODE,
            crate::catalog::FORBIDDEN_ERROR_MESSAGE,
        )
    } else if let Some(identity) = identity {
        match api
            .backends_report(identity.client_id.clone(), identity.device_name(), params)
            .await
        {
            Ok(result) => success_frame(&id_echo, &result),
            Err(error) => crate::router::delivery_error(&id_echo, error),
        }
    } else {
        error_frame_with_data(
            &id_echo,
            -32602,
            "backends.report needs a prior client.hello",
            &json!({ "code": "hello-required" }),
        )
    };
    id_present.then_some(frame)
}
