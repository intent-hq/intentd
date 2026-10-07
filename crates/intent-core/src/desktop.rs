//! Desktop control boundary types. Agent identity never comes from arguments.
use crate::{ClientId, PrincipalId};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Required reminder in active results, wakes and API documentation.
pub const RELEASE_HINT: &str = "You are controlling the workspace primary desktop. Call ws.desktop.endControl() as soon as your desktop work is finished.";
/// Pending permission ends the requesting turn without blocking it.
pub const PENDING_HINT: &str = "End this turn and wait for the desktop control permission outcome.";
/// User Stop must never invite automatic acquisition or retries.
pub const STOP_HINT: &str = "Desktop control permission was rescinded by the user. Respect the interruption; do not automatically restart or retry desktop control.";
/// Desktop failures preserve their stable protocol code and execution outcome.
pub type DesktopResult<T> = std::result::Result<T, DesktopError>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DesktopError {
    pub code: String,
    pub detail: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution: Option<String>,
}
impl DesktopError {
    /// Construct a failure before an execution outcome has been established.
    #[must_use]
    pub fn new(code: &str, detail: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            detail: detail.into(),
            execution: None,
        }
    }
    /// JSON-RPC envelope code for the structured desktop failure.
    #[must_use]
    pub fn numeric_code(&self) -> i32 {
        match self.code.as_str() {
            "forbidden" => -32003,
            "invalid-params"
            | "not-found"
            | "desktop-not-active"
            | "desktop-stale-request"
            | "desktop-stale-command"
            | "desktop-stale-layout"
            | "desktop-display-selection-required"
            | "desktop-display-unavailable"
            | "desktop-command-expired" => -32602,
            _ => -32603,
        }
    }
}
impl std::fmt::Display for DesktopError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.detail)?;
        if let Some(execution) = &self.execution {
            write!(f, " (execution: {execution})")?;
        }
        Ok(())
    }
}
impl std::error::Error for DesktopError {}
impl From<crate::Error> for DesktopError {
    fn from(error: crate::Error) -> Self {
        let code = match error {
            crate::Error::NotFound(_) => "not-found",
            crate::Error::Forbidden(_) => "forbidden",
            _ => "desktop-execution-failed",
        };
        Self::new(code, error.to_string())
    }
}

/// Mint a fresh connection incarnation on admission and every hello.
#[must_use]
pub fn new_connection_epoch() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// An opaque incarnation, bound to admission and the latest hello. Never inferred
/// from a logical hello ID. Transport verifies it again before each dispatch.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DesktopConnection {
    pub client_id: ClientId,
    pub principal_id: PrincipalId,
    pub connection_epoch: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum DesktopState {
    #[default]
    Inactive,
    PendingPermission {
        #[serde(rename = "requestId")]
        request_id: String,
        #[serde(rename = "computerName", skip_serializing_if = "Option::is_none")]
        computer_name: Option<String>,
    },
    Active {
        #[serde(rename = "sessionId")]
        session_id: String,
        #[serde(rename = "computerName")]
        computer_name: String,
        hint: String,
    },
}

/// Validate the complete model action before any reverse request is sent.
///
/// # Errors
/// Returns invalid-params for unknown fields, identity overrides or invalid input.
pub fn validate_action(kind: &str, args: &Value) -> DesktopResult<Value> {
    let invalid = || DesktopError::new("invalid-params", "Invalid desktop action arguments");
    let object = args.as_object().ok_or_else(invalid)?;
    let allowed: &[&str] = match kind {
        "startControl" | "endControl" | "listDisplay" => &[],
        "screenshot" => &["displayId", "layoutId"],
        "move" => &["displayId", "layoutId", "x", "y"],
        "click" => &["displayId", "layoutId", "x", "y", "button", "clickCount"],
        "type" => &["text"],
        "keypress" => &["key", "modifiers"],
        "scroll" => &["displayId", "layoutId", "x", "y", "deltaX", "deltaY"],
        "drag" => &["displayId", "layoutId", "from", "to"],
        _ => return Err(invalid()),
    };
    if object.keys().any(|key| !allowed.contains(&key.as_str())) {
        return Err(invalid());
    }
    let nonempty = |key: &str| args[key].as_str().is_some_and(|s| !s.is_empty());
    let coordinate = |v: &Value| v.as_f64().is_some_and(|n| n.is_finite() && n >= 0.0);
    if object.contains_key("displayId") && !nonempty("displayId") {
        return Err(invalid());
    }
    if object.contains_key("layoutId") && !nonempty("layoutId") {
        return Err(invalid());
    }
    if matches!(kind, "move" | "click" | "scroll" | "drag") && !nonempty("layoutId") {
        return Err(invalid());
    }
    if matches!(kind, "move" | "click" | "scroll")
        && (!coordinate(&args["x"]) || !coordinate(&args["y"]))
    {
        return Err(invalid());
    }
    match kind {
        "click" => {
            if object
                .get("button")
                .is_some_and(|v| !matches!(v.as_str(), Some("left" | "right")))
                || object
                    .get("clickCount")
                    .is_some_and(|v| !matches!(v.as_u64(), Some(1 | 2)))
            {
                return Err(invalid());
            }
        }
        "type" => {
            if args["text"].as_str().is_none_or(|s| s.len() > 16_384) {
                return Err(invalid());
            }
        }
        "keypress" => {
            let key = args["key"].as_str().ok_or_else(invalid)?;
            let named = [
                "Enter",
                "Tab",
                "Escape",
                "Backspace",
                "Delete",
                "Insert",
                "Home",
                "End",
                "PageUp",
                "PageDown",
                "ArrowUp",
                "ArrowDown",
                "ArrowLeft",
                "ArrowRight",
                "Space",
            ];
            let function = (1..=24).any(|n| key == format!("F{n}"));
            if !(named.contains(&key)
                || function
                || (key.chars().count() == 1 && key.chars().all(|c| !c.is_control())))
            {
                return Err(invalid());
            }
            if let Some(modifiers) = object.get("modifiers") {
                let modifiers = modifiers.as_array().ok_or_else(invalid)?;
                let mut seen = std::collections::HashSet::new();
                for modifier in modifiers {
                    let modifier = modifier.as_str().ok_or_else(invalid)?;
                    if !["Shift", "Control", "Alt", "Meta"].contains(&modifier)
                        || !seen.insert(modifier)
                    {
                        return Err(invalid());
                    }
                }
            }
        }
        "scroll" => {
            if !["deltaX", "deltaY"]
                .iter()
                .all(|key| args[key].as_f64().is_some_and(f64::is_finite))
            {
                return Err(invalid());
            }
        }
        "drag" => {
            for key in ["from", "to"] {
                let point = args[key].as_object().ok_or_else(invalid)?;
                if point.len() != 2 || !coordinate(&args[key]["x"]) || !coordinate(&args[key]["y"])
                {
                    return Err(invalid());
                }
            }
        }
        _ => {}
    }
    let mut action = object.clone();
    action.insert("kind".into(), kind.into());
    Ok(Value::Object(action))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn model_visible_error_retains_execution_classification() {
        for execution in ["not_started", "partial", "unknown"] {
            let mut error =
                DesktopError::new("desktop-execution-failed", "Native operation failed");
            error.execution = Some(execution.into());
            let rendered = error.to_string();
            assert!(rendered.contains("desktop-execution-failed"));
            assert!(rendered.contains("Native operation failed"));
            assert!(rendered.contains(&format!("execution: {execution}")));
        }
    }
    use serde_json::json;
    #[test]
    fn refuses_identity_and_unknown_fields_for_every_operation() {
        for kind in [
            "startControl",
            "endControl",
            "listDisplay",
            "screenshot",
            "move",
            "click",
            "type",
            "keypress",
            "scroll",
            "drag",
        ] {
            for field in [
                "agentId",
                "clientId",
                "workspaceId",
                "computerId",
                "sessionId",
                "decision",
            ] {
                assert!(validate_action(kind, &json!({field:"forged"})).is_err());
            }
        }
    }
    #[test]
    fn display_selection_is_optional_but_never_empty_or_identity_controlled() {
        assert!(validate_action("listDisplay", &json!({})).is_ok());
        assert!(validate_action("listDisplay", &json!({"displayId":"screen"})).is_err());
        for kind in ["screenshot", "move", "click", "scroll", "drag"] {
            let mut args = match kind {
                "move" | "click" => json!({"layoutId":"layout","x":0,"y":0}),
                "scroll" => json!({"layoutId":"layout","x":0,"y":0,"deltaX":0,"deltaY":1}),
                "drag" => json!({"layoutId":"layout","from":{"x":0,"y":0},"to":{"x":1,"y":1}}),
                _ => json!({}),
            };
            assert!(validate_action(kind, &args).is_ok());
            args["displayId"] = "screen".into();
            assert!(validate_action(kind, &args).is_ok());
            for invalid in [json!(""), json!(null), json!(17)] {
                args["displayId"] = invalid;
                assert!(validate_action(kind, &args).is_err());
            }
        }
    }
    #[test]
    fn move_requires_finite_nonnegative_coordinates_and_rejects_button_semantics() {
        let valid = json!({"layoutId":"layout","x":0,"y":1.5});
        assert_eq!(
            validate_action("move", &valid).unwrap(),
            json!({"kind":"move","layoutId":"layout","x":0,"y":1.5})
        );
        for field in ["x", "y", "layoutId"] {
            let mut args = valid.clone();
            args.as_object_mut().unwrap().remove(field);
            assert!(validate_action("move", &args).is_err());
        }
        for field in ["x", "y"] {
            for value in [
                json!(-1),
                json!("1"),
                json!(null),
                json!(true),
                json!(f64::NAN),
                json!(f64::INFINITY),
                json!(f64::NEG_INFINITY),
            ] {
                let mut args = valid.clone();
                args[field] = value;
                assert!(validate_action("move", &args).is_err(), "{args}");
            }
        }
        for field in ["button", "clickCount", "from", "to", "modifiers"] {
            let mut args = valid.clone();
            args[field] = json!(1);
            assert!(validate_action("move", &args).is_err(), "{field}");
        }
    }
    #[test]
    fn validates_input_limits_and_coordinates() {
        assert!(validate_action(
            "click",
            &json!({"displayId":"d","layoutId":"l","x":0,"y":1,"button":"right","clickCount":2})
        )
        .is_ok());
        assert!(validate_action(
            "click",
            &json!({"displayId":"d","layoutId":"l","x":-1,"y":1})
        )
        .is_err());
        assert!(validate_action("type", &json!({"text":"🦀".repeat(4096)})).is_ok());
        assert!(validate_action("type", &json!({"text":"🦀".repeat(4097)})).is_err());
        assert!(validate_action(
            "keypress",
            &json!({"key":"F24","modifiers":["Meta","Shift"]})
        )
        .is_ok());
        assert!(validate_action("keypress", &json!({"key":"F25"})).is_err());
        assert!(
            validate_action("keypress", &json!({"key":"x","modifiers":["Meta","Meta"]})).is_err()
        );
        assert!(validate_action(
            "drag",
            &json!({"displayId":"d","layoutId":"l","from":{"x":0,"y":1,"z":2},"to":{"x":1,"y":2}})
        )
        .is_err());
    }
}

// Connection provenance is transport-owned; hooks/agents cannot manufacture it.
tokio::task_local! { static CONNECTION: Option<DesktopConnection>; }
pub fn current_connection() -> Option<DesktopConnection> {
    CONNECTION.try_with(Clone::clone).ok().flatten()
}
pub fn with_connection<F: std::future::Future>(
    connection: Option<DesktopConnection>,
    future: F,
) -> impl std::future::Future<Output = F::Output> {
    CONNECTION.scope(connection, future)
}

#[must_use]
pub fn event_visible(event: &crate::Event) -> bool {
    if !event.event_type.starts_with("desktop:") {
        return true;
    }
    let Some(crate::Caller::Wire { principal_id, .. }) = crate::current_caller() else {
        return false;
    };
    let Some(metadata) = &event.metadata else {
        return false;
    };
    if metadata["desktopPrincipalId"].as_str() != Some(principal_id.as_str()) {
        return false;
    }
    metadata["desktopConnectionEpoch"]
        .as_str()
        .is_none_or(|epoch| {
            current_connection().is_some_and(|connection| connection.connection_epoch == epoch)
        })
}
