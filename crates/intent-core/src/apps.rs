//! Intent apps (PROTOCOL §5.60, Labs): the app principal and the typed
//! `app.*` refusals shared by the transport and the service layer.
//!
//! An app connection is a fresh administrator connection that sent
//! `app.session.attach { token }` as its first request. The transport then
//! binds every later frame on it to [`crate::Caller::App`] with the
//! [`AppPrincipal`] below. That caller has no authority of its own: every
//! capability gate refuses it like an executor credential, and the
//! transport lets it reach only the methods in its level allowlist.

use std::sync::Arc;

use serde_json::{json, Value};

/// Feature names a manifest may request (§5.60 "Features").
pub const APP_FEATURES: &[&str] = &["alwaysOnTop", "globalShortcuts", "models", "notifications"];

/// Feature that grants `app.notify`.
pub const APP_FEATURE_NOTIFICATIONS: &str = "notifications";

/// Feature that grants `app.models.list` and `app.complete`.
pub const APP_FEATURE_MODELS: &str = "models";

/// Why an app session ended. The host maps these to the bridge
/// `session-ended` reasons; the daemon only closes the connections.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppSessionEnd {
    /// `app.session.close`, a newer `app.session.open`, or the opener left.
    Closed,
    /// `app.revoke`.
    Revoked,
    /// `app.uninstall`.
    Uninstalled,
    /// A replace install or a changed feature grant.
    Updated,
}

/// The principal an attached app connection is bound to (§5.60 "The app
/// principal"). Immutable for the life of the session: a change of grant
/// ends the session instead of mutating it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppPrincipal {
    pub app_id: String,
    pub name: String,
    pub bundle_hash: String,
    /// Approved level (1 or 2 in M1).
    pub level: u8,
    /// Approved features, sorted.
    pub features: Vec<String>,
    pub session_id: String,
}

impl AppPrincipal {
    /// Whether the session was granted `feature`.
    #[must_use]
    pub fn has_feature(&self, feature: &str) -> bool {
        self.features.iter().any(|f| f == feature)
    }
}

/// The result of a successful `app.session.attach`: the bound principal, the
/// wire result (an `AppSession` without `token`), and a receiver that
/// resolves when the session ends. The transport closes the connection when
/// `ended` changes or its sender drops.
pub struct AppAttachment {
    pub principal: Arc<AppPrincipal>,
    pub result: Value,
    pub ended: tokio::sync::watch::Receiver<bool>,
}

/// A typed `app.*` refusal (§5.60 "Errors"): the JSON-RPC code, a human
/// message and `error.data` (always an object with `code`).
#[derive(Debug, Clone, PartialEq)]
pub struct AppError {
    pub rpc_code: i32,
    pub message: String,
    pub data: Value,
}

impl AppError {
    /// A `-32602` refusal with `data = { code, ..extra }`.
    #[must_use]
    pub fn params(code: &str, message: impl Into<String>, extra: Value) -> Self {
        Self::with(-32602, code, message, extra)
    }

    /// A `-32603` failure with `data = { code, ..extra }`.
    #[must_use]
    pub fn service(code: &str, message: impl Into<String>, extra: Value) -> Self {
        Self::with(-32603, code, message, extra)
    }

    fn with(rpc_code: i32, code: &str, message: impl Into<String>, extra: Value) -> Self {
        let mut data = json!({ "code": code });
        if let (Some(target), Value::Object(extra)) = (data.as_object_mut(), extra) {
            for (k, v) in extra {
                target.insert(k, v);
            }
        }
        Self {
            rpc_code,
            message: message.into(),
            data,
        }
    }

    /// This refusal as a domain [`crate::Error`]. (No `From` impl: a second
    /// `From` into `Error` breaks `?` inference in other crates.)
    #[must_use]
    pub fn into_error(self) -> crate::Error {
        crate::Error::App(Box::new(self))
    }

    /// The `data.code` string.
    #[must_use]
    pub fn data_code(&self) -> &str {
        self.data.get("code").and_then(Value::as_str).unwrap_or("")
    }
}

impl std::fmt::Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

/// `-32602 { code: "invalid-params", detail }`.
#[must_use]
pub fn app_invalid_params(detail: impl Into<String>) -> crate::Error {
    let detail = detail.into();
    AppError::params(
        "invalid-params",
        format!("invalid params: {detail}"),
        json!({ "detail": detail }),
    )
    .into_error()
}

/// `-32602 { code }` with a short message and no extra data.
#[must_use]
pub fn app_refusal(code: &str, message: impl Into<String>) -> crate::Error {
    AppError::params(code, message, json!({})).into_error()
}

/// The service-level forbidden refusals (§5.60): `-32003 { code:
/// "forbidden", detail }` through [`crate::Error::Forbidden`].
pub mod forbidden {
    /// User-only method (`app.approve`, `app.session.open`) from an agent.
    pub const USER_ONLY: &str = "user-only";
    /// App-principal method from any other caller.
    pub const APP_PRINCIPAL_REQUIRED: &str = "app principal required";

    /// `feature <name> not approved`.
    #[must_use]
    pub fn feature_not_approved(feature: &str) -> String {
        format!("feature {feature} not approved")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn app_error_merges_extra_data_after_code() {
        let e = AppError::params("hash-mismatch", "m", json!({ "bundleHash": "sha256:x" }));
        assert_eq!(e.rpc_code, -32602);
        assert_eq!(
            e.data,
            json!({ "code": "hash-mismatch", "bundleHash": "sha256:x" })
        );
        assert_eq!(e.data_code(), "hash-mismatch");
    }

    #[test]
    fn features_are_sorted_and_unique() {
        let mut sorted = APP_FEATURES.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted, APP_FEATURES);
    }
}
