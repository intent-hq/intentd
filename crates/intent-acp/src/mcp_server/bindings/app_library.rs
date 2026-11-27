//! `ws.appLibrary.*` bindings — install Intent apps (PROTOCOL §5.60, Labs).
//!
//! An agent that built an app bundle in its workspace installs it into the
//! daemon's app library with `ws.appLibrary.install`, and reads the library
//! with `list` / `get`. Every call runs the router `app.*` method through
//! [`WorkspaceApi::app_call`] as the calling agent (`Caller::Agent`), bound
//! to the caller's own workspace: the binding sets `workspaceId` itself and
//! refuses one from the caller.
//!
//! Only these three library methods are bound. An agent can never approve
//! an app, open an app session or call the app-principal methods: none has
//! a binding, and the service refuses `app.approve` / `app.session.open`
//! from an agent (`forbidden` / `user-only`) on every other path too.
//!
//! The namespace is `appLibrary`, not `app`: `ws.app.*` is the Assistant
//! workspace's own namespace (`bindings/app/`).

use std::sync::Arc;

use intent_core::{WorkspaceApi, WorkspaceId};
use serde_json::{json, Map, Value};

use super::{opt_strict_bool, req_str};

pub(crate) const PRELUDE: &str = r"
    globalThis.ws = globalThis.ws || {};
    ws.appLibrary = {
        install: (pathOrOpts, opts) => host({
            method: 'appLibrary.install',
            args: typeof pathOrOpts === 'string'
                ? { ...(opts || {}), path: pathOrOpts }
                : (pathOrOpts || {}),
        }),
        list: () => host({ method: 'appLibrary.list', args: {} }),
        get: (appId) => host({ method: 'appLibrary.get', args: { appId } }),
    };
";

/// `ws.help("appLibrary")`. Labs: the namespace is installed for every
/// agent but documented only here, so the tool description and the system
/// prompt do not promote it. The intent-apps skill names this tool.
pub(crate) const HELP: &str = "  ws.appLibrary.install(path | { path, replace?, dryRun? }) → { app, replaced, approvalReset, dryRun, nextStep }  // Labs. Install an Intent app bundle from this workspace into the app library. `path` is the bundle folder (it holds `intent-app.json`) relative to the workspace root; `dryRun: true` only validates. An installed id needs `replace: true`, and a new bundle hash resets the approval. `app` is `{ id, name, version, description, entry, bundleHash, fileCount, sizeBytes, permissions, approval, open }`.
    The user approves each bundle hash in the app library. Agents cannot approve or open apps, so tell the user `nextStep`. Errors start with the contract code: `invalid-manifest` (with the JSON pointer `field`), `invalid-bundle`, `path-not-found`, `app-exists`, `level-unavailable`.
  ws.appLibrary.list() → { apps }  // Labs. Installed apps as `install` summary rows, sorted by name.
  ws.appLibrary.get(appId) → { app }  // Labs. One installed app with every field (window, source, publisher, signature); an unknown id fails with `app-not-found`.";

/// The `App` keys an install or list row carries: the manifest summary,
/// the bundle hash and the approval. `get` returns the whole `App`.
const SUMMARY_KEYS: &[&str] = &[
    "id",
    "name",
    "version",
    "description",
    "entry",
    "bundleHash",
    "fileCount",
    "sizeBytes",
    "permissions",
    "approval",
    "open",
];

/// What the agent tells the user after an install that needs approval.
const APPROVAL_NEEDED: &str = "The user must approve this version in the Intent app library before it can open. Agents cannot approve apps: ask the user.";

/// What a same-hash replace of an approved app means.
const STILL_APPROVED: &str =
    "This bundle hash is already approved; the user can open the app from the app library.";

/// What a dry run means.
const DRY_RUN_NOTE: &str =
    "Dry run: the bundle is valid and nothing was installed. Call again without dryRun to install.";

pub(crate) async fn dispatch(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    method: &str,
    args: &Value,
) -> Result<Value, String> {
    match method {
        "install" => install(api, ws, args).await,
        "list" => list(api).await,
        "get" => get(api, args).await,
        other => Err(format!("host: unknown method `appLibrary.{other}`")),
    }
}

async fn install(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let params = install_params(ws, args)?;
    let reply = api
        .app_call("app.install".to_owned(), params)
        .await
        .map_err(app_err)?;
    Ok(install_result(&reply))
}

/// The `app.install` params: the caller's workspace, `path` and the two
/// strict booleans. Any other key is refused, so a typo never installs with
/// a default the caller did not mean.
fn install_params(ws: &WorkspaceId, args: &Value) -> Result<Map<String, Value>, String> {
    if let Some(obj) = args.as_object() {
        if obj.contains_key("workspaceId") {
            return Err(
                "invalid params: `workspaceId` is not accepted; ws.appLibrary.install installs from your own workspace"
                    .to_owned(),
            );
        }
        if let Some(key) = obj
            .keys()
            .find(|k| !matches!(k.as_str(), "path" | "replace" | "dryRun"))
        {
            return Err(format!(
                "invalid params: unknown key `{key}` (expected path, replace?, dryRun?)"
            ));
        }
    }
    let path = req_str(args, "path")?;
    let mut params = Map::new();
    params.insert("workspaceId".into(), json!(ws.as_str()));
    params.insert("path".into(), json!(path));
    for key in ["replace", "dryRun"] {
        if let Some(v) = opt_strict_bool(args, key)? {
            params.insert(key.into(), json!(v));
        }
    }
    Ok(params)
}

fn install_result(reply: &Value) -> Value {
    let app = reply.get("app").unwrap_or(&Value::Null);
    let dry_run = reply.get("dryRun").and_then(Value::as_bool) == Some(true);
    let approved = app
        .pointer("/approval/status")
        .and_then(Value::as_str)
        .is_some_and(|s| s == "approved");
    let next_step = if dry_run {
        DRY_RUN_NOTE
    } else if approved {
        STILL_APPROVED
    } else {
        APPROVAL_NEEDED
    };
    json!({
        "app": summary(app),
        "replaced": reply.get("replaced").cloned().unwrap_or(json!(false)),
        "approvalReset": reply.get("approvalReset").cloned().unwrap_or(json!(false)),
        "dryRun": dry_run,
        "nextStep": next_step,
    })
}

async fn list(api: &Arc<dyn WorkspaceApi>) -> Result<Value, String> {
    let reply = api
        .app_call("app.list".to_owned(), Map::new())
        .await
        .map_err(app_err)?;
    let apps: Vec<Value> = reply
        .get("apps")
        .and_then(Value::as_array)
        .map_or(&[][..], Vec::as_slice)
        .iter()
        .map(summary)
        .collect();
    Ok(json!({ "apps": apps }))
}

async fn get(api: &Arc<dyn WorkspaceApi>, args: &Value) -> Result<Value, String> {
    let app_id = req_str(args, "appId")?;
    let mut params = Map::new();
    params.insert("appId".into(), json!(app_id));
    api.app_call("app.get".to_owned(), params)
        .await
        .map_err(app_err)
}

/// Project an `App` to [`SUMMARY_KEYS`], in that order.
fn summary(app: &Value) -> Value {
    let mut out = Map::new();
    for key in SUMMARY_KEYS {
        if let Some(v) = app.get(*key) {
            out.insert((*key).to_owned(), v.clone());
        }
    }
    Value::Object(out)
}

/// Render an `app.*` refusal with its contract code first
/// (`invalid-manifest: invalid manifest: … {"field":"/name"}`), so an agent
/// can act on it; `detail` is already in the message and is left out.
// By-value so it slots point-free into `map_err(app_err)`.
#[expect(clippy::needless_pass_by_value)]
fn app_err(e: intent_core::Error) -> String {
    let intent_core::Error::App(app) = &e else {
        return e.to_string();
    };
    let code = app.data_code();
    let extra: Map<String, Value> = app
        .data
        .as_object()
        .into_iter()
        .flatten()
        .filter(|(k, v)| !matches!(k.as_str(), "code" | "detail") && !v.is_null())
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let head = if code.is_empty() {
        app.message.clone()
    } else {
        format!("{code}: {}", app.message)
    };
    if extra.is_empty() {
        head
    } else {
        format!("{head} {}", Value::Object(extra))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use intent_core::apps::AppError;

    fn ws() -> WorkspaceId {
        WorkspaceId::from_string("ws-1".to_owned())
    }

    #[test]
    fn help_names_every_bound_method() {
        for method in ["install", "list", "get"] {
            assert!(
                HELP.contains(&format!("  ws.appLibrary.{method}(")),
                "{method}"
            );
            assert!(PRELUDE.contains(&format!("{method}: (")), "{method}");
        }
        assert!(!HELP.contains("approve("), "agents get no approve binding");
    }

    #[test]
    fn install_binds_the_callers_workspace() {
        let p = install_params(&ws(), &json!({ "path": "apps/x", "dryRun": true })).unwrap();
        assert_eq!(
            Value::Object(p),
            json!({ "workspaceId": "ws-1", "path": "apps/x", "dryRun": true })
        );
    }

    #[test]
    fn install_refuses_another_workspace_and_unknown_keys() {
        let err =
            install_params(&ws(), &json!({ "path": "x", "workspaceId": "ws-2" })).unwrap_err();
        assert!(err.contains("`workspaceId` is not accepted"), "{err}");
        let err = install_params(&ws(), &json!({ "path": "x", "dryrun": true })).unwrap_err();
        assert!(err.contains("unknown key `dryrun`"), "{err}");
        let err = install_params(&ws(), &json!({ "path": "x", "replace": "yes" })).unwrap_err();
        assert!(err.contains("`replace` must be a boolean"), "{err}");
        let err = install_params(&ws(), &json!({})).unwrap_err();
        assert_eq!(err, "path is required");
    }

    #[test]
    fn install_result_says_what_the_user_must_do() {
        let app = json!({
            "id": "a", "name": "A", "version": "1.0.0", "description": null,
            "entry": "index.html", "bundleHash": "sha256:00", "fileCount": 2,
            "sizeBytes": 10, "permissions": { "level": 1, "features": [] },
            "approval": { "status": "pending" }, "open": false,
            "publisher": null, "signature": null, "icon": null,
        });
        let out = install_result(
            &json!({ "app": app, "replaced": false, "approvalReset": false, "dryRun": false }),
        );
        assert_eq!(out["nextStep"], APPROVAL_NEEDED);
        assert_eq!(out["app"]["bundleHash"], "sha256:00");
        assert!(out["app"].get("publisher").is_none());
        let dry = install_result(&json!({ "app": app, "dryRun": true }));
        assert_eq!(dry["nextStep"], DRY_RUN_NOTE);
        let mut approved = app.clone();
        approved["approval"]["status"] = json!("approved");
        let kept = install_result(&json!({ "app": approved, "replaced": true, "dryRun": false }));
        assert_eq!(kept["nextStep"], STILL_APPROVED);
    }

    #[test]
    fn refusals_lead_with_the_contract_code() {
        let e = AppError::params(
            "invalid-manifest",
            "invalid manifest: name is required",
            json!({ "detail": "name is required", "field": "/name" }),
        )
        .into_error();
        assert_eq!(
            app_err(e),
            r#"invalid-manifest: invalid manifest: name is required {"field":"/name"}"#
        );
        let e = AppError::params(
            "invalid-bundle",
            "invalid bundle: too many files",
            json!({ "detail": "too many files", "path": null }),
        )
        .into_error();
        assert_eq!(app_err(e), "invalid-bundle: invalid bundle: too many files");
    }
}
