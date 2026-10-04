//! Strict opt-in hello only. No lifecycle dispatch, source authority or generic fallback.
use intent_core::{Caller, ClientId, WorkspaceApi};
use intent_services::{prepared_source_bootstrap::Mode, Services};
use serde_json::{json, Value};
use std::{collections::HashSet, io::Write};

pub(super) const LIMIT: usize = 8192;
fn invalid() -> intent_core::Error {
    intent_core::Error::InvalidParams("invalid prepared source hello".into())
}
fn token(v: &Value) -> bool {
    v.as_str()
        .is_some_and(|s| !s.is_empty() && s.len() <= 256 && !s.contains('\0'))
}
fn exact(v: &Value, required: &[&str], optional: &[&str]) -> bool {
    v.as_object().is_some_and(|m| {
        required.iter().all(|k| m.contains_key(*k))
            && m.keys()
                .all(|k| required.contains(&k.as_str()) || optional.contains(&k.as_str()))
    })
}

/// Reject duplicate decoded keys before Value would discard them. Scratch bounded by input.
fn strict(raw: &str) -> intent_core::Result<Value> {
    if raw.len() > LIMIT {
        return Err(invalid());
    }
    let b = raw.as_bytes();
    let mut i = 0;
    let mut stack: Vec<Option<HashSet<String>>> = Vec::new();
    while i < b.len() {
        match b[i] {
            b'"' => {
                let start = i;
                i += 1;
                while i < b.len() {
                    if b[i] == b'\\' {
                        i += 2;
                    } else if b[i] == b'"' {
                        break;
                    } else {
                        i += 1;
                    }
                }
                if i >= b.len() {
                    return Err(invalid());
                }
                let text: String = serde_json::from_str(&raw[start..=i]).map_err(|_| invalid())?;
                let mut after = i + 1;
                while after < b.len() && b[after].is_ascii_whitespace() {
                    after += 1;
                }
                if b.get(after) == Some(&b':') {
                    let Some(Some(keys)) = stack.last_mut() else {
                        return Err(invalid());
                    };
                    if !keys.insert(text) {
                        return Err(invalid());
                    }
                }
            }
            b'{' | b'[' => {
                stack.push((b[i] == b'{').then(HashSet::new));
                if stack.len() > 32 {
                    return Err(invalid());
                }
            }
            b'}' | b']' if stack.pop().is_none() => return Err(invalid()),
            _ => {}
        }
        i += 1;
    }
    if !stack.is_empty() {
        return Err(invalid());
    }
    serde_json::from_str(raw).map_err(|_| invalid())
}

pub(super) fn validate(raw: &str, mode: Mode) -> intent_core::Result<Value> {
    let f = strict(raw)?;
    let id_ok = intent_core::note_source_session::validate_id(&f["id"]).is_ok();
    let p = &f["params"];
    if !exact(&f, &["jsonrpc", "id", "method", "params"], &[])
        || f["jsonrpc"] != "2.0"
        || f["method"] != "client.hello"
        || !id_ok
        || !exact(
            p,
            &["sourceSession"],
            &[
                "clientId",
                "name",
                "hostname",
                "prettyHostname",
                "deviceKind",
                "capabilities",
            ],
        )
        || !exact(&p["sourceSession"], &["version", "mode"], &[])
        || intent_core::note_source_session::safe_integer(&p["sourceSession"]["version"]) != Ok(1)
        || p["sourceSession"]["mode"] != mode.as_str()
    {
        return Err(invalid());
    }
    for key in [
        "clientId",
        "name",
        "hostname",
        "prettyHostname",
        "deviceKind",
    ] {
        if p.get(key).is_some_and(|v| !token(v)) {
            return Err(invalid());
        }
    }
    if let Some(c) = p.get("capabilities") {
        if !c
            .as_object()
            .is_some_and(|m| m.keys().all(|k| token(&Value::String(k.clone()))))
        {
            return Err(invalid());
        }
    }
    Ok(f)
}

struct Bounded(Vec<u8>);
impl Write for Bounded {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > LIMIT - self.0.len() {
            return Err(std::io::Error::other("prepared hello response budget"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(super) async fn response(
    raw: &str,
    mode: Mode,
    incarnation: &str,
    caller: Caller,
    api: &Services,
) -> intent_core::Result<String> {
    let frame = validate(raw, mode)?;
    let req = crate::client::classify(&frame).ok_or_else(invalid)?;
    intent_core::with_caller(caller, async {
        let client = crate::client::scope_to_caller(req.client_id.map(ClientId::from_string).unwrap_or_default());
        let mut server = crate::client::server_json(crate::detect_has_display(),std::env::consts::OS,std::env::consts::ARCH,env!("CARGO_PKG_VERSION"),crate::BUILD_COMMIT,false);
        server["sourceSession"] = json!({"version":1,"mode":mode.as_str(),"daemonIncarnation":incarnation});
        let value = json!({"jsonrpc":"2.0","id":frame["id"],"result":{"clientId":client.as_str(),"protocolVersion":crate::PROTOCOL_VERSION,"server":server}});
        let mut out = Bounded(Vec::with_capacity(LIMIT));
        serde_json::to_writer(&mut out, &value).map_err(|_| intent_core::Error::InvalidParams("note-page-budget".into()))?;
        // Same real client persistence as ordinary hello, after bounded response construction.
        api.upsert_client(client,req.name,req.capabilities,req.host).await?;
        String::from_utf8(out.0).map_err(|_| invalid())
    }).await
}
