use serde_json::{json, Value};

use super::*;

const BRIDGE_TOKEN: &str = "c2VjcmV0LWJyaWRnZS10b2tlbg";

fn redact(args: &[&str]) -> String {
    redact_args(args.iter().map(|arg| (*arg).to_owned()))
}

/// The claude adapter's real shape: `--mcp-config` carries the bridge's env
/// (with `INTENTD_MCP_BRIDGE_TOKEN`) as one JSON argument. Every env value
/// goes; the keys, the command and its args stay so the row still says what
/// runs.
#[test]
fn mcp_config_env_values_are_redacted_and_the_shape_kept() {
    let config = json!({
        "mcpServers": {
            "workspace-mcp": {
                "command": "/usr/local/bin/intentd",
                "args": ["mcp-bridge", "--agent", "agent-1"],
                "env": {
                    "INTENTD_MCP_BRIDGE_TOKEN": BRIDGE_TOKEN,
                    "HOME": "/home/me",
                    "PORT": 5297,
                },
            },
        },
    })
    .to_string();
    let out = redact(&["claude", "--mcp-config", &config, "--verbose"]);
    assert!(!out.contains(BRIDGE_TOKEN), "{out}");
    assert!(!out.contains("/home/me"), "{out}");
    assert!(!out.contains("5297"), "{out}");
    let (head, rest) = out.split_once(' ').expect("args");
    assert_eq!(head, "claude");
    let rest = rest.strip_prefix("--mcp-config ").expect("flag kept");
    let json_part = rest.strip_suffix(" --verbose").expect("tail kept");
    let parsed: Value = serde_json::from_str(json_part).expect("still JSON");
    let server = &parsed["mcpServers"]["workspace-mcp"];
    assert_eq!(server["command"], "/usr/local/bin/intentd");
    assert_eq!(server["args"], json!(["mcp-bridge", "--agent", "agent-1"]));
    assert_eq!(
        server["env"],
        json!({
            "INTENTD_MCP_BRIDGE_TOKEN": REDACTED,
            "HOME": REDACTED,
            "PORT": REDACTED,
        })
    );
}

/// HTTP MCP servers carry `headers`; secret-named keys anywhere in a JSON
/// argument go too, and a JSON args array gets the argument rules.
#[test]
fn json_headers_secret_keys_and_nested_args_are_redacted() {
    let config = json!({
        "servers": [{
            "url": "https://mcp.example/x",
            "headers": { "Authorization": "Bearer abc123", "X-Trace": "t-1" },
            "apiKey": "sk-live-1",
            "args": ["--token", "tok-9", "serve"],
            "name": "kept",
        }],
    })
    .to_string();
    let out = redact(&[&config]);
    for secret in ["abc123", "t-1", "sk-live-1", "tok-9"] {
        assert!(!out.contains(secret), "{secret} leaked: {out}");
    }
    let parsed: Value = serde_json::from_str(&out).expect("still JSON");
    let server = &parsed["servers"][0];
    assert_eq!(server["name"], "kept");
    assert_eq!(server["url"], "https://mcp.example/x");
    assert_eq!(server["args"], json!(["--token", REDACTED, "serve"]));
}

/// A secret-named flag hides its value, whether it is the next argument or
/// after `=`; a JSON value after `=` gets the JSON rules.
#[test]
fn secret_flags_hide_their_values() {
    assert_eq!(
        redact(&[
            "tool",
            "--token",
            "t1",
            "--api-key=k1",
            "--password",
            "p1",
            "x"
        ]),
        format!("tool --token {REDACTED} --api-key={REDACTED} --password {REDACTED} x")
    );
    assert_eq!(
        redact(&["tool", "--config={\"env\":{\"A\":\"v\"}}"]),
        format!("tool --config={{\"env\":{{\"A\":\"{REDACTED}\"}}}}")
    );
}

/// `NAME=value` assignments (an `env` prefix, a shell wrapper) hide
/// secret-named values and keep the rest.
#[test]
fn secret_env_assignments_are_redacted() {
    assert_eq!(
        redact(&[
            "env",
            "OPENAI_API_KEY=sk-1",
            "GH_TOKEN=gh-2",
            "PATH=/usr/bin",
            "node"
        ]),
        format!("env OPENAI_API_KEY={REDACTED} GH_TOKEN={REDACTED} PATH=/usr/bin node")
    );
}

/// Credentials in URLs and bearer tokens inside a longer argument go.
#[test]
fn url_credentials_and_bearer_tokens_are_redacted() {
    let out = redact(&[
        "git",
        "fetch",
        "https://x-access-token:ghs_abc@github.com/o/r.git",
        "-H",
        "Authorization: Bearer eyJhbGciOi",
    ]);
    assert!(!out.contains("ghs_abc"), "{out}");
    assert!(!out.contains("eyJhbGciOi"), "{out}");
    assert!(out.contains("@github.com/o/r.git"), "{out}");
    assert!(
        out.ends_with(&format!("-H Authorization: {REDACTED}")),
        "{out}"
    );
}

/// Ordinary command lines are unchanged; a trailing secret flag with no
/// value, malformed JSON and an empty list are left alone.
#[test]
fn ordinary_arguments_are_unchanged() {
    assert_eq!(
        redact(&["node", "/opt/adapter/index.js", "--acp", "--model", "opus"]),
        "node /opt/adapter/index.js --acp --model opus"
    );
    assert_eq!(redact(&["tool", "--token"]), "tool --token");
    assert_eq!(redact(&["tool", "{not json", "[1,"]), "tool {not json [1,");
    assert_eq!(redact(&[]), "");
}

/// Header-shaped arguments (`Name: value`, with or without the space) hide
/// secret-named headers and any `Bearer` / `Basic` credential, including
/// one quoted inside a config string; ordinary headers stay.
#[test]
fn secret_headers_are_redacted() {
    for arg in [
        "Authorization:Bearer abc",
        "Authorization: token abc",
        "X-Api-Key: abc",
        "mcp_servers.intent.http_headers={Authorization=\"Bearer abc\"}",
    ] {
        let out = redact(&["npx", "mcp-remote", "--header", arg]);
        assert!(!out.contains("abc"), "{arg} leaked: {out}");
    }
    assert_eq!(
        redact(&["curl", "-H", "Content-Type: application/json"]),
        "curl -H Content-Type: application/json"
    );
}

/// Secret-named URL query parameters go, in free text and inside JSON; the
/// rest of the URL stays.
#[test]
fn secret_url_query_parameters_are_redacted() {
    assert_eq!(
        redact(&["x", "--url", "https://h/mcp?token=abc&region=eu#top"]),
        format!("x --url https://h/mcp?token={REDACTED}&region=eu#top")
    );
    let out = redact(&[r#"{"mcpServers":{"i":{"url":"https://h/mcp?api_key=abc"}}}"#]);
    assert!(!out.contains("abc"), "{out}");
    assert!(out.contains("https://h/mcp?api_key="), "{out}");
}

/// A flag whose `=` value is itself an assignment (`--env=GH_TOKEN=x`) and
/// a JSON array mixing strings and other values keep the argument rules.
#[test]
fn nested_assignments_and_mixed_arrays_are_redacted() {
    assert_eq!(
        redact(&["docker", "run", "--env=GH_TOKEN=abc", "--env=LANG=C", "img"]),
        format!("docker run --env=GH_TOKEN={REDACTED} --env=LANG=C img")
    );
    let out = redact(&[r#"{"args":["--api-key","abc",1]}"#]);
    assert!(!out.contains("abc"), "{out}");
}
