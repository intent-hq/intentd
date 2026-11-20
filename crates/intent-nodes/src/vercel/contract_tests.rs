//! The shared fake Vercel ([`super::fake_api`]) held to the recording of
//! the real API in `fixtures/vercel.json`. Each recorded call is replayed
//! in order on a fresh fake, which must answer with the same status, the
//! same error code, and every field the adapter reads with the recorded
//! value; a command must stream the same kinds of lines. The recording
//! must also hold every field the adapter's response types read, so a new
//! read fails here until the fixtures are captured again
//! (`scripts/capture-vercel-fixtures.sh`).

use serde_json::{json, Value};

use super::capture::{TIME_KEYS, WRONG_TOKEN};
use super::client::{CommandBody, LogLine, SandboxBody, SessionBody, SessionReply};
use super::fake_api::{command_stream, Reply, VercelApi, FIXTURE_TEAM, FIXTURE_TOKEN};
use crate::agent37::fields::fields_of;

const FIXTURE: &str = include_str!("fixtures/vercel.json");

/// What the adapter reads of one route's success body.
struct Read {
    /// `METHOD /path` (`*` for one part).
    route: &'static str,
    /// JSON pointer to the items; `/sandboxes/*` walks the list.
    at: &'static str,
    /// Fields read through serde: present, with the same JSON type.
    fields: &'static [&'static str],
    /// Fields compared by value too.
    exact: &'static [&'static str],
}

const SANDBOX_EXACT: &[&str] = &[
    "name",
    "status",
    "currentSessionId",
    "currentSnapshotId",
    "persistent",
    "vcpus",
    "memory",
    "timeout",
    "cwd",
    "tags",
    "keepLastSnapshots",
    "snapshotExpiration",
    "image",
];
const SESSION_EXACT: &[&str] = &["id", "status", "timeout", "sourceSnapshotId", "cwd"];

fn reads() -> Vec<Read> {
    let sandbox = fields_of::<SandboxBody>();
    let session = fields_of::<SessionBody>();
    let reply = fields_of::<SessionReply>();
    let mut reads = Vec::new();
    for route in [
        "POST /v3/sandboxes",
        "GET /v2/sandboxes/*",
        "POST /v2/sandboxes/sessions/*/stop",
    ] {
        reads.push(Read {
            route,
            at: "",
            fields: reply,
            exact: if route.starts_with("GET") {
                &["resumed"]
            } else {
                &[]
            },
        });
        reads.push(Read {
            route,
            at: "/sandbox",
            fields: sandbox,
            exact: SANDBOX_EXACT,
        });
        reads.push(Read {
            route,
            at: "/session",
            fields: session,
            exact: SESSION_EXACT,
        });
    }
    reads.push(Read {
        route: "GET /v2/sandboxes",
        at: "/sandboxes/*",
        fields: sandbox,
        exact: SANDBOX_EXACT,
    });
    reads.push(Read {
        route: "GET /v2/sandboxes",
        at: "/pagination",
        fields: &["next"],
        exact: &["next"],
    });
    reads.push(Read {
        route: "POST /v2/sandboxes/sessions/*/extend-timeout",
        at: "/session",
        fields: session,
        exact: SESSION_EXACT,
    });
    reads.push(Read {
        route: "DELETE /v2/sandboxes/*",
        at: "/sandbox",
        fields: &["name"],
        exact: &["name"],
    });
    reads
}

fn fixture() -> Value {
    serde_json::from_str(FIXTURE).expect("the fixture is JSON")
}

fn route_matches(route: &str, method: &str, path: &str) -> bool {
    let path = path.split('?').next().unwrap_or_default();
    let Some((want_method, want_path)) = route.split_once(' ') else {
        return false;
    };
    let want: Vec<&str> = want_path.trim_matches('/').split('/').collect();
    let got: Vec<&str> = path.trim_matches('/').split('/').collect();
    want_method == method
        && want.len() == got.len()
        && want.iter().zip(&got).all(|(w, g)| *w == "*" || w == g)
}

fn items<'a>(value: &'a Value, at: &str) -> Vec<&'a Value> {
    let mut found = vec![value];
    for part in at.split('/').filter(|p| !p.is_empty()) {
        found = found
            .into_iter()
            .flat_map(|v| match (part, v) {
                ("*", Value::Array(all)) => all.iter().collect(),
                ("*", _) => vec![],
                (key, v) => v.get(key).into_iter().collect(),
            })
            .collect();
    }
    found
}

fn error_code(body: &Value) -> Option<String> {
    Some(body.get("error")?.get("code")?.as_str()?.to_owned())
}

/// The kind of each line of a command stream: `start`, `stdout`,
/// `stderr` or `end`.
fn stream_kinds(lines: &[Value]) -> Vec<String> {
    lines
        .iter()
        .map(
            |line| match (line.pointer("/command/exitCode"), line.get("stream")) {
                (Some(Value::Null), _) => "start".to_owned(),
                (Some(_), _) => "end".to_owned(),
                (None, Some(stream)) => stream.as_str().unwrap_or_default().to_owned(),
                _ => "unknown".to_owned(),
            },
        )
        .collect()
}

/// The recorded output of a command stream: `(exit, stdout, stderr)`.
fn recorded_output(lines: &[Value]) -> (i32, String, String) {
    let mut out = (0, String::new(), String::new());
    for line in lines {
        match line.get("stream").and_then(Value::as_str) {
            Some("stdout") => out.1.push_str(line["data"].as_str().unwrap_or_default()),
            Some("stderr") => out.2.push_str(line["data"].as_str().unwrap_or_default()),
            _ => {}
        }
        if let Some(code) = line.pointer("/command/exitCode").and_then(Value::as_i64) {
            out.0 = i32::try_from(code).unwrap_or(-1);
        }
    }
    out
}

struct Replayed {
    name: String,
    method: String,
    path: String,
    recorded_status: u16,
    recorded: Value,
    fake: Reply,
}

/// The path as the client sends it: `teamId` added unless given.
fn with_team(path: &str) -> String {
    if path.contains("teamId=") {
        return path.to_owned();
    }
    let sep = if path.contains('?') { '&' } else { '?' };
    format!("{path}{sep}teamId={FIXTURE_TEAM}")
}

/// Every recorded call, answered again by a fresh fake.
fn replay(api: &mut VercelApi) -> Vec<Replayed> {
    let fixture = fixture();
    fixture["exchanges"]
        .as_array()
        .expect("exchanges")
        .iter()
        .map(|exchange| {
            let request = &exchange["request"];
            let token = if request["auth"] == "token" {
                FIXTURE_TOKEN
            } else {
                WRONG_TOKEN
            };
            let method = request["method"].as_str().unwrap_or_default().to_owned();
            let path = request["path"].as_str().unwrap_or_default().to_owned();
            let recorded = exchange["response"]["body"].clone();
            let fake = if request["plane"] == "registry" {
                api.manifest(&path, Some((FIXTURE_TEAM, token)))
            } else if method == "POST" && path.ends_with("/cmd") {
                let session = path.split('/').nth(4).unwrap_or_default();
                let body = request.get("body").cloned().unwrap_or(Value::Null);
                match api.command(session, &with_team(&path), Some(token), &body) {
                    Ok(start) => {
                        let lines = recorded["ndjson"].as_array().cloned().unwrap_or_default();
                        let (exit, stdout, stderr) = recorded_output(&lines);
                        let id = api.command_id();
                        Reply::ok(json!({
                            "ndjson": command_stream(&start, &id, exit, &stdout, &stderr),
                        }))
                    }
                    Err(reply) => reply,
                }
            } else {
                api.handle(&method, &with_team(&path), Some(token), request.get("body"))
            };
            Replayed {
                name: exchange["name"].as_str().unwrap_or_default().to_owned(),
                method,
                path,
                recorded_status: u16::try_from(
                    exchange["response"]["status"].as_u64().unwrap_or(0),
                )
                .unwrap_or(0),
                recorded,
                fake,
            }
        })
        .collect()
}

fn same_kind(a: &Value, b: &Value) -> bool {
    std::mem::discriminant(a) == std::mem::discriminant(b)
}

fn compare_stream(what: &str, recorded: &Value, fake: &Value, found: &mut Vec<String>) {
    let (Some(recorded), Some(fake)) = (recorded["ndjson"].as_array(), fake["ndjson"].as_array())
    else {
        found.push(format!("{what}: not a command stream"));
        return;
    };
    // Vercel interleaves output lines as the command writes them; the
    // shape is: the command, output lines, the finished command.
    let shape = |kinds: Vec<String>| {
        let n = kinds.len();
        n >= 2
            && kinds[0] == "start"
            && kinds[n - 1] == "end"
            && kinds[1..n - 1]
                .iter()
                .all(|k| k == "stdout" || k == "stderr")
    };
    if !shape(stream_kinds(recorded)) || !shape(stream_kinds(fake)) {
        found.push(format!(
            "{what}: lines {:?} from the fake, {:?} recorded",
            stream_kinds(fake),
            stream_kinds(recorded)
        ));
        return;
    }
    if recorded_output(recorded) != recorded_output(fake) {
        found.push(format!("{what}: other output from the fake"));
    }
    let command = fields_of::<CommandBody>();
    let log = fields_of::<LogLine>();
    let pairs = [
        (recorded.first(), fake.first(), command, "/command"),
        (recorded.last(), fake.last(), command, "/command"),
        (recorded.get(1), fake.get(1), log, ""),
    ];
    for (want, got, fields, at) in pairs {
        let (Some(want), Some(got)) = (want, got) else {
            continue;
        };
        let (want, got) = if at.is_empty() {
            (want, got)
        } else {
            (&want["command"], &got["command"])
        };
        for field in fields {
            if let Some(want) = want.get(*field) {
                if !got.get(*field).is_some_and(|got| same_kind(want, got)) {
                    found.push(format!(
                        "{what}{at}/{field}: {:?} from the fake, {want} recorded",
                        got.get(*field)
                    ));
                }
            }
        }
    }
}

/// Every way the fake differs from the recording.
fn differences(api: &mut VercelApi) -> Vec<String> {
    let reads = reads();
    let mut found = Vec::new();
    for call in replay(api) {
        let what = &call.name;
        if call.fake.status != call.recorded_status {
            found.push(format!(
                "{what}: status {} from the fake, {} recorded",
                call.fake.status, call.recorded_status
            ));
            continue;
        }
        if call.recorded_status >= 400 {
            let (recorded, fake) = (error_code(&call.recorded), error_code(&call.fake.body));
            if recorded != fake {
                found.push(format!(
                    "{what}: error {fake:?} from the fake, {recorded:?} recorded"
                ));
            }
            let invalid = |body: &Value| body.pointer("/error/invalidToken").cloned();
            if invalid(&call.recorded) != invalid(&call.fake.body) {
                found.push(format!("{what}: invalidToken differs"));
            }
            continue;
        }
        if call.method == "POST" && call.path.ends_with("/cmd") {
            compare_stream(what, &call.recorded, &call.fake.body, &mut found);
            continue;
        }
        for read in reads
            .iter()
            .filter(|r| route_matches(r.route, &call.method, &call.path))
        {
            let recorded = items(&call.recorded, read.at);
            let fake = items(&call.fake.body, read.at);
            if recorded.len() != fake.len() {
                found.push(format!(
                    "{what} {}: {} items from the fake, {} recorded",
                    read.at,
                    fake.len(),
                    recorded.len()
                ));
                continue;
            }
            for (i, (recorded, fake)) in recorded.iter().zip(&fake).enumerate() {
                for field in read.fields.iter().chain(read.exact) {
                    let Some(want) = recorded.get(*field) else {
                        continue;
                    };
                    let Some(got) = fake.get(*field) else {
                        found.push(format!(
                            "{what} {}[{i}]/{field}: missing from the fake",
                            read.at
                        ));
                        continue;
                    };
                    let by_value = read.exact.contains(field) && !TIME_KEYS.contains(field);
                    if (by_value && want != got) || !same_kind(want, got) {
                        found.push(format!(
                            "{what} {}[{i}]/{field}: {got} from the fake, {want} recorded",
                            read.at
                        ));
                    }
                }
            }
        }
    }
    found
}

#[test]
fn the_fake_answers_every_recorded_call_as_vercel_did() {
    let found = differences(&mut VercelApi::default());
    assert!(
        found.is_empty(),
        "the fake Vercel differs from the recording:\n{}",
        found.join("\n")
    );
}

#[test]
fn the_recording_holds_every_field_the_adapter_reads() {
    let fixture = fixture();
    let exchanges = fixture["exchanges"].as_array().expect("exchanges");
    let ok = |e: &&Value| (200..300).contains(&e["response"]["status"].as_u64().unwrap_or(0));
    let mut missing = Vec::new();
    // A field one response type reads must be recorded on some route that
    // answers with it: optional ones (`resumed`) are not on every route.
    let mut by_type: Vec<(&[&str], Vec<&Value>, String)> = Vec::new();
    for read in reads() {
        let bodies: Vec<&Value> = exchanges
            .iter()
            .filter(ok)
            .filter(|e| {
                route_matches(
                    read.route,
                    e["request"]["method"].as_str().unwrap_or_default(),
                    e["request"]["path"].as_str().unwrap_or_default(),
                )
            })
            .flat_map(|e| items(&e["response"]["body"], read.at))
            .collect();
        if bodies.is_empty() {
            missing.push(format!(
                "{} {}: no successful call recorded",
                read.route, read.at
            ));
            continue;
        }
        match by_type
            .iter_mut()
            .find(|(fields, _, _)| std::ptr::eq(*fields, read.fields))
        {
            Some((_, all, _)) => all.extend(bodies),
            None => by_type.push((read.fields, bodies, format!("{} {}", read.route, read.at))),
        }
    }
    for (fields, bodies, first) in by_type {
        for field in fields {
            if !bodies.iter().any(|b| b.get(*field).is_some()) {
                missing.push(format!("{first} {field}"));
            }
        }
    }
    let lines: Vec<&Value> = exchanges
        .iter()
        .filter(ok)
        .filter(|e| {
            e["request"]["path"]
                .as_str()
                .is_some_and(|p| p.ends_with("/cmd"))
        })
        .flat_map(|e| {
            e["response"]["body"]["ndjson"]
                .as_array()
                .into_iter()
                .flatten()
        })
        .collect();
    for field in fields_of::<CommandBody>() {
        if !lines
            .iter()
            .any(|l| l.pointer(&format!("/command/{field}")).is_some())
        {
            missing.push(format!("command stream: command/{field}"));
        }
    }
    for field in fields_of::<LogLine>() {
        if !lines.iter().any(|l| l.get(*field).is_some()) {
            missing.push(format!("command stream: {field}"));
        }
    }
    assert!(
        missing.is_empty(),
        "capture the fixtures again; not recorded:\n{}",
        missing.join("\n")
    );
}

#[test]
fn the_recording_holds_no_token_and_no_real_scope() {
    let fixture = fixture();
    assert_eq!(fixture["token"], FIXTURE_TOKEN);
    // Vercel access tokens are long runs of letters, digits and `_`; the
    // only long runs the fixture may hold are zeroed digests.
    for word in FIXTURE.split(|c: char| !c.is_ascii_alphanumeric() && c != '_') {
        let token_shaped = word.len() >= 40 && word.bytes().any(|b| b != b'0');
        assert!(
            !token_shaped,
            "a token-shaped string in the fixture: {word}"
        );
    }
    for id in FIXTURE
        .split('"')
        .filter(|s| s.starts_with("team_") || s.starts_with("prj_"))
    {
        assert!(
            id.contains("fixture") || id.bytes().skip(4).all(|b| b == b'0' || b == b'_'),
            "a real team or project id in the fixture: {id}"
        );
    }
}

/// A fake that lets a session run past the plan's cap (the kind of drift
/// this test exists for) fails the contract.
#[test]
fn a_fake_without_the_session_cap_fails_the_contract() {
    let mut api = VercelApi::default();
    api.max_session_ms = u64::MAX / 2;
    let found = differences(&mut api);
    assert!(
        found
            .iter()
            .any(|d| d.starts_with("extend-timeout-past-the-cap: status 200")),
        "{found:?}"
    );
}
