//! The shared fake Northflank ([`super::fake_api`]) held to the recording
//! of the real API in `fixtures/northflank.json`. Each recorded call is
//! replayed in order on the fake, which must answer with the same status,
//! error message and every field intentd reads; each recorded command
//! stream must match the fake's for the same `init`. The recording must
//! also hold every field the adapter's response types read, so a new read
//! fails here until the fixtures are captured again
//! (`scripts/capture-northflank-fixtures.sh`).

use serde_json::{json, Value};

use super::client::error_message;
use super::fake_api::{
    exec_frames, message, refused_frame, NorthflankApi, Refusal, FIXTURE_BACKEND_TAG,
    FIXTURE_MISSING_SERVICE, FIXTURE_PROJECT, FIXTURE_SERVICE,
};
use super::fields::Read;

const FIXTURE: &str = include_str!("fixtures/northflank.json");

/// Fields whose value changes from run to run: compared by JSON type.
const VOLATILE: &[&str] = &["/createdAt", "/cursor"];

/// Fields intentd reads that no recording holds, and why that is fine.
const UNCOVERED: &[(&str, &str, &str)] = &[
    (
        "GET /projects/*/services/*",
        "/servicePaused",
        "Northflank shows a pause as deployment.instances 0; intentd reads the flag only as a fallback",
    ),
    (
        "GET /projects/*/services",
        "/cursor",
        "one page in the recording; the paged GET /tags holds the same pagination",
    ),
    (
        "GET /projects/*/volumes",
        "/cursor",
        "one page in the recording; the paged GET /tags holds the same pagination",
    ),
];

/// What intentd must not get back, though it does not read it: a fake
/// that echoed annotations hid intentd#3443.
fn behaviour() -> Vec<Read> {
    vec![Read::pointers(
        "GET /projects/*/services/*",
        "",
        &[
            "/deployment/metadata/annotations",
            "/deployment/metadata/labels",
        ],
    )]
}

fn fixture() -> Value {
    serde_json::from_str(FIXTURE).expect("the fixture is JSON")
}

fn reads() -> Vec<Read> {
    let mut reads = super::client::reads();
    reads.extend(super::provider::reads());
    reads
}

fn route_matches(route: &str, method: &str, target: &str) -> bool {
    let path = target.split('?').next().unwrap_or_default();
    let Some((want_method, want_path)) = route.split_once(' ') else {
        return false;
    };
    let want: Vec<&str> = want_path.trim_matches('/').split('/').collect();
    let got: Vec<&str> = path.trim_matches('/').split('/').collect();
    want_method == method
        && want.len() == got.len()
        && want.iter().zip(&got).all(|(w, g)| *w == "*" || w == g)
}

/// The values at `at`, walking every element of an array at each `*`.
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

fn read_items<'a>(read: &Read, body: &'a Value) -> Vec<&'a Value> {
    if read.envelope {
        items(body, read.at)
    } else {
        body.get("data")
            .map(|d| items(d, read.at))
            .unwrap_or_default()
    }
}

struct Replayed {
    name: String,
    method: String,
    path: String,
    recorded_status: u16,
    recorded: Value,
    fake_status: u16,
    fake: Value,
    /// The recording kept only the list items whose `field` is one of
    /// `values` (a team's other tags and services stay private).
    kept: Option<(String, Vec<Value>)>,
}

/// Every recorded call, answered again by a fake that starts with the
/// team tags the recording found.
fn replay() -> Vec<Replayed> {
    let fixture = fixture();
    let exchanges = fixture["exchanges"].as_array().expect("exchanges");
    let region = exchanges
        .iter()
        .find(|e| e["name"] == "get-project")
        .and_then(|e| e.pointer("/response/body/data/deployment/region"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let mut api = NorthflankApi::new(FIXTURE_PROJECT, region);
    for exchange in exchanges.iter().filter(|e| e["name"] == "list-tags") {
        for tag in items(&exchange["response"]["body"], "/data/tags/*") {
            let Some(name) = tag["name"].as_str() else {
                continue;
            };
            if name != FIXTURE_BACKEND_TAG && !api.tags.iter().any(|t| t == name) {
                api.tags.push(name.to_owned());
            }
        }
    }
    exchanges
        .iter()
        .map(|exchange| {
            let request = &exchange["request"];
            let method = request["method"].as_str().unwrap_or_default().to_owned();
            let path = request["path"].as_str().unwrap_or_default().to_owned();
            let reply = api.handle(&method, &path, request.get("body"));
            Replayed {
                name: exchange["name"].as_str().unwrap_or_default().to_owned(),
                method,
                path,
                recorded_status: u16::try_from(
                    exchange["response"]["status"].as_u64().unwrap_or(0),
                )
                .unwrap_or(0),
                recorded: exchange["response"]["body"].clone(),
                fake_status: reply.status,
                fake: reply.body,
                kept: exchange.get("kept").map(|k| {
                    let field = k["field"].as_str().unwrap_or("id").to_owned();
                    (field, k["values"].as_array().cloned().unwrap_or_default())
                }),
            }
        })
        .collect()
}

fn same_kind(a: &Value, b: &Value) -> bool {
    std::mem::discriminant(a) == std::mem::discriminant(b)
}

/// An object, or an array holding one: a serde struct's nested items,
/// which their own [`Read`] compares. An array of plain values (tags) is
/// compared whole.
fn nested(value: &Value) -> bool {
    match value {
        Value::Object(_) => true,
        Value::Array(items) => items.iter().any(|i| i.is_object() || i.is_array()),
        _ => false,
    }
}

/// An RFC 3339 time, as intentd reads `createdAt` (`2026-01-01T00:00:00Z`).
fn looks_like_a_time(text: &str) -> bool {
    let b = text.as_bytes();
    b.len() >= 20
        && b[..4].iter().all(u8::is_ascii_digit)
        && b[4] == b'-'
        && b[7] == b'-'
        && b[10] == b'T'
        && b[13] == b':'
        && (text.ends_with('Z') || text.contains('+'))
}

/// Equal JSON, numbers by value (`2` is `2.0`).
fn same(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(a), Value::Number(b)) => a.as_f64() == b.as_f64(),
        (Value::Array(a), Value::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(a, b)| same(a, b))
        }
        (Value::Object(a), Value::Object(b)) => {
            a.len() == b.len() && a.iter().all(|(k, v)| b.get(k).is_some_and(|w| same(v, w)))
        }
        (a, b) => a == b,
    }
}

fn assert_none(problems: &[String]) {
    assert!(
        problems.is_empty(),
        "the fake Northflank differs from the recording of the real API \
         (fix the fake in fake_api.rs, or capture again with \
         scripts/capture-northflank-fixtures.sh if Northflank changed):\n  {}",
        problems.join("\n  ")
    );
}

#[test]
fn the_fake_answers_every_recorded_call_with_its_status_and_error() {
    assert_none(&status_problems(&replay()));
}

fn status_problems(calls: &[Replayed]) -> Vec<String> {
    let mut problems = Vec::new();
    for call in calls {
        if call.fake_status != call.recorded_status {
            problems.push(format!(
                "{} ({} {}): HTTP {} from the fake, {} recorded: {}",
                call.name,
                call.method,
                call.path,
                call.fake_status,
                call.recorded_status,
                call.fake
            ));
            continue;
        }
        if !(200..300).contains(&call.recorded_status) {
            let fake = error_message(&call.fake);
            let recorded = error_message(&call.recorded);
            if fake != recorded {
                problems.push(format!(
                    "{}: error {fake:?} from the fake, {recorded:?} recorded",
                    call.name
                ));
            }
        }
    }
    problems
}

#[test]
fn the_fake_returns_every_field_intentd_reads_as_recorded() {
    assert_none(&field_problems(&replay()));
}

fn field_problems(calls: &[Replayed]) -> Vec<String> {
    let mut reads = reads();
    reads.extend(behaviour());
    let mut problems = Vec::new();
    for call in calls {
        let ok =
            (200..300).contains(&call.recorded_status) && call.fake_status == call.recorded_status;
        for read in reads
            .iter()
            .filter(|r| ok && route_matches(r.route, &call.method, &call.path))
        {
            let recorded = read_items(read, &call.recorded);
            let fake = read_items(read, &call.fake);
            // Filter the fake's list as the recording was, then pair items
            // by id (by position when they have none): every recorded item
            // must be returned, and nothing more.
            let fake: Vec<&Value> = match &call.kept {
                Some((field, values)) if read.at.ends_with('*') => fake
                    .into_iter()
                    .filter(|f| f.get(field).is_some_and(|v| values.contains(v)))
                    .collect(),
                _ => fake,
            };
            if recorded.len() != fake.len() {
                problems.push(format!(
                    "{} {}: {} items from the fake, {} recorded",
                    call.name,
                    read.at,
                    fake.len(),
                    recorded.len()
                ));
                continue;
            }
            let by_id = recorded.iter().all(|r| r.get("id").is_some());
            let pairs: Vec<(&Value, Option<&Value>)> = if by_id {
                recorded
                    .iter()
                    .map(|r| {
                        (
                            *r,
                            fake.iter().copied().find(|f| f.get("id") == r.get("id")),
                        )
                    })
                    .collect()
            } else {
                recorded
                    .iter()
                    .copied()
                    .zip(fake.iter().copied().map(Some))
                    .collect()
            };
            for (i, (recorded, fake)) in pairs.into_iter().enumerate() {
                let Some(fake) = fake else {
                    problems.push(format!(
                        "{} {}[{i}]: the fake does not return {}",
                        call.name, read.at, recorded["id"]
                    ));
                    continue;
                };
                for field in &read.fields {
                    let (r, f) = (recorded.pointer(field), fake.pointer(field));
                    let matches = match (r, f) {
                        (Some(r), Some(f)) if VOLATILE.contains(&field.as_str()) => {
                            same_kind(r, f)
                                && (field != "/createdAt"
                                    || f.as_str().is_none_or(looks_like_a_time))
                        }
                        (Some(r), Some(f)) if !read.exact && nested(r) => same_kind(r, f),
                        (Some(r), Some(f)) => same(r, f),
                        (None, None) => true,
                        _ => false,
                    };
                    if !matches {
                        problems.push(format!(
                            "{} {}[{i}]{field}: {} from the fake, {} recorded",
                            call.name,
                            read.at,
                            f.map_or("nothing".into(), Value::to_string),
                            r.map_or("nothing".into(), Value::to_string),
                        ));
                    }
                }
            }
        }
    }
    problems
}

#[test]
fn the_recording_holds_every_field_intentd_reads() {
    let fixture = fixture();
    let exchanges = fixture["exchanges"].as_array().expect("exchanges");
    let mut problems = Vec::new();
    for read in reads() {
        let bodies: Vec<&Value> = exchanges
            .iter()
            .filter(|e| {
                let status = e["response"]["status"].as_u64().unwrap_or(0);
                (200..300).contains(&status)
                    && route_matches(
                        read.route,
                        e["request"]["method"].as_str().unwrap_or_default(),
                        e["request"]["path"].as_str().unwrap_or_default(),
                    )
            })
            .map(|e| &e["response"]["body"])
            .collect();
        if bodies.is_empty() {
            problems.push(format!("no recorded answer to {}", read.route));
            continue;
        }
        let found: Vec<&Value> = bodies.iter().flat_map(|b| read_items(&read, b)).collect();
        for field in &read.fields {
            let held = found.iter().any(|item| item.pointer(field).is_some());
            let excused = UNCOVERED
                .iter()
                .any(|(route, f, _)| *route == read.route && f == field);
            if !held && !excused {
                problems.push(format!(
                    "{} {}{field}: intentd reads it, and no recorded answer has it",
                    read.route, read.at
                ));
            }
        }
    }
    assert_none(&problems);
}

/// The frame types in order, a run of output frames counted once as
/// `output`: Northflank splits output as it comes, and stdout and stderr
/// race each other.
fn kinds(frames: &[Value]) -> Vec<String> {
    let mut kinds: Vec<String> = Vec::new();
    for frame in frames {
        let kind = match frame["type"].as_str() {
            Some("stdOut" | "stdErr") => "output",
            Some(other) => other,
            None => "?",
        };
        if !(kind == "output" && kinds.last().is_some_and(|k| k == "output")) {
            kinds.push(kind.to_owned());
        }
    }
    kinds
}

/// Each output channel's text, and the `completion` frame's data.
fn stream(frames: &[Value]) -> (String, String, Option<Value>, Option<Value>) {
    let mut out = String::new();
    let mut err = String::new();
    let mut init = None;
    let mut completion = None;
    for frame in frames {
        let data = &frame["data"];
        match frame["type"].as_str() {
            Some("stdOut") => out.push_str(data.as_str().unwrap_or_default()),
            Some("stdErr") => err.push_str(data.as_str().unwrap_or_default()),
            Some("init") => init = Some(data.clone()),
            Some("completion") if completion.is_none() => completion = Some(data.clone()),
            _ => {}
        }
    }
    (out, err, init, completion)
}

/// How a fake streams one command that runs: (`init` context, stdout,
/// stderr, exit) to the frames after the client's `init`.
type Render = dyn Fn(&Value, &str, &str, i32) -> Vec<Value>;

/// The refusal a recorded command without a program stands for.
fn refusal(session: &str) -> Refusal<'static> {
    match session {
        "exec-bad-token" => Refusal::BadToken,
        "exec-unknown-service" => Refusal::UnknownService(FIXTURE_MISSING_SERVICE),
        other => panic!("the recorded command {other} has no program and no known refusal"),
    }
}

#[test]
fn the_fake_streams_each_recorded_command_as_northflank_did() {
    let render = |context: &Value, out: &str, err: &str, code: i32| {
        exec_frames(FIXTURE_SERVICE, context, out, err, code)
    };
    assert_none(&stream_problems(&render));
}

fn stream_problems(render: &Render) -> Vec<String> {
    let fixture = fixture();
    let sessions = fixture["exec"].as_array().expect("exec sessions");
    assert!(!sessions.is_empty(), "the recording has no command stream");
    let completion_fields = super::exec::completion_fields();
    let mut problems = Vec::new();
    let mut held: Vec<&str> = Vec::new();
    for session in sessions {
        let name = session["name"].as_str().unwrap_or_default();
        let program = &session["program"];
        let fake = if program.is_null() {
            vec![refused_frame(refusal(name))]
        } else {
            render(
                &session["context"],
                program["stdout"].as_str().unwrap_or_default(),
                program["stderr"].as_str().unwrap_or_default(),
                i32::try_from(program["exitCode"].as_i64().unwrap_or(-1)).unwrap_or(-1),
            )
        };
        let recorded: Vec<Value> = session["frames"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|f| f.get("frame").cloned())
            .collect();
        for frame in session["frames"].as_array().into_iter().flatten() {
            let Some(opcode) = frame["opcode"].as_str() else {
                continue;
            };
            let fake = if message(&frame["frame"]).is_binary() {
                "binary"
            } else {
                "text"
            };
            if opcode != fake {
                problems.push(format!(
                    "{name}: the fake sends {} frames as {fake} messages, Northflank sent {opcode}",
                    frame["frame"]["type"]
                ));
            }
        }
        let (kinds_recorded, kinds_fake) = (kinds(&recorded), kinds(&fake));
        if kinds_recorded != kinds_fake {
            problems.push(format!(
                "{name}: the fake sends frames {kinds_fake:?}, Northflank sent {kinds_recorded:?}"
            ));
        }
        let (r_out, r_err, r_init, r_done) = stream(&recorded);
        let (f_out, f_err, f_init, f_done) = stream(&fake);
        if (r_out.as_str(), r_err.as_str()) != (f_out.as_str(), f_err.as_str()) {
            problems.push(format!(
                "{name}: the fake streams stdout {f_out:?} and stderr {f_err:?}, Northflank streamed {r_out:?} and {r_err:?}"
            ));
        }
        // intentd reads `auth`, and a refusal's `message` to tell a bad
        // token from a missing permission.
        let auth = |init: &Option<Value>| {
            init.as_ref()
                .map(|d| (d.get("auth").cloned(), d.get("message").cloned()))
        };
        if auth(&r_init) != auth(&f_init) {
            problems.push(format!(
                "{name}: init {f_init:?} from the fake, {r_init:?} recorded"
            ));
        }
        let (r_done, f_done) = match (&r_done, &f_done) {
            (Some(r), Some(f)) => (r, f),
            (None, None) => continue,
            _ => {
                problems.push(format!(
                    "{name}: completion {f_done:?} from the fake, {r_done:?} recorded"
                ));
                continue;
            }
        };
        for field in completion_fields {
            let (r, f) = (r_done.get(*field), f_done.get(*field));
            if r.is_some() {
                held.push(field);
            }
            if r != f {
                problems.push(format!(
                    "{name}: completion {field} {f:?} from the fake, {r:?} recorded"
                ));
            }
        }
    }
    for field in completion_fields {
        let excused = UNCOVERED.iter().any(|(route, f, _)| {
            *route == "exec completion" && f.trim_start_matches('/') == *field
        });
        if !held.contains(field) && !excused {
            problems.push(format!(
                "completion {field}: intentd reads it, and no recorded stream has it"
            ));
        }
    }
    problems
}

// The fakes as they were before each bug a live run found. The contract
// must fail on each, or it would not have caught the bug.

/// intent#9469: the old fakes sent stderr on its own channel whatever the
/// `init` asked, so the home-ready marker the setup prints to stderr was
/// found in tests and never on Northflank, which runs a command in a
/// terminal unless the `init` sets `"tty": false`.
#[test]
fn a_fake_that_ignores_the_terminal_fails_the_contract() {
    let terminal_blind = |_: &Value, out: &str, err: &str, code: i32| {
        let status = if code == 0 { "Success" } else { "Failure" };
        vec![
            json!({ "type": "init", "data": { "auth": "successful" } }),
            json!({ "type": "stdOut", "data": out }),
            json!({ "type": "stdErr", "data": err }),
            json!({ "type": "completion", "data": { "exitCode": code, "status": status } }),
        ]
    };
    let problems = stream_problems(&terminal_blind);
    assert!(
        problems.iter().any(|p| p.starts_with(
            "exec-tty-default: the fake streams stdout \"out\\n\" and stderr \"err\\n\""
        )),
        "{problems:#?}"
    );
}

/// intentd#3443: the old fakes echoed a service's custom annotations, so
/// tags kept there read back in tests and never on Northflank, which drops
/// them (and lists by tag found no machine).
#[test]
fn a_fake_that_echoes_annotations_fails_the_contract() {
    let mut calls = replay();
    let sent = fixture()["exchanges"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|e| e["name"] == "create-service-with-annotations")
        .and_then(|e| {
            e.pointer("/request/body/deployment/metadata/annotations")
                .cloned()
        })
        .expect("the recording creates a service with annotations");
    for call in &mut calls {
        if route_matches("GET /projects/*/services/*", &call.method, &call.path) {
            call.fake["data"]["deployment"]["metadata"]["annotations"] = sent.clone();
        }
    }
    let problems = field_problems(&calls);
    assert!(
        problems
            .iter()
            .any(|p| p.starts_with("get-service-running [0]/deployment/metadata/annotations")),
        "{problems:#?}"
    );
}
