//! The shared fake Agent37 ([`super::fake_api`]) held to the recording of
//! the real API in `fixtures/agent37.json`. Each recorded call is replayed
//! in order on a fresh fake, which must answer with the same status, the
//! same error code in the same envelope, and every field the adapter reads
//! with the recorded value. The recording must also hold every field the
//! adapter's response types read, so a new read fails here until the
//! fixtures are captured again (`scripts/capture-agent37-fixtures.sh`).

use serde_json::{json, Value};

use super::capture::{TIME_KEYS, WRONG_KEY};
use super::client::{Ack, ExecBody, InstanceBody, TemplateBody};
use super::fake_api::{exec_reply, Agent37Api, Reply, FIXTURE_KEY};
use super::fields::fields_of;

const FIXTURE: &str = include_str!("fixtures/agent37.json");

/// What the adapter reads of one route's success body.
struct Read {
    /// `METHOD /path` (`*` for one part), and `exec` for the sandbox host.
    route: &'static str,
    /// JSON pointer to the items; `/data/*` walks the list.
    at: &'static str,
    /// Fields read through serde: present, with the same JSON type.
    fields: &'static [&'static str],
    /// Fields compared by value too.
    exact: &'static [&'static str],
}

const INSTANCE_EXACT: &[&str] = &[
    "id",
    "status",
    "template",
    "image_ref",
    "type",
    "region",
    "mount_path",
    "resources",
    "metadata",
    "auto_sleep",
    "idle_timeout_seconds",
    "status_reason",
];

fn reads() -> Vec<Read> {
    let instance = fields_of::<InstanceBody>();
    let template = fields_of::<TemplateBody>();
    let ack = fields_of::<Ack>();
    let exec = fields_of::<ExecBody>();
    vec![
        Read {
            route: "POST /instances",
            at: "",
            fields: instance,
            exact: INSTANCE_EXACT,
        },
        Read {
            route: "GET /instances/*",
            at: "",
            fields: instance,
            exact: INSTANCE_EXACT,
        },
        Read {
            route: "GET /instances",
            at: "/data/*",
            fields: instance,
            exact: INSTANCE_EXACT,
        },
        Read {
            route: "GET /templates/*",
            at: "",
            fields: template,
            exact: &["name", "image_ref", "revision"],
        },
        Read {
            route: "POST /templates",
            at: "",
            fields: template,
            exact: &["name", "image_ref", "revision"],
        },
        Read {
            route: "PATCH /templates/*",
            at: "",
            fields: template,
            exact: &["name", "image_ref", "revision"],
        },
        Read {
            route: "POST /instances/*/sleep",
            at: "",
            fields: ack,
            exact: &["id", "status"],
        },
        Read {
            route: "POST /instances/*/start",
            at: "",
            fields: ack,
            exact: &["id", "status"],
        },
        Read {
            route: "DELETE /instances/*",
            at: "",
            fields: &[],
            exact: &["id", "deleted"],
        },
        Read {
            route: "DELETE /templates/*",
            at: "",
            fields: &[],
            exact: &["name", "deleted"],
        },
        Read {
            route: "exec",
            at: "",
            fields: exec,
            exact: &[],
        },
    ]
}

fn fixture() -> Value {
    serde_json::from_str(FIXTURE).expect("the fixture is JSON")
}

fn route_matches(route: &str, method: &str, path: &str) -> bool {
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

/// `(flat, code)` of an error body: flat is the sandbox host's
/// `{"error": "<code>"}`.
fn error_code(body: &Value) -> Option<(bool, String)> {
    match body.get("error")? {
        Value::String(code) => Some((true, code.clone())),
        error => Some((false, error.get("code")?.as_str()?.to_owned())),
    }
}

struct Replayed {
    name: String,
    exec: bool,
    method: String,
    path: String,
    recorded_status: u16,
    recorded: Value,
    fake: Reply,
}

/// Every recorded call, answered again by a fresh fake.
fn replay(api: &mut Agent37Api) -> Vec<Replayed> {
    let fixture = fixture();
    let image = fixture["image"].as_str().unwrap_or_default().to_owned();
    api.pullable.push(image);
    fixture["exchanges"]
        .as_array()
        .expect("exchanges")
        .iter()
        .map(|exchange| {
            let request = &exchange["request"];
            let exec = request["plane"] == "exec";
            let key = if request["auth"] == "key" {
                FIXTURE_KEY
            } else {
                WRONG_KEY
            };
            let method = request["method"].as_str().unwrap_or_default().to_owned();
            let path = request["path"].as_str().unwrap_or_default().to_owned();
            let fake = if exec {
                let id = path.split('/').nth(2).unwrap_or_default();
                match api.exec(id, Some(key), request.get("body").unwrap_or(&Value::Null)) {
                    Ok(_) => exec_reply(0, "", ""),
                    Err(reply) => reply,
                }
            } else {
                api.handle(&method, &path, Some(key), request.get("body"))
            };
            Replayed {
                name: exchange["name"].as_str().unwrap_or_default().to_owned(),
                exec,
                method,
                path,
                recorded_status: u16::try_from(
                    exchange["response"]["status"].as_u64().unwrap_or(0),
                )
                .unwrap_or(0),
                recorded: exchange["response"]["body"].clone(),
                fake,
            }
        })
        .collect()
}

fn same_kind(a: &Value, b: &Value) -> bool {
    std::mem::discriminant(a) == std::mem::discriminant(b)
}

/// Every way the fake differs from the recording.
fn differences(api: &mut Agent37Api) -> Vec<String> {
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
            continue;
        }
        let route_reads = reads.iter().filter(|r| {
            if call.exec {
                r.route == "exec"
            } else {
                route_matches(r.route, &call.method, &call.path)
            }
        });
        for read in route_reads {
            let recorded = items(&call.recorded, read.at);
            let fake = items(&call.fake.body, read.at);
            if recorded.len() != fake.len() {
                found.push(format!(
                    "{what}: {} items from the fake, {} recorded",
                    fake.len(),
                    recorded.len()
                ));
                continue;
            }
            for (i, (recorded, fake)) in recorded.iter().zip(&fake).enumerate() {
                for field in read.fields.iter().chain(read.exact) {
                    let (Some(want), got) = (recorded.get(*field), fake.get(*field)) else {
                        continue;
                    };
                    // The exec output is the command's, not the fake's.
                    if call.exec {
                        continue;
                    }
                    let Some(got) = got else {
                        found.push(format!("{what} [{i}]/{field}: missing from the fake"));
                        continue;
                    };
                    let by_value = read.exact.contains(field) && !TIME_KEYS.contains(field);
                    if (by_value && want != got) || !same_kind(want, got) {
                        found.push(format!(
                            "{what} [{i}]/{field}: {got} from the fake, {want} recorded"
                        ));
                    }
                }
            }
        }
    }
    found
}

#[test]
fn the_fake_answers_every_recorded_call_as_agent37_did() {
    let found = differences(&mut Agent37Api::new(FIXTURE_KEY));
    assert!(
        found.is_empty(),
        "the fake Agent37 differs from the recording:\n{}",
        found.join("\n")
    );
}

#[test]
fn the_recording_holds_every_field_the_adapter_reads() {
    let fixture = fixture();
    let exchanges = fixture["exchanges"].as_array().expect("exchanges");
    let mut missing = Vec::new();
    for read in reads() {
        let bodies: Vec<&Value> = exchanges
            .iter()
            .filter(|e| {
                let request = &e["request"];
                let status = e["response"]["status"].as_u64().unwrap_or(0);
                (200..300).contains(&status)
                    && if read.route == "exec" {
                        request["plane"] == "exec"
                    } else {
                        request["plane"] == "rest"
                            && route_matches(
                                read.route,
                                request["method"].as_str().unwrap_or_default(),
                                request["path"].as_str().unwrap_or_default(),
                            )
                    }
            })
            .flat_map(|e| items(&e["response"]["body"], read.at))
            .collect();
        if bodies.is_empty() {
            missing.push(format!("{}: no successful call recorded", read.route));
            continue;
        }
        for field in read.fields.iter().chain(read.exact) {
            if !bodies.iter().any(|b| b.get(*field).is_some()) {
                missing.push(format!("{} {field}", read.route));
            }
        }
    }
    assert!(
        missing.is_empty(),
        "capture the fixtures again; not recorded:\n{}",
        missing.join("\n")
    );
}

#[test]
fn the_recording_holds_no_key() {
    // A live Agent37 key starts with sk_live_; the fakes use other keys.
    assert!(!FIXTURE.contains("sk_live_"), "a live key in the fixture");
}

/// A fake that ignores the type-and-region rule (the kind of drift this
/// test exists for) fails the contract.
#[test]
fn a_fake_that_starts_default_instances_in_eu_fails_the_contract() {
    let mut api = Agent37Api::new(FIXTURE_KEY);
    let found = {
        let mut drifted = DriftedApi { inner: &mut api };
        drifted.differences()
    };
    assert!(
        found
            .iter()
            .any(|d| d.starts_with("create-default-in-eu: status 201")),
        "{found:?}"
    );
}

struct DriftedApi<'a> {
    inner: &'a mut Agent37Api,
}

impl DriftedApi<'_> {
    fn differences(&mut self) -> Vec<String> {
        let fixture = fixture();
        let image = fixture["image"].as_str().unwrap_or_default().to_owned();
        self.inner.pullable.push(image);
        let mut found = Vec::new();
        for exchange in fixture["exchanges"].as_array().expect("exchanges") {
            let request = &exchange["request"];
            if request["plane"] == "exec" {
                continue;
            }
            let mut body = request.get("body").cloned();
            // The drift: a default create in eu is sent as if it were in us.
            if exchange["name"] == "create-default-in-eu" {
                if let Some(b) = body.as_mut() {
                    b["region"] = json!("us");
                }
            }
            let key = if request["auth"] == "key" {
                FIXTURE_KEY
            } else {
                WRONG_KEY
            };
            let reply = self.inner.handle(
                request["method"].as_str().unwrap_or_default(),
                request["path"].as_str().unwrap_or_default(),
                Some(key),
                body.as_ref(),
            );
            let recorded = exchange["response"]["status"].as_u64().unwrap_or(0);
            if u64::from(reply.status) != recorded {
                found.push(format!(
                    "{}: status {} from the fake, {recorded} recorded",
                    exchange["name"].as_str().unwrap_or_default(),
                    reply.status
                ));
            }
        }
        found
    }
}
