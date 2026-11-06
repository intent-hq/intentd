//! Payload-budget sweep: every `BoundedRead` method of the catalog, in every
//! parameter variation, plus every seq-0 subscription snapshot, is driven
//! through a real UDS connection against the large daemon-state fixture
//! (`intent_services::test_support::large_state`), and no reply may exceed
//! [`READ_PAYLOAD_BUDGET_BYTES`] (500 KiB, measured as the serialized
//! JSON-RPC frame on the wire).
//!
//! Known offenders are listed with their measured size in
//! `baseline.txt` (ci preset) / `baseline-full.txt` (full preset). The sweep
//! fails on a new offender, on an offender growing more than
//! [`GROWTH_TOLERANCE_PERCENT`] above its recorded size, and on a stale
//! entry (a listed variant that now fits). Fixing an offender means
//! deleting its line; the list only ratchets down.
//!
//! Lives in intent-app because the reads need the feature crates
//! registered by [`intent_app::compose`].
//!
//! Run: `make test-select E='test(/payload_budget/)'`. The full-scale
//! sweep is `#[ignore]`d and runs with `INTENTD_LARGE_STATE_SCALE=full`
//! (`--run-ignored only`). `PAYLOAD_BUDGET_UPDATE=1` rewrites the baseline
//! from the measured offenders instead of comparing.

// The sweep speaks JSON-RPC over a Unix-domain socket, like the UDS suites.
#![cfg(unix)]

mod bulk;
mod harness;
mod params_scan;
mod proxies;
mod rows;
mod seed;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use intent_services::test_support::large_state::LargeStateScale;
use intent_transport::catalog::{
    PayloadClass, PAYLOAD_CLASSES, READ_PAYLOAD_BUDGET_BYTES, SNAPSHOT_CHANNELS,
};
use serde_json::{json, Value};

use rows::{Params, Proof, Row, CANNOT_EXERCISE};

/// An offender may grow this much above its recorded size before the sweep
/// fails: debug-build serialization and store-minted ids wobble a little
/// between runs, real regressions are much larger.
const GROWTH_TOLERANCE_PERCENT: usize = 5;

fn bounded_methods() -> Vec<&'static str> {
    PAYLOAD_CLASSES
        .iter()
        .filter(|r| r.class == PayloadClass::BoundedRead)
        .map(|r| r.method)
        .collect()
}

/// A context with placeholder ids: enough for the static checks, which only
/// look at methods, variant names and param keys.
fn placeholder_ctx(seeded: &seed::Seeded) -> rows::Ctx<'_> {
    rows::Ctx {
        ws: "ws".into(),
        agent: "agent".into(),
        note: "note".into(),
        versioned_note: "note-v".into(),
        task_note: "task".into(),
        thread: "thread".into(),
        conversation: "conversation".into(),
        script: "script".into(),
        huge_script: "huge-script".into(),
        monitor: "monitor".into(),
        worktree: "/tmp".into(),
        specialist: "specialist".into(),
        message_id: "message".into(),
        big_block_message_id: "message".into(),
        tool_message_id: "message".into(),
        tool_block_id: "block".into(),
        member_principal: "member".into(),
        seeded,
    }
}

/// Drift between one sweep table and its catalog class: missing,
/// duplicate or stale rows, unnamed or duplicate variants, `Fixed` proofs
/// without a reason, and router params no variant sets.
fn table_problems(
    class: &str,
    table: &[Row],
    goldens: &[(&str, &str, &str)],
    universe: &[&'static str],
) -> Vec<String> {
    let mut problems = Vec::new();
    let universe_set: BTreeSet<&str> = universe.iter().copied().collect();
    let mut covered = BTreeSet::new();
    for row in table {
        if !covered.insert(row.method) {
            problems.push(format!("{}: listed twice", row.method));
        }
    }
    for (method, _, reason) in goldens {
        if reason.trim().is_empty() {
            problems.push(format!("{method}: cannot-exercise golden needs a reason"));
        }
        if !covered.insert(method) {
            problems.push(format!("{method}: both a row and a cannot-exercise golden"));
        }
    }
    for (method, _, _) in goldens {
        if derived_opaque(method)
            && params_scan::opaque_source(method, None) == params_scan::OpaqueSource::Unknown
        {
            problems.push(format!(
                "{method}: golden whose router arm passes its params on with no typed struct \
                 to derive them from"
            ));
        }
    }
    for m in universe_set.difference(&covered) {
        problems.push(format!(
            "{m}: {class} in PAYLOAD_CLASSES but has no sweep row — add one to \
             crates/intent-app/tests/payload_budget/rows.rs (variants for no params, the \
             largest limit, every optional param, and a `max` variant with every include \
             flag on), or a reasoned cannot-exercise golden"
        ));
    }
    for m in covered.difference(&universe_set) {
        problems.push(format!(
            "{m}: has a sweep row but is not {class} in PAYLOAD_CLASSES — drop the row"
        ));
    }

    let derived = params_scan::router_params(universe);
    for row in table {
        let mut names = BTreeSet::new();
        let mut set: BTreeSet<String> = BTreeSet::new();
        let mut has_fixed = false;
        for v in &row.variants {
            if !names.insert(v.name) {
                problems.push(format!("{} {}: duplicate variant name", row.method, v.name));
            }
            if v.name.contains(char::is_whitespace) {
                problems.push(format!(
                    "{} {:?}: variant names must not contain whitespace (baseline format)",
                    row.method, v.name
                ));
            }
            has_fixed |= matches!(v.proof, Proof::Fixed(_));
            set.extend(v.params.keys());
        }
        if has_fixed && row.small_by_design.is_none() {
            problems.push(format!(
                "{}: a variant proves with Fixed, so the row needs a small_by_design reason",
                row.method
            ));
        }
        if row.variants.is_empty() {
            problems.push(format!("{}: row has no variants", row.method));
        }
        if let Some(arm) = derived.get(row.method).filter(|a| a.opaque) {
            let mut needed: Vec<String> = Vec::new();
            match params_scan::opaque_source(row.method, row.parse) {
                params_scan::OpaqueSource::Struct { keys, .. } => needed.extend(keys),
                params_scan::OpaqueSource::ParseFn { keys, dynamic } => {
                    needed.extend(keys);
                    if dynamic && row.declared.is_empty() {
                        problems.push(format!(
                            "{}: its parse fn reads a key through a variable; name it in the \
                             row's `declared` keys with where it comes from",
                            row.method
                        ));
                    }
                }
                params_scan::OpaqueSource::Unknown => problems.push(format!(
                    "{}: the router arm passes its params object on and they cannot be \
                     derived — deserialize into a typed struct, or name the function that \
                     reads them with `parsed_by`",
                    row.method
                )),
            }
            needed.extend(row.declared.iter().map(|(k, _)| (*k).to_string()));
            needed.extend(arm.keys.iter().cloned());
            for key in needed {
                if !set.contains(&key) && !row.unexercised.iter().any(|(k, _)| *k == key) {
                    problems.push(format!(
                        "{}: the handler reads param `{key}` but no variant sets it — add it \
                         to a variant (or to `unexercised` with a reason)",
                        row.method
                    ));
                }
            }
        } else if let Some(arm) = derived.get(row.method) {
            for key in &arm.keys {
                let skipped = row.unexercised.iter().any(|(k, _)| k == key);
                if !set.contains(key) && !skipped {
                    problems.push(format!(
                        "{}: the router reads param `{key}` but no variant sets it — add it \
                         to a variant (or to `unexercised` with a reason)",
                        row.method
                    ));
                }
            }
        }
        for (key, _) in row.unexercised {
            if derived
                .get(row.method)
                .is_some_and(|arm| !arm.keys.contains(*key))
            {
                problems.push(format!(
                    "{}: `unexercised` names `{key}`, which the router no longer reads",
                    row.method
                ));
            }
        }
    }
    problems
}

fn derived_opaque(method: &str) -> bool {
    params_scan::router_params(&[method])
        .get(method)
        .is_some_and(|a| a.opaque)
}

fn class_methods(pick: fn(&PayloadClass) -> bool) -> Vec<&'static str> {
    PAYLOAD_CLASSES
        .iter()
        .filter(|r| pick(&r.class))
        .map(|r| r.method)
        .collect()
}

/// Static coverage: every `BoundedRead` and `ExternalProxy` method has a
/// sweep row (or a reasoned golden), every router param is exercised, and
/// every snapshot channel and bulk method is swept.
#[test]
fn payload_budget_rows_cover_the_catalog() {
    let seeded = seed::Seeded::default();
    let ctx = placeholder_ctx(&seeded);
    let mut problems = table_problems(
        "BoundedRead",
        &rows::rows(&ctx),
        CANNOT_EXERCISE,
        &bounded_methods(),
    );
    problems.extend(table_problems(
        "ExternalProxy",
        &rows::proxy_rows(),
        rows::PROXY_CANNOT_EXERCISE,
        &class_methods(|c| matches!(c, PayloadClass::ExternalProxy { .. })),
    ));

    let snapshot_rows = rows::snapshot_rows(&ctx);
    let mut swept: BTreeSet<&str> = snapshot_rows.iter().map(|r| r.method).collect();
    for (channel, reason) in rows::SNAPSHOT_CANNOT_EXERCISE {
        if reason.trim().is_empty() || !swept.insert(channel) {
            problems.push(format!(
                "{channel}: SNAPSHOT_CANNOT_EXERCISE needs a reason and must not also have a row"
            ));
        }
    }
    for (channel, _) in SNAPSHOT_CHANNELS {
        if !swept.contains(channel) {
            problems.push(format!(
                "{channel}: in SNAPSHOT_CHANNELS but has no snapshot_rows() entry"
            ));
        }
    }
    for channel in &swept {
        if !SNAPSHOT_CHANNELS.iter().any(|(c, _)| c == channel) {
            problems.push(format!(
                "{channel}: snapshot row for a channel not in SNAPSHOT_CHANNELS"
            ));
        }
    }
    for (method, reason) in rows::VOLATILE {
        if reason.trim().is_empty() || !bounded_methods().contains(method) {
            problems.push(format!(
                "{method}: VOLATILE entries need a reason and a BoundedRead method"
            ));
        }
    }
    for method in class_methods(|c| matches!(c, PayloadClass::BulkTransfer { .. })) {
        if !bulk::METHODS.contains(&method) {
            problems.push(format!(
                "{method}: BulkTransfer in PAYLOAD_CLASSES but bulk.rs does not check its cap"
            ));
        }
    }
    for method in bulk::METHODS {
        if !matches!(
            PAYLOAD_CLASSES
                .iter()
                .find(|r| r.method == *method)
                .map(|r| r.class),
            Some(PayloadClass::BulkTransfer { .. })
        ) {
            problems.push(format!(
                "{method}: bulk.rs checks a method that is not BulkTransfer"
            ));
        }
    }

    assert!(
        problems.is_empty(),
        "payload sweep table drift:\n  {}",
        problems.join("\n  ")
    );
}

/// The router-param derivation sees the params of a few well-known arms
/// (guards the scanner against silently matching nothing).
#[test]
fn payload_budget_param_scan_reads_router_arms() {
    let derived = params_scan::router_params(&[
        "agent.getConversation",
        "git.commits",
        "discussion.recipients",
        "script.list",
    ]);
    let keys = |m: &str| derived[m].keys.iter().cloned().collect::<Vec<_>>();
    for key in [
        "agentId",
        "limit",
        "workspaceId",
        "nextToken",
        "aroundMessageId",
        "aroundIndex",
        "projection",
        "includeInProgress",
    ] {
        assert!(
            keys("agent.getConversation").contains(&key.to_string()),
            "agent.getConversation: {key} missing from {:?}",
            keys("agent.getConversation")
        );
    }
    for key in ["page", "limit", "nextToken", "gitRootId", "workspaceId"] {
        assert!(
            keys("git.commits").contains(&key.to_string()),
            "git.commits: {key}"
        );
    }
    for key in ["query", "afterPrincipalId", "limit", "beforeId"] {
        assert!(
            keys("discussion.recipients").contains(&key.to_string()),
            "discussion.recipients: {key}"
        );
    }
    assert!(
        derived["script.list"].opaque,
        "script.list passes params through"
    );
}

/// One measured reply.
struct Measurement {
    method: String,
    variant: String,
    bytes: usize,
}

/// Outcome label of a reply: `ok` or the JSON-RPC error code.
fn outcome(reply: &harness::Reply) -> String {
    reply
        .error()
        .map_or_else(|| "ok".to_string(), |e| e["code"].to_string())
}

/// Check a proof against a result; `Err` says what was missing.
fn check_proof(proof: &Proof, result: &Value) -> Result<(), String> {
    match proof {
        Proof::Items(at, min) => {
            let n = result.pointer(at).and_then(Value::as_array).map(Vec::len);
            match n {
                Some(n) if n >= *min => Ok(()),
                Some(n) => Err(format!("{n} items at `{at}`, expected at least {min}")),
                None => Err(format!("no array at `{at}`")),
            }
        }
        Proof::Bytes(min) => {
            let n = result.to_string().len();
            if n >= *min {
                Ok(())
            } else {
                Err(format!("result is {n} bytes, expected at least {min}"))
            }
        }
        // Small by design is still not empty: an empty list or object
        // would pass on a bin the fixture never filled.
        Proof::Fixed(at) => match result.pointer(at) {
            None | Some(Value::Null) => Err(format!("nothing at `{at}`")),
            Some(Value::Array(a)) if a.is_empty() => Err(format!("empty array at `{at}`")),
            Some(Value::Object(o)) if o.is_empty() => Err(format!("empty object at `{at}`")),
            Some(_) => Ok(()),
        },
        Proof::Cut(at, min, flag) => {
            check_proof(&Proof::Items(at, *min), result)?;
            match result.pointer(flag) {
                Some(Value::Bool(true)) => Ok(()),
                other => Err(format!(
                    "`{flag}` is {other:?}, expected true (a bounded cut)"
                )),
            }
        }
        Proof::ByteCut(at, min, below, flag) => {
            check_proof(&Proof::Cut(at, *min, flag), result)?;
            let n = result
                .pointer(at)
                .and_then(Value::as_array)
                .map_or(0, Vec::len);
            if n < *below {
                Ok(())
            } else {
                Err(format!(
                    "{n} items at `{at}`, expected fewer than {below} (a byte cut)"
                ))
            }
        }
        Proof::RowsCut(at, min, flag) => {
            check_proof(&Proof::Items(at, *min), result)?;
            let rows = result.pointer(at).and_then(Value::as_array);
            if rows.is_some_and(|rows| {
                rows.iter()
                    .any(|row| row.pointer(flag) == Some(&Value::Bool(true)))
            }) {
                Ok(())
            } else {
                Err(format!(
                    "no row at `{at}` has `{flag}` true (a bounded cut)"
                ))
            }
        }
    }
}

fn set_path(params: &mut Value, path: &str, value: Value) {
    let mut parts = path.split('/');
    let head = parts.next().expect("param path");
    match parts.next() {
        Some(inner) => {
            params[head][inner] = value;
        }
        None => params[head] = value,
    }
}

/// Send one variant (following a cursor first when it is a next-page
/// variant) and return the measured reply.
async fn send_variant(
    client: &mut harness::Client,
    method: &str,
    params: &Params,
) -> Result<harness::Reply, String> {
    match params {
        Params::Static(p) => Ok(client.call(method, p).await),
        Params::NextPage {
            first,
            token_at,
            token_param,
        } => {
            let page = client.call(method, first).await;
            if let Some(e) = page.error() {
                return Err(format!("first page errored: {e}"));
            }
            // `/-/<pointer>` reads the cursor from the reply array's last
            // element (git diff reads end a cut page with `{ nextToken }`).
            let root = page.result();
            let found = match token_at.strip_prefix("/-") {
                Some(rest) => root
                    .as_array()
                    .and_then(|items| items.last())
                    .and_then(|last| last.pointer(rest)),
                None => root.pointer(token_at),
            };
            let token = found
                .filter(|t| !t.is_null())
                .cloned()
                .ok_or_else(|| format!("first page has no cursor at `{token_at}`"))?;
            let mut p = first.clone();
            set_path(&mut p, token_param, token);
            Ok(client.call(method, &p).await)
        }
    }
}

/// Probe mode (`PAYLOAD_BUDGET_PROBE=1`): print every outcome instead of
/// failing on proofs, for triage.
fn probe_mode() -> bool {
    std::env::var("PAYLOAD_BUDGET_PROBE").is_ok_and(|v| v == "1")
}

/// Connections the row sweep spreads its calls over. Each reply is
/// measured alone, so concurrency changes timing, never sizes.
const SWEEP_CONNECTIONS: usize = 6;

/// Sweep `table` over [`SWEEP_CONNECTIONS`] connections.
async fn sweep_rows(
    daemon: &harness::Daemon,
    table: &[Row],
    measured: &mut Vec<Measurement>,
    failures: &mut Vec<String>,
) {
    let mut tasks = tokio::task::JoinSet::new();
    for lane in 0..SWEEP_CONNECTIONS {
        let rows: Vec<Row> = table
            .iter()
            .skip(lane)
            .step_by(SWEEP_CONNECTIONS)
            .cloned()
            .collect();
        let socket = daemon.socket.clone();
        tasks.spawn(async move {
            let mut client = harness::Client::connect_socket(&socket).await;
            hello(&mut client).await;
            let mut measured = Vec::new();
            let mut failures = Vec::new();
            sweep_lane(&mut client, &rows, &mut measured, &mut failures).await;
            (measured, failures)
        });
    }
    while let Some(done) = tasks.join_next().await {
        let (m, f) = done.expect("sweep lane");
        measured.extend(m);
        failures.extend(f);
    }
}

/// Bind the fixture's draft client, so `drafts.get` reads its drafts, and
/// authorize the provider-setup surface.
async fn hello(client: &mut harness::Client) {
    let hello = client
        .call(
            "client.hello",
            // `antigravitySetup` authorizes the local-app provider setup
            // surface (`providers.setup.status`) on this connection.
            &json!({ "clientId": "large-state-client", "capabilities": { "antigravitySetup": 1 } }),
        )
        .await;
    assert!(hello.error().is_none(), "client.hello: {}", hello.frame);
}

async fn sweep_lane(
    client: &mut harness::Client,
    table: &[Row],
    measured: &mut Vec<Measurement>,
    failures: &mut Vec<String>,
) {
    for row in table {
        for variant in &row.variants {
            let started = std::time::Instant::now();
            let reply = match send_variant(client, row.method, &variant.params).await {
                Ok(r) => r,
                Err(e) => {
                    failures.push(format!("{} {}: {e}", row.method, variant.name));
                    continue;
                }
            };
            let proof = match reply.error() {
                Some(e) => Err(format!("error reply {e}")),
                None => check_proof(&variant.proof, reply.result()),
            };
            if probe_mode() {
                eprintln!(
                    "PROBE {:45} {:28} {:>9} {:>6}ms {}",
                    row.method,
                    variant.name,
                    reply.bytes,
                    started.elapsed().as_millis(),
                    proof.as_ref().err().map_or_else(
                        || "ok".to_string(),
                        |e| format!(
                            "{e} :: {}",
                            reply
                                .result()
                                .to_string()
                                .chars()
                                .take(160)
                                .collect::<String>()
                        )
                    )
                );
            }
            if let Err(e) = proof {
                failures.push(format!(
                    "{} {}: {e} — a variant must succeed and reach the large data",
                    row.method, variant.name
                ));
                continue;
            }
            measured.push(Measurement {
                method: row.method.into(),
                variant: variant.name.into(),
                bytes: reply.bytes,
            });
        }
    }
}

async fn sweep_goldens(
    client: &mut harness::Client,
    goldens: &[(&str, &str, &str)],
    ctx: &rows::Ctx<'_>,
    failures: &mut Vec<String>,
) {
    for (method, expected, _) in goldens {
        // The ids a reachable method would need, so a changed outcome means
        // the method itself changed, not that the params were too thin.
        let params = json!({
            "workspaceId": ctx.ws,
            "agentId": ctx.agent,
            "noteId": ctx.note,
            "principalId": ctx.member_principal,
        });
        let reply = client.call(method, &params).await;
        let got = outcome(&reply);
        if probe_mode() {
            eprintln!(
                "PROBE {method:45} {:28} {:>9} golden {got}",
                "golden", reply.bytes
            );
        }
        if got != *expected {
            failures.push(format!(
                "{method}: CANNOT_EXERCISE pins outcome {expected} but it now answers {got} — \
                 it may be reachable now: give it a sweep row, or update the golden"
            ));
        }
    }
}

/// Subscribe to each [`rows::SNAPSHOT_CANNOT_EXERCISE`] channel: the
/// handshake must succeed and the seq-0 snapshot arrive. Measured for the
/// report, never ratcheted.
async fn sweep_snapshot_goldens(
    daemon: &harness::Daemon,
    ctx: &rows::Ctx<'_>,
    measured: &mut Vec<Measurement>,
    failures: &mut Vec<String>,
) {
    for (channel, _) in rows::SNAPSHOT_CANNOT_EXERCISE {
        let mut client = harness::Client::connect(daemon).await;
        let ack = client
            .call(channel, &rows::snapshot_golden_params(ctx, channel))
            .await;
        if let Some(e) = ack.error() {
            failures.push(format!("{channel}: golden subscribe errored {e}"));
            continue;
        }
        let frame = loop {
            let f = client.read_frame().await;
            if f.frame["params"]["seq"] == json!(0) {
                break f;
            }
        };
        if probe_mode() {
            eprintln!(
                "PROBE {channel:45} {:28} {:>9} golden snapshot",
                "golden", frame.bytes
            );
        }
        measured.push(Measurement {
            method: (*channel).into(),
            variant: "golden".into(),
            bytes: frame.bytes,
        });
    }
}

/// Subscribe on a fresh connection and measure the seq-0 snapshot frame.
async fn sweep_snapshots(
    daemon: &harness::Daemon,
    table: &[Row],
    measured: &mut Vec<Measurement>,
    failures: &mut Vec<String>,
) {
    for row in table {
        for variant in &row.variants {
            let Params::Static(params) = &variant.params else {
                panic!("{}: snapshot variants are static", row.method);
            };
            let mut client = harness::Client::connect(daemon).await;
            let ack = client.call(row.method, params).await;
            if let Some(e) = ack.error() {
                failures.push(format!(
                    "{} {}: subscribe errored {e}",
                    row.method, variant.name
                ));
                continue;
            }
            let frame = loop {
                let f = client.read_frame().await;
                if f.frame["params"]["seq"] == json!(0) {
                    break f;
                }
            };
            let snapshot = &frame.frame["params"]["snapshot"];
            let proof = check_proof(&variant.proof, snapshot);
            if probe_mode() {
                eprintln!(
                    "PROBE {:45} {:28} {:>9} {}",
                    row.method,
                    variant.name,
                    frame.bytes,
                    proof.as_ref().err().map_or("ok", String::as_str)
                );
            }
            if let Err(e) = proof {
                failures.push(format!("{} {}: snapshot {e}", row.method, variant.name));
                continue;
            }
            measured.push(Measurement {
                method: row.method.into(),
                variant: variant.name.into(),
                bytes: frame.bytes,
            });
        }
    }
}

/// Parse a baseline file: `<method> <variant> <bytes>` per line, `#`
/// comments and blank lines ignored.
fn parse_baseline(text: &str) -> BTreeMap<(String, String), usize> {
    text.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(|l| {
            let parts: Vec<&str> = l.split_whitespace().collect();
            assert_eq!(
                parts.len(),
                3,
                "baseline line must be `<method> <variant> <bytes>`: {l}"
            );
            let bytes = parts[2]
                .parse()
                .unwrap_or_else(|_| panic!("baseline bytes: {l}"));
            ((parts[0].to_string(), parts[1].to_string()), bytes)
        })
        .collect()
}

/// Compare offenders to the baseline; the failures name what to do.
fn ratchet(
    offenders: &BTreeMap<(String, String), usize>,
    baseline: &BTreeMap<(String, String), usize>,
    file: &str,
) -> Vec<String> {
    let mut failures = Vec::new();
    for ((method, variant), bytes) in offenders {
        match baseline.get(&(method.clone(), variant.clone())) {
            None => failures.push(format!(
                "NEW OFFENDER {method} {variant}: {bytes} bytes > {READ_PAYLOAD_BUDGET_BYTES} \
                 budget — bound the reply (paginate, project, cap), do not add it to {file}"
            )),
            Some(recorded) if *bytes > recorded + recorded * GROWTH_TOLERANCE_PERCENT / 100 => {
                failures.push(format!(
                    "GROWTH {method} {variant}: {bytes} bytes, recorded {recorded} (+{}%, \
                     tolerance {GROWTH_TOLERANCE_PERCENT}%) — the offender got bigger",
                    (bytes - recorded) * 100 / recorded
                ));
            }
            Some(_) => {}
        }
    }
    for ((method, variant), recorded) in baseline {
        if !offenders.contains_key(&(method.clone(), variant.clone())) {
            failures.push(format!(
                "STALE {method} {variant} (recorded {recorded}): now within budget or no \
                 longer swept — delete its line from {file}"
            ));
        }
    }
    failures
}

fn report(measured: &[Measurement]) -> String {
    let mut sorted: Vec<&Measurement> = measured.iter().collect();
    sorted.sort_by(|a, b| b.bytes.cmp(&a.bytes).then(a.method.cmp(&b.method)));
    let mut out = String::new();
    for m in sorted {
        let flag = if rows::VOLATILE.iter().any(|(v, _)| *v == m.method) {
            "vol"
        } else if m.bytes > READ_PAYLOAD_BUDGET_BYTES {
            "OVER"
        } else {
            "ok"
        };
        let _ = writeln!(out, "{:>10} {flag:4} {} {}", m.bytes, m.method, m.variant);
    }
    out
}

async fn run_sweep(scale: &LargeStateScale, baseline_file: &str) {
    let started = std::time::Instant::now();
    let state = harness::fixture(scale).await;
    let fixture_built = started.elapsed();
    let mut seeded = seed::store(&state).await;
    let store_seeded = started.elapsed();
    let daemon = harness::serve(state).await;
    let mut client = harness::Client::connect(&daemon).await;
    hello(&mut client).await;
    let _devices = seed::runtime(
        &mut client,
        &daemon.services,
        &daemon.state,
        &daemon.socket,
        &mut seeded,
    )
    .await;
    let ctx = harness::ctx(&mut client, &daemon, &seeded).await;
    let setup = started.elapsed();
    eprintln!(
        "payload budget setup: fixture {fixture_built:?}, store seeds +{:?}, serve + runtime seeds +{:?}",
        store_seeded.saturating_sub(fixture_built),
        setup.saturating_sub(store_seeded)
    );

    let mut measured = Vec::new();
    let mut failures = Vec::new();
    seed::settle(&mut client, &daemon.state, &seeded).await;
    let mut table = rows::rows(&ctx);
    table.extend(rows::proxy_rows());
    // Rows that read the event log go first, alone: reads in the
    // concurrent lanes can append events, which would make their pages
    // depend on scheduling.
    let (event_rows, table): (Vec<Row>, Vec<Row>) = table
        .into_iter()
        .partition(|r| rows::EVENT_LOG_READS.contains(&r.method));
    sweep_lane(&mut client, &event_rows, &mut measured, &mut failures).await;
    sweep_rows(&daemon, &table, &mut measured, &mut failures).await;
    let rows_done = started.elapsed();
    failures.extend(account_directory_problems(&daemon.accounts));
    sweep_goldens(&mut client, CANNOT_EXERCISE, &ctx, &mut failures).await;
    sweep_goldens(
        &mut client,
        rows::PROXY_CANNOT_EXERCISE,
        &ctx,
        &mut failures,
    )
    .await;
    sweep_snapshot_goldens(&daemon, &ctx, &mut measured, &mut failures).await;
    sweep_snapshots(
        &daemon,
        &rows::snapshot_rows(&ctx),
        &mut measured,
        &mut failures,
    )
    .await;
    let snapshots_done = started.elapsed();
    let bulk_report = bulk::sweep(&mut client, &daemon, &ctx, &mut failures).await;
    eprintln!(
        "payload budget timing: rows {:?}, goldens + snapshots {:?}, bulk {:?}",
        rows_done.saturating_sub(setup),
        snapshots_done.saturating_sub(rows_done),
        started.elapsed().saturating_sub(snapshots_done)
    );

    let mut text = report(&measured);
    text.push_str("\n# BulkTransfer (allowed over budget up to their declared cap)\n");
    text.push_str(&bulk_report);
    let report_path = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("payload-budget-report-{baseline_file}"));
    std::fs::write(&report_path, &text).expect("write size report");
    eprintln!(
        "payload budget sweep: {} replies in {:?} (setup {setup:?}); sorted size report at {}\n{}",
        measured.len(),
        started.elapsed(),
        report_path.display(),
        text.lines().take(40).collect::<Vec<_>>().join("\n")
    );

    let offenders: BTreeMap<(String, String), usize> = measured
        .iter()
        .filter(|m| m.bytes > READ_PAYLOAD_BUDGET_BYTES)
        .filter(|m| !rows::VOLATILE.iter().any(|(v, _)| *v == m.method))
        .filter(|m| m.variant != "golden")
        .map(|m| ((m.method.clone(), m.variant.clone()), m.bytes))
        .collect();
    let baseline_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/payload_budget")
        .join(baseline_file);
    if std::env::var("PAYLOAD_BUDGET_UPDATE").is_ok_and(|v| v == "1") {
        let mut out = String::from(
            "# Known payload-budget offenders: `<method> <variant> <bytes>` (frame bytes).\n\
             # Generated by PAYLOAD_BUDGET_UPDATE=1; only ever delete lines by hand.\n",
        );
        for ((method, variant), bytes) in &offenders {
            let _ = writeln!(out, "{method} {variant} {bytes}");
        }
        std::fs::write(&baseline_path, out).expect("write baseline");
    } else {
        let text = std::fs::read_to_string(&baseline_path).unwrap_or_default();
        failures.extend(ratchet(&offenders, &parse_baseline(&text), baseline_file));
    }

    if probe_mode() {
        for f in &failures {
            eprintln!("FAILURE {f}");
        }
    }
    assert!(
        failures.is_empty() || probe_mode(),
        "payload budget sweep failed ({} problems):\n  {}",
        failures.len(),
        failures.join("\n  ")
    );
}

/// The sweep must stay off the network: `host.invite.searchAccounts`
/// (`default` + `max`) has to have been served by the loopback directory,
/// and nothing else may have asked it for a path it does not serve.
fn account_directory_problems(accounts: &proxies::AccountDirectory) -> Vec<String> {
    let mut problems = Vec::new();
    if accounts.hits() < 2 {
        problems.push(format!(
            "host.invite.searchAccounts: {} of its 2 variants reached the loopback account \
             directory — the sweep would call the real forge (intent-hq/intent#8703)",
            accounts.hits()
        ));
    }
    for line in accounts.unexpected() {
        problems.push(format!("account directory: unexpected request `{line}`"));
    }
    problems
}

/// Per-PR sweep on the `ci` preset.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn payload_budget_sweep_ci() {
    run_sweep(&LargeStateScale::ci(), "baseline.txt").await;
}

/// Nightly sweep on the `full` preset; select it with
/// `INTENTD_LARGE_STATE_SCALE=full` and `--run-ignored only`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full-scale sweep; run with INTENTD_LARGE_STATE_SCALE=full"]
async fn payload_budget_sweep_full_scale() {
    assert_eq!(
        std::env::var(intent_services::test_support::large_state::SCALE_ENV).as_deref(),
        Ok("full"),
        "the full-scale sweep runs only with INTENTD_LARGE_STATE_SCALE=full"
    );
    run_sweep(&LargeStateScale::from_env(), "baseline-full.txt").await;
}

#[test]
fn payload_budget_ratchet_flags_new_growth_and_stale() {
    let key = |m: &str, v: &str| (m.to_string(), v.to_string());
    let baseline = parse_baseline("# c\na.b default 1000\nc.d max 2000\ne.f x 3000\n");
    let offenders = BTreeMap::from([
        (key("a.b", "default"), 1_040),
        (key("c.d", "max"), 2_200),
        (key("g.h", "new"), 900_000),
    ]);
    let failures = ratchet(&offenders, &baseline, "baseline.txt");
    assert_eq!(failures.len(), 3, "{failures:#?}");
    assert!(failures[0].starts_with("GROWTH c.d max"), "{failures:#?}");
    assert!(
        failures[1].starts_with("NEW OFFENDER g.h new"),
        "{failures:#?}"
    );
    assert!(failures[2].starts_with("STALE e.f x"), "{failures:#?}");
}

/// The repository-native proxies (`sourceControl.read.list` /
/// `.discussions`) need read leases the sweep cannot mint, so their page
/// size is proven here instead: one page is at most 100 items, enforced
/// when the query deserializes (the checkout reads clamp the same 1–100 in
/// `page_scope`, pinned by `checkout_page_size_is_capped_at_100` in
/// intent-services).
#[test]
fn payload_budget_proxy_page_caps() {
    use intent_core::repository_resource_pages::{RepositoryListQuery, RepositoryPageQuery};
    for (limit, ok) in [
        (0, false),
        (1, true),
        (100, true),
        (101, false),
        (255, false),
    ] {
        let page = serde_json::from_value::<RepositoryPageQuery>(json!({ "limit": limit }));
        assert_eq!(page.is_ok(), ok, "page.limit {limit}: {page:?}");
        let list = serde_json::from_value::<RepositoryListQuery>(
            json!({ "state": "open", "filter": "all", "search": "", "limit": limit }),
        );
        assert_eq!(list.is_ok(), ok, "list limit {limit}: {list:?}");
    }
}

/// Typed params structs and pass-through parse functions are derived from
/// source; pin a few known shapes so the derivation cannot silently match
/// nothing.
#[test]
fn payload_budget_opaque_params_are_derived() {
    use params_scan::OpaqueSource;
    let set = |keys: &[&str]| keys.iter().map(|k| (*k).to_string()).collect();
    assert_eq!(
        params_scan::opaque_source("workspace.repositoryContext", None),
        OpaqueSource::Struct {
            name: "RepositoryContextBoundQuery".into(),
            keys: set(&["workspaceId", "gitRootId", "repositoryLifetimeId"]),
        }
    );
    assert_eq!(
        params_scan::opaque_source("sourceControl.read.resolveKnownRepository", None),
        OpaqueSource::Struct {
            name: "RepositoryKnownQuery".into(),
            keys: set(&["context", "readLifetimeId", "path"]),
        }
    );
    assert_eq!(
        params_scan::opaque_source("sourceControl.gitlab.childGit.get", None),
        OpaqueSource::Struct {
            name: "GitlabChildGitPolicyQuery".into(),
            keys: set(&[]),
        }
    );
    assert_eq!(
        params_scan::opaque_source(
            "script.list",
            Some(("intent-core/src/script_lists.rs", "parse", "value"))
        ),
        OpaqueSource::ParseFn {
            keys: set(&["view", "limit", "cursor"]),
            dynamic: true,
        }
    );
    assert_eq!(
        params_scan::opaque_source("script.list", None),
        OpaqueSource::Unknown
    );
}
