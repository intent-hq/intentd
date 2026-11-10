//! The harness 3.10 `workspace_api` reference (brief M1–M3, M5): a
//! capability map naming every method, a "use X for Y" routing table and
//! the hot set's signatures. Everything is cut from the bridge's assembled
//! full description, so `[agentFeatures]` pruning, the sub-agent cut and the
//! plain-folder cut apply exactly as they do to `ws.help`.

use std::fmt::Write as _;

use intent_core::settings_file::AgentFeaturesSettings;

use super::tools::{self, SpecialistModelOptions};
use super::workspace_api_docs::WorkspaceApiDocsInputs;

/// What every rendering starts with.
const PREAMBLE: &str = r"Execute JavaScript against the workspace API. Your code runs as an async function — use `return` to send results back.

Rules:
  - Your code is wrapped in `(async () => { ... })()` before execution.
  - Use `await` for async calls. Use `return` to send the final value back.
  - Use `Promise.all([...])` for independent reads/writes in one tool call.
  - Use `Promise.allSettled([...])` when you need partial results even if some calls fail.
  - Errors from invalid method names or bad arguments are returned directly.
  - Do not add code comments — the code is executed and discarded, never read by humans.

Parameters:
  code (required): JavaScript code to execute.
  summary (required): Short description of what this call does, shown in the UI.
";

/// The truncating providers' tool description points here.
const PROMPT_POINTER: &str =
    "\nReference: the system prompt's \"Workspace API Reference\" lists every method, \
     the routing table and the common signatures. ws.help(\"<namespace>\") returns one \
     namespace's full docs.\n";

const MAP_HEADER: &str =
    "Capability map (every method; ws.help(\"<namespace>\") returns its full signatures and docs):";
const ROUTING_HEADER: &str = "Routing (use X for Y):";
const HOT_HEADER: &str = "Common methods (signature + first sentence; the rest via ws.help):";

/// The most-called methods (intent-hq/intent `scripts/agent-context`
/// mining: 93% of historical calls). `ws.help` is in the map and the
/// preamble, so `agent.list` takes its slot. `agent.delegate` is listed for
/// its occupancy refusal (see [`DELEGATE_OCCUPANCY`]), not its call count.
/// `app.question.ask` joins because the eval showed agents guess its
/// required arguments from the map alone; it appears only on bridges that
/// install it.
pub(crate) const HOT_SET: &[&str] = &[
    "agent.send",
    "note.add",
    "script.status",
    "note.read",
    "agent.reportToParent",
    "script.start",
    "script.create",
    "script.output",
    "note.edit",
    "task.updateNoteStatus",
    "file.read",
    "script.monitor",
    "host.exec",
    "hook.schedule",
    "agent.getQueue",
    "workspace.setStatusMessage",
    "agent.status",
    "file.write",
    "agent.watch",
    "git.commit",
    "agent.list",
    "agent.readConversation",
    "script.list",
    "pr.snapshot",
    "agent.unwatch",
    "browser.exec",
    "note.listTasks",
    "pr.monitor",
    "workspace.details",
    "hook.list",
    "agent.delegate",
    "app.question.ask",
];

/// Rules the first sentence of a hot-set line drops but agents need to call
/// it right (agent eval, 2026-11-10): appended after that sentence.
pub(crate) const HOT_NOTES: &[(&str, &str)] = &[
    (
        "script.create",
        "`purpose`: \"saved\" (reusable) or \"oneOff\".",
    ),
    (
        "app.question.ask",
        "Requires `header`, `question` and 2+ `options` objects.",
    ),
];

/// The single-task `agent.delegate` option and rule the shared doc line
/// leaves out. That line's bytes are frozen for older harnesses, so 3.10
/// adds them to its hot-set entry and its `ws.help` page. The refusal is
/// the occupancy pre-gate in intent-services `agent_ops.rs`.
const DELEGATE_OCCUPANCY: (&str, &str, &str) = (
    "skipAutoCommit?",
    ", force?",
    " The single-task form refuses a task note that still has an assigned agent, even an idle \
     or finished one (only a deleted or retired agent frees it), unless the task is complete or \
     cancelled: reach that agent with `ws.agent.sendToTask` or `ws.agent.wakeOrCreate`, use a \
     separate task note, or pass `force: true` to add a second agent.",
);

const DELEGATE_LINE: &str = "  ws.agent.delegate(";

/// A shared `agent.delegate` line, full or condensed, with
/// [`DELEGATE_OCCUPANCY`] added.
fn with_delegate_occupancy(line: &str) -> String {
    let (after, option, rule) = DELEGATE_OCCUPANCY;
    let Some(at) = line.find(after) else {
        debug_assert!(false, "agent.delegate signature lost `{after}`: {line}");
        return line.to_owned();
    };
    let at = at + after.len();
    format!("{}{option}{}{rule}", &line[..at], &line[at..])
}

/// A hot-set entry as 3.10 renders it: the delegate occupancy rule, or a
/// [`HOT_NOTES`] note after the first sentence.
fn hot_line(path: &str, condensed: &str) -> String {
    if path == "agent.delegate" {
        return with_delegate_occupancy(condensed);
    }
    match HOT_NOTES.iter().find(|(p, _)| *p == path) {
        Some((_, note)) => format!("{condensed} {note}"),
        None => condensed.to_owned(),
    }
}

/// A `ws.help` reply as 3.10 serves it.
pub(crate) fn help(text: String) -> String {
    if !text.contains(DELEGATE_LINE) {
        return text;
    }
    text.split('\n')
        .map(|line| {
            if line.starts_with(DELEGATE_LINE) {
                with_delegate_occupancy(line)
            } else {
                line.to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// One routing row and the methods it names. A row whose methods are not
/// all on the bridge (feature-gated, plain folder, sub-agent) is dropped.
pub(crate) const ROUTES: &[(&str, &[&str])] = &[
    (
        "Add to a note (\"add to the spec\") → `ws.note.add`. Replace a whole note → `ws.note.setContent`, only when asked: it replaces everything.",
        &["note.add", "note.setContent"],
    ),
    (
        "Fix one section → `ws.note.edit`; one checkbox → `ws.task.update`; a task note's status → `ws.task.updateNoteStatus`; title or tags only → `ws.note.updateMetadata`.",
        &["note.edit", "task.update", "task.updateNoteStatus", "note.updateMetadata"],
    ),
    (
        "Wait for anything external (CI, a human, a service) → never sleep or poll in your turn (an idle turn is killed after ~30 min); register a watch, tell the user what you watch, end your turn. Its wake resumes you.",
        &[],
    ),
    (
        "Wait for a PR or CI → `ws.pr.monitor`; one-shot PR state → `ws.pr.snapshot(n, { repo: \"owner/name\" })` (repo for another repo, e.g. a submodule).",
        &["pr.monitor", "pr.snapshot"],
    ),
    (
        "Wait for another agent → `ws.agent.watch`; for file, task or git events → `ws.event.subscribe`.",
        &["agent.watch", "event.subscribe"],
    ),
    (
        "Wait for anything else, or a timer → `ws.hook.schedule`: check in the hook, diff against `hookState`, dispatch only on a real change. Timer: `delayMs` = the wait; in the immediate validation run return `{ dispatch: false, state: { armed: true } }`. At most 5 hooks, slowest useful cadence, `ttlMs` = expected time + margin; cancel stale ones with `ws.hook.cancel`.",
        &["hook.schedule", "hook.cancel"],
    ),
    (
        "Before waiting as a delegated agent → `ws.agent.reportToParent` what you watch, then `ws.task.updateNoteStatus(yourTask, \"waiting\")` unless it is complete or cancelled (never reopen your own task).",
        &["agent.reportToParent", "task.updateNoteStatus"],
    ),
    (
        "Build, test suite, dev server or watcher over ~30 s → an Intent script: reuse one from `ws.script.listPage()` or `ws.script.create(name, command, \"command\" | \"service\")`, then `ws.script.start(scriptId)`. Never your shell's background features (`run_in_background`, `Monitor`, `nohup`, trailing `&`, detached terminal sessions).",
        &["script.listPage", "script.create", "script.start"],
    ),
    (
        "Wait for a script → command mode: end your turn only when start's `autoMonitor.ok` is true. Service: `ws.script.monitor(scriptId, { ttlMs, runId, outputPattern })`, end your turn only on `ok: true`. On a refusal or `autoMonitorError` follow `ws.help(\"script\")`; `run-finished` means read `ws.script.output` now. Stop with `ws.script.stop`.",
        &["script.monitor", "script.output", "script.stop"],
    ),
];

/// A namespace from the `Namespaces` index: `ws.note.*` (key `note`) or a
/// callable like `ws.help(namespace?)` (key `help`).
struct Namespace<'a> {
    key: &'a str,
    /// The index line's text before ` — `, e.g. `ws.help(namespace?)`.
    label: &'a str,
    summary: &'a str,
    callable: bool,
    methods: Vec<&'a str>,
}

/// `  ws.git.listRoots() → …` → `git.listRoots`.
pub(crate) fn doc_line_path(line: &str) -> Option<&str> {
    let rest = line.strip_prefix("  ws.")?;
    let path = &rest[..rest.find('(')?];
    path.chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '.')
        .then_some(path)
}

fn namespaces_of(desc: &str) -> Vec<Namespace<'_>> {
    let mut out: Vec<Namespace<'_>> = desc
        .lines()
        .skip_while(|l| !l.starts_with("Namespaces"))
        .skip(1)
        .take_while(|l| !l.trim().is_empty())
        .filter_map(|line| {
            let (label, summary) = line.trim().split_once(" — ")?;
            let rest = label.strip_prefix("ws.")?;
            if let Some(key) = rest.strip_suffix(".*") {
                return Some(Namespace {
                    key,
                    label,
                    summary,
                    callable: false,
                    methods: Vec::new(),
                });
            }
            let key = &rest[..rest.find('(')?];
            Some(Namespace {
                key,
                label,
                summary,
                callable: true,
                methods: Vec::new(),
            })
        })
        .collect();
    for path in api_lines(desc).filter_map(doc_line_path) {
        let owner = out
            .iter_mut()
            .filter(|ns| {
                if ns.callable {
                    path == ns.key
                } else {
                    path.strip_prefix(ns.key)
                        .is_some_and(|rest| rest.starts_with('.'))
                }
            })
            .max_by_key(|ns| ns.key.len());
        if let Some(ns) = owner {
            if !ns.callable {
                let name = &path[ns.key.len() + 1..];
                if !ns.methods.contains(&name) {
                    ns.methods.push(name);
                }
            }
        }
    }
    out.retain(|ns| ns.callable || !ns.methods.is_empty());
    out
}

fn api_lines(desc: &str) -> impl Iterator<Item = &str> {
    desc.lines()
        .skip_while(|l| !l.starts_with("API:"))
        .take_while(|l| !l.starts_with("Examples"))
}

/// Every method path the map names (`note.add`, `help`).
pub(crate) fn map_paths(map: &str) -> Vec<String> {
    let mut out = Vec::new();
    for line in map.lines() {
        let Some(rest) = line.trim().strip_prefix("ws.") else {
            continue;
        };
        let head = rest.split(" — ").next().unwrap_or(rest);
        if let Some((ns, methods)) = head.split_once(".{") {
            for method in methods.trim_end_matches('}').split(", ") {
                out.push(format!("{ns}.{method}"));
            }
        } else if let Some(paren) = head.find('(') {
            out.push(head[..paren].to_string());
        }
    }
    out
}

/// The 3.10 reference parts for one bridge.
pub(crate) struct CompactReference {
    pub(crate) map: String,
    pub(crate) routing: String,
    pub(crate) hot: String,
    /// Remote placement text and specialist model options, when any.
    pub(crate) extras: String,
}

/// Namespaces the Assistant workspace's docs leave out on purpose, though
/// their bindings are installed: host exec, and the Labs-only discussions.
pub(crate) const CHIEF_OMITTED_NAMESPACES: &[&str] = &["host", "discussion"];

impl CompactReference {
    pub(crate) fn build(inputs: &WorkspaceApiDocsInputs<'_>) -> Self {
        let bridge_desc = |is_chief: bool, features: &AgentFeaturesSettings| {
            let desc = tools::workspace_api_description_for_bridge(
                is_chief,
                features,
                inputs.is_sub_agent,
            );
            if inputs.plain_folder {
                tools::plain_folder_description(&desc)
            } else {
                desc.into_owned()
            }
        };
        let mut descs = vec![bridge_desc(inputs.is_chief, inputs.features)];
        if inputs.is_chief {
            // The Assistant workspace installs every binding a project
            // workspace has except `ws.secret`, but its static docs leave
            // some out; take those lines from the project docs, except the
            // ones the Assistant omits on purpose.
            let project = AgentFeaturesSettings {
                secret_requests: false,
                ..inputs.features.clone()
            };
            descs.push(bridge_desc(false, &project));
        }
        let mut namespaces = namespaces_of(&descs[0]);
        for extra in descs[1..]
            .iter()
            .flat_map(|desc| namespaces_of(desc))
            .filter(|ns| !CHIEF_OMITTED_NAMESPACES.contains(&ns.key))
        {
            match namespaces.iter_mut().find(|ns| ns.key == extra.key) {
                Some(ns) => {
                    for name in extra.methods {
                        if !ns.methods.contains(&name) {
                            ns.methods.push(name);
                        }
                    }
                }
                None => namespaces.push(extra),
            }
        }
        for path in inputs.capability_methods {
            let ns = path.rsplit_once('.').and_then(|(key, name)| {
                namespaces
                    .iter_mut()
                    .find(|ns| !ns.callable && ns.key == key)
                    .map(|ns| (ns, name))
            });
            debug_assert!(
                ns.is_some(),
                "capability method {path} has no map namespace"
            );
            if let Some((ns, name)) = ns {
                if !ns.methods.contains(&name) {
                    ns.methods.push(name);
                }
            }
        }
        let mut map = String::from(MAP_HEADER);
        map.push('\n');
        for ns in &namespaces {
            if ns.callable {
                let _ = writeln!(map, "  {} — {}", ns.label, ns.summary);
            } else {
                let _ = writeln!(
                    map,
                    "  ws.{}.{{{}}} — {}",
                    ns.key,
                    ns.methods.join(", "),
                    ns.summary
                );
            }
        }
        let present = map_paths(&map);
        let has = |path: &&str| present.iter().any(|p| p == path);
        let mut routing = String::from(ROUTING_HEADER);
        routing.push('\n');
        for (row, needs) in ROUTES {
            if needs.iter().all(&has) {
                let _ = writeln!(routing, "  - {row}");
            }
        }
        let mut hot = String::from(HOT_HEADER);
        hot.push('\n');
        let mut seen: Vec<&str> = Vec::new();
        for line in descs.iter().flat_map(|desc| api_lines(desc)) {
            let Some(path) = doc_line_path(line).filter(|path| HOT_SET.contains(path)) else {
                continue;
            };
            if seen.contains(&path) {
                continue;
            }
            seen.push(path);
            if let Some(condensed) = tools::condensed_method_line(line) {
                hot.push_str(&hot_line(path, &condensed));
                hot.push('\n');
            }
        }
        Self {
            map,
            routing,
            hot,
            extras: extras(inputs),
        }
    }

    /// Map, routing, hot set and extras, blank-line separated.
    pub(crate) fn body(&self) -> String {
        let mut out = format!("{}\n{}\n{}", self.map, self.routing, self.hot);
        if !self.extras.is_empty() {
            out.push('\n');
            out.push_str(&self.extras);
        }
        out
    }
}

fn extras(inputs: &WorkspaceApiDocsInputs<'_>) -> String {
    let mut out = String::new();
    if let Some(help) = inputs
        .remote_placement_help
        .filter(|_| !inputs.plain_folder)
    {
        out.push_str(help.trim_end());
        out.push('\n');
    }
    let options: &[SpecialistModelOptions] = inputs.model_options;
    if !options.is_empty() {
        out.push_str(&tools::model_options_block(options));
    }
    out
}

/// The tool description: the whole reference for a provider that shows
/// long descriptions, else the preamble and a pointer to the system prompt.
pub(crate) fn tool_description(inputs: &WorkspaceApiDocsInputs<'_>) -> String {
    if inputs.truncating_provider {
        format!("{PREAMBLE}{PROMPT_POINTER}")
    } else {
        format!("{PREAMBLE}\n{}", CompactReference::build(inputs).body())
    }
}

/// The system-prompt reference, for truncating providers only.
pub(crate) fn prompt_reference(inputs: &WorkspaceApiDocsInputs<'_>) -> Option<String> {
    inputs
        .truncating_provider
        .then(|| CompactReference::build(inputs).body())
}

#[cfg(test)]
#[path = "compact_reference_tests.rs"]
mod tests;
