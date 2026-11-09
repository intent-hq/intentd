use intent_core::settings_file::AgentFeaturesSettings;
use intent_core::WorkspaceId;

use super::{
    map_paths, tool_description, CompactReference, CHIEF_OMITTED_NAMESPACES, HOT_SET, ROUTES,
};
use crate::mcp_server::bindings::prelude_for_workspace;
use crate::mcp_server::effective_agent_features;
use crate::mcp_server::tools::{workspace_api_description, SpecialistModelOptions};
use crate::mcp_server::workspace_api_docs::{
    WorkspaceApiDocs, WorkspaceApiDocsInputs, WorkspaceApiDocsV3_10,
};

fn all_on() -> AgentFeaturesSettings {
    AgentFeaturesSettings {
        secret_requests: true,
        ..AgentFeaturesSettings::default()
    }
}

type Toggle = (&'static str, fn(&mut AgentFeaturesSettings));

/// Each toggle off in turn, after all on.
fn feature_sets() -> Vec<(&'static str, AgentFeaturesSettings)> {
    let on = all_on();
    let mut out = vec![("all on", on.clone())];
    let toggles: [Toggle; 13] = [
        ("backgroundHooks", |f| f.background_hooks = false),
        ("hostExec", |f| f.host_exec = false),
        ("scripts", |f| f.scripts = false),
        ("terminalAccess", |f| f.terminal_access = false),
        ("browserAutomation", |f| f.browser_automation = false),
        ("desktopControl", |f| f.desktop_control = false),
        ("structuredQuestions", |f| f.structured_questions = false),
        ("secretRequests", |f| f.secret_requests = false),
        ("attentionRequests", |f| f.attention_requests = false),
        ("prMonitor", |f| f.pr_monitor = false),
        ("taskGraph", |f| f.task_graph = false),
        ("peerAgents", |f| f.peer_agents = false),
        ("mcpTools", |f| f.mcp_tools = false),
    ];
    for (name, off) in toggles {
        let mut features = on.clone();
        off(&mut features);
        out.push((name, features));
    }
    out
}

/// The bridge shape a test renders for.
#[derive(Clone, Copy)]
enum Shape {
    Top,
    Sub,
    Chief,
    PlainFolder,
    Truncating,
}

fn inputs(shape: Shape, features: &AgentFeaturesSettings) -> WorkspaceApiDocsInputs<'_> {
    WorkspaceApiDocsInputs {
        is_chief: matches!(shape, Shape::Chief),
        features,
        model_options: &[],
        is_sub_agent: matches!(shape, Shape::Sub),
        plain_folder: matches!(shape, Shape::PlainFolder),
        remote_placement_help: None,
        truncating_provider: matches!(shape, Shape::Truncating),
        capability_methods: &[],
    }
}

/// Every function path the bridge's prelude installs on `ws`.
async fn installed_paths(
    features: &AgentFeaturesSettings,
    is_sub_agent: bool,
    is_chief: bool,
    machine_models: bool,
) -> Vec<String> {
    let workspace = if is_chief {
        WorkspaceId::chief()
    } else {
        WorkspaceId::from_string("ws-map")
    };
    let code = format!(
        "{}\nconst out = [];\n\
         const walk = (o, p, depth) => {{ for (const k of Object.keys(o)) {{ const v = o[k]; const path = p ? p + '.' + k : k;\n\
         if (typeof v === 'function') out.push(path); else if (v && typeof v === 'object' && depth < 4) walk(v, path, depth + 1); }} }};\n\
         walk(ws, '', 0); return out.sort();",
        prelude_for_workspace(features, is_sub_agent, &workspace, machine_models)
    );
    let value = intent_js::eval(&code, &intent_js::EvalOptions::default(), None)
        .await
        .expect("prelude evaluates");
    serde_json::from_value(value).expect("paths")
}

/// Installed stubs that always refuse ("not yet available in this daemon
/// port"); the map leaves them out so agents never call them.
const UNAVAILABLE_STUBS: &[&str] = &[
    "workspace.context",
    "workspace.timeline",
    "workspace.referenceDocs",
    "workspace.emitNotification",
];

#[test]
fn unavailable_stubs_still_refuse() {
    let source = include_str!("bindings/workspace.rs");
    for path in UNAVAILABLE_STUBS {
        assert!(
            source.contains(&format!("ws.{path} is not yet available")),
            "{path} is implemented now: drop it from UNAVAILABLE_STUBS and document it"
        );
    }
}

/// Installed for listing but out of the map: the stubs above; on a
/// non-Assistant bridge the `ws.app.*` management bindings, whose dispatch
/// refuses outside the Assistant workspace (`ws.app.question` is the
/// exception); and on the Assistant the namespaces its docs omit.
fn listed(path: &str, is_chief: bool) -> bool {
    let namespace = path.split('.').next().unwrap_or_default();
    !UNAVAILABLE_STUBS.contains(&path)
        && (is_chief || !path.starts_with("app.") || path.starts_with("app.question."))
        && !(is_chief && CHIEF_OMITTED_NAMESPACES.contains(&namespace))
}

/// The drift test (brief M1): the map names every installed binding and
/// nothing else, for every bridge shape and every `[agentFeatures]` toggle.
#[tokio::test]
async fn capability_map_names_exactly_the_installed_bindings() {
    for (toggle, features) in feature_sets() {
        for shape in [Shape::Top, Shape::Sub, Shape::Chief] {
            let (is_chief, is_sub_agent) =
                (matches!(shape, Shape::Chief), matches!(shape, Shape::Sub));
            let effective = effective_agent_features(&features, is_sub_agent, is_chief);
            for machine_models in [false, true] {
                let installed: Vec<String> =
                    installed_paths(&features, is_sub_agent, is_chief, machine_models)
                        .await
                        .into_iter()
                        .filter(|path| listed(path, is_chief))
                        .collect();
                let mut docs = inputs(shape, &effective);
                if machine_models {
                    docs.capability_methods = &["agent.machineModels"];
                }
                let reference = CompactReference::build(&docs);
                let mut mapped = map_paths(&reference.map);
                mapped.sort();
                let missing: Vec<&String> =
                    installed.iter().filter(|p| !mapped.contains(p)).collect();
                let extra: Vec<&String> =
                    mapped.iter().filter(|p| !installed.contains(p)).collect();
                assert!(
                    missing.is_empty() && extra.is_empty(),
                    "{toggle} chief={is_chief} sub={is_sub_agent} machineModels={machine_models}: \
                     map is missing {missing:?}, names uninstalled {extra:?}. Document a new \
                     binding in tools.rs."
                );
            }
        }
    }
}

/// Every hot-set method and every routing row's methods exist, so a
/// rename cannot silently drop a row or a signature.
#[test]
fn hot_set_and_routes_name_real_methods() {
    let features = all_on();
    let reference = CompactReference::build(&inputs(Shape::Top, &features));
    let mapped = map_paths(&reference.map);
    for path in HOT_SET {
        assert!(mapped.iter().any(|p| p == path), "hot set: {path}");
        assert!(
            reference.hot.contains(&format!("  ws.{path}(")),
            "hot signature: {path}"
        );
    }
    assert_eq!(reference.hot.matches("\n  ws.").count(), HOT_SET.len());
    for (row, needs) in ROUTES {
        for path in *needs {
            assert!(mapped.iter().any(|p| p == path), "route {row}: {path}");
        }
        assert!(reference.routing.contains(row), "{row}");
    }
}

/// A disabled toggle drops its methods from the map, the hot set and the
/// routing rows that name them.
#[test]
fn disabled_features_prune_every_part() {
    let features = AgentFeaturesSettings {
        scripts: false,
        background_hooks: false,
        pr_monitor: false,
        ..all_on()
    };
    let body = CompactReference::build(&inputs(Shape::Top, &features)).body();
    for gone in ["ws.script", "ws.hook", "ws.pr.monitor", "ws.mr.monitor"] {
        assert!(!body.contains(gone), "{gone} survived:\n{body}");
    }
    assert!(body.contains("ws.pr.snapshot("));
    assert!(body.contains("ws.agent.watch"));
}

/// Plain folders drop git and PR methods; sub-agents drop proposals.
#[test]
fn bridge_shape_cuts_apply() {
    let features = all_on();
    let plain = CompactReference::build(&inputs(Shape::PlainFolder, &features)).body();
    for gone in ["ws.git", "ws.pr.", "ws.mr", "proposeSibling", "git.commit"] {
        assert!(!plain.contains(gone), "{gone} survived in a plain folder");
    }
    let sub_features = AgentFeaturesSettings {
        structured_questions: false,
        secret_requests: false,
        ..features.clone()
    };
    let sub = CompactReference::build(&inputs(Shape::Sub, &sub_features)).body();
    assert!(!sub.contains("proposeSibling") && !sub.contains("applyProposal"));
    assert!(!sub.contains("ws.app.question"));
}

/// Non-truncating providers get the whole reference in the tool
/// description; truncating ones get a short pointer that fits their cut.
#[test]
fn placement_follows_truncation() {
    let features = AgentFeaturesSettings::default();
    let full = tool_description(&inputs(Shape::Top, &features));
    assert!(
        full.contains("Capability map")
            && full.contains("Routing")
            && full.contains("Common methods")
    );
    let short = tool_description(&inputs(Shape::Truncating, &features));
    assert!(!short.contains("Capability map"));
    let heading = crate::mcp_server::tools::WORKSPACE_API_SYSTEM_PROMPT_HEADING;
    assert!(short.contains(heading.trim_start_matches("# ")));
    let section = WorkspaceApiDocsV3_10.prompt_section("body");
    assert!(section.starts_with(heading), "{section}");
    assert!(short.len() < 2_000, "{}", short.len());
    let v1 = workspace_api_description(false, &features);
    assert!(full.len() * 4 < v1.len(), "{} vs {}", full.len(), v1.len());
}

/// Capability-gated bindings join their namespace's map line.
#[test]
fn capability_methods_join_the_map() {
    let features = AgentFeaturesSettings::default();
    let mut with = inputs(Shape::Top, &features);
    assert!(!map_paths(&CompactReference::build(&with).map)
        .contains(&"agent.machineModels".to_string()));
    with.capability_methods = &["agent.machineModels"];
    assert!(
        map_paths(&CompactReference::build(&with).map).contains(&"agent.machineModels".to_string())
    );
}

/// Placement text and model options ride along; placement never in a
/// plain folder.
#[test]
fn extras_carry_placement_and_model_options() {
    let features = AgentFeaturesSettings::default();
    let options = [SpecialistModelOptions {
        specialist: "pr-reviewer".into(),
        default_model: Some("opus".into()),
        options: Vec::new(),
    }];
    let mut with = inputs(Shape::Top, &features);
    with.remote_placement_help = Some("Remote placement (experimental): node \"Mac\".\n");
    with.model_options = &options;
    let body = CompactReference::build(&with).body();
    assert!(body.contains("Remote placement (experimental): node \"Mac\"."));
    assert!(body.contains("pr-reviewer: default `opus`"));
    with.plain_folder = true;
    assert!(!CompactReference::build(&with)
        .body()
        .contains("Remote placement"));
}

/// Prints the default renderings for review:
/// `cargo nextest run -p intent-acp --lib --run-ignored only --no-capture -E 'test(print_v3_10_reference)'`.
#[test]
#[ignore = "prints the reference for review"]
fn print_v3_10_reference() {
    let features = AgentFeaturesSettings::default();
    println!("{}", tool_description(&inputs(Shape::Top, &features)));
    println!(
        "=====\n{}",
        tool_description(&inputs(Shape::Truncating, &features))
    );
}
