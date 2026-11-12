//! Context budget (brief M8): what Intent injects into an agent's context,
//! measured as system prompt plus `workspace_api` tool description for every
//! provider in the registry, for a default top-level agent and a delegated
//! sub-agent, on the baseline harness and the newest one.
//!
//! Two fixtures (`tests/fixtures/context-budget/README.md` records their
//! source):
//! - **realistic**: the monorepo's pinned AGENTS.md, the developer's 15
//!   skills, default `[agentFeatures]` and one exposed remote machine. The
//!   newest harness must be at most half the baseline.
//! - **clean**: no repo rules, 3 skills, no remote machine. The newest
//!   harness must stay under a recorded ceiling, so Intent's own text cannot
//!   regrow.
//!
//! Every run prints a per-section breakdown with its owner (Intent or user);
//! run with `--no-capture` to see it on success.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use intent_acp::WorkspaceMcpServer;
use intent_core::ids::ExecutorCredentialId;
use intent_core::machines::{ExposedMachine, RemoteMachineState};
use intent_core::node_link::{MachineCapabilities, ProviderCapability, ProviderStatus};
use intent_core::placement::AgentPlatform;
use intent_core::{AgentId, AgentSession, WorkspaceApi};
use intent_providers::ProviderConfig;

use crate::session_surfaces::SessionSurfaces;
use crate::skills::SkillMetadata;
use crate::test_support::goldens::{seed_agent, setup, TempDb};
use crate::Services;

/// The release the cut is measured against.
const BASELINE: &str = "3.9";
/// The harness under test.
const CANDIDATE: &str = intent_core::CURRENT_HARNESS_VERSION;
/// The release the candidate replaces, reported for reference only (3.12:
/// the released 3.10 context cuts plus 3.11's inline visuals). The gate
/// stays on [`BASELINE`].
const PREVIOUS: &str = "3.12";
/// The newest harness may carry at most this share of the baseline on the
/// realistic fixture.
const REALISTIC_MAX_RATIO: f64 = 0.5;

/// Documented exception to the realistic 50% gate (user decision,
/// 2026-11-10): providers that already got the short 3.9 reference but
/// still need the fixture's AGENTS.md injected, because Intent cannot
/// confirm they load it themselves:
/// - Grok Build's folder-trust gate covers AGENTS.md and fails closed under
///   the ACP stdio transport (no prompt), so a fresh, untrusted worktree
///   does not load it (xai-org/grok-build `agent/folder_trust.rs`).
/// - Droid documents native AGENTS.md loading, but neither its headless
///   `droid exec` mode nor a minimum version is documented, and the CLI is
///   closed source; an unknown version injects (`native_rules`).
///
/// For these, Intent-owned text must still halve, and the total may not grow
/// past the measured value: `(provider, top-level chars, sub-agent chars)`.
/// Measured on 3.12, which carries the released inline-visuals block from
/// 3.11 (intentd #3240, 1,946 chars) and the `ws.agent.delegate` occupancy
/// entry (#3248) on top of the 3.10 cuts, plus 3.13's explicit-rename
/// Workspace Title rule (+51 chars) and the ordered machine list's
/// preference line (+168 chars, with a remote machine in the fixture).
/// Revisit if either CLI's native load can be confirmed: add it to
/// `native_rules` and drop its row.
const REALISTIC_INJECTED_RULES_CEILING: &[(&str, usize, usize)] =
    &[("droid", 58_228, 56_366), ("grok", 58_228, 56_366)];

/// Clean-fixture ceiling per provider: `(provider, top-level chars,
/// sub-agent chars)`. Measured on harness 3.12: the 3.10 compact ws.*
/// reference (with the `ws.agent.delegate` occupancy entry), trimmed
/// doctrine and skills cut, the eval-driven hot-set notes, and 3.11's
/// released visuals (2026-11-10), plus 3.13's explicit-rename Workspace
/// Title rule (+51 chars). Lower each row again as further cuts land.
const CLEAN_CEILING: &[(&str, usize, usize)] = &[
    ("auggie", 27_444, 25_582),
    ("claude-code", 27_723, 25_861),
    ("codex", 27_444, 25_582),
    ("cortex", 27_444, 25_582),
    ("opencode", 27_444, 25_582),
    ("unsloth", 27_444, 25_582),
    ("pi", 27_444, 25_582),
    ("droid", 27_723, 25_861),
    ("grok", 27_723, 25_861),
    ("antigravity", 27_444, 25_582),
    ("cursor", 27_672, 25_810),
    ("mock", 27_444, 25_582),
];

/// Where the fixtures' paths appear to live, so sizes do not depend on the
/// temp dir.
const FIXTURE_HOME: &str = "/home/dev";
/// The developer's synced skills folder (`~/.claude/skills/synced/<id>`).
const SYNCED_SKILLS: &str =
    ".claude/skills/synced/00000000-0000-4000-8000-000000000001_00000000-0000-4000-8000-000000000002";
/// The repository checkout, under the fixture home.
const REPO_DIR: &str = "intent/workspaces/project/repo";
/// Skills that live in the repository (`<repo>/.agents/skills`).
const PROJECT_SKILLS: &[&str] = &["repo-retrospective"];

/// One fixture: what the workspace and the machine carry.
struct Fixture {
    name: &'static str,
    /// The pinned monorepo AGENTS.md is the repository's rule file.
    repo_rules: bool,
    /// The skills installed (`None`: every fixture skill).
    skills: Option<&'static [&'static str]>,
    /// One remote machine is exposed to the backend.
    remote_machine: bool,
    /// The local Claude Code CLI's `--version` (`None`: unknown, so every
    /// rule file stays injected).
    claude_cli_version: Option<&'static str>,
    /// The local Codex's effective `project_doc_max_bytes` (`None`: unknown,
    /// so every rule file stays injected).
    codex_doc_cap: Option<usize>,
}

/// The developer runs a Claude Code CLI that loads AGENTS.md by itself and
/// a Codex with the default doc cap (the 27.7k AGENTS.md fits), so the
/// newest harness injects it for neither. Other providers still get it.
const REALISTIC: Fixture = Fixture {
    name: "realistic",
    repo_rules: true,
    skills: None,
    remote_machine: true,
    claude_cli_version: Some(crate::native_rules::test_versions::FIRST),
    codex_doc_cap: Some(crate::native_rules::CODEX_DEFAULT_PROJECT_DOC_MAX_BYTES),
};

const CLEAN: Fixture = Fixture {
    name: "clean",
    repo_rules: false,
    skills: Some(&["docx", "pdf", "xlsx"]),
    remote_machine: false,
    claude_cli_version: None,
    codex_doc_cap: None,
};

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/context-budget")
}

/// A fixture laid out on disk the way a developer's machine has it.
struct Materialized {
    _dir: tempfile::TempDir,
    claude_cli_version: Option<&'static str>,
    codex_doc_cap: Option<usize>,
    root: PathBuf,
    repo: PathBuf,
    skills: Vec<SkillMetadata>,
    machines: Vec<ExposedMachine>,
}

fn materialize(fixture: &Fixture) -> Materialized {
    let dir = crate::test_support::test_tempdir("context-budget-");
    let root = dir.path().join("home").join("dev");
    let repo = root.join(REPO_DIR);
    std::fs::create_dir_all(&repo).expect("repo dir");
    // A workspace is a git checkout: Codex treats the folder as its
    // project root.
    std::fs::create_dir_all(repo.join(".git")).expect(".git dir");
    let source = fixture_dir();
    if fixture.repo_rules {
        std::fs::copy(source.join("monorepo-AGENTS.md"), repo.join("AGENTS.md"))
            .expect("copy AGENTS.md");
    }
    let wanted = |name: &str| fixture.skills.is_none_or(|names| names.contains(&name));
    for (kind, target) in [
        ("user", root.join(SYNCED_SKILLS)),
        ("project", repo.join(".agents/skills")),
    ] {
        let mut entries: Vec<_> = std::fs::read_dir(source.join("skills").join(kind))
            .expect("fixture skills")
            .map(|entry| entry.expect("entry").path())
            .collect();
        entries.sort();
        for path in entries {
            let name = path
                .file_stem()
                .expect("stem")
                .to_string_lossy()
                .into_owned();
            if !wanted(&name) {
                continue;
            }
            assert_eq!(
                kind == "project",
                PROJECT_SKILLS.contains(&name.as_str()),
                "{name}"
            );
            let skill_dir = target.join(&name);
            std::fs::create_dir_all(&skill_dir).expect("skill dir");
            std::fs::copy(&path, skill_dir.join("SKILL.md")).expect("copy skill");
        }
    }
    let mut skills: Vec<SkillMetadata> =
        crate::skills::discover_on_machine(Some(&repo), Some(root.clone()))
            .into_iter()
            .map(SkillMetadata::from_machine)
            .collect();
    let canonical = root.canonicalize().expect("canonical root");
    for skill in &mut skills {
        skill.location = normalize(&skill.location, &root, &canonical);
        skill.resource_directory = skill
            .resource_directory
            .as_deref()
            .map(|directory| normalize(directory, &root, &canonical));
    }
    let machines = if fixture.remote_machine {
        vec![mac_studio()]
    } else {
        Vec::new()
    };
    Materialized {
        _dir: dir,
        claude_cli_version: fixture.claude_cli_version,
        codex_doc_cap: fixture.codex_doc_cap,
        root,
        repo,
        skills,
        machines,
    }
}

/// `text` with the fixture root spelled as [`FIXTURE_HOME`].
fn normalize(text: &str, root: &Path, canonical: &Path) -> String {
    text.replace(canonical.to_string_lossy().as_ref(), FIXTURE_HOME)
        .replace(root.to_string_lossy().as_ref(), FIXTURE_HOME)
}

/// The developer's exposed machine: three providers, one of them with a
/// long local model list, last seen and reported a while ago.
fn mac_studio() -> ExposedMachine {
    let platform = AgentPlatform::of("macos", "aarch64");
    let mut capabilities = MachineCapabilities::new(
        platform.expect("platform"),
        "0.80.0",
        8,
        "2026-11-09T11:50:46.82102Z",
    );
    let unsloth = [
        "unsloth/Qwen3.8-27B-GGUF",
        "unsloth/gemma-4-12b-it-GGUF",
        "unsloth/Qwen3.6-35B-A3B-MTP-GGUF",
        "unsloth/gemma-4-26B-A4B-it-GGUF",
        "unsloth/Qwen3.5-9B-GGUF",
        "unsloth/Qwen3.6-35B-A3B-GGUF",
        "unsloth/Qwen3.5-4B-GGUF",
        "unsloth/Muse-Glimmer-30B-GGUF",
        "unsloth/Qwen3-4B-Instruct-2507-GGUF",
        "unsloth/gpt-oss-20b-GGUF",
    ]
    .into_iter()
    .map(str::to_owned)
    .chain((0..219).map(|i| format!("unsloth/local-model-{i:03}-GGUF")))
    .collect();
    capabilities.providers = vec![
        provider(
            "claude-code",
            ProviderStatus::SignedIn,
            ["sonnet", "claude-fable-5-1[1m]", "opus", "haiku"]
                .map(str::to_owned)
                .to_vec(),
        ),
        provider("codex", ProviderStatus::SignedIn, Vec::new()),
        provider("unsloth", ProviderStatus::Unknown, unsloth),
    ];
    ExposedMachine {
        id: "00000000-0000-4000-8000-000000000003".into(),
        name: "Developer’s Mac Studio".into(),
        hosts: vec!["10.0.0.7".into()],
        port: 7443,
        tc_address: None,
        fingerprint: "ab".repeat(32),
        credential_id: ExecutorCredentialId::new(),
        platform,
        capabilities: Some(capabilities),
        observed_at: Some("2026-11-09T11:50:46.82102Z".into()),
        state: RemoteMachineState::Disconnected,
        added_at: "2026-10-01T00:00:00Z".into(),
        last_connected_at: Some("2026-11-09T11:50:47.464045323Z".into()),
        node_protocol: None,
        cloud: None,
    }
}

fn provider(id: &str, status: ProviderStatus, models: Vec<String>) -> ProviderCapability {
    ProviderCapability {
        id: id.into(),
        visible: true,
        status,
        models,
        models_stale: false,
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
enum Role {
    TopLevel,
    SubAgent,
}

impl Role {
    const ALL: [Self; 2] = [Self::TopLevel, Self::SubAgent];

    fn label(self) -> &'static str {
        match self {
            Self::TopLevel => "top-level",
            Self::SubAgent => "sub-agent",
        }
    }
}

/// What one agent carries.
#[derive(Clone, PartialEq, Eq)]
struct Surfaces {
    tool: String,
    prompt: String,
}

impl Surfaces {
    fn chars(&self) -> usize {
        self.tool.chars().count() + self.prompt.chars().count()
    }
}

/// The surfaces a spawn builds for `version`, `provider` and `role` on the
/// materialized fixture.
async fn measure(
    services: &Services,
    fixture: &Materialized,
    base: &AgentSession,
    version: &str,
    provider: &ProviderConfig,
    role: Role,
) -> Surfaces {
    let entry = crate::harness::resolve_entry(version);
    let mut session = base.clone();
    session.harness_version = version.to_owned();
    if role == Role::SubAgent {
        session.parent_agent_id = Some(AgentId::from("agent-parent"));
    }
    let is_sub_agent = role == Role::SubAgent;
    let features = (entry.default_features)();
    let surfaces = SessionSurfaces::new(&session, provider);
    let placement = (surfaces.placement_help())(
        &intent_core::machine_selection::PlacementHelpInputs::new(&fixture.machines, &|_| false),
    );
    let api: Arc<dyn WorkspaceApi> = Arc::new(services.clone());
    let server = surfaces.configure(
        WorkspaceMcpServer::for_agent_type(api, session.workspace_id.clone(), "interactive")
            .with_agent_features(features.clone())
            .with_sub_agent(is_sub_agent)
            .with_remote_placement_help(placement),
    );
    let docs = server.workspace_api_prompt_reference();
    let executor = match provider.id {
        "claude-code" => Some(crate::native_rules::RuleExecutor {
            provider_id: provider.id.to_owned(),
            cli_version: fixture.claude_cli_version.map(str::to_owned),
            codex_doc_cap: None,
        }),
        "codex" => Some(crate::native_rules::RuleExecutor {
            provider_id: provider.id.to_owned(),
            cli_version: None,
            codex_doc_cap: fixture.codex_doc_cap,
        }),
        _ => None,
    };
    let prompt = crate::rules::assemble_agent_system_prompt(
        services.store(),
        Some(&fixture.repo),
        "interactive",
        None,
        is_sub_agent,
        false,
        false,
        &features,
        None,
        Some(&session),
        docs.as_deref(),
        &crate::skill_catalog::SkillEnvironment::default(),
        executor.as_ref(),
    )
    .await
    .expect("assembled prompt");
    // The prompt assembler discovers skills from the workspace's checkout
    // and the daemon's home; the fixture's live in a temp home instead.
    // No MCP servers configured in Intent or the provider, as on a fresh
    // machine: skills that need one are hidden.
    let environment = crate::skill_catalog::SkillEnvironment::new(
        provider.id,
        Some(std::collections::BTreeMap::new()),
    );
    let skills = (entry.context.skills_block)(&crate::harness::SkillsBlockInputs {
        skills: &fixture.skills,
        environment: &environment,
    });
    let prompt = if skills.trim().is_empty() {
        prompt
    } else {
        entry.harness.join_prompt_layers(&[prompt, skills])
    };
    let canonical = fixture.root.canonicalize().expect("canonical root");
    Surfaces {
        tool: server.workspace_api_tool_description(),
        prompt: normalize(
            &crate::rules::with_provider_prompt_note(prompt, provider),
            &fixture.root,
            &canonical,
        ),
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Owner {
    Intent,
    User,
}

impl Owner {
    fn label(self) -> &'static str {
        match self {
            Self::Intent => "Intent",
            Self::User => "user",
        }
    }
}

/// One labeled share of a surface.
struct Section {
    surface: &'static str,
    label: String,
    owner: Owner,
    chars: usize,
}

/// Split `text` into sections: the repo rules and skills list (owner user)
/// and the remote-placement text are carved out by their markers; the rest
/// is Intent text split at its `#` / `##` headings outside code fences.
/// The sections partition `text`, so their sizes sum to its size.
fn sections(surface: &'static str, text: &str, placement: Option<&str>) -> Vec<Section> {
    let mut spans: Vec<(usize, usize, &str, Owner)> = Vec::new();
    let mut carve = |start_marker: &str, end_marker: Option<&str>, label, owner| {
        let mut from = 0;
        while let Some(found) = text[from..].find(start_marker) {
            let start = from + found;
            let end = match end_marker {
                None => start + start_marker.len(),
                Some(marker) => match text[start..].find(marker) {
                    Some(at) => {
                        let after = start + at + marker.len();
                        after + text[after..].find('\n').unwrap_or(text.len() - after)
                    }
                    None => text.len(),
                },
            };
            if spans.iter().all(|&(s, e, ..)| end <= s || start >= e) {
                spans.push((start, end, label, owner));
            }
            from = end.max(start + 1);
        }
    };
    carve(
        "## User Rules & Guidelines",
        Some("These rules are loaded from: "),
        "Repo rules (AGENTS.md)",
        Owner::User,
    );
    carve(
        "The following skills provide specialized instructions",
        Some("</available_skills>"),
        "Skills list",
        Owner::User,
    );
    if let Some(help) = placement {
        let spliced = help.lines().fold(String::new(), |mut block, line| {
            let _ = writeln!(block, "    {}", line.trim());
            block
        });
        for block in [spliced.as_str(), help] {
            carve(block, None, "Remote placement", Owner::Intent);
        }
    }
    spans.sort_unstable_by_key(|span| span.0);
    let mut out = Vec::new();
    let mut at = 0;
    for (start, end, label, owner) in spans {
        intent_sections(surface, &text[at..start], &mut out);
        out.push(Section {
            surface,
            label: label.to_owned(),
            owner,
            chars: text[start..end].chars().count(),
        });
        at = end;
    }
    intent_sections(surface, &text[at..], &mut out);
    out
}

fn intent_sections(surface: &'static str, text: &str, out: &mut Vec<Section>) {
    let mut label: Option<String> = None;
    let mut chars = 0;
    let mut fenced = false;
    let mut flush = |label: &mut Option<String>, chars: &mut usize| {
        if *chars > 0 {
            out.push(Section {
                surface,
                label: label.take().unwrap_or_else(|| "(no heading)".to_owned()),
                owner: Owner::Intent,
                chars: *chars,
            });
        }
        *chars = 0;
    };
    for line in text.split_inclusive('\n') {
        let trimmed = line.trim_start();
        if trimmed.starts_with("```") || trimmed.starts_with("~~~") {
            fenced = !fenced;
        } else if !fenced
            && (line.starts_with("# ") || line.starts_with("## ") || line.starts_with("### "))
        {
            flush(&mut label, &mut chars);
            label = Some(line.trim().trim_start_matches('#').trim().to_owned());
        } else if let Some(namespace) =
            namespace_of(line).filter(|ns| !fenced && label.as_deref() != Some(ns))
        {
            flush(&mut label, &mut chars);
            label = Some(namespace);
        } else if label.is_none() && chars == 0 && !line.trim().is_empty() {
            label = Some(line.trim().chars().take(48).collect());
        }
        chars += line.chars().count();
    }
    flush(&mut label, &mut chars);
}

/// `ws.<namespace>.*` for a `workspace_api` reference line that documents
/// one method (`  ws.note.read(id) → …`).
fn namespace_of(line: &str) -> Option<String> {
    let rest = line.strip_prefix("  ")?.trim_start().strip_prefix("ws.")?;
    let (namespace, method) = rest.split_once('.')?;
    (!namespace.is_empty()
        && namespace.bytes().all(|b| b.is_ascii_alphabetic())
        && method.starts_with(|c: char| c.is_ascii_alphabetic() || c == '*'))
    .then(|| format!("ws.{namespace}.*"))
}

/// Sections summed by `(surface, owner, label)`.
fn breakdown(
    surfaces: &Surfaces,
    placement: Option<&str>,
) -> BTreeMap<(&'static str, Owner, String), usize> {
    let mut sums = BTreeMap::new();
    for section in sections("tool", &surfaces.tool, placement)
        .into_iter()
        .chain(sections("prompt", &surfaces.prompt, placement))
    {
        *sums
            .entry((section.surface, section.owner, section.label))
            .or_insert(0) += section.chars;
    }
    sums
}

/// Rows smaller than this in both versions fold into one line.
const MIN_ROW_CHARS: usize = 400;

/// One fixture's measurements: `(provider, role) → (baseline, candidate)`.
struct Measured {
    rows: BTreeMap<(String, Role), (Surfaces, Surfaces)>,
    /// [`PREVIOUS`]'s total chars per row, for the report only.
    previous: BTreeMap<(String, Role), usize>,
    placement: (Option<String>, Option<String>),
}

async fn measure_fixture(fixture: &Fixture) -> (Measured, Materialized) {
    let (_db, services, session) = services().await;
    let materialized = materialize(fixture);
    let mut rows = BTreeMap::new();
    let mut previous = BTreeMap::new();
    for provider in intent_providers::config::ACP_PROVIDERS {
        for role in Role::ALL {
            let baseline =
                measure(&services, &materialized, &session, BASELINE, provider, role).await;
            let candidate = measure(
                &services,
                &materialized,
                &session,
                CANDIDATE,
                provider,
                role,
            )
            .await;
            let previous_total =
                measure(&services, &materialized, &session, PREVIOUS, provider, role)
                    .await
                    .chars();
            previous.insert((provider.id.to_owned(), role), previous_total);
            rows.insert((provider.id.to_owned(), role), (baseline, candidate));
        }
    }
    let placement = |version: &str| {
        (crate::harness::resolve_entry(version)
            .context
            .placement_help)(&intent_core::machine_selection::PlacementHelpInputs::new(
            &materialized.machines,
            &|_| false,
        ))
    };
    let placement = (placement(BASELINE), placement(CANDIDATE));
    (
        Measured {
            rows,
            previous,
            placement,
        },
        materialized,
    )
}

async fn services() -> (TempDb, Services, AgentSession) {
    let (db, services, ws) = setup().await;
    let id = AgentId::from("agent-budget");
    seed_agent(&services, &ws, &id).await;
    let session = services
        .store()
        .get_agent_session_with_transcript(&id)
        .await
        .expect("seeded session");
    (db, services, session)
}

fn tokens(chars: usize) -> usize {
    chars.div_ceil(4)
}

fn ratio(baseline: usize, candidate: usize) -> f64 {
    #[expect(
        clippy::cast_precision_loss,
        reason = "prompt sizes are far below 2^52"
    )]
    let ratio = candidate as f64 / baseline.max(1) as f64;
    ratio
}

/// The printed report: a summary per provider and role, then one
/// per-section breakdown per group of providers that carry the same text.
fn report(fixture: &Fixture, measured: &Measured, skills: usize) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "\n=== Context budget: {} fixture ({BASELINE} vs {CANDIDATE}) ===\n\
         repo rules: {}; skills: {skills}; remote machines: {}; \
         [agentFeatures]: default\n",
        fixture.name,
        if fixture.repo_rules {
            "monorepo AGENTS.md"
        } else {
            "none"
        },
        u8::from(fixture.remote_machine),
    );
    let _ = writeln!(
        out,
        "| provider | role | {BASELINE} prompt | {BASELINE} tool | {BASELINE} total | {CANDIDATE} prompt | {CANDIDATE} tool | {CANDIDATE} total | ratio | {PREVIOUS} total | vs {PREVIOUS} |\n|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|"
    );
    for ((provider, role), (baseline, candidate)) in &measured.rows {
        let previous = measured.previous[&(provider.clone(), *role)];
        let _ = writeln!(
            out,
            "| {provider} | {} | {} | {} | {} (~{}k tok) | {} | {} | {} (~{}k tok) | {:.0}% | {previous} | {:.0}% |",
            role.label(),
            baseline.prompt.chars().count(),
            baseline.tool.chars().count(),
            baseline.chars(),
            tokens(baseline.chars()) / 1000,
            candidate.prompt.chars().count(),
            candidate.tool.chars().count(),
            candidate.chars(),
            tokens(candidate.chars()) / 1000,
            100.0 * ratio(baseline.chars(), candidate.chars()),
            100.0 * ratio(previous, candidate.chars()),
        );
    }
    let mut groups: Vec<(Role, Vec<&str>, &Surfaces, &Surfaces)> = Vec::new();
    for ((provider, role), (baseline, candidate)) in &measured.rows {
        match groups
            .iter_mut()
            .find(|group| group.0 == *role && group.2 == baseline && group.3 == candidate)
        {
            Some(group) => group.1.push(provider),
            None => groups.push((*role, vec![provider], baseline, candidate)),
        }
    }
    for (role, providers, baseline, candidate) in groups {
        let before = breakdown(baseline, measured.placement.0.as_deref());
        let after = breakdown(candidate, measured.placement.1.as_deref());
        let mut keys: Vec<_> = before.keys().chain(after.keys()).cloned().collect();
        keys.sort();
        keys.dedup();
        keys.sort_by_key(|key| {
            std::cmp::Reverse(
                before
                    .get(key)
                    .copied()
                    .unwrap_or(0)
                    .max(after.get(key).copied().unwrap_or(0)),
            )
        });
        let _ = writeln!(
            out,
            "\n--- {} — {} ---\n| surface | section | owner | {BASELINE} | {CANDIDATE} |\n|---|---|---|---:|---:|",
            role.label(),
            providers.join(", ")
        );
        let (mut folded, mut folded_before, mut folded_after) = (0, 0, 0);
        for key in keys {
            let (old, new) = (before.get(&key).copied(), after.get(&key).copied());
            if old.unwrap_or(0).max(new.unwrap_or(0)) < MIN_ROW_CHARS {
                folded += 1;
                folded_before += old.unwrap_or(0);
                folded_after += new.unwrap_or(0);
                continue;
            }
            let cell =
                |value: Option<usize>| value.map_or_else(|| "–".to_owned(), |v| v.to_string());
            let _ = writeln!(
                out,
                "| {} | {} | {} | {} | {} |",
                key.0,
                key.2,
                key.1.label(),
                cell(old),
                cell(new)
            );
        }
        if folded > 0 {
            let _ = writeln!(
                out,
                "| both | ({folded} smaller sections) | Intent | {folded_before} | {folded_after} |"
            );
        }
        let owned = |sums: &BTreeMap<(&'static str, Owner, String), usize>, owner| {
            sums.iter()
                .filter(|(key, _)| key.1 == owner)
                .map(|(_, chars)| chars)
                .sum::<usize>()
        };
        for owner in [Owner::Intent, Owner::User] {
            let _ = writeln!(
                out,
                "| all | **{} total** | {} | {} | {} |",
                owner.label(),
                owner.label(),
                owned(&before, owner),
                owned(&after, owner)
            );
        }
    }
    out
}

/// The realistic fixture is what the brief describes, and the baseline
/// carries all of it, so the 50% gate measures a real cut.
fn assert_realistic_baseline(measured: &Measured, fixture: &Materialized) {
    let rules = std::fs::read_to_string(fixture_dir().join("monorepo-AGENTS.md")).expect("rules");
    assert_eq!(fixture.skills.len(), 15, "the developer's 15 skills");
    assert_eq!(fixture.machines.len(), 1);
    let placement = measured
        .placement
        .0
        .as_deref()
        .expect("baseline placement text");
    let first_line = placement.lines().next().expect("placement line").trim();
    for ((provider, role), (baseline, _)) in &measured.rows {
        let context = format!("{provider} {}", role.label());
        assert_eq!(
            baseline.prompt.matches(rules.trim()).count(),
            1,
            "{context}: the baseline injects AGENTS.md once"
        );
        for skill in &fixture.skills {
            assert!(
                baseline
                    .prompt
                    .contains(&format!("<name>{}</name>", skill.name)),
                "{context}: the baseline lists {}",
                skill.name
            );
        }
        assert!(
            baseline.tool.contains(first_line) || baseline.prompt.contains(first_line),
            "{context}: the baseline carries the placement text"
        );
    }
}

/// On the newest harness, claude-code and codex (whose CLIs load AGENTS.md
/// by themselves) get no injected copy; every other provider still gets one.
fn assert_native_rules_skipped(measured: &Measured) {
    let rules = std::fs::read_to_string(fixture_dir().join("monorepo-AGENTS.md")).expect("rules");
    for ((provider, role), (_, candidate)) in &measured.rows {
        let expected = usize::from(!matches!(provider.as_str(), "claude-code" | "codex"));
        assert_eq!(
            candidate.prompt.matches(rules.trim()).count(),
            expected,
            "{provider} {}: injected AGENTS.md copies on harness {CANDIDATE}",
            role.label()
        );
    }
}

/// Measures both fixtures on every provider and prints the breakdown; the
/// clean fixture's newest-harness numbers must stay under the ceiling.
#[tokio::test]
async fn context_budget_report() {
    let (realistic, realistic_fixture) = measure_fixture(&REALISTIC).await;
    println!(
        "{}",
        report(&REALISTIC, &realistic, realistic_fixture.skills.len())
    );
    assert_realistic_baseline(&realistic, &realistic_fixture);
    assert_native_rules_skipped(&realistic);

    let (clean, clean_fixture) = measure_fixture(&CLEAN).await;
    let clean_report = report(&CLEAN, &clean, clean_fixture.skills.len());
    println!("{clean_report}");
    assert_eq!(clean_fixture.skills.len(), 3);
    let mut over = Vec::new();
    let mut actual = Vec::new();
    for provider in intent_providers::config::ACP_PROVIDERS {
        let total = |role| clean.rows[&(provider.id.to_owned(), role)].1.chars();
        let (top, sub) = (total(Role::TopLevel), total(Role::SubAgent));
        actual.push(format!("    (\"{}\", {top}, {sub}),", provider.id));
        match CLEAN_CEILING.iter().find(|row| row.0 == provider.id) {
            Some(&(_, top_max, sub_max)) if top <= top_max && sub <= sub_max => {}
            _ => over.push(provider.id),
        }
    }
    assert!(
        over.is_empty(),
        "clean fixture over its ceiling (or missing a row) on {over:?}: Intent's own text grew \
         on harness {CANDIDATE}. Cut it, or for a deliberate addition update CLEAN_CEILING \
         (measured rows below).\n{}\n{clean_report}",
        actual.join("\n")
    );
}

/// Intent-owned chars of `surfaces`.
fn intent_chars(surfaces: &Surfaces, placement: Option<&str>) -> usize {
    breakdown(surfaces, placement)
        .iter()
        .filter(|(key, _)| key.1 == Owner::Intent)
        .map(|(_, chars)| chars)
        .sum()
}

/// The newest harness carries at most half the baseline on the realistic
/// fixture, for every provider, top-level and sub-agent; the providers in
/// [`REALISTIC_INJECTED_RULES_CEILING`] halve Intent's own text and stay
/// under their total ceiling instead.
#[tokio::test]
async fn realistic_fixture_halves_context() {
    let (measured, fixture) = measure_fixture(&REALISTIC).await;
    for row in REALISTIC_INJECTED_RULES_CEILING {
        assert!(
            measured
                .rows
                .contains_key(&(row.0.to_owned(), Role::TopLevel)),
            "exception row {} names no registered provider",
            row.0
        );
    }
    let mut over = Vec::new();
    for ((provider, role), (baseline, candidate)) in &measured.rows {
        let exception = REALISTIC_INJECTED_RULES_CEILING
            .iter()
            .find(|row| row.0 == provider.as_str());
        let Some(&(_, top_max, sub_max)) = exception else {
            if ratio(baseline.chars(), candidate.chars()) > REALISTIC_MAX_RATIO {
                over.push(format!(
                    "{provider} {}: {} > {:.0}% of {}",
                    role.label(),
                    candidate.chars(),
                    100.0 * REALISTIC_MAX_RATIO,
                    baseline.chars()
                ));
            }
            continue;
        };
        let (old, new) = (
            intent_chars(baseline, measured.placement.0.as_deref()),
            intent_chars(candidate, measured.placement.1.as_deref()),
        );
        if ratio(old, new) > REALISTIC_MAX_RATIO {
            over.push(format!(
                "{provider} {}: Intent-owned {new} > {:.0}% of {old}",
                role.label(),
                100.0 * REALISTIC_MAX_RATIO,
            ));
        }
        let max = if *role == Role::SubAgent {
            sub_max
        } else {
            top_max
        };
        if candidate.chars() > max {
            over.push(format!(
                "{provider} {}: {} > ceiling {max} (REALISTIC_INJECTED_RULES_CEILING)",
                role.label(),
                candidate.chars(),
            ));
        }
    }
    assert!(
        over.is_empty(),
        "harness {CANDIDATE} is over budget on the realistic fixture:\n{}\n{}",
        over.join("\n"),
        report(&REALISTIC, &measured, fixture.skills.len())
    );
}
