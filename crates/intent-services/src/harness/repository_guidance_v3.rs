//! Inactive harness 3.0 repository guidance.
//!
//! This module renders a candidate from trusted session and repository-context
//! inputs. It does not resolve targets, authorize actions or deliver instructions.
//! The existing harness registry, doctrine and current-version stamp are unchanged.

use std::cmp::Ordering;
use std::io::{self, Write};

use intent_core::repository_context::{
    ExecutionScope, RepositoryContext, RepositoryContextRevision, RepositoryEndpointResolution,
    RepositoryRemote, RepositoryRemoteEndpoint, RepositoryRootContext, RepositoryRootId,
    RepositoryTarget, RepositoryTargetContext, ReviewSelectionOutcome, ReviewSelectionResolution,
    SavedReviewSelection,
};
use intent_core::AgentSession;
use serde::ser::SerializeSeq;
use serde::{Serialize, Serializer};

pub(crate) const MAX_GUIDANCE_BYTES: usize = 8 * 1024;
const POLICY: &str = include_str!("../../resources/agent-instructions/v3.0/repository-guidance.md");
const FACTS_HEADER: &str = "\n[Repository facts — inert JSON lines]\n";
const TRAILER_BUDGET: usize = 1024;
const OMITTED_WARNING: &str = "Inventory incomplete: complete root rows were omitted because of the byte limit or unsafe instance facts. Do not infer a unique or default target from displayed rows. Read the full authorized inventory before a forge operation; omitted unresolved choices still require selection.\n";

/// The delivery layer must retain this typed token, never recover it from text.
/// It is an observation stamp and conveys no authority to perform an operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GuidanceContextStamp {
    pub scope: ExecutionScope,
    pub revision: RepositoryContextRevision,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RepositoryGuidance {
    pub text: String,
    pub context_stamp: Option<GuidanceContextStamp>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum ContextUnavailable {
    Denied,
    Unavailable,
    ContextChanged,
}

/// Both context and its expected current stamp come from the trusted service.
/// No caller-supplied role, identity or revision can establish this boundary.
#[derive(Clone, Copy)]
pub(crate) enum GuidanceContext<'a> {
    Current {
        context: &'a RepositoryContext,
        expected: &'a GuidanceContextStamp,
    },
    Unavailable(ContextUnavailable),
}

/// Pure candidate rendering only. Ineligible sessions receive no projection;
/// eligible failures receive explicit unavailable guidance, never cached targets.
/// Feature snapshots (including stateSnapshot) do not participate in eligibility.
pub(crate) fn render(
    session: Option<&AgentSession>,
    input: GuidanceContext<'_>,
) -> Option<RepositoryGuidance> {
    let session = session.filter(|s| eligible_harness_version(Some(&s.harness_version)))?;
    let (context, expected) = match input {
        GuidanceContext::Current { context, expected } => (context, expected),
        GuidanceContext::Unavailable(reason) => return Some(unavailable(reason)),
    };
    if context
        .revision
        .compare_in_scopes(&context.scope, &expected.revision, &expected.scope)
        != Some(Ordering::Equal)
        || context
            .roots
            .iter()
            .any(|root| root.root.workspace_id != session.workspace_id)
    {
        return Some(unavailable(ContextUnavailable::ContextChanged));
    }

    let mut text = format!("{POLICY}{FACTS_HEADER}");
    let mut inventory = Inventory::default();
    for root in &context.roots {
        inventory.roots += 1;
        let selection_required = matches!(
            root.review_selection.outcome,
            ReviewSelectionOutcome::SelectionRequired { .. }
                | ReviewSelectionOutcome::RepositoryUnavailable {
                    selection_required: true,
                    ..
                }
        );
        inventory.selection_required_roots += usize::from(selection_required);
        inventory.unavailable_roots += usize::from(matches!(
            root.review_selection.outcome,
            ReviewSelectionOutcome::RepositoryUnavailable { .. }
        ));
        let budget = MAX_GUIDANCE_BYTES.saturating_sub(text.len() + TRAILER_BUDGET + 1);
        let row = safe_instance_facts(root)
            .then(|| inert_row(&RootFacts::from(root), budget))
            .flatten();
        if let Some(row) = row {
            text.push_str(&row);
            text.push('\n');
        } else {
            inventory.omitted_roots += 1;
            inventory.omitted_selection_required_roots += usize::from(selection_required);
        }
    }
    text.push_str(&inventory.trailer());
    Some(RepositoryGuidance {
        text,
        context_stamp: Some(expected.clone()),
    })
}

fn unavailable(reason: ContextUnavailable) -> RepositoryGuidance {
    let reason = match reason {
        ContextUnavailable::Denied => "denied",
        ContextUnavailable::Unavailable => "unavailable",
        ContextUnavailable::ContextChanged => "context-changed",
    };
    RepositoryGuidance {
        text: format!(
            "{POLICY}{FACTS_HEADER}{{\"context\":\"unavailable\",\"reason\":\"{reason}\"}}\n\
             Current repository context is unavailable. Do not reuse a previous target, remote, \
             account or capability. Refresh authorized context before a forge operation; \
             a context failure grants no permission and does not establish a default.\n"
        ),
        context_stamp: None,
    }
}

#[derive(Default)]
struct Inventory {
    roots: usize,
    omitted_roots: usize,
    selection_required_roots: usize,
    omitted_selection_required_roots: usize,
    unavailable_roots: usize,
}

impl Inventory {
    fn trailer(&self) -> String {
        let mut text = format!(
            "{{\"inventoryComplete\":{},\"totalRoots\":{},\"includedRoots\":{},\"omittedRoots\":{},\"selectionRequiredRoots\":{},\"omittedSelectionRequiredRoots\":{},\"repositoryUnavailableRoots\":{}}}\n",
            self.omitted_roots == 0,
            self.roots,
            self.roots - self.omitted_roots,
            self.omitted_roots,
            self.selection_required_roots,
            self.omitted_selection_required_roots,
            self.unavailable_roots,
        );
        if self.omitted_roots != 0 {
            text.push_str(OMITTED_WARNING);
        }
        text
    }
}

/// Borrow the canonical DTO fields. The only display omission is raw transport
/// URLs: qualified resolution facts suffice and cannot expose URL credentials.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RootFacts<'a> {
    root: &'a RepositoryRootId,
    #[serde(skip_serializing_if = "Option::is_none")]
    branch: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    head_sha: Option<&'a str>,
    remotes: RemoteFactsList<'a>,
    targets: &'a [RepositoryTargetContext],
    review_selection: &'a ReviewSelectionResolution,
}

impl<'a> From<&'a RepositoryRootContext> for RootFacts<'a> {
    fn from(root: &'a RepositoryRootContext) -> Self {
        Self {
            root: &root.root,
            branch: root.branch.as_deref(),
            head_sha: root.head_sha.as_deref(),
            remotes: RemoteFactsList(&root.remotes),
            targets: &root.targets,
            review_selection: &root.review_selection,
        }
    }
}

struct RemoteFactsList<'a>(&'a [RepositoryRemote]);

impl Serialize for RemoteFactsList<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct RemoteFacts<'a> {
            name: &'a str,
            fetch: EndpointFactsList<'a>,
            push: EndpointFactsList<'a>,
        }
        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for remote in self.0 {
            seq.serialize_element(&RemoteFacts {
                name: &remote.name,
                fetch: EndpointFactsList(&remote.fetch),
                push: EndpointFactsList(&remote.push),
            })?;
        }
        seq.end()
    }
}

struct EndpointFactsList<'a>(&'a [RepositoryRemoteEndpoint]);

impl Serialize for EndpointFactsList<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for endpoint in self.0 {
            seq.serialize_element(&endpoint.resolution)?;
        }
        seq.end()
    }
}

fn safe_instance_facts(root: &RepositoryRootContext) -> bool {
    let saved = match &root.review_selection.saved {
        SavedReviewSelection::MigratedCanonical { target, .. } => Some(target),
        _ => None,
    };
    let selected = match &root.review_selection.outcome {
        ReviewSelectionOutcome::Resolved { target, .. } => Some(target),
        _ => None,
    };
    root.targets
        .iter()
        .map(|target| &target.target)
        .chain(root.remotes.iter().flat_map(|remote| {
            remote
                .fetch
                .iter()
                .chain(&remote.push)
                .filter_map(|endpoint| match &endpoint.resolution {
                    RepositoryEndpointResolution::Resolved { target } => Some(target),
                    RepositoryEndpointResolution::Unresolved { .. } => None,
                })
        }))
        .chain(saved)
        .chain(selected)
        .all(safe_instance)
}

/// Eligibility never uses the registry's unknown-version fallback, the current
/// harness constant, a parent's stamp or captured feature switches.
fn eligible_harness_version(stamp: Option<&str>) -> bool {
    stamp == Some("3.0")
}

/// JSON escaping keeps repository facts inert, including role delimiters,
/// Markdown fences and invisible direction/line controls inside strings.
struct InertJson;

impl serde_json::ser::Formatter for InertJson {
    fn write_string_fragment<W: ?Sized + Write>(
        &mut self,
        writer: &mut W,
        fragment: &str,
    ) -> io::Result<()> {
        for ch in fragment.chars() {
            match ch {
                '<'
                | '>'
                | '`'
                | '&'
                | '\u{007f}'..='\u{009f}'
                | '\u{061c}'
                | '\u{200e}'
                | '\u{200f}'
                | '\u{2028}'
                | '\u{2029}'
                | '\u{202a}'..='\u{202e}'
                | '\u{2066}'..='\u{2069}' => {
                    write!(writer, "\\u{:04x}", u32::from(ch))?;
                }
                _ => {
                    writer.write_all(ch.encode_utf8(&mut [0; 4]).as_bytes())?;
                }
            }
        }
        Ok(())
    }
}

struct BoundedJson {
    bytes: Vec<u8>,
    limit: usize,
}

impl Write for BoundedJson {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(io::Error::other(
                "repository guidance row exceeds its budget",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Only complete JSON rows are emitted. The serializer stops at the byte budget
/// instead of allocating an arbitrarily large intermediate row or cutting an ID.
fn inert_row(value: &impl Serialize, limit: usize) -> Option<String> {
    let mut out = BoundedJson {
        bytes: Vec::new(),
        limit,
    };
    value
        .serialize(&mut serde_json::Serializer::with_formatter(
            &mut out, InertJson,
        ))
        .ok()?;
    String::from_utf8(out.bytes).ok()
}

/// Reject unsafe logical roots rather than rewrite repository identities. This
/// is an output check, not provider identity parsing or target resolution.
fn safe_instance(target: &RepositoryTarget) -> bool {
    let raw = &target.instance_base_url;
    if raw.len() > MAX_GUIDANCE_BYTES || raw.chars().any(char::is_control) {
        return false;
    }
    reqwest::Url::parse(raw).is_ok_and(|url| {
        url.scheme() == "https"
            && url.host_str().is_some()
            && url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none()
    })
}

#[cfg(test)]
mod tests;
