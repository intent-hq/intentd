//! Private, transport-neutral ownership of an original repository read request.
//!
//! These interfaces carry an existing service owner's scope. They neither
//! authenticate a caller nor grant repository access. They are deliberately
//! independent of transport frames, Store, provider credentials and Serde.

use std::sync::Arc;

use crate::{BoxFuture, Result};

/// Selected by the original transport admission, never by request parameters.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RepositoryWireEntry {
    /// The actual authenticated connection's bearer binding.
    Bearer,
    /// A connection admitted by the local control transport.
    AdmittedLocal,
}

/// The router's typed service outcome, before JSON envelope construction.
///
/// Neither variant establishes whether the payload is private. In particular,
/// service errors can contain private data, and a successful value can itself
/// contain an `error` field. The original scope owns that classification and
/// retains actual provider error/quota evidence independently of delivery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RepositoryReadReplyKind {
    Result,
    ServiceError,
}

/// One original authenticated connection, independent of client and RPC ids.
pub trait RepositoryReadConnection: Send + Sync {
    /// Independent explicit resource reads; unsupported owners fail closed.
    fn capture_resource(
        &self,
        _frame: &RepositoryResourceFrame,
    ) -> Option<Arc<dyn RepositoryReadRequestScope>> {
        None
    }

    fn take_resource_retirements(&self) -> Option<Box<dyn RepositoryResourceRetirements>> {
        None
    }

    /// Capture synchronously before the request's first await or spawn.
    /// Each call returns a distinct original request, including equal RPC ids.
    fn capture(&self) -> Arc<dyn RepositoryReadRequestScope>;

    /// Attach declared native read coverage synchronously before queueing.
    /// The query supplies invalidation keys, never authorization. Older owners
    /// retain their ordinary capture behavior.
    fn capture_context(
        &self,
        _query: &RepositoryContextQuery,
    ) -> Arc<dyn RepositoryReadRequestScope> {
        self.capture()
    }

    /// Capture a distinct native selection frame before queueing. This does not
    /// widen a context read lease. Unsupported owners cannot supply admission.
    fn capture_selection(
        &self,
        _frame: &RepositorySelectionFrame,
    ) -> Option<Arc<dyn RepositoryReadRequestScope>> {
        None
    }

    /// Independent socket-private selection retirement stream.
    fn take_selection_retirements(&self) -> Option<Box<dyn RepositorySelectionRetirements>> {
        None
    }

    /// Original native review frame, captured before any queue or await.
    fn capture_review(
        &self,
        _frame: &NativeReviewFrame,
    ) -> Option<Arc<dyn RepositoryReadRequestScope>> {
        None
    }
    fn take_review_retirements(&self) -> Option<Box<dyn NativeReviewRetirements>> {
        None
    }

    /// Retire only this connection's original request cohort. Idempotent.
    /// Concrete owners must join admitted leaves without holding cohort maps.
    fn retire(&self);

    /// One socket-private retirement feed. Older service implementations have none.
    fn take_retirements(&self) -> Option<Box<dyn RepositoryReadRetirements>> {
        None
    }
}

/// An original request's scope, kept until its final response handling finishes.
pub trait RepositoryReadRequestScope: Send + Sync {
    /// Restore the concrete owner's private context and run `body` exactly once.
    /// This must not turn an absent or retired capture into a later owner.
    fn scope<'a>(&'a self, body: BoxFuture<'a, ()>) -> BoxFuture<'a, ()>;

    /// Complete/cancel the original request even if clones of this scope escape.
    fn retire(&self);

    /// Validate and admit one already prepared response using original evidence.
    ///
    /// An unused ordinary request may transfer directly. A qualified request
    /// must freshly revalidate within a new child of the SAME original request
    /// and consume its authority fence through `transfer`. The synchronous
    /// action may only move a prebuilt packet into a pre-reserved slot: no
    /// serialization, reservation, cache lock, I/O, await or spawn inside.
    ///
    /// Service errors require explicit outcome policy; they are not public
    /// transport failures. Preserve actual provider errors/quota independently
    /// of private-payload eligibility. An error returned here must be safe to
    /// map to the existing local error policy without disclosing the withheld
    /// payload. Never retry an action that already transferred its packet.
    fn deliver<'a>(
        &'a self,
        kind: RepositoryReadReplyKind,
        transfer: &'a mut (dyn FnMut() -> Result<()> + Send),
    ) -> BoxFuture<'a, Result<()>>;
}

#[cfg(test)]
mod tests;

/// Original-socket control receiver. Dropping it permanently closes its feed.
pub trait RepositoryReadRetirements: Send {
    fn next(&mut self) -> BoxFuture<'_, Option<RepositoryContextRetired>>;
}

/// A requested lookup, never a path or caller-provided authority.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RepositoryContextQuery {
    pub workspace_id: crate::WorkspaceId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_root_id: Option<crate::WorkspaceGitRootId>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RepositoryContextBoundQuery {
    pub workspace_id: crate::WorkspaceId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_root_id: Option<crate::WorkspaceGitRootId>,
    pub repository_lifetime_id: String,
}

/// Explicit read coverage; neither variant permits a write or a provider call.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum RepositoryContextCoverage {
    WorkspaceInventory {
        workspace_id: crate::WorkspaceId,
    },
    RegisteredRoot {
        workspace_id: crate::WorkspaceId,
        git_root_id: crate::WorkspaceGitRootId,
    },
}

/// These fields are rejection/correlation data, not transferable permission.
#[derive(Clone, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RepositoryContextCapture {
    pub lifetime_id: String,
    pub scope: crate::ExecutionScope,
    pub coverage: RepositoryContextCoverage,
    pub retirement_sequence: String,
    pub expires_after_ms: u64,
}

#[derive(Clone, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RepositoryContextReleased {
    pub released: bool,
}

/// Contains no root, account, credential or private observation payload.
/// Sequence is emitted from a checked u64 as its canonical decimal string.
#[derive(Clone, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RepositoryContextRetired {
    pub lifetime_ids: Vec<String>,
    pub sequence: String,
    pub all_retired: bool,
    pub terminal: bool,
}

#[cfg(test)]
mod native_contract_tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn repository_native_queries_and_coverage_have_one_strict_wire_shape() {
        let workspace = crate::WorkspaceId::new();
        let root = crate::WorkspaceGitRootId::new();
        let input = json!({"workspaceId":workspace});
        let q: RepositoryContextQuery = serde_json::from_value(input.clone()).unwrap();
        assert_eq!(serde_json::to_value(q).unwrap(), input);
        for key in [
            "principalId",
            "authorityScopeId",
            "path",
            "repositoryLifetimeId",
        ] {
            let mut bad = input.clone();
            bad[key] = json!("forged");
            assert!(serde_json::from_value::<RepositoryContextQuery>(bad).is_err());
        }
        let input =
            json!({"workspaceId":workspace,"gitRootId":root,"repositoryLifetimeId":"original"});
        let q: RepositoryContextBoundQuery = serde_json::from_value(input.clone()).unwrap();
        assert_eq!(serde_json::to_value(q).unwrap(), input);
        let coverage = RepositoryContextCoverage::RegisteredRoot {
            workspace_id: workspace.clone(),
            git_root_id: root.clone(),
        };
        assert_eq!(
            serde_json::to_value(coverage).unwrap(),
            json!({"kind":"registeredRoot","workspaceId":workspace,"gitRootId":root})
        );
        assert_eq!(
            serde_json::to_value(RepositoryContextCoverage::WorkspaceInventory {
                workspace_id: workspace.clone()
            })
            .unwrap(),
            json!({"kind":"workspaceInventory","workspaceId":workspace})
        );
        assert!(serde_json::from_value::<RepositoryContextCoverage>(
            json!({"kind":"workspaceInventory","workspaceId":workspace,"gitRootId":root})
        )
        .is_err());
    }
    #[test]
    fn repository_native_counters_remain_decimal_without_private_retirement_payload() {
        let capture = RepositoryContextCapture {
            lifetime_id: "lifetime".into(),
            scope: crate::ExecutionScope {
                daemon_id: "boot".into(),
                authority_scope_id: "scope".into(),
                authority_generation: u64::MAX,
            },
            coverage: RepositoryContextCoverage::WorkspaceInventory {
                workspace_id: crate::WorkspaceId::new(),
            },
            retirement_sequence: u64::MAX.to_string(),
            expires_after_ms: 300_000,
        };
        let value = serde_json::to_value(capture).unwrap();
        assert_eq!(value["scope"]["authorityGeneration"], u64::MAX.to_string());
        assert_eq!(value["retirementSequence"], u64::MAX.to_string());
        let notice = RepositoryContextRetired {
            lifetime_ids: vec![],
            sequence: u64::MAX.to_string(),
            all_retired: true,
            terminal: true,
        };
        assert_eq!(
            serde_json::to_value(notice).unwrap(),
            json!({"lifetimeIds":[],"sequence":u64::MAX.to_string(),"allRetired":true,"terminal":true})
        );
    }
}

/// Exactly one root: omitted `git_root_id` is Primary, never inventory coverage.
pub type RepositorySelectionQuery = RepositoryContextQuery;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RepositorySelectionBoundQuery {
    pub workspace_id: crate::WorkspaceId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_root_id: Option<crate::WorkspaceGitRootId>,
    pub selection_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(
    tag = "mode",
    rename_all = "kebab-case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum RepositorySelectionChoice {
    Automatic {},
    ExplicitRemote { remote_name: String },
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RepositorySelectionSaveQuery {
    pub workspace_id: crate::WorkspaceId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_root_id: Option<crate::WorkspaceGitRootId>,
    pub selection_id: String,
    pub choice: RepositorySelectionChoice,
}

/// A transport-captured immutable command. Not a deserializable authority token.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RepositorySelectionFrame {
    Capture(RepositorySelectionQuery),
    Save(RepositorySelectionSaveQuery),
    Reset(RepositorySelectionBoundQuery),
    Reconcile(RepositorySelectionBoundQuery),
    Release(RepositorySelectionBoundQuery),
}
impl RepositorySelectionFrame {
    #[must_use]
    pub fn query(&self) -> RepositorySelectionQuery {
        match self {
            Self::Capture(q) => q.clone(),
            Self::Save(q) => RepositorySelectionQuery {
                workspace_id: q.workspace_id.clone(),
                git_root_id: q.git_root_id.clone(),
            },
            Self::Reset(q) | Self::Reconcile(q) | Self::Release(q) => RepositorySelectionQuery {
                workspace_id: q.workspace_id.clone(),
                git_root_id: q.git_root_id.clone(),
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum RepositorySelectionState {
    NeverSaved,
    Reset,
    Saved { value: crate::SavedReviewSelection },
}

/// Public observation only. The actual CAS snapshot remains owned by Store.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RepositorySelectionSnapshot {
    pub root: crate::RepositoryRootId,
    pub root_incarnation: String,
    pub selection_revision: String,
    pub selection: RepositorySelectionState,
}

#[derive(Clone, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RepositorySelectionCapture {
    pub selection_id: String,
    pub scope: crate::ExecutionScope,
    pub root: crate::RepositoryRootId,
    pub snapshot: RepositorySelectionSnapshot,
    pub retirement_sequence: String,
    pub expires_after_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum RepositorySelectionFailure {
    AdmissionRetired,
    AuthorityUnavailable,
    StorageFailed,
    CompletionUnobserved,
}
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum RepositorySelectionResult {
    Applied {
        snapshot: RepositorySelectionSnapshot,
    },
    Unchanged {
        snapshot: RepositorySelectionSnapshot,
    },
    Conflict {
        snapshot: RepositorySelectionSnapshot,
    },
    MissingRoot,
    Failed {
        code: RepositorySelectionFailure,
    },
}
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum RepositorySelectionPersistence {
    NotAttempted,
    NoEffect,
    Committed { selection_revision: String },
    Unknown,
}
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RepositorySelectionReceipt {
    pub result: RepositorySelectionResult,
    pub persistence: RepositorySelectionPersistence,
}
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum RepositorySelectionAttemptState {
    NotStarted,
    Pending,
    Settled {
        receipt: Box<RepositorySelectionReceipt>,
    },
}
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RepositorySelectionAttempt {
    pub selection_id: String,
    pub root: crate::RepositoryRootId,
    pub attempt: RepositorySelectionAttemptState,
}
#[derive(Clone, Debug, serde::Serialize)]
pub struct RepositorySelectionReleased {
    pub released: bool,
}

/// Admission retirement only. No private root, account or receipt payload.
#[derive(Clone, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RepositorySelectionRetired {
    pub selection_ids: Vec<String>,
    pub sequence: String,
    pub all_retired: bool,
    pub terminal: bool,
}
pub trait RepositorySelectionRetirements: Send {
    fn next(&mut self) -> BoxFuture<'_, Option<RepositorySelectionRetired>>;
}

#[cfg(test)]
mod selection_contract_tests {
    use super::{
        RepositorySelectionBoundQuery, RepositorySelectionChoice, RepositorySelectionPersistence,
        RepositorySelectionQuery, RepositorySelectionSaveQuery, RepositorySelectionState,
    };
    use serde_json::json;
    #[test]
    fn native_selection_strict_command_shape_never_accepts_authority_or_snapshot() {
        let base = json!({"workspaceId":"workspace", "selectionId":"original", "choice":{"mode":"automatic"}});
        assert!(serde_json::from_value::<RepositorySelectionSaveQuery>(base.clone()).is_ok());
        for key in [
            "scope",
            "principalId",
            "snapshot",
            "repositoryLifetimeId",
            "revision",
            "accountId",
            "hostRole",
        ] {
            let mut value = base.clone();
            value[key] = json!("forged");
            assert!(serde_json::from_value::<RepositorySelectionSaveQuery>(value).is_err());
        }
        for choice in [
            json!({"mode":"reset"}),
            json!({"mode":"unresolved-historical"}),
            json!({"mode":"automatic","remoteName":"extra"}),
            json!({"mode":"explicit-remote","remoteName":"origin","proof":"forged"}),
        ] {
            assert!(serde_json::from_value::<RepositorySelectionChoice>(choice).is_err());
        }
        assert!(serde_json::from_value::<RepositorySelectionBoundQuery>(
            json!({"workspaceId":"w","selectionId":"a","choice":{"mode":"automatic"}})
        )
        .is_err());
        assert!(serde_json::from_value::<RepositorySelectionQuery>(
            json!({"workspaceId":"w","selectionId":"a"})
        )
        .is_err());
    }
    #[test]
    fn native_selection_independent_persistence_and_history_have_stable_tags() {
        assert_eq!(
            serde_json::to_value(RepositorySelectionPersistence::Committed {
                selection_revision: u64::MAX.to_string()
            })
            .unwrap(),
            json!({"kind":"committed","selectionRevision":u64::MAX.to_string()})
        );
        assert_eq!(
            serde_json::to_value(RepositorySelectionState::NeverSaved).unwrap(),
            json!({"kind":"neverSaved"})
        );
        assert_eq!(
            serde_json::to_value(RepositorySelectionState::Reset).unwrap(),
            json!({"kind":"reset"})
        );
        assert_eq!(
            serde_json::to_value(RepositorySelectionState::Saved {
                value: crate::SavedReviewSelection::Automatic
            })
            .unwrap(),
            json!({"kind":"saved","value":{"mode":"automatic"}})
        );
    }
}

/// Strict, presence-discriminated native review command. Public IDs only correlate
/// with an original socket-owned operation; they never grant execution authority.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeReviewPrepareQuery {
    pub workspace_id: crate::WorkspaceId,
    pub action: crate::NativeReviewStage,
    pub files: Option<Vec<String>>,
    pub options: NativeReviewOptions,
    pub review: NativeReviewChoiceQuery,
}
// Presence is significant only for the opt-in companion forms. The ordinary
// form retains its previous defaults, null acceptance and serialized fields.
#[derive(Default)]
enum NativeReviewPresence<T> {
    #[default]
    Missing,
    Present(T),
}
impl<'de, T: serde::Deserialize<'de>> serde::Deserialize<'de> for NativeReviewPresence<T> {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        T::deserialize(d).map(Self::Present)
    }
}
impl<T> NativeReviewPresence<T> {
    fn present(&self) -> bool {
        matches!(self, Self::Present(_))
    }
    fn value(self) -> Option<T> {
        match self {
            Self::Missing => None,
            Self::Present(v) => Some(v),
        }
    }
}
impl<'de> serde::Deserialize<'de> for NativeReviewPrepareQuery {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        #[derive(serde::Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        struct ChoiceWire {
            #[serde(deserialize_with = "native_review_root")]
            root: crate::RepositoryRootId,
            choice: NativeReviewChoice,
            #[serde(default)]
            target_branch: NativeReviewPresence<Option<String>>,
            #[serde(default)]
            push_remote: NativeReviewPresence<Option<String>>,
            #[serde(default)]
            companion: NativeReviewPresence<NativeReviewCompanion>,
        }
        #[derive(serde::Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        struct Wire {
            workspace_id: crate::WorkspaceId,
            action: crate::NativeReviewStage,
            #[serde(default)]
            files: NativeReviewPresence<Option<Vec<String>>>,
            #[serde(default)]
            options: NativeReviewPresence<NativeReviewOptions>,
            review: ChoiceWire,
        }
        let wire = Wire::deserialize(d)?;
        let r = &wire.review;
        let child = matches!(r.choice, NativeReviewChoice::AfterCommit { .. });
        let marked = r.companion.present();
        let invalid = || serde::de::Error::custom("Unsupported native review companion parameters");
        if child {
            if wire.action != crate::NativeReviewStage::CreatePr
                || wire.files.present()
                || wire.options.present()
                || r.target_branch.present()
                || r.push_remote.present()
                || marked
            {
                return Err(invalid());
            }
            if let NativeReviewChoice::AfterCommit {
                operation_id,
                capture_id,
            } = &r.choice
            {
                if [operation_id, capture_id].iter().any(|id| {
                    id.len() != 36
                        || id.bytes().any(|b| b.is_ascii_uppercase())
                        || uuid::Uuid::parse_str(id).is_err()
                }) {
                    return Err(invalid());
                }
            }
        } else if marked
            && (wire.action != crate::NativeReviewStage::Commit
                || wire.files.present()
                || wire.options.present()
                || r.push_remote.present()
                || !matches!(&r.target_branch, NativeReviewPresence::Present(Some(v)) if !v.is_empty()))
        {
            return Err(invalid());
        }
        Ok(Self {
            workspace_id: wire.workspace_id,
            action: wire.action,
            files: wire.files.value().flatten(),
            options: wire.options.value().unwrap_or_default(),
            review: NativeReviewChoiceQuery {
                root: wire.review.root,
                choice: wire.review.choice,
                target_branch: wire.review.target_branch.value().flatten(),
                push_remote: wire.review.push_remote.value().flatten(),
                companion: wire.review.companion.value(),
            },
        })
    }
}
impl serde::Serialize for NativeReviewPrepareQuery {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let companion = self.review.companion.is_some()
            || matches!(self.review.choice, NativeReviewChoice::AfterCommit { .. });
        let mut out = serializer.serialize_struct(
            "NativeReviewPrepareQuery",
            3 + usize::from(self.files.is_some()) + usize::from(!companion),
        )?;
        out.serialize_field("workspaceId", &self.workspace_id)?;
        out.serialize_field("action", &self.action)?;
        if let Some(files) = &self.files {
            out.serialize_field("files", files)?;
        }
        if !companion {
            out.serialize_field("options", &self.options)?;
        }
        out.serialize_field("review", &self.review)?;
        out.end()
    }
}
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NativeReviewOptions {
    #[serde(default)]
    pub stage_unstaged: bool,
    #[serde(default)]
    pub push_after_commit: bool,
    #[serde(default, rename = "createPRAfterPush")]
    pub create_pr_after_push: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NativeReviewChoiceQuery {
    #[serde(deserialize_with = "native_review_root")]
    pub root: crate::RepositoryRootId,
    pub choice: NativeReviewChoice,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_branch: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub push_remote: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub companion: Option<NativeReviewCompanion>,
}
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
pub enum NativeReviewCompanion {
    #[serde(rename = "create-pr")]
    CreatePr {},
}
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum NativeReviewChoice {
    Saved,
    AfterCommit {
        operation_id: String,
        capture_id: String,
    },
    ExplicitTarget {
        #[serde(deserialize_with = "native_review_target")]
        target: crate::RepositoryTarget,
    },
}
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NativeReviewOperationRef {
    pub operation_id: String,
    #[serde(deserialize_with = "native_review_root")]
    pub root: crate::RepositoryRootId,
}
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NativeReviewExecuteQuery {
    pub workspace_id: crate::WorkspaceId,
    pub action: crate::NativeReviewStage,
    pub review: NativeReviewOperationRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pr_title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pr_body: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NativeReviewBoundQuery {
    pub workspace_id: crate::WorkspaceId,
    pub operation_id: String,
    #[serde(deserialize_with = "native_review_root")]
    pub root: crate::RepositoryRootId,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NativeReviewFrame {
    Prepare(NativeReviewPrepareQuery),
    Execute(NativeReviewExecuteQuery),
    Reconcile(NativeReviewBoundQuery),
    Release(NativeReviewBoundQuery),
}
impl NativeReviewFrame {
    #[must_use]
    pub fn root(&self) -> &crate::RepositoryRootId {
        match self {
            Self::Prepare(q) => &q.review.root,
            Self::Execute(q) => &q.review.root,
            Self::Reconcile(q) | Self::Release(q) => &q.root,
        }
    }
    #[must_use]
    pub fn workspace(&self) -> &crate::WorkspaceId {
        match self {
            Self::Prepare(q) => &q.workspace_id,
            Self::Execute(q) => &q.workspace_id,
            Self::Reconcile(q) | Self::Release(q) => &q.workspace_id,
        }
    }
}
#[derive(Clone, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeReviewOperationCapture {
    pub operation_id: String,
    pub root: crate::RepositoryRootId,
    pub retirement_sequence: String,
    pub expires_after_ms: u64,
}
#[derive(Clone, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeReviewRetired {
    pub operation_ids: Vec<String>,
    pub sequence: String,
    pub all_retired: bool,
    pub terminal: bool,
}
pub trait NativeReviewRetirements: Send {
    fn next(&mut self) -> BoxFuture<'_, Option<NativeReviewRetired>>;
}

fn native_review_root<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<crate::RepositoryRootId, D::Error> {
    use serde::Deserialize as _;
    #[derive(serde::Deserialize)]
    #[serde(
        tag = "kind",
        rename_all = "kebab-case",
        rename_all_fields = "camelCase",
        deny_unknown_fields
    )]
    enum Root {
        Primary {
            workspace_id: crate::WorkspaceId,
        },
        Registered {
            workspace_id: crate::WorkspaceId,
            git_root_id: crate::WorkspaceGitRootId,
        },
    }
    Ok(match Root::deserialize(deserializer)? {
        Root::Primary { workspace_id } => crate::RepositoryRootId {
            workspace_id,
            kind: crate::RepositoryRootKind::Primary,
        },
        Root::Registered {
            workspace_id,
            git_root_id,
        } => crate::RepositoryRootId {
            workspace_id,
            kind: crate::RepositoryRootKind::Registered { git_root_id },
        },
    })
}
fn native_review_target<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<crate::RepositoryTarget, D::Error> {
    use serde::Deserialize as _;
    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase", deny_unknown_fields)]
    struct Target {
        provider: crate::RepositoryProvider,
        instance_base_url: String,
        project_path: String,
    }
    let Target {
        provider,
        instance_base_url,
        project_path,
    } = Target::deserialize(deserializer)?;
    Ok(crate::RepositoryTarget {
        provider,
        instance_base_url,
        project_path,
    })
}

/// Explicit provider reads have their own original-socket lifetime. These
/// queries carry identities, never a credential or permission grant.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RepositoryResourceQuery {
    pub workspace_id: crate::WorkspaceId,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RepositoryResourceBoundQuery {
    pub workspace_id: crate::WorkspaceId,
    #[serde(deserialize_with = "resource_lifetime")]
    pub read_lifetime_id: String,
}

fn resource_lifetime<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> std::result::Result<String, D::Error> {
    let value = <String as serde::Deserialize>::deserialize(d)?;
    if value.is_empty() || value.len() > 128 || value.chars().any(char::is_control) {
        return Err(serde::de::Error::custom("invalid read lifetime"));
    }
    Ok(value)
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RepositoryResourceDetailQuery {
    pub workspace_id: crate::WorkspaceId,
    #[serde(deserialize_with = "resource_lifetime")]
    pub read_lifetime_id: String,
    #[serde(deserialize_with = "resource_target")]
    pub target: crate::ReviewTarget,
    /// Omission means false; explicit null is rejected.
    #[serde(default)]
    pub refresh: bool,
}

fn resource_target<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> std::result::Result<crate::ReviewTarget, D::Error> {
    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase", deny_unknown_fields)]
    struct Repository {
        provider: crate::RepositoryProvider,
        instance_base_url: String,
        project_path: String,
    }
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Target {
        repository: Repository,
        kind: crate::RepositoryResourceKind,
        number: u64,
    }
    let target = <Target as serde::Deserialize>::deserialize(d)?;
    if target.repository.provider != crate::RepositoryProvider::Gitlab
        || !matches!(
            target.kind,
            crate::RepositoryResourceKind::MergeRequest | crate::RepositoryResourceKind::Issue
        )
        || !(1..=9_007_199_254_740_991).contains(&target.number)
        || target.repository.instance_base_url.len() > 2048
        || target.repository.project_path.len() > 1024
    {
        return Err(serde::de::Error::custom("invalid explicit GitLab resource"));
    }
    Ok(crate::ReviewTarget {
        repository: crate::RepositoryTarget {
            provider: target.repository.provider,
            instance_base_url: target.repository.instance_base_url,
            project_path: target.repository.project_path,
        },
        kind: target.kind,
        number: target.number,
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RepositoryResourceFrame {
    Capture(RepositoryResourceQuery),
    Detail(RepositoryResourceDetailQuery),
    Release(RepositoryResourceBoundQuery),
}
impl RepositoryResourceFrame {
    #[must_use]
    pub fn workspace_id(&self) -> &crate::WorkspaceId {
        match self {
            Self::Capture(q) => &q.workspace_id,
            Self::Detail(q) => &q.workspace_id,
            Self::Release(q) => &q.workspace_id,
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RepositoryResourceInstance {
    pub provider: crate::RepositoryProvider,
    pub instance_base_url: String,
    pub availability: crate::RepositoryAvailability,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RepositoryResourceCapture {
    pub read_lifetime_id: String,
    pub scope: crate::ExecutionScope,
    pub revision: crate::RepositoryContextRevision,
    pub expires_after_ms: u64,
    pub retirement_sequence: String,
    pub instances: Vec<RepositoryResourceInstance>,
}

/// Decimal strings retain the provider's complete unsigned quota values;
/// null means the response supplied no corresponding observation.
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RepositoryResourceQuota {
    pub reset_at: Option<String>,
    pub remaining: Option<String>,
    pub limit: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RepositoryResourceFailure {
    Authentication,
    ProjectDenied,
    ResourceDenied,
    OptionalRestricted,
    OptionalUnavailable,
    RateLimited,
    Transient,
    Unavailable,
    Unknown,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum RepositoryResourceOutcome {
    /// The existing qualified snapshot projection, including details and
    /// per-signal availability. Unknown counts stay null or absent.
    MergeRequest { snapshot: serde_json::Value },
    /// The existing source-control Issue serialization, with no inferred fields.
    Issue { issue: serde_json::Value },
    /// Sanitized actual failure evidence, never a cached payload or grant.
    Failure {
        code: RepositoryResourceFailure,
        status: Option<u16>,
    },
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RepositoryResourceResult {
    pub read_lifetime_id: String,
    pub scope: crate::ExecutionScope,
    pub revision: crate::RepositoryContextRevision,
    pub target: crate::ReviewTarget,
    pub outcome: RepositoryResourceOutcome,
    pub quota: RepositoryResourceQuota,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RepositoryResourceRetired {
    pub read_lifetime_ids: Vec<String>,
    pub sequence: String,
    pub all_retired: bool,
    pub terminal: bool,
}

pub trait RepositoryResourceRetirements: Send {
    fn next(&mut self) -> BoxFuture<'_, Option<RepositoryResourceRetired>>;
}
