//! Additive native review preparation and result contracts.
//!
//! These are observations and correlation data, not commands or authority.
//! Nothing here resolves a repository, inspects Git, authorizes a caller,
//! persists a receipt or retries a mutation. Services must capture and
//! revalidate admission, root/ref/project, transport and account bindings.
//!
//! The optional extensions leave existing accept-changes fields and actions
//! intact. Execution failures (including uncertain writes) belong in the
//! resolved response with its legacy string error and completed results.
//! API success does not establish that a local commit was published.

use serde::{Deserialize, Serialize};

use crate::{
    ExecutionScope, RepositoryConnectionScope, RepositoryContextRevision, RepositoryProvider,
    RepositoryRootId, RepositoryTarget, ReviewTarget,
};

/// Optional addition to the existing native prepare response.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeReviewPrepareExtension {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub review_preparation: Option<NativeReviewPreparation>,
}

/// Optional addition to the resolved native execute response, including failure.
///
/// Existing success/steps/result/error fields remain owned by their current
/// producer. In particular, this extension neither creates URL aliases nor
/// discards a result because success is false.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeReviewExecuteExtension {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub review_execution: Option<NativeReviewExecution>,
}

/// A captured operation context, never a client-supplied permission grant.
///
/// operationId and worktreeId are opaque server correlation identifiers, not
/// bearer capabilities. Every mutating stage still requires current admission.
/// The source/target connections are separate from inventory authority. Unknown
/// observations stay null; serializing this value does not confirm them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeReviewPreparation {
    pub operation_id: String,
    pub scope: ExecutionScope,
    pub context_revision: RepositoryContextRevision,
    pub root: RepositoryRootId,
    pub worktree_id: String,
    pub source: NativeReviewBranchTarget,
    pub target: NativeReviewBranchTarget,
    pub local_head_sha: Option<String>,
    pub transport: Option<NativeReviewTransport>,
}

/// Selected project/branch and its own connection, not provider review metadata.
///
/// A provider ID is copied directly to a string in Rust, without a JavaScript
/// numeric roundtrip. It remains null until corroborated by the provider.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeReviewBranchTarget {
    pub repository: RepositoryTarget,
    pub provider_project_id: Option<String>,
    pub connection: Option<RepositoryConnectionScope>,
    pub branch: String,
}

/// Sanitized, observed transport destinations; fetch does not imply push.
///
/// The producer must strip credentials before projection and must revalidate
/// destinations before a mutation. These strings do not choose an account or
/// override the selected project's authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeReviewTransport {
    pub remote_name: String,
    /// All observed destinations; selecting the first would lose a push binding.
    pub fetch_urls: Vec<String>,
    pub push_urls: Vec<String>,
}

/// Receipt for exactly one execute request.
///
/// Sidebar commit and create are separate requests: retain both executions.
/// The create response must not claim the preceding request's commit. A
/// single explicit pipeline may include its own completed commit/push receipts.
/// Only a producer's completed commit receipt can advance its captured HEAD;
/// neither a submitted SHA nor this deserialized structure proves completion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeReviewExecution {
    pub request_id: String,
    pub preparation: NativeReviewPreparation,
    pub git_receipts: Vec<NativeReviewGitReceipt>,
    pub outcome: NativeReviewOutcome,
    pub publication: NativeReviewPublication,
}

/// Only completed Git stages. The review outcome is the API-stage receipt.
///
/// Existing step status/progress messages stay in the legacy steps array.
/// An absent receipt means no completion evidence, not evidence of no effect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "stage",
    rename_all = "kebab-case",
    rename_all_fields = "camelCase"
)]
pub enum NativeReviewGitReceipt {
    Commit { commit_hash: String },
    Push { pushed_sha: String },
}

/// Existing native stage spellings; no additional action or MCP method.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NativeReviewStage {
    Commit,
    Push,
    CreatePr,
}

/// Review creation outcome, independent of local commit publication.
///
/// Created/reused require an actual provider observation. Unknown write
/// completion is uncertain, never a guessed successful review or a promise
/// that retry is safe. Services must reconcile it before another write.
/// `NotAttempted` covers successful commit/push-only calls as well as a
/// preparation that has not attempted creation; it does not imply failure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "kebab-case")]
pub enum NativeReviewOutcome {
    NotAttempted,
    Created {
        review: Box<NativeReviewDetails>,
    },
    Reused {
        review: Box<NativeReviewDetails>,
    },
    Failed {
        stage: NativeReviewStage,
        code: Option<String>,
        message: String,
    },
    Uncertain {
        stage: NativeReviewStage,
        message: String,
    },
}

/// Actual review observation; never filled from submitted title/body or HEAD.
///
/// This is a core projection of provider `ReviewDetails`, avoiding a dependency
/// from core back to intent-sourcecontrol. Nullable fields are deliberate:
/// legacy provider defaults (notably draft=false or locked->Open) are not
/// confirmation. Keep those fields null without a faithful provider signal.
/// Resource and URL identify the observed review, not an unverified match.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeReviewDetails {
    pub resource: ReviewTarget,
    pub url: String,
    pub title: String,
    pub body: Option<String>,
    pub state: Option<NativeReviewState>,
    pub draft: Option<bool>,
    pub source_branch: Option<String>,
    pub target_branch: Option<String>,
    pub source: Option<NativeReviewBranchIdentity>,
    pub target: Option<NativeReviewBranchIdentity>,
    pub author: Option<String>,
    pub mergeable: Option<bool>,
    pub mergeable_state: Option<String>,
    pub head_sha: Option<String>,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
}

/// Faithful provider state, distinct from the unchanged legacy PR projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NativeReviewState {
    Open,
    Locked,
    Closed,
    Merged,
}

/// Provider-confirmed branch identity. Missing identity is null as a whole.
///
/// Project paths may be unavailable even with a confirmed numeric provider ID.
/// A same-named branch or selected request target cannot fill a missing identity.
/// The string project ID preserves all provider bits without JSON u64 rounding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeReviewBranchIdentity {
    pub provider: RepositoryProvider,
    pub instance_base_url: String,
    pub project_id: String,
    pub project_path: Option<String>,
    pub branch: String,
}

/// Producer-supplied publication evidence for the captured source project/ref.
///
/// This module does not compute ancestry. Included requires confirmed equality
/// or ancestry; different SHAs alone cannot distinguish ahead from diverged.
/// A provider review head SHA, completed push, or successful create by itself
/// cannot prove this relationship. Unobserved/missing remote branches differ.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "state",
    rename_all = "kebab-case",
    rename_all_fields = "camelCase"
)]
pub enum NativeReviewPublication {
    Included {
        local_head_sha: String,
        remote_source_sha: String,
    },
    LocalAhead {
        local_head_sha: String,
        remote_source_sha: String,
    },
    Diverged {
        local_head_sha: String,
        remote_source_sha: String,
    },
    RemoteBranchMissing {
        local_head_sha: Option<String>,
    },
    Unknown {
        local_head_sha: Option<String>,
        remote_source_sha: Option<String>,
    },
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Map, Value};

    use super::*;

    // Model only the additive seam. The actual accept-changes payload remains
    // owned by services; this does not implement or authorize that producer.
    #[derive(Debug, Serialize, Deserialize)]
    struct PrepareEnvelope {
        #[serde(flatten)]
        extension: NativeReviewPrepareExtension,
        #[serde(flatten)]
        legacy: Map<String, Value>,
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct ExecuteEnvelope {
        #[serde(flatten)]
        extension: NativeReviewExecuteExtension,
        #[serde(flatten)]
        legacy: Map<String, Value>,
    }

    fn fixture() -> Value {
        serde_json::from_str(include_str!("../tests/fixtures/native_review_v1.json")).unwrap()
    }

    fn execution() -> NativeReviewExecution {
        serde_json::from_value(fixture()["execute"]["reviewExecution"].clone()).unwrap()
    }

    fn review() -> Box<NativeReviewDetails> {
        match execution().outcome {
            NativeReviewOutcome::Reused { review } => review,
            other => panic!("expected reused fixture, got {other:?}"),
        }
    }

    fn envelope(receipt: NativeReviewExecution, success: bool) -> Value {
        let mut value = fixture()["execute"].clone();
        value["success"] = json!(success);
        value["reviewExecution"] = serde_json::to_value(receipt).unwrap();
        value
    }

    #[test]
    fn canonical_fixture_roundtrips_without_changing_legacy_fields() {
        let fixture = fixture();
        let prepare: PrepareEnvelope = serde_json::from_value(fixture["prepare"].clone()).unwrap();
        let execute: ExecuteEnvelope = serde_json::from_value(fixture["execute"].clone()).unwrap();
        assert_eq!(serde_json::to_value(prepare).unwrap(), fixture["prepare"]);
        assert_eq!(serde_json::to_value(execute).unwrap(), fixture["execute"]);
        assert!(fixture["execute"]["result"].get("prHtmlUrl").is_none());
        assert!(fixture["execute"]["result"].get("existingPR").is_none());
    }

    #[test]
    fn omitted_extensions_preserve_old_success_failure_and_non_create_results() {
        for value in [
            json!({
                "success": true,
                "steps": [{"id":"commit", "name":"Commit changes", "status":"completed",
                           "message":"Committed B"}],
                "result": {"commitHash":"B", "pushedSha":"A"}
            }),
            json!({
                "success": false,
                "steps": [{"id":"push", "name":"Push branch", "status":"failed",
                           "error":"github authentication required"}],
                "result": {"commitHash":"B"},
                "error": "github authentication required"
            }),
            json!({
                "success": true, "steps": [],
                "result": {
                    "prNumber": 7, "prUrl":"api-url", "prHtmlUrl":"browser-url",
                    "existingPR":true, "mergeCommitHash":"M", "autoRebased":true,
                    "newHeadSha":"H", "newBaseSha":"T", "futureField":{"kept":true}
                }
            }),
            json!({"success":false, "steps":[], "error":"Response lost"}),
        ] {
            let parsed: ExecuteEnvelope = serde_json::from_value(value.clone()).unwrap();
            assert!(parsed.extension.review_execution.is_none());
            assert_eq!(serde_json::to_value(parsed).unwrap(), value);
        }
        let mut prepare = fixture()["prepare"].clone();
        prepare.as_object_mut().unwrap().remove("reviewPreparation");
        let parsed: PrepareEnvelope = serde_json::from_value(prepare.clone()).unwrap();
        assert!(parsed.extension.review_preparation.is_none());
        assert_eq!(serde_json::to_value(parsed).unwrap(), prepare);
        assert_eq!(
            serde_json::to_value(NativeReviewExecuteExtension::default()).unwrap(),
            json!({})
        );
    }

    #[test]
    fn created_review_at_remote_a_does_not_claim_local_b_was_pushed() {
        let mut receipt = execution();
        let mut created = review();
        created.draft = Some(false);
        created.title = "Provider confirmed title".into();
        receipt.outcome = NativeReviewOutcome::Created { review: created };
        let wire = serde_json::to_value(receipt).unwrap();
        assert_eq!(wire["outcome"]["status"], "created");
        assert_eq!(wire["outcome"]["review"]["headSha"], "remote-A");
        assert_eq!(wire["preparation"]["localHeadSha"], "local-B");
        assert_eq!(wire["publication"]["state"], "local-ahead");
        assert_eq!(wire["gitReceipts"], json!([]));
    }

    #[test]
    fn reused_metadata_stays_provider_supplied_including_draft_and_timestamps() {
        let actual = review();
        assert_ne!(
            json!(actual.title),
            fixture()["prepare"]["suggestedPRTitle"]
        );
        assert_ne!(json!(actual.body), fixture()["prepare"]["suggestedPRBody"]);
        assert_eq!(actual.draft, Some(true));
        assert_eq!(actual.state, Some(NativeReviewState::Open));
        assert_eq!(actual.created_at.as_deref(), Some("2026-09-26T10:00:00Z"));
        assert_eq!(actual.updated_at.as_deref(), Some("2026-09-27T10:00:00Z"));
        assert_eq!(actual.source.unwrap().project_path, None);
        assert_eq!(actual.head_sha.as_deref(), Some("remote-A"));
    }

    #[test]
    fn missing_or_legacy_defaulted_metadata_remains_explicitly_unknown() {
        let mut actual = review();
        actual.state = None;
        actual.draft = None;
        actual.body = None;
        actual.source_branch = None;
        actual.target_branch = None;
        actual.source = None;
        actual.target = None;
        actual.author = None;
        actual.head_sha = None;
        actual.created_at = None;
        actual.updated_at = None;
        let wire = serde_json::to_value(&actual).unwrap();
        for field in [
            "state",
            "draft",
            "body",
            "sourceBranch",
            "targetBranch",
            "source",
            "target",
            "author",
            "headSha",
            "createdAt",
            "updatedAt",
            "mergeable",
            "mergeableState",
        ] {
            assert_eq!(wire.get(field), Some(&Value::Null), "{field}");
        }
        let decoded: NativeReviewDetails = serde_json::from_value(wire).unwrap();
        assert_eq!(*actual, decoded);
    }

    #[test]
    fn locked_closed_and_merged_are_not_rewritten_to_open() {
        for (state, spelling) in [
            (NativeReviewState::Locked, "locked"),
            (NativeReviewState::Closed, "closed"),
            (NativeReviewState::Merged, "merged"),
        ] {
            let mut actual = review();
            actual.state = Some(state);
            let wire = serde_json::to_value(&actual).unwrap();
            assert_eq!(wire["state"], spelling);
            assert_eq!(
                serde_json::from_value::<NativeReviewDetails>(wire)
                    .unwrap()
                    .state,
                Some(state)
            );
        }
    }

    #[test]
    fn uncertain_write_retains_completed_git_stages_without_inventing_a_review() {
        let mut receipt = execution();
        receipt.git_receipts = vec![
            NativeReviewGitReceipt::Commit {
                commit_hash: "local-B".into(),
            },
            NativeReviewGitReceipt::Push {
                pushed_sha: "local-B".into(),
            },
        ];
        receipt.outcome = NativeReviewOutcome::Uncertain {
            stage: NativeReviewStage::CreatePr,
            message: "Create response was lost; reconcile before another write".into(),
        };
        receipt.publication = NativeReviewPublication::Unknown {
            local_head_sha: Some("local-B".into()),
            remote_source_sha: None,
        };
        let mut value = envelope(receipt, false);
        value["result"] = json!({"commitHash":"local-B", "pushedSha":"local-B"});
        value["steps"] = json!([
            {"id":"commit", "name":"Commit changes", "status":"completed"},
            {"id":"push", "name":"Push branch", "status":"completed"},
            {"id":"create-pr", "name":"Create pull request", "status":"failed",
             "error":"Response lost"}
        ]);
        value["error"] = json!("Response lost");
        let parsed: ExecuteEnvelope = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(serde_json::to_value(parsed).unwrap(), value);
        let outcome = &value["reviewExecution"]["outcome"];
        assert_eq!(outcome["status"], "uncertain");
        assert!(outcome.get("review").is_none());
        assert!(value["result"].get("prNumber").is_none());
        assert_eq!(
            value["reviewExecution"]["gitReceipts"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        assert_eq!(value["reviewExecution"]["publication"]["state"], "unknown");
    }

    #[test]
    fn failed_push_keeps_only_the_completed_commit_and_legacy_error_string() {
        let mut receipt = execution();
        receipt.git_receipts = vec![NativeReviewGitReceipt::Commit {
            commit_hash: "local-B".into(),
        }];
        receipt.outcome = NativeReviewOutcome::Failed {
            stage: NativeReviewStage::Push,
            code: None,
            message: "github authentication required".into(),
        };
        let mut value = envelope(receipt, false);
        value["result"] = json!({"commitHash":"local-B"});
        value["steps"] = json!([
            {"id":"commit", "name":"Commit changes", "status":"completed"},
            {"id":"push", "name":"Push branch", "status":"failed",
             "error":"github authentication required"}
        ]);
        value["error"] = json!("github authentication required");
        let parsed: ExecuteEnvelope = serde_json::from_value(value.clone()).unwrap();
        let output = serde_json::to_value(parsed).unwrap();
        assert_eq!(output, value);
        assert_eq!(
            output["reviewExecution"]["gitReceipts"],
            json!([{"stage":"commit", "commitHash":"local-B"}])
        );
        assert_eq!(output["reviewExecution"]["outcome"]["code"], Value::Null);
        assert!(output["result"].get("pushedSha").is_none());
    }

    #[test]
    fn sidebar_commit_and_failed_create_keep_separate_request_receipts() {
        let mut commit = execution();
        commit.request_id = "request-commit".into();
        commit.preparation.operation_id = "operation-commit".into();
        commit.preparation.local_head_sha = Some("local-A".into());
        commit.git_receipts = vec![NativeReviewGitReceipt::Commit {
            commit_hash: "local-B".into(),
        }];
        commit.outcome = NativeReviewOutcome::NotAttempted;
        commit.publication = NativeReviewPublication::Unknown {
            local_head_sha: Some("local-B".into()),
            remote_source_sha: None,
        };

        let mut create = execution();
        create.outcome = NativeReviewOutcome::Failed {
            stage: NativeReviewStage::CreatePr,
            code: None,
            message: "The remote source branch is absent".into(),
        };
        create.publication = NativeReviewPublication::RemoteBranchMissing {
            local_head_sha: Some("local-B".into()),
        };
        let history = serde_json::to_value(vec![commit, create]).unwrap();
        assert_eq!(history[0]["requestId"], "request-commit");
        assert_eq!(history[0]["outcome"]["status"], "not-attempted");
        assert_eq!(history[0]["gitReceipts"][0]["commitHash"], "local-B");
        assert_eq!(history[1]["requestId"], "request-create");
        assert_eq!(history[1]["preparation"]["localHeadSha"], "local-B");
        assert_eq!(history[1]["gitReceipts"], json!([]));
        assert_eq!(history[1]["outcome"]["status"], "failed");
        assert_ne!(
            history[0]["preparation"]["operationId"],
            history[1]["preparation"]["operationId"]
        );
    }

    #[test]
    fn absent_remote_branch_and_unobserved_remote_are_different_facts() {
        let missing = NativeReviewPublication::RemoteBranchMissing {
            local_head_sha: Some("local-B".into()),
        };
        let unknown = NativeReviewPublication::Unknown {
            local_head_sha: Some("local-B".into()),
            remote_source_sha: None,
        };
        assert_eq!(
            serde_json::to_value(&missing).unwrap(),
            json!({"state":"remote-branch-missing", "localHeadSha":"local-B"})
        );
        assert_eq!(
            serde_json::to_value(&unknown).unwrap(),
            json!({"state":"unknown", "localHeadSha":"local-B", "remoteSourceSha":null})
        );
        assert_ne!(missing, unknown);
    }

    #[test]
    fn publication_preserves_supplied_ancestry_without_guessing_from_sha_difference() {
        for state in ["included", "local-ahead", "diverged"] {
            let value = json!({
                "state": state, "localHeadSha":"local-B", "remoteSourceSha":"remote-A"
            });
            let typed: NativeReviewPublication = serde_json::from_value(value.clone()).unwrap();
            assert_eq!(serde_json::to_value(typed).unwrap(), value);
        }
        let unborn: NativeReviewPublication = serde_json::from_value(json!({
            "state":"unknown", "localHeadSha":null, "remoteSourceSha":null
        }))
        .unwrap();
        assert_eq!(
            unborn,
            NativeReviewPublication::Unknown {
                local_head_sha: None,
                remote_source_sha: None
            }
        );
    }

    #[test]
    fn captured_bindings_preserve_independent_connections_and_full_counter_precision() {
        let mut receipt = execution();
        let target = receipt.preparation.target.connection.as_mut().unwrap();
        target.connection_id = "target-connection".into();
        target.account_id = "target-account".into();
        target.connection_generation = 9_007_199_254_740_994;
        let wire = serde_json::to_value(receipt).unwrap();
        let prep = &wire["preparation"];
        assert_eq!(prep["scope"]["authorityGeneration"], "9007199254740995");
        assert_eq!(prep["contextRevision"]["sequence"], "9007199254740993");
        assert_eq!(
            prep["source"]["connection"]["connectionGeneration"],
            "18446744073709551615"
        );
        assert_eq!(
            prep["target"]["connection"]["connectionGeneration"],
            "9007199254740994"
        );
        assert_ne!(prep["source"]["connection"], prep["target"]["connection"]);
        assert_eq!(prep["source"]["providerProjectId"], "18446744073709551615");
        assert_eq!(prep["scope"]["daemonId"], "daemon-A");
    }

    #[test]
    fn transport_mismatch_and_unknown_connection_are_not_filled_from_fetch() {
        let mut prep = execution().preparation;
        let transport = prep.transport.as_mut().unwrap();
        transport
            .push_urls
            .push("ssh://git@github.com/another/project.git".into());
        prep.source.connection = None;
        prep.source.provider_project_id = None;
        let wire = serde_json::to_value(&prep).unwrap();
        assert_ne!(
            wire["transport"]["fetchUrls"],
            wire["transport"]["pushUrls"]
        );
        assert_eq!(wire["transport"]["pushUrls"].as_array().unwrap().len(), 2);
        assert_eq!(wire["source"]["connection"], Value::Null);
        assert_eq!(wire["source"]["providerProjectId"], Value::Null);
        prep.transport = None;
        prep.local_head_sha = None;
        let wire = serde_json::to_value(prep).unwrap();
        assert_eq!(wire["transport"], Value::Null);
        assert_eq!(wire["localHeadSha"], Value::Null);
    }

    #[test]
    fn confirmed_outcomes_require_actual_review_data_and_project_ids_stay_strings() {
        for status in ["created", "reused"] {
            assert!(
                serde_json::from_value::<NativeReviewOutcome>(json!({"status":status})).is_err()
            );
        }
        let value = serde_json::to_value(review()).unwrap();
        assert_eq!(value["source"]["projectId"], "18446744073709551615");
        assert_eq!(value["source"]["projectPath"], Value::Null);
        let mut invalid = value;
        invalid["source"]["projectId"] = json!(42);
        assert!(serde_json::from_value::<NativeReviewDetails>(invalid).is_err());
    }

    #[test]
    fn github_and_gitlab_share_the_contract_without_changing_resource_kind() {
        let mut actual = review();
        actual.resource.repository = RepositoryTarget {
            provider: RepositoryProvider::Github,
            instance_base_url: "https://github.com".into(),
            project_path: "team/app".into(),
        };
        actual.resource.kind = crate::RepositoryResourceKind::PullRequest;
        actual.url = "https://github.com/team/app/pull/7".into();
        actual.source = None; // Legacy provider lacks confirmed project identities.
        actual.target = None;
        let wire = serde_json::to_value(&actual).unwrap();
        assert_eq!(wire["resource"]["kind"], "pull-request");
        assert_eq!(wire["resource"]["repository"]["provider"], "github");
        assert_eq!(wire["source"], Value::Null);
        assert_eq!(wire["target"], Value::Null);
        assert_eq!(
            fixture()["execute"]["reviewExecution"]["outcome"]["review"]["resource"]["kind"],
            "merge-request"
        );
        assert_eq!(
            serde_json::from_value::<NativeReviewDetails>(wire).unwrap(),
            *actual
        );
    }
}
