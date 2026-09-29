use super::*;
use crate::WorkspaceApi;

struct OrdinaryApi;
impl WorkspaceApi for OrdinaryApi {}

#[test]
fn existing_api_has_no_repository_read_connection_for_either_entry() {
    let api: &dyn WorkspaceApi = &OrdinaryApi;
    for entry in [
        RepositoryWireEntry::Bearer,
        RepositoryWireEntry::AdmittedLocal,
    ] {
        assert!(api.repository_read_connection(entry).is_none());
    }
}

#[test]
fn native_review_strict_frame_keeps_root_plan_and_rejects_injected_authority() {
    use super::{NativeReviewBoundQuery, NativeReviewExecuteQuery, NativeReviewPrepareQuery};
    let base = serde_json::json!({"workspaceId":"w","action":"create-pr","review":{"root":{"workspaceId":"w","kind":"primary"},"choice":{"kind":"saved"},"targetBranch":"trunk"}});
    let decoded: NativeReviewPrepareQuery = serde_json::from_value(base.clone()).unwrap();
    assert_eq!(decoded.review.root.workspace_id.as_str(), "w");
    for key in ["caller", "provider", "account", "repositoryLifetimeId"] {
        let mut extra = base.clone();
        extra[key] = serde_json::json!("injected");
        assert!(serde_json::from_value::<NativeReviewPrepareQuery>(extra).is_err());
    }
    let mut malformed = base;
    malformed["review"] = serde_json::Value::Null;
    assert!(serde_json::from_value::<NativeReviewPrepareQuery>(malformed).is_err());
    let execute = serde_json::json!({"workspaceId":"w","action":"commit","review":{"operationId":"original","root":{"workspaceId":"w","kind":"registered","gitRootId":"r"}},"commitMessage":"m"});
    assert!(serde_json::from_value::<NativeReviewExecuteQuery>(execute.clone()).is_ok());
    let mut changed = execute;
    changed["files"] = serde_json::json!(["new"]);
    assert!(serde_json::from_value::<NativeReviewExecuteQuery>(changed).is_err());
    assert!(serde_json::from_value::<NativeReviewBoundQuery>(serde_json::json!({"workspaceId":"w","operationId":"original","root":{"workspaceId":"w","kind":"primary"},"retry":true})).is_err());
}

#[test]
fn native_review_nested_root_and_target_never_accept_authority_or_extra_ids() {
    use super::{NativeReviewBoundQuery, NativeReviewPrepareQuery};
    use serde_json::json;
    let base = json!({"workspaceId":"w","action":"create-pr","review":{"root":{"workspaceId":"w","kind":"primary"},"choice":{"kind":"explicitTarget","target":{"provider":"gitlab","instanceBaseUrl":"https://gitlab.test","projectPath":"group/repo"}},"targetBranch":"main"}});
    assert!(serde_json::from_value::<NativeReviewPrepareQuery>(base.clone()).is_ok());
    for key in ["gitRootId", "principalId", "accountId", "generation"] {
        let mut value = base.clone();
        value["review"]["root"][key] = json!("forged");
        assert!(serde_json::from_value::<NativeReviewPrepareQuery>(value).is_err());
    }
    for key in ["accountId", "connection", "providerProjectId"] {
        let mut value = base.clone();
        value["review"]["choice"]["target"][key] = json!("forged");
        assert!(serde_json::from_value::<NativeReviewPrepareQuery>(value).is_err());
    }
    assert!(serde_json::from_value::<NativeReviewBoundQuery>(json!({"workspaceId":"w","operationId":"o","root":{"workspaceId":"w","kind":"registered","gitRootId":"r","principalId":"forged"}})).is_err());
}

#[test]
fn native_review_companion_strict_presence_roundtrip_and_old_forms() {
    use serde_json::{json, Value};
    let root = json!({"workspaceId":"w","kind":"primary"});
    let parent = json!({"workspaceId":"w","action":"commit","review":{"root":root,"choice":{"kind":"saved"},"targetBranch":"trunk","companion":{"kind":"create-pr"}}});
    let child = json!({"workspaceId":"w","action":"create-pr","review":{"root":root,"choice":{"kind":"afterCommit","operationId":"aaaaaaaa-0000-4000-8000-000000000001","captureId":"aaaaaaaa-0000-4000-8000-000000000002"}}});
    for valid in [&parent, &child] {
        let q: NativeReviewPrepareQuery = serde_json::from_value(valid.clone()).unwrap();
        assert_eq!(serde_json::to_value(&q).unwrap(), *valid);
        for key in [
            "files",
            "options",
            "account",
            "localHeadSha",
            "prTitle",
            "commitMessage",
        ] {
            for value in [Value::Null, json!({}), json!([])] {
                let mut bad = valid.clone();
                bad[key] = value;
                assert!(
                    serde_json::from_value::<NativeReviewPrepareQuery>(bad).is_err(),
                    "{key}"
                );
            }
        }
        for key in ["pushRemote", "account", "connection", "localHeadSha"] {
            let mut bad = valid.clone();
            bad["review"][key] = Value::Null;
            assert!(
                serde_json::from_value::<NativeReviewPrepareQuery>(bad).is_err(),
                "{key}"
            );
        }
    }
    for key in ["companion", "targetBranch"] {
        for value in [Value::Null, json!("trunk"), json!({"kind":"create-pr"})] {
            let mut bad = child.clone();
            bad["review"][key] = value;
            assert!(serde_json::from_value::<NativeReviewPrepareQuery>(bad).is_err());
        }
    }
    for key in ["operationId", "captureId"] {
        for value in [
            Value::Null,
            json!(""),
            json!("not-a-uuid"),
            json!(1),
            json!("AAAAAAAA-0000-4000-8000-000000000001"),
            json!("aaaaaaaa000040008000000000000001"),
            json!("urn:uuid:aaaaaaaa-0000-4000-8000-000000000001"),
        ] {
            let mut bad = child.clone();
            bad["review"]["choice"][key] = value;
            assert!(serde_json::from_value::<NativeReviewPrepareQuery>(bad).is_err());
        }
    }
    for action in ["push", "create-pr"] {
        let mut bad = parent.clone();
        bad["action"] = json!(action);
        assert!(serde_json::from_value::<NativeReviewPrepareQuery>(bad).is_err());
    }
    for value in [
        Value::Null,
        json!({"kind":"push"}),
        json!({"kind":"create-pr","extra":true}),
    ] {
        let mut bad = parent.clone();
        bad["review"]["companion"] = value;
        assert!(serde_json::from_value::<NativeReviewPrepareQuery>(bad).is_err());
    }
    let mut old = parent.clone();
    old["review"].as_object_mut().unwrap().remove("companion");
    old["files"] = Value::Null;
    old["review"]["pushRemote"] = Value::Null;
    let decoded: NativeReviewPrepareQuery = serde_json::from_value(old).unwrap();
    let output = serde_json::to_value(decoded).unwrap();
    assert_eq!(
        output["options"],
        json!({"stageUnstaged":false,"pushAfterCommit":false,"createPRAfterPush":false})
    );
    assert!(output["review"].get("companion").is_none());
    assert!(output.get("files").is_none());
    assert!(serde_json::from_str::<NativeReviewPrepareQuery>(
        r#"{"workspaceId":"w","workspaceId":"x","action":"commit","review":{}}"#
    )
    .is_err());
}
