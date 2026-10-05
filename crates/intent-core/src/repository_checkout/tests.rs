use super::*;
use serde_json::json;

#[test]
fn checkout_queries_cannot_supply_workspace_or_credential_authority() {
    for key in [
        "workspaceId",
        "principalId",
        "token",
        "cachePath",
        "connectionId",
    ] {
        let mut value = json!({"checkoutId":"original", "revision":"original"});
        value[key] = json!("untrusted");
        assert!(
            serde_json::from_value::<CheckoutProjectsQuery>(value).is_err(),
            "{key}"
        );
    }
    let selection = json!({"checkoutId":"original","revision":"original","projectPath":"team/sub/app","branch":"release/next","commitSha":"0123456789012345678901234567890123456789","mode":"cached"});
    assert_eq!(
        serde_json::from_value::<CheckoutSelection>(selection.clone())
            .unwrap()
            .branch,
        "release/next"
    );
    for key in ["branch", "commitSha", "revision"] {
        let mut value = selection.clone();
        value.as_object_mut().unwrap().remove(key);
        assert!(
            serde_json::from_value::<CheckoutSelection>(value).is_err(),
            "{key}"
        );
    }
}

#[test]
fn checkout_missing_default_stays_absent_and_failure_contains_no_payload() {
    let project = CheckoutProject {
        project_path: "team/app".into(),
        name: "app".into(),
        namespace: "team".into(),
        web_url: "https://forge.test/root/team/app".into(),
        clone_url: "https://forge.test/root/team/app.git".into(),
        default_branch: None,
    };
    let value = serde_json::to_value(project).unwrap();
    assert!(value.get("defaultBranch").is_none());
    let result: CheckoutResult<CheckoutProjects> =
        CheckoutResult::unavailable(CheckoutUnavailable::AccessDenied);
    assert_eq!(
        serde_json::to_value(result).unwrap(),
        json!({"status":"unavailable","reason":"access-denied"})
    );
}
