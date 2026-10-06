//! Fixed pre-workspace configuration under the original checkout authority.
use super::{
    create, entry, failure, split_project, BoxFuture, CheckoutFrame, CheckoutMode, CheckoutResult,
    CheckoutSelection, CheckoutUnavailable, PageParams, Result, Services, SourceControl,
    READ_LIMIT,
};
use intent_core::repository_checkout::{CheckoutRepoConfig, CheckoutRepoConfigQuery};

pub(crate) fn read(
    services: &Services,
    q: CheckoutRepoConfigQuery,
) -> BoxFuture<'_, Result<CheckoutResult<CheckoutRepoConfig>>> {
    entry(services, &CheckoutFrame::RepoConfig(q.clone()), move |r| {
        Box::pin(async move {
            let (lease, connection) = r.bind(&q.checkout_id, &q.revision).await?;
            let selection = CheckoutSelection {
                checkout_id: q.checkout_id.clone(),
                revision: q.revision.clone(),
                project_path: q.project_path.clone(),
                branch: q.branch.clone(),
                commit_sha: q.commit_sha.clone(),
                mode: CheckoutMode::Direct,
            };
            create::checked_selection(&lease, &selection)?;
            let (owner, name) = split_project(&q.project_path)?;
            r.private_projects(vec![q.project_path.clone()])?;
            let provider = match connection.provider() {
                Ok(provider) => provider,
                Err(error) => return failure(&r, &error),
            };
            let repo = intent_core::RepoRef::new(owner, name);
            let result = tokio::time::timeout(READ_LIMIT, async {
                let original = provider.checkout_project_identity(&repo).await?;
                if !branch_matches(&provider, owner, name, &q).await? {
                    return Ok(None);
                }
                let config = match provider.checkout_repo_config(&repo, &q.commit_sha).await {
                    Ok(content) => content,
                    // As with github.repoConfig.get, confirmed present but
                    // undecodable content is an empty config, not an absent file.
                    Err(intent_sourcecontrol::Error::Decode(_)) => Some("{}".into()),
                    Err(error) => return Err(error),
                };
                if provider.checkout_project_identity(&repo).await? != original {
                    return Err(intent_sourcecontrol::Error::AdmissionRetired);
                }
                if !branch_matches(&provider, owner, name, &q).await? {
                    return Ok(None);
                }
                Ok(Some(config))
            })
            .await;
            let config = match result {
                Ok(Ok(Some(config))) => config,
                Ok(Ok(None)) => {
                    return Ok(CheckoutResult::unavailable(
                        CheckoutUnavailable::BranchChanged,
                    ));
                }
                Ok(Err(error)) => return failure(&r, &error),
                Err(_) => {
                    r.public()?;
                    return Ok(CheckoutResult::unavailable(
                        CheckoutUnavailable::Unreachable,
                    ));
                }
            };
            r.validate(&lease).await?;
            connection.with_project_current(&q.project_path, &mut || Ok(()))?;
            // A concurrent branch page must not replace the selected observation.
            create::checked_selection(&lease, &selection)?;
            let exists = config.is_some();
            let config = config
                .map(|text| crate::repo_config::parse_repo_config_tolerant(&text, &q.project_path));
            Ok(CheckoutResult::Ready {
                value: CheckoutRepoConfig {
                    project_path: q.project_path,
                    branch: q.branch,
                    commit_sha: q.commit_sha,
                    config,
                    exists,
                },
            })
        })
    })
}

async fn branch_matches(
    provider: &intent_sourcecontrol::GitLabSourceControl,
    owner: &str,
    name: &str,
    q: &CheckoutRepoConfigQuery,
) -> intent_sourcecontrol::Result<bool> {
    // GitLab search's trailing $ matches the end; verify both name and SHA.
    let page = provider
        .list_remote_branches(
            owner,
            name,
            Some(&format!("{}$", q.branch)),
            PageParams::first(100),
        )
        .await?;
    Ok(page.items.iter().any(|branch| {
        branch.name == q.branch && branch.commit_sha.as_deref() == Some(&q.commit_sha)
    }))
}
