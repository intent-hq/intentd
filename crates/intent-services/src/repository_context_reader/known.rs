//! One explicitly admitted local registry path, without a synthetic workspace.

use super::*;

/// Reuses effective Git configuration and canonical fetch selection. The result
/// is a one-shot identity observation, not a persisted selection or permission.
pub(crate) fn selection(
    path: &Path,
    resolver: &CanonicalRemoteResolver,
    environment: &GitConfigEnvironment,
) -> Result<intent_core::ReviewSelectionResolution> {
    let path = path
        .canonicalize()
        .map_err(|_| invalid("root is unavailable"))?;
    let git = GitRead {
        path: &path,
        environment,
    };
    let root_args = ["rev-parse", "--show-toplevel"];
    let dir_args = ["rev-parse", "--path-format=absolute", "--git-dir"];
    let root = git.required(&root_args)?;
    if Path::new(&root)
        .canonicalize()
        .map_err(|_| invalid("root is unavailable"))?
        != path
    {
        return Err(invalid("known path is not a repository root"));
    }
    let git_dir = git.required(&dir_args)?;
    let before = git.config()?;
    if before.origins.len() > 1_048_576 || before.relevant.len() > 1_048_576 {
        return Err(invalid("repository configuration exceeds read budget"));
    }
    let mut names = BTreeSet::new();
    for (_, key) in nul_pairs(&before.origins)? {
        if key.chars().any(char::is_control) {
            return Err(invalid("control character in config key"));
        }
        if let Some((name, _)) = key.strip_prefix("remote.").and_then(|v| v.rsplit_once('.')) {
            names.insert(name.to_owned());
        }
    }
    if names.len() > 64 {
        return Err(invalid("repository remote count exceeds read budget"));
    }
    let mut configured: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut count = 0;
    for (_, entry) in nul_pairs(&before.relevant)? {
        let (key, value) = entry
            .split_once('\n')
            .ok_or_else(|| invalid("invalid config entry"))?;
        if key.chars().any(char::is_control) || value.chars().any(char::is_control) {
            return Err(invalid("control character in relevant Git config"));
        }
        if let Some(name) = key
            .strip_prefix("remote.")
            .and_then(|v| v.strip_suffix(".url"))
        {
            count += 1;
            if count > 128 {
                return Err(invalid("repository URL count exceeds read budget"));
            }
            configured
                .entry(name.into())
                .or_default()
                .push(value.into());
        }
    }
    let mut remotes = Vec::with_capacity(names.len());
    for name in &names {
        let fetch = configured
            .get(name)
            .into_iter()
            .flatten()
            .map(|url| {
                git.fetch_url(url, &names)
                    .map(|url| endpoint(&url, &|url| resolve_endpoint(resolver, url)))
            })
            .collect::<Result<Vec<_>>>()?;
        remotes.push(RepositoryRemote {
            name: name.clone(),
            fetch,
            push: Vec::new(),
        });
    }
    if before != git.config()?
        || root != git.required(&root_args)?
        || git_dir != git.required(&dir_args)?
    {
        return Err(invalid("Git/config changed during read"));
    }
    Ok(resolve_review_selection(
        &SavedReviewSelection::Automatic,
        &remotes,
        None,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use intent_core::{
        RepositoryUnavailableReason, ReviewSelectionOutcome, ReviewSelectionRequiredReason,
    };
    use intent_sourcecontrol::{remote_project::RemoteInstance, GitlabInstance};

    #[test]
    fn known_repository_uses_effective_fetch_identity_without_workspace_or_network() {
        let dir = crate::test_support::test_tempdir("known-repository-selection-");
        let path = dir.path().join("repo");
        let repo = git2::Repository::init(&path).unwrap();
        let empty = dir.path().join("empty-config");
        std::fs::write(&empty, "").unwrap();
        let environment = GitConfigEnvironment {
            global_config: Some(empty.clone()),
            system_config: Some(empty),
            extra_config_paths: vec![],
        };
        let instance = "https://forge.test:8443/install";
        let resolver = CanonicalRemoteResolver::new(
            vec![
                RemoteInstance::github_com(),
                RemoteInstance::gitlab(GitlabInstance::parse(instance).unwrap()),
            ],
            vec![],
        )
        .unwrap();
        let read = || selection(&path, &resolver, &environment).unwrap();
        assert!(matches!(
            read().outcome,
            ReviewSelectionOutcome::RepositoryUnavailable {
                reason: RepositoryUnavailableReason::NoRemote,
                ..
            }
        ));
        repo.config()
            .unwrap()
            .set_str(&format!("url.{instance}/.insteadOf"), "lab:")
            .unwrap();
        repo.remote("actual", "lab:Team/Sub/Project.git").unwrap();
        let target = RepositoryTarget {
            provider: RepositoryProvider::Gitlab,
            instance_base_url: instance.into(),
            project_path: "Team/Sub/Project".into(),
        };
        assert!(
            matches!(read().outcome, ReviewSelectionOutcome::Resolved { target: actual, .. } if actual == target)
        );
        repo.remote("mirror", &format!("{instance}/Team/Sub/Project.git"))
            .unwrap();
        assert!(
            matches!(read().outcome, ReviewSelectionOutcome::Resolved { target: actual, .. } if actual == target)
        );
        repo.remote("origin", "https://github.com/other/project.git")
            .unwrap();
        assert!(matches!(
            read().outcome,
            ReviewSelectionOutcome::SelectionRequired {
                reason: ReviewSelectionRequiredReason::AmbiguousTargets
            }
        ));
        repo.remote_delete("origin").unwrap();
        repo.remote("unknown", "https://unregistered.test/Team/Sub/Project.git")
            .unwrap();
        assert!(matches!(
            read().outcome,
            ReviewSelectionOutcome::SelectionRequired {
                reason: ReviewSelectionRequiredReason::UnresolvedCandidates
            }
        ));
        let child = path.join("child");
        std::fs::create_dir(&child).unwrap();
        assert!(selection(&child, &resolver, &environment).is_err());
    }
}
