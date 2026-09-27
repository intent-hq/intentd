//! Same-project API creation only. No Git commit/push, retries or CLI authentication.
use super::{
    encode, failure, json, number, project, string, unsupported_write, Error, GitLabSourceControl,
    Method, NewPullRequest, ProviderFailureKind, PullRequest, RepoRef, Result,
    ReviewBranchIdentity, ReviewDetails,
};
use super::{provenance::ConfirmedProject, Purpose, RequestProvenance, RequestScope};
use crate::model::{ConfirmedReviewState, ReviewCreateOutcome, ReviewCreateResult};

impl GitLabSourceControl {
    async fn confirmed_project<'a>(&self, repo: &'a RepoRef) -> Result<ConfirmedProject<'a>> {
        let value = self.get_project(repo).await?;
        // Preserve the existing decode classification for malformed responses.
        number(&value, "id")?;
        string(&value, "path_with_namespace")?;
        ConfirmedProject::from_response(repo, &value).ok_or_else(|| {
            Error::Conflict("GitLab project identity is unconfirmed or changed".into())
        })
    }

    /// Search only the addressed project's open branch pair. Incomplete metadata
    /// cannot establish reuse and prevents a blind duplicate POST.
    ///
    /// # Errors
    /// Returns typed upstream errors or a conflict for an ambiguous candidate.
    pub async fn matching_open_review(
        &self,
        repo: &RepoRef,
        source: &ReviewBranchIdentity,
        target: &ReviewBranchIdentity,
    ) -> Result<Option<ReviewDetails>> {
        self.validate_pair(source, target)?;
        let values = self
            .all(
                &format!("{}/merge_requests", project(repo)),
                vec![
                    ("state".into(), "opened".into()),
                    ("scope".into(), "all".into()),
                    ("source_branch".into(), source.branch.clone()),
                    ("target_branch".into(), target.branch.clone()),
                ],
            )
            .await?;
        let mut matched = None;
        for value in values {
            let details = self.details(value)?;
            match (details.confirmed_state, details.confirmed_draft) {
                (Some(ConfirmedReviewState::Closed | ConfirmedReviewState::Merged), _) => continue,
                (Some(ConfirmedReviewState::Open), Some(_)) => {}
                _ => {
                    return Err(Error::Conflict(
                        "GitLab candidate has unconfirmed or locked review metadata".into(),
                    ));
                }
            }
            if details.source.is_none() || details.target.is_none() {
                return Err(Error::Conflict(
                    "GitLab open review has unconfirmed project or branch identity".into(),
                ));
            }
            if details.review.source_branch != source.branch
                || details.review.target_branch != target.branch
            {
                continue;
            }
            if details.matches_open(source, target) {
                if matched.is_some() {
                    return Err(Error::Conflict(
                        "GitLab returned multiple matching open reviews".into(),
                    ));
                }
                matched = Some(details);
            }
        }
        Ok(matched)
    }

    fn validate_pair(
        &self,
        source: &ReviewBranchIdentity,
        target: &ReviewBranchIdentity,
    ) -> Result<()> {
        if source.instance_base_url != self.descriptor.instance().as_str()
            || target.instance_base_url != self.descriptor.instance().as_str()
            || source.project_id == 0
            || source.project_id != target.project_id
            || source.branch.is_empty()
            || target.branch.is_empty()
            || source.branch == target.branch
            || source.branch.contains(':')
            || target.branch.contains(':')
        {
            return Err(Error::Config(
                "GitLab creation requires confirmed same-instance, same-project branches".into(),
            ));
        }
        Ok(())
    }

    /// Reuse a confirmed open branch pair unchanged, or POST exactly once.
    /// Caller supplies provider-confirmed identities captured in its admitted
    /// operation scope; the per-request credential callback rechecks that scope.
    ///
    /// # Errors
    /// Refuses unknown/fork identities before any request, rejects changed project
    /// or branch metadata before POST, and reports ambiguous writes as uncertain.
    pub async fn create_same_project(
        &self,
        repo: &RepoRef,
        input: NewPullRequest,
        source: &ReviewBranchIdentity,
        target: &ReviewBranchIdentity,
    ) -> Result<ReviewCreateResult> {
        self.validate_pair(source, target)?;
        if input.draft {
            return unsupported_write();
        }
        if input.source_branch != source.branch
            || input.target_branch != target.branch
            || input.title.trim().is_empty()
        {
            return Err(Error::Config(
                "GitLab submitted branch pair or title is invalid".into(),
            ));
        }
        let confirmed = self.confirmed_project(repo).await?;
        let (id, path) = (confirmed.id(), confirmed.path());
        if source.project_id != id
            || target.project_id != id
            || source.project_path.as_ref().is_some_and(|p| p != &path)
            || target.project_path.as_ref().is_some_and(|p| p != &path)
        {
            return Err(Error::Conflict(
                "GitLab submitted project no longer matches".into(),
            ));
        }
        if let Some(details) = self.matching_open_review(repo, source, target).await? {
            return Ok(ReviewCreateResult {
                outcome: ReviewCreateOutcome::Reused,
                details,
            });
        }
        // Both branch lookups are under the confirmed target project. Reading a
        // local HEAD or a same-named branch in a fork cannot satisfy this check.
        for branch in [&source.branch, &target.branch] {
            let (value, _) = self
                .request_scoped(
                    Method::GET,
                    &format!("projects/{id}/repository/branches/{}", encode(branch)),
                    &[],
                    None,
                    RequestScope {
                        purpose: Purpose::Primary,
                        provenance: RequestProvenance::Branch {
                            project: confirmed,
                            branch,
                        },
                    },
                )
                .await?;
            if value["name"].as_str() != Some(branch.as_str())
                || value["commit"]["id"].as_str().is_none_or(str::is_empty)
            {
                return Err(Error::Conflict(
                    "GitLab remote branch is not confirmed".into(),
                ));
            }
        }
        let (value, _) = self
            .request_scoped(
                Method::POST,
                &format!("projects/{id}/merge_requests"),
                &[],
                Some(json!({
                    "title": input.title, "description": input.body,
                    "source_branch": source.branch, "target_branch": target.branch,
                })),
                RequestScope {
                    purpose: Purpose::Primary,
                    provenance: RequestProvenance::ReviewCreate(confirmed),
                },
            )
            .await?;
        let mut details = self
            .details(value)
            .map_err(|_| failure(ProviderFailureKind::WriteUncertain, None))?;
        if !details.matches_open(source, target) {
            return Err(failure(ProviderFailureKind::WriteUncertain, None));
        }
        // Confirmed project paths accompany the API's source/target numeric IDs.
        if let Some(s) = &mut details.source {
            s.project_path = Some(path.clone());
        }
        if let Some(t) = &mut details.target {
            t.project_path = Some(path);
        }
        Ok(ReviewCreateResult {
            outcome: ReviewCreateOutcome::Created,
            details,
        })
    }

    pub(super) async fn create_legacy(
        &self,
        repo: &RepoRef,
        input: NewPullRequest,
    ) -> Result<PullRequest> {
        if input.draft {
            return unsupported_write();
        }
        let confirmed = self.confirmed_project(repo).await?;
        let (id, path) = (confirmed.id(), confirmed.path());
        let source = ReviewBranchIdentity {
            instance_base_url: self.descriptor.instance().as_str().into(),
            project_id: id,
            project_path: Some(path.clone()),
            branch: input.source_branch.clone(),
        };
        let target = ReviewBranchIdentity {
            branch: input.target_branch.clone(),
            project_path: Some(path),
            ..source.clone()
        };
        Ok(self
            .create_same_project(repo, input, &source, &target)
            .await?
            .details
            .review)
    }
}
