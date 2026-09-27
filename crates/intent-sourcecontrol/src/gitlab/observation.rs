//! Optional policy/CI/discussion signals cannot masquerade as primary-resource denial.
use super::{
    approval_decision, approval_reviews, discussion_tally, http, job_state, known_check_status, mr,
    optional, pipeline_check, project, project_rules, string, to_pr, Error, GitLabSourceControl,
    MergeRequirementSignals, ProviderAvailability, Purpose, RepoRef, Result, ReviewAvailability,
    ReviewBranchIdentity, ReviewDecision, ReviewDetails, ReviewObservation, RollupCheck,
    RollupCheckKind, Value,
};

impl GitLabSourceControl {
    pub(super) fn details(&self, value: Value) -> Result<ReviewDetails> {
        let identity = |project: &str, branch: &str| -> Option<ReviewBranchIdentity> {
            let id = value[project].as_u64().filter(|id| *id > 0)?;
            let branch = value[branch].as_str().filter(|b| !b.is_empty())?;
            Some(ReviewBranchIdentity {
                instance_base_url: self.descriptor.instance().as_str().into(),
                project_id: id,
                project_path: None,
                branch: branch.into(),
            })
        };
        let source = identity("source_project_id", "source_branch");
        let target = identity("target_project_id", "target_branch");
        let review = to_pr(value)?;
        if !self.descriptor.instance().contains_url(&review.url) {
            return Err(Error::Decode(
                "GitLab review URL escaped logical instance".into(),
            ));
        }
        Ok(ReviewDetails {
            review,
            source,
            target,
        })
    }

    async fn optional_all(&self, path: &str) -> Result<(Option<Vec<Value>>, ProviderAvailability)> {
        match self.all_for(path, vec![], Purpose::Optional).await {
            Ok(values) => Ok((Some(values), ProviderAvailability::Available)),
            Err(error) if http::optional_failure(&error) => Ok((None, http::availability(&error))),
            Err(error) => Err(error),
        }
    }

    async fn mr_checks(
        &self,
        repo: &RepoRef,
        head: &Value,
        policy: Option<&Value>,
    ) -> Result<(Vec<RollupCheck>, ProviderAvailability)> {
        let pipeline = &head["head_pipeline"];
        let required = policy.is_some_and(|p| p["only_allow_merge_if_pipeline_succeeds"] == true);
        if pipeline.is_null() {
            return Ok((
                if required {
                    vec![pipeline_check(pipeline, policy)]
                } else {
                    vec![]
                },
                if policy.is_some_and(|p| p["only_allow_merge_if_pipeline_succeeds"] == false) {
                    ProviderAvailability::Available
                } else {
                    ProviderAvailability::Unknown
                },
            ));
        }
        let Some(id) = pipeline["id"].as_u64().filter(|id| *id > 0) else {
            return Ok((vec![], ProviderAvailability::Unknown));
        };
        if !known_check_status(&pipeline["status"])
            || required
                && pipeline["status"] == "skipped"
                && !policy.is_some_and(|p| p["allow_merge_on_skipped_pipeline"].is_boolean())
        {
            return Ok((
                vec![pipeline_check(pipeline, policy)],
                ProviderAvailability::Unknown,
            ));
        }
        // MR-associated pipelines can run in the fork or have a merged-results SHA.
        let pipeline_project = pipeline["project_id"]
            .as_u64()
            .map_or_else(|| project(repo), |id| format!("projects/{id}"));
        let (jobs, availability) = self
            .optional_all(&format!("{pipeline_project}/pipelines/{id}/jobs"))
            .await?;
        let mut checks = vec![pipeline_check(pipeline, policy)];
        if let Some(jobs) = jobs {
            for job in jobs {
                if !known_check_status(&job["status"]) || !job["allow_failure"].is_boolean() {
                    return Ok((checks, ProviderAvailability::Unknown));
                }
                let (Ok(name), Ok(state)) = (string(&job, "name"), job_state(&job)) else {
                    return Ok((checks, ProviderAvailability::Unknown));
                };
                checks.push(RollupCheck {
                    name,
                    kind: RollupCheckKind::CheckRun,
                    state,
                    is_required: required && job["allow_failure"] == false,
                    url: optional(&job, "web_url"),
                    started_at: optional(&job, "started_at"),
                });
            }
        }
        Ok((checks, availability))
    }

    /// Read one primary MR and retain explicit availability of every optional signal.
    ///
    /// # Errors
    /// A primary denial or a retired injected credential scope fails the operation.
    pub async fn observe_review(&self, repo: &RepoRef, number: u64) -> Result<ReviewObservation> {
        let head = self.get(&mr(repo, number)).await?;
        let details = self.details(head.clone())?;
        let (policy, mut policy_state) = self.optional_get(&project(repo)).await?;
        if policy.as_ref().is_some_and(|p| {
            !p["only_allow_merge_if_pipeline_succeeds"].is_boolean()
                || !p["only_allow_merge_if_all_discussions_are_resolved"].is_boolean()
        }) {
            policy_state = ProviderAvailability::Unknown;
        }
        let (approvals, mut approvals_state) = self
            .optional_get(&format!("{}/approvals", mr(repo, number)))
            .await?;
        let (checks, checks_state) = self.mr_checks(repo, &head, policy.as_ref()).await?;
        let reviews = approvals.as_ref().and_then(|v| {
            if let Ok(reviews) = approval_reviews(v) {
                Some(reviews)
            } else {
                approvals_state = ProviderAvailability::Unknown;
                None
            }
        });
        if approvals.as_ref().is_some_and(|v| {
            v["approvals_required"]
                .as_u64()
                .and_then(|n| u32::try_from(n).ok())
                .is_none()
                || v["approvals_left"].as_u64().is_none()
        }) {
            approvals_state = ProviderAvailability::Unknown;
        }
        let mut rules = policy.as_ref().map(project_rules);
        if let Some(rules) = &mut rules {
            rules.required_approving_review_count = approvals
                .as_ref()
                .and_then(|a| a["approvals_required"].as_u64())
                .and_then(|n| u32::try_from(n).ok());
        }
        let signals = MergeRequirementSignals {
            merge_state_status: optional(&head, "detailed_merge_status"),
            review_decision: if head["detailed_merge_status"] == "requested_changes" {
                Some(ReviewDecision::ChangesRequested)
            } else {
                approvals
                    .as_ref()
                    .filter(|_| approvals_state == ProviderAvailability::Available)
                    .and_then(approval_decision)
            },
            checks,
            checks_known: checks_state == ProviderAvailability::Available
                && policy
                    .as_ref()
                    .is_some_and(|p| p["only_allow_merge_if_pipeline_succeeds"].is_boolean()),
            checks_head_sha: optional(&head, "sha"),
            branch_rules: rules,
            // Train state is optional; absence never means the MR is known unqueued.
            is_in_merge_queue: head["merge_train"].as_object().map(|_| true),
            ..MergeRequirementSignals::default()
        };
        let (discussions, mut discussions_state) = self
            .optional_all(&format!("{}/discussions", mr(repo, number)))
            .await?;
        let counts = discussions.as_ref().and_then(|v| {
            if let Ok(counts) = discussion_tally(v) {
                Some(counts)
            } else {
                discussions_state = ProviderAvailability::Unknown;
                None
            }
        });
        Ok(ReviewObservation {
            details,
            signals,
            reviews,
            threads: counts.map(|c| c.0),
            conversation_count: counts.map(|c| c.1),
            availability: ReviewAvailability {
                policy: policy_state,
                approvals: approvals_state,
                checks: checks_state,
                discussions: discussions_state,
            },
        })
    }
}
