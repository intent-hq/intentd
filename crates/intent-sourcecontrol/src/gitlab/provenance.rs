//! Provider-confirmed relationships for numeric endpoints. A project label is
//! never evidence for an arbitrary numeric project, pipeline, or write.

use super::{encode, RepoRef, Value};

#[derive(Debug, Clone, Copy)]
pub(super) struct ConfirmedProject<'a> {
    repo: &'a RepoRef,
    id: u64,
}

impl<'a> ConfirmedProject<'a> {
    pub(super) fn from_response(repo: &'a RepoRef, value: &Value) -> Option<Self> {
        let id = positive(&value["id"])?;
        (value["path_with_namespace"].as_str()? == format!("{}/{}", repo.owner, repo.name))
            .then_some(Self { repo, id })
    }

    pub(super) fn id(self) -> u64 {
        self.id
    }

    pub(super) fn path(self) -> String {
        format!("{}/{}", self.repo.owner, self.repo.name)
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ConfirmedPipeline<'a> {
    parent: ConfirmedProject<'a>,
    project_id: u64,
    pipeline_id: u64,
}

#[derive(Debug, Clone, Copy)]
pub(super) enum RequestProvenance<'a> {
    Direct,
    Unconfirmed,
    Branch {
        project: ConfirmedProject<'a>,
        branch: &'a str,
    },
    ReviewCreate(ConfirmedProject<'a>),
    Pipeline(ConfirmedPipeline<'a>),
}

impl<'a> RequestProvenance<'a> {
    pub(super) fn review_pipeline(
        repo: &'a RepoRef,
        number: u64,
        head: &Value,
        project: Option<&Value>,
    ) -> Self {
        Self::confirmed_pipeline(repo, number, head, project)
            .map_or(Self::Unconfirmed, Self::Pipeline)
    }

    fn confirmed_pipeline(
        repo: &'a RepoRef,
        number: u64,
        head: &Value,
        project: Option<&Value>,
    ) -> Option<ConfirmedPipeline<'a>> {
        let parent = ConfirmedProject::from_response(repo, project?)?;
        let source_id = positive(&head["source_project_id"])?;
        let target_id = positive(&head["target_project_id"])?;
        if positive(&head["iid"])? != number || target_id != parent.id {
            return None;
        }
        if head
            .get("project_id")
            .is_some_and(|v| positive(v) != Some(target_id))
        {
            return None;
        }
        let pipeline_id = positive(&head["head_pipeline"]["id"])?;
        let project_id = positive(&head["head_pipeline"]["project_id"])?;
        (project_id == source_id || project_id == target_id).then_some(ConfirmedPipeline {
            parent,
            project_id,
            pipeline_id,
        })
    }

    pub(super) fn allows(self, project: &str, path: &str, writing: bool) -> bool {
        match self {
            Self::Direct => within(path, &format!("projects/{}", encode(project))),
            Self::Unconfirmed => false,
            Self::Branch {
                project: confirmed,
                branch,
            } => {
                !writing
                    && project == confirmed.path()
                    && path
                        == format!(
                            "projects/{}/repository/branches/{}",
                            confirmed.id,
                            encode(branch)
                        )
            }
            Self::ReviewCreate(confirmed) => {
                writing
                    && project == confirmed.path()
                    && path == format!("projects/{}/merge_requests", confirmed.id)
            }
            Self::Pipeline(confirmed) => {
                !writing
                    && project == confirmed.parent.path()
                    && path
                        == format!(
                            "projects/{}/pipelines/{}/jobs",
                            confirmed.project_id, confirmed.pipeline_id
                        )
            }
        }
    }
}

fn positive(value: &Value) -> Option<u64> {
    value.as_u64().filter(|id| *id > 0)
}

fn within(path: &str, prefix: &str) -> bool {
    path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|s| s.starts_with('/'))
}
