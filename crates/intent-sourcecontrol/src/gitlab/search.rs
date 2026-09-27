//! Adapted from Bert Colemont, anubissbe/intentd 3c9e692c (Apache-2.0).
//! Bounded, resumable search across projects on one credentialed instance.
use super::{
    number, project, Error, GitLabSourceControl, Page, PageParams, RepoRef, Result, Value,
    URL_SAFE_NO_PAD,
};
use base64::Engine as _;

impl GitLabSourceControl {
    pub(super) async fn search_projects(
        &self,
        repo: &RepoRef,
        extras: &[RepoRef],
        resource: &str,
        params: Vec<(String, String)>,
        page: PageParams,
        involves: Option<&str>,
    ) -> Result<Page<Value>> {
        let mut repos = vec![repo.clone()];
        for extra in extras {
            if !repos
                .iter()
                .any(|r| r.owner == extra.owner && r.name == extra.name)
            {
                repos.push(extra.clone());
            }
        }
        if repos.len() > 20 {
            return Err(Error::Config(
                "GitLab search supports at most 20 projects".into(),
            ));
        }
        // Bind cursors to their query and project order; they contain no URLs or
        // credentials, and cannot redirect the adapter to an arbitrary host.
        let scope = serde_json::to_string(&(
            self.descriptor.instance().as_str(),
            resource,
            &repos,
            &params,
            involves,
            page.limit,
        ))?;
        let (index, cursor) = match page.cursor {
            None => (0_usize, None),
            Some(raw) => {
                let decoded = URL_SAFE_NO_PAD
                    .decode(
                        raw.strip_prefix("gl:")
                            .ok_or_else(|| Error::Config("invalid GitLab search cursor".into()))?,
                    )
                    .map_err(|_| Error::Config("invalid GitLab search cursor".into()))?;
                let (saved, index, cursor): (String, usize, Option<String>) =
                    serde_json::from_slice(&decoded)
                        .map_err(|_| Error::Config("invalid GitLab search cursor".into()))?;
                if saved != scope || index >= repos.len() {
                    return Err(Error::Config(
                        "GitLab search cursor belongs to another query".into(),
                    ));
                }
                (index, cursor)
            }
        };
        let current = &repos[index];
        let result = self
            .page(
                &format!("{}/{resource}", project(current)),
                params,
                PageParams {
                    limit: page.limit,
                    cursor,
                },
            )
            .await?;
        let mut items = Vec::new();
        for item in result.items {
            if let Some(login) = involves {
                let directly_involved = item["author"]["username"] == login
                    || ["assignees", "reviewers"].iter().any(|key| {
                        item[key]
                            .as_array()
                            .is_some_and(|users| users.iter().any(|u| u["username"] == login))
                    });
                if !directly_involved {
                    let iid = number(&item, "iid")?;
                    let participants = self
                        .all(
                            &format!("{}/{resource}/{iid}/participants", project(current)),
                            vec![],
                        )
                        .await?;
                    if !participants.iter().any(|user| user["username"] == login) {
                        continue;
                    }
                }
            }
            items.push(item);
        }
        let next = if let Some(next) = result.next_cursor {
            Some((index, Some(next)))
        } else if index + 1 < repos.len() {
            Some((index + 1, None))
        } else {
            None
        };
        let next_cursor = next
            .map(|(index, cursor)| {
                serde_json::to_vec(&(&scope, index, cursor))
                    .map(|bytes| format!("gl:{}", URL_SAFE_NO_PAD.encode(bytes)))
            })
            .transpose()?;
        // An empty filtered page with a next cursor is not the end of search.
        Ok(Page { items, next_cursor })
    }
}
