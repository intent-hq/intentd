//! One canonical discussion collection; no secondary notes/review-comments merge.
use std::collections::HashSet;

use base64::Engine as _;

use super::{
    array, encode_thread, mr, number, string, to_comment, GitLabSourceControl, URL_SAFE_NO_PAD,
};
use crate::{DiscussionNote, Error, Page, PageParams, RepoRef, Result, ReviewDiscussion};

impl GitLabSourceControl {
    pub(super) async fn complete_discussions(
        &self,
        repo: &RepoRef,
        iid: u64,
        page: PageParams,
    ) -> Result<Page<ReviewDiscussion>> {
        if iid == 0 {
            return Err(Error::Config("review number must be positive".into()));
        }
        let page = self
            .page_for(
                &format!("{}/discussions", mr(repo, iid)),
                vec![],
                page,
                super::http::Purpose::Optional,
            )
            .await?;
        let mut seen_threads = HashSet::new();
        let mut seen_notes = HashSet::new();
        let mut items = Vec::new();
        for thread in page.items {
            let raw_id = string(&thread, "id")?;
            if raw_id.is_empty() || !seen_threads.insert(raw_id.clone()) {
                return Err(Error::Decode(
                    "duplicate or empty GitLab discussion identity".into(),
                ));
            }
            let id = encode_thread(self.descriptor.instance().as_str(), repo, iid, &raw_id);
            let individual_note = thread["individual_note"]
                .as_bool()
                .ok_or_else(|| Error::Decode("missing GitLab discussion kind".into()))?;
            let raw_notes = array(&thread, "notes")?;
            let mut notes = Vec::new();
            for raw in raw_notes.iter().filter(|note| note["system"] != true) {
                let note_id = number(raw, "id")?;
                if note_id == 0 || !seen_notes.insert(note_id) {
                    return Err(Error::Decode(
                        "duplicate or empty GitLab note identity".into(),
                    ));
                }
                let note = to_comment(raw)?;
                let canonical = format!(
                    "gitlab-note:{}",
                    URL_SAFE_NO_PAD.encode(
                        serde_json::json!([
                            self.descriptor.instance().as_str(),
                            format!("{}/{}", repo.owner, repo.name),
                            iid,
                            note_id
                        ])
                        .to_string()
                    )
                );
                let resolvable = raw["resolvable"]
                    .as_bool()
                    .ok_or_else(|| Error::Decode("missing GitLab note resolvability".into()))?;
                notes.push(DiscussionNote {
                    id: canonical,
                    in_reply_to_id: if individual_note {
                        None
                    } else {
                        notes.first().map(|n: &DiscussionNote| n.id.clone())
                    },
                    author: note.author,
                    body: note.body,
                    created_at: note.created_at,
                    path: note.path,
                    line: note.line,
                    position_head_sha: raw["position"]["head_sha"].as_str().map(str::to_owned),
                    url: note.url,
                    resolvable,
                    resolved: if resolvable {
                        raw["resolved"].as_bool()
                    } else {
                        None
                    },
                });
            }
            if individual_note && notes.len() > 1 {
                return Err(Error::Decode(
                    "standalone GitLab discussion has replies".into(),
                ));
            }
            if !notes.is_empty() {
                items.push(ReviewDiscussion {
                    id,
                    individual_note,
                    notes,
                });
            }
        }
        Ok(Page {
            items,
            next_cursor: page.next_cursor,
        })
    }
}
