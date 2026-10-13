//! Strict enclosing requests for opt-in annotation reads. Page payloads are
//! admitted by the annotation store; legacy unpaged methods keep their shapes.
use crate::{note_page::NoteScope, Error, Result};
use serde::Deserialize;
use serde_json::Value;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AnnotationMethod {
    Attribution,
    Comments,
    Replies,
    Context,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AnnotationReadRequest {
    pub backend_id: String,
    pub workspace_id: String,
    pub note_id: String,
    pub note_instance_id: String,
    pub source_revision: String,
    pub attribution_generation: Option<String>,
    pub comment_revision: Option<String>,
    pub thread_id: Option<String>,
    pub include_comments: Option<bool>,
    pub page: Value,
}

fn token(value: &str) -> bool {
    !value.is_empty() && value.len() <= 256 && !value.contains('\0')
}
fn invalid() -> Error {
    Error::InvalidParams("Invalid annotation page request".into())
}

impl AnnotationReadRequest {
    #[must_use]
    pub fn scope(&self) -> NoteScope {
        NoteScope {
            backend_id: self.backend_id.clone(),
            workspace_id: self.workspace_id.clone(),
            note_id: self.note_id.clone(),
            note_instance_id: self.note_instance_id.clone(),
        }
    }

    /// Validate the enclosing method identity before reading persisted state.
    /// The store separately enforces the exact page shape and response budgets.
    ///
    /// # Errors
    /// Rejects mismatched page kinds, oversized identities, incompatible legacy
    /// fields, and absent or unrelated context epochs.
    pub fn validate(&self, method: AnnotationMethod) -> Result<()> {
        if ![
            self.backend_id.as_str(),
            self.workspace_id.as_str(),
            self.note_id.as_str(),
            self.note_instance_id.as_str(),
            self.source_revision.as_str(),
        ]
        .into_iter()
        .all(token)
            || [
                self.attribution_generation.as_deref(),
                self.comment_revision.as_deref(),
                self.thread_id.as_deref(),
            ]
            .into_iter()
            .flatten()
            .any(|value| !token(value))
        {
            return Err(invalid());
        }
        let expected = match method {
            AnnotationMethod::Attribution => "attribution",
            AnnotationMethod::Comments => "comments",
            AnnotationMethod::Replies => "replies",
            AnnotationMethod::Context => "context",
        };
        if self.page.get("kind").and_then(Value::as_str) != Some(expected)
            || (method != AnnotationMethod::Replies && self.thread_id.is_some())
            || (method == AnnotationMethod::Replies && self.thread_id.is_none())
            || self.include_comments == Some(true)
            || (method != AnnotationMethod::Comments && self.include_comments.is_some())
        {
            return Err(invalid());
        }
        let wrong_epoch = match method {
            AnnotationMethod::Attribution => self.comment_revision.is_some(),
            AnnotationMethod::Comments | AnnotationMethod::Replies => {
                self.attribution_generation.is_some()
            }
            AnnotationMethod::Context => {
                self.attribution_generation.is_some() == self.comment_revision.is_some()
            }
        };
        if wrong_epoch {
            return Err(invalid());
        }
        Ok(())
    }

    #[must_use]
    pub fn epoch(&self) -> Option<&str> {
        self.attribution_generation
            .as_deref()
            .or(self.comment_revision.as_deref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn annotation_enclosing_request_keeps_scope_epochs_and_method_strict() {
        let value = json!({"backendId":"backend","workspaceId":"workspace","noteId":"note","noteInstanceId":"instance","sourceRevision":"source","page":{"kind":"comments","ranges":[]}});
        let request: AnnotationReadRequest = serde_json::from_value(value.clone()).unwrap();
        request.validate(AnnotationMethod::Comments).unwrap();
        assert_eq!(request.scope().note_instance_id, "instance");
        for (field, bad) in [
            ("workspaceId", json!("")),
            ("backendId", json!("x".repeat(257))),
            ("sourceRevision", json!("nul\0revision")),
            ("attributionGeneration", json!("generation")),
            ("includeComments", json!(true)),
            ("threadId", json!("thread")),
        ] {
            let mut input = value.clone();
            input[field] = bad;
            let bad: AnnotationReadRequest = serde_json::from_value(input).unwrap();
            assert!(bad.validate(AnnotationMethod::Comments).is_err(), "{field}");
        }
        let mut unknown = value.clone();
        unknown["since"] = json!("date");
        assert!(serde_json::from_value::<AnnotationReadRequest>(unknown).is_err());
        let mut replies = request.clone();
        replies.page = json!({"kind":"replies"});
        assert!(replies.validate(AnnotationMethod::Replies).is_err());
        replies.thread_id = Some("thread".into());
        replies.validate(AnnotationMethod::Replies).unwrap();
        assert!(replies.validate(AnnotationMethod::Comments).is_err());
        let mut context = request;
        context.page = json!({"kind":"context","contextRef":"reference"});
        assert!(context.validate(AnnotationMethod::Context).is_err());
        context.comment_revision = Some("comments".into());
        context.validate(AnnotationMethod::Context).unwrap();
        context.attribution_generation = Some("attribution".into());
        assert!(context.validate(AnnotationMethod::Context).is_err());
        context.comment_revision = None;
        context.validate(AnnotationMethod::Context).unwrap();
    }
}
