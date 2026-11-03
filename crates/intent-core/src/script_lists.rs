//! Shared strict active-list options, scoped keysets and bounded projections.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Clone, Debug, Default)]
pub struct ScriptListOptions {
    pub summary: bool,
    pub limit: usize,
    pub cursor: Option<String>,
}
impl ScriptListOptions {
    /// Validate before any row read, including deprecated history filters.
    /// # Errors
    /// Returns invalid parameters for unsupported filters or paging inputs.
    pub fn parse(value: &Value, monitor: bool) -> Result<Self> {
        let invalid = || {
            Error::InvalidParams("only active lists are supported; summary limit is 1–200 and cursors must be nonempty".into())
        };
        let filter = if monitor { "state" } else { "archive" };
        if value
            .get(filter)
            .is_some_and(|v| v.as_str() != Some("active"))
        {
            return Err(invalid());
        }
        let summary = match value.get("view") {
            None => false,
            Some(Value::String(s)) if s == "full" => false,
            Some(Value::String(s)) if s == "summary" => true,
            _ => return Err(invalid()),
        };
        if !summary && (value.get("limit").is_some() || value.get("cursor").is_some()) {
            return Err(invalid());
        }
        let limit = match value.get("limit") {
            None => 100,
            Some(v) => usize::try_from(
                v.as_u64()
                    .filter(|v| (1..=200).contains(v))
                    .ok_or_else(invalid)?,
            )
            .map_err(|_| invalid())?,
        };
        let cursor = value
            .get("cursor")
            .map(|v| {
                v.as_str()
                    .filter(|s| !s.is_empty())
                    .map(str::to_owned)
                    .ok_or_else(invalid)
            })
            .transpose()?;
        Ok(Self {
            summary,
            limit,
            cursor,
        })
    }
    /// [`Self::parse`] for `script.list` on the wire, which also pages the
    /// full view: `limit` (1–200, default 100) and `cursor` are accepted with
    /// an omitted or `"full"` view. Other views parse as [`Self::parse`].
    /// # Errors
    /// Returns invalid parameters for unsupported filters or paging inputs.
    pub fn parse_full_pages(value: &Value) -> Result<Self> {
        if value
            .get("view")
            .is_some_and(|v| v.as_str() != Some("full"))
        {
            return Self::parse(value, false);
        }
        let mut summary = value.clone();
        if let Some(map) = summary.as_object_mut() {
            map.insert("view".into(), json!("summary"));
        }
        let mut options = Self::parse(&summary, false)?;
        options.summary = false;
        Ok(options)
    }
    /// Decode a keyset scoped to its collection and effective owner.
    /// # Errors
    /// Rejects malformed or differently scoped cursors.
    pub fn boundary(&self, scope: &str) -> Result<Option<(String, String)>> {
        self.cursor
            .as_ref()
            .map(|raw| {
                let invalid =
                    || Error::InvalidParams("invalid or mismatched script list cursor".into());
                let cursor: Cursor = serde_json::from_str(raw).map_err(|_| invalid())?;
                if cursor.version != 1
                    || cursor.scope != scope
                    || cursor.created_at.is_empty()
                    || cursor.id.is_empty()
                {
                    return Err(invalid());
                }
                Ok((cursor.created_at, cursor.id))
            })
            .transpose()
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    version: u8,
    scope: String,
    created_at: String,
    id: String,
}
#[must_use]
pub fn scope(method: &str, workspace: &str, owner: Option<&str>) -> String {
    json!([method, workspace, "summary", "active", owner]).to_string()
}
/// Cursor scope of a full-view `script.list` page.
#[must_use]
pub fn full_scope(workspace: &str) -> String {
    json!(["script.list", workspace, "full", "active", null]).to_string()
}
#[must_use]
pub fn cursor(scope: &str, created_at: &str, id: &str) -> String {
    json!({"version":1,"scope":scope,"created_at":created_at,"id":id}).to_string()
}
#[must_use]
pub fn prefix(text: &str, limit: usize) -> (&str, bool) {
    let mut end = text.len().min(limit);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    (&text[..end], end < text.len())
}
pub fn bound_error(value: &mut Value) {
    if let Some(error) = value.get("error").and_then(Value::as_str) {
        let (preview, truncated) = prefix(error, 256);
        let preview = preview.to_owned();
        value["error"] = json!(preview);
        if truncated {
            value["errorTruncated"] = json!(true);
        }
    }
}
pub fn strip_nulls(value: &mut Value) {
    if let Some(obj) = value.as_object_mut() {
        obj.retain(|_, v| !v.is_null());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn options_reject_history_null_and_unbounded_reads() {
        for value in [
            json!({"archive":"all"}),
            json!({"archive":"archived"}),
            json!({"archive":null}),
            json!({"limit":10}),
            json!({"view":"summary","limit":201}),
            json!({"view":"summary","limit":1.5}),
            json!({"view":"summary","cursor":null}),
        ] {
            assert!(ScriptListOptions::parse(&value, false).is_err(), "{value}");
        }
        for value in [
            json!({"state":"all"}),
            json!({"state":"terminal"}),
            json!({"state":null}),
        ] {
            assert!(ScriptListOptions::parse(&value, true).is_err());
        }
    }
    #[test]
    fn full_pages_accept_limit_and_cursor_only_on_the_wire_parser() {
        for value in [
            json!({}),
            json!({"view":"full"}),
            json!({"limit":10}),
            json!({"view":"full","limit":200,"cursor":"c"}),
        ] {
            let options = ScriptListOptions::parse_full_pages(&value).unwrap();
            assert!(!options.summary, "{value}");
        }
        assert_eq!(
            ScriptListOptions::parse_full_pages(&json!({}))
                .unwrap()
                .limit,
            100
        );
        for value in [
            json!({"limit":0}),
            json!({"limit":201}),
            json!({"cursor":""}),
            json!({"view":"bogus"}),
            json!({"archive":"all"}),
        ] {
            assert!(
                ScriptListOptions::parse_full_pages(&value).is_err(),
                "{value}"
            );
        }
        assert!(
            ScriptListOptions::parse_full_pages(&json!({"view":"summary"}))
                .unwrap()
                .summary
        );
        assert!(ScriptListOptions::parse(&json!({"limit":10}), false).is_err());
        assert_ne!(full_scope("a"), scope("script.list", "a", None));
    }
    #[test]
    fn keysets_are_scoped_and_unicode_prefixes_are_byte_bounded() {
        let scope = scope("script.list", "a", None);
        let c = cursor(&scope, "time", "id");
        let options =
            ScriptListOptions::parse(&json!({"view":"summary","cursor":c}), false).unwrap();
        assert_eq!(
            options.boundary(&scope).unwrap(),
            Some(("time".into(), "id".into()))
        );
        assert!(options.boundary("foreign").is_err());
        assert_eq!(prefix("é😀z", 5), ("é", true));
        assert_eq!(prefix("", 160), ("", false));
    }
}
