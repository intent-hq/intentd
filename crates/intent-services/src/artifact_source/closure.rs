//! Constant-size read progression; never joins or decodes the code value.
use super::{budget, invalid, CanonicalSourceBinding, NotePageRequest, Result, Value};
use intent_core::note_artifact::request::Primitive;

#[derive(Clone)]
pub(super) enum Step {
    Owner(String),
    Attributes(String),
    Fields {
        reference: String,
        cursor: Option<String>,
    },
    Value {
        reference: String,
        offset: u64,
    },
    Done,
}

fn reference(value: &Value) -> Result<String> {
    let text = value.as_str().ok_or_else(invalid)?;
    if text.is_empty() || text.len() > 256 {
        return Err(invalid());
    }
    Ok(text.into())
}

impl Step {
    pub(super) fn check(&self, request: &NotePageRequest) -> Result<()> {
        if request.max_items != Some(1)
            || !request
                .max_wire_bytes
                .is_some_and(|n| (4096..=8192).contains(&n))
        {
            return Err(budget());
        }
        if request.at.is_some()
            || request.direction.is_some()
            || request.snapshot_id.is_some()
            || request.source_revision.is_some()
            || request.note_instance_id.is_some()
            || request.max_source_bytes.is_some()
        {
            return Err(invalid());
        }
        let (kind, expected, cursor) = match self {
            Self::Owner(r) | Self::Value { reference: r, .. } => ("context", r, None),
            Self::Attributes(r) => ("metadata", r, None),
            Self::Fields { reference, cursor } => ("metadata", reference, cursor.as_ref()),
            Self::Done => return Err(invalid()),
        };
        let actual = if kind == "context" {
            &request.context_ref
        } else {
            &request.reference
        };
        let extra = if kind == "context" {
            &request.reference
        } else {
            &request.context_ref
        };
        if request.kind != kind
            || actual.as_ref() != Some(expected)
            || extra.is_some()
            || request.cursor.as_ref() != cursor
        {
            return Err(invalid());
        }
        Ok(())
    }

    pub(super) fn advance(&self, page: &Value, binding: &CanonicalSourceBinding) -> Result<Self> {
        if page["scope"] != serde_json::to_value(&binding.scope).map_err(|_| invalid())?
            || page["snapshotId"] != binding.snapshot_id
            || page["sourceRevision"] != binding.source_revision
        {
            return Err(invalid());
        }
        let items = page["items"].as_array().ok_or_else(invalid)?;
        if items.len() != 1 {
            return Err(invalid());
        }
        let item = &items[0];
        match self {
            Self::Owner(_) => {
                let node = match binding.primitive {
                    Primitive::Diff => "diffBlock",
                    Primitive::Mermaid => "mermaidBlock",
                };
                if item["nativeRef"] != binding.owner_ref
                    || item["nodeType"] != node
                    || !page["nextCursor"].is_null()
                {
                    return Err(invalid());
                }
                Ok(Self::Attributes(reference(&item["attributesRef"])?))
            }
            Self::Attributes(_) => {
                if item["type"] != "object" || !page["nextCursor"].is_null() {
                    return Err(invalid());
                }
                Ok(Self::Fields {
                    reference: reference(&item["childrenRef"])?,
                    cursor: None,
                })
            }
            Self::Fields {
                reference: current,
                cursor,
            } => {
                if item["key"] == "code" {
                    if item["type"] != "string" || item["valueRef"] != binding.source_ref {
                        return Err(invalid());
                    }
                    return Ok(Self::Value {
                        reference: binding.source_ref.clone(),
                        offset: 0,
                    });
                }
                let next = reference(&page["nextCursor"])?;
                if cursor.as_ref() == Some(&next) {
                    return Err(invalid());
                }
                Ok(Self::Fields {
                    reference: current.clone(),
                    cursor: Some(next),
                })
            }
            Self::Value {
                reference: current,
                offset,
            } => {
                if item["kind"] != "fragment"
                    || item["field"] != "value"
                    || item["offset"].as_u64() != Some(*offset)
                {
                    return Err(invalid());
                }
                let text = item["text"].as_str().ok_or_else(invalid)?;
                if item["nextRef"].is_null() {
                    return Ok(Self::Done);
                }
                let next = reference(&item["nextRef"])?;
                if text.is_empty() || &next == current {
                    return Err(invalid());
                }
                let offset = offset
                    .checked_add(text.encode_utf16().count() as u64)
                    .ok_or_else(invalid)?;
                Ok(Self::Value {
                    reference: next,
                    offset,
                })
            }
            Self::Done => Err(invalid()),
        }
    }
}
