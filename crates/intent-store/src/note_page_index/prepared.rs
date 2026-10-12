//! Pure construction shared by the optional pre-writer path and legacy writers.
use super::{context_entries, pieces, sql_offset, task_entries, Entries, Piece};
use intent_core::{Note, Result};
use serde_json::{json, Value};

type EncodedRow = (String, i64, String, Option<i64>, Option<i64>);

pub(super) enum Rows {
    Values(std::vec::IntoIter<(String, usize, Value)>),
    Encoded(std::vec::IntoIter<EncodedRow>),
}

impl Iterator for Rows {
    type Item = Result<EncodedRow>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Encoded(rows) => rows.next().map(Ok),
            Self::Values(rows) => rows.next().map(|(collection, position, mut value)| {
                let admission = value
                    .as_object_mut()
                    .expect("index object")
                    .remove("_admissionRange")
                    .unwrap_or_else(|| value["sourceRange"].clone());
                Ok((
                    collection,
                    sql_offset(position)?,
                    value.to_string(),
                    admission["start"].as_i64(),
                    admission["end"].as_i64(),
                ))
            }),
        }
    }
}

pub(crate) struct Projection {
    pub(super) parts: Vec<Piece>,
    pub(super) rows: Rows,
    pub(super) artifact_sources: Vec<(String, String, &'static str)>,
    pub(super) task_count: usize,
    #[cfg(test)]
    pub(crate) drop_observer: Option<std::sync::Arc<()>>,
}

impl Projection {
    pub(super) fn build(note: &Note, content_changed: bool) -> Self {
        let mut entries = Entries {
            rows: Vec::new(),
            artifact_sources: Vec::new(),
            serial: 1_usize << 40,
        };
        let metadata = json!({"title":note.title,"tags":note.tags,"parentId":note.parent_id,"metadata":note.metadata,"contentType":note.content_type,"isPinned":note.is_pinned,"isArchived":note.is_archived,"isDefault":note.is_default,"visibility":note.visibility,"createdAt":note.created_at,"updatedAt":note.updated_at});
        let root = entries.metadata(&metadata, "m", None, None, None);
        entries.rows.push(("m:root".into(), 0, root));
        let mut parts = Vec::new();
        let mut task_count = 0;
        if content_changed {
            parts = pieces(&note.content);
            entries.serial = 0;
            context_entries(&note.content, &parts, &mut entries);
            task_count = task_entries(&note.content, &mut entries);
        }
        Self {
            parts,
            rows: Rows::Values(entries.rows.into_iter()),
            artifact_sources: entries.artifact_sources,
            task_count,
            #[cfg(test)]
            drop_observer: None,
        }
    }

    /// Optional retention limits, never limits on whether a note may be saved.
    /// The temporary Value tree is still input-relative; these bounds apply to
    /// retained rows after generation, not the parser's peak heap allocation.
    pub(crate) fn prepare(note: &Note) -> Result<Option<Self>> {
        const MAX_ROWS: usize = 750_000;
        const MAX_ENCODED_BYTES: usize = 256 * 1024 * 1024;
        Self::prepare_with_limits(note, MAX_ROWS, MAX_ENCODED_BYTES)
    }

    pub(crate) fn prepare_with_limits(
        note: &Note,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<Option<Self>> {
        let mut projection = Self::build(note, true);
        let Rows::Values(ref values) = projection.rows else {
            unreachable!()
        };
        if values.len() > max_rows {
            return Ok(None);
        }
        let mut rows = Vec::with_capacity(values.len());
        let mut bytes = 0_usize;
        for row in projection.rows.by_ref() {
            let row = row?;
            bytes = bytes
                .saturating_add(row.0.len())
                .saturating_add(row.2.len());
            if bytes > max_bytes {
                return Ok(None);
            }
            rows.push(row);
        }
        projection.rows = Rows::Encoded(rows.into_iter());
        Ok(Some(projection))
    }
}
