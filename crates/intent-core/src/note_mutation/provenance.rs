//! Compose source addresses through actual edits, retaining untouched base spans.
use super::{apply_edits, NoteMutationError, NoteSplice, NoteSpliceMapping};

#[derive(Clone, Debug)]
enum Span {
    Base { start: u64, end: u64 },
    Inserted(u64),
}

impl Span {
    fn len(&self) -> u64 {
        match self {
            Self::Base { start, end } => end - start,
            Self::Inserted(length) => *length,
        }
    }

    fn slice(&self, from: u64, to: u64) -> Self {
        match self {
            Self::Base { start, .. } => Self::Base {
                start: start + from,
                end: start + to,
            },
            Self::Inserted(_) => Self::Inserted(to - from),
        }
    }
}

/// Transaction-local source history. Canonical phases use the exact coordinates
/// supplied by their parser/anchor operation, never a text diff or first match.
/// Cloning at a conversion savepoint preserves the pre-conversion fallback.
#[derive(Clone, Debug)]
pub struct NoteSourceHistory {
    source: String,
    base_length: u64,
    spans: Vec<Span>,
}

impl NoteSourceHistory {
    #[must_use]
    pub fn new(source: String) -> Self {
        let base_length = source.encode_utf16().count() as u64;
        Self {
            source,
            base_length,
            spans: if base_length == 0 {
                Vec::new()
            } else {
                vec![Span::Base {
                    start: 0,
                    end: base_length,
                }]
            },
        }
    }

    #[must_use]
    pub fn source(&self) -> &str {
        &self.source
    }

    /// Apply an internal canonical phase, or an already admitted caller batch.
    /// Internal effects may exceed inline request budgets; they are stored/paged
    /// separately. This still checks scalar boundaries and unambiguous ordering.
    /// A rejected phase leaves both source and provenance untouched.
    ///
    /// # Errors
    /// Rejects overlapping, unordered, invalid UTF-16 or out-of-range edits.
    pub fn apply_phase(&mut self, edits: &[NoteSplice]) -> Result<(), NoteMutationError> {
        let changed = apply_edits(&self.source, edits)?;
        let mut spans = self.spans.clone();
        for edit in edits.iter().rev() {
            let mut before = Vec::new();
            let mut after = Vec::new();
            let mut position = 0;
            for span in &spans {
                let end = position + span.len();
                if position < edit.start {
                    let keep = end.min(edit.start) - position;
                    if keep > 0 {
                        before.push(span.slice(0, keep));
                    }
                }
                if end > edit.end {
                    let from = edit.end.saturating_sub(position);
                    after.push(span.slice(from, span.len()));
                }
                position = end;
            }
            let inserted = edit.text.encode_utf16().count() as u64;
            if inserted > 0 {
                before.push(Span::Inserted(inserted));
            }
            before.extend(after);
            spans = before;
        }
        self.source = changed.source;
        self.spans = spans;
        Ok(())
    }

    /// Final base-to-current changes. Identical replacement bytes still retain
    /// their edit identity; deleting inserted bytes can cancel that insertion.
    #[must_use]
    pub fn mapping(&self) -> Vec<NoteSpliceMapping> {
        let mut result = Vec::new();
        let mut base = 0;
        let mut inserted = 0;
        for span in &self.spans {
            match span {
                Span::Inserted(length) => inserted += length,
                Span::Base { start, end } => {
                    if *start > base || inserted > 0 {
                        result.push(NoteSpliceMapping {
                            start: base,
                            end: *start,
                            inserted_length: inserted,
                        });
                    }
                    base = *end;
                    inserted = 0;
                }
            }
        }
        if base < self.base_length || inserted > 0 {
            result.push(NoteSpliceMapping {
                start: base,
                end: self.base_length,
                inserted_length: inserted,
            });
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn edit(start: u64, end: u64, text: &str) -> NoteSplice {
        NoteSplice {
            start,
            end,
            text: text.into(),
        }
    }

    #[test]
    fn compose_distant_canonical_change_without_matching_repeated_text() {
        let mut history = NoteSourceHistory::new("same😀same😀same".into());
        history.apply_phase(&[edit(6, 10, "same")]).unwrap();
        history.apply_phase(&[edit(12, 16, "last")]).unwrap();
        assert_eq!(history.source(), "same😀same😀last");
        assert_eq!(
            history.mapping(),
            vec![
                NoteSpliceMapping {
                    start: 6,
                    end: 10,
                    inserted_length: 4
                },
                NoteSpliceMapping {
                    start: 12,
                    end: 16,
                    inserted_length: 4
                },
            ]
        );
    }

    #[test]
    fn compose_effect_inside_and_across_inserted_text() {
        let mut history = NoteSourceHistory::new("abcdef".into());
        history.apply_phase(&[edit(2, 3, "XYZ")]).unwrap();
        history.apply_phase(&[edit(3, 6, "!")]).unwrap();
        assert_eq!(history.source(), "abX!ef");
        assert_eq!(
            history.mapping(),
            vec![NoteSpliceMapping {
                start: 2,
                end: 4,
                inserted_length: 2,
            }]
        );
    }

    #[test]
    fn insertion_cancellation_and_savepoint_fallback_preserve_provenance() {
        let mut history = NoteSourceHistory::new("a😀b".into());
        history.apply_phase(&[edit(1, 1, "XYZ")]).unwrap();
        let fallback = history.clone();
        history.apply_phase(&[edit(1, 4, "")]).unwrap();
        assert_eq!(history.source(), "a😀b");
        assert!(history.mapping().is_empty());
        assert_eq!(fallback.source(), "aXYZ😀b");
        assert_eq!(
            fallback.mapping(),
            vec![NoteSpliceMapping {
                start: 1,
                end: 1,
                inserted_length: 3,
            }]
        );
        assert!(history.apply_phase(&[edit(2, 3, "bad")]).is_err());
        assert_eq!(history.source(), "a😀b");
        assert!(history.mapping().is_empty());
    }
}
