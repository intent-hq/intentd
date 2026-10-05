//! Bounded literal SOURCE search using pinned Unicode default full case folding.
//!
//! The Store owns authenticated cursor/view/query identity, scalar-safe source
//! reads, selection-union normalization, resource admission and hit IDs. This
//! module neither reads source nor implements rendered-text search.

use std::collections::VecDeque;

use crate::note_mutation::NoteMutationError;
use serde::{Deserialize, Serialize};

mod folding;
#[cfg(test)]
mod tests;

/// The folding data is independent of the compiler/platform Unicode version.
pub const UNICODE_VERSION: &str = "17.0.0";
/// SHA-256 of the complete vendored Unicode input, before selecting C/F records.
pub const CASE_FOLDING_SHA256: &str =
    "ff8d8fefbf123574205085d6714c36149eb946d717a0c585c27f0f4ef58c4183";
/// Limit applies to the original query, not its folded expansion.
pub const MAX_QUERY_BYTES: usize = 1024;
/// Verified maximum number of folded scalars in one pinned C/F mapping.
pub const MAX_FOLD_EXPANSION: usize = 3;
/// Conservative scalar bound; every input scalar occupies at least one byte.
pub const MAX_PATTERN_SCALARS: usize = MAX_QUERY_BYTES * MAX_FOLD_EXPANSION;
/// Replay carry retains at most one original scalar per folded pattern scalar.
pub const MAX_CARRY_BYTES: usize = MAX_PATTERN_SCALARS * 4;
const SAFE_INTEGER: u64 = 9_007_199_254_740_991;
const CARRY_VERSION: u8 = 1;

/// Half-open original UTF-16 source span, never an offset in folded text.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NoteStageSearchRange {
    pub start: u64,
    pub end: u64,
}

/// Bounded replay state, to be authenticated by the Store with the frozen view
/// and selected union. Decoding alone is NOT validation: use `restore` before
/// consuming it. The transport must cap encoded size before deserialization.
/// A replay tail is not a source read or an independently authoritative source.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NoteStageSearchCarry {
    pub version: u8,
    pub folding_sha256: String,
    pub query: String,
    pub next_offset: Option<u64>,
    pub source: String,
}

/// KMP matching with original scalar-boundary provenance. For a folded pattern
/// of m scalars, logical storage is: m chars + m usize prefix entries + m optional
/// u64 boundaries + m original chars, plus <=1024 query bytes. Snapshot adds at
/// most 4m source bytes and the query/version identity. Allocator overhead is not
/// included in these logical counts. No resident state grows with source length.
#[derive(Debug)]
pub struct NoteStageSearch {
    query: String,
    pattern: Vec<char>,
    prefix: Vec<usize>,
    matched: usize,
    boundaries: VecDeque<Option<u64>>,
    source: VecDeque<char>,
    next_offset: Option<u64>,
}

fn fold(scalar: char) -> ([char; MAX_FOLD_EXPANSION], usize) {
    let mut result = [scalar, '\0', '\0'];
    if let Ok(index) = folding::TABLE.binary_search_by_key(&scalar, |(key, _)| *key) {
        let mapping = folding::TABLE[index].1;
        result[..mapping.len()].copy_from_slice(mapping);
        (result, mapping.len())
    } else {
        (result, 1)
    }
}

impl NoteStageSearch {
    /// Compile an original, nonempty, NUL-free UTF-8 query. No trim, locale
    /// tailoring, normalization or regular-expression interpretation occurs.
    /// # Errors
    /// Invalid query text or an original query exceeding 1024 UTF-8 bytes.
    pub fn new(query: &str) -> Result<Self, NoteMutationError> {
        if query.is_empty() || query.len() > MAX_QUERY_BYTES || query.contains('\0') {
            return Err(NoteMutationError::Invalid);
        }
        let mut pattern = Vec::with_capacity(query.len() * MAX_FOLD_EXPANSION);
        for scalar in query.chars() {
            let (folded, length) = fold(scalar);
            pattern.extend_from_slice(&folded[..length]);
        }
        let mut prefix = vec![0; pattern.len()];
        for at in 1..pattern.len() {
            let mut length = prefix[at - 1];
            while length > 0 && pattern[at] != pattern[length] {
                length = prefix[length - 1];
            }
            if pattern[at] == pattern[length] {
                length += 1;
            }
            prefix[at] = length;
        }
        Ok(Self {
            query: query.into(),
            boundaries: VecDeque::with_capacity(pattern.len()),
            source: VecDeque::with_capacity(pattern.len()),
            pattern,
            prefix,
            matched: 0,
            next_offset: None,
        })
    }

    /// Consume one complete original scalar at its absolute UTF-16 start. The
    /// first scalar may start anywhere; subsequent input must be contiguous.
    /// Matches overlap and are emitted only at the end of an original scalar.
    /// A fixed nonempty pattern has at most one valid match ending at that point.
    /// # Errors
    /// NUL, discontinuity, or offsets outside safe-integer source coordinates.
    /// Invalid input leaves matcher state unchanged.
    pub fn push_scalar(
        &mut self,
        scalar: char,
        absolute_start: u64,
    ) -> Result<Option<NoteStageSearchRange>, NoteMutationError> {
        let end = absolute_start
            .checked_add(scalar.len_utf16() as u64)
            .filter(|end| *end <= SAFE_INTEGER)
            .ok_or(NoteMutationError::Invalid)?;
        if scalar == '\0'
            || self
                .next_offset
                .is_some_and(|expected| expected != absolute_start)
        {
            return Err(NoteMutationError::Invalid);
        }
        let (folded, length) = fold(scalar);
        let mut hit = None;
        for (part, &value) in folded[..length].iter().enumerate() {
            // Pop BEFORE push: queue capacity and logical length stay <= m.
            if self.boundaries.len() == self.pattern.len() {
                self.boundaries.pop_front();
            }
            self.boundaries
                .push_back((part == 0).then_some(absolute_start));
            while self.matched > 0 && self.pattern[self.matched] != value {
                self.matched = self.prefix[self.matched - 1];
            }
            if self.pattern[self.matched] == value {
                self.matched += 1;
            }
            if self.matched == self.pattern.len() {
                if part + 1 == length {
                    if let Some(Some(start)) = self.boundaries.front() {
                        hit = Some(NoteStageSearchRange { start: *start, end });
                    }
                }
                // Retain the suffix needed to emit every overlapping match.
                self.matched = self.prefix[self.matched - 1];
            }
        }
        if self.source.len() == self.pattern.len() {
            self.source.pop_front();
        }
        self.source.push_back(scalar);
        self.next_offset = Some(end);
        Ok(hit)
    }

    /// Start a later selection-union interval without bridging its gap. Touching
    /// intervals are contiguous: an equal offset preserves carry. The caller
    /// sorts/merges overlapping intervals before feeding this matcher.
    /// # Errors
    /// Backward movement or a non-safe-integer offset. Errors leave state intact.
    pub fn reset_at_gap(&mut self, next_start: u64) -> Result<(), NoteMutationError> {
        if next_start > SAFE_INTEGER || self.next_offset.is_some_and(|end| next_start < end) {
            return Err(NoteMutationError::Invalid);
        }
        if self.next_offset != Some(next_start) {
            self.matched = 0;
            self.boundaries.clear();
            self.source.clear();
            self.next_offset = Some(next_start);
        }
        Ok(())
    }

    /// Capture at a complete original scalar boundary, including after a hit.
    /// The caller binds this to the frozen source/selection before retaining it.
    #[must_use]
    pub fn snapshot(&self) -> NoteStageSearchCarry {
        NoteStageSearchCarry {
            version: CARRY_VERSION,
            folding_sha256: CASE_FOLDING_SHA256.into(),
            query: self.query.clone(),
            next_offset: self.next_offset,
            source: self.source.iter().collect(),
        }
    }

    /// Original-source interval sufficient to rebuild this matcher at its scan
    /// frontier. The Store may bind these two offsets into its short authenticated
    /// cursor instead of serializing the carry. Read this range from the SAME
    /// frozen view, feed it into a new matcher with the SAME query, suppress all
    /// replay hits, then resume at `end`. Replay work/bytes still require admission.
    /// At most m original scalars (<=2m UTF-16 units, <=4m UTF-8 bytes) are needed.
    ///
    /// None means no initial position was set; an empty range means a gap reset.
    /// Pending hit emission/count/identity is owned by the Store, not this range.
    #[must_use]
    pub fn replay_range(&self) -> Option<NoteStageSearchRange> {
        let end = self.next_offset?;
        let units: u64 = self.source.iter().map(|c| c.len_utf16() as u64).sum();
        Some(NoteStageSearchRange {
            start: end - units,
            end,
        })
    }

    /// Validate bounded carry and recompute KMP/boundary state from its original
    /// scalar tail. Past hits are discarded, so resuming after a hit emits it
    /// exactly once. At most m original scalars suffice to rebuild all future
    /// prefixes because each scalar folds to at least one scalar.
    ///
    /// This checks structure/query/version, NOT authenticity against the source.
    /// The Store must authenticate the entire carry and frozen-view identity.
    /// # Errors
    /// Wrong query/version, oversized/NUL carry, impossible offsets or bad query.
    pub fn restore(query: &str, carry: &NoteStageSearchCarry) -> Result<Self, NoteMutationError> {
        let mut search = Self::new(query)?;
        if carry.version != CARRY_VERSION
            || carry.folding_sha256 != CASE_FOLDING_SHA256
            || carry.query != query
            || carry.source.len() > search.pattern.len() * 4
            || carry.source.chars().count() > search.pattern.len()
            || carry.source.contains('\0')
            || carry.next_offset.is_some_and(|end| end > SAFE_INTEGER)
        {
            return Err(NoteMutationError::Invalid);
        }
        let Some(end) = carry.next_offset else {
            return if carry.source.is_empty() {
                Ok(search)
            } else {
                Err(NoteMutationError::Invalid)
            };
        };
        let units = carry.source.chars().map(|c| c.len_utf16() as u64).sum();
        let mut offset = end.checked_sub(units).ok_or(NoteMutationError::Invalid)?;
        search.reset_at_gap(offset)?;
        for scalar in carry.source.chars() {
            search.push_scalar(scalar, offset)?;
            offset += scalar.len_utf16() as u64;
        }
        Ok(search)
    }
}
