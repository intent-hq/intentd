use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use intent_core::events::NOTE_UPDATED;
use intent_core::{Event, Note, NoteId, WorkspaceId};
use intent_store::Store;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::classifier::Classifier;

pub const MANIFEST_TAG: &str = "semantic-map";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ManifestSource {
    #[default]
    Curated,
    Structural,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Manifest {
    pub version: u32,
    pub regions: Vec<Region>,
    #[serde(default)]
    pub crossings: Vec<Crossing>,
    #[serde(skip)]
    pub source: ManifestSource,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Region {
    pub id: String,
    pub label: String,
    pub responsibility: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    pub anchor: [f32; 2],
    pub paths: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub color: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Crossing {
    pub from: String,
    pub to: String,
    pub label: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManifestError {
    pub field: String,
    pub message: String,
}

impl ManifestError {
    fn new(field: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            message: message.into(),
        }
    }
}

impl fmt::Display for ManifestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.field, self.message)
    }
}

impl std::error::Error for ManifestError {}

/// Parses and validates a semantic-map manifest.
///
/// # Errors
///
/// Returns an error when the document is not valid JSON or violates the manifest schema.
pub fn parse_manifest(content: &str) -> Result<Manifest, ManifestError> {
    let json = fenced_json(content)?;
    let value: Value = serde_json::from_str(json)
        .map_err(|error| ManifestError::new("document", error.to_string()))?;
    validate_manifest(&value)?;
    serde_json::from_value(value).map_err(|error| ManifestError::new("document", error.to_string()))
}

fn fenced_json(content: &str) -> Result<&str, ManifestError> {
    let trimmed = content.trim();
    if !trimmed.starts_with("```") {
        return Ok(trimmed);
    }
    let first_newline = trimmed
        .find('\n')
        .ok_or_else(|| ManifestError::new("document", "JSON fence has no body"))?;
    let body = &trimmed[first_newline + 1..];
    let closing = body
        .rfind("```")
        .ok_or_else(|| ManifestError::new("document", "JSON fence is not closed"))?;
    if !body[closing + 3..].trim().is_empty() {
        return Err(ManifestError::new(
            "document",
            "content after the JSON fence is not allowed",
        ));
    }
    Ok(body[..closing].trim())
}

fn validate_manifest(value: &Value) -> Result<(), ManifestError> {
    let object = value
        .as_object()
        .ok_or_else(|| ManifestError::new("document", "must be a JSON object"))?;
    let version = required_u32(object.get("version"), "version")?;
    if version != 1 {
        return Err(ManifestError::new(
            "version",
            "unsupported manifest version",
        ));
    }
    let regions = required_array(object.get("regions"), "regions")?;
    for (index, region) in regions.iter().enumerate() {
        validate_region(region, index)?;
    }
    if let Some(crossings) = object.get("crossings") {
        for (index, crossing) in required_array(Some(crossings), "crossings")?
            .iter()
            .enumerate()
        {
            let prefix = format!("crossings[{index}]");
            let crossing = crossing
                .as_object()
                .ok_or_else(|| ManifestError::new(&prefix, "must be an object"))?;
            for field in ["from", "to", "label"] {
                required_string(crossing.get(field), &format!("{prefix}.{field}"))?;
            }
        }
    }
    Ok(())
}

fn validate_region(value: &Value, index: usize) -> Result<(), ManifestError> {
    let prefix = format!("regions[{index}]");
    let region = value
        .as_object()
        .ok_or_else(|| ManifestError::new(&prefix, "must be an object"))?;
    for field in ["id", "label", "responsibility"] {
        required_string(region.get(field), &format!("{prefix}.{field}"))?;
    }
    if let Some(parent) = region.get("parent") {
        required_string(Some(parent), &format!("{prefix}.parent"))?;
    }
    if let Some(color) = region.get("color") {
        required_string(Some(color), &format!("{prefix}.color"))?;
    }
    let anchor = required_array(region.get("anchor"), &format!("{prefix}.anchor"))?;
    if anchor.len() != 2 {
        return Err(ManifestError::new(
            format!("{prefix}.anchor"),
            "must contain exactly two numbers",
        ));
    }
    for (coordinate, value) in anchor.iter().enumerate() {
        let number = value.as_f64().ok_or_else(|| {
            ManifestError::new(format!("{prefix}.anchor[{coordinate}]"), "must be a number")
        })?;
        if !number.is_finite() || !(0.0..=1.0).contains(&number) {
            return Err(ManifestError::new(
                format!("{prefix}.anchor[{coordinate}]"),
                "must be between 0 and 1",
            ));
        }
    }
    for (path_index, path) in required_array(region.get("paths"), &format!("{prefix}.paths"))?
        .iter()
        .enumerate()
    {
        required_string(Some(path), &format!("{prefix}.paths[{path_index}]"))?;
    }
    Ok(())
}

fn required_string<'a>(value: Option<&'a Value>, field: &str) -> Result<&'a str, ManifestError> {
    value
        .ok_or_else(|| ManifestError::new(field, "is required"))?
        .as_str()
        .ok_or_else(|| ManifestError::new(field, "must be a string"))
}

fn required_array<'a>(
    value: Option<&'a Value>,
    field: &str,
) -> Result<&'a Vec<Value>, ManifestError> {
    value
        .ok_or_else(|| ManifestError::new(field, "is required"))?
        .as_array()
        .ok_or_else(|| ManifestError::new(field, "must be an array"))
}

fn required_u32(value: Option<&Value>, field: &str) -> Result<u32, ManifestError> {
    let value = value.ok_or_else(|| ManifestError::new(field, "is required"))?;
    let number = value
        .as_u64()
        .ok_or_else(|| ManifestError::new(field, "must be a non-negative integer"))?;
    u32::try_from(number).map_err(|_| ManifestError::new(field, "is too large"))
}

#[derive(Clone)]
struct CachedManifest {
    note_id: NoteId,
    manifest: Manifest,
    classifier: Arc<Classifier>,
    coverage: Option<(Option<String>, usize, usize)>,
}

#[derive(Clone, Default)]
pub struct ManifestLoader {
    cache: Arc<Mutex<HashMap<WorkspaceId, CachedManifest>>>,
}

impl ManifestLoader {
    /// Loads the workspace manifest from notes, reusing a cached valid manifest when available.
    ///
    /// # Errors
    ///
    /// Returns an error when notes cannot be loaded or the manifest note is invalid.
    pub async fn load(
        &self,
        store: &Store,
        workspace_id: &WorkspaceId,
    ) -> Result<Option<Manifest>, ManifestLoadError> {
        self.load_compiled(store, workspace_id)
            .await
            .map(|loaded| loaded.map(|(manifest, _)| manifest))
    }

    /// Loads the workspace manifest and its shared precompiled classifier.
    ///
    /// # Errors
    ///
    /// Returns an error when notes cannot be loaded or the manifest note is invalid.
    pub async fn load_compiled(
        &self,
        store: &Store,
        workspace_id: &WorkspaceId,
    ) -> Result<Option<(Manifest, Arc<Classifier>)>, ManifestLoadError> {
        if let Some(cached) = self.cached(workspace_id) {
            return Ok(Some((cached.manifest, cached.classifier)));
        }
        let notes = store.list_notes(workspace_id).await?;
        self.load_from_notes_compiled(workspace_id, &notes)
            .map_err(ManifestLoadError::Parse)
    }

    /// Finds, validates, and caches the manifest in a collection of notes.
    ///
    /// # Errors
    ///
    /// Returns an error when the manifest note is invalid.
    pub fn load_from_notes(
        &self,
        workspace_id: &WorkspaceId,
        notes: &[Note],
    ) -> Result<Option<Manifest>, ManifestError> {
        self.load_from_notes_compiled(workspace_id, notes)
            .map(|loaded| loaded.map(|(manifest, _)| manifest))
    }

    fn load_from_notes_compiled(
        &self,
        workspace_id: &WorkspaceId,
        notes: &[Note],
    ) -> Result<Option<(Manifest, Arc<Classifier>)>, ManifestError> {
        if let Some(cached) = self.cached(workspace_id) {
            return Ok(Some((cached.manifest, cached.classifier)));
        }
        let note = notes
            .iter()
            .filter(|note| note.tags.iter().any(|tag| tag == MANIFEST_TAG))
            .max_by(|left, right| {
                (&left.updated_at, &left.created_at, left.id.as_str()).cmp(&(
                    &right.updated_at,
                    &right.created_at,
                    right.id.as_str(),
                ))
            });
        let Some(note) = note else {
            return Ok(None);
        };
        let manifest = parse_manifest(&note.content)?;
        let mut cache = self
            .cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(cached) = cache.get(workspace_id).cloned() {
            return Ok(Some((cached.manifest, cached.classifier)));
        }
        let classifier = Arc::new(Classifier::new(&manifest));
        cache.insert(
            workspace_id.clone(),
            CachedManifest {
                note_id: note.id.clone(),
                manifest: manifest.clone(),
                classifier: Arc::clone(&classifier),
                coverage: None,
            },
        );
        Ok(Some((manifest, classifier)))
    }

    fn cached(&self, workspace_id: &WorkspaceId) -> Option<CachedManifest> {
        self.cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(workspace_id)
            .cloned()
    }

    pub fn coverage(
        &self,
        workspace_id: &WorkspaceId,
        worktree_version: Option<&str>,
    ) -> Option<(usize, usize)> {
        self.cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(workspace_id)
            .and_then(|cached| cached.coverage.as_ref())
            .filter(|(version, _, _)| version.as_deref() == worktree_version)
            .map(|(_, matched, total)| (*matched, *total))
    }

    pub fn set_coverage(
        &self,
        workspace_id: &WorkspaceId,
        worktree_version: Option<String>,
        matched: usize,
        total: usize,
    ) {
        if let Some(cached) = self
            .cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_mut(workspace_id)
        {
            cached.coverage = Some((worktree_version, matched, total));
        }
    }

    pub fn invalidate_on_event(&self, event: &Event) -> bool {
        if event.event_type != NOTE_UPDATED {
            return false;
        }
        let note_id = event.data.get("noteId").and_then(Value::as_str);
        self.invalidate_note_update(&event.workspace_id, note_id)
    }

    pub(crate) fn invalidate_note_updated(
        &self,
        workspace_id: &WorkspaceId,
        note_id: &NoteId,
    ) -> bool {
        self.invalidate_note_update(workspace_id, Some(note_id.as_str()))
    }

    fn invalidate_note_update(&self, workspace_id: &WorkspaceId, note_id: Option<&str>) -> bool {
        let mut cache = self
            .cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let should_remove = cache
            .get(workspace_id)
            .is_some_and(|cached| note_id.is_none_or(|note_id| cached.note_id.as_str() == note_id));
        should_remove && cache.remove(workspace_id).is_some()
    }
}

#[derive(Debug)]
pub enum ManifestLoadError {
    Store(intent_core::Error),
    Parse(ManifestError),
}

impl fmt::Display for ManifestLoadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Store(error) => write!(f, "{error}"),
            Self::Parse(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for ManifestLoadError {}

impl From<intent_core::Error> for ManifestLoadError {
    fn from(error: intent_core::Error) -> Self {
        Self::Store(error)
    }
}

#[cfg(test)]
mod tests {
    use intent_core::{ActorType, ContentType, EventActor, NoteMetadata, NoteVisibility};
    use serde_json::json;

    use super::*;

    fn note(id: &str, updated_at: &str, label: &str) -> Note {
        Note {
            id: NoteId::from(id),
            workspace_id: WorkspaceId::from("ws-1"),
            title: "Semantic map".into(),
            content: format!(
                "{{\"version\":1,\"regions\":[{{\"id\":\"{id}\",\"label\":\"{label}\",\"responsibility\":\"R\",\"anchor\":[0,0],\"paths\":[]}}]}}"
            ),
            content_type: ContentType::Markdown,
            tags: vec![MANIFEST_TAG.into()],
            is_pinned: false,
            is_archived: false,
            is_default: false,
            parent_id: None,
            visibility: NoteVisibility::Workspace,
            metadata: NoteMetadata::default(),
            created_at: updated_at.into(),
            rev: 0,
            updated_at: updated_at.into(),
        }
    }

    #[test]
    fn parse_errors_name_the_field() {
        let error = parse_manifest(
            r#"{"version":1,"regions":[{"id":"a","label":"A","responsibility":"R","anchor":[2,0],"paths":[]}]}"#,
        )
        .unwrap_err();
        assert_eq!(error.field, "regions[0].anchor[0]");
        assert!(error.to_string().contains("regions[0].anchor[0]"));
    }

    #[test]
    fn parses_json_fence_and_keeps_source_out_of_manifest_wire_shape() {
        let manifest =
            parse_manifest("```json\n{\"version\":1,\"regions\":[],\"crossings\":[]}\n```")
                .unwrap();
        assert_eq!(manifest.source, ManifestSource::Curated);
        assert_eq!(
            serde_json::to_value(&manifest).unwrap(),
            json!({"version": 1, "regions": [], "crossings": []})
        );
    }

    #[test]
    fn loader_uses_newest_tagged_note_and_invalidates_its_update() {
        let loader = ManifestLoader::default();
        let workspace_id = WorkspaceId::from("ws-1");
        let older = note("older", "2026-01-01T00:00:00Z", "Older");
        let newer = note("newer", "2026-02-01T00:00:00Z", "Newer");
        let initial_manifest = loader
            .load_from_notes(&workspace_id, &[newer.clone(), older])
            .unwrap()
            .unwrap();
        assert_eq!(initial_manifest.regions[0].label, "Newer");

        let event = Event {
            id: "event-1".into(),
            workspace_id: workspace_id.clone(),
            timestamp: "2026-03-01T00:00:00Z".into(),
            event_type: NOTE_UPDATED.into(),
            actor: EventActor {
                actor_type: ActorType::System,
                ..Default::default()
            },
            session_id: None,
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            data: json!({"noteId": "newer"}),
        };
        assert!(loader.invalidate_on_event(&event));

        let replacement = note("replacement", "2026-04-01T00:00:00Z", "Replacement");
        let replacement_manifest = loader
            .load_from_notes(&workspace_id, &[newer, replacement])
            .unwrap()
            .unwrap();
        assert_eq!(replacement_manifest.regions[0].label, "Replacement");
    }

    #[test]
    fn loader_reuses_compiled_classifier_until_manifest_invalidation() {
        let loader = ManifestLoader::default();
        let workspace_id = WorkspaceId::from("ws-1");
        let original = note("map", "2026-01-01T00:00:00Z", "Original");
        let (_, first) = loader
            .load_from_notes_compiled(&workspace_id, std::slice::from_ref(&original))
            .unwrap()
            .unwrap();
        for _ in 0..10 {
            first.classify("src/main.rs");
        }
        let (_, reused) = loader
            .load_from_notes_compiled(&workspace_id, std::slice::from_ref(&original))
            .unwrap()
            .unwrap();
        assert!(Arc::ptr_eq(&first, &reused));

        assert!(loader.invalidate_note_updated(&workspace_id, &original.id));
        let replacement = note("map", "2026-02-01T00:00:00Z", "Replacement");
        let (_, rebuilt) = loader
            .load_from_notes_compiled(&workspace_id, &[replacement])
            .unwrap()
            .unwrap();
        assert!(!Arc::ptr_eq(&first, &rebuilt));
    }

    #[test]
    fn coverage_cache_tracks_worktree_version() {
        let loader = ManifestLoader::default();
        let workspace_id = WorkspaceId::from("ws-1");
        let manifest = note("map", "2026-01-01T00:00:00Z", "Original");
        loader
            .load_from_notes_compiled(&workspace_id, &[manifest])
            .unwrap()
            .unwrap();
        loader.set_coverage(&workspace_id, Some("event-1".into()), 4, 5);
        assert_eq!(
            loader.coverage(&workspace_id, Some("event-1")),
            Some((4, 5))
        );
        assert_eq!(loader.coverage(&workspace_id, Some("event-2")), None);
    }
}
