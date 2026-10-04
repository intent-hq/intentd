//! Prepared artifact descriptors. Validation establishes shape and numeric bounds,
//! not authorization, source ownership, profile support, or capacity admission.
use crate::note_page::NoteScope;
use serde::{Deserialize, Serialize};

pub const SAFE_INTEGER: u64 = 9_007_199_254_740_991;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestError {
    Invalid,
    Budget,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum ArtifactSource {
    #[serde(rename_all = "camelCase")]
    Snapshot {
        snapshot_id: String,
        source_revision: String,
        owner_ref: String,
        source_ref: String,
    },
    #[serde(rename_all = "camelCase")]
    SessionLive {
        frozen_view_ref: String,
        editor_session_id: String,
        local_edit_sequence: u64,
        live_generation: String,
        owner_ref: String,
        source_ref: String,
    },
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Primitive {
    Diff,
    Mermaid,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Theme {
    Light,
    Dark,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Environment {
    pub width: f64,
    pub height: f64,
    pub theme: Theme,
    pub font_ref: String,
    pub font_size: f64,
    pub device_pixel_ratio: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Reservation {
    pub payload_bytes: u64,
    pub records: u64,
    pub index_entries: u64,
    pub storage_charge_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ArtifactHeader {
    pub scope: NoteScope,
    pub source: ArtifactSource,
    pub primitive: Primitive,
    pub profile: String,
    pub environment: Environment,
    pub reservation: Reservation,
}

fn bounded_text(value: &str, maximum: usize) -> Result<(), RequestError> {
    if value.is_empty() || value.contains('\0') {
        return Err(RequestError::Invalid);
    }
    if value.len() > maximum {
        return Err(RequestError::Budget);
    }
    Ok(())
}

fn safe_integer(value: u64) -> Result<(), RequestError> {
    if value > SAFE_INTEGER {
        return Err(RequestError::Invalid);
    }
    Ok(())
}

fn positive(value: f64) -> Result<(), RequestError> {
    if !value.is_finite() || value <= 0.0 {
        return Err(RequestError::Invalid);
    }
    Ok(())
}

fn nonnegative(value: f64) -> Result<(), RequestError> {
    if !value.is_finite() || value < 0.0 {
        return Err(RequestError::Invalid);
    }
    Ok(())
}

impl ArtifactHeader {
    /// Check the untrusted descriptor against the captured request workspace.
    /// The service must independently authorize the principal and stored grant.
    ///
    /// # Errors
    /// Returns `Invalid` for invalid scope or scalar values and `Budget` for
    /// strings exceeding the declared protocol limits.
    pub fn validate(&self, workspace_id: &str) -> Result<(), RequestError> {
        for id in [
            &self.scope.backend_id,
            &self.scope.workspace_id,
            &self.scope.note_id,
            &self.scope.note_instance_id,
        ] {
            bounded_text(id, 256)?;
        }
        if self.scope.workspace_id != workspace_id {
            return Err(RequestError::Invalid);
        }
        match &self.source {
            ArtifactSource::Snapshot {
                snapshot_id,
                source_revision,
                owner_ref,
                source_ref,
            } => {
                for value in [snapshot_id, source_revision, owner_ref, source_ref] {
                    bounded_text(value, 256)?;
                }
            }
            ArtifactSource::SessionLive {
                frozen_view_ref,
                editor_session_id,
                local_edit_sequence,
                live_generation,
                owner_ref,
                source_ref,
            } => {
                safe_integer(*local_edit_sequence)?;
                for value in [
                    frozen_view_ref,
                    editor_session_id,
                    live_generation,
                    owner_ref,
                    source_ref,
                ] {
                    bounded_text(value, 256)?;
                }
            }
        }
        bounded_text(&self.profile, 128)?;
        bounded_text(&self.environment.font_ref, 256)?;
        for value in [
            self.environment.width,
            self.environment.height,
            self.environment.font_size,
            self.environment.device_pixel_ratio,
        ] {
            positive(value)?;
        }
        for count in [
            self.reservation.payload_bytes,
            self.reservation.records,
            self.reservation.index_entries,
            self.reservation.storage_charge_bytes,
        ] {
            safe_integer(count)?;
            if count == 0 {
                return Err(RequestError::Invalid);
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Viewport {
    pub top: f64,
    pub height: f64,
    pub left: f64,
    pub width: f64,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DiffSide {
    Addition,
    Deletion,
    Context,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase", deny_unknown_fields)]
pub enum LocateTarget {
    Row { row: u64 },
    Line { side: DiffSide, line: u64 },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
pub enum ReadSelector {
    #[serde(rename = "manifest")]
    Manifest {},
    #[serde(rename = "diff.rows")]
    DiffRows { viewport: Viewport },
    #[serde(rename = "diff.fragments")]
    DiffFragments { row: u64, left: f64, right: f64 },
    #[serde(rename = "diff.hunks", rename_all = "camelCase")]
    DiffHunks { start_ordinal: u64 },
    #[serde(rename = "diff.locate")]
    DiffLocate { target: LocateTarget },
}

impl ReadSelector {
    /// Validate selector coordinates and safe-integer indices.
    ///
    /// # Errors
    /// Returns `Invalid` for nonfinite, negative, unordered or unsafe values.
    pub fn validate(&self) -> Result<(), RequestError> {
        match self {
            Self::Manifest {} => Ok(()),
            Self::DiffRows { viewport } => {
                nonnegative(viewport.top)?;
                nonnegative(viewport.left)?;
                positive(viewport.height)?;
                positive(viewport.width)?;
                positive(viewport.top + viewport.height)?;
                positive(viewport.left + viewport.width)
            }
            Self::DiffFragments { row, left, right } => {
                safe_integer(*row)?;
                // Measured fragments can overhang; query positions retain the
                // contract's nonnegative viewport coordinate domain.
                nonnegative(*left)?;
                positive(*right)?;
                if right <= left {
                    return Err(RequestError::Invalid);
                }
                Ok(())
            }
            Self::DiffHunks { start_ordinal } => safe_integer(*start_ordinal),
            Self::DiffLocate { target } => match target {
                LocateTarget::Row { row } => safe_integer(*row),
                LocateTarget::Line { line, .. } => {
                    safe_integer(*line)?;
                    if *line == 0 {
                        return Err(RequestError::Invalid);
                    }
                    Ok(())
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    fn header() -> Value {
        json!({"scope":{"backendId":"b","workspaceId":"w","noteId":"n","noteInstanceId":"i"},
            "source":{"kind":"snapshot","snapshotId":"s","sourceRevision":"r","ownerRef":"o","sourceRef":"v"},
            "primitive":"diff","profile":"registered-profile",
            "environment":{"width":800.5,"height":600,"theme":"dark","fontRef":"font","fontSize":14,"devicePixelRatio":1.25},
            "reservation":{"payloadBytes":16384,"records":2,"indexEntries":3,"storageChargeBytes":32768}})
    }

    #[test]
    fn descriptor_validation_does_not_grant_source_authority() {
        let value: ArtifactHeader = serde_json::from_value(header()).unwrap();
        assert!(value.validate("w").is_ok());
        assert_eq!(value.validate("other"), Err(RequestError::Invalid));
    }

    #[test]
    fn source_discriminants_do_not_accept_mixed_or_missing_grants() {
        let mut value = header();
        value["source"]["frozenViewRef"] = json!("unexpected");
        assert!(serde_json::from_value::<ArtifactHeader>(value).is_err());
        let mut value = header();
        value["source"] = json!({"kind":"session-live","frozenViewRef":"f","editorSessionId":"e","localEditSequence":SAFE_INTEGER,"liveGeneration":"g","ownerRef":"o","sourceRef":"s"});
        let parsed: ArtifactHeader = serde_json::from_value(value.clone()).unwrap();
        assert!(parsed.validate("w").is_ok());
        value["source"]["localEditSequence"] = json!(SAFE_INTEGER + 1);
        let parsed: ArtifactHeader = serde_json::from_value(value).unwrap();
        assert_eq!(parsed.validate("w"), Err(RequestError::Invalid));
    }

    #[test]
    fn all_nested_header_objects_reject_unknown_fields() {
        for path in ["scope", "source", "environment", "reservation"] {
            let mut value = header();
            value[path]["unexpected"] = json!(true);
            assert!(
                serde_json::from_value::<ArtifactHeader>(value).is_err(),
                "{path}"
            );
        }
        let mut value = header();
        value["unexpected"] = json!(true);
        assert!(serde_json::from_value::<ArtifactHeader>(value).is_err());
    }

    #[test]
    fn reservations_and_environment_require_positive_finite_domains() {
        for key in [
            "payloadBytes",
            "records",
            "indexEntries",
            "storageChargeBytes",
        ] {
            for invalid in [0, SAFE_INTEGER + 1] {
                let mut value = header();
                value["reservation"][key] = json!(invalid);
                assert_eq!(
                    serde_json::from_value::<ArtifactHeader>(value)
                        .unwrap()
                        .validate("w"),
                    Err(RequestError::Invalid)
                );
            }
        }
        let mut parsed: ArtifactHeader = serde_json::from_value(header()).unwrap();
        for invalid in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            parsed.environment.width = invalid;
            assert_eq!(parsed.validate("w"), Err(RequestError::Invalid));
        }
    }

    #[test]
    fn identifier_limits_count_utf8_bytes_and_reject_nul() {
        let mut parsed: ArtifactHeader = serde_json::from_value(header()).unwrap();
        parsed.profile = "😀".repeat(32);
        assert!(parsed.validate("w").is_ok());
        parsed.profile.push('x');
        assert_eq!(parsed.validate("w"), Err(RequestError::Budget));
        parsed.profile = "bad\0profile".into();
        assert_eq!(parsed.validate("w"), Err(RequestError::Invalid));
    }

    #[test]
    fn selectors_keep_exact_domains_and_finite_endpoints() {
        for value in [
            json!({"kind":"manifest"}),
            json!({"kind":"diff.rows","viewport":{"top":10.5,"height":20,"left":0,"width":40}}),
            json!({"kind":"diff.fragments","row":0,"left":0,"right":10}),
            json!({"kind":"diff.hunks","startOrdinal":SAFE_INTEGER}),
            json!({"kind":"diff.locate","target":{"kind":"line","side":"context","line":1}}),
        ] {
            let selector: ReadSelector = serde_json::from_value(value).unwrap();
            assert!(selector.validate().is_ok());
        }
        let overflow = ReadSelector::DiffRows {
            viewport: Viewport {
                top: f64::MAX,
                height: f64::MAX,
                left: 0.0,
                width: 1.0,
            },
        };
        assert_eq!(overflow.validate(), Err(RequestError::Invalid));
        for value in [
            json!({"kind":"diff.fragments","row":0,"left":10,"right":10}),
            json!({"kind":"diff.locate","target":{"kind":"line","side":"addition","line":0}}),
            json!({"kind":"diff.hunks","startOrdinal":SAFE_INTEGER+1}),
        ] {
            let selector: ReadSelector = serde_json::from_value(value).unwrap();
            assert_eq!(selector.validate(), Err(RequestError::Invalid));
        }
        assert!(
            serde_json::from_value::<ReadSelector>(json!({"kind":"manifest","row":0})).is_err()
        );
    }
}
