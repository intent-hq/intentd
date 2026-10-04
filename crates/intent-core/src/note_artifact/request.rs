//! Prepared artifact descriptors. Validation establishes shape and numeric bounds,
//! not authorization, source ownership, profile support, or capacity admission.
use crate::note_page::NoteScope;
use serde::{Deserialize, Serialize};

pub const SAFE_INTEGER: u64 = 9_007_199_254_740_991;

#[cfg(test)]
#[path = "request/timestamp_capture.rs"]
mod timestamp_capture;

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

/// Immutable begin identity. Validation does not authorize a renderer or source.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ArtifactBegin {
    pub job_id: String,
    pub expires_at: String,
    pub header: ArtifactHeader,
    pub header_digest: String,
}

impl ArtifactBegin {
    /// Verify the exact job, expiry and header integrity envelope.
    ///
    /// # Errors
    /// Rejects malformed fields, non-UTC expiry, or a mismatched digest.
    pub fn validate(&self, workspace_id: &str) -> Result<(), RequestError> {
        bounded_text(&self.job_id, 256)?;
        bounded_text(&self.expires_at, 64)?;
        self.header.validate(workspace_id)?;
        let expiry = crate::parse_iso(&self.expires_at).ok_or(RequestError::Invalid)?;
        if !expiry.offset().is_utc()
            || self.header_digest.len() != 64
            || !self
                .header_digest
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(RequestError::Invalid);
        }
        let envelope = serde_json::json!({
            "domain":"note.artifact.begin.v1", "jobId":self.job_id,
            "expiresAt":self.expires_at, "header":self.header
        });
        let digest =
            super::canonical::digest(&envelope.to_string()).map_err(|_| RequestError::Invalid)?;
        if digest != self.header_digest {
            return Err(RequestError::Invalid);
        }
        Ok(())
    }
}

/// One exact record in the immutable artifact digest chain.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ArtifactAppend {
    pub job_ref: String,
    pub sequence: u64,
    pub previous_digest: String,
    pub record: String,
    pub digest: String,
}

impl ArtifactAppend {
    /// Check framing and chain integrity using the stored job header digest.
    /// This does not validate native profile semantics or physical index costs.
    ///
    /// # Errors
    /// Rejects oversized/malformed records, invalid counters, or wrong digests.
    pub fn validate(&self, header_digest: &str) -> Result<(), RequestError> {
        bounded_text(&self.job_ref, 256)?;
        safe_integer(self.sequence)?;
        for digest in [header_digest, &self.previous_digest, &self.digest] {
            if digest.len() != 64
                || !digest
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            {
                return Err(RequestError::Invalid);
            }
        }
        super::decode_record(&self.record).map_err(|_| RequestError::Invalid)?;
        let envelope = serde_json::json!([
            "note.artifact.append.v1",
            header_digest,
            self.sequence,
            self.previous_digest,
            self.record
        ]);
        let digest =
            super::canonical::digest(&envelope.to_string()).map_err(|_| RequestError::Invalid)?;
        if digest != self.digest {
            return Err(RequestError::Invalid);
        }
        Ok(())
    }
}

/// Exact accepted-prefix totals requested for private sealing.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ArtifactSeal {
    pub job_ref: String,
    pub expected_records: u64,
    pub expected_bytes: u64,
    pub final_digest: String,
}

impl ArtifactSeal {
    /// Validate bounded seal arguments, independently of stored/profile state.
    ///
    /// # Errors
    /// Rejects malformed handles/digests and zero or unsafe counters.
    pub fn validate(&self) -> Result<(), RequestError> {
        bounded_text(&self.job_ref, 256)?;
        for count in [self.expected_records, self.expected_bytes] {
            safe_integer(count)?;
            if count == 0 {
                return Err(RequestError::Invalid);
            }
        }
        digest_text(&self.final_digest)
    }
}

/// Idempotent admission identity for one privately sealed generation.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ArtifactAdmit {
    pub job_ref: String,
    pub admission_id: String,
    pub final_digest: String,
}

impl ArtifactAdmit {
    /// Validate the immutable lease-admission request shape.
    ///
    /// # Errors
    /// Rejects malformed identifiers or a noncanonical digest string.
    pub fn validate(&self) -> Result<(), RequestError> {
        bounded_text(&self.job_ref, 256)?;
        bounded_text(&self.admission_id, 256)?;
        digest_text(&self.final_digest)
    }
}

fn digest_text(value: &str) -> Result<(), RequestError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(RequestError::Invalid);
    }
    Ok(())
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
    fn begin_identity_matches_frozen_fractional_header_digest() {
        let fixture: Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/native_artifact_canonicalization.json"
        ))
        .unwrap();
        let vector = &fixture["canonicalization"]["header"];
        let mut raw: Value = serde_json::from_str(vector["rawJson"].as_str().unwrap()).unwrap();
        raw.as_object_mut().unwrap().remove("domain");
        raw["headerDigest"] = vector["sha256"].clone();
        let request: ArtifactBegin = serde_json::from_value(raw).unwrap();
        assert!(request.validate("ws-a").is_ok());
        assert!(request.validate("another-workspace").is_err());
        for field in 0..4 {
            let mut changed = request.clone();
            match field {
                0 => changed.job_id.push('x'),
                1 => changed.expires_at = "2026-10-04T01:00:01Z".into(),
                2 => changed.header.environment.width += 0.25,
                _ => changed.header_digest.make_ascii_uppercase(),
            }
            assert!(changed.validate("ws-a").is_err());
        }
    }

    #[test]
    fn begin_rejects_signed_non_utc_or_malformed_expiry() {
        for expiry in ["not-a-date", "2026-10-04T02:00:00+01:00"] {
            let header = header();
            let digest = super::super::canonical::digest(
                &json!({
                    "domain":"note.artifact.begin.v1","jobId":"job",
                    "expiresAt":expiry,"header":header
                })
                .to_string(),
            )
            .unwrap();
            let request: ArtifactBegin = serde_json::from_value(json!({
                "jobId":"job","expiresAt":expiry,"header":header,"headerDigest":digest
            }))
            .unwrap();
            assert!(request.validate("w").is_err());
        }
    }

    #[test]
    fn append_matches_frozen_exact_record_vectors() {
        let fixture: Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/native_artifact_canonicalization.json"
        ))
        .unwrap();
        for vector in fixture["canonicalization"]["append"].as_array().unwrap() {
            let raw: Value = serde_json::from_str(vector["rawJson"].as_str().unwrap()).unwrap();
            let request = ArtifactAppend {
                job_ref: "opaque-job".into(),
                sequence: raw[2].as_u64().unwrap(),
                previous_digest: raw[3].as_str().unwrap().into(),
                record: raw[4].as_str().unwrap().into(),
                digest: vector["sha256"].as_str().unwrap().into(),
            };
            let header = raw[1].as_str().unwrap();
            assert!(request.validate(header).is_ok());
            for field in 0..4 {
                let mut changed = request.clone();
                match field {
                    0 => changed.sequence += 1,
                    1 => changed.record.push(' '),
                    2 => changed.previous_digest = "0".repeat(64),
                    _ => changed.sequence = SAFE_INTEGER + 1,
                }
                assert!(changed.validate(header).is_err());
            }
        }
    }

    #[test]
    fn publication_requests_reject_invalid_totals_and_identity() {
        let seal = ArtifactSeal {
            job_ref: "opaque-job".into(),
            expected_records: 1,
            expected_bytes: 10,
            final_digest: "a".repeat(64),
        };
        assert!(seal.validate().is_ok());
        for count in [0, SAFE_INTEGER + 1] {
            let mut bad = seal.clone();
            bad.expected_records = count;
            assert!(bad.validate().is_err());
            let mut bad = seal.clone();
            bad.expected_bytes = count;
            assert!(bad.validate().is_err());
        }
        let admit = ArtifactAdmit {
            job_ref: seal.job_ref,
            admission_id: "admission".into(),
            final_digest: seal.final_digest,
        };
        assert!(admit.validate().is_ok());
        for id in [String::new(), "x".repeat(257), "a\0b".into()] {
            let mut bad = admit.clone();
            bad.admission_id = id;
            assert!(bad.validate().is_err());
        }
        let mut bad = admit;
        bad.final_digest.make_ascii_uppercase();
        assert!(bad.validate().is_err());
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
