//! Project-only skill discovery for spawned sessions. No home/environment roots.
//! Legacy public skills.list and prompt callers keep their existing scope until
//! explicitly migrated. Reuse the bounded scanner and its symlink observations.

use super::{fingerprints_current_sync, scan_targets, CachePayload, ScanTarget, SkillMetadata};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};

const ROOTS: &[&str] = &[
    ".pi/skills",
    ".agent/skills",
    ".opencode/skills",
    ".grok/skills",
    ".factory/skills",
    ".codex/skills",
    ".agents/skills",
    ".claude/skills",
    ".augment/skills",
    ".intent/skills",
];
const MAX_CACHE_ENTRIES: usize = 64;

// Keys include the complete root/cwd boundary, unlike the legacy workspace cache.
static CACHE: LazyLock<Mutex<HashMap<Vec<PathBuf>, CachePayload>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static GENERATION: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SkillProvenance {
    pub declared_path: PathBuf,
    pub canonical_path: PathBuf,
    pub resource_directory: PathBuf,
}

#[derive(Clone)]
pub(crate) struct ProjectSkillSnapshot {
    pub skills: Vec<SkillMetadata>,
    pub provenance: BTreeMap<String, SkillProvenance>,
    pub watch_directories: Vec<PathBuf>,
    pub diagnostics: Vec<String>,
    pub fingerprint: String,
    payload: Option<CachePayload>,
    generation: u64,
}

impl ProjectSkillSnapshot {
    pub(crate) fn empty() -> Self {
        Self {
            skills: vec![],
            provenance: BTreeMap::new(),
            watch_directories: vec![],
            diagnostics: vec![],
            fingerprint: String::new(),
            payload: None,
            generation: GENERATION.load(Ordering::SeqCst),
        }
    }

    /// Detect linked resource/alias replacement using the same fingerprints as
    /// legacy discovery. Watch events additionally call `invalidate_skills_cache`.
    pub(crate) fn is_current(&self) -> bool {
        self.payload.as_ref().is_none_or(|payload| {
            self.generation == GENERATION.load(Ordering::SeqCst)
                && fingerprints_current_sync(&payload.fingerprints)
        })
    }
}

pub(super) fn invalidate() {
    let mut cache = CACHE.lock().unwrap();
    GENERATION.fetch_add(1, Ordering::SeqCst);
    cache.clear();
}

/// `directories` is the validated canonical root-to-cwd sequence from
/// `spawned_provider_catalog::project_directories`. Repo-less/ephemeral callers
/// pass no directories and do not consult this cache or any host directories.
pub(crate) fn discover_project_skills(directories: &[PathBuf]) -> ProjectSkillSnapshot {
    if directories.is_empty() {
        return ProjectSkillSnapshot::empty();
    }
    let generation = GENERATION.load(Ordering::SeqCst);
    let cached = CACHE.lock().unwrap().get(directories).cloned();
    let targets: Vec<_> = directories
        .iter()
        .flat_map(|directory| ROOTS.iter().map(move |root| directory.join(root)))
        .enumerate()
        .map(|(precedence, root)| ScanTarget {
            root,
            precedence,
            scope: "project".into(),
        })
        .collect();
    let payload = if let Some(payload) =
        cached.filter(|payload| fingerprints_current_sync(&payload.fingerprints))
    {
        payload
    } else {
        let payload = scan_targets(targets.clone(), true);
        let mut cache = CACHE.lock().unwrap();
        if GENERATION.load(Ordering::SeqCst) == generation {
            if cache.len() >= MAX_CACHE_ENTRIES {
                cache.clear();
            }
            cache.insert(directories.to_vec(), payload.clone());
        }
        payload
    };
    let provenance = payload
        .skills
        .iter()
        .filter_map(|skill| {
            let declared_path = PathBuf::from(&skill.location);
            let canonical_path = declared_path.canonicalize().ok()?;
            let resource_directory = canonical_path.parent()?.to_path_buf();
            Some((
                skill.name.clone(),
                SkillProvenance {
                    declared_path,
                    canonical_path,
                    resource_directory,
                },
            ))
        })
        .collect();
    let mut watch_directories = payload.watch_directories.clone();
    watch_directories.extend(targets.into_iter().map(|target| target.root));
    watch_directories.sort();
    watch_directories.dedup();
    let mut digest = Sha256::new();
    digest.update(serde_json::to_vec(&payload.skills).expect("skill metadata is serializable"));
    for fingerprint in &payload.fingerprints {
        digest.update(format!("{fingerprint:?}").as_bytes());
    }
    ProjectSkillSnapshot {
        skills: payload.skills.clone(),
        provenance,
        watch_directories,
        diagnostics: payload.diagnostics.clone(),
        fingerprint: digest
            .finalize()
            .iter()
            .fold(String::with_capacity(64), |mut text, byte| {
                write!(text, "{byte:02x}").expect("writing to a String is infallible");
                text
            }),
        payload: Some(payload),
        generation,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn skill(root: &Path, relative: &str, name: &str, description: &str) {
        let path = root.join(relative).join("SKILL.md");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(
            path,
            format!("---\nname: {name}\ndescription: {description}\n---\nBody"),
        )
        .unwrap();
    }

    #[tokio::test]
    async fn project_scope_is_separate_from_home_and_legacy_cache() {
        let root = crate::test_support::test_tempdir("scoped-skill-project-");
        let home = crate::test_support::test_tempdir("scoped-skill-home-");
        skill(home.path(), ".agents/skills/home", "home", "Ambient");
        for (index, path) in ROOTS.iter().enumerate() {
            skill(
                root.path(),
                &format!("{path}/skill-{index}"),
                &format!("skill-{index}"),
                "Project",
            );
        }
        let legacy = super::super::load_skills_payload_with_home(
            root.path().to_str().unwrap(),
            Some(home.path().to_path_buf()),
        )
        .await;
        assert!(legacy.skills.iter().any(|skill| skill.name == "home"));
        let scoped = discover_project_skills(&[root.path().to_path_buf()]);
        assert_eq!(scoped.skills.len(), ROOTS.len());
        assert!(scoped
            .skills
            .iter()
            .all(|skill| skill.scope == "project" && skill.name != "home"));
        assert!(discover_project_skills(&[]).skills.is_empty());
        let legacy = super::super::load_skills_payload_with_home(
            root.path().to_str().unwrap(),
            Some(home.path().to_path_buf()),
        )
        .await;
        assert!(legacy.skills.iter().any(|skill| skill.name == "home"));
    }

    #[test]
    fn priority_and_cwd_are_part_of_cache_identity() {
        let root = crate::test_support::test_tempdir("scoped-skill-priority-");
        skill(root.path(), ".agents/skills/tool", "tool", "Root agents");
        skill(root.path(), ".intent/skills/tool", "tool", "Root intent");
        skill(root.path(), "nested/.pi/skills/tool", "tool", "Nested pi");
        let roots = vec![root.path().to_path_buf()];
        assert_eq!(
            discover_project_skills(&roots).skills[0].description,
            "Root intent"
        );
        let nested = vec![root.path().to_path_buf(), root.path().join("nested")];
        assert_eq!(
            discover_project_skills(&nested).skills[0].description,
            "Nested pi"
        );
        assert_eq!(
            discover_project_skills(&roots).skills[0].description,
            "Root intent"
        );
    }

    #[cfg(unix)]
    #[test]
    fn explicit_skill_links_keep_provenance_watches_and_invalidation() {
        use std::os::unix::fs::symlink;
        let root = crate::test_support::test_tempdir("scoped-skill-links-");
        let home = crate::test_support::test_tempdir("scoped-skill-target-");
        skill(home.path(), "declared/tool", "tool", "Original");
        skill(
            home.path(),
            ".agents/skills/ambient",
            "ambient",
            "Must not scan",
        );
        std::fs::create_dir_all(root.path().join(".agents/skills")).unwrap();
        symlink(
            home.path().join("declared/tool"),
            root.path().join(".agents/skills/tool"),
        )
        .unwrap();
        symlink(
            root.path().join(".agents"),
            root.path().join(".agents/skills/cycle"),
        )
        .unwrap();
        let roots = vec![root.path().to_path_buf()];
        let before = discover_project_skills(&roots);
        assert_eq!(before.skills.len(), 1);
        assert_eq!(
            before.provenance["tool"].canonical_path,
            home.path().join("declared/tool/SKILL.md")
        );
        assert_eq!(
            before.provenance["tool"].declared_path,
            root.path().join(".agents/skills/tool/SKILL.md")
        );
        assert_eq!(
            before.provenance["tool"].resource_directory,
            home.path().join("declared/tool")
        );
        assert!(before
            .watch_directories
            .contains(&home.path().join("declared/tool")));
        skill(home.path(), "declared/tool", "tool", "Updated description");
        assert!(!before.is_current());
        let after = discover_project_skills(&roots);
        assert_ne!(before.fingerprint, after.fingerprint);
        assert_eq!(after.skills[0].description, "Updated description");
        super::super::invalidate_skills_cache(root.path());
        assert!(!after.is_current());
        assert_eq!(
            discover_project_skills(&roots).skills[0].description,
            "Updated description"
        );
    }

    #[test]
    fn malformed_skills_report_diagnostics_and_equal_rank_is_deterministic() {
        let root = crate::test_support::test_tempdir("scoped-skill-invalid-");
        skill(root.path(), ".agents/skills/z", "duplicate", "Last");
        skill(root.path(), ".agents/skills/a", "duplicate", "First");
        let invalid = root.path().join(".agents/skills/invalid");
        std::fs::create_dir_all(&invalid).unwrap();
        std::fs::write(invalid.join("SKILL.md"), "not frontmatter").unwrap();
        let snapshot = discover_project_skills(&[root.path().to_path_buf()]);
        assert_eq!(snapshot.skills.len(), 1);
        assert_eq!(snapshot.skills[0].description, "First");
        assert_eq!(snapshot.diagnostics.len(), 2);
    }
}
