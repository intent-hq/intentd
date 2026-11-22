//! Where head keeps the git refs of its remote agents and homes: the
//! `refs/intent/ws/...` refs in each workspace's own repository on this
//! host ([`LocalGitStorage`]). An object-store backend can implement the
//! same traits later; every ref read and write head makes itself goes
//! through them, writes through [`RefRepository::update_ref`], a
//! compare-and-swap, so both backends share its semantics.
//!
//! Not yet behind these traits: refs a node pushes over its git session
//! (`git receive-pack` with head's update hook, [`crate::node_git`]) and the
//! hub repositories ([`crate::hub`]). An object-store backend must cover the
//! receive-pack path too; it writes most of these refs.
//!
//! Errors are [`git2::Error`]s so callers keep their own error mapping; a
//! backend that is not git2 builds them with [`git2::Error::new`].

use std::path::Path;

use git2::{ErrorClass, ErrorCode, Oid};

/// What a ref must hold for [`RefRepository::update_ref`] to apply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefExpect {
    /// Whatever it holds, or nothing.
    Any,
    /// Nothing: the update creates the ref.
    Absent,
    /// Exactly this object.
    Is(Oid),
}

/// Head's git storage for remote agents' and homes' refs.
pub trait GitStorage: Send + Sync {
    /// The repository at `repository`.
    ///
    /// # Errors
    ///
    /// When it cannot be opened.
    fn open(&self, repository: &Path) -> Result<Box<dyn RefRepository>, git2::Error>;
}

/// One repository's refs.
pub trait RefRepository {
    /// The object ref `name` points at directly; `None` for a symbolic ref.
    ///
    /// # Errors
    ///
    /// [`ErrorCode::NotFound`] when there is no such ref.
    fn find_ref(&self, name: &str) -> Result<Option<Oid>, git2::Error>;

    /// The commit ref `name` peels to; `None` when there is no such ref.
    ///
    /// # Errors
    ///
    /// When the ref cannot be read or does not name a commit.
    fn find_commit_ref(&self, name: &str) -> Result<Option<Oid>, git2::Error>;

    /// Check that commit `oid` exists.
    ///
    /// # Errors
    ///
    /// When it does not.
    fn find_commit(&self, oid: Oid) -> Result<(), git2::Error>;

    /// Set ref `name` to `new` (`None` deletes it) if it holds `expect`.
    ///
    /// # Errors
    ///
    /// [`ErrorCode::Exists`] when `expect` is [`RefExpect::Absent`] and the
    /// ref exists; [`ErrorCode::Modified`] when it is [`RefExpect::Is`] and
    /// the ref holds something else; [`ErrorCode::NotFound`] when it is
    /// [`RefExpect::Is`] and there is no ref, or when a delete finds no ref;
    /// any write failure. A refused update changes nothing.
    fn update_ref(
        &self,
        name: &str,
        expect: RefExpect,
        new: Option<Oid>,
        reason: &str,
    ) -> Result<(), git2::Error>;

    /// Whether `commit` descends from `ancestor`.
    ///
    /// # Errors
    ///
    /// When either commit cannot be read.
    fn is_descendant_of(&self, commit: Oid, ancestor: Oid) -> Result<bool, git2::Error>;
}

/// The refs live in each workspace's repository on this host.
#[derive(Debug, Clone, Copy, Default)]
pub struct LocalGitStorage;

impl GitStorage for LocalGitStorage {
    fn open(&self, repository: &Path) -> Result<Box<dyn RefRepository>, git2::Error> {
        Ok(Box::new(LocalRefs(git2::Repository::open(repository)?)))
    }
}

struct LocalRefs(git2::Repository);

fn modified(name: &str) -> git2::Error {
    git2::Error::new(
        ErrorCode::Modified,
        ErrorClass::Reference,
        format!("ref {name} does not hold the expected object"),
    )
}

impl RefRepository for LocalRefs {
    fn find_ref(&self, name: &str) -> Result<Option<Oid>, git2::Error> {
        Ok(self.0.find_reference(name)?.target())
    }

    fn find_commit_ref(&self, name: &str) -> Result<Option<Oid>, git2::Error> {
        let reference = match self.0.find_reference(name) {
            Ok(reference) => reference,
            Err(error) if error.code() == ErrorCode::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        Ok(Some(reference.peel_to_commit()?.id()))
    }

    fn find_commit(&self, oid: Oid) -> Result<(), git2::Error> {
        self.0.find_commit(oid).map(drop)
    }

    fn update_ref(
        &self,
        name: &str,
        expect: RefExpect,
        new: Option<Oid>,
        reason: &str,
    ) -> Result<(), git2::Error> {
        match (expect, new) {
            (RefExpect::Any, Some(new)) => self.0.reference(name, new, true, reason).map(drop),
            (RefExpect::Absent, Some(new)) => self.0.reference(name, new, false, reason).map(drop),
            (RefExpect::Is(old), Some(new)) => self
                .0
                .reference_matching(name, new, true, old, reason)
                .map(drop),
            (RefExpect::Any, None) => self.0.find_reference(name)?.delete(),
            (RefExpect::Is(old), None) => {
                let mut reference = self.0.find_reference(name)?;
                if reference.target() != Some(old) {
                    return Err(modified(name));
                }
                reference.delete()
            }
            (RefExpect::Absent, None) => match self.0.find_reference(name) {
                Ok(_) => Err(git2::Error::new(
                    ErrorCode::Exists,
                    ErrorClass::Reference,
                    format!("ref {name} exists"),
                )),
                Err(error) if error.code() == ErrorCode::NotFound => Ok(()),
                Err(error) => Err(error),
            },
        }
    }

    fn is_descendant_of(&self, commit: Oid, ancestor: Oid) -> Result<bool, git2::Error> {
        self.0.graph_descendant_of(commit, ancestor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const REF: &str = "refs/intent/ws/ws-1/agents/a-1/head";

    /// A repository with two commits, the second on the first.
    fn repo() -> (tempfile::TempDir, Oid, Oid) {
        let dir = tempfile::tempdir().unwrap();
        let repo = git2::Repository::init(dir.path()).unwrap();
        let sig = git2::Signature::now("t", "t@example.com").unwrap();
        let tree = repo
            .find_tree(repo.index().unwrap().write_tree().unwrap())
            .unwrap();
        let first = repo.commit(None, &sig, &sig, "one", &tree, &[]).unwrap();
        let parent = repo.find_commit(first).unwrap();
        let second = repo
            .commit(None, &sig, &sig, "two", &tree, &[&parent])
            .unwrap();
        (dir, first, second)
    }

    fn refs(dir: &tempfile::TempDir) -> Box<dyn RefRepository> {
        LocalGitStorage.open(dir.path()).unwrap()
    }

    #[test]
    fn an_absent_ref_is_not_found() {
        let (dir, _, _) = repo();
        let refs = refs(&dir);
        assert_eq!(refs.find_ref(REF).unwrap_err().code(), ErrorCode::NotFound);
        assert_eq!(refs.find_commit_ref(REF).unwrap(), None);
    }

    #[test]
    fn a_forced_update_creates_an_absent_ref() {
        let (dir, first, _) = repo();
        let refs = refs(&dir);
        refs.update_ref(REF, RefExpect::Any, Some(first), "t")
            .unwrap();
        assert_eq!(refs.find_ref(REF).unwrap(), Some(first));
        assert_eq!(refs.find_commit_ref(REF).unwrap(), Some(first));
    }

    #[test]
    fn an_expected_object_on_an_absent_ref_is_not_found() {
        let (dir, first, second) = repo();
        let refs = refs(&dir);
        assert_eq!(
            refs.update_ref(REF, RefExpect::Is(first), Some(second), "t")
                .unwrap_err()
                .code(),
            ErrorCode::NotFound
        );
        assert_eq!(
            refs.update_ref(REF, RefExpect::Is(first), None, "t")
                .unwrap_err()
                .code(),
            ErrorCode::NotFound
        );
        assert_eq!(refs.find_ref(REF).unwrap_err().code(), ErrorCode::NotFound);
    }

    #[test]
    fn deleting_an_expected_absent_ref_checks_it_is_absent() {
        let (dir, first, _) = repo();
        let refs = refs(&dir);
        refs.update_ref(REF, RefExpect::Absent, None, "t").unwrap();
        refs.update_ref(REF, RefExpect::Any, Some(first), "t")
            .unwrap();
        assert_eq!(
            refs.update_ref(REF, RefExpect::Absent, None, "t")
                .unwrap_err()
                .code(),
            ErrorCode::Exists
        );
        assert_eq!(refs.find_ref(REF).unwrap(), Some(first));
    }

    #[test]
    fn a_symbolic_ref_has_no_direct_target() {
        let (dir, first, _) = repo();
        let refs = refs(&dir);
        refs.update_ref(REF, RefExpect::Any, Some(first), "t")
            .unwrap();
        git2::Repository::open(dir.path())
            .unwrap()
            .reference_symbolic("refs/intent/link", REF, true, "t")
            .unwrap();
        assert_eq!(refs.find_ref("refs/intent/link").unwrap(), None);
        assert_eq!(
            refs.find_commit_ref("refs/intent/link").unwrap(),
            Some(first)
        );
    }

    #[test]
    fn a_create_only_update_refuses_an_existing_ref() {
        let (dir, first, second) = repo();
        let refs = refs(&dir);
        refs.update_ref(REF, RefExpect::Absent, Some(first), "t")
            .unwrap();
        assert_eq!(refs.find_ref(REF).unwrap(), Some(first));
        assert_eq!(
            refs.update_ref(REF, RefExpect::Absent, Some(second), "t")
                .unwrap_err()
                .code(),
            ErrorCode::Exists
        );
        assert_eq!(refs.find_commit_ref(REF).unwrap(), Some(first));
    }

    #[test]
    fn a_compare_and_swap_applies_only_over_the_expected_object() {
        let (dir, first, second) = repo();
        let refs = refs(&dir);
        refs.update_ref(REF, RefExpect::Any, Some(first), "t")
            .unwrap();
        assert_eq!(
            refs.update_ref(REF, RefExpect::Is(second), Some(second), "t")
                .unwrap_err()
                .code(),
            ErrorCode::Modified
        );
        assert_eq!(refs.find_ref(REF).unwrap(), Some(first));
        refs.update_ref(REF, RefExpect::Is(first), Some(second), "t")
            .unwrap();
        assert_eq!(refs.find_ref(REF).unwrap(), Some(second));
    }

    #[test]
    fn a_forced_update_overwrites_whatever_the_ref_holds() {
        let (dir, first, second) = repo();
        let refs = refs(&dir);
        refs.update_ref(REF, RefExpect::Any, Some(second), "t")
            .unwrap();
        refs.update_ref(REF, RefExpect::Any, Some(first), "t")
            .unwrap();
        assert_eq!(refs.find_ref(REF).unwrap(), Some(first));
    }

    #[test]
    fn a_compare_and_swap_delete_keeps_a_moved_ref() {
        let (dir, first, second) = repo();
        let refs = refs(&dir);
        refs.update_ref(REF, RefExpect::Any, Some(second), "t")
            .unwrap();
        assert_eq!(
            refs.update_ref(REF, RefExpect::Is(first), None, "t")
                .unwrap_err()
                .code(),
            ErrorCode::Modified
        );
        assert_eq!(refs.find_ref(REF).unwrap(), Some(second));
        refs.update_ref(REF, RefExpect::Is(second), None, "t")
            .unwrap();
        assert_eq!(refs.find_ref(REF).unwrap_err().code(), ErrorCode::NotFound);
        assert_eq!(
            refs.update_ref(REF, RefExpect::Any, None, "t")
                .unwrap_err()
                .code(),
            ErrorCode::NotFound
        );
    }

    #[test]
    fn descent_follows_history() {
        let (dir, first, second) = repo();
        let refs = refs(&dir);
        assert!(refs.is_descendant_of(second, first).unwrap());
        assert!(!refs.is_descendant_of(first, second).unwrap());
        refs.find_commit(first).unwrap();
        assert!(refs.find_commit(Oid::ZERO_SHA1).is_err());
    }
}
