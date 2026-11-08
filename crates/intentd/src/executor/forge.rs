//! The backend's GitHub token on this machine (nodeProtocol 15, node-link.md
//! § Forge token on the machine): held in memory only, never written to
//! disk, and dropped on a new `headSession`, a revoked credential, a
//! machine restart or a `forgeToken` without a value.

use std::sync::Mutex;

use zeroize::Zeroizing;

/// See the module docs.
#[derive(Default)]
pub(super) struct ForgeToken(Mutex<Option<Zeroizing<String>>>);

impl ForgeToken {
    /// Hold `token`, or drop the held one with `None`.
    pub(super) fn set(&self, token: Option<Zeroizing<String>>) {
        *lock(&self.0) = token;
    }

    pub(super) fn clear(&self) {
        self.set(None);
    }

    /// A copy of the held token, wiped when dropped.
    pub(super) fn get(&self) -> Option<Zeroizing<String>> {
        lock(&self.0).clone()
    }

    /// Whether `token` may be held: not empty, and nothing that would break
    /// an environment variable or the git credential protocol.
    pub(super) fn usable(token: &str) -> bool {
        !token.is_empty() && !token.chars().any(char::is_control)
    }
}

impl std::fmt::Debug for ForgeToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ForgeToken")
            .field("held", &lock(&self.0).is_some())
            .finish()
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
