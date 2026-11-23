//! The backend's GitHub token on its machines, domain side: the daemon's own
//! GitHub credential behind its gate, and the settings whose change calls
//! for a fresh resolution. The tokens themselves are the link's
//! ([`crate::link::forge_token`]).

use std::sync::Arc;
use std::time::Duration;

use intent_services::feature_api::Services;
use zeroize::Zeroizing;

use crate::link::forge_token::ForgeTokens;

/// The daemon's own GitHub credential, behind its gate.
pub(crate) fn for_services(svc: &Services) -> ForgeTokens {
    let registry = svc.settings_registry();
    let secrets = svc.secret_source();
    ForgeTokens::new(Arc::new(move || {
        let registry = registry.clone();
        let secrets = secrets.clone();
        Box::pin(async move {
            intent_services::github_git_credential(registry.as_deref(), &secrets)
                .await
                .map(|(_, token)| Zeroizing::new(token))
        })
    }))
}

/// Start `tokens`' rotation watcher once: while any machine is linked, it
/// resolves again every `every`, and at once (within `check`) after a
/// GitHub connect, revoke or token edit, or a change of the gate.
pub(crate) fn watch(tokens: &Arc<ForgeTokens>, svc: &Services, every: Duration, check: Duration) {
    let svc = svc.clone();
    tokens.watch_with(
        move || {
            let gate = intent_services::feature_api::execution_call::expose_git_credential(
                svc.settings_registry().as_deref(),
            );
            (intent_services::github_credential_generation(), gate)
        },
        every,
        check,
    );
}
