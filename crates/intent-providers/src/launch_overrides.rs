//! Final environment operations for an Intent-owned provider profile.
//!
//! Apply after captured/login/ambient environment assembly. These operations
//! deliberately include removals; a string overlay alone cannot exclude loaders.
use std::collections::{BTreeMap, BTreeSet};

/// Contains credentials. Intentionally has no `Debug` or serialization support.
#[derive(Clone, Default)]
pub struct EnvironmentOverrides {
    set: BTreeMap<String, String>,
    remove: BTreeSet<String>,
}

impl EnvironmentOverrides {
    pub fn set(&mut self, name: impl Into<String>, value: impl Into<String>) {
        let name = name.into();
        self.remove.remove(&name);
        self.set.insert(name, value.into());
    }

    pub fn remove(&mut self, name: impl Into<String>) {
        let name = name.into();
        self.set.remove(&name);
        self.remove.insert(name);
    }

    /// Apply to already-merged environment; callers must not merge again after this.
    pub fn apply_to_map(&self, env: &mut BTreeMap<String, String>) {
        env.retain(|key, _| !self.owns(key));
        env.extend(self.set.clone());
    }

    /// Final step before spawning. Removes inherited entries as well as explicit
    /// overrides; applying only `apply_to_map` does not remove process inheritance.
    pub fn apply_to_command(&self, command: &mut std::process::Command) {
        let inherited: Vec<_> = std::env::vars_os()
            .map(|(key, _)| key)
            .chain(command.get_envs().map(|(key, _)| key.to_owned()))
            .filter(|key| key.to_str().is_some_and(|key| self.owns(key)))
            .collect();
        for key in inherited {
            command.env_remove(key);
        }
        for key in &self.remove {
            command.env_remove(key);
        }
        command.envs(&self.set);
    }

    fn owns(&self, key: &str) -> bool {
        self.remove.iter().chain(self.set.keys()).any(|owned| {
            if cfg!(windows) {
                key.eq_ignore_ascii_case(owned)
            } else {
                key == owned
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authoritative_sets_and_removals_win_over_merged_inputs() {
        let mut env = BTreeMap::from([
            ("CONFIG".into(), "ambient".into()),
            ("HOOK".into(), "ambient".into()),
            ("AUTH".into(), "fixture".into()),
        ]);
        let mut overrides = EnvironmentOverrides::default();
        overrides.set("CONFIG", "owned");
        overrides.remove("HOOK");
        overrides.apply_to_map(&mut env);
        assert_eq!(
            env,
            BTreeMap::from([
                ("CONFIG".into(), "owned".into()),
                ("AUTH".into(), "fixture".into())
            ])
        );
    }

    #[test]
    #[cfg(unix)]
    fn subprocess_cannot_restore_explicit_loader_environment() {
        let mut command = std::process::Command::new("/bin/sh");
        command
            .env_clear()
            .env("PROFILE_LOADER", "ambient")
            .env("PROFILE_AUTH", "fixture");
        command.args(["-c", "test -z \"${PROFILE_LOADER+x}\" && test \"$PROFILE_AUTH\" = fixture && test \"$PROFILE_CONFIG\" = owned"]);
        let mut overrides = EnvironmentOverrides::default();
        overrides.remove("PROFILE_LOADER");
        overrides.set("PROFILE_CONFIG", "owned");
        overrides.apply_to_command(&mut command);
        assert!(command.status().unwrap().success());
    }
}
