//! Sitter channel config (`<data_dir>/sitter/config.toml`) and channel
//! resolution.
//!
//! The config file durably pins the update channel for launches that pass
//! no `--sitter-channel` flag and no `INTENTD_CHANNEL` env — service
//! definitions (brew `service do`, systemd units) pass neither. It is
//! user-editable and, unlike `state.json`, never rewritten by the updater.
//! A missing, unreadable, or invalid file is treated as "no pin" —
//! [`load_channel`] warns and never panics.
//!
//! Effective-channel precedence ([`resolve_channel`]):
//! `--sitter-channel` flag > `INTENTD_CHANNEL` env > `config.toml` > stable.

use std::fmt;
use std::fs;
use std::io;
use std::path::Path;

use serde::Deserialize;

use crate::cli::Channel;

/// Where the effective channel came from, in precedence order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelOrigin {
    /// The `--sitter-channel` flag.
    Flag,
    /// The `INTENTD_CHANNEL` environment variable.
    Env,
    /// The `<data_dir>/sitter/config.toml` pin.
    Config,
    /// The built-in stable default.
    Default,
}

impl fmt::Display for ChannelOrigin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ChannelOrigin::Flag => f.write_str("flag"),
            ChannelOrigin::Env => f.write_str("env"),
            ChannelOrigin::Config => f.write_str("config"),
            ChannelOrigin::Default => f.write_str("default"),
        }
    }
}

/// The effective channel plus where it came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedChannel {
    /// The effective release channel.
    pub channel: Channel,
    /// Which precedence level selected it.
    pub origin: ChannelOrigin,
}

/// `config.toml` schema: a single optional `channel` key. Unknown keys are
/// ignored on load; [`save_channel`] rewrites the whole file.
#[derive(Debug, Default, Deserialize)]
struct ConfigFile {
    channel: Option<Channel>,
}

/// Load the pinned channel from `path`. Missing, unreadable, or invalid
/// (unparsable TOML, unknown channel value) files all yield `None` — no
/// pin — with a warning for everything except a missing file.
#[must_use]
pub fn load_channel(path: &Path) -> Option<Channel> {
    let contents = match fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(e) => {
            if e.kind() != io::ErrorKind::NotFound {
                eprintln!(
                    "intentd-sitter: failed to read {}: {e}; ignoring channel pin",
                    path.display()
                );
            }
            return None;
        }
    };
    match toml::from_str::<ConfigFile>(&contents) {
        Ok(config) => config.channel,
        Err(e) => {
            eprintln!(
                "intentd-sitter: invalid {}: {e}; ignoring channel pin",
                path.display()
            );
            None
        }
    }
}

/// Persist `channel` as the config pin (creating parent directories),
/// rewriting the whole file via a temp file + rename so a crash never
/// leaves a truncated `config.toml`.
///
/// # Errors
///
/// Returns the underlying I/O error if creating parent directories, writing the temp file, or the rename fails.
pub fn save_channel(path: &Path, channel: Channel) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("toml.tmp");
    fs::write(&tmp, format!("channel = \"{channel}\"\n"))?;
    fs::rename(&tmp, path)?;
    Ok(())
}

/// Resolve the effective channel: flag > env > config > stable default.
///
/// `explicit` is the flag/env selection from CLI parsing
/// ([`crate::cli::SitterArgs::channel`]); `config` is the `config.toml` pin
/// from [`load_channel`].
#[must_use]
pub fn resolve_channel(
    explicit: Option<ResolvedChannel>,
    config: Option<Channel>,
) -> ResolvedChannel {
    if let Some(explicit) = explicit {
        return explicit;
    }
    match config {
        Some(channel) => ResolvedChannel {
            channel,
            origin: ChannelOrigin::Config,
        },
        None => ResolvedChannel {
            channel: Channel::default(),
            origin: ChannelOrigin::Default,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn save_load_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sitter").join("config.toml");
        for channel in [Channel::Beta, Channel::Alpha, Channel::Stable] {
            save_channel(&path, channel).unwrap();
            assert_eq!(load_channel(&path), Some(channel));
        }
    }

    #[test]
    fn missing_file_is_no_pin() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(load_channel(&dir.path().join("config.toml")), None);
    }

    #[test]
    fn invalid_file_is_no_pin() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        for invalid in ["not toml [", "channel = 5", "channel = \"nightly\""] {
            fs::write(&path, invalid).unwrap();
            assert_eq!(load_channel(&path), None);
        }
    }

    #[test]
    fn missing_channel_key_and_unknown_keys_tolerated() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        fs::write(&path, "# no channel pinned\n").unwrap();
        assert_eq!(load_channel(&path), None);
        fs::write(&path, "future_key = true\nchannel = \"beta\"\n").unwrap();
        assert_eq!(load_channel(&path), Some(Channel::Beta));
    }

    #[test]
    fn precedence_flag_env_config_default() {
        let flag = ResolvedChannel {
            channel: Channel::Stable,
            origin: ChannelOrigin::Flag,
        };
        let env = ResolvedChannel {
            channel: Channel::Beta,
            origin: ChannelOrigin::Env,
        };
        assert_eq!(resolve_channel(Some(flag), Some(Channel::Beta)), flag);
        assert_eq!(resolve_channel(Some(env), Some(Channel::Stable)), env);
        assert_eq!(
            resolve_channel(None, Some(Channel::Beta)),
            ResolvedChannel {
                channel: Channel::Beta,
                origin: ChannelOrigin::Config,
            }
        );
        assert_eq!(
            resolve_channel(None, Some(Channel::Alpha)),
            ResolvedChannel {
                channel: Channel::Alpha,
                origin: ChannelOrigin::Config,
            }
        );
        assert_eq!(
            resolve_channel(None, None),
            ResolvedChannel {
                channel: Channel::Stable,
                origin: ChannelOrigin::Default,
            }
        );
    }

    #[test]
    fn origin_display_is_lowercase() {
        assert_eq!(ChannelOrigin::Flag.to_string(), "flag");
        assert_eq!(ChannelOrigin::Env.to_string(), "env");
        assert_eq!(ChannelOrigin::Config.to_string(), "config");
        assert_eq!(ChannelOrigin::Default.to_string(), "default");
    }
}
