//! Sitter CLI parsing.
//!
//! The sitter forwards ALL args verbatim to the daemon, so it owns only the
//! `--sitter-*` flag namespace, stripped before forwarding. Everything else —
//! including `--version`, `serve`, `--resume-all` — is collected in order as
//! passthrough args. A manual scan is used instead of clap so unknown daemon
//! flags are never rejected here.

use std::ffi::OsString;
use std::fmt;

use serde::{Deserialize, Serialize};

/// Environment variable selecting the release channel (`stable` | `beta`).
pub const CHANNEL_ENV: &str = "INTENTD_CHANNEL";

/// Release channel the sitter tracks. Serialized lowercase to match the
/// `stable.json` / `beta.json` channel-manifest naming.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Channel {
    #[default]
    Stable,
    Beta,
}

impl Channel {
    fn parse(s: &str) -> Result<Self, CliError> {
        match s {
            "stable" => Ok(Channel::Stable),
            "beta" => Ok(Channel::Beta),
            other => Err(CliError::InvalidChannel(other.to_string())),
        }
    }
}

impl fmt::Display for Channel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Channel::Stable => f.write_str("stable"),
            Channel::Beta => f.write_str("beta"),
        }
    }
}

/// Errors from parsing the sitter-owned flag namespace.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CliError {
    #[error("invalid channel {0:?}: expected \"stable\" or \"beta\"")]
    InvalidChannel(String),
    #[error("--sitter-channel requires a value (stable|beta)")]
    MissingChannelValue,
    #[error(
        "unknown sitter flag {0:?} (the sitter owns only --sitter-channel and --sitter-version)"
    )]
    UnknownSitterFlag(String),
}

/// Parsed sitter invocation: the stripped `--sitter-*` options plus the
/// remaining args preserved verbatim (and in order) for the daemon.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SitterArgs {
    /// Release channel: `--sitter-channel` > `INTENTD_CHANNEL` > stable.
    pub channel: Channel,
    /// `--sitter-version` was given: print the sitter's own version and exit.
    pub print_version: bool,
    /// All non-`--sitter-*` args, verbatim, for the daemon.
    pub passthrough: Vec<OsString>,
}

impl SitterArgs {
    /// Parse from process args (without argv[0]) and the raw `INTENTD_CHANNEL`
    /// env value. Both are parameters so tests never mutate process state.
    ///
    /// A bare `--` ends sitter-flag scanning: it and everything after it are
    /// forwarded verbatim, even args that look like `--sitter-*`.
    pub fn parse_from<I, T>(args: I, env_channel: Option<OsString>) -> Result<Self, CliError>
    where
        I: IntoIterator<Item = T>,
        T: Into<OsString>,
    {
        let mut channel_flag: Option<Channel> = None;
        let mut print_version = false;
        let mut passthrough = Vec::new();
        let mut forward_rest = false;

        let mut iter = args.into_iter().map(Into::into);
        while let Some(arg) = iter.next() {
            if forward_rest {
                passthrough.push(arg);
                continue;
            }
            if arg == "--" {
                forward_rest = true;
                passthrough.push(arg);
                continue;
            }
            // Sitter flags are plain ASCII; non-UTF-8 args belong to the daemon.
            match arg.to_str() {
                Some("--sitter-version") => print_version = true,
                Some("--sitter-channel") => {
                    let value = iter.next().ok_or(CliError::MissingChannelValue)?;
                    let value = value.to_str().ok_or_else(|| {
                        CliError::InvalidChannel(value.to_string_lossy().into_owned())
                    })?;
                    channel_flag = Some(Channel::parse(value)?);
                }
                Some(s) if s.starts_with("--sitter-channel=") => {
                    channel_flag = Some(Channel::parse(&s["--sitter-channel=".len()..])?);
                }
                Some(s) if s.starts_with("--sitter-") => {
                    return Err(CliError::UnknownSitterFlag(s.to_string()));
                }
                _ => passthrough.push(arg),
            }
        }

        let channel = match channel_flag {
            Some(c) => c,
            None => match env_channel.filter(|v| !v.is_empty()) {
                Some(v) => {
                    let v = v.to_str().ok_or_else(|| {
                        CliError::InvalidChannel(v.to_string_lossy().into_owned())
                    })?;
                    Channel::parse(v)?
                }
                None => Channel::default(),
            },
        };

        Ok(Self {
            channel,
            print_version,
            passthrough,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str], env: Option<&str>) -> Result<SitterArgs, CliError> {
        SitterArgs::parse_from(args.iter().map(OsString::from), env.map(OsString::from))
    }

    #[test]
    fn channel_defaults_to_stable() {
        assert_eq!(parse(&[], None).unwrap().channel, Channel::Stable);
    }

    #[test]
    fn channel_from_env() {
        assert_eq!(parse(&[], Some("beta")).unwrap().channel, Channel::Beta);
    }

    #[test]
    fn channel_flag_overrides_env() {
        let args = parse(&["--sitter-channel", "stable"], Some("beta")).unwrap();
        assert_eq!(args.channel, Channel::Stable);
    }

    #[test]
    fn channel_equals_form() {
        let args = parse(&["--sitter-channel=beta"], None).unwrap();
        assert_eq!(args.channel, Channel::Beta);
        assert!(args.passthrough.is_empty());
    }

    #[test]
    fn empty_env_is_unset() {
        assert_eq!(parse(&[], Some("")).unwrap().channel, Channel::Stable);
    }

    #[test]
    fn invalid_channel_flag_is_error() {
        assert_eq!(
            parse(&["--sitter-channel", "nightly"], None),
            Err(CliError::InvalidChannel("nightly".to_string()))
        );
    }

    #[test]
    fn invalid_channel_env_is_error() {
        assert_eq!(
            parse(&[], Some("nightly")),
            Err(CliError::InvalidChannel("nightly".to_string()))
        );
    }

    #[test]
    fn missing_channel_value_is_error() {
        assert_eq!(
            parse(&["--sitter-channel"], None),
            Err(CliError::MissingChannelValue)
        );
    }

    #[test]
    fn unknown_sitter_flag_is_error() {
        assert_eq!(
            parse(&["--sitter-bogus"], None),
            Err(CliError::UnknownSitterFlag("--sitter-bogus".to_string()))
        );
    }

    #[test]
    fn sitter_version_flag() {
        let args = parse(&["--sitter-version"], None).unwrap();
        assert!(args.print_version);
        assert!(!parse(&[], None).unwrap().print_version);
    }

    #[test]
    fn sitter_flags_stripped_and_daemon_args_preserved_verbatim_in_order() {
        let args = parse(
            &[
                "serve",
                "--sitter-channel",
                "beta",
                "--resume-all",
                "--version",
                "--sitter-version",
                "-v",
                "positional",
            ],
            None,
        )
        .unwrap();
        assert_eq!(args.channel, Channel::Beta);
        assert!(args.print_version);
        assert_eq!(
            args.passthrough,
            vec![
                OsString::from("serve"),
                OsString::from("--resume-all"),
                OsString::from("--version"),
                OsString::from("-v"),
                OsString::from("positional"),
            ]
        );
    }

    #[test]
    fn double_dash_ends_sitter_flag_scanning() {
        let args = parse(
            &["--sitter-channel=beta", "--", "--sitter-channel=stable"],
            None,
        )
        .unwrap();
        assert_eq!(args.channel, Channel::Beta);
        assert_eq!(
            args.passthrough,
            vec![
                OsString::from("--"),
                OsString::from("--sitter-channel=stable"),
            ]
        );
    }

    #[test]
    fn channel_display_and_serde_are_lowercase() {
        assert_eq!(Channel::Stable.to_string(), "stable");
        assert_eq!(Channel::Beta.to_string(), "beta");
        assert_eq!(serde_json::to_string(&Channel::Beta).unwrap(), "\"beta\"");
        assert_eq!(
            serde_json::from_str::<Channel>("\"stable\"").unwrap(),
            Channel::Stable
        );
    }
}
