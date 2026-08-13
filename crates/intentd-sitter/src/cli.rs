//! Sitter CLI parsing.
//!
//! The sitter forwards ALL args verbatim to the daemon, so it owns only the
//! `--sitter-*` flag namespace, stripped before forwarding, plus the
//! intercepted `sitter` subcommand namespace ([`SitterCommand`]). Everything
//! else — including `--version`, `serve`, `--resume-all` — is collected in
//! order as passthrough args. A manual scan is used instead of clap so
//! unknown daemon flags are never rejected here.

use std::ffi::OsString;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::config::{ChannelOrigin, ResolvedChannel};

/// Environment variable selecting the release channel
/// (`stable` | `beta` | `alpha`).
pub const CHANNEL_ENV: &str = "INTENTD_CHANNEL";

/// Release channel the sitter tracks. Serialized lowercase to match the
/// `stable.json` / `beta.json` / `alpha.json` channel-manifest naming.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Channel {
    #[default]
    Stable,
    Beta,
    Alpha,
}

impl Channel {
    fn parse(s: &str) -> Result<Self, CliError> {
        match s {
            "stable" => Ok(Channel::Stable),
            "beta" => Ok(Channel::Beta),
            "alpha" => Ok(Channel::Alpha),
            other => Err(CliError::InvalidChannel(other.to_string())),
        }
    }
}

impl fmt::Display for Channel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Channel::Stable => f.write_str("stable"),
            Channel::Beta => f.write_str("beta"),
            Channel::Alpha => f.write_str("alpha"),
        }
    }
}

/// Errors from parsing the sitter-owned flag namespace.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CliError {
    #[error("invalid channel {0:?}: expected \"stable\", \"beta\", or \"alpha\"")]
    InvalidChannel(String),
    #[error("--sitter-channel requires a value (stable|beta|alpha)")]
    MissingChannelValue,
    #[error(
        "unknown sitter flag {0:?} (the sitter owns only --sitter-channel and --sitter-version)"
    )]
    UnknownSitterFlag(String),
    #[error(
        "missing sitter subcommand: usage: intentd sitter channel [stable|beta|alpha] [--redownload]"
    )]
    MissingSitterSubcommand,
    #[error("unknown sitter subcommand {0:?} (supported sitter subcommands: channel)")]
    UnknownSitterSubcommand(String),
    #[error(
        "--redownload requires a channel value: intentd sitter channel <stable|beta|alpha> --redownload"
    )]
    RedownloadWithoutChannel,
    #[error("unexpected argument {0:?} to `intentd sitter channel`")]
    UnexpectedChannelArg(String),
    #[error("unexpected argument {0:?} to `intentd restart` (it takes no arguments)")]
    UnexpectedRestartArg(String),
    #[error("unexpected argument {0:?} to `intentd update` (it takes only --check)")]
    UnexpectedUpdateArg(String),
}

/// Intercepted sitter-owned subcommand, recognized when `sitter` (or bare
/// `restart` / `update`) is the first passthrough token — like the
/// `--sitter-*` flag namespace it is never forwarded to the daemon. A bare
/// `--` before it still forwards everything verbatim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SitterCommand {
    /// `intentd sitter channel [stable|beta|alpha] [--redownload]` — get or
    /// set the persistent channel pin in `<data_dir>/sitter/config.toml`.
    Channel {
        /// Channel to pin; `None` is the get form (print the effective
        /// channel and its origin).
        set: Option<Channel>,
        /// Set form only: immediately fetch the channel manifest and
        /// force-install its version, bypassing the newer-only comparison.
        redownload: bool,
    },
    /// `intentd restart` — restart the supervised daemon in place by
    /// signaling the serve-mode sitter found via its pidfile (SIGHUP).
    Restart,
    /// `intentd update [--check]` — force an update check on the effective
    /// channel now, instead of waiting for the periodic serve-mode check.
    Update {
        /// Dry-run: report installed vs latest without installing anything.
        check: bool,
    },
}

impl SitterCommand {
    /// Parse the tokens after the leading `sitter`.
    fn parse(rest: &[OsString]) -> Result<Self, CliError> {
        let Some((sub, args)) = rest.split_first() else {
            return Err(CliError::MissingSitterSubcommand);
        };
        if sub.to_str() != Some("channel") {
            return Err(CliError::UnknownSitterSubcommand(
                sub.to_string_lossy().into_owned(),
            ));
        }
        let mut set = None;
        let mut redownload = false;
        for arg in args {
            match arg.to_str() {
                Some("--redownload") => redownload = true,
                Some(value) if set.is_none() && !value.starts_with('-') => {
                    set = Some(Channel::parse(value)?);
                }
                _ => {
                    return Err(CliError::UnexpectedChannelArg(
                        arg.to_string_lossy().into_owned(),
                    ));
                }
            }
        }
        if redownload && set.is_none() {
            return Err(CliError::RedownloadWithoutChannel);
        }
        Ok(SitterCommand::Channel { set, redownload })
    }
}

/// Parsed sitter invocation: the stripped `--sitter-*` options plus the
/// remaining args preserved verbatim (and in order) for the daemon.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SitterArgs {
    /// Channel explicitly selected this invocation (`--sitter-channel` >
    /// `INTENTD_CHANNEL`), if any. `None` falls back to the `config.toml`
    /// pin, then stable — resolve via [`crate::config::resolve_channel`].
    pub channel: Option<ResolvedChannel>,
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
            Some(channel) => Some(ResolvedChannel {
                channel,
                origin: ChannelOrigin::Flag,
            }),
            None => match env_channel.filter(|v| !v.is_empty()) {
                Some(v) => {
                    let v = v.to_str().ok_or_else(|| {
                        CliError::InvalidChannel(v.to_string_lossy().into_owned())
                    })?;
                    Some(ResolvedChannel {
                        channel: Channel::parse(v)?,
                        origin: ChannelOrigin::Env,
                    })
                }
                None => None,
            },
        };

        Ok(Self {
            channel,
            print_version,
            passthrough,
        })
    }

    /// The intercepted sitter-owned subcommand, when the first passthrough
    /// token is `sitter`, `restart`, or `update`. After a bare `--` the
    /// first passthrough token is the `--` itself, so `intentd -- sitter …`,
    /// `intentd -- restart`, and `intentd -- update` still forward verbatim.
    pub fn sitter_command(&self) -> Option<Result<SitterCommand, CliError>> {
        let first = self.passthrough.first()?;
        match first.to_str() {
            Some("sitter") => Some(SitterCommand::parse(&self.passthrough[1..])),
            Some("restart") => Some(match self.passthrough.get(1) {
                Some(arg) => Err(CliError::UnexpectedRestartArg(
                    arg.to_string_lossy().into_owned(),
                )),
                None => Ok(SitterCommand::Restart),
            }),
            Some("update") => {
                let mut check = false;
                for arg in &self.passthrough[1..] {
                    match arg.to_str() {
                        Some("--check") => check = true,
                        _ => {
                            return Some(Err(CliError::UnexpectedUpdateArg(
                                arg.to_string_lossy().into_owned(),
                            )));
                        }
                    }
                }
                Some(Ok(SitterCommand::Update { check }))
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str], env: Option<&str>) -> Result<SitterArgs, CliError> {
        SitterArgs::parse_from(args.iter().map(OsString::from), env.map(OsString::from))
    }

    fn resolved(channel: Channel, origin: ChannelOrigin) -> Option<ResolvedChannel> {
        Some(ResolvedChannel { channel, origin })
    }

    #[test]
    fn no_flag_or_env_selects_no_channel() {
        assert_eq!(parse(&[], None).unwrap().channel, None);
    }

    #[test]
    fn channel_from_env() {
        assert_eq!(
            parse(&[], Some("beta")).unwrap().channel,
            resolved(Channel::Beta, ChannelOrigin::Env)
        );
    }

    #[test]
    fn alpha_channel_from_env_and_flag() {
        assert_eq!(
            parse(&[], Some("alpha")).unwrap().channel,
            resolved(Channel::Alpha, ChannelOrigin::Env)
        );
        assert_eq!(
            parse(&["--sitter-channel", "alpha"], None).unwrap().channel,
            resolved(Channel::Alpha, ChannelOrigin::Flag)
        );
        assert_eq!(
            parse(&["--sitter-channel=alpha"], None).unwrap().channel,
            resolved(Channel::Alpha, ChannelOrigin::Flag)
        );
    }

    #[test]
    fn channel_flag_overrides_env() {
        let args = parse(&["--sitter-channel", "stable"], Some("beta")).unwrap();
        assert_eq!(args.channel, resolved(Channel::Stable, ChannelOrigin::Flag));
    }

    #[test]
    fn channel_equals_form() {
        let args = parse(&["--sitter-channel=beta"], None).unwrap();
        assert_eq!(args.channel, resolved(Channel::Beta, ChannelOrigin::Flag));
        assert!(args.passthrough.is_empty());
    }

    #[test]
    fn empty_env_is_unset() {
        assert_eq!(parse(&[], Some("")).unwrap().channel, None);
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
        assert_eq!(args.channel, resolved(Channel::Beta, ChannelOrigin::Flag));
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
        assert_eq!(args.channel, resolved(Channel::Beta, ChannelOrigin::Flag));
        assert_eq!(
            args.passthrough,
            vec![
                OsString::from("--"),
                OsString::from("--sitter-channel=stable"),
            ]
        );
    }

    fn sitter_cmd(args: &[&str]) -> Option<Result<SitterCommand, CliError>> {
        parse(args, None).unwrap().sitter_command()
    }

    #[test]
    fn non_sitter_invocations_have_no_sitter_command() {
        assert_eq!(sitter_cmd(&[]), None);
        assert_eq!(sitter_cmd(&["serve", "--resume-all"]), None);
        assert_eq!(sitter_cmd(&["doctor", "sitter"]), None);
    }

    #[test]
    fn sitter_channel_get_form() {
        assert_eq!(
            sitter_cmd(&["sitter", "channel"]),
            Some(Ok(SitterCommand::Channel {
                set: None,
                redownload: false,
            }))
        );
    }

    #[test]
    fn sitter_channel_set_form() {
        assert_eq!(
            sitter_cmd(&["sitter", "channel", "beta"]),
            Some(Ok(SitterCommand::Channel {
                set: Some(Channel::Beta),
                redownload: false,
            }))
        );
        assert_eq!(
            sitter_cmd(&["sitter", "channel", "alpha"]),
            Some(Ok(SitterCommand::Channel {
                set: Some(Channel::Alpha),
                redownload: false,
            }))
        );
    }

    #[test]
    fn sitter_channel_set_with_redownload_in_either_order() {
        let expected = Some(Ok(SitterCommand::Channel {
            set: Some(Channel::Stable),
            redownload: true,
        }));
        assert_eq!(
            sitter_cmd(&["sitter", "channel", "stable", "--redownload"]),
            expected
        );
        assert_eq!(
            sitter_cmd(&["sitter", "channel", "--redownload", "stable"]),
            expected
        );
    }

    #[test]
    fn sitter_channel_redownload_without_value_is_error() {
        assert_eq!(
            sitter_cmd(&["sitter", "channel", "--redownload"]),
            Some(Err(CliError::RedownloadWithoutChannel))
        );
    }

    #[test]
    fn sitter_channel_invalid_value_is_error() {
        assert_eq!(
            sitter_cmd(&["sitter", "channel", "nightly"]),
            Some(Err(CliError::InvalidChannel("nightly".to_string())))
        );
    }

    #[test]
    fn sitter_channel_extra_args_are_errors() {
        assert_eq!(
            sitter_cmd(&["sitter", "channel", "beta", "stable"]),
            Some(Err(CliError::UnexpectedChannelArg("stable".to_string())))
        );
        assert_eq!(
            sitter_cmd(&["sitter", "channel", "beta", "--force"]),
            Some(Err(CliError::UnexpectedChannelArg("--force".to_string())))
        );
    }

    #[test]
    fn bare_sitter_and_unknown_subcommand_are_errors() {
        assert_eq!(
            sitter_cmd(&["sitter"]),
            Some(Err(CliError::MissingSitterSubcommand))
        );
        assert_eq!(
            sitter_cmd(&["sitter", "restart"]),
            Some(Err(CliError::UnknownSitterSubcommand(
                "restart".to_string()
            )))
        );
    }

    #[test]
    fn bare_restart_is_intercepted() {
        assert_eq!(sitter_cmd(&["restart"]), Some(Ok(SitterCommand::Restart)));
    }

    #[test]
    fn restart_with_extra_args_is_error() {
        assert_eq!(
            sitter_cmd(&["restart", "now"]),
            Some(Err(CliError::UnexpectedRestartArg("now".to_string())))
        );
        assert_eq!(
            sitter_cmd(&["restart", "--force"]),
            Some(Err(CliError::UnexpectedRestartArg("--force".to_string())))
        );
    }

    #[test]
    fn restart_not_first_token_is_forwarded() {
        assert_eq!(sitter_cmd(&["serve", "restart"]), None);
    }

    #[test]
    fn bare_update_is_intercepted() {
        assert_eq!(
            sitter_cmd(&["update"]),
            Some(Ok(SitterCommand::Update { check: false }))
        );
    }

    #[test]
    fn update_check_flag_is_parsed() {
        assert_eq!(
            sitter_cmd(&["update", "--check"]),
            Some(Ok(SitterCommand::Update { check: true }))
        );
    }

    #[test]
    fn update_with_extra_args_is_error() {
        assert_eq!(
            sitter_cmd(&["update", "now"]),
            Some(Err(CliError::UnexpectedUpdateArg("now".to_string())))
        );
        assert_eq!(
            sitter_cmd(&["update", "--force"]),
            Some(Err(CliError::UnexpectedUpdateArg("--force".to_string())))
        );
        assert_eq!(
            sitter_cmd(&["update", "--check", "beta"]),
            Some(Err(CliError::UnexpectedUpdateArg("beta".to_string())))
        );
    }

    #[test]
    fn update_not_first_token_is_forwarded() {
        assert_eq!(sitter_cmd(&["serve", "update"]), None);
    }

    #[test]
    fn double_dash_forwards_update_verbatim() {
        let args = parse(&["--", "update"], None).unwrap();
        assert_eq!(args.sitter_command(), None);
        assert_eq!(
            args.passthrough,
            vec![OsString::from("--"), OsString::from("update")]
        );
    }

    #[test]
    fn double_dash_forwards_restart_verbatim() {
        let args = parse(&["--", "restart"], None).unwrap();
        assert_eq!(args.sitter_command(), None);
        assert_eq!(
            args.passthrough,
            vec![OsString::from("--"), OsString::from("restart")]
        );
    }

    #[test]
    fn double_dash_forwards_sitter_verbatim() {
        let args = parse(&["--", "sitter", "channel", "beta"], None).unwrap();
        assert_eq!(args.sitter_command(), None);
        assert_eq!(
            args.passthrough,
            vec![
                OsString::from("--"),
                OsString::from("sitter"),
                OsString::from("channel"),
                OsString::from("beta"),
            ]
        );
    }

    #[test]
    fn sitter_flags_still_stripped_around_sitter_command() {
        let args = parse(&["--sitter-channel=beta", "sitter", "channel"], None).unwrap();
        assert_eq!(args.channel, resolved(Channel::Beta, ChannelOrigin::Flag));
        assert_eq!(
            args.sitter_command(),
            Some(Ok(SitterCommand::Channel {
                set: None,
                redownload: false,
            }))
        );
    }

    #[test]
    fn channel_display_and_serde_are_lowercase() {
        assert_eq!(Channel::Stable.to_string(), "stable");
        assert_eq!(Channel::Beta.to_string(), "beta");
        assert_eq!(Channel::Alpha.to_string(), "alpha");
        assert_eq!(serde_json::to_string(&Channel::Beta).unwrap(), "\"beta\"");
        assert_eq!(serde_json::to_string(&Channel::Alpha).unwrap(), "\"alpha\"");
        assert_eq!(
            serde_json::from_str::<Channel>("\"stable\"").unwrap(),
            Channel::Stable
        );
        assert_eq!(
            serde_json::from_str::<Channel>("\"alpha\"").unwrap(),
            Channel::Alpha
        );
    }
}
