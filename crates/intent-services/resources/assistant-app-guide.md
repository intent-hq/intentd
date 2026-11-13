# Intent app guide for the Assistant

This bundled guide describes the app shipped with the daemon build supplying it. Use this copy instead of
earlier copies in the conversation for UI facts; preserve the user's customized
Assistant behavior. It describes supported paths, not the user's current settings,
connection, permissions, or installed frontend version. Read live tools when those
facts matter, and explain any mismatch with the user's actual UI.

## Features ready for users

Keep ordinary answers and recommendations focused on supported features ready for
users. Do not proactively mention or recommend unfinished, experimental, Labs-only,
internal, or unreleased features, or suggest enabling hidden flags to solve ordinary
tasks. Code, tool, route, or capability availability alone does not prove readiness.
If readiness is unclear, use a supported path and state what you cannot confirm.

When the user explicitly asks about an experiment, or is developing or testing it,
answer honestly, label its experimental status, and explain known limits. Do not deny
that it exists or present it as ready for general use. Ordinary setup or permission
requirements do not, by themselves, make a supported feature experimental.

Multiplayer is always on: there is no switch to enable or disable it, and opening an
invitation link joins directly without an enable step. Workspace sharing, guest roles,
discussions, the People panel, and **Settings → Collaboration** are ready for users.
Personal device pairing is still experimental; do not introduce it in routine answers
about settings, devices, or mobile pairing.
Sharing needs the host owner's GitHub or GitLab account connected in **Settings →
Connections**: without one, new invitation links cannot be created and unused links
cannot be redeemed until it is reconnected. People who already joined keep access.

GitLab is supported alongside GitHub; there is no Labs switch. Connect a GitLab.com or
self-managed GitLab account in **Settings → Connections** or during onboarding, by
device sign-in or a personal access token. With it connected, sharing can pin
invitations to GitLab accounts and a guest can join with a GitLab identity; on hosts
that support GitLab checkout, **Add codebase → Clone** also lists GitLab projects. An
older connected daemon shows an update note instead of the GitLab setup.

<!-- Sources: packages/intentd/crates/intent-services/src/{capability,invite_ops}.rs; packages/cloudlands-fe/src/lib/components/modals/ShareWorkspaceDialog.svelte; packages/cloudlands-fe/src/store/renderer/slices/user-preferences/user-preferences-slice.ts; packages/cloudlands-fe/src/features/invite-progress/invite-progress-service.ts; packages/cloudlands-fe/src/lib/components/settings/GitLabAuthConnection.svelte; packages/cloudlands-fe/src/features/onboarding/steps/OnboardingForgeStep.svelte; packages/cloudlands-fe/src/features/collaboration-auth/renderer/CollaborationSignInModal.svelte. -->

Workspace guest roles: the default collaborator role is Participant. The workspace owner
can change a direct guest between Participant and Manager in Share. Managers can
create agents, choose models/providers and reasoning effort, and run, start, stop,
restart or inspect existing
saved scripts. They cannot edit script commands, control raw terminals, run host
commands, change credentials, or manage sharing. Host membership retains its existing
authority. Role downgrades apply to subsequent operations on open connections.

A guest who joined a workspace by invite can leave it from the workspace sidebar's
**…** menu → **Leave…** (shown where owners see **Share…**), or from **Settings →
Collaboration** → **Leave** beside that workspace. Both ask for confirmation, remove
access to that one workspace, and keep other workspaces on the same host. Rejoining
needs a new invite. Access through host membership cannot be left per workspace; ask
the host owner to remove the membership.

<!-- Sources: packages/cloudlands-fe/src/lib/components/workspace/sidebar/WorkspaceProgressCard.svelte; packages/cloudlands-fe/src/lib/components/settings/GuestSessionsSettings.svelte; packages/cloudlands-fe/src/store/renderer/slices/guest-sessions/guest-sessions-selectors.ts (selectCanLeaveWorkspace); packages/intentd/crates/intent-services/src/invite_ops.rs (workspace_members_leave_op). -->

Workspaces support multiple workspace-wide discussions. Start a discussion by
sending its first message; everyone with workspace access can read and participate.
Workspace owners and managers can rename discussions; only workspace owners can
delete them, including the default discussion. Open the discussion, then use the
discussion panel's **…** menu for **Rename discussion** or **Delete discussion**.
These controls require discussion-management support from the connected daemon.
Archived workspaces whose folder was purged keep discussions read-only; unarchive the
workspace before renaming or deleting a discussion.
Existing private history remains
visible only to its original participants and is read-only; new private discussions
are unavailable. Discussions support reactions 👍 👎 👀 ✅ 🙈. The emoji picker has no tooltips;
hover or focus an existing reaction to see who reacted, and click to toggle yours.
Legacy 🙂 can be removed. Agents can react only
after explicit addressing in the workspace-wide discussion; reactions never wake
agents. Imported reaction names are historical
snapshots, not local access.

An agent author's name or avatar opens that agent beside the discussion. If the
agent is unavailable, the app reports that it cannot open the reference. Sending
in an existing discussion preserves its message row while confirmation arrives; failed messages can be
retried without consuming a newer draft. Discussion composers currently support
text and references, not image pasting or file attachments. The discussion and
agent-chat @ menus have different recipient and delivery rules. Sending a message
with a selected agent in a workspace-wide discussion explicitly addresses that agent.

With person-mention support from the connected daemon, selecting a person in the
discussion's @ menu records who is mentioned. Sending can alert that person when
the discussion is off screen and notifications are enabled. Plain text that looks
like an @ mention does not identify a recipient. Notification actions open the
relevant discussion subject to current access. Unread counts appear in discussion
navigation and inactive tabs. They reflect messages observed by this client and
clear as focused discussion content is read; they are not durable cross-device
read receipts.

In the People panel, invitations to specific accounts appear as **Invited** for people who can manage sharing. A share
link without a named recipient does not add a person. Accepted members remain in
People when they stop viewing the workspace and appear greyed out. Being connected
to the host elsewhere does not mean they are active here.

In the People panel's Activity list, nearby consecutive actions by the same person
share an avatar. Each action remains individually selectable. **Load more** looks
for older activity from people, even when automated events fill the recent feed.
In agent chat, other people's typing appears as a compact status above the prompt
without moving the messages or monitor cards.

<!-- Sources: packages/cloudlands-fe/src/features/presence/components/{WorkspacePeopleRoster,WorkspacePeopleActivity,PresenceTypingIndicator}.svelte; packages/cloudlands-fe/src/store/renderer/slices/{presence/presence-selectors,workspace-share/workspace-share-selectors}.ts; packages/cloudlands-fe/src/lib/components/chat/ChatPanel.svelte. -->

<!-- Sources: packages/intentd/crates/intent-core/src/discussion.rs; packages/intentd/crates/intent-services/src/discussion_ops.rs; packages/intentd/crates/intent-transport/src/retired_gate.rs; packages/intentd/crates/intent-store/src/discussion_management_repo.rs; packages/intentd/crates/intent-acp/src/mcp_server/bindings/discussion.rs; packages/cloudlands-fe/src/features/discussions/components/{DiscussionThread,DiscussionMessageView,DiscussionMentionPicker,DiscussionUnreadBadge}.svelte; packages/cloudlands-fe/src/features/discussions/discussion-reference-navigation.ts; packages/cloudlands-fe/src/features/layout/tab-types/DiscussionTabType.svelte; packages/cloudlands-fe/src/store/renderer/slices/notifications/sagas/notifications-saga.ts; packages/cloudlands-fe/src/store/renderer/slices/workspace-discussion/workspace-discussion-selectors.ts. -->

<!-- Sources (paths relative to packages/cloudlands-fe): src/features/settings/MobileSettings.svelte; src/features/devices/{PersonalDevicesPanel.svelte,personal-devices-selectors.ts}; src/store/renderer/slices/principal/principal-selectors.ts; src/store/renderer/slices/websocket-api/sagas/websocket-api-saga.ts; messages/en.json. -->

Only when explicitly asked about personal device pairing: it pairs another device
with the signed-in person's current access. It is separate from the local Remote
Access QR, which grants access to that computer. Do not offer the local QR as a
substitute or unrelated alternative. If the user asks where to test personal
pairing, use **Settings → Mobile → Intent Mobile → Show QR Code** on the intended
connection. Older instructions may call it **Pair another device as me**; that is
not the current button label. Personal pairing requires a current signed-in identity
and the connection's personal-pairing capability. The restricted-access panel also
requires a refreshed identity; the remote-owner panel needs Remote
Access enabled. Connected-device list support is independent. Do not invent a
readiness date or advise enabling the experiment for ordinary mobile setup.

## Inline visuals

Intent desktop v2.253.0 and later can show interactive visuals directly in chat.
Ask an agent to visualize an explanation, comparison or changing inputs, such as
“visualize how seasons work.” The agent uses a self-contained `ws-block:html`
fence and checks it with the browser `previewHtml` action before showing it.
Ordinary `html` fences remain source; incomplete fences do not execute.

New agents receive their creation-time visual guidance from the daemon. Older
agents keep their original instructions; create a new agent after updating the
daemon to receive corrected guidance. Installed desktop support is separate:
if `previewHtml` is unsupported, the agent reports that limitation and offers a
saved image or text explanation. A newer daemon alone does not update the desktop.
If controls do not respond in a scaled embedded-browser preview, choose
**Fit panel** in its size menu and retry.

Visuals use app typography and theme colors, fit the chat width, and retain local
inputs through theme and size changes. They cannot access external libraries,
network resources, arbitrary app APIs, navigation, popups or embedded frames.
**Source** shows the code; **Reload** restores the last published configuration.
If a completed visual cannot start, it shows an error instead of loading indefinitely.
Choose **Reload** to retry; **Source** remains available to inspect the code.

Visuals publish bounded JSON state, inputs and derived results through
`window.intentVisual`. The host toolbar can **Save scenario** (up to six),
compare, restore or remove scenarios. **Use this configuration** appends the
selected inputs and results to the editable composer draft without sending it.
**Export** saves JSON and editable source to fixed workspace paths. It does not
provide general SVG/PNG export. For an image to share elsewhere, ask for a saved
PNG instead. A labeled file input can read an explicitly selected local CSV,
JSON or image; it cannot read other paths or fetch resources automatically.

The browser reference (`ws.browser.docs("overview")`, **Inline visuals**) gives
agents the authoring, layout, theme and persistence rules. Native author preview
returns a screenshot, content height and console messages at a chosen width and
appearance, so the agent can correct the visual before it appears in chat.

<!-- Sources: packages/cloudlands-fe/src/features/inline-html/{InlineHtmlPreview.svelte,inline-html-document.ts,inline-html-runtime.ts,inline-html-styles.ts}; packages/cloudlands-fe/static/inline-html-sandbox.html; packages/cloudlands-fe/src/features/browser/main/inline-html-preview-service.ts; packages/cloudlands-fe/src/lib/components/browser/BrowserViewportMenu.svelte; packages/intentd/crates/intent-acp/src/mcp_server/bindings/browser_docs/overview.md. Visual workflow, layout and theme rules adapted from T3 Code (MIT, see packages/intentd/NOTICE). -->

## Finding things

Answer location questions with the destination and action first. Read settings only
when current state affects the answer or the user reports a problem; finding a
control is not a request to change configuration.

The paths below identify the documented UI destinations. `ws.app.ui.targets()` can
omit known destinations: an omission does not mean the feature is missing. Use a
returned target for a navigation link when available; otherwise give the documented
click-by-click directions in prose.
Do not invent target IDs, anchors, settings tabs, or current setting values. A backend
settings category is not necessarily a visible Settings section. If navigation
cannot open a documented destination, explain how to reach it manually and check
the current connection, access, and version rather than guessing another URL.

<!-- Sources (paths relative to packages/cloudlands-fe): src/lib/components/settings/SettingsSidebarNav.svelte; src/shared/app-ui-targets.ts; messages/en.json. -->

### Search with Cmd+K

Press **Cmd+K** (Ctrl+K on Windows and Linux), type a few letters, and press Enter.
While you type, all matches show in one list, best match first, each with a small
category label such as **Agent**, **Repo**, or **File**. An exact name match comes
first, then items you picked before for the same text, then stronger matches, then
recent items. Files, note text, and chat messages arrive a moment later and appear
below the top result. With nothing typed, the palette lists items grouped by category.

Repos you have used in Intent are listed too. Enter on a repo opens **New Workspace**
with that repo selected and the cursor in the prompt; nothing is created until you
submit. A repo with a local clone and a GitHub or GitLab entry shows both rows,
labeled by source. Type `+` or click the **Repos** chip (Tab cycles the chips) to see
only repos; `+` alone lists them most recent first. Repos appear only when you can
create workspaces.

<!-- Sources (paths relative to packages/cloudlands-fe): src/lib/components/CommandPalette.svelte; src/lib/components/CommandPaletteFilters.svelte; src/store/renderer/slices/command-palette/command-palette-results.ts. -->

## Find a setting

Open Settings and use these sidebar labels (English labels shown):

| Section | What it contains | Canonical link |
| --- | --- | --- |
| Appearance | Theme, colors, language, fonts, and chat display | `/settings?tab=display#theme` |
| General | Notifications, updates, external apps, GitHub link behavior | `/settings?tab=app-behavior#notifications` |
| Input and shortcuts | Voice dictation and keyboard shortcuts | `/settings?tab=input#keyboard-shortcuts` |
| Agent defaults | Global instructions and agent features | `/settings?tab=agent-behavior#global-instructions` |
| Providers | AI coding CLIs and default model | `/settings?tab=providers#providers` |
| Connections | Integrations, MCP servers and plugins | `/settings?tab=connections#integrations` |
| Machines | Local machine, connected backend and saved remote connections | `/settings?tab=devices#devices` |
| Mobile | Intent Mobile pairing and Remote Access | `/settings?tab=mobile#mobile` |
| Workspace setup | Git, shell, and workspace defaults | `/settings?tab=setup#git-workspace` |
| Advanced | Agent backend, connection, tool output/retention, data, reset | `/settings?tab=advanced#workspace-api` |

In test builds with **Full width**, open **Settings → Appearance → Full width**.
It uses the full panel width for messages, the composer, and status content and is
on by default. Turn it off to center that content within an **800 px** limit.
Narrower panels still use the available width, and aurora still covers the full
panel. The chat pane's **More** menu also offers **Full width**, **Chat aurora**,
and **Collapse tool activity** beside **Font**. These shortcuts change the same
Appearance preferences for all chats on this device.

<!-- Sources (paths relative to packages/cloudlands-fe): src/lib/components/settings/ChatAppearanceSettings.svelte; src/lib/components/chat/ChatContentLane.svelte; src/features/layout/tab-types/AgentViewSettingsDropdown.svelte. Test-build behavior pending its frontend release. -->

When the user asks the Assistant to change a setting, `ws.app.settings.propose` shows a
change card the user applies or discards; the card can be undone. It accepts only
settings whose definition has `proposable: true`. Credentials, remote access and
sharing, forge connections, executable and folder paths, command permissions, and
global or workspace instructions are refused: send the user to the matching Settings section above instead.

<!-- Sources: packages/intentd/crates/intent-services/src/settings.rs (PROPOSAL_EXCLUDED_*); packages/intentd/crates/intent-acp/src/mcp_server/bindings/app/settings.rs; packages/cloudlands-fe/src/lib/components/chat/proposals/{settings-proposal-actions,settings-proposal-definitions}.ts. -->

<!-- Sources: packages/intentd/crates/intent-services/src/mcp_servers.rs; packages/intentd/crates/intent-acp/src/mcp_server/bindings/app/mcp.rs; packages/cloudlands-fe/src/lib/components/settings/McpServersSettings.svelte; packages/cloudlands-fe/src/lib/components/settings/mcp/mcp-options.ts; packages/cloudlands-fe/src/features/mcp/main/mcp-oauth.ts. -->

For MCP configuration, open **Connections → MCP servers**; its reference route is `/settings?tab=connections#mcp-servers`. Use **Quick Install** for the listed presets or **Add New** for a custom configuration. Users can also ask the Assistant to set up or update a server. Use `ws.app.mcp` to read existing configurations, avoid duplicates, and apply the requested change app-wide. These configuration tools are available only in Assistant chat; ordinary workspace agents retain access to enabled servers' tools.

Check the provider's official setup instructions before choosing a URL or local command. Creation connects by default; `enabled: false` saves a disabled server. For edits, read the full configuration first and preserve redacted credential placeholders; omitted keys are removed. When MCP is enabled, saving edits to an enabled server reconnects with the saved settings. Preserve its enabled value unless asked to change it. Check the returned status: saved does not mean connected. For `auth_required`, direct the user to the existing sign-in flow in **Connections → MCP servers**; keep tokens out of chat. Disabled MCP and connection errors must be reported as such.

A failed local server start or restart reports `error` with `lastError`. Fix its configuration and retry; the failure remains visible until the server is stopped or another start replaces it.

**Skills that need an MCP server.** New agents don't see a skill that names tools from an MCP server that isn't enabled here. They also don't see a skill whose `compatibility` field names other agent products but not Intent or the agent's provider. For Claude Code and Codex, servers set up in the provider's own config also count; for other providers, only Intent's settings do. To bring such a skill back, add or enable its server in **Connections → MCP servers**; the next new agent sees it. Existing agents keep the skills they started with. Long skill lists are shortened: descriptions are trimmed, and the last skills appear by name only.

<!-- Sources: packages/intentd/crates/intent-services/src/skills.rs; packages/intentd/crates/intent-services/src/harness/v3_10.rs. -->

<!-- Sources: packages/intentd/crates/intent-services/src/plugins.rs; packages/intentd/crates/intent-services/src/plugins/ops.rs; packages/cloudlands-fe/src/lib/components/settings/{PluginsSettings.svelte,plugins/PluginList.svelte}. -->

Plugins follow the [Agent Plugins](https://agent-plugins.org/specification) format: a folder with `plugin.json`, optional `skills/` and an optional `mcp.json`. Install one once in **Connections → Plugins** (`/settings?tab=connections#plugins`) from a folder path or a git URL, and every agent gets it on any provider. Its skills appear as `<plugin>:<skill>`. Its MCP servers appear in **Connections → MCP servers** as `<plugin>-<server>`, labelled with the plugin; they work like the user's own servers (status, sign-in, switches, `ws.app.mcp` reads) but can only be changed by updating or removing the plugin. Changes reach new agents only. The row shows warnings for parts that were skipped, and **Update** fetches the source again. The Assistant cannot install plugins for the user yet: send the user to that page.

## Mac update availability

Mac update availability can differ by architecture. Intel Macs stay on their last
compatible available release in the selected update channel when a newer release
has no Intel build. Older Apple Silicon apps may also remain on an earlier
compatible release; manually installing a current Apple Silicon build lets them
receive newer releases. Do not
suggest an Apple Silicon build for an Intel Mac.

<!-- Sources: packages/cloudlands-fe/src/features/auto-update/main/{auto-update.service.ts,mac-update-compatibility.ts}; packages/cloudlands-fe/scripts/mac-channel-feeds.mjs; packages/intentd/scripts/optional-intel-release.py; packages/intentd/crates/intentd-sitter/src/updater.rs. -->

## Anonymous update statistics

Update checks go through `updates.intentapp.dev`, which counts them (platform,
version, channel, install source, check trigger, country; no IDs, IPs not stored)
and redirects to the GitHub release; if it is unreachable, updates come straight
from GitHub. Each check also says whether it is this install's first check of the
UTC day, week, or month, or its first check ever, plus a coarse install-age bucket;
only those dates are stored locally. The desktop app's switch is **Settings → General → Updates → Share
anonymous update statistics** (on by default). For standalone intentd, set `DO_NOT_TRACK=1`,
`INTENTD_DISABLE_TELEMETRY=1`, or `telemetry = false` in
`<data-dir>/sitter/config.toml`.

On the stable channel, standalone intentd checks for updates automatically at most
once per UTC day (at startup if it has not checked that day, otherwise within the
first hour after midnight UTC) and restarts into an update once no agent turn is
running, forcing the restart only after 24 hours. Alpha and beta still check
hourly when idle. `intentd update` always checks immediately.

<!-- Sources: packages/intentd/crates/intentd-sitter/src/{telemetry.rs,activity.rs,updater.rs,config.rs,supervisor.rs}; packages/cloudlands-fe/src/features/auto-update/main/auto-update.service.ts. -->

## Manage Assistant chats

A new Assistant chat names itself during its first reply; until then the chat list
previews the first message. A name the user sets by renaming the chat is kept.
Users can ask the Assistant to find, read, rename, or delete another Assistant chat.
Use `ws.app.chats.list` to identify it and read only the relevant conversation slice.
Ask which chat they mean if several match. Apply a rename or deletion only when
requested. These tools are exclusive to Assistant chat and cannot target workspace
agents. A chat cannot delete itself through these tools.

Deletion uses the app's 15-second undo window. Report the deletion as scheduled;
`ws.app.chats.cancelDelete` can undo it before the returned deadline. It cannot
recover a chat after deletion has completed.

<!-- Sources: packages/intentd/crates/intent-acp/src/mcp_server/bindings/app/chats.rs; packages/intentd/crates/intent-services/src/agent_manager.rs; packages/cloudlands-fe/src/store/renderer/slices/sidebar-nav/chief-thread-title.ts; packages/cloudlands-fe/src/lib/components/chat/assistant-thread-rename.svelte.ts; packages/cloudlands-fe/src/lib/components/layout/sidebar-nav/cards/ChiefCard.svelte; packages/cloudlands-fe/src/features/agent/utils/pending-agent-deletions.ts. -->

## Choose a model or change agent behavior

<!-- Sources: packages/cloudlands-fe/src/lib/components/workspace/initializer/InitialAgentPicker.svelte; packages/cloudlands-fe/src/lib/components/settings/{SettingsSidebarNav,ProviderSelector,ProviderProfiles,ProviderProfileDialog,BackgroundAgentSettings}.svelte; packages/cloudlands-fe/src/store/renderer/slices/background-agent-settings/background-agent-settings-slice.ts (editQuickAction); packages/cloudlands-fe/src/lib/components/chat/input/ModelPicker.svelte; packages/cloudlands-fe/src/shared/app-ui-targets.ts; packages/intentd/crates/intent-services/src/{provider_profiles,profile_activation,quota_failover,provider_return,provider_version,agent_manager,complete_ops,enhance_ops}.rs; packages/cloudlands-fe/src/lib/components/chat/ProviderFailoverNotice.svelte; packages/cloudlands-fe/src/store/renderer/slices/workspace/workspace-selectors.ts (selectHidesProviderProfileNames). -->

For the default model, open **Settings → Providers → Default model**
(`/settings?tab=providers#utility-default-model`). For new work, choose a model in
the workspace creation form where offered. Available choices depend on configured
providers; diagnose a missing provider in **Providers**, where each installed
provider shows its CLI version. Changing a default provider
or model affects new agents only; each existing agent keeps its own provider, model
and reasoning level until changed in its own model picker. Specialist entries appear
in the Settings sidebar's Agents group. Create one at
`/settings?tab=specialists&view=create-specialist#create-specialist`; select an
existing specialist to edit its behavior. Global defaults and a specialist's
instructions are different scopes. Some agent feature changes apply only to newly
created sessions; do not promise they alter running conversations.

Quick action models live under **Settings → Providers → Quick Actions**
(`/settings?tab=providers#utility-default-model`). **Commit message** and **PR
description** each have one model picker for the default provider; its default option
is the provider's own default model, and effort is set in the same picker. There is no
shared quick action model. A shared default saved by an older version still applies
until the first change there, which copies it into each action.

**Built-in coding specialists.** Developer implements directly, plans as needed,
and delegates focused work to general agents. Lead, Coordinator, Implementor, and
Verifier are no longer built-in choices for new sessions. Developer defers verification
until work is complete, minimizes runs, and prefers end-to-end tests with repeatable
artifacts for complex features. For isolated testing, it identifies failure modes first.
For UI work, Developer reuses app patterns, preserves user context, and shows
screenshots of the rendered result. Product Manager never edits code: it asks
clarifying questions, explores the code, and writes the spec in the workspace **Spec**
note. A second Product Manager critiques the spec; after you approve it, Product
Manager starts a top-level Developer to implement it (this uses **Top-level agent
spawning & retirement**, on by default). Maintenance Coordinator keeps a steady
pipeline of small maintenance PRs (bugs and issues, dependency and security alerts,
CI failures and flaky tests), each reviewed and green; it never merges without the
user's explicit permission. A new harness version
applies to new sessions; existing sessions keep their captured behavior, and
custom specialist instructions can override the defaults.

<!-- Sources: packages/intentd/crates/intent-services/resources/specialists/{v3.8,v3.9,v3.10}/; packages/intentd/crates/intent-services/src/{specialists.rs,harness/v3_8.rs,harness/v3_9.rs,harness/v3_10.rs,harness/v3_12.rs}. -->

<!-- Sources (paths relative to packages/cloudlands-fe): src/lib/components/settings/agent-feature-definitions.ts; src/lib/components/chat/secrets/SecretRequestCards.svelte. Daemon: packages/intentd/crates/intent-acp/src/mcp_server/bindings/secret.rs. -->

**Secret requests.** Off by default. Open **Agent defaults**
(`/settings?tab=agent-behavior#global-instructions`) and turn on **Secret requests**
under **Agent features**; **Host command execution** must also be on. It applies to
top-level workspace agents created afterwards, not to sub-agents, background hooks
or Assistant chat. When such an agent asks for a secret such as an API key, a masked
card appears above the message box after its turn ends; **Hide** collapses it to a
"Secret requested" chip, and the user can provide or dismiss it. The value is not
put in the chat: Intent holds it in daemon memory and passes it only to that agent's
`ws.host.exec` commands that name it, redacting exact copies from their output. It is
dropped when the daemon restarts, the agent is deleted or retired, or its workspace
is archived; the agent then has to ask again.

**Disable a provider.** In **Settings → Providers**, choose **Disable** from its
⋯ menu, including for the default provider or one used by saved models or
specialists. Saved selections remain intact. A turn already running can finish;
disabling does not stop it. On their next turn, conversations using the provider's
Default configuration move to the configured app default if it is enabled and
usable, with a notice in chat. Named profiles require an explicit replacement
instead. If no usable default exists, re-enable the provider or choose another
enabled provider/model. New agents and quick actions cannot run on a disabled
provider; update their saved selections or re-enable it. Intent does not choose an
arbitrary replacement for a disabled app default.

<!-- Auggie visibility: packages/cloudlands-fe/src/shared/provider-visibility.ts; packages/cloudlands-fe/src/features/onboarding/messages/AgentGrid.svelte; packages/cloudlands-fe/src/store/renderer/slices/provider-catalog/workspace-catalog-selectors.ts. -->
Auggie appears in provider lists, including onboarding, only when its CLI is
installed or it is already enabled. An enabled but missing installation stays
visible so you can disable it; it disappears afterward. Sign-in is not required
for an installed Auggie to appear.

**Provider profiles (Codex and Claude Code only).** A profile runs the installed
CLI with its own environment variables, for example a work and a personal account
or a different endpoint. In **Settings → Providers**, open the ⋯ menu on Codex or
Claude Code and choose **Add profile**, then a kind: **Different login**, **API key**
(Claude Code only), **Token**, or **Advanced**. **Different login** creates a new
sign-in folder under `~/intent/profiles/` on the daemon host. **Create & sign in**
closes the dialog and runs the sign-in in the bottom terminal; the profile row shows
**Waiting for sign-in…**, then a **Signed in** toast. When the daemon runs on another
machine, Codex shows a one-time device code instead of a browser sign-in; device code
login must first be turned on in ChatGPT → **Settings → Security** (or allowed by a
workspace admin). The folder starts without the user's usual Claude Code or Codex
settings (instructions, MCP servers, skills). Later, ⋯ → **Log In** on a Different-login
profile signs in again to the same folder. Removing that profile leaves its folder,
with its saved login, on disk. **API key** saves an Anthropic key (optionally a custom
endpoint); **Token** saves a `claude setup-token` token, or a Codex access token
(ChatGPT Business or Enterprise only). After **Create**, Intent runs a test prompt and
shows **Key works** or **Key was rejected**. These kinds also unset inherited
credentials that would otherwise win. **Advanced** is the raw editor: a name and
environment variables (literal values, **Empty value**, or **Unset**; no shell
expansion). Use `CODEX_HOME` or `CLAUDE_CONFIG_DIR` to choose an account folder; there
only, a leading `~` or `~/` means the provider host user's home folder (`~user` stays
literal), and the saved folder path is shown as typed while other saved values stay
hidden. Credentials inherited from the environment can still apply to an Advanced
profile: for a Claude Code profile that should use another account (or none), set
inherited variables such as `CLAUDE_CODE_OAUTH_TOKEN` and `ANTHROPIC_API_KEY` to
**Unset** in that profile. Codex signed in with a ChatGPT account may ignore a
custom base URL from the environment; for a different endpoint, give the profile
its own `CODEX_HOME` whose Codex configuration sets it. Saving does not prove sign-in: the status shows
**Configured**, **Checking…**, **Ready**, or **Not signed in**. Each profile
appears as its own model-picker group, such as **Codex · Work**; with profiles,
the original configuration is **Codex · Default**. In a shared workspace, guests see
an agent's provider and model without the host's profile name, including in chat notices. Each profile has its own **Fast
mode** (Claude Code) or **Service Tier** (Codex) in its ⋯ menu; it starts off and
never follows the Default setting. Delegated agents follow their
explicit or specialist selection, else their parent agent's provider when it is
enabled, installed and not signed out, else the default; never the parent's profile.
Once a provider has profiles, its original configuration is listed first as the
Default profile, where backups start from; it cannot be moved, and shows its name
with a **Default** badge once renamed. To rename it or give it its own environment
variables (same rules as a profile, without the backup switch), choose **Edit** in
its ⋯ menu, or **Environment variables** in the Codex or Claude Code ⋯ menu; a blank
name restores **Default**. Settings → Providers does not mark the app's default
model or provider; use **Default model** for that.

**Backup profiles.** Drag a named profile in **Settings → Providers**, or focus its
handle and use the Up and Down arrow keys, to set its order below the Default profile. Turn on **Use as backup
provider** in the profile dialog; backup profiles show a **Backup** badge. When a
conversation's current Codex or Claude Code configuration runs out of quota, the turn
continues on the next backup of the same provider in saved order, with the same
model, and a chat notice shows the switch; the conversation then stays there. After
the quota reset time (or 5 hours when none is reported), the next turn goes back to
the configuration it used before its first automatic switch, with a "back on …"
notice; if that one is still out of quota, Intent tries again later with growing
waits. Quick actions (commit messages, PR descriptions, quick tasks) also try the
backups in the same order when their configuration is out of quota; their saved
quick-action setting does not change. Conversations share what they learn: once one finds a configuration out of
quota, others skip it on their next turn (until its reset time, or 5 hours), and once
one succeeds on it again, conversations that left it go back on their next turn.
Saving a change to a profile or to a Default configuration (for example a new API key
or account folder) clears what was learned about it, so it is no longer skipped.
Choosing a model or profile yourself makes that the new starting point. When every
option is out of quota, Intent still tries them, and the usual quota message appears.

Switching a conversation to another profile, even with the same model, applies on
the next message: a turn in progress finishes first, then a fresh provider session
starts with the conversation history. Editing a profile's environment never
interrupts or restarts anything. Running sessions keep their settings and show
**Using earlier settings**; new provider processes, including ones restarted after
idling or a crash, use the saved settings. To refresh existing conversations,
restart Intent when the app runs the daemon on this computer, otherwise restart
intentd on the provider host; reconnecting alone does not apply the edit. Remove a
profile from the same view, after changing any default, specialist, or quick-action
setting that selects it. Conversations keep their history, and their next message
asks for another model (**Choose another model to continue**), as does a profile
whose provider is disabled. Intent does not move them to another account; only the
quota backup switching above changes a conversation's profile automatically.

Settings sections and controls can depend on platform, daemon capabilities, or
access. A supported route does not grant permissions or prove that a control is
available on the current connection.

<!-- Sources: packages/cloudlands-fe/src/features/settings/MobileSettings.svelte; packages/cloudlands-fe/src/lib/components/settings/{DevicesSettings,DeviceRow,WebSocketApiSettings}.svelte; packages/cloudlands-fe/src/store/renderer/slices/websocket-api/sagas/websocket-api-saga.ts; packages/intentd/crates/intentd/src/main.rs (`intentd pair --host`). -->

## Find the mobile pairing QR code

Open **Settings → Mobile → Intent Mobile → Show QR Code**. Use the mobile app's
pairing scanner. The Remote Access reference route is
`/settings?tab=mobile#websocket-api`. There is no need to expand a device's Edit panel.

For ordinary local-machine pairing, select that machine as the active connection.
If connected elsewhere, use **Settings → Machines**, then the local machine's
**Connect** action, and return to **Mobile**. The QR controls remain visible but
disabled when **Enable Remote Access** is off, data is loading, or no running listener
port is available. Explain the required enablement; do not silently change it.
For a generation error, inspect the displayed connection/listener error. TLS and
tunnel switches are not visibility requirements: do not suggest toggling them to
reveal the controls. Local pairing grants access to that machine; do not offer it
as a substitute for someone else's restricted access.

Pairing links and QR codes contain credentials; keep them private. A visible QR does not prove that the
phone can reach the host. Diagnose address/network reachability separately from
finding the QR control.

## Find work on another device

**Settings → Machines** lists **<name> (local)** first, then the backend this window
is connected to as **<name> (connected backend)**, then saved remote connections. Each
row shows an online/offline dot, a macOS, Windows or Linux icon and icons for the AI providers
available there; hover an icon for its name. Cloud entries show a cloud icon, a dot that is green
while the entry is ready, and Linux. A badge appears only while a machine is being made available
(**Exposing**) or for a problem (unreachable, revoked, update required, key rejected), and the
version only when the machine's intentd is older than the app expects. Switched-off providers are hidden, and offline or unknown machines show no
icons, except a machine the backend runs agents on: while idle (disconnected) it still
shows the providers from its last report, or **No providers ready**. **Add machine** opens **Set up a new machine** (when cloud machines can be added, it
is the first item of a menu next to **Add cloud**; the machine rail's **Add machine** opens
it too), which offers **Paste an intent:// link**, **Enter connection details** (host, port and access
token), **Install an Intent server** (a complete, independent Intent) and, when the backend can run agents on other machines,
**Install a node**. The install cards show one command to copy and run on the other
computer (macOS / Linux, or Windows); it installs intentd as a service and ends by printing
an intent:// link to paste back. A
remote machine's actions include **Edit** and **Connect**, with **Test connection** inside
its editing panel.
<!-- Labs (SSH machine setup, desktop app only): describe only when the user asks about
installing over SSH, and say it is Labs (turn on **SSH machine setup** in
Settings → Advanced → Labs). With it on, the install cards also offer
**Set it up for me over SSH** for Linux computers only (a Mac or Windows PC uses the
command): sign in (SSH agent, key file or password; nothing is saved), confirm an unknown
host key, and the app installs, starts, pairs and adds the machine. On a computer that
already runs intentd it never reinstalls, restarts or changes its role: it starts a
stopped daemon, pairs it (which may turn on remote connections and the Tailcat tunnel; the
wizard says so) and adds it. On **Install a node**, a full backend keeps its role and also
becomes a node (a backend set to run no agents itself gets an error). A node set up from
the Install cards is made available without the "Make available" prompt. A server may need
`sudo loginctl enable-linger <user>` so intentd keeps running after logout (the wizard
shows it).
Sources: packages/cloudlands-fe/src/features/machine-setup/,
packages/intentd/scripts/install.sh (INTENTD_PAIR, INTENTD_LINK_MARKER). --> A machine's
actions can also include **Let <backend> run agents here**, which asks to confirm with
**Allow**, and **Stop letting <backend> run agents here**; machines allowed this way are
listed under **Machines <backend> can run agents on**.
Running agents on those machines is experimental. That list is in the user's order of
preference, and its first row is marked **Preferred**: **This computer** first by default, then machines and cloud entries as they
were added. Reorder it by dragging a row, or with **Move up** / **Move down** in the row's
menu. In **Where <backend> runs agents**, a row shows a status badge only when it cannot run
agents (such as **Unreachable**, **Update required** or **Key rejected**). An agent started without a **Start on** choice runs on the first row that can run
it; a sub-agent started by an agent on this computer stays here. A backend started as an
orchestrator (`intentd serve --orchestrator`, experimental) runs every agent on those
machines: its list has no **This computer** row, and **Settings → Providers** shows only
the enable switches and profiles, with no **Test** button and no sign-in status. On an
orchestrator, **Start on** offers **First available machine** instead of the backend, plus
each machine. Agents see each machine's name and
platform, and look up its providers, sign-in status and models when they need them.
A Default with **Use on remote machines** turned on in **Settings → Providers** is
always sent when it has saved settings, even to a machine signed in to that provider. Otherwise a machine uses its
own provider sign-in when it has one, or a profile with that option turned on is sent
there; a profile that sets a sign-in folder can't be turned on, and adding an API key to
a Default turns it on. A provider that is neither signed in on a
machine nor covered by such a profile isn't offered for that machine. A refused choice
says why and how to fix it.
An agent running on one of those machines uses that machine's skills (its
`~/.agents/skills` and `~/.claude/skills`, plus the project's), and its **/** menu lists
them; to give it a skill, add the skill there. A **/name** it lacks still sends, with a
notice in the chat.
<!-- Labs (remote agents): describe only when the user asks about cloud machines,
and say it is Labs. The backend can start its own machines from a saved cloud entry:
an E2B entry (API key) starts a sandbox, a Northflank entry (API token and project id)
starts a Northflank service with its own disk. Northflank machines offer sizes 2x4,
4x8 and 8x16; the region is the project's. Cloud machines have no sign-in, so only a
profile or Default marked for remote machines that sends an API key runs there. An
idle cloud machine pauses. A paused E2B sandbox resumes with its memory; a paused
Northflank machine resumes from its disk only: the checkout, uncommitted changes and
home folder stay, while running processes, terminals and scripts stop, and software
installed outside the home folder may be gone. The agent is told this at its next turn.
While its machine is paused or gone, the agent's model picker lists no models and says
the machine isn't running; the next message resumes it or starts a new one, and its
models then appear. **Check again** is not offered for a cloud machine.
Sources: packages/intentd/crates/intent-svc-remote/src/cloud/ (config.rs, lifecycle.rs),
packages/intentd/crates/intentd/src/executor/checkout.rs (first_turn_notice),
packages/cloudlands-fe/src/lib/components/chat/input/ModelPicker.svelte (remoteCloudEmpty). -->
Workspaces and agents belong to a host, so identify the intended device before
explaining missing work or changing host settings. **Mobile** follows the active
connection and its permissions; do not describe a remote connection's QR as belonging
to the local desktop. To pair the local machine, connect to it first. For intentd
running in WSL on the same Windows PC, add a machine with host `localhost`;
`intentd pair --host localhost` in WSL prints the port and token. Enabling access,
changing listening addresses, and configuring a tunnel are separate choices from
displaying pairing information. Read current state and respect the user's requested scope.

<!-- Sources: packages/cloudlands-fe/src/lib/components/settings/{BrowserAccessSettings,BrowserAccessSection}.svelte; packages/cloudlands-fe/src/lib/components/settings/LabsSettings.svelte; packages/cloudlands-fe/src/features/browser-access/main/{browser-access-server,browser-access-controller}.ts; packages/cloudlands-fe/src/lib/client/live/browser-access-host.ts; packages/cloudlands-fe/src/features/intent-web/components/. -->

## Use Intent from a browser on another device (experimental)

Browser access is experimental and hidden until the user turns on **Browser access** in
**Settings → Advanced → Labs**. Do not offer it as a normal option; describe it only
when the user asks about it, and say it is experimental.
Turning that switch off hides the section and turns the server off.

Once unlocked, **Settings → Mobile → Browser access** (`/settings?tab=mobile#mobile`) lets
a browser on another device, such as a laptop, use the desktop app, including every
backend that desktop connects to. It is off by default and the desktop app must stay
open. Intent listens only on this machine (`127.0.0.1`, port 47120 by default), so the
user reaches it through a private HTTPS front-end they set up themselves; with
Tailscale that is `tailscale serve --bg <port>` on the desktop, then the desktop's
Tailscale address in the browser. Never suggest Tailscale Funnel or another public tunnel.

The browser asks once for the **Access key** shown in that section, or for a code from
**Create sign-in link**: a 6-character code (such as `ABC-DEF`) that works once and
expires after 10 minutes, plus a link once a browser has signed in. Opening the link only
fills in the code; the user still selects **Sign in**. Sessions survive closing the
browser and last 400 days from the last visit. **Reset key** signs out every browser and
cancels unused codes. In the browser, opening a saved connection opens that backend in a
new tab; adding or forgetting connections stays on the desktop. Desktop-only features
such as native file dialogs and window controls are unavailable in the browser; a notice
listing them appears each time the browser loads Intent. If the section shows the port
is in use, change **Port**. If it says the build does not include the browser version,
the installed app predates the feature.

<!-- Sources: packages/cloudlands-fe/src/features/daemon-status/StartupProgress.svelte; packages/cloudlands-fe/src/shared/daemon-startup.ts; packages/cloudlands-fe/src/features/backend/main/json-rpc-client.ts; packages/cloudlands-fe/src/lib/client/live/browser-websocket-transport.ts; packages/cloudlands-fe/messages/en.json; packages/ios/Intent/Services/{ConnectionManagerStartup,WebSocketAdmission}.swift; packages/ios/Intent/Views/Connection/{DaemonStartupProgressView,ConnectingView,ConnectionLossPresenter,ReadOnlyBanner,RemoteReadOnly,AvailableWhenReconnectedView}.swift; packages/ios/Intent/Services/ConversationStorePool.swift; packages/ios/Intent/Views/Workspaces/WorkspaceDetailStores.swift; packages/ios/Intent/App/{AppCoordinator,RootView}.swift. -->

When the app and connected daemon support startup status, the connection screen
shows **Starting intentd** and **Finishing startup** while services initialize.
Keep the app open; it reconnects when ready. Unopened notes stay lazy: startup does
not rebuild their indexes or report indexing counts or an ETA. Preparing a note
on demand after startup does not make the daemon unready again.

If a connected daemon reports an indexing phase, **Rebuilding note indexes** shows
its measured counts and approximate indexing-only estimate; an unchanged count
alone does not mean failure. **Startup status unavailable** means fresh status
could not be received; do not present cached progress as current. For **Daemon
startup failed**, check the daemon logs.

With matching mobile and daemon support, iPhone and iPad show **Starting Desktop**
and **Finishing Startup** during initial connection over saved
direct or Tailcat routes. **Indexing Notes** appears only when the daemon reports
that phase. **Startup status unavailable. Reconnecting securely…** means current
status is unavailable. For **Startup Failed**, use **Retry Connection** after
checking the desktop daemon. **Back to Home** keeps saved credentials.

If the connection drops after the iPhone, iPad, or Vision Pro app has opened, the app
stays readable. A banner at the top shows **Reconnecting to <Mac>… · Read-only**,
**No network connection · Read-only**, or **Local Network access needed · Read-only**.
Tap it for guidance and **Back to Home**. The current screen, the workspace list, the
last 5 workspaces and the last 10 conversations opened this session stay readable.
Other content shows **Available when reconnected** and loads when the connection
returns. Controls that change something on the Mac, such as Send, are disabled.
Typing a draft and adding attachments still work, and nothing is sent automatically.
While the desktop restarts, the banner follows its startup instead, for example
**Desktop is starting… · Read-only** or **Indexing notes (3 of 10)… · Read-only**. **Startup
failed · Read-only** offers **Retry** and does not block reading.
The banner disappears and data refreshes when the connection returns. If reconnection
stops, for example because access was removed, **Can't reconnect** shows the reason
and **Back to Home**. Each window follows its own Mac.

<!-- Sources: packages/cloudlands-fe/src/lib/components/settings/ProviderSelector.svelte; packages/intentd/crates/intent-providers/src/{config.rs,cursor.rs}; packages/intentd/crates/intent-services/src/{cursor_cli.rs,provider_auth.rs}. -->

## Use Cursor as an agent provider

Cursor is an opt-in provider: it stays off until the user enables it in
**Settings → Providers** (`/settings?tab=providers#providers`), like other providers.
It needs the Cursor CLI (`cursor-agent`) on the daemon host, signed in with
`cursor-agent login`. Intent also accepts `CURSOR_API_KEY` in the daemon's environment;
when both exist, the stored login wins. If `cursor-agent` runs the Grok agent host
instead of Cursor, **Providers** shows it as unavailable: run
`cursor-agent update --channel cursor`, or set a custom path to the Cursor install.

Limitations: choosing a model for a Cursor agent also changes the user's default model
for the Cursor CLI. Cursor's own subagent tool still works; Intent only instructs
Cursor agents to delegate through Intent instead, and such runs show as **Subagent**
in the timeline. Cursor also loads your own `~/.cursor/mcp.json` and the project's
`.cursor/mcp.json` servers.

## Use Antigravity as an agent provider

<!-- Sources: packages/intentd/crates/intent-providers/src/{antigravity,config}.rs; packages/intentd/crates/intent-services/src/{agy_cli.rs,provider_models/mod.rs}. -->
Antigravity is an opt-in provider: enable it in **Settings → Providers**
(`/settings?tab=providers#providers`). It needs Google's `agy` CLI, version 1.5.0 or
later, on the daemon host. When agy is older, Intent runs `agy update` before it
starts an agent or refreshes models. Sign in once on that host with
`agy acp --login`. Intent runs `agy acp`; a custom path in **Providers** must point to
the `agy` executable.

Antigravity agents use the user's own agy settings, hooks, and MCP servers. Model
lists and sign-in checks run in a separate agy data folder, so they do not appear in
the user's agy history. Choose the thinking level as the reasoning effort.

## Run Intent in a browser from a cloud machine

`@intent-hq/web` (command `intent-web`) is a new, separately installed server that
serves the Intent web app without the desktop app, for example on a VPS or dev box.
It ships with Intent releases on npm. It is unrelated to the experimental desktop
**Browser access** above: do not tell the user to unlock Labs for it.

- Start it with `npx @intent-hq/web` (Node.js 22.22.2+, 24.15.0+ or 26+). It listens
  only on `127.0.0.1:47120` and prints an **access key** on first run;
  `npx @intent-hq/web key` shows it again and `key --reset` signs out every browser.
- To sign in a phone or another browser without typing the key, run
  `npx @intent-hq/web link` (add `--url https://<address>` for a link) on the server,
  even while it runs. The code works once, expires after 10 minutes and goes in the
  sign-in field. Sessions survive closing the browser and last 400 days from the last
  visit. Pass the server's `--data-dir` to `key` and `link` if it uses a custom one.
- Reach it through a private HTTPS front-end such as `tailscale serve --bg 47120`, then
  open the `https://` address and enter the key. Never suggest Tailscale Funnel or
  another public tunnel.
- If `intentd` is already running on the same machine when `intent-web` starts, it
  appears as **This machine (local)**; if it is missing, restart `intent-web`. Add
  other machines in **Settings → Machines → Add machine** with the host, port and token
  from `intentd pair` on that machine, then check the fingerprint and choose
  **Confirm & connect**. With no machine yet, the browser shows **Add your first
  machine**.
- Its data directory (default `~/.local/state/intent-web`) holds machine tokens in
  plain text; treat it like an SSH key. The **Settings → Mobile** tab, iCloud sync of
  saved machines and desktop-only features such as native file dialogs are not
  available there; a notice listing them appears each time the browser loads Intent,
  and **Got it** closes it.

<!-- Sources: packages/cloudlands-fe/docs/intent-web.md; packages/cloudlands-fe/src/features/intent-web/; packages/cloudlands-fe/src/features/intent-web/package/README.md; packages/cloudlands-fe/src/features/browser-access/main/browser-access-sign-in-codes.ts. -->

## Start or continue work

The **New Workspace** form focuses the prompt box when it opens, so you can type
your task immediately.

<!-- Sources: packages/cloudlands-fe/src/lib/components/modals/NewSpaceModal.svelte. -->

When a new workspace uses a remote-tracking base branch from a local repository,
Intent fetches that branch before creating the checkout; progress shows
"Updating <base> from <remote>…". If the fetch fails, creation stops and the
error names the cause (sign-in, untrusted SSH host, network, timeout, missing
repository, or access denied). The fetch runs on the machine Intent runs on,
so for a remote daemon fix SSH keys, credentials or network there, not on
your Mac, then try again. When the error offers **Continue from last-fetched
<base>**, the workspace starts from the remote branch as last fetched (it may
be out of date; unpushed local commits are not included). Explicit commits,
tags, and local-only branches still use their local state.

<!-- Sources: packages/intentd/crates/intent-services/src/lib.rs; packages/intentd/crates/intent-git/src/fetch.rs. -->

Legacy workspace import has been retired. Settings no longer offers the Data import
section, and a fresh daemon does not discover or import old Electron workspace
directories. Workspaces already stored in the daemon remain available.

<!-- Sources: packages/intentd/crates/intentd/src/main.rs; packages/cloudlands-fe/src/routes/(app)/settings/+page.svelte. -->

<!-- Sources (paths relative to packages/cloudlands-fe): src/features/home/{HomePage,HomeWorkspaceBoard,HomeWorkspaceDetail,HomeWorkspacePullBadge}.svelte; src/features/home/{home-model,home-preview-agent}.ts; src/features/home/home.css; src/lib/components/workspace/sidebar/workspace-pr-presentation.ts. -->

In **Home → Workspaces**, list rows and board cards show organization/repository
labels in views that include multiple repositories. When an organization is selected,
labels show only the repository name. Narrow lists hide the row label; the selected
workspace's preview header shows its repository. Local repositories show their name
or path. Long labels truncate; hover to read the full name. Repository-specific
views identify the repository in the page heading.

Selecting a workspace opens its preview on the top-level agent (not a background or
delegated agent) with the most recent activity; a workspace with no top-level agent
left opens on its most recently active remaining agent. The preview keeps that agent
while it stays open unless the agent is retired or deleted. Choose another from the
agent menu above the chat; Home remembers that choice for the workspace until the app
restarts, the account or connection changes, or Home is reset.

The conversation header's **More** menu groups **Rename**, **Copy conversation**,
**Tasks**, and **Prompts** under **Conversation**. **Settings** contains **Font**
and per-agent **Notifications**; lifecycle actions appear under **Agent** when
available. The footer shows specialist and harness metadata when present; select
the harness version for details. **Move Panel** opens the pane movement submenu.
In companion test builds with direct movement controls, the **Move Panel** arrows
appear directly in panel menus; choose an available arrow to move the panel.
Other panel menus place **Rename** under **Actions** when renaming is available.

The harness details list the agent's features as **On** or **Off**; hover or focus
a feature to read its description. When the agent's harness records it, the details
first show **Starting context**: the approximate size in tokens, then up to three
groups with their summed sizes: **Native** (text the provider loads by itself, such
as repo rules, marked **Not in total**), **User** (text from the user's own files and
settings, such as repo rules and skills) and **Intent** (text Intent writes). Open a
group to list its parts largest first. Skills that were left out of the agent are
counted under **Hidden skills**; open it to see each skill and how to enable it. Agents
on older harnesses, and agents that have not started yet, show only the features.
Sizes are estimates, not provider token counts.

When an agent shows a browser tab while its chat is on screen, the tab opens in the
column next to that chat (right first, then left). It never covers the column you
are working in; if no other neighbor is free, the view splits beside the chat.
Keyboard focus stays where it was. When the chat is not on screen,
Intent picks a panel for the tab without moving focus.

<!-- Sources (paths relative to packages/cloudlands-fe): src/features/layout/tab-types/{AgentTabType,AgentViewSettingsDropdown}.svelte; src/lib/components/chat/HarnessFeaturesModal.svelte; src/features/agent/prompt-breakdown/PromptBreakdownPanel.svelte; src/lib/components/layout/panel-system/PanelTabBar.svelte; src/store/renderer/slices/panel-layout/panel-layout-slice.ts. -->

**Retire Agent** stops an agent and keeps its conversation so it can be restored later.
You can retire any agent, including a workspace's last top-level agent. An agent
cannot retire itself while it is the workspace's last top-level agent. Delegated
agents can always be retired.

Grouped by status, Home sorts workspaces into **Needs you**, **Running**, **Waiting**,
and **Done & idle**, in that priority order. **Running** covers an agent at work, a
running command script such as a build or test, or an agent's background task such as
a shell command it left running; a dev server or other service script, or an open
terminal tab, alone does not count. A mergeable PR stays in **Running** while that
work continues; questions, explicit review requests, blockers, and failures still
take priority. When active work stops, a mergeable PR returns to **Needs you** for
approval, even if a PR monitor is watching it. **Waiting** covers workspaces watching something external: a
monitored PR, a background hook, a delegated agent, a script run monitor, or a PR in
the merge queue (even when another of its PRs is ready to merge). The **Waiting** filter
shows the same workspaces.

The PR badge in list and board views shows a PR in the merge queue first, then
draft PRs, then open PRs (including conflicted PRs), then merged and closed PRs.
Within the same state, it shows the most recently updated PR (for a PR an agent only
watches, its last update on GitHub as of the watch's latest check). Home's shortened PR list
keeps unfinished PRs ahead of merged or closed ones; the **+N** count in list view
includes the other linked PRs, even when their details are not loaded.

<!-- Sources: packages/cloudlands-fe/src/lib/components/workspace/{CompactWorkspaceInitializer,MultiSelectTabbedSidebar,CreateAgentSection}.svelte; packages/cloudlands-fe/src/lib/components/workspace/initializer/{InitialAgentPicker,RepoSelector,BranchSelector,SetupScriptPicker,SetupScriptEditor,StartOnPicker,WorkspaceCreateButton,CreateButtonProgress}.svelte; packages/cloudlands-fe/src/lib/components/modals/SetupScriptModal.svelte; packages/cloudlands-fe/src/store/renderer/slices/workspace-initializer/utils/agent-settings-repo-key.ts; packages/cloudlands-fe/src/features/onboarding/messages/ProjectPickerMessage.svelte; packages/cloudlands-fe/src/features/hardware-console/voice/transcription-service.ts; packages/cloudlands-fe/messages/en.json. -->

<!-- The searchable Machines picker for initial-agent placement is Labs-only;
do not list it among the normal workspace settings below. -->

Open **New workspace** (`/workspace/new`), describe the work in the prompt, and
choose **Create workspace**. During creation, the button shows the current step
and a loader on the right; its progress bar has no numeric percentage. Step labels
slide upward as they change, or switch instantly with reduced motion. Long labels
truncate to fit. The settings beside the prompt (below it on narrow
windows) are **Codebase**, **Branch**, **Setup script**, **Initial agent**, and **Model**.
Use **+** below the prompt to add context, the paperclip to attach files, or the
microphone to dictate. These controls are grouped on the left.
**Transcribing…** stays visible while dictation is transcribed. If transcription
fails, Intent retries up to three times and shows a toast for each retry before
it reports the failure; a missing or rejected provider key fails right away.
These controls open a picker or editor and preserve your prompt draft. **Codebase** lists
your repositories once each, most recently used first, with search at the top;
the selected codebase is included even if it has not reached your history yet.
Codebase icons use owner avatars where available, or folder icons for local paths.
The selected codebase field shows the same icon as its dropdown row.
Use the external-link action beside a codebase to open it in your browser.
**Add codebase** opens the **Clone**, **Existing folder**, and **New** tabs.
If you paste a GitHub URL in codebase search, **Add codebase** carries it into
**Clone**; press Enter to select it. Selecting the current codebase again keeps
your branch selection. If the branch list fails to load, open **Branch** and use
**Refresh branches**.
**Clone** is hidden only once Intent has confirmed no forge account is connected;
to clone, use the connect link or **Settings → Connections**. Onboarding uses
the same tab names. Open the agent picker to choose who starts the workspace;
**None**, last in the list, starts an agent without specialist instructions.
**Create new or manage specialists** closes the workspace form and opens specialist settings.
In **Setup script**, search the list and select the repository's config, a previously
used script, or a template to apply it. Previously used scripts appear first, most
recent first, each under its own name. The selected script is highlighted in the list.
The script and agent lists scroll down to the divider above their fixed actions.
**Edit commands** opens the selected script;
**Create Script** starts a blank draft. Both open a separate **Setup Script** modal:
**Done** or **Save & Done** keeps your edits; **Cancel** discards them and returns
to the workspace form.
For a local repository, use the isolation switch at the bottom of **Branch**.
With isolation on, agents edit a separate copy. Turn it off to edit files in the
existing folder on its current branch, without creating a worktree.
Your prompt draft stays intact. The workspace groups working files, notes, and agent
conversations. Resolve any displayed repository, Git, or provider setup error
before retrying. If creation succeeded but sending the first message failed, use
the form's retry instruction instead of creating a second workspace.
Selecting a repository restores the setup last used to create a workspace there:
the agent, provider, model, and reasoning effort. A repository with no saved setup
uses the most recent setup. Changes you make in the form are kept when you switch
repositories.

<!-- Sources: packages/intentd/crates/intent-services/src/session_import.rs. -->
<!-- Sources (paths relative to packages/cloudlands-fe): src/features/session-import/ImportSessionTab.svelte; src/features/session-import/importable-sessions.client.ts; src/lib/components/workspace/initializer/RepoSelector.svelte; src/features/onboarding/messages/ProjectPickerMessage.svelte; messages/en.json. -->

To continue a session you started in a provider's own CLI, open the
**Codebase** row in **New workspace** and choose **Import a session**. It opens
**Add codebase** on the **Sessions** tab. In onboarding, use the **Sessions**
tab of the **Choose a project** step. The list loads only when that tab opens
and shows sessions from your enabled providers whose adapters support it (such
as Claude Code and Codex), newest first, with search and **Refresh**; a
provider that fails to list shows an error without hiding the others. Intent
keeps each provider's list for 30 seconds, so **Refresh** shows a newer session
only after that. With a very large session history, the list and search may
cover only the newest sessions. Only
sessions on the machine Intent runs on are listed: for a remote daemon,
sessions on your Mac do not appear. A session whose folder has no git
repository shows "No git repository found" and cannot be picked. Picking a
session selects its repository, and its last branch when that branch still
exists locally or on a remote; otherwise the form keeps its default, so check
the **Branch** field before you create. The **Codebase** row shows the session
title with the provider icon. Picking another codebase cancels the
import. The session becomes the workspace's first agent with its history in
the chat, and your next message continues it with the full context. With that
message the agent is told the session moved into Intent and gets Intent's
instructions and tools. Intent copies the session into the workspace (the
original stays untouched); when a provider cannot copy, the original continues
in Intent and a notice says so. Uncommitted changes from where the session ran
are not carried over.


<!-- Sources (paths relative to packages/cloudlands-fe): src/lib/components/chat/proposals/ProposalCard.svelte; src/lib/components/chat/proposals/InlineProposal.svelte; src/lib/components/chat/proposals/open-created-workspace.ts; src/lib/components/chat/proposals/proposal-repo-agent.ts; src/store/renderer/slices/workspace-operations/sagas/workspace-operations-saga.ts; src/store/renderer/slices/tab-state/sagas/proposal-workspace-tab-saga.ts; messages/en.json. -->

An agent can suggest a follow-up workspace as a card in its conversation. Review
it and choose **Create workspace** on the card, or ask the agent to apply it.
Unless the agent named one, the card starts with the agent last used in New
workspace for that repository, and uses that agent's last model and reasoning
effort while it stays selected. The new workspace opens as a tab at the end of
the top tab strip, and the current workspace stays selected. Once created, the
card shows **Open workspace**, whoever applied it; if that workspace was deleted,
a message says it no longer exists.

<!-- Labs (remote agents): describe only when the user asks about remote agents,
and say it is Labs. The card's **Start on** defaults to where the proposing agent
runs: the backend, its remote machine, or a new cloud machine (E2B or Northflank)
from the same entry.
The user can change it before **Create workspace**; an offline machine shows a
warning. An agent applying its own proposal uses that default, and a machine
error is reported back with its reason. Sources: packages/intentd/crates/intent-acp/src/mcp_server/bindings/workspace.rs (proposer_placement, apply_create_failure). -->

<!-- Sources (paths relative to packages/cloudlands-fe): src/features/workspace/utils/workspace-tab-navigation.ts; src/lib/utils/shortcut-bindings.ts; src/lib/components/terminal/QuakeTerminalOverlay.svelte; messages/en.json. -->

Use **Next Workspace Tab** (defaults **Ctrl+Tab** and **Cmd+Shift+]**) or
**Previous Workspace Tab** (defaults **Ctrl+Shift+Tab** and **Cmd+Shift+[**) to
cycle through open workspace tabs in displayed order. On Windows/Linux the
bracket shortcuts are **Ctrl+Shift+]** / **Ctrl+Shift+[**. There, the code
editor keeps them for folding while it has focus; **Ctrl+Tab** still works. In
the terminal drawer with two or more terminals, the bracket shortcuts cycle
drawer terminals instead. From **Home**, next opens the first workspace tab and
previous opens the last, regardless of which workspace was selected before
Home. With no open workspace tabs, these shortcuts do nothing. Customize them in
**Settings → Input and shortcuts → Keyboard shortcuts**.

<!-- Numbered navigation requires the matching frontend release. Until it ships,
describe these bindings only when explicitly asked or testing this change. -->
<!-- Sources (paths relative to packages/cloudlands-fe): src/features/workspace/utils/workspace-tab-navigation.ts; src/shared/keyboard-shortcut-bindings.ts; src/store/renderer/slices/machine-rail/machine-rail-selectors.ts; src/features/machine-rail/MachineRail.svelte. -->

Use **Cmd+Alt+1–9** (**Ctrl+Alt+1–9** on Windows/Linux) to select **Home**
or a workspace tab: **1** opens Home, **2–8** open the first seven workspace
tabs in displayed order, and **9** opens the last workspace tab. When the device
rail is hidden, **Cmd+1–9** (**Ctrl+1–9** on Windows/Linux) also uses this
Home/workspace numbering. User-customized shortcuts take precedence over these
defaults.

<!-- Labs (device rail): describe only when explicitly asked about the device rail
or when developing or testing it, and say it is Labs. With the rail visible,
Cmd+1–9 (Ctrl+1–9 on Windows/Linux) selects devices in displayed rail order:
1–8 select the first eight devices and 9 selects the last. Own devices come before
guest hosts, as displayed. Cmd+Alt+1–9 (Ctrl+Alt+1–9 on Windows/Linux) continues
to select Home/workspace tabs using the numbering above whether the rail is visible
or hidden. User-customized shortcuts take precedence. -->

<!-- Sources: packages/cloudlands-fe/src/lib/components/chat/AgentCard.svelte (toggle-pinned menu item); packages/cloudlands-fe/src/lib/components/workspace/{WorkspaceAgentsList.svelte,workspace-agents-list-utils.ts}; packages/intentd/crates/intent-services/src/agent_ops.rs (agent.update pinned). -->

To keep an agent at the top of a workspace's agent list, right-click it in the
sidebar and choose **Pin to top**. Pinned agents are listed first, most recent
activity first, with a divider below them; choose **Unpin** to return an agent to
the normal order. Pins are saved by Intent, so they appear on every device
connected to the same Intent. Only foreground agents that were not started by
another agent can be pinned.

<!-- Sources: packages/cloudlands-fe/src/features/layout/tab-types/AgentViewSettingsDropdown.svelte; packages/cloudlands-fe/src/lib/components/chat/InlinePermissionRequest.svelte; packages/intentd/crates/intent-acp/src/permission.rs (option kind/scope); packages/intentd/crates/intent-services/src/agent_ops.rs (agent.update permissionPolicy); packages/intentd/crates/intent-services/src/agent_manager.rs (permission_policy_for, respawn on change); packages/intentd/crates/intent-services/src/settings.rs (agents.permissionPolicy); packages/intentd/crates/intentd/src/main.rs (INTENTD_PERMISSION_POLICY override); packages/intentd/crates/intent-acp/src/spawn.rs (Codex INITIAL_AGENT_MODE); packages/cloudlands-fe/src/routes/(app)/settings/+page.svelte (Agent defaults). -->

By default agents use tools without asking. To approve each tool use in one chat,
open the chat's settings menu and turn on **Ask before using tools**. The agent then
pauses at each approval and shows **Allow** / **Deny** in the conversation; an
unanswered request is cancelled after 5 minutes. When the agent offers an **Always allow**
choice, a hint under it says how far it reaches: **This chat only**, **This workspace
only** (for example Claude's "don't ask again" rules, saved in the workspace) or **All
workspaces** (saved in the agent's own settings, for example Cursor's "Allow always" or
Codex's command rules). No hint means Intent does not know how far it reaches. After that the agent stops asking for that action, even with
**Ask before using tools** on. The change applies from the next
message, and a turn already running keeps its setting. Agents this chat starts, and
agents on remote machines, do not use this setting. Codex works differently: with
this setting on, it edits files in the workspace freely and asks only before writing
outside the workspace or using the network.

To change the default for every chat, open **Settings → Agent defaults → Tool
approvals**: **Ask each time**, **Allow all** (the default), **Allow reads only**, or
**Deny all**. A chat's own setting wins. If the backend was started with the
`INTENTD_PERMISSION_POLICY` environment variable, that value replaces this default.
An Assistant settings card cannot change this setting.

<!-- Sources: packages/cloudlands-fe/src/lib/components/chat/MonitoredPrsRow.svelte; packages/intentd/crates/intent-services/src/pr_monitor.rs. -->

When an agent monitors a GitHub pull request, its conversation shows the monitored
PR and its latest status. Required checks that are still running or failing can
block merging. If monitoring is paused or information is unavailable, wait for a
fresh status before treating the PR as ready.

<!-- Sources: packages/cloudlands-fe/src/lib/components/chat/{AgentWorkersRow,EventSubscriptionsCard}.svelte; packages/cloudlands-fe/src/features/agent-workers/agent-workers-channel.ts; packages/cloudlands-fe/src/store/renderer/slices/agent-workers/{agent-workers-selectors.ts,sagas/agent-workers-saga.ts} (connection-drop demotion); packages/cloudlands-fe/messages/en.json; packages/intentd/crates/intent-services/src/{agent_workers,agent_manager,agent_ops}.rs (restart capture and resume wording); packages/intentd/crates/intent-core/src/config.rs (DEFAULT_IDLE_REAP_MINUTES). -->

When a Claude Code agent starts work in the background, such as a dev server
command, its conversation shows a row for it marked **Running**. When the
conversation has several events, they are grouped under a **Subscribed to N
events** header that may need expanding. Expanding the row shows the task type
and when Intent first saw it. If the row has a **…** menu, **Stop** ends that
task without interrupting the agent; the row shows **Stopping…** and disappears
once the agent reports it stopped. Rows that end on their own disappear too.
**Connection lost** means Intent can no longer observe the task, so whether it
is still running is unknown; it is not proof that the task stopped. After the
app reconnects to Intent, the row returns to **Running** if fresh status shows
the task still running. If it stays Connection lost, the agent's connection
ended and the task has probably ended with it. An idle agent stays open while it
has a running background task: Intent closes it only to free a process slot or
memory, or once it has been idle about a day past **Settings → Advanced → Agent
backend → Idle reap minutes** (10 by default; Off disables it). Agents without
running tasks close after Idle reap minutes. Before telling the user it stopped,
confirm independently, for example by checking whether the dev server still
responds. Such a row has no Stop and clears the next time the agent starts.
Restarting Intent stops running background tasks. The agent is then listed with
interrupted agents, and when resumed it is told its tasks were stopped and will
not report back, so it can restart any it still needs.
After an Intent update, interrupted agents resume on their own, except agents
whose provider is turned off or not signed in: those stay listed with interrupted
agents until the user signs in and resumes them.
Codex and other providers do not report background tasks.

<!-- Sources: packages/intentd/crates/intent-services/src/{provider_images,agent_manager,agent_session,agent_ops}.rs. -->

PNG, JPEG, GIF, and WebP attachments may be resized or re-encoded for provider
delivery; original attachments and their displayed previews are unchanged. SVG and
other image formats are sent as files the agent can retrieve and read, rather than
as vision input. To have the agent inspect their appearance, export a PNG or JPEG.
If a supported image cannot be read
or the images together exceed the delivery budget, the turn fails rather than
silently omitting an image. Re-export an unreadable image as PNG/JPEG, or send fewer
images cropped to the relevant detail. A provider request-size error can also come
from images retained in earlier turns: start a new agent conversation with only
the images needed if sending fewer images still fails.

<!-- Sources: packages/intentd/crates/intent-services/src/lib.rs (SETUP_TERMINAL_NAME); packages/intentd/crates/intent-pty/src/host.rs. -->

Workspace setup runs in the **Setup Script** terminal and can finish without that
terminal being open. If setup stays running, inspect its last output before
retrying. Include that output, the Intent version, and the operating system when
reporting the problem.

<!-- Sources (paths relative to packages/cloudlands-fe): src/lib/components/terminal/QuakeTerminalOverlay.svelte; src/features/layout/tab-types/TerminalTabType.svelte; src/lib/components/workspace/WorkspaceShellList.svelte; src/features/scripts/confirm-script-deletion.ts; src/store/renderer/slices/scripts/scripts-selectors.ts; src/lib/components/chat/EventSubscriptionsCard.svelte; messages/en.json. Backend sources: packages/intentd/crates/intent-svc-runtime/src/script_monitor.rs; packages/intentd/crates/intentd/src/main.rs (staged update restart watcher). -->

Script lists show unarchived scripts; use **Load more** when offered to see the
next page. Open a script to view available output; use **Edit command** to inspect
or edit its full definition. Output from each script's latest run is saved and
stays available after a restart or archive. Saved output is cleared 7 days (by
default) after it was last written, when you delete the script, or when you delete
the workspace, which also stops its scripts. Output of a script that is still running
is never cleared by age. Very long runs keep only their most recent output. Script watches show
only active monitors and disappear when they finish, trigger, expire, or are
cancelled; completion notices remain in the agent conversation. An automatic
background update waits for running one-off (command) scripts to finish, for up
to 90 minutes, before restarting; long-running service scripts do not delay it.

To delete a saved script, open its **…** menu in the sidebar **Scripts** list or in
the script panel and choose **Delete script**, or use **Delete script** beside its
play/edit controls in the bottom bar. Each asks **Delete “[name]”?**; confirm with
**Delete script**, or cancel to keep it. A running script is stopped first, and
any agents monitoring it are woken; the dialog says so. Deletion stays disabled while
changes are pending.

To continue work, open its existing card or sidebar entry, then the existing agent's
conversation. Read its status and last response before sending a follow-up. Use
**Create new agent** in that workspace only when a separate conversation is needed;
choose a specialist and model where offered. A specialist defines reusable behavior;
an agent is a particular conversation doing work.

<!-- Sources: packages/cloudlands-fe/src/lib/components/chat/questions/QuestionWizard.svelte. -->

When answering agent questions above the composer, paste images into **Or type your own answer…** to attach removable previews and send them with your answers, with or without text.

In an empty composer, press **Up Arrow** to recall a previous prompt. Recalled
prompts use the visible message text, without hidden sender or delivery metadata.
At the start of the text, **Up Arrow** moves to an older prompt; at the end,
**Down Arrow** moves forward and restores your draft. If an editable queued
message is present, **Up Arrow** in an empty composer edits that message first.

<!-- Sources (paths relative to packages/cloudlands-fe): src/lib/components/chat/ChatPanel.svelte; src/lib/components/chat/input/TipTapEditor.svelte; src/lib/utils/user-message-presentation.ts. -->

On desktop, recent conversation messages appear progressively, newest first, while
older messages in the initial window load. The conversation stays bottom-aligned
unless you scroll away; older-history loading becomes available when that initial
window finishes. Older clients may show the initial messages together.

<!-- Sources: packages/cloudlands-fe/src/lib/components/chat/ChatPanel.svelte; packages/intentd/crates/intent-transport/src/{conn,subscriptions}.rs. -->

<!-- Sources (paths relative to packages/cloudlands-fe): src/features/home/HomeAssistantThreads.svelte; src/lib/components/layout/sidebar-nav/cards/{ChiefCard.svelte,chief-thread-starter.svelte.ts}; src/lib/components/chat/{AssistantThreadTitle.svelte,agent-pin-menu.ts}; src/store/renderer/slices/agent-session/sagas/agent-mutation-saga.ts. Daemon: agent.update `pinned`, agent.retire, agent.restore, agent.delete. -->

In **Home → Assistant**, click **New chat** at the top of the sidebar to start a
chat; a blank chat is reused rather than duplicated. Select a thread in the
sidebar to open it. To rename it, click its title in the conversation header
and edit it inline. The title field uses the available header width. Press
**Enter** or click away to save; press **Escape** to cancel. Sidebar titles select conversations and
cannot be edited there.

Hover a chat in the sidebar to show its delete button, or right-click it for **Pin
to top**, **Archive**, and **Delete**. Pinned chats stay at the top. **Archive**
keeps the chat and moves it to the collapsible **Archived** group below the list;
open an archived chat to read it, or right-click it to **Unarchive** or **Delete**
it. Cmd-click (Ctrl-click on Windows and Linux) or Shift-click chats to select
several, then choose **Archive** or **Delete** in the bar above the list or in the
right-click menu; one **Undo** restores every deleted chat. Press **Escape** to
clear the selection.

<!-- Sources (paths relative to packages/cloudlands-fe): src/features/home/{HomeIntegrations,HomePullSummary,HomePullCode,HomeWorkspacePullBadge}.svelte; src/features/home/home-workspace-opening.ts; messages/en.json. -->

In **Home → Workspaces**, PR badges update as Intent observes changes.
**Queued** means the PR is in the merge queue, including while queue checks
are running. You do not need to open the workspace or hover over the PR to
receive these updates.

<!-- Sources (paths relative to packages/cloudlands-fe): src/lib/components/ui/tooltip/GitHubLinkCard.svelte; src/lib/components/chat/MonitoredPrsRow.svelte; src/lib/components/workspace/sidebar/sidebar-changes-utils.ts; src/lib/utils/merge-queue.ts. -->

For a queued GitHub PR, the PR link hover card, the chat **Monitoring PR** row,
and the sidebar PR tooltip also show its place in the queue (for example,
**2nd of 3**) and GitHub's estimated time to merge when GitHub provides one.
Home badges show only **Queued**. Agents see the position when they request a
PR snapshot; queue movement alone does not wake them.

In **Home → Pull requests**, select a PR to open its preview. If the PR is linked
to a workspace, double-click the row to open that workspace directly. **Summary** shows
reviews and check status counts; expand the overview to inspect individual checks in
open status groups. Collapse a group to hide its checks. **Code** lists changed
files with their directory paths. Search by filename or path, then click a file to
expand its diff in place. Multiple files
can stay open, and the preview scrolls through them together. If a patch is
unavailable, use **Open file on GitHub** when offered or **Review on GitHub**.

<!-- Sources (paths relative to packages/cloudlands-fe): src/features/navigation/{LinkActionMenu.svelte,link-action-menu-state.svelte.ts}; messages/en.json. Daemon: workspace.findByPullRequest, packages/intentd/crates/intent-store/migrations/0161_pull_request_workspace.sql. -->

Clicking a GitHub pull request link opens the link menu (when **GitHub link
behavior** is **Show choices**, the default). If another workspace worked on that
PR, the last menu item is **Open in original workspace**, which opens it; with
several such workspaces it opens a submenu of their names, most recent first.
Archived workspaces are included, and the submenu marks them **Archived**. Intent
links a PR to a workspace when it discovers the PR on that workspace's branch or
one of its submodules; PRs seen only before this feature shipped are not linked
until Intent sees them again.

## Rename a workspace

<!-- Sources: packages/cloudlands-fe/src/lib/components/workspace/WorkspaceSidebarHeader.svelte; packages/intentd/crates/intent-acp/src/mcp_server/bindings/workspace.rs (set_title, userRequested); packages/intentd/crates/intent-services/resources/agent-instructions/v3.13/workspace.md. -->

The workspace agent names a new workspace on its first turn and does not change an
existing name on its own. To rename one, click its title at the top of the workspace
sidebar and edit it, or ask the agent in chat (for example "Rename this workspace to
Docs refresh"). Agents created before this change may decline to rename a named
workspace; ask a new agent or edit the title yourself.

## Home Workspaces

<!-- Sources (paths relative to packages/cloudlands-fe): src/features/home/{HomePage,HomeWorkspaceSettings,HomeWorkspaceBoard,HomeSelectionBar}.svelte; src/lib/components/modals/BulkTransferModal.svelte; src/store/renderer/slices/workspace-bulk-transfer/; src/features/home/{home-workspace-opening,home-multi-select,home-workspace-status-groups,home-workspaces-persistence,home-workspace-menu,home-workspace-labels,home-hidden-repos}.ts; src/lib/components/modals/WorkspaceWarningDialogs.svelte. Daemon: workspace.update `label`, packages/intentd/crates/intent-core/src/model.rs (normalize_workspace_label). -->

In **Home → Workspaces**, click a list row or board card to select it and show its
preview. Double-click it, or press Cmd+Enter (Ctrl+Enter on Windows and Linux) on a
focused row or card, to open the workspace directly.

To select several workspaces, Cmd-click (Ctrl-click on Windows and Linux) each one.
Shift-click selects every workspace between the last workspace you plain-clicked or
Cmd/Ctrl-clicked and the one you Shift-click; further Shift-clicks keep that starting
point (click A, Shift-click C, then Shift-click E selects A to E). With nothing
selected, Shift-click selects just that workspace.
A bar at the bottom shows the count with **Archive**, **Transfer…**, **Purge folders**
and **Delete**; right-clicking a selected workspace offers the same actions. Each
action applies only to the selected workspaces it fits (Archive to active ones,
Transfer to ones that still have a git folder, including archived ones, Purge folders
to archived ones that still have a folder, all four to workspaces you own) and asks
for confirmation first.
**Transfer…** sends the selection to another connected machine. Before it starts, pick
the destination, review each workspace's estimated size and the estimated total, and
choose whether to restart in-flight agents on the destination and whether to archive
the transferred workspaces here (on by default). One progress bar covers the whole
run; workspaces go one at a time, a failed one does not stop the rest, and **Stop**
skips the remaining ones. The result lists each workspace as transferred, failed (with
the reason) or not transferred. Workspaces that were not transferred stay usable here.
**Stop** is unavailable once the destination starts importing, and if a failed or
stopped workspace already reached the destination, its row says so. Bulk transfer does
not download to a file.
Press Escape, choose **Clear selection**, or click one workspace to end the selection;
changing the filter, repository or tab also clears it.

Right-click a repository or local folder in the Home sidebar and choose **New
workspace** to start one there, or **Hide** to remove it from the sidebar. A hidden
repository's workspaces still show under **All repos**. Hidden repositories move to a
collapsed **Hidden** section at the bottom of the sidebar; right-click one there and
choose **Unhide** to bring it back. A hidden repository also comes back when a new
workspace is created for it. Hidden repositories are remembered with Home's settings.

Open **View options → Group by** in either list or board
view. **Activity** keeps the broad **Needs you**, **Running**, and **Done & idle**
groups. In list view, **Status** groups workspace statuses in this order:
**Needs attention** (failed, blocked, or needing attention), **Not started**,
**In progress**, **PR open**, **PR mergeable**, **PR queued**, **PR merged**, then
**Done & idle** (idle or complete).
**Repository** groups by repository; **None** removes grouping.

The board uses **Activity** while the list is grouped by **Status**. Switching back
to the list restores its detailed groups.

Right-click a workspace and choose **Pin** or **Unpin**, or open **Label** to give
it a label. The **Label** submenu lists the labels already in use (A–Z), **New
label…** to name a new one (1–64 characters, not a duplicate and not "Pinned"), and
**Remove label** when the workspace has one. Each workspace has one label; Pinned is
the built-in label, so labeling a pinned workspace unpins it and pinning a labeled
workspace removes its label. A label disappears once no workspace holds it. Labels
are saved by the daemon, so they sync to every client connected to it. Only the
workspace owner sees **Pin**, **Unpin** and **Label**; shared members cannot change them.

In the list, pinned workspaces come first with a pin icon, then one collapsible
group per label (A–Z), then the usual groups without those workspaces. Grouped by
**None**, and on the board, pinned workspaces show a pin icon and stay ahead of the
rest without label groups; **Done & idle** shows newest activity first within the
pinned and unpinned groups.

The grouping choice is remembered with Home's settings for the current signed-in
user and connected backend. **Activity** remains the default; existing saved
grouping choices keep their behavior.

## Set aside workspaces

<!-- Sources (paths relative to packages/cloudlands-fe): src/features/home/{HomePage,HomeWorkspaceList,HomeWorkspaceBoard}.svelte; src/features/home/{home-workspace-menu,home-workspaces-persistence,home-workspaces-saga,home-set-aside-shortcut}.ts. -->

**Set aside** is available in alpha builds starting with v2.262.0. Use **Set aside**
on a Home workspace row, card or menu. List view moves it into a collapsed
**Set aside** section; board view shows it in the **Set aside** column on the right.
The column stays hidden when empty and appears while dragging a card so you can
drop it there to set it aside. Choose **Bring back** on its row or card, or use the
immediate **Undo** action. Searching includes set-aside workspaces. With a Home
row or card focused, **Cmd/Ctrl+Shift+S** toggles its state; customize the shortcut
in **Settings → Input**.

Setting aside keeps agents, scripts, hooks and PR monitors running. Reading its
history keeps it aside; a new instruction or a new question, blocker, failure or
review request brings it back. Ordinary background updates do not. This is personal
Home organization, saved in this app for the current account and daemon; it does
not hide the workspace for collaborators or mark its work complete.

## Archive, auto-archive and disk cleanup

<!-- Sources (paths relative to packages/cloudlands-fe): src/features/home/{home-workspace-menu.ts,HomeWorkspaceControls.svelte}; src/lib/components/modals/RetireWorkspaceConfirmDialog.svelte; src/lib/components/settings/GitWorkspaceSettings.svelte; messages/en.json. Daemon: workspace.archive, workspace.retire, workspace.update `autoArchiveDisabled`, packages/intentd/crates/intent-services/src/{archive_script,auto_archive,retire,auto_retire,local_work}.rs. -->

In **Home → Workspaces**, right-click a workspace and choose **Archive**. To archive,
purge or delete several at once, Cmd-click (Ctrl-click) them first (see
**Home Workspaces**).
Archiving stops its agents, hooks and PR monitors and keeps its folder. Archived
workspaces appear under the **Archived** filter. Choose **Unarchive**, or send an
agent a message, to continue.

If the repository's `.intent/config.json` sets `archiveScript`, Intent runs it once
in the workspace folder when the workspace stops being active: when it is archived,
or when an active workspace is deleted or purged (before the folder is removed).
Deleting or purging an archived workspace does not run it again. Unarchiving waits
for a script that is still running, and does not rerun the setup script; instead, each agent's first message after unarchiving
tells it the archive script ran and to check `.intent/config.json` if the workspace
needs to be set up again. If the script fails or runs longer than 10 minutes, the
workspace is still archived and its card's status shows the failure.

Intent can archive idle workspaces automatically. It is off by default; turn on
**Workspace setup → Archive idle workspaces** (`/settings?tab=setup#git-workspace`)
and set the number of days (7 when first turned on), or set `autoArchiveIdleAfterDays`
(`0` = off) under `[workspace]` in `config.toml`. A workspace with no messages, note
edits or agent activity for that long is archived, unless an agent, script, hook or
PR monitor is still running, it is shared with guests, or it has uncommitted changes
or unpushed commits. Commits from a pull request that Intent saw merged no longer
count as unpushed, even after the merged branch is deleted on the remote; commits
made after the merge still do.

To keep one workspace from being archived automatically, right-click it in **Home →
Workspaces** and turn off **Auto-archive**; turn it back on the same way. It is
on for every workspace by default, only the workspace owner sees it, it appears only
while **Archive idle workspaces** is on, and it does not stop you archiving the
workspace yourself.

To free disk space, right-click an archived workspace and choose **Purge workspace
folder**, then confirm. This deletes the folder, including uncommitted changes and
untracked files, but keeps conversations, notes, tasks and the git branch. The
workspace card's status then reads "Workspace folder purged to free disk space.",
and under the **Archived** filter its row and board card show a **Workspace purged**
pill.
Remote workspaces and workspaces opened on your own folder cannot be purged. A
workspace whose folder is its own copy of the repository (CoW or Direct) can be
purged only when nothing in it is missing from a remote: no uncommitted or untracked
files, no stash, and no commits on any branch, submodule or nested repository that
were not pushed (commits Intent saw merged in a pull request count as pushed).
To purge automatically, turn on **Workspace setup → Free disk space for
archived workspaces** and set the number of days (7 when first turned on), or set
`purgeArchivedAfterDays` in `config.toml`. It is off by default, runs about once an
hour, and skips workspaces with uncommitted changes or with unpushed commits in a
nested repository (such as a submodule), unless Intent saw those commits merged in a
pull request; CoW and Direct workspaces get the stricter check above.

A purged workspace still shows as archived and its history stays readable.
Unarchiving it, or sending a message, recreates it from a fresh checkout of the
default branch on a new branch; the old branch still holds the earlier work (for a
CoW or Direct workspace, on the remote), and the agents are told the folder was
purged. A CoW or Direct workspace is recreated from Intent's repository cache; if
the cache is gone, create a new workspace instead.

## Home workspace status icons

<!-- Sources (paths relative to packages/cloudlands-fe): src/features/home/{HomeWorkspaceStatus.svelte,home-attention.ts,home-model.ts}; src/lib/components/workspace/utils/workspace-status-presentation.ts; messages/en.json. Backend source: packages/intentd/crates/intent-services/src/workspace_status.rs (workspace_is_waiting). -->

In **Home → Workspaces**, the status icon in list and board views describes the
workspace's current status. Hover over it for an explanation:

- **PR Open** (pull request icon): a pull request is open.
- **PR Mergeable** (pull request icon): a pull request is reported ready to merge.
- **PR Queued** (hourglass): a pull request is in the merge queue; it has not merged yet.

Colors follow the current theme: Open and Queued use its informational color;
Mergeable uses its success color. Use the label and tooltip to identify the state.

Questions, review requests, failures, and blockers can take precedence over PR
icons. **PR Mergeable** does not grant permission to merge: agents still need the
user's explicit authorization and must check the current merge requirements.

A muted hourglass indicates waiting on external events or delegated work in the
workspace; it does not mean every agent is waiting. It replaces an otherwise idle
dot or appears beside a PR, merged, or complete status. **PR Queued** keeps one
hourglass, with additional waiting explained in its tooltip. Waiting is also
included in accessible status text. The waiting indicator is hidden when the main
status shows active work, unread updates, a question, a review request, a failure,
or a blocker.

## Workspace numbers for Micro keys

<!-- Sources (paths relative to packages/cloudlands-fe): src/features/home/{HomePage,HomeWorkspaceBoard}.svelte; src/lib/components/layout/WorkspaceTabStrip.svelte; src/features/hardware-console/device/{connection-status,supported-devices}.ts; src/features/hardware-console/assignment/{key-assignment,key-pin-persistence-service,key-switch-service,workspace-key-menu}.ts; src/features/hardware-console/components/WorkspaceMicroKeySlot.svelte; src/features/hardware-console/prompt-picker/{radial-layout,prompt-picker-service}.ts; src/features/hardware-console/prompt-picker/RadialPromptPickerOverlay.svelte; messages/en.json. -->

With a supported **Creator Micro 2** or **Codex Micro** connected to Intent,
**Home → Workspaces** shows colored numbered squares on assigned workspaces in
both list and board views. Matching smaller squares appear at the left of top
workspace tabs, including pinned tabs. Numbers **1–6** identify the workspace's
Micro key assignment. An unassigned workspace has no number.

Right-click a workspace's list row, board card, or top tab and choose **Assign to
Micro Key**, then a key number. This also works for workspaces without a number;
the workspace does not need to be selected. Archived, deleted, and Assistant
workspaces cannot receive Micro assignments.
An occupied key's menu label names the workspace it will replace. Choose
**Unassign** to remove the current assignment. Changes are shared across these
views and saved.

The numbers and assignment actions appear only while Intent's Micro integration
is connected; a device merely being plugged in or detected is not enough.
Disconnecting hides them without deleting saved assignments. If they are missing,
check the device's connection to Intent before trying to change assignments.

Pressing keys **1–6** switches to the assigned workspace. When the press lands on
an agent tab, its chat box is focused so you can type right away.

Push the joystick to open the prompt ring. Up to three suggested prompts from the
focused chat appear at top, top-right, and right, numbered and tinted purple. Your
most-used prompts fill the other directions, and **Cancel** is at the bottom.
Release on a suggestion to send it, or on a used prompt to insert it into the
focused text box. **Settings → Hardware / Creator Micro → Prompt picker limit**
sets how many used prompts appear: up to 7, fewer when chat suggestions take the
top directions.

## Inspect a failed response and recover

<!-- Sources (paths relative to packages/cloudlands-fe): src/features/agent/agent-attention-toast-service.ts; src/store/renderer/slices/agent-session/sagas/agent-failure-toast-saga.ts; src/features/notifications/main/notification.service.ts. -->

While the desktop app is in front, an agent finishing work shows an in-app
notification naming the agent and its workspace instead of a system notification;
choose **Switch To** to open that agent's conversation. For an Assistant chat, it
is skipped while that chat is already open in the Assistant panel.

**Switch To** notifications for agent failures, attention requests, finished agents,
and automatic workspace unarchiving are skipped when that workspace is already
selected, even if another conversation, file, or note is open. Notifications for
other workspaces can still appear. The workspace's agent status and conversation
notices remain available.

<!-- Sources (paths relative to packages/cloudlands-fe): src/lib/components/chat/{StreamingStatus,TurnFailureNotice,FailureDetails,QueuedMessageList,QueuedMessageActions,ChatPanel}.svelte; messages/en.json. -->

In the existing conversation, **Couldn't complete this response** summarizes a
stopped response. Open **Details** to inspect the technical error, then **Copy
details** to copy it. **Needs attention** means action is needed; choose **Retry**
when offered. **Queued** means a message is in the queue, not that a retry is
scheduled. Inspect that message and use its available controls before sending
another copy. Active work keeps its normal **Thinking** or activity status; do not
infer a retry from that status alone. Controls depend on the conversation's state
and your access.

For one queued message, use **Edit**, **Send now**, or **Remove** in the queue
header. While editing, **Save** and **Cancel** stay beside the message. With
several queued messages, each message has its own controls; the header offers bulk actions.

Earlier failures may appear as **1 recorded failure** or **N recorded failures**.
Expand that label to inspect the saved errors and timestamps and use **Copy
details**. These are counts of recorded notices, not total attempts or proof that
all failures belong to the same request. A historical notice alone does not mean
the agent is still failing; check the current status and latest response.

<!-- Sources: packages/intentd/crates/intent-services/src/agent_ops.rs (agent_resolve_blocker_op, clear_daemon_stall_blocker_after_success); packages/cloudlands-fe/src/shared/utils/agent-attention.ts. -->

A current blocker warning means an agent reported a problem that prevents work.
After confirming recovery, the agent can clear its warning while a release or
scheduled check is still pending. A blocker Intent raised because the provider
stalled clears on its own after the agent's next successful turn. A recorded
blocker notice can remain in the conversation after the current warning clears.

<!-- Sources: packages/intentd/crates/intent-services/src/{agent_session.rs,provider_auth.rs,provider_models/parse.rs}; packages/intentd/crates/intent-providers/src/config.rs; packages/cloudlands-fe/src/lib/components/chat/StreamingStatus.svelte. -->

Follow specific recovery guidance when shown: **Retry with [model]** uses the
offered available model; **Retry on [provider]** switches away from a provider
whose usage limit was reached, and **Retry** lets you try the same provider
again after its limit resets. For sign-in errors, including an expired Claude
OAuth login, run the displayed CLI login command on the host running the agent,
complete sign-in, then retry. Signing in to the Claude desktop app does not sign
in its CLI. For **Agent session corrupted**, retry starts a fresh session and
carries over the conversation history. Do not promise a model or provider switch
unless the UI offers it; inspect **Settings → Providers** for setup problems.

<!-- Sources: packages/intentd/crates/intent-services/src/provider_auth.rs; packages/intentd/crates/intent-core/src/{path_utils.rs,cli_env.rs}. -->

Claude authentication checks also use exported credentials from the execution
host's login shell, including `CLAUDE_CODE_OAUTH_TOKEN`. Restart that host's
daemon after changing shell exports so it reloads them; an existing daemon
environment value takes precedence. Never paste tokens into chat.

<!-- Sources: packages/intentd/crates/intent-services/src/{pi_cli.rs,agent_manager.rs,pi_mcp_wrapper.cmd}; packages/intentd/crates/intent-providers/src/config.rs. -->

Pi requires the installed Pi CLI version shown in **Providers**. Intent supplies
its workspace tools automatically and keeps user-installed Pi extensions enabled.
If Pi works in a terminal but fails in Intent, copy the conversation's failure
details and check the CLI found by **Providers**. A Windows error saying Pi MCP
delivery requires a Unix host comes from an older Intent build; changing the
workspace setup script does not resolve it.

<!-- Sources (paths relative to packages/cloudlands-fe): src/features/providers/components/ProviderCliVersionToast.svelte; src/features/providers/utils/provider-cli-warnings.ts; src/store/renderer/slices/host-requirements/sagas/provider-cli-requirements-saga.ts. Daemon: packages/intentd/crates/intent-services/src/{bundled_cli.rs,installed_cli.rs,provider_cli.rs}. -->

When the daemon host's Codex or Claude Code CLI is older than Intent requires,
Intent runs its own bundled copy of that CLI instead, downloading it with npm on
first use. Sessions keep using the installed CLI's sign-in and settings. The host
owner sees a warning that Intent is using its bundled CLI (older apps show
**CLI is out of date**). Updating the installed CLI makes Intent switch back to
it. **Update** runs `codex update` or `claude update` in a terminal on the
daemon host; follow its output until it finishes. If the button is missing or
the command fails or prints another upgrade command, update the CLI with the
tool that installed it. If the bundled download
fails (for example, offline or without npm), Codex launches report the
installed version, the required minimum, and the download error; Claude Code
keeps using the installed CLI.

## Restore workspace browser use

<!-- Sources (paths relative to packages/cloudlands-fe): src/lib/components/workspace/{SetPrimaryClientConfirmDialog.svelte,driving-indicator.ts}; src/lib/components/workspace/sidebar/WorkspaceProgressCard.svelte; src/lib/components/browser/{BrowserStatusControl,BrowserViewerTabHeader,EmbeddedBrowser}.svelte; src/lib/components/layout/panel-system/PanelTabBar.svelte; src/features/layout/tab-types/BrowserTabType.svelte; src/features/events/daemon-events-bridge.client.ts; src/store/renderer/slices/browser-clients/{browser-clients-selectors,browser-clients-slice}.ts; src/store/renderer/slices/browser-clients/sagas/browser-clients-saga.ts; messages/en.json. -->

The workspace sidebar's menu shows **Primary client**: the client that runs this
workspace's agent browser tabs and tunnels. When that client is offline, the menu
button shows a red dot and **Primary client** reads **Offline**, even if the
workspace has no browser tabs yet: agent browser tabs and tunnels fail until that
client reconnects or another connected, browser-capable client is set as primary.
It also reads **Offline** when no primary is set but the workspace's agent browser
tabs or desktop session are on a client that is not connected or cannot run agent
browser tabs; choosing **This app** moves the agent-owned tabs here.
With no primary set and no agent tabs yet, a new agent browser tab opens in the
connected app where you last viewed the workspace, else in the app that connected first.

To switch, open the workspace in the client you want to use, open the workspace
sidebar's menu, choose **Primary client** → **This app**, and confirm **Set as
Primary**. This moves agent-owned tabs to that client without preserving page
state; tabs you opened yourself stay where they are. **This app** depends on the
current client's connection, browser capability and your access; when a guest role or
missing browser support blocks it, a line below it says why. A checkmark marks
the current primary client; it does not guarantee browser availability. The
selection updates when the primary changes; after reconnecting, wait for the
current client and workspace browser state to refresh. Do not promise automatic
failover or change the primary client without the user's instruction.

An agent-owned browser tab shows the controlling agent's avatar in the panel
header; hover or focus it for **Controlled by** and the agent's name, or click it
to open that agent's conversation. Host, offline, and page-error controls also
use icons with tooltips at every panel width. When a tab runs on another computer,
the host icon beside the address names that computer in its tooltip; click it
for host details. For an agent-owned tab, these details explain that cookies
and page state stay on the other computer, so the page may differ from what the
agent sees and its address can change on its own. The offline warning pauses navigation
until that computer reconnects; click the status for details and **Close anyway**.
Page-error icons appear beside Reload; click one for the message and **Retry**.
If the page closed itself, its empty page shows the message and **Retry** directly.

## iOS workspace browser

<!-- Sources: packages/ios/Intent/Views/Workspaces/{WorkspaceBrowserView,WorkspaceBrowserPage}.swift; packages/ios/Intent/Services/BrowserTunnelURL.swift; packages/ios/Intent/Info.plist. -->

In the iOS workspace browser, enter an explicit `http://` or `https://` address
to open a server reachable from the device, including LAN and Tailscale hosts.
Allow Intent's local network access if prompted; if previously denied, enable
it in iOS Settings under **Privacy & Security → Local Network**. Tailscale
addresses require a working route from the device. `daemon.localhost` and
loopback addresses instead open servers on the connected daemon through its
authenticated tunnel.

## Add context, run a task, and find results

<!-- Sources (paths relative to packages/cloudlands-fe): src/lib/components/workspace/{WorkspaceLayout,MultiSelectTabbedSidebar,WorkspaceAgentsList,WorkspaceShellList,NoteMetadataBar}.svelte; src/lib/components/workspace/sidebar/{SidebarSectionTabs,WorkspaceSidebarRail,WorkspaceProgressCard,WorkspaceTokenUsage,WorkspaceStatusImage,AddContextSection,ContextPanel,ContextItemRow,NotesPanel,ExpandableFileSearch,SidebarHeaderAction,SidebarActionIcon,ChangesRefreshAction,SidebarPrDropdown}.svelte; src/features/external-editors/components/OpenComboButton.svelte; src/lib/components/layout/panel-system/PanelTabBar.svelte; src/lib/components/icons/DisclosureIcon.svelte; src/store/renderer/slices/sidebar-nav/sagas/sidebar-section-navigation-saga.ts; src/lib/utils/shortcut-bindings.ts; messages/en.json. -->

Use the icon tabs above the expanded sidebar's content panel to switch sections.
They share available width up to a compact limit, then stay grouped at the left.
Hover over an icon for its section name; scroll the row horizontally when space
is tight. Faded edges show where more tabs are hidden and clear when you reach
that end. **Agents** opens by default; your selected section is remembered. Its
tab shows the main agent's avatar, or a robot while no main agent is available.
**People** follows **Agents** in both the expanded row and collapsed rail.
A token-usage total appears beside **Agents** once tokens have been used. It
excludes cached tokens; its details list cached context as a separate row.
The workspace status image appears as a compact thumbnail in **Context**; click
it to view the full image.
When collapsed, the sidebar leaves a slim vertical icon rail. Hover over or focus
a section tab to reveal just its content panel beside it, aligned with the tab
when space allows. The panel fits its contents; longer sections scroll within the
available space. Moving the pointer between tabs smoothly switches the preview
without changing your remembered section. In the expanded sidebar, selecting a
different section leaves your open content in place. **Shift-click** a section
tab to add or remove its pane; selected panes stack in sidebar order, and at least
one stays open. Your selection is remembered per workspace. Hold **Shift** while
using arrow keys to move focus without changing the selection, then use
**Shift+Enter** or **Shift+Space** to add or remove that pane. A normal click returns to one pane.
The collapsed rail still previews one section at a time; expanding restores the
selected panes. Click the sole selected
tab to open its default
content: **Agents** opens the main agent, **Context** the Spec, **Changes** the
changes list, and **Files** a root README (otherwise the first root file, then a
file inside a folder). **Browsers** opens the first browser in its list; **Shells**
opens the first terminal, creating one when none exists. A click in the collapsed
rail opens the section's default content immediately. With one pane selected,
arrow keys select a section without opening main content. With multiple panes
selected, arrow keys move focus without changing the selection. The focus ring
stays inside the tab as the row scrolls. Enter or Space acts like a click in either
layout. The rail's toggle stays visible as its tabs scroll
and pins the sidebar open;
the expanded sidebar's toggle collapses it.
**Toggle Sidebar** still uses
**⌘B** on macOS or **Ctrl+B** on Windows/Linux by default.
Pane menus offer **Reveal in Sidebar** to locate content in its sidebar section.
This keeps other selected panes open. When collapsed, it opens the matching
floating section beside the rail.
In every sidebar section, a chevron pointing right marks a closed group, row, or
drawer; select it to open, and the chevron points down.
In **Agents**, the **Delegated**, **Background**, and **Retired** groups show their
counts beside the chevron. Closed Delegated and Background groups also show how
many agents are running.
In **Shells**, Start and Stop stay beside each script. Open its **Actions** menu
for Restart or to choose where its output opens.

In **Files**, search and file actions sit beside the section title. Open search to
find a file; use the arrow keys to choose a result and Enter to open it. Escape
clears search and returns focus to its button. The filter shows only changed files;
select it again to show all files.

In the workspace sidebar, open **Context** to find the **Spec** and other notes.
Use its **Add context** action to create a note. In layouts showing the Notes-panel
button, its visible label is **Attach more context**; **New note** is the tooltip.
Notes hold shared context, decisions, and deliverables for that workspace.
A blue dot on a note's icon marks unread changes, like the dot on agent avatars.
Open the note to clear it; task progress rings still show task status.
Notes with child notes, including the Spec, show a chevron and start collapsed,
with a child count. Select the chevron to show or hide the children
without opening the note; that choice is saved for the workspace. Searching shows
the matching children of notes you have not expanded or collapsed.

With a supporting daemon, the desktop app shows a note over 500 KB truncated and
render-only, with a banner, so the user cannot edit it there. Agents still read
and write the full content; make large-note edits with note tools rather than
asking the user to edit them in the app.

In test builds that include **Back to rendered view**, use **Edit Raw Markdown**
in a note's **…** menu to edit Markdown directly; large notes otherwise stay
read-only. **Back to rendered view** flushes pending edits and waits for the save acknowledgement before
leaving the editor. Returning does not undo saved changes. If saving fails or
conflicts, the draft and editor stay open: use **Retry save** after addressing the
error, then return. A rendered-note loading error is separate from a save failure;
returning to rendered view does not guarantee that loading error is resolved.

<!-- Sources: packages/cloudlands-fe/src/features/layout/tab-types/NoteTabType.svelte; packages/cloudlands-fe/src/lib/components/workspace/NoteWithComments.svelte; packages/cloudlands-fe/messages/en.json. Test-build behavior pending its frontend release. -->

Open a task note and inspect its status and assigned agent first. Select the
assigned agent to continue existing work. To start an agent for the task, use the
play button labeled **Run agent** when available. This creates an agent and sends
the task's initial message; do not use it to duplicate work already underway.
It is a task-note action, not an action on the Spec or every ordinary note. Access
can hide it; explain the observed limitation instead of promising it is always there.

Find results in the agent's conversation and relevant task note; follow the file
or artifact links the agent supplied. A status label alone is not proof that work
succeeded. Use live workspace, note, and agent tools for actual IDs, progress, and
available actions. Never construct links from guessed IDs or claim completion from
this static guide.

Workspace file links open a file panel. Markdown files open in a read-only preview
that renders Mermaid diagrams. For files in the main workspace checkout, relative image paths such as
`charts/chart.png` resolve from the Markdown file's folder and cannot reach
outside the workspace; files from other registered repositories do not resolve them.
Use the diagram's **View source** action to inspect
its code, or turn off **Markdown preview** in the panel menu to edit the file's source.
Excel workbooks and other binary files
without a preview show a binary-file notice; use **Download file** in that panel
to save the original file on your computer, including from a remote workspace.
Binary files are not editable in the file panel or Files view. Delete is unavailable
when Undo cannot preserve the file's contents; the file stays unchanged.
A missing-file or access error is different: check the path and access rather than
assuming the file cannot be previewed.

In Assistant chat, a short file link (`intent://local/file/{path}`) opens a file from the
Assistant's own working folder (`chief-cwd` in the daemon's data folder), not from a
workspace. Files there are temporary and are cleared when the daemon restarts, and edits
made to them in the file panel are not saved yet.

<!-- Sources: packages/intentd/crates/intent-core/src/chief_cwd.rs; packages/intentd/crates/intent-services/src/file_ops.rs (resolve_root, chief_root). -->

Assistant chat can show an image from any workspace on the connected computer when its link names that workspace (`intent://local/{workspaceId}/file/{path}`). A workspace agent's chat shows only its own workspace's images; another workspace's image appears as a link or as a placeholder saying it is from another workspace. Open that workspace, or ask the Assistant, to view it.

With a supporting daemon, a tool output too large to load in one read shows its first part in agent chat with **Output truncated — showing <size> of <size>**, or a preview with **Too large to show in full — showing a preview**. An image too large to load keeps its thumbnail with **The full image is too large to show**. Regenerating a message re-sends its original images even when they are too large to show. A single message too large for one page shows its first part, without the parts that did not fit, and a **Show full message** button that loads the rest.

<!-- Sources: packages/cloudlands-fe/src/lib/components/chat/{ToolCall,ChatImageBlock,ChatMessage}.svelte; packages/cloudlands-fe/src/lib/components/chat/block-hydration.ts; packages/cloudlands-fe/src/store/renderer/slices/agent-session/sagas/regenerate-from-message-saga.ts; packages/cloudlands-fe/messages/en.json (chat_toolCall_outputTruncated_label, chat_toolCall_outputTooLarge_label, chat_imageBlock_tooLarge_label, chat_chatMessage_showFullMessage_button). Test-build behavior pending its frontend release. -->

Agents mention another workspace as a link showing its title rather than its internal ID.
Clicking a workspace link in an agent chat or a note switches to that workspace; in Assistant
chat it opens beside the conversation.

<!-- Sources: packages/cloudlands-fe/src/lib/utils/workspaces-link-handler.ts (workspace links outside Assistant chat); packages/intentd/crates/intent-services/resources/agent-instructions/v3.4/common.md. Test-build behavior pending its frontend release. -->

<!-- Sources: packages/cloudlands-fe/src/features/layout/tab-types/FileTabType.svelte; packages/cloudlands-fe/src/lib/components/markdown/{MarkdownViewer,MarkdownDiagram,MermaidRenderer}.svelte; packages/cloudlands-fe/src/lib/components/file-explorer/file-explorer-layout.svelte; packages/cloudlands-fe/src/store/renderer/slices/files/sagas/files-write-saga.ts; packages/cloudlands-fe/src/lib/client/live/live-files-client.ts; packages/cloudlands-fe/src/features/file/services/download-workspace-file.ts; packages/cloudlands-fe/src/lib/utils/workspace-file-image.ts. -->


## Review and commit workspace changes

<!-- Sources (paths relative to packages/cloudlands-fe): src/lib/components/workspace/sidebar/{SidebarChangesPanel,FileChangesSection,CommitDrawer,CommitsTimeline,HistoryBoundary,SecondaryRootChangesView,PRSection,WorkspacePullRequests,SidebarPrList}.svelte. -->

In the workspace sidebar, open **Changes** to review files and commit history.
Owners can toggle **Auto-commit** beside the file summary. **Unstaged** and
**Staged** appear when they contain files; **Stage all** and **Unstage all** sit
in their section headers. After staging files, open **Commit** to enter a message.
An open commit draft stays available if the staged list changes.
Expand a commit to see its files, or select its title to open the complete change.
Open a pull request from the sidebar's **Pull requests** list.
Line-change totals appear at the right of each row. **Workspace start**
marks the boundary; select it to show or hide earlier commits. For a secondary Git
root, **Root registered** marks that root's boundary.

## Daemon lifecycle from the terminal

For a separately installed `intentd` launcher on Windows, macOS, or Linux, use
`intentd start` to run in the background, `intentd status` to inspect live status,
`intentd stop` to confirm shutdown (including a supervisor still starting or recovering;
already stopped succeeds), and `intentd restart` to replace the supervised daemon
or start it if stopped. These commands act on the
host where they run; stopping the daemon disconnects its clients.

Startup-aware launchers keep waiting while fresh startup status arrives, with a
60-second budget for silence. Older launchers can return once the daemon is alive
but still preparing; check `intentd status` before using it. Starting a healthy daemon
again succeeds without restarting it. Startup errors return nonzero and point to
`<data-dir>/sitter/start.log`. `restart` from stopped uses that same startup path.
For a running supervisor, restart retains its launch options; Windows waits for
replacement readiness, while macOS/Linux return after signaling, so check `status`.
During startup, `intentd status` reports not ready. The scripting command
`intentd call system.status` can return successful liveness JSON with `ready: false`;
that is not application readiness. This preserves live startup with older launchers.
This also works with supervisors launched by services or Windows Scheduled Tasks.
Background start/restart does not install or enable boot/login services or tasks;
stop does not disable an existing one.

Use the same `INTENTD_DATA_DIR` and configuration environment for each command.
`start` accepts serve options (`--mode`, `--insecure` for development only,
`--resume-all`, `--specialists-dir`); restart takes no launch options. Stop then
start to change options. `intentd --help` and `intentd start --help` are safe even
before daemon installation. `serve` stays in the foreground. Direct bare daemon
builds and the desktop-bundled daemon are distinct from this installed launcher;
start/restart are launcher commands. Check the installed launcher's help when its
version differs from this guide; do not infer launcher support from daemon version.

`intentd agents` lists the daemon's running agents: name, workspace, provider and
model, owner, memory and process count, then the total and sample time. `--json`
prints the raw data, with secret values in process command lines replaced by
`[redacted]`. It also works on a machine in node mode (`intentd serve --node`).

If note or chat writes fail with a code 267 error ("database disk image is malformed", or
"fts5: corruption ..." on newer builds), run
`intentd doctor`. When it reports only a damaged search index, quit Intent, run
`intentd stop`, then `intentd repair-indexes`. It copies the database to
`<data-dir>/backups/` first, then rebuilds the index from the stored notes and messages
without changing them. If doctor reports damage outside the search indexes, stop and
copy the whole data folder before any recovery.

<!-- Sources: packages/intentd/README.md (Start, status, stop, and restart; Repairing a damaged search index); packages/intentd/crates/intentd-sitter/src/{cli.rs,main.rs,startup.rs,supervisor.rs,paths.rs,readiness.rs}; packages/intentd/crates/intentd/src/main.rs (Serve, Status, Agents, Stop, Doctor, RepairIndexes); packages/intentd/crates/intentd/src/agents_cmd.rs; packages/intentd/crates/intentd/src/db_repair.rs; packages/intentd/crates/intent-store/src/derived_index.rs. -->
