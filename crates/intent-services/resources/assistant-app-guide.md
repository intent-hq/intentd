# Intent app guide for the Assistant

This bundled guide supplies current app feature context. Use this copy instead of
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

Multiplayer, Collaboration, and personal device pairing are currently experimental.
Do not introduce them in routine answers about settings, devices, or mobile pairing.

## Finding things

The paths below identify the documented UI destinations. `ws.app.ui.targets()` can
omit known destinations: an omission does not mean the feature is missing. Use a
returned target for a navigation link when available; otherwise give the documented
click-by-click directions in prose.
Do not invent target IDs, anchors, settings tabs, or current setting values. A backend
settings category is not necessarily a visible Settings section. If navigation
cannot open a documented destination, explain how to reach it manually and check
the current connection, access, and version rather than guessing another URL.

## Settings

Open Settings and use these sidebar labels (English labels shown):

| Section | What it contains | Canonical link |
| --- | --- | --- |
| Appearance | Theme, colors, language, and fonts | `/settings?tab=display#theme` |
| General | Notifications, updates, external apps, GitHub link behavior | `/settings?tab=app-behavior#notifications` |
| Input and shortcuts | Voice dictation and keyboard shortcuts | `/settings?tab=input#keyboard-shortcuts` |
| Agent defaults | Global instructions and agent features | `/settings?tab=agent-behavior#global-instructions` |
| Providers | AI coding CLIs and default model | `/settings?tab=providers#providers` |
| Connections | Integrations and MCP servers | `/settings?tab=connections#integrations` |
| Devices | Saved remote machines and local Remote Access | `/settings?tab=devices#devices` |
| Workspace setup | Git, shell, and workspace defaults | `/settings?tab=setup#git-workspace` |
| Advanced | Agent backend, connection, tool output/retention, data, reset | `/settings?tab=advanced#workspace-api` |

For MCP configuration, use `/settings?tab=connections#mcp-servers`; for the default
model, use `/settings?tab=providers#utility-default-model`. Specialist entries appear
in the Settings sidebar's Agents group. Create one at
`/settings?tab=specialists&view=create-specialist#create-specialist`; select an
existing specialist to edit its behavior. Global defaults and a specialist's
instructions are different scopes. Some agent feature changes apply only to newly
created sessions; do not promise they alter running conversations.

Settings sections and controls can depend on platform, daemon capabilities, or
access. A supported route does not grant permissions or prove that a control is
available on the current connection.

## Mobile pairing and QR codes

Open **Settings → Devices**, open the local machine's actions menu, choose **Edit**,
then find **Connect from other apps → Show QR Code**. The direct Remote Access
link, `/settings?tab=devices#websocket-api`, opens the local editing panel.
The pairing row appears when **Remote Access** is enabled and that panel is
expanded. Generating the QR also needs a running listener port and loaded pairing
data. If Remote Access is off, explain that it must be enabled for this flow;
do not silently change it. If generation fails, inspect the connection/listener
error. TLS and tunnel switches are not visibility requirements for this QR row:
do not suggest toggling them to reveal it. This QR grants access to the local
machine; do not offer it as a substitute for someone else's restricted access.

Use the mobile app's pairing scanner for the displayed QR. Pairing links and QR
codes contain credentials; keep them private. A visible QR does not prove that the
phone can reach the host. Diagnose address/network reachability separately from
finding the QR control.

## Devices and remote access

Devices lists the local machine and saved remote connections. **Add device** opens
the connection form; a remote device's actions include **Edit** and **Connect**,
with **Test connection** inside its editing panel. Workspaces and agents belong to a
host, so identify the intended device before explaining missing work or changing
host settings. Local Remote Access
controls how other apps connect to this machine. Enabling access, changing listening
addresses, and configuring a tunnel are separate choices from displaying pairing
information. Read current state and respect the user's requested scope.

## Workspaces, notes, and agents

Use `/workspace/new` to start a workspace for a repository and describe the work.
Open an existing workspace from its card or sidebar entry. A workspace groups its
working files, notes, and agent conversations; its title and status help identify it.
Use the workspace's **Notes** panel to open the **Spec** or other notes, and **New
note** to add shared context. Task notes track assigned work; they are not separate
repositories. Keep decisions and deliverables in the relevant workspace note.

Create an agent within the intended workspace, choosing its specialist and model
where offered. Open an existing agent to continue its conversation and inspect its
status before starting duplicate work. Specialist settings define reusable behavior;
an agent is a particular conversation doing work. Use live workspace, note, and agent
tools for actual IDs, progress, and available actions; never construct links from
guessed IDs or claim work completed from a static guide.
