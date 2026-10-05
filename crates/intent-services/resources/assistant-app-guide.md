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

Multiplayer, Collaboration, and personal device pairing are currently experimental.
Do not introduce them in routine answers about settings, devices, or mobile pairing.

<!-- Sources (paths relative to packages/cloudlands-fe): src/features/settings/MobileSettings.svelte; src/features/devices/{PersonalDevicesPanel.svelte,personal-devices-selectors.ts}; src/store/renderer/slices/principal/principal-selectors.ts; src/store/renderer/slices/websocket-api/sagas/websocket-api-saga.ts; messages/en.json. -->

Only when explicitly asked about personal device pairing: it pairs another device
with the signed-in person's current access. It is separate from the local Remote
Access QR, which grants access to that computer. Do not offer the local QR as a
substitute or unrelated alternative. If the user asks where to test personal
pairing, use **Settings → Mobile → Intent Mobile → Show QR Code** on the intended
connection. Older instructions may call it **Pair another device as me**; that is
not the current button label. Personal pairing requires a current signed-in identity
and the connection's personal-pairing capability. The restricted-access panel also
requires Multiplayer and a refreshed identity; the remote-owner panel needs Remote
Access enabled. Connected-device list support is independent. Do not invent a
readiness date or advise enabling the experiment for ordinary mobile setup.

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

## Find a setting

Open Settings and use these sidebar labels (English labels shown):

| Section | What it contains | Canonical link |
| --- | --- | --- |
| Appearance | Theme, colors, language, and fonts | `/settings?tab=display#theme` |
| General | Notifications, updates, external apps, GitHub link behavior | `/settings?tab=app-behavior#notifications` |
| Input and shortcuts | Voice dictation and keyboard shortcuts | `/settings?tab=input#keyboard-shortcuts` |
| Agent defaults | Global instructions and agent features | `/settings?tab=agent-behavior#global-instructions` |
| Providers | AI coding CLIs and default model | `/settings?tab=providers#providers` |
| Connections | Integrations and MCP servers | `/settings?tab=connections#integrations` |
| Devices | Local machine and saved remote connections | `/settings?tab=devices#devices` |
| Mobile | Intent Mobile pairing and Remote Access | `/settings?tab=mobile#mobile` |
| Workspace setup | Git, shell, and workspace defaults | `/settings?tab=setup#git-workspace` |
| Advanced | Agent backend, connection, tool output/retention, data, reset | `/settings?tab=advanced#workspace-api` |

For MCP configuration, open **Connections → MCP servers**; its reference route is `/settings?tab=connections#mcp-servers`.

## Choose a model or change agent behavior

<!-- Sources: packages/cloudlands-fe/src/lib/components/workspace/initializer/InitialAgentPicker.svelte; packages/cloudlands-fe/src/lib/components/settings/SettingsSidebarNav.svelte; packages/cloudlands-fe/src/shared/app-ui-targets.ts. -->

For the default model, open **Settings → Providers → Default model**
(`/settings?tab=providers#utility-default-model`). For new work, choose a model in
the workspace creation form where offered. Available choices depend on configured
providers; diagnose a missing provider in **Providers**. Do not promise a default
change will switch an existing agent. Specialist entries appear
in the Settings sidebar's Agents group. Create one at
`/settings?tab=specialists&view=create-specialist#create-specialist`; select an
existing specialist to edit its behavior. Global defaults and a specialist's
instructions are different scopes. Some agent feature changes apply only to newly
created sessions; do not promise they alter running conversations.

Settings sections and controls can depend on platform, daemon capabilities, or
access. A supported route does not grant permissions or prove that a control is
available on the current connection.

<!-- Sources: packages/cloudlands-fe/src/features/settings/MobileSettings.svelte; packages/cloudlands-fe/src/lib/components/settings/{DevicesSettings,DeviceRow,WebSocketApiSettings}.svelte; packages/cloudlands-fe/src/store/renderer/slices/websocket-api/sagas/websocket-api-saga.ts. -->

## Find the mobile pairing QR code

Open **Settings → Mobile → Intent Mobile → Show QR Code**. Use the mobile app's
pairing scanner. The Remote Access reference route is
`/settings?tab=mobile#websocket-api`. There is no need to expand a device's Edit panel.

For ordinary local-machine pairing, select that machine as the active connection.
If connected elsewhere, use **Settings → Devices**, then the local machine's
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

Devices lists the local machine and saved remote connections. **Add device** opens
the connection form; a remote device's actions include **Edit** and **Connect**,
with **Test connection** inside its editing panel. Workspaces and agents belong to a
host, so identify the intended device before explaining missing work or changing
host settings. **Mobile** follows the active connection and its permissions; do not
describe a remote connection's QR as belonging to the local desktop. To pair the
local machine, connect to it first. Enabling access, changing listening addresses,
and configuring a tunnel are separate choices from displaying pairing information.
Read current state and respect the user's requested scope.

## Start or continue work

<!-- Sources: packages/cloudlands-fe/src/lib/components/workspace/{CompactWorkspaceInitializer,MultiSelectTabbedSidebar,CreateAgentSection}.svelte; messages/en.json. -->

Open **New workspace** (`/workspace/new`), select a repository, describe the work,
and choose **Create workspace**. The workspace groups working files, notes, and
agent conversations. Resolve any displayed repository, Git, or provider setup
error before retrying. If creation succeeded but sending the first message failed,
use the form's retry instruction instead of creating a second workspace.

To continue work, open its existing card or sidebar entry, then the existing agent's
conversation. Read its status and last response before sending a follow-up. Use
**Create new agent** in that workspace only when a separate conversation is needed;
choose a specialist and model where offered. A specialist defines reusable behavior;
an agent is a particular conversation doing work.

## Add context, run a task, and find results

<!-- Sources: packages/cloudlands-fe/src/lib/components/workspace/MultiSelectTabbedSidebar.svelte; sidebar/{AddContextSection,ContextPanel,NotesPanel}.svelte; NoteMetadataBar.svelte (sidebar/ and NoteMetadataBar paths relative to the same workspace component directory); messages/en.json. -->

In the workspace sidebar, open **Context** to find the **Spec** and other notes.
Use its **Add context** action to create a note. In layouts showing the Notes-panel
button, its visible label is **Attach more context**; **New note** is the tooltip.
Notes hold shared context, decisions, and deliverables for that workspace.

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
