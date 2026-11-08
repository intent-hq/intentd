# Browser API Overview

The browser API supports desktop page automation via Chrome DevTools Protocol and,
on supporting iOS builds, native tab navigation. Native preview support requires
the companion iOS implementation; do not infer availability from the tool alone.
Actions are executed as a sequence of declarative operations.

## Native navigation

A client advertising `browserNavigation: true` can receive `openTab`, `showTab`,
`focusTab`, `closeTab`, and `navigate`. `browserExec: true` separately enables
page automation. New, unnamed `openTab` requests prefer the device that sent the
conversation's latest accepted user message, if connected and capable of the
whole request. Otherwise the workspace browser assignment applies, then recent
workspace focus among eligible clients. If no focused client is eligible, new
opens use the desktop-first, then native default.
A delegated helper without its own user receipt inherits the nearest parent's
preference within the same workspace. Its own receipt, even from an unknown or
currently unavailable device, stops inheritance; unrelated conversations do not
share preferences. Queued user messages update this preference on acceptance; retries, queue drains,
and automatic or subagent messages do not change it. The preference survives
reconnects and daemon restarts. An explicit workspace client pin always wins and
never falls back. Requests containing page automation still require a desktop.
An offline desktop tab or desktop session does not block opening a new native
preview when no client is pinned. `listTabs` is answered by the daemon.

Native tabs carry `navigationOnly: true` in `listTabs` and stay on their physical
`hostClientId`, even after a desktop connects or the primary client changes.
Named navigation requests never migrate tabs. A batch addressing different hosts
fails with `browser-mixed-hosts` before executing anything. Native tabs cannot be
claimed, resized, inspected, evaluated, screenshotted, or used for capture/trace;
these requests fail with `browser-automation-unavailable`, including mixed
navigation/automation batches. Create a separate desktop tab for automation.
`ws.browser.clients()` describes automation routes; a null automation route does
not rule out native navigation. `client.list` distinguishes the two capabilities.

Native `openTab` defaults to visible (desktop tabs remain hidden by default).
`accepted: true` confirms acceptance, not presentation: check `displayed` in the
result or the later registry update to confirm the native browser appeared.
Native previews use the device's viewport, not desktop viewport emulation.
Native user tabs may be focused/shown without claiming them, but agents cannot
navigate or close them. Another agent's tabs remain protected by `not-owner`.
The desktop ownership, sizing and capture rules below apply to desktop tabs.

## Reuse Tabs by URL

Use one tab per URL by default. Before `openTab`, call `listTabs`. If a tab you own
already has the target URL or that target's rewritten/redirected `finalUrl`, call
`focusTab` with its `tabId` and operate on that tab instead of opening another. For
example, a target using `daemon.localhost` may appear in `listTabs` as `127.0.0.1` or
the remote daemon host. An unowned (user) tab at the target URL can be reused only
after you claim it with `claimTab` (see Tab Ownership & Sizing below); tabs owned by
other agents are never reused. `openTab` dedupe is likewise per-agent: re-opening the
same `requestedUrl` reuses your own existing tab (hidden or visible — visibility does
not affect matching), never another agent's tab or an unowned one — two agents opening
the same URL get two tabs. A dedupe hit never changes the reused tab's visibility:
even with `visible: true`, a hidden tab stays hidden (and a visible tab stays
visible) — revealing an existing tab is `showTab`-only. Use `openTab` only when no
reusable tab matches the target or its `finalUrl`, or when the user explicitly asks for
another tab, side-by-side view, or second instance of the page.

A different URL may open in a new tab. Do not navigate an existing tab away from its
current URL merely to avoid opening a tab for a different URL. Do not close user-opened
tabs; leave extra tabs the user created in place.

## Tab Ownership & Sizing

Tabs are agent-scoped. Every tab carries a nullable `ownerAgentId`: user-opened tabs
start unowned; tabs you open with `openTab` are owned by you from creation. You may
only manipulate (navigate / close / evaluate / screenshot / ...) tabs you own — other
agents' tabs are visible in `listTabs` but not manipulable. Ownership failures surface
as structured action-result errors inside the per-action `{ action, success: false,
error }` envelope (never as a top-level failure): `not-owner` (an op on a tab you do
not own) and `already-claimed` (a claim lost to an earlier claim). Each names the
owning agent when the tab has one; a `not-owner` on an unowned tab carries no owner
info — the remedy is `claimTab`.

For a single-action call the returned value is that action's envelope. A multi-action
batch that fails returns `{ success: false, results, error }`; the batch aborts on the
first failing action, so `results` holds only the actions executed so far (the failing
one last). The multi-action *success* shape is `{ results }` with no `success` key, so
test failure with `r.success === false` — a truthiness check like `!r.success`
misclassifies every successful batch.

- `{ action: "claimTab", tabId, width, height? }` - Claim an unowned (user) tab for
  yourself. `width` is required (a claim without it is a validation error); omitted
  `height` defaults to 800. Claims are atomic, first-claim-wins: a successful claim
  transfers ownership and enables viewport emulation at the given size in one step.
  There is no stealing — a claim on an already-owned tab fails with `already-claimed`.
- `{ action: "resizeTab", tabId, width, height? }` - Change an owned tab's emulated
  size; omitted `height` keeps the tab's current emulated height. Owner-only: on a tab
  you do not own it returns `not-owner`. There is no size op for unowned (user) tabs
  and no reset-to-native form.
- `{ action: "listTabs", scope? }` - `scope: "mine" | "unclaimed" | "all"` (default
  `all`). Every returned tab carries `ownerAgentId` (`null` when unowned) plus owner
  display info, and sizing info: `mode: "native" | "emulated"` and, when emulated, the
  current `width` / `height` — so you can see a tab's size before deciding to claim or
  resize.

Sizing invariant: unowned (user) tabs are always native; agent-owned tabs are always
emulated — the size is applied as viewport emulation, so owned tabs render
deterministically offscreen without disturbing the user's panel layout. There is no
opt-in and no clear/reset op. Everywhere they appear (`claimTab` / `openTab` /
`resizeTab`), `width` and `height` are positive integers (CSS px); zero, negative,
fractional, or non-finite values are validation errors. Agent-issued `openTab` accepts
optional `width` / `height` and the tab is emulated from creation; omitted `width`
defaults to 1280 and omitted `height` to 800.

Lifecycle: ownership is persisted alongside the tab and survives app restarts — an
owned tab never silently reverts to unowned on relaunch — and persists when the owning
agent completes. Agent deletion destroys all the agent's tabs, self-opened and claimed
alike (there is no release-to-unowned path, so no tab ever transitions
emulated→native). A user "close" of an agent-owned tab is a UI-level hide, not a
destroy: the tab returns to hidden (`visibility: "hidden"`), stays alive, continues to
appear in `listTabs`, and can be revealed again with `showTab`.

## Tab Visibility

Agent-opened tabs start hidden: `openTab` without `visible: true` creates the tab
hidden — alive, owned by you, emulated (the sizing invariant above is unchanged),
returned by `listTabs` with `visibility: "hidden"`, and rendering offscreen — with no
panel mount and no focus or active-tab change. Pass `visible: true` to open directly
into the user's panel layout: the tab is mounted AND made its panel's active tab
without stealing panel/keyboard focus, on every `position` (`adjacent`, `same`, or a
`replace` that falls back to a new tab). The `openTab` result carries an **optional**
`displayed` (see below): present when the layout confirmed the tab's display state,
absent when that state is unknown (the layout could not be read, or the tab was not
in the fresh tab list) — absence means unknown, NOT `false`; re-check with
`listTabs`. Only `visible: true` requests carry it (a dedupe reuse under `visible: true`
reports the reused tab's real display state); default hidden opens and reuses without
`visible: true` omit it. Per-agent `openTab` dedupe matches regardless of
visibility — a same-URL reopen reuses your tab whether it is hidden or visible — and a
dedupe hit never changes the reused tab's visibility: a hidden tab stays hidden even
when the `openTab` carried `visible: true` (and a visible tab stays visible).
Revealing an existing tab is `showTab`-only.

- `{ action: "showTab", tabId, focus? }` - Activate an owned tab in a visible panel:
  reveals a hidden tab, or brings a visible-but-inactive tab to the front of its
  panel. Owner-only: on a tab you do not own it returns the structured
  `not-owner` error. `focus` defaults to false: the tab is activated in a visible
  panel without moving panel/keyboard focus and without displacing the
  currently-viewed conversation; `focus: true` activates AND focuses — the tab becomes
  its panel's active tab and the panel takes focus. The reveal is persisted: a
  revealed tab stays visible across app restarts. Idempotent on an already-displayed
  tab (a no-op success with `focus: false`; `focus: true` still focuses its panel).
  An unknown `tabId` fails as an action-result error naming the unknown id.
- `listTabs` results carry `visibility: "visible" | "hidden"` on every tab, plus
  `displayed?: boolean` — a fact about the saved layout only: true when the tab is
  visible AND its panel's active tab (derived from the panel that holds the tab).
  The field is present only while the daemon holds a current host report for it —
  absent before the host first reports it, after a later report that omitted it
  cleared it, and after a daemon restart until the host reconnects — so absent means
  unknown (never `false`, and not "never reported"); re-read with `listTabs`.
  `visibility: "visible"` alone does not mean displayed: a visible tab that is not
  its panel's active tab is in the layout but is not painted on screen, so
  `visibility: "visible", displayed: false` is the "sits behind another tab" state —
  `showTab` it (default `focus: false`) to bring it to the front. Hidden tabs are
  `displayed: false` whenever the field is present. `displayed` says nothing about
  whether the workspace is in view or the panel is hidden by zoom; what a tab can
  paint or capture is stated once in the capture-ops contract below.
- `focusTab` is unchanged for visible tabs (activate + focus the panel). On a hidden
  tab it fails with an action-result error directing you to `showTab` — there is no
  focusTab overload that reveals a hidden tab.

Hidden tabs are fully usable for background work — screenshots, evaluation, and
navigation are deterministic offscreen thanks to viewport emulation. Reveal a tab
only when the user should see it, and prefer the default `focus: false` so you never
steal focus.

**Frame rate during agent use.** While you act on a tab you own, it draws at full
frame rate (requestAnimationFrame, CSS animations, timers) even when it is hidden or
the app window is minimized or covered. Content actions on it (`evaluate`,
`screenshot`, `snapshot`, `getAccessibilityTree`, `navigate`, `focusTab`,
`startSession`, and the `startCapture` / `captureStep` / `startTrace` session steps)
keep it at full rate for as long as they run and for 30 s
after your last action; after that an idle hidden tab drops back to a throttled,
uneven rate to save battery. So animation, flicker and timing measured
inside one `evaluate`, or across calls a few seconds apart, are real. After a longer
gap, the first frames of the next action can still be throttled: send a cheap action
first (e.g. `evaluate` of `1`). A trace or capture whose steps are more than 30 s apart
is throttled in between, so keep its start and stop close together. Tabs you do not
own keep the browser's normal throttling.

**Capture ops and workspace visibility.** Layout-only actions (`openTab` hidden or
visible, `closeTab`, `showTab`, `focusTab`, `claimTab`, `resizeTab`, …) do not need
the workspace to be open/visible in the app: they apply their effects to the
persisted layout state so it is correct when the user next opens the workspace.
When the workspace is not visible in the UI, no actual UI focus/activation is
attempted: `showTab { focus: true }`, `focusTab`, and `openTab { visible: true }`
succeed, skip the UI focus attempt, and the action result carries an additive
`warning` string stating that the workspace is not visible so no UI focus was
attempted (the field is absent when the workspace is visible).

Capture ops (`screenshot`, `getAccessibilityTree`, `evaluate`) and `navigate` (which
runs through `evaluate`) need a mounted webview. A tab whose webview is not mounted —
typically a **visible** tab whose workspace is not in view or that sits behind another
tab in its panel, but also a tab opened while its workspace was not in view, whatever
its `visibility` — is mounted on demand by the op itself (hydrate the layout, wait for
the tab to register). For the three capture ops the whole pipeline — mount, wait for
the page to finish loading, capture — is bounded by one request deadline, so the op
is designed not to outlive the request; `navigate` runs only the mount step with no
deadline (its mount wait has its own cap), never waits for the page to settle, and
does no origin check; `snapshot` and the session actions (`startSession`,
`startCapture`, `captureStep`, `startTrace`) run the same mount step under the request
deadline, without the settle or origin check. A workspace that is not open in any
window (its tab was closed, or it was not reopened since a restart) is hosted in the
background by an open app window on the same host — no workspace tab, navigation, or
focus — so tab ops and captures work without opening it. Captures also keep painting
while the app window is minimized or covered by other apps. A mount on demand against
a not-in-view workspace succeeds with the same additive `warning` string. When a mount, settle, or paint cannot happen, the op fails as an
action-result error (`success: false`, `error` naming the cause and remedy) with an
additive structured `errorCode`: `workspace-not-visible` (the tab could not be
mounted while the workspace is not displayed in any window — either no app window is
connected to its host to host it at all, or a background window hosts it but the
offscreen mount wait ran out at its cap; retry shortly or `listTabs`, and open an
Intent window if none is open), `deadline-exhausted` (capture ops only: the request deadline ran out
at a named stage — retry), `still-loading` (capture ops only: the page was still
loading after the bounded wait — retry, or `snapshot` with `waitFor: { networkIdle }`
first), `navigated-away` (capture ops only, and only checked right after a mount on
demand: the freshly mounted page shows a different origin than the tab list recorded
— `navigate` back, or `listTabs` to re-check; drift on an already-mounted tab is not
detected), or `not-painting` (capture ops only: the webview is mounted but its
surface has not painted — either the capture did not return within the capture
stage's own cap, or it returned an empty / empty-encoded image, reported as soon as
observed — e.g. a `displayed: false` tab behind a sibling, or a `displayed: true`
tab whose panel is hidden by zoom — `showTab` or `focusTab` it, then capture again).
Other failures
carry no `errorCode`, except ownership failures, which keep their `not-owner` /
`already-claimed` codes. One backstop: if a batch still has not settled shortly
after the request deadline (a stage that takes no deadline, such as `navigate`'s
mount or evaluate), the whole `ws.browser.exec` call fails with a top-level error
naming the "action execution" stage instead of a per-action `errorCode`.

## Basic Actions
- `{ action: "listTabs", scope? }` - List browser tabs (`scope: "mine" | "unclaimed" | "all"`, default `all`) with ownership, sizing, visibility, and `displayed` (visible AND its panel's active tab) info
- `{ action: "getAccessibilityTree", tabId? }` - Get page structure as YAML
- `{ action: "screenshot", tabId? }` - Take a screenshot
- `{ action: "evaluate", expression, tabId? }` - Run JavaScript in the page
- `{ action: "focusTab", tabId? }` - Focus/remount a visible tab (fails on a hidden tab — use showTab)
- `{ action: "previewHtml", html, width?, appearance? }` - Check a self-contained `ws-block:html` visual before showing it, in an invisible window with the app sandbox, fonts and themes (no tab, server or focus change). `width` is CSS pixels, 320 to 1600 (default 800; about 390 for phones). `appearance` is `"light"` (default) or `"dark"`. As a single action, `r.result` has `assetUrl`, `width`, `height`, `contentHeight`, `truncated` (full-height PNG capped at 8000px), `appearance` and `consoleMessages` (`[{ level, message }]`); view it with `return await ws.note.readAsset(r.result.assetUrl)`. Without `assetUrl`, the result keeps `base64` and `mimeType`. Frontends without this action report it as unsupported.

## Capture Actions (for debugging)
- `{ action: "snapshot", workspaceId, name?, reload?, waitFor? }` - Quick point-in-time capture
- `{ action: "startSession", workspaceId, name? }` / `{ action: "endSession", sessionId }` - Multi-step capture
- `{ action: "startCapture", sessionId }` / `{ action: "endCapture", sessionId }` - Event capture
- `{ action: "captureStep", sessionId, stepName, reload?, waitFor? }` - Capture a named step
- `{ action: "startTrace", sessionId, traceName }` / `{ action: "stopTrace", sessionId, traceName }` - Performance traces

## Navigation
- `{ action: "navigate", url, tabId? }` - Navigate an existing tab to a new URL

## Recovery
- `{ action: "resetTab", tabId? }` - Force reset CDP connection (use if you get "already attached" errors)

## UI Control
- `{ action: "openTab", url, position?, visible?, width?, height? }` - Open a new browser tab, owned by you; hidden by default (see Tab Visibility)
  - visible: true opens directly into the UI, activated in its panel without stealing focus on any position; the result carries `displayed` when the layout confirmed it and omits it when unknown (absent ≠ false); omitted/false creates the tab hidden
  - position: 'adjacent' (default), 'replace', or 'same'
  - width/height: emulated viewport size in CSS px; omitted width defaults to 1280, omitted height to 800
- `{ action: "showTab", tabId, focus? }` - Activate an owned tab in a visible panel without stealing focus (reveals a hidden tab, or brings a visible-but-inactive one to the front); focus: true also focuses it (see Tab Visibility)
- `{ action: "claimTab", tabId, width, height? }` - Claim an unowned (user) tab (see Tab Ownership & Sizing)
- `{ action: "resizeTab", tabId, width, height? }` - Change an owned tab's emulated size
- `{ action: "closeTab", tabId }` - Close a browser tab
  - tabId is REQUIRED; it does not fall back to the sequence-level default tabId

## Supported Protocols
The embedded browser supports http://, https://, and file:// URLs.
You can open local HTML files directly: `{ action: "openTab", url: "file:///path/to/index.html" }`

## Loopback URLs: daemon vs client machine

The embedded browser runs on the client (user's) machine, while servers you start (via
`ws.host.exec`, scripts, terminals) run on the daemon machine. In a remote-daemon setup
these are different machines, so `navigate`/`openTab` URLs support reserved hostnames:

- `daemon.localhost` — the machine where the daemon runs (where your servers run).
  Local setup: rewritten to `127.0.0.1`. Remote setup: rewritten to the daemon host.
- `client.localhost` — the user's machine (where the embedded browser runs). Always
  rewritten to `127.0.0.1`.
- Bare `127.0.0.1` / `localhost` / `[::1]` — ambiguous; assumed to mean the daemon
  machine (your frame of reference). Unchanged in a local setup; in a remote setup it is
  rewritten to the daemon host and the result carries a `warning` suggesting the explicit
  aliases.

Only the hostname is rewritten — scheme, port, path, query, and hash are preserved. Every
rewritten action's result echoes `{ requestedUrl, finalUrl, rewritten: true, reason, tunneled?, warning? }`
so you can see what was opened. Prefer `daemon.localhost` for servers you started.

Remote-daemon caveat: the client's browser reaches daemon-side servers over the network,
so prefer binding `0.0.0.0` and keeping the port reachable from the client machine. URLs
rewritten to a remote daemon host are reachability-probed before navigating. If the origin
cannot be reached, the Electron desktop client automatically forwards the port over the
daemon connection and opens the tunneled URL instead — the result carries `tunneled: true`
and the `reason` describes the forward — so servers bound to `127.0.0.1` on the daemon
still work. If the tunnel forward itself fails, or on web-browser clients (which cannot
open a local tunnel listener), the action fails with an explanatory error (pointing at
`127.0.0.1`-only binding or a firewall) instead of opening a broken page.

## Port Tunnels

Explicit port forwards from the client machine to daemon-loopback ports — the tab-free
counterpart to the implicit `openTab`/`navigate` auto-tunnel above. Use them when you
need the forwarded port itself rather than a browser tab. On the Electron desktop client
the same three actions work uniformly on every transport: remote daemons forward over
the daemon connection (`backend: "tunnel"`), local daemons use a loopback relay
(`backend: "direct"`). Never branch on the backend — it is diagnostic only. On
web-browser clients (which cannot open a local listener) no tunnel backend exists:
`openTunnel`/`closeTunnel` fail with an explanatory error and `listTunnels` returns
`{ tunnels: [] }`.

- `{ action: "openTunnel", remotePort }` → `{ remotePort, localPort, backend: "tunnel"|"direct", reused }`
  — forward daemon-side `remotePort`; reach it at `127.0.0.1:<localPort>` on the client.
  Reuses a live forward for the same `remotePort` (`reused: true`) or creates one.
  `reused` is best-effort/diagnostic — like `backend`, don't branch on it.
- `{ action: "listTunnels" }` → `{ tunnels: [{ remotePort, localPort, backend }] }`
- `{ action: "closeTunnel", remotePort }` → `{ remotePort, closed: true }` — errors when
  no active forward exists for that port.

### Lifecycle & recovery

Forwards are persistent client-side state: once opened, a forward keeps its `localPort`
for the lifetime of the Electron app. Transient drops of the underlying daemon
connection do not kill it — the forward stays registered on the same `localPort` and
lazily reconnects on the next inbound connection.

- A forward is closed only by: an explicit `closeTunnel`, a daemon backend switch,
  quitting the app, or all of its owning workspaces being archived/deleted. Forwards
  not tied to any workspace live for the app lifetime.
- One exception: a definitively refused connect to the daemon-side port (server gone)
  drops the forward immediately — re-open with `openTunnel { remotePort }` to re-mint
  it.
- `openTunnel` is cheap and idempotent per `remotePort`: it returns the existing
  forward (`reused: true`) when one is already registered, so calling it again is
  always safe and returns the current `localPort`.
- Older Electron clients may still exhibit the previous ephemeral behavior (forwards
  dropping on transport changes or after idling); if a previously returned `localPort`
  refuses connections there, re-open with `openTunnel { remotePort }`.

Do not persist or guess a client `localPort` across app restarts. To recover an ordinary
Chrome link, find the preview's stable `requestedUrl` in `listTabs`, read its daemon-side
port, call idempotent `openTunnel { remotePort }`, and use the returned current
`http://127.0.0.1:<localPort>` URL. This works whether the previous forward survived or
had to be re-created.

## Troubleshooting Timeouts

A `ws.browser.exec` call that gets no answer fails with `reverse request timed out:
browser.exec: browser client "<name>" (<clientId>) on connection <n> over <transport>,
connected <time> did not accept or answer the request within <s>s`. Call the read-only
`ws.browser.clients()` to see what the daemon holds right now:

- `clients` — every connected app (`clientId`, `name?`, `canHostBrowser`) with its live
  `connections`, oldest first: `connectionId` (the `<n>` in the error), `transport`
  (`uds` / `wss`), `connectedAt`, `canHostBrowser`. `clientId` matches `listTabs`
  `hostClientId`.
- `unpinned` — where a request with no pin would go: the first-connected client that can
  host a browser, via its newest connection.
- `workspace` — where this workspace's `ws.browser.exec` goes, with `source`:
  `workspace` (the workspace's browser-client pin), `desktop` (the client controlling
  this workspace's desktop), `tabs` (the host of this workspace's claimed tabs),
  `focus` (the connected app that most recently showed this workspace) or `default`
  (same as `unpinned`, also when no app that showed it is connected).

A route names `clientId` and `connectionId`; when nothing would answer it has
`connectionId: null` and an `error` (for example `no client connected`). If the
connection that timed out is not the newest one of its client, or several connections
of one app are listed, include the `ws.browser.clients()` output when you report it.

Use `ws.browser.docs("capture")` or `ws.browser.docs("examples")` for detailed usage.

## Inline visuals

Intent desktop v2.253.0 and later renders completed assistant `ws-block:html`
fences as interactive visuals in chat. For a request such as “visualize how
seasons work,” use this format to explain the mechanism with linked diagrams and
controls. Image generation is appropriate when the user asks for a bitmap image;
it does not replace an interactive explanation in chat. Keep simple answers in
prose; use Mermaid for a small static diagram and a saved chart or PNG when the
user needs an image to share elsewhere.

Use `previewHtml` to check the connected desktop before showing the visual. If
it reports the action as unsupported, explain that inline preview is unavailable
in that client and offer a saved image or text explanation. Do not claim that a
newer daemon or harness version proves installed frontend support. Do not start
a server or substitute another endpoint for this preview.

Fence raw, self-contained HTML/CSS/JavaScript with `ws-block:html` (not JSON). An ordinary `html` fence remains source code. Close the fence on its own line with the same delimiter and at least the opening length; incomplete fences never execute. Include inline styles/scripts, accessible labels, and a responsive layout. Keep controls compact and content readable in both themes.

Asked to show a visual here: check its HTML with the browser `previewHtml` action, then reply with that HTML in a `ws-block:html` fence. If the visual is a file, use that file's HTML. A screenshot on disk or a text description does not show it. Never start a server for it. Do not announce or restate the visual in prose. Asked for an image to post, send, or use elsewhere: a `.png` file and its path, never `ws-block:html`.

Call `const r = await ws.browser.exec([{ action: "previewHtml", html, width: 800, appearance: "dark" }])`, then view the screenshot with `return await ws.note.readAsset(r.result.assetUrl)`. `r.result` also has `contentHeight`, `truncated` and `consoleMessages`. `width` is 320 to 1600 (default 800); `appearance` is `light` (default) or `dark`. Preview at the chat width and at about 390 for phones, in both appearances when color or layout matters. Fix console errors, clipped or overlapping labels, and overflow before replying. If the frontend reports `previewHtml` as unsupported, say preview is unavailable; do not substitute another tool or endpoint.

Leave html, body, and the outer element with no background color, no outer card, border, banner title, or side padding. This wins over any rule that sets a page background. A box that needs its own background gets `var(--space-4)` padding and `var(--radius-medium)` corners. Give canvas charts fixed pixel heights; SVG keeps its viewBox ratio. Let content set the height. Do not use vh units or height:100% on html or body; the preview fits its height to the body and they can loop.

The supporting preview inherits app body typography and provides an optional composition kit:

- `.visual`: vertical content stack; `.visual-controls`: wrapping inline controls; `.visual-field`: label/value row with a full-width control; `.visual-row`: label and end-aligned value with tabular numbers.
- `.diagram-surface`: quiet neutral figure without an extra inset; `.diagram-node`: soft filled SVG shape; `.diagram-connector`: thin SVG connector; `.diagram-guide`: dashed SVG guide; `.diagram-label`: readable SVG text.

Use the layout classes before adding custom CSS. The neutral diagram classes suit structural diagrams such as flows and architecture; explanatory drawings of real things use meaningful chart colors instead of gray shapes. Authored styles can override the defaults. Prefer body-sized HTML labels outside SVG so text stays readable when the drawing shrinks. Use short connections, soft rounded shapes and thin strokes; avoid oversized canvases, heavy outlines and redundant captions. Keep the SVG viewBox close to its drawing bounds. Reflow controls and labels on narrow screens without overlap or nested scrolling. The wrapper adds no default heading or caption.

Use one content gutter: align headings, chart labels, plot edges, control groups and summaries to the same left edge; align row values to the same right edge. Use `.visual-row` for label/value pairs instead of unrelated margins or spaces. Avoid adding padding or browser-default figure margins inside a transparent chart. If a distinct panel needs padding, keep its labels, controls and results on the same inner edges. Let long labels wrap while values stay in their column; inspect both edges at narrow widths and after values change.

For playback or scrubbing, put the button, range input and output directly in `.visual-controls`. They share one row, with the value beside the track. Keep Play/Pause button width stable. Use `.visual-field` for fields that need a visible label; give an obvious scene-position slider an `aria-label` instead of a redundant Position heading.

For data series and depicted things, use `var(--chart-1)` to `var(--chart-6)`; they are full CSS colors, never `hsl(var(--chart-…))`. Give each distinct thing its own color and keep it consistent across views, such as the Sun versus Earth or day versus night. When a mechanism matters, link views so one control updates every view of the cause and its effect. Label marks directly at the point they describe, in readable text, rather than relying on a legend or color alone. For structural diagrams, use existing diagram colors directly: `var(--diagram-node-surface)`, `var(--diagram-node-outline)`, `var(--diagram-node-title)`, `var(--diagram-connector)`, and `var(--diagram-canvas)` are full CSS colors, never `hsl(var(--diagram-…))`. Ordinary app variables remain HSL channels: `hsl(var(--background))`, `hsl(var(--foreground))`, `hsl(var(--muted))`, `hsl(var(--muted-foreground))`, `hsl(var(--border))`, `hsl(var(--primary))`, and `hsl(var(--primary-foreground))`. Use theme CSS variables, not fixed colors. The base sheet sets background, text, and font. Reserve semantic accents for meaningful data or state, with text or shape cues as well as color. Prefer direct manipulation and native keyboard-accessible controls; give range inputs an accessible name, a visible value and neutral styling. Preserve focus and local inputs during updates, and respect reduced motion.

For persistent configurations, use only the supporting host's narrow `window.intentVisual` capability. `window.intentVisual.state` is saved JSON state (`null` initially), restored before authored scripts run on reopen or reload. Initialize one local model from that state, validate its expected shape, and use defaults only when no usable saved state exists. Render controls and derive computed results from the live model; never recompute the model from initial defaults after restore.

Call `window.intentVisual.publish({ state, title, inputs, results })` initially and on each user change. It returns a boolean for acceptance. Use JSON-compatible state, a nonblank title of at most 160 characters, and `inputs`/`results` records with at most 32 entries each. The entire snapshot must fit within 64 KiB and a maximum nesting depth of 12. Publish compact semantic inputs and explicitly derived results with units, not DOM nodes, functions, secrets or large file bytes. Handle a rejected publication visibly without discarding the local edit or claiming it was saved.

Use raw DOM controls with accessible labels inside authored HTML; app components are not available there. Direct manipulation can use pointer dragging, with a typed or keyboard alternative for every editable value. Keep controls, rows and results on the shared content edges. After every edit, update the live model, render from it and publish the corresponding inputs/results.

The trusted host toolbar owns **Save scenario**, comparison, restore and removal (up to six scenarios). **Use this configuration** appends the selected inputs/results to the editable composer draft; it never sends automatically. **Export** saves JSON and editable source through fixed workspace paths. Do not duplicate these actions inside authored HTML, choose filesystem paths, call app APIs or send messages from the visual. This extension does not promise arbitrary SVG/PNG export.

An explicit user-picked local CSV, JSON or image may be read with the browser File API through a labeled file input. Validate its contents locally; file access is limited to the user's selection, with no automatic path or network access. Keep any published representation within the snapshot limits; do not imply that selected files themselves are durably stored.

Small starting template; copy its contents into a `ws-block:html` fence:

```html
<style>
  .travel-diagram { display: block; width: 100%; height: auto; }
</style>
<div class="visual">
  <div class="diagram-surface">
    <svg class="travel-diagram" viewBox="0 0 320 88" role="img" aria-label="Position along a path">
      <path class="diagram-guide" d="M24 44H296" />
      <path id="travel-path" class="diagram-connector" d="M24 44H160" />
      <rect id="travel-node" class="diagram-node" x="148" y="32" width="24" height="24" rx="8" />
    </svg>
    <div class="visual-row"><span>Start</span><span>Finish</span></div>
  </div>
  <div class="visual-controls">
    <input id="travel-position" type="range" aria-label="Position" min="0" max="100" value="50">
    <output id="travel-value" for="travel-position">50%</output>
  </div>
  <div id="travel-status" role="status"></div>
</div>
<script>
  const position = document.getElementById('travel-position');
  const value = document.getElementById('travel-value');
  const node = document.getElementById('travel-node');
  const path = document.getElementById('travel-path');
  const saved = window.intentVisual.state;
  const model = {
    position: saved && Number.isFinite(saved.position)
      ? Math.max(0, Math.min(100, saved.position)) : 50
  };
  function updatePosition() {
    position.value = String(model.position);
    const x = 24 + model.position * 2.72;
    node.setAttribute('x', String(x - 12));
    path.setAttribute('d', `M24 44H${x}`);
    value.textContent = `${model.position}%`;
    const accepted = window.intentVisual.publish({
      state: model,
      title: 'Position along a path',
      inputs: { positionPercent: model.position },
      results: { remainingPercent: 100 - model.position }
    });
    status.textContent = accepted ? '' : 'Changes could not be saved.';
  }
  const status = document.getElementById('travel-status');
  position.addEventListener('input', () => {
    model.position = Number(position.value);
    updatePosition();
  });
  updatePosition();
</script>
```

The preview is isolated: no external libraries, imports, network requests, remote fonts/assets, app APIs beyond the narrow `intentVisual` capability, parent DOM/storage, navigation, form submissions, popups, or embedded frames. Keep data and interactions local to the block. Do not put secrets in it.
