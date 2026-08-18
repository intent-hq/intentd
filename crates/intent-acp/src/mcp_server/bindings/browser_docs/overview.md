# Browser API Overview

The browser API gives you programmatic access to embedded browser tabs via Chrome DevTools Protocol.
Actions are executed as a sequence of declarative operations.

## Reuse Tabs by URL

Use one tab per URL by default. Before `openTab`, call `listTabs`. If an agent- or
user-opened tab already has the target URL, call `focusTab` with its `tabId` and operate
on that tab instead of opening another. Use `openTab` only when no tab has the target
URL, or when the user explicitly asks for another tab, side-by-side view, or second
instance of the page.

A different URL may open in a new tab. Do not navigate an existing tab away from its
current URL merely to avoid opening a tab for a different URL. Do not close user-opened
tabs; leave extra tabs the user created in place.

## Basic Actions
- `{ action: "listTabs" }` - List all browser tabs
- `{ action: "getAccessibilityTree", tabId? }` - Get page structure as YAML
- `{ action: "screenshot", tabId? }` - Take a screenshot
- `{ action: "evaluate", expression, tabId? }` - Run JavaScript in the page
- `{ action: "focusTab", tabId? }` - Focus/remount a tab

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
- `{ action: "openTab", url, position? }` - Open a new browser tab in the UI
  - position: 'adjacent' (default), 'replace', or 'same'
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

Use `browser_docs` with topic="capture" or topic="examples" for detailed usage.