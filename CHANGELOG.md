# Changelog

All notable changes to this project will be documented in this file.

## [0.2.7] - 2026-07-25

### 🚀 Features

- BaseRef-aware PR-workspace matching (intent-hq/monorepo#459)
- Add hidden flag to specialist definitions ([#471](https://github.com/intent-hq/intentd/pull/471))
- Turn-attachment registry for deterministic resource-block attach ([#482](https://github.com/intent-hq/intentd/pull/482))
- Carry stopReason and messageId on interrupt agent:stream:end ([#492](https://github.com/intent-hq/intentd/pull/492))
- Capture live token usage from ACP turn end ([#485](https://github.com/intent-hq/intentd/pull/485))
- Expose workspace archive/unarchive on the agent MCP bridge ([#733](https://github.com/intent-hq/intentd/pull/733)) ([#499](https://github.com/intent-hq/intentd/pull/499))
- Make comment.respond reply-anchoring contract explicit (monorepo#729) ([#496](https://github.com/intent-hq/intentd/pull/496))
- Carry isBackground on agent:idle payload ([#501](https://github.com/intent-hq/intentd/pull/501))

### 🐛 Bug Fixes

- Comment.getThread/resolveThread/list caller-input errors return -32602 (intent-hq/monorepo#649)
- *(test)* Scale fixed 5s daemon-read timeouts by the shared test budget (intent-hq/monorepo#615) ([#457](https://github.com/intent-hq/intentd/pull/457))
- Reject review-requested filter on github.issues.search (intent-hq/monorepo#551) ([#462](https://github.com/intent-hq/intentd/pull/462))
- Include script:output in ephemeral event retention sweep (monorepo#620) ([#432](https://github.com/intent-hq/intentd/pull/432))
- Inherit hidden flag across specialist tiers ([#480](https://github.com/intent-hq/intentd/pull/480))
- Use UTF-16 offset kind and recover poisoned CRDT sessions mutex (monorepo#721) ([#487](https://github.com/intent-hq/intentd/pull/487))
- Route line-attribution:updated through the transient publish path (monorepo#720) ([#488](https://github.com/intent-hq/intentd/pull/488))
- Auto-activate incremental auto_vacuum at daemon startup (monorepo#720) ([#500](https://github.com/intent-hq/intentd/pull/500))

### ⚡ Performance

- Relax sweep cadences and pause between workspaces (monorepo#703) ([#465](https://github.com/intent-hq/intentd/pull/465))

### 🧪 Testing

- Deflake queue-drain event-order race in e2e_wss_agent_lifecycle (monorepo#456) ([#459](https://github.com/intent-hq/intentd/pull/459))


## [0.2.6] - 2026-07-24

### 🚀 Features

- Add recoverable legacy import RPC ([#423](https://github.com/intent-hq/intentd/pull/423))
- Extend bare-model ownership validation to cached dynamic catalogs ([#607](https://github.com/intent-hq/intentd/pull/607)) ([#433](https://github.com/intent-hq/intentd/pull/433))
- Accept optional authorType on comment.respond ([#434](https://github.com/intent-hq/intentd/pull/434))
- Comment.add echoes post-rewrite noteRev, commits atomically, and emits note:updated (intent-hq/monorepo#638) ([#447](https://github.com/intent-hq/intentd/pull/447))
- *(providers)* Deliver workspace MCP tools to pi via bundled extension ([#452](https://github.com/intent-hq/intentd/pull/452))

### 🐛 Bug Fixes

- Enrich host PATH from login shell ([#422](https://github.com/intent-hq/intentd/pull/422))
- Enforce UDS-only guard on system.shutdown (monorepo#630) ([#436](https://github.com/intent-hq/intentd/pull/436))
- Harden skills/specialists watchers and follow workspace lifecycle ([#439](https://github.com/intent-hq/intentd/pull/439))
- Comment.respond returns -32602 for all caller-input validation errors (intent-hq/monorepo#632) ([#445](https://github.com/intent-hq/intentd/pull/445))
- Survive load spikes on agent spawn — 30s initialize timeout + jittered retry backoff (monorepo#616)

### ⚡ Performance

- *(store)* Index-friendly event retention sweep, 24h agent:tool:call TTL, incremental vacuum

### 🧪 Testing

- Deflake script-runtime and WSS runtime-control tests under load (monorepo#515) ([#448](https://github.com/intent-hq/intentd/pull/448))

### ⚙️ Miscellaneous Tasks

- Add cargo-deny license policy (monorepo#420) ([#451](https://github.com/intent-hq/intentd/pull/451))


## [0.2.5] - 2026-07-24

### 🐛 Bug Fixes

- Reject bare-model/provider mismatch at agent creation and setModel ([#425](https://github.com/intent-hq/intentd/pull/425))
- Lift proposals when the provider collapses raw_output ([#427](https://github.com/intent-hq/intentd/pull/427))

### 🧪 Testing

- Deflake graceful_shutdown_allows_immediate_restart port contention (monorepo#466) ([#429](https://github.com/intent-hq/intentd/pull/429))


## [0.2.4] - 2026-07-24

### 🚀 Features

- Emit specialists:changed on specialist file changes ([#426](https://github.com/intent-hq/intentd/pull/426))

### 🐛 Bug Fixes

- Fail closed on nonexistent agent in agent.queueMessage and agent.watchCompletion (monorepo#568) ([#408](https://github.com/intent-hq/intentd/pull/408))
- Deliver workspace MCP servers to grok sessions ([#412](https://github.com/intent-hq/intentd/pull/412))
- Annotate stale queued-message redrives and keep delivered completion report ([#576](https://github.com/intent-hq/intentd/pull/576)) ([#413](https://github.com/intent-hq/intentd/pull/413))
- Drop the draft workspace FK so opaque draft keys work (PROTOCOL 5.16) ([#420](https://github.com/intent-hq/intentd/pull/420))
- Spawn chief agents in dedicated empty chief-cwd dir instead of /tmp ([#419](https://github.com/intent-hq/intentd/pull/419))

### 🧪 Testing

- Poll system.status for WSS port with bounded backoff ([#409](https://github.com/intent-hq/intentd/pull/409))
- Deflake uds_note_subscription frame/state awaits (monorepo#601)


## [0.2.3] - 2026-07-23

### 🐛 Bug Fixes

- Fail closed on nonexistent agent in agent.send and sender-watch paths ([#407](https://github.com/intent-hq/intentd/pull/407))


## [0.2.2] - 2026-07-23

### 🚀 Features

- Free-text query on github.issues.search / github.pulls.search ([#391](https://github.com/intent-hq/intentd/pull/391))
- NextToken pagination on linear.listIssues / linear.searchIssues ([#398](https://github.com/intent-hq/intentd/pull/398))
- NextToken pagination on sentry.listIssues / sentry.searchIssues ([#403](https://github.com/intent-hq/intentd/pull/403))

### 🐛 Bug Fixes

- Switch codex provider to @agentclientprotocol/codex-acp and FirstTurnPrepend injection ([#387](https://github.com/intent-hq/intentd/pull/387))

### 🧪 Testing

- Bounded retry for WSS e2e connection establishment (intent-hq/monorepo#553)
- Route e2e_wss_sentry_pagination connect through shared retry helper (intent-hq/monorepo#553) ([#405](https://github.com/intent-hq/intentd/pull/405))

### ⚙️ Miscellaneous Tasks

- Bump sysinfo from 0.36.1 to 0.39.6 ([#395](https://github.com/intent-hq/intentd/pull/395))

