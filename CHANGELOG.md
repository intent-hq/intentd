# Changelog

All notable changes to this project will be documented in this file.

<<<<<<< Updated upstream
=======
## [0.2.10] - 2026-07-27

### 🐛 Bug Fixes

- *(acp)* Make mcp-bridge resilient to daemon restarts and TCP drops ([#871](https://github.com/intent-hq/intentd/pull/871)) ([#595](https://github.com/intent-hq/intentd/pull/595))


## [0.2.9] - 2026-07-27

### 🚀 Features

- Unsloth provider registry entry + HF GGUF catalog with memory-fit filtering ([#593](https://github.com/intent-hq/intentd/pull/593))

### 🐛 Bug Fixes

- Robust sandbox merge-back and faster best-effort CoW clone ([#592](https://github.com/intent-hq/intentd/pull/592))

### ⚙️ Miscellaneous Tasks

- Fail PRs containing committed git conflict markers (#588 incident) ([#591](https://github.com/intent-hq/intentd/pull/591))


>>>>>>> Stashed changes
## [0.2.8] - 2026-07-27

### 🚀 Features

- Structured error.data for unresolvable base ref (monorepo#761) ([#525](https://github.com/intent-hq/intentd/pull/525))
- Local wall-clock bucketing for usage stats (D12) ([#544](https://github.com/intent-hq/intentd/pull/544))
- System.capabilities RPC exposing machine-level cowSupported (protocol 2.3) ([#549](https://github.com/intent-hq/intentd/pull/549))
- Github.repoConfig.get RPC — fetch .intent/config.json remotely (protocol 2.4) ([#557](https://github.com/intent-hq/intentd/pull/557))
- Ws.app.question.ask binding with AtTurnEnd question attachments (intent-hq/monorepo#732)
- Inject stored GitHub token into clones and classify auth failures (monorepo#825)
- Circuit breaker for provider-blocked (poisoned) agent sessions (monorepo#840)
- Skip daemon-managed manifests in legacy import ([#579](https://github.com/intent-hq/intentd/pull/579))
- Migrate parked queues and GC poisoned sessions in agent.wakeOrCreate (monorepo#847) ([#585](https://github.com/intent-hq/intentd/pull/585))
- Stream harness-wake session/updates as implicit agent-initiated turns ([#587](https://github.com/intent-hq/intentd/pull/587))

### 🐛 Bug Fixes

- Include archived workspaces in the workspace.subscribe snapshot
- Detect dead ACP child processes and recover transparently ([#764](https://github.com/intent-hq/intentd/pull/764)) ([#523](https://github.com/intent-hq/intentd/pull/523))
- Rename skipWorktree -> skipIsolation in workspace.update params ([#533](https://github.com/intent-hq/intentd/pull/533))
- CowSupported probe default-root fallback + CoW-to-worktree creation fallback ([#540](https://github.com/intent-hq/intentd/pull/540))
- Scrub phantom anchor markers and support overlapping comment ranges ([#541](https://github.com/intent-hq/intentd/pull/541))
- Emit completionReport alongside report on wake/idle payloads ([#548](https://github.com/intent-hq/intentd/pull/548))
- Keep tool title/name/input across sparse tool_call_update events ([#551](https://github.com/intent-hq/intentd/pull/551))
- Expand leading ~ in workspace.create and git.clone paths ([#554](https://github.com/intent-hq/intentd/pull/554))
- Classify workspace.create clone failures into typed errors with sanitized detail (monorepo#826)
- Emit trailing AtTurnEnd attachment blocks on agent:stream:end (intent-hq/monorepo#732)
- Make CoW cloning best-effort and handle git-worktree edge cases ([#574](https://github.com/intent-hq/intentd/pull/574))


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
- Opt-in CoW workspace provisioning (cowIsolation checkouts, checkoutMode, sandboxes) ([#507](https://github.com/intent-hq/intentd/pull/507))
- Comment.add accepts optional client-supplied commentId ([#514](https://github.com/intent-hq/intentd/pull/514))

### 🐛 Bug Fixes

- Comment.getThread/resolveThread/list caller-input errors return -32602 (intent-hq/monorepo#649)
- *(test)* Scale fixed 5s daemon-read timeouts by the shared test budget (intent-hq/monorepo#615) ([#457](https://github.com/intent-hq/intentd/pull/457))
- Reject review-requested filter on github.issues.search (intent-hq/monorepo#551) ([#462](https://github.com/intent-hq/intentd/pull/462))
- Include script:output in ephemeral event retention sweep (monorepo#620) ([#432](https://github.com/intent-hq/intentd/pull/432))
- Inherit hidden flag across specialist tiers ([#480](https://github.com/intent-hq/intentd/pull/480))
- Use UTF-16 offset kind and recover poisoned CRDT sessions mutex (monorepo#721) ([#487](https://github.com/intent-hq/intentd/pull/487))
- Route line-attribution:updated through the transient publish path (monorepo#720) ([#488](https://github.com/intent-hq/intentd/pull/488))
- Auto-activate incremental auto_vacuum at daemon startup (monorepo#720) ([#500](https://github.com/intent-hq/intentd/pull/500))
- Emit full applied delta on workspace archive/unarchive ([#508](https://github.com/intent-hq/intentd/pull/508))
- Statically link vendored OpenSSL on macOS so packaged intentd runs without Homebrew (intent-hq/monorepo#776)

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

