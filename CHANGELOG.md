# Changelog

All notable changes to this project will be documented in this file.

## [0.6.3] - 2026-08-09

### 🐛 Bug Fixes

- *(services)* Apply report_delivered filter to waiting projections ([#1017](https://github.com/intent-hq/intentd/pull/1017))
- *(store,services)* Guard last_activity against full-row clobber and persist derived value ([#1018](https://github.com/intent-hq/intentd/pull/1018))
- *(services)* Derive delegation-group subscription linkage from grouped watches in diagnostics ([#1016](https://github.com/intent-hq/intentd/pull/1016))
- *(services)* Gate turn-end unread raise on top-level foreground agents ([#1021](https://github.com/intent-hq/intentd/pull/1021))

### ⚡ Performance

- *(services)* Dedup pr.monitor forge fetches per (repo, pr) within a sweep ([#1020](https://github.com/intent-hq/intentd/pull/1020))


## [0.6.2] - 2026-08-09

### 🚀 Features

- *(store)* Drop preview self-heal fallback, refuse newer-schema databases ([#1001](https://github.com/intent-hq/intentd/pull/1001))
- *(agents)* Unify PR-monitor waiting into external-wait classification ([#1002](https://github.com/intent-hq/intentd/pull/1002))
- *(intent-services)* Single-flight the full accept-changes.getStatus build ([#1008](https://github.com/intent-hq/intentd/pull/1008))
- Surface waitingOnPrMonitors on the wire (mirrors waitingOnHooks) ([#1007](https://github.com/intent-hq/intentd/pull/1007))
- Resolve quickActions.* daemon-side for agent.completeOnce ([#1012](https://github.com/intent-hq/intentd/pull/1012))
- Cap outstanding slow-path RPCs with -32011 overload rejection ([#1013](https://github.com/intent-hq/intentd/pull/1013))

### 🐛 Bug Fixes

- Tier rpc_profile duration budget for network-bound methods ([#1004](https://github.com/intent-hq/intentd/pull/1004))
- Never stage submodule-internal paths in the superproject (commit/stage guards) ([#1009](https://github.com/intent-hq/intentd/pull/1009))
- Rename backgroundAgents.* to quickActions.* and scope to quick actions ([#1010](https://github.com/intent-hq/intentd/pull/1010))
- Refuse submodule-internal paths in git.discard ([#1011](https://github.com/intent-hq/intentd/pull/1011))
- Remove RpcLimiter Default impl and warn when the overload cap is disabled ([#1014](https://github.com/intent-hq/intentd/pull/1014))

### 🔧 Refactor

- *(transport)* Share envelope-validity rules between router and dispatch pre-check ([#1015](https://github.com/intent-hq/intentd/pull/1015))


## [0.6.1] - 2026-08-08

### 🚀 Features

- Ws.pr.monitor — centralized PR monitoring with merge-requirements checklist
- Provider-neutral agent.completeOnce via ephemeral ACP sessions ([#991](https://github.com/intent-hq/intentd/pull/991))
- Persist and serve session-discovered reasoning-effort levels ([#992](https://github.com/intent-hq/intentd/pull/992))
- Cache provider discovery results and prewarm login-shell PATH ([#994](https://github.com/intent-hq/intentd/pull/994))

### 🐛 Bug Fixes

- *(git)* Resolve inherited origin in CoW checkout provisioning ([#996](https://github.com/intent-hq/intentd/pull/996))
- *(test)* Retry writes in gitignore-suppression e2e under load ([#998](https://github.com/intent-hq/intentd/pull/998))

### 🧪 Testing

- *(services)* Hermetic unit coverage for the unresolvable-adapter unavailable envelope ([#993](https://github.com/intent-hq/intentd/pull/993))


## [0.6.0] - 2026-08-07

### 🚀 Features

- Spread a turn's tokens across the minutes it ran ([#969](https://github.com/intent-hq/intentd/pull/969))
- *(context)* Honor ~/.augment/auggie-path marker in auggie discovery ([#939](https://github.com/intent-hq/intentd/pull/939))
- *(acp)* Derive tool names from Claude Code mcp__<server>__<tool> titles ([#935](https://github.com/intent-hq/intentd/pull/935))
- [**breaking**] Remove inert workspace.autoFetch setting ([#924](https://github.com/intent-hq/intentd/pull/924))
- Raise hook name cap to 50 chars for human-readable names ([#929](https://github.com/intent-hq/intentd/pull/929))
- *(script)* Persist was-running marker and expose previouslyRunning ([#932](https://github.com/intent-hq/intentd/pull/932))
- Status-neutral commit policy and clearer auto-commit-disabled rejection ([#926](https://github.com/intent-hq/intentd/pull/926))
- Kill all daemon-owned PTY sessions on graceful shutdown ([#940](https://github.com/intent-hq/intentd/pull/940))
- *(usage)* Capture ACP usage_update cost in TokenUsage
- Repo cache + cache-hydrated workspace creation ([#944](https://github.com/intent-hq/intentd/pull/944))
- First-class reasoningEffort session field with generic ACP application (protocol 5.2) ([#946](https://github.com/intent-hq/intentd/pull/946))
- Name specialist default model in delegate model-option hints ([#958](https://github.com/intent-hq/intentd/pull/958))
- Emit throttled activity pings for tool-call updates ([#957](https://github.com/intent-hq/intentd/pull/957))
- *(events)* Hybrid file:* persistence; remove event.recentFiles/directoryChanges
- Extend displayStatus into the BE-owned canonical rollup ([#945](https://github.com/intent-hq/intentd/pull/945))
- *(transport)* [**breaking**] Bump protocol version to 6.0 for event method removals
- *(agents)* Persistent question hold across plain user messages ([#965](https://github.com/intent-hq/intentd/pull/965))
- *(settings)* Add model.defaultReasoningEffort setting ([#970](https://github.com/intent-hq/intentd/pull/970))
- Surface ACP agent_thought_chunk as thinking blocks and thoughtTokens usage ([#973](https://github.com/intent-hq/intentd/pull/973))
- Auto-resume agent turns after host sleep ([#972](https://github.com/intent-hq/intentd/pull/972))
- *(services)* Apply model.defaultReasoningEffort at agent creation ([#974](https://github.com/intent-hq/intentd/pull/974))
- Carry thoughtTokens in per-minute usage rate history ([#976](https://github.com/intent-hq/intentd/pull/976))
- Per-turn agent state snapshot — ws.agent.snapshot op, MCP tool, turn-prompt injection, stateSnapshot setting ([#971](https://github.com/intent-hq/intentd/pull/971))
- *(providers)* Probe auggie auth via token print, drop checkAuggie version ([#977](https://github.com/intent-hq/intentd/pull/977))
- Perpetual background hooks that re-arm after dispatch until TTL ([#979](https://github.com/intent-hq/intentd/pull/979))
- *(events)* Emit task:created on every task creation path ([#978](https://github.com/intent-hq/intentd/pull/978))
- Serve model-catalog cache indefinitely; probe only on miss or forceRefresh ([#987](https://github.com/intent-hq/intentd/pull/987))
- Optional providerId param on agent.setModel ([#986](https://github.com/intent-hq/intentd/pull/986))

### 🐛 Bug Fixes

- *(store)* Bound the token-usage fallback message read ([#954](https://github.com/intent-hq/intentd/pull/954))
- *(services)* Reword questions-dismissed notice to informative-only ([#930](https://github.com/intent-hq/intentd/pull/930))
- *(services)* Persist D13 effective model to resolved_model instead of rewriting session.model ([#941](https://github.com/intent-hq/intentd/pull/941))
- *(events)* Defer OS watch registration off the caller's thread ([#952](https://github.com/intent-hq/intentd/pull/952))
- *(events)* Stop workspace watchers on archive, restart on unarchive
- Settle agent-waiting groups when a report_delivered watch retires ([#980](https://github.com/intent-hq/intentd/pull/980))
- *(acp)* Shell-wrap terminal/create for Grok-style packed commands
- Derive pr.snapshot review decision from reviewDecision, not mergeable_state
- *(sourcecontrol)* Stop double-unwrapping the GraphQL data envelope ([#949](https://github.com/intent-hq/intentd/pull/949))
- Close workspace displayStatus audit gaps (G1-G9) ([#928](https://github.com/intent-hq/intentd/pull/928))
- *(acp)* Answer delivered MCP bridge calls with non-retryable outcome-unknown error on TCP drop ([#937](https://github.com/intent-hq/intentd/pull/937))
- State terminal retirement in hook dispatch/eviction wake messages ([#933](https://github.com/intent-hq/intentd/pull/933))
- Support subscribe-style eventType globs in event.query ([#938](https://github.com/intent-hq/intentd/pull/938))
- Persist derived lastActivity in the debounce task ([#959](https://github.com/intent-hq/intentd/pull/959))
- Duplicate standalone-checkout workspaces as standalone checkouts ([#956](https://github.com/intent-hq/intentd/pull/956))
- Scope ws.hook.cancel to the owning agent ([#953](https://github.com/intent-hq/intentd/pull/953))
- *(intentd)* Bind listeners before slow startup initializations
- Exclude the calling agent from the archive interrupt sweep ([#950](https://github.com/intent-hq/intentd/pull/950))
- Run the archive post-persist tail detached so a hook-initiated archive still emits ([#968](https://github.com/intent-hq/intentd/pull/968))

### 🔧 Refactor

- *(services)* Extract central workspace_status module ([#925](https://github.com/intent-hq/intentd/pull/925))

### ⚡ Performance

- *(events)* Consolidate FSEvents streams into shared watchers with in-process demux
- Coalesce concurrent git status scans per worktree ([#982](https://github.com/intent-hq/intentd/pull/982))
- Event-invalidated per-worktree git.status cache

### 🧪 Testing

- *(services)* Make models.list legacy-path tests hermetic via fetch seam ([#934](https://github.com/intent-hq/intentd/pull/934))
- Split agent.watch lifecycle e2e and clamp budgets under nextest kill ([#947](https://github.com/intent-hq/intentd/pull/947))

### ⚙️ Miscellaneous Tasks

- Bump pinned ACP adapters (claude-agent-acp 0.66.0, codex-acp 1.1.14, pi-acp 0.0.33) ([#983](https://github.com/intent-hq/intentd/pull/983))


## [0.5.0] - 2026-08-06

### 🚀 Features

- Sync device-flow token into gh CLI on authorize ([#914](https://github.com/intent-hq/intentd/pull/914))
- *(acp)* Workspace_api discoverability — capability index, ws.help(), hook guidance, pr.snapshot repo override ([#911](https://github.com/intent-hq/intentd/pull/911))
- Clarify ws.app.question.ask hints and stabilize timing tests ([#917](https://github.com/intent-hq/intentd/pull/917))
- *(agents)* Add flushQueuedMessages mode setting (all/systemOnly/off)
- Interrupt agents and cancel hooks on workspace archive; eager hook-task abort on delete ([#896](https://github.com/intent-hq/intentd/pull/896))
- *(voice)* Structured voice-no-api-key error data on voice.transcribe ([#902](https://github.com/intent-hq/intentd/pull/902))
- ModelOptions frontmatter list on specialist definitions ([#900](https://github.com/intent-hq/intentd/pull/900))
- *(voice)* Add voice.language setting as transcription language fallback ([#901](https://github.com/intent-hq/intentd/pull/901))
- Surface specialist modelOptions in the workspace_api delegate docs ([#908](https://github.com/intent-hq/intentd/pull/908))
- *(agent)* Agent.markSeen per-conversation seen marker (protocol 4.5)
- *(events)* Suppress gitignored paths in the file watcher ([#903](https://github.com/intent-hq/intentd/pull/903))
- *(search)* Rank archived-workspace matches below active in search.messages ([#906](https://github.com/intent-hq/intentd/pull/906))
- Log gh CLI out of github.com on github.revoke when its token matches ([#915](https://github.com/intent-hq/intentd/pull/915))
- Shrink ws.pr.* to snapshot and ws.git.* to commit; point agents at gh/git CLIs ([#918](https://github.com/intent-hq/intentd/pull/918))
- Persist interruption reason on interrupted rows and stream:end ([#919](https://github.com/intent-hq/intentd/pull/919))
- [**breaking**] Remove 11 caller-less pr.* router methods; bump protocol to 5.0
- [**breaking**] Remove model tiers and default-provider designation ([#922](https://github.com/intent-hq/intentd/pull/922))
- Workspace-derived vocabulary for voice dictation ([#920](https://github.com/intent-hq/intentd/pull/920))

### 🐛 Bug Fixes

- *(services)* Publish subscriptions-changed on resume watch re-registration ([#904](https://github.com/intent-hq/intentd/pull/904))
- Stop bumping updated_at in markSeen/dismissAttention ([#905](https://github.com/intent-hq/intentd/pull/905))
- Seal after_all groups on queue-idle regardless of hooks ([#909](https://github.com/intent-hq/intentd/pull/909))
- Scope attention writes to the attention column ([#912](https://github.com/intent-hq/intentd/pull/912))
- Load nvm in interactive terminals ([#898](https://github.com/intent-hq/intentd/pull/898))
- Defer completion-watch settlement for idle children waiting on other agents ([#913](https://github.com/intent-hq/intentd/pull/913))
- Resolve agent.delegate provider from configured default, not Auggie ([#910](https://github.com/intent-hq/intentd/pull/910))

### 🧪 Testing

- Make opencode bridge-server test host-independent ([#899](https://github.com/intent-hq/intentd/pull/899))

### ⚙️ Miscellaneous Tasks

- Bump rand from 0.9.5 to 0.10.2 ([#771](https://github.com/intent-hq/intentd/pull/771))
- Bump rquickjs from 0.9.0 to 0.12.2 ([#770](https://github.com/intent-hq/intentd/pull/770))
- Bump tokio-tungstenite from 0.23.1 to 0.30.0 ([#769](https://github.com/intent-hq/intentd/pull/769))


## [0.4.2] - 2026-08-04

### 🚀 Features

- Promote displayStatus to in_progress for top-level agents with child completion watches ([#891](https://github.com/intent-hq/intentd/pull/891))
- Add ws.pr.snapshot for hook-based PR monitoring ([#887](https://github.com/intent-hq/intentd/pull/887))
- *(services)* Bridge file:* events to debounced changes:git-status ([#882](https://github.com/intent-hq/intentd/pull/882))
- *(intentd)* Warn on RPC dispatches exceeding statement or duration budgets ([#884](https://github.com/intent-hq/intentd/pull/884))
- On-demand workspace.diskUsage method; drop diskUsage from list/get ([#886](https://github.com/intent-hq/intentd/pull/886))
- Retire modelTier from specialist frontmatter and resolution ([#889](https://github.com/intent-hq/intentd/pull/889))
- Agent feature toggles in config.toml ([agentFeatures]) ([#890](https://github.com/intent-hq/intentd/pull/890))
- Voice.transcribe RPC with pluggable speech-to-text providers ([#893](https://github.com/intent-hq/intentd/pull/893))
- Notify model on agent.dismissQuestions ([#892](https://github.com/intent-hq/intentd/pull/892))

### ⚡ Performance

- Thin agent.list, add listActive, cap diskUsage walks ([#881](https://github.com/intent-hq/intentd/pull/881))


## [0.4.1] - 2026-08-03

### 🚀 Features

- Hint renderable chat blocks in agent instructions ([#874](https://github.com/intent-hq/intentd/pull/874))
- Hint nav-link blocks in agent instructions ([#877](https://github.com/intent-hq/intentd/pull/877))
- Flush queued messages into one combined turn on idle ([#876](https://github.com/intent-hq/intentd/pull/876))

### 🐛 Bug Fixes

- Derive tool name from dot-separated codex MCP titles ([#869](https://github.com/intent-hq/intentd/pull/869))
- Write per-agent config files under <data_dir>/agent-configs instead of the OS temp dir ([#871](https://github.com/intent-hq/intentd/pull/871))

### 🧪 Testing

- Eliminate remaining test temp residuals (RAII guards, sqlite sidecars, sockets, node caches) ([#872](https://github.com/intent-hq/intentd/pull/872))


## [0.4.0] - 2026-08-03

### 🚀 Features

- Windows named-pipe local transport (listener + CLI client) ([#855](https://github.com/intent-hq/intentd/pull/855))
- Add restarting script status for auto-restart backoff window ([#861](https://github.com/intent-hq/intentd/pull/861))
- [**breaking**] Terminal.list returns { terminals, daemonBootId } envelope ([#862](https://github.com/intent-hq/intentd/pull/862))
- Add structured error.data.code discriminator to -32602 errors ([#863](https://github.com/intent-hq/intentd/pull/863))
- Add host.createDirectory RPC for remote folder creation ([#864](https://github.com/intent-hq/intentd/pull/864))
- Add error.data.code discriminator to fast-path -32602 errors ([#865](https://github.com/intent-hq/intentd/pull/865))

### 🐛 Bug Fixes

- Keep displayStatus in_progress while workspace owns active hooks ([#856](https://github.com/intent-hq/intentd/pull/856))

### 📚 Documentation

- *(agents)* Explain turn idle timeout in hook guidance and timeout warning ([#858](https://github.com/intent-hq/intentd/pull/858))
- *(agents)* Advise estimating hook ttlMs instead of defaulting to the cap ([#860](https://github.com/intent-hq/intentd/pull/860))

### 🧪 Testing

- *(services)* Make in-flight hook TTL expiry test deterministic ([#866](https://github.com/intent-hq/intentd/pull/866))


## [0.3.0] - 2026-08-02

### 🚀 Features

- Queue-aware retire-on-completion delivery for ungrouped watches ([#836](https://github.com/intent-hq/intentd/pull/836))
- Register hardwareConsole.state settings bag key ([#853](https://github.com/intent-hq/intentd/pull/853))
- Stamp queueInfo metadata on drained queue entries ([#834](https://github.com/intent-hq/intentd/pull/834))
- Full-text search over agent chat transcripts ([#845](https://github.com/intent-hq/intentd/pull/845))
- Expose physical workspace disk usage on workspace.list/get ([#849](https://github.com/intent-hq/intentd/pull/849))
- Background hook scheduler with console capture ([#850](https://github.com/intent-hq/intentd/pull/850))
- Daemon-owned specialist default-model resolution with provider guards ([#852](https://github.com/intent-hq/intentd/pull/852))
- [**breaking**] Hook state, 60-min TTL, and hook-aware parent settlement ([#854](https://github.com/intent-hq/intentd/pull/854))

### 🐛 Bug Fixes

- *(services)* Redeliver stranded completion watch after queue retraction/edit ([#841](https://github.com/intent-hq/intentd/pull/841))
- Interim coordinator idle no longer seals the after_all group ([#842](https://github.com/intent-hq/intentd/pull/842))
- Busy-aware interim classification for completion delivery and sealing ([#846](https://github.com/intent-hq/intentd/pull/846))
- *(pty)* Escalate to SIGKILL when the process group is non-empty after grace ([#847](https://github.com/intent-hq/intentd/pull/847))
- *(services)* Reap group stragglers before recording a script exit ([#851](https://github.com/intent-hq/intentd/pull/851))

### 🔧 Refactor

- Remove one_shot from completion-watch registry, registration paths, and store ([#832](https://github.com/intent-hq/intentd/pull/832))
- Drop oneShot from subscription wire payloads and stale watch docs ([#837](https://github.com/intent-hq/intentd/pull/837))

### 🧪 Testing

- Deflake specialists/skills watcher drain helpers ([#839](https://github.com/intent-hq/intentd/pull/839))
- Stop leaking test temp dirs (/tmp) across the suite ([#843](https://github.com/intent-hq/intentd/pull/843))
- Fix residual temp-file leaks (intent-ctx/intent-host, sqlite sidecars, teardown races) ([#848](https://github.com/intent-hq/intentd/pull/848))


## [0.2.16] - 2026-08-01

### 🚀 Features

- Persist and serve lastMessageRole on AgentLite ([#807](https://github.com/intent-hq/intentd/pull/807))
- Sticky attention-state for child/background agents + failure timestamps ([#810](https://github.com/intent-hq/intentd/pull/810))
- Agent-facing queue visibility, single-pending-message guard, dequeue annotation ([#816](https://github.com/intent-hq/intentd/pull/816))
- Restrict agent event subscriptions and add ws.agent.watch/unwatch (intent-hq/monorepo#1229)
- Adjustable workspace_api output limit + TOON encoding ([#819](https://github.com/intent-hq/intentd/pull/819))
- Needs_attention workspace displayStatus ([#825](https://github.com/intent-hq/intentd/pull/825))

### 🐛 Bug Fixes

- Omit exited and script-owned PTYs from terminal.list ([#745](https://github.com/intent-hq/intentd/pull/745))

### 🧪 Testing

- Wss e2e for foreground automatic-delivery attention negative case (monorepo#1237)
- Make pool-contention stress budgets co-tenancy-safe (monorepo#1239) ([#818](https://github.com/intent-hq/intentd/pull/818))


## [0.2.15] - 2026-07-31

### 🚀 Features

- Honor workspace setup settings (worktrees location, per-workspace auto-commit, commit-policy prompts) ([#744](https://github.com/intent-hq/intentd/pull/744))
- *(transport)* Emit user-row deltas on chat.subscribe ([#747](https://github.com/intent-hq/intentd/pull/747))
- Warn-and-continue on prompt idle timeout instead of terminal failure
- Agent attention requests (requestDiscussion/reportBlocker, blocked task status) ([#754](https://github.com/intent-hq/intentd/pull/754))
- Hold automatic deliveries while an agent's question is pending ([#751](https://github.com/intent-hq/intentd/pull/751))
- Scoped cancel for agent.cancelSubscriptions ([#759](https://github.com/intent-hq/intentd/pull/759))
- Rename agent:stream:chunk broadcast to content-free agent:stream:activity with leading-edge 1s throttle ([#775](https://github.com/intent-hq/intentd/pull/775))
- Serve-time synthetic block ids + appMessageId on user-row chat deltas (monorepo#1114, monorepo#1157)
- Overlay live-turn text into AgentLite lastAgentResponse/digest ([#786](https://github.com/intent-hq/intentd/pull/786))
- Per-minute token-rate history and agentSummary parentAgentId (protocol 2.8/2.9) ([#789](https://github.com/intent-hq/intentd/pull/789))
- Carry optional parentAgentId on agent:attention-requested and agent:failed ([#788](https://github.com/intent-hq/intentd/pull/788))
- Carry live preview fields on agent:stream:activity ([#792](https://github.com/intent-hq/intentd/pull/792))
- Derive idle/running agent activity into displayStatus ([#793](https://github.com/intent-hq/intentd/pull/793))

### 🐛 Bug Fixes

- Renumber workspace auto-commit migration to 0067 (intent-hq/monorepo#1126) ([#752](https://github.com/intent-hq/intentd/pull/752))
- *(store)* Retry SQLITE_BUSY on note read path (monorepo#1139) ([#783](https://github.com/intent-hq/intentd/pull/783))
- Await supervisor teardown in script remove/upsert/start to prevent PTY orphans (monorepo#1180)
- Do not clip final text block closed by a tool-call boundary ([#796](https://github.com/intent-hq/intentd/pull/796))
- *(services)* Generation-stamp script registry entries to close the start remove+recreate identity-confusion race (monorepo#1194) ([#801](https://github.com/intent-hq/intentd/pull/801))
- Select Windows-native shells and provider shims (intent-hq/monorepo#1054)
- *(intent-git)* Use COPYFILE_CLONE_FORCE for per-file CoW clone (intent-hq/monorepo#1124) ([#782](https://github.com/intent-hq/intentd/pull/782))
- Normalize spaced workspace-mcp bridge path ([#736](https://github.com/intent-hq/intentd/pull/736))
- Deliver attention-request parent wake immediately in after_all groups ([#758](https://github.com/intent-hq/intentd/pull/758))
- Emit FILE_CHANGED before fs/write_text_file response to close attribution TOCTOU (intent-hq/monorepo#1144)
- Honor INTENTD_TCP_PORT=0 as ephemeral port for the secure WSS boot bind ([#737](https://github.com/intent-hq/intentd/pull/737))
- Stop workspace list CPU thrash and oversized git.diffs wire frames ([#743](https://github.com/intent-hq/intentd/pull/743))
- Enforce one active completion watch per (parent, child) ([#761](https://github.com/intent-hq/intentd/pull/761))
- Suppress SUB-1 child→parent auto-watch and carry row metadata on message deltas ([#773](https://github.com/intent-hq/intentd/pull/773))
- Make script.run cancellation-safe and guard concurrent runs (monorepo#1155) ([#777](https://github.com/intent-hq/intentd/pull/777))
- Agent.diagnostics taskNoteId filter matches assigned agents (monorepo#1150) ([#765](https://github.com/intent-hq/intentd/pull/765))
- Treat workspace prStatus as a PR-stage signal in displayStatus ([#760](https://github.com/intent-hq/intentd/pull/760))
- Guard agent.delegate and task.assignAgent against double-delegating an occupied task ([#774](https://github.com/intent-hq/intentd/pull/774))
- Retain pending attention request across automatic deliveries ([#785](https://github.com/intent-hq/intentd/pull/785))
- Single-flight git.diffs walks, rate-limit slow-walk warn, normalize absolute paths ([#790](https://github.com/intent-hq/intentd/pull/790))
- Clip mid-turn live previews at the last completed newline ([#795](https://github.com/intent-hq/intentd/pull/795))
- Poll session idle status in idle-timeout e2e test (monorepo#1164) ([#799](https://github.com/intent-hq/intentd/pull/799))
- Persist row-level messageMetadata on wake deliveries ([#802](https://github.com/intent-hq/intentd/pull/802))

### 📚 Documentation

- *(acp)* Document why prompt idle timeout must not be raised ([#740](https://github.com/intent-hq/intentd/pull/740))

### ⚡ Performance

- *(store)* Persist last-message previews at write time ([#742](https://github.com/intent-hq/intentd/pull/742))
- *(services)* Cache agent.list message projections until append ([#776](https://github.com/intent-hq/intentd/pull/776))
- *(git)* Use clonefile(2) for whole-tree CoW fast path (monorepo#1125)

### 🧪 Testing

- Add services-level literal paths tests for git.diffs (monorepo#1078) ([#734](https://github.com/intent-hq/intentd/pull/734))
- Adopt _logged WSS readiness pollers in e2e_wss_runtime_control ([#748](https://github.com/intent-hq/intentd/pull/748))
- Assert appMessageId on fresh chat.subscribe snapshot user rows (monorepo#1157) ([#791](https://github.com/intent-hq/intentd/pull/791))

### ⚙️ Miscellaneous Tasks

- Update Cargo.toml dependencies
- Remove dead agent stream event constants ([#756](https://github.com/intent-hq/intentd/pull/756))


## [0.2.14] - 2026-07-29

### 🔧 Refactor

- Rename sandbox.* wire surface to sandbox.cow.* ([#730](https://github.com/intent-hq/intentd/pull/730))


## [0.2.13] - 2026-07-29

### 🚀 Features

- Add ask-tool hint to top-level agent system prompts ([#721](https://github.com/intent-hq/intentd/pull/721))
- *(intent-git)* Single-pass index-to-workdir diff with hunks and pathspec pruning ([#705](https://github.com/intent-hq/intentd/pull/705))
- Derive omitted agent name from specialist display name ([#710](https://github.com/intent-hq/intentd/pull/710))
- *(transport)* Accept paths[] narrowing on git.diffs and prune the diff walk ([#715](https://github.com/intent-hq/intentd/pull/715))

### 🐛 Bug Fixes

- *(services)* ProviderAuthStatus install gate honors providers.paths overrides ([#725](https://github.com/intent-hq/intentd/pull/725))
- Retarget providers.paths[\unsloth\] to the unsloth CLI ([#707](https://github.com/intent-hq/intentd/pull/707))
- ProviderDiscovery installed status honors providers.paths overrides ([#717](https://github.com/intent-hq/intentd/pull/717))
- *(intent-services)* Flush event-bus writer immediately when idle ([#718](https://github.com/intent-hq/intentd/pull/718))

### 🔧 Refactor

- Remove unused file-tracking.sync/.init/.load wire methods ([#704](https://github.com/intent-hq/intentd/pull/704))

### 📚 Documentation

- *(intent-services)* Clarify exit/state vs chunk ordering doc comments ([#720](https://github.com/intent-hq/intentd/pull/720))

### ⚡ Performance

- *(services)* Adopt single-pass index-to-workdir diff in build_diffs and compute_and_store ([#709](https://github.com/intent-hq/intentd/pull/709))


## [0.2.12] - 2026-07-29

### 🚀 Features

- Add providers.catalog RPC serving the intent-providers registry (monorepo#928) ([#694](https://github.com/intent-hq/intentd/pull/694))
- Add atomic agent.sendQueuedMessageNow RPC; remove agent.forceMessage ([#696](https://github.com/intent-hq/intentd/pull/696))
- Turn correlation id (turnId) on agent lifecycle events, queue entries, and RPC responses ([#699](https://github.com/intent-hq/intentd/pull/699))
- Surface secondaryResolvedPath in host.providerDiscovery ([#701](https://github.com/intent-hq/intentd/pull/701))

### 🐛 Bug Fixes

- Thread combined-delivery prepend_* fields through enqueue_message (intent-hq/monorepo#1034) ([#693](https://github.com/intent-hq/intentd/pull/693))

### 🧪 Testing

- Add append-failure auto-queue prepend regression test ([#703](https://github.com/intent-hq/intentd/pull/703))


## [0.2.11] - 2026-07-28

### 🚀 Features

- *(services)* Warn live unsloth agents before a model-switch restart ([#647](https://github.com/intent-hq/intentd/pull/647))
- *(services)* Port-conflict detection for the managed unsloth server ([#660](https://github.com/intent-hq/intentd/pull/660))
- *(unsloth)* Add unsloth.status and unsloth.stop daemon RPCs ([#623](https://github.com/intent-hq/intentd/pull/623))
- *(services)* Real event.subscribe delivery with batching and restart persistence (monorepo#937) ([#632](https://github.com/intent-hq/intentd/pull/632))
- Event-subscription introspection + workspace-delete cleanup (monorepo#947) ([#644](https://github.com/intent-hq/intentd/pull/644))
- Add isWaitingForOtherAgents to the agent:idle payload ([#651](https://github.com/intent-hq/intentd/pull/651))
- *(services)* Expose secondary-binary status in host.providerDiscovery ([#668](https://github.com/intent-hq/intentd/pull/668))
- Remove model.workspaceOverrides setting layer ([#669](https://github.com/intent-hq/intentd/pull/669))
- Workspace status screenshot — statusImageAssetId + ws.workspace.setStatusImage ([#671](https://github.com/intent-hq/intentd/pull/671))
- *(store)* Sweep high-churn state-notification event families (72h retention) ([#677](https://github.com/intent-hq/intentd/pull/677))

### 🐛 Bug Fixes

- Report the platform file manager as always installed on macOS/Windows ([#655](https://github.com/intent-hq/intentd/pull/655))
- Renumber duplicate migration version 0062 to 0063 ([#674](https://github.com/intent-hq/intentd/pull/674))
- Default interactive terminal TERM for Backspace erase ([#952](https://github.com/intent-hq/intentd/pull/952)) ([#638](https://github.com/intent-hq/intentd/pull/638))
- Demote agentCommit unattributed-dirty skip log to debug ([#645](https://github.com/intent-hq/intentd/pull/645))
- Pre-gate wakeOrCreate watch scope before side effects (monorepo#932)
- *(services)* Skip wakeOrCreate SUB-1 watch and pre-gate for a deleted caller ([#667](https://github.com/intent-hq/intentd/pull/667))
- *(metrics)* Stop double counting shared paths across agent attribution rows (monorepo#1009) ([#683](https://github.com/intent-hq/intentd/pull/683))
- *(usage-stats)* Stop re-recording shared-path growth on row updates (monorepo#1023) ([#689](https://github.com/intent-hq/intentd/pull/689))
- *(acp)* Unwrap codex nested MCP tool-call arguments in session mapping
- Stamp sender attribution on wakeOrCreate context message (monorepo#1015) ([#681](https://github.com/intent-hq/intentd/pull/681))
- Recreate ACP session on retry of poisoned session (monorepo#940)
- Decouple CoW sandbox provisioning from the delegate critical path ([#636](https://github.com/intent-hq/intentd/pull/636))
- Filter agent-initiated agentCommit to the agent's attributed paths (intent-hq/monorepo#939)
- Honor isNewRepo in workspace.create — initialize repository before provisioning
- Doctor names the actually-missing binary for dual-binary providers ([#653](https://github.com/intent-hq/intentd/pull/653))
- Multi-agent attribution rows + directory-rename attribution (monorepo#957) ([#670](https://github.com/intent-hq/intentd/pull/670))
- Deliver preempted message combined with interrupt on zero-output interrupt ([#685](https://github.com/intent-hq/intentd/pull/685))
- Annotate suspected-stall completions in parent wakes (monorepo#1016) ([#688](https://github.com/intent-hq/intentd/pull/688))

### 🔧 Refactor

- *(services)* Use UNKNOWN_PROVIDER alias for the stats provider fallback ([#654](https://github.com/intent-hq/intentd/pull/654))

### ⚡ Performance

- *(store)* Index + keys-only window for agent message projections (monorepo#1010) ([#673](https://github.com/intent-hq/intentd/pull/673))
- *(store)* Per-session message projection for agent.get (monorepo#981) ([#659](https://github.com/intent-hq/intentd/pull/659))
- *(services)* Run CoW sandbox clone on the blocking pool ([#656](https://github.com/intent-hq/intentd/pull/656))
- *(store)* Bound agent.list projection payload via SQL text-block extraction ([#679](https://github.com/intent-hq/intentd/pull/679))
- Eliminate multi-core CPU burn on large repos (diff rollup, adaptive TTL, pushed-check) ([#648](https://github.com/intent-hq/intentd/pull/648))
- Bound agent read paths — stop hydrating full transcripts (monorepo#958)
- Cap persisted agent:tool:call payloads at 16KiB and drop TTL to 6h ([#680](https://github.com/intent-hq/intentd/pull/680))

### 🧪 Testing

- Make flaky card-aggregates ordering and token-usage scan tests deterministic ([#658](https://github.com/intent-hq/intentd/pull/658))
- Make flaky provider-models CLI and agent-ops/unsloth timing tests deterministic under load ([#663](https://github.com/intent-hq/intentd/pull/663))
- *(intentd)* Quiesce activity before paired lastActivity reads (monorepo#1004) ([#682](https://github.com/intent-hq/intentd/pull/682))


## [0.2.10] - 2026-07-27

### 🚀 Features

- Daemon-managed Unsloth server lifecycle ([#597](https://github.com/intent-hq/intentd/pull/597))
- *(unsloth)* Select best-fitting GGUF quant variant at spawn time ([#610](https://github.com/intent-hq/intentd/pull/610))
- CoW provisioning phase timings + configurable clone exclusions ([#614](https://github.com/intent-hq/intentd/pull/614))
- Model-change transcript notice + cross-provider replay e2e coverage (monorepo#882) ([#598](https://github.com/intent-hq/intentd/pull/598))
- Inject scoped GitHub credential helper into terminal and agent spawn environments ([#601](https://github.com/intent-hq/intentd/pull/601))
- BE-owned Workspace.displayStatus with change event ([#600](https://github.com/intent-hq/intentd/pull/600))
- Background retry sweep for merge_pending sandboxes ([#608](https://github.com/intent-hq/intentd/pull/608))
- Daemon-backed git credential helper for terminal and agent spawns ([#618](https://github.com/intent-hq/intentd/pull/618))

### 🐛 Bug Fixes

- *(unsloth)* Preserve in-flight startup across mint timeouts and spawn retries (monorepo#878) ([#621](https://github.com/intent-hq/intentd/pull/621))
- *(providers)* Require unsloth CLI alongside opencode for unsloth provider discovery ([#622](https://github.com/intent-hq/intentd/pull/622))
- *(acp)* Hold stdin lines racing a pending mcp-bridge reconnect ([#620](https://github.com/intent-hq/intentd/pull/620))
- *(acp)* Make mcp-bridge resilient to daemon restarts and TCP drops ([#871](https://github.com/intent-hq/intentd/pull/871)) ([#595](https://github.com/intent-hq/intentd/pull/595))
- Allow cross-provider agent.setModel after first turn (monorepo#882) ([#604](https://github.com/intent-hq/intentd/pull/604))
- *(acp)* Buffer stdin during mcp-bridge initial connect window (monorepo#908) ([#611](https://github.com/intent-hq/intentd/pull/611))
- *(services)* Subscribe caller to completion on wakeOrCreate created_new branch ([#627](https://github.com/intent-hq/intentd/pull/627))
- Skip foreign session/load after a committed cross-provider setModel ([#625](https://github.com/intent-hq/intentd/pull/625))

### 🧪 Testing

- *(services)* Fix flaky dismiss_attention_idempotent event-order race (monorepo#905)
- *(unsloth)* Cover retry-attach across a model switch (monorepo#878) ([#628](https://github.com/intent-hq/intentd/pull/628))


## [0.2.9] - 2026-07-27

### 🚀 Features

- Unsloth provider registry entry + HF GGUF catalog with memory-fit filtering ([#593](https://github.com/intent-hq/intentd/pull/593))

### 🐛 Bug Fixes

- Robust sandbox merge-back and faster best-effort CoW clone ([#592](https://github.com/intent-hq/intentd/pull/592))

### ⚙️ Miscellaneous Tasks

- Fail PRs containing committed git conflict markers (#588 incident) ([#591](https://github.com/intent-hq/intentd/pull/591))


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

