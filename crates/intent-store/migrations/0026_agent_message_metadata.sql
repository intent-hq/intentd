-- `messageMetadata` from `agent.sendMessage` / `agent.forceMessage` (PROTOCOL
-- §5.5): opaque per-message JSON payload the FE attaches to the user message
-- (e.g. `{ source: "system" }`); persisted as-is and round-tripped on
-- transcript reads. Nullable so existing rows and daemon-side writes without
-- metadata (queue-drained turns, assistant/tool messages) keep the current
-- shape.

ALTER TABLE agent_message ADD COLUMN metadata TEXT;
