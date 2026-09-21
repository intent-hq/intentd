-- Per-workspace browser-client pin (REV-2, spec "Wire additions"): the
-- logical `client.id` agent-initiated `browser.exec` requests for the
-- workspace are routed to (`workspace.setBrowserClient`). NULL = unpinned
-- (first-connected eligible client). No FK: the pin may outlive the client
-- row's usefulness (client offline) and must still surface as "pinned but
-- not connected" rather than silently fall back.
ALTER TABLE workspace ADD COLUMN browser_client_id TEXT;
