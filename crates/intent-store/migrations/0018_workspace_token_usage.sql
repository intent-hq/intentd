-- Persist the durable `tokenUsage` workspace field (§5.23 / §19.1). Additive
-- only: 0001–0017 are frozen, so this migration appends a single nullable TEXT
-- column holding the JSON-encoded `TokenUsage { byAgentId, totals, byModel,
-- lastScanAt }` snapshot the daemon-internal scan job materializes. NULL means
-- the field is omitted from the wire (no scan has run yet).
ALTER TABLE workspace ADD COLUMN token_usage TEXT;
