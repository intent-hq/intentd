-- Close the agent-session field gaps vs the FE `UnifiedPersistence` writer
-- (P3-1.2b): the delegation/report metadata (`completionReport`,
-- `completionReportTimestamp`, `delegationDepth`, `initialMessage`) plus the
-- session-level `contextReferences` / `imageBlocks` the FE persisted on disk.
-- JSON payloads (`context_references`, `image_blocks`) are stored as TEXT.
-- All columns stay NULL for pre-existing rows. Transient streaming flags
-- (`isResponding`, `currentStreamId`) are intentionally NOT given columns —
-- they are runtime-only and must never be persisted (FE `performAtomicWrite`
-- scrub parity).
ALTER TABLE agent_session ADD COLUMN completion_report TEXT;
ALTER TABLE agent_session ADD COLUMN completion_report_timestamp TEXT;
ALTER TABLE agent_session ADD COLUMN delegation_depth INTEGER;
ALTER TABLE agent_session ADD COLUMN initial_message TEXT;
ALTER TABLE agent_session ADD COLUMN context_references TEXT;
ALTER TABLE agent_session ADD COLUMN image_blocks TEXT;
