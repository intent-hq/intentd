-- Perpetual hooks: a `{ dispatch: true }` run wakes the owner without
-- retiring the hook — it returns to `scheduled` and keeps running on its
-- cadence until TTL expiry, cancel, or eviction. `dispatch_count` tracks how
-- many runs dispatched so the expiry notice can report accurate counts.
--
-- Purely additive (no CHECK constraint change, unlike 0078), so plain ALTERs
-- suffice. Legacy rows read as one-shot with no dispatches.

ALTER TABLE hook ADD COLUMN perpetual INTEGER NOT NULL DEFAULT 0;
ALTER TABLE hook ADD COLUMN dispatch_count INTEGER NOT NULL DEFAULT 0;
