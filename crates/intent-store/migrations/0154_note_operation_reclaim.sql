-- Durable bounded reclamation queues for backend-local note operations.
-- One-time backfill is proportional to retained metadata; ticks never repeat it.
CREATE TABLE note_operation_reclaim (
 operation_key TEXT PRIMARY KEY REFERENCES note_operation(operation_key) ON DELETE CASCADE,
 due_ms INTEGER NOT NULL CHECK(due_ms>=0),
 mode INTEGER NOT NULL CHECK(mode IN (0,1)),
 step INTEGER NOT NULL DEFAULT 0 CHECK(step BETWEEN 0 AND 15)
);
CREATE INDEX note_operation_reclaim_due ON note_operation_reclaim(due_ms,operation_key);
CREATE TABLE note_stage_root_pin (
 operation_key TEXT PRIMARY KEY REFERENCES note_stage(operation_key) ON DELETE CASCADE,
 root_key TEXT NOT NULL REFERENCES note_stage_root(root_key),
 until_ms INTEGER NOT NULL CHECK(until_ms>=0)
);
CREATE INDEX note_stage_root_pin_deadline ON note_stage_root_pin(root_key,until_ms);
CREATE TABLE note_stage_root_reclaim (
 root_key TEXT PRIMARY KEY REFERENCES note_stage_root(root_key) ON DELETE CASCADE,
 due_ms INTEGER NOT NULL CHECK(due_ms>=0)
);
CREATE INDEX note_stage_root_reclaim_due ON note_stage_root_reclaim(due_ms,root_key);

CREATE TRIGGER note_operation_reclaim_insert AFTER INSERT ON note_operation BEGIN
 INSERT INTO note_operation_reclaim(operation_key,due_ms,mode) VALUES(new.operation_key,new.retain_until*1000,1);
END;
CREATE TRIGGER note_stage_root_reclaim_insert AFTER INSERT ON note_stage_root BEGIN
 INSERT INTO note_stage_root_reclaim(root_key,due_ms) VALUES(new.root_key,0);
END;
CREATE TRIGGER note_stage_pin_insert AFTER INSERT ON note_stage_root_pin BEGIN
 INSERT INTO note_stage_root_reclaim(root_key,due_ms) VALUES(new.root_key,
   (SELECT until_ms FROM note_stage_root_pin WHERE root_key=new.root_key ORDER BY until_ms DESC LIMIT 1))
 ON CONFLICT(root_key) DO UPDATE SET due_ms=excluded.due_ms;
END;
CREATE TRIGGER note_stage_pin_delete AFTER DELETE ON note_stage_root_pin BEGIN
 INSERT INTO note_stage_root_reclaim(root_key,due_ms) VALUES(old.root_key,
   COALESCE((SELECT until_ms FROM note_stage_root_pin WHERE root_key=old.root_key ORDER BY until_ms DESC LIMIT 1),0))
 ON CONFLICT(root_key) DO UPDATE SET due_ms=excluded.due_ms;
END;
-- All deadlines come from validated canonical YYYY-MM-DDTHH:MM:SS.sssZ.
-- Use stored integral seconds plus decimal milliseconds, never floating date math.
CREATE TRIGGER note_stage_reclaim_insert AFTER INSERT ON note_stage BEGIN
 UPDATE note_operation_reclaim SET due_ms=CASE WHEN new.phase='committed' THEN
   (SELECT retain_until*1000 FROM note_operation WHERE operation_key=new.operation_key)
   WHEN new.phase IN ('cancelled','expired') THEN 0 ELSE
   (SELECT admission_expires*1000+CAST(substr(json_extract(outcome,'$.expiresAt'),21,3) AS INTEGER)
    FROM note_operation WHERE operation_key=new.operation_key) END,
   mode=CASE WHEN new.phase='committed' THEN 1 ELSE 0 END,step=0 WHERE operation_key=new.operation_key;
 INSERT INTO note_stage_root_pin(operation_key,root_key,until_ms)
 SELECT new.operation_key,new.root_key,CASE WHEN new.phase='committed' THEN retain_until*1000
   ELSE admission_expires*1000+CAST(substr(json_extract(outcome,'$.expiresAt'),21,3) AS INTEGER) END
 FROM note_operation WHERE operation_key=new.operation_key AND new.phase IN ('staging','sealed','committed');
END;
CREATE TRIGGER note_stage_reclaim_phase AFTER UPDATE OF phase ON note_stage WHEN new.phase!=old.phase BEGIN
 DELETE FROM note_stage_root_pin WHERE operation_key=new.operation_key;
 UPDATE note_operation_reclaim SET due_ms=CASE WHEN new.phase='committed' THEN
   (SELECT retain_until*1000 FROM note_operation WHERE operation_key=new.operation_key)
   WHEN new.phase IN ('cancelled','expired') THEN 0 ELSE
   (SELECT admission_expires*1000+CAST(substr(json_extract(outcome,'$.expiresAt'),21,3) AS INTEGER)
    FROM note_operation WHERE operation_key=new.operation_key) END,
   mode=CASE WHEN new.phase='committed' THEN 1 ELSE 0 END,step=0 WHERE operation_key=new.operation_key;
 INSERT INTO note_stage_root_pin(operation_key,root_key,until_ms)
 SELECT new.operation_key,new.root_key,CASE WHEN new.phase='committed' THEN retain_until*1000
   ELSE admission_expires*1000+CAST(substr(json_extract(outcome,'$.expiresAt'),21,3) AS INTEGER) END
 FROM note_operation WHERE operation_key=new.operation_key AND new.phase IN ('staging','sealed','committed');
END;
CREATE TRIGGER note_stage_reclaim_delete AFTER DELETE ON note_stage BEGIN
 INSERT INTO note_stage_root_reclaim(root_key,due_ms) VALUES(old.root_key,
   COALESCE((SELECT until_ms FROM note_stage_root_pin WHERE root_key=old.root_key ORDER BY until_ms DESC LIMIT 1),0))
 ON CONFLICT(root_key) DO UPDATE SET due_ms=excluded.due_ms;
END;

INSERT INTO note_operation_reclaim(operation_key,due_ms,mode)
 SELECT o.operation_key,CASE WHEN s.phase IN ('cancelled','expired') THEN 0
 WHEN s.phase IN ('staging','sealed') THEN o.admission_expires*1000+CAST(substr(json_extract(o.outcome,'$.expiresAt'),21,3) AS INTEGER)
 ELSE o.retain_until*1000 END,CASE WHEN s.phase IN ('staging','sealed','cancelled','expired') THEN 0 ELSE 1 END
 FROM note_operation o LEFT JOIN note_stage s USING(operation_key);
INSERT INTO note_stage_root_pin(operation_key,root_key,until_ms)
 SELECT s.operation_key,s.root_key,CASE WHEN s.phase='committed' THEN o.retain_until*1000
 ELSE o.admission_expires*1000+CAST(substr(json_extract(o.outcome,'$.expiresAt'),21,3) AS INTEGER) END
 FROM note_stage s JOIN note_operation o USING(operation_key) WHERE s.phase IN ('staging','sealed','committed');
INSERT OR IGNORE INTO note_stage_root_reclaim(root_key,due_ms) SELECT root_key,0 FROM note_stage_root;

-- Copy-on-write uses the same exact live-pin index, preventing expired headers
-- from regenerating drained root pieces through the former second-rounded test.
DROP TRIGGER note_stage_preserve_deleted_piece;
DROP TRIGGER note_stage_preserve_moved_piece;
CREATE TRIGGER note_stage_preserve_deleted_piece BEFORE DELETE ON note_page_piece BEGIN
 INSERT OR IGNORE INTO note_stage_base_piece(root_key,start,end,text)
 SELECT r.root_key,old.start,old.end,old.text FROM note_stage_root r
 WHERE r.workspace_id=old.workspace_id AND r.note_id=old.note_id AND r.content_generation=old.content_generation
 AND EXISTS(SELECT 1 FROM note_stage_root_pin p WHERE p.root_key=r.root_key
 AND p.until_ms>unixepoch()*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER));
END;
CREATE TRIGGER note_stage_preserve_moved_piece BEFORE UPDATE ON note_page_piece BEGIN
 SELECT CASE WHEN old.text IS NOT new.text OR old.start IS NOT new.start OR old.end IS NOT new.end OR old.content_generation IS NOT new.content_generation THEN RAISE(ABORT,'source pieces are replaced, never edited in place') END;
 INSERT OR IGNORE INTO note_stage_base_piece(root_key,start,end,text)
 SELECT r.root_key,old.start,old.end,old.text FROM note_stage_root r
 WHERE r.workspace_id=old.workspace_id AND r.note_id=old.note_id AND r.content_generation=old.content_generation
 AND EXISTS(SELECT 1 FROM note_stage_root_pin p WHERE p.root_key=r.root_key
 AND p.until_ms>unixepoch()*1000+CAST(substr(strftime('%f','now'),4,3) AS INTEGER));
END;
