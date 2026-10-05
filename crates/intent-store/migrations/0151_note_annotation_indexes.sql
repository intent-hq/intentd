-- Derived annotation indexes. Canonical comment IDs/markers and the legacy
-- attribution snapshot remain unchanged. No legacy JSON expansion on reads.
CREATE TABLE note_annotation_head (
    id INTEGER PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    note_id TEXT NOT NULL,
    source_rev INTEGER NOT NULL,
    attribution_generation TEXT NOT NULL DEFAULT (lower(hex(randomblob(16)))),
    comment_revision TEXT NOT NULL DEFAULT (lower(hex(randomblob(16)))),
    attribution_rev INTEGER NOT NULL DEFAULT -1,
    anchors_rev INTEGER NOT NULL DEFAULT -1,
    thread_count INTEGER NOT NULL DEFAULT 0,
    comment_count INTEGER NOT NULL DEFAULT 0,
    orphan_thread_count INTEGER NOT NULL DEFAULT 0,
    orphan_comment_count INTEGER NOT NULL DEFAULT 0,
    UNIQUE (workspace_id, note_id),
    FOREIGN KEY (note_id, workspace_id) REFERENCES note(id, workspace_id) ON DELETE CASCADE ON UPDATE CASCADE
);
INSERT INTO note_annotation_head(workspace_id,note_id,source_rev)
    SELECT workspace_id,id,rev FROM note;
CREATE TRIGGER note_annotation_insert AFTER INSERT ON note BEGIN
    INSERT INTO note_annotation_head(workspace_id,note_id,source_rev)
        VALUES (new.workspace_id,new.id,new.rev);
END;
CREATE TRIGGER note_annotation_update AFTER UPDATE ON note BEGIN
    UPDATE note_annotation_head SET source_rev=new.rev,attribution_rev=-1,anchors_rev=-1,
        attribution_generation=lower(hex(randomblob(16))),comment_revision=lower(hex(randomblob(16)))
        WHERE workspace_id=new.workspace_id AND note_id=new.id;
END;

CREATE TABLE note_attribution_line (
    head_id INTEGER NOT NULL REFERENCES note_annotation_head(id) ON DELETE CASCADE,
    line INTEGER NOT NULL CHECK(line > 0),
    start INTEGER NOT NULL CHECK(start >= 0),
    end INTEGER NOT NULL CHECK(end >= start),
    timestamp INTEGER NOT NULL,
    has_author INTEGER NOT NULL,
    PRIMARY KEY (head_id,line)
);
CREATE TABLE note_attribution_author (
    head_id INTEGER NOT NULL,
    line INTEGER NOT NULL,
    author_json TEXT NOT NULL,
    byte_length INTEGER GENERATED ALWAYS AS (length(CAST(author_json AS BLOB))) STORED,
    PRIMARY KEY(head_id,line),
    FOREIGN KEY(head_id,line) REFERENCES note_attribution_line(head_id,line) ON DELETE CASCADE
);
-- Lines are disjoint and ordered; the first intersecting line is found using
-- end, then the admitted source range bounds the remaining rows.
CREATE INDEX note_attribution_extent ON note_attribution_line(head_id,end,start,line);
CREATE INDEX note_attribution_start ON note_attribution_line(head_id,start,line);
-- Older writers can still replace the full map. They must never leave a
-- previously indexed generation looking current. The indexed publisher sets
-- its admitted generation back only after both representations are committed.
CREATE TRIGGER note_annotation_legacy_insert AFTER INSERT ON note_line_attribution BEGIN
    UPDATE note_annotation_head SET attribution_rev=-1,attribution_generation=lower(hex(randomblob(16)))
        WHERE workspace_id=new.workspace_id AND note_id=new.note_id;
END;
CREATE TRIGGER note_annotation_legacy_update AFTER UPDATE ON note_line_attribution BEGIN
    UPDATE note_annotation_head SET attribution_rev=-1,attribution_generation=lower(hex(randomblob(16)))
        WHERE workspace_id=new.workspace_id AND note_id=new.note_id;
END;

CREATE TABLE note_comment_root (
    head_id INTEGER NOT NULL REFERENCES note_annotation_head(id) ON DELETE CASCADE,
    thread_id TEXT NOT NULL,root_comment_id TEXT NOT NULL,root_present INTEGER NOT NULL,
    PRIMARY KEY(head_id,thread_id)
);
CREATE TABLE note_comment_thread (
    head_id INTEGER NOT NULL REFERENCES note_annotation_head(id) ON DELETE CASCADE,
    thread_id TEXT NOT NULL,
    total_comments INTEGER NOT NULL CHECK(total_comments >= 0),
    anchor_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (head_id,thread_id)
);
CREATE INDEX note_comment_orphan_order ON note_comment_thread(head_id,thread_id) WHERE anchor_count=0;
CREATE TRIGGER note_comment_thread_count_insert AFTER INSERT ON note_comment_thread BEGIN
    UPDATE note_annotation_head SET thread_count=thread_count+1,comment_count=comment_count+new.total_comments,
        orphan_thread_count=orphan_thread_count+(new.anchor_count=0),orphan_comment_count=orphan_comment_count+CASE WHEN new.anchor_count=0 THEN new.total_comments ELSE 0 END WHERE id=new.head_id;
END;
CREATE TRIGGER note_comment_thread_count_update AFTER UPDATE ON note_comment_thread BEGIN
    UPDATE note_annotation_head SET comment_count=comment_count+new.total_comments-old.total_comments,
        orphan_thread_count=orphan_thread_count+(new.anchor_count=0)-(old.anchor_count=0),
        orphan_comment_count=orphan_comment_count+CASE WHEN new.anchor_count=0 THEN new.total_comments ELSE 0 END-CASE WHEN old.anchor_count=0 THEN old.total_comments ELSE 0 END WHERE id=new.head_id;
END;
CREATE TRIGGER note_comment_thread_count_delete AFTER DELETE ON note_comment_thread BEGIN
    UPDATE note_annotation_head SET thread_count=thread_count-1,comment_count=comment_count-old.total_comments,
        orphan_thread_count=orphan_thread_count-(old.anchor_count=0),orphan_comment_count=orphan_comment_count-CASE WHEN old.anchor_count=0 THEN old.total_comments ELSE 0 END WHERE id=old.head_id;
END;
CREATE TABLE note_comment_projection (
    comment_id TEXT PRIMARY KEY REFERENCES comment(id) ON DELETE CASCADE,
    head_id INTEGER NOT NULL REFERENCES note_annotation_head(id) ON DELETE CASCADE,
    thread_id TEXT NOT NULL,
    parent_id TEXT,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    preview TEXT NOT NULL,
    truncated INTEGER NOT NULL
);
CREATE INDEX note_comment_reply_order ON note_comment_projection(head_id,thread_id,created_at,comment_id);
CREATE INDEX note_comment_root_order ON note_comment_projection(head_id,thread_id,created_at,comment_id) WHERE parent_id IS NULL;
CREATE TRIGGER note_comment_projection_insert AFTER INSERT ON note_comment_projection BEGIN
    INSERT INTO note_comment_root(head_id,thread_id,root_comment_id,root_present)
        VALUES(new.head_id,new.thread_id,COALESCE(new.parent_id,new.comment_id),new.parent_id IS NULL)
        ON CONFLICT(head_id,thread_id) DO UPDATE SET
            root_comment_id=CASE WHEN new.parent_id IS NULL THEN new.comment_id ELSE root_comment_id END,
            root_present=CASE WHEN new.parent_id IS NULL THEN 1 ELSE root_present END;
    INSERT INTO note_comment_thread(head_id,thread_id,total_comments) VALUES(new.head_id,new.thread_id,1)
        ON CONFLICT(head_id,thread_id) DO UPDATE SET total_comments=total_comments+1;
END;
CREATE TRIGGER note_comment_projection_delete AFTER DELETE ON note_comment_projection BEGIN
    UPDATE note_comment_root SET root_present=0 WHERE head_id=old.head_id AND thread_id=old.thread_id AND root_comment_id=old.comment_id;
    UPDATE note_comment_thread SET total_comments=total_comments-1 WHERE head_id=old.head_id AND thread_id=old.thread_id;
    DELETE FROM note_comment_thread WHERE head_id=old.head_id AND thread_id=old.thread_id AND total_comments=0;
END;
INSERT INTO note_comment_projection
    SELECT c.id,h.id,c.thread_id,c.parent_id,c.status,c.created_at,substr(c.content,1,256),length(CAST(c.content AS BLOB))>length(CAST(substr(c.content,1,256) AS BLOB))
    FROM comment c JOIN note_annotation_head h ON h.workspace_id=c.workspace_id AND h.note_id=c.note_id;

CREATE TRIGGER note_comment_insert AFTER INSERT ON comment BEGIN
    INSERT INTO note_comment_projection
        SELECT new.id,h.id,new.thread_id,new.parent_id,new.status,new.created_at,substr(new.content,1,256),length(CAST(new.content AS BLOB))>length(CAST(substr(new.content,1,256) AS BLOB))
        FROM note_annotation_head h WHERE h.workspace_id=new.workspace_id AND h.note_id=new.note_id;
    UPDATE note_annotation_head SET comment_revision=lower(hex(randomblob(16))),
        anchors_rev=CASE WHEN new.parent_id IS NULL THEN -1 ELSE anchors_rev END
        WHERE workspace_id=new.workspace_id AND note_id=new.note_id;
END;
CREATE TRIGGER note_comment_update AFTER UPDATE ON comment BEGIN
    DELETE FROM note_comment_anchor WHERE comment_id=old.id;
    DELETE FROM note_comment_projection WHERE comment_id=old.id;
    INSERT INTO note_comment_projection
        SELECT new.id,h.id,new.thread_id,new.parent_id,new.status,new.created_at,substr(new.content,1,256),length(CAST(new.content AS BLOB))>length(CAST(substr(new.content,1,256) AS BLOB))
        FROM note_annotation_head h WHERE h.workspace_id=new.workspace_id AND h.note_id=new.note_id;
    UPDATE note_annotation_head SET comment_revision=lower(hex(randomblob(16))),
        anchors_rev=CASE WHEN old.parent_id IS NULL OR new.parent_id IS NULL THEN -1 ELSE anchors_rev END
        WHERE (workspace_id=new.workspace_id AND note_id=new.note_id)
           OR (workspace_id=old.workspace_id AND note_id=old.note_id);
END;
CREATE TRIGGER note_comment_delete AFTER DELETE ON comment BEGIN
    DELETE FROM note_comment_projection WHERE comment_id=old.id;
    UPDATE note_annotation_head SET comment_revision=lower(hex(randomblob(16))),
        anchors_rev=CASE WHEN old.parent_id IS NULL THEN -1 ELSE anchors_rev END
        WHERE workspace_id=old.workspace_id AND note_id=old.note_id;
END;

CREATE TABLE note_comment_anchor (
    id INTEGER PRIMARY KEY,
    head_id INTEGER NOT NULL REFERENCES note_annotation_head(id) ON DELETE CASCADE,
    comment_id TEXT NOT NULL REFERENCES comment(id) ON DELETE CASCADE,
    occurrence_id TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    start INTEGER NOT NULL CHECK(start >= 0),
    end INTEGER NOT NULL CHECK(end >= start),
    UNIQUE(head_id,comment_id,occurrence_id)
);
CREATE INDEX note_comment_anchor_owner ON note_comment_anchor(head_id,comment_id);
-- R-tree bounds are conservative floats. Every overlap read also tests the
-- exact INTEGER coordinates, so rounding cannot alter endpoint semantics.
CREATE VIRTUAL TABLE note_comment_anchor_extent USING rtree(id,scope_min,scope_max,start,end);
CREATE TRIGGER note_comment_anchor_insert AFTER INSERT ON note_comment_anchor BEGIN
    UPDATE note_comment_thread SET anchor_count=anchor_count+1 WHERE head_id=new.head_id AND thread_id=new.thread_id;
    INSERT INTO note_comment_anchor_extent VALUES(new.id,new.head_id,new.head_id,new.start,new.end);
END;
CREATE TRIGGER note_comment_anchor_delete AFTER DELETE ON note_comment_anchor BEGIN
    UPDATE note_comment_thread SET anchor_count=anchor_count-1 WHERE head_id=old.head_id AND thread_id=old.thread_id;
    DELETE FROM note_comment_anchor_extent WHERE id=old.id;
END;
CREATE TRIGGER note_comment_anchor_update AFTER UPDATE ON note_comment_anchor BEGIN
    UPDATE note_comment_thread SET anchor_count=anchor_count-1 WHERE head_id=old.head_id AND thread_id=old.thread_id;
    UPDATE note_comment_thread SET anchor_count=anchor_count+1 WHERE head_id=new.head_id AND thread_id=new.thread_id;
    DELETE FROM note_comment_anchor_extent WHERE id=old.id;
    INSERT INTO note_comment_anchor_extent VALUES(new.id,new.head_id,new.head_id,new.start,new.end);
END;

-- Durable bounded subscription tuples outlive note deletion. The incarnation
-- identity comes from the source page index, never a new annotation identity.
CREATE TABLE note_annotation_state (
    workspace_id TEXT NOT NULL,
    note_id TEXT NOT NULL,
    instance_id TEXT NOT NULL,
    state_generation TEXT NOT NULL DEFAULT '0',
    source_revision TEXT NOT NULL,
    attribution_generation TEXT NOT NULL,
    attribution_ready INTEGER NOT NULL,
    comment_revision TEXT NOT NULL,
    deleted INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(workspace_id,note_id,instance_id),
    CHECK(state_generation NOT GLOB '*[^0-9]*' AND length(state_generation)>0
        AND (state_generation='0' OR substr(state_generation,1,1)<>'0')
        AND (length(state_generation)<20 OR (length(state_generation)=20 AND state_generation<='18446744073709551615')))
);
INSERT INTO note_annotation_state(workspace_id,note_id,instance_id,source_revision,attribution_generation,attribution_ready,comment_revision)
    SELECT a.workspace_id,a.note_id,p.instance_id,'r:'||p.current_rev||':'||p.generation,a.attribution_generation,a.attribution_rev=a.source_rev,a.comment_revision
    FROM note_annotation_head a JOIN note_page_head p USING(workspace_id,note_id);
CREATE TRIGGER note_annotation_state_exhaustion BEFORE UPDATE OF source_revision,attribution_generation,attribution_ready,comment_revision,deleted ON note_annotation_state
WHEN (new.source_revision<>old.source_revision OR new.attribution_generation<>old.attribution_generation
    OR new.attribution_ready<>old.attribution_ready OR new.comment_revision<>old.comment_revision OR new.deleted<>old.deleted)
    AND old.state_generation='18446744073709551615'
BEGIN
    SELECT RAISE(ABORT,'note annotation state generation exhausted');
END;
-- Split decimal arithmetic at nine digits so SQLite never coerces an unsigned
-- 64-bit counter to a lossy float or overflows its signed INTEGER arithmetic.
CREATE TRIGGER note_annotation_state_advance AFTER UPDATE OF source_revision,attribution_generation,attribution_ready,comment_revision,deleted ON note_annotation_state
WHEN new.source_revision<>old.source_revision OR new.attribution_generation<>old.attribution_generation
    OR new.attribution_ready<>old.attribution_ready OR new.comment_revision<>old.comment_revision OR new.deleted<>old.deleted
BEGIN
    UPDATE note_annotation_state SET state_generation=ltrim(
        printf('%011d',CAST(substr(old.state_generation,1,max(length(old.state_generation)-9,0)) AS INTEGER)
            + CASE WHEN CAST(substr(old.state_generation,-9) AS INTEGER)=999999999 THEN 1 ELSE 0 END)
        ||printf('%09d',(CAST(substr(old.state_generation,-9) AS INTEGER)+1)%1000000000),'0')
    WHERE workspace_id=new.workspace_id AND note_id=new.note_id AND instance_id=new.instance_id;
END;
CREATE TRIGGER note_annotation_state_move BEFORE UPDATE OF id,workspace_id ON note
WHEN new.id<>old.id OR new.workspace_id<>old.workspace_id BEGIN
    UPDATE note_annotation_state SET deleted=1 WHERE workspace_id=old.workspace_id AND note_id=old.id AND deleted=0;
END;
CREATE TRIGGER note_annotation_state_delete BEFORE DELETE ON note BEGIN
    UPDATE note_annotation_state SET deleted=1
        WHERE workspace_id=old.workspace_id AND note_id=old.id AND deleted=0;
END;
CREATE TRIGGER note_annotation_head_state_insert AFTER INSERT ON note_annotation_head BEGIN
    INSERT INTO note_annotation_state(workspace_id,note_id,instance_id,source_revision,attribution_generation,attribution_ready,comment_revision)
        SELECT a.workspace_id,a.note_id,p.instance_id,'r:'||p.current_rev||':'||p.generation,a.attribution_generation,a.attribution_rev=a.source_rev,a.comment_revision
        FROM note_annotation_head a JOIN note_page_head p USING(workspace_id,note_id)
        WHERE a.workspace_id=new.workspace_id AND a.note_id=new.note_id
        ON CONFLICT(workspace_id,note_id,instance_id) DO UPDATE SET
            source_revision=excluded.source_revision,attribution_generation=excluded.attribution_generation,
            attribution_ready=excluded.attribution_ready,comment_revision=excluded.comment_revision
        WHERE note_annotation_state.deleted=0;
END;
CREATE TRIGGER note_annotation_head_state_update AFTER UPDATE ON note_annotation_head BEGIN
    INSERT INTO note_annotation_state(workspace_id,note_id,instance_id,source_revision,attribution_generation,attribution_ready,comment_revision)
        SELECT a.workspace_id,a.note_id,p.instance_id,'r:'||p.current_rev||':'||p.generation,a.attribution_generation,a.attribution_rev=a.source_rev,a.comment_revision
        FROM note_annotation_head a JOIN note_page_head p USING(workspace_id,note_id)
        WHERE a.workspace_id=new.workspace_id AND a.note_id=new.note_id
        ON CONFLICT(workspace_id,note_id,instance_id) DO UPDATE SET
            source_revision=excluded.source_revision,attribution_generation=excluded.attribution_generation,
            attribution_ready=excluded.attribution_ready,comment_revision=excluded.comment_revision
        WHERE note_annotation_state.deleted=0;
END;
CREATE TRIGGER note_page_head_state_insert AFTER INSERT ON note_page_head BEGIN
    INSERT INTO note_annotation_state(workspace_id,note_id,instance_id,source_revision,attribution_generation,attribution_ready,comment_revision)
        SELECT a.workspace_id,a.note_id,p.instance_id,'r:'||p.current_rev||':'||p.generation,a.attribution_generation,a.attribution_rev=a.source_rev,a.comment_revision
        FROM note_annotation_head a JOIN note_page_head p USING(workspace_id,note_id)
        WHERE a.workspace_id=new.workspace_id AND a.note_id=new.note_id
        ON CONFLICT(workspace_id,note_id,instance_id) DO UPDATE SET
            source_revision=excluded.source_revision,attribution_generation=excluded.attribution_generation,
            attribution_ready=excluded.attribution_ready,comment_revision=excluded.comment_revision
        WHERE note_annotation_state.deleted=0;
END;
CREATE TRIGGER note_page_head_state_update AFTER UPDATE ON note_page_head BEGIN
    INSERT INTO note_annotation_state(workspace_id,note_id,instance_id,source_revision,attribution_generation,attribution_ready,comment_revision)
        SELECT a.workspace_id,a.note_id,p.instance_id,'r:'||p.current_rev||':'||p.generation,a.attribution_generation,a.attribution_rev=a.source_rev,a.comment_revision
        FROM note_annotation_head a JOIN note_page_head p USING(workspace_id,note_id)
        WHERE a.workspace_id=new.workspace_id AND a.note_id=new.note_id
        ON CONFLICT(workspace_id,note_id,instance_id) DO UPDATE SET
            source_revision=excluded.source_revision,attribution_generation=excluded.attribution_generation,
            attribution_ready=excluded.attribution_ready,comment_revision=excluded.comment_revision
        WHERE note_annotation_state.deleted=0;
END;

-- Full fields are fragmented once on writes. Reads seek fixed-size byte pieces;
-- they never substring or decode an entire comment/author blob. Continuation
-- tokens carry the running UTF-16 offset, avoiding prefix scans on deep pages.
CREATE TABLE note_comment_detail (
    comment_id TEXT NOT NULL REFERENCES comment(id) ON DELETE CASCADE,
    field TEXT NOT NULL,
    byte_length INTEGER NOT NULL,
    is_null INTEGER NOT NULL,
    PRIMARY KEY(comment_id,field)
);
CREATE TABLE note_comment_detail_piece (
    comment_id TEXT NOT NULL,
    field TEXT NOT NULL,
    position INTEGER NOT NULL,
    data BLOB NOT NULL CHECK(length(data)<=1024),
    PRIMARY KEY(comment_id,field,position),
    FOREIGN KEY(comment_id,field) REFERENCES note_comment_detail(comment_id,field) ON DELETE CASCADE
);
    INSERT INTO note_comment_detail
        WITH input AS MATERIALIZED (SELECT c.id AS comment_id,'body' AS field,CAST(COALESCE(c.content,'') AS BLOB) AS data,c.content IS NULL AS is_null FROM comment c UNION ALL SELECT c.id AS comment_id,'author' AS field,CAST(COALESCE(c.author,'') AS BLOB) AS data,c.author IS NULL AS is_null FROM comment c UNION ALL SELECT c.id AS comment_id,'anchor' AS field,CAST(COALESCE(c.anchor_json,'') AS BLOB) AS data,c.anchor_json IS NULL AS is_null FROM comment c UNION ALL SELECT c.id AS comment_id,'anchorText' AS field,CAST(COALESCE(c.anchor_text,'') AS BLOB) AS data,c.anchor_text IS NULL AS is_null FROM comment c UNION ALL SELECT c.id AS comment_id,'startId' AS field,CAST(COALESCE(json_extract(CASE WHEN json_valid(c.anchor_json) THEN c.anchor_json ELSE '{}' END,'$.startId'),'') AS BLOB) AS data,json_extract(CASE WHEN json_valid(c.anchor_json) THEN c.anchor_json ELSE '{}' END,'$.startId') IS NULL AS is_null FROM comment c UNION ALL SELECT c.id AS comment_id,'endId' AS field,CAST(COALESCE(json_extract(CASE WHEN json_valid(c.anchor_json) THEN c.anchor_json ELSE '{}' END,'$.endId'),'') AS BLOB) AS data,json_extract(CASE WHEN json_valid(c.anchor_json) THEN c.anchor_json ELSE '{}' END,'$.endId') IS NULL AS is_null FROM comment c UNION ALL SELECT c.id AS comment_id,'pointId' AS field,CAST(COALESCE(json_extract(CASE WHEN json_valid(c.anchor_json) THEN c.anchor_json ELSE '{}' END,'$.pointId'),'') AS BLOB) AS data,json_extract(CASE WHEN json_valid(c.anchor_json) THEN c.anchor_json ELSE '{}' END,'$.pointId') IS NULL AS is_null FROM comment c UNION ALL SELECT c.id AS comment_id,'extra' AS field,CAST(COALESCE(c.extra_json,'') AS BLOB) AS data,c.extra_json IS NULL AS is_null FROM comment c UNION ALL SELECT c.id AS comment_id,'authorPrincipalId' AS field,CAST(COALESCE(json_extract(CASE WHEN json_valid(c.extra_json) THEN c.extra_json ELSE '{}' END,'$.authorPrincipalId'),'') AS BLOB) AS data,json_extract(CASE WHEN json_valid(c.extra_json) THEN c.extra_json ELSE '{}' END,'$.authorPrincipalId') IS NULL AS is_null FROM comment c UNION ALL SELECT c.id AS comment_id,'provider' AS field,CAST(COALESCE(json_extract(CASE WHEN json_valid(c.extra_json) THEN c.extra_json ELSE '{}' END,'$.authorIdentity.provider'),'') AS BLOB) AS data,json_extract(CASE WHEN json_valid(c.extra_json) THEN c.extra_json ELSE '{}' END,'$.authorIdentity.provider') IS NULL AS is_null FROM comment c UNION ALL SELECT c.id AS comment_id,'host' AS field,CAST(COALESCE(json_extract(CASE WHEN json_valid(c.extra_json) THEN c.extra_json ELSE '{}' END,'$.authorIdentity.host'),'') AS BLOB) AS data,json_extract(CASE WHEN json_valid(c.extra_json) THEN c.extra_json ELSE '{}' END,'$.authorIdentity.host') IS NULL AS is_null FROM comment c UNION ALL SELECT c.id AS comment_id,'externalUserId' AS field,CAST(COALESCE(json_extract(CASE WHEN json_valid(c.extra_json) THEN c.extra_json ELSE '{}' END,'$.authorIdentity.externalUserId'),'') AS BLOB) AS data,json_extract(CASE WHEN json_valid(c.extra_json) THEN c.extra_json ELSE '{}' END,'$.authorIdentity.externalUserId') IS NULL AS is_null FROM comment c)
        SELECT comment_id,field,length(data),is_null FROM input;
    INSERT INTO note_comment_detail_piece
        WITH RECURSIVE input AS MATERIALIZED (SELECT c.id AS comment_id,'body' AS field,CAST(COALESCE(c.content,'') AS BLOB) AS data,c.content IS NULL AS is_null FROM comment c UNION ALL SELECT c.id AS comment_id,'author' AS field,CAST(COALESCE(c.author,'') AS BLOB) AS data,c.author IS NULL AS is_null FROM comment c UNION ALL SELECT c.id AS comment_id,'anchor' AS field,CAST(COALESCE(c.anchor_json,'') AS BLOB) AS data,c.anchor_json IS NULL AS is_null FROM comment c UNION ALL SELECT c.id AS comment_id,'anchorText' AS field,CAST(COALESCE(c.anchor_text,'') AS BLOB) AS data,c.anchor_text IS NULL AS is_null FROM comment c UNION ALL SELECT c.id AS comment_id,'startId' AS field,CAST(COALESCE(json_extract(CASE WHEN json_valid(c.anchor_json) THEN c.anchor_json ELSE '{}' END,'$.startId'),'') AS BLOB) AS data,json_extract(CASE WHEN json_valid(c.anchor_json) THEN c.anchor_json ELSE '{}' END,'$.startId') IS NULL AS is_null FROM comment c UNION ALL SELECT c.id AS comment_id,'endId' AS field,CAST(COALESCE(json_extract(CASE WHEN json_valid(c.anchor_json) THEN c.anchor_json ELSE '{}' END,'$.endId'),'') AS BLOB) AS data,json_extract(CASE WHEN json_valid(c.anchor_json) THEN c.anchor_json ELSE '{}' END,'$.endId') IS NULL AS is_null FROM comment c UNION ALL SELECT c.id AS comment_id,'pointId' AS field,CAST(COALESCE(json_extract(CASE WHEN json_valid(c.anchor_json) THEN c.anchor_json ELSE '{}' END,'$.pointId'),'') AS BLOB) AS data,json_extract(CASE WHEN json_valid(c.anchor_json) THEN c.anchor_json ELSE '{}' END,'$.pointId') IS NULL AS is_null FROM comment c UNION ALL SELECT c.id AS comment_id,'extra' AS field,CAST(COALESCE(c.extra_json,'') AS BLOB) AS data,c.extra_json IS NULL AS is_null FROM comment c UNION ALL SELECT c.id AS comment_id,'authorPrincipalId' AS field,CAST(COALESCE(json_extract(CASE WHEN json_valid(c.extra_json) THEN c.extra_json ELSE '{}' END,'$.authorPrincipalId'),'') AS BLOB) AS data,json_extract(CASE WHEN json_valid(c.extra_json) THEN c.extra_json ELSE '{}' END,'$.authorPrincipalId') IS NULL AS is_null FROM comment c UNION ALL SELECT c.id AS comment_id,'provider' AS field,CAST(COALESCE(json_extract(CASE WHEN json_valid(c.extra_json) THEN c.extra_json ELSE '{}' END,'$.authorIdentity.provider'),'') AS BLOB) AS data,json_extract(CASE WHEN json_valid(c.extra_json) THEN c.extra_json ELSE '{}' END,'$.authorIdentity.provider') IS NULL AS is_null FROM comment c UNION ALL SELECT c.id AS comment_id,'host' AS field,CAST(COALESCE(json_extract(CASE WHEN json_valid(c.extra_json) THEN c.extra_json ELSE '{}' END,'$.authorIdentity.host'),'') AS BLOB) AS data,json_extract(CASE WHEN json_valid(c.extra_json) THEN c.extra_json ELSE '{}' END,'$.authorIdentity.host') IS NULL AS is_null FROM comment c UNION ALL SELECT c.id AS comment_id,'externalUserId' AS field,CAST(COALESCE(json_extract(CASE WHEN json_valid(c.extra_json) THEN c.extra_json ELSE '{}' END,'$.authorIdentity.externalUserId'),'') AS BLOB) AS data,json_extract(CASE WHEN json_valid(c.extra_json) THEN c.extra_json ELSE '{}' END,'$.authorIdentity.externalUserId') IS NULL AS is_null FROM comment c),
        offsets(comment_id,field,position,byte_length) AS (
            SELECT comment_id,field,0,length(data) FROM input WHERE length(data)>0
            UNION ALL SELECT comment_id,field,position+1024,byte_length FROM offsets WHERE position+1024<byte_length
        )
        SELECT o.comment_id,o.field,o.position,substr(i.data,o.position+1,1024)
        FROM offsets o JOIN input i ON i.comment_id=o.comment_id AND i.field=o.field;
CREATE TRIGGER note_comment_detail_insert AFTER INSERT ON comment BEGIN
    INSERT INTO note_comment_detail
        WITH input AS MATERIALIZED (SELECT new.id AS comment_id,'body' AS field,CAST(COALESCE(new.content,'') AS BLOB) AS data,new.content IS NULL AS is_null UNION ALL SELECT new.id AS comment_id,'author' AS field,CAST(COALESCE(new.author,'') AS BLOB) AS data,new.author IS NULL AS is_null UNION ALL SELECT new.id AS comment_id,'anchor' AS field,CAST(COALESCE(new.anchor_json,'') AS BLOB) AS data,new.anchor_json IS NULL AS is_null UNION ALL SELECT new.id AS comment_id,'anchorText' AS field,CAST(COALESCE(new.anchor_text,'') AS BLOB) AS data,new.anchor_text IS NULL AS is_null UNION ALL SELECT new.id AS comment_id,'startId' AS field,CAST(COALESCE(json_extract(CASE WHEN json_valid(new.anchor_json) THEN new.anchor_json ELSE '{}' END,'$.startId'),'') AS BLOB) AS data,json_extract(CASE WHEN json_valid(new.anchor_json) THEN new.anchor_json ELSE '{}' END,'$.startId') IS NULL AS is_null UNION ALL SELECT new.id AS comment_id,'endId' AS field,CAST(COALESCE(json_extract(CASE WHEN json_valid(new.anchor_json) THEN new.anchor_json ELSE '{}' END,'$.endId'),'') AS BLOB) AS data,json_extract(CASE WHEN json_valid(new.anchor_json) THEN new.anchor_json ELSE '{}' END,'$.endId') IS NULL AS is_null UNION ALL SELECT new.id AS comment_id,'pointId' AS field,CAST(COALESCE(json_extract(CASE WHEN json_valid(new.anchor_json) THEN new.anchor_json ELSE '{}' END,'$.pointId'),'') AS BLOB) AS data,json_extract(CASE WHEN json_valid(new.anchor_json) THEN new.anchor_json ELSE '{}' END,'$.pointId') IS NULL AS is_null UNION ALL SELECT new.id AS comment_id,'extra' AS field,CAST(COALESCE(new.extra_json,'') AS BLOB) AS data,new.extra_json IS NULL AS is_null UNION ALL SELECT new.id AS comment_id,'authorPrincipalId' AS field,CAST(COALESCE(json_extract(CASE WHEN json_valid(new.extra_json) THEN new.extra_json ELSE '{}' END,'$.authorPrincipalId'),'') AS BLOB) AS data,json_extract(CASE WHEN json_valid(new.extra_json) THEN new.extra_json ELSE '{}' END,'$.authorPrincipalId') IS NULL AS is_null UNION ALL SELECT new.id AS comment_id,'provider' AS field,CAST(COALESCE(json_extract(CASE WHEN json_valid(new.extra_json) THEN new.extra_json ELSE '{}' END,'$.authorIdentity.provider'),'') AS BLOB) AS data,json_extract(CASE WHEN json_valid(new.extra_json) THEN new.extra_json ELSE '{}' END,'$.authorIdentity.provider') IS NULL AS is_null UNION ALL SELECT new.id AS comment_id,'host' AS field,CAST(COALESCE(json_extract(CASE WHEN json_valid(new.extra_json) THEN new.extra_json ELSE '{}' END,'$.authorIdentity.host'),'') AS BLOB) AS data,json_extract(CASE WHEN json_valid(new.extra_json) THEN new.extra_json ELSE '{}' END,'$.authorIdentity.host') IS NULL AS is_null UNION ALL SELECT new.id AS comment_id,'externalUserId' AS field,CAST(COALESCE(json_extract(CASE WHEN json_valid(new.extra_json) THEN new.extra_json ELSE '{}' END,'$.authorIdentity.externalUserId'),'') AS BLOB) AS data,json_extract(CASE WHEN json_valid(new.extra_json) THEN new.extra_json ELSE '{}' END,'$.authorIdentity.externalUserId') IS NULL AS is_null)
        SELECT comment_id,field,length(data),is_null FROM input;
    INSERT INTO note_comment_detail_piece
        WITH RECURSIVE input AS MATERIALIZED (SELECT new.id AS comment_id,'body' AS field,CAST(COALESCE(new.content,'') AS BLOB) AS data,new.content IS NULL AS is_null UNION ALL SELECT new.id AS comment_id,'author' AS field,CAST(COALESCE(new.author,'') AS BLOB) AS data,new.author IS NULL AS is_null UNION ALL SELECT new.id AS comment_id,'anchor' AS field,CAST(COALESCE(new.anchor_json,'') AS BLOB) AS data,new.anchor_json IS NULL AS is_null UNION ALL SELECT new.id AS comment_id,'anchorText' AS field,CAST(COALESCE(new.anchor_text,'') AS BLOB) AS data,new.anchor_text IS NULL AS is_null UNION ALL SELECT new.id AS comment_id,'startId' AS field,CAST(COALESCE(json_extract(CASE WHEN json_valid(new.anchor_json) THEN new.anchor_json ELSE '{}' END,'$.startId'),'') AS BLOB) AS data,json_extract(CASE WHEN json_valid(new.anchor_json) THEN new.anchor_json ELSE '{}' END,'$.startId') IS NULL AS is_null UNION ALL SELECT new.id AS comment_id,'endId' AS field,CAST(COALESCE(json_extract(CASE WHEN json_valid(new.anchor_json) THEN new.anchor_json ELSE '{}' END,'$.endId'),'') AS BLOB) AS data,json_extract(CASE WHEN json_valid(new.anchor_json) THEN new.anchor_json ELSE '{}' END,'$.endId') IS NULL AS is_null UNION ALL SELECT new.id AS comment_id,'pointId' AS field,CAST(COALESCE(json_extract(CASE WHEN json_valid(new.anchor_json) THEN new.anchor_json ELSE '{}' END,'$.pointId'),'') AS BLOB) AS data,json_extract(CASE WHEN json_valid(new.anchor_json) THEN new.anchor_json ELSE '{}' END,'$.pointId') IS NULL AS is_null UNION ALL SELECT new.id AS comment_id,'extra' AS field,CAST(COALESCE(new.extra_json,'') AS BLOB) AS data,new.extra_json IS NULL AS is_null UNION ALL SELECT new.id AS comment_id,'authorPrincipalId' AS field,CAST(COALESCE(json_extract(CASE WHEN json_valid(new.extra_json) THEN new.extra_json ELSE '{}' END,'$.authorPrincipalId'),'') AS BLOB) AS data,json_extract(CASE WHEN json_valid(new.extra_json) THEN new.extra_json ELSE '{}' END,'$.authorPrincipalId') IS NULL AS is_null UNION ALL SELECT new.id AS comment_id,'provider' AS field,CAST(COALESCE(json_extract(CASE WHEN json_valid(new.extra_json) THEN new.extra_json ELSE '{}' END,'$.authorIdentity.provider'),'') AS BLOB) AS data,json_extract(CASE WHEN json_valid(new.extra_json) THEN new.extra_json ELSE '{}' END,'$.authorIdentity.provider') IS NULL AS is_null UNION ALL SELECT new.id AS comment_id,'host' AS field,CAST(COALESCE(json_extract(CASE WHEN json_valid(new.extra_json) THEN new.extra_json ELSE '{}' END,'$.authorIdentity.host'),'') AS BLOB) AS data,json_extract(CASE WHEN json_valid(new.extra_json) THEN new.extra_json ELSE '{}' END,'$.authorIdentity.host') IS NULL AS is_null UNION ALL SELECT new.id AS comment_id,'externalUserId' AS field,CAST(COALESCE(json_extract(CASE WHEN json_valid(new.extra_json) THEN new.extra_json ELSE '{}' END,'$.authorIdentity.externalUserId'),'') AS BLOB) AS data,json_extract(CASE WHEN json_valid(new.extra_json) THEN new.extra_json ELSE '{}' END,'$.authorIdentity.externalUserId') IS NULL AS is_null),
        offsets(comment_id,field,position,byte_length) AS (
            SELECT comment_id,field,0,length(data) FROM input WHERE length(data)>0
            UNION ALL SELECT comment_id,field,position+1024,byte_length FROM offsets WHERE position+1024<byte_length
        )
        SELECT o.comment_id,o.field,o.position,substr(i.data,o.position+1,1024)
        FROM offsets o JOIN input i ON i.comment_id=o.comment_id AND i.field=o.field;
END;
CREATE TRIGGER note_comment_detail_update AFTER UPDATE OF content,author,anchor_json,anchor_text,extra_json ON comment
WHEN new.content IS NOT old.content OR new.author IS NOT old.author OR new.anchor_json IS NOT old.anchor_json OR new.anchor_text IS NOT old.anchor_text OR new.extra_json IS NOT old.extra_json
BEGIN
    DELETE FROM note_comment_detail WHERE comment_id=old.id;
    INSERT INTO note_comment_detail
        WITH input AS MATERIALIZED (SELECT new.id AS comment_id,'body' AS field,CAST(COALESCE(new.content,'') AS BLOB) AS data,new.content IS NULL AS is_null UNION ALL SELECT new.id AS comment_id,'author' AS field,CAST(COALESCE(new.author,'') AS BLOB) AS data,new.author IS NULL AS is_null UNION ALL SELECT new.id AS comment_id,'anchor' AS field,CAST(COALESCE(new.anchor_json,'') AS BLOB) AS data,new.anchor_json IS NULL AS is_null UNION ALL SELECT new.id AS comment_id,'anchorText' AS field,CAST(COALESCE(new.anchor_text,'') AS BLOB) AS data,new.anchor_text IS NULL AS is_null UNION ALL SELECT new.id AS comment_id,'startId' AS field,CAST(COALESCE(json_extract(CASE WHEN json_valid(new.anchor_json) THEN new.anchor_json ELSE '{}' END,'$.startId'),'') AS BLOB) AS data,json_extract(CASE WHEN json_valid(new.anchor_json) THEN new.anchor_json ELSE '{}' END,'$.startId') IS NULL AS is_null UNION ALL SELECT new.id AS comment_id,'endId' AS field,CAST(COALESCE(json_extract(CASE WHEN json_valid(new.anchor_json) THEN new.anchor_json ELSE '{}' END,'$.endId'),'') AS BLOB) AS data,json_extract(CASE WHEN json_valid(new.anchor_json) THEN new.anchor_json ELSE '{}' END,'$.endId') IS NULL AS is_null UNION ALL SELECT new.id AS comment_id,'pointId' AS field,CAST(COALESCE(json_extract(CASE WHEN json_valid(new.anchor_json) THEN new.anchor_json ELSE '{}' END,'$.pointId'),'') AS BLOB) AS data,json_extract(CASE WHEN json_valid(new.anchor_json) THEN new.anchor_json ELSE '{}' END,'$.pointId') IS NULL AS is_null UNION ALL SELECT new.id AS comment_id,'extra' AS field,CAST(COALESCE(new.extra_json,'') AS BLOB) AS data,new.extra_json IS NULL AS is_null UNION ALL SELECT new.id AS comment_id,'authorPrincipalId' AS field,CAST(COALESCE(json_extract(CASE WHEN json_valid(new.extra_json) THEN new.extra_json ELSE '{}' END,'$.authorPrincipalId'),'') AS BLOB) AS data,json_extract(CASE WHEN json_valid(new.extra_json) THEN new.extra_json ELSE '{}' END,'$.authorPrincipalId') IS NULL AS is_null UNION ALL SELECT new.id AS comment_id,'provider' AS field,CAST(COALESCE(json_extract(CASE WHEN json_valid(new.extra_json) THEN new.extra_json ELSE '{}' END,'$.authorIdentity.provider'),'') AS BLOB) AS data,json_extract(CASE WHEN json_valid(new.extra_json) THEN new.extra_json ELSE '{}' END,'$.authorIdentity.provider') IS NULL AS is_null UNION ALL SELECT new.id AS comment_id,'host' AS field,CAST(COALESCE(json_extract(CASE WHEN json_valid(new.extra_json) THEN new.extra_json ELSE '{}' END,'$.authorIdentity.host'),'') AS BLOB) AS data,json_extract(CASE WHEN json_valid(new.extra_json) THEN new.extra_json ELSE '{}' END,'$.authorIdentity.host') IS NULL AS is_null UNION ALL SELECT new.id AS comment_id,'externalUserId' AS field,CAST(COALESCE(json_extract(CASE WHEN json_valid(new.extra_json) THEN new.extra_json ELSE '{}' END,'$.authorIdentity.externalUserId'),'') AS BLOB) AS data,json_extract(CASE WHEN json_valid(new.extra_json) THEN new.extra_json ELSE '{}' END,'$.authorIdentity.externalUserId') IS NULL AS is_null)
        SELECT comment_id,field,length(data),is_null FROM input;
    INSERT INTO note_comment_detail_piece
        WITH RECURSIVE input AS MATERIALIZED (SELECT new.id AS comment_id,'body' AS field,CAST(COALESCE(new.content,'') AS BLOB) AS data,new.content IS NULL AS is_null UNION ALL SELECT new.id AS comment_id,'author' AS field,CAST(COALESCE(new.author,'') AS BLOB) AS data,new.author IS NULL AS is_null UNION ALL SELECT new.id AS comment_id,'anchor' AS field,CAST(COALESCE(new.anchor_json,'') AS BLOB) AS data,new.anchor_json IS NULL AS is_null UNION ALL SELECT new.id AS comment_id,'anchorText' AS field,CAST(COALESCE(new.anchor_text,'') AS BLOB) AS data,new.anchor_text IS NULL AS is_null UNION ALL SELECT new.id AS comment_id,'startId' AS field,CAST(COALESCE(json_extract(CASE WHEN json_valid(new.anchor_json) THEN new.anchor_json ELSE '{}' END,'$.startId'),'') AS BLOB) AS data,json_extract(CASE WHEN json_valid(new.anchor_json) THEN new.anchor_json ELSE '{}' END,'$.startId') IS NULL AS is_null UNION ALL SELECT new.id AS comment_id,'endId' AS field,CAST(COALESCE(json_extract(CASE WHEN json_valid(new.anchor_json) THEN new.anchor_json ELSE '{}' END,'$.endId'),'') AS BLOB) AS data,json_extract(CASE WHEN json_valid(new.anchor_json) THEN new.anchor_json ELSE '{}' END,'$.endId') IS NULL AS is_null UNION ALL SELECT new.id AS comment_id,'pointId' AS field,CAST(COALESCE(json_extract(CASE WHEN json_valid(new.anchor_json) THEN new.anchor_json ELSE '{}' END,'$.pointId'),'') AS BLOB) AS data,json_extract(CASE WHEN json_valid(new.anchor_json) THEN new.anchor_json ELSE '{}' END,'$.pointId') IS NULL AS is_null UNION ALL SELECT new.id AS comment_id,'extra' AS field,CAST(COALESCE(new.extra_json,'') AS BLOB) AS data,new.extra_json IS NULL AS is_null UNION ALL SELECT new.id AS comment_id,'authorPrincipalId' AS field,CAST(COALESCE(json_extract(CASE WHEN json_valid(new.extra_json) THEN new.extra_json ELSE '{}' END,'$.authorPrincipalId'),'') AS BLOB) AS data,json_extract(CASE WHEN json_valid(new.extra_json) THEN new.extra_json ELSE '{}' END,'$.authorPrincipalId') IS NULL AS is_null UNION ALL SELECT new.id AS comment_id,'provider' AS field,CAST(COALESCE(json_extract(CASE WHEN json_valid(new.extra_json) THEN new.extra_json ELSE '{}' END,'$.authorIdentity.provider'),'') AS BLOB) AS data,json_extract(CASE WHEN json_valid(new.extra_json) THEN new.extra_json ELSE '{}' END,'$.authorIdentity.provider') IS NULL AS is_null UNION ALL SELECT new.id AS comment_id,'host' AS field,CAST(COALESCE(json_extract(CASE WHEN json_valid(new.extra_json) THEN new.extra_json ELSE '{}' END,'$.authorIdentity.host'),'') AS BLOB) AS data,json_extract(CASE WHEN json_valid(new.extra_json) THEN new.extra_json ELSE '{}' END,'$.authorIdentity.host') IS NULL AS is_null UNION ALL SELECT new.id AS comment_id,'externalUserId' AS field,CAST(COALESCE(json_extract(CASE WHEN json_valid(new.extra_json) THEN new.extra_json ELSE '{}' END,'$.authorIdentity.externalUserId'),'') AS BLOB) AS data,json_extract(CASE WHEN json_valid(new.extra_json) THEN new.extra_json ELSE '{}' END,'$.authorIdentity.externalUserId') IS NULL AS is_null),
        offsets(comment_id,field,position,byte_length) AS (
            SELECT comment_id,field,0,length(data) FROM input WHERE length(data)>0
            UNION ALL SELECT comment_id,field,position+1024,byte_length FROM offsets WHERE position+1024<byte_length
        )
        SELECT o.comment_id,o.field,o.position,substr(i.data,o.position+1,1024)
        FROM offsets o JOIN input i ON i.comment_id=o.comment_id AND i.field=o.field;
END;
CREATE TABLE note_attribution_author_piece (
    head_id INTEGER NOT NULL,
    line INTEGER NOT NULL,
    position INTEGER NOT NULL,
    data BLOB NOT NULL CHECK(length(data)<=1024),
    PRIMARY KEY(head_id,line,position),
    FOREIGN KEY(head_id,line) REFERENCES note_attribution_author(head_id,line) ON DELETE CASCADE
);
CREATE TRIGGER note_attribution_author_fragment AFTER INSERT ON note_attribution_author BEGIN
    INSERT INTO note_attribution_author_piece
        WITH RECURSIVE offsets(position,byte_length) AS (
            SELECT 0,length(CAST(new.author_json AS BLOB)) WHERE length(CAST(new.author_json AS BLOB))>0
            UNION ALL SELECT position+1024,byte_length FROM offsets WHERE position+1024<byte_length
        )
        SELECT new.head_id,new.line,position,substr(CAST(new.author_json AS BLOB),position+1,1024) FROM offsets;
END;

CREATE TABLE note_annotation_snapshot (id TEXT PRIMARY KEY,expires_ms INTEGER NOT NULL,payload TEXT NOT NULL);
CREATE INDEX note_annotation_snapshot_expiry ON note_annotation_snapshot(expires_ms,id);

-- At most four live prepared range queries are admitted by the store. Their
-- narrow rows retain at most one entry per current scoped thread per lease.
-- Storage grows at most four times the source index; no result-cardinality cap.
CREATE TABLE note_annotation_match_head (
    snapshot_id TEXT PRIMARY KEY REFERENCES note_annotation_snapshot(id) ON DELETE CASCADE,
    total_threads INTEGER NOT NULL,total_comments INTEGER NOT NULL,prepare_steps INTEGER NOT NULL,
    head_id INTEGER NOT NULL REFERENCES note_annotation_head(id) ON DELETE CASCADE
);
CREATE TABLE note_annotation_match (
    snapshot_id TEXT NOT NULL REFERENCES note_annotation_match_head(snapshot_id) ON DELETE CASCADE,
    thread_id TEXT NOT NULL CHECK(length(CAST(thread_id AS BLOB))<=256),position INTEGER NOT NULL,
    PRIMARY KEY(snapshot_id,position,thread_id)
);

CREATE TRIGGER note_annotation_match_invalidate AFTER UPDATE OF source_rev,comment_revision ON note_annotation_head
WHEN new.source_rev<>old.source_rev OR new.comment_revision<>old.comment_revision BEGIN
    DELETE FROM note_annotation_match_head WHERE head_id=new.id;
END;
CREATE INDEX note_annotation_match_owner ON note_annotation_match_head(head_id);
