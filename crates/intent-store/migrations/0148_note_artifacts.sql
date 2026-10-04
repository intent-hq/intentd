-- Prepared private native-artifact bookkeeping. No method/capability is enabled
-- by this migration. Jobs deliberately do not cascade with note deletion: their
-- physical storage and cleanup charges must outlive revoked source authority.
CREATE TABLE note_artifact_source (
    workspace_id TEXT NOT NULL,
    note_id TEXT NOT NULL,
    native_collection TEXT NOT NULL,
    source_collection TEXT NOT NULL,
    native_position INTEGER NOT NULL DEFAULT 0 CHECK (native_position=0),
    source_position INTEGER NOT NULL DEFAULT 0 CHECK (source_position=0),
    primitive TEXT NOT NULL CHECK (primitive IN ('diff','mermaid')),
    PRIMARY KEY (workspace_id,note_id,native_collection),
    FOREIGN KEY (workspace_id,note_id) REFERENCES note_page_head(workspace_id,note_id)
        ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (workspace_id,note_id,native_collection,native_position)
        REFERENCES note_page_entry(workspace_id,note_id,collection,position) ON DELETE CASCADE,
    FOREIGN KEY (workspace_id,note_id,source_collection,source_position)
        REFERENCES note_page_entry(workspace_id,note_id,collection,position) ON DELETE CASCADE
);

-- Server-configured finite limits; no default quota or native-size limit is
-- invented here. All three scopes must exist before any job can be admitted.
-- These are logical reservations. The file/arena owner must separately meter
-- actual allocated pages/WAL/temp; logical reclaim is not a disk-space receipt.
CREATE TABLE note_artifact_capacity (
    scope_kind TEXT NOT NULL CHECK (scope_kind IN ('global','principal','workspace')),
    scope_id TEXT NOT NULL,
    payload_limit INTEGER NOT NULL CHECK (payload_limit BETWEEN 1 AND 9007199254740991),
    record_limit INTEGER NOT NULL CHECK (record_limit BETWEEN 1 AND 9007199254740991),
    index_limit INTEGER NOT NULL CHECK (index_limit BETWEEN 1 AND 9007199254740991),
    storage_limit INTEGER NOT NULL CHECK (storage_limit BETWEEN 1 AND 9007199254740991),
    job_limit INTEGER NOT NULL CHECK (job_limit BETWEEN 1 AND 9007199254740991),
    payload_reserved INTEGER NOT NULL DEFAULT 0 CHECK (payload_reserved BETWEEN 0 AND payload_limit),
    records_reserved INTEGER NOT NULL DEFAULT 0 CHECK (records_reserved BETWEEN 0 AND record_limit),
    indexes_reserved INTEGER NOT NULL DEFAULT 0 CHECK (indexes_reserved BETWEEN 0 AND index_limit),
    storage_reserved INTEGER NOT NULL DEFAULT 0 CHECK (storage_reserved BETWEEN 0 AND storage_limit),
    jobs_reserved INTEGER NOT NULL DEFAULT 0 CHECK (jobs_reserved BETWEEN 0 AND job_limit),
    PRIMARY KEY (scope_kind,scope_id),
    CHECK (scope_kind<>'global' OR scope_id='')
);

CREATE TABLE note_artifact_job (
    principal TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    generation TEXT NOT NULL UNIQUE,
    runtime_id TEXT NOT NULL,
    header_digest TEXT NOT NULL CHECK (length(header_digest)=64),
    header TEXT NOT NULL CHECK (length(CAST(header AS BLOB)) <= 16384),
    source_snapshot TEXT NOT NULL,
    source_revision TEXT NOT NULL,
    note_id TEXT NOT NULL,
    note_instance_id TEXT NOT NULL,
    source_collection TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('building','sealed','admitted','aborted','expired')),
    expires_at INTEGER NOT NULL,
    status_until INTEGER NOT NULL CHECK (status_until >= expires_at),
    payload_limit INTEGER NOT NULL CHECK (payload_limit BETWEEN 1 AND 9007199254740991),
    record_limit INTEGER NOT NULL CHECK (record_limit BETWEEN 1 AND 9007199254740991),
    index_limit INTEGER NOT NULL CHECK (index_limit BETWEEN 1 AND 9007199254740991),
    storage_limit INTEGER NOT NULL CHECK (storage_limit BETWEEN 1 AND 9007199254740991),
    next_sequence INTEGER NOT NULL DEFAULT 0 CHECK (next_sequence BETWEEN 0 AND record_limit),
    accepted_bytes INTEGER NOT NULL DEFAULT 0 CHECK (accepted_bytes BETWEEN 0 AND payload_limit),
    index_entries INTEGER NOT NULL DEFAULT 0 CHECK (index_entries BETWEEN 0 AND index_limit),
    storage_charge INTEGER NOT NULL DEFAULT 0 CHECK (storage_charge BETWEEN 0 AND storage_limit),
    current_digest TEXT NOT NULL CHECK (length(current_digest)=64),
    final_manifest INTEGER NOT NULL DEFAULT 0 CHECK (final_manifest IN (0,1)),
    cleanup_complete INTEGER NOT NULL DEFAULT 0 CHECK (cleanup_complete IN (0,1)),
    PRIMARY KEY (principal,workspace_id,job_id),
    CHECK (cleanup_complete=0 OR state IN ('aborted','expired','admitted'))
);
CREATE INDEX note_artifact_job_expiry ON note_artifact_job(expires_at,generation)
    WHERE state IN ('building','sealed','admitted');
CREATE INDEX note_artifact_job_cleanup ON note_artifact_job(generation)
    WHERE state IN ('aborted','expired') AND cleanup_complete=0;
CREATE TRIGGER note_artifact_job_reserve BEFORE INSERT ON note_artifact_job BEGIN
    SELECT CASE WHEN new.state<>'building' OR new.next_sequence<>0 OR new.accepted_bytes<>0
        OR new.index_entries<>0 OR new.storage_charge<>0 OR new.final_manifest<>0
        OR new.cleanup_complete<>0 OR new.current_digest<>new.header_digest
    THEN RAISE(ABORT,'invalid initial artifact job') END;
    SELECT CASE WHEN (SELECT count(*) FROM note_artifact_capacity c WHERE
        ((c.scope_kind='global' AND c.scope_id='') OR
         (c.scope_kind='principal' AND c.scope_id=new.principal) OR
         (c.scope_kind='workspace' AND c.scope_id=new.workspace_id))
        AND c.payload_limit-c.payload_reserved>=new.payload_limit
        AND c.record_limit-c.records_reserved>=new.record_limit
        AND c.index_limit-c.indexes_reserved>=new.index_limit
        AND c.storage_limit-c.storage_reserved>=new.storage_limit
        AND c.jobs_reserved<c.job_limit)<>3
    THEN RAISE(ABORT,'artifact reservation capacity unavailable') END;
END;
CREATE TRIGGER note_artifact_job_charge AFTER INSERT ON note_artifact_job BEGIN
    UPDATE note_artifact_capacity SET
        payload_reserved=payload_reserved+new.payload_limit,
        records_reserved=records_reserved+new.record_limit,
        indexes_reserved=indexes_reserved+new.index_limit,
        storage_reserved=storage_reserved+new.storage_limit,
        jobs_reserved=jobs_reserved+1
    WHERE (scope_kind='global' AND scope_id='') OR
          (scope_kind='principal' AND scope_id=new.principal) OR
          (scope_kind='workspace' AND scope_id=new.workspace_id);
END;
CREATE TRIGGER note_artifact_job_identity BEFORE UPDATE OF principal,workspace_id,job_id,generation,
    runtime_id,header_digest,header,source_snapshot,source_revision,note_id,note_instance_id,
    source_collection,expires_at,status_until,payload_limit,record_limit,index_limit,storage_limit
    ON note_artifact_job BEGIN
    SELECT RAISE(ABORT,'artifact job identity and reservation are immutable');
END;
CREATE TRIGGER note_artifact_job_reclaim AFTER UPDATE OF cleanup_complete ON note_artifact_job
WHEN old.cleanup_complete=0 AND new.cleanup_complete=1 BEGIN
    UPDATE note_artifact_capacity SET
        payload_reserved=payload_reserved-old.payload_limit,
        records_reserved=records_reserved-old.record_limit,
        indexes_reserved=indexes_reserved-old.index_limit,
        storage_reserved=storage_reserved-old.storage_limit,
        jobs_reserved=jobs_reserved-1
    WHERE (scope_kind='global' AND scope_id='') OR
          (scope_kind='principal' AND scope_id=old.principal) OR
          (scope_kind='workspace' AND scope_id=old.workspace_id);
END;
CREATE TRIGGER note_artifact_job_no_reclaim_undo BEFORE UPDATE OF cleanup_complete ON note_artifact_job
WHEN old.cleanup_complete=1 AND new.cleanup_complete=0 BEGIN
    SELECT RAISE(ABORT,'artifact cleanup cannot be undone');
END;

-- Exact accepted record bytes are kept in a bounded entry, never concatenated
-- for status/replay. Profile indexes and physical allocation need separate
-- admission/accounting; these logical counters do not claim freed disk space.
CREATE TABLE note_artifact_record (
    generation TEXT NOT NULL REFERENCES note_artifact_job(generation),
    sequence INTEGER NOT NULL CHECK (sequence BETWEEN 0 AND 9007199254740991),
    previous_digest TEXT NOT NULL CHECK (length(previous_digest)=64),
    digest TEXT NOT NULL CHECK (length(digest)=64),
    record TEXT NOT NULL CHECK (length(CAST(record AS BLOB)) BETWEEN 1 AND 16384),
    index_charge INTEGER NOT NULL CHECK (index_charge BETWEEN 0 AND 9007199254740991),
    storage_charge INTEGER NOT NULL CHECK (storage_charge BETWEEN 0 AND 9007199254740991),
    is_manifest INTEGER NOT NULL CHECK (is_manifest IN (0,1)),
    PRIMARY KEY (generation,sequence)
);
-- Original append ACK counters are retained per record. A retry is a point
-- lookup, never a SUM over preceding output or the job's later current state.
CREATE TABLE note_artifact_ack (
    generation TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    accepted_bytes INTEGER NOT NULL CHECK (accepted_bytes BETWEEN 1 AND 9007199254740991),
    PRIMARY KEY (generation,sequence),
    FOREIGN KEY (generation,sequence) REFERENCES note_artifact_record(generation,sequence)
        ON DELETE CASCADE
);
CREATE TRIGGER note_artifact_ack_immutable BEFORE UPDATE ON note_artifact_ack BEGIN
    SELECT RAISE(ABORT,'accepted artifact acknowledgement is immutable');
END;
CREATE TRIGGER note_artifact_record_admit BEFORE INSERT ON note_artifact_record BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1 FROM note_artifact_job j WHERE j.generation=new.generation
        AND j.state='building' AND j.final_manifest=0
        AND j.next_sequence=new.sequence AND j.current_digest=new.previous_digest
        AND j.next_sequence<j.record_limit
        AND length(CAST(new.record AS BLOB))<=j.payload_limit-j.accepted_bytes
        AND new.index_charge<=j.index_limit-j.index_entries
        AND new.storage_charge<=j.storage_limit-j.storage_charge
    ) THEN RAISE(ABORT,'artifact record admission failed') END;
END;
CREATE TRIGGER note_artifact_record_account AFTER INSERT ON note_artifact_record BEGIN
    INSERT INTO note_artifact_ack(generation,sequence,accepted_bytes)
        SELECT new.generation,new.sequence,accepted_bytes+length(CAST(new.record AS BLOB))
        FROM note_artifact_job WHERE generation=new.generation;
    UPDATE note_artifact_job SET next_sequence=next_sequence+1,
        accepted_bytes=accepted_bytes+length(CAST(new.record AS BLOB)),
        index_entries=index_entries+new.index_charge,
        storage_charge=storage_charge+new.storage_charge,
        current_digest=new.digest,final_manifest=new.is_manifest
        WHERE generation=new.generation;
END;

CREATE TABLE note_artifact_lease (
    generation TEXT PRIMARY KEY REFERENCES note_artifact_job(generation),
    admission_id TEXT NOT NULL,
    lease_id TEXT NOT NULL UNIQUE,
    final_digest TEXT NOT NULL CHECK (length(final_digest)=64),
    expires_at INTEGER NOT NULL,
    released INTEGER NOT NULL DEFAULT 0 CHECK (released IN (0,1))
);
CREATE TRIGGER note_artifact_lease_admit BEFORE INSERT ON note_artifact_lease BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1 FROM note_artifact_job j WHERE j.generation=new.generation
        AND j.state='sealed' AND j.final_manifest=1
        AND j.current_digest=new.final_digest AND j.expires_at=new.expires_at
    ) THEN RAISE(ABORT,'artifact lease admission failed') END;
END;
CREATE TRIGGER note_artifact_lease_account AFTER INSERT ON note_artifact_lease BEGIN
    UPDATE note_artifact_job SET state='admitted' WHERE generation=new.generation;
END;
CREATE TRIGGER note_artifact_job_retire AFTER UPDATE OF state ON note_artifact_job
WHEN new.state IN ('aborted','expired') BEGIN
    UPDATE note_artifact_lease SET released=1 WHERE generation=new.generation;
END;
CREATE TRIGGER note_artifact_job_transition BEFORE UPDATE OF state ON note_artifact_job
WHEN new.state<>old.state BEGIN
    SELECT CASE WHEN NOT (
        (old.state='building' AND new.state IN ('sealed','aborted','expired')) OR
        (old.state='sealed' AND new.state IN ('admitted','aborted','expired')) OR
        (old.state='admitted' AND new.state IN ('aborted','expired'))
    ) THEN RAISE(ABORT,'invalid artifact state transition') END;
    SELECT CASE WHEN new.state='sealed' AND (new.final_manifest<>1 OR new.next_sequence=0)
        THEN RAISE(ABORT,'artifact final manifest required') END;
END;
CREATE TRIGGER note_artifact_record_immutable BEFORE UPDATE ON note_artifact_record BEGIN
    SELECT RAISE(ABORT,'accepted artifact record is immutable');
END;
CREATE TRIGGER note_artifact_lease_no_revive BEFORE UPDATE OF released ON note_artifact_lease
WHEN old.released=1 AND new.released=0 BEGIN
    SELECT RAISE(ABORT,'retired artifact lease cannot revive');
END;

-- An admitted job retains its historical state after ordinary consumer release.
-- Reclaim still needs explicit physical-owner acknowledgement; a live lease may
-- never be refunded merely because the job has a retained admission receipt.
CREATE TRIGGER note_artifact_job_reclaim_retired_lease
BEFORE UPDATE OF cleanup_complete ON note_artifact_job
WHEN new.cleanup_complete=1 AND new.state='admitted' BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1 FROM note_artifact_lease l
        WHERE l.generation=new.generation AND l.released=1
    ) THEN RAISE(ABORT,'artifact consumer lease is still active') END;
END;
