-- The intent-store Postgres schema, in one file (version 1).
--
-- While nobody runs Postgres, change this file in place: add or change
-- tables, columns, indexes and functions here, in the section they belong
-- to. Do not add numbered migration files, and do not write data
-- migrations. A schema built from an earlier version of this file fails the
-- schema gate and the migrate job (the file's checksum changed); recreate
-- it (drop and create the schema, then run the migrate job).
--
-- `migrate` runs each statement on its own (Neki cannot plan a statement
-- against a relation created earlier in the same transaction), and replays
-- one after a crash, so every statement must be safe to run twice
-- (`IF [NOT] EXISTS`, `CREATE OR REPLACE`). postgres_tenant_schema_lint
-- keeps every table, key and trigger tenant-scoped.
--
-- Sections: SQLite compatibility functions, tables (with their indexes and
-- foreign keys), triggers, tenant provisioning.

-- SQLite compatibility functions for the multi-tenant Postgres backend. Store
-- SQL keeps SQLite's JSON1 semantics on Postgres by calling these instead of
-- the SQLite built-ins. All are IMMUTABLE (except intent_now_iso and
-- sqlite_julianday, which read the clock) so expression and partial indexes can use them. Each
-- pins its search_path: Postgres 17+ builds indexes with a restricted one.

-- Parse JSON text; NULL when malformed (SQLite json_valid is false).
CREATE OR REPLACE FUNCTION intent_json(t text) RETURNS jsonb
LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE
SET search_path FROM CURRENT AS $$
BEGIN
    RETURN t::jsonb;
EXCEPTION WHEN others THEN
    RETURN NULL;
END
$$;

-- '$.a.b[2]' -> '{a,b,2}'. Only the path shapes the store uses.
CREATE OR REPLACE FUNCTION intent_json_path(p text) RETURNS text[]
LANGUAGE sql IMMUTABLE PARALLEL SAFE
SET search_path FROM CURRENT AS $$
    SELECT CASE WHEN p = '$' THEN ARRAY[]::text[]
        ELSE string_to_array(
            regexp_replace(regexp_replace(substr(p, 2), '\[(\d+)\]', '.\1', 'g'), '^\.', ''),
            '.')
    END
$$;

CREATE OR REPLACE FUNCTION sqlite_json_valid(t text) RETURNS boolean
LANGUAGE sql IMMUTABLE PARALLEL SAFE
SET search_path FROM CURRENT AS $$
    SELECT t IS NOT NULL AND intent_json(t) IS NOT NULL
$$;

-- SQLite json_extract: SQL scalar for scalars (text here), JSON text for
-- objects/arrays, NULL for JSON null or a missing path.
CREATE OR REPLACE FUNCTION sqlite_json_extract(t text, p text) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE
SET search_path FROM CURRENT AS $$
    SELECT CASE jsonb_typeof(v)
        WHEN 'null' THEN NULL
        WHEN 'string' THEN v #>> '{}'
        WHEN 'boolean' THEN CASE WHEN v = 'true'::jsonb THEN '1' ELSE '0' END
        ELSE v::text
    END
    FROM (SELECT intent_json(t) #> intent_json_path(p) AS v) s
$$;

-- SQLite json_type names: null, true, false, integer, real, text, array, object.
CREATE OR REPLACE FUNCTION sqlite_json_type(t text, p text DEFAULT '$') RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE
SET search_path FROM CURRENT AS $$
    SELECT CASE jsonb_typeof(v)
        WHEN 'string' THEN 'text'
        WHEN 'boolean' THEN CASE WHEN v = 'true'::jsonb THEN 'true' ELSE 'false' END
        WHEN 'number' THEN CASE WHEN v::text ~ '^-?\d+$' THEN 'integer' ELSE 'real' END
        ELSE jsonb_typeof(v)
    END
    FROM (SELECT intent_json(t) #> intent_json_path(p) AS v) s
$$;

-- SQLite json_each over an array or object: key, value (scalar as text,
-- containers as JSON text), type.
CREATE OR REPLACE FUNCTION sqlite_json_each(t text)
RETURNS TABLE (key text, value text, type text)
LANGUAGE sql IMMUTABLE PARALLEL SAFE
SET search_path FROM CURRENT AS $$
    SELECT e.key, sqlite_json_extract(e.value::text, '$'), sqlite_json_type(e.value::text)
    FROM jsonb_each(CASE WHEN jsonb_typeof(intent_json(t)) = 'object' THEN intent_json(t) END) e
    UNION ALL
    SELECT (a.ord - 1)::text, sqlite_json_extract(a.value::text, '$'), sqlite_json_type(a.value::text)
    FROM jsonb_array_elements(CASE WHEN jsonb_typeof(intent_json(t)) = 'array' THEN intent_json(t) END)
        WITH ORDINALITY a(value, ord)
$$;

-- strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
CREATE OR REPLACE FUNCTION intent_now_iso() RETURNS text
LANGUAGE sql STABLE PARALLEL SAFE
SET search_path FROM CURRENT AS $$
    SELECT to_char(clock_timestamp() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
$$;

-- x GLOB p, with p already translated to an anchored regex.
CREATE OR REPLACE FUNCTION intent_regex(x text, re text) RETURNS boolean
LANGUAGE sql IMMUTABLE PARALLEL SAFE
SET search_path FROM CURRENT AS $$
    SELECT x ~ re
$$;

-- SQLite typeof() over the column types the baseline uses.
CREATE OR REPLACE FUNCTION sqlite_typeof(x bigint) RETURNS text LANGUAGE sql IMMUTABLE PARALLEL SAFE
SET search_path FROM CURRENT AS
$$ SELECT CASE WHEN x IS NULL THEN 'null' ELSE 'integer' END $$;
CREATE OR REPLACE FUNCTION sqlite_typeof(x text) RETURNS text LANGUAGE sql IMMUTABLE PARALLEL SAFE
SET search_path FROM CURRENT AS
$$ SELECT CASE WHEN x IS NULL THEN 'null' ELSE 'text' END $$;
CREATE OR REPLACE FUNCTION sqlite_typeof(x double precision) RETURNS text LANGUAGE sql IMMUTABLE PARALLEL SAFE
SET search_path FROM CURRENT AS
$$ SELECT CASE WHEN x IS NULL THEN 'null' ELSE 'real' END $$;
CREATE OR REPLACE FUNCTION sqlite_typeof(x bytea) RETURNS text LANGUAGE sql IMMUTABLE PARALLEL SAFE
SET search_path FROM CURRENT AS
$$ SELECT CASE WHEN x IS NULL THEN 'null' ELSE 'blob' END $$;

-- SQLite `t -> '$.p'`: the JSON text of the value at the path.
CREATE OR REPLACE FUNCTION sqlite_json_arrow(t text, p text) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE
SET search_path FROM CURRENT AS $$
    SELECT (intent_json(t) #> intent_json_path(p))::text
$$;

-- SQLite julianday() over 'now' and the ISO-8601 forms the store writes;
-- NULL otherwise. STABLE because of 'now'.
CREATE OR REPLACE FUNCTION sqlite_julianday(t text) RETURNS double precision
LANGUAGE plpgsql STABLE PARALLEL SAFE
SET search_path FROM CURRENT AS $$
BEGIN
    IF t = 'now' THEN
        RETURN extract(epoch FROM clock_timestamp()) / 86400.0 + 2440587.5;
    END IF;
    IF t IS NULL OR t !~ '^\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?)?(Z|[+-]\d{2}:\d{2})?$' THEN
        RETURN NULL;
    END IF;
    RETURN extract(epoch FROM (CASE WHEN t ~ '(Z|[+-]\d{2}:\d{2})$' THEN t ELSE t || 'Z' END)::timestamptz)
        / 86400.0 + 2440587.5;
EXCEPTION WHEN others THEN
    RETURN NULL;
END
$$;

-- SQLite json(): the value as JSON (an error when malformed). Pass it to
-- sqlite_json_set to store JSON rather than a string.
CREATE OR REPLACE FUNCTION sqlite_json(t text) RETURNS jsonb
LANGUAGE sql IMMUTABLE PARALLEL SAFE
SET search_path FROM CURRENT AS $$
    SELECT t::jsonb
$$;

-- One json_set step: like SQLite, create missing parent objects, then set
-- the leaf (SQL NULL becomes JSON null).
CREATE OR REPLACE FUNCTION intent_json_set_step(doc jsonb, p text, v jsonb) RETURNS jsonb
LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE
SET search_path FROM CURRENT AS $$
DECLARE
    path text[] := intent_json_path(p);
    i int;
BEGIN
    FOR i IN 1 .. coalesce(array_length(path, 1), 0) - 1 LOOP
        IF doc #> path[1:i] IS NULL THEN
            doc := jsonb_set(doc, path[1:i], '{}'::jsonb, true);
        END IF;
    END LOOP;
    RETURN jsonb_set(doc, path, coalesce(v, 'null'::jsonb), true);
END
$$;

-- SQLite json_set(t, path, value[, path, value]). The overload picks how the
-- value is stored: text as a JSON string, numbers as numbers, and
-- sqlite_json(...) (SQLite json(...)) as JSON. A NULL document stays NULL;
-- a malformed one is an error, as in SQLite.
CREATE OR REPLACE FUNCTION sqlite_json_set(t text, p text, v jsonb) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE
SET search_path FROM CURRENT AS $$
    SELECT intent_json_set_step(t::jsonb, p, v)::text
$$;
CREATE OR REPLACE FUNCTION sqlite_json_set(t text, p text, v text) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE
SET search_path FROM CURRENT AS $$
    SELECT intent_json_set_step(t::jsonb, p, to_jsonb(v))::text
$$;
CREATE OR REPLACE FUNCTION sqlite_json_set(t text, p text, v bigint) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE
SET search_path FROM CURRENT AS $$
    SELECT intent_json_set_step(t::jsonb, p, to_jsonb(v))::text
$$;
CREATE OR REPLACE FUNCTION sqlite_json_set(t text, p text, v double precision) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE
SET search_path FROM CURRENT AS $$
    SELECT intent_json_set_step(t::jsonb, p, to_jsonb(v))::text
$$;
CREATE OR REPLACE FUNCTION sqlite_json_set(t text, p1 text, v1 jsonb, p2 text, v2 jsonb) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE
SET search_path FROM CURRENT AS $$
    SELECT intent_json_set_step(intent_json_set_step(t::jsonb, p1, v1), p2, v2)::text
$$;
CREATE OR REPLACE FUNCTION sqlite_json_set(t text, p1 text, v1 text, p2 text, v2 text) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE
SET search_path FROM CURRENT AS $$
    SELECT intent_json_set_step(intent_json_set_step(t::jsonb, p1, to_jsonb(v1)), p2, to_jsonb(v2))::text
$$;

-- Mirrors MESSAGE_FTS_TEXT_SQL in agent_repo.rs: plain text, a JSON string,
-- the concatenated `.text` fields of a content-block array, or the JSON itself.
CREATE OR REPLACE FUNCTION intent_message_search_text(content text) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE
SET search_path FROM CURRENT AS $$
    SELECT CASE
        WHEN NOT sqlite_json_valid(content) THEN content
        WHEN sqlite_json_type(content) = 'text' THEN sqlite_json_extract(content, '$')
        WHEN sqlite_json_type(content) = 'array' THEN COALESCE((
            SELECT string_agg(e.value #>> '{text}', ' ' ORDER BY e.ord)
            FROM jsonb_array_elements(intent_json(content)) WITH ORDINALITY e(value, ord)
            WHERE jsonb_typeof(e.value -> 'text') = 'string'), '')
        ELSE intent_json(content)::text
    END
$$;

-- Tables. The SQLite part was generated once from the live SQLite schema
-- (SQLite migrations 0001-0196); later SQLite migrations name their
-- counterpart here with a `-- sqlite: <file>` line. Every table starts with
-- tenant_id, and tenant_id leads every primary key, unique constraint, index
-- and foreign key, so a cascade can never cross tenants. Tables with an
-- SQLite rowid get row_seq (insertion order, numbered by the row_seq
-- triggers below). JSON stays text; see the compatibility functions above.

CREATE TABLE IF NOT EXISTS advisory_wake_delivery (
    tenant_id text NOT NULL,
    parent_agent_id text NOT NULL,
    child_agent_id text NOT NULL,
    delivered_at text NOT NULL,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, parent_agent_id, child_agent_id)
);

-- sqlite: 0198_agent_browser_sender.sql
CREATE TABLE IF NOT EXISTS agent_browser_sender (
    tenant_id text NOT NULL,
    agent_id text NOT NULL,
    submission_id text NOT NULL,
    received_order text NOT NULL,
    client_id text,
    PRIMARY KEY (tenant_id, agent_id, submission_id)
);

CREATE TABLE IF NOT EXISTS agent_flipped_completion (
    tenant_id text NOT NULL,
    agent_id text NOT NULL,
    workspace_id text NOT NULL,
    task_note_id text NOT NULL,
    recorded_at text NOT NULL,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, agent_id, workspace_id, task_note_id)
);

CREATE TABLE IF NOT EXISTS agent_message (
    tenant_id text NOT NULL,
    id text NOT NULL,
    agent_id text NOT NULL,
    seq bigint NOT NULL,
    role text NOT NULL,
    content text NOT NULL,
    created_at text NOT NULL,
    metadata text,
    thumbnails text,
    usage_model text,
    usage_origin text,
    row_seq bigint NOT NULL,
    -- Full-text search: 'english' (stemmed: "plans" -> "plan") plus
    -- 'simple' (every word as written, stopwords included), so a search for
    -- "a", "the" or "about" still matches, as with SQLite FTS5 (porter
    -- unicode61, no stopword list). Built-in configurations only: Neki
    -- rejects user-created text-search dictionaries and configurations
    -- (router errors 302/303). Ranking differs from bm25, so tests compare
    -- match sets, not order.
    search_tsv tsvector GENERATED ALWAYS AS (
        CASE WHEN role IN ('user', 'assistant')
            THEN to_tsvector('english', intent_message_search_text(content))
                || to_tsvector('simple', intent_message_search_text(content)) END
    ) STORED,
    PRIMARY KEY (tenant_id, id),
    UNIQUE (tenant_id, agent_id, seq)
);

CREATE TABLE IF NOT EXISTS agent_message_payload (
    tenant_id text NOT NULL,
    message_id text NOT NULL,
    agent_id text NOT NULL,
    block_ordinal bigint NOT NULL,
    kind text NOT NULL,
    encoding text NOT NULL,
    body bytea NOT NULL,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, message_id, block_ordinal, kind)
);

CREATE TABLE IF NOT EXISTS agent_metrics (
    tenant_id text NOT NULL,
    agent_id text NOT NULL,
    workspace_id text NOT NULL,
    additions bigint NOT NULL DEFAULT 0,
    deletions bigint NOT NULL DEFAULT 0,
    files_changed bigint NOT NULL DEFAULT 0,
    updated_at text NOT NULL,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, workspace_id, agent_id)
);

CREATE TABLE IF NOT EXISTS agent_queue (
    tenant_id text NOT NULL,
    id text NOT NULL,
    agent_id text NOT NULL,
    position bigint NOT NULL,
    payload text NOT NULL,
    created_at text NOT NULL,
    turn_id text,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS agent_queue_admission (
    tenant_id text NOT NULL,
    id text NOT NULL,
    agent_id text NOT NULL,
    workspace_id text NOT NULL,
    turn_id text NOT NULL,
    payload text NOT NULL,
    created_at text NOT NULL,
    phase text NOT NULL,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, id),
    CHECK (phase IN ('pending','accepted','completed'))
);

-- Postgres only (orchestrator mode): a version per agent's persisted
-- queue, so replacing the queue is a compare-and-swap. The in-memory queue
-- is the source of truth and a persist replaces every agent_queue row of
-- the agent with a snapshot; without a version, a process holding an older
-- snapshot (a second handle, or a daemon that lost the tenant) could
-- replace a newer queue and lose a queued message. A replace bumps
-- `version` under the tenant writer lock after checking it is still the one
-- its process last loaded or wrote; otherwise it fails `Conflict` and
-- writes nothing. One row per agent that ever had its queue persisted; it
-- goes with the agent's session row.
CREATE TABLE IF NOT EXISTS agent_queue_version (
    tenant_id text NOT NULL,
    agent_id text NOT NULL,
    version bigint NOT NULL,
    -- A fresh id per write, so a process whose commit failed can tell
    -- whether its own write landed.
    write_id text NOT NULL,
    PRIMARY KEY (tenant_id, agent_id)
);

CREATE TABLE IF NOT EXISTS agent_session (
    tenant_id text NOT NULL,
    id text NOT NULL,
    workspace_id text NOT NULL,
    backend_session_id text,
    acp_session_id text,
    name text NOT NULL,
    name_explicitly_set bigint NOT NULL DEFAULT 0,
    model text,
    provider text,
    status text NOT NULL,
    is_active bigint NOT NULL DEFAULT 0,
    system_prompt text,
    created_at text NOT NULL,
    updated_at text NOT NULL,
    parent_agent_id text,
    specialist text,
    task_note_id text,
    skip_auto_commit bigint NOT NULL DEFAULT 0,
    completion_report text,
    completion_report_timestamp text,
    delegation_depth bigint,
    initial_message text,
    context_references text,
    image_blocks text,
    is_background bigint NOT NULL DEFAULT 0,
    metadata text,
    stop_reason text,
    token_usage text,
    token_usage_baseline text,
    resolved_model text,
    last_turn_model text,
    last_turn_provider text,
    last_assistant_preview text,
    last_user_preview text,
    attention_request_kind text,
    attention_request_reason text,
    attention_request_timestamp text,
    last_message_role text,
    stop_reason_timestamp text,
    reasoning_effort text,
    effort_levels text,
    last_message_id text,
    file_blocks text,
    task_graph_enabled bigint NOT NULL DEFAULT 0,
    harness_version text NOT NULL DEFAULT '1.0',
    harness_features text,
    last_tool_use_preview text,
    retired_at text,
    message_count bigint NOT NULL DEFAULT 0,
    assistant_message_count bigint NOT NULL DEFAULT 0,
    conversation_bytes bigint NOT NULL DEFAULT 0,
    notifications_muted bigint NOT NULL DEFAULT 0,
    last_turn_effort text,
    reasoning_effort_auto bigint NOT NULL DEFAULT 0,
    pinned bigint NOT NULL DEFAULT 0,
    prompt_breakdown text,
    permission_policy text,
    row_seq bigint NOT NULL,
    -- sqlite: 0199_agent_session_goal.sql
    goal text,
    -- sqlite: 0200_agent_session_goal_set_by.sql
    goal_set_by text,
    PRIMARY KEY (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS agent_stop_redelivery (
    tenant_id text NOT NULL,
    agent_id text NOT NULL,
    payload text NOT NULL,
    created_at text NOT NULL,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, agent_id)
);

CREATE TABLE IF NOT EXISTS agent_usage_cell (
    tenant_id text NOT NULL,
    agent_id text NOT NULL,
    model text NOT NULL,
    input_tokens bigint NOT NULL DEFAULT 0,
    output_tokens bigint NOT NULL DEFAULT 0,
    cache_read_tokens bigint NOT NULL DEFAULT 0,
    cache_creation_tokens bigint NOT NULL DEFAULT 0,
    thought_tokens bigint NOT NULL DEFAULT 0,
    costs_json text NOT NULL DEFAULT '{}',
    human_messages bigint NOT NULL DEFAULT 0,
    agent_messages bigint NOT NULL DEFAULT 0,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, agent_id, model)
);

CREATE TABLE IF NOT EXISTS attachment_idempotency_keys (
    tenant_id text NOT NULL,
    workspace_id text NOT NULL,
    "key" text NOT NULL,
    attachment_id text NOT NULL,
    fingerprint text NOT NULL,
    created_at text NOT NULL,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, workspace_id, "key")
);

CREATE TABLE IF NOT EXISTS attachments (
    tenant_id text NOT NULL,
    id text NOT NULL,
    workspace_id text NOT NULL,
    file_name text NOT NULL,
    mime_type text,
    size bigint NOT NULL,
    uploaded_at text NOT NULL,
    stored_path text NOT NULL,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS browser_tab (
    tenant_id text NOT NULL,
    tab_id text NOT NULL,
    workspace_id text NOT NULL,
    host_client_id text NOT NULL,
    url text NOT NULL,
    requested_url text,
    title text,
    owner_agent_id text,
    owner_agent_name text,
    visibility text NOT NULL DEFAULT 'visible',
    emulated_width bigint,
    emulated_height bigint,
    created_at text NOT NULL,
    updated_at text NOT NULL,
    closed_at text,
    row_seq bigint NOT NULL,
    -- sqlite: 0197_browser_tab_navigation_only.sql
    navigation_only bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (tenant_id, tab_id)
);

CREATE TABLE IF NOT EXISTS client (
    tenant_id text NOT NULL,
    id text NOT NULL,
    name text,
    capabilities text NOT NULL DEFAULT '{}',
    first_seen text NOT NULL,
    last_seen text NOT NULL,
    hostname text,
    pretty_hostname text,
    device_kind text,
    last_hello_at text,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS cloud_machine_provider (
    tenant_id text NOT NULL,
    id text NOT NULL,
    config_json text NOT NULL,
    updated_at text NOT NULL,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS comment (
    tenant_id text NOT NULL,
    id text NOT NULL,
    thread_id text NOT NULL,
    note_id text,
    workspace_id text NOT NULL,
    kind text NOT NULL,
    content text NOT NULL,
    author text NOT NULL,
    author_type text NOT NULL,
    status text NOT NULL DEFAULT 'open',
    parent_id text,
    anchor_json text NOT NULL,
    anchor_text text,
    extra_json text,
    created_at text NOT NULL,
    updated_at text NOT NULL,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS completion_wake_delivery (
    tenant_id text NOT NULL,
    parent_agent_id text NOT NULL,
    child_agent_id text NOT NULL,
    completion_identity text NOT NULL,
    delivered_at text NOT NULL,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, parent_agent_id, child_agent_id)
);

CREATE TABLE IF NOT EXISTS completion_watch (
    tenant_id text NOT NULL,
    id text NOT NULL,
    parent_workspace_id text NOT NULL,
    child_workspace_id text NOT NULL,
    parent_agent_id text NOT NULL,
    parent_agent_name text NOT NULL DEFAULT '',
    child_agent_id text NOT NULL,
    group_id text,
    report_delivered bigint NOT NULL DEFAULT 0,
    wake_on_attention bigint NOT NULL DEFAULT 0,
    created_at text NOT NULL,
    completion_only bigint NOT NULL DEFAULT 0,
    row_seq bigint NOT NULL,
    -- Postgres only (orchestrator mode): the delivery claim, so one
    -- completion wakes the parent once even when two processes see it. A
    -- process claims the row before it sends the terminal wake and skips
    -- when another holds it. The claim names the tenant's epoch
    -- (tenant_writer.epoch, read under the writer lock) and the claiming
    -- process (a token per process): it is stale only when a newer epoch
    -- exists (a takeover), never by elapsed time. Retiring the row drops the
    -- claim; a failed delivery releases it for its retry. NULL: unclaimed.
    claim_epoch bigint,
    claim_token text,
    PRIMARY KEY (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS delegation_group (
    tenant_id text NOT NULL,
    group_id text NOT NULL,
    workspace_id text NOT NULL,
    parent_agent_id text NOT NULL,
    await_mode text NOT NULL,
    expected_agent_ids text NOT NULL,
    completed_agent_ids text NOT NULL DEFAULT '[]',
    deleted_agent_ids text NOT NULL DEFAULT '[]',
    sealed bigint NOT NULL DEFAULT 0,
    delivered bigint NOT NULL DEFAULT 0,
    event_summaries text NOT NULL DEFAULT '[]',
    raw_events text NOT NULL DEFAULT '[]',
    created_at text NOT NULL,
    updated_at text NOT NULL,
    row_seq bigint NOT NULL,
    -- The delivery claim, as on completion_watch.
    claim_epoch bigint,
    claim_token text,
    PRIMARY KEY (tenant_id, group_id)
);

-- Postgres only (orchestrator mode): persisted delete grace deadlines. A
-- `*.delete` with `undoDelayMs > 0` arms an in-memory timer and stores its
-- deadline here, so the process that serves the tenant next re-arms it; the
-- commit takes the row first (DELETE ... RETURNING), so the delete commits
-- once even when two processes reach it. A cancel deletes the row. SQLite
-- (desktop) keeps the documented behaviour that a restart drops pending
-- deletions.
CREATE TABLE IF NOT EXISTS delete_grace (
    tenant_id text NOT NULL,
    subject_kind text NOT NULL,
    subject_id text NOT NULL,
    workspace_id text NOT NULL,
    delete_at text NOT NULL,
    created_at text NOT NULL,
    claim_epoch bigint,
    claim_token text,
    PRIMARY KEY (tenant_id, subject_kind, subject_id),
    CHECK (subject_kind IN ('workspace', 'agent'))
);

-- Postgres only (orchestrator mode): work claims. A periodic job that
-- several processes of one tenant run (for example the token-usage scan)
-- claims its work (kind, work_key) here and skips the run when another
-- holder has it. The claim names its own id, the claiming process (a token
-- per process) and the tenant epoch it was taken at; it lapses at
-- expires_at by the database clock unless renewed, and a newer epoch (a
-- takeover) makes it stale at once. Release deletes the row. SQLite
-- (desktop) keeps work claims in its single process.
CREATE TABLE IF NOT EXISTS work_claim (
    tenant_id text NOT NULL,
    kind text NOT NULL,
    work_key text NOT NULL,
    claim_id text NOT NULL,
    claim_token text NOT NULL,
    claim_epoch bigint NOT NULL,
    expires_at timestamptz NOT NULL,
    claimed_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, kind, work_key)
);

-- Postgres only: bounded, resumable workspace and agent deletes. A delete
-- first records itself here in a short writer transaction, then drains the
-- heavy child tables in bounded batches (each its own writer transaction,
-- so other writers of the tenant interleave), and finally deletes the
-- workspace or agent row and this record in one transaction. A record left
-- by a crash or a failed batch is finished by the next store open.
-- `agent_id` is '' for a workspace delete, whose record covers its agents.
-- No foreign keys: the record must outlive its rows until the final
-- transaction removes it. (SQLite drains in bounded batches without a
-- record.)
CREATE TABLE IF NOT EXISTS delete_purge (
    tenant_id text NOT NULL,
    kind text NOT NULL CHECK (kind IN ('workspace', 'agent')),
    workspace_id text NOT NULL,
    agent_id text NOT NULL DEFAULT '',
    started_at text NOT NULL,
    PRIMARY KEY (tenant_id, kind, workspace_id, agent_id)
);

CREATE TABLE IF NOT EXISTS deleted_workspace_id (
    tenant_id text NOT NULL,
    id text NOT NULL,
    deleted_at text NOT NULL,
    plain_folder_path text,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS diffs (
    tenant_id text NOT NULL,
    id text NOT NULL,
    workspace_id text NOT NULL,
    file_path text NOT NULL,
    staged bigint NOT NULL DEFAULT 0,
    old_content text,
    new_content text,
    hunks_json text NOT NULL DEFAULT '[]',
    created_at text NOT NULL,
    updated_at text NOT NULL,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, id),
    UNIQUE (tenant_id, workspace_id, file_path, staged)
);

CREATE TABLE IF NOT EXISTS discussion_agent_activity (
    tenant_id text NOT NULL,
    message_id text NOT NULL,
    agent_id text NOT NULL,
    attempt bigint NOT NULL,
    version bigint NOT NULL,
    state text NOT NULL,
    updated_at text NOT NULL,
    error text,
    turn_id text NOT NULL,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, message_id, agent_id),
    UNIQUE (tenant_id, turn_id),
    CHECK (attempt > 0),
    CHECK (version > 0),
    CHECK (state IN ('queued','working','completed','failed','stopped'))
);

CREATE TABLE IF NOT EXISTS discussion_agent_delivery (
    tenant_id text NOT NULL,
    message_id text NOT NULL,
    agent_id text NOT NULL,
    workspace_id text NOT NULL,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, message_id, agent_id)
);

CREATE TABLE IF NOT EXISTS discussion_agent_grant (
    tenant_id text NOT NULL,
    workspace_id text NOT NULL,
    conversation_id text NOT NULL,
    agent_id text NOT NULL,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, workspace_id, conversation_id, agent_id)
);

CREATE TABLE IF NOT EXISTS discussion_conversation (
    tenant_id text NOT NULL,
    id text NOT NULL,
    workspace_id text NOT NULL,
    kind text NOT NULL,
    participants_json text NOT NULL,
    created_at text NOT NULL,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, id),
    CHECK (kind IN ('direct','group'))
);

CREATE TABLE IF NOT EXISTS discussion_participant (
    tenant_id text NOT NULL,
    conversation_id text NOT NULL,
    workspace_id text NOT NULL,
    source_principal_id text NOT NULL,
    principal_id text,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, conversation_id, source_principal_id)
);

CREATE TABLE IF NOT EXISTS discussion_reaction (
    tenant_id text NOT NULL,
    id text NOT NULL,
    workspace_id text NOT NULL,
    message_id text NOT NULL,
    principal_id text,
    agent_id text,
    reaction text NOT NULL,
    author_json text NOT NULL DEFAULT '{"principalId":null,"login":null,"displayName":null,"avatarUrl":null}',
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, id),
    UNIQUE (tenant_id, message_id, agent_id, reaction),
    UNIQUE (tenant_id, message_id, principal_id, reaction),
    CHECK (reaction IN ('👍','👎','👀','✅','🙈','🙂')),
    CHECK (principal_id IS NULL OR agent_id IS NULL)
);

CREATE TABLE IF NOT EXISTS draft (
    tenant_id text NOT NULL,
    workspace_id text NOT NULL,
    agent_id text NOT NULL,
    client_id text NOT NULL,
    text text NOT NULL,
    updated_at text NOT NULL,
    attachments text,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, workspace_id, agent_id, client_id)
);

CREATE TABLE IF NOT EXISTS event (
    tenant_id text NOT NULL,
    id text NOT NULL,
    workspace_id text NOT NULL,
    timestamp text NOT NULL,
    event_type text NOT NULL,
    actor text NOT NULL,
    session_id text,
    correlation_id text,
    parent_event_id text,
    data_json text NOT NULL DEFAULT '{}',
    metadata_json text,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS event_subscription (
    tenant_id text NOT NULL,
    id text NOT NULL,
    workspace_id text NOT NULL,
    subscriber_agent_id text NOT NULL,
    event_types text NOT NULL,
    exclude_self bigint NOT NULL DEFAULT 1,
    batch_window_ms bigint NOT NULL DEFAULT 500,
    created_at text NOT NULL,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS execution_node (
    tenant_id text NOT NULL,
    id text NOT NULL,
    head_id text NOT NULL,
    node_identity text NOT NULL,
    record_json text NOT NULL,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, id),
    UNIQUE (tenant_id, node_identity)
);

CREATE TABLE IF NOT EXISTS executor_binding (
    tenant_id text NOT NULL,
    credential_id text NOT NULL,
    head_id text NOT NULL,
    node_id text NOT NULL,
    lease_id text NOT NULL,
    generation text NOT NULL,
    bound_at text NOT NULL,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, credential_id)
);

CREATE TABLE IF NOT EXISTS executor_credential (
    tenant_id text NOT NULL,
    id text NOT NULL,
    token_hash text NOT NULL,
    backend_id text NOT NULL,
    backend_name text NOT NULL,
    backend_fingerprint text,
    created_at text NOT NULL,
    last_used_at text,
    revoked_at text,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, id),
    UNIQUE (tenant_id, token_hash)
);

CREATE TABLE IF NOT EXISTS exposed_machine (
    tenant_id text NOT NULL,
    id text NOT NULL,
    name text NOT NULL,
    hosts_json text NOT NULL,
    port bigint NOT NULL,
    tc_address text,
    fingerprint text NOT NULL,
    credential_id text NOT NULL,
    platform_json text,
    capabilities_json text,
    observed_at text,
    state text NOT NULL,
    added_at text NOT NULL,
    last_connected_at text,
    node_protocol bigint,
    cloud_json text,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, id),
    CHECK (port BETWEEN 1 AND 65535),
    CHECK (state IN ('disconnected', 'unreachable', 'revoked', 'update-required'))
);

CREATE TABLE IF NOT EXISTS home_move (
    tenant_id text NOT NULL,
    workspace_id text NOT NULL,
    home_id text NOT NULL,
    node_id text,
    cloud_machine_id text,
    name text NOT NULL,
    from_home_id text NOT NULL,
    from_state text NOT NULL,
    from_since text NOT NULL,
    pushed bigint NOT NULL DEFAULT 0,
    setup_lease_id text,
    setup_seq bigint,
    started_at text NOT NULL,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, workspace_id),
    CHECK (from_state IN ('attached', 'detached', 'starting')),
    CHECK (pushed IN (0, 1))
);

CREATE TABLE IF NOT EXISTS home_rebuild (
    tenant_id text NOT NULL,
    workspace_id text NOT NULL,
    home_id text NOT NULL,
    from_home_id text NOT NULL,
    recorded_at text NOT NULL,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, workspace_id)
);

CREATE TABLE IF NOT EXISTS home_stop (
    tenant_id text NOT NULL,
    home_id text NOT NULL,
    workspace_id text NOT NULL,
    node_id text NOT NULL,
    stop_id text NOT NULL,
    reason text NOT NULL,
    orphaned_at text,
    recorded_at text NOT NULL,
    discard_push bigint NOT NULL DEFAULT 0,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, home_id),
    CHECK (reason IN ('retired', 'moved')),
    CHECK (discard_push IN (0, 1)),
    CHECK ((reason = 'moved') = (orphaned_at IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS hook (
    tenant_id text NOT NULL,
    hook_id text NOT NULL,
    workspace_id text NOT NULL,
    agent_id text NOT NULL,
    name text NOT NULL,
    code text NOT NULL,
    delay_ms bigint NOT NULL,
    state text NOT NULL,
    created_at text NOT NULL,
    last_run_at text,
    next_run_at text,
    run_count bigint NOT NULL DEFAULT 0,
    last_error text,
    last_logs text,
    last_state text,
    expires_at text,
    perpetual bigint NOT NULL DEFAULT 0,
    dispatch_count bigint NOT NULL DEFAULT 0,
    cron text,
    run_at text,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, hook_id),
    CHECK (state IN
                 ('scheduled', 'running', 'dispatched', 'evicted', 'cancelled',
                  'expired'))
);

CREATE TABLE IF NOT EXISTS host_invite (
    tenant_id text NOT NULL,
    id text NOT NULL,
    secret_hash text NOT NULL,
    secret text,
    created_by_principal_id text NOT NULL,
    pin_identity_provider text NOT NULL,
    pin_instance_host text NOT NULL,
    pin_external_user_id text NOT NULL,
    pin_login text NOT NULL,
    created_at text NOT NULL,
    expires_at text NOT NULL,
    redeemed_at text,
    redeemed_by_principal_id text,
    revoked_at text,
    redemption_count bigint NOT NULL DEFAULT 0,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, id),
    UNIQUE (tenant_id, secret_hash),
    CHECK (length(trim(pin_login)) > 0),
    CHECK (redemption_count IN (0, 1)),
    CHECK ((redemption_count = 0 AND redeemed_at IS NULL AND redeemed_by_principal_id IS NULL)
      OR (redemption_count = 1 AND redeemed_at IS NOT NULL AND redeemed_by_principal_id IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS host_member (
    tenant_id text NOT NULL,
    principal_id text NOT NULL,
    added_at text NOT NULL,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, principal_id)
);

CREATE TABLE IF NOT EXISTS host_membership_state (
    tenant_id text NOT NULL,
    id bigint NOT NULL,
    revision bigint NOT NULL DEFAULT 0,
    member_count bigint NOT NULL DEFAULT 0,
    authorization_generation bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (tenant_id, id),
    CHECK (id = 1),
    CHECK (revision >= 0),
    CHECK (member_count >= 0),
    CHECK (authorization_generation >= 0)
);

CREATE TABLE IF NOT EXISTS idempotency_key (
    tenant_id text NOT NULL,
    workspace_id text NOT NULL,
    idempotency_key text NOT NULL,
    method text NOT NULL,
    -- NULL while the operation runs: a keyed operation reserves its key
    -- first, so a retry that reaches another process waits for the result
    -- (or replays it) instead of running the operation twice. A failed
    -- operation deletes its pending row so a retry may run it.
    result_json text,
    created_at text NOT NULL,
    row_seq bigint NOT NULL,
    -- Postgres only (orchestrator mode): the reservation names the tenant's
    -- epoch (tenant_writer.epoch, read under the writer lock) and the
    -- reserving process (a token per process). It is stale only when a
    -- newer epoch exists (a takeover), never by elapsed time (the ~hourly
    -- reaper still drops any row older than its retention, pending or not).
    -- A process's first unleased open of the tenant drops other processes'
    -- pending rows (an unleased tenant has one process by design).
    claim_epoch bigint,
    claim_token text,
    PRIMARY KEY (tenant_id, workspace_id, idempotency_key),
    -- A pending row names its reservation; a completed one names none.
    CONSTRAINT idempotency_key_pending_check
        CHECK ((result_json IS NULL) = (claim_token IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS interrupted_agent (
    tenant_id text NOT NULL,
    agent_id text NOT NULL,
    workspace_id text NOT NULL,
    prev_status text NOT NULL,
    interrupted_at text NOT NULL,
    resolution text NOT NULL DEFAULT 'pending',
    resolved_at text,
    reason text,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, agent_id)
);

CREATE TABLE IF NOT EXISTS known_repo (
    tenant_id text NOT NULL,
    path text NOT NULL,
    name text NOT NULL,
    owner text,
    added_at text NOT NULL,
    last_used_at text NOT NULL,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, path)
);

CREATE TABLE IF NOT EXISTS mcp_oauth_tokens (
    tenant_id text NOT NULL,
    server_id text NOT NULL,
    token_bag text NOT NULL,
    updated_at text NOT NULL,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, server_id)
);

CREATE TABLE IF NOT EXISTS node_assignment (
    tenant_id text NOT NULL,
    agent_id text NOT NULL,
    workspace_id text NOT NULL,
    lease_id text NOT NULL,
    record_json text NOT NULL,
    current_checkpoint_id text,
    capture_revision text NOT NULL DEFAULT '0',
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, agent_id)
);

CREATE TABLE IF NOT EXISTS node_checkpoint (
    tenant_id text NOT NULL,
    id text NOT NULL,
    agent_id text NOT NULL,
    assignment_epoch text NOT NULL,
    capture_revision text NOT NULL,
    record_json text NOT NULL,
    receipt_json text NOT NULL,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, id),
    UNIQUE (tenant_id, agent_id, assignment_epoch, capture_revision)
);

CREATE TABLE IF NOT EXISTS node_invite (
    tenant_id text NOT NULL,
    id text NOT NULL,
    secret_hash text NOT NULL,
    label text,
    created_at text NOT NULL,
    expires_at text NOT NULL,
    used_at text,
    used_by_backend_id text,
    credential_id text,
    revoked_at text,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, id),
    UNIQUE (tenant_id, secret_hash)
);

CREATE TABLE IF NOT EXISTS node_lease (
    tenant_id text NOT NULL,
    id text NOT NULL,
    node_id text NOT NULL,
    terminal bigint NOT NULL DEFAULT 0,
    record_json text NOT NULL,
    ack_seq text NOT NULL DEFAULT '0',
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, id),
    CHECK (terminal IN (0, 1))
);

CREATE TABLE IF NOT EXISTS note (
    tenant_id text NOT NULL,
    id text NOT NULL,
    workspace_id text NOT NULL,
    title text NOT NULL,
    content text NOT NULL,
    content_type text NOT NULL DEFAULT 'markdown',
    tags text NOT NULL DEFAULT '[]',
    is_pinned bigint NOT NULL DEFAULT 0,
    is_archived bigint NOT NULL DEFAULT 0,
    is_default bigint NOT NULL DEFAULT 0,
    parent_id text,
    visibility text NOT NULL DEFAULT 'workspace',
    task_json text,
    created_at text NOT NULL,
    updated_at text NOT NULL,
    rev bigint NOT NULL DEFAULT 0,
    row_seq bigint NOT NULL,
    -- Full-text search, as agent_message.search_tsv.
    search_tsv tsvector GENERATED ALWAYS AS (
        to_tsvector('english',
            coalesce(title, '') || ' ' || coalesce(content, '') || ' ' || coalesce(tags, ''))
        || to_tsvector('simple',
            coalesce(title, '') || ' ' || coalesce(content, '') || ' ' || coalesce(tags, ''))
    ) STORED,
    PRIMARY KEY (tenant_id, id, workspace_id)
);

CREATE TABLE IF NOT EXISTS note_line_attribution (
    tenant_id text NOT NULL,
    note_id text NOT NULL,
    workspace_id text NOT NULL,
    computed_at text NOT NULL,
    attributions_json text NOT NULL,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, workspace_id, note_id)
);

CREATE TABLE IF NOT EXISTS note_page_entry (
    tenant_id text NOT NULL,
    workspace_id text NOT NULL,
    note_id text NOT NULL,
    collection text NOT NULL,
    position bigint NOT NULL,
    value text NOT NULL,
    source_start bigint,
    source_end bigint,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, workspace_id, note_id, collection, position),
    CHECK (octet_length(value) <= 32768)
);

CREATE TABLE IF NOT EXISTS note_page_head (
    tenant_id text NOT NULL,
    workspace_id text NOT NULL,
    note_id text NOT NULL,
    instance_id text NOT NULL,
    indexed_rev bigint NOT NULL DEFAULT -1,
    profile_revision text NOT NULL DEFAULT '',
    current_rev bigint NOT NULL,
    generation text NOT NULL,
    task_count bigint NOT NULL DEFAULT 0,
    source_length bigint NOT NULL DEFAULT 0,
    source_bytes bigint NOT NULL DEFAULT 0,
    scalar_count bigint NOT NULL DEFAULT 0,
    lf_count bigint NOT NULL DEFAULT 0,
    content_generation text NOT NULL DEFAULT '',
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, workspace_id, note_id)
);

CREATE TABLE IF NOT EXISTS note_version (
    tenant_id text NOT NULL,
    note_id text NOT NULL,
    workspace_id text NOT NULL,
    v bigint NOT NULL,
    date text NOT NULL,
    author_id text NOT NULL,
    author_name text NOT NULL,
    author_type text NOT NULL,
    title text NOT NULL,
    content text NOT NULL,
    rev bigint,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, workspace_id, note_id, v)
);

CREATE TABLE IF NOT EXISTS pr_monitor (
    tenant_id text NOT NULL,
    monitor_id text NOT NULL,
    workspace_id text NOT NULL,
    agent_id text NOT NULL,
    repo_owner text NOT NULL,
    repo_name text NOT NULL,
    pr_number bigint NOT NULL,
    state text NOT NULL,
    last_snapshot text,
    pending_changes text,
    pending_since text,
    last_change_at text,
    last_polled_at text,
    last_error text,
    created_at text NOT NULL,
    updated_at text NOT NULL,
    baseline_snapshot text,
    target_provider text,
    target_instance_base_url text,
    target_project_path text,
    target_kind text,
    target_provenance text NOT NULL DEFAULT 'unresolved',
    target_unresolved_reason text DEFAULT 'missing-provenance',
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, monitor_id),
    CHECK (state IN ('active', 'completed', 'cancelled')),
    CHECK (target_provenance IN ('unresolved', 'captured-request', 'legacy-github-writer', 'validated-transfer')),
    CHECK ((target_provenance = 'unresolved' AND target_unresolved_reason IS NOT NULL)
    OR
    (target_provenance != 'unresolved' AND target_unresolved_reason IS NULL
      AND target_provider IS NOT NULL AND target_provider IN ('github', 'gitlab')
      AND target_instance_base_url IS NOT NULL AND length(target_instance_base_url) > 0
      AND target_project_path IS NOT NULL AND length(target_project_path) > 0
      AND target_kind IS NOT NULL
      AND ((target_provider = 'github' AND target_kind IN ('pull-request', 'issue'))
        OR (target_provider = 'gitlab' AND target_kind IN ('merge-request', 'issue')))
      AND pr_number > 0))
);

CREATE TABLE IF NOT EXISTS pr_monitor_message_provenance (
    tenant_id text NOT NULL,
    message_id text NOT NULL,
    agent_id text NOT NULL,
    workspace_id text NOT NULL,
    binding_json text NOT NULL,
    envelope_sha256 bytea NOT NULL,
    version bigint NOT NULL,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, message_id),
    CHECK (length(envelope_sha256) = 32),
    CHECK (version = 1)
);

CREATE TABLE IF NOT EXISTS principal (
    tenant_id text NOT NULL,
    id text NOT NULL,
    github_user_id bigint,
    login text,
    display_name text,
    avatar_url text,
    is_primary bigint NOT NULL DEFAULT 0,
    created_at text NOT NULL,
    updated_at text NOT NULL,
    identity_provider text,
    instance_host text,
    external_user_id text,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, id),
    UNIQUE (tenant_id, github_user_id),
    CHECK (is_primary IN (0, 1))
);

CREATE TABLE IF NOT EXISTS principal_credential (
    tenant_id text NOT NULL,
    token_hash text NOT NULL,
    principal_id text NOT NULL,
    created_at text NOT NULL,
    last_used_at text,
    revoked_at text,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, token_hash)
);

CREATE TABLE IF NOT EXISTS principal_revocation (
    tenant_id text NOT NULL,
    principal_id text NOT NULL,
    generation bigint NOT NULL,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, principal_id),
    CHECK (generation > 0)
);

CREATE TABLE IF NOT EXISTS pull_request_workspace (
    tenant_id text NOT NULL,
    pr_key text NOT NULL,
    workspace_id text NOT NULL,
    source text NOT NULL,
    linked_at text NOT NULL,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, pr_key, workspace_id),
    CHECK (source IN ('workspace', 'git_root'))
);

CREATE TABLE IF NOT EXISTS pull_request_workspace_candidate (
    tenant_id text NOT NULL,
    url text,
    workspace_id text NOT NULL,
    source text NOT NULL,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, row_seq)
);

CREATE TABLE IF NOT EXISTS remote_agent (
    tenant_id text NOT NULL,
    agent_id text NOT NULL,
    workspace_id text NOT NULL,
    node_id text NOT NULL,
    lease_id text NOT NULL,
    created_at text NOT NULL,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, agent_id)
);

CREATE TABLE IF NOT EXISTS remote_final_stop (
    tenant_id text NOT NULL,
    agent_id text NOT NULL,
    workspace_id text NOT NULL,
    machine_id text NOT NULL,
    lease_id text NOT NULL,
    stop_id text NOT NULL,
    reason text NOT NULL,
    repo_path text NOT NULL,
    requested_at text NOT NULL,
    submodules text NOT NULL DEFAULT '[]',
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, agent_id),
    CHECK (reason IN ('agent-deleted', 'agent-retired', 'workspace-retired', 'workspace-deleted', 'machine-removed'))
);

CREATE TABLE IF NOT EXISTS repository_authority_revision (
    tenant_id text NOT NULL,
    kind text NOT NULL,
    subject_id text NOT NULL,
    member_id text NOT NULL DEFAULT '',
    revision bigint NOT NULL,
    PRIMARY KEY (tenant_id, kind, subject_id, member_id),
    CHECK (kind IN ('workspace', 'principal', 'workspace_member', 'host_member', 'credential')),
    CHECK (true AND revision > 0)
);

CREATE TABLE IF NOT EXISTS repository_selection_state (
    tenant_id text NOT NULL,
    workspace_id text NOT NULL,
    root_kind text NOT NULL,
    root_id text NOT NULL,
    root_incarnation bigint NOT NULL,
    selection_revision bigint NOT NULL,
    root_present bigint NOT NULL,
    repository_path text,
    worktree_path text,
    is_remote bigint,
    registered_path text,
    choice_incarnation bigint NOT NULL,
    choice_mode text NOT NULL,
    remote_name text,
    historical_source text,
    historical_record_id text,
    PRIMARY KEY (tenant_id, workspace_id, root_kind, root_id),
    CHECK (root_kind IN ('primary','registered'))
);

CREATE TABLE IF NOT EXISTS review_context_enrollment (
    tenant_id text NOT NULL,
    agent_id text NOT NULL,
    workspace_id text NOT NULL,
    acp_session_id text NOT NULL,
    owner_key text NOT NULL,
    version bigint NOT NULL,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, agent_id),
    UNIQUE (tenant_id, owner_key),
    CHECK (length(trim(acp_session_id)) BETWEEN 1 AND 256 AND true),
    CHECK (length(owner_key)=36 AND substr(owner_key,9,1)='-' AND substr(owner_key,14,1)='-'
        AND substr(owner_key,19,1)='-' AND substr(owner_key,24,1)='-'
        AND length(replace(owner_key,'-',''))=32
        AND replace(owner_key,'-','') !~ '^.*[^0-9a-f].*$'
        AND substr(owner_key,15,1)='4' AND substr(owner_key,20,1) ~ '^[89ab]$'),
    CHECK (version=1)
);

CREATE TABLE IF NOT EXISTS review_context_private_family (
    tenant_id text NOT NULL,
    acp_session_id text NOT NULL,
    version bigint NOT NULL,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, acp_session_id),
    CHECK (length(trim(acp_session_id)) BETWEEN 1 AND 256 AND true),
    CHECK (version=1)
);

CREATE TABLE IF NOT EXISTS script (
    tenant_id text NOT NULL,
    id text NOT NULL,
    workspace_id text NOT NULL,
    name text NOT NULL,
    command text NOT NULL,
    cwd text,
    env text,
    mode text NOT NULL,
    category text,
    source text NOT NULL,
    auto_start bigint,
    created_at text NOT NULL,
    updated_at text,
    was_running bigint NOT NULL DEFAULT 0,
    purpose text NOT NULL DEFAULT 'saved',
    archived_at text,
    last_run text,
    pending_run_id text,
    pending_started_at text,
    latest_run_id text,
    latest_run_result text,
    remote_author text,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS script_monitor (
    tenant_id text NOT NULL,
    id text NOT NULL,
    workspace_id text NOT NULL,
    agent_id text NOT NULL,
    script_id text NOT NULL,
    run_id text NOT NULL,
    state text NOT NULL,
    row_json text NOT NULL,
    created_at text NOT NULL,
    cancel_intent bigint NOT NULL DEFAULT 0,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, id),
    CHECK (state='active')
);

CREATE TABLE IF NOT EXISTS script_notification_job (
    tenant_id text NOT NULL,
    wake_id text NOT NULL,
    workspace_id text NOT NULL,
    agent_id text NOT NULL,
    script_id text NOT NULL,
    run_id text NOT NULL,
    metadata text NOT NULL,
    body text NOT NULL,
    phase text NOT NULL,
    created_at text NOT NULL,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, wake_id),
    CHECK (phase IN ('pending','message-persisted'))
);

CREATE TABLE IF NOT EXISTS settings (
    tenant_id text NOT NULL,
    "key" text NOT NULL,
    value text NOT NULL,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, "key")
);

CREATE TABLE IF NOT EXISTS task_agent_link (
    tenant_id text NOT NULL,
    workspace_id text NOT NULL,
    note_id text NOT NULL,
    task_key text NOT NULL,
    task_text text NOT NULL,
    agent_id text NOT NULL,
    created_at bigint NOT NULL,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, workspace_id, note_id, task_key)
);

-- Postgres only (orchestrator mode): the fenced tenant lease. One
-- orchestrator process owns a tenant at a time. The owner's heartbeat
-- renews it with a plain UPDATE of this row, outside the writer lock, so a
-- busy tenant cannot starve it. A claim takes the writer lock, then this
-- row's lock, and succeeds only when there is no row or `expires_at` has
-- passed by the database clock; it sets the owner and bumps the epoch here
-- and on tenant_writer in one transaction. One row per tenant, keyed by
-- tenant_id alone (no identity columns), so it routes to the tenant's shard
-- on Neki.
CREATE TABLE IF NOT EXISTS tenant_lease (
    tenant_id text NOT NULL,
    owner_instance text NOT NULL,
    epoch bigint NOT NULL,
    expires_at timestamptz NOT NULL,
    renewed_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id)
);

-- Tenant secrets (orchestrator mode). The desktop keeps its secrets in the
-- file store (`intent_core::FileSecretStore`); an orchestrator stores each
-- tenant's secrets here, so they follow the tenant to the process that
-- takes it over. Postgres only; no SQLite counterpart.
--
-- Values are encrypted by the store before they reach the database
-- (`intent_store::postgres::TenantSecretStore`): XChaCha20-Poly1305 with a
-- random 24-byte nonce per value, and associated data that binds the
-- tenant, the account and the key id, so a row copied to another tenant or
-- account does not decrypt. `key_id` names the keyring key that sealed the
-- row, so keys can rotate: new writes use the active key, older rows stay
-- readable under theirs. No plaintext is stored.
--
-- Keyed by tenant_id first, no identity column, so it routes to the
-- tenant's shard on Neki.
CREATE TABLE IF NOT EXISTS tenant_secrets (
    tenant_id text NOT NULL,
    account text NOT NULL,
    key_id text NOT NULL,
    nonce bytea NOT NULL,
    ciphertext bytea NOT NULL,
    updated_at text NOT NULL,
    PRIMARY KEY (tenant_id, account),
    CHECK (octet_length(nonce) = 24),
    CHECK (octet_length(ciphertext) >= 16)
);

-- Postgres only: the tenant writer lock as a row lock. Every Postgres
-- writer transaction locks its tenant's row here (`SELECT ... FOR UPDATE`)
-- before it reads or writes anything else. A transaction advisory lock
-- would read no table and so carry no shard key: on Neki sharded by
-- `tenant_id`, the router could not route it to the tenant's shard. A row
-- keyed by `tenant_id` routes to the tenant's shard like every other
-- statement of the transaction. The store seeds a tenant's row on its first
-- write (`INSERT ... ON CONFLICT DO NOTHING`, then the lock). Once Postgres
-- stores ship, a lock change needs one release that takes both locks.
--
-- The row also carries the lease fence (orchestrator mode, see
-- tenant_lease): the owner instance and the tenant's epoch. A writer
-- compares them with its process's claim after it takes the lock, so a
-- process that lost the tenant cannot write, even one that waited for the
-- lock behind the takeover (READ COMMITTED re-reads the locked row). NULL
-- owner and epoch 0: no process ever claimed the tenant.
--
-- last_row_seq is the tenant's row_seq counter: the last value handed out
-- across all its tables (see intent_next_row_seq).
CREATE TABLE IF NOT EXISTS tenant_writer (
    tenant_id text NOT NULL,
    owner_instance text,
    epoch bigint NOT NULL DEFAULT 0,
    last_row_seq bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (tenant_id)
);

CREATE TABLE IF NOT EXISTS tracked_changes (
    tenant_id text NOT NULL,
    id text NOT NULL,
    workspace_id text NOT NULL,
    path text NOT NULL,
    stage text NOT NULL,
    status text NOT NULL,
    agent_id text,
    session_id text,
    turn bigint,
    commit_hash text,
    old_blob_sha text,
    new_blob_sha text,
    additions bigint NOT NULL DEFAULT 0,
    deletions bigint NOT NULL DEFAULT 0,
    created_at text NOT NULL,
    updated_at text NOT NULL,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS usage_rate_minutely (
    tenant_id text NOT NULL,
    bucket_utc text NOT NULL,
    input_tokens bigint NOT NULL DEFAULT 0,
    output_tokens bigint NOT NULL DEFAULT 0,
    cache_read_tokens bigint NOT NULL DEFAULT 0,
    cache_creation_tokens bigint NOT NULL DEFAULT 0,
    thought_tokens bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (tenant_id, bucket_utc)
);

CREATE TABLE IF NOT EXISTS usage_stats_hourly (
    tenant_id text NOT NULL,
    bucket_utc text NOT NULL,
    model text NOT NULL,
    provider text NOT NULL DEFAULT 'unknown',
    input_tokens bigint NOT NULL DEFAULT 0,
    output_tokens bigint NOT NULL DEFAULT 0,
    cache_read_tokens bigint NOT NULL DEFAULT 0,
    cache_creation_tokens bigint NOT NULL DEFAULT 0,
    runs bigint NOT NULL DEFAULT 0,
    sessions_started bigint NOT NULL DEFAULT 0,
    longest_run_ms bigint NOT NULL DEFAULT 0,
    lines_added bigint NOT NULL DEFAULT 0,
    lines_deleted bigint NOT NULL DEFAULT 0,
    local_date text,
    local_hour bigint,
    thought_tokens bigint NOT NULL DEFAULT 0,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, bucket_utc, model, provider)
);

CREATE TABLE IF NOT EXISTS workspace (
    tenant_id text NOT NULL,
    id text NOT NULL,
    title text NOT NULL,
    branch text NOT NULL,
    base_ref text,
    base_commit_sha text,
    status text NOT NULL DEFAULT 'active',
    status_message text,
    attention text NOT NULL DEFAULT 'none',
    repository_owner text,
    repository_name text,
    worktree_path text,
    scope text,
    skip_worktree bigint NOT NULL DEFAULT 0,
    is_remote bigint NOT NULL DEFAULT 0,
    default_model text,
    pr_number bigint,
    pr_url text,
    archived bigint NOT NULL DEFAULT 0,
    archived_at text,
    tags text NOT NULL DEFAULT '[]',
    created_at text NOT NULL,
    updated_at text NOT NULL,
    last_activity text,
    pr_status text,
    active_pull_request text,
    path text,
    repository_path text,
    token_usage text,
    setup_script text,
    branch_auto_generated bigint NOT NULL DEFAULT 0,
    pull_requests text,
    checkout_mode text,
    status_image_asset_id text,
    auto_commit_enabled bigint,
    context_links text,
    browser_client_id text,
    legacy_author_principal_id text,
    owner_principal_id text,
    last_content_activity text,
    scripts_initialized bigint NOT NULL DEFAULT 0,
    retired_at text,
    label text,
    auto_archive_disabled bigint NOT NULL DEFAULT 0,
    plain_folder bigint NOT NULL DEFAULT 0,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS workspace_context_item (
    tenant_id text NOT NULL,
    workspace_id text NOT NULL,
    id text NOT NULL,
    ordinal bigint NOT NULL,
    payload text NOT NULL,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, workspace_id, id)
);

CREATE TABLE IF NOT EXISTS workspace_discussion_conversation (
    tenant_id text NOT NULL,
    workspace_id text NOT NULL,
    id text NOT NULL,
    name text,
    first_message_id text,
    first_principal_id text,
    first_client_message_id text,
    created_at text NOT NULL,
    deleted_at text,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, workspace_id, id)
);

CREATE TABLE IF NOT EXISTS workspace_discussion_message (
    tenant_id text NOT NULL,
    id text NOT NULL,
    workspace_id text NOT NULL,
    conversation_id text NOT NULL DEFAULT 'default',
    sequence bigint NOT NULL,
    principal_id text,
    client_message_id text NOT NULL,
    body text NOT NULL,
    author_json text NOT NULL,
    created_at text NOT NULL,
    reply_to_message_id text,
    reply_to_json text,
    reactions_json text NOT NULL DEFAULT '[]',
    reaction_version bigint NOT NULL DEFAULT 0,
    agent_id text,
    recipient_agent_ids_json text NOT NULL DEFAULT '[]',
    agent_activity_json text NOT NULL DEFAULT '[]',
    agent_mentions_json text NOT NULL DEFAULT '[]',
    principal_mentions_json text NOT NULL DEFAULT '[]',
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, id),
    UNIQUE (tenant_id, workspace_id, principal_id, client_message_id),
    UNIQUE (tenant_id, workspace_id, conversation_id, sequence),
    CHECK (sequence > 0),
    CHECK (reaction_version >= 0)
);

CREATE TABLE IF NOT EXISTS workspace_git_root (
    tenant_id text NOT NULL,
    id text NOT NULL,
    workspace_id text NOT NULL,
    path text NOT NULL,
    source text NOT NULL,
    repo_owner text,
    repo_name text,
    registered_by_agent_ids text NOT NULL DEFAULT '[]',
    pr_number bigint,
    pr_url text,
    pr_status text,
    pull_requests text,
    created_at text NOT NULL,
    updated_at text NOT NULL,
    registered_commit_sha text,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, id),
    CHECK (source IN ('agent', 'auto'))
);

CREATE TABLE IF NOT EXISTS workspace_home (
    tenant_id text NOT NULL,
    workspace_id text NOT NULL,
    home_id text NOT NULL,
    node_id text,
    cloud_machine_id text,
    name text NOT NULL,
    state text NOT NULL,
    since text NOT NULL,
    last_backup_at text,
    backup_failing_since text,
    auto_start_lease_id text,
    auto_start_seq bigint,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, workspace_id),
    CHECK (state IN ('attached', 'detached', 'moving', 'starting'))
);

CREATE TABLE IF NOT EXISTS workspace_invite (
    tenant_id text NOT NULL,
    id text NOT NULL,
    workspace_id text NOT NULL,
    secret_hash text NOT NULL,
    created_by_principal_id text NOT NULL,
    pin_github_user_id bigint,
    pin_login text,
    created_at text NOT NULL,
    expires_at text NOT NULL,
    redeemed_at text,
    redeemed_by_principal_id text,
    revoked_at text,
    secret text,
    redemption_count bigint NOT NULL DEFAULT 0,
    pin_identity_provider text,
    pin_instance_host text,
    pin_external_user_id text,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, id),
    UNIQUE (tenant_id, secret_hash)
);

CREATE TABLE IF NOT EXISTS workspace_invite_seat (
    tenant_id text NOT NULL,
    invite_id text NOT NULL,
    workspace_id text NOT NULL,
    expires_at text NOT NULL,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, invite_id)
);

CREATE TABLE IF NOT EXISTS workspace_mcp_disabled_server (
    tenant_id text NOT NULL,
    workspace_id text NOT NULL,
    server_id text NOT NULL,
    created_at text NOT NULL,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, workspace_id, server_id)
);

CREATE TABLE IF NOT EXISTS workspace_member (
    tenant_id text NOT NULL,
    workspace_id text NOT NULL,
    principal_id text NOT NULL,
    role text NOT NULL,
    added_at text NOT NULL,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, workspace_id, principal_id),
    CHECK (role IN ('owner', 'collaborator', 'manager'))
);

CREATE TABLE IF NOT EXISTS workspace_metrics (
    tenant_id text NOT NULL,
    workspace_id text NOT NULL,
    additions bigint NOT NULL DEFAULT 0,
    deletions bigint NOT NULL DEFAULT 0,
    files_changed bigint NOT NULL DEFAULT 0,
    updated_at text NOT NULL,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, workspace_id)
);

CREATE TABLE IF NOT EXISTS workspace_sharing_summary (
    tenant_id text NOT NULL,
    workspace_id text NOT NULL,
    direct_count bigint NOT NULL DEFAULT 0,
    non_host_count bigint NOT NULL DEFAULT 0,
    guest_count bigint NOT NULL DEFAULT 0,
    open_invite_count bigint NOT NULL DEFAULT 0,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, workspace_id),
    CHECK (direct_count >= 0),
    CHECK (non_host_count >= 0),
    CHECK (guest_count >= 0),
    CHECK (open_invite_count >= 0)
);

CREATE TABLE IF NOT EXISTS workspace_ui_context (
    tenant_id text NOT NULL,
    workspace_id text NOT NULL,
    payload text NOT NULL,
    row_seq bigint NOT NULL,
    PRIMARY KEY (tenant_id, workspace_id)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_advisory_wake_delivery_child ON advisory_wake_delivery (tenant_id, child_agent_id);
CREATE INDEX IF NOT EXISTS agent_browser_sender_latest ON agent_browser_sender (tenant_id, agent_id, received_order COLLATE "C" DESC);
CREATE INDEX IF NOT EXISTS idx_agent_flipped_completion_task ON agent_flipped_completion (tenant_id, workspace_id, task_note_id);
CREATE INDEX IF NOT EXISTS idx_agent_message_usage_projection ON agent_message (tenant_id, agent_id, usage_model, usage_origin, role);
CREATE UNIQUE INDEX IF NOT EXISTS idx_agent_message_id_agent ON agent_message (tenant_id, id, agent_id);
CREATE INDEX IF NOT EXISTS idx_agent_message_usage ON agent_message (tenant_id, agent_id, seq) WHERE CASE WHEN sqlite_json_valid(content) THEN
    (CASE WHEN sqlite_json_type(content, '$.usage') IS NOT NULL
        THEN sqlite_json_type(content, '$.usage') = 'object'
        ELSE sqlite_json_type(content, '$._meta.usage') = 'object' END) ELSE false END;
CREATE INDEX IF NOT EXISTS idx_agent_message_agent_role_seq ON agent_message (tenant_id, agent_id, role, seq DESC);
CREATE INDEX IF NOT EXISTS agent_message_search_tsv ON agent_message USING gin (search_tsv);
CREATE INDEX IF NOT EXISTS idx_agent_message_payload_agent ON agent_message_payload (tenant_id, agent_id);
CREATE INDEX IF NOT EXISTS idx_agent_metrics_agent ON agent_metrics (tenant_id, agent_id);
CREATE INDEX IF NOT EXISTS idx_agent_queue_agent ON agent_queue (tenant_id, agent_id, position);
CREATE INDEX IF NOT EXISTS idx_agent_queue_admission_agent ON agent_queue_admission (tenant_id, agent_id);
CREATE INDEX IF NOT EXISTS idx_agent_session_unread_top_level ON agent_session (tenant_id, workspace_id, last_message_id, (sqlite_json_extract(metadata, '$.lastSeenMessageId'))) WHERE parent_agent_id IS NULL
    AND is_background = 0
    AND status <> 'deleted'
    AND last_message_id IS NOT NULL
    AND last_message_role = 'assistant'
    AND retired_at IS NULL
    AND notifications_muted = 0;
CREATE INDEX IF NOT EXISTS idx_agent_workspace_retired ON agent_session (tenant_id, workspace_id) WHERE retired_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_agent_parent ON agent_session (tenant_id, parent_agent_id);
CREATE INDEX IF NOT EXISTS idx_agent_workspace ON agent_session (tenant_id, workspace_id);
CREATE INDEX IF NOT EXISTS idx_attachment_idempotency_keys_attachment ON attachment_idempotency_keys (tenant_id, attachment_id, workspace_id);
CREATE INDEX IF NOT EXISTS idx_attachment_idempotency_keys_created ON attachment_idempotency_keys (tenant_id, created_at);
CREATE INDEX IF NOT EXISTS idx_attachments_workspace ON attachments (tenant_id, workspace_id);
CREATE INDEX IF NOT EXISTS idx_browser_tab_workspace ON browser_tab (tenant_id, workspace_id);
CREATE INDEX IF NOT EXISTS idx_browser_tab_host ON browser_tab (tenant_id, host_client_id, created_at, tab_id);
CREATE INDEX IF NOT EXISTS idx_browser_tab_workspace_open ON browser_tab (tenant_id, workspace_id, created_at, tab_id) WHERE closed_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_comment_workspace_note ON comment (tenant_id, workspace_id, note_id) WHERE note_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_comment_thread ON comment (tenant_id, thread_id);
CREATE INDEX IF NOT EXISTS idx_comment_note ON comment (tenant_id, note_id, workspace_id);
CREATE INDEX IF NOT EXISTS idx_completion_wake_delivery_child ON completion_wake_delivery (tenant_id, child_agent_id);
CREATE INDEX IF NOT EXISTS idx_completion_watch_child ON completion_watch (tenant_id, child_agent_id);
CREATE INDEX IF NOT EXISTS idx_completion_watch_parent ON completion_watch (tenant_id, parent_agent_id);
CREATE INDEX IF NOT EXISTS idx_delegation_group_delivered ON delegation_group (tenant_id, delivered);
CREATE INDEX IF NOT EXISTS idx_delegation_group_parent ON delegation_group (tenant_id, parent_agent_id);
CREATE INDEX IF NOT EXISTS idx_delegation_group_workspace ON delegation_group (tenant_id, workspace_id);
CREATE INDEX IF NOT EXISTS idx_diffs_ws ON diffs (tenant_id, workspace_id);
CREATE INDEX IF NOT EXISTS idx_discussion_agent_activity_agent ON discussion_agent_activity (tenant_id, agent_id);
CREATE INDEX IF NOT EXISTS idx_discussion_agent_delivery_agent ON discussion_agent_delivery (tenant_id, agent_id);
CREATE INDEX IF NOT EXISTS discussion_conversation_workspace ON discussion_conversation (tenant_id, workspace_id, id);
CREATE INDEX IF NOT EXISTS discussion_participant_access ON discussion_participant (tenant_id, workspace_id, principal_id, conversation_id);
CREATE INDEX IF NOT EXISTS discussion_reaction_sample ON discussion_reaction (tenant_id, message_id, reaction, id);
CREATE INDEX IF NOT EXISTS discussion_reaction_workspace ON discussion_reaction (tenant_id, workspace_id);
CREATE INDEX IF NOT EXISTS idx_draft_client ON draft (tenant_id, client_id);
CREATE INDEX IF NOT EXISTS idx_event_type_time ON event (tenant_id, event_type, timestamp);
CREATE INDEX IF NOT EXISTS idx_event_session ON event (tenant_id, session_id);
CREATE INDEX IF NOT EXISTS idx_event_type ON event (tenant_id, event_type);
CREATE INDEX IF NOT EXISTS idx_event_ws_time ON event (tenant_id, workspace_id, timestamp);
-- The seq cursor (event_repo.rs EventSeq): events_after and max_event_seq.
CREATE UNIQUE INDEX IF NOT EXISTS idx_event_row_seq ON event (tenant_id, row_seq);
CREATE INDEX IF NOT EXISTS idx_event_subscription_agent ON event_subscription (tenant_id, subscriber_agent_id);
CREATE INDEX IF NOT EXISTS executor_credential_active ON executor_credential (tenant_id, created_at, id) WHERE revoked_at IS NULL;
CREATE INDEX IF NOT EXISTS home_move_node ON home_move (tenant_id, node_id);
CREATE INDEX IF NOT EXISTS home_stop_workspace ON home_stop (tenant_id, workspace_id);
CREATE INDEX IF NOT EXISTS home_stop_node ON home_stop (tenant_id, node_id);
CREATE INDEX IF NOT EXISTS idx_hook_active_workspace ON hook (tenant_id, workspace_id, created_at) WHERE state IN ('scheduled', 'running');
CREATE INDEX IF NOT EXISTS idx_hook_active_agent ON hook (tenant_id, agent_id, created_at) WHERE state IN ('scheduled', 'running');
CREATE INDEX IF NOT EXISTS idx_hook_state ON hook (tenant_id, state);
CREATE INDEX IF NOT EXISTS idx_hook_agent ON hook (tenant_id, agent_id);
CREATE INDEX IF NOT EXISTS idx_hook_workspace ON hook (tenant_id, workspace_id);
CREATE INDEX IF NOT EXISTS host_invite_order_idx ON host_invite (tenant_id, created_at, id);
CREATE INDEX IF NOT EXISTS host_member_order_idx ON host_member (tenant_id, added_at, principal_id);
CREATE INDEX IF NOT EXISTS idx_idempotency_created ON idempotency_key (tenant_id, created_at);
CREATE INDEX IF NOT EXISTS idx_interrupted_resolution ON interrupted_agent (tenant_id, resolution);
CREATE INDEX IF NOT EXISTS idx_interrupted_workspace ON interrupted_agent (tenant_id, workspace_id);
CREATE INDEX IF NOT EXISTS node_assignment_lease ON node_assignment (tenant_id, lease_id);
CREATE INDEX IF NOT EXISTS node_invite_open ON node_invite (tenant_id, created_at, id) WHERE used_at IS NULL AND revoked_at IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS node_lease_live ON node_lease (tenant_id, node_id) WHERE terminal = 0;
CREATE INDEX IF NOT EXISTS idx_note_workspace_parented ON note (tenant_id, workspace_id, parent_id) WHERE parent_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_note_workspace_updated_at ON note (tenant_id, workspace_id, updated_at);
CREATE INDEX IF NOT EXISTS idx_note_task ON note (tenant_id, workspace_id) WHERE task_json IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_note_parent ON note (tenant_id, parent_id);
CREATE INDEX IF NOT EXISTS idx_note_workspace ON note (tenant_id, workspace_id);
CREATE INDEX IF NOT EXISTS note_search_tsv ON note USING gin (search_tsv);
CREATE INDEX IF NOT EXISTS idx_note_line_attribution_workspace ON note_line_attribution (tenant_id, workspace_id);
CREATE INDEX IF NOT EXISTS note_page_map_end ON note_page_entry (tenant_id, workspace_id, note_id, collection, source_end, position) WHERE source_end IS NOT NULL;
CREATE INDEX IF NOT EXISTS note_page_map_start ON note_page_entry (tenant_id, workspace_id, note_id, collection, source_start, position) WHERE source_start IS NOT NULL;
CREATE INDEX IF NOT EXISTS note_page_pending ON note_page_head (tenant_id, workspace_id, note_id) WHERE indexed_rev=-1;
CREATE INDEX IF NOT EXISTS idx_note_version_rev ON note_version (tenant_id, workspace_id, note_id, rev);
CREATE UNIQUE INDEX IF NOT EXISTS idx_pr_monitor_qualified_workspace_identity ON pr_monitor (tenant_id, workspace_id, target_provider, target_instance_base_url, (CASE WHEN target_provider = 'github' THEN lower(target_project_path) ELSE target_project_path END), target_kind, pr_number) WHERE state = 'active' AND target_provenance != 'unresolved';
CREATE UNIQUE INDEX IF NOT EXISTS idx_pr_monitor_qualified_identity ON pr_monitor (tenant_id, agent_id, target_provider, target_instance_base_url, (CASE WHEN target_provider = 'github' THEN lower(target_project_path) ELSE target_project_path END), target_kind, pr_number) WHERE state = 'active' AND target_provenance != 'unresolved';
CREATE UNIQUE INDEX IF NOT EXISTS idx_pr_monitor_unresolved_claim_workspace_identity ON pr_monitor (tenant_id, workspace_id, (lower(repo_owner)), (lower(repo_name)), pr_number, (ARRAY[(target_provider)::text, (target_instance_base_url)::text, (target_project_path)::text, (target_kind)::text])) WHERE state = 'active' AND target_provenance = 'unresolved'
    AND (target_provider IS NOT NULL OR target_instance_base_url IS NOT NULL
      OR target_project_path IS NOT NULL OR target_kind IS NOT NULL);
CREATE UNIQUE INDEX IF NOT EXISTS idx_pr_monitor_unresolved_claim_identity ON pr_monitor (tenant_id, agent_id, (lower(repo_owner)), (lower(repo_name)), pr_number, (ARRAY[(target_provider)::text, (target_instance_base_url)::text, (target_project_path)::text, (target_kind)::text])) WHERE state = 'active' AND target_provenance = 'unresolved'
    AND (target_provider IS NOT NULL OR target_instance_base_url IS NOT NULL
      OR target_project_path IS NOT NULL OR target_kind IS NOT NULL);
CREATE UNIQUE INDEX IF NOT EXISTS idx_pr_monitor_workspace_identity ON pr_monitor (tenant_id, workspace_id, (lower(repo_owner)), (lower(repo_name)), pr_number) WHERE state = 'active' AND ((target_provenance = 'unresolved'
    AND target_provider IS NULL AND target_instance_base_url IS NULL
    AND target_project_path IS NULL AND target_kind IS NULL)
    OR (target_provenance != 'unresolved' AND target_provider = 'github' AND target_instance_base_url = 'https://github.com'
      AND target_kind = 'pull-request'));
CREATE UNIQUE INDEX IF NOT EXISTS idx_pr_monitor_identity ON pr_monitor (tenant_id, agent_id, (lower(repo_owner)), (lower(repo_name)), pr_number) WHERE state = 'active' AND ((target_provenance = 'unresolved'
    AND target_provider IS NULL AND target_instance_base_url IS NULL
    AND target_project_path IS NULL AND target_kind IS NULL)
    OR (target_provenance != 'unresolved' AND target_provider = 'github' AND target_instance_base_url = 'https://github.com'
      AND target_kind = 'pull-request'));
CREATE INDEX IF NOT EXISTS idx_pr_monitor_state ON pr_monitor (tenant_id, state);
CREATE INDEX IF NOT EXISTS idx_pr_monitor_agent ON pr_monitor (tenant_id, agent_id);
CREATE INDEX IF NOT EXISTS idx_pr_monitor_workspace ON pr_monitor (tenant_id, workspace_id);
CREATE UNIQUE INDEX IF NOT EXISTS principal_identity_uq ON principal (tenant_id, identity_provider, instance_host, external_user_id) WHERE identity_provider IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS principal_primary_uq ON principal (tenant_id, is_primary) WHERE is_primary = 1;
CREATE INDEX IF NOT EXISTS principal_credential_principal_idx ON principal_credential (tenant_id, principal_id);
CREATE INDEX IF NOT EXISTS idx_pull_request_workspace_workspace ON pull_request_workspace (tenant_id, workspace_id);
CREATE INDEX IF NOT EXISTS remote_agent_lease ON remote_agent (tenant_id, lease_id);
CREATE INDEX IF NOT EXISTS remote_final_stop_lease ON remote_final_stop (tenant_id, lease_id);
CREATE INDEX IF NOT EXISTS script_active_page ON script (tenant_id, workspace_id, created_at DESC, id DESC) WHERE archived_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_script_workspace ON script (tenant_id, workspace_id);
CREATE INDEX IF NOT EXISTS script_monitor_workspace ON script_monitor (tenant_id, workspace_id, created_at, id);
CREATE INDEX IF NOT EXISTS script_monitor_agent ON script_monitor (tenant_id, agent_id, state);
CREATE UNIQUE INDEX IF NOT EXISTS script_monitor_owner ON script_monitor (tenant_id, workspace_id, script_id);
CREATE INDEX IF NOT EXISTS script_notification_owner ON script_notification_job (tenant_id, agent_id);
CREATE INDEX IF NOT EXISTS idx_task_agent_link_agent ON task_agent_link (tenant_id, workspace_id, agent_id);
CREATE INDEX IF NOT EXISTS idx_task_agent_link_workspace ON task_agent_link (tenant_id, workspace_id);
CREATE INDEX IF NOT EXISTS idx_tracked_changes_ws_path_stage_agent ON tracked_changes (tenant_id, workspace_id, path, stage, agent_id);
CREATE INDEX IF NOT EXISTS idx_tracked_changes_agent ON tracked_changes (tenant_id, agent_id);
CREATE INDEX IF NOT EXISTS idx_tracked_changes_commit ON tracked_changes (tenant_id, commit_hash);
CREATE INDEX IF NOT EXISTS idx_tracked_changes_ws ON tracked_changes (tenant_id, workspace_id);
CREATE INDEX IF NOT EXISTS idx_workspace_context_item_workspace_ordinal ON workspace_context_item (tenant_id, workspace_id, ordinal);
CREATE UNIQUE INDEX IF NOT EXISTS workspace_discussion_creation_retry ON workspace_discussion_conversation (tenant_id, workspace_id, first_principal_id, first_client_message_id);
CREATE UNIQUE INDEX IF NOT EXISTS discussion_agent_retry ON workspace_discussion_message (tenant_id, workspace_id, agent_id, client_message_id);
CREATE UNIQUE INDEX IF NOT EXISTS discussion_message_scope ON workspace_discussion_message (tenant_id, workspace_id, id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_workspace_git_root_identity ON workspace_git_root (tenant_id, workspace_id, path);
CREATE INDEX IF NOT EXISTS idx_workspace_git_root_workspace ON workspace_git_root (tenant_id, workspace_id);
CREATE INDEX IF NOT EXISTS workspace_home_node ON workspace_home (tenant_id, node_id);
CREATE INDEX IF NOT EXISTS workspace_invite_issuer_idx ON workspace_invite (tenant_id, created_by_principal_id);
CREATE INDEX IF NOT EXISTS workspace_invite_workspace_idx ON workspace_invite (tenant_id, workspace_id);
CREATE INDEX IF NOT EXISTS workspace_invite_seat_expiry_idx ON workspace_invite_seat (tenant_id, workspace_id, expires_at);
CREATE UNIQUE INDEX IF NOT EXISTS workspace_member_owner_uq ON workspace_member (tenant_id, workspace_id) WHERE role = 'owner';
CREATE INDEX IF NOT EXISTS workspace_member_principal_idx ON workspace_member (tenant_id, principal_id);

-- Foreign keys (tenant_id is part of every key, so cascades stay inside one tenant)
ALTER TABLE advisory_wake_delivery DROP CONSTRAINT IF EXISTS advisory_wake_delivery_child_agent_id_fkey, ADD CONSTRAINT advisory_wake_delivery_child_agent_id_fkey FOREIGN KEY (tenant_id, child_agent_id) REFERENCES agent_session (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE advisory_wake_delivery DROP CONSTRAINT IF EXISTS advisory_wake_delivery_parent_agent_id_fkey, ADD CONSTRAINT advisory_wake_delivery_parent_agent_id_fkey FOREIGN KEY (tenant_id, parent_agent_id) REFERENCES agent_session (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE agent_browser_sender DROP CONSTRAINT IF EXISTS agent_browser_sender_tenant_id_agent_id_fkey, ADD CONSTRAINT agent_browser_sender_tenant_id_agent_id_fkey FOREIGN KEY (tenant_id, agent_id) REFERENCES agent_session (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE agent_browser_sender DROP CONSTRAINT IF EXISTS agent_browser_sender_tenant_id_client_id_fkey, ADD CONSTRAINT agent_browser_sender_tenant_id_client_id_fkey FOREIGN KEY (tenant_id, client_id) REFERENCES client (tenant_id, id) ON DELETE SET NULL (client_id);
ALTER TABLE agent_flipped_completion DROP CONSTRAINT IF EXISTS agent_flipped_completion_agent_id_fkey, ADD CONSTRAINT agent_flipped_completion_agent_id_fkey FOREIGN KEY (tenant_id, agent_id) REFERENCES agent_session (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE agent_message DROP CONSTRAINT IF EXISTS agent_message_agent_id_fkey, ADD CONSTRAINT agent_message_agent_id_fkey FOREIGN KEY (tenant_id, agent_id) REFERENCES agent_session (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE agent_message_payload DROP CONSTRAINT IF EXISTS agent_message_payload_agent_id_fkey, ADD CONSTRAINT agent_message_payload_agent_id_fkey FOREIGN KEY (tenant_id, agent_id) REFERENCES agent_session (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE agent_metrics DROP CONSTRAINT IF EXISTS agent_metrics_workspace_id_fkey, ADD CONSTRAINT agent_metrics_workspace_id_fkey FOREIGN KEY (tenant_id, workspace_id) REFERENCES workspace (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE agent_queue DROP CONSTRAINT IF EXISTS agent_queue_agent_id_fkey, ADD CONSTRAINT agent_queue_agent_id_fkey FOREIGN KEY (tenant_id, agent_id) REFERENCES agent_session (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE agent_queue_admission DROP CONSTRAINT IF EXISTS agent_queue_admission_agent_id_fkey, ADD CONSTRAINT agent_queue_admission_agent_id_fkey FOREIGN KEY (tenant_id, agent_id) REFERENCES agent_session (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE agent_queue_version DROP CONSTRAINT IF EXISTS agent_queue_version_tenant_id_agent_id_fkey, ADD CONSTRAINT agent_queue_version_tenant_id_agent_id_fkey FOREIGN KEY (tenant_id, agent_id) REFERENCES agent_session (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE agent_session DROP CONSTRAINT IF EXISTS agent_session_workspace_id_fkey, ADD CONSTRAINT agent_session_workspace_id_fkey FOREIGN KEY (tenant_id, workspace_id) REFERENCES workspace (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE agent_stop_redelivery DROP CONSTRAINT IF EXISTS agent_stop_redelivery_agent_id_fkey, ADD CONSTRAINT agent_stop_redelivery_agent_id_fkey FOREIGN KEY (tenant_id, agent_id) REFERENCES agent_session (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE agent_usage_cell DROP CONSTRAINT IF EXISTS agent_usage_cell_agent_id_fkey, ADD CONSTRAINT agent_usage_cell_agent_id_fkey FOREIGN KEY (tenant_id, agent_id) REFERENCES agent_session (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE attachment_idempotency_keys DROP CONSTRAINT IF EXISTS attachment_idempotency_keys_attachment_id_fkey, ADD CONSTRAINT attachment_idempotency_keys_attachment_id_fkey FOREIGN KEY (tenant_id, attachment_id) REFERENCES attachments (tenant_id, id);
ALTER TABLE browser_tab DROP CONSTRAINT IF EXISTS browser_tab_host_client_id_fkey, ADD CONSTRAINT browser_tab_host_client_id_fkey FOREIGN KEY (tenant_id, host_client_id) REFERENCES client (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE browser_tab DROP CONSTRAINT IF EXISTS browser_tab_workspace_id_fkey, ADD CONSTRAINT browser_tab_workspace_id_fkey FOREIGN KEY (tenant_id, workspace_id) REFERENCES workspace (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE comment DROP CONSTRAINT IF EXISTS comment_note_id_workspace_id_fkey, ADD CONSTRAINT comment_note_id_workspace_id_fkey FOREIGN KEY (tenant_id, note_id, workspace_id) REFERENCES note (tenant_id, id, workspace_id) ON DELETE CASCADE;
ALTER TABLE completion_wake_delivery DROP CONSTRAINT IF EXISTS completion_wake_delivery_child_agent_id_fkey, ADD CONSTRAINT completion_wake_delivery_child_agent_id_fkey FOREIGN KEY (tenant_id, child_agent_id) REFERENCES agent_session (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE completion_wake_delivery DROP CONSTRAINT IF EXISTS completion_wake_delivery_parent_agent_id_fkey, ADD CONSTRAINT completion_wake_delivery_parent_agent_id_fkey FOREIGN KEY (tenant_id, parent_agent_id) REFERENCES agent_session (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE delegation_group DROP CONSTRAINT IF EXISTS delegation_group_workspace_id_fkey, ADD CONSTRAINT delegation_group_workspace_id_fkey FOREIGN KEY (tenant_id, workspace_id) REFERENCES workspace (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE diffs DROP CONSTRAINT IF EXISTS diffs_workspace_id_fkey, ADD CONSTRAINT diffs_workspace_id_fkey FOREIGN KEY (tenant_id, workspace_id) REFERENCES workspace (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE discussion_agent_activity DROP CONSTRAINT IF EXISTS discussion_agent_activity_message_id_fkey, ADD CONSTRAINT discussion_agent_activity_message_id_fkey FOREIGN KEY (tenant_id, message_id) REFERENCES workspace_discussion_message (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE discussion_agent_delivery DROP CONSTRAINT IF EXISTS discussion_agent_delivery_workspace_id_fkey, ADD CONSTRAINT discussion_agent_delivery_workspace_id_fkey FOREIGN KEY (tenant_id, workspace_id) REFERENCES workspace (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE discussion_agent_delivery DROP CONSTRAINT IF EXISTS discussion_agent_delivery_agent_id_fkey, ADD CONSTRAINT discussion_agent_delivery_agent_id_fkey FOREIGN KEY (tenant_id, agent_id) REFERENCES agent_session (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE discussion_agent_delivery DROP CONSTRAINT IF EXISTS discussion_agent_delivery_message_id_fkey, ADD CONSTRAINT discussion_agent_delivery_message_id_fkey FOREIGN KEY (tenant_id, message_id) REFERENCES workspace_discussion_message (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE discussion_agent_grant DROP CONSTRAINT IF EXISTS discussion_agent_grant_agent_id_fkey, ADD CONSTRAINT discussion_agent_grant_agent_id_fkey FOREIGN KEY (tenant_id, agent_id) REFERENCES agent_session (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE discussion_agent_grant DROP CONSTRAINT IF EXISTS discussion_agent_grant_workspace_id_fkey, ADD CONSTRAINT discussion_agent_grant_workspace_id_fkey FOREIGN KEY (tenant_id, workspace_id) REFERENCES workspace (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE discussion_conversation DROP CONSTRAINT IF EXISTS discussion_conversation_workspace_id_fkey, ADD CONSTRAINT discussion_conversation_workspace_id_fkey FOREIGN KEY (tenant_id, workspace_id) REFERENCES workspace (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE discussion_participant DROP CONSTRAINT IF EXISTS discussion_participant_workspace_id_fkey, ADD CONSTRAINT discussion_participant_workspace_id_fkey FOREIGN KEY (tenant_id, workspace_id) REFERENCES workspace (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE discussion_participant DROP CONSTRAINT IF EXISTS discussion_participant_conversation_id_fkey, ADD CONSTRAINT discussion_participant_conversation_id_fkey FOREIGN KEY (tenant_id, conversation_id) REFERENCES discussion_conversation (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE discussion_reaction DROP CONSTRAINT IF EXISTS discussion_reaction_workspace_id_message_id_fkey, ADD CONSTRAINT discussion_reaction_workspace_id_message_id_fkey FOREIGN KEY (tenant_id, workspace_id, message_id) REFERENCES workspace_discussion_message (tenant_id, workspace_id, id) ON DELETE CASCADE;
ALTER TABLE discussion_reaction DROP CONSTRAINT IF EXISTS discussion_reaction_workspace_id_fkey, ADD CONSTRAINT discussion_reaction_workspace_id_fkey FOREIGN KEY (tenant_id, workspace_id) REFERENCES workspace (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE draft DROP CONSTRAINT IF EXISTS draft_client_id_fkey, ADD CONSTRAINT draft_client_id_fkey FOREIGN KEY (tenant_id, client_id) REFERENCES client (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE executor_binding DROP CONSTRAINT IF EXISTS executor_binding_credential_id_fkey, ADD CONSTRAINT executor_binding_credential_id_fkey FOREIGN KEY (tenant_id, credential_id) REFERENCES executor_credential (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE home_move DROP CONSTRAINT IF EXISTS home_move_workspace_id_fkey, ADD CONSTRAINT home_move_workspace_id_fkey FOREIGN KEY (tenant_id, workspace_id) REFERENCES workspace (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE home_rebuild DROP CONSTRAINT IF EXISTS home_rebuild_workspace_id_fkey, ADD CONSTRAINT home_rebuild_workspace_id_fkey FOREIGN KEY (tenant_id, workspace_id) REFERENCES workspace (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE hook DROP CONSTRAINT IF EXISTS hook_agent_id_fkey, ADD CONSTRAINT hook_agent_id_fkey FOREIGN KEY (tenant_id, agent_id) REFERENCES agent_session (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE hook DROP CONSTRAINT IF EXISTS hook_workspace_id_fkey, ADD CONSTRAINT hook_workspace_id_fkey FOREIGN KEY (tenant_id, workspace_id) REFERENCES workspace (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE host_invite DROP CONSTRAINT IF EXISTS host_invite_redeemed_by_principal_id_fkey, ADD CONSTRAINT host_invite_redeemed_by_principal_id_fkey FOREIGN KEY (tenant_id, redeemed_by_principal_id) REFERENCES principal (tenant_id, id);
ALTER TABLE host_invite DROP CONSTRAINT IF EXISTS host_invite_created_by_principal_id_fkey, ADD CONSTRAINT host_invite_created_by_principal_id_fkey FOREIGN KEY (tenant_id, created_by_principal_id) REFERENCES principal (tenant_id, id);
ALTER TABLE host_member DROP CONSTRAINT IF EXISTS host_member_principal_id_fkey, ADD CONSTRAINT host_member_principal_id_fkey FOREIGN KEY (tenant_id, principal_id) REFERENCES principal (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE node_assignment DROP CONSTRAINT IF EXISTS node_assignment_lease_id_fkey, ADD CONSTRAINT node_assignment_lease_id_fkey FOREIGN KEY (tenant_id, lease_id) REFERENCES node_lease (tenant_id, id);
ALTER TABLE node_checkpoint DROP CONSTRAINT IF EXISTS node_checkpoint_agent_id_fkey, ADD CONSTRAINT node_checkpoint_agent_id_fkey FOREIGN KEY (tenant_id, agent_id) REFERENCES node_assignment (tenant_id, agent_id);
ALTER TABLE node_invite DROP CONSTRAINT IF EXISTS node_invite_credential_id_fkey, ADD CONSTRAINT node_invite_credential_id_fkey FOREIGN KEY (tenant_id, credential_id) REFERENCES executor_credential (tenant_id, id) ON DELETE SET NULL (credential_id);
ALTER TABLE node_lease DROP CONSTRAINT IF EXISTS node_lease_node_id_fkey, ADD CONSTRAINT node_lease_node_id_fkey FOREIGN KEY (tenant_id, node_id) REFERENCES execution_node (tenant_id, id);
ALTER TABLE note DROP CONSTRAINT IF EXISTS note_workspace_id_fkey, ADD CONSTRAINT note_workspace_id_fkey FOREIGN KEY (tenant_id, workspace_id) REFERENCES workspace (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE note_line_attribution DROP CONSTRAINT IF EXISTS note_line_attribution_note_id_workspace_id_fkey, ADD CONSTRAINT note_line_attribution_note_id_workspace_id_fkey FOREIGN KEY (tenant_id, note_id, workspace_id) REFERENCES note (tenant_id, id, workspace_id) ON DELETE CASCADE;
ALTER TABLE note_page_entry DROP CONSTRAINT IF EXISTS note_page_entry_workspace_id_note_id_fkey, ADD CONSTRAINT note_page_entry_workspace_id_note_id_fkey FOREIGN KEY (tenant_id, workspace_id, note_id) REFERENCES note_page_head (tenant_id, workspace_id, note_id) ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE note_page_head DROP CONSTRAINT IF EXISTS note_page_head_note_id_workspace_id_fkey, ADD CONSTRAINT note_page_head_note_id_workspace_id_fkey FOREIGN KEY (tenant_id, note_id, workspace_id) REFERENCES note (tenant_id, id, workspace_id) ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE note_version DROP CONSTRAINT IF EXISTS note_version_note_id_workspace_id_fkey, ADD CONSTRAINT note_version_note_id_workspace_id_fkey FOREIGN KEY (tenant_id, note_id, workspace_id) REFERENCES note (tenant_id, id, workspace_id) ON DELETE CASCADE;
ALTER TABLE pr_monitor DROP CONSTRAINT IF EXISTS pr_monitor_agent_id_fkey, ADD CONSTRAINT pr_monitor_agent_id_fkey FOREIGN KEY (tenant_id, agent_id) REFERENCES agent_session (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE pr_monitor DROP CONSTRAINT IF EXISTS pr_monitor_workspace_id_fkey, ADD CONSTRAINT pr_monitor_workspace_id_fkey FOREIGN KEY (tenant_id, workspace_id) REFERENCES workspace (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE pr_monitor_message_provenance DROP CONSTRAINT IF EXISTS pr_monitor_message_provenance_message_id_agent_id_fkey, ADD CONSTRAINT pr_monitor_message_provenance_message_id_agent_id_fkey FOREIGN KEY (tenant_id, message_id, agent_id) REFERENCES agent_message (tenant_id, id, agent_id) ON DELETE CASCADE;
ALTER TABLE pr_monitor_message_provenance DROP CONSTRAINT IF EXISTS pr_monitor_message_provenance_workspace_id_fkey, ADD CONSTRAINT pr_monitor_message_provenance_workspace_id_fkey FOREIGN KEY (tenant_id, workspace_id) REFERENCES workspace (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE principal_credential DROP CONSTRAINT IF EXISTS principal_credential_principal_id_fkey, ADD CONSTRAINT principal_credential_principal_id_fkey FOREIGN KEY (tenant_id, principal_id) REFERENCES principal (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE principal_revocation DROP CONSTRAINT IF EXISTS principal_revocation_principal_id_fkey, ADD CONSTRAINT principal_revocation_principal_id_fkey FOREIGN KEY (tenant_id, principal_id) REFERENCES principal (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE pull_request_workspace DROP CONSTRAINT IF EXISTS pull_request_workspace_workspace_id_fkey, ADD CONSTRAINT pull_request_workspace_workspace_id_fkey FOREIGN KEY (tenant_id, workspace_id) REFERENCES workspace (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE remote_agent DROP CONSTRAINT IF EXISTS remote_agent_lease_id_fkey, ADD CONSTRAINT remote_agent_lease_id_fkey FOREIGN KEY (tenant_id, lease_id) REFERENCES node_lease (tenant_id, id);
ALTER TABLE remote_agent DROP CONSTRAINT IF EXISTS remote_agent_node_id_fkey, ADD CONSTRAINT remote_agent_node_id_fkey FOREIGN KEY (tenant_id, node_id) REFERENCES execution_node (tenant_id, id);
ALTER TABLE remote_agent DROP CONSTRAINT IF EXISTS remote_agent_agent_id_fkey, ADD CONSTRAINT remote_agent_agent_id_fkey FOREIGN KEY (tenant_id, agent_id) REFERENCES agent_session (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE review_context_enrollment DROP CONSTRAINT IF EXISTS review_context_enrollment_workspace_id_fkey, ADD CONSTRAINT review_context_enrollment_workspace_id_fkey FOREIGN KEY (tenant_id, workspace_id) REFERENCES workspace (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE review_context_enrollment DROP CONSTRAINT IF EXISTS review_context_enrollment_agent_id_fkey, ADD CONSTRAINT review_context_enrollment_agent_id_fkey FOREIGN KEY (tenant_id, agent_id) REFERENCES agent_session (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE task_agent_link DROP CONSTRAINT IF EXISTS task_agent_link_workspace_id_fkey, ADD CONSTRAINT task_agent_link_workspace_id_fkey FOREIGN KEY (tenant_id, workspace_id) REFERENCES workspace (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE tracked_changes DROP CONSTRAINT IF EXISTS tracked_changes_workspace_id_fkey, ADD CONSTRAINT tracked_changes_workspace_id_fkey FOREIGN KEY (tenant_id, workspace_id) REFERENCES workspace (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE workspace_context_item DROP CONSTRAINT IF EXISTS workspace_context_item_workspace_id_fkey, ADD CONSTRAINT workspace_context_item_workspace_id_fkey FOREIGN KEY (tenant_id, workspace_id) REFERENCES workspace (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE workspace_discussion_conversation DROP CONSTRAINT IF EXISTS workspace_discussion_conversation_workspace_id_fkey, ADD CONSTRAINT workspace_discussion_conversation_workspace_id_fkey FOREIGN KEY (tenant_id, workspace_id) REFERENCES workspace (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE workspace_discussion_message DROP CONSTRAINT IF EXISTS workspace_discussion_message_workspace_id_fkey, ADD CONSTRAINT workspace_discussion_message_workspace_id_fkey FOREIGN KEY (tenant_id, workspace_id) REFERENCES workspace (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE workspace_git_root DROP CONSTRAINT IF EXISTS workspace_git_root_workspace_id_fkey, ADD CONSTRAINT workspace_git_root_workspace_id_fkey FOREIGN KEY (tenant_id, workspace_id) REFERENCES workspace (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE workspace_home DROP CONSTRAINT IF EXISTS workspace_home_workspace_id_fkey, ADD CONSTRAINT workspace_home_workspace_id_fkey FOREIGN KEY (tenant_id, workspace_id) REFERENCES workspace (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE workspace_invite DROP CONSTRAINT IF EXISTS workspace_invite_redeemed_by_principal_id_fkey, ADD CONSTRAINT workspace_invite_redeemed_by_principal_id_fkey FOREIGN KEY (tenant_id, redeemed_by_principal_id) REFERENCES principal (tenant_id, id) ON DELETE SET NULL (redeemed_by_principal_id);
ALTER TABLE workspace_invite DROP CONSTRAINT IF EXISTS workspace_invite_created_by_principal_id_fkey, ADD CONSTRAINT workspace_invite_created_by_principal_id_fkey FOREIGN KEY (tenant_id, created_by_principal_id) REFERENCES principal (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE workspace_invite DROP CONSTRAINT IF EXISTS workspace_invite_workspace_id_fkey, ADD CONSTRAINT workspace_invite_workspace_id_fkey FOREIGN KEY (tenant_id, workspace_id) REFERENCES workspace (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE workspace_invite_seat DROP CONSTRAINT IF EXISTS workspace_invite_seat_workspace_id_fkey, ADD CONSTRAINT workspace_invite_seat_workspace_id_fkey FOREIGN KEY (tenant_id, workspace_id) REFERENCES workspace (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE workspace_invite_seat DROP CONSTRAINT IF EXISTS workspace_invite_seat_invite_id_fkey, ADD CONSTRAINT workspace_invite_seat_invite_id_fkey FOREIGN KEY (tenant_id, invite_id) REFERENCES workspace_invite (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE workspace_mcp_disabled_server DROP CONSTRAINT IF EXISTS workspace_mcp_disabled_server_workspace_id_fkey, ADD CONSTRAINT workspace_mcp_disabled_server_workspace_id_fkey FOREIGN KEY (tenant_id, workspace_id) REFERENCES workspace (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE workspace_member DROP CONSTRAINT IF EXISTS workspace_member_principal_id_fkey, ADD CONSTRAINT workspace_member_principal_id_fkey FOREIGN KEY (tenant_id, principal_id) REFERENCES principal (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE workspace_member DROP CONSTRAINT IF EXISTS workspace_member_workspace_id_fkey, ADD CONSTRAINT workspace_member_workspace_id_fkey FOREIGN KEY (tenant_id, workspace_id) REFERENCES workspace (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE workspace_metrics DROP CONSTRAINT IF EXISTS workspace_metrics_workspace_id_fkey, ADD CONSTRAINT workspace_metrics_workspace_id_fkey FOREIGN KEY (tenant_id, workspace_id) REFERENCES workspace (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE workspace_sharing_summary DROP CONSTRAINT IF EXISTS workspace_sharing_summary_workspace_id_fkey, ADD CONSTRAINT workspace_sharing_summary_workspace_id_fkey FOREIGN KEY (tenant_id, workspace_id) REFERENCES workspace (tenant_id, id) ON DELETE CASCADE;
ALTER TABLE workspace_ui_context DROP CONSTRAINT IF EXISTS workspace_ui_context_workspace_id_fkey, ADD CONSTRAINT workspace_ui_context_workspace_id_fkey FOREIGN KEY (tenant_id, workspace_id) REFERENCES workspace (tenant_id, id) ON DELETE CASCADE;

-- Triggers ported from SQLite. Every statement is scoped to the row's tenant.

CREATE OR REPLACE FUNCTION trg_agent_message_agent_guard_for_payloads() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT COALESCE((EXISTS(SELECT 1 FROM agent_message_payload AS p WHERE (p.message_id = new.id AND p.agent_id <> new.agent_id) AND p.tenant_id = new.tenant_id)), false) THEN
        RETURN NEW;
    END IF;
    IF true THEN
        RAISE EXCEPTION '%', 'agent_message.agent_id does not match its staged agent_message_payload rows';
    END IF;
    RETURN NEW;
END
$$;
CREATE OR REPLACE TRIGGER agent_message_agent_guard_for_payloads BEFORE INSERT ON agent_message
    FOR EACH ROW EXECUTE FUNCTION trg_agent_message_agent_guard_for_payloads();

CREATE OR REPLACE FUNCTION trg_agent_message_payload_after_message_delete() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM agent_message_payload WHERE message_id = old.id AND agent_message_payload.tenant_id = old.tenant_id;
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER agent_message_payload_after_message_delete AFTER DELETE ON agent_message
    FOR EACH ROW EXECUTE FUNCTION trg_agent_message_payload_after_message_delete();

CREATE OR REPLACE FUNCTION trg_agent_message_payload_agent_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT COALESCE((EXISTS(SELECT 1 FROM agent_message AS m WHERE (m.id = new.message_id AND m.agent_id <> new.agent_id) AND m.tenant_id = new.tenant_id)), false) THEN
        RETURN NEW;
    END IF;
    IF true THEN
        RAISE EXCEPTION '%', 'agent_message_payload.agent_id does not match the owning agent_message';
    END IF;
    RETURN NEW;
END
$$;
CREATE OR REPLACE TRIGGER agent_message_payload_agent_guard BEFORE INSERT ON agent_message_payload
    FOR EACH ROW EXECUTE FUNCTION trg_agent_message_payload_agent_guard();

CREATE OR REPLACE FUNCTION trg_agent_session_stats_after_message_delete() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE agent_session SET message_count = message_count - 1, assistant_message_count = assistant_message_count - CAST(((old.role = 'assistant')) AS INT), conversation_bytes = conversation_bytes - OCTET_LENGTH(old.content) WHERE id = old.agent_id AND agent_session.tenant_id = old.tenant_id;
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER agent_session_stats_after_message_delete AFTER DELETE ON agent_message
    FOR EACH ROW EXECUTE FUNCTION trg_agent_session_stats_after_message_delete();

CREATE OR REPLACE FUNCTION trg_agent_session_stats_after_message_insert() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE agent_session SET message_count = message_count + 1, assistant_message_count = assistant_message_count + CAST(((new.role = 'assistant')) AS INT), conversation_bytes = conversation_bytes + OCTET_LENGTH(new.content) WHERE id = new.agent_id AND agent_session.tenant_id = new.tenant_id;
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER agent_session_stats_after_message_insert AFTER INSERT ON agent_message
    FOR EACH ROW EXECUTE FUNCTION trg_agent_session_stats_after_message_insert();

CREATE OR REPLACE FUNCTION trg_agent_session_stats_after_message_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE agent_session SET assistant_message_count = assistant_message_count + CAST(((new.role = 'assistant')) AS INT) - CAST(((old.role = 'assistant')) AS INT), conversation_bytes = conversation_bytes + OCTET_LENGTH(new.content) - OCTET_LENGTH(old.content) WHERE id = new.agent_id AND agent_session.tenant_id = new.tenant_id;
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER agent_session_stats_after_message_update AFTER UPDATE OF role, content ON agent_message
    FOR EACH ROW EXECUTE FUNCTION trg_agent_session_stats_after_message_update();

CREATE OR REPLACE FUNCTION trg_agent_session_stats_after_payload_delete() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE agent_session SET conversation_bytes = conversation_bytes - OCTET_LENGTH(old.body) WHERE id = old.agent_id AND agent_session.tenant_id = old.tenant_id;
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER agent_session_stats_after_payload_delete AFTER DELETE ON agent_message_payload
    FOR EACH ROW EXECUTE FUNCTION trg_agent_session_stats_after_payload_delete();

CREATE OR REPLACE FUNCTION trg_agent_session_stats_after_payload_insert() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE agent_session SET conversation_bytes = conversation_bytes + OCTET_LENGTH(new.body) WHERE id = new.agent_id AND agent_session.tenant_id = new.tenant_id;
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER agent_session_stats_after_payload_insert AFTER INSERT ON agent_message_payload
    FOR EACH ROW EXECUTE FUNCTION trg_agent_session_stats_after_payload_insert();

CREATE OR REPLACE FUNCTION trg_agent_session_stats_after_payload_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE agent_session SET conversation_bytes = conversation_bytes - OCTET_LENGTH(old.body) + OCTET_LENGTH(new.body) WHERE id = new.agent_id AND agent_session.tenant_id = new.tenant_id;
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER agent_session_stats_after_payload_update AFTER UPDATE OF body ON agent_message_payload
    FOR EACH ROW EXECUTE FUNCTION trg_agent_session_stats_after_payload_update();

CREATE OR REPLACE FUNCTION trg_host_member_added_ai() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE host_membership_state SET revision = revision + 1, member_count = member_count + 1 WHERE id = 1 AND host_membership_state.tenant_id = new.tenant_id;
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER host_member_added_ai AFTER INSERT ON host_member
    FOR EACH ROW EXECUTE FUNCTION trg_host_member_added_ai();

CREATE OR REPLACE FUNCTION trg_host_member_no_primary_bi() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT COALESCE((EXISTS(SELECT 1 FROM principal WHERE (id = NEW.principal_id AND is_primary = 1) AND principal.tenant_id = new.tenant_id)), false) THEN
        RETURN NEW;
    END IF;
    IF true THEN
        RAISE EXCEPTION '%', 'the primary principal is already the host owner';
    END IF;
    RETURN NEW;
END
$$;
CREATE OR REPLACE TRIGGER host_member_no_primary_bi BEFORE INSERT ON host_member
    FOR EACH ROW EXECUTE FUNCTION trg_host_member_no_primary_bi();

CREATE OR REPLACE FUNCTION trg_host_member_no_primary_bu() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT COALESCE((EXISTS(SELECT 1 FROM principal WHERE (id = NEW.principal_id AND is_primary = 1) AND principal.tenant_id = new.tenant_id)), false) THEN
        RETURN NEW;
    END IF;
    IF true THEN
        RAISE EXCEPTION '%', 'the primary principal is already the host owner';
    END IF;
    RETURN NEW;
END
$$;
CREATE OR REPLACE TRIGGER host_member_no_primary_bu BEFORE UPDATE OF principal_id ON host_member
    FOR EACH ROW EXECUTE FUNCTION trg_host_member_no_primary_bu();

CREATE OR REPLACE FUNCTION trg_host_member_removed_ad() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE host_membership_state SET revision = revision + 1, member_count = member_count - 1 WHERE id = 1 AND host_membership_state.tenant_id = old.tenant_id;
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER host_member_removed_ad AFTER DELETE ON host_member
    FOR EACH ROW EXECUTE FUNCTION trg_host_member_removed_ad();

CREATE OR REPLACE FUNCTION trg_node_assignment_agent_deleted() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE node_assignment SET record_json = SQLITE_JSON_SET(record_json, '$.active', SQLITE_JSON('false'), '$.tombstoned', SQLITE_JSON('true')) WHERE agent_id = OLD.id AND node_assignment.tenant_id = old.tenant_id;
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER node_assignment_agent_deleted AFTER DELETE ON agent_session
    FOR EACH ROW EXECUTE FUNCTION trg_node_assignment_agent_deleted();

CREATE OR REPLACE FUNCTION trg_note_parent_set_null_on_delete() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE note SET parent_id = NULL WHERE (parent_id = OLD.id AND workspace_id = OLD.workspace_id) AND note.tenant_id = old.tenant_id;
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER note_parent_set_null_on_delete AFTER DELETE ON note
    FOR EACH ROW EXECUTE FUNCTION trg_note_parent_set_null_on_delete();

CREATE OR REPLACE FUNCTION trg_pr_monitor_message_provenance_insert_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS(SELECT 1 FROM pr_monitor_message_provenance WHERE message_id = NEW.message_id AND pr_monitor_message_provenance.tenant_id = new.tenant_id) THEN
        RAISE EXCEPTION '%', 'monitor message provenance is immutable';
    END IF;
    IF NOT EXISTS(SELECT 1 FROM agent_session WHERE (id = NEW.agent_id AND workspace_id = NEW.workspace_id) AND agent_session.tenant_id = new.tenant_id) THEN
        RAISE EXCEPTION '%', 'monitor message workspace mismatch';
    END IF;
    RETURN NEW;
END
$$;
CREATE OR REPLACE TRIGGER pr_monitor_message_provenance_insert_guard BEFORE INSERT ON pr_monitor_message_provenance
    FOR EACH ROW EXECUTE FUNCTION trg_pr_monitor_message_provenance_insert_guard();

CREATE OR REPLACE FUNCTION trg_pr_monitor_message_provenance_message_insert() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM pr_monitor_message_provenance WHERE message_id = NEW.id AND pr_monitor_message_provenance.tenant_id = new.tenant_id;
    RETURN NEW;
END
$$;
CREATE OR REPLACE TRIGGER pr_monitor_message_provenance_message_insert BEFORE INSERT ON agent_message
    FOR EACH ROW EXECUTE FUNCTION trg_pr_monitor_message_provenance_message_insert();

CREATE OR REPLACE FUNCTION trg_pr_monitor_message_provenance_message_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT COALESCE((NEW.id IS DISTINCT FROM OLD.id OR NEW.agent_id IS DISTINCT FROM OLD.agent_id OR NEW.seq IS DISTINCT FROM OLD.seq OR NEW.role IS DISTINCT FROM OLD.role OR NEW.content IS DISTINCT FROM OLD.content OR NEW.metadata IS DISTINCT FROM OLD.metadata OR NEW.created_at IS DISTINCT FROM OLD.created_at OR NEW.thumbnails IS DISTINCT FROM OLD.thumbnails), false) THEN
        RETURN NULL;
    END IF;
    DELETE FROM pr_monitor_message_provenance WHERE message_id IN (OLD.id, NEW.id) AND pr_monitor_message_provenance.tenant_id = new.tenant_id;
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER pr_monitor_message_provenance_message_update AFTER UPDATE OF id, agent_id, seq, role, content, metadata, created_at, thumbnails ON agent_message
    FOR EACH ROW EXECUTE FUNCTION trg_pr_monitor_message_provenance_message_update();

CREATE OR REPLACE FUNCTION trg_pr_monitor_message_provenance_payload_delete() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM pr_monitor_message_provenance WHERE message_id = OLD.message_id AND pr_monitor_message_provenance.tenant_id = old.tenant_id;
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER pr_monitor_message_provenance_payload_delete AFTER DELETE ON agent_message_payload
    FOR EACH ROW EXECUTE FUNCTION trg_pr_monitor_message_provenance_payload_delete();

CREATE OR REPLACE FUNCTION trg_pr_monitor_message_provenance_payload_insert() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM pr_monitor_message_provenance WHERE message_id = NEW.message_id AND pr_monitor_message_provenance.tenant_id = new.tenant_id;
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER pr_monitor_message_provenance_payload_insert AFTER INSERT ON agent_message_payload
    FOR EACH ROW EXECUTE FUNCTION trg_pr_monitor_message_provenance_payload_insert();

CREATE OR REPLACE FUNCTION trg_pr_monitor_message_provenance_payload_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM pr_monitor_message_provenance WHERE message_id IN (OLD.message_id, NEW.message_id) AND pr_monitor_message_provenance.tenant_id = new.tenant_id;
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER pr_monitor_message_provenance_payload_update AFTER UPDATE ON agent_message_payload
    FOR EACH ROW EXECUTE FUNCTION trg_pr_monitor_message_provenance_payload_update();

CREATE OR REPLACE FUNCTION trg_pr_monitor_message_provenance_session_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT COALESCE((NEW.id IS DISTINCT FROM OLD.id OR NEW.workspace_id IS DISTINCT FROM OLD.workspace_id OR NEW.provider IS DISTINCT FROM OLD.provider OR NEW.acp_session_id IS DISTINCT FROM OLD.acp_session_id OR NEW.backend_session_id IS DISTINCT FROM OLD.backend_session_id), false) THEN
        RETURN NULL;
    END IF;
    DELETE FROM pr_monitor_message_provenance WHERE agent_id IN (OLD.id, NEW.id) AND pr_monitor_message_provenance.tenant_id = new.tenant_id;
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER pr_monitor_message_provenance_session_update AFTER UPDATE OF id, workspace_id, provider, acp_session_id, backend_session_id ON agent_session
    FOR EACH ROW EXECUTE FUNCTION trg_pr_monitor_message_provenance_session_update();

CREATE OR REPLACE FUNCTION trg_pr_monitor_message_provenance_update_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF true THEN
        RAISE EXCEPTION '%', 'monitor message provenance is immutable';
    END IF;
    RETURN NEW;
END
$$;
CREATE OR REPLACE TRIGGER pr_monitor_message_provenance_update_guard BEFORE UPDATE ON pr_monitor_message_provenance
    FOR EACH ROW EXECUTE FUNCTION trg_pr_monitor_message_provenance_update_guard();

CREATE OR REPLACE FUNCTION trg_pull_request_workspace_candidate_record() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO pull_request_workspace (tenant_id, pr_key, workspace_id, source, linked_at) SELECT new.tenant_id AS tenant_id, k, NEW.workspace_id, NEW.source, INTENT_NOW_ISO() FROM (SELECT CASE WHEN s LIKE 'www.%' THEN SUBSTRING(s FROM 5) ELSE s END AS k FROM (SELECT CASE WHEN l LIKE 'https://%' THEN SUBSTRING(l FROM 9) WHEN l LIKE 'http://%' THEN SUBSTRING(l FROM 8) END AS s FROM (SELECT TRIM(TRAILING '/' FROM LOWER(TRIM(NEW.url))) AS l))) WHERE NOT k IS NULL AND LENGTH(k) - LENGTH(REPLACE(k, '/', '')) = 4 AND SUBSTRING(k FROM 1 FOR 1) <> '/' AND POSITION('//' IN k) = 0 AND POSITION('@' IN k) = 0 AND POSITION('?' IN k) = 0 AND POSITION('#' IN k) = 0 AND INTENT_REGEX(TRIM(TRAILING '0123456789' FROM k), '^.*/pull/$') AND NOT INTENT_REGEX(k, '^.*/pull/$') AND NOT INTENT_REGEX(k, '^.*/pull/0.*$') ON CONFLICT DO NOTHING;
    IF true THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END
$$;
CREATE OR REPLACE TRIGGER pull_request_workspace_candidate_record BEFORE INSERT ON pull_request_workspace_candidate
    FOR EACH ROW EXECUTE FUNCTION trg_pull_request_workspace_candidate_record();

CREATE OR REPLACE FUNCTION trg_pull_request_workspace_from_git_root_insert() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT COALESCE((EXISTS(SELECT 1 FROM workspace AS w WHERE (w.id = NEW.workspace_id AND w.is_remote = 0) AND w.tenant_id = new.tenant_id)), false) THEN
        RETURN NULL;
    END IF;
    INSERT INTO pull_request_workspace_candidate (tenant_id, url, workspace_id, source) SELECT new.tenant_id AS tenant_id, u, NEW.workspace_id, 'git_root' FROM (SELECT NEW.pr_url AS u UNION ALL SELECT SQLITE_JSON_EXTRACT(je.value, '$.url') FROM SQLITE_JSON_EACH(CASE WHEN SQLITE_JSON_VALID(NEW.pull_requests) AND SQLITE_JSON_TYPE(NEW.pull_requests) = 'array' THEN NEW.pull_requests ELSE '[]' END) AS je) WHERE NOT u IS NULL;
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER pull_request_workspace_from_git_root_insert AFTER INSERT ON workspace_git_root
    FOR EACH ROW EXECUTE FUNCTION trg_pull_request_workspace_from_git_root_insert();

CREATE OR REPLACE FUNCTION trg_pull_request_workspace_from_git_root_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT COALESCE(((NEW.pr_url IS DISTINCT FROM OLD.pr_url OR NEW.pull_requests IS DISTINCT FROM OLD.pull_requests) AND EXISTS(SELECT 1 FROM workspace AS w WHERE (w.id = NEW.workspace_id AND w.is_remote = 0) AND w.tenant_id = new.tenant_id)), false) THEN
        RETURN NULL;
    END IF;
    INSERT INTO pull_request_workspace_candidate (tenant_id, url, workspace_id, source) SELECT new.tenant_id AS tenant_id, u, NEW.workspace_id, 'git_root' FROM (SELECT NEW.pr_url AS u UNION ALL SELECT SQLITE_JSON_EXTRACT(je.value, '$.url') FROM SQLITE_JSON_EACH(CASE WHEN SQLITE_JSON_VALID(NEW.pull_requests) AND SQLITE_JSON_TYPE(NEW.pull_requests) = 'array' THEN NEW.pull_requests ELSE '[]' END) AS je) WHERE NOT u IS NULL;
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER pull_request_workspace_from_git_root_update AFTER UPDATE OF pr_url, pull_requests ON workspace_git_root
    FOR EACH ROW EXECUTE FUNCTION trg_pull_request_workspace_from_git_root_update();

CREATE OR REPLACE FUNCTION trg_pull_request_workspace_from_workspace_insert() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT COALESCE((NEW.is_remote = 0), false) THEN
        RETURN NULL;
    END IF;
    INSERT INTO pull_request_workspace_candidate (tenant_id, url, workspace_id, source) SELECT new.tenant_id AS tenant_id, u, NEW.id, 'workspace' FROM (SELECT NEW.pr_url AS u UNION ALL SELECT CASE WHEN SQLITE_JSON_VALID(NEW.active_pull_request) THEN SQLITE_JSON_EXTRACT(NEW.active_pull_request, '$.url') END UNION ALL SELECT SQLITE_JSON_EXTRACT(je.value, '$.url') FROM SQLITE_JSON_EACH(CASE WHEN SQLITE_JSON_VALID(NEW.pull_requests) AND SQLITE_JSON_TYPE(NEW.pull_requests) = 'array' THEN NEW.pull_requests ELSE '[]' END) AS je) WHERE NOT u IS NULL;
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER pull_request_workspace_from_workspace_insert AFTER INSERT ON workspace
    FOR EACH ROW EXECUTE FUNCTION trg_pull_request_workspace_from_workspace_insert();

CREATE OR REPLACE FUNCTION trg_pull_request_workspace_from_workspace_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT COALESCE((NEW.is_remote = 0 AND (NEW.pr_url IS DISTINCT FROM OLD.pr_url OR NEW.active_pull_request IS DISTINCT FROM OLD.active_pull_request OR NEW.pull_requests IS DISTINCT FROM OLD.pull_requests)), false) THEN
        RETURN NULL;
    END IF;
    INSERT INTO pull_request_workspace_candidate (tenant_id, url, workspace_id, source) SELECT new.tenant_id AS tenant_id, u, NEW.id, 'workspace' FROM (SELECT NEW.pr_url AS u UNION ALL SELECT CASE WHEN SQLITE_JSON_VALID(NEW.active_pull_request) THEN SQLITE_JSON_EXTRACT(NEW.active_pull_request, '$.url') END UNION ALL SELECT SQLITE_JSON_EXTRACT(je.value, '$.url') FROM SQLITE_JSON_EACH(CASE WHEN SQLITE_JSON_VALID(NEW.pull_requests) AND SQLITE_JSON_TYPE(NEW.pull_requests) = 'array' THEN NEW.pull_requests ELSE '[]' END) AS je) WHERE NOT u IS NULL;
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER pull_request_workspace_from_workspace_update AFTER UPDATE OF pr_url, active_pull_request, pull_requests ON workspace
    FOR EACH ROW EXECUTE FUNCTION trg_pull_request_workspace_from_workspace_update();

CREATE OR REPLACE FUNCTION trg_repository_authority_credential_ad() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM repository_authority_revision WHERE (kind = 'credential' AND subject_id = OLD.token_hash AND member_id = '' AND (SQLITE_TYPEOF(revision) <> 'integer' OR revision <= 0 OR revision = 9223372036854775807)) AND repository_authority_revision.tenant_id = old.tenant_id) THEN
        RAISE EXCEPTION '%', 'repository authority revision exhausted';
    END IF;
    UPDATE repository_authority_revision SET revision = revision + 1 WHERE (kind = 'credential' AND subject_id = OLD.token_hash AND member_id = '') AND repository_authority_revision.tenant_id = old.tenant_id;
    INSERT INTO repository_authority_revision (tenant_id, kind, subject_id, member_id, revision) SELECT old.tenant_id AS tenant_id, 'credential', OLD.token_hash, '', 1 WHERE NOT EXISTS(SELECT 1 FROM repository_authority_revision WHERE (kind = 'credential' AND subject_id = OLD.token_hash AND member_id = '') AND repository_authority_revision.tenant_id = old.tenant_id);
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER repository_authority_credential_ad AFTER DELETE ON principal_credential
    FOR EACH ROW EXECUTE FUNCTION trg_repository_authority_credential_ad();

CREATE OR REPLACE FUNCTION trg_repository_authority_credential_ai() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM repository_authority_revision WHERE (kind = 'credential' AND subject_id = NEW.token_hash AND member_id = '' AND (SQLITE_TYPEOF(revision) <> 'integer' OR revision <= 0 OR revision = 9223372036854775807)) AND repository_authority_revision.tenant_id = new.tenant_id) THEN
        RAISE EXCEPTION '%', 'repository authority revision exhausted';
    END IF;
    UPDATE repository_authority_revision SET revision = revision + 1 WHERE (kind = 'credential' AND subject_id = NEW.token_hash AND member_id = '') AND repository_authority_revision.tenant_id = new.tenant_id;
    INSERT INTO repository_authority_revision (tenant_id, kind, subject_id, member_id, revision) SELECT new.tenant_id AS tenant_id, 'credential', NEW.token_hash, '', 1 WHERE NOT EXISTS(SELECT 1 FROM repository_authority_revision WHERE (kind = 'credential' AND subject_id = NEW.token_hash AND member_id = '') AND repository_authority_revision.tenant_id = new.tenant_id);
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER repository_authority_credential_ai AFTER INSERT ON principal_credential
    FOR EACH ROW EXECUTE FUNCTION trg_repository_authority_credential_ai();

CREATE OR REPLACE FUNCTION trg_repository_authority_credential_au() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT COALESCE((NEW.token_hash IS DISTINCT FROM OLD.token_hash OR NEW.principal_id IS DISTINCT FROM OLD.principal_id OR NEW.revoked_at IS DISTINCT FROM OLD.revoked_at), false) THEN
        RETURN NULL;
    END IF;
    IF EXISTS (SELECT 1 FROM repository_authority_revision WHERE (kind = 'credential' AND subject_id = OLD.token_hash AND member_id = '' AND (SQLITE_TYPEOF(revision) <> 'integer' OR revision <= 0 OR revision = 9223372036854775807)) AND repository_authority_revision.tenant_id = new.tenant_id) THEN
        RAISE EXCEPTION '%', 'repository authority revision exhausted';
    END IF;
    UPDATE repository_authority_revision SET revision = revision + 1 WHERE (kind = 'credential' AND subject_id = OLD.token_hash AND member_id = '') AND repository_authority_revision.tenant_id = new.tenant_id;
    INSERT INTO repository_authority_revision (tenant_id, kind, subject_id, member_id, revision) SELECT new.tenant_id AS tenant_id, 'credential', OLD.token_hash, '', 1 WHERE NOT EXISTS(SELECT 1 FROM repository_authority_revision WHERE (kind = 'credential' AND subject_id = OLD.token_hash AND member_id = '') AND repository_authority_revision.tenant_id = new.tenant_id);
    IF EXISTS (SELECT 1 FROM repository_authority_revision WHERE (kind = 'credential' AND subject_id = NEW.token_hash AND member_id = '' AND (SQLITE_TYPEOF(revision) <> 'integer' OR revision <= 0 OR revision = 9223372036854775807) AND (NEW.token_hash IS DISTINCT FROM OLD.token_hash)) AND repository_authority_revision.tenant_id = new.tenant_id) THEN
        RAISE EXCEPTION '%', 'repository authority revision exhausted';
    END IF;
    UPDATE repository_authority_revision SET revision = revision + 1 WHERE (kind = 'credential' AND subject_id = NEW.token_hash AND member_id = '' AND (NEW.token_hash IS DISTINCT FROM OLD.token_hash)) AND repository_authority_revision.tenant_id = new.tenant_id;
    INSERT INTO repository_authority_revision (tenant_id, kind, subject_id, member_id, revision) SELECT new.tenant_id AS tenant_id, 'credential', NEW.token_hash, '', 1 WHERE NOT EXISTS(SELECT 1 FROM repository_authority_revision WHERE (kind = 'credential' AND subject_id = NEW.token_hash AND member_id = '') AND repository_authority_revision.tenant_id = new.tenant_id) AND (NEW.token_hash IS DISTINCT FROM OLD.token_hash);
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER repository_authority_credential_au AFTER UPDATE OF token_hash, principal_id, revoked_at ON principal_credential
    FOR EACH ROW EXECUTE FUNCTION trg_repository_authority_credential_au();

CREATE OR REPLACE FUNCTION trg_repository_authority_host_member_ad() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM repository_authority_revision WHERE (kind = 'host_member' AND subject_id = OLD.principal_id AND member_id = '' AND (SQLITE_TYPEOF(revision) <> 'integer' OR revision <= 0 OR revision = 9223372036854775807)) AND repository_authority_revision.tenant_id = old.tenant_id) THEN
        RAISE EXCEPTION '%', 'repository authority revision exhausted';
    END IF;
    UPDATE repository_authority_revision SET revision = revision + 1 WHERE (kind = 'host_member' AND subject_id = OLD.principal_id AND member_id = '') AND repository_authority_revision.tenant_id = old.tenant_id;
    INSERT INTO repository_authority_revision (tenant_id, kind, subject_id, member_id, revision) SELECT old.tenant_id AS tenant_id, 'host_member', OLD.principal_id, '', 1 WHERE NOT EXISTS(SELECT 1 FROM repository_authority_revision WHERE (kind = 'host_member' AND subject_id = OLD.principal_id AND member_id = '') AND repository_authority_revision.tenant_id = old.tenant_id);
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER repository_authority_host_member_ad AFTER DELETE ON host_member
    FOR EACH ROW EXECUTE FUNCTION trg_repository_authority_host_member_ad();

CREATE OR REPLACE FUNCTION trg_repository_authority_host_member_ai() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM repository_authority_revision WHERE (kind = 'host_member' AND subject_id = NEW.principal_id AND member_id = '' AND (SQLITE_TYPEOF(revision) <> 'integer' OR revision <= 0 OR revision = 9223372036854775807)) AND repository_authority_revision.tenant_id = new.tenant_id) THEN
        RAISE EXCEPTION '%', 'repository authority revision exhausted';
    END IF;
    UPDATE repository_authority_revision SET revision = revision + 1 WHERE (kind = 'host_member' AND subject_id = NEW.principal_id AND member_id = '') AND repository_authority_revision.tenant_id = new.tenant_id;
    INSERT INTO repository_authority_revision (tenant_id, kind, subject_id, member_id, revision) SELECT new.tenant_id AS tenant_id, 'host_member', NEW.principal_id, '', 1 WHERE NOT EXISTS(SELECT 1 FROM repository_authority_revision WHERE (kind = 'host_member' AND subject_id = NEW.principal_id AND member_id = '') AND repository_authority_revision.tenant_id = new.tenant_id);
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER repository_authority_host_member_ai AFTER INSERT ON host_member
    FOR EACH ROW EXECUTE FUNCTION trg_repository_authority_host_member_ai();

CREATE OR REPLACE FUNCTION trg_repository_authority_host_member_au() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT COALESCE((NEW.principal_id IS DISTINCT FROM OLD.principal_id), false) THEN
        RETURN NULL;
    END IF;
    IF EXISTS (SELECT 1 FROM repository_authority_revision WHERE (kind = 'host_member' AND subject_id = OLD.principal_id AND member_id = '' AND (SQLITE_TYPEOF(revision) <> 'integer' OR revision <= 0 OR revision = 9223372036854775807)) AND repository_authority_revision.tenant_id = new.tenant_id) THEN
        RAISE EXCEPTION '%', 'repository authority revision exhausted';
    END IF;
    UPDATE repository_authority_revision SET revision = revision + 1 WHERE (kind = 'host_member' AND subject_id = OLD.principal_id AND member_id = '') AND repository_authority_revision.tenant_id = new.tenant_id;
    INSERT INTO repository_authority_revision (tenant_id, kind, subject_id, member_id, revision) SELECT new.tenant_id AS tenant_id, 'host_member', OLD.principal_id, '', 1 WHERE NOT EXISTS(SELECT 1 FROM repository_authority_revision WHERE (kind = 'host_member' AND subject_id = OLD.principal_id AND member_id = '') AND repository_authority_revision.tenant_id = new.tenant_id);
    IF EXISTS (SELECT 1 FROM repository_authority_revision WHERE (kind = 'host_member' AND subject_id = NEW.principal_id AND member_id = '' AND (SQLITE_TYPEOF(revision) <> 'integer' OR revision <= 0 OR revision = 9223372036854775807) AND (NEW.principal_id IS DISTINCT FROM OLD.principal_id)) AND repository_authority_revision.tenant_id = new.tenant_id) THEN
        RAISE EXCEPTION '%', 'repository authority revision exhausted';
    END IF;
    UPDATE repository_authority_revision SET revision = revision + 1 WHERE (kind = 'host_member' AND subject_id = NEW.principal_id AND member_id = '' AND (NEW.principal_id IS DISTINCT FROM OLD.principal_id)) AND repository_authority_revision.tenant_id = new.tenant_id;
    INSERT INTO repository_authority_revision (tenant_id, kind, subject_id, member_id, revision) SELECT new.tenant_id AS tenant_id, 'host_member', NEW.principal_id, '', 1 WHERE NOT EXISTS(SELECT 1 FROM repository_authority_revision WHERE (kind = 'host_member' AND subject_id = NEW.principal_id AND member_id = '') AND repository_authority_revision.tenant_id = new.tenant_id) AND (NEW.principal_id IS DISTINCT FROM OLD.principal_id);
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER repository_authority_host_member_au AFTER UPDATE OF principal_id ON host_member
    FOR EACH ROW EXECUTE FUNCTION trg_repository_authority_host_member_au();

CREATE OR REPLACE FUNCTION trg_repository_authority_principal_ad() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM repository_authority_revision WHERE (kind = 'principal' AND subject_id = OLD.id AND member_id = '' AND (SQLITE_TYPEOF(revision) <> 'integer' OR revision <= 0 OR revision = 9223372036854775807)) AND repository_authority_revision.tenant_id = old.tenant_id) THEN
        RAISE EXCEPTION '%', 'repository authority revision exhausted';
    END IF;
    UPDATE repository_authority_revision SET revision = revision + 1 WHERE (kind = 'principal' AND subject_id = OLD.id AND member_id = '') AND repository_authority_revision.tenant_id = old.tenant_id;
    INSERT INTO repository_authority_revision (tenant_id, kind, subject_id, member_id, revision) SELECT old.tenant_id AS tenant_id, 'principal', OLD.id, '', 1 WHERE NOT EXISTS(SELECT 1 FROM repository_authority_revision WHERE (kind = 'principal' AND subject_id = OLD.id AND member_id = '') AND repository_authority_revision.tenant_id = old.tenant_id);
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER repository_authority_principal_ad AFTER DELETE ON principal
    FOR EACH ROW EXECUTE FUNCTION trg_repository_authority_principal_ad();

CREATE OR REPLACE FUNCTION trg_repository_authority_principal_ai() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM repository_authority_revision WHERE (kind = 'principal' AND subject_id = NEW.id AND member_id = '' AND (SQLITE_TYPEOF(revision) <> 'integer' OR revision <= 0 OR revision = 9223372036854775807)) AND repository_authority_revision.tenant_id = new.tenant_id) THEN
        RAISE EXCEPTION '%', 'repository authority revision exhausted';
    END IF;
    UPDATE repository_authority_revision SET revision = revision + 1 WHERE (kind = 'principal' AND subject_id = NEW.id AND member_id = '') AND repository_authority_revision.tenant_id = new.tenant_id;
    INSERT INTO repository_authority_revision (tenant_id, kind, subject_id, member_id, revision) SELECT new.tenant_id AS tenant_id, 'principal', NEW.id, '', 1 WHERE NOT EXISTS(SELECT 1 FROM repository_authority_revision WHERE (kind = 'principal' AND subject_id = NEW.id AND member_id = '') AND repository_authority_revision.tenant_id = new.tenant_id);
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER repository_authority_principal_ai AFTER INSERT ON principal
    FOR EACH ROW EXECUTE FUNCTION trg_repository_authority_principal_ai();

CREATE OR REPLACE FUNCTION trg_repository_authority_principal_au() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT COALESCE((NEW.id IS DISTINCT FROM OLD.id OR NEW.is_primary IS DISTINCT FROM OLD.is_primary OR NEW.github_user_id IS DISTINCT FROM OLD.github_user_id OR NEW.identity_provider IS DISTINCT FROM OLD.identity_provider OR NEW.instance_host IS DISTINCT FROM OLD.instance_host OR NEW.external_user_id IS DISTINCT FROM OLD.external_user_id), false) THEN
        RETURN NULL;
    END IF;
    IF EXISTS (SELECT 1 FROM repository_authority_revision WHERE (kind = 'principal' AND subject_id = OLD.id AND member_id = '' AND (SQLITE_TYPEOF(revision) <> 'integer' OR revision <= 0 OR revision = 9223372036854775807)) AND repository_authority_revision.tenant_id = new.tenant_id) THEN
        RAISE EXCEPTION '%', 'repository authority revision exhausted';
    END IF;
    UPDATE repository_authority_revision SET revision = revision + 1 WHERE (kind = 'principal' AND subject_id = OLD.id AND member_id = '') AND repository_authority_revision.tenant_id = new.tenant_id;
    INSERT INTO repository_authority_revision (tenant_id, kind, subject_id, member_id, revision) SELECT new.tenant_id AS tenant_id, 'principal', OLD.id, '', 1 WHERE NOT EXISTS(SELECT 1 FROM repository_authority_revision WHERE (kind = 'principal' AND subject_id = OLD.id AND member_id = '') AND repository_authority_revision.tenant_id = new.tenant_id);
    IF EXISTS (SELECT 1 FROM repository_authority_revision WHERE (kind = 'principal' AND subject_id = NEW.id AND member_id = '' AND (SQLITE_TYPEOF(revision) <> 'integer' OR revision <= 0 OR revision = 9223372036854775807) AND (NEW.id IS DISTINCT FROM OLD.id)) AND repository_authority_revision.tenant_id = new.tenant_id) THEN
        RAISE EXCEPTION '%', 'repository authority revision exhausted';
    END IF;
    UPDATE repository_authority_revision SET revision = revision + 1 WHERE (kind = 'principal' AND subject_id = NEW.id AND member_id = '' AND (NEW.id IS DISTINCT FROM OLD.id)) AND repository_authority_revision.tenant_id = new.tenant_id;
    INSERT INTO repository_authority_revision (tenant_id, kind, subject_id, member_id, revision) SELECT new.tenant_id AS tenant_id, 'principal', NEW.id, '', 1 WHERE NOT EXISTS(SELECT 1 FROM repository_authority_revision WHERE (kind = 'principal' AND subject_id = NEW.id AND member_id = '') AND repository_authority_revision.tenant_id = new.tenant_id) AND (NEW.id IS DISTINCT FROM OLD.id);
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER repository_authority_principal_au AFTER UPDATE OF id, is_primary, github_user_id, identity_provider, instance_host, external_user_id ON principal
    FOR EACH ROW EXECUTE FUNCTION trg_repository_authority_principal_au();

CREATE OR REPLACE FUNCTION trg_repository_authority_revision_monotonic() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT COALESCE((NEW.kind IS DISTINCT FROM OLD.kind OR NEW.subject_id IS DISTINCT FROM OLD.subject_id OR NEW.member_id IS DISTINCT FROM OLD.member_id OR SQLITE_TYPEOF(NEW.revision) <> 'integer' OR NEW.revision <= OLD.revision), false) THEN
        RETURN NEW;
    END IF;
    IF true THEN
        RAISE EXCEPTION '%', 'repository authority revision must increase without overflow';
    END IF;
    RETURN NEW;
END
$$;
CREATE OR REPLACE TRIGGER repository_authority_revision_monotonic BEFORE UPDATE ON repository_authority_revision
    FOR EACH ROW EXECUTE FUNCTION trg_repository_authority_revision_monotonic();

CREATE OR REPLACE FUNCTION trg_repository_authority_revision_no_delete() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF true THEN
        RAISE EXCEPTION '%', 'repository authority tombstone cannot be deleted';
    END IF;
    RETURN OLD;
END
$$;
CREATE OR REPLACE TRIGGER repository_authority_revision_no_delete BEFORE DELETE ON repository_authority_revision
    FOR EACH ROW EXECUTE FUNCTION trg_repository_authority_revision_no_delete();

CREATE OR REPLACE FUNCTION trg_repository_authority_revision_no_reset() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT COALESCE((EXISTS(SELECT 1 FROM repository_authority_revision WHERE (kind = NEW.kind AND subject_id = NEW.subject_id AND member_id = NEW.member_id) AND repository_authority_revision.tenant_id = new.tenant_id)), false) THEN
        RETURN NEW;
    END IF;
    IF true THEN
        RAISE EXCEPTION '%', 'repository authority tombstone cannot be replaced';
    END IF;
    RETURN NEW;
END
$$;
CREATE OR REPLACE TRIGGER repository_authority_revision_no_reset BEFORE INSERT ON repository_authority_revision
    FOR EACH ROW EXECUTE FUNCTION trg_repository_authority_revision_no_reset();

CREATE OR REPLACE FUNCTION trg_repository_authority_workspace_ad() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM repository_authority_revision WHERE (kind = 'workspace' AND subject_id = OLD.id AND member_id = '' AND (SQLITE_TYPEOF(revision) <> 'integer' OR revision <= 0 OR revision = 9223372036854775807)) AND repository_authority_revision.tenant_id = old.tenant_id) THEN
        RAISE EXCEPTION '%', 'repository authority revision exhausted';
    END IF;
    UPDATE repository_authority_revision SET revision = revision + 1 WHERE (kind = 'workspace' AND subject_id = OLD.id AND member_id = '') AND repository_authority_revision.tenant_id = old.tenant_id;
    INSERT INTO repository_authority_revision (tenant_id, kind, subject_id, member_id, revision) SELECT old.tenant_id AS tenant_id, 'workspace', OLD.id, '', 1 WHERE NOT EXISTS(SELECT 1 FROM repository_authority_revision WHERE (kind = 'workspace' AND subject_id = OLD.id AND member_id = '') AND repository_authority_revision.tenant_id = old.tenant_id);
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER repository_authority_workspace_ad AFTER DELETE ON workspace
    FOR EACH ROW EXECUTE FUNCTION trg_repository_authority_workspace_ad();

CREATE OR REPLACE FUNCTION trg_repository_authority_workspace_ai() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM repository_authority_revision WHERE (kind = 'workspace' AND subject_id = NEW.id AND member_id = '' AND (SQLITE_TYPEOF(revision) <> 'integer' OR revision <= 0 OR revision = 9223372036854775807)) AND repository_authority_revision.tenant_id = new.tenant_id) THEN
        RAISE EXCEPTION '%', 'repository authority revision exhausted';
    END IF;
    UPDATE repository_authority_revision SET revision = revision + 1 WHERE (kind = 'workspace' AND subject_id = NEW.id AND member_id = '') AND repository_authority_revision.tenant_id = new.tenant_id;
    INSERT INTO repository_authority_revision (tenant_id, kind, subject_id, member_id, revision) SELECT new.tenant_id AS tenant_id, 'workspace', NEW.id, '', 1 WHERE NOT EXISTS(SELECT 1 FROM repository_authority_revision WHERE (kind = 'workspace' AND subject_id = NEW.id AND member_id = '') AND repository_authority_revision.tenant_id = new.tenant_id);
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER repository_authority_workspace_ai AFTER INSERT ON workspace
    FOR EACH ROW EXECUTE FUNCTION trg_repository_authority_workspace_ai();

CREATE OR REPLACE FUNCTION trg_repository_authority_workspace_au() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT COALESCE((NEW.id IS DISTINCT FROM OLD.id OR NEW.owner_principal_id IS DISTINCT FROM OLD.owner_principal_id), false) THEN
        RETURN NULL;
    END IF;
    IF EXISTS (SELECT 1 FROM repository_authority_revision WHERE (kind = 'workspace' AND subject_id = OLD.id AND member_id = '' AND (SQLITE_TYPEOF(revision) <> 'integer' OR revision <= 0 OR revision = 9223372036854775807)) AND repository_authority_revision.tenant_id = new.tenant_id) THEN
        RAISE EXCEPTION '%', 'repository authority revision exhausted';
    END IF;
    UPDATE repository_authority_revision SET revision = revision + 1 WHERE (kind = 'workspace' AND subject_id = OLD.id AND member_id = '') AND repository_authority_revision.tenant_id = new.tenant_id;
    INSERT INTO repository_authority_revision (tenant_id, kind, subject_id, member_id, revision) SELECT new.tenant_id AS tenant_id, 'workspace', OLD.id, '', 1 WHERE NOT EXISTS(SELECT 1 FROM repository_authority_revision WHERE (kind = 'workspace' AND subject_id = OLD.id AND member_id = '') AND repository_authority_revision.tenant_id = new.tenant_id);
    IF EXISTS (SELECT 1 FROM repository_authority_revision WHERE (kind = 'workspace' AND subject_id = NEW.id AND member_id = '' AND (SQLITE_TYPEOF(revision) <> 'integer' OR revision <= 0 OR revision = 9223372036854775807) AND (NEW.id IS DISTINCT FROM OLD.id)) AND repository_authority_revision.tenant_id = new.tenant_id) THEN
        RAISE EXCEPTION '%', 'repository authority revision exhausted';
    END IF;
    UPDATE repository_authority_revision SET revision = revision + 1 WHERE (kind = 'workspace' AND subject_id = NEW.id AND member_id = '' AND (NEW.id IS DISTINCT FROM OLD.id)) AND repository_authority_revision.tenant_id = new.tenant_id;
    INSERT INTO repository_authority_revision (tenant_id, kind, subject_id, member_id, revision) SELECT new.tenant_id AS tenant_id, 'workspace', NEW.id, '', 1 WHERE NOT EXISTS(SELECT 1 FROM repository_authority_revision WHERE (kind = 'workspace' AND subject_id = NEW.id AND member_id = '') AND repository_authority_revision.tenant_id = new.tenant_id) AND (NEW.id IS DISTINCT FROM OLD.id);
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER repository_authority_workspace_au AFTER UPDATE OF id, owner_principal_id ON workspace
    FOR EACH ROW EXECUTE FUNCTION trg_repository_authority_workspace_au();

CREATE OR REPLACE FUNCTION trg_repository_authority_workspace_member_ad() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM repository_authority_revision WHERE (kind = 'workspace_member' AND subject_id = OLD.workspace_id AND member_id = OLD.principal_id AND (SQLITE_TYPEOF(revision) <> 'integer' OR revision <= 0 OR revision = 9223372036854775807)) AND repository_authority_revision.tenant_id = old.tenant_id) THEN
        RAISE EXCEPTION '%', 'repository authority revision exhausted';
    END IF;
    UPDATE repository_authority_revision SET revision = revision + 1 WHERE (kind = 'workspace_member' AND subject_id = OLD.workspace_id AND member_id = OLD.principal_id) AND repository_authority_revision.tenant_id = old.tenant_id;
    INSERT INTO repository_authority_revision (tenant_id, kind, subject_id, member_id, revision) SELECT old.tenant_id AS tenant_id, 'workspace_member', OLD.workspace_id, OLD.principal_id, 1 WHERE NOT EXISTS(SELECT 1 FROM repository_authority_revision WHERE (kind = 'workspace_member' AND subject_id = OLD.workspace_id AND member_id = OLD.principal_id) AND repository_authority_revision.tenant_id = old.tenant_id);
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER repository_authority_workspace_member_ad AFTER DELETE ON workspace_member
    FOR EACH ROW EXECUTE FUNCTION trg_repository_authority_workspace_member_ad();

CREATE OR REPLACE FUNCTION trg_repository_authority_workspace_member_ai() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM repository_authority_revision WHERE (kind = 'workspace_member' AND subject_id = NEW.workspace_id AND member_id = NEW.principal_id AND (SQLITE_TYPEOF(revision) <> 'integer' OR revision <= 0 OR revision = 9223372036854775807)) AND repository_authority_revision.tenant_id = new.tenant_id) THEN
        RAISE EXCEPTION '%', 'repository authority revision exhausted';
    END IF;
    UPDATE repository_authority_revision SET revision = revision + 1 WHERE (kind = 'workspace_member' AND subject_id = NEW.workspace_id AND member_id = NEW.principal_id) AND repository_authority_revision.tenant_id = new.tenant_id;
    INSERT INTO repository_authority_revision (tenant_id, kind, subject_id, member_id, revision) SELECT new.tenant_id AS tenant_id, 'workspace_member', NEW.workspace_id, NEW.principal_id, 1 WHERE NOT EXISTS(SELECT 1 FROM repository_authority_revision WHERE (kind = 'workspace_member' AND subject_id = NEW.workspace_id AND member_id = NEW.principal_id) AND repository_authority_revision.tenant_id = new.tenant_id);
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER repository_authority_workspace_member_ai AFTER INSERT ON workspace_member
    FOR EACH ROW EXECUTE FUNCTION trg_repository_authority_workspace_member_ai();

CREATE OR REPLACE FUNCTION trg_repository_authority_workspace_member_au() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT COALESCE((NEW.workspace_id IS DISTINCT FROM OLD.workspace_id OR NEW.principal_id IS DISTINCT FROM OLD.principal_id OR NEW.role IS DISTINCT FROM OLD.role), false) THEN
        RETURN NULL;
    END IF;
    IF EXISTS (SELECT 1 FROM repository_authority_revision WHERE (kind = 'workspace_member' AND subject_id = OLD.workspace_id AND member_id = OLD.principal_id AND (SQLITE_TYPEOF(revision) <> 'integer' OR revision <= 0 OR revision = 9223372036854775807)) AND repository_authority_revision.tenant_id = new.tenant_id) THEN
        RAISE EXCEPTION '%', 'repository authority revision exhausted';
    END IF;
    UPDATE repository_authority_revision SET revision = revision + 1 WHERE (kind = 'workspace_member' AND subject_id = OLD.workspace_id AND member_id = OLD.principal_id) AND repository_authority_revision.tenant_id = new.tenant_id;
    INSERT INTO repository_authority_revision (tenant_id, kind, subject_id, member_id, revision) SELECT new.tenant_id AS tenant_id, 'workspace_member', OLD.workspace_id, OLD.principal_id, 1 WHERE NOT EXISTS(SELECT 1 FROM repository_authority_revision WHERE (kind = 'workspace_member' AND subject_id = OLD.workspace_id AND member_id = OLD.principal_id) AND repository_authority_revision.tenant_id = new.tenant_id);
    IF EXISTS (SELECT 1 FROM repository_authority_revision WHERE (kind = 'workspace_member' AND subject_id = NEW.workspace_id AND member_id = NEW.principal_id AND (SQLITE_TYPEOF(revision) <> 'integer' OR revision <= 0 OR revision = 9223372036854775807) AND (NEW.workspace_id IS DISTINCT FROM OLD.workspace_id OR NEW.principal_id IS DISTINCT FROM OLD.principal_id)) AND repository_authority_revision.tenant_id = new.tenant_id) THEN
        RAISE EXCEPTION '%', 'repository authority revision exhausted';
    END IF;
    UPDATE repository_authority_revision SET revision = revision + 1 WHERE (kind = 'workspace_member' AND subject_id = NEW.workspace_id AND member_id = NEW.principal_id AND (NEW.workspace_id IS DISTINCT FROM OLD.workspace_id OR NEW.principal_id IS DISTINCT FROM OLD.principal_id)) AND repository_authority_revision.tenant_id = new.tenant_id;
    INSERT INTO repository_authority_revision (tenant_id, kind, subject_id, member_id, revision) SELECT new.tenant_id AS tenant_id, 'workspace_member', NEW.workspace_id, NEW.principal_id, 1 WHERE NOT EXISTS(SELECT 1 FROM repository_authority_revision WHERE (kind = 'workspace_member' AND subject_id = NEW.workspace_id AND member_id = NEW.principal_id) AND repository_authority_revision.tenant_id = new.tenant_id) AND (NEW.workspace_id IS DISTINCT FROM OLD.workspace_id OR NEW.principal_id IS DISTINCT FROM OLD.principal_id);
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER repository_authority_workspace_member_au AFTER UPDATE OF workspace_id, principal_id, role ON workspace_member
    FOR EACH ROW EXECUTE FUNCTION trg_repository_authority_workspace_member_au();

CREATE OR REPLACE FUNCTION trg_repository_selection_monotonic() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT COALESCE((NEW.workspace_id IS DISTINCT FROM OLD.workspace_id OR NEW.root_kind IS DISTINCT FROM OLD.root_kind OR NEW.root_id IS DISTINCT FROM OLD.root_id OR NEW.root_incarnation < OLD.root_incarnation OR NEW.selection_revision <= OLD.selection_revision), false) THEN
        RETURN NEW;
    END IF;
    IF true THEN
        RAISE EXCEPTION '%', 'repository selection continuity must advance';
    END IF;
    RETURN NEW;
END
$$;
CREATE OR REPLACE TRIGGER repository_selection_monotonic BEFORE UPDATE ON repository_selection_state
    FOR EACH ROW EXECUTE FUNCTION trg_repository_selection_monotonic();

CREATE OR REPLACE FUNCTION trg_repository_selection_no_delete() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF true THEN
        RAISE EXCEPTION '%', 'repository selection tombstone cannot be deleted';
    END IF;
    RETURN OLD;
END
$$;
CREATE OR REPLACE TRIGGER repository_selection_no_delete BEFORE DELETE ON repository_selection_state
    FOR EACH ROW EXECUTE FUNCTION trg_repository_selection_no_delete();

CREATE OR REPLACE FUNCTION trg_repository_selection_no_replace() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT COALESCE((EXISTS(SELECT 1 FROM repository_selection_state WHERE (workspace_id = NEW.workspace_id AND root_kind = NEW.root_kind AND root_id = NEW.root_id) AND repository_selection_state.tenant_id = new.tenant_id)), false) THEN
        RETURN NEW;
    END IF;
    IF true THEN
        RAISE EXCEPTION '%', 'repository selection tombstone cannot be replaced';
    END IF;
    RETURN NEW;
END
$$;
CREATE OR REPLACE TRIGGER repository_selection_no_replace BEFORE INSERT ON repository_selection_state
    FOR EACH ROW EXECUTE FUNCTION trg_repository_selection_no_replace();

CREATE OR REPLACE FUNCTION trg_repository_selection_root_binding() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT COALESCE(((NEW.id IS DISTINCT FROM OLD.id OR NEW.workspace_id IS DISTINCT FROM OLD.workspace_id OR NEW.path IS DISTINCT FROM OLD.path) AND NOT (NEW.id IS DISTINCT FROM OLD.id OR NEW.workspace_id IS DISTINCT FROM OLD.workspace_id)), false) THEN
        RETURN NULL;
    END IF;
    UPDATE repository_selection_state SET root_incarnation = root_incarnation + 1, selection_revision = selection_revision + 1, root_present = 1, repository_path = NULL, worktree_path = NULL, is_remote = NULL, registered_path = NEW.path, choice_incarnation = CASE WHEN choice_mode = 'never' THEN root_incarnation + 1 ELSE choice_incarnation END WHERE (workspace_id = NEW.workspace_id AND root_kind = 'registered' AND root_id = NEW.id) AND repository_selection_state.tenant_id = new.tenant_id;
    INSERT INTO repository_selection_state (tenant_id, workspace_id, root_kind, root_id, root_incarnation, selection_revision, root_present, repository_path, worktree_path, is_remote, registered_path, choice_incarnation, choice_mode, remote_name, historical_source, historical_record_id) SELECT new.tenant_id AS tenant_id, NEW.workspace_id, 'registered', NEW.id, 1, 1, 1, NULL, NULL, NULL, NEW.path, 1, 'never', NULL, NULL, NULL WHERE NOT EXISTS(SELECT 1 FROM repository_selection_state WHERE (workspace_id = NEW.workspace_id AND root_kind = 'registered' AND root_id = NEW.id) AND repository_selection_state.tenant_id = new.tenant_id);
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER repository_selection_root_binding AFTER UPDATE OF id, workspace_id, path ON workspace_git_root
    FOR EACH ROW EXECUTE FUNCTION trg_repository_selection_root_binding();

CREATE OR REPLACE FUNCTION trg_repository_selection_root_delete() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE repository_selection_state SET root_incarnation = root_incarnation + 1, selection_revision = selection_revision + 1, root_present = 0, repository_path = NULL, worktree_path = NULL, is_remote = NULL, registered_path = OLD.path, choice_incarnation = CASE WHEN choice_mode = 'never' THEN root_incarnation + 1 ELSE choice_incarnation END WHERE (workspace_id = OLD.workspace_id AND root_kind = 'registered' AND root_id = OLD.id) AND repository_selection_state.tenant_id = old.tenant_id;
    INSERT INTO repository_selection_state (tenant_id, workspace_id, root_kind, root_id, root_incarnation, selection_revision, root_present, repository_path, worktree_path, is_remote, registered_path, choice_incarnation, choice_mode, remote_name, historical_source, historical_record_id) SELECT old.tenant_id AS tenant_id, OLD.workspace_id, 'registered', OLD.id, 1, 1, 0, NULL, NULL, NULL, OLD.path, 1, 'never', NULL, NULL, NULL WHERE NOT EXISTS(SELECT 1 FROM repository_selection_state WHERE (workspace_id = OLD.workspace_id AND root_kind = 'registered' AND root_id = OLD.id) AND repository_selection_state.tenant_id = old.tenant_id);
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER repository_selection_root_delete AFTER DELETE ON workspace_git_root
    FOR EACH ROW EXECUTE FUNCTION trg_repository_selection_root_delete();

CREATE OR REPLACE FUNCTION trg_repository_selection_root_insert() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE repository_selection_state SET root_incarnation = root_incarnation + 1, selection_revision = selection_revision + 1, root_present = 1, repository_path = NULL, worktree_path = NULL, is_remote = NULL, registered_path = NEW.path, choice_incarnation = CASE WHEN choice_mode = 'never' THEN root_incarnation + 1 ELSE choice_incarnation END WHERE (workspace_id = NEW.workspace_id AND root_kind = 'registered' AND root_id = NEW.id) AND repository_selection_state.tenant_id = new.tenant_id;
    INSERT INTO repository_selection_state (tenant_id, workspace_id, root_kind, root_id, root_incarnation, selection_revision, root_present, repository_path, worktree_path, is_remote, registered_path, choice_incarnation, choice_mode, remote_name, historical_source, historical_record_id) SELECT new.tenant_id AS tenant_id, NEW.workspace_id, 'registered', NEW.id, 1, 1, 1, NULL, NULL, NULL, NEW.path, 1, 'never', NULL, NULL, NULL WHERE NOT EXISTS(SELECT 1 FROM repository_selection_state WHERE (workspace_id = NEW.workspace_id AND root_kind = 'registered' AND root_id = NEW.id) AND repository_selection_state.tenant_id = new.tenant_id);
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER repository_selection_root_insert AFTER INSERT ON workspace_git_root
    FOR EACH ROW EXECUTE FUNCTION trg_repository_selection_root_insert();

CREATE OR REPLACE FUNCTION trg_repository_selection_root_move() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT COALESCE((NEW.id IS DISTINCT FROM OLD.id OR NEW.workspace_id IS DISTINCT FROM OLD.workspace_id), false) THEN
        RETURN NULL;
    END IF;
    UPDATE repository_selection_state SET root_incarnation = root_incarnation + 1, selection_revision = selection_revision + 1, root_present = 0, repository_path = NULL, worktree_path = NULL, is_remote = NULL, registered_path = OLD.path, choice_incarnation = CASE WHEN choice_mode = 'never' THEN root_incarnation + 1 ELSE choice_incarnation END WHERE (workspace_id = OLD.workspace_id AND root_kind = 'registered' AND root_id = OLD.id) AND repository_selection_state.tenant_id = new.tenant_id;
    INSERT INTO repository_selection_state (tenant_id, workspace_id, root_kind, root_id, root_incarnation, selection_revision, root_present, repository_path, worktree_path, is_remote, registered_path, choice_incarnation, choice_mode, remote_name, historical_source, historical_record_id) SELECT new.tenant_id AS tenant_id, OLD.workspace_id, 'registered', OLD.id, 1, 1, 0, NULL, NULL, NULL, OLD.path, 1, 'never', NULL, NULL, NULL WHERE NOT EXISTS(SELECT 1 FROM repository_selection_state WHERE (workspace_id = OLD.workspace_id AND root_kind = 'registered' AND root_id = OLD.id) AND repository_selection_state.tenant_id = new.tenant_id);
    UPDATE repository_selection_state SET root_incarnation = root_incarnation + 1, selection_revision = selection_revision + 1, root_present = 1, repository_path = NULL, worktree_path = NULL, is_remote = NULL, registered_path = NEW.path, choice_incarnation = CASE WHEN choice_mode = 'never' THEN root_incarnation + 1 ELSE choice_incarnation END WHERE (workspace_id = NEW.workspace_id AND root_kind = 'registered' AND root_id = NEW.id) AND repository_selection_state.tenant_id = new.tenant_id;
    INSERT INTO repository_selection_state (tenant_id, workspace_id, root_kind, root_id, root_incarnation, selection_revision, root_present, repository_path, worktree_path, is_remote, registered_path, choice_incarnation, choice_mode, remote_name, historical_source, historical_record_id) SELECT new.tenant_id AS tenant_id, NEW.workspace_id, 'registered', NEW.id, 1, 1, 1, NULL, NULL, NULL, NEW.path, 1, 'never', NULL, NULL, NULL WHERE NOT EXISTS(SELECT 1 FROM repository_selection_state WHERE (workspace_id = NEW.workspace_id AND root_kind = 'registered' AND root_id = NEW.id) AND repository_selection_state.tenant_id = new.tenant_id);
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER repository_selection_root_move AFTER UPDATE OF id, workspace_id, path ON workspace_git_root
    FOR EACH ROW EXECUTE FUNCTION trg_repository_selection_root_move();

CREATE OR REPLACE FUNCTION trg_repository_selection_validate_insert() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT COALESCE((SQLITE_TYPEOF(NEW.root_incarnation) <> 'integer' OR NEW.root_incarnation <= 0 OR SQLITE_TYPEOF(NEW.selection_revision) <> 'integer' OR NEW.selection_revision <= 0 OR SQLITE_TYPEOF(NEW.choice_incarnation) <> 'integer' OR NEW.choice_incarnation <= 0 OR NEW.choice_incarnation > NEW.root_incarnation OR NOT NEW.root_present IN (0, 1) OR SQLITE_TYPEOF(NEW.root_present) <> 'integer' OR NOT NEW.root_kind IN ('primary', 'registered') OR (NEW.root_kind = 'primary' AND (NEW.root_id <> '' OR NOT NEW.registered_path IS NULL OR NOT NEW.is_remote IN (0, 1))) OR (NEW.root_kind = 'registered' AND (NEW.root_id = '' OR NEW.registered_path IS NULL OR NOT NEW.repository_path IS NULL OR NOT NEW.worktree_path IS NULL OR NOT NEW.is_remote IS NULL)) OR NOT NEW.choice_mode IN ('never', 'reset', 'automatic', 'remote', 'unresolved') OR (NEW.choice_mode = 'remote' AND (NEW.remote_name IS NULL OR LENGTH(NEW.remote_name) = 0)) OR (NEW.choice_mode <> 'remote' AND NOT NEW.remote_name IS NULL) OR (NEW.choice_mode <> 'unresolved' AND (NOT NEW.historical_source IS NULL OR NOT NEW.historical_record_id IS NULL)) OR (NEW.historical_source IS NULL) <> (NEW.historical_record_id IS NULL) OR (NOT NEW.historical_source IS NULL AND (NOT NEW.historical_source IN ('workspace-metadata', 'registered-root-metadata') OR LENGTH(NEW.historical_record_id) = 0))), false) THEN
        RETURN NEW;
    END IF;
    IF true THEN
        RAISE EXCEPTION '%', 'invalid repository selection state or exhausted counter';
    END IF;
    RETURN NEW;
END
$$;
CREATE OR REPLACE TRIGGER repository_selection_validate_insert BEFORE INSERT ON repository_selection_state
    FOR EACH ROW EXECUTE FUNCTION trg_repository_selection_validate_insert();

CREATE OR REPLACE FUNCTION trg_repository_selection_validate_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT COALESCE((SQLITE_TYPEOF(NEW.root_incarnation) <> 'integer' OR NEW.root_incarnation <= 0 OR SQLITE_TYPEOF(NEW.selection_revision) <> 'integer' OR NEW.selection_revision <= 0 OR SQLITE_TYPEOF(NEW.choice_incarnation) <> 'integer' OR NEW.choice_incarnation <= 0 OR NEW.choice_incarnation > NEW.root_incarnation OR NOT NEW.root_present IN (0, 1) OR SQLITE_TYPEOF(NEW.root_present) <> 'integer' OR NOT NEW.root_kind IN ('primary', 'registered') OR (NEW.root_kind = 'primary' AND (NEW.root_id <> '' OR NOT NEW.registered_path IS NULL OR NOT NEW.is_remote IN (0, 1))) OR (NEW.root_kind = 'registered' AND (NEW.root_id = '' OR NEW.registered_path IS NULL OR NOT NEW.repository_path IS NULL OR NOT NEW.worktree_path IS NULL OR NOT NEW.is_remote IS NULL)) OR NOT NEW.choice_mode IN ('never', 'reset', 'automatic', 'remote', 'unresolved') OR (NEW.choice_mode = 'remote' AND (NEW.remote_name IS NULL OR LENGTH(NEW.remote_name) = 0)) OR (NEW.choice_mode <> 'remote' AND NOT NEW.remote_name IS NULL) OR (NEW.choice_mode <> 'unresolved' AND (NOT NEW.historical_source IS NULL OR NOT NEW.historical_record_id IS NULL)) OR (NEW.historical_source IS NULL) <> (NEW.historical_record_id IS NULL) OR (NOT NEW.historical_source IS NULL AND (NOT NEW.historical_source IN ('workspace-metadata', 'registered-root-metadata') OR LENGTH(NEW.historical_record_id) = 0))), false) THEN
        RETURN NEW;
    END IF;
    IF true THEN
        RAISE EXCEPTION '%', 'invalid repository selection state or exhausted counter';
    END IF;
    RETURN NEW;
END
$$;
CREATE OR REPLACE TRIGGER repository_selection_validate_update BEFORE UPDATE ON repository_selection_state
    FOR EACH ROW EXECUTE FUNCTION trg_repository_selection_validate_update();

CREATE OR REPLACE FUNCTION trg_repository_selection_workspace_binding() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT COALESCE(((NEW.id IS DISTINCT FROM OLD.id OR NEW.repository_path IS DISTINCT FROM OLD.repository_path OR NEW.worktree_path IS DISTINCT FROM OLD.worktree_path OR NEW.is_remote IS DISTINCT FROM OLD.is_remote) AND NOT (NEW.id IS DISTINCT FROM OLD.id)), false) THEN
        RETURN NULL;
    END IF;
    UPDATE repository_selection_state SET root_incarnation = root_incarnation + 1, selection_revision = selection_revision + 1, root_present = 1, repository_path = NEW.repository_path, worktree_path = NEW.worktree_path, is_remote = NEW.is_remote, registered_path = NULL, choice_incarnation = CASE WHEN choice_mode = 'never' THEN root_incarnation + 1 ELSE choice_incarnation END WHERE (workspace_id = NEW.id AND root_kind = 'primary' AND root_id = '') AND repository_selection_state.tenant_id = new.tenant_id;
    INSERT INTO repository_selection_state (tenant_id, workspace_id, root_kind, root_id, root_incarnation, selection_revision, root_present, repository_path, worktree_path, is_remote, registered_path, choice_incarnation, choice_mode, remote_name, historical_source, historical_record_id) SELECT new.tenant_id AS tenant_id, NEW.id, 'primary', '', 1, 1, 1, NEW.repository_path, NEW.worktree_path, NEW.is_remote, NULL, 1, 'never', NULL, NULL, NULL WHERE NOT EXISTS(SELECT 1 FROM repository_selection_state WHERE (workspace_id = NEW.id AND root_kind = 'primary' AND root_id = '') AND repository_selection_state.tenant_id = new.tenant_id);
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER repository_selection_workspace_binding AFTER UPDATE OF id, repository_path, worktree_path, is_remote ON workspace
    FOR EACH ROW EXECUTE FUNCTION trg_repository_selection_workspace_binding();

CREATE OR REPLACE FUNCTION trg_repository_selection_workspace_delete() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE repository_selection_state SET root_incarnation = root_incarnation + 1, selection_revision = selection_revision + 1, root_present = 0, repository_path = OLD.repository_path, worktree_path = OLD.worktree_path, is_remote = OLD.is_remote, registered_path = NULL, choice_incarnation = CASE WHEN choice_mode = 'never' THEN root_incarnation + 1 ELSE choice_incarnation END WHERE (workspace_id = OLD.id AND root_kind = 'primary' AND root_id = '') AND repository_selection_state.tenant_id = old.tenant_id;
    INSERT INTO repository_selection_state (tenant_id, workspace_id, root_kind, root_id, root_incarnation, selection_revision, root_present, repository_path, worktree_path, is_remote, registered_path, choice_incarnation, choice_mode, remote_name, historical_source, historical_record_id) SELECT old.tenant_id AS tenant_id, OLD.id, 'primary', '', 1, 1, 0, OLD.repository_path, OLD.worktree_path, OLD.is_remote, NULL, 1, 'never', NULL, NULL, NULL WHERE NOT EXISTS(SELECT 1 FROM repository_selection_state WHERE (workspace_id = OLD.id AND root_kind = 'primary' AND root_id = '') AND repository_selection_state.tenant_id = old.tenant_id);
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER repository_selection_workspace_delete AFTER DELETE ON workspace
    FOR EACH ROW EXECUTE FUNCTION trg_repository_selection_workspace_delete();

CREATE OR REPLACE FUNCTION trg_repository_selection_workspace_insert() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE repository_selection_state SET root_incarnation = root_incarnation + 1, selection_revision = selection_revision + 1, root_present = 1, repository_path = NEW.repository_path, worktree_path = NEW.worktree_path, is_remote = NEW.is_remote, registered_path = NULL, choice_incarnation = CASE WHEN choice_mode = 'never' THEN root_incarnation + 1 ELSE choice_incarnation END WHERE (workspace_id = NEW.id AND root_kind = 'primary' AND root_id = '') AND repository_selection_state.tenant_id = new.tenant_id;
    INSERT INTO repository_selection_state (tenant_id, workspace_id, root_kind, root_id, root_incarnation, selection_revision, root_present, repository_path, worktree_path, is_remote, registered_path, choice_incarnation, choice_mode, remote_name, historical_source, historical_record_id) SELECT new.tenant_id AS tenant_id, NEW.id, 'primary', '', 1, 1, 1, NEW.repository_path, NEW.worktree_path, NEW.is_remote, NULL, 1, 'never', NULL, NULL, NULL WHERE NOT EXISTS(SELECT 1 FROM repository_selection_state WHERE (workspace_id = NEW.id AND root_kind = 'primary' AND root_id = '') AND repository_selection_state.tenant_id = new.tenant_id);
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER repository_selection_workspace_insert AFTER INSERT ON workspace
    FOR EACH ROW EXECUTE FUNCTION trg_repository_selection_workspace_insert();

CREATE OR REPLACE FUNCTION trg_repository_selection_workspace_move() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT COALESCE((NEW.id IS DISTINCT FROM OLD.id), false) THEN
        RETURN NULL;
    END IF;
    UPDATE repository_selection_state SET root_incarnation = root_incarnation + 1, selection_revision = selection_revision + 1, root_present = 0, repository_path = OLD.repository_path, worktree_path = OLD.worktree_path, is_remote = OLD.is_remote, registered_path = NULL, choice_incarnation = CASE WHEN choice_mode = 'never' THEN root_incarnation + 1 ELSE choice_incarnation END WHERE (workspace_id = OLD.id AND root_kind = 'primary' AND root_id = '') AND repository_selection_state.tenant_id = new.tenant_id;
    INSERT INTO repository_selection_state (tenant_id, workspace_id, root_kind, root_id, root_incarnation, selection_revision, root_present, repository_path, worktree_path, is_remote, registered_path, choice_incarnation, choice_mode, remote_name, historical_source, historical_record_id) SELECT new.tenant_id AS tenant_id, OLD.id, 'primary', '', 1, 1, 0, OLD.repository_path, OLD.worktree_path, OLD.is_remote, NULL, 1, 'never', NULL, NULL, NULL WHERE NOT EXISTS(SELECT 1 FROM repository_selection_state WHERE (workspace_id = OLD.id AND root_kind = 'primary' AND root_id = '') AND repository_selection_state.tenant_id = new.tenant_id);
    UPDATE repository_selection_state SET root_incarnation = root_incarnation + 1, selection_revision = selection_revision + 1, root_present = 1, repository_path = NEW.repository_path, worktree_path = NEW.worktree_path, is_remote = NEW.is_remote, registered_path = NULL, choice_incarnation = CASE WHEN choice_mode = 'never' THEN root_incarnation + 1 ELSE choice_incarnation END WHERE (workspace_id = NEW.id AND root_kind = 'primary' AND root_id = '') AND repository_selection_state.tenant_id = new.tenant_id;
    INSERT INTO repository_selection_state (tenant_id, workspace_id, root_kind, root_id, root_incarnation, selection_revision, root_present, repository_path, worktree_path, is_remote, registered_path, choice_incarnation, choice_mode, remote_name, historical_source, historical_record_id) SELECT new.tenant_id AS tenant_id, NEW.id, 'primary', '', 1, 1, 1, NEW.repository_path, NEW.worktree_path, NEW.is_remote, NULL, 1, 'never', NULL, NULL, NULL WHERE NOT EXISTS(SELECT 1 FROM repository_selection_state WHERE (workspace_id = NEW.id AND root_kind = 'primary' AND root_id = '') AND repository_selection_state.tenant_id = new.tenant_id);
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER repository_selection_workspace_move AFTER UPDATE OF id, repository_path, worktree_path, is_remote ON workspace
    FOR EACH ROW EXECUTE FUNCTION trg_repository_selection_workspace_move();

CREATE OR REPLACE FUNCTION trg_review_context_enrollment_agent_insert() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM review_context_enrollment WHERE agent_id = NEW.id AND review_context_enrollment.tenant_id = new.tenant_id;
    RETURN NEW;
END
$$;
CREATE OR REPLACE TRIGGER review_context_enrollment_agent_insert BEFORE INSERT ON agent_session
    FOR EACH ROW EXECUTE FUNCTION trg_review_context_enrollment_agent_insert();

CREATE OR REPLACE FUNCTION trg_review_context_enrollment_agent_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT COALESCE((NEW.id IS DISTINCT FROM OLD.id OR NEW.workspace_id IS DISTINCT FROM OLD.workspace_id OR NEW.provider IS DISTINCT FROM OLD.provider OR NEW.acp_session_id IS DISTINCT FROM OLD.acp_session_id OR NEW.backend_session_id IS DISTINCT FROM OLD.backend_session_id OR NEW.parent_agent_id IS DISTINCT FROM OLD.parent_agent_id), false) THEN
        RETURN NULL;
    END IF;
    DELETE FROM review_context_enrollment WHERE agent_id IN (OLD.id, NEW.id) AND review_context_enrollment.tenant_id = new.tenant_id;
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER review_context_enrollment_agent_update AFTER UPDATE OF id, workspace_id, provider, acp_session_id, backend_session_id, parent_agent_id ON agent_session
    FOR EACH ROW EXECUTE FUNCTION trg_review_context_enrollment_agent_update();

CREATE OR REPLACE FUNCTION trg_review_context_enrollment_insert_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS(SELECT 1 FROM review_context_enrollment WHERE agent_id = NEW.agent_id AND review_context_enrollment.tenant_id = new.tenant_id) THEN
        RAISE EXCEPTION '%', 'private native enrollment is immutable';
    END IF;
    IF LENGTH(TRIM(NEW.agent_id)) = 0 OR LENGTH(TRIM(NEW.workspace_id)) = 0 OR NOT EXISTS(SELECT 1 FROM agent_session WHERE (id = NEW.agent_id AND workspace_id = NEW.workspace_id AND acp_session_id = NEW.acp_session_id) AND agent_session.tenant_id = new.tenant_id) THEN
        RAISE EXCEPTION '%', 'private native enrollment identity mismatch';
    END IF;
    RETURN NEW;
END
$$;
CREATE OR REPLACE TRIGGER review_context_enrollment_insert_guard BEFORE INSERT ON review_context_enrollment
    FOR EACH ROW EXECUTE FUNCTION trg_review_context_enrollment_insert_guard();

CREATE OR REPLACE FUNCTION trg_review_context_enrollment_update_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF true THEN
        RAISE EXCEPTION '%', 'private native enrollment is immutable';
    END IF;
    RETURN NEW;
END
$$;
CREATE OR REPLACE TRIGGER review_context_enrollment_update_guard BEFORE UPDATE ON review_context_enrollment
    FOR EACH ROW EXECUTE FUNCTION trg_review_context_enrollment_update_guard();

CREATE OR REPLACE FUNCTION trg_review_context_private_family_delete_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF true THEN
        RAISE EXCEPTION '%', 'private native family denial is immutable';
    END IF;
    RETURN OLD;
END
$$;
CREATE OR REPLACE TRIGGER review_context_private_family_delete_guard BEFORE DELETE ON review_context_private_family
    FOR EACH ROW EXECUTE FUNCTION trg_review_context_private_family_delete_guard();

CREATE OR REPLACE FUNCTION trg_review_context_private_family_enroll() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO review_context_private_family (tenant_id, acp_session_id, version) VALUES (new.tenant_id, NEW.acp_session_id, 1) ON CONFLICT(tenant_id, acp_session_id) DO NOTHING;
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER review_context_private_family_enroll AFTER INSERT ON review_context_enrollment
    FOR EACH ROW EXECUTE FUNCTION trg_review_context_private_family_enroll();

CREATE OR REPLACE FUNCTION trg_review_context_private_family_retire() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO review_context_private_family (tenant_id, acp_session_id, version) VALUES (old.tenant_id, OLD.acp_session_id, 1) ON CONFLICT(tenant_id, acp_session_id) DO NOTHING;
    RETURN OLD;
END
$$;
CREATE OR REPLACE TRIGGER review_context_private_family_retire BEFORE DELETE ON review_context_enrollment
    FOR EACH ROW EXECUTE FUNCTION trg_review_context_private_family_retire();

CREATE OR REPLACE FUNCTION trg_review_context_private_family_update_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF true THEN
        RAISE EXCEPTION '%', 'private native family denial is immutable';
    END IF;
    RETURN NEW;
END
$$;
CREATE OR REPLACE TRIGGER review_context_private_family_update_guard BEFORE UPDATE ON review_context_private_family
    FOR EACH ROW EXECUTE FUNCTION trg_review_context_private_family_update_guard();

CREATE OR REPLACE FUNCTION trg_script_monitor_message_delivered() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT COALESCE(((CASE WHEN SQLITE_JSON_VALID(new.metadata) THEN SQLITE_JSON_EXTRACT(new.metadata, '$.type') END) = 'script_monitor_wake'), false) THEN
        RETURN NULL;
    END IF;
    UPDATE script_notification_job SET phase = 'message-persisted' WHERE wake_id = 'script-monitor:' || SQLITE_JSON_EXTRACT(new.metadata, '$.monitorId') AND script_notification_job.tenant_id = new.tenant_id;
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER script_monitor_message_delivered AFTER INSERT ON agent_message
    FOR EACH ROW EXECUTE FUNCTION trg_script_monitor_message_delivered();

CREATE OR REPLACE FUNCTION trg_script_monitor_message_fence() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT COALESCE(((CASE WHEN SQLITE_JSON_VALID(new.metadata) THEN SQLITE_JSON_EXTRACT(new.metadata, '$.type') END) = 'script_monitor_wake' AND (CASE WHEN SQLITE_JSON_VALID(new.metadata) THEN SQLITE_JSON_EXTRACT(new.metadata, '$.remoteAgentId') END) IS NULL AND NOT EXISTS(SELECT 1 FROM (SELECT wake_id AS id, agent_id, workspace_id FROM script_notification_job WHERE script_notification_job.tenant_id = new.tenant_id UNION ALL SELECT id, agent_id, workspace_id FROM agent_queue_admission WHERE phase <> 'completed' AND agent_queue_admission.tenant_id = new.tenant_id) AS d JOIN agent_session AS a ON a.id = d.agent_id JOIN workspace AS w ON w.id = d.workspace_id WHERE ((d.id = 'script-monitor:' || SQLITE_JSON_EXTRACT(new.metadata, '$.monitorId') AND d.agent_id = new.agent_id AND a.retired_at IS NULL AND w.archived = 0) AND a.tenant_id = new.tenant_id) AND w.tenant_id = new.tenant_id)), false) THEN
        RETURN NEW;
    END IF;
    IF true THEN
        RAISE EXCEPTION '%', 'script monitor wake suppressed';
    END IF;
    RETURN NEW;
END
$$;
CREATE OR REPLACE TRIGGER script_monitor_message_fence BEFORE INSERT ON agent_message
    FOR EACH ROW EXECUTE FUNCTION trg_script_monitor_message_fence();

CREATE OR REPLACE FUNCTION trg_script_monitor_owner_deleted() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM script_monitor WHERE agent_id = old.id AND script_monitor.tenant_id = old.tenant_id;
    DELETE FROM script_notification_job WHERE agent_id = old.id AND script_notification_job.tenant_id = old.tenant_id;
    DELETE FROM agent_queue_admission WHERE agent_id = old.id AND agent_queue_admission.tenant_id = old.tenant_id;
    DELETE FROM agent_queue WHERE (SQLITE_JSON_VALID(payload) AND SQLITE_JSON_EXTRACT(payload, '$.messageMetadata.type') = 'script_monitor_wake' AND agent_id = old.id) AND agent_queue.tenant_id = old.tenant_id;
    RETURN OLD;
END
$$;
CREATE OR REPLACE TRIGGER script_monitor_owner_deleted BEFORE DELETE ON agent_session
    FOR EACH ROW EXECUTE FUNCTION trg_script_monitor_owner_deleted();

CREATE OR REPLACE FUNCTION trg_script_monitor_owner_retired() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT COALESCE((NOT new.retired_at IS NULL), false) THEN
        RETURN NULL;
    END IF;
    DELETE FROM script_monitor WHERE agent_id = new.id AND script_monitor.tenant_id = new.tenant_id;
    DELETE FROM script_notification_job WHERE agent_id = new.id AND script_notification_job.tenant_id = new.tenant_id;
    DELETE FROM agent_queue_admission WHERE agent_id = new.id AND agent_queue_admission.tenant_id = new.tenant_id;
    DELETE FROM agent_queue WHERE (SQLITE_JSON_VALID(payload) AND SQLITE_JSON_EXTRACT(payload, '$.messageMetadata.type') = 'script_monitor_wake' AND agent_id = new.id) AND agent_queue.tenant_id = new.tenant_id;
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER script_monitor_owner_retired AFTER UPDATE OF retired_at ON agent_session
    FOR EACH ROW EXECUTE FUNCTION trg_script_monitor_owner_retired();

CREATE OR REPLACE FUNCTION trg_script_monitor_queue_fence() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT COALESCE(((CASE WHEN SQLITE_JSON_VALID(new.payload) THEN SQLITE_JSON_EXTRACT(new.payload, '$.messageMetadata.type') END) = 'script_monitor_wake' AND (CASE WHEN SQLITE_JSON_VALID(new.payload) THEN SQLITE_JSON_EXTRACT(new.payload, '$.messageMetadata.remoteAgentId') END) IS NULL AND NOT EXISTS(SELECT 1 FROM (SELECT wake_id AS id, agent_id, workspace_id FROM script_notification_job WHERE script_notification_job.tenant_id = new.tenant_id UNION ALL SELECT id, agent_id, workspace_id FROM agent_queue_admission WHERE phase = 'pending' AND agent_queue_admission.tenant_id = new.tenant_id) AS d JOIN agent_session AS a ON a.id = d.agent_id JOIN workspace AS w ON w.id = d.workspace_id WHERE ((d.id = 'script-monitor:' || SQLITE_JSON_EXTRACT(new.payload, '$.messageMetadata.monitorId') AND d.agent_id = new.agent_id AND a.retired_at IS NULL AND w.archived = 0) AND a.tenant_id = new.tenant_id) AND w.tenant_id = new.tenant_id)), false) THEN
        RETURN NEW;
    END IF;
    IF true THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END
$$;
CREATE OR REPLACE TRIGGER script_monitor_queue_fence BEFORE INSERT ON agent_queue
    FOR EACH ROW EXECUTE FUNCTION trg_script_monitor_queue_fence();

CREATE OR REPLACE FUNCTION trg_script_monitor_registration_fence() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT COALESCE((NOT EXISTS(SELECT 1 FROM agent_session AS a JOIN workspace AS w ON w.id = a.workspace_id WHERE ((a.id = new.agent_id AND a.workspace_id = new.workspace_id AND a.retired_at IS NULL AND w.archived = 0) AND a.tenant_id = new.tenant_id) AND w.tenant_id = new.tenant_id)), false) THEN
        RETURN NEW;
    END IF;
    IF true THEN
        RAISE EXCEPTION '%', 'script monitor lifecycle fence';
    END IF;
    RETURN NEW;
END
$$;
CREATE OR REPLACE TRIGGER script_monitor_registration_fence BEFORE INSERT ON script_monitor
    FOR EACH ROW EXECUTE FUNCTION trg_script_monitor_registration_fence();

CREATE OR REPLACE FUNCTION trg_script_monitor_workspace_archived() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT COALESCE((new.archived = 1), false) THEN
        RETURN NULL;
    END IF;
    DELETE FROM script_monitor WHERE workspace_id = new.id AND script_monitor.tenant_id = new.tenant_id;
    DELETE FROM script_notification_job WHERE workspace_id = new.id AND script_notification_job.tenant_id = new.tenant_id;
    DELETE FROM agent_queue_admission WHERE workspace_id = new.id AND agent_queue_admission.tenant_id = new.tenant_id;
    DELETE FROM agent_queue WHERE (SQLITE_JSON_VALID(payload) AND SQLITE_JSON_EXTRACT(payload, '$.messageMetadata.type') = 'script_monitor_wake' AND SQLITE_JSON_EXTRACT(payload, '$.messageMetadata.workspaceId') = new.id) AND agent_queue.tenant_id = new.tenant_id;
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER script_monitor_workspace_archived AFTER UPDATE OF archived ON workspace
    FOR EACH ROW EXECUTE FUNCTION trg_script_monitor_workspace_archived();

CREATE OR REPLACE FUNCTION trg_script_monitor_workspace_deleted() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM script_monitor WHERE workspace_id = old.id AND script_monitor.tenant_id = old.tenant_id;
    DELETE FROM script_notification_job WHERE workspace_id = old.id AND script_notification_job.tenant_id = old.tenant_id;
    DELETE FROM agent_queue_admission WHERE workspace_id = old.id AND agent_queue_admission.tenant_id = old.tenant_id;
    DELETE FROM agent_queue WHERE (SQLITE_JSON_VALID(payload) AND SQLITE_JSON_EXTRACT(payload, '$.messageMetadata.type') = 'script_monitor_wake' AND SQLITE_JSON_EXTRACT(payload, '$.messageMetadata.workspaceId') = old.id) AND agent_queue.tenant_id = old.tenant_id;
    RETURN OLD;
END
$$;
CREATE OR REPLACE TRIGGER script_monitor_workspace_deleted BEFORE DELETE ON workspace
    FOR EACH ROW EXECUTE FUNCTION trg_script_monitor_workspace_deleted();

CREATE OR REPLACE FUNCTION trg_sharing_host_member_ad() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE workspace_sharing_summary SET non_host_count = non_host_count + 1, guest_count = guest_count + CAST((EXISTS(SELECT 1 FROM workspace_member AS m WHERE (m.workspace_id = workspace_sharing_summary.workspace_id AND m.principal_id = OLD.principal_id AND m.role IN ('collaborator', 'manager')) AND m.tenant_id = old.tenant_id)) AS INT) WHERE workspace_id IN (SELECT workspace_id FROM workspace_member WHERE principal_id = OLD.principal_id AND workspace_member.tenant_id = old.tenant_id) AND workspace_sharing_summary.tenant_id = old.tenant_id;
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER sharing_host_member_ad AFTER DELETE ON host_member
    FOR EACH ROW EXECUTE FUNCTION trg_sharing_host_member_ad();

CREATE OR REPLACE FUNCTION trg_sharing_host_member_ai() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE workspace_sharing_summary SET non_host_count = non_host_count - 1, guest_count = guest_count - CAST((EXISTS(SELECT 1 FROM workspace_member AS m WHERE (m.workspace_id = workspace_sharing_summary.workspace_id AND m.principal_id = NEW.principal_id AND m.role IN ('collaborator', 'manager')) AND m.tenant_id = new.tenant_id)) AS INT) WHERE workspace_id IN (SELECT workspace_id FROM workspace_member WHERE principal_id = NEW.principal_id AND workspace_member.tenant_id = new.tenant_id) AND workspace_sharing_summary.tenant_id = new.tenant_id;
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER sharing_host_member_ai AFTER INSERT ON host_member
    FOR EACH ROW EXECUTE FUNCTION trg_sharing_host_member_ai();

CREATE OR REPLACE FUNCTION trg_sharing_host_member_au() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE workspace_sharing_summary SET non_host_count = non_host_count + 1, guest_count = guest_count + CAST((EXISTS(SELECT 1 FROM workspace_member AS m WHERE (m.workspace_id = workspace_sharing_summary.workspace_id AND m.principal_id = OLD.principal_id AND m.role IN ('collaborator', 'manager')) AND m.tenant_id = new.tenant_id)) AS INT) WHERE workspace_id IN (SELECT workspace_id FROM workspace_member WHERE principal_id = OLD.principal_id AND workspace_member.tenant_id = new.tenant_id) AND workspace_sharing_summary.tenant_id = new.tenant_id;
    UPDATE workspace_sharing_summary SET non_host_count = non_host_count - 1, guest_count = guest_count - CAST((EXISTS(SELECT 1 FROM workspace_member AS m WHERE (m.workspace_id = workspace_sharing_summary.workspace_id AND m.principal_id = NEW.principal_id AND m.role IN ('collaborator', 'manager')) AND m.tenant_id = new.tenant_id)) AS INT) WHERE workspace_id IN (SELECT workspace_id FROM workspace_member WHERE principal_id = NEW.principal_id AND workspace_member.tenant_id = new.tenant_id) AND workspace_sharing_summary.tenant_id = new.tenant_id;
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER sharing_host_member_au AFTER UPDATE OF principal_id ON host_member
    FOR EACH ROW EXECUTE FUNCTION trg_sharing_host_member_au();

CREATE OR REPLACE FUNCTION trg_sharing_invite_ai() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT COALESCE((NEW.revoked_at IS NULL AND ((NEW.pin_identity_provider IS NULL AND NEW.pin_github_user_id IS NULL) OR NEW.redeemed_at IS NULL)), false) THEN
        RETURN NULL;
    END IF;
    INSERT INTO workspace_invite_seat (tenant_id, invite_id, workspace_id, expires_at) VALUES (new.tenant_id, NEW.id, NEW.workspace_id, NEW.expires_at);
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER sharing_invite_ai AFTER INSERT ON workspace_invite
    FOR EACH ROW EXECUTE FUNCTION trg_sharing_invite_ai();

CREATE OR REPLACE FUNCTION trg_sharing_invite_au() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM workspace_invite_seat WHERE invite_id = OLD.id AND workspace_invite_seat.tenant_id = new.tenant_id;
    INSERT INTO workspace_invite_seat (tenant_id, invite_id, workspace_id, expires_at) SELECT new.tenant_id AS tenant_id, NEW.id, NEW.workspace_id, NEW.expires_at WHERE NEW.revoked_at IS NULL AND ((NEW.pin_identity_provider IS NULL AND NEW.pin_github_user_id IS NULL) OR NEW.redeemed_at IS NULL);
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER sharing_invite_au AFTER UPDATE OF workspace_id, expires_at, revoked_at, redeemed_at, pin_identity_provider, pin_github_user_id ON workspace_invite
    FOR EACH ROW EXECUTE FUNCTION trg_sharing_invite_au();

CREATE OR REPLACE FUNCTION trg_sharing_invite_seat_ad() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE workspace_sharing_summary SET open_invite_count = open_invite_count - 1 WHERE workspace_id = OLD.workspace_id AND workspace_sharing_summary.tenant_id = old.tenant_id;
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER sharing_invite_seat_ad AFTER DELETE ON workspace_invite_seat
    FOR EACH ROW EXECUTE FUNCTION trg_sharing_invite_seat_ad();

CREATE OR REPLACE FUNCTION trg_sharing_invite_seat_ai() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE workspace_sharing_summary SET open_invite_count = open_invite_count + 1 WHERE workspace_id = NEW.workspace_id AND workspace_sharing_summary.tenant_id = new.tenant_id;
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER sharing_invite_seat_ai AFTER INSERT ON workspace_invite_seat
    FOR EACH ROW EXECUTE FUNCTION trg_sharing_invite_seat_ai();

CREATE OR REPLACE FUNCTION trg_sharing_member_ad() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE workspace_sharing_summary SET direct_count = direct_count - 1, non_host_count = non_host_count - CAST(((NOT EXISTS(SELECT 1 FROM host_member WHERE principal_id = OLD.principal_id AND host_member.tenant_id = old.tenant_id))) AS INT), guest_count = guest_count - CAST(((OLD.role IN ('collaborator', 'manager') AND NOT EXISTS(SELECT 1 FROM host_member WHERE principal_id = OLD.principal_id AND host_member.tenant_id = old.tenant_id))) AS INT) WHERE workspace_id = OLD.workspace_id AND workspace_sharing_summary.tenant_id = old.tenant_id;
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER sharing_member_ad AFTER DELETE ON workspace_member
    FOR EACH ROW EXECUTE FUNCTION trg_sharing_member_ad();

CREATE OR REPLACE FUNCTION trg_sharing_member_ai() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO workspace_sharing_summary (tenant_id, workspace_id, direct_count, non_host_count, guest_count) VALUES (new.tenant_id, NEW.workspace_id, 1, CAST((NOT EXISTS(SELECT 1 FROM host_member WHERE principal_id = NEW.principal_id AND host_member.tenant_id = new.tenant_id)) AS INT), CAST((NEW.role IN ('collaborator', 'manager') AND NOT EXISTS(SELECT 1 FROM host_member WHERE principal_id = NEW.principal_id AND host_member.tenant_id = new.tenant_id)) AS INT)) ON CONFLICT(tenant_id, workspace_id) DO UPDATE SET direct_count = workspace_sharing_summary.direct_count + 1, non_host_count = workspace_sharing_summary.non_host_count + excluded.non_host_count, guest_count = workspace_sharing_summary.guest_count + excluded.guest_count;
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER sharing_member_ai AFTER INSERT ON workspace_member
    FOR EACH ROW EXECUTE FUNCTION trg_sharing_member_ai();

CREATE OR REPLACE FUNCTION trg_sharing_member_au() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE workspace_sharing_summary SET direct_count = direct_count - 1, non_host_count = non_host_count - CAST(((NOT EXISTS(SELECT 1 FROM host_member WHERE principal_id = OLD.principal_id AND host_member.tenant_id = new.tenant_id))) AS INT), guest_count = guest_count - CAST(((OLD.role IN ('collaborator', 'manager') AND NOT EXISTS(SELECT 1 FROM host_member WHERE principal_id = OLD.principal_id AND host_member.tenant_id = new.tenant_id))) AS INT) WHERE workspace_id = OLD.workspace_id AND workspace_sharing_summary.tenant_id = new.tenant_id;
    INSERT INTO workspace_sharing_summary (tenant_id, workspace_id, direct_count, non_host_count, guest_count) VALUES (new.tenant_id, NEW.workspace_id, 1, CAST((NOT EXISTS(SELECT 1 FROM host_member WHERE principal_id = NEW.principal_id AND host_member.tenant_id = new.tenant_id)) AS INT), CAST((NEW.role IN ('collaborator', 'manager') AND NOT EXISTS(SELECT 1 FROM host_member WHERE principal_id = NEW.principal_id AND host_member.tenant_id = new.tenant_id)) AS INT)) ON CONFLICT(tenant_id, workspace_id) DO UPDATE SET direct_count = workspace_sharing_summary.direct_count + 1, non_host_count = workspace_sharing_summary.non_host_count + excluded.non_host_count, guest_count = workspace_sharing_summary.guest_count + excluded.guest_count;
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER sharing_member_au AFTER UPDATE OF workspace_id, principal_id, role ON workspace_member
    FOR EACH ROW EXECUTE FUNCTION trg_sharing_member_au();

CREATE OR REPLACE FUNCTION trg_sharing_workspace_ai() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO workspace_sharing_summary (tenant_id, workspace_id) VALUES (new.tenant_id, NEW.id) ON CONFLICT DO NOTHING;
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER sharing_workspace_ai AFTER INSERT ON workspace
    FOR EACH ROW EXECUTE FUNCTION trg_sharing_workspace_ai();

CREATE OR REPLACE FUNCTION trg_workspace_content_message_insert() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT COALESCE((new.role IN ('user', 'assistant') AND NOT SQLITE_JULIANDAY(new.created_at) IS NULL), false) THEN
        RETURN NULL;
    END IF;
    UPDATE workspace SET last_content_activity = new.created_at WHERE (id = (SELECT workspace_id FROM agent_session WHERE id = new.agent_id AND agent_session.tenant_id = new.tenant_id) AND (last_content_activity IS NULL OR SQLITE_JULIANDAY(last_content_activity) IS NULL OR SQLITE_JULIANDAY(new.created_at) > SQLITE_JULIANDAY(last_content_activity))) AND workspace.tenant_id = new.tenant_id;
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER workspace_content_message_insert AFTER INSERT ON agent_message
    FOR EACH ROW EXECUTE FUNCTION trg_workspace_content_message_insert();

CREATE OR REPLACE FUNCTION trg_workspace_content_message_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT COALESCE((new.role IN ('user', 'assistant') AND NOT SQLITE_JULIANDAY(new.created_at) IS NULL), false) THEN
        RETURN NULL;
    END IF;
    UPDATE workspace SET last_content_activity = new.created_at WHERE (id = (SELECT workspace_id FROM agent_session WHERE id = new.agent_id AND agent_session.tenant_id = new.tenant_id) AND (last_content_activity IS NULL OR SQLITE_JULIANDAY(last_content_activity) IS NULL OR SQLITE_JULIANDAY(new.created_at) > SQLITE_JULIANDAY(last_content_activity))) AND workspace.tenant_id = new.tenant_id;
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER workspace_content_message_update AFTER UPDATE OF created_at, role, agent_id ON agent_message
    FOR EACH ROW EXECUTE FUNCTION trg_workspace_content_message_update();

CREATE OR REPLACE FUNCTION trg_workspace_content_note_insert() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT COALESCE((NOT SQLITE_JULIANDAY(new.updated_at) IS NULL), false) THEN
        RETURN NULL;
    END IF;
    UPDATE workspace SET last_content_activity = new.updated_at WHERE (id = new.workspace_id AND (last_content_activity IS NULL OR SQLITE_JULIANDAY(last_content_activity) IS NULL OR SQLITE_JULIANDAY(new.updated_at) > SQLITE_JULIANDAY(last_content_activity))) AND workspace.tenant_id = new.tenant_id;
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER workspace_content_note_insert AFTER INSERT ON note
    FOR EACH ROW EXECUTE FUNCTION trg_workspace_content_note_insert();

CREATE OR REPLACE FUNCTION trg_workspace_content_note_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT COALESCE((NOT SQLITE_JULIANDAY(new.updated_at) IS NULL), false) THEN
        RETURN NULL;
    END IF;
    UPDATE workspace SET last_content_activity = new.updated_at WHERE (id = new.workspace_id AND (last_content_activity IS NULL OR SQLITE_JULIANDAY(last_content_activity) IS NULL OR SQLITE_JULIANDAY(new.updated_at) > SQLITE_JULIANDAY(last_content_activity))) AND workspace.tenant_id = new.tenant_id;
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER workspace_content_note_update AFTER UPDATE OF updated_at, workspace_id ON note
    FOR EACH ROW EXECUTE FUNCTION trg_workspace_content_note_update();

CREATE OR REPLACE FUNCTION trg_workspace_owner_default_ai() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE workspace SET owner_principal_id = (SELECT id FROM principal WHERE is_primary = 1 AND principal.tenant_id = new.tenant_id) WHERE (id = NEW.id AND owner_principal_id IS NULL) AND workspace.tenant_id = new.tenant_id;
    INSERT INTO workspace_member (tenant_id, workspace_id, principal_id, role, added_at) SELECT new.tenant_id AS tenant_id, NEW.id, p.id, 'owner', INTENT_NOW_ISO() FROM principal AS p WHERE p.id = COALESCE(NEW.owner_principal_id, (SELECT id FROM principal WHERE is_primary = 1 AND principal.tenant_id = new.tenant_id)) AND p.tenant_id = new.tenant_id ON CONFLICT DO NOTHING;
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER workspace_owner_default_ai AFTER INSERT ON workspace
    FOR EACH ROW EXECUTE FUNCTION trg_workspace_owner_default_ai();

CREATE OR REPLACE FUNCTION trg_workspace_scripts_initialized_insert() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE workspace SET scripts_initialized = 1 WHERE (id = new.workspace_id AND scripts_initialized = 0) AND workspace.tenant_id = new.tenant_id;
    RETURN NULL;
END
$$;
CREATE OR REPLACE TRIGGER workspace_scripts_initialized_insert AFTER INSERT ON script
    FOR EACH ROW EXECUTE FUNCTION trg_workspace_scripts_initialized_insert();

-- row_seq: a table's insertion order, numbered per tenant.
--
-- ORDER BY row_seq stands in for SQLite's rowid order, and the replay and
-- compaction pagers page by it. An identity column cannot provide it on
-- sharded Neki: each router hands out identity values in batches of 1000,
-- so with more than one router rows are numbered 1, 1001, 2, 1002, ...
--
-- Instead row_seq comes from tenant_writer.last_row_seq. Every store writer
-- already holds its tenant's tenant_writer row lock (the
-- postgres_backend_lint enforces begin_write), so the counter adds no lock
-- wait, values follow commit order, and the row lives on the tenant's
-- shard: no cross-shard sequence and no advisory lock. One counter serves
-- every table of a tenant; gaps within one table are harmless.
--
-- A BEFORE INSERT trigger fills row_seq when the insert leaves it NULL, so
-- no store query names it. It refuses an insert whose transaction does not
-- hold the tenant's writer lock. An explicit value is kept: only the
-- spec-note id rewrite sets one, copying the row's own earlier value.
--
-- Every table with a row_seq column needs both triggers below;
-- postgres_backend::row_seq::every_row_seq_table_uses_the_tenant_counter
-- checks that.
--
-- Allocation is cached in the transaction: the first row_seq a transaction
-- takes reads tenant_writer.last_row_seq once; later ones count on in
-- transaction-local settings, and an AFTER INSERT statement trigger writes
-- the last value back once per INSERT statement. Updating the counter row
-- for every inserted row instead costs O(n) per row (a transaction's own
-- earlier row versions cannot be pruned): 10,000 rows in one statement took
-- 4 s against 0.09 s with the identity, and 0.11 s with this. The writer
-- lock is held until the transaction ends, so no other transaction reads
-- the counter in between. (A deferred constraint trigger would write it once
-- per transaction, but CREATE CONSTRAINT TRIGGER has no OR REPLACE, and every
-- statement here must survive a replay.)
--
-- Settings, all transaction-local (rolled back with a savepoint):
--   intent.row_seq_tenant  tenant whose counter is loaded ('' or unset: none)
--   intent.row_seq_last    last row_seq handed out
--   intent.row_seq_saved   last value written to tenant_writer

-- Writes p_last as the tenant's counter. A missing row is an error.
CREATE OR REPLACE FUNCTION intent_row_seq_write(p_tenant text, p_last bigint) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    UPDATE tenant_writer SET last_row_seq = p_last WHERE tenant_writer.tenant_id = p_tenant;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'tenant % has no tenant_writer row to number its rows', p_tenant
            USING ERRCODE = '55000';
    END IF;
END
$$;

-- Writes the loaded counter back if it moved since the last write.
CREATE OR REPLACE FUNCTION intent_row_seq_save() RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    v_tenant text := COALESCE(current_setting('intent.row_seq_tenant', true), '');
    v_last text := COALESCE(current_setting('intent.row_seq_last', true), '');
BEGIN
    IF v_tenant = '' OR v_last = COALESCE(current_setting('intent.row_seq_saved', true), '') THEN
        RETURN;
    END IF;
    PERFORM intent_row_seq_write(v_tenant, v_last::bigint);
    PERFORM set_config('intent.row_seq_saved', v_last, true);
END
$$;

-- The tenant's next row_seq. The caller holds the tenant's writer lock, so
-- its tenant_writer row exists; a missing row is an error, never a NULL or
-- 0 row_seq.
CREATE OR REPLACE FUNCTION intent_next_row_seq(p_tenant text) RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE
    v_seq bigint;
    v_stored bigint;
BEGIN
    IF COALESCE(current_setting('intent.row_seq_tenant', true), '') = p_tenant THEN
        v_seq := current_setting('intent.row_seq_last')::bigint + 1;
    ELSE
        PERFORM intent_row_seq_save();
        SELECT tw.last_row_seq INTO v_stored FROM tenant_writer AS tw WHERE tw.tenant_id = p_tenant;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'tenant % has no tenant_writer row to number its rows', p_tenant
                USING ERRCODE = '55000';
        END IF;
        v_seq := v_stored + 1;
        PERFORM set_config('intent.row_seq_tenant', p_tenant, true);
        PERFORM set_config('intent.row_seq_saved', v_stored::text, true);
    END IF;
    PERFORM set_config('intent.row_seq_last', v_seq::text, true);
    RETURN v_seq;
END
$$;

-- Every insert into a row_seq table must run in a transaction holding the
-- row's tenant writer lock. The lock statement (WRITER_LOCK_SQL in
-- src/postgres/mod.rs) sets intent.writer_tenant to the tenant for the rest
-- of its transaction, on the shard that holds the lock row; anything else
-- (a writer on plain begin(), a raw admin insert, another tenant's lock)
-- fails here instead of taking an unordered row_seq.
CREATE OR REPLACE FUNCTION trg_row_seq_from_tenant_counter() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF current_setting('intent.writer_tenant', true) IS DISTINCT FROM NEW.tenant_id THEN
        RAISE EXCEPTION 'insert into % for tenant % without its tenant_writer lock', TG_TABLE_NAME, NEW.tenant_id
            USING ERRCODE = '55000';
    END IF;
    IF NEW.row_seq IS NULL THEN
        NEW.row_seq := intent_next_row_seq(NEW.tenant_id);
    END IF;
    RETURN NEW;
END
$$;

-- After each INSERT statement: persists the transaction's counter.
CREATE OR REPLACE FUNCTION trg_row_seq_save() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM intent_row_seq_save();
    RETURN NULL;
END
$$;

CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON advisory_wake_delivery
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON advisory_wake_delivery
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON agent_flipped_completion
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON agent_flipped_completion
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON agent_message
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON agent_message
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON agent_message_payload
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON agent_message_payload
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON agent_metrics
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON agent_metrics
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON agent_queue
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON agent_queue
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON agent_queue_admission
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON agent_queue_admission
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON agent_session
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON agent_session
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON agent_stop_redelivery
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON agent_stop_redelivery
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON agent_usage_cell
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON agent_usage_cell
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON attachment_idempotency_keys
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON attachment_idempotency_keys
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON attachments
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON attachments
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON browser_tab
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON browser_tab
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON client
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON client
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON cloud_machine_provider
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON cloud_machine_provider
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON comment
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON comment
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON completion_wake_delivery
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON completion_wake_delivery
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON completion_watch
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON completion_watch
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON delegation_group
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON delegation_group
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON deleted_workspace_id
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON deleted_workspace_id
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON diffs
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON diffs
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON discussion_agent_activity
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON discussion_agent_activity
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON discussion_agent_delivery
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON discussion_agent_delivery
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON discussion_agent_grant
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON discussion_agent_grant
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON discussion_conversation
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON discussion_conversation
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON discussion_participant
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON discussion_participant
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON discussion_reaction
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON discussion_reaction
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON draft
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON draft
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON event
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON event
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON event_subscription
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON event_subscription
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON execution_node
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON execution_node
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON executor_binding
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON executor_binding
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON executor_credential
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON executor_credential
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON exposed_machine
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON exposed_machine
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON home_move
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON home_move
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON home_rebuild
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON home_rebuild
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON home_stop
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON home_stop
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON hook
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON hook
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON host_invite
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON host_invite
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON host_member
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON host_member
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON idempotency_key
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON idempotency_key
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON interrupted_agent
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON interrupted_agent
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON known_repo
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON known_repo
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON mcp_oauth_tokens
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON mcp_oauth_tokens
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON node_assignment
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON node_assignment
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON node_checkpoint
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON node_checkpoint
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON node_invite
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON node_invite
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON node_lease
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON node_lease
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON note
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON note
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON note_line_attribution
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON note_line_attribution
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON note_page_entry
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON note_page_entry
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON note_page_head
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON note_page_head
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON note_version
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON note_version
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON pr_monitor
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON pr_monitor
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON pr_monitor_message_provenance
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON pr_monitor_message_provenance
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON principal
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON principal
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON principal_credential
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON principal_credential
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON principal_revocation
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON principal_revocation
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON pull_request_workspace
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON pull_request_workspace
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON pull_request_workspace_candidate
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON pull_request_workspace_candidate
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON remote_agent
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON remote_agent
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON remote_final_stop
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON remote_final_stop
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON review_context_enrollment
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON review_context_enrollment
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON review_context_private_family
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON review_context_private_family
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON script
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON script
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON script_monitor
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON script_monitor
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON script_notification_job
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON script_notification_job
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON settings
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON settings
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON task_agent_link
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON task_agent_link
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON tracked_changes
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON tracked_changes
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON usage_stats_hourly
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON usage_stats_hourly
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON workspace
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON workspace
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON workspace_context_item
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON workspace_context_item
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON workspace_discussion_conversation
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON workspace_discussion_conversation
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON workspace_discussion_message
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON workspace_discussion_message
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON workspace_git_root
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON workspace_git_root
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON workspace_home
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON workspace_home
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON workspace_invite
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON workspace_invite
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON workspace_invite_seat
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON workspace_invite_seat
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON workspace_mcp_disabled_server
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON workspace_mcp_disabled_server
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON workspace_member
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON workspace_member
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON workspace_metrics
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON workspace_metrics
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON workspace_sharing_summary
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON workspace_sharing_summary
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON workspace_ui_context
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON workspace_ui_context
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

-- Rows the SQLite migrations seed once per database, created once per tenant.
-- The Postgres store calls intent_provision_tenant when it opens a tenant;
-- it is idempotent. Mirrors:
--   0033_chief_workspace.sql   the __chief__ workspace
--   0125_principals.sql        the primary principal (random UUID v4)
--   0133_host_membership.sql   the host_membership_state singleton
-- Inserting the workspace after the principal lets the workspace triggers
-- set its owner and create its member, sharing summary, authority revision
-- and repository selection rows, as the SQLite backfills did.
--
-- No advisory lock: a table-less advisory lock carries no shard key, so on
-- Neki sharded by `tenant_id` the router would run it on another shard than
-- the tenant's, and in single-shard mode (every serving pool) that fails
-- with NK313. The store calls this function in a writer transaction, which
-- holds the tenant's tenant_writer row lock, and that already serializes
-- provisioning of one tenant.
CREATE OR REPLACE FUNCTION intent_provision_tenant(p_tenant text) RETURNS void
LANGUAGE plpgsql
SET search_path FROM CURRENT AS $$
DECLARE
    v_primary text;
BEGIN
    IF p_tenant IS NULL OR p_tenant = '' THEN
        RAISE EXCEPTION 'tenant id is required';
    END IF;

    SELECT id INTO v_primary FROM principal
    WHERE principal.tenant_id = p_tenant AND is_primary = 1;
    IF v_primary IS NULL THEN
        v_primary := gen_random_uuid()::text;
        INSERT INTO principal (tenant_id, id, is_primary, created_at, updated_at)
        VALUES (p_tenant, v_primary, 1, intent_now_iso(), intent_now_iso());
    END IF;

    INSERT INTO host_membership_state (tenant_id, id) VALUES (p_tenant, 1)
    ON CONFLICT (tenant_id, id) DO NOTHING;

    INSERT INTO workspace (
        tenant_id, id, title, branch, status, attention, skip_worktree, is_remote,
        archived, tags, created_at, updated_at, legacy_author_principal_id
    ) VALUES (
        p_tenant, '__chief__', 'Chief of Staff', '', 'Active', 'none', 0, 0,
        0, '[]', '2026-01-01T00:00:00.000Z', '2026-01-01T00:00:00.000Z', v_primary
    )
    ON CONFLICT (tenant_id, id) DO NOTHING;
END
$$;
