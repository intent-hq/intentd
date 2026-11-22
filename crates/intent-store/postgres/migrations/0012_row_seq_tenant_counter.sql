-- row_seq from a per-tenant counter instead of an identity column.
--
-- row_seq is a table's insertion order: ORDER BY row_seq stands in for
-- SQLite's rowid order, and the replay and compaction pagers page by it. On
-- sharded Neki each router hands out identity values in batches of 1000, so
-- with more than one router an identity numbers rows 1, 1001, 2, 1002, ...
-- and that order is lost.
--
-- Instead, row_seq now comes from tenant_writer.last_row_seq. Every store
-- writer already holds its tenant's tenant_writer row lock (0008; the
-- postgres_backend_lint enforces begin_write), so the counter adds no new
-- lock wait, values follow commit order, and the row lives on the tenant's
-- shard: no cross-shard sequence and no advisory lock. One counter serves
-- every table of a tenant; gaps within one table are harmless.
--
-- A BEFORE INSERT trigger fills row_seq when the insert leaves it NULL, so
-- no store query changes. It refuses an insert whose transaction does not
-- hold the tenant's writer lock. An explicit value is kept: only the
-- spec-note id rewrite sets one, copying the row's own earlier value.
--
-- last_row_seq starts NULL. A tenant's first allocation sets it to the
-- tenant's largest existing row_seq across these tables, so new rows sort
-- after old ones and this migration scans nothing. Rows already written
-- out of order by identity batches keep that order: nothing records their
-- true insertion order. (Release builds never enable the postgres feature,
-- so only test and dogfood databases hold such rows.)
--
-- A new table with a row_seq column needs the same trigger;
-- postgres_backend::row_seq::every_row_seq_table_uses_the_tenant_counter
-- checks this.

ALTER TABLE tenant_writer ADD COLUMN IF NOT EXISTS last_row_seq bigint;

-- The tenant's largest row_seq across every row_seq table (0 if none).
CREATE OR REPLACE FUNCTION intent_row_seq_floor(p_tenant text) RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE
    v_floor bigint;
BEGIN
    SELECT COALESCE(max(floors.s), 0) INTO v_floor FROM (
        SELECT max(advisory_wake_delivery.row_seq) AS s FROM advisory_wake_delivery WHERE advisory_wake_delivery.tenant_id = p_tenant
        UNION ALL
        SELECT max(agent_flipped_completion.row_seq) AS s FROM agent_flipped_completion WHERE agent_flipped_completion.tenant_id = p_tenant
        UNION ALL
        SELECT max(agent_message.row_seq) AS s FROM agent_message WHERE agent_message.tenant_id = p_tenant
        UNION ALL
        SELECT max(agent_message_payload.row_seq) AS s FROM agent_message_payload WHERE agent_message_payload.tenant_id = p_tenant
        UNION ALL
        SELECT max(agent_metrics.row_seq) AS s FROM agent_metrics WHERE agent_metrics.tenant_id = p_tenant
        UNION ALL
        SELECT max(agent_queue.row_seq) AS s FROM agent_queue WHERE agent_queue.tenant_id = p_tenant
        UNION ALL
        SELECT max(agent_queue_admission.row_seq) AS s FROM agent_queue_admission WHERE agent_queue_admission.tenant_id = p_tenant
        UNION ALL
        SELECT max(agent_session.row_seq) AS s FROM agent_session WHERE agent_session.tenant_id = p_tenant
        UNION ALL
        SELECT max(agent_stop_redelivery.row_seq) AS s FROM agent_stop_redelivery WHERE agent_stop_redelivery.tenant_id = p_tenant
        UNION ALL
        SELECT max(agent_usage_cell.row_seq) AS s FROM agent_usage_cell WHERE agent_usage_cell.tenant_id = p_tenant
        UNION ALL
        SELECT max(attachment_idempotency_keys.row_seq) AS s FROM attachment_idempotency_keys WHERE attachment_idempotency_keys.tenant_id = p_tenant
        UNION ALL
        SELECT max(attachments.row_seq) AS s FROM attachments WHERE attachments.tenant_id = p_tenant
        UNION ALL
        SELECT max(browser_tab.row_seq) AS s FROM browser_tab WHERE browser_tab.tenant_id = p_tenant
        UNION ALL
        SELECT max(client.row_seq) AS s FROM client WHERE client.tenant_id = p_tenant
        UNION ALL
        SELECT max(cloud_machine_provider.row_seq) AS s FROM cloud_machine_provider WHERE cloud_machine_provider.tenant_id = p_tenant
        UNION ALL
        SELECT max(comment.row_seq) AS s FROM comment WHERE comment.tenant_id = p_tenant
        UNION ALL
        SELECT max(completion_wake_delivery.row_seq) AS s FROM completion_wake_delivery WHERE completion_wake_delivery.tenant_id = p_tenant
        UNION ALL
        SELECT max(completion_watch.row_seq) AS s FROM completion_watch WHERE completion_watch.tenant_id = p_tenant
        UNION ALL
        SELECT max(delegation_group.row_seq) AS s FROM delegation_group WHERE delegation_group.tenant_id = p_tenant
        UNION ALL
        SELECT max(deleted_workspace_id.row_seq) AS s FROM deleted_workspace_id WHERE deleted_workspace_id.tenant_id = p_tenant
        UNION ALL
        SELECT max(diffs.row_seq) AS s FROM diffs WHERE diffs.tenant_id = p_tenant
        UNION ALL
        SELECT max(discussion_agent_activity.row_seq) AS s FROM discussion_agent_activity WHERE discussion_agent_activity.tenant_id = p_tenant
        UNION ALL
        SELECT max(discussion_agent_delivery.row_seq) AS s FROM discussion_agent_delivery WHERE discussion_agent_delivery.tenant_id = p_tenant
        UNION ALL
        SELECT max(discussion_agent_grant.row_seq) AS s FROM discussion_agent_grant WHERE discussion_agent_grant.tenant_id = p_tenant
        UNION ALL
        SELECT max(discussion_conversation.row_seq) AS s FROM discussion_conversation WHERE discussion_conversation.tenant_id = p_tenant
        UNION ALL
        SELECT max(discussion_participant.row_seq) AS s FROM discussion_participant WHERE discussion_participant.tenant_id = p_tenant
        UNION ALL
        SELECT max(discussion_reaction.row_seq) AS s FROM discussion_reaction WHERE discussion_reaction.tenant_id = p_tenant
        UNION ALL
        SELECT max(draft.row_seq) AS s FROM draft WHERE draft.tenant_id = p_tenant
        UNION ALL
        SELECT max(event.row_seq) AS s FROM event WHERE event.tenant_id = p_tenant
        UNION ALL
        SELECT max(event_subscription.row_seq) AS s FROM event_subscription WHERE event_subscription.tenant_id = p_tenant
        UNION ALL
        SELECT max(execution_node.row_seq) AS s FROM execution_node WHERE execution_node.tenant_id = p_tenant
        UNION ALL
        SELECT max(executor_binding.row_seq) AS s FROM executor_binding WHERE executor_binding.tenant_id = p_tenant
        UNION ALL
        SELECT max(executor_credential.row_seq) AS s FROM executor_credential WHERE executor_credential.tenant_id = p_tenant
        UNION ALL
        SELECT max(exposed_machine.row_seq) AS s FROM exposed_machine WHERE exposed_machine.tenant_id = p_tenant
        UNION ALL
        SELECT max(home_move.row_seq) AS s FROM home_move WHERE home_move.tenant_id = p_tenant
        UNION ALL
        SELECT max(home_rebuild.row_seq) AS s FROM home_rebuild WHERE home_rebuild.tenant_id = p_tenant
        UNION ALL
        SELECT max(home_stop.row_seq) AS s FROM home_stop WHERE home_stop.tenant_id = p_tenant
        UNION ALL
        SELECT max(hook.row_seq) AS s FROM hook WHERE hook.tenant_id = p_tenant
        UNION ALL
        SELECT max(host_invite.row_seq) AS s FROM host_invite WHERE host_invite.tenant_id = p_tenant
        UNION ALL
        SELECT max(host_member.row_seq) AS s FROM host_member WHERE host_member.tenant_id = p_tenant
        UNION ALL
        SELECT max(idempotency_key.row_seq) AS s FROM idempotency_key WHERE idempotency_key.tenant_id = p_tenant
        UNION ALL
        SELECT max(interrupted_agent.row_seq) AS s FROM interrupted_agent WHERE interrupted_agent.tenant_id = p_tenant
        UNION ALL
        SELECT max(known_repo.row_seq) AS s FROM known_repo WHERE known_repo.tenant_id = p_tenant
        UNION ALL
        SELECT max(mcp_oauth_tokens.row_seq) AS s FROM mcp_oauth_tokens WHERE mcp_oauth_tokens.tenant_id = p_tenant
        UNION ALL
        SELECT max(node_assignment.row_seq) AS s FROM node_assignment WHERE node_assignment.tenant_id = p_tenant
        UNION ALL
        SELECT max(node_checkpoint.row_seq) AS s FROM node_checkpoint WHERE node_checkpoint.tenant_id = p_tenant
        UNION ALL
        SELECT max(node_invite.row_seq) AS s FROM node_invite WHERE node_invite.tenant_id = p_tenant
        UNION ALL
        SELECT max(node_lease.row_seq) AS s FROM node_lease WHERE node_lease.tenant_id = p_tenant
        UNION ALL
        SELECT max(note.row_seq) AS s FROM note WHERE note.tenant_id = p_tenant
        UNION ALL
        SELECT max(note_line_attribution.row_seq) AS s FROM note_line_attribution WHERE note_line_attribution.tenant_id = p_tenant
        UNION ALL
        SELECT max(note_page_entry.row_seq) AS s FROM note_page_entry WHERE note_page_entry.tenant_id = p_tenant
        UNION ALL
        SELECT max(note_page_head.row_seq) AS s FROM note_page_head WHERE note_page_head.tenant_id = p_tenant
        UNION ALL
        SELECT max(note_version.row_seq) AS s FROM note_version WHERE note_version.tenant_id = p_tenant
        UNION ALL
        SELECT max(pr_monitor.row_seq) AS s FROM pr_monitor WHERE pr_monitor.tenant_id = p_tenant
        UNION ALL
        SELECT max(pr_monitor_message_provenance.row_seq) AS s FROM pr_monitor_message_provenance WHERE pr_monitor_message_provenance.tenant_id = p_tenant
        UNION ALL
        SELECT max(principal.row_seq) AS s FROM principal WHERE principal.tenant_id = p_tenant
        UNION ALL
        SELECT max(principal_credential.row_seq) AS s FROM principal_credential WHERE principal_credential.tenant_id = p_tenant
        UNION ALL
        SELECT max(principal_revocation.row_seq) AS s FROM principal_revocation WHERE principal_revocation.tenant_id = p_tenant
        UNION ALL
        SELECT max(pull_request_workspace.row_seq) AS s FROM pull_request_workspace WHERE pull_request_workspace.tenant_id = p_tenant
        UNION ALL
        SELECT max(pull_request_workspace_candidate.row_seq) AS s FROM pull_request_workspace_candidate WHERE pull_request_workspace_candidate.tenant_id = p_tenant
        UNION ALL
        SELECT max(remote_agent.row_seq) AS s FROM remote_agent WHERE remote_agent.tenant_id = p_tenant
        UNION ALL
        SELECT max(remote_final_stop.row_seq) AS s FROM remote_final_stop WHERE remote_final_stop.tenant_id = p_tenant
        UNION ALL
        SELECT max(review_context_enrollment.row_seq) AS s FROM review_context_enrollment WHERE review_context_enrollment.tenant_id = p_tenant
        UNION ALL
        SELECT max(review_context_private_family.row_seq) AS s FROM review_context_private_family WHERE review_context_private_family.tenant_id = p_tenant
        UNION ALL
        SELECT max(script.row_seq) AS s FROM script WHERE script.tenant_id = p_tenant
        UNION ALL
        SELECT max(script_monitor.row_seq) AS s FROM script_monitor WHERE script_monitor.tenant_id = p_tenant
        UNION ALL
        SELECT max(script_notification_job.row_seq) AS s FROM script_notification_job WHERE script_notification_job.tenant_id = p_tenant
        UNION ALL
        SELECT max(settings.row_seq) AS s FROM settings WHERE settings.tenant_id = p_tenant
        UNION ALL
        SELECT max(task_agent_link.row_seq) AS s FROM task_agent_link WHERE task_agent_link.tenant_id = p_tenant
        UNION ALL
        SELECT max(tracked_changes.row_seq) AS s FROM tracked_changes WHERE tracked_changes.tenant_id = p_tenant
        UNION ALL
        SELECT max(usage_stats_hourly.row_seq) AS s FROM usage_stats_hourly WHERE usage_stats_hourly.tenant_id = p_tenant
        UNION ALL
        SELECT max(workspace.row_seq) AS s FROM workspace WHERE workspace.tenant_id = p_tenant
        UNION ALL
        SELECT max(workspace_context_item.row_seq) AS s FROM workspace_context_item WHERE workspace_context_item.tenant_id = p_tenant
        UNION ALL
        SELECT max(workspace_discussion_conversation.row_seq) AS s FROM workspace_discussion_conversation WHERE workspace_discussion_conversation.tenant_id = p_tenant
        UNION ALL
        SELECT max(workspace_discussion_message.row_seq) AS s FROM workspace_discussion_message WHERE workspace_discussion_message.tenant_id = p_tenant
        UNION ALL
        SELECT max(workspace_git_root.row_seq) AS s FROM workspace_git_root WHERE workspace_git_root.tenant_id = p_tenant
        UNION ALL
        SELECT max(workspace_home.row_seq) AS s FROM workspace_home WHERE workspace_home.tenant_id = p_tenant
        UNION ALL
        SELECT max(workspace_invite.row_seq) AS s FROM workspace_invite WHERE workspace_invite.tenant_id = p_tenant
        UNION ALL
        SELECT max(workspace_invite_seat.row_seq) AS s FROM workspace_invite_seat WHERE workspace_invite_seat.tenant_id = p_tenant
        UNION ALL
        SELECT max(workspace_mcp_disabled_server.row_seq) AS s FROM workspace_mcp_disabled_server WHERE workspace_mcp_disabled_server.tenant_id = p_tenant
        UNION ALL
        SELECT max(workspace_member.row_seq) AS s FROM workspace_member WHERE workspace_member.tenant_id = p_tenant
        UNION ALL
        SELECT max(workspace_metrics.row_seq) AS s FROM workspace_metrics WHERE workspace_metrics.tenant_id = p_tenant
        UNION ALL
        SELECT max(workspace_sharing_summary.row_seq) AS s FROM workspace_sharing_summary WHERE workspace_sharing_summary.tenant_id = p_tenant
        UNION ALL
        SELECT max(workspace_ui_context.row_seq) AS s FROM workspace_ui_context WHERE workspace_ui_context.tenant_id = p_tenant
    ) AS floors;
    RETURN v_floor;
END
$$;

-- Allocation is cached in the transaction: the first row_seq a transaction
-- takes reads tenant_writer.last_row_seq once; later ones count on in
-- transaction-local settings, and an AFTER INSERT statement trigger writes
-- the last value back once per INSERT statement. Updating the counter row
-- for every inserted row instead costs O(n) per row (a transaction's own
-- earlier row versions cannot be pruned): 10,000 rows in one statement took
-- 4 s against 0.09 s with the identity, and 0.11 s with this. The writer
-- lock is held until the transaction ends, so no other transaction reads
-- the counter in between. (A deferred constraint trigger would write it once
-- per transaction, but CREATE CONSTRAINT TRIGGER has no OR REPLACE, and a
-- migration statement must survive a replay.)
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
        v_seq := COALESCE(v_stored, intent_row_seq_floor(p_tenant)) + 1;
        PERFORM set_config('intent.row_seq_tenant', p_tenant, true);
        PERFORM set_config('intent.row_seq_saved', COALESCE(v_stored::text, ''), true);
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

ALTER TABLE advisory_wake_delivery ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON advisory_wake_delivery
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON advisory_wake_delivery
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE agent_flipped_completion ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON agent_flipped_completion
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON agent_flipped_completion
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE agent_message ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON agent_message
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON agent_message
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE agent_message_payload ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON agent_message_payload
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON agent_message_payload
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE agent_metrics ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON agent_metrics
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON agent_metrics
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE agent_queue ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON agent_queue
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON agent_queue
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE agent_queue_admission ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON agent_queue_admission
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON agent_queue_admission
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE agent_session ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON agent_session
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON agent_session
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE agent_stop_redelivery ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON agent_stop_redelivery
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON agent_stop_redelivery
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE agent_usage_cell ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON agent_usage_cell
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON agent_usage_cell
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE attachment_idempotency_keys ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON attachment_idempotency_keys
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON attachment_idempotency_keys
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE attachments ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON attachments
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON attachments
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE browser_tab ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON browser_tab
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON browser_tab
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE client ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON client
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON client
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE cloud_machine_provider ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON cloud_machine_provider
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON cloud_machine_provider
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE comment ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON comment
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON comment
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE completion_wake_delivery ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON completion_wake_delivery
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON completion_wake_delivery
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE completion_watch ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON completion_watch
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON completion_watch
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE delegation_group ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON delegation_group
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON delegation_group
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE deleted_workspace_id ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON deleted_workspace_id
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON deleted_workspace_id
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE diffs ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON diffs
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON diffs
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE discussion_agent_activity ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON discussion_agent_activity
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON discussion_agent_activity
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE discussion_agent_delivery ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON discussion_agent_delivery
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON discussion_agent_delivery
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE discussion_agent_grant ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON discussion_agent_grant
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON discussion_agent_grant
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE discussion_conversation ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON discussion_conversation
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON discussion_conversation
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE discussion_participant ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON discussion_participant
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON discussion_participant
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE discussion_reaction ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON discussion_reaction
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON discussion_reaction
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE draft ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON draft
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON draft
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE event ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON event
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON event
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE event_subscription ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON event_subscription
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON event_subscription
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE execution_node ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON execution_node
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON execution_node
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE executor_binding ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON executor_binding
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON executor_binding
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE executor_credential ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON executor_credential
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON executor_credential
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE exposed_machine ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON exposed_machine
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON exposed_machine
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE home_move ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON home_move
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON home_move
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE home_rebuild ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON home_rebuild
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON home_rebuild
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE home_stop ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON home_stop
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON home_stop
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE hook ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON hook
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON hook
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE host_invite ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON host_invite
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON host_invite
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE host_member ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON host_member
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON host_member
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE idempotency_key ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON idempotency_key
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON idempotency_key
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE interrupted_agent ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON interrupted_agent
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON interrupted_agent
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE known_repo ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON known_repo
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON known_repo
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE mcp_oauth_tokens ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON mcp_oauth_tokens
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON mcp_oauth_tokens
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE node_assignment ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON node_assignment
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON node_assignment
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE node_checkpoint ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON node_checkpoint
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON node_checkpoint
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE node_invite ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON node_invite
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON node_invite
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE node_lease ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON node_lease
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON node_lease
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE note ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON note
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON note
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE note_line_attribution ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON note_line_attribution
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON note_line_attribution
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE note_page_entry ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON note_page_entry
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON note_page_entry
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE note_page_head ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON note_page_head
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON note_page_head
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE note_version ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON note_version
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON note_version
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE pr_monitor ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON pr_monitor
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON pr_monitor
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE pr_monitor_message_provenance ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON pr_monitor_message_provenance
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON pr_monitor_message_provenance
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE principal ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON principal
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON principal
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE principal_credential ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON principal_credential
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON principal_credential
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE principal_revocation ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON principal_revocation
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON principal_revocation
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE pull_request_workspace ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON pull_request_workspace
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON pull_request_workspace
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE pull_request_workspace_candidate ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON pull_request_workspace_candidate
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON pull_request_workspace_candidate
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE remote_agent ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON remote_agent
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON remote_agent
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE remote_final_stop ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON remote_final_stop
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON remote_final_stop
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE review_context_enrollment ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON review_context_enrollment
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON review_context_enrollment
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE review_context_private_family ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON review_context_private_family
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON review_context_private_family
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE script ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON script
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON script
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE script_monitor ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON script_monitor
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON script_monitor
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE script_notification_job ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON script_notification_job
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON script_notification_job
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE settings ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON settings
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON settings
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE task_agent_link ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON task_agent_link
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON task_agent_link
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE tracked_changes ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON tracked_changes
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON tracked_changes
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE usage_stats_hourly ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON usage_stats_hourly
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON usage_stats_hourly
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE workspace ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON workspace
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON workspace
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE workspace_context_item ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON workspace_context_item
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON workspace_context_item
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE workspace_discussion_conversation ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON workspace_discussion_conversation
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON workspace_discussion_conversation
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE workspace_discussion_message ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON workspace_discussion_message
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON workspace_discussion_message
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE workspace_git_root ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON workspace_git_root
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON workspace_git_root
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE workspace_home ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON workspace_home
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON workspace_home
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE workspace_invite ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON workspace_invite
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON workspace_invite
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE workspace_invite_seat ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON workspace_invite_seat
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON workspace_invite_seat
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE workspace_mcp_disabled_server ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON workspace_mcp_disabled_server
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON workspace_mcp_disabled_server
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE workspace_member ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON workspace_member
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON workspace_member
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE workspace_metrics ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON workspace_metrics
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON workspace_metrics
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE workspace_sharing_summary ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON workspace_sharing_summary
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON workspace_sharing_summary
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();

ALTER TABLE workspace_ui_context ALTER COLUMN row_seq DROP IDENTITY IF EXISTS;
CREATE OR REPLACE TRIGGER row_seq_from_tenant_counter BEFORE INSERT ON workspace_ui_context
    FOR EACH ROW EXECUTE FUNCTION trg_row_seq_from_tenant_counter();
CREATE OR REPLACE TRIGGER row_seq_save AFTER INSERT ON workspace_ui_context
    FOR EACH STATEMENT EXECUTE FUNCTION trg_row_seq_save();
