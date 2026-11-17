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
