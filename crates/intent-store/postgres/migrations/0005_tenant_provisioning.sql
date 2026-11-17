-- Rows the SQLite migrations seed once per database, created once per tenant.
-- The Postgres store calls intent_provision_tenant when it opens a tenant;
-- it is idempotent. Mirrors:
--   0033_chief_workspace.sql   the __chief__ workspace
--   0125_principals.sql        the primary principal (random UUID v4)
--   0133_host_membership.sql   the host_membership_state singleton
-- Inserting the workspace after the principal lets the workspace triggers
-- set its owner and create its member, sharing summary, authority revision
-- and repository selection rows, as the SQLite backfills did.
CREATE OR REPLACE FUNCTION intent_provision_tenant(p_tenant text) RETURNS void
LANGUAGE plpgsql
SET search_path FROM CURRENT AS $$
DECLARE
    v_primary text;
BEGIN
    IF p_tenant IS NULL OR p_tenant = '' THEN
        RAISE EXCEPTION 'tenant id is required';
    END IF;
    -- One provisioning per tenant at a time.
    PERFORM pg_advisory_xact_lock(hashtextextended('intent_provision_tenant:' || p_tenant, 0));

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
