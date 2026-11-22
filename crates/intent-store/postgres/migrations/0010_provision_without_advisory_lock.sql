-- intent_provision_tenant without its transaction advisory lock. A
-- table-less advisory lock carries no shard key, so on Neki sharded by
-- `tenant_id` the router runs it on another shard than the tenant's: in
-- single-shard mode (every serving pool) that fails with NK313, so no tenant
-- on a non-authoritative shard could open. The lock is redundant: the store
-- calls this function in a writer transaction, which holds the tenant's
-- `tenant_writer` row lock (0008), and that already serializes provisioning
-- of one tenant. Otherwise identical to 0005.
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
