-- SQLite compatibility functions for the multi-tenant Postgres backend. Store
-- SQL keeps SQLite's JSON1 semantics on Postgres by calling these instead of
-- the SQLite built-ins. All are IMMUTABLE (except intent_now_iso and
-- sqlite_julianday, which read the clock) so expression and partial indexes can use them. Each
-- pins its search_path: Postgres 17+ builds indexes with a restricted one.

-- Parse JSON text; NULL when malformed (SQLite json_valid is false).
CREATE FUNCTION intent_json(t text) RETURNS jsonb
LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE
SET search_path FROM CURRENT AS $$
BEGIN
    RETURN t::jsonb;
EXCEPTION WHEN others THEN
    RETURN NULL;
END
$$;

-- '$.a.b[2]' -> '{a,b,2}'. Only the path shapes the store uses.
CREATE FUNCTION intent_json_path(p text) RETURNS text[]
LANGUAGE sql IMMUTABLE PARALLEL SAFE
SET search_path FROM CURRENT AS $$
    SELECT CASE WHEN p = '$' THEN ARRAY[]::text[]
        ELSE string_to_array(
            regexp_replace(regexp_replace(substr(p, 2), '\[(\d+)\]', '.\1', 'g'), '^\.', ''),
            '.')
    END
$$;

CREATE FUNCTION sqlite_json_valid(t text) RETURNS boolean
LANGUAGE sql IMMUTABLE PARALLEL SAFE
SET search_path FROM CURRENT AS $$
    SELECT t IS NOT NULL AND intent_json(t) IS NOT NULL
$$;

-- SQLite json_extract: SQL scalar for scalars (text here), JSON text for
-- objects/arrays, NULL for JSON null or a missing path.
CREATE FUNCTION sqlite_json_extract(t text, p text) RETURNS text
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
CREATE FUNCTION sqlite_json_type(t text, p text DEFAULT '$') RETURNS text
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
CREATE FUNCTION sqlite_json_each(t text)
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
CREATE FUNCTION intent_now_iso() RETURNS text
LANGUAGE sql STABLE PARALLEL SAFE
SET search_path FROM CURRENT AS $$
    SELECT to_char(clock_timestamp() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
$$;

-- x GLOB p, with p already translated to an anchored regex.
CREATE FUNCTION intent_regex(x text, re text) RETURNS boolean
LANGUAGE sql IMMUTABLE PARALLEL SAFE
SET search_path FROM CURRENT AS $$
    SELECT x ~ re
$$;

-- SQLite typeof() over the column types the baseline uses.
CREATE FUNCTION sqlite_typeof(x bigint) RETURNS text LANGUAGE sql IMMUTABLE PARALLEL SAFE
SET search_path FROM CURRENT AS
$$ SELECT CASE WHEN x IS NULL THEN 'null' ELSE 'integer' END $$;
CREATE FUNCTION sqlite_typeof(x text) RETURNS text LANGUAGE sql IMMUTABLE PARALLEL SAFE
SET search_path FROM CURRENT AS
$$ SELECT CASE WHEN x IS NULL THEN 'null' ELSE 'text' END $$;
CREATE FUNCTION sqlite_typeof(x double precision) RETURNS text LANGUAGE sql IMMUTABLE PARALLEL SAFE
SET search_path FROM CURRENT AS
$$ SELECT CASE WHEN x IS NULL THEN 'null' ELSE 'real' END $$;
CREATE FUNCTION sqlite_typeof(x bytea) RETURNS text LANGUAGE sql IMMUTABLE PARALLEL SAFE
SET search_path FROM CURRENT AS
$$ SELECT CASE WHEN x IS NULL THEN 'null' ELSE 'blob' END $$;

-- SQLite `t -> '$.p'`: the JSON text of the value at the path.
CREATE FUNCTION sqlite_json_arrow(t text, p text) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE
SET search_path FROM CURRENT AS $$
    SELECT (intent_json(t) #> intent_json_path(p))::text
$$;

-- SQLite julianday() over 'now' and the ISO-8601 forms the store writes;
-- NULL otherwise. STABLE because of 'now'.
CREATE FUNCTION sqlite_julianday(t text) RETURNS double precision
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
CREATE FUNCTION sqlite_json(t text) RETURNS jsonb
LANGUAGE sql IMMUTABLE PARALLEL SAFE
SET search_path FROM CURRENT AS $$
    SELECT t::jsonb
$$;

-- One json_set step: like SQLite, create missing parent objects, then set
-- the leaf (SQL NULL becomes JSON null).
CREATE FUNCTION intent_json_set_step(doc jsonb, p text, v jsonb) RETURNS jsonb
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
CREATE FUNCTION sqlite_json_set(t text, p text, v jsonb) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE
SET search_path FROM CURRENT AS $$
    SELECT intent_json_set_step(t::jsonb, p, v)::text
$$;
CREATE FUNCTION sqlite_json_set(t text, p text, v text) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE
SET search_path FROM CURRENT AS $$
    SELECT intent_json_set_step(t::jsonb, p, to_jsonb(v))::text
$$;
CREATE FUNCTION sqlite_json_set(t text, p text, v bigint) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE
SET search_path FROM CURRENT AS $$
    SELECT intent_json_set_step(t::jsonb, p, to_jsonb(v))::text
$$;
CREATE FUNCTION sqlite_json_set(t text, p text, v double precision) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE
SET search_path FROM CURRENT AS $$
    SELECT intent_json_set_step(t::jsonb, p, to_jsonb(v))::text
$$;
CREATE FUNCTION sqlite_json_set(t text, p1 text, v1 jsonb, p2 text, v2 jsonb) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE
SET search_path FROM CURRENT AS $$
    SELECT intent_json_set_step(intent_json_set_step(t::jsonb, p1, v1), p2, v2)::text
$$;
CREATE FUNCTION sqlite_json_set(t text, p1 text, v1 text, p2 text, v2 text) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE
SET search_path FROM CURRENT AS $$
    SELECT intent_json_set_step(intent_json_set_step(t::jsonb, p1, to_jsonb(v1)), p2, to_jsonb(v2))::text
$$;
