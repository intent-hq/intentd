-- SQLite compatibility functions for the multi-tenant Postgres backend.
-- SQLite compatibility functions. Store SQL keeps SQLite's JSON1 semantics on
-- Postgres by calling these instead of the SQLite built-ins. All are IMMUTABLE
-- (except intent_now_iso) so expression and partial indexes can use them.

-- Parse JSON text; NULL when malformed (SQLite json_valid is false).
CREATE FUNCTION intent_json(t text) RETURNS jsonb
LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$
BEGIN
    RETURN t::jsonb;
EXCEPTION WHEN others THEN
    RETURN NULL;
END
$$;

-- '$.a.b[2]' -> '{a,b,2}'. Only the path shapes the store uses.
CREATE FUNCTION intent_json_path(p text) RETURNS text[]
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT CASE WHEN p = '$' THEN ARRAY[]::text[]
        ELSE string_to_array(
            regexp_replace(regexp_replace(substr(p, 2), '\[(\d+)\]', '.\1', 'g'), '^\.', ''),
            '.')
    END
$$;

CREATE FUNCTION sqlite_json_valid(t text) RETURNS boolean
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT t IS NOT NULL AND intent_json(t) IS NOT NULL
$$;

-- SQLite json_extract: SQL scalar for scalars (text here), JSON text for
-- objects/arrays, NULL for JSON null or a missing path.
CREATE FUNCTION sqlite_json_extract(t text, p text) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
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
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
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
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT e.key, sqlite_json_extract(e.value::text, '$'), sqlite_json_type(e.value::text)
    FROM jsonb_each(CASE WHEN jsonb_typeof(intent_json(t)) = 'object' THEN intent_json(t) END) e
    UNION ALL
    SELECT (a.ord - 1)::text, sqlite_json_extract(a.value::text, '$'), sqlite_json_type(a.value::text)
    FROM jsonb_array_elements(CASE WHEN jsonb_typeof(intent_json(t)) = 'array' THEN intent_json(t) END)
        WITH ORDINALITY a(value, ord)
$$;

-- strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
CREATE FUNCTION intent_now_iso() RETURNS text
LANGUAGE sql STABLE PARALLEL SAFE AS $$
    SELECT to_char(clock_timestamp() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
$$;

-- x GLOB p, with p already translated to an anchored regex.
CREATE FUNCTION intent_regex(x text, re text) RETURNS boolean
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT x ~ re
$$;

-- SQLite typeof() over the column types the baseline uses.
CREATE FUNCTION sqlite_typeof(x bigint) RETURNS text LANGUAGE sql IMMUTABLE PARALLEL SAFE AS
$$ SELECT CASE WHEN x IS NULL THEN 'null' ELSE 'integer' END $$;
CREATE FUNCTION sqlite_typeof(x text) RETURNS text LANGUAGE sql IMMUTABLE PARALLEL SAFE AS
$$ SELECT CASE WHEN x IS NULL THEN 'null' ELSE 'text' END $$;
CREATE FUNCTION sqlite_typeof(x double precision) RETURNS text LANGUAGE sql IMMUTABLE PARALLEL SAFE AS
$$ SELECT CASE WHEN x IS NULL THEN 'null' ELSE 'real' END $$;
CREATE FUNCTION sqlite_typeof(x bytea) RETURNS text LANGUAGE sql IMMUTABLE PARALLEL SAFE AS
$$ SELECT CASE WHEN x IS NULL THEN 'null' ELSE 'blob' END $$;

-- SQLite `t -> '$.p'`: the JSON text of the value at the path.
CREATE FUNCTION sqlite_json_arrow(t text, p text) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT (intent_json(t) #> intent_json_path(p))::text
$$;

-- SQLite julianday() over the ISO-8601 forms the store writes; NULL otherwise.
CREATE FUNCTION sqlite_julianday(t text) RETURNS double precision
LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$
BEGIN
    IF t IS NULL OR t !~ '^\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?)?(Z|[+-]\d{2}:\d{2})?$' THEN
        RETURN NULL;
    END IF;
    RETURN extract(epoch FROM (CASE WHEN t ~ '(Z|[+-]\d{2}:\d{2})$' THEN t ELSE t || 'Z' END)::timestamptz)
        / 86400.0 + 2440587.5;
EXCEPTION WHEN others THEN
    RETURN NULL;
END
$$;

-- SQLite json_set(t, path, json(value), ...) where every value is JSON text.
CREATE FUNCTION sqlite_json_set(t text, VARIADIC kv text[]) RETURNS text
LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$
DECLARE
    doc jsonb := intent_json(t);
    i int := 1;
BEGIN
    IF doc IS NULL THEN
        RETURN NULL;
    END IF;
    WHILE i < array_length(kv, 1) LOOP
        doc := jsonb_set(doc, intent_json_path(kv[i]), kv[i + 1]::jsonb, true);
        i := i + 2;
    END LOOP;
    RETURN doc::text;
END
$$;
