-- Install only after all build .5 workers and evidence queries have finished.
-- A new backend must load the .6 library; the old nine-field call is removed.
DROP FUNCTION pgdm_bulk_chunk(text, bigint, bigint, text[], boolean);
ALTER TYPE pgdm_bulk_result ADD ATTRIBUTE manifest jsonb;
ALTER TYPE pgdm_bulk_result ADD ATTRIBUTE setup_ms double precision;
ALTER TYPE pgdm_bulk_result ADD ATTRIBUTE source_read_ms double precision;
ALTER TYPE pgdm_bulk_result ADD ATTRIBUTE finalize_ms double precision;
CREATE FUNCTION pgdm_bulk_chunk(shadow_schema text, lo bigint, hi bigint, scope text[], by_tid boolean DEFAULT true)
RETURNS pgdm_bulk_result AS 'MODULE_PATHNAME', 'pgdm_bulk_chunk'
LANGUAGE C VOLATILE STRICT PARALLEL UNSAFE;
