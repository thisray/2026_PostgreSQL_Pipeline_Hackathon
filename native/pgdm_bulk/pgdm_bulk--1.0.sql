CREATE TYPE pgdm_bulk_result AS (
 source_rows bigint, table1_rows bigint, table2_rows bigint, table5_rows bigint, table6_rows bigint,
 observed_codes text[], decode_ms double precision, insert_ms double precision, total_ms double precision
);
CREATE FUNCTION pgdm_bulk_chunk(shadow_schema text, lo bigint, hi bigint, scope text[], by_tid boolean DEFAULT true)
RETURNS pgdm_bulk_result AS 'MODULE_PATHNAME', 'pgdm_bulk_chunk'
LANGUAGE C VOLATILE STRICT PARALLEL UNSAFE;
CREATE FUNCTION pgdm_bulk_build_id() RETURNS text
AS 'MODULE_PATHNAME', 'pgdm_bulk_build_id' LANGUAGE C IMMUTABLE PARALLEL SAFE;
