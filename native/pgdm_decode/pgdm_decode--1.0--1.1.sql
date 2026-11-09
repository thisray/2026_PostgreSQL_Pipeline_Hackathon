CREATE TYPE pgdm_decoded_v2 AS (
 raw_id bigint, external_id integer, observed_at timestamp, max_declared smallint,
 n2 integer, n1 integer, speed numeric, ref text, code2 text, code3 text,
 parent_ok boolean, slot_pos smallint[], slot_weight integer[], slot_type text[], slot_gap integer[],
 raw_schema text, raw_hash text
);
CREATE FUNCTION pgdm_decode_chunk_v2(lo bigint, hi bigint, scope text[], by_tid boolean DEFAULT true)
RETURNS SETOF pgdm_decoded_v2 AS 'MODULE_PATHNAME', 'pgdm_decode_chunk_v2'
LANGUAGE C STABLE STRICT PARALLEL RESTRICTED;
CREATE FUNCTION pgdm_decode_build_id() RETURNS text
AS 'MODULE_PATHNAME', 'pgdm_decode_build_id' LANGUAGE C IMMUTABLE PARALLEL SAFE;
