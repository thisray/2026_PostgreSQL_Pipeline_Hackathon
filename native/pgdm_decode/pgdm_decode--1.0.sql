CREATE TYPE pgdm_decoded AS (
 raw_id bigint, external_id integer, observed_at timestamp, max_declared smallint,
 n2 integer, n1 integer, speed numeric, ref text, code2 text, code3 text,
 parent_ok boolean, slot_pos smallint[], slot_weight integer[], slot_type text[], slot_gap integer[]
);
CREATE FUNCTION pgdm_decode_row(bigint, jsonb) RETURNS pgdm_decoded
AS 'MODULE_PATHNAME', 'pgdm_decode_row' LANGUAGE C STABLE STRICT PARALLEL SAFE;
CREATE FUNCTION pgdm_decode_chunk(lo bigint, hi bigint, scope text[])
RETURNS SETOF pgdm_decoded AS 'MODULE_PATHNAME', 'pgdm_decode_chunk'
LANGUAGE C STABLE STRICT PARALLEL RESTRICTED;
