CREATE FUNCTION pgdm_bulk_hint_pages(regclass,bigint[]) RETURNS bigint
AS 'MODULE_PATHNAME', 'pgdm_bulk_hint_pages'
LANGUAGE C VOLATILE STRICT PARALLEL UNSAFE;
