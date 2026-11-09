"""Batch the existing expansion checks in one remote psql process."""
import json
import os
import shlex

from config import CONTROL_DB
from utils import sql_ident, sql_literal


def preflight(service, database, source, prepared, cancel_event, requested=None):
    version_sql = service.get_raw_schema_version_references(database, source, prepared['raw_schemas'], sql_only=True)
    function_sql = service._validate_cross_table_expansion_functions(database, prepared['destinations'], sql_only=True)
    service._native_prepared_catalog = None
    checks = ["SET client_min_messages=warning;",
              "BEGIN;", service.ensure_admin_schema(sql_only=True), "COMMIT;",
              r"\connect " + sql_ident(database), "BEGIN;", service._data_table_row_counts_ddl_sql()]
    schema, table = prepared['source_schema_name'], prepared['source_table_name']
    checks.append(f"""DO $source$ DECLARE missing text; BEGIN
 IF NOT EXISTS(SELECT FROM pg_tables WHERE schemaname={sql_literal(schema)} AND tablename={sql_literal(table)}) THEN
 RAISE EXCEPTION USING MESSAGE={sql_literal(f'Source table {source} does not exist.')}; END IF;
 SELECT string_agg(name, ', ' ORDER BY ord) INTO missing FROM unnest(ARRAY['raw_schema','raw_hash']) WITH ORDINALITY c(name,ord)
 WHERE NOT EXISTS(SELECT FROM pg_attribute WHERE attrelid=to_regclass({sql_literal(source)}) AND attname=c.name AND attnum>0 AND NOT attisdropped);
 IF missing IS NOT NULL THEN RAISE EXCEPTION 'Source % is missing required columns: %.',{sql_literal(source)},missing; END IF;
 END $source$;""")
    for d in prepared['destinations']:
        name = d['table_name']
        checks.append(f"""DO $target$ BEGIN
 IF NOT EXISTS(SELECT FROM pg_tables WHERE schemaname={sql_literal(d['schema_name'])} AND tablename={sql_literal(d['pure_table_name'])}) THEN
 RAISE EXCEPTION USING MESSAGE={sql_literal(f'Destination table {name} does not exist.')}; END IF;
 IF NOT EXISTS(SELECT FROM public.pgdm_table_row_counts WHERE schema_name={sql_literal(d['schema_name'])} AND table_name={sql_literal(d['pure_table_name'])}) THEN
 LOCK TABLE {sql_ident(d['schema_name'])}.{sql_ident(d['pure_table_name'])} IN SHARE ROW EXCLUSIVE MODE;
 INSERT INTO public.pgdm_table_row_counts(schema_name,table_name,row_count,last_reconciled_at)
 SELECT {sql_literal(d['schema_name'])},{sql_literal(d['pure_table_name'])},count(*),clock_timestamp() FROM {sql_ident(d['schema_name'])}.{sql_ident(d['pure_table_name'])} ON CONFLICT DO NOTHING;
 END IF; END $target$;""")
        checks.append(service._validate_managed_sql_dependencies(database, name, {'insert'}, sql_only=True))
    checks.append(function_sql or "SELECT '[]'::json;")
    checks.append('COMMIT;')
    from services.expansion_optimizer import build_plan
    batch_catalog = os.getenv('PDM_EXPANSION_MODE', 'auto') == 'auto' and build_plan(prepared, requested or [], mode='compact')
    if batch_catalog:
        from services import s2_expansion as s2, s3_expansion as s3
        checks.extend(["SELECT jsonb_build_object('rc5_mark','catalog_begin','at',clock_timestamp());",
                       'BEGIN;', s2.preparation_sql(prepared, s3.EXTRA_INFRASTRUCTURE, profile=True), 'COMMIT;',
                       "SELECT jsonb_build_object('rc5_mark','catalog_end','at',clock_timestamp());"])

    checks.append(r'\connect ' + sql_ident(CONTROL_DB))
    checks.append(version_sql)
    command = f'psql -h localhost -U {shlex.quote(service.sql_username)} -d {shlex.quote(CONTROL_DB)} -XqAt -v ON_ERROR_STOP=1 -f -'
    output = service.run_remote_command(command, stdin_text='\n'.join(checks), cancel_event=cancel_event)
    lines = [line for line in output.splitlines() if line.strip()]
    marks = [json.loads(line) for line in lines if line.startswith('{') and 'rc5_mark' in json.loads(line)]
    lines = [line for line in lines if not (line.startswith('{') and 'rc5_mark' in json.loads(line))]
    service._control_metadata_ready = True
    if batch_catalog:
        # Six dependency results plus one function result precede the catalog.
        offset = len(prepared['destinations']) + 1
        cpu, catalog = lines[offset:offset+2]
        service._native_prepared_catalog = (database, cpu, json.loads(catalog), None)
        service._native_prepare_marks = marks
        lines = lines[:offset] + lines[offset+2:]
    results = [json.loads(line) for line in lines]
    if len(results) != len(prepared['destinations']) + 2:
        raise RuntimeError('Unexpected batched preflight response')
    service._data_metadata_ready_databases.add(database)
    for d, metadata in zip(prepared['destinations'], results):
        service._validate_managed_sql_dependencies(database, d['table_name'], {'insert'}, metadata=metadata)
    service._validate_cross_table_expansion_functions(database, prepared['destinations'], metadata=results[-2])
    versions = results[-1]
    missing = [v for v in prepared['raw_schemas'] if v not in {r['raw_schema'] for r in versions}]
    if missing:
        raise RuntimeError('No Raw version is registered for the selected scope: ' + ', '.join(missing) + '.')
    return versions
