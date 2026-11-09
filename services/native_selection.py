"""Probe the installed bulk API before creating any resumable job state."""
import json
import shlex
import uuid

from services.expansion_wal import SESSION_WAL_SQL
from services.expansion_optimizer import build_plan
from services import s2_expansion as s2
from services import s3_expansion as s3
from services import s3_native_expansion as native
from utils import sql_ident, sql_literal


def fallback_reason(reason):
    if 'pgdm_bulk API' in reason or 'pgdm_bulk build mismatch' in reason:
        return reason + '; README section 3: sudo bash release/native_install.sh --pg-config /usr/lib/postgresql/18/bin/pg_config --psql /usr/lib/postgresql/18/bin/psql'
    return reason


def probe_sql(prepared, requested):
    plan = build_plan(prepared, requested, mode='compact')
    if not plan:
        return "SELECT json_build_object('available',false,'reason','Native requires the reviewed official six-table workload');"
    tag = uuid.uuid4().hex
    schemas = ['pgdm_probe_' + tag + suffix for suffix in ('_n', '_s')]
    fields = [('source_rows', 'int8'), ('table1_rows', 'int8'), ('table2_rows', 'int8'),
              ('table5_rows', 'int8'), ('table6_rows', 'int8'), ('observed_codes', '_text'),
              ('decode_ms', 'float8'), ('insert_ms', 'float8'), ('total_ms', 'float8'),
              ('manifest', 'jsonb'), ('setup_ms', 'float8'), ('source_read_ms', 'float8'),
              ('finalize_ms', 'float8')]
    expected_fields = 'ARRAY[' + ','.join(sql_literal(name + ':') + '||' +
                      sql_literal('pg_catalog.' + kind) + '::regtype::oid::text'
                      for name, kind in fields) + ']::text[]'
    scope = ','.join(sql_literal(v) for v in prepared['raw_schemas'])
    setup = []
    for schema in schemas:
        setup.append('CREATE SCHEMA ' + sql_ident(schema) + ';')
        for table in s2.TABLES:
            setup.append(f'CREATE TABLE {s2.relation(schema, table)} (LIKE public.{table} INCLUDING CONSTRAINTS);')
        dimensions = s2.dimension_phase(schema, scope)
        dimensions = dimensions.replace(' ORDER BY raw_id LIMIT 10000', " AND ctid >= '(0,0)'::tid AND ctid < '(1,0)'::tid")
        setup.append(dimensions + "DROP TABLE pg_temp.pgdm_shadow_dimension_keys;")
    job = {'job_id': tag, 'schema_name': schemas[1], 'payload': {'chunk_kind': 'tid'}}
    # The rollback-only oracle needs the original INSERTs, without a worker's
    # post-commit optimization or its procedure-local profiling variables.
    worker = s3.worker_procedure(job, plan, prepared, hint_readback=False)
    expected = worker[worker.index(' CREATE TEMP TABLE pgdm_compact_stage'):worker.index(' INSERT INTO ' + s2.HASH_TABLE)]
    comparisons = []
    for table in ('table1', 'table2', 'table5', 'table6'):
        left, right = (s2.relation(s, table) for s in schemas)
        comparisons.append(f"IF EXISTS((TABLE {left} EXCEPT ALL TABLE {right}) UNION ALL (TABLE {right} EXCEPT ALL TABLE {left})) THEN RAISE EXCEPTION 'Native self-check differs: {table}'; END IF;")
    return f"""SET client_min_messages=warning;
DO $probe$
DECLARE outcome jsonb; c record; n1 bigint; n2 bigint; n5 bigint; n6 bigint; stats record;
 installed text; observed_build text; message text; code text; marks jsonb:='[]';
BEGIN
 BEGIN
  SELECT extversion INTO installed FROM pg_extension WHERE extname='pgdm_bulk' AND extnamespace='public'::regnamespace;
  IF installed IS DISTINCT FROM '1.5' THEN
   RAISE EXCEPTION 'pgdm_bulk API 1.5 required; installed=%',COALESCE(installed,'missing');
  END IF;
  EXECUTE 'SELECT public.pgdm_bulk_build_id()' INTO observed_build;
  IF observed_build IS DISTINCT FROM {sql_literal(native.BULK_BUILD)} THEN
   RAISE EXCEPTION 'pgdm_bulk build mismatch: expected %, installed %',{sql_literal(native.BULK_BUILD)},observed_build;
  END IF;
  IF NOT COALESCE((SELECT prorettype='pg_catalog.int8'::regtype AND NOT proretset
                   AND NOT prosecdef AND proisstrict FROM pg_proc
                   WHERE oid=to_regprocedure('public.pgdm_bulk_hint_pages(regclass,bigint[])')),false)
   OR to_regprocedure('public.pgdm_bulk_chunk(text,bigint,bigint,text[],boolean)') IS NULL
   OR NOT COALESCE((SELECT prorettype=to_regtype('public.pgdm_bulk_result') AND NOT proretset
                   AND NOT prosecdef AND proisstrict FROM pg_proc
                   WHERE oid=to_regprocedure('public.pgdm_bulk_chunk(text,bigint,bigint,text[],boolean)')),false)
   OR (SELECT array_agg(attname::text||':'||atttypid::text ORDER BY attnum) FROM pg_attribute
       WHERE attrelid=(SELECT typrelid FROM pg_type WHERE oid=to_regtype('public.pgdm_bulk_result'))
       AND attnum>0 AND NOT attisdropped) IS DISTINCT FROM {expected_fields}
   OR EXISTS(SELECT FROM pg_attribute WHERE attrelid=(SELECT typrelid FROM pg_type WHERE oid=to_regtype('public.pgdm_bulk_result'))
             AND attnum>0 AND (attisdropped OR atttypmod<>-1)) THEN
   RAISE EXCEPTION 'pgdm_bulk chunk API/result shape mismatch: expected fields %, observed fields %, function %',
    {expected_fields},
    (SELECT jsonb_agg(jsonb_build_object('name',attname,'type',format_type(atttypid,atttypmod),'dropped',attisdropped) ORDER BY attnum)
     FROM pg_attribute WHERE attrelid=(SELECT typrelid FROM pg_type WHERE oid=to_regtype('public.pgdm_bulk_result')) AND attnum>0),
    (SELECT jsonb_build_object('return_type',prorettype::regtype::text,'set',proretset,'security_definer',prosecdef,'strict',proisstrict)
     FROM pg_proc WHERE oid=to_regprocedure('public.pgdm_bulk_chunk(text,bigint,bigint,text[],boolean)'));
  END IF;
  marks:=marks||jsonb_build_object('mark','api_checked','at',clock_timestamp());
  SELECT 0::bigint lo,0::bigint hi INTO c;
  {chr(10).join(setup)}
  marks:=marks||jsonb_build_object('mark','probe_tables_created','at',clock_timestamp());
  SELECT * INTO stats FROM public.pgdm_bulk_chunk({sql_literal(schemas[0])},0,0,ARRAY[{scope}]::text[],true);
  PERFORM public.pgdm_bulk_hint_pages({sql_literal(schemas[0]+'.table1')}::regclass,ARRAY[]::bigint[]);
  marks:=marks||jsonb_build_object('mark','native_page_done','at',clock_timestamp());
  {expected}
  marks:=marks||jsonb_build_object('mark','sql_page_done','at',clock_timestamp());
  {chr(10).join(comparisons)}
  IF stats.source_rows IS DISTINCT FROM (SELECT count(*) FROM public.raw_data WHERE raw_schema IN ({scope}) AND ctid >= '(0,0)'::tid AND ctid < '(1,0)'::tid)
   OR stats.source_rows IS DISTINCT FROM (SELECT COALESCE(sum((m->>'row_count')::bigint),0) FROM jsonb_array_elements(stats.manifest) m) THEN
   RAISE EXCEPTION 'Native self-check source/manifest mismatch';
  END IF;
  outcome:=jsonb_build_object('marks',marks||jsonb_build_object('mark','comparison_done','at',clock_timestamp()),'available',true,'build_id',observed_build,'api',installed,'self_check','first heap page: four tables EXCEPT ALL including IDs and manifest count','source_rows',stats.source_rows);
  -- Roll back every probe object and write, including successful output.
  RAISE SQLSTATE 'PZ001';
 EXCEPTION WHEN SQLSTATE 'PZ001' THEN NULL;
 WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS message=MESSAGE_TEXT,code=RETURNED_SQLSTATE;
  outcome:=jsonb_build_object('available',false,'reason','Native preflight ['||code||']: '||message);
 END;
 PERFORM set_config('pgdm.native_probe',outcome::text,false);
END $probe$;
SELECT current_setting('pgdm.native_probe');"""


def select_native(service, database, prepared, requested, report):
    cached = getattr(service, '_native_prepared_catalog', None)
    if cached and cached[0] == database and not cached[2].get('jobs') and build_plan(prepared, requested, mode='compact'):
        if 'native_bulk_api' in cached[2] and cached[2]['native_bulk_api'] is None:
            service._native_probe_deferred = False
            result = {'available': False, 'reason': 'Native preflight [P0001]: pgdm_bulk API 1.5 required; installed=missing',
                      'catalog_checked_before_job': True}
            report['native_selection'] = result
            report['fallback_reason'] = fallback_reason(result['reason'])
            return False
        service._native_probe_deferred = True
        service._deferred_native_probe_sql = probe_sql(prepared, requested)
        service._native_prepared_catalog = (*cached[:3], native.BULK_BUILD)
        report['native_prepare_marks'] = getattr(service, '_native_prepare_marks', [])
        return True
    service._native_probe_deferred = False
    command = f'psql -h localhost -U {shlex.quote(service.sql_username)} -d {shlex.quote(database)} -XqAt -v ON_ERROR_STOP=1 -f -'
    try:
        sql='BEGIN;\n'+probe_sql(prepared, requested)
        if build_plan(prepared, requested, mode='compact'):
            sql+='\n'+s2.preparation_sql(prepared,s3.EXTRA_INFRASTRUCTURE)
        sql += '\nCOMMIT;\n'
        with service.expansion_timing_scope(report, 'prepare', 'Check installed native build, API and rollback-only SQL equivalence'):
            output=service.run_remote_command(command, stdin_text=SESSION_WAL_SQL + sql)
        lines=output.strip().split('\n',2)
        result=json.loads(lines[0])
        if len(lines)==3:
            service._native_prepared_catalog=(database,lines[1],json.loads(lines[2]),result.get('build_id'))
    except (RuntimeError, ValueError) as exc:
        result = {'available': False, 'reason': 'Native preflight unavailable: ' + str(exc)}
    report['native_selection'] = result
    if not result['available']:
        report['fallback_reason'] = fallback_reason(result['reason'])
    return result['available']
