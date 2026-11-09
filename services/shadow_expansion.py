"""Logged bounded chunks with an atomic six-table shadow publication."""
import hashlib
import json
import os
import re
import shlex
import uuid
from pathlib import Path

from services.expansion_optimizer import build_plan
from services.expansion_recovery import fingerprint, remote_sql, restore_report
from utils import sql_ident, sql_literal

TABLES = ('table1', 'table2', 'table3', 'table4', 'table5', 'table6')
PLAN_VERSION = 'shadow-chunks-v2'
JOB_TABLE = 'pgdm_runtime.shadow_jobs'
CHUNK_TABLE = 'pgdm_runtime.shadow_chunks'
HASH_TABLE = 'pgdm_runtime.shadow_hashes'
FENCE = 'pgdm_shadow_source_fence'


class UnsupportedShadowError(RuntimeError):
    pass

INFRASTRUCTURE = """
SET client_min_messages=warning;
CREATE SCHEMA IF NOT EXISTS pgdm_runtime;
CREATE TABLE IF NOT EXISTS pgdm_runtime.shadow_jobs(
 job_id text PRIMARY KEY, signature text NOT NULL, source_oid oid NOT NULL,
 state text NOT NULL, schema_name text NOT NULL, payload jsonb NOT NULL,
 source_identity jsonb NOT NULL, target_identity jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 load_seconds double precision NOT NULL DEFAULT 0,
 attempts integer NOT NULL DEFAULT 0, published_at timestamptz
);
CREATE UNIQUE INDEX IF NOT EXISTS shadow_active_source
 ON pgdm_runtime.shadow_jobs(source_oid) WHERE state IN ('loading','indexing','data_ready');
CREATE TABLE IF NOT EXISTS pgdm_runtime.shadow_chunks(
 job_id text NOT NULL REFERENCES pgdm_runtime.shadow_jobs(job_id),
 chunk_id integer NOT NULL, lo bigint NOT NULL, hi bigint NOT NULL,
 state text NOT NULL DEFAULT 'pending', row_counts jsonb, source_rows bigint,
 backend_pid integer, started_at timestamptz, committed_at timestamptz,
 stage_bytes bigint, PRIMARY KEY(job_id,chunk_id)
);
CREATE TABLE IF NOT EXISTS pgdm_runtime.shadow_hashes(
 job_id text NOT NULL, chunk_id integer NOT NULL,
 raw_schema text,raw_hash text,row_count bigint NOT NULL
);
DO $install_source_fence$ BEGIN
 IF to_regprocedure('pgdm_runtime.reject_shadow_source_write()') IS NULL
 OR NOT EXISTS(SELECT FROM pgdm_runtime.shadow_jobs
  WHERE state IN ('loading','indexing','data_ready') AND payload ? 'raw_fk_proof') THEN
 EXECUTE $definition$CREATE OR REPLACE FUNCTION pgdm_runtime.reject_shadow_source_write() RETURNS trigger
 LANGUAGE plpgsql AS $fence$
 BEGIN RAISE EXCEPTION 'Source is frozen by a resumable shadow expansion'; END $fence$;$definition$;
 END IF;
END $install_source_fence$;
SELECT true;
"""

CATALOG = """
SET search_path=pg_catalog;
SELECT json_build_object(
 'database_oid',(SELECT oid FROM pg_database WHERE datname=current_database()),
 'source',json_build_object('oid',r.oid,'filenode',r.relfilenode,
  'columns',(SELECT json_agg(json_build_object('name',attname,'type',format_type(atttypid,atttypmod),'notnull',attnotnull) ORDER BY attnum)
   FROM pg_attribute WHERE attrelid=r.oid AND attnum>0 AND NOT attisdropped)),
 'fenced',EXISTS(SELECT FROM pg_trigger WHERE tgrelid=r.oid AND tgname='pgdm_shadow_source_fence' AND tgenabled='O'),
 'targets',(SELECT json_agg(json_build_object('name',c.relname,'oid',c.oid,'kind',c.relkind,
   'comment',obj_description(c.oid,'pg_class'),'owner',pg_get_userbyid(c.relowner),'acl',c.relacl,'rls',c.relrowsecurity,
   'columns',(SELECT json_agg(json_build_object('name',a.attname,'type',format_type(a.atttypid,a.atttypmod),'notnull',a.attnotnull,'identity',a.attidentity) ORDER BY a.attnum) FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped)) ORDER BY c.relname)
   FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public'
   AND c.relname IN ('table1','table2','table3','table4','table5','table6')),
 'constraints',(SELECT json_agg(json_build_object('table',c.relname,'name',k.conname,'type',k.contype,'index_name',(SELECT relname FROM pg_class WHERE oid=k.conindid),'index_definition',CASE WHEN k.contype IN ('p','u') THEN pg_get_indexdef(k.conindid) END,'definition',pg_get_constraintdef(k.oid),'referenced_oid',k.confrelid,'reference_table',(SELECT relname FROM pg_class WHERE oid=k.confrelid),'reference_schema',(SELECT n.nspname FROM pg_class f JOIN pg_namespace n ON n.oid=f.relnamespace WHERE f.oid=k.confrelid)) ORDER BY c.relname,k.conname)
  FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace
  WHERE n.nspname='public' AND c.relname IN ('table1','table2','table3','table4','table5','table6')),
 'indexes',(SELECT json_agg(json_build_object('table',c.relname,'name',(SELECT relname FROM pg_class WHERE oid=i.indexrelid),'definition',pg_get_indexdef(i.indexrelid)))
  FROM pg_index i JOIN pg_class c ON c.oid=i.indrelid JOIN pg_namespace n ON n.oid=c.relnamespace
  WHERE n.nspname='public' AND c.relname IN ('table1','table2','table3','table4','table5','table6')
  AND NOT EXISTS(SELECT FROM pg_constraint k WHERE k.conindid=i.indexrelid)),
 'external',(SELECT count(*) FROM pg_constraint k WHERE k.contype='f'
   AND k.confrelid IN (SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relname IN ('table1','table2','table3','table4','table5','table6'))
   AND k.conrelid NOT IN (SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relname IN ('table1','table2','table3','table4','table5','table6')))
  +(SELECT count(*) FROM pg_depend d JOIN pg_rewrite w ON d.classid='pg_rewrite'::regclass AND d.objid=w.oid
   WHERE d.refclassid='pg_class'::regclass AND d.refobjid IN
   (SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relname IN ('table1','table2','table3','table4','table5','table6')))
  +(SELECT count(*) FROM pg_depend d WHERE d.classid='pg_proc'::regclass AND d.refclassid='pg_type'::regclass AND d.refobjid IN
   (SELECT c.reltype FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relname IN ('table1','table2','table3','table4','table5','table6')))
) FROM pg_class r WHERE r.oid='public.raw_data'::regclass;
"""


def query(service, database, sql, report, name, stage='execute', include=True):
    try:
        with service.expansion_timing_scope(report, stage, name, record_local_cpu=False, remote_include_in_total=include):
            return remote_sql(service, database, sql)
    except RuntimeError as exc:
        if report is not None and 'Publication identity changed:' in str(exc):
            report['publication_identity_error'] = str(exc)
            for item in reversed(report.get('items', [])):
                if item['name'] == name:
                    item['details'] = (item.get('details') or '') + '\n' + str(exc)
                    break
        raise


def read_json(service, database, sql, report, name, stage='prepare'):
    output = query(service, database, sql, report, name, stage)
    try:
        return json.loads(output) if output.strip() else None
    except json.JSONDecodeError as exc:
        raise RuntimeError(f'{name}: invalid JSON response {output[:700]!r}') from exc


def target_ids(catalog):
    return {t['name']: t['oid'] for t in catalog['targets']}


def target_shapes(catalog):
    shapes = {t['name']:{key:value for key,value in t.items() if key!='oid'} for t in catalog['targets']}
    for name, shape in shapes.items():
        shape['constraints'] = sorted(
            (k['name'], k['type'], k['definition'])
            for k in catalog.get('constraints') or [] if k['table'] == name)
    return shapes


def validate_catalog(catalog):
    """Reject unsupported constraints before creating a resumable generation."""
    for constraint in catalog.get('constraints') or []:
        if constraint['type'] not in ('c', 'n', 'p', 'u', 'f'):
            raise UnsupportedShadowError(
                f"Unsupported constraint type {constraint['type']!r}: "
                f"{constraint['table']}.{constraint['name']}")


def relation(schema, table):
    return f'{sql_ident(schema)}.{sql_ident(table)}'


def replay_children(prepared):
    for d in prepared['destinations']:
        name = d['pure_table_name']
        if name not in ('table2', 'table5', 'table6'):
            continue
        original = d['sql_recipe']
        expression = '(source_row.raw_id-1)*14+position_data.position_number' if name == 'table2' else 'source_row.raw_id'
        changed = original.replace(f'INSERT INTO public.{name} (', f'INSERT INTO public.{name} (id_column_1,', 1)
        changed = changed.replace('\nSELECT\n', f'\nSELECT\n    {expression},\n', 1)
        if changed == original:
            raise RuntimeError('Independent child replay recipe did not match')
        d['original_sql_recipe'] = original
        d['sql_recipe'] = changed + f"\nSELECT setval(pg_get_serial_sequence('public.{name}','id_column_1'),COALESCE((SELECT MAX(id_column_1) FROM public.{name}),0)+1,false);"
        d['destination_columns'] = ['id_column_1'] + d['destination_columns']


def replay_dimensions(prepared):
    d = next(item for item in prepared['destinations'] if item['pure_table_name']=='table3')
    original = d['statements'][0]
    header, projection = original.split('\nSELECT DISTINCT\n',1)
    header = header.replace('INSERT INTO public.table3 (','INSERT INTO public.table3 (id_column_1,',1)
    query = 'SELECT DISTINCT\n'+projection.rstrip(';')
    d['original_sql_recipe']=d['sql_recipe']
    d['sql_recipe']=header+'\nSELECT row_number() OVER (ORDER BY v.ref COLLATE "C",v.cleft COLLATE "C",v.cright COLLATE "C"),v.*\nFROM ('+query+') AS v(ref,label,unused,cleft,cright);'
    d['sql_recipe']+="\nSELECT setval(pg_get_serial_sequence('public.table3','id_column_1'),COALESCE((SELECT MAX(id_column_1) FROM public.table3),0)+1,false);"
    d['destination_columns']=['id_column_1']+d['destination_columns']


def dimension_phase(schema, scope):
    return f"""
CREATE TEMP TABLE pgdm_shadow_dimension_keys ON COMMIT DROP AS
SELECT DISTINCT BTRIM(r.raw->'values'->>'text_column_1') AS ref,
 UPPER(BTRIM(r.raw->'values'->>'text_column_2')) AS code2,
 UPPER(BTRIM(r.raw->'values'->>'text_column_3')) AS code3
FROM public.raw_data r WHERE r.raw_schema IN ({scope});
INSERT INTO {relation(schema,'table3')}(id_column_1,text_column_1,text_column_2,numeric_column_1,text_column_3,text_column_4)
SELECT row_number() OVER (ORDER BY ref COLLATE "C",LEFT(code2,1) COLLATE "C",RIGHT(code2,1) COLLATE "C"),
 ref,'SYNTHETIC/REFERENCE',NULL::numeric,LEFT(code2,1),RIGHT(code2,1)
FROM (SELECT DISTINCT ref,code2 FROM pg_temp.pgdm_shadow_dimension_keys WHERE ref<>'' AND code2 ~ '^[A-Z]{{2}}$') d;
INSERT INTO {relation(schema,'table4')}(id_column_1,text_column_1,text_column_2)
SELECT DISTINCT ((ASCII(LEFT(code3,1))-65)*26+ASCII(RIGHT(code3,1))-65+1)::smallint,
 'SYNTHETIC/GENERIC',code3 FROM pg_temp.pgdm_shadow_dimension_keys WHERE code3 ~ '^[A-Z]{{2}}$';
"""


def worker_procedure(job, plan, prepared):
    job_id, schema = job['job_id'], job['schema_name']
    scope = ','.join(sql_literal(v) for v in prepared['raw_schemas'])
    rewrite = lambda text: re.sub(r'\bpublic\.(table[1-6])\b', lambda m: relation(schema, m.group(1)), text)
    inserts = {name: rewrite(sql) for name, sql in plan['canonical_inserts'].items()}
    parent = f"WITH inserted AS ({inserts['table1']} RETURNING id_column_3) INSERT INTO pg_temp.pgdm_parent_ids SELECT id_column_3 FROM inserted"
    d2 = inserts['table2'].replace(f'INSERT INTO {relation(schema,"table2")}(', f'INSERT INTO {relation(schema,"table2")}(id_column_1,', 1)
    d2 = d2.replace('SELECT s.raw_id,p.pos', 'SELECT (s.raw_id-1)*14+p.pos,s.raw_id,p.pos', 1)
    d2 = d2.replace('FROM pg_temp.pgdm_compact_stage s', 'FROM pg_temp.pgdm_compact_stage s JOIN pg_temp.pgdm_parent_ids m USING(raw_id)', 1).replace('WHERE s.parent_ok', '')
    d5 = f"""INSERT INTO {relation(schema,'table5')}(id_column_1,numeric_column_1,numeric_column_2,integer_column_1,integer_column_2,id_column_2,id_column_3)
SELECT s.raw_id,ROUND(s.speed,1),NULL::numeric,NULL::integer,NULL::integer,s.raw_id,NULL::bigint
FROM pg_temp.pgdm_compact_stage s JOIN pg_temp.pgdm_parent_ids m USING(raw_id) WHERE s.speed>0"""
    d6 = inserts['table6'].replace(f'INSERT INTO {relation(schema,"table6")}(', f'INSERT INTO {relation(schema,"table6")}(id_column_1,', 1)
    d6 = d6.replace('SELECT s.raw_id,d.id_column_1', 'SELECT s.raw_id,s.raw_id,d.id_column_1', 1)
    d6 = d6.replace('FROM pg_temp.pgdm_compact_stage s', 'FROM pg_temp.pgdm_compact_stage s JOIN pg_temp.pgdm_parent_ids m USING(raw_id)', 1).replace('WHERE s.parent_ok', '')
    select = plan['compact_select'] + ' AND r.raw_id BETWEEN c.lo AND c.hi'
    return f"""
CREATE OR REPLACE PROCEDURE pgdm_runtime.worker_{job_id}() LANGUAGE plpgsql AS $worker_{job_id}$
DECLARE c record; n1 bigint;n2 bigint;n3 bigint;n4 bigint;n5 bigint;n6 bigint; stage_size bigint;
BEGIN
 PERFORM set_config('application_name',{sql_literal('pgdm_shadow:'+job_id)},false);
 LOOP
  SELECT * INTO c FROM {CHUNK_TABLE} WHERE job_id={sql_literal(job_id)} AND state='pending'
  ORDER BY chunk_id FOR UPDATE SKIP LOCKED LIMIT 1;
  IF NOT FOUND THEN RETURN; END IF;
  UPDATE {CHUNK_TABLE} SET backend_pid=pg_backend_pid(),started_at=clock_timestamp()
  WHERE job_id=c.job_id AND chunk_id=c.chunk_id;
  CREATE TEMP TABLE pgdm_compact_stage ON COMMIT DROP AS {select};
  ANALYZE pg_temp.pgdm_compact_stage;
  CREATE TEMP TABLE pgdm_parent_ids(raw_id bigint) ON COMMIT DROP;
  n3:=0; n4:=0;
  {parent}; GET DIAGNOSTICS n1=ROW_COUNT;
  ANALYZE pg_temp.pgdm_parent_ids;
  {d5}; GET DIAGNOSTICS n5=ROW_COUNT;
  {d6}; GET DIAGNOSTICS n6=ROW_COUNT;
  {d2}; GET DIAGNOSTICS n2=ROW_COUNT;
  INSERT INTO {HASH_TABLE}
  SELECT {sql_literal(job_id)},c.chunk_id,raw_schema,raw_hash,count(*) FROM public.raw_data
  WHERE raw_schema IN ({scope}) AND raw_id BETWEEN c.lo AND c.hi GROUP BY raw_schema,raw_hash;
  stage_size := pg_total_relation_size('pg_temp.pgdm_compact_stage')+pg_total_relation_size('pg_temp.pgdm_parent_ids');
  UPDATE {CHUNK_TABLE} SET state='done',row_counts=jsonb_build_object('table1',n1,'table2',n2,'table3',n3,'table4',n4,'table5',n5,'table6',n6),
   source_rows=(SELECT sum(row_count) FROM {HASH_TABLE} WHERE job_id=c.job_id AND chunk_id=c.chunk_id),
   stage_bytes=stage_size,committed_at=clock_timestamp()
  WHERE job_id=c.job_id AND chunk_id=c.chunk_id;
  COMMIT;
 END LOOP;
END $worker_{job_id}$;
SELECT true;
"""


def initialize_job(service, database, job_id, signature, catalog, payload, chunk_rows, report):
    schema = 'pgdm_shadow_' + job_id
    target_list = ','.join('public.' + t for t in TABLES)
    target_check = '\n'.join(f"IF EXISTS(SELECT FROM public.{t} LIMIT 1) THEN RAISE EXCEPTION 'Shadow requires empty public.{t}'; END IF;" for t in TABLES)
    clones = '\n'.join(f'CREATE TABLE {relation(schema,t)} (LIKE public.{t} INCLUDING DEFAULTS INCLUDING IDENTITY INCLUDING CONSTRAINTS INCLUDING STORAGE INCLUDING COMMENTS);\nALTER TABLE {relation(schema,t)} OWNER TO {sql_ident(next(x["owner"] for x in catalog["targets"] if x["name"]==t))};' for t in TABLES)
    scope = ','.join(sql_literal(v) for v in payload['raw_schemas'])
    dimension_sql = dimension_phase(schema,scope)
    sql = f"""
SET client_min_messages=warning;
BEGIN;
SELECT pg_advisory_xact_lock(hashtextextended('pgdm-shadow-initialize',0));
LOCK TABLE public.raw_data IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE {target_list} IN SHARE ROW EXCLUSIVE MODE;
DO $empty$ BEGIN
 IF 'public.raw_data'::regclass::oid <> {catalog['source']['oid']} OR pg_relation_filenode('public.raw_data') <> {catalog['source']['filenode']} THEN RAISE EXCEPTION 'Source changed during initialization'; END IF;
 {target_check} END $empty$;
CREATE SCHEMA {sql_ident(schema)};
{clones}
{dimension_sql}
INSERT INTO {JOB_TABLE}(job_id,signature,source_oid,state,schema_name,payload,source_identity,target_identity)
VALUES({sql_literal(job_id)},{sql_literal(signature)},{catalog['source']['oid']},'loading',{sql_literal(schema)},
 {sql_literal(json.dumps(payload))}::jsonb,{sql_literal(json.dumps(catalog['source']))}::jsonb,{sql_literal(json.dumps(target_ids(catalog)))}::jsonb);
UPDATE {JOB_TABLE} SET payload=payload || jsonb_build_object('dimension_rows',jsonb_build_object('table3',(SELECT count(*) FROM {relation(schema,'table3')}),'table4',(SELECT count(*) FROM {relation(schema,'table4')})),'shadow_identity',(SELECT jsonb_object_agg(c.relname,c.oid) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname={sql_literal(schema)} AND c.relname IN ('table1','table2','table3','table4','table5','table6'))) WHERE job_id={sql_literal(job_id)};
INSERT INTO {CHUNK_TABLE}(job_id,chunk_id,lo,hi)
SELECT {sql_literal(job_id)},(row_number() OVER (ORDER BY ranges.lo))::integer,ranges.lo,LEAST(bounds.hi,ranges.lo+{chunk_rows}-1)
FROM (SELECT min(raw_id) AS lo,max(raw_id) AS hi FROM public.raw_data WHERE raw_schema IN ({scope})) bounds
CROSS JOIN LATERAL generate_series(bounds.lo,bounds.hi,{chunk_rows}::bigint) ranges(lo);
CREATE TRIGGER {FENCE} BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.raw_data
FOR EACH STATEMENT EXECUTE FUNCTION pgdm_runtime.reject_shadow_source_write();
COMMIT;
SELECT row_to_json(j)::text FROM {JOB_TABLE} j WHERE job_id={sql_literal(job_id)};
"""
    return read_json(service, database, sql, report, 'Create logged shadows, durable chunks and persistent source fence')


def load_chunks(service, database, job, workers, stop_after, report):
    script = Path(__file__).with_name('shadow_runner.sh').read_text(encoding='utf-8')
    arguments = [database, service.sql_username, job['job_id'], str(workers), str(stop_after)]
    command = 'bash -s -- ' + ' '.join(shlex.quote(value) for value in arguments)
    with service.expansion_timing_scope(report, 'execute', 'Parallel logged chunk transformations and durable commits', record_local_cpu=False):
        output = service.run_remote_command(command, stdin_text=script)
    try:
        return json.loads(output)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f'Chunk runner: invalid JSON response {output[:700]!r}') from exc


def build_publish_sql(service, prepared, job, catalog, report):
    job_id, schema, payload = job['job_id'], job['schema_name'], job['payload']
    original_catalog = payload['catalog']
    validate_catalog(original_catalog)
    constraints = []
    for k in original_catalog['constraints']:
        if k['type'] in ('c', 'n'):
            continue
        definition = k['definition']
        if k['type']=='f':
            parent = next((name for name,oid in job['target_identity'].items() if oid==k['referenced_oid']),None)
            target = relation(schema,parent) if parent else relation(k['reference_schema'],k['reference_table'])
            prefix,tail=definition.split('REFERENCES ',1)
            definition=prefix+'REFERENCES '+target+'('+tail.split('(',1)[1]
        constraints.append((k['type']=='f',f'ALTER TABLE {relation(schema,k["table"])} ADD CONSTRAINT {sql_ident(k["name"])} {definition};'))
    constraint_sql = '\n'.join(text for _,text in sorted(constraints,key=lambda x:x[0]))
    indexes = '\n'.join(re.sub(r'\bON public\.(table[1-6])\b', lambda m:'ON '+relation(schema,m.group(1)), i['definition'])+';' for i in original_catalog.get('indexes') or [])
    sequence_statements = '\n'.join(f"PERFORM setval(pg_get_serial_sequence({sql_literal(schema+'.'+t)},'id_column_1'),COALESCE((SELECT MAX(id_column_1) FROM {relation(schema,t)}),0)+1,false);" for t in TABLES if t!='table4')
    sequences = 'DO $sequences$ BEGIN '+sequence_statements+' END $sequences$;'
    counter_statements = []
    for table in TABLES:
        if table in ('table3','table4'):
            value = str(payload['dimension_rows'][table])
        else:
            value = f"(SELECT COALESCE(sum((row_counts->>{sql_literal(table)})::bigint),0) FROM {CHUNK_TABLE} WHERE job_id={sql_literal(job_id)} AND state='done')"
        counter_statements.append(f"UPDATE public.pgdm_table_row_counts SET row_count={value},updated_at=clock_timestamp() WHERE schema_name='public' AND table_name={sql_literal(table)};")
    counters = '\n'.join(counter_statements)
    # The original manifest formatter still supplies all source column metadata.
    manifest = service._build_expansion_manifest_sql('public','raw_data',prepared['raw_schemas'])
    manifest = manifest.replace('COUNT(*)::bigint AS row_count','SUM(row_count)::bigint AS row_count',1)
    manifest = manifest.replace('FROM "public"."raw_data"\nWHERE',f'FROM {HASH_TABLE}\nWHERE job_id={sql_literal(job_id)} AND',1)
    manifest_query = 'SELECT json_build_object(' + manifest.split('SELECT json_build_object(',1)[1].strip().rstrip(';')
    manifest = manifest.split('SELECT json_build_object(',1)[0]
    wire = {k:payload[k] for k in ('source_database_name','source_schema_name','source_table_name','raw_schemas','source_versions','destinations')}
    wire.update(expansion_job_id=job_id,shadow_job_id=job_id)
    shadow_identity = sql_literal(json.dumps(payload['shadow_identity']))
    serial_option = "SET LOCAL max_parallel_maintenance_workers=0;" if os.getenv('PDM_EXPANSION_MODE') == 'shadow_serial' else ''
    sql = f"""
SET client_min_messages=warning;
BEGIN;
SELECT pg_advisory_xact_lock(hashtextextended({sql_literal('pgdm-shadow:'+job_id)},0));
LOCK TABLE public.raw_data IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE {','.join('public.'+t for t in TABLES)} IN ACCESS EXCLUSIVE MODE;
DO $ready$
BEGIN
 IF EXISTS(SELECT FROM {CHUNK_TABLE} WHERE job_id={sql_literal(job_id)} AND state<>'done') THEN RAISE EXCEPTION 'Unfinished chunks cannot be published'; END IF;
 IF (SELECT jsonb_object_agg(c.relname,c.oid) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname={sql_literal(schema)} AND c.relname IN ('table1','table2','table3','table4','table5','table6')) <> {shadow_identity}::jsonb THEN RAISE EXCEPTION 'Shadow table identity changed'; END IF;
 {' '.join(f"IF EXISTS(SELECT FROM public.{t} LIMIT 1) THEN RAISE EXCEPTION 'Destination changed during shadow load'; END IF;" for t in TABLES)}
END $ready$;
SET LOCAL work_mem='512MB'; SET LOCAL maintenance_work_mem='2GB';
{serial_option}
{constraint_sql}
{indexes}
{sequences}
{manifest}
DROP TABLE {','.join('public.'+t for t in TABLES)} RESTRICT;
{''.join(f'ALTER TABLE {relation(schema,t)} SET SCHEMA public;' for t in TABLES)}
{counters}
CREATE TABLE IF NOT EXISTS pgdm_runtime.expansion_jobs(
 job_id text PRIMARY KEY,fingerprint text NOT NULL,state text NOT NULL,payload jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),completed_at timestamptz);
INSERT INTO pgdm_runtime.expansion_jobs(job_id,fingerprint,state,payload)
SELECT {sql_literal(job_id)},{sql_literal(payload['base_signature'])},'data_ready',
 {sql_literal(json.dumps(wire))}::jsonb || jsonb_build_object('dependency_manifest',({manifest_query})::jsonb);
UPDATE {JOB_TABLE} SET state='data_ready',published_at=clock_timestamp() WHERE job_id={sql_literal(job_id)};
DROP SCHEMA {sql_ident(schema)};
COMMIT;
SELECT payload::text FROM pgdm_runtime.expansion_jobs WHERE job_id={sql_literal(job_id)};
"""
    return sql


def execute_shadow(service, database, prepared, requested_destinations, source_versions, report, progress_callback=None, post_commit_callback=None):
    service._last_shadow_report = report
    mode = os.getenv('PDM_EXPANSION_MODE', 'auto')
    service._record_expansion_engine(report, 'shadow_serial' if mode == 'shadow_serial' else ('S2' if mode == 's2' else 'S3'))
    with service.expansion_timing_scope(report, 'prepare', 'Plan and coordinate resumable shadow expansion', remote_include_in_total=False):
        if os.getenv('PDM_EXPANSION_MODE','auto') != 'shadow_serial':
            if mode == 's2':
                from services.s2_expansion import execute
            else:
                from services.s3_expansion import execute
            return execute(service, database, prepared, requested_destinations, source_versions, report, progress_callback, post_commit_callback)
        return _execute_shadow(service, database, prepared, requested_destinations, source_versions, report, progress_callback, post_commit_callback)


def _execute_shadow(service, database, prepared, requested_destinations, source_versions, report, progress_callback=None, post_commit_callback=None):
    plan = build_plan(prepared, requested_destinations, mode='parallel8')
    if not plan:
        raise UnsupportedShadowError('Shadow mode requires the reviewed official six-table workload')
    replay_children(prepared)
    replay_dimensions(prepared)
    query(service,database,INFRASTRUCTURE,report,'Ensure durable chunk infrastructure','prepare')
    catalog = read_json(service,database,CATALOG,report,'Read source and destination identity')
    validate_catalog(catalog)
    if len(catalog['targets']) != 6 or any(t['kind']!='r' or t['acl'] is not None or t['rls'] for t in catalog['targets']) or catalog['external']:
        if catalog['fenced']:
            raise RuntimeError('An active shadow generation has incompatible destination dependencies')
        raise UnsupportedShadowError('Shadow publish requires isolated ordinary default-ACL destinations')
    base_signature = fingerprint(prepared,source_versions)
    stable_plan = {'worker':worker_procedure({'job_id':'fingerprint','schema_name':'pgdm_shadow_template'},plan,prepared),'dimensions':dimension_phase('pgdm_shadow_template',','.join(sql_literal(v) for v in prepared['raw_schemas'])),'recipes':[d['sql_recipe'] for d in prepared['destinations']], 'version':PLAN_VERSION}
    signature = hashlib.sha256(json.dumps({'source':catalog['source'],'database_oid':catalog['database_oid'],'logical':base_signature,'plan':stable_plan},sort_keys=True).encode()).hexdigest()
    job = read_json(service,database,f"SELECT row_to_json(j)::text FROM {JOB_TABLE} j WHERE signature={sql_literal(signature)} AND state IN ('loading','indexing','data_ready') ORDER BY created_at DESC LIMIT 1;",report,'Find resumable shadow generation')
    reused = 0
    resuming = bool(job)
    if job:
        if job['source_identity'] != catalog['source'] or not catalog['fenced']:
            raise RuntimeError('Source identity or persistent write fence changed')
        if target_shapes(job['payload']['catalog']) != target_shapes(catalog):
            raise RuntimeError('Destination column or ownership definition changed')
        if job['state']=='data_ready':
            if target_ids(catalog) != job['payload']['shadow_identity']:
                raise RuntimeError('Published destination identity changed')
            result=read_json(service,database,f"SELECT payload::text FROM pgdm_runtime.expansion_jobs WHERE job_id={sql_literal(job['job_id'])};",report,'Recover atomically published generation')
            result['timing_report']=report
            result['timing_report']['historical_timing_complete']=False
            counts=read_json(service,database,f"SELECT jsonb_object_agg(t.name,(SELECT COALESCE(sum((row_counts->>t.name)::bigint),0) FROM {CHUNK_TABLE} WHERE job_id={sql_literal(job['job_id'])} AND state='done')) FROM unnest(ARRAY['table1','table2','table3','table4','table5','table6']) t(name);",report,'Recover published row counters')
            counts.update(job['payload']['dimension_rows'])
            report['source_rows']=result['dependency_manifest']['expected_row_count']
            report['destination_rows']=[{'table_name':d['table_name'],'rows':int(counts[d['pure_table_name']])} for d in prepared['destinations']]
            report['total_rows_inserted']=sum(r['rows'] for r in report['destination_rows'])
            service._pending_expansion_job_id=job['job_id']
            if post_commit_callback: post_commit_callback()
            return result
        if job['target_identity'] != target_ids(catalog):
            raise RuntimeError('Original destination identity changed during interrupted load')
        reused=read_json(service,database,f"SELECT count(*) FROM {CHUNK_TABLE} WHERE job_id={sql_literal(job['job_id'])} AND state='done';",report,'Count reused committed chunks')
    else:
        if catalog['fenced']:
            raise RuntimeError('Another resumable generation owns the source fence')
        job_id=uuid.uuid4().hex
        payload={'source_database_name':database,'source_schema_name':'public','source_table_name':'raw_data','raw_schemas':prepared['raw_schemas'],'source_versions':source_versions,'destinations':prepared['destinations'],'catalog':catalog,'base_signature':base_signature}
        chunk_rows=int(os.getenv('PDM_SHADOW_CHUNK_ROWS','250000'))
        if chunk_rows<1: raise ValueError('Chunk size must be positive')
        job=initialize_job(service,database,job_id,signature,catalog,payload,chunk_rows,report)
    query(service,database,worker_procedure(job,plan,prepared),report,'Prepare resumable chunk worker procedure','prepare')
    workers=int(os.getenv('PDM_STAGE_WORKERS','8'))
    if not 1<=workers<=32: raise ValueError('Workers must be between 1 and 32')
    stop_after=int(os.getenv('PDM_SHADOW_STOP_AFTER_CHUNKS','0'))
    service._notify_progress(progress_callback,'execute',10,f'Loading logged chunks; reusing {reused} committed chunks')
    loaded=load_chunks(service,database,job,workers,stop_after,report)
    report['shadow_job_id']=job['job_id']
    reused=max(reused,int(loaded.get('reused_at_launch') or 0))
    report['shadow_chunks_reused']=reused
    report['shadow_chunks_done']=loaded['done']
    report['shadow_chunks_total']=loaded['total']
    report['stage_peak_bytes_bound']=int(loaded.get('max_chunk_bytes') or 0)*workers
    if loaded['done']!=loaded['total']:
        raise RuntimeError(f"CONTROLLED_SHADOW_INTERRUPTION: {loaded['done']}/{loaded['total']} chunks durable; public destinations remain unchanged")
    current=read_json(service,database,CATALOG,report,'Revalidate frozen source and publication identity')
    if current['source']!=job['source_identity'] or target_ids(current)!=job['target_identity'] or not current['fenced']:
        raise RuntimeError('Publication identity changed')
    if target_shapes(current)!=target_shapes(job['payload']['catalog']):
        raise RuntimeError('Destination shape changed during shadow load')
    service._notify_progress(progress_callback,'execute',80,'Building complete integrity and atomically publishing six shadow tables')
    result=read_json(service,database,build_publish_sql(service,prepared,job,catalog,report),report,'Build complete integrity and atomically publish logged shadows')
    loaded['rows'].update(job['payload']['dimension_rows'])
    report['source_rows']=result['dependency_manifest']['expected_row_count']
    report['destination_rows']=[{'table_name':d['table_name'],'rows':int(loaded['rows'][d['pure_table_name']])} for d in prepared['destinations']]
    report['total_rows_inserted']=sum(r['rows'] for r in report['destination_rows'])
    if resuming: report['historical_timing_complete']=False
    result['timing_report']=report
    service._pending_expansion_job_id=job['job_id']
    if post_commit_callback: post_commit_callback()
    return result
