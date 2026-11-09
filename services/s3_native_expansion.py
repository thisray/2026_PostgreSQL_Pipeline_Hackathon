"""Native decoding and same-transaction bulk fanout with durable publication."""
import hashlib
import json
import os
import re
import shlex
import uuid
from pathlib import Path

from services.expansion_wal import SESSION_WAL_SQL, SESSION_WAL_BODY
from services import shadow_expansion as base
from services import raw_fk_proof, shared_fk_proof
from services.s3_expansion import CPU_PROBE, prepare_command
from services import s3_publication_dag as s3_publication
from services.expansion_optimizer import build_plan
from services.expansion_recovery import fingerprint, shadow_transient_cleanup
from services.publication_identity import locked_check
from services.native_build import bulk_build_id
from services.chunk_workers import choose_workers
from utils import sql_ident, sql_literal

TABLES = base.TABLES
JOB_TABLE, CHUNK_TABLE, HASH_TABLE = base.JOB_TABLE, base.CHUNK_TABLE, base.HASH_TABLE
relation, query, read_json = base.relation, base.query, base.read_json
PLAN_VERSION = 'native-durable-bulk-read-ahead-v8-rc7'
NATIVE_BUILD = 's3-native-v2-20261102.3'
BULK_BUILD = bulk_build_id()


def default_chunk_rows(source_rows, workers, native_bulk, hint_readback):
    if native_bulk and source_rows <= 1000000:
        return max(10000, min(250000, (source_rows+workers-1)//workers))
    if native_bulk and source_rows > 1000000 and hint_readback:
        return 62500
    return max(10000, min(250000, (source_rows+workers*4-1)//(workers*4)))


def dimension_phase(schema, scope, full=False):
    # OFFSET 0 preserves one values extraction, GROUP BY permits hash aggregation.
    source = f'SELECT raw FROM public.raw_data WHERE raw_schema IN ({scope})'
    if not full:
        source += ' ORDER BY raw_id LIMIT 10000'
    return f"""
SET LOCAL jit=off;
SET LOCAL enable_sort=off;
CREATE TEMP TABLE pgdm_shadow_dimension_keys ON COMMIT DROP AS
SELECT BTRIM(v.j->>'text_column_1') ref,UPPER(BTRIM(v.j->>'text_column_2')) code2,
 UPPER(BTRIM(v.j->>'text_column_3')) code3
FROM ({source}) r CROSS JOIN LATERAL (SELECT r.raw->'values' j OFFSET 0) v
GROUP BY 1,2,3;
INSERT INTO {relation(schema,'table3')}(id_column_1,text_column_1,text_column_2,numeric_column_1,text_column_3,text_column_4)
SELECT row_number() OVER (ORDER BY ref COLLATE "C",LEFT(code2,1) COLLATE "C",RIGHT(code2,1) COLLATE "C"),
ref,'SYNTHETIC/REFERENCE',NULL::numeric,LEFT(code2,1),RIGHT(code2,1)
FROM (SELECT ref,code2 FROM pg_temp.pgdm_shadow_dimension_keys WHERE ref<>'' AND code2 ~ '^[A-Z]{{2}}$' GROUP BY 1,2) d;
INSERT INTO {relation(schema,'table4')}(id_column_1,text_column_1,text_column_2)
SELECT ((ASCII(LEFT(code3,1))-65)*26+ASCII(RIGHT(code3,1))-65+1)::smallint,'SYNTHETIC/GENERIC',code3
FROM pg_temp.pgdm_shadow_dimension_keys WHERE code3 ~ '^[A-Z]{{2}}$' GROUP BY code3;
SET LOCAL enable_sort=on;
"""


def compact_select(schema, scope, tid):
    bounds = "r.ctid >= format('(%s,0)',c.lo)::tid AND r.ctid < format('(%s,0)',c.hi+1)::tid" if tid else 'r.raw_id BETWEEN c.lo AND c.hi'
    return f"""SELECT r.raw_id,
 (x.d).external_id,(x.d).observed_at,(x.d).max_declared,
 (x.d).n2,(x.d).n1,(x.d).speed,d3.id_column_1 dim3_id,
 CASE WHEN (x.d).code3 ~ '^[A-Z]{{2}}$' THEN
 ((ASCII(LEFT((x.d).code3,1))-65)*26+ASCII(RIGHT((x.d).code3,1))-65+1)::smallint END dim4_id,
 ((x.d).parent_ok IS TRUE AND d3.id_column_1 IS NULL) dim3_miss,
 (x.d).slot_pos,(x.d).slot_weight,(x.d).slot_type,(x.d).slot_gap
FROM public.raw_data r
CROSS JOIN LATERAL (SELECT public.pgdm_decode_row(r.raw_id,r.raw) AS d OFFSET 0) x
LEFT JOIN {relation(schema,'table3')} d3 ON d3.text_column_1=(x.d).ref
 AND d3.text_column_2='SYNTHETIC/REFERENCE' AND d3.numeric_column_1 IS NULL
 AND d3.text_column_3=LEFT((x.d).code2,1) AND d3.text_column_4=RIGHT((x.d).code2,1)
WHERE r.raw_schema IN ({scope}) AND {bounds}"""


def worker_procedure(job, plan, prepared):
    if job.get('payload', {}).get('native_bulk'):
        return bulk_worker_procedure(job, prepared)
    return scalar_worker_procedure(job, plan, prepared)


def bulk_worker_procedure(job, prepared):
    job_id, schema = job['job_id'], job['schema_name']
    scope = ','.join(sql_literal(v) for v in prepared['raw_schemas'])
    tid = job.get('payload', {}).get('chunk_kind', 'tid') == 'tid'
    proof_ledger = (",'_raw_fk_proof',bulk_stats.manifest->'raw_fk_proof'"
                    if job.get('payload', {}).get('raw_fk_proof') else '')
    return f"""
CREATE OR REPLACE PROCEDURE pgdm_runtime.worker_{job_id}() LANGUAGE plpgsql AS $worker$
DECLARE c record;bulk_stats record;tick timestamptz;profile jsonb;manifest_rows bigint;pages jsonb;t text;hint_needed boolean:=current_setting('data_checksums')='on' OR current_setting('wal_log_hints')='on';
BEGIN
 {SESSION_WAL_BODY}
 PERFORM set_config('application_name',{sql_literal('pgdm_shadow:'+job_id)},false);
 LOOP
 SELECT * INTO c FROM {CHUNK_TABLE} WHERE job_id={sql_literal(job_id)} AND state='pending'
 ORDER BY chunk_id FOR UPDATE SKIP LOCKED LIMIT 1;
 IF NOT FOUND THEN RETURN; END IF;
 PERFORM set_config('jit','off',true);
 PERFORM set_config('pgdm_bulk.direct_read_ahead','on',true);
 UPDATE {CHUNK_TABLE} SET backend_pid=pg_backend_pid(),started_at=clock_timestamp() WHERE job_id=c.job_id AND chunk_id=c.chunk_id;
 SELECT * INTO bulk_stats FROM public.pgdm_bulk_chunk({sql_literal(schema)},c.lo,c.hi,ARRAY[{scope}]::text[],{str(tid).lower()});
 {raw_fk_proof.worker_check_sql(job)}
 profile:=jsonb_build_object('bulk_decode_s',bulk_stats.decode_ms/1000.0,'bulk_insert_s',bulk_stats.insert_ms/1000.0,'bulk_total_s',bulk_stats.total_ms/1000.0,
  'bulk_setup_s',bulk_stats.setup_ms/1000.0,'bulk_source_read_s',bulk_stats.source_read_ms/1000.0,'bulk_finalize_s',bulk_stats.finalize_ms/1000.0,
  'source_io',current_setting('pgdm_bulk.last_source_io')::jsonb,
  'decode_counters',current_setting('pgdm_bulk.last_decode_counters')::jsonb);
 tick:=clock_timestamp();
 INSERT INTO {relation(schema,'observed_codes')}(chunk_id,code_id)
 SELECT c.chunk_id,((ASCII(LEFT(code,1))-65)*26+ASCII(RIGHT(code,1))-65+1)::smallint
 FROM unnest(bulk_stats.observed_codes) codes(code);
 profile:=profile || jsonb_build_object('observed_codes_s',EXTRACT(EPOCH FROM clock_timestamp()-tick));
 tick:=clock_timestamp();
 INSERT INTO {HASH_TABLE}(job_id,chunk_id,raw_schema,raw_hash,row_count)
 SELECT {sql_literal(job_id)},c.chunk_id,m.raw_schema,m.raw_hash,m.row_count
 FROM jsonb_to_recordset(CASE WHEN jsonb_typeof(bulk_stats.manifest)='object' THEN bulk_stats.manifest->'groups' ELSE bulk_stats.manifest END) AS m(raw_schema text,raw_hash text,row_count bigint);
 SELECT COALESCE(sum(row_count),0) INTO manifest_rows FROM {HASH_TABLE} WHERE job_id=c.job_id AND chunk_id=c.chunk_id;
 IF manifest_rows IS DISTINCT FROM bulk_stats.source_rows THEN RAISE EXCEPTION 'Native source count and manifest differ'; END IF;
 profile:=profile || jsonb_build_object('manifest_s',EXTRACT(EPOCH FROM clock_timestamp()-tick));
 UPDATE {CHUNK_TABLE} SET state='done',row_counts=jsonb_build_object('table1',bulk_stats.table1_rows,'table2',bulk_stats.table2_rows,'table3',0,'table4',0,'table5',bulk_stats.table5_rows,'table6',bulk_stats.table6_rows,'_profile',profile{proof_ledger}),
 source_rows=bulk_stats.source_rows,stage_bytes=0,committed_at=clock_timestamp()
 WHERE job_id=c.job_id AND chunk_id=c.chunk_id;
 pages:=current_setting('pgdm_bulk.last_written_pages')::jsonb;
 COMMIT;
 IF hint_needed THEN
 FOREACH t IN ARRAY ARRAY['table1','table2','table5','table6'] LOOP
  EXECUTE format('SELECT public.pgdm_bulk_hint_pages(%L::regclass,$1)',{sql_literal(schema)}||'.'||t)
  USING ARRAY(SELECT jsonb_array_elements_text(pages->t)::bigint);
 END LOOP;
 COMMIT;
 END IF;
 END LOOP;
END $worker$;
"""


def scalar_worker_procedure(job, plan, prepared):
    job_id,schema = job['job_id'],job['schema_name']
    scope = ','.join(sql_literal(v) for v in prepared['raw_schemas'])
    tid = job.get('payload',{}).get('chunk_kind','tid')=='tid'
    bounds = "r.ctid >= format('(%s,0)',c.lo)::tid AND r.ctid < format('(%s,0)',c.hi+1)::tid" if tid else 'r.raw_id BETWEEN c.lo AND c.hi'
    parent = plan['canonical_inserts']['table1']
    parent = parent[:parent.index('FROM pg_temp.pgdm_compact_stage')]+'FROM pg_temp.pgdm_compact_stage s WHERE s.dim3_id IS NOT NULL'
    parent = parent.replace('public.table1',relation(schema,'table1')).replace('d.id_column_1','s.dim3_id')
    return f"""
CREATE OR REPLACE PROCEDURE pgdm_runtime.worker_{job_id}() LANGUAGE plpgsql AS $worker$
DECLARE c record;n1 bigint;n2 bigint;n5 bigint;n6 bigint;stage_size bigint;tick timestamptz;profile jsonb;
BEGIN
 {SESSION_WAL_BODY}
 PERFORM set_config('application_name',{sql_literal('pgdm_shadow:'+job_id)},false);
 PERFORM set_config('temp_buffers','128MB',false);
 LOOP
 SELECT * INTO c FROM {CHUNK_TABLE} WHERE job_id={sql_literal(job_id)} AND state='pending'
 ORDER BY chunk_id FOR UPDATE SKIP LOCKED LIMIT 1;
 IF NOT FOUND THEN RETURN; END IF;
 PERFORM set_config('enable_memoize','off',true);
 PERFORM set_config('jit','off',true);
 UPDATE {CHUNK_TABLE} SET backend_pid=pg_backend_pid(),started_at=clock_timestamp() WHERE job_id=c.job_id AND chunk_id=c.chunk_id;
 tick:=clock_timestamp();
 CREATE TEMP TABLE pgdm_compact_stage ON COMMIT DROP AS {compact_select(schema,scope,tid)};
 profile:=jsonb_build_object('decode_stage_s',EXTRACT(EPOCH FROM clock_timestamp()-tick));
 IF EXISTS(SELECT FROM pg_temp.pgdm_compact_stage WHERE dim3_miss) THEN
 RAISE EXCEPTION 'S2_DIMENSION_MISS: full dimensions and deterministic reprocessing required'; END IF;
 tick:=clock_timestamp();
 INSERT INTO {relation(schema,'observed_codes')}(chunk_id,code_id)
 SELECT c.chunk_id,dim4_id FROM pg_temp.pgdm_compact_stage WHERE dim4_id IS NOT NULL GROUP BY dim4_id;
 profile:=profile || jsonb_build_object('observed_codes_s',EXTRACT(EPOCH FROM clock_timestamp()-tick));
 tick:=clock_timestamp();
 {parent}; GET DIAGNOSTICS n1=ROW_COUNT;
 profile:=profile || jsonb_build_object('table1_s',EXTRACT(EPOCH FROM clock_timestamp()-tick));
 tick:=clock_timestamp();
 INSERT INTO {relation(schema,'table5')}(id_column_1,numeric_column_1,numeric_column_2,integer_column_1,integer_column_2,id_column_2,id_column_3)
 SELECT raw_id,ROUND(speed,1),NULL::numeric,NULL::integer,NULL::integer,raw_id,NULL::bigint FROM pg_temp.pgdm_compact_stage WHERE dim3_id IS NOT NULL AND speed>0;
 GET DIAGNOSTICS n5=ROW_COUNT;
 profile:=profile || jsonb_build_object('table5_s',EXTRACT(EPOCH FROM clock_timestamp()-tick));
 tick:=clock_timestamp();
 INSERT INTO {relation(schema,'table6')}(id_column_1,id_column_2,id_column_3)
 SELECT raw_id,raw_id,dim4_id FROM pg_temp.pgdm_compact_stage WHERE dim3_id IS NOT NULL AND dim4_id IS NOT NULL;
 GET DIAGNOSTICS n6=ROW_COUNT;
 profile:=profile || jsonb_build_object('table6_s',EXTRACT(EPOCH FROM clock_timestamp()-tick));
 tick:=clock_timestamp();
 INSERT INTO {relation(schema,'table2')}(id_column_1,id_column_2,integer_column_1,integer_column_2,text_column_1,integer_column_3,integer_column_4,integer_column_5,integer_column_6)
 SELECT (s.raw_id-1)*14+p.pos,s.raw_id,p.pos,p.weight,p.type_code,NULL::integer,NULL::integer,NULL::integer,p.gap
 FROM pg_temp.pgdm_compact_stage s CROSS JOIN LATERAL unnest(s.slot_pos,s.slot_weight,s.slot_type,s.slot_gap) p(pos,weight,type_code,gap)
 WHERE s.dim3_id IS NOT NULL;
 GET DIAGNOSTICS n2=ROW_COUNT;
 profile:=profile || jsonb_build_object('table2_s',EXTRACT(EPOCH FROM clock_timestamp()-tick));
 tick:=clock_timestamp();
 INSERT INTO {HASH_TABLE} SELECT {sql_literal(job_id)},c.chunk_id,r.raw_schema,r.raw_hash,count(*)
 FROM public.raw_data r WHERE r.raw_schema IN ({scope}) AND {bounds} GROUP BY r.raw_schema,r.raw_hash;
 profile:=profile || jsonb_build_object('manifest_s',EXTRACT(EPOCH FROM clock_timestamp()-tick));
 stage_size:=pg_total_relation_size('pg_temp.pgdm_compact_stage');
 UPDATE {CHUNK_TABLE} SET state='done',row_counts=jsonb_build_object('table1',n1,'table2',n2,'table3',0,'table4',0,'table5',n5,'table6',n6,'_profile',profile),
 source_rows=(SELECT sum(row_count) FROM {HASH_TABLE} WHERE job_id=c.job_id AND chunk_id=c.chunk_id),stage_bytes=stage_size,committed_at=clock_timestamp()
 WHERE job_id=c.job_id AND chunk_id=c.chunk_id;
 COMMIT;
 END LOOP;
END $worker$;
"""


def initialize_job(service,database,job_id,signature,catalog,payload,chunk_rows,report,plan):
    schema='pgdm_shadow_'+job_id
    clones=[]
    for t in TABLES:
        original=next(x for x in catalog['targets'] if x['name']==t)
        early_fk = (f", CONSTRAINT {sql_ident(raw_fk_proof.NAME)} {raw_fk_proof.DEFINITION}"
                    if t == 'table1' and payload.get('raw_fk_proof') else '')
        clones.append(f'CREATE TABLE {relation(schema,t)} (LIKE public.{t} INCLUDING DEFAULTS INCLUDING IDENTITY INCLUDING CONSTRAINTS INCLUDING STORAGE INCLUDING COMMENTS{early_fk}) WITH (autovacuum_enabled=false);')
        clones.append(f'ALTER TABLE {relation(schema,t)} OWNER TO {sql_ident(original["owner"])};')
        clones.append(f'COMMENT ON TABLE {relation(schema,t)} IS {sql_literal(original["comment"]) if original["comment"] is not None else "NULL"};')
    scope=','.join(sql_literal(v) for v in payload['raw_schemas'])
    n=max(int(catalog['scope_rows']),1); pages=int(catalog['pages'])
    tid=payload['chunk_kind']=='tid'
    width=max(1,(chunk_rows*pages+n-1)//n) if tid else chunk_rows
    if tid and catalog['scope_count_basis'] and n <= 1000000:
        waves = 1 if payload.get('native_bulk') else 4
        width=max((10000*pages+n-1)//n,min((250000*pages+n-1)//n,(pages+catalog['workers']*waves-1)//(catalog['workers']*waves)))
    bounds=f'SELECT 0::bigint lo,{pages-1}::bigint hi' if tid else f'SELECT min(raw_id) lo,max(raw_id) hi FROM public.raw_data WHERE raw_schema IN ({scope})'
    payload.update(chunk_rows=chunk_rows,chunk_span=width,chunk_units='pages' if tid else 'raw_id',scope_count_basis='page count with density bounds' if tid and catalog['scope_count_basis'] else 'exact scoped rows',workers=catalog['workers'])
    job={'job_id':job_id,'schema_name':schema,'payload':payload}
    sql=f"""SET client_min_messages=warning;
BEGIN;
SELECT pg_advisory_xact_lock(hashtextextended('pgdm-shadow-initialize',0));
LOCK TABLE public.raw_data IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE {','.join('public.'+t for t in TABLES)} IN SHARE ROW EXCLUSIVE MODE;
DO $empty$ BEGIN
 IF pg_relation_filenode('public.raw_data') <> {catalog['source']['filenode']} THEN RAISE EXCEPTION 'Source changed during initialization'; END IF;
 IF pg_relation_size('public.raw_data')/current_setting('block_size')::bigint > {pages} THEN RAISE EXCEPTION 'Source grew during preparation; retry before creating a job'; END IF;
 {''.join(f"IF EXISTS(SELECT FROM public.{t} LIMIT 1) THEN RAISE EXCEPTION 'Shadow requires empty public.{t}'; END IF;" for t in TABLES)}
END $empty$;
CREATE SCHEMA {sql_ident(schema)};
CREATE TABLE {relation(schema,'observed_codes')}(chunk_id integer NOT NULL,code_id smallint NOT NULL,PRIMARY KEY(chunk_id,code_id));
{chr(10).join(clones)}
{dimension_phase(schema,scope)}
ALTER TABLE {relation(schema,'table4')} ADD CONSTRAINT {sql_ident(next(k['name'] for k in catalog['constraints'] if k['table']=='table4' and k['type']=='p'))} PRIMARY KEY(id_column_1);
INSERT INTO {JOB_TABLE}(job_id,signature,source_oid,state,schema_name,payload,source_identity,target_identity)
VALUES({sql_literal(job_id)},{sql_literal(signature)},{catalog['source']['oid']},'loading',{sql_literal(schema)},{sql_literal(json.dumps(payload))}::jsonb,{sql_literal(json.dumps(catalog['source']))}::jsonb,{sql_literal(json.dumps(base.target_ids(catalog)))}::jsonb);
UPDATE {JOB_TABLE} SET payload=payload || jsonb_build_object('dimension_rows',jsonb_build_object('table3',(SELECT count(*) FROM {relation(schema,'table3')}),'table4',(SELECT count(*) FROM {relation(schema,'table4')})),'shadow_identity',(SELECT jsonb_object_agg(c.relname,c.oid) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname={sql_literal(schema)} AND c.relname IN ('table1','table2','table3','table4','table5','table6'))) WHERE job_id={sql_literal(job_id)};
INSERT INTO {CHUNK_TABLE}(job_id,chunk_id,lo,hi)
SELECT {sql_literal(job_id)},row_number() OVER (ORDER BY r.lo),r.lo,LEAST(b.hi,r.lo+{width}-1)
FROM ({bounds}) b CROSS JOIN LATERAL generate_series(b.lo,b.hi,{width}::bigint) r(lo);
CREATE TRIGGER {base.FENCE} BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.raw_data FOR EACH STATEMENT EXECUTE FUNCTION pgdm_runtime.reject_shadow_source_write();
{raw_fk_proof.initialize_sql(job)}
{worker_procedure(job,plan,payload)}
COMMIT;
SELECT row_to_json(j)::text FROM {JOB_TABLE} j WHERE job_id={sql_literal(job_id)};"""
    if getattr(service, '_native_probe_deferred', False):
        service._native_probe_deferred = False
        mark = lambda label: "SELECT jsonb_build_object('rc5_mark'," + sql_literal(label) + ",'at',clock_timestamp());\n"
        prefix = mark('self_check_begin') + 'BEGIN;\n' + service._deferred_native_probe_sql
        prefix += "\nSELECT (current_setting('pgdm.native_probe')::jsonb->>'available')::boolean AS pgdm_native_available \\gset\nCOMMIT;\n"
        prefix += mark('self_check_end') + '\\if :pgdm_native_available\n'
        output = query(service, database, prefix + sql + '\n\\endif\n' + mark('initialize_end'), report,
                       'Native rollback-only self-check, dimensions, durable ledger and source fence', stage='prepare')
        values = [json.loads(line) for line in output.splitlines() if line.strip().startswith('{')]
        selection = next(v for v in values if 'available' in v)
        report['native_selection'] = selection
        report.setdefault('native_prepare_marks', []).extend(v for v in values if 'rc5_mark' in v)
        if not selection['available']:
            service._native_prepared_catalog = (database, service._chunk_cpu_receipt, catalog, None)
            from services.native_selection import fallback_reason
            report['fallback_reason'] = fallback_reason(selection['reason'])
            raise base.UnsupportedShadowError(report['fallback_reason'])
        return next(v for v in values if 'job_id' in v)
    return read_json(service,database,sql,report,'Create sampled dimensions, adaptive chunk ledger and frozen logged shadows')


def parallel_wave(service,database,job,statements,workers,report,name):
    return s3_publication.parallel_wave(service,database,job,statements,workers,report,name)


def publication_waves(service,database,job,workers,report,publish=None):
    if publish is None:
        return s3_publication.publication_waves(service,database,job,workers,report)
    return s3_publication.publication_waves(service,database,job,workers,report,publish=publish)


def build_publish_sql(service,prepared,job,catalog,report):
    import copy
    clean=copy.deepcopy(job)
    clean['payload']['catalog']['constraints']=[]
    clean['payload']['catalog']['indexes']=[]
    sql=base.build_publish_sql(service,prepared,clean,catalog,report)
    sql=sql.replace(f'DROP SCHEMA {sql_ident(job["schema_name"])};',
                    f'DROP TABLE {relation(job["schema_name"],"observed_codes")}; DROP SCHEMA {sql_ident(job["schema_name"])};')
    catalog_query=base.CATALOG.split('SELECT json_build_object(',1)[1].rstrip().rstrip(';')
    catalog_query='SELECT json_build_object('+catalog_query
    check=locked_check(job,catalog_query)+raw_fk_proof.guard_sql(job,final=True)+shared_fk_proof.publish_sql(job)
    sql=sql.replace('DO $ready$',check+'DO $ready$',1)
    for t in TABLES:
        sql=sql.replace(f'ALTER TABLE {relation(job["schema_name"],t)} SET SCHEMA public;',f'ALTER TABLE {relation(job["schema_name"],t)} RESET (autovacuum_enabled); ALTER TABLE {relation(job["schema_name"],t)} SET SCHEMA public;')
    return sql


def full_dimension_fallback(service,database,job,report):
    schema=job['schema_name'];jid=sql_literal(job['job_id'])
    scope=','.join(sql_literal(v) for v in job['payload']['raw_schemas'])
    keys=relation(schema,'s2_dimension_keys')
    ranges=read_json(service,database,f"DROP TABLE IF EXISTS {keys}; CREATE TABLE {keys}(ref text,code2 text,code3 text); SELECT json_agg(json_build_array(lo,hi) ORDER BY chunk_id) FROM {CHUNK_TABLE} WHERE job_id={jid};",report,'Prepare parallel full dimension fallback')
    statements=[]
    for lo,hi in ranges:
        bounds=f"r.ctid >= '({lo},0)'::tid AND r.ctid < '({hi+1},0)'::tid" if job['payload']['chunk_kind']=='tid' else f'r.raw_id BETWEEN {lo} AND {hi}'
        statements.append(f"""BEGIN; SET LOCAL enable_sort=off;
INSERT INTO {keys} SELECT BTRIM(v.j->>'text_column_1'),UPPER(BTRIM(v.j->>'text_column_2')),UPPER(BTRIM(v.j->>'text_column_3'))
FROM public.raw_data r CROSS JOIN LATERAL (SELECT r.raw->'values' j OFFSET 0) v
WHERE r.raw_schema IN ({scope}) AND {bounds} GROUP BY 1,2,3; COMMIT;""")
    parallel_wave(service,database,job,statements,job['payload']['workers'],report,'Full dimensions: parallel per-range hash aggregates from one values extraction')
    dimension_sql=dimension_phase(schema,scope,full=True)
    begin=dimension_sql.index('CREATE TEMP TABLE');end=dimension_sql.index('INSERT INTO',begin)
    dimension_sql=dimension_sql[:begin]+f'CREATE TEMP TABLE pgdm_shadow_dimension_keys ON COMMIT DROP AS SELECT ref,code2,code3 FROM {keys} GROUP BY 1,2,3;\n'+dimension_sql[end:]
    sql=f"""BEGIN;
TRUNCATE {','.join(relation(schema,t) for t in TABLES)},{relation(schema,'observed_codes')};
DELETE FROM {HASH_TABLE} WHERE job_id={jid};
UPDATE {CHUNK_TABLE} SET state='pending',row_counts=NULL,source_rows=NULL,backend_pid=NULL,started_at=NULL,committed_at=NULL,stage_bytes=NULL WHERE job_id={jid};
{dimension_sql}
DROP TABLE {keys};
UPDATE {JOB_TABLE} SET payload=payload || jsonb_build_object('full_dimensions',true,'dimension_rows',jsonb_build_object('table3',(SELECT count(*) FROM {relation(schema,'table3')}),'table4',(SELECT count(*) FROM {relation(schema,'table4')}))) WHERE job_id={jid};
COMMIT; SELECT row_to_json(j) FROM {JOB_TABLE} j WHERE job_id={jid};"""
    return read_json(service,database,sql,report,'S2 dimension miss: reset outputs and deterministic reprocessing')


def execute(service,database,prepared,requested,versions,report,progress_callback=None,post_commit_callback=None,native_bulk=None):
    service._last_shadow_report = report
    if native_bulk is None:
        native_bulk = os.getenv('PDM_EXPANSION_MODE') == 'native_bulk'
    expected_build = BULK_BUILD if native_bulk else NATIVE_BUILD
    build_function = 'pgdm_bulk_build_id' if native_bulk else 'pgdm_decode_build_id'
    plan=build_plan(prepared,requested,mode='compact')
    if not plan: raise base.UnsupportedShadowError('Shadow mode requires the reviewed official six-table workload')
    base.replay_children(prepared);base.replay_dimensions(prepared)
    scope=','.join(sql_literal(v) for v in prepared['raw_schemas'])
    page_scope=os.getenv('PDM_S2_SCOPE_COUNT','pages')!='exact' and os.getenv('PDM_S2_TID','1')!='0'
    fraction=f"(SELECT COALESCE(sum(f.freq),0) FROM pg_stats st CROSS JOIN LATERAL unnest(st.most_common_vals::text::text[],st.most_common_freqs) f(value,freq) WHERE st.schemaname='public' AND st.tablename='raw_data' AND st.attname='raw_schema' AND f.value IN ({scope}))"
    scope_count=f"CASE WHEN r.reltuples>0 AND {fraction}>=0.8 THEN ceil(r.reltuples*{fraction})::bigint ELSE (SELECT count(*) FROM public.raw_data WHERE raw_schema IN ({scope})) END" if page_scope else f"(SELECT count(*) FROM public.raw_data WHERE raw_schema IN ({scope}))"
    catalog_sql=base.CATALOG.replace("'database_oid',",f"'scope_rows',({scope_count}),'scope_count_basis',{'true' if page_scope else 'false'},'pages',pg_relation_size('public.raw_data')/current_setting('block_size')::bigint,'estimated_rows',r.reltuples,'shared_buffers_mb',pg_size_bytes(current_setting('shared_buffers'))/1048576,'connection_budget',GREATEST(1,current_setting('max_connections')::int-current_setting('superuser_reserved_connections')::int-COALESCE(current_setting('reserved_connections',true),'0')::int-(SELECT count(*) FROM pg_stat_activity)-4),'jobs',(SELECT json_agg(j) FROM {JOB_TABLE} j WHERE state IN ('loading','indexing','data_ready')),'database_oid',",1)
    # Both metadata and nproc are read on Linux, in the included prepare timer.
    cleanup = f"""DO $cleanup$ DECLARE j record; BEGIN
 FOR j IN SELECT * FROM {JOB_TABLE} WHERE state='aborted' LOOP
 IF NOT EXISTS(SELECT FROM pg_stat_activity WHERE application_name IN ('pgdm_shadow:'||j.job_id,'pgdm_shadow_control:'||j.job_id)) THEN
 EXECUTE format('DROP SCHEMA IF EXISTS %I CASCADE',j.schema_name);
 {shadow_transient_cleanup('j.job_id')}
 END IF; END LOOP; END $cleanup$;
"""
    catalog_sql=catalog_sql.replace("'database_oid',", "'native_hint_readback',current_setting('data_checksums')='on' OR current_setting('wal_log_hints')='on','server_version_num',current_setting('server_version_num')::integer,'database_oid',",1)
    sql=('SELECT pg_backend_pid() AS pgdm_backend_pid \\gset\n'
         '\\setenv PGDM_BACKEND_PID :pgdm_backend_pid\n'
         '\\! sh -c ' + shlex.quote(CPU_PROBE) + '\n'
         f'SELECT public.{build_function}();\n'+base.INFRASTRUCTURE.replace('SELECT true;','')+cleanup+catalog_sql)
    command=prepare_command(service.sql_username,database)
    cached=getattr(service,'_native_prepared_catalog',None)
    if cached and cached[0]==database:
        service._native_prepared_catalog=None
        cpu,catalog,build_id=cached[1:]
        report['catalog_preflight_batched']=True
    else:
        with service.expansion_timing_scope(report,'prepare','Prepare shadow infrastructure, catalog, resumable jobs and Linux worker budget'):
            output=service.run_remote_command(command,stdin_text=SESSION_WAL_SQL + sql)
        cpu,build_id,metadata=output.split('\n',2);catalog=json.loads(metadata)
    if build_id != expected_build: raise base.UnsupportedShadowError('Native build does not match this candidate: '+build_id)
    report.update(expansion_engine='S4_NATIVE_BULK' if native_bulk else 'S3_NATIVE_SCALAR',native_build_id=build_id)
    report['engine'] = report['expansion_engine']
    service._record_expansion_engine(report, report['engine'])
    workers=choose_workers(cpu,catalog,report,small_cpu_oversubscription=False)
    if any(k['type'] not in ('c','n','p','u','f') for k in catalog['constraints']):
        raise base.UnsupportedShadowError('Native shadow requires standard constraint kinds')
    if len(catalog['targets'])!=6 or any(t['kind']!='r' or t['acl'] is not None or t['rls'] for t in catalog['targets']) or catalog['external']:
        if catalog['fenced']: raise RuntimeError('An active shadow generation has incompatible destination dependencies')
        raise base.UnsupportedShadowError('Shadow publish requires isolated ordinary default-ACL destinations')
    raw_proof = raw_fk_proof.plan(catalog, native_bulk)
    shared_proof = shared_fk_proof.supported(catalog,native_bulk)
    report['raw_fk_proof_policy'] = raw_proof
    stable_plan={'workers':[worker_procedure({'job_id':'fingerprint','schema_name':'pgdm_shadow_template','payload':{'chunk_kind':kind,'native_bulk':native_bulk,'raw_fk_proof':raw_proof}},plan,prepared) for kind in ('tid','raw_id')],
                 'dimensions':[dimension_phase('pgdm_shadow_template',scope,full=full) for full in (False,True)],'recipes':[d['sql_recipe'] for d in prepared['destinations']]}
    from services.shadow_signature import resumable_job, migrate_replay_metadata
    signature,job,legacy_signature=resumable_job(catalog,prepared,versions,stable_plan,PLAN_VERSION,build_id)
    resuming=bool(job)
    report['shadow_resuming'] = resuming
    if job:
        if job['source_identity']!=catalog['source'] or not catalog['fenced']: raise RuntimeError('Source identity or persistent write fence changed')
        if base.target_shapes(job['payload']['catalog'])!=base.target_shapes(catalog): raise RuntimeError('Destination column or ownership definition changed')
        if job['state']=='data_ready':
            if base.target_ids(catalog)!=job['payload']['shadow_identity']:
                raise RuntimeError('Published destination identity changed')
            recovered=read_json(service,database,f"SELECT json_build_object('payload',(SELECT payload FROM pgdm_runtime.expansion_jobs WHERE job_id={sql_literal(job['job_id'])}),'rows',(SELECT jsonb_object_agg(t.name,(SELECT COALESCE(sum((row_counts->>t.name)::bigint),0) FROM {CHUNK_TABLE} WHERE job_id={sql_literal(job['job_id'])} AND state='done')) FROM unnest(ARRAY['table1','table2','table3','table4','table5','table6']) t(name)));",report,'Recover atomically published generation and counters')
            result=recovered['payload'];counts=recovered['rows'];counts.update(job['payload']['dimension_rows'])
            report['source_rows']=result['dependency_manifest']['expected_row_count']
            report['destination_rows']=[{'table_name':d['table_name'],'rows':int(counts[d['pure_table_name']])} for d in prepared['destinations']]
            report['total_rows_inserted']=sum(d['rows'] for d in report['destination_rows'])
            report['historical_timing_complete']=False;result['timing_report']=report
            service._pending_expansion_job_id=job['job_id']
            if post_commit_callback:post_commit_callback()
            return result
        if job['target_identity']!=base.target_ids(catalog): raise RuntimeError('Original destination identity changed during interrupted load')
        if legacy_signature:
            job=migrate_replay_metadata(service,database,job,signature,prepared,report,read_json)
    else:
        if catalog['fenced']: raise RuntimeError('Another resumable generation owns the source fence; use Discard expansion to release it')
        source_rows = int(catalog['scope_rows'])
        planned_chunk_rows = default_chunk_rows(source_rows, workers, native_bulk,
                                                catalog.get('native_hint_readback', True))
        chunk_rows=int(os.getenv('PDM_SHADOW_CHUNK_ROWS',str(planned_chunk_rows)))
        if chunk_rows<1: raise ValueError('Chunk size must be positive')
        payload={'source_database_name':database,'source_schema_name':'public','source_table_name':'raw_data','raw_schemas':prepared['raw_schemas'],'source_versions':versions,'destinations':prepared['destinations'],'catalog':{k:v for k,v in catalog.items() if k!='jobs'},'base_signature':fingerprint(prepared,versions),'chunk_kind':'tid' if int(catalog['scope_rows'])>=0.8*max(1,catalog['estimated_rows']) and os.getenv('PDM_S2_TID','1')!='0' else 'raw_id'}
        payload.update(native_bulk=native_bulk,native_build_id=build_id)
        if raw_proof:
            payload['raw_fk_proof'] = raw_proof
        if shared_proof:
            payload['shared_fk_proof'] = {'policy':shared_fk_proof.POLICY}
        if native_bulk:
            payload['native_reader_policy']=('core_read_stream' if catalog['server_version_num']>=170000 else 'direct_read_ahead') if payload['chunk_kind']=='tid' else 'spi1024'
        service._chunk_cpu_receipt = cpu
        job=initialize_job(service,database,uuid.uuid4().hex,signature,catalog,payload,chunk_rows,report,plan)
    # A lost durable policy must not turn a validated early FK into a bypass.
    job['_raw_fk_required'] = bool(raw_proof)
    job['_shared_fk_required'] = shared_proof
    stop_after=int(os.getenv('PDM_SHADOW_STOP_AFTER_CHUNKS','0'))
    service._notify_progress(progress_callback,'execute',10,'S4 native bulk logged chunks' if native_bulk else 'S3 native scalar logged chunks')
    try:
        loaded=s3_publication.load_chunks(service,database,job,workers,stop_after,report)
    except RuntimeError as exc:
        if 'S2_DIMENSION_MISS' not in str(exc) or job['payload'].get('full_dimensions'): raise
        job=full_dimension_fallback(service,database,job,report)
        job['_raw_fk_required'] = bool(raw_proof)
        job['_shared_fk_required'] = shared_proof
        loaded=s3_publication.load_chunks(service,database,job,workers,stop_after,report)
    report.update(shadow_job_id=job['job_id'],shadow_chunks_reused=loaded['reused_at_launch'],shadow_chunks_done=loaded['done'],shadow_chunks_total=loaded['total'],stage_peak_bytes_bound=int(loaded.get('max_chunk_bytes') or 0)*workers,s2_workers=workers,s2_chunk_kind=job['payload']['chunk_kind'],s2_chunk_rows=job['payload']['chunk_rows'])
    if loaded['done']!=loaded['total']: raise RuntimeError(f"CONTROLLED_SHADOW_INTERRUPTION: {loaded['done']}/{loaded['total']} chunks durable; public destinations remain unchanged")
    service._notify_progress(progress_callback,'execute',80,'Overlapping native indexes and foreign key validation')
    small = int(job['payload']['catalog']['scope_rows']) <= 1000000
    result=publication_waves(service,database,job,report['chunk_worker_policy']['publication_workers'],report,
        publish=(lambda: build_publish_sql(service,prepared,job,catalog,report)) if small else None)
    if result is None:
        result=read_json(service,database,build_publish_sql(service,prepared,job,catalog,report),report,'S2 wave 4: locked identity checks and atomic six-table publication',stage='publication')
    loaded['rows'].update(job['payload']['dimension_rows'])
    report['source_rows']=result['dependency_manifest']['expected_row_count']
    report['destination_rows']=[{'table_name':d['table_name'],'rows':int(loaded['rows'][d['pure_table_name']])} for d in prepared['destinations']]
    report['total_rows_inserted']=sum(r['rows'] for r in report['destination_rows'])
    if resuming:report['historical_timing_complete']=False
    result['timing_report']=report;service._pending_expansion_job_id=job['job_id']
    if post_commit_callback:post_commit_callback()
    return result


def discard(service,database,source):
    """Explicitly discard a private interrupted job without changing public outputs."""
    sql=f"""SET client_min_messages=warning;
DO $discard$ DECLARE j record; BEGIN
 IF to_regclass('pgdm_runtime.shadow_jobs') IS NULL THEN RETURN; END IF;
 FOR j IN SELECT * FROM {JOB_TABLE} WHERE source_oid=to_regclass({sql_literal(source)}) AND state IN ('loading','indexing') FOR UPDATE LOOP
 IF EXISTS(SELECT FROM pg_stat_activity WHERE application_name IN ('pgdm_shadow:'||j.job_id,'pgdm_shadow_control:'||j.job_id)) THEN
 RAISE EXCEPTION 'Stop the active expansion before discarding its job'; END IF;
 EXECUTE format('LOCK TABLE %s IN SHARE ROW EXCLUSIVE MODE',j.source_oid::regclass);
 EXECUTE format('DROP SCHEMA IF EXISTS %I CASCADE',j.schema_name);
 EXECUTE format('DROP TRIGGER IF EXISTS pgdm_shadow_source_fence ON %s',j.source_oid::regclass);
 {shadow_transient_cleanup('j.job_id')}
 UPDATE {JOB_TABLE} SET state='aborted' WHERE job_id=j.job_id;
 END LOOP;
 FOR j IN SELECT * FROM {JOB_TABLE} WHERE state='aborted' LOOP
 IF NOT EXISTS(SELECT FROM pg_stat_activity WHERE application_name='pgdm_shadow:'||j.job_id) THEN
 EXECUTE format('DROP SCHEMA IF EXISTS %I CASCADE',j.schema_name);
 EXECUTE format('DROP PROCEDURE IF EXISTS pgdm_runtime.%I()', 'worker_'||j.job_id);
 END IF; END LOOP;
IF NOT EXISTS(SELECT FROM {JOB_TABLE} WHERE source_oid=to_regclass({sql_literal(source)}) AND state IN ('loading','indexing','data_ready')) THEN
 EXECUTE format('DROP TRIGGER IF EXISTS pgdm_shadow_source_fence ON %s',to_regclass({sql_literal(source)}));
 END IF;
END $discard$;"""
    from services.expansion_recovery import remote_sql
    remote_sql(service,database,sql)
