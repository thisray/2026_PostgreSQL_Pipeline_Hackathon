"""Adaptive SQL shadow candidate with durable parallel publication waves."""
import hashlib
import json
import os
import re
import shlex
import uuid
from pathlib import Path

from services.expansion_wal import SESSION_WAL_SQL
from services import shadow_expansion as base
from services.expansion_optimizer import build_plan
from services.expansion_recovery import fingerprint, shadow_transient_cleanup, terminal_shadow_cleanup
from services.publication_identity import locked_check
from utils import sql_ident, sql_literal

TABLES = base.TABLES
JOB_TABLE, CHUNK_TABLE, HASH_TABLE = base.JOB_TABLE, base.CHUNK_TABLE, base.HASH_TABLE
relation, query, read_json = base.relation, base.query, base.read_json
PLAN_VERSION = 's2-sql-v3'


def preparation_sql(prepared, extra='', profile=False):
    """Render the same job-free catalog preparation for batched auto probing."""
    scope=','.join(sql_literal(v) for v in prepared['raw_schemas'])
    page_scope=os.getenv('PDM_S2_SCOPE_COUNT','pages')!='exact' and os.getenv('PDM_S2_TID','1')!='0'
    fraction=f"(SELECT COALESCE(sum(f.freq),0) FROM pg_stats st CROSS JOIN LATERAL unnest(st.most_common_vals::text::text[],st.most_common_freqs) f(value,freq) WHERE st.schemaname='public' AND st.tablename='raw_data' AND st.attname='raw_schema' AND f.value IN ({scope}))"
    scope_count=f"CASE WHEN r.reltuples>0 AND {fraction}>=0.8 THEN ceil(r.reltuples*{fraction})::bigint ELSE (SELECT count(*) FROM public.raw_data WHERE raw_schema IN ({scope})) END" if page_scope else f"(SELECT count(*) FROM public.raw_data WHERE raw_schema IN ({scope}))"
    catalog_sql=base.CATALOG.replace("'database_oid',",f"'server_version_num',current_setting('server_version_num')::integer,'scope_rows',({scope_count}),'scope_count_basis',{'true' if page_scope else 'false'},'pages',pg_relation_size('public.raw_data')/current_setting('block_size')::bigint,'estimated_rows',r.reltuples,'shared_buffers_mb',pg_size_bytes(current_setting('shared_buffers'))/1048576,'connection_budget',GREATEST(1,current_setting('max_connections')::int-current_setting('superuser_reserved_connections')::int-COALESCE(current_setting('reserved_connections',true),'0')::int-(SELECT count(*) FROM pg_stat_activity)-4),'jobs',(SELECT json_agg(j) FROM {JOB_TABLE} j WHERE state IN ('loading','indexing','data_ready')),'database_oid',",1)
    cleanup=terminal_shadow_cleanup()
    mark = lambda label: ("SELECT jsonb_build_object('rc5_mark',"+sql_literal(label)+",'at',clock_timestamp());\n") if profile else ''
    catalog_sql = catalog_sql.replace("'server_version_num',", "'native_bulk_api',(SELECT extversion FROM pg_extension WHERE extname='pgdm_bulk' AND extnamespace='public'::regnamespace),'native_hint_readback',current_setting('data_checksums')='on' OR current_setting('wal_log_hints')='on','server_version_num',", 1)
    return (base.INFRASTRUCTURE.replace('SELECT true;','') + mark('infrastructure_end') + extra + cleanup
            + mark('cleanup_end') + catalog_sql)


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
    base_keys = ['id_column_1','text_column_1','datetime_column_1','integer_column_1','text_column_2','numeric_column_1','text_column_3','numeric_column_2']
    fields = ','.join(f"v.j->>{sql_literal(key)} AS {key}" for key in base_keys)
    declared = ','.join(f"NULLIF(BTRIM(v.j->>'integer_column_{k+1}'),'')::smallint AS d{k}" for k in range(1,15))
    slots = ','.join(
        f"CASE WHEN j.d{k}={k} THEN NULLIF(BTRIM(v.j->>'numeric_column_{2*k+1}'),'')::numeric END w{k},"
        f"CASE WHEN j.d{k}={k} THEN UPPER(BTRIM(v.j->>'text_column_{k+3}')) END t{k},"
        f"CASE WHEN j.d{k}={k} THEN NULLIF(BTRIM(v.j->>'numeric_column_{2*k+2}'),'')::numeric END g{k}"
        for k in range(1,15))
    arrays = []
    for name, expression in [('slot_pos',lambda k:f'{k}::smallint'),('slot_weight',lambda k:f'ROUND(w{k}*1000)::integer'),('slot_type',lambda k:f't{k}'),('slot_gap',lambda k:f'CASE WHEN g{k}>0 THEN ROUND(g{k}*1000)::integer END')]:
        values = ','.join(f"CASE WHEN j.d{k}={k} AND w{k}>=0 AND t{k} IN ('X','Y','Z') THEN {expression(k)} END" for k in range(1,15))
        arrays.append(f'ARRAY[{values}] AS {name}')
    bounds = "r.ctid >= format('(%s,0)',c.lo)::tid AND r.ctid < format('(%s,0)',c.hi+1)::tid" if tid else 'r.raw_id BETWEEN c.lo AND c.hi'
    return f"""SELECT r.raw_id,r.raw_schema,r.raw_hash,
 NULLIF(BTRIM(j.id_column_1),'')::integer external_id,
 NULLIF(BTRIM(j.datetime_column_1),'')::timestamptz AT TIME ZONE 'America/Sao_Paulo' observed_at,
 GREATEST({','.join(f'j.d{k}' for k in range(1,15))}) max_declared,
 CASE WHEN NULLIF(BTRIM(j.numeric_column_2),'')::numeric>0 THEN ROUND(NULLIF(BTRIM(j.numeric_column_2),'')::numeric*1000)::integer END n2,
 CASE WHEN NULLIF(BTRIM(j.numeric_column_1),'')::numeric>0 THEN ROUND(NULLIF(BTRIM(j.numeric_column_1),'')::numeric*1000)::integer END n1,
 NULLIF(BTRIM(j.integer_column_1),'')::numeric speed,
 b.ref,b.code2,b.code3,(b.ref<>'' AND b.code2 ~ '^[A-Z]{{2}}$') parent_ok,
 d3.id_column_1 dim3_id,d4.id_column_1 dim4_id,{','.join(arrays)}
FROM public.raw_data r
CROSS JOIN LATERAL (SELECT r.raw->'values' j OFFSET 0) v
CROSS JOIN LATERAL (SELECT {fields},{declared} OFFSET 0) j
CROSS JOIN LATERAL (SELECT BTRIM(j.text_column_1) ref,UPPER(BTRIM(j.text_column_2)) code2,UPPER(BTRIM(j.text_column_3)) code3 OFFSET 0) b
CROSS JOIN LATERAL (SELECT {slots} OFFSET 0) s
LEFT JOIN {relation(schema,'table3')} d3 ON d3.text_column_1=b.ref AND d3.text_column_2='SYNTHETIC/REFERENCE'
 AND d3.numeric_column_1 IS NULL AND d3.text_column_3=LEFT(b.code2,1) AND d3.text_column_4=RIGHT(b.code2,1)
LEFT JOIN {relation(schema,'table4')} d4 ON d4.text_column_1='SYNTHETIC/GENERIC' AND d4.text_column_2=b.code3
WHERE r.raw_schema IN ({scope}) AND {bounds}"""


def worker_procedure(job, plan, prepared):
    job_id,schema = job['job_id'],job['schema_name']
    scope = ','.join(sql_literal(v) for v in prepared['raw_schemas'])
    tid = job.get('payload',{}).get('chunk_kind','tid')=='tid'
    parent = plan['canonical_inserts']['table1']
    parent = parent[:parent.index('FROM pg_temp.pgdm_compact_stage')]+'FROM pg_temp.pgdm_compact_stage s WHERE s.dim3_id IS NOT NULL'
    parent = parent.replace('public.table1',relation(schema,'table1')).replace('d.id_column_1','s.dim3_id')
    return f"""
CREATE OR REPLACE PROCEDURE pgdm_runtime.worker_{job_id}() LANGUAGE plpgsql AS $worker$
DECLARE c record;n1 bigint;n2 bigint;n5 bigint;n6 bigint;stage_size bigint;
BEGIN
 PERFORM set_config('application_name',{sql_literal('pgdm_shadow:'+job_id)},false);
 PERFORM set_config('temp_buffers','128MB',false);
 LOOP
 SELECT * INTO c FROM {CHUNK_TABLE} WHERE job_id={sql_literal(job_id)} AND state='pending'
 ORDER BY chunk_id FOR UPDATE SKIP LOCKED LIMIT 1;
 IF NOT FOUND THEN RETURN; END IF;
 PERFORM set_config('enable_memoize','off',true);
 UPDATE {CHUNK_TABLE} SET backend_pid=pg_backend_pid(),started_at=clock_timestamp() WHERE job_id=c.job_id AND chunk_id=c.chunk_id;
 CREATE TEMP TABLE pgdm_compact_stage ON COMMIT DROP AS {compact_select(schema,scope,tid)};
 IF EXISTS(SELECT FROM pg_temp.pgdm_compact_stage WHERE code3 ~ '^[A-Z]{{2}}$' AND dim4_id IS NULL) THEN
 INSERT INTO {relation(schema,'table4')}(id_column_1,text_column_1,text_column_2)
 SELECT ((ASCII(LEFT(code3,1))-65)*26+ASCII(RIGHT(code3,1))-65+1)::smallint,'SYNTHETIC/GENERIC',code3
 FROM pg_temp.pgdm_compact_stage WHERE code3 ~ '^[A-Z]{{2}}$' AND dim4_id IS NULL GROUP BY code3
 ORDER BY code3 COLLATE "C" ON CONFLICT(id_column_1) DO NOTHING;
 UPDATE pg_temp.pgdm_compact_stage s SET dim4_id=d.id_column_1 FROM {relation(schema,'table4')} d
 WHERE s.dim4_id IS NULL AND d.text_column_1='SYNTHETIC/GENERIC' AND d.text_column_2=s.code3;
 END IF;
 IF EXISTS(SELECT FROM pg_temp.pgdm_compact_stage WHERE (parent_ok AND dim3_id IS NULL) OR (code3 ~ '^[A-Z]{{2}}$' AND dim4_id IS NULL)) THEN
 RAISE EXCEPTION 'S2_DIMENSION_MISS: full dimensions and deterministic reprocessing required'; END IF;
 {parent}; GET DIAGNOSTICS n1=ROW_COUNT;
 INSERT INTO {relation(schema,'table5')}(id_column_1,numeric_column_1,numeric_column_2,integer_column_1,integer_column_2,id_column_2,id_column_3)
 SELECT raw_id,ROUND(speed,1),NULL::numeric,NULL::integer,NULL::integer,raw_id,NULL::bigint FROM pg_temp.pgdm_compact_stage WHERE dim3_id IS NOT NULL AND speed>0;
 GET DIAGNOSTICS n5=ROW_COUNT;
 INSERT INTO {relation(schema,'table6')}(id_column_1,id_column_2,id_column_3)
 SELECT raw_id,raw_id,dim4_id FROM pg_temp.pgdm_compact_stage WHERE dim3_id IS NOT NULL AND dim4_id IS NOT NULL;
 GET DIAGNOSTICS n6=ROW_COUNT;
 INSERT INTO {relation(schema,'table2')}(id_column_1,id_column_2,integer_column_1,integer_column_2,text_column_1,integer_column_3,integer_column_4,integer_column_5,integer_column_6)
 SELECT (s.raw_id-1)*14+p.pos,s.raw_id,p.pos,p.weight,p.type_code,NULL::integer,NULL::integer,NULL::integer,p.gap
 FROM pg_temp.pgdm_compact_stage s CROSS JOIN LATERAL unnest(s.slot_pos,s.slot_weight,s.slot_type,s.slot_gap) p(pos,weight,type_code,gap)
 WHERE s.dim3_id IS NOT NULL AND p.pos IS NOT NULL;
 GET DIAGNOSTICS n2=ROW_COUNT;
 INSERT INTO {HASH_TABLE} SELECT {sql_literal(job_id)},c.chunk_id,raw_schema,raw_hash,count(*) FROM pg_temp.pgdm_compact_stage GROUP BY raw_schema,raw_hash;
 stage_size:=pg_total_relation_size('pg_temp.pgdm_compact_stage');
 UPDATE {CHUNK_TABLE} SET state='done',row_counts=jsonb_build_object('table1',n1,'table2',n2,'table3',0,'table4',0,'table5',n5,'table6',n6),
 source_rows=(SELECT sum(row_count) FROM {HASH_TABLE} WHERE job_id=c.job_id AND chunk_id=c.chunk_id),stage_bytes=stage_size,committed_at=clock_timestamp()
 WHERE job_id=c.job_id AND chunk_id=c.chunk_id;
 COMMIT;
 END LOOP;
END $worker$;
"""


def initialize_job(service,database,job_id,signature,catalog,payload,chunk_rows,report,plan,implementation=None):
    import sys
    engine = implementation or sys.modules[__name__]
    schema='pgdm_shadow_'+job_id
    clones=[]
    for t in TABLES:
        original=next(x for x in catalog['targets'] if x['name']==t)
        clones.append(f'CREATE TABLE {relation(schema,t)} (LIKE public.{t} INCLUDING DEFAULTS INCLUDING IDENTITY INCLUDING CONSTRAINTS INCLUDING STORAGE INCLUDING COMMENTS) WITH (autovacuum_enabled=false);')
        clones.append(f'ALTER TABLE {relation(schema,t)} OWNER TO {sql_ident(original["owner"])};')
        clones.append(f'COMMENT ON TABLE {relation(schema,t)} IS {sql_literal(original["comment"]) if original["comment"] is not None else "NULL"};')
    scope=','.join(sql_literal(v) for v in payload['raw_schemas'])
    n=max(int(catalog['scope_rows']),1); pages=int(catalog['pages'])
    if implementation:
        payload['small_manifest_from_stage'] = n <= 1000000
        if n > 1000000:
            payload['manifest_from_stage'] = True
    tid=payload['chunk_kind']=='tid'
    width=max(1,(chunk_rows*pages+n-1)//n) if tid else chunk_rows
    if tid and catalog['scope_count_basis']:
        width=max((10000*pages+n-1)//n,min((250000*pages+n-1)//n,(pages+catalog['workers']*4-1)//(catalog['workers']*4)))
        if implementation and n <= 1000000:
            width=max(1,min((250000*pages+n-1)//n,(pages+catalog['workers']-1)//catalog['workers']))
            chunk_rows=min(250000,(n+catalog['workers']-1)//catalog['workers'])
    if implementation and tid and n > 1000000 and catalog.get('native_hint_readback', False):
        chunk_rows=min(chunk_rows,62500)
        width=min(width,max(1,(62500*pages+n-1)//n))
    bounds=f'SELECT 0::bigint lo,{pages-1}::bigint hi' if tid else f'SELECT min(raw_id) lo,max(raw_id) hi FROM public.raw_data WHERE raw_schema IN ({scope})'
    payload.update(chunk_rows=chunk_rows,chunk_span=width,chunk_units='pages' if tid else 'raw_id',scope_count_basis='page count with density bounds' if tid and catalog['scope_count_basis'] else 'exact scoped rows',workers=catalog['workers'])
    job={'job_id':job_id,'schema_name':schema,'payload':payload}
    early_key = '' if implementation else f"ALTER TABLE {relation(schema,'table4')} ADD CONSTRAINT {sql_ident(next(k['name'] for k in catalog['constraints'] if k['table']=='table4' and k['type']=='p'))} PRIMARY KEY(id_column_1);"
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
{chr(10).join(clones)}
{engine.dimension_phase(schema,scope)}
{early_key}
INSERT INTO {JOB_TABLE}(job_id,signature,source_oid,state,schema_name,payload,source_identity,target_identity)
VALUES({sql_literal(job_id)},{sql_literal(signature)},{catalog['source']['oid']},'loading',{sql_literal(schema)},{sql_literal(json.dumps(payload))}::jsonb,{sql_literal(json.dumps(catalog['source']))}::jsonb,{sql_literal(json.dumps(base.target_ids(catalog)))}::jsonb);
UPDATE {JOB_TABLE} SET payload=payload || jsonb_build_object('dimension_rows',jsonb_build_object('table3',(SELECT count(*) FROM {relation(schema,'table3')}),'table4',(SELECT count(*) FROM {relation(schema,'table4')})),'shadow_identity',(SELECT jsonb_object_agg(c.relname,c.oid) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname={sql_literal(schema)} AND c.relname IN ('table1','table2','table3','table4','table5','table6'))) WHERE job_id={sql_literal(job_id)};
INSERT INTO {CHUNK_TABLE}(job_id,chunk_id,lo,hi)
SELECT {sql_literal(job_id)},row_number() OVER (ORDER BY r.lo),r.lo,LEAST(b.hi,r.lo+{width}-1)
FROM ({bounds}) b CROSS JOIN LATERAL generate_series(b.lo,b.hi,{width}::bigint) r(lo);
CREATE TRIGGER {base.FENCE} BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.raw_data FOR EACH STATEMENT EXECUTE FUNCTION pgdm_runtime.reject_shadow_source_write();
{engine.worker_procedure(job,plan,payload)}
COMMIT;
SELECT row_to_json(j)::text FROM {JOB_TABLE} j WHERE job_id={sql_literal(job_id)};"""
    return read_json(service,database,sql,report,'Create sampled dimensions, adaptive chunk ledger and frozen logged shadows')


def parallel_wave(service,database,job,statements,workers,report,name):
    # Each statement is committed by its own session; completed work survives disconnects.
    commands=[]
    for sql in statements:
        command=['psql','-h','localhost','-U',service.sql_username,'-d',database,'-XqAt','-v','ON_ERROR_STOP=1','-c',sql]
        commands.append(' '.join(shlex.quote(x) for x in command)+' </dev/null &\npids+=("$!")\n'
                        'if ((${#pids[@]}>=limit)); then for p in "${pids[@]}"; do wait "$p" || failed=1; done; pids=(); fi')
    script=f'''set -euo pipefail
export PGAPPNAME={shlex.quote('pgdm_shadow:'+job['job_id'])}
exec 9>/tmp/pgdm-shadow-{job['job_id']}.lock
flock -x 9
limit={workers}; failed=0; pids=()
trap 'for p in "${{pids[@]}}"; do kill -TERM "$p" 2>/dev/null || true; done; exit 130' HUP INT TERM
{chr(10).join(commands)}
for p in "${{pids[@]}}"; do wait "$p" || failed=1; done
exit "$failed"
'''
    with service.expansion_timing_scope(report,'execute',name,record_local_cpu=False):
        service.run_remote_command('bash -s',stdin_text=script)


def publication_waves(service,database,job,workers,report):
    schema=job['schema_name']; catalog=job['payload']['catalog']
    base.validate_catalog(catalog)
    indexes=[]; attach=[]; validations=[]
    total_budget=int(os.getenv('PDM_S2_INDEX_MEMORY_MB','1024'))
    if not 32<=total_budget<=8192: raise ValueError('Index memory budget must be between 32 and 8192 MB')
    budget=max(1,total_budget//workers)
    settings=f"SET LOCAL max_parallel_maintenance_workers=0; SET LOCAL maintenance_work_mem='{budget}MB'; SET LOCAL work_mem='512MB';"
    compression="""DO $wal$ BEGIN IF current_setting('is_superuser')='on' THEN
 BEGIN PERFORM set_config('wal_compression','lz4',true); EXCEPTION WHEN invalid_parameter_value OR feature_not_supported THEN NULL; END;
 END IF; END $wal$;"""
    specs=[{'table':k['table'],'name':k['index_name'],'definition':k['index_definition']} for k in catalog['constraints'] if k['type'] in ('p','u')]
    specs+=catalog.get('indexes') or []
    for i in specs:
        definition=re.sub(r'\bON public\.(table[1-6])\b',lambda m:'ON '+relation(schema,m.group(1)),i['definition'])
        name=i['name']
        statement=f"""BEGIN; {settings} {compression}
DO $index$ BEGIN
 IF NOT EXISTS(SELECT FROM pg_index WHERE indexrelid=to_regclass({sql_literal(schema+'.'+name)}) AND indisvalid) THEN
 EXECUTE {sql_literal(definition)}; END IF;
END $index$; COMMIT;"""
        indexes.append(statement)
    parallel_wave(service,database,job,indexes,workers,report,'S2 wave 1: parallel durable indexes')
    for k in catalog['constraints']:
        if k['type'] in ('c','n'): continue
        if k['type'] in ('p','u'):
            definition=('PRIMARY KEY' if k['type']=='p' else 'UNIQUE')+' USING INDEX '+sql_ident(k['index_name'])
        elif k['type']=='f':
            parent=next((name for name,oid in job['target_identity'].items() if oid==k['referenced_oid']),None)
            target=relation(schema,parent) if parent else relation(k['reference_schema'],k['reference_table'])
            prefix,tail=k['definition'].split('REFERENCES ',1)
            definition=prefix+'REFERENCES '+target+'('+tail.split('(',1)[1]+' NOT VALID'
            validations.append(f"""BEGIN; {settings}
DO $validate$ BEGIN
 IF EXISTS(SELECT FROM pg_constraint WHERE conrelid={sql_literal(schema+'.'+k['table'])}::regclass AND conname={sql_literal(k['name'])} AND NOT convalidated) THEN
 ALTER TABLE {relation(schema,k['table'])} VALIDATE CONSTRAINT {sql_ident(k['name'])}; END IF;
END $validate$; COMMIT;""")
        ddl=f"ALTER TABLE {relation(schema,k['table'])} ADD CONSTRAINT {sql_ident(k['name'])} {definition}"
        attach.append(f"IF NOT EXISTS(SELECT FROM pg_constraint WHERE conrelid={sql_literal(schema+'.'+k['table'])}::regclass AND conname={sql_literal(k['name'])}) THEN EXECUTE {sql_literal(ddl)}; END IF;")
    updated=read_json(service,database,'BEGIN; DO $attach$ BEGIN '+''.join(attach)+f" END $attach$; UPDATE {JOB_TABLE} SET state='indexing',payload=jsonb_set(payload,'{{dimension_rows,table4}}',to_jsonb((SELECT count(*) FROM {relation(schema,'table4')}))) WHERE job_id={sql_literal(job['job_id'])}; COMMIT; SELECT payload FROM {JOB_TABLE} WHERE job_id={sql_literal(job['job_id'])};",report,'S2 wave 2: attach keys and all foreign keys NOT VALID')
    job['payload']=updated
    parallel_wave(service,database,job,validations,workers,report,'S2 wave 3: parallel durable foreign key validation')


def build_publish_sql(service,prepared,job,catalog,report):
    import copy
    clean=copy.deepcopy(job)
    clean['payload']['catalog']['constraints']=[]
    clean['payload']['catalog']['indexes']=[]
    sql=base.build_publish_sql(service,prepared,clean,catalog,report)
    catalog_query=base.CATALOG.split('SELECT json_build_object(',1)[1].rstrip().rstrip(';')
    catalog_query='SELECT json_build_object('+catalog_query
    check=locked_check(job,catalog_query)

    sql=sql.replace('DO $ready$',check+'DO $ready$',1)
    for t in TABLES:
        sql=sql.replace(f'ALTER TABLE {relation(job["schema_name"],t)} SET SCHEMA public;',f'ALTER TABLE {relation(job["schema_name"],t)} RESET (autovacuum_enabled); ALTER TABLE {relation(job["schema_name"],t)} SET SCHEMA public;')
    return sql


def full_dimension_fallback(service,database,job,report,implementation=None):
    import sys
    engine = implementation or sys.modules[__name__]
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
    dimension_sql=engine.dimension_phase(schema,scope,full=True)
    begin=dimension_sql.index('CREATE TEMP TABLE');end=dimension_sql.index('INSERT INTO',begin)
    dimension_sql=dimension_sql[:begin]+f'CREATE TEMP TABLE pgdm_shadow_dimension_keys ON COMMIT DROP AS SELECT ref,code2,code3 FROM {keys} GROUP BY 1,2,3;\n'+dimension_sql[end:]
    sql=f"""BEGIN;
TRUNCATE {','.join(relation(schema,t) for t in TABLES)};
DELETE FROM {HASH_TABLE} WHERE job_id={jid};
{f'DELETE FROM pgdm_runtime.shadow_codes WHERE job_id={jid};' if implementation else ''}
UPDATE {CHUNK_TABLE} SET state='pending',row_counts=NULL,source_rows=NULL,backend_pid=NULL,started_at=NULL,committed_at=NULL,stage_bytes=NULL WHERE job_id={jid};
{dimension_sql}
DROP TABLE {keys};
UPDATE {JOB_TABLE} SET payload=payload || jsonb_build_object('full_dimensions',true,'dimension_rows',jsonb_build_object('table3',(SELECT count(*) FROM {relation(schema,'table3')}),'table4',(SELECT count(*) FROM {relation(schema,'table4')}))) WHERE job_id={jid};
COMMIT; SELECT row_to_json(j) FROM {JOB_TABLE} j WHERE job_id={jid};"""
    return read_json(service,database,sql,report,'S2 dimension miss: reset outputs and deterministic reprocessing')



def large_s3_policy(catalog,prepared,versions):
    # Recovery uses the persisted SQL shape even if ANALYZE crosses the cutoff.
    if catalog.get('jobs'):
        logical=fingerprint(prepared,versions)
        for job in catalog['jobs']:
            payload=job['payload']
            if (job['source_identity']==catalog['source'] and payload['base_signature']==logical
                and not payload.get('native_build_id')):
                return bool(payload.get('manifest_from_stage',False))
    return int(catalog['scope_rows'])>1000000


def execute(service,database,prepared,requested,versions,report,progress_callback=None,post_commit_callback=None,implementation=None):
    import sys
    engine = implementation or sys.modules[__name__]
    plan=build_plan(prepared,requested,mode='compact')
    if not plan: raise base.UnsupportedShadowError('Shadow mode requires the reviewed official six-table workload')
    base.replay_children(prepared);base.replay_dimensions(prepared)
    scope=','.join(sql_literal(v) for v in prepared['raw_schemas'])
    page_scope=os.getenv('PDM_S2_SCOPE_COUNT','pages')!='exact' and os.getenv('PDM_S2_TID','1')!='0'
    fraction=f"(SELECT COALESCE(sum(f.freq),0) FROM pg_stats st CROSS JOIN LATERAL unnest(st.most_common_vals::text::text[],st.most_common_freqs) f(value,freq) WHERE st.schemaname='public' AND st.tablename='raw_data' AND st.attname='raw_schema' AND f.value IN ({scope}))"
    scope_count=f"CASE WHEN r.reltuples>0 AND {fraction}>=0.8 THEN ceil(r.reltuples*{fraction})::bigint ELSE (SELECT count(*) FROM public.raw_data WHERE raw_schema IN ({scope})) END" if page_scope else f"(SELECT count(*) FROM public.raw_data WHERE raw_schema IN ({scope}))"
    catalog_sql=base.CATALOG.replace("'database_oid',",f"'scope_rows',({scope_count}),'scope_count_basis',{'true' if page_scope else 'false'},'pages',pg_relation_size('public.raw_data')/current_setting('block_size')::bigint,'estimated_rows',r.reltuples,'shared_buffers_mb',pg_size_bytes(current_setting('shared_buffers'))/1048576,'connection_budget',GREATEST(1,current_setting('max_connections')::int-current_setting('superuser_reserved_connections')::int-COALESCE(current_setting('reserved_connections',true),'0')::int-(SELECT count(*) FROM pg_stat_activity)-4),'jobs',(SELECT json_agg(j) FROM {JOB_TABLE} j WHERE state IN ('loading','indexing','data_ready')),'database_oid',",1)
    # Both metadata and nproc are read on Linux, in the included prepare timer.
    cleanup = terminal_shadow_cleanup()
    sql=base.INFRASTRUCTURE.replace('SELECT true;','')+getattr(engine,'EXTRA_INFRASTRUCTURE','')+cleanup+catalog_sql
    command=getattr(engine,'prepare_command',lambda role,db: f"printf '%s\\n' \"$(nproc)\"; psql -h localhost -U {shlex.quote(role)} -d {shlex.quote(db)} -XqAt -v ON_ERROR_STOP=1 -f -")(service.sql_username,database)
    cached=getattr(service,'_native_prepared_catalog',None)
    if cached and cached[0]==database:
        service._native_prepared_catalog=None
        cpu,catalog=cached[1],cached[2]
        report['catalog_preflight_batched']=True
    else:
        with service.expansion_timing_scope(report,'prepare','Prepare shadow infrastructure, catalog, resumable jobs and Linux worker budget'):
            output=service.run_remote_command(command,stdin_text=SESSION_WAL_SQL + sql)
        cpu,metadata=output.split('\n',1);catalog=json.loads(metadata)
    base.validate_catalog(catalog)
    large_s3 = implementation and large_s3_policy(catalog,prepared,versions)
    if large_s3 and 'native_hint_readback' not in catalog:
        catalog.update(read_json(service,database,
            "SELECT json_build_object('native_hint_readback',current_setting('data_checksums')='on' OR current_setting('wal_log_hints')='on');",
            report,'Read large S3 hint read-back policy'))
    if implementation:
        workers=engine.choose_workers(cpu,catalog,report)
    else:
        workers=int(os.getenv('PDM_STAGE_WORKERS',str(min(int(cpu),16))))
        if not 1<=workers<=32: raise ValueError('Workers must be between 1 and 32')
        workers=min(workers,catalog['connection_budget']);catalog['workers']=workers
    if len(catalog['targets'])!=6 or any(t['kind']!='r' or t['acl'] is not None or t['rls'] for t in catalog['targets']) or catalog['external']:
        if catalog['fenced']: raise RuntimeError('An active shadow generation has incompatible destination dependencies')
        raise base.UnsupportedShadowError('Shadow publish requires isolated ordinary default-ACL destinations')
    # Hash the complete supported repertoire, not a choice based on ANALYZE
    # estimates. The persisted job payload keeps its original worker policy.
    stable_plan={'workers':[engine.worker_procedure({'job_id':'fingerprint','schema_name':'pgdm_shadow_template','payload':{'chunk_kind':kind,'small_manifest_from_stage':small}},plan,prepared) for kind in ('tid','raw_id') for small in ((False,True) if implementation else (False,))],
                 'dimensions':[engine.dimension_phase('pgdm_shadow_template',scope,full=full) for full in (False,True)],'recipes':[d['sql_recipe'] for d in prepared['destinations']]}
    if large_s3:
        stable_plan['workers'].extend(engine.worker_procedure(
            {'job_id':'fingerprint','schema_name':'pgdm_shadow_template',
             'payload':{'chunk_kind':kind,'manifest_from_stage':True}},plan,prepared)
            for kind in ('tid','raw_id'))
    from services.shadow_signature import resumable_job, migrate_replay_metadata
    signature,job,legacy_signature=resumable_job(catalog,prepared,versions,stable_plan,engine.PLAN_VERSION)
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
        chunk_rows=int(os.getenv('PDM_SHADOW_CHUNK_ROWS',str(max(10000,min(250000,(int(catalog['scope_rows'])+workers*4-1)//(workers*4))))))
        if chunk_rows<1: raise ValueError('Chunk size must be positive')
        payload={'source_database_name':database,'source_schema_name':'public','source_table_name':'raw_data','raw_schemas':prepared['raw_schemas'],'source_versions':versions,'destinations':prepared['destinations'],'catalog':{k:v for k,v in catalog.items() if k!='jobs'},'base_signature':fingerprint(prepared,versions),'chunk_kind':'tid' if int(catalog['scope_rows'])>=0.8*max(1,catalog['estimated_rows']) and os.getenv('PDM_S2_TID','1')!='0' else 'raw_id'}
        job=initialize_job(service,database,uuid.uuid4().hex,signature,catalog,payload,chunk_rows,report,plan,implementation)
    stop_after=int(os.getenv('PDM_SHADOW_STOP_AFTER_CHUNKS','0'))
    if implementation and int(job['payload']['catalog']['scope_rows']) <= 1000000:
        job['_publish_sql_factory'] = lambda: build_publish_sql(service,prepared,job,catalog,report)
    service._notify_progress(progress_callback,'execute',10,'S2 adaptive logged chunks')
    try:
        loaded=engine.load_and_publish(service,database,job,workers,stop_after,report) if implementation else base.load_chunks(service,database,job,workers,stop_after,report)
    except RuntimeError as exc:
        if 'S2_DIMENSION_MISS' not in str(exc) or job['payload'].get('full_dimensions'): raise
        job=full_dimension_fallback(service,database,job,report,implementation)
        loaded=engine.load_and_publish(service,database,job,workers,stop_after,report) if implementation else base.load_chunks(service,database,job,workers,stop_after,report)
    report.update(shadow_job_id=job['job_id'],shadow_chunks_reused=loaded['reused_at_launch'],shadow_chunks_done=loaded['done'],shadow_chunks_total=loaded['total'],stage_peak_bytes_bound=int(loaded.get('max_chunk_bytes') or 0)*workers,s2_workers=workers,s2_chunk_kind=job['payload']['chunk_kind'],s2_chunk_rows=job['payload']['chunk_rows'])
    if loaded['done']!=loaded['total'] or (implementation and loaded.get('interrupted')): raise RuntimeError(f"CONTROLLED_SHADOW_INTERRUPTION: {loaded['done']}/{loaded['total']} chunks durable; public destinations remain unchanged")
    service._notify_progress(progress_callback,'execute',80,'S2 durable index and validation waves')
    if not implementation:
        publication_waves(service,database,job,workers,report)
    result=job.pop('_published_result', None)
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
 {shadow_transient_cleanup('j.job_id')}
 END IF; END LOOP;
IF NOT EXISTS(SELECT FROM {JOB_TABLE} WHERE source_oid=to_regclass({sql_literal(source)}) AND state IN ('loading','indexing','data_ready')) THEN
 EXECUTE format('DROP TRIGGER IF EXISTS pgdm_shadow_source_fence ON %s',to_regclass({sql_literal(source)}));
 END IF;
END $discard$;"""
    from services.expansion_recovery import remote_sql
    remote_sql(service,database,sql)
