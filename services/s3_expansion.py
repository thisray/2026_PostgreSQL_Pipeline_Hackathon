"""Portable SQL-only sparse shadow engine with bounded publication queues."""
import base64
import json
import os
import re
import shlex
import sys
from pathlib import Path

from services.expansion_wal import SESSION_WAL_BODY
from services import s2_expansion as s2
from services import shadow_expansion as base
from services.publication_memory import MEMORY_PROBE
from services.chunk_workers import choose_workers
from utils import sql_ident, sql_literal

PLAN_VERSION = 's3-sql-rc9-exact-pages-v1'
CODE_TABLE = 'pgdm_runtime.shadow_codes'
relation = base.relation

# Run inside the psql client's namespace. This also handles project Docker wrappers.
# A native local server uses its backend's service cgroup, rather than the SSH cgroup.
CPU_PROBE = 'printf %s ' + base64.b64encode(MEMORY_PROBE.encode()).decode('ascii') + ' | base64 -d | sh'
EXTRA_INFRASTRUCTURE = f"""
CREATE TABLE IF NOT EXISTS {CODE_TABLE}(
 job_id text NOT NULL,chunk_id integer NOT NULL,code text NOT NULL,
 PRIMARY KEY(job_id,chunk_id,code));
SELECT pg_backend_pid() AS pgdm_backend_pid \\gset
\\setenv PGDM_BACKEND_PID :pgdm_backend_pid
\\! sh -c {shlex.quote(CPU_PROBE)}
"""


def prepare_command(role, database):
    return f'psql -h localhost -U {shlex.quote(role)} -d {shlex.quote(database)} -XqAt -v ON_ERROR_STOP=1 -f -'


def dimension_phase(schema, scope, full=False):
    sql = s2.dimension_phase(schema, scope, full)
    return sql[:sql.index(f"INSERT INTO {relation(schema,'table4')}")] + 'SET LOCAL enable_sort=on;\n'


def compact_select(schema, scope, tid, keep_source_identity=False):
    sql = s2.compact_select(schema, scope, tid)
    if not keep_source_identity:
        sql = sql.replace('r.raw_id,r.raw_schema,r.raw_hash,', 'r.raw_id,', 1)
    sql = sql.replace("b.ref,b.code2,b.code3,(b.ref<>'' AND b.code2 ~ '^[A-Z]{2}$') parent_ok,",
                      "b.code3,((b.ref<>'' AND b.code2 ~ '^[A-Z]{2}$') IS TRUE AND d3.id_column_1 IS NULL) dim3_miss,")
    sql = sql.replace('d3.id_column_1 dim3_id,d4.id_column_1 dim4_id,',
                      "d3.id_column_1 dim3_id,CASE WHEN b.code3 ~ '^[A-Z]{2}$' THEN ((ASCII(LEFT(b.code3,1))-65)*26+ASCII(RIGHT(b.code3,1))-65+1)::smallint END dim4_id,")
    sql = re.sub(r'LEFT JOIN .*? d4 ON .*?\nWHERE', '\nWHERE', sql, count=1)
    return sql


def exact_page_readback(sql, schema, collection_memory_mb=None):
    """Keep INSERT SELECTs intact and collect their actual heap block numbers."""
    sql = sql.replace('stage_size bigint;', """stage_size bigint;
 tick timestamptz;hint_tick timestamptz;profile jsonb;pages jsonb;written bigint[];
 t text;page_range record;visible_rows bigint;read_rows bigint;range_count integer;
 hint_needed boolean:=current_setting('data_checksums')='on' OR current_setting('wal_log_hints')='on';""", 1)
    sql = sql.replace(' CREATE TEMP TABLE pgdm_compact_stage',
                      " profile:='{}'::jsonb;pages:='{}'::jsonb;\n tick:=clock_timestamp();\n CREATE TEMP TABLE pgdm_compact_stage", 1)
    sql = sql.replace(' IF EXISTS',
                      " profile:=jsonb_build_object('decode_stage_s',EXTRACT(EPOCH FROM clock_timestamp()-tick));\n IF EXISTS", 1)
    for table, counter in [('table1','n1'),('table5','n5'),('table6','n6'),('table2','n2')]:
        pattern = r'INSERT INTO ' + re.escape(relation(schema, table)) + r'.*?;\s*GET DIAGNOSTICS ' + counter + r'=ROW_COUNT;'
        match = re.search(pattern, sql, re.DOTALL)
        if match is None:
            raise ValueError('Missing SQL shadow INSERT: ' + table)
        original = match.group()
        insert = original[:original.index(';')]
        # The grouped counts preserve ROW_COUNT, including an empty INSERT.
        # Only returned TIDs identify pages; concurrent workers may share them.
        memory = ("PERFORM set_config('work_mem',GREATEST("
                  "pg_size_bytes(current_setting('work_mem'))/1024,"
                  f"{int(collection_memory_mb) * 1024})::text||'kB',true);\n "
                  if collection_memory_mb is not None else '')
        wrapped = f"""tick:=clock_timestamp();
 IF hint_needed THEN
 {memory}WITH inserted AS ({insert} RETURNING ctid)
 SELECT COALESCE(sum(rows),0)::bigint,COALESCE(array_agg(block ORDER BY block),'{{}}'::bigint[])
 INTO {counter},written FROM (
  SELECT split_part(substr(ctid::text,2),',',1)::bigint AS block,count(*) AS rows
  FROM inserted GROUP BY 1) blocks;
 pages:=pages || jsonb_build_object('{table}',to_jsonb(written));
 ELSE
 {original}
 END IF;
 profile:=profile || jsonb_build_object('{table}_insert_collect_s',EXTRACT(EPOCH FROM clock_timestamp()-tick));"""
        sql = sql[:match.start()] + wrapped + sql[match.end():]
    sql = sql.replace("'table5',n5,'table6',n6)", "'table5',n5,'table6',n6,'_profile',profile)", 1)
    # The original data/manifest/done commit remains the recovery authority.
    # A lost read-back never makes a committed chunk pending again.
    sql = sql.replace(' COMMIT;\n END LOOP;', f""" COMMIT;
 IF hint_needed THEN
 hint_tick:=clock_timestamp();
 PERFORM set_config('enable_seqscan','off',true);
 PERFORM set_config('enable_tidscan','on',true);
 FOREACH t IN ARRAY ARRAY['table1','table5','table6','table2'] LOOP
  tick:=clock_timestamp();read_rows:=0;range_count:=0;
  FOR page_range IN
   SELECT min(block) AS lo,max(block) AS hi FROM (
    SELECT block,block-row_number() OVER (ORDER BY block) AS island
    FROM (SELECT jsonb_array_elements_text(pages->t)::bigint AS block) blocks
   ) islands GROUP BY island ORDER BY min(block)
  LOOP
   EXECUTE format('SELECT count(*) FROM %I.%I WHERE ctid >= $1 AND ctid < $2',{sql_literal(schema)},t)
    INTO visible_rows USING format('(%s,0)',page_range.lo)::tid,format('(%s,0)',page_range.hi+1)::tid;
   read_rows:=read_rows+visible_rows;range_count:=range_count+1;
  END LOOP;
  profile:=profile || jsonb_build_object(t||'_readback_s',EXTRACT(EPOCH FROM clock_timestamp()-tick),
   t||'_readback_pages',jsonb_array_length(pages->t),t||'_readback_ranges',range_count,t||'_readback_rows',read_rows);
 END LOOP;
 profile:=profile || jsonb_build_object('readback_s',EXTRACT(EPOCH FROM clock_timestamp()-hint_tick));
 UPDATE {s2.CHUNK_TABLE} SET row_counts=jsonb_set(row_counts,'{{_profile}}',profile)
 WHERE job_id=c.job_id AND chunk_id=c.chunk_id;
 COMMIT;
 END IF;
 END LOOP;""", 1)
    return sql


def worker_procedure(job, plan, prepared, hint_readback=True):
    sql = s2.worker_procedure(job, plan, prepared)
    sql = sql.replace("BEGIN\n PERFORM set_config('application_name'", "BEGIN\n " + SESSION_WAL_BODY + "\n PERFORM set_config('application_name'", 1)
    sql = sql.replace(" PERFORM set_config('enable_memoize','off',true);",
                      " PERFORM set_config('enable_memoize','off',true); PERFORM set_config('jit','off',true);")
    start = sql.index(' CREATE TEMP TABLE pgdm_compact_stage')
    end = sql.index(' IF EXISTS', start)
    scope = ','.join(sql_literal(v) for v in prepared['raw_schemas'])
    tid = job.get('payload', {}).get('chunk_kind', 'tid') == 'tid'
    small_manifest = job.get('payload', {}).get('small_manifest_from_stage', False)
    keep_identity = job.get('payload', {}).get('manifest_from_stage', small_manifest)
    sql = sql[:start] + f' CREATE TEMP TABLE pgdm_compact_stage ON COMMIT DROP AS {compact_select(job["schema_name"],scope,tid,keep_identity)};\n' + sql[end:]
    start = sql.index(' IF EXISTS')
    end = sql.index(' RAISE EXCEPTION', start)
    sql = sql[:start] + ' IF EXISTS(SELECT FROM pg_temp.pgdm_compact_stage WHERE dim3_miss) THEN\n' + sql[end:]
    sql = sql.replace('p.pos,s.raw_id,p.pos,p.weight,p.type_code', 's.slot_pos[p.idx],s.raw_id,s.slot_pos[p.idx],s.slot_weight[p.idx],s.slot_type[p.idx]')
    sql = sql.replace('NULL::integer,p.gap', 'NULL::integer,s.slot_gap[p.idx]')
    sql = sql.replace('unnest(s.slot_pos,s.slot_weight,s.slot_type,s.slot_gap) p(pos,weight,type_code,gap)',
                      'generate_series(1,LEAST(14,GREATEST(COALESCE(s.max_declared,0),0))) p(idx)')
    sql = sql.replace('AND p.pos IS NOT NULL', 'AND s.slot_pos[p.idx] IS NOT NULL')
    bounds = "r.ctid >= format('(%s,0)',c.lo)::tid AND r.ctid < format('(%s,0)',c.hi+1)::tid" if tid else 'r.raw_id BETWEEN c.lo AND c.hi'
    manifest_source = ('FROM pg_temp.pgdm_compact_stage GROUP BY raw_schema,raw_hash;' if keep_identity else
                       f'FROM public.raw_data r WHERE r.raw_schema IN ({scope}) AND {bounds} GROUP BY raw_schema,raw_hash;')
    sql = sql.replace('FROM pg_temp.pgdm_compact_stage GROUP BY raw_schema,raw_hash;',
                      manifest_source + '\n'
                      f' INSERT INTO {CODE_TABLE} SELECT {sql_literal(job["job_id"])},c.chunk_id,code3 FROM pg_temp.pgdm_compact_stage WHERE dim4_id IS NOT NULL GROUP BY code3;')
    # The RETURNING tuplestore uses work_mem, not temp_buffers. The stage
    # already fits 128 MiB; 32 MiB avoids collection spills inside the existing
    # additional 128 MiB executor allowance per admitted chunk worker.
    return exact_page_readback(sql, job['schema_name'],
        collection_memory_mb=32 if not small_manifest else None) if hint_readback else sql


def execute(service, database, prepared, requested, versions, report, progress_callback=None, post_commit_callback=None):
    return s2.execute(service, database, prepared, requested, versions, report,
                      progress_callback, post_commit_callback, implementation=sys.modules[__name__])


def load_and_publish(service, database, job, workers, stop_after, report):
    """SQL loading and the shared dependency-driven publication implementation."""
    from services.s3_publication_dag import publication_waves, load_chunks
    loaded = load_chunks(service, database, job, workers, stop_after, report)
    if loaded['done'] == loaded['total'] and not loaded.get('interrupted'):
        job['_published_result'] = publication_waves(service, database, job,
            report['chunk_worker_policy']['publication_workers'], report,
            publish=job.pop('_publish_sql_factory', None))
    return loaded
