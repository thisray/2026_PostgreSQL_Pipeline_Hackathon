"""Recognize and repair only the historical repeated table1 replay projection."""
import copy
import hashlib
import json

from services.expansion_recovery import fingerprint
from utils import sql_literal

TABLE1_PREFIX = 'INSERT INTO public.table1 (id_column_1,'
TABLE1_SELECT = '\nSELECT\n    source_row.raw_id,\n'
TABLE1_SEQUENCE = "\nSELECT setval(pg_get_serial_sequence('public.table1','id_column_1'),COALESCE((SELECT MAX(id_column_1) FROM public.table1),0)+1,false);"


def plan_signature(catalog, logical, plan, version, native_build_id=None):
    identity=dict(source=catalog['source'],database_oid=catalog['database_oid'],
        logical=logical,plan=plan,version=version)
    if native_build_id is not None:
        identity['native_build_id']=native_build_id
    return hashlib.sha256(json.dumps(identity,sort_keys=True).encode()).hexdigest()


def repeated_replay_equivalent(stored, canonical):
    if len(stored)!=len(canonical):
        return False
    changed=False
    for old,new in zip(stored,canonical):
        if old['table_name']!=new['table_name'] or old['statements']!=new['statements']:
            return False
        sql=new['sql_recipe']
        columns=new['destination_columns']
        if new['pure_table_name']=='table1':
            repeats=old['destination_columns'].count('id_column_1')
            if repeats not in (2,3,4):
                return False
            sql=sql.replace(TABLE1_PREFIX,'INSERT INTO public.table1 ('+'id_column_1,'*repeats,1)
            sql=sql.replace(TABLE1_SELECT,'\nSELECT\n'+'    source_row.raw_id,\n'*repeats,1)
            sql+=TABLE1_SEQUENCE*(repeats-1)
            columns=['id_column_1']*(repeats-1)+columns
            changed=True
        if old['sql_recipe']!=sql or old['destination_columns']!=columns:
            return False
    return changed


def resumable_job(catalog, prepared, versions, stable_plan, version, native_build_id=None):
    logical=fingerprint(prepared,versions)
    signature=plan_signature(catalog,logical,stable_plan,version,native_build_id)
    jobs=catalog.get('jobs') or []
    direct=next((j for j in jobs if j['signature']==signature),None)
    if direct:
        return signature,direct,None
    for job in jobs:
        payload=job['payload']
        if (job['state'] not in ('loading','indexing') or job['source_identity']!=catalog['source']
            or payload['base_signature']!=logical
            or not repeated_replay_equivalent(payload['destinations'],prepared['destinations'])):
            continue
        legacy=dict(stable_plan,recipes=[d['sql_recipe'] for d in payload['destinations']])
        if plan_signature(catalog,logical,legacy,version,native_build_id)==job['signature']:
            return signature,job,job['signature']
    return signature,None,None


def migrate_replay_metadata(service,database,job,signature,prepared,report,read_json):
    destinations=copy.deepcopy(job['payload']['destinations'])
    for old,new in zip(destinations,prepared['destinations']):
        for key in ('sql_recipe','original_sql_recipe','destination_columns'):
            if key in new:
                old[key]=copy.deepcopy(new[key])
    previous=job['signature']
    migration=dict(previous_signature=previous,reason='repeated_planner_table1_projection')
    patch=dict(destinations=destinations,replay_recipe_migration=migration)
    jid=sql_literal(job['job_id'])
    sql=f"""BEGIN;
SELECT pg_advisory_xact_lock(hashtextextended('pgdm-replay-migration:'||{jid},0));
DO $quiet$ BEGIN
 IF EXISTS(SELECT FROM pg_stat_activity WHERE pid<>pg_backend_pid()
  AND application_name LIKE '%'||{jid}||'%') THEN
  RAISE EXCEPTION 'Legacy generation still has live backends; wait before retrying Expand';
 END IF;
END $quiet$;
UPDATE pgdm_runtime.shadow_jobs SET signature={sql_literal(signature)},
 payload=payload||{sql_literal(json.dumps(patch))}::jsonb
WHERE job_id={jid} AND signature={sql_literal(previous)} AND state IN ('loading','indexing');
SELECT row_to_json(j) FROM pgdm_runtime.shadow_jobs j WHERE job_id={jid};
COMMIT;"""
    repaired=read_json(service,database,sql,report,'Normalize legacy replay metadata without changing committed chunks')
    if not repaired or repaired['signature']!=signature:
        raise RuntimeError('Legacy generation changed during replay metadata repair; retry Expand')
    report['replay_recipe_migration']=migration
    return repaired
