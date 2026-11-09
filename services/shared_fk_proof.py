"""Shared actual-index scans with protected, immutable FK epoch metadata."""
import math
import re

from services import shadow_expansion as base
from services import s3_publication as waves
from utils import sql_literal

POLICY = 'shared_index_complete_fk_v1'
GRAPH = {
 'table1_id_column_3_fkey': ('table1','id_column_3','raw_data','raw_id'),
 'table1_id_column_4_fkey': ('table1','id_column_4','table3','id_column_1'),
 'table2_id_column_2_fkey': ('table2','id_column_2','table1','id_column_1'),
 'table5_id_column_2_fkey': ('table5','id_column_2','table1','id_column_1'),
 'table6_id_column_2_fkey': ('table6','id_column_2','table1','id_column_1'),
 'table6_id_column_3_fkey': ('table6','id_column_3','table4','id_column_1'),
}


def supported(catalog, native_bulk):
    if not native_bulk or not 180000 <= int(catalog.get('server_version_num',0)) < 190000:
        return False
    keys = [k for k in catalog['constraints'] if k['type']=='f']
    if len(keys)!=len(GRAPH):
        return False
    for key in keys:
        expected=GRAPH.get(key['name'])
        if expected is None:
            return False
        child,column,parent,parent_column=expected
        definition=(f'FOREIGN KEY ({column}) REFERENCES public.{parent}({parent_column}) '
                    'ON UPDATE CASCADE ON DELETE CASCADE')
        if key['table']!=child or key['definition']!=definition:
            return False
    return True


def required(job):
    return bool(job.get('_shared_fk_required') or job['payload'].get('shared_fk_proof'))


def seal_sql(job):
    if not required(job):
        return ''
    return f"""UPDATE {base.JOB_TABLE} SET payload=jsonb_set(payload,'{{shared_fk_proof}}',
 jsonb_build_object('policy',{sql_literal(POLICY)},'sealed',
 public.pgdm_fk_proof_seal({sql_literal(job['job_id'])},{sql_literal(job['schema_name'])})))
WHERE job_id={sql_literal(job['job_id'])};"""



def dimensions_sql(job):
    if not required(job):
        return ''
    return f"DO $shared_fk_dimensions$ BEGIN PERFORM public.pgdm_fk_proof_dimensions({sql_literal(job['job_id'])}); END $shared_fk_dimensions$;"


def publish_sql(job):
    if not required(job):
        return ''
    return f"DO $shared_fk_publish$ BEGIN PERFORM public.pgdm_fk_proof_publish({sql_literal(job['job_id'])},{sql_literal(job['schema_name'])}); END $shared_fk_publish$;\n"


def rewrite_dag(job,tasks,specs,rows,shared_total):
    if not required(job):
        return
    schema=job['schema_name']
    replacements={}
    for table in ('table1','table2','table5','table6'):
        members=[spec for spec in specs.values() if spec['table']==table]
        expected=3 if table=='table5' else 2
        if len(members)!=expected or any('index:'+s['name'] not in tasks for s in members):
            continue
        constraints=job['payload']['catalog']['constraints']
        primary=next((k['index_name'] for k in constraints if k['table']==table and k['type']=='p'),None)
        def rank(spec):
            if spec['name']==primary:
                return 0
            return 2 if table=='table5' and re.search(r'IS NOT NULL\)\s*$',spec['definition']) else 1
        members.sort(key=rank)
        count=rows.get(table)
        if count is None or count<0:
            continue
        arrays=3 if table=='table5' else 2
        fields=2 if table in ('table1','table6') else 1
        needed=(int(count)*arrays*16 + (8+64*fields)*1048576 +1048575)//1048576
        memory=max(16,64*math.ceil(needed/64))
        if memory>shared_total:
            # The ordinary PK build keeps its grant and ordering. This explicit
            # extra heap scan is timed in that task and releases proof RAM
            # before core sorting starts. Bitmap storage grows by actual keys.
            primary_task=tasks['index:'+members[0]['name']]
            collect=(f'SELECT public.pgdm_fk_proof_collect('
                     f'{sql_literal(schema+"."+table)}::regclass,{sql_literal(table)}); ')
            primary_task['sql']=primary_task['sql'].replace('BEGIN;', 'BEGIN; '+collect, 1)
            primary_task['proof_policy']='independent_full_heap_scan'
            primary_task['proof_extra_scans']=1
            continue
        old_names={'index:'+s['name'] for s in members}
        originals=[tasks[name] for name in old_names]
        definitions=[re.sub(r'\bON public\.(table[1-6])\b',lambda m:'ON '+base.relation(schema,m.group(1)),s['definition']) for s in members]
        name='index:'+table+':shared'
        definitions_sql='ARRAY['+','.join(sql_literal(d) for d in definitions)+']::text[]'
        sql=(f'BEGIN; {waves._settings(memory,1,1)} '
             f'SELECT public.pgdm_build_verified_radix_bundle({sql_literal(schema+"."+table)}::regclass,'
             f'{definitions_sql},{sql_literal(table)},{int(count)},{memory}); COMMIT;')
        task=dict(originals[0],name=name,sql=sql,memory_mb=memory,cost=sum(t['cost'] for t in originals),
                  deps=set().union(*(t['deps'] for t in originals))-old_names,
                  priority_predecessors=set().union(*(t['priority_predecessors'] for t in originals))-old_names,
                  reservations=[],window_policy=None,parallel_workers=0,
                  memory_policy=dict(tier='actual_shared_radix',actual_rows=int(count),row_arrays=arrays,
                                     bitmap_reserve_mb=64*fields,writer_reserve_mb=8,requested_mb=memory))
        for old in old_names:
            del tasks[old]
            replacements[old]=name
        tasks[name]=task
    def replace(value):
        if isinstance(value,str):
            return replacements.get(value,value)
        if isinstance(value,set):
            return {replace(v) for v in value}
        if isinstance(value,list):
            return [replace(v) for v in value]
        if isinstance(value,dict):
            return {k:replace(v) for k,v in value.items()}
        return value
    for task in tasks.values():
        for key in ('deps','priority_predecessors','reservations','window_policy'):
            task[key]=replace(task[key])
        if task['kind']=='validate':
            task['sql']=task['sql'].replace('BEGIN;',f"BEGIN; SELECT public.pgdm_fk_proof_enable({sql_literal(job['job_id'])});")
