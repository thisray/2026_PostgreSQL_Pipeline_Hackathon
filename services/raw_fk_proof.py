"""Inserted-tuple FK evidence with normal PostgreSQL validation on any miss."""
from services import shadow_expansion as base
from utils import sql_ident, sql_literal

POLICY = 'raw_fk_tuple_witness_v2'
DEFINITION = ('FOREIGN KEY (id_column_3) REFERENCES public.raw_data(raw_id) '
              'ON UPDATE CASCADE ON DELETE CASCADE')
NAME = 'table1_id_column_3_fkey'


def plan(catalog, native_bulk):
    if not native_bulk:
        return None
    keys = [k for k in catalog['constraints'] if k['table'] == 'table1'
            and k['type'] == 'f' and k.get('reference_schema') == 'public'
            and k.get('reference_table') == 'raw_data']
    if (len(keys) != 1 or keys[0]['name'] != NAME
            or keys[0]['definition'] != DEFINITION
            or keys[0].get('referenced_oid') != catalog['source']['oid']):
        return None
    return dict(policy=POLICY, constraint_name=NAME, definition=DEFINITION,
                source_oid=catalog['source']['oid'])



def initialize_sql(job):
    if not job['payload'].get('raw_fk_proof'):
        return ''
    child = sql_literal(job['schema_name'] + '.table1')
    return f"""UPDATE {base.JOB_TABLE} SET payload=jsonb_set(payload,'{{raw_fk_proof}}',
 payload->'raw_fk_proof' || jsonb_build_object(
 'target_oid',{child}::regclass::oid,
 'source_filenode',pg_relation_filenode('public.raw_data'),
 'target_filenode',pg_relation_filenode({child}::regclass),
 'constraint_oid',(SELECT oid FROM pg_constraint WHERE conrelid={child}::regclass AND conname={sql_literal(NAME)}),
 'source_fence_oid',(SELECT oid FROM pg_trigger WHERE tgrelid='public.raw_data'::regclass AND tgname={sql_literal(base.FENCE)}),
 'source_fence_xmin',(SELECT xmin::text FROM pg_trigger WHERE tgrelid='public.raw_data'::regclass AND tgname={sql_literal(base.FENCE)}),
 'source_function_oid',(SELECT tgfoid FROM pg_trigger WHERE tgrelid='public.raw_data'::regclass AND tgname={sql_literal(base.FENCE)}),
 'source_function_xmin',(SELECT p.xmin::text FROM pg_proc p JOIN pg_trigger t ON t.tgfoid=p.oid WHERE t.tgrelid='public.raw_data'::regclass AND t.tgname={sql_literal(base.FENCE)})))
WHERE job_id={sql_literal(job['job_id'])};"""


def worker_check_sql(job):
    if not job['payload'].get('raw_fk_proof'):
        return ''
    return f"""IF bulk_stats.manifest->'raw_fk_proof'->>'policy' IS DISTINCT FROM {sql_literal(POLICY)}
 OR (bulk_stats.manifest->'raw_fk_proof'->>'verified_rows')::bigint IS DISTINCT FROM bulk_stats.table1_rows
 OR (bulk_stats.manifest->'raw_fk_proof'->>'inserted_rows')::bigint IS DISTINCT FROM bulk_stats.table1_rows
 THEN RAISE EXCEPTION 'Native raw FK tuple witness incomplete; chunk aborted'; END IF;"""


def guard_sql(job, *, final=False):
    """Downgrade an unproven early FK with DDL before the ordinary DAG validates."""
    if not job['payload'].get('raw_fk_proof') and not job.get('_raw_fk_required'):
        return ''
    child = base.relation(job['schema_name'], 'table1')
    child_literal = sql_literal(job['schema_name'] + '.table1')
    jid = sql_literal(job['job_id'])
    # All fallback work is timed. A post-snapshot miss validates in this transaction.
    validate = f'ALTER TABLE {child} VALIDATE CONSTRAINT {sql_ident(NAME)};' if final else ''
    return f"""LOCK TABLE public.raw_data IN ROW SHARE MODE;
LOCK TABLE {child} IN SHARE MODE;
DO $raw_fk_proof$
DECLARE expected jsonb; job_payload jsonb; valid boolean; key_id oid;
 previous_path text:=current_setting('search_path'); child_id oid; source_id oid;
BEGIN
 PERFORM set_config('search_path','pg_catalog',true);
 SELECT payload INTO STRICT job_payload FROM {base.JOB_TABLE} WHERE job_id={jid};
 expected:=job_payload->'raw_fk_proof';
 IF jsonb_typeof(expected) IS DISTINCT FROM 'object' THEN expected:='{{}}'::jsonb; END IF;
 child_id:={child_literal}::regclass; source_id:='public.raw_data'::regclass;
 SELECT oid INTO key_id FROM pg_constraint WHERE conrelid=child_id AND conname={sql_literal(NAME)};
 valid:=EXISTS(SELECT FROM pg_constraint k JOIN pg_index i ON i.indexrelid=k.conindid
 WHERE k.oid=key_id AND k.contype='f' AND k.conrelid=child_id AND k.confrelid=source_id
 AND k.convalidated AND NOT k.condeferrable AND NOT k.condeferred
 AND k.conkey=ARRAY[10]::smallint[] AND k.confkey=ARRAY[1]::smallint[]
 AND k.confmatchtype='s' AND k.confupdtype='c' AND k.confdeltype='c'
 AND i.indrelid=source_id AND i.indisunique AND i.indisvalid AND i.indisready
 AND pg_get_constraintdef(k.oid)={sql_literal(DEFINITION)});
 BEGIN
   valid:=valid AND expected->>'policy'={sql_literal(POLICY)}
    AND expected->>'definition'={sql_literal(DEFINITION)}
    AND (expected->>'constraint_oid')::oid=key_id
    AND (expected->>'source_oid')::oid=source_id
    AND (expected->>'target_oid')::oid=child_id
    AND (expected->>'source_filenode')::oid=pg_relation_filenode(source_id)
    AND (expected->>'target_filenode')::oid=pg_relation_filenode(child_id)
    AND EXISTS(SELECT FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid
     WHERE t.oid=(expected->>'source_fence_oid')::oid AND t.tgrelid=source_id
     AND t.tgname={sql_literal(base.FENCE)} AND t.tgenabled='O' AND t.tgtype=62
     AND t.xmin::text=expected->>'source_fence_xmin'
     AND p.oid=(expected->>'source_function_oid')::oid
     AND p.xmin::text=expected->>'source_function_xmin'
     AND p.pronargs=0 AND p.prorettype='trigger'::regtype AND p.prokind='f'
     AND (SELECT lanname FROM pg_language WHERE oid=p.prolang)='plpgsql'
     AND regexp_replace(p.prosrc,'[[:space:]]','','g')=
       'BEGINRAISEEXCEPTION''Sourceisfrozenbyaresumableshadowexpansion'';END'
     AND NOT t.tgisinternal AND t.tgnargs=0 AND t.tgattr=''::int2vector
     AND t.tgqual IS NULL AND p.pronamespace='pgdm_runtime'::regnamespace
     AND p.proname='reject_shadow_source_write')
    AND (SELECT count(*) FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid
     WHERE t.tgconstraint=key_id AND t.tgisinternal AND t.tgenabled='O'
     AND NOT t.tgdeferrable AND NOT t.tginitdeferred AND t.tgnargs=0
     AND t.tgattr=''::int2vector AND t.tgqual IS NULL
     AND p.pronamespace='pg_catalog'::regnamespace
     AND ((t.tgrelid=child_id AND t.tgconstrrelid=source_id
      AND ((p.proname='RI_FKey_check_ins' AND t.tgtype=5)
        OR (p.proname='RI_FKey_check_upd' AND t.tgtype=17)))
      OR (t.tgrelid=source_id AND t.tgconstrrelid=child_id
      AND ((p.proname='RI_FKey_cascade_del' AND t.tgtype=9)
        OR (p.proname='RI_FKey_cascade_upd' AND t.tgtype=17)))))=4
    AND (SELECT count(*) FROM pg_trigger WHERE tgconstraint=key_id)=4
    AND NOT EXISTS(SELECT FROM {base.CHUNK_TABLE} c WHERE c.job_id={jid} AND (
     c.state IS DISTINCT FROM 'done' OR c.committed_at IS NULL
     OR c.row_counts->'_raw_fk_proof'->>'policy' IS DISTINCT FROM {sql_literal(POLICY)}
     OR c.row_counts->'_raw_fk_proof'->>'build_id' IS DISTINCT FROM job_payload->>'native_build_id'
     OR c.row_counts->'_raw_fk_proof'->>'definition' IS DISTINCT FROM {sql_literal(DEFINITION)}
     OR (c.row_counts->'_raw_fk_proof'->>'source_oid')::oid IS DISTINCT FROM source_id
     OR (c.row_counts->'_raw_fk_proof'->>'target_oid')::oid IS DISTINCT FROM child_id
     OR (c.row_counts->'_raw_fk_proof'->>'constraint_oid')::oid IS DISTINCT FROM key_id
     OR (c.row_counts->'_raw_fk_proof'->>'source_fence_oid')::oid IS DISTINCT FROM (expected->>'source_fence_oid')::oid
     OR (c.row_counts->'_raw_fk_proof'->>'source_filenode')::oid IS DISTINCT FROM pg_relation_filenode(source_id)
     OR (c.row_counts->'_raw_fk_proof'->>'target_filenode')::oid IS DISTINCT FROM pg_relation_filenode(child_id)
     OR (c.row_counts->'_raw_fk_proof'->>'verified_rows')::bigint IS DISTINCT FROM (c.row_counts->>'table1')::bigint
     OR (c.row_counts->'_raw_fk_proof'->>'inserted_rows')::bigint IS DISTINCT FROM (c.row_counts->>'table1')::bigint
     OR (c.row_counts->>'table1')::bigint IS NULL OR (c.row_counts->>'table1')::bigint<0));
  EXCEPTION WHEN data_exception THEN valid:=false;
  END;
  IF valid IS DISTINCT FROM true THEN
   ALTER TABLE {child} DROP CONSTRAINT IF EXISTS {sql_ident(NAME)};
   ALTER TABLE {child} ADD CONSTRAINT {sql_ident(NAME)} {DEFINITION} NOT VALID;
   UPDATE {base.JOB_TABLE} SET payload=jsonb_set(payload,'{{raw_fk_proof}}',
    COALESCE(expected,'{{}}'::jsonb)||'{{"fallback":true,"reason":"proof_precondition_mismatch"}}'::jsonb)
   WHERE job_id={jid};
   {validate}
  END IF;
 PERFORM set_config('search_path',previous_path,true);
END $raw_fk_proof$;
"""
