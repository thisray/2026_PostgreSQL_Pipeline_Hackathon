"""Persist a complete post-data result and finalize versions idempotently."""
import hashlib
import json
import shlex

from services.expansion_wal import SESSION_WAL_SQL
from services.expansion_optimizer import timed_sql
from utils import sql_literal


def remote_sql(service, database, sql):
    command = f'psql -h localhost -U {shlex.quote(service.sql_username)} -d {shlex.quote(database)} -XqAt -v ON_ERROR_STOP=1 -f -'
    return service.run_remote_command(command, stdin_text=SESSION_WAL_SQL + sql)


def fingerprint(prepared, source_versions):
    value = {'source': [prepared['source_schema_name'], prepared['source_table_name']],
             'scope': prepared['raw_schemas'], 'versions': source_versions,
             'recipes': [d['statements'] for d in prepared['destinations']]}
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def pending_result(service, database, signature):
    exists = remote_sql(service, database, "SELECT to_regclass('pgdm_runtime.expansion_jobs') IS NOT NULL;").strip()
    if exists != 't':
        return None
    output = remote_sql(service, database, f"SELECT payload::text FROM pgdm_runtime.expansion_jobs WHERE fingerprint={sql_literal(signature)} AND state='data_ready' ORDER BY created_at DESC LIMIT 1;").strip()
    return json.loads(output) if output else None


def build_outbox(job_id, signature, payload, manifest_sql):
    manifest_query = 'SELECT json_build_object(' + manifest_sql.split('SELECT json_build_object(', 1)[1].strip().rstrip(';')
    body = f"""
CREATE SCHEMA IF NOT EXISTS pgdm_runtime;
CREATE TABLE IF NOT EXISTS pgdm_runtime.expansion_jobs(
 job_id text PRIMARY KEY, fingerprint text NOT NULL, state text NOT NULL,
 payload jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 completed_at timestamptz
);
INSERT INTO pgdm_runtime.expansion_jobs(job_id,fingerprint,state,payload)
SELECT {sql_literal(job_id)},{sql_literal(signature)},'data_ready',
 {sql_literal(json.dumps(payload, ensure_ascii=True))}::jsonb
 || jsonb_build_object('dependency_manifest',({manifest_query})::jsonb,
 'execution_timings',(SELECT jsonb_agg(jsonb_build_object(
 'stage',stage_key,'name',item_name,'duration_seconds',EXTRACT(EPOCH FROM (finished_at-started_at)),
 'rows',rows_affected,'destination',destination_name) ORDER BY sequence_number)
 FROM pg_temp.pgdm_expansion_timings WHERE finished_at IS NOT NULL));
"""
    return timed_sql(8000, 'Persist data-ready outbox and complete replay metadata', body)


def finalization_guard(job_id, control_schema):
    table = f'"{control_schema}".expansion_finalizations'
    setup = f"""
CREATE TABLE IF NOT EXISTS {table}(job_id text PRIMARY KEY,result jsonb NOT NULL);
SELECT pg_advisory_xact_lock(hashtextextended({sql_literal('expansion-finalize:'+job_id)},0));
SELECT EXISTS(SELECT FROM {table} WHERE job_id={sql_literal(job_id)}) AS pgdm_already_finalized
\\gset
"""
    return timed_sql(-10, 'Prepare and lock idempotent version finalization', setup, 'version') + f"""
\\if :pgdm_already_finalized
SELECT jsonb_array_elements(result)::text FROM {table} WHERE job_id={sql_literal(job_id)};
\\else
"""


def finalization_receipt(job_id, control_schema):
    schema = f'"{control_schema}"'
    body = f"""
INSERT INTO {schema}.expansion_finalizations(job_id,result)
SELECT {sql_literal(job_id)},jsonb_agg(jsonb_build_object(
 'table_name',v.schema_name||'.'||v.table_name,'version_code',v.version_code,'dependency_count',1)
 ORDER BY v.schema_name,v.table_name)
FROM {schema}.table_versions v JOIN {schema}.table_version_dependencies d ON d.table_version_id=v.id
WHERE d.dependency_payload->>'expansion_job_id'={sql_literal(job_id)};
"""
    return timed_sql(8000, 'Persist atomic version finalization receipt', body, 'version') + '\n\\endif\n'


def shadow_transient_cleanup(job_expression):
    """Remove executable/staging work; retain durable recovery audit records.

    The expression is generated internally inside a PL/pgSQL block. Call only
    for a terminal job after workers have stopped or version finalization has
    succeeded. Keep this in the terminal-state transaction so retries are safe.
    """
    return f"""
EXECUTE format('DROP PROCEDURE IF EXISTS pgdm_runtime.%I()', 'worker_'||{job_expression});
IF to_regclass('pgdm_runtime.shadow_codes') IS NOT NULL THEN
 EXECUTE 'DELETE FROM pgdm_runtime.shadow_codes WHERE job_id=$1' USING {job_expression};
END IF;
"""


def terminal_shadow_cleanup():
    """Also collect terminal work left by older application versions."""
    return f"""DO $cleanup$ DECLARE j record; BEGIN
 FOR j IN SELECT * FROM pgdm_runtime.shadow_jobs WHERE state IN ('complete','aborted') LOOP
 IF NOT EXISTS(SELECT FROM pg_stat_activity WHERE application_name IN ('pgdm_shadow:'||j.job_id,'pgdm_shadow_control:'||j.job_id)) THEN
 EXECUTE format('DROP SCHEMA IF EXISTS %I CASCADE',j.schema_name);
 {shadow_transient_cleanup('j.job_id')}
 END IF; END LOOP; END $cleanup$;
"""


def mark_complete(service, database, job_id, reload_catalog=False):
    output = remote_sql(service, database, f"""
BEGIN;
UPDATE pgdm_runtime.expansion_jobs SET state='complete',completed_at=clock_timestamp() WHERE job_id={sql_literal(job_id)};
DO $shadow_complete$
DECLARE source_id oid;
BEGIN
 IF to_regclass('pgdm_runtime.shadow_jobs') IS NOT NULL THEN
  UPDATE pgdm_runtime.shadow_jobs SET state='complete'
  WHERE job_id={sql_literal(job_id)} AND state='data_ready' RETURNING source_oid INTO source_id;
  IF source_id IS NOT NULL THEN
   EXECUTE format('DROP TRIGGER IF EXISTS pgdm_shadow_source_fence ON %s',source_id::regclass);
  END IF;
  IF EXISTS(SELECT FROM pgdm_runtime.shadow_jobs WHERE job_id={sql_literal(job_id)} AND state='complete') THEN
   {shadow_transient_cleanup(sql_literal(job_id))}
  END IF;
 END IF;
END $shadow_complete$;
COMMIT;
""" + (service.list_tables(database, sql_only=True) if reload_catalog else ""))
    service._pending_expansion_job_id = None
    if reload_catalog:
        return [line.strip() for line in output.splitlines() if line.strip()]


def restore_report(service, payload, current_report):
    for item in payload.get('execution_timings') or []:
        service._append_expansion_timing(current_report, item['stage'], item['name'],
            float(item.get('duration_seconds') or 0), 'postgresql_backend_wall',
            rows=item.get('rows'), destination=item.get('destination'),
            details='Previously committed work recovered from the data outbox; commit/outbox tail timing unavailable after interruption.')
    current_report['source_rows'] = payload['dependency_manifest']['expected_row_count']
    current_report['destination_rows'] = [
        {'table_name': d['table_name'], 'rows': sum(int(t.get('rows') or 0) for t in payload.get('execution_timings') or [] if t.get('destination') == d['table_name'])}
        for d in payload['destinations']]
    current_report['total_rows_inserted'] = sum(d['rows'] for d in current_report['destination_rows'])
    current_report['recovered_data_ready_job'] = True
    current_report['historical_timing_complete'] = False
    payload['timing_report'] = current_report
    return payload
