"""Completion-driven publication of the unchanged shadow table schema."""
import re
import shlex

from services.expansion_wal import SESSION_WAL_SQL
from services import shadow_expansion as base
from utils import sql_ident, sql_literal


def parallel_wave(service, database, job, statements, workers, report, name):
    """Refill every free slot; a statement may contain durable transactions."""
    statements = [SESSION_WAL_SQL + sql for sql in statements]
    if not statements:
        return
    workers = int(workers)
    if not 1 <= workers <= 32:
        raise ValueError('Publication workers must be between 1 and 32')
    job_id = job['job_id']
    if not re.fullmatch(r'[a-f0-9]{32}', job_id):
        raise ValueError('Invalid shadow job ID')
    command = ['psql', '-h', 'localhost', '-U', service.sql_username,
               '-d', database, '-XqAt', '-v', 'ON_ERROR_STOP=1']
    application = 'pgdm_shadow:' + job_id
    cancel_sql = ("SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE "
                  f"application_name={sql_literal(application)} AND state='active' "
                  "AND pid<>pg_backend_pid();")
    sqls = '\n'.join(shlex.quote(sql) for sql in statements)
    script = f'''set -euo pipefail
export PGAPPNAME={shlex.quote(application)}
psql_cmd=({' '.join(shlex.quote(arg) for arg in command)})
exec 9>/tmp/pgdm-shadow-{job_id}.lock
flock -x 9
limit={min(workers, len(statements))}
failed=0
pids=()
stop_workers() {{
  "${{psql_cmd[@]}}" -c {shlex.quote(cancel_sql)} </dev/null >/dev/null 2>&1 || true
  for pid in "${{pids[@]}}"; do kill -TERM "$pid" 2>/dev/null || true; done
  wait || true
}}
trap 'stop_workers; exit 130' HUP INT TERM
reap_finished() {{
  local pid
  local -a active=()
  for pid in "${{pids[@]}}"; do
    if kill -0 "$pid" 2>/dev/null; then
      active+=("$pid")
    else
      if ! wait "$pid"; then failed=1; fi
    fi
  done
  pids=("${{active[@]}}")
}}
await_completion() {{
  local status=0 before=${{#pids[@]}}
  reap_finished
  if ((${{#pids[@]}}<before)); then return; fi
  if ((${{#pids[@]}}>0)); then
    # A child can finish between reap_finished and wait. With no unwaited
    # child, wait -n returns 127; the explicit wait in reap_finished still
    # retrieves every child's real status, including an actual exit 127.
    wait -n || status=$?
    if ((status!=0 && status!=127)); then failed=1; fi
    reap_finished
  fi
}}
sqls=(
{sqls}
)
for sql in "${{sqls[@]}}"; do
  while ((${{#pids[@]}}>=limit)); do await_completion; done
  if ((failed)); then break; fi
  "${{psql_cmd[@]}}" -c "$sql" </dev/null &
  pids+=("$!")
done
while ((${{#pids[@]}}>0)); do await_completion; done
exit "$failed"
'''
    with service.expansion_timing_scope(report, 'execute', name,
                                        record_local_cpu=False):
        service.run_remote_command('bash -s', stdin_text=script)


def _settings(total_mb, workers, task_count):
    active = min(int(workers), max(1, task_count))
    per_task = max(1, total_mb // active)
    # FK RI_Initial_Check replaces work_mem with maintenance_work_mem. Budget
    # that setting explicitly, and avoid multiplying the outer worker count.
    return ("SET LOCAL jit=off; SET LOCAL max_parallel_maintenance_workers=0; "
            "SET LOCAL max_parallel_workers_per_gather=0; "
            f"SET LOCAL maintenance_work_mem='{per_task}MB'; "
            f"SET LOCAL work_mem='{min(per_task, 64)}MB'; "
            "SET LOCAL hash_mem_multiplier=1;")


def _parent_table(job, constraint):
    return next((name for name, oid in job['target_identity'].items()
                 if str(oid) == str(constraint['referenced_oid'])), None)
