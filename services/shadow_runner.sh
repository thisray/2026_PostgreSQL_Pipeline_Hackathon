#!/usr/bin/env bash
set -euo pipefail
database=$1
role=$2
job=$3
workers=$4
stop_after=$5
[[ $job =~ ^[a-f0-9]{32}$ ]] || exit 2
[[ $workers =~ ^[0-9]+$ ]] && ((workers>=1 && workers<=32)) || exit 2
export PGAPPNAME="pgdm_shadow_control:$job"
psql_cmd=(psql -h localhost -U "$role" -d "$database" -XqAt -v ON_ERROR_STOP=1)
# Child commands must not consume the SSH-delivered shell source on stdin.
run_psql() { "${psql_cmd[@]}" "$@" </dev/null; }
exec 9>"/tmp/pgdm-shadow-$job.lock"
flock -x 9
started=$(date +%s%N)
pids=()
stop_workers() {
  run_psql -c "SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE application_name='pgdm_shadow:$job' AND state='active';" >/dev/null 2>&1 || true
  for pid in "${pids[@]}"; do kill -TERM "$pid" 2>/dev/null || true; done
  wait || true
}
trap 'stop_workers; exit 130' HUP INT TERM
reused=$(run_psql -c "DO \$active\$ BEGIN WHILE EXISTS(SELECT FROM pg_stat_activity WHERE application_name='pgdm_shadow:$job' AND state='active') LOOP PERFORM pg_sleep(0.2); END LOOP; END \$active\$; UPDATE pgdm_runtime.shadow_jobs SET attempts=attempts+1 WHERE job_id='$job'; SELECT count(*) FROM pgdm_runtime.shadow_chunks WHERE job_id='$job' AND state='done';")
for ((worker=0; worker<workers; worker++)); do
  PGAPPNAME="pgdm_shadow:$job" run_psql -c "CALL pgdm_runtime.worker_$job();" &
  pids+=("$!")
done
interrupted=0
while ((stop_after>0)); do
  running=0
  for pid in "${pids[@]}"; do if kill -0 "$pid" 2>/dev/null; then running=1; fi; done
  ((running)) || break
  if ((stop_after>0)); then
    done_count=$(run_psql -c "SELECT count(*) FROM pgdm_runtime.shadow_chunks WHERE job_id='$job' AND state='done';")
    if ((done_count>=stop_after)); then
      run_psql -c "SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE application_name='pgdm_shadow:$job' AND state='active';" >/dev/null
      interrupted=1
      break
    fi
  fi
  sleep 0.1
done
failed=0
for pid in "${pids[@]}"; do if ! wait "$pid"; then failed=1; fi; done
finished=$(date +%s%N)
elapsed=$((finished-started))
run_psql -c "UPDATE pgdm_runtime.shadow_jobs SET load_seconds=load_seconds+$elapsed/1000000000.0 WHERE job_id='$job'; SELECT json_build_object('done',count(*) FILTER(WHERE state='done'),'total',count(*),'reused_at_launch',$reused,'interrupted',$interrupted,'source_rows',sum(source_rows),'max_chunk_bytes',max(stage_bytes),'rows',(SELECT jsonb_object_agg(t.name,(SELECT COALESCE(sum((row_counts->>t.name)::bigint),0) FROM pgdm_runtime.shadow_chunks WHERE job_id='$job' AND state='done')) FROM unnest(ARRAY['table1','table2','table3','table4','table5','table6']) t(name))) FROM pgdm_runtime.shadow_chunks WHERE job_id='$job';"
if ((failed && !interrupted)); then exit 1; fi
