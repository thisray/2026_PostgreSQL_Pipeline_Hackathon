# Persistent psql slots share the original DAG admission and ready function.
# The per-job flock is already held. A hard-killed controller can leave its
# private FIFO directory, but no live worker can still own that lock here.
pool=__PGDM_POOL_PATH__
if [[ -e $pool || -L $pool ]]; then
  [[ -d $pool && ! -L $pool && -O $pool ]] || exit 2
  rm -rf -- "$pool"
fi
mkdir -m 700 -- "$pool"
mkfifo "$pool/events"
exec {events}<>"$pool/events"
slot_inputs=()
slot_pids=()
slot_tasks=()
states=()
active=()
reservation_logged=()
participants=()
granted_parallel=()
used_memory=0
used_cpu=0
used_parallel=0
completed=0
failed=0
pool_success=0
partial_event=''
for ((i=0;i<total;i++)); do states[i]=pending; done

worker_loop() {
  local slot=$1 task granted line writer reader sql_pid status token sql dispatch fd
  exec {dispatch}<"$pool/slot-$slot"
  # Only the controller retains FIFO writers. Its death delivers EOF even
  # when idle slots survive SIGKILL; they then close psql and the job lock.
  for fd in "${slot_inputs[@]}"; do exec {fd}>&-; done
  coproc PGDM_SQL { exec "${psql_cmd[@]}" -f -; }
  sql_pid=$PGDM_SQL_PID
  exec {writer}>&"${PGDM_SQL[1]}"
  exec {reader}<&"${PGDM_SQL[0]}"
  trap 'kill -TERM "$sql_pid" 2>/dev/null || true; wait "$sql_pid" 2>/dev/null || true' EXIT
  trap 'exit 130' HUP INT TERM
  printf "SELECT 'PGDM_DAG_SESSION' || chr(9) || pg_backend_pid();\n" >&"$writer"
  while read -r task granted <&"$dispatch"; do
    [[ $task == quit ]] && break
    token="PGDM_SLOT_DONE_${slot}_${task}"
    sql=${dispatch_sqls[task]//__PGDM_PARALLEL_WORKERS__/$granted}
    printf '%s\n\\echo %s\n' "$sql" "$token" >&"$writer"
    status=1
    while IFS= read -r line <&"$reader"; do
      if [[ $line == "$token" ]]; then status=0; break; fi
      printf '%s\n' "$line"
    done
    if ((status)); then
      wait "$sql_pid" || status=$?
      ((status)) || status=1
    fi
    printf '%s %s %s\n' "$slot" "$task" "$status" >&"$events"
    ((status==0)) || return "$status"
  done
  printf '\\q\n' >&"$writer"
  wait "$sql_pid"
}

stop_workers() {
  "${psql_cmd[@]}" -c "$cancel_sql" </dev/null >/dev/null 2>&1 || true
  for pid in "${slot_pids[@]}"; do kill -TERM "$pid" 2>/dev/null || true; done
  wait || true
}
cleanup_pool() {
  if ((pool_success==0)); then stop_workers; fi
  rm -rf -- "$pool"
}
trap cleanup_pool EXIT
trap 'stop_workers; exit 130' HUP INT TERM

# Dispatch SQL stays in each slot process; only task numbers cross its FIFO.
dispatch_sqls=("${sqls[@]}")
for ((slot=0;slot<limit;slot++)); do
  mkfifo "$pool/slot-$slot"
  exec {slot_fd}<>"$pool/slot-$slot"
  slot_inputs[slot]=$slot_fd
  slot_tasks[slot]=-1
  worker_loop "$slot" &
  slot_pids[slot]=$!
done

finish_task() {
  local slot=$1 i=$2 status=$3
  [[ ${slot_tasks[slot]} == "$i" ]] || return 2
  slot_tasks[slot]=-1
  used_memory=$((used_memory-memories[i]))
  used_cpu=$((used_cpu-participants[i]))
  used_parallel=$((used_parallel-granted_parallel[i]))
  if ((status)); then failed=1; states[i]=failed
  else states[i]=done; completed=$((completed+1)); fi
  printf 'PGDM_DAG_FINISH\t%s\t%s\t%s\n' "${labels[i]}" "$status" "${EPOCHREALTIME:-0}"
}

next_event() {
  # A timed Bash read can consume part of a line before its timeout.
  # Retain that prefix so scheduler load cannot corrupt completion records.
  local line=''
  if IFS= read -r -t "$1" line <&"$events"; then
    read -r slot task status <<< "$partial_event$line"
    partial_event=''
    return 0
  fi
  partial_event+=$line
  return 1
}

__PGDM_READY_FUNCTION__
while ((completed<total)); do
  while next_event 0.001; do finish_task "$slot" "$task" "$status"; done
  if ((failed)); then stop_workers; exit 1; fi
  launched=0
  for ((i=0;i<total;i++)); do
    slot=-1
    for ((candidate=0;candidate<limit;candidate++)); do
      if ((${slot_tasks[candidate]}<0)); then slot=$candidate; break; fi
    done
    ((slot>=0)) || break
    if ready "$i"; then
      states[i]=running
      granted=${parallel_requests[i]}
      headroom=$((cpu_limit-used_cpu-1))
      if ((granted>headroom)); then granted=$headroom; fi
      headroom=$((parallel_limit-used_parallel))
      if ((granted>headroom)); then granted=$headroom; fi
      granted_parallel[i]=$granted
      participants[i]=$((granted+1))
      used_cpu=$((used_cpu+participants[i]))
      used_parallel=$((used_parallel+granted))
      used_memory=$((used_memory+memories[i]))
      printf 'PGDM_DAG_CPU\t%s\t%s\t%s\t%s\t%s\n' "${labels[i]}" "$granted" "${participants[i]}" "$((memories[i]/participants[i]))" "${EPOCHREALTIME:-0}"
      printf 'PGDM_DAG_START\t%s\t%s\t%s\t%s\n' "${labels[i]}" "${kinds[i]}" "${memories[i]}" "${EPOCHREALTIME:-0}"
      slot_tasks[slot]=$i
      printf '%s %s\n' "$i" "$granted" >&"${slot_inputs[slot]}"
      launched=1
    fi
  done
  if ((completed==total)); then break; fi
  active=()
  for ((slot=0;slot<limit;slot++)); do
    if ((${slot_tasks[slot]}>=0)); then active+=("${slot_tasks[slot]}"); fi
    if ! kill -0 "${slot_pids[slot]}" 2>/dev/null; then
      printf 'Publication slot exited before completion\n' >&2
      stop_workers; exit 1
    fi
  done
  if ((${#active[@]}==0)); then
    printf 'Publication DAG has pending work but no runnable task\n' >&2
    stop_workers; exit 2
  fi
  if ((launched==0)); then
    if next_event 0.05; then finish_task "$slot" "$task" "$status"; fi
  fi
done
for ((slot=0;slot<limit;slot++)); do printf 'quit\n' >&"${slot_inputs[slot]}"; done
for pid in "${slot_pids[@]}"; do wait "$pid"; done
pool_success=1
exit 0
