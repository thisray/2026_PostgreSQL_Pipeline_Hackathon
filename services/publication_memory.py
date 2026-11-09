"""Discover PostgreSQL-server memory in the psql namespace, including cgroups."""
import base64
import os


MEMORY_PROBE = r'''pid=$PGDM_BACKEND_PID
total=$(awk '/^MemTotal:/{printf "%.0f",$2*1024}' /proc/meminfo)
available=$(awk '/^MemAvailable:/{printf "%.0f",$2*1024}' /proc/meminfo)
if ! tr '\0' ' ' < /proc/$pid/cmdline 2>/dev/null | grep -q postgres; then
 printf '{"verified_server":false,"nproc":%s,"effective_cpus":%s}\n' "$(nproc)" "$(nproc)"; exit 0
fi
limit=$total
root=/sys/fs/cgroup
if [ -r "$root/cgroup.controllers" ]; then
 path=$(awk -F: '$1=="0"{print $3}' /proc/$pid/cgroup)
 dir="$root$path"
 if [ ! -r "$dir/memory.max" ]; then dir=$root; fi
else
 root=/sys/fs/cgroup/memory
 path=$(awk -F: '$2 ~ /(^|,)memory(,|$)/{print $3}' /proc/$pid/cgroup)
 dir="$root$path"
 if [ ! -r "$dir/memory.limit_in_bytes" ]; then dir=$root; fi
fi
groups=''
separator=''
while [ -d "$dir" ]; do
 version=0
 if [ -r "$dir/memory.max" ]; then
  read cap < "$dir/memory.max"
  read used < "$dir/memory.current"
  version=2
 elif [ -r "$dir/memory.limit_in_bytes" ]; then
  read cap < "$dir/memory.limit_in_bytes"
  read used < "$dir/memory.usage_in_bytes"
  version=1
 fi
 if [ "$version" -gt 0 ] && [ "$cap" != max ] && [ "$cap" -gt 0 ] 2>/dev/null; then
  if [ "$cap" -lt "$limit" ]; then limit=$cap; fi
  stat=$(awk -v version="$version" '
   {s[$1]=$2}
   END {
    if(version==2) {
     file=s["file"]; active=s["active_file"]; inactive=s["inactive_file"];
     shmem=s["shmem"]; dirty=s["file_dirty"]; writeback=s["file_writeback"];
    } else {
     file=s["total_cache"]; active=s["total_active_file"]; inactive=s["total_inactive_file"];
     shmem=s["total_shmem"]; dirty=s["total_dirty"]; writeback=s["total_writeback"];
     if (!("total_cache" in s)) {
      file=s["cache"]; active=s["active_file"]; inactive=s["inactive_file"];
      shmem=s["shmem"]; dirty=s["dirty"]; writeback=s["writeback"];
     }
    }
    printf "\"file_bytes\":%.0f,\"active_file_bytes\":%.0f,\"inactive_file_bytes\":%.0f,\"shmem_bytes\":%.0f,\"dirty_bytes\":%.0f,\"writeback_bytes\":%.0f",file,active,inactive,shmem,dirty,writeback;
   }' "$dir/memory.stat")
  groups="$groups$separator{\"version\":$version,\"limit_bytes\":$cap,\"current_bytes\":$used,$stat}"
  separator=','
 fi
 [ "$dir" = "$root" ] && break
 dir=${dir%/*}
done
cpu_nproc=$(nproc)
cpu_limit=$cpu_nproc
count_set() { awk -F, '{n=0;for(i=1;i<=NF;i++){split($i,r,"-");n+=(r[2]==""?1:r[2]-r[1]+1)};print n}'; }
allowed=$(awk '/^Cpus_allowed_list:/{print $2}' /proc/$pid/status)
if [ -n "$allowed" ]; then
 cpus=$(printf '%s\n' "$allowed" | count_set)
 if [ "$cpus" -gt 0 ] && [ "$cpus" -lt "$cpu_limit" ]; then cpu_limit=$cpus; fi
fi
cpu_root=/sys/fs/cgroup
cpu_groups=''
cpu_separator=''
if [ -r "$cpu_root/cgroup.controllers" ]; then
 cpu_path=$(awk -F: '$1=="0"{print $3}' /proc/$pid/cgroup)
 cpu_dir="$cpu_root$cpu_path"
 if [ ! -r "$cpu_dir/cpu.max" ]; then cpu_dir=$cpu_root; fi
else
 cpu_root=/sys/fs/cgroup/cpu
 cpu_path=$(awk -F: '$2 ~ /(^|,)cpu(,|$)/{print $3}' /proc/$pid/cgroup)
 cpu_dir="$cpu_root$cpu_path"
 if [ ! -r "$cpu_dir/cpu.cfs_quota_us" ]; then cpu_dir=$cpu_root; fi
fi
while [ -d "$cpu_dir" ]; do
 quota=max; period=0
 if [ -r "$cpu_dir/cpu.max" ]; then read quota period < "$cpu_dir/cpu.max";
 elif [ -r "$cpu_dir/cpu.cfs_quota_us" ]; then
  read quota < "$cpu_dir/cpu.cfs_quota_us"; read period < "$cpu_dir/cpu.cfs_period_us"
 fi
 if [ "$quota" != max ] && [ "$quota" -gt 0 ] 2>/dev/null && [ "$period" -gt 0 ] 2>/dev/null; then
  cpus=$((quota/period)); if [ "$cpus" -lt 1 ]; then cpus=1; fi
  if [ "$cpus" -lt "$cpu_limit" ]; then cpu_limit=$cpus; fi
 fi
 cpuset=''
 for set_file in "$cpu_dir/cpuset.cpus.effective" "$cpu_dir/cpuset.effective_cpus" "$cpu_dir/cpuset.cpus"; do
  if [ -r "$set_file" ]; then read cpuset < "$set_file"; [ -z "$cpuset" ] || break; fi
 done
 if [ -n "$cpuset" ]; then
  cpus=$(printf '%s\n' "$cpuset" | count_set)
  if [ "$cpus" -gt 0 ] && [ "$cpus" -lt "$cpu_limit" ]; then cpu_limit=$cpus; fi
 fi
 cpu_groups="$cpu_groups$cpu_separator{\"quota\":\"$quota\",\"period\":$period,\"cpuset\":\"$cpuset\"}"
 cpu_separator=','
 [ "$cpu_dir" = "$cpu_root" ] && break
 cpu_dir=${cpu_dir%/*}
done
if [ -d /sys/fs/cgroup/cpuset ]; then
 set_path=$(awk -F: '$2 ~ /(^|,)cpuset(,|$)/{print $3}' /proc/$pid/cgroup)
 set_dir="/sys/fs/cgroup/cpuset$set_path"
 if [ ! -d "$set_dir" ]; then set_dir=/sys/fs/cgroup/cpuset; fi
 while [ -d "$set_dir" ]; do
  cpuset=''
  for set_file in "$set_dir/cpuset.effective_cpus" "$set_dir/cpuset.cpus"; do
   if [ -r "$set_file" ]; then read cpuset < "$set_file"; [ -z "$cpuset" ] || break; fi
  done
  if [ -n "$cpuset" ]; then
   cpus=$(printf '%s\n' "$cpuset" | count_set)
   if [ "$cpus" -gt 0 ] && [ "$cpus" -lt "$cpu_limit" ]; then cpu_limit=$cpus; fi
   cpu_groups="$cpu_groups$cpu_separator{\"quota\":\"max\",\"period\":0,\"cpuset\":\"$cpuset\"}"
   cpu_separator=','
  fi
  [ "$set_dir" = /sys/fs/cgroup/cpuset ] && break
  set_dir=${set_dir%/*}
 done
fi
printf '{"nproc":%s,"effective_cpus":%s,"cpu_allowed_list":"%s","cpu_cgroups":[%s],"verified_server":true,"host_total_bytes":%s,"host_available_bytes":%s,"effective_limit_bytes":%s,"cgroups":[%s]}\n' "$cpu_nproc" "$cpu_limit" "$allowed" "$cpu_groups" "$total" "$available" "$limit" "$groups"
'''


def probe_command():
    encoded = base64.b64encode(MEMORY_PROBE.encode()).decode('ascii')
    return ('SELECT pg_backend_pid() AS pgdm_backend_pid \\gset\n'
            '\\setenv PGDM_BACKEND_PID :pgdm_backend_pid\n'
            '\\! printf %s ' + encoded + ' | base64 -d | sh\n')


def reclaimable_memory(observed):
    """Bound clean file-cache headroom by every cgroup and host MemAvailable."""
    host_available = max(0, int(observed['host_available_bytes']))
    available = host_available
    details = []
    groups = observed.get('cgroups')
    if groups is None:
        # Historical receipts recorded only the inactive-file aggregate.
        return min(available, max(0, int(observed['reclaimable_available_bytes']))), details
    for group in groups:
        limit = max(0, int(group['limit_bytes']))
        current = max(0, int(group['current_bytes']))
        file_bytes = max(0, int(group.get('file_bytes',
            int(group.get('active_file_bytes', 0)) + int(group.get('inactive_file_bytes', 0)))))
        excluded = sum(max(0, int(group.get(key, 0))) for key in
                       ('shmem_bytes', 'dirty_bytes', 'writeback_bytes'))
        clean_file = max(0, file_bytes - excluded)
        headroom = max(0, min(limit, limit - current + clean_file))
        available = min(available, headroom)
        details.append(dict(group, clean_reclaimable_file_bytes=clean_file,
                            available_bytes=headroom))
    return available, details


def admission_budget(observed, shared_buffers_mb=0, cap=None, planned_tasks=None):
    mib = 1024 * 1024
    admitted = max(1, min(32, int(planned_tasks if planned_tasks is not None else
                                  observed.get('effective_cpus', 4))))
    default_floor = max(256, admitted * 64)
    group_details = []
    available_bytes = None
    limit = None
    if observed.get('verified_server'):
        limit = max(1, int(observed['effective_limit_bytes']) // mib)
        available_bytes, group_details = reclaimable_memory(observed)
        available = available_bytes // mib
        # Shared buffers are already charged in current; reserve modest extra
        # operating headroom instead of charging a quarter of all server RAM.
        reserve = max(128, min(1024, limit // 32), int(shared_buffers_mb))
        floor = min(default_floor, max(1, limit // 4))
        before_floor = max(1, min(8192, limit // 3, available - reserve))
        safe = min(8192, max(floor, before_floor))
        basis = 'clean_file_cache_host_bound_modest_headroom_default_task_floor'
    else:
        safe = min(8192, default_floor)
        reserve, floor, before_floor = None, safe, 0
        basis = 'unverified_server_namespace_postgresql_default_task_floor'
    requested = {}
    for name in ('PDM_S3_DAG_MEMORY_MB', 'PDM_S3_INDEX_MEMORY_MB',
                 'PDM_S2_INDEX_MEMORY_MB', 'PDM_S3_FK_MEMORY_MB'):
        if name in os.environ:
            value = int(os.environ[name])
            if not 32 <= value <= 8192:
                raise ValueError(name + ' must be between 32 and 8192 MB')
            requested[name] = value
    desired = requested.get('PDM_S3_DAG_MEMORY_MB', max(requested.values(), default=safe))
    pool = min(safe, desired)
    if cap is not None:
        pool = min(pool, int(cap))
    if pool < 1:
        raise ValueError('No publication admission memory available')
    return dict(observed, reclaimable_available_bytes=available_bytes,
                cgroup_memory_details=group_details,
                shared_total_mb=pool, automatic_safe_mb=safe,
                automatic_cap_mb=8192, planned_concurrent_tasks=admitted,
                postgresql_default_per_task_mb=64, default_pool_floor_mb=default_floor,
                effective_pool_floor_mb=floor, floor_applied=safe > before_floor,
                tiny_limit_floor_reduced=floor < default_floor,
                automatic_before_floor_mb=before_floor,
                reserved_headroom_mb=reserve, requested=requested, policy=basis,
                reason=('Verified PostgreSQL backend memory and cgroup headroom.'
                        if observed.get('verified_server') else
                        'Cannot verify the PostgreSQL backend in this PID namespace '
                        '(for example hidepid or a separate container). Use the '
                        'PostgreSQL default maintenance_work_mem of 64 MiB per '
                        'planned concurrent task, at least 256 MiB total; '
                        'explicit memory options can only lower this pool.'))
