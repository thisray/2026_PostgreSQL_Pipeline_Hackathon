"""Bound small-server chunk oversubscription using the server resource receipt."""
import json
import os

from services.publication_memory import admission_budget


def choose_workers(cpu, catalog, report, small_cpu_oversubscription=True):
    observed = json.loads(cpu)
    if isinstance(observed, int):
        observed = dict(effective_cpus=observed, verified_server=False)
    budget = max(1, int(observed['effective_cpus']))
    baseline = min(budget, 16)
    requested = baseline + (budget + 1) // 2 if small_cpu_oversubscription and budget <= 6 and observed.get('verified_server') else baseline
    explicit = os.getenv('PDM_STAGE_WORKERS')
    if explicit is not None:
        requested = int(explicit)
    if not 1 <= requested <= 32:
        raise ValueError('Workers must be between 1 and 32')
    memory = admission_budget(observed, catalog.get('shared_buffers_mb', 0), planned_tasks=requested)
    # Existing SQL temp_buffers is 128 MiB. Reserve another 128 MiB per
    # backend for stage/executor/native buffers; this is admission, not RSS.
    per_worker_mb = 256
    memory_limit = max(1, memory['shared_total_mb'] // per_worker_mb) if observed.get('verified_server') else requested
    workers = min(requested, int(catalog['connection_budget']), memory_limit)
    # Publication keeps its own existing per-task memory admission.
    publication = min(baseline if explicit is None else requested,
                      int(catalog['connection_budget']))
    report['chunk_worker_policy'] = dict(
        policy='small_server_io_wal_headroom_v1', observed=observed,
        small_cpu_oversubscription_enabled=small_cpu_oversubscription,
        effective_cpus=budget, small_cpu_ceiling=6, baseline_workers=baseline,
        explicit_workers=explicit, requested_workers=requested,
        connection_budget=catalog['connection_budget'], memory=memory,
        memory_per_worker_mb=per_worker_mb, memory_worker_limit=memory_limit,
        chosen_workers=workers, publication_workers=publication,
        oversubscribed=workers > budget)
    catalog['workers'] = workers
    return workers
