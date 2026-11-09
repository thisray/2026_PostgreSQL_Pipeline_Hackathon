"""Dependency-driven shadow publication with one worker and memory pool.

SQL and native share this implementation. Every index, key
attachment, and FK validation keeps its own durable transaction. The final
schema swap and its identity checks remain the caller's responsibility.
"""
import base64
import json
import re
import shlex
from pathlib import Path

from services.expansion_wal import SESSION_WAL_SQL
from services import shadow_expansion as base
from services import raw_fk_proof, shared_fk_proof
from services import s3_publication as waves
from services.publication_memory import probe_command, admission_budget
from utils import sql_ident, sql_literal


# The native adapter also uses this API for dimension fallback.
parallel_wave = waves.parallel_wave
SCHEDULER_VERSION = 'rc15-in-memory-index-admission-v1'
# RC4: IOS/seq = 41.896873/27.876337 at 35.4394% visibility.
# Even ignoring fixed index cost, break-even is about 57%; force only below 50%.
RAW_FK_SEQUENTIAL_VISIBLE_FRACTION = 0.50


def _fk_validation_plan(schema, key, parent_table, memory):
    """Select measured RI plans without executing the check or changing data.

    RI_Initial_Check uses maintenance_work_mem and hash_mem_multiplier=1.
    Clearly low raw visibility favors a sequential hash anti-join. The fresh
    shadow-parent probe already chose sequential scans with default settings.
    Only ordinary ALTER TABLE VALIDATE makes the integrity decision.
    """
    raw = parent_table is None
    parent = (base.relation(key['reference_schema'], key['reference_table']) if raw
              else base.relation(schema, parent_table))
    child = base.relation(schema, key['table'])
    fk_column = 'id_column_3' if raw else 'id_column_2'
    pk_column = 'raw_id' if raw else 'id_column_1'
    check = (f'SELECT fk.{fk_column} FROM ONLY {child} fk '
             f'LEFT OUTER JOIN ONLY {parent} pk ON (pk.{pk_column} = fk.{fk_column}) '
             f'WHERE pk.{pk_column} IS NULL AND (fk.{fk_column} IS NOT NULL)')
    explain = sql_literal('EXPLAIN (FORMAT JSON) ' + check)
    return f"""SET LOCAL work_mem='{memory}MB';
DO $fk_plan$
DECLARE baseline jsonb; selected jsonb; visibility jsonb; low_visibility boolean;
BEGIN
 SELECT jsonb_build_object('relpages',relpages,'relallvisible',relallvisible,
                          'visible_fraction',relallvisible::numeric/NULLIF(relpages,0)),
        (relpages>0 AND relallvisible::numeric/relpages < {RAW_FK_SEQUENTIAL_VISIBLE_FRACTION}) INTO visibility,low_visibility
 FROM pg_class WHERE oid={sql_literal(parent)}::regclass;
 EXECUTE {explain} INTO baseline;
 IF low_visibility AND {str(raw).lower()} THEN
  PERFORM set_config('enable_mergejoin','off',true);
  PERFORM set_config('enable_nestloop','off',true);
  PERFORM set_config('enable_indexonlyscan','off',true);
  PERFORM set_config('enable_indexscan','off',true);
  PERFORM set_config('enable_bitmapscan','off',true);
 END IF;
 EXECUTE {explain} INTO selected;
 PERFORM set_config('pgdm.fk_plan',jsonb_build_object(
  'policy','visibility_fk_rc7','constraint',{sql_literal(key['name'])},
  'child',{sql_literal(child)},'parent',{sql_literal(parent)},
  'referenced_visibility',visibility,'sequential_forced',low_visibility AND {str(raw).lower()},
  'sequential_below_visible_fraction',{RAW_FK_SEQUENTIAL_VISIBLE_FRACTION},
  'decision_inputs',jsonb_build_object('rc4_visible_fraction',0.354394,
   'rc4_ios_seconds',41.896873,'rc4_seq_seconds',27.876337,
   'rc6_native_visible_fraction',0.805436,'rc6_native_seq_seconds',76.027783,
   'rc6_sql_visible_fraction',1.0,'rc6_sql_ios_seconds',13.530282),
  'maintenance_memory_mb',{memory},'baseline',baseline,'selected',selected)::text,true);
END $fk_plan$;
SELECT current_setting('pgdm.fk_plan');
"""


def _snapshot_sql(job):
    """Refresh resume state and exact committed counts without heap scans."""
    schema = sql_literal(job['schema_name'])
    job_id = sql_literal(job['job_id'])
    if 'native_build_id' in job['payload']:
        codes = (f"SELECT code_id id,'SYNTHETIC/GENERIC' label,"
                 f"CHR((code_id-1)/26+65)||CHR((code_id-1)%26+65) code "
                 f"FROM {base.relation(job['schema_name'],'observed_codes')} GROUP BY code_id")
        finish_codes = f"INSERT INTO {base.relation(job['schema_name'],'table4')} {codes} ON CONFLICT(id_column_1) DO NOTHING;"
    else:
        finish_codes = f"""DO $codes$ BEGIN
 IF NOT COALESCE((SELECT (payload->>'codes_finalized')::boolean FROM {base.JOB_TABLE} WHERE job_id={job_id}),false) THEN
 INSERT INTO {base.relation(job['schema_name'],'table4')}
 SELECT ((ASCII(LEFT(code,1))-65)*26+ASCII(RIGHT(code,1))-65+1)::smallint,'SYNTHETIC/GENERIC',code
 FROM pgdm_runtime.shadow_codes WHERE job_id={job_id} GROUP BY code;
 UPDATE {base.JOB_TABLE} SET payload=payload || '{{"codes_finalized":true}}'::jsonb WHERE job_id={job_id};
 END IF; END $codes$;"""
    if shared_fk_proof.required(job):
        finish_codes = f"""DO $sealed_codes$ BEGIN
 IF NOT EXISTS(SELECT FROM pgdm_fk_proof.epochs WHERE job_id={job_id}) THEN
 {finish_codes}
 END IF;
END $sealed_codes$;"""
    sql = probe_command() + f"""BEGIN;
{raw_fk_proof.guard_sql(job)}
DO $lock$ BEGIN PERFORM pg_advisory_xact_lock(hashtextextended({sql_literal(job['job_id']+':dimensions')},0)); END $lock$;
{finish_codes}
UPDATE {base.JOB_TABLE} SET state='indexing',
 payload=jsonb_set(payload,'{{dimension_rows,table4}}',
 to_jsonb((SELECT count(*) FROM {base.relation(job['schema_name'],'table4')})))
WHERE job_id={job_id};
{shared_fk_proof.seal_sql(job)}
COMMIT;
{shared_fk_proof.dimensions_sql(job)}
WITH counts AS (
 SELECT e.key AS table_name,sum(e.value::bigint) AS rows
 FROM {base.CHUNK_TABLE} c
 CROSS JOIN LATERAL jsonb_each_text(COALESCE(c.row_counts,'{{}}'::jsonb)) e
 WHERE c.job_id={job_id} AND c.state='done'
 AND e.key IN ('table1','table2','table3','table4','table5','table6')
 GROUP BY e.key
), widths AS (
 SELECT ic.relname AS index_name,
  (SELECT COALESCE(sum(CASE WHEN a.attlen>0 THEN a.attlen ELSE 32 END),0)
   FROM unnest(i.indkey::smallint[]) AS key_column(attnum)
   LEFT JOIN pg_attribute a ON a.attrelid=i.indrelid
    AND a.attnum=key_column.attnum) AS bytes
 FROM pg_index i JOIN pg_class ic ON ic.oid=i.indexrelid
 JOIN pg_class t ON t.oid=i.indrelid JOIN pg_namespace n ON n.oid=t.relnamespace
 WHERE n.nspname='public'
 AND t.relname IN ('table1','table2','table3','table4','table5','table6')
)
SELECT jsonb_build_object(
 'cpu_limits',jsonb_build_object(
  'max_worker_processes',current_setting('max_worker_processes')::int,
  'max_parallel_workers',current_setting('max_parallel_workers')::int,
  'max_parallel_maintenance_workers',current_setting('max_parallel_maintenance_workers')::int,
  'busy_parallel_workers',(SELECT count(*) FROM pg_stat_activity WHERE backend_type='parallel worker')),
 'shared_buffers_mb',pg_size_bytes(current_setting('shared_buffers'))/1048576,
 'payload',(SELECT payload FROM {base.JOB_TABLE} WHERE job_id={job_id}),
 'raw_fk_proof',(SELECT payload->'raw_fk_proof' FROM {base.JOB_TABLE} WHERE job_id={job_id}),
 'valid_indexes',COALESCE((SELECT jsonb_agg(c.relname) FROM pg_index i
  JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_namespace n ON n.oid=c.relnamespace
  WHERE n.nspname={schema} AND i.indisvalid),'[]'::jsonb),
 'constraints',COALESCE((SELECT jsonb_agg(jsonb_build_object(
  'table',c.relname,'name',k.conname,'type',k.contype,'validated',k.convalidated))
  FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid
  JOIN pg_namespace n ON n.oid=c.relnamespace
  WHERE n.nspname={schema}),'[]'::jsonb),
 'index_widths',COALESCE((SELECT jsonb_object_agg(index_name,bytes) FROM widths),
  '{{}}'::jsonb),
 'row_counts',COALESCE((SELECT jsonb_object_agg(table_name,rows) FROM counts),
  '{{}}'::jsonb));"""
    return sql


def _parse_snapshot(output):
    memory, snapshot = map(json.loads, output.strip().split('\n', 1))
    snapshot['memory'] = memory
    return snapshot


def _snapshot(service, database, job, report):
    cached = job.pop('_publication_snapshot', None)
    if cached is not None:
        return cached
    output = base.query(service, database, _snapshot_sql(job), report,
                        'S3 DAG: dimensions, server memory and durable publication state')
    return _parse_snapshot(output)


def load_chunks(service, database, job, workers, stop_after, report):
    """Batch post-load metadata into the loader's SSH envelope."""
    script = Path(__file__).with_name('shadow_runner.sh').read_text()
    script = script.replace('psql_cmd=(psql ', 'psql_cmd=(psql -c ' + shlex.quote(SESSION_WAL_SQL) + ' ')
    if int(job['payload']['catalog']['scope_rows']) <= 1000000:
        cancel_sql = Path(__file__).with_name('small_chunk_cancel.sql').read_text()
        cancel_sql = cancel_sql.replace('__PGDM_JOB__', job['job_id'])
        monitor = ('cancel_receipt=$(mktemp /tmp/pgdm-cancel-' + job['job_id'] + '.XXXXXX)\n'
            'PGAPPNAME="pgdm_shadow_cancel:$job" "${psql_cmd[@]}" -f - <<\'PGDM_CANCEL_SQL\' >"$cancel_receipt" &\n'
            + cancel_sql + '\nPGDM_CANCEL_SQL\ncancel_pid=$!\n')
        script = script.replace('for ((worker=0;', monitor + 'for ((worker=0;', 1)
        script = script.replace("application_name='pgdm_shadow:$job' AND state='active';\" >/dev/null 2>&1",
            "application_name IN ('pgdm_shadow:$job','pgdm_shadow_cancel:$job') AND state='active';\" >/dev/null 2>&1", 1)
        script = script.replace('  wait || true\n}',
            '  if [[ -n ${cancel_pid:-} ]]; then kill -TERM "$cancel_pid" 2>/dev/null || true; fi\n'
            '  wait || true\n  rm -f -- "${cancel_receipt:-}"\n}', 1)
        script = script.replace('finished=$(date +%s%N)',
            'if ((failed || interrupted)); then\n'
            '  run_psql -c "SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE application_name=\'pgdm_shadow_cancel:$job\';" >/dev/null\n'
            'fi\n'
            'if ! wait "$cancel_pid"; then failed=1; fi\n'
            'cancel_json=$(cat "$cancel_receipt")\nrm -f -- "$cancel_receipt"\n'
            'finished=$(date +%s%N)', 1)
        script = script.replace("'done',count(*) FILTER", "'raw_vacuum_cancel','${cancel_json:-null}'::json,'done',count(*) FILTER", 1)
    script = script.replace("'done',count(*) FILTER", "'phase_origin_ns',$started,'chunk_end_ns',$finished,'done',count(*) FILTER")
    final_start = script.index('run_psql -c "UPDATE pgdm_runtime.shadow_jobs SET load_seconds=')
    final_end = script.index('\nif ((failed && !interrupted));', final_start)
    summary = script[final_start + len('run_psql -c '):final_end]
    encoded = base64.b64encode(_snapshot_sql(job).encode()).decode('ascii')
    script = script[:final_start] + 'if ((failed && !interrupted)); then exit 1; fi\n{\n'
    script += "printf '%s\\n' " + summary + '\n'
    script += 'if ((interrupted==0)); then printf %s ' + encoded + ' | base64 -d; fi\n'
    script += '} | "${psql_cmd[@]}" -f -\n'
    arguments = [database, service.sql_username, job['job_id'], str(workers), str(stop_after)]
    with service.expansion_timing_scope(report, 'execute', 'Logged chunks, dimensions and publication metadata', record_local_cpu=False):
        output = service.run_remote_command('bash -s -- ' + ' '.join(map(shlex.quote, arguments)), stdin_text=script)
    lines = output.strip().split('\n', 1)
    loaded = json.loads(lines[0])
    report['chunk_phase_origin_ns'] = loaded.pop('phase_origin_ns')
    report['chunk_phase_end_ns'] = loaded.pop('chunk_end_ns')
    if 'raw_vacuum_cancel' in loaded:
        report['raw_vacuum_cancel'] = loaded.pop('raw_vacuum_cancel')
    if len(lines) == 2:
        job['_publication_snapshot'] = _parse_snapshot(lines[1])
    return loaded


def _guarded_attach(schema, constraint):
    """Use the same DDL and resume predicate as the frozen publication path."""
    kind = constraint['type']
    if kind not in ('p', 'u'):
        raise ValueError('Key attachment requires a primary or unique key')
    definition = 'PRIMARY KEY' if kind == 'p' else 'UNIQUE'
    definition += ' USING INDEX ' + sql_ident(constraint['index_name'])
    deferred = re.search(r'\b(?:NOT )?DEFERRABLE\b.*$', constraint['definition'])
    if deferred:
        definition += ' ' + deferred.group(0)
    return _guarded_add(schema, constraint, definition)


def _guarded_add(schema, constraint, definition):
    table = base.relation(schema, constraint['table'])
    ddl = f"ALTER TABLE {table} ADD CONSTRAINT {sql_ident(constraint['name'])} {definition}"
    return f"""IF NOT EXISTS(SELECT FROM pg_constraint
 WHERE conrelid={sql_literal(schema+'.'+constraint['table'])}::regclass
 AND conname={sql_literal(constraint['name'])}) THEN
 EXECUTE {sql_literal(ddl)}; END IF;"""


def _foreign_definition(job, constraint):
    parent = waves._parent_table(job, constraint)
    target = (base.relation(job['schema_name'], parent) if parent else
              base.relation(constraint['reference_schema'],
                            constraint['reference_table']))
    prefix, tail = constraint['definition'].split('REFERENCES ', 1)
    definition = prefix + 'REFERENCES ' + target + '(' + tail.split('(', 1)[1]
    return re.sub(r'\s+NOT VALID\s*$', '', definition) + ' NOT VALID'


def _compatible(left, right):
    # R: RowShare (FK parent validation); I: Share (normal index build);
    # V: ShareUpdateExclusive (child validation);
    # F: ShareRowExclusive (ADD FK, child and parent); A: AccessExclusive.
    if 'A' in (left, right):
        return False
    if 'R' in (left, right):
        return True
    return left == right == 'I'


def _add_lock(locks, relation, mode):
    strength = {'R': 0, 'I': 1, 'V': 2, 'F': 3, 'A': 4}
    if relation not in locks or strength[mode] > strength[locks[relation]]:
        locks[relation] = mode


def build_dag(job, snapshot, workers, index_total, fk_total, shared_total):
    """Build inspectable pending tasks; no SQL execution or catalog mutation."""
    workers = int(workers)
    if not 1 <= workers <= 32:
        raise ValueError('Publication workers must be between 1 and 32')
    if min(index_total, fk_total, shared_total) < 1:
        raise ValueError('Publication memory budgets must be positive')
    schema = job['schema_name']
    catalog = job['payload']['catalog']
    constraints = catalog['constraints']
    for constraint in constraints:
        if constraint['type'] not in ('c', 'n', 'p', 'u', 'f'):
            raise base.UnsupportedShadowError(
                f"Unsupported shadow constraint type: {constraint['type']}")
    existing = {(k['table'], k['name']): k for k in snapshot['constraints']}
    valid = set(snapshot['valid_indexes'])
    rows = {name: int(value) for name, value in snapshot['row_counts'].items()}
    rows.update({name: int(value) for name, value in
                 job['payload'].get('dimension_rows', {}).items()})
    widths = snapshot['index_widths']
    specs = [{'table': k['table'], 'name': k['index_name'],
              'definition': k['index_definition']}
             for k in constraints if k['type'] in ('p', 'u')]
    specs += catalog.get('indexes') or []
    specs = {spec['name']: spec for spec in specs}
    pending = [spec for spec in specs.values() if spec['name'] not in valid]
    foreign = [k for k in constraints if k['type'] == 'f']
    unvalidated = [k for k in foreign
                   if not existing.get((k['table'], k['name']), {}).get('validated')]
    index_memory = min(shared_total, max(1, index_total // min(workers, max(1, len(pending)))))
    fk_memory = min(shared_total, max(1, fk_total // min(
        workers, max(1, len({k['table'] for k in unvalidated})))))
    size_aware = True
    def rounded_mb(byte_count):
        needed = max(1, (int(byte_count) + 1048575) // 1048576)
        return max(16, 1 << (needed - 1).bit_length())

    def entry_rows(spec):
        # Both reviewed writers store id_column_3=NULL. Only NOT NULL has
        # no entries; IS NULL keeps the entire committed table5 row count.
        if spec['table'] == 'table5' and re.search(
                r'\bWHERE\s+\(id_column_3\s+IS\s+NOT\s+NULL\)\s*$',
                spec['definition']):
            return 0
        return rows.get(spec['table'], 0)
    index_costs = {
        spec['name']: entry_rows(spec) *
        (8 + int(widths.get(spec['name'], 8))) // 16
        for spec in pending}
    max_index_cost = max(index_costs.values(), default=0)
    large_indexes = set()
    if size_aware:
        # Actual committed row counts drive allocation; partial-index SQL
        # text cannot establish how much data its heap scan will encounter.
        ranked = sorted(pending, key=lambda spec: (
            index_costs[spec['name']], spec['name']), reverse=True)
        eligible = [spec['name'] for spec in ranked
                    if rows.get(spec['table'], 0) > 100000
                    and index_costs[spec['name']] * 4 >= max_index_cost * 3]
        large_indexes = set(eligible[:2])
    tasks = {}
    index_tasks = {}
    table_indexes = {}
    key_tasks = {}
    foreign_tasks = {}

    def add(name, kind, sql, dependencies, locks, memory, cost):
        tasks[name] = {'name': name, 'kind': kind, 'sql': sql,
                       'deps': set(dependencies), 'locks': locks,
                       'memory_mb': memory, 'cost': max(1, int(cost)),
                       'priority_predecessors': set(), 'reservations': [],
                       'window_policy': None, 'memory_policy': None, 'parallel_workers': 0}
        return name

    cpu_budget = max(1, int(snapshot.get('memory', {}).get('effective_cpus', 1)))
    limits = snapshot.get('cpu_limits', {})
    parallel_limit = max(0, min(int(limits.get('max_worker_processes', 0)),
                                int(limits.get('max_parallel_workers', 0)))
                         - int(limits.get('busy_parallel_workers', 0)))
    parallel_candidates = [spec for spec in pending if spec['table'] in ('table1', 'table2')
                           and rows.get(spec['table'], 0) > 1000000
                           and index_costs[spec['name']] >= 1000000]
    parallel_share = min(int(limits.get('max_parallel_maintenance_workers', 0)),
                         parallel_limit, max(0, cpu_budget // max(1, len(parallel_candidates)) - 1))
    if cpu_budget <= 4 and len(pending) >= cpu_budget:
        parallel_share = 0

    for spec in pending:
        table, name = spec['table'], spec['name']
        actual_rows = rows.get(table)
        estimated_entries = entry_rows(spec)
        memory, tier = index_memory, 'uniform'
        if size_aware:
            if actual_rows is not None and estimated_entries <= 100000:
                memory, tier = min(index_memory, 32), 'tiny_entries'
            elif name in large_indexes:
                memory, tier = min(shared_total, max(1, index_total // 4)), 'large_index'
            else:
                regular_cap = index_memory if int(widths.get(name, 8)) > 8 else max(1, index_memory // 2)
                memory, tier = min(shared_total, regular_cap), 'regular_index'
            memory = min(memory, rounded_mb(estimated_entries * (48 + int(widths.get(name, 8)))))
        # A parallel btree always writes worker tapes, even when each sort
        # fits. Prefer a serial in-memory sort when it fits a quarter-pool
        # grant; retain parallel builds beyond that bound for throughput.
        # SortTuple, aligned index tuples and allocation overhead are covered
        # by the row/width estimate; rounding leaves room at size boundaries.
        serial_memory = rounded_mb(estimated_entries * (40 + int(widths.get(name, 8))))
        if int(catalog.get('scope_rows', 0)) > 1000000 and estimated_entries > 100000:
            memory = max(memory, min(shared_total, max(1, index_total // 4), serial_memory))
        definition = re.sub(r'\bON public\.(table[1-6])\b',
                            lambda m: 'ON ' + base.relation(schema, m.group(1)),
                            spec['definition'])
        sql = f"""BEGIN; {waves._settings(memory,1,1)}
DO $index$ BEGIN
 IF NOT EXISTS(SELECT FROM pg_index
 WHERE indexrelid=to_regclass({sql_literal(schema+'.'+name)}) AND indisvalid)
 THEN EXECUTE {sql_literal(definition)}; END IF;
END $index$; COMMIT;"""
        cost = index_costs[name]
        task = add('index:' + name, 'index', sql, [],
                   {(schema, table): 'I'}, memory, cost)
        if spec in parallel_candidates and (int(catalog.get('scope_rows', 0)) <= 1000000
                                           or serial_memory > memory):
            tasks[task]['parallel_workers'] = parallel_share
            tasks[task]['sql'] = sql.replace('max_parallel_maintenance_workers=0',
                'max_parallel_maintenance_workers=__PGDM_PARALLEL_WORKERS__')
        tasks[task]['memory_policy'] = {
            'tier': tier, 'actual_rows': actual_rows, 'estimated_entries': estimated_entries,
            'index_width_bytes': int(widths.get(name, 8)),
            'uniform_mb': index_memory,
            'serial_sort_estimate_mb': serial_memory,
            'parallel_memory_policy': 'one_build_grant_split_by_postgresql_across_participants'}
        index_tasks[name] = task
        table_indexes.setdefault(table, set()).add(task)

    missing_keys = {}
    missing_foreign = {}
    for constraint in constraints:
        if (constraint['table'], constraint['name']) in existing:
            continue
        if constraint['type'] in ('p', 'u'):
            missing_keys.setdefault(constraint['table'], []).append(constraint)
        elif constraint['type'] == 'f':
            missing_foreign.setdefault(constraint['table'], []).append(constraint)

    for table, keys in missing_keys.items():
        # Only backing indexes are dependencies. Extra indexes are protected
        # by the runtime lock matrix, without adding another global barrier.
        deps = {index_tasks[k['index_name']] for k in keys
                if k['index_name'] in index_tasks}
        body = '\n'.join(_guarded_attach(schema, k) for k in keys)
        key_tasks[table] = add('keys:' + table, 'keys',
                              'BEGIN; DO $keys$ BEGIN ' + body +
                              ' END $keys$; COMMIT;', deps,
                              {(schema, table): 'A'}, 1, 1)

    for table, keys in missing_foreign.items():
        # ADD FK needs the referenced unique key, not the child's PK/UQ.
        # Runtime locks keep this short attachment out of active child sorts.
        deps = set()
        locks = {(schema, table): 'F'}
        for key in keys:
            parent = waves._parent_table(job, key)
            relation = ((schema, parent) if parent else
                        (key['reference_schema'], key['reference_table']))
            _add_lock(locks, relation, 'F')
            if parent in key_tasks:
                deps.add(key_tasks[parent])
            if parent and key['index_name'] in index_tasks:
                deps.add(index_tasks[key['index_name']])
        body = '\n'.join(_guarded_add(schema, k, _foreign_definition(job, k))
                         for k in keys)
        task = add('attach_fk:' + table, 'attach_fk',
                   'BEGIN; DO $foreign$ BEGIN ' + body +
                   ' END $foreign$; COMMIT;', deps, locks, 1, 1)
        foreign_tasks[table] = task
        # These are priority edges, not execution dependencies. ADD FK can
        # precede the child's own indexes, but after those builds have started
        # they form the lock frontier that releases this short attachment.
        tasks[task]['priority_predecessors'].update(table_indexes.get(table, ()))
        if table in key_tasks:
            tasks[task]['priority_predecessors'].add(key_tasks[table])

    def fk_parent_rows(key):
        parent = waves._parent_table(job, key)
        parent_rows = rows.get(parent)
        if parent is None and key['reference_table'] == 'raw_data':
            source_rows = catalog.get('scope_rows')
            parent_rows = int(source_rows) if source_rows is not None else None
        return parent_rows

    def fk_cost(key):
        return rows.get(key['table'], 0) + (fk_parent_rows(key) or 0)

    chains = {}
    for key in unvalidated:
        chains.setdefault(key['table'], []).append(key)

    def index_frontier_cost(table):
        return max((tasks[name]['cost']
                    for name in table_indexes.get(table, ())), default=0)

    inbound = {}
    for child, keys in missing_foreign.items():
        attachment = foreign_tasks[child]
        parents = {waves._parent_table(job, key) for key in keys}
        # Once these have completed, only short metadata work can stand
        # between the attachment and its locks. Do not include child key
        # attachment here: reserving its parent during that short A-lock is
        # precisely what prevents a long parent validation stealing the gap.
        ready_after = set(table_indexes.get(child, ()))
        ready_after.update(tasks[attachment]['deps'])
        release_cost = max(
            [index_frontier_cost(child)] +
            [index_frontier_cost(parent) for parent in parents if parent])
        child_fk_cost = sum(fk_cost(key) for key in chains.get(child, ()))
        branch = {'attach': attachment, 'child': child,
                  'ready_after': sorted(ready_after),
                  'release_cost': release_cost,
                  'branch_cost': release_cost + child_fk_cost}
        for parent in parents:
            if parent and parent != child:
                inbound.setdefault(parent, []).append(branch)

    for child, keys in chains.items():
        incoming = inbound.get(child, [])
        critical = (max(incoming, key=lambda branch: (
            branch['branch_cost'], branch['release_cost'], branch['attach']))
                    if incoming else None)
        # A parent has an additional scarce resource: its F-lock window for
        # incoming attachments. Fill an early window with its short FK first.
        # Leaf tables retain longest-first validation.
        ordered = sorted(keys, key=lambda key: (fk_cost(key), key['name']),
                         reverse=not bool(incoming))
        ready_cost = max(
            [index_frontier_cost(child)] +
            [index_frontier_cost(waves._parent_table(job, key)) for key in keys])
        cursor_cost = ready_cost
        reserved = False
        previous = None
        for key in ordered:
            deps = set(table_indexes.get(child, ()))
            if child in key_tasks:
                deps.add(key_tasks[child])
            if child in foreign_tasks:
                deps.add(foreign_tasks[child])
            if previous:
                deps.add(previous)
            window_policy = None
            finish_cost = cursor_cost + fk_cost(key)
            if critical and not reserved and critical['release_cost'] <= finish_cost:
                # Protect only the critical branch and only at the validation
                # whose estimated interval crosses its release. Earlier short
                # validations remain free to overlap the child's index work.
                # If even the complete parent chain fits before the release,
                # no static attachment dependency is introduced.
                deps.add(critical['attach'])
                reserved = True
                window_policy = {
                    'critical_attach': critical['attach'],
                    'critical_child': critical['child'],
                    'critical_release_cost': critical['release_cost'],
                    'critical_branch_cost': critical['branch_cost'],
                    'parent_ready_cost': ready_cost,
                    'validation_start_cost': cursor_cost,
                    'validation_finish_cost': finish_cost,
                    'basis': 'pending_index_width_rows_and_fk_rows'}
            cursor_cost = finish_cost
            parent = waves._parent_table(job, key)
            if parent in key_tasks:
                deps.add(key_tasks[parent])
            parent_relation = ((schema, parent) if parent else
                               (key['reference_schema'], key['reference_table']))
            locks = {(schema, child): 'V'}
            _add_lock(locks, parent_relation, 'R')
            actual_parent_rows = fk_parent_rows(key)
            small_parent = (size_aware and actual_parent_rows is not None
                            and actual_parent_rows <= 100000)
            memory = min(fk_memory, 32) if small_parent else fk_memory
            if actual_parent_rows is not None:
                memory = min(memory, rounded_mb(actual_parent_rows * (64 if child == 'table1' and parent is None else 128)))
            measured_parent = (parent == 'table1' or (
                child == 'table1' and parent is None
                and key['reference_schema'] == 'public'
                and key['reference_table'] == 'raw_data'))
            plan_sql = ('\n' + _fk_validation_plan(schema, key, parent, memory)
                        if measured_parent else '')
            sql = f"""BEGIN; {waves._settings(memory,1,1)}{plan_sql}
DO $validate$ BEGIN
 IF EXISTS(SELECT FROM pg_constraint
 WHERE conrelid={sql_literal(schema+'.'+child)}::regclass
 AND conname={sql_literal(key['name'])} AND NOT convalidated) THEN
 ALTER TABLE {base.relation(schema,child)}
 VALIDATE CONSTRAINT {sql_ident(key['name'])}; END IF;
END $validate$; COMMIT;"""
            previous = add('validate:' + child + ':' + key['name'], 'validate',
                           sql, deps, locks, memory, fk_cost(key))
            tasks[previous]['memory_policy'] = {
                'tier': 'small_actual_parent' if small_parent else 'uniform_fk',
                'actual_parent_rows': actual_parent_rows, 'uniform_mb': fk_memory}
            tasks[previous]['window_policy'] = window_policy
            tasks[previous]['reservations'] = [
                {'attach': branch['attach'], 'ready_after': branch['ready_after']}
                for branch in sorted(incoming, key=lambda branch: branch['attach'])]

    if int(catalog.get('scope_rows', 0)) <= 1000000:
        # Small workloads benefit from one metadata connection/transaction.
        # Ordinary PostgreSQL still attaches every key and FK before validation.
        # Tiny builds leave no index window to hide extra metadata sessions.
        # Retain their measured RC4 batch; split only the larger small workload.
        early_table1 = foreign_tasks.get('table1') if int(catalog.get('scope_rows', 0)) > 100000 else None
        early_dimensions = ({key_tasks[t] for t in ('table3', 'table4') if t in key_tasks}
                            if early_table1 else set())
        metadata = [t for t in tasks.values() if t['kind'] in ('keys', 'attach_fk')
                    and t['name'] != early_table1 and t['name'] not in early_dimensions]
        if metadata:
            removed = {t['name'] for t in metadata}
            # Keep even tiny index builds runnable in the shared pool before
            # the all-index metadata barrier; do not append sorts to its tail.
            indexes = {t['name'] for t in tasks.values() if t['kind'] == 'index'
                       and t['name'] not in removed}
            body = '\n'.join(_guarded_attach(schema, k) for k in constraints
                             if k['type'] in ('p', 'u') and key_tasks.get(k['table']) not in early_dimensions and (k['table'], k['name']) not in existing)
            body += '\n' + '\n'.join(_guarded_add(schema, k, _foreign_definition(job, k)) for k in foreign
                                      if foreign_tasks.get(k['table']) != early_table1 and (k['table'], k['name']) not in existing)
            locks = {}
            for task in metadata:
                for relation, mode in task['locks'].items():
                    _add_lock(locks, relation, mode)
            for name in removed:
                del tasks[name]
            for task in tasks.values():
                if task['deps'] & removed:
                    task['deps'] = (task['deps'] - removed) | {'metadata'}
                task['priority_predecessors'] -= removed
                task['reservations'] = []
                task['window_policy'] = None
            add('metadata', 'keys', 'BEGIN; DO $metadata$ BEGIN ' + body +
                ' END $metadata$; COMMIT;', indexes | early_dimensions, locks, 1, 1)
            # Keep one durable transaction per validation while reusing each
            # child's connection. This removes short-session fixed costs.
            for child in chains:
                chain = [t for t in tasks.values() if t['kind'] == 'validate'
                         and t['name'].startswith('validate:' + child + ':')]
                ordered = []
                while chain:
                    ready = next(t for t in chain if not (t['deps'] & {v['name'] for v in chain}))
                    ordered.append(ready)
                    chain.remove(ready)
                names = {t['name'] for t in ordered}
                deps = set().union(*(t['deps'] for t in ordered)) - names
                locks = {}
                for task in ordered:
                    for relation, mode in task['locks'].items():
                        _add_lock(locks, relation, mode)
                    del tasks[task['name']]
                name = add('validate:' + child, 'validate', '\n'.join(t['sql'] for t in ordered),
                           deps, locks, max(t['memory_mb'] for t in ordered),
                           sum(t['cost'] for t in ordered))
                tasks[name]['memory_policy'] = {'tier': 'sequential_child_chain',
                                                'validations': [t['memory_policy'] for t in ordered]}

    shared_fk_proof.rewrite_dag(job,tasks,specs,rows,shared_total)
    downstream = {name: set() for name in tasks}
    for name, task in tasks.items():
        for dependency in task['deps'] | task['priority_predecessors']:
            if dependency not in tasks:
                raise ValueError('Missing publication dependency: ' + dependency)
            downstream[dependency].add(name)
    descendants = {}

    def reachable(name, visiting):
        if name in visiting:
            raise ValueError('Cyclic publication dependency: ' + name)
        if name not in descendants:
            result = set(downstream[name])
            for child in downstream[name]:
                result.update(reachable(child, visiting | {name}))
            descendants[name] = result
        return descendants[name]

    for name, task in tasks.items():
        task['priority'] = task['cost'] + sum(
            tasks[child]['cost'] for child in reachable(name, set()))
        task['deps'] = sorted(task['deps'])
        task['priority_predecessors'] = sorted(task['priority_predecessors'])
    for task in tasks.values():
        if any(rel == (schema, 'table1') for rel in task['locks']) and (
                task['name'].startswith('index:table1') or task['name'].startswith('validate:table1')):
            task['priority'] += sum(t['cost'] for t in tasks.values())
    for child, keys in chains.items():
        if child not in inbound:
            continue
        longest = max(keys, key=lambda key: (fk_cost(key), key['name']))
        task = tasks.get('validate:' + child + ':' + longest['name'])
        if task is None:  # Small batched workloads already wait for metadata.
            continue
        # Preserve frozen priorities and admission; only close the lock window
        # before the longest validation. Index estimates can miss the last child.
        task['deps'] = sorted(set(task['deps']) | {
            name for name in foreign_tasks.values()
            if any(not _compatible(mode, tasks[name]['locks'][relation])
                   for relation, mode in task['locks'].items()
                   if relation in tasks[name]['locks'])})
    # Cheap attachments release dependencies before more long work starts.
    # Parent key builds receive all downstream work in their priority.
    return sorted(tasks.values(), key=lambda t: (
        t['kind'] in ('keys', 'attach_fk'), t['priority'], t['cost'], t['name']),
        reverse=True)


def render_runner(service, database, job, tasks, workers, memory_mb, cpu_budget=1, parallel_limit=0, persistent=False):
    """Render one remote Bash worker pool; SQL always travels through -c."""
    job_id = job['job_id']
    if not re.fullmatch(r'[a-f0-9]{32}', job_id):
        raise ValueError('Invalid shadow job ID')
    position = {task['name']: i for i, task in enumerate(tasks)}
    command = ['psql', '-h', 'localhost', '-U', service.sql_username,
               '-d', database, '-XqAt', '-v', 'ON_ERROR_STOP=1']
    application = 'pgdm_shadow:' + job_id
    cancel_sql = ("SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE "
                  f"application_name={sql_literal(application)} AND state='active' "
                  "AND pid<>pg_backend_pid();")
    conflicts = [[] for _ in tasks]
    for i, left in enumerate(tasks):
        for j in range(i):
            right = tasks[j]
            if any(not _compatible(mode, right['locks'][relation])
                   for relation, mode in left['locks'].items()
                   if relation in right['locks']):
                conflicts[i].append(j)
                conflicts[j].append(i)

    reservation_specs = []
    reservation_ids = {}
    task_reservations = []
    for task in tasks:
        guards = []
        for reservation in task.get('reservations', []):
            target = position[reservation['attach']]
            ready_after = tuple(position[name] for name in reservation['ready_after'])
            key = (target, ready_after)
            if key not in reservation_ids:
                reservation_ids[key] = len(reservation_specs)
                reservation_specs.append(key)
            guards.append(reservation_ids[key])
        task_reservations.append(guards)

    def array(name, values):
        return name + '=(\n' + '\n'.join(shlex.quote(str(v)) for v in values) + '\n)\n'

    header = '\n'.join([
        'set -euo pipefail',
        'export PGAPPNAME=' + shlex.quote(application),
        'psql_cmd=(' + ' '.join(shlex.quote(arg) for arg in command) + ')',
        f'exec 9>/tmp/pgdm-shadow-{job_id}.lock',
        'flock -x 9',
        'cancel_sql=' + shlex.quote(cancel_sql),
        f'limit={int(workers)}',
        f'memory_limit={int(memory_mb)}',
        f'total={len(tasks)}',
        f'cpu_limit={max(1,int(cpu_budget))}',
        f'parallel_limit={max(0,int(parallel_limit))}',
        array('parallel_requests', [t['parallel_workers'] for t in tasks]),
        f'reservation_total={len(reservation_specs)}',
        array('sqls', ['SET application_name=' + sql_literal(application) + ';\n' + SESSION_WAL_SQL + t['sql'] for t in tasks]),
        array('labels', [t['name'] for t in tasks]),
        array('kinds', [t['kind'] for t in tasks]),
        array('memories', [t['memory_mb'] for t in tasks]),
        array('dependencies', [' '.join(str(position[d]) for d in t['deps']) for t in tasks]),
        array('conflicts', [' '.join(map(str, group)) for group in conflicts]),
        array('reservations', [' '.join(map(str, group)) for group in task_reservations]),
        array('reservation_targets', [target for target, _ in reservation_specs]),
        array('reservation_ready', [' '.join(map(str, ready))
                                    for _, ready in reservation_specs]),
    ])
    body = r'''
active=()
states=()
pids=()
reservation_logged=()
used_memory=0
used_cpu=0
used_parallel=0
participants=()
granted_parallel=()
completed=0
failed=0
for ((i=0;i<total;i++)); do states[i]=pending; done
stop_workers() {
  "${psql_cmd[@]}" -c "$cancel_sql" </dev/null >/dev/null 2>&1 || true
  for i in "${active[@]}"; do kill -TERM "${pids[i]}" 2>/dev/null || true; done
  wait || true
}
trap 'stop_workers; exit 130' HUP INT TERM
reap_finished() {
  local i status
  local -a running=()
  for i in "${active[@]}"; do
    if kill -0 "${pids[i]}" 2>/dev/null; then
      running+=("$i")
    else
      status=0
      wait "${pids[i]}" || status=$?
      used_memory=$((used_memory-memories[i]))
      used_cpu=$((used_cpu-participants[i]))
      used_parallel=$((used_parallel-granted_parallel[i]))
      if ((status)); then
        failed=1
        states[i]=failed
      else
        states[i]=done
        completed=$((completed+1))
      fi
      printf 'PGDM_DAG_FINISH\t%s\t%s\t%s\n' "${labels[i]}" "$status" "${EPOCHREALTIME:-0}"
    fi
  done
  active=("${running[@]}")
}
ready() {
  local i=$1 d other guard target reserve seen
  [[ ${states[i]} == pending ]] || return 1
  for d in ${dependencies[i]}; do
    [[ ${states[d]} == done ]] || return 1
  done
  for other in ${conflicts[i]}; do
    [[ ${states[other]} != running ]] || return 1
  done
  for guard in ${reservations[i]}; do
    target=${reservation_targets[guard]}
    [[ ${states[target]} == done ]] && continue
    reserve=1
    for d in ${reservation_ready[guard]}; do
      if [[ ${states[d]} != done ]]; then reserve=0; break; fi
    done
    if ((reserve)); then
      seen=$((i*reservation_total+guard))
      if [[ ${reservation_logged[seen]:-0} != 1 ]]; then
        printf 'PGDM_DAG_RESERVE\t%s\t%s\t%s\n' "${labels[i]}" "${labels[target]}" "${EPOCHREALTIME:-0}"
        reservation_logged[seen]=1
      fi
      return 1
    fi
  done
  ((used_cpu<cpu_limit)) || return 1
  local reserved_memory=0 raw
  for ((raw=0;raw<total;raw++)); do
    if [[ ${labels[raw]} == validate:table1:*id_column_3_fkey && ${states[raw]} == pending && $i != "$raw" ]]; then
      if ((memory_limit>=2*memories[raw])); then reserved_memory=${memories[raw]}; fi
    fi
  done
  ((used_memory+memories[i]+reserved_memory<=memory_limit)) || return 1
  return 0
}
while ((completed<total)); do
  reap_finished
  if ((failed)); then stop_workers; exit 1; fi
  launched=0
  for ((i=0;i<total;i++)); do
    if ((${#active[@]}>=limit)); then break; fi
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
      sql=${sqls[i]//__PGDM_PARALLEL_WORKERS__/$granted}
      printf 'PGDM_DAG_CPU\t%s\t%s\t%s\t%s\t%s\n' "${labels[i]}" "$granted" "${participants[i]}" "$((memories[i]/participants[i]))" "${EPOCHREALTIME:-0}"
      printf 'PGDM_DAG_START\t%s\t%s\t%s\t%s\n' "${labels[i]}" "${kinds[i]}" "${memories[i]}" "${EPOCHREALTIME:-0}"
      "${psql_cmd[@]}" -c "$sql" </dev/null &
      pids[i]=$!
      active+=("$i")
      launched=1
    fi
  done
  if ((completed==total)); then break; fi
  if ((${#active[@]}==0)); then
    printf 'Publication DAG has pending work but no runnable task\n' >&2
    exit 2
  fi
  if ((launched==0)); then
    # A child may finish after reap_finished but before wait -n. An empty
    # unwaited set returns 127; explicit wait above retrieves its real status.
    wait -n || true
  fi
done
exit 0
'''
    if persistent:
        # Reuse exactly the same dependency, lock and admission predicates.
        ready = body[body.index('ready() {'):body.index('while ((completed<total));')]
        pool = Path(__file__).with_name('small_publication_runner.sh').read_text()
        pool = pool.replace('__PGDM_POOL_PATH__', shlex.quote('/tmp/pgdm-dag-' + job_id + '.pool'))
        return header + pool.replace('__PGDM_READY_FUNCTION__', ready)
    return header + body


def publication_waves(service, database, job, workers, report, publish=None):
    """Publish pending work across phase boundaries; keep ordinary PG checks."""
    workers = int(workers)
    if not 1 <= workers <= 32:
        raise ValueError('Publication workers must be between 1 and 32')
    snapshot = _snapshot(service, database, job, report)
    job['payload'] = snapshot['payload']
    budget = admission_budget(snapshot['memory'], snapshot['shared_buffers_mb'],
                              job['payload'].get('publication_memory_mb'),
                              planned_tasks=min(workers, max(1, int(snapshot['memory'].get('effective_cpus', workers)))))
    shared_total = budget['shared_total_mb']
    index_total = fk_total = shared_total
    tasks = build_dag(job, snapshot, workers, index_total, fk_total, shared_total)
    small = int(job['payload']['catalog'].get('scope_rows', 0)) <= 1000000
    if small and publish is not None:
        # The original locked publication SQL follows all durable DAG commits.
        # Building it here uses the finalized, exact dimension counts.
        tasks.append(dict(name='publish', kind='publication', sql=publish(),
            deps=[task['name'] for task in tasks], locks={}, memory_mb=1,
            cost=1, priority=1, parallel_workers=0, priority_predecessors=[],
            reservations=[], window_policy=None, memory_policy=None))
    if report is not None:
        # Compare the durable catalog with a fresh DAG to name skipped work.
        fresh = dict(snapshot, valid_indexes=[], constraints=[])
        fresh_tasks = build_dag(job, fresh, workers, index_total, fk_total, shared_total)
        pending_names = {task['name'] for task in tasks}
        report['publication_tasks_reused'] = (sorted(
            task['name'] for task in fresh_tasks if task['name'] not in pending_names)
            if report.get('shadow_resuming') else [])
        report['raw_fk_proof'] = snapshot.get('raw_fk_proof')
        report['publication_scheduler'] = report['native_publication_scheduler'] = SCHEDULER_VERSION
        report['publication_admission'] = budget
        report['native_publication_tasks'] = [
            {'name': task['name'], 'kind': task['kind'],
             'deps': task['deps'], 'memory_mb': task['memory_mb'],
             'cost': task['cost'], 'priority': task['priority'],
             'parallel_workers': task['parallel_workers'],
             'priority_predecessors': task['priority_predecessors'],
             'reservations': task['reservations'],
             'window_policy': task['window_policy'],
             'memory_policy': task['memory_policy'],
             'locks': [{'schema': relation[0], 'table': relation[1], 'mode': mode}
                       for relation, mode in sorted(task['locks'].items())]}
            for task in tasks]
        report['native_publication_memory_mb'] = shared_total
        report['native_publication_memory_budgets'] = {
            'index_total_mb': index_total, 'fk_total_mb': fk_total,
            'shared_total_mb': shared_total,
            'policy': 'size_aware'}
        report['native_publication_log'] = ''
    if not tasks:
        return
    limits = snapshot.get('cpu_limits', {})
    cpu_budget = max(1, int(snapshot['memory'].get('effective_cpus', 1)))
    parallel_limit = max(0, min(int(limits.get('max_worker_processes', 0)), int(limits.get('max_parallel_workers', 0))) - int(limits.get('busy_parallel_workers', 0)))
    report['publication_cpu'] = dict(limits, effective_cpus=cpu_budget)
    script = render_runner(service, database, job, tasks, workers, shared_total, cpu_budget, parallel_limit,
                           persistent=small)
    report['publication_session_policy'] = 'persistent_small_slots' if small else 'per_task'
    with service.expansion_timing_scope(
            report, 'execute', 'S3 DAG: overlapping durable indexes and FK validation',
            record_local_cpu=False):
        output = service.run_remote_command('bash -s', stdin_text=script)
        if report is not None:
            report['native_publication_log'] = output
            report['publication_tasks'] = report['native_publication_tasks']
            report['publication_timeline'] = [line.split('\t') for line in output.splitlines()
                                               if line.startswith('PGDM_DAG_')]
            service._append_expansion_timing(
                report, 'information', 'Publication grants and timeline', 0,
                'publication_information', include_in_total=False,
                details=json.dumps({'admission': budget,
                                    'tasks': report['publication_tasks'],
                                    'timeline': report['publication_timeline']}, indent=2))
    if small and publish is not None:
        return next(json.loads(line) for line in output.splitlines()
                    if line.startswith('{') and 'expansion_job_id' in json.loads(line))
