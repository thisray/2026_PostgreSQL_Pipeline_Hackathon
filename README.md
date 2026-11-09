# PostgreSQL Data Manager: faster, resumable Expand

Built on the official challenge repository (<https://github.com/igorsiecz/kaggle_postgre_challenge>, base commit `5dfde9d967425982229b92d6ffe98efcd74612e9`), with its full history kept, so `git diff 5dfde9d HEAD` shows every change. It keeps the Windows GUI → SSH → Linux PostgreSQL design and makes **Expand** much faster and resumable after interruptions. `generate_database.py`, its fixed seed and the six recipes in `SQL files/` are unchanged (byte-identical to the base commit).

- Technical report: [release/WRITEUP.md](release/WRITEUP.md)
- Submission commit: given in the submitted Kaggle Writeup; branch `main` points to it.
- Measurements quoted here are our own local results, not official scores.

## Quick start

1. Linux server: create an ordinary `LOGIN CREATEDB` role and the loopback access rules (section 3).
2. Linux server, optional but 1.7–2.2× faster in our tests: build and install the PostgreSQL extensions with one command (section 4). Without them the application uses a slower SQL-only mode automatically.
3. Windows: create the conda environment and start `python main.py` (section 5).
4. Generate official data with the unchanged generator (section 6).
5. In the GUI, run Expand with the six official recipes. The **Expansion Performance Report** shows `MEASURED PROCESSING TOTAL` (section 7). To test recovery, interrupt and rerun the same Expand (section 8).

## 1. What changed

| Path | Purpose |
| --- | --- |
| `services/` | Expand engine: chunked loading, scheduling, publication, foreign-key checks, recovery. Entry point: `services/postgres_service.py` |
| `native/pgdm_decode`, `native/pgdm_bulk` | Optional PostgreSQL C extensions (JSON decoding, bulk row writes, foreign-key key sets) |
| `release/native_install.sh` | Builds and installs the extensions |
| `release/benchmark_gui.py` | Optional scripted run of the real GUI |
| `main.py`, `ui/` | GUI, with small changes for resume and Discard |
| `tests/` | Unit tests: `python -m unittest discover -s tests` |
| `UPSTREAM_README.md`, `GENERATE_DATABASE_README.md` | Original instructions |

## 2. Requirements

- **Linux server:** PostgreSQL 18 (tested end to end; 16 and 17 compile but were not fully tested), matching `psql`, `pg_dump` and `pg_restore`, OpenSSH server, Bash 4.3+, `flock`, GNU coreutils, awk, grep, `git`. No Python is needed on the server. The extensions also need a C compiler, `make` and the PostgreSQL server headers (PGXS).
- **Windows desktop:** interactive session, OpenSSH client, PostgreSQL client tools (`psql`), Tk, conda Python 3.12 (tested).
- **Database role:** an ordinary `LOGIN CREATEDB` role is enough.

Do not commit passwords, SSH keys, tokens, dumps or generated data.

## 3. Prepare the Linux server (once)

Use a dedicated test cluster. As the database administrator:

```sql
CREATE ROLE challenge_owner WITH LOGIN CREATEDB PASSWORD '<database-password>';
SHOW hba_file;
```

Add these rules before any conflicting rules in that file and reload PostgreSQL. The generator reaches PostgreSQL through an IPv4 SSH tunnel, while commands run on the server may use IPv6, so both rules are needed:

```conf
host all challenge_owner 127.0.0.1/32 scram-sha-256
host all challenge_owner ::1/128 scram-sha-256
```

Keep normal durability settings. Prepare a Linux SSH account that can run `psql`, `pg_dump`, `pg_restore`, Bash and `flock`.

## 4. Install the extensions (optional, before generating data)

On the Linux server (Debian/Ubuntu and PostgreSQL 18 shown). The checkout must be readable by the `postgres` user, so a clone under `/opt` is used:

```bash
sudo apt-get install build-essential git postgresql-server-dev-18
sudo git clone https://github.com/thisray/2026_PostgreSQL_Pipeline_Hackathon /opt/pgdm-challenge
cd /opt/pgdm-challenge
sudo git checkout <commit hash stated in the Kaggle Writeup>
sudo bash release/native_install.sh \
  --pg-config /usr/lib/postgresql/18/bin/pg_config \
  --psql /usr/lib/postgresql/18/bin/psql
```

The script builds both extensions, installs them into `template1` (databases created afterwards inherit them) and checks the result. By default it uses local peer authentication as `postgres`; options: `--database`, `--host`, `--port`, `--username`, `--sudo`. It never takes a password on the command line, and it refuses to run while another session uses `template1`.

Check the installation (the second command needs the database generated in section 6):

```bash
sudo -u postgres psql -d template1 -Atc "SELECT extname,extversion FROM pg_extension WHERE extname LIKE 'pgdm%' ORDER BY 1"
psql -h localhost -U challenge_owner -d kaggle_challenge -Atc "SELECT public.pgdm_decode_build_id(), public.pgdm_bulk_build_id()"
```

Expected versions: `pgdm_decode` 1.1 and `pgdm_bulk` 1.5. A database generated before the installation needs `--database kaggle_challenge` or regeneration.

At the start of every Expand the application checks the extension versions and build and runs a rollback-only self-check, then picks the mode. The report shows `Engine` (`S4_NATIVE_BULK` with extensions, `S3` for SQL-only mode) and a `Fallback` reason if the extensions were rejected. Do not install or change the extensions while an interrupted job is pending.

To uninstall: finish or discard pending jobs, drop both extensions from `template1` and from databases created while they were installed, and delete the library, control and SQL files that `pg_config` points to.

## 5. Set up the Windows GUI

```powershell
git clone https://github.com/thisray/2026_PostgreSQL_Pipeline_Hackathon challenge
cd challenge
git checkout <commit hash stated in the Kaggle Writeup>
conda create -y -n pg-judge python=3.12 pip
conda activate pg-judge
python -m pip install -r requirements.txt
conda install -n pg-judge postgresql
python -c "import tkinter"
Get-Command psql, ssh
```

## 6. Generate data and run Expand

Generate official data with the unchanged generator, from Windows, in the conda environment (it also creates the control database if needed):

```powershell
$env:PG_KAGGLE_CHALLENGE_PASS = '<database-password>'
python generate_database.py --rows 100000 --host <linux-address> --ssh_port <ssh-port> --ssh_user <linux-user> --postgres_port 5432 --user challenge_owner --admin_database postgres
Remove-Item Env:PG_KAGGLE_CHALLENGE_PASS
```

Use `--rows 1000000 --replace` or `--rows 10000000 --replace` for larger tests; `--replace` deletes the existing test database, so use it only on a dedicated one. See `GENERATE_DATABASE_README.md` for all options.

Then in the GUI (`python main.py`):

1. Connect: Linux SSH address, port, account and password; PostgreSQL port, role and password. For temporary tests disable "Remember credentials".
2. Select `kaggle_challenge` / `public.raw_data` as the source and keep `group001` (the generator's Raw group) as both ends of the Raw scope, as described in `GENERATE_DATABASE_README.md`.
3. Open Expand and add the six recipes as one batch, in this order:

| Destination | Recipe |
| --- | --- |
| public.table3 | SQL files/01_table3.sql |
| public.table4 | SQL files/02_table4.sql |
| public.table1 | SQL files/03_table1.sql |
| public.table5 | SQL files/04_table5.sql |
| public.table6 | SQL files/05_table6.sql |
| public.table2 | SQL files/06_table2.sql |

4. Run the expansion. The optimized path applies when the six destination tables are empty ordinary tables with compatible ownership and grants, no row-level security and no outside dependencies; otherwise the original code path runs or the application shows an explicit error. Regenerate the database before the next fresh benchmark.

## 7. Benchmark

For each fresh condition, generate new official data (no manual prewarming, `VACUUM` or `CHECKPOINT`), run the six recipes once and save the Expansion Performance Report window that opens when Expand finishes. Useful fields: `MEASURED PROCESSING TOTAL`, `Engine`, `Fallback`, and chunks reused/done/total.

Timing follows the upstream model: local Python CPU time, server wall time and PostgreSQL timings, without SSH transit. Preparation, loading, index builds, foreign-key validation, publication, version registration and cleanup are inside the total; phases that run in parallel count once.

Optional scripted run of the real GUI (interactive Windows desktop):

```powershell
python release/benchmark_gui.py --app . --output C:\benchmarks\run-10m --connection C:\private\connection.json
```

The connection JSON holds `host`, `ssh_port`, `ssh_username`, `ssh_password`, `postgres_port` (optional), `sql_username` and `sql_password`; keep it outside the repository. `--workers N` sets the concurrency and `--publication-memory-mb N` lowers the memory pool (32–8192).

To compare with the original code path, start the GUI with `PDM_EXPANSION_MODE=classic` (PowerShell: `$env:PDM_EXPANSION_MODE = 'classic'; python main.py`); `s3` forces SQL-only mode. The default is `auto`. No environment variable is required; the upstream `PDM_*` names are listed in `UPSTREAM_README.md`.

## 8. Test failure recovery

Use a fresh dedicated 1M or 10M database. Repeating the same Expand resumes the job; there is no separate Resume button. Source, recipes, destinations and extension build must stay unchanged between interruption and resume.

Follow the job in the database holding the data (`kaggle_challenge`). Save the output of these queries before interrupting and again after the rerun:

```sql
SELECT job_id, state, attempts FROM pgdm_runtime.shadow_jobs ORDER BY created_at DESC LIMIT 1;
SELECT job_id, chunk_id, committed_at, row_counts FROM pgdm_runtime.shadow_chunks
WHERE state = 'done' ORDER BY job_id, chunk_id;
SELECT pid, application_name, state, left(query, 80) FROM pg_stat_activity
WHERE application_name LIKE 'pgdm_shadow%';
```

1. **Client killed during loading.** After at least one chunk is committed, terminate the GUI process (Task Manager or `Stop-Process`). The server workers keep running and finish their chunks; wait until they are idle, relaunch the GUI and run the same Expand. Expect the same job, a higher attempt count, reused chunks in the report, and the chunk rows committed before the rerun keeping their original `committed_at`.
2. **Client killed during indexing.** Repeat on a fresh database while `CREATE INDEX` or `VALIDATE` shows in `pg_stat_activity`. Valid indexes and validated constraints are reused.
3. **PostgreSQL crash (dedicated cluster only).** Publication is short (seconds at 1M, about 45 s at 10M on our VMs). Once its index builds or `VALIDATE` appear in `pg_stat_activity`, stop the cluster immediately, for example `pg_ctl -D <data-directory> stop -m immediate` as the cluster owner, start it again and repeat Expand. Uncommitted work is rolled back, committed work is reused. This tests crash recovery, not OS power loss.
4. **SQL-only mode.** Repeat step 1 without the extensions. Do not switch modes while a job is pending.
5. **Version registration.** If the job state is `data_ready`, the data is published and only the version registration is missing; repeat Expand to finish it. This window is short and hard to hit by hand; the Writeup lists a test of it on an early build.
6. **Discard.** Interrupt a loading or indexing job, wait until its workers are idle and choose Expand → Discard interrupted expansion. Expect an `aborted` job, unchanged public tables and sequences, and no versions for that job. A `data_ready` job must be resumed instead.

What was reused (data database):

```sql
SELECT c.oid, c.relname, c.relfilenode, i.indisvalid FROM pg_index i
JOIN pg_class c ON c.oid = i.indexrelid
WHERE c.relnamespace::regnamespace::text LIKE 'pgdm\_shadow\_%';
SELECT oid, conname, contype, convalidated FROM pg_constraint
WHERE connamespace::regnamespace::text LIKE 'pgdm\_shadow\_%';
```

**Check the result.** Run the same Expand uninterrupted on identically generated data (same seed, same row count), then compare all columns of the six tables, ids included, with ordered-output hashes. Run this after the uninterrupted run and again after the interrupted-and-resumed run, and compare the six lines (psql asks for the password, or uses your usual password file):

```bash
for t in table1 table2 table3 table4 table5 table6; do
  echo -n "$t "
  psql -h localhost -U challenge_owner -d kaggle_challenge -Atc "COPY (SELECT * FROM public.$t ORDER BY 1) TO STDOUT" | sha256sum | cut -d' ' -f1
done
```

Foreign keys must all be validated, and each identity sequence must continue after the largest id (`is_called = false`; `raw_id` is a plain key without a sequence):

```sql
SELECT conrelid::regclass AS table_name, conname, convalidated FROM pg_constraint
WHERE contype = 'f' AND connamespace = 'public'::regnamespace;
SELECT last_value, is_called FROM public.table1_id_column_1_seq;   -- likewise table2, 3, 5, 6
```

In the control database (`postgres_data_manager`), replace `<job_id>` with the job from the first query above; six versions with dependencies and one completion record are expected:

```sql
SELECT v.table_name, v.version_code FROM app_control.table_versions v
JOIN app_control.table_version_dependencies d ON d.table_version_id = v.id
WHERE d.dependency_payload->>'expansion_job_id' = '<job_id>' ORDER BY 1;
SELECT count(*) FROM app_control.expansion_finalizations WHERE job_id = '<job_id>';
```

No active workers, shadow schema, source write block or invalid constraint should remain. The job, chunk and version records stay on purpose.

## 9. Integrity

Source and destination validation and version registration are preserved. Foreign keys are always created and validated by PostgreSQL's own `ALTER TABLE ... VALIDATE CONSTRAINT`. While loading, the extension records the keys that actually occur in the parent and child tables; only a complete key set for unchanged tables lets that command skip a redundant scan. A missing, incomplete or stale set triggers the normal full scan. No output values, row counts, dimension contents or distributions are hard-coded; the only data-shape constant is the 14 position slots per row that the official `table2` recipe expands. `tests/` covers recipe handling, chunk packages, job signatures, memory limits and the proof lifecycle.

## 10. Resources and objects created

- **Memory:** index builds and validations share one pool of at most 8192 MiB, sized from the memory actually available to the server (container or cgroup limit, with cache and reserve accounted for); settings can only lower it. `work_mem` is at most 64 MiB per task. The pool does not limit the whole server: `shared_buffers`, loaders, key sets, the OS cache and other sessions use memory too.
- **Disk:** with the extensions, rows are written straight into private logged tables that later become the final tables, so no second copy of the output remains. SQL-only mode uses a temporary stage per active chunk. Index sorts may spill to temporary files. Budget space for the source, output, indexes, temporary files and WAL separately; `max_wal_size` is a checkpoint target, not a disk limit.

| Object | Lifetime |
| --- | --- |
| `pgdm_decode`, `pgdm_bulk` extensions and their libraries | Installed by the administrator; stay in `template1` and inherited databases |
| `pgdm_runtime.shadow_jobs`, `shadow_chunks`, `shadow_hashes` | Job, chunk and source audit tables; kept after completion |
| `pgdm_runtime.shadow_codes` | Observed codes; rows removed on success or Discard |
| `pgdm_runtime.expansion_jobs` | Durable list of version registrations still to do; kept |
| Worker procedures, source write-block trigger | Removed on completion or Discard |
| `pgdm_shadow_<job_id>` schema | Private tables; moved into `public`, then the schema is dropped (or dropped on Discard) |
| `pgdm_fk_proof` tables | Key sets; released on publication or Discard |
| `app_control.expansion_finalizations` | One record per job; kept with the version records |
| `/tmp/pgdm-shadow-<job_id>.lock` on the server | Empty lock file; safe to remove when no job is active |

The application creates its own schemas automatically. The original application's raw dumps and version history are kept. After saving evidence and confirming no active or pending job, the owner may drop the whole dedicated test database.

## 11. Limitations and troubleshooting

- A pending job blocks writes to its source table until it is completed or discarded; changing source, recipes, mode or extension build requires completing or discarding it first.
- Surrogate ids are assigned deterministically. `table2` ids are `(raw_id - 1) * 14 + slot position`, not the original's dense range, and `table6` ids in the original depend on the query plan; we did not compare the ids of `table4`. Other columns, row counts and foreign-key relations matched the original in earlier comparisons. Details in the Writeup, section 6.
- No run at 300M rows and no OS crash or power-loss test.
- If the application uses SQL-only mode unexpectedly, read the `Fallback` line and compare the installed extension versions in the target database. If generation works but GUI authentication fails, check both loopback rules from section 3. The message `Shadow requires empty public.table1` means the database must be regenerated.

## 12. License and attribution

Based on the official challenge repository above, under the Apache License 2.0 (see `LICENSE`). Python dependencies (CustomTkinter, Paramiko, pandas, ReportLab, openpyxl, xlrd) keep their own licenses; see `requirements.txt`. The extensions link no additional libraries.
