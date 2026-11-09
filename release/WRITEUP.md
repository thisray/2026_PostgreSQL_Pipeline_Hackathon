# Faster, resumable Expand: one-pass decoding and parallel chunks

- Repository: https://github.com/thisray/2026_PostgreSQL_Pipeline_Hackathon
- Submission commit: given in the submitted Kaggle Writeup; branch `main` points to it.
- Setup, benchmark and recovery-test instructions: `README.md` in the repository.

**Summary.** On a freshly generated official 10M-row input, Expand through the Windows GUI → SSH → Linux path takes 64 s with the C extensions installed, against 743 s for the original code in an earlier run of the same kind (not a controlled comparison, see section 3). An interrupted expansion resumes from its committed work, and rerunning Expand completes any missing version registration. The six output tables matched our reference outputs in every checked run, and row contents matched the original code's output in our earlier comparisons. Known difference: some surrogate `id` values, notably in `table2`, are assigned by a formula and differ from the original's, and not all ids were compared (section 6). All numbers are our own local measurements, not official scores.

## 1. Bottlenecks in the original implementation

At 10M rows the original Expand took 743.4 s (Windows GUI → SSH → Linux server, PostgreSQL 16.13). Six `INSERT ... SELECT` statements make up 736.9 s of it (99.1%): table2 298 s, table1 192 s, table6 114 s, table5 99 s, table3 20 s, table4 14 s. They run one after another. Each re-reads and re-decodes the JSONB source, and inserts into tables that already carry primary keys, foreign keys and indexes.

The original also runs as one uninterrupted operation without checkpoints, and the version record is written afterwards in a separate database (the control database `postgres_data_manager`). An interruption therefore loses the work done so far, or leaves published data without a version record.

## 2. Strategy, architecture and optimizations

We keep the Windows GUI → SSH → Linux PostgreSQL architecture, the official generator and the six recipes. When the application sees the six official recipes, it replaces the six sequential statements with this plan; any other recipe set runs the original code path.

1. Split the source rows (`raw_data`) into chunks. Each chunk is decoded once; the four large tables (table1, table2, table5, table6) are derived from that single decode, and the two small dimension tables (table3, table4) from the values actually observed.
2. Load the chunks in parallel into private "shadow" tables (same columns as the final tables, in a separate schema). The chunk's rows and a "done" record for it (a row in a progress table) commit in the same transaction.
3. Build indexes and validate foreign keys concurrently, within CPU and memory limits.
4. Publish atomically: after rechecking that source and destination are unchanged and that all chunks, indexes and constraints are complete and valid, move the six shadow tables into `public` in one transaction. They become the final tables, so no second copy of the output exists.

**Database side**
- Two modes. Optional C extensions (`pgdm_decode`, `pgdm_bulk`) decode the JSON, build rows and write them in bulk. If they are missing or incompatible, the application automatically runs the same plan in SQL (slower, same outputs). The report names the mode with a label: `S4_NATIVE_BULK` for extension mode, `S3` for SQL-only mode.
- Only the fields actually present in each JSON row are extracted, and for `table2` only the populated position slots (up to 14 per row) are expanded.
- Nothing is precomputed or hard-coded: row counts, dimension tables and values all come from the data. The only data-shape constant is the 14 position slots per row that the official `table2` recipe expands.
- Foreign keys are still created and checked by PostgreSQL's own `ALTER TABLE ... VALIDATE CONSTRAINT`. While the indexes are built, the extension records which keys actually occur in each parent and child table, taken from the stored rows rather than from the source data. Inside the validation command it lets PostgreSQL answer "does every child key have a parent?" from those key sets instead of re-scanning the child table. This is used only when the sets are complete and the tables are unchanged since; in every other case PostgreSQL scans as usual. In our scratch tests (29 cases, 11 of them invalid data: child rows without a parent or with a missing parent key, also with NULL keys and dead tuples) validation rejected every invalid case.
- Index builds and validations run in parallel, drawing memory from one shared pool of at most 8192 MiB, with `work_mem` at most 64 MiB per task.

**Python side**
- One combined preflight (source, destinations, functions, counters) replaces many separate SSH commands.
- The application plans the chunks, starts and monitors the server-side workers, schedules the index and validation tasks and builds the timing report, which also shows reused chunks.
- Version registration survives interruption: the data transaction records what must be registered, and registration in the control database is safe to repeat (a lock plus one completion record per job).
- The GUI gains "Expand → Discard interrupted expansion" to abandon a pending job.

All work that the original counted in the metric (preparation, self-check, loading, index and constraint rebuilds, publication, version registration, cleanup) stays inside the measured total; parallel phases count once.

## 3. Measured improvements

**Original vs. submitted code**, fresh official data, Windows GUI → SSH → Linux server, 53,225,999 output rows at 10M in both runs:

| | Original | Submitted (extensions installed) |
| --- | ---: | ---: |
| 10M rows: `MEASURED PROCESSING TOTAL` | 743.4 s | 64.2 s |
| 10M rows: wall time of the whole GUI operation* | 748.7 s | 68.3 s |
| 1M rows: `MEASURED PROCESSING TOTAL` | not measured | 3.4 s (wall 6.9 s) |
| PostgreSQL | 16.13, `shared_buffers` 2 GiB, `max_wal_size` 16 GiB | 18, default settings |
| CPU | 8-CPU container | 3 CPU cores on a shared, busy host |
| Date | 2026-10-29 | 2026-11-07 |

\*Wall time includes SSH transit and GUI overhead that the official metric excludes (4.1 s for the submitted run, 5.3 s for the original).

The ratio is about 11.6×, but the runs are not a controlled comparison: the PostgreSQL major version, CPU allotment and date differ, and the original ran first while autovacuum was still active on the source table. Both ran through the real Windows GUI, with fsync and synchronous commit on. In the same GUI session as the submitted run, an earlier build in SQL-only mode took 141.8 s at 10M rows and 13.2 s at 1M.

**Clean x86 VM, run headless through the application service (no GUI)** (AMD EPYC 7B12, 4 vCPU, 30 GiB, PostgreSQL 18.6 with default settings, fsync and data checksums on, one shared fresh 10M input). The extensions were installed for the first two runs and absent for the third. The build is an earlier one that differs from the submitted code only by the proof changes described below, which did not measurably change the 10M default path.

| Mode | Total | CPU seconds in the timing scope | Peak RAM* | Temporary files written** |
| --- | ---: | ---: | ---: | ---: |
| Extensions, run 1 | 144.7 s | 153.3 | 1.33 GB | 0 bytes |
| Extensions, run 2 | 126.4 s | 134.2 | 1.50 GB | 0 bytes |
| SQL-only | 244.7 s | 707.4 | 3.25 GB | 0 bytes |

\*Sampled anonymous plus shared memory of the whole test container (PostgreSQL and application).

\*\*Cumulative bytes of temporary files PostgreSQL wrote (spills of sorts and hashes); temporary tables are not counted.

With the extensions this build was 1.7–1.9× faster than in SQL-only mode. These seconds differ from the Windows-GUI runs above because CPU, storage, PostgreSQL build and access path differ; compare only within a table.

Changes made after that session, measured separately: the foreign-key key-set collection no longer depends on the memory pool being large enough and is used in every validation transaction, and it is now also used for inputs up to 1M rows. On a 4-vCPU Intel Xeon VM, four 1M-row pairs all improved (total −28%, CPU −33%, the publication phase about 2.0 s instead of 4.8–7.3 s, excluding the first, cold run). At 10M no difference was measurable, so we claim no 10M gain. With a deliberately small 2 GiB pool at 30M rows, the index-and-validation phase improved in three paired tests (−12% to −52%).

**Output checks.** After each measured run the six tables were compared with a reference output, all columns including ids, duplicates counted, using `SELECT *` in both directions with `EXCEPT ALL`: no differences. The references came from earlier builds of this solution, which during development were compared with the original code's output (100k rows column by column without surrogate ids; 10M rows by row counts and 64-bit fingerprints). Our final-build checks use these references rather than the original code's output.

Foreign-key anti-joins found no orphan rows; identity sequences, the six versions with their dependencies and one completion record were verified. A clean-room run of the exported repository (Ubuntu with distribution PostgreSQL 18, ordinary role, real Windows GUI) passed the same checks at 1M and 10M, plus a 100k run without extensions.

## 4. Resource usage

- **RAM.** On the clean VM the extension build peaked at 1.3–1.5 GB and SQL-only mode at 3.25 GB. For the submitted version in the Windows-GUI runs the sampled peak was 1.56 GB at 10M rows and 0.27 GB at 1M (sampled values, not per-process RSS). The 8192 MiB cap limits only the memory grants for index builds and validation; `shared_buffers`, loaders, key sets, the OS cache and other sessions come on top.
- **Temporary disk.** PostgreSQL wrote no temporary files in the three VM runs above, nor in the 10M and 1M GUI runs. Larger inputs or smaller pools can spill: in one 30M-row pair with a 2 GiB pool, cumulative temporary writes fell from 7.19 GB to 3.35 GB with the proof change (not a peak-space measurement). SQL-only mode keeps a temporary table (stage) per active chunk, which does not show up as temporary files. WAL volume was not measured in the final runs.
- **Extra persistent storage.** The shadow tables are working storage until publication, then become the final tables; at 10M rows they and their indexes peaked at 5.0 GB (sampled). The job and chunk records, the version record and the completion record remain: about 0.7 MB (extensions) and 1.3 MB (SQL-only) at 10M, measured on an earlier build. Plan disk for the source table, output, indexes, temporary files and WAL separately. The original application's raw dumps and version history are kept.

## 5. Checkpoint, resume and failure recovery

Source writes are blocked while a job is pending, and locks prevent two workers from loading the same chunk. Repeating the same Expand reuses committed chunks and valid indexes and constraints, after checking that source, recipes, mode and schema are unchanged. The durable record written together with the data closes the gap between data publication and version registration.

| Interruption | Observed result |
| --- | --- |
| GUI process killed during loading, 10M, submitted version, real Windows GUI | Same job resumed; all 160 chunks reused; committed chunk records unchanged; six tables identical to the reference |
| Same test in the clean-room run of the exported repository | 3 of 160 chunks were committed at the kill; the server workers finished the rest; all 160 reused on rerun |
| PostgreSQL immediate stop and restart during publication, 10M* | Same job reused 160/160 chunks and 9 indexes; 6.7 s of work remained |
| Client interrupted in SQL-only mode, 1M* | Reused 8/8 chunks; output identical |
| Client interrupted near the end of a native 1M run* | Reused 8/8 chunks |
| Interrupted job discarded* | Empty output, sequences unchanged, no versions for the job |
| Stopped after the data commit but before version registration, 10M (early build) | Rerun registered the six versions and dependencies in 7.6 s without reloading; repeating the registration step added none |

\*Run on the build just before the submitted one.

Server workers keep running after the client is killed, so the first rows show that finished work is kept and not redone; they are not a mid-chunk restart. Chunks that were in flight when PostgreSQL crashed are rolled back and redone. To reproduce, follow README section 8. These tests do not cover OS crashes or power loss.

## 6. Known limitations and trade-offs

- **Surrogate ids.** Row contents, row counts and foreign-key relations match the original; some surrogate `id` values do not. In an audit of an early build, `table1`, `table3` and `table5` ids were identical to the original's. `table2` ids are `(raw_id − 1) × 14 + slot position` (`bigint`): unique, but not the original's dense 1…N range (at 10M rows the largest id is 139,999,988 for 23,225,968 rows). `table6` ids in the original depend on the query plan, and we did not compare the ids of `table4`. Sequences continue after the largest id.
- The fast path targets the six official recipes on empty, ordinary destination tables with compatible grants, no row-level security and no outside dependencies; other cases use the original path or fail with an explicit message. A pending job blocks writes to its source table.
- The extensions need one administrator installation (compiler, PostgreSQL server headers); without them the application is slower but correct. PostgreSQL 18 is tested end to end; 16 and 17 are only compile-checked.
- Scaling: from 1M to 10M rows the processing total grew about 19× (3.4 s to 64.2 s). The largest run is 30M rows (memory-pool test); there is no 300M run, and no OS-crash or power-loss test.
- Timings come from a shared host and small VMs; same-condition runs there differed by up to about 45%, and different machines are not comparable.
