# Faster, resumable Expand: one-pass decoding and parallel chunks

- Repository: https://github.com/thisray/2026_PostgreSQL_Pipeline_Hackathon
- Submission commit: given in the submitted Kaggle Writeup; branch `main` points to it.
- Setup, benchmark and recovery tests: `README.md` in the repository.

On a fresh official 10M-row database, Expand through the Windows GUI → SSH → Linux path finishes in 64 s with our C extensions installed. The original code took 743 s in our earlier run, on a different PostgreSQL version and CPU allotment (section 3). An interrupted Expand resumes from its committed work instead of starting over, and a missing version registration is completed by running Expand again. The output matched our reference outputs in every checked run; some surrogate ids, notably in table2, differ from the original's (section 6). All numbers here are our own local measurements, not official scores.

## 1. Where the original spends its time

At 10M rows the original Expand took 743.4 s. Of that, 736.9 s (99.1%) went to six `INSERT ... SELECT` statements: table2 298 s, table1 192 s, table6 114 s, table5 99 s, table3 20 s, table4 14 s. They run one after another, each one reads and decodes the whole JSONB source again, and each inserts into tables that already have their primary keys, foreign keys and indexes.

The operation also has no checkpoints. An interruption loses all work done so far. Versions are registered afterwards in a separate control database (`postgres_data_manager`), so a failure at the wrong moment leaves published data without a version record.

## 2. What we changed

The Windows GUI → SSH → Linux PostgreSQL architecture, the official generator and the six recipes stay as they are. When the application recognizes the six official recipes, it runs the following plan instead of the six statements. Any other recipe set uses the original code.

1. Split `raw_data` into chunks and decode each chunk once. The four large tables (table1, table2, table5, table6) come from that single decode; the dimension tables table3 and table4 are built from the values actually seen.
2. Load the chunks in parallel into private shadow tables (same columns as the final tables, in a separate schema). Each chunk's rows and its "done" record commit in the same transaction.
3. Build the indexes and validate the foreign keys in parallel, within a shared memory limit.
4. Recheck that source and destinations are unchanged and that everything is complete, then move the six shadow tables into `public` in one transaction. They become the final tables, so the output is never copied a second time.

Database side:

- Two optional C extensions, `pgdm_decode` and `pgdm_bulk`, decode the JSON and write rows in bulk. If they are missing or the installed build does not match, the application runs the same plan in plain SQL: slower, same output. The report names the mode: `S4_NATIVE_BULK` with the extensions, `S3` in SQL-only mode.
- Only the JSON fields present in each row are extracted, and for table2 only the occupied position slots (up to 14 per row) are expanded.
- Nothing is hard-coded or precomputed. Row counts, dimension tables and values all come from the data; the only shape constant is the 14 slots of the official table2 recipe.
- Foreign keys are still created and checked by PostgreSQL's own `ALTER TABLE ... VALIDATE CONSTRAINT`. While the indexes are built, the extension collects the keys that actually occur in the stored parent and child rows. If those key sets are complete and the tables have not changed since, validation uses them instead of scanning the child table again; otherwise PostgreSQL scans as usual. In 29 test cases, 11 of them with invalid data (orphan child rows or missing parent keys, also combined with NULL keys and dead tuples), validation rejected every invalid case.
- Index builds and validations share one memory pool of at most 8192 MiB, with `work_mem` capped at 64 MiB per task.

Python side:

- One combined preflight replaces many separate SSH round trips for the source, destination and function checks.
- The application plans the chunks, starts and watches the server-side workers, schedules the index and validation tasks, and writes the timing report, including how many chunks were reused.
- Version registration is safe to repeat: the data transaction records what still has to be registered, and the control-database step runs under a lock with one completion record per job.
- A new menu entry, Expand → Discard interrupted expansion, abandons a pending job.

Everything the original measured (preparation, checks, loading, index and constraint builds, publication, version registration, cleanup) is still inside `MEASURED PROCESSING TOTAL`. Phases that run in parallel count once.

## 3. Results (local measurements)

Fresh official data, real Windows GUI on a desktop PC → SSH → Linux server (PostgreSQL in a container on an NVIDIA GB10 machine, ARM64), fsync and synchronous commit on. Both runs produced 53,225,999 output rows at 10M:

| | Original | Submitted (extensions installed) |
| --- | ---: | ---: |
| 10M rows, `MEASURED PROCESSING TOTAL` | 743.4 s | 64.2 s |
| 10M rows, wall time of the GUI operation | 748.7 s | 68.3 s |
| 1M rows, `MEASURED PROCESSING TOTAL` | not measured | 3.4 s |
| PostgreSQL | 16.13, `shared_buffers` 2 GiB, `max_wal_size` 16 GiB | 18, default settings |
| CPU | 8-CPU container | 3 cores of a shared, busy host |
| Date | 2026-10-29 | 2026-11-07 |

That is about 11.6× faster, but it is not a controlled comparison: the PostgreSQL version, CPU allotment and date differ, and autovacuum was still running on the source table during the original run.

Clean x86 VM: AMD EPYC 7B12, 4 vCPU, 30 GiB RAM, PostgreSQL 18.6 with default settings, fsync and data checksums on, one fresh 10M input, run without the GUI by calling the application's Expand code directly. This used the build just before the submitted one. The only difference is that foreign-key validation now uses the collected key sets in every case, including small inputs and small memory pools; on the default path this made no measurable difference at 10M.

| Mode | Total | CPU seconds | Peak RAM | Temporary files |
| --- | ---: | ---: | ---: | ---: |
| Extensions, run 1 | 144.7 s | 153.3 | 1.33 GB | 0 |
| Extensions, run 2 | 126.4 s | 134.2 | 1.50 GB | 0 |
| SQL-only | 244.7 s | 707.4 | 3.25 GB | 0 |

CPU seconds are counted within the timed operation. Peak RAM is the sampled anonymous plus shared memory of the whole test container. Temporary files are the sort and hash spills PostgreSQL wrote; temporary tables are not included. Seconds from different machines are not comparable.

**Correctness.** After each measured run, all six tables were compared with a reference output in both directions with `EXCEPT ALL` (all columns including ids, duplicates counted), with no differences. The references came from earlier builds of this solution, which were checked against the original code during development: at 100k rows column by column (surrogate ids excluded), at 10M by row counts and 64-bit fingerprints. Foreign-key anti-joins found no orphan rows, and the sequences, the six versions with their dependencies and the job's completion record were checked. An earlier release of this repository was also installed from scratch on a clean machine (Ubuntu, distribution PostgreSQL 18, ordinary role, real Windows GUI) and passed the same checks at 1M and 10M, and at 100k without the extensions.

## 4. Resource usage

- **RAM.** In the submitted build's GUI runs the sampled peak was 1.56 GB at 10M and 0.27 GB at 1M. On the VM it was 1.3–1.5 GB with the extensions and 3.25 GB in SQL-only mode. The 8192 MiB pool limits only index and validation memory; `shared_buffers`, the loading workers and the OS cache come on top.
- **Temporary disk.** PostgreSQL wrote no temporary files in the VM runs or in the submitted build's GUI runs. Larger inputs or a smaller memory pool can make index sorts spill. SQL-only mode also keeps one temporary stage table per active chunk.
- **Extra storage.** The shadow tables become the final tables, so there is no second copy of the output (at 10M the tables and their indexes reached 5.0 GB). What remains afterwards is the job, chunk, version and completion records: about 0.7 MB at 10M, 1.3 MB in SQL-only mode (measured on an earlier build). The original raw dumps and version history are kept.

## 5. Checkpoints and recovery

While a job is pending, writes to its source table are blocked, and locks keep two workers from loading the same chunk. Running the same Expand again resumes the job: after checking that source, recipes, mode and schema are unchanged, it reuses the committed chunks and any valid indexes and constraints. Killing the client does not stop the server-side workers, which keep loading; on rerun the application reuses every chunk they committed. Chunks in flight when PostgreSQL crashes are rolled back and redone.

| Interruption | Result |
| --- | --- |
| GUI process killed during loading (10M, real GUI) | Same job resumed, 160/160 chunks reused, output identical to the reference |
| PostgreSQL immediate stop during publication (10M)† | 160/160 chunks and 9 indexes reused; the rerun finished the remaining work in 6.7 s |
| Client interrupted in SQL-only mode (1M)† | 8/8 chunks reused, output identical |
| Interrupted job discarded† | Output tables empty, sequences unchanged, no versions recorded |
| Stopped after the data commit, before version registration (10M, early build) | Rerun registered the six versions in 7.6 s without reloading; repeating the registration step added none |

† Measured on an earlier build. README section 8 explains how to run these tests. OS crashes and power loss were not tested.

## 6. Limitations

- **Surrogate ids.** Row contents, row counts and foreign-key relations match the original, but some surrogate ids do not. table2 ids are `(raw_id − 1) × 14 + slot`: unique `bigint` values, but not the original's dense 1…N (at 10M the largest is 139,999,988 for 23,225,968 rows). table6 ids in the original depend on the query plan, and table4 ids were not compared. In an audit of an early build, table1, table3 and table5 ids were identical to the original's. Sequences continue after the largest id.
- The fast path covers the six official recipes on empty, ordinary destination tables (compatible grants, no row-level security, no outside dependencies). Other cases run the original code or stop with a clear message.
- The extensions need a one-time administrator install (C compiler, PostgreSQL server headers). PostgreSQL 18 is tested end to end; on 16 and 17 we only checked that the extensions compile.
- From 1M to 10M rows the total grew about 19× (3.4 s to 64.2 s). The largest input we ran is 30M rows, in a memory-pool test; there is no 300M run.
- Our timings come from a shared host and small VMs, where repeated runs under the same conditions differed by up to about 45%.
