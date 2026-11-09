SET statement_timeout='5min';
DO $claim$
DECLARE available boolean;
BEGIN
 LOOP
  -- Claimed chunks remain pending until commit. Test only the final row,
  -- releasing any acquired tuple lock in the exception subtransaction.
  BEGIN
   SELECT EXISTS(SELECT FROM pgdm_runtime.shadow_chunks
    WHERE job_id='__PGDM_JOB__' AND state='pending'
    AND chunk_id=(SELECT max(chunk_id) FROM pgdm_runtime.shadow_chunks WHERE job_id='__PGDM_JOB__')
    FOR UPDATE SKIP LOCKED) INTO available;
   RAISE no_data_found;
  EXCEPTION WHEN no_data_found THEN NULL;
  END;
  EXIT WHEN NOT available;
  PERFORM pg_sleep(0.01);
 END LOOP;
END $claim$;
SELECT (extract(epoch FROM clock_timestamp())*1e9)::bigint AS cancel_start_ns,
 (SELECT coalesce(json_agg(pid),'[]'::json) FROM pg_stat_progress_vacuum
  WHERE datid=(SELECT oid FROM pg_database WHERE datname=current_database())
  AND relid='public.raw_data'::regclass) AS vacuum_pids \gset
BEGIN;
SET LOCAL lock_timeout='5s';
LOCK TABLE public.raw_data IN SHARE ROW EXCLUSIVE MODE;
SELECT (extract(epoch FROM clock_timestamp())*1e9)::bigint AS acquired_ns \gset
COMMIT;
SELECT json_build_object('request_ns',:cancel_start_ns,'acquired_ns',:acquired_ns,
 'finished_ns',(extract(epoch FROM clock_timestamp())*1e9)::bigint,
 'vacuum_pids',:'vacuum_pids'::json,'mode','ShareRowExclusiveLock');
