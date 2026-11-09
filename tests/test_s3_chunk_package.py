"""Verify SQL manifest policy and chunk boundaries without generating input."""
import copy
import json
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from services import s2_expansion as s2, s3_expansion as s3
from services.expansion_optimizer import build_plan
from services.postgres_service import PostgresAdminService


class S3ChunkPackageTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        recipes = Path(__file__).resolve().parents[1] / 'SQL files'
        requested = [dict(table_name='public.'+p.stem.split('_', 1)[1],
                          version_title='Test', sql=p.read_text()) for p in sorted(recipes.glob('*.sql'))]
        cls.requested = requested
        cls.prepared = PostgresAdminService._prepare_cross_table_expansion('public.raw_data', ['group001'], requested)
        cls.plan = build_plan(cls.prepared, requested, 'compact')

    def initialize(self, rows, *, hints=True, tid=True, estimated=True, implementation=s3):
        catalog = dict(scope_rows=rows, pages=rows//4, workers=8, scope_count_basis=estimated,
                       native_hint_readback=hints, source=dict(oid=1, filenode=1),
                       constraints=[dict(table='table4', type='p', name='table4_pkey')],
                       targets=[dict(name='table'+str(i), oid=i+1, owner='owner', comment=None) for i in range(1,7)])
        payload = dict(raw_schemas=['group001'], chunk_kind='tid' if tid else 'raw_id', catalog=catalog)
        with patch.object(s2, 'read_json', return_value=None):
            s2.initialize_job(None, 'test', 'test', 'signature', catalog, payload, 250000, {}, self.plan, implementation)
        return payload

    def worker(self, payload):
        return s3.worker_procedure(dict(job_id='test', schema_name='shadow', payload=payload), self.plan, self.prepared)

    def test_large_tid_cap_requires_hint_readback(self):
        for estimated in (False, True):
            payload = self.initialize(10000000, estimated=estimated)
            self.assertEqual((payload['chunk_rows'], payload['chunk_span']), (62500, 15625))
            self.assertTrue(payload['manifest_from_stage'])
        self.assertEqual(self.initialize(10000000, hints=False)['chunk_rows'], 250000)
        self.assertEqual(self.initialize(10000000, tid=False)['chunk_rows'], 250000)
        self.assertEqual(self.initialize(10000000, implementation=None)['chunk_rows'], 250000)

    def test_execute_routes_large_s3_hint_policy_after_catalog(self):
        # Execute the real preparation/signature/dispatch/publication route;
        # only remote database work is replaced by deterministic receipts.
        for implementation, rows in ((s3, 100000), (s3, 1000000),
                                     (s3, 10000000), (None, 10000000)):
            with self.subTest(implementation=implementation, rows=rows):
                catalog = dict(scope_rows=rows, estimated_rows=rows, pages=rows//4,
                    scope_count_basis=True, connection_budget=16, shared_buffers_mb=128,
                    source=dict(oid=1, filenode=1), database_oid=2, jobs=[],
                    constraints=[], indexes=[], external=[], fenced=False,
                    targets=[dict(name='table'+str(i), oid=i+1, kind='r',
                                  acl=None, rls=False) for i in range(1,7)])
                captured, jobs, reads = [], [], []
                def remote(command, stdin_text):
                    captured.append(stdin_text)
                    return '8\n'+json.dumps(catalog)
                service = SimpleNamespace(sql_username='owner', run_remote_command=remote,
                    expansion_timing_scope=lambda *args: nullcontext(),
                    _notify_progress=lambda *args: None)
                def initialize(service, database, job_id, signature, metadata, payload,
                               chunk_rows, report, plan, implementation):
                    payload.update(chunk_rows=chunk_rows,
                                   dimension_rows={'table3': 0, 'table4': 0})
                    job = dict(job_id=job_id, schema_name='shadow', payload=payload)
                    jobs.append(job)
                    return job
                result = {'dependency_manifest': {'expected_row_count': rows}}
                def read(service, database, sql, report, name, **kwargs):
                    reads.append(name)
                    if name == 'Read large S3 hint read-back policy':
                        return {'native_hint_readback': True}
                    if name.startswith('S2 wave 2:'):
                        return jobs[0]['payload']
                    if name.startswith('S2 wave 4:'):
                        return result
                    self.fail('Unexpected database read: '+name)
                loaded = dict(reused_at_launch=0, done=1, total=1,
                              rows={table: 0 for table in s2.TABLES})
                with patch.object(s2, 'initialize_job', side_effect=initialize), \
                     patch.object(s2, 'read_json', side_effect=read), \
                     patch.object(s2, 'build_publish_sql', return_value='PUBLISH'), \
                     patch.object(s2, 'parallel_wave'), \
                     patch.object(s3, 'load_and_publish', return_value=loaded) as s3_load, \
                     patch.object(s2.base, 'load_chunks', return_value=loaded) as s2_load:
                    actual = s2.execute(service, 'test', copy.deepcopy(self.prepared),
                        self.requested, [], {}, implementation=implementation)
                self.assertIs(actual, result)
                self.assertEqual(s3_load.call_count, int(implementation is s3))
                self.assertEqual(s2_load.call_count, int(implementation is None))
                self.assertNotIn("'native_hint_readback'", captured[0])
                expected_hint = implementation is s3 and rows > 1000000
                self.assertEqual(reads.count('Read large S3 hint read-back policy'),
                                 int(expected_hint))
                self.assertEqual('native_hint_readback' in jobs[0]['payload']['catalog'],
                                 expected_hint)

    def test_direct_execute_gates_metadata_and_reaches_initialization(self):
        for rows in (1000000, 1000001):
            catalog=dict(scope_rows=rows,pages=rows//4,workers=8,scope_count_basis=True,
                source=dict(oid=1,filenode=1),database_oid=1,jobs=[],fenced=False,external=[],
                estimated_rows=rows,targets=[dict(name='table'+str(i),oid=i+1,owner='owner',
                    kind='r',acl=None,rls=False,comment=None) for i in range(1,7)])
            service=SimpleNamespace(sql_username='owner',
                run_remote_command=lambda *args,**kwargs:'8\n'+json.dumps(catalog),
                expansion_timing_scope=lambda *args: nullcontext())
            with patch.object(s2.base,'validate_catalog'), patch.object(s3,'choose_workers',return_value=8), \
                 patch.object(s2,'read_json',return_value=dict(native_hint_readback=True)) as metadata, \
                 patch.object(s2,'initialize_job',side_effect=RuntimeError('Captured initialize')) as initialize:
                with self.assertRaisesRegex(RuntimeError,'Captured initialize'):
                    s2.execute(service,'test',self.prepared,self.requested,[],{},implementation=s3)
                self.assertEqual(metadata.call_count,int(rows>1000000))
                self.assertEqual(initialize.call_count,1)
                self.assertEqual(initialize.call_args.args[5]['catalog'].get('native_hint_readback'),True if rows>1000000 else None)

    def test_resume_manifest_policy_survives_scope_estimate_crossing(self):
        from services.expansion_recovery import fingerprint
        logical=fingerprint(self.prepared,[])
        source=dict(oid=1,filenode=1)
        for persisted,estimate in ((True,1000000),(False,1000001)):
            job=dict(source_identity=source,payload=dict(base_signature=logical,manifest_from_stage=persisted))
            catalog=dict(source=source,jobs=[job],scope_rows=estimate)
            self.assertEqual(s2.large_s3_policy(catalog,self.prepared,[]),persisted)
            job['payload']['base_signature']='different scope or versions'
            self.assertEqual(s2.large_s3_policy(catalog,self.prepared,[]),estimate>1000000)

    def test_small_boundary_preserves_one_wave_and_worker_sql(self):
        for rows in (100000, 1000000):
            payload = self.initialize(rows)
            self.assertEqual(payload['chunk_rows'], rows//8)
            self.assertEqual(payload['chunk_span'], rows//32)
            self.assertNotIn('manifest_from_stage', payload)
            self.assertEqual(self.worker(payload), self.worker(dict(chunk_kind='tid', small_manifest_from_stage=True)))

    def test_large_stage_manifest_keeps_collection_memory_and_commit_authority(self):
        payload = self.initialize(10000000)
        sql = self.worker(payload)
        self.assertIn('SELECT r.raw_id,r.raw_schema,r.raw_hash,', sql)
        self.assertIn('FROM pg_temp.pgdm_compact_stage GROUP BY raw_schema,raw_hash;', sql)
        self.assertNotIn('FROM public.raw_data r WHERE', sql)
        self.assertEqual(sql.count('32768'), 4)
        self.assertLess(sql.index("SET state='done'"), sql.index(' COMMIT;'))
        self.assertLess(sql.index(' COMMIT;'), sql.index('hint_tick:=clock_timestamp()'))
        legacy = copy.deepcopy(payload)
        legacy.pop('manifest_from_stage')
        self.assertIn('FROM public.raw_data r WHERE', self.worker(legacy))
        self.assertEqual(self.worker(legacy).count('32768'), 4)


if __name__ == '__main__':
    unittest.main()
