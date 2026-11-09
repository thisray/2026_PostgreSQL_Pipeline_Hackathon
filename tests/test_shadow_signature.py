"""Keep repeated planning and legacy replay metadata safe across GUI restarts."""
import copy
import json
from pathlib import Path
import re
import unittest

from services.expansion_optimizer import build_plan
from services.expansion_recovery import fingerprint
from services.postgres_service import PostgresAdminService
from services import shadow_expansion as base
from services import shadow_signature as signatures


class ShadowSignatureTests(unittest.TestCase):
    def setUp(self):
        recipes=Path(__file__).resolve().parents[1]/'SQL files'
        self.requested=[dict(table_name='public.'+p.stem.split('_',1)[1],version_title='new title',sql=p.read_text())
            for p in sorted(recipes.glob('*.sql'))]
        self.prepared=PostgresAdminService._prepare_cross_table_expansion('public.raw_data',['group001'],self.requested)
        self.catalog=dict(source=dict(oid=1,filenode=1),database_oid=2,jobs=[])

    def canonical(self):
        prepared=copy.deepcopy(self.prepared)
        build_plan(prepared,self.requested,mode='compact')
        base.replay_children(prepared)
        base.replay_dimensions(prepared)
        return prepared

    def legacy(self,repeats=3):
        prepared=self.canonical()
        old=copy.deepcopy(prepared['destinations'])
        row=next(d for d in old if d['pure_table_name']=='table1')
        row['sql_recipe']=row['sql_recipe'].replace(signatures.TABLE1_PREFIX,
            'INSERT INTO public.table1 ('+'id_column_1,'*repeats,1)
        row['sql_recipe']=row['sql_recipe'].replace(signatures.TABLE1_SELECT,
            '\nSELECT\n'+'    source_row.raw_id,\n'*repeats,1)+signatures.TABLE1_SEQUENCE*(repeats-1)
        row['destination_columns']=['id_column_1']*(repeats-1)+row['destination_columns']
        for d in old:d['version_title']='original title'
        plan=dict(workers=['unchanged worker'],dimensions=['unchanged dimension'],
            recipes=[d['sql_recipe'] for d in prepared['destinations']])
        old_plan=dict(plan,recipes=[d['sql_recipe'] for d in old])
        logical=fingerprint(prepared,[])
        job=dict(job_id='a'*32,state='indexing',signature=signatures.plan_signature(self.catalog,logical,old_plan,'test'),
            source_identity=self.catalog['source'],payload=dict(base_signature=logical,destinations=old,codes_finalized=True))
        return prepared,plan,job

    def test_repeated_planning_is_idempotent(self):
        prepared=copy.deepcopy(self.prepared)
        build_plan(prepared,self.requested,mode='compact')
        expected=copy.deepcopy(prepared)
        for _ in range(4):
            build_plan(prepared,self.requested,mode='compact')
            self.assertEqual(prepared,expected)
        row=next(d for d in prepared['destinations'] if d['pure_table_name']=='table1')
        self.assertEqual(row['destination_columns'].count('id_column_1'),1)
        self.assertEqual(row['sql_recipe'].count(signatures.TABLE1_SEQUENCE),1)

    def test_recognizes_only_reviewed_legacy_metadata_with_identical_worker_plan(self):
        for repeats in (2,3,4):
            prepared,plan,job=self.legacy(repeats)
            self.catalog['jobs']=[job]
            signature,found,previous=signatures.resumable_job(self.catalog,prepared,[],plan,'test')
            self.assertIs(found,job)
            self.assertEqual(previous,job['signature'])
            self.assertNotEqual(signature,previous)
            changed=dict(plan,workers=['different worker'])
            self.assertIsNone(signatures.resumable_job(self.catalog,prepared,[],changed,'test')[1])
            job['payload']['destinations'][0]['sql_recipe']+=' SELECT 1;'
            self.assertIsNone(signatures.resumable_job(self.catalog,prepared,[],plan,'test')[1])

    def test_native_compatibility_requires_the_identical_build(self):
        prepared,plan,job=self.legacy()
        old_plan=dict(plan,recipes=[d['sql_recipe'] for d in job['payload']['destinations']])
        job['signature']=signatures.plan_signature(self.catalog,fingerprint(prepared,[]),old_plan,'native','same build')
        self.catalog['jobs']=[job]
        self.assertIs(signatures.resumable_job(self.catalog,prepared,[],plan,'native','same build')[1],job)
        self.assertIsNone(signatures.resumable_job(self.catalog,prepared,[],plan,'native','different build')[1])

    def test_metadata_repair_preserves_title_and_never_rewrites_chunks_or_manifests(self):
        prepared,plan,job=self.legacy()
        self.catalog['jobs']=[job]
        signature,_,_=signatures.resumable_job(self.catalog,prepared,[],plan,'test')
        captured=[]
        def read(service,database,sql,report,name):
            captured.append(sql)
            match=re.search(r"payload=payload\|\|'(.*)'::jsonb",sql)
            patch=json.loads(match[1].replace("''","'"))
            result=copy.deepcopy(job)
            result['signature']=signature
            result['payload'].update(patch)
            return result
        result=signatures.migrate_replay_metadata(None,'test',job,signature,prepared,{},read)
        self.assertTrue(result['payload']['codes_finalized'])
        for d in result['payload']['destinations']:
            self.assertEqual(d['version_title'],'original title')
        row=next(d for d in result['payload']['destinations'] if d['pure_table_name']=='table1')
        self.assertEqual(row['destination_columns'].count('id_column_1'),1)
        self.assertNotIn('shadow_chunks',captured[0])
        self.assertNotIn('shadow_hashes',captured[0])
        self.assertIn('pg_stat_activity',captured[0])
