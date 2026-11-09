"""Keep low-RAM proof admission independent from index sort admission."""
import json
from pathlib import Path
import unittest

from services import shared_fk_proof as proof
from services.s3_publication_dag import build_dag


class ProofLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.fixture = json.loads((Path(__file__).parent / 'fixtures/rc11_publication.json').read_text())
        self.fixture['job']['_shared_fk_required'] = True

    def tasks(self, pool):
        f = self.fixture
        return build_dag(f['job'], f['snapshot'], 8, pool, pool, pool)

    def test_low_pool_keeps_core_indexes_and_adds_one_complete_scan(self):
        tasks = {t['name']: t for t in self.tasks(2048)}
        self.assertNotIn('index:table2:shared', tasks)
        self.assertIn('index:table2_id_column_2_integer_column_1_key', tasks)
        pk = tasks['index:table2_pkey']
        self.assertEqual(pk['proof_extra_scans'], 1)
        self.assertEqual(pk['sql'].count('pgdm_fk_proof_collect'), 1)
        self.assertEqual(sum(t['sql'].count('pgdm_fk_proof_collect') for t in tasks.values()), 1)
        self.assertIn('CREATE UNIQUE INDEX', pk['sql'])

    def test_high_pool_uses_existing_bundles_without_extra_scan(self):
        tasks = {t['name']: t for t in self.tasks(8192)}
        self.assertEqual(tasks['index:table2:shared']['memory_mb'], 2240)
        self.assertFalse(any('pgdm_fk_proof_collect' in t['sql'] for t in tasks.values()))
        self.assertIn('pgdm_build_verified_radix_bundle', tasks['index:table2:shared']['sql'])

    def test_small_catalog_is_admitted_by_capability(self):
        catalog = self.fixture['job']['payload']['catalog']
        catalog['scope_rows'] = 100000
        catalog['server_version_num'] = 180006
        self.assertTrue(proof.supported(catalog, True))
        self.assertFalse(proof.supported(catalog, False))

    def test_every_batched_validate_transaction_enables_proof(self):
        job = self.fixture['job']
        tasks = {'validate:x': dict(kind='validate', sql='BEGIN; SELECT 1; COMMIT; BEGIN; SELECT 2; COMMIT;',
                                   deps=set(), priority_predecessors=set(), reservations=[], window_policy=None)}
        proof.rewrite_dag(job, tasks, {}, {}, 2048)
        self.assertEqual(tasks['validate:x']['sql'].count('pgdm_fk_proof_enable'), 2)


if __name__ == '__main__':
    unittest.main()
