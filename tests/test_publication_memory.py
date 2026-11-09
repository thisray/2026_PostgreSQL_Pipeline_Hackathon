"""Offline replay of recorded publication admission inputs; no database needed."""
import json
import os
import subprocess
import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch

from services.publication_memory import admission_budget, reclaimable_memory, MEMORY_PROBE

MIB = 1024**2


class PublicationMemoryTests(unittest.TestCase):
    def setUp(self):
        self.environment = patch.dict(os.environ, {}, clear=True)
        self.environment.start()
        self.addCleanup(self.environment.stop)

    def test_recorded_g2_rc3_rc4_rc5_snapshots(self):
        rows = json.loads((Path(__file__).parent / 'fixtures/publication_memory_receipts.json').read_text())
        self.assertEqual(len(rows), 20)
        for row in rows:
            with self.subTest(receipt=row['receipt'], condition=row['condition']):
                tasks = 4 if 'g2-x86' in row['receipt'] else 8
                budget = admission_budget(row['observed'], 128, planned_tasks=tasks)
                self.assertGreaterEqual(budget['shared_total_mb'], tasks * 64)
                self.assertLessEqual(budget['shared_total_mb'], 8192)
                if row['condition'] == 'sql-10m':
                    self.assertEqual(row['previous_pool_mb'], 1)
                    self.assertEqual(budget['shared_total_mb'], 6367)

    def test_active_clean_file_cache_and_host_bound(self):
        group = dict(version=2, limit_bytes=30720*MIB, current_bytes=30000*MIB,
                     file_bytes=23000*MIB, active_file_bytes=21000*MIB,
                     inactive_file_bytes=2000*MIB, shmem_bytes=200*MIB,
                     dirty_bytes=300*MIB, writeback_bytes=100*MIB)
        observed = dict(verified_server=True, effective_limit_bytes=30720*MIB,
                        host_available_bytes=31000*MIB, cgroups=[group])
        available, details = reclaimable_memory(observed)
        self.assertEqual(available, 23120*MIB)
        self.assertEqual(details[0]['clean_reclaimable_file_bytes'], 22400*MIB)
        self.assertEqual(admission_budget(observed, 128, planned_tasks=4)['shared_total_mb'], 8192)
        observed['host_available_bytes'] = 3000*MIB
        self.assertEqual(reclaimable_memory(observed)[0], 3000*MIB)
        self.assertEqual(admission_budget(observed)['shared_total_mb'], 2040)

    def test_all_ancestors_dirty_cache_v1_and_missing_file(self):
        child = dict(version=1, limit_bytes=24000*MIB, current_bytes=23000*MIB,
                     active_file_bytes=15000*MIB, inactive_file_bytes=1000*MIB,
                     shmem_bytes=500*MIB, dirty_bytes=2000*MIB, writeback_bytes=100*MIB)
        parent = dict(version=2, limit_bytes=30000*MIB, current_bytes=29000*MIB,
                      file_bytes=4000*MIB, dirty_bytes=4500*MIB)
        observed = dict(host_available_bytes=32000*MIB, cgroups=[child,parent])
        available, details = reclaimable_memory(observed)
        self.assertEqual(details[0]['clean_reclaimable_file_bytes'], 13400*MIB)
        self.assertEqual(details[1]['clean_reclaimable_file_bytes'], 0)
        self.assertEqual(available, 1000*MIB)
        self.assertIn('total_cache', MEMORY_PROBE)
        self.assertIn('total_dirty', MEMORY_PROBE)

    def test_shell_probe_normalizes_both_cgroup_versions(self):
        for version in (1, 2):
            with self.subTest(version=version), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                proc = root / 'proc'
                backend = proc / '42'
                backend.mkdir(parents=True)
                (proc / 'meminfo').write_text('MemTotal: 32768000 kB\nMemAvailable: 32000000 kB\n')
                (backend / 'cmdline').write_text('postgres: test backend')
                (backend / 'status').write_text('Cpus_allowed_list:\t0-3\n')
                cgroup = root / 'cgroup'
                memory = cgroup if version == 2 else cgroup / 'memory'
                memory.mkdir(parents=True)
                (backend / 'cgroup').write_text('0::/\n' if version == 2 else '5:memory:/\n')
                if version == 2:
                    (cgroup / 'cgroup.controllers').write_text('memory cpu')
                    (memory / 'memory.max').write_text(str(30720*MIB))
                    (memory / 'memory.current').write_text(str(30000*MIB))
                    (memory / 'memory.stat').write_text('file 23000000000\nactive_file 21000000000\ninactive_file 2000000000\nshmem 100000000\nfile_dirty 200000000\nfile_writeback 300000000\n')
                    (memory / 'cpu.max').write_text('400000 100000')
                else:
                    (memory / 'memory.limit_in_bytes').write_text(str(30720*MIB))
                    (memory / 'memory.usage_in_bytes').write_text(str(30000*MIB))
                    (memory / 'memory.stat').write_text('total_cache 23000000000\ntotal_active_file 21000000000\ntotal_inactive_file 2000000000\ntotal_shmem 100000000\ntotal_dirty 200000000\ntotal_writeback 300000000\n')
                script = MEMORY_PROBE.replace('/proc/', str(proc)+'/').replace('/sys/fs/cgroup', str(cgroup))
                process = subprocess.run(['sh'], input=script, text=True, capture_output=True,
                                         env={'PGDM_BACKEND_PID':'42'}, check=True)
                observed = json.loads(process.stdout)
                self.assertTrue(observed['verified_server'])
                self.assertEqual(observed['effective_cpus'], 4)
                self.assertEqual(observed['cgroups'][0]['version'], version)
                self.assertEqual(observed['cgroups'][0]['shmem_bytes'], 100000000)
                self.assertEqual(observed['cgroups'][0]['writeback_bytes'], 300000000)
                self.assertEqual(admission_budget(observed)['shared_total_mb'], 8192)

    def test_default_floor_tiny_limit_and_explicit_cap(self):
        observed = dict(verified_server=True, effective_limit_bytes=30720*MIB,
                        host_available_bytes=32000*MIB, reclaimable_available_bytes=0)
        budget = admission_budget(observed, planned_tasks=8)
        self.assertEqual(budget['shared_total_mb'], 512)
        self.assertTrue(budget['floor_applied'])
        self.assertEqual(admission_budget(observed, cap=64, planned_tasks=8)['shared_total_mb'], 64)
        observed['effective_limit_bytes'] = 512*MIB
        budget = admission_budget(observed, planned_tasks=8)
        self.assertEqual(budget['shared_total_mb'], 128)
        self.assertTrue(budget['tiny_limit_floor_reduced'])
        self.assertEqual(admission_budget({'verified_server': False}, planned_tasks=8)['shared_total_mb'], 512)
        os.environ['PDM_S3_DAG_MEMORY_MB'] = '32'
        self.assertEqual(admission_budget(observed)['shared_total_mb'], 32)

    def test_unverified_namespace_defaults_and_reason(self):
        for tasks in (1, 4, 8, 16, 32):
            budget = admission_budget({'verified_server': False}, planned_tasks=tasks)
            self.assertEqual(budget['shared_total_mb'], max(256, tasks * 64))
            self.assertLessEqual(budget['shared_total_mb'], 8192)
            self.assertIn('PID namespace', budget['reason'])
            self.assertEqual(admission_budget({'verified_server': False},
                planned_tasks=tasks, cap=64)['shared_total_mb'], 64)


if __name__ == '__main__':
    unittest.main()
