"""Exercise resource bounds and source identity without running benchmarks."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from services.chunk_workers import choose_workers
from services.native_build import bulk_build_id
from services.native_selection import fallback_reason
from services.native_selection import select_native
from types import SimpleNamespace
from services.s3_expansion import CPU_PROBE


def native_source():
    """Locate native/pgdm_bulk in the public repository layout or in the private monorepo layout."""
    here = Path(__file__).resolve()
    for base in here.parents[1:3]:
        if (base / 'native/pgdm_bulk').is_dir():
            return base / 'native/pgdm_bulk'
    raise FileNotFoundError('native/pgdm_bulk not found next to the tests')


class ChunkWorkerTests(unittest.TestCase):
    def choose(self, cpus, connections=90, memory_mb=30720):
        observed = dict(verified_server=True, effective_cpus=cpus, nproc=20,
                        effective_limit_bytes=memory_mb*1024**2,
                        host_available_bytes=memory_mb*1024**2, cgroups=[])
        report = {}
        chosen = choose_workers(json.dumps(observed), dict(connection_budget=connections), report)
        return chosen, report['chunk_worker_policy']

    def test_small_cpu_headroom_and_eight_cpu_boundary(self):
        with patch.dict(os.environ, {}, clear=True):
            for cpus, expected in ((1,2),(2,3),(3,5),(4,6),(6,9),(7,7),(8,8),(20,16)):
                chosen, policy = self.choose(cpus)
                self.assertEqual(chosen, expected)
                self.assertEqual(policy['publication_workers'], min(cpus,16))

    def test_limits_override_and_unverified_server(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(self.choose(4, connections=5)[0], 5)
            chosen, policy = self.choose(4, memory_mb=2048)
            self.assertEqual(chosen, policy['memory']['shared_total_mb']//256)
            self.assertEqual(policy['publication_workers'], 4)
            report = {}
            self.assertEqual(choose_workers('4', dict(connection_budget=90), report), 4)
        with patch.dict(os.environ, {'PDM_STAGE_WORKERS':'3'}, clear=True):
            self.assertEqual(self.choose(4)[0], 3)

    def test_source_hash_matches_build_and_detects_stale_include(self):
        root = native_source()
        expected = bulk_build_id()
        self.assertEqual(subprocess.check_output(['bash',str(root/'build_id.sh')],text=True).strip(), expected)
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory)/'bulk'
            shutil.copytree(root,target)
            with (target/'decoder_core.inc').open('a') as stream:
                stream.write('\n/* Changed decoder source. */\n')
            changed = subprocess.check_output(['bash',str(target/'build_id.sh')],text=True).strip()
            self.assertNotEqual(changed, expected)
            manifest = ''.join(hashlib.sha256(p.read_bytes()).hexdigest()+'  '+p.name+'\n'
                               for p in sorted(target.iterdir()) if p.suffix in ('.c','.h','.inc'))
            self.assertTrue(changed.endswith(hashlib.sha256(manifest.encode()).hexdigest()))

    def test_install_hint_only_for_missing_or_old_native(self):
        self.assertIn('sudo bash release/native_install.sh',fallback_reason('pgdm_bulk API 1.4 required; installed=missing'))
        self.assertIn('README section 3',fallback_reason('pgdm_bulk build mismatch'))
        self.assertEqual(fallback_reason('Unsupported workload'), 'Unsupported workload')

    def test_catalog_missing_extension_falls_back_before_any_job_command(self):
        service = SimpleNamespace(_native_prepared_catalog=(
            'test', '8', {'jobs': [], 'native_bulk_api': None}, None))
        report = {}
        with patch('services.native_selection.build_plan', return_value=True):
            self.assertFalse(select_native(service, 'test', {}, [], report))
        self.assertTrue(report['native_selection']['catalog_checked_before_job'])
        self.assertIn('installed=missing', report['fallback_reason'])
        self.assertFalse(service._native_probe_deferred)

    def test_psql_shell_probe_fits_one_meta_command(self):
        self.assertNotIn('\n', CPU_PROBE)
        self.assertIn('base64 -d | sh', CPU_PROBE)

    def test_export_layout_keeps_the_same_native_identity(self):
        expected = bulk_build_id()
        source = native_source()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)/'export'
            shutil.copytree(source,root/'native/pgdm_bulk')
            with patch('services.native_build.__file__',str(root/'services/native_build.py')):
                self.assertEqual(bulk_build_id(),expected)

    def test_native_selection_reverts_only_automatic_oversubscription(self):
        observed = json.dumps(dict(verified_server=True,effective_cpus=4,
            effective_limit_bytes=30720*1024**2,host_available_bytes=30720*1024**2,cgroups=[]))
        with patch.dict(os.environ,{},clear=True):
            report = {}
            self.assertEqual(choose_workers(observed,dict(connection_budget=90),report,
                                           small_cpu_oversubscription=False),4)
            self.assertFalse(report['chunk_worker_policy']['small_cpu_oversubscription_enabled'])
        with patch.dict(os.environ,{'PDM_STAGE_WORKERS':'6'},clear=True):
            self.assertEqual(choose_workers(observed,dict(connection_budget=90),{},
                                           small_cpu_oversubscription=False),6)
