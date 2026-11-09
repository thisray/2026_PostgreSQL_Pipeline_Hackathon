"""Check the post-freeze attachment barrier without a PostgreSQL server."""
import copy
import json
import os
from pathlib import Path
import subprocess
import signal
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import uuid

from services.s3_publication_dag import build_dag, render_runner
from services.s3_native_expansion import publication_waves


class AttachmentBarrierTests(unittest.TestCase):
    def test_native_publication_preserves_the_optional_atomic_publish_factory(self):
        service, job, report = object(), {}, {}
        publish = lambda: 'atomic publish'
        with patch('services.s3_native_expansion.s3_publication.publication_waves',
                   return_value={'published': True}) as shared:
            result = publication_waves(service, 'test', job, 8, report, publish=publish)
            shared.assert_called_once_with(service, 'test', job, 8, report, publish=publish)
            self.assertEqual(result, {'published': True})
            shared.reset_mock()
            publication_waves(service, 'test', job, 8, report, publish=None)
            shared.assert_called_once_with(service, 'test', job, 8, report)

    def setUp(self):
        self.fixture = json.loads((Path(__file__).parent /
            'fixtures/rc11_publication.json').read_text())

    def build(self):
        f = self.fixture
        return build_dag(f['job'], f['snapshot'], 8, f['budget'], f['budget'], f['budget'])

    def test_long_validation_waits_for_all_conflicting_attachments(self):
        tasks = {t['name']: t for t in self.build()}
        raw = tasks['validate:table1:table1_id_column_3_fkey']
        self.assertTrue({'attach_fk:' + table for table in
            ('table1', 'table2', 'table5', 'table6')} <= set(raw['deps']))
        short = tasks['validate:table1:table1_id_column_4_fkey']
        self.assertNotIn('attach_fk:table6', short['deps'])
        child = tasks['validate:table6:table6_id_column_2_fkey']
        self.assertNotIn(raw['name'], child['deps'])

    def test_resume_with_attached_foreign_keys_has_no_missing_dependencies(self):
        self.fixture['snapshot']['constraints'] += [dict(k, validated=False)
            for k in self.fixture['job']['payload']['catalog']['constraints'] if k['type'] == 'f']
        tasks = {t['name']: t for t in self.build()}
        self.assertFalse(any(t['kind'] == 'attach_fk' for t in tasks.values()))
        self.assertTrue(all(set(t['deps']) <= tasks.keys() for t in tasks.values()))

    def test_runner_finishes_large_and_batched_workloads(self):
        original = copy.deepcopy(self.fixture)
        for rows in (30000000, 1000000, 100000):
            with self.subTest(rows=rows), tempfile.TemporaryDirectory() as directory:
                self.fixture = copy.deepcopy(original)
                self.fixture['job']['payload']['catalog']['scope_rows'] = rows
                tasks = self.build()
                script = render_runner(SimpleNamespace(sql_username='challenge_owner'),
                    'test', self.fixture['job'], tasks, 8, self.fixture['budget'], 8, 8)
                subprocess.run(['bash', '-n'], input=script, text=True, check=True)
                stub = Path(directory) / 'psql'
                stub.write_text('#!/bin/bash\nsleep 0.002\n')
                stub.chmod(0o755)
                script = script.replace('psql_cmd=(psql ', 'psql_cmd=(' + str(stub) + ' ', 1)
                result = subprocess.run(['bash'], input=script, text=True,
                    capture_output=True, check=True, timeout=20)
                self.assertEqual(result.stdout.count('PGDM_DAG_FINISH'), len(tasks))

    def test_resource_grants_never_overcommit_or_deadlock_capped_pools(self):
        for budget in (32, 512, 8192):
            with self.subTest(budget=budget), tempfile.TemporaryDirectory() as directory:
                tasks = build_dag(self.fixture['job'], self.fixture['snapshot'],
                                  8, budget, budget, budget)
                stub = Path(directory) / 'psql'
                stub.write_text('#!/bin/bash\nsleep 0.005\n')
                stub.chmod(0o755)
                script = render_runner(SimpleNamespace(sql_username='challenge_owner'),
                    'test', self.fixture['job'], tasks, 8, budget, 8, 8)
                script = script.replace('psql_cmd=(psql ', 'psql_cmd=(' + str(stub) + ' ', 1)
                result = subprocess.run(['bash'], input=script, text=True,
                    capture_output=True, check=True, timeout=20)
                active = {}
                finished = set()
                by_name = {task['name']: task for task in tasks}
                for line in result.stdout.splitlines():
                    fields = line.split('\t')
                    if fields[0] == 'PGDM_DAG_START':
                        self.assertTrue(set(by_name[fields[1]]['deps']) <= finished)
                        active[fields[1]] = int(fields[3])
                        self.assertLessEqual(sum(active.values()), budget)
                    elif fields[0] == 'PGDM_DAG_FINISH':
                        self.assertEqual(fields[2], '0')
                        del active[fields[1]]
                        finished.add(fields[1])
                self.assertEqual(finished, set(by_name))
                self.assertFalse(active)

    def test_persistent_slots_reuse_sessions_and_obey_the_same_dependencies(self):
        self.fixture['job']['payload']['catalog']['scope_rows'] = 1000000
        tasks = self.build()
        with tempfile.TemporaryDirectory() as directory:
            stub = Path(directory) / 'psql'
            stub.write_text("""#!/bin/bash
printf 'PGDM_DAG_SESSION\\t%s\\n' "$$"
while IFS= read -r line; do
  case "$line" in
    *'__PGDM_PARALLEL_WORKERS__'*) exit 8;;
    '\\echo '*) sleep 0.002; printf '%s\\n' "${line#'\\echo '}";;
    '\\q') exit 0;;
  esac
done
""")
            stub.chmod(0o755)
            script = render_runner(SimpleNamespace(sql_username='challenge_owner'),
                'test', self.fixture['job'], tasks, 4, self.fixture['budget'], 4, 4,
                persistent=True)
            script = script.replace('psql_cmd=(psql ', 'psql_cmd=(' + str(stub) + ' ', 1)
            subprocess.run(['bash', '-n'], input=script, text=True, check=True)
            result = subprocess.run(['bash'], input=script, text=True,
                capture_output=True, check=True, timeout=20)
            self.assertEqual(result.stdout.count('PGDM_DAG_SESSION'), 4)
            self.assertEqual(result.stdout.count('PGDM_DAG_FINISH'), len(tasks))
            finished = set()
            by_name = {task['name']: task for task in tasks}
            for line in result.stdout.splitlines():
                fields = line.split('\t')
                if fields[0] == 'PGDM_DAG_START':
                    self.assertTrue(set(by_name[fields[1]]['deps']) <= finished)
                elif fields[0] == 'PGDM_DAG_FINISH':
                    self.assertEqual(fields[2], '0')
                    finished.add(fields[1])

    def test_persistent_sql_error_stops_without_dispatching_dependents(self):
        tasks = self.build()[:1]
        tasks[0] = dict(tasks[0], sql='SELECT pgdm_test_fail;', deps=[], reservations=[])
        tasks.append(dict(tasks[0], name='must_not_run', deps=[tasks[0]['name']]))
        with tempfile.TemporaryDirectory() as directory:
            stub = Path(directory) / 'psql'
            stub.write_text("#!/bin/bash\nwhile IFS= read -r line; do\n"
                "  case \"$line\" in *pgdm_test_fail*) exit 9;; esac\ndone\n")
            stub.chmod(0o755)
            script = render_runner(SimpleNamespace(sql_username='challenge_owner'),
                'test', self.fixture['job'], tasks, 1, self.fixture['budget'], 1, 0,
                persistent=True)
            script = script.replace('psql_cmd=(psql ', 'psql_cmd=(' + str(stub) + ' ', 1)
            result = subprocess.run(['bash'], input=script, text=True,
                capture_output=True, timeout=10)
            self.assertNotEqual(result.returncode, 0)
            self.assertNotIn('PGDM_DAG_START\tmust_not_run', result.stdout)

    def test_controller_sigkill_releases_idle_slots_and_reclaims_fifo_on_resume(self):
        job = dict(self.fixture['job'], job_id=uuid.uuid4().hex)
        prototype = dict(self.build()[0], deps=[], reservations=[])
        tasks = [dict(prototype, name='short', sql='SELECT pgdm_short;'),
                 dict(prototype, name='wait', sql='SELECT pgdm_wait;')]
        with tempfile.TemporaryDirectory() as directory:
            stub = Path(directory)/'psql'
            stub.write_text("""#!/bin/bash
while IFS= read -r line; do
  case "$line" in
    'SELECT pgdm_wait;') sleep 0.3;;
    '\\echo '*) printf '%s\\n' "${line#'\\echo '}";;
    '\\q') exit 0;;
  esac
done
""")
            stub.chmod(0o755)
            def script(selected):
                return render_runner(SimpleNamespace(sql_username='challenge_owner'), 'test', job,
                    selected, 2, 256, 2, 0, persistent=True).replace(
                        'psql_cmd=(psql ', 'psql_cmd=('+str(stub)+' ', 1)
            initial = Path(directory)/'initial.sh'
            initial.write_text(script(tasks))
            output = Path(directory)/'initial.log'
            with output.open('w') as log:
                process = subprocess.Popen(['bash', initial], stdout=log,
                    stderr=subprocess.STDOUT, start_new_session=True)
                try:
                    deadline = time.monotonic()+3
                    while 'PGDM_DAG_FINISH\tshort' not in output.read_text():
                        self.assertLess(time.monotonic(), deadline)
                        time.sleep(.005)
                    process.kill()
                    process.wait(timeout=2)
                    resumed = subprocess.run(['bash'], input=script(tasks[:1]), text=True,
                        capture_output=True, check=True, timeout=3)
                    self.assertEqual(resumed.stdout.count('PGDM_DAG_FINISH'), 1)
                    self.assertFalse(Path('/tmp/pgdm-dag-'+job['job_id']+'.pool').exists())
                finally:
                    try:
                        os.killpg(process.pid, signal.SIGTERM)
                    except ProcessLookupError:
                        pass


if __name__ == '__main__':
    unittest.main()
