"""Keep batched checks on the configured control database."""
import json
import os
import shlex
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from services import s2_preflight
from utils import sql_ident


class PreflightControlDatabaseTests(unittest.TestCase):
    def capture(self, control):
        service = SimpleNamespace(
            sql_username='test owner',
            get_raw_schema_version_references=Mock(return_value='SELECT raw_versions;'),
            _validate_cross_table_expansion_functions=Mock(return_value='SELECT functions;'),
            ensure_admin_schema=Mock(return_value='SELECT admin_schema;'),
            _data_table_row_counts_ddl_sql=Mock(return_value='SELECT row_counts;'),
            _validate_managed_sql_dependencies=Mock(return_value='SELECT dependencies;'),
            _data_metadata_ready_databases=set(),
            run_remote_command=Mock(return_value='[]\n' + json.dumps([{'raw_schema': 'group001'}])),
        )
        prepared = dict(raw_schemas=['group001'], destinations=[],
                        source_schema_name='public', source_table_name='raw_data')
        with patch.object(s2_preflight, 'CONTROL_DB', control), patch.dict(
                os.environ, {'PDM_EXPANSION_MODE': 'classic'}):
            result = s2_preflight.preflight(service, 'data " db', 'public.raw_data',
                                            prepared, None)
        self.assertEqual(result, [{'raw_schema': 'group001'}])
        call = service.run_remote_command.call_args
        return call.args[0], call.kwargs['stdin_text']

    def test_custom_control_database_in_both_connections(self):
        control = 'control " db\' $value'
        command, script = self.capture(control)
        self.assertEqual(shlex.split(command)[5:7], ['-d', control])
        self.assertIn('\\connect ' + sql_ident(control) + '\nSELECT raw_versions;', script)
        self.assertIn('\\connect ' + sql_ident('data " db'), script)
        self.assertNotIn('postgres_data_manager', command + script)

    def test_default_control_database_preserved(self):
        command, script = self.capture('postgres_data_manager')
        self.assertIn('-d postgres_data_manager ', command)
        self.assertIn('\\connect "postgres_data_manager"\nSELECT raw_versions;', script)


if __name__ == '__main__':
    unittest.main()
