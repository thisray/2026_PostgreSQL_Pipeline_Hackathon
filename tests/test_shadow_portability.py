import unittest
from pathlib import Path

from services.expansion_optimizer import build_plan, normalize
from services.postgres_service import PostgresAdminService
from services.shadow_expansion import UnsupportedShadowError, validate_catalog
from ui.dialogs import ExpansionTimingReportDialog


class ShadowPortabilityTests(unittest.TestCase):
    def test_recipe_variants_keep_replay_working_and_literals_exact(self):
        recipes = sorted((Path(__file__).parents[1] / 'SQL files').glob('*.sql'))
        requested = []
        for recipe in recipes:
            sql = recipe.read_text(encoding='utf-8')
            variant = '\ufeff-- leading comment {{source}}\r\n' + sql.replace('INSERT INTO', 'insert /* nested /* {{source}} */ comment */ into').replace('{{source}}','{{SOURCE}}').replace('source_row','SOURCE_ROW').replace('\n', '\r\n') + ' -- tail'
            self.assertEqual(normalize(sql), normalize(variant))
            requested.append({'table_name': 'public.' + recipe.stem.split('_', 1)[1],
                              'version_title': 'Portability', 'sql': variant})
        prepared = PostgresAdminService._prepare_cross_table_expansion('public.raw_data', ['group001'], requested)
        self.assertIsNotNone(build_plan(prepared, requested, 'compact'))
        from services.shadow_expansion import replay_children, replay_dimensions
        replay_children(prepared)
        replay_dimensions(prepared)
        self.assertNotEqual(normalize("SELECT 'Ab--/*x*/'"), normalize("SELECT 'ab--/*x*/'"))
        self.assertNotEqual(normalize('SELECT "Ab"'), normalize('SELECT "ab"'))
        self.assertNotEqual(normalize('SELECT a b'), normalize('SELECT ab'))
        self.assertNotEqual(normalize('SELECT a >= b'), normalize('SELECT a > = b'))

    def test_not_null_supported_and_unknown_type_rejected(self):
        validate_catalog({'constraints': [{'type': 'n', 'name': 'id_not_null', 'table': 'table2'}]})
        with self.assertRaisesRegex(UnsupportedShadowError, 'Unsupported constraint type'):
            validate_catalog({'constraints': [{'type': 'x', 'name': 'extra', 'table': 'table2'}]})

    def test_engine_information_is_visible_and_excluded(self):
        report = {'items': [], 'fallback_reason': 'Unsupported constraint type: x'}
        PostgresAdminService._record_expansion_engine(report, 'classic')
        text = ExpansionTimingReportDialog.build_report_text(report)
        self.assertIn('Engine:        classic', text)
        self.assertIn('Unsupported constraint type: x', text)
        self.assertEqual(ExpansionTimingReportDialog.measured_total_seconds(report), 0)
        self.assertFalse(report['items'][0]['include_in_total'])


if __name__ == '__main__':
    unittest.main()
