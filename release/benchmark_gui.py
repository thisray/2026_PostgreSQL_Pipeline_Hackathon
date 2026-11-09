"""Run the real Windows GUI with password auth and an explicit candidate mode.

Connection JSON fields: host, ssh_port, ssh_username, ssh_password,
postgres_port (optional, defaults to 5432), sql_username, sql_password.
Store this file outside Git with permissions restricted to the current user.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import traceback

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--app', type=Path, required=True)
parser.add_argument('--output', type=Path, required=True)
parser.add_argument('--connection', type=Path, required=True)
parser.add_argument('--mode', choices=('default', 'native', 'native_bulk'), default='default',
                    help='Explicit native candidate; default retains the application default engine')
parser.add_argument('--publication-memory-mb', type=int,
                    help='Only lower the automatic publication memory pool, 32..8192 MiB; cannot raise it')
parser.add_argument('--workers', type=int,
                    help='Optional explicit worker count, 1..32')
args = parser.parse_args()
if args.publication_memory_mb is not None and not 32 <= args.publication_memory_mb <= 8192:
    parser.error('--publication-memory-mb must be between 32 and 8192')
if args.workers is not None and not 1 <= args.workers <= 32:
    parser.error('--workers must be between 1 and 32')
if sys.platform != 'win32':
    raise RuntimeError('Run on the Windows interactive desktop')
configuration_variables = {
    'PDM_CONTROL_DB', 'PDM_CONTROL_SCHEMA', 'PDM_LEGACY_CONTROL_SCHEMA',
    'PDM_REMOTE_STORAGE_DIR', 'PDM_LOCAL_STORAGE_DIR', 'PDM_APP_TITLE',
}
for name in tuple(os.environ):
    if name.startswith('PDM_') and name not in configuration_variables:
        del os.environ[name]
if args.mode != 'default':
    os.environ['PDM_EXPANSION_MODE'] = args.mode
if args.workers is not None:
    os.environ['PDM_STAGE_WORKERS'] = str(args.workers)
if args.publication_memory_mb is not None:
    for name in ('PDM_S2_INDEX_MEMORY_MB', 'PDM_S3_FK_MEMORY_MB', 'PDM_S3_DAG_MEMORY_MB'):
        os.environ[name] = str(args.publication_memory_mb)
root = args.app.resolve()
output = args.output.resolve()
output.mkdir(parents=True, exist_ok=False)
connection = json.loads(args.connection.read_text(encoding='utf-8-sig'))
os.chdir(root)
sys.path.insert(0, str(root))
from main import App
from ui.dialogs import ConnectionDialog, SqlExpansionDialog, ExpansionTimingReportDialog


class Benchmark(App):
    def __init__(self):
        self.started = None
        super().__init__()
        self.after(600, self.connect_vm)
        self.after(1200, self.wait_connected)
        self.after(7200000, lambda: self.show_error('Timeout', 'Two hours elapsed'))

    def ask_connection_info(self):
        dialog = ConnectionDialog(self)
        def fill():
            for field in ('host', 'ssh_port', 'ssh_username', 'ssh_password', 'sql_username', 'sql_password'):
                getattr(dialog, field + '_var').set(str(connection[field]))
            dialog.postgres_port_var.set(str(connection.get('postgres_port', 5432)))
            dialog.remember_var.set(False)
            dialog._confirm()
        dialog.after(500, fill)
        self.wait_window(dialog)
        return dialog.result

    def wait_connected(self):
        if not self.can_create_db or str(self.connect_button.cget('state')) != 'normal':
            self.after(500, self.wait_connected)
            return
        self.current_db = 'kaggle_challenge'
        self.selected_table_name = 'public.raw_data'
        self.started = time.perf_counter()
        self.execute_cross_table_expansion_flow()

    def ask_cross_table_expansion_info(self, database_name, source_table_name, available_tables, available_raw_schemas):
        dialog = SqlExpansionDialog(self, database_name, source_table_name, available_tables, available_raw_schemas)
        def fill():
            dialog.author_entry.insert(0, 'Judge')
            dialog.version_title_entry.insert(0, output.name)
            recipes = sorted((root / 'SQL files').glob('*.sql'))
            for _ in recipes[1:]:
                dialog._add_destination_tab()
            for widgets, recipe in zip(dialog._destination_tabs.values(), recipes):
                widgets['table_combo'].set('public.' + recipe.stem.split('_', 1)[1])
                widgets['sql_editor'].set_sql(recipe.read_text(encoding='utf-8'))
            dialog._confirm()
        dialog.after(500, fill)
        self.wait_window(dialog)
        return dialog.result

    def open_expansion_timing_report(self, report):
        super().open_expansion_timing_report(report)
        manifest = root / 'release/manifest.json'
        sha = json.loads(manifest.read_text())['source_commit'] if manifest.exists() else subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
        result = {'status': 'complete', 'source_sha': sha, 'gui_callbacks': True,
                  'password_auth': True, 'mode': args.mode,
                  'publication_memory_mb': args.publication_memory_mb,
                  'workers': args.workers,
                  'pdm_environment': sorted(name for name in os.environ if name.startswith('PDM_')),
                  'measured_processing_total': ExpansionTimingReportDialog.measured_total_seconds(report),
                  'expansion_wall_seconds': time.perf_counter() - self.started, 'report': report}
        (output / 'result.json').write_text(json.dumps(result, indent=2), encoding='utf-8')
        (output / 'report.txt').write_text(ExpansionTimingReportDialog.build_report_text(report), encoding='utf-8')
        self.after(1800, self.capture)

    def capture(self):
        from PIL import ImageGrab
        dialog = self.expansion_timing_report_dialog
        dialog.lift()
        dialog.attributes('-topmost', True)
        dialog.update()
        ImageGrab.grab(window=dialog.winfo_id()).save(output / 'gui-report.png')
        self.on_close()

    def show_info(self, title, message):
        with (output / 'events.log').open('a', encoding='utf-8') as stream:
            stream.write(f'{title}: {message}\n')

    def show_warning(self, title, message):
        self.show_info(title, message)

    def show_error(self, title, message):
        (output / 'error.json').write_text(json.dumps({'title': title, 'message': str(message)}, indent=2), encoding='utf-8')
        self.after(500, self.on_close)

    def report_callback_exception(self, kind, value, tb):
        self.show_error('Callback error', ''.join(traceback.format_exception(kind, value, tb)))


try:
    Benchmark().mainloop()
except Exception:
    (output / 'fatal.log').write_text(traceback.format_exc(), encoding='utf-8')
    raise
