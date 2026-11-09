"""Derive the required library identity from this checkout's native sources."""
import hashlib
from pathlib import Path


def bulk_build_id():
    app_root = Path(__file__).resolve().parents[1]
    root = app_root / 'native/pgdm_bulk'
    if not root.is_dir() and app_root.name == 'app':
        root = app_root.parent / 'native/pgdm_bulk'
    sources = sorted(path for path in root.glob('*') if path.suffix in ('.c', '.h', '.inc') and path.is_file())
    if not sources:
        return 'native-sources-unavailable'
    manifest = ''.join(hashlib.sha256(path.read_bytes()).hexdigest() + '  ' + path.name + '\n' for path in sources)
    prefix = next(line.split('"')[1] for line in (root/'pgdm_bulk.c').read_text().splitlines()
                  if line.startswith('#define PGDM_BULK_BUILD_PREFIX '))
    return prefix + hashlib.sha256(manifest.encode()).hexdigest()
