#!/usr/bin/env bash
# Build and install the reviewed PostgreSQL 16/17/18 native candidate.
set -euo pipefail

project_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)
pg_config_bin=${PG_CONFIG:-pg_config}
psql_bin=psql
database=template1
host=''
port=5432
role=postgres
use_sudo=0

usage() {
  cat <<'USAGE'
Usage: bash release/native_install.sh [options]
  --pg-config PATH   PostgreSQL 16/17/18 pg_config (default: PG_CONFIG or pg_config)
  --psql PATH        Matching psql executable (default: psql)
  --database NAME    Target database (default: template1; inherited by generated databases)
  --host HOST        PostgreSQL host or socket directory (default: local Unix socket)
  --port PORT        PostgreSQL port (default: 5432)
  --username ROLE    Database administrator for CREATE EXTENSION (default: postgres)
  --sudo             Use sudo only for make install
Builds pgdm_decode 1.1 and pgdm_bulk 1.5 from this checkout.
Run on the PostgreSQL server with PGXS/compiler headers installed.
Use normal libpq password handling; no password is accepted in command arguments.
USAGE
}
while (($#)); do
  case "$1" in
    --pg-config|--psql|--database|--host|--port|--username)
      (($# >= 2)) || { usage >&2; exit 2; }
      case "$1" in
        --pg-config) pg_config_bin=$2 ;;
        --psql) psql_bin=$2 ;;
        --database) database=$2 ;;
        --host) host=$2 ;;
        --port) port=$2 ;;
        --username) role=$2 ;;
      esac
      shift 2 ;;
    --sudo) use_sudo=1; shift ;;
    --help|-h) usage; exit 0 ;;
    *) usage >&2; exit 2 ;;
  esac
done

pg_config_bin=$(command -v -- "$pg_config_bin")
psql_bin=$(command -v -- "$psql_bin")
connection=("$psql_bin" -X -p "$port" -U "$role" -d "$database" -v ON_ERROR_STOP=1)
[[ -z $host ]] || connection+=(-h "$host")
# Root can install system libraries, while the postgres OS user satisfies peer auth.
if [[ $EUID -eq 0 && -z $host && $role == postgres ]]; then
  connection=(runuser -u postgres -- "${connection[@]}")
fi
if ! is_superuser=$("${connection[@]}" -Atc "SELECT rolsuper FROM pg_roles WHERE rolname=current_user"); then
  echo 'Cannot connect to the target database. For local peer authentication, run as the postgres OS user (or root); use --host for an explicit TCP/socket endpoint.' >&2
  echo 'If template1 is being accessed by other users, close those sessions and finish installation before running the generator.' >&2
  exit 2
fi
if [[ $is_superuser != t ]]; then
  echo 'Native installation requires a PostgreSQL superuser for CREATE EXTENSION; choose that role with --username.' >&2
  exit 2
fi
if [[ $database == template1 ]]; then
  other_sessions=$("${connection[@]}" -Atc "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()")
  if [[ $other_sessions != 0 ]]; then
    echo 'template1 is being accessed by other users. Close its other sessions, then rerun installation; do not run the generator concurrently.' >&2
    exit 2
  fi
fi
case "$("$pg_config_bin" --version)" in
  "PostgreSQL 16."*|"PostgreSQL 17."*|"PostgreSQL 18."*) ;;
  *) echo 'This native candidate requires PostgreSQL 16/17/18 server headers.' >&2; exit 2 ;;
esac
[[ -f $("$pg_config_bin" --pgxs) ]] || {
  echo 'PGXS is missing; install the matching server development package.' >&2
  exit 2
}
for extension in pgdm_decode pgdm_bulk; do
  module="$project_root/native/$extension"
  [[ -f "$module/Makefile" ]] || {
    echo "Missing native source: native/$extension; use a release export with native sources included." >&2
    exit 2
  }
done
decode_build=$(sed -n 's/^#define PGDM_DECODE_BUILD_ID "\(.*\)"/\1/p' "$project_root/native/pgdm_decode/pgdm_decode.c")
bulk_build=$(bash "$project_root/native/pgdm_bulk/build_id.sh")
[[ -n $decode_build && -n $bulk_build ]] || { echo 'Missing build identifiers.' >&2; exit 2; }

for extension in pgdm_decode pgdm_bulk; do
  module="$project_root/native/$extension"
  make -C "$module" "PG_CONFIG=$pg_config_bin" with_llvm=no clean
  make -C "$module" -j2 "PG_CONFIG=$pg_config_bin" with_llvm=no all
  if ((use_sudo)); then
    sudo -- make -C "$module" "PG_CONFIG=$pg_config_bin" with_llvm=no install
  else
    make -C "$module" "PG_CONFIG=$pg_config_bin" with_llvm=no install
  fi
done

"${connection[@]}" -v expected_decode="$decode_build" -v expected_bulk="$bulk_build" <<'SQL'
SELECT current_setting('server_version_num')::integer BETWEEN 160000 AND 189999 AS supported \gset
\if :supported
\else
  \echo 'The target server must be PostgreSQL 16/17/18.'
  DO $unsupported$ BEGIN RAISE EXCEPTION 'The target server must be PostgreSQL 16/17/18'; END $unsupported$;
\endif
CREATE EXTENSION IF NOT EXISTS pgdm_decode WITH SCHEMA public VERSION '1.1';
CREATE EXTENSION IF NOT EXISTS pgdm_bulk WITH SCHEMA public VERSION '1.5';
DO $upgrade$
DECLARE extension_name text; installed_version text;
BEGIN
  FOREACH extension_name IN ARRAY ARRAY['pgdm_decode', 'pgdm_bulk'] LOOP
    SELECT extversion INTO installed_version FROM pg_extension WHERE extname=extension_name;
    IF installed_version='1.0' THEN
      EXECUTE format('ALTER EXTENSION %I UPDATE TO %L', extension_name, '1.1');
    ELSIF installed_version IS DISTINCT FROM '1.1' AND NOT (extension_name='pgdm_bulk' AND installed_version IN ('1.2','1.3','1.4','1.5')) THEN
      RAISE EXCEPTION 'Unsupported extension version for %: %', extension_name, installed_version;
    END IF;
  END LOOP;
END $upgrade$;
ALTER EXTENSION pgdm_bulk UPDATE TO '1.5';
SELECT public.pgdm_decode_build_id()=:'expected_decode'
   AND public.pgdm_bulk_build_id()=:'expected_bulk' AS correct_build \gset
\if :correct_build
  SELECT public.pgdm_decode_build_id() AS decoder_build,
         public.pgdm_bulk_build_id() AS bulk_build;
\else
  \echo 'Installed native library build does not match this checkout.'
  DO $mismatch$ BEGIN RAISE EXCEPTION 'Installed native library build does not match this checkout'; END $mismatch$;
\endif
SQL
