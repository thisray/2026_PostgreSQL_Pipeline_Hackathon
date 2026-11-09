"""Internal planner for the unchanged official six-table workload."""
import os
from pathlib import Path
import re
import uuid

from utils import sql_literal


def timed_sql(sequence, name, sql, stage='execute'):
    return f"""
INSERT INTO pg_temp.pgdm_expansion_timings VALUES
({sequence}, {sql_literal(stage)}, {sql_literal(name)}, clock_timestamp(), NULL, NULL, NULL, NULL);
{sql}
UPDATE pg_temp.pgdm_expansion_timings SET finished_at=clock_timestamp()
WHERE sequence_number={sequence};
"""


def normalize(sql):
    """Compare SQL tokens, preserving every byte inside literals and identifiers."""
    text = sql.lstrip('\ufeff')
    tokens = []
    index = 0
    while index < len(text):
        if text[index].isspace():
            index += 1
            continue
        if text.startswith('--', index):
            end = text.find('\n', index + 2)
            index = len(text) if end < 0 else end + 1
            continue
        if text.startswith('/*', index):
            depth = 1
            index += 2
            while index < len(text) and depth:
                if text.startswith('/*', index):
                    depth += 1
                    index += 2
                elif text.startswith('*/', index):
                    depth -= 1
                    index += 2
                else:
                    index += 1
            if depth:
                raise ValueError('Unterminated SQL comment')
            continue
        start = index
        char = text[index]
        tag = re.match(r'\$(?:[A-Za-z_][A-Za-z_0-9]*)?\$', text[index:])
        if tag:
            end = text.find(tag[0], index + len(tag[0]))
            if end < 0:
                raise ValueError('Unterminated dollar literal')
            index = end + len(tag[0])
        elif char in "'\"":
            index += 1
            while index < len(text):
                if text[index] == char:
                    index += 1
                    if index < len(text) and text[index] == char:
                        index += 1
                        continue
                    break
                # Treat escape strings conservatively; never normalize their contents.
                if char == "'" and text[index] == '\\':
                    index += 1
                index += 1
            else:
                raise ValueError('Unterminated quoted SQL token')
        elif char.isalpha() or char == '_':
            index += 1
            while index < len(text) and (text[index].isalnum() or text[index] in '_$'):
                index += 1
            tokens.append(('word', text[start:index].lower()))
            continue
        elif char.isdigit():
            match = re.match(r'\d+(?:\.\d*)?(?:[eE][+-]?\d+)?', text[index:])
            index += len(match[0])
        elif char in '+-*/<>=~!@#%^&|`?:':
            index += 1
            while index < len(text) and text[index] in '+-*/<>=~!@#%^&|`?:' and not text.startswith(('--','/*'), index):
                index += 1
        else:
            index += 1
        tokens.append(('exact', text[start:index]))
    if tokens and tokens[-1] == ('exact', ';'):
        tokens.pop()
    return tuple(tokens)


def build_plan(prepared, requested_destinations, mode=None):
    mode = mode or os.getenv('PDM_EXPANSION_MODE', 'classic')
    if mode not in ('compact', 'parallel', 'parallel8'):
        return None
    recipes = sorted((Path(__file__).parents[1] / 'SQL files').glob('*.sql'))
    if (prepared['source_schema_name'], prepared['source_table_name']) != ('public', 'raw_data'):
        return None
    if len(requested_destinations) != 6:
        return None
    for item, recipe in zip(requested_destinations, recipes):
        if item['table_name'] != 'public.' + recipe.stem.split('_', 1)[1]:
            return None
        if normalize(item['sql']) != normalize(recipe.read_text(encoding='utf-8')):
            return None

    # Replay rewriting uses the reviewed spelling after exact semantic matching.
    # Keep the submitted recipe available for provenance.
    from services.postgres_service import PostgresAdminService
    canonical = [dict(item, sql=recipe.read_text(encoding='utf-8'))
                 for item, recipe in zip(requested_destinations, recipes)]
    if any(item['sql'] != reviewed['sql'] for item, reviewed in zip(requested_destinations, canonical)):
        canonical_prepared = PostgresAdminService._prepare_cross_table_expansion(
            'public.raw_data', prepared['raw_schemas'], canonical)
        for destination, reviewed in zip(prepared['destinations'], canonical_prepared['destinations']):
            submitted = destination['sql_recipe']
            destination.update(reviewed)
            destination['submitted_sql_recipe'] = submitted

    base = ['id_column_1', 'text_column_1', 'datetime_column_1', 'integer_column_1',
            'text_column_2', 'numeric_column_1', 'text_column_3', 'numeric_column_2']
    fields = base + [field for k in range(1, 15) for field in (
        f'integer_column_{k+1}', f'text_column_{k+3}',
        f'numeric_column_{2*k+1}', f'numeric_column_{2*k+2}')]
    definitions = ','.join(f'{field} text' for field in fields)
    values = ',\n'.join(
        f'({k}::smallint,j.integer_column_{k+1},j.text_column_{k+3},'
        f'j.numeric_column_{2*k+1},j.numeric_column_{2*k+2})'
        for k in range(1, 15))
    declared = ','.join(f"NULLIF(BTRIM(j.integer_column_{k+1}),'')::smallint" for k in range(1, 15))
    scope = ','.join(sql_literal(value) for value in prepared['raw_schemas'])
    stage = f"""
CREATE TEMP TABLE pgdm_compact_stage ON COMMIT DROP AS
SELECT r.raw_id, r.raw_schema, r.raw_hash,
 NULLIF(BTRIM(j.id_column_1),'')::integer AS external_id,
 NULLIF(BTRIM(j.datetime_column_1),'')::timestamptz AT TIME ZONE 'America/Sao_Paulo' AS observed_at,
 GREATEST({declared}) AS max_declared,
 CASE WHEN NULLIF(BTRIM(j.numeric_column_2),'')::numeric > 0
 THEN ROUND(NULLIF(BTRIM(j.numeric_column_2),'')::numeric*1000)::integer END AS n2,
 CASE WHEN NULLIF(BTRIM(j.numeric_column_1),'')::numeric > 0
 THEN ROUND(NULLIF(BTRIM(j.numeric_column_1),'')::numeric*1000)::integer END AS n1,
 NULLIF(BTRIM(j.integer_column_1),'')::numeric AS speed,
 BTRIM(j.text_column_1) AS ref, UPPER(BTRIM(j.text_column_2)) AS code2,
 UPPER(BTRIM(j.text_column_3)) AS code3,
 (BTRIM(j.text_column_1) <> '' AND UPPER(BTRIM(j.text_column_2)) ~ '^[A-Z]{{2}}$') AS parent_ok,
 slots.*
FROM public.raw_data r
CROSS JOIN LATERAL jsonb_to_record(r.raw->'values') AS j({definitions})
CROSS JOIN LATERAL (
 SELECT array_agg(v.pos ORDER BY v.pos) AS slot_pos,
 array_agg(ROUND(v.weight*1000)::integer ORDER BY v.pos) AS slot_weight,
 array_agg(v.type_code ORDER BY v.pos) AS slot_type,
 array_agg(CASE WHEN v.gap>0 THEN ROUND(v.gap*1000)::integer END ORDER BY v.pos) AS slot_gap
 FROM (
   SELECT p.pos, NULLIF(BTRIM(p.declared),'')::smallint AS declared,
    UPPER(BTRIM(p.type_text)) AS type_code,
    NULLIF(BTRIM(p.weight),'')::numeric AS weight,
    NULLIF(BTRIM(p.gap),'')::numeric AS gap
   FROM (VALUES {values}) AS p(pos,declared,type_text,weight,gap)
 ) v WHERE v.declared=v.pos AND v.weight>=0 AND v.type_code IN ('X','Y','Z')
) slots
WHERE r.raw_schema IN ({scope});
ANALYZE pg_temp.pgdm_compact_stage;
"""
    # Emptiness and schema ownership are checked under the destination locks.
    bulk = """
DO $empty$
DECLARE t text; occupied boolean;
BEGIN
 FOR t IN SELECT unnest(ARRAY['table1','table2','table3','table4','table5','table6']) LOOP
  EXECUTE format('SELECT EXISTS(SELECT FROM public.%I LIMIT 1)',t) INTO occupied;
  IF occupied THEN RAISE EXCEPTION 'Compact fastpath requires empty destinations: %',t; END IF;
 END LOOP;
 IF EXISTS(SELECT FROM pg_constraint WHERE contype='f'
   AND confrelid IN ('public.table1'::regclass,'public.table2'::regclass,'public.table3'::regclass,
                    'public.table4'::regclass,'public.table5'::regclass,'public.table6'::regclass)
   AND conrelid NOT IN ('public.table1'::regclass,'public.table2'::regclass,'public.table3'::regclass,
                       'public.table4'::regclass,'public.table5'::regclass,'public.table6'::regclass))
 THEN RAISE EXCEPTION 'Compact fastpath requires isolated destination dependencies'; END IF;
END $empty$;
CREATE TEMP TABLE pgdm_saved_constraints ON COMMIT DROP AS
SELECT conrelid::regclass::text AS relation, conname, contype, pg_get_constraintdef(oid) AS definition
FROM pg_constraint WHERE conrelid IN
('public.table1'::regclass,'public.table2'::regclass,'public.table3'::regclass,
 'public.table4'::regclass,'public.table5'::regclass,'public.table6'::regclass)
AND contype IN ('f','p','u');
CREATE TEMP TABLE pgdm_saved_indexes ON COMMIT DROP AS
SELECT indexrelid::regclass::text AS index_name,pg_get_indexdef(indexrelid) AS definition
FROM pg_index i WHERE indrelid IN
('public.table1'::regclass,'public.table2'::regclass,'public.table3'::regclass,
 'public.table4'::regclass,'public.table5'::regclass,'public.table6'::regclass)
AND NOT EXISTS(SELECT FROM pg_constraint c WHERE c.conindid=i.indexrelid);
DO $drop$
DECLARE r record;
BEGIN
 FOR r IN SELECT * FROM pgdm_saved_constraints ORDER BY CASE WHEN contype='f' THEN 0 ELSE 1 END LOOP
  EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I',r.relation,r.conname);
 END LOOP;
 FOR r IN SELECT * FROM pgdm_saved_indexes LOOP EXECUTE 'DROP INDEX '||r.index_name; END LOOP;
END $drop$;
"""
    inserts = {
        'table3': """INSERT INTO public.table3(text_column_1,text_column_2,numeric_column_1,text_column_3,text_column_4)
SELECT DISTINCT ref,'SYNTHETIC/REFERENCE',NULL::numeric,LEFT(code2,1),RIGHT(code2,1)
FROM pg_temp.pgdm_compact_stage WHERE parent_ok""",
        'table4': """INSERT INTO public.table4(id_column_1,text_column_1,text_column_2)
SELECT DISTINCT ((ASCII(LEFT(code3,1))-65)*26+ASCII(RIGHT(code3,1))-65+1)::smallint,
'SYNTHETIC/GENERIC',code3 FROM pg_temp.pgdm_compact_stage WHERE code3 ~ '^[A-Z]{2}$'""",
        'table1': """INSERT INTO public.table1(id_column_1,id_column_2,text_column_1,datetime_column_1,text_column_2,
integer_column_1,integer_column_2,integer_column_3,text_column_3,id_column_3,id_column_4,id_column_5,
integer_column_4,integer_column_5,id_column_6,integer_column_6,integer_column_7)
SELECT s.raw_id,s.external_id,NULL::varchar,s.observed_at,NULL::varchar,s.max_declared,NULL::smallint,s.n2,
NULL::varchar,s.raw_id,d.id_column_1,NULL::bigint,NULL::integer,NULL::integer,NULL::integer,NULL::integer,s.n1
FROM pg_temp.pgdm_compact_stage s JOIN public.table3 d ON d.text_column_1=s.ref
AND d.text_column_2='SYNTHETIC/REFERENCE' AND d.numeric_column_1 IS NULL
AND d.text_column_3=LEFT(s.code2,1) AND d.text_column_4=RIGHT(s.code2,1)""",
        'table5': """INSERT INTO public.table5(numeric_column_1,numeric_column_2,integer_column_1,integer_column_2,id_column_2,id_column_3)
SELECT ROUND(speed,1),NULL::numeric,NULL::integer,NULL::integer,raw_id,NULL::bigint
FROM pg_temp.pgdm_compact_stage WHERE parent_ok AND speed>0""",
        'table6': """INSERT INTO public.table6(id_column_2,id_column_3)
SELECT s.raw_id,d.id_column_1 FROM pg_temp.pgdm_compact_stage s
JOIN public.table4 d ON d.text_column_1='SYNTHETIC/GENERIC' AND d.text_column_2=s.code3
WHERE s.parent_ok""",
        'table2': """INSERT INTO public.table2(id_column_2,integer_column_1,integer_column_2,text_column_1,
integer_column_3,integer_column_4,integer_column_5,integer_column_6)
SELECT s.raw_id,p.pos,p.weight,p.type_code,NULL::integer,NULL::integer,NULL::integer,p.gap
FROM pg_temp.pgdm_compact_stage s
CROSS JOIN LATERAL unnest(s.slot_pos,s.slot_weight,s.slot_type,s.slot_gap) p(pos,weight,type_code,gap)
WHERE s.parent_ok""",
    }
    finalize = """
SELECT setval(pg_get_serial_sequence('public.table1','id_column_1'),
COALESCE((SELECT MAX(id_column_1) FROM public.table1),0)+1,false);
DO $rebuild$
DECLARE r record;
BEGIN
 FOR r IN SELECT * FROM pgdm_saved_constraints ORDER BY CASE WHEN contype='f' THEN 1 ELSE 0 END LOOP
  EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %I %s',r.relation,r.conname,r.definition);
 END LOOP;
 FOR r IN SELECT * FROM pgdm_saved_indexes LOOP EXECUTE r.definition; END LOOP;
END $rebuild$;
DROP TABLE pg_temp.pgdm_compact_stage,pg_temp.pgdm_saved_constraints,pg_temp.pgdm_saved_indexes;
"""
    for destination in prepared['destinations']:
        if destination['pure_table_name'] == 'table1':
            original = destination.get('original_sql_recipe', destination['sql_recipe'])
            destination['original_sql_recipe'] = original
            replay = original.replace('INSERT INTO public.table1 (', 'INSERT INTO public.table1 (id_column_1,', 1)
            replay = replay.replace('\nSELECT\n', '\nSELECT\n    source_row.raw_id,\n', 1)
            if replay == original:
                raise RuntimeError('Could not build independent direct-key replay recipe')
            destination['sql_recipe'] = replay + "\nSELECT setval(pg_get_serial_sequence('public.table1','id_column_1'),COALESCE((SELECT MAX(id_column_1) FROM public.table1),0)+1,false);"
            destination['destination_columns'] = ['id_column_1'] + [
                name for name in destination['destination_columns'] if name != 'id_column_1']
    plan = {'prepare': timed_sql(4, 'Validate empty destinations and suspend bulk indexes', bulk)
            + timed_sql(5, 'Build compact typed projection and sparse slots', stage),
            'inserts': inserts, 'finalize': timed_sql(500, 'Rebuild and validate all constraints and indexes; synchronize sequence; clean stage', finalize)}
    plan['canonical_inserts'] = dict(inserts)
    if mode in ('parallel', 'parallel8'):
        stage_name = 'pgdm_stage_' + uuid.uuid4().hex
        relation = 'public.' + stage_name
        select_sql = stage.split(' AS\n', 1)[1].split(';\nANALYZE', 1)[0].strip()
        lean = mode == 'parallel8'
        if lean:
            select_sql = select_sql.replace('r.raw_id, r.raw_schema, r.raw_hash,', 'r.raw_id,', 1)
        plan['compact_select'] = select_sql
        plan['create_sql'] = 'SET client_min_messages=warning; CREATE EXTENSION IF NOT EXISTS dblink;\n' + f'CREATE UNLOGGED TABLE {relation} AS {select_sql} WITH NO DATA;\nSELECT json_build_object(\'stage_created\',\'{relation}\');'
        insert_prefix = f'INSERT INTO {relation} ' + select_sql
        workers = int(os.getenv('PDM_STAGE_WORKERS', '8' if lean else '4'))
        if not 1 <= workers <= 8:
            raise ValueError('Stage worker count must be between 1 and 8')
        parallel_sql = f"""
LOCK TABLE public.raw_data IN SHARE MODE;
DO $parallel$
DECLARE lo bigint; hi bigint; width bigint; k integer; query text;
BEGIN
 SELECT min(raw_id),max(raw_id) INTO lo,hi FROM public.raw_data WHERE raw_schema IN ({scope});
 width := GREATEST(1,ceil((hi-lo+1)::numeric/{workers})::bigint);
 FOR k IN 0..{workers-1} LOOP
  PERFORM dblink_connect('stage_'||k,format('dbname=%L user=%L',current_database(),current_user));
  query := {sql_literal(insert_prefix)} || format(' AND r.raw_id BETWEEN %s AND %s',lo+k*width,LEAST(hi,lo+(k+1)*width-1));
  PERFORM dblink_send_query('stage_'||k,query);
 END LOOP;
 FOR k IN 0..{workers-1} LOOP
  PERFORM status FROM dblink_get_result('stage_'||k) AS response(status text);
  PERFORM dblink_disconnect('stage_'||k);
 END LOOP;
END $parallel$;
ANALYZE {relation};
SELECT json_build_object('pgdm_stage_bytes',pg_total_relation_size('{relation}'));
"""
        # Dropping a foreign key can lock its referenced source exclusively.
        # Finish and disconnect all source readers before changing constraints.
        plan['prepare'] = timed_sql(5, f'Parallel compact projection and sparse slots ({workers} workers)', parallel_sql) + timed_sql(4, 'Validate empty destinations and suspend bulk indexes', bulk)
        plan['inserts'] = {name: sql.replace('pg_temp.pgdm_compact_stage', relation) for name, sql in inserts.items()}
        plan['finalize'] = plan['finalize'].replace('pg_temp.pgdm_compact_stage', relation)
        plan['stage_relation'] = relation
        plan['isolation'] = 'READ COMMITTED'
        if lean:
            manifest_relation = relation + '_hashes'
            plan['create_sql'] += f'\nCREATE UNLOGGED TABLE {manifest_relation}(raw_schema text,raw_hash text,row_count bigint);'
            worker_tag = '$worker_' + stage_name + '$'
            worker_sql = (
                f'DO {worker_tag} BEGIN ' + insert_prefix
                + ' AND r.raw_id BETWEEN %s AND %s; '
                + f'INSERT INTO {manifest_relation} '
                + f'SELECT raw_schema,raw_hash,count(*) FROM public.raw_data WHERE raw_schema IN ({scope}) '
                + 'AND raw_id BETWEEN %s AND %s GROUP BY raw_schema,raw_hash; '
                + f'END {worker_tag};'
            )
            original_query = 'query := ' + sql_literal(insert_prefix) + " || format(' AND r.raw_id BETWEEN %s AND %s',lo+k*width,LEAST(hi,lo+(k+1)*width-1));"
            lean_query = 'query := format(' + sql_literal(worker_sql) + ',lo+k*width,LEAST(hi,lo+(k+1)*width-1),lo+k*width,LEAST(hi,lo+(k+1)*width-1));'
            if original_query not in parallel_sql:
                raise RuntimeError('Parallel worker query template did not match')
            parallel_sql = parallel_sql.replace(original_query, lean_query)
            plan['prepare'] = timed_sql(5, f'Lean compact projection and hash aggregates ({workers} workers)', parallel_sql) + timed_sql(4, 'Validate empty destinations and suspend bulk indexes', bulk)
            sequence_sql = finalize[:finalize.index('DO $rebuild$')]
            rebuild_sql = finalize[finalize.index('DO $rebuild$'):finalize.index('DROP TABLE')]
            cleanup_sql = finalize[finalize.index('DROP TABLE'):].replace('pg_temp.pgdm_compact_stage', relation)
            cleanup_sql += f'\nDROP TABLE {manifest_relation};'
            plan['finalize'] = (
                timed_sql(500, 'Build all constraints and indexes with bounded bulk memory', "SET LOCAL work_mem='512MB'; SET LOCAL maintenance_work_mem='2GB';\n" + rebuild_sql)
                + timed_sql(510, 'Synchronize parent sequence using rebuilt primary index', sequence_sql)
                + timed_sql(520, 'Remove compact projection and aggregate caches', cleanup_sql)
            )
            plan['manifest_relation'] = manifest_relation
    return plan
