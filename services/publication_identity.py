"""Compare stable publication identities and report every changed JSON leaf."""
import json

from utils import sql_literal


def expected_identity(job):
    catalog = job['payload']['catalog']
    return {
        'source': job['source_identity'],
        'fence': True,
        'target_oids': job['target_identity'],
        'target_shapes': {t['name']: {k: v for k, v in t.items() if k != 'oid'}
                          for t in catalog['targets']},
        'constraints': {k['table'] + '.' + k['name']: k for k in catalog['constraints']},
        'indexes': {i['table'] + '.' + i['name']: i for i in catalog.get('indexes') or []},
        'external': catalog['external'],
    }


def locked_check(job, catalog_query):
    expected = sql_literal(json.dumps(expected_identity(job)))
    return f"""DO $identity$
DECLARE observed_catalog jsonb; expected_identity jsonb := {expected}::jsonb;
 observed_identity jsonb; differences jsonb; previous_path text := current_setting('search_path');
BEGIN
 -- Match CATALOG's deparser visibility without changing the caller's later SQL.
 PERFORM set_config('search_path','pg_catalog',true);
 observed_catalog:=({catalog_query});
 PERFORM set_config('search_path',previous_path,true);
 observed_identity:=jsonb_build_object(
  'source',observed_catalog->'source','fence',observed_catalog->'fenced',
  'target_oids',(SELECT jsonb_object_agg(t->>'name',t->'oid') FROM jsonb_array_elements(observed_catalog->'targets') t),
  'target_shapes',(SELECT jsonb_object_agg(t->>'name',t-'oid') FROM jsonb_array_elements(observed_catalog->'targets') t),
  'constraints',COALESCE((SELECT jsonb_object_agg(k->>'table'||'.'||(k->>'name'),k) FROM jsonb_array_elements(NULLIF(observed_catalog->'constraints','null'::jsonb)) k),'{{}}'::jsonb),
  'indexes',COALESCE((SELECT jsonb_object_agg(i->>'table'||'.'||(i->>'name'),i) FROM jsonb_array_elements(NULLIF(observed_catalog->'indexes','null'::jsonb)) i),'{{}}'::jsonb),
  'external',observed_catalog->'external');
 IF observed_identity IS DISTINCT FROM expected_identity THEN
  WITH RECURSIVE delta(path,expected,observed) AS (
   SELECT ARRAY[]::text[],expected_identity,observed_identity
   UNION ALL
   SELECT d.path||keys.key,d.expected->keys.key,d.observed->keys.key
   FROM delta d CROSS JOIN LATERAL (
    SELECT jsonb_object_keys(CASE WHEN jsonb_typeof(d.expected)='object' THEN d.expected ELSE '{{}}'::jsonb END) key
    UNION SELECT jsonb_object_keys(CASE WHEN jsonb_typeof(d.observed)='object' THEN d.observed ELSE '{{}}'::jsonb END)
   ) keys WHERE d.expected IS DISTINCT FROM d.observed
  )
  SELECT jsonb_agg(jsonb_build_object('path',array_to_string(path,'.'),
   'expected',expected,'observed',observed) ORDER BY path COLLATE "C") INTO differences
  FROM delta WHERE expected IS DISTINCT FROM observed
   AND (COALESCE(jsonb_typeof(expected),'null')<>'object' OR expected='{{}}'::jsonb)
   AND (COALESCE(jsonb_typeof(observed),'null')<>'object' OR observed='{{}}'::jsonb);
  RAISE EXCEPTION 'Publication identity changed: %',differences;
 END IF;
 IF EXISTS(SELECT FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname={sql_literal(job['schema_name'])} AND NOT k.convalidated) THEN
  RAISE EXCEPTION 'Unvalidated shadow constraints';
 END IF;
END $identity$;
"""
