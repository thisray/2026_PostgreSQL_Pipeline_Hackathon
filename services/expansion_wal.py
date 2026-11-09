"""Session-local WAL compression without changing durability or role privileges."""

# Session scope survives durable chunk commits. Ordinary roles never SET it.
SESSION_WAL_BODY = """IF current_setting('is_superuser')='on' THEN
 PERFORM set_config('deadlock_timeout','50ms',false);
 BEGIN
  PERFORM set_config('wal_compression','lz4',false);
 EXCEPTION WHEN invalid_parameter_value OR feature_not_supported THEN
  PERFORM set_config('wal_compression','pglz',false);
 END;
END IF;"""
SESSION_WAL_SQL = 'DO $pgdm_wal$ BEGIN ' + SESSION_WAL_BODY + ' END $pgdm_wal$;\n'
