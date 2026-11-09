-- Installed only through the extension upgrade, never by an application role.
CREATE SCHEMA pgdm_fk_proof;
REVOKE ALL ON SCHEMA pgdm_fk_proof FROM PUBLIC;
GRANT USAGE ON SCHEMA pgdm_fk_proof TO PUBLIC;
CREATE TABLE pgdm_fk_proof.epochs (
 job_id text PRIMARY KEY,
 epoch_id bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
 owner_oid oid NOT NULL,
 schema_oid oid NOT NULL,
 build_id text NOT NULL,
 valid boolean NOT NULL DEFAULT true
);
CREATE TABLE pgdm_fk_proof.relations (
 job_id text NOT NULL REFERENCES pgdm_fk_proof.epochs(job_id) ON DELETE CASCADE,
 relation_oid oid NOT NULL,
 filenode oid NOT NULL,
 guard_oid oid NOT NULL,
 guard_xmin text NOT NULL,
 function_oid oid NOT NULL,
 function_xmin text NOT NULL,
 PRIMARY KEY(job_id,relation_oid)
);
CREATE TABLE pgdm_fk_proof.key_sets (
 job_id text NOT NULL REFERENCES pgdm_fk_proof.epochs(job_id) ON DELETE CASCADE,
 relation_oid oid NOT NULL,
 attnum smallint NOT NULL,
 type_oid oid NOT NULL,
 rows bigint NOT NULL CHECK(rows>=0),
 null_rows bigint NOT NULL CHECK(null_rows>=0 AND null_rows<=rows),
 distinct_keys bigint NOT NULL CHECK(distinct_keys>=0),
 min_key bigint NOT NULL,
 max_key bigint NOT NULL,
 bits bytea NOT NULL,
 PRIMARY KEY(job_id,relation_oid,attnum),
 FOREIGN KEY(job_id,relation_oid) REFERENCES pgdm_fk_proof.relations(job_id,relation_oid) ON DELETE CASCADE
);
GRANT SELECT ON pgdm_fk_proof.epochs,pgdm_fk_proof.relations,pgdm_fk_proof.key_sets TO PUBLIC;
