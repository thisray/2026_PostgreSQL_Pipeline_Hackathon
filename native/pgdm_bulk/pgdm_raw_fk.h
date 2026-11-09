#ifndef PGDM_RAW_FK_H
#define PGDM_RAW_FK_H
#define PGDM_RAW_FK_POLICY "raw_fk_tuple_witness_v2"

typedef struct PgdmRawFkProof
{
    bool enabled;
    Oid source_oid, target_oid, constraint_oid, fence_oid;
    Oid source_filenode, target_filenode;
    int64 source_raw_id;
    int64 witness[BULK_TUPLES];
    int64 verified_rows, inserted_rows;
} PgdmRawFkProof;
#endif
