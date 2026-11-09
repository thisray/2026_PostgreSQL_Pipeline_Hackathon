/* Actual integer-key sets; an unsupported value invalidates the whole proof. */
#ifndef PGDM_FK_SETS_INC
#define PGDM_FK_SETS_INC

#define PGDM_FK_SET_MAX_BYTES ((Size) 64 * 1024 * 1024)
#define PGDM_FK_SET_MAX_KEY ((uint64) PGDM_FK_SET_MAX_BYTES * 8 - 1)

typedef struct PgdmFkSet
{
    Oid relation_oid;
    Oid type_oid;
    AttrNumber attnum;
    MemoryContext context;
    unsigned char *bits;
    Size allocated;
    uint64 rows, null_rows, distinct_keys;
    int64 min_key, max_key;
    bool active, invalid;
    const char *reason;
} PgdmFkSet;

static void
pgdm_fk_set_init(PgdmFkSet *set, Relation relation, AttrNumber attnum,
                 MemoryContext context)
{
    memset(set, 0, sizeof(*set));
    set->relation_oid = RelationGetRelid(relation);
    set->attnum = attnum;
    set->type_oid = TupleDescAttr(RelationGetDescr(relation), attnum - 1)->atttypid;
    set->context = context;
    set->min_key = PG_INT64_MAX;
    set->max_key = -1;
    set->active = true;
    if (set->type_oid != INT8OID && set->type_oid != INT2OID)
    {
        set->invalid = true;
        set->reason = "unsupported_key_type";
    }
}

static void
pgdm_fk_set_observe(PgdmFkSet *set, Datum value, bool isnull, bool alive)
{
    int64 key;
    Size offset;
    unsigned char mask;
    MemoryContext old;
    if (!set->active) return;
    set->rows++;
    if (!alive)
    {
        set->invalid = true;
        set->reason = "nonlive_index_tuple";
    }
    if (isnull)
    {
        set->null_rows++;
        return;
    }
    if (set->invalid) return;
    key = set->type_oid == INT8OID ? DatumGetInt64(value) : DatumGetInt16(value);
    if (key < 0 || (uint64) key > PGDM_FK_SET_MAX_KEY)
    {
        set->invalid = true;
        set->reason = "actual_key_out_of_range";
        return;
    }
    set->min_key = Min(set->min_key, key);
    set->max_key = Max(set->max_key, key);
    offset = (Size) ((uint64) key >> 3);
    if (offset >= set->allocated)
    {
        Size previous = set->allocated;
        Size next = previous ? previous : 128;
        while (next <= offset) next *= 2;
        Assert(next <= PGDM_FK_SET_MAX_BYTES);
        old = MemoryContextSwitchTo(set->context);
        set->bits = set->bits ? repalloc(set->bits, next) : palloc(next);
        memset(set->bits + previous, 0, next - previous);
        set->allocated = next;
        MemoryContextSwitchTo(old);
    }
    mask = (unsigned char) (1U << ((uint64) key & 7));
    if ((set->bits[offset] & mask) == 0) set->distinct_keys++;
    set->bits[offset] |= mask;
}

static bool
pgdm_fk_set_complete(PgdmFkSet *set, uint64 actual_rows)
{
    if (!set->active || set->invalid || set->rows != actual_rows ||
        set->null_rows > set->rows)
        return false;
    /* Density comes from actual distinct keys, never a generator estimate. */
    if (set->max_key >= 0 && (uint64) set->max_key + 1 >
        set->distinct_keys * UINT64CONST(64) + UINT64CONST(4096))
    {
        set->invalid = true;
        set->reason = "actual_sparse_key_range";
        return false;
    }
    return true;
}

static bytea *
pgdm_fk_set_bytes(PgdmFkSet *set)
{
    Size bytes = set->max_key < 0 ? 0 : (Size) ((uint64) set->max_key / 8 + 1);
    bytea *result = palloc(VARHDRSZ + bytes);
    SET_VARSIZE(result, VARHDRSZ + bytes);
    if (bytes) memcpy(VARDATA(result), set->bits, bytes);
    return result;
}
#endif
