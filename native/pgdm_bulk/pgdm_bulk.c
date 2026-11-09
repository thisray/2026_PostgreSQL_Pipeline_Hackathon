#include "postgres.h"
#include <sys/resource.h>
#include "access/xlog.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "funcapi.h"
#include "access/heapam.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_class.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_collation_d.h"
#include "catalog/pg_namespace_d.h"
#include "catalog/pg_type_d.h"
#include "common/hashfn.h"
#include "common/int.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "storage/bufmgr.h"
#include "storage/fd.h"
#include "storage/smgr.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "utils/syscache.h"

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(pgdm_bulk_chunk);
PG_FUNCTION_INFO_V1(pgdm_bulk_build_id);
PG_FUNCTION_INFO_V1(pgdm_bulk_hint_pages);
PGDLLEXPORT void _PG_init(void);

#define PGDM_BULK_BUILD_PREFIX "s4-native-bulk-20261106.27-b1-stage1:"
#define PGDM_BULK_BUILD_ID PGDM_BULK_BUILD_PREFIX PGDM_BULK_SOURCE_SHA256
#define BULK_TUPLES 1024
#define BULK_BYTES (1024 * 1024)
#define SOURCE_BATCH_TUPLES 1024
#define SOURCE_LEGACY_BATCH_TUPLES 32
#define SOURCE_READ_RING_KB (16 * 1024)
#define SOURCE_PREFETCH_BLOCKS 256
#define SOURCE_STREAM_PREFETCH_BLOCKS 4096
#define INVALID_CODE_CACHE_ENTRIES 4096
#define INVALID_CODE_CACHE_BYTES (1024 * 1024)

/* Keep the measured .6 reader selectable without discarding its source path. */
static bool pgdm_direct_read_ahead = true;
static char *pgdm_last_written_pages = NULL;
static char *pgdm_last_source_io = NULL;
static char *pgdm_last_decode_counters = NULL;

#include "pgdm_fk_sets.inc"
#include "pgdm_fk_epoch.inc"
#include "pgdm_fk_collectors.inc"
#include "pgdm_fk_consumer.inc"
#include "pgdm_fk_publish.inc"
#include "pgdm_parallel_ri.inc"

void
_PG_init(void)
{
    pgdm_init_parallel_ri();
    RegisterXactCallback(pgdm_fk_context_end,NULL);
    DefineCustomStringVariable("pgdm_bulk.last_decode_counters",
                               "Exact-byte cache counters from the last successful chunk.",
                               NULL, &pgdm_last_decode_counters, "{}", PGC_USERSET,
                               GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);
    DefineCustomStringVariable("pgdm_bulk.last_source_io",
                               "Bounded source prefetch hints from the last successful chunk.",
                               NULL, &pgdm_last_source_io, "{}", PGC_USERSET,
                               GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);
    DefineCustomStringVariable("pgdm_bulk.last_written_pages",
                               "Physical pages written by the last successful bulk chunk.",
                               NULL, &pgdm_last_written_pages, "{}", PGC_USERSET,
                               GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);
    DefineCustomBoolVariable("pgdm_bulk.direct_read_ahead",
                             "Use bounded larger source batches and read-ahead for TID chunks.",
                             "Off retains the build .6 32-slot direct reader policy.",
                             &pgdm_direct_read_ahead, true, PGC_USERSET, 0,
                             NULL, NULL, NULL);
}

#include "decoder_core.inc"
#include "pgdm_check_memo.inc"
#include "pgdm_tuple_template.inc"
#include "pgdm_slot_slab.inc"

typedef struct TargetSpec
{
    const char *name;
    int natts;
    const char *columns[17];
    Oid types[17];
} TargetSpec;

static const TargetSpec target_specs[4] = {
    {"table1", 17,
     {"id_column_1", "id_column_2", "text_column_1", "datetime_column_1", "text_column_2",
      "integer_column_1", "integer_column_2", "integer_column_3", "text_column_3", "id_column_3",
      "id_column_4", "id_column_5", "integer_column_4", "integer_column_5", "id_column_6",
      "integer_column_6", "integer_column_7"},
     {INT8OID, INT4OID, VARCHAROID, TIMESTAMPOID, VARCHAROID, INT2OID, INT2OID, INT4OID,
      VARCHAROID, INT8OID, INT8OID, INT8OID, INT4OID, INT4OID, INT4OID, INT4OID, INT4OID}},
    {"table2", 9,
     {"id_column_1", "id_column_2", "integer_column_1", "integer_column_2", "text_column_1",
      "integer_column_3", "integer_column_4", "integer_column_5", "integer_column_6"},
     {INT8OID, INT8OID, INT2OID, INT4OID, VARCHAROID, INT4OID, INT4OID, INT4OID, INT4OID}},
    {"table5", 7,
     {"id_column_1", "numeric_column_1", "numeric_column_2", "integer_column_1",
      "integer_column_2", "id_column_2", "id_column_3"},
     {INT8OID, NUMERICOID, NUMERICOID, INT4OID, INT4OID, INT8OID, INT8OID}},
    {"table6", 3,
     {"id_column_1", "id_column_2", "id_column_3"},
     {INT8OID, INT8OID, INT2OID}}
};

#include "pgdm_raw_fk.h"

typedef struct TargetBuffer
{
    Relation relation;
    ResultRelInfo result;
    TupleTableSlot *check_slot;
    TupleTableSlot *slots[BULK_TUPLES];
    MemoryContext tuples_context;
    BulkInsertState bulk_state;
    int used;
    Size bytes;
    int64 rows;
    MemoryContext pages_context;
    BlockNumber *written_pages;
    int page_count;
    int page_capacity;
    int not_null_count;
    int not_null_attrs[17];
    bool has_check_constraints;
    PgdmCheckMemo *check_memo;
    PgdmTupleTemplateCache *tuple_templates;
    PgdmHeapSlotSlab slot_slab;
    uint64 constraint_calls;
    uint64 not_null_rows;
    uint64 not_null_failures;
    uint64 flushes;
    double insert_cpu_ms;
    PgdmRawFkProof raw_fk;
} TargetBuffer;

typedef struct DimensionKey
{
    Datum ref, first, last;
    Oid ref_collation, first_collation, last_collation;
    uint32 hash;
} DimensionKey;

typedef struct DimensionEntry
{
    DimensionKey key;
    int64 id;
} DimensionEntry;

typedef struct DimensionMap
{
    HTAB *table;
    Oid ref_collation, first_collation, last_collation;
} DimensionMap;

typedef struct CodeKey
{
    Datum value;
    Oid collation;
    uint32 hash;
} CodeKey;

typedef struct CodeEntry
{
    CodeKey key;
    int16 id;
    bool valid;
} CodeEntry;

typedef struct SourceReader
{
    DecodeContext *decoder;
    Relation relation;
    TableScanDesc scan;
    Snapshot snapshot;
    AttrNumber id_att, raw_att, schema_att, hash_att;
    Oid schema_collation, hash_collation;
    Datum *scope_values;
    bool *scope_nulls;
    int scope_count;
    bool have_scope;
    bool exhausted;
    int batch_capacity;
    int prefetch_blocks;
    uint64 prefetch_next, prefetch_limit, prefetch_refill_at;
    uint64 prefetch_calls, prefetch_pages;
    TupleTableSlot *slots[SOURCE_BATCH_TUPLES];
} SourceReader;

typedef struct ManifestKey
{
    Datum schema, hash_value;
    bool schema_null, hash_null;
    Oid schema_collation, hash_collation;
    uint32 hash;
} ManifestKey;

typedef struct ManifestEntry
{
    ManifestKey key;
    int64 rows;
} ManifestEntry;

typedef struct ManifestMap
{
    HTAB *table;
    DecodeContext *decoder;
    ManifestEntry *last;
    MemoryContext context;
    Oid schema_collation, hash_collation;
} ManifestMap;

static double
elapsed_ms(TimestampTz started)
{
    return (double) (GetCurrentTimestamp() - started) / 1000.0;
}

static AttrNumber
source_attribute(Relation relation, const char *name, Oid type)
{
    AttrNumber number = get_attnum(RelationGetRelid(relation), name);
    Form_pg_attribute attr;
    if (number <= 0)
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
                        errmsg("raw_data.%s does not exist", name)));
    attr = TupleDescAttr(RelationGetDescr(relation), number - 1);
    if (attr->attisdropped || attr->atttypid != type)
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                        errmsg("unexpected type for raw_data.%s", name)));
    return number;
}

static void
open_source(SourceReader *source, ArrayType *scope, bool direct)
{
    Oid namespace_oid = get_namespace_oid("public", false);
    Oid relation_oid = get_relname_relid("raw_data", namespace_oid);
    AclResult acl;
    AttrNumber columns[4];
    int i;

    memset(source, 0, sizeof(*source));
    acl = object_aclcheck(NamespaceRelationId, namespace_oid, GetUserId(), ACL_USAGE);
    if (acl != ACLCHECK_OK) aclcheck_error(acl, OBJECT_SCHEMA, "public");
    if (!OidIsValid(relation_oid))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("public.raw_data does not exist")));
    source->relation = table_open(relation_oid, AccessShareLock);
    if (direct &&
        (source->relation->rd_rel->relkind != RELKIND_RELATION ||
         source->relation->rd_rel->relam != HEAP_TABLE_AM_OID ||
         source->relation->rd_rel->relispartition ||
         source->relation->rd_rel->relhassubclass ||
         source->relation->rd_rel->relrowsecurity))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("direct pgdm_bulk source must be an ordinary heap without inheritance, partitioning, or row security")));

    source->id_att = source_attribute(source->relation, "raw_id", INT8OID);
    source->raw_att = source_attribute(source->relation, "raw", JSONBOID);
    source->schema_att = source_attribute(source->relation, "raw_schema", TEXTOID);
    source->hash_att = source_attribute(source->relation, "raw_hash", TEXTOID);
    source->schema_collation = TupleDescAttr(RelationGetDescr(source->relation), source->schema_att - 1)->attcollation;
    source->hash_collation = TupleDescAttr(RelationGetDescr(source->relation), source->hash_att - 1)->attcollation;
    columns[0] = source->id_att; columns[1] = source->raw_att;
    columns[2] = source->schema_att; columns[3] = source->hash_att;
    acl = pg_class_aclcheck(relation_oid, GetUserId(), ACL_SELECT);
    if (acl != ACLCHECK_OK)
        for (i = 0; i < 4; ++i)
        {
            acl = pg_attribute_aclcheck(relation_oid, columns[i], GetUserId(), ACL_SELECT);
            if (acl != ACLCHECK_OK) aclcheck_error(acl, OBJECT_TABLE, "raw_data");
        }
    deconstruct_array(scope, TEXTOID, -1, false, TYPALIGN_INT,
                      &source->scope_values, &source->scope_nulls, &source->scope_count);
    for (i = 0; i < source->scope_count; ++i)
        if (!source->scope_nulls[i]) source->have_scope = true;
}

static void
begin_source_range(SourceReader *source, int64 lo, int64 hi)
{
    ItemPointerData minimum, maximum;
    HeapScanDesc heap_scan;
    uint32 flags = SO_TYPE_TIDRANGESCAN | SO_ALLOW_PAGEMODE | SO_ALLOW_STRAT;
    int i;

    if (lo < 0 || lo > (int64) MaxBlockNumber ||
        hi < -1 || hi > (int64) MaxBlockNumber)
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                        errmsg("pgdm_bulk TID page bounds are out of range")));
    if (hi < lo || !source->have_scope) return;
    if (!ActiveSnapshotSet() || !IsMVCCSnapshot(GetActiveSnapshot()))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("direct pgdm_bulk requires an active MVCC snapshot")));
    source->snapshot = RegisterSnapshot(GetActiveSnapshot());
    /*
     * lo and hi are inclusive heap page numbers. These valid offsets cover
     * exactly ctid >= (lo,0) AND ctid < (hi+1,0), without overflow at hi+1.
     */
    ItemPointerSet(&minimum, (BlockNumber) lo, FirstOffsetNumber);
    ItemPointerSet(&maximum, (BlockNumber) hi, MaxOffsetNumber);
    /*
     * PG16's standard TID-range wrapper omits SO_ALLOW_STRAT. Initialize via
     * the same table-AM callbacks, adding only the bounded bulk-read policy.
     * Do not enable syncscan: each durable chunk has a fixed start block.
     */
    source->scan = source->relation->rd_tableam->scan_begin(
        source->relation, source->snapshot, 0, NULL, NULL, flags);
    source->relation->rd_tableam->scan_set_tidrange(source->scan, &minimum, &maximum);
    heap_scan = (HeapScanDesc) source->scan;  /* open_source checked heap AM. */
    source->batch_capacity = SOURCE_LEGACY_BATCH_TUPLES;
    if (pgdm_direct_read_ahead)
    {
        int ring_buffers;
        /*
         * PG16 exports this size-aware strategy API. The heap scan owns the
         * replacement and heap_endscan will free it. PG caps its size at
         * shared_buffers/8; cap the live input pins below that actual size.
         */
#if PG_VERSION_NUM < 170000
        FreeAccessStrategy(heap_scan->rs_strategy);
        heap_scan->rs_strategy = NULL;
        heap_scan->rs_strategy = GetAccessStrategyWithSize(BAS_BULKREAD,
                                                          SOURCE_READ_RING_KB);
#else
        /* PG17+ read streams retain this strategy from scan_begin. Replacing
         * it here leaves the stream pointing at freed memory. Keep the core
         * stream and its strategy together until heap_endscan owns cleanup. */
#endif
        ring_buffers = heap_scan->rs_strategy ?
            GetAccessStrategyBufferCount(heap_scan->rs_strategy) : SOURCE_BATCH_TUPLES;
        source->batch_capacity = Min(SOURCE_BATCH_TUPLES, Max(1, ring_buffers / 2));
#if PG_VERSION_NUM < 170000
        source->prefetch_blocks = Min(SOURCE_PREFETCH_BLOCKS, Max(1, ring_buffers / 4));
#else
        /* Kernel advice owns no shared buffers or pins. Keep the core stream
         * strategy intact and warm a bounded range ahead of its demand reads. */
        source->prefetch_blocks = (io_direct_flags & IO_DIRECT_DATA) ?
            0 : SOURCE_STREAM_PREFETCH_BLOCKS;
#endif
        source->prefetch_next = (uint64) lo;
        source->prefetch_limit = Min((uint64) hi + 1, (uint64) heap_scan->rs_nblocks);
        source->prefetch_refill_at = (uint64) lo;
    }
    for (i = 0; i < source->batch_capacity; ++i)
        source->slots[i] = MakeSingleTupleTableSlot(RelationGetDescr(source->relation),
                                                   table_slot_callbacks(source->relation));
}

static void
prefetch_source(SourceReader *source)
{
    HeapScanDesc scan = (HeapScanDesc) source->scan;
    uint64 position, end;
    if (source->prefetch_blocks == 0 || source->prefetch_next >= source->prefetch_limit)
        return;
    position = scan->rs_inited ? (uint64) scan->rs_cblock : (uint64) scan->rs_startblock;
    if (position < source->prefetch_refill_at || position >= source->prefetch_limit)
        return;
    /* Dead/filtered pages can move the scan ahead without yielding a row. */
    source->prefetch_next = Max(source->prefetch_next, position);
    end = Min(source->prefetch_limit, position + source->prefetch_blocks);
#if PG_VERSION_NUM >= 170000
    /* smgrprefetch -> mdprefetch -> FilePrefetch issues kernel WILLNEED
     * directly, independently of PG18's synchronous IO-worker queue. The
     * storage manager splits relation segments; actual reads still use the
     * unchanged MVCC heap scan, checksums and PostgreSQL read stream. */
    if (source->prefetch_next < end)
    {
        (void) smgrprefetch(RelationGetSmgr(source->relation), MAIN_FORKNUM,
                            (BlockNumber) source->prefetch_next,
                            (int) (end - source->prefetch_next));
        ++source->prefetch_calls;
        source->prefetch_pages += end - source->prefetch_next;
        source->prefetch_next = end;
    }
#else
    while (source->prefetch_next < end)
    {
        /*
         * Best-effort kernel read-ahead only: no buffer is allocated or
         * pinned, no tuple/visibility shortcut, and never outside this
         * chunk or beyond the relation size captured by the MVCC scan.
         */
        (void) PrefetchBuffer(source->relation, MAIN_FORKNUM,
                              (BlockNumber) source->prefetch_next);
        ++source->prefetch_calls;
        ++source->prefetch_pages;
        ++source->prefetch_next;
    }
#endif
    source->prefetch_refill_at = position + Max(1, source->prefetch_blocks / 4);
}

static bool
source_in_scope(SourceReader *source, Datum schema, bool isnull)
{
    int i;
    if (isnull) return false;  /* NULL = ANY(scope) cannot pass a WHERE clause. */
    for (i = 0; i < source->scope_count; ++i)
        if (!source->scope_nulls[i] &&
            exact_text_equal(schema, source->scope_values[i], source->schema_collation,
                             &source->decoder->scope_equal_hits))
            return true;
    return false;
}

static uint64
fetch_source_batch(SourceReader *source, MemoryContext scratch)
{
    uint64 fetched = 0;
    if (source->scan == NULL || source->exhausted) return 0;
    /*
     * Buffer-backed slots borrow heap pages; no HeapTuple/raw JSON copy.
     * The .7 batch is at most half the actual sized read ring, with one
     * extra scan pin. The .6 policy remains 32 slots on the original ring.
     * The decode loop clears every slot before refilling the batch.
     */
    while (fetched < source->batch_capacity)
    {
        TupleTableSlot *slot = source->slots[fetched];
        bool isnull, selected;
        Datum schema;
        MemoryContext old;
        prefetch_source(source);
        if (!table_scan_getnextslot_tidrange(source->scan, ForwardScanDirection, slot))
        {
            /* A heap AM may restart if called again after end-of-scan. */
            source->exhausted = true;
            break;
        }
        {
            BufferHeapTupleTableSlot *buffer_slot = (BufferHeapTupleTableSlot *) slot;
            /*
             * heap_getnextslot_tidrange lends scan->rs_ctup, whose small
             * HeapTupleData header changes on the next fetch. Preserve that
             * header per slot exactly as PG's buffer-slot copyslot does;
             * tuple payload still points into this slot's pinned heap page.
             */
            buffer_slot->base.tupdata = *buffer_slot->base.tuple;
            buffer_slot->base.tuple = &buffer_slot->base.tupdata;
        }
        old = MemoryContextSwitchTo(scratch);
        schema = slot_getattr(slot, source->schema_att, &isnull);
        selected = source_in_scope(source, schema, isnull);
        MemoryContextSwitchTo(old);
        MemoryContextReset(scratch);
        if (selected) ++fetched;
        else ExecClearTuple(slot);
        CHECK_FOR_INTERRUPTS();
    }
    return fetched;
}

static void
close_source(SourceReader *source)
{
    int i;
    for (i = 0; i < source->batch_capacity; ++i)
        if (source->slots[i] != NULL) ExecDropSingleTupleTableSlot(source->slots[i]);
    if (source->scan != NULL) table_endscan(source->scan);
    if (source->snapshot != NULL) UnregisterSnapshot(source->snapshot);
    /* Like a normal SELECT, retain the relation lock until transaction end. */
    table_close(source->relation, NoLock);
}

static uint32
manifest_hash(const void *key, Size size)
{
    return ((const ManifestKey *) key)->hash;
}

static int
manifest_match(const void *left, const void *right, Size size)
{
    const ManifestKey *a = left, *b = right;
    if (a->schema_null != b->schema_null || a->hash_null != b->hash_null) return 1;
    if (!a->schema_null &&
        !DatumGetBool(DirectFunctionCall2Coll(texteq, a->schema_collation, a->schema, b->schema))) return 1;
    if (!a->hash_null &&
        !DatumGetBool(DirectFunctionCall2Coll(texteq, a->hash_collation, a->hash_value, b->hash_value))) return 1;
    return 0;
}

static ManifestMap
create_manifest(SourceReader *source, MemoryContext context)
{
    ManifestMap map;
    HASHCTL ctl;
    memset(&map, 0, sizeof(map));
    memset(&ctl, 0, sizeof(ctl));
    ctl.keysize = sizeof(ManifestKey); ctl.entrysize = sizeof(ManifestEntry);
    ctl.hash = manifest_hash; ctl.match = manifest_match; ctl.hcxt = context;
    map.table = hash_create("pgdm source manifest", 32, &ctl,
                            HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_CONTEXT);
    map.context = context;
    map.schema_collation = source->schema_collation;
    map.hash_collation = source->hash_collation;
    return map;
}

static void
observe_manifest(ManifestMap *map, Datum schema, bool schema_null,
                 Datum hash_value, bool hash_null)
{
    ManifestKey key;
    ManifestEntry *entry = map->last;
    bool found;
    uint32 a, b;
    memset(&key, 0, sizeof(key));
    key.schema = schema; key.schema_null = schema_null;
    key.hash_value = hash_value; key.hash_null = hash_null;
    key.schema_collation = map->schema_collation;
    key.hash_collation = map->hash_collation;
    /* Repeated adjacent real groups avoid rehashing long schema/hash strings. */
    if (entry == NULL || key.schema_null != entry->key.schema_null ||
        key.hash_null != entry->key.hash_null ||
        (!key.schema_null && !exact_text_equal(schema, entry->key.schema, map->schema_collation,
                                               &map->decoder->manifest_equal_hits)) ||
        (!key.hash_null && !exact_text_equal(hash_value, entry->key.hash_value, map->hash_collation,
                                             &map->decoder->manifest_equal_hits)))
    {
        a = schema_null ? 0 : DatumGetUInt32(DirectFunctionCall1Coll(hashtext, key.schema_collation, schema));
        b = hash_null ? 0 : DatumGetUInt32(DirectFunctionCall1Coll(hashtext, key.hash_collation, hash_value));
        key.hash = hash_combine(a, b);
        entry = hash_search(map->table, &key, HASH_FIND, NULL);
        if (entry == NULL)
        {
            MemoryContext old = MemoryContextSwitchTo(map->context);
            if (!schema_null) key.schema = PointerGetDatum(PG_DETOAST_DATUM_COPY(schema));
            if (!hash_null) key.hash_value = PointerGetDatum(PG_DETOAST_DATUM_COPY(hash_value));
            entry = hash_search(map->table, &key, HASH_ENTER, &found);
            entry->rows = 0;
            MemoryContextSwitchTo(old);
        }
        map->last = entry;
    }
    if (pg_add_s64_overflow(entry->rows, 1, &entry->rows))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("manifest row count out of range")));
}

static void
manifest_json_key(JsonbParseState **state, const char *name)
{
    JsonbValue value;
    value.type = jbvString;
    value.val.string.val = (char *) name;
    value.val.string.len = strlen(name);
    pushJsonbValue(state, WJB_KEY, &value);
}

static void
manifest_json_text(JsonbParseState **state, Datum datum, bool isnull)
{
    JsonbValue value;
    if (isnull) value.type = jbvNull;
    else
    {
        text *string = DatumGetTextPP(datum);
        value.type = jbvString;
        value.val.string.val = VARDATA_ANY(string);
        value.val.string.len = VARSIZE_ANY_EXHDR(string);
    }
    pushJsonbValue(state, WJB_VALUE, &value);
}

static Jsonb *
manifest_json(ManifestMap *map)
{
    HASH_SEQ_STATUS scan;
    ManifestEntry *entry;
    JsonbParseState *state = NULL;
    JsonbValue value, *result;
    pushJsonbValue(&state, WJB_BEGIN_ARRAY, NULL);
    hash_seq_init(&scan, map->table);
    while ((entry = hash_seq_search(&scan)) != NULL)
    {
        pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
        manifest_json_key(&state, "raw_schema");
        manifest_json_text(&state, entry->key.schema, entry->key.schema_null);
        manifest_json_key(&state, "raw_hash");
        manifest_json_text(&state, entry->key.hash_value, entry->key.hash_null);
        manifest_json_key(&state, "row_count");
        value.type = jbvNumeric;
        value.val.numeric = DatumGetNumeric(DirectFunctionCall1(int8_numeric, Int64GetDatum(entry->rows)));
        pushJsonbValue(&state, WJB_VALUE, &value);
        pushJsonbValue(&state, WJB_END_OBJECT, NULL);
    }
    result = pushJsonbValue(&state, WJB_END_ARRAY, NULL);
    return JsonbValueToJsonb(result);
}

static uint32
dimension_hash(const void *key, Size size)
{
    return ((const DimensionKey *) key)->hash;
}

static int
dimension_match(const void *left, const void *right, Size size)
{
    const DimensionKey *a = left, *b = right;
    if (!DatumGetBool(DirectFunctionCall2Coll(texteq, a->ref_collation, a->ref, b->ref))) return 1;
    if (!DatumGetBool(DirectFunctionCall2Coll(texteq, a->first_collation, a->first, b->first))) return 1;
    if (!DatumGetBool(DirectFunctionCall2Coll(texteq, a->last_collation, a->last, b->last))) return 1;
    return 0;
}

static DimensionKey
dimension_key(Datum ref, Datum first, Datum last, const DimensionMap *map)
{
    DimensionKey key;
    uint32 a, b, c;
    key.ref = ref; key.first = first; key.last = last;
    key.ref_collation = map->ref_collation;
    key.first_collation = map->first_collation;
    key.last_collation = map->last_collation;
    a = DatumGetUInt32(DirectFunctionCall1Coll(hashtext, key.ref_collation, ref));
    b = DatumGetUInt32(DirectFunctionCall1Coll(hashtext, key.first_collation, first));
    c = DatumGetUInt32(DirectFunctionCall1Coll(hashtext, key.last_collation, last));
    key.hash = hash_combine(hash_combine(a, b), c);
    return key;
}

static uint32
code_hash(const void *key, Size size)
{
    return ((const CodeKey *) key)->hash;
}

static int
code_match(const void *left, const void *right, Size size)
{
    const CodeKey *a = left, *b = right;
    return !DatumGetBool(DirectFunctionCall2Coll(texteq, a->collation, a->value, b->value));
}

static HTAB *
create_code_set(MemoryContext context)
{
    HASHCTL ctl;
    memset(&ctl, 0, sizeof(ctl));
    ctl.keysize = sizeof(CodeKey); ctl.entrysize = sizeof(CodeEntry);
    ctl.hash = code_hash; ctl.match = code_match; ctl.hcxt = context;
    return hash_create("pgdm observed codes", 64, &ctl,
                       HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_CONTEXT);
}

static ByteMemoEntry *
text_result_entry(ByteMemo *memo, Datum value, const Datum *second,
                  unsigned slots, DecodeContext *ctx)
{
    text *a = DatumGetTextPP(value);
    Size length = VARSIZE_ANY_EXHDR(a);
    char key[MEMO_KEY_BYTES];
    if (length > 31)
    {
        ++memo->bypass;
        return NULL;
    }
    key[0] = (char) length;
    memcpy(key + 1, VARDATA_ANY(a), length);
    if (second)
    {
        text *b = DatumGetTextPP(*second);
        Size b_length = VARSIZE_ANY_EXHDR(b);
        if (b_length > 31)
        {
            ++memo->bypass;
            return NULL;
        }
        key[length + 1] = (char) b_length;
        memcpy(key + length + 2, VARDATA_ANY(b), b_length);
        length += 1 + b_length;
    }
    return memo_entry(memo, key, length + 1, MEMO_KEY_BYTES,
                      slots, slots / 2, ctx->context);
}

static bool
observe_code(HTAB *codes, Datum code, DecodeContext *ctx, MemoryContext context,
             Size *cache_bytes, int16 *code_id)
{
    CodeKey key;
    CodeEntry *entry;
    bool found, valid;
    Size bytes;
    ByteMemoEntry *memo = text_result_entry(&ctx->code, code, NULL, PGDM_TEXT_SLOTS, ctx);
    if (memo && (memo->flags & MEMO_RESULT))
    {
        ++ctx->code.calls_saved;
        if (memo->valid) *code_id = (int16) memo->id;
        return memo->valid;
    }
    key.value = code; key.collation = ctx->collation;
    key.hash = DatumGetUInt32(DirectFunctionCall1Coll(hashtext, key.collation, code));
    entry = hash_search(codes, &key, HASH_FIND, NULL);
    if (entry)
    {
        if (memo)
        {
            memo->valid = entry->valid;
            memo->id = entry->id;
            memo->flags |= MEMO_RESULT;
        }
        if (entry->valid) *code_id = entry->id;
        return entry->valid;
    }
    valid = DatumGetBool(DirectFunctionCall2Coll(textregexeq, ctx->collation, code, ctx->pattern));
    bytes = VARSIZE_ANY(DatumGetTextPP(code));
    /* Unique invalid strings must not turn a tiny code cache into a stage. */
    if (!valid && (hash_get_num_entries(codes) >= INVALID_CODE_CACHE_ENTRIES ||
                   bytes > INVALID_CODE_CACHE_BYTES || *cache_bytes > INVALID_CODE_CACHE_BYTES - bytes))
        return false;
    {
        MemoryContext old = MemoryContextSwitchTo(context);
        key.value = datumCopy(code, false, -1);
        entry = hash_search(codes, &key, HASH_ENTER, &found);
        entry->valid = valid;
        entry->id = 0;
        *cache_bytes += bytes;
        if (valid)
        {
            Datum first = DirectFunctionCall2(text_left, code, Int32GetDatum(1));
            Datum last = DirectFunctionCall2(text_right, code, Int32GetDatum(1));
            Datum a = DirectFunctionCall2(int4mi, DirectFunctionCall1(ascii, first), Int32GetDatum(65));
            Datum b = DirectFunctionCall2(int4mi, DirectFunctionCall1(ascii, last), Int32GetDatum(65));
            Datum id = DirectFunctionCall2(int4pl,
                           DirectFunctionCall2(int4pl,
                               DirectFunctionCall2(int4mul, a, Int32GetDatum(26)), b),
                           Int32GetDatum(1));
            entry->id = DatumGetInt16(DirectFunctionCall1(i4toi2, id));
        }
        MemoryContextSwitchTo(old);
    }
    /* Cache only after the original codes HTAB owns the successful result.
     * Invalid strings rejected by its cap never acquire a ready memo entry.
     */
    if (memo)
    {
        memo->valid = entry->valid;
        memo->id = entry->id;
        memo->flags |= MEMO_RESULT;
    }
    if (valid) *code_id = entry->id;
    return valid;
}

static DimensionMap
load_dimensions(const char *schema, MemoryContext context)
{
    DimensionMap map;
    HASHCTL ctl;
    char *sql = psprintf("SELECT id_column_1, text_column_1::text, text_column_3::text, text_column_4::text "
                         "FROM %s WHERE text_column_2 = 'SYNTHETIC/REFERENCE' AND numeric_column_1 IS NULL "
                         "AND text_column_1 IS NOT NULL AND text_column_3 IS NOT NULL AND text_column_4 IS NOT NULL",
                         quote_qualified_identifier(schema, "table3"));
    uint64 i, count;
    int status = SPI_execute(sql, true, 0);
    if (status != SPI_OK_SELECT) elog(ERROR, "could not read table3 dimensions");
    count = SPI_processed;
    map.ref_collation = TupleDescAttr(SPI_tuptable->tupdesc, 1)->attcollation;
    map.first_collation = TupleDescAttr(SPI_tuptable->tupdesc, 2)->attcollation;
    map.last_collation = TupleDescAttr(SPI_tuptable->tupdesc, 3)->attcollation;
    memset(&ctl, 0, sizeof(ctl));
    ctl.keysize = sizeof(DimensionKey); ctl.entrysize = sizeof(DimensionEntry);
    ctl.hash = dimension_hash; ctl.match = dimension_match; ctl.hcxt = context;
    map.table = hash_create("pgdm table3 dimensions", 64, &ctl,
                           HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_CONTEXT);
    for (i = 0; i < count; ++i)
    {
        bool isnull, found;
        Datum id = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull);
        Datum ref, first, last;
        DimensionKey key;
        DimensionEntry *entry;
        MemoryContext old;
        if (isnull) elog(ERROR, "table3 dimension ID must not be null");
        ref = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2, &isnull);
        first = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 3, &isnull);
        last = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 4, &isnull);
        old = MemoryContextSwitchTo(context);
        key = dimension_key(datumCopy(ref, false, -1), datumCopy(first, false, -1), datumCopy(last, false, -1), &map);
        entry = hash_search(map.table, &key, HASH_ENTER, &found);
        if (found) ereport(ERROR, (errcode(ERRCODE_CARDINALITY_VIOLATION), errmsg("ambiguous table3 dimension mapping")));
        entry->id = DatumGetInt64(id);
        MemoryContextSwitchTo(old);
    }
    SPI_freetuptable(SPI_tuptable);
    return map;
}

static bool
lookup_dimension_original(const DecodedRow *row, const DimensionMap *map, int64 *id)
{
    DimensionKey key;
    DimensionEntry *entry;
    if (row->nulls[7] || row->nulls[8]) return false;
    key = dimension_key(row->values[7],
                        DirectFunctionCall2(text_left, row->values[8], Int32GetDatum(1)),
                        DirectFunctionCall2(text_right, row->values[8], Int32GetDatum(1)), map);
    entry = hash_search(map->table, &key, HASH_FIND, NULL);
    if (!entry) return false;
    *id = entry->id;
    return true;
}

static bool
lookup_dimension(const DecodedRow *row, const DimensionMap *map, int64 *id, DecodeContext *ctx)
{
    ByteMemoEntry *memo;
    bool found;
    if (row->nulls[7] || row->nulls[8]) return false;
    memo = text_result_entry(&ctx->dimension, row->values[7], &row->values[8],
                             PGDM_DIMENSION_SLOTS, ctx);
    if (memo && (memo->flags & MEMO_RESULT))
    {
        ++ctx->dimension.calls_saved;
        if (memo->valid) *id = memo->id;
        return memo->valid;
    }
    found = lookup_dimension_original(row, map, id);
    if (memo)
    {
        memo->valid = found;
        if (found) memo->id = *id;
        memo->flags |= MEMO_RESULT;
    }
    return found;
}

static bool
valid_missing_parent(const DecodedRow *row, DecodeContext *ctx)
{
    /* This predicate is needed only when the actual dimension JOIN missed. */
    bool code_valid = !row->nulls[8] &&
        DatumGetBool(DirectFunctionCall2Coll(textregexeq, ctx->collation, row->values[8], ctx->pattern));
    return code_valid && !row->nulls[7] && VARSIZE_ANY_EXHDR(DatumGetTextPP(row->values[7])) > 0;
}

#include "pgdm_raw_fk.inc"

static void
open_target(TargetBuffer *target, Oid namespace_oid, const TargetSpec *spec,
            SourceReader *source)
{
    Oid relid = get_relname_relid(spec->name, namespace_oid);
    AclResult acl;
    TupleDesc desc;
    List *indexes;
    int i;
    if (!OidIsValid(relid)) ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("shadow table %s does not exist", spec->name)));
    target->relation = table_open(relid, RowExclusiveLock);
    pgdm_fk_require_unsealed_writer(target->relation);
    acl = pg_class_aclcheck(relid, GetUserId(), ACL_INSERT);
    if (acl != ACLCHECK_OK) aclcheck_error(acl, OBJECT_TABLE, spec->name);
    desc = RelationGetDescr(target->relation);
    indexes = RelationGetIndexList(target->relation);
    if (target->relation->rd_rel->relkind != RELKIND_RELATION ||
        target->relation->rd_rel->relam != HEAP_TABLE_AM_OID ||
        target->relation->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT ||
        target->relation->rd_rel->relispartition || target->relation->rd_rel->relrowsecurity ||
        !pgdm_raw_fk_open(target, spec, source) || target->relation->rd_rules != NULL || indexes != NIL)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                       errmsg("pgdm_bulk requires logged heap shadows without indexes, unsupported triggers, rules, partitions, or row security: %s", spec->name)));
    list_free(indexes);
    if (desc->natts != spec->natts) ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("unexpected column count in %s", spec->name)));
    target->has_check_constraints = desc->constr != NULL &&
                                    target->relation->rd_rel->relchecks > 0;
    for (i = 0; i < desc->natts; ++i)
    {
        Form_pg_attribute attr = TupleDescAttr(desc, i);
        if (attr->attisdropped || attr->attgenerated || attr->attidentity == ATTRIBUTE_IDENTITY_ALWAYS ||
            strcmp(NameStr(attr->attname), spec->columns[i]) != 0 || attr->atttypid != spec->types[i] || attr->atttypmod != -1)
            ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("unexpected target column %d in %s", i + 1, spec->name)));
        if (desc->constr != NULL && desc->constr->has_not_null && attr->attnotnull)
            target->not_null_attrs[target->not_null_count++] = i;
    }
    InitResultRelInfo(&target->result, target->relation, 0, NULL, 0);
    target->check_slot = MakeSingleTupleTableSlot(desc, &TTSOpsVirtual);
    target->check_slot->tts_tableOid = relid;
    target->check_memo = pgdm_check_memo_create(target->relation, CurrentMemoryContext);
    target->tuple_templates = pgdm_tuple_template_create(desc, spec->name, CurrentMemoryContext);
    target->tuples_context = AllocSetContextCreate(CurrentMemoryContext, "pgdm target buffer", ALLOCSET_DEFAULT_SIZES);
    target->bulk_state = GetBulkInsertState();
    target->pages_context = CurrentMemoryContext;
    pgdm_heap_slot_slab_init(&target->slot_slab, desc, target->slots,
                             BULK_TUPLES, CurrentMemoryContext);
}

static double
pgdm_cpu_ms(const struct rusage *usage)
{
    return (usage->ru_utime.tv_sec + usage->ru_stime.tv_sec) * 1000.0 +
           (usage->ru_utime.tv_usec + usage->ru_stime.tv_usec) / 1000.0;
}

static void
flush_target(TargetBuffer *target, EState *estate, CommandId cid, double *insert_ms)
{
    MemoryContext old;
    TimestampTz started;
    struct rusage cpu_before, cpu_after;
    bool cpu_ok;
    int i;
    if (target->used == 0) return;
    cpu_ok = getrusage(RUSAGE_SELF, &cpu_before) == 0;
    started = GetCurrentTimestamp();
    old = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
    table_multi_insert(target->relation, target->slots, target->used, cid, 0, target->bulk_state);
    MemoryContextSwitchTo(old);
    pgdm_raw_fk_verify(target);
    if (XLogHintBitIsNeeded())
    for (i = 0; i < target->used; ++i)
    {
        BlockNumber block = ItemPointerGetBlockNumber(&target->slots[i]->tts_tid);
        if (target->page_count && target->written_pages[target->page_count - 1] == block)
            continue;
        if (target->page_count == target->page_capacity)
        {
            MemoryContext page_old = MemoryContextSwitchTo(target->pages_context);
            target->page_capacity = target->page_capacity ? target->page_capacity * 2 : 128;
            target->written_pages = target->written_pages ?
                repalloc(target->written_pages, sizeof(BlockNumber) * target->page_capacity) :
                palloc(sizeof(BlockNumber) * target->page_capacity);
            MemoryContextSwitchTo(page_old);
        }
        target->written_pages[target->page_count++] = block;
    }
    for (i = 0; i < target->used; ++i)
    {
#if (PG_VERSION_NUM >= 160000 && PG_VERSION_NUM < 170000) || \
    (PG_VERSION_NUM >= 180000 && PG_VERSION_NUM < 190000)
        /* Collect written TIDs above, then release all pointers before reset.
         * Both Bump and AllocSet tuples belong to the bounded batch contexts.
         */
        target->slots[i]->tts_flags &= ~TTS_FLAG_SHOULDFREE;
#endif
        ExecClearTuple(target->slots[i]);
    }
    target->used = 0; target->bytes = 0;
    pgdm_tuple_template_reset(target->tuple_templates);
    MemoryContextReset(target->tuples_context);
    ResetExprContext(GetPerTupleExprContext(estate));
    *insert_ms += elapsed_ms(started);
    target->flushes++;
    if (getrusage(RUSAGE_SELF, &cpu_after) == 0 && cpu_ok)
        target->insert_cpu_ms += pgdm_cpu_ms(&cpu_after) - pgdm_cpu_ms(&cpu_before);
}

static void
check_target(TargetBuffer *target, Datum *values, bool *nulls, EState *estate)
{
    TupleDesc desc = RelationGetDescr(target->relation);
    MemoryContext old;
    bool need_core = target->has_check_constraints;
    bool not_null_failed = false;
    int learn_key = -1;
    int i;

    target->not_null_rows++;
    for (i = 0; i < target->not_null_count; ++i)
        if (nulls[target->not_null_attrs[i]])
        {
            not_null_failed = true;
            target->not_null_failures++;
            need_core = true;
            break;
        }
    if (!not_null_failed && need_core &&
        pgdm_check_memo_probe(target->check_memo, values, nulls, &learn_key))
        need_core = false;
    if (need_core)
    {
        TupleTableSlot *check_slot = target->check_slot;
        /* Check the existing Datums before encoding a physical heap tuple. */
        ExecClearTuple(check_slot);
        memcpy(check_slot->tts_values, values, sizeof(Datum) * desc->natts);
        memcpy(check_slot->tts_isnull, nulls, sizeof(bool) * desc->natts);
        ExecStoreVirtualTuple(check_slot);
        check_slot->tts_tableOid = RelationGetRelid(target->relation);
        old = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
        target->constraint_calls++;
        ExecConstraints(&target->result, check_slot, estate);
        MemoryContextSwitchTo(old);
        pgdm_check_memo_learn(target->check_memo, learn_key);
        ExecClearTuple(check_slot);
        ResetExprContext(GetPerTupleExprContext(estate));
    }
}

static void
append_target(TargetBuffer *target, Datum *values, bool *nulls,
              EState *estate, CommandId cid, double *insert_ms)
{
    TupleDesc desc = RelationGetDescr(target->relation);
    MemoryContext old;
    HeapTuple tuple;
    TupleTableSlot *slot = target->slots[target->used];
    check_target(target, values, nulls, estate);
    old = MemoryContextSwitchTo(pgdm_tuple_template_batch_context(
                                    target->tuple_templates, target->tuples_context));
    tuple = pgdm_form_cached_tuple(target->tuple_templates, desc, values, nulls,
                                    target->tuples_context);
    MemoryContextSwitchTo(old);
    tuple->t_tableOid = RelationGetRelid(target->relation);
    /* Owning slots avoid heap_multi_insert materializing a second tuple copy. */
    pgdm_store_new_heap_tuple(tuple, slot);
    if (target->raw_fk.enabled)
        target->raw_fk.witness[target->used] = target->raw_fk.source_raw_id;
    ++target->used; ++target->rows;
    target->bytes += tuple->t_len;
    if (target->used >= BULK_TUPLES || target->bytes >= BULK_BYTES)
        flush_target(target, estate, cid, insert_ms);
}

static void
emit_parent(TargetBuffer *targets, const DecodedRow *row, int64 dimension_id,
            bool have_code, int16 code_id, EState *estate, CommandId cid,
            DecodeContext *ctx, double *insert_ms)
{
    Datum values[17] = {0};
    bool nulls[17];
    int i;
    int64 raw_id = DatumGetInt64(row->values[0]);
    memset(nulls, true, sizeof(nulls));
    values[0] = row->values[0]; nulls[0] = false;
    values[1] = row->values[1]; nulls[1] = row->nulls[1];
    values[3] = row->values[2]; nulls[3] = row->nulls[2];
    values[5] = row->values[3]; nulls[5] = row->nulls[3];
    values[7] = row->values[4]; nulls[7] = row->nulls[4];
    values[9] = row->values[0]; nulls[9] = false;
    values[10] = Int64GetDatum(dimension_id); nulls[10] = false;
    values[16] = row->values[5]; nulls[16] = row->nulls[5];
    append_target(&targets[0], values, nulls, estate, cid, insert_ms);

    if (!row->nulls[6] && numeric_predicate(row->speed, ctx, false))
    {
        memset(nulls, true, sizeof(nulls));
        values[0] = row->values[0]; nulls[0] = false;
        values[1] = round_speed(row->speed, ctx); nulls[1] = false;
        values[5] = row->values[0]; nulls[5] = false;
        append_target(&targets[2], values, nulls, estate, cid, insert_ms);
    }
    if (have_code)
    {
        memset(nulls, true, sizeof(nulls));
        values[0] = row->values[0]; nulls[0] = false;
        values[1] = row->values[0]; nulls[1] = false;
        values[2] = Int16GetDatum(code_id); nulls[2] = false;
        append_target(&targets[3], values, nulls, estate, cid, insert_ms);
    }
    for (i = 0; i < row->slot_count; ++i)
    {
        int64 child_id, product;
        if (pg_sub_s64_overflow(raw_id, 1, &product) ||
            pg_mul_s64_overflow(product, 14, &product) ||
            pg_add_s64_overflow(product, DatumGetInt16(row->positions[i]), &child_id))
            ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("bigint out of range")));
        memset(nulls, true, sizeof(nulls));
        values[0] = Int64GetDatum(child_id); nulls[0] = false;
        values[1] = row->values[0]; nulls[1] = false;
        values[2] = row->positions[i]; nulls[2] = false;
        values[3] = row->weights[i]; nulls[3] = false;
        values[4] = row->types[i]; nulls[4] = false;
        values[8] = row->gaps[i]; nulls[8] = row->gap_nulls[i];
        append_target(&targets[1], values, nulls, estate, cid, insert_ms);
    }
}

static void
close_target(TargetBuffer *target)
{
    Assert(target->used == 0);
    FreeBulkInsertState(target->bulk_state);
    table_finish_bulk_insert(target->relation, 0);
    pgdm_heap_slot_slab_destroy(&target->slot_slab);
    ExecDropSingleTupleTableSlot(target->check_slot);
    table_close(target->relation, NoLock);
}

static char *
hotpath_receipt(TargetBuffer *targets, double read_cpu_ms, double decode_cpu_ms,
                double chunk_cpu_ms, uint64 batches)
{
    StringInfoData out;
    int i;
    initStringInfo(&out);
    appendStringInfo(&out, "{\"source_batches\":" UINT64_FORMAT
                     ",\"read_cpu_ms\":%.3f,\"decode_and_form_cpu_ms\":%.3f,\"chunk_cpu_ms\":%.3f",
                     batches, read_cpu_ms, decode_cpu_ms, chunk_cpu_ms);
    for (i = 0; i < 4; ++i)
    {
        TargetBuffer *target = &targets[i];
        appendStringInfo(&out, ",\"%s\":{\"rows\":" UINT64_FORMAT
                         ",\"not_null_rows\":" UINT64_FORMAT ",\"not_null_failures\":" UINT64_FORMAT
                         ",\"core_calls\":" UINT64_FORMAT ",\"flushes\":" UINT64_FORMAT
                         ",\"insert_cpu_ms\":%.3f,\"check\":",
                         target_specs[i].name, (uint64) target->rows, target->not_null_rows,
                         target->not_null_failures, target->constraint_calls, target->flushes,
                         target->insert_cpu_ms);
        pgdm_check_memo_json(&out, target->check_memo);
        appendStringInfoString(&out, ",\"formation\":");
        pgdm_tuple_template_json(&out, target->tuple_templates, target->rows);
        appendStringInfoChar(&out, '}');
    }
    appendStringInfoChar(&out, '}');
    return out.data;
}

Datum
pgdm_bulk_chunk(PG_FUNCTION_ARGS)
{
    TimestampTz started = GetCurrentTimestamp(), phase_started;
    struct rusage chunk_cpu_started, cpu_before, cpu_after;
    bool chunk_cpu_ok = getrusage(RUSAGE_SELF, &chunk_cpu_started) == 0;
    double source_cpu_ms = 0, decode_cpu_ms = 0;
    uint64 source_batches = 0;
    MemoryContext outer_context = CurrentMemoryContext;
    MemoryContext context, row_context;
    char *schema = text_to_cstring(PG_GETARG_TEXT_PP(0));
    Oid namespace_oid;
    AclResult acl;
    TupleDesc result_desc;
    TargetBuffer *targets;
    SourceReader source;
    ManifestMap manifest;
    DecodeContext decoder;
    DimensionMap dimensions;
    HTAB *codes;
    EState *estate;
    CommandId cid;
    Oid argtypes[3] = {INT8OID, INT8OID, TEXTARRAYOID};
    Datum args[3] = {PG_GETARG_DATUM(1), PG_GETARG_DATUM(2), PG_GETARG_DATUM(3)};
    SPIPlanPtr plan = NULL;
    Portal portal = NULL;
    bool by_tid = PG_GETARG_BOOL(4);
    int64 source_rows = 0;
    Size code_cache_bytes = 0;
    double decode_ms = 0, insert_ms = 0;
    double setup_ms, source_read_ms = 0, finalize_ms = 0, final_insert_before;
    Datum output[13];
    bool output_nulls[13] = {false};
    Datum *code_values;
    HASH_SEQ_STATUS scan;
    CodeEntry *code;
    int code_count = 0, i;
    HeapTuple result;
    StringInfoData written_pages;

    if (XactReadOnly) ereport(ERROR, (errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION), errmsg("cannot execute pgdm_bulk_chunk in a read-only transaction")));
    if (IsInParallelMode()) ereport(ERROR, (errcode(ERRCODE_INVALID_TRANSACTION_STATE), errmsg("cannot execute pgdm_bulk_chunk during parallel operation")));
    if (get_call_result_type(fcinfo, NULL, &result_desc) != TYPEFUNC_COMPOSITE) elog(ERROR, "composite result required");
    if (result_desc->natts != 13)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("pgdm_bulk build .7 requires extension version 1.1")));
    namespace_oid = get_namespace_oid(schema, false);
    acl = object_aclcheck(NamespaceRelationId, namespace_oid, GetUserId(), ACL_USAGE);
    if (acl != ACLCHECK_OK) aclcheck_error(acl, OBJECT_SCHEMA, schema);
    context = AllocSetContextCreate(outer_context, "pgdm bulk chunk", ALLOCSET_DEFAULT_SIZES);
    MemoryContextSwitchTo(context);
    row_context = AllocSetContextCreate(context, "pgdm bulk row", ALLOCSET_DEFAULT_SIZES);
    open_source(&source, PG_GETARG_ARRAYTYPE_P(3), by_tid);
    manifest = create_manifest(&source, context);
    targets = palloc0(sizeof(TargetBuffer) * 4);
    estate = CreateExecutorState();
    cid = GetCurrentCommandId(true);
    init_context(&decoder, PG_GET_COLLATION());
    source.decoder = manifest.decoder = &decoder;
    codes = create_code_set(context);
    for (i = 0; i < 4; ++i) open_target(&targets[i], namespace_oid, &target_specs[i], &source);

    if (SPI_connect() != SPI_OK_CONNECT) elog(ERROR, "could not connect SPI");
    dimensions = load_dimensions(schema, context);
    if (by_tid)
    {
        MemoryContext old_context = MemoryContextSwitchTo(context);
        begin_source_range(&source, PG_GETARG_INT64(1), PG_GETARG_INT64(2));
        MemoryContextSwitchTo(old_context);
    }
    else
    {
        /* Preserve the planner/index path for selective raw_id chunks. */
        plan = SPI_prepare(
            "SELECT raw_id, raw, raw_schema, raw_hash FROM public.raw_data "
            "WHERE raw_id BETWEEN $1 AND $2 AND raw_schema = ANY($3)",
            3, argtypes);
        if (!plan) elog(ERROR, "could not prepare raw_data scan");
        portal = SPI_cursor_open(NULL, plan, args, NULL, true);
    }
    setup_ms = elapsed_ms(started);
    for (;;)
    {
        uint64 j, fetched;
        TimestampTz batch_started;
        double insert_before;
        double insert_cpu_before = 0, insert_cpu_after = 0;
        bool batch_cpu_ok = getrusage(RUSAGE_SELF, &cpu_before) == 0;
        phase_started = GetCurrentTimestamp();
        if (by_tid) fetched = fetch_source_batch(&source, row_context);
        else
        {
            SPI_cursor_fetch(portal, true, BULK_TUPLES);
            fetched = SPI_processed;
        }
        source_read_ms += elapsed_ms(phase_started);
        if (getrusage(RUSAGE_SELF, &cpu_after) == 0 && batch_cpu_ok)
            source_cpu_ms += pgdm_cpu_ms(&cpu_after) - pgdm_cpu_ms(&cpu_before);
        if (!fetched) break;
        source_batches++;
        batch_cpu_ok = getrusage(RUSAGE_SELF, &cpu_before) == 0;
        for (i = 0; i < 4; ++i) insert_cpu_before += targets[i].insert_cpu_ms;
        batch_started = GetCurrentTimestamp();
        insert_before = insert_ms;
        for (j = 0; j < fetched; ++j)
        {
            bool isnull, raw_null, schema_null, hash_null, have_dimension, have_code = false;
            int64 dimension_id = 0;
            int16 code_id = 0;
            Datum id, raw, raw_schema, raw_hash;
            DecodedRow row;
            MemoryContext old_context;
            if (by_tid)
            {
                TupleTableSlot *slot = source.slots[j];
                id = slot_getattr(slot, source.id_att, &isnull);
                if (isnull) elog(ERROR, "raw_data.raw_id must not be null");
                raw = slot_getattr(slot, source.raw_att, &raw_null);
                raw_schema = slot_getattr(slot, source.schema_att, &schema_null);
                raw_hash = slot_getattr(slot, source.hash_att, &hash_null);
            }
            else
            {
                HeapTuple tuple = SPI_tuptable->vals[j];
                TupleDesc desc = SPI_tuptable->tupdesc;
                id = SPI_getbinval(tuple, desc, 1, &isnull);
                if (isnull) elog(ERROR, "raw_data.raw_id must not be null");
                raw = SPI_getbinval(tuple, desc, 2, &raw_null);
                raw_schema = SPI_getbinval(tuple, desc, 3, &schema_null);
                raw_hash = SPI_getbinval(tuple, desc, 4, &hash_null);
            }
            /* Capture from the source tuple before decoding or projection. */
            targets[0].raw_fk.source_raw_id = DatumGetInt64(id);
            old_context = MemoryContextSwitchTo(row_context);
            observe_manifest(&manifest, raw_schema, schema_null, raw_hash, hash_null);
            ++source_rows;
            /* SQL NULL raw has no fields/parent/code, but remains a source row. */
            if (!raw_null)
            {
                decode_bulk(DatumGetInt64(id), DatumGetJsonbP(raw), &decoder, &row);
                if (!row.nulls[9])
                    have_code = observe_code(codes, row.values[9], &decoder, context, &code_cache_bytes, &code_id);
                have_dimension = lookup_dimension(&row, &dimensions, &dimension_id, &decoder);
                if (!have_dimension && valid_missing_parent(&row, &decoder))
                    ereport(ERROR, (errmsg("S2_DIMENSION_MISS: full dimensions and deterministic reprocessing required")));
                if (have_dimension)
                    emit_parent(targets, &row, dimension_id, have_code, code_id, estate, cid, &decoder, &insert_ms);
            }
            MemoryContextSwitchTo(old_context);
            MemoryContextReset(row_context);
            /* Output heaps and manifest groups already own all retained bytes. */
            if (by_tid) ExecClearTuple(source.slots[j]);
            CHECK_FOR_INTERRUPTS();
        }
        decode_ms += elapsed_ms(batch_started) - (insert_ms - insert_before);
        for (i = 0; i < 4; ++i) insert_cpu_after += targets[i].insert_cpu_ms;
        if (getrusage(RUSAGE_SELF, &cpu_after) == 0 && batch_cpu_ok)
            decode_cpu_ms += pgdm_cpu_ms(&cpu_after) - pgdm_cpu_ms(&cpu_before) -
                             (insert_cpu_after - insert_cpu_before);
        if (!by_tid)
        {
            phase_started = GetCurrentTimestamp();
            SPI_freetuptable(SPI_tuptable);
            finalize_ms += elapsed_ms(phase_started);
        }
    }
    phase_started = GetCurrentTimestamp();
    final_insert_before = insert_ms;
    if (!by_tid)
    {
        SPI_cursor_close(portal);
        SPI_freeplan(plan);
    }
    close_source(&source);
    SetConfigOption("pgdm_bulk.last_source_io",
                    psprintf("{\"window_blocks\":%d,\"advice_calls\":" UINT64_FORMAT
                             ",\"advice_blocks\":" UINT64_FORMAT
                             ",\"server_version_num\":%d}",
                             source.prefetch_blocks, source.prefetch_calls,
                             source.prefetch_pages, PG_VERSION_NUM),
                    PGC_USERSET, PGC_S_SESSION);
    for (i = 0; i < 4; ++i) flush_target(&targets[i], estate, cid, &insert_ms);
    if (SPI_finish() != SPI_OK_FINISH) elog(ERROR, "could not finish SPI");
    output[0] = Int64GetDatum(source_rows);
    for (i = 0; i < 4; ++i) output[i + 1] = Int64GetDatum(targets[i].rows);
    initStringInfo(&written_pages);
    appendStringInfoChar(&written_pages, '{');
    for (i = 0; i < 4; ++i)
    {
        int page;
        if (i) appendStringInfoChar(&written_pages, ',');
        appendStringInfo(&written_pages, "\"%s\":[", target_specs[i].name);
        for (page = 0; page < targets[i].page_count; ++page)
        {
            if (page) appendStringInfoChar(&written_pages, ',');
            appendStringInfo(&written_pages, "%u", targets[i].written_pages[page]);
        }
        appendStringInfoChar(&written_pages, ']');
    }
    appendStringInfoChar(&written_pages, '}');
    SetConfigOption("pgdm_bulk.last_written_pages", written_pages.data, PGC_USERSET, PGC_S_SESSION);
    for (i = 0; i < 4; ++i) close_target(&targets[i]);
    FreeExecutorState(estate);
    CommandCounterIncrement();

    MemoryContextSwitchTo(outer_context);
    code_values = palloc(sizeof(Datum) * Max(1, hash_get_num_entries(codes)));
    hash_seq_init(&scan, codes);
    while ((code = hash_seq_search(&scan)) != NULL)
        if (code->valid) code_values[code_count++] = code->key.value;
    output[5] = PointerGetDatum(code_count ? construct_array(code_values, code_count, TEXTOID, -1, false, TYPALIGN_INT) : construct_empty_array(TEXTOID));
    pfree(code_values);
    output[9] = JsonbPGetDatum(manifest_json(&manifest));
    decoder.hotpath_json = hotpath_receipt(targets, source_cpu_ms, decode_cpu_ms,
        getrusage(RUSAGE_SELF, &cpu_after) == 0 && chunk_cpu_ok ?
        pgdm_cpu_ms(&cpu_after) - pgdm_cpu_ms(&chunk_cpu_started) : -1, source_batches);
    output[9] = pgdm_raw_fk_manifest(&targets[0], output[9]);
    decode_receipt(&decoder);
    MemoryContextDelete(context);
    finalize_ms += elapsed_ms(phase_started) - (insert_ms - final_insert_before);
    output[6] = Float8GetDatum(decode_ms);
    output[7] = Float8GetDatum(insert_ms);
    output[8] = Float8GetDatum(elapsed_ms(started));
    output[10] = Float8GetDatum(setup_ms);
    output[11] = Float8GetDatum(source_read_ms);
    output[12] = Float8GetDatum(finalize_ms);
    result = heap_form_tuple(result_desc, output, output_nulls);
    PG_RETURN_DATUM(HeapTupleGetDatum(result));
}

Datum
pgdm_bulk_build_id(PG_FUNCTION_ARGS)
{
    PG_RETURN_TEXT_P(cstring_to_text(PGDM_BULK_BUILD_ID));
}

static int
compare_blocks(const void *left, const void *right)
{
    BlockNumber a = *((const BlockNumber *) left);
    BlockNumber b = *((const BlockNumber *) right);
    return (a > b) - (a < b);
}

/* Read committed pages through the normal MVCC table AM. No header edits,
 * freeze flags, visibility-map writes, or uncommitted-row shortcuts. */
Datum
pgdm_bulk_hint_pages(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    ArrayType *input = PG_GETARG_ARRAYTYPE_P(1);
    Relation rel;
    Datum *values;
    bool *nulls;
    int count, i, used = 0;
    BlockNumber *blocks;
    Snapshot snapshot;
    TableScanDesc scan = NULL;
    TupleTableSlot *slot;
    int64 rows = 0;
    AclResult acl;

    if (!ActiveSnapshotSet() || !IsMVCCSnapshot(GetActiveSnapshot()))
        ereport(ERROR, (errmsg("pgdm hint reads require an active MVCC snapshot")));
    rel = table_open(relid, AccessShareLock);
    acl = object_aclcheck(NamespaceRelationId, RelationGetNamespace(rel), GetUserId(), ACL_USAGE);
    if (acl != ACLCHECK_OK)
        aclcheck_error(acl, OBJECT_SCHEMA, get_namespace_name(RelationGetNamespace(rel)));
    acl = pg_class_aclcheck(relid, GetUserId(), ACL_SELECT);
    if (acl != ACLCHECK_OK) aclcheck_error(acl, OBJECT_TABLE, RelationGetRelationName(rel));
    if (rel->rd_rel->relkind != RELKIND_RELATION ||
        rel->rd_rel->relam != HEAP_TABLE_AM_OID || rel->rd_rel->relrowsecurity)
        ereport(ERROR, (errmsg("pgdm hint reads require a plain heap without row security")));
    deconstruct_array(input, INT8OID, 8, true, TYPALIGN_DOUBLE, &values, &nulls, &count);
    blocks = palloc(sizeof(BlockNumber) * Max(count, 1));
    for (i = 0; i < count; ++i)
    {
        int64 block;
        if (nulls[i]) continue;
        block = DatumGetInt64(values[i]);
        if (block < 0 || block > MaxBlockNumber)
            ereport(ERROR, (errmsg("pgdm hint page is out of range")));
        blocks[used++] = (BlockNumber) block;
    }
    qsort(blocks, used, sizeof(BlockNumber), compare_blocks);
    snapshot = RegisterSnapshot(GetActiveSnapshot());
    slot = table_slot_create(rel, NULL);
    for (i = 0; i < used; ++i)
    {
        BlockNumber lo = blocks[i], hi = lo;
        ItemPointerData minimum, maximum;
        while (i + 1 < used && (blocks[i + 1] == hi ||
               (hi < MaxBlockNumber && blocks[i + 1] == hi + 1))) hi = blocks[++i];
        ItemPointerSet(&minimum, lo, FirstOffsetNumber);
        ItemPointerSet(&maximum, hi, MaxOffsetNumber);
        if (scan) table_rescan_tidrange(scan, &minimum, &maximum);
        else scan = table_beginscan_tidrange(rel, snapshot, &minimum, &maximum);
        while (table_scan_getnextslot_tidrange(scan, ForwardScanDirection, slot))
        {
            ++rows;
            ExecClearTuple(slot);
            CHECK_FOR_INTERRUPTS();
        }
    }
    if (scan) table_endscan(scan);
    ExecDropSingleTupleTableSlot(slot);
    UnregisterSnapshot(snapshot);
    table_close(rel, AccessShareLock);
    PG_RETURN_INT64(rows);
}

#include "pgdm_radix_twin.inc"
#include "pgdm_verified_radix_bundle.inc"
