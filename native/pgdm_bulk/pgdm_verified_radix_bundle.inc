/*
 * pgdm_verified_radix_bundle.inc
 * One real heap scan and a verified shared integer order for related indexes.
 * Include AFTER pgdm_radix_twin.inc in pgdm_bulk.c; use core visibility.
 *
 * This file reuses the existing sixteen-byte radix rows, integer sorter and
 * normal PostgreSQL Btree page writer.  It does not infer any input row from
 * a generator, change constraint validity, or share pages between indexes.
 * Every SQL definition still goes through transformIndexStmt / DefineIndex.
 * Every finished tree goes through core index_build for normal finishing.
 */
#ifndef PGDM_VERIFIED_RADIX_BUNDLE_INC
#define PGDM_VERIFIED_RADIX_BUNDLE_INC

#include "postgres.h"
#include "fmgr.h"

PG_FUNCTION_INFO_V1(pgdm_build_verified_radix_bundle);

#if PG_VERSION_NUM >= 180000 && PG_VERSION_NUM < 190000

#include "access/amapi.h"
#include "access/genam.h"
#include "access/nbtree.h"
#include "catalog/index.h"
#include "catalog/pg_type_d.h"
#include "nodes/primnodes.h"
#include "parser/parser.h"
#include "parser/parse_utilcmd.h"
#include "utils/array.h"
#include "utils/fmgroids.h"

#define PGDM_MI_MAX_INDEXES 3
#define PGDM_MI_SUFFIX_NULL ((OffsetNumber) 0x8000)

typedef enum PgdmMILayout
{
    PGDM_MI_TABLE1,
    PGDM_MI_TABLE2,
    PGDM_MI_TABLE6,
    PGDM_MI_TABLE5
} PgdmMILayout;

typedef struct PgdmMIState
{
    PgdmMILayout layout;
    Relation heap;
    Relation indexes[PGDM_MI_MAX_INDEXES];
    Oid index_oids[PGDM_MI_MAX_INDEXES];
    int nindexes;
    bool twin_written;
    PgdmFkCollector proof;
    MemoryContext proof_context;
    uint64 scanned_live_rows;
    bool scan_complete;
    AttrNumber primary_att;
    AttrNumber equal_att;
    AttrNumber extra_att;
    MemoryContext data_context;
    PgdmRTScan main;
    PgdmRTScan secondary;
    PgdmRTRow *scratch;
    PgdmRTRow *main_sorted;
    PgdmRTRow *secondary_sorted;
    const char *fallback_reason;
    double heap_tuples;
    double scan_ms;
    double sort_ms;
    int main_passes;
    int secondary_passes;
    uint64 index_rows[PGDM_MI_MAX_INDEXES];
    PgdmRTIOStats io[PGDM_MI_MAX_INDEXES];
    PgdmRTLeafStats leaf[PGDM_MI_MAX_INDEXES];
    double write_ms[PGDM_MI_MAX_INDEXES];
    double build_ms[PGDM_MI_MAX_INDEXES];
} PgdmMIState;

typedef struct PgdmMIBuildScope
{
    PgdmMIState *state;
    int ordinal;
    PgdmRTRow *rows;
    uint64 count;
    uint64 emitted;
    bool filter_null_branch;
    bool composite;
    const IndexAmRoutine *core_am;
    IndexAmRoutine scoped_am;
    struct PgdmMIBuildScope *previous;
} PgdmMIBuildScope;

static PgdmMIBuildScope *pgdm_mi_build_scope = NULL;

static AttrNumber
pgdm_mi_attribute(Relation heap, const char *name, Oid type)
{
    AttrNumber attnum = get_attnum(RelationGetRelid(heap), name);
    Form_pg_attribute attr;

    if (attnum <= 0)
        return InvalidAttrNumber;
    attr = TupleDescAttr(RelationGetDescr(heap), attnum - 1);
    if (attr->attisdropped || attr->atttypid != type)
        return InvalidAttrNumber;
    return attnum;
}

/* Inspect the stored predicate, not the SQL spelling or estimated selectivity. */
static bool
pgdm_mi_null_predicate(List *predicate, AttrNumber attnum, bool want_null)
{
    NullTest *test;
    Var *var;

    if (list_length(predicate) != 1 || !IsA(linitial(predicate), NullTest))
        return false;
    test = (NullTest *) linitial(predicate);
    if (test->argisrow || test->nulltesttype != (want_null ? IS_NULL : IS_NOT_NULL) ||
        !IsA(test->arg, Var))
        return false;
    var = (Var *) test->arg;
    return var->varno == 1 && var->varlevelsup == 0 &&
           var->varattno == attnum && var->vartype == INT8OID &&
           var->vartypmod == -1 && var->varcollid == InvalidOid &&
           var->varnullingrels == NULL;
}

/* All fast orders are the actual pg_catalog integer Btree order, ASC NULLS LAST. */
static bool
pgdm_mi_index_shape(Relation index, int nkeys, const AttrNumber *atts,
                    bool composite, AttrNumber predicate_att, int predicate_kind)
{
    IndexInfo *info = BuildIndexInfo(index);
    int i;

    if (index->rd_rel->relam != BTREE_AM_OID || !info->ii_Unique ||
        info->ii_NumIndexAttrs != nkeys || info->ii_NumIndexKeyAttrs != nkeys ||
        RelationGetDescr(index)->natts != nkeys ||
        info->ii_Expressions != NIL || info->ii_ExclusionOps != NULL)
        return false;
    if (predicate_kind == 0)
    {
        if (info->ii_Predicate != NIL)
            return false;
    }
    else if (!pgdm_mi_null_predicate(info->ii_Predicate, predicate_att,
                                    predicate_kind == 1))
        return false;

    for (i = 0; i < nkeys; i++)
    {
        Oid type = composite && i == 1 ? INT2OID : INT8OID;
        const char *opname = type == INT2OID ? "int2_ops" : "int8_ops";
        Oid opclass = get_opclass_oid(BTREE_AM_OID,
            list_make2(makeString("pg_catalog"), makeString(pstrdup(opname))), false);
        Oid cmp = type == INT2OID ? F_BTINT2CMP : F_BTINT8CMP;

        if (info->ii_IndexAttrNumbers[i] != atts[i] ||
            TupleDescAttr(RelationGetDescr(index), i)->atttypid != type ||
            index->rd_opcintype[i] != type ||
            index->rd_opfamily[i] != get_opclass_family(opclass) ||
            index->rd_indcollation[i] != InvalidOid ||
            index->rd_indoption[i] != 0 ||
            index_getprocid(index, i + 1, BTORDER_PROC) != cmp)
            return false;
    }
    return true;
}

static bool
pgdm_mi_catalog_matches(PgdmMIState *state)
{
    AttrNumber atts[2];

    state->primary_att = pgdm_mi_attribute(state->heap, "id_column_1", INT8OID);
    state->equal_att = pgdm_mi_attribute(state->heap,
        state->layout == PGDM_MI_TABLE1 ? "id_column_3" : "id_column_2", INT8OID);
    state->extra_att = InvalidAttrNumber;
    if (state->layout == PGDM_MI_TABLE1)
        state->extra_att = pgdm_mi_attribute(state->heap, "id_column_4", INT8OID);
    else if (state->layout == PGDM_MI_TABLE2)
        state->extra_att = pgdm_mi_attribute(state->heap, "integer_column_1", INT2OID);
    else
        state->extra_att = pgdm_mi_attribute(state->heap, "id_column_3",
            state->layout == PGDM_MI_TABLE6 ? INT2OID : INT8OID);
    if (state->primary_att <= 0 || state->equal_att <= 0 ||
        state->extra_att <= 0)
        return false;

    atts[0] = state->primary_att;
    if (!pgdm_mi_index_shape(state->indexes[0], 1, atts, false, 0, 0))
        return false;
    atts[0] = state->equal_att;
    if (state->layout == PGDM_MI_TABLE1)
        return pgdm_mi_index_shape(state->indexes[1], 1, atts, false, 0, 0);
    if (state->layout == PGDM_MI_TABLE6 || state->layout == PGDM_MI_TABLE2)
    {
        atts[1] = state->extra_att;
        return pgdm_mi_index_shape(state->indexes[1], 2, atts, true, 0, 0);
    }
    if (!pgdm_mi_index_shape(state->indexes[1], 1, atts, false, state->extra_att, 1))
        return false;
    atts[0] = state->extra_att;
    return pgdm_mi_index_shape(state->indexes[2], 1, atts, false, state->extra_att, 2);
}

static inline PgdmRTRow *
pgdm_mi_push(PgdmRTScan *scan, int64 id, ItemPointer tid)
{
    PgdmRTRow *row = &scan->rows[scan->count];
    uint64 key = ((uint64) id) ^ PGDM_RT_SIGN_BIT;

    row->id = id;
    ItemPointerCopy(tid, &row->tid);
    row->padding = 0;
    if (scan->count == 0)
        scan->first_key = key;
    else
        scan->varying_bits |= key ^ scan->first_key;
    scan->count++;
    return row;
}

/* Callback receives actual attributes and the core visibility/root TID result. */
static void
pgdm_mi_collect(Relation index, ItemPointer tid, Datum *values, bool *isnull,
                bool tuple_is_alive, void *opaque)
{
    PgdmMIState *state = (PgdmMIState *) opaque;
    int primary_col = 2;
    int equal_col = primary_col - 1;
    bool needs_equal_key = state->layout != PGDM_MI_TABLE5 || isnull[0];
    PgdmRTRow *row;
    int64 id;

    (void) index;
    if (tuple_is_alive)
        state->scanned_live_rows++;
    if(state->proof.nsets)
    {
        int column=state->layout==PGDM_MI_TABLE1 ? 2 : 1;
        pgdm_fk_set_observe(&state->proof.sets[0],values[column],isnull[column],tuple_is_alive);
        if(state->proof.nsets==2)
            pgdm_fk_set_observe(&state->proof.sets[1],values[0],isnull[0],tuple_is_alive);
    }
    if (state->fallback_reason != NULL)
        return;
    if (!tuple_is_alive)
    {
        state->fallback_reason = "nonlive_index_tuple";
        return;
    }
    if (isnull[primary_col] || (needs_equal_key && isnull[equal_col]))
    {
        state->fallback_reason = "null_shared_key";
        return;
    }
    id = DatumGetInt64(values[primary_col]);
    if (state->layout == PGDM_MI_TABLE2)
    {
        int64 product, expected;
        int64 raw = isnull[1] ? 0 : DatumGetInt64(values[1]);
        int16 position = isnull[0] ? 0 : DatumGetInt16(values[0]);
        if (isnull[0] || isnull[1] || raw <= 0 || position < 1 || position > 14 ||
            pg_sub_s64_overflow(raw, 1, &product) ||
            pg_mul_s64_overflow(product, 14, &product) ||
            pg_add_s64_overflow(product, position, &expected) || id != expected)
        {
            state->fallback_reason = "actual_table2_mapping_failed";
            return;
        }
    }
    else if (needs_equal_key && id != DatumGetInt64(values[equal_col]))
    {
        state->fallback_reason = "actual_key_equality_failed";
        return;
    }
    if (state->main.count == state->main.capacity)
    {
        state->fallback_reason = "actual_rows_exceed_capacity";
        return;
    }
    if (!ItemPointerIsValid(tid) ||
        (state->layout == PGDM_MI_TABLE6 && (tid->ip_posid & PGDM_MI_SUFFIX_NULL) != 0))
    {
        state->fallback_reason = "unsupported_actual_tid";
        return;
    }
    row = pgdm_mi_push(&state->main, id, tid);
    if (state->layout == PGDM_MI_TABLE6)
    {
        /*
         * All 65536 int2 bit patterns fit in padding.  The separately verified
         * unused high offset bit carries NULL only inside the compact array.
         * Every writer restores the exact original TID before using it.
         */
        if (isnull[0])
            row->tid.ip_posid |= PGDM_MI_SUFFIX_NULL;
        else
            row->padding = (uint16) DatumGetInt16(values[0]);
    }
    else if (state->layout == PGDM_MI_TABLE5 && !isnull[0])
    {
        row->padding = 1;
        if (state->secondary.rows == NULL)
            state->secondary.rows = MemoryContextAllocHuge(state->data_context,
                (Size) (state->secondary.capacity * sizeof(PgdmRTRow)));
        (void) pgdm_mi_push(&state->secondary, DatumGetInt64(values[0]), tid);
    }
}

static bool
pgdm_mi_strictly_increasing(PgdmRTRow *rows, uint64 count)
{
    uint64 i;

    for (i = 1; i < count; i++)
    {
        if (rows[i - 1].id >= rows[i].id)
            return false;
        if ((i & 65535) == 0)
            CHECK_FOR_INTERRUPTS();
    }
    return true;
}

static void
pgdm_mi_scan_and_sort(PgdmMIState *state)
{
    IndexInfo *scan_info = BuildIndexInfo(state->indexes[0]);
    Oid save_userid;
    int save_sec_context;
    int save_nestlevel;
    instr_time started;
    instr_time scanned;
    instr_time sorted;

    scan_info->ii_NumIndexAttrs = 3;
    scan_info->ii_NumIndexKeyAttrs = 3;
    /* Include every required FK attribute in this same real heap scan. */
    scan_info->ii_IndexAttrNumbers[0] = state->extra_att;
    scan_info->ii_IndexAttrNumbers[1] = state->equal_att;
    scan_info->ii_IndexAttrNumbers[2] = state->primary_att;
    scan_info->ii_ParallelWorkers = 0;
    GetUserIdAndSecContext(&save_userid, &save_sec_context);
    save_nestlevel = NewGUCNestLevel();
    SetUserIdAndSecContext(state->heap->rd_rel->relowner,
                           save_sec_context | SECURITY_RESTRICTED_OPERATION);
    PG_TRY();
    {
        RestrictSearchPath();
        pgstat_progress_start_command(PROGRESS_COMMAND_CREATE_INDEX,
                                      RelationGetRelid(state->heap));
        pgstat_progress_update_param(PROGRESS_CREATEIDX_COMMAND,
                                     PROGRESS_CREATEIDX_COMMAND_CREATE);
        pgstat_progress_update_param(PROGRESS_CREATEIDX_INDEX_OID, state->index_oids[0]);
        pgstat_progress_update_param(PROGRESS_CREATEIDX_ACCESS_METHOD_OID, BTREE_AM_OID);
        pgstat_progress_update_param(PROGRESS_CREATEIDX_PHASE, PROGRESS_CREATEIDX_PHASE_BUILD);
        pgstat_progress_update_param(PROGRESS_CREATEIDX_SUBPHASE,
                                     PROGRESS_BTREE_PHASE_INDEXBUILD_TABLESCAN);
        INSTR_TIME_SET_CURRENT(started);
        state->heap_tuples = table_index_build_scan(state->heap, state->indexes[0],
            scan_info, true, true, pgdm_mi_collect, state, NULL);
        state->scan_complete = !scan_info->ii_BrokenHotChain &&
            state->heap_tuples == (double) state->scanned_live_rows;
        INSTR_TIME_SET_CURRENT(scanned);
        state->scan_ms = pgdm_rt_ms(started, scanned);
        if (scan_info->ii_BrokenHotChain)
            state->fallback_reason = "broken_hot_chain";
        if (state->fallback_reason == NULL &&
            state->heap_tuples != (double) state->main.count)
            state->fallback_reason = "heap_live_count_mismatch";

        if (state->fallback_reason == NULL)
        {
            pgstat_progress_update_param(PROGRESS_CREATEIDX_SUBPHASE,
                                         PROGRESS_BTREE_PHASE_PERFORMSORT_1);
            state->main_sorted = pgdm_rt_sort(&state->main, state->scratch,
                                              &state->main_passes);
            if (!pgdm_mi_strictly_increasing(state->main_sorted, state->main.count))
                state->fallback_reason = "duplicate_or_unordered_primary";
            if (state->fallback_reason == NULL && state->secondary.count > 0)
            {
                /* The buffer not holding the sorted primary is free scratch. */
                PgdmRTRow *spare = state->main_sorted == state->main.rows ?
                                  state->scratch : state->main.rows;

                state->secondary_sorted = pgdm_rt_sort(&state->secondary, spare,
                                                        &state->secondary_passes);
                if (!pgdm_mi_strictly_increasing(state->secondary_sorted, state->secondary.count))
                    state->fallback_reason = "duplicate_or_unordered_partial_key";
            }
        }
        INSTR_TIME_SET_CURRENT(sorted);
        state->sort_ms = pgdm_rt_ms(scanned, sorted);
    }
    PG_FINALLY();
    {
        pgstat_progress_end_command();
        AtEOXact_GUC(false, save_nestlevel);
        SetUserIdAndSecContext(save_userid, save_sec_context);
    }
    PG_END_TRY();
}

static inline IndexTuple
pgdm_mi_fill_tuple(PgdmMIBuildScope *scope, const PgdmRTRow *row,
                   IndexTuple full, IndexTuple null_suffix,
                   char *full_data, char *null_data)
{
    bool packed_tid = scope->state->layout == PGDM_MI_TABLE6;
    bool suffix_is_null = packed_tid && (row->tid.ip_posid & PGDM_MI_SUFFIX_NULL) != 0;
    IndexTuple tuple = scope->composite && suffix_is_null ? null_suffix : full;
    char *data = scope->composite && suffix_is_null ? null_data : full_data;

    ItemPointerCopy(&row->tid, &tuple->t_tid);
    if (packed_tid)
        tuple->t_tid.ip_posid &= (OffsetNumber) ~PGDM_MI_SUFFIX_NULL;
    memcpy(data, &row->id, sizeof(int64));
    if (scope->composite && !suffix_is_null)
        memcpy(data + sizeof(int64), &row->padding, sizeof(int16));
    return tuple;
}

/*
 * Select a literal copy size only after inspecting the actual core templates.
 * All other physical layouts keep the original runtime-size batch path.
 */
static Size
pgdm_mi_fixed_tuple_bytes(PgdmMIBuildScope *scope, Relation index,
                         IndexTuple full, IndexTuple null_suffix)
{
    TupleDesc descriptor = RelationGetDescr(index);
    Size full_offset = IndexInfoFindDataOffset(full->t_info);

    if (IndexTupleHasNulls(full) || IndexTupleHasVarwidths(full) ||
        descriptor->natts != (scope->composite ? 2 : 1) ||
        TupleDescAttr(descriptor, 0)->atttypid != INT8OID ||
        TupleDescAttr(descriptor, 0)->attlen != (int16) sizeof(int64))
        return 0;
    if (!scope->composite)
        return IndexTupleSize(full) == 16 &&
               full_offset + sizeof(int64) == 16 ? 16 : 0;
    if (TupleDescAttr(descriptor, 1)->atttypid != INT2OID ||
        TupleDescAttr(descriptor, 1)->attlen != (int16) sizeof(int16) ||
        IndexTupleSize(full) != 24 || null_suffix == NULL ||
        IndexTupleSize(null_suffix) != 24 ||
        !IndexTupleHasNulls(null_suffix) || IndexTupleHasVarwidths(null_suffix) ||
        MAXALIGN(full_offset + sizeof(int64) + sizeof(int16)) != 24 ||
        IndexInfoFindDataOffset(null_suffix->t_info) + sizeof(int64) != 24)
        return 0;
    return 24;
}

/*
 * Keep one source body for the existing batch algorithm.  Force inlining so
 * the two verified call sites expose memcpy(..., 16) / memcpy(..., 24) to the
 * compiler.  The size dispatch is per page run, never per emitted tuple.
 */
static pg_always_inline uint32
pgdm_mi_append_leaf_batch(PgdmMIBuildScope *scope, PgdmRTPageState *leaf,
                          uint64 *cursor, uint32 run,
                          IndexTuple full, IndexTuple null_suffix,
                          char *full_data, char *null_data, Size tuple_bytes)
{
    PageHeader header = (PageHeader) leaf->btps_page;
    int upper = header->pd_upper;
    OffsetNumber off = leaf->btps_lastoff;
    uint64 i = *cursor;
    uint32 written = 0;

    while (i < scope->count && written < run)
    {
        const PgdmRTRow *row = &scope->rows[i++];
        IndexTuple tuple;

        if (scope->filter_null_branch && row->padding != 0)
        {
            if ((i & 65535) == 0)
                CHECK_FOR_INTERRUPTS();
            continue;
        }
        tuple = pgdm_mi_fill_tuple(scope, row, full, null_suffix,
                                    full_data, null_data);
        upper -= tuple_bytes;
        off = OffsetNumberNext(off);
        ItemIdSetNormal(&header->pd_linp[off - 1], upper, tuple_bytes);
        memcpy((char *) header + upper, tuple, tuple_bytes);
        written++;
    }
    header->pd_lower += written * sizeof(ItemIdData);
    header->pd_upper = (LocationIndex) upper;
    leaf->btps_lastoff = off;
    leaf->btps_lastextra = 0;
    *cursor = i;
    return written;
}

/* Real page batches; first items, page boundaries, pivots and WAL stay core. */
static void
pgdm_mi_write_one(PgdmMIBuildScope *scope, Relation heap, Relation index)
{
    PgdmRTWriteState writer;
    PgdmRTPageState *leaf = NULL;
    PgdmRTLeafStats *leaf_stats = &scope->state->leaf[scope->ordinal];
    Datum values[2] = {Int64GetDatum(0), Int16GetDatum(0)};
    bool nulls[2] = {false, false};
    IndexTuple full;
    IndexTuple null_suffix = NULL;
    char *full_data;
    char *null_data = NULL;
    Size tuple_bytes;
    Size fixed_tuple_bytes;
    uint64 i = 0;
    uint64 next_report = 0;
    uint64 expected = scope->state->index_rows[scope->ordinal];

    full = index_form_tuple(RelationGetDescr(index), values, nulls);
    full_data = (char *) full + IndexInfoFindDataOffset(full->t_info);
    tuple_bytes = IndexTupleSize(full);
    if (scope->composite)
    {
        nulls[1] = true;
        null_suffix = index_form_tuple(RelationGetDescr(index), values, nulls);
        null_data = (char *) null_suffix + IndexInfoFindDataOffset(null_suffix->t_info);
        if (IndexTupleSize(null_suffix) != tuple_bytes)
            elog(ERROR, "pgdm integer index tuple sizes differ across NULL shapes");
    }
    if (tuple_bytes != MAXALIGN(tuple_bytes))
        elog(ERROR, "pgdm integer index tuple is not MAXALIGNed");
    fixed_tuple_bytes = pgdm_mi_fixed_tuple_bytes(scope, index, full, null_suffix);
    pgdm_rt_writer_init(&writer, heap, index);
    if (expected > 0)
        leaf = pgdm_rt_pagestate(&writer, 0);
    /* expected=0 is learned from the completed actual heap scan. */
    while (scope->emitted < expected)
    {
        uint32 run;

        CHECK_FOR_INTERRUPTS();
        while (i < scope->count && scope->filter_null_branch && scope->rows[i].padding != 0)
        {
            i++;
            if ((i & 65535) == 0)
                CHECK_FOR_INTERRUPTS();
        }
        if (i == scope->count)
            elog(ERROR, "pgdm verified index branch count changed");
        run = pgdm_rt_leaf_run(leaf, tuple_bytes);
        if (run == 0)
        {
            IndexTuple tuple = pgdm_mi_fill_tuple(scope, &scope->rows[i++],
                full, null_suffix, full_data, null_data);

            pgdm_rt_buildadd(&writer, leaf, tuple, 0);
            leaf_stats->core_rows++;
            scope->emitted++;
        }
        else
        {
            uint32 written;

            if (fixed_tuple_bytes == 16)
                written = pgdm_mi_append_leaf_batch(scope, leaf, &i, run,
                    full, null_suffix, full_data, null_data, 16);
            else if (fixed_tuple_bytes == 24)
                written = pgdm_mi_append_leaf_batch(scope, leaf, &i, run,
                    full, null_suffix, full_data, null_data, 24);
            else
                written = pgdm_mi_append_leaf_batch(scope, leaf, &i, run,
                    full, null_suffix, full_data, null_data, tuple_bytes);
            leaf_stats->batched_rows += written;
            leaf_stats->batches++;
            scope->emitted += written;
        }
        if (scope->emitted >= next_report)
        {
            pgstat_progress_update_param(PROGRESS_CREATEIDX_TUPLES_DONE, scope->emitted);
            next_report = scope->emitted + 8192;
        }
    }
    if (scope->emitted != expected)
        elog(ERROR, "pgdm verified index emitted an unexpected row count");
    pgdm_rt_writer_finish(&writer, leaf);
    pgstat_progress_update_param(PROGRESS_CREATEIDX_TUPLES_DONE, scope->emitted);
    scope->state->io[scope->ordinal] = writer.io;
    pfree(full);
    if (null_suffix != NULL)
        pfree(null_suffix);
}

/*
 * Restore the real AM before writing and before index_build's catalog updates.
 * This avoids keeping a relcache-owned private pointer through the finishing
 * CCI.  Core index_build owns security/GUCs, statistics, HOT flags and CCI.
 */
static IndexBuildResult *
pgdm_mi_ambuild(Relation heap, Relation index, IndexInfo *info)
{
    PgdmMIBuildScope *scope = pgdm_mi_build_scope;
    IndexBuildResult *result;
    instr_time started;
    instr_time finished;

    (void) info;
    if (scope == NULL || scope->state->heap != heap ||
        scope->state->indexes[scope->ordinal] != index ||
        index->rd_indam != &scope->scoped_am)
        elog(ERROR, "pgdm verified radix build lost its index AM scope");
    index->rd_indam = (IndexAmRoutine *) scope->core_am;
    pgstat_progress_update_param(PROGRESS_CREATEIDX_SUBPHASE, PROGRESS_BTREE_PHASE_LEAF_LOAD);
    pgstat_progress_update_param(PROGRESS_CREATEIDX_TUPLES_TOTAL,
                                 scope->state->index_rows[scope->ordinal]);
    INSTR_TIME_SET_CURRENT(started);
    if (scope->state->layout == PGDM_MI_TABLE2)
    {
        if (!scope->state->twin_written)
        {
            BlockNumber pk_pages, uq_pages;
            PgdmRTIOStats io;
            PgdmRTLeafStats leaf;
            memset(&io, 0, sizeof(io));
            memset(&leaf, 0, sizeof(leaf));
            pgdm_rt_write_trees(heap, scope->state->indexes[0], scope->state->indexes[1],
                scope->state->main_sorted, scope->state->main.count,
                &pk_pages, &uq_pages, &leaf, &io);
            scope->state->io[0] = io;
            scope->state->leaf[0] = leaf;
            scope->state->twin_written = true;
        }
        scope->emitted = scope->state->main.count;
    }
    else
        pgdm_mi_write_one(scope, heap, index);
    INSTR_TIME_SET_CURRENT(finished);
    scope->state->write_ms[scope->ordinal] = pgdm_rt_ms(started, finished);
    result = palloc0(sizeof(*result));
    result->heap_tuples = scope->state->heap_tuples;
    result->index_tuples = (double) scope->emitted;
    return result;
}

static void
pgdm_mi_build_one(PgdmMIState *state, int ordinal, bool fast)
{
    Relation index = state->indexes[ordinal];
    IndexInfo *info = BuildIndexInfo(index);
    PgdmMIBuildScope *scope = palloc0(sizeof(*scope));
    instr_time started;
    instr_time finished;

    scope->state = state;
    scope->ordinal = ordinal;
    scope->rows = ordinal == 2 ? state->secondary_sorted : state->main_sorted;
    scope->count = ordinal == 2 ? state->secondary.count : state->main.count;
    scope->composite = state->layout == PGDM_MI_TABLE6 && ordinal == 1;
    scope->filter_null_branch = state->layout == PGDM_MI_TABLE5 && ordinal == 1;
    scope->core_am = index->rd_indam;
    scope->scoped_am = *scope->core_am;
    scope->scoped_am.ambuild = pgdm_mi_ambuild;
    scope->previous = pgdm_mi_build_scope;
    INSTR_TIME_SET_CURRENT(started);
    pgstat_progress_start_command(PROGRESS_COMMAND_CREATE_INDEX, RelationGetRelid(state->heap));
    pgstat_progress_update_param(PROGRESS_CREATEIDX_COMMAND, PROGRESS_CREATEIDX_COMMAND_CREATE);
    pgstat_progress_update_param(PROGRESS_CREATEIDX_INDEX_OID, state->index_oids[ordinal]);
    pgstat_progress_update_param(PROGRESS_CREATEIDX_ACCESS_METHOD_OID, BTREE_AM_OID);
    PG_TRY();
    {
        if (fast)
        {
            pgdm_mi_build_scope = scope;
            index->rd_indam = &scope->scoped_am;
            info->ii_ParallelWorkers = 0;
        }
        /* Unsupported data/catalog uses the original AM, sort and unique check. */
        index_build(state->heap, index, info, false, !fast);
    }
    PG_FINALLY();
    {
        /* Never overwrite a fresh relcache pointer installed by core's CCI. */
        if (index->rd_indam == &scope->scoped_am)
            index->rd_indam = (IndexAmRoutine *) scope->core_am;
        pgdm_mi_build_scope = scope->previous;
        pgstat_progress_end_command();
    }
    PG_END_TRY();
    INSTR_TIME_SET_CURRENT(finished);
    state->build_ms[ordinal] = pgdm_rt_ms(started, finished);
    pfree(scope);
}

Datum
pgdm_build_verified_radix_bundle(PG_FUNCTION_ARGS)
{
    Oid heap_oid = PG_GETARG_OID(0);
    ArrayType *definitions = PG_GETARG_ARRAYTYPE_P(1);
    char *layout = text_to_cstring(PG_GETARG_TEXT_PP(2));
    int64 capacity = PG_GETARG_INT64(3);
    int32 memory_mb = PG_GETARG_INT32(4);
    PgdmMIState *state = palloc0(sizeof(*state));
    Datum *sql_values;
    bool *sql_nulls;
    int ndefinitions;
    char *sql[PGDM_MI_MAX_INDEXES];
    IndexStmt *statements[PGDM_MI_MAX_INDEXES];
    uint64 budget;
    uint64 row_bytes;
    uint64 reserve_bytes;
    int arrays;
    int i;
    bool fast;
    instr_time started;
    instr_time defined;
    instr_time finished;
    StringInfoData result;

    INSTR_TIME_SET_CURRENT(started);
    PreventCommandIfReadOnly("pgdm_build_verified_radix_bundle");
    PreventCommandIfParallelMode("pgdm_build_verified_radix_bundle");
    if (strcmp(layout, "table1") == 0)
        state->layout = PGDM_MI_TABLE1;
    else if (strcmp(layout, "table2") == 0)
        state->layout = PGDM_MI_TABLE2;
    else if (strcmp(layout, "table6") == 0)
        state->layout = PGDM_MI_TABLE6;
    else if (strcmp(layout, "table5") == 0)
        state->layout = PGDM_MI_TABLE5;
    else
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("radix bundle layout must be table1, table2, table5, or table6")));
    state->nindexes = state->layout == PGDM_MI_TABLE5 ? 3 : 2;
    arrays = state->layout == PGDM_MI_TABLE5 ? 3 : 2;
    if (capacity < 0 || memory_mb < 9)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("capacity must be nonnegative and memory_mb at least 9")));
    if (ARR_ELEMTYPE(definitions) != TEXTOID || ARR_NDIM(definitions) != 1 ||
        ARR_DIMS(definitions)[0] != state->nindexes)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("radix bundle definitions have the wrong array shape")));
    deconstruct_array(definitions, TEXTOID, -1, false, 'i',
                      &sql_values, &sql_nulls, &ndefinitions);

    state->heap = table_open(heap_oid, ShareLock);
    if (!object_ownercheck(RelationRelationId, heap_oid, GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TABLE, RelationGetRelationName(state->heap));
    CheckTableNotInUse(state->heap, "CREATE INDEX");
    if (state->heap->rd_rel->relkind != RELKIND_RELATION ||
        state->heap->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT ||
        state->heap->rd_rel->relispartition || state->heap->rd_rel->relhassubclass ||
        state->heap->rd_rel->relrowsecurity || state->heap->rd_rel->relforcerowsecurity ||
        IsSystemRelation(state->heap) || state->heap->rd_tableam != GetHeapamTableAmRoutine())
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("radix bundle requires a permanent ordinary heap without RLS")));
    for (i = 0; i < ndefinitions; i++)
    {
        List *parsed;
        RawStmt *raw;
        IndexStmt *stmt;

        if (sql_nulls[i])
            ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                            errmsg("radix bundle SQL definition must not be NULL")));
        sql[i] = TextDatumGetCString(sql_values[i]);
        parsed = raw_parser(sql[i], RAW_PARSE_DEFAULT);
        if (list_length(parsed) != 1)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("each radix bundle definition must contain one CREATE INDEX")));
        raw = linitial_node(RawStmt, parsed);
        if (!IsA(raw->stmt, IndexStmt))
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("radix bundle accepts only CREATE INDEX")));
        stmt = (IndexStmt *) raw->stmt;
        if (!stmt->unique || stmt->concurrent || stmt->if_not_exists ||
            stmt->isconstraint || stmt->primary || stmt->excludeOpNames != NIL ||
            stmt->iswithoutoverlaps || stmt->relation == NULL ||
            stmt->accessMethod == NULL || strcmp(stmt->accessMethod, "btree") != 0)
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("radix bundle requires ordinary nonconcurrent unique Btree definitions")));
        if (RangeVarGetRelid(stmt->relation, ShareLock, false) != heap_oid)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("radix bundle SQL target differs from the supplied heap")));
        statements[i] = transformIndexStmt(heap_oid, stmt, sql[i]);
    }
    for (i = 0; i < ndefinitions; i++)
    {
        ObjectAddress address = DefineIndex(heap_oid, statements[i], InvalidOid,
            InvalidOid, InvalidOid, -1, false, true, false, true, false);

        state->index_oids[i] = address.objectId;
        /* Core index_create already did CCI, including its skip-build branch. */
        state->indexes[i] = index_open(address.objectId, AccessExclusiveLock);
        if (RelationGetNumberOfBlocks(state->indexes[i]) != 0)
            elog(ERROR, "new radix bundle index storage is not empty");
    }
    INSTR_TIME_SET_CURRENT(defined);
    budget = (uint64) memory_mb * 1024 * 1024;
    row_bytes = (uint64) capacity * sizeof(PgdmRTRow);
    reserve_bytes=PGDM_RT_WRITER_RESERVE+
        (state->layout==PGDM_MI_TABLE1 || state->layout==PGDM_MI_TABLE6 ? 2 : 1)*
        (uint64)PGDM_FK_SET_MAX_BYTES;
    if (strcmp(GetConfigOption("max_parallel_maintenance_workers", false, false), "0") != 0)
        state->fallback_reason = "parallel_maintenance_requested";
    else if (!pgdm_mi_catalog_matches(state))
        state->fallback_reason = "actual_catalog_shape";
    else if (budget<=reserve_bytes || (uint64) capacity > (budget - reserve_bytes) /
                                ((uint64) arrays * sizeof(PgdmRTRow)) ||
             (uint64) capacity > (uint64) MaxAllocHugeSize / sizeof(PgdmRTRow))
        state->fallback_reason = "compact_memory_grant";
    if (state->fallback_reason == NULL)
    {
        state->data_context = AllocSetContextCreate(CurrentMemoryContext,
            "pgdm verified multi-index rows", ALLOCSET_DEFAULT_SIZES);
        state->main.capacity = state->secondary.capacity = (uint64) capacity;
        if (row_bytes > 0)
        {
            state->main.rows = MemoryContextAllocHuge(state->data_context, (Size) row_bytes);
            state->scratch = MemoryContextAllocHuge(state->data_context, (Size) row_bytes);
        }
        {
            AttrNumber attributes[2];
            int fields=state->layout==PGDM_MI_TABLE1 || state->layout==PGDM_MI_TABLE6 ? 2 : 1;
            attributes[0]=state->layout==PGDM_MI_TABLE1 ? state->primary_att : state->equal_att;
            attributes[1]=state->extra_att;
            state->proof_context = AllocSetContextCreate(CurrentMemoryContext,
                "pgdm independent membership proof", ALLOCSET_DEFAULT_SIZES);
            pgdm_fk_collector_init(&state->proof,state->heap,attributes,fields,state->proof_context);
        }
        pgdm_mi_scan_and_sort(state);
    }
    fast = state->fallback_reason == NULL;
    if (fast)
    {
        state->index_rows[0] = state->main.count;
        state->index_rows[1] = state->main.count;
        if (state->layout == PGDM_MI_TABLE5)
        {
            state->index_rows[1] -= state->secondary.count;
            state->index_rows[2] = state->secondary.count;
        }
    }
    else if (state->data_context != NULL)
    {
        /* No index page has been written; release all compact memory first. */
        MemoryContextDelete(state->data_context);
        state->data_context = NULL;
        state->main_sorted = state->secondary_sorted = NULL;
    }
    for (i = 0; i < state->nindexes; i++)
        pgdm_mi_build_one(state, i, fast);
    pgdm_fk_collector_finish(&state->proof,state->scanned_live_rows,state->scan_complete);
    if (state->proof_context != NULL)
        MemoryContextDelete(state->proof_context);
    if (state->data_context != NULL)
        MemoryContextDelete(state->data_context);
    for (i = 0; i < state->nindexes; i++)
        index_close(state->indexes[i], NoLock);
    table_close(state->heap, NoLock);
    INSTR_TIME_SET_CURRENT(finished);

    initStringInfo(&result);
    appendStringInfo(&result,
        "{\"builder\":\"pg18_verified_radix_bundle_v1\",\"layout\":\"%s\","
        "\"route\":\"%s\",\"fallback_reason\":\"%s\",\"capacity\":" INT64_FORMAT ","
        "\"proof_scanned_live_rows\":" UINT64_FORMAT ",\"proof_scan_complete\":%s,"
        "\"collected_rows\":" UINT64_FORMAT ",\"actual_nonnull_branch_rows\":" UINT64_FORMAT ","
        "\"main_radix_passes\":%d,\"secondary_radix_passes\":%d,"
        "\"vacuum_fast\":" UINT64_FORMAT ",\"vacuum_core\":" UINT64_FORMAT ","
        "\"ddl_ms\":%.3f,\"scan_ms\":%.3f,\"sort_ms\":%.3f,\"total_ms\":%.3f,\"indexes\":[",
        layout, fast ? "verified_radix" : "original_core",
        fast ? "" : state->fallback_reason, capacity, state->scanned_live_rows,
        state->scan_complete ? "true" : "false", state->main.count, state->secondary.count,
        state->main_passes, state->secondary_passes,
        UINT64CONST(0), UINT64CONST(0),
        pgdm_rt_ms(started, defined), state->scan_ms, state->sort_ms,
        pgdm_rt_ms(started, finished));
    for (i = 0; i < state->nindexes; i++)
        appendStringInfo(&result,
            "%s{\"index_oid\":%u,\"verified_rows\":" UINT64_FORMAT ","
            "\"build_ms\":%.3f,\"write_ms\":%.3f,\"pages\":" UINT64_FORMAT ","
            "\"wal_records\":" UINT64_FORMAT ",\"leaf_batched_rows\":" UINT64_FORMAT ","
            "\"leaf_core_rows\":" UINT64_FORMAT "}",
            i == 0 ? "" : ",", state->index_oids[i], state->index_rows[i],
            state->build_ms[i], state->write_ms[i], state->io[i].pages,
            state->io[i].wal_records, state->leaf[i].batched_rows, state->leaf[i].core_rows);
    appendStringInfoString(&result, "]}");
    PG_RETURN_DATUM(DirectFunctionCall1(jsonb_in, CStringGetDatum(result.data)));
}

#else

Datum
pgdm_build_verified_radix_bundle(PG_FUNCTION_ARGS)
{
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("verified radix bundles are available only on PostgreSQL 18")));
    PG_RETURN_NULL();
}

#endif /* PostgreSQL 18 */
#endif /* PGDM_VERIFIED_RADIX_BUNDLE_INC */
