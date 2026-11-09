#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "funcapi.h"
#include "access/tupdesc.h"
#include "executor/spi.h"
#include "catalog/pg_type_d.h"
#include "catalog/pg_collation_d.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/jsonb.h"
#include "utils/memutils.h"
#include "utils/tuplestore.h"

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(pgdm_decode_row);
PG_FUNCTION_INFO_V1(pgdm_decode_chunk);
PG_FUNCTION_INFO_V1(pgdm_decode_chunk_v2);
PG_FUNCTION_INFO_V1(pgdm_decode_build_id);

#define PGDM_DECODE_BUILD_ID "s3-native-v2-20261102.3"

typedef struct DecodeContext
{
    Oid collation;
    Datum zero, thousand, zone, pattern;
    Datum slot_types[3];
} DecodeContext;

typedef struct DecodeFields
{
    JsonbValue values[64];
    bool present[64];
} DecodeFields;

typedef struct RowDecodeCache
{
    DecodeContext decode;
    TupleDesc desc;
} RowDecodeCache;

typedef struct SourceMetadata
{
    Datum schema;
    Datum hash;
    bool schema_null;
    bool hash_null;
} SourceMetadata;

static void
init_context(DecodeContext *ctx, Oid collation)
{
    ctx->collation = OidIsValid(collation) ? collation : DEFAULT_COLLATION_OID;
    ctx->zero = DirectFunctionCall1(int4_numeric, Int32GetDatum(0));
    ctx->thousand = DirectFunctionCall1(int4_numeric, Int32GetDatum(1000));
    ctx->zone = CStringGetTextDatum("America/Sao_Paulo");
    ctx->pattern = CStringGetTextDatum("^[A-Z]{2}$");
    ctx->slot_types[0] = CStringGetTextDatum("X");
    ctx->slot_types[1] = CStringGetTextDatum("Y");
    ctx->slot_types[2] = CStringGetTextDatum("Z");
}

/* Match jsonb_to_record's conversion to text, including non-string values. */
static char *
value_text(JsonbValue *value)
{
    switch (value->type)
    {
        case jbvNull: return NULL;
        case jbvString: return pnstrdup(value->val.string.val, value->val.string.len);
        case jbvBool: return pstrdup(value->val.boolean ? "true" : "false");
        case jbvNumeric: return DatumGetCString(DirectFunctionCall1(numeric_out, NumericGetDatum(value->val.numeric)));
        default: return DatumGetCString(DirectFunctionCall1(jsonb_out, JsonbPGetDatum(JsonbValueToJsonb(value))));
    }
}

/* Only U+0020 is trimmed by SQL BTRIM(text). */
static char *
trim(char *s)
{
    char *end;
    if (!s) return NULL;
    while (*s == ' ') s++;
    end = s + strlen(s);
    while (end > s && end[-1] == ' ') --end;
    *end = '\0';
    return s;
}

/* Keep JSONB spans until a field is actually needed by the projection. */
static char *
field_text(DecodeFields *fields, int index)
{
    /* Each field is requested at most once by decode(). */
    return fields->present[index] ? trim(value_text(&fields->values[index])) : NULL;
}

static Datum
number(char *s)
{
    return DirectFunctionCall3(numeric_in, CStringGetDatum(s), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
}

static Datum
scaled(Datum n, DecodeContext *ctx)
{
    Datum product = DirectFunctionCall2(numeric_mul, n, ctx->thousand);
    Datum rounded = DirectFunctionCall2(numeric_round, product, Int32GetDatum(0));
    return DirectFunctionCall1(numeric_int4, rounded);
}

static bool
positive(Datum n, DecodeContext *ctx)
{
    return DatumGetBool(DirectFunctionCall2(numeric_gt, n, ctx->zero));
}

static Datum
upper_trim(char *s, DecodeContext *ctx)
{
    return DirectFunctionCall1Coll(upper, ctx->collation, CStringGetTextDatum(s));
}

/* Recognize exact names only; unknown keys must not alias a known column. */
static int
key_index(JsonbValue *key)
{
    static const char *prefixes[] = {"id_column_", "datetime_column_", "integer_column_", "text_column_", "numeric_column_"};
    static const int lengths[] = {10, 16, 15, 12, 15};
    static const int offsets[] = {0, 1, 2, 17, 34};
    static const int limits[] = {1, 1, 15, 17, 30};
    int p, len, n = 0, i;
    if (key->val.string.len < 2) return -1;
    switch (key->val.string.val[0])
    {
        case 'd': p = 1; break;
        case 'i':
            if (key->val.string.val[1] == 'd') p = 0;
            else if (key->val.string.val[1] == 'n') p = 2;
            else return -1;
            break;
        case 't': p = 3; break;
        case 'n': p = 4; break;
        default: return -1;
    }
    len = lengths[p];
    if (key->val.string.len <= len || memcmp(key->val.string.val, prefixes[p], len) != 0) return -1;
    if (key->val.string.val[len] == '0') return -1;
    for (i = len; i < key->val.string.len; ++i)
    {
        char c = key->val.string.val[i];
        if (c < '0' || c > '9' || n > 100) return -1;
        n = n * 10 + c - '0';
    }
    return n >= 1 && n <= limits[p] ? offsets[p] + n - 1 : -1;
}

static bool
decode_slot_type(char *type, DecodeContext *ctx, Datum *result)
{
    int i;
    /* Exact already-normalized ASCII codes need no upper/texteq calls. */
    if (type[0] && type[1] == '\0')
    {
        switch (type[0])
        {
            case 'X': *result = ctx->slot_types[0]; return true;
            case 'Y': *result = ctx->slot_types[1]; return true;
            case 'Z': *result = ctx->slot_types[2]; return true;
        }
    }
    *result = upper_trim(type, ctx);
    for (i = 0; i < 3; ++i)
        if (DatumGetBool(DirectFunctionCall2Coll(texteq, ctx->collation, *result, ctx->slot_types[i]))) return true;
    return false;
}

static HeapTuple
decode(int64 raw_id, Jsonb *raw, TupleDesc desc, DecodeContext *ctx, SourceMetadata *metadata)
{
    DecodeFields fields;
    Datum out[17] = {0}, pos[14], weights[14], types[14], gaps[14];
    bool nulls[17], gap_nulls[14];
    JsonbValue key, *values;
    char *external_id, *observed_at, *speed, *ref, *code2, *code3;
    int i, count = 0;
    int16 maximum = PG_INT16_MIN;
    bool have_max = false;

    memset(nulls, true, sizeof(nulls));
    memset(gap_nulls, true, sizeof(gap_nulls));
    memset(fields.present, false, sizeof(fields.present));
    key.type = jbvString;
    key.val.string.val = "values";
    key.val.string.len = 6;
    values = findJsonbValueFromContainer(&raw->root, JB_FOBJECT, &key);
    if (values)
    {
        JsonbIterator *it;
        JsonbValue item;
        JsonbIteratorToken token;
        if (values->type == jbvBinary && JsonContainerIsArray(values->val.binary.data))
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("cannot call populate_composite on an array")));
        if (values->type != jbvBinary || !JsonContainerIsObject(values->val.binary.data))
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("cannot call populate_composite on a scalar")));
        it = JsonbIteratorInit(values->val.binary.data);
        while ((token = JsonbIteratorNext(&it, &item, true)) != WJB_DONE)
        {
            if (token == WJB_KEY)
            {
                int idx = key_index(&item);
                JsonbIteratorNext(&it, &item, true);
                if (idx >= 0)
                {
                    fields.values[idx] = item;
                    fields.present[idx] = true;
                }
            }
        }
    }
    out[0] = Int64GetDatum(raw_id); nulls[0] = false;
    external_id = field_text(&fields, 0);
    observed_at = field_text(&fields, 1);
    if (external_id && *external_id) { out[1] = DirectFunctionCall1(int4in, CStringGetDatum(external_id)); nulls[1] = false; }
    if (observed_at && *observed_at)
    {
        Datum ts = DirectFunctionCall3(timestamptz_in, CStringGetDatum(observed_at), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
        out[2] = DirectFunctionCall2(timestamptz_zone, ctx->zone, ts); nulls[2] = false;
    }
    for (i = 0; i < 14; ++i)
    {
        char *declared = field_text(&fields, 3 + i);
        if (declared && *declared)
        {
            int16 d = DatumGetInt16(DirectFunctionCall1(int2in, CStringGetDatum(declared)));
            if (!have_max || d > maximum) maximum = d;
            have_max = true;
            if (d == i + 1)
            {
                char *weight = field_text(&fields, 36 + 2 * i);
                char *type;
                char *gap_text;
                Datum w, t;
                if (!weight || !*weight) continue;
                w = number(weight);
                if (!DatumGetBool(DirectFunctionCall2(numeric_ge, w, ctx->zero))) continue;
                type = field_text(&fields, 20 + i);
                if (!type) continue;
                if (!decode_slot_type(type, ctx, &t)) continue;
                pos[count] = Int16GetDatum(i + 1);
                weights[count] = scaled(w, ctx);
                types[count] = t;
                gaps[count] = (Datum) 0;
                gap_text = field_text(&fields, 37 + 2 * i);
                if (gap_text && *gap_text)
                {
                    Datum gap = number(gap_text);
                    if (positive(gap, ctx)) { gaps[count] = scaled(gap, ctx); gap_nulls[count] = false; }
                }
                ++count;
            }
        }
    }
    if (have_max) { out[3] = Int16GetDatum(maximum); nulls[3] = false; }
    for (i = 0; i < 2; ++i)
    {
        char *s = field_text(&fields, 35 - i);
        if (s && *s)
        {
            Datum n = number(s);
            if (positive(n, ctx)) { out[4 + i] = scaled(n, ctx); nulls[4 + i] = false; }
        }
    }
    speed = field_text(&fields, 2);
    ref = field_text(&fields, 17);
    code2 = field_text(&fields, 18);
    code3 = field_text(&fields, 19);
    if (speed && *speed) { out[6] = number(speed); nulls[6] = false; }
    if (ref) { out[7] = CStringGetTextDatum(ref); nulls[7] = false; }
    if (code2) { out[8] = upper_trim(code2, ctx); nulls[8] = false; }
    if (code3) { out[9] = upper_trim(code3, ctx); nulls[9] = false; }
    /* SQL three-valued AND: false dominates NULL. */
    {
        bool ref_false = ref && !*ref;
        bool code_false = !nulls[8] && !DatumGetBool(DirectFunctionCall2Coll(textregexeq, ctx->collation, out[8], ctx->pattern));
        if (ref_false || code_false || (!nulls[7] && !nulls[8]))
        { out[10] = BoolGetDatum(!ref_false && !code_false); nulls[10] = false; }
    }
    if (count)
    {
        int dims[1] = {count}, lbs[1] = {1};
        out[11] = PointerGetDatum(construct_array(pos, count, INT2OID, 2, true, TYPALIGN_SHORT));
        out[12] = PointerGetDatum(construct_array(weights, count, INT4OID, 4, true, TYPALIGN_INT));
        out[13] = PointerGetDatum(construct_array(types, count, TEXTOID, -1, false, TYPALIGN_INT));
        out[14] = PointerGetDatum(construct_md_array(gaps, gap_nulls, 1, dims, lbs, INT4OID, 4, true, TYPALIGN_INT));
        for (i = 11; i < 15; ++i) nulls[i] = false;
    }
    if (metadata)
    {
        out[15] = metadata->schema; nulls[15] = metadata->schema_null;
        out[16] = metadata->hash; nulls[16] = metadata->hash_null;
    }
    return heap_form_tuple(desc, out, nulls);
}

Datum
pgdm_decode_row(PG_FUNCTION_ARGS)
{
    RowDecodeCache *cache = (RowDecodeCache *) fcinfo->flinfo->fn_extra;
    if (cache == NULL)
    {
        TupleDesc desc;
        MemoryContext oldctx = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);
        cache = palloc(sizeof(RowDecodeCache));
        if (get_call_result_type(fcinfo, NULL, &desc) != TYPEFUNC_COMPOSITE) elog(ERROR, "composite result required");
        cache->desc = BlessTupleDesc(CreateTupleDescCopy(desc));
        init_context(&cache->decode, PG_GET_COLLATION());
        fcinfo->flinfo->fn_extra = cache;
        MemoryContextSwitchTo(oldctx);
    }
    PG_RETURN_DATUM(HeapTupleGetDatum(decode(PG_GETARG_INT64(0), PG_GETARG_JSONB_P(1), cache->desc, &cache->decode, NULL)));
}

static Datum
decode_chunk(FunctionCallInfo fcinfo, bool with_metadata, bool by_tid)
{
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    Oid argtypes[3] = {INT8OID, INT8OID, TEXTARRAYOID};
    Datum args[3] = {PG_GETARG_DATUM(0), PG_GETARG_DATUM(1), PG_GETARG_DATUM(2)};
    SPIPlanPtr plan;
    Portal portal;
    MemoryContext rowctx, oldctx;
    DecodeContext ctx;
    InitMaterializedSRF(fcinfo, 0);
    init_context(&ctx, PG_GET_COLLATION());
    rowctx = AllocSetContextCreate(CurrentMemoryContext, "pgdm decode row", ALLOCSET_DEFAULT_SIZES);
    SPI_connect();
    if (by_tid)
        plan = SPI_prepare("SELECT raw_id, raw, raw_schema, raw_hash FROM public.raw_data "
                           "WHERE ctid >= format('(%s,0)', $1)::tid "
                           "AND ctid < format('(%s,0)', $2 + 1)::tid AND raw_schema = ANY($3)", 3, argtypes);
    else if (with_metadata)
        plan = SPI_prepare("SELECT raw_id, raw, raw_schema, raw_hash FROM public.raw_data "
                           "WHERE raw_id BETWEEN $1 AND $2 AND raw_schema = ANY($3)", 3, argtypes);
    else
        plan = SPI_prepare("SELECT raw_id, raw FROM public.raw_data WHERE raw_id BETWEEN $1 AND $2 AND raw_schema = ANY($3)", 3, argtypes);
    if (!plan) elog(ERROR, "could not prepare raw_data scan");
    portal = SPI_cursor_open(NULL, plan, args, NULL, true);
    for (;;)
    {
        uint64 i, fetched;
        SPI_cursor_fetch(portal, true, 1024);
        fetched = SPI_processed;
        if (!fetched) break;
        for (i = 0; i < fetched; ++i)
        {
            bool isnull;
            Datum id = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull);
            Datum raw;
            SourceMetadata metadata;
            HeapTuple tuple;
            if (isnull) elog(ERROR, "raw_data.raw_id must not be null");
            raw = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2, &isnull);
            if (isnull) elog(ERROR, "raw_data.raw must not be null");
            if (with_metadata)
            {
                metadata.schema = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 3, &metadata.schema_null);
                metadata.hash = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 4, &metadata.hash_null);
            }
            oldctx = MemoryContextSwitchTo(rowctx);
            tuple = decode(DatumGetInt64(id), DatumGetJsonbP(raw), rsinfo->setDesc, &ctx, with_metadata ? &metadata : NULL);
            tuplestore_puttuple(rsinfo->setResult, tuple);
            MemoryContextSwitchTo(oldctx);
            MemoryContextReset(rowctx);
            CHECK_FOR_INTERRUPTS();
        }
        SPI_freetuptable(SPI_tuptable);
    }
    SPI_cursor_close(portal);
    SPI_freeplan(plan);
    SPI_finish();
    MemoryContextDelete(rowctx);
    return (Datum) 0;
}

Datum
pgdm_decode_chunk(PG_FUNCTION_ARGS)
{
    return decode_chunk(fcinfo, false, false);
}

Datum
pgdm_decode_chunk_v2(PG_FUNCTION_ARGS)
{
    return decode_chunk(fcinfo, true, PG_GETARG_BOOL(3));
}

Datum
pgdm_decode_build_id(PG_FUNCTION_ARGS)
{
    PG_RETURN_TEXT_P(cstring_to_text(PGDM_DECODE_BUILD_ID));
}
