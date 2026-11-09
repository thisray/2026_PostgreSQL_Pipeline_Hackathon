/* Copied from pgdm_decode build s3-native-v2-20261102.3.
 * S4 emits typed scalars and slot vectors directly: no composite heap tuple,
 * PostgreSQL arrays, unnest, or stage table. Keep casts aligned with that source.
 * TODO: share the decoder core after the integrated candidate is measured.
 */

#include "byte_memo.h"

#define DECODE_TEXT_BUFFER 64
#define DECODE_LAYOUT_KEYS 256
#define DECODE_LAYOUT_BYTES (16 * 1024)

typedef struct MemoNumber
{
    Datum value;
    ByteMemoEntry *entry;
} MemoNumber;

typedef struct DecodedRow
{
    Datum values[11];
    bool nulls[11];
    int slot_count;
    Datum positions[14], weights[14], types[14], gaps[14];
    bool gap_nulls[14];
    MemoNumber speed;
} DecodedRow;

typedef struct DecodeContext
{
    Oid collation;
    MemoryContext context;
    ByteMemo numeric, upper, ref, dimension, code, int2;
    uint64 layout_hits, layout_fallbacks, short_text_hits;
    uint64 scope_equal_hits, manifest_equal_hits;
    const char *hotpath_json;
    char text_buffer[64][DECODE_TEXT_BUFFER];
    bool layout_ready;
    uint32 layout_count, layout_bytes;
    JEntry *layout_entries;
    char *layout_keys;
    int *layout_indices;
    Datum zero, thousand, zone, pattern;
    Datum slot_types[3];
} DecodeContext;

typedef struct DecodeFields
{
    JsonbValue values[64];
    bool present[64];
} DecodeFields;

static void
init_context(DecodeContext *ctx, Oid collation)
{
    memset(ctx, 0, sizeof(*ctx));
    ctx->context = CurrentMemoryContext;
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
field_text(DecodeFields *fields, int index, DecodeContext *ctx)
{
    /* Each field is requested at most once by decode(). */
    char *result = NULL;
    if (fields->present[index])
    {
        JsonbValue *value = &fields->values[index];
        if (value->type == jbvString)
        {
            const char *start = value->val.string.val;
            int length = value->val.string.len;
            while (length && *start == ' ') { ++start; --length; }
            while (length && start[length - 1] == ' ') --length;
            if (length < DECODE_TEXT_BUFFER)
            {
                result = ctx->text_buffer[index];
                memcpy(result, start, length);
                result[length] = '\0';
                ++ctx->short_text_hits;
            }
            else result = trim(value_text(value));
        }
        else result = trim(value_text(value));
    }
    return result;
}

static MemoNumber
number(char *s, DecodeContext *ctx)
{
    MemoNumber result;
    ByteMemoEntry *entry = memo_entry(&ctx->numeric, s, strlen(s), 15,
                                      PGDM_NUMERIC_SLOTS, PGDM_NUMERIC_SLOTS * 3 / 4, ctx->context);
    result.entry = entry;
    if (entry && (entry->flags & MEMO_NUMBER))
    {
        ++ctx->numeric.calls_saved;
        result.value = entry->value;
        return result;
    }
    result.value = DirectFunctionCall3(numeric_in, CStringGetDatum(s), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
    /* Short input can still expand into a huge numeric (for example 1e10000). */
    if (entry && datumGetSize(result.value, false, -1) <= MEMO_DATUM_BYTES)
    {
        entry->value = memo_copy(result.value, ctx->context);
        entry->flags |= MEMO_NUMBER;
    }
    else result.entry = NULL;
    return result;
}

static Datum
scaled(MemoNumber n, DecodeContext *ctx)
{
    Datum product, rounded, result;
    if (n.entry && (n.entry->flags & MEMO_MILLI))
    {
        ctx->numeric.calls_saved += 3;
        return Int32GetDatum(n.entry->milli);
    }
    product = DirectFunctionCall2(numeric_mul, n.value, ctx->thousand);
    rounded = DirectFunctionCall2(numeric_round, product, Int32GetDatum(0));
    result = DirectFunctionCall1(numeric_int4, rounded);
    if (n.entry)
    {
        n.entry->milli = DatumGetInt32(result);
        n.entry->flags |= MEMO_MILLI;
    }
    return result;
}

static bool
numeric_predicate(MemoNumber n, DecodeContext *ctx, bool inclusive)
{
    int flag = inclusive ? MEMO_GE : MEMO_GT;
    bool result;
    if (n.entry && (n.entry->flags & flag))
    {
        ++ctx->numeric.calls_saved;
        return inclusive ? n.entry->ge : n.entry->gt;
    }
    result = DatumGetBool(inclusive ? DirectFunctionCall2(numeric_ge, n.value, ctx->zero) :
                                     DirectFunctionCall2(numeric_gt, n.value, ctx->zero));
    if (n.entry)
    {
        if (inclusive) n.entry->ge = result;
        else n.entry->gt = result;
        n.entry->flags |= flag;
    }
    return result;
}

static Datum
round_speed(MemoNumber n, DecodeContext *ctx)
{
    Datum result;
    if (n.entry && (n.entry->flags & MEMO_ROUND1))
    {
        ++ctx->numeric.calls_saved;
        return n.entry->round1;
    }
    result = DirectFunctionCall2(numeric_round, n.value, Int32GetDatum(1));
    if (n.entry && datumGetSize(result, false, -1) <= MEMO_DATUM_BYTES)
    {
        n.entry->round1 = memo_copy(result, ctx->context);
        n.entry->flags |= MEMO_ROUND1;
    }
    return result;
}

static Datum
memo_text(char *s, DecodeContext *ctx, bool uppercase)
{
    ByteMemo *memo = uppercase ? &ctx->upper : &ctx->ref;
    ByteMemoEntry *entry = memo_entry(memo, s, strlen(s), 31,
                                      PGDM_TEXT_SLOTS, PGDM_TEXT_SLOTS / 2, ctx->context);
    Datum result;
    if (entry && (entry->flags & MEMO_RESULT))
    {
        ++memo->calls_saved;
        return entry->value;
    }
    result = CStringGetTextDatum(s);
    if (uppercase) result = DirectFunctionCall1Coll(upper, ctx->collation, result);
    if (entry && datumGetSize(result, false, -1) <= MEMO_DATUM_BYTES)
    {
        entry->value = memo_copy(result, ctx->context);
        entry->flags |= MEMO_RESULT;
    }
    return result;
}

static Datum
upper_trim(char *s, DecodeContext *ctx)
{
    return memo_text(s, ctx, true);
}

static int16
declared_int2(char *s, DecodeContext *ctx)
{
    ByteMemoEntry *entry = memo_entry(&ctx->int2, s, strlen(s), 15,
                                      PGDM_TEXT_SLOTS, PGDM_TEXT_SLOTS / 2, ctx->context);
    int16 result;
    if (entry && (entry->flags & MEMO_RESULT))
    {
        ++ctx->int2.calls_saved;
        return DatumGetInt16(entry->value);
    }
    /* Learn only after the original input function succeeds, with its errors. */
    result = DatumGetInt16(DirectFunctionCall1(int2in, CStringGetDatum(s)));
    if (entry)
    {
        entry->value = Int16GetDatum(result);
        entry->flags |= MEMO_RESULT;
    }
    return result;
}

/* Identical bytes imply equality even with nondeterministic collations.
 * Different/compressed/external representations retain PostgreSQL texteq.
 */
static bool
exact_text_equal(Datum a, Datum b, Oid collation, uint64 *hits)
{
    struct varlena *left = (struct varlena *) DatumGetPointer(a);
    struct varlena *right = (struct varlena *) DatumGetPointer(b);
    if (!VARATT_IS_EXTERNAL(left) && !VARATT_IS_COMPRESSED(left))
        if (!VARATT_IS_EXTERNAL(right) && !VARATT_IS_COMPRESSED(right) &&
            VARSIZE_ANY_EXHDR(left) == VARSIZE_ANY_EXHDR(right) &&
            memcmp(VARDATA_ANY(left), VARDATA_ANY(right), VARSIZE_ANY_EXHDR(left)) == 0)
        {
            ++*hits;
            return true;
        }
    return DatumGetBool(DirectFunctionCall2Coll(texteq, collation, a, b));
}

static void
decode_receipt(DecodeContext *ctx)
{
    StringInfoData json;
    initStringInfo(&json);
    appendStringInfo(&json, "{\"layout_hits\":" UINT64_FORMAT
                     ",\"layout_fallbacks\":" UINT64_FORMAT
                     ",\"short_text_hits\":" UINT64_FORMAT
                     ",\"scope_equal_hits\":" UINT64_FORMAT
                     ",\"manifest_equal_hits\":" UINT64_FORMAT ",",
                     ctx->layout_hits, ctx->layout_fallbacks, ctx->short_text_hits,
                     ctx->scope_equal_hits, ctx->manifest_equal_hits);
    memo_json(&json, "numeric", &ctx->numeric); appendStringInfoChar(&json, ',');
    memo_json(&json, "upper", &ctx->upper); appendStringInfoChar(&json, ',');
    memo_json(&json, "ref", &ctx->ref); appendStringInfoChar(&json, ',');
    memo_json(&json, "dimension", &ctx->dimension); appendStringInfoChar(&json, ',');
    memo_json(&json, "code", &ctx->code); appendStringInfoChar(&json, ',');
    memo_json(&json, "int2", &ctx->int2);
    appendStringInfo(&json, ",\"hotpath\":%s}", ctx->hotpath_json ? ctx->hotpath_json : "{}");
    SetConfigOption("pgdm_bulk.last_decode_counters", json.data, PGC_USERSET, PGC_S_SESSION);
    pfree(json.data);
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

/* Cache only observed key bytes and their public JEntry representation.
 * No assumed key order, column count, offsets, or generated values.
 * Strings/nulls use spans; every other JSONB type retains the core iterator.
 */
static bool
layout_fields(JsonbContainer *container, DecodeFields *fields, DecodeContext *ctx)
{
    uint32 count = JsonContainerSize(container);
    uint32 key_bytes = count ? getJsonbOffset(container, count) : 0;
    char *data = (char *) &container->children[count * 2];
    uint32 offset = key_bytes, i;
    if (!ctx->layout_ready)
    {
        MemoryContext old;
        uint32 key_offset = 0;
        if (count > DECODE_LAYOUT_KEYS || key_bytes > DECODE_LAYOUT_BYTES)
            return false;
        old = MemoryContextSwitchTo(ctx->context);
        ctx->layout_entries = palloc(Max(1, count * sizeof(JEntry)));
        ctx->layout_keys = palloc(Max(1, key_bytes));
        ctx->layout_indices = palloc(Max(1, count * sizeof(int)));
        memcpy(ctx->layout_entries, container->children, count * sizeof(JEntry));
        memcpy(ctx->layout_keys, data, key_bytes);
        for (i = 0; i < count; ++i)
        {
            JsonbValue key;
            uint32 end = key_offset;
            JBE_ADVANCE_OFFSET(end, container->children[i]);
            key.type = jbvString;
            key.val.string.val = data + key_offset;
            key.val.string.len = end - key_offset;
            ctx->layout_indices[i] = key_index(&key);
            key_offset = end;
        }
        ctx->layout_count = count;
        ctx->layout_bytes = key_bytes;
        ctx->layout_ready = true;
        MemoryContextSwitchTo(old);
    }
    if (count != ctx->layout_count || key_bytes != ctx->layout_bytes ||
        memcmp(ctx->layout_entries, container->children, count * sizeof(JEntry)) ||
        memcmp(ctx->layout_keys, data, key_bytes))
        return false;
    for (i = 0; i < count; ++i)
    {
        JEntry entry = container->children[count + i];
        uint32 end = offset;
        int index = ctx->layout_indices[i];
        JBE_ADVANCE_OFFSET(end, entry);
        if (!JBE_ISSTRING(entry) && !JBE_ISNULL(entry)) return false;
        if (index >= 0)
        {
            JsonbValue *value = &fields->values[index];
            value->type = JBE_ISNULL(entry) ? jbvNull : jbvString;
            value->val.string.val = data + offset;
            value->val.string.len = end - offset;
            fields->present[index] = true;
        }
        offset = end;
    }
    ++ctx->layout_hits;
    return true;
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

static void
decode_bulk(int64 raw_id, Jsonb *raw, DecodeContext *ctx, DecodedRow *row)
{
    DecodeFields fields;
    Datum *out = row->values, *pos = row->positions, *weights = row->weights;
    Datum *types = row->types, *gaps = row->gaps;
    bool *nulls = row->nulls, *gap_nulls = row->gap_nulls;
    JsonbValue key, *values;
    char *external_id, *observed_at, *speed, *ref, *code2, *code3;
    int i, count = 0;
    int16 maximum = PG_INT16_MIN;
    bool have_max = false;

    memset(row->values, 0, sizeof(row->values));
    memset(row->nulls, true, sizeof(row->nulls));
    memset(row->gap_nulls, true, sizeof(row->gap_nulls));
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
        if (!layout_fields(values->val.binary.data, &fields, ctx))
        {
            ++ctx->layout_fallbacks;
            memset(fields.present, false, sizeof(fields.present));
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
    }
    out[0] = Int64GetDatum(raw_id); nulls[0] = false;
    external_id = field_text(&fields, 0, ctx);
    observed_at = field_text(&fields, 1, ctx);
    if (external_id && *external_id) { out[1] = DirectFunctionCall1(int4in, CStringGetDatum(external_id)); nulls[1] = false; }
    if (observed_at && *observed_at)
    {
        Datum ts = DirectFunctionCall3(timestamptz_in, CStringGetDatum(observed_at), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
        out[2] = DirectFunctionCall2(timestamptz_zone, ctx->zone, ts); nulls[2] = false;
    }
    for (i = 0; i < 14; ++i)
    {
        char *declared = field_text(&fields, 3 + i, ctx);
        if (declared && *declared)
        {
            int16 d = declared_int2(declared, ctx);
            if (!have_max || d > maximum) maximum = d;
            have_max = true;
            if (d == i + 1)
            {
                char *weight = field_text(&fields, 36 + 2 * i, ctx);
                char *type;
                char *gap_text;
                MemoNumber w;
                Datum t;
                if (!weight || !*weight) continue;
                w = number(weight, ctx);
                if (!numeric_predicate(w, ctx, true)) continue;
                type = field_text(&fields, 20 + i, ctx);
                if (!type) continue;
                if (!decode_slot_type(type, ctx, &t)) continue;
                pos[count] = Int16GetDatum(i + 1);
                weights[count] = scaled(w, ctx);
                types[count] = t;
                gaps[count] = (Datum) 0;
                gap_text = field_text(&fields, 37 + 2 * i, ctx);
                if (gap_text && *gap_text)
                {
                    MemoNumber gap = number(gap_text, ctx);
                    if (numeric_predicate(gap, ctx, false)) { gaps[count] = scaled(gap, ctx); gap_nulls[count] = false; }
                }
                ++count;
            }
        }
    }
    if (have_max) { out[3] = Int16GetDatum(maximum); nulls[3] = false; }
    for (i = 0; i < 2; ++i)
    {
        char *s = field_text(&fields, 35 - i, ctx);
        if (s && *s)
        {
            MemoNumber n = number(s, ctx);
            if (numeric_predicate(n, ctx, false)) { out[4 + i] = scaled(n, ctx); nulls[4 + i] = false; }
        }
    }
    speed = field_text(&fields, 2, ctx);
    ref = field_text(&fields, 17, ctx);
    code2 = field_text(&fields, 18, ctx);
    code3 = field_text(&fields, 19, ctx);
    if (speed && *speed) { row->speed = number(speed, ctx); out[6] = row->speed.value; nulls[6] = false; }
    if (ref) { out[7] = memo_text(ref, ctx, false); nulls[7] = false; }
    if (code2) { out[8] = upper_trim(code2, ctx); nulls[8] = false; }
    if (code3) { out[9] = upper_trim(code3, ctx); nulls[9] = false; }
    /* parent_ok is evaluated by the bulk receiver only on a dimension miss. */
    row->slot_count = count;
}
