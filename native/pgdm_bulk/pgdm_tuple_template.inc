/*
 * Cache physical layouts learned from actual PostgreSQL-formed heap tuples.
 *
 * PG16/18 layouts: table1/table6 fixed-width values, and table2 with an
 * actual one-byte X/Y/Z text value. Other rows use normal heap_form_tuple.
 * The cache belongs to the chunk owner; every inserted tuple is a separate
 * batch-owned copy. PG18 can allocate proven short template copies in Bump;
 * every generic or TOAST-capable tuple keeps the ordinary AllocSet path.
 * Constraints and ordinary heap/WAL insertion stay intact.
 */

#ifndef PGDM_TUPLE_TEMPLATE_INC
#define PGDM_TUPLE_TEMPLATE_INC

#include "access/htup_details.h"
#include "access/heaptoast.h"
#include "access/tupmacs.h"
#include "utils/memutils.h"

typedef struct PgdmTupleTemplateCache PgdmTupleTemplateCache;

#if (PG_VERSION_NUM >= 160000 && PG_VERSION_NUM < 170000) || \
    (PG_VERSION_NUM >= 180000 && PG_VERSION_NUM < 190000)

#define PGDM_TT_MAX_ATTRS 17
#define PGDM_TT_SLOTS 64
#define PGDM_TT_BUMP_KEEPER_BYTES ((Size) 2 * 1024 * 1024)

typedef struct PgdmTuplePatch
{
    int attribute;
    int length;
    Size offset;
} PgdmTuplePatch;

typedef struct PgdmTupleTemplate
{
    bool occupied;
    unsigned char text_key;
    uint32 null_mask;
    bool bump_safe;
    bool nulls[PGDM_TT_MAX_ATTRS];
    HeapTuple prototype;
    int patch_count;
    PgdmTuplePatch patches[PGDM_TT_MAX_ATTRS];
} PgdmTupleTemplate;

struct PgdmTupleTemplateCache
{
    MemoryContext context;
    MemoryContext bump_context;
    TupleDesc descriptor;
    uint32 byref_mask;
    int text_attribute;
    PgdmTupleTemplate *last;
    uint64 learned;
    uint64 copies;
    uint64 bump_copies;
    uint64 allocset_copies;
    uint64 generic_forms;
    uint64 bump_resets;
    Size bump_peak_bytes;
    PgdmTupleTemplate entries[PGDM_TT_SLOTS];
};

static PgdmTupleTemplateCache *
pgdm_tuple_template_create(TupleDesc descriptor, const char *table_name,
                           MemoryContext context)
{
    PgdmTupleTemplateCache *cache;
    int i;

    if (strcmp(table_name, "table1") != 0 &&
        strcmp(table_name, "table2") != 0 &&
        strcmp(table_name, "table6") != 0)
        return NULL;
    if (descriptor->natts <= 0 || descriptor->natts > PGDM_TT_MAX_ATTRS)
        return NULL;

    /* open_target has already checked exact names, types and typmods. */
    for (i = 0; i < descriptor->natts; ++i)
    {
        Form_pg_attribute attribute = TupleDescAttr(descriptor, i);

        if (attribute->attbyval &&
            attribute->attlen != 1 && attribute->attlen != 2 &&
            attribute->attlen != 4 &&
            !(sizeof(Datum) == 8 && attribute->attlen == 8))
            return NULL;
    }

    cache = MemoryContextAllocZero(context, sizeof(*cache));
    cache->context = context;
    cache->descriptor = descriptor;
    cache->text_attribute = strcmp(table_name, "table2") == 0 ? 4 : -1;
    for (i = 0; i < descriptor->natts; ++i)
        if (!TupleDescAttr(descriptor, i)->attbyval)
            cache->byref_mask |= ((uint32) 1 << i);
    if (cache->text_attribute >= 0 &&
        (descriptor->natts <= cache->text_attribute ||
         TupleDescAttr(descriptor, cache->text_attribute)->atttypid != VARCHAROID ||
         TupleDescAttr(descriptor, cache->text_attribute)->attlen != -1 ||
         TupleDescAttr(descriptor, cache->text_attribute)->attbyval))
    {
        pfree(cache);
        return NULL;
    }
#if PG_VERSION_NUM >= 180000 && PG_VERSION_NUM < 190000
    /*
     * BumpReset retains only its keeper.  A fixed 2 MiB keeper covers the
     * current <= 1024-slot / 1 MiB-payload batch plus HeapTupleData, alignment
     * and checking-build headers, without malloc/free churn at every flush.
     * Only successful, short, non-external template copies use this context.
     * Prototypes and all generic/TOAST-capable rows stay in AllocSet contexts.
     */
    cache->bump_context = BumpContextCreate(context, "pgdm template tuple batch",
                                           PGDM_TT_BUMP_KEEPER_BYTES,
                                           PGDM_TT_BUMP_KEEPER_BYTES,
                                           PGDM_TT_BUMP_KEEPER_BYTES);
#endif
    return cache;
}

static inline MemoryContext
pgdm_tuple_template_batch_context(PgdmTupleTemplateCache *cache,
                                  MemoryContext ordinary_context)
{
#if PG_VERSION_NUM >= 180000 && PG_VERSION_NUM < 190000
    return cache != NULL && cache->bump_context != NULL ?
        cache->bump_context : ordinary_context;
#else
    return ordinary_context;
#endif
}

/* Called only after every used slot has lost its tuple pointer/ownership. */
static void
pgdm_tuple_template_reset(PgdmTupleTemplateCache *cache)
{
#if PG_VERSION_NUM >= 180000 && PG_VERSION_NUM < 190000
    if (cache != NULL && cache->bump_context != NULL)
    {
        Size allocated = MemoryContextMemAllocated(cache->bump_context, false);

        cache->bump_peak_bytes = Max(cache->bump_peak_bytes, allocated);
        cache->bump_resets++;
        MemoryContextReset(cache->bump_context);
    }
#endif
}

static void
pgdm_tuple_template_json(StringInfo out, PgdmTupleTemplateCache *cache, uint64 rows)
{
    appendStringInfo(out, "{\"learned\":" UINT64_FORMAT
                     ",\"copies\":" UINT64_FORMAT ",\"generic_forms\":" UINT64_FORMAT
                     ",\"bump_copies\":" UINT64_FORMAT ",\"allocset_copies\":" UINT64_FORMAT
                     ",\"bump_resets\":" UINT64_FORMAT ",\"bump_peak_bytes\":" UINT64_FORMAT "}",
                     cache ? cache->learned : 0, cache ? cache->copies : 0,
                     cache ? cache->generic_forms : rows, cache ? cache->bump_copies : 0,
                     cache ? cache->allocset_copies : 0, cache ? cache->bump_resets : 0,
                     cache ? (uint64) cache->bump_peak_bytes : 0);
}

/* This function is the only generic formation entry from the Bump lane. */
static inline HeapTuple
pgdm_tuple_template_generic(PgdmTupleTemplateCache *cache,
                            TupleDesc descriptor, Datum *values, bool *nulls,
                            MemoryContext ordinary_context)
{
#if PG_VERSION_NUM >= 180000 && PG_VERSION_NUM < 190000
    MemoryContext old = MemoryContextSwitchTo(ordinary_context);
    HeapTuple tuple = heap_form_tuple(descriptor, values, nulls);

    MemoryContextSwitchTo(old);
    if (cache != NULL)
        cache->generic_forms++;
    return tuple;
#else
    if (cache != NULL)
        cache->generic_forms++;
    return heap_form_tuple(descriptor, values, nulls);
#endif
}

/* Inspect actual bytes; compressed/external/other values retain core handling. */
static bool
pgdm_tuple_template_text_key(Datum value, unsigned char *key)
{
    struct varlena *string = (struct varlena *) DatumGetPointer(value);
    unsigned char character;

    if (VARATT_IS_EXTERNAL(string) || VARATT_IS_COMPRESSED(string) ||
        VARSIZE_ANY_EXHDR(string) != 1)
        return false;
    character = (unsigned char) VARDATA_ANY(string)[0];
    *key = character;
    return true;
}

/* The prototype is never inserted and never receives transaction/TID changes. */
static void
pgdm_tuple_template_learn(PgdmTupleTemplateCache *cache,
                          PgdmTupleTemplate *entry, uint32 null_mask,
                          unsigned char text_key, Datum *values, bool *nulls)
{
    MemoryContext old = MemoryContextSwitchTo(cache->context);
    HeapTuple prototype = heap_form_tuple(cache->descriptor, values, nulls);
    char *data = (char *) prototype->t_data + prototype->t_data->t_hoff;
    Size offset = 0;
    int i;

    entry->prototype = prototype;
    entry->patch_count = 0;
    entry->null_mask = null_mask;
    entry->text_key = text_key;
    memcpy(entry->nulls, nulls, cache->descriptor->natts * sizeof(bool));
    for (i = 0; i < cache->descriptor->natts; ++i)
    {
        Form_pg_attribute attribute = TupleDescAttr(cache->descriptor, i);

        if (nulls[i])
            continue;
        /* Read the actual core-packed layout, including short varlenas. */
        offset = att_align_pointer(offset, attribute->attalign,
                                   attribute->attlen, data + offset);
        if (attribute->attbyval)
        {
            PgdmTuplePatch *patch = &entry->patches[entry->patch_count++];

            patch->attribute = i;
            patch->length = attribute->attlen;
            patch->offset = prototype->t_data->t_hoff + offset;
        }
        offset = att_addlength_pointer(offset, attribute->attlen, data + offset);
    }
    if (offset != prototype->t_len - prototype->t_data->t_hoff)
        elog(ERROR, "pgdm tuple template does not match core tuple layout");
    /* Match the actual condition that can send heap_prepare_insert to TOAST. */
    entry->bump_safe = !HeapTupleHasExternal(prototype) &&
                       prototype->t_len <= TOAST_TUPLE_THRESHOLD;
    entry->occupied = true;
    cache->learned++;
    MemoryContextSwitchTo(old);
}

static inline HeapTuple
pgdm_tuple_template_copy(PgdmTupleTemplateCache *cache,
                         PgdmTupleTemplate *entry, Datum *values,
                         MemoryContext ordinary_context)
{
#if PG_VERSION_NUM >= 180000 && PG_VERSION_NUM < 190000
    HeapTuple tuple;
    bool in_bump = cache->bump_context != NULL &&
                   CurrentMemoryContext == cache->bump_context;
#else
    HeapTuple tuple = heap_copytuple(entry->prototype);
#endif
    int i;

    cache->copies++;
#if PG_VERSION_NUM < 180000
    cache->allocset_copies++;
#endif
#if PG_VERSION_NUM >= 180000 && PG_VERSION_NUM < 190000
    if (in_bump && !entry->bump_safe)
    {
        MemoryContext old = MemoryContextSwitchTo(ordinary_context);

        tuple = heap_copytuple(entry->prototype);
        MemoryContextSwitchTo(old);
        cache->allocset_copies++;
    }
    else
    {
        /* Keep core heap_copytuple, including its full HeapTupleData layout. */
        tuple = heap_copytuple(entry->prototype);
        if (in_bump)
            cache->bump_copies++;
        else
            cache->allocset_copies++;
    }
#endif
    for (i = 0; i < entry->patch_count; ++i)
    {
        PgdmTuplePatch *patch = &entry->patches[i];

        store_att_byval((char *) tuple->t_data + patch->offset,
                        values[patch->attribute], patch->length);
    }
    return tuple;
}

static HeapTuple
pgdm_form_cached_tuple(PgdmTupleTemplateCache *cache, TupleDesc descriptor,
                       Datum *values, bool *nulls,
                       MemoryContext ordinary_context)
{
    PgdmTupleTemplate *entry;
    uint32 null_mask = 0;
    uint32 present_byref;
    uint32 hash;
    unsigned char text_key = 0;
    int i, probe;

    if (cache == NULL)
        return heap_form_tuple(descriptor, values, nulls);
    if (cache->text_attribute >= 0 && !nulls[cache->text_attribute] &&
        !pgdm_tuple_template_text_key(values[cache->text_attribute], &text_key))
        return pgdm_tuple_template_generic(cache, descriptor, values, nulls,
                                            ordinary_context);

    /* Common table1/table6 rows need no mask loop or cache probing at all. */
    entry = cache->last;
    if (entry != NULL && entry->text_key == text_key &&
        memcmp(entry->nulls, nulls, descriptor->natts * sizeof(bool)) == 0)
        return pgdm_tuple_template_copy(cache, entry, values, ordinary_context);

    for (i = 0; i < descriptor->natts; ++i)
        if (nulls[i])
            null_mask |= ((uint32) 1 << i);
    present_byref = cache->byref_mask & ~null_mask;
    if (cache->text_attribute >= 0)
        present_byref &= ~((uint32) 1 << cache->text_attribute);
    if (present_byref != 0)
        return pgdm_tuple_template_generic(cache, descriptor, values, nulls,
                                            ordinary_context);

    hash = null_mask ^ (null_mask >> 7) ^ (null_mask >> 13) ^
           ((uint32) text_key * 0x9e3779b9U);
    hash ^= hash >> 4;
    for (probe = 0; probe < PGDM_TT_SLOTS; ++probe)
    {
        entry = &cache->entries[(hash + probe) & (PGDM_TT_SLOTS - 1)];
        if (!entry->occupied)
        {
            pgdm_tuple_template_learn(cache, entry, null_mask, text_key,
                                      values, nulls);
            cache->last = entry;
            return pgdm_tuple_template_copy(cache, entry, values, ordinary_context);
        }
        if (entry->null_mask == null_mask && entry->text_key == text_key)
        {
            cache->last = entry;
            return pgdm_tuple_template_copy(cache, entry, values, ordinary_context);
        }
    }
    /* The fixed cache never becomes an unbounded row/value stage. */
    return pgdm_tuple_template_generic(cache, descriptor, values, nulls,
                                        ordinary_context);
}

#else

/* Keep other major versions on their already-reviewed physical tuple path. */
static PgdmTupleTemplateCache *
pgdm_tuple_template_create(TupleDesc descriptor, const char *table_name,
                           MemoryContext context)
{
    return NULL;
}

static HeapTuple
pgdm_form_cached_tuple(PgdmTupleTemplateCache *cache, TupleDesc descriptor,
                       Datum *values, bool *nulls,
                       MemoryContext ordinary_context)
{
    return heap_form_tuple(descriptor, values, nulls);
}

static inline MemoryContext
pgdm_tuple_template_batch_context(PgdmTupleTemplateCache *cache,
                                  MemoryContext ordinary_context)
{
    return ordinary_context;
}

static void
pgdm_tuple_template_reset(PgdmTupleTemplateCache *cache)
{
}

static void
pgdm_tuple_template_json(StringInfo out, PgdmTupleTemplateCache *cache, uint64 rows)
{
    appendStringInfo(out, "{\"learned\":0,\"copies\":0,\"generic_forms\":" UINT64_FORMAT "}", rows);
}

#endif
#endif /* PGDM_TUPLE_TEMPLATE_INC */
