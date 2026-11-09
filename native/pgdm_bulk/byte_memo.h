/* Chunk-local exact-byte memoization of successful PostgreSQL calls only.
 * The caller owns the fixed type, typmod, collation and immutable lookup maps.
 * No session/backend state survives the chunk memory context.
 */
#ifndef PGDM_BYTE_MEMO_H
#define PGDM_BYTE_MEMO_H

#ifndef PGDM_NUMERIC_SLOTS
#define PGDM_NUMERIC_SLOTS 16384
#endif
#ifndef PGDM_TEXT_SLOTS
#define PGDM_TEXT_SLOTS 128
#endif
#ifndef PGDM_DIMENSION_SLOTS
#define PGDM_DIMENSION_SLOTS 32
#endif
#define MEMO_KEY_BYTES 64
#define MEMO_DATUM_BYTES 256
#define MEMO_NUMBER 1
#define MEMO_GE 2
#define MEMO_GT 4
#define MEMO_MILLI 8
#define MEMO_ROUND1 16
#define MEMO_RESULT 32

typedef struct ByteMemoEntry
{
    uint32 hash;
    uint8 length, flags;
    bool occupied, ge, gt, valid;
    char key[MEMO_KEY_BYTES];
    Datum value, round1;
    int32 milli;
    int64 id;
} ByteMemoEntry;

typedef struct ByteMemo
{
    ByteMemoEntry *entries;
    unsigned slots, used;
    uint64 hits, misses, full, bypass, calls_saved;
} ByteMemo;

/* A bounded load factor also bounds unsuccessful probes on diverse inputs. */
static ByteMemoEntry *
memo_entry(ByteMemo *memo, const char *key, Size length, Size max_length,
           unsigned slots, unsigned limit, MemoryContext context)
{
    uint32 hash;
    unsigned probe;
    if (length > max_length)
    {
        ++memo->bypass;
        return NULL;
    }
    if (memo->entries == NULL)
    {
        memo->slots = slots;
        memo->entries = MemoryContextAllocZero(context, sizeof(ByteMemoEntry) * slots);
    }
    hash = hash_bytes((const unsigned char *) key, length);
    for (probe = 0; probe < slots; ++probe)
    {
        ByteMemoEntry *entry = &memo->entries[(hash + probe) & (slots - 1)];
        if (!entry->occupied)
        {
            ++memo->misses;
            if (memo->used >= limit)
            {
                ++memo->full;
                return NULL;
            }
            entry->occupied = true;
            entry->hash = hash;
            entry->length = length;
            memcpy(entry->key, key, length);
            ++memo->used;
            return entry;
        }
        if (entry->hash == hash && entry->length == length &&
            memcmp(entry->key, key, length) == 0)
        {
            if (entry->flags) ++memo->hits;
            else ++memo->misses;
            return entry;
        }
    }
    elog(ERROR, "pgdm byte memo load bound violated");
    return NULL;
}

static Datum
memo_copy(Datum value, MemoryContext context)
{
    MemoryContext old = MemoryContextSwitchTo(context);
    Datum result = datumCopy(value, false, -1);
    MemoryContextSwitchTo(old);
    return result;
}

static void
memo_json(StringInfo out, const char *name, const ByteMemo *memo)
{
    appendStringInfo(out, "\"%s\":{\"hits\":" UINT64_FORMAT
                     ",\"misses\":" UINT64_FORMAT ",\"full\":" UINT64_FORMAT
                     ",\"bypass\":" UINT64_FORMAT ",\"calls_saved\":" UINT64_FORMAT
                     ",\"entries\":%u}", name, memo->hits, memo->misses,
                     memo->full, memo->bypass, memo->calls_saved, memo->used);
}

#endif
