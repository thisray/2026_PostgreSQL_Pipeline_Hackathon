/*
 * Bounded heap-slot storage with one real TupleDesc pin per target.
 *
 * PG16/18 heap slot init/release have no per-slot resources.  Initialize one
 * prototype through core, then clone only its empty heap-slot state into one
 * aligned allocation.  Each clone has independent Datum/null arrays, physical
 * tuple pointer, deformation state, flags, and TID.  Only the prototype owns a
 * descriptor reference; clones borrow it until this slab is destroyed.
 *
 * NEVER pass a cloned slot to ExecDropSingleTupleTableSlot, ExecSetSlotDescriptor,
 * or an executor tuple table.  New tuples use the exact core heap-slot store
 * state below; table_multi_insert and ExecClearTuple remain core operations.
 * Heap tuple ownership stays unchanged.
 */

#ifndef PGDM_SLOT_SLAB_INC
#define PGDM_SLOT_SLAB_INC

#include "executor/tuptable.h"
#include "utils/memutils.h"

#define PGDM_SLOT_SLAB_MAX_SLOTS 8192

typedef struct PgdmHeapSlotSlab
{
    MemoryContext context;
    TupleTableSlot *prototype;
    TupleTableSlot **slots;
    int capacity;
    bool packed;
} PgdmHeapSlotSlab;

/*
 * flush_target already clears every used slot.  For an actually empty core
 * heap slot with no owned tuple, the clear inside ExecStoreHeapTuple would
 * only write state which the store immediately overwrites.  Preserve exactly
 * the PG16/18 tts_heap_store_tuple + ExecStoreHeapTuple final state, including
 * all unrelated flags and the owning flag needed by heap materialization.
 * Nonempty/owning slots and other majors keep the complete core path.
 */
static inline TupleTableSlot *
pgdm_store_new_heap_tuple(HeapTuple tuple, TupleTableSlot *slot)
{
    Assert(tuple != NULL);
    Assert(slot != NULL);
    Assert(slot->tts_tupleDescriptor != NULL);

#if (PG_VERSION_NUM >= 160000 && PG_VERSION_NUM < 170000) || \
    (PG_VERSION_NUM >= 180000 && PG_VERSION_NUM < 190000)
    if (likely(TTS_IS_HEAPTUPLE(slot) &&
               (slot->tts_flags & (TTS_FLAG_EMPTY | TTS_FLAG_SHOULDFREE)) ==
                   TTS_FLAG_EMPTY))
    {
        HeapTupleTableSlot *hslot = (HeapTupleTableSlot *) slot;

        slot->tts_nvalid = 0;
        hslot->tuple = tuple;
        hslot->off = 0;
        slot->tts_flags =
            (slot->tts_flags & ~(TTS_FLAG_EMPTY | TTS_FLAG_SHOULDFREE)) |
            TTS_FLAG_SHOULDFREE;
        slot->tts_tid = tuple->t_self;
        slot->tts_tableOid = tuple->t_tableOid;
        return slot;
    }
#endif

    return ExecStoreHeapTuple(tuple, slot, true);
}

static void
pgdm_heap_slot_slab_init(PgdmHeapSlotSlab *slab, TupleDesc descriptor,
                         TupleTableSlot **slots, int capacity,
                         MemoryContext parent)
{
    MemoryContext old;
    int i;

    if (descriptor == NULL || descriptor->natts <= 0 || slots == NULL ||
        capacity <= 0 || capacity > PGDM_SLOT_SLAB_MAX_SLOTS)
        elog(ERROR, "invalid pgdm heap slot slab dimensions");
    memset(slab, 0, sizeof(*slab));
    slab->context = AllocSetContextCreate(parent, "pgdm heap slot slab",
                                           ALLOCSET_DEFAULT_SIZES);
    slab->slots = slots;
    slab->capacity = capacity;
    old = MemoryContextSwitchTo(slab->context);

#if (PG_VERSION_NUM >= 160000 && PG_VERSION_NUM < 170000) || \
    (PG_VERSION_NUM >= 180000 && PG_VERSION_NUM < 190000)
    {
        Size base_size = TTSOpsHeapTuple.base_slot_size;
        Size values_offset = MAXALIGN(base_size);
        Size nulls_offset = values_offset +
                            MAXALIGN((Size) descriptor->natts * sizeof(Datum));
        Size stride = nulls_offset +
                      MAXALIGN((Size) descriptor->natts * sizeof(bool));
        char *storage;

        if (base_size < sizeof(HeapTupleTableSlot) ||
            stride > MaxAllocSize / (Size) capacity)
            elog(ERROR, "pgdm heap slot slab exceeds allocation bounds");

        /* This is the only owner of the descriptor reference for all clones. */
        slab->prototype = MakeSingleTupleTableSlot(descriptor, &TTSOpsHeapTuple);
        ExecClearTuple(slab->prototype);
        Assert(TTS_EMPTY(slab->prototype));
        Assert(TTS_FIXED(slab->prototype));
        Assert(!TTS_SHOULDFREE(slab->prototype));
        Assert(((HeapTupleTableSlot *) slab->prototype)->tuple == NULL);
        Assert(((HeapTupleTableSlot *) slab->prototype)->off == 0);

        storage = palloc0(stride * (Size) capacity);
        for (i = 0; i < capacity; ++i)
        {
            char *address = storage + stride * (Size) i;
            TupleTableSlot *slot = (TupleTableSlot *) address;

            /* Also preserves the core-initialized const tts_ops member. */
            memcpy(address, slab->prototype, base_size);
            slot->tts_values = (Datum *) (address + values_offset);
            slot->tts_isnull = (bool *) (address + nulls_offset);
            slot->tts_mcxt = slab->context;
            slots[i] = slot;
        }
        slab->packed = true;
    }
#else
    /* Other majors keep their ordinary core construction/destruction path. */
    for (i = 0; i < capacity; ++i)
        slots[i] = MakeSingleTupleTableSlot(descriptor, &TTSOpsHeapTuple);
#endif

    MemoryContextSwitchTo(old);
}

static void
pgdm_heap_slot_slab_destroy(PgdmHeapSlotSlab *slab)
{
    MemoryContext context;
    int i;

    if (slab->context == NULL)
        return;
    context = slab->context;
    if (slab->packed)
    {
        for (i = 0; i < slab->capacity; ++i)
        {
            TupleTableSlot *slot = slab->slots[i];

            /*
             * Clear real tuple ownership before releasing the shared descriptor.
             * The heap release callback is empty in both reviewed major versions.
             * Do not run ExecDrop: clones never acquired descriptor references.
             */
            ExecClearTuple(slot);
            slot->tts_ops->release(slot);
            slab->slots[i] = NULL;
        }
        ExecDropSingleTupleTableSlot(slab->prototype);
        slab->prototype = NULL;
    }
    else
    {
        for (i = 0; i < slab->capacity; ++i)
        {
            ExecDropSingleTupleTableSlot(slab->slots[i]);
            slab->slots[i] = NULL;
        }
    }
    slab->context = NULL;
    slab->slots = NULL;
    slab->capacity = 0;
    slab->packed = false;
    MemoryContextDelete(context);
}

#endif /* PGDM_SLOT_SLAB_INC */
