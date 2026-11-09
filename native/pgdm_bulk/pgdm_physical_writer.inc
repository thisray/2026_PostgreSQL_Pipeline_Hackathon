/*
 * PostgreSQL 18 bulk Btree storage: bounded contiguous vector writes.
 *
 * Based on the public storage contracts and bulk-write finish behavior in
 * PostgreSQL REL_18_6, src/backend/storage/smgr/{bulk_write,smgr,md}.c.
 * We own our page allocations and queue.  Core log_newpages, checksums,
 * smgrextend/smgrwritev and smgr_bulk_finish retain their ordinary behavior.
 * No private BulkWriteState/MdfdVec layout or raw file descriptor is used.
 *
 * PostgreSQL Database Management System
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without a written agreement
 * is hereby granted, provided that the above copyright notice and this
 * paragraph and the following two paragraphs appear in all copies.
 *
 * IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
 * DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 * THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
 * ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO
 * PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 */
#ifndef PGDM_PHYSICAL_WRITER_INC
#define PGDM_PHYSICAL_WRITER_INC

#if PG_VERSION_NUM >= 180000 && PG_VERSION_NUM < 190000

#include "access/xloginsert.h"
#include "executor/instrument.h"
#include "port/pg_iovec.h"
#include "portability/instr_time.h"
#include "storage/bufpage.h"
#include "storage/bulk_write.h"
#include "storage/smgr.h"
#include "utils/memutils.h"
#include "utils/rel.h"

/* At most 1 MiB of owned pending pages, even with nondefault BLCKSZ. */
#define PGDM_PW_PENDING_PAGES \
    ((BLCKSZ <= 8192) ? 128 : (1024 * 1024 / BLCKSZ))

typedef struct PgdmPhysicalWriteStats
{
    uint64 pages;
    uint64 flushes;
    uint64 wal_records;
    uint64 runs;
    uint64 extend_calls;
    uint64 writev_calls;
    uint64 writev_pages;
    uint64 zero_extend_calls;
    uint64 zero_pages;
    double wal_ms;
    double checksum_ms;
    double storage_ms;
    double finish_ms;
} PgdmPhysicalWriteStats;

typedef struct PgdmPhysicalPage
{
    BlockNumber block;
    Page page;
} PgdmPhysicalPage;

typedef struct PgdmPhysicalWriteState
{
    SMgrRelation smgr;
    ForkNumber forknum;
    Oid index_oid;
    bool use_wal;
    BlockNumber relsize;
    MemoryContext context;
    BulkWriteState *finish_guard;
    int npending;
    PgdmPhysicalPage pending[PGDM_PW_PENDING_PAGES];
    PgdmPhysicalWriteStats stats;
} PgdmPhysicalWriteState;

static inline double
pgdm_pw_ms(instr_time begin, instr_time end)
{
    INSTR_TIME_SUBTRACT(end, begin);
    return INSTR_TIME_GET_MILLISEC(end);
}

static PgdmPhysicalWriteState *
pgdm_pw_start_rel(Relation index, ForkNumber forknum)
{
    PgdmPhysicalWriteState *state = palloc0(sizeof(*state));

    state->smgr = RelationGetSmgr(index);
    state->forknum = forknum;
    state->index_oid = RelationGetRelid(index);
    state->use_wal = RelationNeedsWAL(index) || forknum == INIT_FORKNUM;
    state->context = CurrentMemoryContext;
    /*
     * Start before any of our writes.  This ordinary core bulk state keeps
     * its own start_RedoRecPtr, use_wal and SMgrRelation.  Its queue stays
     * empty: our pages never cross the core allocation/ownership boundary.
     * After our queue is flushed, core finish performs its unchanged
     * checkpoint interlock, sync registration or immediate fsync.
     */
    state->finish_guard = smgr_bulk_start_rel(index, forknum);
    state->relsize = smgrnblocks(state->smgr, forknum);
    return state;
}

static Page
pgdm_pw_get_buf(PgdmPhysicalWriteState *state)
{
    return (Page) MemoryContextAllocAligned(state->context, BLCKSZ,
                                            PG_IO_ALIGN_SIZE, 0);
}

static int
pgdm_pw_page_order(const void *a, const void *b)
{
    BlockNumber left = ((const PgdmPhysicalPage *) a)->block;
    BlockNumber right = ((const PgdmPhysicalPage *) b)->block;

    Assert(left != right);
    return left > right ? 1 : -1;
}

/*
 * A single run never crosses either smgr's combine boundary or PG_IOV_MAX.
 * smgrextend explicitly supports writing AT OR BEYOND EOF.  Write its last
 * page first, then all other pages are existing blocks for smgrwritev.
 * Each final page is written exactly once; no pre-zero of the run is needed.
 */
static void
pgdm_pw_write_run(PgdmPhysicalWriteState *state, BlockNumber first,
                   const void **buffers, int count)
{
    BlockNumber last;
    int write_count = count;

    Assert(count > 0 && count <= PG_IOV_MAX);
    Assert((uint32) count <= smgrmaxcombine(state->smgr, state->forknum, first));
    if ((uint64) first + (uint64) count > (uint64) InvalidBlockNumber)
        ereport(ERROR,
            (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
             errmsg("pgdm index exceeds PostgreSQL's block-number limit")));
    last = first + (BlockNumber) count - 1;

    /* Preserve genuine unsubmitted holes (e.g. reserved parent pages). */
    while (first > state->relsize)
    {
        int zeros = (int) Min((uint64) first - state->relsize,
                              (uint64) PG_INT32_MAX);

        smgrzeroextend(state->smgr, state->forknum, state->relsize, zeros, true);
        state->relsize += zeros;
        state->stats.zero_extend_calls++;
        state->stats.zero_pages += zeros;
    }
    if (last >= state->relsize)
    {
        /* Core creates/opens the segment and owns all cached-length rules. */
        smgrextend(state->smgr, state->forknum, last, buffers[count - 1], true);
        state->relsize = last + 1;
        state->stats.extend_calls++;
        write_count--;
    }
    if (write_count > 0)
    {
        /* Core handles VFDs, EINTR, short writes, wait events and segments. */
        smgrwritev(state->smgr, state->forknum, first, buffers,
                    (BlockNumber) write_count, true);
        state->stats.writev_calls++;
        state->stats.writev_pages += write_count;
    }
    state->stats.runs++;
}

static void
pgdm_pw_flush(PgdmPhysicalWriteState *state)
{
    BlockNumber blocks[PGDM_PW_PENDING_PAGES];
    Page pages[PGDM_PW_PENDING_PAGES];
    const void *buffers[PGDM_PW_PENDING_PAGES];
    instr_time before_wal;
    instr_time after_wal;
    instr_time after_checksum;
    instr_time after_storage;
    uint64 before_records;
    int i;

    if (state->npending == 0)
        return;
    CHECK_FOR_INTERRUPTS();
    if (state->npending > 1)
        qsort(state->pending, state->npending, sizeof(PgdmPhysicalPage),
                pgdm_pw_page_order);
    for (i = 0; i < state->npending; i++)
    {
        blocks[i] = state->pending[i].block;
        pages[i] = state->pending[i].page;
        buffers[i] = pages[i];
        Assert(i == 0 || blocks[i - 1] < blocks[i]);
    }

    before_records = pgWalUsage.wal_records;
    INSTR_TIME_SET_CURRENT(before_wal);
    if (state->use_wal)
        log_newpages(&state->smgr->smgr_rlocator.locator, state->forknum,
                        state->npending, blocks, pages, true);
    INSTR_TIME_SET_CURRENT(after_wal);
    state->stats.wal_records += pgWalUsage.wal_records - before_records;

    /* Actual LSNs are assigned by log_newpages before the normal checksum. */
    for (i = 0; i < state->npending; i++)
        PageSetChecksumInplace(pages[i], blocks[i]);
    INSTR_TIME_SET_CURRENT(after_checksum);

    for (i = 0; i < state->npending;)
    {
        uint32 limit = Min((uint32) PG_IOV_MAX,
                            smgrmaxcombine(state->smgr, state->forknum, blocks[i]));
        int count = 1;

        while (i + count < state->npending && (uint32) count < limit &&
                (uint64) blocks[i + count] == (uint64) blocks[i] + count)
            count++;
        pgdm_pw_write_run(state, blocks[i], buffers + i, count);
        i += count;
    }
    for (i = 0; i < state->npending; i++)
        pfree(pages[i]);
    INSTR_TIME_SET_CURRENT(after_storage);

    state->stats.wal_ms += pgdm_pw_ms(before_wal, after_wal);
    state->stats.checksum_ms += pgdm_pw_ms(after_wal, after_checksum);
    state->stats.storage_ms += pgdm_pw_ms(after_checksum, after_storage);
    state->stats.flushes++;
    state->npending = 0;
}

/* Take ownership once; page memory is released after its normal storage write. */
static void
pgdm_pw_write(PgdmPhysicalWriteState *state, BlockNumber block, Page page)
{
    int n = state->npending;

    Assert(n < PGDM_PW_PENDING_PAGES);
    state->pending[n].block = block;
    state->pending[n].page = page;
    state->npending++;
    state->stats.pages++;
    if (state->npending == PGDM_PW_PENDING_PAGES)
        pgdm_pw_flush(state);
}

static void
pgdm_pw_finish(PgdmPhysicalWriteState *state)
{
    instr_time before_sync;
    instr_time after_sync;

    pgdm_pw_flush(state);
    INSTR_TIME_SET_CURRENT(before_sync);
    smgr_bulk_finish(state->finish_guard);
    INSTR_TIME_SET_CURRENT(after_sync);
    state->stats.finish_ms += pgdm_pw_ms(before_sync, after_sync);

    /* API-call counts are not syscall counts when core retries a short write. */
    ereport(LOG,
        (errmsg("pgdm physical writer {\"index_oid\":%u,"
                "\"backend\":\"pg18_smgr_vector_v1\",\"pages\":" UINT64_FORMAT ","
                "\"pending_limit\":%d,\"iov_limit\":%d,\"flushes\":" UINT64_FORMAT ","
                "\"wal_records\":" UINT64_FORMAT ",\"runs\":" UINT64_FORMAT ","
                "\"extend_calls\":" UINT64_FORMAT ",\"writev_calls\":" UINT64_FORMAT ","
                "\"writev_pages\":" UINT64_FORMAT ",\"zero_extend_calls\":" UINT64_FORMAT ","
                "\"zero_pages\":" UINT64_FORMAT ",\"wal_ms\":%.3f,\"checksum_ms\":%.3f,"
                "\"storage_ms\":%.3f,\"finish_ms\":%.3f}",
                state->index_oid, state->stats.pages,
                PGDM_PW_PENDING_PAGES, PG_IOV_MAX, state->stats.flushes,
                state->stats.wal_records, state->stats.runs,
                state->stats.extend_calls, state->stats.writev_calls,
                state->stats.writev_pages, state->stats.zero_extend_calls,
                state->stats.zero_pages, state->stats.wal_ms, state->stats.checksum_ms,
                state->stats.storage_ms, state->stats.finish_ms)));
}

#endif /* PostgreSQL 18 */
#endif /* PGDM_PHYSICAL_WRITER_INC */
