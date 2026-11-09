/*
 * pgdm_radix_twin.inc
 * Specialized table2 radix sort and two independent PostgreSQL Btree builds.
 *
 * Include this file once at the end of pgdm_bulk.c. PostgreSQL 16 and 18.
 * Writer derived from PostgreSQL REL_16_13:
 * src/backend/access/nbtree/nbtsort.c
 * blob c2665fce411af94913aaeb7b23ea78849656724a
 * Six private helper bodies are copied unchanged except private names.
 * blwritepage uses bounded multi-page WAL and sequential physical writes.
 * No tuplesort, deduplication, or parallel-sort machinery is copied.
 * PostgreSQL 18 uses bounded smgr vector page runs with core bulk finish,
 * based on REL_18_6 public storage contracts:
 * src/backend/storage/smgr/bulk_write.c
 * blob b958be15716452a84f153801c904a56f1e2ef151.
 * The normal core table_index_build_scan supplies visibility and real TIDs
 * on both majors; no custom visibility or hint-bit shortcut is used.
 *
 * PostgreSQL Database Management System
 * (also known as Postgres, formerly known as Postgres95)
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * Portions Copyright (c) 1994, The Regents of the University of California
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
 *
 */

#ifndef PGDM_RADIX_TWIN_INC
#define PGDM_RADIX_TWIN_INC

#include "postgres.h"
#include "fmgr.h"

/* Only the PostgreSQL 18 verified bundle calls these private helpers. */
#if PG_VERSION_NUM >= 180000 && PG_VERSION_NUM < 190000

#include "access/heapam.h"
#include "access/multixact.h"
#include "access/nbtree.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogrecord.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type_d.h"
#include "commands/defrem.h"
#include "commands/progress.h"
#include "commands/tablecmds.h"
#include "commands/vacuum.h"
#include "common/int.h"
#include "executor/executor.h"
#include "executor/instrument.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/fd.h"
#include "storage/smgr.h"
#if PG_VERSION_NUM >= 180000
#include "storage/bulk_write.h"
#include "pgdm_physical_writer.inc"
#endif
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/wait_event.h"


/* At most 1 MiB per contiguous buffer, including non-default block sizes. */
#define PGDM_RT_WRITE_BATCH_PAGES \
	((BLCKSZ <= 8192) ? 128 : (1024 * 1024 / BLCKSZ))

/* Exact PostgreSQL REL_16_13 md.c private layout; never used on other majors. */
#if PG_VERSION_NUM < 170000
struct _MdfdVec
{
	File		mdfd_vfd;
	BlockNumber mdfd_segno;
};

typedef struct PgdmRTQueuedPage
{
	BlockNumber block;
	Page		page;
} PgdmRTQueuedPage;
#endif

typedef struct PgdmRTIOStats
{
	uint64		pages;
	uint64		batches;
	uint64		wal_records;
	uint64		write_runs;
	uint64		page_writes;
	uint64		zero_extends;
	double		wal_ms;
	double		storage_ms;
	double		sync_ms;
} PgdmRTIOStats;

static double pgdm_rt_ms(instr_time begin, instr_time end);

/* Normal Btree page layout; writer storage branches are pinned per major. */
typedef struct PgdmRTPageState
{
	Page		btps_page;		/* workspace for page building */
	BlockNumber btps_blkno;		/* block # to write this page at */
	IndexTuple	btps_lowkey;	/* page's strict lower bound pivot tuple */
	OffsetNumber btps_lastoff;	/* last item offset loaded */
	Size		btps_lastextra; /* last item's extra posting list space */
	uint32		btps_level;		/* tree level (0 = leaf) */
	Size		btps_full;		/* "full" if less than this much free space */
	struct PgdmRTPageState *btps_next;	/* link to parent level, if any */
} PgdmRTPageState;

/*
 * Overall status record for index writing phase.
 */
typedef struct PgdmRTWriteState
{
	Relation	heap;
	Relation	index;
	BTScanInsert inskey;		/* generic insertion scankey */
	bool		btws_use_wal;	/* dump pages to WAL? */
	BlockNumber btws_pages_alloced; /* # pages allocated */
	BlockNumber btws_pages_written; /* # pages written out */
#if PG_VERSION_NUM < 170000
	Page		btws_zeropage;	/* workspace for filling zeroes */
	PgdmRTQueuedPage pending[PGDM_RT_WRITE_BATCH_PAGES];
	int			npending;
	char	   *write_buffer;
#else
	PgdmPhysicalWriteState *bulkstate; /* core WAL/smgr/finish, vector page runs */
#endif
	PgdmRTIOStats io;
} PgdmRTWriteState;



static Page pgdm_rt_blnewpage(PgdmRTWriteState *wstate, uint32 level);
static void pgdm_rt_blwritepage(PgdmRTWriteState *wstate, Page page, BlockNumber blkno);
static PgdmRTPageState *pgdm_rt_pagestate(PgdmRTWriteState *wstate, uint32 level);
static void pgdm_rt_slideleft(Page page);
static void pgdm_rt_sortaddtup(Page page, Size itemsize, IndexTuple itup, OffsetNumber itup_off, bool newfirstdataitem);
static void pgdm_rt_buildadd(PgdmRTWriteState *wstate, PgdmRTPageState *state, IndexTuple itup, Size truncextra);
static void pgdm_rt_uppershutdown(PgdmRTWriteState *wstate, PgdmRTPageState *state);

static Page
pgdm_rt_get_buf(PgdmRTWriteState *wstate)
{
#if PG_VERSION_NUM >= 180000
	return pgdm_pw_get_buf(wstate->bulkstate);
#else
	(void) wstate;
	return (Page) palloc_aligned(BLCKSZ, PG_IO_ALIGN_SIZE, 0);
#endif
}

static Page
pgdm_rt_blnewpage(PgdmRTWriteState *wstate, uint32 level)
{
	Page		page;
	BTPageOpaque opaque;

	page = pgdm_rt_get_buf(wstate);

	/* Zero the page and set up standard page header info */
	_bt_pageinit(page, BLCKSZ);

	/* Initialize BT opaque state */
	opaque = BTPageGetOpaque(page);
	opaque->btpo_prev = opaque->btpo_next = P_NONE;
	opaque->btpo_level = level;
	opaque->btpo_flags = (level > 0) ? 0 : BTP_LEAF;
	opaque->btpo_cycleid = 0;

	/* Make the P_HIKEY line pointer appear allocated */
	((PageHeader) page)->pd_lower += sizeof(ItemIdData);

	return page;
}

#if PG_VERSION_NUM < 170000
static int
pgdm_rt_page_order(const void *left, const void *right)
{
	BlockNumber a = ((const PgdmRTQueuedPage *) left)->block;
	BlockNumber b = ((const PgdmRTQueuedPage *) right)->block;
	return (a > b) - (a < b);
}

/*
 * Write a contiguous run contained in one PostgreSQL relation segment.
 * The first page always uses core smgr: it opens/creates the exact segment,
 * applies ordinary extension rules and owns the VFD for its whole lifetime.
 * Only the remaining already-aligned pages use the same VFD in one FileWrite.
 * This narrowly extends PG16's single-page mdwrite/mdextend path; no raw fd,
 * independent file lifecycle, altered WAL, or alternate fsync is introduced.
 */
static void
pgdm_rt_write_run(PgdmRTWriteState *wstate, BlockNumber first, int count)
{
	SMgrRelation reln = RelationGetSmgr(wstate->index);
	BlockNumber segment = first / (BlockNumber) RELSEG_SIZE;
	BlockNumber offset = first % (BlockNumber) RELSEG_SIZE;
	BlockNumber end;
	BlockNumber before;
	struct _MdfdVec *file;
	int			amount;
	int			written;
	uint32		wait_event;

	Assert(count > 0 && count <= PGDM_RT_WRITE_BATCH_PAGES);
	Assert(offset + count <= RELSEG_SIZE);
	Assert(reln->smgr_which == 0); /* PG16's sole built-in manager is md.c. */
	if ((uint64) first + (uint64) count >= (uint64) InvalidBlockNumber)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("table2 index exceeds PostgreSQL's block-number limit")));
	end = first + count;
	if (first > wstate->btws_pages_written)
	{
		/* Same unlogged all-zero holes as the original Btree bulk writer. */
		smgrzeroextend(reln, MAIN_FORKNUM, wstate->btws_pages_written,
					   first - wstate->btws_pages_written, true);
		wstate->btws_pages_written = first;
		wstate->io.zero_extends++;
	}
	before = wstate->btws_pages_written;
	if (first == before)
	{
		smgrextend(reln, MAIN_FORKNUM, first, wstate->write_buffer, true);
		wstate->btws_pages_written++;
	}
	else
		smgrwrite(reln, MAIN_FORKNUM, first, wstate->write_buffer, true);
	wstate->io.page_writes++;
	wstate->io.write_runs++;
	if (count == 1)
		return;

	/* The preceding core call has installed this segment's normal VFD. */
	if (segment >= (BlockNumber) reln->md_num_open_segs[MAIN_FORKNUM])
		elog(ERROR, "table2 bulk write segment was not opened by smgr");
	file = &reln->md_seg_fds[MAIN_FORKNUM][segment];
	if (file->mdfd_segno != segment)
		elog(ERROR, "unexpected PostgreSQL 16 storage segment layout");
	amount = (count - 1) * BLCKSZ;
	wait_event = end > before ? WAIT_EVENT_DATA_FILE_EXTEND :
		WAIT_EVENT_DATA_FILE_WRITE;
	written = FileWrite(file->mdfd_vfd, wstate->write_buffer + BLCKSZ,
					amount, (off_t) (offset + 1) * BLCKSZ, wait_event);
	wstate->io.page_writes++;
	if (written != amount)
	{
		if (written < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write table2 index block %u in file \"%s\": %m",
							first + 1, FilePathName(file->mdfd_vfd))));
		ereport(ERROR,
				(errcode(ERRCODE_DISK_FULL),
				 errmsg("could not write table2 index block %u in file \"%s\": wrote only %d of %d bytes",
						first + 1, FilePathName(file->mdfd_vfd), written, amount),
				 errhint("Check free disk space.")));
	}
	if (end > wstate->btws_pages_written)
	{
		/* Same cache rule as smgrextend: advance only a matching value. */
		if (reln->smgr_cached_nblocks[MAIN_FORKNUM] ==
			wstate->btws_pages_written)
			reln->smgr_cached_nblocks[MAIN_FORKNUM] = end;
		else
			reln->smgr_cached_nblocks[MAIN_FORKNUM] = InvalidBlockNumber;
		wstate->btws_pages_written = end;
	}
}

static void
pgdm_rt_flush_pages(PgdmRTWriteState *wstate)
{
	BlockNumber blocks[PGDM_RT_WRITE_BATCH_PAGES];
	Page		pages[PGDM_RT_WRITE_BATCH_PAGES];
	instr_time start;
	instr_time after_wal;
	instr_time finished;
	int			i;

	if (wstate->npending == 0)
		return;
	CHECK_FOR_INTERRUPTS();
	qsort(wstate->pending, wstate->npending, sizeof(PgdmRTQueuedPage),
		  pgdm_rt_page_order);
	for (i = 0; i < wstate->npending; i++)
	{
		blocks[i] = wstate->pending[i].block;
		pages[i] = wstate->pending[i].page;
		Assert(i == 0 || blocks[i] > blocks[i - 1]);
	}
	INSTR_TIME_SET_CURRENT(start);
	if (wstate->btws_use_wal)
	{
		/* Core emits one normal FPI record per up-to-32 page group. */
		log_newpages(&wstate->index->rd_locator, MAIN_FORKNUM,
					 wstate->npending, blocks, pages, true);
		wstate->io.wal_records +=
			(wstate->npending + XLR_MAX_BLOCK_ID - 1) / XLR_MAX_BLOCK_ID;
	}
	INSTR_TIME_SET_CURRENT(after_wal);
	for (i = 0; i < wstate->npending;)
	{
		int			count = 1;
		int			j;
		BlockNumber first = blocks[i];
		BlockNumber segment_left = RELSEG_SIZE - first % RELSEG_SIZE;

		while (i + count < wstate->npending &&
			   (BlockNumber) count < segment_left &&
			   blocks[i + count] == first + count)
			count++;
		for (j = 0; j < count; j++)
		{
			/* log_newpages set real LSNs before the normal page checksum. */
			PageSetChecksumInplace(pages[i + j], blocks[i + j]);
			memcpy(wstate->write_buffer + (Size) j * BLCKSZ,
				   pages[i + j], BLCKSZ);
		}
		pgdm_rt_write_run(wstate, first, count);
		for (j = 0; j < count; j++)
			pfree(pages[i + j]);
		i += count;
	}
	INSTR_TIME_SET_CURRENT(finished);
	wstate->io.wal_ms += pgdm_rt_ms(start, after_wal);
	wstate->io.storage_ms += pgdm_rt_ms(after_wal, finished);
	wstate->io.batches++;
	wstate->npending = 0;
}

/* Accept ownership of a completed page; release it after the bounded flush. */
static void
pgdm_rt_blwritepage(PgdmRTWriteState *wstate, Page page, BlockNumber blkno)
{
	int			n;

	/* Keep the valid metapage physically last, as in the original writer. */
	if (blkno == BTREE_METAPAGE)
		pgdm_rt_flush_pages(wstate);
	n = wstate->npending;
	Assert(n < PGDM_RT_WRITE_BATCH_PAGES);
	wstate->pending[n].block = blkno;
	wstate->pending[n].page = page;
	wstate->npending++;
	wstate->io.pages++;
	if (wstate->npending == PGDM_RT_WRITE_BATCH_PAGES ||
		blkno == BTREE_METAPAGE)
		pgdm_rt_flush_pages(wstate);
}
#else
/*
 * PG18 pages use the bounded physical writer.  Core still owns WAL records,
 * checksum calculation, storage APIs and unchanged bulk-finish durability.
 * Only our own page allocations are transferred to this private queue.
 */
static void
pgdm_rt_blwritepage(PgdmRTWriteState *wstate, Page page, BlockNumber blkno)
{
	pgdm_pw_write(wstate->bulkstate, blkno, page);
	wstate->btws_pages_written = Max(wstate->btws_pages_written, blkno + 1);
}
#endif

/*
 * allocate and initialize a new PgdmRTPageState.  the returned structure
 * is suitable for immediate use by pgdm_rt_buildadd.
 */
static PgdmRTPageState *
pgdm_rt_pagestate(PgdmRTWriteState *wstate, uint32 level)
{
	PgdmRTPageState *state = (PgdmRTPageState *) palloc0(sizeof(PgdmRTPageState));

	/* create initial page for level */
	state->btps_page = pgdm_rt_blnewpage(wstate, level);

	/* and assign it a page position */
	state->btps_blkno = wstate->btws_pages_alloced++;

	state->btps_lowkey = NULL;
	/* initialize lastoff so first item goes into P_FIRSTKEY */
	state->btps_lastoff = P_HIKEY;
	state->btps_lastextra = 0;
	state->btps_level = level;
	/* set "full" threshold based on level.  See notes at head of file. */
	if (level > 0)
		state->btps_full = (BLCKSZ * (100 - BTREE_NONLEAF_FILLFACTOR) / 100);
	else
		state->btps_full = BTGetTargetPageFreeSpace(wstate->index);

	/* no parent level, yet */
	state->btps_next = NULL;

	return state;
}

/*
 * Slide the array of ItemIds from the page back one slot (from P_FIRSTKEY to
 * P_HIKEY, overwriting P_HIKEY).
 *
 * pgdm_rt_blnewpage() makes the P_HIKEY line pointer appear allocated, but the
 * rightmost page on its level is not supposed to get a high key.  Now that
 * it's clear that this page is a rightmost page, remove the unneeded empty
 * P_HIKEY line pointer space.
 */
static void
pgdm_rt_slideleft(Page rightmostpage)
{
	OffsetNumber off;
	OffsetNumber maxoff;
	ItemId		previi;

	maxoff = PageGetMaxOffsetNumber(rightmostpage);
	Assert(maxoff >= P_FIRSTKEY);
	previi = PageGetItemId(rightmostpage, P_HIKEY);
	for (off = P_FIRSTKEY; off <= maxoff; off = OffsetNumberNext(off))
	{
		ItemId		thisii = PageGetItemId(rightmostpage, off);

		*previi = *thisii;
		previi = thisii;
	}
	((PageHeader) rightmostpage)->pd_lower -= sizeof(ItemIdData);
}

/*
 * Add an item to a page being built.
 *
 * This is very similar to nbtinsert.c's _bt_pgaddtup(), but this variant
 * raises an error directly.
 *
 * Note that our nbtsort.c caller does not know yet if the page will be
 * rightmost.  Offset P_FIRSTKEY is always assumed to be the first data key by
 * caller.  Page that turns out to be the rightmost on its level is fixed by
 * calling pgdm_rt_slideleft().
 */
static void
pgdm_rt_sortaddtup(Page page,
			   Size itemsize,
			   IndexTuple itup,
			   OffsetNumber itup_off,
			   bool newfirstdataitem)
{
	IndexTupleData trunctuple;

	if (newfirstdataitem)
	{
		trunctuple = *itup;
		trunctuple.t_info = sizeof(IndexTupleData);
		BTreeTupleSetNAtts(&trunctuple, 0, false);
		itup = &trunctuple;
		itemsize = sizeof(IndexTupleData);
	}

	if (PageAddItem(page, (Item) itup, itemsize, itup_off,
					false, false) == InvalidOffsetNumber)
		elog(ERROR, "failed to add item to the index page");
}

/*----------
 * Add an item to a disk page from the sort output (or add a posting list
 * item formed from the sort output).
 *
 * We must be careful to observe the page layout conventions of nbtsearch.c:
 * - rightmost pages start data items at P_HIKEY instead of at P_FIRSTKEY.
 * - on non-leaf pages, the key portion of the first item need not be
 *	 stored, we should store only the link.
 *
 * A leaf page being built looks like:
 *
 * +----------------+---------------------------------+
 * | PageHeaderData | linp0 linp1 linp2 ...           |
 * +-----------+----+---------------------------------+
 * | ... linpN |									  |
 * +-----------+--------------------------------------+
 * |	 ^ last										  |
 * |												  |
 * +-------------+------------------------------------+
 * |			 | itemN ...                          |
 * +-------------+------------------+-----------------+
 * |		  ... item3 item2 item1 | "special space" |
 * +--------------------------------+-----------------+
 *
 * Contrast this with the diagram in bufpage.h; note the mismatch
 * between linps and items.  This is because we reserve linp0 as a
 * placeholder for the pointer to the "high key" item; when we have
 * filled up the page, we will set linp0 to point to itemN and clear
 * linpN.  On the other hand, if we find this is the last (rightmost)
 * page, we leave the items alone and slide the linp array over.  If
 * the high key is to be truncated, offset 1 is deleted, and we insert
 * the truncated high key at offset 1.
 *
 * 'last' pointer indicates the last offset added to the page.
 *
 * 'truncextra' is the size of the posting list in itup, if any.  This
 * information is stashed for the next call here, when we may benefit
 * from considering the impact of truncating away the posting list on
 * the page before deciding to finish the page off.  Posting lists are
 * often relatively large, so it is worth going to the trouble of
 * accounting for the saving from truncating away the posting list of
 * the tuple that becomes the high key (that may be the only way to
 * get close to target free space on the page).  Note that this is
 * only used for the soft fillfactor-wise limit, not the critical hard
 * limit.
 *----------
 */
static void
pgdm_rt_buildadd(PgdmRTWriteState *wstate, PgdmRTPageState *state, IndexTuple itup,
			 Size truncextra)
{
	Page		npage;
	BlockNumber nblkno;
	OffsetNumber last_off;
	Size		last_truncextra;
	Size		pgspc;
	Size		itupsz;
	bool		isleaf;

	/*
	 * This is a handy place to check for cancel interrupts during the btree
	 * load phase of index creation.
	 */
	CHECK_FOR_INTERRUPTS();

	npage = state->btps_page;
	nblkno = state->btps_blkno;
	last_off = state->btps_lastoff;
	last_truncextra = state->btps_lastextra;
	state->btps_lastextra = truncextra;

	pgspc = PageGetFreeSpace(npage);
	itupsz = IndexTupleSize(itup);
	itupsz = MAXALIGN(itupsz);
	/* Leaf case has slightly different rules due to suffix truncation */
	isleaf = (state->btps_level == 0);

	/*
	 * Check whether the new item can fit on a btree page on current level at
	 * all.
	 *
	 * Every newly built index will treat heap TID as part of the keyspace,
	 * which imposes the requirement that new high keys must occasionally have
	 * a heap TID appended within _bt_truncate().  That may leave a new pivot
	 * tuple one or two MAXALIGN() quantums larger than the original
	 * firstright tuple it's derived from.  v4 deals with the problem by
	 * decreasing the limit on the size of tuples inserted on the leaf level
	 * by the same small amount.  Enforce the new v4+ limit on the leaf level,
	 * and the old limit on internal levels, since pivot tuples may need to
	 * make use of the reserved space.  This should never fail on internal
	 * pages.
	 */
#if PG_VERSION_NUM >= 180000
	if (unlikely(itupsz > BTMaxItemSize))
#else
	if (unlikely(itupsz > BTMaxItemSize(npage)))
#endif
		_bt_check_third_page(wstate->index, wstate->heap, isleaf, npage,
							 itup);

	/*
	 * Check to see if current page will fit new item, with space left over to
	 * append a heap TID during suffix truncation when page is a leaf page.
	 *
	 * It is guaranteed that we can fit at least 2 non-pivot tuples plus a
	 * high key with heap TID when finishing off a leaf page, since we rely on
	 * _bt_check_third_page() rejecting oversized non-pivot tuples.  On
	 * internal pages we can always fit 3 pivot tuples with larger internal
	 * page tuple limit (includes page high key).
	 *
	 * Most of the time, a page is only "full" in the sense that the soft
	 * fillfactor-wise limit has been exceeded.  However, we must always leave
	 * at least two items plus a high key on each page before starting a new
	 * page.  Disregard fillfactor and insert on "full" current page if we
	 * don't have the minimum number of items yet.  (Note that we deliberately
	 * assume that suffix truncation neither enlarges nor shrinks new high key
	 * when applying soft limit, except when last tuple has a posting list.)
	 */
	Assert(last_truncextra == 0 || isleaf);
	if (pgspc < itupsz + (isleaf ? MAXALIGN(sizeof(ItemPointerData)) : 0) ||
		(pgspc + last_truncextra < state->btps_full && last_off > P_FIRSTKEY))
	{
		/*
		 * Finish off the page and write it out.
		 */
		Page		opage = npage;
		BlockNumber oblkno = nblkno;
		ItemId		ii;
		ItemId		hii;
		IndexTuple	oitup;

		/* Create new page of same level */
		npage = pgdm_rt_blnewpage(wstate, state->btps_level);

		/* and assign it a page position */
		nblkno = wstate->btws_pages_alloced++;

		/*
		 * We copy the last item on the page into the new page, and then
		 * rearrange the old page so that the 'last item' becomes its high key
		 * rather than a true data item.  There had better be at least two
		 * items on the page already, else the page would be empty of useful
		 * data.
		 */
		Assert(last_off > P_FIRSTKEY);
		ii = PageGetItemId(opage, last_off);
		oitup = (IndexTuple) PageGetItem(opage, ii);
		pgdm_rt_sortaddtup(npage, ItemIdGetLength(ii), oitup, P_FIRSTKEY,
					   !isleaf);

		/*
		 * Move 'last' into the high key position on opage.  pgdm_rt_blnewpage()
		 * allocated empty space for a line pointer when opage was first
		 * created, so this is a matter of rearranging already-allocated space
		 * on page, and initializing high key line pointer. (Actually, leaf
		 * pages must also swap oitup with a truncated version of oitup, which
		 * is sometimes larger than oitup, though never by more than the space
		 * needed to append a heap TID.)
		 */
		hii = PageGetItemId(opage, P_HIKEY);
		*hii = *ii;
		ItemIdSetUnused(ii);	/* redundant */
		((PageHeader) opage)->pd_lower -= sizeof(ItemIdData);

		if (isleaf)
		{
			IndexTuple	lastleft;
			IndexTuple	truncated;

			/*
			 * Truncate away any unneeded attributes from high key on leaf
			 * level.  This is only done at the leaf level because downlinks
			 * in internal pages are either negative infinity items, or get
			 * their contents from copying from one level down.  See also:
			 * _bt_split().
			 *
			 * We don't try to bias our choice of split point to make it more
			 * likely that _bt_truncate() can truncate away more attributes,
			 * whereas the split point used within _bt_split() is chosen much
			 * more delicately.  Even still, the lastleft and firstright
			 * tuples passed to _bt_truncate() here are at least not fully
			 * equal to each other when deduplication is used, unless there is
			 * a large group of duplicates (also, unique index builds usually
			 * have few or no spool2 duplicates).  When the split point is
			 * between two unequal tuples, _bt_truncate() will avoid including
			 * a heap TID in the new high key, which is the most important
			 * benefit of suffix truncation.
			 *
			 * Overwrite the old item with new truncated high key directly.
			 * oitup is already located at the physical beginning of tuple
			 * space, so this should directly reuse the existing tuple space.
			 */
			ii = PageGetItemId(opage, OffsetNumberPrev(last_off));
			lastleft = (IndexTuple) PageGetItem(opage, ii);

			Assert(IndexTupleSize(oitup) > last_truncextra);
			truncated = _bt_truncate(wstate->index, lastleft, oitup,
									 wstate->inskey);
			if (!PageIndexTupleOverwrite(opage, P_HIKEY, (Item) truncated,
										 IndexTupleSize(truncated)))
				elog(ERROR, "failed to add high key to the index page");
			pfree(truncated);

			/* oitup should continue to point to the page's high key */
			hii = PageGetItemId(opage, P_HIKEY);
			oitup = (IndexTuple) PageGetItem(opage, hii);
		}

		/*
		 * Link the old page into its parent, using its low key.  If we don't
		 * have a parent, we have to create one; this adds a new btree level.
		 */
		if (state->btps_next == NULL)
			state->btps_next = pgdm_rt_pagestate(wstate, state->btps_level + 1);

		Assert((BTreeTupleGetNAtts(state->btps_lowkey, wstate->index) <=
				IndexRelationGetNumberOfKeyAttributes(wstate->index) &&
				BTreeTupleGetNAtts(state->btps_lowkey, wstate->index) > 0) ||
			   P_LEFTMOST(BTPageGetOpaque(opage)));
		Assert(BTreeTupleGetNAtts(state->btps_lowkey, wstate->index) == 0 ||
			   !P_LEFTMOST(BTPageGetOpaque(opage)));
		BTreeTupleSetDownLink(state->btps_lowkey, oblkno);
		pgdm_rt_buildadd(wstate, state->btps_next, state->btps_lowkey, 0);
		pfree(state->btps_lowkey);

		/*
		 * Save a copy of the high key from the old page.  It is also the low
		 * key for the new page.
		 */
		state->btps_lowkey = CopyIndexTuple(oitup);

		/*
		 * Set the sibling links for both pages.
		 */
		{
			BTPageOpaque oopaque = BTPageGetOpaque(opage);
			BTPageOpaque nopaque = BTPageGetOpaque(npage);

			oopaque->btpo_next = nblkno;
			nopaque->btpo_prev = oblkno;
			nopaque->btpo_next = P_NONE;	/* redundant */
		}

		/*
		 * Write out the old page.  We never need to touch it again, so we can
		 * free the opage workspace too.
		 */
		pgdm_rt_blwritepage(wstate, opage, oblkno);

		/*
		 * Reset last_off to point to new page
		 */
		last_off = P_FIRSTKEY;
	}

	/*
	 * By here, either original page is still the current page, or a new page
	 * was created that became the current page.  Either way, the current page
	 * definitely has space for new item.
	 *
	 * If the new item is the first for its page, it must also be the first
	 * item on its entire level.  On later same-level pages, a low key for a
	 * page will be copied from the prior page in the code above.  Generate a
	 * minus infinity low key here instead.
	 */
	if (last_off == P_HIKEY)
	{
		Assert(state->btps_lowkey == NULL);
		state->btps_lowkey = palloc0(sizeof(IndexTupleData));
		state->btps_lowkey->t_info = sizeof(IndexTupleData);
		BTreeTupleSetNAtts(state->btps_lowkey, 0, false);
	}

	/*
	 * Add the new item into the current page.
	 */
	last_off = OffsetNumberNext(last_off);
	pgdm_rt_sortaddtup(npage, itupsz, itup, last_off,
				   !isleaf && last_off == P_FIRSTKEY);

	state->btps_page = npage;
	state->btps_blkno = nblkno;
	state->btps_lastoff = last_off;
}

static void
pgdm_rt_uppershutdown(PgdmRTWriteState *wstate, PgdmRTPageState *state)
{
	PgdmRTPageState *s;
	BlockNumber rootblkno = P_NONE;
	uint32		rootlevel = 0;
	Page		metapage;

	/*
	 * Each iteration of this loop completes one more level of the tree.
	 */
	for (s = state; s != NULL; s = s->btps_next)
	{
		BlockNumber blkno;
		BTPageOpaque opaque;

		blkno = s->btps_blkno;
		opaque = BTPageGetOpaque(s->btps_page);

		/*
		 * We have to link the last page on this level to somewhere.
		 *
		 * If we're at the top, it's the root, so attach it to the metapage.
		 * Otherwise, add an entry for it to its parent using its low key.
		 * This may cause the last page of the parent level to split, but
		 * that's not a problem -- we haven't gotten to it yet.
		 */
		if (s->btps_next == NULL)
		{
			opaque->btpo_flags |= BTP_ROOT;
			rootblkno = blkno;
			rootlevel = s->btps_level;
		}
		else
		{
			Assert((BTreeTupleGetNAtts(s->btps_lowkey, wstate->index) <=
					IndexRelationGetNumberOfKeyAttributes(wstate->index) &&
					BTreeTupleGetNAtts(s->btps_lowkey, wstate->index) > 0) ||
				   P_LEFTMOST(opaque));
			Assert(BTreeTupleGetNAtts(s->btps_lowkey, wstate->index) == 0 ||
				   !P_LEFTMOST(opaque));
			BTreeTupleSetDownLink(s->btps_lowkey, blkno);
			pgdm_rt_buildadd(wstate, s->btps_next, s->btps_lowkey, 0);
			pfree(s->btps_lowkey);
			s->btps_lowkey = NULL;
		}

		/*
		 * This is the rightmost page, so the ItemId array needs to be slid
		 * back one slot.  Then we can dump out the page.
		 */
		pgdm_rt_slideleft(s->btps_page);
		pgdm_rt_blwritepage(wstate, s->btps_page, s->btps_blkno);
		s->btps_page = NULL;	/* writepage freed the workspace */
	}

	/*
	 * As the last step in the process, construct the metapage and make it
	 * point to the new root (unless we had no data at all, in which case it's
	 * set to point to "P_NONE").  This changes the index to the "valid" state
	 * by filling in a valid magic number in the metapage.
	 */
	metapage = pgdm_rt_get_buf(wstate);
	_bt_initmetapage(metapage, rootblkno, rootlevel,
					 wstate->inskey->allequalimage);
	pgdm_rt_blwritepage(wstate, metapage, BTREE_METAPAGE);
}

/* END PostgreSQL normal Btree page writer. */


#define PGDM_RT_WRITER_RESERVE (UINT64CONST(8) * 1024 * 1024)
#define PGDM_RT_SIGN_BIT UINT64CONST(0x8000000000000000)

/* A live heap tuple contributes exactly sixteen bytes, including the TID. */
typedef struct PgdmRTRow
{
	int64		id;
	ItemPointerData tid;
	uint16		padding;
} PgdmRTRow;

StaticAssertDecl(sizeof(PgdmRTRow) == 16, "radix row must be sixteen bytes");

typedef struct PgdmRTScan
{
	PgdmRTRow *rows;
	uint64		capacity;
	uint64		count;
	uint64		first_key;
	uint64		varying_bits;
} PgdmRTScan;

static double
pgdm_rt_ms(instr_time begin, instr_time end)
{
	INSTR_TIME_SUBTRACT(end, begin);
	return INSTR_TIME_GET_MILLISEC(end);
}

/*
 * Stable LSD byte radix. Histograms for all significant bytes are gathered in
 * one read of the compact entries. Uniform bytes require no scatter pass.
 * The actual observed differing bits bound the passes, not an ID-density guess.
 */
static PgdmRTRow *
pgdm_rt_sort(PgdmRTScan *scan, PgdmRTRow *scratch, int *passes)
{
	uint64		counts[8][256];
	uint64		offsets[256];
	PgdmRTRow *src = scan->rows;
	PgdmRTRow *dst = scratch;
	uint64		i;
	int			bytes = 0;
	int			b;
	int			bucket;

	*passes = 0;
	if (scan->count < 2 || scan->varying_bits == 0)
		return src;
	while (bytes < 8 && (scan->varying_bits >> (bytes * 8)) != 0)
		bytes++;

	memset(counts, 0, sizeof(counts));
	for (i = 0; i < scan->count; i++)
	{
		uint64 key = ((uint64) src[i].id) ^ PGDM_RT_SIGN_BIT;

		for (b = 0; b < bytes; b++)
			counts[b][(key >> (b * 8)) & 255]++;
		if ((i & 65535) == 0)
			CHECK_FOR_INTERRUPTS();
	}

	for (b = 0; b < bytes; b++)
	{
		uint64 sum = 0;
		bool uniform = false;
		PgdmRTRow *tmp;

		for (bucket = 0; bucket < 256; bucket++)
		{
			if (counts[b][bucket] == scan->count)
				uniform = true;
			offsets[bucket] = sum;
			sum += counts[b][bucket];
		}
		if (uniform)
			continue;
		for (i = 0; i < scan->count; i++)
		{
			uint64 key = ((uint64) src[i].id) ^ PGDM_RT_SIGN_BIT;

			dst[offsets[(key >> (b * 8)) & 255]++] = src[i];
			if ((i & 65535) == 0)
				CHECK_FOR_INTERRUPTS();
		}
		tmp = src;
		src = dst;
		dst = tmp;
		(*passes)++;
	}
	return src;
}

static void
pgdm_rt_writer_init(PgdmRTWriteState *writer, Relation heap, Relation index)
{
	memset(writer, 0, sizeof(*writer));
	writer->heap = heap;
	writer->index = index;
	writer->inskey = _bt_mkscankey(index, NULL);
	writer->inskey->allequalimage = _bt_allequalimage(index, true);
	writer->btws_use_wal = RelationNeedsWAL(index);
	writer->btws_pages_alloced = BTREE_METAPAGE + 1;
#if PG_VERSION_NUM >= 180000
	writer->bulkstate = pgdm_pw_start_rel(index, MAIN_FORKNUM);
#else
	writer->write_buffer = palloc_aligned(
		(Size) PGDM_RT_WRITE_BATCH_PAGES * BLCKSZ, PG_IO_ALIGN_SIZE, 0);
#endif
}

static void
pgdm_rt_writer_finish(PgdmRTWriteState *writer, PgdmRTPageState *leaf)
{
	PgdmRTPageState *state;
#if PG_VERSION_NUM < 170000
	instr_time sync_start;
	instr_time sync_end;
#endif

	pgdm_rt_uppershutdown(writer, leaf);
#if PG_VERSION_NUM >= 180000
	pgdm_pw_finish(writer->bulkstate);
	writer->io.pages = writer->bulkstate->stats.pages;
	writer->io.batches = writer->bulkstate->stats.flushes;
	writer->io.wal_records = writer->bulkstate->stats.wal_records;
	writer->io.write_runs = writer->bulkstate->stats.runs;
	writer->io.page_writes = writer->bulkstate->stats.extend_calls +
		writer->bulkstate->stats.writev_calls;
	writer->io.zero_extends = writer->bulkstate->stats.zero_extend_calls;
	writer->io.wal_ms = writer->bulkstate->stats.wal_ms;
	writer->io.storage_ms = writer->bulkstate->stats.checksum_ms +
		writer->bulkstate->stats.storage_ms;
	writer->io.sync_ms = writer->bulkstate->stats.finish_ms;
	pfree(writer->bulkstate);
	writer->bulkstate = NULL;
#else
	pgdm_rt_flush_pages(writer);
	/* Same mandatory WAL-build fsync as core _bt_load. */
	INSTR_TIME_SET_CURRENT(sync_start);
	if (writer->btws_use_wal)
		smgrimmedsync(RelationGetSmgr(writer->index), MAIN_FORKNUM);
	INSTR_TIME_SET_CURRENT(sync_end);
	writer->io.sync_ms += pgdm_rt_ms(sync_start, sync_end);
	pfree(writer->write_buffer);
	writer->write_buffer = NULL;
	if (writer->btws_zeropage)
		pfree(writer->btws_zeropage);
#endif
	pfree(writer->inskey);
	while (leaf)
	{
		state = leaf->btps_next;
		pfree(leaf);
		leaf = state;
	}
}


/* Fixed, nonnull tuple sizes from each real index descriptor on PG16. */
#define PGDM_RT_PK_TUPLE_BYTES \
	MAXALIGN(sizeof(IndexTupleData) + sizeof(int64))
#define PGDM_RT_UQ_TUPLE_BYTES \
	MAXALIGN(sizeof(IndexTupleData) + sizeof(int64) + sizeof(int16))

typedef struct PgdmRTLeafStats
{
	uint64		batched_rows;	/* each row appends to both leaf pages */
	uint64		core_rows;		/* each row uses both normal buildadd calls */
	uint64		batches;
} PgdmRTLeafStats;

/*
 * Count consecutive inserts that the original buildadd would keep on this leaf.
 * First tuples, copied-page transitions and every split stay in buildadd.
 *
 * For an established leaf, before insert j:
 *
 *   PageGetFreeSpace_j = free_0 - j * (tuple_bytes + sizeof(ItemIdData)).
 *
 * Both original boundary predicates are false exactly while this value is at
 * least max(btps_full, tuple_bytes + MAXALIGN(sizeof(ItemPointerData))).
 * The +1 below admits the final insert at equality, as core does.  No page
 * threshold is rounded or weakened.  lastoff > P_FIRSTKEY makes the core soft
 * threshold applicable to every tuple in the returned run.
 */
static uint32
pgdm_rt_leaf_run(PgdmRTPageState *state, Size tuple_bytes)
{
	PageHeader	header;
	Size		free_space;
	Size		threshold;

	if (state->btps_level != 0 ||
		state->btps_lastextra != 0 ||
		state->btps_lastoff <= P_FIRSTKEY)
		return 0;

	header = (PageHeader) state->btps_page;
	/* Same pointer validity conditions as PageAddItemExtended, once per run. */
	if (header->pd_lower < SizeOfPageHeaderData ||
		header->pd_lower > header->pd_upper ||
		header->pd_upper > header->pd_special ||
		header->pd_special > BLCKSZ)
		ereport(PANIC,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("corrupted page pointers: lower = %u, upper = %u, special = %u",
						header->pd_lower, header->pd_upper,
						header->pd_special)));
	if (state->btps_lastoff != PageGetMaxOffsetNumber(state->btps_page))
		return 0;

	Assert(tuple_bytes == MAXALIGN(tuple_bytes));
	free_space = PageGetFreeSpace(state->btps_page);
	threshold = Max(state->btps_full,
					tuple_bytes + MAXALIGN(sizeof(ItemPointerData)));
	if (free_space < threshold)
		return 0;
	return (uint32) (1 + (free_space - threshold) /
					 (tuple_bytes + sizeof(ItemIdData)));
}

/* Called only on the exceptional duplicate/nonmonotonic path. */
static void
pgdm_rt_bad_sorted_key(int64 previous, int64 current)
{
	if (previous == current)
		ereport(ERROR,
				(errcode(ERRCODE_UNIQUE_VIOLATION),
				 errmsg("duplicate key value violates table2 unique indexes"),
				 errdetail("Key (id_column_1)=(" INT64_FORMAT ") is duplicated.",
						   current)));
	elog(ERROR, "table2 radix sort produced nonmonotonic keys");
}

/*
 * Each tree has its own correctly formed leaf tuples, pivots, high keys,
 * sibling links, metapage and file. No pages are shared or layout-rewritten.
 */
static void
pgdm_rt_write_trees(Relation heap, Relation pk, Relation uq,
					 PgdmRTRow *rows, uint64 count,
					 BlockNumber *pk_pages, BlockNumber *uq_pages,
					 PgdmRTLeafStats *leaf_stats, PgdmRTIOStats *io_stats)
{
	PgdmRTWriteState pk_writer;
	PgdmRTWriteState uq_writer;
	PgdmRTPageState *pk_leaf = NULL;
	PgdmRTPageState *uq_leaf = NULL;
	Datum		values[2] = {Int64GetDatum(0), Int16GetDatum(1)};
	bool		nulls[2] = {false, false};
	IndexTuple	pk_tuple;
	IndexTuple	uq_tuple;
	char	   *pk_data;
	char	   *uq_data;
	uint64		i = 0;
	uint64		next_report = 0;

	memset(leaf_stats, 0, sizeof(*leaf_stats));
	pgdm_rt_writer_init(&pk_writer, heap, pk);
	pgdm_rt_writer_init(&uq_writer, heap, uq);

	/*
	 * Core forms the real fixed-width headers and initializes tuple padding.
	 * The fast loop copies exactly these tuple layouts, including real TIDs.
	 */
	pk_tuple = index_form_tuple(RelationGetDescr(pk), values, nulls);
	uq_tuple = index_form_tuple(RelationGetDescr(uq), values, nulls);
	if (IndexTupleSize(pk_tuple) != PGDM_RT_PK_TUPLE_BYTES ||
		IndexTupleSize(uq_tuple) != PGDM_RT_UQ_TUPLE_BYTES)
		elog(ERROR, "unexpected table2 index tuple layout");
	pk_data = ((char *) pk_tuple) + IndexInfoFindDataOffset(pk_tuple->t_info);
	uq_data = ((char *) uq_tuple) + IndexInfoFindDataOffset(uq_tuple->t_info);

	if (count > 0)
	{
		pk_leaf = pgdm_rt_pagestate(&pk_writer, 0);
		uq_leaf = pgdm_rt_pagestate(&uq_writer, 0);
	}

	while (i < count)
	{
		uint32 pk_run;
		uint32 uq_run;
		uint32 run;

		CHECK_FOR_INTERRUPTS();
		pk_run = pgdm_rt_leaf_run(pk_leaf, PGDM_RT_PK_TUPLE_BYTES);
		uq_run = pgdm_rt_leaf_run(uq_leaf, PGDM_RT_UQ_TUPLE_BYTES);
		run = Min(pk_run, uq_run);
		if ((uint64) run > count - i)
			run = (uint32) (count - i);

		if (run == 0)
		{
			int64 raw;
			int16 pos;

			if (unlikely(i > 0 && rows[i - 1].id >= rows[i].id))
				pgdm_rt_bad_sorted_key(rows[i - 1].id, rows[i].id);
			raw = (rows[i].id - 1) / 14 + 1;
			pos = (int16) ((rows[i].id - 1) % 14 + 1);
			ItemPointerCopy(&rows[i].tid, &pk_tuple->t_tid);
			ItemPointerCopy(&rows[i].tid, &uq_tuple->t_tid);
			memcpy(pk_data, &rows[i].id, sizeof(int64));
			memcpy(uq_data, &raw, sizeof(int64));
			memcpy(uq_data + sizeof(int64), &pos, sizeof(int16));

			/*
			 * Exact original first-tuple, boundary, split, truncation,
			 * parent-page, WAL and storage paths for both trees.
			 */
			pgdm_rt_buildadd(&pk_writer, pk_leaf, pk_tuple, 0);
			pgdm_rt_buildadd(&uq_writer, uq_leaf, uq_tuple, 0);
			leaf_stats->core_rows++;
			i++;
		}
		else
		{
			PageHeader pk_header = (PageHeader) pk_leaf->btps_page;
			PageHeader uq_header = (PageHeader) uq_leaf->btps_page;
			int pk_upper = pk_header->pd_upper;
			int uq_upper = uq_header->pd_upper;
			OffsetNumber pk_off = pk_leaf->btps_lastoff;
			OffsetNumber uq_off = uq_leaf->btps_lastoff;
			uint64 end = i + run;
			uint64 j;

			/*
			 * A real page batch: no per-tuple PageGetFreeSpace,
			 * PageAddItem, page-header update or generic buildadd.
			 * pgdm_rt_leaf_run proved the original pre-insert bounds
			 * for every j in this run. The pages are private workspaces.
			 */
			for (j = i; j < end; j++)
			{
				int64 raw;
				int16 pos;

				/* Actual-row mapping was checked in the heap scan. */
				if (unlikely(j > 0 && rows[j - 1].id >= rows[j].id))
					pgdm_rt_bad_sorted_key(rows[j - 1].id, rows[j].id);
				raw = (rows[j].id - 1) / 14 + 1;
				pos = (int16) ((rows[j].id - 1) % 14 + 1);
				ItemPointerCopy(&rows[j].tid, &pk_tuple->t_tid);
				ItemPointerCopy(&rows[j].tid, &uq_tuple->t_tid);
				memcpy(pk_data, &rows[j].id, sizeof(int64));
				memcpy(uq_data, &raw, sizeof(int64));
				memcpy(uq_data + sizeof(int64), &pos, sizeof(int16));

				/*
				 * PageAddItemExtended's explicit append case:
				 * no overwrite, no hole search, no line-pointer shuffle.
				 * Both sizes are MAXALIGNed constants (16/24 on GB10).
				 */
				pk_upper -= PGDM_RT_PK_TUPLE_BYTES;
				uq_upper -= PGDM_RT_UQ_TUPLE_BYTES;
				pk_off = OffsetNumberNext(pk_off);
				uq_off = OffsetNumberNext(uq_off);
				ItemIdSetNormal(&pk_header->pd_linp[pk_off - 1],
								pk_upper, PGDM_RT_PK_TUPLE_BYTES);
				ItemIdSetNormal(&uq_header->pd_linp[uq_off - 1],
								uq_upper, PGDM_RT_UQ_TUPLE_BYTES);
				memcpy(((char *) pk_header) + pk_upper, pk_tuple,
					   PGDM_RT_PK_TUPLE_BYTES);
				memcpy(((char *) uq_header) + uq_upper, uq_tuple,
					   PGDM_RT_UQ_TUPLE_BYTES);
			}

			/* Commit this private batch's headers/state once, before core. */
			pk_header->pd_lower += run * sizeof(ItemIdData);
			uq_header->pd_lower += run * sizeof(ItemIdData);
			pk_header->pd_upper = (LocationIndex) pk_upper;
			uq_header->pd_upper = (LocationIndex) uq_upper;
			pk_leaf->btps_lastoff = pk_off;
			uq_leaf->btps_lastoff = uq_off;
			pk_leaf->btps_lastextra = 0;
			uq_leaf->btps_lastextra = 0;
			leaf_stats->batched_rows += run;
			leaf_stats->batches++;
			i = end;
		}

		if (i >= next_report)
		{
			pgstat_progress_update_param(PROGRESS_CREATEIDX_TUPLES_DONE,
										 (int64) i);
			next_report = i + 8192;
		}
	}
	pfree(pk_tuple);
	pfree(uq_tuple);
	pgdm_rt_writer_finish(&pk_writer, pk_leaf);
	pgdm_rt_writer_finish(&uq_writer, uq_leaf);
	*pk_pages = pk_writer.btws_pages_written;
	*uq_pages = uq_writer.btws_pages_written;
	io_stats->pages = pk_writer.io.pages + uq_writer.io.pages;
	io_stats->batches = pk_writer.io.batches + uq_writer.io.batches;
	io_stats->wal_records = pk_writer.io.wal_records + uq_writer.io.wal_records;
	io_stats->write_runs = pk_writer.io.write_runs + uq_writer.io.write_runs;
	io_stats->page_writes = pk_writer.io.page_writes + uq_writer.io.page_writes;
	io_stats->zero_extends = pk_writer.io.zero_extends + uq_writer.io.zero_extends;
	io_stats->wal_ms = pk_writer.io.wal_ms + uq_writer.io.wal_ms;
	io_stats->storage_ms = pk_writer.io.storage_ms + uq_writer.io.storage_ms;
	io_stats->sync_ms = pk_writer.io.sync_ms + uq_writer.io.sync_ms;
	pgstat_progress_update_param(PROGRESS_CREATEIDX_TUPLES_DONE, (int64) count);
}


#endif /* supported PostgreSQL majors */
#endif /* PGDM_RADIX_TWIN_INC */
