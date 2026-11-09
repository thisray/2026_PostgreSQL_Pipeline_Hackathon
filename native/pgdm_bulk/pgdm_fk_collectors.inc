/* Complete field coverage from the actual index heap callback. */
#ifndef PGDM_FK_COLLECTORS_INC
#define PGDM_FK_COLLECTORS_INC

#include "portability/instr_time.h"

PG_FUNCTION_INFO_V1(pgdm_fk_proof_collect);

typedef struct PgdmFkCollector
{
    char *job_id;
    Oid owner_oid;
    int nsets;
    PgdmFkSet sets[2];
} PgdmFkCollector;

static void
pgdm_fk_collector_init(PgdmFkCollector *collector, Relation relation,
                       AttrNumber *attributes, int count, MemoryContext context)
{
    Oid types[2] = {OIDOID,OIDOID};
    Datum args[2] = {ObjectIdGetDatum(RelationGetRelid(relation)),ObjectIdGetDatum(GetUserId())};
    char *job = NULL;
    bool isnull;
    int i;
    memset(collector,0,sizeof(*collector));
    if (IsolationUsesXactSnapshot()) return;
    if (SPI_connect()!=SPI_OK_CONNECT) elog(ERROR,"could not open index proof metadata");
    if (pgdm_fk_metadata(
        "SELECT e.job_id FROM pgdm_fk_proof.epochs e JOIN pgdm_fk_proof.relations r USING(job_id) "
        "WHERE r.relation_oid=$1 AND e.owner_oid=$2 AND e.valid "
        "AND e.build_id='" PGDM_BULK_BUILD_ID "'",2,types,args,true)==SPI_OK_SELECT && SPI_processed==1)
    {
        Datum value=SPI_getbinval(SPI_tuptable->vals[0],SPI_tuptable->tupdesc,1,&isnull);
        if(!isnull)
        {
            MemoryContext old=MemoryContextSwitchTo(context);
            job=TextDatumGetCString(value);
            MemoryContextSwitchTo(old);
        }
    }
    if(job && (!pgdm_fk_epoch_owned(job,GetUserId(),InvalidOid) ||
               !pgdm_fk_guard_matches(job,RelationGetRelid(relation)))) job=NULL;
    SPI_finish();
    if(!job) return;
    collector->job_id=job;
    collector->owner_oid=GetUserId();
    collector->nsets=count;
    Assert(count>0 && count<=2);
    for(i=0;i<count;i++) pgdm_fk_set_init(&collector->sets[i],relation,attributes[i],context);
}

static void
pgdm_fk_collector_finish(PgdmFkCollector *collector, uint64 actual_rows, bool complete_scan)
{
    int i;
    if(!complete_scan || !collector->job_id) return;
    if(SPI_connect()!=SPI_OK_CONNECT) elog(ERROR,"could not store complete FK proof");
    if(!pgdm_fk_epoch_owned(collector->job_id,collector->owner_oid,InvalidOid))
    {
        SPI_finish();
        return;
    }
    for(i=0;i<collector->nsets;i++)
    {
        PgdmFkSet *set=&collector->sets[i];
        Oid types[11]={TEXTOID,OIDOID,INT2OID,OIDOID,INT8OID,INT8OID,INT8OID,INT8OID,INT8OID,BYTEAOID,TEXTOID};
        Datum values[11];
        if(!pgdm_fk_set_complete(set,actual_rows) ||
            !pgdm_fk_guard_matches(collector->job_id,set->relation_oid)) continue;
        values[0]=CStringGetTextDatum(collector->job_id);
        values[1]=ObjectIdGetDatum(set->relation_oid);
        values[2]=Int16GetDatum(set->attnum);
        values[3]=ObjectIdGetDatum(set->type_oid);
        values[4]=Int64GetDatum((int64)set->rows);
        values[5]=Int64GetDatum((int64)set->null_rows);
        values[6]=Int64GetDatum((int64)set->distinct_keys);
        values[7]=Int64GetDatum(set->min_key);
        values[8]=Int64GetDatum(set->max_key);
        values[9]=PointerGetDatum(pgdm_fk_set_bytes(set));
        values[10]=CStringGetTextDatum(PGDM_BULK_BUILD_ID);
        pgdm_fk_metadata(
            "INSERT INTO pgdm_fk_proof.key_sets(job_id,relation_oid,attnum,type_oid,rows,null_rows,distinct_keys,min_key,max_key,bits) "
            "SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10 WHERE EXISTS(SELECT FROM pgdm_fk_proof.epochs WHERE job_id=$1 AND valid AND build_id=$11) "
            "ON CONFLICT(job_id,relation_oid,attnum) DO UPDATE SET type_oid=excluded.type_oid,rows=excluded.rows,null_rows=excluded.null_rows,"
            "distinct_keys=excluded.distinct_keys,min_key=excluded.min_key,max_key=excluded.max_key,bits=excluded.bits",11,types,values,false);
    }
    SPI_finish();
}

/* Tiny dimensions are read as real tuples after the epoch's locks are held. */
static void
pgdm_fk_dimension_membership(const char *job, Relation relation)
{
    MemoryContext context=AllocSetContextCreate(CurrentMemoryContext,"pgdm dimension membership",ALLOCSET_DEFAULT_SIZES);
    PgdmFkCollector collector;
    AttrNumber attribute=get_attnum(RelationGetRelid(relation),"id_column_1");
    TableScanDesc scan;
    TupleTableSlot *slot;
    if(attribute<=0) { MemoryContextDelete(context); return; }
    pgdm_fk_collector_init(&collector,relation,&attribute,1,context);
    if(!collector.job_id || strcmp(job,collector.job_id)!=0) { MemoryContextDelete(context); return; }
    scan=table_beginscan(relation,GetActiveSnapshot(),0,NULL);
    slot=table_slot_create(relation,NULL);
    while(table_scan_getnextslot(scan,ForwardScanDirection,slot))
    {
        bool isnull;
        Datum value=slot_getattr(slot,attribute,&isnull);
        pgdm_fk_set_observe(&collector.sets[0],value,isnull,true);
        ExecClearTuple(slot);
        CHECK_FOR_INTERRUPTS();
    }
    ExecDropSingleTupleTableSlot(slot);
    table_endscan(scan);
    pgdm_fk_collector_finish(&collector,collector.sets[0].rows,true);
    MemoryContextDelete(context);
}

/* Explicit low-memory producer. No expected row count or index order is used. */
Datum
pgdm_fk_proof_collect(PG_FUNCTION_ARGS)
{
    Oid oid = PG_GETARG_OID(0);
    char *layout = text_to_cstring(PG_GETARG_TEXT_PP(1));
    Relation relation;
    AttrNumber attributes[2];
    int fields, i;
    MemoryContext context;
    PgdmFkCollector collector;
    TableScanDesc scan;
    TupleTableSlot *slot;
    uint64 scanned_live_rows = 0;
    StringInfoData result;
    instr_time started, finished;

    PreventCommandIfReadOnly("pgdm_fk_proof_collect");
    PreventCommandIfParallelMode("pgdm_fk_proof_collect");
    relation = table_open(oid, ShareLock);
    if (!object_ownercheck(RelationRelationId, oid, GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TABLE, RelationGetRelationName(relation));
    if (relation->rd_rel->relkind != RELKIND_RELATION ||
        relation->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT ||
        relation->rd_rel->relrowsecurity || relation->rd_rel->relforcerowsecurity ||
        relation->rd_rel->relispartition || relation->rd_rel->relhassubclass ||
        relation->rd_tableam != GetHeapamTableAmRoutine() || IsSystemRelation(relation))
        elog(ERROR, "proof collection requires an ordinary permanent heap without RLS");
    if (strcmp(layout,"table1") && strcmp(layout,"table2") &&
        strcmp(layout,"table5") && strcmp(layout,"table6"))
        elog(ERROR, "unsupported proof collection layout");
    fields = !strcmp(layout,"table1") || !strcmp(layout,"table6") ? 2 : 1;
    attributes[0] = get_attnum(oid,!strcmp(layout,"table1") ? "id_column_1" : "id_column_2");
    attributes[1] = fields == 2 ? get_attnum(oid,!strcmp(layout,"table1") ? "id_column_4" : "id_column_3") : 0;
    for (i=0;i<fields;i++)
        if (attributes[i]<=0) elog(ERROR,"missing proof collection attribute");
    context = AllocSetContextCreate(CurrentMemoryContext,"pgdm proof-only scan",ALLOCSET_DEFAULT_SIZES);
    pgdm_fk_collector_init(&collector,relation,attributes,fields,context);
    if (!collector.job_id)
    {
        MemoryContextDelete(context);
        table_close(relation,NoLock);
        PG_RETURN_DATUM(DirectFunctionCall1(jsonb_in,CStringGetDatum(
            "{\"producer\":\"proof_only_scan\",\"extra_heap_scans\":0,\"eof\":false,"
            "\"reason\":\"missing_owned_epoch_or_snapshot_capability\"}")));
    }
    INSTR_TIME_SET_CURRENT(started);
    scan = table_beginscan(relation,GetActiveSnapshot(),0,NULL);
    slot = table_slot_create(relation,NULL);
    while (table_scan_getnextslot(scan,ForwardScanDirection,slot))
    {
        scanned_live_rows++;
        for (i=0;i<fields;i++)
        {
            bool isnull;
            Datum value = slot_getattr(slot,attributes[i],&isnull);
            pgdm_fk_set_observe(&collector.sets[i],value,isnull,true);
        }
        ExecClearTuple(slot);
        CHECK_FOR_INTERRUPTS();
    }
    ExecDropSingleTupleTableSlot(slot);
    table_endscan(scan);
    /* EOF is reached before any complete set can be persisted. */
    pgdm_fk_collector_finish(&collector,scanned_live_rows,true);
    INSTR_TIME_SET_CURRENT(finished);
    INSTR_TIME_SUBTRACT(finished,started);
    initStringInfo(&result);
    appendStringInfo(&result,"{\"producer\":\"proof_only_scan\",\"extra_heap_scans\":1,"
        "\"scanned_live_rows\":" UINT64_FORMAT ",\"eof\":true,\"scan_ms\":%.3f,\"sets\":[",
        scanned_live_rows,INSTR_TIME_GET_MILLISEC(finished));
    for (i=0;i<fields;i++)
    {
        PgdmFkSet *set = &collector.sets[i];
        appendStringInfo(&result,"%s{\"attnum\":%d,\"complete\":%s,\"null_rows\":" UINT64_FORMAT
            ",\"max_key\":" INT64_FORMAT ",\"allocated_bytes\":%zu,\"reason\":\"%s\"}",
            i ? "," : "",set->attnum,pgdm_fk_set_complete(set,scanned_live_rows) ? "true" : "false",
            set->null_rows,set->max_key,set->allocated,set->reason ? set->reason : "");
    }
    appendStringInfoString(&result,"]}");
    elog(LOG,"pgdm independent FK producer %s",result.data);
    MemoryContextDelete(context);
    table_close(relation,NoLock);
    PG_RETURN_DATUM(DirectFunctionCall1(jsonb_in,CStringGetDatum(result.data)));
}
#endif
