/* Protected metadata and normal statement guards for a sealed shadow epoch. */
#ifndef PGDM_FK_EPOCH_INC
#define PGDM_FK_EPOCH_INC
#include "catalog/pg_extension.h"
#include "access/transam.h"
#include "catalog/pg_namespace.h"
#include "commands/extension.h"
#include "commands/trigger.h"
#include "parser/parse_func.h"
#include "storage/lmgr.h"
#include "tcop/utility.h"

PG_FUNCTION_INFO_V1(pgdm_fk_proof_reject);
PG_FUNCTION_INFO_V1(pgdm_fk_proof_seal);
PG_FUNCTION_INFO_V1(pgdm_fk_proof_enable);
PG_FUNCTION_INFO_V1(pgdm_fk_proof_forget);
PG_FUNCTION_INFO_V1(pgdm_fk_proof_dimensions);

#define PGDM_FK_GUARD "pgdm_fk_immutable"
#define PGDM_FK_SET_POLICY "shared_index_complete_fk_v1"

static char *pgdm_fk_enabled_job = NULL;
static void pgdm_fk_dimension_membership(const char *job, Relation relation);

/* A released SAVEPOINT still belongs to this transaction until outer COMMIT. */
static bool
pgdm_fk_xmin_committed(Datum value)
{
    TransactionId xid = DatumGetTransactionId(value);
    return TransactionIdIsValid(xid) &&
        !TransactionIdIsCurrentTransactionId(xid) &&
        (TransactionIdEquals(xid, FrozenTransactionId) || TransactionIdDidCommit(xid));
}

static Oid
pgdm_fk_extension_owner(void)
{
    Oid extension_oid = get_extension_oid("pgdm_bulk", false);
    HeapTuple tuple = SearchSysCache1(EXTENSIONOID, ObjectIdGetDatum(extension_oid));
    Oid owner;
    if (!HeapTupleIsValid(tuple)) elog(ERROR, "pgdm_bulk extension identity is missing");
    owner = ((Form_pg_extension) GETSTRUCT(tuple))->extowner;
    ReleaseSysCache(tuple);
    return owner;
}

/* SQL is a compile-time constant; caller values use typed SPI arguments. */
static int
pgdm_fk_metadata(const char *sql, int nargs, Oid *types, Datum *values, bool read_only)
{
    Oid user;
    int context;
    int result = 0;
    GetUserIdAndSecContext(&user, &context);
    PG_TRY();
    {
        SetUserIdAndSecContext(pgdm_fk_extension_owner(), context | SECURITY_LOCAL_USERID_CHANGE);
        result = SPI_execute_with_args(sql, nargs, types, values, NULL, read_only, 0);
    }
    PG_FINALLY();
    {
        SetUserIdAndSecContext(user, context);
    }
    PG_END_TRY();
    return result;
}

static bool
pgdm_fk_guard_matches(const char *job, Oid relation_oid)
{
    Oid types[3] = {TEXTOID,OIDOID,OIDOID};
    Datum values[3] = {CStringGetTextDatum(job),ObjectIdGetDatum(relation_oid),
                       ObjectIdGetDatum(pgdm_fk_extension_owner())};
    bool isnull;
    Datum found;
    static const char sql[] =
        "SELECT EXISTS(SELECT FROM pgdm_fk_proof.relations r "
        "JOIN pgdm_fk_proof.epochs e USING(job_id) "
        "JOIN pg_catalog.pg_class c ON c.oid=r.relation_oid "
        "JOIN pg_catalog.pg_trigger t ON t.oid=r.guard_oid "
        "JOIN pg_catalog.pg_proc p ON p.oid=t.tgfoid "
        "WHERE r.job_id=$1 AND r.relation_oid=$2 AND e.valid "
        "AND e.build_id='" PGDM_BULK_BUILD_ID "' "
        "AND c.relnamespace=e.schema_oid AND c.relowner=e.owner_oid "
        "AND pg_catalog.pg_relation_filenode(c.oid)=r.filenode "
        "AND c.relkind='r' AND c.relpersistence='p' AND NOT c.relispartition "
        "AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity "
        "AND NOT c.relhassubclass "
        "AND t.tgrelid=c.oid AND t.tgname='" PGDM_FK_GUARD "' "
        "AND t.xmin::pg_catalog.text=r.guard_xmin AND t.tgfoid=r.function_oid "
        "AND p.xmin::pg_catalog.text=r.function_xmin AND p.proowner=$3 "
        "AND p.prosrc='pgdm_fk_proof_reject' AND p.prorettype=2279 AND p.pronargs=0 "
        "AND p.prolang=(SELECT oid FROM pg_catalog.pg_language WHERE lanname='c') "
        "AND t.tgenabled='A' AND t.tgtype=62 AND NOT t.tgisinternal "
        "AND t.tgconstraint=0 AND t.tgqual IS NULL AND t.tgnargs=0 "
        "AND t.tgattr=''::pg_catalog.int2vector)";
    if (pgdm_fk_metadata(sql, 3, types, values, true) != SPI_OK_SELECT || SPI_processed != 1)
        return false;
    found = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
    return !isnull && DatumGetBool(found);
}

Datum
pgdm_fk_proof_reject(PG_FUNCTION_ARGS)
{
    if (!CALLED_AS_TRIGGER(fcinfo)) elog(ERROR, "pgdm FK guard requires trigger context");
    ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    errmsg("sealed shadow rows are immutable until publication")));
    PG_RETURN_NULL();
}

static void
pgdm_fk_context_end(XactEvent event, void *argument)
{
    (void) argument;
    if (event == XACT_EVENT_COMMIT || event == XACT_EVENT_ABORT ||
        event == XACT_EVENT_PARALLEL_COMMIT || event == XACT_EVENT_PARALLEL_ABORT)
    {
        if (pgdm_fk_enabled_job) pfree(pgdm_fk_enabled_job);
        pgdm_fk_enabled_job = NULL;
    }
}

static bool
pgdm_fk_epoch_owned(const char *job, Oid caller, Oid schema_oid)
{
    Oid types[3] = {TEXTOID,OIDOID,OIDOID};
    Datum values[3] = {CStringGetTextDatum(job),ObjectIdGetDatum(caller),ObjectIdGetDatum(schema_oid)};
    bool isnull;
    Datum xmin;
    const char *sql =
        "SELECT xmin FROM pgdm_fk_proof.epochs WHERE job_id=$1 "
        "AND owner_oid=$2 AND ($3=0 OR schema_oid=$3) AND valid "
        "AND build_id='" PGDM_BULK_BUILD_ID "'";
    if (pgdm_fk_metadata(sql, 3, types, values, true) != SPI_OK_SELECT || SPI_processed != 1)
        return false;
    xmin = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
    return !isnull && pgdm_fk_xmin_committed(xmin);
}

/* Native heap insertion bypasses statement triggers, so it checks the seal too.
 * The caller holds RowExclusiveLock, conflicting with the seal's ShareLock.
 * Read the latest snapshot even when the caller started before the seal. */
static void
pgdm_fk_require_unsealed_writer(Relation relation)
{
    Oid namespace_oid = get_namespace_oid("pgdm_fk_proof", true);
    Oid types[1] = {OIDOID};
    Datum values[1] = {ObjectIdGetDatum(RelationGetRelid(relation))};
    bool sealed = false;
    if (!OidIsValid(namespace_oid) ||
        !OidIsValid(get_relname_relid("relations", namespace_oid))) return;
    PushActiveSnapshot(GetLatestSnapshot());
    if (SPI_connect() != SPI_OK_CONNECT) elog(ERROR, "could not check native writer seal");
    /* SELECT is granted to writers; never elevate a pre-upgrade schema lookup. */
    if (SPI_execute_with_args("SELECT 1 FROM pgdm_fk_proof.relations WHERE relation_oid=$1",
                              1, types, values, NULL, true, 0) == SPI_OK_SELECT)
        sealed = SPI_processed != 0;
    SPI_finish();
    PopActiveSnapshot();
    if (sealed)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("native writer cannot modify a sealed shadow relation")));
}

Datum
pgdm_fk_proof_enable(PG_FUNCTION_ARGS)
{
    char *job = text_to_cstring(PG_GETARG_TEXT_PP(0));
    bool valid;
    if (pgdm_fk_enabled_job) pfree(pgdm_fk_enabled_job);
    pgdm_fk_enabled_job = NULL;
    if (IsolationUsesXactSnapshot()) PG_RETURN_BOOL(false);
    if (SPI_connect() != SPI_OK_CONNECT) elog(ERROR, "could not open FK proof metadata");
    valid = pgdm_fk_epoch_owned(job, GetUserId(), InvalidOid);
    SPI_finish();
    if (valid) pgdm_fk_enabled_job = MemoryContextStrdup(TopMemoryContext, job);
    PG_RETURN_BOOL(valid);
}

Datum
pgdm_fk_proof_forget(PG_FUNCTION_ARGS)
{
    char *job = text_to_cstring(PG_GETARG_TEXT_PP(0));
    Oid types[2] = {TEXTOID,OIDOID};
    Datum values[2] = {CStringGetTextDatum(job),ObjectIdGetDatum(GetUserId())};
    if (SPI_connect() != SPI_OK_CONNECT) elog(ERROR, "could not open FK proof metadata");
    pgdm_fk_metadata("DELETE FROM pgdm_fk_proof.epochs WHERE job_id=$1 AND owner_oid=$2", 2, types, values, false);
    SPI_finish();
    PG_RETURN_BOOL(true);
}

Datum
pgdm_fk_proof_dimensions(PG_FUNCTION_ARGS)
{
    char *job=text_to_cstring(PG_GETARG_TEXT_PP(0));
    Oid namespace_oid=InvalidOid;
    Oid types[2]={TEXTOID,OIDOID};
    Datum values[2]={CStringGetTextDatum(job),ObjectIdGetDatum(GetUserId())};
    bool isnull;
    int i;
    if(IsolationUsesXactSnapshot()) PG_RETURN_BOOL(false);
    if(SPI_connect()!=SPI_OK_CONNECT) elog(ERROR,"could not collect dimension proof");
    if(pgdm_fk_epoch_owned(job,GetUserId(),InvalidOid) &&
       pgdm_fk_metadata("SELECT schema_oid FROM pgdm_fk_proof.epochs WHERE job_id=$1 AND owner_oid=$2",2,types,values,true)==SPI_OK_SELECT && SPI_processed==1)
        namespace_oid=DatumGetObjectId(SPI_getbinval(SPI_tuptable->vals[0],SPI_tuptable->tupdesc,1,&isnull));
    SPI_finish();
    if(!OidIsValid(namespace_oid)) PG_RETURN_BOOL(false);
    for(i=3;i<=4;i++)
    {
        char name[16];
        Relation dimension;
        snprintf(name,sizeof(name),"table%d",i);
        dimension=table_open(get_relname_relid(name,namespace_oid),ShareLock);
        pgdm_fk_dimension_membership(job,dimension);
        table_close(dimension,NoLock);
    }
    PG_RETURN_BOOL(true);
}

Datum
pgdm_fk_proof_seal(PG_FUNCTION_ARGS)
{
    char *job = text_to_cstring(PG_GETARG_TEXT_PP(0));
    char *schema = text_to_cstring(PG_GETARG_TEXT_PP(1));
    Oid namespace_oid = get_namespace_oid(schema, false);
    Oid caller = GetUserId();
    Oid types[4] = {TEXTOID,OIDOID,OIDOID,TEXTOID};
    Datum values[4] = {CStringGetTextDatum(job),ObjectIdGetDatum(caller),
                       ObjectIdGetDatum(namespace_oid),CStringGetTextDatum(PGDM_BULK_BUILD_ID)};
    Relation relations[6];
    Oid relation_oids[6];
    bool existing, valid = true;
    int i;
    PreventCommandIfReadOnly("pgdm_fk_proof_seal");
    PreventCommandIfParallelMode("pgdm_fk_proof_seal");
    if (IsolationUsesXactSnapshot()) PG_RETURN_BOOL(false);
    if (strncmp(schema,"pgdm_shadow_",12) != 0 ||
        !object_ownercheck(NamespaceRelationId,namespace_oid,caller))
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("FK epoch requires an owned shadow schema")));
    for (i=0;i<6;i++)
    {
        char name[16];
        Oid relation_oid;
        snprintf(name,sizeof(name),"table%d",i+1);
        relation_oid=get_relname_relid(name,namespace_oid);
        if (!OidIsValid(relation_oid)) elog(ERROR,"FK epoch relation is missing");
        relation_oids[i]=relation_oid;
        relations[i]=table_open(relation_oid,ShareLock);
        if (!object_ownercheck(RelationRelationId,relation_oid,caller) ||
            relations[i]->rd_rel->relkind!=RELKIND_RELATION ||
            relations[i]->rd_rel->relpersistence!=RELPERSISTENCE_PERMANENT ||
            relations[i]->rd_rel->relispartition || relations[i]->rd_rel->relhassubclass ||
            relations[i]->rd_rel->relrowsecurity || relations[i]->rd_rel->relforcerowsecurity)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("FK epoch requires ordinary owned logged heaps")));
    }
    /* Retain locks, not relcache references, across normal trigger DDL. */
    for(i=0;i<6;i++) table_close(relations[i],NoLock);
    PushActiveSnapshot(GetLatestSnapshot());
    if (SPI_connect()!=SPI_OK_CONNECT) elog(ERROR,"could not open FK proof metadata");
    {
        Oid check_types[2]={TEXTOID,TEXTOID};
        Datum check_values[2]={CStringGetTextDatum(job),CStringGetTextDatum(schema)};
        bool isnull;
        Datum ready;
        if(SPI_execute_with_args("SELECT EXISTS(SELECT FROM pgdm_runtime.shadow_jobs j WHERE j.job_id=$1 AND j.schema_name=$2 AND j.state='indexing' AND NOT EXISTS(SELECT FROM pgdm_runtime.shadow_chunks c WHERE c.job_id=j.job_id AND c.state<>'done'))",2,check_types,check_values,NULL,true,1)!=SPI_OK_SELECT)
            elog(ERROR,"could not check completed job before sealing");
        ready=SPI_getbinval(SPI_tuptable->vals[0],SPI_tuptable->tupdesc,1,&isnull);
        if(isnull || !DatumGetBool(ready))
            ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("only a completed shadow load can be sealed")));
    }
    pgdm_fk_metadata("SELECT 1 FROM pgdm_fk_proof.epochs WHERE job_id=$1 FOR UPDATE",1,types,values,false);
    existing=SPI_processed!=0;
    if (existing)
    {
        valid=pgdm_fk_epoch_owned(job,caller,namespace_oid);
        for(i=0;valid && i<6;i++) valid=pgdm_fk_guard_matches(job,relation_oids[i]);
    }
    else
    {
        pgdm_fk_metadata("INSERT INTO pgdm_fk_proof.epochs(job_id,owner_oid,schema_oid,build_id) VALUES($1,$2,$3,$4)",4,types,values,false);
        for(i=0;i<6;i++)
        {
            char *qualified=quote_qualified_identifier(schema,get_rel_name(relation_oids[i]));
            char *ddl=psprintf("CREATE TRIGGER %s BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON %s FOR EACH STATEMENT EXECUTE FUNCTION public.pgdm_fk_proof_reject(); ALTER TABLE %s ENABLE ALWAYS TRIGGER %s",PGDM_FK_GUARD,qualified,qualified,PGDM_FK_GUARD);
            Oid row_types[2]={TEXTOID,OIDOID};
            Datum row_values[2]={CStringGetTextDatum(job),ObjectIdGetDatum(relation_oids[i])};
            if(SPI_execute(ddl,false,0)<0) elog(ERROR,"could not install FK epoch guard");
            CommandCounterIncrement();
            pgdm_fk_metadata(
                "INSERT INTO pgdm_fk_proof.relations SELECT $1,c.oid,pg_catalog.pg_relation_filenode(c.oid),t.oid,t.xmin::pg_catalog.text,p.oid,p.xmin::pg_catalog.text "
                "FROM pg_catalog.pg_class c JOIN pg_catalog.pg_trigger t ON t.tgrelid=c.oid JOIN pg_catalog.pg_proc p ON p.oid=t.tgfoid "
                "WHERE c.oid=$2 AND t.tgname='" PGDM_FK_GUARD "'",2,row_types,row_values,false);
        }
    }
    if(!valid) pgdm_fk_metadata("UPDATE pgdm_fk_proof.epochs SET valid=false WHERE job_id=$1",1,types,values,false);
    SPI_finish();
    PopActiveSnapshot();
    PG_RETURN_BOOL(valid);
}
#endif
