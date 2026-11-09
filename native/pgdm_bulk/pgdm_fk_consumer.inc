/* A complete child set may prove an RI query empty; every miss uses its plan. */
#ifndef PGDM_FK_CONSUMER_INC
#define PGDM_FK_CONSUMER_INC

/* PG17 has no membership consumer; keep its proof closure out of the build. */
#if (PG_VERSION_NUM >= 160000 && PG_VERSION_NUM < 170000) || \
    (PG_VERSION_NUM >= 180000 && PG_VERSION_NUM < 190000)

static bool pgdm_raw_fk_key(HeapTuple tuple, int attribute, Oid element_type, int expected);

typedef struct PgdmFkProofUse
{
    uint64 child_rows;
    uint64 parent_rows;
    uint64 bitmap_bytes;
    const char *reason;
} PgdmFkProofUse;

static bool
pgdm_fk_prove_membership(Oid constraint_oid, Oid child_oid, Oid parent_oid,
                         AttrNumber child_att, AttrNumber parent_att,
                         Oid child_type, Oid parent_type, PgdmFkProofUse *use)
{
    HeapTuple constraint_tuple;
    Form_pg_constraint constraint;
    Relation child, parent;
    bool matches;
    bool isnull;
    Oid types[6]={TEXTOID,OIDOID,INT2OID,OIDOID,INT2OID,OIDOID};
    Datum values[6];
    Datum data[10];
    bytea *child_bits, *parent_bits;
    Size child_bytes, parent_bytes, i;
    int64 child_rows, parent_rows, child_nulls, parent_nulls, child_max, parent_max;
    int function_oid;
    unsigned char *c, *p;

    memset(use,0,sizeof(*use));
    use->reason="missing_proof_context";
    if(!pgdm_fk_enabled_job || IsolationUsesXactSnapshot()) return false;
    use->reason="unsupported_key_operator";
    if(child_type!=parent_type || (child_type!=INT8OID && child_type!=INT2OID)) return false;
    function_oid=child_type==INT8OID ? F_INT8EQ : F_INT2EQ;
    /* Lock the parent against writes and fence DDL through the RI transaction. */
    parent=table_open(parent_oid,ShareLock);
    child=table_open(child_oid,NoLock);
    matches=child_att>0 && child_att<=RelationGetDescr(child)->natts &&
        parent_att>0 && parent_att<=RelationGetDescr(parent)->natts &&
        TupleDescAttr(RelationGetDescr(child),child_att-1)->atttypid==child_type &&
        TupleDescAttr(RelationGetDescr(parent),parent_att-1)->atttypid==parent_type &&
        !TupleDescAttr(RelationGetDescr(child),child_att-1)->attisdropped &&
        !TupleDescAttr(RelationGetDescr(parent),parent_att-1)->attisdropped;
    constraint_tuple=SearchSysCache1(CONSTROID,ObjectIdGetDatum(constraint_oid));
    if(HeapTupleIsValid(constraint_tuple))
    {
        constraint=(Form_pg_constraint)GETSTRUCT(constraint_tuple);
        matches=matches && constraint->contype==CONSTRAINT_FOREIGN &&
            constraint->conrelid==child_oid && constraint->confrelid==parent_oid &&
            constraint->confmatchtype=='s' &&
            pgdm_raw_fk_key(constraint_tuple,Anum_pg_constraint_conkey,INT2OID,child_att) &&
            pgdm_raw_fk_key(constraint_tuple,Anum_pg_constraint_confkey,INT2OID,parent_att) &&
            pgdm_raw_fk_key(constraint_tuple,Anum_pg_constraint_conpfeqop,OIDOID,function_oid) &&
            pgdm_raw_fk_key(constraint_tuple,Anum_pg_constraint_conppeqop,OIDOID,function_oid) &&
            pgdm_raw_fk_key(constraint_tuple,Anum_pg_constraint_conffeqop,OIDOID,function_oid);
#if PG_VERSION_NUM >= 180000
        matches=matches && constraint->conenforced && !constraint->conperiod;
#endif
        ReleaseSysCache(constraint_tuple);
    }
    else matches=false;
    table_close(child,NoLock);
    table_close(parent,NoLock);
    if(!matches) return false;
    if(SPI_connect()!=SPI_OK_CONNECT) elog(ERROR,"could not inspect FK proof metadata");
    if(!pgdm_fk_epoch_owned(pgdm_fk_enabled_job,GetUserId(),InvalidOid) ||
        !pgdm_fk_guard_matches(pgdm_fk_enabled_job,child_oid) ||
        !pgdm_fk_guard_matches(pgdm_fk_enabled_job,parent_oid))
    {
        use->reason="epoch_or_relation_identity";
        SPI_finish();
        return false;
    }
    values[0]=CStringGetTextDatum(pgdm_fk_enabled_job);
    values[1]=ObjectIdGetDatum(child_oid);
    values[2]=Int16GetDatum(child_att);
    values[3]=ObjectIdGetDatum(parent_oid);
    values[4]=Int16GetDatum(parent_att);
    values[5]=ObjectIdGetDatum(child_type);
    if(pgdm_fk_metadata(
        "SELECT c.rows,p.rows,c.null_rows,p.null_rows,c.max_key,p.max_key,c.bits,p.bits,c.xmin,p.xmin "
        "FROM pgdm_fk_proof.key_sets c JOIN pgdm_fk_proof.key_sets p ON p.job_id=c.job_id "
        "WHERE c.job_id=$1 AND c.relation_oid=$2 AND c.attnum=$3 AND p.relation_oid=$4 AND p.attnum=$5 "
        "AND c.type_oid=$6 AND p.type_oid=$6",6,types,values,true)!=SPI_OK_SELECT || SPI_processed!=1)
    {
        use->reason="missing_complete_key_set";
        SPI_finish();
        return false;
    }
    for(i=0;i<10;i++)
    {
        data[i]=SPI_getbinval(SPI_tuptable->vals[0],SPI_tuptable->tupdesc,(int)i+1,&isnull);
        if(isnull) { SPI_finish(); use->reason="null_certificate_field"; return false; }
    }
    if(!pgdm_fk_xmin_committed(data[8]) || !pgdm_fk_xmin_committed(data[9]))
    {
        use->reason="uncommitted_key_set";
        SPI_finish();
        return false;
    }
    child_rows=DatumGetInt64(data[0]); parent_rows=DatumGetInt64(data[1]);
    child_nulls=DatumGetInt64(data[2]); parent_nulls=DatumGetInt64(data[3]);
    child_max=DatumGetInt64(data[4]); parent_max=DatumGetInt64(data[5]);
    child_bits=DatumGetByteaPP(data[6]); parent_bits=DatumGetByteaPP(data[7]);
    child_bytes=VARSIZE_ANY_EXHDR(child_bits); parent_bytes=VARSIZE_ANY_EXHDR(parent_bits);
    matches=child_rows>=0 && parent_rows>=0 && child_nulls>=0 && child_nulls<=child_rows &&
        parent_nulls>=0 && parent_nulls<=parent_rows && child_bytes<=PGDM_FK_SET_MAX_BYTES &&
        parent_bytes<=PGDM_FK_SET_MAX_BYTES &&
        ((child_rows==child_nulls && child_max==-1 && child_bytes==0) ||
         (child_rows>child_nulls && child_max>=0 && (uint64)child_max<=PGDM_FK_SET_MAX_KEY && child_bytes==(Size)((uint64)child_max/8+1))) &&
        ((parent_rows==parent_nulls && parent_max==-1 && parent_bytes==0) ||
         (parent_rows>parent_nulls && parent_max>=0 && (uint64)parent_max<=PGDM_FK_SET_MAX_KEY && parent_bytes==(Size)((uint64)parent_max/8+1)));
    if(matches)
    {
        c=(unsigned char *)VARDATA_ANY(child_bits);
        p=(unsigned char *)VARDATA_ANY(parent_bits);
        for(i=0;i<child_bytes;i++)
        {
            unsigned char available=i<parent_bytes ? p[i] : 0;
            if((c[i] & (unsigned char)~available)!=0) { matches=false; break; }
            if((i & 65535)==0) CHECK_FOR_INTERRUPTS();
        }
    }
    use->child_rows=(uint64)Max(child_rows,0);
    use->parent_rows=(uint64)Max(parent_rows,0);
    use->bitmap_bytes=child_bytes+parent_bytes;
    use->reason=matches ? "complete_index_sets" : "key_miss_or_invalid_certificate";
    SPI_finish();
    return matches;
}
#endif /* PostgreSQL 16 or 18 membership consumer */
#endif /* PGDM_FK_CONSUMER_INC */
