/* Remove only our temporary guards after normal DDL validated all six FKs. */
#ifndef PGDM_FK_PUBLISH_INC
#define PGDM_FK_PUBLISH_INC
PG_FUNCTION_INFO_V1(pgdm_fk_proof_publish);

Datum
pgdm_fk_proof_publish(PG_FUNCTION_ARGS)
{
    static const int children[6]={0,0,1,4,5,5};
    static const int parents[6]={-1,2,0,0,0,3};
    static const char *const child_columns[6]={"id_column_3","id_column_4","id_column_2","id_column_2","id_column_2","id_column_3"};
    static const char *const names[6]={"table1_id_column_3_fkey","table1_id_column_4_fkey","table2_id_column_2_fkey","table5_id_column_2_fkey","table6_id_column_2_fkey","table6_id_column_3_fkey"};
    char *job=text_to_cstring(PG_GETARG_TEXT_PP(0));
    char *schema=text_to_cstring(PG_GETARG_TEXT_PP(1));
    Oid namespace_oid=get_namespace_oid(schema,false);
    Oid caller=GetUserId();
    Relation relations[6];
    Oid relation_oids[6];
    Oid raw_oid=get_relname_relid("raw_data",get_namespace_oid("public",false));
    int i;
    if(strncmp(schema,"pgdm_shadow_",12)!=0 || !object_ownercheck(NamespaceRelationId,namespace_oid,caller))
        ereport(ERROR,(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),errmsg("FK publication requires an owned shadow schema")));
    for(i=0;i<6;i++)
    {
        char name[16];
        snprintf(name,sizeof(name),"table%d",i+1);
        relations[i]=table_open(get_relname_relid(name,namespace_oid),AccessExclusiveLock);
        relation_oids[i]=RelationGetRelid(relations[i]);
        if(!object_ownercheck(RelationRelationId,relation_oids[i],caller))
            ereport(ERROR,(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),errmsg("FK publication owner changed")));
    }
    for(i=0;i<6;i++) table_close(relations[i],NoLock);
    if(SPI_connect()!=SPI_OK_CONNECT) elog(ERROR,"could not check FK publication catalog");
    for(i=0;i<6;i++)
    {
        Oid child_oid=relation_oids[children[i]];
        Oid parent_oid=parents[i]<0 ? raw_oid : relation_oids[parents[i]];
        AttrNumber child_att=get_attnum(child_oid,child_columns[i]);
        AttrNumber parent_att=get_attnum(parent_oid,parents[i]<0 ? "raw_id" : "id_column_1");
        Oid type=get_atttype(child_oid,child_att);
        Oid types[6]={OIDOID,OIDOID,INT2OID,INT2OID,TEXTOID,OIDOID};
        Datum values[6]={ObjectIdGetDatum(child_oid),ObjectIdGetDatum(parent_oid),
            Int16GetDatum(child_att),Int16GetDatum(parent_att),CStringGetTextDatum(names[i]),
            ObjectIdGetDatum(type==INT8OID ? F_INT8EQ : F_INT2EQ)};
        bool isnull;
        Datum valid;
        const char *sql=
            "SELECT EXISTS(SELECT FROM pg_catalog.pg_constraint k JOIN pg_catalog.pg_index x ON x.indexrelid=k.conindid "
            "WHERE k.conrelid=$1 AND k.confrelid=$2 AND k.conname=$5 AND k.contype='f' AND k.convalidated "
            "AND NOT k.condeferrable AND NOT k.condeferred AND k.confmatchtype='s' AND k.confupdtype='c' AND k.confdeltype='c' "
#if PG_VERSION_NUM >= 180000
            "AND k.conenforced AND NOT k.conperiod "
#endif
            "AND k.conkey=ARRAY[$3]::pg_catalog.int2[] AND k.confkey=ARRAY[$4]::pg_catalog.int2[] "
            "AND pg_catalog.array_length(k.conpfeqop,1)=1 "
            "AND k.conppeqop=k.conpfeqop AND k.conffeqop=k.conpfeqop "
            "AND (SELECT oprcode FROM pg_catalog.pg_operator WHERE oid=k.conpfeqop[1])=$6 "
            "AND x.indrelid=$2 AND x.indisunique AND x.indisvalid AND x.indisready "
            "AND (SELECT count(*) FROM pg_catalog.pg_trigger t WHERE t.tgconstraint=k.oid)=4 "
            "AND (SELECT count(*) FROM pg_catalog.pg_trigger t JOIN pg_catalog.pg_proc p ON p.oid=t.tgfoid "
            "WHERE t.tgconstraint=k.oid AND t.tgisinternal AND t.tgenabled='O' AND NOT t.tgdeferrable AND NOT t.tginitdeferred "
            "AND ((t.tgrelid=$1 AND t.tgconstrrelid=$2 AND ((p.proname='RI_FKey_check_ins' AND t.tgtype=5) OR (p.proname='RI_FKey_check_upd' AND t.tgtype=17))) "
            "OR (t.tgrelid=$2 AND t.tgconstrrelid=$1 AND ((p.proname='RI_FKey_cascade_del' AND t.tgtype=9) OR (p.proname='RI_FKey_cascade_upd' AND t.tgtype=17)))))=4)";
        if((type!=INT8OID && type!=INT2OID) || get_atttype(parent_oid,parent_att)!=type ||
            SPI_execute_with_args(sql,6,types,values,NULL,true,1)!=SPI_OK_SELECT || SPI_processed!=1)
            elog(ERROR,"required FK publication metadata is incomplete");
        valid=SPI_getbinval(SPI_tuptable->vals[0],SPI_tuptable->tupdesc,1,&isnull);
        if(isnull || !DatumGetBool(valid))
            ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("required FK or normal RI triggers changed before publication: %s",names[i])));
    }
    for(i=0;i<6;i++)
    {
        Oid types[1]={OIDOID};
        Datum values[1]={ObjectIdGetDatum(relation_oids[i])};
        if(SPI_execute_with_args("SELECT p.prosrc,p.proowner FROM pg_catalog.pg_trigger t JOIN pg_catalog.pg_proc p ON p.oid=t.tgfoid WHERE t.tgrelid=$1 AND t.tgname='" PGDM_FK_GUARD "'",1,types,values,NULL,true,1)!=SPI_OK_SELECT)
            elog(ERROR,"could not inspect temporary FK guard");
        if(SPI_processed)
        {
            bool isnull;
            char *symbol=TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[0],SPI_tuptable->tupdesc,1,&isnull));
            Oid owner=DatumGetObjectId(SPI_getbinval(SPI_tuptable->vals[0],SPI_tuptable->tupdesc,2,&isnull));
            char *qualified=quote_qualified_identifier(schema,get_rel_name(relation_oids[i]));
            if(strcmp(symbol,"pgdm_fk_proof_reject")!=0 || owner!=pgdm_fk_extension_owner())
                elog(ERROR,"temporary FK guard identity changed before publication");
            if(SPI_execute(psprintf("DROP TRIGGER %s ON %s",PGDM_FK_GUARD,qualified),false,0)<0)
                elog(ERROR,"could not retire temporary FK guard");
        }
    }
    {
        Oid types[2]={TEXTOID,OIDOID};
        Datum values[2]={CStringGetTextDatum(job),ObjectIdGetDatum(caller)};
        pgdm_fk_metadata("DELETE FROM pgdm_fk_proof.epochs WHERE job_id=$1 AND owner_oid=$2",2,types,values,false);
    }
    SPI_finish();
    PG_RETURN_BOOL(true);
}
#endif
