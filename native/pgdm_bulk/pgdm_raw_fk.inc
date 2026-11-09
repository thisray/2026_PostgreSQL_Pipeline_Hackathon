#include "catalog/pg_proc.h"

/* Actual inserted-tuple witness; no catalog writes or visibility changes. */
static bool
pgdm_raw_fk_key(HeapTuple tuple, int attribute, Oid element_type, int expected)
{
    Datum datum;
    ArrayType *array;
    bool isnull;
    datum = SysCacheGetAttr(CONSTROID, tuple, attribute, &isnull);
    if (isnull) return false;
    array = DatumGetArrayTypeP(datum);
    if (ARR_NDIM(array) != 1 || ARR_DIMS(array)[0] != 1 ||
        ARR_HASNULL(array) || ARR_ELEMTYPE(array) != element_type)
        return false;
    if (element_type == INT2OID)
        return *((int16 *) ARR_DATA_PTR(array)) == expected;
    return get_opcode(*((Oid *) ARR_DATA_PTR(array))) == (Oid) expected;
}

/* Compare the entire PL/pgSQL body, permitting only whitespace differences. */
static bool
pgdm_raw_fk_fence_body(const char *body)
{
    const char *expected = "BEGINRAISEEXCEPTION'Sourceisfrozenbyaresumableshadowexpansion';END";
    while (*body)
    {
        char c = *body++;
        if (c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v')
            continue;
        if (*expected == '\0' || c != *expected++) return false;
    }
    return *expected == '\0';
}

static bool
pgdm_raw_fk_fence_function(Oid function_oid)
{
    HeapTuple tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(function_oid));
    Form_pg_proc proc;
    Datum body;
    bool isnull, valid;
    if (!HeapTupleIsValid(tuple)) return false;
    proc = (Form_pg_proc) GETSTRUCT(tuple);
    body = SysCacheGetAttr(PROCOID, tuple, Anum_pg_proc_prosrc, &isnull);
    valid = !isnull && proc->pronargs == 0 && proc->prokind == PROKIND_FUNCTION &&
        proc->prorettype == TRIGGEROID &&
        strcmp(get_language_name(proc->prolang, false), "plpgsql") == 0 &&
        pgdm_raw_fk_fence_body(TextDatumGetCString(body));
    ReleaseSysCache(tuple);
    return valid;
}

static Oid
pgdm_raw_fk_fence(SourceReader *source)
{
    TriggerDesc *triggers = source->relation->trigdesc;
    Oid runtime_namespace = get_namespace_oid("pgdm_runtime", false);
    int i;
    int16 expected_type = TRIGGER_TYPE_BEFORE | TRIGGER_TYPE_INSERT |
                          TRIGGER_TYPE_UPDATE | TRIGGER_TYPE_DELETE |
                          TRIGGER_TYPE_TRUNCATE;
    if (triggers)
    for (i = 0; i < triggers->numtriggers; ++i)
    {
        Trigger *trigger = &triggers->triggers[i];
        if (strcmp(trigger->tgname, "pgdm_shadow_source_fence") != 0)
            continue;
        if (!trigger->tgisinternal && trigger->tgtype == expected_type &&
            trigger->tgenabled == 'O' && trigger->tgnargs == 0 &&
            trigger->tgnattr == 0 && trigger->tgqual == NULL &&
            !OidIsValid(trigger->tgconstraint) &&
            get_func_namespace(trigger->tgfoid) == runtime_namespace &&
            get_func_rettype(trigger->tgfoid) == TRIGGEROID &&
            strcmp(get_func_name(trigger->tgfoid), "reject_shadow_source_write") == 0 &&
            pgdm_raw_fk_fence_function(trigger->tgfoid))
            return trigger->tgoid;
        break;
    }
    ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    errmsg("raw FK tuple witness requires the committed source write fence")));
    return InvalidOid;
}

static bool
pgdm_raw_fk_open(TargetBuffer *target, const TargetSpec *spec, SourceReader *source)
{
    TriggerDesc *triggers = target->relation->trigdesc;
    HeapTuple tuple;
    Form_pg_constraint constraint;
    Oid constraint_oid;
    bool valid, seen_insert = false, seen_update = false;
    int i;

    if (triggers == NULL) return true; /* Existing jobs keep normal validation. */
    if (strcmp(spec->name, "table1") != 0 || triggers->numtriggers != 2)
        return false;
    constraint_oid = triggers->triggers[0].tgconstraint;
    if (!OidIsValid(constraint_oid)) return false;
    tuple = SearchSysCache1(CONSTROID, ObjectIdGetDatum(constraint_oid));
    if (!HeapTupleIsValid(tuple)) return false;
    constraint = (Form_pg_constraint) GETSTRUCT(tuple);
    valid = constraint->contype == CONSTRAINT_FOREIGN &&
        constraint->conrelid == RelationGetRelid(target->relation) &&
        constraint->confrelid == RelationGetRelid(source->relation) &&
        strcmp(NameStr(constraint->conname), "table1_id_column_3_fkey") == 0 &&
        constraint->convalidated && !constraint->condeferrable &&
        !constraint->condeferred && !OidIsValid(constraint->conparentid) &&
        constraint->confupdtype == 'c' && constraint->confdeltype == 'c' &&
        constraint->confmatchtype == 's' &&
        pgdm_raw_fk_key(tuple, Anum_pg_constraint_conkey, INT2OID, 10) &&
        pgdm_raw_fk_key(tuple, Anum_pg_constraint_confkey, INT2OID, source->id_att) &&
        pgdm_raw_fk_key(tuple, Anum_pg_constraint_conpfeqop, OIDOID, F_INT8EQ) &&
        pgdm_raw_fk_key(tuple, Anum_pg_constraint_conppeqop, OIDOID, F_INT8EQ) &&
        pgdm_raw_fk_key(tuple, Anum_pg_constraint_conffeqop, OIDOID, F_INT8EQ);
#if PG_VERSION_NUM >= 180000
    valid = valid && constraint->conenforced && !constraint->conperiod;
#endif
    for (i = 0; valid && i < triggers->numtriggers; ++i)
    {
        Trigger *trigger = &triggers->triggers[i];
        bool insert = trigger->tgfoid == F_RI_FKEY_CHECK_INS &&
            trigger->tgtype == (TRIGGER_TYPE_ROW | TRIGGER_TYPE_INSERT);
        bool update = trigger->tgfoid == F_RI_FKEY_CHECK_UPD &&
            trigger->tgtype == (TRIGGER_TYPE_ROW | TRIGGER_TYPE_UPDATE);
        valid = trigger->tgisinternal && trigger->tgenabled == 'O' &&
            trigger->tgconstraint == constraint_oid &&
            trigger->tgconstrrelid == constraint->confrelid &&
            trigger->tgconstrindid == constraint->conindid &&
            !trigger->tgdeferrable && !trigger->tginitdeferred &&
            trigger->tgnargs == 0 && trigger->tgnattr == 0 &&
            trigger->tgqual == NULL && trigger->tgoldtable == NULL &&
            trigger->tgnewtable == NULL &&
            ((insert && !seen_insert) || (update && !seen_update));
        seen_insert |= insert;
        seen_update |= update;
    }
    ReleaseSysCache(tuple);
    if (!valid || !seen_insert || !seen_update) return false;
    target->raw_fk.fence_oid = pgdm_raw_fk_fence(source);
    target->raw_fk.enabled = true;
    target->raw_fk.source_filenode = source->relation->rd_locator.relNumber;
    target->raw_fk.target_filenode = target->relation->rd_locator.relNumber;
    target->raw_fk.source_oid = RelationGetRelid(source->relation);
    target->raw_fk.target_oid = RelationGetRelid(target->relation);
    target->raw_fk.constraint_oid = constraint_oid;
    return true;
}

static void
pgdm_raw_fk_verify(TargetBuffer *target)
{
    int i;
    if (!target->raw_fk.enabled) return;
    /* Read the physical heap tuple retained by table_multi_insert, not values[]. */
    for (i = 0; i < target->used; ++i)
    {
        bool isnull;
        HeapTuple tuple = ExecFetchSlotHeapTuple(target->slots[i], false, NULL);
        Datum actual = heap_getattr(tuple, 10, RelationGetDescr(target->relation), &isnull);
        if (!ItemPointerIsValid(&target->slots[i]->tts_tid) || isnull ||
            DatumGetInt64(actual) != target->raw_fk.witness[i])
            ereport(ERROR, (errcode(ERRCODE_FOREIGN_KEY_VIOLATION),
                            errmsg("inserted table1 raw FK differs from its source tuple witness")));
    }
    target->raw_fk.inserted_rows += target->used;
    target->raw_fk.verified_rows += target->used;
}

static Datum
pgdm_raw_fk_manifest(TargetBuffer *target, Datum groups_json)
{
    PgdmRawFkProof *proof = &target->raw_fk;
    char *groups, *receipt;
    if (!proof->enabled) return groups_json;
    if (proof->verified_rows != target->rows || proof->inserted_rows != target->rows)
        elog(ERROR, "raw FK tuple witness does not cover every inserted table1 row");
    groups = DatumGetCString(DirectFunctionCall1(jsonb_out, groups_json));
    receipt = psprintf(
        "{\"groups\":%s,\"raw_fk_proof\":{\"policy\":\"%s\",\"build_id\":\"%s\","
        "\"source_oid\":%u,\"target_oid\":%u,\"constraint_oid\":%u,\"source_fence_oid\":%u,"
        "\"source_filenode\":%u,\"target_filenode\":%u,"
        "\"definition\":\"FOREIGN KEY (id_column_3) REFERENCES public.raw_data(raw_id) ON UPDATE CASCADE ON DELETE CASCADE\","
        "\"verified_rows\":" INT64_FORMAT ",\"inserted_rows\":" INT64_FORMAT "}}",
        groups, PGDM_RAW_FK_POLICY, PGDM_BULK_BUILD_ID,
        proof->source_oid, proof->target_oid, proof->constraint_oid, proof->fence_oid,
        proof->source_filenode, proof->target_filenode, proof->verified_rows, proof->inserted_rows);
    return DirectFunctionCall1(jsonb_in, CStringGetDatum(receipt));
}
