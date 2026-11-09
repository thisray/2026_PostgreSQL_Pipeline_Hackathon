/* Exact normal-RI plan adapter; complete index sets or the untouched plan. */
#include "nodes/extensible.h"
#include "commands/explain.h"
#if PG_VERSION_NUM >= 180000
#include "commands/explain_format.h"
#endif

enum PgdmRiMembershipPrivate {
 PGDM_RI_MEMBER_CONSTRAINT,PGDM_RI_MEMBER_CHILD,PGDM_RI_MEMBER_PARENT,
 PGDM_RI_MEMBER_CHILD_ATT,PGDM_RI_MEMBER_PARENT_ATT,
 PGDM_RI_MEMBER_CHILD_TYPE,PGDM_RI_MEMBER_PARENT_TYPE,PGDM_RI_MEMBER_PRIVATE_COUNT
};
typedef struct PgdmRiMembershipState {
 CustomScanState css;
 Oid constraint_oid,child_oid,parent_oid,child_type,parent_type;
 AttrNumber child_att,parent_att;
 int eflags;
 bool started,proved;
 PlanState *fallback_state;
 PgdmFkProofUse use;
} PgdmRiMembershipState;
static Node *pgdm_ri_membership_create_state(CustomScan *plan);
static void pgdm_ri_membership_begin(CustomScanState *node,EState *estate,int eflags);
static TupleTableSlot *pgdm_ri_membership_exec(CustomScanState *node);
static void pgdm_ri_membership_end(CustomScanState *node);
static void pgdm_ri_membership_rescan(CustomScanState *node);
static void pgdm_ri_membership_explain(CustomScanState *node,List *ancestors,ExplainState *es);
static const CustomScanMethods pgdm_ri_membership_plan_methods={
 .CustomName="PgdmRiCompleteIndexProof",.CreateCustomScanState=pgdm_ri_membership_create_state
};
static const CustomExecMethods pgdm_ri_membership_exec_methods={
 .CustomName="PgdmRiCompleteIndexProof",.BeginCustomScan=pgdm_ri_membership_begin,
 .ExecCustomScan=pgdm_ri_membership_exec,.EndCustomScan=pgdm_ri_membership_end,
 .ReScanCustomScan=pgdm_ri_membership_rescan,.ExplainCustomScan=pgdm_ri_membership_explain
};
static void pgdm_ri_register_membership(void) {
 RegisterCustomScanMethods(&pgdm_ri_membership_plan_methods);
}
static Const *
pgdm_ri_membership_oid_const(Oid value)
{
    return makeConst(OIDOID, -1, InvalidOid, sizeof(Oid),
                     ObjectIdGetDatum(value), false, true);
}

static Const *
pgdm_ri_membership_att_const(AttrNumber value)
{
    return makeConst(INT2OID, -1, InvalidOid, sizeof(int16),
                     Int16GetDatum(value), false, true);
}

static Plan *
pgdm_ri_make_membership_plan(Plan *original, PgdmRiForeignKey *fk,
                             Oid child_type, Oid parent_type, int node_id)
{
    CustomScan *scan;
    Plan *plan;
    TargetEntry *source;
    TargetEntry *output;
    TargetEntry *scan_output;

    if (original == NULL || fk == NULL || fk->nkeys != 1 ||
        list_length(original->targetlist) != 1 ||
        fk->child_keys[0] <= 0 || fk->parent_keys[0] <= 0 ||
        (child_type != INT2OID && child_type != INT4OID && child_type != INT8OID) ||
        (parent_type != INT2OID && parent_type != INT4OID && parent_type != INT8OID))
        return original;

    source = (TargetEntry *) linitial(original->targetlist);
    if (source->resjunk || source->resno != 1 ||
        exprType((Node *) source->expr) != child_type)
        return original;

    scan = makeNode(CustomScan);
    plan = &scan->scan.plan;
    plan->startup_cost = original->startup_cost;
    plan->total_cost = original->total_cost;
    plan->plan_rows = original->plan_rows;
    plan->plan_width = original->plan_width;
    plan->parallel_aware = false;
    plan->parallel_safe = false;
    plan->plan_node_id = node_id;
    plan->extParam = bms_copy(original->extParam);
    plan->allParam = bms_copy(original->allParam);

    output = copyObject(source);
    output->expr = (Expr *) makeVar(INDEX_VAR, 1, child_type,
                                    exprTypmod((Node *) source->expr),
                                    InvalidOid, 0);
    plan->targetlist = list_make1(output);

    /* The recognized original RI query has child RTE 1 and parent RTE 2. */
    scan_output = copyObject(source);
    scan_output->expr = (Expr *) makeVar(1, fk->child_keys[0], child_type,
                                         exprTypmod((Node *) source->expr),
                                         InvalidOid, 0);
    scan->scan.scanrelid = 0;
    scan->custom_scan_tlist = list_make1(scan_output);
    scan->custom_relids = bms_add_member(bms_make_singleton(1), 2);
    scan->custom_plans = list_make1(original);
    scan->methods = &pgdm_ri_membership_plan_methods;
    scan->custom_private = list_make5(
        pgdm_ri_membership_oid_const(fk->constraint_oid),
        pgdm_ri_membership_oid_const(fk->child_oid),
        pgdm_ri_membership_oid_const(fk->parent_oid),
        pgdm_ri_membership_att_const(fk->child_keys[0]),
        pgdm_ri_membership_att_const(fk->parent_keys[0]));
    scan->custom_private = lappend(scan->custom_private,
                                   pgdm_ri_membership_oid_const(child_type));
    scan->custom_private = lappend(scan->custom_private,
                                   pgdm_ri_membership_oid_const(parent_type));

    return plan;
}
static Datum
pgdm_ri_membership_private(CustomScan *plan, int index, Oid expected_type)
{
    Const *value;

    if (list_length(plan->custom_private) != PGDM_RI_MEMBER_PRIVATE_COUNT)
        elog(ERROR, "invalid pgdm RI membership plan metadata");
    value = (Const *) list_nth(plan->custom_private, index);
    if (!IsA(value, Const) || value->constisnull ||
        value->consttype != expected_type || !value->constbyval)
        elog(ERROR, "invalid pgdm RI membership metadata field %d", index);
    return value->constvalue;
}

static Node *
pgdm_ri_membership_create_state(CustomScan *plan)
{
    PgdmRiMembershipState *state = palloc0(sizeof(*state));

    NodeSetTag(state, T_CustomScanState);
    state->css.methods = &pgdm_ri_membership_exec_methods;
    return (Node *) state;
}

static void
pgdm_ri_membership_begin(CustomScanState *node, EState *estate, int eflags)
{
    PgdmRiMembershipState *state = (PgdmRiMembershipState *) node;
    CustomScan *plan = (CustomScan *) node->ss.ps.plan;

    state->constraint_oid = DatumGetObjectId(pgdm_ri_membership_private(
        plan, PGDM_RI_MEMBER_CONSTRAINT, OIDOID));
    state->child_oid = DatumGetObjectId(pgdm_ri_membership_private(
        plan, PGDM_RI_MEMBER_CHILD, OIDOID));
    state->parent_oid = DatumGetObjectId(pgdm_ri_membership_private(
        plan, PGDM_RI_MEMBER_PARENT, OIDOID));
    state->child_att = DatumGetInt16(pgdm_ri_membership_private(
        plan, PGDM_RI_MEMBER_CHILD_ATT, INT2OID));
    state->parent_att = DatumGetInt16(pgdm_ri_membership_private(
        plan, PGDM_RI_MEMBER_PARENT_ATT, INT2OID));
    state->child_type = DatumGetObjectId(pgdm_ri_membership_private(
        plan, PGDM_RI_MEMBER_CHILD_TYPE, OIDOID));
    state->parent_type = DatumGetObjectId(pgdm_ri_membership_private(
        plan, PGDM_RI_MEMBER_PARENT_TYPE, OIDOID));
    state->eflags = eflags;
    if (list_length(plan->custom_plans) != 1 ||
        node->ss.ss_ScanTupleSlot->tts_tupleDescriptor->natts != 1 ||
        TupleDescAttr(node->ss.ss_ScanTupleSlot->tts_tupleDescriptor, 0)->atttypid != state->child_type)
        elog(ERROR, "invalid pgdm RI membership result descriptor");
}

static TupleTableSlot *
pgdm_ri_membership_exec(CustomScanState *node)
{
 PgdmRiMembershipState *state=(PgdmRiMembershipState *)node;
 if(!state->started)
 {
  CustomScan *plan=(CustomScan *)node->ss.ps.plan;
  MemoryContext old=MemoryContextSwitchTo(node->ss.ps.state->es_query_cxt);
  state->started=true;
  if(!(state->eflags & (EXEC_FLAG_BACKWARD|EXEC_FLAG_MARK)))
   state->proved=pgdm_fk_prove_membership(state->constraint_oid,state->child_oid,state->parent_oid,
    state->child_att,state->parent_att,state->child_type,state->parent_type,&state->use);
  if(!state->proved)
  {
   state->fallback_state=ExecInitNode((Plan *)linitial(plan->custom_plans),node->ss.ps.state,state->eflags);
   node->custom_ps=list_make1(state->fallback_state);
  }
  MemoryContextSwitchTo(old);
 }
 if(state->proved) return ExecClearTuple(node->ss.ss_ScanTupleSlot);
 return ExecProcNode(state->fallback_state);
}
static void
pgdm_ri_membership_end(CustomScanState *node)
{
 PgdmRiMembershipState *state=(PgdmRiMembershipState *)node;
 ereport(LOG,(errmsg("pgdm complete FK constraint=%u proof=%s child_covered=" UINT64_FORMAT
  " parent_covered=" UINT64_FORMAT " bitmap_bytes=" UINT64_FORMAT " fallback=%s",
  state->constraint_oid,state->proved?"true":"false",state->use.child_rows,state->use.parent_rows,
  state->use.bitmap_bytes,state->use.reason?state->use.reason:"executor_flags")));
 if(state->fallback_state) ExecEndNode(state->fallback_state);
 state->fallback_state=NULL;
 node->custom_ps=NIL;
}
static void
pgdm_ri_membership_rescan(CustomScanState *node)
{
 PgdmRiMembershipState *state=(PgdmRiMembershipState *)node;
 if(state->fallback_state) { ExecReScan(state->fallback_state); return; }
 state->started=false;
 state->proved=false;
}
static void
pgdm_ri_membership_explain(CustomScanState *node,List *ancestors,ExplainState *es)
{
 PgdmRiMembershipState *state=(PgdmRiMembershipState *)node;
 (void)ancestors;
 ExplainPropertyBool("Complete FK proof",state->proved,es);
 ExplainPropertyInteger("Child rows covered",NULL,(int64)state->use.child_rows,es);
 ExplainPropertyInteger("Parent rows covered",NULL,(int64)state->use.parent_rows,es);
}
