/*
 * Normal-RI execution adapters: PG16 parallel and PG16/18 exact membership.
 *
 * Include this file before pgdm_bulk's _PG_init(), then call
 * pgdm_init_parallel_ri() from that function.
 *
 * This module never writes constraint catalogs and never decides validity.
 * RI_Initial_Check still constructs the query, checks permissions, chooses the
 * snapshot, receives a real violating row, and raises the original RI error.
 * ALTER TABLE still owns the normal constraint transition and its rollback.
 *
 * Two independent PG16 restrictions must be addressed together:
 *   SPI_prepare(...): cursorOptions == 0 excludes parallel planning.
 *   SPI_execute_snapshot(..., tcount = 1): ExecutePlan disables parallel mode.
 *
 * For an exact recognized RI query in a normal VALIDATE utility scope, allow
 * the planner to cost its full result, then wrap its finished plan in Limit 1.
 * Only this registered PlannedStmt may execute with external count = 0.
 * The inner Limit preserves SPI's original at-most-one-row result.
 *
 * Exact membership is a serial CustomScan for a single built-in integer key.
 * The original serial plan remains available for unsupported actual key ranges.
 * Deliberately restricted to reviewed majors, ordinary tables and integer keys.
 * A shape/version mismatch keeps PostgreSQL's ordinary validation behavior.
 */

#include "access/parallel.h"
#include "catalog/pg_constraint.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "optimizer/cost.h"
#include "optimizer/planner.h"
#include "tcop/utility.h"
#include "utils/fmgroids.h"
#include "utils/syscache.h"

#if (PG_VERSION_NUM >= 160000 && PG_VERSION_NUM < 170000) || \
    (PG_VERSION_NUM >= 180000 && PG_VERSION_NUM < 190000)

typedef struct PgdmRiForeignKey
{
    Oid constraint_oid;
    Oid child_oid;
    Oid parent_oid;
    char matchtype;
    int nkeys;
    AttrNumber child_keys[INDEX_MAX_KEYS];
    AttrNumber parent_keys[INDEX_MAX_KEYS];
    Oid equality_ops[INDEX_MAX_KEYS];
    struct PgdmRiForeignKey *next;
} PgdmRiForeignKey;

typedef struct PgdmRiPlan
{
    PlannedStmt *stmt;
    Plan *limited_root;
    QueryDesc *bound_query_desc;
    char *serialized_plan;
    char *source_text;
    PgdmRiForeignKey *fk;
    bool executed;
    struct PgdmRiPlan *next;
} PgdmRiPlan;

typedef struct PgdmRiScope
{
    MemoryContext context;
    PgdmRiForeignKey *keys;
    PgdmRiPlan *plans;
} PgdmRiScope;

typedef struct PgdmRiExecution
{
    int gathers;
    int workers_planned;
    int workers_launched;
    int hashes;
    int parallel_hashes;
} PgdmRiExecution;

static bool pgdm_parallel_ri = false;
static bool pgdm_parallel_ri_trace = false;
static bool pgdm_membership_ri = true;
static PgdmRiScope *pgdm_ri_scope = NULL;
static ProcessUtility_hook_type pgdm_ri_previous_utility = NULL;
static planner_hook_type pgdm_ri_previous_planner = NULL;
#if PG_VERSION_NUM < 170000
static ExecutorStart_hook_type pgdm_ri_previous_executor_start = NULL;
static ExecutorRun_hook_type pgdm_ri_previous_executor_run = NULL;
#endif

#include "pgdm_membership_exec.inc"

/* No command other than a pure VALIDATE statement opens an eligible scope. */
static bool
pgdm_ri_is_validate(PlannedStmt *pstmt)
{
    AlterTableStmt *alter;
    ListCell *cell;

    if (pstmt->utilityStmt == NULL ||
        !IsA(pstmt->utilityStmt, AlterTableStmt))
        return false;
    alter = (AlterTableStmt *) pstmt->utilityStmt;
    if (alter->objtype != OBJECT_TABLE || alter->cmds == NIL)
        return false;
    foreach(cell, alter->cmds)
    {
        AlterTableCmd *command = (AlterTableCmd *) lfirst(cell);
        if (command->subtype != AT_ValidateConstraint)
            return false;
    }
    return true;
}

/*
 * Read only the named, not-yet-validated FK definitions.  Resolve without
 * introducing a new table lock; core immediately acquires its normal locks.
 * If an object changes between these lookups and core's lookup, the complete
 * query/object identity comparison below declines the optimization.
 */
static void
pgdm_ri_collect_keys(AlterTableStmt *alter, PgdmRiScope *scope)
{
    Oid child_oid = RangeVarGetRelid(alter->relation, NoLock, true);
    ListCell *cell;

    if (!OidIsValid(child_oid))
        return;
    foreach(cell, alter->cmds)
    {
        AlterTableCmd *command = (AlterTableCmd *) lfirst(cell);
        Oid constraint_oid;
        HeapTuple tuple;
        Form_pg_constraint form;

        constraint_oid = get_relation_constraint_oid(child_oid,
                                                       command->name, true);
        if (!OidIsValid(constraint_oid))
            continue;
        tuple = SearchSysCache1(CONSTROID, ObjectIdGetDatum(constraint_oid));
        if (!HeapTupleIsValid(tuple))
            continue;
        form = (Form_pg_constraint) GETSTRUCT(tuple);
        if (form->contype == CONSTRAINT_FOREIGN &&
            !form->convalidated &&
            form->conrelid == child_oid &&
            !OidIsValid(form->conparentid) &&
            (form->confmatchtype == FKCONSTR_MATCH_SIMPLE ||
             form->confmatchtype == FKCONSTR_MATCH_FULL))
        {
            PgdmRiForeignKey *fk = palloc0(sizeof(*fk));

            fk->constraint_oid = constraint_oid;
            fk->child_oid = child_oid;
            fk->parent_oid = form->confrelid;
            fk->matchtype = form->confmatchtype;
            DeconstructFkConstraintRow(tuple, &fk->nkeys,
                                       fk->child_keys, fk->parent_keys,
                                       fk->equality_ops, NULL, NULL, NULL, NULL);
            fk->next = scope->keys;
            scope->keys = fk;
        }
        ReleaseSysCache(tuple);
    }
}

static void
pgdm_ri_call_utility(PlannedStmt *pstmt, const char *query_string,
                     bool read_only_tree, ProcessUtilityContext context,
                     ParamListInfo params, QueryEnvironment *query_env,
                     DestReceiver *dest, QueryCompletion *completion)
{
    if (pgdm_ri_previous_utility)
        pgdm_ri_previous_utility(pstmt, query_string, read_only_tree, context,
                                params, query_env, dest, completion);
    else
        standard_ProcessUtility(pstmt, query_string, read_only_tree, context,
                                params, query_env, dest, completion);
}

static void
pgdm_ri_process_utility(PlannedStmt *pstmt, const char *query_string,
                        bool read_only_tree, ProcessUtilityContext context,
                        ParamListInfo params, QueryEnvironment *query_env,
                        DestReceiver *dest, QueryCompletion *completion)
{
    PgdmRiScope scope;
    MemoryContext previous_context;
    MemoryContext scope_context;
    PgdmRiScope *previous_scope;

    if (pgdm_fk_enabled_job == NULL || (!pgdm_parallel_ri && !pgdm_membership_ri) || IsParallelWorker() ||
        !pgdm_ri_is_validate(pstmt))
    {
        pgdm_ri_call_utility(pstmt, query_string, read_only_tree, context,
                             params, query_env, dest, completion);
        return;
    }

    memset(&scope, 0, sizeof(scope));
    previous_scope = pgdm_ri_scope;
    previous_context = CurrentMemoryContext;
    scope_context = AllocSetContextCreate(previous_context,
                                         "pgdm normal RI scope",
                                         ALLOCSET_SMALL_SIZES);
    scope.context = scope_context;
    PG_TRY();
    {
        MemoryContextSwitchTo(scope.context);
        pgdm_ri_collect_keys((AlterTableStmt *) pstmt->utilityStmt, &scope);
        MemoryContextSwitchTo(previous_context);
        pgdm_ri_scope = &scope;
        pgdm_ri_call_utility(pstmt, query_string, read_only_tree, context,
                             params, query_env, dest, completion);
    }
    PG_FINALLY();
    {
        pgdm_ri_scope = previous_scope;
        MemoryContextSwitchTo(previous_context);
        MemoryContextDelete(scope_context);
    }
    PG_END_TRY();
}

static bool
pgdm_ri_is_key_var(Node *node, Index relation, AttrNumber attribute)
{
    Var *var;

    if (node == NULL || !IsA(node, Var))
        return false;
    var = (Var *) node;
    return var->varno == relation &&
           var->varattno == attribute &&
           var->varlevelsup == 0 &&
           (var->vartype == INT2OID ||
            var->vartype == INT4OID ||
            var->vartype == INT8OID);
}

static bool
pgdm_ri_is_nulltest(Node *node, Index relation, AttrNumber attribute,
                    NullTestType test)
{
    NullTest *nulltest;

    if (node == NULL || !IsA(node, NullTest))
        return false;
    nulltest = (NullTest *) node;
    return nulltest->nulltesttype == test && !nulltest->argisrow &&
           pgdm_ri_is_key_var((Node *) nulltest->arg, relation, attribute);
}

static void
pgdm_ri_flatten_boolean(Node *node, BoolExprType operation, List **terms)
{
    if (node && IsA(node, BoolExpr) &&
        ((BoolExpr *) node)->boolop == operation)
    {
        ListCell *cell;
        foreach(cell, ((BoolExpr *) node)->args)
            pgdm_ri_flatten_boolean((Node *) lfirst(cell), operation, terms);
    }
    else
        *terms = lappend(*terms, node);
}

static bool
pgdm_ri_relation_matches(RangeTblEntry *rte, Oid relation, const char *alias)
{
    return rte->rtekind == RTE_RELATION &&
           rte->relid == relation &&
           rte->relkind == RELKIND_RELATION &&
           !rte->inh && !rte->lateral &&
           rte->tablesample == NULL && rte->securityQuals == NIL &&
           rte->alias != NULL &&
           strcmp(rte->alias->aliasname, alias) == 0;
}

/* Match every meaningful clause of core's RI_Initial_Check SELECT. */
static bool
pgdm_ri_query_matches(Query *query, PgdmRiForeignKey *fk)
{
    JoinExpr *join;
    RangeTblEntry *child;
    RangeTblEntry *parent;
    RangeTblEntry *join_rte;
    List *equalities = NIL;
    List *where_terms = NIL;
    List *child_nulltests = NIL;
    ListCell *cell;
    int key = 0;

    if (query->commandType != CMD_SELECT || query->utilityStmt != NULL ||
        query->resultRelation != 0 || query->hasAggs ||
        query->hasWindowFuncs || query->hasTargetSRFs || query->hasSubLinks ||
        query->hasDistinctOn || query->hasRecursive || query->hasModifyingCTE ||
        query->hasForUpdate || query->hasRowSecurity ||
        query->cteList != NIL || query->mergeActionList != NIL ||
        query->onConflict != NULL || query->returningList != NIL ||
        query->groupClause != NIL || query->groupingSets != NIL ||
        query->havingQual != NULL || query->windowClause != NIL ||
        query->distinctClause != NIL || query->sortClause != NIL ||
        query->limitOffset != NULL || query->limitCount != NULL ||
        query->rowMarks != NIL || query->setOperations != NULL ||
        query->withCheckOptions != NIL ||
        list_length(query->rtable) != 3 ||
        list_length(query->targetList) != fk->nkeys ||
        query->jointree == NULL ||
        list_length(query->jointree->fromlist) != 1)
        return false;

    child = (RangeTblEntry *) linitial(query->rtable);
    parent = (RangeTblEntry *) lsecond(query->rtable);
    join_rte = (RangeTblEntry *) lthird(query->rtable);
    if (!pgdm_ri_relation_matches(child, fk->child_oid, "fk") ||
        !pgdm_ri_relation_matches(parent, fk->parent_oid, "pk") ||
        join_rte->rtekind != RTE_JOIN || join_rte->jointype != JOIN_LEFT ||
        !IsA(linitial(query->jointree->fromlist), JoinExpr))
        return false;
    join = (JoinExpr *) linitial(query->jointree->fromlist);
    if (join->jointype != JOIN_LEFT || join->isNatural ||
        join->usingClause != NIL || join->alias != NULL ||
        join->join_using_alias != NULL || join->rtindex != 3 ||
        !IsA(join->larg, RangeTblRef) || !IsA(join->rarg, RangeTblRef) ||
        ((RangeTblRef *) join->larg)->rtindex != 1 ||
        ((RangeTblRef *) join->rarg)->rtindex != 2)
        return false;

    foreach(cell, query->targetList)
    {
        TargetEntry *entry = (TargetEntry *) lfirst(cell);
        if (entry->resjunk || entry->resno != key + 1 ||
            !pgdm_ri_is_key_var((Node *) entry->expr, 1, fk->child_keys[key]))
            return false;
        key++;
    }

    pgdm_ri_flatten_boolean(join->quals, AND_EXPR, &equalities);
    if (list_length(equalities) != fk->nkeys)
        return false;
    key = 0;
    foreach(cell, equalities)
    {
        OpExpr *equality = (OpExpr *) lfirst(cell);
        if (equality == NULL || !IsA(equality, OpExpr) ||
            equality->opno != fk->equality_ops[key] ||
            equality->opresulttype != BOOLOID || equality->opretset ||
            list_length(equality->args) != 2 ||
            !pgdm_ri_is_key_var((Node *) linitial(equality->args),
                                2, fk->parent_keys[key]) ||
            !pgdm_ri_is_key_var((Node *) lsecond(equality->args),
                                1, fk->child_keys[key]))
            return false;
        key++;
    }

    pgdm_ri_flatten_boolean(query->jointree->quals, AND_EXPR, &where_terms);
    if (where_terms == NIL ||
        !pgdm_ri_is_nulltest((Node *) linitial(where_terms),
                             2, fk->parent_keys[0], IS_NULL))
        return false;
    if (fk->matchtype == FKCONSTR_MATCH_FULL && fk->nkeys > 1)
    {
        if (list_length(where_terms) != 2)
            return false;
        pgdm_ri_flatten_boolean((Node *) lsecond(where_terms),
                                OR_EXPR, &child_nulltests);
    }
    else
        child_nulltests = list_copy_tail(where_terms, 1);
    if (list_length(child_nulltests) != fk->nkeys)
        return false;
    key = 0;
    foreach(cell, child_nulltests)
    {
        if (!pgdm_ri_is_nulltest((Node *) lfirst(cell),
                                1, fk->child_keys[key], IS_NOT_NULL))
            return false;
        key++;
    }
    return true;
}

/*
 * Enumerate only the standard plan shapes reachable from the recognized query.
 * Unknown/custom/partitioned/subplan shapes remain valid serial executions.
 * This supplies a genuinely unique node id for the new post-setrefs root.
 */
static bool
pgdm_ri_max_plan_id(Plan *plan, int *maximum)
{
    List *extra_children = NIL;
    ListCell *cell;

    if (plan == NULL)
        return true;
    if (plan->initPlan != NIL)
        return false;
    *maximum = Max(*maximum, plan->plan_node_id);
    switch (nodeTag(plan))
    {
        case T_Result:
        case T_SeqScan:
        case T_IndexScan:
        case T_IndexOnlyScan:
        case T_BitmapHeapScan:
        case T_BitmapIndexScan:
        case T_TidScan:
        case T_TidRangeScan:
        case T_NestLoop:
        case T_MergeJoin:
        case T_HashJoin:
        case T_Hash:
        case T_Material:
        case T_Memoize:
        case T_Sort:
        case T_IncrementalSort:
        case T_Gather:
        case T_GatherMerge:
            break;
        case T_BitmapAnd:
            extra_children = ((BitmapAnd *) plan)->bitmapplans;
            break;
        case T_BitmapOr:
            extra_children = ((BitmapOr *) plan)->bitmapplans;
            break;
        default:
            return false;
    }
    if (!pgdm_ri_max_plan_id(plan->lefttree, maximum) ||
        !pgdm_ri_max_plan_id(plan->righttree, maximum))
        return false;
    foreach(cell, extra_children)
        if (!pgdm_ri_max_plan_id((Plan *) lfirst(cell), maximum))
            return false;
    return true;
}

#if PG_VERSION_NUM < 170000
static Plan *
pgdm_ri_limit_finished_plan(Plan *child, int node_id)
{
    Limit *limit = makeNode(Limit);
    Plan *plan = &limit->plan;
    ListCell *cell;

    plan->startup_cost = child->startup_cost;
    plan->total_cost = child->total_cost;
    plan->plan_rows = Min(child->plan_rows, 1.0);
    plan->plan_width = child->plan_width;
    plan->parallel_safe = child->parallel_safe;
    plan->plan_node_id = node_id;
    plan->lefttree = child;
    plan->extParam = bms_copy(child->extParam);
    plan->allParam = bms_copy(child->allParam);

    /*
     * This is a finished plan, after setrefs.  Use dummy OUTER_VAR references
     * just as set_plan_references would for a normal Limit node.  Limit does
     * not project; these entries retain the exact result tuple descriptor.
     */
    foreach(cell, child->targetlist)
    {
        TargetEntry *source = (TargetEntry *) lfirst(cell);
        TargetEntry *entry = copyObject(source);
        entry->expr = (Expr *) makeVar(OUTER_VAR, source->resno,
                                      exprType((Node *) source->expr),
                                      exprTypmod((Node *) source->expr),
                                      exprCollation((Node *) source->expr), 0);
        plan->targetlist = lappend(plan->targetlist, entry);
    }
    limit->limitCount = (Node *) makeConst(INT8OID, -1, InvalidOid,
                                          sizeof(int64), Int64GetDatum(1),
                                          false, FLOAT8PASSBYVAL);
    limit->limitOption = LIMIT_OPTION_COUNT;
    return plan;
}
#endif

/* Integer bytes represent equality only for the actual built-in equality op. */
static bool
pgdm_ri_membership_types(PgdmRiForeignKey *fk,
                         Oid *child_type, Oid *parent_type)
{
    Oid opcode;

    if (fk->nkeys != 1)
        return false;
    *child_type = get_atttype(fk->child_oid, fk->child_keys[0]);
    *parent_type = get_atttype(fk->parent_oid, fk->parent_keys[0]);
    if (*child_type != *parent_type)
        return false;
    opcode = get_opcode(fk->equality_ops[0]);
    return (*child_type == INT2OID && opcode == F_INT2EQ) ||
           (*child_type == INT4OID && opcode == F_INT4EQ) ||
           (*child_type == INT8OID && opcode == F_INT8EQ);
}

static PlannedStmt *
pgdm_ri_planner(Query *query, const char *query_string,
                int cursor_options, ParamListInfo bound_params)
{
    PgdmRiForeignKey *matched = NULL;
    PlannedStmt *result;
    Oid child_type = InvalidOid;
    Oid parent_type = InvalidOid;
    bool membership = false;
    bool parallel = false;
    int maximum = 0;

    if ((pgdm_parallel_ri || pgdm_membership_ri) && pgdm_ri_scope != NULL &&
        !IsParallelWorker() &&
        (cursor_options & CURSOR_OPT_PARALLEL_OK) == 0)
    {
        PgdmRiForeignKey *fk;
        for (fk = pgdm_ri_scope->keys; fk != NULL; fk = fk->next)
        {
            if (pgdm_ri_query_matches(query, fk))
            {
                matched = fk;
                break;
            }
        }
    }

    if (matched && pgdm_membership_ri)
        membership = pgdm_ri_membership_types(matched, &child_type, &parent_type);
#if PG_VERSION_NUM < 170000
    parallel = matched && !membership && pgdm_parallel_ri &&
               max_parallel_workers_per_gather > 0;
#endif

    /*
     * Do not add LIMIT to Query: that would bias join selection toward
     * returning one estimated orphan, although valid data produces none.
     */
    if (pgdm_ri_previous_planner)
        result = pgdm_ri_previous_planner(query, query_string,
                     cursor_options | (parallel ? CURSOR_OPT_PARALLEL_OK : 0),
                     bound_params);
    else
        result = standard_planner(query, query_string,
                     cursor_options | (parallel ? CURSOR_OPT_PARALLEL_OK : 0),
                     bound_params);

    if (membership && !result->parallelModeNeeded &&
        result->commandType == CMD_SELECT && result->subplans == NIL &&
        result->planTree != NULL &&
        pgdm_ri_max_plan_id(result->planTree, &maximum) && maximum < INT_MAX - 1)
    {
        Plan *original = result->planTree;
        Plan *replacement = pgdm_ri_make_membership_plan(original,
                                    matched, child_type, parent_type, maximum + 1);

        if (replacement == original)
            return result;
        result->planTree = replacement;
        /* No parallel mode or external count rewrite is needed for this plan. */
        result->parallelModeNeeded = false;
        result->jitFlags = 0;
        if (pgdm_parallel_ri_trace)
            elog(LOG, "pgdm_membership_ri planned constraint=%u child=%u parent=%u"
                      " actual_keys=true normal_validate=true",
                 matched->constraint_oid, matched->child_oid, matched->parent_oid);
        return result;
    }

#if PG_VERSION_NUM < 170000
    if (parallel && result->parallelModeNeeded &&
        result->commandType == CMD_SELECT &&
        result->subplans == NIL &&
        result->planTree != NULL &&
        pgdm_ri_max_plan_id(result->planTree, &maximum) &&
        maximum < INT_MAX)
    {
        PgdmRiPlan *record;
        char *serialized;

        result->planTree = pgdm_ri_limit_finished_plan(result->planTree,
                                                      maximum + 1);
        record = MemoryContextAllocZero(pgdm_ri_scope->context,
                                         sizeof(*record));
        record->stmt = result;
        record->limited_root = result->planTree;
        /* SPI's BuildCachedPlan copies this tree after planner_hook returns. */
        serialized = nodeToString(result);
        record->serialized_plan = MemoryContextStrdup(pgdm_ri_scope->context,
                                                       serialized);
        pfree(serialized);
        record->source_text = MemoryContextStrdup(pgdm_ri_scope->context,
                                                   query_string ? query_string : "");
        record->fk = matched;
        record->next = pgdm_ri_scope->plans;
        pgdm_ri_scope->plans = record;
        if (pgdm_parallel_ri_trace)
            elog(LOG, "pgdm_parallel_ri planned constraint=%u child=%u parent=%u"
                      " full_cost_plan=true internal_limit=1",
                 matched->constraint_oid, matched->child_oid,
                 matched->parent_oid);
    }
    else if (matched && pgdm_parallel_ri_trace)
        elog(LOG, "pgdm_parallel_ri serial constraint=%u parallel_plan=%s"
                  " supported_plan=%s",
             matched->constraint_oid,
             result->parallelModeNeeded ? "true" : "false",
             (result->subplans == NIL && result->planTree != NULL &&
              pgdm_ri_max_plan_id(result->planTree, &maximum)) ?
             "true" : "false");
#endif
    return result;
}

#if PG_VERSION_NUM < 170000
/*
 * SPI_prepare is not a one-shot plan: BuildCachedPlan uses copyObject(plist).
 * Match its exact copied tree before executor initialization, then bind the
 * actual QueryDesc/PlannedStmt/root pointers used by ExecutorRun.  Comparing
 * the complete node serialization (not a digest) avoids collision-based plan
 * identity and also works in COPY_PARSE_PLAN_TREES debug builds.
 */
static void
pgdm_ri_executor_start(QueryDesc *query_desc, int eflags)
{
    if (pgdm_parallel_ri && pgdm_ri_scope != NULL &&
        query_desc->operation == CMD_SELECT &&
        query_desc->dest->mydest == DestSPI &&
        (eflags & EXEC_FLAG_SKIP_TRIGGERS) != 0 &&
        query_desc->sourceText != NULL &&
        query_desc->plannedstmt->planTree != NULL &&
        IsA(query_desc->plannedstmt->planTree, Limit))
    {
        PgdmRiPlan *record;
        char *serialized = NULL;

        for (record = pgdm_ri_scope->plans; record; record = record->next)
        {
            if (record->executed || record->bound_query_desc != NULL ||
                strcmp(record->source_text, query_desc->sourceText) != 0)
                continue;
            if (serialized == NULL)
                serialized = nodeToString(query_desc->plannedstmt);
            if (strcmp(record->serialized_plan, serialized) == 0)
            {
                bool copied = record->stmt != query_desc->plannedstmt;

                record->bound_query_desc = query_desc;
                record->stmt = query_desc->plannedstmt;
                record->limited_root = query_desc->plannedstmt->planTree;
                if (pgdm_parallel_ri_trace)
                    elog(LOG, "pgdm_parallel_ri bound constraint=%u copied_plan=%s",
                         record->fk->constraint_oid, copied ? "true" : "false");
                break;
            }
        }
        if (serialized)
            pfree(serialized);
    }

    if (pgdm_ri_previous_executor_start)
        pgdm_ri_previous_executor_start(query_desc, eflags);
    else
        standard_ExecutorStart(query_desc, eflags);
}

static bool
pgdm_ri_execution_walker(PlanState *state, void *context)
{
    PgdmRiExecution *execution = (PgdmRiExecution *) context;

    if (state == NULL)
        return false;
    if (IsA(state, GatherState))
    {
        execution->gathers++;
        execution->workers_planned += ((Gather *) state->plan)->num_workers;
        execution->workers_launched += ((GatherState *) state)->nworkers_launched;
    }
    else if (IsA(state, GatherMergeState))
    {
        execution->gathers++;
        execution->workers_planned += ((GatherMerge *) state->plan)->num_workers;
        execution->workers_launched +=
            ((GatherMergeState *) state)->nworkers_launched;
    }
    else if (IsA(state, HashState))
    {
        execution->hashes++;
        if (state->plan->parallel_aware)
            execution->parallel_hashes++;
    }
    return planstate_tree_walker(state, pgdm_ri_execution_walker, context);
}

static void
pgdm_ri_executor_run(QueryDesc *query_desc, ScanDirection direction,
                     uint64 count, bool execute_once)
{
    PgdmRiPlan *matched = NULL;

    if (pgdm_parallel_ri && pgdm_ri_scope != NULL &&
        count == 1 && execute_once &&
        direction == ForwardScanDirection &&
        !query_desc->already_executed &&
        query_desc->operation == CMD_SELECT &&
        query_desc->dest->mydest == DestSPI &&
        (query_desc->estate->es_top_eflags & EXEC_FLAG_SKIP_TRIGGERS) != 0)
    {
        PgdmRiPlan *record;
        for (record = pgdm_ri_scope->plans; record; record = record->next)
        {
            if (!record->executed && record->bound_query_desc == query_desc &&
                record->stmt == query_desc->plannedstmt &&
                record->limited_root == query_desc->plannedstmt->planTree)
            {
                record->executed = true;
                matched = record;
                count = 0;  /* The exact registered plan already has Limit 1. */
                break;
            }
        }
    }

    if (pgdm_ri_previous_executor_run)
        pgdm_ri_previous_executor_run(query_desc, direction, count, execute_once);
    else
        standard_ExecutorRun(query_desc, direction, count, execute_once);

    if (matched && pgdm_parallel_ri_trace)
    {
        PgdmRiExecution execution;
        memset(&execution, 0, sizeof(execution));
        pgdm_ri_execution_walker(query_desc->planstate, &execution);
        elog(LOG, "pgdm_parallel_ri executed constraint=%u child=%u parent=%u"
                  " returned=" UINT64_FORMAT " gathers=%d"
                  " workers_planned=%d workers_launched=%d"
                  " hashes=%d parallel_hashes=%d",
             matched->fk->constraint_oid, matched->fk->child_oid,
             matched->fk->parent_oid, query_desc->estate->es_processed,
             execution.gathers, execution.workers_planned,
             execution.workers_launched, execution.hashes,
             execution.parallel_hashes);
    }
}
#endif

static void
pgdm_init_parallel_ri(void)
{
    pgdm_ri_register_membership();
    pgdm_ri_previous_utility = ProcessUtility_hook;
    ProcessUtility_hook = pgdm_ri_process_utility;
    pgdm_ri_previous_planner = planner_hook;
    planner_hook = pgdm_ri_planner;
#if PG_VERSION_NUM < 170000
    pgdm_ri_previous_executor_start = ExecutorStart_hook;
    ExecutorStart_hook = pgdm_ri_executor_start;
    pgdm_ri_previous_executor_run = ExecutorRun_hook;
    ExecutorRun_hook = pgdm_ri_executor_run;
#endif
}

#else

/*
 * No behavioral change on other major versions.  Their separately reviewed
 * implementations can be added without breaking the existing extension build.
 */
static void
pgdm_init_parallel_ri(void)
{
}

#endif

