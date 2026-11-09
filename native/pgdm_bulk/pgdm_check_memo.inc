/*
 * Compile a narrow actual CHECK dependency, then memoize real core successes.
 * The bitmap starts empty. Only a completed ExecConstraints call can learn a
 * value. Every later row still checks actual NOT NULLs and actual scalar bytes.
 */
#ifndef PGDM_CHECK_MEMO_INC
#define PGDM_CHECK_MEMO_INC

#include "nodes/primnodes.h"
#include "nodes/readfuncs.h"

typedef struct PgdmCheckMemo
{
    AttrNumber attribute;
    uint64 passed[5];                 /* 256 one-byte values plus SQL NULL. */
    uint64 hits;
    uint64 learned;
    uint64 uncached;
    const char *reason;
} PgdmCheckMemo;

static bool
pgdm_check_text_type(Oid type)
{
    return type == TEXTOID || type == VARCHAROID;
}

/* Only binary text/varchar relabels; no function, domain or I/O coercion. */
static Node *
pgdm_check_strip_text_relabel(Node *node)
{
    while (node != NULL && IsA(node, RelabelType))
    {
        RelabelType *relabel = (RelabelType *) node;

        if (!pgdm_check_text_type(relabel->resulttype) ||
            relabel->resulttypmod != -1)
            return NULL;
        node = (Node *) relabel->arg;
    }
    return node;
}

static AttrNumber
pgdm_check_scalar_attribute(Node *node, TupleDesc desc)
{
    Var *var;

    node = pgdm_check_strip_text_relabel(node);
    if (node == NULL || !IsA(node, Var))
        return InvalidAttrNumber;
    var = (Var *) node;
    if (var->varno != 1 || var->varlevelsup != 0 ||
        var->varattno <= 0 || var->varattno > desc->natts ||
        !pgdm_check_text_type(var->vartype) ||
        TupleDescAttr(desc, var->varattno - 1)->atttypid != var->vartype ||
        TupleDescAttr(desc, var->varattno - 1)->attisdropped)
        return InvalidAttrNumber;
    return var->varattno;
}

static bool
pgdm_check_literal_text(Node *node)
{
    node = pgdm_check_strip_text_relabel(node);
    return node != NULL && IsA(node, Const) &&
           pgdm_check_text_type(((Const *) node)->consttype);
}

static bool
pgdm_check_array_element_relabel(Node *node)
{
    node = pgdm_check_strip_text_relabel(node);
    return node != NULL && IsA(node, CaseTestExpr) &&
           pgdm_check_text_type(((CaseTestExpr *) node)->typeId);
}

/* Inspect raw ccbin without expression_planner or executing any expression. */
static bool
pgdm_check_literal_array(Node *node)
{
    check_stack_depth();
    if (node == NULL)
        return false;
    if (IsA(node, Const))
    {
        Oid type = ((Const *) node)->consttype;

        return type == TEXTARRAYOID || type == VARCHARARRAYOID;
    }
    if (IsA(node, ArrayExpr))
    {
        ArrayExpr *array = (ArrayExpr *) node;
        ListCell *cell;

        if ((array->array_typeid != TEXTARRAYOID &&
             array->array_typeid != VARCHARARRAYOID) ||
            !pgdm_check_text_type(array->element_typeid) || array->multidims)
            return false;
        foreach(cell, array->elements)
            if (!pgdm_check_literal_text((Node *) lfirst(cell)))
                return false;
        return true;
    }
    if (IsA(node, ArrayCoerceExpr))
    {
        ArrayCoerceExpr *coerce = (ArrayCoerceExpr *) node;

        return (coerce->resulttype == TEXTARRAYOID ||
                coerce->resulttype == VARCHARARRAYOID) &&
               coerce->resulttypmod == -1 &&
               pgdm_check_array_element_relabel((Node *) coerce->elemexpr) &&
               pgdm_check_literal_array((Node *) coerce->arg);
    }
    if (IsA(node, RelabelType))
    {
        RelabelType *relabel = (RelabelType *) node;

        return (relabel->resulttype == TEXTARRAYOID ||
                relabel->resulttype == VARCHARARRAYOID) &&
               relabel->resulttypmod == -1 &&
               pgdm_check_literal_array((Node *) relabel->arg);
    }
    return false;
}

static AttrNumber
pgdm_check_membership_attribute(Node *node, TupleDesc desc)
{
    ScalarArrayOpExpr *member;

    if (node == NULL || !IsA(node, ScalarArrayOpExpr))
        return InvalidAttrNumber;
    member = (ScalarArrayOpExpr *) node;
    if (!member->useOr || list_length(member->args) != 2 ||
        get_opcode(member->opno) != F_TEXTEQ ||
        (OidIsValid(member->opfuncid) && member->opfuncid != F_TEXTEQ) ||
        OidIsValid(member->hashfuncid) || OidIsValid(member->negfuncid) ||
        !pgdm_check_literal_array((Node *) lsecond(member->args)))
        return InvalidAttrNumber;
    return pgdm_check_scalar_attribute((Node *) linitial(member->args), desc);
}

static PgdmCheckMemo *
pgdm_check_memo_create(Relation relation, MemoryContext owner)
{
    TupleDesc desc = RelationGetDescr(relation);
    PgdmCheckMemo *memo;
    Node *node;
    BoolExpr *either;
    NullTest *nullable;
    AttrNumber first, second;
    Node *left, *right;

    if (relation->rd_rel->relchecks <= 0)
        return NULL;
    memo = MemoryContextAllocZero(owner, sizeof(*memo));
    memo->reason = "catalog_count";
    if (desc->constr == NULL || desc->constr->num_check != 1 ||
        relation->rd_rel->relchecks != 1)
        return memo;

    memo->reason = "unsupported_expression";
    node = stringToNode(desc->constr->check[0].ccbin);
    if (node == NULL || !IsA(node, BoolExpr))
        return memo;
    either = (BoolExpr *) node;
    if (either->boolop != OR_EXPR || list_length(either->args) != 2)
        return memo;
    left = (Node *) linitial(either->args);
    right = (Node *) lsecond(either->args);
    if (left == NULL || right == NULL)
        return memo;
    if (!IsA(left, NullTest))
    {
        Node *swap = left;

        left = right;
        right = swap;
    }
    if (!IsA(left, NullTest))
        return memo;
    nullable = (NullTest *) left;
    if (nullable->nulltesttype != IS_NULL || nullable->argisrow)
        return memo;
    first = pgdm_check_scalar_attribute((Node *) nullable->arg, desc);
    second = pgdm_check_membership_attribute(right, desc);
    if (first <= 0 || second != first)
        return memo;
    memo->attribute = first;
    memo->reason = "compiled_actual_scalar";
    return memo;
}

/* The caller must check every actual NOT NULL before calling this probe. */
static bool
pgdm_check_memo_probe(PgdmCheckMemo *memo, Datum *values, bool *nulls,
                      int *learn_key)
{
    int key;

    *learn_key = -1;
    if (memo == NULL || memo->attribute <= 0)
        return false;
    if (nulls[memo->attribute - 1])
        key = 256;
    else
    {
        struct varlena *value =
            (struct varlena *) DatumGetPointer(values[memo->attribute - 1]);

        if (VARATT_IS_EXTERNAL(value) || VARATT_IS_COMPRESSED(value) ||
            VARSIZE_ANY_EXHDR(value) != 1)
        {
            memo->uncached++;
            return false;
        }
        key = (unsigned char) VARDATA_ANY(value)[0];
    }
    *learn_key = key;
    if ((memo->passed[key >> 6] & (UINT64CONST(1) << (key & 63))) != 0)
    {
        memo->hits++;
        return true;
    }
    return false;
}

/* Call only after the original ExecConstraints has returned successfully. */
static void
pgdm_check_memo_learn(PgdmCheckMemo *memo, int key)
{
    if (memo != NULL && memo->attribute > 0 && key >= 0 && key <= 256)
    {
        memo->passed[key >> 6] |= UINT64CONST(1) << (key & 63);
        memo->learned++;
    }
}

static void
pgdm_check_memo_json(StringInfo out, PgdmCheckMemo *memo)
{
    if (memo != NULL)
        appendStringInfo(out, "{\"attribute\":%d,\"state\":\"%s\",\"hits\":" UINT64_FORMAT
                         ",\"core_learned\":" UINT64_FORMAT ",\"uncached\":" UINT64_FORMAT "}",
                         memo->attribute, memo->reason, memo->hits, memo->learned, memo->uncached);
    else
        appendStringInfoString(out, "{}");
}

#endif
