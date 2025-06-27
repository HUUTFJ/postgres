/*-------------------------------------------------------------------------
 *
 * vci_aggmergetranstype.c
 *	  Parallel merge uility routines to merge between aggregate function's
 *    internal transition (state) data.
 *
 * Portions Copyright (c) 2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/vci/executor/vci_aggmergetranstype.c
 */
#include "postgres.h"

#include "access/htup.h"
#include "access/htup_details.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "datatype/timestamp.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/fmgrprotos.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/numeric.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"

#include "vci.h"

#include "vci_executor.h"

#include "postgresql_copy.h"

static Datum merge_intX_accum(PG_FUNCTION_ARGS);
static Datum merge_floatX_accum(PG_FUNCTION_ARGS);
static Datum merge_interval_avg_accum(PG_FUNCTION_ARGS);

static Datum copy_numeric_agg_state(Datum src, bool typByVal, int typLen);
static Datum copy_numeric_avg_agg_state(Datum src, bool typByVal, int typLen);
static Datum copy_poly_num_agg_state(Datum src, bool typByVal, int typLen);
static Datum copy_poly_avg_num_agg_state(Datum src, bool typByVal, int typLen);

/**
 * If Transition data is INTERNALOID, an enum indicating its type
 */
typedef enum vci_aggtranstype_kind
{
	VCI_AGG_NOT_INTERNAL,		/* not INTERNALOID */
	VCI_AGG_NUMERIC_AGG_STATE,	/* INTERNALOID (use NumericAggState struct) */
	VCI_AGG_POLY_NUM_AGG_STATE, /* INTERNALOID (use PolyNumAggState struct) */
	VCI_AGG_POLY_AVG_NUM_AGG_STATE, /* INTERNALOID (use PolyNumAggState without sumx2 struct) */
	VCI_AGG_AVG_NUMERIC_AGG_STATE,	/* INTERNALOID (use NumericAggState without sumx2 struct) */
	VCI_AGG_ARRAY_BUILD_STATE,	/* INTERNALOID (use ArrayBuildState struct) */
} vci_aggtranstype_kind;

/**
 * Record information necessary for parallel aggregation for each transition function
 */
static struct
{
	int			transfn_oid;	/* Transition function's funcoid. Arrays are sorted in ascending order */
	Oid			transtype;		/* Transition data type */
	PGFunction	merge_trans;	/* Function pointer set required for parallel aggregation for each transfn_oid */
	vci_aggtranstype_kind kind; /* If transtype is INTEROID, its details */
}			trans_funcs_table[] = {
	{F_FLOAT4_ACCUM, 1022, merge_floatX_accum, VCI_AGG_NOT_INTERNAL},	/* 208 */
	{F_FLOAT8_ACCUM, 1022, merge_floatX_accum, VCI_AGG_NOT_INTERNAL},	/* 222 */
	{F_INT8INC, 20, int8pl, VCI_AGG_NOT_INTERNAL},	/* 1833 */
	{F_NUMERIC_ACCUM, 2281, numeric_combine, VCI_AGG_NUMERIC_AGG_STATE},	/* 1834 */
	{F_INT2_ACCUM, 2281, numeric_poly_combine, VCI_AGG_POLY_NUM_AGG_STATE}, /* 1836 */
	{F_INT4_ACCUM, 2281, numeric_poly_combine, VCI_AGG_POLY_NUM_AGG_STATE}, /* 1835 */
	{F_INT8_ACCUM, 2281, numeric_combine, VCI_AGG_NUMERIC_AGG_STATE},	/* 1836 */
	{F_INT2_SUM, 20, int8pl, VCI_AGG_NOT_INTERNAL}, /* 1840 */
	{F_INT4_SUM, 20, int8pl, VCI_AGG_NOT_INTERNAL}, /* 1841 */
	{F_INTERVAL_AVG_COMBINE, 2281, merge_interval_avg_accum, VCI_AGG_NOT_INTERNAL}, /* 3325 */
	{F_INT2_AVG_ACCUM, 1016, merge_intX_accum, VCI_AGG_NOT_INTERNAL},	/* 1962 */
	{F_INT4_AVG_ACCUM, 1016, merge_intX_accum, VCI_AGG_NOT_INTERNAL},	/* 1963 */
	{F_INT8INC_ANY, 20, int8pl, VCI_AGG_NOT_INTERNAL},	/* 2804 */
	{F_INT8_AVG_ACCUM, 2281, int8_avg_combine, VCI_AGG_POLY_AVG_NUM_AGG_STATE}, /* 2746 */
	{F_NUMERIC_AVG_ACCUM, 2281, numeric_avg_combine, VCI_AGG_AVG_NUMERIC_AGG_STATE},	/* 2858 */
};

/**
 * If INTEROID, copy functions, SEND function, RECV function
 */
static vci_agg_trans_copy_funcs trans_internal_type_funcs_table[] = {
	{datumCopy, NULL, NULL},	/* VCI_AGG_NOT_INTERNAL */
	{copy_numeric_agg_state, numeric_serialize, numeric_deserialize},	/* VCI_AGG_NUMERIC_AGG_STATE
																		 * */
	{copy_poly_num_agg_state, numeric_poly_serialize, numeric_poly_deserialize},	/* VCI_AGG_NUMERIC_POLY_AGG_STATE
																					 * */
	{copy_poly_avg_num_agg_state, int8_avg_serialize, int8_avg_deserialize},	/* VCI_AGG_POLY_AVG_NUM_AGG_STATE
																				 * */
	{copy_numeric_avg_agg_state, numeric_avg_serialize, numeric_avg_deserialize},	/* VCI_AGG_AVG_NUMERIC_AGG_STATE
																					 * */
};

/**
 * Determine if the given aggregation function is a type that can be supported by VCI
 *
 * @param[in] aggref Pointer to Aggref that holds the aggregate function to be determined
 * @return true if supportable, false if not
 */
bool
vci_is_supported_aggregation(Aggref *aggref)
{
	int			numInputs;
	HeapTuple	aggTuple;
	Form_pg_aggregate aggform;
	AclResult	aclresult;
	Oid			transfn_oid;
	Oid			rettype;
	Oid		   *argtypes;
	int			nargs;
	bool		ret = false;

	/* not UDF */
	if (FirstNormalObjectId <= aggref->aggfnoid)
	{
		elog(DEBUG1, "Aggref contains user-defined aggregation");
		return false;
	}

	/* 0 or 1 input function */
	numInputs = list_length(aggref->args);
	if (1 < numInputs)
	{
		elog(DEBUG1, "Aggref contains an aggregation with 2 or more arguments");
		return false;
	}

	/* Fetch the pg_aggregate row */
	aggTuple = SearchSysCache1(AGGFNOID,
							   ObjectIdGetDatum(aggref->aggfnoid));
	if (!HeapTupleIsValid(aggTuple))
		elog(ERROR, "cache lookup failed for aggregate %u",
			 aggref->aggfnoid);

	aggform = (Form_pg_aggregate) GETSTRUCT(aggTuple);

	aclresult = object_aclcheck(ProcedureRelationId, aggref->aggfnoid, GetUserId(),
								ACL_EXECUTE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, OBJECT_AGGREGATE,
					   get_func_name(aggref->aggfnoid));

	transfn_oid = aggform->aggtransfn;

	/* Check that aggregate owner has permission to call component fns */

	rettype = get_func_signature(transfn_oid, &argtypes, &nargs);

	if ((rettype != INTERNALOID) &&
		(nargs == 2) && (rettype == argtypes[0]) && (rettype == argtypes[1]))
	{
		ret = true;
	}
	else
	{
		int			i;

		for (i = 0; i < lengthof(trans_funcs_table); i++)
		{
			if (transfn_oid == trans_funcs_table[i].transfn_oid)
			{
				ret = true;
				break;
			}
		}
	}

	if (!ret)
		elog(DEBUG1, "Aggref contains not-support aggregation function");

	ReleaseSysCache(aggTuple);

	return ret;
}

/**
 * Obtain the merge and copy-related functions for the transition internal state
 * generated by the given transition function.
 *
 * @param[in]     transfn_oid    specify a transition function
 * @param[out]    merge_trans_p  set the merge function
 * @param[out]    copy_trans_p   set the copy function
 * @param[out]    send_trans_p   set the send function
 * @param[out]    recv_trans_p   set the recv function
 *
 * @retval true  Found the copy and merge function
 * @retval false Not exist
 *
 * @note Output parameters are set only if the return value is true.
 */
bool
vci_set_merge_and_copy_trans_funcs(Oid transfn_oid, PGFunction *merge_trans_p, VciCopyDatumFunc *copy_trans_p, PGFunction *send_trans_p, PGFunction *recv_trans_p)
{
	int			i;

	for (i = 0; i < lengthof(trans_funcs_table); i++)
	{
		if (transfn_oid == trans_funcs_table[i].transfn_oid)
		{
			vci_aggtranstype_kind kind = trans_funcs_table[i].kind;

			*merge_trans_p = trans_funcs_table[i].merge_trans;
			*copy_trans_p = trans_internal_type_funcs_table[kind].copy_trans;
			*send_trans_p = trans_internal_type_funcs_table[kind].send_trans;
			*recv_trans_p = trans_internal_type_funcs_table[kind].recv_trans;

			return true;
		}
	}

	return false;
}

static Datum
merge_intX_accum(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray0 = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType  *transarray1 = PG_GETARG_ARRAYTYPE_P(1);
	Int8TransTypeData *transdata0;
	Int8TransTypeData *transdata1;

	transdata0 = (Int8TransTypeData *) ARR_DATA_PTR(transarray0);
	transdata1 = (Int8TransTypeData *) ARR_DATA_PTR(transarray1);

	transdata0->count += transdata1->count;
	transdata0->sum += transdata1->sum;

	PG_RETURN_ARRAYTYPE_P(transarray0);
}

static Datum
merge_floatX_accum(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray1 = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType  *transarray2 = PG_GETARG_ARRAYTYPE_P(1);
	float8	   *transvalues1;
	float8	   *transvalues2;
	float8		N1,
				Sx1,
				Sxx1,
				N2,
				Sx2,
				Sxx2,
				tmp,
				N,
				Sx,
				Sxx;

	transvalues1 = check_float8_array(transarray1, "merge_floatX_accum", 3);
	transvalues2 = check_float8_array(transarray2, "merge_floatX_accum", 3);

	N1 = transvalues1[0];
	Sx1 = transvalues1[1];
	Sxx1 = transvalues1[2];

	N2 = transvalues2[0];
	Sx2 = transvalues2[1];
	Sxx2 = transvalues2[2];

	/*--------------------
	 * The transition values combine using a generalization of the
	 * Youngs-Cramer algorithm as follows:
	 *
	 *	N = N1 + N2
	 *	Sx = Sx1 + Sx2
	 *	Sxx = Sxx1 + Sxx2 + N1 * N2 * (Sx1/N1 - Sx2/N2)^2 / N;
	 *
	 * It's worth handling the special cases N1 = 0 and N2 = 0 separately
	 * since those cases are trivial, and we then don't need to worry about
	 * division-by-zero errors in the general case.
	 *--------------------
	 */
	if (N1 == 0.0)
	{
		N = N2;
		Sx = Sx2;
		Sxx = Sxx2;
	}
	else if (N2 == 0.0)
	{
		N = N1;
		Sx = Sx1;
		Sxx = Sxx1;
	}
	else
	{
		N = N1 + N2;
		/* Sx = float8_pl(Sx1, Sx2); */
		Sx = Sx1 + Sx2;
		CHECKFLOATVAL(Sx, isinf(Sx1) || isinf(Sx2), true);
		tmp = Sx1 / N1 - Sx2 / N2;
		Sxx = Sxx1 + Sxx2 + N1 * N2 * tmp * tmp / N;
		CHECKFLOATVAL(Sxx, isinf(Sxx1) || isinf(Sxx2), true);
	}

	transvalues1[0] = N;
	transvalues1[1] = Sx;
	transvalues1[2] = Sxx;

	PG_RETURN_ARRAYTYPE_P(transarray1);
}

static Datum
merge_interval_avg_accum(PG_FUNCTION_ARGS)
{
	IntervalAggState *state1;
	IntervalAggState *state2;

	state1 = PG_ARGISNULL(0) ? NULL : (IntervalAggState *) PG_GETARG_POINTER(0);
	state2 = PG_ARGISNULL(1) ? NULL : (IntervalAggState *) PG_GETARG_POINTER(1);


	/* Create the state data on the first call */
	if (state2 == NULL)
		PG_RETURN_POINTER(state1);

	if (state1 == NULL)
	{
		state1 = makeIntervalAggState(fcinfo);

		state1->N = state2->N;
		state1->pInfcount = state2->pInfcount;
		state1->nInfcount = state2->nInfcount;

		state1->sumX.day = state2->sumX.day;
		state1->sumX.month = state2->sumX.month;
		state1->sumX.time = state2->sumX.time;

		PG_RETURN_POINTER(state1);
	}

	state1->N += state2->N;
	state1->pInfcount += state2->pInfcount;
	state1->nInfcount += state2->nInfcount;

	/* Accumulate finite interval values, if any. */
	if (state2->N > 0)
		finite_interval_pl(&state1->sumX, &state2->sumX, &state1->sumX);

	PG_RETURN_POINTER(state1);
}

/*****************************************************************************/
/* numeric                                                                   */
/*****************************************************************************/

/*
 * Copy an accumulator's state.
 *
 * 'dst' is assumed to be uninitialized beforehand.  No attempt is made at
 * freeing old values.
 *
 * copied from src/backend/utils/adt/numeric.c
 */
static void
accum_sum_copy(NumericSumAccum *dst, NumericSumAccum *src)
{
	dst->pos_digits = palloc0(src->ndigits * sizeof(int32));
	dst->neg_digits = palloc0(src->ndigits * sizeof(int32));

	memcpy(dst->pos_digits, src->pos_digits, src->ndigits * sizeof(int32));
	memcpy(dst->neg_digits, src->neg_digits, src->ndigits * sizeof(int32));
	dst->num_uncarried = src->num_uncarried;
	dst->ndigits = src->ndigits;
	dst->weight = src->weight;
	dst->dscale = src->dscale;
}

static Datum
copy_numeric_agg_state(Datum src, bool typByVal, int typLen)
{
	NumericAggState *from,
			   *to;

	from = (NumericAggState *) DatumGetPointer(src);

	if (from == NULL)
		return (Datum) NULL;

	to = palloc0(sizeof(NumericAggState));

	to->calcSumX2 = from->calcSumX2;
	to->agg_context = CurrentMemoryContext;
	to->N = from->N;
	to->NaNcount = from->NaNcount;
	to->pInfcount = from->pInfcount;
	to->nInfcount = from->nInfcount;
	to->maxScale = from->maxScale;
	to->maxScaleCount = from->maxScaleCount;

	accum_sum_copy(&to->sumX, &from->sumX);
	accum_sum_copy(&to->sumX2, &from->sumX2);

	return PointerGetDatum(to);
}

static Datum
copy_numeric_avg_agg_state(Datum src, bool typByVal, int typLen)
{
	NumericAggState *from,
			   *to;

	from = (NumericAggState *) DatumGetPointer(src);

	if (from == NULL)
		return (Datum) NULL;

	to = palloc0(sizeof(NumericAggState));

	to->calcSumX2 = from->calcSumX2;
	to->agg_context = CurrentMemoryContext;
	to->N = from->N;
	to->NaNcount = from->NaNcount;
	to->pInfcount = from->pInfcount;
	to->nInfcount = from->nInfcount;
	to->maxScale = from->maxScale;
	to->maxScaleCount = from->maxScaleCount;

	accum_sum_copy(&to->sumX, &from->sumX);

	return PointerGetDatum(to);
}

static Datum
copy_poly_num_agg_state(Datum src, bool typByVal, int typLen)
{
	PolyNumAggState *from,
			   *to;

	from = (PolyNumAggState *) DatumGetPointer(src);

	if (from == NULL)
		return (Datum) NULL;

	to = palloc0(sizeof(PolyNumAggState));

	to->calcSumX2 = from->calcSumX2;
	to->N = from->N;

#ifdef HAVE_INT128
	to->sumX = from->sumX;
	to->sumX2 = from->sumX2;
#else
	to->agg_context = CurrentMemoryContext;
	accum_sum_copy(&to->sumX, &from->sumX);
	accum_sum_copy(&to->sumX2, &from->sumX2);
#endif

	return PointerGetDatum(to);
}

static Datum
copy_poly_avg_num_agg_state(Datum src, bool typByVal, int typLen)
{
	PolyNumAggState *from,
			   *to;

	from = (PolyNumAggState *) DatumGetPointer(src);

	if (from == NULL)
		return (Datum) NULL;

	to = palloc0(sizeof(PolyNumAggState));

	to->calcSumX2 = from->calcSumX2;
	to->N = from->N;

#ifdef HAVE_INT128
	to->sumX = from->sumX;
#else
	to->agg_context = CurrentMemoryContext;
	accum_sum_copy(&to->sumX, &from->sumX);
#endif

	return PointerGetDatum(to);
}
