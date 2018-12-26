#include <postgres.h>
#include <fmgr.h>
#include <utils/tuplesort.h>
#include <catalog/namespace.h>
#include <nodes/parsenodes.h>


#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#define MEDIANDEBUG DEBUG1

/*
Pat Mancuso - Timescale homework project 12/16/2018
Find the median value
*/

PG_FUNCTION_INFO_V1(median_transfn);

typedef struct {
  int64 values_num;   /* Number of entries */
  Tuplesortstate *tuplesort_state;
} MEDIAN_STATE;

#define VALUES_SIZE_START 16
#define MAX_UNSIGNED_INT UINT32_MAX

/*
 * Median state transfer function.
 *
 * This function is called for every value in the set that we are calculating
 * the median for. On first call, the aggregate state, if any, needs to be
 * initialized.
 */
Datum
median_transfn(PG_FUNCTION_ARGS)
{
    MemoryContext agg_context;

    if (!AggCheckCallContext(fcinfo, &agg_context)) {
      elog(ERROR, "median_transfn called in non-aggregate context");
    }

    {
      MEDIAN_STATE *state = (MEDIAN_STATE*) PG_GETARG_POINTER(0);
      MemoryContext old_context = MemoryContextSwitchTo(agg_context);

      if (!state) {
        /* If state is null, this is first call, which means we should create the state */
        state = palloc(sizeof(MEDIAN_STATE));
        if (!state) {
          MemoryContextSwitchTo(old_context);
          elog(ERROR, "median_transfn failed to alloc state");
        }

        state->values_num = 0;

        {
          Oid datumType = get_fn_expr_argtype(fcinfo->flinfo, 1);
          Oid sortOperator = 0;
          Oid sortCollation = fcinfo->fncollation;
          bool nullsFirstFlag = false;
          int workMem = 65536;  /* wild guess - may need to be tuned */
          bool randomAccess = false;
          char *oprname = "<";

          /* Get the sort operator oid (ref: namespace.c) */
          sortOperator = OpernameGetOprid(list_make1(makeString(oprname)), datumType, datumType);

          /* Initialize the tuplesort state */
          state->tuplesort_state = tuplesort_begin_datum(datumType, sortOperator, sortCollation, nullsFirstFlag, workMem, randomAccess);
        }

        if (!state->tuplesort_state) {
          MemoryContextSwitchTo(old_context);
          elog(ERROR, "median_transfn failed to alloc tuplesort_state");
        }
      }

      /* Now that we've got a state, and it's ready to accept the data...store it...if it's not null */
      if (PG_ARGISNULL(1))
      {
        elog(MEDIANDEBUG, "median_transfn ignoring null arg");
      }
      else
      {
        Datum value = PG_GETARG_DATUM(1);
        elog(MEDIANDEBUG, "median_transfn about to add entry");
        tuplesort_putdatum(state->tuplesort_state, value, PG_ARGISNULL(1));
        state->values_num++;
        elog(MEDIANDEBUG, "median_transfn added entry %lld", state->values_num);
      }

      MemoryContextSwitchTo(old_context);
      PG_RETURN_POINTER(state);
    }
}

PG_FUNCTION_INFO_V1(median_finalfn);

/*
 * Median final function.
 *
 * This function is called after all values in the median set has been
 * processed by the state transfer function. It should perform any necessary
 * post processing and clean up any temporary state.
 */
Datum
median_finalfn(PG_FUNCTION_ARGS)
{
    MemoryContext agg_context;

    if (!AggCheckCallContext(fcinfo, &agg_context)) {
      elog(ERROR, "median_finalfn called in non-aggregate context");
    }

    {
      MemoryContext old_context = MemoryContextSwitchTo(agg_context);
      MEDIAN_STATE *state = (MEDIAN_STATE*) PG_GETARG_POINTER(0);
      bool valid_result = false;
      Datum result = {0};
  
      elog(MEDIANDEBUG, "median_finalfn about to retrieve state");

      if (!state) {
        /* State will be null if the table was empty...all done */
        elog(MEDIANDEBUG, "median_finalfn can not retrieve state");
        MemoryContextSwitchTo(old_context);
        PG_RETURN_NULL();
      }
  
      {
        /* cheat a bit here, use the mid value even if it's an even number of total values to avoid trying to average non-numeric values */
        int64 midpoint = (state->values_num+1)/2;

        elog(MEDIANDEBUG, "median_finalfn values_num=%lld midpoint=%lld", state->values_num, midpoint);

        /* do the sort */
        elog(MEDIANDEBUG, "median_finalfn about to do sort");
        tuplesort_performsort(state->tuplesort_state);

        elog(MEDIANDEBUG, "median_finalfn about to skip to result");
        valid_result = tuplesort_skiptuples(state->tuplesort_state, midpoint-1, true);
        if (!valid_result) {
          MemoryContextSwitchTo(old_context);
          elog(ERROR, "median_finalfn can not skip tuples");
        }
        else
        {
          bool forward = true;
          bool isNull = false;
          Datum abbrev = {0};

          elog(MEDIANDEBUG, "median_finalfn about to retrieve result");
          valid_result = tuplesort_getdatum(state->tuplesort_state, forward, &result, &isNull, &abbrev);
          if (!valid_result) {
            MemoryContextSwitchTo(old_context);
            elog(ERROR, "median_finalfn can not get result datum");
          }
          else
          {
            elog(MEDIANDEBUG, "median_finalfn retrieved result");
          }
        }
      }
  
      /* Free the state here */
      if (state && state->tuplesort_state)
      {
        elog(MEDIANDEBUG, "median_finalfn about to end tuplesort state");
        tuplesort_end(state->tuplesort_state);
      }
      if (state)
      {
        elog(MEDIANDEBUG, "median_finalfn about to free state");
        pfree(state);
      }
  
      MemoryContextSwitchTo(old_context);
      if (valid_result) {
        elog(MEDIANDEBUG, "median_finalfn returning valid result");
        PG_RETURN_DATUM(result);
      }
      else
      {
        elog(MEDIANDEBUG, "median_finalfn no valid result, returning null");
        PG_RETURN_NULL();
      }
    }
  }
