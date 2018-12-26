#include <postgres.h>
#include <fmgr.h>

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/*
Pat Mancuso - Timescale homework project 12/16/2018
Find the median value
*/

PG_FUNCTION_INFO_V1(median_transfn);

typedef struct {
  unsigned int values_size;  /* Allocated size of values array */
  unsigned int values_num;   /* Number of active entries in the values array */
  Datum * values;
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
      PG_RETURN_NULL();
    }
    else
    {
      MEDIAN_STATE *state = (MEDIAN_STATE*) PG_GETARG_POINTER(0);

      if (!state) {
          /* If state is null, this is first call, which means we should create the state */
          state = palloc(sizeof(*state));
          if (!state) {
             elog(ERROR, "median_transfn failed to alloc state");
             PG_RETURN_NULL();
          }
          state->values_size = VALUES_SIZE_START;
          state->values_num = 0;
          state->values = (Datum*)(palloc(sizeof(Datum) * state->values_size));
          if (!state->values) {
             elog(ERROR, "median_transfn failed to alloc values array");
             PG_RETURN_NULL();
          }
       }

       if (state->values_size < state->values_num) {
         /* A Very Bad Thing has happened, should never have a number of values stored bigger than the size of the array */
         elog(ERROR, "median_transfn invalid index in values array");
         PG_RETURN_NULL();
       }

       if (state->values_size == state->values_num) {
         /* At max size, need to realloc the values array to make room for new value, double the previous size */
         unsigned int new_values_size = state->values_size * 2;

         if (new_values_size < state->values_size) {
           /* If doubling the size results in a smaller number, the size has overflowed */

           /* one more try...set it to the max */
           new_values_size = MAX_UNSIGNED_INT;

           if (new_values_size == MAX_UNSIGNED_INT) {
             /* if we're already at the max...fail gracefully */
             elog(ERROR, "median_transfn exceeded max size");
             PG_RETURN_NULL();
           }
         }

         /* Try to allocate array to new_values_size length */
         {
           Datum *new_values = (Datum*)(palloc(sizeof(Datum) * new_values_size));
           Datum *old_values = state->values;
         
           if (!new_values) {
             elog(ERROR, "median_transfn failed to alloc values array");
             PG_RETURN_NULL();
           }
           /* copy the old array data to the new array and free the old array */
           memcpy(new_values, old_values, state->values_size * sizeof(Datum));
           state->values = new_values;
           state->values_size = new_values_size;
           pfree(old_values);
         }
      }

      /* Now that we've got a state, and the values array is big enough to accept the data...store it */
      {
        Datum value = PG_GETARG_DATUM(1);
        state->values[state->values_num++] = value;
      }

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
    Datum result = 0;

    if (!AggCheckCallContext(fcinfo, &agg_context)) {
      elog(ERROR, "median_finalfn called in non-aggregate context");
      PG_RETURN_NULL();
    }

    {
    MEDIAN_STATE *state = (MEDIAN_STATE*) PG_GETARG_POINTER(0);

    if (!state) {
      /* A Bad Thing has happened, state should not be null here */
      elog(ERROR, "median_finalfn can not retrieve state");
      PG_RETURN_NULL();
    }

    if (state->values_num == 0) {
      /* No data to operate on */
      elog(ERROR, "median_finalfn no data to operate on");
      if (state->values)
        pfree(state->values);
      if (state)
        pfree(state);
      PG_RETURN_NULL();
    }

    /* TODO: sort the data */

    if (state->values_num % 2) {
      /* median is the middle value if there are an odd number of values... */
      result = state->values[state->values_num/2];
    }
    else
    {
      /* ...or the average of the two values at the middle if there are an odd number of values */
      /* TODO: do the math in terms of Datum */
      result = (state->values[state->values_num/2] + state->values[(state->values_num/2)+1]) / 2;
    }

    /* Free the state here */
    if (state->values)
      pfree(state->values);
    if (state)
      pfree(state);

    }

    PG_RETURN_DATUM(result);
}
