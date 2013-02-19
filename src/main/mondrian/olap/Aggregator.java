/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2003-2005 Julian Hyde
// Copyright (C) 2005-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.olap;

import mondrian.calc.Calc;
import mondrian.calc.TupleList;
import mondrian.spi.Dialect.Datatype;
import mondrian.spi.SegmentBody;

import java.util.List;

/**
 * Describes an aggregation operator, such as "sum" or "count".
 *
 * @see FunDef
 * @see Evaluator
 *
 * @author jhyde$
 * @since Jul 9, 2003$
 */
public interface Aggregator {
    /**
     * Returns the aggregator used to combine sub-totals into a grand-total.
     *
     * @return aggregator used to combine sub-totals into a grand-total
     */
    Aggregator getRollup();

    /**
     * Applies this aggregator to an expression over a set of members and
     * returns the result.
     *
     * @param evaluator Evaluation context
     * @param members List of members, not null
     * @param calc Expression to evaluate
     *
     * @return result of applying this aggregator to a set of members/tuples
     */
    Object aggregate(Evaluator evaluator, TupleList members, Calc calc);

    /**
     * Tells Mondrian if this aggregator can perform fast aggregation
     * using only the raw data of a given object type. This will
     * determine if Mondrian will attempt to perform in-memory rollups
     * on raw segment data by invoking {@link #aggregate}.
     *
     * <p>This is only invoked for rollup operations.
     *
     * @param datatype The datatype of the object we would like to rollup.
     * @return Whether this aggregator supports fast aggregation
     */
    boolean supportsFastAggregates(Datatype datatype);

    /**
     * Applies this aggregator over a raw list of objects for a rollup
     * operation. This is useful when the values are already resolved
     * and we are dealing with a raw {@link SegmentBody} object.
     *
     * <p>Only gets called if
     * {@link #supportsFastAggregates(mondrian.spi.Dialect.Datatype)} is true.
     *
     * <p>This is only invoked for rollup operations.
     *
     * @param rawData An array of values in its raw form, to be aggregated.
     * @return A rolled up value of the raw data.
     * if the object type is not supported.
     */
    Object aggregate(List<Object> rawData, Datatype datatype);
}

// End Aggregator.java
