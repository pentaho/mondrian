/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2003-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap;

import mondrian.calc.Calc;

import java.util.List;

/**
 * Describes an aggregation operator, such as "sum" or "count".
 *
 * @see FunDef
 * @see Evaluator
 *
 * @author jhyde$
 * @since Jul 9, 2003$
 * @version $Id$
 */
public interface Aggregator {
    /**
     * Returns the aggregator used to combine sub-totals into a grand-total.
     */
    Aggregator getRollup();

    /**
     * Applies this aggregator to an expression over a set of members and
     * returns the result.
     *
     * @param evaluator Evaluation context
     * @param members List of members, not null
     * @param calc Expression to evaluate
     */
    Object aggregate(Evaluator evaluator, List members, Calc calc);
}

// End Aggregator.java
