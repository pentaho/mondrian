/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

package mondrian.calc;

import mondrian.olap.Evaluator;

/**
 * Expression that evaluates a set of tuples to a {@link TupleIterable}.
 *
 * @author Richard Emberson
 * @since Jan 11, 2007
 */
public interface IterCalc extends Calc {
    /**
     * Evaluates an expression to yield an Iterable of members or tuples.
     *
     * <p>The Iterable is immutable.
     *
     * @param evaluator Evaluation context
     * @return An Iterable of members or tuples, never null.
     */
    TupleIterable evaluateIterable(Evaluator evaluator);
}

// End IterCalc.java
