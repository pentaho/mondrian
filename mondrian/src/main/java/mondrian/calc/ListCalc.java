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
 * Expression which evaluates a set of members or tuples to a list.
 *
 * @author jhyde
 * @since Sep 27, 2005
 */
public interface ListCalc extends IterCalc {
    /**
     * Evaluates an expression to yield a list of tuples.
     *
     * <p>The list is immutable if {@link #getResultStyle()} yields
     * {@link ResultStyle#MUTABLE_LIST}. Otherwise,
     * the caller must not modify the list.
     *
     * @param evaluator Evaluation context
     * @return A list of tuples, never null.
     */
    TupleList evaluateList(Evaluator evaluator);
}

// End ListCalc.java
