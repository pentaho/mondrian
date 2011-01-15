/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2006-2011 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.calc;

import mondrian.olap.Evaluator;

/**
 * Expression which evaluates a set of members or tuples to a list.
 *
 * @author jhyde
 * @version $Id$
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
