/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2011 Pentaho
// All Rights Reserved.
*/
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
