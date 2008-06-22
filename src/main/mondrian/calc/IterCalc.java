/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.calc;

import mondrian.olap.Evaluator;

import java.util.List;

/**
 * Expression which evaluates a set of members or tuples to an Iterable.
 *
 * @author Richard Emberson
 * @version $Id$
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
    Iterable evaluateIterable(Evaluator evaluator);
}

// End IterCalc.java
