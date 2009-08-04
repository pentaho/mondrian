/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2006-2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.calc;

import mondrian.olap.Evaluator;
import mondrian.olap.Member;

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
