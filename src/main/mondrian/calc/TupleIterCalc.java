/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2008-2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.calc;

import mondrian.olap.Evaluator;
import mondrian.olap.Member;
import mondrian.olap.type.SetType;

/**
 * Expression which evaluates to an iterator over a set of members.
 *
 * @author jhyde
 * @version $Id$
 * @since Oct 26, 2008
 */
public interface TupleIterCalc extends IterCalc {
    /**
     * Evaluates an expression to yield an iterator over tuples (arrays of
     * members).
     *
     * @param evaluator Evaluation context
     * @return A tuple iterator, never null.
     */
    Iterable<Member []> evaluateTupleIterable(Evaluator evaluator);

    // override Calc.getType with stricter return type
    SetType getType();
}

// End TupleIterCalc.java
