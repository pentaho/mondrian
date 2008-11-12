/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2008-2008 Julian Hyde
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
public interface MemberIterCalc extends IterCalc {
    /**
     * Evaluates an expression to yield a member iterator.
     *
     * @param evaluator Evaluation context
     * @return A member iterator, never null.
     */
    Iterable<Member> evaluateMemberIterable(Evaluator evaluator);

    // override Calc.getType with stricter return type
    SetType getType();
}

// End MemberIterCalc.java
