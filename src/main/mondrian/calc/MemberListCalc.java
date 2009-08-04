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

import java.util.List;

import mondrian.olap.Evaluator;
import mondrian.olap.Member;

/**
 * Expression which evaluates a set of members or tuples to a list.
 *
 * @author jhyde
 * @version $Id$
 * @since Sep 27, 2005
 */
public interface MemberListCalc extends ListCalc {
    /**
     * Evaluates an expression to yield a list of members.
     *
     * <p>The list is immutable if {@link #getResultStyle()} yields
     * {@link mondrian.calc.ResultStyle#MUTABLE_LIST}. Otherwise,
     * the caller must not modify the list.
     *
     * @param evaluator Evaluation context
     * @return A list of members, never null.
     */
    List<Member> evaluateMemberList(Evaluator evaluator);
}

// End MemberListCalc.java
