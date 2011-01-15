/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2010-2011 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.calc;

import mondrian.olap.Member;

import java.util.List;

/**
 * List of members.
 *
 * <h2>Design notes</h2>
 *
 * <ul>
 *
 * <li>Remove {@link mondrian.calc.impl.AbstractListCalc#evaluateList(mondrian.olap.Evaluator)}?
 *
 * <li>Change {@link TupleCalc#evaluateTuple(mondrian.olap.Evaluator)}
 * and {@link mondrian.olap.Evaluator.NamedSetEvaluator#currentTuple()}
 * to List&lt;Member&gt;</li>
 *
 * <li>Search for potential uses of {@link TupleList#get(int, int)}
 *
 * <li>Worth creating {@link TupleList}.addAll(TupleIterator)?
 *
 * </ul>
 *
 * <p>Done</p>
 * <ul>
 * <li>obsolete AbstractTupleListCalc (merge into AbstractListCalc)
 * <li>obsolete AbstractMemberListCalc
 * <li>obsolete AbstractTupleIterCalc (merge into AbstractIterCalc)
 * <li>obsolete AbstractMembeIterCalc
 * <li>obsolete TupleIterCalc (merge into IterCalc)
 * <li>obsolete MemberIterCalc
 * <li>rename IterableTupleListCalc to IterableListCalc
 * <li>obsolete IterableMemberListCalc
 *
 * </ul>
 */
public interface MemberList extends List<Member> {
}

// End MemberList.java
