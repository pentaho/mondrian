/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2008-2008 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.calc.impl;

import mondrian.olap.Evaluator;
import mondrian.olap.Member;
import mondrian.calc.DummyExp;
import mondrian.calc.Calc;
import mondrian.calc.MemberIterCalc;

import java.util.List;
import java.util.ArrayList;

/**
 * Adapter that converts a {@link mondrian.calc.MemberIterCalc} to a
 * {@link mondrian.calc.MemberListCalc}.
 *
 * @author jhyde
 * @version $Id$
 * @since Oct 23, 2008
 */
public class IterableMemberListCalc extends AbstractMemberListCalc {
    private final MemberIterCalc iterCalc;

    /**
     * Creates a IterableMemberListCalc.
     *
     * @param iterCalc Calculation that returns an iterable over members.
     */
    public IterableMemberListCalc(MemberIterCalc iterCalc) {
        super(new DummyExp(iterCalc.getType()), new Calc[] {iterCalc});
        this.iterCalc = iterCalc;
    }

    public List<Member> evaluateMemberList(Evaluator evaluator) {
        final List<Member> list = new ArrayList<Member>();
        for (Member o : iterCalc.evaluateMemberIterable(evaluator)) {
            list.add(o);
        }
        return list;
    }
}

// End IterableMemberListCalc.java
