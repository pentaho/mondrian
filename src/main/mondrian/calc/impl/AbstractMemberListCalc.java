/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2008 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.calc.impl;

import mondrian.olap.*;
import mondrian.olap.type.SetType;
import mondrian.calc.*;

import java.util.List;

/**
 * Abstract implementation of the {@link mondrian.calc.ListCalc} interface
 * for expressions that return a list of members but never a list of tuples.
 *
 * <p>The derived class must
 * implement the {@link #evaluateMemberList(mondrian.olap.Evaluator)} method,
 * and the {@link #evaluate(mondrian.olap.Evaluator)} method will call it.
 *
 * @see mondrian.calc.impl.AbstractListCalc
 *
 * @author jhyde
 * @version $Id$
 * @since Feb 20, 2008
 */
public abstract class AbstractMemberListCalc
    extends AbstractCalc
    implements MemberListCalc
{
    private final Calc[] calcs;
    private final boolean mutable;

    /**
     * Creates an abstract implementation of a compiled expression which
     * returns a mutable list of members.
     *
     * @param exp Expression which was compiled
     * @param calcs List of child compiled expressions (for dependency
     *   analysis)
     */
    protected AbstractMemberListCalc(Exp exp, Calc[] calcs) {
        this(exp, calcs, true);
    }

    /**
     * Creates an abstract implementation of a compiled expression which
     * returns a list.
     *
     * @param exp Expression which was compiled
     * @param calcs List of child compiled expressions (for dependency
     *   analysis)
     * @param mutable Whether the list is mutable
     */
    protected AbstractMemberListCalc(Exp exp, Calc[] calcs, boolean mutable) {
        super(exp);
        this.calcs = calcs;
        this.mutable = mutable;
        assert getType() instanceof SetType : "expecting a set: " + getType();
        assert ((SetType) getType()).getArity() == 1;
    }

    public Object evaluate(Evaluator evaluator) {
        final List<Member> memberList = evaluateMemberList(evaluator);
        assert memberList != null : "null as empty memberList is deprecated";
        return memberList;
    }

    public Calc[] getCalcs() {
        return calcs;
    }

    public ResultStyle getResultStyle() {
        return mutable ?
            ResultStyle.MUTABLE_LIST :
            ResultStyle.LIST;
    }

    public String toString() {
        return "AbstractMemberListCalc object";
    }

    public List<Member> evaluateList(Evaluator evaluator) {
        return evaluateMemberList(evaluator);
    }

    public List<Member[]> evaluateTupleList(Evaluator evaluator) {
        throw new UnsupportedOperationException();
    }
}

// End AbstractMemberListCalc.java
