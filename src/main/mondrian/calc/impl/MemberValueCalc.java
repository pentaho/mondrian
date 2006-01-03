/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2006-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.calc.impl;

import mondrian.olap.*;
import mondrian.olap.type.Type;
import mondrian.olap.type.ScalarType;
import mondrian.calc.*;

import java.util.ArrayList;

/**
 * Expression which evaluates a few member expressions,
 * sets the dimensional context to the result of those expressions,
 * then yields the value of the current measure in the current
 * dimensional context.
 *
 * <p>The evaluator's context is preserved.
 *
 * <p>Note that a MemberValueCalc with 0 member expressions is equivalent to a
 * {@link mondrian.calc.impl.ValueCalc}; see also {@link mondrian.calc.impl.TupleValueCalc}.
 *
 * @author jhyde
 * @version $Id$
 * @since Sep 27, 2005
 */
public class MemberValueCalc extends GenericCalc {
    private final MemberCalc[] memberCalcs;
    private final Member[] savedMembers;

    public MemberValueCalc(Exp exp, MemberCalc[] memberCalcs) {
        super(exp);
        final Type type = exp.getType();
        assert type instanceof ScalarType : exp;
        this.memberCalcs = memberCalcs;
        this.savedMembers = new Member[memberCalcs.length];
    }

    public Object evaluate(Evaluator evaluator) {
        for (int i = 0; i < memberCalcs.length; i++) {
            MemberCalc memberCalc = memberCalcs[i];
            final Member member = memberCalc.evaluateMember(evaluator);
            if (member == null ||
                    member.isNull()) {
                return null;
            }
            savedMembers[i] = evaluator.setContext(member);
        }
        final Object o = evaluator.evaluateCurrent();
        evaluator.setContext(savedMembers);
        return o;
    }

    public Calc[] getCalcs() {
        return memberCalcs;
    }

    public boolean dependsOn(Dimension dimension) {
        if (super.dependsOn(dimension)) {
            return true;
        }
        for (int i = 0; i < memberCalcs.length; i++) {
            MemberCalc memberCalc = memberCalcs[i];
            // If the expression
            if (memberCalc.getType().usesDimension(dimension, true)) {
                return false;
            }
        }
        return true;
    }
}

// End MemberValueCalc.java
