/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.calc.impl;

import mondrian.olap.*;
import mondrian.olap.type.Type;
import mondrian.olap.type.ScalarType;
import mondrian.calc.*;

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
                // This method needs to leave the evaluator in the same state
                // it found it.
                for (int j = 0; j < i; j++) {
                    evaluator.setContext(savedMembers[j]);
                }
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
        for (MemberCalc memberCalc : memberCalcs) {
            // If the expression definitely includes the dimension (in this
            // case, that means it is a member of that dimension) then we
            // do not depend on the dimension. For example, the scalar value of
            //   [Store].[USA]
            // does not depend on [Store].
            //
            // If the dimensionality of the expression is unknown, then the
            // expression MIGHT include the dimension, so to be safe we have to
            // say that it depends on the given dimension. For example,
            //   Dimensions(3).CurrentMember.Parent
            // may depend on [Store].
            if (memberCalc.getType().usesDimension(dimension, true)) {
                return false;
            }
        }
        return true;
    }
}

// End MemberValueCalc.java
