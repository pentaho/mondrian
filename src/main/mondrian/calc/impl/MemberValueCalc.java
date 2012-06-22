/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2011 Pentaho
// All Rights Reserved.
*/
package mondrian.calc.impl;

import mondrian.calc.Calc;
import mondrian.calc.MemberCalc;
import mondrian.olap.*;
import mondrian.olap.type.ScalarType;
import mondrian.olap.type.Type;

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
 * @since Sep 27, 2005
 */
public class MemberValueCalc extends GenericCalc {
    private final MemberCalc memberCalc;
    private final boolean nullCheck;

    /**
     * Creates a MemberArrayValueCalc.
     *
     * <p>Clients outside this package should use the
     * {@link MemberValueCalc#create(mondrian.olap.Exp,
     * mondrian.calc.MemberCalc[], boolean)}
     * factory method.
     *
     * @param exp Expression
     * @param memberCalc Compiled expression
     * @param nullCheck Whether to check for null values due to non-joining
     *     dimensions in a virtual cube
     */
    public MemberValueCalc(Exp exp, MemberCalc memberCalc, boolean nullCheck) {
        super(exp);
        this.nullCheck = nullCheck;
        final Type type = exp.getType();
        assert type instanceof ScalarType : exp;
        this.memberCalc = memberCalc;
    }

    /**
     * Creates a {@link ValueCalc}, {@link MemberValueCalc} or
     * {@link MemberArrayValueCalc}.
     *
     * @param exp Expression
     * @param memberCalcs Array of members to evaluate
     * @param nullCheck Whether to check for null values due to non-joining
     *     dimensions in a virtual cube
     * @return Compiled expression to evaluate each member expression, set
     *   evaluator context to each resulting member, then evaluate the current
     *   context
     */
    public static GenericCalc create(
        Exp exp,
        MemberCalc[] memberCalcs,
        boolean nullCheck)
    {
        switch (memberCalcs.length) {
        case 0:
            return new ValueCalc(exp);
        case 1:
            return new MemberValueCalc(exp, memberCalcs[0], nullCheck);
        default:
            return new MemberArrayValueCalc(exp, memberCalcs, nullCheck);
        }
    }

    public Object evaluate(Evaluator evaluator) {
        final int savepoint = evaluator.savepoint();
        try {
            final Member member = memberCalc.evaluateMember(evaluator);
            if (member == null
                || member.isNull())
            {
                return null;
            }
            evaluator.setContext(member);
            if (nullCheck
                && evaluator.needToReturnNullForUnrelatedDimension(
                    new Member[] {member}))
            {
                return null;
            }
            final Object result = evaluator.evaluateCurrent();
            return result;
        } finally {
            evaluator.restore(savepoint);
        }
    }

    public Calc[] getCalcs() {
        return new MemberCalc[] {memberCalc};
    }

    public boolean dependsOn(Hierarchy hierarchy) {
        if (super.dependsOn(hierarchy)) {
            return true;
        }
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
        return !memberCalc.getType().usesHierarchy(hierarchy, true);
    }
}

// End MemberValueCalc.java
