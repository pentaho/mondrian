/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.calc.impl;

import mondrian.olap.*;
import mondrian.olap.fun.TupleFunDef;
import mondrian.olap.type.TupleType;
import mondrian.olap.type.Type;
import mondrian.rolap.RolapCube;
import mondrian.rolap.RolapStoredMeasure;
import mondrian.calc.TupleCalc;
import mondrian.calc.Calc;
import mondrian.calc.DummyExp;

import java.util.Set;
import java.util.List;

/**
 * Expression which evaluates a tuple expression,
 * sets the dimensional context to the result of that expression,
 * then yields the value of the current measure in the current
 * dimensional context.
 *
 * <p>The evaluator's context is preserved.
 *
 * @see mondrian.calc.impl.ValueCalc
 * @see mondrian.calc.impl.MemberValueCalc
 *
 * @author jhyde
 * @version $Id$
 * @since Sep 27, 2005
 */
public class TupleValueCalc extends GenericCalc {
    private final TupleCalc tupleCalc;

    public TupleValueCalc(Exp exp, TupleCalc tupleCalc) {
        super(exp);
        this.tupleCalc = tupleCalc;
    }

    public Object evaluate(Evaluator evaluator) {
        final Member[] members = tupleCalc.evaluateTuple(evaluator);
        if (members == null) {
            return null;
        }

        final boolean needToReturnNull =
            handleUnrelatedDimension(evaluator, members);
        if (needToReturnNull) {
            return null;
        }

        Member [] savedMembers = new Member[members.length];
        for (int i = 0; i < members.length; i++) {
            savedMembers[i] = evaluator.setContext(members[i]);
        }
        final Object o = evaluator.evaluateCurrent();
        evaluator.setContext(savedMembers);
        return o;
    }

    private boolean handleUnrelatedDimension(Evaluator evaluator, Member[] members) {
        if (MondrianProperties.instance()
            .IgnoreMeasureForNonJoiningDimension.get()) {
            RolapCube cube =
                ((RolapStoredMeasure)evaluator.getMembers()[0]).getCube();
            Set<Dimension> nonJoiningDimensions =
                cube.nonJoiningDimensions(members);
            if (!nonJoiningDimensions.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    public Calc[] getCalcs() {
        return new Calc[] {tupleCalc};
    }

    public boolean dependsOn(Dimension dimension) {
        if (super.dependsOn(dimension)) {
            return true;
        }
        for (Type type : ((TupleType) tupleCalc.getType()).elementTypes) {
            // If the expression definitely includes the dimension (in this
            // case, that means it is a member of that dimension) then we
            // do not depend on the dimension. For example, the scalar value of
            //   ([Store].[USA], [Gender].[F])
            // does not depend on [Store].
            //
            // If the dimensionality of the expression is unknown, then the
            // expression MIGHT include the dimension, so to be safe we have to
            // say that it depends on the given dimension. For example,
            //   (Dimensions(3).CurrentMember.Parent, [Gender].[F])
            // may depend on [Store].
            if (type.usesDimension(dimension, true)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Optimizes the scalar evaluation of a tuple. It evaluates the members
     * of the tuple, sets the context to these members, and evaluates the
     * scalar result in one step, without generating a tuple.<p/>
     *
     * This is useful when evaluating calculated members:<blockquote><code>
     *
     * <pre>WITH MEMBER [Measures].[Sales last quarter]
     *   AS ' ([Measures].[Unit Sales], [Time].PreviousMember '</pre>
     *
     * </code></blockquote>
     *
     * @return optimized expression
     */
    public Calc optimize() {
        if (tupleCalc instanceof TupleFunDef.CalcImpl) {
            TupleFunDef.CalcImpl calc = (TupleFunDef.CalcImpl) tupleCalc;
            return new MemberValueCalc(
                    new DummyExp(type), calc.getMemberCalcs());
        }
        return this;
    }
}

// End TupleValueCalc.java
