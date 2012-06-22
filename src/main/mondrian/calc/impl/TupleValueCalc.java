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

import mondrian.calc.*;
import mondrian.olap.*;
import mondrian.olap.fun.TupleFunDef;
import mondrian.olap.type.TupleType;
import mondrian.olap.type.Type;

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
 * @since Sep 27, 2005
 */
public class TupleValueCalc extends GenericCalc {
    private final TupleCalc tupleCalc;
    private final boolean nullCheck;

    /**
     * Creates a TupleValueCalc.
     *
     * @param exp Expression
     * @param tupleCalc Compiled expression to evaluate the tuple
     * @param nullCheck Whether to check for null values due to non-joining
     *     dimensions in a virtual cube
     */
    public TupleValueCalc(Exp exp, TupleCalc tupleCalc, boolean nullCheck) {
        super(exp);
        this.tupleCalc = tupleCalc;
        this.nullCheck = nullCheck;
    }

    public Object evaluate(Evaluator evaluator) {
        final Member[] members = tupleCalc.evaluateTuple(evaluator);
        if (members == null) {
            return null;
        }

        if (nullCheck
            && evaluator.needToReturnNullForUnrelatedDimension(members))
        {
            return null;
        }

        final int savepoint = evaluator.savepoint();
        try {
            evaluator.setContext(members);
            Object result = evaluator.evaluateCurrent();
            return result;
        } finally {
            evaluator.restore(savepoint);
        }
    }

    public Calc[] getCalcs() {
        return new Calc[] {tupleCalc};
    }

    public boolean dependsOn(Hierarchy hierarchy) {
        if (super.dependsOn(hierarchy)) {
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
            if (type.usesHierarchy(hierarchy, true)) {
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
     *   AS ' ([Measures].[Unit Sales], [Time].PreviousMember) '</pre>
     *
     * </code></blockquote>
     *
     * @return optimized expression
     */
    public Calc optimize() {
        if (tupleCalc instanceof TupleFunDef.CalcImpl) {
            TupleFunDef.CalcImpl calc = (TupleFunDef.CalcImpl) tupleCalc;
            return MemberValueCalc.create(
                new DummyExp(type),
                calc.getMemberCalcs(),
                nullCheck);
        }
        return this;
    }
}

// End TupleValueCalc.java
