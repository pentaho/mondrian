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
import mondrian.olap.fun.TupleFunDef;
import mondrian.olap.type.TupleType;
import mondrian.calc.impl.MemberValueCalc;
import mondrian.calc.*;

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
    private final Member[] savedMembers;

    public TupleValueCalc(Exp exp, TupleCalc tupleCalc) {
        super(exp);
        this.tupleCalc = tupleCalc;
        final TupleType tupleType = (TupleType) this.tupleCalc.getType();
        this.savedMembers = new Member[tupleType.elementTypes.length];
    }

    public Object evaluate(Evaluator evaluator) {
        final Member[] members = tupleCalc.evaluateTuple(evaluator);
        if (members == null) {
            return null;
        }
        for (int i = 0; i < members.length; i++) {
            savedMembers[i] = evaluator.setContext(members[i]);
        }
        final Object o = evaluator.evaluateCurrent();
        evaluator.setContext(savedMembers);
        return o;
    }

    public Calc[] getCalcs() {
        return new Calc[] {tupleCalc};
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
