/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.calc.impl;

import mondrian.calc.Calc;
import mondrian.calc.IterCalc;
import mondrian.calc.ResultStyle;
import mondrian.olap.Evaluator;
import mondrian.olap.Exp;
import mondrian.olap.Member;
import mondrian.olap.type.SetType;

/**
 * Abstract implementation of the {@link mondrian.calc.IterCalc} interface.
 *
 * <p>The derived class must
 * implement the {@link #evaluateIterable(mondrian.olap.Evaluator)} method,
 * and the {@link #evaluate(mondrian.olap.Evaluator)} method will call it.
 *
 * @author <a>Richard M. Emberson</a>
 * @version $Id$
 * @since Jan 14, 2007
 */

public abstract class AbstractIterCalc
    extends AbstractCalc
    implements IterCalc
{
    protected final boolean tuple;

    /**
     * Creates an abstract implementation of a compiled expression which returns
     * an Iterable.
     *
     * @param exp Expression which was compiled
     * @param calcs List of child compiled expressions (for dependency
     *   analysis)
     */
    protected AbstractIterCalc(Exp exp, Calc[] calcs) {
        super(exp, calcs);
        assert getType() instanceof SetType : "expecting a set: " + getType();
        this.tuple = ((SetType) exp.getType()).getArity() != 1;
    }

    public Object evaluate(Evaluator evaluator) {
        return evaluateIterable(evaluator);
    }

    /**
     * Helper method with which to implement {@link #evaluateIterable}
     * if you have implemented {@link #evaluateMemberIterable} and
     * {@link #evaluateTupleIterable}.
     *
     * @param evaluator Evaluator
     * @return List
     */
    protected Iterable evaluateEitherIterable(Evaluator evaluator) {
        return tuple
            ? evaluateTupleIterable(evaluator)
            : evaluateMemberIterable(evaluator);
    }

    public ResultStyle getResultStyle() {
        return ResultStyle.ITERABLE;
    }

    /**
     * Available implementation of
     * {@link mondrian.calc.MemberIterCalc#evaluateMemberIterable(mondrian.olap.Evaluator)}
     * if the subclass chooses to implement
     * {@link mondrian.calc.MemberIterCalc}.
     *
     * @param evaluator Evaluation context
     * @return A member iterator, never null
     */
    @SuppressWarnings({"unchecked"})
    public Iterable<Member> evaluateMemberIterable(Evaluator evaluator) {
        return (Iterable<Member>) evaluateIterable(evaluator);
    }

    /**
     * Available implementation of
     * {@link mondrian.calc.TupleIterCalc#evaluateTupleIterable(mondrian.olap.Evaluator)}
     * if the subclass chooses to implement
     * {@link mondrian.calc.TupleIterCalc}.
     *
     * @param evaluator Evaluation context
     * @return A tuple iterator, never null
     */
    @SuppressWarnings({"unchecked"})
    public Iterable<Member[]> evaluateTupleIterable(Evaluator evaluator) {
        return (Iterable<Member[]>) evaluateIterable(evaluator);
    }
}

// End AbstractIterCalc.java
