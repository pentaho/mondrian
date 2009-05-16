/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2009-2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.calc.Calc;
import mondrian.olap.Exp;
import mondrian.olap.fun.AggregateFunDef;
import mondrian.mdx.ResolvedFunCall;

/**
 * Implementation of {@link mondrian.rolap.RolapCalculation}
 * that wraps a {@link RolapMember calculated member}.
 *
 * @version $Id$
 * @author jhyde
 * @since May 15, 2009
 */
class RolapMemberCalculation implements RolapCalculation {
    private final RolapMember member;
    private final int solveOrder;
    private Boolean containsAggregateFunction;

    /**
     * Creates a RolapMemberCalculation.
     *
     * @param member Calculated member
     */
    public RolapMemberCalculation(RolapMember member) {
        this.member = member;
        // compute and solve order: it is used frequently
        solveOrder = this.member.getSolveOrder();
        assert member.isEvaluated();
    }

    public int hashCode() {
        return member.hashCode();
    }

    public boolean equals(Object obj) {
        return obj instanceof RolapMemberCalculation
            && member == ((RolapMemberCalculation) obj).member;
    }

    public RolapEvaluator pushSelf(RolapEvaluator evaluator) {
        final RolapMember defaultMember =
            evaluator.root.defaultMembers[
                getDimensionOrdinal(evaluator.root.cube)];

        // This method does not need to call
        // RolapEvaluator.removeCalcMember. That happens implicitly when
        // push calls setContext.
        final RolapEvaluator evaluator2 = evaluator.push(defaultMember);
        evaluator2.setExpanding(member);
        return evaluator2;
    }

    public int getSolveOrder() {
        return solveOrder;
    }

    public int getDimensionOrdinal(RolapCube cube) {
        return member.getDimension().getOrdinal(cube);
    }

    public Calc getCompiledExpression(RolapEvaluatorRoot root) {
        final Exp exp = member.getExpression();
        return root.getCompiled(exp, true, null);
    }

    public boolean isCalculatedInQuery() {
        return member.isCalculatedInQuery();
    }

    public boolean containsAggregateFunction() {
        // searching for agg functions is expensive, so cache result
        if (containsAggregateFunction == null) {
            containsAggregateFunction =
                foundAggregateFunction(member.getExpression());
        }
        return containsAggregateFunction;
    }

    /**
     * Returns whether an expression contains a call to an aggregate
     * function such as "Aggregate" or "Sum".
     *
     * @param exp Expression
     * @return Whether expression contains a call to an aggregate function.
     */
    private static boolean foundAggregateFunction(Exp exp) {
        if (exp instanceof ResolvedFunCall) {
            ResolvedFunCall resolvedFunCall = (ResolvedFunCall) exp;
            if (resolvedFunCall.getFunDef() instanceof AggregateFunDef) {
                return true;
            } else {
                for (Exp argExp : resolvedFunCall.getArgs()) {
                    if (foundAggregateFunction(argExp)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }
}

// End RolapMemberCalculation.java
