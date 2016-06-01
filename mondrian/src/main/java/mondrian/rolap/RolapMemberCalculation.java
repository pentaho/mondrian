/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.rolap;

import mondrian.calc.Calc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Exp;
import mondrian.olap.fun.AggregateFunDef;

/**
 * Implementation of {@link mondrian.rolap.RolapCalculation}
 * that wraps a {@link RolapMember calculated member}.
 *
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

    public void setContextIn(RolapEvaluator evaluator) {
        final RolapMember defaultMember =
            evaluator.root.defaultMembers[getHierarchyOrdinal()];

        // This method does not need to call RolapEvaluator.removeCalcMember.
        // That happens implicitly in setContext.
        evaluator.setContext(defaultMember);
        evaluator.setExpanding(member);
    }

    public int getSolveOrder() {
        return solveOrder;
    }

    public int getHierarchyOrdinal() {
        return member.getHierarchy().getOrdinalInCube();
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
