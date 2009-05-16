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
import mondrian.olap.Hierarchy;

import java.util.List;

/**
 * Implementation of {@link mondrian.rolap.RolapCalculation}
 * that changes one or more dimensions, then evaluates a given calculation.
 *
 * <p>It is used to implement sets in slicers, in particular sets of tuples in
 * the slicer.
 *
 * @version $Id$
 * @author jhyde
 * @since May 15, 2009
 */
class RolapTupleCalculation implements RolapCalculation {
    private final List<Hierarchy> hierarchyList;
    private final Calc calc;

    /**
     * Creates a RolapTupleCalculation.
     *
     * @param hierarchyList List of hierarchies to be replaced.
     * @param calc Compiled scalar expression to compute cell
     */
    public RolapTupleCalculation(List<Hierarchy> hierarchyList, Calc calc) {
        this.hierarchyList = hierarchyList;
        this.calc = calc;
    }

    public RolapEvaluator pushSelf(RolapEvaluator evaluator) {
        final RolapEvaluator evaluator2 = evaluator.push();
        // Restore default member for each hierarchy
        // in the tuple.
        for (Hierarchy hierarchy : hierarchyList) {
            final int ordinal =
                hierarchy.getDimension().getOrdinal(
                    evaluator.root.cube);
            final RolapMember defaultMember =
                evaluator.root.defaultMembers[ordinal];
            evaluator2.setContext(defaultMember);
        }

        evaluator2.removeCalcMember(this);
        return evaluator2;
    }

    public int getSolveOrder() {
        return Integer.MIN_VALUE;
    }

    public int getDimensionOrdinal(RolapCube cube) {
        throw new UnsupportedOperationException();
    }

    public Calc getCompiledExpression(RolapEvaluatorRoot root) {
        return calc;
    }

    public boolean containsAggregateFunction() {
        return false;
    }

    public boolean isCalculatedInQuery() {
        return true;
    }
}

// End RolapTupleCalculation.java
