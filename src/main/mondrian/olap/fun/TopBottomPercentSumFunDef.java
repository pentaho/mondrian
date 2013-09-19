/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde
// Copyright (C) 2005-2011 Pentaho and others
// All Rights Reserved.
*/

package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;

import java.util.List;
import java.util.Map;

/**
 * Definition of the <code>TopPercent</code>, <code>BottomPercent</code>,
 * <code>TopSum</code> and <code>BottomSum</code> MDX builtin functions.
 *
 * @author jhyde
 * @since Mar 23, 2006
 */
class TopBottomPercentSumFunDef extends FunDefBase {
    /**
     * Whether to calculate top (as opposed to bottom).
     */
    final boolean top;
    /**
     * Whether to calculate percent (as opposed to sum).
     */
    final boolean percent;

    static final ResolverImpl TopPercentResolver =
        new ResolverImpl(
            "TopPercent",
            "TopPercent(<Set>, <Percentage>, <Numeric Expression>)",
            "Sorts a set and returns the top N elements whose cumulative total is at least a specified percentage.",
            new String[]{"fxxnn"}, true, true);

    static final ResolverImpl BottomPercentResolver =
        new ResolverImpl(
            "BottomPercent",
            "BottomPercent(<Set>, <Percentage>, <Numeric Expression>)",
            "Sorts a set and returns the bottom N elements whose cumulative total is at least a specified percentage.",
            new String[]{"fxxnn"}, false, true);

    static final ResolverImpl TopSumResolver =
        new ResolverImpl(
            "TopSum",
            "TopSum(<Set>, <Value>, <Numeric Expression>)",
            "Sorts a set and returns the top N elements whose cumulative total is at least a specified value.",
            new String[]{"fxxnn"}, true, false);

    static final ResolverImpl BottomSumResolver =
        new ResolverImpl(
            "BottomSum",
            "BottomSum(<Set>, <Value>, <Numeric Expression>)",
            "Sorts a set and returns the bottom N elements whose cumulative total is at least a specified value.",
            new String[]{"fxxnn"}, false, false);

    public TopBottomPercentSumFunDef(
        FunDef dummyFunDef, boolean top, boolean percent)
    {
        super(dummyFunDef);
        this.top = top;
        this.percent = percent;
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final ListCalc listCalc =
            (ListCalc) compiler.compileAs(
                call.getArg(0),
                null, ResultStyle.MUTABLELIST_ONLY);
        final DoubleCalc doubleCalc = compiler.compileDouble(call.getArg(1));
        final Calc calc = compiler.compileScalar(call.getArg(2), true);
        return new CalcImpl(call, listCalc, doubleCalc, calc);
    }

    private static class ResolverImpl extends MultiResolver {
        private final boolean top;
        private final boolean percent;

        public ResolverImpl(
            final String name, final String signature,
            final String description, final String[] signatures,
            boolean top, boolean percent)
        {
            super(name, signature, description, signatures);
            this.top = top;
            this.percent = percent;
        }

        protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
            return new TopBottomPercentSumFunDef(dummyFunDef, top, percent);
        }
    }

    private class CalcImpl extends AbstractListCalc {
        private final ListCalc listCalc;
        private final DoubleCalc doubleCalc;
        private final Calc calc;

        public CalcImpl(
            ResolvedFunCall call,
            ListCalc listCalc,
            DoubleCalc doubleCalc,
            Calc calc)
        {
            super(call, new Calc[]{listCalc, doubleCalc, calc});
            this.listCalc = listCalc;
            this.doubleCalc = doubleCalc;
            this.calc = calc;
        }

        public TupleList evaluateList(Evaluator evaluator) {
            TupleList list = listCalc.evaluateList(evaluator);
            double target = doubleCalc.evaluateDouble(evaluator);
            if (list.isEmpty()) {
                return list;
            }
            Map<List<Member>, Object> mapMemberToValue =
                evaluateTuples(evaluator, calc, list);
            final int savepoint = evaluator.savepoint();
            try {
                evaluator.setNonEmpty(false);
                list = sortTuples(
                    evaluator,
                    list,
                    list,
                    calc,
                    top,
                    true,
                    getType().getArity());
            } finally {
                evaluator.restore(savepoint);
            }
            if (percent) {
                toPercent(list, mapMemberToValue);
            }
            double runningTotal = 0;
            int memberCount = list.size();
            int nullCount = 0;
            for (int i = 0; i < memberCount; i++) {
                if (runningTotal >= target) {
                    list = list.subList(0, i);
                    break;
                }
                final List<Member> key = list.get(i);
                final Object o = mapMemberToValue.get(key);
                if (o == Util.nullValue) {
                    nullCount++;
                } else if (o instanceof Number) {
                    runningTotal += ((Number) o).doubleValue();
                } else if (o instanceof Exception) {
                    // ignore the error
                } else {
                    throw Util.newInternal(
                        "got " + o + " when expecting Number");
                }
            }

            // MSAS exhibits the following behavior. If the value of all members
            // is null, then the first (or last) member of the set is returned
            // for percent operations.
            if (memberCount > 0 && percent && nullCount == memberCount) {
                return top
                    ? list.subList(0, 1)
                    : list.subList(memberCount - 1, memberCount);
            }
            return list;
        }

        public boolean dependsOn(Hierarchy hierarchy) {
            return anyDependsButFirst(getCalcs(), hierarchy);
        }
    }
}

// End TopBottomPercentSumFunDef.java
