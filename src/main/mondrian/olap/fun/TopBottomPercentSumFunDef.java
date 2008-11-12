/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2002 Kana Software, Inc.
// Copyright (C) 2002-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
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
 * @version $Id$
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

    static final ResolverImpl TopPercentResolver = new ResolverImpl(
            "TopPercent",
            "TopPercent(<Set>, <Percentage>, <Numeric Expression>)",
            "Sorts a set and returns the top N elements whose cumulative total is at least a specified percentage.",
            new String[]{"fxxnn"}, true, true);

    static final ResolverImpl BottomPercentResolver = new ResolverImpl(
            "BottomPercent",
            "BottomPercent(<Set>, <Percentage>, <Numeric Expression>)",
            "Sorts a set and returns the bottom N elements whose cumulative total is at least a specified percentage.",
            new String[]{"fxxnn"}, false, true);

    static final ResolverImpl TopSumResolver = new ResolverImpl(
            "TopSum",
            "TopSum(<Set>, <Value>, <Numeric Expression>)",
            "Sorts a set and returns the top N elements whose cumulative total is at least a specified value.",
            new String[]{"fxxnn"}, true, false);

    static final ResolverImpl BottomSumResolver = new ResolverImpl(
            "BottomSum",
            "BottomSum(<Set>, <Value>, <Numeric Expression>)",
            "Sorts a set and returns the bottom N elements whose cumulative total is at least a specified value.",
            new String[]{"fxxnn"}, false, false);

    public TopBottomPercentSumFunDef(
            FunDef dummyFunDef, boolean top, boolean percent) {
        super(dummyFunDef);
        this.top = top;
        this.percent = percent;
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final ListCalc listCalc = (ListCalc) compiler.compileAs(call.getArg(0),
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
                boolean top, boolean percent) {
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

        public CalcImpl(ResolvedFunCall call, ListCalc listCalc, DoubleCalc doubleCalc, Calc calc) {
            super(call, new Calc[]{listCalc, doubleCalc, calc});
            this.listCalc = listCalc;
            this.doubleCalc = doubleCalc;
            this.calc = calc;
        }

        public List evaluateList(Evaluator evaluator) {
            List list = listCalc.evaluateList(evaluator);
            double target = doubleCalc.evaluateDouble(evaluator);
            if (list.isEmpty()) {
                return list;
            }
            Map mapMemberToValue;
            Object first = list.get(0);
            boolean isMember = true;
            if (first instanceof Member) {
                List<Member> memberList = (List<Member>) list;
                mapMemberToValue =
                    evaluateMembers(evaluator, calc, memberList, null, false);
                sortMembers(
                    evaluator,
                    memberList,
                    memberList,
                    calc,
                    top,
                    true);
            } else {
                isMember = false;
                List<Member[]> tupleList = (List<Member[]>) list;
                mapMemberToValue =
                    evaluateTuples(evaluator, calc, tupleList);
                int arity = ((Member[]) first).length;
                sortTuples(
                    evaluator,
                    tupleList,
                    tupleList,
                    calc,
                    top,
                    true,
                    arity);
            }
            if (percent) {
                toPercent(list, mapMemberToValue, isMember);
            }
            double runningTotal = 0;
            int memberCount = list.size();
            int nullCount = 0;
            for (int i = 0; i < memberCount; i++) {
                if (runningTotal >= target) {
                    list = list.subList(0, i);
                    break;
                }
                Object o = (isMember)
                    ? mapMemberToValue.get(list.get(i))
                    : mapMemberToValue.get(
                        new ArrayHolder<Member>((Member []) list.get(i)));
                if (o == Util.nullValue) {
                    nullCount++;
                } else if (o instanceof Number) {
                    runningTotal += ((Number) o).doubleValue();
                } else if (o instanceof Exception) {
                    // ignore the error
                } else {
                    throw Util.newInternal("got " + o + " when expecting Number");
                }
            }

            // MSAS exhibits the following behavior. If the value of all members is
            // null, then the first (or last) member of the set is returned for percent
            // operations.
            if (memberCount > 0 && percent && nullCount == memberCount) {
                return top ?
                        list.subList(0, 1) :
                        list.subList(memberCount - 1, memberCount);
            }
            return list;
        }

        public boolean dependsOn(Dimension dimension) {
            return anyDependsButFirst(getCalcs(), dimension);
        }
    }
}

// End TopBottomPercentSumFunDef.java
