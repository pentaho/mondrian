/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.ListCalc;
import mondrian.calc.impl.AbstractDoubleCalc;
import mondrian.calc.impl.GenericCalc;
import mondrian.calc.impl.ValueCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.rolap.RolapAggregator;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

/**
 * Definition of the <code>AGGREGATE</code> MDX function.
 *
 * @author jhyde
 * @since 2005/8/14
 * @version $Id$
 */
public class AggregateFunDef extends AbstractAggregateFunDef {
    static final ReflectiveMultiResolver resolver =
        new ReflectiveMultiResolver(
            "Aggregate", "Aggregate(<Set>[, <Numeric Expression>])",
            "Returns a calculated value using the appropriate aggregate function, based on the context of the query.",
            new String[]{"fnx", "fnxn"},
            AggregateFunDef.class);

    public AggregateFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final ListCalc listCalc = compiler.compileList(call.getArg(0));
        final Calc calc = call.getArgCount() > 1 ?
            compiler.compileScalar(call.getArg(1), true) :
                new ValueCalc(call);
        return new AggregateCalc(call, listCalc, calc);
    }

    public static class AggregateCalc extends AbstractDoubleCalc {
        private final ListCalc listCalc;
        private final Calc calc;

        public AggregateCalc(Exp exp, ListCalc listCalc, Calc calc) {
            super(exp, new Calc[]{listCalc, calc});
            this.listCalc = listCalc;
            this.calc = calc;
        }

        public double evaluateDouble(Evaluator evaluator) {
            Aggregator aggregator =
                (Aggregator) evaluator.getProperty(
                    Property.AGGREGATION_TYPE.name, null);
            if (aggregator == null) {
                throw newEvalException(
                    null,
                    "Could not find an aggregator in the current evaluation context");
            }
            Aggregator rollup = aggregator.getRollup();
            if (rollup == null) {
                throw newEvalException(
                    null,
                    "Don't know how to rollup aggregator '" + aggregator + "'");
            }
            List list = evaluateCurrentList(listCalc, evaluator);
            if (aggregator == RolapAggregator.DistinctCount) {
                // If the list is empty, it means the current context
                // contains no qualifying cells. The result set is empty.
                if (list.size() == 0) {
                    return DoubleNull;
                }

                // Optimize the list
                // E.g.
                // List consists of:
                //  (Gender.[All Gender], [Product].[All Products]),
                //  (Gender.[All Gender].[F], [Product].[All Products].[Drink]),
                //  (Gender.[All Gender].[M], [Product].[All Products].[Food])
                // Can be optimized to:
                //  (Gender.[All Gender], [Product].[All Products])
                //
                // Similar optimization can also be done for list of members.

                if (list.get(0) instanceof Member) {
                    list = makeTupleList(list);
                }
                list = removeOverlappingTupleEntries(list);
                checkIfAggregationSizeIsTooLarge(list);

                // Can't aggregate distinct-count values in the same way
                // which is used for other types of aggregations. To evaluate a
                // distinct-count across multiple members, we need to gather
                // the members together, then evaluate the collection of
                // members all at once. To do this, we postpone evaluation,
                // and create a lambda function containing the members.
                Evaluator evaluator2 =
                    evaluator.pushAggregation((List<Member>) list);
                final Object o = evaluator2.evaluateCurrent();
                final Number number = (Number) o;
                return GenericCalc.numberToDouble(number);
            }
            return (Double) rollup.aggregate(evaluator.push(), list, calc);
        }

        /**
         * In case of distinct count aggregation if a tuple which is a super
         * set of other tuples in the set exists then the child tuples can be
         * ignored.
         *
         * <p>
         * E.g.
         * List consists of:
         *  (Gender.[All Gender], [Product].[All Products]),
         *  (Gender.[All Gender].[F], [Product].[All Products].[Drink]),
         *  (Gender.[All Gender].[M], [Product].[All Products].[Food])
         * Can be optimized to:
         *  (Gender.[All Gender], [Product].[All Products])
         *
         * @param list
         */

        public static List removeOverlappingTupleEntries(List<Member[]> list) {
            List<Member[]> trimmedList = new ArrayList<Member[]>();
            for (Member[] tuple1 : list) {
                if (trimmedList.isEmpty()) {
                    trimmedList.add(tuple1);
                } else {
                    boolean ignore = false;
                    final Iterator<Member[]> iterator = trimmedList.iterator();
                    while (iterator.hasNext()) {
                        Member[] tuple2 = iterator.next();
                        if (isSuperSet(tuple1, tuple2)) {
                            iterator.remove();
                        } else if (isSuperSet(tuple2,  tuple1) ||
                            isEqual(tuple1, tuple2)) {
                            ignore = true;
                            break;
                        }
                    }
                    if (!ignore) {
                        trimmedList.add(tuple1);
                    }
                }
            }
            return trimmedList;
        }

        private static boolean isEqual(Member[] tuple1, Member[] tuple2) {
            for (int i = 0; i < tuple1.length; i++) {
                if (!tuple1[i].getUniqueName().
                    equals(tuple2[i].getUniqueName())) {
                   return false;
                }
            }
            return true; 
        }

        /**
         * Forms a list tuples from a list of members
         * @param list of members
         * @return list of tuples
         */
        public static List<Member[]> makeTupleList(List<Member> list) {
            List<Member[]> tupleList = new ArrayList<Member[]>(list.size());
            for (Member member : list) {
                tupleList.add(new Member[] {member});
            }
            return tupleList;
        }

        /**
         * Returns whether tuple1 is a superset of tuple2
         * @param tuple1
         * @param tuple2
         * @return boolean
         */
        public static boolean isSuperSet(Member[] tuple1, Member[] tuple2) {
            int parentLevelCount = 0;
            for (int i = 0; i < tuple1.length; i++) {
                Member member1 = tuple1[i];
                Member member2 = tuple2[i];

                if (!member2.isChildOrEqualTo(member1)) {
                    return false;
                }
                if (member1.getLevel().getDepth() < member2.getLevel().getDepth()) {
                    parentLevelCount++;
                }
            }
            return parentLevelCount > 0;
        }

        private void checkIfAggregationSizeIsTooLarge(List list) {
            if (list.size() > MondrianProperties.instance().MaxConstraints.get()) {
                throw newEvalException(
                    null,"Distinct Count aggregation is not supported over a " +
                    "large list");
            }
        }

        public Calc[] getCalcs() {
            return new Calc[] {listCalc, calc};
        }

        public boolean dependsOn(Dimension dimension) {
            if (dimension.isMeasures()) {
                return true;
            }
            return anyDependsButFirst(getCalcs(), dimension);
        }
    }
}

// End AggregateFunDef.java
