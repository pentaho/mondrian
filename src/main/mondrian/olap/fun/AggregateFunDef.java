/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2008 Julian Hyde
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
import mondrian.rolap.RolapEvaluator;

import java.util.*;

import org.eigenbase.util.property.IntegerProperty;

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
                List<Member[]> tupleList;
                if (list.get(0) instanceof Member) {
                    tupleList = makeTupleList((List<Member>)list);
                } else {
                    tupleList =  (List<Member[]>) list;
                }

                RolapEvaluator rolapEvaluator = null;
                if (evaluator instanceof RolapEvaluator) {
                    rolapEvaluator = (RolapEvaluator) evaluator;
                }

                if ((rolapEvaluator != null) &&
                    rolapEvaluator.getDialect().supportsUnlimitedValueList()) {
                    // If the DBMS does not have an upper limit on IN list
                    // predicate size, then don't attempt any list
                    // optimization, since the current algorithm is
                    // very slow.  May want to revisit this if someone
                    // improves the algorithm.
                } else {
                    // FIXME: We remove overlapping tuple entries only to pass
                    // AggregationOnDistinctCountMeasuresTest
                    // .testOptimizeListWithTuplesOfLength3 on Access. Without
                    // the optimization, we generate a statement 7000
                    // characters long and Access gives "Query is too complex".
                    // The optimization is expensive, so we only want to do it
                    // if the DBMS can't execute the query otherwise.
                    if ((rolapEvaluator != null) &&
                        rolapEvaluator.getDialect().isAccess() &&
                        false) {
                        tupleList = removeOverlappingTupleEntries(tupleList);
                    }
                    if (true) {
                        tupleList =
                            optimizeChildren(
                                tupleList,
                                evaluator.getSchemaReader(),
                                evaluator.getMeasureCube());
                    }
                    checkIfAggregationSizeIsTooLarge(tupleList);
                }

                // Can't aggregate distinct-count values in the same way
                // which is used for other types of aggregations. To evaluate a
                // distinct-count across multiple members, we need to gather
                // the members together, then evaluate the collection of
                // members all at once. To do this, we postpone evaluation,
                // and create a lambda function containing the members.
                Evaluator evaluator2 =
                    evaluator.pushAggregation(tupleList);
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

        public static List<Member[]> removeOverlappingTupleEntries(List<Member[]> list) {
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

        private void checkIfAggregationSizeIsTooLarge(List list) {
            final IntegerProperty property =
                MondrianProperties.instance().MaxConstraints;
            final int maxConstraints = property.get();
            if (list.size() > maxConstraints) {
                throw newEvalException(
                    null,
                    "Distinct Count aggregation is not supported over a list"
                        + " with more than " + maxConstraints + " predicates"
                        + " (see property " + property.getPath() + ")");
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

        /**
         * In distinct Count aggregation, if tuple list is a result
         * m.children * n.children then it can be optimized to m * n
         *
         * <p>
         * E.g.
         * List consist of:
         *  (Gender.[All Gender].[F], [Store].[All Stores].[USA]),
         *  (Gender.[All Gender].[F], [Store].[All Stores].[USA].[OR]),
         *  (Gender.[All Gender].[F], [Store].[All Stores].[USA].[CA]),
         *  (Gender.[All Gender].[F], [Store].[All Stores].[USA].[WA]),
         *  (Gender.[All Gender].[F], [Store].[All Stores].[CANADA])
         *  (Gender.[All Gender].[M], [Store].[All Stores].[USA]),
         *  (Gender.[All Gender].[M], [Store].[All Stores].[USA].[OR]),
         *  (Gender.[All Gender].[M], [Store].[All Stores].[USA].[CA]),
         *  (Gender.[All Gender].[M], [Store].[All Stores].[USA].[WA]),
         *  (Gender.[All Gender].[M], [Store].[All Stores].[CANADA])
         * Can be optimized to:
         *  (Gender.[All Gender], [Store].[All Stores].[USA])
         *  (Gender.[All Gender], [Store].[All Stores].[CANADA])
         *
         * @param tuples Tuples
         * @param reader Schema reader
         * @param baseCubeForMeasure Cube
         * @return xxxx
         */
        public static List<Member[]> optimizeChildren(
            List<Member[]> tuples,
            SchemaReader reader,
            Cube baseCubeForMeasure)
        {
            Map<Member, Integer>[] membersOccurencesInTuple =
                membersVersusOccurencesInTuple(tuples);
            int tupleLength = tuples.get(0).length;

            //noinspection unchecked
            Set<Member>[] sets = new HashSet[tupleLength];
            boolean optimized = false;
            for (int i = 0; i < tupleLength; i++) {
                if (areOccurencesEqual(membersOccurencesInTuple[i].values())) {
                    Set<Member> members = membersOccurencesInTuple[i].keySet();
                    int originalSize = members.size();
                    sets[i] =
                        optimizeMemberSet(
                            new HashSet<Member>(members),
                            reader,
                            baseCubeForMeasure);
                    if (sets[i].size() != originalSize) {
                        optimized = true;
                    }
                }
            }
            if (optimized) {
                if (sets.length == 1) {
                    Set<Member> set = sets[0];
                    List<Member[]> tupleList =
                        new ArrayList<Member[]>(set.size());
                    for (Member member : set) {
                        tupleList.add(new Member[] {member});
                    }
                    return tupleList;
                }
                return crossProd(sets);
            }
            return tuples;
        }

        /**
         * Finds member occurrences in tuple and generates a map of Members
         * versus their occurrences in tuples.
         *
         * @param tuples List of tuples
         * @return Map of the number of occurrences of each member in a tuple
         */
        public static Map<Member, Integer>[] membersVersusOccurencesInTuple(
            List<Member[]> tuples)
        {

            int tupleLength = tuples.get(0).length;
            //noinspection unchecked
            Map<Member, Integer>[] counters = new Map[tupleLength];
            for (int i = 0; i < counters.length; i++) {
                counters[i] = new HashMap<Member, Integer>();
            }
            for (Member[] tuple : tuples) {
                for (int i = 0; i < tuple.length; i++) {
                    Member member = tuple[i];
                    Map<Member, Integer> map = counters[i];
                    if (map.containsKey(member)) {
                        Integer count = map.get(member);
                        map.put(member, ++count);
                    } else {
                        map.put(member, 1);
                    }
                }
            }
            return counters;
        }

        private static Set<Member> optimizeMemberSet(
            Set<Member> members,
            SchemaReader reader,
            Cube baseCubeForMeasure)
        {

            boolean didOptimize;
            Set<Member> membersToBeOptimized = new HashSet<Member>();
            Set<Member> optimizedMembers = new HashSet<Member>();
            while (members.size() > 0) {
                Iterator<Member> iterator = members.iterator();
                Member first = iterator.next();
                if(first.isAll()){
                    optimizedMembers.clear();
                    optimizedMembers.add(first);
                    return optimizedMembers;
                }
                membersToBeOptimized.add(first);
                iterator.remove();

                Member firstParentMember = first.getParentMember();
                while (iterator.hasNext()) {
                    Member current =  iterator.next();
                    if(current.isAll()){
                        optimizedMembers.clear();
                        optimizedMembers.add(current);
                        return optimizedMembers;
                    }

                    Member currentParentMember = current.getParentMember();
                    if (firstParentMember == null &&
                        currentParentMember == null ||
                        (firstParentMember!= null &&
                        firstParentMember.equals(currentParentMember))) {
                        membersToBeOptimized.add(current);
                        iterator.remove();
                    }
                }

                int childCountOfParent = -1;
                if (firstParentMember != null) {
                    childCountOfParent = getChildCount(firstParentMember, reader);
                }
                if (childCountOfParent != -1 &&
                    membersToBeOptimized.size() == childCountOfParent &&
                    canOptimize(firstParentMember,baseCubeForMeasure)) {
                    optimizedMembers.add(firstParentMember);
                    didOptimize = true;
                } else {
                    optimizedMembers.addAll(membersToBeOptimized);
                    didOptimize = false;
                }
                membersToBeOptimized.clear();

                if (members.size() == 0 && didOptimize) {
                    Set temp = members;
                    members = optimizedMembers;
                    optimizedMembers = temp;
                }
            }
            return optimizedMembers;
        }

        /**
         * Returns whether tuples are equal. They must have the same length.
         *
         * @param tuple1 First tuple
         * @param tuple2 Second tuple
         * @return whether tuples are equal
         */
        private static boolean isEqual(Member[] tuple1, Member[] tuple2) {
            for (int i = 0; i < tuple1.length; i++) {
                if (!tuple1[i].getUniqueName().
                    equals(tuple2[i].getUniqueName())) {
                    return false;
                }
            }
            return true;
        }

        private static boolean canOptimize(
            Member parentMember,
            Cube baseCube)
        {
            return dimensionJoinsToBaseCube(
                parentMember.getDimension(), baseCube)
                || !parentMember.isAll();
        }

        private static List<Member[]> crossProd(Set<Member>[] sets) {
            List<Member> firstList = new ArrayList<Member>(sets[0]);
            List<Member> secondList = new ArrayList<Member>(sets[1]);
            List<Member[]> tupleList =
                CrossJoinFunDef.crossJoin(firstList, secondList);
            for (int i = 2; i < sets.length; i++) {
                Set<Member> set = sets[i];
                tupleList =
                    CrossJoinFunDef.crossJoin(
                        tupleList, new ArrayList<Member>(set));
            }
            return tupleList;
        }

        private static boolean dimensionJoinsToBaseCube(
            Dimension dimension,
            Cube baseCube)
        {
            HashSet<Dimension> dimensions = new HashSet<Dimension>();
            dimensions.add(dimension);
            return baseCube.nonJoiningDimensions(dimensions).size() == 0;
        }

        private static int getChildCount(
            Member parentMember,
            SchemaReader reader)
        {
            int childrenCountFromCache =
                reader.getChildrenCountFromCache(parentMember);
            if (childrenCountFromCache != -1) {
                return childrenCountFromCache;
            }
            return reader.getMemberChildren(parentMember).size();
        }

    }
}

// End AggregateFunDef.java
