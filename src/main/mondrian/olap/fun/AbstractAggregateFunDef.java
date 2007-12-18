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

import mondrian.calc.*;
import mondrian.olap.*;
import mondrian.resource.MondrianResource;
import mondrian.mdx.UnresolvedFunCall;
import mondrian.rolap.RolapMember;
import mondrian.rolap.RolapCube;
import mondrian.rolap.RolapStoredMeasure;

import java.util.*;

/**
 * Abstract base class for all aggregate functions (<code>Aggregate</code>,
 * <code>Sum</code>, <code>Avg</code>, et cetera).
 *
 * @author jhyde
 * @since 2005/8/14
 * @version $Id$
 */
public class AbstractAggregateFunDef extends FunDefBase {
    public AbstractAggregateFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    protected Exp validateArg(
            Validator validator, Exp[] args, int i, int category) {
        // If expression cache is enabled, wrap first expression (the set)
        // in a function which will use the expression cache.
        if (i == 0) {
            if (MondrianProperties.instance().EnableExpCache.get()) {
                Exp arg = args[0];
                final Exp cacheCall = new UnresolvedFunCall(
                        CacheFunDef.NAME,
                        Syntax.Function,
                        new Exp[] {arg});
                return validator.validate(cacheCall, false);
            }
        }
        return super.validateArg(validator, args, i, category);
    }

    /**
     * Evaluates the list of members or tuples used in computing the aggregate.
     * If the measure for aggregation has to ignore unrelated dimensions
     * this method will push unrelated dimension members to top level member.
     * This behaviour is driven by the ignoreUnrelatedDimensions property
     * on a base cube usage specified in the virtual cube.Keeps track of the
     * number of iterations that will be required to iterate over the members
     * or tuples needed to compute the aggregate within the current context.
     * In doing so, also determines if the cross product of all iterations
     * across all parent evaluation contexts will exceed the limit set in the
     * properties file.
     *
     * @param listCalc  calculator used to evaluate the member list
     * @param evaluator current evalutor
     * @return list of evaluated members or tuples
     */
    protected static List evaluateCurrentList(
            ListCalc listCalc,
            Evaluator evaluator)
    {
        List tuples = listCalc.evaluateList(evaluator);

        int currLen = tuples.size();
        crossProd(evaluator, currLen);

        return processUnrelatedDimensions(tuples, evaluator);
    }

    protected Iterable evaluateCurrentIterable(
            IterCalc iterCalc,
            Evaluator evaluator)
    {
        Iterable iter = iterCalc.evaluateIterable(evaluator);

        int currLen = 0;
        crossProd(evaluator, currLen);

        return iter;
    }

    private static void crossProd(Evaluator evaluator, int currLen) {
        long iterationLimit =
                MondrianProperties.instance().IterationLimit.get();
        if (iterationLimit > 0) {
            int productLen = currLen;
            Evaluator parent = evaluator.getParent();
            while (parent != null) {
                productLen *= parent.getIterationLength();
                parent = parent.getParent();
            }
            if (productLen > iterationLimit) {
                throw MondrianResource.instance().
                        IterationLimitExceeded.ex(iterationLimit);
            }
        }
        evaluator.setIterationLength(currLen);
    }

    /**
     * Pushes unrelated dimensions to the top level member from the given list
     * of tuples if the ignoreUnrelatedDimensions property is set on the base
     * cube usage in the virtual cube
     *
     * If IgnoreMeasureForNonJoiningDimension is set to true and
     * ignoreUnrelatedDimensions on CubeUsage is set to false then if a non
     * joining dimension exists in the aggregation list then return an empty
     * list else return the original list
     *
     * @param tuplesForAggregation is a list of members or tuples used in
     * computing the aggregate
     * @param evaluator
     * @return list of members or tuples
     */
    private static List processUnrelatedDimensions(
            List tuplesForAggregation,
            Evaluator evaluator) {

        RolapMember measure = (RolapMember) evaluator.getMembers()[0];

        if (measure.isCalculated()) {
            return tuplesForAggregation;
        }

        RolapCube virtualCube = (RolapCube) evaluator.getCube();
        RolapCube baseCube = ((RolapStoredMeasure) measure).getCube();

        if (virtualCube.shouldIgnoreUnrelatedDimensions(baseCube.getName())) {
            return ignoreUnrelatedDimensions(tuplesForAggregation, baseCube);
        } else if (shouldIgnoreMeasureForNonJoiningDimension()) {
            return ignoreMeasureForNonJoiningDimension(
                    tuplesForAggregation, baseCube);
        }
        return tuplesForAggregation;
    }

    /**
     * If a non joining dimension exists in the aggregation list then return
     * an empty list else return the original list

     * @param tuplesForAggregation is a list of members or tuples used in
     * computing the aggregate
     * @param baseCube
     * @return list of members or tuples
     */
    private static List ignoreMeasureForNonJoiningDimension(
            List tuplesForAggregation,
            RolapCube baseCube)
    {
        Set nonJoiningDimensions =
                nonJoiningDimensions(baseCube, tuplesForAggregation);
        if (nonJoiningDimensions.size() > 0) {
            return new ArrayList();
        }
        return tuplesForAggregation;
    }

    /**
     * Pushes unrelated dimensions to the top level member from the given list
     * of tuples if the ignoreUnrelatedDimensions property is set on the base
     * cube usage in the virtual cube
     *
     * @param tuplesForAggregation is a list of members or tuples used in
     * computing the aggregate
     * @return list of members or tuples
     */
    private static List ignoreUnrelatedDimensions(
            List tuplesForAggregation,
            RolapCube baseCube)
    {
        Set nonJoiningDimensions =
                nonJoiningDimensions(baseCube, tuplesForAggregation);
        Set processedTuples = new LinkedHashSet(tuplesForAggregation.size());
        for (int i = 0; i < tuplesForAggregation.size(); i++) {
            Member[] tuples = copy(tupleAsArray(tuplesForAggregation.get(i)));
            for (int j = 0; j < tuples.length; j++) {
                if (nonJoiningDimensions.contains(
                        tuples[j].getDimension().getUniqueName())) {
                    final Hierarchy hierarchy =
                            tuples[j].getDimension().getHierarchy();
                    if(hierarchy.hasAll()){
                        tuples[j] = hierarchy.getAllMember();
                    } else {
                        tuples[j] = hierarchy.getDefaultMember();
                    }
                }
            }
            if (tuplesForAggregation.get(i) instanceof Member[]) {
                processedTuples.add(new MemberArray(tuples));
            } else {
                processedTuples.add(tuples[0]);
            }

        }
        return tuplesAsList(processedTuples);
    }

    private static Set nonJoiningDimensions(
            RolapCube baseCube,
            List tuplesForAggregation)
    {
        Set nonJoiningDimensions = new HashSet();
        Dimension[] baseCubeDimensions = baseCube.getDimensions();
        Member[] tuple = tupleAsArray(tuplesForAggregation.get(0));
        for (Member member : tuple) {
            nonJoiningDimensions.add(member.getHierarchy().getDimension()
                    .getUniqueName());
        }
        for (Dimension dimension : baseCubeDimensions) {
            nonJoiningDimensions.remove(dimension.getUniqueName());
            if (nonJoiningDimensions.size() == 0) {
                break;
            }
        }
        return nonJoiningDimensions;
    }

    private static boolean shouldIgnoreMeasureForNonJoiningDimension() {
        return MondrianProperties.instance()
                .IgnoreMeasureForNonJoiningDimension.get();
    }

    private static List tuplesAsList(Set tuples) {
        List results = new ArrayList(tuples.size());
        for (Object tuple : tuples) {
            if (tuple instanceof MemberArray){
                results.add(((MemberArray) tuple).memberArray);
            } else {
                results.add(tuple);
            }
        }
        return results;
    }

    private static Member[] copy(Member[] members) {
        Member[] result = new Member[members.length];
        System.arraycopy(members, 0, result, 0, members.length);
        return result;
    }

    private static Member[] tupleAsArray(Object tuple) {
        Member[] result;
        if (tuple instanceof Member[]) {
            result = ((Member[]) tuple);
        } else {
            result = new Member[]{((Member) tuple)};
        }
        return result;
    }

    private static class MemberArray {
        private Object[] memberArray;

        public MemberArray(Object[] memberArray) {
            this.memberArray = memberArray;
        }

        public int hashCode() {
            return Arrays.hashCode(memberArray);
        }

        public boolean equals(Object obj) {
            return Arrays.deepEquals(
                    memberArray,
                    ((MemberArray) obj).memberArray);
        }
    }

}

// End AbstractAggregateFunDef.java
