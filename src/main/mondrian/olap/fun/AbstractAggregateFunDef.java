/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.DelegatingTupleList;
import mondrian.mdx.UnresolvedFunCall;
import mondrian.olap.*;
import mondrian.resource.MondrianResource;
import mondrian.rolap.RolapCube;
import mondrian.rolap.RolapMember;

import java.util.*;

/**
 * Abstract base class for all aggregate functions (<code>Aggregate</code>,
 * <code>Sum</code>, <code>Avg</code>, et cetera).
 *
 * @author jhyde
 * @since 2005/8/14
 */
public class AbstractAggregateFunDef extends FunDefBase {
    public AbstractAggregateFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    protected Exp validateArg(
        Validator validator, Exp[] args, int i, int category)
    {
        // If expression cache is enabled, wrap first expression (the set)
        // in a function which will use the expression cache.
        if (i == 0) {
            if (MondrianProperties.instance().EnableExpCache.get()) {
                Exp arg = args[0];
                if (FunUtil.worthCaching(arg)) {
                    final Exp cacheCall =
                        new UnresolvedFunCall(
                            CacheFunDef.NAME,
                            Syntax.Function,
                            new Exp[] {arg});
                    return validator.validate(cacheCall, false);
                }
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
     * @param evaluator current evaluation context
     * @return list of evaluated members or tuples
     */
    protected static TupleList evaluateCurrentList(
        ListCalc listCalc,
        Evaluator evaluator)
    {
        final int savepoint = evaluator.savepoint();
        TupleList tuples;
        try {
            evaluator.setNonEmpty(false);
            tuples = listCalc.evaluateList(evaluator);
        } finally {
            evaluator.restore(savepoint);
        }
        int currLen = tuples.size();
        TupleList dims;
        try {
            dims = processUnrelatedDimensions(tuples, evaluator);
        } finally {
            evaluator.restore(savepoint);
        }
        crossProd(evaluator, currLen);
        return dims;
    }

    protected TupleIterable evaluateCurrentIterable(
        IterCalc iterCalc,
        Evaluator evaluator)
    {
        final int savepoint = evaluator.savepoint();
        int currLen = 0;
        TupleIterable iterable;
        try {
            evaluator.setNonEmpty(false);
            iterable = iterCalc.evaluateIterable(evaluator);
        } finally {
            evaluator.restore(savepoint);
        }
        crossProd(evaluator, currLen);
        return iterable;
    }

    private static void crossProd(Evaluator evaluator, int currLen) {
        long iterationLimit =
            MondrianProperties.instance().IterationLimit.get();
        final int productLen = currLen * evaluator.getIterationLength();
        if (iterationLimit > 0) {
            if (productLen > iterationLimit) {
                throw MondrianResource.instance()
                    .IterationLimitExceeded.ex(iterationLimit);
            }
        }
        evaluator.setIterationLength(currLen);
    }

    /**
     * Pushes unrelated dimensions to the top level member from the given list
     * of tuples if the ignoreUnrelatedDimensions property is set on the base
     * cube usage in the virtual cube.
     *
     * <p>If IgnoreMeasureForNonJoiningDimension is set to true and
     * ignoreUnrelatedDimensions on CubeUsage is set to false then if a non
     * joining dimension exists in the aggregation list then return an empty
     * list else return the original list.
     *
     * @param tuplesForAggregation is a list of members or tuples used in
     * computing the aggregate
     * @param evaluator Evaluator
     * @return list of members or tuples
     */
    private static TupleList processUnrelatedDimensions(
        TupleList tuplesForAggregation,
        Evaluator evaluator)
    {
        if (tuplesForAggregation.size() == 0) {
            return tuplesForAggregation;
        }

        RolapMember measure = (RolapMember) evaluator.getMembers()[0];

        if (measure.isCalculated()) {
            return tuplesForAggregation;
        }

        RolapCube virtualCube = (RolapCube) evaluator.getCube();
        RolapCube baseCube = (RolapCube) evaluator.getMeasureCube();
        if (virtualCube.isVirtual() && baseCube != null) {
            if (virtualCube.shouldIgnoreUnrelatedDimensions(baseCube.getName()))
            {
                return ignoreUnrelatedDimensions(
                    tuplesForAggregation, baseCube);
            } else if (MondrianProperties.instance()
                .IgnoreMeasureForNonJoiningDimension.get())
            {
                return ignoreMeasureForNonJoiningDimension(
                    tuplesForAggregation, baseCube);
            }
        }
        return tuplesForAggregation;
    }

    /**
     * If a non joining dimension exists in the aggregation list then return
     * an empty list else return the original list.
     *
     * @param tuplesForAggregation is a list of members or tuples used in
     * computing the aggregate
     * @param baseCube
     * @return list of members or tuples
     */
    private static TupleList ignoreMeasureForNonJoiningDimension(
        TupleList tuplesForAggregation,
        RolapCube baseCube)
    {
        Set<Dimension> nonJoiningDimensions =
            nonJoiningDimensions(baseCube, tuplesForAggregation);
        if (nonJoiningDimensions.size() > 0) {
            return TupleCollections.emptyList(tuplesForAggregation.getArity());
        }
        return tuplesForAggregation;
    }

    /**
     * Pushes unrelated dimensions to the top level member from the given list
     * of tuples if the ignoreUnrelatedDimensions property is set on the base
     * cube usage in the virtual cube.
     *
     * @param tuplesForAggregation is a list of members or tuples used in
     * computing the aggregate
     * @return list of members or tuples
     */
    private static TupleList ignoreUnrelatedDimensions(
        TupleList tuplesForAggregation,
        RolapCube baseCube)
    {
        Set<Dimension> nonJoiningDimensions =
            nonJoiningDimensions(baseCube, tuplesForAggregation);
        final Set<List<Member>> processedTuples =
            new LinkedHashSet<List<Member>>(tuplesForAggregation.size());
        for (List<Member> tuple : tuplesForAggregation) {
            List<Member> tupleCopy = tuple;
            for (int j = 0; j < tuple.size(); j++) {
                final Member member = tuple.get(j);
                if (nonJoiningDimensions.contains(member.getDimension())) {
                    if (tupleCopy == tuple) {
                        // Avoid making a copy until we have to change a tuple.
                        tupleCopy = new ArrayList<Member>(tuple);
                    }
                    final Hierarchy hierarchy =
                        member.getDimension().getHierarchy();
                    if (hierarchy.hasAll()) {
                        tupleCopy.set(j, hierarchy.getAllMember());
                    } else {
                        tupleCopy.set(j, hierarchy.getDefaultMember());
                    }
                }
            }
            processedTuples.add(tupleCopy);
        }
        return new DelegatingTupleList(
            tuplesForAggregation.getArity(),
            new ArrayList<List<Member>>(
                processedTuples));
    }

    private static Set<Dimension> nonJoiningDimensions(
        RolapCube baseCube,
        TupleList tuplesForAggregation)
    {
        List<Member> tuple = tuplesForAggregation.get(0);
        return baseCube.nonJoiningDimensions(
            tuple.toArray(new Member[tuple.size()]));
    }
}

// End AbstractAggregateFunDef.java
