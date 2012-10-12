/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2005-2005 Julian Hyde
// Copyright (C) 2005-2012 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.mdx.MdxVisitorImpl;
import mondrian.mdx.MemberExpr;
import mondrian.olap.*;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.sql.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.sql.DataSource;

/**
 * Computes a Filter(set, condition) in SQL.
 *
 * @author av
 * @since Nov 21, 2005
 */
public class RolapNativeFilter extends RolapNativeSet {

    public RolapNativeFilter() {
        super.setEnabled(
            MondrianProperties.instance().EnableNativeFilter.get());
    }

    static class FilterConstraint extends SetConstraint {
        Exp filterExpr;

        public FilterConstraint(
            CrossJoinArg[] args,
            RolapEvaluator evaluator,
            Exp filterExpr)
        {
            super(args, evaluator, true);
            this.filterExpr = filterExpr;
        }

        /**
         * {@inheritDoc}
         *
         * <p>Overriding isJoinRequired() for native filters because
         * we have to force a join to the fact table if the filter
         * expression references a measure.
         */
        protected boolean isJoinRequired() {
            // Use a visitor and check all member expressions.
            // If any of them is a measure, we will have to
            // force the join to the fact table. If it is something
            // else then we don't really care. It will show up in
            // the evaluator as a non-all member and trigger the
            // join when we call RolapNativeSet.isJoinRequired().
            final AtomicBoolean mustJoin = new AtomicBoolean(false);
            filterExpr.accept(
                new MdxVisitorImpl() {
                    public Object visit(MemberExpr memberExpr) {
                        if (memberExpr.getMember().isMeasure()) {
                            mustJoin.set(true);
                            return null;
                        }
                        return super.visit(memberExpr);
                    }
                });
            if (mustJoin.get() || super.isJoinRequired()) {
                return true;
            }
            return false;
        }

        public void addConstraint(
            SqlQuery sqlQuery,
            RolapCube baseCube,
            AggStar aggStar)
        {
            // Use aggregate table to generate filter condition
            RolapNativeSql sql =
                new RolapNativeSql(
                    sqlQuery, aggStar, getEvaluator(), args[0].getLevel());
            String filterSql = sql.generateFilterCondition(filterExpr);
            sqlQuery.addHaving(filterSql);
            super.addConstraint(sqlQuery, baseCube, aggStar);
        }

        public Object getCacheKey() {
            List<Object> key = new ArrayList<Object>();
            key.add(super.getCacheKey());
            // Note required to use string in order for caching to work
            if (filterExpr != null) {
                key.add(filterExpr.toString());
            }

            if (this.getEvaluator() instanceof RolapEvaluator) {
                key.add(
                    ((RolapEvaluator)this.getEvaluator())
                    .getSlicerMembers());
            }

         // Add restrictions imposed by Role based access filtering
            SchemaReader schemaReader = this.getEvaluator().getSchemaReader();
            Member[] mm = this.getEvaluator().getMembers();
            for (int mIndex = 0; mIndex < mm.length; mIndex++) {
                if (mm[mIndex] instanceof RolapHierarchy.LimitedRollupMember
                    || mm[mIndex] instanceof
                       RestrictedMemberReader.MultiCardinalityDefaultMember)
                {
                    List<Level> hierarchyLevels = schemaReader
                            .getHierarchyLevels(mm[mIndex].getHierarchy());
                    for (Level affectedLevel : hierarchyLevels) {
                        List<Member> availableMembers = schemaReader
                                .getLevelMembers(affectedLevel, false);
                        for (Member member : availableMembers) {
                            if (!member.isAll()) {
                                key.add(member);
                            }
                        }
                    }
                }
            }

            return key;
        }
    }

    protected boolean restrictMemberTypes() {
        return true;
    }

    NativeEvaluator createEvaluator(
        RolapEvaluator evaluator,
        FunDef fun,
        Exp[] args)
    {
        if (!isEnabled()) {
            return null;
        }
        if (!FilterConstraint.isValidContext(
                evaluator, restrictMemberTypes()))
        {
            return null;
        }
        // is this "Filter(<set>, <numeric expr>)"
        String funName = fun.getName();
        if (!"Filter".equalsIgnoreCase(funName)) {
            return null;
        }

        if (args.length != 2) {
            return null;
        }

        // extract the set expression
        List<CrossJoinArg[]> allArgs =
            crossJoinArgFactory().checkCrossJoinArg(evaluator, args[0]);

        // checkCrossJoinArg returns a list of CrossJoinArg arrays.  The first
        // array is the CrossJoin dimensions.  The second array, if any,
        // contains additional constraints on the dimensions. If either the
        // list or the first array is null, then native cross join is not
        // feasible.
        if (allArgs == null || allArgs.isEmpty() || allArgs.get(0) == null) {
            return null;
        }

        CrossJoinArg[] cjArgs = allArgs.get(0);
        if (isPreferInterpreter(cjArgs, false)) {
            return null;
        }

        // extract "order by" expression
        SchemaReader schemaReader = evaluator.getSchemaReader();
        DataSource ds = schemaReader.getDataSource();

        // generate the WHERE condition
        // Need to generate where condition here to determine whether
        // or not the filter condition can be created. The filter
        // condition could change to use an aggregate table later in evaluation
        SqlQuery sqlQuery = SqlQuery.newQuery(ds, "NativeFilter");
        RolapNativeSql sql =
            new RolapNativeSql(
                sqlQuery, null, evaluator, cjArgs[0].getLevel());
        final Exp filterExpr = args[1];
        String filterExprStr = sql.generateFilterCondition(filterExpr);
        if (filterExprStr == null) {
            return null;
        }

        // Check to see if evaluator contains a calculated member that can't be
        // expanded.  This is necessary due to the SqlConstraintsUtils.
        // addContextConstraint()
        // method which gets called when generating the native SQL.
        if (SqlConstraintUtils.containsCalculatedMember(
                evaluator.getNonAllMembers(), true))
        {
            return null;
        }

        LOGGER.debug("using native filter");

        final int savepoint = evaluator.savepoint();
        try {
            overrideContext(evaluator, cjArgs, sql.getStoredMeasure());
            // Now construct the TupleConstraint that contains both the CJ
            // dimensions and the additional filter on them.
            CrossJoinArg[] combinedArgs = cjArgs;
            if (allArgs.size() == 2) {
                CrossJoinArg[] predicateArgs = allArgs.get(1);
                if (predicateArgs != null) {
                    // Combined the CJ and the additional predicate args.
                    combinedArgs =
                        Util.appendArrays(cjArgs, predicateArgs);
                }
            }

            TupleConstraint constraint =
                new FilterConstraint(combinedArgs, evaluator, filterExpr);
            return new SetEvaluator(cjArgs, schemaReader, constraint);
        } finally {
            evaluator.restore(savepoint);
        }
    }
}

// End RolapNativeFilter.java
