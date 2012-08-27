/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2005-2005 Julian Hyde
// Copyright (C) 2005-2011 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.mdx.MemberExpr;
import mondrian.olap.*;
import mondrian.olap.MondrianDef.RelationOrJoin;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.sql.*;
import mondrian.spi.Dialect;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

/**
 * Computes a TopCount in SQL.
 *
 * @author av
 * @since Nov 21, 2005
  */
public class RolapNativeTopCount extends RolapNativeSet {

    public RolapNativeTopCount() {
        super.setEnabled(
            MondrianProperties.instance().EnableNativeTopCount.get());
    }

    static class TopCountConstraint extends SetConstraint {
        Exp orderByExpr;
        boolean ascending;
        Integer topCount;

        public TopCountConstraint(
            int count,
            CrossJoinArg[] args, RolapEvaluator evaluator,
            Exp orderByExpr, boolean ascending)
        {
            super(args, evaluator, true);
            this.orderByExpr = orderByExpr;
            this.ascending = ascending;
            this.topCount = new Integer(count);
        }

        /**
         * {@inheritDoc}
         *
         * <p>TopCount always needs to join the fact table because we want to
         * evaluate the top count expression which involves a fact.
         */
        protected boolean isJoinRequired() {
            return true;
        }

        public void addConstraint(
            SqlQuery sqlQuery,
            RolapCube baseCube,
            AggStar aggStar)
        {
            if (orderByExpr != null) {
                RolapNativeSql sql =
                    new RolapNativeSql(
                        sqlQuery, aggStar, getEvaluator(), null);
                String orderBySql = sql.generateTopCountOrderBy(orderByExpr);
                Dialect dialect = sqlQuery.getDialect();
                boolean nullable = deduceNullability(orderByExpr);
                if (dialect.requiresOrderByAlias()) {
                    String alias = sqlQuery.nextColumnAlias();
                    alias = dialect.quoteIdentifier(alias);
                    sqlQuery.addSelect(orderBySql, null, alias);
                    sqlQuery.addOrderBy(alias, ascending, true, nullable);
                } else {
                    sqlQuery.addOrderBy(orderBySql, ascending, true, nullable);
                }
            }
            super.addConstraint(sqlQuery, baseCube, aggStar);
        }

        private boolean deduceNullability(Exp expr) {
            if (!(expr instanceof MemberExpr)) {
                return true;
            }
            final MemberExpr memberExpr = (MemberExpr) expr;
            if (!(memberExpr.getMember() instanceof RolapStoredMeasure)) {
                return true;
            }
            final RolapStoredMeasure measure =
                (RolapStoredMeasure) memberExpr.getMember();
            return measure.getAggregator() != RolapAggregator.DistinctCount;
        }

        public Object getCacheKey() {
            List<Object> key = new ArrayList<Object>();
            key.add(super.getCacheKey());
            // Note: need to use string in order for caching to work
            if (orderByExpr != null) {
                key.add(orderByExpr.toString());
            }
            key.add(ascending);
            key.add(topCount);

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
        boolean ascending;

        if (!isEnabled()) {
            return null;
        }
        if (!TopCountConstraint.isValidContext(
                evaluator, restrictMemberTypes()))
        {
            return null;
        }

        // is this "TopCount(<set>, <count>, [<numeric expr>])"
        String funName = fun.getName();
        if ("TopCount".equalsIgnoreCase(funName)) {
            ascending = false;
        } else if ("BottomCount".equalsIgnoreCase(funName)) {
            ascending = true;
        } else {
            return null;
        }
        if (args.length < 2 || args.length > 3) {
            return null;
        }

        // extract the set expression
        List<CrossJoinArg[]> allArgs =
            crossJoinArgFactory().checkCrossJoinArg(evaluator, args[0]);

        // checkCrossJoinArg returns a list of CrossJoinArg arrays.  The first
        // array is the CrossJoin dimensions.  The second array, if any,
        // contains additional constraints on the dimensions. If either the list
        // or the first array is null, then native cross join is not feasible.
        if (allArgs == null || allArgs.isEmpty() || allArgs.get(0) == null) {
            return null;
        }

        CrossJoinArg[] cjArgs = allArgs.get(0);
        if (isPreferInterpreter(cjArgs, false)) {
            return null;
        }

        // extract count
        if (!(args[1] instanceof Literal)) {
            return null;
        }
        int count = ((Literal) args[1]).getIntValue();

        // extract "order by" expression
        SchemaReader schemaReader = evaluator.getSchemaReader();
        DataSource ds = schemaReader.getDataSource();

        // generate the ORDER BY Clause
        // Need to generate top count order by to determine whether
        // or not it can be created. The top count
        // could change to use an aggregate table later in evaulation
        SqlQuery sqlQuery = SqlQuery.newQuery(ds, "NativeTopCount");
        RolapNativeSql sql =
            new RolapNativeSql(
                sqlQuery, null, evaluator, null);
        Exp orderByExpr = null;
        if (args.length == 3) {
            orderByExpr = args[2];
            String orderBySQL = sql.generateTopCountOrderBy(args[2]);
            if (orderBySQL == null) {
                return null;
            }
        }
        LOGGER.debug("using native topcount");
        final int savepoint = evaluator.savepoint();
        try {
            overrideContext(evaluator, cjArgs, sql.getStoredMeasure());

            CrossJoinArg[] predicateArgs = null;
            if (allArgs.size() == 2) {
                predicateArgs = allArgs.get(1);
            }

            CrossJoinArg[] combinedArgs;
            if (predicateArgs != null) {
                // Combined the CJ and the additional predicate args
                // to form the TupleConstraint.
                combinedArgs =
                        Util.appendArrays(cjArgs, predicateArgs);
            } else {
                combinedArgs = cjArgs;
            }
            TupleConstraint constraint =
                new TopCountConstraint(
                    count, combinedArgs, evaluator, orderByExpr, ascending);
            SetEvaluator sev =
                new SetEvaluator(cjArgs, schemaReader, constraint);
            sev.setMaxRows(count);
            return sev;
        } finally {
            evaluator.restore(savepoint);
        }
    }
}

// End RolapNativeTopCount.java
