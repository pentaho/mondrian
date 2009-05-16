/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2005-2009 Julian Hyde
// Copyright (C) 2004-2005 TONBELLER AG
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import mondrian.olap.*;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.sql.SqlQuery;
import mondrian.rolap.sql.TupleConstraint;

/**
 * Computes a Filter(set, condition) in SQL.
 *
 * @author av
 * @since Nov 21, 2005
 * @version $Id$
 */
public class RolapNativeFilter extends RolapNativeSet {

    public RolapNativeFilter() {
        super.setEnabled(MondrianProperties.instance().EnableNativeFilter.get());
    }

    static class FilterConstraint extends SetConstraint {
        Exp filterExpr;

        public FilterConstraint(CrossJoinArg[] args, RolapEvaluator evaluator, Exp filterExpr) {
            super(args, evaluator, true);
            this.filterExpr = filterExpr;
        }

        /**
         * we alwas need to join the fact table because we want to evalutate
         * the filter expression against a fact.
         */
        protected boolean isJoinRequired() {
            return true;
        }

        public void addConstraint(
            SqlQuery sqlQuery,
            RolapCube baseCube,
            AggStar aggStar) {
            // Use aggregate table to generate filter condition
            RolapNativeSql sql = new RolapNativeSql(sqlQuery, aggStar);
            String filterSql =  sql.generateFilterCondition(filterExpr);
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
            return key;
        }
    }

    protected boolean restrictMemberTypes() {
        return true;
    }

    NativeEvaluator createEvaluator(RolapEvaluator evaluator, FunDef fun, Exp[] args) {
        if (!isEnabled()) {
            return null;
        }
        if (!FilterConstraint.isValidContext(evaluator, restrictMemberTypes())) {
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
        CrossJoinArg[] cargs = checkCrossJoinArg(evaluator, args[0]);
        if (cargs == null) {
            return null;
        }
        if (isPreferInterpreter(cargs, false)) {
            return null;
        }

        // extract "order by" expression
        SchemaReader schemaReader = evaluator.getSchemaReader();
        DataSource ds = schemaReader.getDataSource();

        // generate the WHERE condition
        // Need to generate where condition here to determine whether
        // or not the filter condition can be created. The filter
        // condition could change to use an aggregate table later in evaulation
        SqlQuery sqlQuery = SqlQuery.newQuery(ds, "NativeFilter");
        RolapNativeSql sql = new RolapNativeSql(sqlQuery, null);
        String filterExpr = sql.generateFilterCondition(args[1]);
        if (filterExpr == null) {
            return null;
        }

        // check to see if evaluator contains a calculated member.
        // this is necessary due to the SqlConstraintsUtils.addContextConstraint()
        // method which gets called when generating the native SQL
        if (SqlConstraintUtils.containsCalculatedMember(evaluator.getMembers())) {
            return null;
        }

        LOGGER.debug("using native filter");

        evaluator = overrideContext(evaluator, cargs, sql.getStoredMeasure());

        TupleConstraint constraint = new FilterConstraint(cargs, evaluator, args[1]);
        return new SetEvaluator(cargs, schemaReader, constraint);
    }

}

// End RolapNativeFilter.java
