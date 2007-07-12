/*
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2005 TONBELLER AG
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import mondrian.olap.*;
import mondrian.rolap.sql.SqlQuery;
import mondrian.rolap.sql.TupleConstraint;
import mondrian.rolap.sql.SqlQuery.Dialect;

/**
 * Computes a TopCount in SQL.
 *
 * @author av
 * @since Nov 21, 2005
 * @version $Id$
 */
public class RolapNativeTopCount extends RolapNativeSet {

    public RolapNativeTopCount() {
        super.setEnabled(MondrianProperties.instance().EnableNativeTopCount.get());
    }

    static class TopCountConstraint extends SetConstraint {
        String orderByExpr;
        boolean ascending;

        public TopCountConstraint(
            CrossJoinArg[] args, RolapEvaluator evaluator, 
            String orderByExpr, boolean ascending) {
            super(args, evaluator, true);
            this.orderByExpr = orderByExpr;
            this.ascending = ascending;
        }

        /**
         * we alwas need to join the fact table because we want to evalutate
         * the top count expression which involves a fact.
         */
        protected boolean isJoinRequired() {
            return true;
        }
        
        public void addConstraint(
            SqlQuery sqlQuery,
            Map<RolapLevel, RolapStar.Column> levelToColumnMap,
            Map<String, RolapStar.Table> relationNamesToStarTableMap) {
            if (orderByExpr != null) {
                Dialect dialect = sqlQuery.getDialect();
                if (dialect.requiresOrderByAlias()) {
                    String alias = sqlQuery.nextColumnAlias();
                    alias = dialect.quoteIdentifier(alias);
                    sqlQuery.addSelect(orderByExpr, alias);
                    sqlQuery.addOrderBy(alias, ascending, true, false);
                } else {
                    sqlQuery.addOrderBy(orderByExpr, ascending, true, false);
                }
            }
            super.addConstraint(sqlQuery, levelToColumnMap,
                relationNamesToStarTableMap);
        }

        public Object getCacheKey() {
            List<Object> key = new ArrayList<Object>();
            key.add(super.getCacheKey());
            key.add(orderByExpr);
            key.add(ascending);
            return key;
        }
    }

    protected boolean restrictMemberTypes() {
        return true;
    }

    NativeEvaluator createEvaluator(RolapEvaluator evaluator, FunDef fun, Exp[] args) {
        boolean ascending;
        
        if (!isEnabled())
            return null;
        if (!TopCountConstraint.isValidContext(evaluator)) {
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
        CrossJoinArg[] cargs = checkCrossJoinArg(evaluator, args[0]);
        if (cargs == null) {
            return null;
        }
        if (isPreferInterpreter(cargs, false)) {
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
        SqlQuery sqlQuery = SqlQuery.newQuery(ds, "NativeTopCount");
        RolapNativeSql sql = new RolapNativeSql(sqlQuery);
        String orderByExpr = null;
        if (args.length == 3) {
            orderByExpr = sql.generateTopCountOrderBy(args[2]);
            if (orderByExpr == null) {
                return null;
            }
        }
        LOGGER.debug("using native topcount");
        evaluator = overrideContext(evaluator, cargs, sql.getStoredMeasure());

        TupleConstraint constraint = 
            new TopCountConstraint(cargs, evaluator, orderByExpr, ascending);
        SetEvaluator sev = new SetEvaluator(cargs, schemaReader, constraint);
        sev.setMaxRows(count);
        return sev;
    }
}

// End RolapNativeTopCount.java
