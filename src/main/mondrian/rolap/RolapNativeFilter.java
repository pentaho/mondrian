/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2005 Julian Hyde
// Copyright (C) 2004-2005 TONBELLER AG
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import mondrian.olap.*;
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

    class FilterConstraint extends SetConstraint {
        String filterExpr;

        public FilterConstraint(CrossJoinArg[] args, RolapEvaluator evaluator, String filterByExpr) {
            super(args, evaluator, true);
            this.filterExpr = filterByExpr;
        }

        /**
         * we alwas need to join the fact table because we want to evalutate
         * the filter expression against a fact.
         */
        protected boolean isJoinRequired() {
            return true;
        }

        public void addConstraint(SqlQuery sqlQuery) {
            sqlQuery.addHaving(filterExpr);
            super.addConstraint(sqlQuery);
        }

        public Object getCacheKey() {
            List key = new ArrayList();
            key.add(super.getCacheKey());
            key.add(filterExpr);
            return key;
        }
    }

    protected boolean isStrict() {
        return true;
    }

    NativeEvaluator createEvaluator(RolapEvaluator evaluator, FunDef fun, Exp[] args) {
        if (!isEnabled()) {
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
        CrossJoinArg[] cargs = checkCrossJoinArg(args[0]);
        if (cargs == null) {
            return null;
        }
        if (isPreferInterpreter(cargs)) {
            return null;
        }

        // extract "order by" expression
        SchemaReader schemaReader = evaluator.getSchemaReader();
        DataSource ds = schemaReader.getDataSource();
        Connection con = null;
        try {
            con = ds.getConnection();

            // generate the WHERE condition
            SqlQuery sqlQuery = SqlTupleReader.newQuery(con, "NativeFilter");
            RolapNativeSql sql = new RolapNativeSql(sqlQuery);
            String filterExpr = sql.generateFilterCondition(args[1]);
            if (filterExpr == null) {
                return null;
            }
            LOGGER.info("using native filter");

            TupleConstraint constraint = new FilterConstraint(cargs, evaluator, filterExpr);
            return new SetEvaluator(cargs, schemaReader, constraint);
        } catch (SQLException e) {
            throw Util.newInternal(e, "RolapNativeFilter");
        } finally {
            if (con != null) {
                try {
                    con.close();
                } catch (SQLException e) {
                    LOGGER.error(null, e);
                }
            }
        }
    }

}

// End RolapNativeFilter.java
