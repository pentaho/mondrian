/*
 //This software is subject to the terms of the Common Public License
 //Agreement, available at the following URL:
 //http://www.opensource.org/licenses/cpl.html.
 //Copyright (C) 2004-2005 TONBELLER AG
 //All Rights Reserved.
 //You must accept the terms of that agreement to use this software.
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
import mondrian.rolap.sql.SqlQuery.Dialect;

/**
 * Computes a TopCount in SQL.
 *
 * @author av
 * @since Nov 21, 2005
 * @version $Id$
 */
public class RolapNativeTopCount extends RolapNativeSet {

    boolean ascending;

    public RolapNativeTopCount() {
        super.setEnabled(MondrianProperties.instance().EnableNativeTopCount.get());
    }

    class TopCountConstraint extends SetConstraint {
        String orderByExpr;

        public TopCountConstraint(CrossJoinArg[] args, RolapEvaluator evaluator, String orderByExpr) {
            super(args, evaluator, true);
            this.orderByExpr = orderByExpr;
        }

        /**
         * we alwas need to join the fact table because we want to evalutate
         * the top count expression which involves a fact.
         */
        protected boolean isJoinRequired() {
            return true;
        }

        public void addConstraint(SqlQuery sqlQuery) {
            if (orderByExpr != null) {
                Dialect dialect = sqlQuery.getDialect();
                if (dialect.isMySQL() || dialect.isDB2()) {
                    String alias = sqlQuery.nextColumnAlias();
                    alias = dialect.quoteIdentifier(alias);
                    sqlQuery.addSelect(orderByExpr, alias);
                    sqlQuery.addOrderBy(alias, ascending, true);
                } else {
                    sqlQuery.addOrderBy(orderByExpr, ascending, true);
                }
            }
            super.addConstraint(sqlQuery);
        }

        public Object getCacheKey() {
            List key = new ArrayList();
            key.add(super.getCacheKey());
            key.add(orderByExpr);
            key.add(Boolean.valueOf(ascending));
            return key;
        }
    }

    protected boolean isStrict() {
        return true;
    }

    NativeEvaluator createEvaluator(RolapEvaluator evaluator, FunDef fun, Exp[] args) {
        if (!isEnabled())
            return null;
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
        CrossJoinArg[] cargs = checkCrossJoinArg(args[0]);
        if (cargs == null) {
            return null;
        }
        if (isPreferInterpreter(cargs)) {
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
        Connection con = null;
        try {
            con = ds.getConnection();

            // generate the ORDER BY Clause
            SqlQuery sqlQuery = SqlTupleReader.newQuery(con, "NativeTopCount");
            RolapNativeSql sql = new RolapNativeSql(sqlQuery);
            String orderByExpr = null;
            if (args.length == 3) {
                orderByExpr = sql.generateTopCountOrderBy(args[2]);
                if (orderByExpr == null) {
                    return null;
                }
            }
            LOGGER.info("using native topcount");

            TupleConstraint constraint = new TopCountConstraint(cargs, evaluator, orderByExpr);
            SetEvaluator sev = new SetEvaluator(cargs, schemaReader, constraint);
            sev.setMaxRows(count);
            return sev;
        } catch (SQLException e) {
            throw Util.newInternal(e, "RolapNativeTopCount");
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

// End RolapNativeTopCount.java
