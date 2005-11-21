/*
 //This software is subject to the terms of the Common Public License
 //Agreement, available at the following URL:
 //http://www.opensource.org/licenses/cpl.html.
 //Copyright (C) 2004-2005 TONBELLER AG
 //All Rights Reserved.
 //You must accept the terms of that agreement to use this software.
 */
package mondrian.rolap;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import mondrian.olap.Exp;
import mondrian.olap.FunDef;
import mondrian.olap.Literal;
import mondrian.olap.MondrianProperties;
import mondrian.olap.NativeEvaluator;
import mondrian.olap.Util;
import mondrian.rolap.sql.SqlQuery;
import mondrian.rolap.sql.TupleConstraint;

/**
 * computes a TopCount in SQL
 * 
 * @author av
 * @since Nov 21, 2005
 */
public class RolapNativeTopCount extends RolapNativeSet {

    boolean ascending;

    public RolapNativeTopCount() {
        super.setEnabled(MondrianProperties.instance().EnableNativeTopCount.get());
    }

    class TopCountConstraint extends SetConstraint {
        String selectExpr;

        public TopCountConstraint(CrossJoinArg[] args, RolapEvaluator evaluator, String selectExpr) {
            super(args, evaluator, true);
            this.selectExpr = selectExpr;
        }

        /**
         * we alwas need to join the fact table because we want to evalutate
         * the top count expression which involves a fact.
         */
        protected boolean isJoinRequired() {
            return true;
        }

        public void addConstraint(SqlQuery sqlQuery) {
            super.addConstraint(sqlQuery);
            String alias = sqlQuery.nextColumnAlias();
            sqlQuery.addSelect(selectExpr, alias);
            sqlQuery.addOrderBy(alias, ascending);
        }

        public Object getCacheKey() {
            List key = new ArrayList();
            key.add(super.getCacheKey());
            key.add(selectExpr);
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
        if ("TopCount".equalsIgnoreCase(funName))
            ascending = false;
        else if ("BottomCount".equalsIgnoreCase(funName))
            ascending = true;
        else
            return null;

        if (args.length < 2 || args.length > 3)
            return null;

        // extract the set expression
        CrossJoinArg[] cargs = checkCrossJoinArg(args[0]);
        if (cargs == null)
            return null;
        if (isPreferInterpreter(cargs))
            return null;

        // extract count
        if (!(args[1] instanceof Literal))
            return null;
        int count = ((Literal) args[1]).getIntValue();

        // extract "order by" expression
        RolapSchemaReader schemaReader = (RolapSchemaReader) evaluator.getSchemaReader();
        DataSource ds = schemaReader.getDataSource();
        try {
            RolapNativeSql sql = new RolapNativeSql(SqlTupleReader.newQuery(ds.getConnection(),
                    "NativeTopCount"));
            Exp exp;
            if (args.length == 3)
                exp = args[2];
            else
                exp = evaluator.getMembers()[0];
            String orderByExpr = sql.generateTopCountOrderBy(exp);

            LOGGER.info("using native topcount");

            TupleConstraint constraint = new TopCountConstraint(cargs, evaluator, orderByExpr);
            SetEvaluator sev = new SetEvaluator(cargs, schemaReader, constraint);
            sev.setMaxRows(count);
            return sev;
        } catch (SQLException e) {
            throw Util.newInternal(e, "RolapNativeTopCount");
        }
    }

}
