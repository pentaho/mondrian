/*
//This software is subject to the terms of the Common Public License
//Agreement, available at the following URL:
//http://www.opensource.org/licenses/cpl.html.
//Copyright (C) 2004-2005 TONBELLER AG
//All Rights Reserved.
//You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.olap.Exp;
import mondrian.olap.fun.FunTableImpl.MemberScalarExp;
import mondrian.rolap.sql.SqlQuery;

/**
 * creates SQL from parse tree nodes. For example, it creates the SQL that accesses
 * a measure for the ORDER BY clause that is generated for a TopCount.
 * <p>
 * TODO: this could be very much improved, e.g. topcount(set, (a + b) / c) could 
 * be transformed to SQL.
 *  
 * @author av
 * @since Nov 17, 2005
 */
public class RolapNativeSql {

    private SqlQuery sqlQuery;

    /**
     * @param the query - its not modified
     */
    RolapNativeSql(SqlQuery sqlQuery) {
        this.sqlQuery = sqlQuery;
    }

    /**
     * generates an aggregate of a measure, e.g. "sum(Store_Sales)" for TopCount. The
     * returned expr will be added to the select list and to the order by clause.
     */
    public String generateTopCountOrderBy(Exp exp) {
        RolapStoredMeasure measure = checkMeasure(exp);
        if (measure == null)
            return null;
        String exprInner = measure.getMondrianDefExpression().getExpression(sqlQuery);
        return measure.getAggregator().getExpression(exprInner);
    }

    /**
     * extracts the RolapStoredMeasure from MemberScalarExp
     */
    public RolapStoredMeasure checkMeasure(Exp exp) {
        if (exp instanceof RolapStoredMeasure)
            return (RolapStoredMeasure) exp;
        if (exp instanceof MemberScalarExp) {
            Object[] children = ((MemberScalarExp) exp).getChildren();
            if (children.length != 1)
                return null;
            Exp child = (Exp) children[0];
            return checkMeasure(child);
        }
        return null;
    }
}
