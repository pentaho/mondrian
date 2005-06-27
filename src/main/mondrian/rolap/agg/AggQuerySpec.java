/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 21 March, 2002
*/
package mondrian.rolap.agg;

import mondrian.olap.Util;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.RolapStar;
import mondrian.rolap.sql.SqlQuery;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import org.apache.log4j.Logger;

/**
 * An AggStar's version of the QuerySpec.
 *
 * When/If the AggStar code is merged into RolapStar (or RolapStar is merged)
 * into AggStar, then this, indeed, can implement the QuerySpec interface.
 *
 * @author <a>Richard M. Emberson</a>
 * @version
 */
class AggQuerySpec {
    private final Logger LOGGER = Logger.getLogger(AggQuerySpec.class);
    private final AggStar aggStar;
    private final Segment[] segments;
    /**
     * Whether the query contains any distinct aggregates. If so, a direct hit
     * is required; it cannot be rolled up.
     */
    private final boolean isDistinct;

    AggQuerySpec(final AggStar aggStar,
                 final Segment[] segments,
                 final boolean isDistinct) {
        this.aggStar = aggStar;
        this.segments = segments;
        this.isDistinct = isDistinct;
    }

    protected SqlQuery newSqlQuery() {
        return getStar().getSqlQuery();
    }
    public RolapStar getStar() {
        return aggStar.getStar();
    }
    public int getMeasureCount() {
        return segments.length;
    }
    public AggStar.FactTable.Measure getMeasure(final int i) {
        int bitPos = segments[i].measure.getBitPosition();
        return aggStar.lookupMeasure(bitPos);
    }
    public String getMeasureAlias(final int i) {
        return "m" + Integer.toString(i);
    }
    public int getColumnCount() {
        return segments[0].aggregation.getColumns().length;
    }
    public AggStar.Table.Column getColumn(final int i) {
        RolapStar.Column[] columns = segments[0].aggregation.getColumns();
        int bitPos = columns[i].getBitPosition();
        AggStar.Table.Column column = aggStar.lookupColumn(bitPos);

        // this should never happen
        if (column == null) {
            LOGGER.error("column null for bitPos="+bitPos);
        }
        return column;
    }
    public String getColumnAlias(final int i) {
        return "c" + Integer.toString(i);
    }
    public ColumnConstraint[] getConstraints(final int i) {
        return segments[0].axes[i].getConstraints();
    }
    public String generateSqlQuery() {
        SqlQuery sqlQuery = newSqlQuery();

        if ((! sqlQuery.getDialect().allowsCountDistinct()) && hasDistinct()) {
            distinctGenerateSQL(sqlQuery);
        } else {
            nonDistinctGenerateSQL(sqlQuery);
        }

        return sqlQuery.toString();
    }
    protected boolean hasDistinct() {
        return isDistinct;
    }
    protected void addMeasure(final int i, final SqlQuery sqlQuery) {
        AggStar.FactTable.Measure measure = getMeasure(i);

        measure.getTable().addToFrom(sqlQuery, false, true);

        String expr = measure.getExpression(sqlQuery);
        sqlQuery.addSelect(expr, getMeasureAlias(i));
    }

    protected boolean isAggregate() {
        return true;
    }

    protected void distinctGenerateSQL(final SqlQuery outerSqlQuery) {
        // Generate something like
        //  select d0, d1, count(m0)
        //  from (
        //    select distinct x as d0, y as d1, z as m0
        //    from t) as foo
        //  group by d0, d1

        final SqlQuery innerSqlQuery = newSqlQuery();
        innerSqlQuery.setDistinct(true);

        // add constraining dimensions
        int columnCnt = getColumnCount();
        for (int i = 0; i < columnCnt; i++) {
            AggStar.Table.Column column = getColumn(i);
            AggStar.Table table = column.getTable();
            table.addToFrom(innerSqlQuery, false, true);
            String expr = column.getExpression(innerSqlQuery);
            ColumnConstraint[] constraints = getConstraints(i);
            if (constraints != null) {
                innerSqlQuery.addWhere(RolapStar.Column.createInExpr(expr,
                                                    constraints,
                                                    column.isNumeric()));
            }
            final String alias = "d" + i;
            innerSqlQuery.addSelect(expr, alias);
            outerSqlQuery.addSelect(alias);
            outerSqlQuery.addGroupBy(alias);
        }
        for (int i = 0, count = getMeasureCount(); i < count; i++) {
            AggStar.FactTable.Measure measure = getMeasure(i);

            measure.getTable().addToFrom(innerSqlQuery, false, true);

            String alias = getMeasureAlias(i);
            String expr = measure.getExpression(outerSqlQuery);
            innerSqlQuery.addSelect(expr, alias);

            outerSqlQuery.addSelect(
                measure.getAggregator().getNonDistinctAggregator().getExpression(
                        alias));
        }
        outerSqlQuery.addFrom(innerSqlQuery, "dummyname", true);
    }
    protected void nonDistinctGenerateSQL(final SqlQuery sqlQuery) {
        // add constraining dimensions
        int columnCnt = getColumnCount();
        for (int i = 0; i < columnCnt; i++) {
            AggStar.Table.Column column = getColumn(i);
            AggStar.Table table = column.getTable();
            table.addToFrom(sqlQuery, false, true);

            String expr = column.getExpression(sqlQuery);

            ColumnConstraint[] constraints = getConstraints(i);
            if (constraints != null) {
                sqlQuery.addWhere(RolapStar.Column.createInExpr(expr,
                                               constraints,
                                               column.isNumeric()));
            }

            // some DB2 (AS400) versions throw an error, if a column alias is
            // there and *not* used in a subsequent order by/group by
            if (sqlQuery.getDialect().isAS400()) {
                sqlQuery.addSelect(expr, null);
            } else {
                sqlQuery.addSelect(expr, getColumnAlias(i));
            }

            if (isAggregate()) {
                sqlQuery.addGroupBy(expr);
            }
        }

        // add measures
        for (int i = 0, count = getMeasureCount(); i < count; i++) {
            addMeasure(i, sqlQuery);
        }
    }

}
