/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 21 March, 2002
*/
package mondrian.rolap.agg;

import mondrian.rolap.RolapStar;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.sql.SqlQuery;

import org.apache.log4j.Logger;

/**
 * An AggStar's version of the {@link QuerySpec}. <p/>
 *
 * When/if the {@link AggStar} code is merged into {@link RolapStar}
 * (or RolapStar is merged into AggStar}, then this, indeed, can implement the
 * {@link QuerySpec} interface.
 *
 * @author Richard M. Emberson
 * @version $Id$
 */
class AggQuerySpec {
    private static final Logger LOGGER = Logger.getLogger(AggQuerySpec.class);
    
    private final AggStar aggStar;
    private final Segment[] segments;
    /**
     * Whether the query contains any distinct aggregates. If so, a direct hit
     * is required; it cannot be rolled up.
     */
    private final boolean isDistinct;

    AggQuerySpec(
            final AggStar aggStar,
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

    public AggStar.FactTable.Column getMeasureAsColumn(final int i) {
        int bitPos = segments[i].measure.getBitPosition();
        return aggStar.lookupColumn(bitPos);
    }
/*
    public AggStar.FactTable.Measure getMeasure(final int i) {
        int bitPos = segments[i].measure.getBitPosition();
        return aggStar.lookupMeasure(bitPos);
    }
*/

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
        generateSql(sqlQuery);
        return sqlQuery.toString();
    }

    protected boolean hasDistinct() {
        return isDistinct;
    }

    protected void addMeasure(final int i, final SqlQuery sqlQuery) {
        AggStar.FactTable.Column column = getMeasureAsColumn(i);

        column.getTable().addToFrom(sqlQuery, false, true);
        String alias = getMeasureAlias(i);

        String expr = column.getExpression(sqlQuery);
        sqlQuery.addSelect(expr, alias);
    }

    protected void generateSql(final SqlQuery sqlQuery) {
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

            if (!hasDistinct()) {
                sqlQuery.addGroupBy(expr);
            }
        }

        // Add measures.
        // This can also add non-shared local dimension columns, which are
        // not measures.
        for (int i = 0, count = getMeasureCount(); i < count; i++) {
            addMeasure(i, sqlQuery);
        }
    }
}

// End AggQuerySpec.java
