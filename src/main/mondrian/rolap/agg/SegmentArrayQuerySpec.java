/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/

package mondrian.rolap.agg;

import mondrian.olap.Util;
import mondrian.rolap.RolapStar;
import mondrian.rolap.StarColumnPredicate;
import mondrian.rolap.sql.SqlQuery;

import java.util.ArrayList;
import java.util.List;

/**
 * Provides the information necessary to generate a SQL statement to
 * retrieve a list of segments.
 *
 * @author jhyde
 * @author Richard M. Emberson
 * @version $Id$
 */
class SegmentArrayQuerySpec extends AbstractQuerySpec {
    private final Segment[] segments;
    private final GroupByGroupingSets groupByGroupingSets;

    /**
     * Creates a SegmentArrayQuerySpec.
     *
     * @param groupByGroupingSets
     */
    SegmentArrayQuerySpec(
        GroupByGroupingSets groupByGroupingSets) {
        super(groupByGroupingSets.getStar());
        this.segments = groupByGroupingSets.getDefaultSegments();
        this.groupByGroupingSets = groupByGroupingSets;
        assert isValid(true);
    }

    /**
     * Returns whether this query specification is valid, or throws if invalid
     * and <code>fail</code> is true.
     *
     * @param fail Whether to throw if invalid
     * @return Whether this query specification is valid
     */
    private boolean isValid(boolean fail) {
        assert segments.length > 0;
        for (Segment segment : segments) {
            if (segment.aggregation != segments[0].aggregation) {
                assert!fail;
                return false;
            }
            int n = segment.axes.length;
            if (n != segments[0].axes.length) {
                assert!fail;
                return false;
            }
            for (int j = 0; j < segment.axes.length; j++) {
                // We only require that the two arrays have the same
                // contents, we but happen to know they are the same array,
                // because we constructed them at the same time.
                if (segment.axes[j].getPredicate() !=
                    segments[0].axes[j].getPredicate()) {
                    assert!fail;
                    return false;
                }
            }
        }
        return true;
    }

    public int getMeasureCount() {
        return segments.length;
    }

    public RolapStar.Measure getMeasure(final int i) {
        return segments[i].measure;
    }

    public String getMeasureAlias(final int i) {
        return "m" + Integer.toString(i);
    }

    public RolapStar.Column[] getColumns() {
        return segments[0].aggregation.getColumns();
    }

    /**
     * SqlQuery relies on "c" and index. All this should go into SqlQuery!
     *
     * @see mondrian.rolap.sql.SqlQuery#addOrderBy
     */
    public String getColumnAlias(final int i) {
        return "c" + Integer.toString(i);
    }

    public StarColumnPredicate getColumnPredicate(final int i) {
        return segments[0].axes[i].getPredicate();
    }

    public String generateSqlQuery() {
        SqlQuery sqlQuery = newSqlQuery();

        int k = getDistinctMeasureCount();
        final SqlQuery.Dialect dialect = sqlQuery.getDialect();
        if (!dialect.allowsCountDistinct() && k > 0 ||
            !dialect.allowsMultipleCountDistinct() && k > 1) {
            distinctGenerateSql(sqlQuery);
        } else {
            nonDistinctGenerateSql(sqlQuery, false, false);
        }
        addGroupingFunction(sqlQuery);
        addGroupingSets(sqlQuery);
        return sqlQuery.toString();
    }

    private void addGroupingFunction(SqlQuery sqlQuery) {
        List<RolapStar.Column> list = groupByGroupingSets.getRollupColumns();
        for (RolapStar.Column column : list) {
            sqlQuery.addGroupingFunction(column.generateExprString(sqlQuery));
        }
    }

    private void addGroupingSets(SqlQuery sqlQuery) {
        List<RolapStar.Column[]> groupingSetsColumns =
            groupByGroupingSets.getGroupingSetsColumns();
        for (RolapStar.Column[] groupingSetsColumn : groupingSetsColumns) {
            ArrayList<String> groupingColumnsExpr = new ArrayList<String>();
            for (RolapStar.Column aColumn : groupingSetsColumn) {
                groupingColumnsExpr.add(aColumn.generateExprString(sqlQuery));
            }
            sqlQuery.addGroupingSet(groupingColumnsExpr);
        }
    }
    
    /**
     * Returns the number of measures which are distinct.
     *
     * @return the number of measures which are distinct
     */
    protected int getDistinctMeasureCount() {
        int k = 0;
        for (int i = 0, count = getMeasureCount(); i < count; i++) {
            RolapStar.Measure measure = getMeasure(i);
            if (measure.getAggregator().isDistinct()) {
                ++k;
            }
        }
        return k;
    }

    protected void addMeasure(final int i, final SqlQuery sqlQuery) {
        RolapStar.Measure measure = getMeasure(i);
        Util.assertTrue(measure.getTable() == getStar().getFactTable());
        measure.getTable().addToFrom(sqlQuery, false, true);

        String exprInner = measure.generateExprString(sqlQuery);
        String exprOuter = measure.getAggregator().getExpression(exprInner);
        sqlQuery.addSelect(exprOuter, getMeasureAlias(i));
    }

    protected boolean isAggregate() {
        return true;
    }

    /**
     * Generates a SQL query to retrieve the values in this segment using
     * an algorithm which converts distinct-aggregates to non-distinct
     * aggregates over subqueries.
     *
     * @param outerSqlQuery Query to modify
     */
    protected void distinctGenerateSql(final SqlQuery outerSqlQuery) {
        final SqlQuery.Dialect dialect = outerSqlQuery.getDialect();
        // Generate something like
        //  select d0, d1, count(m0)
        //  from (
        //    select distinct x as d0, y as d1, z as m0
        //    from t) as foo
        //  group by d0, d1

        final SqlQuery innerSqlQuery = newSqlQuery();
        innerSqlQuery.setDistinct(true);

        // add constraining dimensions
        RolapStar.Column[] columns = getColumns();
        int arity = columns.length;
        for (int i = 0; i < arity; i++) {
            RolapStar.Column column = columns[i];
            RolapStar.Table table = column.getTable();
            if (table.isFunky()) {
                // this is a funky dimension -- ignore for now
                continue;
            }
            table.addToFrom(innerSqlQuery, false, true);
            String expr = column.generateExprString(innerSqlQuery);
            StarColumnPredicate predicate = getColumnPredicate(i);
            final String where = RolapStar.Column.createInExpr(
                expr,
                predicate,
                column.getDatatype(),
                innerSqlQuery.getDialect());
            if (!where.equals("true")) {
                innerSqlQuery.addWhere(where);
            }
            final String alias = "d" + i;
            innerSqlQuery.addSelect(expr, alias);
            final String quotedAlias = dialect.quoteIdentifier(alias);
            outerSqlQuery.addSelect(quotedAlias);
            outerSqlQuery.addGroupBy(quotedAlias);
        }
        for (int i = 0, count = getMeasureCount(); i < count; i++) {
            RolapStar.Measure measure = getMeasure(i);

            Util.assertTrue(measure.getTable() == getStar().getFactTable());
            measure.getTable().addToFrom(innerSqlQuery, false, true);

            String alias = getMeasureAlias(i);
            String expr = measure.generateExprString(outerSqlQuery);
            innerSqlQuery.addSelect(expr, alias);

            outerSqlQuery.addSelect(
                measure.getAggregator().getNonDistinctAggregator().getExpression(
                    dialect.quoteIdentifier(alias)));
        }
        outerSqlQuery.addFrom(innerSqlQuery, "dummyname", true);
    }
}

// End SegmentArrayQuerySpec.java
