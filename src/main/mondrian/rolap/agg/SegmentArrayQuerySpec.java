/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2006 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/

package mondrian.rolap.agg;

import mondrian.olap.Util;
import mondrian.rolap.RolapStar;
import mondrian.rolap.sql.SqlQuery;

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

    SegmentArrayQuerySpec(final Segment[] segments) {
        super(segments[0].aggregation.getStar());
        this.segments = segments;

        // the following code is all assertion checking
        Util.assertPrecondition(segments.length > 0, "segments.length > 0");
        for (int i = 0; i < segments.length; i++) {
            Segment segment = segments[i];
            Util.assertPrecondition(segment.aggregation == segments[0].aggregation);
            int n = segment.axes.length;
            Util.assertTrue(n == segments[0].axes.length);
            for (int j = 0; j < segment.axes.length; j++) {
                // We only require that the two arrays have the same
                // contents, we but happen to know they are the same array,
                // because we constructed them at the same time.
                Util.assertTrue(segment.axes[j].getConstraints() ==
                    segments[0].axes[j].getConstraints());
            }
        }
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

    public ColumnConstraint[] getConstraints(final int i) {
        return segments[0].axes[i].getConstraints();
    }

    public String generateSqlQuery() {
        SqlQuery sqlQuery = newSqlQuery();

        if (!sqlQuery.getDialect().allowsCountDistinct() && hasDistinct()) {
            distinctGenerateSql(sqlQuery);
        } else {
            nonDistinctGenerateSql(sqlQuery, false, false);
        }

        return sqlQuery.toString();
    }

    /**
     * Returns whether one or more of the measures is a distinct measure.
     */
    protected boolean hasDistinct() {
        for (int i = 0, count = getMeasureCount(); i < count; i++) {
            RolapStar.Measure measure = getMeasure(i);
            if (measure.getAggregator().isDistinct()) {
                return true;
            }
        }
        return false;
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

    protected void distinctGenerateSql(final SqlQuery outerSqlQuery) {
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
            ColumnConstraint[] constraints = getConstraints(i);
            if (constraints != null) {
                innerSqlQuery.addWhere(
                    RolapStar.Column.createInExpr(
                        expr,
                        constraints,
                        column.getDatatype(),
                        innerSqlQuery.getDialect()));
            }
            final String alias = "d" + i;
            innerSqlQuery.addSelect(expr, alias);
            outerSqlQuery.addSelect(alias);
            outerSqlQuery.addGroupBy(alias);
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
                        alias));
        }
        outerSqlQuery.addFrom(innerSqlQuery, "dummyname", true);
    }
}

// End SegmentArrayQuerySpec.java
