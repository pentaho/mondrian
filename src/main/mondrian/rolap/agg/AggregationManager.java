/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 30 August, 2001
*/

package mondrian.rolap.agg;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import mondrian.olap.MondrianDef;
import mondrian.olap.Util;
import mondrian.rolap.RolapAggregationManager;
import mondrian.rolap.RolapStar;
import mondrian.rolap.sql.SqlQuery;

/**
 * <code>RolapAggregationManager</code> manages all {@link Aggregation}s
 * in the system. It is a singleton class.
 *
 * @author jhyde
 * @since 30 August, 2001
 * @version $Id$
 **/
public class AggregationManager extends RolapAggregationManager {
    private static AggregationManager instance;

    /** Returns or creates the singleton. **/
    public static synchronized AggregationManager instance() {
        if (instance == null) {
            instance = new AggregationManager();
        }
        return instance;
    }

    AggregationManager() {
        super();
    }

    public void loadAggregation(RolapStar.Measure[] measures, 
                                RolapStar.Column[] columns,
                                ColumnConstraint[][] constraintses, 
                                Collection pinnedSegments) {
        RolapStar star = measures[0].getStar();
        Aggregation aggregation = star.lookupOrCreateAggregation(columns);

        // try to eliminate unneccessary constraints
        // for Oracle: prevent an IN-clause with more than 1000 elements
        constraintses = aggregation.optimizeConstraints(constraintses);

        aggregation.load(measures, constraintses, pinnedSegments);
    }



    public Object getCellFromCache(CellRequest request) {
        RolapStar.Measure measure = request.getMeasure();
        Aggregation aggregation = measure.getStar().lookupAggregation(
            request.getColumns());
        if (aggregation == null) {
            return null; // cell is not in any aggregation
        }
        Object o = aggregation.get(
                measure, request.getSingleValues(), null);
        if (o != null) {
            return o;
        }
        throw Util.getRes().newInternal("not found");
    }

    public Object getCellFromCache(CellRequest request, Set pinSet) {
        Util.assertPrecondition(pinSet != null);

        RolapStar.Measure measure = request.getMeasure();
        Aggregation aggregation = measure.getStar().lookupAggregation(
            request.getColumns());
        return (aggregation == null)
            // cell is not in any aggregation
            ? null 
            : aggregation.get(measure, request.getSingleValues(), pinSet);
    }

    public String getDrillThroughSQL(final CellRequest request) {
        final DrillThroughQuerySpec spec = new DrillThroughQuerySpec(request);
        return generateSQL(spec, false);
    }

    /**
     * Generates the query to retrieve the cells for a list of segments.
     */
    static String generateSQL(Segment[] segments) {
        final SegmentArrayQuerySpec spec = new SegmentArrayQuerySpec(segments);
        return generateSQL(spec, true);
    }

    /**
     * Generates the query to retrieve the cells for a list of segments.
     *
     * @param spec Query specification
     * @param aggregate Whether to aggregate; <code>true</code> for populating
     *   a segment, <code>false</code> for drill-through
     */
    private static String generateSQL(QuerySpec spec, boolean aggregate) {
        java.sql.Connection jdbcConnection = spec.getStar().getJdbcConnection();
        try {
            return generateSql(spec, jdbcConnection, aggregate);
        } finally {
            try {
                jdbcConnection.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    private static String generateSql(QuerySpec spec,
                                      java.sql.Connection jdbcConnection, 
                                      boolean aggregate) {
        final DatabaseMetaData metaData;
        try {
            metaData = jdbcConnection.getMetaData();
        } catch (SQLException e) {
            throw Util.getRes().newInternal("while loading segment", e);
        }
        RolapStar star = spec.getStar();
        // are there any distinct measures?
        int distinctCount = 0;
        for (int i = 0, measureCount = spec.getMeasureCount(); i < measureCount; i++) {
            RolapStar.Measure measure = spec.getMeasure(i);
            if (measure.getAggregator().isDistinct()) {
                distinctCount++;
            }
        }
        SqlQuery sqlQuery = new SqlQuery(metaData);
        final boolean wrapInDistinct = aggregate &&
                distinctCount > 0 &&
                !sqlQuery.allowsCountDistinct();
        if (wrapInDistinct) {
            // Generate something like
            //  select d0, d1, count(m0)
            //  from (
            //    select distinct x as d0, y as d1, z as m0
            //    from t) as foo
            //  group by d0, d1
            final SqlQuery innerSqlQuery = sqlQuery;
            innerSqlQuery.setDistinct(true);
            final SqlQuery outerSqlQuery = new SqlQuery(metaData);
            // add constraining dimensions
            RolapStar.Column[] columns = spec.getColumns();
            int arity = columns.length;
            for (int i = 0; i < arity; i++) {
                RolapStar.Column column = columns[i];
                RolapStar.Table table = column.getTable();
                if (table.isFunky()) {
                    // this is a funky dimension -- ignore for now
                    continue;
                }
                table.addToFrom(innerSqlQuery, false, true);
                String expr = column.getExpression(innerSqlQuery);
                ColumnConstraint[] constraints = spec.getConstraints(i);
                if (constraints != null) {
                    innerSqlQuery.addWhere(
                            column.createInExpr(expr, constraints));
                }
                final String alias = "d" + i;
                innerSqlQuery.addSelect(expr, alias);
                outerSqlQuery.addSelect(alias);
                outerSqlQuery.addGroupBy(alias);
            }
            for (int i = 0, measureCount = spec.getMeasureCount(); i < measureCount; i++) {
                RolapStar.Measure measure = spec.getMeasure(i);
                Util.assertTrue(measure.getTable() == star.getFactTable());
                star.getFactTable().addToFrom(innerSqlQuery, false, true);
                final String alias = "m" + i;
                innerSqlQuery.addSelect(measure.getExpression(outerSqlQuery), alias);
                outerSqlQuery.addSelect(
                    measure.getAggregator().getNonDistinctAggregator().getExpression(
                            alias));
            }
            outerSqlQuery.addFrom(innerSqlQuery, "foo", true);
            sqlQuery = outerSqlQuery;
        } else {
            // add constraining dimensions
            RolapStar.Column[] columns = spec.getColumns();
            int arity = columns.length;
            for (int i = 0; i < arity; i++) {
                RolapStar.Column column = columns[i];
                RolapStar.Table table = column.getTable();
                if (table.isFunky()) {
                    // this is a funky dimension -- ignore for now
                    continue;
                }
                table.addToFrom(sqlQuery, false, true);
                String expr = column.getExpression(sqlQuery);
                ColumnConstraint[] constraints = spec.getConstraints(i);
                if (constraints != null) {
                    sqlQuery.addWhere(
                            column.createInExpr(expr, constraints));
                }

                // some DB2 (AS400) versions throw an error, if a column alias is there
                //  and *not* used in a subsequent order by/group by
                if (sqlQuery.isAS400()) {
                    sqlQuery.addSelect(expr, null);
                } else {
                    sqlQuery.addSelect(expr, spec.getColumnAlias(i));
                }

                if (aggregate) {
                    sqlQuery.addGroupBy(expr);
                }
            }
            // add measures
            for (int i = 0, measureCount = spec.getMeasureCount(); i < measureCount; i++) {
                RolapStar.Measure measure = spec.getMeasure(i);
                Util.assertTrue(measure.getTable() == star.getFactTable());
                star.getFactTable().addToFrom(sqlQuery, false, true);
                String expr = measure.getExpression(sqlQuery);
                if (aggregate) {
                    expr = measure.getAggregator().getExpression(expr);
                }
                sqlQuery.addSelect(expr, spec.getMeasureAlias(i));
            }
        }
        return sqlQuery.toString();
    }

    /**
     * Contains the information necessary to generate a SQL statement to
     * retrieve a set of cells.
     */
    private interface QuerySpec {
        int getMeasureCount();
        RolapStar.Measure getMeasure(int i);
        String getMeasureAlias(int i);
        RolapStar getStar();
        RolapStar.Column[] getColumns();
        String getColumnAlias(int i);
        ColumnConstraint[] getConstraints(int i);
    }

    /**
     * Provides the information necessary to generate a SQL statement to
     * retrieve a list of segments.
     */
    private static class SegmentArrayQuerySpec implements QuerySpec {
        private final Segment[] segments;

        SegmentArrayQuerySpec(Segment[] segments) {
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

        public RolapStar.Measure getMeasure(int i) {
            return segments[i].measure;
        }

        public String getMeasureAlias(int i) {
            return "m" + Integer.toString(i);
        }

        public RolapStar getStar() {
            return segments[0].aggregation.getStar();
        }

        public RolapStar.Column[] getColumns() {
            return segments[0].aggregation.getColumns();
        }

        public String getColumnAlias(int i) {
            return "c" + Integer.toString(i);
        }

        public ColumnConstraint[] getConstraints(int i) {
            return segments[0].axes[i].getConstraints();
        }
    }

    /**
     * Provides the information necessary to generate SQL for a drill-through
     * request.
     */
    private static class DrillThroughQuerySpec implements QuerySpec {
        private final CellRequest request;
        private final String[] columnNames;
        private final RolapStar star;

        public DrillThroughQuerySpec(CellRequest request) {
            this.request = request;
            this.star = request.getMeasure().getStar();
            this.columnNames = computeDistinctColumnNames();
        }

        private String[] computeDistinctColumnNames() {
            final List columnNames = new ArrayList();
            final RolapStar.Column[] columns = getColumns();
            Set columnNameSet = new HashSet();
            for (int i = 0; i < columns.length; i++) {
                RolapStar.Column column = columns[i];
                addColumnName(column, columnNames, columnNameSet);
            }
            addColumnName(request.getMeasure(), columnNames, columnNameSet);
            return (String[]) columnNames.toArray(new String[columnNames.size()]);
        }

        private void addColumnName(RolapStar.Column column, 
                                   final List columnNames, 
                                   Set columnNameSet) {
            String columnName = star.getColumnName(column);
            if (columnName != null) {
                // nothing
            } else if (column.getExpression() instanceof MondrianDef.Column) {
                columnName = ((MondrianDef.Column) column.getExpression()).name;
            } else {
                columnName = "c" + Integer.toString(columnNames.size());
            }
            // Register the column name, and if it's not unique,
            // generate names until it is.
            for (int j = 0; !columnNameSet.add(columnName); j++) {
                columnName = "x" + Integer.toString(j);
            }
            columnNames.add(columnName);
        }

        public int getMeasureCount() {
            return 1;
        }

        public RolapStar.Measure getMeasure(int i) {
            Util.assertTrue(i == 0);
            return request.getMeasure();
        }

        public String getMeasureAlias(int i) {
            Util.assertTrue(i == 0);
            return columnNames[columnNames.length - 1];
        }

        public RolapStar getStar() {
            return request.getMeasure().getStar();
        }

        public RolapStar.Column[] getColumns() {
            return request.getColumns();
        }

        public String getColumnAlias(int i) {
            return columnNames[i];
        }

        public ColumnConstraint[] getConstraints(int i) {
            final ColumnConstraint constr = 
                (ColumnConstraint) request.getValueList().get(i);
            return (constr == null)
                ? null
                : new ColumnConstraint[] {constr};
        }
    }

}

// End RolapAggregationManager.java
