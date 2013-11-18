/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2005 Julian Hyde
// Copyright (C) 2005-2013 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap.agg;

import mondrian.rolap.*;
import mondrian.rolap.sql.*;
import mondrian.util.Pair;

import java.util.*;

/**
 * Provides the information necessary to generate a SQL statement to
 * retrieve a list of segments.
 *
 * @author jhyde
 * @author Richard M. Emberson
 */
public class SegmentArrayQuerySpec extends AbstractQuerySpec {
    private final List<Segment> segments;
    private final Segment segment0;
    private final GroupingSetsList groupingSetsList;

    /**
     * Compound member predicates.
     * Each list constrains one dimension.
     */
    private final List<StarPredicate> compoundPredicateList;

    /**
     * Creates a SegmentArrayQuerySpec.
     *
     * @param groupingSetsList Collection of grouping sets
     * @param compoundPredicateList list of predicates representing the
     * compound member constraints
     */
    public SegmentArrayQuerySpec(
        GroupingSetsList groupingSetsList,
        List<StarPredicate> compoundPredicateList)
    {
        super(groupingSetsList.getStar(), false);
        this.segments = groupingSetsList.getDefaultSegments();
        this.segment0 = segments.get(0);
        this.groupingSetsList = groupingSetsList;
        this.compoundPredicateList = compoundPredicateList;
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
        assert segments.size() > 0;
        for (Segment segment : segments) {
            if (!Arrays.equals(segment0.predicates, segment.predicates)) {
                assert !fail;
                return false;
            }
            if (true) {
                continue;
            }
            int n = segment.predicates.length;
            if (n != segment0.predicates.length) {
                assert !fail;
                return false;
            }
            for (int j = 0; j < segment.predicates.length; j++) {
                // We only require that the two arrays have the same
                // contents, we but happen to know they are the same array,
                // because we constructed them at the same time.
                if (segment.predicates[j] != segment0.predicates[j]) {
                    assert !fail;
                    return false;
                }
            }
        }
        return true;
    }

    public List<Pair<RolapStar.Measure, String>> getMeasures() {
        return new AbstractList<Pair<RolapStar.Measure, String>>() {
            public int size() {
                return segments.size();
            }

            public Pair<RolapStar.Measure, String> get(int index) {
                return Pair.of(segments.get(index).aggMeasure, "m" + index);
            }
        };
    }

    public List<Pair<RolapStar.Column, String>> getColumns() {
        return new AbstractList<Pair<RolapStar.Column, String>>() {
            public int size() {
                return segment0.aggColumns.length;
            }

            public Pair<RolapStar.Column, String> get(int index) {
                // FIXME: SqlQuery relies on "c" and index.
                return Pair.of(segment0.aggColumns[index], "c" + index);
            }
        };
    }

    public StarColumnPredicate getColumnPredicate(final int i) {
        return segment0.predicates[i];
    }

    protected List<StarPredicate> getPredicateList() {
        if (compoundPredicateList == null) {
            return super.getPredicateList();
        } else {
            return compoundPredicateList;
        }
    }

    protected void addGroupingFunction(SqlQueryBuilder queryBuilder) {
        List<RolapStar.Column> list = groupingSetsList.getRollupColumns();
        for (RolapStar.Column column : list) {
            queryBuilder.sqlQuery.addGroupingFunction(
                column.getExpression().toSql());
        }
    }

    protected void addGroupingSets(
        SqlQueryBuilder queryBuilder,
        Map<String, String> groupingSetsAliases)
    {
        List<RolapStar.Column[]> groupingSetsColumns =
            groupingSetsList.getGroupingSetsColumns();
        for (RolapStar.Column[] groupingSetsColumn : groupingSetsColumns) {
            List<String> groupingColumnsExpr = new ArrayList<String>();
            for (RolapStar.Column column : groupingSetsColumn) {
                final String columnExpr = column.getExpression().toSql();
                if (groupingSetsAliases.containsKey(columnExpr)) {
                    groupingColumnsExpr.add(
                        groupingSetsAliases.get(columnExpr));
                } else {
                    groupingColumnsExpr.add(columnExpr);
                }
            }
            queryBuilder.sqlQuery.addGroupingSet(groupingColumnsExpr);
        }
    }

    protected boolean isAggregate() {
        return true;
    }
}

// End SegmentArrayQuerySpec.java
