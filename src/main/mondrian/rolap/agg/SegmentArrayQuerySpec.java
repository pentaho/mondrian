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

import mondrian.rolap.RolapStar;
import mondrian.rolap.StarColumnPredicate;
import mondrian.rolap.StarPredicate;
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
    private final GroupingSetsList groupingSetsList;

    /*
     * Compunnd member predicates.
     * Each list constrains one dimension.
     */
    private List<StarPredicate> compoundPredicateList;

    /**
     * Creates a SegmentArrayQuerySpec.
     *
     * @param groupingSetsList Collection of grouping sets
     * @param compoundPredicateList list of predicates representing the 
     * compound member constraints
     */
    SegmentArrayQuerySpec(
        GroupingSetsList groupingSetsList,
        List<StarPredicate> compoundPredicateList)
    {
        super(groupingSetsList.getStar(), false);
        this.segments = groupingSetsList.getDefaultSegments();
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

    protected List<StarPredicate> getPredicateList() {
        if (compoundPredicateList == null) {
            return super.getPredicateList();
        } else {
            return compoundPredicateList;
        }
    }
    
    protected void addGroupingFunction(SqlQuery sqlQuery) {
        List<RolapStar.Column> list = groupingSetsList.getRollupColumns();
        for (RolapStar.Column column : list) {
            sqlQuery.addGroupingFunction(column.generateExprString(sqlQuery));
        }
    }

    protected void addGroupingSets(SqlQuery sqlQuery) {
        List<RolapStar.Column[]> groupingSetsColumns =
            groupingSetsList.getGroupingSetsColumns();
        for (RolapStar.Column[] groupingSetsColumn : groupingSetsColumns) {
            ArrayList<String> groupingColumnsExpr = new ArrayList<String>();
            for (RolapStar.Column aColumn : groupingSetsColumn) {
                groupingColumnsExpr.add(aColumn.generateExprString(sqlQuery));
            }
            sqlQuery.addGroupingSet(groupingColumnsExpr);
        }
    }

    protected boolean isAggregate() {
        return true;
    }
}

// End SegmentArrayQuerySpec.java
