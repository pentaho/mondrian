/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2007-2009 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap.agg;

import mondrian.rolap.RolapStar;
import mondrian.rolap.BitKey;

import java.util.*;

/**
 * Class for using GROUP BY GROUPING SETS sql query.
 *
 * <p>For example, suppose we have the 3 grouping sets (a, b, c), (a, b) and
 * (b, c).<ul>
 * <li>detailed grouping set -> (a, b, c)
 * <li>rolled-up grouping sets -> (a, b), (b, c)
 * <li>rollup columns -> c, a (c for (a, b) and a for (b, c))
 * <li>rollup columns bitkey -><br/>
 *    (a, b, c) grouping set represented as 0, 0, 0<br/>
 *    (a, b) grouping set represented as 0, 0, 1<br/>
 *    (b, c) grouping set represented as 1, 0, 0
 * </ul>
 *
 * @author Thiyagu
 * @version $Id$
 * @since 24 May 2007
 */
class GroupingSetsList {

    private final List<RolapStar.Column> rollupColumns;

    private final List<RolapStar.Column[]> groupingSetsColumns;
    private final boolean useGroupingSet;

    private final List<BitKey> rollupColumnsBitKeyList =
        new ArrayList<BitKey>();

    /**
     * Maps column index to grouping function index.
     */
    private final Map<Integer, Integer> columnIndexToGroupingIndexMap =
        new HashMap<Integer, Integer>();

    private final List<GroupingSet> groupingSets;

    /**
     * Creates a GroupingSetsList.
     *
     * <p>First element of the groupingSets list should be the detailed
     * grouping set (default grouping set), followed by grouping sets which can
     * be rolled-up.
     *
     * @param groupingSets List of groups of columns
     */
    public GroupingSetsList(List<GroupingSet> groupingSets) {
        this.groupingSets = groupingSets;
        this.useGroupingSet = groupingSets.size() > 1;
        if (useGroupingSet) {
            this.groupingSetsColumns = getGroupingColumnsList(groupingSets);
            this.rollupColumns = findRollupColumns();
            loadRollupIndex();
            loadGroupingColumnBitKeys();
        } else {
            this.groupingSetsColumns = new ArrayList<RolapStar.Column[]>();
            this.rollupColumns = new ArrayList<RolapStar.Column>();
        }
    }

    List<RolapStar.Column[]> getGroupingColumnsList(
        List<GroupingSet> groupingSets)
    {
        List<RolapStar.Column[]> groupingColumns =
            new ArrayList<RolapStar.Column[]>();
        for (GroupingSet aggBatchDetail : groupingSets) {
            groupingColumns.add(aggBatchDetail.getSegments()[0]
                .aggregation.getColumns());
        }
        return groupingColumns;
    }

    public List<RolapStar.Column> getRollupColumns() {
        return rollupColumns;
    }

    public List<RolapStar.Column[]> getGroupingSetsColumns() {
        return groupingSetsColumns;
    }

    public List<BitKey> getRollupColumnsBitKeyList() {
        return rollupColumnsBitKeyList;
    }

    public BitKey getDetailedColumnsBitKey() {
        return rollupColumnsBitKeyList.get(0);
    }

    private void loadGroupingColumnBitKeys() {
        int bitKeyLength = getDefaultColumns().length;
        for (RolapStar.Column[] groupingSetColumns : groupingSetsColumns) {
            BitKey groupingColumnsBitKey =
                BitKey.Factory.makeBitKey(bitKeyLength);
            Set<RolapStar.Column> columns =
                convertToSet(groupingSetColumns);
            int bitPosition = 0;
            for (RolapStar.Column rollupColumn : rollupColumns) {
                if (!columns.contains(rollupColumn)) {
                    groupingColumnsBitKey.set(bitPosition);
                }
                bitPosition++;
            }
            rollupColumnsBitKeyList.add(groupingColumnsBitKey);
        }
    }

    private void loadRollupIndex() {
        RolapStar.Column[] detailedColumns = getDefaultColumns();
        for (int columnIndex = 0; columnIndex < detailedColumns.length;
             columnIndex++) {
            int rollupIndex =
                rollupColumns.indexOf(detailedColumns[columnIndex]);
            columnIndexToGroupingIndexMap.put(columnIndex, rollupIndex);
        }
    }

    private List<RolapStar.Column> findRollupColumns() {
        Set<RolapStar.Column> rollupSet = new TreeSet<RolapStar.Column>(
            RolapStar.ColumnComparator.instance);
        for (RolapStar.Column[] groupingSetColumn : groupingSetsColumns) {
            Set<RolapStar.Column> summaryColumns =
                convertToSet(groupingSetColumn);
            for (RolapStar.Column column : getDefaultColumns()) {
                if (!summaryColumns.contains(column)) {
                    rollupSet.add(column);
                }
            }
        }
        return new ArrayList<RolapStar.Column>(rollupSet);
    }

    private Set<RolapStar.Column> convertToSet(RolapStar.Column[] columns) {
        HashSet<RolapStar.Column> columnSet = new HashSet<RolapStar.Column>();
        for (RolapStar.Column column : columns) {
            columnSet.add(column);
        }
        return columnSet;
    }

    public boolean useGroupingSets() {
        return useGroupingSet;
    }

    public int findGroupingFunctionIndex(int columnIndex) {
        return columnIndexToGroupingIndexMap.get(columnIndex);
    }

    public Aggregation.Axis[] getDefaultAxes() {
        return getDefaultGroupingSet().getAxes();
    }

    protected GroupingSet getDefaultGroupingSet() {
        return groupingSets.get(0);
    }

    public RolapStar.Column[] getDefaultColumns() {
        return getDefaultGroupingSet().getSegments()[0].aggregation
            .getColumns();
    }

    public Segment[] getDefaultSegments() {
        return getDefaultGroupingSet().getSegments();
    }

    public BitKey getDefaultLevelBitKey() {
        return getDefaultGroupingSet().getLevelBitKey();
    }

    public BitKey getDefaultMeasureBitKey() {
        return getDefaultGroupingSet().getMeasureBitKey();
    }

    public RolapStar getStar() {
        return getDefaultSegments()[0].aggregation.getStar();
    }

    public List<GroupingSet> getGroupingSets() {
        return groupingSets;
    }

    public List<GroupingSet> getRollupGroupingSets() {
        ArrayList<GroupingSet> rollupGroupingSets =
            new ArrayList<GroupingSet>(groupingSets);
        rollupGroupingSets.remove(0);
        return rollupGroupingSets;
    }
}

// End GroupingSetsList.java
