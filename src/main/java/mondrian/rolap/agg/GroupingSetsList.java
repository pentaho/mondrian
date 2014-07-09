/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2007-2012 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap.agg;

import mondrian.rolap.*;

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
 * @since 24 May 2007
 */
public final class GroupingSetsList {

    private final List<RolapStar.Column> rollupColumns;
    private final List<RolapStar.Column[]> groupingSetsColumns;
    private final boolean useGroupingSet;

    private final List<BitKey> rollupColumnsBitKeyList;

    /**
     * Maps column index to grouping function index.
     */
    private final int[] columnIndexToGroupingIndexMap;

    private final List<GroupingSet> groupingSets;
    private final int groupingBitKeyIndex;

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

            int arity = getDefaultColumns().length;
            int segmentLength = getDefaultSegments().size();
            this.groupingBitKeyIndex = arity + segmentLength;
        } else {
            this.groupingSetsColumns = Collections.emptyList();
            this.rollupColumns = Collections.emptyList();
            this.groupingBitKeyIndex = -1;
        }
        this.columnIndexToGroupingIndexMap = loadRollupIndex();
        this.rollupColumnsBitKeyList = loadGroupingColumnBitKeys();
    }

    List<RolapStar.Column[]> getGroupingColumnsList(
        List<GroupingSet> groupingSets)
    {
        List<RolapStar.Column[]> groupingColumns =
            new ArrayList<RolapStar.Column[]>();
        for (GroupingSet aggBatchDetail : groupingSets) {
            groupingColumns.add(
                aggBatchDetail.segment0.getColumns());
        }
        return groupingColumns;
    }

    public int getGroupingBitKeyIndex() {
        return groupingBitKeyIndex;
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

    private List<BitKey> loadGroupingColumnBitKeys() {
        if (!useGroupingSet) {
            return Collections.singletonList(BitKey.EMPTY);
        }
        final List<BitKey> rollupColumnsBitKeyList = new ArrayList<BitKey>();
        final int bitKeyLength = getDefaultColumns().length;
        for (RolapStar.Column[] groupingSetColumns : groupingSetsColumns) {
            BitKey groupingColumnsBitKey =
                BitKey.Factory.makeBitKey(bitKeyLength);
            Set<RolapStar.Column> columns =
                new HashSet<RolapStar.Column>(
                    Arrays.asList(groupingSetColumns));
            int bitPosition = 0;
            for (RolapStar.Column rollupColumn : rollupColumns) {
                if (!columns.contains(rollupColumn)) {
                    groupingColumnsBitKey.set(bitPosition);
                }
                bitPosition++;
            }
            rollupColumnsBitKeyList.add(groupingColumnsBitKey);
        }
        return rollupColumnsBitKeyList;
    }

    private int[] loadRollupIndex() {
        if (!useGroupingSet) {
            return new int[0];
        }
        RolapStar.Column[] detailedColumns = getDefaultColumns();
        int[] columnIndexToGroupingIndexMap = new int[detailedColumns.length];
        for (int columnIndex = 0; columnIndex < detailedColumns.length;
             columnIndex++)
        {
            int rollupIndex =
                rollupColumns.indexOf(detailedColumns[columnIndex]);
            columnIndexToGroupingIndexMap[columnIndex] = rollupIndex;
        }
        return columnIndexToGroupingIndexMap;
    }

    private List<RolapStar.Column> findRollupColumns() {
        Set<RolapStar.Column> rollupSet = new TreeSet<RolapStar.Column>(
            RolapStar.ColumnComparator.instance);
        for (RolapStar.Column[] groupingSetColumn : groupingSetsColumns) {
            Set<RolapStar.Column> summaryColumns =
                new HashSet<RolapStar.Column>(
                    Arrays.asList(groupingSetColumn));
            for (RolapStar.Column column : getDefaultColumns()) {
                if (!summaryColumns.contains(column)) {
                    rollupSet.add(column);
                }
            }
        }
        return new ArrayList<RolapStar.Column>(rollupSet);
    }

    public boolean useGroupingSets() {
        return useGroupingSet;
    }

    public int findGroupingFunctionIndex(int columnIndex) {
        return columnIndexToGroupingIndexMap[columnIndex];
    }

    public SegmentAxis[] getDefaultAxes() {
        return getDefaultGroupingSet().getAxes();
    }

    public StarColumnPredicate[] getDefaultPredicates() {
        return getDefaultGroupingSet().getPredicates();
    }

    protected GroupingSet getDefaultGroupingSet() {
        return groupingSets.get(0);
    }

    public RolapStar.Column[] getDefaultColumns() {
        return getDefaultGroupingSet().segment0.getColumns();
    }

    public List<Segment> getDefaultSegments() {
        return getDefaultGroupingSet().getSegments();
    }

    public BitKey getDefaultLevelBitKey() {
        return getDefaultGroupingSet().getLevelBitKey();
    }

    public BitKey getDefaultMeasureBitKey() {
        return getDefaultGroupingSet().getMeasureBitKey();
    }

    public RolapStar getStar() {
        return getDefaultGroupingSet().segment0.aggMeasure.getStar();
    }

    public List<GroupingSet> getGroupingSets() {
        return groupingSets;
    }

    public List<GroupingSet> getRollupGroupingSets() {
        return groupingSets.subList(1, groupingSets.size());
    }

    /**
     * Collection of {@link mondrian.rolap.agg.SegmentDataset} that have the
     * same dimensionality and identical axis values. A cohort contains
     * corresponding cell values for set of measures.
     */
    public static class Cohort
    {
        final List<SegmentDataset> segmentDatasetList;
        final SegmentAxis[] axes;
        // workspace
        final int[] pos;

        Cohort(
            List<SegmentDataset> segmentDatasetList,
            SegmentAxis[] axes)
        {
            this.segmentDatasetList = segmentDatasetList;
            this.axes = axes;
            this.pos = new int[axes.length];
        }
    }
}

// End GroupingSetsList.java
