/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2008 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap.agg;

import mondrian.rolap.*;
import mondrian.olap.Util;
import mondrian.olap.MondrianProperties;

import java.util.*;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * <p>The <code>SegmentLoader</code> queries database and loads the data into
 * the given set of segments.</p>
 *
 * <p>It reads a segment of <code>measure</code>, where <code>columns</code> are
 * constrained to <code>values</code>.  Each entry in <code>values</code>
 * can be null, meaning don't constrain, or can have several values. For
 * example, <code>getSegment({Unit_sales}, {Region, State, Year}, {"West"},
 * {"CA", "OR", "WA"}, null})</code> returns sales in states CA, OR and WA
 * in the Western region, for all years.</p>
 *
 * @author Thiyagu
 * @version $Id$
 * @since 24 May 2007
 */
public class SegmentLoader {
    /**
     * Loads data for all the segments of the GroupingSets. If the grouping sets
     * list contains more than one Grouping Set then data is loaded using the
     * GROUP BY GROUPING SETS sql. Else if only one grouping set is passed in
     * the list data is loaded without using GROUP BY GROUPING SETS sql. If the
     * database does not support grouping sets
     * {@link mondrian.spi.Dialect#supportsGroupingSets()} then
     * grouping sets list should always have only one element in it.
     *
     * <p>For example, if list has 2 grouping sets with columns A, B, C and B, C
     * respectively, then the SQL will be
     * "GROUP BY GROUPING SETS ((A, B, C), (B, C))".
     *
     * <p>Else if the list has only one grouping set then sql would be without
     * grouping sets.
     *
     * <p>The <code>groupingSets</code> list should be topological order, with
     * more detailed higher-level grouping sets occuring first. In other words,
     * the first element of the list should always be the detailed grouping
     * set (default grouping set), followed by grouping sets which can be
     * rolled-up on this detailed grouping set.
     * In the example (A, B, C) is the detailed grouping set and (B, C) is
     * rolled-up using the detailed.
     *
     * @param groupingSets   List of grouping sets whose segments are loaded
     * @param pinnedSegments Pinned segments
     */
    public void load(
        List<GroupingSet> groupingSets,
        RolapAggregationManager.PinSet pinnedSegments,
        List<StarPredicate> compoundPredicateList)
    {
        GroupingSetsList groupingSetsList =
            new GroupingSetsList(groupingSets);
        boolean useGroupingSet = groupingSetsList.useGroupingSets();
        RolapStar.Column[] defaultColumns =
            groupingSetsList.getDefaultColumns();
        SqlStatement stmt = null;
        try {
            stmt = createExecuteSql(
                groupingSetsList,
                compoundPredicateList);
            int arity = defaultColumns.length;
            SortedSet<Comparable<?>>[] axisValueSets =
                getDistinctValueWorkspace(arity);

            boolean[] axisContainsNull = new boolean[arity];

            List<Object[]> rows =
                processData(
                    stmt, axisContainsNull,
                    axisValueSets, groupingSetsList);

            boolean sparse =
                setAxisDataAndDecideSparseUse(axisValueSets,
                    axisContainsNull, groupingSetsList,
                    rows);

            SegmentDataset[] nonGroupingDataSets = null;

            final Map<BitKey, SegmentDataset[]> groupingDataSetsMap =
                new HashMap<BitKey, SegmentDataset[]>();

            if (useGroupingSet) {
                populateDataSetMapOnGroupingColumnsBitKeys(
                    groupingSetsList,
                    sparse, groupingDataSetsMap);
            } else {
                nonGroupingDataSets = createDataSets(
                    sparse,
                    groupingSetsList.getDefaultSegments(),
                    groupingSetsList.getDefaultAxes());
            }

            loadDataToDataSets(
                groupingSetsList, rows, groupingDataSetsMap,
                nonGroupingDataSets, axisContainsNull, sparse);

            setDataToSegments(
                groupingSetsList, nonGroupingDataSets,
                groupingDataSetsMap, pinnedSegments);

        } catch (SQLException e) {
            throw stmt.handle(e);
        } finally {
            if (stmt != null) {
                stmt.close();
            }
            // Any segments which are still loading have failed.
            setFailOnStillLoadingSegments(groupingSetsList);
        }
    }

    void setFailOnStillLoadingSegments(GroupingSetsList groupingSetsList) {
        for (GroupingSet groupingset : groupingSetsList.getGroupingSets()) {
            for (Segment segment : groupingset.getSegments()) {
                segment.setFailIfStillLoading();
            }
        }
    }

    /**
     * Loads data to the datasets. If the grouping sets is used,
     * dataset is fetched from groupingDataSetMap using grouping bit keys of
     * the row data. If grouping sets is not used, data is loaded on to
     * nonGroupingDataSets.
     */
    private void loadDataToDataSets(
        GroupingSetsList groupingSetsList, List<Object[]> rows,
        Map<BitKey, SegmentDataset[]> groupingDataSetMap,
        SegmentDataset[] nonGroupingDataSets, boolean[] axisContainsNull,
        boolean sparse)
    {
        int arity = groupingSetsList.getDefaultColumns().length;
        boolean useGroupingSet = groupingSetsList.useGroupingSets();
        Aggregation.Axis[] axes = groupingSetsList.getDefaultAxes();
        int segmentLength = groupingSetsList.getDefaultSegments().length;

        List<Integer> pos = new ArrayList<Integer>(arity);
        for (Object[] row : rows) {
            final SegmentDataset[] datasets;
            int groupingBitKeyIndex = arity + segmentLength;
            if (useGroupingSet) {
                BitKey groupingBitKey = (BitKey) row[groupingBitKeyIndex];
                datasets = groupingDataSetMap.get(groupingBitKey);
            } else {
                datasets = nonGroupingDataSets;
            }
            int k = 0;
            for (int j = 0; j < arity; j++) {
                Object o = row[j];
                if (useGroupingSet &&
                        isRollupNull(groupingSetsList, row, groupingBitKeyIndex, j)) {
                    continue;
                }
                Aggregation.Axis axis = axes[j];
                int offset = axis.getOffset(o);
                pos.add(offset);
                k *= axes[j].getKeys().length;
                k += offset;
            }

            if (sparse) {
                CellKey key = CellKey.Generator.newCellKey(toArray(pos));
                for (int j = 0; j < segmentLength; j++) {
                    final Object o = row[arity + j];
                    datasets[j].put(key, o);
                }
            } else {
                for (int j = 0; j < segmentLength; j++) {
                    final Object o = row[arity + j];
                    ((DenseSegmentDataset) datasets[j]).set(k, o);
                }
            }
            pos.clear();
        }
    }

    private boolean isRollupNull(
            GroupingSetsList groupingSetsList, Object[] row,
            int groupingBitKeyIndex, int j) {
        BitKey groupingBitKey = (BitKey) row[groupingBitKeyIndex];
        boolean isGroupingBitSet =
                groupingBitKey.get(groupingSetsList.findGroupingFunctionIndex(j));
        return row[j].equals(RolapUtil.sqlNullValue) && isGroupingBitSet;
    }

    private int[] toArray(List<Integer> pos) {
        int posArr[] = new int[pos.size()];
        for (int i = 0; i < posArr.length; i++) {
            posArr[i] = pos.get(i);
        }
        return posArr;
    }

    private boolean setAxisDataAndDecideSparseUse(
        SortedSet<Comparable<?>>[] axisValueSets,
        boolean[] axisContainsNull,
        GroupingSetsList groupingSetsList,
        List<Object[]> rows)
    {
        Aggregation.Axis[] axes = groupingSetsList.getDefaultAxes();
        RolapStar.Column[] allColumns = groupingSetsList.getDefaultColumns();
        // Figure out size of dense array, and allocate it, or use a sparse
        // array if appropriate.
        boolean sparse = false;
        int n = 1;
        for (int i = 0; i < axes.length; i++) {
            Aggregation.Axis axis = axes[i];
            SortedSet<Comparable<?>> valueSet = axisValueSets[i];
            int size = axis.loadKeys(valueSet, axisContainsNull[i]);
            setAxisDataToGroupableList(groupingSetsList, valueSet,
                axisContainsNull[i], allColumns[i]);
            int previous = n;
            n *= size;
            if ((n < previous) || (n < size)) {
                // Overflow has occurred.
                n = Integer.MAX_VALUE;
                sparse = true;
            }
        }
        return useSparse(sparse, n, rows);
    }

    boolean useSparse(boolean sparse, int n, List<Object[]> rows) {
        sparse = sparse || useSparse((double) n, (double) rows.size());
        return sparse;
    }

    private void setDataToSegments(
        GroupingSetsList groupingSetsList,
        SegmentDataset[] detailedDataSet,
        Map<BitKey, SegmentDataset[]> datasetsMap,
        RolapAggregationManager.PinSet pinnedSegments)
    {
        List<GroupingSet> groupingSets = groupingSetsList.getGroupingSets();
        boolean useGroupingSet = groupingSetsList.useGroupingSets();
        for (int i = 0; i < groupingSets.size(); i++) {
            Segment[] groupedSegments = groupingSets.get(i).getSegments();
            SegmentDataset[] dataSets = useGroupingSet ? datasetsMap
                .get(groupingSetsList.getRollupColumnsBitKeyList().get(i)) :
                detailedDataSet;
            for (int j = 0; j < groupedSegments.length; j++) {
                Segment groupedSegment = groupedSegments[j];
                groupedSegment
                    .setData(dataSets[j], pinnedSegments);
            }
        }
    }

    private void populateDataSetMapOnGroupingColumnsBitKeys(
        GroupingSetsList groupingSetsList, boolean sparse,
        Map<BitKey, SegmentDataset[]> datasetsMap)
    {
        List<GroupingSet> groupingSets = groupingSetsList.getGroupingSets();
        List<BitKey> groupingColumnsBitKeyList =
            groupingSetsList.getRollupColumnsBitKeyList();
        for (int i = 0; i < groupingSets.size(); i++) {
            GroupingSet groupingSet = groupingSets.get(i);
            SegmentDataset[] datasets = createDataSets(sparse,
                groupingSet.getSegments(), groupingSet.getAxes());
            datasetsMap.put(groupingColumnsBitKeyList.get(i), datasets);
        }
    }

    private int calcuateMaxDataSize(Aggregation.Axis[] axes) {
        int n = 1;
        for (Aggregation.Axis axis : axes) {
            n *= axis.getKeys().length;
        }
        return n;
    }

    private SegmentDataset[] createDataSets(boolean sparse,
        Segment[] segments, Aggregation.Axis[] axes)
    {
        int n = (sparse ? 0 : calcuateMaxDataSize(axes));
        SegmentDataset[] datasets;
        if (sparse) {
            datasets = new SparseSegmentDataset[segments.length];
            for (int i = 0; i < segments.length; i++) {
                datasets[i] = new SparseSegmentDataset(segments[i]);
            }
        } else {
            datasets = new DenseSegmentDataset[segments.length];
            for (int i = 0; i < segments.length; i++) {
                datasets[i] = new DenseSegmentDataset(
                    segments[i], new Object[n]);
            }
        }
        return datasets;
    }

    private void setAxisDataToGroupableList(
        GroupingSetsList groupingSetsList,
        SortedSet<Comparable<?>> valueSet, boolean axisContainsNull,
        RolapStar.Column column)
    {
        for (GroupingSet groupingSet : groupingSetsList.getRollupGroupingSets()) {
            RolapStar.Column[] columns = groupingSet.getColumns();
            for (int i = 0; i < columns.length; i++) {
                if (columns[i].equals(column)) {
                    groupingSet.getAxes()[i]
                        .loadKeys(valueSet, axisContainsNull);
                }
            }
        }
    }

    /**
     * Creates and executes a SQL statement to retrieve the set of cells
     * specified by a GroupingSetsList.
     *
     * <p>This method may be overridden in tests.
     *
     * @param groupingSetsList Grouping
     * @return An executed SQL statement, or null
     */
    SqlStatement createExecuteSql(
        GroupingSetsList groupingSetsList,
        List<StarPredicate> compoundPredicateList) {
        RolapStar star = groupingSetsList.getStar();
        String sql =
            AggregationManager.instance().generateSql(
                groupingSetsList, compoundPredicateList);
        return RolapUtil.executeQuery(
            star.getDataSource(), sql, "Segment.load",
            "Error while loading segment");
    }

    List<Object[]> processData(
        SqlStatement stmt,
        boolean[] axisContainsNull,
        SortedSet<Comparable<?>>[] axisValueSets,
        GroupingSetsList groupingSetsList) throws SQLException
    {
        Segment[] segments = groupingSetsList.getDefaultSegments();
        int measureCount = segments.length;
        List<Object[]> rawData = loadData(stmt, groupingSetsList);
        List<Object[]> processedRows = new ArrayList<Object[]>(rawData.size());

        int arity = axisValueSets.length;
        int groupingColumnStartIndex = arity + measureCount;
        for (Object[] row : rawData) {
            Object[] processedRow =
                groupingSetsList.useGroupingSets() ?
                    new Object[row.length - (groupingSetsList
                        .getRollupColumns().size()) + 1] :
                    new Object[row.length];
            // get the columns
            int columnIndex = 0;
            for (int axisIndex = 0; axisIndex < arity;
                 axisIndex++, columnIndex++) {
                Object o = row[columnIndex];
                if (o == null) {
                    o = RolapUtil.sqlNullValue;
                    if (!groupingSetsList.useGroupingSets() ||
                        !isAggregateNull(row, groupingColumnStartIndex,
                            groupingSetsList, axisIndex)) {
                        axisContainsNull[axisIndex] = true;
                    }
                } else {
                    axisValueSets[axisIndex].add(Aggregation.Axis.wrap(o));
                }
                processedRow[columnIndex] = o;
            }
            // get the measure
            for (int i = 0; i < measureCount; i++, columnIndex++) {
                Object o = row[columnIndex];
                if (o == null) {
                    o = Util.nullValue; // convert to placeholder
                } else if (segments[i].measure.getDatatype().isNumeric()) {
                    if (o instanceof Double) {
                        // nothing to do
                    } else if (o instanceof Number) {
                        o = ((Number) o).doubleValue();
                    } else if (o instanceof byte[]) {
                        // On MySQL 5.0 in German locale, values can come
                        // out as byte arrays. Don't know why. Bug 1594119.
                        o = Double.parseDouble(new String((byte[]) o));
                    } else {
                        o = Double.parseDouble(o.toString());
                    }
                }
                processedRow[columnIndex] = o;
            }
            if (groupingSetsList.useGroupingSets()) {
                processedRow[columnIndex] = getRollupBitKey(
                    groupingSetsList.getRollupColumns().size(), row,
                    columnIndex);
            }
            processedRows.add(processedRow);
        }
        return processedRows;
    }

    /**
     * Generates bit key representing roll up columns
     */
    BitKey getRollupBitKey(int arity, Object[] row, int k) {
        BitKey groupingBitKey = BitKey.Factory.makeBitKey(arity);
        for (int i = 0; i < arity; i++) {
            Object o = row[k + i];
            if (isOne(o)) {
                groupingBitKey.set(i);
            }
        }
        return groupingBitKey;
    }

    private static boolean isOne(Object o) {
        return ((Number) o).intValue() == 1;
    }

    private boolean isAggregateNull(
        Object[] row,
        int groupingColumnStartIndex,
        GroupingSetsList groupingSetsList,
        int axisIndex)
    {
        int groupingFunctionIndex =
            groupingSetsList.findGroupingFunctionIndex(axisIndex);
        if (groupingFunctionIndex == -1) {
            // Not a rollup column
            return false;
        }
        return isOne(row[groupingColumnStartIndex + groupingFunctionIndex]);
    }

    List<Object[]> loadData(
        SqlStatement stmt,
        GroupingSetsList groupingSetsList)
        throws SQLException
    {
        int arity = groupingSetsList.getDefaultColumns().length;
        int measureCount = groupingSetsList.getDefaultSegments().length;
        int groupingFunctionsCount = groupingSetsList.getRollupColumns().size();

        List<Object[]> rows = new ArrayList<Object[]>();
        ResultSet resultSet = stmt.getResultSet();
        while (resultSet.next()) {
            ++stmt.rowCount;
            Object[] row =
                groupingSetsList.useGroupingSets() ?
                    new Object[arity + measureCount +
                        groupingFunctionsCount] :
                    new Object[arity + measureCount];
            for (int i = 0; i < row.length; i++) {
                row[i] = resultSet.getObject(i + 1);
            }
            rows.add(row);
        }
        return rows;
    }

    List<RolapStar.Column[]> getGroupingColumnsList(
        RolapStar.Column[] detailedBatchColumns,
        List<GroupingSet> aggBatchDetails)
    {
        List<RolapStar.Column[]> groupingColumns =
            new ArrayList<RolapStar.Column[]>();
        if (aggBatchDetails.isEmpty()) {
            return groupingColumns;
        }
        groupingColumns.add(detailedBatchColumns);
        for (GroupingSet aggBatchDetail : aggBatchDetails) {
            groupingColumns.add(aggBatchDetail.getSegments()[0]
                .aggregation.getColumns());

        }
        return groupingColumns;
    }

    SortedSet<Comparable<?>>[] getDistinctValueWorkspace(int arity) {
        // Workspace to build up lists of distinct values for each axis.
        SortedSet<Comparable<?>>[] axisValueSets = new SortedSet[arity];
        for (int i = 0; i < axisValueSets.length; i++) {

            if (Util.PreJdk15) {
                // Work around the fact that Boolean is not Comparable until JDK
                // 1.5.
                assert !(Comparable.class.isAssignableFrom(Boolean.class));
                final SortedSet set =
                    new TreeSet<Comparable<Object>>(
                        new Comparator<Object>() {
                            public int compare(Object o1, Object o2) {
                                if (o1 instanceof Boolean) {
                                    boolean b1 = (Boolean) o1;
                                    if (o2 instanceof Boolean) {
                                        boolean b2 = (Boolean) o2;
                                        return (b1 == b2 ? 0 :
                                            (b1 ? 1 : -1));
                                    } else {
                                        return -1;
                                    }
                                } else {
                                    return ((Comparable) o1).compareTo(o2);
                                }
                            }
                        }
                   );
                axisValueSets[i] = set;
            } else {
                assert Comparable.class.isAssignableFrom(Boolean.class);
                axisValueSets[i] = new TreeSet<Comparable<?>>();
            }
        }
        return axisValueSets;
    }

    /**
     * Decides whether to use a sparse representation for this segment, using
     * the formula described
     * {@link mondrian.olap.MondrianProperties#SparseSegmentCountThreshold here}.
     *
     * @param possibleCount Number of values in the space.
     * @param actualCount   Actual number of values.
     * @return Whether to use a sparse representation.
     */
    private static boolean useSparse(
        final double possibleCount, final double actualCount)
    {
        final MondrianProperties properties = MondrianProperties.instance();
        double densityThreshold =
            properties.SparseSegmentDensityThreshold.get();
        if (densityThreshold < 0) {
            densityThreshold = 0;
        }
        if (densityThreshold > 1) {
            densityThreshold = 1;
        }
        int countThreshold = properties.SparseSegmentCountThreshold.get();
        if (countThreshold < 0) {
            countThreshold = 0;
        }
        boolean sparse =
            (possibleCount - countThreshold) * densityThreshold >
                actualCount;
        if (possibleCount < countThreshold) {
            assert !sparse :
                "Should never use sparse if count is less " +
                    "than threshold, possibleCount=" + possibleCount +
                    ", actualCount=" + actualCount +
                    ", countThreshold=" + countThreshold +
                    ", densityThreshold=" + densityThreshold;
        }
        if (possibleCount == actualCount) {
            assert !sparse :
                "Should never use sparse if result is 100% dense: " +
                    "possibleCount=" + possibleCount +
                    ", actualCount=" + actualCount +
                    ", countThreshold=" + countThreshold +
                    ", densityThreshold=" + densityThreshold;
        }
        return sparse;
    }
}

// End SegmentLoader.java
