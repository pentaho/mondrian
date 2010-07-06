/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2002-2010 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap.agg;

import mondrian.rolap.*;
import mondrian.olap.*;

import java.sql.*;
import java.util.*;

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

    private static final Comparator<Object> BOOLEAN_COMPARATOR;

    static {
        if (Util.PreJdk15) {
            // Work around the fact that Boolean is not Comparable until JDK
            // 1.5.
            assert !(Comparable.class.isAssignableFrom(Boolean.class));
            BOOLEAN_COMPARATOR =
                new Comparator<Object>() {
                    public int compare(Object o1, Object o2) {
                        if (o1 instanceof Boolean) {
                            boolean b1 = (Boolean) o1;
                            if (o2 instanceof Boolean) {
                                boolean b2 = (Boolean) o2;
                                return b1 == b2
                                    ? 0
                                    : (b1 ? 1 : -1);
                            } else {
                                return -1;
                            }
                        } else {
                            return ((Comparable) o1).compareTo(o2);
                        }
                    }
                };
        } else {
            assert Comparable.class.isAssignableFrom(Boolean.class);
            BOOLEAN_COMPARATOR = null;
        }
    }

    /**
     * Creates a SegmentLoader.
     */
    public SegmentLoader() {
    }

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

            RowList rows =
                processData(
                    stmt,
                    axisContainsNull,
                    axisValueSets,
                    groupingSetsList);

            boolean sparse =
                setAxisDataAndDecideSparseUse(
                    axisValueSets,
                    axisContainsNull,
                    groupingSetsList,
                    rows);

            final Map<BitKey, GroupingSetsList.Cohort> groupingDataSetsMap =
                createDataSetsForGroupingSets(
                    groupingSetsList,
                    sparse,
                    rows.getTypes().subList(
                        arity, rows.getTypes().size()));

            loadDataToDataSets(
                groupingSetsList, rows, groupingDataSetsMap);

            setDataToSegments(
                groupingSetsList, groupingDataSetsMap, pinnedSegments);
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
        GroupingSetsList groupingSetsList,
        RowList rows,
        Map<BitKey, GroupingSetsList.Cohort> groupingDataSetMap)
    {
        int arity = groupingSetsList.getDefaultColumns().length;
        Aggregation.Axis[] axes = groupingSetsList.getDefaultAxes();
        int segmentLength = groupingSetsList.getDefaultSegments().size();

        final List<SqlStatement.Type> types = rows.getTypes();
        final boolean useGroupingSet = groupingSetsList.useGroupingSets();
        for (rows.first(); rows.next();) {
            final BitKey groupingBitKey;
            final GroupingSetsList.Cohort cohort;
            if (useGroupingSet) {
                groupingBitKey =
                    (BitKey) rows.getObject(
                        groupingSetsList.getGroupingBitKeyIndex());
                cohort = groupingDataSetMap.get(groupingBitKey);
            } else {
                groupingBitKey = null;
                cohort = groupingDataSetMap.get(BitKey.EMPTY);
            }
            final int[] pos = cohort.pos;
            for (int j = 0, k = 0; j < arity; j++) {
                final SqlStatement.Type type = types.get(j);
                switch (type) {
                // TODO: different treatment for INT, DOUBLE
                case OBJECT:
                case INT:
                case DOUBLE:
                    Object o = rows.getObject(j);
                    if (useGroupingSet
                        && (o == null || o == RolapUtil.sqlNullValue)
                        && groupingBitKey.get(
                            groupingSetsList.findGroupingFunctionIndex(j)))
                    {
                        continue;
                    }
                    Aggregation.Axis axis = axes[j];
                    if (o == null) {
                        o = RolapUtil.sqlNullValue;
                    }
                    int offset = axis.getOffset(o);
                    pos[k++] = offset;
                    break;
                default:
                    throw Util.unexpected(type);
                }
            }

            for (int j = 0; j < segmentLength; j++) {
                cohort.segmentDatasetList.get(j).populateFrom(
                    pos, rows, arity + j);
            }
        }
    }

    private boolean setAxisDataAndDecideSparseUse(
        SortedSet<Comparable<?>>[] axisValueSets,
        boolean[] axisContainsNull,
        GroupingSetsList groupingSetsList,
        RowList rows)
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
            setAxisDataToGroupableList(
                groupingSetsList,
                valueSet,
                axisContainsNull[i],
                allColumns[i]);
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

    boolean useSparse(boolean sparse, int n, RowList rows) {
        sparse = sparse || useSparse((double) n, (double) rows.size());
        return sparse;
    }

    private void setDataToSegments(
        GroupingSetsList groupingSetsList,
        Map<BitKey, GroupingSetsList.Cohort> datasetsMap,
        RolapAggregationManager.PinSet pinnedSegments)
    {
        List<GroupingSet> groupingSets = groupingSetsList.getGroupingSets();
        for (int i = 0; i < groupingSets.size(); i++) {
            List<Segment> groupedSegments = groupingSets.get(i).getSegments();
            GroupingSetsList.Cohort cohort =
                datasetsMap.get(
                    groupingSetsList.getRollupColumnsBitKeyList().get(i));
            for (int j = 0; j < groupedSegments.size(); j++) {
                Segment groupedSegment = groupedSegments.get(j);
                final SegmentDataset segmentDataset =
                    cohort.segmentDatasetList.get(j);
                groupedSegment.setData(segmentDataset, pinnedSegments);
            }
        }
    }

    private Map<BitKey, GroupingSetsList.Cohort> createDataSetsForGroupingSets(
        GroupingSetsList groupingSetsList,
        boolean sparse,
        List<SqlStatement.Type> types)
    {
        if (!groupingSetsList.useGroupingSets()) {
            final GroupingSetsList.Cohort datasets = createDataSets(
                sparse,
                groupingSetsList.getDefaultSegments(),
                groupingSetsList.getDefaultAxes(),
                types);
            return Collections.singletonMap(BitKey.EMPTY, datasets);
        }
        Map<BitKey, GroupingSetsList.Cohort> datasetsMap =
            new HashMap<BitKey, GroupingSetsList.Cohort>();
        List<GroupingSet> groupingSets = groupingSetsList.getGroupingSets();
        List<BitKey> groupingColumnsBitKeyList =
            groupingSetsList.getRollupColumnsBitKeyList();
        for (int i = 0; i < groupingSets.size(); i++) {
            GroupingSet groupingSet = groupingSets.get(i);
            GroupingSetsList.Cohort cohort =
                createDataSets(
                    sparse,
                    groupingSet.getSegments(),
                    groupingSet.getAxes(),
                    types);
            datasetsMap.put(groupingColumnsBitKeyList.get(i), cohort);
        }
        return datasetsMap;
    }

    private int calculateMaxDataSize(Aggregation.Axis[] axes) {
        int n = 1;
        for (Aggregation.Axis axis : axes) {
            n *= axis.getKeys().length;
        }
        return n;
    }

    private GroupingSetsList.Cohort createDataSets(
        boolean sparse,
        List<Segment> segments,
        Aggregation.Axis[] axes,
        List<SqlStatement.Type> types)
    {
        final List<SegmentDataset> datasets =
            new ArrayList<SegmentDataset>(segments.size());
        final int n;
        if (sparse) {
            n = 0;
        } else {
            n = calculateMaxDataSize(axes);
        }
        for (int i = 0; i < segments.size(); i++) {
            final Segment segment = segments.get(i);
            datasets.add(segment.createDataset(sparse, types.get(i), n));
        }
        return new GroupingSetsList.Cohort(datasets, axes.length);
    }

    private void setAxisDataToGroupableList(
        GroupingSetsList groupingSetsList,
        SortedSet<Comparable<?>> valueSet,
        boolean axisContainsNull,
        RolapStar.Column column)
    {
        for (GroupingSet groupingSet
            : groupingSetsList.getRollupGroupingSets())
        {
            RolapStar.Column[] columns = groupingSet.getColumns();
            for (int i = 0; i < columns.length; i++) {
                if (columns[i].equals(column)) {
                    groupingSet.getAxes()[i].loadKeys(
                        valueSet, axisContainsNull);
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
        List<StarPredicate> compoundPredicateList)
    {
        RolapStar star = groupingSetsList.getStar();
        String sql =
            AggregationManager.instance().generateSql(
                groupingSetsList, compoundPredicateList);
        return RolapUtil.executeQuery(
            star.getDataSource(), sql, "Segment.load",
            "Error while loading segment");
    }

    RowList processData(
        SqlStatement stmt,
        final boolean[] axisContainsNull,
        final SortedSet<Comparable<?>>[] axisValueSets,
        final GroupingSetsList groupingSetsList) throws SQLException
    {
        List<Segment> segments = groupingSetsList.getDefaultSegments();
        int measureCount = segments.size();
        ResultSet rawRows = loadData(stmt, groupingSetsList);
        final List<SqlStatement.Type> types =
            stmt == null
                ? Collections.nCopies(
                rawRows.getMetaData().getColumnCount(),
                SqlStatement.Type.OBJECT)
                : stmt.guessTypes();
        int arity = axisValueSets.length;
        final int groupingColumnStartIndex = arity + measureCount;

        // If we're using grouping sets, the SQL query will have a number of
        // indicator columns, and we roll these into a single BitSet column in
        // the processed data set.
        final List<SqlStatement.Type> processedTypes;
        if (groupingSetsList.useGroupingSets()) {
            processedTypes =
                new ArrayList<SqlStatement.Type>(
                    types.subList(0, groupingColumnStartIndex));
            processedTypes.add(SqlStatement.Type.OBJECT);
        } else {
            processedTypes = types;
        }
        final RowList processedRows = new RowList(processedTypes, 100);

        while (rawRows.next()) {
            ++stmt.rowCount;
            processedRows.createRow();
            // get the columns
            int columnIndex = 0;
            for (int axisIndex = 0; axisIndex < arity;
                 axisIndex++, columnIndex++)
            {
                final SqlStatement.Type type = types.get(columnIndex);
                switch (type) {
                case OBJECT:
                    Object o = rawRows.getObject(columnIndex + 1);
                    if (o == null) {
                        o = RolapUtil.sqlNullValue;
                        if (!groupingSetsList.useGroupingSets()
                            || !isAggregateNull(
                            rawRows, groupingColumnStartIndex,
                            groupingSetsList,
                            axisIndex))
                        {
                            axisContainsNull[axisIndex] = true;
                        }
                    } else {
                        axisValueSets[axisIndex].add(Aggregation.Axis.wrap(o));
                    }
                    processedRows.setObject(columnIndex, o);
                    break;
                case INT:
                    final int intValue = rawRows.getInt(columnIndex + 1);
                    if (intValue == 0 && rawRows.wasNull()) {
                        if (!groupingSetsList.useGroupingSets()
                            || !isAggregateNull(
                            rawRows, groupingColumnStartIndex,
                            groupingSetsList,
                            axisIndex))
                        {
                            axisContainsNull[axisIndex] = true;
                        }
                        processedRows.setNull(columnIndex, true);
                    } else {
                        axisValueSets[axisIndex].add(intValue);
                        processedRows.setInt(columnIndex, intValue);
                    }
                    break;
                case DOUBLE:
                    final double doubleValue =
                        rawRows.getDouble(columnIndex + 1);
                    if (doubleValue == 0 && rawRows.wasNull()) {
                        if (!groupingSetsList.useGroupingSets()
                            || !isAggregateNull(
                            rawRows, groupingColumnStartIndex,
                            groupingSetsList,
                            axisIndex))
                        {
                            axisContainsNull[axisIndex] = true;
                        }
                    }
                    axisValueSets[axisIndex].add(doubleValue);
                    processedRows.setDouble(columnIndex, doubleValue);
                    break;
                default:
                    throw Util.unexpected(type);
                }
            }

            // pre-compute which measures are numeric
            final boolean[] numeric = new boolean[measureCount];
            int k = 0;
            for (Segment segment : segments) {
                numeric[k++] = segment.measure.getDatatype().isNumeric();
            }

            // get the measure
            for (int i = 0; i < measureCount; i++, columnIndex++) {
                final SqlStatement.Type type =
                    types.get(columnIndex);
                switch (type) {
                case OBJECT:
                    Object o = rawRows.getObject(columnIndex + 1);
                    if (o == null) {
                        o = Util.nullValue; // convert to placeholder
                    } else if (numeric[i]) {
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
                    processedRows.setObject(columnIndex, o);
                    break;
                case INT:
                    final int intValue = rawRows.getInt(columnIndex + 1);
                    processedRows.setInt(columnIndex, intValue);
                    if (intValue == 0 && rawRows.wasNull()) {
                        processedRows.setNull(columnIndex, true);
                    }
                    break;
                case DOUBLE:
                    final double doubleValue =
                        rawRows.getDouble(columnIndex + 1);
                    processedRows.setDouble(columnIndex, doubleValue);
                    if (doubleValue == 0 && rawRows.wasNull()) {
                        processedRows.setNull(columnIndex, true);
                    }
                    break;
                default:
                    throw Util.unexpected(type);
                }
            }

            if (groupingSetsList.useGroupingSets()) {
                processedRows.setObject(
                    columnIndex,
                    getRollupBitKey(
                        groupingSetsList.getRollupColumns().size(),
                        rawRows, columnIndex));
            }
        }
        return processedRows;
    }

    /**
     * Generates bit key representing roll up columns
     */
    BitKey getRollupBitKey(int arity, ResultSet rowList, int k)
        throws SQLException
    {
        BitKey groupingBitKey = BitKey.Factory.makeBitKey(arity);
        for (int i = 0; i < arity; i++) {
            int o = rowList.getInt(k + i + 1);
            if (o == 1) {
                groupingBitKey.set(i);
            }
        }
        return groupingBitKey;
    }

    private boolean isAggregateNull(
        ResultSet rowList,
        int groupingColumnStartIndex,
        GroupingSetsList groupingSetsList,
        int axisIndex) throws SQLException
    {
        int groupingFunctionIndex =
            groupingSetsList.findGroupingFunctionIndex(axisIndex);
        if (groupingFunctionIndex == -1) {
            // Not a rollup column
            return false;
        }
        return rowList.getInt(
            groupingColumnStartIndex + groupingFunctionIndex + 1) == 1;
    }

    ResultSet loadData(
        SqlStatement stmt,
        GroupingSetsList groupingSetsList)
        throws SQLException
    {
        int arity = groupingSetsList.getDefaultColumns().length;
        int measureCount = groupingSetsList.getDefaultSegments().size();
        int groupingFunctionsCount = groupingSetsList.getRollupColumns().size();
        List<SqlStatement.Type> types = stmt.guessTypes();
        assert arity + measureCount + groupingFunctionsCount == types.size();

        return stmt.getResultSet();
    }

    RowList loadData2(
        SqlStatement stmt,
        GroupingSetsList groupingSetsList)
        throws SQLException
    {
        int arity = groupingSetsList.getDefaultColumns().length;
        int measureCount = groupingSetsList.getDefaultSegments().size();
        int groupingFunctionsCount = groupingSetsList.getRollupColumns().size();
        List<SqlStatement.Type> types = stmt.guessTypes();
        assert arity + measureCount + groupingFunctionsCount == types.size();

        final RowList rows = new RowList(types, 100);
        final ResultSet resultSet = stmt.getResultSet();
        while (resultSet.next()) {
            ++stmt.rowCount;
            rows.createRow(resultSet);
        }
        return rows;
    }

    SortedSet<Comparable<?>>[] getDistinctValueWorkspace(int arity) {
        // Workspace to build up lists of distinct values for each axis.
        SortedSet<Comparable<?>>[] axisValueSets = new SortedSet[arity];
        for (int i = 0; i < axisValueSets.length; i++) {
            axisValueSets[i] =
                Util.PreJdk15
                    ? new TreeSet<Comparable<?>>(BOOLEAN_COMPARATOR)
                    : new TreeSet<Comparable<?>>();
        }
        return axisValueSets;
    }

    /**
     * Decides whether to use a sparse representation for this segment, using
     * the formula described
     * {@link mondrian.olap.MondrianProperties#SparseSegmentCountThreshold
     * here}.
     *
     * @param possibleCount Number of values in the space.
     * @param actualCount   Actual number of values.
     * @return Whether to use a sparse representation.
     */
    private static boolean useSparse(
        final double possibleCount,
        final double actualCount)
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
            assert !sparse
                : "Should never use sparse if count is less "
                + "than threshold, possibleCount=" + possibleCount
                + ", actualCount=" + actualCount
                + ", countThreshold=" + countThreshold
                + ", densityThreshold=" + densityThreshold;
        }
        if (possibleCount == actualCount) {
            assert !sparse
                : "Should never use sparse if result is 100% dense: "
                + "possibleCount=" + possibleCount
                + ", actualCount=" + actualCount
                + ", countThreshold=" + countThreshold
                + ", densityThreshold=" + densityThreshold;
        }
        return sparse;
    }

    /**
     * Collection of rows, each with a set of columns of type Object, double, or
     * int. Native types are not boxed.
     */
    static protected class RowList {
        private final Column[] columns;
        private int rowCount = 0;
        private int capacity = 0;
        private int currentRow = -1;

        /**
         * Creates a RowList.
         *
         * @param types Column types
         */
        RowList(List<SqlStatement.Type> types) {
            this(types, 100);
        }

        /**
         * Creates a RowList with a specified initial capacity.
         *
         * @param types Column types
         * @param capacity Initial capacity
         */
        RowList(List<SqlStatement.Type> types, int capacity) {
            this.columns = new Column[types.size()];
            this.capacity = capacity;
            for (int i = 0; i < columns.length; i++) {
                columns[i] = Column.forType(i, types.get(i), capacity);
            }
        }

        void createRow() {
            currentRow = rowCount++;
            if (rowCount > capacity) {
                capacity *= 3;
                for (Column column : columns) {
                    column.resize(capacity);
                }
            }
        }

        void setObject(int column, Object value) {
            columns[column].setObject(currentRow, value);
        }

        void setDouble(int column, double value) {
            columns[column].setDouble(currentRow, value);
        }

        void setInt(int column, int value) {
            columns[column].setInt(currentRow, value);
        }

        public int size() {
            return rowCount;
        }

        public void createRow(ResultSet resultSet) throws SQLException {
            createRow();
            for (Column column : columns) {
                column.populateFrom(currentRow, resultSet);
            }
        }

        public List<SqlStatement.Type> getTypes() {
            return new AbstractList<SqlStatement.Type>() {
                public SqlStatement.Type get(int index) {
                    return columns[index].type;
                }

                public int size() {
                    return columns.length;
                }
            };
        }

        /**
         * Moves to before the first row.
         */
        public void first() {
            currentRow = -1;
        }

        /**
         * Moves to after the last row.
         */
        public void last() {
            currentRow = rowCount;
        }

        /**
         * Moves forward one row, or returns false if at the last row.
         *
         * @return whether moved forward
         */
        public boolean next() {
            if (currentRow < rowCount - 1) {
                ++currentRow;
                return true;
            }
            return false;
        }

        /**
         * Moves backward one row, or returns false if at the first row.
         *
         * @return whether moved backward
         */
        public boolean previous() {
            if (currentRow > 0) {
                --currentRow;
                return true;
            }
            return false;
        }

        /**
         * Returns the object in the given column of the current row.
         *
         * @param columnIndex Column index
         * @return Value of the column
         */
        public Object getObject(int columnIndex) {
            return columns[columnIndex].getObject(currentRow);
        }

        public int getInt(int columnIndex) {
            return columns[columnIndex].getInt(currentRow);
        }

        public double getDouble(int columnIndex) {
            return columns[columnIndex].getDouble(currentRow);
        }

        public boolean isNull(int columnIndex) {
            return columns[columnIndex].isNull(currentRow);
        }

        public void setNull(int columnIndex, boolean b) {
            columns[columnIndex].setNull(currentRow, b);
        }

        static abstract class Column {
            final int ordinal;
            final SqlStatement.Type type;

            protected Column(int ordinal, SqlStatement.Type type) {
                this.ordinal = ordinal;
                this.type = type;
            }

            static Column forType(
                int ordinal,
                SqlStatement.Type type,
                int capacity)
            {
                switch (type) {
                case OBJECT:
                    return new ObjectColumn(ordinal, type, capacity);
                case INT:
                    return new IntColumn(ordinal, type, capacity);
                case DOUBLE:
                    return new DoubleColumn(ordinal, type, capacity);
                default:
                    throw Util.unexpected(type);
                }
            }

            public abstract void resize(int newSize);

            public void setObject(int row, Object value) {
                throw new UnsupportedOperationException();
            }

            public void setDouble(int row, double value) {
                throw new UnsupportedOperationException();
            }

            public void setInt(int row, int value) {
                throw new UnsupportedOperationException();
            }

            public void setNull(int row, boolean b) {
                throw new UnsupportedOperationException();
            }

            public abstract void populateFrom(int row, ResultSet resultSet)
                throws SQLException;

            public Object getObject(int row) {
                throw new UnsupportedOperationException();
            }

            public int getInt(int row) {
                throw new UnsupportedOperationException();
            }

            public double getDouble(int row) {
                throw new UnsupportedOperationException();
            }

            protected abstract int getCapacity();

            public abstract boolean isNull(int row);
        }

        static class ObjectColumn extends Column {
            private Object[] objects;

            ObjectColumn(int ordinal, SqlStatement.Type type, int size) {
                super(ordinal, type);
                objects = new Object[size];
            }

            protected int getCapacity() {
                return objects.length;
            }

            public boolean isNull(int row) {
                return objects[row] == null;
            }

            public void resize(int newSize) {
                objects = Util.copyOf(objects, newSize);
            }

            public void populateFrom(int row, ResultSet resultSet)
                throws SQLException
            {
                objects[row] = resultSet.getObject(ordinal + 1);
            }

            public void setObject(int row, Object value) {
                objects[row] = value;
            }

            public Object getObject(int row) {
                return objects[row];
            }
        }

        static abstract class NativeColumn extends Column {
            protected BitSet nullIndicators;

            NativeColumn(int ordinal, SqlStatement.Type type) {
                super(ordinal, type);
            }

            public void setNull(int row, boolean b) {
                getNullIndicators().set(row, b);
            }

            protected BitSet getNullIndicators() {
                if (nullIndicators == null) {
                    nullIndicators = new BitSet(getCapacity());
                }
                return nullIndicators;
            }
        }

        static class IntColumn extends NativeColumn {
            private int[] ints;

            IntColumn(int ordinal, SqlStatement.Type type, int size) {
                super(ordinal, type);
                ints = new int[size];
            }

            public void resize(int newSize) {
                ints = Util.copyOf(ints, newSize);
            }

            public void populateFrom(int row, ResultSet resultSet)
                throws SQLException
            {
                int i = ints[row] = resultSet.getInt(ordinal + 1);
                if (i == 0) {
                    getNullIndicators().set(row, resultSet.wasNull());
                }
            }

            public void setInt(int row, int value) {
                ints[row] = value;
            }

            public int getInt(int row) {
                return ints[row];
            }

            public boolean isNull(int row) {
                return ints[row] == 0
                   && nullIndicators != null
                   && nullIndicators.get(row);
            }

            protected int getCapacity() {
                return ints.length;
            }

            public Integer getObject(int row) {
                return isNull(row) ? null : ints[row];
            }
        }

        static class DoubleColumn extends NativeColumn {
            private double[] doubles;

            DoubleColumn(int ordinal, SqlStatement.Type type, int size) {
                super(ordinal, type);
                doubles = new double[size];
            }

            public void resize(int newSize) {
                doubles = Util.copyOf(doubles, newSize);
            }

            public void populateFrom(int row, ResultSet resultSet)
                throws SQLException
            {
                double d = doubles[row] = resultSet.getDouble(ordinal + 1);
                if (d == 0d) {
                    getNullIndicators().set(row, resultSet.wasNull());
                }
            }

            public void setDouble(int row, double value) {
                doubles[row] = value;
            }

            public double getDouble(int row) {
                return doubles[row];
            }

            protected int getCapacity() {
                return doubles.length;
            }

            public boolean isNull(int row) {
                return doubles[row] == 0d
                   && nullIndicators != null
                   && nullIndicators.get(row);
            }

            public Double getObject(int row) {
                return isNull(row) ? null : doubles[row];
            }
        }

        public interface Handler {
        }
    }
}

// End SegmentLoader.java
