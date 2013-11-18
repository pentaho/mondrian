/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 Julian Hyde
// Copyright (C) 2005-2013 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap.agg;

import mondrian.olap.*;
import mondrian.rolap.*;
import mondrian.server.*;
import mondrian.server.Statement;
import mondrian.spi.Dialect;
import mondrian.test.*;
import mondrian.util.DelegatingInvocationHandler;

import java.io.PrintWriter;
import java.lang.reflect.Proxy;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * <p>Test for <code>SegmentLoader</code></p>
 *
 * @author Thiyagu
 * @since 06-Jun-2007
 */
public class SegmentLoaderTest extends BatchTestCase {

    private Execution execution;
    private Locus locus;
    private SegmentCacheManager cacheMgr;
    private Statement statement;

    protected void setUp() throws Exception {
        super.setUp();
        cacheMgr =
            ((RolapConnection) getConnection())
                .getServer().getAggregationManager().cacheMgr;
        statement = ((RolapConnection) getConnection()).getInternalStatement();
        execution = new Execution(statement, 1000);
        locus = new Locus(execution, null, null);
        cacheMgr = execution.getMondrianStatement().getMondrianConnection()
            .getServer().getAggregationManager().cacheMgr;

        Locus.push(locus);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        Locus.pop(locus);
        try {
            statement.cancel();
        } catch (Exception e) {
            // ignore.
        }
        try {
            execution.cancel();
        } catch (Exception e) {
            // ignore.
        }
        statement = null;
        execution = null;
        locus = null;
        cacheMgr = null;
    }

    public void testRollup() throws Exception {
        for (boolean rollup : new Boolean[] {true, false}) {
            PrintWriter pw = new PrintWriter(System.out);
            getConnection().getCacheControl(pw).flushSchemaCache();
            pw.flush();
            propSaver.set(propSaver.props.DisableCaching, true);
            propSaver.set(propSaver.props.EnableInMemoryRollup, rollup);
            final String queryOracle =
                "select \"time_by_day\".\"the_year\" as \"c0\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" from \"time_by_day\" \"time_by_day\", \"sales_fact_1997\" \"sales_fact_1997\" where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" group by \"time_by_day\".\"the_year\"";
            final String queryMySQL =
                "select `time_by_day`.`the_year` as `c0`, sum(`sales_fact_1997`.`unit_sales`) as `m0` from `time_by_day` as `time_by_day`, `sales_fact_1997` as `sales_fact_1997` where `sales_fact_1997`.`time_id` = `time_by_day`.`time_id` group by `time_by_day`.`the_year`";
            executeQuery(
                "select {[Store].[Store Country].Members} on rows, {[Time].[Time].[Year].Members} on columns from [Sales]");
            assertQuerySqlOrNot(
                getTestContext(),
                "select {[Time].[Time].[Year].Members} on columns from [Sales]",
                new SqlPattern[] {
                    new SqlPattern(
                        Dialect.DatabaseProduct.ORACLE,
                        queryOracle,
                        queryOracle.length()),
                    new SqlPattern(
                        Dialect.DatabaseProduct.MYSQL,
                        queryMySQL,
                        queryMySQL.length())
                },
                rollup,
                false,
                false);
        }
    }

    public void testLoadWithMockResultsForLoadingSummaryAndDetailedSegments()
        throws ExecutionException, InterruptedException
    {
        GroupingSet groupableSetsInfo = getGroupingSetRollupOnGender();

        GroupingSet groupingSetsInfo = getDefaultGroupingSet();
        ArrayList<GroupingSet> groupingSets =
            new ArrayList<GroupingSet>();
        groupingSets.add(groupingSetsInfo);
        groupingSets.add(groupableSetsInfo);
        SegmentLoader loader = new SegmentLoader(cacheMgr) {
            SqlStatement createExecuteSql(
                int cellRequestCount,
                final GroupingSetsList groupingSetsList,
                List<StarPredicate> compoundPredicateList)
            {
                return new MockSqlStatement(
                    cellRequestCount,
                    groupingSetsList,
                    getData(true));
            }
        };
        final List<Future<Map<Segment, SegmentWithData>>> segmentFutures =
            new ArrayList<Future<Map<Segment, SegmentWithData>>>();
        loader.load(0, groupingSets, null, segmentFutures);
        for (Future<?> future : segmentFutures) {
            Util.safeGet(future, "");
        }
        SegmentAxis[] axes = groupingSetsInfo.getAxes();
        verifyYearAxis(axes[0]);
        verifyProductFamilyAxis(axes[1]);
        verifyProductDepartmentAxis(axes[2]);
        verifyGenderAxis(axes[3]);
        verifyUnitSalesDetailed(
            getFor(
                segmentFutures,
                groupingSets.get(0).getSegments().get(0)));

        axes = groupingSets.get(0).getAxes();
        verifyYearAxis(axes[0]);
        verifyProductFamilyAxis(axes[1]);
        verifyProductDepartmentAxis(axes[2]);
        verifyUnitSalesAggregate(
            getFor(
                segmentFutures,
                groupingSets.get(1).getSegments().get(0)));
    }

    private ResultSet toResultSet(final List<Object[]> list) {
        final MyDelegatingInvocationHandler handler =
            new MyDelegatingInvocationHandler(list);
        Object o =
            Proxy.newProxyInstance(
                null,
                new Class[] {ResultSet.class, ResultSetMetaData.class},
                handler);
        handler.resultSetMetaData = (ResultSetMetaData) o;
        return (ResultSet) o;
    }

    private SegmentLoader.RowList toRowList2(List<Object[]> list) {
        final SegmentLoader.RowList rowList =
            new SegmentLoader.RowList(
                Collections.nCopies(
                    list.get(0).length,
                    SqlStatement.Type.OBJECT));
        for (Object[] objects : list) {
            rowList.createRow();
            for (int i = 0; i < objects.length; i++) {
                Object object = objects[i];
                rowList.setObject(i, object);
            }
        }
        return rowList;
    }

    /**
     * Tests load with mock results for loading summary and detailed
     * segments with null in rollup column.
     */
    public void testLoadWithWithNullInRollupColumn()
        throws ExecutionException, InterruptedException
    {
        GroupingSet groupableSetsInfo = getGroupingSetRollupOnGender();

        GroupingSet groupingSetsInfo = getDefaultGroupingSet();
        ArrayList<GroupingSet> groupingSets =
            new ArrayList<GroupingSet>();
        groupingSets.add(groupingSetsInfo);
        groupingSets.add(groupableSetsInfo);
        SegmentLoader loader = new SegmentLoader(cacheMgr) {
            SqlStatement createExecuteSql(
                int cellRequestCount,
                GroupingSetsList groupingSetsList,
                List<StarPredicate> compoundPredicateList)
            {
                return new MockSqlStatement(
                    cellRequestCount,
                    groupingSetsList,
                    getDataWithNullInRollupColumn(true));
            }
        };
        final List<Future<Map<Segment, SegmentWithData>>> segmentFutures =
            new ArrayList<Future<Map<Segment, SegmentWithData>>>();
        loader.load(0, groupingSets, null, segmentFutures);
        SegmentWithData detailedSegment =
            getFor(
                segmentFutures,
                groupingSets.get(0).getSegments().get(0));
        assertEquals(3, detailedSegment.getCellCount());
    }

    public void
        testLoadWithMockResultsForLoadingSummaryAndDetailedSegmentsUsingSparse()
        throws ExecutionException, InterruptedException
    {
        GroupingSet groupableSetsInfo = getGroupingSetRollupOnGender();

        GroupingSet groupingSetsInfo = getDefaultGroupingSet();
        ArrayList<GroupingSet> groupingSets =
            new ArrayList<GroupingSet>();
        groupingSets.add(groupingSetsInfo);
        groupingSets.add(groupableSetsInfo);
        SegmentLoader loader = new SegmentLoader(cacheMgr) {
            SqlStatement createExecuteSql(
                int cellRequestCount,
                GroupingSetsList groupingSetsList,
                List<StarPredicate> compoundPredicateList)
            {
                return new MockSqlStatement(
                    cellRequestCount,
                    groupingSetsList,
                    getData(true));
            }

            public boolean useSparse(boolean sparse, int n, RowList rows) {
                return true;
            }
        };
        final List<Future<Map<Segment, SegmentWithData>>> segmentFutures =
            new ArrayList<Future<Map<Segment, SegmentWithData>>>();
        loader.load(0, groupingSets, null, segmentFutures);
        for (Future<?> future : segmentFutures) {
            Util.safeGet(future, "");
        }
        SegmentAxis[] axes = groupingSetsInfo.getAxes();
        verifyYearAxis(axes[0]);
        verifyProductFamilyAxis(axes[1]);
        verifyProductDepartmentAxis(axes[2]);
        verifyGenderAxis(axes[3]);
        verifyUnitSalesDetailedForSparse(
            getFor(
                segmentFutures,
                groupingSets.get(0).getSegments().get(0)));

        axes = groupingSets.get(0).getAxes();
        verifyYearAxis(axes[0]);
        verifyProductFamilyAxis(axes[1]);
        verifyProductDepartmentAxis(axes[2]);
        verifyUnitSalesAggregateForSparse(
            getFor(
                segmentFutures,
                groupingSets.get(1).getSegments().get(0)));
    }

    private SegmentWithData getFor(
        List<Future<Map<Segment, SegmentWithData>>> mapFutures,
        Segment segment)
        throws ExecutionException, InterruptedException
    {
        for (Future<Map<Segment, SegmentWithData>> mapFuture : mapFutures) {
            final Map<Segment, SegmentWithData> map = mapFuture.get();
            if (map.containsKey(segment)) {
                return map.get(segment);
            }
        }
        return null;
    }

    private List<Object[]> trim(final int length, final List<Object[]> data) {
        return new AbstractList<Object[]>() {
            @Override
            public Object[] get(int index) {
                return Util.copyOf(data.get(index), length);
            }

            @Override
            public int size() {
                return data.size();
            }
        };
    }

    public void testLoadWithMockResultsForLoadingOnlyDetailedSegments()
        throws ExecutionException, InterruptedException
    {
        GroupingSet groupingSetsInfo = getDefaultGroupingSet();
        ArrayList<GroupingSet> groupingSets =
            new ArrayList<GroupingSet>();
        groupingSets.add(groupingSetsInfo);
        SegmentLoader loader = new SegmentLoader(cacheMgr) {
            SqlStatement createExecuteSql(
                int cellRequestCount,
                GroupingSetsList groupingSetsList,
                List<StarPredicate> compoundPredicateList)
            {
                return new MockSqlStatement(
                    cellRequestCount,
                    groupingSetsList,
                    trim(5, getData(false)));
            }
        };
        final List<Future<Map<Segment, SegmentWithData>>> segmentFutures =
            new ArrayList<Future<Map<Segment, SegmentWithData>>>();
        loader.load(0, groupingSets, null, segmentFutures);
        for (Future<?> future : segmentFutures) {
            Util.safeGet(future, "");
        }
        SegmentAxis[] axes = groupingSetsInfo.getAxes();
        verifyYearAxis(axes[0]);
        verifyProductFamilyAxis(axes[1]);
        verifyProductDepartmentAxis(axes[2]);
        verifyGenderAxis(axes[3]);
        verifyUnitSalesDetailed(
            getFor(
                segmentFutures,
                groupingSetsInfo.getSegments().get(0)));
    }

    public void
        testProcessDataForGettingGroupingSetsBitKeysAndLoadingAxisValueSet()
        throws SQLException
    {
        GroupingSet groupableSetsInfo = getGroupingSetRollupOnGender();

        GroupingSet groupingSetsInfo = getDefaultGroupingSet();

        List<GroupingSet> groupingSets = new ArrayList<GroupingSet>();
        groupingSets.add(groupingSetsInfo);
        groupingSets.add(groupableSetsInfo);

        final SqlStatement stmt =
            new MockSqlStatement(
                0,
                new GroupingSetsList(groupingSets),
                getData(true));
        SegmentLoader loader = new SegmentLoader(cacheMgr) {
            @Override
            SqlStatement createExecuteSql(
                int cellRequestCount,
                GroupingSetsList groupingSetsList,
                List<StarPredicate> compoundPredicateList)
            {
                return stmt;
            }
        };
        int axisCount = 4;
        SortedSet<Comparable>[] axisValueSet =
            loader.getDistinctValueWorkspace(axisCount);
        boolean[] axisContainsNull = new boolean[axisCount];

        SegmentLoader.RowList list =
            loader.processData(
                stmt,
                axisContainsNull,
                axisValueSet,
                new GroupingSetsList(groupingSets));
        int totalNoOfRows = 12;
        int lengthOfRowWithBitKey = 6;
        assertEquals(totalNoOfRows, list.size());
        assertEquals(lengthOfRowWithBitKey, list.getTypes().size());
        list.first();
        list.next();
        assertEquals(BitKey.Factory.makeBitKey(0), list.getObject(5));

        BitKey bitKeyForSummaryRow = BitKey.Factory.makeBitKey(0);
        bitKeyForSummaryRow.set(0);
        list.next();
        list.next();
        assertEquals(bitKeyForSummaryRow, list.getObject(5));

        SortedSet<Comparable> yearAxis = axisValueSet[0];
        assertEquals(1, yearAxis.size());
        SortedSet<Comparable> productFamilyAxis = axisValueSet[1];
        assertEquals(3, productFamilyAxis.size());
        SortedSet<Comparable> productDepartmentAxis = axisValueSet[2];
        assertEquals(4, productDepartmentAxis.size());
        SortedSet<Comparable> genderAxis = axisValueSet[3];
        assertEquals(2, genderAxis.size());

        assertFalse(axisContainsNull[0]);
        assertFalse(axisContainsNull[1]);
        assertFalse(axisContainsNull[2]);
        assertFalse(axisContainsNull[3]);
    }

    private GroupingSet getGroupingSetRollupOnGender() {
        return
            getGroupingSet(
                getTestContext(),
                list(tableTime, tableProductClass, tableProductClass),
                list(fieldYear, fieldProductFamily, fieldProductDepartment),
                list(
                    fieldValuesYear,
                    fieldValuesProductFamily,
                    fieldValueProductDepartment),
                cubeNameSales,
                measureUnitSales);
    }

    public void testProcessDataForSettingNullAxis()
        throws SQLException
    {
        GroupingSet groupingSetsInfo = getDefaultGroupingSet();

        final SqlStatement stmt =
            new MockSqlStatement(
                0,
                new GroupingSetsList(
                    Collections.singletonList(groupingSetsInfo)),
                trim(5, getDataWithNullInAxisColumn(false)));
        SegmentLoader loader = new SegmentLoader(cacheMgr) {
            @Override
            SqlStatement createExecuteSql(
                int cellRequestCount,
                GroupingSetsList groupingSetsList,
                List<StarPredicate> compoundPredicateList)
            {
                return stmt;
            }
        };
        int axisCount = 4;
        SortedSet<Comparable>[] axisValueSet =
            loader.getDistinctValueWorkspace(axisCount);
        boolean[] axisContainsNull = new boolean[axisCount];
        List<GroupingSet> groupingSets = new ArrayList<GroupingSet>();
        groupingSets.add(groupingSetsInfo);

        loader.processData(
            stmt,
            axisContainsNull,
            axisValueSet,
            new GroupingSetsList(groupingSets));

        assertFalse(axisContainsNull[0]);
        assertFalse(axisContainsNull[1]);
        assertTrue(axisContainsNull[2]);
        assertFalse(axisContainsNull[3]);
    }

    public void testProcessDataForNonGroupingSetsScenario()
        throws SQLException
    {
        GroupingSet groupingSetsInfo = getDefaultGroupingSet();
        final List<Object[]> data = new ArrayList<Object[]>();
        data.add(new Object[]{"1997", "Food", "Deli", "F", "5990"});
        data.add(new Object[]{"1997", "Food", "Deli", "M", "6047"});
        data.add(new Object[]{"1997", "Food", "Canned_Products", "F", "867"});
        final SqlStatement stmt =
            new MockSqlStatement(
                0,
                new GroupingSetsList(
                    Collections.singletonList(groupingSetsInfo)),
                data);
        SegmentLoader loader = new SegmentLoader(cacheMgr) {
            @Override
            SqlStatement createExecuteSql(
                int cellRequestCount,
                GroupingSetsList groupingSetsList,
                List<StarPredicate> compoundPredicateList)
            {
                return stmt;
            }
        };
        List<GroupingSet> groupingSets = new ArrayList<GroupingSet>();
        groupingSets.add(groupingSetsInfo);

        SortedSet<Comparable>[] axisValueSet =
            loader.getDistinctValueWorkspace(4);
        SegmentLoader.RowList list =
            loader.processData(
                stmt,
                new boolean[4],
                axisValueSet,
                new GroupingSetsList(groupingSets));
        int totalNoOfRows = 3;
        assertEquals(totalNoOfRows, list.size());
        int lengthOfRowWithoutBitKey = 5;
        assertEquals(lengthOfRowWithoutBitKey, list.getTypes().size());

        SortedSet<Comparable> yearAxis = axisValueSet[0];
        assertEquals(1, yearAxis.size());
        SortedSet<Comparable> productFamilyAxis = axisValueSet[1];
        assertEquals(1, productFamilyAxis.size());
        SortedSet<Comparable> productDepartmentAxis = axisValueSet[2];
        assertEquals(2, productDepartmentAxis.size());
        SortedSet<Comparable> genderAxis = axisValueSet[3];
        assertEquals(2, genderAxis.size());
    }

    private void verifyUnitSalesDetailed(SegmentWithData segment) {
        Double[] unitSalesValues = {
            null, null, null, null, 1987.0, 2199.0,
            null, null, 867.0, 945.0, null, null, null, null, 5990.0,
            6047.0, null, null, 368.0, 473.0, null, null, null, null
        };
        int index = 0;
        for (Map.Entry<CellKey, Object> x : segment.getData()) {
            assertEquals(unitSalesValues[index++], x.getValue());
        }
    }

    private void verifyUnitSalesDetailedForSparse(SegmentWithData segment) {
        Map<CellKey, Double> cells = new HashMap<CellKey, Double>();
        cells.put(
            CellKey.Generator.newCellKey(new int[]{0, 2, 1, 0}),
            368.0);
        cells.put(
            CellKey.Generator.newCellKey(new int[]{0, 0, 2, 0}),
            1987.0);
        cells.put(
            CellKey.Generator.newCellKey(new int[]{0, 1, 0, 0}),
            867.0);
        cells.put(
            CellKey.Generator.newCellKey(new int[]{0, 2, 1, 1}),
            473.0);
        cells.put(
            CellKey.Generator.newCellKey(new int[]{0, 1, 0, 1}),
            945.0);
        cells.put(
            CellKey.Generator.newCellKey(new int[]{0, 1, 3, 0}),
            5990.0);
        cells.put(
            CellKey.Generator.newCellKey(new int[]{0, 0, 2, 1}),
            2199.0);
        cells.put(
            CellKey.Generator.newCellKey(new int[]{0, 1, 3, 1}),
            6047.0);

        for (Map.Entry<CellKey, Object> x : segment.getData()) {
            assertTrue(cells.containsKey(x.getKey()));
            assertEquals(cells.get(x.getKey()), x.getValue());
        }
    }

    private void verifyUnitSalesAggregateForSparse(SegmentWithData segment) {
        Map<CellKey, Double> cells = new HashMap<CellKey, Double>();
        cells.put(
            CellKey.Generator.newCellKey(new int[]{0, 2, 1}),
            841.0);
        cells.put(
            CellKey.Generator.newCellKey(new int[]{0, 1, 0}),
            1812.0);
        cells.put(
            CellKey.Generator.newCellKey(new int[]{0, 1, 3}),
            12037.0);
        cells.put(
            CellKey.Generator.newCellKey(new int[]{0, 0, 2}),
            4186.0);

        for (Map.Entry<CellKey, Object> x : segment.getData()) {
            assertTrue(cells.containsKey(x.getKey()));
            assertEquals(cells.get(x.getKey()), x.getValue());
        }
    }

    private void verifyUnitSalesAggregate(SegmentWithData segment) {
        Double[] unitSalesValues = {
            null, null, 4186.0, null, 1812.0, null,
            null, 12037.0, null, 841.0, null, null
        };
        int index = 0;
        for (Map.Entry<CellKey, Object> x : segment.getData()) {
            assertEquals(unitSalesValues[index++], x.getValue());
        }
    }

    public void testGetGroupingBitKey() throws SQLException {
        Object[] data = {
            "1997", "Food", "Deli", "M", "6047", 0, 0, 0, 0
        };
        ResultSet rowList =
            toResultSet(Collections.singletonList(data));
        assertTrue(rowList.next());
        assertEquals(
            BitKey.Factory.makeBitKey(4),
            new SegmentLoader(cacheMgr).getRollupBitKey(4, rowList, 5));

        data = new Object[]{
            "1997", "Food", "Deli", null, "12037", 0, 0, 0, 1
        };
        rowList = toResultSet(Collections.singletonList(data));
        BitKey key = BitKey.Factory.makeBitKey(4);
        key.set(3);
        assertEquals(
            key,
            new SegmentLoader(cacheMgr).getRollupBitKey(4, rowList, 5));

        data = new Object[] {
            "1997", null, "Deli", null, "12037", 0, 1, 0, 1
        };
        rowList = toResultSet(Collections.singletonList(data));
        key = BitKey.Factory.makeBitKey(4);
        key.set(1);
        key.set(3);
        assertEquals(
            key,
            new SegmentLoader(cacheMgr).getRollupBitKey(4, rowList, 5));
    }

    public void testGroupingSetsUtilForMissingGroupingBitKeys() {
        List<GroupingSet> groupingSets = new ArrayList<GroupingSet>();
        groupingSets.add(getDefaultGroupingSet());
        groupingSets.add(getGroupingSetRollupOnGender());
        GroupingSetsList detail = new GroupingSetsList(groupingSets);

        List<BitKey> bitKeysList = detail.getRollupColumnsBitKeyList();
        int columnsCount = 4;
        assertEquals(
            BitKey.Factory.makeBitKey(columnsCount),
            bitKeysList.get(0));
        BitKey key = BitKey.Factory.makeBitKey(columnsCount);
        key.set(0);
        assertEquals(key, bitKeysList.get(1));

        groupingSets = new ArrayList<GroupingSet>();
        groupingSets.add(getDefaultGroupingSet());
        groupingSets.add(getGroupingSetRollupOnGenderAndProductFamily());
        bitKeysList = new GroupingSetsList(groupingSets)
            .getRollupColumnsBitKeyList();
        assertEquals(
            BitKey.Factory.makeBitKey(columnsCount),
            bitKeysList.get(0));
        key = BitKey.Factory.makeBitKey(columnsCount);
        key.set(0);
        key.set(1);
        assertEquals(key, bitKeysList.get(1));

        assertTrue(
            new GroupingSetsList(
                new ArrayList<GroupingSet>())
                .getRollupColumnsBitKeyList().isEmpty());
    }

    private GroupingSet getGroupingSetRollupOnGenderAndProductFamily() {
        return getGroupingSet(
            getTestContext(),
            list(tableTime, tableProductClass),
            list(fieldYear, fieldProductDepartment),
            list(fieldValuesYear, fieldValueProductDepartment),
            cubeNameSales, measureUnitSales);
    }

    public void testGroupingSetsUtilSetsDetailForRollupColumns() {
        RolapStar.Measure measure =
            getMeasure(getTestContext(), cubeNameSales, measureUnitSales);
        RolapStar star = measure.getStar();
        RolapStar.Column year = star.lookupColumn(tableTime, fieldYear);
        RolapStar.Column productFamily =
            star.lookupColumn(tableProductClass, fieldProductFamily);
        RolapStar.Column productDepartment =
            star.lookupColumn(tableProductClass, fieldProductDepartment);
        RolapStar.Column gender = star.lookupColumn(tableCustomer, fieldGender);

        List<GroupingSet> groupingSets = new ArrayList<GroupingSet>();
        groupingSets.add(getDefaultGroupingSet());
        groupingSets.add(getGroupingSetRollupOnProductDepartment());
        groupingSets.add(getGroupingSetRollupOnGenderAndProductDepartment());
        GroupingSetsList detail = new GroupingSetsList(groupingSets);

        List<RolapStar.Column> rollupColumnsList = detail.getRollupColumns();
        assertEquals(2, rollupColumnsList.size());
        assertEquals(gender, rollupColumnsList.get(0));
        assertEquals(productDepartment, rollupColumnsList.get(1));

        groupingSets
            .add(getGroupingSetRollupOnGenderAndProductDepartmentAndYear());
        detail = new GroupingSetsList(groupingSets);
        rollupColumnsList = detail.getRollupColumns();
        assertEquals(3, rollupColumnsList.size());
        assertEquals(gender, rollupColumnsList.get(0));
        assertEquals(productDepartment, rollupColumnsList.get(1));
        assertEquals(year, rollupColumnsList.get(2));

        groupingSets
            .add(getGroupingSetRollupOnProductFamilyAndProductDepartment());
        detail = new GroupingSetsList(groupingSets);
        rollupColumnsList = detail.getRollupColumns();
        assertEquals(4, rollupColumnsList.size());
        assertEquals(gender, rollupColumnsList.get(0));
        assertEquals(productDepartment, rollupColumnsList.get(1));
        assertEquals(productFamily, rollupColumnsList.get(2));
        assertEquals(year, rollupColumnsList.get(3));

        assertTrue(
            new GroupingSetsList(new ArrayList<GroupingSet>())
            .getRollupColumns().isEmpty());
    }

    private GroupingSet getGroupingSetRollupOnGenderAndProductDepartment() {
        return getGroupingSet(
            getTestContext(),
            list(tableProductClass, tableTime),
            list(fieldProductFamily, fieldYear),
            list(fieldValuesProductFamily, fieldValuesYear),
            cubeNameSales,
            measureUnitSales);
    }

    private GroupingSet
        getGroupingSetRollupOnProductFamilyAndProductDepartment()
    {
        return getGroupingSet(
            getTestContext(),
            list(tableCustomer, tableTime),
            list(fieldGender, fieldYear),
            list(fieldValuesGender, fieldValuesYear),
            cubeNameSales,
            measureUnitSales);
    }

    private GroupingSet
        getGroupingSetRollupOnGenderAndProductDepartmentAndYear()
    {
        return getGroupingSet(
            getTestContext(),
            list(tableProductClass),
            list(fieldProductFamily),
            list(fieldValuesProductFamily),
            cubeNameSales,
            measureUnitSales);
    }

    private GroupingSet getGroupingSetRollupOnProductDepartment() {
        return getGroupingSet(
            getTestContext(),
            list(tableCustomer, tableProductClass, tableTime),
            list(fieldGender, fieldProductFamily, fieldYear),
            list(
                fieldValuesGender, fieldValuesProductFamily, fieldValuesYear),
            cubeNameSales,
            measureUnitSales);
    }

    public void testGroupingSetsUtilSetsForDetailForRollupColumns() {
        final TestContext testContext = getTestContext();
        RolapStar.Measure measure =
            getMeasure(testContext, cubeNameSales, measureUnitSales);
        RolapStar star = measure.getStar();
        RolapStar.Column year = star.lookupColumn(tableTime, fieldYear);
        RolapStar.Column productFamily =
            star.lookupColumn(tableProductClass, fieldProductFamily);
        RolapStar.Column productDepartment =
            star.lookupColumn(tableProductClass, fieldProductDepartment);
        RolapStar.Column gender = star.lookupColumn(tableCustomer, fieldGender);

        List<GroupingSet> groupingSets = new ArrayList<GroupingSet>();
        groupingSets.add(getDefaultGroupingSet());
        groupingSets.add(getGroupingSetRollupOnProductDepartment());
        groupingSets.add(getGroupingSetRollupOnGenderAndProductDepartment());
        GroupingSetsList detail = new GroupingSetsList(groupingSets);

        List<RolapStar.Column> rollupColumnsList = detail.getRollupColumns();
        assertEquals(2, rollupColumnsList.size());
        assertEquals(gender, rollupColumnsList.get(0));
        assertEquals(productDepartment, rollupColumnsList.get(1));

        groupingSets
            .add(getGroupingSetRollupOnGenderAndProductDepartmentAndYear());
        detail = new GroupingSetsList(groupingSets);
        rollupColumnsList = detail.getRollupColumns();
        assertEquals(3, rollupColumnsList.size());
        assertEquals(gender, rollupColumnsList.get(0));
        assertEquals(productDepartment, rollupColumnsList.get(1));
        assertEquals(year, rollupColumnsList.get(2));

        groupingSets
            .add(getGroupingSetRollupOnProductFamilyAndProductDepartment());
        detail = new GroupingSetsList(groupingSets);
        rollupColumnsList = detail.getRollupColumns();
        assertEquals(4, rollupColumnsList.size());
        assertEquals(gender, rollupColumnsList.get(0));
        assertEquals(productDepartment, rollupColumnsList.get(1));
        assertEquals(productFamily, rollupColumnsList.get(2));
        assertEquals(year, rollupColumnsList.get(3));

        assertTrue(
            new GroupingSetsList(new ArrayList<GroupingSet>())
            .getRollupColumns().isEmpty());
    }

    public void testGroupingSetsUtilSetsForGroupingFunctionIndex() {
        List<GroupingSet> groupingSets = new ArrayList<GroupingSet>();
        groupingSets.add(getDefaultGroupingSet());
        groupingSets.add(getGroupingSetRollupOnProductDepartment());
        groupingSets.add(getGroupingSetRollupOnGenderAndProductDepartment());
        GroupingSetsList detail = new GroupingSetsList(groupingSets);
        assertEquals(0, detail.findGroupingFunctionIndex(3));
        assertEquals(1, detail.findGroupingFunctionIndex(2));

        groupingSets
            .add(getGroupingSetRollupOnGenderAndProductDepartmentAndYear());
        detail = new GroupingSetsList(groupingSets);
        assertEquals(0, detail.findGroupingFunctionIndex(3));
        assertEquals(1, detail.findGroupingFunctionIndex(2));
        assertEquals(2, detail.findGroupingFunctionIndex(0));

        groupingSets
            .add(getGroupingSetRollupOnProductFamilyAndProductDepartment());
        detail = new GroupingSetsList(groupingSets);
        assertEquals(0, detail.findGroupingFunctionIndex(3));
        assertEquals(1, detail.findGroupingFunctionIndex(2));
        assertEquals(2, detail.findGroupingFunctionIndex(1));
        assertEquals(3, detail.findGroupingFunctionIndex(0));
    }

    public void testGetGroupingColumnsList() {
        GroupingSet groupingSetsInfo = getDefaultGroupingSet();

        GroupingSet groupableSetsInfo = getGroupingSetRollupOnGender();


        RolapStar.Column[] detailedColumns =
            groupingSetsInfo.getSegments().get(0).getColumns();
        RolapStar.Column[] summaryColumns =
            groupableSetsInfo.getSegments().get(0).getColumns();
        List<GroupingSet> groupingSets = new ArrayList<GroupingSet>();
        groupingSets.add(groupingSetsInfo);
        groupingSets.add(groupableSetsInfo);

        List<RolapStar.Column[]> groupingColumns =
            new GroupingSetsList(groupingSets).getGroupingSetsColumns();
        assertEquals(2, groupingColumns.size());
        assertEquals(detailedColumns, groupingColumns.get(0));
        assertEquals(summaryColumns, groupingColumns.get(1));

        groupingColumns = new GroupingSetsList(
            new ArrayList<GroupingSet>()).getGroupingSetsColumns();
        assertEquals(0, groupingColumns.size());
    }

    private GroupingSet getDefaultGroupingSet() {
        return getGroupingSet(
            getTestContext(),
            list(
                tableCustomer, tableProductClass, tableProductClass, tableTime),
            list(
                fieldGender, fieldProductDepartment, fieldProductFamily,
                fieldYear),
            list(
                fieldValuesGender, fieldValueProductDepartment,
                fieldValuesProductFamily, fieldValuesYear),
            cubeNameSales,
            measureUnitSales);
    }

    private void verifyYearAxis(SegmentAxis axis) {
        Comparable[] keys = axis.getKeys();
        assertEquals(1, keys.length);
        assertEquals("1997", keys[0].toString());
    }

    private void verifyProductFamilyAxis(SegmentAxis axis) {
        Comparable[] keys = axis.getKeys();
        assertEquals(3, keys.length);
        assertEquals("Drink", keys[0].toString());
        assertEquals("Food", keys[1].toString());
        assertEquals("Non-Consumable", keys[2].toString());
    }

    private void verifyProductDepartmentAxis(SegmentAxis axis) {
        Comparable[] keys = axis.getKeys();
        assertEquals(4, keys.length);
        assertEquals("Canned_Products", keys[0].toString());
    }

    private void verifyGenderAxis(SegmentAxis axis) {
        Comparable[] keys = axis.getKeys();
        assertEquals(2, keys.length);
        assertEquals("F", keys[0].toString());
        assertEquals("M", keys[1].toString());
    }

    private List<Object[]> getData(boolean incSummaryData) {
        List<Object[]> data = new ArrayList<Object[]>();
        data.add(new Object[]{"1997", "Food", "Deli", "F", "5990", 0});
        data.add(new Object[]{"1997", "Food", "Deli", "M", "6047", 0});
        if (incSummaryData) {
            data.add(new Object[]{"1997", "Food", "Deli", null, "12037", 1});
        }
        data.add(
            new Object[]{"1997", "Food", "Canned_Products", "F", "867", 0});
        data.add(
            new Object[]{"1997", "Food", "Canned_Products", "M", "945", 0});
        if (incSummaryData) {
            data.add(
                new Object[]{
                    "1997", "Food", "Canned_Products", null, "1812", 1});
        }
        data.add(new Object[]{"1997", "Drink", "Dairy", "F", "1987", 0});
        data.add(new Object[]{"1997", "Drink", "Dairy", "M", "2199", 0});
        if (incSummaryData) {
            data.add(new Object[]{"1997", "Drink", "Dairy", null, "4186", 1});
        }
        data.add(
            new Object[]{
                "1997", "Non-Consumable", "Carousel", "F", "368", 0});
        data.add(
            new Object[]{
                "1997", "Non-Consumable", "Carousel", "M", "473", 0});
        if (incSummaryData) {
            data.add(
                new Object[]{
                    "1997", "Non-Consumable", "Carousel", null, "841", 1});
        }
        return data;
    }

    private List<Object[]> getDataWithNullInRollupColumn(
        boolean incSummaryData)
    {
        List<Object[]> data = new ArrayList<Object[]>();
        data.add(new Object[]{"1997", "Food", "Deli", "F", "5990", 0});
        data.add(new Object[]{"1997", "Food", "Deli", "M", "6047", 0});
        data.add(new Object[]{"1997", "Food", "Deli", null, "867", 0});
        if (incSummaryData) {
            data.add(new Object[]{"1997", "Food", "Deli", null, "12037", 1});
        }
        return data;
    }

    private List<Object[]> getDataWithNullInAxisColumn(
        boolean incSummaryData)
    {
        List<Object[]> data = new ArrayList<Object[]>();
        data.add(new Object[]{"1997", "Food", "Deli", "F", "5990", 0});
        data.add(new Object[]{"1997", "Food", "Deli", "M", "6047", 0});
        if (incSummaryData) {
            data.add(new Object[]{"1997", "Food", "Deli", null, "12037", 1});
        }
        data.add(
            new Object[]{"1997", "Food", null, "F", "867", 0});
        return data;
    }

    public static class MyDelegatingInvocationHandler
        extends DelegatingInvocationHandler
    {
        int row;
        public boolean wasNull;
        ResultSetMetaData resultSetMetaData;
        private final List<Object[]> list;

        public MyDelegatingInvocationHandler(List<Object[]> list) {
            this.list = list;
            row = -1;
        }

        protected Object getTarget() {
            return null;
        }

        public ResultSetMetaData getMetaData() {
            return resultSetMetaData;
        }

        // implement ResultSetMetaData
        public int getColumnCount() {
            return list.get(0).length;
        }

        // implement ResultSetMetaData
        public int getColumnType(int column) {
            return Types.VARCHAR;
        }

        public boolean next() {
            if (row < list.size() - 1) {
                ++row;
                return true;
            }
            return false;
        }

        public Object getObject(int column) {
            return list.get(row)[column - 1];
        }

        public int getInt(int column) {
            final Object o = list.get(row)[column - 1];
            if (o == null) {
                wasNull = true;
                return 0;
            } else {
                wasNull = false;
                return ((Number) o).intValue();
            }
        }

        public double getDouble(int column) {
            final Object o = list.get(row)[column - 1];
            if (o == null) {
                wasNull = true;
                return 0D;
            } else {
                wasNull = false;
                return ((Number) o).doubleValue();
            }
        }

        public boolean wasNull() {
            return wasNull;
        }
    }

    private class MockSqlStatement extends SqlStatement {
        private final List<Object[]> data;

        public MockSqlStatement(
            int cellRequestCount,
            GroupingSetsList groupingSetsList,
            List<Object[]> data)
        {
            super(
                groupingSetsList.getStar().getDataSource(),
                "",
                null,
                cellRequestCount,
                0,
                SegmentLoaderTest.this.locus,
                0,
                0,
                null);
            this.data = data;
        }

        @Override
        public List<Type> guessTypes() throws SQLException {
            return Collections.nCopies(
                getResultSet().getMetaData().getColumnCount(),
                Type.OBJECT);
        }

        @Override
        public ResultSet getResultSet() {
            return toResultSet(data);
        }
    }
}

// End SegmentLoaderTest.java
