/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2004-2009 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap.agg;

import mondrian.rolap.*;

import java.util.*;
import java.sql.SQLException;

/**
 * <p>Test for <code>SegmentLoader</code></p>
 *
 * @author Thiyagu
 * @version $Id$
 * @since 06-Jun-2007
 */
public class SegmentLoaderTest extends BatchTestCase {

    protected void setUp() throws Exception {
        super.setUp();
    }

    public void testLoadWithMockResultsForLoadingSummaryAndDetailedSegments() {
        GroupingSet groupableSetsInfo = getGroupingSetRollupOnGender();

        GroupingSet groupingSetsInfo = getDefaultGroupingSet();
        ArrayList<GroupingSet> groupingSets =
            new ArrayList<GroupingSet>();
        groupingSets.add(groupingSetsInfo);
        groupingSets.add(groupableSetsInfo);
        SegmentLoader loader = new SegmentLoader() {
            SqlStatement createExecuteSql(
                GroupingSetsList groupingSetsList,
                List<StarPredicate> compoundPredicateList)
            {
                return null;
            }

            List<Object[]> loadData(
                SqlStatement stmt,
                GroupingSetsList groupingSetsList)
                throws SQLException
            {
                return getData(true);
            }
        };
        loader.load(groupingSets, null, null);
        Aggregation.Axis[] axes = groupingSetsInfo.getAxes();
        verifyYearAxis(axes[0]);
        verifyProductFamilyAxis(axes[1]);
        verifyProductDepartmentAxis(axes[2]);
        verifyGenderAxis(axes[3]);
        verifyUnitSalesDetailed(groupingSets.get(0).getSegments()[0]);

        axes = groupingSets.get(0).getAxes();
        verifyYearAxis(axes[0]);
        verifyProductFamilyAxis(axes[1]);
        verifyProductDepartmentAxis(axes[2]);
        verifyUnitSalesAggregate(groupingSets.get(1).getSegments()[0]);
    }

    /**
     * Tests load with mock results for loading summary and detailed
     * segments with null in rollup column.
     */
    public void testLoadWithWithNullInRollupColumn() {
        GroupingSet groupableSetsInfo = getGroupingSetRollupOnGender();

        GroupingSet groupingSetsInfo = getDefaultGroupingSet();
        ArrayList<GroupingSet> groupingSets =
            new ArrayList<GroupingSet>();
        groupingSets.add(groupingSetsInfo);
        groupingSets.add(groupableSetsInfo);
        SegmentLoader loader = new SegmentLoader() {
            SqlStatement createExecuteSql(
                GroupingSetsList groupingSetsList,
                List<StarPredicate> compoundPredicateList)
            {
                return null;
            }

            List<Object[]> loadData(
                SqlStatement stmt,
                GroupingSetsList groupingSetsList)
                throws SQLException
            {
                return getDataWithNullInRollupColumn(true);
            }
        };
        loader.load(groupingSets, null, null);
        Segment detailedSegment = groupingSets.get(0).getSegments()[0];
        assertEquals(3, detailedSegment.getCellCount());
    }

    public void
        testLoadWithMockResultsForLoadingSummaryAndDetailedSegmentsUsingSparse()
    {
        GroupingSet groupableSetsInfo = getGroupingSetRollupOnGender();

        GroupingSet groupingSetsInfo = getDefaultGroupingSet();
        ArrayList<GroupingSet> groupingSets =
            new ArrayList<GroupingSet>();
        groupingSets.add(groupingSetsInfo);
        groupingSets.add(groupableSetsInfo);
        SegmentLoader loader = new SegmentLoader() {
            SqlStatement createExecuteSql(
                GroupingSetsList groupingSetsList,
                List<StarPredicate> compoundPredicateList)
            {
                return null;
            }

            List<Object[]> loadData(
                SqlStatement stmt,
                GroupingSetsList groupingSetsList) throws
                SQLException
            {
                return getData(true);
            }

            boolean useSparse(boolean sparse, int n, List<Object[]> rows) {
                return true;
            }
        };
        loader.load(groupingSets, null, null);
        Aggregation.Axis[] axes = groupingSetsInfo.getAxes();
        verifyYearAxis(axes[0]);
        verifyProductFamilyAxis(axes[1]);
        verifyProductDepartmentAxis(axes[2]);
        verifyGenderAxis(axes[3]);
        verifyUnitSalesDetailedForSparse(groupingSets.get(0).getSegments()[0]);

        axes = groupingSets.get(0).getAxes();
        verifyYearAxis(axes[0]);
        verifyProductFamilyAxis(axes[1]);
        verifyProductDepartmentAxis(axes[2]);
        verifyUnitSalesAggregateForSparse(
            groupingSets.get(1).getSegments()[0]);
    }

    public void testLoadWithMockResultsForLoadingOnlyDetailedSegments() {
        GroupingSet groupingSetsInfo = getDefaultGroupingSet();
        ArrayList<GroupingSet> groupingSets =
            new ArrayList<GroupingSet>();
        groupingSets.add(groupingSetsInfo);
        SegmentLoader loader = new SegmentLoader() {
            SqlStatement createExecuteSql(
                GroupingSetsList groupingSetsList,
                List<StarPredicate> compoundPredicateList)
            {
                return null;
            }

            List<Object[]> loadData(
                SqlStatement stmt,
                GroupingSetsList groupingSetsList)
                throws SQLException
            {
                return getData(false);
            }
        };
        loader.load(groupingSets, null, null);
        Aggregation.Axis[] axes = groupingSetsInfo.getAxes();
        verifyYearAxis(axes[0]);
        verifyProductFamilyAxis(axes[1]);
        verifyProductDepartmentAxis(axes[2]);
        verifyGenderAxis(axes[3]);
        verifyUnitSalesDetailed(groupingSetsInfo.getSegments()[0]);
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

        SegmentLoader loader = new SegmentLoader() {
            List<Object[]> loadData(
                SqlStatement stmt,
                GroupingSetsList groupingSetsList) throws
                SQLException
            {
                return getData(true);
            }
        };
        int axisCount = 4;
        SortedSet<Comparable<?>>[] axisValueSet =
            loader.getDistinctValueWorkspace(axisCount);
        boolean[] axisContainsNull = new boolean[axisCount];

        List<Object[]> list =
            loader.processData(
                null, axisContainsNull,
                axisValueSet, new GroupingSetsList(groupingSets));
        int totalNoOfRows = 12;
        int lengthOfRowWithBitKey = 6;
        Object[] detailedRow = list.get(0);
        assertEquals(totalNoOfRows, list.size());
        assertEquals(lengthOfRowWithBitKey, detailedRow.length);
        assertEquals(BitKey.Factory.makeBitKey(0), detailedRow[5]);

        BitKey bitKeyForSummaryRow = BitKey.Factory.makeBitKey(0);
        bitKeyForSummaryRow.set(0);
        Object[] summaryRow = list.get(2);
        assertEquals(bitKeyForSummaryRow, summaryRow[5]);

        SortedSet<Comparable<?>> yearAxis = axisValueSet[0];
        assertEquals(1, yearAxis.size());
        SortedSet<Comparable<?>> productFamilyAxis = axisValueSet[1];
        assertEquals(3, productFamilyAxis.size());
        SortedSet<Comparable<?>> productDepartmentAxis = axisValueSet[2];
        assertEquals(4, productDepartmentAxis.size());
        SortedSet<Comparable<?>> genderAxis = axisValueSet[3];
        assertEquals(2, genderAxis.size());

        assertFalse(axisContainsNull[0]);
        assertFalse(axisContainsNull[1]);
        assertFalse(axisContainsNull[2]);
        assertFalse(axisContainsNull[3]);
    }

    private GroupingSet getGroupingSetRollupOnGender() {
        GroupingSet groupableSetsInfo =
            getGroupingSet(
                new String[]{tableTime, tableProductClass, tableProductClass},
                new String[]{
                    fieldYear, fieldProductFamily, fieldProductDepartment},
                new String[][]{
                    fieldValuesYear,
                    fieldValuesProductFamily,
                    fieldValueProductDepartment},
                cubeNameSales,
                measureUnitSales);
        return groupableSetsInfo;
    }

    public void testProcessDataForSettingNullAxis()
        throws SQLException
    {
        GroupingSet groupingSetsInfo = getDefaultGroupingSet();

        SegmentLoader loader = new SegmentLoader() {
            List<Object[]> loadData(
                SqlStatement stmt,
                GroupingSetsList groupingSetsList) throws
                SQLException
            {
                return getDataWithNullInAxisColumn(false);
            }
        };
        int axisCount = 4;
        SortedSet<Comparable<?>>[] axisValueSet =
            loader.getDistinctValueWorkspace(axisCount);
        boolean[] axisContainsNull = new boolean[axisCount];
        List<GroupingSet> groupingSets = new ArrayList<GroupingSet>();
        groupingSets.add(groupingSetsInfo);

        loader.processData(
            null, axisContainsNull,
            axisValueSet, new GroupingSetsList(groupingSets));

        assertFalse(axisContainsNull[0]);
        assertFalse(axisContainsNull[1]);
        assertTrue(axisContainsNull[2]);
        assertFalse(axisContainsNull[3]);
    }

    public void testProcessDataForNonGroupingSetsScenario()
        throws SQLException
    {
        GroupingSet groupingSetsInfo = getDefaultGroupingSet();

        SegmentLoader loader = new SegmentLoader() {
            List<Object[]> loadData(
                SqlStatement stmt,
                GroupingSetsList groupingSetsList)
                throws SQLException
            {
                List<Object[]> data = new ArrayList<Object[]>();
                data.add(new Object[]{"1997", "Food", "Deli", "F", "5990"});
                data.add(new Object[]{"1997", "Food", "Deli", "M", "6047"});
                data.add(
                    new Object[] {
                        "1997", "Food", "Canned_Products", "F", "867"});

                return data;
            }
        };
        List<GroupingSet> groupingSets = new ArrayList<GroupingSet>();
        groupingSets.add(groupingSetsInfo);

        SortedSet<Comparable<?>>[] axisValueSet =
            loader.getDistinctValueWorkspace(4);
        List<Object[]> list =
            loader.processData(
                null, new boolean[4],
                axisValueSet,
                new GroupingSetsList(groupingSets));
        int totalNoOfRows = 3;
        assertEquals(totalNoOfRows, list.size());
        int lengthOfRowWithoutBitKey = 5;
        assertEquals(lengthOfRowWithoutBitKey, list.get(0).length);

        SortedSet<Comparable<?>> yearAxis = axisValueSet[0];
        assertEquals(1, yearAxis.size());
        SortedSet<Comparable<?>> productFamilyAxis = axisValueSet[1];
        assertEquals(1, productFamilyAxis.size());
        SortedSet<Comparable<?>> productDepartmentAxis = axisValueSet[2];
        assertEquals(2, productDepartmentAxis.size());
        SortedSet<Comparable<?>> genderAxis = axisValueSet[3];
        assertEquals(2, genderAxis.size());
    }

    private void verifyUnitSalesDetailed(Segment segment) {
        Double[] unitSalesValues = {null, null, null, null, 1987.0, 2199.0,
            null, null, 867.0, 945.0, null, null, null, null, 5990.0,
            6047.0, null, null, 368.0, 473.0, null, null, null, null};
        Iterator<Map.Entry<CellKey, Object>> iterator =
            segment.getData().iterator();
        int index = 0;
        while (iterator.hasNext()) {
            Map.Entry<CellKey, Object> x = iterator.next();
            assertEquals(unitSalesValues[index++], x.getValue());
        }
    }

    private void verifyUnitSalesDetailedForSparse(Segment segment) {
        List<CellKey> cellKeys = new ArrayList<CellKey>();
        cellKeys.add(CellKey.Generator.newCellKey(new int[]{0, 2, 1, 0}));
        cellKeys.add(CellKey.Generator.newCellKey(new int[]{0, 0, 2, 0}));
        cellKeys.add(CellKey.Generator.newCellKey(new int[]{0, 1, 0, 0}));
        cellKeys.add(CellKey.Generator.newCellKey(new int[]{0, 2, 1, 1}));
        cellKeys.add(CellKey.Generator.newCellKey(new int[]{0, 1, 0, 1}));
        cellKeys.add(CellKey.Generator.newCellKey(new int[]{0, 1, 3, 0}));
        cellKeys.add(CellKey.Generator.newCellKey(new int[]{0, 0, 2, 1}));
        cellKeys.add(CellKey.Generator.newCellKey(new int[]{0, 1, 3, 1}));
        Double[] unitSalesValues = {
            368.0, 1987.0, 867.0, 473.0, 945.0, 5990.0, 2199.0,
            6047.0
        };

        Iterator<Map.Entry<CellKey, Object>> iterator =
            segment.getData().iterator();
        int index = 0;
        while (iterator.hasNext()) {
            Map.Entry<CellKey, Object> x = iterator.next();
            assertEquals(cellKeys.get(index), x.getKey());
            assertEquals(unitSalesValues[index], x.getValue());
            index++;
        }
    }

    private void verifyUnitSalesAggregateForSparse(Segment segment) {
        List<CellKey> cellKeys = new ArrayList<CellKey>();
        cellKeys.add(CellKey.Generator.newCellKey(new int[]{0, 2, 1}));
        cellKeys.add(CellKey.Generator.newCellKey(new int[]{0, 1, 0}));
        cellKeys.add(CellKey.Generator.newCellKey(new int[]{0, 1, 3}));
        cellKeys.add(CellKey.Generator.newCellKey(new int[]{0, 0, 2}));
        Double[] unitSalesValues = {841.0, 1812.0, 12037.0, 4186.0,};

        Iterator<Map.Entry<CellKey, Object>> iterator =
            segment.getData().iterator();
        int index = 0;
        while (iterator.hasNext()) {
            Map.Entry<CellKey, Object> x = iterator.next();
            assertEquals(cellKeys.get(index), x.getKey());
            assertEquals(unitSalesValues[index], x.getValue());
            index++;
        }
    }

    private void verifyUnitSalesAggregate(Segment segment) {
        Double[] unitSalesValues = {
            null, null, 4186.0, null, 1812.0, null,
            null, 12037.0, null, 841.0, null, null
        };
        Iterator<Map.Entry<CellKey, Object>> iterator =
            segment.getData().iterator();
        int index = 0;
        while (iterator.hasNext()) {
            Map.Entry<CellKey, Object> x = iterator.next();
            assertEquals(unitSalesValues[index++], x.getValue());
        }
    }

    public void testGetGroupingBitKey() {
        Object[] data = {
            "1997", "Food", "Deli", "M", "6047", 0, 0, 0, 0
        };
        assertEquals(
            BitKey.Factory.makeBitKey(4),
            new SegmentLoader().getRollupBitKey(4, data, 5));

        data = new Object[]{
            "1997", "Food", "Deli", null, "12037", 0, 0, 0, 1};
        BitKey key = BitKey.Factory.makeBitKey(4);
        key.set(3);
        assertEquals(key, new SegmentLoader().getRollupBitKey(4, data, 5));

        data = new Object[]{"1997", null, "Deli", null, "12037", 0,
            1, 0, 1};
        key = BitKey.Factory.makeBitKey(4);
        key.set(1);
        key.set(3);
        assertEquals(key, new SegmentLoader().getRollupBitKey(4, data, 5));
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

        assertTrue(new GroupingSetsList(new ArrayList<GroupingSet>())
            .getRollupColumnsBitKeyList().isEmpty());
    }

    private GroupingSet getGroupingSetRollupOnGenderAndProductFamily() {
        return getGroupingSet(
            new String[]{tableTime, tableProductClass},
            new String[]{fieldYear, fieldProductDepartment},
            new String[][]{fieldValuesYear, fieldValueProductDepartment},
            cubeNameSales, measureUnitSales);
    }

    public void testGroupingSetsUtilSetsDetailForRollupColumns() {
        RolapStar.Measure measure = getMeasure(cubeNameSales, measureUnitSales);
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
            new String[]{tableProductClass, tableTime},
            new String[]{fieldProductFamily, fieldYear},
            new String[][]{fieldValuesProductFamily, fieldValuesYear},
            cubeNameSales,
            measureUnitSales);
    }

    private GroupingSet
        getGroupingSetRollupOnProductFamilyAndProductDepartment()
    {
        return getGroupingSet(
            new String[]{tableCustomer, tableTime},
            new String[]{fieldGender, fieldYear},
            new String[][]{fieldValuesGender, fieldValuesYear},
            cubeNameSales,
            measureUnitSales);
    }

    private GroupingSet
        getGroupingSetRollupOnGenderAndProductDepartmentAndYear()
    {
        return getGroupingSet(
            new String[]{tableProductClass},
            new String[]{fieldProductFamily},
            new String[][]{fieldValuesProductFamily},
            cubeNameSales,
            measureUnitSales);
    }

    private GroupingSet getGroupingSetRollupOnProductDepartment() {
        return getGroupingSet(
            new String[]{tableCustomer, tableProductClass,
                tableTime},
            new String[]{fieldGender, fieldProductFamily,
                fieldYear},
            new String[][]{fieldValuesGender, fieldValuesProductFamily,
                fieldValuesYear},
            cubeNameSales,
            measureUnitSales);
    }

    public void testGroupingSetsUtilSetsForDetailForRollupColumns() {
        RolapStar.Measure measure = getMeasure(cubeNameSales, measureUnitSales);
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
            groupingSetsInfo.getSegments()[0].aggregation.getColumns();
        RolapStar.Column[] summaryColumns =
            groupableSetsInfo.getSegments()[0].aggregation.getColumns();
        List<GroupingSet> summayAggs = new ArrayList<GroupingSet>();
        summayAggs.add(groupableSetsInfo);
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

    public void testSetFailOnStillLoadingSegments() {
        List<GroupingSet> groupingSets = new ArrayList<GroupingSet>();
        groupingSets.add(getDefaultGroupingSet());
        new SegmentLoader().setFailOnStillLoadingSegments(
            new GroupingSetsList(groupingSets));

        for (GroupingSet groupingSet : groupingSets) {
            for (Segment segment : groupingSet.getSegments()) {
                assertTrue(segment.isFailed());
            }
        }

        groupingSets = new ArrayList<GroupingSet>();
        groupingSets.add(getDefaultGroupingSet());
        groupingSets.add(getGroupingSetRollupOnGender());
        new SegmentLoader().setFailOnStillLoadingSegments(
            new GroupingSetsList(groupingSets));
        for (GroupingSet groupingSet : groupingSets) {
            for (Segment segment : groupingSet.getSegments()) {
                assertTrue(segment.isFailed());
            }
        }
    }


    private GroupingSet getDefaultGroupingSet() {
        return getGroupingSet(
            new String[]{tableCustomer, tableProductClass,
                tableProductClass, tableTime},
            new String[]{fieldGender, fieldProductDepartment,
                fieldProductFamily, fieldYear},
            new String[][]{fieldValuesGender, fieldValueProductDepartment,
                fieldValuesProductFamily, fieldValuesYear},
            cubeNameSales,
            measureUnitSales);
    }

    private void verifyYearAxis(Aggregation.Axis axis) {
        Comparable<?>[] keys = axis.getKeys();
        assertEquals(1, keys.length);
        assertEquals("1997", keys[0].toString());
    }

    private void verifyProductFamilyAxis(Aggregation.Axis axis) {
        Comparable<?>[] keys = axis.getKeys();
        assertEquals(3, keys.length);
        assertEquals("Drink", keys[0].toString());
        assertEquals("Food", keys[1].toString());
        assertEquals("Non-Consumable", keys[2].toString());
    }

    private void verifyProductDepartmentAxis(Aggregation.Axis axis) {
        Comparable<?>[] keys = axis.getKeys();
        assertEquals(4, keys.length);
        assertEquals("Canned_Products", keys[0].toString());
    }

    private void verifyGenderAxis(Aggregation.Axis axis) {
        Comparable<?>[] keys = axis.getKeys();
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
}

// End SegmentLoaderTest.java
