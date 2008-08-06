/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2007-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.olap.MondrianProperties;
import mondrian.olap.Util;
import mondrian.rolap.sql.SqlQuery;
import mondrian.rolap.agg.SegmentLoader;
import mondrian.rolap.agg.GroupingSet;
import mondrian.rolap.agg.AggregationKey;
import mondrian.test.TestContext;
import mondrian.test.SqlPattern;

import java.util.*;

/**
 * Test for <code>FastBatchingCellReader</code>.
 *
 * @author Thiyagu
 * @version $Id$
 * @since 24-May-2007
 */
public class FastBatchingCellReaderTest extends BatchTestCase {

    protected void setUp() throws Exception {
        super.setUp();
        getTestContext().clearConnection();
    }

    public void testMissingSubtotalBugMetricFilter() {
        String query =
            "With " +
            "Set [*NATIVE_CJ_SET] as " +
            "'NonEmptyCrossJoin({[Time].[Year].[1997]}," +
            "                   NonEmptyCrossJoin({[Product].[All Products].[Drink]},{[Education Level].[All Education Levels].[Bachelors Degree]}))' " +
            "Set [*METRIC_CJ_SET] as 'Filter([*NATIVE_CJ_SET],[Measures].[*Unit Sales_SEL~SUM] > 1000.0)' "+
            "Set [*METRIC_MEMBERS_Education Level] as 'Generate([*METRIC_CJ_SET], {[Education Level].CurrentMember})' " +
            "Member [Measures].[*Unit Sales_SEL~SUM] as '([Measures].[Unit Sales],[Time].CurrentMember,[Product].CurrentMember,[Education Level].CurrentMember)', SOLVE_ORDER=200 " +
            "Member [Education Level].[*CTX_MEMBER_SEL~SUM] as 'Sum(Filter([*METRIC_MEMBERS_Education Level],[Measures].[*Unit Sales_SEL~SUM] > 1000.0))', SOLVE_ORDER=-102 " +
            "Select " +
            "{[Measures].[Unit Sales]} on columns, " +
            "Non Empty Union(CrossJoin(Generate([*METRIC_CJ_SET], {([Time].CurrentMember,[Product].CurrentMember)}),{[Education Level].[*CTX_MEMBER_SEL~SUM]})," +
            "                Generate([*METRIC_CJ_SET], {([Time].CurrentMember,[Product].CurrentMember,[Education Level].CurrentMember)})) on rows " +
            "From [Sales]";

        String result =
            "Axis #0:\n" + 
            "{}\n" +
            "Axis #1:\n" +
            "{[Measures].[Unit Sales]}\n"+
            "Axis #2:\n" +
            "{[Time].[1997], [Product].[All Products].[Drink], [Education Level].[*CTX_MEMBER_SEL~SUM]}\n" +
            "{[Time].[1997], [Product].[All Products].[Drink], [Education Level].[All Education Levels].[Bachelors Degree]}\n" +
            "Row #0: 6,423\n" +
            "Row #1: 6,423\n";

        assertQueryReturns(query, fold(result));
    }

    public void testMissingSubtotalBugMultiLevelMetricFilter() {
        String query =
            "With " + 
            "Set [*NATIVE_CJ_SET] as 'NonEmptyCrossJoin([*BASE_MEMBERS_Product],[*BASE_MEMBERS_Education Level])' " + 
            "Set [*METRIC_CJ_SET] as 'Filter([*NATIVE_CJ_SET],[Measures].[*Store Cost_SEL~SUM] > 1000.0)' " +
            "Set [*BASE_MEMBERS_Product] as '{[Product].[All Products].[Drink].[Beverages],[Product].[All Products].[Food].[Baked Goods]}' " + 
            "Set [*METRIC_MEMBERS_Product] as 'Generate([*METRIC_CJ_SET], {[Product].CurrentMember})' " + 
            "Set [*BASE_MEMBERS_Education Level] as '{[Education Level].[All Education Levels].[High School Degree],[Education Level].[All Education Levels].[Partial High School]}' " + 
            "Set [*METRIC_MEMBERS_Education Level] as 'Generate([*METRIC_CJ_SET], {[Education Level].CurrentMember})' " +
            "Member [Measures].[*Store Cost_SEL~SUM] as '([Measures].[Store Cost],[Product].CurrentMember,[Education Level].CurrentMember)', SOLVE_ORDER=200 " + 
            "Member [Product].[All Products].[Drink].[*CTX_MEMBER_SEL~SUM] as 'Sum(Filter([*METRIC_MEMBERS_Product],[Product].CurrentMember.Parent = [Product].[All Products].[Drink]))', SOLVE_ORDER=-100 " + 
            "Member [Product].[All Products].[Food].[*CTX_MEMBER_SEL~SUM] as 'Sum(Filter([*METRIC_MEMBERS_Product],[Product].CurrentMember.Parent = [Product].[All Products].[Food]))', SOLVE_ORDER=-100 " +
            "Member [Education Level].[*CTX_MEMBER_SEL~SUM] as 'Sum(Filter([*METRIC_MEMBERS_Education Level],[Measures].[*Store Cost_SEL~SUM] > 1000.0))', SOLVE_ORDER=-101 " +
            "Select " +
            "{[Measures].[Store Cost]} on columns, " + 
            "NonEmptyCrossJoin({[Product].[All Products].[Drink].[*CTX_MEMBER_SEL~SUM],[Product].[All Products].[Food].[*CTX_MEMBER_SEL~SUM]},{[Education Level].[*CTX_MEMBER_SEL~SUM]}) " +
            "on rows From [Sales]";
        
        String result = 
            "Axis #0:\n" +
            "{}\n" +
            "Axis #1:\n" +
            "{[Measures].[Store Cost]}\n" +
            "Axis #2:\n" +
            "{[Product].[All Products].[Drink].[*CTX_MEMBER_SEL~SUM], [Education Level].[*CTX_MEMBER_SEL~SUM]}\n" +
            "{[Product].[All Products].[Food].[*CTX_MEMBER_SEL~SUM], [Education Level].[*CTX_MEMBER_SEL~SUM]}\n" +
            "Row #0: 6,535.30\n" +
            "Row #1: 3,860.89\n";
        
        assertQueryReturns(query, fold(result));
    }
        
    public void testShouldUseGroupingFunctionOnPropertyTrueAndOnSupportedDB() {
        boolean oldValue = MondrianProperties.instance().EnableGroupingSets.get();
        MondrianProperties.instance().EnableGroupingSets.set(true);
        FastBatchingCellReader fbcr = new FastBatchingCellReader(salesCube) {
            boolean doesDBSupportGroupingSets() {
                return true;
            }
        };
        assertTrue(fbcr.shouldUseGroupingFunction());
        MondrianProperties.instance().EnableGroupingSets.set(oldValue);
    }

    public void testShouldUseGroupingFunctionOnPropertyTrueAndOnNonSupportedDB() {
        boolean oldValue = MondrianProperties.instance().EnableGroupingSets.get();
        MondrianProperties.instance().EnableGroupingSets.set(true);
        FastBatchingCellReader fbcr = new FastBatchingCellReader(salesCube) {
            boolean doesDBSupportGroupingSets() {
                return false;
            }
        };
        assertFalse(fbcr.shouldUseGroupingFunction());
        MondrianProperties.instance().EnableGroupingSets.set(oldValue);
    }

    public void testShouldUseGroupingFunctionOnPropertyFalseOnSupportedDB() {
        boolean oldValue = MondrianProperties.instance().EnableGroupingSets.get();
        MondrianProperties.instance().EnableGroupingSets.set(false);
        FastBatchingCellReader fbcr = new FastBatchingCellReader(salesCube) {
            boolean doesDBSupportGroupingSets() {
                return true;
            }
        };
        assertFalse(fbcr.shouldUseGroupingFunction());
        MondrianProperties.instance().EnableGroupingSets.set(oldValue);
    }

    public void testShouldUseGroupingFunctionOnPropertyFalseOnNonSupportedDB() {
        boolean oldValue = MondrianProperties.instance().EnableGroupingSets.get();
        MondrianProperties.instance().EnableGroupingSets.set(false);
        FastBatchingCellReader fbcr = new FastBatchingCellReader(salesCube) {
            boolean doesDBSupportGroupingSets() {
                return false;
            }
        };
        assertFalse(fbcr.shouldUseGroupingFunction());
        MondrianProperties.instance().EnableGroupingSets.set(oldValue);
    }

    public void testDoesDBSupportGroupingSets() {
        final SqlQuery.Dialect dialect = getTestContext().getDialect();
        FastBatchingCellReader fbcr = new FastBatchingCellReader(salesCube) {
            SqlQuery.Dialect getDialect() {
                return dialect;
            }
        };
        if (dialect.isOracle() || dialect.isTeradata() || dialect.isDB2()) {
            assertTrue(fbcr.doesDBSupportGroupingSets());
        } else {
            assertFalse(fbcr.doesDBSupportGroupingSets());
        }
    }

    private RolapCube salesCube =
        (RolapCube) getTestContext().getConnection().getSchemaReader().getCubes()[0];

    public void testGroupBatchesForNonGroupableBatchesWithSorting() {
        FastBatchingCellReader fbcr = new FastBatchingCellReader(salesCube);
        FastBatchingCellReader.Batch genderBatch = fbcr.new Batch(
            createRequest(cubeNameSales, measureUnitSales,
                "customer", "gender", "F"));
        FastBatchingCellReader.Batch maritalStatusBatch = fbcr.new Batch(
            createRequest(cubeNameSales, measureUnitSales,
                "customer", "marital_status", "M"));
        ArrayList<FastBatchingCellReader.Batch> batchList =
            new ArrayList<FastBatchingCellReader.Batch>();
        batchList.add(genderBatch);
        batchList.add(maritalStatusBatch);
        List<FastBatchingCellReader.CompositeBatch> groupedBatches =
            fbcr.groupBatches(batchList);
        assertEquals(batchList.size(), groupedBatches.size());
        assertEquals(genderBatch, groupedBatches.get(0).detailedBatch);
        assertEquals(maritalStatusBatch, groupedBatches.get(1).detailedBatch);
    }

    public void testGroupBatchesForNonGroupableBatchesWithConstraints() {
        FastBatchingCellReader fbcr = new FastBatchingCellReader(salesCube);
        List<String[]> compoundMembers = new ArrayList<String[]>();
        compoundMembers.add(new String[] {"USA", "CA"});
        compoundMembers.add(new String[] {"Canada", "BC"});
        CellRequestConstraint constraint =
            makeConstraintCountryState(compoundMembers);

        FastBatchingCellReader.Batch genderBatch = fbcr.new Batch(
            createRequest(cubeNameSales, measureUnitSales,
                "customer", "gender", "F", constraint));
        FastBatchingCellReader.Batch maritalStatusBatch = fbcr.new Batch(
            createRequest(cubeNameSales, measureUnitSales,
                "customer", "marital_status", "M", constraint));
        ArrayList<FastBatchingCellReader.Batch> batchList =
            new ArrayList<FastBatchingCellReader.Batch>();
        batchList.add(genderBatch);
        batchList.add(maritalStatusBatch);
        List<FastBatchingCellReader.CompositeBatch> groupedBatches =
            fbcr.groupBatches(batchList);
        assertEquals(batchList.size(), groupedBatches.size());
        assertEquals(genderBatch, groupedBatches.get(0).detailedBatch);
        assertEquals(maritalStatusBatch, groupedBatches.get(1).detailedBatch);
    }

    public void testGroupBatchesForGroupableBatches() {
        FastBatchingCellReader fbcr = new FastBatchingCellReader(salesCube);
        FastBatchingCellReader.Batch genderBatch = fbcr.new Batch(
            createRequest(cubeNameSales, measureUnitSales,
                "customer", "gender", "F")) {
            boolean canBatch(FastBatchingCellReader.Batch other) {
                return false;
            }
        };
        FastBatchingCellReader.Batch superBatch = fbcr.new Batch(
            createRequest(cubeNameSales, measureUnitSales,
                new String[0], new String[0], new String[0])) {
            boolean canBatch(FastBatchingCellReader.Batch batch) {
                return true;
            }
        };
        ArrayList<FastBatchingCellReader.Batch> batchList =
            new ArrayList<FastBatchingCellReader.Batch>();
        batchList.add(genderBatch);
        batchList.add(superBatch);
        List<FastBatchingCellReader.CompositeBatch> groupedBatches =
            fbcr.groupBatches(batchList);
        assertEquals(1, groupedBatches.size());
        assertEquals(superBatch, groupedBatches.get(0).detailedBatch);
        assertTrue(
            groupedBatches.get(0).summaryBatches.contains(genderBatch));
    }

    public void testGroupBatchesForGroupableBatchesAndNonGroupableBatches() {
        FastBatchingCellReader fbcr = new FastBatchingCellReader(salesCube);
        final FastBatchingCellReader.Batch group1Agg2 = fbcr.new Batch(
            createRequest(cubeNameSales, measureUnitSales,
                "customer", "gender", "F")) {
            boolean canBatch(FastBatchingCellReader.Batch batch) {
                return false;
            }
        };
        final FastBatchingCellReader.Batch group1Agg1 = fbcr.new Batch(
            createRequest(cubeNameSales, measureUnitSales,
                "customer", "country", "F")) {
            boolean canBatch(FastBatchingCellReader.Batch batch) {
                return batch.equals(group1Agg2);
            }
        };
        FastBatchingCellReader.Batch group1Detailed = fbcr.new Batch(
            createRequest(cubeNameSales, measureUnitSales,
                new String[0], new String[0], new String[0])) {
            boolean canBatch(FastBatchingCellReader.Batch batch) {
                return batch.equals(group1Agg1);
            }
        };

        final FastBatchingCellReader.Batch group2Agg1 = fbcr.new Batch(
            createRequest(cubeNameSales, measureUnitSales,
                "customer", "education", "F")) {
            boolean canBatch(FastBatchingCellReader.Batch batch) {
                return false;
            }
        };
        FastBatchingCellReader.Batch group2Detailed = fbcr.new Batch(
            createRequest(cubeNameSales, measureUnitSales,
                "customer", "yearly_income", "")) {
            boolean canBatch(FastBatchingCellReader.Batch batch) {
                return batch.equals(group2Agg1);
            }
        };
        ArrayList<FastBatchingCellReader.Batch> batchList =
            new ArrayList<FastBatchingCellReader.Batch>();
        batchList.add(group1Agg1);
        batchList.add(group1Agg2);
        batchList.add(group1Detailed);
        batchList.add(group2Agg1);
        batchList.add(group2Detailed);
        List<FastBatchingCellReader.CompositeBatch> groupedBatches =
            fbcr.groupBatches(batchList);
        assertEquals(2, groupedBatches.size());
        assertEquals(group1Detailed, groupedBatches.get(0).detailedBatch);
        assertTrue(groupedBatches.get(0).summaryBatches.contains(group1Agg1));
        assertTrue(groupedBatches.get(0).summaryBatches.contains(group1Agg2));
        assertEquals(group2Detailed, groupedBatches.get(1).detailedBatch);
        assertTrue(groupedBatches.get(1).summaryBatches.contains(group2Agg1));
    }

    public void testGroupBatchesForTwoSetOfGroupableBatches() {
        String[] fieldValuesStoreType = {"Deluxe Supermarket", "Gourmet Supermarket", "HeadQuarters",
                "Mid-Size Grocery", "Small Grocery", "Supermarket"};
        String fieldStoreType = "store_type";
        String tableStore = "store";

        String[] fieldValuesWarehouseCountry = {"Canada", "Mexico", "USA"};
        String fieldWarehouseCountry = "warehouse_country";
        String tableWarehouse = "warehouse";

        FastBatchingCellReader fbcr = new FastBatchingCellReader(salesCube);
        FastBatchingCellReader.Batch batch1RollupOnGender =
                createBatch(fbcr,
                        new String[]{tableTime, tableStore, tableProductClass},
                        new String[]{fieldYear, fieldStoreType, fieldProductFamily},
                        new String[][]{fieldValuesYear, fieldValuesStoreType, fieldValuesProductFamily},
                        cubeNameSales,
                        measureUnitSales);

        FastBatchingCellReader.Batch batch1RollupOnGenderAndProductDepartment =
                createBatch(fbcr, new String[]{tableTime, tableProductClass},
                        new String[]{fieldYear, fieldProductFamily},
                        new String[][]{fieldValuesYear, fieldValuesProductFamily},
                        cubeNameSales, measureUnitSales);

        FastBatchingCellReader.Batch batch1RollupOnStoreTypeAndProductDepartment =
                createBatch(fbcr,
                        new String[]{tableTime, tableCustomer},
                        new String[]{fieldYear, fieldGender},
                        new String[][]{fieldValuesYear, fieldValuesGender},
                        cubeNameSales, measureUnitSales);

        FastBatchingCellReader.Batch batch1Detailed =
                createBatch(fbcr,
                        new String[]{tableTime, tableStore,
                                tableProductClass, tableCustomer},
                        new String[]{fieldYear, fieldStoreType, fieldProductFamily,
                                fieldGender},
                        new String[][]{fieldValuesYear, fieldValuesStoreType,
                                fieldValuesProductFamily,
                                fieldValuesGender},
                        cubeNameSales,
                        measureUnitSales);


        String warehouseCube = "Warehouse";
        String measure2 = "[Measures].[Warehouse Sales]";
        FastBatchingCellReader.Batch batch2RollupOnStoreType =
                createBatch(fbcr,
                        new String[]{tableWarehouse, tableTime, tableProductClass
                        }, new String[]{fieldWarehouseCountry, fieldYear,
                        fieldProductFamily},
                        new String[][]{fieldValuesWarehouseCountry, fieldValuesYear,
                                fieldValuesProductFamily}, warehouseCube,
                        measure2);

        FastBatchingCellReader.Batch batch2RollupOnStoreTypeAndWareHouseCountry =
                createBatch(fbcr, new String[]{tableTime,tableProductClass},
                        new String[]{fieldYear,fieldProductFamily},
                        new String[][]{fieldValuesYear,fieldValuesProductFamily},
                        warehouseCube, measure2);

        FastBatchingCellReader.Batch batch2RollupOnProductFamilyAndWareHouseCountry =
                createBatch(fbcr,
                        new String[]{tableTime, tableStore},
                        new String[]{fieldYear, fieldStoreType},
                        new String[][]{fieldValuesYear, fieldValuesStoreType},
                        warehouseCube, measure2);

        FastBatchingCellReader.Batch batch2Detailed =
                createBatch(fbcr,
                        new String[]{tableWarehouse, tableTime, tableStore, tableProductClass},
                        new String[]{fieldWarehouseCountry, fieldYear, fieldStoreType, fieldProductFamily},
                        new String[][]{fieldValuesWarehouseCountry, fieldValuesYear, fieldValuesStoreType,
                                fieldValuesProductFamily},
                        warehouseCube,
                        measure2);

        List<FastBatchingCellReader.Batch> batchList = new ArrayList<FastBatchingCellReader.Batch>();

        batchList.add(batch1RollupOnGender);
        batchList.add(batch2RollupOnStoreType);
        batchList.add(batch2RollupOnStoreTypeAndWareHouseCountry);
        batchList.add(batch2RollupOnProductFamilyAndWareHouseCountry);
        batchList.add(batch1RollupOnGenderAndProductDepartment);
        batchList.add(batch1RollupOnStoreTypeAndProductDepartment);
        batchList.add(batch2Detailed);
        batchList.add(batch1Detailed);
        List<FastBatchingCellReader.CompositeBatch> groupedBatches =
                fbcr.groupBatches(batchList);
        if ((MondrianProperties.instance().UseAggregates.get()
                && MondrianProperties.instance().ReadAggregates.get())
            || Util.Retrowoven)
        {
            assertEquals(4, groupedBatches.size());
        } else {
            assertEquals(2, groupedBatches.size());
        }

    }

    public void testAddToCompositeBatchForBothBatchesNotPartOfCompositeBatch() {
        FastBatchingCellReader fbcr = new FastBatchingCellReader(salesCube);
        FastBatchingCellReader.Batch batch1 = fbcr.new Batch(
            createRequest(cubeNameSales, measureUnitSales,
                "customer", "country", "F"));
        FastBatchingCellReader.Batch batch2 = fbcr.new Batch(
            createRequest(cubeNameSales, measureUnitSales,
                "customer", "gender", "F"));
        Map<AggregationKey, FastBatchingCellReader.CompositeBatch> batchGroups =
            new HashMap<AggregationKey, FastBatchingCellReader.CompositeBatch>();
        fbcr.addToCompositeBatch(batchGroups, batch1, batch2);
        assertEquals(1, batchGroups.size());
        FastBatchingCellReader.CompositeBatch compositeBatch =
            batchGroups.get(batch1.batchKey);
        assertEquals(batch1, compositeBatch.detailedBatch);
        assertEquals(1, compositeBatch.summaryBatches.size());
        assertTrue(compositeBatch.summaryBatches.contains(batch2));
    }

    public void testAddToCompositeBatchForDetailedBatchAlreadyPartOfACompositeBatch() {
        FastBatchingCellReader fbcr = new FastBatchingCellReader(salesCube);
        FastBatchingCellReader.Batch detailedBatch = fbcr.new Batch(
            createRequest(cubeNameSales, measureUnitSales,
                "customer", "country", "F"));
        FastBatchingCellReader.Batch aggBatch1 = fbcr.new Batch(
            createRequest(cubeNameSales, measureUnitSales,
                "customer", "gender", "F"));
        FastBatchingCellReader.Batch aggBatchAlreadyInCompisite =
            fbcr.new Batch(createRequest(cubeNameSales,
                measureUnitSales, "customer", "gender", "F"));
        Map<AggregationKey, FastBatchingCellReader.CompositeBatch> batchGroups =
            new HashMap<AggregationKey, FastBatchingCellReader.CompositeBatch>();
        FastBatchingCellReader.CompositeBatch existingCompositeBatch =
            fbcr.new CompositeBatch(detailedBatch);
        existingCompositeBatch.add(aggBatchAlreadyInCompisite);
        batchGroups.put(detailedBatch.batchKey, existingCompositeBatch);

        fbcr.addToCompositeBatch(batchGroups, detailedBatch, aggBatch1);

        assertEquals(1, batchGroups.size());
        FastBatchingCellReader.CompositeBatch compositeBatch =
            batchGroups.get(detailedBatch.batchKey);
        assertEquals(detailedBatch, compositeBatch.detailedBatch);
        assertEquals(2, compositeBatch.summaryBatches.size());
        assertTrue(compositeBatch.summaryBatches.contains(aggBatch1));
        assertTrue(compositeBatch.summaryBatches.contains(
            aggBatchAlreadyInCompisite));
    }

    public void testAddToCompositeBatchForAggregationBatchAlreadyPartOfACompositeBatch() {
        FastBatchingCellReader fbcr = new FastBatchingCellReader(salesCube);
        FastBatchingCellReader.Batch detailedBatch = fbcr.new Batch(
            createRequest(cubeNameSales, measureUnitSales,
                "customer", "country", "F"));
        FastBatchingCellReader.Batch aggBatchToAddToDetailedBatch =
            fbcr.new Batch(createRequest(cubeNameSales,
                measureUnitSales, "customer", "gender", "F"));
        FastBatchingCellReader.Batch aggBatchAlreadyInComposite =
            fbcr.new Batch(createRequest(cubeNameSales,
                measureUnitSales, "customer", "city", "F"));
        Map<AggregationKey, FastBatchingCellReader.CompositeBatch> batchGroups =
            new HashMap<AggregationKey, FastBatchingCellReader.CompositeBatch>();
        FastBatchingCellReader.CompositeBatch existingCompositeBatch =
            fbcr.new CompositeBatch(aggBatchToAddToDetailedBatch);
        existingCompositeBatch.add(aggBatchAlreadyInComposite);
        batchGroups.put(aggBatchToAddToDetailedBatch.batchKey,
            existingCompositeBatch);

        fbcr.addToCompositeBatch(batchGroups, detailedBatch,
            aggBatchToAddToDetailedBatch);

        assertEquals(1, batchGroups.size());
        FastBatchingCellReader.CompositeBatch compositeBatch =
            batchGroups.get(detailedBatch.batchKey);
        assertEquals(detailedBatch, compositeBatch.detailedBatch);
        assertEquals(2, compositeBatch.summaryBatches.size());
        assertTrue(compositeBatch.summaryBatches.contains(
            aggBatchToAddToDetailedBatch));
        assertTrue(compositeBatch.summaryBatches.contains(
            aggBatchAlreadyInComposite));
    }

    public void testAddToCompositeBatchForBothBatchAlreadyPartOfACompositeBatch() {
        FastBatchingCellReader fbcr = new FastBatchingCellReader(salesCube);
        FastBatchingCellReader.Batch detailedBatch = fbcr.new Batch(
            createRequest(cubeNameSales, measureUnitSales,
                "customer", "country", "F"));
        FastBatchingCellReader.Batch aggBatchToAddToDetailedBatch =
            fbcr.new Batch(createRequest(cubeNameSales,
                measureUnitSales, "customer", "gender", "F"));
        FastBatchingCellReader.Batch aggBatchAlreadyInCompositeOfAgg =
            fbcr.new Batch(createRequest(cubeNameSales,
                measureUnitSales, "customer", "city", "F"));
        FastBatchingCellReader.Batch aggBatchAlreadyInCompositeOfDetail =
            fbcr.new Batch(createRequest(cubeNameSales,
                measureUnitSales, "customer", "state_province",
                "F"));

        Map<AggregationKey, FastBatchingCellReader.CompositeBatch> batchGroups =
            new HashMap<AggregationKey, FastBatchingCellReader.CompositeBatch>();
        FastBatchingCellReader.CompositeBatch existingAggCompositeBatch =
            fbcr.new CompositeBatch(aggBatchToAddToDetailedBatch);
        existingAggCompositeBatch.add(aggBatchAlreadyInCompositeOfAgg);
        batchGroups.put(aggBatchToAddToDetailedBatch.batchKey,
            existingAggCompositeBatch);

        FastBatchingCellReader.CompositeBatch existingCompositeBatch =
            fbcr.new CompositeBatch(detailedBatch);
        existingCompositeBatch.add(aggBatchAlreadyInCompositeOfDetail);
        batchGroups.put(detailedBatch.batchKey, existingCompositeBatch);

        fbcr.addToCompositeBatch(batchGroups, detailedBatch,
            aggBatchToAddToDetailedBatch);

        assertEquals(1, batchGroups.size());
        FastBatchingCellReader.CompositeBatch compositeBatch =
            batchGroups.get(detailedBatch.batchKey);
        assertEquals(detailedBatch, compositeBatch.detailedBatch);
        assertEquals(3, compositeBatch.summaryBatches.size());
        assertTrue(compositeBatch.summaryBatches.contains(
            aggBatchToAddToDetailedBatch));
        assertTrue(compositeBatch.summaryBatches.contains(
            aggBatchAlreadyInCompositeOfAgg));
        assertTrue(compositeBatch.summaryBatches.contains(
            aggBatchAlreadyInCompositeOfDetail));
    }

    public void testCanBatchForBatchWithSuperSetOfContraintColumnBitKeyAndAllValuesForAdditionalCondition() {
        FastBatchingCellReader fbcr = new FastBatchingCellReader(salesCube);

        FastBatchingCellReader.Batch aggregationBatch =
            createBatch(fbcr,
                new String[]{tableTime, tableProductClass,
                    tableProductClass}, new String[]{fieldYear,
                fieldProductFamily, fieldProductDepartment},
                new String[][]{fieldValuesYear,
                    fieldValuesProductFamily,
                    fieldValueProductDepartment}, cubeNameSales,
                measureUnitSales);

        FastBatchingCellReader.Batch detailedBatch =
            createBatch(fbcr,
                new String[]{tableTime, tableProductClass,
                    tableProductClass, tableCustomer},
                new String[]{fieldYear, fieldProductFamily,
                    fieldProductDepartment, fieldGender},
                new String[][]{fieldValuesYear,
                    fieldValuesProductFamily,
                    fieldValueProductDepartment, fieldValuesGender},
                cubeNameSales,
                measureUnitSales);

        assertTrue(detailedBatch.canBatch(aggregationBatch));
        assertFalse(aggregationBatch.canBatch(detailedBatch));
    }

    public void testCanBatchForBatchWithConstraint() {
        FastBatchingCellReader fbcr = new FastBatchingCellReader(salesCube);

        List<String[]> compoundMembers = new ArrayList<String[]>();
        compoundMembers.add(new String[] {"USA", "CA"});
        compoundMembers.add(new String[] {"Canada", "BC"});
        CellRequestConstraint constraint =
            makeConstraintCountryState(compoundMembers);

        FastBatchingCellReader.Batch aggregationBatch =
            createBatch(fbcr,
                new String[]{tableTime, tableProductClass,
                    tableProductClass}, new String[]{fieldYear,
                fieldProductFamily, fieldProductDepartment},
                new String[][]{fieldValuesYear,
                    fieldValuesProductFamily,
                    fieldValueProductDepartment}, cubeNameSales,
                measureUnitSales, constraint);

        FastBatchingCellReader.Batch detailedBatch =
            createBatch(fbcr,
                new String[]{tableTime, tableProductClass,
                    tableProductClass, tableCustomer},
                new String[]{fieldYear, fieldProductFamily,
                    fieldProductDepartment, fieldGender},
                new String[][]{fieldValuesYear,
                    fieldValuesProductFamily,
                    fieldValueProductDepartment, fieldValuesGender},
                cubeNameSales,
                measureUnitSales, constraint);

        assertTrue(detailedBatch.canBatch(aggregationBatch));
        assertFalse(aggregationBatch.canBatch(detailedBatch));
    }

    public void testCanBatchForBatchWithConstraint2() {
        FastBatchingCellReader fbcr = new FastBatchingCellReader(salesCube);

        List<String[]> compoundMembers1 = new ArrayList<String[]>();
        compoundMembers1.add(new String[] {"USA", "CA"});
        compoundMembers1.add(new String[] {"Canada", "BC"});
        CellRequestConstraint constraint1 =
            makeConstraintCountryState(compoundMembers1);

        // Different constraint will cause the Batch not to match.
        List<String[]> compoundMembers2 = new ArrayList<String[]>();
        compoundMembers2.add(new String[] {"USA", "CA"});
        compoundMembers2.add(new String[] {"USA", "OR"});
        CellRequestConstraint constraint2 =
            makeConstraintCountryState(compoundMembers2);

        FastBatchingCellReader.Batch aggregationBatch =
            createBatch(fbcr,
                new String[]{tableTime, tableProductClass,
                    tableProductClass}, new String[]{fieldYear,
                fieldProductFamily, fieldProductDepartment},
                new String[][]{fieldValuesYear,
                    fieldValuesProductFamily,
                    fieldValueProductDepartment}, cubeNameSales,
                measureUnitSales, constraint1);

        FastBatchingCellReader.Batch detailedBatch =
            createBatch(fbcr,
                new String[]{tableTime, tableProductClass,
                    tableProductClass, tableCustomer},
                new String[]{fieldYear, fieldProductFamily,
                    fieldProductDepartment, fieldGender},
                new String[][]{fieldValuesYear,
                    fieldValuesProductFamily,
                    fieldValueProductDepartment, fieldValuesGender},
                cubeNameSales,
                measureUnitSales, constraint2);

        assertTrue(detailedBatch.canBatch(aggregationBatch));
        assertFalse(aggregationBatch.canBatch(detailedBatch));
    }

    public void testCanBatchForBatchWithDistinctCountInDetailedBatch() {
        if (MondrianProperties.instance().UseAggregates.get() &&
            MondrianProperties.instance().ReadAggregates.get()) {
            FastBatchingCellReader fbcr = new FastBatchingCellReader(salesCube);

            FastBatchingCellReader.Batch aggregationBatch =
                createBatch(fbcr,
                    new String[]{tableTime, tableProductClass,
                        tableProductClass},
                    new String[]{fieldYear, fieldProductFamily,
                        fieldProductDepartment},
                    new String[][]{fieldValuesYear,
                        fieldValuesProductFamily,
                        fieldValueProductDepartment},
                    cubeNameSales, measureUnitSales);

            FastBatchingCellReader.Batch detailedBatch =
                createBatch(fbcr,
                    new String[]{tableTime, tableProductClass,
                        tableProductClass},
                    new String[]{fieldYear, fieldProductFamily,
                        fieldProductDepartment},
                    new String[][]{fieldValuesYear,
                        fieldValuesProductFamily,
                        fieldValueProductDepartment},
                    cubeNameSales,
                    "[Measures].[Customer Count]");

            assertFalse(detailedBatch.canBatch(aggregationBatch));
            assertFalse(aggregationBatch.canBatch(detailedBatch));
        }
    }

    public void testCanBatchForBatchWithDistinctCountInAggregateBatch() {
        if (MondrianProperties.instance().UseAggregates.get() &&
            MondrianProperties.instance().ReadAggregates.get()) {
            FastBatchingCellReader fbcr = new FastBatchingCellReader(salesCube);

            FastBatchingCellReader.Batch aggregationBatch =
                createBatch(fbcr,
                    new String[]{tableTime, tableProductClass,
                        tableProductClass}, new String[]{fieldYear,
                    fieldProductFamily, fieldProductDepartment},
                    new String[][]{fieldValuesYear,
                        fieldValuesProductFamily,
                        fieldValueProductDepartment}, cubeNameSales,
                    "[Measures].[Customer Count]");

            FastBatchingCellReader.Batch detailedBatch =
                createBatch(fbcr,
                    new String[]{tableTime, tableProductClass,
                        tableProductClass},
                    new String[]{fieldYear, fieldProductFamily,
                        fieldProductDepartment},
                    new String[][]{fieldValuesYear,
                        fieldValuesProductFamily,
                        fieldValueProductDepartment},
                    cubeNameSales, measureUnitSales);

            assertFalse(detailedBatch.canBatch(aggregationBatch));
            assertFalse(aggregationBatch.canBatch(detailedBatch));
        }
    }

    public void testCanBatchSummaryBatchWithDetailedBatchWithDistinctCount() {
        if (!MondrianProperties.instance().UseAggregates.get() &&
            !MondrianProperties.instance().ReadAggregates.get()) {
            FastBatchingCellReader fbcr = new FastBatchingCellReader(salesCube);

            FastBatchingCellReader.Batch aggregationBatch =
                createBatch(fbcr,
                    new String[]{tableTime}, new String[]{fieldYear},
                    new String[][]{fieldValuesYear}, cubeNameSales,
                    "[Measures].[Customer Count]");

            FastBatchingCellReader.Batch detailedBatch =
                createBatch(fbcr,
                    new String[]{tableTime, tableProductClass,
                        tableProductClass},
                    new String[]{fieldYear, fieldProductFamily,
                        fieldProductDepartment},
                    new String[][]{fieldValuesYear,
                        fieldValuesProductFamily,
                        fieldValueProductDepartment},
                    cubeNameSales, measureUnitSales);

            assertFalse(detailedBatch.canBatch(aggregationBatch));
            assertFalse(aggregationBatch.canBatch(detailedBatch));
        }
    }


    public void testCanBatchForBatchWithNonSuperSetOfContraintColumnBitKeyAndAllValuesForAdditionalCondition() {
        FastBatchingCellReader fbcr = new FastBatchingCellReader(salesCube);

        FastBatchingCellReader.Batch aggregationBatch =
            createBatch(fbcr,
                new String[]{tableTime, tableProductClass,
                    tableProductClass}, new String[]{fieldYear,
                fieldProductFamily, fieldProductDepartment},
                new String[][]{fieldValuesYear,
                    fieldValuesProductFamily,
                    fieldValueProductDepartment}, cubeNameSales,
                measureUnitSales);

        FastBatchingCellReader.Batch detailedBatch =
            createBatch(fbcr,
                new String[]{tableProductClass,
                    tableProductClass, tableCustomer},
                new String[]{fieldProductFamily,
                    fieldProductDepartment, fieldGender},
                new String[][]{fieldValuesProductFamily,
                    fieldValueProductDepartment, fieldValuesGender},
                cubeNameSales,
                measureUnitSales);

        assertFalse(detailedBatch.canBatch(aggregationBatch));
        assertFalse(aggregationBatch.canBatch(detailedBatch));
    }

    public void testCanBatchForBatchWithSuperSetOfContraintColumnBitKeyAndNOTAllValuesForAdditionalCondition() {
        FastBatchingCellReader fbcr = new FastBatchingCellReader(salesCube);

        FastBatchingCellReader.Batch aggregationBatch =
            createBatch(fbcr,
                new String[]{tableTime, tableProductClass,
                    tableProductClass}, new String[]{fieldYear,
                fieldProductFamily, fieldProductDepartment},
                new String[][]{fieldValuesYear,
                    fieldValuesProductFamily,
                    fieldValueProductDepartment}, cubeNameSales,
                measureUnitSales);

        FastBatchingCellReader.Batch detailedBatch =
            createBatch(fbcr,
                new String[]{tableTime, tableProductClass,
                    tableProductClass, tableCustomer},
                new String[]{fieldYear, fieldProductFamily,
                    fieldProductDepartment, fieldGender},
                new String[][]{fieldValuesYear,
                    fieldValuesProductFamily,
                    fieldValueProductDepartment, new String[]{"M"}},
                cubeNameSales,
                measureUnitSales);

        assertFalse(detailedBatch.canBatch(aggregationBatch));
        assertFalse(aggregationBatch.canBatch(detailedBatch));
    }

    public void testCanBatchForBatchesFromSameAggregationButDifferentRollupOption() {
        FastBatchingCellReader fbcr = new FastBatchingCellReader(salesCube);

        FastBatchingCellReader.Batch batch1 =
                createBatch(fbcr,
                        new String[]{tableTime}, new String[]{fieldYear},
                        new String[][]{fieldValuesYear}, cubeNameSales,
                        measureUnitSales);

        FastBatchingCellReader.Batch batch2 =
                createBatch(fbcr,
                        new String[]{tableTime, tableTime, tableTime},
                        new String[]{fieldYear, "quarter", "month_of_year"},
                        new String[][]{fieldValuesYear, {"Q1", "Q2", "Q3",
                                "Q4"},
                                {"1", "2", "3", "4", "5", "6", "7", "8",
                                        "9", "10", "11", "12"}},
                        cubeNameSales,
                        measureUnitSales);

        if (MondrianProperties.instance().UseAggregates.get()
                && MondrianProperties.instance().ReadAggregates.get()) {

            assertFalse(batch2.canBatch(batch1));
            assertFalse(batch1.canBatch(batch2));
        } else {
            if (!Util.Retrowoven) {
                // Under retroweaver, trigger which controls UseAggregates is
                // not working properly, which causes this test to fail.
                assertTrue(batch2.canBatch(batch1));
            }
        }
    }

    public void testCanBatchForBatchWithSuperSetOfContraintColumnBitKeyAndDifferentValuesForOverlappingColumns() {
        FastBatchingCellReader fbcr = new FastBatchingCellReader(salesCube);

        FastBatchingCellReader.Batch aggregationBatch =
            createBatch(fbcr,
                new String[]{tableTime, tableProductClass,
                    tableProductClass}, new String[]{fieldYear,
                fieldProductFamily, fieldProductDepartment},
                new String[][]{new String[]{"1997"},
                    fieldValuesProductFamily,
                    fieldValueProductDepartment}, cubeNameSales,
                measureUnitSales);

        FastBatchingCellReader.Batch detailedBatch =
            createBatch(fbcr,
                new String[]{tableTime, tableProductClass,
                    tableProductClass, tableCustomer},
                new String[]{fieldYear, fieldProductFamily,
                    fieldProductDepartment, fieldGender},
                new String[][]{new String[]{"1998"},
                    fieldValuesProductFamily,
                    fieldValueProductDepartment, fieldValuesGender},
                cubeNameSales,
                measureUnitSales);

        assertFalse(detailedBatch.canBatch(aggregationBatch));
        assertFalse(aggregationBatch.canBatch(detailedBatch));
    }

    public void testCanBatchForBatchWithDifferentAggregationTable() {
        if (getTestContext().getDialect().isTeradata()) {
            // On Teradata we don't create aggregate tables, so this test will
            // fail.
            return;
        }
        getTestContext().clearConnection();
        boolean useAggregates =
            MondrianProperties.instance().UseAggregates.get();
        boolean readAggregates =
            MondrianProperties.instance().ReadAggregates.get();
        MondrianProperties.instance().UseAggregates.set(true);
        MondrianProperties.instance().ReadAggregates.set(true);

        FastBatchingCellReader fbcr =
            new FastBatchingCellReader(getCube(cubeNameSales));

        FastBatchingCellReader.Batch summaryBatch =
            createBatch(fbcr,
                new String[]{tableTime}, new String[]{fieldYear},
                new String[][]{fieldValuesYear}, cubeNameSales,
                measureUnitSales);

        FastBatchingCellReader.Batch detailedBatch =
            createBatch(fbcr,
                new String[]{tableTime, tableCustomer},
                new String[]{fieldYear, fieldGender},
                new String[][]{fieldValuesYear, fieldValuesGender},
                cubeNameSales,
                measureUnitSales);

        assertFalse(detailedBatch.canBatch(summaryBatch));
        assertFalse(summaryBatch.canBatch(detailedBatch));
        MondrianProperties.instance().UseAggregates.set(useAggregates);
        MondrianProperties.instance().ReadAggregates.set(readAggregates);
    }

    public void testCannotBatchTwoBatchesAtTheSameLevel() {
        FastBatchingCellReader fbcr = new FastBatchingCellReader(salesCube);

        FastBatchingCellReader.Batch firstBatch =
            createBatch(fbcr,
                new String[]{tableTime, tableProductClass,
                    tableProductClass}, new String[]{fieldYear,
                fieldProductFamily, fieldProductDepartment},
                new String[][]{fieldValuesYear,
                    {"Food"},
                    fieldValueProductDepartment}, cubeNameSales,
                "[Measures].[Customer Count]");

        FastBatchingCellReader.Batch secondBatch =
            createBatch(fbcr,
                new String[]{tableTime, tableProductClass,
                    tableProductClass}, new String[]{fieldYear,
                fieldProductFamily, fieldProductDepartment},
                new String[][]{fieldValuesYear,
                    {"Drink"},
                    fieldValueProductDepartment}, cubeNameSales,
                "[Measures].[Customer Count]");

        assertFalse(firstBatch.canBatch(secondBatch));
        assertFalse(secondBatch.canBatch(firstBatch));
    }

    public void testCompositeBatchLoadAggregation() {
        FastBatchingCellReader fbcr = new FastBatchingCellReader(salesCube);

        FastBatchingCellReader.Batch summaryBatch =
            createBatch(fbcr,
                new String[]{tableTime, tableProductClass,
                    tableProductClass}, new String[]{fieldYear,
                fieldProductFamily, fieldProductDepartment},
                new String[][]{fieldValuesYear,
                    fieldValuesProductFamily,
                    fieldValueProductDepartment}, cubeNameSales,
                measureUnitSales);

        FastBatchingCellReader.Batch detailedBatch =
            createBatch(fbcr,
                new String[]{tableTime, tableProductClass,
                    tableProductClass, tableCustomer},
                new String[]{fieldYear, fieldProductFamily,
                    fieldProductDepartment, fieldGender},
                new String[][]{fieldValuesYear,
                    fieldValuesProductFamily,
                    fieldValueProductDepartment, fieldValuesGender},
                cubeNameSales,
                measureUnitSales);

        final List<GroupingSet> groupingSets =
            new ArrayList<GroupingSet>();
        FastBatchingCellReader.CompositeBatch compositeBatch =
            fbcr.new CompositeBatch(detailedBatch) {
                SegmentLoader getSegmentLoader() {
                    return new SegmentLoader() {
                        public void load(
                            List<GroupingSet> sets,
                            RolapAggregationManager.PinSet pinnedSegments,
                            List<StarPredicate> compoundPredicateList)
                        {
                            groupingSets.addAll(sets);
                            for (GroupingSet groupingSet : sets) {
                                groupingSet.setSegmentsFailed();
                            }
                        }
                    };
                }
            };

        compositeBatch.add(summaryBatch);

        compositeBatch.loadAggregation();

        assertEquals(2, groupingSets.size());
        assertEquals(detailedBatch.getConstrainedColumnsBitKey(),
            groupingSets.get(0).getLevelBitKey());
        assertEquals(summaryBatch.getConstrainedColumnsBitKey(),
            groupingSets.get(1).getLevelBitKey());

    }

    /**
     * Checks that in dialects that request it (e.g. LucidDB),
     * distinct aggregates based on SQL expressions,
     * e.g. <code>count(distinct "col1" + "col2"), count(distinct query)</code>,
     * are loaded individually, and separately from the other aggregates.
     */
    public void testLoadDistinctSqlMeasure() {
        // Some databases cannot handle scalar subqueries inside
        // count(distinct).
        final SqlQuery.Dialect dialect = getTestContext().getDialect();
        switch (SqlPattern.Dialect.get(dialect)) {
        case ORACLE:
            // Oracle gives 'feature not supported' in Express 10.2
        case ACCESS:
        case TERADATA:
            // Teradata gives "Syntax error: expected something between '(' and
            // the 'select' keyword." in 12.0.
            return;
        }

        String cube =
            "<Cube name=\"Warehouse2\">" +
            "   <Table name=\"warehouse\"/>" +
            "   <DimensionUsage name=\"Store Type\" source=\"Store Type\" foreignKey=\"stores_id\"/>" +
            "   <Measure name=\"Count Distinct of Warehouses (Large Owned)\" aggregator=\"distinct count\" formatString=\"#,##0\">" +
            "       <MeasureExpression>" +
            "       <SQL dialect=\"generic\">(select `warehouse_class`.`warehouse_class_id` AS `warehouse_class_id` from `warehouse_class` AS `warehouse_class` where `warehouse_class`.`warehouse_class_id` = `warehouse`.`warehouse_class_id` and `warehouse_class`.`description` = 'Large Owned')</SQL>" +
            "       </MeasureExpression>" +
            "   </Measure>" +
            "   <Measure name=\"Count Distinct of Warehouses (Large Independent)\" aggregator=\"distinct count\" formatString=\"#,##0\">" +
            "       <MeasureExpression>" +
            "       <SQL dialect=\"generic\">(select `warehouse_class`.`warehouse_class_id` AS `warehouse_class_id` from `warehouse_class` AS `warehouse_class` where `warehouse_class`.`warehouse_class_id` = `warehouse`.`warehouse_class_id` and `warehouse_class`.`description` = 'Large Independent')</SQL>" +
            "       </MeasureExpression>" +
            "   </Measure>" +
            "   <Measure name=\"Count All of Warehouses (Large Independent)\" aggregator=\"count\" formatString=\"#,##0\">" +
            "       <MeasureExpression>" +
            "           <SQL dialect=\"generic\">(select `warehouse_class`.`warehouse_class_id` AS `warehouse_class_id` from `warehouse_class` AS `warehouse_class` where `warehouse_class`.`warehouse_class_id` = `warehouse`.`warehouse_class_id` and `warehouse_class`.`description` = 'Large Independent')</SQL>" +
            "       </MeasureExpression>" +
            "   </Measure>" +
            "   <Measure name=\"Count Distinct Store+Warehouse\" aggregator=\"distinct count\" formatString=\"#,##0\">" +
            "       <MeasureExpression><SQL dialect=\"generic\">`store_id`+`warehouse_id`</SQL></MeasureExpression>" +
            "   </Measure>" +
            "   <Measure name=\"Count All Store+Warehouse\" aggregator=\"count\" formatString=\"#,##0\">" +
            "       <MeasureExpression><SQL dialect=\"generic\">`store_id`+`warehouse_id`</SQL></MeasureExpression>" +
            "   </Measure>" +
            "   <Measure name=\"Store Count\" column=\"stores_id\" aggregator=\"count\" formatString=\"#,###\"/>" +
            "</Cube>";
        cube = cube.replaceAll("`", dialect.getQuoteIdentifierString());
        if (dialect.isOracle()) {
            cube = cube.replaceAll(" AS ", " ");
        }

        String query =
            "select " +
            "   [Store Type].Children on rows, " +
            "   {[Measures].[Count Distinct of Warehouses (Large Owned)]," +
            "    [Measures].[Count Distinct of Warehouses (Large Independent)]," +
            "    [Measures].[Count All of Warehouses (Large Independent)]," +
            "    [Measures].[Count Distinct Store+Warehouse]," +
            "    [Measures].[Count All Store+Warehouse]," +
            "    [Measures].[Store Count]} on columns " +
            "from [Warehouse2]";

        TestContext testContext =
            TestContext.create(
                null,
                cube,
                null,
                null,
                null,
                null);

        testContext.assertQueryReturns(
            query,
            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Count Distinct of Warehouses (Large Owned)]}\n" +
                "{[Measures].[Count Distinct of Warehouses (Large Independent)]}\n" +
                "{[Measures].[Count All of Warehouses (Large Independent)]}\n" +
                "{[Measures].[Count Distinct Store+Warehouse]}\n" +
                "{[Measures].[Count All Store+Warehouse]}\n" +
                "{[Measures].[Store Count]}\n" +
                "Axis #2:\n" +
                "{[Store Type].[All Store Types].[Deluxe Supermarket]}\n" +
                "{[Store Type].[All Store Types].[Gourmet Supermarket]}\n" +
                "{[Store Type].[All Store Types].[HeadQuarters]}\n" +
                "{[Store Type].[All Store Types].[Mid-Size Grocery]}\n" +
                "{[Store Type].[All Store Types].[Small Grocery]}\n" +
                "{[Store Type].[All Store Types].[Supermarket]}\n" +
                "Row #0: 1\n" +
                "Row #0: 0\n" +
                "Row #0: 0\n" +
                "Row #0: 6\n" +
                "Row #0: 6\n" +
                "Row #0: 6\n" +
                "Row #1: 1\n" +
                "Row #1: 0\n" +
                "Row #1: 0\n" +
                "Row #1: 2\n" +
                "Row #1: 2\n" +
                "Row #1: 2\n" +
                "Row #2: \n" +
                "Row #2: \n" +
                "Row #2: \n" +
                "Row #2: \n" +
                "Row #2: \n" +
                "Row #2: \n" +
                "Row #3: 0\n" +
                "Row #3: 1\n" +
                "Row #3: 1\n" +
                "Row #3: 4\n" +
                "Row #3: 4\n" +
                "Row #3: 4\n" +
                "Row #4: 0\n" +
                "Row #4: 1\n" +
                "Row #4: 1\n" +
                "Row #4: 4\n" +
                "Row #4: 4\n" +
                "Row #4: 4\n" +
                "Row #5: 0\n" +
                "Row #5: 1\n" +
                "Row #5: 3\n" +
                "Row #5: 8\n" +
                "Row #5: 8\n" +
                "Row #5: 8\n"));

        String loadCountDistinct_luciddb1 =
            "select " +
            "\"store\".\"store_type\" as \"c0\", " +
            "count(distinct " +
            "(select \"warehouse_class\".\"warehouse_class_id\" AS \"warehouse_class_id\" " +
            "from \"warehouse_class\" AS \"warehouse_class\" " +
            "where \"warehouse_class\".\"warehouse_class_id\" = \"warehouse\".\"warehouse_class_id\" and \"warehouse_class\".\"description\" = 'Large Owned')) as \"m0\" " +
            "from \"store\" as \"store\", \"warehouse\" as \"warehouse\" " +
            "where \"warehouse\".\"stores_id\" = \"store\".\"store_id\" " +
            "group by \"store\".\"store_type\"";

        String loadCountDistinct_luciddb2 =
            "select " +
            "\"store\".\"store_type\" as \"c0\", " +
            "count(distinct " +
            "(select \"warehouse_class\".\"warehouse_class_id\" AS \"warehouse_class_id\" " +
            "from \"warehouse_class\" AS \"warehouse_class\" " +
            "where \"warehouse_class\".\"warehouse_class_id\" = \"warehouse\".\"warehouse_class_id\" and \"warehouse_class\".\"description\" = 'Large Independent')) as \"m0\" " +
            "from \"store\" as \"store\", \"warehouse\" as \"warehouse\" " +
            "where \"warehouse\".\"stores_id\" = \"store\".\"store_id\" " +
            "group by \"store\".\"store_type\"";

        String loadOtherAggs_luciddb =
            "select " +
            "\"store\".\"store_type\" as \"c0\", " +
            "count(" +
            "(select \"warehouse_class\".\"warehouse_class_id\" AS \"warehouse_class_id\" " +
            "from \"warehouse_class\" AS \"warehouse_class\" " +
            "where \"warehouse_class\".\"warehouse_class_id\" = \"warehouse\".\"warehouse_class_id\" and \"warehouse_class\".\"description\" = 'Large Independent')) as \"m0\", " +
            "count(distinct \"store_id\"+\"warehouse_id\") as \"m1\", " +
            "count(\"store_id\"+\"warehouse_id\") as \"m2\", " +
            "count(\"warehouse\".\"stores_id\") as \"m3\" " +
            "from \"store\" as \"store\", \"warehouse\" as \"warehouse\" " +
            "where \"warehouse\".\"stores_id\" = \"store\".\"store_id\" " +
            "group by \"store\".\"store_type\"";

        // Derby splits into multiple statements.
        String loadCountDistinct_derby1 = "select \"store\".\"store_type\" as \"c0\", count(distinct (select \"warehouse_class\".\"warehouse_class_id\" AS \"warehouse_class_id\" from \"warehouse_class\" AS \"warehouse_class\" where \"warehouse_class\".\"warehouse_class_id\" = \"warehouse\".\"warehouse_class_id\" and \"warehouse_class\".\"description\" = 'Large Owned')) as \"m0\" from \"store\" as \"store\", \"warehouse\" as \"warehouse\" where \"warehouse\".\"stores_id\" = \"store\".\"store_id\" group by \"store\".\"store_type\"";
            String loadCountDistinct_derby2 = "select \"store\".\"store_type\" as \"c0\", count(distinct (select \"warehouse_class\".\"warehouse_class_id\" AS \"warehouse_class_id\" from \"warehouse_class\" AS \"warehouse_class\" where \"warehouse_class\".\"warehouse_class_id\" = \"warehouse\".\"warehouse_class_id\" and \"warehouse_class\".\"description\" = 'Large Independent')) as \"m0\" from \"store\" as \"store\", \"warehouse\" as \"warehouse\" where \"warehouse\".\"stores_id\" = \"store\".\"store_id\" group by \"store\".\"store_type\"";
            String loadCountDistinct_derby3 = "select \"store\".\"store_type\" as \"c0\", count(distinct \"store_id\"+\"warehouse_id\") as \"m0\" from \"store\" as \"store\", \"warehouse\" as \"warehouse\" where \"warehouse\".\"stores_id\" = \"store\".\"store_id\" group by \"store\".\"store_type\"";
            String loadOtherAggs_derby = "select \"store\".\"store_type\" as \"c0\", count((select \"warehouse_class\".\"warehouse_class_id\" AS \"warehouse_class_id\" from \"warehouse_class\" AS \"warehouse_class\" where \"warehouse_class\".\"warehouse_class_id\" = \"warehouse\".\"warehouse_class_id\" and \"warehouse_class\".\"description\" = 'Large Independent')) as \"m0\", count(\"store_id\"+\"warehouse_id\") as \"m1\", count(\"warehouse\".\"stores_id\") as \"m2\" from \"store\" as \"store\", \"warehouse\" as \"warehouse\" where \"warehouse\".\"stores_id\" = \"store\".\"store_id\" group by \"store\".\"store_type\"";

        // MySQL does it in one statement.
        String load_mysql = "select"
            + " `store`.`store_type` as `c0`,"
            + " count(distinct (select `warehouse_class`.`warehouse_class_id` AS `warehouse_class_id` from `warehouse_class` AS `warehouse_class` where `warehouse_class`.`warehouse_class_id` = `warehouse`.`warehouse_class_id` and `warehouse_class`.`description` = 'Large Owned')) as `m0`,"
            + " count(distinct (select `warehouse_class`.`warehouse_class_id` AS `warehouse_class_id` from `warehouse_class` AS `warehouse_class` where `warehouse_class`.`warehouse_class_id` = `warehouse`.`warehouse_class_id` and `warehouse_class`.`description` = 'Large Independent')) as `m1`,"
            + " count((select `warehouse_class`.`warehouse_class_id` AS `warehouse_class_id` from `warehouse_class` AS `warehouse_class` where `warehouse_class`.`warehouse_class_id` = `warehouse`.`warehouse_class_id` and `warehouse_class`.`description` = 'Large Independent')) as `m2`,"
            + " count(distinct `store_id`+`warehouse_id`) as `m3`,"
            + " count(`store_id`+`warehouse_id`) as `m4`,"
            + " count(`warehouse`.`stores_id`) as `m5` "
            + "from `store` as `store`,"
            + " `warehouse` as `warehouse` "
            + "where `warehouse`.`stores_id` = `store`.`store_id` "
            + "group by `store`.`store_type`";

        SqlPattern[] patterns = {
            new SqlPattern(SqlPattern.Dialect.LUCIDDB, loadCountDistinct_luciddb1, loadCountDistinct_luciddb1),
            new SqlPattern(SqlPattern.Dialect.LUCIDDB, loadCountDistinct_luciddb2, loadCountDistinct_luciddb2),
            new SqlPattern(SqlPattern.Dialect.LUCIDDB, loadOtherAggs_luciddb, loadOtherAggs_luciddb),

            new SqlPattern(SqlPattern.Dialect.DERBY, loadCountDistinct_derby1, loadCountDistinct_derby1),
            new SqlPattern(SqlPattern.Dialect.DERBY, loadCountDistinct_derby2, loadCountDistinct_derby2),
            new SqlPattern(SqlPattern.Dialect.DERBY, loadCountDistinct_derby3, loadCountDistinct_derby3),
            new SqlPattern(SqlPattern.Dialect.DERBY, loadOtherAggs_derby, loadOtherAggs_derby),

            new SqlPattern(SqlPattern.Dialect.MYSQL, load_mysql, load_mysql),
        };

        assertQuerySql(testContext, query, patterns);
    }

    public void testAggregateDistinctCount() {
        // solve_order=1 says to aggregate [CA] and [OR] before computing their
        // sums
        assertQueryReturns(
            "WITH MEMBER [Time].[1997 Q1 plus Q2] AS 'AGGREGATE({[Time].[1997].[Q1], [Time].[1997].[Q2]})', solve_order=1\n" +
                "SELECT {[Measures].[Customer Count]} ON COLUMNS,\n" +
                "      {[Time].[1997].[Q1], [Time].[1997].[Q2], [Time].[1997 Q1 plus Q2]} ON ROWS\n" +
                "FROM Sales\n" +
                "WHERE ([Store].[USA].[CA])",
            fold("Axis #0:\n" +
                "{[Store].[All Stores].[USA].[CA]}\n" +
                "Axis #1:\n" +
                "{[Measures].[Customer Count]}\n" +
                "Axis #2:\n" +
                "{[Time].[1997].[Q1]}\n" +
                "{[Time].[1997].[Q2]}\n" +
                "{[Time].[1997 Q1 plus Q2]}\n" +
                "Row #0: 1,110\n" +
                "Row #1: 1,173\n" +
                "Row #2: 1,854\n"));
    }

    /**
     *  As {@link #testAggregateDistinctCount()}, but (a) calc member includes
     * members from different levels and (b) also display [unit sales].
     */
    public void testAggregateDistinctCount2() {
        assertQueryReturns(
            "WITH MEMBER [Time].[1997 Q1 plus July] AS\n" +
                " 'AGGREGATE({[Time].[1997].[Q1], [Time].[1997].[Q3].[7]})', solve_order=1\n" +
                "SELECT {[Measures].[Unit Sales], [Measures].[Customer Count]} ON COLUMNS,\n" +
                "      {[Time].[1997].[Q1],\n" +
                "       [Time].[1997].[Q2],\n" +
                "       [Time].[1997].[Q3].[7],\n" +
                "       [Time].[1997 Q1 plus July]} ON ROWS\n" +
                "FROM Sales\n" +
                "WHERE ([Store].[USA].[CA])",
            fold("Axis #0:\n" +
                "{[Store].[All Stores].[USA].[CA]}\n" +
                "Axis #1:\n" +
                "{[Measures].[Unit Sales]}\n" +
                "{[Measures].[Customer Count]}\n" +
                "Axis #2:\n" +
                "{[Time].[1997].[Q1]}\n" +
                "{[Time].[1997].[Q2]}\n" +
                "{[Time].[1997].[Q3].[7]}\n" +
                "{[Time].[1997 Q1 plus July]}\n" +
                "Row #0: 16,890\n" +
                "Row #0: 1,110\n" +
                "Row #1: 18,052\n" +
                "Row #1: 1,173\n" +
                "Row #2: 5,403\n" +
                "Row #2: 412\n" + // !!!
                "Row #3: 22,293\n" + // = 16,890 + 5,403
                "Row #3: 1,386\n")); // between 1,110 and 1,110 + 412
    }

    /**
     * As {@link #testAggregateDistinctCount2()}, but with two calc members
     * simultaneously.
     */
    public void testAggregateDistinctCount3() {
        String mdxQuery = "WITH\n" +
            "  MEMBER [Promotion Media].[TV plus Radio] AS 'AGGREGATE({[Promotion Media].[TV], [Promotion Media].[Radio]})', solve_order=1\n" +
            "  MEMBER [Time].[1997 Q1 plus July] AS 'AGGREGATE({[Time].[1997].[Q1], [Time].[1997].[Q3].[7]})', solve_order=1\n" +
            "SELECT {[Promotion Media].[TV plus Radio],\n" +
            "        [Promotion Media].[TV],\n" +
            "        [Promotion Media].[Radio]} ON COLUMNS,\n" +
            "       {[Time].[1997],\n" +
            "        [Time].[1997].[Q1],\n" +
            "        [Time].[1997 Q1 plus July]} ON ROWS\n" +
            "FROM Sales\n" +
            "WHERE [Measures].[Customer Count]";

        assertQueryReturns(
            mdxQuery,
            fold("Axis #0:\n" +
                "{[Measures].[Customer Count]}\n" +
                "Axis #1:\n" +
                "{[Promotion Media].[TV plus Radio]}\n" +
                "{[Promotion Media].[All Media].[TV]}\n" +
                "{[Promotion Media].[All Media].[Radio]}\n" +
                "Axis #2:\n" +
                "{[Time].[1997]}\n" +
                "{[Time].[1997].[Q1]}\n" +
                "{[Time].[1997 Q1 plus July]}\n" +
                "Row #0: 455\n" +
                "Row #0: 274\n" +
                "Row #0: 186\n" +
                "Row #1: 139\n" +
                "Row #1: 99\n" +
                "Row #1: 40\n" +
                "Row #2: 139\n" +
                "Row #2: 99\n" +
                "Row #2: 40\n"));

        // There are 9 cells in the result. 6 sql statements have to be issued to fetch all
        // of them, with each loading these cells:
        // (1) ([1997], [TV Plus radio])
        //
        // (2) ([1997], [TV])
        //     ([1997], [radio])
        //
        // (3) ([1997].[Q1], [TV Plus radio])
        //
        // (4) ([1997].[Q1], [TV])
        //     ([1997].[Q1], [radio])
        //
        // (5) ([1997 Q1 plus July], [TV Plus radio])
        //
        // (6) ([1997 Q1 Plus July], [TV])
        //     ([1997 Q1 Plus July], [radio])
        final String accessSql = "select count(`m0`) as `c0` " +
            "from (" +
            "select distinct `sales_fact_1997`.`customer_id` as `m0` " +
            "from `time_by_day` as `time_by_day`," +
            " `sales_fact_1997` as `sales_fact_1997`," +
            " `promotion` as `promotion` " +
            "where `sales_fact_1997`.`time_id` = `time_by_day`.`time_id` " +
            "and ((`time_by_day`.`quarter` = 'Q1' and `time_by_day`.`the_year` = 1997)" +
            " or (`time_by_day`.`month_of_year` = 7 and `time_by_day`.`quarter` = 'Q3' and `time_by_day`.`the_year` = 1997)) " +
            "and `sales_fact_1997`.`promotion_id` = `promotion`.`promotion_id` " +
            "and `promotion`.`media_type` in ('TV', 'Radio')) as `dummyname`";

        final String oracleSql;
        if (!isGroupingSetsSupported()) {
            oracleSql = "select " +
            "\"time_by_day\".\"the_year\" as \"c0\", \"time_by_day\".\"quarter\" as \"c1\", " +
            "\"promotion\".\"media_type\" as \"c2\", " +
            "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\" " +
            "from " +
            "\"time_by_day\" \"time_by_day\", \"sales_fact_1997\" \"sales_fact_1997\", " +
            "\"promotion\" \"promotion\" " +
            "where " +
            "\"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and " +
            "\"time_by_day\".\"the_year\" = 1997 and " +
            "\"time_by_day\".\"quarter\" = 'Q1' and " +
            "\"sales_fact_1997\".\"promotion_id\" = \"promotion\".\"promotion_id\" and " +
            "\"promotion\".\"media_type\" in ('Radio', 'TV') " +
            "group by " +
            "\"time_by_day\".\"the_year\", \"time_by_day\".\"quarter\", " +
            "\"promotion\".\"media_type\"";
        } else {
            oracleSql = "select "
                + "\"time_by_day\".\"the_year\" as \"c0\", "
                + "\"time_by_day\".\"quarter\" as \"c1\", "
                + "\"promotion\".\"media_type\" as \"c2\", "
                + "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\", "
                + "grouping(\"promotion\".\"media_type\") as \"g0\" "
                + "from \"time_by_day\" \"time_by_day\", "
                + "\"sales_fact_1997\" \"sales_fact_1997\", "
                + "\"promotion\" \"promotion\" "
                + "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" "
                + "and \"time_by_day\".\"the_year\" = 1997 "
                + "and \"time_by_day\".\"quarter\" = 'Q1' "
                + "and \"sales_fact_1997\".\"promotion_id\" = \"promotion\".\"promotion_id\" "
                + "and \"promotion\".\"media_type\" in ('Radio', 'TV') "
                + "group by grouping sets "
                + "((\"time_by_day\".\"the_year\",\"time_by_day\".\"quarter\",\"promotion\".\"media_type\"),"
                + "(\"time_by_day\".\"the_year\",\"time_by_day\".\"quarter\"))";
        }

        final String mysqlSql =
            "select " +
            "`time_by_day`.`the_year` as `c0`, `time_by_day`.`quarter` as `c1`, " +
            "`promotion`.`media_type` as `c2`, count(distinct `sales_fact_1997`.`customer_id`) as `m0` " +
            "from " +
            "`time_by_day` as `time_by_day`, `sales_fact_1997` as `sales_fact_1997`, " +
            "`promotion` as `promotion` " +
            "where " +
            "`sales_fact_1997`.`time_id` = `time_by_day`.`time_id` and " +
            "`time_by_day`.`the_year` = 1997 and `time_by_day`.`quarter` = 'Q1' and `" +
            "sales_fact_1997`.`promotion_id` = `promotion`.`promotion_id` and " +
            "`promotion`.`media_type` in ('Radio', 'TV') " +
            "group by " +
            "`time_by_day`.`the_year`, `time_by_day`.`quarter`, `promotion`.`media_type`";

        final String derbySql =
            "select " +
            "\"time_by_day\".\"the_year\" as \"c0\", \"time_by_day\".\"quarter\" as \"c1\", " +
            "\"promotion\".\"media_type\" as \"c2\", " +
            "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\" " +
            "from " +
            "\"time_by_day\" as \"time_by_day\", \"sales_fact_1997\" as \"sales_fact_1997\", " +
            "\"promotion\" as \"promotion\" " +
            "where " +
            "\"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and " +
            "\"time_by_day\".\"the_year\" = 1997 and \"time_by_day\".\"quarter\" = 'Q1' and " +
            "\"sales_fact_1997\".\"promotion_id\" = \"promotion\".\"promotion_id\" and " +
            "\"promotion\".\"media_type\" in ('Radio', 'TV') " +
            "group by " +
            "\"time_by_day\".\"the_year\", \"time_by_day\".\"quarter\", " +
            "\"promotion\".\"media_type\"";

        assertQuerySql(mdxQuery, new SqlPattern[] {
            new SqlPattern(SqlPattern.Dialect.ACCESS, accessSql, accessSql),
            new SqlPattern(SqlPattern.Dialect.ORACLE, oracleSql, oracleSql),
            new SqlPattern(SqlPattern.Dialect.MYSQL, mysqlSql, mysqlSql),
            new SqlPattern(SqlPattern.Dialect.DERBY, derbySql, derbySql)
        });
    }

    /**
     * Distinct count over aggregate member which contains overlapping
     * members. Need to count them twice for rollable measures such as
     * [Unit Sales], but not for distinct-count measures such as
     * [Customer Count].
     */
    public void testAggregateDistinctCount4() {
        // CA and USA are overlapping members
        final String mdxQuery = "WITH\n" +
            "  MEMBER [Store].[CA plus USA] AS 'AGGREGATE({[Store].[USA].[CA], [Store].[USA]})', solve_order=1\n" +
            "  MEMBER [Time].[Q1 plus July] AS 'AGGREGATE({[Time].[1997].[Q1], [Time].[1997].[Q3].[7]})', solve_order=1\n" +
            "SELECT {[Measures].[Customer Count], [Measures].[Unit Sales]} ON COLUMNS,\n" +
            "      {[Store].[CA plus USA]} * {[Time].[Q1 plus July]} ON ROWS\n" +
            "FROM Sales";

        String accessSql = "select count(`m0`) as `c0` " +
            "from (select distinct `sales_fact_1997`.`customer_id` as `m0` from `store` as `store`, " +
            "`sales_fact_1997` as `sales_fact_1997`, `time_by_day` as `time_by_day` " +
            "where `sales_fact_1997`.`store_id` = `store`.`store_id` and (`store`.`store_state` = 'CA' " +
            "or `store`.`store_country` = 'USA') and `sales_fact_1997`.`time_id` = `time_by_day`.`time_id` " +
            "and ((`time_by_day`.`quarter` = 'Q1' and `time_by_day`.`the_year` = 1997) " +
            "or (`time_by_day`.`month_of_year` = 7 and `time_by_day`.`quarter` = 'Q3' " +
            "and `time_by_day`.`the_year` = 1997))) as `dummyname`";

        String derbySql = "select count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\" " +
            "from \"store\" as \"store\", \"sales_fact_1997\" as \"sales_fact_1997\", " +
            "\"time_by_day\" as \"time_by_day\" where \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" " +
            "and (\"store\".\"store_state\" = 'CA' or \"store\".\"store_country\" = 'USA') " +
            "and \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" " +
            "and ((\"time_by_day\".\"quarter\" = 'Q1' and \"time_by_day\".\"the_year\" = 1997) " +
            "or (\"time_by_day\".\"month_of_year\" = 7 and \"time_by_day\".\"quarter\" = 'Q3' " +
            "and \"time_by_day\".\"the_year\" = 1997))"; 

        final String mysqlSql = "select count(distinct `sales_fact_1997`.`customer_id`) as `m0` " +
            "from `store` as `store`, `sales_fact_1997` as `sales_fact_1997`, " +
            "`time_by_day` as `time_by_day` where `sales_fact_1997`.`store_id` = `store`.`store_id` " +
            "and (`store`.`store_state` = 'CA' or `store`.`store_country` = 'USA') " +
            "and `sales_fact_1997`.`time_id` = `time_by_day`.`time_id` " +
            "and ((`time_by_day`.`month_of_year` = 7 and `time_by_day`.`quarter` = 'Q3' " +
            "and `time_by_day`.`the_year` = 1997) " +
            "or (`time_by_day`.`quarter` = 'Q1' and `time_by_day`.`the_year` = 1997))";

        assertQuerySql(mdxQuery, new SqlPattern[] {
            new SqlPattern(SqlPattern.Dialect.ACCESS, accessSql, accessSql),
            new SqlPattern(SqlPattern.Dialect.DERBY, derbySql, derbySql),
            new SqlPattern(SqlPattern.Dialect.MYSQL, mysqlSql, mysqlSql)
        });

        assertQueryReturns(
            mdxQuery,
            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Customer Count]}\n" +
                "{[Measures].[Unit Sales]}\n" +
                "Axis #2:\n" +
                "{[Store].[CA plus USA], [Time].[Q1 plus July]}\n" +
                "Row #0: 3,505\n" +
                "Row #0: 112,347\n"));
    }

    /**
     * Fix a problem when genergating predicates for distinct count aggregate loading
     * and using the aggregate function in the slicer.
     */
    public void testAggregateDistinctCount5() {
        String query =
            "With " +
            "Set [Products] as " +
            " '{[Product].[All Products].[Drink], " +
            "   [Product].[All Products].[Food], " +
            "   [Product].[All Products].[Non-Consumable]}' " +
            "Member [Product].[Selected Products] as " +
            " 'Aggregate([Products])', SOLVE_ORDER=2 " +
            "Select " +
            " {[Store].[Store State].Members} on rows, " +
            " {[Measures].[Customer Count]} on columns " +
            "From [Sales] " +
            "Where ([Product].[Selected Products])";

        String derbySql =
            "select \"store\".\"store_state\" as \"c0\", " +
            "\"time_by_day\".\"the_year\" as \"c1\", " +
            "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\" " +
            "from \"store\" as \"store\", \"sales_fact_1997\" as \"sales_fact_1997\", " +
            "\"time_by_day\" as \"time_by_day\" " +
            "where \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" " +
            "and \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" " +
            "and \"time_by_day\".\"the_year\" = 1997 " +
            "group by \"store\".\"store_state\", \"time_by_day\".\"the_year\"";

        String mysqlSql =
            "select `store`.`store_state` as `c0`, `time_by_day`.`the_year` as `c1`, " +
            "count(distinct `sales_fact_1997`.`customer_id`) as `m0` " +
            "from `store` as `store`, `sales_fact_1997` as `sales_fact_1997`, " +
            "`time_by_day` as `time_by_day` " +
            "where `sales_fact_1997`.`store_id` = `store`.`store_id` " +
            "and `sales_fact_1997`.`time_id` = `time_by_day`.`time_id` " +
            "and `time_by_day`.`the_year` = 1997 " +
            "group by `store`.`store_state`, `time_by_day`.`the_year`";

        SqlPattern[] patterns = {
            new SqlPattern(SqlPattern.Dialect.DERBY, derbySql, derbySql),
            new SqlPattern(SqlPattern.Dialect.MYSQL, mysqlSql, mysqlSql)};

        assertQuerySql(query, patterns);
    }

    /*
     * Test for multiple members on different levels within the same hierarchy.
     */
    public void testAggregateDistinctCount6() {
        // CA and USA are overlapping members
        final String mdxQuery =
            "WITH " +
            " MEMBER [Store].[Select Region] AS " +
            " 'AGGREGATE({[Store].[USA].[CA], [Store].[Mexico], [Store].[Canada], [Store].[USA].[OR]})', solve_order=1\n" +
            " MEMBER [Time].[Select Time Period] AS " +
            " 'AGGREGATE({[Time].[1997].[Q1], [Time].[1997].[Q3].[7], [Time].[1997].[Q4], [Time].[1997]})', solve_order=1\n" +
            "SELECT {[Measures].[Customer Count], [Measures].[Unit Sales]} ON COLUMNS,\n" +
            "      {[Store].[Select Region]} * {[Time].[Select Time Period]} ON ROWS\n" +
            "FROM Sales";

        String derbySql = "select count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\" " +
            "from \"store\" as \"store\", \"sales_fact_1997\" as \"sales_fact_1997\", \"time_by_day\" " +
            "as \"time_by_day\" where \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" " +
            "and (\"store\".\"store_state\" in ('CA', 'OR') or \"store\".\"store_country\" in ('Mexico', 'Canada')) " +
            "and \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and (((\"time_by_day\".\"quarter\" = 'Q1' " +
            "and \"time_by_day\".\"the_year\" = 1997) or (\"time_by_day\".\"quarter\" = 'Q4' " +
            "and \"time_by_day\".\"the_year\" = 1997)) or (\"time_by_day\".\"month_of_year\" = 7 " +
            "and \"time_by_day\".\"quarter\" = 'Q3' and \"time_by_day\".\"the_year\" = 1997) " +
            "or \"time_by_day\".\"the_year\" = 1997)";

        final String mysqlSql = "select count(distinct `sales_fact_1997`.`customer_id`) as `m0` " +
            "from `store` as `store`, `sales_fact_1997` as `sales_fact_1997`, " +
            "`time_by_day` as `time_by_day` where `sales_fact_1997`.`store_id` = `store`.`store_id` " +
            "and (`store`.`store_state` in ('CA', 'OR') or `store`.`store_country` in ('Mexico', 'Canada')) " +
            "and `sales_fact_1997`.`time_id` = `time_by_day`.`time_id` and ((`time_by_day`.`month_of_year` = 7 " +
            "and `time_by_day`.`quarter` = 'Q3' and `time_by_day`.`the_year` = 1997) " +
            "or (((`time_by_day`.`the_year`, `time_by_day`.`quarter`) in ((1997, 'Q1'), (1997, 'Q4')))) " +
            "or `time_by_day`.`the_year` = 1997)";

        assertQuerySql(mdxQuery, new SqlPattern[] {
            new SqlPattern(SqlPattern.Dialect.DERBY, derbySql, derbySql),
            new SqlPattern(SqlPattern.Dialect.MYSQL, mysqlSql, mysqlSql)
        });

        assertQueryReturns(
            mdxQuery,
            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Customer Count]}\n" +
                "{[Measures].[Unit Sales]}\n" +
                "Axis #2:\n" +
                "{[Store].[Select Region], [Time].[Select Time Period]}\n" +
                "Row #0: 3,753\n" +
                "Row #0: 229,496\n"));
    }

    /*
     * Test case for bug 1785406 to fix "query already contains alias" exception.
     *
     * Note: 1785406 is a regression from checkin 9710. Code changes made in 9710 is no longer
     * in use(and removed). So this bug will not occur; however, keeping the test case here to
     * get some coverage for a query with a slicer.
     */
    public void testDistinctCountBug1785406() {
        String query = "With \n" +
                "Set [*BASE_MEMBERS_Product] as {[Product].[All Products].[Food].[Deli]}\n" +
                "Set [*BASE_MEMBERS_Store] as {[Store].[All Stores].[USA].[WA]}\n" +
                "Member [Product].[*CTX_MEMBER_SEL~SUM] As Aggregate([*BASE_MEMBERS_Product])\n" +
                "Select\n" +
                "{[Measures].[Customer Count]} on columns,\n" +
                "NonEmptyCrossJoin([*BASE_MEMBERS_Store],{([Product].[*CTX_MEMBER_SEL~SUM])})\n" +
                "on rows\n" +
                "From [Sales]\n" +
                "where ([Time].[1997])";

        String expectedResult = "Axis #0:\n" +
                "{[Time].[1997]}\n" +
                "Axis #1:\n" +
                "{[Measures].[Customer Count]}\n" +
                "Axis #2:\n" +
                "{[Store].[All Stores].[USA].[WA], [Product].[*CTX_MEMBER_SEL~SUM]}\n" +
                "Row #0: 889\n";

        assertQueryReturns(query, fold(expectedResult));

        String mysqlSql =
            "select " +
            "`store`.`store_state` as `c0`, `time_by_day`.`the_year` as `c1`, " +
            "count(distinct `sales_fact_1997`.`customer_id`) as `m0` " +
            "from " +
            "`store` as `store`, `sales_fact_1997` as `sales_fact_1997`, " +
            "`time_by_day` as `time_by_day`, `product_class` as `product_class`, " +
            "`product` as `product` " +
            "where " +
            "`sales_fact_1997`.`store_id` = `store`.`store_id` " +
            "and `store`.`store_state` = 'WA' " +
            "and `sales_fact_1997`.`time_id` = `time_by_day`.`time_id` " +
            "and `time_by_day`.`the_year` = 1997 " +
            "and `sales_fact_1997`.`product_id` = `product`.`product_id` " +
            "and `product`.`product_class_id` = `product_class`.`product_class_id` " +
            "and (`product_class`.`product_department` = 'Deli' " +
            "and `product_class`.`product_family` = 'Food') " +
            "group by `store`.`store_state`, `time_by_day`.`the_year`";

        String accessSql = "select `d0` as `c0`," +
                " `d1` as `c1`," +
                " count(`m0`) as `c2` " +
                "from (select distinct `store`.`store_state` as `d0`," +
                " `time_by_day`.`the_year` as `d1`," +
                " `sales_fact_1997`.`customer_id` as `m0` " +
                "from `store` as `store`," +
                " `sales_fact_1997` as `sales_fact_1997`," +
                " `time_by_day` as `time_by_day`," +
                " `product_class` as `product_class`," +
                " `product` as `product` " +
                "where `sales_fact_1997`.`store_id` = `store`.`store_id` " +
                "and `store`.`store_state` = 'WA' " +
                "and `sales_fact_1997`.`time_id` = `time_by_day`.`time_id` " +
                "and `time_by_day`.`the_year` = 1997 " +
                "and `sales_fact_1997`.`product_id` = `product`.`product_id` " +
                "and `product`.`product_class_id` = `product_class`.`product_class_id` " +
                "and (`product_class`.`product_department` = 'Deli' " +
                "and `product_class`.`product_family` = 'Food')) as `dummyname` " +
                "group by `d0`, `d1`";

        String derbySql =
            "select " +
            "\"store\".\"store_state\" as \"c0\", " +
            "\"time_by_day\".\"the_year\" as \"c1\", " +
            "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\" " +
            "from " +
            "\"store\" as \"store\", " +
            "\"sales_fact_1997\" as \"sales_fact_1997\", " +
            "\"time_by_day\" as \"time_by_day\", " +
            "\"product_class\" as \"product_class\", " +
            "\"product\" as \"product\" " +
            "where " +
            "\"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" " +
            "and \"store\".\"store_state\" = 'WA' " +
            "and \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" " +
            "and \"time_by_day\".\"the_year\" = 1997 " +
            "and \"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\" " +
            "and \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\" " +
            "and (\"product_class\".\"product_department\" = 'Deli' " +
            "and \"product_class\".\"product_family\" = 'Food') " +
            "group by \"store\".\"store_state\", \"time_by_day\".\"the_year\"";

        SqlPattern[] patterns = {
            new SqlPattern(SqlPattern.Dialect.ACCESS, accessSql, accessSql),
            new SqlPattern(SqlPattern.Dialect.DERBY, derbySql, derbySql),
            new SqlPattern(SqlPattern.Dialect.MYSQL, mysqlSql, mysqlSql)};

        assertQuerySql(query, patterns);
    }

    public void testDistinctCountBug1785406_2() {
        String query =
            "With " +
            "Member [Product].[x] as 'Aggregate({Gender.CurrentMember})'\n" +
            "member [Measures].[foo] as '([Product].[x],[Measures].[Customer Count])'\n" +
            "select Filter([Gender].members,(Not IsEmpty([Measures].[foo]))) on 0 " +
            "from Sales";

        String expectedResult = "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Gender].[All Gender]}\n" +
                "{[Gender].[All Gender].[F]}\n" +
                "{[Gender].[All Gender].[M]}\n" +
                "Row #0: 266,773\n" +
                "Row #0: 131,558\n" +
                "Row #0: 135,215\n";

        assertQueryReturns(query, fold(expectedResult));

        String mysqlSql =
            "select " +
            "`time_by_day`.`the_year` as `c0`, " +
            "count(distinct `sales_fact_1997`.`customer_id`) as `m0` " +
            "from " +
            "`time_by_day` as `time_by_day`, " +
            "`sales_fact_1997` as `sales_fact_1997` " +
            "where `sales_fact_1997`.`time_id` = `time_by_day`.`time_id` " +
            "and `time_by_day`.`the_year` = 1997 " +
            "group by `time_by_day`.`the_year`";

        String accessSql = "select `d0` as `c0`," +
             " count(`m0`) as `c1` " +
             "from (select distinct `time_by_day`.`the_year` as `d0`," +
             " `sales_fact_1997`.`customer_id` as `m0` " +
             "from `time_by_day` as `time_by_day`, " +
             "`sales_fact_1997` as `sales_fact_1997` " +
             "where `sales_fact_1997`.`time_id` = `time_by_day`.`time_id` " +
             "and `time_by_day`.`the_year` = 1997) as `dummyname` group by `d0`";

        String derbySql =
            "select " +
            "\"time_by_day\".\"the_year\" as \"c0\", " +
            "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\" " +
            "from " +
            "\"time_by_day\" as \"time_by_day\", " +
            "\"sales_fact_1997\" as \"sales_fact_1997\" " +
            "where " +
            "\"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" " +
            "and \"time_by_day\".\"the_year\" = 1997 " +
            "group by \"time_by_day\".\"the_year\"";

        SqlPattern[] patterns = {
            new SqlPattern(SqlPattern.Dialect.ACCESS, accessSql, accessSql),
            new SqlPattern(SqlPattern.Dialect.DERBY, derbySql, derbySql),
            new SqlPattern(SqlPattern.Dialect.MYSQL, mysqlSql, mysqlSql)};

        assertQuerySql(query, patterns);
    }

    public void testAggregateDistinctCountInDimensionFilter() {
        String query =
            "With " +
            "Set [Products] as '{[Product].[All Products].[Drink], [Product].[All Products].[Food]}' " +
            "Set [States] as '{[Store].[All Stores].[USA].[CA], [Store].[All Stores].[USA].[OR]}' " +
            "Member [Product].[Selected Products] as 'Aggregate([Products])', SOLVE_ORDER=2 " +
            "Select " +
            "Filter([States], not IsEmpty([Measures].[Customer Count])) on rows, " +
            "{[Measures].[Customer Count]} on columns " +
            "From [Sales] " +
            "Where ([Product].[Selected Products])";

        String result =
            "Axis #0:\n" +
            "{[Product].[Selected Products]}\n" +
            "Axis #1:\n" +
            "{[Measures].[Customer Count]}\n" +
            "Axis #2:\n" +
            "{[Store].[All Stores].[USA].[CA]}\n" +
            "{[Store].[All Stores].[USA].[OR]}\n" +
            "Row #0: 2,692\n" +
            "Row #1: 1,036\n";

        assertQueryReturns(query, fold(result));

        String mysqlSql =
            "select " +
            "`store`.`store_state` as `c0`, `time_by_day`.`the_year` as `c1`, " +
            "count(distinct `sales_fact_1997`.`customer_id`) as `m0` " +
            "from " +
            "`store` as `store`, `sales_fact_1997` as `sales_fact_1997`, " +
            "`time_by_day` as `time_by_day`, `product_class` as `product_class`, " +
            "`product` as `product` " +
            "where " +
            "`sales_fact_1997`.`store_id` = `store`.`store_id` and " +
            "`store`.`store_state` in ('CA', 'OR') and " +
            "`sales_fact_1997`.`time_id` = `time_by_day`.`time_id` and " +
            "`time_by_day`.`the_year` = 1997 and " +
            "`sales_fact_1997`.`product_id` = `product`.`product_id` and " +
            "`product`.`product_class_id` = `product_class`.`product_class_id` and " +
            "`product_class`.`product_family` in ('Drink', 'Food') " +
            "group by " +
            "`store`.`store_state`, `time_by_day`.`the_year`";

        String derbySql =
            "select " +
            "\"store\".\"store_state\" as \"c0\", \"time_by_day\".\"the_year\" as \"c1\", " +
            "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\" " +
            "from " +
            "\"store\" as \"store\", \"sales_fact_1997\" as \"sales_fact_1997\", " +
            "\"time_by_day\" as \"time_by_day\", \"product_class\" as \"product_class\", " +
            "\"product\" as \"product\" " +
            "where " +
            "\"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" and " +
            "\"store\".\"store_state\" in ('CA', 'OR') and " +
            "\"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and " +
            "\"time_by_day\".\"the_year\" = 1997 and " +
            "\"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\" and " +
            "\"product\".\"product_class_id\" = \"product_class\".\"product_class_id\" and " +
            "\"product_class\".\"product_family\" in ('Drink', 'Food') " +
            "group by " +
            "\"store\".\"store_state\", \"time_by_day\".\"the_year\"";

        SqlPattern[] patterns = {
            new SqlPattern(SqlPattern.Dialect.DERBY, derbySql, derbySql),
            new SqlPattern(SqlPattern.Dialect.MYSQL, mysqlSql, mysqlSql)};

        assertQuerySql(query, patterns);
    }
}

// End FastBatchingCellReaderTest.java
