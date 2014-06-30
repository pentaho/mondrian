/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2007-2014 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.Connection;
import mondrian.olap.MondrianProperties;
import mondrian.olap.MondrianServer;
import mondrian.rolap.agg.*;
import mondrian.server.*;
import mondrian.spi.Dialect;
import mondrian.test.SqlPattern;
import mondrian.test.TestContext;
import mondrian.util.Bug;
import mondrian.util.DelegatingInvocationHandler;

import junit.framework.Assert;

import java.lang.reflect.Proxy;
import java.util.*;
import java.util.concurrent.Future;

/**
 * Test for <code>FastBatchingCellReader</code>.
 *
 * @author Thiyagu
 * @since 24-May-2007
 */
public class FastBatchingCellReaderTest extends BatchTestCase {

    public static final List<String> ESL = Collections.emptyList();

    private Locus locus;
    private Execution e;
    private AggregationManager aggMgr;
    private RolapCube salesCube;
    private Connection connection;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        connection = getTestContext().getConnection();
        connection.getCacheControl(null).flushSchemaCache();
        final Statement statement =
            ((RolapConnection) getTestContext().getConnection())
                .getInternalStatement();
        e = new Execution(statement, 0);
        aggMgr =
            e.getMondrianStatement()
                .getMondrianConnection()
                .getServer().getAggregationManager();
        locus = new Locus(e, "FastBatchingCellReaderTest", null);
        Locus.push(locus);
        salesCube = (RolapCube)
            getTestContext().getConnection().getSchemaReader()
                .withLocus().getCubes()[0];
    }

    @Override
    protected void tearDown() throws Exception {
        Locus.pop(locus);
        // cleanup
        connection.close();
        connection = null;
        e = null;
        aggMgr = null;
        locus = null;
        salesCube = null;
        super.tearDown();
    }

    private BatchLoader createFbcr(
        Boolean useGroupingSets,
        RolapCube cube)
    {
        Dialect dialect = cube.getSchema().getDialect();
        if (useGroupingSets != null) {
            dialect = dialectWithGroupingSets(dialect, useGroupingSets);
        }
        return new BatchLoader(
            Locus.peek(),
            aggMgr.cacheMgr,
            dialect,
            cube);
    }

    private Dialect dialectWithGroupingSets(
        final Dialect dialect,
        final boolean supportsGroupingSets)
    {
        return (Dialect) Proxy.newProxyInstance(
            Dialect.class.getClassLoader(),
            new Class[] {Dialect.class},
            new MyDelegatingInvocationHandler(dialect, supportsGroupingSets));
    }

    public void testMissingSubtotalBugMetricFilter() {
        assertQueryReturns(
            "With "
            + "Set [*NATIVE_CJ_SET] as "
            + "'NonEmptyCrossJoin({[Time].[Year].[1997]},"
            + "                   NonEmptyCrossJoin({[Product].[All Products].[Drink]},{[Education Level].[All Education Levels].[Bachelors Degree]}))' "
            + "Set [*METRIC_CJ_SET] as 'Filter([*NATIVE_CJ_SET],[Measures].[*Unit Sales_SEL~SUM] > 1000.0)' "
            + "Set [*METRIC_MEMBERS_Education Level] as 'Generate([*METRIC_CJ_SET], {[Education Level].CurrentMember})' "
            + "Member [Measures].[*Unit Sales_SEL~SUM] as '([Measures].[Unit Sales],[Time].[Time].CurrentMember,[Product].CurrentMember,[Education Level].CurrentMember)', SOLVE_ORDER=200 "
            + "Member [Education Level].[*CTX_MEMBER_SEL~SUM] as 'Sum(Filter([*METRIC_MEMBERS_Education Level],[Measures].[*Unit Sales_SEL~SUM] > 1000.0))', SOLVE_ORDER=-102 "
            + "Select "
            + "{[Measures].[Unit Sales]} on columns, "
            + "Non Empty Union(CrossJoin(Generate([*METRIC_CJ_SET], {([Time].[Time].CurrentMember,[Product].CurrentMember)}),{[Education Level].[*CTX_MEMBER_SEL~SUM]}),"
            + "                Generate([*METRIC_CJ_SET], {([Time].[Time].CurrentMember,[Product].CurrentMember,[Education Level].CurrentMember)})) on rows "
            + "From [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[1997], [Product].[Products].[Drink], [Customer].[Education Level].[*CTX_MEMBER_SEL~SUM]}\n"
            + "{[Time].[Time].[1997], [Product].[Products].[Drink], [Customer].[Education Level].[Bachelors Degree]}\n"
            + "Row #0: 6,423\n"
            + "Row #1: 6,423\n");
    }

    public void testMissingSubtotalBugMultiLevelMetricFilter() {
        assertQueryReturns(
            "With "
            + "Set [*NATIVE_CJ_SET] as 'NonEmptyCrossJoin([*BASE_MEMBERS_Product],[*BASE_MEMBERS_Education Level])' "
            + "Set [*METRIC_CJ_SET] as 'Filter([*NATIVE_CJ_SET],[Measures].[*Store Cost_SEL~SUM] > 1000.0)' "
            + "Set [*BASE_MEMBERS_Product] as '{[Product].[All Products].[Drink].[Beverages],[Product].[All Products].[Food].[Baked Goods]}' "
            + "Set [*METRIC_MEMBERS_Product] as 'Generate([*METRIC_CJ_SET], {[Product].CurrentMember})' "
            + "Set [*BASE_MEMBERS_Education Level] as '{[Education Level].[All Education Levels].[High School Degree],[Education Level].[All Education Levels].[Partial High School]}' "
            + "Set [*METRIC_MEMBERS_Education Level] as 'Generate([*METRIC_CJ_SET], {[Education Level].CurrentMember})' "
            + "Member [Measures].[*Store Cost_SEL~SUM] as '([Measures].[Store Cost],[Product].CurrentMember,[Education Level].CurrentMember)', SOLVE_ORDER=200 "
            + "Member [Product].[Drink].[*CTX_MEMBER_SEL~SUM] as 'Sum(Filter([*METRIC_MEMBERS_Product],[Product].CurrentMember.Parent = [Product].[All Products].[Drink]))', SOLVE_ORDER=-100 "
            + "Member [Product].[Food].[*CTX_MEMBER_SEL~SUM] as 'Sum(Filter([*METRIC_MEMBERS_Product],[Product].CurrentMember.Parent = [Product].[All Products].[Food]))', SOLVE_ORDER=-100 "
            + "Member [Education Level].[*CTX_MEMBER_SEL~SUM] as 'Sum(Filter([*METRIC_MEMBERS_Education Level],[Measures].[*Store Cost_SEL~SUM] > 1000.0))', SOLVE_ORDER=-101 "
            + "Select "
            + "{[Measures].[Store Cost]} on columns, "
            + "NonEmptyCrossJoin({[Product].[Drink].[*CTX_MEMBER_SEL~SUM],[Product].[Food].[*CTX_MEMBER_SEL~SUM]},{[Education Level].[*CTX_MEMBER_SEL~SUM]}) "
            + "on rows From [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Cost]}\n"
            + "Axis #2:\n"
            + "{[Product].[Products].[Drink].[*CTX_MEMBER_SEL~SUM], [Customer].[Education Level].[*CTX_MEMBER_SEL~SUM]}\n"
            + "{[Product].[Products].[Food].[*CTX_MEMBER_SEL~SUM], [Customer].[Education Level].[*CTX_MEMBER_SEL~SUM]}\n"
            + "Row #0: 6,535.30\n"
            + "Row #1: 3,860.89\n");
    }

    public void testShouldUseGroupingFunctionOnPropertyTrueAndOnSupportedDB() {
        propSaver.set(propSaver.props.EnableGroupingSets, true);
        BatchLoader fbcr = createFbcr(true, salesCube);
        assertTrue(fbcr.shouldUseGroupingFunction());
    }

    public void testShouldUseGroupingFunctionOnPropertyTrueAndOnNonSupportedDB()
    {
        propSaver.set(propSaver.props.EnableGroupingSets, true);
        BatchLoader fbcr = createFbcr(false, salesCube);
        assertFalse(fbcr.shouldUseGroupingFunction());
    }

    public void testShouldUseGroupingFunctionOnPropertyFalseOnSupportedDB() {
        propSaver.set(propSaver.props.EnableGroupingSets, false);
        BatchLoader fbcr = createFbcr(true, salesCube);
        assertFalse(fbcr.shouldUseGroupingFunction());
    }

    public void testShouldUseGroupingFunctionOnPropertyFalseOnNonSupportedDB() {
        propSaver.set(propSaver.props.EnableGroupingSets, false);
        BatchLoader fbcr = createFbcr(false, salesCube);
        assertFalse(fbcr.shouldUseGroupingFunction());
    }

    public void testDoesDBSupportGroupingSets() {
        final Dialect dialect = getTestContext().getDialect();
        FastBatchingCellReader fbcr =
            new FastBatchingCellReader(e, salesCube, aggMgr) {
                Dialect getDialect() {
                    return dialect;
                }
            };
        switch (dialect.getDatabaseProduct()) {
        case ORACLE:
        case TERADATA:
        case DB2:
        case DB2_AS400:
        case DB2_OLD_AS400:
        case GREENPLUM:
            assertTrue(fbcr.getDialect().supportsGroupingSets());
            break;
        default:
            assertFalse(fbcr.getDialect().supportsGroupingSets());
            break;
        }
    }

    public void testGroupBatchesForNonGroupableBatchesWithSorting() {
        final TestContext testContext = getTestContext();
        final BatchLoader fbcr = createFbcr(null, salesCube);
        BatchLoader.Batch genderBatch = fbcr.new Batch(
            createRequest(
                testContext,
                cubeNameSales, measureUnitSales,
                "customer", "gender", "F"));
        BatchLoader.Batch maritalStatusBatch = fbcr.new Batch(
            createRequest(
                testContext,
                cubeNameSales, measureUnitSales,
                "customer", "marital_status", "M"));
        ArrayList<BatchLoader.Batch> batchList =
            new ArrayList<BatchLoader.Batch>();
        batchList.add(genderBatch);
        batchList.add(maritalStatusBatch);
        List<BatchLoader.Loadable> groupedBatches =
            BatchLoader.groupBatches(batchList);
        assertEquals(batchList.size(), groupedBatches.size());
        assertEquals(genderBatch, groupedBatches.get(0).getDetailedBatch());
        assertEquals(
            maritalStatusBatch, groupedBatches.get(1).getDetailedBatch());
    }

    public void testGroupBatchesForNonGroupableBatchesWithConstraints() {
        final TestContext testContext = getTestContext();
        final BatchLoader fbcr = createFbcr(null, salesCube);
        List<List<String>> compoundMembers = list(
            list("USA", "CA"),
            list("Canada", "BC"));
        CellRequestConstraint constraint =
            makeConstraintCountryState(compoundMembers);

        BatchLoader.Batch genderBatch = fbcr.new Batch(
            createRequest(
                testContext,
                cubeNameSales, measureUnitSales,
                "customer", "gender", "F", constraint));
        BatchLoader.Batch maritalStatusBatch = fbcr.new Batch(
            createRequest(
                testContext,
                cubeNameSales, measureUnitSales,
                "customer", "marital_status", "M", constraint));
        ArrayList<BatchLoader.Batch> batchList =
            new ArrayList<BatchLoader.Batch>();
        batchList.add(genderBatch);
        batchList.add(maritalStatusBatch);
        List<BatchLoader.Loadable> groupedBatches =
            BatchLoader.groupBatches(batchList);
        assertEquals(batchList.size(), groupedBatches.size());
        assertEquals(genderBatch, groupedBatches.get(0).getDetailedBatch());
        assertEquals(
            maritalStatusBatch, groupedBatches.get(1).getDetailedBatch());
    }

    public void testGroupBatchesForGroupableBatches() {
        final BatchLoader fbcr = createFbcr(null, salesCube);
        final TestContext testContext = getTestContext();
        BatchLoader.Batch genderBatch = fbcr.new Batch(
            createRequest(
                testContext,
                cubeNameSales, measureUnitSales,
                "customer", "gender", "F"))
            {
                boolean canBatch(BatchLoader.Batch other) {
                    return false;
                }
            };
        BatchLoader.Batch superBatch = fbcr.new Batch(
            createRequest(
                testContext,
                cubeNameSales, measureUnitSales,
                ESL, ESL, ESL))
            {
                boolean canBatch(BatchLoader.Batch batch) {
                    return true;
                }
            };
        ArrayList<BatchLoader.Batch> batchList =
            new ArrayList<BatchLoader.Batch>();
        batchList.add(genderBatch);
        batchList.add(superBatch);
        List<BatchLoader.Loadable> groupedBatches =
            BatchLoader.groupBatches(batchList);
        assertEquals(1, groupedBatches.size());
        assertEquals(superBatch, groupedBatches.get(0).getDetailedBatch());
        final BatchLoader.CompositeBatch batch0 =
            (BatchLoader.CompositeBatch) groupedBatches.get(0);
        assertTrue(batch0.summaryBatches.contains(genderBatch));
    }

    public void testGroupBatchesForGroupableBatchesAndNonGroupableBatches() {
        final TestContext testContext = getTestContext();
        final BatchLoader fbcr = createFbcr(null, salesCube);
        final BatchLoader.Batch group1Agg2 = fbcr.new Batch(
            createRequest(
                testContext,
                cubeNameSales, measureUnitSales,
                "customer", "gender", "F"))
            {
                boolean canBatch(BatchLoader.Batch batch) {
                    return false;
                }
            };
        final BatchLoader.Batch group1Agg1 = fbcr.new Batch(
            createRequest(
                testContext,
                cubeNameSales, measureUnitSales,
                "customer", "country", "F"))
            {
                boolean canBatch(BatchLoader.Batch batch) {
                    return batch.equals(group1Agg2);
                }
            };
        BatchLoader.Batch group1Detailed = fbcr.new Batch(
            createRequest(
                testContext,
                cubeNameSales, measureUnitSales,
                ESL, ESL, ESL))
            {
                boolean canBatch(BatchLoader.Batch batch) {
                    return batch.equals(group1Agg1);
                }
            };

        final BatchLoader.Batch group2Agg1 = fbcr.new Batch(
            createRequest(
                testContext,
                cubeNameSales, measureUnitSales,
                "customer", "education", "F"))
            {
                boolean canBatch(BatchLoader.Batch batch) {
                    return false;
                }
            };
        BatchLoader.Batch group2Detailed = fbcr.new Batch(
            createRequest(
                testContext,
                cubeNameSales, measureUnitSales,
                "customer", "yearly_income", ""))
            {
                boolean canBatch(BatchLoader.Batch batch) {
                    return batch.equals(group2Agg1);
                }
            };
        ArrayList<BatchLoader.Batch> batchList =
            new ArrayList<BatchLoader.Batch>();
        batchList.add(group1Agg1);
        batchList.add(group1Agg2);
        batchList.add(group1Detailed);
        batchList.add(group2Agg1);
        batchList.add(group2Detailed);
        List<BatchLoader.Loadable> groupedBatches =
            BatchLoader.groupBatches(batchList);
        assertEquals(2, groupedBatches.size());
        final BatchLoader.CompositeBatch batch0 =
            (BatchLoader.CompositeBatch) groupedBatches.get(0);
        assertEquals(group1Detailed, batch0.getDetailedBatch());
        assertTrue(batch0.summaryBatches.contains(group1Agg1));
        assertTrue(batch0.summaryBatches.contains(group1Agg2));
        final BatchLoader.CompositeBatch batch1 =
            (BatchLoader.CompositeBatch) groupedBatches.get(1);
        assertEquals(group2Detailed, batch1.detailedBatch);
        assertTrue(batch1.summaryBatches.contains(group2Agg1));
    }

    public void testGroupBatchesForTwoSetOfGroupableBatches() {
        final TestContext testContext = getTestContext();
        List<String> fieldValuesStoreType = list(
            "Deluxe Supermarket", "Gourmet Supermarket", "HeadQuarters",
            "Mid-Size Grocery", "Small Grocery", "Supermarket");
        String fieldStoreType = "store_type";
        String tableStore = "store";

        List<String> fieldValuesWarehouseCountry =
            list("Canada", "Mexico", "USA");
        String fieldWarehouseCountry = "warehouse_country";
        String tableWarehouse = "warehouse";

        final BatchLoader fbcr = createFbcr(null, salesCube);
        BatchLoader.Batch batch1RollupOnGender =
            createBatch(
                testContext,
                fbcr,
                list(tableTime, tableStore, tableProductClass),
                list(fieldYear, fieldStoreType, fieldProductFamily),
                list(
                    fieldValuesYear,
                    fieldValuesStoreType,
                    fieldValuesProductFamily),
                cubeNameSales,
                measureUnitSales);

        BatchLoader.Batch batch1RollupOnGenderAndProductDepartment =
            createBatch(
                testContext,
                fbcr,
                list(tableTime, tableProductClass),
                list(fieldYear, fieldProductFamily),
                list(fieldValuesYear, fieldValuesProductFamily),
                cubeNameSales, measureUnitSales);

        BatchLoader.Batch
            batch1RollupOnStoreTypeAndProductDepartment =
            createBatch(
                testContext,
                fbcr,
                list(tableTime, tableCustomer),
                list(fieldYear, fieldGender),
                list(fieldValuesYear, fieldValuesGender),
                cubeNameSales, measureUnitSales);

        BatchLoader.Batch batch1Detailed =
            createBatch(
                testContext,
                fbcr,
                list(
                    tableTime, tableStore, tableProductClass, tableCustomer),
                list(
                    fieldYear, fieldStoreType, fieldProductFamily, fieldGender),
                list(
                    fieldValuesYear, fieldValuesStoreType,
                    fieldValuesProductFamily, fieldValuesGender),
                cubeNameSales,
                measureUnitSales);

        String warehouseCube = "Warehouse";
        String measure2 = "[Measures].[Warehouse Sales]";
        BatchLoader.Batch batch2RollupOnStoreType =
            createBatch(
                testContext,
                fbcr,
                list(tableWarehouse, tableTime, tableProductClass),
                list(fieldWarehouseCountry, fieldYear, fieldProductFamily),
                list(
                    fieldValuesWarehouseCountry, fieldValuesYear,
                    fieldValuesProductFamily),
                warehouseCube,
                measure2);

        BatchLoader.Batch
            batch2RollupOnStoreTypeAndWareHouseCountry =
            createBatch(
                testContext,
                fbcr,
                list(tableTime, tableProductClass),
                list(fieldYear, fieldProductFamily),
                list(fieldValuesYear, fieldValuesProductFamily),
                warehouseCube, measure2);

        BatchLoader.Batch
            batch2RollupOnProductFamilyAndWareHouseCountry =
            createBatch(
                testContext,
                fbcr,
                list(tableTime, tableStore),
                list(fieldYear, fieldStoreType),
                list(fieldValuesYear, fieldValuesStoreType),
                warehouseCube, measure2);

        BatchLoader.Batch batch2Detailed =
            createBatch(
                testContext,
                fbcr,
                list(tableWarehouse, tableTime, tableStore, tableProductClass),
                list(
                    fieldWarehouseCountry, fieldYear, fieldStoreType,
                    fieldProductFamily),
                list(
                    fieldValuesWarehouseCountry, fieldValuesYear,
                    fieldValuesStoreType, fieldValuesProductFamily),
                warehouseCube,
                measure2);

        List<BatchLoader.Batch> batchList =
            new ArrayList<BatchLoader.Batch>();

        batchList.add(batch1RollupOnGender);
        batchList.add(batch2RollupOnStoreType);
        batchList.add(batch2RollupOnStoreTypeAndWareHouseCountry);
        batchList.add(batch2RollupOnProductFamilyAndWareHouseCountry);
        batchList.add(batch1RollupOnGenderAndProductDepartment);
        batchList.add(batch1RollupOnStoreTypeAndProductDepartment);
        batchList.add(batch2Detailed);
        batchList.add(batch1Detailed);
        List<BatchLoader.Loadable> groupedBatches =
            BatchLoader.groupBatches(batchList);
        final int groupedBatchCount = groupedBatches.size();

        // Until MONDRIAN-1001 is fixed, behavior is flaky due to interaction
        // with previous tests.
        if (Bug.BugMondrian1001Fixed) {
            if (MondrianProperties.instance().UseAggregates.get()
                && MondrianProperties.instance().ReadAggregates.get())
            {
                assertEquals(4, groupedBatchCount);
            } else {
                assertEquals(2, groupedBatchCount);
            }
        } else {
            assertTrue(groupedBatchCount == 2 || groupedBatchCount == 4);
        }
    }

    public void testAddToCompositeBatchForBothBatchesNotPartOfCompositeBatch() {
        final BatchLoader fbcr = createFbcr(null, salesCube);
        final TestContext testContext = getTestContext();
        BatchLoader.Batch batch1 = fbcr.new Batch(
            createRequest(
                testContext,
                cubeNameSales, measureUnitSales,
                "customer", "country", "F"));
        BatchLoader.Batch batch2 = fbcr.new Batch(
            createRequest(
                testContext,
                cubeNameSales, measureUnitSales,
                "customer", "gender", "F"));
        Map<AggregationKey, BatchLoader.CompositeBatch> batchGroups =
            new HashMap<
                AggregationKey, BatchLoader.CompositeBatch>();
        BatchLoader.addToCompositeBatch(batchGroups, batch1, batch2);
        assertEquals(1, batchGroups.size());
        BatchLoader.CompositeBatch compositeBatch =
            batchGroups.get(batch1.batchKey);
        assertEquals(batch1, compositeBatch.detailedBatch);
        assertEquals(1, compositeBatch.summaryBatches.size());
        assertTrue(compositeBatch.summaryBatches.contains(batch2));
    }

    public void
        testAddToCompositeBatchForDetailedBatchAlreadyPartOfACompositeBatch()
    {
        final TestContext testContext = getTestContext();
        final BatchLoader fbcr = createFbcr(null, salesCube);
        BatchLoader.Batch detailedBatch = fbcr.new Batch(
            createRequest(
                testContext,
                cubeNameSales, measureUnitSales,
                "customer", "country", "F"));
        BatchLoader.Batch aggBatch1 = fbcr.new Batch(
            createRequest(
                testContext,
                cubeNameSales, measureUnitSales,
                "customer", "gender", "F"));
        BatchLoader.Batch aggBatchAlreadyInComposite =
            fbcr.new Batch(
                createRequest(
                    testContext,
                    cubeNameSales,
                    measureUnitSales, "customer", "gender", "F"));
        Map<AggregationKey, BatchLoader.CompositeBatch> batchGroups =
            new HashMap<
                AggregationKey, BatchLoader.CompositeBatch>();
        BatchLoader.CompositeBatch existingCompositeBatch =
            new BatchLoader.CompositeBatch(detailedBatch);
        existingCompositeBatch.add(aggBatchAlreadyInComposite);
        batchGroups.put(detailedBatch.batchKey, existingCompositeBatch);

        BatchLoader.addToCompositeBatch(batchGroups, detailedBatch, aggBatch1);

        assertEquals(1, batchGroups.size());
        BatchLoader.CompositeBatch compositeBatch =
            batchGroups.get(detailedBatch.batchKey);
        assertEquals(detailedBatch, compositeBatch.detailedBatch);
        assertEquals(2, compositeBatch.summaryBatches.size());
        assertTrue(compositeBatch.summaryBatches.contains(aggBatch1));
        assertTrue(compositeBatch.summaryBatches.contains(
            aggBatchAlreadyInComposite));
    }

    public void
        testAddToCompositeBatchForAggregationBatchAlreadyPartOfACompositeBatch()
    {
        final TestContext testContext = getTestContext();
        final BatchLoader fbcr = createFbcr(null, salesCube);
        BatchLoader.Batch detailedBatch = fbcr.new Batch(
            createRequest(
                testContext,
                cubeNameSales, measureUnitSales,
                "customer", "country", "F"));
        BatchLoader.Batch aggBatchToAddToDetailedBatch =
            fbcr.new Batch(
                createRequest(
                    testContext,
                    cubeNameSales,
                    measureUnitSales, "customer", "gender", "F"));
        BatchLoader.Batch aggBatchAlreadyInComposite =
            fbcr.new Batch(
                createRequest(
                    testContext,
                    cubeNameSales,
                    measureUnitSales, "customer", "city", "F"));
        Map<AggregationKey, BatchLoader.CompositeBatch> batchGroups =
            new HashMap<
                AggregationKey, BatchLoader.CompositeBatch>();
        BatchLoader.CompositeBatch existingCompositeBatch =
            new BatchLoader.CompositeBatch(aggBatchToAddToDetailedBatch);
        existingCompositeBatch.add(aggBatchAlreadyInComposite);
        batchGroups.put(
            aggBatchToAddToDetailedBatch.batchKey,
            existingCompositeBatch);

        BatchLoader.addToCompositeBatch(
            batchGroups, detailedBatch,
            aggBatchToAddToDetailedBatch);

        assertEquals(1, batchGroups.size());
        BatchLoader.CompositeBatch compositeBatch =
            batchGroups.get(detailedBatch.batchKey);
        assertEquals(detailedBatch, compositeBatch.detailedBatch);
        assertEquals(2, compositeBatch.summaryBatches.size());
        assertTrue(compositeBatch.summaryBatches.contains(
            aggBatchToAddToDetailedBatch));
        assertTrue(compositeBatch.summaryBatches.contains(
            aggBatchAlreadyInComposite));
    }

    public void
        testAddToCompositeBatchForBothBatchAlreadyPartOfACompositeBatch()
    {
        final TestContext testContext = getTestContext();
        final BatchLoader fbcr = createFbcr(null, salesCube);
        BatchLoader.Batch detailedBatch = fbcr.new Batch(
            createRequest(
                testContext,
                cubeNameSales, measureUnitSales,
                "customer", "country", "F"));
        BatchLoader.Batch aggBatchToAddToDetailedBatch =
            fbcr.new Batch(
                createRequest(
                    testContext,
                    cubeNameSales,
                    measureUnitSales, "customer", "gender", "F"));
        BatchLoader.Batch aggBatchAlreadyInCompositeOfAgg =
            fbcr.new Batch(
                createRequest(
                    testContext,
                    cubeNameSales,
                    measureUnitSales, "customer", "city", "F"));
        BatchLoader.Batch aggBatchAlreadyInCompositeOfDetail =
            fbcr.new Batch(
                createRequest(
                    testContext,
                    cubeNameSales,
                    measureUnitSales, "customer", "state_province", "F"));

        Map<AggregationKey, BatchLoader.CompositeBatch> batchGroups =
            new HashMap<
                AggregationKey, BatchLoader.CompositeBatch>();
        BatchLoader.CompositeBatch existingAggCompositeBatch =
            new BatchLoader.CompositeBatch(aggBatchToAddToDetailedBatch);
        existingAggCompositeBatch.add(aggBatchAlreadyInCompositeOfAgg);
        batchGroups.put(
            aggBatchToAddToDetailedBatch.batchKey,
            existingAggCompositeBatch);

        BatchLoader.CompositeBatch existingCompositeBatch =
            new BatchLoader.CompositeBatch(detailedBatch);
        existingCompositeBatch.add(aggBatchAlreadyInCompositeOfDetail);
        batchGroups.put(detailedBatch.batchKey, existingCompositeBatch);

        BatchLoader.addToCompositeBatch(
            batchGroups, detailedBatch,
            aggBatchToAddToDetailedBatch);

        assertEquals(1, batchGroups.size());
        BatchLoader.CompositeBatch compositeBatch =
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

    /**
     * Tests that can batch for batch with super set of contraint
     * column bit key and all values for additional condition.
     */
    public void testCanBatchForSuperSet() {
        final TestContext testContext = getTestContext();
        final BatchLoader fbcr = createFbcr(null, salesCube);

        BatchLoader.Batch aggregationBatch =
            createBatch(
                testContext,
                fbcr,
                list(tableTime, tableProductClass, tableProductClass),
                list(fieldYear, fieldProductFamily, fieldProductDepartment),
                list(
                    fieldValuesYear, fieldValuesProductFamily,
                    fieldValueProductDepartment),
                cubeNameSales,
                measureUnitSales);

        BatchLoader.Batch detailedBatch =
            createBatch(
                testContext,
                fbcr,
                list(
                    tableTime, tableProductClass,
                    tableProductClass, tableCustomer),
                list(
                    fieldYear, fieldProductFamily,
                    fieldProductDepartment, fieldGender),
                list(
                    fieldValuesYear,
                    fieldValuesProductFamily,
                    fieldValueProductDepartment, fieldValuesGender),
                cubeNameSales,
                measureUnitSales);

        assertTrue(detailedBatch.canBatch(aggregationBatch));
        assertFalse(aggregationBatch.canBatch(detailedBatch));
    }

    public void testCanBatchForBatchWithConstraint() {
        final TestContext testContext = getTestContext();
        final BatchLoader fbcr = createFbcr(null, salesCube);

        List<List<String>> compoundMembers = list(
            list("USA", "CA"),
            list("Canada", "BC"));
        CellRequestConstraint constraint =
            makeConstraintCountryState(compoundMembers);

        BatchLoader.Batch aggregationBatch =
            createBatch(
                testContext,
                fbcr,
                list(tableTime, tableProductClass, tableProductClass),
                list(
                    fieldYear, fieldProductFamily, fieldProductDepartment),
                list(
                    fieldValuesYear,
                    fieldValuesProductFamily,
                    fieldValueProductDepartment),
                cubeNameSales,
                measureUnitSales,
                constraint);

        BatchLoader.Batch detailedBatch =
            createBatch(
                testContext,
                fbcr,
                list(
                    tableTime, tableProductClass,
                    tableProductClass, tableCustomer),
                list(
                    fieldYear, fieldProductFamily,
                    fieldProductDepartment, fieldGender),
                list(
                    fieldValuesYear,
                    fieldValuesProductFamily,
                    fieldValueProductDepartment, fieldValuesGender),
                cubeNameSales,
                measureUnitSales, constraint);

        assertTrue(detailedBatch.canBatch(aggregationBatch));
        assertFalse(aggregationBatch.canBatch(detailedBatch));
    }

    public void testCanBatchForBatchWithConstraint2() {
        final TestContext testContext = getTestContext();
        final BatchLoader fbcr = createFbcr(null, salesCube);

        List<List<String>> compoundMembers1 = list(
            list("USA", "CA"),
            list("Canada", "BC"));
        CellRequestConstraint constraint1 =
            makeConstraintCountryState(compoundMembers1);

        // Different constraint will cause the Batch not to match.
        List<List<String>> compoundMembers2 = list(
            list("USA", "CA"),
            list("USA", "OR"));
        CellRequestConstraint constraint2 =
            makeConstraintCountryState(compoundMembers2);

        BatchLoader.Batch aggregationBatch =
            createBatch(
                testContext,
                fbcr,
                list(
                    tableTime, tableProductClass,
                    tableProductClass),
                list(
                    fieldYear, fieldProductFamily, fieldProductDepartment),
                list(
                    fieldValuesYear,
                    fieldValuesProductFamily,
                    fieldValueProductDepartment),
                cubeNameSales,
                measureUnitSales,
                constraint1);

        BatchLoader.Batch detailedBatch =
            createBatch(
                testContext,
                fbcr,
                list(
                    tableTime, tableProductClass,
                    tableProductClass, tableCustomer),
                list(
                    fieldYear, fieldProductFamily,
                    fieldProductDepartment, fieldGender),
                list(
                    fieldValuesYear,
                    fieldValuesProductFamily,
                    fieldValueProductDepartment, fieldValuesGender),
                cubeNameSales,
                measureUnitSales,
                constraint2);

        assertTrue(detailedBatch.canBatch(aggregationBatch));
        assertFalse(aggregationBatch.canBatch(detailedBatch));
    }

    public void testCanBatchForBatchWithDistinctCountInDetailedBatch() {
        if (!MondrianProperties.instance().UseAggregates.get()
            || !MondrianProperties.instance().ReadAggregates.get())
        {
            return;
        }
        final TestContext testContext = getTestContext();
        final BatchLoader fbcr = createFbcr(null, salesCube);

        BatchLoader.Batch aggregationBatch =
            createBatch(
                testContext,
                fbcr,
                list(
                    tableTime, tableProductClass,
                    tableProductClass),
                list(
                    fieldYear, fieldProductFamily,
                    fieldProductDepartment),
                list(
                    fieldValuesYear,
                    fieldValuesProductFamily,
                    fieldValueProductDepartment),
                cubeNameSales, measureUnitSales);

        BatchLoader.Batch detailedBatch =
            createBatch(
                testContext,
                fbcr,
                list(
                    tableTime, tableProductClass,
                    tableProductClass),
                list(
                    fieldYear, fieldProductFamily,
                    fieldProductDepartment),
                list(
                    fieldValuesYear,
                    fieldValuesProductFamily,
                    fieldValueProductDepartment),
                cubeNameSales,
                "[Measures].[Customer Count]");

        assertFalse(detailedBatch.canBatch(aggregationBatch));
        assertFalse(aggregationBatch.canBatch(detailedBatch));
    }

    public void testCanBatchForBatchWithDistinctCountInAggregateBatch() {
        if (!MondrianProperties.instance().UseAggregates.get()
            || !MondrianProperties.instance().ReadAggregates.get())
        {
            return;
        }
        final TestContext testContext = getTestContext();
        final BatchLoader fbcr = createFbcr(null, salesCube);

        BatchLoader.Batch aggregationBatch =
            createBatch(
                testContext,
                fbcr,
                list(
                    tableTime, tableProductClass,
                    tableProductClass),
                list(
                    fieldYear,
                    fieldProductFamily, fieldProductDepartment),
                list(
                    fieldValuesYear,
                    fieldValuesProductFamily,
                    fieldValueProductDepartment),
                cubeNameSales,
                "[Measures].[Customer Count]");

        BatchLoader.Batch detailedBatch =
            createBatch(
                testContext,
                fbcr,
                list(
                    tableTime, tableProductClass,
                    tableProductClass),
                list(
                    fieldYear, fieldProductFamily,
                    fieldProductDepartment),
                list(
                    fieldValuesYear,
                    fieldValuesProductFamily,
                    fieldValueProductDepartment),
                cubeNameSales,
                measureUnitSales);

        assertFalse(detailedBatch.canBatch(aggregationBatch));
        assertFalse(aggregationBatch.canBatch(detailedBatch));
    }

    public void testCanBatchSummaryBatchWithDetailedBatchWithDistinctCount() {
        if (MondrianProperties.instance().UseAggregates.get()
            || MondrianProperties.instance().ReadAggregates.get())
        {
            return;
        }
        final TestContext testContext = getTestContext();
        final BatchLoader fbcr = createFbcr(null, salesCube);

        BatchLoader.Batch aggregationBatch =
            createBatch(
                testContext,
                fbcr,
                list(tableTime),
                list(fieldYear),
                list(fieldValuesYear),
                cubeNameSales,
                "[Measures].[Customer Count]");

        BatchLoader.Batch detailedBatch =
            createBatch(
                testContext,
                fbcr,
                list(
                    tableTime, tableProductClass, tableProductClass),
                list(
                    fieldYear, fieldProductFamily, fieldProductDepartment),
                list(
                    fieldValuesYear,
                    fieldValuesProductFamily,
                    fieldValueProductDepartment),
                cubeNameSales, measureUnitSales);

        assertFalse(detailedBatch.canBatch(aggregationBatch));
        assertFalse(aggregationBatch.canBatch(detailedBatch));
    }


    /**
     * Test that can batch for batch with non superset of constraint
     * column bit key and all values for additional condition.
     */
    public void testNonSuperSet() {
        final TestContext testContext = getTestContext();
        final BatchLoader fbcr = createFbcr(null, salesCube);

        BatchLoader.Batch aggregationBatch =
            createBatch(
                testContext,
                fbcr,
                list(
                    tableTime, tableProductClass, tableProductClass),
                list(
                    fieldYear, fieldProductFamily, fieldProductDepartment),
                list(
                    fieldValuesYear,
                    fieldValuesProductFamily,
                    fieldValueProductDepartment),
                cubeNameSales,
                measureUnitSales);

        BatchLoader.Batch detailedBatch =
            createBatch(
                testContext,
                fbcr,
                list(
                    tableProductClass,
                    tableProductClass, tableCustomer),
                list(
                    fieldProductFamily,
                    fieldProductDepartment, fieldGender),
                list(
                    fieldValuesProductFamily,
                    fieldValueProductDepartment, fieldValuesGender),
                cubeNameSales,
                measureUnitSales);

        assertFalse(detailedBatch.canBatch(aggregationBatch));
        assertFalse(aggregationBatch.canBatch(detailedBatch));
    }

    /**
     * Tests that can batch for batch with super set of constraint
     * column bit key and NOT all values for additional condition.
     */
    public void testSuperSetAndNotAllValues() {
        final TestContext testContext = getTestContext();
        final BatchLoader fbcr = createFbcr(null, salesCube);

        BatchLoader.Batch aggregationBatch =
            createBatch(
                testContext,
                fbcr,
                list(
                    tableTime, tableProductClass, tableProductClass),
                list(
                    fieldYear, fieldProductFamily, fieldProductDepartment),
                list(
                    fieldValuesYear,
                    fieldValuesProductFamily,
                    fieldValueProductDepartment),
                cubeNameSales,
                measureUnitSales);

        BatchLoader.Batch detailedBatch =
            createBatch(
                testContext,
                fbcr,
                list(
                    tableTime, tableProductClass,
                    tableProductClass, tableCustomer),
                list(
                    fieldYear, fieldProductFamily,
                    fieldProductDepartment, fieldGender),
                list(
                    fieldValuesYear,
                    fieldValuesProductFamily,
                    fieldValueProductDepartment,
                    list("M")),
                cubeNameSales,
                measureUnitSales);

        assertFalse(detailedBatch.canBatch(aggregationBatch));
        assertFalse(aggregationBatch.canBatch(detailedBatch));
    }

    public void
        testCanBatchForBatchesFromSameAggregationButDifferentRollupOption()
    {
        final TestContext testContext = getTestContext();
        final BatchLoader fbcr = createFbcr(null, salesCube);

        BatchLoader.Batch batch1 =
            createBatch(
                testContext,
                fbcr,
                list(tableTime),
                list(fieldYear),
                list(fieldValuesYear),
                cubeNameSales,
                measureUnitSales);

        BatchLoader.Batch batch2 =
            createBatch(
                testContext,
                fbcr,
                list(tableTime, tableTime, tableTime),
                list(fieldYear, "quarter", "month_of_year"),
                list(
                    fieldValuesYear,
                    list("Q1", "Q2", "Q3", "Q4"),
                    list(
                        "1", "2", "3", "4", "5", "6", "7", "8",
                        "9", "10", "11", "12")),
                cubeNameSales,
                measureUnitSales);

        // Until MONDRIAN-1001 is fixed, behavior is flaky due to interaction
        // with previous tests.
        final boolean batch2CanBatch1 = batch2.canBatch(batch1);
        final boolean batch1CanBatch2 = batch1.canBatch(batch2);
        if (Bug.BugMondrian1001Fixed) {
            if (MondrianProperties.instance().UseAggregates.get()
                && MondrianProperties.instance().ReadAggregates.get())
            {
                assertFalse(batch2CanBatch1);
                assertFalse(batch1CanBatch2);
            } else {
                assertTrue(batch2CanBatch1);
            }
        }
    }

    /**
     * Tests that Can Batch For Batch With Super Set Of Constraint
     * Column Bit Key And Different Values For Overlapping Columns.
     */
    public void testSuperSetDifferentValues() {
        final TestContext testContext = getTestContext();
        final BatchLoader fbcr = createFbcr(null, salesCube);

        BatchLoader.Batch aggregationBatch =
            createBatch(
                testContext,
                fbcr,
                list(
                    tableTime, tableProductClass, tableProductClass),
                list(
                    fieldYear, fieldProductFamily, fieldProductDepartment),
                list(
                    list("1997"),
                    fieldValuesProductFamily,
                    fieldValueProductDepartment),
                cubeNameSales,
                measureUnitSales);

        BatchLoader.Batch detailedBatch =
            createBatch(
                testContext,
                fbcr,
                list(
                    tableTime, tableProductClass,
                    tableProductClass, tableCustomer),
                list(
                    fieldYear, fieldProductFamily,
                    fieldProductDepartment, fieldGender),
                list(
                    list("1998"),
                    fieldValuesProductFamily,
                    fieldValueProductDepartment, fieldValuesGender),
                cubeNameSales,
                measureUnitSales);

        assertFalse(detailedBatch.canBatch(aggregationBatch));
        assertFalse(aggregationBatch.canBatch(detailedBatch));
    }

    public void testCanBatchForBatchWithDifferentAggregationTable() {
        final TestContext testContext = getTestContext();
        final Dialect dialect = testContext.getDialect();
        final Dialect.DatabaseProduct product = dialect.getDatabaseProduct();
        switch (product) {
        case TERADATA:
        case INFOBRIGHT:
        case NEOVIEW:
            // On Teradata, Infobright and Neoview we don't create aggregate
            // tables, so this test will fail.
            return;
        }

        final BatchLoader fbcr = createFbcr(null, salesCube);

        BatchLoader.Batch summaryBatch =
            createBatch(
                testContext,
                fbcr,
                list(tableTime),
                list(fieldYear),
                list(fieldValuesYear),
                cubeNameSales,
                measureUnitSales);

        BatchLoader.Batch detailedBatch =
            createBatch(
                testContext,
                fbcr,
                list(tableTime, tableCustomer),
                list(fieldYear, fieldGender),
                list(fieldValuesYear, fieldValuesGender),
                cubeNameSales,
                measureUnitSales);

        if (MondrianProperties.instance().UseAggregates.get()
            && MondrianProperties.instance().ReadAggregates.get())
        {
            assertFalse(detailedBatch.canBatch(summaryBatch));
            assertFalse(summaryBatch.canBatch(detailedBatch));
        } else {
            assertTrue(detailedBatch.canBatch(summaryBatch));
            assertFalse(summaryBatch.canBatch(detailedBatch));
        }
    }

    public void testCannotBatchTwoBatchesAtTheSameLevel() {
        final TestContext testContext = getTestContext();
        final BatchLoader fbcr = createFbcr(null, salesCube);

        BatchLoader.Batch firstBatch =
            createBatch(
                testContext,
                fbcr,
                list(
                    tableTime,
                    tableProductClass,
                    tableProductClass),
                list(
                    fieldYear,
                    fieldProductFamily,
                    fieldProductDepartment),
                list(
                    fieldValuesYear,
                    list("Food"),
                    fieldValueProductDepartment),
                cubeNameSales,
                "[Measures].[Customer Count]");

        BatchLoader.Batch secondBatch =
            createBatch(
                testContext,
                fbcr,
                list(
                    tableTime,
                    tableProductClass,
                    tableProductClass),
                list(
                    fieldYear,
                    fieldProductFamily,
                    fieldProductDepartment),
                list(
                    fieldValuesYear,
                    list("Drink"),
                    fieldValueProductDepartment),
                cubeNameSales,
                "[Measures].[Customer Count]");

        assertFalse(firstBatch.canBatch(secondBatch));
        assertFalse(secondBatch.canBatch(firstBatch));
    }

    public void testCompositeBatchLoadAggregation() throws Exception {
        final TestContext testContext = getTestContext();
        if (!testContext.getDialect().supportsGroupingSets()) {
            return;
        }
        final BatchLoader fbcr = createFbcr(null, salesCube);

        BatchLoader.Batch summaryBatch =
            createBatch(
                testContext,
                fbcr,
                list(tableTime, tableProductClass, tableProductClass),
                list(fieldYear, fieldProductFamily, fieldProductDepartment),
                list(
                    fieldValuesYear,
                    fieldValuesProductFamily,
                    fieldValueProductDepartment),
                cubeNameSales,
                measureUnitSales);

        BatchLoader.Batch detailedBatch =
            createBatch(
                testContext,
                fbcr,
                list(
                    tableTime,
                    tableProductClass,
                    tableProductClass,
                    tableCustomer),
                list(
                    fieldYear,
                    fieldProductFamily,
                    fieldProductDepartment,
                    fieldGender),
                list(
                    fieldValuesYear,
                    fieldValuesProductFamily,
                    fieldValueProductDepartment,
                    fieldValuesGender),
                cubeNameSales,
                measureUnitSales);

        final BatchLoader.CompositeBatch compositeBatch =
            new BatchLoader.CompositeBatch(detailedBatch);

        compositeBatch.add(summaryBatch);

        final List<Future<Map<Segment, SegmentWithData>>> segmentFutures =
            new ArrayList<Future<Map<Segment, SegmentWithData>>>();
        MondrianServer.forConnection(
            testContext.getConnection())
                .getAggregationManager().cacheMgr.execute(
                    new SegmentCacheManager.Command<Void>() {
                        private final Locus locus =
                            Locus.peek();
                        public Void call() throws Exception {
                            compositeBatch.load(segmentFutures);
                            return null;
                        }
                        public Locus getLocus() {
                            return locus;
                        }
                    });

        assertEquals(1, segmentFutures.size());
        assertEquals(2, segmentFutures.get(0).get().size());
        // The order of the segments is not deterministic, so we need to
        // iterate over the segments and find a match for the batch.
        // If none are found, we fail.
        boolean found = false;
        for (Segment seg : segmentFutures.get(0).get().keySet()) {
            if (detailedBatch.getConstrainedColumnsBitKey()
                .equals(seg.getConstrainedColumnsBitKey()))
            {
                found = true;
                break;
            }
        }
        if (!found) {
            fail("No bitkey match found.");
        }
        found = false;
        for (Segment seg : segmentFutures.get(0).get().keySet()) {
            if (summaryBatch.getConstrainedColumnsBitKey()
                .equals(seg.getConstrainedColumnsBitKey()))
            {
                found = true;
                break;
            }
        }
        if (!found) {
            fail("No bitkey match found.");
        }
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
        final Dialect dialect = getTestContext().getDialect();
        switch (dialect.getDatabaseProduct()) {
        case ORACLE:
            // Oracle gives 'feature not supported' in Express 10.2
        case ACCESS:
        case TERADATA:
            // Teradata gives "Syntax error: expected something between '(' and
            // the 'select' keyword." in 12.0.
        case NEOVIEW:
            // Neoview gives "ERROR[4008] A subquery is not allowed inside an
            // aggregate function."
        case NETEZZA:
            // Netezza gives an "ERROR:  Correlated Subplan expressions not
            // supported"
        case GREENPLUM:
            // Greenplum says 'Does not support yet that query'
            return;
        }

        String cube =
            "<Cube name=\"Warehouse2\">"
            + "   <Table name=\"warehouse\"/>"
            + "   <DimensionUsage name=\"Store Type\" source=\"Store Type\" foreignKey=\"stores_id\"/>"
            + "   <Measure name=\"Count Distinct of Warehouses (Large Owned)\" aggregator=\"distinct count\" formatString=\"#,##0\">"
            + "       <MeasureExpression>"
            + "       <SQL dialect=\"generic\">(select `warehouse_class`.`warehouse_class_id` AS `warehouse_class_id` from `warehouse_class` AS `warehouse_class` where `warehouse_class`.`warehouse_class_id` = `warehouse`.`warehouse_class_id` and `warehouse_class`.`description` = 'Large Owned')</SQL>"
            + "       </MeasureExpression>"
            + "   </Measure>"
            + "   <Measure name=\"Count Distinct of Warehouses (Large Independent)\" aggregator=\"distinct count\" formatString=\"#,##0\">"
            + "       <MeasureExpression>"
            + "       <SQL dialect=\"generic\">(select `warehouse_class`.`warehouse_class_id` AS `warehouse_class_id` from `warehouse_class` AS `warehouse_class` where `warehouse_class`.`warehouse_class_id` = `warehouse`.`warehouse_class_id` and `warehouse_class`.`description` = 'Large Independent')</SQL>"
            + "       </MeasureExpression>"
            + "   </Measure>"
            + "   <Measure name=\"Count All of Warehouses (Large Independent)\" aggregator=\"count\" formatString=\"#,##0\">"
            + "       <MeasureExpression>"
            + "           <SQL dialect=\"generic\">(select `warehouse_class`.`warehouse_class_id` AS `warehouse_class_id` from `warehouse_class` AS `warehouse_class` where `warehouse_class`.`warehouse_class_id` = `warehouse`.`warehouse_class_id` and `warehouse_class`.`description` = 'Large Independent')</SQL>"
            + "       </MeasureExpression>"
            + "   </Measure>"
            + "   <Measure name=\"Count Distinct Store+Warehouse\" aggregator=\"distinct count\" formatString=\"#,##0\">"
            + "       <MeasureExpression><SQL dialect=\"generic\">`stores_id`+`warehouse_id`</SQL></MeasureExpression>"
            + "   </Measure>"
            + "   <Measure name=\"Count All Store+Warehouse\" aggregator=\"count\" formatString=\"#,##0\">"
            + "       <MeasureExpression><SQL dialect=\"generic\">`stores_id`+`warehouse_id`</SQL></MeasureExpression>"
            + "   </Measure>"
            + "   <Measure name=\"Store Count\" column=\"stores_id\" aggregator=\"count\" formatString=\"#,###\"/>"
            + "</Cube>";
        cube = cube.replaceAll("`", dialect.getQuoteIdentifierString());
        if (dialect.getDatabaseProduct() == Dialect.DatabaseProduct.ORACLE) {
            cube = cube.replaceAll(" AS ", " ");
        }

        String query =
            "select "
            + "   [Store Type].Children on rows, "
            + "   {[Measures].[Count Distinct of Warehouses (Large Owned)],"
            + "    [Measures].[Count Distinct of Warehouses (Large Independent)],"
            + "    [Measures].[Count All of Warehouses (Large Independent)],"
            + "    [Measures].[Count Distinct Store+Warehouse],"
            + "    [Measures].[Count All Store+Warehouse],"
            + "    [Measures].[Store Count]} on columns "
            + "from [Warehouse2]";

        TestContext testContext =
            TestContext.instance().legacy().create(
                null,
                cube,
                null,
                null,
                null,
                null);

        String desiredResult =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Count Distinct of Warehouses (Large Owned)]}\n"
            + "{[Measures].[Count Distinct of Warehouses (Large Independent)]}\n"
            + "{[Measures].[Count All of Warehouses (Large Independent)]}\n"
            + "{[Measures].[Count Distinct Store+Warehouse]}\n"
            + "{[Measures].[Count All Store+Warehouse]}\n"
            + "{[Measures].[Store Count]}\n"
            + "Axis #2:\n"
            + "{[Store Type].[Store Type].[Deluxe Supermarket]}\n"
            + "{[Store Type].[Store Type].[Gourmet Supermarket]}\n"
            + "{[Store Type].[Store Type].[HeadQuarters]}\n"
            + "{[Store Type].[Store Type].[Mid-Size Grocery]}\n"
            + "{[Store Type].[Store Type].[Small Grocery]}\n"
            + "{[Store Type].[Store Type].[Supermarket]}\n"
            + "Row #0: 1\n"
            + "Row #0: 0\n"
            + "Row #0: 0\n"
            + "Row #0: 6\n"
            + "Row #0: 6\n"
            + "Row #0: 6\n"
            + "Row #1: 1\n"
            + "Row #1: 0\n"
            + "Row #1: 0\n"
            + "Row #1: 2\n"
            + "Row #1: 2\n"
            + "Row #1: 2\n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #3: 0\n"
            + "Row #3: 1\n"
            + "Row #3: 1\n"
            + "Row #3: 4\n"
            + "Row #3: 4\n"
            + "Row #3: 4\n"
            + "Row #4: 0\n"
            + "Row #4: 1\n"
            + "Row #4: 1\n"
            + "Row #4: 4\n"
            + "Row #4: 4\n"
            + "Row #4: 4\n"
            + "Row #5: 0\n"
            + "Row #5: 1\n"
            + "Row #5: 3\n"
            + "Row #5: 8\n"
            + "Row #5: 8\n"
            + "Row #5: 8\n";

        testContext.assertQueryReturns(query, desiredResult);

        String loadCountDistinct_luciddb1 =
            "select "
            + "\"store\".\"store_type\" as \"c0\", "
            + "count(distinct "
            + "(select \"warehouse_class\".\"warehouse_class_id\" AS \"warehouse_class_id\" "
            + "from \"warehouse_class\" AS \"warehouse_class\" "
            + "where \"warehouse_class\".\"warehouse_class_id\" = \"warehouse\".\"warehouse_class_id\" and \"warehouse_class\".\"description\" = 'Large Owned')) as \"m0\" "
            + "from \"store\" as \"store\", \"warehouse\" as \"warehouse\" "
            + "where \"warehouse\".\"stores_id\" = \"store\".\"store_id\" "
            + "group by \"store\".\"store_type\"";

        String loadCountDistinct_luciddb2 =
            "select "
            + "\"store\".\"store_type\" as \"c0\", "
            + "count(distinct "
            + "(select \"warehouse_class\".\"warehouse_class_id\" AS \"warehouse_class_id\" "
            + "from \"warehouse_class\" AS \"warehouse_class\" "
            + "where \"warehouse_class\".\"warehouse_class_id\" = \"warehouse\".\"warehouse_class_id\" and \"warehouse_class\".\"description\" = 'Large Independent')) as \"m0\" "
            + "from \"store\" as \"store\", \"warehouse\" as \"warehouse\" "
            + "where \"warehouse\".\"stores_id\" = \"store\".\"store_id\" "
            + "group by \"store\".\"store_type\"";

        String loadOtherAggs_luciddb =
            "select "
            + "\"store\".\"store_type\" as \"c0\", "
            + "count("
            + "(select \"warehouse_class\".\"warehouse_class_id\" AS \"warehouse_class_id\" "
            + "from \"warehouse_class\" AS \"warehouse_class\" "
            + "where \"warehouse_class\".\"warehouse_class_id\" = \"warehouse\".\"warehouse_class_id\" and \"warehouse_class\".\"description\" = 'Large Independent')) as \"m0\", "
            + "count(distinct \"store_id\"+\"warehouse_id\") as \"m1\", "
            + "count(\"store_id\"+\"warehouse_id\") as \"m2\", "
            + "count(\"warehouse\".\"stores_id\") as \"m3\" "
            + "from \"store\" as \"store\", \"warehouse\" as \"warehouse\" "
            + "where \"warehouse\".\"stores_id\" = \"store\".\"store_id\" "
            + "group by \"store\".\"store_type\"";

        // Derby splits into multiple statements.
        String loadCountDistinct_derby1 =
            "select \"store\".\"store_type\" as \"c0\", count(distinct (select \"warehouse_class\".\"warehouse_class_id\" AS \"warehouse_class_id\" from \"warehouse_class\" AS \"warehouse_class\" where \"warehouse_class\".\"warehouse_class_id\" = \"warehouse\".\"warehouse_class_id\" and \"warehouse_class\".\"description\" = 'Large Owned')) as \"m0\" from \"store\" as \"store\", \"warehouse\" as \"warehouse\" where \"warehouse\".\"stores_id\" = \"store\".\"store_id\" group by \"store\".\"store_type\"";
        String loadCountDistinct_derby2 =
            "select \"store\".\"store_type\" as \"c0\", count(distinct (select \"warehouse_class\".\"warehouse_class_id\" AS \"warehouse_class_id\" from \"warehouse_class\" AS \"warehouse_class\" where \"warehouse_class\".\"warehouse_class_id\" = \"warehouse\".\"warehouse_class_id\" and \"warehouse_class\".\"description\" = 'Large Independent')) as \"m0\" from \"store\" as \"store\", \"warehouse\" as \"warehouse\" where \"warehouse\".\"stores_id\" = \"store\".\"store_id\" group by \"store\".\"store_type\"";
        String loadCountDistinct_derby3 =
            "select \"store\".\"store_type\" as \"c0\", count(distinct \"store_id\"+\"warehouse_id\") as \"m0\" from \"store\" as \"store\", \"warehouse\" as \"warehouse\" where \"warehouse\".\"stores_id\" = \"store\".\"store_id\" group by \"store\".\"store_type\"";
        String loadOtherAggs_derby =
            "select \"store\".\"store_type\" as \"c0\", count((select \"warehouse_class\".\"warehouse_class_id\" AS \"warehouse_class_id\" from \"warehouse_class\" AS \"warehouse_class\" where \"warehouse_class\".\"warehouse_class_id\" = \"warehouse\".\"warehouse_class_id\" and \"warehouse_class\".\"description\" = 'Large Independent')) as \"m0\", count(\"store_id\"+\"warehouse_id\") as \"m1\", count(\"warehouse\".\"stores_id\") as \"m2\" from \"store\" as \"store\", \"warehouse\" as \"warehouse\" where \"warehouse\".\"stores_id\" = \"store\".\"store_id\" group by \"store\".\"store_type\"";

        // MySQL does it in one statement.
        String load_mysql =
            "select\n"
            + "    `store`.`store_type` as `c0`,\n"
            + "    count(distinct (select `warehouse_class`.`warehouse_class_id` AS `warehouse_class_id` from `warehouse_class` AS `warehouse_class` where `warehouse_class`.`warehouse_class_id` = `warehouse`.`warehouse_class_id` and `warehouse_class`.`description` = 'Large Owned')) as `m0`,\n"
            + "    count(distinct (select `warehouse_class`.`warehouse_class_id` AS `warehouse_class_id` from `warehouse_class` AS `warehouse_class` where `warehouse_class`.`warehouse_class_id` = `warehouse`.`warehouse_class_id` and `warehouse_class`.`description` = 'Large Independent')) as `m1`,\n"
            + "    count((select `warehouse_class`.`warehouse_class_id` AS `warehouse_class_id` from `warehouse_class` AS `warehouse_class` where `warehouse_class`.`warehouse_class_id` = `warehouse`.`warehouse_class_id` and `warehouse_class`.`description` = 'Large Independent')) as `m2`,\n"
            + "    count(distinct `stores_id`+`warehouse_id`) as `m3`,\n"
            + "    count(`stores_id`+`warehouse_id`) as `m4`,\n"
            + "    count(`warehouse`.`stores_id`) as `m5`\n"
            + "from\n"
            + "    `warehouse` as `warehouse`,\n"
            + "    `store` as `store`\n"
            + "where\n"
            + "    `warehouse`.`stores_id` = `store`.`store_id`\n"
            + "group by\n"
            + "    `store`.`store_type`";

        SqlPattern[] patterns = {
            new SqlPattern(
                Dialect.DatabaseProduct.LUCIDDB,
                loadCountDistinct_luciddb1,
                loadCountDistinct_luciddb1),
            new SqlPattern(
                Dialect.DatabaseProduct.LUCIDDB,
                loadCountDistinct_luciddb2,
                loadCountDistinct_luciddb2),
            new SqlPattern(
                Dialect.DatabaseProduct.LUCIDDB,
                loadOtherAggs_luciddb,
                loadOtherAggs_luciddb),

            new SqlPattern(
                Dialect.DatabaseProduct.DERBY,
                loadCountDistinct_derby1,
                loadCountDistinct_derby1),
            new SqlPattern(
                Dialect.DatabaseProduct.DERBY,
                loadCountDistinct_derby2,
                loadCountDistinct_derby2),
            new SqlPattern(
                Dialect.DatabaseProduct.DERBY,
                loadCountDistinct_derby3,
                loadCountDistinct_derby3),
            new SqlPattern(
                Dialect.DatabaseProduct.DERBY,
                loadOtherAggs_derby,
                loadOtherAggs_derby),

            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL,
                load_mysql,
                load_mysql),
        };

        assertQuerySql(testContext, query, patterns);
    }

    public void testAggregateDistinctCount() {
        // solve_order=1 says to aggregate [CA] and [OR] before computing their
        // sums
        assertQueryReturns(
            "WITH MEMBER [Time].[Time].[1997 Q1 plus Q2] AS 'AGGREGATE({[Time].[1997].[Q1], [Time].[1997].[Q2]})', solve_order=1\n"
            + "SELECT {[Measures].[Customer Count]} ON COLUMNS,\n"
            + "      {[Time].[1997].[Q1], [Time].[1997].[Q2], [Time].[1997 Q1 plus Q2]} ON ROWS\n"
            + "FROM Sales\n"
            + "WHERE ([Store].[USA].[CA])",
            "Axis #0:\n"
            + "{[Store].[Stores].[USA].[CA]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Customer Count]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[1997].[Q1]}\n"
            + "{[Time].[Time].[1997].[Q2]}\n"
            + "{[Time].[Time].[1997 Q1 plus Q2]}\n"
            + "Row #0: 1,110\n"
            + "Row #1: 1,173\n"
            + "Row #2: 1,854\n");
    }

    /**
     *  As {@link #testAggregateDistinctCount()}, but (a) calc member includes
     * members from different levels and (b) also display [unit sales].
     */
    public void testAggregateDistinctCount2() {
        assertQueryReturns(
            "WITH MEMBER [Time].[Time].[1997 Q1 plus July] AS\n"
            + " 'AGGREGATE({[Time].[1997].[Q1], [Time].[1997].[Q3].[7]})', solve_order=1\n"
            + "SELECT {[Measures].[Unit Sales], [Measures].[Customer Count]} ON COLUMNS,\n"
            + "      {[Time].[1997].[Q1],\n"
            + "       [Time].[1997].[Q2],\n"
            + "       [Time].[1997].[Q3].[7],\n"
            + "       [Time].[1997 Q1 plus July]} ON ROWS\n"
            + "FROM Sales\n"
            + "WHERE ([Store].[USA].[CA])",
            "Axis #0:\n"
            + "{[Store].[Stores].[USA].[CA]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Customer Count]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[1997].[Q1]}\n"
            + "{[Time].[Time].[1997].[Q2]}\n"
            + "{[Time].[Time].[1997].[Q3].[7]}\n"
            + "{[Time].[Time].[1997 Q1 plus July]}\n"
            + "Row #0: 16,890\n"
            + "Row #0: 1,110\n"
            + "Row #1: 18,052\n"
            + "Row #1: 1,173\n"
            + "Row #2: 5,403\n"
            + "Row #2: 412\n"
            // !!!
            + "Row #3: 22,293\n"
            // = 16,890 + 5,403
            + "Row #3: 1,386\n"); // between 1,110 and 1,110 + 412
    }

    /**
     * As {@link #testAggregateDistinctCount2()}, but with two calc members
     * simultaneously.
     */
    public void testAggregateDistinctCount3() {
        assertQueryReturns(
            "WITH\n"
            + "  MEMBER [Promotion].[Media Type].[TV plus Radio] AS 'AGGREGATE({[Promotion].[Media Type].[TV], [Promotion].[Media Type].[Radio]})', solve_order=1\n"
            + "  MEMBER [Time].[Time].[1997 Q1 plus July] AS 'AGGREGATE({[Time].[1997].[Q1], [Time].[1997].[Q3].[7]})', solve_order=1\n"
            + "SELECT {[Promotion].[Media Type].[TV plus Radio],\n"
            + "        [Promotion].[Media Type].[TV],\n"
            + "        [Promotion].[Media Type].[Radio]} ON COLUMNS,\n"
            + "       {[Time].[1997],\n"
            + "        [Time].[1997].[Q1],\n"
            + "        [Time].[1997 Q1 plus July]} ON ROWS\n"
            + "FROM Sales\n"
            + "WHERE [Measures].[Customer Count]",
            "Axis #0:\n"
            + "{[Measures].[Customer Count]}\n"
            + "Axis #1:\n"
            + "{[Promotion].[Media Type].[TV plus Radio]}\n"
            + "{[Promotion].[Media Type].[TV]}\n"
            + "{[Promotion].[Media Type].[Radio]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[1997]}\n"
            + "{[Time].[Time].[1997].[Q1]}\n"
            + "{[Time].[Time].[1997 Q1 plus July]}\n"
            + "Row #0: 455\n"
            + "Row #0: 274\n"
            + "Row #0: 186\n"
            + "Row #1: 139\n"
            + "Row #1: 99\n"
            + "Row #1: 40\n"
            + "Row #2: 139\n"
            + "Row #2: 99\n"
            + "Row #2: 40\n");

        // There are 9 cells in the result. 6 sql statements have to be issued
        // to fetch all of them, with each loading these cells:
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
        final String oracleSql =
            "select "
            + "\"time_by_day\".\"the_year\" as \"c0\", \"time_by_day\".\"quarter\" as \"c1\", "
            + "\"promotion\".\"media_type\" as \"c2\", "
            + "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\" "
            + "from "
            + "\"time_by_day\" \"time_by_day\", \"sales_fact_1997\" \"sales_fact_1997\", "
            + "\"promotion\" \"promotion\" "
            + "where "
            + "\"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and "
            + "\"time_by_day\".\"the_year\" = 1997 and "
            + "\"time_by_day\".\"quarter\" = 'Q1' and "
            + "\"sales_fact_1997\".\"promotion_id\" = \"promotion\".\"promotion_id\" and "
            + "\"promotion\".\"media_type\" in ('Radio', 'TV') "
            + "group by "
            + "\"time_by_day\".\"the_year\", \"time_by_day\".\"quarter\", "
            + "\"promotion\".\"media_type\"";

        final String mysqlSql =
            "select\n"
            + "    `time_by_day`.`the_year` as `c0`,\n"
            + "    `time_by_day`.`quarter` as `c1`,\n"
            + "    `promotion`.`media_type` as `c2`,\n"
            + "    count(distinct `sales_fact_1997`.`customer_id`) as `m0`\n"
            + "from\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`,\n"
            + "    `promotion` as `promotion`\n"
            + "where\n"
            + "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    `time_by_day`.`quarter` = 'Q1'\n"
            + "and\n"
            + "    `promotion`.`media_type` in ('Radio', 'TV')\n"
            + "and\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`promotion_id` = `promotion`.`promotion_id`\n"
            + "group by\n"
            + "    `time_by_day`.`the_year`,\n"
            + "    `time_by_day`.`quarter`,\n"
            + "    `promotion`.`media_type`";

        final String derbySql =
            "select "
            + "\"time_by_day\".\"the_year\" as \"c0\", \"time_by_day\".\"quarter\" as \"c1\", "
            + "\"promotion\".\"media_type\" as \"c2\", "
            + "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\" "
            + "from "
            + "\"time_by_day\" as \"time_by_day\", \"sales_fact_1997\" as \"sales_fact_1997\", "
            + "\"promotion\" as \"promotion\" "
            + "where "
            + "\"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and "
            + "\"time_by_day\".\"the_year\" = 1997 and \"time_by_day\".\"quarter\" = 'Q1' and "
            + "\"sales_fact_1997\".\"promotion_id\" = \"promotion\".\"promotion_id\" and "
            + "\"promotion\".\"media_type\" in ('Radio', 'TV') "
            + "group by "
            + "\"time_by_day\".\"the_year\", \"time_by_day\".\"quarter\", "
            + "\"promotion\".\"media_type\"";

        assertQuerySql(
            getTestContext(),
            "WITH\n"
            + "  MEMBER [Promotion].[Media Type].[TV plus Radio] AS 'AGGREGATE({[Promotion].[Media Type].[TV], [Promotion].[Media Type].[Radio]})', solve_order=1\n"
            + "  MEMBER [Time].[Time].[1997 Q1 plus July] AS 'AGGREGATE({[Time].[1997].[Q1], [Time].[1997].[Q3].[7]})', solve_order=1\n"
            + "SELECT {[Promotion].[Media Type].[TV plus Radio],\n"
            + "        [Promotion].[Media Type].[TV],\n"
            + "        [Promotion].[Media Type].[Radio]} ON COLUMNS,\n"
            + "       {[Time].[1997],\n"
            + "        [Time].[1997].[Q1],\n"
            + "        [Time].[1997 Q1 plus July]} ON ROWS\n"
            + "FROM Sales\n"
            + "WHERE [Measures].[Customer Count]",
            new SqlPattern[] {
                new SqlPattern(
                    Dialect.DatabaseProduct.ORACLE, oracleSql, oracleSql),
                new SqlPattern(
                    Dialect.DatabaseProduct.MYSQL, mysqlSql, mysqlSql),
                new SqlPattern(
                    Dialect.DatabaseProduct.DERBY, derbySql, derbySql)
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
        final String mdxQuery =
            "WITH\n"
            + "  MEMBER [Store].[Stores].[CA plus USA] AS 'AGGREGATE({[Store].[USA].[CA], [Store].[USA]})', solve_order=1\n"
            + "  MEMBER [Time].[Time].[Q1 plus July] AS 'AGGREGATE({[Time].[1997].[Q1], [Time].[1997].[Q3].[7]})', solve_order=1\n"
            + "SELECT {[Measures].[Customer Count], [Measures].[Unit Sales]} ON COLUMNS,\n"
            + "      Union({[Store].[CA plus USA]} * {[Time].[Q1 plus July]}, "
            + "      Union({[Store].[USA].[CA]} * {[Time].[Q1 plus July]},"
            + "      Union({[Store].[USA]} * {[Time].[Q1 plus July]},"
            + "      Union({[Store].[CA plus USA]} * {[Time].[1997].[Q1]},"
            + "            {[Store].[CA plus USA]} * {[Time].[1997].[Q3].[7]})))) ON ROWS\n"
            + "FROM Sales";

        String result =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Customer Count]}\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Store].[Stores].[CA plus USA], [Time].[Time].[Q1 plus July]}\n"
            + "{[Store].[Stores].[USA].[CA], [Time].[Time].[Q1 plus July]}\n"
            + "{[Store].[Stores].[USA], [Time].[Time].[Q1 plus July]}\n"
            + "{[Store].[Stores].[CA plus USA], [Time].[Time].[1997].[Q1]}\n"
            + "{[Store].[Stores].[CA plus USA], [Time].[Time].[1997].[Q3].[7]}\n"
            + "Row #0: 3,505\n"
            + "Row #0: 112,347\n"
            + "Row #1: 1,386\n"
            + "Row #1: 22,293\n"
            + "Row #2: 3,505\n"
            + "Row #2: 90,054\n"
            + "Row #3: 2,981\n"
            + "Row #3: 83,181\n"
            + "Row #4: 1,462\n"
            + "Row #4: 29,166\n";

        assertQueryReturns(mdxQuery, result);
    }

    /**
     * Fix a problem when generating predicates for distinct count aggregate
     * loading and using the aggregate function in the slicer.
     */
    public void testAggregateDistinctCount5() {
        String query =
            "With "
            + "Set [Products] as "
            + " '{[Product].[Drink], "
            + "   [Product].[Food], "
            + "   [Product].[Non-Consumable]}' "
            + "Member [Product].[Selected Products] as "
            + " 'Aggregate([Products])', SOLVE_ORDER=2 "
            + "Select "
            + " {[Store].[Store State].Members} on rows, "
            + " {[Measures].[Customer Count]} on columns "
            + "From [Sales] "
            + "Where ([Product].[Selected Products])";

        String sql =
            "select\n"
            + "    `store`.`store_state` as `c0`,\n"
            + "    `time_by_day`.`the_year` as `c1`,\n"
            + "    count(distinct `sales_fact_1997`.`customer_id`) as `m0`\n"
            + "from\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `store` as `store`,\n"
            + "    `time_by_day` as `time_by_day`\n"
            + "where\n"
            + "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    `sales_fact_1997`.`store_id` = `store`.`store_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "group by\n"
            + "    `store`.`store_state`,\n"
            + "    `time_by_day`.`the_year`";

        final TestContext testContext = getTestContext();
        assertQuerySql(testContext, query, sql);
    }

    // Test for multiple members on different levels within the same hierarchy.
    public void testAggregateDistinctCount6() {
        // CA and USA are overlapping members
        final String mdxQuery =
            "WITH "
            + " MEMBER [Store].[Stores].[Select Region] AS "
            + " 'AGGREGATE({[Store].[USA].[CA], [Store].[Mexico], [Store].[Canada], [Store].[USA].[OR]})', solve_order=1\n"
            + " MEMBER [Time].[Time].[Select Time Period] AS "
            + " 'AGGREGATE({[Time].[1997].[Q1], [Time].[1997].[Q3].[7], [Time].[1997].[Q4], [Time].[1997]})', solve_order=1\n"
            + "SELECT {[Measures].[Customer Count], [Measures].[Unit Sales]} ON COLUMNS,\n"
            + "      Union({[Store].[Select Region]} * {[Time].[Select Time Period]},"
            + "      Union({[Store].[Select Region]} * {[Time].[1997].[Q1]},"
            + "      Union({[Store].[Select Region]} * {[Time].[1997].[Q3].[7]},"
            + "      Union({[Store].[Select Region]} * {[Time].[1997].[Q4]},"
            + "            {[Store].[Select Region]} * {[Time].[1997]})))) "
            + "ON ROWS\n"
            + "FROM Sales";

        String result =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Customer Count]}\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Store].[Stores].[Select Region], [Time].[Time].[Select Time Period]}\n"
            + "{[Store].[Stores].[Select Region], [Time].[Time].[1997].[Q1]}\n"
            + "{[Store].[Stores].[Select Region], [Time].[Time].[1997].[Q3].[7]}\n"
            + "{[Store].[Stores].[Select Region], [Time].[Time].[1997].[Q4]}\n"
            + "{[Store].[Stores].[Select Region], [Time].[Time].[1997]}\n"
            + "Row #0: 3,753\n"
            + "Row #0: 229,496\n"
            + "Row #1: 1,877\n"
            + "Row #1: 36,177\n"
            + "Row #2: 845\n"
            + "Row #2: 13,123\n"
            + "Row #3: 2,073\n"
            + "Row #3: 37,789\n"
            + "Row #4: 3,753\n"
            + "Row #4: 142,407\n";

        assertQueryReturns(mdxQuery, result);
    }

    /**
     * Test case for bug 1785406 to fix "query already contains alias"
     * exception.
     *
     * <p>Note: 1785406 is a regression from checkin 9710. Code changes made in
     * 9710 is no longer in use (and removed). So this bug will not occur;
     * however, keeping the test case here to get some coverage for a query with
     * a slicer.
     */
    public void testDistinctCountBug1785406() {
        String query =
            "With \n"
            + "Set [*BASE_MEMBERS_Product] as {[Product].[All Products].[Food].[Deli]}\n"
            + "Set [*BASE_MEMBERS_Store] as {[Store].[All Stores].[USA].[WA]}\n"
            + "Member [Product].[*CTX_MEMBER_SEL~SUM] As Aggregate([*BASE_MEMBERS_Product])\n"
            + "Select\n"
            + "{[Measures].[Customer Count]} on columns,\n"
            + "NonEmptyCrossJoin([*BASE_MEMBERS_Store],{([Product].[*CTX_MEMBER_SEL~SUM])})\n"
            + "on rows\n"
            + "From [Sales]\n"
            + "where ([Time].[1997])";

        assertQueryReturns(
            query,
            "Axis #0:\n"
            + "{[Time].[Time].[1997]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Customer Count]}\n"
            + "Axis #2:\n"
            + "{[Store].[Stores].[USA].[WA], [Product].[Products].[*CTX_MEMBER_SEL~SUM]}\n"
            + "Row #0: 889\n");

        String mysqlSql =
            "select\n"
            + "    `store`.`store_state` as `c0`,\n"
            + "    `time_by_day`.`the_year` as `c1`,\n"
            + "    count(distinct `sales_fact_1997`.`customer_id`) as `m0`\n"
            + "from\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `store` as `store`,\n"
            + "    `time_by_day` as `time_by_day`,\n"
            + "    `product` as `product`,\n"
            + "    `product_class` as `product_class`\n"
            + "where\n"
            + "    `store`.`store_state` = 'WA'\n"
            + "and\n"
            + "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    (`product_class`.`product_family` = 'Food' and `product_class`.`product_department` = 'Deli')\n"
            + "and\n"
            + "    `sales_fact_1997`.`store_id` = `store`.`store_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
            + "and\n"
            + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
            + "group by\n"
            + "    `store`.`store_state`,\n"
            + "    `time_by_day`.`the_year`";

        String accessSql =
            "select `d0` as `c0`,"
            + " `d1` as `c1`,"
            + " count(`m0`) as `c2` "
            + "from (select distinct `store`.`store_state` as `d0`,"
            + " `time_by_day`.`the_year` as `d1`,"
            + " `sales_fact_1997`.`customer_id` as `m0` "
            + "from `store` as `store`,"
            + " `sales_fact_1997` as `sales_fact_1997`,"
            + " `time_by_day` as `time_by_day`,"
            + " `product_class` as `product_class`,"
            + " `product` as `product` "
            + "where `sales_fact_1997`.`store_id` = `store`.`store_id` "
            + "and `store`.`store_state` = 'WA' "
            + "and `sales_fact_1997`.`time_id` = `time_by_day`.`time_id` "
            + "and `time_by_day`.`the_year` = 1997 "
            + "and `sales_fact_1997`.`product_id` = `product`.`product_id` "
            + "and `product`.`product_class_id` = `product_class`.`product_class_id` "
            + "and (`product_class`.`product_department` = 'Deli' "
            + "and `product_class`.`product_family` = 'Food')) as `dummyname` "
            + "group by `d0`, `d1`";

        String derbySql =
            "select "
            + "\"store\".\"store_state\" as \"c0\", "
            + "\"time_by_day\".\"the_year\" as \"c1\", "
            + "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\" "
            + "from "
            + "\"store\" as \"store\", "
            + "\"sales_fact_1997\" as \"sales_fact_1997\", "
            + "\"time_by_day\" as \"time_by_day\", "
            + "\"product_class\" as \"product_class\", "
            + "\"product\" as \"product\" "
            + "where "
            + "\"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" "
            + "and \"store\".\"store_state\" = 'WA' "
            + "and \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" "
            + "and \"time_by_day\".\"the_year\" = 1997 "
            + "and \"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\" "
            + "and \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\" "
            + "and (\"product_class\".\"product_department\" = 'Deli' "
            + "and \"product_class\".\"product_family\" = 'Food') "
            + "group by \"store\".\"store_state\", \"time_by_day\".\"the_year\"";

        SqlPattern[] patterns = {
            new SqlPattern(
                Dialect.DatabaseProduct.ACCESS, accessSql, accessSql),
            new SqlPattern(Dialect.DatabaseProduct.DERBY, derbySql, derbySql),
            new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysqlSql, mysqlSql)};

        final TestContext testContext = getTestContext();
        assertQuerySql(testContext, query, patterns);
    }

    public void testDistinctCountBug1785406_2() {
        String query =
            "With "
            + "Member [Product].[x] as 'Aggregate({Gender.CurrentMember})'\n"
            + "member [Measures].[foo] as '([Product].[x],[Measures].[Customer Count])'\n"
            + "select Filter([Gender].members,(Not IsEmpty([Measures].[foo]))) on 0 "
            + "from Sales";

        assertQueryReturns(
            query,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Customer].[Gender].[All Gender]}\n"
            + "{[Customer].[Gender].[F]}\n"
            + "{[Customer].[Gender].[M]}\n"
            + "Row #0: 266,773\n"
            + "Row #0: 131,558\n"
            + "Row #0: 135,215\n");

        String sql =
            "select\n"
            + "    `time_by_day`.`the_year` as `c0`,\n"
            + "    count(distinct `sales_fact_1997`.`customer_id`) as `m0`\n"
            + "from\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`\n"
            + "where\n"
            + "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "group by\n"
            + "    `time_by_day`.`the_year`";

        final TestContext testContext = getTestContext();
        assertQuerySql(testContext, query, sql);
    }

    public void testAggregateDistinctCount2ndParameter() {
        // simple case of count distinct measure as second argument to
        // Aggregate().  Should apply distinct-count aggregator (MONDRIAN-2016)
        assertQueryReturns(
            "with\n"
            + "  set periods as [Time].[1997].[Q1].[1] : [Time].[1997].[Q4].[10]\n"
            + "  member [Time].[Time].[agg] as Aggregate(periods, [Measures].[Customer Count])\n"
            + "select\n"
            + "  [Time].[Time].[agg]  ON COLUMNS,\n"
            + "  [Gender].[M] on ROWS\n"
            + "FROM [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time].[Time].[agg]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Gender].[M]}\n"
            + "Row #0: 2,651\n");
        assertQueryReturns(
            "WITH MEMBER [Measures].[My Distinct Count] AS \n"
            + "'AGGREGATE([1997].Children, [Measures].[Customer Count])' \n"
            + "SELECT {[Measures].[My Distinct Count], [Measures].[Customer Count]} ON COLUMNS,\n"
            + "{[1997].Children} ON ROWS\n"
            + "FROM Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[My Distinct Count]}\n"
            + "{[Measures].[Customer Count]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[1997].[Q1]}\n"
            + "{[Time].[Time].[1997].[Q2]}\n"
            + "{[Time].[Time].[1997].[Q3]}\n"
            + "{[Time].[Time].[1997].[Q4]}\n"
            + "Row #0: 5,581\n"
            + "Row #0: 2,981\n"
            + "Row #1: 5,581\n"
            + "Row #1: 2,973\n"
            + "Row #2: 5,581\n"
            + "Row #2: 3,026\n"
            + "Row #3: 5,581\n"
            + "Row #3: 3,261\n");
    }

    public void testCountDistinctAggWithOtherCountDistinctInContext() {
        // tests that Aggregate( <set>, <count-distinct measure>) aggregates
        // the correct measure when a *different* count-distinct measure is
        // in context (MONDRIAN-2128)
        TestContext testContext = TestContext.instance().legacy().create(
            null,
            "<Cube name=\"2CountDistincts\" defaultMeasure=\"Store Count\">\n"
            + "  <Table name=\"sales_fact_1997\"/>\n"
            + "    <DimensionUsage name=\"Time\" source=\"Time\" "
            + "foreignKey=\"time_id\"/>"
            + "  <DimensionUsage name=\"Store\" source=\"Store\" "
            + "foreignKey=\"store_id\"/>\n"
            + "    <DimensionUsage name=\"Product\" source=\"Product\" "
            + "  foreignKey=\"product_id\"/>"
            + "  <Measure name=\"Store Count\" column=\"store_id\" "
            + "aggregator=\"distinct-count\"/>\n"
            + "  <Measure name=\"Customer Count\" column=\"customer_id\" "
            + "aggregator=\"distinct-count\"/>\n"
            + "  <Measure name=\"Unit Sales\" column=\"unit_sales\" "
            + "aggregator=\"sum\"/>\n"
            + "</Cube>",
            null,
            null,
            null,
            null);
        // We should get the same answer whether the default [Store Count]
        // measure is in context or [Unit Sales].  The measure specified in the
        // second param of Aggregate() should be used.
        final String queryStoreCountInContext =
            "with member Store.agg as "
            + "'aggregate({[Store].[USA].[CA],[Store].[USA].[OR]}, "
            + "           measures.[Customer Count])'"
            + " select Store.agg on 0 from [2CountDistincts] ";
        final String queryUnitSalesInContext =
            "with member Store.agg as "
            + "'aggregate({[Store].[USA].[CA],[Store].[USA].[OR]}, "
            + "           measures.[Customer Count])'"
            + " select Store.agg on 0 from [2CountDistincts] where "
            + "measures.[Unit Sales] ";
        assertQueriesReturnSimilarResults(
            queryStoreCountInContext, queryUnitSalesInContext, testContext);

        final String queryCAORRollup =
            "with member measures.agg as "
            + "'aggregate({[Store].[USA].[CA],[Store].[USA].[OR]}, "
            + "           measures.[Customer Count])'"
            + " select {measures.agg, measures.[Customer Count]} on 0,  "
            + " [Product].[All Products].children on 1 "
            + "from [2CountDistincts] ";
        testContext.assertQueryReturns(
            queryCAORRollup,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[agg]}\n"
            + "{[Measures].[Customer Count]}\n"
            + "Axis #2:\n"
            + "{[Product].[Product].[Drink]}\n"
            + "{[Product].[Product].[Food]}\n"
            + "{[Product].[Product].[Non-Consumable]}\n"
            + "Row #0: 2,243\n"
            + "Row #0: 3,485\n"
            + "Row #1: 3,711\n"
            + "Row #1: 5,525\n"
            + "Row #2: 2,957\n"
            + "Row #2: 4,468\n");

        // [Customer Count] should override context
        testContext.assertQueryReturns(
            "with member Store.agg as "
            + "'aggregate({[Store].[USA].[CA],[Store].[USA].[OR]}, "
            + "           measures.[Customer Count])'"
            + " select {measures.[Store Count], measures.[Customer Count]} on 0,  "
            + " [Store].agg on 1 "
            + "from [2CountDistincts] ",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Count]}\n"
            + "{[Measures].[Customer Count]}\n"
            + "Axis #2:\n"
            + "{[Store].[Store].[agg]}\n"
            + "Row #0: 3,753\n"
            + "Row #0: 3,753\n");
        // aggregate should pick up measure in context
        testContext.assertQueryReturns(
            "with member Store.agg as "
            + "'aggregate({[Store].[USA].[CA],[Store].[USA].[OR]})'"
            + " select {measures.[Store Count], measures.[Customer Count]} on 0,  "
            + " [Store].agg on 1 "
            + "from [2CountDistincts] ",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Count]}\n"
            + "{[Measures].[Customer Count]}\n"
            + "Axis #2:\n"
            + "{[Store].[Store].[agg]}\n"
            + "Row #0: 6\n"
            + "Row #0: 3,753\n");
    }

    public void testContextSetCorrectlyWith2ParamAggregate() {
        // Aggregate with a second parameter may change context.  Verify
        // the evaluator is restored.   The query below would return
        // the [Unit Sales] value instead of [Store Sales] if context was
        // not restored.
        assertQueryReturns(
            "with \n"
            + "member [Store].Stores.cond as 'iif( \n"
            + "aggregate({[Store].[Stores].[All Stores].[USA]}, measures.[unit sales])\n"
            + " > 70000, ([Store].Stores.[All Stores], measures.currentMember), 0)'\n"
            + "select [Store].Stores.cond on 0 from sales\n"
            + "where measures.[store sales]\n",
            "Axis #0:\n"
            + "{[Measures].[Store Sales]}\n"
            + "Axis #1:\n"
            + "{[Store].[Stores].[cond]}\n"
            + "Row #0: 565,238.13\n");
    }

    public void testAggregateDistinctCountInDimensionFilter() {
        String query =
            "With "
            + "Set [Products] as '{[Product].[All Products].[Drink], [Product].[All Products].[Food]}' "
            + "Set [States] as '{[Store].[All Stores].[USA].[CA], [Store].[All Stores].[USA].[OR]}' "
            + "Member [Product].[Selected Products] as 'Aggregate([Products])', SOLVE_ORDER=2 "
            + "Select "
            + "Filter([States], not IsEmpty([Measures].[Customer Count])) on rows, "
            + "{[Measures].[Customer Count]} on columns "
            + "From [Sales] "
            + "Where ([Product].[Selected Products])";

        assertQueryReturns(
            query,
            "Axis #0:\n"
            + "{[Product].[Products].[Selected Products]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Customer Count]}\n"
            + "Axis #2:\n"
            + "{[Store].[Stores].[USA].[CA]}\n"
            + "{[Store].[Stores].[USA].[OR]}\n"
            + "Row #0: 2,692\n"
            + "Row #1: 1,036\n");

        String sql =
            "select\n"
            + "    `store`.`store_state` as `c0`,\n"
            + "    `time_by_day`.`the_year` as `c1`,\n"
            + "    count(distinct `sales_fact_1997`.`customer_id`) as `m0`\n"
            + "from\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `store` as `store`,\n"
            + "    `time_by_day` as `time_by_day`,\n"
            + "    `product` as `product`,\n"
            + "    `product_class` as `product_class`\n"
            + "where\n"
            + "    `store`.`store_state` in ('CA', 'OR')\n"
            + "and\n"
            + "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    ((`product_class`.`product_family` in ('Drink', 'Food')))\n"
            + "and\n"
            + "    `sales_fact_1997`.`store_id` = `store`.`store_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
            + "and\n"
            + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
            + "group by\n"
            + "    `store`.`store_state`,\n"
            + "    `time_by_day`.`the_year`";

        final TestContext testContext = getTestContext();
        assertQuerySql(testContext, query, sql);
    }

    public static class MyDelegatingInvocationHandler
        extends DelegatingInvocationHandler
    {
        private final Dialect dialect;
        private final boolean supportsGroupingSets;

        private MyDelegatingInvocationHandler(
            Dialect dialect,
            boolean supportsGroupingSets)
        {
            this.dialect = dialect;
            this.supportsGroupingSets = supportsGroupingSets;
        }

        protected Object getTarget() {
            return dialect;
        }

        /**
         * Handler for
         * {@link mondrian.spi.Dialect#supportsGroupingSets()}.
         *
         * @return whether dialect supports GROUPING SETS syntax
         */
        public boolean supportsGroupingSets() {
            return supportsGroupingSets;
        }
    }

    public void testInMemoryAggSum() throws Exception {
        // Double arrays
        final Object[] dblSet1 =
            new Double[] {null, 0.0, 1.1, 2.4};
        final Object[] dblSet2 =
            new Double[] {null, null, null};
        final Object[] dblSet3 =
            new Double[] {};
        final Object[] dblSet4 =
            new Double[] {2.7, 1.9};

        // Arrays of ints
        final Object[] intSet1 =
            new Integer[] {null, 0, 1, 4};
        final Object[] intSet2 =
            new Integer[] {null, null, null};
        final Object[] intSet3 =
            new Integer[] {};
        final Object[] intSet4 =
            new Integer[] {3, 7};

        // Test with double
        Assert.assertEquals(
            3.5,
            RolapAggregator.Sum.aggregate(
                Arrays.asList(dblSet1),
                Dialect.Datatype.Numeric));
        Assert.assertEquals(
            null,
            RolapAggregator.Sum.aggregate(
                Arrays.asList(dblSet2),
                Dialect.Datatype.Numeric));
        try {
            RolapAggregator.Sum.aggregate(
                Arrays.asList(dblSet3),
                Dialect.Datatype.Numeric);
            fail();
        } catch (AssertionError e) {
            // ok.
        }
        Assert.assertEquals(
            4.6,
            RolapAggregator.Sum.aggregate(
                Arrays.asList(dblSet4),
                Dialect.Datatype.Numeric));

        // test with int
        Assert.assertEquals(
            5,
            RolapAggregator.Sum.aggregate(
                Arrays.asList(intSet1),
                Dialect.Datatype.Integer));
        Assert.assertEquals(
            null,
            RolapAggregator.Sum.aggregate(
                Arrays.asList(intSet2),
                Dialect.Datatype.Integer));
        try {
            RolapAggregator.Sum.aggregate(
                Arrays.asList(intSet3),
                Dialect.Datatype.Integer);
            fail();
        } catch (AssertionError e) {
            // ok.
        }
        Assert.assertEquals(
            10,
            RolapAggregator.Sum.aggregate(
                Arrays.asList(intSet4),
                Dialect.Datatype.Integer));
    }

    public void testInMemoryAggMin() throws Exception {
        // Double arrays
        final Object[] dblSet1 =
            new Double[] {null, 0.0, 1.1, 2.4};
        final Object[] dblSet2 =
            new Double[] {null, null, null};
        final Object[] dblSet3 =
            new Double[] {};
        final Object[] dblSet4 =
            new Double[] {2.7, 1.9};

        // Arrays of ints
        final Object[] intSet1 =
            new Integer[] {null, 0, 1, 4};
        final Object[] intSet2 =
            new Integer[] {null, null, null};
        final Object[] intSet3 =
            new Integer[] {};
        final Object[] intSet4 =
            new Integer[] {3, 7};

        // Test with double
        Assert.assertEquals(
            0.0,
            RolapAggregator.Min.aggregate(
                Arrays.asList(dblSet1),
                Dialect.Datatype.Numeric));
        Assert.assertEquals(
            null,
            RolapAggregator.Min.aggregate(
                Arrays.asList(dblSet2),
                Dialect.Datatype.Numeric));
        try {
            RolapAggregator.Min.aggregate(
                Arrays.asList(dblSet3),
                Dialect.Datatype.Numeric);
            fail();
        } catch (AssertionError e) {
            // ok.
        }
        Assert.assertEquals(
            1.9,
            RolapAggregator.Min.aggregate(
                Arrays.asList(dblSet4),
                Dialect.Datatype.Numeric));

        // test with int
        Assert.assertEquals(
            0,
            RolapAggregator.Min.aggregate(
                Arrays.asList(intSet1),
                Dialect.Datatype.Integer));
        Assert.assertEquals(
            null,
            RolapAggregator.Min.aggregate(
                Arrays.asList(intSet2),
                Dialect.Datatype.Integer));
        try {
            RolapAggregator.Min.aggregate(
                Arrays.asList(intSet3),
                Dialect.Datatype.Integer);
            fail();
        } catch (AssertionError e) {
            // ok.
        }
        Assert.assertEquals(
            3,
            RolapAggregator.Min.aggregate(
                Arrays.asList(intSet4),
                Dialect.Datatype.Integer));
    }

    public void testInMemoryAggMax() throws Exception {
        // Double arrays
        final Object[] dblSet1 =
            new Double[] {null, 0.0, 1.1, 2.4};
        final Object[] dblSet2 =
            new Double[] {null, null, null};
        final Object[] dblSet3 =
            new Double[] {};
        final Object[] dblSet4 =
            new Double[] {2.7, 1.9};

        // Arrays of ints
        final Object[] intSet1 =
            new Integer[] {null, 0, 1, 4};
        final Object[] intSet2 =
            new Integer[] {null, null, null};
        final Object[] intSet3 =
            new Integer[] {};
        final Object[] intSet4 =
            new Integer[] {3, 7};

        // Test with double
        Assert.assertEquals(
            2.4,
            RolapAggregator.Max.aggregate(
                Arrays.asList(dblSet1),
                Dialect.Datatype.Numeric));
        Assert.assertEquals(
            null,
            RolapAggregator.Max.aggregate(
                Arrays.asList(dblSet2),
                Dialect.Datatype.Numeric));
        try {
            RolapAggregator.Max.aggregate(
                Arrays.asList(dblSet3),
                Dialect.Datatype.Numeric);
            fail();
        } catch (AssertionError e) {
            // ok.
        }
        Assert.assertEquals(
            2.7,
            RolapAggregator.Max.aggregate(
                Arrays.asList(dblSet4),
                Dialect.Datatype.Numeric));

        // test with int
        Assert.assertEquals(
            4,
            RolapAggregator.Max.aggregate(
                Arrays.asList(intSet1),
                Dialect.Datatype.Integer));
        Assert.assertEquals(
            null,
            RolapAggregator.Max.aggregate(
                Arrays.asList(intSet2),
                Dialect.Datatype.Integer));
        try {
            RolapAggregator.Max.aggregate(
                Arrays.asList(intSet3),
                Dialect.Datatype.Integer);
            fail();
        } catch (AssertionError e) {
            // ok.
        }
        Assert.assertEquals(
            7,
            RolapAggregator.Max.aggregate(
                Arrays.asList(intSet4),
                Dialect.Datatype.Integer));
    }

    private static class Bacon {
        // It's just bacon.
    };
}

// End FastBatchingCellReaderTest.java
