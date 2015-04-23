/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2007-2008 Bart Pappyn
// Copyright (C) 2007-2015 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.rolap.cache.HardSmartCache;
import mondrian.spi.impl.*;
import mondrian.test.FoodMartTestCase;
import mondrian.test.TestContext;
import mondrian.util.Bug;
import mondrian.util.Pair;

import junit.framework.TestCase;

import java.util.*;

/**
 * Tests for testing the DataSourceChangeListener plugin.
 *
 * @author Bart Pappyn
 * @since Jan 05, 2007
 */
public class DataSourceChangeListenerTest extends BatchTestCase {

    public DataSourceChangeListenerTest() {
        super();
    }

    public DataSourceChangeListenerTest(String name) {
        super(name);
    }

    /**
     * Tests whether the data source plugin is able to tell mondrian
     * to read the hierarchy and aggregates again.
     */
    public void testDataSourceChangeListenerPlugin() {
        final MondrianProperties properties = MondrianProperties.instance();
        if (properties.TestExpDependencies.get() > 0) {
            // Dependency testing produces side-effects in the cache.
            return;
        }
        // got to clean out the cache
        final TestContext testContext = getTestContext();
        final mondrian.olap.CacheControl cacheControl =
            testContext.getConnection().getCacheControl(null);

        // Flush the entire cache.
        final Connection connection = testContext.getConnection();
        final mondrian.olap.Cube salesCube =
            connection.getSchema().lookupCube("Sales", true);
        final mondrian.olap.CacheControl.CellRegion measuresRegion =
            cacheControl.createMeasuresRegion(salesCube);
        cacheControl.flush(measuresRegion);

        // turn on caching
        propSaver.set(properties.DisableCaching, false);

        cacheControl.flushSchemaCache();

        // Use hard caching for testing. When using soft references, we can not
        // test caching because things may be garbage collected during the
        // tests.
        SmartMemberReader smr = getSmartMemberReader("Store");
        MemberCacheHelper smrch = (MemberCacheHelper)smr.getMemberCache();

        MemberCacheHelper rcsmrch =
            ((RolapCubeHierarchy.RolapCubeHierarchyMemberReader) smr)
                .getRolapCubeMemberCacheHelper();

        SmartMemberReader ssmr = getSharedSmartMemberReader("Store");
        MemberCacheHelper ssmrch = (MemberCacheHelper)ssmr.getMemberCache();

        clearAndHardenCache(ssmrch);
        clearAndHardenCache(rcsmrch);
        clearAndHardenCache(smrch);

        // Create a dummy DataSource which will throw a 'bomb' if it is asked
        // to execute a particular SQL statement, but will otherwise behave
        // exactly the same as the current DataSource.
        SqlLogger sqlLogger = new SqlLogger();
        RolapUtil.setHook(sqlLogger);

        try {
            String s1, s2, s3, s4, s5, s6;

            // Flush the cache, to ensure that the query gets executed.
            Result r1 = executeQuery(
                "select {[Store].[All Stores].[USA].[CA].[San Francisco]} on columns from [Sales]");
            Util.discard(r1);
            s1 = sqlLogger.getSqlQueries().toString();
            sqlLogger.clear();
            // s1 should not be empty

            assertFalse("[]".equals(s1));

            // Run query again, to make sure only cache is used
            Result r2 = executeQuery(
                "select {[Store].[All Stores].[USA].[CA].[San Francisco]} on columns from [Sales]");
            Util.discard(r2);
            s2 = sqlLogger.getSqlQueries().toString();
            sqlLogger.clear();
            assertEquals("[]", s2);

            // Attach dummy change listener that tells mondrian the
            // datasource is never changed.
            smrch.changeListener = new DataSourceChangeListenerImpl();
            ssmrch.changeListener = new DataSourceChangeListenerImpl();
            rcsmrch.changeListener = new DataSourceChangeListenerImpl();

            // Run query again, to make sure only cache is used
            Result r3 = executeQuery(
                "select {[Store].[All Stores].[USA].[CA].[San Francisco]} on columns from [Sales]");
            Util.discard(r3);
            s3 = sqlLogger.getSqlQueries().toString();
            sqlLogger.clear();
            assertEquals("[]", s3);

            // Manually clear the cache to make compare sql result later on
            clearAndHardenCache(smrch);
            clearAndHardenCache(ssmrch);
            clearAndHardenCache(rcsmrch);

            // Run query again, to make sure only cache is used
            Result r4 = executeQuery(
                "select {[Store].[All Stores].[USA].[CA].[San Francisco]} "
                + "on columns from [Sales]");
            Util.discard(r4);
            s4 = sqlLogger.getSqlQueries().toString();

            sqlLogger.clear();
            assertFalse("[]".equals(s4));

            // Attach dummy change listener that tells mondrian the
            // datasource is always changed.
            smrch.changeListener = new DataSourceChangeListenerImpl2();
            ssmrch.changeListener = new DataSourceChangeListenerImpl2();
            rcsmrch.changeListener = new DataSourceChangeListenerImpl2();
            // Run query again, to make sure only cache is used
            Result r5 = executeQuery(
                "select {[Store].[All Stores].[USA].[CA].[San Francisco]} "
                + "on columns from [Sales]");
            Util.discard(r5);
            s5 = sqlLogger.getSqlQueries().toString();
            sqlLogger.clear();
            assertEquals(s4, s5);

            // Attach dummy change listener that tells mondrian the datasource
            // is always changed and tells that aggregate cache is always
            // cached.
            smrch.changeListener = new DataSourceChangeListenerImpl3();
            ssmrch.changeListener = new DataSourceChangeListenerImpl3();
            rcsmrch.changeListener = new DataSourceChangeListenerImpl3();

            RolapStar star = getStar("Sales");
            star.setChangeListener(smrch.changeListener);
            // Run query again, to make sure only cache is used
            Result r6 = executeQuery(
                "select {[Store].[All Stores].[USA].[CA].[San Francisco]} "
                + "on columns from [Sales]");
            Util.discard(r6);
            s6 = sqlLogger.getSqlQueries().toString();
            sqlLogger.clear();
            assertEquals(s1, s6);
        } finally {
            smrch.changeListener = null;
            ssmrch.changeListener = null;
            rcsmrch.changeListener = null;

            RolapStar star = getStar("Sales");
            star.setChangeListener(null);

            RolapUtil.setHook(null);
        }
    }

    /**
     * Tests whether the flushing of the cache is thread safe.
     * <p>This test is deactivated because it isn't thread safe
     * and causes hangs every now and then. The datasource change
     * listener is also something deprecated and already removed
     * from Mondrian 4.0, so we don't anticipate to fix this issue.
     */
    public void dont_testParallelDataSourceChangeListenerPlugin() {
        if (true) {
            return;
        }
        if (Bug.avoidSlowTestOnLucidDB(getTestContext().getDialect())) {
            return;
        }
        // 5 threads, 8 cycles each
        checkCacheFlushing(5, 8);
    }

    /**
     * Tests several threads, each of which is creating connections and
     * periodically flushing the schema cache.
     *
     * @param workerCount Number of worker threads
     * @param cycleCount Number of cycles each thread should perform
     */
    private void checkCacheFlushing(
        final int workerCount,
        final int cycleCount)
    {
        final Random random = new Random(123456);
        Worker[] workers = new Worker[workerCount];
        Thread[] threads = new Thread[workerCount];


        final String[] queries = {
            "with member [Store Type].[All Types] as 'Aggregate({[Store Type].[All Store Types].[Deluxe Supermarket],  "
            + "[Store Type].[All Store Types].[Gourmet Supermarket],  "
            + "[Store Type].[All Store Types].[HeadQuarters],  "
            + "[Store Type].[All Store Types].[Mid-Size Grocery],  "
            + "[Store Type].[All Store Types].[Small Grocery],  "
            + "[Store Type].[All Store Types].[Supermarket]})'  "
            + "select NON EMPTY {[Time].[1997]} ON COLUMNS,   "
            + "NON EMPTY [Store].[All Stores].[USA].[CA].Children ON ROWS   "
            + "from [Sales] "
            + "where ([Store Type].[All Types], [Measures].[Unit Sales], [Customers].[All Customers].[USA], [Product].[All Products].[Drink])  ",

            "with member [Measures].[Shipped per Ordered] as ' [Measures].[Units Shipped] / [Measures].[Unit Sales] ', format_string='#.00%'\n"
            + " member [Measures].[Profit per Unit Shipped] as ' [Measures].[Profit] / [Measures].[Units Shipped] '\n"
            + "select\n"
            + " {[Measures].[Unit Sales], \n"
            + "  [Measures].[Units Shipped],\n"
            + "  [Measures].[Shipped per Ordered],\n"
            + "  [Measures].[Profit per Unit Shipped]} on 0,\n"
            + " NON EMPTY Crossjoin([Product].Children, [Time].[1997].Children) on 1\n"
            + "from [Warehouse and Sales]",

            "select {[Measures].[Profit Per Unit Shipped]} ON COLUMNS, "
            + "{[Store].[All Stores].[USA].[CA], [Store].[All Stores].[USA].[OR], [Store].[All Stores].[USA].[WA]} ON ROWS "
            + "from [Warehouse and Sales Format Expression Cube No Cache] "
            + "where [Time].[1997]",

            "select {[Store].[All Stores].[USA].[CA].[San Francisco]} on columns from [Sales]"
        };
        final String[] results = {
            "Axis #0:\n"
            + "{[Store Type].[All Types], [Measures].[Unit Sales], [Customers].[USA], [Product].[Drink]}\n"
            + "Axis #1:\n"
            + "{[Time].[1997]}\n"
            + "Axis #2:\n"
            + "{[Store].[USA].[CA].[Beverly Hills]}\n"
            + "{[Store].[USA].[CA].[Los Angeles]}\n"
            + "{[Store].[USA].[CA].[San Diego]}\n"
            + "{[Store].[USA].[CA].[San Francisco]}\n"
            + "Row #0: 1,945\n"
            + "Row #1: 2,422\n"
            + "Row #2: 2,560\n"
            + "Row #3: 175\n",

            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Units Shipped]}\n"
            + "{[Measures].[Shipped per Ordered]}\n"
            + "{[Measures].[Profit per Unit Shipped]}\n"
            + "Axis #2:\n"
            + "{[Product].[Drink], [Time].[1997].[Q1]}\n"
            + "{[Product].[Drink], [Time].[1997].[Q2]}\n"
            + "{[Product].[Drink], [Time].[1997].[Q3]}\n"
            + "{[Product].[Drink], [Time].[1997].[Q4]}\n"
            + "{[Product].[Food], [Time].[1997].[Q1]}\n"
            + "{[Product].[Food], [Time].[1997].[Q2]}\n"
            + "{[Product].[Food], [Time].[1997].[Q3]}\n"
            + "{[Product].[Food], [Time].[1997].[Q4]}\n"
            + "{[Product].[Non-Consumable], [Time].[1997].[Q1]}\n"
            + "{[Product].[Non-Consumable], [Time].[1997].[Q2]}\n"
            + "{[Product].[Non-Consumable], [Time].[1997].[Q3]}\n"
            + "{[Product].[Non-Consumable], [Time].[1997].[Q4]}\n"
            + "Row #0: 5,976\n"
            + "Row #0: 4637.0\n"
            + "Row #0: 77.59%\n"
            + "Row #0: $1.50\n"
            + "Row #1: 5,895\n"
            + "Row #1: 4501.0\n"
            + "Row #1: 76.35%\n"
            + "Row #1: $1.60\n"
            + "Row #2: 6,065\n"
            + "Row #2: 6258.0\n"
            + "Row #2: 103.18%\n"
            + "Row #2: $1.15\n"
            + "Row #3: 6,661\n"
            + "Row #3: 5802.0\n"
            + "Row #3: 87.10%\n"
            + "Row #3: $1.38\n"
            + "Row #4: 47,809\n"
            + "Row #4: 37153.0\n"
            + "Row #4: 77.71%\n"
            + "Row #4: $1.64\n"
            + "Row #5: 44,825\n"
            + "Row #5: 35459.0\n"
            + "Row #5: 79.11%\n"
            + "Row #5: $1.62\n"
            + "Row #6: 47,440\n"
            + "Row #6: 41545.0\n"
            + "Row #6: 87.57%\n"
            + "Row #6: $1.47\n"
            + "Row #7: 51,866\n"
            + "Row #7: 34706.0\n"
            + "Row #7: 66.91%\n"
            + "Row #7: $1.91\n"
            + "Row #8: 12,506\n"
            + "Row #8: 9161.0\n"
            + "Row #8: 73.25%\n"
            + "Row #8: $1.76\n"
            + "Row #9: 11,890\n"
            + "Row #9: 9227.0\n"
            + "Row #9: 77.60%\n"
            + "Row #9: $1.65\n"
            + "Row #10: 12,343\n"
            + "Row #10: 9986.0\n"
            + "Row #10: 80.90%\n"
            + "Row #10: $1.59\n"
            + "Row #11: 13,497\n"
            + "Row #11: 9291.0\n"
            + "Row #11: 68.84%\n"
            + "Row #11: $1.86\n",

            "Axis #0:\n"
            + "{[Time].[1997]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Profit Per Unit Shipped]}\n"
            + "Axis #2:\n"
            + "{[Store].[USA].[CA]}\n"
            + "{[Store].[USA].[OR]}\n"
            + "{[Store].[USA].[WA]}\n"
            + "Row #0: |1.6|style=red\n"
            + "Row #1: |2.1|style=green\n"
            + "Row #2: |1.5|style=red\n",

            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[USA].[CA].[San Francisco]}\n"
            + "Row #0: 2,117\n"
        };
        final TestContext testContext =
            TestContext.instance().create(
                null, null,
                "<Cube name=\"Warehouse No Cache\" cache=\"false\">\n"
                + "  <Table name=\"inventory_fact_1997\"/>\n"
                + "\n"
                + "  <DimensionUsage name=\"Time\" source=\"Time\" foreignKey=\"time_id\"/>\n"
                + "  <DimensionUsage name=\"Store\" source=\"Store\" foreignKey=\"store_id\"/>\n"
                + "  <Measure name=\"Units Shipped\" column=\"units_shipped\" aggregator=\"sum\" formatString=\"#.0\"/>\n"
                + "</Cube>\n"
                + "<VirtualCube name=\"Warehouse and Sales Format Expression Cube No Cache\">\n"
                + "  <VirtualCubeDimension name=\"Store\"/>\n"
                + "  <VirtualCubeDimension name=\"Time\"/>\n"
                + "  <VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Store Cost]\"/>\n"
                + "  <VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Store Sales]\"/>\n"
                + "  <VirtualCubeMeasure cubeName=\"Warehouse No Cache\" name=\"[Measures].[Units Shipped]\"/>\n"
                + "  <CalculatedMember name=\"Profit\" dimension=\"Measures\">\n"
                + "    <Formula>[Measures].[Store Sales] - [Measures].[Store Cost]</Formula>\n"
                + "  </CalculatedMember>\n"
                + "  <CalculatedMember name=\"Profit Per Unit Shipped\" dimension=\"Measures\">\n"
                + "    <Formula>[Measures].[Profit] / [Measures].[Units Shipped]</Formula>\n"
                + "    <CalculatedMemberProperty name=\"FORMAT_STRING\" expression=\"IIf(([Measures].[Profit Per Unit Shipped] > 2.0), '|0.#|style=green', '|0.#|style=red')\"/>\n"
                + "  </CalculatedMember>\n"
                + "</VirtualCube>",
                null, null, null);

        SmartMemberReader smrStore =
            getSmartMemberReader(testContext.getConnection(), "Store");
        MemberCacheHelper smrStoreCacheHelper =
            (MemberCacheHelper) smrStore.getMemberCache();
        SmartMemberReader smrProduct =
            getSmartMemberReader(testContext.getConnection(), "Product");
        MemberCacheHelper smrProductCacheHelper =
            (MemberCacheHelper) smrProduct.getMemberCache();

        // 1/500 of the time, the hierarchies are flushed
        // 1/50 of the time, the aggregates are flushed
        smrStoreCacheHelper.changeListener =
            new DataSourceChangeListenerImpl4(500, 50);
        smrProductCacheHelper.changeListener =
            smrStoreCacheHelper.changeListener;

        RolapStar star = getStar(testContext.getConnection(), "Sales");
        star.setChangeListener(smrStoreCacheHelper.changeListener);

        star = getStar(testContext.getConnection(), "Warehouse No Cache");
        star.setChangeListener(smrStoreCacheHelper.changeListener);


        for (int i = 0; i < workerCount; i++) {
            workers[i] = new Worker() {
                public void runSafe() {
                    for (int i = 0; i < cycleCount; ++i) {
                        cycle();
                        try {
                            // Sleep up to 100ms.
                            Thread.sleep(random.nextInt(100));
                        } catch (InterruptedException e) {
                            throw Util.newInternal(e, "interrupted");
                        }
                    }
                }

                private void cycle() {
                    int idx = random.nextInt(4);
                    String query = queries[idx];
                    String result = results[idx];
                    testContext.assertQueryReturns(query, result);
                }
            };
            threads[i] = new Thread(workers[i]);
        }
        for (int i = 0; i < workerCount; i++) {
            threads[i].start();
        }
        for (int i = 0; i < workerCount; i++) {
            try {
                threads[i].join();
            } catch (InterruptedException e) {
                throw Util.newInternal(e, "while joining thread #" + i);
            }
        }
        List<String> messages = new ArrayList<String>();
        for (Worker worker : workers) {
            for (Throwable throwable : worker.failures) {
                messages.add(TestContext.getStackTrace(throwable));
            }
        }
        if (!messages.isEmpty()) {
            fail(messages.size() + " threads failed\n" + messages);
        }
    }

    private static abstract class Worker implements Runnable {
        final List<Throwable> failures = new ArrayList<Throwable>();

        public void run() {
            try {
                runSafe();
            } catch (Throwable e) {
                synchronized (failures) {
                    failures.add(e);
                }
            }
        }
        public abstract void runSafe();
    }

    private static class SqlLogger implements RolapUtil.ExecuteQueryHook {
        private final List<String> sqlQueries;

        public SqlLogger() {
            this.sqlQueries = new ArrayList<String>();
        }

        public void clear() {
            sqlQueries.clear();
        }

        public List<String> getSqlQueries() {
            return sqlQueries;
        }

        public void onExecuteQuery(String sql) {
            // We can safely ignore the count requests. These are
            // generated by the segment builder and are not always
            // needed across queries, despite the cache flushing.
            if (!sql.startsWith("select count(")) {
                sqlQueries.add(sql);
            }
        }
    }

//    Result executeQuery(String mdx, Connection connection) {
//        Query query = connection.parseQuery(mdx);
//        return connection.execute(query);
//    }

    SmartMemberReader getSmartMemberReader(String hierName) {
        Connection con = getTestContext().getConnection();
        return getSmartMemberReader(con, hierName);
    }

    SmartMemberReader getSmartMemberReader(Connection con, String hierName) {
        RolapCube cube = (RolapCube) con.getSchema().lookupCube("Sales", true);
        RolapSchemaReader schemaReader =
            (RolapSchemaReader) cube.getSchemaReader();
        RolapHierarchy hierarchy =
            (RolapHierarchy) cube.lookupHierarchy(
                new Id.NameSegment(hierName, Id.Quoting.UNQUOTED), false);
        assertNotNull(hierarchy);
        return
            (SmartMemberReader) hierarchy.createMemberReader(
                schemaReader.getRole());
    }

    SmartMemberReader getSharedSmartMemberReader(String hierName) {
        Connection con = getTestContext().getConnection();
        return getSharedSmartMemberReader(con, hierName);
    }

    SmartMemberReader getSharedSmartMemberReader(
        Connection con,
        String hierName)
    {
        RolapCube cube = (RolapCube) con.getSchema().lookupCube("Sales", true);
        RolapSchemaReader schemaReader =
            (RolapSchemaReader) cube.getSchemaReader();
        RolapCubeHierarchy hierarchy =
            (RolapCubeHierarchy) cube.lookupHierarchy(
                new Id.NameSegment(hierName, Id.Quoting.UNQUOTED), false);
        assertNotNull(hierarchy);
        return (SmartMemberReader) hierarchy.getRolapHierarchy()
            .createMemberReader(schemaReader.getRole());
    }

    RolapStar getStar(String starName) {
        Connection con = getTestContext().getConnection();
        return getStar(con, starName);
    }

    RolapStar getStar(Connection con, String starName) {
        RolapCube cube = (RolapCube) con.getSchema().lookupCube(starName, true);
        return cube.getStar();
    }
}

// End DataSourceChangeListenerTest.java
