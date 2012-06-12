/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 Julian Hyde
// Copyright (C) 2005-2012 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.calc.ResultStyle;
import mondrian.olap.*;
import mondrian.rolap.RolapNative.*;
import mondrian.rolap.agg.*;
import mondrian.server.Execution;
import mondrian.server.Locus;
import mondrian.spi.Dialect;
import mondrian.test.*;

import org.apache.log4j.Logger;

import org.eigenbase.util.property.IntegerProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * To support all <code>Batch</code> related tests.
 *
 * @author Thiyagu
  * @since 06-Jun-2007
 */
public class BatchTestCase extends FoodMartTestCase {

    public BatchTestCase(String name) {
        super(name);
    }

    public BatchTestCase() {
    }

    protected final String tableTime = "time_by_day";
    protected final String tableProductClass = "product_class";
    protected final String tableCustomer = "customer";
    protected final String fieldYear = "the_year";
    protected final String fieldProductFamily = "product_family";
    protected final String fieldProductDepartment = "product_department";
    protected final String[] fieldValuesYear = {"1997"};
    protected final String[] fieldValuesProductFamily = {
        "Food", "Non-Consumable", "Drink"
    };
    protected final String[] fieldValueProductDepartment = {
        "Alcoholic Beverages", "Baked Goods", "Baking Goods",
         "Beverages", "Breakfast Foods", "Canned Foods",
        "Canned Products", "Carousel", "Checkout", "Dairy",
        "Deli", "Eggs", "Frozen Foods", "Health and Hygiene",
        "Household", "Meat", "Packaged Foods", "Periodicals",
        "Produce", "Seafood", "Snack Foods", "Snacks",
        "Starchy Foods"
    };
    protected final String[] fieldValuesGender = {"M", "F"};
    protected final String cubeNameSales = "Sales";
    protected final String measureUnitSales = "[Measures].[Unit Sales]";
    protected String fieldGender = "gender";

    protected BatchLoader.Batch createBatch(
        BatchLoader fbcr,
        String[] tableNames, String[] fieldNames, String[][] fieldValues,
        String cubeName, String measure)
    {
        List<String> values = new ArrayList<String>();
        for (int i = 0; i < tableNames.length; i++) {
            values.add(fieldValues[i][0]);
        }
        BatchLoader.Batch batch = fbcr.new Batch(
            createRequest(
                cubeName, measure, tableNames, fieldNames,
                values.toArray(new String[values.size()])));

        addRequests(
            batch, cubeName, measure, tableNames, fieldNames,
            fieldValues, new ArrayList<String>(), 0);
        return batch;
    }

    protected BatchLoader.Batch createBatch(
        BatchLoader fbcr,
        String[] tableNames, String[] fieldNames, String[][] fieldValues,
        String cubeName, String measure, CellRequestConstraint constraint)
    {
        List<String> values = new ArrayList<String>();
        for (int i = 0; i < tableNames.length; i++) {
            values.add(fieldValues[i][0]);
        }
        BatchLoader.Batch batch = fbcr.new Batch(
            createRequest(
                cubeName, measure, tableNames, fieldNames,
                values.toArray(new String[values.size()]), constraint));

        addRequests(
            batch, cubeName, measure, tableNames, fieldNames,
            fieldValues, new ArrayList<String>(), 0, constraint);
        return batch;
    }

    private void addRequests(
        BatchLoader.Batch batch,
        String cubeName,
        String measure,
        String[] tableNames,
        String[] fieldNames,
        String[][] fieldValues,
        List<String> selectedValues,
        int currPos)
    {
        if (currPos < fieldNames.length) {
            for (int j = 0; j < fieldValues[currPos].length; j++) {
                selectedValues.add(fieldValues[currPos][j]);
                addRequests(
                    batch, cubeName, measure, tableNames,
                    fieldNames, fieldValues, selectedValues, currPos + 1);
                selectedValues.remove(fieldValues[currPos][j]);
            }
        } else {
            batch.add(
                createRequest(
                    cubeName, measure, tableNames, fieldNames,
                    selectedValues.toArray(new String[selectedValues.size()])));
        }
    }

    private void addRequests(
        BatchLoader.Batch batch,
        String cubeName,
        String measure,
        String[] tableNames,
        String[] fieldNames,
        String[][] fieldValues,
        List<String> selectedValues,
        int currPos,
        CellRequestConstraint constraint)
    {
        if (currPos < fieldNames.length) {
            for (int j = 0; j < fieldValues[currPos].length; j++) {
                selectedValues.add(fieldValues[currPos][j]);
                addRequests(
                    batch, cubeName, measure, tableNames,
                    fieldNames, fieldValues, selectedValues, currPos + 1,
                    constraint);
                selectedValues.remove(fieldValues[currPos][j]);
            }
        } else {
            batch.add(
                createRequest(
                    cubeName, measure, tableNames, fieldNames,
                    selectedValues.toArray(
                        new String[selectedValues.size()]), constraint));
        }
    }

    protected GroupingSet getGroupingSet(
        Execution execution,
        String[] tableNames,
        String[] fieldNames,
        String[][] fieldValues,
        String cubeName,
        String measure)
    {
        final RolapCube cube = getCube(cubeName);
        final BatchLoader fbcr =
            new BatchLoader(
                Locus.peek(),
                execution.getMondrianStatement().getMondrianConnection()
                    .getServer().getAggregationManager().cacheMgr,
                cube.getStar().getSqlQueryDialect(),
                cube);
        BatchLoader.Batch batch =
            createBatch(
                fbcr,
                tableNames, fieldNames,
                fieldValues, cubeName,
                measure);
        GroupingSetsCollector collector = new GroupingSetsCollector(true);
        final List<Future<Map<Segment, SegmentWithData>>> segmentFutures =
            new ArrayList<Future<Map<Segment, SegmentWithData>>>();
        batch.loadAggregation(collector, segmentFutures);
        return collector.getGroupingSets().get(0);
    }

    /**
     * Checks that a given sequence of cell requests results in a
     * particular SQL statement being generated.
     *
     * <p>Always clears the cache before running the requests.
     *
     * <p>Runs the requests once for each SQL pattern in the current
     * dialect. If there are multiple patterns, runs the MDX query multiple
     * times, and expects to see each SQL statement appear. If there are no
     * patterns in this dialect, the test trivially succeeds.
     *
     * @param requests Sequence of cell requests
     * @param patterns Set of patterns
     */
    protected void assertRequestSql(
        CellRequest[] requests,
        SqlPattern[] patterns)
    {
        assertRequestSql(requests, patterns, false);
    }

    /**
     * Checks that a given sequence of cell requests results in a
     * particular SQL statement being generated.
     *
     * <p>Always clears the cache before running the requests.
     *
     * <p>Runs the requests once for each SQL pattern in the current
     * dialect. If there are multiple patterns, runs the MDX query multiple
     * times, and expects to see each SQL statement appear. If there are no
     * patterns in this dialect, the test trivially succeeds.
     *
     * @param requests Sequence of cell requests
     * @param patterns Set of patterns
     * @param negative Set to false in order to 'expect' a query or
     * true to 'forbid' a query.
     */
    protected void assertRequestSql(
        CellRequest[] requests,
        SqlPattern[] patterns,
        boolean negative)
    {
        final RolapStar star = requests[0].getMeasure().getStar();
        final String cubeName = requests[0].getMeasure().getCubeName();
        final RolapCube cube = lookupCube(cubeName);
        final Dialect sqlDialect = star.getSqlQueryDialect();
        Dialect.DatabaseProduct d = sqlDialect.getDatabaseProduct();
        SqlPattern sqlPattern = SqlPattern.getPattern(d, patterns);
        if (d == Dialect.DatabaseProduct.UNKNOWN) {
            // If the dialect is not one in the pattern set, do not run the
            // test. We do not print any warning message.
            return;
        }

        boolean patternFound = false;
        for (SqlPattern pattern : patterns) {
            if (!pattern.hasDatabaseProduct(d)) {
                continue;
            }

            patternFound = true;

            clearCache(cube);

            String sql = sqlPattern.getSql();
            String trigger = sqlPattern.getTriggerSql();
            switch (d) {
            case ORACLE:
                sql = sql.replaceAll(" =as= ", " ");
                trigger = trigger.replaceAll(" =as= ", " ");
                break;
            case TERADATA:
                sql = sql.replaceAll(" =as= ", " as ");
                trigger = trigger.replaceAll(" =as= ", " as ");
                break;
            }

            // Create a dummy DataSource which will throw a 'bomb' if it is
            // asked to execute a particular SQL statement, but will otherwise
            // behave exactly the same as the current DataSource.
            RolapUtil.setHook(new TriggerHook(trigger));
            Bomb bomb;
            final Execution execution =
                new Execution(
                    ((RolapConnection) getConnection()).getInternalStatement(),
                    1000);
            final AggregationManager aggMgr =
                execution.getMondrianStatement()
                    .getMondrianConnection()
                    .getServer().getAggregationManager();
            final Locus locus =
                new Locus(
                    execution,
                    "BatchTestCase",
                    "BatchTestCase");
            try {
                FastBatchingCellReader fbcr =
                    new FastBatchingCellReader(
                        execution, getCube(cubeName), aggMgr);
                for (CellRequest request : requests) {
                    fbcr.recordCellRequest(request);
                }
                // The FBCR will presume there is a current Locus in the stack,
                // so let's create a mock one.
                Locus.push(locus);
                fbcr.loadAggregations();
                bomb = null;
            } catch (Bomb e) {
                bomb = e;
            } finally {
                RolapUtil.setHook(null);
                Locus.pop(locus);
            }
            if (!negative && bomb == null) {
                fail("expected query [" + sql + "] did not occur");
            } else if (negative && bomb != null) {
                fail("forbidden query [" + sql + "] detected");
            }
            TestContext.assertEqualsVerbose(
                replaceQuotes(sql),
                replaceQuotes(bomb.sql));
        }

        // Print warning message that no pattern was specified for the current
        // dialect.
        if (!patternFound) {
            String warnDialect =
                MondrianProperties.instance().WarnIfNoPatternForDialect.get();

            if (warnDialect.equals(d.toString())) {
                System.out.println(
                    "[No expected SQL statements found for dialect \""
                    + sqlDialect.toString()
                    + "\" and test not run]");
            }
        }
    }

    private RolapCube lookupCube(String cubeName) {
        Connection connection = TestContext.instance().getConnection();
        for (Cube cube : connection.getSchema().getCubes()) {
            if (cube.getName().equals(cubeName)) {
                return (RolapCube) cube;
            }
        }
        return null;
    }

    /**
     * Checks that a given MDX query results in a particular SQL statement
     * being generated.
     *
     * @param mdxQuery MDX query
     * @param patterns Set of patterns for expected SQL statements
     */
    protected void assertQuerySql(
        String mdxQuery,
        SqlPattern[] patterns)
    {
        assertQuerySqlOrNot(
            getTestContext(), mdxQuery, patterns, false, false, true);
    }

    /**
     * Checks that a given MDX query results in a particular SQL statement
     * being generated.
     *
     * @param testContext non-default test context if required
     * @param mdxQuery MDX query
     * @param patterns Set of patterns for expected SQL statements
     */
    protected void assertQuerySql(
        TestContext testContext,
        String mdxQuery,
        SqlPattern[] patterns)
    {
        assertQuerySqlOrNot(
            testContext, mdxQuery, patterns, false, false, true);
    }

    /**
     * Checks that a given MDX query does not result in a particular SQL
     * statement being generated.
     *
     * @param mdxQuery MDX query
     * @param patterns Set of patterns for expected SQL statements
     */
    protected void assertNoQuerySql(
        String mdxQuery,
        SqlPattern[] patterns)
    {
        assertQuerySqlOrNot(
            getTestContext(), mdxQuery, patterns, true, false, true);
    }

    /**
     * Checks that a given MDX query results in a particular SQL statement
     * being generated.
     *
     * @param mdxQuery MDX query
     * @param patterns Set of patterns, one for each dialect.
     * @param clearCache whether to clear cache before running the query
     */
    protected void assertQuerySql(
        String mdxQuery,
        SqlPattern[] patterns,
        boolean clearCache)
    {
        assertQuerySqlOrNot(
            getTestContext(), mdxQuery, patterns, false, false, clearCache);
    }

    /**
     * During MDX query parse and execution, checks that the query results
     * (or does not result) in a particular SQL statement being generated.
     *
     * <p>Parses and executes the MDX query once for each SQL
     * pattern in the current dialect. If there are multiple patterns, runs the
     * MDX query multiple times, and expects to see each SQL statement appear.
     * If there are no patterns in this dialect, the test trivially succeeds.
     *
     * @param testContext non-default test context if required
     * @param mdxQuery MDX query
     * @param patterns Set of patterns
     * @param negative false to assert if SQL is generated;
     *                 true to assert if SQL is NOT generated
     * @param bypassSchemaCache whether to grab a new connection and bypass the
     *        schema cache before parsing the MDX query
     * @param clearCache whether to clear cache before executing the MDX query
     */
    protected void assertQuerySqlOrNot(
        TestContext testContext,
        String mdxQuery,
        SqlPattern[] patterns,
        boolean negative,
        boolean bypassSchemaCache,
        boolean clearCache)
    {
        Connection connection = testContext.getConnection();

        mdxQuery = testContext.upgradeQuery(mdxQuery);

        // Run the test once for each pattern in this dialect.
        // (We could optimize and run it once, collecting multiple queries, and
        // comparing all queries at the end.)
        Dialect dialect = testContext.getDialect();
        Dialect.DatabaseProduct d = dialect.getDatabaseProduct();
        boolean patternFound = false;
        for (SqlPattern sqlPattern : patterns) {
            if (!sqlPattern.hasDatabaseProduct(d)) {
                // If the dialect is not one in the pattern set, skip the
                // test. If in the end no pattern is located, print a warning
                // message if required.
                continue;
            }

            patternFound = true;

            String sql = sqlPattern.getSql();
            String trigger = sqlPattern.getTriggerSql();

            sql = dialectize(d, sql);
            trigger = dialectize(d, trigger);

            // Create a dummy DataSource which will throw a 'bomb' if it is
            // asked to execute a particular SQL statement, but will otherwise
            // behave exactly the same as the current DataSource.
            RolapUtil.setHook(new TriggerHook(trigger));
            Bomb bomb = null;
            try {
                if (bypassSchemaCache) {
                    connection =
                        testContext.withSchemaPool(false).getConnection();
                }
                final Query query = connection.parseQuery(mdxQuery);
                if (clearCache) {
                    clearCache((RolapCube)query.getCube());
                }
                final Result result = connection.execute(query);
                Util.discard(result);
                bomb = null;
            } catch (Bomb e) {
                bomb = e;
            } catch (RuntimeException e) {
                // Walk up the exception tree and see if the root cause
                // was a SQL bomb.
                bomb = Util.getMatchingCause(e, Bomb.class);
                if (bomb == null) {
                    throw e;
                }
            } finally {
                RolapUtil.setHook(null);
            }
            if (negative) {
                if (bomb != null) {
                    fail("forbidden query [" + sql + "] detected");
                }
            } else {
                if (bomb == null) {
                    fail("expected query [" + sql + "] did not occur");
                }
                assertEquals(
                    replaceQuotes(
                        sql.replaceAll("\r\n", "\n")),
                    replaceQuotes(
                        bomb.sql.replaceAll("\r\n", "\n")));
            }
        }

        // Print warning message that no pattern was specified for the current
        // dialect.
        if (!patternFound) {
            String warnDialect =
                MondrianProperties.instance().WarnIfNoPatternForDialect.get();

            if (warnDialect.equals(d.toString())) {
                System.out.println(
                    "[No expected SQL statements found for dialect \""
                    + dialect.toString()
                    + "\" and test not run]");
            }
        }
    }

    protected String dialectize(Dialect.DatabaseProduct d, String sql) {
        sql = sql.replaceAll("\r\n", "\n");
        switch (d) {
        case ORACLE:
            return sql.replaceAll(" =as= ", " ");
        case GREENPLUM:
        case POSTGRESQL:
        case TERADATA:
            return sql.replaceAll(" =as= ", " as ");
        case DERBY:
            return sql.replaceAll("`", "\"");
        case ACCESS:
            return sql.replaceAll(
                "ISNULL\\(([^)]*)\\)",
                "Iif($1 IS NULL, 1, 0)");
        default:
            return sql;
        }
    }

    private void clearCache(RolapCube cube) {
        // Clear the cache for the Sales cube, so the query runs as if
        // for the first time. (TODO: Cleaner way to do this.)
        final Cube salesCube =
            getConnection().getSchema().lookupCube("Sales", true);
        RolapHierarchy hierarchy =
            (RolapHierarchy) salesCube.lookupHierarchy(
                new Id.NameSegment("Store", Id.Quoting.UNQUOTED),
                false);
        SmartMemberReader memberReader =
            (SmartMemberReader) hierarchy.getMemberReader();
        MemberCacheHelper cacheHelper = memberReader.cacheHelper;
        cacheHelper.mapLevelToMembers.cache.clear();
        cacheHelper.mapMemberToChildren.cache.clear();

        // Flush the cache, to ensure that the query gets executed.
        cube.clearCachedAggregations(true);

        CacheControl cacheControl = getConnection().getCacheControl(null);
        final CacheControl.CellRegion measuresRegion =
            cacheControl.createMeasuresRegion(cube);
        cacheControl.flush(measuresRegion);
    }

    private static String replaceQuotes(String s) {
        s = s.replace('`', '\"');
        s = s.replace('\'', '\"');
        return s;
    }

    protected CellRequest createRequest(
        final String cube, final String measure,
        final String table, final String column, final String value)
    {
        return createRequest(
            cube, measure,
            new String[]{table}, new String[]{column}, new String[]{value});
    }

    protected CellRequest createRequest(
        final String cube, final String measureName,
        final String[] tables, final String[] columns, final String[] values)
    {
        RolapStar.Measure starMeasure = getMeasure(cube, measureName);
        CellRequest request = new CellRequest(starMeasure, false, false);
        final RolapStar star = starMeasure.getStar();
        for (int i = 0; i < tables.length; i++) {
            String table = tables[i];
            if (table != null && table.length() > 0) {
                String column = columns[i];
                String value = values[i];
                final RolapStar.Column storeTypeColumn =
                    star.lookupColumn(table, column);
                request.addConstrainedColumn(
                    storeTypeColumn,
                    new ValueColumnPredicate(storeTypeColumn, value));
            }
        }
        return request;
    }

    protected CellRequest createRequest(
        final String cube, final String measure,
        final String table, final String column, final String value,
        CellRequestConstraint aggConstraint)
    {
        return createRequest(
            cube, measure,
            new String[]{table}, new String[]{column}, new String[]{value},
            aggConstraint);
    }

    protected CellRequest createRequest(
        final String cube, final String measureName,
        final String[] tables, final String[] columns, final String[] values,
        CellRequestConstraint aggConstraint)
    {
        RolapStar.Measure starMeasure = getMeasure(cube, measureName);

        CellRequest request =
            createRequest(cube, measureName, tables, columns, values);
        final RolapStar star = starMeasure.getStar();

        request.addAggregateList(
            aggConstraint.getBitKey(star),
            aggConstraint.toPredicate(star));

        return request;
    }

    static CellRequestConstraint makeConstraintYearQuarterMonth(
        List<String[]> values)
    {
        String[] aggConstraintTables =
            new String[] { "time_by_day", "time_by_day", "time_by_day" };
        String[] aggConstraintColumns =
            new String[] { "the_year", "quarter", "month_of_year" };
        List<String[]> aggConstraintValues = new ArrayList<String[]>();

        for (String[] value : values) {
            assert value.length == 3;
            aggConstraintValues.add(value);
        }

        return new CellRequestConstraint(
            aggConstraintTables, aggConstraintColumns, aggConstraintValues);
    }

    static CellRequestConstraint makeConstraintCountryState(
        List<String[]> values)
    {
        String[] aggConstraintTables =
            new String[] { "store", "store"};
        String[] aggConstraintColumns =
            new String[] { "store_country", "store_state"};
        List<String[]> aggConstraintValues = new ArrayList<String[]>();

        for (String[] value : values) {
            assert value.length == 2;
            aggConstraintValues.add(value);
        }

        return new CellRequestConstraint(
            aggConstraintTables, aggConstraintColumns, aggConstraintValues);
    }

    static CellRequestConstraint makeConstraintProductFamilyDepartment(
        List<String[]> values)
    {
        String[] aggConstraintTables =
            new String[] { "product_class", "product_class"};
        String[] aggConstraintColumns =
            new String[] { "product_family", "product_department"};
        List<String[]> aggConstraintValues = new ArrayList<String[]>();

        for (String[] value : values) {
            assert value.length == 2;
            aggConstraintValues.add(value);
        }

        return new CellRequestConstraint(
            aggConstraintTables, aggConstraintColumns, aggConstraintValues);
    }

    protected RolapStar.Measure getMeasure(String cube, String measureName) {
        final Connection connection = getFoodMartConnection();
        final boolean fail = true;
        Cube salesCube = connection.getSchema().lookupCube(cube, fail);
        Member measure = salesCube.getSchemaReader(null).getMemberByUniqueName(
            Util.parseIdentifier(measureName), fail);
        return RolapStar.getStarMeasure(measure);
    }

    protected Connection getFoodMartConnection() {
        return TestContext.instance().getConnection();
    }

    protected RolapCube getCube(final String cube) {
        final Connection connection = getFoodMartConnection();
        final boolean fail = true;
        return (RolapCube) connection.getSchema().lookupCube(cube, fail);
    }

    /**
     * Make sure the mdx runs correctly and not in native mode.
     *
     * @param rowCount number of rows returned
     * @param mdx      query
     */
    protected void checkNotNative(int rowCount, String mdx) {
        checkNotNative(rowCount, mdx, null);
    }

    /**
     * Makes sure the MDX runs correctly and not in native mode.
     *
     * @param rowCount       Number of rows returned
     * @param mdx            Query
     * @param expectedResult Expected result string
     */
    protected void checkNotNative(
        int rowCount,
        String mdx,
        String expectedResult)
    {
        getConnection().getCacheControl(null).flushSchemaCache();
        Connection con =
            getTestContext().withSchemaPool(false).getConnection();
        RolapNativeRegistry reg = getRegistry(con);
        reg.setListener(
            new Listener() {
                public void foundEvaluator(NativeEvent e) {
                    fail("should not be executed native");
                }

                public void foundInCache(TupleEvent e) {
                }

                public void executingSql(TupleEvent e) {
                }
            });

        TestCase c = new TestCase(con, 0, rowCount, mdx);
        Result result = c.run();

        if (expectedResult != null) {
            String nonNativeResult = TestContext.toString(result);
            if (!nonNativeResult.equals(expectedResult)) {
                TestContext.assertEqualsVerbose(
                    expectedResult,
                    nonNativeResult,
                    false,
                    "Non Native implementation returned different result than "
                    + "expected; MDX=" + mdx);
            }
        }
    }

    RolapNativeRegistry getRegistry(Connection connection) {
        RolapCube cube =
            (RolapCube) connection.getSchema().lookupCube("Sales", true);
        RolapSchemaReader schemaReader =
            (RolapSchemaReader) cube.getSchemaReader();
        return schemaReader.getSchema().getNativeRegistry();
    }

    /**
     * Runs a query twice, with native crossjoin optimization enabled and
     * disabled. If both results are equal, its considered correct.
     *
     * @param resultLimit Maximum result size of all the MDX operations in this
     *                    query. This might be hard to estimate as it is usually
     *                    larger than the rowCount of the final result. Setting
     *                    it to 0 will cause this limit to be ignored.
     * @param rowCount    Number of rows returned
     * @param mdx         Query
     */
    protected void checkNative(
        int resultLimit, int rowCount, String mdx)
    {
        checkNative(resultLimit, rowCount, mdx, null, false);
    }

    /**
     * Runs a query twice, with native crossjoin optimization enabled and
     * disabled. If both results are equal,and both aggree with the expected
     * result, it is considered correct.
     *
     * <p>Optionally the query can be run with
     * fresh connection. This is useful if the test case sets its certain
     * mondrian properties, e.g. native properties like:
     * mondrian.native.filter.enable
     *
     * @param resultLimit     Maximum result size of all the MDX operations in
     *                        this query. This might be hard to estimate as it
     *                        is usually larger than the rowCount of the final
     *                        result. Setting it to 0 will cause this limit to
     *                        be ignored.
     * @param rowCount        Number of rows returned. (That is, the number
     *                        of positions on the last axis of the query.)
     * @param mdx             Query
     * @param expectedResult  Expected result string
     * @param freshConnection Whether fresh connection is required
     */
    protected void checkNative(
        int resultLimit,
        int rowCount,
        String mdx,
        String expectedResult,
        boolean freshConnection)
    {
        // Don't run the test if we're testing expression dependencies.
        // Expression dependencies cause spurious interval calls to
        // 'level.getMembers()' which create false negatives in this test.
        if (MondrianProperties.instance().TestExpDependencies.get() > 0) {
            return;
        }

        getConnection().getCacheControl(null).flushSchemaCache();
        try {
            Logger.getLogger(getClass()).debug("*** Native: " + mdx);
            boolean reuseConnection = !freshConnection;
            Connection con =
                getTestContext()
                    .withSchemaPool(reuseConnection)
                    .getConnection();
            RolapNativeRegistry reg = getRegistry(con);
            reg.useHardCache(true);
            TestListener listener = new TestListener();
            reg.setListener(listener);
            reg.setEnabled(true);
            TestCase c = new TestCase(con, resultLimit, rowCount, mdx);
            Result result = c.run();
            String nativeResult = TestContext.toString(result);
            if (!listener.isFoundEvaluator()) {
                fail("expected native execution of " + mdx);
            }
            if (!listener.isExecuteSql()) {
                fail("cache is empty: expected SQL query to be executed");
            }
            if (MondrianProperties.instance().EnableRolapCubeMemberCache.get())
            {
                // run once more to make sure that the result comes from cache
                // now
                listener.setExecuteSql(false);
                c.run();
                if (listener.isExecuteSql()) {
                    fail("expected result from cache when query runs twice");
                }
            }
            con.close();

            Logger.getLogger(getClass()).debug("*** Interpreter: " + mdx);

            getConnection().getCacheControl(null).flushSchemaCache();
            con = getTestContext().withSchemaPool(false).getConnection();
            reg = getRegistry(con);
            listener.setFoundEvaluator(false);
            reg.setListener(listener);
            // disable RolapNativeSet
            reg.setEnabled(false);
            result = executeQuery(mdx, con);
            String interpretedResult = TestContext.toString(result);
            if (listener.isFoundEvaluator()) {
                fail("did not expect native executions of " + mdx);
            }

            if (expectedResult != null) {
                TestContext.assertEqualsVerbose(
                    expectedResult,
                    nativeResult,
                    false,
                    "Native implementation returned different result than "
                    + "expected; MDX=" + mdx);
                TestContext.assertEqualsVerbose(
                    expectedResult,
                    interpretedResult,
                    false,
                    "Interpreter implementation returned different result than "
                    + "expected; MDX=" + mdx);
            }

            if (!nativeResult.equals(interpretedResult)) {
                TestContext.assertEqualsVerbose(
                    interpretedResult,
                    nativeResult,
                    false,
                    "Native implementation returned different result than "
                    + "interpreter; MDX=" + mdx);
            }
        } finally {
            Connection con = getConnection();
            RolapNativeRegistry reg = getRegistry(con);
            reg.setEnabled(true);
            reg.useHardCache(false);
        }
    }

    public static void checkNotNative(String mdx, Result expectedResult) {
        BatchTestCase test = new BatchTestCase();
        test.checkNotNative(
            getRowCount(expectedResult),
            mdx,
            TestContext.toString(expectedResult));
    }

    public static void checkNative(String mdx, Result expectedResult) {
        BatchTestCase test = new BatchTestCase();
        test.checkNative(
            0,
            getRowCount(expectedResult),
            mdx,
            TestContext.toString(expectedResult),
            true);
    }

    private static int getRowCount(Result result) {
        return result.getAxes()[result.getAxes().length - 1]
            .getPositions().size();
    }

    protected Result executeQuery(String mdx, Connection connection) {
        Query query = connection.parseQuery(mdx);
        query.setResultStyle(ResultStyle.LIST);
        return connection.execute(query);
    }

    /**
     * Convenience method for debugging; please do not delete.
     */
    public void assertNotNative(String mdx) {
        new BatchTestCase().checkNotNative(0, mdx, null);
    }

    /**
     * Convenience method for debugging; please do not delete.
     */
    public void assertNative(String mdx) {
        new BatchTestCase().checkNative(0, 0, mdx, null, true);
    }

    /**
     * Runs an MDX query with a predefined resultLimit and checks the number of
     * positions of the row axis. The reduced resultLimit ensures that the
     * optimization is present.
     */
    protected class TestCase {
        /**
         * Maximum number of rows to be read from SQL. If more than this number
         * of rows are read, the test will fail.
         */
        final int resultLimit;

        /**
         * MDX query to execute.
         */
        final String query;

        /**
         * Number of positions we expect on rows axis of result.
         */
        final int rowCount;

        /**
         * Mondrian connection.
         */
        final Connection con;

        public TestCase(int resultLimit, int rowCount, String query) {
            this.con = getConnection();
            this.resultLimit = resultLimit;
            this.rowCount = rowCount;
            this.query = query;
        }

        public TestCase(
            Connection con, int resultLimit, int rowCount, String query)
        {
            this.con = con;
            this.resultLimit = resultLimit;
            this.rowCount = rowCount;
            this.query = query;
        }

        protected Result run() {
            getConnection().getCacheControl(null).flushSchemaCache();
            IntegerProperty monLimit =
                MondrianProperties.instance().ResultLimit;
            int oldLimit = monLimit.get();
            try {
                monLimit.set(this.resultLimit);
                Result result = executeQuery(query, con);

                // Check the number of positions on the last axis, which is
                // the ROWS axis in a 2 axis query.
                int numAxes = result.getAxes().length;
                Axis a = result.getAxes()[numAxes - 1];
                final int positionCount = a.getPositions().size();
                assertEquals(rowCount, positionCount);
                return result;
            } finally {
                monLimit.set(oldLimit);
            }
        }
    }

    /**
     * Fake exception to interrupt the test when we see the desired query.
     * It is an {@link Error} because we need it to be unchecked
     * ({@link Exception} is checked), and we don't want handlers to handle
     * it.
     */
    static class Bomb extends Error {
        final String sql;

        Bomb(final String sql) {
            this.sql = sql;
        }
    }

    private static class TriggerHook implements RolapUtil.ExecuteQueryHook {
        private final String trigger;

        public TriggerHook(String trigger) {
            this.trigger = trigger.replaceAll("\r\n", "\n");
        }

        private boolean matchTrigger(String sql) {
            if (trigger == null) {
                return true;
            }
            // Cleanup the endlines.
            sql = sql.replaceAll("\r\n", "\n");
            // different versions of mysql drivers use different quoting, so
            // ignore quotes
            String s = replaceQuotes(sql);
            String t = replaceQuotes(trigger);
            return s.startsWith(t);
        }

        public void onExecuteQuery(String sql) {
            if (matchTrigger(sql)) {
                throw new Bomb(sql);
            }
        }
    }

    static class CellRequestConstraint {
        String[] tables;
        String[] columns;
        List<String[]> valueList;

        CellRequestConstraint(
            String[] tables,
            String[] columns,
            List<String[]> valueList)
        {
            this.tables = tables;
            this.columns = columns;
            this.valueList = valueList;
        }

        BitKey getBitKey(RolapStar star) {
            return star.getBitKey(tables, columns);
        }

        StarPredicate toPredicate(RolapStar star) {
            RolapStar.Column starColumn[] = new RolapStar.Column[tables.length];
            for (int i = 0; i < tables.length; i++) {
                String table = tables[i];
                String column = columns[i];
                starColumn[i] = star.lookupColumn(table, column);
            }

            List<StarPredicate> orPredList = new ArrayList<StarPredicate>();
            for (String[] values : valueList) {
                assert (values.length == tables.length);
                List<StarPredicate> andPredList =
                    new ArrayList<StarPredicate>();
                for (int i = 0; i < values.length; i++) {
                    andPredList.add(
                        new ValueColumnPredicate(starColumn[i], values[i]));
                }
                final StarPredicate predicate =
                    andPredList.size() == 1
                        ? andPredList.get(0)
                        : new AndPredicate(andPredList);
                orPredList.add(predicate);
            }

            return orPredList.size() == 1
                ? orPredList.get(0)
                : new OrPredicate(orPredList);
        }
    }

    /**
     * Gets notified on various test events:
     * <ul>
     * <li>when a matching native evaluator was found
     * <li>when SQL is executed
     * <li>when result is found in the cache
     * </ul>
     */
    static class TestListener implements Listener {
        boolean foundEvaluator;
        boolean foundInCache;
        boolean executeSql;

        boolean isExecuteSql() {
            return executeSql;
        }

        void setExecuteSql(boolean excecuteSql) {
            this.executeSql = excecuteSql;
        }

        boolean isFoundEvaluator() {
            return foundEvaluator;
        }

        void setFoundEvaluator(boolean foundEvaluator) {
            this.foundEvaluator = foundEvaluator;
        }

        boolean isFoundInCache() {
            return foundInCache;
        }

        void setFoundInCache(boolean foundInCache) {
            this.foundInCache = foundInCache;
        }

        public void foundEvaluator(NativeEvent e) {
            this.foundEvaluator = true;
        }

        public void foundInCache(TupleEvent e) {
            this.foundInCache = true;
        }

        public void executingSql(TupleEvent e) {
            this.executeSql = true;
        }
    }
}

// End BatchTestCase.java

