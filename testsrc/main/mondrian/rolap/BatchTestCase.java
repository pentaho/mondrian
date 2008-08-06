/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2008 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.test.FoodMartTestCase;
import mondrian.test.TestContext;
import mondrian.test.SqlPattern;
import mondrian.rolap.agg.*;
import mondrian.rolap.sql.SqlQuery;
import mondrian.olap.*;

import java.util.List;
import java.util.ArrayList;

/**
 * To support all <code>Batch</code> related tests.
 *
 * @author Thiyagu
 * @version $Id$
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
    protected final String[] fieldValuesProductFamily =
        {"Food", "Non-Consumable", "Drink"};
    protected final String[] fieldValueProductDepartment =
        {"Alcoholic Beverages", "Baked Goods", "Baking Goods",
         "Beverages", "Breakfast Foods", "Canned Foods",
        "Canned Products","Carousel", "Checkout", "Dairy",
        "Deli", "Eggs", "Frozen Foods", "Health and Hygiene",
        "Household", "Meat", "Packaged Foods", "Periodicals",
        "Produce", "Seafood", "Snack Foods", "Snacks",
        "Starchy Foods"};
    protected final String[] fieldValuesGender = {"M", "F"};
    protected final String cubeNameSales = "Sales";
    protected final String measureUnitSales = "[Measures].[Unit Sales]";
    protected String fieldGender = "gender";

    protected FastBatchingCellReader.Batch createBatch(
        FastBatchingCellReader fbcr,
        String[] tableNames, String[] fieldNames, String[][] fieldValues,
        String cubeName, String measure)
    {
        List<String> values = new ArrayList<String>();
        for (int i = 0; i < tableNames.length; i++) {
            values.add(fieldValues[i][0]);
        }
        FastBatchingCellReader.Batch batch = fbcr.new Batch(
            createRequest(cubeName, measure,
                tableNames, fieldNames, values.toArray(new String[0])));

        addRequests(batch, cubeName, measure, tableNames, fieldNames,
            fieldValues, new ArrayList<String>(), 0);
        return batch;

    }

    protected FastBatchingCellReader.Batch createBatch(
        FastBatchingCellReader fbcr,
        String[] tableNames, String[] fieldNames, String[][] fieldValues,
        String cubeName, String measure, CellRequestConstraint constraint)
    {
        List<String> values = new ArrayList<String>();
        for (int i = 0; i < tableNames.length; i++) {
            values.add(fieldValues[i][0]);
        }
        FastBatchingCellReader.Batch batch = fbcr.new Batch(
            createRequest(cubeName, measure,
                tableNames, fieldNames, values.toArray(new String[0]), constraint));

        addRequests(batch, cubeName, measure, tableNames, fieldNames,
            fieldValues, new ArrayList<String>(), 0, constraint);
        return batch;

    }

    private void addRequests(
        FastBatchingCellReader.Batch batch,
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
                addRequests(batch, cubeName, measure, tableNames,
                    fieldNames, fieldValues, selectedValues, currPos + 1);
                selectedValues.remove(fieldValues[currPos][j]);
            }
        } else {
            batch.add(createRequest(cubeName, measure, tableNames,
                fieldNames, selectedValues.toArray(new String[0])));
        }
    }

    private void addRequests(
        FastBatchingCellReader.Batch batch,
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
                addRequests(batch, cubeName, measure, tableNames,
                    fieldNames, fieldValues, selectedValues, currPos + 1,
                    constraint);
                selectedValues.remove(fieldValues[currPos][j]);
            }
        } else {
            batch.add(createRequest(cubeName, measure, tableNames,
                fieldNames, selectedValues.toArray(new String[0]), constraint));
        }
    }

    protected GroupingSet getGroupingSet(
        String[] tableNames, String[] fieldNames, String[][] fieldValues,
        String cubeName, String measure)
    {
        FastBatchingCellReader.Batch batch =
            createBatch(new FastBatchingCellReader(getCube(cubeName)),
                tableNames, fieldNames,
                fieldValues, cubeName,
                measure);
        GroupingSetsCollector collector = new GroupingSetsCollector(true);
        batch.loadAggregation(collector);
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
    void assertRequestSql(
        CellRequest[] requests, SqlPattern[] patterns)
    {
        final RolapStar star = requests[0].getMeasure().getStar();
        final String cubeName = requests[0].getMeasure().getCubeName();
        final RolapCube cube = lookupCube(cubeName);
        final SqlQuery.Dialect sqlDialect = star.getSqlQueryDialect();
        SqlPattern.Dialect d = SqlPattern.Dialect.get(sqlDialect);
        SqlPattern sqlPattern = SqlPattern.getPattern(d, patterns);
        if (d == SqlPattern.Dialect.UNKNOWN) {
            // If the dialect is not one in the pattern set, do not run the
            // test. We do not print any warning message.
            return;
        }

        boolean patternFound = false;
        for (SqlPattern pattern : patterns) {
            if (!pattern.hasDialect(d)) {
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
            RolapUtil.threadHooks.set(new TriggerHook(trigger));
            Bomb bomb;
            try {
                FastBatchingCellReader fbcr =
                    new FastBatchingCellReader(getCube(cubeName));
                for (CellRequest request : requests) {
                    fbcr.recordCellRequest(request);
                }
                fbcr.loadAggregations();
                bomb = null;
            } catch (Bomb e) {
                bomb = e;
            } finally {
                RolapUtil.threadHooks.set(null);
            }
            if (bomb == null) {
                fail("expected query [" + sql + "] did not occur");
            }
            TestContext.assertEqualsVerbose(
                replaceQuotes(sql),
                replaceQuotes(bomb.sql));
        }

        /*
         * Print warning message that no pattern was specified for the current dialect.
         */
        if (!patternFound) {
            String warnDialect =
                MondrianProperties.instance().WarnIfNoPatternForDialect.get();

            if (warnDialect.equals(d.toString())) {
                System.out.println(
                    "[No expected sqls found for dialect \"" +
                    sqlDialect.toString() +
                    "\" and test not run]"
                );
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
    protected void assertQuerySql(String mdxQuery, SqlPattern[] patterns) {
        assertQuerySqlOrNot(getTestContext(), mdxQuery, patterns, false, false, true);
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
        TestContext testContext, String mdxQuery, SqlPattern[] patterns) {
        assertQuerySqlOrNot(testContext, mdxQuery, patterns, false, false, true);
    }

    /**
     * Checks that a given MDX query does not result in a particular SQL
     * statement being generated.
     *
     * @param mdxQuery MDX query
     * @param patterns Set of patterns for expected SQL statements
     */
    protected void assertNoQuerySql(String mdxQuery, SqlPattern[] patterns) {
        assertQuerySqlOrNot(getTestContext(), mdxQuery, patterns, true, false, true);
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
        boolean clearCache) {
        assertQuerySqlOrNot(getTestContext(), mdxQuery, patterns, false, false, clearCache);
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

        // Run the test once for each pattern in this dialect.
        // (We could optimize and run it once, collecting multiple queries, and
        // comparing all queries at the end.)
        SqlQuery.Dialect dialect = testContext.getDialect();
        SqlPattern.Dialect d = SqlPattern.Dialect.get(dialect);
        boolean patternFound = false;
        for (SqlPattern sqlPattern : patterns) {
            if (!sqlPattern.hasDialect(d)) {
                // If the dialect is not one in the pattern set, skip the
                // test. If in the end no pattern is located, print a warning
                // message if required.
                continue;
            }

            patternFound = true;

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
            RolapUtil.threadHooks.set(new TriggerHook(trigger));

            Bomb bomb;
            try {
                if (bypassSchemaCache) {
                    connection = testContext.getFoodMartConnection(false);
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
            } finally {
                RolapUtil.threadHooks.set(null);
            }
            if (negative) {
                if (bomb != null) {
                    fail("forbidden query [" + sql + "] detected");
                }
            } else {
                if (bomb == null) {
                    fail("expected query [" + sql + "] did not occur");
                }
                assertEquals(replaceQuotes(sql), replaceQuotes(bomb.sql));
            }
        }

        /*
         * Print warning message that no pattern was specified for the current dialect.
         */
        if (!patternFound) {
            String warnDialect =
                MondrianProperties.instance().WarnIfNoPatternForDialect.get();

            if (warnDialect.equals(d.toString())) {
                System.out.println(
                    "[No expected sqls found for dialect \"" +
                    dialect.toString() +
                    "\" and test not run]"
                );
            }
        }
    }


    private void clearCache(RolapCube cube) {
        // Clear the cache for the Sales cube, so the query runs as if
        // for the first time. (TODO: Cleaner way to do this.)
        final Cube salesCube =
            getConnection().getSchema().lookupCube("Sales", true);
        RolapHierarchy hierarchy =
            (RolapHierarchy) salesCube.lookupHierarchy(
                new Id.Segment("Store", Id.Quoting.UNQUOTED),
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

    CellRequest createRequest(
        final String cube, final String measure,
        final String table, final String column, final String value)
    {
        return createRequest(
            cube, measure,
            new String[]{table}, new String[]{column}, new String[]{value});
    }

    CellRequest createRequest(
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

    CellRequest createRequest(
        final String cube, final String measure,
        final String table, final String column, final String value,
        CellRequestConstraint aggConstraint)
    {
        return createRequest(
            cube, measure,
            new String[]{table}, new String[]{column}, new String[]{value},
            aggConstraint);
    }

    CellRequest createRequest(
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
        List<String[]> values) {
        String[] aggConstraintTables =
            new String[] { "time_by_day", "time_by_day", "time_by_day" };
        String[] aggConstraintColumns =
            new String[] { "the_year", "quarter", "month_of_year" };
        List<String[]> aggConstraintValues = new ArrayList<String[]>();

        for (String[] value : values) {
            assert (value.length == 3);
            aggConstraintValues.add(value);
        }

        CellRequestConstraint aggConstraint =
            new CellRequestConstraint(
                aggConstraintTables, aggConstraintColumns, aggConstraintValues);

        return aggConstraint;
    }

    static CellRequestConstraint makeConstraintCountryState(
        List<String[]> values) {
        String[] aggConstraintTables =
            new String[] { "store", "store"};
        String[] aggConstraintColumns =
            new String[] { "store_country", "store_state"};
        List<String[]> aggConstraintValues = new ArrayList<String[]>();

        for (String[] value : values) {
            assert (value.length == 2);
            aggConstraintValues.add(value);
        }

        CellRequestConstraint aggConstraint =
            new CellRequestConstraint(
                aggConstraintTables, aggConstraintColumns, aggConstraintValues);

        return aggConstraint;
    }

    static CellRequestConstraint makeConstraintProductFamilyDepartment(
        List<String[]> values) {
        String[] aggConstraintTables =
            new String[] { "product_class", "product_class"};
        String[] aggConstraintColumns =
            new String[] { "product_family", "product_department"};
        List<String[]> aggConstraintValues = new ArrayList<String[]>();

        for (String[] value : values) {
            assert (value.length == 2);
            aggConstraintValues.add(value);
        }

        CellRequestConstraint aggConstraint =
            new CellRequestConstraint(
                aggConstraintTables, aggConstraintColumns, aggConstraintValues);

        return aggConstraint;
    }

    protected RolapStar.Measure getMeasure(String cube, String measureName) {
        final Connection connection = getFoodMartConnection();
        final boolean fail = true;
        Cube salesCube = connection.getSchema().lookupCube(cube, fail);
        Member measure = salesCube.getSchemaReader(null).getMemberByUniqueName(
            Util.parseIdentifier(measureName), fail);
        RolapStar.Measure starMeasure = RolapStar.getStarMeasure(measure);
        return starMeasure;
    }

    protected Connection getFoodMartConnection() {
        return TestContext.instance().getFoodMartConnection();
    }

    protected RolapCube getCube(final String cube) {
        final Connection connection = getFoodMartConnection();
        final boolean fail = true;
        return (RolapCube) connection.getSchema().lookupCube(cube, fail);
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
            this.trigger = trigger;
        }

        private boolean matchTrigger(String sql) {
            if (trigger == null) {
                return true;
            }
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
            List<String[]> valueList) {
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
                List<StarPredicate> andPredList = new ArrayList<StarPredicate>();
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
}

// End BatchTestCase.java
