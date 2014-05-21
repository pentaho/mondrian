/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 Julian Hyde
// Copyright (C) 2005-2014 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap.sql;

import mondrian.olap.MondrianProperties;
import mondrian.rolap.BatchTestCase;
import mondrian.spi.Dialect;
import mondrian.spi.impl.JdbcDialectImpl;
import mondrian.test.SqlPattern;
import mondrian.test.TestContext;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static mondrian.spi.Dialect.DatabaseProduct.MYSQL;
import static mondrian.spi.Dialect.DatabaseProduct.POSTGRESQL;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Test for <code>SqlQuery</code>.
 *
 * @author Thiyagu
 * @since 06-Jun-2007
 */
public class SqlQueryTest extends BatchTestCase {
    private final MondrianProperties prop = propSaver.props;

    protected void setUp() throws Exception {
        super.setUp();

        // This test warns of missing sql patterns for MYSQL.
        final Dialect dialect = getTestContext().getDialect();
        if (prop.WarnIfNoPatternForDialect.get().equals("ANY")
            || dialect.getDatabaseProduct() == MYSQL)
        {
            propSaver.set(
                propSaver.props.WarnIfNoPatternForDialect,
                dialect.getDatabaseProduct().toString());
        } else {
            // Do not warn unless the dialect is "MYSQL", or
            // if the test chooses to warn regardless of the dialect.
            propSaver.set(propSaver.props.WarnIfNoPatternForDialect, "NONE");
        }
    }

    public void testToStringForSingleGroupingSetSql() {
        if (!isGroupingSetsSupported()) {
            return;
        }
        for (boolean b : new boolean[]{false, true}) {
            Dialect dialect = getTestContext().getDialect();
            SqlQuery sqlQuery = new SqlQuery(dialect, b);
            sqlQuery.addSelect("c1", null);
            sqlQuery.addSelect("c2", null);
            sqlQuery.addGroupingFunction("gf0");
            sqlQuery.addFromTable(
                "s", "t1", "t1alias", null, null, null, null, true);
            sqlQuery.addWhere("a=b");
            ArrayList<String> groupingsetsList = new ArrayList<String>();
            groupingsetsList.add("gs1");
            groupingsetsList.add("gs2");
            groupingsetsList.add("gs3");
            sqlQuery.addGroupingSet(groupingsetsList);
            String expected;
            String lineSep = System.getProperty("line.separator");
            if (!b) {
                expected =
                    "select c1 as \"c0\", c2 as \"c1\", grouping(gf0) as \"g0\" "
                    + "from \"s\".\"t1\" =as= \"t1alias\" where a=b "
                    + "group by grouping sets ((gs1, gs2, gs3))";
            } else {
                expected =
                    "select" + lineSep
                    + "    c1 as \"c0\"," + lineSep
                    + "    c2 as \"c1\"," + lineSep
                    + "    grouping(gf0) as \"g0\"" + lineSep
                    + "from" + lineSep
                    + "    \"s\".\"t1\" =as= \"t1alias\"" + lineSep
                    + "where" + lineSep
                    + "    a=b" + lineSep
                    + "group by grouping sets (" + lineSep
                    + "    (gs1, gs2, gs3))";
            }
            assertEquals(
                dialectize(dialect.getDatabaseProduct(), expected),
                dialectize(
                    sqlQuery.getDialect().getDatabaseProduct(),
                    sqlQuery.toString()));
        }
    }

    public void testOrderBy() throws SQLException {
        // Test with requireAlias = true
        assertEquals(
            "\norder by\n"
            + "    CASE WHEN alias IS NULL THEN 1 ELSE 0 END, alias ASC",
            makeTestSqlQuery("expr", "alias", true, true, true, true)
                .toString());
        // requireAlias = false
        assertEquals(
            "\norder by\n"
            + "    CASE WHEN expr IS NULL THEN 1 ELSE 0 END, expr ASC",
            makeTestSqlQuery("expr", "alias", true, true, true, false)
                .toString());
        //  nullable = false
        assertEquals(
            "\norder by\n"
            + "    expr ASC",
            makeTestSqlQuery("expr", "alias", true, false, true, false)
                .toString());
        //  ascending=false, collateNullsLast=false
        assertEquals(
            "\norder by\n"
            + "    CASE WHEN alias IS NULL THEN 0 ELSE 1 END, alias DESC",
            makeTestSqlQuery("expr", "alias", false, true, false, true)
                .toString());
    }

    /**
     * Builds a SqlQuery with flags set according to params.
     * Uses a Mockito spy to construct a dialect which will give the desired
     * boolean value for reqOrderByAlias.
     */
    private SqlQuery makeTestSqlQuery(
        String expr, String alias, boolean ascending,
        boolean nullable, boolean collateNullsLast, boolean reqOrderByAlias)
    {
        JdbcDialectImpl dialect = spy(new JdbcDialectImpl());
        when(dialect.requiresOrderByAlias()).thenReturn(reqOrderByAlias);
        SqlQuery query = new SqlQuery(dialect, true);
        query.addOrderBy(
            expr, alias, ascending, true, nullable, collateNullsLast);
        return query;
    }

    public void testToStringForForcedIndexHint() {
        Map<String, String> hints = new HashMap<String, String>();
        hints.put("force_index", "myIndex");

        String unformattedMysql =
            "select c1 as `c0`, c2 as `c1` "
            + "from `s`.`t1` as `t1alias`"
            + " FORCE INDEX (myIndex)"
            + " where a=b";
        String formattedMysql =
            "select\n"
            + "    c1 as `c0`,\n"
            + "    c2 as `c1`\n"
            + "from\n"
            + "    `s`.`t1` as `t1alias` FORCE INDEX (myIndex)\n"
            + "where\n"
            + "    a=b";

        SqlPattern[] unformattedSqlPatterns = {
            new SqlPattern(
                MYSQL,
                unformattedMysql,
                null)};
        SqlPattern[] formattedSqlPatterns = {
            new SqlPattern(
                MYSQL,
                formattedMysql,
                null)};

        for (boolean formatted : new boolean[]{false, true}) {
            Dialect dialect = getTestContext().getDialect();
            SqlQuery sqlQuery = new SqlQuery(dialect, formatted);
            sqlQuery.setAllowHints(true);
            sqlQuery.addSelect("c1", null);
            sqlQuery.addSelect("c2", null);
            sqlQuery.addGroupingFunction("gf0");
            sqlQuery.addFromTable(
                "s", "t1", "t1alias", null, hints, null, null, true);
            sqlQuery.addWhere("a=b");
            SqlPattern[] expected;
            if (!formatted) {
                expected = unformattedSqlPatterns;
            } else {
                expected = formattedSqlPatterns;
            }
            assertSqlQueryToStringMatches(sqlQuery, expected);
        }
    }

    private void assertSqlQueryToStringMatches(
        SqlQuery query,
        SqlPattern[] patterns)
    {
        Dialect dialect = getTestContext().getDialect();
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

            String trigger = sqlPattern.getTriggerSql();

            trigger = dialectize(d, trigger);

            assertEquals(
                dialectize(dialect.getDatabaseProduct(), trigger),
                dialectize(
                    query.getDialect().getDatabaseProduct(),
                    query.toString()));
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


    public void testPredicatesAreOptimizedWhenPropertyIsTrue() {
        if (prop.ReadAggregates.get() && prop.UseAggregates.get()) {
            // Sql pattner will be different if using aggregate tables.
            // This test cover predicate generation so it's sufficient to
            // only check sql pattern when aggregate tables are not used.
            return;
        }

        String mdx =
            "select {[Time].[1997].[Q1],[Time].[1997].[Q2],"
            + "[Time].[1997].[Q3]} on 0 from sales";

        String mysqlSql =
            "select\n"
            + "    `time_by_day`.`the_year` as `c0`,\n"
            + "    `time_by_day`.`quarter` as `c1`,\n"
            + "    sum(`sales_fact_1997`.`unit_sales`) as `m0`\n"
            + "from\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`\n"
            + "where\n"
            + "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "group by\n"
            + "    `time_by_day`.`the_year`,\n"
            + "    `time_by_day`.`quarter`";

        SqlPattern[] sqlPatterns = {
            new SqlPattern(MYSQL, mysqlSql, mysqlSql)};

        assertSqlEqualsOptimzePredicates(true, mdx, sqlPatterns);
    }

    public void testTableNameIsIncludedWithParentChildQuery() {
        String sql =
            "select\n"
            + "    `employee`.`employee_id` as `c0`,\n"
            + "    `employee`.`full_name` as `c1`\n"
            + "from\n"
            + "    `employee` as `employee`\n"
            + "where\n"
            + "    `employee`.`supervisor_id` = 0\n"
            + "group by\n"
            + "    `employee`.`employee_id`,\n"
            + "    `employee`.`full_name`\n"
            + "order by\n"
            + "    `employee`.`employee_id` ASC";

        final String mdx =
            "SELECT "
            + "  GENERATE("
            + "    {[Employees].[All Employees].[Sheri Nowmer]},"
            + "{"
            + "  {([Employees].CURRENTMEMBER)},"
            + "  HEAD("
            + "    ADDCALCULATEDMEMBERS([Employees].CURRENTMEMBER.CHILDREN), 51)"
            + "},"
            + "ALL"
            + ") DIMENSION PROPERTIES PARENT_LEVEL, CHILDREN_CARDINALITY, PARENT_UNIQUE_NAME ON AXIS(0) \n"
            + "FROM [HR]  CELL PROPERTIES VALUE, FORMAT_STRING";
        assertQuerySql(getTestContext().withFreshConnection(), mdx, sql);
    }

    public void testPredicatesAreNotOptimizedWhenPropertyIsFalse() {
        if (prop.ReadAggregates.get() && prop.UseAggregates.get()) {
            // Sql pattern will be different if using aggregate tables.
            // This test cover predicate generation so it's sufficient to
            // only check sql pattern when aggregate tables are not used.
            return;
        }

        String mdx =
            "select {[Time].[1997].[Q1],[Time].[1997].[Q2],"
            + "[Time].[1997].[Q3]} on 0 from sales";

        String mysqlSql =
            "select\n"
            + "    `time_by_day`.`the_year` as `c0`,\n"
            + "    `time_by_day`.`quarter` as `c1`,\n"
            + "    sum(`sales_fact_1997`.`unit_sales`) as `m0`\n"
            + "from\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`\n"
            + "where\n"
            + "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    `time_by_day`.`quarter` in ('Q1', 'Q2', 'Q3')\n"
            + "and\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "group by\n"
            + "    `time_by_day`.`the_year`,\n"
            + "    `time_by_day`.`quarter`";

        SqlPattern[] sqlPatterns = {
            new SqlPattern(MYSQL, mysqlSql, mysqlSql)};

        assertSqlEqualsOptimzePredicates(false, mdx, sqlPatterns);
    }

    public void testPredicatesAreOptimizedWhenAllTheMembersAreIncluded() {
        if (prop.ReadAggregates.get() && prop.UseAggregates.get()) {
            // Sql pattner will be different if using aggregate tables.
            // This test cover predicate generation so it's sufficient to
            // only check sql pattern when aggregate tables are not used.
            return;
        }

        String mdx =
            "select {[Time].[1997].[Q1],[Time].[1997].[Q2],"
            + "[Time].[1997].[Q3],[Time].[1997].[Q4]} on 0 from sales";

        String accessSql =
            "select `time_by_day`.`the_year` as `c0`, "
            + "`time_by_day`.`quarter` as `c1`, "
            + "sum(`sales_fact_1997`.`unit_sales`) as `m0` from "
            + "`time_by_day` as `time_by_day`, `sales_fact_1997` as"
            + " `sales_fact_1997` where `sales_fact_1997`.`time_id`"
            + " = `time_by_day`.`time_id` and `time_by_day`."
            + "`the_year` = 1997 group by `time_by_day`.`the_year`,"
            + " `time_by_day`.`quarter`";

        String mysqlSql =
            "select\n"
            + "    `time_by_day`.`the_year` as `c0`,\n"
            + "    `time_by_day`.`quarter` as `c1`,\n"
            + "    sum(`sales_fact_1997`.`unit_sales`) as `m0`\n"
            + "from\n"

            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
                + "    `time_by_day` as `time_by_day`\n"
            + "where\n"
                + "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"

                + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
                + "group by\n"
            + "    `time_by_day`.`the_year`,\n"
            + "    `time_by_day`.`quarter`";

        SqlPattern[] sqlPatterns = {
            new SqlPattern(
                Dialect.DatabaseProduct.ACCESS, accessSql, accessSql),
            new SqlPattern(MYSQL, mysqlSql, mysqlSql)};

        assertSqlEqualsOptimzePredicates(true, mdx, sqlPatterns);
        assertSqlEqualsOptimzePredicates(false, mdx, sqlPatterns);
    }

    private void assertSqlEqualsOptimzePredicates(
        boolean optimizePredicatesValue,
        String inputMdx,
        SqlPattern[] sqlPatterns)
    {
        propSaver.props.OptimizePredicates.set(optimizePredicatesValue);
        assertQuerySql(getTestContext(), inputMdx, sqlPatterns);
    }

    public void testToStringForGroupingSetSqlWithEmptyGroup() {
        if (!isGroupingSetsSupported()) {
            return;
        }
        final Dialect dialect = getTestContext().getDialect();
        for (boolean b : new boolean[]{false, true}) {
            SqlQuery sqlQuery = new SqlQuery(getTestContext().getDialect(), b);
            sqlQuery.addSelect("c1", null);
            sqlQuery.addSelect("c2", null);
            sqlQuery.addFromTable(
                "s", "t1", "t1alias", null, null, null, null, true);
            sqlQuery.addWhere("a=b");
            sqlQuery.addGroupingFunction("g1");
            sqlQuery.addGroupingFunction("g2");
            ArrayList<String> groupingsetsList = new ArrayList<String>();
            groupingsetsList.add("gs1");
            groupingsetsList.add("gs2");
            groupingsetsList.add("gs3");
            sqlQuery.addGroupingSet(new ArrayList<String>());
            sqlQuery.addGroupingSet(groupingsetsList);
            String expected;
            if (b) {
                expected =
                    "select\n"
                    + "    c1 as \"c0\",\n"
                    + "    c2 as \"c1\",\n"
                    + "    grouping(g1) as \"g0\",\n"
                    + "    grouping(g2) as \"g1\"\n"
                    + "from\n"
                    + "    \"s\".\"t1\" =as= \"t1alias\"\n"
                    + "where\n"
                    + "    a=b\n"
                    + "group by grouping sets (\n"
                    + "    (),\n"
                    + "    (gs1, gs2, gs3))";
            } else {
                expected =
                    "select c1 as \"c0\", c2 as \"c1\", grouping(g1) as \"g0\", "
                    + "grouping(g2) as \"g1\" from \"s\".\"t1\" =as= \"t1alias\" where a=b "
                    + "group by grouping sets ((), (gs1, gs2, gs3))";
            }
            assertEquals(
                dialectize(dialect.getDatabaseProduct(), expected),
                dialectize(
                    sqlQuery.getDialect().getDatabaseProduct(),
                    sqlQuery.toString()));
        }
    }

    public void testToStringForMultipleGroupingSetsSql() {
        if (!isGroupingSetsSupported()) {
            return;
        }
        final Dialect dialect = getTestContext().getDialect();
        for (boolean b : new boolean[]{false, true}) {
            SqlQuery sqlQuery = new SqlQuery(dialect, b);
            sqlQuery.addSelect("c0", null);
            sqlQuery.addSelect("c1", null);
            sqlQuery.addSelect("c2", null);
            sqlQuery.addSelect("m1", null, "m1");
            sqlQuery.addFromTable(
                "s", "t1", "t1alias", null, null, null, null, true);
            sqlQuery.addWhere("a=b");
            sqlQuery.addGroupingFunction("c0");
            sqlQuery.addGroupingFunction("c1");
            sqlQuery.addGroupingFunction("c2");
            ArrayList<String> groupingSetlist1 = new ArrayList<String>();
            groupingSetlist1.add("c0");
            groupingSetlist1.add("c1");
            groupingSetlist1.add("c2");
            sqlQuery.addGroupingSet(groupingSetlist1);
            ArrayList<String> groupingsetsList2 = new ArrayList<String>();
            groupingsetsList2.add("c1");
            groupingsetsList2.add("c2");
            sqlQuery.addGroupingSet(groupingsetsList2);
            String expected;
            if (b) {
                expected =
                    "select\n"
                    + "    c0 as \"c0\",\n"
                    + "    c1 as \"c1\",\n"
                    + "    c2 as \"c2\",\n"
                    + "    m1 as \"m1\",\n"
                    + "    grouping(c0) as \"g0\",\n"
                    + "    grouping(c1) as \"g1\",\n"
                    + "    grouping(c2) as \"g2\"\n"
                    + "from\n"
                    + "    \"s\".\"t1\" =as= \"t1alias\"\n"
                    + "where\n"
                    + "    a=b\n"
                    + "group by grouping sets (\n"
                    + "    (c0, c1, c2),\n"
                    + "    (c1, c2))";
            } else {
                expected =
                    "select c0 as \"c0\", c1 as \"c1\", c2 as \"c2\", m1 as \"m1\", "
                    + "grouping(c0) as \"g0\", grouping(c1) as \"g1\", grouping(c2) as \"g2\" "
                    + "from \"s\".\"t1\" =as= \"t1alias\" where a=b "
                    + "group by grouping sets ((c0, c1, c2), (c1, c2))";
            }
            assertEquals(
                dialectize(dialect.getDatabaseProduct(), expected),
                dialectize(
                    sqlQuery.getDialect().getDatabaseProduct(),
                    sqlQuery.toString()));
        }
    }

    /**
     * Verifies that the correct SQL string is generated for literals of
     * SQL type "double".
     *
     * <p>Mondrian only generates SQL DOUBLE values in a special format for
     * LucidDB; therefore, this test is a no-op on other databases.
     */
    public void testDoubleInList() {
        final Dialect dialect = getTestContext().getDialect();
        if (dialect.getDatabaseProduct() != Dialect.DatabaseProduct.LUCIDDB) {
            return;
        }

        propSaver.set(propSaver.props.IgnoreInvalidMembers, true);
        propSaver.set(propSaver.props.IgnoreInvalidMembersDuringQuery, true);

        // assertQuerySql(testContext, query, patterns);

        // Test when the double value itself contains "E".
        String dimensionSqlExpression =
            "cast(cast(\"salary\" as double)*cast(1000.0 as double)/cast(3.1234567890123456 as double) as double)\n";

        String cubeFirstPart =
            "<Cube name=\"Sales 3\">\n"
            + "  <Table name=\"sales_fact_1997\"/>\n"
            + "  <Dimension name=\"StoreEmpSalary\" foreignKey=\"store_id\">\n"
            + "    <Hierarchy hasAll=\"true\" allMemberName=\"All Salary\" primaryKey=\"store_id\">\n"
            + "      <Table name=\"employee\"/>\n"
            + "      <Level name=\"Salary\" column=\"salary\" type=\"Numeric\" uniqueMembers=\"true\" approxRowCount=\"10000000\">\n"
            + "        <KeyExpression>\n"
            + "          <SQL dialect=\"luciddb\">\n";

        String cubeSecondPart =
            "          </SQL>\n"
            + "        </KeyExpression>\n"
            + "      </Level>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>"
            + "  <Measure name=\"Store Cost\" column=\"store_cost\" aggregator=\"sum\"/>\n"
            + "</Cube>";

        String cube =
            cubeFirstPart
            + dimensionSqlExpression
            + cubeSecondPart;

        String query =
            "select "
            + "{[StoreEmpSalary].[All Salary].[6403.162057613773],[StoreEmpSalary].[All Salary].[1184584.980658548],[StoreEmpSalary].[All Salary].[1344664.0320988924], "
            + " [StoreEmpSalary].[All Salary].[1376679.8423869612],[StoreEmpSalary].[All Salary].[1408695.65267503],[StoreEmpSalary].[All Salary].[1440711.462963099], "
            + " [StoreEmpSalary].[All Salary].[1456719.3681071333],[StoreEmpSalary].[All Salary].[1472727.2732511677],[StoreEmpSalary].[All Salary].[1488735.1783952022], "
            + " [StoreEmpSalary].[All Salary].[1504743.0835392366],[StoreEmpSalary].[All Salary].[1536758.8938273056],[StoreEmpSalary].[All Salary].[1600790.5144034433], "
            + " [StoreEmpSalary].[All Salary].[1664822.134979581],[StoreEmpSalary].[All Salary].[1888932.806996063],[StoreEmpSalary].[All Salary].[1952964.4275722008], "
            + " [StoreEmpSalary].[All Salary].[1984980.2378602696],[StoreEmpSalary].[All Salary].[2049011.8584364073],[StoreEmpSalary].[All Salary].[2081027.6687244761], "
            + " [StoreEmpSalary].[All Salary].[2113043.479012545],[StoreEmpSalary].[All Salary].[2145059.289300614],[StoreEmpSalary].[All Salary].[2.5612648230455093E7]} "
            + " on rows, {[Measures].[Store Cost]} on columns from [Sales 3]";

        // Notice there are a few members missing in this sql. This is a LucidDB
        // bug wrt comparison involving "approximate number literals".
        // Mondrian properties "IgnoreInvalidMembers" and
        // "IgnoreInvalidMembersDuringQuery" are required for this MDX to
        // finish, even though the the generated sql(below) and the final result
        // are both incorrect.
        String loadSqlLucidDB =
            "select cast(cast(\"salary\" as double)*cast(1000.0 as double)/cast(3.1234567890123456 as double) as double) as \"c0\", "
            + "sum(\"sales_fact_1997\".\"store_cost\") as \"m0\" "
            + "from \"employee\" as \"employee\", \"sales_fact_1997\" as \"sales_fact_1997\" "
            + "where \"sales_fact_1997\".\"store_id\" = \"employee\".\"store_id\" and "
            + "cast(cast(\"salary\" as double)*cast(1000.0 as double)/cast(3.1234567890123456 as double) as double) in "
            + "(6403.162057613773E0, 1184584.980658548E0, 1344664.0320988924E0, "
            + "1376679.8423869612E0, 1408695.65267503E0, 1440711.462963099E0, "
            + "1456719.3681071333E0, 1488735.1783952022E0, "
            + "1504743.0835392366E0, 1536758.8938273056E0, "
            + "1664822.134979581E0, 1888932.806996063E0, 1952964.4275722008E0, "
            + "1984980.2378602696E0, 2049011.8584364073E0, "
            + "2113043.479012545E0, 2145059.289300614E0, 2.5612648230455093E7) "
            + "group by cast(cast(\"salary\" as double)*cast(1000.0 as double)/cast(3.1234567890123456 as double) as double)";

        SqlPattern[] patterns = {
            new SqlPattern(
                Dialect.DatabaseProduct.LUCIDDB,
                loadSqlLucidDB,
                loadSqlLucidDB)
        };

        TestContext testContext =
            TestContext.instance().create(
                null,
                cube,
                null,
                null,
                null,
                null);

        assertQuerySql(testContext, query, patterns);
    }

    /**
     * Testcase for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-457">bug MONDRIAN-457,
     * "Strange SQL condition appears when executing query"</a>. The fix
     * implemented MatchType.EXACT_SCHEMA, which only
     * queries known schema objects. This prevents SQL such as
     * "UPPER(`store`.`store_country`) = UPPER('Time.Weekly')" from being
     * generated.
     */
    public void testInvalidSqlMemberLookup() {
        String sqlMySql =
            "select `store`.`store_type` as `c0` from `store` as `store` "
            + "where UPPER(`store`.`store_type`) = UPPER('Time.Weekly') "
            + "group by `store`.`store_type` "
            + "order by ISNULL(`store`.`store_type`), `store`.`store_type` ASC";
        String sqlOracle =
            "select \"store\".\"store_type\" as \"c0\" from \"store\" \"store\" "
            + "where UPPER(\"store\".\"store_type\") = UPPER('Time.Weekly') "
            + "group by \"store\".\"store_type\" "
            + "order by \"store\".\"store_type\" ASC";

        SqlPattern[] patterns = {
            new SqlPattern(MYSQL, sqlMySql, sqlMySql),
            new SqlPattern(
                Dialect.DatabaseProduct.ORACLE, sqlOracle, sqlOracle),
        };

        assertNoQuerySql(
            getTestContext(),
            "select {[Time.Weekly].[All Time.Weeklys]} ON COLUMNS from [Sales]",
            patterns);
    }

    /**
     * This test makes sure that a level which specifies an
     * approxRowCount property prevents Mondrian from executing a
     * count() sql query. It was discovered in bug MONDRIAN-711
     * that the aggregate tables predicates optimization code was
     * not considering the approxRowCount property. It is fixed and
     * this test will ensure it won't happen again.
     */
    public void testApproxRowCountOverridesCount() {
        final String cubeSchema =
            "<Cube name=\"ApproxTest\"> \n"
            + "  <Table name=\"sales_fact_1997\"/> \n"
            + "  <Dimension name=\"Gender\" foreignKey=\"customer_id\">\n"
            + "    <Hierarchy hasAll=\"true\" allMemberName=\"All Gender\" primaryKey=\"customer_id\">\n"
            + "      <Table name=\"customer\"/>\n"
            + "      <Level name=\"Gender\" column=\"gender\" uniqueMembers=\"true\" approxRowCount=\"2\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>"
            + "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\"/> \n"
            + "</Cube>";

        final String mdxQuery =
            "SELECT {[Gender].[Gender].Members} ON ROWS, {[Measures].[Unit Sales]} ON COLUMNS FROM [ApproxTest]";

        final String forbiddenSqlOracle =
            "select count(distinct \"customer\".\"gender\") as \"c0\" from \"customer\" \"customer\"";

        final String forbiddenSqlMysql =
            "select count(distinct `customer`.`gender`) as `c0` from `customer` `customer`;";

        SqlPattern[] patterns = {
            new SqlPattern(
                Dialect.DatabaseProduct.ORACLE, forbiddenSqlOracle, null),
            new SqlPattern(
                MYSQL, forbiddenSqlMysql, null)
        };

        final TestContext testContext =
            TestContext.instance().legacy().create(
                null,
                cubeSchema,
                null,
                null,
                null,
                null);

        assertQuerySqlOrNot(
            testContext,
            mdxQuery,
            patterns,
            true,
            true,
            true);
    }

    public void testLimitedRollupMemberRetrievableFromCache() throws Exception {
        final String mdx =
            "select NON EMPTY { [Store].[Stores].[Store State].members } on 0 from [Sales]";
        final TestContext context =
            TestContext.instance().create(
                null, null, null, null, null,
                " <Role name='justCA'>\n"
                + " <SchemaGrant access='all'>\n"
                + " <CubeGrant cube='Sales' access='all'>\n"
                + " <HierarchyGrant hierarchy='[Stores]' access='custom' rollupPolicy='partial'>\n"
                + " <MemberGrant member='[Store].[USA].[CA]' access='all'/>\n"
                + " </HierarchyGrant>\n"
                + " </CubeGrant>\n"
                + " </SchemaGrant>\n"
                + " </Role>\n").withRole("justCA");

        String pgSql =
            "select \"store\".\"store_country\" as \"c0\","
            + " \"store\".\"store_state\" as \"c1\""
            + " from \"sales_fact_1997\" as \"sales_fact_1997\","
            + " \"store\" as \"store\" "
            + "where (\"store\".\"store_country\" = 'USA') "
            + "and (\"store\".\"store_state\" = 'CA') "
            + "and \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" "
            + "group by \"store\".\"store_country\", \"store\".\"store_state\" "
            + "order by \"store\".\"store_country\" ASC NULLS LAST,"
            + " \"store\".\"store_state\" ASC NULLS LAST";
        SqlPattern pgPattern =
            new SqlPattern(POSTGRESQL, pgSql, pgSql.length());
        String mySql =
            "select `store`.`store_country` as `c0`,"
            + " `store`.`store_state` as `c1`"
            + " from `store` as `store`, `sales_fact_1997` as `sales_fact_1997` "
            + "where `sales_fact_1997`.`store_id` = `store`.`store_id` "
            + "and `store`.`store_country` = 'USA' "
            + "and `store`.`store_state` = 'CA' "
            + "group by `store`.`store_country`, `store`.`store_state` "
            + "order by ISNULL(`store`.`store_country`) ASC,"
            + " `store`.`store_country` ASC,"
            + " ISNULL(`store`.`store_state`) ASC, `store`.`store_state` ASC";
        SqlPattern myPattern = new SqlPattern(MYSQL, mySql, mySql.length());
        SqlPattern[] patterns = {pgPattern, myPattern};
        context.executeQuery(mdx);
        assertQuerySqlOrNot(context, mdx, patterns, true, false, false);
    }

    /**
     * This is a test for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1869">MONDRIAN-1869</a>
     *
     * <p>Avg Aggregates need to be computed in SQL to get correct values.
     */
    public void testAvgAggregator() {
        propSaver.set(propSaver.props.GenerateFormattedSql, true);
        TestContext context = getTestContext().createSubstitutingCube(
            "Sales",
            null,
            " <Measure name=\"Avg Sales\" column=\"unit_sales\" aggregator=\"avg\"\n"
            + " formatString=\"#.###\"/>",
            null,
            null);
        String mdx = "select measures.[avg sales] on 0 from sales"
                       + " where { time.[1997].q1, time.[1997].q2.[4] }";
        context.assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{[Time].[Time].[1997].[Q1]}\n"
            + "{[Time].[Time].[1997].[Q2].[4]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Avg Sales]}\n"
            + "Row #0: 3.069\n");
        String sql =
            "select\n"
            + "    avg(`sales_fact_1997`.`unit_sales`) as `m0`\n"
            + "from\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`\n"
            + "where\n"
            + "    ((`time_by_day`.`the_year` = 1997 and `time_by_day`.`quarter` = 'Q1') "
            + "or (`time_by_day`.`the_year` = 1997 and `time_by_day`.`month_of_year` = 4))\n"
            + "and\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`";
        SqlPattern mySqlPattern =
            new SqlPattern(Dialect.DatabaseProduct.MYSQL, sql, sql.length());
        assertQuerySql(context, mdx, new SqlPattern[]{mySqlPattern});
    }
}

// End SqlQueryTest.java
