/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde
// Copyright (C) 2005-2014 Pentaho and others
// All Rights Reserved.
//
// jhyde, 28 September, 2002
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.rolap.agg.*;
import mondrian.server.*;
import mondrian.spi.Dialect;
import mondrian.test.*;

import org.olap4j.impl.Olap4jUtil;

import java.util.*;

/**
 * Unit test for {@link AggregationManager}.
 *
 * @author jhyde
 * @since 21 March, 2002
 */
public class TestAggregationManager extends BatchTestCase {
    private static final Set<Dialect.DatabaseProduct> ACCESS_MYSQL =
        Olap4jUtil.enumSetOf(
            Dialect.DatabaseProduct.ACCESS,
            Dialect.DatabaseProduct.MYSQL);

    private Locus locus;
    private Execution execution;
    private AggregationManager aggMgr;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        final Statement statement =
            ((RolapConnection) getTestContext().getConnection())
                .getInternalStatement();
        execution = new Execution(statement, 0);
        aggMgr =
            execution.getMondrianStatement()
                .getMondrianConnection()
                .getServer().getAggregationManager();
        locus = new Locus(execution, "TestAggregationManager", null);
        Locus.push(locus);
    }

    @Override
    protected void tearDown() throws Exception {
        Locus.pop(locus);

        // allow gc
        locus = null;
        execution = null;
        aggMgr = null;

        super.tearDown();
    }

    public TestAggregationManager(String name) {
        super(name);
    }

    public TestAggregationManager() {
        super();
    }

    public void testFemaleUnitSales() {
        final TestContext testContext = getTestContext();
        final FastBatchingCellReader fbcr =
            new FastBatchingCellReader(
                execution, getCube(testContext, "Sales"), aggMgr);
        CellRequest request =
            createRequest(
                testContext,
                "Sales", "[Measures].[Unit Sales]",
                "customer", "gender", "F");
        Object value = aggMgr.getCellFromCache(request);
        assertNull(value); // before load, the cell is not found
        fbcr.recordCellRequest(request);
        fbcr.loadAggregations();
        value = aggMgr.getCellFromCache(request); // after load, cell found
        assertTrue(value instanceof Number);
        assertEquals(131558, ((Number) value).intValue());
    }

    public void testFemaleCustomerCount() {
        final TestContext testContext = getTestContext();
        final FastBatchingCellReader fbcr =
            new FastBatchingCellReader(
                execution, getCube(testContext, "Sales"), aggMgr);
        CellRequest request =
            createRequest(
                testContext,
                "Sales", "[Measures].[Customer Count]",
                "customer", "gender", "F");
        Object value = aggMgr.getCellFromCache(request);
        assertNull(value); // before load, the cell is not found
        fbcr.recordCellRequest(request);
        fbcr.loadAggregations();
        value = aggMgr.getCellFromCache(request); // after load, cell found
        assertTrue(value instanceof Number);
        assertEquals(2755, ((Number) value).intValue());
    }

    public void testFemaleCustomerCountWithConstraints() {
        final TestContext testContext = getTestContext();

        List<List<String>> Q1M1 = list(list("1997", "Q1", "1"));

        List<List<String>> Q2M5 = list(list("1997", "Q2", "5"));

        List<List<String>> Q1M1Q2M5 =
            list(
                list("1997", "Q1", "1"),
                list("1997", "Q2", "5"));

        CellRequest request1 =
            createRequest(
                testContext,
                "Sales", "[Measures].[Customer Count]",
                "customer", "gender", "F",
                makeConstraintYearQuarterMonth(Q1M1));

        CellRequest request2 =
            createRequest(
                testContext,
                "Sales", "[Measures].[Customer Count]",
                "customer", "gender", "F",
                makeConstraintYearQuarterMonth(Q2M5));

        CellRequest request3 =
            createRequest(
                testContext,
                "Sales", "[Measures].[Customer Count]",
                "customer", "gender", "F",
                makeConstraintYearQuarterMonth(Q1M1Q2M5));

        FastBatchingCellReader fbcr =
            new FastBatchingCellReader(
                execution, getCube(testContext, "Sales"), aggMgr);

        Object value = aggMgr.getCellFromCache(request1);
        assertNull(value); // before load, the cell is not found

        fbcr.recordCellRequest(request1);
        fbcr.recordCellRequest(request2);
        fbcr.recordCellRequest(request3);
        fbcr.loadAggregations();

        value = aggMgr.getCellFromCache(request1); // after load, found
        assertTrue(value instanceof Number);
        assertEquals(694, ((Number) value).intValue());

        value = aggMgr.getCellFromCache(request2); // after load, found
        assertTrue(value instanceof Number);
        assertEquals(672, ((Number) value).intValue());

        value = aggMgr.getCellFromCache(request3); // after load, found
        assertTrue(value instanceof Number);
        assertEquals(1122, ((Number) value).intValue());
        // Note: 1122 != (694 + 672)
    }

    /**
     * Tests that a request for ([Measures].[Unit Sales], [Gender].[F])
     * generates the correct SQL.
     */
    public void testFemaleUnitSalesSql() {
        if (!(propSaver.props.UseAggregates.get()
              && propSaver.props.ReadAggregates.get()))
        {
            return;
        }
        final TestContext testContext = getTestContext();
        CellRequest request =
            createRequest(
                testContext,
                "Sales", "[Measures].[Unit Sales]", "customer", "gender", "F");

        SqlPattern[] patterns = {
            new SqlPattern(
                ACCESS_MYSQL,
                "select\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`gender` as `c0`,\n"
                + "    sum(`agg_g_ms_pcat_sales_fact_1997`.`unit_sales`) as `m0`\n"
                + "from\n"
                + "    `agg_g_ms_pcat_sales_fact_1997` as `agg_g_ms_pcat_sales_fact_1997`\n"
                + "where\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`gender` = 'F'\n"
                + "group by\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`gender`",
                26)
        };

        assertRequestSql(
            testContext,
            new CellRequest[]{request},
            patterns);
    }

    /**
     * As {@link #testFemaleUnitSalesSql()}, but with aggregate tables switched
     * on.
     *
     * TODO: Enable this test.
     */
    private void _testFemaleUnitSalesSql_withAggs() {
        final TestContext testContext = getTestContext();
        CellRequest request = createRequest(
            testContext,
            "Sales", "[Measures].[Unit Sales]", "customer", "gender", "F");

        SqlPattern[] patterns = {
            new SqlPattern(
                ACCESS_MYSQL,
                "select `customer`.`gender` as `c0`,"
                + " sum(`agg_l_03_sales_fact_1997`.`unit_sales`) as `m0` "
                + "from `customer` as `customer`,"
                + " `agg_l_03_sales_fact_1997` as `agg_l_03_sales_fact_1997` "
                + "where `agg_l_03_sales_fact_1997`.`customer_id` = `customer`.`customer_id` "
                + "and `customer`.`gender` = 'F' "
                + "group by `customer`.`gender`",
                26)
        };

        assertRequestSql(
            testContext,
            new CellRequest[]{request},
            patterns);
    }

    /**
     * Test a batch containing multiple measures:
     *   (store_state=CA, gender=F, measure=[Unit Sales])
     *   (store_state=CA, gender=M, measure=[Store Sales])
     *   (store_state=OR, gender=M, measure=[Unit Sales])
     */
    public void testMultipleMeasures() {
        if (!(propSaver.props.UseAggregates.get()
              && propSaver.props.ReadAggregates.get()))
        {
            return;
        }

        final TestContext testContext = getTestContext();
        CellRequest[] requests = {
            createRequest(
                testContext,
                "Sales",
                "[Measures].[Unit Sales]",
                list("customer", "store"),
                list("gender", "store_state"),
                list("F", "CA")),
            createRequest(
                testContext,
                "Sales",
                "[Measures].[Store Sales]",
                list("customer", "store"),
                list("gender", "store_state"),
                list("M", "CA")),
            createRequest(
                testContext,
                "Sales",
                "[Measures].[Unit Sales]",
                list("customer", "store"),
                list("gender", "store_state"),
                list("F", "OR"))};

        SqlPattern[] patterns = {
            new SqlPattern(
                ACCESS_MYSQL,
                "select\n"
                + "    `store`.`store_state` as `c0`,\n"
                + "    `customer`.`gender` as `c1`,\n"
                + "    sum(`agg_l_05_sales_fact_1997`.`unit_sales`) as `m0`,\n"
                + "    sum(`agg_l_05_sales_fact_1997`.`store_sales`) as `m1`\n"
                + "from\n"
                + "    `store` as `store`,\n"
                + "    `agg_l_05_sales_fact_1997` as `agg_l_05_sales_fact_1997`,\n"
                + "    `customer` as `customer`\n"
                + "where\n"
                + "    `agg_l_05_sales_fact_1997`.`store_id` = `store`.`store_id`\n"
                + "and\n"
                + "    `store`.`store_state` in ('CA', 'OR')\n"
                + "and\n"
                + "    `agg_l_05_sales_fact_1997`.`customer_id` = `customer`.`customer_id`\n"
                + "group by\n"
                + "    `store`.`store_state`,\n"
                + "    `customer`.`gender`",
                29)
        };

        assertRequestSql(testContext, requests, patterns);
    }

    /**
     * As {@link #testMultipleMeasures()}, but with aggregate tables switched
     * on.
     *
     * TODO: Enable this test.
     */
    private void _testMultipleMeasures_withAgg() {
        final TestContext testContext = getTestContext();
        CellRequest[] requests = {
            createRequest(
                testContext,
                "Sales",
                "[Measures].[Unit Sales]",
                list("customer", "store"),
                list("gender", "store_state"),
                list("F", "CA")),
            createRequest(
                testContext,
                "Sales",
                "[Measures].[Store Sales]",
                list("customer", "store"),
                list("gender", "store_state"),
                list("M", "CA")),
            createRequest(
                testContext,
                "Sales",
                "[Measures].[Unit Sales]",
                list("customer", "store"),
                list("gender", "store_state"),
                list("F", "OR"))};

        SqlPattern[] patterns = {
            new SqlPattern(
                ACCESS_MYSQL,
                "select `customer`.`gender` as `c0`,"
                + " `store`.`store_state` as `c1`,"
                + " sum(`agg_l_05_sales_fact_1997`.`unit_sales`) as `m0`,"
                + " sum(`agg_l_05_sales_fact_1997`.`store_sales`) as `m1` "
                + "from `customer` as `customer`,"
                + " `agg_l_05_sales_fact_1997` as `agg_l_05_sales_fact_1997`,"
                + " `store` as `store` "
                + "where `agg_l_05_sales_fact_1997`.`customer_id` = `customer`.`customer_id`"
                + " and `agg_l_05_sales_fact_1997`.`store_id` = `store`.`store_id`"
                + " and `store`.`store_state` in ('CA', 'OR') "
                + "group by `customer`.`gender`, `store`.`store_state`",
                26)
        };

        assertRequestSql(testContext, requests, patterns);
    }

    /**
     */
    private CellRequest createMultipleMeasureCellRequest() {
        String cube = "Sales";
        String measure = "[Measures].[Unit Sales]";
        String table = "store";
        String column = "store_state";
        String value = "CA";
        final Connection connection =
                TestContext.instance().getConnection();
        final boolean fail = true;
        Cube salesCube = connection.getSchema().lookupCube(cube, fail);
        Member storeSqftMeasure =
            salesCube.getSchemaReader(null).getMemberByUniqueName(
                Util.parseIdentifier(measure), fail);
        RolapStar.Measure starMeasure =
            RolapStar.getStarMeasure(storeSqftMeasure);
        CellRequest request = new CellRequest(starMeasure, false, false);
        final RolapStar star = starMeasure.getStar();
        final RolapStar.Column storeTypeColumn =
            star.lookupColumn(table, column);
        request.addConstrainedColumn(
            storeTypeColumn,
            new ValueColumnPredicate(
                new PredicateColumn(
                    RolapSchema.BadRouter.INSTANCE,
                    storeTypeColumn.getExpression()),
                value));
        return request;
    }

    // todo: test unrestricted column, (Unit Sales, Gender=*)

    // todo: test one unrestricted, one restricted, (UNit Sales, Gender=*,
    //  State={CA, OR})

    // todo: test with 2 dimension columns on the same table, e.g.
    //  (Unit Sales, Gender={F}, MaritalStatus={S}) and make sure that the
    // table only appears once in the from clause.

    /**
     * Tests that if a level is marked 'unique members', then its parent
     * is not constrained.
     */
    public void testUniqueMembers() {
        // [Store].[Store State] is unique, so we don't expect to see any
        // references to country.
        final String mdxQuery =
            "select {[Measures].[Unit Sales]} on columns,"
            + " {[Store].[USA].[CA], [Store].[USA].[OR]} on rows "
            + "from [Sales]";
        String sql;

        // Note: the following aggregate loading SQL statements contain no
        // references to the parent level column "store_country".
        if (propSaver.props.UseAggregates.get()
            && propSaver.props.ReadAggregates.get())
        {
            sql =
                "select\n"
                + "    `store`.`store_state` as `c0`,\n"
                + "    `agg_c_14_sales_fact_1997`.`the_year` as `c1`,\n"
                + "    sum(`agg_c_14_sales_fact_1997`.`unit_sales`) as `m0`\n"
                + "from\n"
                + "    `agg_c_14_sales_fact_1997` as `agg_c_14_sales_fact_1997`,\n"
                + "    `store` as `store`\n"
                + "where\n"
                + "    `store`.`store_state` in ('CA', 'OR')\n"
                + "and\n"
                + "    `agg_c_14_sales_fact_1997`.`the_year` = 1997\n"
                + "and\n"
                + "    `agg_c_14_sales_fact_1997`.`store_id` = `store`.`store_id`\n"
                + "group by\n"
                + "    `store`.`store_state`,\n"
                + "    `agg_c_14_sales_fact_1997`.`the_year`";
        } else {
            sql =
                "select\n"
                + "    `store`.`store_state` as `c0`,\n"
                + "    `time_by_day`.`the_year` as `c1`,\n"
                + "    sum(`sales_fact_1997`.`unit_sales`) as `m0`\n"
                + "from\n"
                + "    `sales_fact_1997` as `sales_fact_1997`,\n"
                + "    `store` as `store`,\n"
                + "    `time_by_day` as `time_by_day`\n"
                + "where\n"
                + "    `store`.`store_state` in ('CA', 'OR')\n"
                + "and\n"
                + "    `time_by_day`.`the_year` = 1997\n"
                + "and\n"
                + "    `sales_fact_1997`.`store_id` = `store`.`store_id`\n"
                + "and\n"
                + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
                + "group by\n"
                + "    `store`.`store_state`,\n"
                + "    `time_by_day`.`the_year`";
        }
        assertQuerySql(getTestContext(), mdxQuery, sql);
    }

    /**
     * Tests that a NonEmptyCrossJoin uses the measure referenced by the query
     * (Store Sales) instead of the default measure (Unit Sales) in the case
     * where the query only has one result axis.  The setup here is necessarily
     * elaborate because the original bug was quite arbitrary.
     */
    public void testNonEmptyCrossJoinLoneAxis() {
        // Not sure what this test is checking.
        // For now, only run it for derby.
        final TestContext testContext = getTestContext();
        final Dialect dialect = testContext.getDialect();
        if (dialect.getDatabaseProduct() != Dialect.DatabaseProduct.DERBY) {
            return;
        }
        String mdxQuery =
            "With "
            + "Set [*NATIVE_CJ_SET] as "
            + "'NonEmptyCrossJoin([*BASE_MEMBERS_Store],[*BASE_MEMBERS_Product])' "
            + "Set [*BASE_MEMBERS_Store] as '{[Store].[All Stores].[USA]}' "
            + "Set [*GENERATED_MEMBERS_Store] as "
            + "'Generate([*NATIVE_CJ_SET], {[Store].CurrentMember})' "
            + "Set [*BASE_MEMBERS_Product] as "
            + "'{[Product].[All Products].[Food],[Product].[All Products].[Drink]}' "
            + "Set [*GENERATED_MEMBERS_Product] as "
            + "'Generate([*NATIVE_CJ_SET], {[Product].CurrentMember})' "
            + "Member [Store].[Stores].[*FILTER_MEMBER] as 'Aggregate ([*GENERATED_MEMBERS_Store])' "
            + "Member [Product].[*FILTER_MEMBER] as 'Aggregate ([*GENERATED_MEMBERS_Product])' "
            + "Select {[Measures].[Store Sales]} on columns "
            + "From [Sales] "
            + "Where ([Store].[*FILTER_MEMBER], [Product].[*FILTER_MEMBER])";

        String derbySql =
            "select "
            + "\"store\".\"store_country\" as \"c0\", "
            + "\"time_by_day\".\"the_year\" as \"c1\", "
            + "\"product_class\".\"product_family\" as \"c2\", "
            + "sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" "
            + "from "
            + "\"store\" as \"store\", "
            + "\"sales_fact_1997\" as \"sales_fact_1997\", "
            + "\"time_by_day\" as \"time_by_day\", "
            + "\"product_class\" as \"product_class\", "
            + "\"product\" as \"product\" "
            + "where "
            + "\"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" and "
            + "\"store\".\"store_country\" = 'USA' and "
            + "\"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and "
            + "\"time_by_day\".\"the_year\" = 1997 and "
            + "\"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\" and "
            + "\"product\".\"product_class_id\" = \"product_class\".\"product_class_id\" "
            + "group by "
            + "\"store\".\"store_country\", \"time_by_day\".\"the_year\", "
            + "\"product_class\".\"product_family\"";

        SqlPattern[] patterns = {
            new SqlPattern(Dialect.DatabaseProduct.DERBY, derbySql, derbySql)};

        // For derby, the TestAggregationManager.testNonEmptyCrossJoinLoneAxis
        // test fails if the non-empty crossjoin optimizer is used.
        // With it on one gets a recursive call coming through the
        //  RolapEvaluator.getCachedResult.
        assertNoQuerySql(testContext, mdxQuery, patterns);
    }

    /**
     * If a hierarchy lives in the fact table, we should not generate a join.
     */
    public void testHierarchyInFactTable() {
        final TestContext testContext = getTestContext();
        CellRequest request =
            createRequest(
                testContext,
                "Store",
                "[Measures].[Store Sqft]",
                "store",
                "store_type",
                "Supermarket");

        String accessMysqlSql =
            "select\n"
            + "    `store`.`store_type` as `c0`,\n"
            + "    sum(`store`.`store_sqft`) as `m0`\n"
            + "from\n"
            + "    `store` as `store`\n"
            + "where\n"
            + "    `store`.`store_type` = 'Supermarket'\n"
            + "group by\n"
            + "    `store`.`store_type`";

        String derbySql =
            "select\n"
            + "    \"store\".\"store_type\" as \"c0\",\n"
            + "    sum(\"store\".\"store_sqft\") as \"m0\"\n"
            + "from\n"
            + "    \"store\" as \"store\"\n"
            + "where\n"
            + "    \"store\".\"store_type\" = 'Supermarket'\n"
            + "group by\n"
            + "    \"store\".\"store_type\"";

        SqlPattern[] patterns = {
            new SqlPattern(
                ACCESS_MYSQL, accessMysqlSql, 26),
            new SqlPattern(Dialect.DatabaseProduct.DERBY, derbySql, derbySql)
        };

        assertRequestSql(
            testContext,
            new CellRequest[]{request},
            patterns);
    }

    public void testCountDistinctAggMiss() {
        final TestContext testContext = getTestContext();
        CellRequest request =
            createRequest(
                testContext,
                "Sales",
                "[Measures].[Customer Count]",
                list("time_by_day", "time_by_day"),
                list("the_year", "quarter"),
                list("1997", "Q1"));

        String mysqlSql =
            "select\n"
            + "    \"time_by_day\".\"the_year\" as \"c0\",\n"
            + "    \"time_by_day\".\"quarter\" as \"c1\",\n"
            + "    count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\"\n"
            + "from\n"
            + "    \"sales_fact_1997\" as \"sales_fact_1997\",\n"
            + "    \"time_by_day\" as \"time_by_day\"\n"
            + "where\n"
            + "    \"time_by_day\".\"the_year\" = 1997\n"
            + "and\n"
            + "    \"time_by_day\".\"quarter\" = \"Q1\"\n"
            + "and\n"
            + "    \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\"\n"
            + "group by\n"
            + "    \"time_by_day\".\"the_year\",\n"
            + "    \"time_by_day\".\"quarter\"";

        SqlPattern[] patterns = {
            new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysqlSql, 26)
        };

        assertRequestSql(
            testContext,
            new CellRequest[]{request},
            patterns);
    }

    public void testCountDistinctAggMatch() {
        if (!(propSaver.props.UseAggregates.get()
              && propSaver.props.ReadAggregates.get()))
        {
            return;
        }
        final TestContext testContext = getTestContext();
        CellRequest request =
            createRequest(
                testContext,
                "Sales",
                "[Measures].[Customer Count]",
                list("time_by_day", "time_by_day", "time_by_day"),
                list("the_year", "quarter", "month_of_year"),
                list("1997", "Q1", "1"));

        String accessSql =
            "select "
            + "`agg_c_10_sales_fact_1997`.`the_year` as `c0`, "
            + "`agg_c_10_sales_fact_1997`.`quarter` as `c1`, "
            + "`agg_c_10_sales_fact_1997`.`month_of_year` as `c2`, "
            + "`agg_c_10_sales_fact_1997`.`customer_count` as `m0` "
            + "from "
            + "`agg_c_10_sales_fact_1997` as `agg_c_10_sales_fact_1997` "
            + "where "
            + "`agg_c_10_sales_fact_1997`.`the_year` = 1997 and "
            + "`agg_c_10_sales_fact_1997`.`quarter` = 'Q1' and "
            + "`agg_c_10_sales_fact_1997`.`month_of_year` = 1";

        SqlPattern[] patterns = {
            new SqlPattern(Dialect.DatabaseProduct.ACCESS, accessSql, 26)};

        assertRequestSql(
            testContext,
            new CellRequest[]{request},
            patterns);
    }

    public void testCountDistinctCannotRollup() {
        // Summary "agg_g_ms_pcat_sales_fact_1997" doesn't match,
        // because we'd need to roll-up the distinct-count measure over
        // "month_of_year".
        final TestContext testContext = getTestContext();
        CellRequest request =
            createRequest(
                testContext,
                "Sales",
                "[Measures].[Customer Count]",
                list("time_by_day", "time_by_day", "product_class"),
                list("the_year", "quarter", "product_family"),
                list("1997", "Q1", "Food"));

        SqlPattern[] patterns = {
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL,
                "select\n"
                + "    \"time_by_day\".\"the_year\" as \"c0\",\n"
                + "    \"time_by_day\".\"quarter\" as \"c1\",\n"
                + "    \"product_class\".\"product_family\" as \"c2\",\n"
                + "    count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\"\n"
                + "from\n"
                + "    \"sales_fact_1997\" as \"sales_fact_1997\",\n"
                + "    \"time_by_day\" as \"time_by_day\",\n"
                + "    \"product\" as \"product\",\n"
                + "    \"product_class\" as \"product_class\"\n"
                + "where\n"
                + "    \"time_by_day\".\"the_year\" = 1997\n"
                + "and\n"
                + "    \"time_by_day\".\"quarter\" = \"Q1\"\n"
                + "and\n"
                + "    \"product_class\".\"product_family\" = \"Food\"\n"
                + "and\n"
                + "    \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\"\n"
                + "and\n"
                + "    \"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\"\n"
                + "and\n"
                + "    \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\"\n"
                + "group by\n"
                + "    \"time_by_day\".\"the_year\",\n"
                + "    \"time_by_day\".\"quarter\",\n"
                + "    \"product_class\".\"product_family\"",
                23)
        };

        assertRequestSql(
            testContext,
            new CellRequest[]{request},
            patterns);
    }

    /**
     * Now, here's a funny thing. Usually you can't roll up a distinct-count
     * aggregate. But if you're rolling up along the dimension which the
     * count is counting, it's OK. In this case, you know that every member
     * can only belong to one group.
     */
    public void testCountDistinctRollupAlongDim() {
        if (!(propSaver.props.UseAggregates.get()
              && propSaver.props.ReadAggregates.get()))
        {
            return;
        }
        // Request has granularity
        //  [Time].[Month]
        //  [Product].[Category]
        //
        // whereas agg table "agg_g_ms_pcat_sales_fact_1997" has
        // granularity
        //
        //  [Time].[Month]
        //  [Product].[Category]
        //  [Gender].[Gender]
        //  [Marital Status].[Marital Status]
        //
        // Because [Gender] and [Marital Status] come from the [Customer]
        // table (the same as the distinct-count measure), we can roll up.
        final TestContext testContext = getTestContext();
        CellRequest request =
            createRequest(
                testContext,
                "Sales",
                "[Measures].[Customer Count]",
                list(
                    "time_by_day", "time_by_day", "time_by_day",
                    "product_class", "product_class", "product_class"),
                list(
                    "the_year", "quarter", "month_of_year",
                    "product_family", "product_department", "product_category"),
                list("1997", "Q1", "1", "Food", "Deli", "Meat"));

        SqlPattern[] patterns = {
            new SqlPattern(
                ACCESS_MYSQL,
                "select\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`the_year` as `c0`,\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`quarter` as `c1`,\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`month_of_year` as `c2`,\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`product_family` as `c3`,\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`product_department` as `c4`,\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`product_category` as `c5`,\n"
                + "    sum(`agg_g_ms_pcat_sales_fact_1997`.`customer_count`) as `m0`\n"
                + "from\n"
                + "    `agg_g_ms_pcat_sales_fact_1997` as `agg_g_ms_pcat_sales_fact_1997`\n"
                + "where\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`the_year` = 1997\n"
                + "and\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`quarter` = 'Q1'\n"
                + "and\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`month_of_year` = 1\n"
                + "and\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`product_family` = 'Food'\n"
                + "and\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`product_department` = 'Deli'\n"
                + "and\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`product_category` = 'Meat'\n"
                + "group by\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`the_year`,\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`quarter`,\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`month_of_year`,\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`product_family`,\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`product_department`,\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`product_category`",
                58)
        };

        assertRequestSql(
            testContext,
            new CellRequest[]{request},
            patterns);
    }

    /**
     * As {@link #testCountDistinctRollupAlongDim}, but we rollup
     * [Marital Status] but not [Gender].
     */
    public void testCountDistinctRollup2() {
        if (!(propSaver.props.UseAggregates.get()
              && propSaver.props.ReadAggregates.get()))
        {
            return;
        }
        final TestContext testContext = getTestContext();
        CellRequest request =
            createRequest(
                testContext,
                "Sales", "[Measures].[Customer Count]",
                list(
                    "time_by_day", "time_by_day", "time_by_day",
                    "product_class", "product_class", "product_class",
                    "customer"),
                list(
                    "the_year", "quarter", "month_of_year", "product_family",
                    "product_department", "product_category", "gender"),
                list(
                    "1997", "Q1", "1", "Food", "Deli", "Meat", "F"));

        SqlPattern[] patterns = {
            new SqlPattern(
                ACCESS_MYSQL,
                "select\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`the_year` as `c0`,\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`quarter` as `c1`,\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`month_of_year` as `c2`,\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`product_family` as `c3`,\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`product_department` as `c4`,\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`product_category` as `c5`,\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`gender` as `c6`,\n"
                + "    sum(`agg_g_ms_pcat_sales_fact_1997`.`customer_count`) as `m0`\n"
                + "from\n"
                + "    `agg_g_ms_pcat_sales_fact_1997` as `agg_g_ms_pcat_sales_fact_1997`\n"
                + "where\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`the_year` = 1997\n"
                + "and\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`quarter` = 'Q1'\n"
                + "and\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`month_of_year` = 1\n"
                + "and\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`product_family` = 'Food'\n"
                + "and\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`product_department` = 'Deli'\n"
                + "and\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`product_category` = 'Meat'\n"
                + "and\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`gender` = 'F'\n"
                + "group by\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`the_year`,\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`quarter`,\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`month_of_year`,\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`product_family`,\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`product_department`,\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`product_category`,\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`gender`",
                58)
        };

        assertRequestSql(
            testContext,
            new CellRequest[]{request},
            patterns);
    }

    /**
     * Tests that cells with the same compound member constraints are
     * loaded in one Sql statement.
     *
     * <p>Cells [Food] and [Drink] have the same constraint:
     *
     * <pre>{[1997].[Q1].[1], [1997].[Q3].[7]}</pre>
     */
    public void testCountDistinctBatchLoading() {
        final TestContext testContext = getTestContext();
        List<List<String>> compoundMembers = list(
            list("1997", "Q1", "1"),
            list("1997", "Q3", "7"));

        CellRequestConstraint aggConstraint =
            makeConstraintYearQuarterMonth(compoundMembers);

        CellRequest request1 =
            createRequest(
                testContext,
                "Sales",
                "[Measures].[Customer Count]",
                list("product_class"),
                list("product_family"),
                list("Food"),
                aggConstraint);

        CellRequest request2 =
            createRequest(
                testContext,
                "Sales",
                "[Measures].[Customer Count]",
                list("product_class"),
                list("product_family"),
                list("Drink"),
                aggConstraint);

        String mysqlSql =
            "select\n"
            + "    `product_class`.`product_family` as `c0`,\n"
            + "    count(distinct `sales_fact_1997`.`customer_id`) as `m0`\n"
            + "from\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `product` as `product`,\n"
            + "    `product_class` as `product_class`,\n"
            + "    `time_by_day` as `time_by_day`\n"
            + "where\n"
            + "    (((`time_by_day`.`the_year`, `time_by_day`.`month_of_year`, `time_by_day`.`quarter`) in ((1997, 1, 'Q1'), (1997, 7, 'Q3'))))\n"
            + "and\n"
            + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
            + "and\n"
            + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "group by\n"
            + "    `product_class`.`product_family`";

        SqlPattern[] patterns = {
            new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysqlSql, mysqlSql)
        };

        assertRequestSql(
            testContext,
            new CellRequest[]{request1, request2},
            patterns);
    }

    /**
     * Tests that an aggregate table is used to speed up a
     * <code>&lt;Member&gt;.Children</code> expression.
     */
    public void testAggMembers() {
        if (propSaver.props.TestExpDependencies.get() > 0) {
            return;
        }
        if (!(propSaver.props.UseAggregates.get()
                && propSaver.props.ReadAggregates.get()))
        {
            return;
        }
        if (!(propSaver.props.EnableNativeCrossJoin.get())) {
            return;
        }
        SqlPattern[] patterns = {
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL,
                "select\n"
                + "    `store`.`store_country` as `c0`\n"
                + "from\n"
                + "    `agg_c_14_sales_fact_1997` as `agg_c_14_sales_fact_1997`,\n"
                + "    `store` as `store`\n"
                + "where\n"
                + "    `agg_c_14_sales_fact_1997`.`the_year` = 1998\n"
                + "and\n"
                + "    `agg_c_14_sales_fact_1997`.`store_id` = `store`.`store_id`\n"
                + "group by\n"
                + "    `store`.`store_country`\n"
                + "order by\n"
                + "    ISNULL(`store`.`store_country`) ASC, `store`.`store_country` ASC",
                26)};

        assertQuerySql(
            getTestContext(),
            "select NON EMPTY {[Customers].[USA]} ON COLUMNS,\n"
            + "       NON EMPTY Crossjoin(Hierarchize(Union({[Store].[All Stores]},\n"
            + "           [Store].[All Stores].Children)), {[Product].[All Products]}) \n"
            + "           ON ROWS\n"
            + "    from [Sales]\n"
            + "    where ([Measures].[Unit Sales], [Time].[1998])",
            patterns);
    }

    /**
     * As {@link #testAggMembers()}, but asks for children of a leaf level.
     * Rewrite using an aggregate table is not possible, so just check that it
     * gets the right result.
     */
    public void testAggChildMembersOfLeaf() {
        assertQueryReturns(
            "select NON EMPTY {[Time].[1997]} ON COLUMNS,\n"
            + "       NON EMPTY Crossjoin(Hierarchize(Union({[Store].[All Stores]},\n"
            + "           [Store].[USA].[CA].[San Francisco].[Store 14].Children)), {[Product].[All Products]}) \n"
            + "           ON ROWS\n"
            + "    from [Sales]\n"
            + "    where [Measures].[Unit Sales]",
            "Axis #0:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #1:\n"
            + "{[Time].[Time].[1997]}\n"
            + "Axis #2:\n"
            + "{[Store].[Stores].[All Stores], [Product].[Products].[All Products]}\n"
            + "Row #0: 266,773\n");
    }

    /**
     * This test case tests for a null pointer that was being thrown
     * inside of CellRequest.
     */
    public void testNoNullPtrInCellRequest() {
        TestContext testContext =
            getTestContext().legacy().createSubstitutingCube(
                "Sales",
                "<Dimension name='Store2' foreignKey='store_id'>\n"
                + "  <Hierarchy hasAll='true' primaryKey='store_id' allMemberName='All Stores'>"
                + "    <Table name='store'/>\n"
                + "    <Level name='Store Country' column='store_country' uniqueMembers='true'/>\n"
                + "    <Level name='Store State'   column='store_state'   uniqueMembers='true'/>\n"
                + "    <Level name='Store City'    column='store_city'    uniqueMembers='false'/>\n"
                + "    <Level name='Store Type'    column='store_type'    uniqueMembers='false'/>\n"
                + "    <Level name='Store Name'    column='store_name'    uniqueMembers='true'/>\n"
                + "  </Hierarchy>\n"
                + "</Dimension>");

        testContext.assertQueryReturns(
            "select {[Measures].[Unit Sales]} on columns, "
            + "Filter ({ "
            + "[Store2].[All Stores].[USA].[CA].[Beverly Hills], "
            + "[Store2].[All Stores].[USA].[CA].[Beverly Hills].[Gourmet Supermarket] "
            + "},[Measures].[Unit Sales] > 0) on rows "
            + "from [Sales] "
            + "where [Store Type].[Store Type].[Small Grocery]",
            "Axis #0:\n"
            + "{[Store Type].[Store Type].[Small Grocery]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n");
    }

    /**
     *  Test that once fetched, column cardinality can be shared between
     *  different queries using the same connection.
     *
     *  <p>Test also that expressions with only table alias difference do not
     *  share cardinality result.
     */
    public void testColumnCardinalityCache() {
        String query1 =
            "select "
            + "NonEmptyCrossJoin("
            + "[Product].[Product Family].Members, "
            + "[Gender].[Gender].Members) on columns "
            + "from [Sales]";

        String query2 =
            "select "
            + "NonEmptyCrossJoin("
            + "[Store].[Store Country].Members, "
            + "[Product].[Product Family].Members) on columns "
            + "from [Warehouse]";

        String cardinalitySqlDerby =
            "select "
            + "count(distinct \"product_class\".\"product_family\") "
            + "from \"product_class\" as \"product_class\"";

        String cardinalitySqlMySql =
            "select "
            + "count(distinct `product_class`.`product_family`) as `c0` "
            + "from `product_class` as `product_class`";

        SqlPattern[] patterns =
            new SqlPattern[] {
                new SqlPattern(
                    Dialect.DatabaseProduct.DERBY,
                    cardinalitySqlDerby,
                    cardinalitySqlDerby),
                new SqlPattern(
                    Dialect.DatabaseProduct.MYSQL,
                    cardinalitySqlMySql,
                    cardinalitySqlMySql)
            };

        final TestContext context = getTestContext().withFreshConnection();

        // This MDX gets the [Product].[Product Family] cardinality from the DB.
        context.executeQuery(query1);

        // This MDX should be able to reuse the cardinality for
        // [Product].[Product Family]; and should not issue a SQL to fetch
        // that from DB again.
        assertQuerySqlOrNot(context, query2, patterns, true, false, false);
    }

    public void testKeyExpressionCardinalityCache() {
        String storeDim1 =
            "<Dimension name=\"Store1\">\n"
            + "  <Hierarchy hasAll=\"true\" primaryKey=\"store_id\">\n"
            + "  <Table name=\"store\"/>\n"
            + "    <Level name=\"Store Country\" uniqueMembers=\"true\">\n"
            + "      <KeyExpression>\n"
            + "         <SQL>\n"
            + "           <Column name=\"store_country\"/>\n"
            + "         </SQL>\n"
            + "      </KeyExpression>\n"
            + "    </Level>\n"
            + "  </Hierarchy>\n"
            + "</Dimension>\n";

        String storeDim2 =
            "<Dimension name=\"Store2\">\n"
            + "  <Hierarchy hasAll=\"true\" primaryKey=\"store_id\">\n"
            + "  <Table name=\"store_ragged\"/>\n"
            + "    <Level name=\"Store Country\" uniqueMembers=\"true\">\n"
            + "      <KeyExpression>\n"
            + "         <SQL>\n"
            + "           <Column name=\"store_country\"/>\n"
            + "         </SQL>\n"
            + "      </KeyExpression>\n"
            + "    </Level>\n"
            + "  </Hierarchy>\n"
            + "</Dimension>\n";

        String salesCube1 =
            "<Cube name=\"Sales1\" defaultMeasure=\"Unit Sales\">\n"
            + "  <Table name=\"sales_fact_1997\" />\n"
            + "  <DimensionUsage name=\"Store1\" source=\"Store1\" foreignKey=\"store_id\"/>\n"
            + "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\" formatString=\"Standard\"/>\n"
            + "  <Measure name=\"Store Sales\" column=\"store_sales\" aggregator=\"sum\" formatString=\"Standard\"/>\n"
            + "</Cube>\n";

        String salesCube2 =
            "<Cube name=\"Sales2\" defaultMeasure=\"Unit Sales\">\n"
            + "  <Table name=\"sales_fact_1997\" />\n"
            + "  <DimensionUsage name=\"Store2\" source=\"Store2\" foreignKey=\"store_id\"/>\n"
            + "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\" formatString=\"Standard\"/>\n"
            + "</Cube>\n";

        String query =
            "select {[Measures].[Unit Sales]} ON COLUMNS, {[Store1].members} ON ROWS FROM [Sales1]";

        String query1 =
            "select {[Measures].[Store Sales]} ON COLUMNS, {[Store1].members} ON ROWS FROM [Sales1]";

        String query2 =
            "select {[Measures].[Unit Sales]} ON COLUMNS, {[Store2].members} ON ROWS FROM [Sales2]";

        String cardinalitySqlDerby1 =
            "select count(distinct \"store\".\"store_country\") from \"store\" as \"store\"";

        String cardinalitySqlMySql1 =
            "select count(distinct `store`.`store_country`) as `c0` from `store` as `store`";

        String cardinalitySqlDerby2 =
            "select count(*) from (select distinct \"store_country\" as \"c0\" from \"store_ragged\" as \"store_ragged\") as \"init\"";

        String cardinalitySqlMySql2 =
            "select count(*) from (select distinct `store_ragged`.`store_country` as `c0` from `store_ragged` as `store_ragged`) as `init`";

        SqlPattern[] patterns1 =
            new SqlPattern[] {
                new SqlPattern(
                    Dialect.DatabaseProduct.DERBY,
                    cardinalitySqlDerby1,
                    cardinalitySqlDerby1),
                new SqlPattern(
                    Dialect.DatabaseProduct.MYSQL,
                    cardinalitySqlMySql1,
                    cardinalitySqlMySql1)
            };

        SqlPattern[] patterns2 =
            new SqlPattern[] {
                new SqlPattern(
                    Dialect.DatabaseProduct.DERBY,
                    cardinalitySqlDerby2,
                    cardinalitySqlDerby2),
                new SqlPattern(
                    Dialect.DatabaseProduct.MYSQL,
                    cardinalitySqlMySql2,
                    cardinalitySqlMySql2)
            };

        TestContext testContext =
            TestContext.instance().legacy().create(
                storeDim1 + storeDim2,
                salesCube1 + salesCube2,
                null,
                null,
                null,
                null);

        // This query causes "store"."store_country" cardinality to be
        // retrieved.
        testContext.executeQuery(query);

        // Query1 will find the "store"."store_country" cardinality in cache.
        assertQuerySqlOrNot(testContext, query1, patterns1, true, false, false);

        // Query2 again will not find the "store_ragged"."store_country"
        // cardinality in cache.
        assertQuerySqlOrNot(
            testContext, query2, patterns2, false, false, false);
    }

    /**
     * Tests that using compound member constrant disables using AggregateTable.
     */
    public void testCountDistinctWithConstraintAggMiss() {
        if (!(propSaver.props.UseAggregates.get()
              && propSaver.props.ReadAggregates.get()))
        {
            return;
        }

        // Request has granularity
        //  [Product].[Category]
        // and the compound constraint on
        //  [Time].[Quarter]
        //
        // whereas agg table "agg_g_ms_pcat_sales_fact_1997" has
        // granularity
        //
        //  [Time].[Quarter]
        //  [Product].[Category]
        //  [Gender].[Gender]
        //  [Marital Status].[Marital Status]
        //
        // The presence of compound constraint causes agg table not used.
        //
        // Note ideally we should also test that non distinct measures could be
        // loaded from Aggregate table; however, the testing framework here uses
        // CellRequest directly which causes any compound constraint to be kept
        // separately. This will cause Aggregate tables not to be used.
        //
        // CellRequest generated by the code form MDX will in this case not
        // separate out the compound constraint from the "regular" constraints
        // and Aggregate tables can still be used.

        final TestContext testContext = getTestContext();
        List<List<String>> compoundMembers = list(list("1997", "Q1", "1"));

        CellRequest request =
            createRequest(
                testContext,
                "Sales",
                "[Measures].[Customer Count]",
                list("product_class", "product_class", "product_class"),
                list(
                    "product_family", "product_department", "product_category"),
                list("Food", "Deli", "Meat"),
                makeConstraintYearQuarterMonth(compoundMembers));

        SqlPattern[] patterns = {
            new SqlPattern(
                ACCESS_MYSQL,
                "select "
                + "`product_class`.`product_family` as `c0`, "
                + "`product_class`.`product_department` as `c1`, "
                + "`product_class`.`product_category` as `c2`, "
                + "count(distinct `sales_fact_1997`.`customer_id`) as `m0` "
                + "from "
                + "`product_class` as `product_class`, `product` as `product`, "
                + "`sales_fact_1997` as `sales_fact_1997`, `time_by_day` as `time_by_day` "
                + "where "
                + "`sales_fact_1997`.`product_id` = `product`.`product_id` and "
                + "`product`.`product_class_id` = `product_class`.`product_class_id` and "
                + "`product_class`.`product_family` = 'Food' and "
                + "`product_class`.`product_department` = 'Deli' and "
                + "`product_class`.`product_category` = 'Meat' and "
                + "`sales_fact_1997`.`time_id` = `time_by_day`.`time_id` and "
                + "(`time_by_day`.`the_year` = 1997 and `time_by_day`.`quarter` = 'Q1' and "
                + "`time_by_day`.`month_of_year` = 1) "
                + "group by "
                + "`product_class`.`product_family`, `product_class`.`product_department`, "
                + "`product_class`.`product_category`",
                58)
        };

        assertRequestSql(
            testContext,
            new CellRequest[]{request},
            patterns);
    }

    /**
     * Test case for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-663">bug MONDRIAN-663,
     * "Improve metadata query (TupleReader) support for aggregation tables to
     * include dimensions defining more than one column"</a>.
     */
    public void testOrdinalExprAggTuplesAndChildren() {
        // this verifies that we can load properties, ordinals, etc out of
        // agg tables in member lookups (tuples and children)
        if (!(propSaver.props.UseAggregates.get()
                && propSaver.props.ReadAggregates.get()))
        {
            return;
        }
        if (!(propSaver.props.EnableNativeCrossJoin.get())) {
            return;
        }
        TestContext.instance().flushSchemaCache();

        String cube = "<Cube name=\"Sales_Prod_Ord\">\n"
        + "  <Table name=\"sales_fact_1997\"/>\n"
        + "  <Dimension name=\"Product\" foreignKey=\"product_id\">\n"
        + "    <Hierarchy hasAll=\"true\" primaryKey=\"product_id\" primaryKeyTable=\"product\">\n"
        + "      <Join leftKey=\"product_class_id\" rightKey=\"product_class_id\">\n"
        + "        <Table name=\"product\"/>\n"
        + "        <Table name=\"product_class\"/>\n"
        + "      </Join>\n"
        + "      <Level name=\"Product Family\" table=\"product_class\" column=\"product_family\"\n"
        + "          uniqueMembers=\"true\"/>\n"
        + "      <Level name=\"Product Department\" table=\"product_class\" column=\"product_department\"\n"
        + "          uniqueMembers=\"false\"/>\n"
        + "      <Level name=\"Product Category\" table=\"product_class\" captionColumn=\"product_family\" column=\"product_category\"\n"
        + "          uniqueMembers=\"false\"/>\n"
        + "      <Level name=\"Product Subcategory\" table=\"product_class\" column=\"product_subcategory\"\n"
        + "          uniqueMembers=\"false\"/>\n"
        + "      <Level name=\"Brand Name\" table=\"product\" column=\"brand_name\" uniqueMembers=\"false\"/>\n"
        + "      <Level name=\"Product Name\" table=\"product\" column=\"product_name\"\n"
        + "          uniqueMembers=\"true\"/>\n"
        + "    </Hierarchy>\n"
        + "  </Dimension>\n"
        + "  <Dimension name=\"Gender\" foreignKey=\"customer_id\">\n"
        + "    <Hierarchy hasAll=\"false\" primaryKey=\"customer_id\">\n"
        + "    <Table name=\"customer\"/>\n"
        + "      <Level name=\"Gender\" column=\"gender\" uniqueMembers=\"true\"/>\n"
        + "    </Hierarchy>\n"
        + "  </Dimension>"
        + "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\"\n"
        + "      formatString=\"Standard\" visible=\"false\"/>\n"
        + "  <Measure name=\"Store Cost\" column=\"store_cost\" aggregator=\"sum\"\n"
        + "      formatString=\"#,###.00\"/>\n"
        + "</Cube>";

        TestContext testContext =
            TestContext.instance().legacy().create(
                null,
                cube,
                null,
                null,
                null,
                null);

        String query =
            "select {[Measures].[Unit Sales]} on columns, "
            + "non empty CrossJoin({[Product].[Food].[Deli].[Meat]},{[Gender].[M]}) on rows "
            + "from [Sales_Prod_Ord] ";

        // first check that the sql is generated correctly

        SqlPattern[] patterns = {
            new SqlPattern(
                ACCESS_MYSQL,
                "select `agg_g_ms_pcat_sales_fact_1997`.`product_family` as `c0`, `agg_g_ms_pcat_sales_fact_1997`.`product_department` as `c1`, `product_class`.`product_category` as `c2`, `product_class`.`product_family` as `c3`, `agg_g_ms_pcat_sales_fact_1997`.`gender` as `c4` from `agg_g_ms_pcat_sales_fact_1997` as `agg_g_ms_pcat_sales_fact_1997`, `product_class` as `product_class` where `product_class`.`product_category` = `agg_g_ms_pcat_sales_fact_1997`.`product_category` and (`agg_g_ms_pcat_sales_fact_1997`.`product_category` = 'Meat' and `agg_g_ms_pcat_sales_fact_1997`.`product_department` = 'Deli' and `agg_g_ms_pcat_sales_fact_1997`.`product_family` = 'Food') and (`agg_g_ms_pcat_sales_fact_1997`.`gender` = 'M') group by `agg_g_ms_pcat_sales_fact_1997`.`product_family`, `agg_g_ms_pcat_sales_fact_1997`.`product_department`, `product_class`.`product_category`, `product_class`.`product_family`, `agg_g_ms_pcat_sales_fact_1997`.`gender` order by ISNULL(`agg_g_ms_pcat_sales_fact_1997`.`product_family`) ASC, `agg_g_ms_pcat_sales_fact_1997`.`product_family` ASC, ISNULL(`agg_g_ms_pcat_sales_fact_1997`.`product_department`) ASC, `agg_g_ms_pcat_sales_fact_1997`.`product_department` ASC, ISNULL(`product_class`.`product_category`) ASC, `product_class`.`product_category` ASC, ISNULL(`agg_g_ms_pcat_sales_fact_1997`.`gender`) ASC, `agg_g_ms_pcat_sales_fact_1997`.`gender` ASC",
                null)
        };

        assertQuerySqlOrNot(
            testContext, query, patterns, false, false, false);

        testContext.assertQueryReturns(
            query,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Products].[Food].[Deli].[Meat], [Customer].[Gender].[M]}\n"
            + "Row #0: 4,705\n");

        Result result = testContext.executeQuery(query);
        // this verifies that the caption for meat is Food
        assertEquals(
            "Meat",
            result.getAxes()[1].getPositions().get(0).get(0).getName());
        assertEquals(
            "Food",
            result.getAxes()[1].getPositions().get(0).get(0).getCaption());

        // Test children
        query =
            "select {[Measures].[Unit Sales]} on columns, "
            + "non empty [Product].[Food].[Deli].Children on rows "
            + "from [Sales_Prod_Ord] ";

        testContext.assertQueryReturns(
            query,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Products].[Food].[Deli].[Meat]}\n"
            + "{[Product].[Products].[Food].[Deli].[Side Dishes]}\n"
            + "Row #0: 4,728\n"
            + "Row #1: 1,262\n");
    }

    public void testAggregatingTuples() {
        if (!(propSaver.props.UseAggregates.get()
                && propSaver.props.ReadAggregates.get()))
        {
            return;
        }
        if (!(propSaver.props.EnableNativeCrossJoin.get())) {
            return;
        }
        // flush cache, to be sure sql is executed
        TestContext.instance().flushSchemaCache();

        // This first query verifies that simple collapsed levels in aggregate
        // tables load as tuples correctly.  The collapsed levels appear
        // in the aggregate table SQL below.

        // also note that at the time of this writing, this exercising the high
        // cardinality tuple reader

        String query =
            "select {[Measures].[Unit Sales]} on columns, "
            + "non empty CrossJoin({[Gender].[M]},{[Marital Status].[M]}) on rows "
            + "from [Sales] ";

        SqlPattern[] patterns = {
            new SqlPattern(
                ACCESS_MYSQL,
                "select\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`gender` as `c0`,\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`marital_status` as `c1`\n"
                + "from\n"
                + "    `agg_g_ms_pcat_sales_fact_1997` as `agg_g_ms_pcat_sales_fact_1997`\n"
                + "where\n"
                + "    (`agg_g_ms_pcat_sales_fact_1997`.`gender` = 'M')\n"
                + "and\n"
                + "    (`agg_g_ms_pcat_sales_fact_1997`.`marital_status` = 'M')\n"
                + "group by\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`gender`,\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`marital_status`\n"
                + "order by\n"
                + "    ISNULL(`agg_g_ms_pcat_sales_fact_1997`.`gender`) ASC, `agg_g_ms_pcat_sales_fact_1997`.`gender` ASC,\n"
                + "    ISNULL(`agg_g_ms_pcat_sales_fact_1997`.`marital_status`) ASC, `agg_g_ms_pcat_sales_fact_1997`.`marital_status` ASC",
                null)
        };

        propSaver.set(propSaver.props.GenerateFormattedSql, true);
        final TestContext testContext = getTestContext();
        assertQuerySqlOrNot(
            testContext, query, patterns, false, false, false);

        assertQueryReturns(
            query,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[M]}\n"
            + "Row #0: 66,460\n");

        // This second query verifies that joined levels on aggregate tables
        // load correctly.

        String query2 =
            "select {[Measures].[Unit Sales]} ON COLUMNS, "
            + "NON EMPTY {[Store].[Store State].Members} ON ROWS "
            + "from [Sales] where [Time].[1997].[Q1]";

        SqlPattern[] patterns2 = {
            new SqlPattern(
                ACCESS_MYSQL,
                "select "
                + "`store`.`store_country` as `c0`, "
                + "`store`.`store_state` as `c1` "
                + "from "
                + "`store` as `store`, "
                + "`agg_c_14_sales_fact_1997` as `agg_c_14_sales_fact_1997` "
                + "where "
                + "`agg_c_14_sales_fact_1997`.`store_id` = `store`.`store_id` and "
                + "`agg_c_14_sales_fact_1997`.`the_year` = 1997 and "
                + "`agg_c_14_sales_fact_1997`.`quarter` = 'Q1' "
                + "group by "
                + "`store`.`store_country`, `store`.`store_state` "
                + "order by "
                + "ISNULL(`store`.`store_country`) ASC, "
                + "`store`.`store_country` ASC, "
                + "ISNULL(`store`.`store_state`) ASC, "
                + "`store`.`store_state` ASC",
                null)
        };

        assertQuerySqlOrNot(
            testContext, query2, patterns2, false, false, false);

        assertQueryReturns(
            query2,
            "Axis #0:\n"
            + "{[Time].[Time].[1997].[Q1]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Store].[USA].[CA]}\n"
            + "{[Store].[USA].[OR]}\n"
            + "{[Store].[USA].[WA]}\n"
            + "Row #0: 16,890\n"
            + "Row #1: 19,287\n"
            + "Row #2: 30,114\n");
    }

    /**
     * this test verifies the collapsed children code in SqlMemberSource
     */
    public void testCollapsedChildren() {
        if (!(propSaver.props.UseAggregates.get()
                && propSaver.props.ReadAggregates.get()))
        {
            return;
        }
        if (!(propSaver.props.EnableNativeCrossJoin.get())) {
            return;
        }
        // flush cache to be sure sql is executed
        TestContext.instance().flushSchemaCache();

        SqlPattern[] patterns = {
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL,
                "select\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`gender` as `c0`\n"
                + "from\n"
                + "    `agg_g_ms_pcat_sales_fact_1997` as `agg_g_ms_pcat_sales_fact_1997`\n"
                + "group by\n"
                + "    `agg_g_ms_pcat_sales_fact_1997`.`gender`\n"
                + "order by\n"
                + "    ISNULL(`agg_g_ms_pcat_sales_fact_1997`.`gender`) ASC, `agg_g_ms_pcat_sales_fact_1997`.`gender` ASC",
                null)
        };

        String query =
            "select non empty [Gender].Children on columns\n"
            + "from [Sales]";

        final TestContext testContext = getTestContext();
        propSaver.set(propSaver.props.GenerateFormattedSql, true);
        assertQuerySqlOrNot(
            testContext, query, patterns, false, false, false);

        assertQueryReturns(
            query,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Customer].[Gender].[F]}\n"
            + "{[Customer].[Gender].[M]}\n"
            + "Row #0: 131,558\n"
            + "Row #0: 135,215\n");
    }

    /**
     * Validates that sql expressions can be used with aggregate copy links.
     * This is an adaptation from a test for MONDRIAN-812 which had specifically
     * tested a bug in the DefaultRecognizer, which is no longer used.
     */
    public void testAttrKeyAsSqlExprWithAgg()
    {
        propSaver.set(propSaver.props.UseAggregates, true);
        propSaver.set(propSaver.props.ReadAggregates, true);
        TestContext testContext = getTestContext()
            .replace(
                "<Table name='agg_c_special_sales_fact_1997'/>",
                "<Table name='agg_c_special_sales_fact_1997'>"
                +    "<ColumnDefs>"
                + "<CalculatedColumnDef name='qtr_expression_agg' type='String'>\n"
                + "    <ExpressionView>\n"
                + "        <SQL dialect='generic'>lower(time_quarter)</SQL>\n"
                + "    </ExpressionView>\n"
                + "</CalculatedColumnDef>"
                + "<CalculatedColumnDef name='unit_sales_negative' type='Integer'>\n"
                + "    <ExpressionView>\n"
                + "        <SQL dialect='generic'>unit_sales_sum * -1</SQL>\n"
                + "    </ExpressionView>\n"
                + "</CalculatedColumnDef>"
                + "</ColumnDefs></Table>")
            .insertCalculatedColumnDef(
                "time_by_day",
                "<ColumnDefs>"
                + "<CalculatedColumnDef name='qtr_expression' type='String'>\n"
                + "    <ExpressionView>\n"
                + "        <SQL dialect='generic'>lower(quarter)</SQL>\n"
                + "    </ExpressionView>\n"
                + "</CalculatedColumnDef>"
                + "</ColumnDefs>")
            .insertCube(
                "<Cube name='FooBar'>\n"
                + "  <Dimensions>"
                + "    <Dimension name='Time2' table='time_by_day' key='Time Id' >\n"
                + "  <Attributes>    "
                + " <Attribute name='Time Id' keyColumn='time_id' hasHierarchy='false'/>"
                + " <Attribute name='qtr' keyColumn='qtr_expression' hasHierarchy='true'/>"
                + "  </Attributes>    "
                + "    </Dimension>\n"
                + "  </Dimensions>"
                + "  <MeasureGroups>"
                + "    <MeasureGroup table='sales_fact_1997'>"
                + "      <Measures>"
                + "        <Measure name='Unit Sales' aggregator='sum' column='unit_sales'>\n"
                + "        </Measure>\n"
                + "      </Measures>"
                + "      <DimensionLinks>\n"
                + "        <ForeignKeyLink dimension='Time2' foreignKeyColumn='time_id'/>\n"
                + "      </DimensionLinks>\n"
                + "    </MeasureGroup>"
                + "            <MeasureGroup table='agg_c_special_sales_fact_1997' type='aggregate'>\n"
                + "                <Measures>\n"
                + "                    <MeasureRef name='Fact Count' aggColumn='fact_count'/>\n"
                + "                    <MeasureRef name='Unit Sales' aggColumn='unit_sales_negative'/>\n"
                + "                </Measures>\n"
                + "                <DimensionLinks>\n"
                + "                    <CopyLink dimension='Time2' attribute='Month'>\n"
                + "                        <Column aggColumn='qtr_expression_agg' table='time_by_day' name='qtr_expression'/>\n"
                + "                    </CopyLink>\n"
                + "                </DimensionLinks>\n"
                + "            </MeasureGroup>\n"
                + "  </MeasureGroups>"
                + "</Cube>");
        // the expression for the measure column multiplies by -1,
        // so we can verifyresults are coming from the agg table.
        testContext.assertQueryReturns(
            "select "
            + "Time2.qtr.members ON COLUMNS\n"
            + "from [FooBar] "
            + "  \n",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time2].[qtr].[All qtr]}\n"
            + "{[Time2].[qtr].[q1]}\n"
            + "{[Time2].[qtr].[q2]}\n"
            + "{[Time2].[qtr].[q3]}\n"
            + "{[Time2].[qtr].[q4]}\n"
            + "Row #0: -266,773\n"
            + "Row #0: -66,291\n"
            + "Row #0: -62,610\n"
            + "Row #0: -65,848\n"
            + "Row #0: -72,024\n");
    }

    /**
     * Test for MONDRIAN-918 and MONDRIAN-903, on the legacy (mondrian
     * version 3) schema. We have added
     * an attribute to AggName called approxRowCount so that the
     * aggregation manager can optimize the aggregation tables without
     * having to issue a select count() query.
     */
    public void testAggNameApproxRowCountLegacy() {
        propSaver.set(propSaver.props.UseAggregates, true);
        propSaver.set(propSaver.props.ReadAggregates, true);
        final TestContext context =
            TestContext.instance().withSchema(
                "<schema name='FooSchema'><Cube name='Sales_Foo' defaultMeasure='Unit Sales'>\n"
                + "  <Table name='sales_fact_1997'>\n"
                + " <AggName name='agg_pl_01_sales_fact_1997' approxRowCount='86000'>\n"
                + "     <AggFactCount column='FACT_COUNT'/>\n"
                + "     <AggForeignKey factColumn='product_id' aggColumn='PRODUCT_ID' />\n"
                + "     <AggForeignKey factColumn='customer_id' aggColumn='CUSTOMER_ID' />\n"
                + "     <AggForeignKey factColumn='time_id' aggColumn='TIME_ID' />\n"
                + "     <AggMeasure name='[Measures].[Unit Sales]' column='UNIT_SALES_SUM' />\n"
                + "     <AggMeasure name='[Measures].[Store Cost]' column='STORE_COST_SUM' />\n"
                + "     <AggMeasure name='[Measures].[Store Sales]' column='STORE_SALES_SUM' />\n"
                + " </AggName>\n"
                + "    <AggExclude name='agg_c_special_sales_fact_1997' />\n"
                + "    <AggExclude name='agg_lc_100_sales_fact_1997' />\n"
                + "    <AggExclude name='agg_lc_10_sales_fact_1997' />\n"
                + "    <AggExclude name='agg_pc_10_sales_fact_1997' />\n"
                + "  </Table>\n"
                + "<Dimension name='Time' type='TimeDimension' foreignKey='time_id'>\n"
                + "    <Hierarchy hasAll='true' name='Weekly' primaryKey='time_id'>\n"
                + "      <Table name='time_by_day'/>\n"
                + "      <Level name='Year' column='the_year' type='Numeric' uniqueMembers='true'\n"
                + "          levelType='TimeYears'/>\n"
                + "      <Level name='Week' column='week_of_year' type='Numeric' uniqueMembers='false'\n"
                + "          levelType='TimeWeeks'/>\n"
                + "      <Level name='Day' column='day_of_month' uniqueMembers='false' type='Numeric'\n"
                + "          levelType='TimeDays'/>\n"
                + "    </Hierarchy>\n"
                + "</Dimension>\n"
                + "<Dimension name='Product' foreignKey='product_id'>\n"
                + "    <Hierarchy hasAll='true' primaryKey='product_id' primaryKeyTable='product'>\n"
                + "      <Join leftKey='product_class_id' rightKey='product_class_id'>\n"
                + "        <Table name='product'/>\n"
                + "        <Table name='product_class'/>\n"
                + "      </Join>\n"
                + "      <Level name='Product Family' table='product_class' column='product_family'\n"
                + "          uniqueMembers='true'/>\n"
                + "      <Level name='Product Department' table='product_class' column='product_department'\n"
                + "          uniqueMembers='false'/>\n"
                + "      <Level name='Product Category' table='product_class' column='product_category'\n"
                + "          uniqueMembers='false'/>\n"
                + "      <Level name='Product Subcategory' table='product_class' column='product_subcategory'\n"
                + "          uniqueMembers='false'/>\n"
                + "      <Level name='Brand Name' table='product' column='brand_name' uniqueMembers='false'/>\n"
                + "      <Level name='Product Name' table='product' column='product_name'\n"
                + "          uniqueMembers='true'/>\n"
                + "    </Hierarchy>\n"
                + "</Dimension>\n"
                + "  <Dimension name='Customers' foreignKey='customer_id'>\n"
                + "    <Hierarchy hasAll='true' allMemberName='All Customers' primaryKey='customer_id'>\n"
                + "      <Table name='customer'/>\n"
                + "      <Level name='Country' column='country' uniqueMembers='true'/>\n"
                + "      <Level name='State Province' column='state_province' uniqueMembers='true'/>\n"
                + "      <Level name='City' column='city' uniqueMembers='false'/>\n"
                + "      <Level name='Name' column='customer_id' type='Numeric' uniqueMembers='true'>\n"
                + "        <NameExpression>\n"
                + "          <SQL dialect='oracle'>\n"
                + "'fname' || ' ' || 'lname'\n"
                + "          </SQL>\n"
                + "          <SQL dialect='hive'>\n"
                + "`customer`.`fullname`\n"
                + "          </SQL>\n"
                + "          <SQL dialect='hsqldb'>\n"
                + "'fname' || ' ' || 'lname'\n"
                + "          </SQL>\n"
                + "          <SQL dialect='access'>\n"
                + "fname + ' ' + lname\n"
                + "          </SQL>\n"
                + "          <SQL dialect='postgres'>\n"
                + "'fname' || ' ' || 'lname'\n"
                + "          </SQL>\n"
                + "          <SQL dialect='mysql'>\n"
                + "CONCAT(`customer`.`fname`, ' ', `customer`.`lname`)\n"
                + "          </SQL>\n"
                + "          <SQL dialect='mssql'>\n"
                + "fname + ' ' + lname\n"
                + "          </SQL>\n"
                + "          <SQL dialect='derby'>\n"
                + "'customer'.'fullname'\n"
                + "          </SQL>\n"
                + "          <SQL dialect='db2'>\n"
                + "CONCAT(CONCAT('customer'.'fname', ' '), 'customer'.'lname')\n"
                + "          </SQL>\n"
                + "          <SQL dialect='luciddb'>\n"
                + "'fname' || ' ' || 'lname'\n"
                + "          </SQL>\n"
                + "          <SQL dialect='neoview'>\n"
                + "'customer'.'fullname'\n"
                + "          </SQL>\n"
                + "          <SQL dialect='teradata'>\n"
                + "'fname' || ' ' || 'lname'\n"
                + "          </SQL>\n"
                + "          <SQL dialect='generic'>\n"
                + "fullname\n"
                + "          </SQL>\n"
                + "        </NameExpression>\n"
                + "        <OrdinalExpression>\n"
                + "          <SQL dialect='oracle'>\n"
                + "'fname' || ' ' || 'lname'\n"
                + "          </SQL>\n"
                + "          <SQL dialect='hsqldb'>\n"
                + "'fname' || ' ' || 'lname'\n"
                + "          </SQL>\n"
                + "          <SQL dialect='access'>\n"
                + "fname + ' ' + lname\n"
                + "          </SQL>\n"
                + "          <SQL dialect='postgres'>\n"
                + "'fname' || ' ' || 'lname'\n"
                + "          </SQL>\n"
                + "          <SQL dialect='mysql'>\n"
                + "CONCAT(`customer`.`fname`, ' ', `customer`.`lname`)\n"
                + "          </SQL>\n"
                + "          <SQL dialect='mssql'>\n"
                + "fname + ' ' + lname\n"
                + "          </SQL>\n"
                + "          <SQL dialect='neoview'>\n"
                + "'customer'.'fullname'\n"
                + "          </SQL>\n"
                + "          <SQL dialect='derby'>\n"
                + "'customer'.'fullname'\n"
                + "          </SQL>\n"
                + "          <SQL dialect='db2'>\n"
                + "CONCAT(CONCAT('customer'.'fname', ' '), 'customer'.'lname')\n"
                + "          </SQL>\n"
                + "          <SQL dialect='luciddb'>\n"
                + "'fname' || ' ' || 'lname'\n"
                + "          </SQL>\n"
                + "          <SQL dialect='generic'>\n"
                + "fullname\n"
                + "          </SQL>\n"
                + "        </OrdinalExpression>\n"
                + "        <Property name='Gender' column='gender'/>\n"
                + "        <Property name='Marital Status' column='marital_status'/>\n"
                + "        <Property name='Education' column='education'/>\n"
                + "        <Property name='Yearly Income' column='yearly_income'/>\n"
                + "      </Level>\n"
                + "    </Hierarchy>\n"
                + "  </Dimension>\n"
                + "  <Measure name='Unit Sales' column='unit_sales' aggregator='sum'\n"
                + "      formatString='Standard'/>\n"
                + "  <Measure name='Store Cost' column='store_cost' aggregator='sum'\n"
                + "      formatString='#,###.00'/>\n"
                + "  <Measure name='Store Sales' column='store_sales' aggregator='sum'\n"
                + "      formatString='#,###.00'/>\n"
                + "  <Measure name='Sales Count' column='product_id' aggregator='count'\n"
                + "      formatString='#,###'/>\n"
                + "  <Measure name='Customer Count' column='customer_id'\n"
                + "      aggregator='distinct-count' formatString='#,###'/>\n"
                + "</Cube></schema>\n");
        final String mdxQuery =
            "select {[Measures].[Unit Sales]} on columns, "
            + "non empty CrossJoin({[Time.Weekly].[1997].[1].[15]},CrossJoin({[Customers].[USA].[CA].[Lincoln Acres].[William Smith]}, {[Product].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Washington].[Washington Diet Cola]})) on rows "
            + "from [Sales_Foo] ";
        final String sqlOracle =
            "select count(*) as \"c0\" from \"agg_pl_01_sales_fact_1997\" \"agg_pl_01_sales_fact_1997\"";
        final String sqlMysql =
            "select count(*) as `c0` from `agg_pl_01_sales_fact_1997` as `agg_pl_01_sales_fact_1997`";
        // If the approxRowCount is used, there should not be
        // a query like : select count(*) from agg_pl_01_sales_fact_1997
        assertQuerySqlOrNot(
            context,
            mdxQuery,
            new SqlPattern[] {
                new SqlPattern(
                    Dialect.DatabaseProduct.ORACLE,
                    sqlOracle,
                    sqlOracle.length()),
                new SqlPattern(
                    Dialect.DatabaseProduct.MYSQL,
                    sqlMysql,
                    sqlMysql.length())
            },
            true,
            false,
            false);
    }

    /**
     * Test for bug
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-918">MONDRIAN-918,
     * "Add an approxRowCount attribute to aggregate tables in the schema"</a>.
     * We have added
     * an attribute to AggName called approxRowCount so that the
     * aggregation manager can optimize the aggregation tables without
     * having to issue a select count() query.
     */
    public void testAggNameApproxRowCount() {
        propSaver.set(propSaver.props.UseAggregates, true);
        propSaver.set(propSaver.props.ReadAggregates, true);

        final String measureGroup =
            "<MeasureGroup table='agg_pl_01_sales_fact_1997' approxRowCount='86000' type='aggregate'>\n"
            + "    <Measures>\n"
            + "        <MeasureRef name='Fact Count' aggColumn='fact_count'/>\n"
            + "        <MeasureRef name='Unit Sales' aggColumn='unit_sales_sum'/>\n"
            + "        <MeasureRef name='Store Cost' aggColumn='store_cost_sum'/>\n"
            + "        <MeasureRef name='Store Sales' aggColumn='store_sales_sum'/>\n"
            + "    </Measures>\n"
            + "    <DimensionLinks>\n"
            + "        <ForeignKeyLink dimension='Product' foreignKeyColumn='product_id'/>\n"
            + "        <ForeignKeyLink dimension='Customer' foreignKeyColumn='customer_id'/>\n"
            + "        <ForeignKeyLink dimension='Time' foreignKeyColumn='time_id'/>\n"
            + "        <NoLink dimension='Promotion'/>\n"
            + "        <NoLink dimension='Store'/>\n"
            + "    </DimensionLinks>\n"
            + "</MeasureGroup>\n";

        final TestContext context =
            getTestContext().withSubstitution(
                new Util.Function1<String, String>() {
                    public String apply(String schema) {
                        int i = schema.indexOf("</MeasureGroup>");
                        assert i >= 0;
                        i += "</MeasureGroup>".length();
                        return schema.substring(0, i) + "\n"
                                + measureGroup
                                + schema.substring(i);
                    }
                }
            );
        final String mdxQuery =
            "select {[Measures].[Unit Sales]} on columns, "
            + "non empty CrossJoin({[Time.Weekly].[1997].[1].[15]},CrossJoin({[Customers].[USA].[CA].[Lincoln Acres].[William Smith]}, {[Product].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Washington].[Washington Diet Cola]})) on rows "
            + "from [Sales]";
        final String sqlOracle =
            "select count(*) as \"c0\" from \"agg_pl_01_sales_fact_1997\" \"agg_pl_01_sales_fact_1997\"";
        final String sqlMysql =
            "select count(*) as `c0` from `agg_pl_01_sales_fact_1997` as `agg_pl_01_sales_fact_1997`";
        // If the approxRowCount is used, there should not be
        // a query like : select count(*) from agg_pl_01_sales_fact_1997
        context.assertQueryReturns(
            "select [Measures].[Unit Sales] on 0,\n"
            + " [Time].[1997].[Q1] * [Customers].[USA].[CA] on 1\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[1997].[Q1], [Customer].[Customers].[USA].[CA]}\n"
            + "Row #0: 16,890\n");
        assertQuerySqlOrNot(
            context,
            mdxQuery,
            new SqlPattern[] {
                new SqlPattern(
                    Dialect.DatabaseProduct.ORACLE,
                    sqlOracle,
                    sqlOracle.length()),
                new SqlPattern(
                    Dialect.DatabaseProduct.MYSQL,
                    sqlMysql,
                    sqlMysql.length())
            },
            true,
            false,
            false);
    }

    public void testNonCollapsedAggregate() throws Exception {
        propSaver.set(propSaver.props.UseAggregates, true);
        propSaver.set(propSaver.props.ReadAggregates, true);

        final String mdx =
            "select {[Product].[Product Family].Members} on rows, {[Measures].[Unit Sales]} on columns from [Foo]";

        final TestContext context =
            getTestContext().withSchema(
                "<Schema name=\"FoodMart\" missingLink=\"ignore\" metamodelVersion=\"4.0\">\n"
                + " <PhysicalSchema>\n"
                + "  <Table name=\"product\" alias=\"product\">\n"
                + "   <ColumnDefs>\n"
                + "   </ColumnDefs>\n"
                + "   <Key name=\"key$0\">\n"
                + "    <Column table=\"product\" name=\"product_id\">\n"
                + "    </Column>\n"
                + "   </Key>\n"
                + "  </Table>\n"
                + "  <Table name=\"product_class\" alias=\"product_class\">\n"
                + "   <ColumnDefs>\n"
                + "   </ColumnDefs>\n"
                + "   <Key name=\"key$0\">\n"
                + "    <Column table=\"product_class\" name=\"product_class_id\">\n"
                + "    </Column>\n"
                + "   </Key>\n"
                + "  </Table>\n"
                + "  <Table name=\"sales_fact_1997\" alias=\"sales_fact_1997\">\n"
                + "   <ColumnDefs>\n"
                + "   </ColumnDefs>\n"
                + "  </Table>\n"
                + "  <Table name=\"agg_l_05_sales_fact_1997\" alias=\"agg_l_05_sales_fact_1997\">\n"
                + "   <ColumnDefs>\n"
                + "   </ColumnDefs>\n"
                + "  </Table>\n"
                + "  <Table name=\"agg_c_special_sales_fact_1997\" alias=\"agg_c_special_sales_fact_1997\">\n"
                + "   <ColumnDefs>\n"
                + "   </ColumnDefs>\n"
                + "  </Table>\n"
                + "  <Link source=\"product\" target=\"sales_fact_1997\" key=\"key$0\">\n"
                + "   <ForeignKey>\n"
                + "    <Column table=\"sales_fact_1997\" name=\"product_id\">\n"
                + "    </Column>\n"
                + "   </ForeignKey>\n"
                + "  </Link>\n"
                + "  <Link source=\"product_class\" target=\"product\" key=\"key$0\">\n"
                + "   <ForeignKey>\n"
                + "    <Column table=\"product\" name=\"product_class_id\">\n"
                + "    </Column>\n"
                + "   </ForeignKey>\n"
                + "  </Link>\n"
                + " </PhysicalSchema>\n"
                + " <Cube name=\"Foo\" visible=\"true\" defaultMeasure=\"Unit Sales\" cache=\"true\" enabled=\"true\" enableScenarios=\"false\">\n"
                + "  <Dimensions>\n"
                + "   <Dimension name=\"Product\" visible=\"true\" key=\"$Id\" hanger=\"false\">\n"
                + "    <Hierarchies>\n"
                + "     <Hierarchy name=\"Product\" visible=\"true\" hasAll=\"true\">\n"
                + "      <Level name=\"Product Family\" visible=\"true\" attribute=\"Product Family\" hideMemberIf=\"Never\">\n"
                + "      </Level>\n"
                + "      <Level name=\"Product Department\" visible=\"true\" attribute=\"Product Department\" hideMemberIf=\"Never\">\n"
                + "      </Level>\n"
                + "     </Hierarchy>\n"
                + "    </Hierarchies>\n"
                + "    <Attributes>\n"
                + "     <Attribute name=\"Product Family\" levelType=\"Regular\" table=\"product_class\" datatype=\"String\" hasHierarchy=\"false\">\n"
                + "      <Key>\n"
                + "       <Column table=\"product_class\" name=\"product_family\">\n"
                + "       </Column>\n"
                + "      </Key>\n"
                + "     </Attribute>\n"
                + "     <Attribute name=\"Product Department\" levelType=\"Regular\" table=\"product_class\" datatype=\"String\" hasHierarchy=\"false\">\n"
                + "      <Key>\n"
                + "       <Column table=\"product_class\" name=\"product_family\">\n"
                + "       </Column>\n"
                + "       <Column table=\"product_class\" name=\"product_department\">\n"
                + "       </Column>\n"
                + "      </Key>\n"
                + "      <Name>\n"
                + "       <Column table=\"product_class\" name=\"product_department\">\n"
                + "       </Column>\n"
                + "      </Name>\n"
                + "     </Attribute>\n"
                + "     <Attribute name=\"$Id\" levelType=\"Regular\" table=\"product\" keyColumn=\"product_id\" hasHierarchy=\"false\">\n"
                + "     </Attribute>\n"
                + "    </Attributes>\n"
                + "   </Dimension>\n"
                + "  </Dimensions>\n"
                + "  <MeasureGroups>\n"
                + "   <MeasureGroup name=\"Foo\" type=\"fact\" table=\"sales_fact_1997\">\n"
                + "    <Measures>\n"
                + "     <Measure name=\"Unit Sales\" formatString=\"Standard\" aggregator=\"sum\">\n"
                + "      <Arguments>\n"
                + "       <Column table=\"sales_fact_1997\" name=\"unit_sales\">\n"
                + "       </Column>\n"
                + "      </Arguments>\n"
                + "     </Measure>\n"
                + "    </Measures>\n"
                + "    <DimensionLinks>\n"
                + "     <ForeignKeyLink dimension=\"Product\">\n"
                + "      <ForeignKey>\n"
                + "       <Column table=\"sales_fact_1997\" name=\"product_id\">\n"
                + "       </Column>\n"
                + "      </ForeignKey>\n"
                + "     </ForeignKeyLink>\n"
                + "    </DimensionLinks>\n"
                + "   </MeasureGroup>\n"
                + "   <MeasureGroup name=\"agg_l_05_sales_fact_1997\" type=\"aggregate\" table=\"agg_l_05_sales_fact_1997\">\n"
                + "    <Measures>\n"
                + "     <MeasureRef name=\"Fact Count\" aggColumn=\"fact_count\">\n"
                + "     </MeasureRef>\n"
                + "     <MeasureRef name=\"[Measures].[Unit Sales]\" aggColumn=\"unit_sales\">\n"
                + "     </MeasureRef>\n"
                + "    </Measures>\n"
                + "    <DimensionLinks>\n"
                + "    <ForeignKeyLink dimension='Product' foreignKeyColumn='product_id'/>"
                + "    </DimensionLinks>\n"
                + "   </MeasureGroup>\n"
                + "  </MeasureGroups>\n"
                + " </Cube>\n"
                + "\n"
                + "</Schema>\n");
        final String sqlMysql =
            "select\n"
            + "    `product_class`.`product_family` as `c0`,\n"
            + "    sum(`agg_l_05_sales_fact_1997`.`unit_sales`) as `m0`\n"
            + "from\n"
            + "    `agg_l_05_sales_fact_1997` as `agg_l_05_sales_fact_1997`,\n"
            + "    `product` as `product`,\n"
            + "    `product_class` as `product_class`\n"
            + "where\n"
            + "    `agg_l_05_sales_fact_1997`.`product_id` = `product`.`product_id`\n"
            + "and\n"
            + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
            + "group by\n"
            + "    `product_class`.`product_family`";
        final String sqlOracle =
            "select\n"
            + "    \"product_class\".\"product_family\" as \"c0\",\n"
            + "    sum(\"agg_l_05_sales_fact_1997\".\"unit_sales\") as \"m0\"\n"
            + "from\n"
            + "    \"agg_l_05_sales_fact_1997\" \"agg_l_05_sales_fact_1997\",\n"
            + "    \"product\" \"product\",\n"
            + "    \"product_class\" \"product_class\"\n"
            + "where\n"
            + "    \"agg_l_05_sales_fact_1997\".\"product_id\" = \"product\".\"product_id\"\n"
            + "and\n"
            + "    \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\"\n"
            + "group by\n"
            + "    \"product_class\".\"product_family\"";

      propSaver.set(propSaver.props.GenerateFormattedSql, true);

      assertQuerySqlOrNot(
          context,
          mdx,
          new SqlPattern[] {
              new SqlPattern(
                  Dialect.DatabaseProduct.ORACLE,
                  sqlOracle,
                  sqlOracle.length()),
              new SqlPattern(
                  Dialect.DatabaseProduct.MYSQL,
                  sqlMysql,
                  sqlMysql.length())
          },
          false, false, true);
    }

    public void testTwoNonCollapsedAggregate() throws Exception {
        propSaver.set(propSaver.props.UseAggregates, true);
        propSaver.set(propSaver.props.ReadAggregates, true);

        final TestContext context =
            getTestContext().withSchema(
                "<Schema name=\"FoodMart\" missingLink=\"ignore\" metamodelVersion=\"4.0\">\n"
                + " <PhysicalSchema>\n"
                + "  <Table name=\"product\" alias=\"product\">\n"
                + "   <ColumnDefs>\n"
                + "   </ColumnDefs>\n"
                + "   <Key name=\"key$0\">\n"
                + "    <Column table=\"product\" name=\"product_id\">\n"
                + "    </Column>\n"
                + "   </Key>\n"
                + "  </Table>\n"
                + "  <Table name=\"product_class\" alias=\"product_class\">\n"
                + "   <ColumnDefs>\n"
                + "   </ColumnDefs>\n"
                + "   <Key name=\"key$0\">\n"
                + "    <Column table=\"product_class\" name=\"product_class_id\">\n"
                + "    </Column>\n"
                + "   </Key>\n"
                + "  </Table>\n"
                + "  <Table name='store'>\n"
                + "       <Key>\n"
                + "           <Column name='store_id'/>\n"
                + "        </Key>\n"
                + "    </Table>\n"
                + "  <Table name=\"sales_fact_1997\" alias=\"sales_fact_1997\">\n"
                + "   <ColumnDefs>\n"
                + "   </ColumnDefs>\n"
                + "  </Table>\n"
                + "  <Table name=\"agg_l_05_sales_fact_1997\" alias=\"agg_l_05_sales_fact_1997\">\n"
                + "   <ColumnDefs>\n"
                + "   </ColumnDefs>\n"
                + "  </Table>\n"
                + "  <Table name=\"agg_c_special_sales_fact_1997\" alias=\"agg_c_special_sales_fact_1997\">\n"
                + "   <ColumnDefs>\n"
                + "   </ColumnDefs>\n"
                + "  </Table>\n"
                + "  <Link source=\"product\" target=\"sales_fact_1997\" key=\"key$0\">\n"
                + "   <ForeignKey>\n"
                + "    <Column table=\"sales_fact_1997\" name=\"product_id\">\n"
                + "    </Column>\n"
                + "   </ForeignKey>\n"
                + "  </Link>\n"
                + "  <Link source=\"product_class\" target=\"product\" key=\"key$0\">\n"
                + "   <ForeignKey>\n"
                + "    <Column table=\"product\" name=\"product_class_id\">\n"
                + "    </Column>\n"
                + "   </ForeignKey>\n"
                + "  </Link>\n"
                + " </PhysicalSchema>\n"
                + " <Cube name=\"Foo\" visible=\"true\" defaultMeasure=\"Unit Sales\" cache=\"true\" enabled=\"true\" enableScenarios=\"false\">\n"
                + "  <Dimensions>\n"
                + "      <Dimension name='Store' table='store' key='Store Id'>\n"
                + "        <Attributes>\n"
                + "          <Attribute name='Store Id' keyColumn='store_id' hasHierarchy='true'/>\n"
                + "        </Attributes>\n"
                + "    </Dimension>\n"
                + "   <Dimension name=\"Product\" visible=\"true\" key=\"$Id\" hanger=\"false\">\n"
                + "    <Hierarchies>\n"
                + "     <Hierarchy name=\"Product\" visible=\"true\" hasAll=\"true\">\n"
                + "      <Level name=\"Product Family\" visible=\"true\" attribute=\"Product Family\" hideMemberIf=\"Never\">\n"
                + "      </Level>\n"
                + "      <Level name=\"Product Department\" visible=\"true\" attribute=\"Product Department\" hideMemberIf=\"Never\">\n"
                + "      </Level>\n"
                + "     </Hierarchy>\n"
                + "    </Hierarchies>\n"
                + "    <Attributes>\n"
                + "     <Attribute name=\"Product Family\" levelType=\"Regular\" table=\"product_class\" datatype=\"String\" hasHierarchy=\"false\">\n"
                + "      <Key>\n"
                + "       <Column table=\"product_class\" name=\"product_family\">\n"
                + "       </Column>\n"
                + "      </Key>\n"
                + "     </Attribute>\n"
                + "     <Attribute name=\"Product Department\" levelType=\"Regular\" table=\"product_class\" datatype=\"String\" hasHierarchy=\"false\">\n"
                + "      <Key>\n"
                + "       <Column table=\"product_class\" name=\"product_family\">\n"
                + "       </Column>\n"
                + "       <Column table=\"product_class\" name=\"product_department\">\n"
                + "       </Column>\n"
                + "      </Key>\n"
                + "      <Name>\n"
                + "       <Column table=\"product_class\" name=\"product_department\">\n"
                + "       </Column>\n"
                + "      </Name>\n"
                + "     </Attribute>\n"
                + "     <Attribute name=\"$Id\" levelType=\"Regular\" table=\"product\" keyColumn=\"product_id\" hasHierarchy=\"false\">\n"
                + "     </Attribute>\n"
                + "    </Attributes>\n"
                + "   </Dimension>\n"
                + "  </Dimensions>\n"
                + "  <MeasureGroups>\n"
                + "   <MeasureGroup name=\"Foo\" type=\"fact\" table=\"sales_fact_1997\">\n"
                + "    <Measures>\n"
                + "     <Measure name=\"Unit Sales\" formatString=\"Standard\" aggregator=\"sum\">\n"
                + "      <Arguments>\n"
                + "       <Column table=\"sales_fact_1997\" name=\"unit_sales\">\n"
                + "       </Column>\n"
                + "      </Arguments>\n"
                + "     </Measure>\n"
                + "    </Measures>\n"
                + "    <DimensionLinks>\n"
                + "     <ForeignKeyLink dimension=\"Product\" foreignKeyColumn='product_id'/>\n"
                + "     <ForeignKeyLink dimension=\"Store\" foreignKeyColumn='store_id'/>\n"
                + "    </DimensionLinks>\n"
                + "   </MeasureGroup>\n"
                + "   <MeasureGroup name=\"agg_l_05_sales_fact_1997\" type=\"aggregate\" table=\"agg_l_05_sales_fact_1997\">\n"
                + "    <Measures>\n"
                + "     <MeasureRef name=\"Fact Count\" aggColumn=\"fact_count\">\n"
                + "     </MeasureRef>\n"
                + "     <MeasureRef name=\"[Measures].[Unit Sales]\" aggColumn=\"unit_sales\">\n"
                + "     </MeasureRef>\n"
                + "    </Measures>\n"
                + "    <DimensionLinks>\n"
                + "     <ForeignKeyLink dimension=\"Store\" foreignKeyColumn='store_id'/>\n"
                + "    <ForeignKeyLink dimension='Product' foreignKeyColumn='product_id'/>"
                + "    </DimensionLinks>\n"
                + "   </MeasureGroup>\n"
                + "  </MeasureGroups>\n"
                + " </Cube>\n"
                + "\n"
                + "</Schema>\n");

        final String mdx =
            "select {Crossjoin([Product].[Product Family].Members, [Store].[Store Id].Members)} on rows, {[Measures].[Unit Sales]} on columns from [Foo]";
        final String sqlOracle =
            "select\n"
            + "    \"store\".\"store_id\" as \"c0\",\n"
            + "    \"product_class\".\"product_family\" as \"c1\",\n"
            + "    sum(\"agg_l_05_sales_fact_1997\".\"unit_sales\") as \"m0\",\n"
            + "    grouping(\"store\".\"store_id\") as \"g0\"\n"
            + "from\n"
            + "    \"agg_l_05_sales_fact_1997\" \"agg_l_05_sales_fact_1997\",\n"
            + "    \"store\" \"store\",\n"
            + "    \"product\" \"product\",\n"
            + "    \"product_class\" \"product_class\"\n"
            + "where\n"
            + "    \"agg_l_05_sales_fact_1997\".\"store_id\" = \"store\".\"store_id\"\n"
            + "and\n"
            + "    \"agg_l_05_sales_fact_1997\".\"product_id\" = \"product\".\"product_id\"\n"
            + "and\n"
            + "    \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\"\n"
            + "group by grouping sets (\n"
            + "    (\"store\".\"store_id\", \"product_class\".\"product_family\"),\n"
            + "    (\"product_class\".\"product_family\"))";
        final String sqlMysql =
            "select\n"
            + "    `store`.`store_id` as `c0`,\n"
            + "    `product_class`.`product_family` as `c1`,\n"
            + "    sum(`agg_l_05_sales_fact_1997`.`unit_sales`) as `m0`\n"
            + "from\n"
            + "    `agg_l_05_sales_fact_1997` as `agg_l_05_sales_fact_1997`,\n"
            + "    `store` as `store`,\n"
            + "    `product` as `product`,\n"
            + "    `product_class` as `product_class`\n"
            + "where\n"
            + "    `agg_l_05_sales_fact_1997`.`store_id` = `store`.`store_id`\n"
            + "and\n"
            + "    `agg_l_05_sales_fact_1997`.`product_id` = `product`.`product_id`\n"
            + "and\n"
            + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
            + "group by\n"
            + "    `store`.`store_id`,\n"
            + "    `product_class`.`product_family`";
        propSaver.set(propSaver.props.GenerateFormattedSql, true);
        assertQuerySqlOrNot(
            context,
            mdx,
            new SqlPattern[] {
                new SqlPattern(
                    Dialect.DatabaseProduct.ORACLE,
                    sqlOracle,
                    sqlOracle.length()),
                new SqlPattern(
                    Dialect.DatabaseProduct.MYSQL,
                    sqlMysql,
                    sqlMysql.length())
            },
            false, false, true);
    }

    /**
     * Tests that, if the lowest level of a hierarchy uses the same column
     * for name as for key, then mondrian does not do a join.
     */
    public void _testSpuriousJoin() {
        final TestContext testContext =
            TestContext.instance().createSubstitutingCube(
                "Sales",
                "  <Dimension name=\"Promotion Plus\" foreignKey=\"promotion_id\">\n"
                + "    <Hierarchy hasAll=\"true\" allMemberName=\"All Promotions\" primaryKey=\"promotion_id\" defaultMember=\"[All Promotions]\">\n"
                + "      <Table name=\"promotion\"/>\n"
                + "      <Level name=\"Promotion Name\" column=\"promotion_name\" uniqueMembers=\"true\"/>\n"
                + "      <Level name=\"Promotion Id\" column=\"promotion_id\" type=\"Numeric\" uniqueMembers=\"true\"/>\n"
                + "    </Hierarchy>\n"
                + "  </Dimension>");

        // Note that query does not join in the "promotion" table.
        String sql =
            "select `time_by_day`.`the_year` as `c0`,"
            + " `sales_fact_1997`.`promotion_id` as `c1`,"
            + " sum(`sales_fact_1997`.`unit_sales`) as `m0` "
            + "from `time_by_day` as `time_by_day`,"
            + " `sales_fact_1997` as `sales_fact_1997` "
            + "where `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`"
            + " and `time_by_day`.`the_year` = 1997"
            + " and `sales_fact_1997`.`promotion_id` in (5, 49, 112, 123, 130, 150, 168, 196, 344, 393, 403, 601, 640, 683, 760, 831, 900, 914, 992, 1005, 1076, 1266, 1288, 1315, 1339, 1354, 1388, 1401, 1454, 1461, 1469, 1483, 1505, 1609, 1626, 1705, 1726, 1784, 1788, 1864) "
            + "group by `time_by_day`.`the_year`, `sales_fact_1997`.`promotion_id`";

        assertQuerySql(
            testContext,
            "select {[Promotion Plus].[Bag Stuffers].Children} on 0\n"
            + "from [Sales]",
            sql);
    }
}

// End TestAggregationManager.java
