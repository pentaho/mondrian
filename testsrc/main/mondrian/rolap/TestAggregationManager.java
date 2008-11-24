/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2002 Kana Software, Inc.
// Copyright (C) 2002-2008 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 28 September, 2002
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.rolap.agg.*;
import mondrian.test.TestContext;
import mondrian.test.SqlPattern;
import mondrian.spi.Dialect;

import java.util.*;

/**
 * Unit test for {@link AggregationManager}.
 *
 * @author jhyde
 * @since 21 March, 2002
 * @version $Id$
 */
public class TestAggregationManager extends BatchTestCase {
    private static final Set<Dialect.DatabaseProduct> ACCESS_MYSQL =
        Util.enumSetOf(
            Dialect.DatabaseProduct.ACCESS,
            Dialect.DatabaseProduct.MYSQL);

    public TestAggregationManager(String name) {
        super(name);
    }
    public TestAggregationManager() {
        super();
    }

    public void testFemaleUnitSales() {
        CellRequest request = createRequest(
            "Sales", "[Measures].[Unit Sales]", "customer", "gender", "F");
        final RolapAggregationManager aggMan = AggregationManager.instance();
        Object value = aggMan.getCellFromCache(request);
        assertNull(value); // before load, the cell is not found
        FastBatchingCellReader fbcr =
                new FastBatchingCellReader(getCube("Sales"));
        fbcr.recordCellRequest(request);
        fbcr.loadAggregations();
        value = aggMan.getCellFromCache(request); // after load, cell is found
        assertTrue(value instanceof Number);
        assertEquals(131558, ((Number) value).intValue());
    }

    public void testFemaleCustomerCount() {
        CellRequest request =
            createRequest("Sales", "[Measures].[Customer Count]", "customer", "gender", "F");
        final RolapAggregationManager aggMan = AggregationManager.instance();
        FastBatchingCellReader fbcr =
            new FastBatchingCellReader(getCube("Sales"));
        Object value = aggMan.getCellFromCache(request);
        assertNull(value); // before load, the cell is not found
        fbcr.recordCellRequest(request);
        fbcr.loadAggregations();
        value = aggMan.getCellFromCache(request); // after load, cell is found
        assertTrue(value instanceof Number);
        assertEquals(2755, ((Number) value).intValue());
    }

    public void testFemaleCustomerCountWithConstraints() {
        List<String[]> Q1M1 = new ArrayList<String[]> ();
        Q1M1.add(new String[] {"1997", "Q1", "1"});

        List<String[]> Q2M5 = new ArrayList<String[]> ();
        Q2M5.add(new String[] {"1997", "Q2", "5"});

        List<String[]> Q1M1Q2M5 = new ArrayList<String[]> ();
        Q1M1Q2M5.add(new String[] {"1997", "Q1", "1"});
        Q1M1Q2M5.add(new String[] {"1997", "Q2", "5"});

        CellRequest request1 =
            createRequest(
                "Sales", "[Measures].[Customer Count]",
                "customer", "gender", "F",
                makeConstraintYearQuarterMonth(Q1M1));

        CellRequest request2 =
            createRequest(
                "Sales", "[Measures].[Customer Count]",
                "customer", "gender", "F",
                makeConstraintYearQuarterMonth(Q2M5));

        CellRequest request3 =
            createRequest(
                "Sales", "[Measures].[Customer Count]",
                "customer", "gender", "F",
                makeConstraintYearQuarterMonth(Q1M1Q2M5));

        final RolapAggregationManager aggMan = AggregationManager.instance();

        Object value = aggMan.getCellFromCache(request1);
        assertNull(value); // before load, the cell is not found

        FastBatchingCellReader fbcr =
                new FastBatchingCellReader(getCube("Sales"));
        fbcr.recordCellRequest(request1);
        fbcr.recordCellRequest(request2);
        fbcr.recordCellRequest(request3);
        fbcr.loadAggregations();

        value = aggMan.getCellFromCache(request1); // after load, cell is found
        assertTrue(value instanceof Number);
        assertEquals(694, ((Number) value).intValue());

        value = aggMan.getCellFromCache(request2); // after load, cell is found
        assertTrue(value instanceof Number);
        assertEquals(672, ((Number) value).intValue());

        value = aggMan.getCellFromCache(request3); // after load, cell is found
        assertTrue(value instanceof Number);
        assertEquals(1122, ((Number) value).intValue());
        // Note: 1122 != (694 + 672)
    }

    /**
     * Tests that a request for ([Measures].[Unit Sales], [Gender].[F])
     * generates the correct SQL.
     */
    public void testFemaleUnitSalesSql() {
        if (!(MondrianProperties.instance().UseAggregates.get() &&
                MondrianProperties.instance().ReadAggregates.get())) {
            return;
        }
        CellRequest request = createRequest("Sales",
            "[Measures].[Unit Sales]", "customer", "gender", "F");

        SqlPattern[] patterns = {
            new SqlPattern(
                ACCESS_MYSQL,
                "select `agg_g_ms_pcat_sales_fact_1997`.`gender` as `c0`," +
                " sum(`agg_g_ms_pcat_sales_fact_1997`.`unit_sales`) as `m0` " +
                "from `agg_g_ms_pcat_sales_fact_1997` as `agg_g_ms_pcat_sales_fact_1997` " +
                "where `agg_g_ms_pcat_sales_fact_1997`.`gender` = 'F' " +
                "group by `agg_g_ms_pcat_sales_fact_1997`.`gender`",
                26)
        };

        assertRequestSql(new CellRequest[]{request}, patterns);
    }

    /**
     * As {@link #testFemaleUnitSalesSql()}, but with aggregate tables switched
     * on.
     *
     * TODO: Enable this test.
     */
    private void _testFemaleUnitSalesSql_withAggs() {
        CellRequest request = createRequest("Sales",
            "[Measures].[Unit Sales]", "customer", "gender", "F");

        SqlPattern[] patterns = {
            new SqlPattern(
                ACCESS_MYSQL,
                "select `customer`.`gender` as `c0`," +
                " sum(`agg_l_03_sales_fact_1997`.`unit_sales`) as `m0` " +
                "from `customer` as `customer`," +
                " `agg_l_03_sales_fact_1997` as `agg_l_03_sales_fact_1997` " +
                "where `agg_l_03_sales_fact_1997`.`customer_id` = `customer`.`customer_id` " +
                "and `customer`.`gender` = 'F' " +
                "group by `customer`.`gender`",
                26)
        };

        assertRequestSql(new CellRequest[]{request}, patterns);
    }

    /**
     * Test a batch containing multiple measures:
     *   (store_state=CA, gender=F, measure=[Unit Sales])
     *   (store_state=CA, gender=M, measure=[Store Sales])
     *   (store_state=OR, gender=M, measure=[Unit Sales])
     */
    public void testMultipleMeasures() {
        if (!(MondrianProperties.instance().UseAggregates.get() &&
                MondrianProperties.instance().ReadAggregates.get())) {
            return;
        }

        CellRequest[] requests = new CellRequest[] {
            createRequest("Sales", "[Measures].[Unit Sales]",
                    new String[] {"customer", "store"},
                    new String[] {"gender", "store_state"},
                    new String[] {"F", "CA"}),
            createRequest("Sales", "[Measures].[Store Sales]",
                    new String[] {"customer", "store"},
                    new String[] {"gender", "store_state"},
                    new String[] {"M", "CA"}),
            createRequest("Sales", "[Measures].[Unit Sales]",
                    new String[] {"customer", "store"},
                    new String[] {"gender", "store_state"},
                    new String[] {"F", "OR"})};

        SqlPattern[] patterns = {
            new SqlPattern(
                ACCESS_MYSQL,
                "select `store`.`store_state` as `c0`," +
                " `customer`.`gender` as `c1`," +
                " sum(`agg_l_05_sales_fact_1997`.`unit_sales`) as `m0`," +
                " sum(`agg_l_05_sales_fact_1997`.`store_sales`) as `m1` " +
                "from `store` as `store`," +
                " `agg_l_05_sales_fact_1997` as `agg_l_05_sales_fact_1997`," +
                " `customer` as `customer` " +
                "where `agg_l_05_sales_fact_1997`.`store_id` = `store`.`store_id` " +
                "and `store`.`store_state` in ('CA', 'OR') " +
                "and `agg_l_05_sales_fact_1997`.`customer_id` = `customer`.`customer_id` " +
                "group by `store`.`store_state`, " +
                "`customer`.`gender`",
                29)
        };

        assertRequestSql(requests, patterns);
    }

    /**
     * As {@link #testMultipleMeasures()}, but with aggregate tables switched
     * on.
     *
     * TODO: Enable this test.
     */
    private void _testMultipleMeasures_withAgg() {
        CellRequest[] requests = new CellRequest[] {
            createRequest("Sales", "[Measures].[Unit Sales]",
                    new String[] {"customer", "store"},
                    new String[] {"gender", "store_state"},
                    new String[] {"F", "CA"}),
            createRequest("Sales", "[Measures].[Store Sales]",
                    new String[] {"customer", "store"},
                    new String[] {"gender", "store_state"},
                    new String[] {"M", "CA"}),
            createRequest("Sales", "[Measures].[Unit Sales]",
                    new String[] {"customer", "store"},
                    new String[] {"gender", "store_state"},
                    new String[] {"F", "OR"})};

        SqlPattern[] patterns = {
            new SqlPattern(
                ACCESS_MYSQL,
                "select `customer`.`gender` as `c0`," +
                " `store`.`store_state` as `c1`," +
                " sum(`agg_l_05_sales_fact_1997`.`unit_sales`) as `m0`," +
                " sum(`agg_l_05_sales_fact_1997`.`store_sales`) as `m1` " +
                "from `customer` as `customer`," +
                " `agg_l_05_sales_fact_1997` as `agg_l_05_sales_fact_1997`," +
                " `store` as `store` " +
                "where `agg_l_05_sales_fact_1997`.`customer_id` = `customer`.`customer_id`" +
                " and `agg_l_05_sales_fact_1997`.`store_id` = `store`.`store_id`" +
                " and `store`.`store_state` in ('CA', 'OR') " +
                "group by `customer`.`gender`, `store`.`store_state`",
                26)
        };

        assertRequestSql(requests, patterns);
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
                TestContext.instance().getFoodMartConnection();
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
            new ValueColumnPredicate(storeTypeColumn, value));
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
                "select {[Measures].[Unit Sales]} on columns," +
                " {[Store].[USA].[CA], [Store].[USA].[OR]} on rows " +
                "from [Sales]";
        SqlPattern[] patterns;
        String accessMysqlSql, derbySql;

        /*
         * Note: the following aggregate loading sqls contain no
         * references to the parent level column "store_country".
         */
        if (MondrianProperties.instance().UseAggregates.get() &&
                MondrianProperties.instance().ReadAggregates.get()) {
            accessMysqlSql =
                "select `store`.`store_state` as `c0`," +
                " `agg_c_14_sales_fact_1997`.`the_year` as `c1`," +
                " sum(`agg_c_14_sales_fact_1997`.`unit_sales`) as `m0` " +
                "from `store` as `store`," +
                " `agg_c_14_sales_fact_1997` as `agg_c_14_sales_fact_1997` " +
                "where `agg_c_14_sales_fact_1997`.`store_id` = `store`.`store_id`" +
                " and `store`.`store_state` in ('CA', 'OR')" +
                " and `agg_c_14_sales_fact_1997`.`the_year` = 1997 " +
                "group by `store`.`store_state`," +
                " `agg_c_14_sales_fact_1997`.`the_year`";

            derbySql =
                "select " +
                "\"store\".\"store_state\" as \"c0\", \"agg_c_14_sales_fact_1997\".\"the_year\" as \"c1\", " +
                "sum(\"agg_c_14_sales_fact_1997\".\"unit_sales\") as \"m0\" " +
                "from " +
                "\"store\" as \"store\", \"agg_c_14_sales_fact_1997\" as \"agg_c_14_sales_fact_1997\" " +
                "where " +
                "\"agg_c_14_sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" and " +
                "\"store\".\"store_state\" in ('CA', 'OR') and " +
                "\"agg_c_14_sales_fact_1997\".\"the_year\" = 1997 " +
                "group by " +
                "\"store\".\"store_state\", \"agg_c_14_sales_fact_1997\".\"the_year\"";

            patterns = new SqlPattern[] {
                new SqlPattern(
                    ACCESS_MYSQL,
                    accessMysqlSql, 50),
                new SqlPattern(Dialect.DatabaseProduct.DERBY, derbySql, derbySql)
            };
        } else {
            accessMysqlSql =
                "select `store`.`store_state` as `c0`," +
                " `time_by_day`.`the_year` as `c1`," +
                " sum(`sales_fact_1997`.`unit_sales`) as `m0` from `store` as `store`," +
                " `sales_fact_1997` as `sales_fact_1997`," +
                " `time_by_day` as `time_by_day` " +
                "where `sales_fact_1997`.`store_id` = `store`.`store_id`" +
                " and `store`.`store_state` in ('CA', 'OR')" +
                " and `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`" +
                " and `time_by_day`.`the_year` = 1997 " +
                "group by `store`.`store_state`, `time_by_day`.`the_year`";

            derbySql =
                "select \"store\".\"store_state\" as \"c0\", \"time_by_day\".\"the_year\" as \"c1\", " +
                "sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" " +
                "from " +
                "\"store\" as \"store\", \"sales_fact_1997\" as \"sales_fact_1997\", " +
                "\"time_by_day\" as \"time_by_day\" " +
                "where " +
                "\"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" and " +
                "\"store\".\"store_state\" in ('CA', 'OR') and " +
                "\"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and " +
                "\"time_by_day\".\"the_year\" = 1997 " +
                "group by " +
                "\"store\".\"store_state\", \"time_by_day\".\"the_year\"";

            patterns = new SqlPattern[] {
                new SqlPattern(
                    ACCESS_MYSQL,
                    accessMysqlSql, 50),
                new SqlPattern(Dialect.DatabaseProduct.DERBY, derbySql, derbySql)
            };
        }
        assertQuerySql(mdxQuery, patterns);
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
        final Dialect dialect = getTestContext().getDialect();
        if (dialect.getDatabaseProduct() != Dialect.DatabaseProduct.DERBY) {
            return;
        }
        String mdxQuery =
            "With " +
            "Set [*NATIVE_CJ_SET] as " +
            "'NonEmptyCrossJoin([*BASE_MEMBERS_Store],[*BASE_MEMBERS_Product])' " +
            "Set [*BASE_MEMBERS_Store] as '{[Store].[All Stores].[USA]}' " +
            "Set [*GENERATED_MEMBERS_Store] as " +
            "'Generate([*NATIVE_CJ_SET], {[Store].CurrentMember})' " +
            "Set [*BASE_MEMBERS_Product] as " +
            "'{[Product].[All Products].[Food],[Product].[All Products].[Drink]}' " +
            "Set [*GENERATED_MEMBERS_Product] as " +
            "'Generate([*NATIVE_CJ_SET], {[Product].CurrentMember})' " +
            "Member [Store].[*FILTER_MEMBER] as 'Aggregate ([*GENERATED_MEMBERS_Store])' " +
            "Member [Product].[*FILTER_MEMBER] as 'Aggregate ([*GENERATED_MEMBERS_Product])' " +
            "Select {[Measures].[Store Sales]} on columns " +
            "From [Sales] " +
            "Where ([Store].[*FILTER_MEMBER], [Product].[*FILTER_MEMBER])";

        String derbySql =
            "select " +
            "\"store\".\"store_country\" as \"c0\", " +
            "\"time_by_day\".\"the_year\" as \"c1\", " +
            "\"product_class\".\"product_family\" as \"c2\", " +
            "sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" " +
            "from " +
            "\"store\" as \"store\", " +
            "\"sales_fact_1997\" as \"sales_fact_1997\", " +
            "\"time_by_day\" as \"time_by_day\", " +
            "\"product_class\" as \"product_class\", " +
            "\"product\" as \"product\" " +
            "where " +
            "\"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" and " +
            "\"store\".\"store_country\" = 'USA' and " +
            "\"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and " +
            "\"time_by_day\".\"the_year\" = 1997 and " +
            "\"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\" and " +
            "\"product\".\"product_class_id\" = \"product_class\".\"product_class_id\" " +
            "group by " +
            "\"store\".\"store_country\", \"time_by_day\".\"the_year\", " +
            "\"product_class\".\"product_family\"";

        SqlPattern[] patterns = {
            new SqlPattern(Dialect.DatabaseProduct.DERBY, derbySql, derbySql)};

        // For derby, the TestAggregationManager.testNonEmptyCrossJoinLoneAxis
        // test fails if the non-empty crossjoin optimizer is used.
        // With it on one gets a recursive call coming through the
        //  RolapEvaluator.getCachedResult.
        assertNoQuerySql(mdxQuery, patterns);
    }

    /**
     * If a hierarchy lives in the fact table, we should not generate a join.
     */
    public void testHierarchyInFactTable() {
        CellRequest request = createRequest("Store",
            "[Measures].[Store Sqft]", "store", "store_type", "Supermarket");

        String accessMysqlSql =
            "select `store`.`store_type` as `c0`," +
            " sum(`store`.`store_sqft`) as `m0` " +
            "from `store` as `store` " +
            "where `store`.`store_type` = 'Supermarket' " +
            "group by `store`.`store_type`";

        String derbySql =
            "select " +
            "\"store\".\"store_type\" as \"c0\", " +
            "sum(\"store\".\"store_sqft\") as \"m0\" " +
            "from " +
            "\"store\" as \"store\" " +
            "where " +
            "\"store\".\"store_type\" = 'Supermarket' " +
            "group by \"store\".\"store_type\"";

        SqlPattern[] patterns = {
            new SqlPattern(
                ACCESS_MYSQL,
                accessMysqlSql, 26),
            new SqlPattern(Dialect.DatabaseProduct.DERBY, derbySql, derbySql)
        };

        assertRequestSql(new CellRequest[]{request}, patterns);
    }

    public void testCountDistinctAggMiss() {
        CellRequest request = createRequest(
            "Sales", "[Measures].[Customer Count]",
            new String[] { "time_by_day", "time_by_day" },
            new String[] { "the_year", "quarter" },
            new String[] { "1997", "Q1" });

        String accessSql =
            "select" +
            " `d0` as `c0`," +
            " `d1` as `c1`," +
            " count(`m0`) as `c2` " +
            "from (" +
            "select distinct `time_by_day`.`the_year` as `d0`, " +
            "`time_by_day`.`quarter` as `d1`, " +
            "`sales_fact_1997`.`customer_id` as `m0` " +
            "from " +
            "`time_by_day` as `time_by_day`, " +
            "`sales_fact_1997` as `sales_fact_1997` " +
            "where " +
            "`sales_fact_1997`.`time_id` = `time_by_day`.`time_id` and " +
            "`time_by_day`.`the_year` = 1997 and " +
            "`time_by_day`.`quarter` = 'Q1'" +
            ") as `dummyname` " +
            "group by `d0`, `d1`";

        String mysqlSql =
            "select" +
            " `time_by_day`.`the_year` as `c0`," +
            " `time_by_day`.`quarter` as `c1`," +
            " count(distinct `sales_fact_1997`.`customer_id`) as `m0` " +
            "from `time_by_day` as `time_by_day`," +
            " `sales_fact_1997` as `sales_fact_1997` " +
            "where `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`" +
            " and `time_by_day`.`the_year` = 1997" +
            " and `time_by_day`.`quarter` = 'Q1' " +
            "group by `time_by_day`.`the_year`," +
            " `time_by_day`.`quarter`";

        String derbySql =
            "select " +
            "\"time_by_day\".\"the_year\" as \"c0\", " +
            "\"time_by_day\".\"quarter\" as \"c1\", " +
            "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\" " +
            "from " +
            "\"time_by_day\" as \"time_by_day\", " +
            "\"sales_fact_1997\" as \"sales_fact_1997\" " +
            "where " +
            "\"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and " +
            "\"time_by_day\".\"the_year\" = 1997 and " +
            "\"time_by_day\".\"quarter\" = 'Q1' " +
            "group by \"time_by_day\".\"the_year\", \"time_by_day\".\"quarter\"";

        SqlPattern[] patterns = {
            new SqlPattern(Dialect.DatabaseProduct.ACCESS, accessSql, 26),
            new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysqlSql, 26),
            new SqlPattern(Dialect.DatabaseProduct.DERBY, derbySql, derbySql)
            };

        assertRequestSql(new CellRequest[]{request}, patterns);
    }

    public void testCountDistinctAggMatch() {
        if (!(MondrianProperties.instance().UseAggregates.get() &&
                MondrianProperties.instance().ReadAggregates.get())) {
            return;
        }
        CellRequest request = createRequest(
            "Sales", "[Measures].[Customer Count]",
            new String[] { "time_by_day", "time_by_day", "time_by_day" },
            new String[] { "the_year", "quarter", "month_of_year" },
            new String[] { "1997", "Q1", "1" });

        String accessSql =
            "select " +
            "`agg_c_10_sales_fact_1997`.`the_year` as `c0`, " +
            "`agg_c_10_sales_fact_1997`.`quarter` as `c1`, " +
            "`agg_c_10_sales_fact_1997`.`month_of_year` as `c2`, " +
            "`agg_c_10_sales_fact_1997`.`customer_count` as `m0` " +
            "from " +
            "`agg_c_10_sales_fact_1997` as `agg_c_10_sales_fact_1997` " +
            "where " +
            "`agg_c_10_sales_fact_1997`.`the_year` = 1997 and " +
            "`agg_c_10_sales_fact_1997`.`quarter` = 'Q1' and " +
            "`agg_c_10_sales_fact_1997`.`month_of_year` = 1";

        SqlPattern[] patterns = {
            new SqlPattern(Dialect.DatabaseProduct.ACCESS, accessSql, 26)};

        assertRequestSql(new CellRequest[]{request}, patterns);
    }

    public void testCountDistinctCannotRollup() {
        // Summary "agg_g_ms_pcat_sales_fact_1997" doesn't match,
        // because we'd need to roll-up the distinct-count measure over
        // "month_of_year".
        CellRequest request = createRequest(
            "Sales", "[Measures].[Customer Count]",
            new String[] { "time_by_day", "time_by_day", "product_class" },
            new String[] { "the_year", "quarter", "product_family" },
            new String[] { "1997", "Q1", "Food" });

        SqlPattern[] patterns = {
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL,
                "select" +
                " `time_by_day`.`the_year` as `c0`," +
                " `time_by_day`.`quarter` as `c1`," +
                " `product_class`.`product_family` as `c2`," +
                " count(distinct `sales_fact_1997`.`customer_id`) as `m0` " +
                "from `time_by_day` as `time_by_day`," +
                " `sales_fact_1997` as `sales_fact_1997`," +
                " `product_class` as `product_class`," +
                " `product` as `product` " +
                "where `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`" +
                " and `time_by_day`.`the_year` = 1997" +
                " and `time_by_day`.`quarter` = `Q1`" +
                " and `sales_fact_1997`.`product_id` = `product`.`product_id`" +
                " and `product`.`product_class_id` = `product_class`.`product_class_id`" +
                " and `product_class`.`product_family` = `Food` " +
                "group by `time_by_day`.`the_year`," +
                " `time_by_day`.`quarter`," +
                " `product_class`.`product_family`",
                23),
            new SqlPattern(
                Dialect.DatabaseProduct.ACCESS,
                "select" +
                " `d0` as `c0`," +
                " `d1` as `c1`," +
                " `d2` as `c2`," +
                " count(`m0`) as `c3` " +
                "from (" +
                "select distinct `time_by_day`.`the_year` as `d0`," +
                " `time_by_day`.`quarter` as `d1`," +
                " `product_class`.`product_family` as `d2`," +
                " `sales_fact_1997`.`customer_id` as `m0` " +
                "from `time_by_day` as `time_by_day`," +
                " `sales_fact_1997` as `sales_fact_1997`," +
                " `product_class` as `product_class`," +
                " `product` as `product` " +
                "where `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`" +
                " and `time_by_day`.`the_year` = 1997" +
                " and `time_by_day`.`quarter` = 'Q1'" +
                " and `sales_fact_1997`.`product_id` = `product`.`product_id`" +
                " and `product`.`product_class_id` = `product_class`.`product_class_id`" +
                " and `product_class`.`product_family` = 'Food') as `dummyname` " +
                "group by `d0`, `d1`, `d2`",
                23),
             new SqlPattern(
                 Dialect.DatabaseProduct.DERBY,
                 "select " +
                 "\"time_by_day\".\"the_year\" as \"c0\", \"time_by_day\".\"quarter\" as \"c1\", " +
                 "\"product_class\".\"product_family\" as \"c2\", " +
                 "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\" " +
                 "from " +
                 "\"time_by_day\" as \"time_by_day\", \"sales_fact_1997\" as \"sales_fact_1997\", " +
                 "\"product_class\" as \"product_class\", \"product\" as \"product\" " +
                 "where " +
                 "\"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and " +
                 "\"time_by_day\".\"the_year\" = 1997 and " +
                 "\"time_by_day\".\"quarter\" = 'Q1' and " +
                 "\"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\" and " +
                 "\"product\".\"product_class_id\" = \"product_class\".\"product_class_id\" and " +
                 "\"product_class\".\"product_family\" = 'Food' " +
                 "group by \"time_by_day\".\"the_year\", \"time_by_day\".\"quarter\", " +
                 "\"product_class\".\"product_family\"",
                 23)
        };

        assertRequestSql(new CellRequest[]{request}, patterns);
    }

    /**
     * Now, here's a funny thing. Usually you can't roll up a distinct-count
     * aggregate. But if you're rolling up along the dimension which the
     * count is counting, it's OK. In this case, you know that every member
     * can only belong to one group.
     */
    public void testCountDistinctRollupAlongDim() {
        if (!(MondrianProperties.instance().UseAggregates.get() &&
                MondrianProperties.instance().ReadAggregates.get())) {
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
        CellRequest request = createRequest(
            "Sales", "[Measures].[Customer Count]",
            new String[] { "time_by_day", "time_by_day", "time_by_day", "product_class", "product_class", "product_class" },
            new String[] { "the_year", "quarter", "month_of_year", "product_family", "product_department", "product_category" },
            new String[] { "1997", "Q1", "1", "Food", "Deli", "Meat" });

        SqlPattern[] patterns = {
            new SqlPattern(
                ACCESS_MYSQL,
                "select `agg_g_ms_pcat_sales_fact_1997`.`the_year` as `c0`," +
                " `agg_g_ms_pcat_sales_fact_1997`.`quarter` as `c1`," +
                " `agg_g_ms_pcat_sales_fact_1997`.`month_of_year` as `c2`," +
                " `agg_g_ms_pcat_sales_fact_1997`.`product_family` as `c3`," +
                " `agg_g_ms_pcat_sales_fact_1997`.`product_department` as `c4`," +
                " `agg_g_ms_pcat_sales_fact_1997`.`product_category` as `c5`," +
                " sum(`agg_g_ms_pcat_sales_fact_1997`.`customer_count`) as `m0` " +
                "from `agg_g_ms_pcat_sales_fact_1997` as `agg_g_ms_pcat_sales_fact_1997` " +
                "where `agg_g_ms_pcat_sales_fact_1997`.`the_year` = 1997" +
                " and `agg_g_ms_pcat_sales_fact_1997`.`quarter` = 'Q1'" +
                " and `agg_g_ms_pcat_sales_fact_1997`.`month_of_year` = 1" +
                " and `agg_g_ms_pcat_sales_fact_1997`.`product_family` = 'Food'" +
                " and `agg_g_ms_pcat_sales_fact_1997`.`product_department` = 'Deli'" +
                " and `agg_g_ms_pcat_sales_fact_1997`.`product_category` = 'Meat' " +
                "group by `agg_g_ms_pcat_sales_fact_1997`.`the_year`," +
                " `agg_g_ms_pcat_sales_fact_1997`.`quarter`," +
                " `agg_g_ms_pcat_sales_fact_1997`.`month_of_year`," +
                " `agg_g_ms_pcat_sales_fact_1997`.`product_family`," +
                " `agg_g_ms_pcat_sales_fact_1997`.`product_department`," +
                " `agg_g_ms_pcat_sales_fact_1997`.`product_category`",
                58)
        };

        assertRequestSql(new CellRequest[]{request}, patterns);
    }

    /**
     * As above, but we rollup [Marital Status] but not [Gender].
     */
    public void testCountDistinctRollup2() {
        if (!(MondrianProperties.instance().UseAggregates.get() &&
                MondrianProperties.instance().ReadAggregates.get())) {
            return;
        }
        CellRequest request = createRequest(
            "Sales", "[Measures].[Customer Count]",
            new String[] { "time_by_day", "time_by_day", "time_by_day", "product_class", "product_class", "product_class", "customer" },
            new String[] { "the_year", "quarter", "month_of_year", "product_family", "product_department", "product_category", "gender" },
            new String[] { "1997", "Q1", "1", "Food", "Deli", "Meat", "F" });

        SqlPattern[] patterns = {
            new SqlPattern(
                ACCESS_MYSQL,
                "select `agg_g_ms_pcat_sales_fact_1997`.`the_year` as `c0`," +
                " `agg_g_ms_pcat_sales_fact_1997`.`quarter` as `c1`," +
                " `agg_g_ms_pcat_sales_fact_1997`.`month_of_year` as `c2`," +
                " `agg_g_ms_pcat_sales_fact_1997`.`product_family` as `c3`," +
                " `agg_g_ms_pcat_sales_fact_1997`.`product_department` as `c4`," +
                " `agg_g_ms_pcat_sales_fact_1997`.`product_category` as `c5`," +
                " `agg_g_ms_pcat_sales_fact_1997`.`gender` as `c6`," +
                " sum(`agg_g_ms_pcat_sales_fact_1997`.`customer_count`) as `m0` " +
                "from `agg_g_ms_pcat_sales_fact_1997` as `agg_g_ms_pcat_sales_fact_1997` " +
                "where `agg_g_ms_pcat_sales_fact_1997`.`the_year` = 1997" +
                " and `agg_g_ms_pcat_sales_fact_1997`.`quarter` = 'Q1'" +
                " and `agg_g_ms_pcat_sales_fact_1997`.`month_of_year` = 1" +
                " and `agg_g_ms_pcat_sales_fact_1997`.`product_family` = 'Food'" +
                " and `agg_g_ms_pcat_sales_fact_1997`.`product_department` = 'Deli'" +
                " and `agg_g_ms_pcat_sales_fact_1997`.`product_category` = 'Meat'" +
                " and `agg_g_ms_pcat_sales_fact_1997`.`gender` = 'F' " +
                "group by `agg_g_ms_pcat_sales_fact_1997`.`the_year`," +
                " `agg_g_ms_pcat_sales_fact_1997`.`quarter`," +
                " `agg_g_ms_pcat_sales_fact_1997`.`month_of_year`," +
                " `agg_g_ms_pcat_sales_fact_1997`.`product_family`," +
                " `agg_g_ms_pcat_sales_fact_1997`.`product_department`," +
                " `agg_g_ms_pcat_sales_fact_1997`.`product_category`," +
                " `agg_g_ms_pcat_sales_fact_1997`.`gender`",
                58)
        };

        assertRequestSql(new CellRequest[]{request}, patterns);
    }

    /*
     * Test that cells with the same compound member constraints are
     * loaded in one Sql statement.
     *
     * Cells [Food] and [Drink] have the same constraint:
     *
     *  {[1997].[Q1].[1], [1997].[Q3].[7]}
     */
    public void testCountDistinctBatchLoading() {
        List<String[]> compoundMembers = new ArrayList<String[]>();
        compoundMembers.add(new String[] {"1997", "Q1", "1"});
        compoundMembers.add(new String[] {"1997", "Q3", "7"});

        CellRequestConstraint aggConstraint =
            makeConstraintYearQuarterMonth(compoundMembers);

        CellRequest request1 = createRequest(
            "Sales", "[Measures].[Customer Count]",
            new String[] {"product_class"},
            new String[] {"product_family"},
            new String[] {"Food"},
            aggConstraint);

        CellRequest request2 = createRequest(
            "Sales", "[Measures].[Customer Count]",
            new String[] {"product_class"},
            new String[] {"product_family"},
            new String[] {"Drink"},
            aggConstraint);

        String mysqlSql =
            "select `product_class`.`product_family` as `c0`, " +
            "count(distinct `sales_fact_1997`.`customer_id`) as `m0` " +
            "from `product_class` as `product_class`, `product` as `product`, " +
            "`sales_fact_1997` as `sales_fact_1997`, `time_by_day` as `time_by_day` " +
            "where `sales_fact_1997`.`product_id` = `product`.`product_id` and " +
            "`product`.`product_class_id` = `product_class`.`product_class_id` and " +
            "`sales_fact_1997`.`time_id` = `time_by_day`.`time_id` and " +
            "(((`time_by_day`.`the_year`, `time_by_day`.`quarter`, `time_by_day`.`month_of_year`) " +
            "in ((1997, 'Q1', 1), (1997, 'Q3', 7)))) " +
            "group by `product_class`.`product_family`";

        String derbySql =
            "select \"product_class\".\"product_family\" as \"c0\", " +
            "count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\" " +
            "from \"product_class\" as \"product_class\", \"product\" as \"product\", " +
            "\"sales_fact_1997\" as \"sales_fact_1997\", \"time_by_day\" as \"time_by_day\" " +
            "where \"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\" and " +
            "\"product\".\"product_class_id\" = \"product_class\".\"product_class_id\" and " +
            "\"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and " +
            "((\"time_by_day\".\"the_year\" = 1997 and \"time_by_day\".\"quarter\" = 'Q1' and \"time_by_day\".\"month_of_year\" = 1) or " +
            "(\"time_by_day\".\"the_year\" = 1997 and \"time_by_day\".\"quarter\" = 'Q3' and \"time_by_day\".\"month_of_year\" = 7)) " +
            "group by \"product_class\".\"product_family\"";


        SqlPattern[] patterns = {
            new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysqlSql, mysqlSql),
            new SqlPattern(Dialect.DatabaseProduct.DERBY, derbySql, derbySql)
        };

        assertRequestSql(new CellRequest[]{request1, request2}, patterns);
    }

    /**
     * Tests that an aggregate table is used to speed up a
     * <code>&lt;Member&gt;.Children</code> expression.
     */
    public void testAggMembers() {
        if (MondrianProperties.instance().TestExpDependencies.get() > 0) {
            return;
        }
        if (!(MondrianProperties.instance().UseAggregates.get() &&
                MondrianProperties.instance().ReadAggregates.get())) {
            return;
        }
        SqlPattern[] patterns = {
            new SqlPattern(
                Dialect.DatabaseProduct.ACCESS,
                "select `store`.`store_country` as `c0` " +
                    "from `agg_c_14_sales_fact_1997` as `agg_c_14_sales_fact_1997`," +
                    " `store` as `store` " +
                    "where `agg_c_14_sales_fact_1997`.`the_year` = 1998 " +
                    "and `agg_c_14_sales_fact_1997`.`store_id` = `store`.`store_id` " +
                    "group by `store`.`store_country` " +
                    "order by `store`.`store_country` ASC",
                26),
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL,
                "select `store`.`store_country` as `c0` " +
                    "from `agg_c_14_sales_fact_1997` as `agg_c_14_sales_fact_1997`," +
                    " `store` as `store` " +
                    "where `agg_c_14_sales_fact_1997`.`the_year` = 1998 " +
                    "and `agg_c_14_sales_fact_1997`.`store_id` = `store`.`store_id` " +
                    "group by `store`.`store_country` " +
                    "order by ISNULL(`store`.`store_country`), `store`.`store_country` ASC",
                26)};

        assertQuerySql(
            "select NON EMPTY {[Customers].[USA]} ON COLUMNS,\n" +
            "       NON EMPTY Crossjoin(Hierarchize(Union({[Store].[All Stores]},\n" +
            "           [Store].[All Stores].Children)), {[Product].[All Products]}) \n" +
            "           ON ROWS\n" +
            "    from [Sales]\n" +
            "    where ([Measures].[Unit Sales], [Time].[1998])",
            patterns);
    }

    /**
     * As {@link #testAggMembers()}, but asks for children of a leaf level.
     * Rewrite using an aggregate table is not possible, so just check that it
     * gets the right result.
     */
    public void testAggChildMembersOfLeaf() {
        assertQueryReturns(
            "select NON EMPTY {[Time].[1997]} ON COLUMNS,\n" +
                "       NON EMPTY Crossjoin(Hierarchize(Union({[Store].[All Stores]},\n" +
                "           [Store].[USA].[CA].[San Francisco].[Store 14].Children)), {[Product].[All Products]}) \n" +
                "           ON ROWS\n" +
                "    from [Sales]\n" +
                "    where [Measures].[Unit Sales]",
            fold(
                "Axis #0:\n" +
                    "{[Measures].[Unit Sales]}\n" +
                    "Axis #1:\n" +
                    "{[Time].[1997]}\n" +
                    "Axis #2:\n" +
                    "{[Store].[All Stores], [Product].[All Products]}\n" +
                    "Row #0: 266,773\n"));
    }

    /**
     * This test case tests for a null pointer that was being thrown
     * inside of CellRequest.
     */
    public void testNoNullPtrInCellRequest() {
        final TestContext testContext = TestContext.createSubstitutingCube(
                "Sales",
                "<Dimension name=\"Store2\" foreignKey=\"store_id\">\n" +
                "  <Hierarchy hasAll=\"true\" primaryKey=\"store_id\" allMemberName=\"All Stores\">" +
                "    <Table name=\"store\"/>\n" +
                "    <Level name=\"Store Country\" column=\"store_country\" uniqueMembers=\"true\"/>\n" +
                "    <Level name=\"Store State\"   column=\"store_state\"   uniqueMembers=\"true\"/>\n" +
                "    <Level name=\"Store City\"    column=\"store_city\"    uniqueMembers=\"false\"/>\n" +
                "    <Level name=\"Store Type\"    column=\"store_type\"    uniqueMembers=\"false\"/>\n" +
                "    <Level name=\"Store Name\"    column=\"store_name\"    uniqueMembers=\"true\"/>\n" +
                "  </Hierarchy>\n" +
                "</Dimension>");

        testContext.assertQueryReturns(
                "select {[Measures].[Unit Sales]} on columns, " +
                "Filter ({ " +
                "[Store2].[All Stores].[USA].[CA].[Beverly Hills], " +
                "[Store2].[All Stores].[USA].[CA].[Beverly Hills].[Gourmet Supermarket] " +
                "},[Measures].[Unit Sales] > 0) on rows " +
                "from [Sales] " +
                "where [Store Type].[Store Type].[Small Grocery]",
            fold(
                "Axis #0:\n" +
                "{[Store Type].[All Store Types].[Small Grocery]}\n" +
                "Axis #1:\n" +
                "{[Measures].[Unit Sales]}\n" +
                "Axis #2:\n"));
    }

    /**
     *  Test that once fetched, column cardinality can be shared between different
     *  queries using the same connection.
     *  Test also that expressions with only table alias difference do not
     *  share cardinality result.
     */
    public void testColumnCadinalityCache() {
        String query1 =
            "select " +
            "NonEmptyCrossJoin(" +
            "[Product].[Product Family].Members, " +
            "[Gender].[Gender].Members) on rows " +
            "from [Sales]";

        String query2 =
            "select " +
            "NonEmptyCrossJoin(" +
            "[Store].[Store Country].Members, " +
            "[Product].[Product Family].Members) on rows " +
            "from [Warehouse]";

        String cardinalitySqlDerby =
            "select " +
            "count(distinct \"product_class\".\"product_family\") " +
            "from \"product_class\" as \"product_class\"";

        String cardinalitySqlMySql =
            "select " +
            "count(distinct `product_class`.`product_family`) as `c0` " +
            "from `product_class` as `product_class`";

        SqlPattern[] patterns =
            new SqlPattern[] {
                new SqlPattern(Dialect.DatabaseProduct.DERBY, cardinalitySqlDerby, cardinalitySqlDerby),
                new SqlPattern(Dialect.DatabaseProduct.MYSQL, cardinalitySqlMySql, cardinalitySqlMySql)
            };

        Connection conn = getTestContext().getFoodMartConnection(false);
        TestContext context = getTestContext(conn);

        // This MDX gets the [Product].[Product Family] cardinality from the DB.
        context.executeQuery(query1);

        // This MDX should be able to reuse the cardinality for
        // [Product].[Product Family]; and should not issue a SQL to fetch
        // that from DB again.
        assertQuerySqlOrNot(context, query2, patterns, true, false, false);
    }

    public void testKeyExpressionCardinalityCache() {
        String storeDim1 =
            "<Dimension name=\"Store1\">\n" +
            "  <Hierarchy hasAll=\"true\" primaryKey=\"store_id\">\n" +
            "  <Table name=\"store\"/>\n" +
            "    <Level name=\"Store Country\" uniqueMembers=\"true\">\n" +
            "      <KeyExpression>\n" +
            "        <SQL dialect=\"oracle\">\n" +
            "\"store_country\"\n" +
            "        </SQL>\n" +
            "        <SQL dialect=\"derby\">\n" +
            "\"store_country\"\n" +
            "        </SQL>\n" +
            "        <SQL dialect=\"luciddb\">\n" +
            "\"store_country\"\n" +
            "        </SQL>\n" +
            "        <SQL dialect=\"mysql\">\n" +
            "`store_country`\n" +
            "        </SQL>\n" +
            "        <SQL dialect=\"generic\">\n" +
            "store_country\n" +
            "        </SQL>\n" +
            "      </KeyExpression>\n" +
            "    </Level>\n" +
            "  </Hierarchy>\n" +
            "</Dimension>\n";

        String storeDim2 =
            "<Dimension name=\"Store2\">\n" +
            "  <Hierarchy hasAll=\"true\" primaryKey=\"store_id\">\n" +
            "  <Table name=\"store_ragged\"/>\n" +
            "    <Level name=\"Store Country\" uniqueMembers=\"true\">\n" +
            "      <KeyExpression>\n" +
            "        <SQL dialect=\"oracle\">\n" +
            "\"store_country\"\n" +
            "        </SQL>\n" +
            "        <SQL dialect=\"derby\">\n" +
            "\"store_country\"\n" +
            "        </SQL>\n" +
            "        <SQL dialect=\"luciddb\">\n" +
            "\"store_country\"\n" +
            "        </SQL>\n" +
            "        <SQL dialect=\"mysql\">\n" +
            "`store_country`\n" +
            "        </SQL>\n" +
            "        <SQL dialect=\"generic\">\n" +
            "store_country\n" +
            "        </SQL>\n" +
            "      </KeyExpression>\n" +
            "    </Level>\n" +
            "  </Hierarchy>\n" +
            "</Dimension>\n";

        String salesCube1 =
            "<Cube name=\"Sales1\" defaultMeasure=\"Unit Sales\">\n" +
            "  <Table name=\"sales_fact_1997\" />\n" +
            "  <DimensionUsage name=\"Store1\" source=\"Store1\" foreignKey=\"store_id\"/>\n" +
            "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\" formatString=\"Standard\"/>\n" +
            "  <Measure name=\"Store Sales\" column=\"store_sales\" aggregator=\"sum\" formatString=\"Standard\"/>\n" +
            "</Cube>\n";

        String salesCube2 =
            "<Cube name=\"Sales2\" defaultMeasure=\"Unit Sales\">\n" +
            "  <Table name=\"sales_fact_1997\" />\n" +
            "  <DimensionUsage name=\"Store2\" source=\"Store2\" foreignKey=\"store_id\"/>\n" +
            "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\" formatString=\"Standard\"/>\n" +
            "</Cube>\n";

        String query =
            "select {[Measures].[Unit Sales]} ON COLUMNS, {[Store1].members} ON ROWS FROM [Sales1]";

        String query1 =
            "select {[Measures].[Store Sales]} ON COLUMNS, {[Store1].members} ON ROWS FROM [Sales1]";

        String query2 =
            "select {[Measures].[Unit Sales]} ON COLUMNS, {[Store2].members} ON ROWS FROM [Sales2]";

        String cardinalitySqlDerby1 =
            "select count(distinct \"store_country\") from \"store\" as \"store\"";

        String cardinalitySqlMySql1 =
            "select count(distinct `store_country`) as `c0` from `store` as `store`";

        String cardinalitySqlDerby2 =
            "select count(distinct \"store_country\") from \"store_ragged\" as \"store_ragged\"";

        String cardinalitySqlMySql2 =
            "select count(distinct `store_country`) as `c0` from `store_ragged` as `store_ragged`";

        SqlPattern[] patterns1 =
            new SqlPattern[] {
                new SqlPattern(Dialect.DatabaseProduct.DERBY, cardinalitySqlDerby1, cardinalitySqlDerby1),
                new SqlPattern(Dialect.DatabaseProduct.MYSQL, cardinalitySqlMySql1, cardinalitySqlMySql1)
            };

        SqlPattern[] patterns2 =
            new SqlPattern[] {
                new SqlPattern(Dialect.DatabaseProduct.DERBY, cardinalitySqlDerby2, cardinalitySqlDerby2),
                new SqlPattern(Dialect.DatabaseProduct.MYSQL, cardinalitySqlMySql2, cardinalitySqlMySql2)
            };

        TestContext testContext =
            TestContext.create(
                storeDim1 + storeDim2,
                salesCube1 + salesCube2,
                null,
                null,
                null,
                null);

        // This query causes "store"."store_country" cardinality to be retrieved.
        testContext.executeQuery(query);

        // Query1 will find the "store"."store_country" cardinality in cache.
        assertQuerySqlOrNot(testContext, query1, patterns1, true, false, false);

        // Query2 again will not find the "store_ragged"."store_country" cardinality in cache.
        assertQuerySqlOrNot(testContext, query2, patterns2, false, false, false);
    }

    /*
     * Test that using compound member constrant disables using AggregateTable
     */
    public void testCountDistinctWithConstraintAggMiss() {
        if (!(MondrianProperties.instance().UseAggregates.get() &&
                MondrianProperties.instance().ReadAggregates.get())) {
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
        // Note ideally we should also test that non distinct measures could be loaded from
        // Aggregate table; however, the testing framework here uses CellRequest directly
        // which causes any compound constraint to be kept separately. This will cause
        // Aggregate tables not to be used.
        // CellRequest generated by the code form MDX will in this case not separate out the
        // compound constraint from the "regular" constraints and Aggregate tables can still
        // be used.

        List<String[]> compoundMembers = new ArrayList<String[]> ();
        compoundMembers.add(new String[] {"1997", "Q1", "1"});

        CellRequest request = createRequest(
            "Sales", "[Measures].[Customer Count]",
            new String[] { "product_class", "product_class", "product_class" },
            new String[] { "product_family", "product_department", "product_category" },
            new String[] { "Food", "Deli", "Meat" },
            makeConstraintYearQuarterMonth(compoundMembers));

        SqlPattern[] patterns = {
            new SqlPattern(
                ACCESS_MYSQL,
                "select " +
                "`product_class`.`product_family` as `c0`, " +
                "`product_class`.`product_department` as `c1`, " +
                "`product_class`.`product_category` as `c2`, " +
                "count(distinct `sales_fact_1997`.`customer_id`) as `m0` " +
                "from " +
                "`product_class` as `product_class`, `product` as `product`, " +
                "`sales_fact_1997` as `sales_fact_1997`, `time_by_day` as `time_by_day` " +
                "where " +
                "`sales_fact_1997`.`product_id` = `product`.`product_id` and " +
                "`product`.`product_class_id` = `product_class`.`product_class_id` and " +
                "`product_class`.`product_family` = 'Food' and " +
                "`product_class`.`product_department` = 'Deli' and " +
                "`product_class`.`product_category` = 'Meat' and " +
                "`sales_fact_1997`.`time_id` = `time_by_day`.`time_id` and " +
                "(`time_by_day`.`the_year` = 1997 and `time_by_day`.`quarter` = 'Q1' and " +
                "`time_by_day`.`month_of_year` = 1) " +
                "group by " +
                "`product_class`.`product_family`, `product_class`.`product_department`, " +
                "`product_class`.`product_category`",
                58)
        };

        assertRequestSql(new CellRequest[]{request}, patterns);
    }

    public void testAggregatingTuples() {
        if (!(MondrianProperties.instance().UseAggregates.get() &&
                MondrianProperties.instance().ReadAggregates.get())) {
            return;
        }

        // flush cache, to be sure sql is executed

        getConnection().getCacheControl(null).flushSchemaCache();

        // This first query verifies that simple collapsed levels in aggregate
        // tables load as tuples correctly.  The collapsed levels appear
        // in the aggregate table SQL below.

        // also note that at the time of this writing, this exercising the high
        // cardinality tuple reader

        String query = "select {[Measures].[Unit Sales]} on columns, " +
        "non empty CrossJoin({[Gender].[M]},{[Marital Status].[M]}) on rows " +
        "from [Sales] ";

        SqlPattern[] patterns = {
                new SqlPattern(
                    ACCESS_MYSQL,
                    "select " +
                    "`agg_g_ms_pcat_sales_fact_1997`.`gender` as `c0`, " +
                    "`agg_g_ms_pcat_sales_fact_1997`.`marital_status` as `c1` " +
                    "from " +
                    "`agg_g_ms_pcat_sales_fact_1997` as `agg_g_ms_pcat_sales_fact_1997` " +
                    "where " +
                    "(`agg_g_ms_pcat_sales_fact_1997`.`gender` = 'M') " +
                    "and (`agg_g_ms_pcat_sales_fact_1997`.`marital_status` = 'M') " +
                    "group by " +
                    "`agg_g_ms_pcat_sales_fact_1997`.`gender`, " +
                    "`agg_g_ms_pcat_sales_fact_1997`.`marital_status` " +
                    "order by " +
                    "ISNULL(`agg_g_ms_pcat_sales_fact_1997`.`gender`), " +
                    "`agg_g_ms_pcat_sales_fact_1997`.`gender` ASC, " +
                    "ISNULL(`agg_g_ms_pcat_sales_fact_1997`.`marital_status`), " +
                    "`agg_g_ms_pcat_sales_fact_1997`.`marital_status` ASC",
                    null)
            };

        assertQuerySqlOrNot(getTestContext(), query, patterns, false, false, false);

        assertQueryReturns(
            query,
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Unit Sales]}\n" +
                "Axis #2:\n" +
                "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M]}\n" +
                "Row #0: 66,460\n"));

        // This second query verifies that joined levels on aggregate tables
        // load correctly.

        String query2 = "select {[Measures].[Unit Sales]} ON COLUMNS, " +
            "NON EMPTY {[Store].[Store State].Members} ON ROWS " +
            "from [Sales] where [Time].[1997].[Q1]";

        SqlPattern[] patterns2 = {
                new SqlPattern(
                    ACCESS_MYSQL,
                    "select " +
                    "`store`.`store_country` as `c0`, " +
                    "`store`.`store_state` as `c1` " +
                    "from " +
                    "`store` as `store`, " +
                    "`agg_c_14_sales_fact_1997` as `agg_c_14_sales_fact_1997` " +
                    "where " +
                    "`agg_c_14_sales_fact_1997`.`store_id` = `store`.`store_id` and " +
                    "`agg_c_14_sales_fact_1997`.`the_year` = 1997 and " +
                    "`agg_c_14_sales_fact_1997`.`quarter` = 'Q1' " +
                    "group by " +
                    "`store`.`store_country`, `store`.`store_state` " +
                    "order by " +
                    "ISNULL(`store`.`store_country`), " +
                    "`store`.`store_country` ASC, " +
                    "ISNULL(`store`.`store_state`), " +
                    "`store`.`store_state` ASC",
                    null)
            };

        assertQuerySqlOrNot(getTestContext(), query2, patterns2, false, false, false);

        assertQueryReturns(
                 query2,
                 fold(
                    "Axis #0:\n" +
                    "{[Time].[1997].[Q1]}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Unit Sales]}\n" +
                    "Axis #2:\n" +
                    "{[Store].[All Stores].[USA].[CA]}\n" +
                    "{[Store].[All Stores].[USA].[OR]}\n" +
                    "{[Store].[All Stores].[USA].[WA]}\n" +
                    "Row #0: 16,890\n" +
                    "Row #1: 19,287\n" +
                    "Row #2: 30,114\n"));
    }

    /**
     * this test verifies the collapsed children code in SqlMemberSource
     */
    public void testCollapsedChildren() {
        if (!(MondrianProperties.instance().UseAggregates.get() &&
                MondrianProperties.instance().ReadAggregates.get())) {
            return;
        }

        // flush cache to be sure sql is executed
        getConnection().getCacheControl(null).flushSchemaCache();

        SqlPattern[] patterns = {
                new SqlPattern(
                    ACCESS_MYSQL,
                    "select " +
                    "`agg_g_ms_pcat_sales_fact_1997`.`gender` as `c0` " +
                    "from " +
                    "`agg_g_ms_pcat_sales_fact_1997` as `agg_g_ms_pcat_sales_fact_1997` " +
                    "group by " +
                    "`agg_g_ms_pcat_sales_fact_1997`.`gender`",
                    null)
            };

        String query = "select non empty [Gender].[All Gender].Children on rows from [Sales]";

        assertQuerySqlOrNot(getTestContext(), query, patterns, false, false, false);

        assertQueryReturns(
                query,
                fold("Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Gender].[All Gender].[F]}\n" +
                    "{[Gender].[All Gender].[M]}\n" +
                    "Row #0: 131,558\n" +
                    "Row #0: 135,215\n"));
    }

}

// End TestAggregationManager.java
