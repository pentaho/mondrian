/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2002 Kana Software, Inc.
// Copyright (C) 2002-2006 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 28 September, 2002
*/
package mondrian.rolap;

import mondrian.olap.Connection;
import mondrian.olap.*;
import mondrian.rolap.agg.*;
import mondrian.rolap.sql.SqlQuery;
import mondrian.rolap.cache.CachePool;
import mondrian.test.TestContext;
import mondrian.test.FoodMartTestCase;
import mondrian.util.DelegatingInvocationHandler;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.sql.*;

/**
 * Unit test for {@link AggregationManager}.
 *
 * @author jhyde
 * @since 21 March, 2002
 * @version $Id$
 */
public class TestAggregationManager extends FoodMartTestCase {
    public TestAggregationManager(String name) {
        super(name);
    }
    public TestAggregationManager() {
        super();
    }

    public void testFemaleUnitSales() {
        CellRequest request = createRequest("Sales", "[Measures].[Unit Sales]", "customer", "gender", "F");
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
                SqlPattern.ACCESS_DIALECT | SqlPattern.MY_SQL_DIALECT,
                "select `agg_g_ms_pcat_sales_fact_1997`.`gender` as `c0`," +
                " sum(`agg_g_ms_pcat_sales_fact_1997`.`unit_sales`) as `m0` " +
                "from `agg_g_ms_pcat_sales_fact_1997` as `agg_g_ms_pcat_sales_fact_1997` " +
                "where `agg_g_ms_pcat_sales_fact_1997`.`gender` = 'F' " +
                "group by `agg_g_ms_pcat_sales_fact_1997`.`gender`",
                26
            )
        };

        assertRequestSql(new CellRequest[] {request}, patterns);
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
                SqlPattern.ACCESS_DIALECT | SqlPattern.MY_SQL_DIALECT,
                "select `customer`.`gender` as `c0`," +
                " sum(`agg_l_03_sales_fact_1997`.`unit_sales`) as `m0` " +
                "from `customer` as `customer`," +
                " `agg_l_03_sales_fact_1997` as `agg_l_03_sales_fact_1997` " +
                "where `agg_l_03_sales_fact_1997`.`customer_id` = `customer`.`customer_id` " +
                "and `customer`.`gender` = 'F' " +
                "group by `customer`.`gender`",
                26
            )
        };

        assertRequestSql(new CellRequest[] {request}, patterns);
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
                SqlPattern.ACCESS_DIALECT | SqlPattern.MY_SQL_DIALECT,
                "select `customer`.`gender` as `c0`," +
                " `store`.`store_state` as `c1`," +
                " sum(`agg_l_05_sales_fact_1997`.`unit_sales`) as `m0`," +
                " sum(`agg_l_05_sales_fact_1997`.`store_sales`) as `m1` " +
                "from `customer` as `customer`," +
                " `agg_l_05_sales_fact_1997` as `agg_l_05_sales_fact_1997`," +
                " `store` as `store` " +
                "where `agg_l_05_sales_fact_1997`.`customer_id` = `customer`.`customer_id` " +
                "and `agg_l_05_sales_fact_1997`.`store_id` = `store`.`store_id` " +
                "and `store`.`store_state` in ('CA', 'OR') " +
                "group by `customer`.`gender`, `store`.`store_state`",
                26
            )
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
                SqlPattern.ACCESS_DIALECT | SqlPattern.MY_SQL_DIALECT,
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
                26
            )
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
                TestContext.instance().getFoodMartConnection(false);
        final boolean fail = true;
        Cube salesCube = connection.getSchema().lookupCube(cube, fail);
        Member storeSqftMeasure = salesCube.getSchemaReader(null)
                .getMemberByUniqueName(Util.explode(measure), fail);
        RolapStar.Measure starMeasure =
                RolapStar.getStarMeasure(storeSqftMeasure);
        CellRequest request = new CellRequest(starMeasure, false, false);
        final RolapStar star = starMeasure.getStar();
        final RolapStar.Column storeTypeColumn = star.lookupColumn(
                table, column);
        request.addConstrainedColumn(
                storeTypeColumn, new ColumnConstraint(value));
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
        if (MondrianProperties.instance().UseAggregates.get() &&
                MondrianProperties.instance().ReadAggregates.get()) {
            patterns = new SqlPattern[] {
                new SqlPattern(
                        SqlPattern.ACCESS_DIALECT | SqlPattern.MY_SQL_DIALECT,
                        "select `store`.`store_state` as `c0`," +
                    " `agg_c_14_sales_fact_1997`.`the_year` as `c1`," +
                    " sum(`agg_c_14_sales_fact_1997`.`unit_sales`) as `m0` " +
                    "from `store` as `store`," +
                    " `agg_c_14_sales_fact_1997` as `agg_c_14_sales_fact_1997` " +
                    "where `agg_c_14_sales_fact_1997`.`store_id` = `store`.`store_id`" +
                    " and `store`.`store_state` in ('CA', 'OR')" +
                    " and `agg_c_14_sales_fact_1997`.`the_year` = 1997 " +
                    "group by `store`.`store_state`," +
                    " `agg_c_14_sales_fact_1997`.`the_year`",
                        26
                )
            };
        } else {
            // Note -- no references to store_country!!
            patterns = new SqlPattern[] {
                new SqlPattern(
                        SqlPattern.ACCESS_DIALECT | SqlPattern.MY_SQL_DIALECT,
                        "select `store`.`store_state` as `c0`," +
                    " `time_by_day`.`the_year` as `c1`," +
                    " sum(`sales_fact_1997`.`unit_sales`) as `m0` from `store` as `store`," +
                    " `sales_fact_1997` as `sales_fact_1997`," +
                    " `time_by_day` as `time_by_day` " +
                    "where `sales_fact_1997`.`store_id` = `store`.`store_id`" +
                    " and `store`.`store_state` in ('CA', 'OR')" +
                    " and `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`" +
                    " and `time_by_day`.`the_year` = 1997 " +
                    "group by `store`.`store_state`, `time_by_day`.`the_year`",
                        26
                )
            };
        }
        assertQuerySql(
                mdxQuery,
                patterns);
    }

    /**
     * If a hierarchy lives in the fact table, we should not generate a join.
     */
    public void testHierarchyInFactTable() {
        CellRequest request = createRequest("Store",
            "[Measures].[Store Sqft]", "store", "store_type", "Supermarket");

        SqlPattern[] patterns = {
            new SqlPattern(
                SqlPattern.ACCESS_DIALECT | SqlPattern.MY_SQL_DIALECT,
                "select `store`.`store_type` as `c0`," +
                " sum(`store`.`store_sqft`) as `m0` " +
                "from `store` as `store` " +
                "where `store`.`store_type` = 'Supermarket' " +
                "group by `store`.`store_type`",
                26
            )
        };

        assertRequestSql(new CellRequest[] {request}, patterns);
    }

    public void testCountDistinctAggMiss() {
        CellRequest request = createRequest(
            "Sales", "[Measures].[Customer Count]",
            new String[] { "time_by_day", "time_by_day" },
            new String[] { "the_year", "quarter" },
            new String[] { "1997", "Q1" });

        SqlPattern[] patterns = {
            new SqlPattern(
                SqlPattern.ACCESS_DIALECT,
                "select" +
                " d0 as `c0`," +
                " d1 as `c1`," +
                " count(m0) as `c2` " +
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
                "group by d0, d1",
                26
            ),
            new SqlPattern(
                SqlPattern.MY_SQL_DIALECT,
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
                " `time_by_day`.`quarter`",
                26
            )
        };

        assertRequestSql(new CellRequest[] {request}, patterns);
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

        SqlPattern[] patterns = {
            new SqlPattern(
                SqlPattern.ACCESS_DIALECT,
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
                "`agg_c_10_sales_fact_1997`.`month_of_year` = 1",
                26),
        };

        assertRequestSql(new CellRequest[] {request}, patterns);
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
                SqlPattern.MY_SQL_DIALECT,
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
                SqlPattern.ACCESS_DIALECT,
                "select" +
                " d0 as `c0`," +
                " d1 as `c1`," +
                " d2 as `c2`," +
                " count(m0) as `c3` " +
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
                "group by d0, d1, d2",
                23)
        };

        assertRequestSql(new CellRequest[] {request}, patterns);
    }

    /**
     * Now, here's a funny thing. Usually you can't roll up a distinct-count
     * aggregate. But if you're rolling up along the dimension which the
     * count is counting, it's OK. In this case, you know that every
     */
    public void testCountDistinctRollupAlongDim() {
        if (!(MondrianProperties.instance().UseAggregates.get() &&
                MondrianProperties.instance().ReadAggregates.get())) {
            return;
        }
        // Request has granularity
        //  [Time].[Quarter]
        //  [Product].[Category]
        //
        // whereas agg table "agg_g_ms_pcat_sales_fact_1997" has
        // granularity
        //
        //  [Time].[Quarter]
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
                SqlPattern.MY_SQL_DIALECT | SqlPattern.ACCESS_DIALECT,
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

        assertRequestSql(new CellRequest[] {request}, patterns);
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
                SqlPattern.MY_SQL_DIALECT | SqlPattern.ACCESS_DIALECT,
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

        assertRequestSql(new CellRequest[] {request}, patterns);
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

    private void assertRequestSql(
            CellRequest[] requests,
            SqlPattern[] patterns) {
        RolapStar star = requests[0].getMeasure().getStar();
        SqlQuery.Dialect dialect = star.getSqlQueryDialect();
        int d = SqlPattern.getDialect(dialect);
        SqlPattern sqlPattern = SqlPattern.getPattern(d, patterns);
        if (sqlPattern == null) {
            // This is the current behavior: when the dialect is not
            // Access then do not run the test. We do not print
            // any warning message.
            return;
        }

        String sql = sqlPattern.getSql();
        String trigger = sqlPattern.getTriggerSql();

        // Create a dummy DataSource which will throw a 'bomb' if it is asked
        // to execute a particular SQL statement, but will otherwise behave
        // exactly the same as the current DataSource.
        DataSource oldDataSource = star.getDataSource();
        final DataSource dataSource = (DataSource) Proxy.newProxyInstance(
                null,
                new Class[] {DataSource.class},
                new DataSourceHandler(oldDataSource, trigger));
        star.setDataSource(dataSource);
        Bomb bomb;
        try {
            FastBatchingCellReader fbcr =
                new FastBatchingCellReader(getCube("Sales"));
            for (int i = 0; i < requests.length; i++)
                fbcr.recordCellRequest(requests[i]);
            fbcr.loadAggregations();
            bomb = null;
        } catch (Bomb e) {
            bomb = e;
        } finally {
            star.setDataSource(oldDataSource);
        }
        assertTrue(bomb != null);
        assertEquals(replaceQuotes(sql), replaceQuotes(bomb.sql));
    }

    /**
     * Checks that a given MDX query results in a particular SQL statement
     * being generated.
     *
     * @param mdxQuery MDX query
     * @param patterns Set of patterns, one for each dialect.
     */
    private void assertQuerySql(String mdxQuery, SqlPattern[] patterns) {
        final TestContext testContext = getTestContext();
        final Connection connection = testContext.getConnection();
        final Query query = connection.parseQuery(mdxQuery);
        final Cube cube = query.getCube();
        RolapStar star = ((RolapCube) cube).getStar();

        SqlQuery.Dialect dialect = star.getSqlQueryDialect();
        int d = SqlPattern.getDialect(dialect);
        SqlPattern sqlPattern = SqlPattern.getPattern(d, patterns);
        if (sqlPattern == null) {
            // This is the current behavior: when the dialect is not
            // Access then do not run the test. We do not print
            // any warning message.
            return;
        }

        String sql = sqlPattern.getSql();
        String trigger = sqlPattern.getTriggerSql();

        // Create a dummy DataSource which will throw a 'bomb' if it is asked
        // to execute a particular SQL statement, but will otherwise behave
        // exactly the same as the current DataSource.
        DataSource oldDataSource = star.getDataSource();
        final DataSource dataSource = (DataSource) Proxy.newProxyInstance(
                null,
                new Class[] {DataSource.class},
                new DataSourceHandler(oldDataSource, trigger));
        star.setDataSource(dataSource);
        Bomb bomb;
        try {
            // Flush the cache, to ensure that the query gets executed.
            CachePool.instance().flush();

            final Result result = connection.execute(query);
            Util.discard(result);
            bomb = null;
        } catch (Bomb e) {
            bomb = e;
        } finally {
            star.setDataSource(oldDataSource);
        }
        if (bomb == null) {
            fail("expected query [" + sql + "] did not occur");
        }
        assertEquals(replaceQuotes(sql), replaceQuotes(bomb.sql));
    }

    private static String replaceQuotes(String s) {
      s = s.replace('`', '\"');
      s = s.replace('\'', '\"');
      return s;
    }

    private CellRequest createRequest(
            final String cube, final String measure,
            final String table, final String column, final String value) {
        final Connection connection =
            TestContext.instance().getFoodMartConnection(false);
        final boolean fail = true;
        Cube salesCube = connection.getSchema().lookupCube(cube, fail);
        Member storeSqftMeasure =
            salesCube.getSchemaReader(null).getMemberByUniqueName(
                Util.explode(measure), fail);
        RolapStar.Measure starMeasure =
            RolapStar.getStarMeasure(storeSqftMeasure);
        CellRequest request = new CellRequest(starMeasure, false, false);
        final RolapStar star = starMeasure.getStar();
        final RolapStar.Column storeTypeColumn = star.lookupColumn(
                table, column);
        request.addConstrainedColumn(
                storeTypeColumn, new ColumnConstraint(value));
        return request;
    }

    private CellRequest createRequest(
            final String cube, final String measureName,
            final String[] tables, final String[] columns,
            final String[] values) {
        final Connection connection =
            TestContext.instance().getFoodMartConnection(false);
        final boolean fail = true;
        Cube salesCube = connection.getSchema().lookupCube(cube, fail);
        Member measure = salesCube.getSchemaReader(null).getMemberByUniqueName(
                Util.explode(measureName), fail);
        RolapStar.Measure starMeasure = RolapStar.getStarMeasure(measure);
        CellRequest request = new CellRequest(starMeasure, false, false);
        final RolapStar star = starMeasure.getStar();
        for (int i = 0; i < tables.length; i++) {
            String table = tables[i];
            String column = columns[i];
            String value = values[i];
            final RolapStar.Column storeTypeColumn = star.lookupColumn(
                    table, column);
            request.addConstrainedColumn(
                    storeTypeColumn, new ColumnConstraint(value));
        }
        return request;
    }

    private RolapCube getCube(final String cube) {
        final Connection connection =
            TestContext.instance().getFoodMartConnection(false);
        final boolean fail = true;
        return (RolapCube) connection.getSchema().lookupCube(cube, fail);
    }


    public static class DataSourceHandler extends DelegatingInvocationHandler {
        private final DataSource dataSource;
        private final String trigger;

        public DataSourceHandler(DataSource dataSource, String trigger) {
            super(dataSource);
            this.dataSource = dataSource;
            this.trigger = trigger;
        }

        public java.sql.Connection getConnection() throws SQLException {
            final java.sql.Connection connection = dataSource.getConnection();
            return (java.sql.Connection) Proxy.newProxyInstance(
                    null,
                    new Class[]{java.sql.Connection.class},
                    new TriggerHandler(connection));
        }

        public class TriggerHandler extends DelegatingInvocationHandler {
            private final java.sql.Connection connection;

            public TriggerHandler(java.sql.Connection connection) {
                super(connection);
                this.connection = connection;
            }

            public Statement createStatement() throws SQLException {
                final Statement statement = connection.createStatement();
                return (Statement) Proxy.newProxyInstance(
                        null,
                        new Class[]{Statement.class},
                        new StatementHandler(statement));
            }
        }

        public class StatementHandler extends DelegatingInvocationHandler {
            private final Statement statement;

            public StatementHandler(Statement statement) {
                super(statement);
                this.statement = statement;
            }

            public ResultSet executeQuery(String sql) throws SQLException {
                if (matchTrigger(sql)) {
                    throw new Bomb(sql);
                } else {
                    return statement.executeQuery(sql);
                }
            }

            // different versions of mysql drivers use different quoting, so ignore quotes
            private boolean matchTrigger(String sql) {
                if (trigger == null)
                    return true;
                String s = replaceQuotes(sql);
                String t = replaceQuotes(trigger);
                return s.startsWith(t);
            }
        }
    }

    private static class SqlPattern {
        /**
         * Duplicating information in SqlQuery. Switch from type "int" to
         * "long" if we get more than 32 dialects
         */
        private static final int UNKNOWN_DIALECT        = 0x00000000;
        private static final int ACCESS_DIALECT         = 0x00000001;
        private static final int DERBY_DIALECT          = 0x00000002;
        private static final int CLOUDSCAPE_DIALECT     = 0x00000004;
        private static final int DB2_DIALECT            = 0x00000008;
        private static final int AS400_DIALECT          = 0x00000010;
        private static final int OLD_AS400_DIALECT      = 0x00000020;
        private static final int INFOMIX_DIALECT        = 0x00000040;
        private static final int MS_SQL_DIALECT         = 0x00000080;
        private static final int ORACLE_DIALECT         = 0x00000100;
        private static final int POSTGRES_DIALECT       = 0x00000200;
        private static final int MY_SQL_DIALECT         = 0x00000400;
        private static final int SYBASE_DIALECT         = 0x00000800;

        public static int getDialect(SqlQuery.Dialect dialect) {
            if (dialect.isAccess()) {
                return ACCESS_DIALECT;
            } else if (dialect.isDerby()) {
                return DERBY_DIALECT;
            } else if (dialect.isCloudscape()) {
                return CLOUDSCAPE_DIALECT;
            } else if (dialect.isDB2()) {
                return DB2_DIALECT;
            } else if (dialect.isAS400()) {
                return AS400_DIALECT;
            } else if (dialect.isOldAS400()) {
                return OLD_AS400_DIALECT;
            } else if (dialect.isInformix()) {
                return INFOMIX_DIALECT;
            } else if (dialect.isMSSQL()) {
                return MS_SQL_DIALECT;
            } else if (dialect.isOracle()) {
                return ORACLE_DIALECT;
            } else if (dialect.isPostgres()) {
                return POSTGRES_DIALECT;
            } else if (dialect.isMySQL()) {
                return MY_SQL_DIALECT;
            } else if (dialect.isSybase()) {
                return SYBASE_DIALECT;
            } else {
                return UNKNOWN_DIALECT;
            }
        }
        public static SqlPattern getPattern(int d, SqlPattern[] patterns) {
            if (patterns == null) {
                return null;
            }
            if (d == UNKNOWN_DIALECT) {
                return null;
            }
            for (int i = 0; i < patterns.length; i++) {
                SqlPattern pattern = patterns[i];
                if (pattern.hasDialect(d)) {
                    return pattern;
                }
            }
            return null;
        }

        private final int dialect;
        private final String sql;
        private final String triggerSql;

        private SqlPattern(final int dialect,
            final String sql,
            final int startsWithLen) {
            this(dialect, sql, sql.substring(0, startsWithLen));
        }
        private SqlPattern(final int dialect,
            final String sql,
            final String triggerSql) {
            this.dialect = dialect;
            this.sql = sql;
            this.triggerSql = triggerSql;
        }

        public boolean hasDialect(int d) {
            return (dialect & d) != 0;
        }
        public String getSql() {
            return sql;
        }
        public String getTriggerSql() {
            return triggerSql;
        }
    }
}


// End TestAggregationManager.java
