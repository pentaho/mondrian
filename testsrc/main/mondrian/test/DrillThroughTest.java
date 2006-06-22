/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2003-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Feb 14, 2003
*/
package mondrian.test;

import mondrian.olap.Result;
import mondrian.rolap.RolapConnection;

/**
 * Test generation of SQL to access the fact table data underlying an MDX
 * result set.
 *
 * @author jhyde
 * @since May 10, 2006
 * @version $Id$
 */
public class DrillThroughTest extends FoodMartTestCase {
    public DrillThroughTest(String name) {
        super(name);
    }

    /**
     * Checks that expected SQL equals actual SQL.
     * Performs some normalization on the actual SQL to compensate for
     * differences between dialects.
     */
    private void assertSqlEquals(String expectedSql, String actualSql) {
        RolapConnection conn = (RolapConnection) getConnection();
        String jdbcUrl = conn.getConnectInfo().get("Jdbc");
        final String search = "fname \\+ ' ' \\+ lname";
        if (jdbcUrl.toLowerCase().indexOf("mysql") >= 0) {
            // Mysql would generate "CONCAT( ... )"
            expectedSql = expectedSql.replaceAll(
                    search,
                    "CONCAT(`customer`.`fname`, ' ', `customer`.`lname`)");
        } else if (jdbcUrl.toLowerCase().indexOf("postgresql") >= 0  ||
                jdbcUrl.toLowerCase().indexOf("oracle") >= 0) {
            expectedSql = expectedSql.replaceAll(
                    search,
                    "`fname` || ' ' || `lname`");
        } else if (jdbcUrl.toLowerCase().indexOf("derby") >= 0  ||
                jdbcUrl.toLowerCase().indexOf("cloudscape") >= 0) {
            expectedSql = expectedSql.replaceAll(
                    search,
                    "`customer`.`fullname`");
        } else if (jdbcUrl.toLowerCase().indexOf(":db2:") >= 0) {
            expectedSql = expectedSql.replaceAll(
                    search,
                    "CONCAT(CONCAT(fname, ' '), lname)");
        }

        // DB2 does not have quotes on identifiers
        if (jdbcUrl.toLowerCase().indexOf(":db2:") >= 0) {
            expectedSql = expectedSql.replaceAll("`", "");
        }

        // the following replacement is for databases in ANSI mode
        //  using '"' to quote identifiers
        actualSql = actualSql.replace('"', '`');

        if (jdbcUrl.toLowerCase().indexOf("oracle") >= 0) {
            // " + tableQualifier + "
            expectedSql = expectedSql.replaceAll(" =as= ", " ");
        } else {
            expectedSql = expectedSql.replaceAll(" =as= ", " as ");
        }

        assertEquals(expectedSql, actualSql);
    }

    // ~ Tests ================================================================

    public void testDrillThrough() {
        Result result = executeQuery(
                "WITH MEMBER [Measures].[Price] AS '[Measures].[Store Sales] / [Measures].[Unit Sales]'" + nl +
                "SELECT {[Measures].[Unit Sales], [Measures].[Price]} on columns," + nl +
                " {[Product].Children} on rows" + nl +
                "from Sales");
        String sql = result.getCell(new int[] {0, 0}).getDrillThroughSQL(false);

        String expectedSql =
                "select `time_by_day`.`the_year` as `Year`," +
                " `product_class`.`product_family` as `Product Family`," +
                " `sales_fact_1997`.`unit_sales` as `Unit Sales` " +
                "from `time_by_day` =as= `time_by_day`," +
                " `sales_fact_1997` =as= `sales_fact_1997`," +
                " `product_class` =as= `product_class`," +
                " `product` =as= `product` " +
                "where `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`" +
                " and `time_by_day`.`the_year` = 1997" +
                " and `sales_fact_1997`.`product_id` = `product`.`product_id`" +
                " and `product`.`product_class_id` = `product_class`.`product_class_id`" +
                " and `product_class`.`product_family` = 'Drink'";

        assertSqlEquals(expectedSql, sql);

        sql = result.getCell(new int[] {1, 1}).getDrillThroughSQL(false);
        assertNull(sql); // because it is a calculated member
    }

    public void testDrillThrough2() {
        Result result = executeQuery(
                "WITH MEMBER [Measures].[Price] AS '[Measures].[Store Sales] / [Measures].[Unit Sales]'" + nl +
                "SELECT {[Measures].[Unit Sales], [Measures].[Price]} on columns," + nl +
                " {[Product].Children} on rows" + nl +
                "from Sales");
        String sql = result.getCell(new int[] {0, 0}).getDrillThroughSQL(true);

        String expectedSql =
                "select `store`.`store_name` as `Store Name`," +
                " `store`.`store_city` as `Store City`," +
                " `store`.`store_state` as `Store State`," +
                " `store`.`store_country` as `Store Country`," +
                " `store`.`store_sqft` as `Store Sqft`," +
                " `store`.`store_type` as `Store Type`," +
                " `time_by_day`.`month_of_year` as `Month`," +
                " `time_by_day`.`quarter` as `Quarter`," +
                " `time_by_day`.`the_year` as `Year`," +
                " `product`.`product_name` as `Product Name`," +
                " `product`.`brand_name` as `Brand Name`," +
                " `product_class`.`product_subcategory` as `Product Subcategory`," +
                " `product_class`.`product_category` as `Product Category`," +
                " `product_class`.`product_department` as `Product Department`," +
                " `product_class`.`product_family` as `Product Family`," +
                " `promotion`.`media_type` as `Media Type`," +
                " `promotion`.`promotion_name` as `Promotion Name`," +
                " `customer`.`customer_id` as `Name (Key)`," +
                " fname + ' ' + lname as `Name`," +
                " `customer`.`city` as `City`," +
                " `customer`.`state_province` as `State Province`," +
                " `customer`.`country` as `Country`," +
                " `customer`.`education` as `Education Level`," +
                " `customer`.`gender` as `Gender`," +
                " `customer`.`marital_status` as `Marital Status`," +
                " `customer`.`yearly_income` as `Yearly Income`," +
                " `sales_fact_1997`.`unit_sales` as `Unit Sales` " +
                "from `store` =as= `store`," +
                " `sales_fact_1997` =as= `sales_fact_1997`," +
                " `time_by_day` =as= `time_by_day`," +
                " `product` =as= `product`," +
                " `product_class` =as= `product_class`," +
                " `promotion` =as= `promotion`," +
                " `customer` =as= `customer` " +
                "where `sales_fact_1997`.`store_id` = `store`.`store_id`" +
                " and `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`" +
                " and `time_by_day`.`the_year` = 1997" +
                " and `sales_fact_1997`.`product_id` = `product`.`product_id`" +
                " and `product`.`product_class_id` = `product_class`.`product_class_id`" +
                " and `product_class`.`product_family` = 'Drink'" +
                " and `sales_fact_1997`.`promotion_id` = `promotion`.`promotion_id`" +
                " and `sales_fact_1997`.`customer_id` = `customer`.`customer_id`";
        assertSqlEquals(expectedSql, sql);

        // Drillthrough SQL is null for cell based on calc member
        sql = result.getCell(new int[] {1, 1}).getDrillThroughSQL(true);
        assertNull(sql);
    }

    /**
     * Testcase for bug 1472311, "Drillthrough fails, if Aggregate in
     * MDX-query". The problem actually occurs with any calculated member,
     * not just Aggregate. The bug was causing a syntactically invalid
     * constraint to be added to the WHERE clause; after the fix, we do
     * not constrain on the member at all.
     */
    public void testDrillThroughBug1472311() {
        Result result = executeQuery(
                "with set [Date Range] as" + nl +
                "'{[Time].[1997].[Q1],[Time].[1997].[Q2]}'" + nl +
                "member [Time].[Date Range] as 'Aggregate([Date Range])'" + nl +
                "select {[Store]} on rows," + nl +
                "{[Measures].[Unit Sales]} on columns" + nl +
                "from [Sales]" + nl +
                "where [Time].[Date Range]");
        String sql = result.getCell(new int[] {0, 0}).getDrillThroughSQL(true);
        final String expectedSql = "select" +
                " `store`.`store_name` as `Store Name`," +
                " `store`.`store_city` as `Store City`," +
                " `store`.`store_state` as `Store State`," +
                " `store`.`store_country` as `Store Country`," +
                " `store`.`store_sqft` as `Store Sqft`," +
                " `store`.`store_type` as `Store Type`," +
                " `time_by_day`.`month_of_year` as `Month`," +
                " `time_by_day`.`quarter` as `Quarter`," +
                " `time_by_day`.`the_year` as `Year`," +
                " `product`.`product_name` as `Product Name`," +
                " `product`.`brand_name` as `Brand Name`," +
                " `product_class`.`product_subcategory` as `Product Subcategory`," +
                " `product_class`.`product_category` as `Product Category`," +
                " `product_class`.`product_department` as `Product Department`," +
                " `product_class`.`product_family` as `Product Family`," +
                " `promotion`.`media_type` as `Media Type`," +
                " `promotion`.`promotion_name` as `Promotion Name`," +
                " `customer`.`customer_id` as `Name (Key)`," +
                " fname + ' ' + lname as `Name`," +
                " `customer`.`city` as `City`," +
                " `customer`.`state_province` as `State Province`," +
                " `customer`.`country` as `Country`," +
                " `customer`.`education` as `Education Level`," +
                " `customer`.`gender` as `Gender`," +
                " `customer`.`marital_status` as `Marital Status`," +
                " `customer`.`yearly_income` as `Yearly Income`," +
                " `sales_fact_1997`.`unit_sales` as `Unit Sales` " +
                "from `store` =as= `store`," +
                " `sales_fact_1997` =as= `sales_fact_1997`," +
                " `time_by_day` =as= `time_by_day`," +
                " `product` =as= `product`," +
                " `product_class` =as= `product_class`," +
                " `promotion` =as= `promotion`," +
                " `customer` =as= `customer` " +
                "where `sales_fact_1997`.`store_id` = `store`.`store_id` " +
                "and `sales_fact_1997`.`time_id` = `time_by_day`.`time_id` " +
                "and `sales_fact_1997`.`product_id` = `product`.`product_id` " +
                "and `product`.`product_class_id` = `product_class`.`product_class_id` " +
                "and `sales_fact_1997`.`promotion_id` = `promotion`.`promotion_id` " +
                "and `sales_fact_1997`.`customer_id` = `customer`.`customer_id`";
        assertSqlEquals(expectedSql, sql);
    }
    
    // Test that proper SQL is being generated for a Measure specified
    // as an expression
    public void testDrillThroughMeasureExp() {
        Result result = executeQuery(
                "SELECT {[Measures].[Promotion Sales]} on columns," + nl +
                " {[Product].Children} on rows" + nl +
                "from Sales");
        String sql = result.getCell(new int[] {0, 0}).getDrillThroughSQL(false);

        String expectedSql =
                "select `time_by_day`.`the_year` as `Year`," +
                " `product_class`.`product_family` as `Product Family`," +
                " (case when `sales_fact_1997`.`promotion_id` = 0 then 0" +
                " else `sales_fact_1997`.`store_sales` end)" +
                " as `Promotion Sales` " +
                "from `time_by_day` =as= `time_by_day`," +
                " `sales_fact_1997` =as= `sales_fact_1997`," +
                " `product_class` =as= `product_class`," +
                " `product` =as= `product` " +
                "where `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`" +
                " and `time_by_day`.`the_year` = 1997" +
                " and `sales_fact_1997`.`product_id` = `product`.`product_id`" +
                " and `product`.`product_class_id` = `product_class`.`product_class_id`" +
                " and `product_class`.`product_family` = 'Drink'";

        assertSqlEquals(expectedSql, sql);
    }
}

// End DrillThroughTest.java
