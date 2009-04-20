/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2003-2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Feb 14, 2003
*/
package mondrian.test;


import mondrian.olap.*;
import mondrian.rolap.RolapCube;
import mondrian.rolap.RolapStar;
import mondrian.rolap.RolapLevel;
import mondrian.spi.Dialect;

import javax.sql.DataSource;
import java.sql.Statement;
import java.sql.ResultSet;

/**
 * Test generation of SQL to access the fact table data underlying an MDX
 * result set.
 *
 * @author jhyde
 * @since May 10, 2006
 * @version $Id$
 */
public class DrillThroughTest extends FoodMartTestCase {
    public DrillThroughTest() {
        super();
    }
    public DrillThroughTest(String name) {
        super(name);
    }

    // ~ Tests ================================================================

    public void testTrivalCalcMemberDrillThrough() throws Exception {
        Result result = executeQuery(
            "WITH MEMBER [Measures].[Formatted Unit Sales]"
            + " AS '[Measures].[Unit Sales]', FORMAT_STRING='$#,###.000'\n"
            + "MEMBER [Measures].[Twice Unit Sales]"
            + " AS '[Measures].[Unit Sales] * 2'\n"
            + "MEMBER [Measures].[Twice Unit Sales Plus Store Sales] "
            + " AS '[Measures].[Twice Unit Sales] + [Measures].[Store Sales]',"
            + "  FOMRAT_STRING='#'\n"
            + "MEMBER [Measures].[Foo] "
            + " AS '[Measures].[Unit Sales] + ([Measures].[Unit Sales], [Time].PrevMember)'\n"
            + "MEMBER [Measures].[Unit Sales Percentage] "
            + " AS '[Measures].[Unit Sales] / [Measures].[Twice Unit Sales]'\n"
            + "SELECT {[Measures].[Unit Sales],\n"
            + "  [Measures].[Formatted Unit Sales],\n"
            + "  [Measures].[Twice Unit Sales],\n"
            + "  [Measures].[Twice Unit Sales Plus Store Sales],\n"
            + "  [Measures].[Foo],\n"
            + "  [Measures].[Unit Sales Percentage]} on columns,\n"
            + " {[Product].Children} on rows\n"
            + "from Sales");

        // can drill through [Formatted Unit Sales]
        final Cell cell = result.getCell(new int[]{0, 0});
        assertTrue(cell.canDrillThrough());
        // can drill through [Unit Sales]
        assertTrue(result.getCell(new int[]{1, 0}).canDrillThrough());
        // can drill through [Twice Unit Sales]
        assertTrue(result.getCell(new int[]{2, 0}).canDrillThrough());
        // can drill through [Twice Unit Sales Plus Store Sales]
        assertTrue(result.getCell(new int[]{3, 0}).canDrillThrough());
        // can not drill through [Foo]
        assertFalse(result.getCell(new int[]{4, 0}).canDrillThrough());
        // can drill through [Unit Sales Percentage]
        assertTrue(result.getCell(new int[]{5, 0}).canDrillThrough());
        assertNotNull(
            result.getCell(
                new int[]{
                    5, 0
                }).getDrillThroughSQL(false));

        String sql = cell.getDrillThroughSQL(false);
        String expectedSql =
            "select `time_by_day`.`the_year` as `Year`,"
            + " `product_class`.`product_family` as `Product Family`,"
            + " `sales_fact_1997`.`unit_sales` as `Unit Sales` "
            + "from `time_by_day` =as= `time_by_day`,"
            + " `sales_fact_1997` =as= `sales_fact_1997`,"
            + " `product_class` =as= `product_class`,"
            + " `product` =as= `product` "
            + "where `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`"
            + " and `time_by_day`.`the_year` = 1997"
            + " and `sales_fact_1997`.`product_id` = `product`.`product_id`"
            + " and `product`.`product_class_id` = `product_class`.`product_class_id`"
            + " and `product_class`.`product_family` = 'Drink' "
            + "order by `time_by_day`.`the_year` ASC,"
            + " `product_class`.`product_family` ASC";

        getTestContext().assertSqlEquals(expectedSql, sql, 7978);

        // Can drill through a trivial calc member.
        final Cell calcCell = result.getCell(new int[]{1, 0});
        assertTrue(calcCell.canDrillThrough());
        sql = calcCell.getDrillThroughSQL(false);
        assertNotNull(sql);
        expectedSql =
            "select `time_by_day`.`the_year` as `Year`,"
            + " `product_class`.`product_family` as `Product Family`,"
            + " `sales_fact_1997`.`unit_sales` as `Unit Sales` "
            + "from `time_by_day` =as= `time_by_day`,"
            + " `sales_fact_1997` =as= `sales_fact_1997`,"
            + " `product_class` =as= `product_class`,"
            + " `product` =as= `product` "
            + "where `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`"
            + " and `time_by_day`.`the_year` = 1997"
            + " and `sales_fact_1997`.`product_id` = `product`.`product_id`"
            + " and `product`.`product_class_id` = `product_class`.`product_class_id`"
            + " and `product_class`.`product_family` = 'Drink' "
            + "order by `time_by_day`.`the_year` ASC,"
            + " `product_class`.`product_family` ASC";

        getTestContext().assertSqlEquals(expectedSql, sql, 7978);

        assertEquals(calcCell.getDrillThroughCount(), 7978);
    }


    public void testDrillThrough() throws Exception {
        Result result = executeQuery(
            "WITH MEMBER [Measures].[Price] AS '[Measures].[Store Sales] / ([Measures].[Store Sales], [Time].PrevMember)'\n"
            + "SELECT {[Measures].[Unit Sales], [Measures].[Price]} on columns,\n"
            + " {[Product].Children} on rows\n"
            + "from Sales");
        final Cell cell = result.getCell(new int[]{0, 0});
        assertTrue(cell.canDrillThrough());
        String sql = cell.getDrillThroughSQL(false);

        String expectedSql =
            "select `time_by_day`.`the_year` as `Year`,"
            + " `product_class`.`product_family` as `Product Family`,"
            + " `sales_fact_1997`.`unit_sales` as `Unit Sales` "
            + "from `time_by_day` =as= `time_by_day`,"
            + " `sales_fact_1997` =as= `sales_fact_1997`,"
            + " `product_class` =as= `product_class`,"
            + " `product` =as= `product` "
            + "where `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`"
            + " and `time_by_day`.`the_year` = 1997"
            + " and `sales_fact_1997`.`product_id` = `product`.`product_id`"
            + " and `product`.`product_class_id` = `product_class`.`product_class_id`"
            + " and `product_class`.`product_family` = 'Drink' "
            + "order by `time_by_day`.`the_year` ASC,"
            + " `product_class`.`product_family` ASC";

        getTestContext().assertSqlEquals(expectedSql, sql, 7978);

        // Cannot drill through a calc member.
        final Cell calcCell = result.getCell(new int[]{1, 1});
        assertFalse(calcCell.canDrillThrough());
        sql = calcCell.getDrillThroughSQL(false);
        assertNull(sql);
    }

    private String getNameExp(
        Result result,
        String hierName,
        String levelName)
    {
        final Cube cube = result.getQuery().getCube();
        RolapStar star = ((RolapCube) cube).getStar();

        Hierarchy h =
            cube.lookupHierarchy(
                new Id.Segment(hierName, Id.Quoting.UNQUOTED), false);
        if (h == null) {
            return null;
        }
        for (Level l : h.getLevels()) {
            if (l.getName().equals(levelName)) {
                MondrianDef.Expression exp = ((RolapLevel) l).getNameExp();
                String nameExpStr = exp.getExpression(star.getSqlQuery());
                nameExpStr = nameExpStr.replace('"', '`') ;
                return nameExpStr;
            }
        }
        return null;
    }

    public void testDrillThrough2() throws Exception {
        Result result = executeQuery(
            "WITH MEMBER [Measures].[Price] AS '[Measures].[Store Sales] / ([Measures].[Unit Sales], [Time].PrevMember)'\n"
            + "SELECT {[Measures].[Unit Sales], [Measures].[Price]} on columns,\n"
            + " {[Product].Children} on rows\n"
            + "from Sales");
        String sql = result.getCell(new int[]{0, 0}).getDrillThroughSQL(true);

        String nameExpStr = getNameExp(result, "Customers", "Name");

        String expectedSql =
            "select `store`.`store_country` as `Store Country`,"
            + " `store`.`store_state` as `Store State`,"
            + " `store`.`store_city` as `Store City`,"
            + " `store`.`store_name` as `Store Name`,"
            + " `store`.`store_sqft` as `Store Sqft`,"
            + " `store`.`store_type` as `Store Type`,"
            + " `time_by_day`.`the_year` as `Year`,"
            + " `time_by_day`.`quarter` as `Quarter`,"
            + " `time_by_day`.`month_of_year` as `Month`,"
            + " `product_class`.`product_family` as `Product Family`,"
            + " `product_class`.`product_department` as `Product Department`,"
            + " `product_class`.`product_category` as `Product Category`,"
            + " `product_class`.`product_subcategory` as `Product Subcategory`,"
            + " `product`.`brand_name` as `Brand Name`,"
            + " `product`.`product_name` as `Product Name`,"
            + " `promotion`.`media_type` as `Media Type`,"
            + " `promotion`.`promotion_name` as `Promotion Name`,"
            + " `customer`.`country` as `Country`,"
            + " `customer`.`state_province` as `State Province`,"
            + " `customer`.`city` as `City`, "
            + nameExpStr
            + " as `Name`,"
            + " `customer`.`customer_id` as `Name (Key)`,"
            + " `customer`.`education` as `Education Level`,"
            + " `customer`.`gender` as `Gender`,"
            + " `customer`.`marital_status` as `Marital Status`,"
            + " `customer`.`yearly_income` as `Yearly Income`,"
            + " `sales_fact_1997`.`unit_sales` as `Unit Sales` "
            + "from `store` =as= `store`,"
            + " `sales_fact_1997` =as= `sales_fact_1997`,"
            + " `time_by_day` =as= `time_by_day`,"
            + " `product_class` =as= `product_class`,"
            + " `product` =as= `product`,"
            + " `promotion` =as= `promotion`,"
            + " `customer` =as= `customer` "
            + "where `sales_fact_1997`.`store_id` = `store`.`store_id`"
            + " and `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`"
            + " and `time_by_day`.`the_year` = 1997"
            + " and `sales_fact_1997`.`product_id` = `product`.`product_id`"
            + " and `product`.`product_class_id` = `product_class`.`product_class_id`"
            + " and `product_class`.`product_family` = 'Drink'"
            + " and `sales_fact_1997`.`promotion_id` = `promotion`.`promotion_id`"
            + " and `sales_fact_1997`.`customer_id` = `customer`.`customer_id` "
            + "order by `store`.`store_country` ASC,"
            + " `store`.`store_state` ASC,"
            + " `store`.`store_city` ASC,"
            + " `store`.`store_name` ASC,"
            + " `store`.`store_sqft` ASC,"
            + " `store`.`store_type` ASC,"
            + " `time_by_day`.`the_year` ASC,"
            + " `time_by_day`.`quarter` ASC,"
            + " `time_by_day`.`month_of_year` ASC,"
            + " `product_class`.`product_family` ASC,"
            + " `product_class`.`product_department` ASC,"
            + " `product_class`.`product_category` ASC,"
            + " `product_class`.`product_subcategory` ASC,"
            + " `product`.`brand_name` ASC,"
            + " `product`.`product_name` ASC,"
            + " `promotion`.`media_type` ASC,"
            + " `promotion`.`promotion_name` ASC,"
            + " `customer`.`country` ASC,"
            + " `customer`.`state_province` ASC,"
            + " `customer`.`city` ASC, "
            + nameExpStr
            + " ASC,"
            + " `customer`.`customer_id` ASC,"
            + " `customer`.`education` ASC,"
            + " `customer`.`gender` ASC,"
            + " `customer`.`marital_status` ASC,"
            + " `customer`.`yearly_income` ASC";

        getTestContext().assertSqlEquals(expectedSql, sql, 7978);

        // Drillthrough SQL is null for cell based on calc member
        sql = result.getCell(new int[]{1, 1}).getDrillThroughSQL(true);
        assertNull(sql);
    }

    public void testDrillThrough3() throws Exception {
        Result result = executeQuery(
            "select {[Measures].[Unit Sales], [Measures].[Store Cost], [Measures].[Store Sales]} ON COLUMNS, \n"
            + "Hierarchize(Union(Union(Crossjoin({[Promotion Media].[All Media]}, {[Product].[All Products]}), \n"
            + "Crossjoin({[Promotion Media].[All Media]}, [Product].[All Products].Children)), Crossjoin({[Promotion Media].[All Media]}, [Product].[All Products].[Drink].Children))) ON ROWS \n"
            + "from [Sales] where [Time].[1997].[Q4].[12]");

        // [Promotion Media].[All Media], [Product].[All Products].[Drink].[Dairy], [Measures].[Store Cost]
        Cell cell = result.getCell(new int[]{0, 4});

        String sql = cell.getDrillThroughSQL(true);

        String nameExpStr = getNameExp(result, "Customers", "Name");

        String expectedSql =
            "select "
            + "`store`.`store_country` as `Store Country`, `store`.`store_state` as `Store State`, `store`.`store_city` as `Store City`, `store`.`store_name` as `Store Name`, "
            + "`store`.`store_sqft` as `Store Sqft`, `store`.`store_type` as `Store Type`, "
            + "`time_by_day`.`the_year` as `Year`, `time_by_day`.`quarter` as `Quarter`, `time_by_day`.`month_of_year` as `Month`, "
            + "`product_class`.`product_family` as `Product Family`, `product_class`.`product_department` as `Product Department`, "
            + "`product_class`.`product_category` as `Product Category`, `product_class`.`product_subcategory` as `Product Subcategory`, "
            + "`product`.`brand_name` as `Brand Name`, `product`.`product_name` as `Product Name`, "
            + "`promotion`.`media_type` as `Media Type`, `promotion`.`promotion_name` as `Promotion Name`, "
            + "`customer`.`country` as `Country`, `customer`.`state_province` as `State Province`, `customer`.`city` as `City`, "
            + nameExpStr + " as `Name`, `customer`.`customer_id` as `Name (Key)`, "
            + "`customer`.`education` as `Education Level`, `customer`.`gender` as `Gender`, `customer`.`marital_status` as `Marital Status`, "
            + "`customer`.`yearly_income` as `Yearly Income`, "
            + "`sales_fact_1997`.`unit_sales` as `Unit Sales` "
            + "from `store =as= `store`, "
            + "`sales_fact_1997` =as= `sales_fact_1997`, "
            + "`time_by_day` =as= `time_by_day`, "
            + "`product_class` =as= `product_class`, "
            + "`product` =as= `product`, "
            + "`promotion` =as= `promotion`, "
            + "`customer` =as= `customer` "
            + "where `sales_fact_1997`.`store_id` = `store`.`store_id` and "
            + "`sales_fact_1997`.`time_id` = `time_by_day`.`time_id` and "
            + "`time_by_day`.`the_year` = 1997 and "
            + "`time_by_day`.`quarter` = 'Q4' and "
            + "`time_by_day`.`month_of_year` = 12 and "
            + "`sales_fact_1997`.`product_id` = `product`.`product_id` and "
            + "`product`.`product_class_id` = `product_class`.`product_class_id` and "
            + "`product_class`.`product_family` = 'Drink' and "
            + "`product_class`.`product_department` = 'Dairy' and "
            + "`sales_fact_1997`.`promotion_id` = `promotion`.`promotion_id` and "
            + "`sales_fact_1997`.`customer_id` = `customer`.`customer_id` "
            + "order by `store`.`store_country` ASC, `store`.`store_state` ASC, `store`.`store_city` ASC, `store`.`store_name` ASC, `store`.`store_sqft` ASC, "
            + "`store`.`store_type` ASC, `time_by_day`.`the_year` ASC, `time_by_day`.`quarter` ASC, `time_by_day`.`month_of_year` ASC, "
            + "`product_class`.`product_family` ASC, `product_class`.`product_department` ASC, `product_class`.`product_category` ASC, "
            + "`product_class`.`product_subcategory` ASC, `product`.`brand_name` ASC, `product`.`product_name` ASC, "
            + "`promotion.media_type` ASC, `promotion`.`promotion_name` ASC, "
            + "`customer`.`country` ASC, `customer`.`state_province` ASC, `customer`.`city` ASC, " + nameExpStr + " ASC, "
            + "`customer`.`customer_id` ASC, `customer`.`education` ASC, `customer`.gender` ASC, `customer`.`marital_status` ASC, `customer`.`yearly_income` ASC";

        getTestContext().assertSqlEquals(expectedSql, sql, 141);
    }

    /**
     * Testcase for bug 1472311, "Drillthrough fails, if Aggregate in
     * MDX-query". The problem actually occurs with any calculated member,
     * not just Aggregate. The bug was causing a syntactically invalid
     * constraint to be added to the WHERE clause; after the fix, we do
     * not constrain on the member at all.
     */
    public void testDrillThroughBug1472311() throws Exception {
        /*
                "with set [Date Range] as\n"
                + "'{[Time].[1997].[Q1],[Time].[1997].[Q2]}'\n"
                + "member [Time].[Date Range] as 'Aggregate([Date Range])'\n"
                + "select {[Store]} on rows,\n"
                + "{[Measures].[Unit Sales]} on columns\n"
                + "from [Sales]\n"
                + "where [Time].[Date Range]");

         */
        Result result = executeQuery(
            "with set [Date Range] as '{[Time].[1997].[Q1], [Time].[1997].[Q2]}'\n"
            + "member [Time].[Date Range] as 'Aggregate([Date Range])'\n"
            + "select {[Measures].[Unit Sales]} ON COLUMNS,\n"
            + "Hierarchize(Union(Union(Union({[Store].[All Stores]}, [Store].[All Stores].Children), [Store].[All Stores].[USA].Children), [Store].[All Stores].[USA].[CA].Children)) ON ROWS\n"
            + "from [Sales]\n"
            + "where [Time].[Date Range]");

        //String sql = result.getCell(new int[] {0, 0}).getDrillThroughSQL(true);
        String sql = result.getCell(new int[] {0, 6}).getDrillThroughSQL(true);

        String nameExpStr = getNameExp(result, "Customers", "Name");

        final String expectedSql =
            "select"
            //`+ store`.`store_country` as `Store Country`,"
            + " `store`.`store_state` as `Store State`,"
            + " `store`.`store_city` as `Store City`,"
            + " `store`.`store_name` as `Store Name`,"
            + " `store`.`store_sqft` as `Store Sqft`,"
            + " `store`.`store_type` as `Store Type`,"
            + " `time_by_day`.`the_year` as `Year`,"
            + " `time_by_day`.`quarter` as `Quarter`,"
            + " `time_by_day`.`month_of_year` as `Month`,"
            + " `product_class`.`product_family` as `Product Family`,"
            + " `product_class`.`product_department` as `Product Department`,"
            + " `product_class`.`product_category` as `Product Category`,"
            + " `product_class`.`product_subcategory` as `Product Subcategory`,"
            + " `product`.`brand_name` as `Brand Name`,"
            + " `product`.`product_name` as `Product Name`,"
            + " `promotion`.`media_type` as `Media Type`,"
            + " `promotion`.`promotion_name` as `Promotion Name`,"
            + " `customer`.`country` as `Country`,"
            + " `customer`.`state_province` as `State Province`,"
            + " `customer`.`city` as `City`, "
            + nameExpStr
            + " as `Name`,"
            + " `customer`.`customer_id` as `Name (Key)`,"
            + " `customer`.`education` as `Education Level`,"
            + " `customer`.`gender` as `Gender`,"
            + " `customer`.`marital_status` as `Marital Status`,"
            + " `customer`.`yearly_income` as `Yearly Income`,"
            + " `sales_fact_1997`.`unit_sales` as `Unit Sales` "
            + "from `store` =as= `store`,"
            + " `sales_fact_1997` =as= `sales_fact_1997`,"
            + " `time_by_day` =as= `time_by_day`,"
            + " `product_class` =as= `product_class`,"
            + " `product` =as= `product`,"
            + " `promotion` =as= `promotion`,"
            + " `customer` =as= `customer` "
            + "where `sales_fact_1997`.`store_id` = `store`.`store_id` and"
            + " `store`.`store_state` = 'CA' and"
            + " `store`.`store_city` = 'Beverly Hills' and"
            + " `sales_fact_1997`.time_id` = `time_by_day`.`time_id` and"
            + " `sales_fact_1997`.`product_id` = `product`.`product_id` and"
            + " `product`.`product_class_id` = `product_class`.`product_class_id` and"
            + " `sales_fact_1997`.`promotion_id` = `promotion`.`promotion_id` and"
            + " `sales_fact_1997`.`customer_id` = `customer`.`customer_id`"
            + " order by"
            // + `store`.`store_country` ASC,"
            + " `store`.`store_state` ASC,"
            + " `store`.`store_city` ASC,"
            + " `store`.`store_name` ASC,"
            + " `store`.`store_sqft` ASC,"
            + " `store`.`store_type` ASC,"
            + " `time_by_day`.`the_year` ASC,"
            + " `time_by_day`.`quarter` ASC,"
            + " `time_by_day`.`month_of_year` ASC,"
            + " `product_class`.`product_family` ASC,"
            + " `product_class`.`product_department` ASC,"
            + " `product_class`.`product_category` ASC,"
            + " `product_class`.`product_subcategory` ASC,"
            + " `product`.`brand_name` ASC,"
            + " `product`.`product_name` ASC,"
            + " `promotion`.`media_type` ASC,"
            + " `promotion`.`promotion_name` ASC,"
            + " `customer`.`country` ASC,"
            + " `customer`.`state_province` ASC,"
            + " `customer`.`city` ASC, "
            + nameExpStr
            + " ASC,"
            + " `customer`.`customer_id` ASC,"
            + " `customer`.`education` ASC,"
            + " `customer`.`gender` ASC,"
            + " `customer`.`marital_status` ASC,"
            + " `customer`.`yearly_income` ASC";

        //getTestContext().assertSqlEquals(expectedSql, sql, 86837);
        getTestContext().assertSqlEquals(expectedSql, sql, 6815);
    }

    // Test that proper SQL is being generated for a Measure specified
    // as an expression
    public void testDrillThroughMeasureExp() throws Exception {
        Result result = executeQuery(
            "SELECT {[Measures].[Promotion Sales]} on columns,\n"
            + " {[Product].Children} on rows\n"
            + "from Sales");
        String sql = result.getCell(new int[] {0, 0}).getDrillThroughSQL(false);

        String expectedSql =
            "select `time_by_day`.`the_year` as `Year`,"
            + " `product_class`.`product_family` as `Product Family`,"
            + " (case when `sales_fact_1997`.`promotion_id` = 0 then 0"
            + " else `sales_fact_1997`.`store_sales` end)"
            + " as `Promotion Sales` "
            + "from `time_by_day` =as= `time_by_day`,"
            + " `sales_fact_1997` =as= `sales_fact_1997`,"
            + " `product_class` =as= `product_class`,"
            + " `product` =as= `product` "
            + "where `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`"
            + " and `time_by_day`.`the_year` = 1997"
            + " and `sales_fact_1997`.`product_id` = `product`.`product_id`"
            + " and `product`.`product_class_id` = `product_class`.`product_class_id`"
            + " and `product_class`.`product_family` = 'Drink' "
            + "order by `time_by_day`.`the_year` ASC, `product_class`.`product_family` ASC";

        final Cube cube = result.getQuery().getCube();
        RolapStar star = ((RolapCube) cube).getStar();

        // Adjust expected SQL for dialect differences in FoodMart.xml.
        Dialect dialect = star.getSqlQueryDialect();
        final String caseStmt =
            " \\(case when `sales_fact_1997`.`promotion_id` = 0 then 0"
            + " else `sales_fact_1997`.`store_sales` end\\)";
        switch (dialect.getDatabaseProduct()) {
        case ACCESS:
            expectedSql = expectedSql.replaceAll(
                caseStmt,
                " Iif(`sales_fact_1997`.`promotion_id` = 0, 0,"
                + " `sales_fact_1997`.`store_sales`)");
            break;
        case INFOBRIGHT:
            expectedSql = expectedSql.replaceAll(
                caseStmt, " `sales_fact_1997`.`store_sales`");
            break;
        }

        getTestContext().assertSqlEquals(expectedSql, sql, 7978);
    }

    /**
     * Tests that drill-through works if two dimension tables have primary key
     * columns with the same name. Related to bug 1592556, "XMLA Drill through
     * bug".
     */
    public void testDrillThroughDupKeys() throws Exception {
        /*
         * Note here that the type on the Store Id level is Integer or Numeric. The default, of course, would be String.
         *
         * For DB2 and Derby, we need the Integer type, otherwise the generated SQL will be something like:
         *
         *      `store_ragged`.`store_id` = '19'
         *
         *  and DB2 and Derby don't like converting from CHAR to INTEGER
         */
        final TestContext testContext = TestContext.createSubstitutingCube(
            "Sales",
            "  <Dimension name=\"Store2\" foreignKey=\"store_id\">\n"
            + "    <Hierarchy hasAll=\"true\" primaryKey=\"store_id\">\n"
            + "      <Table name=\"store_ragged\"/>\n"
            + "      <Level name=\"Store Country\" column=\"store_country\" uniqueMembers=\"true\"/>\n"
            + "      <Level name=\"Store Id\" column=\"store_id\" captionColumn=\"store_name\" uniqueMembers=\"true\" type=\"Integer\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Dimension name=\"Store3\" foreignKey=\"store_id\">\n"
            + "    <Hierarchy hasAll=\"true\" primaryKey=\"store_id\">\n"
            + "      <Table name=\"store\"/>\n"
            + "      <Level name=\"Store Country\" column=\"store_country\" uniqueMembers=\"true\"/>\n"
            + "      <Level name=\"Store Id\" column=\"store_id\" captionColumn=\"store_name\" uniqueMembers=\"true\" type=\"Numeric\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n");
        Result result = testContext.executeQuery(
            "SELECT {[Store2].[Store Id].Members} on columns,\n"
            + " NON EMPTY([Store3].[Store Id].Members) on rows\n"
            + "from Sales");
        String sql = result.getCell(new int[] {0, 0}).getDrillThroughSQL(false);

        String expectedSql =
            "select `time_by_day`.`the_year` as `Year`,"
            + " `store_ragged`.`store_id` as `Store Id`,"
            + " `store`.`store_id` as `Store Id_0`,"
            + " `sales_fact_1997`.`unit_sales` as `Unit Sales` "
            + "from `time_by_day` =as= `time_by_day`,"
            + " `sales_fact_1997` =as= `sales_fact_1997`,"
            + " `store_ragged` =as= `store_ragged`,"
            + " `store` =as= `store` "
            + "where `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`"
            + " and `time_by_day`.`the_year` = 1997"
            + " and `sales_fact_1997`.`store_id` = `store_ragged`.`store_id`"
            + " and `store_ragged`.`store_id` = 19"
            + " and `sales_fact_1997`.`store_id` = `store`.`store_id`"
            + " and `store`.`store_id` = 2 "
            + "order by `time_by_day`.`the_year` ASC, `store_ragged`.`store_id` ASC, `store`.`store_id` ASC";

        getTestContext().assertSqlEquals(expectedSql, sql, 0);
    }

    /**
     * Tests that cells in a virtual cube say they can be drilled through.
     */
    public void testDrillThroughVirtualCube() throws Exception {
        Result result = executeQuery(
            "select Crossjoin([Customers].[All Customers].[USA].[OR].Children, {[Measures].[Unit Sales]}) ON COLUMNS, "
            + " [Gender].[All Gender].Children ON ROWS"
            + " from [Warehouse and Sales]"
            + " where [Time].[1997].[Q4].[12]");

        String sql = result.getCell(new int[]{0, 0}).getDrillThroughSQL(false);

        String expectedSql =
            "select `time_by_day`.`the_year` as `Year`,"
            + " `time_by_day`.`quarter` as `Quarter`,"
            + " `time_by_day`.month_of_year` as `Month`,"
            + " `customer`.`state_province` as `State Province`,"
            + " `customer`.`city` as `City`,"
            + " `customer`.`gender` as `Gender`,"
            + " `sales_fact_1997`.`unit_sales` as `Unit Sales`"
            + " from `time_by_day` =as= `time_by_day`,"
            + " `sales_fact_1997` =as= `sales_fact_1997`,"
            + " `customer` =as= `customer`"
            + " where `sales_fact_1997`.`time_id` = `time_by_day`.`time_id` and"
            + " `time_by_day`.`the_year` = 1997 and"
            + " `time_by_day`.`quarter` = 'Q4' and"
            + " `time_by_day`.`month_of_year` = 12 and"
            + " `sales_fact_1997`.`customer_id` = `customer`.customer_id` and"
            + " `customer`.`state_province` = 'OR' and"
            + " `customer`.`city` = 'Albany' and"
            + " `customer`.`gender` = 'F'"
            + " order by `time_by_day`.`the_year` ASC,"
            + " `time_by_day`.`quarter` ASC,"
            + " `time_by_day`.`month_of_year` ASC,"
            + " `customer`.`state_province` ASC,"
            + " `customer`.`city` ASC,"
            + " `customer`.`gender` ASC";

        getTestContext().assertSqlEquals(expectedSql, sql, 73);
    }

    /**
     * This tests for bug 1438285, "nameColumn cannot be column in level
     * definition".
     */
    public void testBug1438285() throws Exception {
        final Dialect dialect = getTestContext().getDialect();
        if (dialect.getDatabaseProduct() == Dialect.DatabaseProduct.TERADATA) {
            // On default Teradata express instance there isn't enough spool
            // space to run this query.
            return;
        }

        // Specify the column and nameColumn to be the same
        // in order to reproduce the problem
        final TestContext testContext =
            TestContext.createSubstitutingCube(
                "Sales",
                "  <Dimension name=\"Store2\" foreignKey=\"store_id\">\n"
                + "    <Hierarchy hasAll=\"true\" allMemberName=\"All Stores\" >\n"
                + "      <Table name=\"store_ragged\"/>\n"
                + "      <Level name=\"Store Id\" column=\"store_id\" nameColumn=\"store_id\" ordinalColumn=\"region_id\" uniqueMembers=\"true\">\n"
                + "     </Level>"
                + "    </Hierarchy>\n"
                + "  </Dimension>\n");

        Result result = testContext.executeQuery(
            "SELECT {[Measures].[Unit Sales]} on columns, "
            + "{[Store2].members} on rows FROM [Sales]");

        // Prior to fix the request for the drill through SQL would result in
        // an assertion error
        String sql = result.getCell(new int[] {0, 0}).getDrillThroughSQL(true);
        String nameExpStr = getNameExp(result, "Customers", "Name");
        String expectedSql =
            "select `store`.`store_country` as `Store Country`,"
            + " `store`.`store_state` as `Store State`,"
            + " `store`.`store_city` as `Store City`, `store`.`store_name` as `Store Name`,"
            + " `store`.`store_sqft` as `Store Sqft`, `store`.`store_type` as `Store Type`,"
            + " `time_by_day`.`the_year` as `Year`, `time_by_day`.`quarter` as `Quarter`,"
            + " `time_by_day`.`month_of_year` as `Month`,"
            + " `product_class`.`product_family` as `Product Family`,"
            + " `product_class`.`product_department` as `Product Department`,"
            + " `product_class`.`product_category` as `Product Category`,"
            + " `product_class`.`product_subcategory` as `Product Subcategory`,"
            + " `product`.`brand_name` as `Brand Name`,"
            + " `product`.`product_name` as `Product Name`, `store_ragged`.`store_id` as `Store Id`,"
            + " `store_ragged`.`store_id` as `Store Id (Key)`, `promotion`.`media_type` as `Media Type`,"
            + " `promotion`.`promotion_name` as `Promotion Name`, `customer`.`country` as `Country`,"
            + " `customer`.`state_province` as `State Province`, `customer`.`city` as `City`,"
            + " " + nameExpStr + " as `Name`, `customer`.`customer_id` as `Name (Key)`,"
            + " `customer`.`education` as `Education Level`, `customer`.`gender` as `Gender`,"
            + " `customer`.`marital_status` as `Marital Status`,"
            + " `customer`.`yearly_income` as `Yearly Income`,"
            + " `sales_fact_1997`.`unit_sales` as `Unit Sales`"
            + " from `store` =as= `store`, `sales_fact_1997` =as= `sales_fact_1997`,"
            + " `time_by_day` =as= `time_by_day`, `product_class` =as= `product_class`,"
            + " `product` =as= `product`, `store_ragged` =as= `store_ragged`,"
            + " `promotion` =as= `promotion`, `customer` =as= `customer`"
            + " where `sales_fact_1997`.`store_id` = `store`.`store_id`"
            + " and `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`"
            + " and `time_by_day`.`the_year` = 1997"
            + " and `sales_fact_1997`.`product_id` = `product`.`product_id`"
            + " and `product`.`product_class_id` = `product_class`.`product_class_id`"
            + " and `sales_fact_1997`.`store_id` = `store_ragged`.`store_id`"
            + " and `sales_fact_1997`.`promotion_id` = `promotion`.`promotion_id`"
            + " and `sales_fact_1997`.`customer_id` = `customer`.`customer_id`"
            + " order by `store`.`store_country` ASC, `store`.`store_state` ASC,"
            + " `store`.`store_city` ASC, `store`.`store_name` ASC, `store`.`store_sqft` ASC,"
            + " `store`.`store_type` ASC, `time_by_day`.`the_year` ASC,"
            + " `time_by_day`.`quarter` ASC, `time_by_day`.`month_of_year` ASC,"
            + " `product_class`.`product_family` ASC, `product_class`.`product_department` ASC,"
            + " `product_class`.`product_category` ASC,"
            + " `product_class`.`product_subcategory` ASC, `product`.`brand_name` ASC,"
            + " `product`.`product_name` ASC, `store_ragged`.`store_id` ASC,"
            + " `promotion`.`media_type` ASC, `promotion`.`promotion_name` ASC,"
            + " `customer`.`country` ASC, `customer`.`state_province` ASC,"
            + " `customer`.`city` ASC, " + nameExpStr + " ASC,"
            + " `customer`.`customer_id` ASC, `customer`.`education` ASC,"
            + " `customer`.`gender` ASC, `customer`.`marital_status` ASC,"
            + " `customer`.`yearly_income` ASC";

        getTestContext().assertSqlEquals(expectedSql, sql, 86837);
    }

    /**
     * Tests that long levels do not result in column aliases larger than the
     * database can handle. For example, Access allows maximum of 64; Oracle
     * allows 30.
     *
     * <p>Testcase for bug 1893959, "Generated drill-through columns too long
     * for DBMS".
     *
     * @throws Exception on error
     */
    public void testTruncateLevelName() throws Exception {
        final TestContext testContext = TestContext.createSubstitutingCube(
            "Sales",
            "  <Dimension name=\"Education Level2\" foreignKey=\"customer_id\">\n"
            + "    <Hierarchy hasAll=\"true\" primaryKey=\"customer_id\">\n"
            + "      <Table name=\"customer\"/>\n"
            + "      <Level name=\"Education Level but with a very long name that will be too long if converted directly into a column\" column=\"education\" uniqueMembers=\"true\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>",
            null);

        Result result = testContext.executeQuery(
            "SELECT {[Measures].[Unit Sales]} on columns,\n"
            + "{[Education Level2].Children} on rows\n"
            + "FROM [Sales]\n"
            + "WHERE ([Time].[1997].[Q1].[1], [Product].[Non-Consumable].[Carousel].[Specialty].[Sunglasses].[ADJ].[ADJ Rosy Sunglasses]) ");

        String sql = result.getCell(new int[] {0, 0}).getDrillThroughSQL(false);

        // Check that SQL is valid.
        java.sql.Connection connection = null;
        try {
            DataSource dataSource = getConnection().getDataSource();
            connection = dataSource.getConnection();
            final Statement statement = connection.createStatement();
            final ResultSet resultSet = statement.executeQuery(sql);
            final int columnCount = resultSet.getMetaData().getColumnCount();
            final Dialect dialect = testContext.getDialect();
            if (dialect.getDatabaseProduct() == Dialect.DatabaseProduct.DERBY) {
                // derby counts ORDER BY columns as columns. insane!
                assertEquals(11, columnCount);
            } else {
                assertEquals(6, columnCount);
            }
            final String columnName = resultSet.getMetaData().getColumnLabel(5);
            assertTrue(
                columnName,
                columnName.startsWith("Education Level but with a"));
            int n = 0;
            while (resultSet.next()) {
                ++n;
            }
            assertEquals(2, n);
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    public void testDrillThroughExprs() {
        assertCanDrillThrough(
            true,
            "Sales",
            "CoalesceEmpty([Measures].[Unit Sales], [Measures].[Store Sales])");
        assertCanDrillThrough(
            true,
            "Sales",
            "[Measures].[Unit Sales] + [Measures].[Unit Sales]");
        assertCanDrillThrough(
            true,
            "Sales",
            "[Measures].[Unit Sales] / ([Measures].[Unit Sales] - 5.0)");
        assertCanDrillThrough(
            true,
            "Sales",
            "[Measures].[Unit Sales] * [Measures].[Unit Sales]");
        // constants are drillable - in a virtual cube it means take the first
        // cube
        assertCanDrillThrough(
            true,
            "Warehouse and Sales",
            "2.0");
        assertCanDrillThrough(
            true,
            "Warehouse and Sales",
            "[Measures].[Unit Sales] * 2.0");
        // in virtual cube, mixture of measures from two cubes is not drillable
        assertCanDrillThrough(
            false,
            "Warehouse and Sales",
            "[Measures].[Unit Sales] + [Measures].[Units Ordered]");
        // expr with measures both from [Sales] is drillable
        assertCanDrillThrough(
            true,
            "Warehouse and Sales",
            "[Measures].[Unit Sales] + [Measures].[Store Sales]");
        // expr with measures both from [Warehouse] is drillable
        assertCanDrillThrough(
            true,
            "Warehouse and Sales",
            "[Measures].[Warehouse Cost] + [Measures].[Units Ordered]");
        // <Member>.Children not drillable
        assertCanDrillThrough(
            false,
            "Sales",
            "Sum([Product].Children)");
        // Sets of members not drillable
        assertCanDrillThrough(
            false,
            "Sales",
            "Sum({[Store].[USA], [Store].[Canada].[BC]})");
        // Tuples not drillable
        assertCanDrillThrough(
            false,
            "Sales",
            "([Time].[1997].[Q1], [Measures].[Unit Sales])");
    }

    /**
     * Asserts that a cell based on the given measure expression has a given
     * drillability.
     *
     * @param canDrillThrough Whether we expect the cell to be drillable
     * @param cubeName Cube name
     * @param expr Scalar expression
     */
    private void assertCanDrillThrough(
        boolean canDrillThrough,
        String cubeName,
        String expr)
    {
        Result result = executeQuery(
            "WITH MEMBER [Measures].[Foo] AS '" + expr + "'\n"
            + "SELECT {[Measures].[Foo]} on columns,\n"
            + " {[Product].Children} on rows\n"
            + "from [" + cubeName + "]");
        final Cell cell = result.getCell(new int[] {0, 0});
        assertEquals(canDrillThrough, cell.canDrillThrough());
        final String sql = cell.getDrillThroughSQL(false);
        if (canDrillThrough) {
            assertNotNull(sql);
        } else {
            assertNull(sql);
        }
    }
}

// End DrillThroughTest.java
