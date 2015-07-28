/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2003-2005 Julian Hyde
// Copyright (C) 2005-2015 Pentaho
// All Rights Reserved.
//
// jhyde, Feb 14, 2003
*/
package mondrian.test;

import mondrian.olap.*;
import mondrian.rolap.*;
import mondrian.spi.Dialect;

import java.sql.*;
import javax.sql.DataSource;


/**
 * Test generation of SQL to access the fact table data underlying an MDX
 * result set.
 *
 * @author jhyde
 * @since May 10, 2006
 */
public class DrillThroughTest extends FoodMartTestCase {
    public DrillThroughTest() {
        super();
    }
    public DrillThroughTest(String name) {
        super(name);
    }

    // ~ Tests ================================================================

    public void testTrivialCalcMemberDrillThrough() {
        Result result = executeQuery(
            "WITH MEMBER [Measures].[Formatted Unit Sales]"
            + " AS '[Measures].[Unit Sales]', FORMAT_STRING='$#,###.000'\n"
            + "MEMBER [Measures].[Twice Unit Sales]"
            + " AS '[Measures].[Unit Sales] * 2'\n"
            + "MEMBER [Measures].[Twice Unit Sales Plus Store Sales] "
            + " AS '[Measures].[Twice Unit Sales] + [Measures].[Store Sales]',"
            + "  FOMRAT_STRING='#'\n"
            + "MEMBER [Measures].[Foo] "
            + " AS '[Measures].[Unit Sales] + ([Measures].[Unit Sales], [Time].[Time].PrevMember)'\n"
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

    public void testTrivialCalcMemberNotMeasure() {
        // [Product].[My Food] is trivial because it maps to a single member.
        // First, on ROWS axis.
        Result result = executeQuery(
            "with member [Product].[My Food]\n"
            + " AS [Product].[Food], FORMAT_STRING = '#,###'\n"
            + "SELECT [Measures].[Unit Sales] * [Gender].[M] on 0,\n"
            + " [Marital Status].[S] * [Product].[My Food] on 1\n"
            + "from [Sales]");
        Cell cell = result.getCell(new int[] {0, 0});
        assertTrue(cell.canDrillThrough());
        assertEquals(16129, cell.getDrillThroughCount());

        // Next, on filter axis.
        result = executeQuery(
            "with member [Product].[My Food]\n"
            + " AS [Product].[Food], FORMAT_STRING = '#,###'\n"
            + "SELECT [Measures].[Unit Sales] * [Gender].[M] on 0,\n"
            + " [Marital Status].[S] on 1\n"
            + "from [Sales]\n"
            + "where [Product].[My Food]");
        cell = result.getCell(new int[] {0, 0});
        assertTrue(cell.canDrillThrough());
        assertEquals(16129, cell.getDrillThroughCount());

        // Trivial member with Aggregate.
        result = executeQuery(
            "with member [Product].[My Food]\n"
            + " AS Aggregate({[Product].[Food]}), FORMAT_STRING = '#,###'\n"
            + "SELECT [Measures].[Unit Sales] * [Gender].[M] on 0,\n"
            + " [Marital Status].[S] * [Product].[My Food] on 1\n"
            + "from [Sales]");
        cell = result.getCell(new int[] {0, 0});
        assertTrue(cell.canDrillThrough());
        assertEquals(16129, cell.getDrillThroughCount());

        // Non-trivial member on rows.
        result = executeQuery(
            "with member [Product].[My Food Drink]\n"
            + " AS Aggregate({[Product].[Food], [Product].[Drink]}),\n"
            + "       FORMAT_STRING = '#,###'\n"
            + "SELECT [Measures].[Unit Sales] * [Gender].[M] on 0,\n"
            + " [Marital Status].[S] * [Product].[My Food Drink] on 1\n"
            + "from [Sales]");
        cell = result.getCell(new int[] {0, 0});
        assertFalse(cell.canDrillThrough());

        // drop the constraint when we drill through
        assertEquals(22479, cell.getDrillThroughCount());

        // Non-trivial member on filter axis.
        result = executeQuery(
            "with member [Product].[My Food Drink]\n"
            + " AS Aggregate({[Product].[Food], [Product].[Drink]}),\n"
            + "       FORMAT_STRING = '#,###'\n"
            + "SELECT [Measures].[Unit Sales] * [Gender].[M] on 0,\n"
            + " [Marital Status].[S] on 1\n"
            + "from [Sales]\n"
            + "where [Product].[My Food Drink]");
        cell = result.getCell(new int[] {0, 0});
        assertFalse(cell.canDrillThrough());
    }

    public void testDrillthroughCompoundSlicer() {
        // Tests a case associated with
        // http://jira.pentaho.com/browse/MONDRIAN-1587
        // hsqldb was failing with SQL that included redundant parentheses
        // around IN list items.

        propSaver.set(propSaver.properties.GenerateFormattedSql, true);
        Result result = executeQuery(
            "select from sales where "
            + "{[Promotion Media].[Bulk Mail],[Promotion Media].[Cash Register Handout]}");
        final Cell cell = result.getCell(new int[]{});
        assertTrue(cell.canDrillThrough());
        assertEquals(3584, cell.getDrillThroughCount());
        getTestContext().assertSqlEquals(
            "select\n"
            + "    time_by_day.the_year as Year,\n"
            + "    promotion.media_type as Media Type,\n"
            + "    sales_fact_1997.unit_sales as Unit Sales\n"
            + "from\n"
            + "    time_by_day =as= time_by_day,\n"
            + "    sales_fact_1997 =as= sales_fact_1997,\n"
            + "    promotion =as= promotion\n"
            + "where\n"
            + "    sales_fact_1997.time_id = time_by_day.time_id\n"
            + "and\n"
            + "    time_by_day.the_year = 1997\n"
            + "and\n"
            + "    sales_fact_1997.promotion_id = promotion.promotion_id\n"
            + "and\n"
            + "    ((promotion.media_type in "
            + "('Bulk Mail', 'Cash Register Handout')))\n"
            + "order by\n"
            + "    time_by_day.the_year ASC",
            cell.getDrillThroughSQL(false), 3584);
    }

    public void testDrillThrough() {
        Result result = executeQuery(
            "WITH MEMBER [Measures].[Price] AS '[Measures].[Store Sales] / ([Measures].[Store Sales], [Time].[Time].PrevMember)'\n"
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
                new Id.NameSegment(hierName, Id.Quoting.UNQUOTED), false);
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

    public void testDrillThrough2() {
        Result result = executeQuery(
            "WITH MEMBER [Measures].[Price] AS '[Measures].[Store Sales] / ([Measures].[Unit Sales], [Time].[Time].PrevMember)'\n"
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
            + " `time_by_day`.`week_of_year` as `Week`,"
            + " `time_by_day`.`day_of_month` as `Day`,"
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
            + " `time_by_day`.`week_of_year` ASC,"
            + " `time_by_day`.`day_of_month` ASC,"
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

    public void testDrillThrough3() {
        Result result = executeQuery(
            "select {[Measures].[Unit Sales], [Measures].[Store Cost], [Measures].[Store Sales]} ON COLUMNS, \n"
            + "Hierarchize(Union(Union(Crossjoin({[Promotion Media].[All Media]}, {[Product].[All Products]}), \n"
            + "Crossjoin({[Promotion Media].[All Media]}, [Product].[All Products].Children)), Crossjoin({[Promotion Media].[All Media]}, [Product].[All Products].[Drink].Children))) ON ROWS \n"
            + "from [Sales] where [Time].[1997].[Q4].[12]");

        // [Promotion Media].[All Media], [Product].[All
        // Products].[Drink].[Dairy], [Measures].[Store Cost]
        Cell cell = result.getCell(new int[]{0, 4});

        String sql = cell.getDrillThroughSQL(true);

        String nameExpStr = getNameExp(result, "Customers", "Name");

        String expectedSql =
            "select"
            + " `store`.`store_country` as `Store Country`,"
            + " `store`.`store_state` as `Store State`,"
            + " `store`.`store_city` as `Store City`,"
            + " `store`.`store_name` as `Store Name`,"
            + " `store`.`store_sqft` as `Store Sqft`,"
            + " `store`.`store_type` as `Store Type`,"
            + " `time_by_day`.`the_year` as `Year`,"
            + " `time_by_day`.`quarter` as `Quarter`,"
            + " `time_by_day`.`month_of_year` as `Month`,"
            + " `time_by_day`.`week_of_year` as `Week`,"
            + " `time_by_day`.`day_of_month` as `Day`,"
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
            + " `customer`.`city` as `City`,"
            + " " + nameExpStr + " as `Name`,"
            + " `customer`.`customer_id` as `Name (Key)`,"
            + " `customer`.`education` as `Education Level`,"
            + " `customer`.`gender` as `Gender`,"
            + " `customer`.`marital_status` as `Marital Status`,"
            + " `customer`.`yearly_income` as `Yearly Income`,"
            + " `sales_fact_1997`.`unit_sales` as `Unit Sales` "
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
            + "order by `store`.`store_country` ASC,"
            + " `store`.`store_state` ASC,"
            + " `store`.`store_city` ASC,"
            + " `store`.`store_name` ASC,"
            + " `store`.`store_sqft` ASC,"
            + " `store`.`store_type` ASC,"
            + " `time_by_day`.`the_year` ASC,"
            + " `time_by_day`.`quarter` ASC,"
            + " `time_by_day`.`month_of_year` ASC,"
            + " `time_by_day`.`week_of_year` ASC,"
            + " `time_by_day`.`day_of_month` ASC,"
            + " `product_class`.`product_family` ASC,"
            + " `product_class`.`product_department` ASC,"
            + " `product_class`.`product_category` ASC,"
            + " `product_class`.`product_subcategory` ASC,"
            + " `product`.`brand_name` ASC,"
            + " `product`.`product_name` ASC,"
            + " `promotion.media_type` ASC,"
            + " `promotion`.`promotion_name` ASC,"
            + " `customer`.`country` ASC,"
            + " `customer`.`state_province` ASC,"
            + " `customer`.`city` ASC,"
            + " " + nameExpStr + " ASC,"
            + " `customer`.`customer_id` ASC,"
            + " `customer`.`education` ASC,"
            + " `customer`.gender` ASC,"
            + " `customer`.`marital_status` ASC,"
            + " `customer`.`yearly_income` ASC";

        getTestContext().assertSqlEquals(expectedSql, sql, 141);
    }

    /**
     * Test case for bug <a href="http://jira.pentaho.com/browse/MONDRIAN-180">
     * MONDRIAN-180, "Drillthrough fails, if Aggregate in
     * MDX-query"</a>. The problem actually occurs with any calculated member,
     * not just Aggregate. The bug was causing a syntactically invalid
     * constraint to be added to the WHERE clause; after the fix, we do
     * not constrain on the member at all.
     */
    public void testDrillThroughBugMondrian180() {
        Result result = executeQuery(
            "with set [Date Range] as '{[Time].[1997].[Q1], [Time].[1997].[Q2]}'\n"
            + "member [Time].[Time].[Date Range] as 'Aggregate([Date Range])'\n"
            + "select {[Measures].[Unit Sales]} ON COLUMNS,\n"
            + "Hierarchize(Union(Union(Union({[Store].[All Stores]}, [Store].[All Stores].Children), [Store].[All Stores].[USA].Children), [Store].[All Stores].[USA].[CA].Children)) ON ROWS\n"
            + "from [Sales]\n"
            + "where [Time].[Date Range]");

        final Cell cell = result.getCell(new int[]{0, 6});

        // It is not valid to drill through this cell, because it contains a
        // non-trivial calculated member.
        assertFalse(cell.canDrillThrough());

        // For backwards compatibility, generate drill-through SQL (ignoring
        // calculated members) even though we said we could not drill through.
        String sql = cell.getDrillThroughSQL(true);

        String nameExpStr = getNameExp(result, "Customers", "Name");

        final String expectedSql =
            "select"
            + " `store`.`store_state` as `Store State`,"
            + " `store`.`store_city` as `Store City`,"
            + " `store`.`store_name` as `Store Name`,"
            + " `store`.`store_sqft` as `Store Sqft`,"
            + " `store`.`store_type` as `Store Type`,"
            + " `time_by_day`.`the_year` as `Year`,"
            + " `time_by_day`.`quarter` as `Quarter`,"
            + " `time_by_day`.`month_of_year` as `Month`,"
            + " `time_by_day`.`week_of_year` as `Week`,"
            + " `time_by_day`.`day_of_month` as `Day`,"
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
            + " `store`.`store_state` ASC,"
            + " `store`.`store_city` ASC,"
            + " `store`.`store_name` ASC,"
            + " `store`.`store_sqft` ASC,"
            + " `store`.`store_type` ASC,"
            + " `time_by_day`.`the_year` ASC,"
            + " `time_by_day`.`quarter` ASC,"
            + " `time_by_day`.`month_of_year` ASC,"
            + " `time_by_day`.`week_of_year` ASC,"
            + " `time_by_day`.`day_of_month` ASC,"
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

        getTestContext().assertSqlEquals(expectedSql, sql, 6815);
    }

    /**
     * Tests that proper SQL is being generated for a Measure specified
     * as an expression.
     */
    public void testDrillThroughMeasureExp() {
        Result result = executeQuery(
            "SELECT {[Measures].[Promotion Sales]} on columns,\n"
            + " {[Product].Children} on rows\n"
            + "from Sales");
        String sql = result.getCell(new int[] {0, 0}).getDrillThroughSQL(false);

        String expectedSql =
            "select"
            + " `time_by_day`.`the_year` as `Year`,"
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
    public void testDrillThroughDupKeys() {
         // Note here that the type on the Store Id level is Integer or
         // Numeric. The default, of course, would be String.
         //
         // For DB2 and Derby, we need the Integer type, otherwise the
         // generated SQL will be something like:
         //
         //      `store_ragged`.`store_id` = '19'
         //
         //  and DB2 and Derby don't like converting from CHAR to INTEGER
        TestContext testContext = TestContext.instance().createSubstitutingCube(
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
    public void testDrillThroughVirtualCube() {
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
    public void testBug1438285() {
        final Dialect dialect = getTestContext().getDialect();
        if (dialect.getDatabaseProduct() == Dialect.DatabaseProduct.TERADATA) {
            // On default Teradata express instance there isn't enough spool
            // space to run this query.
            return;
        }

        // Specify the column and nameColumn to be the same
        // in order to reproduce the problem
        final TestContext testContext =
            TestContext.instance().createSubstitutingCube(
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
            "select "
            + "`store`.`store_country` as `Store Country`,"
            + " `store`.`store_state` as `Store State`,"
            + " `store`.`store_city` as `Store City`,"
            + " `store`.`store_name` as `Store Name`,"
            + " `store`.`store_sqft` as `Store Sqft`,"
            + " `store`.`store_type` as `Store Type`,"
            + " `time_by_day`.`the_year` as `Year`,"
            + " `time_by_day`.`quarter` as `Quarter`,"
            + " `time_by_day`.`month_of_year` as `Month`,"
            + " `time_by_day`.`week_of_year` as `Week`,"
            + " `time_by_day`.`day_of_month` as `Day`,"
            + " `product_class`.`product_family` as `Product Family`,"
            + " `product_class`.`product_department` as `Product Department`,"
            + " `product_class`.`product_category` as `Product Category`,"
            + " `product_class`.`product_subcategory` as `Product Subcategory`,"
            + " `product`.`brand_name` as `Brand Name`,"
            + " `product`.`product_name` as `Product Name`,"
            + " `store_ragged`.`store_id` as `Store Id`,"
            + " `store_ragged`.`store_id` as `Store Id (Key)`,"
            + " `promotion`.`media_type` as `Media Type`,"
            + " `promotion`.`promotion_name` as `Promotion Name`,"
            + " `customer`.`country` as `Country`,"
            + " `customer`.`state_province` as `State Province`,"
            + " `customer`.`city` as `City`,"
            + " " + nameExpStr + " as `Name`,"
            + " `customer`.`customer_id` as `Name (Key)`,"
            + " `customer`.`education` as `Education Level`,"
            + " `customer`.`gender` as `Gender`,"
            + " `customer`.`marital_status` as `Marital Status`,"
            + " `customer`.`yearly_income` as `Yearly Income`,"
            + " `sales_fact_1997`.`unit_sales` as `Unit Sales`"
            + " from `store` =as= `store`,"
            + " `sales_fact_1997` =as= `sales_fact_1997`,"
            + " `time_by_day` =as= `time_by_day`,"
            + " `product_class` =as= `product_class`,"
            + " `product` =as= `product`,"
            + " `store_ragged` =as= `store_ragged`,"
            + " `promotion` =as= `promotion`,"
            + " `customer` =as= `customer`"
            + " where `sales_fact_1997`.`store_id` = `store`.`store_id`"
            + " and `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`"
            + " and `time_by_day`.`the_year` = 1997"
            + " and `sales_fact_1997`.`product_id` = `product`.`product_id`"
            + " and `product`.`product_class_id` = `product_class`.`product_class_id`"
            + " and `sales_fact_1997`.`store_id` = `store_ragged`.`store_id`"
            + " and `sales_fact_1997`.`promotion_id` = `promotion`.`promotion_id`"
            + " and `sales_fact_1997`.`customer_id` = `customer`.`customer_id`"
            + " order by `store`.`store_country` ASC,"
            + " `store`.`store_state` ASC,"
            + " `store`.`store_city` ASC,"
            + " `store`.`store_name` ASC,"
            + " `store`.`store_sqft` ASC,"
            + " `store`.`store_type` ASC,"
            + " `time_by_day`.`the_year` ASC,"
            + " `time_by_day`.`quarter` ASC,"
            + " `time_by_day`.`month_of_year` ASC,"
            + " `time_by_day`.`week_of_year` ASC,"
            + " `time_by_day`.`day_of_month` ASC,"
            + " `product_class`.`product_family` ASC,"
            + " `product_class`.`product_department` ASC,"
            + " `product_class`.`product_category` ASC,"
            + " `product_class`.`product_subcategory` ASC,"
            + " `product`.`brand_name` ASC,"
            + " `product`.`product_name` ASC,"
            + " `store_ragged`.`store_id` ASC,"
            + " `promotion`.`media_type` ASC,"
            + " `promotion`.`promotion_name` ASC,"
            + " `customer`.`country` ASC,"
            + " `customer`.`state_province` ASC,"
            + " `customer`.`city` ASC,"
            + " " + nameExpStr + " ASC,"
            + " `customer`.`customer_id` ASC,"
            + " `customer`.`education` ASC,"
            + " `customer`.`gender` ASC,"
            + " `customer`.`marital_status` ASC,"
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
        TestContext testContext = TestContext.instance().createSubstitutingCube(
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

    public void testDrillthroughMaxRows() throws SQLException {
        assertMaxRows("", 29);
        assertMaxRows("maxrows 1000", 29);
        assertMaxRows("maxrows 0", 29);
        assertMaxRows("maxrows 3", 3);
        assertMaxRows("maxrows 10 firstrowset 6", 4);
        assertMaxRows("firstrowset 20", 9);
        assertMaxRows("firstrowset 30", 0);
    }

    private void assertMaxRows(String firstMaxRow, int expectedCount)
        throws SQLException
    {
        final ResultSet resultSet = getTestContext().executeStatement(
            "drillthrough\n"
            + firstMaxRow
            + " select\n"
            + "non empty{[Customers].[USA].[CA]} on 0,\n"
            + "non empty {[Product].[Drink].[Beverages].[Pure Juice Beverages].[Juice]} on 1\n"
            + "from\n"
            + "[Sales]\n"
            + "where([Measures].[Sales Count], [Time].[1997].[Q3].[8])");
        int actualCount = 0;
        while (resultSet.next()) {
            ++actualCount;
        }
        assertEquals(expectedCount, actualCount);
        resultSet.close();
    }

    public void testDrillthroughNegativeMaxRowsFails() throws SQLException {
        try {
            final ResultSet resultSet = getTestContext().executeStatement(
                "DRILLTHROUGH MAXROWS -3\n"
                + "SELECT {[Customers].[USA].[CA].[Berkeley]} ON 0,\n"
                + "{[Time].[1997]} ON 1\n"
                + "FROM Sales");
            fail("expected error, got " + resultSet);
        } catch (SQLException e) {
            TestContext.checkThrowable(
                e, "Syntax error at line 1, column 22, token '-'");
        }
    }

    public void testDrillThroughCalculatedMemberMeasure() {
        try {
            final ResultSet resultSet = getTestContext().executeStatement(
                "DRILLTHROUGH\n"
                + "SELECT {[Customers].[USA].[CA].[Berkeley]} ON 0,\n"
                + "{[Time].[1997]} ON 1\n"
                + "FROM Sales\n"
                + "RETURN  [Measures].[Profit]");
            fail("expected error, got " + resultSet);
        } catch (Exception e) {
            TestContext.checkThrowable(
                e,
                "Can't perform drillthrough operations because '[Measures].[Profit]' is a calculated member.");
        }
    }

    public void testDrillThroughNotDrillableFails() {
        try {
            final ResultSet resultSet = getTestContext().executeStatement(
                "DRILLTHROUGH\n"
                + "WITH MEMBER [Measures].[Foo] "
                + " AS [Measures].[Unit Sales]\n"
                + "   + ([Measures].[Unit Sales], [Time].[Time].PrevMember)\n"
                + "SELECT {[Customers].[USA].[CA].[Berkeley]} ON 0,\n"
                + "{[Time].[1997]} ON 1\n"
                + "FROM Sales\n"
                + "WHERE [Measures].[Foo]");
            fail("expected error, got " + resultSet);
        } catch (Exception e) {
            TestContext.checkThrowable(
                e, "Cannot do DrillThrough operation on the cell");
        }
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

    /**
     * Test case for bug <a href="http://jira.pentaho.com/browse/MONDRIAN-752">
     * MONDRIAN-752, "cell.getDrillCount returns 0".
     */
    public void testDrillThroughOneAxis() {
        Result result = executeQuery(
            "SELECT [Measures].[Unit Sales] on 0\n"
            + "from Sales");

        final Cell cell = result.getCell(new int[]{0});
        assertTrue(cell.canDrillThrough());
        assertEquals(86837, cell.getDrillThroughCount());
    }

    /**
     * Test case for bug <a href="http://jira.pentaho.com/browse/MONDRIAN-751">
     * MONDRIAN-751, "Drill SQL does not include slicer members in WHERE
     * clause".
     */
    public void testDrillThroughCalcMemberInSlicer() {
        Result result = executeQuery(
            "WITH MEMBER [Product].[Aggregate Food Drink] AS \n"
            + " Aggregate({[Product].[Food], [Product].[Drink]})\n"
            + "SELECT [Measures].[Unit Sales] on 0\n"
            + "from Sales\n"
            + "WHERE [Product].[Aggregate Food Drink]");

        final Cell cell = result.getCell(new int[]{0});
        assertFalse(cell.canDrillThrough());
    }



    /**
     * Test case for MONDRIAN-791.
     */
    public void testDrillThroughMultiPositionCompoundSlicer() {
        propSaver.set(propSaver.properties.GenerateFormattedSql, true);
        // A query with a simple multi-position compound slicer
        Result result =
            executeQuery(
                "SELECT {[Measures].[Unit Sales]} ON COLUMNS,\n"
                + " {[Product].[All Products]} ON ROWS\n"
                + "FROM [Sales]\n"
                + "WHERE {[Time].[1997].[Q1], [Time].[1997].[Q2]}");
        Cell cell = result.getCell(new int[]{0, 0});
        assertTrue(cell.canDrillThrough());
        String sql = cell.getDrillThroughSQL(false);
        String expectedSql;
        switch (getTestContext().getDialect().getDatabaseProduct()) {
        case MYSQL:
            expectedSql =
                "select\n"
                + "    time_by_day.the_year as Year,\n"
                + "    time_by_day.quarter as Quarter,\n"
                + "    sales_fact_1997.unit_sales as Unit Sales\n"
                + "from\n"
                + "    time_by_day as time_by_day,\n"
                + "    sales_fact_1997 as sales_fact_1997\n"
                + "where\n"
                + "    sales_fact_1997.time_id = time_by_day.time_id\n"
                + "and\n"
                + "    (((time_by_day.the_year, time_by_day.quarter) in ((1997, 'Q1'), (1997, 'Q2'))))";
            break;
        case ORACLE:
            expectedSql =
                "select\n"
                + "    time_by_day.the_year as Year,\n"
                + "    time_by_day.quarter as Quarter,\n"
                + "    sales_fact_1997.unit_sales as Unit Sales\n"
                + "from\n"
                + "    time_by_day time_by_day,\n"
                + "    sales_fact_1997 sales_fact_1997\n"
                + "where\n"
                + "    sales_fact_1997.time_id = time_by_day.time_id\n"
                + "and\n"
                + "    ((time_by_day.quarter = 'Q1' and time_by_day.the_year = 1997) or (time_by_day.quarter = 'Q2' and time_by_day.the_year = 1997))";
            break;
        default:
                return;
        }
        getTestContext().assertSqlEquals(expectedSql, sql, 41956);

        // A query with a slightly more complex multi-position compound slicer
        result =
            executeQuery(
                "SELECT {[Measures].[Unit Sales]} ON COLUMNS,\n"
                + " {[Product].[All Products]} ON ROWS\n"
                + "FROM [Sales]\n"
                + "WHERE Crossjoin(Crossjoin({[Gender].[F]}, {[Marital Status].[M]}),"
                + "                {[Time].[1997].[Q1], [Time].[1997].[Q2]})");
        cell = result.getCell(new int[]{0, 0});
        assertTrue(cell.canDrillThrough());
        sql = cell.getDrillThroughSQL(false);

        // Note that gender and marital status get their own predicates,
        // independent of the time portion of the slicer
        switch (getTestContext().getDialect().getDatabaseProduct()) {
        case MYSQL:
            expectedSql =
                "select\n"
                + "    customer.gender as Gender,\n"
                + "    customer.marital_status as Marital Status,\n"
                + "    time_by_day.the_year as Year,\n"
                + "    time_by_day.quarter as Quarter,\n"
                + "    sales_fact_1997.unit_sales as Unit Sales\n"
                + "from\n"
                + "    customer as customer,\n"
                + "    sales_fact_1997 as sales_fact_1997,\n"
                + "    time_by_day as time_by_day\n"
                + "where\n"
                + "    sales_fact_1997.customer_id = customer.customer_id\n"
                + "and\n"
                + "    customer.gender = 'F'\n"
                + "and\n"
                + "    customer.marital_status = 'M'\n"
                + "and\n"
                + "    sales_fact_1997.time_id = time_by_day.time_id\n"
                + "and\n"
                + "    (((time_by_day.the_year, time_by_day.quarter) in ((1997, 'Q1'), (1997, 'Q2'))))\n"
                + "order by\n"
                + "    customer.gender ASC,\n"
                + "    customer.marital_status ASC";
            break;
        case ORACLE:
            expectedSql =
                "select\n"
                + "    customer.gender as Gender,\n"
                + "    customer.marital_status as Marital Status,\n"
                + "    time_by_day.the_year as Year,\n"
                + "    time_by_day.quarter as Quarter,\n"
                + "    sales_fact_1997.unit_sales as Unit Sales\n"
                + "from\n"
                + "    customer customer,\n"
                + "    sales_fact_1997 sales_fact_1997,\n"
                + "    time_by_day time_by_day\n"
                + "where\n"
                + "    sales_fact_1997.customer_id = customer.customer_id\n"
                + "and\n"
                + "    customer.gender = 'F'\n"
                + "and\n"
                + "    customer.marital_status = 'M'\n"
                + "and\n"
                + "    sales_fact_1997.time_id = time_by_day.time_id\n"
                + "and\n"
                + "    ((time_by_day.quarter = 'Q1' and time_by_day.the_year = 1997) or (time_by_day.quarter = 'Q2' and time_by_day.the_year = 1997))\n"
                + "order by\n"
                + "    customer.gender ASC,\n"
                + "    customer.marital_status ASC";
            break;
        default:
            return;
        }
        getTestContext().assertSqlEquals(expectedSql, sql, 10430);

        // A query with an even more complex multi-position compound slicer
        // (gender must be in the slicer predicate along with time)
        result =
            executeQuery(
                "SELECT {[Measures].[Unit Sales]} ON COLUMNS,\n"
                + " {[Product].[All Products]} ON ROWS\n"
                + "FROM [Sales]\n"
                + "WHERE Union(Crossjoin({[Gender].[F]}, {[Time].[1997].[Q1]}),"
                + "            Crossjoin({[Gender].[M]}, {[Time].[1997].[Q2]}))");
        cell = result.getCell(new int[]{0, 0});
        assertTrue(cell.canDrillThrough());
        sql = cell.getDrillThroughSQL(false);

        // Note that gender and marital status get their own predicates,
        // independent of the time portion of the slicer
        switch (getTestContext().getDialect().getDatabaseProduct()) {
        case MYSQL:
            expectedSql =
                "select\n"
                + "    time_by_day.the_year as Year,\n"
                + "    time_by_day.quarter as Quarter,\n"
                + "    customer.gender as Gender,\n"
                + "    sales_fact_1997.unit_sales as Unit Sales\n"
                + "from\n"
                + "    time_by_day as time_by_day,\n"
                + "    sales_fact_1997 as sales_fact_1997,\n"
                + "    customer as customer\n"
                + "where\n"
                + "    sales_fact_1997.time_id = time_by_day.time_id\n"
                + "and\n"
                + "    sales_fact_1997.customer_id = customer.customer_id\n"
                + "and\n"
                + "    (((time_by_day.the_year, time_by_day.quarter, customer.gender) in ((1997, 'Q1', 'F'), (1997, 'Q2', 'M'))))";
            break;
        case ORACLE:
            expectedSql =
                "select\n"
                + "    time_by_day.the_year as Year,\n"
                + "    time_by_day.quarter as Quarter,\n"
                + "    customer.gender as Gender,\n"
                + "    sales_fact_1997.unit_sales as Unit Sales\n"
                + "from\n"
                + "    time_by_day time_by_day,\n"
                + "    sales_fact_1997 sales_fact_1997,\n"
                + "    customer customer\n"
                + "where\n"
                + "    sales_fact_1997.time_id = time_by_day.time_id\n"
                + "and\n"
                + "    sales_fact_1997.customer_id = customer.customer_id\n"
                + "and\n"
                + "    ((customer.gender = 'F' and time_by_day.quarter = 'Q1' and time_by_day.the_year = 1997) or (customer.gender = 'M' and time_by_day.quarter = 'Q2' and time_by_day.the_year = 1997))";
            break;
        default:
            return;
        }
        getTestContext().assertSqlEquals(expectedSql, sql, 20971);

        // A query with a simple multi-position compound slicer with
        // different levels (overlapping)
        result =
            executeQuery(
                "SELECT {[Measures].[Unit Sales]} ON COLUMNS,\n"
                + " {[Product].[All Products]} ON ROWS\n"
                + "FROM [Sales]\n"
                + "WHERE {[Time].[1997].[Q1], [Time].[1997].[Q1].[1]}");
        cell = result.getCell(new int[]{0, 0});
        assertTrue(cell.canDrillThrough());
        sql = cell.getDrillThroughSQL(false);

        // With overlapping slicer members, the first slicer predicate is
        // redundant, but does not affect the query's results
        switch (getTestContext().getDialect().getDatabaseProduct()) {
        case MYSQL:
            expectedSql =
                "select\n"
                + "    time_by_day.the_year as Year,\n"
                + "    time_by_day.quarter as Quarter,\n"
                + "    time_by_day.month_of_year as Month,\n"
                + "    sales_fact_1997.unit_sales as Unit Sales\n"
                + "from\n"
                + "    time_by_day as time_by_day,\n"
                + "    sales_fact_1997 as sales_fact_1997\n"
                + "where\n"
                + "    sales_fact_1997.time_id = time_by_day.time_id\n"
                + "and\n"
                + "    ((time_by_day.quarter = 'Q1' and time_by_day.the_year = 1997) or (time_by_day.month_of_year = 1 and time_by_day.quarter = 'Q1' and time_by_day.the_year = 1997))";
            break;
        case ORACLE:
            expectedSql =
                "select\n"
                + "    time_by_day.the_year as Year,\n"
                + "    time_by_day.quarter as Quarter,\n"
                + "    time_by_day.month_of_year as Month,\n"
                + "    sales_fact_1997.unit_sales as Unit Sales\n"
                + "from\n"
                + "    time_by_day time_by_day,\n"
                + "    sales_fact_1997 sales_fact_1997\n"
                + "where\n"
                + "    sales_fact_1997.time_id = time_by_day.time_id\n"
                + "and\n"
                + "    ((time_by_day.quarter = 'Q1' and time_by_day.the_year = 1997) or (time_by_day.month_of_year = 1 and time_by_day.quarter = 'Q1' and time_by_day.the_year = 1997))";
            break;
        default:
            return;
        }
        getTestContext().assertSqlEquals(expectedSql, sql, 21588);

        // A query with a simple multi-position compound slicer with
        // different levels (non-overlapping)
        result =
            executeQuery(
                "SELECT {[Measures].[Unit Sales]} ON COLUMNS,\n"
                + " {[Product].[All Products]} ON ROWS\n"
                + "FROM [Sales]\n"
                + "WHERE {[Time].[1997].[Q1].[1], [Time].[1997].[Q2]}");
        cell = result.getCell(new int[]{0, 0});
        assertTrue(cell.canDrillThrough());
        sql = cell.getDrillThroughSQL(false);
        switch (getTestContext().getDialect().getDatabaseProduct()) {
        case MYSQL:
            expectedSql =
                "select\n"
                + "    time_by_day.the_year as Year,\n"
                + "    time_by_day.quarter as Quarter,\n"
                + "    time_by_day.month_of_year as Month,\n"
                + "    sales_fact_1997.unit_sales as Unit Sales\n"
                + "from\n"
                + "    time_by_day as time_by_day,\n"
                + "    sales_fact_1997 as sales_fact_1997\n"
                + "where\n"
                + "    sales_fact_1997.time_id = time_by_day.time_id\n"
                + "and\n"
                + "    ((time_by_day.month_of_year = 1 and time_by_day.quarter = 'Q1' and time_by_day.the_year = 1997) or (time_by_day.quarter = 'Q2' and time_by_day.the_year = 1997))";
            break;
        case ORACLE:
            expectedSql =
                "select\n"
                + "    time_by_day.the_year as Year,\n"
                + "    time_by_day.quarter as Quarter,\n"
                + "    time_by_day.month_of_year as Month,\n"
                + "    sales_fact_1997.unit_sales as Unit Sales\n"
                + "from\n"
                + "    time_by_day time_by_day,\n"
                + "    sales_fact_1997 sales_fact_1997\n"
                + "where\n"
                + "    sales_fact_1997.time_id = time_by_day.time_id\n"
                + "and\n"
                + "    ((time_by_day.month_of_year = 1 and time_by_day.quarter = 'Q1' and time_by_day.the_year = 1997) or (time_by_day.quarter = 'Q2' and time_by_day.the_year = 1997))";
            break;
        default:
            return;
        }
        getTestContext().assertSqlEquals(expectedSql, sql, 27402);
    }

    public void testDrillthroughDisable() {
        propSaver.set(
            MondrianProperties.instance().EnableDrillThrough,
            true);
        Result result =
            executeQuery(
                "SELECT {[Measures].[Unit Sales]} ON COLUMNS,\n"
                + " {[Product].[All Products]} ON ROWS\n"
                + "FROM [Sales]\n"
                + "WHERE {[Time].[1997].[Q1], [Time].[1997].[Q2]}");
        Cell cell = result.getCell(new int[]{0, 0});
        assertTrue(cell.canDrillThrough());

        propSaver.set(
            MondrianProperties.instance().EnableDrillThrough,
            false);
        result =
            executeQuery(
                "SELECT {[Measures].[Unit Sales]} ON COLUMNS,\n"
                + " {[Product].[All Products]} ON ROWS\n"
                + "FROM [Sales]\n"
                + "WHERE {[Time].[1997].[Q1], [Time].[1997].[Q2]}");
        cell = result.getCell(new int[]{0, 0});
        assertFalse(cell.canDrillThrough());
        try {
            cell.getDrillThroughSQL(false);
            fail();
        } catch (MondrianException e) {
            assertTrue(
                e.getMessage().contains(
                    "Can't perform drillthrough operations because"));
        }
    }

    /**
     * Tests that dialects that require alias in order by are correctly quoted
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1983">MONDRIAN-1983</a>.
     */
    public void testColumnAliasQuotedInOrderBy() throws Exception {
        Result result = executeQuery(
            "WITH\n"
            + "SET [*NATIVE_CJ_SET] AS 'FILTER([*BASE_MEMBERS__Customers_], NOT ISEMPTY ([Measures].[Unit Sales]))'\n"
            + "SET [*BASE_MEMBERS__Measures_] AS '{[Measures].[*FORMATTED_MEASURE_0]}'\n"
            + "SET [*BASE_MEMBERS__Customers_] AS '[Customers].[Name].MEMBERS'\n"
            + "SET [*CJ_ROW_AXIS] AS 'GENERATE([*NATIVE_CJ_SET], {([Customers].CURRENTMEMBER)})'\n"
            + "SET [*SORTED_ROW_AXIS] AS 'ORDER([*CJ_ROW_AXIS],"
            + "[Customers].CURRENTMEMBER.ORDERKEY,BASC,"
            + "ANCESTOR([Customers].CURRENTMEMBER,[Customers].[City]).ORDERKEY,BASC)'\n"
            + "MEMBER [Measures].[*FORMATTED_MEASURE_0] AS '[Measures].[Unit Sales]',"
            + " FORMAT_STRING = 'Standard', SOLVE_ORDER=500\n"
            + "SELECT\n"
            + "FILTER([*BASE_MEMBERS__Measures_],([Measures].CurrentMember Is [Measures].[*FORMATTED_MEASURE_0])) ON COLUMNS\n"
            + ",FILTER([*SORTED_ROW_AXIS],([Customers].CurrentMember Is [Customers].[USA].[CA].[San Gabriel].[A. Joyce Jarvis])) ON ROWS\n"
            + "FROM [Sales]");
        Cell cell = result.getCell(new int[]{0, 0});
        String sql = cell.getDrillThroughSQL(true);
        String expectedSql;
        switch (getTestContext().getDialect().getDatabaseProduct()) {
        case VECTORWISE:
            expectedSql =
                "select \"store\".\"store_country\" as \"Store Country\","
                + " \"store\".\"store_state\" as \"Store State\","
                + " \"store\".\"store_city\" as \"Store City\","
                + " \"store\".\"store_name\" as \"Store Name\","
                + " \"store\".\"store_sqft\" as \"Store Sqft\","
                + " \"store\".\"store_type\" as \"Store Type\","
                + " \"time_by_day\".\"the_year\" as \"Year\","
                + " \"time_by_day\".\"quarter\" as \"Quarter\","
                + " \"time_by_day\".\"month_of_year\" as \"Month\","
                + " \"time_by_day\".\"week_of_year\" as \"Week\","
                + " \"time_by_day\".\"day_of_month\" as \"Day\","
                + " \"product_class\".\"product_family\" as \"Product Family\","
                + " \"product_class\".\"product_department\" as \"Product Department\", "
                + "\"product_class\".\"product_category\" as \"Product Category\","
                + " \"product_class\".\"product_subcategory\" as \"Product Subcategory\","
                + " \"product\".\"brand_name\" as \"Brand Name\","
                + " \"product\".\"product_name\" as \"Product Name\","
                + " \"promotion\".\"media_type\" as \"Media Type\","
                + " \"promotion\".\"promotion_name\" as \"Promotion Name\", "
                + "fullname as \"Name\", "
                + "\"customer\".\"customer_id\" as \"Name (Key)\","
                + " \"customer\".\"education\" as \"Education Level\","
                + " \"customer\".\"gender\" as \"Gender\","
                + " \"customer\".\"marital_status\" as \"Marital Status\","
                + " \"customer\".\"yearly_income\" as \"Yearly Income\","
                + " \"sales_fact_1997\".\"unit_sales\" as \"Unit Sales\""
                + " from \"store\" as \"store\", \"sales_fact_1997\" as \"sales_fact_1997\","
                + " \"time_by_day\" as \"time_by_day\", \"product_class\" as \"product_class\","
                + " \"product\" as \"product\", \"promotion\" as \"promotion\","
                + " \"customer\" as \"customer\" "
                + "where \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" "
                + "and \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" "
                + "and \"time_by_day\".\"the_year\" = 1997 "
                + "and \"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\" "
                + "and \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\" "
                + "and \"sales_fact_1997\".\"promotion_id\" = \"promotion\".\"promotion_id\" "
                + "and \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" "
                + "and \"customer\".\"customer_id\" = 665 "
                + "order by \"Store Country\" ASC, "
                + "\"Store State\" ASC, \"Store City\" ASC, "
                + "\"Store Name\" ASC, \"Store Sqft\" ASC, "
                + "\"Store Type\" ASC, \"Year\" ASC,"
                + " \"Quarter\" ASC, "
                + "\"Month\" ASC, "
                + "\"Week\" ASC, "
                + "\"Day\" ASC, "
                + "\"Product Family\" ASC, "
                + "\"Product Department\" ASC, "
                + "\"Product Category\" ASC, "
                + "\"Product Subcategory\" ASC, "
                + "\"Brand Name\" ASC, "
                + "\"Product Name\" ASC, "
                + "\"Media Type\" ASC, "
                + "\"Promotion Name\" ASC, "
                + "\"Name\" ASC, "
                + "\"Name (Key)\" ASC, "
                + "\"Education Level\" ASC, "
                + "\"Gender\" ASC, "
                + "\"Marital Status\" ASC, "
                + "\"Yearly Income\" ASC";
            break;
        default:
            return;
        }
        getTestContext().assertSqlEquals(expectedSql, sql, 11);
    }

    public void testDrillthroughVirtualCubeWithReturnClause()
        throws SQLException
    {
        // Validates that a RETURN clause including a mix of applicable
        // and inapplicable fields will not cause an error (ANALYZER-3017)
        // Should return w/o error and have NULL values for inapplicable
        // columns.
        ResultSet rs = null;
        try {
            rs = getTestContext().executeStatement(
                "DRILLTHROUGH \n"
                + "// Request ID: d73ea21c-2a29-11e5-ba1d-d4bed923da37 - RUN_REPORT\n"
                + "WITH\n"
                + "SET [*NATIVE_CJ_SET] AS 'FILTER([*BASE_MEMBERS__Gender_], NOT ISEMPTY ([Measures].[Unit Sales]))'\n"
                + "SET [*BASE_MEMBERS__Measures_] AS '{[Measures].[*FORMATTED_MEASURE_0]}'\n"
                + "SET [*CJ_SLICER_AXIS] AS 'GENERATE([*NATIVE_CJ_SET], {([Gender].CURRENTMEMBER)})'\n"
                + "SET [*BASE_MEMBERS__Gender_] AS '{[Gender].[F]}'\n"
                + "MEMBER [Measures].[*FORMATTED_MEASURE_0] AS '[Measures].[Unit Sales]', FORMAT_STRING = 'Standard', SOLVE_ORDER=500\n"
                + "SELECT\n"
                + "FILTER([*BASE_MEMBERS__Measures_],([Measures].CurrentMember Is [Measures].[*FORMATTED_MEASURE_0])) ON COLUMNS\n"
                + "FROM [Warehouse and Sales]\n"
                + "WHERE ([*CJ_SLICER_AXIS]) RETURN [Gender].[Gender], [Measures].[Unit Sales], [Measures].[Warehouse Sales], [Time].[Year], [Warehouse].[Country]");
            assertEquals(
                5, rs.getMetaData().getColumnCount());
            while (rs.next()) {
                assertEquals(
                    "Each year in results should be 1997",
                    1997, rs.getObject(1));
                assertEquals(
                    "Each gender in results should be F",
                    "F", rs.getObject(2));
                assertNotNull(
                    "Should be a non-null value for unit sales",
                    rs.getObject(3));
                assertEquals(
                    "Non applicable fields should be null",
                    null, rs.getObject(4));
                assertEquals(
                    "Non applicable fields should be null",
                    null, rs.getObject(5));
            }
        } finally {
            if (rs != null) {
                rs.close();
            }
        }
    }
}

// End DrillThroughTest.java
