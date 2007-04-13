/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2003-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Feb 14, 2003
*/
package mondrian.test;


import mondrian.olap.Cube;
import mondrian.olap.Result;
import mondrian.olap.Cell;
import mondrian.rolap.RolapCube;
import mondrian.rolap.RolapStar;
import mondrian.rolap.sql.SqlQuery;

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

    public void testDrillThrough() throws Exception {
        Result result = executeQuery(
                "WITH MEMBER [Measures].[Price] AS '[Measures].[Store Sales] / [Measures].[Unit Sales]'" + nl +
                "SELECT {[Measures].[Unit Sales], [Measures].[Price]} on columns," + nl +
                " {[Product].Children} on rows" + nl +
                "from Sales");
        final Cell cell = result.getCell(new int[]{0, 0});
        assertTrue(cell.canDrillThrough());
        String sql = cell.getDrillThroughSQL(false);

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
                " and `product_class`.`product_family` = 'Drink' " +
                "order by `time_by_day`.`the_year` ASC," +
                " `product_class`.`product_family` ASC";

        getTestContext().assertSqlEquals(expectedSql, sql, 7978);

        // Cannot drill through a calc member.
        final Cell calcCell = result.getCell(new int[]{1, 1});
        assertFalse(calcCell.canDrillThrough());
        sql = calcCell.getDrillThroughSQL(false);
        assertNull(sql);
    }

    private String getNameExp(Result result, String hierName, String levelName) {
        final Cube cube = result.getQuery().getCube();
        RolapStar star = ((RolapCube) cube).getStar();
        String nameExpStr = null;       

        mondrian.olap.Hierarchy h = cube.lookupHierarchy("Customers", false);
        if (h != null) {
            mondrian.rolap.RolapLevel rl = null;
            mondrian.olap.Level[] levels = h.getLevels();
            for (mondrian.olap.Level l : levels) {
                if (l.getName().equals("Name")) {
                    rl = (mondrian.rolap.RolapLevel) l;
                    break;
                }
            }
            if (rl != null) {
                mondrian.olap.MondrianDef.Expression exp = rl.getNameExp();
                nameExpStr = exp.getExpression(star.getSqlQuery());
                nameExpStr = nameExpStr.replace('"', '`') ;
            }
        }
        return nameExpStr;
    }

    public void testDrillThrough2() throws Exception {
        Result result = executeQuery(
                "WITH MEMBER [Measures].[Price] AS '[Measures].[Store Sales] / [Measures].[Unit Sales]'" + nl +
                "SELECT {[Measures].[Unit Sales], [Measures].[Price]} on columns," + nl +
                " {[Product].Children} on rows" + nl +
                "from Sales");
        String sql = result.getCell(new int[] {0, 0}).getDrillThroughSQL(true);

        String nameExpStr = getNameExp(result, "Customers", "Name");

        String expectedSql =
                "select `store`.`store_country` as `Store Country`," +
                " `store`.`store_state` as `Store State`," +
                " `store`.`store_city` as `Store City`," +
                " `store`.`store_name` as `Store Name`," +
                " `store`.`store_sqft` as `Store Sqft`," +
                " `store`.`store_type` as `Store Type`," +
                " `time_by_day`.`the_year` as `Year`," +
                " `time_by_day`.`quarter` as `Quarter`," +
                " `time_by_day`.`month_of_year` as `Month`," +
                " `product_class`.`product_family` as `Product Family`," +
                " `product_class`.`product_department` as `Product Department`," +
                " `product_class`.`product_category` as `Product Category`," +
                " `product_class`.`product_subcategory` as `Product Subcategory`," +
                " `product`.`brand_name` as `Brand Name`," +
                " `product`.`product_name` as `Product Name`," +
                " `promotion`.`media_type` as `Media Type`," +
                " `promotion`.`promotion_name` as `Promotion Name`," +
                " `customer`.`country` as `Country`," +
                " `customer`.`state_province` as `State Province`," +
                " `customer`.`city` as `City`, " +
                nameExpStr +
                " as `Name`," +
                " `customer`.`customer_id` as `Name (Key)`," +
                " `customer`.`education` as `Education Level`," +
                " `customer`.`gender` as `Gender`," +
                " `customer`.`marital_status` as `Marital Status`," +
                " `customer`.`yearly_income` as `Yearly Income`," +
                " `sales_fact_1997`.`unit_sales` as `Unit Sales` " +
                "from `store` =as= `store`," +
                " `sales_fact_1997` =as= `sales_fact_1997`," +
                " `time_by_day` =as= `time_by_day`," +
                " `product_class` =as= `product_class`," +
                " `product` =as= `product`," +
                " `promotion` =as= `promotion`," +
                " `customer` =as= `customer` " +
                "where `sales_fact_1997`.`store_id` = `store`.`store_id`" +
                " and `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`" +
                " and `time_by_day`.`the_year` = 1997" +
                " and `sales_fact_1997`.`product_id` = `product`.`product_id`" +
                " and `product`.`product_class_id` = `product_class`.`product_class_id`" +
                " and `product_class`.`product_family` = 'Drink'" +
                " and `sales_fact_1997`.`promotion_id` = `promotion`.`promotion_id`" +
                " and `sales_fact_1997`.`customer_id` = `customer`.`customer_id` " +
                "order by `store`.`store_country` ASC," +
                " `store`.`store_state` ASC," +
                " `store`.`store_city` ASC," +
                " `store`.`store_name` ASC," +
                " `store`.`store_sqft` ASC," +
                " `store`.`store_type` ASC," +
                " `time_by_day`.`the_year` ASC," +
                " `time_by_day`.`quarter` ASC," +
                " `time_by_day`.`month_of_year` ASC," +
                " `product_class`.`product_family` ASC," +
                " `product_class`.`product_department` ASC," +
                " `product_class`.`product_category` ASC," +
                " `product_class`.`product_subcategory` ASC," +
                " `product`.`brand_name` ASC," +
                " `product`.`product_name` ASC," +
                " `promotion`.`media_type` ASC," +
                " `promotion`.`promotion_name` ASC," +
                " `customer`.`country` ASC," +
                " `customer`.`state_province` ASC," +
                " `customer`.`city` ASC, " +
                nameExpStr +
                " ASC," +
                " `customer`.`customer_id` ASC," +
                " `customer`.`education` ASC," +
                " `customer`.`gender` ASC," +
                " `customer`.`marital_status` ASC," +
                " `customer`.`yearly_income` ASC";
        
        getTestContext().assertSqlEquals(expectedSql, sql, 7978);

        // Drillthrough SQL is null for cell based on calc member
        sql = result.getCell(new int[] {1, 1}).getDrillThroughSQL(true);
        assertNull(sql);
    }

    public void testDrillThrough3() throws Exception {
        Result result = executeQuery(
                "select {[Measures].[Unit Sales], [Measures].[Store Cost], [Measures].[Store Sales]} ON COLUMNS, " + nl +
                "Hierarchize(Union(Union(Crossjoin({[Promotion Media].[All Media]}, {[Product].[All Products]}), " + nl +
                "Crossjoin({[Promotion Media].[All Media]}, [Product].[All Products].Children)), Crossjoin({[Promotion Media].[All Media]}, [Product].[All Products].[Drink].Children))) ON ROWS " + nl +
                "from [Sales] where [Time].[1997].[Q4].[12]");
        
        // [Promotion Media].[All Media], [Product].[All Products].[Drink].[Dairy], [Measures].[Store Cost]
        Cell cell = result.getCell(new int[] {0, 4});

        String sql = cell.getDrillThroughSQL(true);

        String nameExpStr = getNameExp(result, "Customers", "Name");
        
        String expectedSql = 
            "select " + 
            "`store`.`store_country` as `Store Country`, `store`.`store_state` as `Store State`, `store`.`store_city` as `Store City`, `store`.`store_name` as `Store Name`, " +
            "`store`.`store_sqft` as `Store Sqft`, `store`.`store_type` as `Store Type`, " + 
            "`time_by_day`.`the_year` as `Year`, `time_by_day`.`quarter` as `Quarter`, `time_by_day`.`month_of_year` as `Month`, " +
            "`product_class`.`product_family` as `Product Family`, `product_class`.`product_department` as `Product Department`, " +
            "`product_class`.`product_category` as `Product Category`, `product_class`.`product_subcategory` as `Product Subcategory`, " + 
            "`product`.`brand_name` as `Brand Name`, `product`.`product_name` as `Product Name`, " +
            "`promotion`.`media_type` as `Media Type`, `promotion`.`promotion_name` as `Promotion Name`, " +
            "`customer`.`country` as `Country`, `customer`.`state_province` as `State Province`, `customer`.`city` as `City`, " + 
            "fname + ' ' + lname as `Name`, `customer`.`customer_id` as `Name (Key)`, " +
            "`customer`.`education` as `Education Level`, `customer`.`gender` as `Gender`, `customer`.`marital_status` as `Marital Status`, " + 
            "`customer`.`yearly_income` as `Yearly Income`, " +
            "`sales_fact_1997`.`unit_sales` as `Unit Sales` " +
            "from `store =as= `store`, " +
            "`sales_fact_1997` =as= `sales_fact_1997`, " + 
            "`time_by_day` =as= `time_by_day`, " +
            "`product_class` =as= `product_class`, " + 
            "`product` =as= `product`, " +
            "`promotion` =as= `promotion`, " +
            "`customer` =as= `customer` " +
            "where `sales_fact_1997`.`store_id` = `store`.`store_id` and " + 
            "`sales_fact_1997`.`time_id` = `time_by_day`.`time_id` and " +
            "`time_by_day`.`the_year` = 1997 and " +
            "`time_by_day`.`quarter` = 'Q4' and " +
            "`time_by_day`.`month_of_year` = 12 and " +
            "`sales_fact_1997`.`product_id` = `product`.`product_id` and " + 
            "`product`.`product_class_id` = `product_class`.`product_class_id` and " + 
            "`product_class`.`product_family` = 'Drink' and " +
            "`product_class`.`product_department` = 'Dairy' and " +
            "`sales_fact_1997`.`promotion_id` = `promotion`.`promotion_id` and " + 
            "`sales_fact_1997`.`customer_id` = `customer`.`customer_id` " +
            "order by `store`.`store_country` ASC, `store`.`store_state` ASC, `store`.`store_city` ASC, `store`.`store_name` ASC, `store`.`store_sqft` ASC, " +
            "`store`.`store_type` ASC, `time_by_day`.`the_year` ASC, `time_by_day`.`quarter` ASC, `time_by_day`.`month_of_year` ASC, " +
            "`product_class`.`product_family` ASC, `product_class`.`product_department` ASC, `product_class`.`product_category` ASC, " +
            "`product_class`.`product_subcategory` ASC, `product`.`brand_name` ASC, `product`.`product_name` ASC, " +
            "`promotion.media_type` ASC, `promotion`.`promotion_name` ASC, " +
            "`customer`.`country` ASC, `customer`.`state_province` ASC, `customer`.`city` ASC, " +
            nameExpStr +
            " ASC, " +
            "`customer`.`customer_id` ASC, `customer`.`education` ASC, `customer`.gender` ASC, `customer`.`marital_status` ASC, `customer`.`yearly_income` ASC";
        
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
         *                 "with set [Date Range] as" + nl +
                "'{[Time].[1997].[Q1],[Time].[1997].[Q2]}'" + nl +
                "member [Time].[Date Range] as 'Aggregate([Date Range])'" + nl +
                "select {[Store]} on rows," + nl +
                "{[Measures].[Unit Sales]} on columns" + nl +
                "from [Sales]" + nl +
                "where [Time].[Date Range]");
  
         */
        Result result = executeQuery(
      "with set [Date Range] as '{[Time].[1997].[Q1], [Time].[1997].[Q2]}'" + nl +
        "member [Time].[Date Range] as 'Aggregate([Date Range])'" + nl +
        "select {[Measures].[Unit Sales]} ON COLUMNS," + nl + 
        "Hierarchize(Union(Union(Union({[Store].[All Stores]}, [Store].[All Stores].Children), [Store].[All Stores].[USA].Children), [Store].[All Stores].[USA].[CA].Children)) ON ROWS" + nl +
        "from [Sales]" + nl + 
        "where [Time].[Date Range]" );
        
        //String sql = result.getCell(new int[] {0, 0}).getDrillThroughSQL(true);
        String sql = result.getCell(new int[] {0, 6}).getDrillThroughSQL(true);

        String nameExpStr = getNameExp(result, "Customers", "Name");

        final String expectedSql = 
                "select" + //`store`.`store_country` as `Store Country`," +
                " `store`.`store_state` as `Store State`," +
                " `store`.`store_city` as `Store City`," +
                " `store`.`store_name` as `Store Name`," +
                " `store`.`store_sqft` as `Store Sqft`," +
                " `store`.`store_type` as `Store Type`," +
                " `time_by_day`.`the_year` as `Year`," +
                " `time_by_day`.`quarter` as `Quarter`," +
                " `time_by_day`.`month_of_year` as `Month`," +
                " `product_class`.`product_family` as `Product Family`," +
                " `product_class`.`product_department` as `Product Department`," +
                " `product_class`.`product_category` as `Product Category`," +
                " `product_class`.`product_subcategory` as `Product Subcategory`," +
                " `product`.`brand_name` as `Brand Name`," +
                " `product`.`product_name` as `Product Name`," +
                " `promotion`.`media_type` as `Media Type`," +
                " `promotion`.`promotion_name` as `Promotion Name`," +
                " `customer`.`country` as `Country`," +
                " `customer`.`state_province` as `State Province`," +
                " `customer`.`city` as `City`, " +
                nameExpStr +
                " as `Name`," +
                " `customer`.`customer_id` as `Name (Key)`," +
                " `customer`.`education` as `Education Level`," +
                " `customer`.`gender` as `Gender`," +
                " `customer`.`marital_status` as `Marital Status`," +
                " `customer`.`yearly_income` as `Yearly Income`," +
                " `sales_fact_1997`.`unit_sales` as `Unit Sales` " +
                "from `store` =as= `store`," +
                " `sales_fact_1997` =as= `sales_fact_1997`," +
                " `time_by_day` =as= `time_by_day`," +
                " `product_class` =as= `product_class`," +
                " `product` =as= `product`," +
                " `promotion` =as= `promotion`," +
                " `customer` =as= `customer` " +
                "where `sales_fact_1997`.`store_id` = `store`.`store_id` and" +
                " `store`.`store_state` = 'CA' and" +
                " `store`.`store_city` = 'Beverly Hills' and" +
                " `sales_fact_1997`.time_id` = `time_by_day`.`time_id` and" + 
                " `sales_fact_1997`.`product_id` = `product`.`product_id` and" + 
                " `product`.`product_class_id` = `product_class`.`product_class_id` and" + 
                " `sales_fact_1997`.`promotion_id` = `promotion`.`promotion_id` and" + 
                " `sales_fact_1997`.`customer_id` = `customer`.`customer_id`" +
                " order by" + // `store`.`store_country` ASC," +
                " `store`.`store_state` ASC," +
                " `store`.`store_city` ASC," +
                " `store`.`store_name` ASC," +
                " `store`.`store_sqft` ASC," +
                " `store`.`store_type` ASC," +
                " `time_by_day`.`the_year` ASC," +
                " `time_by_day`.`quarter` ASC," +
                " `time_by_day`.`month_of_year` ASC," +
                " `product_class`.`product_family` ASC," +
                " `product_class`.`product_department` ASC," +
                " `product_class`.`product_category` ASC," +
                " `product_class`.`product_subcategory` ASC," +
                " `product`.`brand_name` ASC," +
                " `product`.`product_name` ASC," +
                " `promotion`.`media_type` ASC," +
                " `promotion`.`promotion_name` ASC," +
                " `customer`.`country` ASC," +
                " `customer`.`state_province` ASC," +
                " `customer`.`city` ASC, " +
                nameExpStr +
                " ASC," +
                " `customer`.`customer_id` ASC," +
                " `customer`.`education` ASC," +
                " `customer`.`gender` ASC," +
                " `customer`.`marital_status` ASC," +
                " `customer`.`yearly_income` ASC";

        //getTestContext().assertSqlEquals(expectedSql, sql, 86837);
        getTestContext().assertSqlEquals(expectedSql, sql, 6815);
    }

    // Test that proper SQL is being generated for a Measure specified
    // as an expression
    public void testDrillThroughMeasureExp() throws Exception {
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
                " and `product_class`.`product_family` = 'Drink' " +
                "order by `time_by_day`.`the_year` ASC, `product_class`.`product_family` ASC";

        final Cube cube = result.getQuery().getCube();
        RolapStar star = ((RolapCube) cube).getStar();

        SqlQuery.Dialect dialect = star.getSqlQueryDialect();
        if (dialect.isAccess()) {
            String caseStmt =
                " \\(case when `sales_fact_1997`.`promotion_id` = 0 then 0" +
                " else `sales_fact_1997`.`store_sales` end\\)";
            expectedSql = expectedSql.replaceAll(
                caseStmt,
                " Iif(`sales_fact_1997`.`promotion_id` = 0, 0," +
                " `sales_fact_1997`.`store_sales`)");
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
            "  <Dimension name=\"Store2\" foreignKey=\"store_id\">\n" +
                "    <Hierarchy hasAll=\"true\" primaryKey=\"store_id\">\n" +
                "      <Table name=\"store_ragged\"/>\n" +
                "      <Level name=\"Store Country\" column=\"store_country\" uniqueMembers=\"true\"/>\n" +
                "      <Level name=\"Store Id\" column=\"store_id\" captionColumn=\"store_name\" uniqueMembers=\"true\" type=\"Integer\"/>\n" +
                "    </Hierarchy>\n" +
                "  </Dimension>\n" +
                "  <Dimension name=\"Store3\" foreignKey=\"store_id\">\n" +
                "    <Hierarchy hasAll=\"true\" primaryKey=\"store_id\">\n" +
                "      <Table name=\"store\"/>\n" +
                "      <Level name=\"Store Country\" column=\"store_country\" uniqueMembers=\"true\"/>\n" +
                "      <Level name=\"Store Id\" column=\"store_id\" captionColumn=\"store_name\" uniqueMembers=\"true\" type=\"Numeric\"/>\n" +
                "    </Hierarchy>\n" +
                "  </Dimension>\n");
        Result result = testContext.executeQuery(
                "SELECT {[Store2].[Store Id].Members} on columns," + nl +
                " NON EMPTY([Store3].[Store Id].Members) on rows" + nl +
                "from Sales");
        String sql = result.getCell(new int[] {0, 0}).getDrillThroughSQL(false);

        String expectedSql =
            "select `time_by_day`.`the_year` as `Year`," +
                " `store_ragged`.`store_id` as `Store Id`," +
                " `store`.`store_id` as `Store Id_0`," +
                " `sales_fact_1997`.`unit_sales` as"
                + " `Unit Sales` " +
                "from `time_by_day` =as= `time_by_day`," +
                " `sales_fact_1997` =as= `sales_fact_1997`," +
                " `store_ragged` =as= `store_ragged`," +
                " `store` =as= `store` " +
                "where `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`" +
                " and `time_by_day`.`the_year` = 1997" +
                " and `sales_fact_1997`.`store_id` = `store_ragged`.`store_id`" +
                " and `store_ragged`.`store_id` = 19" +
                " and `sales_fact_1997`.`store_id` = `store`.`store_id`" +
                " and `store`.`store_id` = 2 " +
                "order by `time_by_day`.`the_year` ASC, `store_ragged`.`store_id` ASC, `store`.`store_id` ASC";
        
        getTestContext().assertSqlEquals(expectedSql, sql, 0);
    }
    /**
     * Tests that cells in a virtual cube say they can be drilled through.
     */
    public void testDrillThroughVirtualCube() throws Exception {
        Result result = executeQuery(
                "select Crossjoin([Customers].[All Customers].[USA].[OR].Children, {[Measures].[Unit Sales]}) ON COLUMNS, " +
                " [Gender].[All Gender].Children ON ROWS" +
                " from [Warehouse and Sales]" +
                " where [Time].[1997].[Q4].[12]");

        String sql = result.getCell(new int[] {0, 0}).getDrillThroughSQL(false);

        String expectedSql =
            "select `time_by_day`.`the_year` as `Year`," +
                " `time_by_day`.`quarter` as `Quarter`," + 
                " `time_by_day`.month_of_year` as `Month`," +
                " `customer`.`state_province` as `State Province`," +
                " `customer`.`city` as `City`," +
                " `customer`.`gender` as `Gender`," +
                " `sales_fact_1997`.`unit_sales` as `Unit Sales`" +
                " from `time_by_day` as `time_by_day`," +
                " `sales_fact_1997` as `sales_fact_1997`," +
                " `customer` as `customer`" + 
                " where `sales_fact_1997`.`time_id` = `time_by_day`.`time_id` and" +
                " `time_by_day`.`the_year` = 1997 and" +
                " `time_by_day`.`quarter` = 'Q4' and" +
                " `time_by_day`.`month_of_year` = 12 and" +
                " `sales_fact_1997`.`customer_id` = `customer`.customer_id` and" +
                " `customer`.`state_province` = 'OR' and" +
                " `customer`.`city` = 'Albany' and" +
                " `customer`.`gender` = 'F'" +
                " order by `time_by_day`.`the_year` ASC," +
                " `time_by_day`.`quarter` ASC," +
                " `time_by_day`.`month_of_year` ASC," +
                " `customer`.`state_province` ASC," +
                " `customer`.`city` ASC," +
                " `customer`.`gender` ASC";

        getTestContext().assertSqlEquals(expectedSql, sql, 73);
}

}

// End DrillThroughTest.java
