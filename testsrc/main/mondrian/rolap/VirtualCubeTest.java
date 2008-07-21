/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2003-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.test.SqlPattern;
import mondrian.test.TestContext;

import java.util.List;

/**
 * <code>VirtualCubeTest</code> shows virtual cube tests.
 *
 * @author remberson
 * @since Feb 14, 2003
 * @version $Id$
 */
public class VirtualCubeTest extends BatchTestCase {
    public VirtualCubeTest() {
    }
    public VirtualCubeTest(String name) {
        super(name);
    }

    /**
     * This method demonstrates bug 1449929
     */
    public void testNoTimeDimension() {
        TestContext testContext = TestContext.create(
            null, null,
            "<VirtualCube name=\"Sales vs Warehouse\">\n" +
                "<VirtualCubeDimension name=\"Product\"/>\n" +
                "<VirtualCubeMeasure cubeName=\"Warehouse\" name=\"[Measures].[Warehouse Sales]\"/>\n" +
                "<VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Unit Sales]\"/>\n" +
                "</VirtualCube>",
            null, null, null);
        checkXxx(testContext);
    }

    public void testCalculatedMeasureAsDefaultMeasureInVC() {
        TestContext testContext = TestContext.create(
            null, null,
            "<VirtualCube name=\"Sales vs Warehouse\" defaultMeasure=\"Profit\">\n" +
                "<VirtualCubeDimension name=\"Product\"/>\n" +
                "<VirtualCubeMeasure cubeName=\"Warehouse\" " +
                    "name=\"[Measures].[Warehouse Sales]\"/>\n" +
                "<VirtualCubeMeasure cubeName=\"Sales\" " +
                    "name=\"[Measures].[Unit Sales]\"/>\n" +
                "<VirtualCubeMeasure cubeName=\"Sales\" " +
                    "name=\"[Measures].[Profit]\"/>\n" +
                "</VirtualCube>",
            null, null, null);
        String query1 = "select from [Sales vs Warehouse]";
        String query2 = "select from [Sales vs Warehouse] where measures.profit";
        assertQueriesReturnSimilarResults(query1,query2, testContext);
    }

    public void testDefaultMeasureInVCForIncorrectMeasureName() {
        TestContext testContext = TestContext.create(
            null, null,
            "<VirtualCube name=\"Sales vs Warehouse\" defaultMeasure=\"Profit Error\">\n" +
                "<VirtualCubeDimension name=\"Product\"/>\n" +
                "<VirtualCubeMeasure cubeName=\"Warehouse\" " +
                    "name=\"[Measures].[Warehouse Sales]\"/>\n" +
                "<VirtualCubeMeasure cubeName=\"Sales\" " +
                    "name=\"[Measures].[Unit Sales]\"/>\n" +
                "<VirtualCubeMeasure cubeName=\"Sales\" " +
                    "name=\"[Measures].[Profit]\"/>\n" +
                "</VirtualCube>",
            null, null, null);
        String query1 = "select from [Sales vs Warehouse]";
        String query2 = "select from [Sales vs Warehouse] " +
                "where measures.[Warehouse Sales]";
        assertQueriesReturnSimilarResults(query1,query2, testContext);
    }

    public void testDefaultMeasureInVCForCaseSensitivity() {
        TestContext testContext = TestContext.create(
            null, null,
            "<VirtualCube name=\"Sales vs Warehouse\" defaultMeasure=\"PROFIT\">\n" +
                "<VirtualCubeDimension name=\"Product\"/>\n" +
                "<VirtualCubeMeasure cubeName=\"Warehouse\" " +
                    "name=\"[Measures].[Warehouse Sales]\"/>\n" +
                "<VirtualCubeMeasure cubeName=\"Sales\" " +
                    "name=\"[Measures].[Unit Sales]\"/>\n" +
                "<VirtualCubeMeasure cubeName=\"Sales\" " +
                    "name=\"[Measures].[Profit]\"/>\n" +
                "</VirtualCube>",
            null, null, null);
        String queryWithoutFilter = "select from [Sales vs Warehouse]";
        String queryWithFirstMeasure = "select from [Sales vs Warehouse] " +
                "where measures.[Warehouse Sales]";
        String queryWithDefaultMeasureFilter = "select from [Sales vs Warehouse] " +
                "where measures.[Profit]";

        if (MondrianProperties.instance().CaseSensitive.get()) {
            assertQueriesReturnSimilarResults(queryWithoutFilter,
                    queryWithFirstMeasure, testContext);
        } else {
            assertQueriesReturnSimilarResults(queryWithoutFilter,
                    queryWithDefaultMeasureFilter, testContext);
        }
    }


    public void testWithTimeDimension() {
        TestContext testContext = TestContext.create(
            null, null,
            "<VirtualCube name=\"Sales vs Warehouse\">\n" +
                "<VirtualCubeDimension name=\"Time\"/>\n" +
                "<VirtualCubeDimension name=\"Product\"/>\n" +
                "<VirtualCubeMeasure cubeName=\"Warehouse\" name=\"[Measures].[Warehouse Sales]\"/>\n" +
                "<VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Unit Sales]\"/>\n" +
                "</VirtualCube>",
            null, null, null);
        checkXxx(testContext);
    }


    private void checkXxx(TestContext testContext) {
        // I do not know/believe that the return values are correct.
        testContext.assertQueryReturns(
            "select\n" +
            "{ [Measures].[Warehouse Sales], [Measures].[Unit Sales] }\n" +
            "ON COLUMNS,\n" +
            "{[Product].[All Products]}\n" +
            "ON ROWS\n" +
            "from [Sales vs Warehouse]",
            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Warehouse Sales]}\n" +
                "{[Measures].[Unit Sales]}\n" +
                "Axis #2:\n" +
                "{[Product].[All Products]}\n" +
                "Row #0: 196,770.888\n" +
                "Row #0: 266,773\n"));
    }

    /**
     * verifies the cartesion join sql does not appear
     */
    public void testCartesionJoinSqlDoesNotOccur() {
    	
    	// only test on mysql dialect
    	if (!getTestContext().getDialect().isMySQL()) {
    		return;
    	}
    	
    	String mdxQuery = 
    		"select " +
    		"{ [measures].[store sales], [measures].[warehouse sales] } on 0, " +
  		  	"non empty { [product].[product family].members } on 1 " +
  		  	"from [warehouse and sales] " +
  		  	"where [store].[all stores].[usa].[or]";
    	
    	String expectedSql = 
  			"select "+
  			"`product_class`.`product_family` as `c0` " +
  			"from " +
  			"`product` as `product`, " +
  			"`product_class` as `product_class`, " +
  			"`inventory_fact_1997` as `inventory_fact_1997` " +
  			"where " +
  			"`product`.`product_class_id` = `product_class`.`product_class_id` " +
  			"and `inventory_fact_1997`.`product_id` = `product`.`product_id` " +
  			"group by " +
  			"`product_class`.`product_family` " +
  			"union " +
  			"select " +
  			"`product_class`.`product_family` as `c0` " +
  			"from " +
  			"`product` as `product`, " +
  			"`product_class` as `product_class`, " +
  			"`sales_fact_1997` as `sales_fact_1997`, " +
  			"`store` as `store` " +
  			"where " +
  			"`product`.`product_class_id` = `product_class`.`product_class_id` " +
  			"and `sales_fact_1997`.`product_id` = `product`.`product_id` " +
  			"and `sales_fact_1997`.`store_id` = `store`.`store_id` " +
  			"and `store`.`store_state` = 'OR' " +
  			"group by " +
  			"`product_class`.`product_family` " +
  			"order by " +
  			"ISNULL(1), 1 ASC";
    	
    	SqlPattern[] patterns =
        new SqlPattern[] {
            new SqlPattern(SqlPattern.Dialect.MYSQL, expectedSql, expectedSql)
        };

    	assertQuerySql(mdxQuery, patterns, true);
    }

    public void testCartesianJoin() {
      // these examples caused cartesian joins to occur, a fix in SqlTupleReader
    	// was made and now these queries run normally.
      TestContext testContext = createContextWithNonDefaultAllMember();

      testContext.assertQueryReturns(
      		"select " +
      		"{ [measures].[store sales], [measures].[warehouse sales] } on 0, " +
      		"non empty { [product].[product family].members } on 1 " +
      		"from [warehouse and sales] " +
      		"where [store].[all stores].[usa].[or]",
        	fold(
            	"Axis #0:\n" +
            	"{[Store].[All Stores].[USA].[OR]}\n" +
            	"Axis #1:\n" +
            	"{[Measures].[Store Sales]}\n" +
            	"{[Measures].[Warehouse Sales]}\n" +
            	"Axis #2:\n" +
            	"{[Product].[All Products].[Drink]}\n" +
            	"{[Product].[All Products].[Food]}\n" +
            	"{[Product].[All Products].[Non-Consumable]}\n" +
            	"Row #0: 12,137.29\n" +
            	"Row #0: 3,986.32\n" +
            	"Row #1: 102,564.67\n" +
            	"Row #1: 26,496.483\n" +
            	"Row #2: 27,575.11\n" +
            	"Row #2: 8,352.25\n"));
      
      testContext.assertQueryReturns(
      		"select " +
      		"{ [measures].[warehouse sales], [measures].[store sales] } on 0, " +
      		"non empty { [product].[product family].members } on 1 " +
      		"from [warehouse and sales] " +
      		"where [store].[all stores].[usa].[or]",
        	fold(
            	"Axis #0:\n" +
            	"{[Store].[All Stores].[USA].[OR]}\n" +
            	"Axis #1:\n" +
            	"{[Measures].[Warehouse Sales]}\n" +
            	"{[Measures].[Store Sales]}\n" +            	
            	"Axis #2:\n" +
            	"{[Product].[All Products].[Drink]}\n" +
            	"{[Product].[All Products].[Food]}\n" +
            	"{[Product].[All Products].[Non-Consumable]}\n" +
            	"Row #0: 3,986.32\n" +
            	"Row #0: 12,137.29\n" +
            	"Row #1: 26,496.483\n" +
            	"Row #1: 102,564.67\n" +
            	"Row #2: 8,352.25\n" +
            	"Row #2: 27,575.11\n"));
    }
    
    /**
     * Query a virtual cube that contains a non-conforming dimension that
     * does not have ALL as its default member.
     */
    public void testNonDefaultAllMember() {
        // Create a virtual cube with a non-conforming dimension (Warehouse)
        // that does not have ALL as its default member.
        TestContext testContext = createContextWithNonDefaultAllMember();

        testContext.assertQueryReturns(
            "select {[Warehouse].defaultMember} on columns, " +
            "{[Measures].[Warehouse Cost]} on rows from [Warehouse (Default USA)]",
            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Warehouse].[USA]}\n" +
                "Axis #2:\n" +
                "{[Measures].[Warehouse Cost]}\n" +
                "Row #0: 89,043.253\n"));

        // There is a value for [USA] -- because it is the default member and
        // the hierarchy has no all member -- but not for [USA].[CA].
        testContext.assertQueryReturns(
            "select {[Warehouse].defaultMember, [Warehouse].[USA].[CA]} on columns, " +
                "{[Measures].[Warehouse Cost], [Measures].[Sales Count]} on rows " +
                "from [Warehouse (Default USA) and Sales]",
            fold(
                "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Warehouse].[USA]}\n" +
                    "{[Warehouse].[USA].[CA]}\n" +
                    "Axis #2:\n" +
                    "{[Measures].[Warehouse Cost]}\n" +
                    "{[Measures].[Sales Count]}\n" +
                    "Row #0: 89,043.253\n" +
                    "Row #0: 25,789.086\n" +
                    "Row #1: 86,837\n" +
                    "Row #1: \n"));
    }

    public void testNonDefaultAllMember2() {
        TestContext testContext = createContextWithNonDefaultAllMember();
        testContext.assertQueryReturns(
            "select { measures.[unit sales] } on 0 \n" +
                "from [warehouse (Default USA) and Sales]",
            fold(
                "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Unit Sales]}\n" +
                    "Row #0: 266,773\n"));
    }

    /**
     * Creates a TestContext containing a cube
     * "Warehouse (Default USA) and Sales".
     */
    private TestContext createContextWithNonDefaultAllMember() {
        return TestContext.create(
            null,

            // Warehouse cube where the default member in the Warehouse
            // dimension is USA.
            "<Cube name=\"Warehouse (Default USA)\">\n" +
                "  <Table name=\"inventory_fact_1997\"/>\n" +
                "\n" +
                "  <DimensionUsage name=\"Time\" source=\"Time\" foreignKey=\"time_id\"/>\n" +
                "  <DimensionUsage name=\"Product\" source=\"Product\" foreignKey=\"product_id\"/>\n" +
                "  <DimensionUsage name=\"Store\" source=\"Store\" foreignKey=\"store_id\"/>\n" +
                "  <Dimension name=\"Warehouse\" foreignKey=\"warehouse_id\">\n" +
                "    <Hierarchy hasAll=\"false\" defaultMember=\"[USA]\" primaryKey=\"warehouse_id\"> \n" +
                "      <Table name=\"warehouse\"/>\n" +
                "      <Level name=\"Country\" column=\"warehouse_country\" uniqueMembers=\"true\"/>\n" +
                "      <Level name=\"State Province\" column=\"warehouse_state_province\"\n" +
                "          uniqueMembers=\"true\"/>\n" +
                "      <Level name=\"City\" column=\"warehouse_city\" uniqueMembers=\"false\"/>\n" +
                "      <Level name=\"Warehouse Name\" column=\"warehouse_name\" uniqueMembers=\"true\"/>\n" +
                "    </Hierarchy>\n" +
                "  </Dimension>\n" +
                "  <Measure name=\"Warehouse Cost\" column=\"warehouse_cost\" aggregator=\"sum\"/>\n" +
                "  <Measure name=\"Warehouse Sales\" column=\"warehouse_sales\" aggregator=\"sum\"/>\n" +
                "</Cube>",

            // Virtual cube based on [Warehouse (Default USA)]
            "<VirtualCube name=\"Warehouse (Default USA) and Sales\">\n" +
                "  <VirtualCubeDimension name=\"Product\"/>\n" +
                "  <VirtualCubeDimension name=\"Store\"/>\n" +
                "  <VirtualCubeDimension name=\"Time\"/>\n" +
                "  <VirtualCubeDimension cubeName=\"Warehouse (Default USA)\" name=\"Warehouse\"/>\n" +
                "  <VirtualCubeMeasure cubeName=\"Sales 2\" name=\"[Measures].[Sales Count]\"/>\n" +
                "  <VirtualCubeMeasure cubeName=\"Sales 2\" name=\"[Measures].[Store Cost]\"/>\n" +
                "  <VirtualCubeMeasure cubeName=\"Sales 2\" name=\"[Measures].[Store Sales]\"/>\n" +
                "  <VirtualCubeMeasure cubeName=\"Sales 2\" name=\"[Measures].[Unit Sales]\"/>\n" +
                "  <VirtualCubeMeasure cubeName=\"Warehouse\" name=\"[Measures].[Store Invoice]\"/>\n" +
                "  <VirtualCubeMeasure cubeName=\"Warehouse\" name=\"[Measures].[Supply Time]\"/>\n" +
                "  <VirtualCubeMeasure cubeName=\"Warehouse\" name=\"[Measures].[Units Ordered]\"/>\n" +
                "  <VirtualCubeMeasure cubeName=\"Warehouse\" name=\"[Measures].[Units Shipped]\"/>\n" +
                "  <VirtualCubeMeasure cubeName=\"Warehouse\" name=\"[Measures].[Warehouse Cost]\"/>\n" +
                "  <VirtualCubeMeasure cubeName=\"Warehouse\" name=\"[Measures].[Warehouse Sales]\"/>\n" +
                "</VirtualCube>",
            null, null, null);
    }

    public void testMemberVisibility() {
        TestContext testContext = TestContext.create(
            null, null,
            "<VirtualCube name=\"Warehouse and Sales Member Visibility\">\n" +
                "  <VirtualCubeDimension cubeName=\"Sales\" name=\"Customers\"/>\n" +
                "  <VirtualCubeDimension name=\"Time\"/>\n" +
                "  <VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Sales Count]\" visible=\"true\" />\n" +
                "  <VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Store Cost]\" visible=\"false\" />\n" +
                "  <VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Store Sales]\"/>\n" +
                "  <VirtualCubeMeasure cubeName=\"Warehouse\" name=\"[Measures].[Units Shipped]\" visible=\"false\" />\n" +
                "  <CalculatedMember name=\"Profit\" dimension=\"Measures\" visible=\"false\" >\n" +
                "    <Formula>[Measures].[Store Sales] - [Measures].[Store Cost]</Formula>\n" +
                "  </CalculatedMember>\n" +
                "</VirtualCube>",
            null, null, null);
        Result result = testContext.executeQuery(
            "select {[Measures].[Sales Count],\n" +
                " [Measures].[Store Cost],\n" +
                " [Measures].[Store Sales],\n" +
                " [Measures].[Units Shipped],\n" +
                " [Measures].[Profit]} on columns\n" +
                "from [Warehouse and Sales Member Visibility]");
        assertVisibility(result, 0, "Sales Count", true); // explicitly visible
        assertVisibility(result, 1, "Store Cost", false); // explicitly invisible
        assertVisibility(result, 2, "Store Sales", true); // visible by default
        assertVisibility(result, 3, "Units Shipped", false); // explicitly invisible
        assertVisibility(result, 4, "Profit", false); // explicitly invisible
    }

    private void assertVisibility(
        Result result, int ordinal, String expectedName,
        boolean expectedVisibility)
    {
        List<Position> columnPositions = result.getAxes()[0].getPositions();
        Member measure = columnPositions.get(ordinal).get(0);
        assertEquals(expectedName, measure.getName());
        assertEquals(
            Boolean.valueOf(expectedVisibility),
            measure.getPropertyValue(Property.VISIBLE.name));
    }

    /**
     * Test an expression for the format_string of a calculated member that
     * evaluates calculated members based on a virtual cube.  One cube has cache
     * turned on, the other cache turned off.
     *
     * <p>Since evaluation of the format_string used to happen after the
     * aggregate cache was cleared, this used to fail, this should be solved
     * with the caching of the format string.
     *
     * <p>Without caching of format string, the query returns green for all
     * styles.
     */
    public void testFormatStringExpressionCubeNoCache() {
        TestContext testContext = TestContext.create(
            null, null,
            "<Cube name=\"Warehouse No Cache\" cache=\"false\">\n" +
                "  <Table name=\"inventory_fact_1997\"/>\n" +
                "\n" +
                "  <DimensionUsage name=\"Time\" source=\"Time\" foreignKey=\"time_id\"/>\n" +
                "  <DimensionUsage name=\"Store\" source=\"Store\" foreignKey=\"store_id\"/>\n" +
                "  <Measure name=\"Units Shipped\" column=\"units_shipped\" aggregator=\"sum\" formatString=\"#.0\"/>\n" +
                "</Cube>\n" +
            "<VirtualCube name=\"Warehouse and Sales Format Expression Cube No Cache\">\n" +
                "  <VirtualCubeDimension name=\"Store\"/>\n" +
                "  <VirtualCubeDimension name=\"Time\"/>\n" +
                "  <VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Store Cost]\"/>\n" +
                "  <VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Store Sales]\"/>\n" +
                "  <VirtualCubeMeasure cubeName=\"Warehouse No Cache\" name=\"[Measures].[Units Shipped]\"/>\n" +
                "  <CalculatedMember name=\"Profit\" dimension=\"Measures\">\n" +
                "    <Formula>[Measures].[Store Sales] - [Measures].[Store Cost]</Formula>\n" +
                "  </CalculatedMember>\n" +
                "  <CalculatedMember name=\"Profit Per Unit Shipped\" dimension=\"Measures\">\n" +
                "    <Formula>[Measures].[Profit] / [Measures].[Units Shipped]</Formula>\n" +
                "    <CalculatedMemberProperty name=\"FORMAT_STRING\" expression=\"IIf(([Measures].[Profit Per Unit Shipped] > 2.0), '|0.#|style=green', '|0.#|style=red')\"/>\n" +
                "  </CalculatedMember>\n" +
                "</VirtualCube>",
            null, null, null);

        testContext.assertQueryReturns(
            "select {[Measures].[Profit Per Unit Shipped]} ON COLUMNS, " +
                "{[Store].[All Stores].[USA].[CA], [Store].[All Stores].[USA].[OR], [Store].[All Stores].[USA].[WA]} ON ROWS " +
                "from [Warehouse and Sales Format Expression Cube No Cache] " +
                "where [Time].[1997]",
            fold(
                "Axis #0:\n" +
                    "{[Time].[1997]}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Profit Per Unit Shipped]}\n" +
                    "Axis #2:\n" +
                    "{[Store].[All Stores].[USA].[CA]}\n" +
                    "{[Store].[All Stores].[USA].[OR]}\n" +
                    "{[Store].[All Stores].[USA].[WA]}\n" +
                    "Row #0: |1.6|style=red\n" +
                    "Row #1: |2.1|style=green\n" +
                    "Row #2: |1.5|style=red\n"));
    }

    public void testCalculatedMeasure()
    {
        // calculated measures reference measures defined in the base cube
        assertQueryReturns(
            "select\n" +
            "{[Measures].[Profit Growth], " +
            "[Measures].[Profit], " +
            "[Measures].[Average Warehouse Sale] }\n" +
            "ON COLUMNS\n" +
            "from [Warehouse and Sales]",
            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Profit Growth]}\n" +
                "{[Measures].[Profit]}\n" +
                "{[Measures].[Average Warehouse Sale]}\n" +
                "Row #0: 0.0%\n" +
                "Row #0: $339,610.90\n" +
                "Row #0: $2.21\n"));
    }

    public void testLostData()
    {
        assertQueryReturns(
            "select {[Time].Members} on columns,\n" +
                " {[Product].Children} on rows\n" +
                "from [Sales]",
            fold(
                "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Time].[1997]}\n" +
                    "{[Time].[1997].[Q1]}\n" +
                    "{[Time].[1997].[Q1].[1]}\n" +
                    "{[Time].[1997].[Q1].[2]}\n" +
                    "{[Time].[1997].[Q1].[3]}\n" +
                    "{[Time].[1997].[Q2]}\n" +
                    "{[Time].[1997].[Q2].[4]}\n" +
                    "{[Time].[1997].[Q2].[5]}\n" +
                    "{[Time].[1997].[Q2].[6]}\n" +
                    "{[Time].[1997].[Q3]}\n" +
                    "{[Time].[1997].[Q3].[7]}\n" +
                    "{[Time].[1997].[Q3].[8]}\n" +
                    "{[Time].[1997].[Q3].[9]}\n" +
                    "{[Time].[1997].[Q4]}\n" +
                    "{[Time].[1997].[Q4].[10]}\n" +
                    "{[Time].[1997].[Q4].[11]}\n" +
                    "{[Time].[1997].[Q4].[12]}\n" +
                    "{[Time].[1998]}\n" +
                    "{[Time].[1998].[Q1]}\n" +
                    "{[Time].[1998].[Q1].[1]}\n" +
                    "{[Time].[1998].[Q1].[2]}\n" +
                    "{[Time].[1998].[Q1].[3]}\n" +
                    "{[Time].[1998].[Q2]}\n" +
                    "{[Time].[1998].[Q2].[4]}\n" +
                    "{[Time].[1998].[Q2].[5]}\n" +
                    "{[Time].[1998].[Q2].[6]}\n" +
                    "{[Time].[1998].[Q3]}\n" +
                    "{[Time].[1998].[Q3].[7]}\n" +
                    "{[Time].[1998].[Q3].[8]}\n" +
                    "{[Time].[1998].[Q3].[9]}\n" +
                    "{[Time].[1998].[Q4]}\n" +
                    "{[Time].[1998].[Q4].[10]}\n" +
                    "{[Time].[1998].[Q4].[11]}\n" +
                    "{[Time].[1998].[Q4].[12]}\n" +
                    "Axis #2:\n" +
                    "{[Product].[All Products].[Drink]}\n" +
                    "{[Product].[All Products].[Food]}\n" +
                    "{[Product].[All Products].[Non-Consumable]}\n" +
                    "Row #0: 24,597\n" +
                    "Row #0: 5,976\n" +
                    "Row #0: 1,910\n" +
                    "Row #0: 1,951\n" +
                    "Row #0: 2,115\n" +
                    "Row #0: 5,895\n" +
                    "Row #0: 1,948\n" +
                    "Row #0: 2,039\n" +
                    "Row #0: 1,908\n" +
                    "Row #0: 6,065\n" +
                    "Row #0: 2,205\n" +
                    "Row #0: 1,921\n" +
                    "Row #0: 1,939\n" +
                    "Row #0: 6,661\n" +
                    "Row #0: 1,898\n" +
                    "Row #0: 2,344\n" +
                    "Row #0: 2,419\n" +
                    "Row #0: \n" +
                    "Row #0: \n" +
                    "Row #0: \n" +
                    "Row #0: \n" +
                    "Row #0: \n" +
                    "Row #0: \n" +
                    "Row #0: \n" +
                    "Row #0: \n" +
                    "Row #0: \n" +
                    "Row #0: \n" +
                    "Row #0: \n" +
                    "Row #0: \n" +
                    "Row #0: \n" +
                    "Row #0: \n" +
                    "Row #0: \n" +
                    "Row #0: \n" +
                    "Row #0: \n" +
                    "Row #1: 191,940\n" +
                    "Row #1: 47,809\n" +
                    "Row #1: 15,604\n" +
                    "Row #1: 15,142\n" +
                    "Row #1: 17,063\n" +
                    "Row #1: 44,825\n" +
                    "Row #1: 14,393\n" +
                    "Row #1: 15,055\n" +
                    "Row #1: 15,377\n" +
                    "Row #1: 47,440\n" +
                    "Row #1: 17,036\n" +
                    "Row #1: 15,741\n" +
                    "Row #1: 14,663\n" +
                    "Row #1: 51,866\n" +
                    "Row #1: 14,232\n" +
                    "Row #1: 18,278\n" +
                    "Row #1: 19,356\n" +
                    "Row #1: \n" +
                    "Row #1: \n" +
                    "Row #1: \n" +
                    "Row #1: \n" +
                    "Row #1: \n" +
                    "Row #1: \n" +
                    "Row #1: \n" +
                    "Row #1: \n" +
                    "Row #1: \n" +
                    "Row #1: \n" +
                    "Row #1: \n" +
                    "Row #1: \n" +
                    "Row #1: \n" +
                    "Row #1: \n" +
                    "Row #1: \n" +
                    "Row #1: \n" +
                    "Row #1: \n" +
                    "Row #2: 50,236\n" +
                    "Row #2: 12,506\n" +
                    "Row #2: 4,114\n" +
                    "Row #2: 3,864\n" +
                    "Row #2: 4,528\n" +
                    "Row #2: 11,890\n" +
                    "Row #2: 3,838\n" +
                    "Row #2: 3,987\n" +
                    "Row #2: 4,065\n" +
                    "Row #2: 12,343\n" +
                    "Row #2: 4,522\n" +
                    "Row #2: 4,035\n" +
                    "Row #2: 3,786\n" +
                    "Row #2: 13,497\n" +
                    "Row #2: 3,828\n" +
                    "Row #2: 4,648\n" +
                    "Row #2: 5,021\n" +
                    "Row #2: \n" +
                    "Row #2: \n" +
                    "Row #2: \n" +
                    "Row #2: \n" +
                    "Row #2: \n" +
                    "Row #2: \n" +
                    "Row #2: \n" +
                    "Row #2: \n" +
                    "Row #2: \n" +
                    "Row #2: \n" +
                    "Row #2: \n" +
                    "Row #2: \n" +
                    "Row #2: \n" +
                    "Row #2: \n" +
                    "Row #2: \n" +
                    "Row #2: \n" +
                    "Row #2: \n"));
        assertQueryReturns(
                "select\n" +
                " {[Measures].[Unit Sales]} on 0,\n" +
                " {[Product].Children} on 1\n" +
                "from [Warehouse and Sales]",
                fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Unit Sales]}\n" +
                "Axis #2:\n" +
                "{[Product].[All Products].[Drink]}\n" +
                "{[Product].[All Products].[Food]}\n" +
                "{[Product].[All Products].[Non-Consumable]}\n" +
                "Row #0: 24,597\n" +
                "Row #1: 191,940\n" +
                "Row #2: 50,236\n"));
    }

    /**
     * Tests a calc measure which combines a measures from the Sales cube with a
     * measures from the Warehouse cube.
     */
    public void testCalculatedMeasureAcrossCubes()
    {
        assertQueryReturns(
            "with member [Measures].[Shipped per Ordered] as ' [Measures].[Units Shipped] / [Measures].[Unit Sales] ', format_string='#.00%'\n" +
                " member [Measures].[Profit per Unit Shipped] as ' [Measures].[Profit] / [Measures].[Units Shipped] '\n" +
                "select\n" +
                " {[Measures].[Unit Sales], \n" +
                "  [Measures].[Units Shipped],\n" +
                "  [Measures].[Shipped per Ordered],\n" +
                "  [Measures].[Profit per Unit Shipped]} on 0,\n" +
                " NON EMPTY Crossjoin([Product].Children, [Time].[1997].Children) on 1\n" +
                "from [Warehouse and Sales]",
            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Unit Sales]}\n" +
                "{[Measures].[Units Shipped]}\n" +
                "{[Measures].[Shipped per Ordered]}\n" +
                "{[Measures].[Profit per Unit Shipped]}\n" +
                "Axis #2:\n" +
                "{[Product].[All Products].[Drink], [Time].[1997].[Q1]}\n" +
                "{[Product].[All Products].[Drink], [Time].[1997].[Q2]}\n" +
                "{[Product].[All Products].[Drink], [Time].[1997].[Q3]}\n" +
                "{[Product].[All Products].[Drink], [Time].[1997].[Q4]}\n" +
                "{[Product].[All Products].[Food], [Time].[1997].[Q1]}\n" +
                "{[Product].[All Products].[Food], [Time].[1997].[Q2]}\n" +
                "{[Product].[All Products].[Food], [Time].[1997].[Q3]}\n" +
                "{[Product].[All Products].[Food], [Time].[1997].[Q4]}\n" +
                "{[Product].[All Products].[Non-Consumable], [Time].[1997].[Q1]}\n" +
                "{[Product].[All Products].[Non-Consumable], [Time].[1997].[Q2]}\n" +
                "{[Product].[All Products].[Non-Consumable], [Time].[1997].[Q3]}\n" +
                "{[Product].[All Products].[Non-Consumable], [Time].[1997].[Q4]}\n" +
                "Row #0: 5,976\n" +
                "Row #0: 4637.0\n" +
                "Row #0: 77.59%\n" +
                "Row #0: $1.50\n" +
                "Row #1: 5,895\n" +
                "Row #1: 4501.0\n" +
                "Row #1: 76.35%\n" +
                "Row #1: $1.60\n" +
                "Row #2: 6,065\n" +
                "Row #2: 6258.0\n" +
                "Row #2: 103.18%\n" +
                "Row #2: $1.15\n" +
                "Row #3: 6,661\n" +
                "Row #3: 5802.0\n" +
                "Row #3: 87.10%\n" +
                "Row #3: $1.38\n" +
                "Row #4: 47,809\n" +
                "Row #4: 37153.0\n" +
                "Row #4: 77.71%\n" +
                "Row #4: $1.64\n" +
                "Row #5: 44,825\n" +
                "Row #5: 35459.0\n" +
                "Row #5: 79.11%\n" +
                "Row #5: $1.62\n" +
                "Row #6: 47,440\n" +
                "Row #6: 41545.0\n" +
                "Row #6: 87.57%\n" +
                "Row #6: $1.47\n" +
                "Row #7: 51,866\n" +
                "Row #7: 34706.0\n" +
                "Row #7: 66.91%\n" +
                "Row #7: $1.91\n" +
                "Row #8: 12,506\n" +
                "Row #8: 9161.0\n" +
                "Row #8: 73.25%\n" +
                "Row #8: $1.76\n" +
                "Row #9: 11,890\n" +
                "Row #9: 9227.0\n" +
                "Row #9: 77.60%\n" +
                "Row #9: $1.65\n" +
                "Row #10: 12,343\n" +
                "Row #10: 9986.0\n" +
                "Row #10: 80.90%\n" +
                "Row #10: $1.59\n" +
                "Row #11: 13,497\n" +
                "Row #11: 9291.0\n" +
                "Row #11: 68.84%\n" +
                "Row #11: $1.86\n"));
    }

    /**
     * Tests a calc member defined in the cube.
     */
    public void testCalculatedMemberInSchema() {
        TestContext testContext =
            TestContext.createSubstitutingCube(
                "Warehouse and Sales",
                null,
                "  <CalculatedMember name=\"Shipped per Ordered\" dimension=\"Measures\">\n" +
                    "    <Formula>[Measures].[Units Shipped] / [Measures].[Unit Sales]</Formula>\n" +
                    "    <CalculatedMemberProperty name=\"FORMAT_STRING\" value=\"#.0%\"/>\n" +
                    "  </CalculatedMember>\n");
        testContext.assertQueryReturns(
            "select\n" +
                " {[Measures].[Unit Sales], \n" +
                "  [Measures].[Shipped per Ordered]} on 0,\n" +
                " NON EMPTY Crossjoin([Product].Children, [Time].[1997].Children) on 1\n" +
                "from [Warehouse and Sales]",
            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Unit Sales]}\n" +
                "{[Measures].[Shipped per Ordered]}\n" +
                "Axis #2:\n" +
                "{[Product].[All Products].[Drink], [Time].[1997].[Q1]}\n" +
                "{[Product].[All Products].[Drink], [Time].[1997].[Q2]}\n" +
                "{[Product].[All Products].[Drink], [Time].[1997].[Q3]}\n" +
                "{[Product].[All Products].[Drink], [Time].[1997].[Q4]}\n" +
                "{[Product].[All Products].[Food], [Time].[1997].[Q1]}\n" +
                "{[Product].[All Products].[Food], [Time].[1997].[Q2]}\n" +
                "{[Product].[All Products].[Food], [Time].[1997].[Q3]}\n" +
                "{[Product].[All Products].[Food], [Time].[1997].[Q4]}\n" +
                "{[Product].[All Products].[Non-Consumable], [Time].[1997].[Q1]}\n" +
                "{[Product].[All Products].[Non-Consumable], [Time].[1997].[Q2]}\n" +
                "{[Product].[All Products].[Non-Consumable], [Time].[1997].[Q3]}\n" +
                "{[Product].[All Products].[Non-Consumable], [Time].[1997].[Q4]}\n" +
                "Row #0: 5,976\n" +
                "Row #0: 77.6%\n" +
                "Row #1: 5,895\n" +
                "Row #1: 76.4%\n" +
                "Row #2: 6,065\n" +
                "Row #2: 103.2%\n" +
                "Row #3: 6,661\n" +
                "Row #3: 87.1%\n" +
                "Row #4: 47,809\n" +
                "Row #4: 77.7%\n" +
                "Row #5: 44,825\n" +
                "Row #5: 79.1%\n" +
                "Row #6: 47,440\n" +
                "Row #6: 87.6%\n" +
                "Row #7: 51,866\n" +
                "Row #7: 66.9%\n" +
                "Row #8: 12,506\n" +
                "Row #8: 73.3%\n" +
                "Row #9: 11,890\n" +
                "Row #9: 77.6%\n" +
                "Row #10: 12,343\n" +
                "Row #10: 80.9%\n" +
                "Row #11: 13,497\n" +
                "Row #11: 68.8%\n"));
    }

    public void testAllMeasureMembers()
    {
        // result should exclude measures that are not explicitly defined
        // in the virtual cube (e.g., [Profit last Period])
        assertQueryReturns(
            "select\n" +
            "{[Measures].allMembers} on columns\n" +
            "from [Warehouse and Sales]",
            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Sales Count]}\n" +
                "{[Measures].[Store Cost]}\n" +
                "{[Measures].[Store Sales]}\n" +
                "{[Measures].[Unit Sales]}\n" +
                "{[Measures].[Store Invoice]}\n" +
                "{[Measures].[Supply Time]}\n" +
                "{[Measures].[Units Ordered]}\n" +
                "{[Measures].[Units Shipped]}\n" +
                "{[Measures].[Warehouse Cost]}\n" +
                "{[Measures].[Warehouse Profit]}\n" +
                "{[Measures].[Warehouse Sales]}\n" +
                "{[Measures].[Profit]}\n" +
                "{[Measures].[Profit Growth]}\n" +
                "{[Measures].[Average Warehouse Sale]}\n" +
                "{[Measures].[Profit Per Unit Shipped]}\n" +
                "Row #0: 86,837\n" +
                "Row #0: 225,627.23\n" +
                "Row #0: 565,238.13\n" +
                "Row #0: 266,773\n" +
                "Row #0: 102,278.409\n" +
                "Row #0: 10,425\n" +
                "Row #0: 227238.0\n" +
                "Row #0: 207726.0\n" +
                "Row #0: 89,043.253\n" +
                "Row #0: 107,727.635\n" +
                "Row #0: 196,770.888\n" +
                "Row #0: $339,610.90\n" +
                "Row #0: 0.0%\n" +
                "Row #0: $2.21\n" +
                "Row #0: $1.63\n"));
    }

    /**
     * Test a virtual cube where one of the dimensions contains an
     * ordinalColumn property
     */
    public void testOrdinalColumn()
    {
        TestContext testContext = TestContext.create(
            null, null,
            "<VirtualCube name=\"Sales vs HR\">\n" +
                "<VirtualCubeDimension name=\"Store\"/>\n" +
                "<VirtualCubeDimension cubeName=\"HR\" name=\"Position\"/>\n" +
                "<VirtualCubeMeasure cubeName=\"HR\" name=\"[Measures].[Org Salary]\"/>\n" +
                "</VirtualCube>",
            null, null, null);
        testContext.assertQueryReturns(
            "select {[Measures].[Org Salary]} on columns, " +
            "non empty " +
            "crossjoin([Store].[Store Country].members, [Position].[Store Management].children) " +
            "on rows from [Sales vs HR]",
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Org Salary]}\n" +
                "Axis #2:\n" +
                "{[Store].[All Stores].[Canada], [Position].[All Position].[Store Management].[Store Manager]}\n" +
                "{[Store].[All Stores].[Canada], [Position].[All Position].[Store Management].[Store Assistant Manager]}\n" +
                "{[Store].[All Stores].[Canada], [Position].[All Position].[Store Management].[Store Shift Supervisor]}\n" +
                "{[Store].[All Stores].[Mexico], [Position].[All Position].[Store Management].[Store Manager]}\n" +
                "{[Store].[All Stores].[Mexico], [Position].[All Position].[Store Management].[Store Assistant Manager]}\n" +
                "{[Store].[All Stores].[Mexico], [Position].[All Position].[Store Management].[Store Shift Supervisor]}\n" +
                "{[Store].[All Stores].[USA], [Position].[All Position].[Store Management].[Store Manager]}\n" +
                "{[Store].[All Stores].[USA], [Position].[All Position].[Store Management].[Store Assistant Manager]}\n" +
                "{[Store].[All Stores].[USA], [Position].[All Position].[Store Management].[Store Shift Supervisor]}\n" +
                "Row #0: $462.86\n" +
                "Row #1: $394.29\n" +
                "Row #2: $565.71\n" +
                "Row #3: $13,254.55\n" +
                "Row #4: $11,443.64\n" +
                "Row #5: $17,705.46\n" +
                "Row #6: $4,069.80\n" +
                "Row #7: $3,417.72\n" +
                "Row #8: $5,145.96\n"));
    }

    public void testDefaultMeasureProperty() {
         TestContext testContext = TestContext.create(
            null, null,
            "<VirtualCube name=\"Sales vs Warehouse\" defaultMeasure=\"Unit Sales\">\n" +
                "<VirtualCubeDimension name=\"Product\"/>\n" +
                "<VirtualCubeMeasure cubeName=\"Warehouse\" " +
                    "name=\"[Measures].[Warehouse Sales]\"/>\n" +
                "<VirtualCubeMeasure cubeName=\"Sales\" " +
                    "name=\"[Measures].[Unit Sales]\"/>\n" +
                "<VirtualCubeMeasure cubeName=\"Sales\" " +
                    "name=\"[Measures].[Profit]\"/>\n" +
                "</VirtualCube>",
            null, null, null);

        String queryWithoutFilter = "select"+
                " from [Sales vs Warehouse]";
        String queryWithDeflaultMeasureFilter = "select "+
                "from [Sales vs Warehouse] where measures.[Unit Sales]";
        assertQueriesReturnSimilarResults(queryWithoutFilter,
                queryWithDeflaultMeasureFilter, testContext);
    }

    /**
     * Checks that native set caching considers base cubes in the cache key.
     * Native sets referencing different base cubes do not share the cached result.
     */
    public void testNativeSetCaching() {
        /*
         * Only need to run this against one db to verify caching
         * behavior is correct. 
         */ 
        if (!getTestContext().getDialect().isDerby()) {
            return;
        }
        
        if (!MondrianProperties.instance().EnableNativeCrossJoin.get() &&
            !MondrianProperties.instance().EnableNativeNonEmpty.get()) {
            // Only run the tests if either native CrossJoin or native NonEmpty
            // is enabled.
            return;
        }

        String query1 =
            "With " +
            "Set [*NATIVE_CJ_SET] as 'NonEmptyCrossJoin([Product].[Product Family].Members, [Store].[Store Country].Members)' " +
            "Select " +
            "{[Store Sales]} on columns, " +
            "Non Empty Generate([*NATIVE_CJ_SET], {([Product].CurrentMember,[Store].CurrentMember)}) on rows " +
            "From [Warehouse and Sales]";

        String query2 =
            "With " +
            "Set [*NATIVE_CJ_SET] as 'NonEmptyCrossJoin([Product].[Product Family].Members, [Store].[Store Country].Members)' " +
            "Select " +
            "{[Warehouse Sales]} on columns, " +
            "Non Empty Generate([*NATIVE_CJ_SET], {([Product].CurrentMember,[Store].CurrentMember)}) on rows " +
            "From [Warehouse and Sales]";


        String derbyNecjSql1, derbyNecjSql2;

        if (MondrianProperties.instance().EnableNativeCrossJoin.get()) {
            derbyNecjSql1 =
                "select " +
                "\"product_class\".\"product_family\", " +
                "\"store\".\"store_country\" " +
                "from " +
                "\"product\" as \"product\", " +
                "\"product_class\" as \"product_class\", " +
                "\"sales_fact_1997\" as \"sales_fact_1997\", " +
                "\"store\" as \"store\" " +
                "where " +
                "\"product\".\"product_class_id\" = \"product_class\".\"product_class_id\" " +
                "and \"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\" " +
                "and \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" " +
                "group by \"product_class\".\"product_family\", \"store\".\"store_country\" " +
                "order by 1 ASC, 2 ASC";


            derbyNecjSql2 =
                "select " +
                "\"product_class\".\"product_family\", " +
                "\"store\".\"store_country\" " +
                "from " +
                "\"product\" as \"product\", " +
                "\"product_class\" as \"product_class\", " +
                "\"inventory_fact_1997\" as \"inventory_fact_1997\", " +
                "\"store\" as \"store\" " +
                "where " +
                "\"product\".\"product_class_id\" = \"product_class\".\"product_class_id\" " +
                "and \"inventory_fact_1997\".\"product_id\" = \"product\".\"product_id\" " +
                "and \"inventory_fact_1997\".\"store_id\" = \"store\".\"store_id\" " +
                "group by \"product_class\".\"product_family\", \"store\".\"store_country\" " +
                "order by 1 ASC, 2 ASC";
        } else {
            // NECJ is truend off so native NECJ SQL will not be generated;
            // however, because the NECJ set should not find match in the cache,
            // each NECJ input will still be joined with the correct
            // fact table if NonEmpty condition is natively evaluated.
            derbyNecjSql1 =
                "select " +
                "\"store\".\"store_country\" " +
                "from " +
                "\"store\" as \"store\", " +
                "\"sales_fact_1997\" as \"sales_fact_1997\" " +
                "where " +
                "\"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" " +
                "group by \"store\".\"store_country\" " +
                "order by 1 ASC";

            derbyNecjSql2 =
                "select " +
                "\"store\".\"store_country\" " +
                "from " +
                "\"store\" as \"store\", " +
                "\"inventory_fact_1997\" as \"inventory_fact_1997\" " +
                "where " +
                "\"inventory_fact_1997\".\"store_id\" = \"store\".\"store_id\" " +
                "group by \"store\".\"store_country\" " +
                "order by 1 ASC";
        }

        SqlPattern[] patterns1 =
            new SqlPattern[] {
                new SqlPattern(SqlPattern.Dialect.DERBY, derbyNecjSql1, derbyNecjSql1)
            };

        SqlPattern[] patterns2 =
            new SqlPattern[] {
                new SqlPattern(SqlPattern.Dialect.DERBY, derbyNecjSql2, derbyNecjSql2)
            };

        // Run query 1 with cleared cache;
        // Make sure NECJ 1 is evaluated natively.
        assertQuerySql(query1, patterns1, true);

        // Now run query 2 with warm cache;
        // Make sure NECJ 2 does not reuse the cache result from NECJ 1, and
        // NECJ 2 is evaluated natively.
        assertQuerySql(query2, patterns2, false);
    }

    /**
     * Test for bug 1778358, "cube.getStar() throws NullPointerException".
     * Happens when you aggregate distinct-count measures in a virtual cube.
     */
    public void testBug1778358() {
        final TestContext testContext =
            TestContext.create(null, null,
                "<VirtualCube name=\"Warehouse and Sales2\" defaultMeasure=\"Store Sales\">\n"
                    + "  <VirtualCubeDimension cubeName=\"Sales\" name=\"Customers\"/>\n"
                    + "  <VirtualCubeDimension name=\"Time\"/>\n"
                    + "  <VirtualCubeDimension cubeName=\"Warehouse\" name=\"Warehouse\"/>\n"
                    + "  <VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Customer Count]\"/>\n"
                    + "  <VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Store Sales]\"/>\n"
                    + "</VirtualCube>",
                null, null, null);
        /*
         * This test case does not actually reject the dimension constraint from an 
         * unrelated base cube. The reason is that the constraint contains an AllLevel
         * member. Even though semantically constraining Cells using an non-existent 
         * dimension perhaps does not make sense; however, in the case where the constraint
         * contains AllLevel member, the constraint can be considered "always true".
         * 
         * See the next test case for a constraint that does not contain AllLevel member
         * and hence cannot be satisfied. The cell should be empty.
         */
        testContext.assertQueryReturns(
            "with member [Warehouse].[x] as 'Aggregate([Warehouse].members)'\n"
                + "member [Measures].[foo] AS '([Warehouse].[x],[Measures].[Customer Count])'\n"
                + "select {[Measures].[foo]} on 0 from [Warehouse And Sales2]",
            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[foo]}\n" +
                "Row #0: 5,581\n"));
    }

    
    public void testBug1778358a() {
        final TestContext testContext =
            TestContext.create(null, null,
                "<VirtualCube name=\"Warehouse and Sales2\" defaultMeasure=\"Store Sales\">\n"
                    + "  <VirtualCubeDimension cubeName=\"Sales\" name=\"Customers\"/>\n"
                    + "  <VirtualCubeDimension name=\"Time\"/>\n"
                    + "  <VirtualCubeDimension cubeName=\"Warehouse\" name=\"Warehouse\"/>\n"
                    + "  <VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Customer Count]\"/>\n"
                    + "  <VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Store Sales]\"/>\n"
                    + "</VirtualCube>",
                null, null, null);
        testContext.assertQueryReturns(
            "with member [Warehouse].[x] as 'Aggregate({[Warehouse].[Canada], [Warehouse].[USA]})'\n"
                + "member [Measures].[foo] AS '([Warehouse].[x],[Measures].[Customer Count])'\n"
                + "select {[Measures].[foo]} on 0 from [Warehouse And Sales2]",
            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[foo]}\n" +
                "Row #0: \n"));
    }

    /**
     * Testcase for bug 1835125, "Caption is not set on
     * RolapVirtualCubeMesure".
     */
    public void testVirtualCubeMeasureCaption() {
        TestContext testContext = TestContext.create(
            null,
            "<Cube name=\"TestStore\">\n" +
                "  <Table name=\"store\"/>\n" +
                "  <Dimension name=\"HCB\" caption=\"Has coffee bar caption\">\n" +
                "    <Hierarchy hasAll=\"true\">\n" +
                "      <Level name=\"Has coffee bar\" column=\"coffee_bar\" uniqueMembers=\"true\"\n" +
                "          type=\"Boolean\"/>\n" +
                "    </Hierarchy>\n" +
                "  </Dimension>\n" +
                "  <Measure name=\"Store Sqft\" caption=\"Store Sqft Caption\" column=\"store_sqft\" aggregator=\"sum\" formatString=\"#,###\"/>\n" +
                "</Cube>\n",

            "<VirtualCube name=\"VirtualTestStore\">\n" +
                "  <VirtualCubeDimension cubeName=\"TestStore\" name=\"HCB\"/>\n" +
                "  <VirtualCubeMeasure   cubeName=\"TestStore\" name=\"[Measures].[Store Sqft]\"/>\n" +
                "</VirtualCube>",
            null, null, null);

        Result result = testContext.executeQuery(
            "select {[Measures].[Store Sqft]} ON COLUMNS," +
                "{[HCB]} ON ROWS " +
                "from [VirtualTestStore]");

        Axis[] axes = result.getAxes();
        List<Position> positions = axes[0].getPositions();
        Member m0 = positions.get(0).get(0);
        String caption = m0.getCaption();
        assertEquals("Store Sqft Caption", caption);
    }
}

// End VirtualCubeTest.java
