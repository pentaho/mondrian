/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2003-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.test.FoodMartTestCase;
import mondrian.test.TestContext;
import java.util.List;

/**
 * <code>VirtualCubeTest</code> shows virtual cube tests.
 *
 * @author remberson
 * @since Feb 14, 2003
 * @version $Id$
 */
public class VirtualCubeTest extends FoodMartTestCase {
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
            null, null);
        checkXxx(testContext);
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
            null, null);
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
            null, null);
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
            null, null);
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
            null, null);

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
}

// End VirtualCubeTest.java
