/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2003-2005 Julian Hyde
// Copyright (C) 2005-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.spi.Dialect;
import mondrian.test.SqlPattern;
import mondrian.test.TestContext;
import mondrian.util.Bug;

import java.util.List;

/**
 * Unit tests for virtual cubes.
 *
 * @author remberson
 * @since Feb 14, 2003
 */
public class VirtualCubeTest extends BatchTestCase {
    /**
     * Creates an anonymous VirtualCubeTest.
     */
    public VirtualCubeTest() {
    }

    /**
     * Creates a VirtualCubeTest.
     * @param name Test case name
     */
    public VirtualCubeTest(String name) {
        super(name);
    }

    public TestContext getTestContext() {
        return TestContext.instance().legacy();
    }

    /**
     * Test case for bug <a href="http://jira.pentaho.com/browse/MONDRIAN-163">
     * MONDRIAN-163, "VirtualCube SegmentArrayQuerySpec.addMeasure assert"</a>.
     */
    public void testNoTimeDimension() {
        TestContext testContext = getTestContext().create(
            null,
            null,
            "<VirtualCube name=\"Sales vs Warehouse\">\n"
            + "<VirtualCubeDimension name=\"Product\"/>\n"
            + (Bug.VirtualCubeConversionMissesHiddenFixed
                ? ""
                : "  <VirtualCubeDimension cubeName=\"Sales\" name=\"Time\"/>\n"
                + "  <VirtualCubeDimension name=\"Warehouse\"/>\n")
            + "<VirtualCubeMeasure cubeName=\"Warehouse\" name=\"[Measures].[Warehouse Sales]\"/>\n"
            + "<VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Unit Sales]\"/>\n"
            + "</VirtualCube>",
            null,
            null,
            null);
        checkXxx(testContext);
    }

    public void testCalculatedMeasureAsDefaultMeasureInVC() {
        TestContext testContext = getTestContext().create(
            null,
            null,
            "<VirtualCube name=\"Sales vs Warehouse\" defaultMeasure=\"Profit\">\n"
            + "<VirtualCubeDimension name=\"Product\"/>\n"
            + "<VirtualCubeDimension cubeName=\"Sales\" name=\"Time\"/>\n"
            + (Bug.VirtualCubeConversionMissesHiddenFixed
                ? ""
                : "<VirtualCubeDimension name=\"Warehouse\"/>\n")
            + "<VirtualCubeMeasure cubeName=\"Warehouse\" name=\"[Measures].[Warehouse Sales]\"/>\n"
            + "<VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Unit Sales]\"/>\n"
            + "<VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Profit]\"/>\n"
            + "<VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Store Sales]\"/>\n"
            + "<VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Store Cost]\"/>\n"
            + "</VirtualCube>",
            null,
            null,
            null);
        String query1 = "select from [Sales vs Warehouse]";
        String query2 =
            "select from [Sales vs Warehouse] where measures.profit";
        assertQueriesReturnSimilarResults(query1, query2, testContext);
    }

    public void testDefaultMeasureInVCForIncorrectMeasureName() {
        TestContext testContext = getTestContext().create(
            null,
            null,
            "<VirtualCube name=\"Sales vs Warehouse\" defaultMeasure=\"Profit Error\">\n"
            + "<VirtualCubeDimension name=\"Product\"/>\n"
            + (Bug.VirtualCubeConversionMissesHiddenFixed
                ? ""
                : "<VirtualCubeDimension name=\"Warehouse\"/>\n")
            + "<VirtualCubeDimension cubeName=\"Sales\" name=\"Time\"/>\n"
            + "<VirtualCubeMeasure cubeName=\"Warehouse\" name=\"[Measures].[Warehouse Sales]\"/>\n"
            + "<VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Unit Sales]\"/>\n"
            + "<VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Profit]\"/>\n"
            + "<VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Store Sales]\"/>\n"
            + "<VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Store Cost]\"/>\n"
            + "</VirtualCube>",
            null,
            null,
            null);
        testContext.assertQueryThrows(
            "select from [Sales vs Warehouse]",
            "Default measure 'Profit Error' not found");
    }

    public void testVirtualCubeMeasureInvalidCubeName() {
        TestContext testContext = getTestContext().create(
            null,
            null,
            "<VirtualCube name=\"Sales vs Warehouse\">\n"
            + "<VirtualCubeDimension name=\"Product\"/>\n"
            + "<VirtualCubeDimension cubeName=\"Sales\" name=\"Time\"/>\n"
            + "<VirtualCubeMeasure cubeName=\"Warehouse\" name=\"[Measures].[Warehouse Sales]\"/>\n"
            + "<VirtualCubeMeasure cubeName=\"Bad cube\" name=\"[Measures].[Unit Sales]\"/>\n"
            + "</VirtualCube>",
            null,
            null,
            null);
        testContext.assertQueryThrows(
            "select from [Sales vs Warehouse]",
            "Cube 'Bad cube' not found");
    }

    public void testDefaultMeasureInVCForCaseSensitivity() {
        TestContext testContext = getTestContext().create(
            null,
            null,
            "<VirtualCube name=\"Sales vs Warehouse\" defaultMeasure=\"PROFIT\">\n"
            + "<VirtualCubeDimension name=\"Product\"/>\n"
            + (Bug.VirtualCubeConversionMissesHiddenFixed
                ? ""
                : "  <VirtualCubeDimension cubeName=\"Sales\" name=\"Time\"/>\n"
                + "<VirtualCubeDimension name=\"Warehouse\"/>\n")
            + "<VirtualCubeMeasure cubeName=\"Warehouse\" name=\"[Measures].[Warehouse Sales]\"/>\n"
            + "<VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Unit Sales]\"/>\n"
            + "<VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Profit]\"/>\n"
            + "<VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Store Sales]\"/>\n"
            + "<VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Store Cost]\"/>\n"
            + "</VirtualCube>",
            null,
            null,
            null);
        String queryWithoutFilter = "select from [Sales vs Warehouse]";
        String queryWithDefaultMeasureFilter =
            "select from [Sales vs Warehouse] "
            + "where measures.[Profit]";

        if (MondrianProperties.instance().CaseSensitive.get()) {
            testContext.assertQueryThrows(
                "select from [Sales vs Warehouse]",
                "Default measure 'PROFIT' not found");
        } else {
            assertQueriesReturnSimilarResults(
                queryWithoutFilter, queryWithDefaultMeasureFilter, testContext);
        }
    }

    public void testWithTimeDimension() {
        TestContext testContext = getTestContext().create(
            null,
            null,
            "<VirtualCube name=\"Sales vs Warehouse\">\n"
            + "<VirtualCubeDimension name=\"Time\"/>\n"
            + "<VirtualCubeDimension name=\"Product\"/>\n"
            + "<VirtualCubeDimension name=\"Product\"/>\n"
            + (Bug.VirtualCubeConversionMissesHiddenFixed
                ? ""
                : "<VirtualCubeDimension name=\"Warehouse\"/>\n")
            + "<VirtualCubeMeasure cubeName=\"Warehouse\" name=\"[Measures].[Warehouse Sales]\"/>\n"
            + "<VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Unit Sales]\"/>\n"
            + "</VirtualCube>",
            null,
            null,
            null);
        checkXxx(testContext);
    }


    private void checkXxx(TestContext testContext) {
        // I do not know/believe that the return values are correct.
        testContext.assertQueryReturns(
            "select\n"
            + "{ [Measures].[Warehouse Sales], [Measures].[Unit Sales] }\n"
            + "ON COLUMNS,\n"
            + "{[Product].[Product].[All Products]}\n"
            + "ON ROWS\n"
            + "from [Sales vs Warehouse]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Warehouse Sales]}\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Product].[All Products]}\n"
            + "Row #0: 196,770.888\n"
            + "Row #0: 266,773\n");
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
            "select {[Warehouse].[Warehouse].defaultMember} on columns, "
            + "{[Measures].[Warehouse Cost]} on rows from [Warehouse (Default USA)]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Warehouse].[Warehouse].[USA]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Warehouse Cost]}\n"
            + "Row #0: 89,043.253\n");

        // There is a value for [USA] -- because it is the default member and
        // the hierarchy has no all member -- but not for [USA].[CA].
        testContext.assertQueryReturns(
            "select {[Warehouse].[Warehouse].defaultMember, [Warehouse].[Warehouse].[USA].[CA]} on columns, "
            + "{[Measures].[Warehouse Cost], [Measures].[Sales Count]} on rows "
            + "from [Warehouse (Default USA) and Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Warehouse].[Warehouse].[USA]}\n"
            + "{[Warehouse].[Warehouse].[USA].[CA]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Warehouse Cost]}\n"
            + "{[Measures].[Sales Count]}\n"
            + "Row #0: 89,043.253\n"
            + "Row #0: 25,789.086\n"
            + "Row #1: 86,837\n"
            + "Row #1: \n");
    }

    public void testNonDefaultAllMember2() {
        TestContext testContext = createContextWithNonDefaultAllMember();
        testContext.assertQueryReturns(
            "select { measures.[unit sales] } on 0 \n"
            + "from [warehouse (Default USA) and Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Row #0: 266,773\n");
    }

    /**
     * Creates a TestContext containing a cube
     * "Warehouse (Default USA) and Sales".
     *
     * @return test context with a cube where the default member in the
     *     Warehouse dimension is USA
     */
    private TestContext createContextWithNonDefaultAllMember() {
        return getTestContext().create(
            null,

            // Warehouse cube where the default member in the Warehouse
            // dimension is USA.
            "<Cube name=\"Warehouse (Default USA)\">\n"
            + "  <Table name=\"inventory_fact_1997\"/>\n"
            + "\n"
            + "  <DimensionUsage name=\"Time\" source=\"Time\" foreignKey=\"time_id\"/>\n"
            + "  <DimensionUsage name=\"Product\" source=\"Product\" foreignKey=\"product_id\"/>\n"
            + "  <DimensionUsage name=\"Store\" source=\"Store\" foreignKey=\"store_id\"/>\n"
            + "  <Dimension name=\"Warehouse\" foreignKey=\"warehouse_id\">\n"
            + "    <Hierarchy hasAll=\"false\" defaultMember=\"[USA]\" primaryKey=\"warehouse_id\"> \n"
            + "      <Table name=\"warehouse\"/>\n"
            + "      <Level name=\"Country\" column=\"warehouse_country\" uniqueMembers=\"true\"/>\n"
            + "      <Level name=\"State Province\" column=\"warehouse_state_province\"\n"
            + "          uniqueMembers=\"true\"/>\n"
            + "      <Level name=\"City\" column=\"warehouse_city\" uniqueMembers=\"false\"/>\n"
            + "      <Level name=\"Warehouse Name\" column=\"warehouse_name\" uniqueMembers=\"true\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Measure name=\"Warehouse Cost\" column=\"warehouse_cost\" aggregator=\"sum\"/>\n"
            + "  <Measure name=\"Warehouse Sales\" column=\"warehouse_sales\" aggregator=\"sum\"/>\n"
            + "</Cube>",

            // Virtual cube based on [Warehouse (Default USA)]
            "<VirtualCube name=\"Warehouse (Default USA) and Sales\">\n"
            + "  <VirtualCubeDimension name=\"Product\"/>\n"
            + "  <VirtualCubeDimension name=\"Store\"/>\n"
            + "  <VirtualCubeDimension name=\"Time\"/>\n"
            + "  <VirtualCubeDimension cubeName=\"Warehouse (Default USA)\" name=\"Warehouse\"/>\n"
            + "  <VirtualCubeMeasure cubeName=\"Sales 2\" name=\"[Measures].[Sales Count]\"/>\n"
            + "  <VirtualCubeMeasure cubeName=\"Sales 2\" name=\"[Measures].[Store Cost]\"/>\n"
            + "  <VirtualCubeMeasure cubeName=\"Sales 2\" name=\"[Measures].[Store Sales]\"/>\n"
            + "  <VirtualCubeMeasure cubeName=\"Sales 2\" name=\"[Measures].[Unit Sales]\"/>\n"
            + "  <VirtualCubeMeasure cubeName=\"Warehouse\" name=\"[Measures].[Store Invoice]\"/>\n"
            + "  <VirtualCubeMeasure cubeName=\"Warehouse\" name=\"[Measures].[Supply Time]\"/>\n"
            + "  <VirtualCubeMeasure cubeName=\"Warehouse\" name=\"[Measures].[Units Ordered]\"/>\n"
            + "  <VirtualCubeMeasure cubeName=\"Warehouse\" name=\"[Measures].[Units Shipped]\"/>\n"
            + "  <VirtualCubeMeasure cubeName=\"Warehouse (Default USA)\" name=\"[Measures].[Warehouse Cost]\"/>\n"
            + "  <VirtualCubeMeasure cubeName=\"Warehouse\" name=\"[Measures].[Warehouse Sales]\"/>\n"
            + "</VirtualCube>",
            null, null, null);
    }

    public void testMemberVisibility() {
        TestContext testContext = getTestContext().create(
            null,
            null,
            "<VirtualCube name=\"Warehouse and Sales Member Visibility\">\n"
            + "  <VirtualCubeDimension cubeName=\"Sales\" name=\"Customers\"/>\n"
            + "  <VirtualCubeDimension name=\"Time\"/>\n"
            + (Bug.VirtualCubeConversionMissesHiddenFixed
                ? ""
                : "  <VirtualCubeDimension name=\"Warehouse\"/>\n")
            + "  <VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Sales Count]\" visible=\"true\" />\n"
            + "  <VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Store Cost]\" visible=\"false\" />\n"
            + "  <VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Store Sales]\"/>\n"
            + "  <VirtualCubeMeasure cubeName=\"Warehouse\" name=\"[Measures].[Units Shipped]\" visible=\"false\" />\n"
            + "  <CalculatedMember name=\"Profit2\" dimension=\"Measures\" visible=\"false\" >\n"
            + "    <Formula>[Measures].[Store Sales] - [Measures].[Store Cost]</Formula>\n"
            + "  </CalculatedMember>\n"
            + "</VirtualCube>",
            null,
            null,
            null);
        Result result = testContext.executeQuery(
            "select {[Measures].[Sales Count],\n"
            + " [Measures].[Store Cost],\n"
            + " [Measures].[Store Sales],\n"
            + " [Measures].[Units Shipped],\n"
            + " [Measures].[Profit2]} on columns\n"
            + "from [Warehouse and Sales Member Visibility]");
        assertVisibility(result, 0, "Sales Count", true); // explicitly visible
        assertVisibility(
            result, 1, "Store Cost", false); // explicitly invisible
        assertVisibility(result, 2, "Store Sales", true); // visible by default
        assertVisibility(
            result, 3, "Units Shipped", false); // explicitly invisible
        assertVisibility(result, 4, "Profit2", false); // explicitly invisible
    }

    private void assertVisibility(
        Result result,
        int ordinal,
        String expectedName,
        boolean expectedVisibility)
    {
        List<Position> columnPositions = result.getAxes()[0].getPositions();
        Member measure = columnPositions.get(ordinal).get(0);
        assertEquals(expectedName, measure.getName());
        assertEquals(
            expectedVisibility,
            measure.getPropertyValue(Property.VISIBLE));
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
        // NOTE: I renamed the CalculatedMember from Profit to Profit2
        // when moving to mondrian-4 schemas
        // because the underlying Sales cube has a member called Profit
        // and the schema convert is not smart enough to let them coexist.
        TestContext testContext = getTestContext().create(
            null,
            null,
            "<Cube name=\"Warehouse No Cache\" cache=\"false\">\n"
            + "  <Table name=\"inventory_fact_1997\"/>\n"
            + "\n"
            + "  <DimensionUsage name=\"Time\" source=\"Time\" foreignKey=\"time_id\"/>\n"
            + "  <DimensionUsage name=\"Store\" source=\"Store\" foreignKey=\"store_id\"/>\n"
            + "  <Measure name=\"Units Shipped\" column=\"units_shipped\" aggregator=\"sum\" formatString=\"#.0\"/>\n"
            + "</Cube>\n"
            + "<VirtualCube name=\"Warehouse and Sales Format Expression Cube No Cache\">\n"
            + "  <VirtualCubeDimension name=\"Store\"/>\n"
            + "  <VirtualCubeDimension name=\"Time\"/>\n"
            + "  <VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Store Cost]\"/>\n"
            + "  <VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Store Sales]\"/>\n"
            + "  <VirtualCubeMeasure cubeName=\"Warehouse No Cache\" name=\"[Measures].[Units Shipped]\"/>\n"
            + "  <CalculatedMember name=\"Profit2\" dimension=\"Measures\">\n"
            + "    <Formula>[Measures].[Store Sales] - [Measures].[Store Cost]</Formula>\n"
            + "  </CalculatedMember>\n"
            + "  <CalculatedMember name=\"Profit Per Unit Shipped\" dimension=\"Measures\">\n"
            + "    <Formula>[Measures].[Profit2] / [Measures].[Units Shipped]</Formula>\n"
            + "    <CalculatedMemberProperty name=\"FORMAT_STRING\" expression=\"IIf(([Measures].[Profit Per Unit Shipped] > 2.0), '|0.#|style=green', '|0.#|style=red')\"/>\n"
            + "  </CalculatedMember>\n"
            + "</VirtualCube>",
            null,
            null,
            null);

        testContext.assertQueryReturns(
            "select {[Measures].[Profit Per Unit Shipped]} ON COLUMNS, "
            + "{[Store].[All Stores].[USA].[CA], [Store].[All Stores].[USA].[OR], [Store].[All Stores].[USA].[WA]} ON ROWS "
            + "from [Warehouse and Sales Format Expression Cube No Cache] "
            + "where [Time].[1997]",
            "Axis #0:\n"
            + "{[Time].[Time].[1997]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Profit Per Unit Shipped]}\n"
            + "Axis #2:\n"
            + "{[Store].[Store].[USA].[CA]}\n"
            + "{[Store].[Store].[USA].[OR]}\n"
            + "{[Store].[Store].[USA].[WA]}\n"
            + "Row #0: |1.6|style=red\n"
            + "Row #1: |2.1|style=green\n"
            + "Row #2: |1.5|style=red\n");
    }

    public void testCalculatedMeasure() {
        // calculated measures reference measures defined in the base cube
        assertQueryReturns(
            "select\n"
            + "{[Measures].[Profit Growth], "
            + "[Measures].[Profit], "
            + "[Measures].[Average Warehouse Sale] }\n"
            + "ON COLUMNS\n"
            + "from [Warehouse and Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Profit Growth]}\n"
            + "{[Measures].[Profit]}\n"
            + "{[Measures].[Average Warehouse Sale]}\n"
            + "Row #0: 0.0%\n"
            + "Row #0: $339,610.90\n"
            + "Row #0: $2.21\n");
    }

    public void testLostData() {
        assertQueryReturns(
            "select {[Time].[Time].Members} on columns,\n"
            + " {[Product].Children} on rows\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time].[Time].[1997]}\n"
            + "{[Time].[Time].[1997].[Q1]}\n"
            + "{[Time].[Time].[1997].[Q1].[1]}\n"
            + "{[Time].[Time].[1997].[Q1].[2]}\n"
            + "{[Time].[Time].[1997].[Q1].[3]}\n"
            + "{[Time].[Time].[1997].[Q2]}\n"
            + "{[Time].[Time].[1997].[Q2].[4]}\n"
            + "{[Time].[Time].[1997].[Q2].[5]}\n"
            + "{[Time].[Time].[1997].[Q2].[6]}\n"
            + "{[Time].[Time].[1997].[Q3]}\n"
            + "{[Time].[Time].[1997].[Q3].[7]}\n"
            + "{[Time].[Time].[1997].[Q3].[8]}\n"
            + "{[Time].[Time].[1997].[Q3].[9]}\n"
            + "{[Time].[Time].[1997].[Q4]}\n"
            + "{[Time].[Time].[1997].[Q4].[10]}\n"
            + "{[Time].[Time].[1997].[Q4].[11]}\n"
            + "{[Time].[Time].[1997].[Q4].[12]}\n"
            + "{[Time].[Time].[1998]}\n"
            + "{[Time].[Time].[1998].[Q1]}\n"
            + "{[Time].[Time].[1998].[Q1].[1]}\n"
            + "{[Time].[Time].[1998].[Q1].[2]}\n"
            + "{[Time].[Time].[1998].[Q1].[3]}\n"
            + "{[Time].[Time].[1998].[Q2]}\n"
            + "{[Time].[Time].[1998].[Q2].[4]}\n"
            + "{[Time].[Time].[1998].[Q2].[5]}\n"
            + "{[Time].[Time].[1998].[Q2].[6]}\n"
            + "{[Time].[Time].[1998].[Q3]}\n"
            + "{[Time].[Time].[1998].[Q3].[7]}\n"
            + "{[Time].[Time].[1998].[Q3].[8]}\n"
            + "{[Time].[Time].[1998].[Q3].[9]}\n"
            + "{[Time].[Time].[1998].[Q4]}\n"
            + "{[Time].[Time].[1998].[Q4].[10]}\n"
            + "{[Time].[Time].[1998].[Q4].[11]}\n"
            + "{[Time].[Time].[1998].[Q4].[12]}\n"
            + "Axis #2:\n"
            + "{[Product].[Product].[Drink]}\n"
            + "{[Product].[Product].[Food]}\n"
            + "{[Product].[Product].[Non-Consumable]}\n"
            + "Row #0: 24,597\n"
            + "Row #0: 5,976\n"
            + "Row #0: 1,910\n"
            + "Row #0: 1,951\n"
            + "Row #0: 2,115\n"
            + "Row #0: 5,895\n"
            + "Row #0: 1,948\n"
            + "Row #0: 2,039\n"
            + "Row #0: 1,908\n"
            + "Row #0: 6,065\n"
            + "Row #0: 2,205\n"
            + "Row #0: 1,921\n"
            + "Row #0: 1,939\n"
            + "Row #0: 6,661\n"
            + "Row #0: 1,898\n"
            + "Row #0: 2,344\n"
            + "Row #0: 2,419\n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #1: 191,940\n"
            + "Row #1: 47,809\n"
            + "Row #1: 15,604\n"
            + "Row #1: 15,142\n"
            + "Row #1: 17,063\n"
            + "Row #1: 44,825\n"
            + "Row #1: 14,393\n"
            + "Row #1: 15,055\n"
            + "Row #1: 15,377\n"
            + "Row #1: 47,440\n"
            + "Row #1: 17,036\n"
            + "Row #1: 15,741\n"
            + "Row #1: 14,663\n"
            + "Row #1: 51,866\n"
            + "Row #1: 14,232\n"
            + "Row #1: 18,278\n"
            + "Row #1: 19,356\n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #2: 50,236\n"
            + "Row #2: 12,506\n"
            + "Row #2: 4,114\n"
            + "Row #2: 3,864\n"
            + "Row #2: 4,528\n"
            + "Row #2: 11,890\n"
            + "Row #2: 3,838\n"
            + "Row #2: 3,987\n"
            + "Row #2: 4,065\n"
            + "Row #2: 12,343\n"
            + "Row #2: 4,522\n"
            + "Row #2: 4,035\n"
            + "Row #2: 3,786\n"
            + "Row #2: 13,497\n"
            + "Row #2: 3,828\n"
            + "Row #2: 4,648\n"
            + "Row #2: 5,021\n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n");
        assertQueryReturns(
            "select\n"
            + " {[Measures].[Unit Sales]} on 0,\n"
            + " {[Product].Children} on 1\n"
            + "from [Warehouse and Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Product].[Drink]}\n"
            + "{[Product].[Product].[Food]}\n"
            + "{[Product].[Product].[Non-Consumable]}\n"
            + "Row #0: 24,597\n"
            + "Row #1: 191,940\n"
            + "Row #2: 50,236\n");
    }

    /**
     * Tests a calc measure which combines a measures from the Sales cube with a
     * measures from the Warehouse cube.
     */
    public void testCalculatedMeasureAcrossCubes() {
        assertQueryReturns(
            "with member [Measures].[Shipped per Ordered] as ' [Measures].[Units Shipped] / [Measures].[Unit Sales] ', format_string='#.00%'\n"
            + " member [Measures].[Profit per Unit Shipped] as ' [Measures].[Profit] / [Measures].[Units Shipped] '\n"
            + "select\n"
            + " {[Measures].[Unit Sales], \n"
            + "  [Measures].[Units Shipped],\n"
            + "  [Measures].[Shipped per Ordered],\n"
            + "  [Measures].[Profit per Unit Shipped]} on 0,\n"
            + " NON EMPTY Crossjoin([Product].Children, [Time].[1997].Children) on 1\n"
            + "from [Warehouse and Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Units Shipped]}\n"
            + "{[Measures].[Shipped per Ordered]}\n"
            + "{[Measures].[Profit per Unit Shipped]}\n"
            + "Axis #2:\n"
            + "{[Product].[Product].[Drink], [Time].[Time].[1997].[Q1]}\n"
            + "{[Product].[Product].[Drink], [Time].[Time].[1997].[Q2]}\n"
            + "{[Product].[Product].[Drink], [Time].[Time].[1997].[Q3]}\n"
            + "{[Product].[Product].[Drink], [Time].[Time].[1997].[Q4]}\n"
            + "{[Product].[Product].[Food], [Time].[Time].[1997].[Q1]}\n"
            + "{[Product].[Product].[Food], [Time].[Time].[1997].[Q2]}\n"
            + "{[Product].[Product].[Food], [Time].[Time].[1997].[Q3]}\n"
            + "{[Product].[Product].[Food], [Time].[Time].[1997].[Q4]}\n"
            + "{[Product].[Product].[Non-Consumable], [Time].[Time].[1997].[Q1]}\n"
            + "{[Product].[Product].[Non-Consumable], [Time].[Time].[1997].[Q2]}\n"
            + "{[Product].[Product].[Non-Consumable], [Time].[Time].[1997].[Q3]}\n"
            + "{[Product].[Product].[Non-Consumable], [Time].[Time].[1997].[Q4]}\n"
            + "Row #0: 5,976\n"
            + "Row #0: 4637.0\n"
            + "Row #0: 77.59%\n"
            + "Row #0: $1.50\n"
            + "Row #1: 5,895\n"
            + "Row #1: 4501.0\n"
            + "Row #1: 76.35%\n"
            + "Row #1: $1.60\n"
            + "Row #2: 6,065\n"
            + "Row #2: 6258.0\n"
            + "Row #2: 103.18%\n"
            + "Row #2: $1.15\n"
            + "Row #3: 6,661\n"
            + "Row #3: 5802.0\n"
            + "Row #3: 87.10%\n"
            + "Row #3: $1.38\n"
            + "Row #4: 47,809\n"
            + "Row #4: 37153.0\n"
            + "Row #4: 77.71%\n"
            + "Row #4: $1.64\n"
            + "Row #5: 44,825\n"
            + "Row #5: 35459.0\n"
            + "Row #5: 79.11%\n"
            + "Row #5: $1.62\n"
            + "Row #6: 47,440\n"
            + "Row #6: 41545.0\n"
            + "Row #6: 87.57%\n"
            + "Row #6: $1.47\n"
            + "Row #7: 51,866\n"
            + "Row #7: 34706.0\n"
            + "Row #7: 66.91%\n"
            + "Row #7: $1.91\n"
            + "Row #8: 12,506\n"
            + "Row #8: 9161.0\n"
            + "Row #8: 73.25%\n"
            + "Row #8: $1.76\n"
            + "Row #9: 11,890\n"
            + "Row #9: 9227.0\n"
            + "Row #9: 77.60%\n"
            + "Row #9: $1.65\n"
            + "Row #10: 12,343\n"
            + "Row #10: 9986.0\n"
            + "Row #10: 80.90%\n"
            + "Row #10: $1.59\n"
            + "Row #11: 13,497\n"
            + "Row #11: 9291.0\n"
            + "Row #11: 68.84%\n"
            + "Row #11: $1.86\n");
    }

    /**
     * Tests a calc member defined in the cube.
     */
    public void testCalculatedMemberInSchema() {
        TestContext testContext = getTestContext().createSubstitutingCube(
            "Warehouse and Sales",
            null,
            "  <CalculatedMember name=\"Shipped per Ordered\" dimension=\"Measures\">\n"
            + "    <Formula>[Measures].[Units Shipped] / [Measures].[Unit Sales]</Formula>\n"
            + "    <CalculatedMemberProperty name=\"FORMAT_STRING\" value=\"#.0%\"/>\n"
            + "  </CalculatedMember>\n");
        testContext.assertQueryReturns(
            "select\n"
            + " {[Measures].[Unit Sales], \n"
            + "  [Measures].[Shipped per Ordered]} on 0,\n"
            + " NON EMPTY Crossjoin([Product].Children, [Time].[1997].Children) on 1\n"
            + "from [Warehouse and Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Shipped per Ordered]}\n"
            + "Axis #2:\n"
            + "{[Product].[Product].[Drink], [Time].[Time].[1997].[Q1]}\n"
            + "{[Product].[Product].[Drink], [Time].[Time].[1997].[Q2]}\n"
            + "{[Product].[Product].[Drink], [Time].[Time].[1997].[Q3]}\n"
            + "{[Product].[Product].[Drink], [Time].[Time].[1997].[Q4]}\n"
            + "{[Product].[Product].[Food], [Time].[Time].[1997].[Q1]}\n"
            + "{[Product].[Product].[Food], [Time].[Time].[1997].[Q2]}\n"
            + "{[Product].[Product].[Food], [Time].[Time].[1997].[Q3]}\n"
            + "{[Product].[Product].[Food], [Time].[Time].[1997].[Q4]}\n"
            + "{[Product].[Product].[Non-Consumable], [Time].[Time].[1997].[Q1]}\n"
            + "{[Product].[Product].[Non-Consumable], [Time].[Time].[1997].[Q2]}\n"
            + "{[Product].[Product].[Non-Consumable], [Time].[Time].[1997].[Q3]}\n"
            + "{[Product].[Product].[Non-Consumable], [Time].[Time].[1997].[Q4]}\n"
            + "Row #0: 5,976\n"
            + "Row #0: 77.6%\n"
            + "Row #1: 5,895\n"
            + "Row #1: 76.4%\n"
            + "Row #2: 6,065\n"
            + "Row #2: 103.2%\n"
            + "Row #3: 6,661\n"
            + "Row #3: 87.1%\n"
            + "Row #4: 47,809\n"
            + "Row #4: 77.7%\n"
            + "Row #5: 44,825\n"
            + "Row #5: 79.1%\n"
            + "Row #6: 47,440\n"
            + "Row #6: 87.6%\n"
            + "Row #7: 51,866\n"
            + "Row #7: 66.9%\n"
            + "Row #8: 12,506\n"
            + "Row #8: 73.3%\n"
            + "Row #9: 11,890\n"
            + "Row #9: 77.6%\n"
            + "Row #10: 12,343\n"
            + "Row #10: 80.9%\n"
            + "Row #11: 13,497\n"
            + "Row #11: 68.8%\n");
    }

    public void testAllMeasureMembers() {
        // result should exclude measures that are not explicitly defined
        // in the virtual cube (e.g., [Profit last Period])
        assertQueryReturns(
            "select\n"
            + "{[Measures].allMembers} on columns\n"
            + "from [Warehouse and Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Sales Count]}\n"
            + "{[Measures].[Store Cost]}\n"
            + "{[Measures].[Store Sales]}\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Customer Count]}\n"
            + "{[Measures].[Promotion Sales]}\n"
            + "{[Measures].[Store Invoice]}\n"
            + "{[Measures].[Supply Time]}\n"
            + "{[Measures].[Units Ordered]}\n"
            + "{[Measures].[Units Shipped]}\n"
            + "{[Measures].[Warehouse Cost]}\n"
            + "{[Measures].[Warehouse Profit]}\n"
            + "{[Measures].[Warehouse Sales]}\n"
            + "{[Measures].[Profit]}\n"
            + "{[Measures].[Profit Growth]}\n"
            + "{[Measures].[Profit last Period]}\n"
            + "{[Measures].[Average Warehouse Sale]}\n"
            + "{[Measures].[Profit Per Unit Shipped]}\n"
            + "Row #0: 86,837\n"
            + "Row #0: 225,627.23\n"
            + "Row #0: 565,238.13\n"
            + "Row #0: 266,773\n"
            + "Row #0: 5,581\n"
            + "Row #0: 565,238.13\n"
            + "Row #0: 102,278.409\n"
            + "Row #0: 10,425\n"
            + "Row #0: 227238.0\n"
            + "Row #0: 207726.0\n"
            + "Row #0: 89,043.253\n"
            + "Row #0: 107,727.635\n"
            + "Row #0: 196,770.888\n"
            + "Row #0: $339,610.90\n"
            + "Row #0: 0.0%\n"
            + "Row #0: $339,610.90\n"
            + "Row #0: $2.21\n"
            + "Row #0: $1.63\n");
    }

    /**
     * Test a virtual cube where one of the dimensions contains an
     * ordinalColumn property
     */
    public void testOrdinalColumn() {
        TestContext testContext = getTestContext().create(
            null,
            null,
            "<VirtualCube name=\"Sales vs HR\">\n"
            + "<VirtualCubeDimension cubeName=\"HR\" name=\"Store\"/>\n"
            + "<VirtualCubeDimension cubeName=\"HR\" name=\"Position\"/>\n"
            + (Bug.VirtualCubeConversionMissesHiddenFixed
                ? ""
                : "  <VirtualCubeDimension cubeName=\"HR\" name=\"Employees\"/>\n")
            + "<VirtualCubeMeasure cubeName=\"HR\" name=\"[Measures].[Org Salary]\"/>\n"
            + "</VirtualCube>",
            null,
            null,
            null);
        testContext.assertQueryReturns(
            "select {[Measures].[Org Salary]} on columns, "
            + "non empty "
            + "crossjoin([Store].[Store Country].members, [Position].[Store Management].children) "
            + "on rows from [Sales vs HR]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Org Salary]}\n"
            + "Axis #2:\n"
            + "{[Store].[Store].[Canada], [Position].[Position].[Store Management].[Store Manager]}\n"
            + "{[Store].[Store].[Canada], [Position].[Position].[Store Management].[Store Assistant Manager]}\n"
            + "{[Store].[Store].[Canada], [Position].[Position].[Store Management].[Store Shift Supervisor]}\n"
            + "{[Store].[Store].[Mexico], [Position].[Position].[Store Management].[Store Manager]}\n"
            + "{[Store].[Store].[Mexico], [Position].[Position].[Store Management].[Store Assistant Manager]}\n"
            + "{[Store].[Store].[Mexico], [Position].[Position].[Store Management].[Store Shift Supervisor]}\n"
            + "{[Store].[Store].[USA], [Position].[Position].[Store Management].[Store Manager]}\n"
            + "{[Store].[Store].[USA], [Position].[Position].[Store Management].[Store Assistant Manager]}\n"
            + "{[Store].[Store].[USA], [Position].[Position].[Store Management].[Store Shift Supervisor]}\n"
            + "Row #0: $462.86\n"
            + "Row #1: $394.29\n"
            + "Row #2: $565.71\n"
            + "Row #3: $13,254.55\n"
            + "Row #4: $11,443.64\n"
            + "Row #5: $17,705.46\n"
            + "Row #6: $4,069.80\n"
            + "Row #7: $3,417.72\n"
            + "Row #8: $5,145.96\n");
    }

    /**
     * Tests that mondrian gives an error if a dimension in a virtual cube
     * does not join to any cubes.
     */
    public void testUnjoinedDimension() {
        TestContext testContext = getTestContext().create(
            null,
            null,
            "<VirtualCube name=\"Sales vs HR\">\n"
            + "<VirtualCubeDimension name=\"Warehouse\"/>\n"
            + "<VirtualCubeDimension cubeName=\"HR\" name=\"Position\"/>\n"
            + (Bug.VirtualCubeConversionMissesHiddenFixed
                ? ""
                : "  <VirtualCubeDimension cubeName=\"HR\" name=\"Employees\"/>\n")
            + "<VirtualCubeMeasure cubeName=\"HR\" name=\"[Measures].[Org Salary]\"/>\n"
            + "</VirtualCube>",
            null,
            null,
            null);
        testContext.assertErrorList().containsError(
            "Virtual cube dimension must join to at least one cube: dimension 'Warehouse' in cube 'Sales vs HR' \\(in VirtualCubeDimension\\) \\(at ${pos}\\)",
            "<VirtualCubeDimension name=\"Warehouse\"/>");
    }

    public void testDefaultMeasureProperty() {
        TestContext testContext = getTestContext().create(
            null,
            null,
            "<VirtualCube name=\"Sales vs Warehouse\" defaultMeasure=\"Unit Sales\">\n"
            + "<VirtualCubeDimension name=\"Product\"/>\n"
            + (Bug.VirtualCubeConversionMissesHiddenFixed
                ? ""
                : "  <VirtualCubeDimension cubeName=\"Sales\" name=\"Time\"/>\n"
                + "  <VirtualCubeDimension name=\"Warehouse\"/>\n")
            + "<VirtualCubeMeasure cubeName=\"Warehouse\" name=\"[Measures].[Warehouse Sales]\"/>\n"
            + "<VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Unit Sales]\"/>\n"
            + "<VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Profit]\"/>\n"
            + "<VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Store Sales]\"/>\n"
            + "<VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Store Cost]\"/>\n"
            + "</VirtualCube>",
            null,
            null,
            null);

        String queryWithoutFilter =
            "select"
            + " from [Sales vs Warehouse]";
        String queryWithDeflaultMeasureFilter =
            "select "
            + "from [Sales vs Warehouse] where measures.[Unit Sales]";
        assertQueriesReturnSimilarResults(
            queryWithoutFilter, queryWithDeflaultMeasureFilter, testContext);
    }

    /**
     * Checks that native set caching considers base cubes in the cache key.
     * Native sets referencing different base cubes do not share the cached
     * result.
     */
    public void testNativeSetCaching() {
        // Only need to run this against one db to verify caching
        // behavior is correct.
        final Dialect dialect = getTestContext().getDialect();
        if (dialect.getDatabaseProduct() != Dialect.DatabaseProduct.DERBY) {
            return;
        }

        if (!MondrianProperties.instance().EnableNativeCrossJoin.get()
            && !MondrianProperties.instance().EnableNativeNonEmpty.get())
        {
            // Only run the tests if either native CrossJoin or native NonEmpty
            // is enabled.
            return;
        }

        String query1 =
            "With "
            + "Set [*NATIVE_CJ_SET] as 'NonEmptyCrossJoin([Product].[Product Family].Members, [Store].[Store Country].Members)' "
            + "Select "
            + "{[Store Sales]} on columns, "
            + "Non Empty Generate([*NATIVE_CJ_SET], {([Product].CurrentMember,[Store].CurrentMember)}) on rows "
            + "From [Warehouse and Sales]";

        String query2 =
            "With "
            + "Set [*NATIVE_CJ_SET] as 'NonEmptyCrossJoin([Product].[Product Family].Members, [Store].[Store Country].Members)' "
            + "Select "
            + "{[Warehouse Sales]} on columns, "
            + "Non Empty Generate([*NATIVE_CJ_SET], {([Product].CurrentMember,[Store].CurrentMember)}) on rows "
            + "From [Warehouse and Sales]";

        String derbyNecjSql1, derbyNecjSql2;

        if (MondrianProperties.instance().EnableNativeCrossJoin.get()) {
            derbyNecjSql1 =
                "select "
                + "\"product_class\".\"product_family\", "
                + "\"store\".\"store_country\" "
                + "from "
                + "\"product\" as \"product\", "
                + "\"product_class\" as \"product_class\", "
                + "\"sales_fact_1997\" as \"sales_fact_1997\", "
                + "\"store\" as \"store\" "
                + "where "
                + "\"product\".\"product_class_id\" = \"product_class\".\"product_class_id\" "
                + "and \"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\" "
                + "and \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" "
                + "group by \"product_class\".\"product_family\", \"store\".\"store_country\" "
                + "order by 1 ASC, 2 ASC";

            derbyNecjSql2 =
                "select "
                + "\"product_class\".\"product_family\", "
                + "\"store\".\"store_country\" "
                + "from "
                + "\"product\" as \"product\", "
                + "\"product_class\" as \"product_class\", "
                + "\"inventory_fact_1997\" as \"inventory_fact_1997\", "
                + "\"store\" as \"store\" "
                + "where "
                + "\"product\".\"product_class_id\" = \"product_class\".\"product_class_id\" "
                + "and \"inventory_fact_1997\".\"product_id\" = \"product\".\"product_id\" "
                + "and \"inventory_fact_1997\".\"store_id\" = \"store\".\"store_id\" "
                + "group by \"product_class\".\"product_family\", \"store\".\"store_country\" "
                + "order by 1 ASC, 2 ASC";
        } else {
            // NECJ is truend off so native NECJ SQL will not be generated;
            // however, because the NECJ set should not find match in the cache,
            // each NECJ input will still be joined with the correct
            // fact table if NonEmpty condition is natively evaluated.
            derbyNecjSql1 =
                "select "
                + "\"store\".\"store_country\" "
                + "from "
                + "\"store\" as \"store\", "
                + "\"sales_fact_1997\" as \"sales_fact_1997\" "
                + "where "
                + "\"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\" "
                + "group by \"store\".\"store_country\" "
                + "order by 1 ASC";

            derbyNecjSql2 =
                "select "
                + "\"store\".\"store_country\" "
                + "from "
                + "\"store\" as \"store\", "
                + "\"inventory_fact_1997\" as \"inventory_fact_1997\" "
                + "where "
                + "\"inventory_fact_1997\".\"store_id\" = \"store\".\"store_id\" "
                + "group by \"store\".\"store_country\" "
                + "order by 1 ASC";
        }

        SqlPattern[] patterns1 = {
            new SqlPattern(
                Dialect.DatabaseProduct.DERBY, derbyNecjSql1, derbyNecjSql1)
        };

        SqlPattern[] patterns2 = {
            new SqlPattern(
                Dialect.DatabaseProduct.DERBY, derbyNecjSql2, derbyNecjSql2)
        };

        // Run query 1 with cleared cache;
        // Make sure NECJ 1 is evaluated natively.
        assertQuerySql(getTestContext(), query1, patterns1, true);

        // Now run query 2 with warm cache;
        // Make sure NECJ 2 does not reuse the cache result from NECJ 1, and
        // NECJ 2 is evaluated natively.
        assertQuerySql(getTestContext(), query2, patterns2, false);
    }

    /**
     * Test case for bug <a href="http://jira.pentaho.com/browse/MONDRIAN-322">
     * MONDRIAN-322, "cube.getStar() throws NullPointerException"</a>.
     * Happens when you aggregate distinct-count measures in a virtual cube.
     */
    public void testBugMondrian322() {
        final TestContext testContext = getTestContext().create(
            null,
            null,
            "<VirtualCube name=\"Warehouse and Sales2\" defaultMeasure=\"Store Sales\">\n"
            + "  <VirtualCubeDimension cubeName=\"Sales\" name=\"Customers\"/>\n"
            + "  <VirtualCubeDimension name=\"Time\"/>\n"
            + "  <VirtualCubeDimension cubeName=\"Warehouse\" name=\"Warehouse\"/>\n"
            + "  <VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Customer Count]\"/>\n"
            + "  <VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Store Sales]\"/>\n"
            + "</VirtualCube>",
            null,
            null,
            null);

        // This test case does not actually reject the dimension constraint from
        // an unrelated base cube. The reason is that the constraint contains an
        // AllLevel member. Even though semantically constraining Cells using an
        // non-existent dimension perhaps does not make sense; however, in the
        // case where the constraint contains AllLevel member, the constraint
        // can be considered "always true".
        //
        // See the next test case for a constraint that does not contain
        // AllLevel member and hence cannot be satisfied. The cell should be
        // empty.
        testContext.assertQueryReturns(
            "with member [Warehouse].[x] as 'Aggregate([Warehouse].members)'\n"
            + "member [Measures].[foo] AS '([Warehouse].[x],[Measures].[Customer Count])'\n"
            + "select {[Measures].[foo]} on 0 from [Warehouse And Sales2]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[foo]}\n"
            + "Row #0: 5,581\n");
    }

    public void testBugMondrian322a() {
        final TestContext testContext = getTestContext().create(
            null,
            null,
            "<VirtualCube name=\"Warehouse and Sales2\" defaultMeasure=\"Store Sales\">\n"
            + "  <VirtualCubeDimension cubeName=\"Sales\" name=\"Customers\"/>\n"
            + "  <VirtualCubeDimension name=\"Time\"/>\n"
            + "  <VirtualCubeDimension cubeName=\"Warehouse\" name=\"Warehouse\"/>\n"
            + "  <VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Customer Count]\"/>\n"
            + "  <VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Store Sales]\"/>\n"
            + "</VirtualCube>",
            null,
            null,
            null);
        testContext.assertQueryReturns(
            "with member [Warehouse].[x] as 'Aggregate({[Warehouse].[Canada], [Warehouse].[USA]})'\n"
            + "member [Measures].[foo] AS '([Warehouse].[x],[Measures].[Customer Count])'\n"
            + "select {[Measures].[foo]} on 0 from [Warehouse And Sales2]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[foo]}\n"
            + "Row #0: \n");
    }

    /**
     * Test case for bug <a href="http://jira.pentaho.com/browse/MONDRIAN-352">
     * MONDRIAN-352, "Caption is not set on RolapVirtualCubeMesure"</a>.
     */
    public void testVirtualCubeMeasureCaption() {
        TestContext testContext = getTestContext().create(
            null,
            "<Cube name=\"TestStore\">\n"
            + "  <Table name=\"store\"/>\n"
            + "  <Dimension name=\"HCB\" caption=\"Has coffee bar caption\">\n"
            + "    <Hierarchy hasAll=\"true\">\n"
            + "      <Level name=\"Has coffee bar\" column=\"coffee_bar\" uniqueMembers=\"true\"\n"
            + "          type=\"Boolean\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Measure name=\"Store Sqft\" caption=\"Store Sqft Caption\" column=\"store_sqft\" aggregator=\"sum\" formatString=\"#,###\"/>\n"
            + "</Cube>\n",

            "<VirtualCube name=\"VirtualTestStore\">\n"
            + "  <VirtualCubeDimension cubeName=\"TestStore\" name=\"HCB\"/>\n"
            + "  <VirtualCubeMeasure   cubeName=\"TestStore\" name=\"[Measures].[Store Sqft]\"/>\n"
            + "</VirtualCube>",
            null,
            null,
            null);

        Result result = testContext.executeQuery(
            "select {[Measures].[Store Sqft]} ON COLUMNS,"
            + "{[HCB]} ON ROWS "
            + "from [VirtualTestStore]");

        Axis[] axes = result.getAxes();
        List<Position> positions = axes[0].getPositions();
        Member m0 = positions.get(0).get(0);
        String caption = m0.getCaption();
        assertEquals("Store Sqft Caption", caption);
    }

    /**
     * Test that RolapCubeLevel is used correctly in the context of virtual
     * cube.
     */
    public void testRolapCubeLevelInVirtualCube() {
        String query1 =
            "With "
            + "Set [*NATIVE_CJ_SET] as 'NonEmptyCrossJoin([*BASE_MEMBERS_Warehouse],[*BASE_MEMBERS_Time])' "
            + "Set [*NATIVE_MEMBERS_Warehouse] as 'Generate([*NATIVE_CJ_SET], {[Warehouse].CurrentMember})' "
            + "Set [*BASE_MEMBERS_Warehouse] as '[Warehouse].[Country].Members' "
            + "Set [*NATIVE_MEMBERS_Time] as 'Generate([*NATIVE_CJ_SET], {[Time].[Time].CurrentMember})' "
            + "Set [*BASE_MEMBERS_Time] as '[Time].[Month].Members' "
            + "Set [*BASE_MEMBERS_Measures] as '{[Measures].[*FORMATTED_MEASURE_0]}' Member [Measures].[*FORMATTED_MEASURE_0] as '[Measures].[Warehouse Sales]', FORMAT_STRING = '#,##0', SOLVE_ORDER=400 "
            + "Select [*BASE_MEMBERS_Measures] on columns, Non Empty Generate([*NATIVE_CJ_SET], {([Warehouse].currentMember,[Time].[Time].currentMember)}) on rows From [Warehouse and Sales] ";

        String query2 =
            "With "
            + "Set [*NATIVE_CJ_SET] as 'NonEmptyCrossJoin([*BASE_MEMBERS_Warehouse],[*BASE_MEMBERS_Time])' "
            + "Set [*NATIVE_MEMBERS_Warehouse] as 'Generate([*NATIVE_CJ_SET], {[Warehouse].CurrentMember})' "
            + "Set [*BASE_MEMBERS_Warehouse] as '[Warehouse].[Country].Members' "
            + "Set [*NATIVE_MEMBERS_Time] as 'Generate([*NATIVE_CJ_SET], {[Time].[Time].CurrentMember})' "
            + "Set [*BASE_MEMBERS_Time] as 'Filter([Time].[Month].Members,[Time].[Time].CurrentMember Not In {[Time].[1997].[Q1].[2]})' "
            + "Set [*BASE_MEMBERS_Measures] as '{[Measures].[*FORMATTED_MEASURE_0]}' Member [Measures].[*FORMATTED_MEASURE_0] as '[Measures].[Warehouse Sales]', FORMAT_STRING = '#,##0', SOLVE_ORDER=400 "
            + "Select [*BASE_MEMBERS_Measures] on columns, Non Empty Generate([*NATIVE_CJ_SET], {([Warehouse].currentMember,[Time].[Time].currentMember)}) on rows From [Warehouse and Sales]";

        executeQuery(query1);

        /* The query with the filter should now succeed without NPE */
        String result =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[*FORMATTED_MEASURE_0]}\n"
            + "Axis #2:\n"
            + "{[Warehouse].[Warehouse].[USA], [Time].[Time].[1997].[Q1].[1]}\n"
            + "{[Warehouse].[Warehouse].[USA], [Time].[Time].[1997].[Q1].[3]}\n"
            + "{[Warehouse].[Warehouse].[USA], [Time].[Time].[1997].[Q2].[4]}\n"
            + "{[Warehouse].[Warehouse].[USA], [Time].[Time].[1997].[Q2].[5]}\n"
            + "{[Warehouse].[Warehouse].[USA], [Time].[Time].[1997].[Q2].[6]}\n"
            + "{[Warehouse].[Warehouse].[USA], [Time].[Time].[1997].[Q3].[7]}\n"
            + "{[Warehouse].[Warehouse].[USA], [Time].[Time].[1997].[Q3].[8]}\n"
            + "{[Warehouse].[Warehouse].[USA], [Time].[Time].[1997].[Q3].[9]}\n"
            + "{[Warehouse].[Warehouse].[USA], [Time].[Time].[1997].[Q4].[10]}\n"
            + "{[Warehouse].[Warehouse].[USA], [Time].[Time].[1997].[Q4].[11]}\n"
            + "{[Warehouse].[Warehouse].[USA], [Time].[Time].[1997].[Q4].[12]}\n"
            + "Row #0: 21,762\n"
            + "Row #1: 13,775\n"
            + "Row #2: 15,938\n"
            + "Row #3: 15,649\n"
            + "Row #4: 14,629\n"
            + "Row #5: 18,626\n"
            + "Row #6: 15,833\n"
            + "Row #7: 21,393\n"
            + "Row #8: 17,100\n"
            + "Row #9: 15,356\n"
            + "Row #10: 13,948\n";

        assertQueryReturns(query2, result);
    }

    /**
     * Tests that the logic to apply non empty context constraint in virtual
     * cube is correct.  The joins shouldn't be cartesian product.
     */
    public void testNonEmptyCJConstraintOnVirtualCube() {
        if (!MondrianProperties.instance().EnableNativeCrossJoin.get()) {
            // Generated SQL is different if NonEmptyCrossJoin is evaluated in
            // memory.
            return;
        }
        String query =
            "with "
            + "set [foo] as [Time].[Month].members "
            + "set [bar] as {[Store].[USA]} "
            + "Select {[Measures].[Warehouse Sales],[Measures].[Store Sales]} on columns, "
            + "nonemptycrossjoin([foo],[bar]) on rows "
            + "From [Warehouse and Sales] "
            + "Where ([Product].[All Products].[Food])";

        // Note that for MySQL (because MySQL sorts NULLs first), because there
        // is a UNION (which prevents us from sorting on column names or
        // expressions) the ORDER BY clause should be something like
        //   ORDER BY ISNULL(1), 1 ASC, ISNULL(2), 2 ASC, ISNULL(3), 3 ASC,
        //   ISNULL(4), 4 ASC
        // but ISNULL(1) isn't valid SQL, so we forego correct ordering of NULL
        // values.
        String mysqlSQL =
            "select\n"
            + "    `time_by_day`.`the_year` as `c0`,\n"
            + "    `time_by_day`.`quarter` as `c1`,\n"
            + "    `time_by_day`.`month_of_year` as `c2`,\n"
            + "    `store`.`store_country` as `c3`\n"
            + "from\n"
            + "    `inventory_fact_1997` as `inventory_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`,\n"
            + "    `store` as `store`,\n"
            + "    `product` as `product`,\n"
            + "    `product_class` as `product_class`\n"
            + "where\n"
            + "    `product_class`.`product_family` = 'Food'\n"
            + "and\n"
            + "    (`store`.`store_country` = 'USA')\n"
            + "and\n"
            + "    `inventory_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            + "    `inventory_fact_1997`.`store_id` = `store`.`store_id`\n"
            + "and\n"
            + "    `inventory_fact_1997`.`product_id` = `product`.`product_id`\n"
            + "and\n"
            + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
            + "group by\n"
            + "    `time_by_day`.`the_year`,\n"
            + "    `time_by_day`.`quarter`,\n"
            + "    `time_by_day`.`month_of_year`,\n"
            + "    `store`.`store_country`\n"
            + "union\n"
            + "select\n"
            + "    `time_by_day`.`the_year` as `c0`,\n"
            + "    `time_by_day`.`quarter` as `c1`,\n"
            + "    `time_by_day`.`month_of_year` as `c2`,\n"
            + "    `store`.`store_country` as `c3`\n"
            + "from\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `time_by_day` as `time_by_day`,\n"
            + "    `store` as `store`,\n"
            + "    `product` as `product`,\n"
            + "    `product_class` as `product_class`\n"
            + "where\n"
            + "    `product_class`.`product_family` = 'Food'\n"
            + "and\n"
            + "    (`store`.`store_country` = 'USA')\n"
            + "and\n"
            + "    `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`store_id` = `store`.`store_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
            + "and\n"
            + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
            + "group by\n"
            + "    `time_by_day`.`the_year`,\n"
            + "    `time_by_day`.`quarter`,\n"
            + "    `time_by_day`.`month_of_year`,\n"
            + "    `store`.`store_country`\n"
            + "order by\n"
            + "    1 ASC,\n"
            + "    2 ASC,\n"
            + "    3 ASC,\n"
            + "    4 ASC";

        SqlPattern[] mysqlPattern = {
            new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysqlSQL, mysqlSQL)
        };

        String result =
            "Axis #0:\n"
            + "{[Product].[Product].[Food]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Warehouse Sales]}\n"
            + "{[Measures].[Store Sales]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[1997].[Q1].[1], [Store].[Store].[USA]}\n"
            + "{[Time].[Time].[1997].[Q1].[2], [Store].[Store].[USA]}\n"
            + "{[Time].[Time].[1997].[Q1].[3], [Store].[Store].[USA]}\n"
            + "{[Time].[Time].[1997].[Q2].[4], [Store].[Store].[USA]}\n"
            + "{[Time].[Time].[1997].[Q2].[5], [Store].[Store].[USA]}\n"
            + "{[Time].[Time].[1997].[Q2].[6], [Store].[Store].[USA]}\n"
            + "{[Time].[Time].[1997].[Q3].[7], [Store].[Store].[USA]}\n"
            + "{[Time].[Time].[1997].[Q3].[8], [Store].[Store].[USA]}\n"
            + "{[Time].[Time].[1997].[Q3].[9], [Store].[Store].[USA]}\n"
            + "{[Time].[Time].[1997].[Q4].[10], [Store].[Store].[USA]}\n"
            + "{[Time].[Time].[1997].[Q4].[11], [Store].[Store].[USA]}\n"
            + "{[Time].[Time].[1997].[Q4].[12], [Store].[Store].[USA]}\n"
            + "Row #0: 16,083.015\n"
            + "Row #0: 32,993.12\n"
            + "Row #1: 9,298.379\n"
            + "Row #1: 32,139.91\n"
            + "Row #2: 10,129.659\n"
            + "Row #2: 36,128.29\n"
            + "Row #3: 11,415.462\n"
            + "Row #3: 30,747.21\n"
            + "Row #4: 11,358.086\n"
            + "Row #4: 31,896.24\n"
            + "Row #5: 10,425.768\n"
            + "Row #5: 32,792.55\n"
            + "Row #6: 13,684.193\n"
            + "Row #6: 36,324.76\n"
            + "Row #7: 11,332.797\n"
            + "Row #7: 33,842.75\n"
            + "Row #8: 15,667.978\n"
            + "Row #8: 31,640.09\n"
            + "Row #9: 11,902.18\n"
            + "Row #9: 30,337.12\n"
            + "Row #10: 10,144.841\n"
            + "Row #10: 38,709.15\n"
            + "Row #11: 9,705.561\n"
            + "Row #11: 41,484.40\n";
        propSaver.set(propSaver.props.GenerateFormattedSql, true);
        assertQuerySql(getTestContext(), query, mysqlPattern, true);
        assertQueryReturns(query, result);
    }

    /**
     * Tests that the logic to apply non empty context constraint in virtual
     * cube is correct.  The joins shouldn't be cartesian product.
     */
    public void testNonEmptyConstraintOnVirtualCubeWithCalcMeasure() {
        if (!MondrianProperties.instance().EnableNativeNonEmpty.get()) {
            // Generated SQL is different if NON EMPTY is evaluated in memory.
            return;
        }
        String query =
            "with "
            + "set [bar] as {[Store].[USA]} "
            + "member [Measures].[CalcMeasure] as '[Measures].[Warehouse Sales] / [Measures].[Store Sales]' "
            + "Select "
            + "{[Measures].[CalcMeasure]} on columns, "
            + "non empty([Product].[Product Family].Members) on rows "
            + "From [Warehouse and Sales] "
            + "where [bar]";
        propSaver.set(propSaver.props.GenerateFormattedSql, true);
        // Comments as for testNonEmptyCJConstraintOnVirtualCube. The ORDER BY
        // clause should be "order by ISNULL(1), 1 ASC" but we will settle for
        // "order by 1 ASC" and forego correct sorting of NULL values.
        String mysqlSQL =
            "select\n"
            + "    `product_class`.`product_family` as `c0`\n"
            + "from\n"
            + "    `inventory_fact_1997` as `inventory_fact_1997`,\n"
            + "    `product` as `product`,\n"
            + "    `product_class` as `product_class`,\n"
            + "    `store` as `store`\n"
            + "where\n"
            + "    `store`.`store_country` = 'USA'\n"
            + "and\n"
            + "    `inventory_fact_1997`.`product_id` = `product`.`product_id`\n"
            + "and\n"
            + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
            + "and\n"
            + "    `inventory_fact_1997`.`store_id` = `store`.`store_id`\n"
            + "group by\n"
            + "    `product_class`.`product_family`\n"
            + "union\n"
            + "select\n"
            + "    `product_class`.`product_family` as `c0`\n"
            + "from\n"
            + "    `sales_fact_1997` as `sales_fact_1997`,\n"
            + "    `product` as `product`,\n"
            + "    `product_class` as `product_class`,\n"
            + "    `store` as `store`\n"
            + "where\n"
            + "    `store`.`store_country` = 'USA'\n"
            + "and\n"
            + "    `sales_fact_1997`.`product_id` = `product`.`product_id`\n"
            + "and\n"
            + "    `product`.`product_class_id` = `product_class`.`product_class_id`\n"
            + "and\n"
            + "    `sales_fact_1997`.`store_id` = `store`.`store_id`\n"
            + "group by\n"
            + "    `product_class`.`product_family`\n"
            + "order by\n"
            + "    1 ASC";

        String result =
            "Axis #0:\n"
            + "{[Store].[Store].[USA]}\n"
            + "Axis #1:\n"
            + "{[Measures].[CalcMeasure]}\n"
            + "Axis #2:\n"
            + "{[Product].[Product].[Drink]}\n"
            + "{[Product].[Product].[Food]}\n"
            + "{[Product].[Product].[Non-Consumable]}\n"
            + "Row #0: 0.369\n"
            + "Row #1: 0.345\n"
            + "Row #2: 0.35\n";

        SqlPattern[] patterns = {
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL, mysqlSQL, mysqlSQL)};

        assertQuerySql(getTestContext(), query, patterns, true);
        assertQueryReturns(query, result);
    }

    /**
     * Test case for bug <a href="http://jira.pentaho.com/browse/MONDRIAN-902">
     * MONDRIAN-902, "mondrian populating the same members on both axes"</a>.
     */
    public void testBugMondrian902() {
        Result result = executeQuery(
            "SELECT\n"
            + "NON EMPTY CrossJoin(\n"
            + "  [Education Level].[Education Level].[Education Level].Members,\n"
            + "  CrossJoin(\n"
            + "    [Product].[Product Family].Members,\n"
            + "    [Store].[Store State].Members)) ON COLUMNS,\n"
            + "NON EMPTY CrossJoin(\n"
            + "  [Promotions].[Promotion Name].Members,\n"
            + "  [Marital Status].[Marital Status].[Marital Status].Members) ON ROWS\n"
            + "FROM [Warehouse and Sales]");
        assertEquals(
            "[[Education Level].[Education Level].[Bachelors Degree], [Product].[Product].[Drink], [Store].[Store].[USA].[CA]]",
            result.getAxes()[0].getPositions().get(0).toString());
        assertEquals(45, result.getAxes()[0].getPositions().size());
        // With bug MONDRIAN-902, this gave the same result as for axis #0:
        assertEquals(
            "[[Promotions].[Promotions].[Bag Stuffers], [Marital Status].[Marital Status].[M]]",
            result.getAxes()[1].getPositions().get(0).toString());
        assertEquals(96, result.getAxes()[1].getPositions().size());
    }

    /**
     * <p>MONDRIAN-1061</p>
     * <p>recursive members for 4.0 schema version</p>
     * @throws IOException
     */
    public void testVirtualCubeRecursiveMember() {
      String cube = "<Cube name='MONDRIAN-1062' defaultMeasure='Unit Sales'>"
         + "<Dimensions>"
         + "<Dimension source='Time'/>"
         + "</Dimensions>"
         + "<MeasureGroups>"
         + "<MeasureGroup name='Sales' table='sales_fact_1997'>"
         + "<Measures>"
         + "<Measure name='Unit Sales' column='unit_sales' aggregator='sum' formatString='Standard'/>"
         + "</Measures>"
         + "<DimensionLinks>"
         + "<ForeignKeyLink dimension='Time' foreignKeyColumn='time_id'/>"
         + "</DimensionLinks>"
         + "</MeasureGroup>"
         + "</MeasureGroups>"
    + "<CalculatedMembers>"
    + "<CalculatedMember name='RECURSIVE' dimension='Measures' "
    + "formula='COALESCEEMPTY((Measures.[Unit Sales], [Time].[Time].CurrentMember), "
    + "(Measures.[RECURSIVE], [Time].[Time].CurrentMember.PrevMember))'"
    + " visible='true'>"
    + "<CalculatedMemberProperty name='FORMAT_STRING' value='#,##' />"
    + "</CalculatedMember>"
    + "</CalculatedMembers>"
    + "</Cube>";
      TestContext context = TestContext.instance()
          .create(null, cube, null, null, null, null);
      String query = "select {[RECURSIVE]} on rows, "
          + "{[Time].[1998].Children} on columns "
          + "from [MONDRIAN-1062]";
      final String expected = "Axis #0:\n"
        + "{}\n"
        + "Axis #1:\n"
        + "{[Time].[Time].[1998].[Q1]}\n"
        + "{[Time].[Time].[1998].[Q2]}\n"
        + "{[Time].[Time].[1998].[Q3]}\n"
        + "{[Time].[Time].[1998].[Q4]}\n"
        + "Axis #2:\n"
        + "{[Measures].[RECURSIVE]}\n"
        + "Row #0: 72,024\n"
        + "Row #0: 72,024\n"
        + "Row #0: 72,024\n"
        + "Row #0: 72,024\n";
      context.assertQueryReturns(query, expected);
    }
}

// End VirtualCubeTest.java
