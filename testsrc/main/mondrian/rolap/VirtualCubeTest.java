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
    public void _testNoTimeDimension() {
        Schema schema = getConnection().getSchema();
        final Cube cube = schema.createCube(
                "<VirtualCube name=\"Sales vs Warehouse\">\n" +
                "<VirtualCubeDimension name=\"Product\"/>\n" +
                "<VirtualCubeMeasure cubeName=\"Warehouse\" name=\"[Measures].[Warehouse Sales]\"/>\n" +
                "<VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Unit Sales]\"/>\n" +
                "</VirtualCube>");

        try  {
            checkXxx();
        } finally {
            schema.removeCube(cube.getName());
        }
    }

    /**
     * I do not know/believe that the return values are correct.
     */
    public void testWithTimeDimension() {
        Schema schema = getConnection().getSchema();
        final Cube cube = schema.createCube(
                "<VirtualCube name=\"Sales vs Warehouse\">\n" +
                "<VirtualCubeDimension name=\"Time\"/>\n" +
                "<VirtualCubeDimension name=\"Product\"/>\n" +
                "<VirtualCubeMeasure cubeName=\"Warehouse\" name=\"[Measures].[Warehouse Sales]\"/>\n" +
                "<VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Unit Sales]\"/>\n" +
                "</VirtualCube>");

        try  {
            checkXxx();

        } finally {
            schema.removeCube(cube.getName());
        }
    }


    private void checkXxx() {
        // I do not know/believe that the return values are correct.

        assertQueryReturns(
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
        TestContext testContext = TestContext.create(null, null,
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

        testContext.assertQueryReturns(
            "select {[Warehouse].defaultMember} on columns, " +
            "{[Measures].[Warehouse Cost], [Measures].[Sales Count]} on rows " +
            "from [Warehouse (Default USA) and Sales]",
            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Warehouse].[USA]}\n" +
                "Axis #2:\n" +
                "{[Measures].[Warehouse Cost]}\n" +
                "{[Measures].[Sales Count]}\n" +
                "Row #0: 89,043.253\n" +
                "Row #1: \n"));
    }

    // disabled pending fix
    public void _testMemberVisibility() {
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
        Position[] columnPositions = result.getAxes()[0].positions;
        Member measure = columnPositions[ordinal].getMembers()[0];
        assertEquals(expectedName, measure.getName());
        assertEquals(expectedVisibility, measure.getPropertyValue(Property.VISIBLE.name));
    }
}

// End VirtualCubeTest.java
