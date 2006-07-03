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
        final Cube cube = schema.createCube(fold(new String[] {
                "<VirtualCube name=\"Sales vs Warehouse\">",
                "<VirtualCubeDimension name=\"Product\"/>",
                "<VirtualCubeMeasure cubeName=\"Warehouse\" name=\"[Measures].[Warehouse Sales]\"/>",
                "<VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Unit Sales]\"/>",
                "</VirtualCube>"}));

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
        final Cube cube = schema.createCube(fold(new String[] {
                "<VirtualCube name=\"Sales vs Warehouse\">",
                "<VirtualCubeDimension name=\"Time\"/>",
                "<VirtualCubeDimension name=\"Product\"/>",
                "<VirtualCubeMeasure cubeName=\"Warehouse\" name=\"[Measures].[Warehouse Sales]\"/>",
                "<VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Unit Sales]\"/>",
                "</VirtualCube>"}));

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
        assertQueryReturns(
            "select {[Warehouse].defaultMember} on columns, " +
            "{[Measures].[Warehouse Cost]} on rows from [Warehouse (Default USA)]",
            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Warehouse].[USA]}\n" +
                "Axis #2:\n" +
                "{[Measures].[Warehouse Cost]}\n" +
                "Row #0: 89,043.253\n"));  
        
        assertQueryReturns(
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
}

// End VirtualCubeTest.java
