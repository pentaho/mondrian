/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2003-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Feb 14, 2003
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.test.FoodMartTestCase;
/*
import mondrian.rolap.RolapConnection;
import mondrian.rolap.cache.CachePool;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.regex.Pattern;
import java.io.PrintWriter;

import junit.framework.Assert;
*/

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
    static class QueryAndResult {
        String query;
        String result;
        QueryAndResult(String query, String result) {
            this.query = query;
            this.result = result;
        }
    }
    private static final QueryAndResult simpleQuery = 
            new QueryAndResult(
                "select" + nl +
                "{ [Measures].[Warehouse Sales], [Measures].[Unit Sales] }" + nl +
                "ON COLUMNS," + nl +
                "{[Product].[All Products]}" + nl +
                "ON ROWS" + nl +
                "from [Sales vs Warehouse]",

                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Warehouse Sales]}" + nl +
                "{[Measures].[Unit Sales]}" + nl +
                "Axis #2:" + nl +
                "{[Product].[All Products]}" + nl +
                "Row #0: 196770.8876" + nl +
                "Row #0: 266,773" + nl

            );

    /** 
     * This method demonstrates bug 1449929
     */
    public void testNoTimeDimension() {
        Schema schema = getConnection().getSchema();
        final Cube cube = schema.createCube(fold(new String[] {
                "<VirtualCube name=\"Sales vs Warehouse\">",
                "<VirtualCubeDimension name=\"Product\"/>",
                "<VirtualCubeMeasure cubeName=\"Warehouse\" name=\"[Measures].[Warehouse Sales]\"/>",
                "<VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Unit Sales]\"/>",
                "</VirtualCube>"}));

        try  {
            assertQueryReturns(simpleQuery.query, simpleQuery.result);

        } finally {
            ((RolapSchema)schema).removeCube(cube);
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
            assertQueryReturns(simpleQuery.query, simpleQuery.result);

        } finally {
            ((RolapSchema)schema).removeCube(cube);
        }
    }
}

// End VirtualCubeTest.java
