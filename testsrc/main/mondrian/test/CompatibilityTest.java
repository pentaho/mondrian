/*
 // $Id$
 // This software is subject to the terms of the Common Public License
 // Agreement, available at the following URL:
 // http://www.opensource.org/licenses/cpl.html.
 // Copyright (C) 2005 SAS Institute, Inc.
 // All Rights Reserved.
 // You must accept the terms of that agreement to use this software.
 //
 // sasebb, March 30, 2005
 */
package mondrian.test;

import junit.framework.Assert;

/**
 * <code>CompatibilityTest</code> is a test case which tests
 * MDX syntax compatibility with Microsoft and SAS servers.
 * There is no MDX spec document per se, so compatibility with de-facto
 * standards from the major vendors is important. Uses the FoodMart database.
 *
 * @author sasebb
 * @since March 30, 2005
 */
public class CompatibilityTest extends FoodMartTestCase {
    public CompatibilityTest(String name) {
        super(name);
    }

    /**
     * Cube names are case insensitive.
     */
    public void testCubeCase() {
        String queryFrom = "select {[Measures].[Unit Sales]} on columns from ";
        String result = "Axis #0:" + nl + "{}" + nl + "Axis #1:" + nl + "{[Measures].[Unit Sales]}"
                + nl + "Row #0: 266,773" + nl;

        runQueryCheckResult(queryFrom + "[Sales]", result);
        runQueryCheckResult(queryFrom + "[SALES]", result);
        runQueryCheckResult(queryFrom + "[sAlEs]", result);
        runQueryCheckResult(queryFrom + "[sales]", result);
    }

    /**
     * Brackets around cube names are optional.
     */
    public void testCubeBrackets() {
        String queryFrom = "select {[Measures].[Unit Sales]} on columns from ";
        String result = "Axis #0:" + nl + "{}" + nl + "Axis #1:" + nl + "{[Measures].[Unit Sales]}"
                + nl + "Row #0: 266,773" + nl;

        runQueryCheckResult(queryFrom + "Sales", result);
        runQueryCheckResult(queryFrom + "SALES", result);
        runQueryCheckResult(queryFrom + "sAlEs", result);
        runQueryCheckResult(queryFrom + "sales", result);
    }

    /**
     * See how we are at diagnosing reserved words.
     */
    public void testReservedWord() {
        assertAxisThrows("with member [Measures].ordinal as '1'" + nl
                    + " select {[Measures].ordinal} on columns from Sales", "Syntax error");
        runQueryCheckResult("with member [Measures].[ordinal] as '1'" + nl
                + " select {[Measures].[ordinal]} on columns from Sales", "Axis #0:" + nl + "{}"
                + nl + "Axis #1:" + nl + "{[Measures].[ordinal]}" + nl + "Row #0: 1" + nl);
    }

    /**
     * Dimension names are case insensitive.
     */
    public void testDimensionCase() {
        checkAxis("[Measures].[Unit Sales]", "[Measures].[Unit Sales]");
        checkAxis("[Measures].[Unit Sales]", "[MEASURES].[Unit Sales]");
        checkAxis("[Measures].[Unit Sales]", "[mEaSuReS].[Unit Sales]");
        checkAxis("[Measures].[Unit Sales]", "[measures].[Unit Sales]");

        checkAxis("[Customers].[All Customers]", "[Customers].[All Customers]");
        checkAxis("[Customers].[All Customers]", "[CUSTOMERS].[All Customers]");
        checkAxis("[Customers].[All Customers]", "[cUsToMeRs].[All Customers]");
        checkAxis("[Customers].[All Customers]", "[customers].[All Customers]");
    }

    /**
     * Brackets around dimension names are optional.
     */
    public void testDimensionBrackets() {
        checkAxis("[Measures].[Unit Sales]", "Measures.[Unit Sales]");
        checkAxis("[Measures].[Unit Sales]", "MEASURES.[Unit Sales]");
        checkAxis("[Measures].[Unit Sales]", "mEaSuReS.[Unit Sales]");
        checkAxis("[Measures].[Unit Sales]", "measures.[Unit Sales]");

        checkAxis("[Customers].[All Customers]", "Customers.[All Customers]");
        checkAxis("[Customers].[All Customers]", "CUSTOMERS.[All Customers]");
        checkAxis("[Customers].[All Customers]", "cUsToMeRs.[All Customers]");
        checkAxis("[Customers].[All Customers]", "customers.[All Customers]");
    }

    /**
     * Member names are case insensitive.
     */
    public void testMemberCase() {
        checkAxis("[Measures].[Unit Sales]", "[Measures].[UNIT SALES]");
        checkAxis("[Measures].[Unit Sales]", "[Measures].[uNiT sAlEs]");
        checkAxis("[Measures].[Unit Sales]", "[Measures].[unit sales]");

        checkAxis("[Measures].[Profit]", "[Measures].[Profit]");
        checkAxis("[Measures].[Profit]", "[Measures].[pRoFiT]");
        checkAxis("[Measures].[Profit]", "[Measures].[PROFIT]");
        checkAxis("[Measures].[Profit]", "[Measures].[profit]");

        checkAxis("[Customers].[All Customers]", "[Customers].[All Customers]");
        checkAxis("[Customers].[All Customers]", "[Customers].[ALL CUSTOMERS]");
        checkAxis("[Customers].[All Customers]", "[Customers].[aLl CuStOmErS]");
        checkAxis("[Customers].[All Customers]", "[Customers].[all customers]");

        checkAxis("[Customers].[All Customers].[Mexico]", "[Customers].[All Customers].[Mexico]");
        checkAxis("[Customers].[All Customers].[Mexico]", "[Customers].[All Customers].[MEXICO]");
        checkAxis("[Customers].[All Customers].[Mexico]", "[Customers].[All Customers].[mExIcO]");
        checkAxis("[Customers].[All Customers].[Mexico]", "[Customers].[All Customers].[mexico]");
    }

    /**
     * Calculated member names are case insensitive.
     */
    public void testCalculatedMemberCase() {
        runQueryCheckResult("with member [Measures].[CaLc] as '1'" + nl
                + " select {[Measures].[CaLc]} on columns from Sales", "Axis #0:" + nl + "{}" + nl
                + "Axis #1:" + nl + "{[Measures].[CaLc]}" + nl + "Row #0: 1" + nl);
        runQueryCheckResult("with member [Measures].[CaLc] as '1'" + nl
                + " select {[Measures].[cAlC]} on columns from Sales", "Axis #0:" + nl + "{}" + nl
                + "Axis #1:" + nl + "{[Measures].[CaLc]}" + nl + "Row #0: 1" + nl);
        runQueryCheckResult("with member [mEaSuReS].[CaLc] as '1'" + nl
                + " select {[MeAsUrEs].[cAlC]} on columns from Sales", "Axis #0:" + nl + "{}" + nl
                + "Axis #1:" + nl + "{[Measures].[CaLc]}" + nl + "Row #0: 1" + nl);
    }

    /**
     * Brackets around member names are optional.
     */
    public void testMemberBrackets() {
        checkAxis("[Measures].[Profit]", "[Measures].Profit");
        checkAxis("[Measures].[Profit]", "[Measures].pRoFiT");
        checkAxis("[Measures].[Profit]", "[Measures].PROFIT");
        checkAxis("[Measures].[Profit]", "[Measures].profit");

        checkAxis("[Customers].[All Customers].[Mexico]", "[Customers].[All Customers].Mexico");
        checkAxis("[Customers].[All Customers].[Mexico]", "[Customers].[All Customers].MEXICO");
        checkAxis("[Customers].[All Customers].[Mexico]", "[Customers].[All Customers].mExIcO");
        checkAxis("[Customers].[All Customers].[Mexico]", "[Customers].[All Customers].mexico");
    }

    /**
     * Hierarchy names of the form [Dim].[Hier], [Dim.Hier], and Dim.Hier are accepted.
     */
    public void testHierarchyNames() {
        checkAxis("[Customers].[All Customers]", "[Customers].[All Customers]");
        checkAxis("[Customers].[All Customers]", "[Customers].[Customers].[All Customers]");
        checkAxis("[Customers].[All Customers]", "Customers.[Customers].[All Customers]");
        checkAxis("[Customers].[All Customers]", "[Customers].Customers.[All Customers]");
        // don't know if this makes sense: checkAxis("[Customers].[All Customers]", "[Customers.Customers].[All Customers]");
    }

    private void checkAxis(String result, String expression) {
        Assert.assertEquals(result, executeAxis(expression).toString());
    }

}
