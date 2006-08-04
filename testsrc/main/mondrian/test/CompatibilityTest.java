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

import mondrian.olap.Cube;
import mondrian.olap.MondrianProperties;
import mondrian.olap.Schema;
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

        assertQueryReturns(queryFrom + "[Sales]", result);
        assertQueryReturns(queryFrom + "[SALES]", result);
        assertQueryReturns(queryFrom + "[sAlEs]", result);
        assertQueryReturns(queryFrom + "[sales]", result);
    }

    /**
     * Brackets around cube names are optional.
     */
    public void testCubeBrackets() {
        String queryFrom = "select {[Measures].[Unit Sales]} on columns from ";
        String result = "Axis #0:" + nl + "{}" + nl + "Axis #1:" + nl + "{[Measures].[Unit Sales]}"
                + nl + "Row #0: 266,773" + nl;

        assertQueryReturns(queryFrom + "Sales", result);
        assertQueryReturns(queryFrom + "SALES", result);
        assertQueryReturns(queryFrom + "sAlEs", result);
        assertQueryReturns(queryFrom + "sales", result);
    }

    /**
     * See how we are at diagnosing reserved words.
     */
    public void testReservedWord() {
        assertAxisThrows("with member [Measures].ordinal as '1'" + nl
                    + " select {[Measures].ordinal} on columns from Sales", "Syntax error");
        assertQueryReturns("with member [Measures].[ordinal] as '1'" + nl
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
        assertQueryReturns("with member [Measures].[CaLc] as '1'" + nl
                + " select {[Measures].[CaLc]} on columns from Sales", "Axis #0:" + nl + "{}" + nl
                + "Axis #1:" + nl + "{[Measures].[CaLc]}" + nl + "Row #0: 1" + nl);
        assertQueryReturns("with member [Measures].[CaLc] as '1'" + nl
                + " select {[Measures].[cAlC]} on columns from Sales", "Axis #0:" + nl + "{}" + nl
                + "Axis #1:" + nl + "{[Measures].[CaLc]}" + nl + "Row #0: 1" + nl);
        assertQueryReturns("with member [mEaSuReS].[CaLc] as '1'" + nl
                + " select {[MeAsUrEs].[cAlC]} on columns from Sales", "Axis #0:" + nl + "{}" + nl
                + "Axis #1:" + nl + "{[Measures].[CaLc]}" + nl + "Row #0: 1" + nl);
    }

    /**
     * Solve order is case insensitive.
     */
    public void testSolveOrderCase() {
        checkSolveOrder("SOLVE_ORDER");
        checkSolveOrder("SoLvE_OrDeR");
        checkSolveOrder("solve_order");
    }

    private void checkSolveOrder(String keyword) {
        assertQueryReturns(
                "WITH" + nl +
                "   MEMBER [Store].[StoreCalc] as '0', " + keyword + "=0" + nl +
                "   MEMBER [Product].[ProdCalc] as '1', " + keyword + "=1" + nl +
                "SELECT" + nl +
                "   { [Product].[ProdCalc] } ON columns," + nl +
                "   { [Store].[StoreCalc] } ON rows" + nl +
                "FROM Sales",

                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Product].[ProdCalc]}" + nl +
                "Axis #2:" + nl +
                "{[Store].[StoreCalc]}" + nl +
                "Row #0: 1" + nl);
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
        Assert.assertEquals(result, executeSingletonAxis(expression).toString());
    }


    /**
     * Tests that a #null member on a Hiearchy Level of type String can
     * still be looked up when case sensitive is off.
     *
     */
    public void testCaseInsensitiveNullMember() {
    	boolean old = MondrianProperties.instance().CaseSensitive.get();
    	MondrianProperties.instance().CaseSensitive.set(false);

    	Schema schema = getConnection().getSchema();
        final String cubeName = "Sales_inline";
        final Cube cube = schema.createCube(
            "<Cube name=\"" + cubeName + "\">\n" +
            "  <Table name=\"sales_fact_1997\"/>\n" +
            "  <DimensionUsage name=\"Time\" source=\"Time\" foreignKey=\"time_id\"/>\n" +
            "  <Dimension name=\"Alternative Promotion\" foreignKey=\"promotion_id\">\n" +
            "    <Hierarchy hasAll=\"true\" primaryKey=\"promo_id\">\n" +
            "      <InlineTable alias=\"alt_promotion\">\n" +
            "        <ColumnDefs>\n" +
            "          <ColumnDef name=\"promo_id\" type=\"Numeric\"/>\n" +
            "          <ColumnDef name=\"promo_name\" type=\"String\"/>\n" +
            "        </ColumnDefs>\n" +
            "        <Rows>\n" +
            "          <Row>\n" +
            "            <Value column=\"promo_id\">0</Value>\n" +
            "            <Value column=\"promo_name\">Promo0</Value>\n" +
            "          </Row>\n" +
            "          <Row>\n" +
            "            <Value column=\"promo_id\">1</Value>\n" +
            "          </Row>\n" +
            "        </Rows>\n" +
            "      </InlineTable>\n" +
            "      <Level name=\"Alternative Promotion\" column=\"promo_name\" uniqueMembers=\"true\"/> \n" +
            "    </Hierarchy>\n" +
            "  </Dimension>\n" +
            "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\"\n" +
            "      formatString=\"Standard\" visible=\"false\"/>\n" +
            "  <Measure name=\"Store Sales\" column=\"store_sales\" aggregator=\"sum\"\n" +
            "      formatString=\"#,###.00\"/>\n" +
            "</Cube>");

        try {
	    	getTestContext().assertQueryReturns(
		            "select {[Measures].[Unit Sales]} ON COLUMNS,\n" +
		            "  {[Alternative Promotion].[All Alternative Promotions].[#null]} ON ROWS \n" +
		            "  from [Sales_inline]",
		            fold(
	                        "Axis #0:\n" +
	                        "{}\n" +
	                        "Axis #1:\n" +
	                        "{[Measures].[Unit Sales]}\n" +
	                        "Axis #2:\n" +
	                        "{[Alternative Promotion].[All Alternative Promotions].[#null]}\n" +
	                        "Row #0: \n"));
        } finally {
            schema.removeCube(cubeName);
        	MondrianProperties.instance().CaseSensitive.set(old);
        }
    }

    /**
     * Tests that data in Hiearchy.Level attribute "nameColumn" can be null.  This will map
     * to the #null memeber.
     */
    public void testNullNameColumn() {
    	Schema schema = getConnection().getSchema();
        final String cubeName = "Sales_inline";
        final Cube cube = schema.createCube(
            "<Cube name=\"" + cubeName + "\">\n" +
            "  <Table name=\"sales_fact_1997\"/>\n" +
            "  <DimensionUsage name=\"Time\" source=\"Time\" foreignKey=\"time_id\"/>\n" +
            "  <Dimension name=\"Alternative Promotion\" foreignKey=\"promotion_id\">\n" +
            "    <Hierarchy hasAll=\"true\" primaryKey=\"promo_id\">\n" +
            "      <InlineTable alias=\"alt_promotion\">\n" +
            "        <ColumnDefs>\n" +
            "          <ColumnDef name=\"promo_id\" type=\"Numeric\"/>\n" +
            "          <ColumnDef name=\"promo_name\" type=\"String\"/>\n" +
            "        </ColumnDefs>\n" +
            "        <Rows>\n" +
            "          <Row>\n" +
            "            <Value column=\"promo_id\">0</Value>\n" +
            "          </Row>\n" +
            "          <Row>\n" +
            "            <Value column=\"promo_id\">1</Value>\n" +
            "            <Value column=\"promo_name\">Promo1</Value>\n" +
            "          </Row>\n" +
            "        </Rows>\n" +
            "      </InlineTable>\n" +
            "      <Level name=\"Alternative Promotion\" column=\"promo_id\" nameColumn=\"promo_name\" uniqueMembers=\"true\"/> \n" +
            "    </Hierarchy>\n" +
            "  </Dimension>\n" +
            "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\"\n" +
            "      formatString=\"Standard\" visible=\"false\"/>\n" +
            "  <Measure name=\"Store Sales\" column=\"store_sales\" aggregator=\"sum\"\n" +
            "      formatString=\"#,###.00\"/>\n" +
            "</Cube>");

        try {
            getTestContext().assertQueryReturns(
                    fold(
                        "select {[Alternative Promotion].[All Alternative Promotions].[#null], [Alternative Promotion].[All Alternative Promotions].[Promo1]} ON COLUMNS\n" +
                        "from [" + cubeName + "] "),
                    fold(
                        "Axis #0:\n" +
                        "{}\n" +
                        "Axis #1:\n" +
                        "{[Alternative Promotion].[All Alternative Promotions].[#null]}\n" +
                        "{[Alternative Promotion].[All Alternative Promotions].[Promo1]}\n" +
                        "Row #0: 195,448\n" +
                        "Row #0: \n"));
        } finally {
            schema.removeCube(cubeName);
        }
    }
}

// End CompatibilityTest.java
