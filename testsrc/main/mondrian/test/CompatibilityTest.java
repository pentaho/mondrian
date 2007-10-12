/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2005 SAS Institute, Inc.
// Copyright (C) 2006-2006 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.test;

import mondrian.olap.MondrianProperties;
import mondrian.olap.Result;
import mondrian.olap.Cell;
import mondrian.olap.Util;
import junit.framework.Assert;

/**
 * <code>CompatibilityTest</code> is a test case which tests
 * MDX syntax compatibility with Microsoft and SAS servers.
 * There is no MDX spec document per se, so compatibility with de-facto
 * standards from the major vendors is important. Uses the FoodMart database.
 *
 * @author sasebb
 * @since March 30, 2005
 * @version $Id$
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
     */
    public void testCaseInsensitiveNullMember() {
        if (getTestContext().getDialect().isLucidDB()) {
            // TODO jvs 29-Nov-2006:  LucidDB is strict about
            // null literals (type can't be inferred in this context);
            // maybe enhance the inline table to use the columndef
            // types to apply a CAST.
            return;
        }
        final String cubeName = "Sales_inline";
        TestContext testContext = TestContext.create(
            null,
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
                "</Cube>",
            null, null, null, null);

        // This test should work irrespective of the case-sensitivity setting.
        Util.discard(MondrianProperties.instance().CaseSensitive.get());

        testContext.assertQueryReturns(
            "select {[Measures].[Unit Sales]} ON COLUMNS,\n" +
                "  {[Alternative Promotion].[All Alternative Promotions].[#null]} ON ROWS \n" +
                "  from [Sales_inline]",
            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Unit Sales]}\n" +
                "Axis #2:\n" +
                "{[Alternative Promotion].[All Alternative Promotions].[#null]}\n" +
                "Row #0: \n"));
    }

    /**
     * Tests that data in Hierarchy.Level attribute "nameColumn" can be null.
     * This will map to the #null memeber.
     */
    public void testNullNameColumn() {
        if (getTestContext().getDialect().isLucidDB()) {
            // TODO jvs 29-Nov-2006:  See corresponding comment in
            // testCaseInsensitiveNullMember
            return;
        }
        final String cubeName = "Sales_inline";
        TestContext testContext = TestContext.create(
            null,
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
                "</Cube>",
            null, null, null, null);

        testContext.assertQueryReturns(
            "select {" +
                "[Alternative Promotion].[All Alternative Promotions].[#null], " +
                "[Alternative Promotion].[All Alternative Promotions].[Promo1]} ON COLUMNS\n" +
                "from [" + cubeName + "] ",
            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Alternative Promotion].[All Alternative Promotions].[#null]}\n" +
                "{[Alternative Promotion].[All Alternative Promotions].[Promo1]}\n" +
                "Row #0: 195,448\n" +
                "Row #0: \n"));
    }

    /**
     * Tests that NULL values sort last on all platforms. On some platforms,
     * such as MySQL, NULLs naturally come before other values, so we have to
     * generate a modified ORDER BY clause.
      */
    public void testNullCollation() {
        if (!getTestContext().getDialect().supportsGroupByExpressions()) {
            // Derby does not support expressions in the GROUP BY clause,
            // therefore this testing strategy of using an expression for the
            // store key won't work. Give the test a bye.
            return;
        }
        final String cubeName = "Store_NullsCollation";
        TestContext testContext = TestContext.create(
            null,
            "<Cube name=\"" + cubeName + "\">\n" +
                "  <Table name=\"store\"/>\n" +
                "  <Dimension name=\"Store\" foreignKey=\"store_id\">\n" +
                "    <Hierarchy hasAll=\"true\" primaryKey=\"store_id\">\n" +
                "      <Level name=\"Store Name\" column=\"store_name\"  uniqueMembers=\"true\">\n" +
                "       <OrdinalExpression>\n" +
                "        <SQL dialect=\"access\">\n" +
                "           Iif(store_name = 'HQ', null, store_name)\n" +
                "       </SQL>\n" +
                "        <SQL dialect=\"oracle\">\n" +
                "           case \"store_name\" when 'HQ' then null else \"store_name\" end\n" +
                "       </SQL>\n" +
                "        <SQL dialect=\"db2\">\n" +
                "           case \"store\".\"store_name\" when 'HQ' then null else \"store\".\"store_name\" end\n" +
                "       </SQL>\n" +
                "        <SQL dialect=\"luciddb\">\n" +
                "           case \"store_name\" when 'HQ' then null else \"store_name\" end\n" +
                "       </SQL>\n" +
                "        <SQL dialect=\"generic\">\n" +
                "           case store_name when 'HQ' then null else store_name end\n" +
                "       </SQL>\n" +
                "       </OrdinalExpression>\n" +
                "        <Property name=\"Store Sqft\" column=\"store_sqft\" type=\"Numeric\"/>\n" +
                "      </Level>\n" +
                "    </Hierarchy>\n" +
                "  </Dimension>\n" +
                "  <Measure name=\"Store Sqft\" column=\"store_sqft\" aggregator=\"sum\"\n" +
                "      formatString=\"#,###\"/>\n" +
                "</Cube>",
            null, null, null, null);

        testContext.assertQueryReturns(
            "select { [Measures].[Store Sqft] } on columns,\n" +
                " NON EMPTY topcount(\n" +
                "    {[Store].[Store Name].members},\n" +
                "    5,\n" +
                "    [measures].[store sqft]) on rows\n" +
                "from [" + cubeName + "] ",
            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Store Sqft]}\n" +
                "Axis #2:\n" +
                "{[Store].[All Stores].[Store 3]}\n" +
                "{[Store].[All Stores].[Store 18]}\n" +
                "{[Store].[All Stores].[Store 9]}\n" +
                "{[Store].[All Stores].[Store 10]}\n" +
                "{[Store].[All Stores].[Store 20]}\n" +
                "Row #0: 39,696\n" +
                "Row #1: 38,382\n" +
                "Row #2: 36,509\n" +
                "Row #3: 34,791\n" +
                "Row #4: 34,452\n"));
    }

    /**
     * Tests that property names are case sensitive iff the
     * "mondrian.olap.case.sensitive" property is set.
     *
     * <p>The test does not alter this property: for testing coverage, we assume
     * that you run the test once with mondrian.olap.case.sensitive=true,
     * and once with mondrian.olap.case.sensitive=false.
     */
    public void testPropertyCaseSensitivity() {
        boolean caseSensitive = MondrianProperties.instance().CaseSensitive.get();

        // A user-defined property of a member.
        assertExprReturns(
            "[Store].[USA].[CA].[Beverly Hills].[Store 6].Properties(\"Store Type\")",
            "Gourmet Supermarket");

        if (caseSensitive) {
            assertExprThrows(
                "[Store].[USA].[CA].[Beverly Hills].[Store 6].Properties(\"store tYpe\")",
                "Property 'store tYpe' is not valid for member '[Store].[All Stores].[USA].[CA].[Beverly Hills].[Store 6]'");
        } else {
            assertExprReturns(
                "[Store].[USA].[CA].[Beverly Hills].[Store 6].Properties(\"store tYpe\")",
                "Gourmet Supermarket");
        }

        // A builtin property of a member.
        assertExprReturns(
            "[Store].[USA].[CA].[Beverly Hills].[Store 6].Properties(\"LEVEL_NUMBER\")",
            "4");
        if (caseSensitive) {
            assertExprThrows(
                "[Store].[USA].[CA].[Beverly Hills].[Store 6].Properties(\"Level_Number\")",
                "Property 'store tYpe' is not valid for member '[Store].[All Stores].[USA].[CA].[Beverly Hills].[Store 6]'");
        } else {
            assertExprReturns(
                "[Store].[USA].[CA].[Beverly Hills].[Store 6].Properties(\"Level_Number\")",
                "4");
        }

        // Cell properties.
        Result result = executeQuery(
            "select {[Measures].[Unit Sales],[Measures].[Store Sales]} on columns,\n" +
                " {[Gender].[M]} on rows\n" +
                "from Sales");
        Cell cell = result.getCell(new int[]{0, 0});
        assertEquals("135,215", cell.getPropertyValue("FORMATTED_VALUE"));
        if (caseSensitive) {
            assertNull(cell.getPropertyValue("Formatted_Value"));
        } else {
            assertEquals("135,215", cell.getPropertyValue("Formatted_Value"));
        }
    }
}

// End CompatibilityTest.java
