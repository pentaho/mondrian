/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2005 SAS Institute, Inc.
// Copyright (C) 2006-2012 Pentaho and others
// All Rights Reserved.
*/
package mondrian.test;

import mondrian.olap.*;
import mondrian.spi.Dialect;

import junit.framework.Assert;

/**
 * Test for MDX syntax compatibility with Microsoft and SAS servers.
 *
 * <p>There is no MDX spec document per se, so compatibility with de facto
 * standards from the major vendors is important. Uses the FoodMart
 * database.</p>
 *
 * @see Ssas2005CompatibilityTest
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
        String result =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Row #0: 266,773\n";

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
        String result =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Row #0: 266,773\n";

        assertQueryReturns(queryFrom + "Sales", result);
        assertQueryReturns(queryFrom + "SALES", result);
        assertQueryReturns(queryFrom + "sAlEs", result);
        assertQueryReturns(queryFrom + "sales", result);
    }

    /**
     * See how we are at diagnosing reserved words.
     */
    public void testReservedWord() {
        assertAxisThrows(
            "with member [Measures].ordinal as '1'\n"
            + " select {[Measures].ordinal} on columns from Sales",
            "Syntax error");
        assertQueryReturns(
            "with member [Measures].[ordinal] as '1'\n"
            + " select {[Measures].[ordinal]} on columns from Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[ordinal]}\n"
            + "Row #0: 1\n");
    }

    /**
     * Dimension names are case insensitive.
     */
    public void testDimensionCase() {
        checkAxis("[Measures].[Unit Sales]", "[Measures].[Unit Sales]");
        checkAxis("[Measures].[Unit Sales]", "[MEASURES].[Unit Sales]");
        checkAxis("[Measures].[Unit Sales]", "[mEaSuReS].[Unit Sales]");
        checkAxis("[Measures].[Unit Sales]", "[measures].[Unit Sales]");

        checkAxis(
            "[Customer].[Customers].[All Customers]",
            "[Customers].[All Customers]");
        checkAxis(
            "[Customer].[Customers].[All Customers]",
            "[CUSTOMERS].[All Customers]");
        checkAxis(
            "[Customer].[Customers].[All Customers]",
            "[cUsToMeRs].[All Customers]");
        checkAxis(
            "[Customer].[Customers].[All Customers]",
            "[customers].[All Customers]");
    }

    /**
     * Brackets around dimension names are optional.
     */
    public void testDimensionBrackets() {
        checkAxis("[Measures].[Unit Sales]", "Measures.[Unit Sales]");
        checkAxis("[Measures].[Unit Sales]", "MEASURES.[Unit Sales]");
        checkAxis("[Measures].[Unit Sales]", "mEaSuReS.[Unit Sales]");
        checkAxis("[Measures].[Unit Sales]", "measures.[Unit Sales]");

        checkAxis(
            "[Customer].[Customers].[All Customers]",
            "Customers.[All Customers]");
        checkAxis(
            "[Customer].[Customers].[All Customers]",
            "CUSTOMERS.[All Customers]");
        checkAxis(
            "[Customer].[Customers].[All Customers]",
            "cUsToMeRs.[All Customers]");
        checkAxis(
            "[Customer].[Customers].[All Customers]",
            "customers.[All Customers]");
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

        checkAxis(
            "[Customer].[Customers].[All Customers]",
            "[Customers].[All Customers]");
        checkAxis(
            "[Customer].[Customers].[All Customers]",
            "[Customers].[ALL CUSTOMERS]");
        checkAxis(
            "[Customer].[Customers].[All Customers]",
            "[Customers].[aLl CuStOmErS]");
        checkAxis(
            "[Customer].[Customers].[All Customers]",
            "[Customers].[all customers]");

        checkAxis("[Customer].[Customers].[Mexico]", "[Customers].[Mexico]");
        checkAxis("[Customer].[Customers].[Mexico]", "[Customers].[MEXICO]");
        checkAxis("[Customer].[Customers].[Mexico]", "[Customers].[mExIcO]");
        checkAxis("[Customer].[Customers].[Mexico]", "[Customers].[mexico]");
    }

    /**
     * Calculated member names are case insensitive.
     */
    public void testCalculatedMemberCase() {
        propSaver.set(propSaver.props.CaseSensitive, false);
        assertQueryReturns(
            "with member [Measures].[CaLc] as '1'\n"
            + " select {[Measures].[CaLc]} on columns from Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[CaLc]}\n"
            + "Row #0: 1\n");
        assertQueryReturns(
            "with member [Measures].[CaLc] as '1'\n"
            + " select {[Measures].[cAlC]} on columns from Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[CaLc]}\n"
            + "Row #0: 1\n");
        assertQueryReturns(
            "with member [mEaSuReS].[CaLc] as '1'\n"
            + " select {[MeAsUrEs].[cAlC]} on columns from Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[CaLc]}\n"
            + "Row #0: 1\n");
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
            "WITH\n"
            + "   MEMBER [Store].[Stores].[StoreCalc] as '0', "
            + keyword + "=0\n"
            + "   MEMBER [Product].[ProdCalc] as '1', " + keyword + "=1\n"
            + "SELECT\n"
            + "   { [Product].[ProdCalc] } ON columns,\n"
            + "   { [Store].[StoreCalc] } ON rows\n"
            + "FROM Sales",

            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Product].[Products].[ProdCalc]}\n"
            + "Axis #2:\n"
            + "{[Store].[Stores].[StoreCalc]}\n"
            + "Row #0: 1\n");
    }

    /**
     * Brackets around member names are optional.
     */
    public void testMemberBrackets() {
        checkAxis("[Measures].[Profit]", "[Measures].Profit");
        checkAxis("[Measures].[Profit]", "[Measures].pRoFiT");
        checkAxis("[Measures].[Profit]", "[Measures].PROFIT");
        checkAxis("[Measures].[Profit]", "[Measures].profit");

        checkAxis(
            "[Customer].[Customers].[Mexico]",
            "[Customers].Mexico");
        checkAxis(
            "[Customer].[Customers].[Mexico]",
            "[Customers].MEXICO");
        checkAxis(
            "[Customer].[Customers].[Mexico]",
            "[Customers].mExIcO");
        checkAxis(
            "[Customer].[Customers].[Mexico]",
            "[Customers].mexico");
    }

    /**
     * Hierarchy names of the form [Dim].[Hier], [Dim.Hier], and
     * Dim.Hier are accepted.
     */
    public void testHierarchyNames() {
        checkAxis(
            "[Customer].[Customers].[All Customers]",
            "[Customers].[All Customers]");
        checkAxis(
            "[Customer].[Customers].[All Customers]",
            "[Customer].[Customers].[All Customers]");
        checkAxis(
            "[Customer].[Customers].[All Customers]",
            "Customer.[Customers].[All Customers]");
        checkAxis(
            "[Customer].[Customers].[All Customers]",
            "[Customer].Customers.[All Customers]");
        if (false) {
            // don't know if this makes sense
            checkAxis(
                "[Customer].[Customers].[All Customers]",
                "[Customers.Customers].[All Customers]");
        }
    }

    private void checkAxis(String result, String expression) {
        Assert.assertEquals(
            result, executeSingletonAxis(expression).toString());
    }


    /**
     * Tests that a #null member on a Hiearchy Level of type String can
     * still be looked up when case sensitive is off.
     */
    public void testCaseInsensitiveNullMember() {
        final Dialect dialect = getTestContext().getDialect();
        if (dialect.getDatabaseProduct() == Dialect.DatabaseProduct.LUCIDDB) {
            // TODO jvs 29-Nov-2006:  LucidDB is strict about
            // null literals (type can't be inferred in this context);
            // maybe enhance the inline table to use the columndef
            // types to apply a CAST.
            return;
        }
        if (!isDefaultNullMemberRepresentation()) {
            return;
        }
        final String cubeName = "Sales_inline";
        final TestContext testContext = TestContext.instance().legacy().create(
            null,
            "<Cube name=\"" + cubeName + "\">\n"
            + "  <Table name=\"sales_fact_1997\"/>\n"
            + "  <DimensionUsage name=\"Time\" source=\"Time\" foreignKey=\"time_id\"/>\n"
            + "  <Dimension name=\"Alternative Promotion\" foreignKey=\"promotion_id\">\n"
            + "    <Hierarchy hasAll=\"true\" primaryKey=\"promo_id\">\n"
            + "      <InlineTable alias=\"alt_promotion\">\n"
            + "        <ColumnDefs>\n"
            + "          <ColumnDef name=\"promo_id\" type=\"Numeric\"/>\n"
            + "          <ColumnDef name=\"promo_name\" type=\"String\"/>\n"
            + "        </ColumnDefs>\n"
            + "        <Rows>\n"
            + "          <Row>\n"
            + "            <Value column=\"promo_id\">0</Value>\n"
            + "            <Value column=\"promo_name\">Promo0</Value>\n"
            + "          </Row>\n"
            + "          <Row>\n"
            + "            <Value column=\"promo_id\">1</Value>\n"
            + "          </Row>\n"
            + "        </Rows>\n"
            + "      </InlineTable>\n"
            + "      <Level name=\"Alternative Promotion\" column=\"promo_name\" uniqueMembers=\"true\"/> \n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\"\n"
            + "      formatString=\"Standard\" visible=\"false\"/>\n"
            + "  <Measure name=\"Store Sales\" column=\"store_sales\" aggregator=\"sum\"\n"
            + "      formatString=\"#,###.00\"/>\n"
            + "</Cube>",
            null,
            null,
            null,
            null);

        // This test should work irrespective of the case-sensitivity setting.
        Util.discard(propSaver.props.CaseSensitive.get());

        testContext.assertQueryReturns(
            "select {[Measures].[Unit Sales]} ON COLUMNS,\n"
            + "  {[Alternative Promotion].[#null]} ON ROWS \n"
            + "  from [Sales_inline]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Alternative Promotion].[Alternative Promotion].[#null]}\n"
            + "Row #0: \n");
    }

    /**
     * Tests that data in Hierarchy.Level attribute "nameColumn" can be null.
     * This will map to the #null memeber.
     */
    public void testNullNameColumn() {
        switch (getTestContext().getDialect().getDatabaseProduct()) {
        case LUCIDDB:
            // TODO jvs 29-Nov-2006:  See corresponding comment in
            // testCaseInsensitiveNullMember
            return;
        case HSQLDB:
            // This test exposes a bug in hsqldb. The following query should
            // return 1 row, but returns none.
            //
            // select "alt_promotion"."promo_id" as "c0",
            //   "alt_promotion"."promo_name" as "c1"
            // from (
            //    select 0 as "promo_id", null as "promo_name"
            //    from "days" where "day" = 1
            //    union all
            //    select 1 as "promo_id", 'Promo1' as "promo_name"
            //    from "days" where "day" = 1) as "alt_promotion"
            // where UPPER("alt_promotion"."promo_name") = UPPER('Promo1')
            // group by "alt_promotion"."promo_id",
            //    "alt_promotion"."promo_name"
            // order by
            //   CASE WHEN "alt_promotion"."promo_id" IS NULL THEN 1 ELSE 0 END,
            //   "alt_promotion"."promo_id" ASC
            return;
        }
        if (!isDefaultNullMemberRepresentation()) {
            return;
        }
        final String cubeName = "Sales_inline";
        final TestContext testContext = TestContext.instance().legacy().create(
            null,
            "<Cube name=\"" + cubeName + "\">\n"
            + "  <Table name=\"sales_fact_1997\"/>\n"
            + "  <DimensionUsage name=\"Time\" source=\"Time\" foreignKey=\"time_id\"/>\n"
            + "  <Dimension name=\"Alternative Promotion\" foreignKey=\"promotion_id\">\n"
            + "    <Hierarchy hasAll=\"true\" primaryKey=\"promo_id\">\n"
            + "      <InlineTable alias=\"alt_promotion\">\n"
            + "        <ColumnDefs>\n"
            + "          <ColumnDef name=\"promo_id\" type=\"Numeric\"/>\n"
            + "          <ColumnDef name=\"promo_name\" type=\"String\"/>\n"
            + "        </ColumnDefs>\n"
            + "        <Rows>\n"
            + "          <Row>\n"
            + "            <Value column=\"promo_id\">0</Value>\n"
            + "          </Row>\n"
            + "          <Row>\n"
            + "            <Value column=\"promo_id\">1</Value>\n"
            + "            <Value column=\"promo_name\">Promo1</Value>\n"
            + "          </Row>\n"
            + "        </Rows>\n"
            + "      </InlineTable>\n"
            + "      <Level name=\"Alternative Promotion\" column=\"promo_id\" nameColumn=\"promo_name\" uniqueMembers=\"true\"/> \n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\"\n"
            + "      formatString=\"Standard\" visible=\"false\"/>\n"
            + "  <Measure name=\"Store Sales\" column=\"store_sales\" aggregator=\"sum\"\n"
            + "      formatString=\"#,###.00\"/>\n"
            + "</Cube>", null, null, null, null);

        testContext.assertQueryReturns(
            "select {"
            + "[Alternative Promotion].[#null], "
            + "[Alternative Promotion].[Promo1]} ON COLUMNS\n"
            + "from [" + cubeName + "] ",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Alternative Promotion].[Alternative Promotion].[#null]}\n"
            + "{[Alternative Promotion].[Alternative Promotion].[Promo1]}\n"
            + "Row #0: 195,448\n"
            + "Row #0: \n");
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
        final TestContext testContext = TestContext.instance().legacy().create(
            null,
            "<Cube name=\"" + cubeName + "\">\n"
            + "  <Table name=\"store\"/>\n"
            + "  <Dimension name=\"Store\">\n"
            + "    <Hierarchy hasAll=\"true\">\n"
            + "      <Level name=\"Store Name\" column=\"store_name\"  uniqueMembers=\"true\">\n"
            + "       <OrdinalExpression>\n"
            + "        <SQL dialect=\"access\">\n"
            + "           Iif(<Column table=\"store\" name=\"store_name\"/>  = 'HQ', null, <Column table=\"store\" name=\"store_name\"/>)\n"
            + "       </SQL>\n"
            + "        <SQL dialect=\"oracle\">\n"
            + "           case \"store_name\" when 'HQ' then null else \"store_name\" end\n"
            + "       </SQL>\n"
            + "        <SQL dialect=\"hsqldb\">\n"
            + "           case \"store_name\" when 'HQ' then null else \"store_name\" end\n"
            + "       </SQL>\n"
            + "        <SQL dialect=\"db2\">\n"
            + "           case \"store\".\"store_name\" when 'HQ' then null else \"store\".\"store_name\" end\n"
            + "       </SQL>\n"
            + "        <SQL dialect=\"luciddb\">\n"
            + "           case \"store_name\" when 'HQ' then null else \"store_name\" end\n"
            + "       </SQL>\n"
            + "        <SQL dialect=\"netezza\">\n"
            + "           case \"store_name\" when 'HQ' then null else \"store_name\" end\n"
            + "       </SQL>\n"
            + "        <SQL dialect=\"generic\">\n"
            + "           case store_name when 'HQ' then null else store_name end\n"
            + "       </SQL>\n"
            + "       </OrdinalExpression>\n"
            + "        <Property name=\"Store Sqft\" column=\"store_sqft\" type=\"Numeric\"/>\n"
            + "      </Level>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Measure name=\"Store Sqft\" column=\"store_sqft\" aggregator=\"sum\"\n"
            + "      formatString=\"#,###\"/>\n"
            + "</Cube>",
            null, null, null, null);

        testContext.assertQueryReturns(
            "select { [Measures].[Store Sqft] } on columns,\n"
            + " NON EMPTY topcount(\n"
            + "    {[Store].[Store Name].members},\n"
            + "    5,\n"
            + "    [measures].[store sqft]) on rows\n"
            + "from [" + cubeName + "] ",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Sqft]}\n"
            + "Axis #2:\n"
            + "{[Store].[Store].[Store 3]}\n"
            + "{[Store].[Store].[Store 18]}\n"
            + "{[Store].[Store].[Store 9]}\n"
            + "{[Store].[Store].[Store 10]}\n"
            + "{[Store].[Store].[Store 20]}\n"
            + "Row #0: 39,696\n"
            + "Row #1: 38,382\n"
            + "Row #2: 36,509\n"
            + "Row #3: 34,791\n"
            + "Row #4: 34,452\n");
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
        boolean caseSensitive = propSaver.props.CaseSensitive.get();

        // A user-defined property of a member.
        assertExprReturns(
            "[Store].[USA].[CA].[Beverly Hills].[Store 6].Properties(\"Store Type\")",
            "Gourmet Supermarket");

        if (caseSensitive) {
            assertExprThrows(
                "[Store].[USA].[CA].[Beverly Hills].[Store 6].Properties(\"store tYpe\")",
                "Property 'store tYpe' is not valid for member '[Store].[USA].[CA].[Beverly Hills].[Store 6]'");
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
                "Property 'store tYpe' is not valid for member '[Store].[USA].[CA].[Beverly Hills].[Store 6]'");
        } else {
            assertExprReturns(
                "[Store].[USA].[CA].[Beverly Hills].[Store 6].Properties(\"Level_Number\")",
                "4");
        }

        // Cell properties.
        Result result = executeQuery(
            "select {[Measures].[Unit Sales],[Measures].[Store Sales]} on columns,\n"
            + " {[Gender].[M]} on rows\n"
            + "from Sales");
        Cell cell = result.getCell(new int[]{0, 0});
        assertEquals("135,215", cell.getPropertyValue("FORMATTED_VALUE"));
        if (caseSensitive) {
            assertNull(cell.getPropertyValue("Formatted_Value"));
        } else {
            assertEquals("135,215", cell.getPropertyValue("Formatted_Value"));
        }
    }

    public void testWithDimensionPrefix() {
        assertAxisWithDimensionPrefix(true);
        assertAxisWithDimensionPrefix(false);
    }

    private void assertAxisWithDimensionPrefix(boolean prefixNeeded) {
        propSaver.set(propSaver.props.NeedDimensionPrefix, prefixNeeded);
        assertAxisReturns("[Gender].[M]", "[Customer].[Gender].[M]");
        assertAxisReturns(
            "[Gender].[All Gender].[M]", "[Customer].[Gender].[M]");
        assertAxisReturns("[Store].[USA]", "[Store].[Stores].[USA]");
        assertAxisReturns(
            "[Store].[All Stores].[USA]", "[Store].[Stores].[USA]");
    }

    public void testWithNoDimensionPrefix() {
        propSaver.set(propSaver.props.NeedDimensionPrefix, false);
        assertAxisReturns("{[M]}", "[Customer].[Gender].[M]");
        assertAxisReturns("{M}", "[Customer].[Gender].[M]");
        assertAxisReturns("{[USA].[CA]}", "[Store].[Stores].[USA].[CA]");
        assertAxisReturns("{USA.CA}", "[Store].[Stores].[USA].[CA]");
        propSaver.set(propSaver.props.NeedDimensionPrefix, true);
        assertAxisThrows(
            "{[M]}",
            "Mondrian Error:MDX object '[M]' not found in cube 'Sales'");
        assertAxisThrows(
            "{M}",
            "Mondrian Error:MDX object 'M' not found in cube 'Sales'");
        assertAxisThrows(
            "{[USA].[CA]}",
            "Mondrian Error:MDX object '[USA].[CA]' not found in cube 'Sales'");
        assertAxisThrows(
            "{USA.CA}",
            "Mondrian Error:MDX object 'USA.CA' not found in cube 'Sales'");
    }
}

// End CompatibilityTest.java
