/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.test;

/**
 * <code>BasicQueryTest</code> is a test case which tests simple queries
 * against the FoodMart database.
 *
 * @author jhyde
 * @since Feb 14, 2003
 * @version $Id$
 */
public class InlineTableTest extends FoodMartTestCase {

    public InlineTableTest(String name) {
        super(name);
    }

    public void testInlineTable() {
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
            "select {[Alternative Promotion].[All Alternative Promotions].children} ON COLUMNS\n" +
                "from [" + cubeName + "] ",
            fold(
                "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Alternative Promotion].[All Alternative Promotions].[Promo0]}\n" +
                    "{[Alternative Promotion].[All Alternative Promotions].[Promo1]}\n" +
                    "Row #0: 195,448\n" +
                    "Row #0: \n"));
    }

    public void testInlineTableInSharedDim() {
        final String cubeName = "Sales_inline_shared";
        TestContext testContext = TestContext.create(
            null,
            "  <Dimension name=\"Shared Alternative Promotion\">\n" +
                "    <Hierarchy hasAll=\"true\" primaryKey=\"promo_id\">\n" +
                "      <InlineTable alias=\"alt_promotion\">\n" +
                "        <ColumnDefs>\n" +
                "          <ColumnDef name=\"promo_id\" type=\"Numeric\"/>\n" +
                "          <ColumnDef name=\"promo_name\" type=\"String\"/>\n" +
                "        </ColumnDefs>\n" +
                "        <Rows>\n" +
                "          <Row>\n" +
                "            <Value column=\"promo_id\">0</Value>\n" +
                "            <Value column=\"promo_name\">First promo</Value>\n" +
                "          </Row>\n" +
                "          <Row>\n" +
                "            <Value column=\"promo_id\">1</Value>\n" +
                "            <Value column=\"promo_name\">Second promo</Value>\n" +
                "          </Row>\n" +
                "        </Rows>\n" +
                "      </InlineTable>\n" +
                "      <Level name=\"Alternative Promotion\" column=\"promo_id\" nameColumn=\"promo_name\" uniqueMembers=\"true\"/> \n" +
                "    </Hierarchy>\n" +
                "  </Dimension>\n" +
                "<Cube name=\"" + cubeName + "\">\n" +
                "  <Table name=\"sales_fact_1997\"/>\n" +
                "  <DimensionUsage name=\"Time\" source=\"Time\" foreignKey=\"time_id\"/>\n" +
                "  <DimensionUsage name=\"Shared Alternative Promotion\" source=\"Shared Alternative Promotion\" foreignKey=\"promotion_id\"/>\n" +
                "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\"\n" +
                "      formatString=\"Standard\" visible=\"false\"/>\n" +
                "  <Measure name=\"Store Sales\" column=\"store_sales\" aggregator=\"sum\"\n" +
                "      formatString=\"#,###.00\"/>\n" +
                "</Cube>",
            null, null, null, null);
        testContext.assertQueryReturns(
            "select {[Shared Alternative Promotion].[All Shared Alternative Promotions].children} ON COLUMNS\n" +
                "from [" + cubeName + "] ",
            fold(
                "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Shared Alternative Promotion].[All Shared Alternative Promotions].[First promo]}\n" +
                    "{[Shared Alternative Promotion].[All Shared Alternative Promotions].[Second promo]}\n" +
                    "Row #0: 195,448\n" +
                    "Row #0: \n"));
    }
}

// End InlineTableTest.java
