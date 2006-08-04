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

import mondrian.olap.*;

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
                "select {[Alternative Promotion].members} ON COLUMNS\n" +
                "from [" + cubeName + "] ",
                    fold(
                        "Axis #0:\n" +
                        "{}\n" +
                        "Axis #1:\n" +
                        "{[Alternative Promotion].[All Alternative Promotions]}\n" +
                        "{[Alternative Promotion].[All Alternative Promotions].[Promo0]}\n" +
                        "{[Alternative Promotion].[All Alternative Promotions].[Promo1]}\n" +
                        "Row #0: 266,773\n" +
                        "Row #0: 195,448\n" +
                        "Row #0: \n"));
        } finally {
            schema.removeCube(cubeName);
        }
    }
}

// End InlineTableTest.java
