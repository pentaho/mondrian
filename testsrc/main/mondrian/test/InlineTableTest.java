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
        final Cube cube = schema.createCube(fold(new String[] {
            "<Cube name=\"" + cubeName + "\">",
            "  <Table name=\"sales_fact_1997\"/>",
            "  <DimensionUsage name=\"Time\" source=\"Time\" foreignKey=\"time_id\"/>",
            "  <Dimension name=\"Alternative Promotion\" foreignKey=\"promotion_id\">",
            "    <Hierarchy hasAll=\"true\" primaryKey=\"promo_id\">",
            "      <InlineTable alias=\"alt_promotion\">",
            "        <ColumnDefs>",
            "          <ColumnDef name=\"promo_id\" type=\"Numeric\"/>",
            "          <ColumnDef name=\"promo_name\" type=\"String\"/>",
            "        </ColumnDefs>",
            "        <Rows>",
            "          <Row>",
            "            <Value column=\"promo_id\">0</Value>",
            "            <Value column=\"promo_name\">Promo0</Value>",
            "          </Row>",
            "          <Row>",
            "            <Value column=\"promo_id\">1</Value>",
            "            <Value column=\"promo_name\">Promo1</Value>",
            "          </Row>",
            "        </Rows>",
            "      </InlineTable>",
            "      <Level name=\"Alternative Promotion\" column=\"promo_id\" nameColumn=\"promo_name\" uniqueMembers=\"true\"/> ",
            "    </Hierarchy>",
            "  </Dimension>",
            "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\"",
            "      formatString=\"Standard\" visible=\"false\"/>",
            "  <Measure name=\"Store Sales\" column=\"store_sales\" aggregator=\"sum\"",
            "      formatString=\"#,###.00\"/>",
            "</Cube>"}));

        try {
            getTestContext().assertQueryReturns(
                    fold(new String[] {
                        "select {[Alternative Promotion].members} ON COLUMNS",
                        "from [" + cubeName + "] "}),
                    fold(new String[] {
                        "Axis #0:",
                        "{}",
                        "Axis #1:",
                        "{[Alternative Promotion].[All Alternative Promotions]}",
                        "{[Alternative Promotion].[All Alternative Promotions].[Promo0]}",
                        "{[Alternative Promotion].[All Alternative Promotions].[Promo1]}",
                        "Row #0: 266,773",
                        "Row #0: 195,448",
                        "Row #0: (null)",
                        ""}));
        } finally {
            schema.removeCube(cubeName);
        }
    }
}

// End InlineTableTest.java
