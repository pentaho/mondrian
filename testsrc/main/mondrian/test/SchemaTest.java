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
 * Unit tests for various schema features.
 *
 * @author jhyde
 * @since August 7, 2006
 * @version $Id$
 */
public class SchemaTest extends FoodMartTestCase {

    public SchemaTest(String name) {
        super(name);
    }

    public void testHierarchyDefaultMember() {
        final TestContext testContext = TestContext.createSubstitutingCube(
            "Sales",
            "  <Dimension name=\"Gender with default\" foreignKey=\"customer_id\">\n" +
                "    <Hierarchy hasAll=\"true\" " +
                "primaryKey=\"customer_id\" " +
                // Define a default member's whose unique name includes the
                // 'all' member.
                "defaultMember=\"[Gender with default].[All Gender with defaults].[M]\" >\n" +
                "      <Table name=\"customer\"/>\n" +
                "      <Level name=\"Gender\" column=\"gender\" uniqueMembers=\"true\" />\n" +
                "    </Hierarchy>\n" +
                "  </Dimension>");
        testContext.assertQueryReturns(
            "select {[Gender with default]} on columns from [Sales]",
            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Gender with default].[All Gender with defaults].[M]}\n" +
                "Row #0: 135,215\n"));
    }

    public void testHierarchyAbbreviatedDefaultMember() {
        final TestContext testContext = TestContext.createSubstitutingCube(
            "Sales",
            "  <Dimension name=\"Gender with default\" foreignKey=\"customer_id\">\n" +
                "    <Hierarchy hasAll=\"true\" " +
                "primaryKey=\"customer_id\" " +
                // Default member unique name does not include 'All'.
                "defaultMember=\"[Gender with default].[F]\" >\n" +
                "      <Table name=\"customer\"/>\n" +
                "      <Level name=\"Gender\" column=\"gender\" uniqueMembers=\"true\" />\n" +
                "    </Hierarchy>\n" +
                "  </Dimension>");
        testContext.assertQueryReturns(
            "select {[Gender with default]} on columns from [Sales]",
            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                // Note that the 'all' member is named according to the rule
                // '[<hierarchy>].[All <hierarchy>s]'.
                "{[Gender with default].[All Gender with defaults].[F]}\n" +
                "Row #0: 131,558\n"));
    }

    public void testHierarchyBadDefaultMember() {
        final TestContext testContext = TestContext.createSubstitutingCube(
            "Sales",
            "  <Dimension name=\"Gender with default\" foreignKey=\"customer_id\">\n" +
                "    <Hierarchy hasAll=\"true\" " +
                "primaryKey=\"customer_id\" " +
                // Default member unique name does not include 'All'.
                "defaultMember=\"[Gender with default].[Non].[Existent]\" >\n" +
                "      <Table name=\"customer\"/>\n" +
                "      <Level name=\"Gender\" column=\"gender\" uniqueMembers=\"true\" />\n" +
                "    </Hierarchy>\n" +
                "  </Dimension>");
        testContext.assertThrows(
            "select {[Gender with default]} on columns from [Sales]",
            "Can not find Default Member with name \"[Gender with default].[Non].[Existent]\" in Hierarchy \"Gender with default\"");
    }

    /**
     * Tests that an error is issued if two dimensions use the same table via
     * different drill-paths and do not use a different alias. If this error is
     * not issued, the generated SQL can be missing a join condition, as in
     * <a href="https://sourceforge.net/tracker/?func=detail&atid=414613&aid=1583462&group_id=35302">
     * Bug 1583462, "Mondrian generates invalid SQL"</a>.
     */
    public void testDuplicateTableAlias() {
        final TestContext testContext = TestContext.createSubstitutingCube(
            "Sales",
            "<Dimension name=\"Yearly Income2\" foreignKey=\"product_id\">\n" +
                "  <Hierarchy hasAll=\"true\" primaryKey=\"customer_id\">\n" +
                "    <Table name=\"customer\"/>\n" +
                "    <Level name=\"Yearly Income\" column=\"yearly_income\" uniqueMembers=\"true\"/>\n" +
                "  </Hierarchy>\n" +
                "</Dimension>");
        testContext.assertThrows(
            "select from [Sales]",
            "Duplicate table alias 'customer' in cube 'Sales'");
    }

    /**
     * Tests two dimensions using same table (via different join paths).
     * Without the table alias, generates SQL which is missing a join condition.
     * See {@link #testDuplicateTableAlias()}.
     */
    public void testDimensionsShareTable() {
        final TestContext testContext = TestContext.createSubstitutingCube(
            "Sales",
            "<Dimension name=\"Yearly Income2\" foreignKey=\"product_id\">\n" +
                "  <Hierarchy hasAll=\"true\" primaryKey=\"customer_id\">\n" +
                "    <Table name=\"customer\" alias=\"customerx\" />\n" +
                "    <Level name=\"Yearly Income\" column=\"yearly_income\" uniqueMembers=\"true\"/>\n" +
                "  </Hierarchy>\n" +
                "</Dimension>");
        testContext.assertQueryReturns(
            "select NON EMPTY {[Measures].[Unit Sales]} ON COLUMNS,\n" +
                "NON EMPTY Crossjoin({[Yearly Income].[All Yearly Incomes].Children},\n" +
                "                     [Yearly Income2].[All Yearly Income2s].Children) ON ROWS\n" +
                "from [Sales]",
            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Unit Sales]}\n" +
                "Axis #2:\n" +
                "{[Yearly Income].[All Yearly Incomes].[$10K - $30K], [Yearly Income2].[All Yearly Income2s].[$10K - $30K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$10K - $30K], [Yearly Income2].[All Yearly Income2s].[$110K - $130K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$10K - $30K], [Yearly Income2].[All Yearly Income2s].[$130K - $150K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$10K - $30K], [Yearly Income2].[All Yearly Income2s].[$150K +]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$10K - $30K], [Yearly Income2].[All Yearly Income2s].[$30K - $50K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$10K - $30K], [Yearly Income2].[All Yearly Income2s].[$50K - $70K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$10K - $30K], [Yearly Income2].[All Yearly Income2s].[$70K - $90K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$10K - $30K], [Yearly Income2].[All Yearly Income2s].[$90K - $110K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$110K - $130K], [Yearly Income2].[All Yearly Income2s].[$10K - $30K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$110K - $130K], [Yearly Income2].[All Yearly Income2s].[$110K - $130K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$110K - $130K], [Yearly Income2].[All Yearly Income2s].[$130K - $150K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$110K - $130K], [Yearly Income2].[All Yearly Income2s].[$150K +]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$110K - $130K], [Yearly Income2].[All Yearly Income2s].[$30K - $50K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$110K - $130K], [Yearly Income2].[All Yearly Income2s].[$50K - $70K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$110K - $130K], [Yearly Income2].[All Yearly Income2s].[$70K - $90K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$110K - $130K], [Yearly Income2].[All Yearly Income2s].[$90K - $110K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$130K - $150K], [Yearly Income2].[All Yearly Income2s].[$10K - $30K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$130K - $150K], [Yearly Income2].[All Yearly Income2s].[$110K - $130K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$130K - $150K], [Yearly Income2].[All Yearly Income2s].[$130K - $150K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$130K - $150K], [Yearly Income2].[All Yearly Income2s].[$150K +]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$130K - $150K], [Yearly Income2].[All Yearly Income2s].[$30K - $50K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$130K - $150K], [Yearly Income2].[All Yearly Income2s].[$50K - $70K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$130K - $150K], [Yearly Income2].[All Yearly Income2s].[$70K - $90K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$130K - $150K], [Yearly Income2].[All Yearly Income2s].[$90K - $110K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$150K +], [Yearly Income2].[All Yearly Income2s].[$10K - $30K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$150K +], [Yearly Income2].[All Yearly Income2s].[$110K - $130K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$150K +], [Yearly Income2].[All Yearly Income2s].[$130K - $150K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$150K +], [Yearly Income2].[All Yearly Income2s].[$150K +]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$150K +], [Yearly Income2].[All Yearly Income2s].[$30K - $50K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$150K +], [Yearly Income2].[All Yearly Income2s].[$50K - $70K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$150K +], [Yearly Income2].[All Yearly Income2s].[$70K - $90K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$150K +], [Yearly Income2].[All Yearly Income2s].[$90K - $110K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$30K - $50K], [Yearly Income2].[All Yearly Income2s].[$10K - $30K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$30K - $50K], [Yearly Income2].[All Yearly Income2s].[$110K - $130K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$30K - $50K], [Yearly Income2].[All Yearly Income2s].[$130K - $150K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$30K - $50K], [Yearly Income2].[All Yearly Income2s].[$150K +]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$30K - $50K], [Yearly Income2].[All Yearly Income2s].[$30K - $50K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$30K - $50K], [Yearly Income2].[All Yearly Income2s].[$50K - $70K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$30K - $50K], [Yearly Income2].[All Yearly Income2s].[$70K - $90K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$30K - $50K], [Yearly Income2].[All Yearly Income2s].[$90K - $110K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$50K - $70K], [Yearly Income2].[All Yearly Income2s].[$10K - $30K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$50K - $70K], [Yearly Income2].[All Yearly Income2s].[$110K - $130K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$50K - $70K], [Yearly Income2].[All Yearly Income2s].[$130K - $150K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$50K - $70K], [Yearly Income2].[All Yearly Income2s].[$150K +]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$50K - $70K], [Yearly Income2].[All Yearly Income2s].[$30K - $50K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$50K - $70K], [Yearly Income2].[All Yearly Income2s].[$50K - $70K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$50K - $70K], [Yearly Income2].[All Yearly Income2s].[$70K - $90K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$50K - $70K], [Yearly Income2].[All Yearly Income2s].[$90K - $110K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$70K - $90K], [Yearly Income2].[All Yearly Income2s].[$10K - $30K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$70K - $90K], [Yearly Income2].[All Yearly Income2s].[$110K - $130K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$70K - $90K], [Yearly Income2].[All Yearly Income2s].[$130K - $150K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$70K - $90K], [Yearly Income2].[All Yearly Income2s].[$150K +]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$70K - $90K], [Yearly Income2].[All Yearly Income2s].[$30K - $50K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$70K - $90K], [Yearly Income2].[All Yearly Income2s].[$50K - $70K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$70K - $90K], [Yearly Income2].[All Yearly Income2s].[$70K - $90K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$70K - $90K], [Yearly Income2].[All Yearly Income2s].[$90K - $110K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$90K - $110K], [Yearly Income2].[All Yearly Income2s].[$10K - $30K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$90K - $110K], [Yearly Income2].[All Yearly Income2s].[$110K - $130K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$90K - $110K], [Yearly Income2].[All Yearly Income2s].[$130K - $150K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$90K - $110K], [Yearly Income2].[All Yearly Income2s].[$150K +]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$90K - $110K], [Yearly Income2].[All Yearly Income2s].[$30K - $50K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$90K - $110K], [Yearly Income2].[All Yearly Income2s].[$50K - $70K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$90K - $110K], [Yearly Income2].[All Yearly Income2s].[$70K - $90K]}\n" +
                "{[Yearly Income].[All Yearly Incomes].[$90K - $110K], [Yearly Income2].[All Yearly Income2s].[$90K - $110K]}\n" +
                "Row #0: 12,824\n" +
                "Row #1: 2,822\n" +
                "Row #2: 2,933\n" +
                "Row #3: 918\n" +
                "Row #4: 18,381\n" +
                "Row #5: 10,436\n" +
                "Row #6: 6,777\n" +
                "Row #7: 2,859\n" +
                "Row #8: 2,432\n" +
                "Row #9: 532\n" +
                "Row #10: 566\n" +
                "Row #11: 177\n" +
                "Row #12: 3,877\n" +
                "Row #13: 2,131\n" +
                "Row #14: 1,319\n" +
                "Row #15: 527\n" +
                "Row #16: 3,331\n" +
                "Row #17: 643\n" +
                "Row #18: 703\n" +
                "Row #19: 187\n" +
                "Row #20: 4,497\n" +
                "Row #21: 2,629\n" +
                "Row #22: 1,681\n" +
                "Row #23: 721\n" +
                "Row #24: 1,123\n" +
                "Row #25: 224\n" +
                "Row #26: 257\n" +
                "Row #27: 109\n" +
                "Row #28: 1,924\n" +
                "Row #29: 1,026\n" +
                "Row #30: 675\n" +
                "Row #31: 291\n" +
                "Row #32: 19,067\n" +
                "Row #33: 4,078\n" +
                "Row #34: 4,235\n" +
                "Row #35: 1,569\n" +
                "Row #36: 28,160\n" +
                "Row #37: 15,368\n" +
                "Row #38: 10,329\n" +
                "Row #39: 4,504\n" +
                "Row #40: 9,708\n" +
                "Row #41: 2,353\n" +
                "Row #42: 2,243\n" +
                "Row #43: 748\n" +
                "Row #44: 14,469\n" +
                "Row #45: 7,966\n" +
                "Row #46: 5,272\n" +
                "Row #47: 2,208\n" +
                "Row #48: 7,320\n" +
                "Row #49: 1,630\n" +
                "Row #50: 1,602\n" +
                "Row #51: 541\n" +
                "Row #52: 10,550\n" +
                "Row #53: 5,843\n" +
                "Row #54: 3,997\n" +
                "Row #55: 1,562\n" +
                "Row #56: 2,722\n" +
                "Row #57: 597\n" +
                "Row #58: 568\n" +
                "Row #59: 193\n" +
                "Row #60: 3,800\n" +
                "Row #61: 2,192\n" +
                "Row #62: 1,324\n" +
                "Row #63: 523\n"));
    }
}

// End SchemaTest.java
