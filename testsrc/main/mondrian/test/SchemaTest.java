/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.test;

import org.apache.log4j.*;
import org.apache.log4j.varia.LevelRangeFilter;

import mondrian.rolap.RolapCube;
import mondrian.rolap.aggmatcher.AggTableManager;
import mondrian.util.Bug;
import mondrian.olap.Cube;
import mondrian.olap.MondrianProperties;

import java.io.StringWriter;

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

    public void testSolveOrderInCalculatedMember(){
        final TestContext testContext = TestContext.createSubstitutingCube(
            "Sales",null,"<CalculatedMember\n" +
            "      name=\"QuantumProfit\"\n" +
            "      dimension=\"Measures\">\n" +
            "    <Formula>[Measures].[Store Sales] / [Measures].[Store Cost]</Formula>\n" +
            "    <CalculatedMemberProperty name=\"FORMAT_STRING\" value=\"$#,##0.00\"/>\n" +
            "  </CalculatedMember>, <CalculatedMember\n" +
            "      name=\"foo\"\n" +
            "      dimension=\"Gender\">\n" +
            "    <Formula>Sum(Gender.Members)</Formula>\n" +
            "    <CalculatedMemberProperty name=\"FORMAT_STRING\" value=\"$#,##0.00\"/>\n" +
            "    <CalculatedMemberProperty name=\"SOLVE_ORDER\" value=\'2000\'/>\n" +
            "  </CalculatedMember>");

         testContext.assertQueryReturns(
            "select {[Measures].[QuantumProfit]} on 0, {(Gender.foo)} on 1 from sales",
            fold("Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[QuantumProfit]}\n" +
                    "Axis #2:\n" +
                    "{[Gender].[foo]}\n" +
                    "Row #0: $7.52\n"));
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
     * WG: Note, this no longer throws an exception with the new RolapCubeMember functionality.
     * 
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

        testContext.assertQueryReturns(
                "select {[Yearly Income2]} on columns, {[Measures].[Unit Sales]} on rows from [Sales]",
                fold(
                        "Axis #0:\n" +
                        "{}\n" +
                        "Axis #1:\n" +
                        "{[Yearly Income2].[All Yearly Income2s]}\n" +
                        "Axis #2:\n" +
                        "{[Measures].[Unit Sales]}\n" +
                        "Row #0: 266,773\n"));
    }

    /**
     * This result is somewhat peculiar. If two dimensions share a foreign key, what
     * is the expected result?  Also, in this case, they share the same table without
     * an alias, and the system doesn't complain.
     */
    public void testDuplicateTableAliasSameForeignKey() {
        final TestContext testContext = TestContext.createSubstitutingCube(
            "Sales",
            "<Dimension name=\"Yearly Income2\" foreignKey=\"customer_id\">\n" +
                "  <Hierarchy hasAll=\"true\" primaryKey=\"customer_id\">\n" +
                "    <Table name=\"customer\"/>\n" +
                "    <Level name=\"Yearly Income\" column=\"yearly_income\" uniqueMembers=\"true\"/>\n" +
                "  </Hierarchy>\n" +
                "</Dimension>");
        testContext.assertQueryReturns(
            "select from [Sales]",
            fold("Axis #0:\n" +
                 "{}\n" +
                 "266,773"));

        /** NonEmptyCrossJoin Fails
        testContext.assertQueryReturns(
                "select NonEmptyCrossJoin({[Yearly Income2].[All Yearly Income2s]},{[Customers].[All Customers]}) on rows," +
                "NON EMPTY {[Measures].[Unit Sales]} on columns" +
                " from [Sales]",
                fold("Axis #0:\n" +
                     "{}\n" +
                     "266,773"));
        */
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

        testContext.assertQueryReturns("select {[Yearly Income].[$10K - $30K]} on columns," +
                "{[Yearly Income2].[$150K +]} on rows from [Sales]"
                ,
                fold("Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Yearly Income].[All Yearly Incomes].[$10K - $30K]}\n" +
                    "Axis #2:\n" +
                    "{[Yearly Income2].[All Yearly Income2s].[$150K +]}\n" +
                    "Row #0: 918\n")
        );

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

    /**
     * Tests two dimensions using same table (via different join paths).
     * native non empty cross join sql generation returns empty query.
     * note that this works when native cross join is disabled
     */
    public void testDimensionsShareTableNativeNonEmptyCrossJoin() {
        final TestContext testContext = TestContext.createSubstitutingCube(
            "Sales",
            "<Dimension name=\"Yearly Income2\" foreignKey=\"product_id\">\n" +
                "  <Hierarchy hasAll=\"true\" primaryKey=\"customer_id\">\n" +
                "    <Table name=\"customer\" alias=\"customerx\" />\n" +
                "    <Level name=\"Yearly Income\" column=\"yearly_income\" uniqueMembers=\"true\"/>\n" +
                "  </Hierarchy>\n" +
                "</Dimension>");

        testContext.assertQueryReturns(
                "select NonEmptyCrossJoin({[Yearly Income2].[All Yearly Income2s]},{[Customers].[All Customers]}) on rows," +
                "NON EMPTY {[Measures].[Unit Sales]} on columns" +
                " from [Sales]",
                fold("Axis #0:\n" +
                     "{}\n" +
                     "Axis #1:\n" +
                     "{[Measures].[Unit Sales]}\n" +
                     "Axis #2:\n" +
                     "{[Yearly Income2].[All Yearly Income2s], [Customers].[All Customers]}\n" +
                     "Row #0: 266,773\n"));
    }

    /**
     * Tests two dimensions using same table with same foreign key
     * one table uses an alias.
     *
     */
    public void testDimensionsShareTableSameForeignKeys() {
        final TestContext testContext = TestContext.createSubstitutingCube(
            "Sales",
            "<Dimension name=\"Yearly Income2\" foreignKey=\"customer_id\">\n" +
                "  <Hierarchy hasAll=\"true\" primaryKey=\"customer_id\">\n" +
                "    <Table name=\"customer\" alias=\"customerx\" />\n" +
                "    <Level name=\"Yearly Income\" column=\"yearly_income\" uniqueMembers=\"true\"/>\n" +
                "  </Hierarchy>\n" +
                "</Dimension>");

        testContext.assertQueryReturns("select {[Yearly Income].[$10K - $30K]} on columns," +
                "{[Yearly Income2].[$150K +]} on rows from [Sales]"
                ,
                fold("Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Yearly Income].[All Yearly Incomes].[$10K - $30K]}\n" +
                    "Axis #2:\n" +
                    "{[Yearly Income2].[All Yearly Income2s].[$150K +]}\n" +
                    "Row #0: \n"));

        testContext.assertQueryReturns(
                "select NON EMPTY {[Measures].[Unit Sales]} ON COLUMNS,\n" +
                    "NON EMPTY Crossjoin({[Yearly Income].[All Yearly Incomes].Children},\n" +
                    "                     [Yearly Income2].[All Yearly Income2s].Children) ON ROWS\n" +
                    "from [Sales]",
                    fold(
                            "Axis #0:\n" +
                            "{}\n" +
                            "Axis #1:\n" +
                            "{[Measures].[Unit Sales]}\n" +
                            "Axis #2:\n" +
                            "{[Yearly Income].[All Yearly Incomes].[$10K - $30K], [Yearly Income2].[All Yearly Income2s].[$10K - $30K]}\n" +
                            "{[Yearly Income].[All Yearly Incomes].[$110K - $130K], [Yearly Income2].[All Yearly Income2s].[$110K - $130K]}\n" +
                            "{[Yearly Income].[All Yearly Incomes].[$130K - $150K], [Yearly Income2].[All Yearly Income2s].[$130K - $150K]}\n" +
                            "{[Yearly Income].[All Yearly Incomes].[$150K +], [Yearly Income2].[All Yearly Income2s].[$150K +]}\n" +
                            "{[Yearly Income].[All Yearly Incomes].[$30K - $50K], [Yearly Income2].[All Yearly Income2s].[$30K - $50K]}\n" +
                            "{[Yearly Income].[All Yearly Incomes].[$50K - $70K], [Yearly Income2].[All Yearly Income2s].[$50K - $70K]}\n" +
                            "{[Yearly Income].[All Yearly Incomes].[$70K - $90K], [Yearly Income2].[All Yearly Income2s].[$70K - $90K]}\n" +
                            "{[Yearly Income].[All Yearly Incomes].[$90K - $110K], [Yearly Income2].[All Yearly Income2s].[$90K - $110K]}\n" +
                            "Row #0: 57,950\n" +
                            "Row #1: 11,561\n" +
                            "Row #2: 14,392\n" +
                            "Row #3: 5,629\n" +
                            "Row #4: 87,310\n" +
                            "Row #5: 44,967\n" +
                            "Row #6: 33,045\n" +
                            "Row #7: 11,919\n"));

    }

    /**
     * WG: This no longer throws an exception, it is now possible
     * 
     * Tests two dimensions using same table (via different join paths).
     * both using a table alias.
     */
    public void testDimensionsShareJoinTable() {

        final TestContext testContext = TestContext.create(
                null,
                "<Cube name=\"AliasedDimensionsTesting\" defaultMeasure=\"Supply Time\">\n" +
                        "  <Table name=\"sales_fact_1997\"/>\n" +
                        "<Dimension name=\"Store\" foreignKey=\"store_id\">\n" +

                        "<Hierarchy hasAll=\"true\" primaryKeyTable=\"store\" primaryKey=\"store_id\">\n" +
                        "    <Join leftKey=\"region_id\" rightKey=\"region_id\">\n" +
                        "      <Table name=\"store\"/>\n" +
                        "      <Table name=\"region\"/>\n" +
                        "    </Join>\n" +
                        " <Level name=\"Store Country\" table=\"store\"  column=\"store_country\" uniqueMembers=\"true\"/>\n" +
                        " <Level name=\"Store Region\"  table=\"region\" column=\"sales_region\"  uniqueMembers=\"true\"/>\n" +
                        " <Level name=\"Store Name\"    table=\"store\"  column=\"store_name\"    uniqueMembers=\"true\"/>\n" +
                        "</Hierarchy>\n" +
                        "</Dimension>\n" +
                        "<Dimension name=\"Customers\" foreignKey=\"customer_id\">\n" +
                        "<Hierarchy hasAll=\"true\" allMemberName=\"All Customers\" primaryKeyTable=\"customer\" primaryKey=\"customer_id\">\n" +
                        "    <Join leftKey=\"customer_region_id\" rightKey=\"region_id\">\n" +
                        "      <Table name=\"customer\"/>\n" +
                        "      <Table name=\"region\"/>\n" +
                        "    </Join>\n" +
                        "  <Level name=\"Country\" table=\"customer\" column=\"country\"                      uniqueMembers=\"true\"/>\n" +
                        "  <Level name=\"Region\"  table=\"region\"   column=\"sales_region\"                 uniqueMembers=\"true\"/>\n" +
                        "  <Level name=\"City\"    table=\"customer\" column=\"city\"                         uniqueMembers=\"false\"/>\n" +
                        "  <Level name=\"Name\"    table=\"customer\" column=\"customer_id\" type=\"Numeric\" uniqueMembers=\"true\"/>\n" +
                        "</Hierarchy>\n" +
                        "</Dimension>\n" +
                        "<Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\" formatString=\"Standard\"/>\n" +
                        "<Measure name=\"Store Sales\" column=\"store_sales\" aggregator=\"sum\" formatString=\"#,###.00\"/>\n" +
                        "</Cube>",
                null, null, null, null);

        testContext.assertQueryReturns(
                "select  {[Store].[USA].[South West]} on rows," +
                "{[Customers].[USA].[South West]} on columns" +
                " from " +
                "AliasedDimensionsTesting",
                fold("Axis #0:\n" +
                     "{}\n" +
                     "Axis #1:\n" +
                     "{[Customers].[All Customers].[USA].[South West]}\n" +
                     "Axis #2:\n" +
                     "{[Store].[All Stores].[USA].[South West]}\n" +
                     "Row #0: 72,631\n"));
    }

    /**
     * Tests two dimensions using same table (via different join paths).
     * both using a table alias.
     */
    public void testDimensionsShareJoinTableOneAlias() {
        final TestContext testContext = TestContext.create(
                null,
                "<Cube name=\"AliasedDimensionsTesting\" defaultMeasure=\"Supply Time\">\n" +
                        "  <Table name=\"sales_fact_1997\"/>\n" +
                        "<Dimension name=\"Store\" foreignKey=\"store_id\">\n" +
                        "<Hierarchy hasAll=\"true\" primaryKeyTable=\"store\" primaryKey=\"store_id\">\n" +
                        "    <Join leftKey=\"region_id\" rightKey=\"region_id\">\n" +
                        "      <Table name=\"store\"/>\n" +
                        "      <Table name=\"region\"/>\n" +
                        "    </Join>\n" +
                        " <Level name=\"Store Country\" table=\"store\"  column=\"store_country\" uniqueMembers=\"true\"/>\n" +
                        " <Level name=\"Store Region\"  table=\"region\" column=\"sales_region\"  uniqueMembers=\"true\"/>\n" +
                        " <Level name=\"Store Name\"    table=\"store\"  column=\"store_name\"    uniqueMembers=\"true\"/>\n" +
                        "</Hierarchy>\n" +
                        "</Dimension>\n" +
                        "<Dimension name=\"Customers\" foreignKey=\"customer_id\">\n" +
                        "<Hierarchy hasAll=\"true\" allMemberName=\"All Customers\" primaryKeyTable=\"customer\" primaryKey=\"customer_id\">\n" +
                        "    <Join leftKey=\"customer_region_id\" rightKey=\"region_id\">\n" +
                        "      <Table name=\"customer\"/>\n" +
                        "      <Table name=\"region\" alias=\"customer_region\"/>\n" +
                        "    </Join>\n" +
                        "  <Level name=\"Country\" table=\"customer\" column=\"country\"                      uniqueMembers=\"true\"/>\n" +
                        "  <Level name=\"Region\"  table=\"customer_region\"   column=\"sales_region\"                 uniqueMembers=\"true\"/>\n" +
                        "  <Level name=\"City\"    table=\"customer\" column=\"city\"                         uniqueMembers=\"false\"/>\n" +
                        "  <Level name=\"Name\"    table=\"customer\" column=\"customer_id\" type=\"Numeric\" uniqueMembers=\"true\"/>\n" +
                        "</Hierarchy>\n" +
                        "</Dimension>\n" +
                        "<Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\" formatString=\"Standard\"/>\n" +
                        "<Measure name=\"Store Sales\" column=\"store_sales\" aggregator=\"sum\" formatString=\"#,###.00\"/>\n" +
                        "</Cube>",
                null, null, null, null);

        testContext.assertQueryReturns(
                "select  {[Store].[USA].[South West]} on rows," +
                "{[Customers].[USA].[South West]} on columns" +
                " from " +
                "AliasedDimensionsTesting",
                fold("Axis #0:\n" +
                     "{}\n" +
                     "Axis #1:\n" +
                     "{[Customers].[All Customers].[USA].[South West]}\n" +
                     "Axis #2:\n" +
                     "{[Store].[All Stores].[USA].[South West]}\n" +
                     "Row #0: 72,631\n"));
    }

    /**
     * Tests two dimensions using same table (via different join paths).
     * both using a table alias.
     */
    public void testDimensionsShareJoinTableTwoAliases() {
        final TestContext testContext = TestContext.create(
                null,
                "<Cube name=\"AliasedDimensionsTesting\" defaultMeasure=\"Supply Time\">\n" +
                        "  <Table name=\"sales_fact_1997\"/>\n" +
                        "<Dimension name=\"Store\" foreignKey=\"store_id\">\n" +
                        "<Hierarchy hasAll=\"true\" primaryKeyTable=\"store\" primaryKey=\"store_id\">\n" +
                        "    <Join leftKey=\"region_id\" rightKey=\"region_id\">\n" +
                        "      <Table name=\"store\"/>\n" +
                        "      <Table name=\"region\" alias=\"store_region\"/>\n" +
                        "    </Join>\n" +
                        " <Level name=\"Store Country\" table=\"store\"  column=\"store_country\" uniqueMembers=\"true\"/>\n" +
                        " <Level name=\"Store Region\"  table=\"store_region\" column=\"sales_region\"  uniqueMembers=\"true\"/>\n" +
                        " <Level name=\"Store Name\"    table=\"store\"  column=\"store_name\"    uniqueMembers=\"true\"/>\n" +
                        "</Hierarchy>\n" +
                        "</Dimension>\n" +
                        "<Dimension name=\"Customers\" foreignKey=\"customer_id\">\n" +
                        "<Hierarchy hasAll=\"true\" allMemberName=\"All Customers\" primaryKeyTable=\"customer\" primaryKey=\"customer_id\">\n" +
                        "    <Join leftKey=\"customer_region_id\" rightKey=\"region_id\">\n" +
                        "      <Table name=\"customer\"/>\n" +
                        "      <Table name=\"region\" alias=\"customer_region\"/>\n" +
                        "    </Join>\n" +
                        "  <Level name=\"Country\" table=\"customer\" column=\"country\"                      uniqueMembers=\"true\"/>\n" +
                        "  <Level name=\"Region\"  table=\"customer_region\"   column=\"sales_region\"                 uniqueMembers=\"true\"/>\n" +
                        "  <Level name=\"City\"    table=\"customer\" column=\"city\"                         uniqueMembers=\"false\"/>\n" +
                        "  <Level name=\"Name\"    table=\"customer\" column=\"customer_id\" type=\"Numeric\" uniqueMembers=\"true\"/>\n" +
                        "</Hierarchy>\n" +
                        "</Dimension>\n" +
                        "<Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\" formatString=\"Standard\"/>\n" +
                        "<Measure name=\"Store Sales\" column=\"store_sales\" aggregator=\"sum\" formatString=\"#,###.00\"/>\n" +
                        "</Cube>",
                null, null, null, null);

        testContext.assertQueryReturns(
                "select  {[Store].[USA].[South West]} on rows," +
                "{[Customers].[USA].[South West]} on columns" +
                " from " +
                "AliasedDimensionsTesting",
                fold("Axis #0:\n" +
                     "{}\n" +
                     "Axis #1:\n" +
                     "{[Customers].[All Customers].[USA].[South West]}\n" +
                     "Axis #2:\n" +
                     "{[Store].[All Stores].[USA].[South West]}\n" +
                     "Row #0: 72,631\n"));
    }

    /**
     * Tests two dimensions using same table (via different join paths).
     * both using a table alias.
     */
    public void testTwoAliasesDimensionsShareTable() {
        final TestContext testContext = TestContext.create(
                null,
                "<Cube name=\"AliasedDimensionsTesting\" defaultMeasure=\"Supply Time\">\n" +
                        "  <Table name=\"inventory_fact_1997\"/>\n" +
                        "  <Dimension name=\"StoreA\" foreignKey=\"store_id\">" +
                        "    <Hierarchy hasAll=\"true\" primaryKey=\"store_id\">" +
                        "      <Table name=\"store\" alias=\"storea\"/>" +
                        "      <Level name=\"Store Country\" column=\"store_country\" uniqueMembers=\"true\"/>" +
                        "      <Level name=\"Store Name\"  column=\"store_name\" uniqueMembers=\"true\"/>" +
                        "    </Hierarchy>" +
                        "  </Dimension>" +

                        "  <Dimension name=\"StoreB\" foreignKey=\"warehouse_id\">" +
                        "    <Hierarchy hasAll=\"true\" primaryKey=\"store_id\">" +
                        "      <Table name=\"store\"  alias=\"storeb\"/>" +
                        "      <Level name=\"Store Country\" column=\"store_country\" uniqueMembers=\"true\"/>" +
                        "      <Level name=\"Store Name\" column=\"store_name\" uniqueMembers=\"true\"/>" +
                        "    </Hierarchy>" +
                        "  </Dimension>" +
                        "  <Measure name=\"Store Invoice\" column=\"store_invoice\" " +
                        "aggregator=\"sum\"/>\n" +
                        "  <Measure name=\"Supply Time\" column=\"supply_time\" " +
                        "aggregator=\"sum\"/>\n" +
                        "  <Measure name=\"Warehouse Cost\" column=\"warehouse_cost\" " +
                        "aggregator=\"sum\"/>\n" +
                        "</Cube>",
                null, null, null, null);

        testContext.assertQueryReturns(
                "select {[StoreA].[USA]} on rows," +
                "{[StoreB].[USA]} on columns" +
                " from " +
                "AliasedDimensionsTesting",
                fold(
                        "Axis #0:\n" +
                        "{}\n" +
                        "Axis #1:\n" +
                        "{[StoreB].[All StoreBs].[USA]}\n" +
                        "Axis #2:\n" +
                        "{[StoreA].[All StoreAs].[USA]}\n" +
                        "Row #0: 10,425\n"));
    }

    /**
     * Tests two dimensions using same table with same foreign key.
     * both using a table alias.
     */
    public void testTwoAliasesDimensionsShareTableSameForeignKeys() {
        final TestContext testContext = TestContext.create(
                null,
                "<Cube name=\"AliasedDimensionsTesting\" defaultMeasure=\"Supply Time\">\n" +
                        "  <Table name=\"inventory_fact_1997\"/>\n" +
                        "  <Dimension name=\"StoreA\" foreignKey=\"store_id\">" +
                        "    <Hierarchy hasAll=\"true\" primaryKey=\"store_id\">" +
                        "      <Table name=\"store\" alias=\"storea\"/>" +
                        "      <Level name=\"Store Country\" column=\"store_country\" uniqueMembers=\"true\"/>" +
                        "      <Level name=\"Store Name\" column=\"store_name\" uniqueMembers=\"true\"/>" +
                        "    </Hierarchy>" +
                        "  </Dimension>" +

                        "  <Dimension name=\"StoreB\" foreignKey=\"store_id\">" +
                        "    <Hierarchy hasAll=\"true\" primaryKey=\"store_id\">" +
                        "      <Table name=\"store\"  alias=\"storeb\"/>" +
                        "      <Level name=\"Store Country\" column=\"store_country\" uniqueMembers=\"true\"/>" +
                        "      <Level name=\"Store Name\" column=\"store_name\" uniqueMembers=\"true\"/>" +
                        "    </Hierarchy>" +
                        "  </Dimension>" +
                        "  <Measure name=\"Store Invoice\" column=\"store_invoice\" " +
                        "aggregator=\"sum\"/>\n" +
                        "  <Measure name=\"Supply Time\" column=\"supply_time\" " +
                        "aggregator=\"sum\"/>\n" +
                        "  <Measure name=\"Warehouse Cost\" column=\"warehouse_cost\" " +
                        "aggregator=\"sum\"/>\n" +
                        "</Cube>",
                null, null, null, null);

        testContext.assertQueryReturns(
                "select {[StoreA].[USA]} on rows," +
                "{[StoreB].[USA]} on columns" +
                " from " +
                "AliasedDimensionsTesting",
                    fold(
                    "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[StoreB].[All StoreBs].[USA]}\n" +
                    "Axis #2:\n" +
                    "{[StoreA].[All StoreAs].[USA]}\n" +
                    "Row #0: 10,425\n")
         );
    }

    /**
     * Test Multiple DimensionUsages on same Dimension.
     */
    public void testMultipleDimensionUsages() {
        TestContext testContext = TestContext.create(
                null,

                "<Cube name=\"Sales Two Dimensions\">\n" +
                    "  <Table name=\"sales_fact_1997\"/>\n" + 
                    "  <DimensionUsage name=\"Time\" source=\"Time\" foreignKey=\"time_id\"/>\n" +
                    "  <DimensionUsage name=\"Time2\" source=\"Time\" foreignKey=\"product_id\"/>\n" +
                    "  <DimensionUsage name=\"Store\" source=\"Store\" foreignKey=\"store_id\"/>\n" +
                    "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\" "+
                    "   formatString=\"Standard\"/>\n" +
                    "  <Measure name=\"Store Cost\" column=\"store_cost\" aggregator=\"sum\"" +
                    "   formatString=\"#,###.00\"/>\n" +
                    "</Cube>",
                null, null, null, null);

       testContext.assertQueryReturns(
                "select\n" +
                    " {[Time2].[1997]} on columns,\n" +
                    " {[Time].[1997].[Q3]} on rows\n" +
                    "From [Sales Two Dimensions]",
                fold(
                    "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Time2].[1997]}\n" +
                    "Axis #2:\n" +
                    "{[Time].[1997].[Q3]}\n" +
                    "Row #0: 16,266\n"));
    }

    /**
     * This test verifies that the createDimension() API call is working correctly.
     */
    public void testDimensionCreation() {
        TestContext testContext = TestContext.create(
                null,

                "<Cube name=\"Sales Create Dimension\">\n" +
                    "  <Table name=\"sales_fact_1997\"/>\n" +
                    "  <DimensionUsage name=\"Store\" source=\"Store\" foreignKey=\"store_id\"/>\n" +
                    "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\" "+
                    "   formatString=\"Standard\"/>\n" +
                    "  <Measure name=\"Store Cost\" column=\"store_cost\" aggregator=\"sum\"" +
                    "   formatString=\"#,###.00\"/>\n" +
                    "</Cube>",
                    null, null, null, null);
        Cube cube = testContext.getConnection().getSchema().lookupCube("Sales Create Dimension", true);
       
        testContext.assertQueryReturns(
                "select\n" +
                    "NON EMPTY {[Store].[All Stores].children} on columns \n" +
                    "From [Sales Create Dimension]",
                    fold(
                            "Axis #0:\n" +
                            "{}\n" +
                            "Axis #1:\n" +
                            "{[Store].[All Stores].[USA]}\n" +
                            "Row #0: 266,773\n"));
        
        String dimension = "<DimensionUsage name=\"Time\" source=\"Time\" foreignKey=\"time_id\"/>";
        testContext.getConnection().getSchema().createDimension(cube, dimension);
        
        testContext.assertQueryReturns(
                "select\n" +
                    "NON EMPTY {[Store].[All Stores].children} on columns, \n" +
                    "{[Time].[1997].[Q1]} on rows \n" +
                    "From [Sales Create Dimension]",
                    fold(
                            "Axis #0:\n" +
                            "{}\n" +
                            "Axis #1:\n" +
                            "{[Store].[All Stores].[USA]}\n" +
                            "Axis #2:\n" +
                            "{[Time].[1997].[Q1]}\n" +
                            "Row #0: 66,291\n"));
        
    }
    
    /**
     * Test DimensionUsage level attribute
     */
    public void testDimensionUsageLevel() {
        TestContext testContext = TestContext.create(
                null,

                "<Cube name=\"Customer Usage Level\">\n" +
                    "  <Table name=\"customer\"/>\n" + // alias=\"sales_fact_1997_multi\"/>\n" +
                    "  <DimensionUsage name=\"Store\" source=\"Store\" level=\"Store State\" foreignKey=\"state_province\"/>\n" +
                    "  <Measure name=\"Cars\" column=\"num_cars_owned\" aggregator=\"sum\"/>\n" +
                    "  <Measure name=\"Children\" column=\"total_children\" aggregator=\"sum\"/>\n" +
                    "</Cube>",
                    null, null, null, null);
        
       testContext.assertQueryReturns(
                "select\n" +
                    " {[Store].[Store State].members} on columns \n" +
                    "From [Customer Usage Level]",
                    fold(
                            "Axis #0:\n" +
                            "{}\n" +
                            "Axis #1:\n" +
                            "{[Store].[All Stores].[Canada].[BC]}\n" +
                            "{[Store].[All Stores].[Mexico].[DF]}\n" +
                            "{[Store].[All Stores].[Mexico].[Guerrero]}\n" +
                            "{[Store].[All Stores].[Mexico].[Jalisco]}\n" +
                            "{[Store].[All Stores].[Mexico].[Veracruz]}\n" +
                            "{[Store].[All Stores].[Mexico].[Yucatan]}\n" +
                            "{[Store].[All Stores].[Mexico].[Zacatecas]}\n" +
                            "{[Store].[All Stores].[USA].[CA]}\n" +
                            "{[Store].[All Stores].[USA].[OR]}\n" +
                            "{[Store].[All Stores].[USA].[WA]}\n" +
                            "Row #0: 7,700\n" +
                            "Row #0: 1,492\n" +
                            "Row #0: 228\n" +
                            "Row #0: 206\n" +
                            "Row #0: 195\n" +
                            "Row #0: 229\n" +
                            "Row #0: 1,209\n" +
                            "Row #0: 46,965\n" +
                            "Row #0: 4,686\n" +
                            "Row #0: 32,767\n"));
       
       // BC.children should return an empty list, considering that we've 
       // joined Store at the State level.
       
//       testContext.assertQueryReturns(
//               "select\n" +
//                   " {[Store].[All Stores].[Canada].[BC].children} on columns \n" +
//                   "From [Customer Usage Level]",
//               fold(
//                       "Axis #0:\n" +
//                       "{}\n" +
//                       "Axis #1:\n"));
    }

    
    /**
     * Test to verify naming of all member with 
     * dimension usage name is different then source name
     */
    public void testAllMemberMultipleDimensionUsages() {
        TestContext testContext = TestContext.create(
                null,

                "<Cube name=\"Sales Two Sales Dimensions\">\n" +
                    "  <Table name=\"sales_fact_1997\"/>\n" +
                    "  <DimensionUsage name=\"Store\" source=\"Store\" foreignKey=\"store_id\"/>\n" +
                    "  <DimensionUsage name=\"Store2\" source=\"Store\" foreignKey=\"product_id\"/>\n" +
                    "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\" "+
                    "   formatString=\"Standard\"/>\n" +
                    "  <Measure name=\"Store Cost\" column=\"store_cost\" aggregator=\"sum\"" +
                    "   formatString=\"#,###.00\"/>\n" +
                    "</Cube>",
                null, null, null, null);
        
       testContext.assertQueryReturns(
                "select\n" +
                    " {[Store].[All Stores]} on columns,\n" +
                    " {[Store2].[All Store2s]} on rows\n" +
                    "From [Sales Two Sales Dimensions]",
                    fold(
                            "Axis #0:\n" +
                            "{}\n" +
                            "Axis #1:\n" +
                            "{[Store].[All Stores]}\n" +
                            "Axis #2:\n" +
                            "{[Store2].[All Store2s]}\n" +
                            "Row #0: 266,773\n"));
    }
    
    /**
     * This test displays an informative error message if someone uses
     * an unaliased name instead of an aliased name
     */
    public void testNonAliasedDimensionUsage() {
        TestContext testContext = TestContext.create(
                null,

                "<Cube name=\"Sales Two Dimensions\">\n" +
                    "  <Table name=\"sales_fact_1997\"/>\n" +
                    "  <DimensionUsage name=\"Time2\" source=\"Time\" foreignKey=\"time_id\"/>\n" +
                    "  <DimensionUsage name=\"Store\" source=\"Store\" foreignKey=\"store_id\"/>\n" +
                    "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\" "+
                    "   formatString=\"Standard\"/>\n" +
                    "  <Measure name=\"Store Cost\" column=\"store_cost\" aggregator=\"sum\"" +
                    "   formatString=\"#,###.00\"/>\n" +
                    "</Cube>",
                null, null, null, null);

       testContext.assertThrows(
                "select\n" +
                    " {[Time].[1997]} on columns \n" +
                    "From [Sales Two Dimensions]",
                "In cube \"Sales Two Dimensions\" use of unaliased Dimension name \"[Time]\" rather than the alias name \"Time2\"");
    }

    /**
     * Tests a cube whose fact table is a &lt;View&gt; element.
     */
    public void testViewFactTable() {
        TestContext testContext = TestContext.create(
            null,

            // Warehouse cube where the default member in the Warehouse
            // dimension is USA.
            "<Cube name=\"Warehouse (based on view)\">\n" +
                "  <View alias=\"FACT\">\n" +
                "    <SQL dialect=\"generic\">\n" +
                "     <![CDATA[select * from \"inventory_fact_1997\" as \"FOOBAR\"]]>\n" +
                "    </SQL>\n" +
                "    <SQL dialect=\"oracle\">\n" +
                "     <![CDATA[select * from \"inventory_fact_1997\" \"FOOBAR\"]]>\n" +
                "    </SQL>\n" +
                "    <SQL dialect=\"mysql\">\n" +
                "     <![CDATA[select * from `inventory_fact_1997` as `FOOBAR`]]>\n" +
                "    </SQL>\n" +
                "  </View>\n" +
                "  <DimensionUsage name=\"Time\" source=\"Time\" foreignKey=\"time_id\"/>\n" +
                "  <DimensionUsage name=\"Product\" source=\"Product\" foreignKey=\"product_id\"/>\n" +
                "  <DimensionUsage name=\"Store\" source=\"Store\" foreignKey=\"store_id\"/>\n" +
                "  <Dimension name=\"Warehouse\" foreignKey=\"warehouse_id\">\n" +
                "    <Hierarchy hasAll=\"false\" defaultMember=\"[USA]\" primaryKey=\"warehouse_id\"> \n" +
                "      <Table name=\"warehouse\"/>\n" +
                "      <Level name=\"Country\" column=\"warehouse_country\" uniqueMembers=\"true\"/>\n" +
                "      <Level name=\"State Province\" column=\"warehouse_state_province\"\n" +
                "          uniqueMembers=\"true\"/>\n" +
                "      <Level name=\"City\" column=\"warehouse_city\" uniqueMembers=\"false\"/>\n" +
                "      <Level name=\"Warehouse Name\" column=\"warehouse_name\" uniqueMembers=\"true\"/>\n" +
                "    </Hierarchy>\n" +
                "  </Dimension>\n" +
                "  <Measure name=\"Warehouse Cost\" column=\"warehouse_cost\" aggregator=\"sum\"/>\n" +
                "  <Measure name=\"Warehouse Sales\" column=\"warehouse_sales\" aggregator=\"sum\"/>\n" +
                "</Cube>",
            null, null, null, null);

        testContext.assertQueryReturns(
            "select\n" +
                " {[Time].[1997], [Time].[1997].[Q3]} on columns,\n" +
                " {[Store].[USA].Children} on rows\n" +
                "From [Warehouse (based on view)]\n" +
                "where [Warehouse].[USA]",
            fold("Axis #0:\n" +
                "{[Warehouse].[USA]}\n" +
                "Axis #1:\n" +
                "{[Time].[1997]}\n" +
                "{[Time].[1997].[Q3]}\n" +
                "Axis #2:\n" +
                "{[Store].[All Stores].[USA].[CA]}\n" +
                "{[Store].[All Stores].[USA].[OR]}\n" +
                "{[Store].[All Stores].[USA].[WA]}\n" +
                "Row #0: 25,789.086\n" +
                "Row #0: 8,624.791\n" +
                "Row #1: 17,606.904\n" +
                "Row #1: 3,812.023\n" +
                "Row #2: 45,647.262\n" +
                "Row #2: 12,664.162\n"));
    }

    /**
     * Tests a cube whose fact table is a &lt;View&gt; element, and which
     * has dimensions based on the fact table.
     */
    public void testViewFactTable2() {
        TestContext testContext = TestContext.create(
            null,
            // Similar to "Store" cube in FoodMart.xml.
            "<Cube name=\"Store2\">\n" +
                "  <View alias=\"FACT\">\n" +
                "    <SQL dialect=\"generic\">\n" +
                "     <![CDATA[select * from \"store\" as \"FOOBAR\"]]>\n" +
                "    </SQL>\n" +
                "    <SQL dialect=\"oracle\">\n" +
                "     <![CDATA[select * from \"store\" \"FOOBAR\"]]>\n" +
                "    </SQL>\n" +
                "    <SQL dialect=\"mysql\">\n" +
                "     <![CDATA[select * from `store` as `FOOBAR`]]>\n" +
                "    </SQL>\n" +
                "  </View>\n" +
                "  <!-- We could have used the shared dimension \"Store Type\", but we\n" +
                "     want to test private dimensions without primary key. -->\n" +
                "  <Dimension name=\"Store Type\">\n" +
                "    <Hierarchy hasAll=\"true\">\n" +
                "      <Level name=\"Store Type\" column=\"store_type\" uniqueMembers=\"true\"/>\n" +
                "    </Hierarchy>\n" +
                "  </Dimension>\n" +
                "\n" +
                "  <Measure name=\"Store Sqft\" column=\"store_sqft\" aggregator=\"sum\"\n" +
                "      formatString=\"#,###\"/>\n" +
                "  <Measure name=\"Grocery Sqft\" column=\"grocery_sqft\" aggregator=\"sum\"\n" +
                "      formatString=\"#,###\"/>\n" +
                "\n" +
                "</Cube>",
            null, null, null, null);
        testContext.assertQueryReturns(
            "select {[Store Type].Children} on columns from [Store2]",
            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Store Type].[All Store Types].[Deluxe Supermarket]}\n" +
                "{[Store Type].[All Store Types].[Gourmet Supermarket]}\n" +
                "{[Store Type].[All Store Types].[HeadQuarters]}\n" +
                "{[Store Type].[All Store Types].[Mid-Size Grocery]}\n" +
                "{[Store Type].[All Store Types].[Small Grocery]}\n" +
                "{[Store Type].[All Store Types].[Supermarket]}\n" +
                "Row #0: 146,045\n" +
                "Row #0: 47,447\n" +
                "Row #0: \n" +
                "Row #0: 109,343\n" +
                "Row #0: 75,281\n" +
                "Row #0: 193,480\n"));
    }

    /**
     * Tests that the deprecated "distinct count" value for the
     * Measure@aggregator attribute still works. The preferred value these days
     * is "distinct-count".
     */
    public void testDeprecatedDistinctCountAggregator() {
        TestContext testContext = TestContext.createSubstitutingCube(
            "Sales", null,
                "  <Measure name=\"Customer Count2\" column=\"customer_id\"\n" +
                "      aggregator=\"distinct count\" formatString=\"#,###\"/>\n" +
                "  <CalculatedMember\n" +
                "      name=\"Half Customer Count\"\n" +
                "      dimension=\"Measures\"\n" +
                "      visible=\"false\"\n" +
                "      formula=\"[Measures].[Customer Count2] / 2\">\n" +
                "  </CalculatedMember>");
        testContext.assertQueryReturns(
            "select {[Measures].[Unit Sales]," +
                "    [Measures].[Customer Count], " +
                "    [Measures].[Customer Count2], " +
                "    [Measures].[Half Customer Count]} on 0,\n" +
                " {[Store].[USA].Children} ON 1\n" +
                "FROM [Sales]\n" +
                "WHERE ([Gender].[M])",
            fold("Axis #0:\n" +
                "{[Gender].[All Gender].[M]}\n" +
                "Axis #1:\n" +
                "{[Measures].[Unit Sales]}\n" +
                "{[Measures].[Customer Count]}\n" +
                "{[Measures].[Customer Count2]}\n" +
                "{[Measures].[Half Customer Count]}\n" +
                "Axis #2:\n" +
                "{[Store].[All Stores].[USA].[CA]}\n" +
                "{[Store].[All Stores].[USA].[OR]}\n" +
                "{[Store].[All Stores].[USA].[WA]}\n" +
                "Row #0: 37,989\n" +
                "Row #0: 1,389\n" +
                "Row #0: 1,389\n" +
                "Row #0: 695\n" +
                "Row #1: 34,623\n" +
                "Row #1: 536\n" +
                "Row #1: 536\n" +
                "Row #1: 268\n" +
                "Row #2: 62,603\n" +
                "Row #2: 901\n" +
                "Row #2: 901\n" +
                "Row #2: 451\n"));
    }

    /**
     * Tests that an invalid aggregator causes an error.
     */
    public void testInvalidAggregator() {
        TestContext testContext = TestContext.createSubstitutingCube(
            "Sales", null,
                "  <Measure name=\"Customer Count3\" column=\"customer_id\"\n" +
                "      aggregator=\"invalidAggregator\" formatString=\"#,###\"/>\n" +
                "  <CalculatedMember\n" +
                "      name=\"Half Customer Count\"\n" +
                "      dimension=\"Measures\"\n" +
                "      visible=\"false\"\n" +
                "      formula=\"[Measures].[Customer Count2] / 2\">\n" +
                "  </CalculatedMember>");
        testContext.assertThrows(
            "select from [Sales]",
            "Unknown aggregator 'invalidAggregator'; valid aggregators are: 'sum', 'count', 'min', 'max', 'avg', 'distinct-count'");
    }

    /**
     * Testcase for
     * <a href="https://sourceforge.net/tracker/?func=detail&atid=414613&aid=1583462&group_id=35302">
     * Bug 1721514, "'unknown usage' messages"</a>.
     */
    public void testUnknownUsages() {
        if (!MondrianProperties.instance().ReadAggregates.get()) {
            return;
        }
        final Logger logger = Logger.getLogger(AggTableManager.class);
        final StringWriter sw = new StringWriter();
        final Appender appender =
            new WriterAppender(new SimpleLayout(), sw);
        final LevelRangeFilter filter = new LevelRangeFilter();
        filter.setLevelMin(Level.WARN);
        appender.addFilter(filter);
        logger.addAppender(appender);
        try {
            TestContext testContext = TestContext.create(
                "<?xml version=\"1.0\"?>\n" +
                    "<Schema name=\"FoodMart\">\n" +
                    "<Cube name=\"Sales Degen\">\n" +
                    "  <Table name=\"sales_fact_1997\">\n" +
                    "    <AggExclude pattern=\"agg_c_14_sales_fact_1997\"/>\n" +
                    "    <AggExclude pattern=\"agg_l_05_sales_fact_1997\"/>\n" +
                    "    <AggExclude pattern=\"agg_g_ms_pcat_sales_fact_1997\"/>\n" +
                    "    <AggExclude pattern=\"agg_ll_01_sales_fact_1997\"/>\n" +
                    "    <AggExclude pattern=\"agg_c_special_sales_fact_1997\"/>\n" +
                    "    <AggExclude pattern=\"agg_l_03_sales_fact_1997\"/>\n" +
                    "    <AggExclude pattern=\"agg_l_04_sales_fact_1997\"/>\n" +
                    "    <AggExclude pattern=\"agg_pl_01_sales_fact_1997\"/>\n" +
                    "    <AggName name=\"agg_c_10_sales_fact_1997\">\n" +
                    "      <AggFactCount column=\"fact_count\"/>\n" +
                    "      <AggMeasure name=\"[Measures].[Store Cost]\" column=\"store_cost\" />\n" +
                    "      <AggMeasure name=\"[Measures].[Store Sales]\" column=\"store_sales\" />\n" +
                    "     </AggName>\n" +
                    "  </Table>\n" +
                    "  <Dimension name=\"Time\" type=\"TimeDimension\" foreignKey=\"time_id\">\n" +
                    "    <Hierarchy hasAll=\"false\" primaryKey=\"time_id\">\n" +
                    "      <Table name=\"time_by_day\"/>\n" +
                    "      <Level name=\"Year\" column=\"the_year\" type=\"Numeric\" uniqueMembers=\"true\"\n" +
                    "          levelType=\"TimeYears\"/>\n" +
                    "      <Level name=\"Quarter\" column=\"quarter\" uniqueMembers=\"false\"\n" +
                    "          levelType=\"TimeQuarters\"/>\n" +
                    "      <Level name=\"Month\" column=\"month_of_year\" uniqueMembers=\"false\" type=\"Numeric\"\n" +
                    "          levelType=\"TimeMonths\"/>\n" +
                    "    </Hierarchy>\n" +
                    "  </Dimension>\n" +
                    "  <Dimension name=\"Time Degenerate\">\n" +
                    "    <Hierarchy hasAll=\"true\" primaryKey=\"time_id\">\n" +
                    "      <Level name=\"day\" column=\"time_id\"/>\n" +
                    "      <Level name=\"month\" column=\"product_id\" type=\"Numeric\"/>\n" +
                    "    </Hierarchy>" +
                    "  </Dimension>" +
                    "  <Measure name=\"Store Cost\" column=\"store_cost\" aggregator=\"sum\"\n" +
                    "      formatString=\"#,###.00\"/>\n" +
                    "  <Measure name=\"Store Sales\" column=\"store_sales\" aggregator=\"sum\"\n" +
                    "      formatString=\"#,###.00\"/>\n" +
                    "</Cube>\n" +
                    "</Schema>");
            testContext.assertQueryReturns(
                "select from [Sales Degen]",
                fold(
                    "Axis #0:\n" +
                        "{}\n" +
                        "225,627.23"));
        } finally {
            logger.removeAppender(appender);
        }
        // Note that 'product_id' is NOT one of the columns with unknown usage.
        // It is used as a level in the degenerate dimension [Time Degenerate].
        assertEquals(
            fold("WARN - Recognizer.checkUnusedColumns: Candidate aggregate table 'agg_c_10_sales_fact_1997' for fact table 'sales_fact_1997' has a column 'customer_count' with unknown usage.\n" +
                "WARN - Recognizer.checkUnusedColumns: Candidate aggregate table 'agg_c_10_sales_fact_1997' for fact table 'sales_fact_1997' has a column 'month_of_year' with unknown usage.\n" +
                "WARN - Recognizer.checkUnusedColumns: Candidate aggregate table 'agg_c_10_sales_fact_1997' for fact table 'sales_fact_1997' has a column 'quarter' with unknown usage.\n" +
                "WARN - Recognizer.checkUnusedColumns: Candidate aggregate table 'agg_c_10_sales_fact_1997' for fact table 'sales_fact_1997' has a column 'the_year' with unknown usage.\n" +
                "WARN - Recognizer.checkUnusedColumns: Candidate aggregate table 'agg_c_10_sales_fact_1997' for fact table 'sales_fact_1997' has a column 'unit_sales' with unknown usage.\n" +
                "WARN - Recognizer.checkUnusedColumns: Candidate aggregate table 'agg_lc_100_sales_fact_1997' for fact table 'sales_fact_1997' has a column 'customer_id' with unknown usage.\n" +
                "WARN - Recognizer.checkUnusedColumns: Candidate aggregate table 'agg_lc_100_sales_fact_1997' for fact table 'sales_fact_1997' has a column 'unit_sales' with unknown usage.\n"),
            sw.toString());
    }

    public void testUnknownUsages1() {
        if (!MondrianProperties.instance().ReadAggregates.get()) {
            return;
        }
        final Logger logger = Logger.getLogger(AggTableManager.class);
        final StringWriter sw = new StringWriter();
        final Appender appender =
            new WriterAppender(new SimpleLayout(), sw);
        final LevelRangeFilter filter = new LevelRangeFilter();
        filter.setLevelMin(Level.WARN);
        appender.addFilter(filter);
        logger.addAppender(appender);
        try {
            TestContext testContext = TestContext.create(
                "<?xml version=\"1.0\"?>\n" +
                    "<Schema name=\"FoodMart\">\n" +
                    "<Cube name=\"Denormalized Sales\">\n" +
                    "  <Table name=\"sales_fact_1997\">\n" +
                    "    <AggExclude pattern=\"agg_c_14_sales_fact_1997\"/>\n" +
                    "    <AggExclude pattern=\"agg_l_05_sales_fact_1997\"/>\n" +
                    "    <AggExclude pattern=\"agg_g_ms_pcat_sales_fact_1997\"/>\n" +
                    "    <AggExclude pattern=\"agg_ll_01_sales_fact_1997\"/>\n" +
                    "    <AggExclude pattern=\"agg_c_special_sales_fact_1997\"/>\n" +
                    "    <AggExclude pattern=\"agg_l_04_sales_fact_1997\"/>\n" +
                    "    <AggExclude pattern=\"agg_pl_01_sales_fact_1997\"/>\n" +
                    "    <AggExclude pattern=\"agg_c_10_sales_fact_1997\"/>\n" +
                    "    <AggName name=\"agg_l_03_sales_fact_1997\">\n" +
                    "      <AggFactCount column=\"fact_count\"/>\n" +
                    "      <AggMeasure name=\"[Measures].[Store Cost]\" column=\"store_cost\" />\n" +
                    "      <AggMeasure name=\"[Measures].[Store Sales]\" column=\"store_sales\" />\n" +
                    "      <AggMeasure name=\"[Measures].[Unit Sales]\" column=\"unit_sales\" />\n" +
                    "      <AggLevel name=\"[Customer].[Customer ID]\" column=\"customer_id\" />\n" +
                    "      <AggForeignKey factColumn=\"time_id\" aggColumn=\"time_id\" />\n" +
                    "     </AggName>\n" +
                    "  </Table>\n" +
                    "  <Dimension name=\"Time\" type=\"TimeDimension\" foreignKey=\"time_id\">\n" +
                    "    <Hierarchy hasAll=\"false\" primaryKey=\"time_id\">\n" +
                    "      <Table name=\"time_by_day\"/>\n" +
                    "      <Level name=\"Year\" column=\"the_year\" type=\"Numeric\" uniqueMembers=\"true\"\n" +
                    "          levelType=\"TimeYears\"/>\n" +
                    "      <Level name=\"Quarter\" column=\"quarter\" uniqueMembers=\"false\"\n" +
                    "          levelType=\"TimeQuarters\"/>\n" +
                    "      <Level name=\"Month\" column=\"month_of_year\" uniqueMembers=\"false\" type=\"Numeric\"\n" +
                    "          levelType=\"TimeMonths\"/>\n" +
                    "    </Hierarchy>\n" +
                    "  </Dimension>\n" +
                    "  <Dimension name=\"Customer\">\n" +
                    "    <Hierarchy hasAll=\"true\" primaryKey=\"customer_id\">\n" +
                    "      <Level name=\"Customer ID\" column=\"customer_id\"/>\n" +
                    "    </Hierarchy>" +
                    "  </Dimension>" +
                    "  <Dimension name=\"Product\">\n" +
                    "    <Hierarchy hasAll=\"true\" primaryKey=\"product_id\">\n" +
                    "      <Level name=\"Product ID\" column=\"product_id\"/>\n" +
                    "    </Hierarchy>" +
                    "  </Dimension>" +
                    "  <Measure name=\"Store Cost\" column=\"store_cost\" aggregator=\"sum\"\n" +
                    "      formatString=\"#,###.00\"/>\n" +
                    "  <Measure name=\"Store Sales\" column=\"store_sales\" aggregator=\"sum\"\n" +
                    "      formatString=\"#,###.00\"/>\n" +
                    "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\"\n" +
                    "      formatString=\"#,###\"/>\n" +
                    "</Cube>\n" +
                    "</Schema>");
            testContext.assertQueryReturns(
                "select from [Denormalized Sales]",
                fold(
                    "Axis #0:\n" +
                        "{}\n" +
                        "225,627.23"));
        } finally {
            logger.removeAppender(appender);
        }
        assertEquals(
            fold("WARN - Recognizer.checkUnusedColumns: Candidate aggregate table 'agg_l_03_sales_fact_1997' for fact table 'sales_fact_1997' has a column 'time_id' with unknown usage.\n"),
            sw.toString());
    }

    /**
     * Bug 1578545, "ClassCastException in AggQuerySpec" occurs when two cubes
     * have the same fact table, distinct aggregate tables, and measures with
     * the same name.
     *
     * <p>This test case attempts to reproduce this issue by creating that
     * environment, but it found a different issue: a measure came back with a
     * cell value which was from a different measure. The root cause is
     * probably the same: when measures are registered in a star, they should
     * be qualified by cube name.
     */
    public void testBug1578545() {
        final TestContext testContext =
            TestContext.create(
                null,
                "<Cube name=\"Sales2\" defaultMeasure=\"Unit Sales\">"
                    + "  <Table name=\"sales_fact_1997\">\n"
                    + "  </Table>\n"
                    + "  <DimensionUsage name=\"Time\" source=\"Time\" foreignKey=\"time_id\"/>\n"
                    + "  <DimensionUsage name=\"Product\" source=\"Product\" foreignKey=\"product_id\"/>\n"
                    + "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\"\n"
                    + "      formatString=\"Standard\"/>\n"
                    + "  <Measure name=\"Store Cost\" column=\"store_cost\" aggregator=\"sum\"\n"
                    + "      formatString=\"#,###.00\"/>\n"
                    + "</Cube>",
                null, null, null, null);
        
        // With bug, and with aggregates enabled, query against Sales returns
        // 565,238, which is actually the total for [Store Sales]. I think the
        // aggregate tables are getting crossed.
        final String expected =
            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Unit Sales]}\n" +
                "Row #0: 266,773\n");
        testContext.assertQueryReturns(
            "select {[Measures]} on 0 from [Sales2]",
            expected);
        testContext.assertQueryReturns(
            "select {[Measures]} on 0 from [Sales]",
            expected);
    }
    
    /** 
     * This tests for bug #1746362 Property column shifting when use captionColumn.
     */
    public void testBug1746362() {

        // In order to reproduce the problem a dimension specifying captionColumn and 
        // Properties were required.
        final TestContext testContext = TestContext.createSubstitutingCube(
                "Sales",
                "  <Dimension name=\"Store2\" foreignKey=\"store_id\">\n" +
                "    <Hierarchy name=\"Store2\" hasAll=\"true\" allMemberName=\"All Stores\" primaryKey=\"store_id\">\n" +
                "      <Table name=\"store_ragged\"/>\n" +
                "      <Level name=\"Store2\" table=\"store_ragged\" column=\"store_id\" captionColumn=\"store_name\" uniqueMembers=\"true\">\n" +
                "           <Property name=\"Store Type\" column=\"store_type\"/>" + 
                "           <Property name=\"Store Manager\" column=\"store_manager\"/>" +
                "     </Level>" +                     
                "    </Hierarchy>\n" +
                "  </Dimension>\n");

        // In the query below Mondrian (prior to the fix) would 
        // return the store name instead of the store type.
        testContext.assertQueryReturns("WITH\n" +
                "   MEMBER [Measures].[StoreType] AS \n" +
                "   '[Store2].CurrentMember.Properties(\"Store Type\")'\n" +
                "SELECT\n" +
                "   NonEmptyCrossJoin({[Store2].[All Stores].children}, {[Product].[All Products]}) ON ROWS,\n" +
                "   { [Measures].[Store Sales], [Measures].[StoreType]} ON COLUMNS\n" +
                "FROM Sales",
                fold("Axis #0:\n" +
                        "{}\n" +
                        "Axis #1:\n" +
                        "{[Measures].[Store Sales]}\n" +
                        "{[Measures].[StoreType]}\n" +
                        "Axis #2:\n" +
                        "{[Store2.Store2].[All Stores].[2], [Product].[All Products]}\n" +
                        "{[Store2.Store2].[All Stores].[3], [Product].[All Products]}\n" +
                        "{[Store2.Store2].[All Stores].[6], [Product].[All Products]}\n" +
                        "{[Store2.Store2].[All Stores].[7], [Product].[All Products]}\n" +
                        "{[Store2.Store2].[All Stores].[11], [Product].[All Products]}\n" +
                        "{[Store2.Store2].[All Stores].[13], [Product].[All Products]}\n" +
                        "{[Store2.Store2].[All Stores].[14], [Product].[All Products]}\n" +
                        "{[Store2.Store2].[All Stores].[15], [Product].[All Products]}\n" +
                        "{[Store2.Store2].[All Stores].[16], [Product].[All Products]}\n" +
                        "{[Store2.Store2].[All Stores].[17], [Product].[All Products]}\n" +
                        "{[Store2.Store2].[All Stores].[22], [Product].[All Products]}\n" +
                        "{[Store2.Store2].[All Stores].[23], [Product].[All Products]}\n" +
                        "{[Store2.Store2].[All Stores].[24], [Product].[All Products]}\n" +
                        "Row #0: 4,739.23\n" +
                        "Row #0: Small Grocery\n" +
                        "Row #1: 52,896.30\n" +
                        "Row #1: Supermarket\n" +
                        "Row #2: 45,750.24\n" +
                        "Row #2: Gourmet Supermarket\n" +
                        "Row #3: 54,545.28\n" +
                        "Row #3: Supermarket\n" +
                        "Row #4: 55,058.79\n" +
                        "Row #4: Supermarket\n" +
                        "Row #5: 87,218.28\n" +
                        "Row #5: Deluxe Supermarket\n" +
                        "Row #6: 4,441.18\n" +
                        "Row #6: Small Grocery\n" +
                        "Row #7: 52,644.07\n" +
                        "Row #7: Supermarket\n" +
                        "Row #8: 49,634.46\n" +
                        "Row #8: Supermarket\n" +
                        "Row #9: 74,843.96\n" +
                        "Row #9: Deluxe Supermarket\n" +
                        "Row #10: 4,705.97\n" +
                        "Row #10: Small Grocery\n" +
                        "Row #11: 24,329.23\n" +
                        "Row #11: Mid-Size Grocery\n" +
                        "Row #12: 54,431.14\n" +
                        "Row #12: Supermarket\n"));              
    }

    public void testCubeWithOneDimensionOneMeasure() {
        final TestContext testContext = TestContext.create(
            null,
            "<Cube name=\"OneDim\" defaultMeasure=\"Unit Sales\">\n"
            + "  <Table name=\"sales_fact_1997\"/>\n"
            + "  <Dimension name=\"Promotion Media\" foreignKey=\"promotion_id\">\n"
            + "    <Hierarchy hasAll=\"true\" allMemberName=\"All Media\" primaryKey=\"promotion_id\" defaultMember=\"All Media\">\n"
            + "      <Table name=\"promotion\"/>\n"
            + "      <Level name=\"Media Type\" column=\"media_type\" uniqueMembers=\"true\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\"\n"
            + "      formatString=\"Standard\"/>\n"
            + "</Cube>",
            null, null, null, null);
        testContext.assertQueryReturns(
            "select {[Promotion Media]} on columns from [OneDim]",
            fold(
                "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Promotion Media].[All Media]}\n" +
                    "Row #0: 266,773\n"));
    }

    public void testCubeWithOneDimensionUsageOneMeasure() {
        final TestContext testContext = TestContext.create(
            null,
            "<Cube name=\"OneDimUsage\" defaultMeasure=\"Unit Sales\">\n"
            + "  <Table name=\"sales_fact_1997\"/>\n"
            + "  <DimensionUsage name=\"Product\" source=\"Product\" foreignKey=\"product_id\"/>\n"
            + "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\"\n"
            + "      formatString=\"Standard\"/>\n"
            + "</Cube>",
            null, null, null, null);
        testContext.assertQueryReturns(
            "select {[Product].Children} on columns from [OneDimUsage]",
            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Product].[All Products].[Drink]}\n" +
                "{[Product].[All Products].[Food]}\n" +
                "{[Product].[All Products].[Non-Consumable]}\n" +
                "Row #0: 24,597\n" +
                "Row #0: 191,940\n" +
                "Row #0: 50,236\n"));
    }

    public void testCubeWithNoDimensions() {
        final TestContext testContext = TestContext.create(
            null,
            "<Cube name=\"NoDim\" defaultMeasure=\"Unit Sales\">\n"
            + "  <Table name=\"sales_fact_1997\"/>\n"
            + "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\"\n"
            + "      formatString=\"Standard\"/>\n"
            + "</Cube>",
            null, null, null, null);
        testContext.assertQueryReturns(
            "select {[Measures].[Unit Sales]} on columns from [NoDim]",
            fold(
                "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Unit Sales]}\n" +
                    "Row #0: 266,773\n"));
    }

    public void testCubeWithNoMeasuresFails() {
        final TestContext testContext = TestContext.create(
            null,
            "<Cube name=\"NoMeasures\">\n"
            + "  <Table name=\"sales_fact_1997\"/>\n"
            + "  <Dimension name=\"Promotion Media\" foreignKey=\"promotion_id\">\n"
            + "    <Hierarchy hasAll=\"true\" allMemberName=\"All Media\" primaryKey=\"promotion_id\" defaultMember=\"All Media\">\n"
            + "      <Table name=\"promotion\"/>\n"
            + "      <Level name=\"Media Type\" column=\"media_type\" uniqueMembers=\"true\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "</Cube>",
            null, null, null, null);
        testContext.assertThrows(
            "select {[Promotion Media]} on columns from [NoMeasures]",
            "Hierarchy '[Measures]' is invalid (has no members)");
    }

    public void testCubeWithOneCalcMeasure() {
        final TestContext testContext = TestContext.create(
            null,
            "<Cube name=\"OneCalcMeasure\">\n"
            + "  <Table name=\"sales_fact_1997\"/>\n"
            + "  <Dimension name=\"Promotion Media\" foreignKey=\"promotion_id\">\n"
            + "    <Hierarchy hasAll=\"true\" allMemberName=\"All Media\" primaryKey=\"promotion_id\" defaultMember=\"All Media\">\n"
            + "      <Table name=\"promotion\"/>\n"
            + "      <Level name=\"Media Type\" column=\"media_type\" uniqueMembers=\"true\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <CalculatedMember\n"
            + "      name=\"One\"\n"
            + "      dimension=\"Measures\"\n"
            + "      formula=\"1\"/>\n"
            + "</Cube>",
            null, null, null, null);

        // We would prefer if this query worked. I think we're hitting the bug
        // which occurs where the default member is calculated. For now, just
        // make sure that we get a reasonable error.
        testContext.assertThrows(
            "select {[Measures]} on columns from [OneCalcMeasure] where [Promotion Media].[TV]",
            "Hierarchy '[Measures]' is invalid (has no members)");
    }
    
    /**
     * this test verifies that RolapHierarchy.tableExists() supports views
     */
    public void testLevelTableAttributeAsView() {
        // Don't run this test if aggregates are enabled: two levels mapped to
        // the "gender" column confuse the agg engine.
        if (MondrianProperties.instance().ReadAggregates.get()) {
            return;
        }
        TestContext testContext = TestContext.createSubstitutingCube(
            "Sales",
            "<Dimension name=\"Gender2\" foreignKey=\"customer_id\">\n" +
                "  <Hierarchy hasAll=\"true\" allMemberName=\"All Gender\" primaryKey=\"customer_id\">\n" +
                "    <View alias=\"gender2\">\n" +
                "      <SQL dialect=\"generic\">\n" +
                "        <![CDATA[SELECT * FROM customer]]>\n" +
                "      </SQL>\n" +
                "      <SQL dialect=\"oracle\">\n" +
                "        <![CDATA[SELECT * FROM \"customer\"]]>\n" +
                "      </SQL>\n" +
                "      <SQL dialect=\"derby\">\n" +
                "        <![CDATA[SELECT * FROM \"customer\"]]>\n" +
                "      </SQL>\n" +
                "      <SQL dialect=\"luciddb\">\n" +
                "        <![CDATA[SELECT * FROM \"customer\"]]>\n" +
                "      </SQL>\n" +
                "      <SQL dialect=\"db2\">\n" +
                "        <![CDATA[SELECT * FROM \"customer\"]]>\n" +
                "      </SQL>\n" +
                "    </View>\n" +
                "    <Level name=\"Gender\" table=\"gender2\" column=\"gender\" uniqueMembers=\"true\"/>\n" +
                "  </Hierarchy>\n" +
                "</Dimension>",
            null);
        if (!testContext.getDialect().allowsFromQuery()) {
            return;
        }
        testContext.assertAxisReturns(
            "[Gender2].members",
            fold("[Gender2].[All Gender]\n" +
                "[Gender2].[All Gender].[F]\n" +
                "[Gender2].[All Gender].[M]"));
    }

    public void testInvalidSchemaAccess() {
        final TestContext testContext = TestContext.create(
            null, null, null, null, null,
            "<Role name=\"Role1\">\n"
                + "  <SchemaGrant access=\"invalid\"/>\n"
                + "</Role>")
            .withRole("Role1");
        testContext.assertThrows(
            "select from [Sales]",
            "In Schema: In Role: In SchemaGrant: "
                + "Value 'invalid' of attribute 'access' has illegal value 'invalid'.  "
                + "Legal values: {all, custom, none, all_dimensions}");
    }

    public void testUnionRole() {
        final TestContext testContext = TestContext.create(
            null, null, null, null, null,
            "<Role name=\"Role1\">\n"
                + "  <SchemaGrant access=\"all\"/>\n"
                + "</Role>\n"
                + "<Role name=\"Role2\">\n"
                + "  <SchemaGrant access=\"all\"/>\n"
                + "</Role>\n"
                + "<Role name=\"Role1Plus2\">\n"
                + "  <Union>\n"
                + "    <RoleUsage roleName=\"Role1\"/>\n"
                + "    <RoleUsage roleName=\"Role2\"/>\n"
                + "  </Union>\n"
                + "</Role>\n"
                + "<Role name=\"Role1Plus2Plus1\">\n"
                + "  <Union>\n"
                + "    <RoleUsage roleName=\"Role1Plus2\"/>\n"
                + "    <RoleUsage roleName=\"Role1\"/>\n"
                + "  </Union>\n"
                + "</Role>\n").withRole("Role1Plus2Plus1");
        testContext.assertQueryReturns("select from [Sales]", fold(
            "Axis #0:\n" +
                "{}\n" +
                "266,773"));
    }

    public void testUnionRoleContainsGrants() {
        final TestContext testContext = TestContext.create(
            null, null, null, null, null,
            "<Role name=\"Role1\">\n"
                + "  <SchemaGrant access=\"all\"/>\n"
                + "</Role>\n"
                + "<Role name=\"Role1Plus2\">\n"
                + "  <SchemaGrant access=\"all\"/>\n"
                + "  <Union>\n"
                + "    <RoleUsage roleName=\"Role1\"/>\n"
                + "    <RoleUsage roleName=\"Role1\"/>\n"
                + "  </Union>\n"
                + "</Role>\n").withRole("Role1Plus2");
        testContext.assertThrows(
            "select from [Sales]", "Union role must not contain grants");
    }

    public void testUnionRoleIllegalForwardRef() {
        final TestContext testContext = TestContext.create(
            null, null, null, null, null,
            "<Role name=\"Role1\">\n"
                + "  <SchemaGrant access=\"all\"/>\n"
                + "</Role>\n"
                + "<Role name=\"Role1Plus2\">\n"
                + "  <Union>\n"
                + "    <RoleUsage roleName=\"Role1\"/>\n"
                + "    <RoleUsage roleName=\"Role2\"/>\n"
                + "  </Union>\n"
                + "</Role>\n"
                + "<Role name=\"Role2\">\n"
                + "  <SchemaGrant access=\"all\"/>\n"
                + "</Role>").withRole("Role1Plus2");
        testContext.assertThrows(
            "select from [Sales]", "Unknown role 'Role2'");
    }
}

// End SchemaTest.java
