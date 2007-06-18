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
import mondrian.rolap.aggmatcher.AggTableManager;
import mondrian.util.Bug;
import mondrian.olap.MondrianException;
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
        if (Bug.Bug1735827Fixed) {
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
     * Tests two dimensions using same table (via different join paths).
     * both using a table alias.
     */
    public void testDimensionsShareJoinTable() {
        try {
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
                    null, null, null);
            // expecting a mondrian exception from this call
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
            // if we've made it here, the exception isn't being thrown
            fail();
        } catch (MondrianException e) {
            assertTrue(e.getMessage().indexOf("Duplicate table alias") >= 0);
        } catch (Throwable t) {
            fail();
        }
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
                null, null, null);
        
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
                null, null, null);
        
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
                null, null, null);
        
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
                null, null, null);
        
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
     * Test Multiple DimensionUsages on same Dimension
     * This functionality currently does not exist.
     */
    public void testMultipleDimensionUsages() {
        if (false) {
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
                null, null, null);
    
            testContext.assertQueryReturns(
                "select\n" +
                    " {[Time2].[1997]} on columns,\n" +
                    " {[Time].[1997].[Q3]} on rows\n" +
                    "From [Sales Two Dimensions]",
                fold("Axis #0:\n" +
                    "{[Warehouse].[USA]}\n" +
                    "Axis #1:\n" +
                    "{[Time2].[1997]}\n" +
                    
                    "Axis #2:\n" +
                    "{[Time].[1997].[Q3]}\n" +
                    "Row #0: UNKNOWN\n"));
        }
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
            null, null, null);

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
        TestContext testContext = TestContext.create(null,
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
            null, null, null);
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
                "WARN - Recognizer.checkUnusedColumns: Candidate aggregate table 'agg_c_10_sales_fact_1997' for fact table 'sales_fact_1997' has a column 'quarter' with unknown usage.\n" +
                "WARN - Recognizer.checkUnusedColumns: Candidate aggregate table 'agg_c_10_sales_fact_1997' for fact table 'sales_fact_1997' has a column 'the_year' with unknown usage.\n" +
                "WARN - Recognizer.checkUnusedColumns: Candidate aggregate table 'agg_c_10_sales_fact_1997' for fact table 'sales_fact_1997' has a column 'unit_sales' with unknown usage.\n" +
                "WARN - Recognizer.checkUnusedColumns: Candidate aggregate table 'agg_c_10_sales_fact_1997' for fact table 'sales_fact_1997' has a column 'month_of_year' with unknown usage.\n" +
                "WARN - Recognizer.checkUnusedColumns: Candidate aggregate table 'agg_lc_100_sales_fact_1997' for fact table 'sales_fact_1997' has a column 'unit_sales' with unknown usage.\n" +
                "WARN - Recognizer.checkUnusedColumns: Candidate aggregate table 'agg_lc_100_sales_fact_1997' for fact table 'sales_fact_1997' has a column 'customer_id' with unknown usage.\n"),
            sw.toString());
    }
}

// End SchemaTest.java
