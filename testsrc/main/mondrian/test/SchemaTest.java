/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2006-2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.test;

import org.apache.log4j.*;
import org.apache.log4j.Level;
import org.apache.log4j.varia.LevelRangeFilter;
import org.olap4j.metadata.*;

import mondrian.rolap.aggmatcher.AggTableManager;
import mondrian.olap.*;
import mondrian.util.Bug;
import mondrian.olap.Member;
import mondrian.olap.Position;
import mondrian.olap.Cube;
import mondrian.olap.Dimension;
import mondrian.olap.Hierarchy;
import mondrian.olap.Property;
import mondrian.olap.NamedSet;
import mondrian.olap.Schema;
import mondrian.spi.Dialect;

import java.io.StringWriter;
import java.util.*;
import java.sql.SQLException;

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

    /**
     * Asserts that a list of exceptions (probably from
     * {@link mondrian.olap.Schema#getWarnings()}) contains the expected
     * exception.
     *
     * @param exceptionList List of exceptions
     * @param expected Expected message
     */
    private void assertContains(
        List<Exception> exceptionList,
        String expected)
    {
        StringBuilder buf = new StringBuilder();
        for (Exception exception : exceptionList) {
            if (exception.getMessage().matches(expected)) {
                return;
            }
            if (buf.length() > 0) {
                buf.append(Util.nl);
            }
            buf.append(exception.getMessage());
        }
        fail(
            "Exception list did not contain expected exception '"
                + expected + "'. Exception list is:" + buf.toString());
    }

    // Tests follow...

    public void testSolveOrderInCalculatedMember() {
        final TestContext testContext = TestContext.createSubstitutingCube(
            "Sales",
            null,
            "<CalculatedMember\n"
            + "      name=\"QuantumProfit\"\n"
            + "      dimension=\"Measures\">\n"
            + "    <Formula>[Measures].[Store Sales] / [Measures].[Store Cost]</Formula>\n"
            + "    <CalculatedMemberProperty name=\"FORMAT_STRING\" value=\"$#,##0.00\"/>\n"
            + "  </CalculatedMember>, <CalculatedMember\n"
            + "      name=\"foo\"\n"
            + "      dimension=\"Gender\">\n"
            + "    <Formula>Sum(Gender.Members)</Formula>\n"
            + "    <CalculatedMemberProperty name=\"FORMAT_STRING\" value=\"$#,##0.00\"/>\n"
            + "    <CalculatedMemberProperty name=\"SOLVE_ORDER\" value=\'2000\'/>\n"
            + "  </CalculatedMember>");

        testContext.assertQueryReturns(
            "select {[Measures].[QuantumProfit]} on 0, {(Gender.foo)} on 1 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[QuantumProfit]}\n"
            + "Axis #2:\n"
            + "{[Gender].[foo]}\n"
            + "Row #0: $7.52\n");
    }

    public void testHierarchyDefaultMember() {
        final TestContext testContext = TestContext.createSubstitutingCube(
            "Sales",
            "  <Dimension name=\"Gender with default\" foreignKey=\"customer_id\">\n"
            + "    <Hierarchy hasAll=\"true\" "
            + "primaryKey=\"customer_id\" "
            // Define a default member's whose unique name includes the
            // 'all' member.
            + "defaultMember=\"[Gender with default].[All Gender with defaults].[M]\" >\n"
            + "      <Table name=\"customer\"/>\n"
            + "      <Level name=\"Gender\" column=\"gender\" uniqueMembers=\"true\" />\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>");
        testContext.assertQueryReturns(
            "select {[Gender with default]} on columns from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Gender with default].[All Gender with defaults].[M]}\n"
            + "Row #0: 135,215\n");
    }

    /**
     * Test case for the issue described in
     * <a href="http://forums.pentaho.org/showthread.php?p=190737">Pentaho
     * forum post 'wrong unique name for default member when hasAll=false'</a>.
     */
    public void testDefaultMemberName() {
        final TestContext testContext = TestContext.createSubstitutingCube(
            "Sales",
            "  <Dimension name=\"Product with no all\" foreignKey=\"product_id\">\n"
                + "    <Hierarchy hasAll=\"false\" primaryKey=\"product_id\" primaryKeyTable=\"product\">\n"
                + "      <Join leftKey=\"product_class_id\" rightKey=\"product_class_id\">\n"
                + "        <Table name=\"product\"/>\n"
                + "        <Table name=\"product_class\"/>\n"
                + "      </Join>\n"
                + "      <Level name=\"Product Class\" table=\"product_class\" nameColumn=\"product_subcategory\"\n"
                + "          column=\"product_class_id\" type=\"Numeric\" uniqueMembers=\"true\"/>\n"
                + "      <Level name=\"Brand Name\" table=\"product\" column=\"brand_name\" uniqueMembers=\"false\"/>\n"
                + "      <Level name=\"Product Name\" table=\"product\" column=\"product_name\"\n"
                + "          uniqueMembers=\"true\"/>\n"
                + "    </Hierarchy>\n"
                + "  </Dimension>\n");
        // note that default member name has no 'all' and has a name not an id
        testContext.assertQueryReturns(
            "select {[Product with no all]} on columns from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Product with no all].[Nuts]}\n"
            + "Row #0: 4,400\n");
    }

    public void testHierarchyAbbreviatedDefaultMember() {
        final TestContext testContext = TestContext.createSubstitutingCube(
            "Sales",
            "  <Dimension name=\"Gender with default\" foreignKey=\"customer_id\">\n"
            + "    <Hierarchy hasAll=\"true\" "
            + "primaryKey=\"customer_id\" "
            // Default member unique name does not include 'All'.
            + "defaultMember=\"[Gender with default].[F]\" >\n"
            + "      <Table name=\"customer\"/>\n"
            + "      <Level name=\"Gender\" column=\"gender\" uniqueMembers=\"true\" />\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>");
        testContext.assertQueryReturns(
            "select {[Gender with default]} on columns from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            // Note that the 'all' member is named according to the rule
            // '[<hierarchy>].[All <hierarchy>s]'.
            + "{[Gender with default].[All Gender with defaults].[F]}\n"
            + "Row #0: 131,558\n");
    }

    /**
     * Tests a measure based on 'count'.
     */
    public void testCountMeasure() {
        final TestContext testContext = TestContext.createSubstitutingCube(
            "Sales",
            null,
            "<Measure name=\"Fact Count\" aggregator=\"count\"/>\n");
        testContext.assertQueryReturns(
            "select {[Measures].[Fact Count], [Measures].[Unit Sales]} on 0,\n"
            + "[Gender].members on 1\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Fact Count]}\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Gender].[All Gender]}\n"
            + "{[Gender].[All Gender].[F]}\n"
            + "{[Gender].[All Gender].[M]}\n"
            + "Row #0: 86,837\n"
            + "Row #0: 266,773\n"
            + "Row #1: 42,831\n"
            + "Row #1: 131,558\n"
            + "Row #2: 44,006\n"
            + "Row #2: 135,215\n");
    }

    /**
     * Tests that an error occurs if a hierarchy is based on a non-existent
     * table.
     */
    public void testHierarchyTableNotFound() {
        final TestContext testContext = TestContext.createSubstitutingCube(
            "Sales",
            "<Dimension name=\"Yearly Income3\" foreignKey=\"product_id\">\n"
            + "  <Hierarchy hasAll=\"true\" primaryKey=\"customer_id\">\n"
            + "    <Table name=\"customer_not_found\"/>\n"
            + "    <Level name=\"Yearly Income\" column=\"yearly_income\" uniqueMembers=\"true\"/>\n"
            + "  </Hierarchy>\n"
            + "</Dimension>");
        // FIXME: This should validate the schema, and fail.
        testContext.assertSimpleQuery();
        // FIXME: Should give better error.
        testContext.assertQueryThrows(
            "select [Yearly Income3].Children from [Sales]",
            "Error while parsing MDX statement");
    }

    public void testPrimaryKeyTableNotFound() {
        final TestContext testContext = TestContext.createSubstitutingCube(
            "Sales",
            "<Dimension name=\"Yearly Income4\" foreignKey=\"product_id\">\n"
            + "  <Hierarchy hasAll=\"true\" primaryKey=\"customer_id\" primaryKeyTable=\"customer_not_found\">\n"
            + "    <Table name=\"customer\"/>\n"
            + "    <Level name=\"Yearly Income\" column=\"yearly_income\" uniqueMembers=\"true\"/>\n"
            + "  </Hierarchy>\n"
            + "</Dimension>");
        testContext.assertQueryThrows(
            "select from [Sales]",
            "no table 'customer_not_found' found in hierarchy [Yearly Income4]");
    }

    public void testLevelTableNotFound() {
        final TestContext testContext = TestContext.createSubstitutingCube(
            "Sales",
            "<Dimension name=\"Yearly Income5\" foreignKey=\"product_id\">\n"
            + "  <Hierarchy hasAll=\"true\" primaryKey=\"customer_id\">\n"
            + "    <Table name=\"customer\"/>\n"
            + "    <Level name=\"Yearly Income\" table=\"customer_not_found\" column=\"yearly_income\" uniqueMembers=\"true\"/>\n"
            + "  </Hierarchy>\n"
            + "</Dimension>");
        testContext.assertQueryThrows(
            "select from [Sales]",
            "Table 'customer_not_found' not found");
    }

    public void testHierarchyBadDefaultMember() {
        final TestContext testContext = TestContext.createSubstitutingCube(
            "Sales",
            "  <Dimension name=\"Gender with default\" foreignKey=\"customer_id\">\n"
            + "    <Hierarchy hasAll=\"true\" "
            + "primaryKey=\"customer_id\" "
            // Default member unique name does not include 'All'.
            + "defaultMember=\"[Gender with default].[Non].[Existent]\" >\n"
            + "      <Table name=\"customer\"/>\n"
            + "      <Level name=\"Gender\" column=\"gender\" uniqueMembers=\"true\" />\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>");
        testContext.assertQueryThrows(
            "select {[Gender with default]} on columns from [Sales]",
            "Can not find Default Member with name \"[Gender with default].[Non].[Existent]\" in Hierarchy \"Gender with default\"");
    }

    /**
     * WG: Note, this no longer throws an exception with the new RolapCubeMember
     * functionality.
     *
     * <p>Tests that an error is issued if two dimensions use the same table via
     * different drill-paths and do not use a different alias. If this error is
     * not issued, the generated SQL can be missing a join condition, as in
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-236">
     * Bug MONDRIAN-236, "Mondrian generates invalid SQL"</a>.
     */
    public void testDuplicateTableAlias() {
        final TestContext testContext = TestContext.createSubstitutingCube(
            "Sales",
            "<Dimension name=\"Yearly Income2\" foreignKey=\"product_id\">\n"
            + "  <Hierarchy hasAll=\"true\" primaryKey=\"customer_id\">\n"
            + "    <Table name=\"customer\"/>\n"
            + "    <Level name=\"Yearly Income\" column=\"yearly_income\" uniqueMembers=\"true\"/>\n"
            + "  </Hierarchy>\n"
            + "</Dimension>");

        testContext.assertQueryReturns(
            "select {[Yearly Income2]} on columns, {[Measures].[Unit Sales]} on rows from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Yearly Income2].[All Yearly Income2s]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Row #0: 266,773\n");
    }

    /**
     * This result is somewhat peculiar. If two dimensions share a foreign key,
     * what is the expected result?  Also, in this case, they share the same
     * table without an alias, and the system doesn't complain.
     */
    public void testDuplicateTableAliasSameForeignKey() {
        final TestContext testContext = TestContext.createSubstitutingCube(
            "Sales",
            "<Dimension name=\"Yearly Income2\" foreignKey=\"customer_id\">\n"
            + "  <Hierarchy hasAll=\"true\" primaryKey=\"customer_id\">\n"
            + "    <Table name=\"customer\"/>\n"
            + "    <Level name=\"Yearly Income\" column=\"yearly_income\" uniqueMembers=\"true\"/>\n"
            + "  </Hierarchy>\n"
            + "</Dimension>");
        testContext.assertQueryReturns(
            "select from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "266,773");

        // NonEmptyCrossJoin Fails
        if (false) {
            testContext.assertQueryReturns(
                "select NonEmptyCrossJoin({[Yearly Income2].[All Yearly Income2s]},{[Customers].[All Customers]}) on rows,"
                + "NON EMPTY {[Measures].[Unit Sales]} on columns"
                + " from [Sales]",
                "Axis #0:\n"
                + "{}\n"
                + "266,773");
        }
    }

    /**
     * Tests two dimensions using same table (via different join paths).
     * Without the table alias, generates SQL which is missing a join condition.
     * See {@link #testDuplicateTableAlias()}.
     */
    public void testDimensionsShareTable() {
        final TestContext testContext = TestContext.createSubstitutingCube(
            "Sales",
            "<Dimension name=\"Yearly Income2\" foreignKey=\"product_id\">\n"
            + "  <Hierarchy hasAll=\"true\" primaryKey=\"customer_id\">\n"
            + "    <Table name=\"customer\" alias=\"customerx\" />\n"
            + "    <Level name=\"Yearly Income\" column=\"yearly_income\" uniqueMembers=\"true\"/>\n"
            + "  </Hierarchy>\n"
            + "</Dimension>");

        testContext.assertQueryReturns(
            "select {[Yearly Income].[$10K - $30K]} on columns,"
            + "{[Yearly Income2].[$150K +]} on rows from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Yearly Income].[All Yearly Incomes].[$10K - $30K]}\n"
            + "Axis #2:\n"
            + "{[Yearly Income2].[All Yearly Income2s].[$150K +]}\n"
            + "Row #0: 918\n");

        testContext.assertQueryReturns(
            "select NON EMPTY {[Measures].[Unit Sales]} ON COLUMNS,\n"
            + "NON EMPTY Crossjoin({[Yearly Income].[All Yearly Incomes].Children},\n"
            + "                     [Yearly Income2].[All Yearly Income2s].Children) ON ROWS\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Yearly Income].[All Yearly Incomes].[$10K - $30K], [Yearly Income2].[All Yearly Income2s].[$10K - $30K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$10K - $30K], [Yearly Income2].[All Yearly Income2s].[$110K - $130K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$10K - $30K], [Yearly Income2].[All Yearly Income2s].[$130K - $150K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$10K - $30K], [Yearly Income2].[All Yearly Income2s].[$150K +]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$10K - $30K], [Yearly Income2].[All Yearly Income2s].[$30K - $50K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$10K - $30K], [Yearly Income2].[All Yearly Income2s].[$50K - $70K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$10K - $30K], [Yearly Income2].[All Yearly Income2s].[$70K - $90K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$10K - $30K], [Yearly Income2].[All Yearly Income2s].[$90K - $110K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$110K - $130K], [Yearly Income2].[All Yearly Income2s].[$10K - $30K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$110K - $130K], [Yearly Income2].[All Yearly Income2s].[$110K - $130K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$110K - $130K], [Yearly Income2].[All Yearly Income2s].[$130K - $150K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$110K - $130K], [Yearly Income2].[All Yearly Income2s].[$150K +]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$110K - $130K], [Yearly Income2].[All Yearly Income2s].[$30K - $50K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$110K - $130K], [Yearly Income2].[All Yearly Income2s].[$50K - $70K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$110K - $130K], [Yearly Income2].[All Yearly Income2s].[$70K - $90K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$110K - $130K], [Yearly Income2].[All Yearly Income2s].[$90K - $110K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$130K - $150K], [Yearly Income2].[All Yearly Income2s].[$10K - $30K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$130K - $150K], [Yearly Income2].[All Yearly Income2s].[$110K - $130K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$130K - $150K], [Yearly Income2].[All Yearly Income2s].[$130K - $150K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$130K - $150K], [Yearly Income2].[All Yearly Income2s].[$150K +]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$130K - $150K], [Yearly Income2].[All Yearly Income2s].[$30K - $50K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$130K - $150K], [Yearly Income2].[All Yearly Income2s].[$50K - $70K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$130K - $150K], [Yearly Income2].[All Yearly Income2s].[$70K - $90K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$130K - $150K], [Yearly Income2].[All Yearly Income2s].[$90K - $110K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$150K +], [Yearly Income2].[All Yearly Income2s].[$10K - $30K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$150K +], [Yearly Income2].[All Yearly Income2s].[$110K - $130K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$150K +], [Yearly Income2].[All Yearly Income2s].[$130K - $150K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$150K +], [Yearly Income2].[All Yearly Income2s].[$150K +]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$150K +], [Yearly Income2].[All Yearly Income2s].[$30K - $50K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$150K +], [Yearly Income2].[All Yearly Income2s].[$50K - $70K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$150K +], [Yearly Income2].[All Yearly Income2s].[$70K - $90K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$150K +], [Yearly Income2].[All Yearly Income2s].[$90K - $110K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$30K - $50K], [Yearly Income2].[All Yearly Income2s].[$10K - $30K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$30K - $50K], [Yearly Income2].[All Yearly Income2s].[$110K - $130K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$30K - $50K], [Yearly Income2].[All Yearly Income2s].[$130K - $150K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$30K - $50K], [Yearly Income2].[All Yearly Income2s].[$150K +]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$30K - $50K], [Yearly Income2].[All Yearly Income2s].[$30K - $50K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$30K - $50K], [Yearly Income2].[All Yearly Income2s].[$50K - $70K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$30K - $50K], [Yearly Income2].[All Yearly Income2s].[$70K - $90K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$30K - $50K], [Yearly Income2].[All Yearly Income2s].[$90K - $110K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$50K - $70K], [Yearly Income2].[All Yearly Income2s].[$10K - $30K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$50K - $70K], [Yearly Income2].[All Yearly Income2s].[$110K - $130K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$50K - $70K], [Yearly Income2].[All Yearly Income2s].[$130K - $150K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$50K - $70K], [Yearly Income2].[All Yearly Income2s].[$150K +]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$50K - $70K], [Yearly Income2].[All Yearly Income2s].[$30K - $50K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$50K - $70K], [Yearly Income2].[All Yearly Income2s].[$50K - $70K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$50K - $70K], [Yearly Income2].[All Yearly Income2s].[$70K - $90K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$50K - $70K], [Yearly Income2].[All Yearly Income2s].[$90K - $110K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$70K - $90K], [Yearly Income2].[All Yearly Income2s].[$10K - $30K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$70K - $90K], [Yearly Income2].[All Yearly Income2s].[$110K - $130K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$70K - $90K], [Yearly Income2].[All Yearly Income2s].[$130K - $150K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$70K - $90K], [Yearly Income2].[All Yearly Income2s].[$150K +]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$70K - $90K], [Yearly Income2].[All Yearly Income2s].[$30K - $50K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$70K - $90K], [Yearly Income2].[All Yearly Income2s].[$50K - $70K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$70K - $90K], [Yearly Income2].[All Yearly Income2s].[$70K - $90K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$70K - $90K], [Yearly Income2].[All Yearly Income2s].[$90K - $110K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$90K - $110K], [Yearly Income2].[All Yearly Income2s].[$10K - $30K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$90K - $110K], [Yearly Income2].[All Yearly Income2s].[$110K - $130K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$90K - $110K], [Yearly Income2].[All Yearly Income2s].[$130K - $150K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$90K - $110K], [Yearly Income2].[All Yearly Income2s].[$150K +]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$90K - $110K], [Yearly Income2].[All Yearly Income2s].[$30K - $50K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$90K - $110K], [Yearly Income2].[All Yearly Income2s].[$50K - $70K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$90K - $110K], [Yearly Income2].[All Yearly Income2s].[$70K - $90K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$90K - $110K], [Yearly Income2].[All Yearly Income2s].[$90K - $110K]}\n"
            + "Row #0: 12,824\n"
            + "Row #1: 2,822\n"
            + "Row #2: 2,933\n"
            + "Row #3: 918\n"
            + "Row #4: 18,381\n"
            + "Row #5: 10,436\n"
            + "Row #6: 6,777\n"
            + "Row #7: 2,859\n"
            + "Row #8: 2,432\n"
            + "Row #9: 532\n"
            + "Row #10: 566\n"
            + "Row #11: 177\n"
            + "Row #12: 3,877\n"
            + "Row #13: 2,131\n"
            + "Row #14: 1,319\n"
            + "Row #15: 527\n"
            + "Row #16: 3,331\n"
            + "Row #17: 643\n"
            + "Row #18: 703\n"
            + "Row #19: 187\n"
            + "Row #20: 4,497\n"
            + "Row #21: 2,629\n"
            + "Row #22: 1,681\n"
            + "Row #23: 721\n"
            + "Row #24: 1,123\n"
            + "Row #25: 224\n"
            + "Row #26: 257\n"
            + "Row #27: 109\n"
            + "Row #28: 1,924\n"
            + "Row #29: 1,026\n"
            + "Row #30: 675\n"
            + "Row #31: 291\n"
            + "Row #32: 19,067\n"
            + "Row #33: 4,078\n"
            + "Row #34: 4,235\n"
            + "Row #35: 1,569\n"
            + "Row #36: 28,160\n"
            + "Row #37: 15,368\n"
            + "Row #38: 10,329\n"
            + "Row #39: 4,504\n"
            + "Row #40: 9,708\n"
            + "Row #41: 2,353\n"
            + "Row #42: 2,243\n"
            + "Row #43: 748\n"
            + "Row #44: 14,469\n"
            + "Row #45: 7,966\n"
            + "Row #46: 5,272\n"
            + "Row #47: 2,208\n"
            + "Row #48: 7,320\n"
            + "Row #49: 1,630\n"
            + "Row #50: 1,602\n"
            + "Row #51: 541\n"
            + "Row #52: 10,550\n"
            + "Row #53: 5,843\n"
            + "Row #54: 3,997\n"
            + "Row #55: 1,562\n"
            + "Row #56: 2,722\n"
            + "Row #57: 597\n"
            + "Row #58: 568\n"
            + "Row #59: 193\n"
            + "Row #60: 3,800\n"
            + "Row #61: 2,192\n"
            + "Row #62: 1,324\n"
            + "Row #63: 523\n");
    }

    /**
     * Tests two dimensions using same table (via different join paths).
     * native non empty cross join sql generation returns empty query.
     * note that this works when native cross join is disabled
     */
    public void testDimensionsShareTableNativeNonEmptyCrossJoin() {
        final TestContext testContext = TestContext.createSubstitutingCube(
            "Sales",
            "<Dimension name=\"Yearly Income2\" foreignKey=\"product_id\">\n"
            + "  <Hierarchy hasAll=\"true\" primaryKey=\"customer_id\">\n"
            + "    <Table name=\"customer\" alias=\"customerx\" />\n"
            + "    <Level name=\"Yearly Income\" column=\"yearly_income\" uniqueMembers=\"true\"/>\n"
            + "  </Hierarchy>\n"
            + "</Dimension>");

        testContext.assertQueryReturns(
            "select NonEmptyCrossJoin({[Yearly Income2].[All Yearly Income2s]},{[Customers].[All Customers]}) on rows,"
            + "NON EMPTY {[Measures].[Unit Sales]} on columns"
            + " from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Yearly Income2].[All Yearly Income2s], [Customers].[All Customers]}\n"
            + "Row #0: 266,773\n");
    }

    /**
     * Tests two dimensions using same table with same foreign key
     * one table uses an alias.
     */
    public void testDimensionsShareTableSameForeignKeys() {
        final TestContext testContext = TestContext.createSubstitutingCube(
            "Sales",
            "<Dimension name=\"Yearly Income2\" foreignKey=\"customer_id\">\n"
            + "  <Hierarchy hasAll=\"true\" primaryKey=\"customer_id\">\n"
            + "    <Table name=\"customer\" alias=\"customerx\" />\n"
            + "    <Level name=\"Yearly Income\" column=\"yearly_income\" uniqueMembers=\"true\"/>\n"
            + "  </Hierarchy>\n"
            + "</Dimension>");

        testContext.assertQueryReturns(
            "select {[Yearly Income].[$10K - $30K]} on columns,"
            + "{[Yearly Income2].[$150K +]} on rows from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Yearly Income].[All Yearly Incomes].[$10K - $30K]}\n"
            + "Axis #2:\n"
            + "{[Yearly Income2].[All Yearly Income2s].[$150K +]}\n"
            + "Row #0: \n");

        testContext.assertQueryReturns(
            "select NON EMPTY {[Measures].[Unit Sales]} ON COLUMNS,\n"
            + "NON EMPTY Crossjoin({[Yearly Income].[All Yearly Incomes].Children},\n"
            + "                     [Yearly Income2].[All Yearly Income2s].Children) ON ROWS\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Yearly Income].[All Yearly Incomes].[$10K - $30K], [Yearly Income2].[All Yearly Income2s].[$10K - $30K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$110K - $130K], [Yearly Income2].[All Yearly Income2s].[$110K - $130K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$130K - $150K], [Yearly Income2].[All Yearly Income2s].[$130K - $150K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$150K +], [Yearly Income2].[All Yearly Income2s].[$150K +]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$30K - $50K], [Yearly Income2].[All Yearly Income2s].[$30K - $50K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$50K - $70K], [Yearly Income2].[All Yearly Income2s].[$50K - $70K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$70K - $90K], [Yearly Income2].[All Yearly Income2s].[$70K - $90K]}\n"
            + "{[Yearly Income].[All Yearly Incomes].[$90K - $110K], [Yearly Income2].[All Yearly Income2s].[$90K - $110K]}\n"
            + "Row #0: 57,950\n"
            + "Row #1: 11,561\n"
            + "Row #2: 14,392\n"
            + "Row #3: 5,629\n"
            + "Row #4: 87,310\n"
            + "Row #5: 44,967\n"
            + "Row #6: 33,045\n"
            + "Row #7: 11,919\n");
    }

    /**
     * test hierarchy with completely different join path to fact table than
     * first hierarchy. tables are auto-aliased as necessary to guarantee
     * unique joins to the fact table.
     */
    public void testSnowflakeHierarchyValidationNotNeeded() {
        // this test breaks when using aggregates at the moment
        // due to a known limitation
        if ((MondrianProperties.instance().ReadAggregates.get()
             || MondrianProperties.instance().UseAggregates.get())
            && !Bug.BugMondrian361Fixed)
        {
            return;
        }

        final TestContext testContext = TestContext.create(
            null,
            "<Cube name=\"AliasedDimensionsTesting\" defaultMeasure=\"Supply Time\">\n"
            + "  <Table name=\"sales_fact_1997\"/>\n"
            + "  <Dimension name=\"Store\" foreignKey=\"store_id\">\n"
            + "    <Hierarchy hasAll=\"true\" primaryKeyTable=\"store\" primaryKey=\"store_id\">\n"
            + "      <Join leftKey=\"region_id\" rightKey=\"region_id\">\n"
            + "        <Table name=\"store\"/>\n"
            + "        <Join leftKey=\"sales_district_id\" rightKey=\"promotion_id\">\n"
            + "          <Table name=\"region\"/>\n"
            + "          <Table name=\"promotion\"/>\n"
            + "        </Join>\n"
            + "      </Join>\n"
            + "      <Level name=\"Store Country\" table=\"store\" column=\"store_country\"/>\n"
            + "      <Level name=\"Store Region\" table=\"region\" column=\"sales_region\" />\n"
            + "      <Level name=\"Store Name\" table=\"store\" column=\"store_name\" />\n"
            + "    </Hierarchy>\n"
            + "    <Hierarchy name=\"MyHierarchy\" hasAll=\"true\" primaryKeyTable=\"customer\" primaryKey=\"customer_id\">\n"
            + "      <Join leftKey=\"customer_region_id\" rightKey=\"region_id\">\n"
            + "        <Table name=\"customer\"/>\n"
            + "        <Table name=\"region\"/>\n"
            + "      </Join>\n"
            + "      <Level name=\"Country\" table=\"customer\" column=\"country\" uniqueMembers=\"true\"/>\n"
            + "      <Level name=\"Region\" table=\"region\" column=\"sales_region\" uniqueMembers=\"true\"/>\n"
            + "      <Level name=\"City\" table=\"customer\" column=\"city\" uniqueMembers=\"false\"/>\n"
            + "      <Level name=\"Name\" table=\"customer\" column=\"customer_id\" type=\"Numeric\" uniqueMembers=\"true\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Dimension name=\"Customers\" foreignKey=\"customer_id\">\n"
            + "    <Hierarchy hasAll=\"true\" allMemberName=\"All Customers\" primaryKeyTable=\"customer\" primaryKey=\"customer_id\">\n"
            + "      <Join leftKey=\"customer_region_id\" rightKey=\"region_id\">\n"
            + "        <Table name=\"customer\"/>\n"
            + "        <Table name=\"region\"/>\n"
            + "      </Join>\n"
            + "      <Level name=\"Country\" table=\"customer\" column=\"country\" uniqueMembers=\"true\"/>\n"
            + "      <Level name=\"Region\" table=\"region\" column=\"sales_region\" uniqueMembers=\"true\"/>\n"
            + "      <Level name=\"City\" table=\"customer\" column=\"city\" uniqueMembers=\"false\"/>\n"
            + "      <Level name=\"Name\" table=\"customer\" column=\"customer_id\" type=\"Numeric\" uniqueMembers=\"true\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "<Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\" formatString=\"Standard\"/>\n"
            + "</Cube>",
            null,
            null,
            null,
            null);

        testContext.assertQueryReturns(
            "select  {[Store.MyHierarchy].[Mexico]} on rows,"
            + "{[Customers].[USA].[South West]} on columns"
            + " from "
            + "AliasedDimensionsTesting",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Customers].[All Customers].[USA].[South West]}\n"
            + "Axis #2:\n"
            + "{[Store].[MyHierarchy].[All MyHierarchys].[Mexico]}\n"
            + "Row #0: 51,298\n");
    }

    /**
     * test hierarchy with slightly different join path to fact table than
     * first hierarchy. tables from first and second hierarchy should contain
     * the same join aliases to the fact table.
     */
    public void testSnowflakeHierarchyValidationNotNeeded2() {
        final TestContext testContext = TestContext.create(
            null,
            "<Cube name=\"AliasedDimensionsTesting\" defaultMeasure=\"Supply Time\">\n"
            + "  <Table name=\"sales_fact_1997\"/>\n"
            + "  <Dimension name=\"Store\" foreignKey=\"store_id\">\n"
            + "    <Hierarchy hasAll=\"true\" primaryKeyTable=\"store\" primaryKey=\"store_id\">\n"
            + "      <Join leftKey=\"region_id\" rightKey=\"region_id\">\n"
            + "        <Table name=\"store\"/>\n"
            + "        <Join leftKey=\"sales_district_id\" rightKey=\"promotion_id\">\n"
            + "          <Table name=\"region\"/>\n"
            + "          <Table name=\"promotion\"/>\n"
            + "        </Join>\n"
            + "      </Join>\n"
            + "      <Level name=\"Store Country\" table=\"store\" column=\"store_country\"/>\n"
            + "      <Level name=\"Store Region\" table=\"region\" column=\"sales_region\" />\n"
            + "      <Level name=\"Store Name\" table=\"store\" column=\"store_name\" />\n"
            + "    </Hierarchy>\n"
            + "    <Hierarchy name=\"MyHierarchy\" hasAll=\"true\" primaryKeyTable=\"store\" primaryKey=\"store_id\">\n"
            + "      <Join leftKey=\"region_id\" rightKey=\"region_id\">\n"
            + "        <Table name=\"store\"/>\n"
            + "        <Table name=\"region\"/>\n"
            + "      </Join>\n"
            + "      <Level name=\"Store Country\" table=\"store\" column=\"store_country\"/>\n"
            + "      <Level name=\"Store Region\" table=\"region\" column=\"sales_region\" />\n"
            + "      <Level name=\"Store Name\" table=\"store\" column=\"store_name\" />\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Dimension name=\"Customers\" foreignKey=\"customer_id\">\n"
            + "    <Hierarchy hasAll=\"true\" allMemberName=\"All Customers\" primaryKeyTable=\"customer\" primaryKey=\"customer_id\">\n"
            + "    <Join leftKey=\"customer_region_id\" rightKey=\"region_id\">\n"
            + "      <Table name=\"customer\"/>\n"
            + "      <Table name=\"region\"/>\n"
            + "    </Join>\n"
            + "    <Level name=\"Country\" table=\"customer\" column=\"country\" uniqueMembers=\"true\"/>\n"
            + "    <Level name=\"Region\" table=\"region\" column=\"sales_region\" uniqueMembers=\"true\"/>\n"
            + "    <Level name=\"City\" table=\"customer\" column=\"city\" uniqueMembers=\"false\"/>\n"
            + "    <Level name=\"Name\" table=\"customer\" column=\"customer_id\" type=\"Numeric\" uniqueMembers=\"true\"/>\n"
            + "  </Hierarchy>\n"
            + "</Dimension>\n"
            + "<Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\" formatString=\"Standard\"/>\n"
            + "</Cube>",
            null,
            null,
            null,
            null);

        testContext.assertQueryReturns(
            "select  {[Store.MyHierarchy].[USA].[South West]} on rows,"
            + "{[Customers].[USA].[South West]} on columns"
            + " from "
            + "AliasedDimensionsTesting",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Customers].[All Customers].[USA].[South West]}\n"
            + "Axis #2:\n"
            + "{[Store].[MyHierarchy].[All MyHierarchys].[USA].[South West]}\n"
            + "Row #0: 72,631\n");
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
            "<Cube name=\"AliasedDimensionsTesting\" defaultMeasure=\"Supply Time\">\n"
            + "  <Table name=\"sales_fact_1997\"/>\n"
            + "<Dimension name=\"Store\" foreignKey=\"store_id\">\n"

            + "<Hierarchy hasAll=\"true\" primaryKeyTable=\"store\" primaryKey=\"store_id\">\n"
            + "    <Join leftKey=\"region_id\" rightKey=\"region_id\">\n"
            + "      <Table name=\"store\"/>\n"
            + "      <Table name=\"region\"/>\n"
            + "    </Join>\n"
            + " <Level name=\"Store Country\" table=\"store\"  column=\"store_country\" uniqueMembers=\"true\"/>\n"
            + " <Level name=\"Store Region\"  table=\"region\" column=\"sales_region\"  uniqueMembers=\"true\"/>\n"
            + " <Level name=\"Store Name\"    table=\"store\"  column=\"store_name\"    uniqueMembers=\"true\"/>\n"
            + "</Hierarchy>\n"
            + "</Dimension>\n"
            + "<Dimension name=\"Customers\" foreignKey=\"customer_id\">\n"
            + "<Hierarchy hasAll=\"true\" allMemberName=\"All Customers\" primaryKeyTable=\"customer\" primaryKey=\"customer_id\">\n"
            + "    <Join leftKey=\"customer_region_id\" rightKey=\"region_id\">\n"
            + "      <Table name=\"customer\"/>\n"
            + "      <Table name=\"region\"/>\n"
            + "    </Join>\n"
            + "  <Level name=\"Country\" table=\"customer\" column=\"country\"                      uniqueMembers=\"true\"/>\n"
            + "  <Level name=\"Region\"  table=\"region\"   column=\"sales_region\"                 uniqueMembers=\"true\"/>\n"
            + "  <Level name=\"City\"    table=\"customer\" column=\"city\"                         uniqueMembers=\"false\"/>\n"
            + "  <Level name=\"Name\"    table=\"customer\" column=\"customer_id\" type=\"Numeric\" uniqueMembers=\"true\"/>\n"
            + "</Hierarchy>\n"
            + "</Dimension>\n"
            + "<Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\" formatString=\"Standard\"/>\n"
            + "<Measure name=\"Store Sales\" column=\"store_sales\" aggregator=\"sum\" formatString=\"#,###.00\"/>\n"
            + "</Cube>",
            null,
            null,
            null,
            null);

        testContext.assertQueryReturns(
            "select  {[Store].[USA].[South West]} on rows,"
            + "{[Customers].[USA].[South West]} on columns"
            + " from "
            + "AliasedDimensionsTesting",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Customers].[All Customers].[USA].[South West]}\n"
            + "Axis #2:\n"
            + "{[Store].[All Stores].[USA].[South West]}\n"
            + "Row #0: 72,631\n");
    }

    /**
     * Tests two dimensions using same table (via different join paths).
     * both using a table alias.
     */
    public void testDimensionsShareJoinTableOneAlias() {
        final TestContext testContext = TestContext.create(
            null,
            "<Cube name=\"AliasedDimensionsTesting\" defaultMeasure=\"Supply Time\">\n"
            + "  <Table name=\"sales_fact_1997\"/>\n"
            + "<Dimension name=\"Store\" foreignKey=\"store_id\">\n"
            + "<Hierarchy hasAll=\"true\" primaryKeyTable=\"store\" primaryKey=\"store_id\">\n"
            + "    <Join leftKey=\"region_id\" rightKey=\"region_id\">\n"
            + "      <Table name=\"store\"/>\n"
            + "      <Table name=\"region\"/>\n"
            + "    </Join>\n"
            + " <Level name=\"Store Country\" table=\"store\"  column=\"store_country\" uniqueMembers=\"true\"/>\n"
            + " <Level name=\"Store Region\"  table=\"region\" column=\"sales_region\"  uniqueMembers=\"true\"/>\n"
            + " <Level name=\"Store Name\"    table=\"store\"  column=\"store_name\"    uniqueMembers=\"true\"/>\n"
            + "</Hierarchy>\n"
            + "</Dimension>\n"
            + "<Dimension name=\"Customers\" foreignKey=\"customer_id\">\n"
            + "<Hierarchy hasAll=\"true\" allMemberName=\"All Customers\" primaryKeyTable=\"customer\" primaryKey=\"customer_id\">\n"
            + "    <Join leftKey=\"customer_region_id\" rightKey=\"region_id\">\n"
            + "      <Table name=\"customer\"/>\n"
            + "      <Table name=\"region\" alias=\"customer_region\"/>\n"
            + "    </Join>\n"
            + "  <Level name=\"Country\" table=\"customer\" column=\"country\"                      uniqueMembers=\"true\"/>\n"
            + "  <Level name=\"Region\"  table=\"customer_region\"   column=\"sales_region\"                 uniqueMembers=\"true\"/>\n"
            + "  <Level name=\"City\"    table=\"customer\" column=\"city\"                         uniqueMembers=\"false\"/>\n"
            + "  <Level name=\"Name\"    table=\"customer\" column=\"customer_id\" type=\"Numeric\" uniqueMembers=\"true\"/>\n"
            + "</Hierarchy>\n"
            + "</Dimension>\n"
            + "<Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\" formatString=\"Standard\"/>\n"
            + "<Measure name=\"Store Sales\" column=\"store_sales\" aggregator=\"sum\" formatString=\"#,###.00\"/>\n"
            + "</Cube>",
            null,
            null,
            null,
            null);

        testContext.assertQueryReturns(
            "select  {[Store].[USA].[South West]} on rows,"
            + "{[Customers].[USA].[South West]} on columns"
            + " from "
            + "AliasedDimensionsTesting",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Customers].[All Customers].[USA].[South West]}\n"
            + "Axis #2:\n"
            + "{[Store].[All Stores].[USA].[South West]}\n"
            + "Row #0: 72,631\n");
    }

    /**
     * Tests two dimensions using same table (via different join paths).
     * both using a table alias.
     */
    public void testDimensionsShareJoinTableTwoAliases() {
        final TestContext testContext = TestContext.create(
            null,
            "<Cube name=\"AliasedDimensionsTesting\" defaultMeasure=\"Supply Time\">\n"
            + "  <Table name=\"sales_fact_1997\"/>\n"
            + "<Dimension name=\"Store\" foreignKey=\"store_id\">\n"
            + "<Hierarchy hasAll=\"true\" primaryKeyTable=\"store\" primaryKey=\"store_id\">\n"
            + "    <Join leftKey=\"region_id\" rightKey=\"region_id\">\n"
            + "      <Table name=\"store\"/>\n"
            + "      <Table name=\"region\" alias=\"store_region\"/>\n"
            + "    </Join>\n"
            + " <Level name=\"Store Country\" table=\"store\"  column=\"store_country\" uniqueMembers=\"true\"/>\n"
            + " <Level name=\"Store Region\"  table=\"store_region\" column=\"sales_region\"  uniqueMembers=\"true\"/>\n"
            + " <Level name=\"Store Name\"    table=\"store\"  column=\"store_name\"    uniqueMembers=\"true\"/>\n"
            + "</Hierarchy>\n"
            + "</Dimension>\n"
            + "<Dimension name=\"Customers\" foreignKey=\"customer_id\">\n"
            + "<Hierarchy hasAll=\"true\" allMemberName=\"All Customers\" primaryKeyTable=\"customer\" primaryKey=\"customer_id\">\n"
            + "    <Join leftKey=\"customer_region_id\" rightKey=\"region_id\">\n"
            + "      <Table name=\"customer\"/>\n"
            + "      <Table name=\"region\" alias=\"customer_region\"/>\n"
            + "    </Join>\n"
            + "  <Level name=\"Country\" table=\"customer\" column=\"country\"                      uniqueMembers=\"true\"/>\n"
            + "  <Level name=\"Region\"  table=\"customer_region\"   column=\"sales_region\"                 uniqueMembers=\"true\"/>\n"
            + "  <Level name=\"City\"    table=\"customer\" column=\"city\"                         uniqueMembers=\"false\"/>\n"
            + "  <Level name=\"Name\"    table=\"customer\" column=\"customer_id\" type=\"Numeric\" uniqueMembers=\"true\"/>\n"
            + "</Hierarchy>\n"
            + "</Dimension>\n"
            + "<Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\" formatString=\"Standard\"/>\n"
            + "<Measure name=\"Store Sales\" column=\"store_sales\" aggregator=\"sum\" formatString=\"#,###.00\"/>\n"
            + "</Cube>",
            null,
            null,
            null,
            null);

        testContext.assertQueryReturns(
            "select  {[Store].[USA].[South West]} on rows,"
            + "{[Customers].[USA].[South West]} on columns"
            + " from "
            + "AliasedDimensionsTesting",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Customers].[All Customers].[USA].[South West]}\n"
            + "Axis #2:\n"
            + "{[Store].[All Stores].[USA].[South West]}\n"
            + "Row #0: 72,631\n");
    }

    /**
     * Tests two dimensions using same table (via different join paths).
     * both using a table alias.
     */
    public void testTwoAliasesDimensionsShareTable() {
        final TestContext testContext = TestContext.create(
            null,
            "<Cube name=\"AliasedDimensionsTesting\" defaultMeasure=\"Supply Time\">\n"
            + "  <Table name=\"inventory_fact_1997\"/>\n"
            + "  <Dimension name=\"StoreA\" foreignKey=\"store_id\">"
            + "    <Hierarchy hasAll=\"true\" primaryKey=\"store_id\">"
            + "      <Table name=\"store\" alias=\"storea\"/>"
            + "      <Level name=\"Store Country\" column=\"store_country\" uniqueMembers=\"true\"/>"
            + "      <Level name=\"Store Name\"  column=\"store_name\" uniqueMembers=\"true\"/>"
            + "    </Hierarchy>"
            + "  </Dimension>"

            + "  <Dimension name=\"StoreB\" foreignKey=\"warehouse_id\">"
            + "    <Hierarchy hasAll=\"true\" primaryKey=\"store_id\">"
            + "      <Table name=\"store\"  alias=\"storeb\"/>"
            + "      <Level name=\"Store Country\" column=\"store_country\" uniqueMembers=\"true\"/>"
            + "      <Level name=\"Store Name\" column=\"store_name\" uniqueMembers=\"true\"/>"
            + "    </Hierarchy>"
            + "  </Dimension>"
            + "  <Measure name=\"Store Invoice\" column=\"store_invoice\" "
            + "aggregator=\"sum\"/>\n"
            + "  <Measure name=\"Supply Time\" column=\"supply_time\" "
            + "aggregator=\"sum\"/>\n"
            + "  <Measure name=\"Warehouse Cost\" column=\"warehouse_cost\" "
            + "aggregator=\"sum\"/>\n"
            + "</Cube>",
            null,
            null,
            null,
            null);

        testContext.assertQueryReturns(
            "select {[StoreA].[USA]} on rows,"
            + "{[StoreB].[USA]} on columns"
            + " from "
            + "AliasedDimensionsTesting",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[StoreB].[All StoreBs].[USA]}\n"
            + "Axis #2:\n"
            + "{[StoreA].[All StoreAs].[USA]}\n"
            + "Row #0: 10,425\n");
    }

    /**
     * Tests two dimensions using same table with same foreign key.
     * both using a table alias.
     */
    public void testTwoAliasesDimensionsShareTableSameForeignKeys() {
        final TestContext testContext = TestContext.create(
            null,
            "<Cube name=\"AliasedDimensionsTesting\" defaultMeasure=\"Supply Time\">\n"
            + "  <Table name=\"inventory_fact_1997\"/>\n"
            + "  <Dimension name=\"StoreA\" foreignKey=\"store_id\">"
            + "    <Hierarchy hasAll=\"true\" primaryKey=\"store_id\">"
            + "      <Table name=\"store\" alias=\"storea\"/>"
            + "      <Level name=\"Store Country\" column=\"store_country\" uniqueMembers=\"true\"/>"
            + "      <Level name=\"Store Name\" column=\"store_name\" uniqueMembers=\"true\"/>"
            + "    </Hierarchy>"
            + "  </Dimension>"

            + "  <Dimension name=\"StoreB\" foreignKey=\"store_id\">"
            + "    <Hierarchy hasAll=\"true\" primaryKey=\"store_id\">"
            + "      <Table name=\"store\"  alias=\"storeb\"/>"
            + "      <Level name=\"Store Country\" column=\"store_country\" uniqueMembers=\"true\"/>"
            + "      <Level name=\"Store Name\" column=\"store_name\" uniqueMembers=\"true\"/>"
            + "    </Hierarchy>"
            + "  </Dimension>"
            + "  <Measure name=\"Store Invoice\" column=\"store_invoice\" "
            + "aggregator=\"sum\"/>\n"
            + "  <Measure name=\"Supply Time\" column=\"supply_time\" "
            + "aggregator=\"sum\"/>\n"
            + "  <Measure name=\"Warehouse Cost\" column=\"warehouse_cost\" "
            + "aggregator=\"sum\"/>\n"
            + "</Cube>",
            null,
            null,
            null,
            null);

        testContext.assertQueryReturns(
            "select {[StoreA].[USA]} on rows,"
            + "{[StoreB].[USA]} on columns"
            + " from "
            + "AliasedDimensionsTesting",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[StoreB].[All StoreBs].[USA]}\n"
            + "Axis #2:\n"
            + "{[StoreA].[All StoreAs].[USA]}\n"
            + "Row #0: 10,425\n");
    }

    /**
     * Test Multiple DimensionUsages on same Dimension.
     * Alias the fact table to avoid issues with aggregation rules
     * and multiple column names
     */
    public void testMultipleDimensionUsages() {
        TestContext testContext = TestContext.create(
            null,

            "<Cube name=\"Sales Two Dimensions\">\n"
            + "  <Table name=\"sales_fact_1997\" alias=\"sales_fact_1997_mdu\"/>\n"
            + "  <DimensionUsage name=\"Time\" source=\"Time\" foreignKey=\"time_id\"/>\n"
            + "  <DimensionUsage name=\"Time2\" source=\"Time\" foreignKey=\"product_id\"/>\n"
            + "  <DimensionUsage name=\"Store\" source=\"Store\" foreignKey=\"store_id\"/>\n"
            + "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\" "
            + "   formatString=\"Standard\"/>\n"
            + "  <Measure name=\"Store Cost\" column=\"store_cost\" aggregator=\"sum\""
            + "   formatString=\"#,###.00\"/>\n"
            + "</Cube>", null, null, null, null);

        testContext.assertQueryReturns(
            "select\n"
            + " {[Time2].[1997]} on columns,\n"
            + " {[Time].[1997].[Q3]} on rows\n"
            + "From [Sales Two Dimensions]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + (MondrianProperties.instance().SsasCompatibleNaming.get()
                ? "{[Time2].[Time].[1997]}\n"
                : "{[Time2].[1997]}\n")
            + "Axis #2:\n"
            + "{[Time].[1997].[Q3]}\n"
            + "Row #0: 16,266\n");
    }

    /**
     * Test Multiple DimensionUsages on same Dimension.
     * Alias the fact table to avoid issues with aggregation rules
     * and multiple column names
     */
    public void testMultipleDimensionHierarchyCaptionUsages() {
        TestContext testContext = TestContext.create(
            null,

            "<Cube name=\"Sales Two Dimensions\">\n"
            + "  <Table name=\"sales_fact_1997\" alias=\"sales_fact_1997_mdu\"/>\n"
            + "  <DimensionUsage name=\"Time\" caption=\"TimeOne\" source=\"Time\" foreignKey=\"time_id\"/>\n"
            + "  <DimensionUsage name=\"Time2\" caption=\"TimeTwo\" source=\"Time\" foreignKey=\"product_id\"/>\n"
            + "  <DimensionUsage name=\"Store\" source=\"Store\" foreignKey=\"store_id\"/>\n"
            + "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\" "
            + "   formatString=\"Standard\"/>\n"
            + "  <Measure name=\"Store Cost\" column=\"store_cost\" aggregator=\"sum\""
            + "   formatString=\"#,###.00\"/>\n"
            + "</Cube>", null, null, null, null);

        String query =
            "select\n"
            + " {[Time2].[1997]} on columns,\n"
            + " {[Time].[1997].[Q3]} on rows\n"
            + "From [Sales Two Dimensions]";

        Result result = testContext.executeQuery(query);

        // Time2.1997 Member
        Member member1 =
            result.getAxes()[0].getPositions().iterator().next().iterator()
                .next();

        // NOTE: The caption is modified at the dimension, not the hierarchy
        assertEquals("TimeTwo", member1.getLevel().getDimension().getCaption());

        Member member2 =
            result.getAxes()[1].getPositions().iterator().next().iterator()
                .next();
        assertEquals("TimeOne", member2.getLevel().getDimension().getCaption());
    }


    /**
     * This test verifies that the createDimension() API call is working
     * correctly.
     */
    public void testDimensionCreation() {
        TestContext testContext = TestContext.create(
            null,

            "<Cube name=\"Sales Create Dimension\">\n"
            + "  <Table name=\"sales_fact_1997\"/>\n"
            + "  <DimensionUsage name=\"Store\" source=\"Store\" foreignKey=\"store_id\"/>\n"
            + "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\" "
            + "   formatString=\"Standard\"/>\n"
            + "  <Measure name=\"Store Cost\" column=\"store_cost\" aggregator=\"sum\""
            + "   formatString=\"#,###.00\"/>\n"
            + "</Cube>", null, null, null, null);
        Cube cube = testContext.getConnection().getSchema().lookupCube(
            "Sales Create Dimension", true);

        testContext.assertQueryReturns(
            "select\n"
            + "NON EMPTY {[Store].[All Stores].children} on columns \n"
            + "From [Sales Create Dimension]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[All Stores].[USA]}\n"
            + "Row #0: 266,773\n");

        String dimension =
            "<DimensionUsage name=\"Time\" source=\"Time\" foreignKey=\"time_id\"/>";
        testContext.getConnection().getSchema().createDimension(
            cube, dimension);

        testContext.assertQueryReturns(
            "select\n"
            + "NON EMPTY {[Store].[All Stores].children} on columns, \n"
            + "{[Time].[1997].[Q1]} on rows \n"
            + "From [Sales Create Dimension]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[All Stores].[USA]}\n"
            + "Axis #2:\n"
            + "{[Time].[1997].[Q1]}\n"
            + "Row #0: 66,291\n");
    }

    /**
     * Test DimensionUsage level attribute
     */
    public void testDimensionUsageLevel() {
        TestContext testContext = TestContext.create(
            null,

            "<Cube name=\"Customer Usage Level\">\n"
            + "  <Table name=\"customer\"/>\n"
            // + alias=\"sales_fact_1997_multi\"/>\n"
            + "  <DimensionUsage name=\"Store\" source=\"Store\" level=\"Store State\" foreignKey=\"state_province\"/>\n"
            + "  <Measure name=\"Cars\" column=\"num_cars_owned\" aggregator=\"sum\"/>\n"
            + "  <Measure name=\"Children\" column=\"total_children\" aggregator=\"sum\"/>\n"
            + "</Cube>", null, null, null, null);

        testContext.assertQueryReturns(
            "select\n"
            + " {[Store].[Store State].members} on columns \n"
            + "From [Customer Usage Level]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[All Stores].[Canada].[BC]}\n"
            + "{[Store].[All Stores].[Mexico].[DF]}\n"
            + "{[Store].[All Stores].[Mexico].[Guerrero]}\n"
            + "{[Store].[All Stores].[Mexico].[Jalisco]}\n"
            + "{[Store].[All Stores].[Mexico].[Veracruz]}\n"
            + "{[Store].[All Stores].[Mexico].[Yucatan]}\n"
            + "{[Store].[All Stores].[Mexico].[Zacatecas]}\n"
            + "{[Store].[All Stores].[USA].[CA]}\n"
            + "{[Store].[All Stores].[USA].[OR]}\n"
            + "{[Store].[All Stores].[USA].[WA]}\n"
            + "Row #0: 7,700\n"
            + "Row #0: 1,492\n"
            + "Row #0: 228\n"
            + "Row #0: 206\n"
            + "Row #0: 195\n"
            + "Row #0: 229\n"
            + "Row #0: 1,209\n"
            + "Row #0: 46,965\n"
            + "Row #0: 4,686\n"
            + "Row #0: 32,767\n");

        // BC.children should return an empty list, considering that we've
        // joined Store at the State level.
        if (false) {
            testContext.assertQueryReturns(
                "select\n"
                + " {[Store].[All Stores].[Canada].[BC].children} on columns \n"
                + "From [Customer Usage Level]",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n");
        }
    }

    /**
     * Test to verify naming of all member with
     * dimension usage name is different then source name
     */
    public void testAllMemberMultipleDimensionUsages() {
        TestContext testContext = TestContext.create(
            null,
            "<Cube name=\"Sales Two Sales Dimensions\">\n"
            + "  <Table name=\"sales_fact_1997\"/>\n"
            + "  <DimensionUsage name=\"Store\" caption=\"First Store\" source=\"Store\" foreignKey=\"store_id\"/>\n"
            + "  <DimensionUsage name=\"Store2\" caption=\"Second Store\" source=\"Store\" foreignKey=\"product_id\"/>\n"
            + "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\" "
            + "   formatString=\"Standard\"/>\n"
            + "  <Measure name=\"Store Cost\" column=\"store_cost\" aggregator=\"sum\""
            + "   formatString=\"#,###.00\"/>\n"
            + "</Cube>",
            null,
            null,
            null,
            null);

        // If SsasCompatibleNaming (the new behavior), the usages of the
        // [Store] dimension create dimensions called [Store]
        // and [Store2], each with a hierarchy called [Store].
        // Therefore Store2's all member is [Store2].[Store].[All Stores],
        // or [Store2].[All Stores] for short.
        //
        // Under the old behavior, the member is called [Store2].[All Store2s].
        final String store2AllMember =
            MondrianProperties.instance().SsasCompatibleNaming.get()
                ? "[Store2].[All Stores]"
                : "[Store2].[All Store2s]";
        testContext.assertQueryReturns(
            "select\n"
            + " {[Store].[Store].[All Stores]} on columns,\n"
            + " {" + store2AllMember + "} on rows\n"
            + "From [Sales Two Sales Dimensions]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[All Stores]}\n"
            + "Axis #2:\n"
            + "{[Store2].[Store].[All Stores]}\n"
            + "Row #0: 266,773\n");

        final Result result = testContext.executeQuery(
            "select ([Store].[All Stores], " + store2AllMember + ") on 0\n"
            + "from [Sales Two Sales Dimensions]");
        final Axis axis = result.getAxes()[0];
        final Position position = axis.getPositions().get(0);
        assertEquals(
            "First Store", position.get(0).getDimension().getCaption());
        assertEquals(
            "Second Store", position.get(1).getDimension().getCaption());
    }

    /**
     * This test displays an informative error message if someone uses
     * an unaliased name instead of an aliased name
     */
    public void testNonAliasedDimensionUsage() {
        TestContext testContext = TestContext.create(
            null,

            "<Cube name=\"Sales Two Dimensions\">\n"
            + "  <Table name=\"sales_fact_1997\"/>\n"
            + "  <DimensionUsage name=\"Time2\" source=\"Time\" foreignKey=\"time_id\"/>\n"
            + "  <DimensionUsage name=\"Store\" source=\"Store\" foreignKey=\"store_id\"/>\n"
            + "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\" "
            + "   formatString=\"Standard\"/>\n"
            + "  <Measure name=\"Store Cost\" column=\"store_cost\" aggregator=\"sum\""
            + "   formatString=\"#,###.00\"/>\n"
            + "</Cube>", null, null, null, null);

        final String query = "select\n"
                             + " {[Time].[1997]} on columns \n"
                             + "From [Sales Two Dimensions]";
        if (!MondrianProperties.instance().SsasCompatibleNaming.get()) {
            testContext.assertQueryThrows(
                query,
                "In cube \"Sales Two Dimensions\" use of unaliased Dimension name \"[Time]\" rather than the alias name \"Time2\"");
        } else {
            // In new behavior, resolves to the hierarchy name [Time] even if
            // not qualified by dimension name [Time2].
            testContext.assertQueryReturns(
                query,
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Time2].[Time].[1997]}\n"
                + "Row #0: 266,773\n");
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
            "<Cube name=\"Warehouse (based on view)\">\n"
            + "  <View alias=\"FACT\">\n"
            + "    <SQL dialect=\"generic\">\n"
            + "     <![CDATA[select * from \"inventory_fact_1997\" as \"FOOBAR\"]]>\n"
            + "    </SQL>\n"
            + "    <SQL dialect=\"oracle\">\n"
            + "     <![CDATA[select * from \"inventory_fact_1997\" \"FOOBAR\"]]>\n"
            + "    </SQL>\n"
            + "    <SQL dialect=\"mysql\">\n"
            + "     <![CDATA[select * from `inventory_fact_1997` as `FOOBAR`]]>\n"
            + "    </SQL>\n"
            + "    <SQL dialect=\"infobright\">\n"
            + "     <![CDATA[select * from `inventory_fact_1997` as `FOOBAR`]]>\n"
            + "    </SQL>\n"
            + "  </View>\n"
            + "  <DimensionUsage name=\"Time\" source=\"Time\" foreignKey=\"time_id\"/>\n"
            + "  <DimensionUsage name=\"Product\" source=\"Product\" foreignKey=\"product_id\"/>\n"
            + "  <DimensionUsage name=\"Store\" source=\"Store\" foreignKey=\"store_id\"/>\n"
            + "  <Dimension name=\"Warehouse\" foreignKey=\"warehouse_id\">\n"
            + "    <Hierarchy hasAll=\"false\" defaultMember=\"[USA]\" primaryKey=\"warehouse_id\"> \n"
            + "      <Table name=\"warehouse\"/>\n"
            + "      <Level name=\"Country\" column=\"warehouse_country\" uniqueMembers=\"true\"/>\n"
            + "      <Level name=\"State Province\" column=\"warehouse_state_province\"\n"
            + "          uniqueMembers=\"true\"/>\n"
            + "      <Level name=\"City\" column=\"warehouse_city\" uniqueMembers=\"false\"/>\n"
            + "      <Level name=\"Warehouse Name\" column=\"warehouse_name\" uniqueMembers=\"true\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Measure name=\"Warehouse Cost\" column=\"warehouse_cost\" aggregator=\"sum\"/>\n"
            + "  <Measure name=\"Warehouse Sales\" column=\"warehouse_sales\" aggregator=\"sum\"/>\n"
            + "</Cube>", null, null, null, null);

        testContext.assertQueryReturns(
            "select\n"
            + " {[Time].[1997], [Time].[1997].[Q3]} on columns,\n"
            + " {[Store].[USA].Children} on rows\n"
            + "From [Warehouse (based on view)]\n"
            + "where [Warehouse].[USA]",
            "Axis #0:\n"
            + "{[Warehouse].[USA]}\n"
            + "Axis #1:\n"
            + "{[Time].[1997]}\n"
            + "{[Time].[1997].[Q3]}\n"
            + "Axis #2:\n"
            + "{[Store].[All Stores].[USA].[CA]}\n"
            + "{[Store].[All Stores].[USA].[OR]}\n"
            + "{[Store].[All Stores].[USA].[WA]}\n"
            + "Row #0: 25,789.086\n"
            + "Row #0: 8,624.791\n"
            + "Row #1: 17,606.904\n"
            + "Row #1: 3,812.023\n"
            + "Row #2: 45,647.262\n"
            + "Row #2: 12,664.162\n");
    }

    /**
     * Tests a cube whose fact table is a &lt;View&gt; element, and which
     * has dimensions based on the fact table.
     */
    public void testViewFactTable2() {
        TestContext testContext = TestContext.create(
            null,
            // Similar to "Store" cube in FoodMart.xml.
            "<Cube name=\"Store2\">\n"
            + "  <View alias=\"FACT\">\n"
            + "    <SQL dialect=\"generic\">\n"
            + "     <![CDATA[select * from \"store\" as \"FOOBAR\"]]>\n"
            + "    </SQL>\n"
            + "    <SQL dialect=\"oracle\">\n"
            + "     <![CDATA[select * from \"store\" \"FOOBAR\"]]>\n"
            + "    </SQL>\n"
            + "    <SQL dialect=\"mysql\">\n"
            + "     <![CDATA[select * from `store` as `FOOBAR`]]>\n"
            + "    </SQL>\n"
            + "    <SQL dialect=\"infobright\">\n"
            + "     <![CDATA[select * from `store` as `FOOBAR`]]>\n"
            + "    </SQL>\n"
            + "  </View>\n"
            + "  <!-- We could have used the shared dimension \"Store Type\", but we\n"
            + "     want to test private dimensions without primary key. -->\n"
            + "  <Dimension name=\"Store Type\">\n"
            + "    <Hierarchy hasAll=\"true\">\n"
            + "      <Level name=\"Store Type\" column=\"store_type\" uniqueMembers=\"true\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "\n"
            + "  <Measure name=\"Store Sqft\" column=\"store_sqft\" aggregator=\"sum\"\n"
            + "      formatString=\"#,###\"/>\n"
            + "  <Measure name=\"Grocery Sqft\" column=\"grocery_sqft\" aggregator=\"sum\"\n"
            + "      formatString=\"#,###\"/>\n"
            + "\n"
            + "</Cube>", null, null, null, null);
        testContext.assertQueryReturns(
            "select {[Store Type].Children} on columns from [Store2]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store Type].[All Store Types].[Deluxe Supermarket]}\n"
            + "{[Store Type].[All Store Types].[Gourmet Supermarket]}\n"
            + "{[Store Type].[All Store Types].[HeadQuarters]}\n"
            + "{[Store Type].[All Store Types].[Mid-Size Grocery]}\n"
            + "{[Store Type].[All Store Types].[Small Grocery]}\n"
            + "{[Store Type].[All Store Types].[Supermarket]}\n"
            + "Row #0: 146,045\n"
            + "Row #0: 47,447\n"
            + "Row #0: \n"
            + "Row #0: 109,343\n"
            + "Row #0: 75,281\n"
            + "Row #0: 193,480\n");
    }

    /**
     * Tests that the deprecated "distinct count" value for the
     * Measure@aggregator attribute still works. The preferred value these days
     * is "distinct-count".
     */
    public void testDeprecatedDistinctCountAggregator() {
        TestContext testContext = TestContext.createSubstitutingCube(
            "Sales",
            null,
            "  <Measure name=\"Customer Count2\" column=\"customer_id\"\n"
            + "      aggregator=\"distinct count\" formatString=\"#,###\"/>\n"
            + "  <CalculatedMember\n"
            + "      name=\"Half Customer Count\"\n"
            + "      dimension=\"Measures\"\n"
            + "      visible=\"false\"\n"
            + "      formula=\"[Measures].[Customer Count2] / 2\">\n"
            + "  </CalculatedMember>");
        testContext.assertQueryReturns(
            "select {[Measures].[Unit Sales],"
            + "    [Measures].[Customer Count], "
            + "    [Measures].[Customer Count2], "
            + "    [Measures].[Half Customer Count]} on 0,\n"
            + " {[Store].[USA].Children} ON 1\n"
            + "FROM [Sales]\n"
            + "WHERE ([Gender].[M])",
            "Axis #0:\n"
            + "{[Gender].[All Gender].[M]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Customer Count]}\n"
            + "{[Measures].[Customer Count2]}\n"
            + "{[Measures].[Half Customer Count]}\n"
            + "Axis #2:\n"
            + "{[Store].[All Stores].[USA].[CA]}\n"
            + "{[Store].[All Stores].[USA].[OR]}\n"
            + "{[Store].[All Stores].[USA].[WA]}\n"
            + "Row #0: 37,989\n"
            + "Row #0: 1,389\n"
            + "Row #0: 1,389\n"
            + "Row #0: 695\n"
            + "Row #1: 34,623\n"
            + "Row #1: 536\n"
            + "Row #1: 536\n"
            + "Row #1: 268\n"
            + "Row #2: 62,603\n"
            + "Row #2: 901\n"
            + "Row #2: 901\n"
            + "Row #2: 451\n");
    }

    /**
     * Tests that an invalid aggregator causes an error.
     */
    public void testInvalidAggregator() {
        TestContext testContext = TestContext.createSubstitutingCube(
            "Sales",
            null,
            "  <Measure name=\"Customer Count3\" column=\"customer_id\"\n"
            + "      aggregator=\"invalidAggregator\" formatString=\"#,###\"/>\n"
            + "  <CalculatedMember\n"
            + "      name=\"Half Customer Count\"\n"
            + "      dimension=\"Measures\"\n"
            + "      visible=\"false\"\n"
            + "      formula=\"[Measures].[Customer Count2] / 2\">\n"
            + "  </CalculatedMember>");
        testContext.assertQueryThrows(
            "select from [Sales]",
            "Unknown aggregator 'invalidAggregator'; valid aggregators are: 'sum', 'count', 'min', 'max', 'avg', 'distinct-count'");
    }

    /**
     * Testcase for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-291">
     * Bug MONDRIAN-291, "'unknown usage' messages"</a>.
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
                "<?xml version=\"1.0\"?>\n"
                + "<Schema name=\"FoodMart\">\n"
                + "<Cube name=\"Sales Degen\">\n"
                + "  <Table name=\"sales_fact_1997\">\n"
                + "    <AggExclude pattern=\"agg_c_14_sales_fact_1997\"/>\n"
                + "    <AggExclude pattern=\"agg_l_05_sales_fact_1997\"/>\n"
                + "    <AggExclude pattern=\"agg_g_ms_pcat_sales_fact_1997\"/>\n"
                + "    <AggExclude pattern=\"agg_ll_01_sales_fact_1997\"/>\n"
                + "    <AggExclude pattern=\"agg_c_special_sales_fact_1997\"/>\n"
                + "    <AggExclude pattern=\"agg_l_03_sales_fact_1997\"/>\n"
                + "    <AggExclude pattern=\"agg_l_04_sales_fact_1997\"/>\n"
                + "    <AggExclude pattern=\"agg_pl_01_sales_fact_1997\"/>\n"
                + "    <AggExclude pattern=\"agg_lc_06_sales_fact_1997\"/>\n"
                + "    <AggName name=\"agg_c_10_sales_fact_1997\">\n"
                + "      <AggFactCount column=\"fact_count\"/>\n"
                + "      <AggMeasure name=\"[Measures].[Store Cost]\" column=\"store_cost\" />\n"
                + "      <AggMeasure name=\"[Measures].[Store Sales]\" column=\"store_sales\" />\n"
                + "     </AggName>\n"
                + "  </Table>\n"
                + "  <Dimension name=\"Time\" type=\"TimeDimension\" foreignKey=\"time_id\">\n"
                + "    <Hierarchy hasAll=\"false\" primaryKey=\"time_id\">\n"
                + "      <Table name=\"time_by_day\"/>\n"
                + "      <Level name=\"Year\" column=\"the_year\" type=\"Numeric\" uniqueMembers=\"true\"\n"
                + "          levelType=\"TimeYears\"/>\n"
                + "      <Level name=\"Quarter\" column=\"quarter\" uniqueMembers=\"false\"\n"
                + "          levelType=\"TimeQuarters\"/>\n"
                + "      <Level name=\"Month\" column=\"month_of_year\" uniqueMembers=\"false\" type=\"Numeric\"\n"
                + "          levelType=\"TimeMonths\"/>\n"
                + "    </Hierarchy>\n"
                + "  </Dimension>\n"
                + "  <Dimension name=\"Time Degenerate\">\n"
                + "    <Hierarchy hasAll=\"true\" primaryKey=\"time_id\">\n"
                + "      <Level name=\"day\" column=\"time_id\"/>\n"
                + "      <Level name=\"month\" column=\"product_id\" type=\"Numeric\"/>\n"
                + "    </Hierarchy>"
                + "  </Dimension>"
                + "  <Measure name=\"Store Cost\" column=\"store_cost\" aggregator=\"sum\"\n"
                + "      formatString=\"#,###.00\"/>\n"
                + "  <Measure name=\"Store Sales\" column=\"store_sales\" aggregator=\"sum\"\n"
                + "      formatString=\"#,###.00\"/>\n"
                + "</Cube>\n"
                + "</Schema>");
            testContext.assertQueryReturns(
                "select from [Sales Degen]",
                "Axis #0:\n"
                + "{}\n"
                + "225,627.23");
        } finally {
            logger.removeAppender(appender);
        }
        // Note that 'product_id' is NOT one of the columns with unknown usage.
        // It is used as a level in the degenerate dimension [Time Degenerate].
        TestContext.assertEqualsVerbose(
            "WARN - Recognizer.checkUnusedColumns: Candidate aggregate table 'agg_c_10_sales_fact_1997' for fact table 'sales_fact_1997' has a column 'customer_count' with unknown usage.\n"
            + "WARN - Recognizer.checkUnusedColumns: Candidate aggregate table 'agg_c_10_sales_fact_1997' for fact table 'sales_fact_1997' has a column 'month_of_year' with unknown usage.\n"
            + "WARN - Recognizer.checkUnusedColumns: Candidate aggregate table 'agg_c_10_sales_fact_1997' for fact table 'sales_fact_1997' has a column 'quarter' with unknown usage.\n"
            + "WARN - Recognizer.checkUnusedColumns: Candidate aggregate table 'agg_c_10_sales_fact_1997' for fact table 'sales_fact_1997' has a column 'the_year' with unknown usage.\n"
            + "WARN - Recognizer.checkUnusedColumns: Candidate aggregate table 'agg_c_10_sales_fact_1997' for fact table 'sales_fact_1997' has a column 'unit_sales' with unknown usage.\n"
            + "WARN - Recognizer.checkUnusedColumns: Candidate aggregate table 'agg_lc_100_sales_fact_1997' for fact table 'sales_fact_1997' has a column 'customer_id' with unknown usage.\n"
            + "WARN - Recognizer.checkUnusedColumns: Candidate aggregate table 'agg_lc_100_sales_fact_1997' for fact table 'sales_fact_1997' has a column 'unit_sales' with unknown usage.\n",
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
                "<?xml version=\"1.0\"?>\n"
                + "<Schema name=\"FoodMart\">\n"
                + "<Cube name=\"Denormalized Sales\">\n"
                + "  <Table name=\"sales_fact_1997\">\n"
                + "    <AggExclude pattern=\"agg_c_14_sales_fact_1997\"/>\n"
                + "    <AggExclude pattern=\"agg_l_05_sales_fact_1997\"/>\n"
                + "    <AggExclude pattern=\"agg_g_ms_pcat_sales_fact_1997\"/>\n"
                + "    <AggExclude pattern=\"agg_ll_01_sales_fact_1997\"/>\n"
                + "    <AggExclude pattern=\"agg_c_special_sales_fact_1997\"/>\n"
                + "    <AggExclude pattern=\"agg_l_04_sales_fact_1997\"/>\n"
                + "    <AggExclude pattern=\"agg_pl_01_sales_fact_1997\"/>\n"
                + "    <AggExclude pattern=\"agg_c_10_sales_fact_1997\"/>\n"
                + "    <AggExclude pattern=\"agg_lc_06_sales_fact_1997\"/>\n"
                + "    <AggName name=\"agg_l_03_sales_fact_1997\">\n"
                + "      <AggFactCount column=\"fact_count\"/>\n"
                + "      <AggMeasure name=\"[Measures].[Store Cost]\" column=\"store_cost\" />\n"
                + "      <AggMeasure name=\"[Measures].[Store Sales]\" column=\"store_sales\" />\n"
                + "      <AggMeasure name=\"[Measures].[Unit Sales]\" column=\"unit_sales\" />\n"
                + "      <AggLevel name=\"[Customer].[Customer ID]\" column=\"customer_id\" />\n"
                + "      <AggForeignKey factColumn=\"time_id\" aggColumn=\"time_id\" />\n"
                + "     </AggName>\n"
                + "  </Table>\n"
                + "  <Dimension name=\"Time\" type=\"TimeDimension\" foreignKey=\"time_id\">\n"
                + "    <Hierarchy hasAll=\"false\" primaryKey=\"time_id\">\n"
                + "      <Table name=\"time_by_day\"/>\n"
                + "      <Level name=\"Year\" column=\"the_year\" type=\"Numeric\" uniqueMembers=\"true\"\n"
                + "          levelType=\"TimeYears\"/>\n"
                + "      <Level name=\"Quarter\" column=\"quarter\" uniqueMembers=\"false\"\n"
                + "          levelType=\"TimeQuarters\"/>\n"
                + "      <Level name=\"Month\" column=\"month_of_year\" uniqueMembers=\"false\" type=\"Numeric\"\n"
                + "          levelType=\"TimeMonths\"/>\n"
                + "    </Hierarchy>\n"
                + "  </Dimension>\n"
                + "  <Dimension name=\"Customer\">\n"
                + "    <Hierarchy hasAll=\"true\" primaryKey=\"customer_id\">\n"
                + "      <Level name=\"Customer ID\" column=\"customer_id\"/>\n"
                + "    </Hierarchy>"
                + "  </Dimension>"
                + "  <Dimension name=\"Product\">\n"
                + "    <Hierarchy hasAll=\"true\" primaryKey=\"product_id\">\n"
                + "      <Level name=\"Product ID\" column=\"product_id\"/>\n"
                + "    </Hierarchy>"
                + "  </Dimension>"
                + "  <Measure name=\"Store Cost\" column=\"store_cost\" aggregator=\"sum\"\n"
                + "      formatString=\"#,###.00\"/>\n"
                + "  <Measure name=\"Store Sales\" column=\"store_sales\" aggregator=\"sum\"\n"
                + "      formatString=\"#,###.00\"/>\n"
                + "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\"\n"
                + "      formatString=\"#,###\"/>\n"
                + "</Cube>\n"
                + "</Schema>");
            testContext.assertQueryReturns(
                "select from [Denormalized Sales]",
                "Axis #0:\n"
                + "{}\n"
                + "225,627.23");
        } finally {
            logger.removeAppender(appender);
        }
        TestContext.assertEqualsVerbose(
            "WARN - Recognizer.checkUnusedColumns: Candidate aggregate table 'agg_l_03_sales_fact_1997' for fact table 'sales_fact_1997' has a column 'time_id' with unknown usage.\n",
            sw.toString());
    }

    public void testPropertyFormatter() {
        final TestContext testContext =
            TestContext.createSubstitutingCube(
                "Sales",
                "  <Dimension name=\"Store2\" foreignKey=\"store_id\">\n"
                + "    <Hierarchy name=\"Store2\" hasAll=\"true\" allMemberName=\"All Stores\" primaryKey=\"store_id\">\n"
                + "      <Table name=\"store_ragged\"/>\n"
                + "      <Level name=\"Store2\" table=\"store_ragged\" column=\"store_id\" captionColumn=\"store_name\" uniqueMembers=\"true\">\n"
                + "           <Property name=\"Store Type\" column=\"store_type\" formatter=\""
                + DummyPropertyFormatter.class.getName()
                + "\"/>"
                + "           <Property name=\"Store Manager\" column=\"store_manager\"/>"
                + "     </Level>"
                + "    </Hierarchy>\n"
                + "  </Dimension>\n");
        try {
            testContext.assertSimpleQuery();
            fail("expected exception");
        } catch (RuntimeException e) {
            TestContext.checkThrowable(
                e,
                "Failed to load formatter class 'mondrian.test.SchemaTest$DummyPropertyFormatter' for property 'Store Type'.");
        }
    }

    /**
     * Bug <a href="http://jira.pentaho.com/browse/MONDRIAN-233">MONDRIAN-233,
     * "ClassCastException in AggQuerySpec"</a> occurs when two cubes
     * have the same fact table, distinct aggregate tables, and measures with
     * the same name.
     *
     * <p>This test case attempts to reproduce this issue by creating that
     * environment, but it found a different issue: a measure came back with a
     * cell value which was from a different measure. The root cause is
     * probably the same: when measures are registered in a star, they should
     * be qualified by cube name.
     */
    public void testBugMondrian233() {
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
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Row #0: 266,773\n";
        testContext.assertQueryReturns(
            "select {[Measures]} on 0 from [Sales2]",
            expected);
        testContext.assertQueryReturns(
            "select {[Measures]} on 0 from [Sales]",
            expected);
    }

    /**
     * Test case for bug <a href="http://jira.pentaho.com/browse/MONDRIAN-303">
     * MONDRIAN-303, "Property column shifting when use captionColumn"</a>.
     */
    public void testBugMondrian303() {
        // In order to reproduce the problem a dimension specifying
        // captionColumn and Properties were required.
        final TestContext testContext = TestContext.createSubstitutingCube(
            "Sales",
            "  <Dimension name=\"Store2\" foreignKey=\"store_id\">\n"
            + "    <Hierarchy name=\"Store2\" hasAll=\"true\" allMemberName=\"All Stores\" primaryKey=\"store_id\">\n"
            + "      <Table name=\"store_ragged\"/>\n"
            + "      <Level name=\"Store2\" table=\"store_ragged\" column=\"store_id\" captionColumn=\"store_name\" uniqueMembers=\"true\">\n"
            + "           <Property name=\"Store Type\" column=\"store_type\"/>"
            + "           <Property name=\"Store Manager\" column=\"store_manager\"/>"
            + "     </Level>"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n");

        // In the query below Mondrian (prior to the fix) would
        // return the store name instead of the store type.
        testContext.assertQueryReturns(
            "WITH\n"
            + "   MEMBER [Measures].[StoreType] AS \n"
            + "   '[Store2].CurrentMember.Properties(\"Store Type\")'\n"
            + "SELECT\n"
            + "   NonEmptyCrossJoin({[Store2].[All Stores].children}, {[Product].[All Products]}) ON ROWS,\n"
            + "   { [Measures].[Store Sales], [Measures].[StoreType]} ON COLUMNS\n"
            + "FROM Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Sales]}\n"
            + "{[Measures].[StoreType]}\n"
            + "Axis #2:\n"
            + "{[Store2].[All Stores].[2], [Product].[All Products]}\n"
            + "{[Store2].[All Stores].[3], [Product].[All Products]}\n"
            + "{[Store2].[All Stores].[6], [Product].[All Products]}\n"
            + "{[Store2].[All Stores].[7], [Product].[All Products]}\n"
            + "{[Store2].[All Stores].[11], [Product].[All Products]}\n"
            + "{[Store2].[All Stores].[13], [Product].[All Products]}\n"
            + "{[Store2].[All Stores].[14], [Product].[All Products]}\n"
            + "{[Store2].[All Stores].[15], [Product].[All Products]}\n"
            + "{[Store2].[All Stores].[16], [Product].[All Products]}\n"
            + "{[Store2].[All Stores].[17], [Product].[All Products]}\n"
            + "{[Store2].[All Stores].[22], [Product].[All Products]}\n"
            + "{[Store2].[All Stores].[23], [Product].[All Products]}\n"
            + "{[Store2].[All Stores].[24], [Product].[All Products]}\n"
            + "Row #0: 4,739.23\n"
            + "Row #0: Small Grocery\n"
            + "Row #1: 52,896.30\n"
            + "Row #1: Supermarket\n"
            + "Row #2: 45,750.24\n"
            + "Row #2: Gourmet Supermarket\n"
            + "Row #3: 54,545.28\n"
            + "Row #3: Supermarket\n"
            + "Row #4: 55,058.79\n"
            + "Row #4: Supermarket\n"
            + "Row #5: 87,218.28\n"
            + "Row #5: Deluxe Supermarket\n"
            + "Row #6: 4,441.18\n"
            + "Row #6: Small Grocery\n"
            + "Row #7: 52,644.07\n"
            + "Row #7: Supermarket\n"
            + "Row #8: 49,634.46\n"
            + "Row #8: Supermarket\n"
            + "Row #9: 74,843.96\n"
            + "Row #9: Deluxe Supermarket\n"
            + "Row #10: 4,705.97\n"
            + "Row #10: Small Grocery\n"
            + "Row #11: 24,329.23\n"
            + "Row #11: Mid-Size Grocery\n"
            + "Row #12: 54,431.14\n"
            + "Row #12: Supermarket\n");
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
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Promotion Media].[All Media]}\n"
            + "Row #0: 266,773\n");
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
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Product].[All Products].[Drink]}\n"
            + "{[Product].[All Products].[Food]}\n"
            + "{[Product].[All Products].[Non-Consumable]}\n"
            + "Row #0: 24,597\n"
            + "Row #0: 191,940\n"
            + "Row #0: 50,236\n");
    }

    public void testCubeHasFact() {
        final TestContext testContext = TestContext.create(
            null,
            "<Cube name=\"Cube with caption\" caption=\"Cube with name\"/>\n",
            null, null, null, null);
        Throwable throwable = null;
        try {
            testContext.assertSimpleQuery();
        } catch (Throwable e) {
            throwable = e;
        }
        TestContext.checkThrowable(
            throwable,
            "Must specify fact table of cube 'Cube with caption'");
    }

    public void testCubeCaption() throws SQLException {
        final TestContext testContext = TestContext.create(
            null,
            "<Cube name=\"Cube with caption\" caption=\"Cube with name\">"
                + "  <Table name='sales_fact_1997'/>"
                + "</Cube>\n",
            "<VirtualCube name=\"Warehouse and Sales with caption\" "
                + " caption=\"Warehouse and Sales with name\" "
                + "defaultMeasure=\"Store Sales\">\n"
                + "  <VirtualCubeDimension cubeName=\"Sales\" name=\"Customers\"/>\n"
                + "</VirtualCube>",
            null, null, null);
        final NamedList<org.olap4j.metadata.Cube> cubes =
            testContext.getOlap4jConnection().getSchema().getCubes();
        final org.olap4j.metadata.Cube cube = cubes.get("Cube with caption");
        assertEquals("Cube with name", cube.getCaption(null));
        final org.olap4j.metadata.Cube cube2 =
            cubes.get("Warehouse and Sales with caption");
        assertEquals("Warehouse and Sales with name", cube2.getCaption(null));
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
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Row #0: 266,773\n");
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
        testContext.assertQueryThrows(
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
        testContext.assertQueryThrows(
            "select {[Measures]} on columns from [OneCalcMeasure]\n"
            + "where [Promotion Media].[TV]",
            "Hierarchy '[Measures]' is invalid (has no members)");
    }

    /**
     * this test triggers an exception out of the aggregate table manager
     */
    public void testAggTableSupportOfSharedDims() {
        if (Bug.BugMondrian361Fixed) {
            TestContext testContext = TestContext.create(
                null,
                "<Cube name=\"Sales Two Dimensions\">\n"
                + "  <Table name=\"sales_fact_1997\"/>\n"
                + "  <DimensionUsage name=\"Time\" source=\"Time\" foreignKey=\"time_id\"/>\n"
                + "  <DimensionUsage name=\"Time2\" source=\"Time\" foreignKey=\"product_id\"/>\n"
                + "  <DimensionUsage name=\"Store\" source=\"Store\" foreignKey=\"store_id\"/>\n"
                + "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\" "
                + "   formatString=\"Standard\"/>\n"
                + "  <Measure name=\"Store Cost\" column=\"store_cost\" aggregator=\"sum\""
                + "   formatString=\"#,###.00\"/>\n"
                + "</Cube>",
                null,
                null,
                null,
                null);

            testContext.assertQueryReturns(
                "select\n"
                + " {[Time2].[1997]} on columns,\n"
                + " {[Time].[1997].[Q3]} on rows\n"
                + "From [Sales Two Dimensions]",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Time2].[1997]}\n"
                + "Axis #2:\n"
                + "{[Time].[1997].[Q3]}\n"
                + "Row #0: 16,266\n");

            MondrianProperties props = MondrianProperties.instance();
            boolean currentUse = props.UseAggregates.get();
            boolean currentRead = props.ReadAggregates.get();
            boolean do_caching_orig = props.DisableCaching.get();

            // turn off caching
            props.DisableCaching.setString("true");

            // re-read aggregates
            props.UseAggregates.setString("true");
            props.ReadAggregates.setString("false");
            props.ReadAggregates.setString("true");

            if (currentRead) {
                props.ReadAggregates.setString("true");
            } else {
                props.ReadAggregates.setString("false");
            }
            if (currentUse) {
                props.UseAggregates.setString("true");
            } else {
                props.UseAggregates.setString("false");
            }
            if (do_caching_orig) {
                props.DisableCaching.setString("true");
            } else {
                props.DisableCaching.setString("false");
            }
            // force reloading of aggregates, which currently throws an
            // exception
        }
    }

    /**
     * Verifies that RolapHierarchy.tableExists() supports views.
     */
    public void testLevelTableAttributeAsView() {
        final TestContext testContext = TestContext.create(
              null,
              "<Cube name=\"GenderCube\">\n"
              + "  <Table name=\"sales_fact_1997\" alias=\"sales_fact_1997_gender\"/>\n"
              + "<Dimension name=\"Gender2\" foreignKey=\"customer_id\">\n"
              + "  <Hierarchy hasAll=\"true\" allMemberName=\"All Gender\" primaryKey=\"customer_id\">\n"
              + "    <View alias=\"gender2\">\n"
              + "      <SQL dialect=\"generic\">\n"
              + "        <![CDATA[SELECT * FROM customer]]>\n"
              + "      </SQL>\n"
              + "      <SQL dialect=\"oracle\">\n"
              + "        <![CDATA[SELECT * FROM \"customer\"]]>\n"
              + "      </SQL>\n"
              + "      <SQL dialect=\"derby\">\n"
              + "        <![CDATA[SELECT * FROM \"customer\"]]>\n"
              + "      </SQL>\n"
              + "      <SQL dialect=\"hsqldb\">\n"
              + "        <![CDATA[SELECT * FROM \"customer\"]]>\n"
              + "      </SQL>\n"
              + "      <SQL dialect=\"luciddb\">\n"
              + "        <![CDATA[SELECT * FROM \"customer\"]]>\n"
              + "      </SQL>\n"
              + "      <SQL dialect=\"neoview\">\n"
              + "        <![CDATA[SELECT * FROM \"customer\"]]>\n"
              + "      </SQL>\n"
              + "      <SQL dialect=\"netezza\">\n"
              + "        <![CDATA[SELECT * FROM \"customer\"]]>\n"
              + "      </SQL>\n"
              + "      <SQL dialect=\"db2\">\n"
              + "        <![CDATA[SELECT * FROM \"customer\"]]>\n"
              + "      </SQL>\n"
              + "    </View>\n"
              + "    <Level name=\"Gender\" table=\"gender2\" column=\"gender\" uniqueMembers=\"true\"/>\n"
              + "  </Hierarchy>\n"
              + "</Dimension>"
              + "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\"\n"
              + "      formatString=\"Standard\"/>\n"
              + "</Cube>",
              null, null, null, null);

        if (!testContext.getDialect().allowsFromQuery()) {
            return;
        }

        Result result = testContext.executeQuery(
                "select {[Gender2].members} on columns from [GenderCube]");

        TestContext.assertEqualsVerbose(
            "[Gender2].[All Gender]\n"
            + "[Gender2].[All Gender].[F]\n"
            + "[Gender2].[All Gender].[M]",
            TestContext.toString(
                result.getAxes()[0].getPositions()));
    }

    public void testInvalidSchemaAccess() {
        final TestContext testContext = TestContext.create(
            null, null, null, null, null,
            "<Role name=\"Role1\">\n"
                + "  <SchemaGrant access=\"invalid\"/>\n"
                + "</Role>")
            .withRole("Role1");
        testContext.assertQueryThrows(
            "select from [Sales]",
            "In Schema: In Role: In SchemaGrant: "
                + "Value 'invalid' of attribute 'access' has illegal value 'invalid'.  "
                + "Legal values: {all, custom, none, all_dimensions}");
    }

    public void testAllMemberNoStringReplace() {
        TestContext testContext = TestContext.create(
            null,
            "<Cube name=\"Sales Special Time\">\n"
            + "  <Table name=\"sales_fact_1997\"/>\n"
            + "<Dimension name=\"TIME\" foreignKey=\"time_id\" type=\"TimeDimension\">"
            + "<Hierarchy name=\"CALENDAR\" hasAll=\"true\" allMemberName=\"All TIME(CALENDAR)\" primaryKey=\"time_id\">"
            + "  <Table name=\"time_by_day\"/>"
            + "  <Level name=\"Years\" column=\"the_year\" uniqueMembers=\"true\" levelType=\"TimeYears\"/>"
            + "  <Level name=\"Quarters\" column=\"quarter\" uniqueMembers=\"false\" levelType=\"TimeQuarters\"/>"
            + "  <Level name=\"Months\" column=\"month_of_year\" uniqueMembers=\"false\" levelType=\"TimeMonths\"/>"
            + "</Hierarchy>"
            + "</Dimension>"
            + "  <DimensionUsage name=\"Store\" source=\"Store\" foreignKey=\"store_id\"/>\n"
            + "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\" "
            + "   formatString=\"Standard\"/>\n"
            + "  <Measure name=\"Store Cost\" column=\"store_cost\" aggregator=\"sum\""
            + "   formatString=\"#,###.00\"/>\n"
            + "</Cube>",
            null,
            null,
            null,
            null);

        testContext.assertQueryReturns(
            "select [TIME.CALENDAR].[All TIME(CALENDAR)] on columns\n"
            + "from [Sales Special Time]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[TIME].[CALENDAR].[All TIME(CALENDAR)]}\n"
            + "Row #0: 266,773\n");
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
        testContext.assertQueryReturns(
            "select from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "266,773");
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
        testContext.assertQueryThrows(
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
        testContext.assertQueryThrows(
            "select from [Sales]", "Unknown role 'Role2'");
    }

    public void testVirtualCubeNamedSetSupportInSchema() {
        final TestContext testContext = TestContext.createSubstitutingCube(
            "Warehouse and Sales",
            null,
            null,
            "<NamedSet name=\"Non CA State Stores\" "
            + "formula=\"EXCEPT({[Store].[Store Country].[USA].children},{[Store].[Store Country].[USA].[CA]})\"/>");
        testContext.assertQueryReturns(
            "WITH "
            + "SET [Non CA State Stores] AS 'EXCEPT({[Store].[Store Country].[USA].children},"
            + "{[Store].[Store Country].[USA].[CA]})'\n"
            + "MEMBER "
            + "[Store].[Total Non CA State] AS \n"
            + "'SUM({[Non CA State Stores]})'\n"
            + "SELECT {[Store].[Store Country].[USA],[Store].[Total Non CA State]} ON 0,"
            + "{[Measures].[Unit Sales]} ON 1 FROM [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[All Stores].[USA]}\n"
            + "{[Store].[Total Non CA State]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Row #0: 266,773\n"
            + "Row #0: 192,025\n");

        testContext.assertQueryReturns(
            "WITH "
            + "MEMBER "
            + "[Store].[Total Non CA State] AS \n"
            + "'SUM({[Non CA State Stores]})'\n"
            + "SELECT {[Store].[Store Country].[USA],[Store].[Total Non CA State]} ON 0,"
            + "{[Measures].[Unit Sales]} ON 1 FROM [Warehouse and Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[All Stores].[USA]}\n"
            + "{[Store].[Total Non CA State]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Row #0: 266,773\n"
            + "Row #0: 192,025\n");
    }

    public void testVirtualCubeNamedSetSupportInSchemaError() {
        final TestContext testContext = TestContext.createSubstitutingCube(
            "Warehouse and Sales",
            null,
            null,
            "<NamedSet name=\"Non CA State Stores\" "
            + "formula=\"EXCEPT({[Store].[Store State].[USA].children},{[Store].[Store Country].[USA].[CA]})\"/>");
        try {
            testContext.assertQueryReturns(
                "WITH "
                + "SET [Non CA State Stores] AS 'EXCEPT({[Store].[Store Country].[USA].children},"
                + "{[Store].[Store Country].[USA].[CA]})'\n"
                + "MEMBER "
                + "[Store].[Total Non CA State] AS \n"
                + "'SUM({[Non CA State Stores]})'\n"
                + "SELECT {[Store].[Store Country].[USA],[Store].[Total Non CA State]} ON 0,"
                + "{[Measures].[Unit Sales]} ON 1 FROM [Sales]",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Store].[All Stores].[USA]}\n"
                + "{[Store].[Total Non CA State]}\n"
                + "Axis #2:\n"
                + "{[Measures].[Unit Sales]}\n"
                + "Row #0: 266,773\n"
                + "Row #0: 192,025\n");
            fail();
        } catch (MondrianException e) {
            assertTrue(e.getMessage().indexOf("bad formula") >= 0);
        }
    }

    public void _testValidatorFindsNumericLevel() {
        // In the real foodmart, the level has type="Numeric"
        final TestContext testContext =
            TestContext.createSubstitutingCube(
            "Sales",
            "  <Dimension name=\"Store Size in SQFT\">\n"
                + "    <Hierarchy hasAll=\"true\" primaryKey=\"store_id\">\n"
                + "      <Table name=\"store\"/>\n"
                + "      <Level name=\"Store Sqft\" column=\"store_sqft\" type=\"Numeric\" uniqueMembers=\"true\"/>\n"
                + "    </Hierarchy>\n"
                + "  </Dimension>");
        final List<Exception> exceptionList = testContext.getSchemaWarnings();
        assertContains(exceptionList, "todo xxxxx");
    }

    public void testInvalidRoleError() {
        String schema = TestContext.getRawFoodMartSchema();
        schema =
            schema.replaceFirst(
                "<Schema name=\"FoodMart\"",
                "<Schema name=\"FoodMart\" defaultRole=\"Unknown\"");
        final TestContext testContext = TestContext.create(schema);
        final List<Exception> exceptionList = testContext.getSchemaWarnings();
        assertContains(exceptionList, "Role 'Unknown' not found");
    }

    /**
     * Test case for bug <a href="http://jira.pentaho.com/browse/MONDRIAN-413">
     * MONDRIAN-413, "RolapMember causes ClassCastException in compare()"</a>,
     * caused by binary column value.
     */
    public void testBinaryLevelKey() {
        switch (TestContext.instance().getDialect().getDatabaseProduct()) {
        case DERBY:
        case MYSQL:
            break;
        default:
            // Not all databases support binary literals (e.g. X'AB01'). Only
            // Derby returns them as byte[] values from its JDBC driver and
            // therefore experiences bug MONDRIAN-413.
            return;
        }
        final TestContext testContext = TestContext.createSubstitutingCube(
            "Sales",
            "  <Dimension name=\"Binary\" foreignKey=\"promotion_id\">\n"
            + "    <Hierarchy hasAll=\"false\" primaryKey=\"id\">\n"
            + "      <InlineTable alias=\"binary\">\n"
            + "        <ColumnDefs>\n"
            + "          <ColumnDef name=\"id\" type=\"Integer\"/>\n"
            + "          <ColumnDef name=\"bin\" type=\"Integer\"/>\n"
            + "          <ColumnDef name=\"name\" type=\"String\"/>\n"
            + "        </ColumnDefs>\n"
            + "        <Rows>\n"
            + "          <Row>\n"
            + "            <Value column=\"id\">2</Value>\n"
            + "            <Value column=\"bin\">X'4546'</Value>\n"
            + "            <Value column=\"name\">Ben</Value>\n"
            + "          </Row>\n"
            + "          <Row>\n"
            + "            <Value column=\"id\">3</Value>\n"
            + "            <Value column=\"bin\">X'424344'</Value>\n"
            + "            <Value column=\"name\">Bill</Value>\n"
            + "          </Row>\n"
            + "          <Row>\n"
            + "            <Value column=\"id\">4</Value>\n"
            + "            <Value column=\"bin\">X'424344'</Value>\n"
            + "            <Value column=\"name\">Bill</Value>\n"
            + "          </Row>\n"
            + "        </Rows>\n"
            + "      </InlineTable>\n"
            + "      <Level name=\"Level1\" column=\"bin\" nameColumn=\"name\" ordinalColumn=\"name\" />\n"
            + "      <Level name=\"Level2\" column=\"id\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n");
        testContext.assertQueryReturns(
            "select {[Binary].members} on 0 from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Binary].[Ben]}\n"
            + "{[Binary].[Ben].[2]}\n"
            + "{[Binary].[Bill]}\n"
            + "{[Binary].[Bill].[3]}\n"
            + "{[Binary].[Bill].[4]}\n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n");
        testContext.assertQueryReturns(
            "select hierarchize({[Binary].members}) on 0 from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Binary].[Ben]}\n"
            + "{[Binary].[Ben].[2]}\n"
            + "{[Binary].[Bill]}\n"
            + "{[Binary].[Bill].[3]}\n"
            + "{[Binary].[Bill].[4]}\n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n");
    }

    public void _testAttributeHierarchy() {
        // from email from peter tran dated 2008/9/8
        // TODO: schema syntax to create attribute hierarchy
        assertQueryReturns(
            "WITH \n"
                + " MEMBER\n"
                + "  Measures.SalesPerWorkingDay AS \n"
                + "    IIF(\n"
                + "     Count(\n"
                + "      Filter(\n"
                + "        Descendants(\n"
                + "          [Date].[Calendar].CurrentMember\n"
                + "          ,[Date].[Calendar].[Date]\n"
                + "          ,SELF)\n"
                + "       ,  [Date].[Day of Week].CurrentMember.Name <> \"1\"\n"
                + "      )\n"
                + "    ) = 0\n"
                + "     ,NULL\n"
                + "     ,[Measures].[Internet Sales Amount]\n"
                + "      /\n"
                + "       Count(\n"
                + "         Filter(\n"
                + "           Descendants(\n"
                + "             [Date].[Calendar].CurrentMember\n"
                + "             ,[Date].[Calendar].[Date]\n"
                + "             ,SELF)\n"
                + "          ,  [Date].[Day of Week].CurrentMember.Name <> \"1\"\n"
                + "         )\n"
                + "       )\n"
                + "    )\n"
                + "   '\n"
                + "SELECT [Measures].[SalesPerWorkingDay]  ON 0\n"
                + ", [Date].[Calendar].[Month].MEMBERS ON 1\n"
                + "FROM [Adventure Works]",
            "x");
    }

    /**
     * Testcase for a problem which involved a slowly changing dimension.
     * Not actually a slowly-changing dimension - we don't have such a thing in
     * the foodmart schema - but the same structure. The dimension is a two
     * table snowflake, and the table nearer to the fact table is not used by
     * any level.
     */
    public void testScdJoin() {
        final TestContext testContext =
            TestContext.createSubstitutingCube(
                "Sales",
                "  <Dimension name=\"Product truncated\" foreignKey=\"product_id\">\n"
                + "    <Hierarchy hasAll=\"true\" primaryKey=\"product_id\" primaryKeyTable=\"product\">\n"
                + "      <Join leftKey=\"product_class_id\" rightKey=\"product_class_id\">\n"
                + "        <Table name=\"product\"/>\n"
                + "        <Table name=\"product_class\"/>\n"
                + "      </Join>\n"
                + "      <Level name=\"Product Class\" table=\"product_class\" nameColumn=\"product_subcategory\"\n"
                + "          column=\"product_class_id\" type=\"Numeric\" uniqueMembers=\"true\"/>\n"
                + "    </Hierarchy>\n"
                + "  </Dimension>\n", null, null);
        testContext.assertQueryReturns(
            "select non empty {[Measures].[Unit Sales]} on 0,\n"
                + " non empty Filter({[Product truncated].Members}, [Measures].[Unit Sales] > 10000) on 1\n"
                + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product truncated].[All Product truncateds]}\n"
            + "{[Product truncated].[All Product truncateds].[Fresh Vegetables]}\n"
            + "{[Product truncated].[All Product truncateds].[Fresh Fruit]}\n"
            + "Row #0: 266,773\n"
            + "Row #1: 20,739\n"
            + "Row #2: 11,767\n");
    }

    // TODO: enable this test as part of PhysicalSchema work
    // TODO: also add a test that Table.alias, Join.leftAlias and
    // Join.rightAlias cannot be the empty string.
    public void _testNonUniqueAlias() {
        final TestContext testContext =
            TestContext.createSubstitutingCube(
                "Sales",
                "  <Dimension name=\"Product truncated\" foreignKey=\"product_id\">\n"
                + "    <Hierarchy hasAll=\"true\" primaryKey=\"product_id\" primaryKeyTable=\"product\">\n"
                + "      <Join leftKey=\"product_class_id\" rightKey=\"product_class_id\">\n"
                + "        <Table name=\"product\" alias=\"product_class\"/>\n"
                + "        <Table name=\"product_class\"/>\n"
                + "      </Join>\n"
                + "      <Level name=\"Product Class\" table=\"product_class\" nameColumn=\"product_subcategory\"\n"
                + "          column=\"product_class_id\" type=\"Numeric\" uniqueMembers=\"true\"/>\n"
                + "    </Hierarchy>\n"
                + "  </Dimension>\n", null, null);
        Throwable throwable = null;
        try {
            testContext.assertSimpleQuery();
        } catch (Throwable e) {
            throwable = e;
        }
        // neither a source column or source expression specified
        TestContext.checkThrowable(
            throwable,
            "Alias not unique");
    }

    /**
     * Test case for bug <a href="http://jira.pentaho.com/browse/MONDRIAN-482">
     * MONDRIAN-482, "ClassCastException when obtaining RolapCubeLevel"</a>.
     */
    public void testBugMondrian482() {
        // until bug MONDRIAN-495, "Table filter concept does not support
        // dialects." is fixed, this test case only works on MySQL
        if (!Bug.BugMondrian495Fixed
            && TestContext.instance().getDialect().getDatabaseProduct()
            != Dialect.DatabaseProduct.MYSQL)
        {
            return;
        }

        // skip this test if using aggregates, the agg tables do not
        // enforce the SQL element in the fact table
        if (MondrianProperties.instance().UseAggregates.booleanValue()) {
            return;
        }

        // In order to reproduce the problem it was necessary to only have one
        // non empty member under USA. In the cube definition below we create a
        // cube with only CA data to achieve this.
        String salesCube1 =
            "<Cube name=\"Sales2\" defaultMeasure=\"Unit Sales\">\n"
            + "  <Table name=\"sales_fact_1997\" >\n"
            + "    <SQL dialect=\"default\">\n"
            + "     <![CDATA[`sales_fact_1997`.`store_id` in (select distinct `store_id` from `store` where `store`.`store_state` = \"CA\")]]>\n"
            + "    </SQL>\n"
            + "  </Table>\n"
            + "  <DimensionUsage name=\"Store\" source=\"Store\" foreignKey=\"store_id\"/>\n"
            + "  <DimensionUsage name=\"Product\" source=\"Product\" foreignKey=\"product_id\"/>\n"
            + "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\" formatString=\"Standard\"/>\n"
            + "  <Measure name=\"Store Sales\" column=\"store_sales\" aggregator=\"sum\" formatString=\"Standard\"/>\n"
            + "</Cube>\n";

        TestContext testContext =
        TestContext.create(
            null,
            salesCube1,
            null,
            null,
            null,
            null);

        // First query all children of the USA. This should only return CA since
        // all the other states were filtered out. CA will be put in the member
        // cache
        String query1 =
            "WITH SET [#DataSet#] as "
            + "'NonEmptyCrossjoin({[Product].[All Products]}, {[Store].[All Stores].[USA].Children})' "
            + "SELECT {[Measures].[Unit Sales]} on columns, "
            + "NON EMPTY Hierarchize({[#DataSet#]}) on rows FROM [Sales2]";

        testContext.assertQueryReturns(
            query1,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[All Products], [Store].[All Stores].[USA].[CA]}\n"
            + "Row #0: 74,748\n");

        // Now query the children of CA using the descendants function
        // This is where the ClassCastException occurs
        String query2 =
            "WITH SET [#DataSet#] as "
            + "'{Descendants([Store].[All Stores], 3)}' "
            + "SELECT {[Measures].[Unit Sales]} on columns, "
            + "NON EMPTY Hierarchize({[#DataSet#]}) on rows FROM [Sales2]";

        testContext.assertQueryReturns(
            query2,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Store].[All Stores].[USA].[CA].[Beverly Hills]}\n"
            + "{[Store].[All Stores].[USA].[CA].[Los Angeles]}\n"
            + "{[Store].[All Stores].[USA].[CA].[San Diego]}\n"
            + "{[Store].[All Stores].[USA].[CA].[San Francisco]}\n"
            + "Row #0: 21,333\n"
            + "Row #1: 25,663\n"
            + "Row #2: 25,635\n"
            + "Row #3: 2,117\n");
    }

    /**
     * Test case for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-355">Bug MONDRIAN-355,
     * "adding hours/mins as levelType for level of type Dimension"</a>.
     */
    public void testBugMondrian355() {
        final String xml =
            "<Dimension name=\"Time2\" foreignKey=\"time_id\" type=\"TimeDimension\">\n"
            + "<Hierarchy hasAll=\"true\" primaryKey=\"time_id\">\n"
            + "  <Table name=\"time_by_day\"/>\n"
            + "  <Level name=\"Years\" column=\"the_year\" uniqueMembers=\"true\" type=\"Numeric\" levelType=\"TimeYears\"/>\n"
            + "  <Level name=\"Half year\" column=\"quarter\" uniqueMembers=\"false\" levelType=\"TimeHalfYear\"/>\n"
            + "  <Level name=\"Hours\" column=\"month_of_year\" uniqueMembers=\"false\" type=\"Numeric\" levelType=\"TimeHours\"/>\n"
            + "  <Level name=\"Quarter hours\" column=\"time_id\" uniqueMembers=\"false\" type=\"Numeric\" levelType=\"TimeUndefined\"/>\n"
            + "</Hierarchy>\n"
            + "</Dimension>";
        TestContext testContext = TestContext.createSubstitutingCube(
            "Sales", xml);

        testContext.assertQueryReturns(
            "select Head([Time2].[Quarter hours].Members, 3) on columns\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time2].[All Time2s].[1997].[Q1].[1].[367]}\n"
            + "{[Time2].[All Time2s].[1997].[Q1].[1].[368]}\n"
            + "{[Time2].[All Time2s].[1997].[Q1].[1].[369]}\n"
            + "Row #0: 348\n"
            + "Row #0: 635\n"
            + "Row #0: 589\n");

        // Check that can apply ParallelPeriod to a TimeUndefined level.
        testContext.assertAxisReturns(
            "PeriodsToDate([Time2].[Quarter hours], [Time2].[1997].[Q1].[1].[368])",
            "[Time2].[All Time2s].[1997].[Q1].[1].[368]");

        testContext.assertAxisReturns(
            "PeriodsToDate([Time2].[Half year], [Time2].[1997].[Q1].[1].[368])",
            "[Time2].[All Time2s].[1997].[Q1].[1].[367]\n"
            + "[Time2].[All Time2s].[1997].[Q1].[1].[368]");

        // Check that get an error if give invalid level type
        try {
            TestContext
                .createSubstitutingCube(
                    "Sales",
                    Util.replace(xml, "TimeUndefined", "TimeUnspecified"))
                .assertSimpleQuery();
            fail("expected error");
        } catch (Throwable e) {
            TestContext.checkThrowable(
                e,
                "Value 'TimeUnspecified' of attribute 'levelType' has illegal value 'TimeUnspecified'.  Legal values: {Regular, TimeYears, ");
        }
    }

    /**
     * Test for descriptions, captions and annotations of various schema
     * elements.
     */
    public void testCaptionDescriptionAndAnnotation() {
        final String schemaName = "Description schema";
        final String cubeName = "DescSales";
        final TestContext testContext = TestContext.create(
            "<Schema name=\"" + schemaName + "\"\n"
            + " description=\"Schema to test descriptions and captions\">\n"
            + "  <Annotations>\n"
            + "    <Annotation name=\"a\">Schema</Annotation>\n"
            + "    <Annotation name=\"b\">Xyz</Annotation>\n"
            + "  </Annotations>\n"
            + "  <Dimension name=\"Time\" type=\"TimeDimension\"\n"
            + "      caption=\"Time shared caption\"\n"
            + "      description=\"Time shared description\">\n"
            + "    <Annotations><Annotation name=\"a\">Time shared</Annotation></Annotations>\n"
            + "    <Hierarchy hasAll=\"false\" primaryKey=\"time_id\"\n"
            + "        caption=\"Time shared hierarchy caption\"\n"
            + "        description=\"Time shared hierarchy description\">\n"
            + "      <Table name=\"time_by_day\"/>\n"
            + "      <Level name=\"Year\" column=\"the_year\" type=\"Numeric\" uniqueMembers=\"true\"\n"
            + "          levelType=\"TimeYears\"/>\n"
            + "      <Level name=\"Quarter\" column=\"quarter\" uniqueMembers=\"false\"\n"
            + "          levelType=\"TimeQuarters\"/>\n"
            + "      <Level name=\"Month\" column=\"month_of_year\" uniqueMembers=\"false\" type=\"Numeric\"\n"
            + "          levelType=\"TimeMonths\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Cube name=\"" + cubeName + "\"\n"
            + "    description=\"Cube description\">\n"
            + "  <Annotations><Annotation name=\"a\">Cube</Annotation></Annotations>\n"
            + "  <Table name=\"sales_fact_1997\"/>\n"
            + "  <Dimension name=\"Store\" foreignKey=\"store_id\"\n"
            + "      caption=\"Dimension caption\"\n"
            + "      description=\"Dimension description\">\n"
            + "    <Annotations><Annotation name=\"a\">Dimension</Annotation></Annotations>\n"
            + "    <Hierarchy hasAll=\"true\" primaryKeyTable=\"store\" primaryKey=\"store_id\"\n"
            + "        caption=\"Hierarchy caption\"\n"
            + "        description=\"Hierarchy description\">\n"
            + "      <Annotations><Annotation name=\"a\">Hierarchy</Annotation></Annotations>\n"
            + "      <Join leftKey=\"region_id\" rightKey=\"region_id\">\n"
            + "        <Table name=\"store\"/>\n"
            + "        <Join leftKey=\"sales_district_id\" rightKey=\"promotion_id\">\n"
            + "          <Table name=\"region\"/>\n"
            + "          <Table name=\"promotion\"/>\n"
            + "        </Join>\n"
            + "      </Join>\n"
            + "      <Level name=\"Store Country\" table=\"store\" column=\"store_country\"\n"
            + "          description=\"Level description\""
            + "          caption=\"Level caption\">\n"
            + "        <Annotations><Annotation name=\"a\">Level</Annotation></Annotations>\n"
            + "      </Level>\n"
            + "      <Level name=\"Store Region\" table=\"region\" column=\"sales_region\" />\n"
            + "      <Level name=\"Store Name\" table=\"store\" column=\"store_name\" />\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <DimensionUsage name=\"Time1\"\n"
            + "    caption=\"Time usage caption\"\n"
            + "    description=\"Time usage description\"\n"
            + "    source=\"Time\" foreignKey=\"time_id\">\n"
            + "    <Annotations><Annotation name=\"a\">Time usage</Annotation></Annotations>\n"
            + "  </DimensionUsage>\n"
            + "  <DimensionUsage name=\"Time2\"\n"
            + "    source=\"Time\" foreignKey=\"time_id\"/>\n"
            + "<Measure name=\"Unit Sales\" column=\"unit_sales\"\n"
            + "    aggregator=\"sum\" formatString=\"Standard\"\n"
            + "    caption=\"Measure caption\"\n"
            + "    description=\"Measure description\">\n"
            + "  <Annotations><Annotation name=\"a\">Measure</Annotation></Annotations>\n"
            + "</Measure>\n"
            + "<CalculatedMember name=\"Foo\" dimension=\"Measures\" \n"
            + "    caption=\"Calc member caption\"\n"
            + "    description=\"Calc member description\">\n"
            + "    <Annotations><Annotation name=\"a\">Calc member</Annotation></Annotations>\n"
            + "    <Formula>[Measures].[Unit Sales] + 1</Formula>\n"
            + "    <CalculatedMemberProperty name=\"FORMAT_STRING\" value=\"$#,##0.00\"/>\n"
            + "  </CalculatedMember>\n"
            + "  <NamedSet name=\"Top Periods\"\n"
            + "      caption=\"Named set caption\"\n"
            + "      description=\"Named set description\">\n"
            + "    <Annotations><Annotation name=\"a\">Named set</Annotation></Annotations>\n"
            + "    <Formula>TopCount([Time1].MEMBERS, 5, [Measures].[Foo])</Formula>\n"
            + "  </NamedSet>\n"
            + "</Cube>\n"
            + "</Schema>");
        final Result result =
            testContext.executeQuery("select from [" + cubeName + "]");
        final Cube cube = result.getQuery().getCube();
        assertEquals("Cube description", cube.getDescription());
        checkAnnotations(cube.getAnnotationMap(), "a", "Cube");

        final Schema schema = cube.getSchema();
        checkAnnotations(schema.getAnnotationMap(), "a", "Schema", "b", "Xyz");

        final Dimension dimension = cube.getDimensions()[1];
        assertEquals("Dimension description", dimension.getDescription());
        assertEquals("Dimension caption", dimension.getCaption());
        checkAnnotations(dimension.getAnnotationMap(), "a", "Dimension");

        final Hierarchy hierarchy = dimension.getHierarchies()[0];
        assertEquals("Hierarchy description", hierarchy.getDescription());
        assertEquals("Hierarchy caption", hierarchy.getCaption());
        checkAnnotations(hierarchy.getAnnotationMap(), "a", "Hierarchy");

        final mondrian.olap.Level level = hierarchy.getLevels()[1];
        assertEquals("Level description", level.getDescription());
        assertEquals("Level caption", level.getCaption());
        checkAnnotations(level.getAnnotationMap(), "a", "Level");

        // Caption comes from the CAPTION member property, defaults to name.
        // Description comes from the DESCRIPTION member property.
        // Annotations are always empty for regular members.
        final List<Member> memberList =
            cube.getSchemaReader(null).getLevelMembers(level, false);
        final Member member = memberList.get(0);
        assertEquals("Canada", member.getName());
        assertEquals("Canada", member.getCaption());
        assertNull(member.getDescription());
        checkAnnotations(member.getAnnotationMap());

        // All member. Caption defaults to name; description is null.
        final Member allMember = member.getParentMember();
        assertEquals("All Stores", allMember.getName());
        assertEquals("All Stores", allMember.getCaption());
        assertNull(allMember.getDescription());

        // All level.
        final mondrian.olap.Level allLevel = hierarchy.getLevels()[0];
        assertEquals("(All)", allLevel.getName());
        assertNull(allLevel.getDescription());
        assertEquals(allLevel.getName(), allLevel.getCaption());
        checkAnnotations(allLevel.getAnnotationMap());

        // the first time dimension overrides the caption and description of the
        // shared time dimension
        final Dimension timeDimension = cube.getDimensions()[2];
        assertEquals("Time1", timeDimension.getName());
        assertEquals("Time usage description", timeDimension.getDescription());
        assertEquals("Time usage caption", timeDimension.getCaption());
        checkAnnotations(timeDimension.getAnnotationMap(), "a", "Time usage");

        // Time1 is a usage of a shared dimension Time.
        // Now look at the hierarchy usage within that dimension usage.
        // Because the dimension usage has a name, use that as a prefix for
        // name, caption and description of the hierarchy usage.
        final Hierarchy timeHierarchy = timeDimension.getHierarchies()[0];
        // The hierarchy in the shared dimension does not have a name, so the
        // hierarchy usage inherits the name of the dimension usage, Time1.
        assertEquals("Time1", timeHierarchy.getName());
        // The description is prefixed by the dimension usage name.
        assertEquals(
            "Time usage caption.Time shared hierarchy description",
            timeHierarchy.getDescription());
        // The hierarchy caption is prefixed by the caption of the dimension
        // usage.
        assertEquals(
            "Time usage caption.Time shared hierarchy caption",
            timeHierarchy.getCaption());
        // No annotations.
        checkAnnotations(timeHierarchy.getAnnotationMap());

        // the second time dimension does not overrides caption and description
        final Dimension time2Dimension = cube.getDimensions()[3];
        assertEquals("Time2", time2Dimension.getName());
        assertEquals(
            "Time shared description", time2Dimension.getDescription());
        assertEquals("Time shared caption", time2Dimension.getCaption());
        checkAnnotations(time2Dimension.getAnnotationMap());

        final Hierarchy time2Hierarchy = time2Dimension.getHierarchies()[0];
        // The hierarchy in the shared dimension does not have a name, so the
        // hierarchy usage inherits the name of the dimension usage, Time2.
        assertEquals("Time2", time2Hierarchy.getName());
        // The description is prefixed by the dimension usage name (because
        // dimension usage has no caption).
        assertEquals(
            "Time2.Time shared hierarchy description",
            time2Hierarchy.getDescription());
        // The hierarchy caption is prefixed by the dimension usage name
        // (because the dimension usage has no caption.
        assertEquals(
            "Time2.Time shared hierarchy caption",
            time2Hierarchy.getCaption());
        // No annotations.
        checkAnnotations(time2Hierarchy.getAnnotationMap());

        final Dimension measuresDimension = cube.getDimensions()[0];
        final Hierarchy measuresHierarchy =
            measuresDimension.getHierarchies()[0];
        final mondrian.olap.Level measuresLevel =
            measuresHierarchy.getLevels()[0];
        final SchemaReader schemaReader = cube.getSchemaReader(null);
        final List<Member> measures =
            schemaReader.getLevelMembers(measuresLevel, true);
        final Member measure = measures.get(0);
        assertEquals("Unit Sales", measure.getName());
        assertEquals("Measure caption", measure.getCaption());
        assertEquals("Measure description", measure.getDescription());
        assertEquals(
            measure.getDescription(),
            measure.getPropertyValue(Property.DESCRIPTION.name));
        assertEquals(
            measure.getCaption(),
            measure.getPropertyValue(Property.CAPTION.name));
        assertEquals(
            measure.getCaption(),
            measure.getPropertyValue(Property.MEMBER_CAPTION.name));
        checkAnnotations(measure.getAnnotationMap(), "a", "Measure");

        final Member calcMeasure = measures.get(1);
        assertEquals("Foo", calcMeasure.getName());
        assertEquals("Calc member caption", calcMeasure.getCaption());
        assertEquals("Calc member description", calcMeasure.getDescription());
        assertEquals(
            calcMeasure.getDescription(),
            calcMeasure.getPropertyValue(Property.DESCRIPTION.name));
        assertEquals(
            calcMeasure.getCaption(),
            calcMeasure.getPropertyValue(Property.CAPTION.name));
        assertEquals(
            calcMeasure.getCaption(),
            calcMeasure.getPropertyValue(Property.MEMBER_CAPTION.name));
        checkAnnotations(calcMeasure.getAnnotationMap(), "a", "Calc member");

        final NamedSet namedSet = cube.getNamedSets()[0];
        assertEquals("Top Periods", namedSet.getName());
        assertEquals("Named set caption", namedSet.getCaption());
        assertEquals("Named set description", namedSet.getDescription());
        checkAnnotations(namedSet.getAnnotationMap(), "a", "Named set");
    }

    private static void checkAnnotations(
        Map<String, Annotation> annotationMap,
        String... nameVal)
    {
        assertNotNull(annotationMap);
        assertEquals(0, nameVal.length % 2);
        assertEquals(nameVal.length / 2, annotationMap.size());
        int i = 0;
        for (Map.Entry<String, Annotation> entry : annotationMap.entrySet()) {
            assertEquals(nameVal[i++], entry.getKey());
            assertEquals(nameVal[i++], entry.getValue().getValue());
        }
    }

    /**
     * Implementation of {@link mondrian.olap.PropertyFormatter} that throws.
     */
    public static class DummyPropertyFormatter implements PropertyFormatter {
        public DummyPropertyFormatter() {
            throw new RuntimeException("oops");
        }

        public String formatProperty(
            Member member, String propertyName, Object propertyValue)
        {
            return null;
        }
    }
}

// End SchemaTest.java
