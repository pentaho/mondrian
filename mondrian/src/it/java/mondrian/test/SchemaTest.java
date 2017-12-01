/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2002-2017 Pentaho Corporation..  All rights reserved.
*/
package mondrian.test;

import junit.framework.Assert;
import mondrian.olap.*;
import mondrian.olap.Category;
import mondrian.olap.Hierarchy;
import mondrian.olap.Level;
import mondrian.rolap.RolapConnection;
import mondrian.rolap.RolapConnectionProperties;
import mondrian.rolap.RolapCube;
import mondrian.rolap.RolapSchema;
import mondrian.rolap.aggmatcher.AggTableManager;
import mondrian.spi.Dialect;
import mondrian.spi.PropertyFormatter;
import mondrian.util.Bug;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.*;
import org.apache.log4j.varia.LevelRangeFilter;

import org.olap4j.metadata.NamedList;

import java.io.*;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for various schema features.
 *
 * @see SchemaVersionTest
 * @see mondrian.rolap.SharedDimensionTest
 *
 * @author jhyde
 * @since August 7, 2006
 */
public class SchemaTest extends FoodMartTestCase {

    private static final String CUBES_AB =
            "<Cube name=\"CubeA\" defaultMeasure=\"Unit Sales\">\n"
            + " <Table name=\"sales_fact_1997\" alias=\"TableAlias\">\n"
            + "   <!-- count=32 -->\n"
            + "   <SQL dialect=\"mysql\">\n"
            + "     `TableAlias`.`promotion_id` = 108\n"
            + "   </SQL>\n"
            + " </Table>\n"
            + " <DimensionUsage name=\"Store Type\" source=\"Store Type\" foreignKey=\"store_id\"/>\n"
            + " <Measure name=\"Customer Count\" column=\"customer_id\" aggregator=\"distinct-count\" formatString=\"#,###\"/>\n"
            + " <Measure name=\"Fantastic Count for Different Types of Promotion\" column=\"promotion_id\" aggregator=\"count\" formatString=\"Standard\"/>\n"
            + "</Cube>\n"
            + "<Cube name=\"CubeB\" defaultMeasure=\"Unit Sales\">\n"
            + " <Table name=\"sales_fact_1997\" alias=\"TableAlias\">\n"
            + "   <!-- count=22 -->\n"
            + "   <SQL dialect=\"mysql\">\n"
            + "     `TableAlias`.`promotion_id` = 112\n"
            + "   </SQL>\n"
            + " </Table>\n"
            + " <DimensionUsage name=\"Store Type\" source=\"Store Type\" foreignKey=\"store_id\"/>\n"
            + " <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\" formatString=\"Standard\"/>\n"
            + " <Measure name=\"Fantastic Count for Different Types of Promotion\" column=\"promotion_id\" aggregator=\"count\" formatString=\"Standard\"/>\n"
            + "</Cube>";

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
        TestContext testContext = TestContext.instance().createSubstitutingCube(
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
        TestContext testContext = TestContext.instance().createSubstitutingCube(
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
            + "{[Gender with default].[M]}\n"
            + "Row #0: 135,215\n");
    }

    /**
     * Test case for the issue described in
     * <a href="http://forums.pentaho.com/showthread.php?p=190737">Pentaho
     * forum post 'wrong unique name for default member when hasAll=false'</a>.
     */
    public void testDefaultMemberName() {
        TestContext testContext = TestContext.instance().createSubstitutingCube(
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
        TestContext testContext = TestContext.instance().createSubstitutingCube(
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
            + "{[Gender with default].[F]}\n"
            + "Row #0: 131,558\n");
    }

    public void testHierarchyNoLevelsFails() {
        TestContext testContext = TestContext.instance().createSubstitutingCube(
            "Sales",
            "  <Dimension name='Gender no levels' foreignKey='customer_id'>\n"
            + "    <Hierarchy hasAll='true' primaryKey='customer_id'>\n"
            + "      <Table name='customer'/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>");
        testContext.assertQueryThrows(
            "select {[Gender no levels]} on columns from [Sales]",
            "Hierarchy '[Gender no levels]' must have at least one level.");
    }

    public void testHierarchyNonUniqueLevelsFails() {
        TestContext testContext = TestContext.instance().createSubstitutingCube(
            "Sales",
            "  <Dimension name='Gender dup levels' foreignKey='customer_id'>\n"
            + "    <Hierarchy hasAll='true' primaryKey='customer_id'>\n"
            + "      <Table name='customer'/>\n"
            + "      <Level name='Gender' column='gender' uniqueMembers='true' />\n"
            + "      <Level name='Gender' column='gender' uniqueMembers='true' />\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>");
        testContext.assertQueryThrows(
            "select {[Gender dup levels]} on columns from [Sales]",
            "Level names within hierarchy '[Gender dup levels]' are not unique; there is more than one level with name 'Gender'.");
    }

    /**
     * Tests a measure based on 'count'.
     */
    public void testCountMeasure() {
        TestContext testContext = TestContext.instance().createSubstitutingCube(
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
            + "{[Gender].[F]}\n"
            + "{[Gender].[M]}\n"
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
        TestContext testContext = TestContext.instance().createSubstitutingCube(
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
            "select [Yearly Income3].Children on 0 from [Sales]",
            "Internal error: while building member cache");
    }

    public void testPrimaryKeyTableNotFound() {
        TestContext testContext = TestContext.instance().createSubstitutingCube(
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
        TestContext testContext = TestContext.instance().createSubstitutingCube(
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
        TestContext testContext = TestContext.instance().createSubstitutingCube(
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
        TestContext testContext = TestContext.instance().createSubstitutingCube(
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
        TestContext testContext = TestContext.instance().createSubstitutingCube(
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
        TestContext testContext = TestContext.instance().createSubstitutingCube(
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
            + "{[Yearly Income].[$10K - $30K]}\n"
            + "Axis #2:\n"
            + "{[Yearly Income2].[$150K +]}\n"
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
            + "{[Yearly Income].[$10K - $30K], [Yearly Income2].[$10K - $30K]}\n"
            + "{[Yearly Income].[$10K - $30K], [Yearly Income2].[$110K - $130K]}\n"
            + "{[Yearly Income].[$10K - $30K], [Yearly Income2].[$130K - $150K]}\n"
            + "{[Yearly Income].[$10K - $30K], [Yearly Income2].[$150K +]}\n"
            + "{[Yearly Income].[$10K - $30K], [Yearly Income2].[$30K - $50K]}\n"
            + "{[Yearly Income].[$10K - $30K], [Yearly Income2].[$50K - $70K]}\n"
            + "{[Yearly Income].[$10K - $30K], [Yearly Income2].[$70K - $90K]}\n"
            + "{[Yearly Income].[$10K - $30K], [Yearly Income2].[$90K - $110K]}\n"
            + "{[Yearly Income].[$110K - $130K], [Yearly Income2].[$10K - $30K]}\n"
            + "{[Yearly Income].[$110K - $130K], [Yearly Income2].[$110K - $130K]}\n"
            + "{[Yearly Income].[$110K - $130K], [Yearly Income2].[$130K - $150K]}\n"
            + "{[Yearly Income].[$110K - $130K], [Yearly Income2].[$150K +]}\n"
            + "{[Yearly Income].[$110K - $130K], [Yearly Income2].[$30K - $50K]}\n"
            + "{[Yearly Income].[$110K - $130K], [Yearly Income2].[$50K - $70K]}\n"
            + "{[Yearly Income].[$110K - $130K], [Yearly Income2].[$70K - $90K]}\n"
            + "{[Yearly Income].[$110K - $130K], [Yearly Income2].[$90K - $110K]}\n"
            + "{[Yearly Income].[$130K - $150K], [Yearly Income2].[$10K - $30K]}\n"
            + "{[Yearly Income].[$130K - $150K], [Yearly Income2].[$110K - $130K]}\n"
            + "{[Yearly Income].[$130K - $150K], [Yearly Income2].[$130K - $150K]}\n"
            + "{[Yearly Income].[$130K - $150K], [Yearly Income2].[$150K +]}\n"
            + "{[Yearly Income].[$130K - $150K], [Yearly Income2].[$30K - $50K]}\n"
            + "{[Yearly Income].[$130K - $150K], [Yearly Income2].[$50K - $70K]}\n"
            + "{[Yearly Income].[$130K - $150K], [Yearly Income2].[$70K - $90K]}\n"
            + "{[Yearly Income].[$130K - $150K], [Yearly Income2].[$90K - $110K]}\n"
            + "{[Yearly Income].[$150K +], [Yearly Income2].[$10K - $30K]}\n"
            + "{[Yearly Income].[$150K +], [Yearly Income2].[$110K - $130K]}\n"
            + "{[Yearly Income].[$150K +], [Yearly Income2].[$130K - $150K]}\n"
            + "{[Yearly Income].[$150K +], [Yearly Income2].[$150K +]}\n"
            + "{[Yearly Income].[$150K +], [Yearly Income2].[$30K - $50K]}\n"
            + "{[Yearly Income].[$150K +], [Yearly Income2].[$50K - $70K]}\n"
            + "{[Yearly Income].[$150K +], [Yearly Income2].[$70K - $90K]}\n"
            + "{[Yearly Income].[$150K +], [Yearly Income2].[$90K - $110K]}\n"
            + "{[Yearly Income].[$30K - $50K], [Yearly Income2].[$10K - $30K]}\n"
            + "{[Yearly Income].[$30K - $50K], [Yearly Income2].[$110K - $130K]}\n"
            + "{[Yearly Income].[$30K - $50K], [Yearly Income2].[$130K - $150K]}\n"
            + "{[Yearly Income].[$30K - $50K], [Yearly Income2].[$150K +]}\n"
            + "{[Yearly Income].[$30K - $50K], [Yearly Income2].[$30K - $50K]}\n"
            + "{[Yearly Income].[$30K - $50K], [Yearly Income2].[$50K - $70K]}\n"
            + "{[Yearly Income].[$30K - $50K], [Yearly Income2].[$70K - $90K]}\n"
            + "{[Yearly Income].[$30K - $50K], [Yearly Income2].[$90K - $110K]}\n"
            + "{[Yearly Income].[$50K - $70K], [Yearly Income2].[$10K - $30K]}\n"
            + "{[Yearly Income].[$50K - $70K], [Yearly Income2].[$110K - $130K]}\n"
            + "{[Yearly Income].[$50K - $70K], [Yearly Income2].[$130K - $150K]}\n"
            + "{[Yearly Income].[$50K - $70K], [Yearly Income2].[$150K +]}\n"
            + "{[Yearly Income].[$50K - $70K], [Yearly Income2].[$30K - $50K]}\n"
            + "{[Yearly Income].[$50K - $70K], [Yearly Income2].[$50K - $70K]}\n"
            + "{[Yearly Income].[$50K - $70K], [Yearly Income2].[$70K - $90K]}\n"
            + "{[Yearly Income].[$50K - $70K], [Yearly Income2].[$90K - $110K]}\n"
            + "{[Yearly Income].[$70K - $90K], [Yearly Income2].[$10K - $30K]}\n"
            + "{[Yearly Income].[$70K - $90K], [Yearly Income2].[$110K - $130K]}\n"
            + "{[Yearly Income].[$70K - $90K], [Yearly Income2].[$130K - $150K]}\n"
            + "{[Yearly Income].[$70K - $90K], [Yearly Income2].[$150K +]}\n"
            + "{[Yearly Income].[$70K - $90K], [Yearly Income2].[$30K - $50K]}\n"
            + "{[Yearly Income].[$70K - $90K], [Yearly Income2].[$50K - $70K]}\n"
            + "{[Yearly Income].[$70K - $90K], [Yearly Income2].[$70K - $90K]}\n"
            + "{[Yearly Income].[$70K - $90K], [Yearly Income2].[$90K - $110K]}\n"
            + "{[Yearly Income].[$90K - $110K], [Yearly Income2].[$10K - $30K]}\n"
            + "{[Yearly Income].[$90K - $110K], [Yearly Income2].[$110K - $130K]}\n"
            + "{[Yearly Income].[$90K - $110K], [Yearly Income2].[$130K - $150K]}\n"
            + "{[Yearly Income].[$90K - $110K], [Yearly Income2].[$150K +]}\n"
            + "{[Yearly Income].[$90K - $110K], [Yearly Income2].[$30K - $50K]}\n"
            + "{[Yearly Income].[$90K - $110K], [Yearly Income2].[$50K - $70K]}\n"
            + "{[Yearly Income].[$90K - $110K], [Yearly Income2].[$70K - $90K]}\n"
            + "{[Yearly Income].[$90K - $110K], [Yearly Income2].[$90K - $110K]}\n"
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
        TestContext testContext = TestContext.instance().createSubstitutingCube(
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
        TestContext testContext = TestContext.instance().createSubstitutingCube(
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
            + "{[Yearly Income].[$10K - $30K]}\n"
            + "Axis #2:\n"
            + "{[Yearly Income2].[$150K +]}\n"
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
            + "{[Yearly Income].[$10K - $30K], [Yearly Income2].[$10K - $30K]}\n"
            + "{[Yearly Income].[$110K - $130K], [Yearly Income2].[$110K - $130K]}\n"
            + "{[Yearly Income].[$130K - $150K], [Yearly Income2].[$130K - $150K]}\n"
            + "{[Yearly Income].[$150K +], [Yearly Income2].[$150K +]}\n"
            + "{[Yearly Income].[$30K - $50K], [Yearly Income2].[$30K - $50K]}\n"
            + "{[Yearly Income].[$50K - $70K], [Yearly Income2].[$50K - $70K]}\n"
            + "{[Yearly Income].[$70K - $90K], [Yearly Income2].[$70K - $90K]}\n"
            + "{[Yearly Income].[$90K - $110K], [Yearly Income2].[$90K - $110K]}\n"
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

        final TestContext testContext = TestContext.instance().create(
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
            + "{[Customers].[USA].[South West]}\n"
            + "Axis #2:\n"
            + "{[Store].[MyHierarchy].[Mexico]}\n"
            + "Row #0: 51,298\n");
    }

    /**
     * test hierarchy with slightly different join path to fact table than
     * first hierarchy. tables from first and second hierarchy should contain
     * the same join aliases to the fact table.
     */
    public void testSnowflakeHierarchyValidationNotNeeded2() {
        final TestContext testContext = TestContext.instance().create(
            null,
            "<Cube name=\"AliasedDimensionsTesting\" defaultMeasure=\"Supply Time\">\n"
            + "  <Table name=\"sales_fact_1997\">\n"
            + "    <AggExclude pattern=\"agg_lc_06_sales_fact_1997\"/>\n"
            + "  </Table>"
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
            + "{[Customers].[USA].[South West]}\n"
            + "Axis #2:\n"
            + "{[Store].[MyHierarchy].[USA].[South West]}\n"
            + "Row #0: 72,631\n");
    }

    /**
     * WG: This no longer throws an exception, it is now possible
     *
     * Tests two dimensions using same table (via different join paths).
     * both using a table alias.
     */
    public void testDimensionsShareJoinTable() {
        final TestContext testContext = TestContext.instance().create(
            null,
            "<Cube name=\"AliasedDimensionsTesting\" defaultMeasure=\"Supply Time\">\n"
            + "  <Table name=\"sales_fact_1997\">\n"
            + "    <AggExclude pattern=\"agg_lc_06_sales_fact_1997\"/>\n"
            + "  </Table>"
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
            + "{[Customers].[USA].[South West]}\n"
            + "Axis #2:\n"
            + "{[Store].[USA].[South West]}\n"
            + "Row #0: 72,631\n");
    }

    /**
     * Tests two dimensions using same table (via different join paths).
     * both using a table alias.
     */
    public void testDimensionsShareJoinTableOneAlias() {
        final TestContext testContext = TestContext.instance().create(
            null,
            "<Cube name=\"AliasedDimensionsTesting\" defaultMeasure=\"Supply Time\">\n"
            + "  <Table name=\"sales_fact_1997\">\n"
            + "    <AggExclude pattern=\"agg_lc_06_sales_fact_1997\"/>\n"
            + "  </Table>"
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
            + "{[Customers].[USA].[South West]}\n"
            + "Axis #2:\n"
            + "{[Store].[USA].[South West]}\n"
            + "Row #0: 72,631\n");
    }

    /**
     * Tests two dimensions using same table (via different join paths).
     * both using a table alias.
     */
    public void testDimensionsShareJoinTableTwoAliases() {
        final TestContext testContext = TestContext.instance().create(
            null,
            "<Cube name=\"AliasedDimensionsTesting\" defaultMeasure=\"Supply Time\">\n"
            + "  <Table name=\"sales_fact_1997\">\n"
            + "    <AggExclude pattern=\"agg_lc_06_sales_fact_1997\"/>\n"
            + "  </Table>"
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
            + "{[Customers].[USA].[South West]}\n"
            + "Axis #2:\n"
            + "{[Store].[USA].[South West]}\n"
            + "Row #0: 72,631\n");
    }

    /**
     * Tests two dimensions using same table (via different join paths).
     * both using a table alias.
     */
    public void testTwoAliasesDimensionsShareTable() {
        final TestContext testContext = TestContext.instance().create(
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
            + "{[StoreB].[USA]}\n"
            + "Axis #2:\n"
            + "{[StoreA].[USA]}\n"
            + "Row #0: 10,425\n");
    }

    /**
     * Tests two dimensions using same table with same foreign key.
     * both using a table alias.
     */
    public void testTwoAliasesDimensionsShareTableSameForeignKeys() {
        final TestContext testContext = TestContext.instance().create(
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
            + "{[StoreB].[USA]}\n"
            + "Axis #2:\n"
            + "{[StoreA].[USA]}\n"
            + "Row #0: 10,425\n");
    }

    /**
     * Test Multiple DimensionUsages on same Dimension.
     */
    public void testMultipleDimensionUsages() {
        final TestContext testContext = TestContext.instance().create(
            null,
            "<Cube name=\"Sales Two Dimensions\">\n"
            + "  <Table name=\"sales_fact_1997\">\n"
            + "    <AggExclude pattern=\"agg_c_10_sales_fact_1997\"/>\n"
            + "    <AggExclude pattern=\"agg_g_ms_pcat_sales_fact_1997\"/>\n"
            + "  </Table>\n"
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
     */
    public void testMultipleDimensionHierarchyCaptionUsages() {
        final TestContext testContext = TestContext.instance().create(
            null,
            "<Cube name=\"Sales Two Dimensions\">\n"
            + "  <Table name=\"sales_fact_1997\">\n"
            + "    <AggExclude pattern=\"agg_c_10_sales_fact_1997\"/>\n"
            + "    <AggExclude pattern=\"agg_g_ms_pcat_sales_fact_1997\"/>\n"
            + "  </Table>\n"
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
        final TestContext testContext = TestContext.instance().create(
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
            + "{[Store].[USA]}\n"
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
            + "{[Store].[USA]}\n"
            + "Axis #2:\n"
            + "{[Time].[1997].[Q1]}\n"
            + "Row #0: 66,291\n");
    }

    /**
     * Test DimensionUsage level attribute
     */
    public void testDimensionUsageLevel() {
        final TestContext testContext = TestContext.instance().create(
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
            + "{[Store].[Canada].[BC]}\n"
            + "{[Store].[Mexico].[DF]}\n"
            + "{[Store].[Mexico].[Guerrero]}\n"
            + "{[Store].[Mexico].[Jalisco]}\n"
            + "{[Store].[Mexico].[Veracruz]}\n"
            + "{[Store].[Mexico].[Yucatan]}\n"
            + "{[Store].[Mexico].[Zacatecas]}\n"
            + "{[Store].[USA].[CA]}\n"
            + "{[Store].[USA].[OR]}\n"
            + "{[Store].[USA].[WA]}\n"
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
        final TestContext testContext = TestContext.instance().create(
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
        final TestContext testContext = TestContext.instance().create(
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
     * Tests a cube whose fact table is a &lt;View&gt; element as well as a
     * degenerate dimension.
     */
    public void testViewDegenerateDims() {
        final TestContext testContext = TestContext.instance().create(
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
            + "  <Dimension name=\"Warehouse\">\n"
            + "    <Hierarchy hasAll=\"true\"> \n"
            + "      <View alias=\"FACT\">\n"
            + "        <SQL dialect=\"generic\">\n"
            + "         <![CDATA[select * from \"inventory_fact_1997\" as \"FOOBAR\"]]>\n"
            + "        </SQL>\n"
            + "        <SQL dialect=\"oracle\">\n"
            + "         <![CDATA[select * from \"inventory_fact_1997\" \"FOOBAR\"]]>\n"
            + "        </SQL>\n"
            + "        <SQL dialect=\"mysql\">\n"
            + "         <![CDATA[select * from `inventory_fact_1997` as `FOOBAR`]]>\n"
            + "        </SQL>\n"
            + "        <SQL dialect=\"infobright\">\n"
            + "         <![CDATA[select * from `inventory_fact_1997` as `FOOBAR`]]>\n"
            + "        </SQL>\n"
            + "      </View>\n"
            + "      <Level name=\"Warehouse ID\" column=\"warehouse_id\"\n"
            + "          uniqueMembers=\"true\" type=\"Numeric\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Measure name=\"Warehouse Cost\" column=\"warehouse_cost\" aggregator=\"sum\"/>\n"
            + "  <Measure name=\"Warehouse Sales\" column=\"warehouse_sales\" aggregator=\"sum\"/>\n"
            + "</Cube>", null, null, null, null);

        testContext.assertQueryReturns(
            "select\n"
            + " NON EMPTY {[Time].[1997], [Time].[1997].[Q3]} on columns,\n"
            + " NON EMPTY {[Store].[USA].Children} on rows\n"
            + "From [Warehouse (based on view)]\n"
            + "where [Warehouse].[2]",
            "Axis #0:\n"
            + "{[Warehouse].[2]}\n"
            + "Axis #1:\n"
            + "{[Time].[1997]}\n"
            + "Axis #2:\n"
            + "{[Store].[USA].[WA]}\n"
            + "Row #0: 917.554\n");
    }

    /**
     * Tests a cube whose fact table is a &lt;View&gt; element.
     */
    public void testViewFactTable() {
        final TestContext testContext = TestContext.instance().create(
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
            + "{[Store].[USA].[CA]}\n"
            + "{[Store].[USA].[OR]}\n"
            + "{[Store].[USA].[WA]}\n"
            + "Row #0: 25,789.087\n"
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
        final TestContext testContext = TestContext.instance().create(
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
            + "{[Store Type].[Deluxe Supermarket]}\n"
            + "{[Store Type].[Gourmet Supermarket]}\n"
            + "{[Store Type].[HeadQuarters]}\n"
            + "{[Store Type].[Mid-Size Grocery]}\n"
            + "{[Store Type].[Small Grocery]}\n"
            + "{[Store Type].[Supermarket]}\n"
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
        TestContext testContext = TestContext.instance().createSubstitutingCube(
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
            + "{[Gender].[M]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Customer Count]}\n"
            + "{[Measures].[Customer Count2]}\n"
            + "{[Measures].[Half Customer Count]}\n"
            + "Axis #2:\n"
            + "{[Store].[USA].[CA]}\n"
            + "{[Store].[USA].[OR]}\n"
            + "{[Store].[USA].[WA]}\n"
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
        TestContext testContext = TestContext.instance().createSubstitutingCube(
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
        propSaver.setAtLeast(logger, org.apache.log4j.Level.WARN);
        final StringWriter sw = new StringWriter();
        final Appender appender =
            new WriterAppender(new SimpleLayout(), sw);
        final LevelRangeFilter filter = new LevelRangeFilter();
        filter.setLevelMin(org.apache.log4j.Level.WARN);
        appender.addFilter(filter);
        logger.addAppender(appender);
        try {
            final TestContext testContext = TestContext.instance().withSchema(
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
                + "    <AggExclude pattern=\"agg_lc_100_sales_fact_1997\"/>\n"
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
            + "WARN - Recognizer.checkUnusedColumns: Candidate aggregate table 'agg_c_10_sales_fact_1997' for fact table 'sales_fact_1997' has a column 'unit_sales' with unknown usage.\n",
            sw.toString());
    }

    public void testUnknownUsages1() {
        if (!MondrianProperties.instance().ReadAggregates.get()) {
            return;
        }
        final Logger logger = Logger.getLogger(AggTableManager.class);
        propSaver.setAtLeast(logger, org.apache.log4j.Level.WARN);
        final StringWriter sw = new StringWriter();
        final Appender appender =
            new WriterAppender(new SimpleLayout(), sw);
        final LevelRangeFilter filter = new LevelRangeFilter();
        filter.setLevelMin(org.apache.log4j.Level.WARN);
        appender.addFilter(filter);
        logger.addAppender(appender);
        try {
            final TestContext testContext = TestContext.instance().withSchema(
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
            TestContext.instance().createSubstitutingCube(
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
            TestContext.instance().create(
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
        TestContext testContext = TestContext.instance().createSubstitutingCube(
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
            + "{[Store2].[2], [Product].[All Products]}\n"
            + "{[Store2].[3], [Product].[All Products]}\n"
            + "{[Store2].[6], [Product].[All Products]}\n"
            + "{[Store2].[7], [Product].[All Products]}\n"
            + "{[Store2].[11], [Product].[All Products]}\n"
            + "{[Store2].[13], [Product].[All Products]}\n"
            + "{[Store2].[14], [Product].[All Products]}\n"
            + "{[Store2].[15], [Product].[All Products]}\n"
            + "{[Store2].[16], [Product].[All Products]}\n"
            + "{[Store2].[17], [Product].[All Products]}\n"
            + "{[Store2].[22], [Product].[All Products]}\n"
            + "{[Store2].[23], [Product].[All Products]}\n"
            + "{[Store2].[24], [Product].[All Products]}\n"
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
        final TestContext testContext = TestContext.instance().create(
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
        final TestContext testContext = TestContext.instance().create(
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
            + "{[Product].[Drink]}\n"
            + "{[Product].[Food]}\n"
            + "{[Product].[Non-Consumable]}\n"
            + "Row #0: 24,597\n"
            + "Row #0: 191,940\n"
            + "Row #0: 50,236\n");
    }

    public void testCubeHasFact() {
        final TestContext testContext = TestContext.instance().create(
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
        final TestContext testContext = TestContext.instance().create(
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
            testContext.getOlap4jConnection().getOlapSchema().getCubes();
        final org.olap4j.metadata.Cube cube = cubes.get("Cube with caption");
        assertEquals("Cube with name", cube.getCaption());
        final org.olap4j.metadata.Cube cube2 =
            cubes.get("Warehouse and Sales with caption");
        assertEquals("Warehouse and Sales with name", cube2.getCaption());
    }

    public void testCubeWithNoDimensions() {
        final TestContext testContext = TestContext.instance().create(
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
        final TestContext testContext = TestContext.instance().create(
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
        // Does not fail with
        //    "Hierarchy '[Measures]' is invalid (has no members)"
        // because of the implicit [Fact Count] measure.
        testContext.assertSimpleQuery();
    }

    public void testCubeWithOneCalcMeasure() {
        final TestContext testContext = TestContext.instance().create(
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

        // Because there are no explicit stored measures, the default measure is
        // the implicit stored measure, [Fact Count]. Stored measures, even
        // non-visible ones, come before calculated measures.
        testContext.assertQueryReturns(
            "select {[Measures]} on columns from [OneCalcMeasure]\n"
            + "where [Promotion Media].[TV]",
            "Axis #0:\n"
            + "{[Promotion Media].[TV]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Fact Count]}\n"
            + "Row #0: 1,171\n");
    }

    /**
     * Test case for feature
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-960">MONDRIAN-960,
     * "Ability to define non-measure calculated members in a cube under a
     * specifc parent"</a>.
     */
    public void testCalcMemberInCube() {
        final TestContext testContext =
            TestContext.instance().createSubstitutingCube(
                "Sales",
                null,
                null,
                "<CalculatedMember\n"
                + "      name='SF and LA'\n"
                + "      hierarchy='[Store]'\n"
                + "      parent='[Store].[USA].[CA]'>\n"
                + "  <Formula>\n"
                + "    [Store].[USA].[CA].[San Francisco]\n"
                + "    + [Store].[USA].[CA].[Los Angeles]\n"
                + "  </Formula>\n"
                + "</CalculatedMember>",
                null);

        // Because there are no explicit stored measures, the default measure is
        // the implicit stored measure, [Fact Count]. Stored measures, even
        // non-visible ones, come before calculated measures.
        testContext.assertQueryReturns(
            "select {[Store].[USA].[CA].[SF and LA]} on columns from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[USA].[CA].[SF and LA]}\n"
            + "Row #0: 27,780\n");

        // Now access the same member using a path that is not its unique name.
        // Only works with new name resolver (if ssas = true).
        if (MondrianProperties.instance().SsasCompatibleNaming.get()) {
            testContext.assertQueryReturns(
                "select {[Store].[All Stores].[USA].[CA].[SF and LA]} on columns from [Sales]",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Store].[USA].[CA].[SF and LA]}\n"
                + "Row #0: 27,780\n");
        }

        // Test where hierarchy & dimension both specified. should fail
        try {
            final TestContext testContextFail1 =
                TestContext.instance().createSubstitutingCube(
                    "Sales",
                    null,
                    null,
                    "<CalculatedMember\n"
                    + "      name='SF and LA'\n"
                    + "      hierarchy='[Store]'\n"
                    + "      dimension='[Store]'\n"
                    + "      parent='[Store].[USA].[CA]'>\n"
                    + "  <Formula>\n"
                    + "    [Store].[USA].[CA].[San Francisco]\n"
                    + "    + [Store].[USA].[CA].[Los Angeles]\n"
                    + "  </Formula>\n"
                    + "</CalculatedMember>",
                    null);
            testContextFail1.assertQueryReturns(
                "select {[Store].[All Stores].[USA].[CA].[SF and LA]} on columns from [Sales]",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Store].[USA].[CA].[SF and LA]}\n"
                + "Row #0: 27,780\n");
            fail();
        } catch (MondrianException e) {
            assertTrue(
                e.getMessage().contains(
                    "Cannot specify both a dimension and hierarchy"
                    + " for calculated member 'SF and LA' in cube 'Sales'"));
        }

        // test where hierarchy is not uname of valid hierarchy. should fail
        try {
            final TestContext testContextFail1 =
                TestContext.instance().createSubstitutingCube(
                    "Sales",
                    null,
                    null,
                    "<CalculatedMember\n"
                    + "      name='SF and LA'\n"
                    + "      hierarchy='[Bacon]'\n"
                    + "      parent='[Store].[USA].[CA]'>\n"
                    + "  <Formula>\n"
                    + "    [Store].[USA].[CA].[San Francisco]\n"
                    + "    + [Store].[USA].[CA].[Los Angeles]\n"
                    + "  </Formula>\n"
                    + "</CalculatedMember>",
                    null);
            testContextFail1.assertQueryReturns(
                "select {[Store].[All Stores].[USA].[CA].[SF and LA]} on columns from [Sales]",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Store].[USA].[CA].[SF and LA]}\n"
                + "Row #0: 27,780\n");
            fail();
        } catch (MondrianException e) {
            assertTrue(
                e.getMessage().contains(
                    "Unknown dimension '[Bacon]' for calculated member"
                    + " 'SF and LA' in cube 'Sales'"));
        }

        // test where formula is invalid. should fail
        try {
            final TestContext testContextFail1 =
                TestContext.instance().createSubstitutingCube(
                    "Sales",
                    null,
                    null,
                    "<CalculatedMember\n"
                    + "      name='SF and LA'\n"
                    + "      hierarchy='[Store]'\n"
                    + "      parent='[Store].[USA].[CA]'>\n"
                    + "  <Formula>\n"
                    + "    Baconating!\n"
                    + "  </Formula>\n"
                    + "</CalculatedMember>",
                    null);
            testContextFail1.assertQueryReturns(
                "select {[Store].[All Stores].[USA].[CA].[SF and LA]} on columns from [Sales]",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Store].[USA].[CA].[SF and LA]}\n"
                + "Row #0: 27,780\n");
            fail();
        } catch (MondrianException e) {
            assertTrue(
                e.getMessage().contains(
                    "Named set in cube 'Sales' has bad formula"));
        }

        // Test where parent is invalid. should fail
        try {
            final TestContext testContextFail1 =
                TestContext.instance().createSubstitutingCube(
                    "Sales",
                    null,
                    null,
                    "<CalculatedMember\n"
                    + "      name='SF and LA'\n"
                    + "      hierarchy='[Store]'\n"
                    + "      parent='[Store].[USA].[CA].[Baconville]'>\n"
                    + "  <Formula>\n"
                    + "    [Store].[USA].[CA].[San Francisco]\n"
                    + "    + [Store].[USA].[CA].[Los Angeles]\n"
                    + "  </Formula>\n"
                    + "</CalculatedMember>",
                    null);
            testContextFail1.assertQueryReturns(
                "select {[Store].[All Stores].[USA].[CA].[SF and LA]} on columns from [Sales]",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Store].[USA].[CA].[SF and LA]}\n"
                + "Row #0: 27,780\n");
            fail();
        } catch (MondrianException e) {
            assertTrue(
                e.getMessage().contains(
                    "Cannot find a parent with name '[Store].[USA].[CA]"
                    + ".[Baconville]' for calculated member 'SF and LA'"
                    + " in cube 'Sales'"));
        }

        // test where parent is not in same hierarchy as hierarchy. should fail
        try {
            final TestContext testContextFail1 =
                TestContext.instance().createSubstitutingCube(
                    "Sales",
                    null,
                    null,
                    "<CalculatedMember\n"
                    + "      name='SF and LA'\n"
                    + "      hierarchy='[Store Type]'\n"
                    + "      parent='[Store].[USA].[CA]'>\n"
                    + "  <Formula>\n"
                    + "    [Store].[USA].[CA].[San Francisco]\n"
                    + "    + [Store].[USA].[CA].[Los Angeles]\n"
                    + "  </Formula>\n"
                    + "</CalculatedMember>",
                    null);
            testContextFail1.assertQueryReturns(
                "select {[Store].[All Stores].[USA].[CA].[SF and LA]} on columns from [Sales]",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Store].[USA].[CA].[SF and LA]}\n"
                + "Row #0: 27,780\n");
            fail();
        } catch (MondrianException e) {
            assertTrue(
                e.getMessage().contains(
                    "The calculated member 'SF and LA' in cube 'Sales'"
                    + " is defined for hierarchy '[Store Type]' but its"
                    + " parent member is not part of that hierarchy"));
        }

        // test where calc member has no formula (formula attribute or
        //   embedded element); should fail
        try {
            final TestContext testContextFail1 =
                TestContext.instance().createSubstitutingCube(
                    "Sales",
                    null,
                    null,
                    "<CalculatedMember\n"
                    + "      name='SF and LA'\n"
                    + "      hierarchy='[Store]'\n"
                    + "      parent='[Store].[USA].[CA]'>\n"
                    + "  <Formula>\n"
                    + "  </Formula>\n"
                    + "</CalculatedMember>",
                    null);
            testContextFail1.assertQueryReturns(
                "select {[Store].[All Stores].[USA].[CA].[SF and LA]} on columns from [Sales]",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Store].[USA].[CA].[SF and LA]}\n"
                + "Row #0: 27,780\n");
            fail();
        } catch (MondrianException e) {
            assertTrue(
                e.getMessage().contains(
                    "Named set in cube 'Sales' has bad formula"));
        }
    }

    /**
     * this test triggers an exception out of the aggregate table manager
     */
    public void testAggTableSupportOfSharedDims() {
        if (Bug.BugMondrian361Fixed) {
            final TestContext testContext = TestContext.instance().create(
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

            // turn off caching
            propSaver.set(props.DisableCaching, true);

            // re-read aggregates
            propSaver.set(props.UseAggregates, true);
            propSaver.set(props.ReadAggregates, false);
            propSaver.set(props.ReadAggregates, true);

            // force reloading of aggregates, which currently throws an
            // exception
        }
    }

    /**
     * Verifies that RolapHierarchy.tableExists() supports views.
     */
    public void testLevelTableAttributeAsView() {
        final TestContext testContext = TestContext.instance().create(
            null,
            "<Cube name=\"GenderCube\">\n"
            + "  <Table name=\"sales_fact_1997\">\n"
            + "    <AggExclude pattern=\"agg_g_ms_pcat_sales_fact_1997\"/>\n"
            + "  </Table>\n"
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
            + "[Gender2].[F]\n"
            + "[Gender2].[M]",
            TestContext.toString(
                result.getAxes()[0].getPositions()));
    }

    public void testInvalidSchemaAccess() {
        final TestContext testContext = TestContext.instance().create(
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
        final TestContext testContext = TestContext.instance().create(
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
        final TestContext testContext = TestContext.instance().create(
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
        final TestContext testContext = TestContext.instance().create(
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
        final TestContext testContext = TestContext.instance().create(
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
        TestContext testContext = TestContext.instance().createSubstitutingCube(
            "Warehouse and Sales",
            null, null, null,
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
            + "{[Store].[USA]}\n"
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
            + "{[Store].[USA]}\n"
            + "{[Store].[Total Non CA State]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Row #0: 266,773\n"
            + "Row #0: 192,025\n");
    }

    public void testVirtualCubeNamedSetSupportInSchemaError() {
        TestContext testContext = TestContext.instance().createSubstitutingCube(
            "Warehouse and Sales",
            null, null, null,
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
                + "{[Store].[USA]}\n"
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
            TestContext.instance().createSubstitutingCube(
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
        final TestContext testContext =
            TestContext.instance().withSchema(schema);
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
        case MARIADB:
        case MYSQL:
            break;
        default:
            // Not all databases support binary literals (e.g. X'AB01'). Only
            // Derby returns them as byte[] values from its JDBC driver and
            // therefore experiences bug MONDRIAN-413.
            return;
        }
        TestContext testContext = TestContext.instance().createSubstitutingCube(
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

    /**
     * Test case for the Level@internalType attribute.
     *
     * <p>See bug <a href="http://jira.pentaho.com/browse/MONDRIAN-896">
     * MONDRIAN-896, "Oracle integer columns overflow if value &gt;>2^31"</a>.
     */
    public void testLevelInternalType() {
        // One of the keys is larger than Integer.MAX_VALUE (2 billion), so
        // will only work if we use long values.
        TestContext testContext = TestContext.instance().createSubstitutingCube(
            "Sales",
            "  <Dimension name=\"Big numbers\" foreignKey=\"promotion_id\">\n"
            + "    <Hierarchy hasAll=\"false\" primaryKey=\"id\">\n"
            + "      <InlineTable alias=\"t\">\n"
            + "        <ColumnDefs>\n"
            + "          <ColumnDef name=\"id\" type=\"Integer\"/>\n"
            + "          <ColumnDef name=\"big_num\" type=\"Integer\"/>\n"
            + "          <ColumnDef name=\"name\" type=\"String\"/>\n"
            + "        </ColumnDefs>\n"
            + "        <Rows>\n"
            + "          <Row>\n"
            + "            <Value column=\"id\">0</Value>\n"
            + "            <Value column=\"big_num\">1234</Value>\n"
            + "            <Value column=\"name\">Ben</Value>\n"
            + "          </Row>\n"
            + "          <Row>\n"
            + "            <Value column=\"id\">519</Value>\n"
            + "            <Value column=\"big_num\">1234567890123</Value>\n"
            + "            <Value column=\"name\">Bill</Value>\n"
            + "          </Row>\n"
            + "        </Rows>\n"
            + "      </InlineTable>\n"
            + "      <Level name=\"Level1\" column=\"big_num\" internalType=\"long\"/>\n"
            + "      <Level name=\"Level2\" column=\"id\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n");
        testContext.assertQueryReturns(
            "select {[Big numbers].members} on 0 from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Big numbers].[1234]}\n"
            + "{[Big numbers].[1234].[0]}\n"
            + "{[Big numbers].[1234567890123]}\n"
            + "{[Big numbers].[1234567890123].[519]}\n"
            + "Row #0: 195,448\n"
            + "Row #0: 195,448\n"
            + "Row #0: 739\n"
            + "Row #0: 739\n");
    }

    /**
     * Negative test for Level@internalType attribute.
     */
    public void testLevelInternalTypeErr() {
        TestContext testContext = TestContext.instance().createSubstitutingCube(
            "Sales",
            "  <Dimension name=\"Big numbers\" foreignKey=\"promotion_id\">\n"
            + "    <Hierarchy hasAll=\"false\" primaryKey=\"id\">\n"
            + "      <InlineTable alias=\"t\">\n"
            + "        <ColumnDefs>\n"
            + "          <ColumnDef name=\"id\" type=\"Integer\"/>\n"
            + "          <ColumnDef name=\"big_num\" type=\"Integer\"/>\n"
            + "          <ColumnDef name=\"name\" type=\"String\"/>\n"
            + "        </ColumnDefs>\n"
            + "        <Rows>\n"
            + "          <Row>\n"
            + "            <Value column=\"id\">0</Value>\n"
            + "            <Value column=\"big_num\">1234</Value>\n"
            + "            <Value column=\"name\">Ben</Value>\n"
            + "          </Row>\n"
            + "        </Rows>\n"
            + "      </InlineTable>\n"
            + "      <Level name=\"Level1\" column=\"big_num\" type=\"Integer\" internalType=\"char\"/>\n"
            + "      <Level name=\"Level2\" column=\"id\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n");
        testContext.assertQueryThrows(
            "select {[Big numbers].members} on 0 from [Sales]",
            "In Schema: In Cube: In Dimension: In Hierarchy: In Level: Value 'char' of attribute 'internalType' has illegal value 'char'.  Legal values: {int, long, Object, String}");
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
            TestContext.instance().createSubstitutingCube(
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
                + "  </Dimension>\n",
                null, null, null);
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
            + "{[Product truncated].[Fresh Vegetables]}\n"
            + "{[Product truncated].[Fresh Fruit]}\n"
            + "Row #0: 266,773\n"
            + "Row #1: 20,739\n"
            + "Row #2: 11,767\n");
    }

    // TODO: enable this test as part of PhysicalSchema work
    // TODO: also add a test that Table.alias, Join.leftAlias and
    // Join.rightAlias cannot be the empty string.
    public void _testNonUniqueAlias() {
        final TestContext testContext =
            TestContext.instance().createSubstitutingCube(
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
                + "  </Dimension>\n",
                null, null, null);
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

        final TestContext testContext = TestContext.instance().create(
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
            + "{[Product].[All Products], [Store].[USA].[CA]}\n"
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
            + "{[Store].[USA].[CA].[Beverly Hills]}\n"
            + "{[Store].[USA].[CA].[Los Angeles]}\n"
            + "{[Store].[USA].[CA].[San Diego]}\n"
            + "{[Store].[USA].[CA].[San Francisco]}\n"
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
        checkBugMondrian355("TimeHalfYears");

        // make sure that the deprecated name still works
        checkBugMondrian355("TimeHalfYear");
    }

    public void checkBugMondrian355(String timeHalfYear) {
        final String xml =
            "<Dimension name=\"Time2\" foreignKey=\"time_id\" type=\"TimeDimension\">\n"
            + "<Hierarchy hasAll=\"true\" primaryKey=\"time_id\">\n"
            + "  <Table name=\"time_by_day\"/>\n"
            + "  <Level name=\"Years\" column=\"the_year\" uniqueMembers=\"true\" type=\"Numeric\" levelType=\"TimeYears\"/>\n"
            + "  <Level name=\"Half year\" column=\"quarter\" uniqueMembers=\"false\" levelType=\""
            + timeHalfYear
            + "\"/>\n"
            + "  <Level name=\"Hours\" column=\"month_of_year\" uniqueMembers=\"false\" type=\"Numeric\" levelType=\"TimeHours\"/>\n"
            + "  <Level name=\"Quarter hours\" column=\"time_id\" uniqueMembers=\"false\" type=\"Numeric\" levelType=\"TimeUndefined\"/>\n"
            + "</Hierarchy>\n"
            + "</Dimension>";
        TestContext testContext = TestContext.instance().createSubstitutingCube(
            "Sales", xml);

        testContext.assertQueryReturns(
            "select Head([Time2].[Quarter hours].Members, 3) on columns\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time2].[1997].[Q1].[1].[367]}\n"
            + "{[Time2].[1997].[Q1].[1].[368]}\n"
            + "{[Time2].[1997].[Q1].[1].[369]}\n"
            + "Row #0: 348\n"
            + "Row #0: 635\n"
            + "Row #0: 589\n");

        // Check that can apply ParallelPeriod to a TimeUndefined level.
        testContext.assertAxisReturns(
            "PeriodsToDate([Time2].[Quarter hours], [Time2].[1997].[Q1].[1].[368])",
            "[Time2].[1997].[Q1].[1].[368]");

        testContext.assertAxisReturns(
            "PeriodsToDate([Time2].[Half year], [Time2].[1997].[Q1].[1].[368])",
            "[Time2].[1997].[Q1].[1].[367]\n"
            + "[Time2].[1997].[Q1].[1].[368]");

        // Check that get an error if give invalid level type
        try {
            TestContext.instance()
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
        final String salesCubeName = "DescSales";
        final String virtualCubeName = "DescWarehouseAndSales";
        final String warehouseCubeName = "Warehouse";
        final TestContext testContext = TestContext.instance().withSchema(
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
            + "  <Dimension name=\"Warehouse\">\n"
            + "    <Hierarchy hasAll=\"true\" primaryKey=\"warehouse_id\">\n"
            + "      <Table name=\"warehouse\"/>\n"
            + "      <Level name=\"Country\" column=\"warehouse_country\" uniqueMembers=\"true\"/>\n"
            + "      <Level name=\"State Province\" column=\"warehouse_state_province\"\n"
            + "          uniqueMembers=\"true\"/>\n"
            + "      <Level name=\"City\" column=\"warehouse_city\" uniqueMembers=\"false\"/>\n"
            + "      <Level name=\"Warehouse Name\" column=\"warehouse_name\" uniqueMembers=\"true\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Cube name=\"" + salesCubeName + "\"\n"
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
            + "<Cube name=\"" + warehouseCubeName + "\">\n"
            + "  <Table name=\"inventory_fact_1997\"/>\n"
            + "\n"
            + "  <DimensionUsage name=\"Time\" source=\"Time\" foreignKey=\"time_id\"/>\n"
            + "  <DimensionUsage name=\"Warehouse\" source=\"Warehouse\" foreignKey=\"warehouse_id\"/>\n"
            + "\n"
            + "  <Measure name=\"Units Shipped\" column=\"units_shipped\" aggregator=\"sum\" formatString=\"#.0\"/>\n"
            + "</Cube>\n"
            + "<VirtualCube name=\"" + virtualCubeName + "\"\n"
            + "    caption=\"Virtual cube caption\"\n"
            + "    description=\"Virtual cube description\">\n"
            + "  <Annotations><Annotation name=\"a\">Virtual cube</Annotation></Annotations>\n"
            + "  <VirtualCubeDimension name=\"Time\"/>\n"
            + "  <VirtualCubeDimension cubeName=\"" + warehouseCubeName
            + "\" name=\"Warehouse\"/>\n"
            + "  <VirtualCubeMeasure cubeName=\"" + salesCubeName
            + "\" name=\"[Measures].[Unit Sales]\">\n"
            + "    <Annotations><Annotation name=\"a\">Virtual cube measure</Annotation></Annotations>\n"
            + "  </VirtualCubeMeasure>\n"
            + "  <VirtualCubeMeasure cubeName=\"" + warehouseCubeName
            + "\" name=\"[Measures].[Units Shipped]\"/>\n"
            + "  <CalculatedMember name=\"Profit Per Unit Shipped\" dimension=\"Measures\">\n"
            + "    <Formula>1 / [Measures].[Units Shipped]</Formula>\n"
            + "  </CalculatedMember>\n"
            + "</VirtualCube>"
            + "</Schema>");
        final Result result =
            testContext.executeQuery("select from [" + salesCubeName + "]");
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
            cube.getSchemaReader(null).withLocus()
                .getLevelMembers(level, false);
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
        final boolean ssasCompatibleNaming =
            MondrianProperties.instance().SsasCompatibleNaming.get();
        if (ssasCompatibleNaming) {
            assertEquals("Time", timeHierarchy.getName());
            assertEquals("Time1", timeHierarchy.getDimension().getName());
        } else {
            assertEquals("Time1", timeHierarchy.getName());
        }
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
        checkAnnotations(time2Dimension.getAnnotationMap(), "a", "Time shared");

        final Hierarchy time2Hierarchy = time2Dimension.getHierarchies()[0];
        // The hierarchy in the shared dimension does not have a name, so the
        // hierarchy usage inherits the name of the dimension usage, Time2.
        if (ssasCompatibleNaming) {
            assertEquals("Time", time2Hierarchy.getName());
            assertEquals("Time2", time2Hierarchy.getDimension().getName());
        } else {
            assertEquals("Time2", time2Hierarchy.getName());
        }
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

        // The implicitly created [Fact Count] measure
        final Member factCountMeasure = measures.get(1);
        assertEquals("Fact Count", factCountMeasure.getName());
        assertEquals(
            false,
            factCountMeasure.getPropertyValue(Property.VISIBLE.name));
        checkAnnotations(
            factCountMeasure.getAnnotationMap(), "Internal Use",
            "For internal use");

        final Member calcMeasure = measures.get(2);
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

        final Result result2 =
            testContext.executeQuery("select from [" + virtualCubeName + "]");
        final Cube cube2 = result2.getQuery().getCube();
        assertEquals("Virtual cube description", cube2.getDescription());
        checkAnnotations(cube2.getAnnotationMap(), "a", "Virtual cube");

        final SchemaReader schemaReader2 = cube2.getSchemaReader(null);
        final Dimension measuresDimension2 = cube2.getDimensions()[0];
        final Hierarchy measuresHierarchy2 =
            measuresDimension2.getHierarchies()[0];
        final mondrian.olap.Level measuresLevel2 =
            measuresHierarchy2.getLevels()[0];
        final List<Member> measures2 =
            schemaReader2.getLevelMembers(measuresLevel2, true);
        final Member measure2 = measures2.get(0);
        assertEquals("Unit Sales", measure2.getName());
        assertEquals("Measure caption", measure2.getCaption());
        assertEquals("Measure description", measure2.getDescription());
        assertEquals(
            measure2.getDescription(),
            measure2.getPropertyValue(Property.DESCRIPTION.name));
        assertEquals(
            measure2.getCaption(),
            measure2.getPropertyValue(Property.CAPTION.name));
        assertEquals(
            measure2.getCaption(),
            measure2.getPropertyValue(Property.MEMBER_CAPTION.name));
        checkAnnotations(
            measure2.getAnnotationMap(), "a", "Virtual cube measure");
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

    public void testCaption() {
        TestContext testContext = TestContext.instance().createSubstitutingCube(
            "Sales",
            "  <Dimension name=\"Gender2\" foreignKey=\"customer_id\">\n"
            + "    <Hierarchy hasAll=\"true\" primaryKey=\"customer_id\" >\n"
            + "      <Table name=\"customer\"/>\n"
            + "      <Level name=\"Gender\" column=\"gender\" uniqueMembers=\"true\" >\n"
            + "        <CaptionExpression>\n"
            + "          <SQL dialect='generic'>'foobar'</SQL>\n"
            + "        </CaptionExpression>\n"
            + "      </Level>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>");
        switch (testContext.getDialect().getDatabaseProduct()) {
        case POSTGRESQL:
            // Postgres fails with:
            //   Internal error: while building member cache; sql=[select
            //     "customer"."gender" as "c0", 'foobar' as "c1" from "customer"
            //     as "customer" group by "customer"."gender", 'foobar' order by
            //     "customer"."\ gender" ASC NULLS LAST]
            //   Caused by: org.postgresql.util.PSQLException: ERROR:
            //     non-integer constant in GROUP BY
            //
            // It's difficult for mondrian to spot that it's been given a
            // constant expression. We can live with this bug. Postgres
            // shouldn't be so picky, and people shouldn't be so daft.
            return;
        }
        Result result = testContext.executeQuery(
            "select {[Gender2].Children} on columns from [Sales]");
        assertEquals(
            "foobar",
            result.getAxes()[0].getPositions().get(0).get(0).getCaption());
    }

    /**
     * Implementation of {@link PropertyFormatter} that throws.
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

    /**
     * Unit test for bug
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-747">
     * MONDRIAN-747, "When joining a shared dimension into a cube at a level
     * other than its leaf level, Mondrian gives wrong results"</a>.
     */
    public void testBugMondrian747() {
        // Test case requires a pecular inline view, and it works on dialects
        // that scalar subqery, viz oracle. I believe that the mondrian code
        // being works in all dialects.
        switch (TestContext.instance().getDialect().getDatabaseProduct()) {
        case ORACLE:
            break;
        default:
            return;
        }
        final TestContext testContext = TestContext.instance().withSchema(
            "<Schema name='Test_DimensionUsage'> \n"
            + "  <Dimension type='StandardDimension' name='Store'> \n"
            + "    <Hierarchy hasAll='true' primaryKey='store_id'> \n"
            + "      <Table name='store'> \n"
            + "      </Table> \n"
            + "      <Level name='country' column='store_country' type='String' uniqueMembers='false' levelType='Regular' hideMemberIf='Never'> \n"
            + "      </Level> \n"
            + "      <Level name='state' column='store_state' type='String' uniqueMembers='false' levelType='Regular' hideMemberIf='Never'> \n"
            + "      </Level> \n"
            + "      <Level name='city' column='store_city' type='String' uniqueMembers='false' levelType='Regular' hideMemberIf='Never'> \n"
            + "      </Level> \n"
            + "    </Hierarchy> \n"
            + "  </Dimension> \n"
            + "  <Dimension type='StandardDimension' name='Product'> \n"
            + "    <Hierarchy name='New Hierarchy 0' hasAll='true' primaryKey='product_id'> \n"
            + "      <Table name='product'> \n"
            + "      </Table> \n"
            + "      <Level name='product_name' column='product_name' type='String' uniqueMembers='false' levelType='Regular' hideMemberIf='Never'> \n"
            + "      </Level> \n"
            + "    </Hierarchy> \n"
            + "  </Dimension> \n"
            + "  <Cube name='cube1' cache='true' enabled='true'> \n"
            + "    <Table name='sales_fact_1997'> \n"
            + "    </Table> \n"
            + "    <DimensionUsage source='Store' name='Store' foreignKey='store_id'> \n"
            + "    </DimensionUsage> \n"
            + "    <DimensionUsage source='Product' name='Product' foreignKey='product_id'> \n"
            + "    </DimensionUsage> \n"
            + "    <Measure name='unitsales1' column='unit_sales' datatype='Numeric' aggregator='sum' visible='true'> \n"
            + "    </Measure> \n"
            + "  </Cube> \n"
            + "  <Cube name='cube2' cache='true' enabled='true'> \n"
//            + "    <Table name='sales_fact_1997_test'/> \n"
            + "    <View alias='sales_fact_1997_test'> \n"
            + "      <SQL dialect='generic'>select \"product_id\", \"time_id\", \"customer_id\", \"promotion_id\", \"store_id\", \"store_sales\", \"store_cost\", \"unit_sales\", (select \"store_state\" from \"store\" where \"store_id\" = \"sales_fact_1997\".\"store_id\") as \"sales_state_province\" from \"sales_fact_1997\"</SQL>\n"
            + "    </View> \n"
            + "    <DimensionUsage source='Store' level='state' name='Store' foreignKey='sales_state_province'> \n"
            + "    </DimensionUsage> \n"
            + "    <DimensionUsage source='Product' name='Product' foreignKey='product_id'> \n"
            + "    </DimensionUsage> \n"
            + "    <Measure name='unitsales2' column='unit_sales' datatype='Numeric' aggregator='sum' visible='true'> \n"
            + "    </Measure> \n"
            + "  </Cube> \n"
            + "  <VirtualCube enabled='true' name='virtual_cube'> \n"
            + "    <VirtualCubeDimension name='Store'> \n"
            + "    </VirtualCubeDimension> \n"
            + "    <VirtualCubeDimension name='Product'> \n"
            + "    </VirtualCubeDimension> \n"
            + "    <VirtualCubeMeasure cubeName='cube1' name='[Measures].[unitsales1]' visible='true'> \n"
            + "    </VirtualCubeMeasure> \n"
            + "    <VirtualCubeMeasure cubeName='cube2' name='[Measures].[unitsales2]' visible='true'> \n"
            + "    </VirtualCubeMeasure> \n"
            + "  </VirtualCube> \n"
            + "</Schema>");

        if (!Bug.BugMondrian747Fixed
            && MondrianProperties.instance().EnableGroupingSets.get())
        {
            // With grouping sets enabled, MONDRIAN-747 behavior is even worse.
            return;
        }

        // [Store].[All Stores] and [Store].[USA] should be 266,773. A higher
        // value would indicate that there is a cartesian product going on --
        // because "store_state" is not unique in "store" table.
        final String x = !Bug.BugMondrian747Fixed
            ? "1,379,620"
            : "266,773";
        testContext.assertQueryReturns(
            "select non empty {[Measures].[unitsales2]} on 0,\n"
            + " non empty [Store].members on 1\n"
            + "from [cube2]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[unitsales2]}\n"
            + "Axis #2:\n"
            + "{[Store].[All Stores]}\n"
            + "{[Store].[USA]}\n"
            + "{[Store].[USA].[CA]}\n"
            + "{[Store].[USA].[OR]}\n"
            + "{[Store].[USA].[WA]}\n"
            + "Row #0: 266,773\n"
            + "Row #1: " + x + "\n"
            + "Row #2: 373,740\n"
            + "Row #3: 135,318\n"
            + "Row #4: 870,562\n");

        testContext.assertQueryReturns(
            "select non empty {[Measures].[unitsales1]} on 0,\n"
            + " non empty [Store].members on 1\n"
            + "from [cube1]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[unitsales1]}\n"
            + "Axis #2:\n"
            + "{[Store].[All Stores]}\n"
            + "{[Store].[USA]}\n"
            + "{[Store].[USA].[CA]}\n"
            + "{[Store].[USA].[CA].[Beverly Hills]}\n"
            + "{[Store].[USA].[CA].[Los Angeles]}\n"
            + "{[Store].[USA].[CA].[San Diego]}\n"
            + "{[Store].[USA].[CA].[San Francisco]}\n"
            + "{[Store].[USA].[OR]}\n"
            + "{[Store].[USA].[OR].[Portland]}\n"
            + "{[Store].[USA].[OR].[Salem]}\n"
            + "{[Store].[USA].[WA]}\n"
            + "{[Store].[USA].[WA].[Bellingham]}\n"
            + "{[Store].[USA].[WA].[Bremerton]}\n"
            + "{[Store].[USA].[WA].[Seattle]}\n"
            + "{[Store].[USA].[WA].[Spokane]}\n"
            + "{[Store].[USA].[WA].[Tacoma]}\n"
            + "{[Store].[USA].[WA].[Walla Walla]}\n"
            + "{[Store].[USA].[WA].[Yakima]}\n"
            + "Row #0: 266,773\n"
            + "Row #1: 266,773\n"
            + "Row #2: 74,748\n"
            + "Row #3: 21,333\n"
            + "Row #4: 25,663\n"
            + "Row #5: 25,635\n"
            + "Row #6: 2,117\n"
            + "Row #7: 67,659\n"
            + "Row #8: 26,079\n"
            + "Row #9: 41,580\n"
            + "Row #10: 124,366\n"
            + "Row #11: 2,237\n"
            + "Row #12: 24,576\n"
            + "Row #13: 25,011\n"
            + "Row #14: 23,591\n"
            + "Row #15: 35,257\n"
            + "Row #16: 2,203\n"
            + "Row #17: 11,491\n");

        testContext.assertQueryReturns(
            "select non empty {[Measures].[unitsales2], [Measures].[unitsales1]} on 0,\n"
            + " non empty [Store].members on 1\n"
            + "from [virtual_cube]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[unitsales2]}\n"
            + "{[Measures].[unitsales1]}\n"
            + "Axis #2:\n"
            + "{[Store].[All Stores]}\n"
            + "{[Store].[USA]}\n"
            + "{[Store].[USA].[CA]}\n"
            + "{[Store].[USA].[CA].[Beverly Hills]}\n"
            + "{[Store].[USA].[CA].[Los Angeles]}\n"
            + "{[Store].[USA].[CA].[San Diego]}\n"
            + "{[Store].[USA].[CA].[San Francisco]}\n"
            + "{[Store].[USA].[OR]}\n"
            + "{[Store].[USA].[OR].[Portland]}\n"
            + "{[Store].[USA].[OR].[Salem]}\n"
            + "{[Store].[USA].[WA]}\n"
            + "{[Store].[USA].[WA].[Bellingham]}\n"
            + "{[Store].[USA].[WA].[Bremerton]}\n"
            + "{[Store].[USA].[WA].[Seattle]}\n"
            + "{[Store].[USA].[WA].[Spokane]}\n"
            + "{[Store].[USA].[WA].[Tacoma]}\n"
            + "{[Store].[USA].[WA].[Walla Walla]}\n"
            + "{[Store].[USA].[WA].[Yakima]}\n"
            + "Row #0: 266,773\n"
            + "Row #0: 266,773\n"
            + "Row #1: 1,379,620\n"
            + "Row #1: 266,773\n"
            + "Row #2: 373,740\n"
            + "Row #2: 74,748\n"
            + "Row #3: \n"
            + "Row #3: 21,333\n"
            + "Row #4: \n"
            + "Row #4: 25,663\n"
            + "Row #5: \n"
            + "Row #5: 25,635\n"
            + "Row #6: \n"
            + "Row #6: 2,117\n"
            + "Row #7: 135,318\n"
            + "Row #7: 67,659\n"
            + "Row #8: \n"
            + "Row #8: 26,079\n"
            + "Row #9: \n"
            + "Row #9: 41,580\n"
            + "Row #10: 870,562\n"
            + "Row #10: 124,366\n"
            + "Row #11: \n"
            + "Row #11: 2,237\n"
            + "Row #12: \n"
            + "Row #12: 24,576\n"
            + "Row #13: \n"
            + "Row #13: 25,011\n"
            + "Row #14: \n"
            + "Row #14: 23,591\n"
            + "Row #15: \n"
            + "Row #15: 35,257\n"
            + "Row #16: \n"
            + "Row #16: 2,203\n"
            + "Row #17: \n"
            + "Row #17: 11,491\n");
    }

    /**
     * Unit test for bug
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-463">
     * MONDRIAN-463, "Snowflake dimension with 3-way join."</a>.
     */
    public void testBugMondrian463() {
        if (!MondrianProperties.instance().FilterChildlessSnowflakeMembers
            .get())
        {
            // Similar to aggregates. If we turn off filtering,
            // we get wild stuff because of referential integrity.
            return;
        }
        // To build a dimension that is a 3-way snowflake, take the 2-way
        // product -> product_class join and convert to product -> store ->
        // product_class.
        //
        // It works because product_class_id covers the range 1 .. 110;
        // store_id covers every value in 0 .. 24;
        // region_id has 24 distinct values in the range 0 .. 106 (region_id 25
        // occurs twice).
        // Therefore in store, store_id -> region_id is a 25 to 24 mapping.
        checkBugMondrian463(
            TestContext.instance().createSubstitutingCube(
                "Sales",
                "<Dimension name='Product3' foreignKey='product_id'>\n"
                + "  <Hierarchy hasAll='true' primaryKey='product_id' primaryKeyTable='product'>\n"
                + "    <Join leftKey='product_class_id' rightKey='store_id'>\n"
                + "      <Table name='product'/>\n"
                + "      <Join leftKey='region_id' rightKey='product_class_id'>\n"
                + "        <Table name='store'/>\n"
                + "        <Table name='product_class'/>\n"
                + "      </Join>\n"
                + "    </Join>\n"
                + "    <Level name='Product Family' table='product_class' column='product_family' uniqueMembers='true'/>\n"
                + "    <Level name='Product Department' table='product_class' column='product_department' uniqueMembers='false'/>\n"
                + "    <Level name='Product Category' table='product_class' column='product_category' uniqueMembers='false'/>\n"
                + "    <Level name='Product Subcategory' table='product_class' column='product_subcategory' uniqueMembers='false'/>\n"
                + "    <Level name='Product Class' table='store' column='store_id' type='Numeric' uniqueMembers='true'/>\n"
                + "    <Level name='Brand Name' table='product' column='brand_name' uniqueMembers='false'/>\n"
                + "    <Level name='Product Name' table='product' column='product_name' uniqueMembers='true'/>\n"
                + "  </Hierarchy>\n"
                + "</Dimension>"));

        // As above, but using shared dimension.
        if (MondrianProperties.instance().ReadAggregates.get()
            && MondrianProperties.instance().UseAggregates.get())
        {
            // With aggregates enabled, query gives different answer. This is
            // expected because some of the foreign keys have referential
            // integrity problems.
            return;
        }
        checkBugMondrian463(
            TestContext.instance().withSchema(
                "<?xml version='1.0'?>\n"
                + "<Schema name='FoodMart'>\n"
                + "<Dimension name='Product3'>\n"
                + "  <Hierarchy hasAll='true' primaryKey='product_id' primaryKeyTable='product'>\n"
                + "    <Join leftKey='product_class_id' rightKey='store_id'>\n"
                + "      <Table name='product'/>\n"
                + "      <Join leftKey='region_id' rightKey='product_class_id'>\n"
                + "        <Table name='store'/>\n"
                + "        <Table name='product_class'/>\n"
                + "      </Join>\n"
                + "    </Join>\n"
                + "    <Level name='Product Family' table='product_class' column='product_family' uniqueMembers='true'/>\n"
                + "    <Level name='Product Department' table='product_class' column='product_department' uniqueMembers='false'/>\n"
                + "    <Level name='Product Category' table='product_class' column='product_category' uniqueMembers='false'/>\n"
                + "    <Level name='Product Subcategory' table='product_class' column='product_subcategory' uniqueMembers='false'/>\n"
                + "    <Level name='Product Class' table='store' column='store_id' type='Numeric' uniqueMembers='true'/>\n"
                + "    <Level name='Brand Name' table='product' column='brand_name' uniqueMembers='false'/>\n"
                + "    <Level name='Product Name' table='product' column='product_name' uniqueMembers='true'/>\n"
                + "  </Hierarchy>\n"
                + "</Dimension>\n"
                + "<Cube name='Sales'>\n"
                + "  <Table name='sales_fact_1997'/>\n"
                + "  <Dimension name='Time' type='TimeDimension' foreignKey='time_id'>\n"
                + "    <Hierarchy hasAll='false' primaryKey='time_id'>\n"
                + "      <Table name='time_by_day'/>\n"
                + "      <Level name='Year' column='the_year' type='Numeric' uniqueMembers='true'\n"
                + "          levelType='TimeYears'/>\n"
                + "      <Level name='Quarter' column='quarter' uniqueMembers='false'\n"
                + "          levelType='TimeQuarters'/>\n"
                + "      <Level name='Month' column='month_of_year' uniqueMembers='false' type='Numeric'\n"
                + "          levelType='TimeMonths'/>\n"
                + "    </Hierarchy>\n"
                + "  </Dimension>\n"
                + "  <DimensionUsage source='Product3' name='Product3' foreignKey='product_id'/>\n"
                + "  <Measure name='Unit Sales' column='unit_sales' aggregator='sum'\n"
                + "      formatString='#,###'/>\n"
                + "</Cube>\n"
                + "</Schema>"));
    }

    private void checkBugMondrian463(TestContext testContext) {
        testContext.assertQueryReturns(
            "select [Measures] on 0,\n"
            + " head([Product3].members, 10) on 1\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product3].[All Product3s]}\n"
            + "{[Product3].[Drink]}\n"
            + "{[Product3].[Drink].[Baking Goods]}\n"
            + "{[Product3].[Drink].[Baking Goods].[Dry Goods]}\n"
            + "{[Product3].[Drink].[Baking Goods].[Dry Goods].[Coffee]}\n"
            + "{[Product3].[Drink].[Baking Goods].[Dry Goods].[Coffee].[24]}\n"
            + "{[Product3].[Drink].[Baking Goods].[Dry Goods].[Coffee].[24].[Amigo]}\n"
            + "{[Product3].[Drink].[Baking Goods].[Dry Goods].[Coffee].[24].[Amigo].[Amigo Lox]}\n"
            + "{[Product3].[Drink].[Baking Goods].[Dry Goods].[Coffee].[24].[Curlew]}\n"
            + "{[Product3].[Drink].[Baking Goods].[Dry Goods].[Coffee].[24].[Curlew].[Curlew Lox]}\n"
            + "Row #0: 266,773\n"
            + "Row #1: 2,647\n"
            + "Row #2: 835\n"
            + "Row #3: 835\n"
            + "Row #4: 835\n"
            + "Row #5: 835\n"
            + "Row #6: 175\n"
            + "Row #7: 175\n"
            + "Row #8: 186\n"
            + "Row #9: 186\n");
    }

    /**
     * Tests that a join nested left-deep, that is (Join (Join A B) C), fails.
     * The correct way to use a join is right-deep, that is (Join A (Join B C)).
     * Same schema as {@link #testBugMondrian463}, except left-deep.
     */
    public void testLeftDeepJoinFails() {
        TestContext testContext = TestContext.instance().createSubstitutingCube(
            "Sales",
            "<Dimension name='Product3' foreignKey='product_id'>\n"
            + "  <Hierarchy hasAll='true' primaryKey='product_id' primaryKeyTable='product'>\n"
            + "    <Join leftKey='store_id' rightKey='product_class_id'>\n"
            + "      <Join leftKey='product_class_id' rightKey='region_id'>\n"
            + "        <Table name='product'/>\n"
            + "        <Table name='store'/>\n"
            + "      </Join>\n"
            + "      <Table name='product_class'/>\n"
            + "    </Join>\n"
            + "    <Level name='Product Family' table='product_class' column='product_family' uniqueMembers='true'/>\n"
            + "    <Level name='Product Department' table='product_class' column='product_department' uniqueMembers='false'/>\n"
            + "    <Level name='Product Category' table='product_class' column='product_category' uniqueMembers='false'/>\n"
            + "    <Level name='Product Subcategory' table='product_class' column='product_subcategory' uniqueMembers='false'/>\n"
            + "    <Level name='Product Class' table='store' column='store_id' uniqueMembers='true'/>\n"
            + "    <Level name='Brand Name' table='product' column='brand_name' uniqueMembers='false'/>\n"
            + "    <Level name='Product Name' table='product' column='product_name' uniqueMembers='true'/>\n"
            + "  </Hierarchy>\n"
            + "</Dimension>");
        try {
            testContext.assertSimpleQuery();
            fail("expected error");
        } catch (MondrianException e) {
            assertEquals(
                "Mondrian Error:Left side of join must not be a join; mondrian only supports right-deep joins.",
                e.getMessage());
        }
    }

    /**
     * Test for MONDRIAN-943 and MONDRIAN-465.
     */
    public void testCaptionWithOrdinalColumn() {
        final TestContext tc =
            TestContext.instance().createSubstitutingCube(
                "HR",
                "<Dimension name=\"Position\" foreignKey=\"employee_id\">\n"
                + "  <Hierarchy hasAll=\"true\" allMemberName=\"All Position\" primaryKey=\"employee_id\">\n"
                + "    <Table name=\"employee\"/>\n"
                + "    <Level name=\"Management Role\" uniqueMembers=\"true\" column=\"management_role\"/>\n"
                + "    <Level name=\"Position Title\" uniqueMembers=\"false\" column=\"position_title\" ordinalColumn=\"position_id\" captionColumn=\"position_title\"/>\n"
                + "  </Hierarchy>\n"
                + "</Dimension>\n");
        String mdxQuery =
            "WITH SET [#DataSet#] as '{Descendants([Position].[All Position], 2)}' "
            + "SELECT {[Measures].[Org Salary]} on columns, "
            + "NON EMPTY Hierarchize({[#DataSet#]}) on rows FROM [HR]";
        Result result = tc.executeQuery(mdxQuery);
        Axis[] axes = result.getAxes();
        List<Position> positions = axes[1].getPositions();
        Member mall = positions.get(0).get(0);
        String caption = mall.getHierarchy().getCaption();
        assertEquals("Position", caption);
        String captionValue = mall.getCaption();
        assertEquals("HQ Information Systems", captionValue);
        mall = positions.get(14).get(0);
        captionValue = mall.getCaption();
        assertEquals("Store Manager", captionValue);
        mall = positions.get(15).get(0);
        captionValue = mall.getCaption();
        assertEquals("Store Assistant Manager", captionValue);
    }

    /**
     * This is a test case for bug Mondrian-923. When a virtual cube included
     * calculated members in its schema, they were not included in the list of
     * existing measures because of an override of the hierarchy schema reader
     * which was done at cube init time when resolving the calculated members
     * of the base cubes.
     */
    public void testBugMondrian923() throws Exception {
        TestContext context =
            TestContext.instance().createSubstitutingCube(
                "Warehouse and Sales",
                null,
                null,
                "<CalculatedMember name=\"Image Unit Sales\" dimension=\"Measures\"><Formula>[Measures].[Unit Sales]</Formula><CalculatedMemberProperty name=\"FORMAT_STRING\" value=\"|$#,###.00|image=icon_chart\\.gif|link=http://www\\.pentaho\\.com\"/></CalculatedMember>"
                + "<CalculatedMember name=\"Arrow Unit Sales\" dimension=\"Measures\"><Formula>[Measures].[Unit Sales]</Formula><CalculatedMemberProperty name=\"FORMAT_STRING\" expression=\"IIf([Measures].[Unit Sales] > 10000,'|#,###|arrow=up',IIf([Measures].[Unit Sales] > 5000,'|#,###|arrow=down','|#,###|arrow=none'))\"/></CalculatedMember>"
                + "<CalculatedMember name=\"Style Unit Sales\" dimension=\"Measures\"><Formula>[Measures].[Unit Sales]</Formula><CalculatedMemberProperty name=\"FORMAT_STRING\" expression=\"IIf([Measures].[Unit Sales] > 100000,'|#,###|style=green',IIf([Measures].[Unit Sales] > 50000,'|#,###|style=yellow','|#,###|style=red'))\"/></CalculatedMember>",
                null);
        for (Cube cube
                : context.getConnection().getSchemaReader().getCubes())
        {
            if (cube.getName().equals("Warehouse and Sales")) {
                for (Dimension dim : cube.getDimensions()) {
                    if (dim.isMeasures()) {
                        List<Member> members =
                            context.getConnection()
                                .getSchemaReader().getLevelMembers(
                                    dim.getHierarchy().getLevels()[0],
                                    true);
                        assertTrue(
                            members.toString().contains(
                                "[Measures].[Profit Per Unit Shipped]"));
                        assertTrue(
                            members.toString().contains(
                                "[Measures].[Image Unit Sales]"));
                        assertTrue(
                            members.toString().contains(
                                "[Measures].[Arrow Unit Sales]"));
                        assertTrue(
                            members.toString().contains(
                                "[Measures].[Style Unit Sales]"));
                        assertTrue(
                            members.toString().contains(
                                "[Measures].[Average Warehouse Sale]"));
                        return;
                    }
                }
            }
        }
        fail("Didn't find measures in sales cube.");
    }

    public void testCubesVisibility() throws Exception {
        for (Boolean testValue : new Boolean[] {true, false}) {
            String cubeDef =
                "<Cube name=\"Foo\" visible=\"@REPLACE_ME@\">\n"
                + "  <Table name=\"store\"/>\n"
                + "  <Dimension name=\"Store Type\">\n"
                + "    <Hierarchy hasAll=\"true\">\n"
                + "      <Level name=\"Store Type\" column=\"store_type\" uniqueMembers=\"true\"/>\n"
                + "    </Hierarchy>\n"
                + "  </Dimension>\n"
                + "  <Measure name=\"Store Sqft\" column=\"store_sqft\" aggregator=\"sum\"\n"
                + "      formatString=\"#,###\"/>\n"
                + "</Cube>\n";
            cubeDef = cubeDef.replace(
                "@REPLACE_ME@",
                String.valueOf(testValue));
            final TestContext context =
                TestContext.instance().create(
                    null, cubeDef, null, null, null, null);
            final Cube cube =
                context.getConnection().getSchema()
                    .lookupCube("Foo", true);
            assertTrue(testValue.equals(cube.isVisible()));
        }
    }

    public void testVirtualCubesVisibility() throws Exception {
        for (Boolean testValue : new Boolean[] {true, false}) {
            String cubeDef =
                "<VirtualCube name=\"Foo\" defaultMeasure=\"Store Sales\" visible=\"@REPLACE_ME@\">\n"
                + "  <VirtualCubeDimension cubeName=\"Sales\" name=\"Customers\"/>\n"
                + "  <VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Store Sales]\"/>\n"
                + "</VirtualCube>\n";
            cubeDef = cubeDef.replace(
                "@REPLACE_ME@",
                String.valueOf(testValue));
            final TestContext context =
                TestContext.instance().create(
                    null, null, cubeDef, null, null, null);
            final Cube cube =
                context.getConnection().getSchema()
                    .lookupCube("Foo", true);
            assertTrue(testValue.equals(cube.isVisible()));
        }
    }

    public void testDimensionVisibility() throws Exception {
        for (Boolean testValue : new Boolean[] {true, false}) {
            String cubeDef =
                "<Cube name=\"Foo\">\n"
                + "  <Table name=\"store\"/>\n"
                + "  <Dimension name=\"Bar\" visible=\"@REPLACE_ME@\">\n"
                + "    <Hierarchy hasAll=\"true\">\n"
                + "      <Level name=\"Store Type\" column=\"store_type\" uniqueMembers=\"true\"/>\n"
                + "    </Hierarchy>\n"
                + "  </Dimension>\n"
                + "  <Measure name=\"Store Sqft\" column=\"store_sqft\" aggregator=\"sum\"\n"
                + "      formatString=\"#,###\"/>\n"
                + "</Cube>\n";
            cubeDef = cubeDef.replace(
                "@REPLACE_ME@",
                String.valueOf(testValue));
            final TestContext context =
                TestContext.instance().create(
                    null, cubeDef, null, null, null, null);
            final Cube cube =
                context.getConnection().getSchema()
                    .lookupCube("Foo", true);
            Dimension dim = null;
            for (Dimension dimCheck : cube.getDimensions()) {
                if (dimCheck.getName().equals("Bar")) {
                    dim = dimCheck;
                }
            }
            assertNotNull(dim);
            assertTrue(testValue.equals(dim.isVisible()));
        }
    }

    public void testVirtualDimensionVisibility() throws Exception {
        for (Boolean testValue : new Boolean[] {true, false}) {
            String cubeDef =
                "<VirtualCube name=\"Foo\" defaultMeasure=\"Store Sales\">\n"
                + "  <VirtualCubeDimension cubeName=\"Sales\" name=\"Customers\" visible=\"@REPLACE_ME@\"/>\n"
                + "  <VirtualCubeMeasure cubeName=\"Sales\" name=\"[Measures].[Store Sales]\"/>\n"
                + "</VirtualCube>\n";
            cubeDef = cubeDef.replace(
                "@REPLACE_ME@",
                String.valueOf(testValue));
            final TestContext context =
                TestContext.instance().create(
                    null, null, cubeDef, null, null, null);
            final Cube cube =
                context.getConnection().getSchema()
                    .lookupCube("Foo", true);
            Dimension dim = null;
            for (Dimension dimCheck : cube.getDimensions()) {
                if (dimCheck.getName().equals("Customers")) {
                    dim = dimCheck;
                }
            }
            assertNotNull(dim);
            assertTrue(testValue.equals(dim.isVisible()));
        }
    }

    public void testDimensionUsageVisibility() throws Exception {
        for (Boolean testValue : new Boolean[] {true, false}) {
            String cubeDef =
                "<Cube name=\"Foo\">\n"
                + "  <Table name=\"store\"/>\n"
                + "  <Dimension name=\"Bacon\">\n"
                + "    <Hierarchy hasAll=\"true\">\n"
                + "      <Level name=\"Store Type\" column=\"store_type\" uniqueMembers=\"true\"/>\n"
                + "    </Hierarchy>\n"
                + "  </Dimension>\n"
                + "  <Measure name=\"Store Sqft\" column=\"store_sqft\" aggregator=\"sum\"\n"
                + "      formatString=\"#,###\"/>\n"
                + "</Cube>\n";
            final TestContext context =
                TestContext.instance().create(
                    null, cubeDef, null, null, null, null);
            final Cube cube =
                context.getConnection().getSchema()
                    .lookupCube("Foo", true);
            String dimensionDef =
                "<DimensionUsage name=\"Bar\" source=\"Time\" foreignKey=\"time_id\" visible=\"@REPLACE_ME@\"/>";
            dimensionDef = dimensionDef.replace(
                "@REPLACE_ME@",
                String.valueOf(testValue));
            context.getConnection().getSchema().createDimension(
                cube, dimensionDef);
            Dimension dim = null;
            for (Dimension dimCheck : cube.getDimensions()) {
                if (dimCheck.getName().equals("Bar")) {
                    dim = dimCheck;
                }
            }
            assertNotNull(dim);
            assertTrue(testValue.equals(dim.isVisible()));
        }
    }

    public void testHierarchyVisibility() throws Exception {
        for (Boolean testValue : new Boolean[] {true, false}) {
            String cubeDef =
                "<Cube name=\"Foo\">\n"
                + "  <Table name=\"store\"/>\n"
                + "  <Dimension name=\"Bar\">\n"
                + "    <Hierarchy name=\"Bacon\" hasAll=\"true\" visible=\"@REPLACE_ME@\">\n"
                + "      <Level name=\"Store Type\" column=\"store_type\" uniqueMembers=\"true\"/>\n"
                + "    </Hierarchy>\n"
                + "  </Dimension>\n"
                + "  <Measure name=\"Store Sqft\" column=\"store_sqft\" aggregator=\"sum\"\n"
                + "      formatString=\"#,###\"/>\n"
                + "</Cube>\n";
            cubeDef = cubeDef.replace(
                "@REPLACE_ME@",
                String.valueOf(testValue));
            final TestContext context =
                TestContext.instance().create(
                    null, cubeDef, null, null, null, null);
            final Cube cube =
                context.getConnection().getSchema()
                    .lookupCube("Foo", true);
            Dimension dim = null;
            for (Dimension dimCheck : cube.getDimensions()) {
                if (dimCheck.getName().equals("Bar")) {
                    dim = dimCheck;
                }
            }
            assertNotNull(dim);
            final Hierarchy hier = dim.getHierarchy();
            assertNotNull(hier);
            assertEquals(
                MondrianProperties.instance().SsasCompatibleNaming.get()
                    ? "Bacon"
                    : "Bar.Bacon",
                hier.getName());
            assertTrue(testValue.equals(hier.isVisible()));
        }
    }

    public void testLevelVisibility() throws Exception {
        for (Boolean testValue : new Boolean[] {true, false}) {
            String cubeDef =
                "<Cube name=\"Foo\">\n"
                + "  <Table name=\"store\"/>\n"
                + "  <Dimension name=\"Bar\">\n"
                + "    <Hierarchy name=\"Bacon\" hasAll=\"false\">\n"
                + "      <Level name=\"Samosa\" column=\"store_type\" uniqueMembers=\"true\" visible=\"@REPLACE_ME@\"/>\n"
                + "    </Hierarchy>\n"
                + "  </Dimension>\n"
                + "  <Measure name=\"Store Sqft\" column=\"store_sqft\" aggregator=\"sum\"\n"
                + "      formatString=\"#,###\"/>\n"
                + "</Cube>\n";
            cubeDef = cubeDef.replace(
                "@REPLACE_ME@",
                String.valueOf(testValue));
            final TestContext context =
                TestContext.instance().create(
                    null, cubeDef, null, null, null, null);
            final Cube cube =
                context.getConnection().getSchema()
                    .lookupCube("Foo", true);
            Dimension dim = null;
            for (Dimension dimCheck : cube.getDimensions()) {
                if (dimCheck.getName().equals("Bar")) {
                    dim = dimCheck;
                }
            }
            assertNotNull(dim);
            final Hierarchy hier = dim.getHierarchy();
            assertNotNull(hier);
            assertEquals(
                MondrianProperties.instance().SsasCompatibleNaming.get()
                    ? "Bacon"
                    : "Bar.Bacon",
                hier.getName());
            final mondrian.olap.Level level = hier.getLevels()[0];
            assertEquals("Samosa", level.getName());
            assertTrue(testValue.equals(level.isVisible()));
        }
    }

    public void testNonCollapsedAggregate() throws Exception {
        if (MondrianProperties.instance().UseAggregates.get() == false
            && MondrianProperties.instance().ReadAggregates.get() == false)
        {
            return;
        }
        final String cube =
            "<Cube name=\"Foo\" defaultMeasure=\"Unit Sales\">\n"
            + "  <Table name=\"sales_fact_1997\">\n"
            + "    <AggExclude name=\"agg_g_ms_pcat_sales_fact_1997\"/>"
            + "    <AggExclude name=\"agg_c_14_sales_fact_1997\"/>"
            + "    <AggExclude name=\"agg_pl_01_sales_fact_1997\"/>"
            + "    <AggExclude name=\"agg_ll_01_sales_fact_1997\"/>"
            + "    <AggName name=\"agg_l_05_sales_fact_1997\">"
            + "        <AggFactCount column=\"fact_count\"/>\n"
            + "        <AggIgnoreColumn column=\"customer_id\"/>\n"
            + "        <AggIgnoreColumn column=\"store_id\"/>\n"
            + "        <AggIgnoreColumn column=\"promotion_id\"/>\n"
            + "        <AggIgnoreColumn column=\"store_sales\"/>\n"
            + "        <AggIgnoreColumn column=\"store_cost\"/>\n"
            + "        <AggMeasure name=\"[Measures].[Unit Sales]\" column=\"unit_sales\" />\n"
            + "        <AggLevel name=\"[Product].[Product Id]\" column=\"product_id\" collapsed=\"false\"/>\n"
            + "    </AggName>\n"
            + "</Table>\n"
            + "<Dimension foreignKey=\"product_id\" name=\"Product\">\n"
            + "<Hierarchy hasAll=\"true\" primaryKey=\"product_id\" primaryKeyTable=\"product\">\n"
            + "  <Join leftKey=\"product_class_id\" rightKey=\"product_class_id\">\n"
            + " <Table name=\"product\"/>\n"
            + " <Table name=\"product_class\"/>\n"
            + "  </Join>\n"
            + "  <Level name=\"Product Family\" table=\"product_class\" column=\"product_family\"\n"
            + "   uniqueMembers=\"true\"/>\n"
            + "  <Level name=\"Product Department\" table=\"product_class\" column=\"product_department\"\n"
            + "   uniqueMembers=\"false\"/>\n"
            + "  <Level name=\"Product Category\" table=\"product_class\" column=\"product_category\"\n"
            + "   uniqueMembers=\"false\"/>\n"
            + "  <Level name=\"Product Subcategory\" table=\"product_class\" column=\"product_subcategory\"\n"
            + "   uniqueMembers=\"false\"/>\n"
            + "  <Level name=\"Brand Name\" table=\"product\" column=\"brand_name\" uniqueMembers=\"false\"/>\n"
            + "  <Level name=\"Product Name\" table=\"product\" column=\"product_name\"\n"
            + "   uniqueMembers=\"true\"/>\n"
            + "  <Level name=\"Product Id\" table=\"product\" column=\"product_id\"\n"
            + "   uniqueMembers=\"true\"/>\n"
            + "</Hierarchy>\n"
            + "</Dimension>\n"
            + "<Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\"\n"
            + "      formatString=\"Standard\"/>\n"
            + "</Cube>\n";
        final TestContext context =
            TestContext.instance().create(
                null, cube, null, null, null, null);
        context.assertQueryReturns(
            "select {[Product].[Product Family].Members} on rows, {[Measures].[Unit Sales]} on columns from [Foo]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Drink]}\n"
            + "{[Product].[Food]}\n"
            + "{[Product].[Non-Consumable]}\n"
            + "Row #0: 24,597\n"
            + "Row #1: 191,940\n"
            + "Row #2: 50,236\n");
    }

    public void testNonCollapsedAggregateOnNonUniqueLevelFails()
        throws Exception
    {
        if (MondrianProperties.instance().UseAggregates.get() == false
            && MondrianProperties.instance().ReadAggregates.get() == false)
        {
            return;
        }
        final String cube =
            "<Cube name=\"Foo\" defaultMeasure=\"Unit Sales\">\n"
            + "  <Table name=\"sales_fact_1997\">\n"
            + "    <AggExclude name=\"agg_g_ms_pcat_sales_fact_1997\"/>"
            + "    <AggExclude name=\"agg_c_14_sales_fact_1997\"/>"
            + "    <AggExclude name=\"agg_pl_01_sales_fact_1997\"/>"
            + "    <AggExclude name=\"agg_ll_01_sales_fact_1997\"/>"
            + "    <AggName name=\"agg_l_05_sales_fact_1997\">"
            + "        <AggFactCount column=\"fact_count\"/>\n"
            + "        <AggIgnoreColumn column=\"customer_id\"/>\n"
            + "        <AggIgnoreColumn column=\"store_id\"/>\n"
            + "        <AggIgnoreColumn column=\"promotion_id\"/>\n"
            + "        <AggIgnoreColumn column=\"store_sales\"/>\n"
            + "        <AggIgnoreColumn column=\"store_cost\"/>\n"
            + "        <AggMeasure name=\"[Measures].[Unit Sales]\" column=\"unit_sales\" />\n"
            + "        <AggLevel name=\"[Product].[Product Name]\" column=\"product_id\" collapsed=\"false\"/>\n"
            + "    </AggName>\n"
            + "</Table>\n"
            + "<Dimension foreignKey=\"product_id\" name=\"Product\">\n"
            + "<Hierarchy hasAll=\"true\" primaryKey=\"product_id\" primaryKeyTable=\"product\">\n"
            + "  <Join leftKey=\"product_class_id\" rightKey=\"product_class_id\">\n"
            + " <Table name=\"product\"/>\n"
            + " <Table name=\"product_class\"/>\n"
            + "  </Join>\n"
            + "  <Level name=\"Product Family\" table=\"product_class\" column=\"product_family\"\n"
            + "   uniqueMembers=\"true\"/>\n"
            + "  <Level name=\"Product Department\" table=\"product_class\" column=\"product_department\"\n"
            + "   uniqueMembers=\"false\"/>\n"
            + "  <Level name=\"Product Category\" table=\"product_class\" column=\"product_category\"\n"
            + "   uniqueMembers=\"false\"/>\n"
            + "  <Level name=\"Product Subcategory\" table=\"product_class\" column=\"product_subcategory\"\n"
            + "   uniqueMembers=\"false\"/>\n"
            + "  <Level name=\"Brand Name\" table=\"product\" column=\"brand_name\" uniqueMembers=\"false\"/>\n"
            + "  <Level name=\"Product Name\" table=\"product\" column=\"product_name\"\n"
            + "   uniqueMembers=\"false\"/>\n"
            + "  <Level name=\"Product Id\" table=\"product\" column=\"product_id\"\n"
            + "   uniqueMembers=\"true\"/>\n"
            + "</Hierarchy>\n"
            + "</Dimension>\n"
            + "<Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\"\n"
            + "      formatString=\"Standard\"/>\n"
            + "</Cube>\n";
        final TestContext context =
            TestContext.instance().create(
                null, cube, null, null, null, null);
        context.assertQueryThrows(
            "select {[Product].[Product Family].Members} on rows, {[Measures].[Unit Sales]} on columns from [Foo]",
            "mondrian.olap.MondrianException: Mondrian Error:Too many errors, '1', while loading/reloading aggregates.");
    }

    public void testTwoNonCollapsedAggregate() throws Exception {
        if (MondrianProperties.instance().UseAggregates.get() == false
            && MondrianProperties.instance().ReadAggregates.get() == false)
        {
            return;
        }
        final String cube =
            "<Cube name=\"Foo\" defaultMeasure=\"Unit Sales\">\n"
            + "  <Table name=\"sales_fact_1997\">\n"
            + "    <AggExclude name=\"agg_g_ms_pcat_sales_fact_1997\"/>"
            + "    <AggExclude name=\"agg_c_14_sales_fact_1997\"/>"
            + "    <AggExclude name=\"agg_pl_01_sales_fact_1997\"/>"
            + "    <AggExclude name=\"agg_ll_01_sales_fact_1997\"/>"
            + "    <AggName name=\"agg_l_05_sales_fact_1997\">"
            + "        <AggFactCount column=\"fact_count\"/>\n"
            + "        <AggIgnoreColumn column=\"customer_id\"/>\n"
            + "        <AggIgnoreColumn column=\"promotion_id\"/>\n"
            + "        <AggIgnoreColumn column=\"store_sales\"/>\n"
            + "        <AggIgnoreColumn column=\"store_cost\"/>\n"
            + "        <AggMeasure name=\"[Measures].[Unit Sales]\" column=\"unit_sales\" />\n"
            + "        <AggLevel name=\"[Product].[Product Id]\" column=\"product_id\" collapsed=\"false\"/>\n"
            + "        <AggLevel name=\"[Store].[Store Id]\" column=\"store_id\" collapsed=\"false\"/>\n"
            + "    </AggName>\n"
            + "</Table>\n"
            + "<Dimension foreignKey=\"product_id\" name=\"Product\">\n"
            + "<Hierarchy hasAll=\"true\" primaryKey=\"product_id\" primaryKeyTable=\"product\">\n"
            + "  <Join leftKey=\"product_class_id\" rightKey=\"product_class_id\">\n"
            + " <Table name=\"product\"/>\n"
            + " <Table name=\"product_class\"/>\n"
            + "  </Join>\n"
            + "  <Level name=\"Product Family\" table=\"product_class\" column=\"product_family\"\n"
            + "   uniqueMembers=\"true\"/>\n"
            + "  <Level name=\"Product Department\" table=\"product_class\" column=\"product_department\"\n"
            + "   uniqueMembers=\"false\"/>\n"
            + "  <Level name=\"Product Category\" table=\"product_class\" column=\"product_category\"\n"
            + "   uniqueMembers=\"false\"/>\n"
            + "  <Level name=\"Product Subcategory\" table=\"product_class\" column=\"product_subcategory\"\n"
            + "   uniqueMembers=\"false\"/>\n"
            + "  <Level name=\"Brand Name\" table=\"product\" column=\"brand_name\" uniqueMembers=\"false\"/>\n"
            + "  <Level name=\"Product Name\" table=\"product\" column=\"product_name\"\n"
            + "   uniqueMembers=\"true\"/>\n"
            + "  <Level name=\"Product Id\" table=\"product\" column=\"product_id\"\n"
            + "   uniqueMembers=\"true\"/>\n"
            + "</Hierarchy>\n"
            + "</Dimension>\n"
            + "  <Dimension name=\"Store\" foreignKey=\"store_id\" >\n"
            + "    <Hierarchy hasAll=\"true\" primaryKey=\"store_id\"\n"
            + "        primaryKeyTable=\"store\">\n"
            + "      <Join leftKey=\"region_id\" rightKey=\"region_id\">\n"
            + "        <Table name=\"store\"/>\n"
            + "        <Table name=\"region\"/>\n"
            + "      </Join>\n"
            + "      <Level name=\"Store Region\" table=\"region\" column=\"sales_city\"\n"
            + "          uniqueMembers=\"false\"/>\n"
            + "      <Level name=\"Store Id\" table=\"store\" column=\"store_id\"\n"
            + "          uniqueMembers=\"true\">\n"
            + "      </Level>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "<Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\"\n"
            + "      formatString=\"Standard\"/>\n"
            + "</Cube>\n";
        final TestContext context =
            TestContext.instance().create(
                null, cube, null, null, null, null);
        context.assertQueryReturns(
            "select {Crossjoin([Product].[Product Family].Members, [Store].[Store Id].Members)} on rows, {[Measures].[Unit Sales]} on columns from [Foo]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Drink], [Store].[Acapulco].[1]}\n"
            + "{[Product].[Drink], [Store].[Bellingham].[2]}\n"
            + "{[Product].[Drink], [Store].[Beverly Hills].[6]}\n"
            + "{[Product].[Drink], [Store].[Bremerton].[3]}\n"
            + "{[Product].[Drink], [Store].[Camacho].[4]}\n"
            + "{[Product].[Drink], [Store].[Guadalajara].[5]}\n"
            + "{[Product].[Drink], [Store].[Hidalgo].[12]}\n"
            + "{[Product].[Drink], [Store].[Hidalgo].[18]}\n"
            + "{[Product].[Drink], [Store].[Los Angeles].[7]}\n"
            + "{[Product].[Drink], [Store].[Merida].[8]}\n"
            + "{[Product].[Drink], [Store].[Mexico City].[9]}\n"
            + "{[Product].[Drink], [Store].[None].[0]}\n"
            + "{[Product].[Drink], [Store].[Orizaba].[10]}\n"
            + "{[Product].[Drink], [Store].[Portland].[11]}\n"
            + "{[Product].[Drink], [Store].[Salem].[13]}\n"
            + "{[Product].[Drink], [Store].[San Andres].[21]}\n"
            + "{[Product].[Drink], [Store].[San Diego].[24]}\n"
            + "{[Product].[Drink], [Store].[San Francisco].[14]}\n"
            + "{[Product].[Drink], [Store].[Seattle].[15]}\n"
            + "{[Product].[Drink], [Store].[Spokane].[16]}\n"
            + "{[Product].[Drink], [Store].[Tacoma].[17]}\n"
            + "{[Product].[Drink], [Store].[Vancouver].[19]}\n"
            + "{[Product].[Drink], [Store].[Victoria].[20]}\n"
            + "{[Product].[Drink], [Store].[Walla Walla].[22]}\n"
            + "{[Product].[Drink], [Store].[Yakima].[23]}\n"
            + "{[Product].[Food], [Store].[Acapulco].[1]}\n"
            + "{[Product].[Food], [Store].[Bellingham].[2]}\n"
            + "{[Product].[Food], [Store].[Beverly Hills].[6]}\n"
            + "{[Product].[Food], [Store].[Bremerton].[3]}\n"
            + "{[Product].[Food], [Store].[Camacho].[4]}\n"
            + "{[Product].[Food], [Store].[Guadalajara].[5]}\n"
            + "{[Product].[Food], [Store].[Hidalgo].[12]}\n"
            + "{[Product].[Food], [Store].[Hidalgo].[18]}\n"
            + "{[Product].[Food], [Store].[Los Angeles].[7]}\n"
            + "{[Product].[Food], [Store].[Merida].[8]}\n"
            + "{[Product].[Food], [Store].[Mexico City].[9]}\n"
            + "{[Product].[Food], [Store].[None].[0]}\n"
            + "{[Product].[Food], [Store].[Orizaba].[10]}\n"
            + "{[Product].[Food], [Store].[Portland].[11]}\n"
            + "{[Product].[Food], [Store].[Salem].[13]}\n"
            + "{[Product].[Food], [Store].[San Andres].[21]}\n"
            + "{[Product].[Food], [Store].[San Diego].[24]}\n"
            + "{[Product].[Food], [Store].[San Francisco].[14]}\n"
            + "{[Product].[Food], [Store].[Seattle].[15]}\n"
            + "{[Product].[Food], [Store].[Spokane].[16]}\n"
            + "{[Product].[Food], [Store].[Tacoma].[17]}\n"
            + "{[Product].[Food], [Store].[Vancouver].[19]}\n"
            + "{[Product].[Food], [Store].[Victoria].[20]}\n"
            + "{[Product].[Food], [Store].[Walla Walla].[22]}\n"
            + "{[Product].[Food], [Store].[Yakima].[23]}\n"
            + "{[Product].[Non-Consumable], [Store].[Acapulco].[1]}\n"
            + "{[Product].[Non-Consumable], [Store].[Bellingham].[2]}\n"
            + "{[Product].[Non-Consumable], [Store].[Beverly Hills].[6]}\n"
            + "{[Product].[Non-Consumable], [Store].[Bremerton].[3]}\n"
            + "{[Product].[Non-Consumable], [Store].[Camacho].[4]}\n"
            + "{[Product].[Non-Consumable], [Store].[Guadalajara].[5]}\n"
            + "{[Product].[Non-Consumable], [Store].[Hidalgo].[12]}\n"
            + "{[Product].[Non-Consumable], [Store].[Hidalgo].[18]}\n"
            + "{[Product].[Non-Consumable], [Store].[Los Angeles].[7]}\n"
            + "{[Product].[Non-Consumable], [Store].[Merida].[8]}\n"
            + "{[Product].[Non-Consumable], [Store].[Mexico City].[9]}\n"
            + "{[Product].[Non-Consumable], [Store].[None].[0]}\n"
            + "{[Product].[Non-Consumable], [Store].[Orizaba].[10]}\n"
            + "{[Product].[Non-Consumable], [Store].[Portland].[11]}\n"
            + "{[Product].[Non-Consumable], [Store].[Salem].[13]}\n"
            + "{[Product].[Non-Consumable], [Store].[San Andres].[21]}\n"
            + "{[Product].[Non-Consumable], [Store].[San Diego].[24]}\n"
            + "{[Product].[Non-Consumable], [Store].[San Francisco].[14]}\n"
            + "{[Product].[Non-Consumable], [Store].[Seattle].[15]}\n"
            + "{[Product].[Non-Consumable], [Store].[Spokane].[16]}\n"
            + "{[Product].[Non-Consumable], [Store].[Tacoma].[17]}\n"
            + "{[Product].[Non-Consumable], [Store].[Vancouver].[19]}\n"
            + "{[Product].[Non-Consumable], [Store].[Victoria].[20]}\n"
            + "{[Product].[Non-Consumable], [Store].[Walla Walla].[22]}\n"
            + "{[Product].[Non-Consumable], [Store].[Yakima].[23]}\n"
            + "Row #0: \n"
            + "Row #1: 208\n"
            + "Row #2: 1,945\n"
            + "Row #3: 2,288\n"
            + "Row #4: \n"
            + "Row #5: \n"
            + "Row #6: \n"
            + "Row #7: \n"
            + "Row #8: 2,422\n"
            + "Row #9: \n"
            + "Row #10: \n"
            + "Row #11: \n"
            + "Row #12: \n"
            + "Row #13: 2,371\n"
            + "Row #14: 3,735\n"
            + "Row #15: \n"
            + "Row #16: 2,560\n"
            + "Row #17: 175\n"
            + "Row #18: 2,213\n"
            + "Row #19: 2,238\n"
            + "Row #20: 3,092\n"
            + "Row #21: \n"
            + "Row #22: \n"
            + "Row #23: 191\n"
            + "Row #24: 1,159\n"
            + "Row #25: \n"
            + "Row #26: 1,587\n"
            + "Row #27: 15,438\n"
            + "Row #28: 17,809\n"
            + "Row #29: \n"
            + "Row #30: \n"
            + "Row #31: \n"
            + "Row #32: \n"
            + "Row #33: 18,294\n"
            + "Row #34: \n"
            + "Row #35: \n"
            + "Row #36: \n"
            + "Row #37: \n"
            + "Row #38: 18,632\n"
            + "Row #39: 29,905\n"
            + "Row #40: \n"
            + "Row #41: 18,369\n"
            + "Row #42: 1,555\n"
            + "Row #43: 18,159\n"
            + "Row #44: 16,925\n"
            + "Row #45: 25,453\n"
            + "Row #46: \n"
            + "Row #47: \n"
            + "Row #48: 1,622\n"
            + "Row #49: 8,192\n"
            + "Row #50: \n"
            + "Row #51: 442\n"
            + "Row #52: 3,950\n"
            + "Row #53: 4,479\n"
            + "Row #54: \n"
            + "Row #55: \n"
            + "Row #56: \n"
            + "Row #57: \n"
            + "Row #58: 4,947\n"
            + "Row #59: \n"
            + "Row #60: \n"
            + "Row #61: \n"
            + "Row #62: \n"
            + "Row #63: 5,076\n"
            + "Row #64: 7,940\n"
            + "Row #65: \n"
            + "Row #66: 4,706\n"
            + "Row #67: 387\n"
            + "Row #68: 4,639\n"
            + "Row #69: 4,428\n"
            + "Row #70: 6,712\n"
            + "Row #71: \n"
            + "Row #72: \n"
            + "Row #73: 390\n"
            + "Row #74: 2,140\n");
    }

    public void testCollapsedError() throws Exception {
        if (MondrianProperties.instance().UseAggregates.get() == false
            && MondrianProperties.instance().ReadAggregates.get() == false)
        {
            return;
        }
        final String cube =
            "<Cube name=\"Foo\" defaultMeasure=\"Unit Sales\">\n"
            + "  <Table name=\"sales_fact_1997\">\n"
            + "    <AggExclude name=\"agg_g_ms_pcat_sales_fact_1997\"/>"
            + "    <AggExclude name=\"agg_c_14_sales_fact_1997\"/>"
            + "    <AggExclude name=\"agg_pl_01_sales_fact_1997\"/>"
            + "    <AggExclude name=\"agg_ll_01_sales_fact_1997\"/>"
            + "    <AggName name=\"agg_l_05_sales_fact_1997\">"
            + "        <AggFactCount column=\"fact_count\"/>\n"
            + "        <AggIgnoreColumn column=\"customer_id\"/>\n"
            + "        <AggIgnoreColumn column=\"store_id\"/>\n"
            + "        <AggIgnoreColumn column=\"promotion_id\"/>\n"
            + "        <AggIgnoreColumn column=\"store_sales\"/>\n"
            + "        <AggIgnoreColumn column=\"store_cost\"/>\n"
            + "        <AggMeasure name=\"[Measures].[Unit Sales]\" column=\"unit_sales\" />\n"
            + "        <AggLevel name=\"[Product].[Product Id]\" column=\"product_id\" collapsed=\"true\"/>\n"
            + "    </AggName>\n"
            + "</Table>\n"
            + "<Dimension foreignKey=\"product_id\" name=\"Product\">\n"
            + "<Hierarchy hasAll=\"true\" primaryKey=\"product_id\" primaryKeyTable=\"product\">\n"
            + "  <Join leftKey=\"product_class_id\" rightKey=\"product_class_id\">\n"
            + " <Table name=\"product\"/>\n"
            + " <Table name=\"product_class\"/>\n"
            + "  </Join>\n"
            + "  <Level name=\"Product Family\" table=\"product_class\" column=\"product_family\"\n"
            + "   uniqueMembers=\"true\"/>\n"
            + "  <Level name=\"Product Department\" table=\"product_class\" column=\"product_department\"\n"
            + "   uniqueMembers=\"false\"/>\n"
            + "  <Level name=\"Product Category\" table=\"product_class\" column=\"product_category\"\n"
            + "   uniqueMembers=\"false\"/>\n"
            + "  <Level name=\"Product Subcategory\" table=\"product_class\" column=\"product_subcategory\"\n"
            + "   uniqueMembers=\"false\"/>\n"
            + "  <Level name=\"Brand Name\" table=\"product\" column=\"brand_name\" uniqueMembers=\"false\"/>\n"
            + "  <Level name=\"Product Name\" table=\"product\" column=\"product_name\"\n"
            + "   uniqueMembers=\"true\"/>\n"
            + "  <Level name=\"Product Id\" table=\"product\" column=\"product_id\"\n"
            + "   uniqueMembers=\"true\"/>\n"
            + "</Hierarchy>\n"
            + "</Dimension>\n"
            + "<Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\"\n"
            + "      formatString=\"Standard\"/>\n"
            + "</Cube>\n";
        final TestContext context =
            TestContext.instance().create(
                null, cube, null, null, null, null);
        context.assertQueryThrows(
            "select {[Product].[Product Family].Members} on rows, {[Measures].[Unit Sales]} on columns from [Foo]",
            "Too many errors, '1', while loading/reloading aggregates.");
    }

    /**
     * Test case for bug
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1047">MONDRIAN-1047,
     * "IllegalArgumentException when cube has closure tables and many
     * levels"</a>.
     */
    public void testBugMondrian1047() {
        // Test case only works under MySQL, due to how columns are quoted.
        switch (TestContext.instance().getDialect().getDatabaseProduct()) {
        case MARIADB:
        case MYSQL:
            break;
        default:
            return;
        }
        checkBugMondrian1047(100); // 115 bits
        checkBugMondrian1047(50); // 65 bits
        checkBugMondrian1047(49); // 64 bits
        checkBugMondrian1047(48); // 63 bits
        checkBugMondrian1047(113); // 128 bits
        checkBugMondrian1047(114); // 129 bits
    }


    public void checkBugMondrian1047(int n) {
        TestContext testContext =
            TestContext.instance().createSubstitutingCube(
                "HR",
                TestContext.repeatString(
                    n,
                    "<Dimension name='Position %1$d' foreignKey='employee_id'>\n"
                    + "  <Hierarchy hasAll='true' allMemberName='All Position' primaryKey='employee_id'>\n"
                    + "    <Table name='employee'/>\n"
                    + "    <Level name='Position Title' uniqueMembers='false' ordinalColumn='position_id'>\n"
                    + "      <KeyExpression><SQL dialect='generic'>`position_title` + %1$d</SQL></KeyExpression>\n"
                    + "    </Level>\n"
                    + "  </Hierarchy>\n"
                    + "</Dimension>"),
                null);
        testContext.assertQueryReturns(
            "select from [HR]",
            "Axis #0:\n"
            + "{}\n"
            + "$39,431.67");
    }

    /**
     * Test case for bug
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1065">MONDRIAN-1065,
     * Incorrect data column is used in the WHERE clause of the SQL when
     * using Oracle DB</a>.
     */
    public void testBugMondrian1065() {
        // Test case only works under Oracle
        switch (TestContext.instance().getDialect().getDatabaseProduct()) {
        case ORACLE:
            break;
        default:
            return;
        }
        TestContext testContext = TestContext.instance().createSubstitutingCube(
            "Sales",
            "  <Dimension name=\"PandaSteak\" foreignKey=\"promotion_id\">\n"
            + "    <Hierarchy hasAll=\"false\" primaryKey=\"lvl_3_id\">\n"
            + "      <InlineTable alias=\"meatShack\">\n"
            + "        <ColumnDefs>\n"
            + "          <ColumnDef name=\"lvl_1_id\" type=\"Integer\"/>\n"
            + "          <ColumnDef name=\"lvl_1_name\" type=\"String\"/>\n"
            + "          <ColumnDef name=\"lvl_2_id\" type=\"Integer\"/>\n"
            + "          <ColumnDef name=\"lvl_2_name\" type=\"String\"/>\n"
            + "          <ColumnDef name=\"lvl_3_id\" type=\"Integer\"/>\n"
            + "          <ColumnDef name=\"lvl_3_name\" type=\"String\"/>\n"
            + "        </ColumnDefs>\n"
            + "        <Rows>\n"
            + "          <Row>\n"
            + "            <Value column=\"lvl_1_id\">1</Value>\n"
            + "            <Value column=\"lvl_1_name\">level 1</Value>\n"
            + "            <Value column=\"lvl_2_id\">1</Value>\n"
            + "            <Value column=\"lvl_2_name\">level 2 - 1</Value>\n"
            + "            <Value column=\"lvl_3_id\">112</Value>\n"
            + "            <Value column=\"lvl_3_name\">level 3 - 1</Value>\n"
            + "          </Row>\n"
            + "          <Row>\n"
            + "            <Value column=\"lvl_1_id\">1</Value>\n"
            + "            <Value column=\"lvl_1_name\">level 1</Value>\n"
            + "            <Value column=\"lvl_2_id\">1</Value>\n"
            + "            <Value column=\"lvl_2_name\">level 2 - 1</Value>\n"
            + "            <Value column=\"lvl_3_id\">114</Value>\n"
            + "            <Value column=\"lvl_3_name\">level 3 - 2</Value>\n"
            + "          </Row>\n"
            + "        </Rows>\n"
            + "      </InlineTable>\n"
            + "      <Level name=\"Level1\" column=\"lvl_1_id\" nameColumn=\"lvl_1_name\" />\n"
            + "      <Level name=\"Level2\" column=\"lvl_2_id\" nameColumn=\"lvl_2_name\" />\n"
            + "      <Level name=\"Level3\" column=\"lvl_3_id\" nameColumn=\"lvl_3_name\" />\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n");
        testContext.assertQueryReturns(
            "select non empty crossjoin({[PandaSteak].[Level3].[level 3 - 1], [PandaSteak].[Level3].[level 3 - 2]}, {[Measures].[Unit Sales], [Measures].[Store Cost]}) on columns, {[Product].[Product Family].Members} on rows from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[PandaSteak].[level 1].[level 2 - 1].[level 3 - 1], [Measures].[Unit Sales]}\n"
            + "{[PandaSteak].[level 1].[level 2 - 1].[level 3 - 1], [Measures].[Store Cost]}\n"
            + "{[PandaSteak].[level 1].[level 2 - 1].[level 3 - 2], [Measures].[Unit Sales]}\n"
            + "{[PandaSteak].[level 1].[level 2 - 1].[level 3 - 2], [Measures].[Store Cost]}\n"
            + "Axis #2:\n"
            + "{[Product].[Drink]}\n"
            + "{[Product].[Food]}\n"
            + "{[Product].[Non-Consumable]}\n"
            + "Row #0: 5\n"
            + "Row #0: 3.50\n"
            + "Row #0: 9\n"
            + "Row #0: 7.70\n"
            + "Row #1: 27\n"
            + "Row #1: 20.77\n"
            + "Row #1: 46\n"
            + "Row #1: 39.88\n"
            + "Row #2: 10\n"
            + "Row #2: 9.63\n"
            + "Row #2: 17\n"
            + "Row #2: 16.21\n");
    }

    /**
     * This is a test for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1390">MONDRIAN-1390</a>
     *
     * <p>Calling {@link SchemaReader#getLevelMembers(Level, boolean)}
     * directly would return the null members at the end, since it was
     * using TupleReader#readTuples instead of TupleReader#readMembers.
     */
    public void testMondrian1390() throws Exception {
        Schema schema = getConnection().getSchema();
        Cube salesCube = schema.lookupCube("Sales", true);
        SchemaReader sr = salesCube.getSchemaReader(null).withLocus();
        List<Member> members = sr.getLevelMembers(
            (Level)Util.lookupCompound(
                sr,
                salesCube,
                Util.parseIdentifier(
                    "[Store Size in SQFT].[Store Sqft]"),
                true,
                Category.Level),
            true);
        assertEquals(
            "[[Store Size in SQFT].[#null], "
            + "[Store Size in SQFT].[20319], "
            + "[Store Size in SQFT].[21215], "
            + "[Store Size in SQFT].[22478], "
            + "[Store Size in SQFT].[23112], "
            + "[Store Size in SQFT].[23593], "
            + "[Store Size in SQFT].[23598], "
            + "[Store Size in SQFT].[23688], "
            + "[Store Size in SQFT].[23759], "
            + "[Store Size in SQFT].[24597], "
            + "[Store Size in SQFT].[27694], "
            + "[Store Size in SQFT].[28206], "
            + "[Store Size in SQFT].[30268], "
            + "[Store Size in SQFT].[30584], "
            + "[Store Size in SQFT].[30797], "
            + "[Store Size in SQFT].[33858], "
            + "[Store Size in SQFT].[34452], "
            + "[Store Size in SQFT].[34791], "
            + "[Store Size in SQFT].[36509], "
            + "[Store Size in SQFT].[38382], "
            + "[Store Size in SQFT].[39696]]",
            members.toString());
    }

    public void testMondrian1499() throws Exception {
        propSaver.set(propSaver.properties.UseAggregates, false);
        propSaver.set(propSaver.properties.ReadAggregates, false);
        final TestContext woAlias =
            TestContext.instance().withSchema(
                "<?xml version='1.0'?>\n"
                + "<Schema name='FoodMart'>\n"
                + "<Cube name=\"HR\">\n"
                + "  <Table name=\"salary\"/>\n"
                + "  <Dimension name=\"Store\" foreignKey=\"employee_id\" >\n"
                + "    <Hierarchy hasAll=\"true\" primaryKey=\"employee_id\"\n"
                + "        primaryKeyTable=\"employee\">\n"
                + "      <Join leftKey=\"store_id\" rightKey=\"store_id\">\n"
                + "        <Table name=\"employee\">\n"
                + "         <SQL>1 = 1</SQL>\n"
                + "     </Table>\n"
                + "        <Table name=\"store\"/>\n"
                + "      </Join>\n"
                + "      <Level name=\"Store Country\" table=\"store\" column=\"store_country\"\n"
                + "          uniqueMembers=\"true\"/>\n"
                + "      <Level name=\"Store State\" table=\"store\" column=\"store_state\"\n"
                + "          uniqueMembers=\"true\"/>\n"
                + "      <Level name=\"Store City\" table=\"store\" column=\"store_city\"\n"
                + "          uniqueMembers=\"false\"/>\n"
                + "      <Level name=\"Store Name\" table=\"store\" column=\"store_name\"\n"
                + "          uniqueMembers=\"true\">\n"
                + "        <Property name=\"Store Type\" column=\"store_type\"/>\n"
                + "        <Property name=\"Store Manager\" column=\"store_manager\"/>\n"
                + "        <Property name=\"Store Sqft\" column=\"store_sqft\" type=\"Numeric\"/>\n"
                + "        <Property name=\"Grocery Sqft\" column=\"grocery_sqft\" type=\"Numeric\"/>\n"
                + "        <Property name=\"Frozen Sqft\" column=\"frozen_sqft\" type=\"Numeric\"/>\n"
                + "        <Property name=\"Meat Sqft\" column=\"meat_sqft\" type=\"Numeric\"/>\n"
                + "        <Property name=\"Has coffee bar\" column=\"coffee_bar\" type=\"Boolean\"/>\n"
                + "        <Property name=\"Street address\" column=\"store_street_address\"\n"
                + "            type=\"String\"/>\n"
                + "      </Level>\n"
                + "    </Hierarchy>\n"
                + "  </Dimension>\n"
                + "  <Dimension name=\"Pay Type\" foreignKey=\"employee_id\">\n"
                + "    <Hierarchy hasAll=\"true\" primaryKey=\"employee_id\"\n"
                + "        primaryKeyTable=\"employee\">\n"
                + "      <Join leftKey=\"position_id\" rightKey=\"position_id\">\n"
                + "        <Table name=\"employee\">\n"
                + "       <SQL>1 = 1</SQL>\n"
                + "     </Table>\n"
                + "        <Table name=\"position\"/>\n"
                + "      </Join>\n"
                + "      <Level name=\"Pay Type\" table=\"position\" column=\"pay_type\"\n"
                + "          uniqueMembers=\"true\"/>\n"
                + "    </Hierarchy>\n"
                + "  </Dimension>\n"
                + "  <Dimension name=\"Store Type\" foreignKey=\"employee_id\">\n"
                + "    <Hierarchy hasAll=\"true\" primaryKeyTable=\"employee\" primaryKey=\"employee_id\">\n"
                + "      <Join leftKey=\"store_id\" rightKey=\"store_id\">\n"
                + "        <Table name=\"employee\">\n"
                + "       <SQL>1 = 1</SQL>\n"
                + "     </Table>\n"
                + "        <Table name=\"store\"/>\n"
                + "      </Join>\n"
                + "      <Level name=\"Store Type\" table=\"store\" column=\"store_type\"\n"
                + "          uniqueMembers=\"true\"/>\n"
                + "    </Hierarchy>\n"
                + "  </Dimension>\n"
                + "  <Dimension name=\"Position\" foreignKey=\"employee_id\">\n"
                + "    <Hierarchy hasAll=\"true\" allMemberName=\"All Position\"\n"
                + "        primaryKey=\"employee_id\">\n"
                + "      <Table name=\"employee\">\n"
                + "     <SQL>1 = 1</SQL>\n"
                + "   </Table>\n"
                + "      <Level name=\"Management Role\" uniqueMembers=\"true\"\n"
                + "          column=\"management_role\"/>\n"
                + "      <Level name=\"Position Title\" uniqueMembers=\"false\"\n"
                + "          column=\"position_title\" ordinalColumn=\"position_id\"/>\n"
                + "    </Hierarchy>\n"
                + "  </Dimension>\n"
                + "  <Dimension name=\"Employees\" foreignKey=\"employee_id\">\n"
                + "    <Hierarchy hasAll=\"true\" allMemberName=\"All Employees\"\n"
                + "        primaryKey=\"employee_id\">\n"
                + "      <Table name=\"employee\">\n"
                + "     <SQL>1 = 1</SQL>\n"
                + "   </Table>\n"
                + "      <Level name=\"Employee Id\" type=\"Numeric\" uniqueMembers=\"true\"\n"
                + "          column=\"employee_id\" parentColumn=\"supervisor_id\"\n"
                + "          nameColumn=\"full_name\" nullParentValue=\"0\">\n"
                + "        <Closure parentColumn=\"supervisor_id\" childColumn=\"employee_id\">\n"
                + "          <Table name=\"employee_closure\"/>\n"
                + "        </Closure>\n"
                + "        <Property name=\"Marital Status\" column=\"marital_status\"/>\n"
                + "        <Property name=\"Position Title\" column=\"position_title\"/>\n"
                + "        <Property name=\"Gender\" column=\"gender\"/>\n"
                + "        <Property name=\"Salary\" column=\"salary\"/>\n"
                + "        <Property name=\"Education Level\" column=\"education_level\"/>\n"
                + "        <Property name=\"Management Role\" column=\"management_role\"/>\n"
                + "      </Level>\n"
                + "    </Hierarchy>\n"
                + "  </Dimension>\n"
                + "  <Measure name=\"Org Salary\" column=\"salary_paid\" aggregator=\"sum\"\n"
                + "      formatString=\"Currency\"/>\n"
                + "</Cube>\n"
                + "</Schema>");
        woAlias.assertQueryReturns(
            "select {[Measures].[Org Salary]} on columns,\n"
            + "{[Store].[Store Country].Members} on rows from [HR]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Org Salary]}\n"
            + "Axis #2:\n"
            + "{[Store].[Canada]}\n"
            + "{[Store].[Mexico]}\n"
            + "{[Store].[USA]}\n"
            + "Row #0: $7,473.54\n"
            + "Row #1: $180,599.76\n"
            + "Row #2: $83,479.14\n");
    }

    /**
    * Testcase for bug
    * <a href="http://jira.pentaho.com/browse/MONDRIAN-1073">MONDRIAN-1073,
    * "Two cubes operating on same fact table gives wrong WHERE clause"</a>.
    */
    public void testMondrian1073() throws Exception {
        final String schema = TestContext.instance()
                .getSchema(
                    null, CUBES_AB,
                    null, null, null, null);
        final TestContext testContextWithCubeAAndCubeB =
            TestContext.instance().withSchema(schema).withCube("CubeB");

        testContextWithCubeAAndCubeB.assertQueryReturns(
            "SELECT [Measures].[Fantastic Count for Different Types of Promotion] ON COLUMNS\n"
            + "FROM [CubeB]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Fantastic Count for Different Types of Promotion]}\n"
            + "Row #0: 22\n");
  }

    public void testMultiByteSchemaReadFromFile() throws IOException {
        String rawSchema = TestContext.getRawFoodMartSchema().replace(
            "<Hierarchy hasAll=\"true\" allMemberName=\"All Gender\" primaryKey=\"customer_id\">",
            "<Hierarchy name=\"地域\" hasAll=\"true\" allMemberName=\"All Gender\" primaryKey=\"customer_id\">");
        File schemaFile = File.createTempFile("multiByteSchema", ",xml");
        schemaFile.deleteOnExit();
        FileOutputStream output = new FileOutputStream(schemaFile);
        IOUtils.write(rawSchema, output);
        output.close();
        TestContext context = getTestContext();
        final Util.PropertyList properties =
            context.getConnectionProperties().clone();
        properties.put(
            RolapConnectionProperties.Catalog.name(),
            schemaFile.getAbsolutePath());
        context.withProperties(properties).assertQueryReturns(
            "select [Gender].members on 0 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Gender.地域].[All Gender]}\n"
            + "{[Gender.地域].[F]}\n"
            + "{[Gender.地域].[M]}\n"
            + "Row #0: 266,773\n"
            + "Row #0: 131,558\n"
            + "Row #0: 135,215\n");
    }

    public void testBugMonrian2528() {
      // Default member [Measures].[Unit Sales] is denied for the current role.
      // Before the fix ClassCastException was thrown on query.
      final TestContext testContext = TestContext.instance().create(
          null, null, null, null, null,
          "<Role name=\"admin\">\n"
          + "  <SchemaGrant access=\"all\">\n"
          + "  </SchemaGrant>\n"
          + "</Role>\n"
          + "<Role name=\"dev\">\n"
          + "  <SchemaGrant access=\"all\">\n"
          + "    <CubeGrant cube=\"Sales\" access=\"all\">\n"
          + "      <HierarchyGrant hierarchy=\"[Measures]\""
          + " access=\"custom\">\n"
          + "        <MemberGrant member=\"[Measures].[Store Cost]\""
          + " access=\"all\">\n"
          + "        </MemberGrant>\n"
          + "        <MemberGrant member=\"[Measures].[Store Sales]\""
          + " access=\"all\">\n"
          + "        </MemberGrant>\n"
          + "        <MemberGrant member=\"[Measures].[Sales Count]\""
          + " access=\"all\">\n"
          + "        </MemberGrant>\n"
          + "      </HierarchyGrant>\n"
          + "    </CubeGrant>\n"
          + "  </SchemaGrant>\n"
          + "</Role>\n").withRole("dev");

      testContext.assertQueryReturns(
          "SELECT\n"
          + "[Product].[All Products] ON 0,\n"
          + "[Measures].[Store Sales] ON 1\n"
          + "FROM [Sales]\n"
          + "WHERE FILTER([Store Type].children, [Store Type].CURRENTMEMBER NOT IN {[Store Type].[Deluxe Supermarket], [Store Type].[Gourmet Supermarket]})\n",
          "Axis #0:\n"
          + "{[Store Type].[HeadQuarters]}\n"
          + "{[Store Type].[Mid-Size Grocery]}\n"
          + "{[Store Type].[Small Grocery]}\n"
          + "{[Store Type].[Supermarket]}\n"
          + "Axis #1:\n"
          + "{[Product].[All Products]}\n"
          + "Axis #2:\n"
          + "{[Measures].[Store Sales]}\n"
          + "Row #0: 357,425.65\n"
);
  }

    public void testMondrian1275() throws Exception {
        final TestContext tc =
                getTestContext()
                        .withSchema(
                                "<?xml version=\"1.0\"?>\n"
                                        + "<Schema name=\"FoodMart\">\n"
                                        + "  <Dimension name=\"Store Type\">\n"
                                        + "    <Annotations>\n"
                                        + "      <Annotation name=\"foo\">bar</Annotation>\n"
                                        + "    </Annotations>\n"
                                        + "    <Hierarchy hasAll=\"true\" primaryKey=\"store_id\">\n"
                                        + "      <Table name=\"store\"/>\n"
                                        + "      <Level name=\"Store Type\" column=\"store_type\" uniqueMembers=\"true\"/>\n"
                                        + "    </Hierarchy>\n"
                                        + "  </Dimension>\n"
                                        + "<Cube name=\"Sales\" defaultMeasure=\"Unit Sales\">\n"
                                        + "  <Table name=\"sales_fact_1997\">\n"
                                        + "    <AggExclude name=\"agg_c_special_sales_fact_1997\" />\n"
                                        + "    <AggExclude name=\"agg_lc_100_sales_fact_1997\" />\n"
                                        + "    <AggExclude name=\"agg_lc_10_sales_fact_1997\" />\n"
                                        + "    <AggExclude name=\"agg_pc_10_sales_fact_1997\" />\n"
                                        + "  </Table>\n"
                                        + "  <DimensionUsage name=\"Store Type\" source=\"Store Type\" foreignKey=\"store_id\"/>\n"
                                        + "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\"\n"
                                        + "      formatString=\"Standard\"/>\n"
                                        + "</Cube>\n"
                                        + "</Schema>\n");

        final RolapConnection rolapConn = tc.getOlap4jConnection().unwrap(RolapConnection.class);
        final SchemaReader schemaReader = rolapConn.getSchemaReader();
        final RolapSchema schema = schemaReader.getSchema();
        for (RolapCube cube : schema.getCubeList()) {
            Dimension dim = cube.getDimensions()[1];
            final Map<String, Annotation> annotations = dim.getAnnotationMap();
            Assert.assertEquals(1, annotations.size());
            Assert.assertEquals("bar", annotations.get("foo").getValue());
        }
    }

}

// End SchemaTest.java
