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
}

// End SchemaTest.java
