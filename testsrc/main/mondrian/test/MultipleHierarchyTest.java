/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.test;

import mondrian.olap.MondrianProperties;

/**
 * Tests multiple hierarchies within the same dimension.
 *
 * @author jhyde
 * @since Dec 15, 2005
 */
public class MultipleHierarchyTest extends FoodMartTestCase {
    private static final String timeWeekly =
        TestContext.hierarchyName("Time", "Weekly");
    private static final String timeTime =
        TestContext.hierarchyName("Time", "Time");

    public MultipleHierarchyTest(String name) {
        super(name);
    }

    public void testWeekly() {
        // [Time.Weekly] has an 'all' member, but [Time] does not.
        assertAxisReturns(
            "{[Time].[Time].CurrentMember}",
            "[Time].[Time].[1997]");
        assertAxisReturns(
            "{[Time].[Weekly].CurrentMember}",
            "[Time].[Weekly].[All Weeklys]");
    }

    public void testWeekly2() {
        // When the context is one hierarchy,
        // the current member of other hierarchy must be its default member.
        assertQueryReturns(
            "with\n"
            + "  member [Measures].[Foo] as ' "
            + timeWeekly + ".CurrentMember.UniqueName '\n"
            + "  member [Measures].[Foo2] as ' "
            + timeTime + ".CurrentMember.UniqueName '\n"
            + "select\n"
            + "  {[Measures].[Unit Sales], [Measures].[Foo], [Measures].[Foo2]} on columns,\n"
            + "  {" + timeTime + ".children} on rows\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Foo]}\n"
            + "{[Measures].[Foo2]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[1997].[Q1]}\n"
            + "{[Time].[Time].[1997].[Q2]}\n"
            + "{[Time].[Time].[1997].[Q3]}\n"
            + "{[Time].[Time].[1997].[Q4]}\n"
            + "Row #0: 66,291\n"
            + "Row #0: [Time].[Weekly].[All Weeklys]\n"
            + "Row #0: [Time].[Time].[1997].[Q1]\n"
            + "Row #1: 62,610\n"
            + "Row #1: [Time].[Weekly].[All Weeklys]\n"
            + "Row #1: [Time].[Time].[1997].[Q2]\n"
            + "Row #2: 65,848\n"
            + "Row #2: [Time].[Weekly].[All Weeklys]\n"
            + "Row #2: [Time].[Time].[1997].[Q3]\n"
            + "Row #3: 72,024\n"
            + "Row #3: [Time].[Weekly].[All Weeklys]\n"
            + "Row #3: [Time].[Time].[1997].[Q4]\n");
    }

    public void testMultipleMembersOfSameDimensionInSlicerFails() {
        assertQueryThrows(
            "select {[Measures].[Unit Sales]} on columns,\n"
            + " {[Store].[Stores].children} on rows\n"
            + "from [Sales]\n"
            + "where ([Gender].[M], [Time].[1997], [Time].[1997].[Q1])",
            "Tuple contains more than one member of hierarchy '[Time].[Time]'.");
    }

    public void testMembersOfHierarchiesInSameDimensionInSlicer() {
        assertQueryReturns(
            "select {[Measures].[Unit Sales]} on columns,\n"
            + " {[Store].[Stores].children} on rows\n"
            + "from [Sales]\n"
            + "where ([Gender].[M], "
            + TestContext.hierarchyName("Time", "Weekly")
            + ".[1997], [Time].[1997].[Q1])",
            "Axis #0:\n"
            + "{[Customer].[Gender].[M], [Time].[Weekly].[1997], [Time].[Time].[1997].[Q1]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Store].[Stores].[Canada]}\n"
            + "{[Store].[Stores].[Mexico]}\n"
            + "{[Store].[Stores].[USA]}\n"
            + "Row #0: \n"
            + "Row #1: \n"
            + "Row #2: 33,381\n");
    }

    public void testCalcMember() {
        assertQueryReturns(
            "with member [Measures].[Sales to Date] as \n"
            + " ' Sum(PeriodsToDate([Time].[Year], [Time].[Time].CurrentMember), [Measures].[Unit Sales])'\n"
            + "select {[Measures].[Sales to Date]} on columns,\n"
            + " {[Time].[1997].[Q2].[4],"
            + "  [Time].[1997].[Q2].[5]} on rows\n"
            + "from [Sales]",
            // msas give 86740, 107551
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Sales to Date]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[1997].[Q2].[4]}\n"
            + "{[Time].[Time].[1997].[Q2].[5]}\n"
            + "Row #0: 86,470\n"
            + "Row #1: 107,551\n");

        assertQueryReturns(
            "with member [Measures].[Sales to Date] as \n"
            + " ' Sum(PeriodsToDate(" + timeWeekly + ".[Year], "
            + timeWeekly + ".CurrentMember), [Measures].[Unit Sales])'\n"
            + "select {[Measures].[Sales to Date]} on columns,\n"
            + " {" + timeWeekly + ".[1997].[14] : "
            + timeWeekly + ".[1997].[16]} on rows\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Sales to Date]}\n"
            + "Axis #2:\n"
            + "{[Time].[Weekly].[1997].[14]}\n"
            + "{[Time].[Weekly].[1997].[15]}\n"
            + "{[Time].[Weekly].[1997].[16]}\n"
            + "Row #0: 81,670\n"
            + "Row #1: 86,300\n"
            + "Row #2: 90,139\n");
    }

    /**
     * Tests <a href="http://jira.pentaho.com/browse/MONDRIAN-191">
     * bug MONDRIAN-191, "Properties not working with multiple hierarchies"</a>.
     */
    public void testProperty() {
        TestContext testContext =
            TestContext.instance().legacy().createSubstitutingCube(
                "Sales",
                "<Dimension name=\"NuStore\" foreignKey=\"store_id\">\n"
                + "<Hierarchy hasAll=\"true\" primaryKey=\"store_id\">\n"
                + "  <Table name=\"store\"/>\n"
                + "  <Level name=\"NuStore Country\" column=\"store_country\" uniqueMembers=\"true\"/>\n"
                + "  <Level name=\"NuStore State\" column=\"store_state\" uniqueMembers=\"true\"/>\n"
                + "  <Level name=\"NuStore City\" column=\"store_city\" uniqueMembers=\"false\"/>\n"
                + "  <Level name=\"NuStore Name\" column=\"store_name\" uniqueMembers=\"true\">\n"
                + "    <Property name=\"NuStore Type\" column=\"store_type\"/>\n"
                + "    <Property name=\"NuStore Manager\" column=\"store_manager\"/>\n"
                + "    <Property name=\"NuStore Sqft\" column=\"store_sqft\" type=\"Numeric\"/>\n"
                + "    <Property name=\"Grocery Sqft\" column=\"grocery_sqft\" type=\"Numeric\"/>\n"
                + "    <Property name=\"Frozen Sqft\" column=\"frozen_sqft\" type=\"Numeric\"/>\n"
                + "    <Property name=\"Meat Sqft\" column=\"meat_sqft\" type=\"Numeric\"/>\n"
                + "    <Property name=\"Has coffee bar\" column=\"coffee_bar\" type=\"Boolean\"/>\n"
                + "    <Property name=\"Street address\" column=\"store_street_address\" type=\"String\"/>\n"
                + "  </Level>\n"
                + "</Hierarchy>\n"
                + "<Hierarchy caption=\"NuStore2\" name=\"NuStore2\" allMemberName=\"All NuStore2s\" hasAll=\"true\" primaryKey=\"store_id\">\n"
                + "  <Table name=\"store\"/>\n"
                + "  <Level name=\"NuStore City\" column=\"store_city\" uniqueMembers=\"true\"/>\n"
                + "  <Level name=\"NuStore Name\" column=\"store_name\"  uniqueMembers=\"true\">\n"
                + "    <Property name=\"NuStore Type\" column=\"store_type\"/>\n"
                + "    <Property name=\"NuStore Manager\" column=\"store_manager\"/>\n"
                + "    <Property name=\"NuStore Sqft\" column=\"store_sqft\" type=\"Numeric\"/>\n"
                + "    <Property name=\"Grocery Sqft\" column=\"grocery_sqft\" type=\"Numeric\"/>\n"
                + "    <Property name=\"Frozen Sqft\" column=\"frozen_sqft\" type=\"Numeric\"/>\n"
                + "    <Property name=\"Meat Sqft\" column=\"meat_sqft\" type=\"Numeric\"/>\n"
                + "    <Property name=\"Has coffee bar\" column=\"coffee_bar\" type=\"Boolean\"/>\n"
                + "    <Property name=\"Street address\" column=\"store_street_address\" type=\"String\"/>\n"
                + "  </Level>\n"
                + "</Hierarchy>\n"
                + "</Dimension>");
        testContext.assertQueryReturns(
            "with member [Measures].[Store level] as '[NuStore].[NuStore]"
            + ".CurrentMember.Level.Name'\n"
            + "member [Measures].[Store type] as 'IIf(([NuStore].[NuStore]"
            + ".CurrentMember.Level.Name = \"NuStore Name\"), CAST([NuStore].[NuStore]"
            + ".CurrentMember.Properties(\"NuStore Type\") AS STRING), \"No type\")'\n"
            + "member [Measures].[Store Sqft] as 'IIf(([NuStore].[NuStore]"
            + ".CurrentMember.Level.Name = \"NuStore Name\"), CAST([NuStore].[NuStore]"
            + ".CurrentMember.Properties(\"NuStore Sqft\") AS INTEGER), 0.0)'\n"
            + "select {"
            + "[Measures].[Unit Sales], "
            + "[Measures].[Store Cost], "
            + "[Measures].[Store Sales], "
            + "[Measures].[Store level], "
            + "[Measures].[Store type], "
            + "[Measures].[Store Sqft]"
            + "} ON COLUMNS,\n"
            + "{"
            + "[NuStore].[NuStore].[All NuStores], "
            + "[NuStore].[NuStore].[Canada], "
            + "[NuStore].[NuStore].[Canada].[BC], "
            + "[NuStore].[NuStore].[Canada].[BC].[Vancouver], "
            + "[NuStore].[NuStore].[Canada].[BC].[Vancouver].[Store 19], "
            + "[NuStore].[NuStore].[Canada].[BC].[Victoria], "
            + "[NuStore].[NuStore].[Mexico], "
            + "[NuStore].[NuStore].[USA]"
            + "} ON ROWS\n"
            + "from [Sales]\n"
            + "where [Time].[1997] ",
            "Axis #0:\n"
            + "{[Time].[Time].[1997]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Store Cost]}\n"
            + "{[Measures].[Store Sales]}\n"
            + "{[Measures].[Store level]}\n"
            + "{[Measures].[Store type]}\n"
            + "{[Measures].[Store Sqft]}\n"
            + "Axis #2:\n"
            + "{[NuStore].[NuStore].[All NuStores]}\n"
            + "{[NuStore].[NuStore].[Canada]}\n"
            + "{[NuStore].[NuStore].[Canada].[BC]}\n"
            + "{[NuStore].[NuStore].[Canada].[BC].[Vancouver]}\n"
            + "{[NuStore].[NuStore].[Canada].[BC].[Vancouver].[Store 19]}\n"
            + "{[NuStore].[NuStore].[Canada].[BC].[Victoria]}\n"
            + "{[NuStore].[NuStore].[Mexico]}\n"
            + "{[NuStore].[NuStore].[USA]}\n"
            + "Row #0: 266,773\n"
            + "Row #0: 225,627.23\n"
            + "Row #0: 565,238.13\n"
            + "Row #0: (All)\n"
            + "Row #0: No type\n"
            + "Row #0: 0\n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: NuStore Country\n"
            + "Row #1: No type\n"
            + "Row #1: 0\n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: NuStore State\n"
            + "Row #2: No type\n"
            + "Row #2: 0\n"
            + "Row #3: \n"
            + "Row #3: \n"
            + "Row #3: \n"
            + "Row #3: NuStore City\n"
            + "Row #3: No type\n"
            + "Row #3: 0\n"
            + "Row #4: \n"
            + "Row #4: \n"
            + "Row #4: \n"
            + "Row #4: NuStore Name\n"
            + "Row #4: Deluxe Supermarket\n"
            + "Row #4: 23,112\n"
            + "Row #5: \n"
            + "Row #5: \n"
            + "Row #5: \n"
            + "Row #5: NuStore City\n"
            + "Row #5: No type\n"
            + "Row #5: 0\n"
            + "Row #6: \n"
            + "Row #6: \n"
            + "Row #6: \n"
            + "Row #6: NuStore Country\n"
            + "Row #6: No type\n"
            + "Row #6: 0\n"
            + "Row #7: 266,773\n"
            + "Row #7: 225,627.23\n"
            + "Row #7: 565,238.13\n"
            + "Row #7: NuStore Country\n"
            + "Row #7: No type\n"
            + "Row #7: 0\n");
    }

    /**
     * Tests that mondrian detects an ambiguous hierarchy in a calculated member
     * at compile time. (SSAS detects at run time, and generates a cell error,
     * but this is better.)
     */
    public void testAmbiguousHierarchyInCalcMember() {
        final String query =
            "with member [Measures].[Time Child Count] as\n"
            + "  [Time].Children.Count\n"
            + "select [Measures].[Time Child Count] on 0\n"
            + "from [Sales]";
        assertQueryThrows(
            query,
            "The 'Time' dimension contains more than one hierarchy, "
            + "therefore the hierarchy must be explicitly specified.");
    }

    /**
     * Tests <a href="http://jira.pentaho.com/browse/MONDRIAN-750">
     * bug MONDRIAN-750, "... multiple hierarchies beneath a single dimension
     * throws exception"</a>.
     */
    public void testDefaultNamedHierarchy() {
        TestContext testContext =
            TestContext.instance().legacy().createSubstitutingCube(
                "Sales",
                "<Dimension name=\"NuStore\" foreignKey=\"store_id\">\n"
                + "<Hierarchy name=\"NuStore\" hasAll=\"true\" primaryKey=\"store_id\">\n"
                + "  <Table name=\"store\"/>\n"
                + "  <Level name=\"NuStore Country\" column=\"store_country\" uniqueMembers=\"true\"/>\n"
                + "  <Level name=\"NuStore State\" column=\"store_state\" uniqueMembers=\"true\"/>\n"
                + "  <Level name=\"NuStore City\" column=\"store_city\" uniqueMembers=\"false\"/>\n"
                + "  <Level name=\"NuStore Name\" column=\"store_name\" uniqueMembers=\"true\"/>\n"
                + "</Hierarchy>\n"
                + "<Hierarchy caption=\"NuStore2\" name=\"NuStore2\" allMemberName=\"All NuStore2s\" hasAll=\"true\" primaryKey=\"store_id\">\n"
                + "  <Table name=\"store\"/>\n"
                + "  <Level name=\"NuStore City\" column=\"store_city\" uniqueMembers=\"false\"/>\n"
                + "  <Level name=\"NuStore Name\" column=\"store_name\"  uniqueMembers=\"true\"/>\n"
                + "</Hierarchy>\n"
                + "</Dimension>");

        testContext.assertQueryReturns(
            "with set [*NATIVE_CJ_SET] as '[*BASE_MEMBERS_NuStore]' "
            + "set [*SORTED_ROW_AXIS] as 'Order([*CJ_ROW_AXIS], "
            + "[NuStore].[NuStore].CurrentMember.OrderKey, BASC)' "
            + "set [*BASE_MEMBERS_NuStore] as '"
            + "[NuStore].[NuStore].[NuStore Country].Members' "
            + "set [*BASE_MEMBERS_Measures] as '{[Measures].[*ZERO]}' "
            + "set [*CJ_ROW_AXIS] as 'Generate([*NATIVE_CJ_SET], {"
            + "[NuStore].[NuStore].CurrentMember})' "
            + "set [*CJ_COL_AXIS] as '[*NATIVE_CJ_SET]' "
            + "member [Measures].[*ZERO] as '0.0', SOLVE_ORDER = 0.0 "
            + "select [*BASE_MEMBERS_Measures] ON COLUMNS, "
            + "[*SORTED_ROW_AXIS] ON ROWS "
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[*ZERO]}\n"
            + "Axis #2:\n"
            + "{[NuStore].[NuStore].[Canada]}\n"
            + "{[NuStore].[NuStore].[Mexico]}\n"
            + "{[NuStore].[NuStore].[USA]}\n"
            + "Row #0: 0\n"
            + "Row #1: 0\n"
            + "Row #2: 0\n");
    }
}

// End MultipleHierarchyTest.java
