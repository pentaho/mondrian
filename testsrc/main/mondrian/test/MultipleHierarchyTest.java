/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.test;

/**
 * Tests multiple hierarchies within the same dimension.
 *
 * @author jhyde
 * @version $Id$
 * @since Dec 15, 2005
 */
public class MultipleHierarchyTest extends FoodMartTestCase {
    public MultipleHierarchyTest(String name) {
        super(name);
    }

    public void testWeekly() {
        // [Time.Weekly] has an 'all' member, but [Time] does not.
        assertAxisReturns("{[Time].CurrentMember}", "[Time].[1997]");
        assertAxisReturns("{[Time.Weekly].CurrentMember}", "[Time].[Weekly].[All Weeklys]");
    }

    public void testWeekly2() {
        // When the context is one hierarchy,
        // the current member of other hierarchy must be its default member.
        assertQueryReturns(
                fold(
                    "with\n" +
                    "  member [Measures].[Foo] as ' [Time.Weekly].CurrentMember.UniqueName '\n" +
                    "  member [Measures].[Foo2] as ' [Time].CurrentMember.UniqueName '\n" +
                    "select\n" +
                    "  {[Measures].[Unit Sales], [Measures].[Foo], [Measures].[Foo2]} on columns,\n" +
                    "  {[Time].children} on rows\n" +
                    "from [Sales]"),
                fold(
                    "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Unit Sales]}\n" +
                    "{[Measures].[Foo]}\n" +
                    "{[Measures].[Foo2]}\n" +
                    "Axis #2:\n" +
                    "{[Time].[1997].[Q1]}\n" +
                    "{[Time].[1997].[Q2]}\n" +
                    "{[Time].[1997].[Q3]}\n" +
                    "{[Time].[1997].[Q4]}\n" +
                    "Row #0: 66,291\n" +
                    "Row #0: [Time].[Weekly].[All Weeklys]\n" +
                    "Row #0: [Time].[1997].[Q1]\n" +
                    "Row #1: 62,610\n" +
                    "Row #1: [Time].[Weekly].[All Weeklys]\n" +
                    "Row #1: [Time].[1997].[Q2]\n" +
                    "Row #2: 65,848\n" +
                    "Row #2: [Time].[Weekly].[All Weeklys]\n" +
                    "Row #2: [Time].[1997].[Q3]\n" +
                    "Row #3: 72,024\n" +
                    "Row #3: [Time].[Weekly].[All Weeklys]\n" +
                    "Row #3: [Time].[1997].[Q4]\n"));
    }

    public void testMultipleSlicersFails() {
        assertThrows(fold(
            "select {[Measures].[Unit Sales]} on columns,\n" +
            " {[Store].children} on rows\n" +
            "from [Sales]\n" +
            "where ([Gender].[M], [Time.Weekly].[1997], [Time].[1997])"),
            "Tuple contains more than one member of dimension '[Time]'.");
    }

    public void testCalcMember() {
        assertQueryReturns(
                "with member [Measures].[Sales to Date] as \n" +
                " ' Sum(PeriodsToDate([Time].[Year], [Time].CurrentMember), [Measures].[Unit Sales])'\n" +
                "select {[Measures].[Sales to Date]} on columns,\n" +
                " {[Time].[1997].[Q2].[4]," +
                "  [Time].[1997].[Q2].[5]} on rows\n" +
                "from [Sales]",
                // msas give 86740, 107551
                fold(
                    "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Sales to Date]}\n" +
                    "Axis #2:\n" +
                    "{[Time].[1997].[Q2].[4]}\n" +
                    "{[Time].[1997].[Q2].[5]}\n" +
                    "Row #0: 86,470\n" +
                    "Row #1: 107,551\n"));

        assertQueryReturns(
                "with member [Measures].[Sales to Date] as \n" +
                " ' Sum(PeriodsToDate([Time.Weekly].[Year], [Time.Weekly].CurrentMember), [Measures].[Unit Sales])'\n" +
                "select {[Measures].[Sales to Date]} on columns,\n" +
                " {[Time.Weekly].[1997].[14] : [Time.Weekly].[1997].[16]} on rows\n" +
                "from [Sales]",
                fold(
                    "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Sales to Date]}\n" +
                    "Axis #2:\n" +
                    "{[Time].[Weekly].[All Weeklys].[1997].[14]}\n" +
                    "{[Time].[Weekly].[All Weeklys].[1997].[15]}\n" +
                    "{[Time].[Weekly].[All Weeklys].[1997].[16]}\n" +
                    "Row #0: 81,670\n" +
                    "Row #1: 86,300\n" +
                    "Row #2: 90,139\n"));
    }

    /**
     * Tests <a href="http://jira.pentaho.com/browse/MONDRIAN-191">
     * bug MONDRIAN-191, "Properties not working with multiple hierarchies"</a>.
     */
    public void testProperty() {
        TestContext testContext = TestContext.createSubstitutingCube("Sales",
            "<Dimension name=\"NuStore\" foreignKey=\"store_id\">\n" +
                "<Hierarchy hasAll=\"true\" primaryKey=\"store_id\">\n" +
                "  <Table name=\"store\"/>\n" +
                "  <Level name=\"NuStore Country\" column=\"store_country\" uniqueMembers=\"true\"/>\n" +
                "  <Level name=\"NuStore State\" column=\"store_state\" uniqueMembers=\"true\"/>\n" +
                "  <Level name=\"NuStore City\" column=\"store_city\" uniqueMembers=\"false\"/>\n" +
                "  <Level name=\"NuStore Name\" column=\"store_name\" uniqueMembers=\"true\">\n" +
                "    <Property name=\"NuStore Type\" column=\"store_type\"/>\n" +
                "    <Property name=\"NuStore Manager\" column=\"store_manager\"/>\n" +
                "    <Property name=\"NuStore Sqft\" column=\"store_sqft\" type=\"Numeric\"/>\n" +
                "    <Property name=\"Grocery Sqft\" column=\"grocery_sqft\" type=\"Numeric\"/>\n" +
                "    <Property name=\"Frozen Sqft\" column=\"frozen_sqft\" type=\"Numeric\"/>\n" +
                "    <Property name=\"Meat Sqft\" column=\"meat_sqft\" type=\"Numeric\"/>\n" +
                "    <Property name=\"Has coffee bar\" column=\"coffee_bar\" type=\"Boolean\"/>\n" +
                "    <Property name=\"Street address\" column=\"store_street_address\" type=\"String\"/>\n" +
                "  </Level>\n" +
                "</Hierarchy>\n" +
                "<Hierarchy caption=\"NuStore2\" name=\"NuStore2\" allMemberName=\"All NuStore2s\" hasAll=\"true\" primaryKey=\"NuStore_id\">\n" +
                "  <Table name=\"store\"/>\n" +
                "  <Level name=\"NuStore City\" column=\"store_city\" uniqueMembers=\"false\"/>\n" +
                "  <Level name=\"NuStore Name\" column=\"store_name\"  uniqueMembers=\"true\">\n" +
                "    <Property name=\"NuStore Type\" column=\"store_type\"/>\n" +
                "    <Property name=\"NuStore Manager\" column=\"store_manager\"/>\n" +
                "    <Property name=\"NuStore Sqft\" column=\"store_sqft\" type=\"Numeric\"/>\n" +
                "    <Property name=\"Grocery Sqft\" column=\"grocery_sqft\" type=\"Numeric\"/>\n" +
                "    <Property name=\"Frozen Sqft\" column=\"frozen_sqft\" type=\"Numeric\"/>\n" +
                "    <Property name=\"Meat Sqft\" column=\"meat_sqft\" type=\"Numeric\"/>\n" +
                "    <Property name=\"Has coffee bar\" column=\"coffee_bar\" type=\"Boolean\"/>\n" +
                "    <Property name=\"Street address\" column=\"store_street_address\" type=\"String\"/>\n" +
                "  </Level>\n" +
                "</Hierarchy>\n" +
                "</Dimension>");
        testContext.assertQueryReturns(
            "with member [Measures].[Store level] as '[NuStore].CurrentMember.Level.Name'\n" +
                "member [Measures].[Store type] as 'IIf(([NuStore].CurrentMember.Level.Name = \"NuStore Name\"), CAST([NuStore].CurrentMember.Properties(\"NuStore Type\") AS STRING), \"No type\")'\n" +
                "member [Measures].[Store Sqft] as 'IIf(([NuStore].CurrentMember.Level.Name = \"NuStore Name\"), CAST([NuStore].CurrentMember.Properties(\"NuStore Sqft\") AS INTEGER), 0.0)'\n" +
                "select {[Measures].[Unit Sales], [Measures].[Store Cost], [Measures].[Store Sales], [Measures].[Store level], [Measures].[Store type], [Measures].[Store Sqft]} ON COLUMNS,\n" +
                "{[NuStore].[All NuStores], [NuStore].[All NuStores].[Canada], [NuStore].[All NuStores].[Canada].[BC], [NuStore].[All NuStores].[Canada].[BC].[Vancouver], [NuStore].[All NuStores].[Canada].[BC].[Vancouver].[Store 19], [NuStore].[All NuStores].[Canada].[BC].[Victoria], [NuStore].[All NuStores].[Mexico], [NuStore].[All NuStores].[USA]} ON ROWS\n" +
                "from [Sales]\n" +
                "where [Time].[1997] ",
            fold("Axis #0:\n" +
                "{[Time].[1997]}\n" +
                "Axis #1:\n" +
                "{[Measures].[Unit Sales]}\n" +
                "{[Measures].[Store Cost]}\n" +
                "{[Measures].[Store Sales]}\n" +
                "{[Measures].[Store level]}\n" +
                "{[Measures].[Store type]}\n" +
                "{[Measures].[Store Sqft]}\n" +
                "Axis #2:\n" +
                "{[NuStore].[All NuStores]}\n" +
                "{[NuStore].[All NuStores].[Canada]}\n" +
                "{[NuStore].[All NuStores].[Canada].[BC]}\n" +
                "{[NuStore].[All NuStores].[Canada].[BC].[Vancouver]}\n" +
                "{[NuStore].[All NuStores].[Canada].[BC].[Vancouver].[Store 19]}\n" +
                "{[NuStore].[All NuStores].[Canada].[BC].[Victoria]}\n" +
                "{[NuStore].[All NuStores].[Mexico]}\n" +
                "{[NuStore].[All NuStores].[USA]}\n" +
                "Row #0: 266,773\n" +
                "Row #0: 225,627.23\n" +
                "Row #0: 565,238.13\n" +
                "Row #0: (All)\n" +
                "Row #0: No type\n" +
                "Row #0: 0\n" +
                "Row #1: \n" +
                "Row #1: \n" +
                "Row #1: \n" +
                "Row #1: NuStore Country\n" +
                "Row #1: No type\n" +
                "Row #1: 0\n" +
                "Row #2: \n" +
                "Row #2: \n" +
                "Row #2: \n" +
                "Row #2: NuStore State\n" +
                "Row #2: No type\n" +
                "Row #2: 0\n" +
                "Row #3: \n" +
                "Row #3: \n" +
                "Row #3: \n" +
                "Row #3: NuStore City\n" +
                "Row #3: No type\n" +
                "Row #3: 0\n" +
                "Row #4: \n" +
                "Row #4: \n" +
                "Row #4: \n" +
                "Row #4: NuStore Name\n" +
                "Row #4: Deluxe Supermarket\n" +
                "Row #4: 23,112\n" +
                "Row #5: \n" +
                "Row #5: \n" +
                "Row #5: \n" +
                "Row #5: NuStore City\n" +
                "Row #5: No type\n" +
                "Row #5: 0\n" +
                "Row #6: \n" +
                "Row #6: \n" +
                "Row #6: \n" +
                "Row #6: NuStore Country\n" +
                "Row #6: No type\n" +
                "Row #6: 0\n" +
                "Row #7: 266,773\n" +
                "Row #7: 225,627.23\n" +
                "Row #7: 565,238.13\n" +
                "Row #7: NuStore Country\n" +
                "Row #7: No type\n" +
                "Row #7: 0\n"));
    }
}

// End MultipleHierarchyTest.java
