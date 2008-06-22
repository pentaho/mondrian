/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2008 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
*/
package mondrian.test;

/**
 * <code>SolveOrderScopeIsolationTest</code> Test conformance to SSAS2005 solve
 * order scope isolation behavior.
 * Scope Isolation: In SQL Server 2005 Analysis Services, when a cube
 * Multidimensional Expressions (MDX) script contains calculated members,
 * by default the calculated members are resolved before any session-scoped
 * calculations are resolved and before any query-defined calculations are
 * resolved. This is different from SQL Server 2000 Analysis Services behavior,
 * where solve order can explicitly be used to insert a session-scoped or
 * query-defined calculation in between two cube-level calculations.
 * Further details at: http://msdn2.microsoft.com/en-us/library/ms144787.aspx
 *
 * This initial set of tests are added to indicate the kind of behavior that is
 * expected to support this SSAS 2005 feature. All tests start with an underscore
 * so as to not to execute even if the test class is added to Main
 *
 * @author ajogleka
 * @version $Id$
 * @since Apr 04, 2008
 */
public class SolveOrderScopeIsolationTest extends FoodMartTestCase {
    private String memberDefs =
        "<CalculatedMember\n" +
        "    name=\"maleMinusFemale\"\n" +
        "    dimension=\"gender\"\n" +
        "    visible=\"false\"\n" +
        "    formula=\"gender.m - gender.f\">\n" +
        "  <CalculatedMemberProperty name=\"FORMAT_STRING\" value=\"#.##\"/>\n" +
        "  <CalculatedMemberProperty name=\"SOLVE_ORDER\" value=\"3000\"/>\n" +
        "</CalculatedMember>" +
        "<CalculatedMember\n" +
        "    name=\"ratio\"\n" +
        "    dimension=\"measures\"\n" +
        "    visible=\"false\"\n" +
        "    formula=\"measures.[unit sales] / measures.[sales count]\">\n" +
        "  <CalculatedMemberProperty name=\"FORMAT_STRING\" value=\"#.##\"/>\n" +
        "  <CalculatedMemberProperty name=\"SOLVE_ORDER\" value=\"10\"/>\n" +
        "</CalculatedMember>" +
        "<CalculatedMember\n" +
        "    name=\"Total\"\n" +
        "    dimension=\"Time\"\n" +
        "    visible=\"false\"\n" +
        "    formula=\"AGGREGATE({[Time].[1997].[Q1],[Time].[1997].[Q2]})\">\n" +
        "  <CalculatedMemberProperty name=\"FORMAT_STRING\" value=\"#.##\"/>\n" +
        "  <CalculatedMemberProperty name=\"SOLVE_ORDER\" value=\"20\"/>\n" +
        "</CalculatedMember>";

    public TestContext getTestContext() {
        return TestContext.createSubstitutingCube("Sales", null, memberDefs);
    }

    public void _testOverrideOverCubeMemberDoesNotHappen() {

        assertQueryReturns("with\n" +
            "member gender.override as 'gender.maleMinusFemale', " +
            "SOLVE_ORDER=5, FORMAT_STRING='#.##'\n" +
            "select {measures.[ratio], measures.[unit sales], " +
            "measures.[sales count]} on 0,\n" +
            "{gender.override,gender.maleMinusFemale} on 1\n" +
            "from sales", fold("Axis #0:\n" +
            "{}\n" +
            "Axis #1:\n" +
            "{[Measures].[ratio]}\n" +
            "{[Measures].[Unit Sales]}\n" +
            "{[Measures].[Sales Count]}\n" +
            "Axis #2:\n" +
            "{[Gender].[override]}\n" +
            "{[Gender].[maleMinusFemale]}\n" +
            "Row #0: \n" +
            "Row #0: 3657\n" +
            "Row #0: 1175\n" +
            "Row #1: \n" +
            "Row #1: 3657\n" +
            "Row #1: 1175\n"));
    }

    public void _testOverrideOverCubeMemberHappensWithScopeIsolation() {

        assertQueryReturns("with\n" +
            "member gender.maleMinusFemale as 'gender.m - gender.f', " +
            "SOLVE_ORDER=3000, FORMAT_STRING='#.##'\n" +
            "member gender.override as 'gender.maleMinusFemale', " +
            "SOLVE_ORDER=5, FORMAT_STRING='#.##', SCOPE_ISOLATION=CUBE \n" +
            "member measures.[ratio] as " +
            "'measures.[unit sales] / measures.[sales count]', SOLVE_ORDER=10\n" +
            "select {measures.[ratio],\n" +
            "measures.[unit sales],\n" +
            "measures.[sales count]} on 0,\n" +
            "{gender.override, gender.maleMinusFemale} on 1\n" +
            "from sales", fold("Axis #0:\n" +
            "{}\n" +
            "Axis #1:\n" +
            "{[Measures].[ratio]}\n" +
            "{[Measures].[Unit Sales]}\n" +
            "{[Measures].[Sales Count]}\n" +
            "Axis #2:\n" +
            "{[Gender].[override]}\n" +
            "{[Gender].[maleMinusFemale]}\n" +
            "Row #0: 3.11" +
            "Row #0: 3657\n" +
            "Row #0: 1175\n" +
            "Row #1: \n" +
            "Row #1: 3657\n" +
            "Row #1: 1175\n"));
    }

    public void _testCubeMemberIsEvaluatedBeforeQueryMember() {
        assertQueryReturns("WITH MEMBER [Customers].USAByWA AS\n" +
            "'[Customers].[Country].[USA] / [Customers].[State Province].[WA]', " +
            "SOLVE_ORDER=5\n" +
            "SELECT {[Country].[USA],[State Province].[WA], [Customers].USAByWA} ON 0 " +
            "FROM SALES\n" +
            "WHERE Profit",
            fold(
                "Axis #0:\n" +
                    "{[Measures].[Profit]}\n" +
                    "Axis #1:\n" +
                    "{[Customers].[All Customers].[USA]}\n" +
                    "{[Customers].[All Customers].[USA].[WA]}\n" +
                    "{[Customers].[USAByWA]}\n" +
                    "Row #0: $339,610.90\n" +
                    "Row #0: $158,468.91\n" +
                    "Row #0: $2.14\n"));
    }

    public void _testOverrideOverCubeMemberInTupleDoesNotHappen() {
        assertQueryReturns("with\n" +
            "member gender.override as " +
            "'([Gender].[maleMinusFemale], [Product].[Food])', SOLVE_ORDER=5,\n" +
            "FORMAT_STRING='#.##'\n" +
            "select {measures.[ratio],\n" +
            "measures.[unit sales],\n" +
            "measures.[sales count]} on 0,\n" +
            "{gender.override, gender.maleMinusFemale} on 1\n" +
            "from sales", fold("Axis #0:\n" +
            "{}\n" +
            "Axis #1:\n" +
            "{[Measures].[ratio]}\n" +
            "{[Measures].[Unit Sales]}\n" +
            "{[Measures].[Sales Count]}\n" +
            "Axis #2:\n" +
            "{[Gender].[override]}\n" +
            "{[Gender].[maleMinusFemale]}\n" +
            "Row #0: \n" +
            "Row #0: 2312\n" +
            "Row #0: 749\n" +
            "Row #1: \n" +
            "Row #1: 3657\n" +
            "Row #1: 1175\n"));
    }

    public void _testConditionalCubeMemberGetsEvaluatedBeforeOtherMembers() {

        assertQueryReturns("with\n" +
            "member gender.override as 'iif(1=0," +
            "[gender].[all gender].[m], [Gender].[maleMinusFemale])', " +
            "SOLVE_ORDER=5, FORMAT_STRING='#.##'\n" +
            "select {measures.[ratio],\n" +
            "measures.[unit sales],\n" +
            "measures.[sales count]} on 0,\n" +
            "{[Gender].[override], gender.maleMinusFemale} on 1\n" +
            "from sales", fold("Axis #0:\n" +
            "{}\n" +
            "Axis #1:\n" +
            "{[Measures].[ratio]}\n" +
            "{[Measures].[Unit Sales]}\n" +
            "{[Measures].[Sales Count]}\n" +
            "Axis #2:\n" +
            "{[Gender].[override]}\n" +
            "{[Gender].[maleMinusFemale]}\n" +
            "Row #0: \n" +
            "Row #0: 3657\n" +
            "Row #0: 1175\n" +
            "Row #1: \n" +
            "Row #1: 3657\n" +
            "Row #1: 1175\n"));

    }

    public void _testOverrideOverCubeMemberUsingStrToMemberDoesNotHappen() {

        assertQueryReturns("with\n" +
            "member gender.override as 'iif(1=0,[gender].[all gender].[m], " +
            "StrToMember(\"[Gender].[maleMinusFemale]\"))', " +
            "SOLVE_ORDER=5, FORMAT_STRING='#.##'\n" +
            "select {measures.[ratio],\n" +
            "measures.[unit sales],\n" +
            "measures.[sales count]} on 0,\n" +
            "{[Gender].[override], gender.maleMinusFemale} on 1\n" +
            "from sales", fold("Axis #0:\n" +
            "{}\n" +
            "Axis #1:\n" +
            "{[Measures].[ratio]}\n" +
            "{[Measures].[Unit Sales]}\n" +
            "{[Measures].[Sales Count]}\n" +
            "Axis #2:\n" +
            "{[Gender].[override]}\n" +
            "{[Gender].[maleMinusFemale]}\n" +
            "Row #0: \n" +
            "Row #0: 3657\n" +
            "Row #0: 1175\n" +
            "Row #1: \n" +
            "Row #1: 3657\n" +
            "Row #1: 1175\n"));
    }

    public void _testAggregateMemberIsEvaluatedAfterOtherMembers() {
        assertQueryReturns("With\n" +
            "member Time.Total1 as " +
            "'AGGREGATE({[Time].[1997].[Q1],[Time].[1997].[Q2]})' , SOLVE_ORDER=20 \n" +
            ", FORMAT_STRING='#.##'\n" +
            "member measures.[ratio1] as " +
            "'measures.[unit sales] / measures.[sales count]', " +
            "SOLVE_ORDER=10 , FORMAT_STRING='#.##'\n" +
            "select {measures.[ratio1],\n" +
            "measures.[unit sales],\n" +
            "measures.[sales count]} on 0,\n" +
            "{Time.Total, Time.Total1, [Time].[1997].[Q1], [Time].[1997].[Q2]} on 1\n" +
            "from sales", fold(
            "Axis #0:\n" +
            "{}\n" +
            "Axis #1:\n" +
            "{[Measures].[ratio1]}\n" +
            "{[Measures].[Unit Sales]}\n" +
            "{[Measures].[Sales Count]}\n" +
            "Axis #2:\n" +
            "{[Time].[Total]}\n" +
            "{[Time].[Total1]}\n" +
            "{[Time].[1997].[Q1]}\n" +
            "{[Time].[1997].[Q2]}\n" +
            "Row #0: 3.07\n" +
            "Row #0: 128901\n" +
            "Row #0: 41956\n" +
            "Row #1: 3.07\n" +
            "Row #1: 128901\n" +
            "Row #1: 41956\n" +
            "Row #2: 3.07\n" +
            "Row #2: 66,291\n" +
            "Row #2: 21,588\n" +
            "Row #3: 3.07\n" +
            "Row #3: 62,610\n" +
            "Row #3: 20,368\n"));
    }

    public void _testConditionalAggregateMemberIsEvaluatedAfterOtherMembers() {
        assertQueryReturns("With\n" +
            "member Time.Total1 as 'IIF(Measures.CURRENTMEMBER IS Measures.Profit, 1, " +
            "AGGREGATE({[Time].[1997].[Q1],[Time].[1997].[Q2]}))' , SOLVE_ORDER=20 \n" +
            ", FORMAT_STRING='#.##'\n" +
            "member measures.[ratio1] as 'measures.[unit sales] / measures.[sales count]', " +
            "SOLVE_ORDER=10, FORMAT_STRING='#.##'\n" +
            "select {measures.[ratio1],\n" +
            "measures.[unit sales],\n" +
            "measures.[sales count]} on 0,\n" +
            "{Time.Total, Time.Total1, [Time].[1997].[Q1], [Time].[1997].[Q2]} on 1\n" +
            "from sales", fold(
            "Axis #0:\n" +
            "{}\n" +
            "Axis #1:\n" +
            "{[Measures].[ratio1]}\n" +
            "{[Measures].[Unit Sales]}\n" +
            "{[Measures].[Sales Count]}\n" +
            "Axis #2:\n" +
            "{[Time].[Total]}\n" +
            "{[Time].[Total1]}\n" +
            "{[Time].[1997].[Q1]}\n" +
            "{[Time].[1997].[Q2]}\n" +
            "Row #0: 3.07\n" +
            "Row #0: 128901\n" +
            "Row #0: 41956\n" +
            "Row #1: 3.07\n" +
            "Row #1: 128901\n" +
            "Row #1: 41956\n" +
            "Row #2: 3.07\n" +
            "Row #2: 66,291\n" +
            "Row #2: 21,588\n" +
            "Row #3: 3.07\n" +
            "Row #3: 62,610\n" +
            "Row #3: 20,368\n"));
    }

    public void _testStrToMemberReturningAggregateIsEvaluatedAfterOtherMembers() {
        assertQueryReturns("With\n" +
            "member Time.StrTotal as 'AGGREGATE({[Time].[1997].[Q1],[Time].[1997].[Q2]})', " +
            "SOLVE_ORDER=100, FORMAT_STRING='#.##'\n" +
            "member Time.Total as 'IIF(Measures.CURRENTMEMBER IS Measures.Profit, 1, \n" +
            "StrToMember(\"[Time].[StrTotal]\"))' , SOLVE_ORDER=20 \n" +
            ", FORMAT_STRING='#.##'\n" +
            "member measures.[ratio1] as 'measures.[unit sales] / measures.[sales count]', " +
            "SOLVE_ORDER=10, FORMAT_STRING='#.##'\n" +
            "select {measures.[ratio1],\n" +
            "measures.[unit sales],\n" +
            "measures.[sales count]} on 0,\n" +
            "{Time.Total, [Time].[1997].[Q1], [Time].[1997].[Q2]} on 1\n" +
            "from sales", fold("Axis #0:\n" +
            "{}\n" +
            "Axis #1:\n" +
            "{[Measures].[ratio1]}\n" +
            "{[Measures].[Unit Sales]}\n" +
            "{[Measures].[Sales Count]}\n" +
            "Axis #2:\n" +
            "{[Time].[Total]}\n" +
            "{[Time].[1997].[Q1]}\n" +
            "{[Time].[1997].[Q2]}\n" +
            "Row #0: 3.07\n" +
            "Row #0: 128901\n" +
            "Row #0: 41956\n" +
            "Row #1: 3.07\n" +
            "Row #1: 66,291\n" +
            "Row #1: 21,588\n" +
            "Row #2: 3.07\n" +
            "Row #2: 62,610\n" +
            "Row #2: 20,368\n"));
    }

    public void _test2LevelOfOverrideOverCubeMemberDoesNotHappen() {

        assertQueryReturns("With member gender.override1 as 'gender.maleMinusFemale',\n" +
            "SOLVE_ORDER=20, FORMAT_STRING='#.##'\n" +
            "member gender.override2 as 'gender.override1', SOLVE_ORDER=2,\n" +
            "FORMAT_STRING='#.##'\n" +
            "member measures.[ratio1] as 'measures.[unit sales] / measures.[sales count]', " +
            "SOLVE_ORDER=50, FORMAT_STRING='#.##'\n" +
            "select {measures.[ratio], measures.[ratio1],\n" +
            "measures.[unit sales],\n" +
            "measures.[sales count]} on 0,\n" +
            "{gender.override1, gender.override2, gender.maleMinusFemale} on 1\n" +
            "from sales", fold(
            "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[ratio]}\n" +
                "{[Measures].[ratio1]}\n" +
                "{[Measures].[Unit Sales]}\n" +
                "{[Measures].[Sales Count]}\n" +
                "Axis #2:\n" +
                "{[Gender].[override1]}\n" +
                "{[Gender].[override2]}\n" +
                "{[Gender].[maleMinusFemale]}\n" +
                "Row #0: \n" +
                "Row #0: 3.11\n" +
                "Row #0: 3657\n" +
                "Row #0: 1175\n" +
                "Row #1: \n" +
                "Row #1: 3.11\n" +
                "Row #1: 3657\n" +
                "Row #1: 1175\n" +
                "Row #2: \n" +
                "Row #2: 3.11\n" +
                "Row #2: 3657\n" +
                "Row #2: 1175\n"));
    }
}

// End SolveOrderScopeIsolationTest.java
