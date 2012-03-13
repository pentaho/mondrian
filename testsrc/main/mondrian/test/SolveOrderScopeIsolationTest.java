/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2008-2011 Pentaho
// All Rights Reserved.
*/
package mondrian.test;

import mondrian.olap.*;

import static mondrian.olap.SolveOrderMode.ABSOLUTE;
import static mondrian.olap.SolveOrderMode.SCOPED;

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
 * expected to support this SSAS 2005 feature. All tests start with an
 * underscore so as to not to execute even if the test class is added to Main
 *
 * @author ajogleka
 * @since Apr 04, 2008
 */
public class SolveOrderScopeIsolationTest extends FoodMartTestCase {
    SolveOrderMode defaultSolveOrderMode;

    public void setUp() throws Exception {
        super.setUp();
        defaultSolveOrderMode = getSolveOrderMode();
    }

    public void tearDown() throws Exception {
        setSolveOrderMode(defaultSolveOrderMode);
        super.tearDown();
    }

    private static final String memberDefs =
        "<CalculatedMember\n"
        + "    name='maleMinusFemale'\n"
        + "    hierarchy='[Customer].[Gender]'\n"
        + "    visible='false'\n"
        + "    formula='gender.m - gender.f'>\n"
        + "  <CalculatedMemberProperty name='SOLVE_ORDER' value='3000'/>\n"
        + "</CalculatedMember>"
        + "<CalculatedMember\n"
        + "    name='ProfitSolveOrder3000'\n"
        + "    dimension='Measures'>\n"
        + "  <Formula>[Measures].[Store Sales] - [Measures].[Store Cost]</Formula>\n"
        + "  <CalculatedMemberProperty name='SOLVE_ORDER' value='3000'/>\n"
        + "  <CalculatedMemberProperty name='FORMAT_STRING' value='$#,##0.000000'/>\n"
        + "</CalculatedMember>"
        + "<CalculatedMember\n"
        + "    name='ratio'\n"
        + "    dimension='measures'\n"
        + "    visible='false'\n"
        + "    formula='measures.[unit sales] / measures.[sales count]'>\n"
        + "  <CalculatedMemberProperty name='FORMAT_STRING' value='0.0#'/>\n"
        + "  <CalculatedMemberProperty name='SOLVE_ORDER' value='10'/>\n"
        + "</CalculatedMember>"
        + "<CalculatedMember\n"
        + "    name='Total'\n"
        + "    hierarchy='[Time].[Time]'\n"
        + "    visible='false'\n"
        + "    formula='AGGREGATE({[Time].[1997].[Q1],[Time].[1997].[Q2]})'>\n"
        + "  <CalculatedMemberProperty name='SOLVE_ORDER' value='20'/>\n"
        + "</CalculatedMember>";

    private SolveOrderMode getSolveOrderMode()
    {
        return Util.lookup(
            SolveOrderMode.class,
            MondrianProperties.instance().SolveOrderMode.get().toUpperCase());
    }

    final void setSolveOrderMode(SolveOrderMode mode) {
        MondrianProperties.instance().SolveOrderMode.set(mode.toString());
    }

    public TestContext getTestContext() {
        return TestContext.instance().createSubstitutingCube(
            "Sales", null, memberDefs);
    }

    public void testAllSolveOrderModesHandled()
    {
        for (SolveOrderMode mode : SolveOrderMode.values()) {
            switch (mode) {
            case ABSOLUTE:
            case SCOPED:
                break;
            default:
                fail(
                    "Tests for solve order mode " + mode.toString()
                    + " have not been implemented.");
            }
        }
    }

    public void testSetSolveOrderMode()
    {
        setSolveOrderMode(ABSOLUTE);
        assertEquals(ABSOLUTE, getSolveOrderMode());

        setSolveOrderMode(SCOPED);
        assertEquals(SCOPED, getSolveOrderMode());
    }

    public void testOverrideCubeMemberDoesNotHappenAbsolute() {
        final String mdx =
            "with\n"
            + "member gender.override as 'gender.maleMinusFemale', "
            + "SOLVE_ORDER=5\n"
            + "select {measures.[ratio], measures.[unit sales], "
            + "measures.[sales count]} on 0,\n"
            + "{gender.override,gender.maleMinusFemale} on 1\n"
            + "from sales";

        setSolveOrderMode(SolveOrderMode.ABSOLUTE);
        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[ratio]}\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Sales Count]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Gender].[override]}\n"
            + "{[Customer].[Gender].[maleMinusFemale]}\n"
            + "Row #0: 3.11\n"
            + "Row #0: 3,657\n"
            + "Row #0: 1,175\n"
            + "Row #1: 0.0\n"
            + "Row #1: 3,657\n"
            + "Row #1: 1,175\n");
    }

    public void testOverrideCubeMemberDoesNotHappenScoped() {
        final String mdx =
            "with\n"
            + "member gender.override as 'gender.maleMinusFemale', "
            + "SOLVE_ORDER=5\n"
            + "select {measures.[ratio], measures.[unit sales], "
            + "measures.[sales count]} on 0,\n"
            + "{gender.override,gender.maleMinusFemale} on 1\n"
            + "from sales";

        setSolveOrderMode(SolveOrderMode.SCOPED);
        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[ratio]}\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Sales Count]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Gender].[override]}\n"
            + "{[Customer].[Gender].[maleMinusFemale]}\n"
            + "Row #0: 0.0\n"
            + "Row #0: 3,657\n"
            + "Row #0: 1,175\n"
            + "Row #1: 0.0\n"
            + "Row #1: 3,657\n"
            + "Row #1: 1,175\n");
    }

    /**
     * Test for future capability: SCOPE_ISOLATION=CUBE which is implemented in
     * Analysis Services but not yet in Mondrian.
     */
    public void _future_testOverrideCubeMemberHappensWithScopeIsolation() {
        setSolveOrderMode(SCOPED);
        assertQueryReturns(
            "with\n"
            + "member gender.maleMinusFemale as 'Gender.M - gender.f', "
            + "SOLVE_ORDER=3000, FORMAT_STRING='#.##'\n"
            + "member gender.override as 'gender.maleMinusFemale', "
            + "SOLVE_ORDER=5, FORMAT_STRING='#.##', SCOPE_ISOLATION=CUBE \n"
            + "member measures.[ratio] as "
            + "'measures.[unit sales] / measures.[sales count]', SOLVE_ORDER=10\n"
            + "select {measures.[ratio],\n"
            + "measures.[unit sales],\n"
            + "measures.[sales count]} on 0,\n"
            + "{gender.override, gender.maleMinusFemale} on 1\n"
            + "from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[ratio]}\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Sales Count]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Gender].[override]}\n"
            + "{[Customer].[Gender].[maleMinusFemale]}\n"
            + "Row #0: 3.11"
            + "Row #0: 3657\n"
            + "Row #0: 1175\n"
            + "Row #1: \n"
            + "Row #1: 3657\n"
            + "Row #1: 1175\n");
    }

    public void testCubeMemberEvalBeforeQueryMemberAbsolute() {
        final String mdx =
            "WITH MEMBER [Customers].USAByWA AS\n"
            + "'[Customers].[Country].[USA] / [Customers].[State Province].[WA]', "
            + "SOLVE_ORDER=5\n"
            + "SELECT {[Country].[USA],[State Province].[WA], [Customers].USAByWA} ON 0, "
            + " {[Measures].[Store Sales], [Measures].[Store Cost], [Measures].[ProfitSolveOrder3000]} ON 1 "
            + "FROM SALES\n";
        setSolveOrderMode(ABSOLUTE);
        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Customer].[Customers].[USA]}\n"
            + "{[Customer].[Customers].[USA].[WA]}\n"
            + "{[Customer].[Customers].[USAByWA]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Store Sales]}\n"
            + "{[Measures].[Store Cost]}\n"
            + "{[Measures].[ProfitSolveOrder3000]}\n"
            + "Row #0: 565,238.13\n"
            + "Row #0: 263,793.22\n"
            + "Row #0: 2.14\n"
            + "Row #1: 225,627.23\n"
            + "Row #1: 105,324.31\n"
            + "Row #1: 2.14\n"
            + "Row #2: $339,610.896400\n"
            + "Row #2: $158,468.912100\n"
            + "Row #2: $0.000518\n");
    }

    public void testCubeMemberEvalBeforeQueryMemberScoped() {
        final String mdx =
            "WITH MEMBER [Customers].USAByWA AS\n"
            + "'[Customers].[Country].[USA] / [Customers].[State Province].[WA]', "
            + "SOLVE_ORDER=5\n"
            + "SELECT {[Country].[USA],[State Province].[WA], [Customers].USAByWA} ON 0 "
            + "FROM SALES\n"
            + "WHERE Measures.ProfitSolveOrder3000";
        setSolveOrderMode(SCOPED);
        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{[Measures].[ProfitSolveOrder3000]}\n"
            + "Axis #1:\n"
            + "{[Customer].[Customers].[USA]}\n"
            + "{[Customer].[Customers].[USA].[WA]}\n"
            + "{[Customer].[Customers].[USAByWA]}\n"
            + "Row #0: $339,610.896400\n"
            + "Row #0: $158,468.912100\n"
            + "Row #0: $2.143076\n");
    }

    public void testOverrideCubeMemberInTupleDoesNotHappenAbsolute() {
        final String mdx =
            "with\n"
            + "member gender.override as "
            + "'([Gender].[maleMinusFemale], [Product].[Food])', SOLVE_ORDER=5\n"
            + "select {measures.[ratio],\n"
            + "measures.[unit sales],\n"
            + "measures.[sales count]} on 0,\n"
            + "{gender.override, gender.maleMinusFemale} on 1\n"
            + "from sales";
        setSolveOrderMode(ABSOLUTE);
        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[ratio]}\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Sales Count]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Gender].[override]}\n"
            + "{[Customer].[Gender].[maleMinusFemale]}\n"
            + "Row #0: 3.09\n"
            + "Row #0: 2,312\n"
            + "Row #0: 749\n"
            + "Row #1: 0.0\n"
            + "Row #1: 3,657\n"
            + "Row #1: 1,175\n");
    }

    public void testOverrideCubeMemberInTupleDoesNotHappenScoped() {
        final String mdx =
            "with\n"
            + "member gender.override as "
            + "'([Gender].[maleMinusFemale], [Product].[Food])', SOLVE_ORDER=5\n"
            + "select {measures.[ratio],\n"
            + "measures.[unit sales],\n"
            + "measures.[sales count]} on 0,\n"
            + "{gender.override, gender.maleMinusFemale} on 1\n"
            + "from sales";
        setSolveOrderMode(SCOPED);
        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[ratio]}\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Sales Count]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Gender].[override]}\n"
            + "{[Customer].[Gender].[maleMinusFemale]}\n"
            + "Row #0: 0.0\n"
            + "Row #0: 2,312\n"
            + "Row #0: 749\n"
            + "Row #1: 0.0\n"
            + "Row #1: 3,657\n"
            + "Row #1: 1,175\n");
    }

    public void testConditionalCubeMemberEvalBeforeOtherMembersAbsolute() {
        final String mdx =
            "with\n"
            + "member gender.override as 'iif(1=0,"
            + "[gender].[all gender].[m], [Gender].[maleMinusFemale])', "
            + "SOLVE_ORDER=5\n"
            + "select {measures.[ratio],\n"
            + "measures.[unit sales],\n"
            + "measures.[sales count]} on 0,\n"
            + "{[Customer].[Gender].[override], gender.maleMinusFemale} on 1\n"
            + "from sales";
        setSolveOrderMode(ABSOLUTE);
        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[ratio]}\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Sales Count]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Gender].[override]}\n"
            + "{[Customer].[Gender].[maleMinusFemale]}\n"
            + "Row #0: 3.11\n"
            + "Row #0: 3,657\n"
            + "Row #0: 1,175\n"
            + "Row #1: 0.0\n"
            + "Row #1: 3,657\n"
            + "Row #1: 1,175\n");
    }

    public void testConditionalCubeMemberEvalBeforeOtherMembersScoped() {
        final String mdx =
            "with\n"
            + "member gender.override as 'iif(1=0,"
            + "[gender].[all gender].[m], [Gender].[maleMinusFemale])', "
            + "SOLVE_ORDER=5\n"
            + "select {measures.[ratio],\n"
            + "measures.[unit sales],\n"
            + "measures.[sales count]} on 0,\n"
            + "{[Customer].[Gender].[override], gender.maleMinusFemale} on 1\n"
            + "from sales";
        setSolveOrderMode(SCOPED);
        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[ratio]}\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Sales Count]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Gender].[override]}\n"
            + "{[Customer].[Gender].[maleMinusFemale]}\n"
            + "Row #0: 0.0\n"
            + "Row #0: 3,657\n"
            + "Row #0: 1,175\n"
            + "Row #1: 0.0\n"
            + "Row #1: 3,657\n"
            + "Row #1: 1,175\n");
    }

    public void testOverrideCubeMemberUsingStrToMemberDoesNotHappenAbsolute() {
        final String mdx =
            "with\n"
            + "member gender.override as 'iif(1=0,[gender].[all gender].[m], "
            + "StrToMember(\"[Gender].[maleMinusFemale]\"))', "
            + "SOLVE_ORDER=5\n"
            + "select {measures.[ratio],\n"
            + "measures.[unit sales],\n"
            + "measures.[sales count]} on 0,\n"
            + "{[Customer].[Gender].[override], gender.maleMinusFemale} on 1\n"
            + "from sales";
        setSolveOrderMode(ABSOLUTE);
        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[ratio]}\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Sales Count]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Gender].[override]}\n"
            + "{[Customer].[Gender].[maleMinusFemale]}\n"
            + "Row #0: 3.11\n"
            + "Row #0: 3,657\n"
            + "Row #0: 1,175\n"
            + "Row #1: 0.0\n"
            + "Row #1: 3,657\n"
            + "Row #1: 1,175\n");
    }

    public void testOverrideCubeMemberUsingStrToMemberDoesNotHappenScoped() {
        final String mdx =
            "with\n"
            + "member gender.override as 'iif(1=0,[gender].[all gender].[m], "
            + "StrToMember(\"[Gender].[maleMinusFemale]\"))', "
            + "SOLVE_ORDER=5\n"
            + "select {measures.[ratio],\n"
            + "measures.[unit sales],\n"
            + "measures.[sales count]} on 0,\n"
            + "{[Customer].[Gender].[override], gender.maleMinusFemale} on 1\n"
            + "from sales";
        setSolveOrderMode(SCOPED);
        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[ratio]}\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Sales Count]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Gender].[override]}\n"
            + "{[Customer].[Gender].[maleMinusFemale]}\n"
            + "Row #0: 0.0\n"
            + "Row #0: 3,657\n"
            + "Row #0: 1,175\n"
            + "Row #1: 0.0\n"
            + "Row #1: 3,657\n"
            + "Row #1: 1,175\n");
    }

    /**
     * This test validates that behavior is consistent with Analysis Services
     * 2000 when Solve order scope is ABSOLUTE.  AS2K will throw an error
     * whenever attempting to aggregate over calculated members (i.e. when the
     * solve order of the Aggregate member is higher than the calculations it
     * intersects with).
     */
    public void testAggregateMemberEvalAfterOtherMembersAbsolute() {
        final String mdx =
            "With\n"
            + "member Time.Time.Total1 as "
            + "'AGGREGATE({[Time].[1997].[Q1],[Time].[1997].[Q2]})' , SOLVE_ORDER=20 \n"
            + ", FORMAT_STRING='#,###'\n"
            + "member measures.[ratio1] as "
            + "'measures.[unit sales] / measures.[sales count]', "
            + "SOLVE_ORDER=10 , FORMAT_STRING='#.##'\n"
            + "select {measures.[ratio1],\n"
            + "measures.[unit sales],\n"
            + "measures.[sales count]} on 0,\n"
            + "{Time.Total, Time.Total1, [Time].[1997].[Q1], [Time].[1997].[Q2]} on 1\n"
            + "from sales";
        setSolveOrderMode(ABSOLUTE);
        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[ratio1]}\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Sales Count]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[Total]}\n"
            + "{[Time].[Time].[Total1]}\n"
            + "{[Time].[Time].[1997].[Q1]}\n"
            + "{[Time].[Time].[1997].[Q2]}\n"
            + "Row #0: #ERR: mondrian.olap.fun.MondrianEvaluationException: Could not find an aggregator in the current evaluation context\n"
            + "Row #0: 128,901\n"
            + "Row #0: 41,956\n"
            + "Row #1: #ERR: mondrian.olap.fun.MondrianEvaluationException: Could not find an aggregator in the current evaluation context\n"
            + "Row #1: 128,901\n"
            + "Row #1: 41,956\n"
            + "Row #2: 3.07\n"
            + "Row #2: 66,291\n"
            + "Row #2: 21,588\n"
            + "Row #3: 3.07\n"
            + "Row #3: 62,610\n"
            + "Row #3: 20,368\n");
    }

    public void testAggregateMemberEvalAfterOtherMembersScoped() {
        final String mdx =
            "With\n"
            + "member Time.Time.Total1 as "
            + "'AGGREGATE({[Time].[1997].[Q1],[Time].[1997].[Q2]})' , SOLVE_ORDER=20 \n"
            + ", FORMAT_STRING='#,###'\n"
            + "member measures.[ratio1] as "
            + "'measures.[unit sales] / measures.[sales count]', "
            + "SOLVE_ORDER=10 , FORMAT_STRING='#.##'\n"
            + "select {measures.[ratio1],\n"
            + "measures.[unit sales],\n"
            + "measures.[sales count]} on 0,\n"
            + "{Time.Total, Time.Total1, [Time].[1997].[Q1], [Time].[1997].[Q2]} on 1\n"
            + "from sales";
        setSolveOrderMode(SCOPED);
        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[ratio1]}\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Sales Count]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[Total]}\n"
            + "{[Time].[Time].[Total1]}\n"
            + "{[Time].[Time].[1997].[Q1]}\n"
            + "{[Time].[Time].[1997].[Q2]}\n"
            + "Row #0: 3.07\n"
            + "Row #0: 128,901\n"
            + "Row #0: 41,956\n"
            + "Row #1: 3\n"
            + "Row #1: 128,901\n"
            + "Row #1: 41,956\n"
            + "Row #2: 3.07\n"
            + "Row #2: 66,291\n"
            + "Row #2: 21,588\n"
            + "Row #3: 3.07\n"
            + "Row #3: 62,610\n"
            + "Row #3: 20,368\n");
    }

    /**
     * This test validates that behavior is consistent with Analysis Services
     * 2000 when Solve order scope is ABSOLUTE.  AS2K will throw an error
     * whenever attempting to aggregate over calculated members (i.e. when the
     * solve order of the Aggregate member is higher than the calculations it
     * intersects with).
     */
    public void testConditionalAggregateMemberEvalAfterOtherMembersAbsolute() {
        final String mdx =
            "With\n"
            + "member Time.Time.Total1 as 'IIF(Measures.CURRENTMEMBER IS Measures.Profit, 1, "
            + "AGGREGATE({[Time].[1997].[Q1],[Time].[1997].[Q2]}))' , SOLVE_ORDER=20 \n"
            + "\n"
            + "member measures.[ratio1] as 'measures.[unit sales] / measures.[sales count]', "
            + "SOLVE_ORDER=10\n"
            + "select {measures.[ratio1],\n"
            + "measures.[unit sales],\n"
            + "measures.[sales count]} on 0,\n"
            + "{Time.Total, Time.Total1, [Time].[1997].[Q1], [Time].[1997].[Q2]} on 1\n"
            + "from sales";
        setSolveOrderMode(ABSOLUTE);
        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[ratio1]}\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Sales Count]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[Total]}\n"
            + "{[Time].[Time].[Total1]}\n"
            + "{[Time].[Time].[1997].[Q1]}\n"
            + "{[Time].[Time].[1997].[Q2]}\n"
            + "Row #0: #ERR: mondrian.olap.fun.MondrianEvaluationException: Could not find an aggregator in the current evaluation context\n"
            + "Row #0: 128,901\n"
            + "Row #0: 41,956\n"
            + "Row #1: #ERR: mondrian.olap.fun.MondrianEvaluationException: Could not find an aggregator in the current evaluation context\n"
            + "Row #1: 128,901\n"
            + "Row #1: 41,956\n"
            + "Row #2: 3\n"
            + "Row #2: 66,291\n"
            + "Row #2: 21,588\n"
            + "Row #3: 3\n"
            + "Row #3: 62,610\n"
            + "Row #3: 20,368\n");
    }

    public void testConditionalAggregateMemberEvalAfterOtherMembersScoped() {
        final String mdx =
            "With\n"
            + "member Time.Time.Total1 as 'IIF(Measures.CURRENTMEMBER IS Measures.Profit, 1, "
            + "AGGREGATE({[Time].[1997].[Q1],[Time].[1997].[Q2]}))' , SOLVE_ORDER=20 \n"
            + "\n"
            + "member measures.[ratio1] as 'measures.[unit sales] / measures.[sales count]', "
            + "SOLVE_ORDER=10, FORMAT_STRING='#.##'\n"
            + "select {measures.[ratio1],\n"
            + "measures.[unit sales],\n"
            + "measures.[sales count]} on 0,\n"
            + "{Time.Total, Time.Total1, [Time].[1997].[Q1], [Time].[1997].[Q2]} on 1\n"
            + "from sales";
        setSolveOrderMode(SCOPED);
        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[ratio1]}\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Sales Count]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[Total]}\n"
            + "{[Time].[Time].[Total1]}\n"
            + "{[Time].[Time].[1997].[Q1]}\n"
            + "{[Time].[Time].[1997].[Q2]}\n"
            + "Row #0: 3.07\n"
            + "Row #0: 128,901\n"
            + "Row #0: 41,956\n"
            + "Row #1: 3.07\n"
            + "Row #1: 128,901\n"
            + "Row #1: 41,956\n"
            + "Row #2: 3.07\n"
            + "Row #2: 66,291\n"
            + "Row #2: 21,588\n"
            + "Row #3: 3.07\n"
            + "Row #3: 62,610\n"
            + "Row #3: 20,368\n");
    }

    /**
     * This test validates that behavior is consistent with Analysis Services
     * 2000 when Solve order scope is ABSOLUTE.  AS2K will throw an error
     * whenever attempting to aggregate over calculated members (i.e. when the
     * solve order of the Aggregate member is higher than the calculations it
     * intersects with).
     */
    public void testStrToMemberReturningAggEvalAfterOtherMembersAbsolute() {
        final String mdx =
            "With\n"
            + "member Time.Time.StrTotal as 'AGGREGATE({[Time].[1997].[Q1],[Time].[1997].[Q2]})', "
            + "SOLVE_ORDER=100\n"
            + "member Time.Time.Total as 'IIF(Measures.CURRENTMEMBER IS Measures.Profit, 1, \n"
            + "StrToMember(\"[Time].[StrTotal]\"))' , SOLVE_ORDER=20 \n"
            + "member measures.[ratio1] as 'measures.[unit sales] / measures.[sales count]', "
            + "SOLVE_ORDER=10, FORMAT_STRING='#.##'\n"
            + "select {measures.[ratio1],\n"
            + "measures.[unit sales],\n"
            + "measures.[sales count]} on 0,\n"
            + "{Time.Total, [Time].[1997].[Q1], [Time].[1997].[Q2]} on 1\n"
            + "from sales";
        setSolveOrderMode(ABSOLUTE);
        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[ratio1]}\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Sales Count]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[Total]}\n"
            + "{[Time].[Time].[1997].[Q1]}\n"
            + "{[Time].[Time].[1997].[Q2]}\n"
            + "Row #0: #ERR: mondrian.olap.fun.MondrianEvaluationException: Could not find an aggregator in the current evaluation context\n"
            + "Row #0: 128,901\n"
            + "Row #0: 41,956\n"
            + "Row #1: 3.07\n"
            + "Row #1: 66,291\n"
            + "Row #1: 21,588\n"
            + "Row #2: 3.07\n"
            + "Row #2: 62,610\n"
            + "Row #2: 20,368\n");
    }

    public void testStrToMemberReturningAggEvalAfterOtherMembersScoped() {
        final String mdx =
            "With\n"
            + "member Time.Time.StrTotal as 'AGGREGATE({[Time].[1997].[Q1],[Time].[1997].[Q2]})', "
            + "SOLVE_ORDER=100\n"
            + "member Time.Time.Total as 'IIF(Measures.CURRENTMEMBER IS Measures.Profit, 1, \n"
            + "StrToMember(\"[Time].[StrTotal]\"))' , SOLVE_ORDER=20 \n"
            + "member measures.[ratio1] as 'measures.[unit sales] / measures.[sales count]', "
            + "SOLVE_ORDER=10, FORMAT_STRING='#.##'\n"
            + "select {measures.[ratio1],\n"
            + "measures.[unit sales],\n"
            + "measures.[sales count]} on 0,\n"
            + "{Time.Total, [Time].[1997].[Q1], [Time].[1997].[Q2]} on 1\n"
            + "from sales";
        setSolveOrderMode(SCOPED);
        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[ratio1]}\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Sales Count]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[Total]}\n"
            + "{[Time].[Time].[1997].[Q1]}\n"
            + "{[Time].[Time].[1997].[Q2]}\n"
            + "Row #0: 3.07\n"
            + "Row #0: 128,901\n"
            + "Row #0: 41,956\n"
            + "Row #1: 3.07\n"
            + "Row #1: 66,291\n"
            + "Row #1: 21,588\n"
            + "Row #2: 3.07\n"
            + "Row #2: 62,610\n"
            + "Row #2: 20,368\n");
    }

    public void test2LevelOfOverrideCubeMemberDoesNotHappenAbsolute() {
        final String mdx =
            "With member gender.override1 as 'gender.maleMinusFemale',\n"
            + "SOLVE_ORDER=20\n"
            + "member gender.override2 as 'gender.override1', SOLVE_ORDER=2\n"
            + "member measures.[ratio1] as 'measures.[unit sales] / measures.[sales count]', "
            + "SOLVE_ORDER=50, FORMAT_STRING='0.0#'\n"
            + "select {measures.[ratio], measures.[ratio1],\n"
            + "measures.[unit sales],\n"
            + "measures.[sales count]} on 0,\n"
            + "{gender.override1, gender.override2, gender.maleMinusFemale} on 1\n"
            + "from sales";
        setSolveOrderMode(ABSOLUTE);
        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[ratio]}\n"
            + "{[Measures].[ratio1]}\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Sales Count]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Gender].[override1]}\n"
            + "{[Customer].[Gender].[override2]}\n"
            + "{[Customer].[Gender].[maleMinusFemale]}\n"
            + "Row #0: 0.0\n"
            + "Row #0: 3.11\n"
            + "Row #0: 3,657\n"
            + "Row #0: 1,175\n"
            + "Row #1: 3.11\n"
            + "Row #1: 3.11\n"
            + "Row #1: 3,657\n"
            + "Row #1: 1,175\n"
            + "Row #2: 0.0\n"
            + "Row #2: 0.0\n"
            + "Row #2: 3,657\n"
            + "Row #2: 1,175\n");
    }

    public void test2LevelOfOverrideCubeMemberDoesNotHappenScoped() {
        final String mdx =
            "With member gender.override1 as 'gender.maleMinusFemale',\n"
            + "SOLVE_ORDER=20\n"
            + "member gender.override2 as 'gender.override1', SOLVE_ORDER=2\n"
            + "member measures.[ratio1] as 'measures.[unit sales] / measures.[sales count]', "
            + "SOLVE_ORDER=50, FORMAT_STRING='0.0#'\n"
            + "select {measures.[ratio], measures.[ratio1],\n"
            + "measures.[unit sales],\n"
            + "measures.[sales count]} on 0,\n"
            + "{gender.override1, gender.override2, gender.maleMinusFemale} on 1\n"
            + "from sales";
        setSolveOrderMode(SCOPED);
        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[ratio]}\n"
            + "{[Measures].[ratio1]}\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Sales Count]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Gender].[override1]}\n"
            + "{[Customer].[Gender].[override2]}\n"
            + "{[Customer].[Gender].[maleMinusFemale]}\n"
            + "Row #0: 0.0\n"
            + "Row #0: 3.11\n"
            + "Row #0: 3,657\n"
            + "Row #0: 1,175\n"
            + "Row #1: 0.0\n"
            + "Row #1: 3.11\n"
            + "Row #1: 3,657\n"
            + "Row #1: 1,175\n"
            + "Row #2: 0.0\n"
            + "Row #2: 3.11\n"
            + "Row #2: 3,657\n"
            + "Row #2: 1,175\n");
    }
}

// End SolveOrderScopeIsolationTest.java
