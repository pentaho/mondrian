/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2004 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 5 October, 2002
*/
package mondrian.test;

import junit.framework.Assert;

/**
 * Tests the expressions used for calculated members. Please keep in sync
 * with the actual code used by the wizard.
 *
 * @author jhyde
 * @since 5 October, 2002
 * @version $Id$
 */
public class TestCalculatedMembers extends FoodMartTestCase {
	public TestCalculatedMembers(String name) {
		super(name);
	}

    public void testCalculatedMemberInCube() {
        String s = executeExpr("[Measures].[Profit]");
        Assert.assertEquals("339,610.90", s);
    }

    public void testCalculatedMemberInCubeAndQuery() {
        runQueryCheckResult("WITH MEMBER [Measures].[Profit Growth]" + nl +
            " AS '[Measures].[Profit] - ([Measures].[Profit], [Time].PrevMember)'" + nl +
            "SELECT {[Measures].[Profit], [Measures].[Profit Growth]} ON COLUMNS," + nl +
            " {[Time].[1997].[Q2].children} ON ROWS" + nl +
            "FROM [Sales]",
            "Axis #0:" + nl +
            "{}" + nl +
            "Axis #1:" + nl +
            "{[Measures].[Profit]}" + nl +
            "{[Measures].[Profit Growth]}" + nl +
            "Axis #2:" + nl +
            "{[Time].[1997].[Q2].[4]}" + nl +
            "{[Time].[1997].[Q2].[5]}" + nl +
            "{[Time].[1997].[Q2].[6]}" + nl +
            "Row #0: 25,766.55" + nl +
            "Row #0: -4,289.24" + nl +
            "Row #1: 26,673.73" + nl +
            "Row #1: 907.18" + nl +
            "Row #2: 27,261.76" + nl +
            "Row #2: 588.03" + nl);
    }

	public void _testWhole() {
		execute(
				"with" + nl +
				"member [Measures].[Total Store Sales by Product Name] as" + nl +
				"  'Sum([Product].[Product Name].members, [Measures].[Store Sales])'" + nl +
				"" + nl +
				"member [Measures].[Average Store Sales by Product Name] as" + nl +
				"  'Avg([Product].[Product Name].members, [Measures].[Store Sales])'" + nl +
				"" + nl +
				"member [Measures].[Number of Product Name members] as" + nl +
				"  'Count([Product].[Product Name].members)'" + nl +
				"" + nl +
				"member [Measures].[Standard Deviation of Store Sales for Product Name] as" + nl +
				"  'Stddev([Product].[Product Name].members, [Measures].[Store Sales])'" + nl +
				"" + nl +
				"member [Measures].[Variance between Store Sales and Store Cost] as" + nl +
				"  '[Measures].[Store Sales] - [Measures].[Store Cost]'" + nl +
				"" + nl +
				"member [Measures].[% Variance between Store Sales and Store Cost] as" + nl +
				"  'iif([Measures].[Store Cost] = 0, 1, [Measures].[Store Sales] / [Measures].[Store Cost])'" + nl +
				", format_string='Percent'" + nl +
				"" + nl +
				"member [Measures].[% Difference between Store Sales and Store Cost] as" + nl +
				"  'iif([Measures].[Store Sales] = 0, -1, ([Measures].[Store Sales] - [Measures].[Store Cost]) / [Measures].[Store Sales])'" + nl +
				", format_string='Percent'" + nl +
				"" + nl +
				"member [Measures].[% Markup between Store Sales and Store Cost] as" + nl +
				"  'iif([Measures].[Store Cost] = 0, 1, ([Measures].[Store Sales] - [Measures].[Store Cost]) / [Measures].[Store Cost])'" + nl +
				", format_string='Percent'" + nl +
				"" + nl +
				"member [Measures].[Growth of Store Sales since previous period] as" + nl +
				"  '[Measures].[Store Sales] - ([Measures].[Store Sales], ParallelPeriod([Time].CurrentMember.level, 1))'" + nl +
				"" + nl +
				"member [Measures].[% Growth of Store Sales since previous period] as" + nl +
				"  'iif(([Measures].[Store Sales], ParallelPeriod([Time].CurrentMember.level, 1)) = 0, 1, ([Measures].[Store Sales] - ([Measures].[Store Sales], ParallelPeriod([Time].CurrentMember.level, 1))) / ([Measures].[Store Sales], ParallelPeriod([Time].CurrentMember.level, 1)))'" + nl +
				", format_string='Percent'" + nl +
				"" + nl +
				"member [Measures].[Growth of Store Sales since previous year] as" + nl +
				"  '[Measures].[Store Sales] - ([Measures].[Store Sales], ParallelPeriod([Time].[Year], 1))'" + nl +
				"" + nl +
				"member [Measures].[% Growth of Store Sales since previous year] as" + nl +
				"  'iif(([Measures].[Store Sales], ParallelPeriod([Time].[Year], 1)) = 0, 1, ([Measures].[Store Sales] - ([Measures].[Store Sales], ParallelPeriod([Time].[Year], 1))) / ([Measures].[Store Sales], ParallelPeriod([Time].[Year], 1)))'" + nl +
				", format_string='Percent'" + nl +
				"" + nl +
				"member [Measures].[Store Sales as % of parent Store] as" + nl +
				"  'iif(([Measures].[Store Sales], [Store].CurrentMember.Parent) = 0, 1, [Measures].[Store Sales] / ([Measures].[Store Sales], [Store].CurrentMember.Parent))'" + nl +
				", format_string='Percent'" + nl +
				"" + nl +
				"member [Measures].[Store Sales as % of all Store] as" + nl +
				"  'iif(([Measures].[Store Sales], [Store].Members.Item(0)) = 0, 1, [Measures].[Store Sales] / ([Measures].[Store Sales], [Store].Members.Item(0)))'" + nl +
				", format_string='Percent'" + nl +
				"" + nl +
				"member [Measures].[Total Store Sales, period to date] as" + nl +
				" 'sum(PeriodsToDate([Time].CurrentMember.Parent.Level), [Measures].[Store Sales])'" + nl +
				"" + nl +
				"member [Measures].[Total Store Sales, Quarter to date] as" + nl +
				" 'sum(PeriodsToDate([Time].[Quarter]), [Measures].[Store Sales])'" + nl +
				"" + nl +
				"member [Measures].[Average Store Sales, period to date] as" + nl +
				" 'avg(PeriodsToDate([Time].CurrentMember.Parent.Level), [Measures].[Store Sales])'" + nl +
				"" + nl +
				"member [Measures].[Average Store Sales, Quarter to date] as" + nl +
				" 'avg(PeriodsToDate([Time].[Quarter]), [Measures].[Store Sales])'" + nl +
				"" + nl +
				"member [Measures].[Rolling Total of Store Sales over previous 3 periods] as" + nl +
				" 'sum([Time].CurrentMember.Lag(2) : [Time].CurrentMember, [Measures].[Store Sales])'" + nl +
				"" + nl +
				"member [Measures].[Rolling Average of Store Sales over previous 3 periods] as" + nl +
				" 'avg([Time].CurrentMember.Lag(2) : [Time].CurrentMember, [Measures].[Store Sales])'" + nl +
				"" + nl +
				"select" + nl +
				" CrossJoin(" + nl +
				"  {[Time].[1997], [Time].[1997].[Q2]}," + nl +
				"  {[Store].[All Stores], " + nl +
				"	[Store].[USA]," + nl +
				"	[Store].[USA].[CA]," + nl +
				"	[Store].[USA].[CA].[San Francisco]}) on columns," + nl +
				" AddCalculatedMembers([Measures].members) on rows" + nl +
				" from Sales");

		// Repeat time-related measures with more time members.
		execute(
				"with" + nl +
				"member [Measures].[Total Store Sales, Quarter to date] as" + nl +
				" 'sum(PeriodsToDate([Time].[Quarter]), [Measures].[Store Sales])'" + nl +
				"" + nl +
				"member [Measures].[Average Store Sales, period to date] as" + nl +
				" 'avg(PeriodsToDate([Time].CurrentMember.Parent.Level), [Measures].[Store Sales])'" + nl +
				"" + nl +
				"member [Measures].[Average Store Sales, Quarter to date] as" + nl +
				" 'avg(PeriodsToDate([Time].[Quarter]), [Measures].[Store Sales])'" + nl +
				"" + nl +
				"member [Measures].[Rolling Total of Store Sales over previous 3 periods] as" + nl +
				" 'sum([Time].CurrentMember.Lag(2) : [Time].CurrentMember, [Measures].[Store Sales])'" + nl +
				"" + nl +
				"member [Measures].[Rolling Average of Store Sales over previous 3 periods] as" + nl +
				" 'avg([Time].CurrentMember.Lag(2) : [Time].CurrentMember, [Measures].[Store Sales])'" + nl +
				"" + nl +
				"select" + nl +
				" CrossJoin(" + nl +
				"  {[Store].[USA].[CA]," + nl +
				"	[Store].[USA].[CA].[San Francisco]}," + nl +
				"  [Time].[Month].members) on columns," + nl +
				" AddCalculatedMembers([Measures].members) on rows" + nl +
				" from Sales");
	}
}

// End CalculatedMembersTestCase.java
