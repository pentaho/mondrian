/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2003-2003 Julian Hyde <jhyde@users.sf.net>
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Mar 6, 2003
*/
package mondrian.test;

/**
 * <code>ParentChildHierarchyTest</code> tests parent-child hierarchies.
 *
 * @author jhyde
 * @since Mar 6, 2003
 * @version $Id$
 **/
public class ParentChildHierarchyTest extends FoodMartTestCase {
	public ParentChildHierarchyTest(String name) {
		super(name);
	}
	public void testAll() {
		runQueryCheckResult(
				"select {[Measures].[Org Salary], [Measures].[Count]} on columns," + nl +
				" {[Employees]} on rows" + nl +
				"from [HR]",
				"Axis #0:" + nl +
				"{}" + nl +
				"Axis #1:" + nl +
				"{[Measures].[Org Salary]}" + nl +
				"{[Measures].[Count]}" + nl +
				"Axis #2:" + nl +
				"{[Employees].[All Employees]}" + nl +
				"Row #0: $39,431.67" + nl +
				"Row #0: 7,392" + nl);
	}
	public void testChildrenOfAll() {
		runQueryCheckResult(
				"select {[Measures].[Org Salary], [Measures].[Count]} on columns," + nl +
				" {[Employees].children} on rows" + nl +
				"from [HR]",
				"Axis #0:" + nl +
				"{}" + nl +
				"Axis #1:" + nl +
				"{[Measures].[Org Salary]}" + nl +
				"{[Measures].[Count]}" + nl +
				"Axis #2:" + nl +
				"{[Employees].[All Employees].[Sheri Nowmer]}" + nl +
				"Row #0: $39,431.67" + nl +
				"Row #0: 7,392" + nl);
	}
	public void testLeaf() {
		// Juanita Sharp has no reports
		runQueryCheckResult(
				"select {[Measures].[Org Salary], [Measures].[Count]} on columns," + nl +
				" {[Employees].[All Employees].[Sheri Nowmer].[Rebecca Kanagaki].[Juanita Sharp]} on rows" + nl +
				"from [HR]",
				"Axis #0:" + nl +
				"{}" + nl +
				"Axis #1:" + nl +
				"{[Measures].[Org Salary]}" + nl +
				"{[Measures].[Count]}" + nl +
				"Axis #2:" + nl +
				"{[Employees].[All Employees].[Sheri Nowmer].[Rebecca Kanagaki].[Juanita Sharp]}" + nl +
				"Row #0: $72.36" + nl +
				"Row #0: 12" + nl);
	}
	public void testOneAboveLeaf() {
		// Rebecca Kanagaki has 2 direct reports, and they have no reports
		runQueryCheckResult(
				"select {[Measures].[Org Salary], [Measures].[Count]} on columns," + nl +
				" {[Employees].[All Employees].[Sheri Nowmer].[Rebecca Kanagaki]} on rows" + nl +
				"from [HR]",
				"Axis #0:" + nl +
				"{}" + nl +
				"Axis #1:" + nl +
				"{[Measures].[Org Salary]}" + nl +
				"{[Measures].[Count]}" + nl +
				"Axis #2:" + nl +
				"{[Employees].[All Employees].[Sheri Nowmer].[Rebecca Kanagaki]}" + nl +
				"Row #0: $234.36" + nl +
				"Row #0: 24" + nl);
	}
	/**
	 * Script That Uses the LEAVES Flag to Return the Bottom 10 Dimension
	 * Members, from <a href="http://www.winscriptingsolutions.com/Files/09/27139/Listing_01.txt">here</a>.
	 */
	public void _testFoo() {
		runQueryCheckResult(
				"WITH SET [NonEmptyEmployees] AS 'FILTER(DESCENDANTS([Employees].[All Employees], 10, LEAVES)," + nl +
				"  NOT ISEMPTY( [Measures].[Employee Salary]) )'" + nl +
				"SELECT { [Measures].[Employee Salary], [Measures].[Number of Employees] } ON COLUMNS," + nl +
				"  BOTTOMCOUNT([NonEmptyEmployees], 10, [Measures].[Employee Salary]) ON ROWS" + nl +
				"FROM HR" + nl +
				"WHERE ([Pay Type].[All Pay Type].[Hourly])",
				"");
	}
	/**
	 * Script from <a href="http://www.winscriptingsolutions.com/Files/09/27139/Listing_02.txt">here</a>.
	 */
	public void _testBar() {
		runQueryCheckResult(
				"with set [Leaves] as 'Descendants([Employees].[All Employees], 15, LEAVES )'" + nl +
				" set [Parents] as 'Generate( [Leaves], {[Employees].CurrentMember.Parent} )'" + nl +
				" set [FirstParents] as 'Filter( [Parents], " + nl +
				"Count( Descendants( [Employees].CurrentMember, 2 )) = 0 )'" + nl +
				"select {[Measures].[Number of Employees]} on Columns," + nl +
				"  TopCount( [FirstParents], 10, [Measures].[Number of Employees]) on Rows" + nl +
				"from HR",
				"");
	}
	// todo: test DimensionUsage which joins to a level which is not in the
	// same table as the lowest level.
}

// End ParentChildHierarchyTest.java
