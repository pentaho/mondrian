/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2003-2003 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Mar 6, 2003
*/
package mondrian.test;

import mondrian.olap.Result;
import junit.framework.Assert;

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

    /**
     * The recursion cyclicity check kicks in when the recursion depth reachs
     * the number of dimensions in the cube. So create a cube with fewer
     * dimensions (3) than the depth of the emp dimension (6).
     */
    public void testHierarchyFalseCycle() {
        // On the regular HR cube, this has always worked.
        runQueryCheckResult(
            "SELECT {[Employees].[All Employees].Children} on columns," + nl +
            " {[Measures].[Org Salary]} on rows" + nl +
            "FROM [HR]",

            "Axis #0:" + nl +
            "{}" + nl +
            "Axis #1:" + nl +
            "{[Employees].[All Employees].[Sheri Nowmer]}" + nl +
            "Axis #2:" + nl +
            "{[Measures].[Org Salary]}" + nl +
            "Row #0: $39,431.67" + nl);

        getConnection().getSchema().createCube(
            "<Cube name='HR-fewer-dims'>" + nl +
            "    <Table name='salary'/>" + nl +
            "    <Dimension name='Department' foreignKey='department_id'>" + nl +
            "        <Hierarchy hasAll='true' primaryKey='department_id'>" + nl +
            "            <Table name='department'/>" + nl +
            "            <Level name='Department Description' uniqueMembers='true' column='department_id'/>" + nl +
            "        </Hierarchy>" + nl +
            "    </Dimension>" + nl +
            "    <Dimension name='Employees' foreignKey='employee_id'>" + nl +
            "        <Hierarchy hasAll='true' allMemberName='All Employees' primaryKey='employee_id'>" + nl +
            "            <Table name='employee'/>" + nl +
            "            <Level name='Employee Id' type='Numeric' uniqueMembers='true' column='employee_id' parentColumn='supervisor_id' nameColumn='full_name' nullParentValue='0'>" + nl +
            "                <Property name='Marital Status' column='marital_status'/>" + nl +
            "                <Property name='Position Title' column='position_title'/>" + nl +
            "                <Property name='Gender' column='gender'/>" + nl +
            "                <Property name='Salary' column='salary'/>" + nl +
            "                <Property name='Education Level' column='education_level'/>" + nl +
            "                <Property name='Management Role' column='management_role'/>" + nl +
            "            </Level>" + nl +
            "        </Hierarchy>" + nl +
            "    </Dimension>" + nl +
            "    <Measure name='Org Salary' column='salary_paid' aggregator='sum' formatString='Currency' />" + nl +
            "    <Measure name='Count' column='employee_id' aggregator='count' formatString='#,#'/>" + nl +
            "</Cube>");
        // On a cube with fewer dimensions, this gave a false failure.
        runQueryCheckResult(
            "SELECT {[Employees].[All Employees].Children} on columns," + nl +
            " {[Measures].[Org Salary]} on rows" + nl +
            "FROM [HR-fewer-dims]",
            "Axis #0:" + nl +
            "{}" + nl +
            "Axis #1:" + nl +
            "{[Employees].[All Employees].[Sheri Nowmer]}" + nl +
            "Axis #2:" + nl +
            "{[Measures].[Org Salary]}" + nl +
            "Row #0: $271,552.44" + nl);
    }

    public void testGenuineCycle() {
        Result result = runQuery("with member [Measures].[Foo] as " + nl +
            "  '([Measures].[Foo], OpeningPeriod([Time].[Month]))'" + nl +
            "select" + nl +
            " {[Measures].[Unit Sales], [Measures].[Foo]} on Columns," + nl +
            " { [Time].[1997].[Q2]} on rows" + nl +
            "from [Sales]");
        String resultString = toString(result);
        final String[] expected = {
            "Axis #0:" + nl +
            "{}" + nl +
            "Axis #1:" + nl +
            "{[Measures].[Unit Sales]}" + nl +
            "{[Measures].[Foo]}" + nl +
            "Axis #2:" + nl +
            "{[Time].[1997].[Q2]}" + nl +
            "Row #0: 62,610" + nl +
            "Row #0: #ERR: mondrian.olap.fun.MondrianEvaluationException: Infinite loop while evaluating calculated member '[Measures].[Foo]'; context stack is {([Time].[1997].[Q2].[4]), ([Time].[1997].[Q2].[4]), ([Time].[1997].[Q2].[4]), ([Time].[1997].[Q2].[4]), ([Time].[1997].[Q2].[4]), ([Time].[1997].[Q2].[4]), ([Time].[1997].[Q2].[4]), ([Time].[1997].[Q2].[4]), ([Time].[1997].[Q2].[4]), ([Time].[1997].[Q2].[4]), ([Time].[1997].[Q2].[4]), ([Time].[1997].[Q2])}" + nl,

            "Axis #0:" + nl +
            "{}" + nl +
            "Axis #1:" + nl +
            "{[Measures].[Unit Sales]}" + nl +
            "{[Measures].[Foo]}" + nl +
            "Axis #2:" + nl +
            "{[Time].[1997].[Q2]}" + nl +
            "Row #0: (null)" + nl +
            "Row #0: #ERR: mondrian.olap.fun.MondrianEvaluationException: Infinite loop while evaluating calculated member '[Measures].[Foo]'; context stack is {([Store].[All Stores].[Mexico], [Time].[1997].[Q2].[4]), ([Store].[All Stores].[Mexico], [Time].[1997].[Q2].[4]), ([Store].[All Stores].[Mexico], [Time].[1997].[Q2].[4]), ([Store].[All Stores].[Mexico], [Time].[1997].[Q2].[4]), ([Store].[All Stores].[Mexico], [Time].[1997].[Q2].[4]), ([Store].[All Stores].[Mexico], [Time].[1997].[Q2].[4]), ([Store].[All Stores].[Mexico], [Time].[1997].[Q2].[4]), ([Store].[All Stores].[Mexico], [Time].[1997].[Q2].[4]), ([Store].[All Stores].[Mexico], [Time].[1997].[Q2].[4]), ([Store].[All Stores].[Mexico], [Time].[1997].[Q2].[4]), ([Store].[All Stores].[Mexico], [Time].[1997].[Q2].[4]), ([Store].[All Stores].[Mexico], [Time].[1997].[Q2].[4]), ([Store].[All Stores].[Mexico], [Time].[1997].[Q2].[4]), ([Store].[All Stores].[Mexico], [Time].[1997].[Q2])}" + nl,

            "Axis #0:" + nl +
            "{}" + nl +
            "Axis #1:" + nl +
            "{[Measures].[Unit Sales]}" + nl +
            "{[Measures].[Foo]}" + nl +
            "Axis #2:" + nl +
            "{[Time].[1997].[Q2]}" + nl +
            "Row #0: (null)" + nl +
            "Row #0: #ERR: mondrian.olap.fun.MondrianEvaluationException: Infinite loop while evaluating calculated member '[Measures].[Foo]'; context stack is {([Store].[All Stores].[Mexico], [Time].[1997].[Q2].[4]), ([Store].[All Stores].[Mexico], [Time].[1997].[Q2].[4]), ([Store].[All Stores].[Mexico], [Time].[1997].[Q2].[4]), ([Store].[All Stores].[Mexico], [Time].[1997].[Q2].[4]), ([Store].[All Stores].[Mexico], [Time].[1997].[Q2].[4]), ([Store].[All Stores].[Mexico], [Time].[1997].[Q2].[4]), ([Store].[All Stores].[Mexico], [Time].[1997].[Q2].[4]), ([Store].[All Stores].[Mexico], [Time].[1997].[Q2].[4]), ([Store].[All Stores].[Mexico], [Time].[1997].[Q2].[4]), ([Store].[All Stores].[Mexico], [Time].[1997].[Q2].[4]), ([Store].[All Stores].[Mexico], [Time].[1997].[Q2].[4]), ([Store].[All Stores].[Mexico], [Time].[1997].[Q2])}" + nl,

            "Axis #0:" + nl +
            "{}" + nl +
            "Axis #1:" + nl +
            "{[Measures].[Unit Sales]}" + nl +
            "{[Measures].[Foo]}" + nl +
            "Axis #2:" + nl +
            "{[Time].[1997].[Q2]}" + nl +
            "Row #0: 62,610" + nl +
            "Row #0: #ERR: mondrian.olap.fun.MondrianEvaluationException: Infinite loop while evaluating calculated member '[Measures].[Foo]'; context stack is {([Time].[1997].[Q2].[4]), ([Time].[1997].[Q2].[4]), ([Time].[1997].[Q2].[4]), ([Time].[1997].[Q2].[4]), ([Time].[1997].[Q2].[4]), ([Time].[1997].[Q2].[4]), ([Time].[1997].[Q2].[4]), ([Time].[1997].[Q2].[4]), ([Time].[1997].[Q2].[4]), ([Time].[1997].[Q2].[4]), ([Time].[1997].[Q2].[4]), ([Time].[1997].[Q2].[4]), ([Time].[1997].[Q2].[4]), ([Time].[1997].[Q2])}"
        };
        if (!resultString.equals(expected[0]) &&
            !resultString.equals(expected[1]) &&
            !resultString.equals(expected[2]) &&
            !resultString.equals(expected[3])) {
            System.out.println(resultString);
            Assert.assertEquals(expected, resultString);
        }
    }
}

// End ParentChildHierarchyTest.java
