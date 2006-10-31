/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2003-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Mar 6, 2003
*/
package mondrian.test;

import junit.framework.Assert;
import mondrian.olap.Result;
import mondrian.olap.Member;
import mondrian.olap.Cell;
import mondrian.rolap.RolapConnection;

/**
 * <code>ParentChildHierarchyTest</code> tests parent-child hierarchies.
 *
 * @author jhyde
 * @since Mar 6, 2003
 * @version $Id$
 */
public class ParentChildHierarchyTest extends FoodMartTestCase {
    public ParentChildHierarchyTest(String name) {
        super(name);
    }

    // -- Helper methods -------------------------------------------------------
    /**
     * Returns a TestContext in which the "HR" cube contains an extra dimension,
     * "EmployeesClosure", which is an explicit closure of [Employees].
     *
     * <p>[Employees] is a parent/child hierarchy (along the relationship
     * supervisor_id/employee_id). The table employee_closure expresses the
     * closure of the parent/child relation, ie it represents
     * ancestor/descendant, having a row for each ancestor/descendant pair.
     *
     * <p>The closed hierarchy has two levels: the detail level (here named
     * [Employee]) is equivalent to the base hierarchy; the [Closure] level
     * relates each descendant to all its ancestors.
     */
    private TestContext getEmpClosureTestContext() {
        return TestContext.createSubstitutingCube(
            "HR",
            "  <Dimension name=\"EmployeesClosure\" foreignKey=\"employee_id\">\n" +
                "      <Hierarchy hasAll=\"true\" allMemberName=\"All Employees\"\n" +
                "          primaryKey=\"employee_id\" primaryKeyTable=\"employee_closure\">\n" +
                "        <Join leftKey=\"supervisor_id\" rightKey=\"employee_id\">\n" +
                "          <Table name=\"employee_closure\"/>\n" +
                "          <Table name=\"employee\" alias=\"employee2\" />\n" +
                "        </Join>\n" +
                "        <Level name=\"Closure\"  type=\"Numeric\" uniqueMembers=\"false\"\n" +
                "            table=\"employee_closure\" column=\"supervisor_id\"/>\n" +
                "        <Level name=\"Employee\" type=\"Numeric\" uniqueMembers=\"true\"\n" +
                "            table=\"employee_closure\" column=\"employee_id\"/>\n" +
                "      </Hierarchy>\n" +
                "  </Dimension>");
    }

    public void testAll() {
        assertQueryReturns(
            "select {[Measures].[Org Salary], [Measures].[Count]} on columns,\n" +
                " {[Employees]} on rows\n" +
                "from [HR]",
            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Org Salary]}\n" +
                "{[Measures].[Count]}\n" +
                "Axis #2:\n" +
                "{[Employees].[All Employees]}\n" +
                "Row #0: $39,431.67\n" +
                "Row #0: 7,392\n"));
    }

    public void testChildrenOfAll() {
        assertQueryReturns(
            "select {[Measures].[Org Salary], [Measures].[Count]} on columns,\n" +
                " {[Employees].children} on rows\n" +
                "from [HR]",
            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Org Salary]}\n" +
                "{[Measures].[Count]}\n" +
                "Axis #2:\n" +
                "{[Employees].[All Employees].[Sheri Nowmer]}\n" +
                "Row #0: $39,431.67\n" +
                "Row #0: 7,392\n"));
    }

    // bug 1063369: DISTINCT COUNT applied to a parent/child hierarchy fails:
    // unsupported when children expanded
     public void testDistinctAll() {
        // parent/child dimension not expanded, and the query works
        assertQueryReturns(
            "select {[Measures].[Count], [Measures].[Org Salary], \n" +
                "[Measures].[Number Of Employees], [Measures].[Avg Salary]} on columns,\n" +
                "{[Employees]} on rows\n" +
                "from [HR]",
            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Count]}\n" +
                "{[Measures].[Org Salary]}\n" +
                "{[Measures].[Number of Employees]}\n" +
                "{[Measures].[Avg Salary]}\n" +
                "Axis #2:\n" +
                "{[Employees].[All Employees]}\n" +
                "Row #0: 7,392\n" +
                "Row #0: $39,431.67\n" +
                "Row #0: 616\n" +
                "Row #0: $64.01\n"));
    }

    public void testDistinctChildrenOfAll() {
        // parent/child dimension expanded: fails with
        // java.lang.UnsupportedOperationException at
        // mondrian.rolap.RolapAggregator$6.aggregate(RolapAggregator.java:72)
        assertQueryReturns(
            "select {[Measures].[Count], [Measures].[Org Salary], \n" +
                "[Measures].[Number Of Employees], [Measures].[Avg Salary]} on columns,\n" +
                "{[Employees].children} on rows\n" +
                "from [HR]",
            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Count]}\n" +
                "{[Measures].[Org Salary]}\n" +
                "{[Measures].[Number of Employees]}\n" +
                "{[Measures].[Avg Salary]}\n" +
                "Axis #2:\n" +
                "{[Employees].[All Employees].[Sheri Nowmer]}\n" +
                "Row #0: 7,392\n" +
                "Row #0: $39,431.67\n" +
                "Row #0: 616\n" +
                "Row #0: $64.01\n"));
    }

    // same two tests, but on a subtree
    public void testDistinctSubtree() {
        // also fails with UnsupportedOperationException
        assertQueryReturns(
            "select {[Measures].[Count], [Measures].[Org Salary], \n" +
                "[Measures].[Number Of Employees], [Measures].[Avg Salary]} on columns,\n" +
                "{[Employees].[All Employees].[Sheri Nowmer].[Rebecca Kanagaki]} on rows\n" +
                "from [HR]",
            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Count]}\n" +
                "{[Measures].[Org Salary]}\n" +
                "{[Measures].[Number of Employees]}\n" +
                "{[Measures].[Avg Salary]}\n" +
                "Axis #2:\n" +
                "{[Employees].[All Employees].[Sheri Nowmer].[Rebecca Kanagaki]}\n" +
                "Row #0: 24\n" +
                "Row #0: $234.36\n" +
                "Row #0: 2\n" +
                "Row #0: $117.18\n"));
    }


    /**
     * Verifies that COUNT DISTINCT works against the explict closure of the
     * parent/child hierarchy. (Repeats the last 4 tests.)
     */
    public void testDistinctAllExplicitClosure() {
        getEmpClosureTestContext().assertQueryReturns(
            "select {[Measures].[Count], [Measures].[Org Salary], \n" +
                "[Measures].[Number Of Employees], [Measures].[Avg Salary]} on columns,\n" +
                "{[EmployeesClosure]} on rows\n" +
                "from [HR]",
            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Count]}\n" +
                "{[Measures].[Org Salary]}\n" +
                "{[Measures].[Number of Employees]}\n" +
                "{[Measures].[Avg Salary]}\n" +
                "Axis #2:\n" +
                "{[EmployeesClosure].[All Employees]}\n" +
                "Row #0: 7,392\n" +
                "Row #0: $39,431.67\n" +
                "Row #0: 616\n" +
                "Row #0: $64.01\n"));
    }

    public void testDistinctChildrenOfAllExplicitClosure() {
        // the children of the closed relation are all the descendants, so limit results
        getEmpClosureTestContext().assertQueryReturns(
                "select {[Measures].[Count], [Measures].[Org Salary], \n" +
                "[Measures].[Number Of Employees], [Measures].[Avg Salary]} on columns,\n" +
                "{[EmployeesClosure].FirstChild} on rows\n" +
                "from [HR]",
                fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Count]}\n" +
                "{[Measures].[Org Salary]}\n" +
                "{[Measures].[Number of Employees]}\n" +
                "{[Measures].[Avg Salary]}\n" +
                "Axis #2:\n" +
                "{[EmployeesClosure].[All Employees].[1]}\n" +
                "Row #0: 7,392\n" +
                "Row #0: $39,431.67\n" +
                "Row #0: 616\n" +
                "Row #0: $64.01\n"));
    }

    public void testDistinctSubtreeExplicitClosure() {
        getEmpClosureTestContext().assertQueryReturns(
            "select {[Measures].[Count], [Measures].[Org Salary], \n" +
                "[Measures].[Number Of Employees], [Measures].[Avg Salary]} on columns,\n" +
                "{[EmployeesClosure].[All Employees].[7]} on rows\n" +
                "from [HR]",
            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Count]}\n" +
                "{[Measures].[Org Salary]}\n" +
                "{[Measures].[Number of Employees]}\n" +
                "{[Measures].[Avg Salary]}\n" +
                "Axis #2:\n" +
                "{[EmployeesClosure].[All Employees].[7]}\n" +
                "Row #0: 24\n" +
                "Row #0: $234.36\n" +
                "Row #0: 2\n" +
                "Row #0: $117.18\n"));
    }



    public void testLeaf() {
        // Juanita Sharp has no reports
        assertQueryReturns(
            "select {[Measures].[Org Salary], [Measures].[Count]} on columns,\n" +
                " {[Employees].[All Employees].[Sheri Nowmer].[Rebecca Kanagaki].[Juanita Sharp]} on rows\n" +
                "from [HR]",
            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Org Salary]}\n" +
                "{[Measures].[Count]}\n" +
                "Axis #2:\n" +
                "{[Employees].[All Employees].[Sheri Nowmer].[Rebecca Kanagaki].[Juanita Sharp]}\n" +
                "Row #0: $72.36\n" +
                "Row #0: 12\n"));
    }

    public void testOneAboveLeaf() {
        // Rebecca Kanagaki has 2 direct reports, and they have no reports
        assertQueryReturns(
            "select {[Measures].[Org Salary], [Measures].[Count]} on columns,\n" +
                " {[Employees].[All Employees].[Sheri Nowmer].[Rebecca Kanagaki]} on rows\n" +
                "from [HR]",
            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Org Salary]}\n" +
                "{[Measures].[Count]}\n" +
                "Axis #2:\n" +
                "{[Employees].[All Employees].[Sheri Nowmer].[Rebecca Kanagaki]}\n" +
                "Row #0: $234.36\n" +
                "Row #0: 24\n"));
    }

    /**
     * Script That Uses the LEAVES Flag to Return the Bottom 10 Dimension
     * Members, from <a href="http://www.winscriptingsolutions.com/Files/09/27139/Listing_01.txt">here</a>.
     */
    public void testFoo() {
        assertQueryReturns(
                "WITH SET [NonEmptyEmployees] AS 'FILTER(DESCENDANTS([Employees].[All Employees], 10, LEAVES),\n" +
                "  NOT ISEMPTY( [Measures].[Employee Salary]) )'\n" +
                "SELECT { [Measures].[Employee Salary], [Measures].[Number of Employees] } ON COLUMNS,\n" +
                "  BOTTOMCOUNT([NonEmptyEmployees], 10, [Measures].[Employee Salary]) ON ROWS\n" +
                "FROM HR\n" +
                "WHERE ([Pay Type].[Hourly])",
            fold("Axis #0:\n" +
                "{[Pay Type].[All Pay Types].[Hourly]}\n" +
                "Axis #1:\n" +
                "{[Measures].[Employee Salary]}\n" +
                "{[Measures].[Number of Employees]}\n" +
                "Axis #2:\n" +
                "{[Employees].[All Employees].[Sheri Nowmer].[Derrick Whelply].[Laurie Borges].[Eric Long].[Adam Reynolds].[William Hapke].[Marie Richmeier]}\n" +
                "{[Employees].[All Employees].[Sheri Nowmer].[Derrick Whelply].[Pedro Castillo].[Jose Bernard].[Mary Hunt].[Bonnie Bruno].[Ellen Gray]}\n" +
                "{[Employees].[All Employees].[Sheri Nowmer].[Derrick Whelply].[Laurie Borges].[Paula Nickell].[Kristine Cleary].[Carla Zubaty].[Hattie Haemon]}\n" +
                "{[Employees].[All Employees].[Sheri Nowmer].[Derrick Whelply].[Pedro Castillo].[Lois Wood].[Dell Gras].[Christopher Solano].[Sarah Amole]}\n" +
                "{[Employees].[All Employees].[Sheri Nowmer].[Derrick Whelply].[Pedro Castillo].[Charles Macaluso].[Barbara Wallin].[Kenneth Turner].[Shirley Head]}\n" +
                "{[Employees].[All Employees].[Sheri Nowmer].[Derrick Whelply].[Pedro Castillo].[Lois Wood].[Dell Gras].[Christopher Solano].[Mary Hall]}\n" +
                "{[Employees].[All Employees].[Sheri Nowmer].[Derrick Whelply].[Pedro Castillo].[Lin Conley].[Paul Tays].[Pat Chin].[Yasmina Brown]}\n" +
                "{[Employees].[All Employees].[Sheri Nowmer].[Derrick Whelply].[Laurie Borges].[Eric Long].[Adam Reynolds].[Joshua Huff].[Teanna Cobb]}\n" +
                "{[Employees].[All Employees].[Sheri Nowmer].[Derrick Whelply].[Pedro Castillo].[Lois Wood].[Dell Gras].[Kristine Aldred].[Kenton Forham]}\n" +
                "{[Employees].[All Employees].[Sheri Nowmer].[Derrick Whelply].[Laurie Borges].[Mary Solimena].[Matthew Hunter].[Eddie Holmes].[Donald Thompson]}\n" +
                "Row #0: $39.44\n" +
                "Row #0: 1\n" +
                "Row #1: $39.52\n" +
                "Row #1: 1\n" +
                "Row #2: $39.52\n" +
                "Row #2: 1\n" +
                "Row #3: $39.60\n" +
                "Row #3: 1\n" +
                "Row #4: $39.62\n" +
                "Row #4: 1\n" +
                "Row #5: $39.62\n" +
                "Row #5: 1\n" +
                "Row #6: $39.66\n" +
                "Row #6: 1\n" +
                "Row #7: $39.67\n" +
                "Row #7: 1\n" +
                "Row #8: $39.75\n" +
                "Row #8: 1\n" +
                "Row #9: $39.75\n" +
                "Row #9: 1\n"));
    }

    /**
     * Script from <a href="http://www.winscriptingsolutions.com/Files/09/27139/Listing_02.txt">here</a>.
     */
    public void testBar() {
        assertQueryReturns(
                "with set [Leaves] as 'Descendants([Employees].[All Employees], 15, LEAVES )'\n" +
                " set [Parents] as 'Generate( [Leaves], {[Employees].CurrentMember.Parent} )'\n" +
                " set [FirstParents] as 'Filter( [Parents], \n" +
                "Count( Descendants( [Employees].CurrentMember, 2 )) = 0 )'\n" +
                "select {[Measures].[Number of Employees]} on Columns,\n" +
                "  TopCount( [FirstParents], 10, [Measures].[Number of Employees]) on Rows\n" +
                "from HR",
            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Number of Employees]}\n" +
                "Axis #2:\n" +
                "{[Employees].[All Employees].[Sheri Nowmer].[Derrick Whelply].[Beverly Baker].[Jacqueline Wyllie].[Ralph Mccoy].[Anne Tuck]}\n" +
                "{[Employees].[All Employees].[Sheri Nowmer].[Derrick Whelply].[Beverly Baker].[Jacqueline Wyllie].[Ralph Mccoy].[Joy Sincich]}\n" +
                "{[Employees].[All Employees].[Sheri Nowmer].[Derrick Whelply].[Beverly Baker].[Jacqueline Wyllie].[Ralph Mccoy].[Bertha Jameson]}\n" +
                "{[Employees].[All Employees].[Sheri Nowmer].[Derrick Whelply].[Laurie Borges].[Mary Solimena].[Matthew Hunter].[Florence Vonholt]}\n" +
                "{[Employees].[All Employees].[Sheri Nowmer].[Derrick Whelply].[Laurie Borges].[Mary Solimena].[Matthew Hunter].[Eddie Holmes]}\n" +
                "{[Employees].[All Employees].[Sheri Nowmer].[Derrick Whelply].[Laurie Borges].[Mary Solimena].[Matthew Hunter].[Gerald Drury]}\n" +
                "{[Employees].[All Employees].[Sheri Nowmer].[Derrick Whelply].[Pedro Castillo].[Jose Bernard].[Mary Hunt].[Libby Allen]}\n" +
                "{[Employees].[All Employees].[Sheri Nowmer].[Derrick Whelply].[Pedro Castillo].[Jose Bernard].[Mary Hunt].[Bonnie Bruno]}\n" +
                "{[Employees].[All Employees].[Sheri Nowmer].[Derrick Whelply].[Pedro Castillo].[Jose Bernard].[Mary Hunt].[Angela Bowers]}\n" +
                "{[Employees].[All Employees].[Sheri Nowmer].[Derrick Whelply].[Pedro Castillo].[Charles Macaluso].[Barbara Wallin].[Michael Bruha]}\n" +
                "Row #0: 23\n" +
                "Row #1: 23\n" +
                "Row #2: 23\n" +
                "Row #3: 23\n" +
                "Row #4: 23\n" +
                "Row #5: 23\n" +
                "Row #6: 19\n" +
                "Row #7: 19\n" +
                "Row #8: 19\n" +
                "Row #9: 19\n"));
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
        assertQueryReturns(
            "SELECT {[Employees].[All Employees].Children} on columns,\n" +
                " {[Measures].[Org Salary]} on rows\n" +
                "FROM [HR]",

            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Employees].[All Employees].[Sheri Nowmer]}\n" +
                "Axis #2:\n" +
                "{[Measures].[Org Salary]}\n" +
                "Row #0: $39,431.67\n"));

        TestContext testContext = TestContext.create(
            null,
            "<Cube name='HR-fewer-dims'>\n" +
                "    <Table name='salary'/>\n" +
                "    <Dimension name='Department' foreignKey='department_id'>\n" +
                "        <Hierarchy hasAll='true' primaryKey='department_id'>\n" +
                "            <Table name='department'/>\n" +
                "            <Level name='Department Description' uniqueMembers='true' column='department_id'/>\n" +
                "        </Hierarchy>\n" +
                "    </Dimension>\n" +
                "    <Dimension name='Employees' foreignKey='employee_id'>\n" +
                "        <Hierarchy hasAll='true' allMemberName='All Employees' primaryKey='employee_id'>\n" +
                "            <Table name='employee'/>\n" +
                "            <Level name='Employee Id' type='Numeric' uniqueMembers='true' column='employee_id' parentColumn='supervisor_id' nameColumn='full_name' nullParentValue='0'>\n" +
                "                <Property name='Marital Status' column='marital_status'/>\n" +
                "                <Property name='Position Title' column='position_title'/>\n" +
                "                <Property name='Gender' column='gender'/>\n" +
                "                <Property name='Salary' column='salary'/>\n" +
                "                <Property name='Education Level' column='education_level'/>\n" +
                "                <Property name='Management Role' column='management_role'/>\n" +
                "            </Level>\n" +
                "        </Hierarchy>\n" +
                "    </Dimension>\n" +
                "    <Measure name='Org Salary' column='salary_paid' aggregator='sum' formatString='Currency' />\n" +
                "    <Measure name='Count' column='employee_id' aggregator='count' formatString='#,#'/>\n" +
                "</Cube>", null, null, null);

        // On a cube with fewer dimensions, this gave a false failure.
        testContext.assertQueryReturns(
            "SELECT {[Employees].[All Employees].Children} on columns,\n" +
                " {[Measures].[Org Salary]} on rows\n" +
                "FROM [HR-fewer-dims]",
            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Employees].[All Employees].[Sheri Nowmer]}\n" +
                "Axis #2:\n" +
                "{[Measures].[Org Salary]}\n" +
                "Row #0: $271,552.44\n"));
    }

    public void testGenuineCycle() {
        Result result = executeQuery("with member [Measures].[Foo] as \n" +
                    "  '([Measures].[Foo], OpeningPeriod([Time].[Month]))'\n" +
                    "select\n" +
                    " {[Measures].[Unit Sales], [Measures].[Foo]} on Columns,\n" +
                    " { [Time].[1997].[Q2]} on rows\n" +
                    "from [Sales]");
        String resultString = TestContext.toString(result);

        // The precise moment when the cycle is detected depends upon the state
        // of the cache, so this test can throw various errors. Here are come
        // examples:
        //
        // Axis #0:
        // {}
        // Axis #1:
        // {[Measures].[Unit Sales]}
        // {[Measures].[Foo]}
        // Axis #2:
        // {[Time].[1997].[Q2]}
        // Row #0: 62,610
        // Row #0: #ERR: mondrian.olap.fun.MondrianEvaluationException: Infinite loop while evaluating calculated member '[Measures].[Foo]'; context stack is {([Time].[1997].[Q2].[4]), ([Time].[1997].[Q2].[4]), ([Time].[1997].[Q2].[4]), ([Time].[1997].[Q2].[4]), ([Time].[1997].[Q2].[4]), ([Time].[1997].[Q2].[4]), ([Time].[1997].[Q2].[4]), ([Time].[1997].[Q2].[4]), ([Time].[1997].[Q2].[4]), ([Time].[1997].[Q2].[4]), ([Time].[1997].[Q2].[4]), ([Time].[1997].[Q2])}
        //
        // Axis #0:
        // {}
        // Axis #1:
        // {[Measures].[Unit Sales]}
        // {[Measures].[Foo]}
        // Axis #2:
        // {[Time].[1997].[Q2]}
        // Row #0: (null)
        // Row #0: #ERR: mondrian.olap.fun.MondrianEvaluationException: Infinite loop while evaluating calculated member '[Measures].[Foo]'; context stack is {([Store].[All Stores].[Mexico], [Time].[1997].[Q2].[4]), ([Store].[All Stores].[Mexico], [Time].[1997].[Q2].[4]), ([Store].[All Stores].[Mexico], [Time].[1997].[Q2].[4]), ([Store].[All Stores].[Mexico], [Time].[1997].[Q2].[4]), ([Store].[All Stores].[Mexico], [Time].[1997].[Q2].[4]), ([Store].[All Stores].[Mexico], [Time].[1997].[Q2].[4]), ([Store].[All Stores].[Mexico], [Time].[1997].[Q2].[4]), ([Store].[All Stores].[Mexico], [Time].[1997].[Q2].[4]), ([Store].[All Stores].[Mexico], [Time].[1997].[Q2].[4]), ([Store].[All Stores].[Mexico], [Time].[1997].[Q2].[4]), ([Store].[All Stores].[Mexico], [Time].[1997].[Q2].[4]), ([Store].[All Stores].[Mexico], [Time].[1997].[Q2].[4]), ([Store].[All Stores].[Mexico], [Time].[1997].[Q2].[4]), ([Store].[All Stores].[Mexico], [Time].[1997].[Q2])}"
        //
        // So encapsulate the error string as a pattern.
        final String expectedPattern = "(?s).*Row #0: #ERR: mondrian.olap.fun.MondrianEvaluationException.*: Infinite loop while evaluating calculated member \\'\\[Measures\\].\\[Foo\\]\\'; context stack is.*";
        if (!resultString.matches(expectedPattern)) {
            System.out.println(resultString);
            Assert.assertEquals(expectedPattern, resultString);
        }
    }

    public void testParentChildDrillThrough() {
        Result result = executeQuery(
            "select {[Measures].Members} ON columns,\n" +
                "  {[Employees].Members} ON rows\n" +
                "from [HR]");

        String tableQualifier = "as ";
        if (getTestContext().getDialect().isOracle()) {
            // " + tableQualifier + "
            tableQualifier = "";
        }

        // Drill-through for row #0, Employees.All.
        // Note that the SQL does not contain the employees or employee_closure
        // tables.
        final boolean extendedContext = false;
        checkDrillThroughSql(
            result,
            0,
            extendedContext,
            "[Employees].[All Employees]",
            "$39,431.67",
            "select" +
            " `time_by_day`.`the_year` as `Year`," +
            " `salary`.`salary_paid` as `Org Salary` " +
            "from `time_by_day` " + tableQualifier + "`time_by_day`," +
            " `salary` " + tableQualifier + "`salary` " +
            "where `salary`.`pay_date` = `time_by_day`.`the_date`" +
            " and `time_by_day`.`the_year` = 1997");

        // Drill-through for row #1, [Employees].[All].[Sheri Nowmer]
        // Note that the SQL does not contain the employee_closure table.
        // That's because when we drill through, we don't want to roll up
        // measures along the hierarchy.
        checkDrillThroughSql(
            result,
            1,
            extendedContext,
            "[Employees].[All Employees].[Sheri Nowmer]",
            "$39,431.67",
            "select `time_by_day`.`the_year` as `Year`," +
            " `employee_1`.`employee_id` as `Employee Id (Key)`," +
            " `salary`.`salary_paid` as `Org Salary` " +
            "from `time_by_day` " + tableQualifier + "`time_by_day`," +
            " `salary` " + tableQualifier + "`salary`," +
            " `employee` " + tableQualifier + "`employee_1` " +
            "where `salary`.`pay_date` = `time_by_day`.`the_date`" +
            " and `time_by_day`.`the_year` = 1997" +
            " and `salary`.`employee_id` = `employee_1`.`employee_id`" +
            " and `employee_1`.`employee_id` = 1");

        // Drill-through for row #2, [Employees].[All].[Sheri Nowmer].
        // Note that the SQL does not contain the employee_closure table.
        checkDrillThroughSql(
            result,
            2,
            extendedContext,
            "[Employees].[All Employees].[Derrick Whelply]",
            "$36,494.07",
            "select `time_by_day`.`the_year` as `Year`," +
            " `employee_1`.`employee_id` as `Employee Id (Key)`," +
            " `salary`.`salary_paid` as `Org Salary` " +
            "from `time_by_day` " + tableQualifier + "`time_by_day`," +
            " `salary` " + tableQualifier + "`salary`," +
            " `employee` " + tableQualifier + "`employee_1` " +
            "where `salary`.`pay_date` = `time_by_day`.`the_date`" +
            " and `time_by_day`.`the_year` = 1997" +
            " and `salary`.`employee_id` = `employee_1`.`employee_id`" +
            " and `employee_1`.`employee_id` = 2");
    }

    public void testParentChildDrillThroughWithContext() {
        Result result = executeQuery("select {[Measures].Members} ON columns,\n" +
                    "  {[Employees].Members} ON rows\n" +
                    "from [HR]");

        String tableQualifier = "as ";
        if (getTestContext().getDialect().isOracle()) {
            // " + tableQualifier + "
            tableQualifier = "";
        }

        // Now with full context.
        final boolean extendedContext = true;
        checkDrillThroughSql(
            result,
            2,
            extendedContext,
            "[Employees].[All Employees].[Derrick Whelply]",
            "$36,494.07",
            "select" +
            " `time_by_day`.`month_of_year` as `Month (Key)`," +
            " `time_by_day`.`the_month` as `Month`," +
            " `time_by_day`.`quarter` as `Quarter`," +
            " `time_by_day`.`the_year` as `Year`," +
            " `store`.`store_name` as `Store Name`," +
            " `store`.`store_city` as `Store City`," +
            " `store`.`store_state` as `Store State`," +
            " `store`.`store_country` as `Store Country`," +
            " `position`.`pay_type` as `Pay Type`," +
            " `store`.`store_type` as `Store Type`," +
            " `employee_1`.`position_title` as `Position Title`," +
            " `employee_1`.`management_role` as `Management Role`," +
            " `department`.`department_id` as `Department Description`," +
            " `employee_1`.`employee_id` as `Employee Id (Key)`," +
            " `employee_1`.`full_name` as `Employee Id`," +
            " `salary`.`salary_paid` as `Org Salary` " +
            "from" +
            " `time_by_day` " + tableQualifier + "`time_by_day`," +
            " `salary` " + tableQualifier + "`salary`," +
            " `store` " + tableQualifier + "`store`," +
            " `employee` " + tableQualifier + "`employee_1`," +
            " `position` " + tableQualifier + "`position`," +
            " `department` " + tableQualifier + "`department` " +
            "where `salary`.`pay_date` = `time_by_day`.`the_date`" +
            " and `time_by_day`.`the_year` = 1997" +
            " and `salary`.`employee_id` = `employee_1`.`employee_id`" +
            " and `employee_1`.`store_id` = `store`.`store_id`" +
            " and `employee_1`.`position_id` = `position`.`position_id`" +
            " and `salary`.`department_id` = `department`.`department_id`" +
            " and `employee_1`.`employee_id` = 2");
    }

    private void checkDrillThroughSql(Result result,
        int row,
        boolean extendedContext, String expectedMember,
        String expectedCell,
        String expectedSql)
    {
        final Member empMember = result.getAxes()[1].positions[row].members[0];
        assertEquals(expectedMember, empMember.getUniqueName());
        // drill through member
        final Cell cell = result.getCell(new int[] {0, row});
        assertEquals(expectedCell, cell.getFormattedValue());
        String sql = cell.getDrillThroughSQL(extendedContext);
        sql = sql.replace('"', '`');
        /*
         * DB2 does not have quotes on identifiers
         */
        RolapConnection conn = (RolapConnection) getConnection();
        String jdbc_url = conn.getConnectInfo().get("Jdbc");
        if (jdbc_url.toLowerCase().indexOf(":db2:") >= 0) {
            expectedSql = expectedSql.replaceAll("`", "");
        }
        assertEquals(expectedSql, sql);
    }

    /**
     * Testcase for bug 1459995, "NullPointerException in
     * RolapEvaluator.setContext(....)".
     */
    public void testBug1459995() {
        assertQueryReturns(
                "select \n" +
                "     {[Employee Salary]} on columns, \n" +
                "     {[Employees]} on rows \n" +
                "from [HR]",
                fold(
                    "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Employee Salary]}\n" +
                    "Axis #2:\n" +
                    "{[Employees].[All Employees]}\n" +
                    "Row #0: \n"));

        assertQueryReturns(
                "select \n" +
                "     {[Position]} on columns,\n" +
                "     {[Employee Salary]} on rows\n" +
                "from [HR]",
                fold(
                    "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Position].[All Position]}\n" +
                    "Axis #2:\n" +
                    "{[Measures].[Employee Salary]}\n" +
                    "Row #0: \n"));
    }

    /**
     * Tests that a parent-child hierarchy is sorted correctly if the
     * "ordinalColumn" attribute is included in its definition.
     * Testcase for bug 1522608, "Sorting of Parent/Child Hierarchy is wrong".
     */
    public void testParentChildOrdinal() {
        TestContext testContext = TestContext.create(
            null,
            "<Cube name=\"HR-ordered\">\n" +
                "  <Table name=\"salary\"/>\n" +
                "  <Dimension name=\"Employees\" foreignKey=\"employee_id\">\n" +
                "    <Hierarchy hasAll=\"true\" allMemberName=\"All Employees\"\n" +
                "        primaryKey=\"employee_id\">\n" +
                "      <Table name=\"employee\"/>\n" +
                "      <Level name=\"Employee Id\" type=\"Numeric\" uniqueMembers=\"true\"\n" +
                "          column=\"employee_id\" parentColumn=\"supervisor_id\"\n" +
                "          nameColumn=\"full_name\" nullParentValue=\"0\"" +
                // Original "HR" cube has no ordinalColumn.
                "          ordinalColumn=\"last_name\" >\n" +
                "        <Closure parentColumn=\"supervisor_id\" childColumn=\"employee_id\">\n" +
                "          <Table name=\"employee_closure\"/>\n" +
                "        </Closure>\n" +
                "        <Property name=\"First Name\" column=\"first_name\"/>\n" +
                "      </Level>\n" +
                "    </Hierarchy>\n" +
                "  </Dimension>\n" +
                "\n" +
                "  <Measure name=\"Org Salary\" column=\"salary_paid\" aggregator=\"sum\"\n" +
                "      formatString=\"Currency\"/>\n" +
                "  <Measure name=\"Count\" column=\"employee_id\" aggregator=\"count\"\n" +
                "      formatString=\"#,#\"/>\n" +
                "</Cube>", null, null, null);

        // Make sure <Hierarchy>.MEMBERS is sorted.
        // Note that last_name is not unique, and databases may return members
        // in arbitrary order -- so to keep things deterministic, this example
        // deliberately uses a set of  employees with unique last names.
        testContext.assertQueryReturns(
                "with member [Measures].[First Name] as " +
                        " 'Iif([Employees].Level.Name = \"Employee Id\", [Employees].CurrentMember.Properties(\"First Name\"), Cast(NULL AS STRING)) '\n" +
                        "select {[Measures].[Org Salary], [Measures].[First Name]} on columns,\n" +
                        " {Tail(Head([Employees].Members, 15), 4)} on rows\n" +
                        "from [HR-ordered]",
                fold("Axis #0:\n" +
                        "{}\n" +
                        "Axis #1:\n" +
                        "{[Measures].[Org Salary]}\n" +
                        "{[Measures].[First Name]}\n" +
                        "Axis #2:\n" +
                        "{[Employees].[All Employees].[Ronald Adina]}\n" +
                        "{[Employees].[All Employees].[Samuel Agcaoili]}\n" +
                        "{[Employees].[All Employees].[James Aguilar]}\n" +
                        "{[Employees].[All Employees].[Robert Ahlering]}\n" +
                        "Row #0: $107.16\n" +
                        "Row #0: Ronald\n" +
                        "Row #1: $981.82\n" +
                        "Row #1: Samuel\n" +
                        "Row #2: $403.64\n" +
                        "Row #2: James\n" +
                        "Row #3: $40.47\n" +
                        "Row #3: Robert\n"));

        // Make sure <Member>.CHILDREN is sorted.
        testContext.assertQueryReturns(
            "select {[Employees].[All Employees].[Sheri Nowmer].[Rebecca Kanagaki].Children} on columns from [HR-ordered]",

            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Employees].[All Employees].[Sheri Nowmer].[Rebecca Kanagaki].[Sandra Brunner]}\n" +
                "{[Employees].[All Employees].[Sheri Nowmer].[Rebecca Kanagaki].[Juanita Sharp]}\n" +
                "Row #0: $60.00\n" +
                "Row #0: $152.76\n"));

        // Make sure <Member>.DESCENDANTS is sorted.
        testContext.assertQueryReturns(
            "select {HEAD(DESCENDANTS([Employees].[Sheri Nowmer], [Employees].[Employee Id], LEAVES), 6)} on columns from [HR-ordered]",
            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Employees].[All Employees].[Sheri Nowmer].[Donna Arnold].[Howard Bechard]}\n" +
                "{[Employees].[All Employees].[Sheri Nowmer].[Donna Arnold].[Doris Carter]}\n" +
                "{[Employees].[All Employees].[Sheri Nowmer].[Roberta Damstra].[Phyllis Burchett]}\n" +
                "{[Employees].[All Employees].[Sheri Nowmer].[Roberta Damstra].[Jennifer Cooper]}\n" +
                "{[Employees].[All Employees].[Sheri Nowmer].[Roberta Damstra].[Jessica Olguin]}\n" +
                "{[Employees].[All Employees].[Sheri Nowmer].[Roberta Damstra].[Peggy Petty]}\n" +
                "Row #0: $193.80\n" +
                "Row #0: $60.00\n" +
                "Row #0: $120.00\n" +
                "Row #0: $152.76\n" +
                "Row #0: $120.00\n" +
                "Row #0: $182.40\n"));
    }

    public void testLevelMembers() {
        final TestContext testContext = new TestContext() {
            public String getDefaultCubeName() {
                return "HR";
            }
        };
        // <Dimension>.MEMBERS
        testContext.assertExprReturns("[Employees].Members.Count", "1,156");
        // <Level>.MEMBERS
        testContext.assertExprReturns("[Employees].[Employee Id].Members.Count", "1,155");
        // <Member>.CHILDREN
        testContext.assertExprReturns("[Employees].[Sheri Nowmer].Children.Count", "7");

        // Make sure that members of the [Employee] hierarachy don't
        // as calculated (even though they are calculated, internally)
        // but that real calculated members are counted as calculated.
        testContext.assertQueryReturns(
                "with member [Employees].[Foo] as ' Sum([Employees].[All Employees].[Sheri Nowmer].[Donna Arnold].Children) '\n" +
                        "member [Measures].[Count1] AS [Employees].MEMBERS.Count\n" +
                        "member [Measures].[Count2] AS [Employees].ALLMEMBERS.COUNT\n" +
                        "select {[Measures].[Count1], [Measures].[Count2]} ON COLUMNS\n" +
                        "from [HR]",
                fold("Axis #0:\n" +
                        "{}\n" +
                        "Axis #1:\n" +
                        "{[Measures].[Count1]}\n" +
                        "{[Measures].[Count2]}\n" +
                        "Row #0: 1,156\n" +
                        "Row #0: 1,157\n"));
    }
}

// End ParentChildHierarchyTest.java
