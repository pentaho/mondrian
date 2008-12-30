/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2003-2008 Julian Hyde
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

    /**
     * Returns a TestContext in which the "HR" cube contains an extra dimension,
     * "EmployeesSnowFlake", which is a joined hierarchy with a closure.
     * this is almost identical to employee, except we do a join with store
     * to validate joins with closures work
     */
    private TestContext getEmpSnowFlakeClosureTestContext() {
        return TestContext.createSubstitutingCube(
            "HR",
            "<Dimension name=\"EmployeeSnowFlake\" foreignKey=\"employee_id\">" +
            "<Hierarchy hasAll=\"true\" allMemberName=\"All Employees\"" +
            "    primaryKey=\"employee_id\" primaryKeyTable=\"employee\">" +
            "  <Join leftKey=\"store_id\"" +
            "    rightAlias=\"store\" rightKey=\"store_id\">" +
            "    <Table name=\"employee\"/>" +
            "    <Table name=\"store\"/>" +
            "  </Join>" +
            "  <Level name=\"Employee Stores\" table=\"store\"" +
            "      column=\"store_id\" uniqueMembers=\"true\"/>" +
            "  <Level name=\"Employee Id\" type=\"Numeric\" table=\"employee\" uniqueMembers=\"true\"" +
            "      column=\"employee_id\" parentColumn=\"supervisor_id\"" +
            "      nameColumn=\"full_name\" nullParentValue=\"0\">" +
            "    <Closure parentColumn=\"supervisor_id\" childColumn=\"employee_id\">" +
            "      <Table name=\"employee_closure\"/>" +
            "    </Closure>" +
            "    <Property name=\"Marital Status\" column=\"marital_status\"/>" +
            "    <Property name=\"Position Title\" column=\"position_title\"/>" +
            "    <Property name=\"Gender\" column=\"gender\"/>" +
            "    <Property name=\"Salary\" column=\"salary\"/>" +
            "    <Property name=\"Education Level\" column=\"education_level\"/>" +
            "    <Property name=\"Management Role\" column=\"management_role\"/>" +
            "  </Level>" +
            "</Hierarchy>" +
            "</Dimension>");
    }

    /**
     * Returns a TestContext in which the "HR" cube contains an extra dimension,
     * "EmployeesSnowFlake", which is a joined hierarchy with a closure.
     * this is almost identical to employee, except we do a join with store
     * to validate joins with closures work
     */
    private TestContext getEmpSharedClosureTestContext() {
        String sharedClosureDimension =
            "<Dimension name=\"SharedEmployee\">" +
            "<Hierarchy hasAll=\"true\"" +
            "    primaryKey=\"employee_id\" primaryKeyTable=\"employee\">" +
            "  <Join leftKey=\"store_id\"" +
            "    rightAlias=\"store\" rightKey=\"store_id\">" +
            "    <Table name=\"employee\"/>" +
            "    <Table name=\"store\"/>" +
            "  </Join>" +
            "  <Level name=\"Employee Id\" type=\"Numeric\" table=\"employee\" uniqueMembers=\"true\"" +
            "      column=\"employee_id\" parentColumn=\"supervisor_id\"" +
            "      nameColumn=\"full_name\" nullParentValue=\"0\">" +
            "    <Closure parentColumn=\"supervisor_id\" childColumn=\"employee_id\">" +
            "      <Table name=\"employee_closure\"/>" +
            "    </Closure>" +
            "    <Property name=\"Marital Status\" column=\"marital_status\"/>" +
            "    <Property name=\"Position Title\" column=\"position_title\"/>" +
            "    <Property name=\"Gender\" column=\"gender\"/>" +
            "    <Property name=\"Salary\" column=\"salary\"/>" +
            "    <Property name=\"Education Level\" column=\"education_level\"/>" +
            "    <Property name=\"Management Role\" column=\"management_role\"/>" +
            "  </Level>" +
            "</Hierarchy>" +
            "</Dimension>";

        String cube =
            "<Cube name=\"EmployeeSharedClosureCube\">\n" +
            "  <Table name=\"salary\" alias=\"salary_closure\" />\n" +
            "  <DimensionUsage name=\"SharedEmployee\" source=\"SharedEmployee\" foreignKey=\"employee_id\" />\n" +
            "  <Dimension name=\"Department\" foreignKey=\"department_id\">" +
            "    <Hierarchy hasAll=\"true\" primaryKey=\"department_id\">" +
            "      <Table name=\"department\"/>" +
            "        <Level name=\"Department Description\" uniqueMembers=\"true\"" +
            "          column=\"department_id\"/>" +
            "    </Hierarchy>" +
            "  </Dimension>" +
            "  <DimensionUsage name=\"Store Type\" source=\"Store Type\" foreignKey=\"warehouse_id\" />\n" +
            "  <Measure name=\"Org Salary\" column=\"salary_paid\" aggregator=\"sum\""+
            "      formatString=\"Currency\"/>"+
            "   <Measure name=\"Count\" column=\"employee_id\" aggregator=\"count\""+
            "    formatString=\"#,#\"/>" +
            "</Cube>";


        return TestContext.create(sharedClosureDimension, cube, null, null, null, null);
    }

    /**
     * Returns a TestContext in which the "HR" cube contains an extra dimension,
     * "EmployeesNonClosure", which is a joined parent child hierarchy with no
     * closure. this is almost identical to employee, except we removed the closure
     * to validate that non-closures work
     */
    private TestContext getEmpNonClosureTestContext() {
        return TestContext.createSubstitutingCube(
            "HR",
            "<Dimension name=\"EmployeesNonClosure\" foreignKey=\"employee_id\">" +
            "<Hierarchy hasAll=\"true\" allMemberName=\"All Employees\"" +
            "    primaryKey=\"employee_id\">" +
            "  <Table name=\"employee\"/>" +
            "  <Level name=\"Employee Id\" type=\"Numeric\" uniqueMembers=\"true\"" +
            "      column=\"employee_id\" parentColumn=\"supervisor_id\"" +
            "      nameColumn=\"full_name\" nullParentValue=\"0\">" +
            "    <Property name=\"Marital Status\" column=\"marital_status\"/>" +
            "    <Property name=\"Position Title\" column=\"position_title\"/>" +
            "    <Property name=\"Gender\" column=\"gender\"/>" +
            "    <Property name=\"Salary\" column=\"salary\"/>" +
            "    <Property name=\"Education Level\" column=\"education_level\"/>" +
            "    <Property name=\"Management Role\" column=\"management_role\"/>" +
            "  </Level>" +
            "</Hierarchy>" +
            "</Dimension>",
            null);
    }

    /**
     * Tests snow flake closure combination.
     * bug #1675125 - now fixed.
     */
    public void testSnowflakeClosure() {
        getEmpSnowFlakeClosureTestContext().assertQueryReturns(
            "select {[Measures].[Count], [Measures].[Org Salary], \n" +
                "[Measures].[Number Of Employees], [Measures].[Avg Salary]} on columns,\n" +
                "{[EmployeeSnowFlake]} on rows\n" +
                "from [HR]",
            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Count]}\n" +
                "{[Measures].[Org Salary]}\n" +
                "{[Measures].[Number of Employees]}\n" +
                "{[Measures].[Avg Salary]}\n" +
                "Axis #2:\n" +
                "{[EmployeeSnowFlake].[All Employees]}\n" +
                "Row #0: 7,392\n" +
                "Row #0: $39,431.67\n" +
                "Row #0: 616\n" +
                "Row #0: $64.01\n"));
    }

    public void testSharedClosureParentChildHierarchy() {
        TestContext context = getEmpSharedClosureTestContext();
        context.assertQueryReturns(
                "Select " +
                "{[SharedEmployee].[All SharedEmployees].[Sheri Nowmer].[Derrick Whelply].children} on columns " + // [SharedEmployee].[Sheri Nowmer].children
                "from EmployeeSharedClosureCube",
                fold(
                        "Axis #0:\n" +
                        "{}\n" +
                        "Axis #1:\n" +
                        "{[SharedEmployee].[All SharedEmployees].[Sheri Nowmer].[Derrick Whelply].[Beverly Baker]}\n" +
                        "{[SharedEmployee].[All SharedEmployees].[Sheri Nowmer].[Derrick Whelply].[Pedro Castillo]}\n" +
                        "{[SharedEmployee].[All SharedEmployees].[Sheri Nowmer].[Derrick Whelply].[Laurie Borges]}\n" +
                        "Row #0: $10,256.30\n" +
                        "Row #0: $29,121.55\n" +
                        "Row #0: $35,487.69\n"));
    }

    /**
     * Test case for bug #1708327
     */
    public void _testNonClosureParentChildHierarchy() {
        getEmpNonClosureTestContext().assertQueryReturns(
                "Select " +
                "{[EmployeesNonClosure].[Sheri Nowmer].children} on columns," +
                "{[Time].[1997]} ON rows " +
                "from HR",
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[EmployeesNonClosure].[All Employees].[Sheri Nowmer].[Derrick Whelply]}\n" +
                "{[EmployeesNonClosure].[All Employees].[Sheri Nowmer].[Michael Spence]}\n" +
                "{[EmployeesNonClosure].[All Employees].[Sheri Nowmer].[Maya Gutierrez]}\n" +
                "{[EmployeesNonClosure].[All Employees].[Sheri Nowmer].[Roberta Damstra]}\n" +
                "{[EmployeesNonClosure].[All Employees].[Sheri Nowmer].[Rebecca Kanagaki]}\n" +
                "{[EmployeesNonClosure].[All Employees].[Sheri Nowmer].[Darren Stanz]}\n" +
                "{[EmployeesNonClosure].[All Employees].[Sheri Nowmer].[Donna Arnold]}\n" +
                "Axis #2:\n" +
                "{[Time].[1997]}\n" +
                "Row #0: $36,494.07\n" +
                "Row #0: \n" +
                "Row #0: \n" +
                "Row #0: $428.76\n" +
                "Row #0: $234.36\n" +
                "Row #0: $832.68\n" +
                "Row #0: $577.80\n");
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
    public void testParentChildDescendantsLeavesBottom() {
        assertQueryReturns(
                "WITH SET [NonEmptyEmployees] AS 'FILTER(DESCENDANTS([Employees].[All Employees], 10, LEAVES),\n" +
                "  NOT ISEMPTY([Measures].[Employee Salary]))'\n" +
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
    public void testParentChildDescendantsLeavesTop() {
        assertQueryReturns(
                "with set [Leaves] as 'Descendants([Employees].[All Employees], 15, LEAVES)'\n" +
                " set [Parents] as 'Generate([Leaves], {[Employees].CurrentMember.Parent})'\n" +
                " set [FirstParents] as 'Filter([Parents], \n" +
                "Count(Descendants( [Employees].CurrentMember, 2)) = 0 )'\n" +
                "select {[Measures].[Number of Employees]} on Columns,\n" +
                "  TopCount([FirstParents], 10, [Measures].[Number of Employees]) on Rows\n" +
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

    public void testAllMembersParent() {
        final String expected = fold("Axis #0:\n" +
            "{}\n" +
            "Axis #1:\n" +
            "{[Measures].[Parent]}\n" +
            "Axis #2:\n" +
            "{[Employees].[All Employees]}\n" +
            "{[Employees].[All Employees].[Sheri Nowmer]}\n" +
            "{[Employees].[All Employees].[Sheri Nowmer].[Derrick Whelply]}\n" +
            "{[Employees].[All Employees].[Sheri Nowmer].[Derrick Whelply].[Beverly Baker]}\n" +
            "{[Employees].[All Employees].[Sheri Nowmer].[Derrick Whelply].[Beverly Baker].[Shauna Wyro]}\n" +
            "{[Employees].[All Employees].[Sheri Nowmer].[Derrick Whelply].[Beverly Baker].[Shauna Wyro].[Bunny McCown]}\n" +
            "{[Employees].[All Employees].[Sheri Nowmer].[Derrick Whelply].[Beverly Baker].[Shauna Wyro].[Bunny McCown].[Nancy Miller]}\n" +
            "{[Employees].[All Employees].[Sheri Nowmer].[Derrick Whelply].[Beverly Baker].[Shauna Wyro].[Bunny McCown].[Wanda Hollar]}\n" +
            "{[Employees].[All Employees].[Sheri Nowmer].[Derrick Whelply].[Beverly Baker].[Jacqueline Wyllie]}\n" +
            "{[Employees].[All Employees].[Sheri Nowmer].[Derrick Whelply].[Beverly Baker].[Jacqueline Wyllie].[Ralph Mccoy]}\n" +
            "{[Employees].[All Employees].[Sheri Nowmer].[Derrick Whelply].[Beverly Baker].[Jacqueline Wyllie].[Ralph Mccoy].[Anne Tuck]}\n" +
            "{[Employees].[All Employees].[Sheri Nowmer].[Derrick Whelply].[Beverly Baker].[Jacqueline Wyllie].[Ralph Mccoy].[Anne Tuck].[Corinne Zugschwert]}\n" +
            "{[Employees].[All Employees].[Sheri Nowmer].[Derrick Whelply].[Beverly Baker].[Jacqueline Wyllie].[Ralph Mccoy].[Anne Tuck].[Michelle Adams]}\n" +
            "{[Employees].[All Employees].[Sheri Nowmer].[Derrick Whelply].[Beverly Baker].[Jacqueline Wyllie].[Ralph Mccoy].[Anne Tuck].[Donahue Steen]}\n" +
            "{[Employees].[All Employees].[Sheri Nowmer].[Derrick Whelply].[Beverly Baker].[Jacqueline Wyllie].[Ralph Mccoy].[Anne Tuck].[John Baker]}\n" +
            "Row #0: #null\n" +
            "Row #1: All Employees\n" +
            "Row #2: Sheri Nowmer\n" +
            "Row #3: Derrick Whelply\n" +
            "Row #4: Beverly Baker\n" +
            "Row #5: Shauna Wyro\n" +
            "Row #6: Bunny McCown\n" +
            "Row #7: Bunny McCown\n" +
            "Row #8: Beverly Baker\n" +
            "Row #9: Jacqueline Wyllie\n" +
            "Row #10: Ralph Mccoy\n" +
            "Row #11: Anne Tuck\n" +
            "Row #12: Anne Tuck\n" +
            "Row #13: Anne Tuck\n" +
            "Row #14: Anne Tuck\n");

        // Query contains 'Head' just to keep the number of rows reasonable. We
        // assume that it does not affect the behavior of <Hierarchy>.Members.
        assertQueryReturns(
            "with member [Measures].[Parent] as '[Employees].CurrentMember.Parent.Name'\n"
                + "select {[Measures].[Parent]}\n"
                + "ON COLUMNS,\n"
                + "Head([Employees].Members, 15)\n"
                + "ON ROWS from [HR]",
            expected);

        // Similar query, using <Hierarchy>.AllMembers rather than
        // <Hierarchy>.Members, returns the same result.
        assertQueryReturns(
            "with member [Measures].[Parent] as '[Employees].CurrentMember.Parent.Name'\n"
                + "select {[Measures].[Parent]}\n"
                + "ON COLUMNS,\n"
                + "Head([Employees].AllMembers, 15)\n"
                + "ON ROWS from [HR]",
            expected);

        // Similar query use <Level>.Members, same result expected.
        assertQueryReturns(
            "with member [Measures].[Parent] as '[Employees].CurrentMember.Parent.Name'\n"
                + "select {[Measures].[Parent]}\n"
                + "ON COLUMNS,\n"
                + "{[Employees], Head([Employees].[Employee Id].Members, 14)}\n"
                + "ON ROWS from [HR]",
            expected);
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
                "</Cube>", null, null, null, null);

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

    public void testParentChildDrillThrough() throws Exception {
        Result result = executeQuery(
            "select {[Measures].Members} ON columns,\n" +
                "  {[Employees].Members} ON rows\n" +
                "from [HR]");

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
            "from `time_by_day` =as= `time_by_day`," +
            " `salary` =as= `salary` " +
            "where `salary`.`pay_date` = `time_by_day`.`the_date`" +
            " and `time_by_day`.`the_year` = 1997 " +
            "order by `time_by_day`.`the_year` ASC",
            7392);

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
            " `employee`.`employee_id` as `Employee Id (Key)`," +
            " `salary`.`salary_paid` as `Org Salary` " +
            "from `time_by_day` =as= `time_by_day`," +
            " `salary` =as= `salary`," +
            " `employee` =as= `employee` " +
            "where `salary`.`pay_date` = `time_by_day`.`the_date`" +
            " and `time_by_day`.`the_year` = 1997" +
            " and `salary`.`employee_id` = `employee`.`employee_id`" +
            " and `employee`.`employee_id` = 1 " +
            "order by `time_by_day`.`the_year` ASC, `employee`.`employee_id` ASC",
            12);

        // Drill-through for row #2, [Employees].[All].[Sheri Nowmer].
        // Note that the SQL does not contain the employee_closure table.
        checkDrillThroughSql(
            result,
            2,
            extendedContext,
            "[Employees].[All Employees].[Sheri Nowmer].[Derrick Whelply]",
            "$36,494.07",
            "select `time_by_day`.`the_year` as `Year`," +
            " `employee`.`employee_id` as `Employee Id (Key)`," +
            " `salary`.`salary_paid` as `Org Salary` " +
            "from `time_by_day` =as= `time_by_day`," +
            " `salary` =as= `salary`," +
            " `employee` =as= `employee` " +
            "where `salary`.`pay_date` = `time_by_day`.`the_date`" +
            " and `time_by_day`.`the_year` = 1997" +
            " and `salary`.`employee_id` = `employee`.`employee_id`" +
            " and `employee`.`employee_id` = 2 " +
            "order by `time_by_day`.`the_year` ASC, `employee`.`employee_id` ASC",
            12);
    }

    public void testParentChildDrillThroughWithContext() throws Exception {
        Result result = executeQuery("select {[Measures].Members} ON columns,\n" +
                    "  {[Employees].Members} ON rows\n" +
                    "from [HR]");

        // Now with full context.
        final boolean extendedContext = true;
        checkDrillThroughSql(
            result,
            2,
            extendedContext,
            "[Employees].[All Employees].[Sheri Nowmer].[Derrick Whelply]",
            "$36,494.07",
            "select" +
            " `time_by_day`.`the_year` as `Year`," +
            " `time_by_day`.`quarter` as `Quarter`," +
            " `time_by_day`.`the_month` as `Month`," +
            " `time_by_day`.`month_of_year` as `Month (Key)`," +
            " `store`.`store_country` as `Store Country`," +
            " `store`.`store_state` as `Store State`," +
            " `store`.`store_city` as `Store City`," +
            " `store`.`store_name` as `Store Name`," +
            " `position`.`pay_type` as `Pay Type`," +
            " `store`.`store_type` as `Store Type`," +
            " `employee`.`management_role` as `Management Role`," +
            " `employee`.`position_title` as `Position Title`," +
            " `department`.`department_id` as `Department Description`," +
            " `employee`.`full_name` as `Employee Id`," +
            " `employee`.`employee_id` as `Employee Id (Key)`," +
            " `salary`.`salary_paid` as `Org Salary` " +
            "from" +
            " `time_by_day` =as= `time_by_day`," +
            " `salary` =as= `salary`," +
            " `store` =as= `store`," +
            " `employee` =as= `employee`," +
            " `position` =as= `position`," +
            " `department` =as= `department` " +
            "where" +
            " `salary`.`pay_date` = `time_by_day`.`the_date`" +
            " and `time_by_day`.`the_year` = 1997" +
            " and `salary`.`employee_id` = `employee`.`employee_id`" +
            " and `employee`.`store_id` = `store`.`store_id`" +
            " and `employee`.`position_id` = `position`.`position_id`" +
            " and `salary`.`department_id` = `department`.`department_id`" +
            " and `employee`.`employee_id` = 2 " +
            "order by" +
            " `time_by_day`.`the_year` ASC," +
            " `time_by_day`.`quarter` ASC," +
            " `time_by_day`.`the_month` ASC," +
            " `time_by_day`.`month_of_year` ASC," +
            " `store`.`store_country` ASC," +
            " `store`.`store_state` ASC," +
            " `store`.`store_city` ASC," +
            " `store`.`store_name` ASC," +
            " `position`.`pay_type` ASC," +
            " `store`.`store_type` ASC," +
            " `employee`.`management_role` ASC," +
            " `employee`.`position_title` ASC," +
            " `department`.`department_id` ASC," +
            " `employee`.`full_name` ASC," +
            " `employee`.`employee_id` ASC",
            12);
    }

    private void checkDrillThroughSql(
        Result result,
        int row,
        boolean extendedContext,
        String expectedMember,
        String expectedCell,
        String expectedSql,
        int expectedRows)
    throws Exception {
        final Member empMember = result.getAxes()[1].getPositions().get(row).get(0);
        assertEquals(expectedMember, empMember.getUniqueName());
        // drill through member
        final Cell cell = result.getCell(new int[] {0, row});
        assertEquals(expectedCell, cell.getFormattedValue());
        String sql = cell.getDrillThroughSQL(extendedContext);

        getTestContext().assertSqlEquals(expectedSql, sql, expectedRows);
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
                "</Cube>", null, null, null, null);

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
                "{[Employees].[All Employees].[Sheri Nowmer].[Maya Gutierrez].[Brenda Blumberg]}\n" +
                "{[Employees].[All Employees].[Sheri Nowmer].[Maya Gutierrez].[Brenda Blumberg].[Wayne Banack]}\n" +
                "{[Employees].[All Employees].[Sheri Nowmer].[Maya Gutierrez].[Brenda Blumberg].[Wayne Banack].[Samuel Agcaoili]}\n" +
                "{[Employees].[All Employees].[Sheri Nowmer].[Maya Gutierrez].[Brenda Blumberg].[Wayne Banack].[Megan Davis]}\n" +
                "Row #0: $29,762.88\n" +
                "Row #0: Brenda\n" +
                "Row #1: $27,908.33\n" +
                "Row #1: Wayne\n" +
                "Row #2: $981.82\n" +
                "Row #2: Samuel\n" +
                "Row #3: $927.27\n" +
                "Row #3: Megan\n"));

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

    /**
     * test case for bug #2477623, Closure Tables not working with Virtual Cubes
     */
    public void testClosureTableInVirtualCube() {
        TestContext testContext = TestContext.create(
            "<Dimension name=\"Employees\" >" +
            "   <Hierarchy hasAll=\"true\" allMemberName=\"All Employees\"" +
            "      primaryKey=\"employee_id\" primaryKeyTable=\"employee\">" +
            "      <Table name=\"employee\"/>" +
            "      <Level name=\"Employee Name\" type=\"String\" uniqueMembers=\"true\"" +
            "         column=\"employee_id\" parentColumn=\"supervisor_id\"" +
            "         nameColumn=\"full_name\" nullParentValue=\"0\">" +
            "         <Closure parentColumn=\"supervisor_id\" childColumn=\"employee_id\">" +
            "            <Table name=\"employee_closure\"/>" +
            "         </Closure>" +
            "      </Level>" +
            "   </Hierarchy>" +
            "</Dimension>"
            , null,
            "<Cube name=\"CustomSales\">" +
            "   <Table name=\"sales_fact_1997\"/>" +
            "   <DimensionUsage name=\"Employees\" source=\"Employees\" foreignKey=\"time_id\"/>" +
            "   <Measure name=\"Store Sales\" column=\"store_sales\" aggregator=\"sum\"/>" +
            "</Cube>" +
            "<Cube name=\"CustomHR\">" +
            "   <Table name=\"salary\"/>" +
            "   <DimensionUsage name=\"Employees\" source=\"Employees\" foreignKey=\"employee_id\"/>" +
            "   <Measure name=\"Org Salary\" column=\"salary_paid\" aggregator=\"sum\"/>" +
            "</Cube>" +
            "<VirtualCube name=\"CustomSalesAndHR\" >" +
            "<VirtualCubeDimension name=\"Employees\"/>" +
            "<VirtualCubeMeasure cubeName=\"CustomSales\" name=\"[Measures].[Store Sales]\"/>" +
            "<VirtualCubeMeasure cubeName=\"CustomHR\" name=\"[Measures].[Org Salary]\"/>" +
            "<CalculatedMember name=\"HR Cost per Sale\" dimension=\"Measures\">" +
            "<Formula>[Measures].[Store Sales] / [Measures].[Org Salary]</Formula>" +
            "</CalculatedMember>" +
            "</VirtualCube>",
            null, null, null);

        testContext.assertQueryReturns(
            "select " +
            "[Employees].[All Employees].[Sheri Nowmer].[Rebecca Kanagaki].Children" +
            " ON COLUMNS, " +
            "{[Measures].[Org Salary]} ON ROWS from [CustomSalesAndHR]",
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Employees].[All Employees].[Sheri Nowmer].[Rebecca Kanagaki].[Juanita Sharp]}\n" +
                "{[Employees].[All Employees].[Sheri Nowmer].[Rebecca Kanagaki].[Sandra Brunner]}\n" +
                "Axis #2:\n" +
                "{[Measures].[Org Salary]}\n" +
                "Row #0: 152.76\n" +
                "Row #0: 60\n"));
    }
}

// End ParentChildHierarchyTest.java
