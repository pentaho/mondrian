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
    public void testAll() {
        assertQueryReturns(
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
        assertQueryReturns(
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

    // bug 1063369: DISTINCT COUNT applied to a parent/child hierarchy fails:
    // unsupported when children expanded
     public void testDistinctAll() {
        // parent/child dimension not expanded, and the query works
        assertQueryReturns(
                "select {[Measures].[Count], [Measures].[Org Salary], " + nl +
                "[Measures].[Number Of Employees], [Measures].[Avg Salary]} on columns," + nl +
                "{[Employees]} on rows" + nl +
                "from [HR]",
                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Count]}" + nl +
                "{[Measures].[Org Salary]}" + nl +
                "{[Measures].[Number of Employees]}" + nl +
                "{[Measures].[Avg Salary]}" + nl +
                "Axis #2:" + nl +
                "{[Employees].[All Employees]}" + nl +
                "Row #0: 7,392" + nl +
                "Row #0: $39,431.67" + nl +
                "Row #0: 616" + nl +
                "Row #0: $64.01" + nl);
    }

    public void testDistinctChildrenOfAll() {
        // parent/child dimension expanded: fails with
        // java.lang.UnsupportedOperationException at
        // mondrian.rolap.RolapAggregator$6.aggregate(RolapAggregator.java:72)
        assertQueryReturns(
            "select {[Measures].[Count], [Measures].[Org Salary], " + nl +
            "[Measures].[Number Of Employees], [Measures].[Avg Salary]} on columns," + nl +
            "{[Employees].children} on rows" + nl +
            "from [HR]",
            "Axis #0:" + nl +
            "{}" + nl +
            "Axis #1:" + nl +
            "{[Measures].[Count]}" + nl +
            "{[Measures].[Org Salary]}" + nl +
            "{[Measures].[Number of Employees]}" + nl +
            "{[Measures].[Avg Salary]}" + nl +
            "Axis #2:" + nl +
            "{[Employees].[All Employees].[Sheri Nowmer]}" + nl +
            "Row #0: 7,392" + nl +
            "Row #0: $39,431.67" + nl +
            "Row #0: 616" + nl +
            "Row #0: $64.01" + nl);
    }

    // same two tests, but on a subtree:
    // disable test til bug fixed
    public void testDistinctSubtree() {
        // also fails with UnsupportedOperationException
        assertQueryReturns(
                "select {[Measures].[Count], [Measures].[Org Salary], " + nl +
                "[Measures].[Number Of Employees], [Measures].[Avg Salary]} on columns," + nl +
                "{[Employees].[All Employees].[Sheri Nowmer].[Rebecca Kanagaki]} on rows" + nl +
                "from [HR]",
                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Count]}" + nl +
                "{[Measures].[Org Salary]}" + nl +
                "{[Measures].[Number of Employees]}" + nl +
                "{[Measures].[Avg Salary]}" + nl +
                "Axis #2:" + nl +
                "{[Employees].[All Employees].[Sheri Nowmer].[Rebecca Kanagaki]}" + nl +
                "Row #0: 24" + nl +
                "Row #0: $234.36" + nl +
                "Row #0: 2" + nl +
                "Row #0: $117.18" + nl);
    }


    /**
     * Verifies that COUNT DISTINCT works against the explict closure of the
     * parent/child hierarchy. (Repeats the last 4 tests.)
     */
    // Disabled because I removed the [ExplicitClosure] hierarchy from the [HR]
    // cube. todo: Create a temporary cube and re-enable test.
    public void _testDistinctAllExplicitClosure() {
        assertQueryReturns(
                "select {[Measures].[Count], [Measures].[Org Salary], " + nl +
                "[Measures].[Number Of Employees], [Measures].[Avg Salary]} on columns," + nl +
                "{[EmployeesClosure]} on rows" + nl +
                "from [HR]",
                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Count]}" + nl +
                "{[Measures].[Org Salary]}" + nl +
                "{[Measures].[Number of Employees]}" + nl +
                "{[Measures].[Avg Salary]}" + nl +
                "Axis #2:" + nl +
                "{[EmployeesClosure].[All Employees]}" + nl +
                "Row #0: 7,392" + nl +
                "Row #0: $39,431.67" + nl +
                "Row #0: 616" + nl +
                "Row #0: $64.01" + nl);
    }

    // Disabled because I removed the [ExplicitClosure] hierarchy from the [HR]
    // cube. todo: Create a temporary cube and re-enable test.
    public void _testDistinctChildrenOfAllExplicitClosure() {
        // the children of the closed relation are all the descendants, so limit results
        assertQueryReturns(
                "select {[Measures].[Count], [Measures].[Org Salary], " + nl +
                "[Measures].[Number Of Employees], [Measures].[Avg Salary]} on columns," + nl +
                "{[EmployeesClosure].FirstChild} on rows" + nl +
                "from [HR]",
                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Count]}" + nl +
                "{[Measures].[Org Salary]}" + nl +
                "{[Measures].[Number of Employees]}" + nl +
                "{[Measures].[Avg Salary]}" + nl +
                "Axis #2:" + nl +
                "{[EmployeesClosure].[All Employees].[1]}" + nl +
                "Row #0: 7,392" + nl +
                "Row #0: $39,431.67" + nl +
                "Row #0: 616" + nl +
                "Row #0: $64.01" + nl);
    }

    // Disabled because I removed the [ExplicitClosure] hierarchy from the [HR]
    // cube. todo: Create a temporary cube and re-enable test.
    public void _testDistinctSubtreeExplicitClosure() {
        assertQueryReturns(
                "select {[Measures].[Count], [Measures].[Org Salary], " + nl +
                "[Measures].[Number Of Employees], [Measures].[Avg Salary]} on columns," + nl +
                "{[EmployeesClosure].[All Employees].[7]} on rows" + nl +
                "from [HR]",
                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Count]}" + nl +
                "{[Measures].[Org Salary]}" + nl +
                "{[Measures].[Number of Employees]}" + nl +
                "{[Measures].[Avg Salary]}" + nl +
                "Axis #2:" + nl +
                "{[EmployeesClosure].[All Employees].[7]}" + nl +
                "Row #0: 24" + nl +
                "Row #0: $234.36" + nl +
                "Row #0: 2" + nl +
                "Row #0: $117.18" + nl);
    }



    public void testLeaf() {
        // Juanita Sharp has no reports
        assertQueryReturns(
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
        assertQueryReturns(
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
        assertQueryReturns(
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
        assertQueryReturns(
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
        assertQueryReturns(
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

        TestContext testContext = TestContext.create(
            null,
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
                "</Cube>", null, null);

        // On a cube with fewer dimensions, this gave a false failure.
        testContext.assertQueryReturns(
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
        Result result = executeQuery("with member [Measures].[Foo] as " + nl +
                    "  '([Measures].[Foo], OpeningPeriod([Time].[Month]))'" + nl +
                    "select" + nl +
                    " {[Measures].[Unit Sales], [Measures].[Foo]} on Columns," + nl +
                    " { [Time].[1997].[Q2]} on rows" + nl +
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
            "select {[Measures].Members} ON columns," + nl +
                "  {[Employees].Members} ON rows" + nl +
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
        Result result = executeQuery("select {[Measures].Members} ON columns," + nl +
                    "  {[Employees].Members} ON rows" + nl +
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
                // Original "HR" cube has no ordinalColumn
                "          ordinalColumn=\"last_name\" >\n" +
                "        <Closure parentColumn=\"supervisor_id\" childColumn=\"employee_id\">\n" +
                "          <Table name=\"employee_closure\"/>\n" +
                "        </Closure>\n" +
                "      </Level>\n" +
                "    </Hierarchy>\n" +
                "  </Dimension>\n" +
                "\n" +
                "  <Measure name=\"Org Salary\" column=\"salary_paid\" aggregator=\"sum\"\n" +
                "      formatString=\"Currency\"/>\n" +
                "  <Measure name=\"Count\" column=\"employee_id\" aggregator=\"count\"\n" +
                "      formatString=\"#,#\"/>\n" +
                "</Cube>", null, null);

        // Make sure <Hierarchy>.MEMBERS is sorted.
        testContext.assertQueryReturns(
            "select {Tail(Head([Employees].Members, 15), 6)} on columns from [HR-ordered]",
            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Employees].[All Employees].[Margaret Adams]}\n" +
                "{[Employees].[All Employees].[Carla Adams]}\n" +
                "{[Employees].[All Employees].[Ronald Adina]}\n" +
                "{[Employees].[All Employees].[Samuel Agcaoili]}\n" +
                "{[Employees].[All Employees].[James Aguilar]}\n" +
                "{[Employees].[All Employees].[Robert Ahlering]}\n" +
                "Row #0: $2,077.18\n" +
                "Row #0: $80.92\n" +
                "Row #0: $107.16\n" +
                "Row #0: $981.82\n" +
                "Row #0: $403.64\n" +
                "Row #0: $40.47\n"));

        // Make sure <Member>.CHILDREN is sorted.
        testContext.assertQueryReturns(
            "select {[Employees].[Sheri Nowmer].[Rebecca Kanagaki].Children} on columns from [HR-ordered]",

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
}

// End ParentChildHierarchyTest.java
