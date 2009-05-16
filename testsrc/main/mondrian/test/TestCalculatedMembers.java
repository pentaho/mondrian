/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2002-2002 Kana Software, Inc.
// Copyright (C) 2002-2009 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 5 October, 2002
*/
package mondrian.test;

import junit.framework.Assert;
import junit.framework.AssertionFailedError;
import mondrian.olap.*;
import mondrian.rolap.BatchTestCase;
import mondrian.spi.Dialect;

/**
 * Tests the expressions used for calculated members. Please keep in sync
 * with the actual code used by the wizard.
 *
 * @author jhyde
 * @since 5 October, 2002
 * @version $Id$
 */
public class TestCalculatedMembers extends BatchTestCase {
    public TestCalculatedMembers() {
        super();
    }
    public TestCalculatedMembers(String name) {
        super(name);
    }

    public void testCalculatedMemberInCube() {
        assertExprReturns("[Measures].[Profit]", "$339,610.90");

        // Testcase for bug 829012.
        assertQueryReturns(
            "select {[Measures].[Avg Salary], [Measures].[Org Salary]} ON columns,\n"
            + "{([Time].[1997], [Store].[All Stores], [Employees].[All Employees])} ON rows\n"
            + "from [HR]\n"
            + "where [Pay Type].[Hourly]",
            "Axis #0:\n"
            + "{[Pay Type].[All Pay Types].[Hourly]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Avg Salary]}\n"
            + "{[Measures].[Org Salary]}\n"
            + "Axis #2:\n"
            + "{[Time].[1997], [Store].[All Stores], [Employees].[All Employees]}\n"
            + "Row #0: $40.31\n"
            + "Row #0: $11,406.75\n");
    }

    public void testCalculatedMemberInCubeViaApi() {
        Cube salesCube = getSalesCube("Sales");
        salesCube.createCalculatedMember(
            "<CalculatedMember name='Profit2'"
            + "  dimension='Measures'"
            + "  formula='[Measures].[Store Sales]-[Measures].[Store Cost]'/>");

        String s = executeExpr("[Measures].[Profit2]");
        Assert.assertEquals("339,610.90", s);

        // should fail if member of same name exists
        try {
            salesCube.createCalculatedMember(
                "<CalculatedMember name='Profit2'"
                + "  dimension='Measures'"
                + "  formula='[Measures].[Store Sales]-[Measures].[Store Cost]'/>");
            throw new AssertionFailedError("expected error, got none");
        } catch (RuntimeException e) {
            final String msg = e.getMessage();
            if (!msg.equals("Mondrian Error:Calculated member '[Measures].[Profit2]' already exists in cube 'Sales'")) {
                throw e;
            }
        }
    }

    /**
     * Tests a calculated member with spaces in its name against a virtual
     * cube with spaces in its name.
     */
    public void testCalculatedMemberInCubeWithSpace() {
        TestContext testContext = TestContext.createSubstitutingCube(
            "Warehouse and Sales",
            null,
            "<CalculatedMember name='Profit With Spaces'"
            + "  dimension='Measures'"
            + "  formula='[Measures].[Store Sales]-[Measures].[Store Cost]'/>");

        Cell s = testContext.executeExprRaw(
            "[Measures].[Profit With Spaces]", "Warehouse and Sales");
        Assert.assertEquals("339,610.90", s.getFormattedValue());
    }

    public void testCalculatedMemberInCubeWithProps() {
        Cube salesCube = getSalesCube("Sales");

        // member with a property
        salesCube.createCalculatedMember(
            "<CalculatedMember name='Profit3'"
            + "  dimension='Measures'"
            + "  formula='[Measures].[Store Sales]-[Measures].[Store Cost]'>"
            + "    <CalculatedMemberProperty name='FORMAT_STRING' value='#'/>"
            + "</CalculatedMember>");

        // note that result uses format string
        Result result = TestContext.instance().executeQuery(
            "select {[Measures].[Profit3]} on columns from Sales");
        String s = result.getCell(new int[]{0}).getFormattedValue();
        Assert.assertEquals("339611", s);

        // should fail if member property has expr and value
        try {
            salesCube.createCalculatedMember(
                "<CalculatedMember name='Profit4'"
                + "  dimension='Measures'"
                + "  formula='[Measures].[Store Sales]-[Measures].[Store Cost]'>"
                + "    <CalculatedMemberProperty name='FORMAT_STRING' />"
                + "</CalculatedMember>");
            throw new AssertionFailedError("expected error, got none");
        } catch (RuntimeException e) {
            final String msg = e.getMessage();
            if (!msg.equals("Mondrian Error:Member property must have a value or an expression. (Property 'FORMAT_STRING' of member 'Profit4' of cube 'Sales'.)")) {
                throw e;
            }
        }

        // should fail if member property both expr and value
        try {
            salesCube.createCalculatedMember(
                "<CalculatedMember name='Profit4'"
                + "  dimension='Measures'"
                + "  formula='[Measures].[Store Sales]-[Measures].[Store Cost]'>"
                + "    <CalculatedMemberProperty name='FORMAT_STRING' value='#' expression='\"#\"' />"
                + "</CalculatedMember>");
            throw new AssertionFailedError("expected error, got none");
        } catch (RuntimeException e) {
            final String msg = e.getMessage();
            if (!msg.equals("Mondrian Error:Member property must not have both a value and an expression. (Property 'FORMAT_STRING' of member 'Profit4' of cube 'Sales'.)")) {
                throw e;
            }
        }

        // should fail if member property's expression is invalid
        try {
            salesCube.createCalculatedMember(
                "<CalculatedMember name='Profit4'"
                + "  dimension='Measures'"
                + "  formula='[Measures].[Store Sales]-[Measures].[Store Cost]'>"
                + "    <CalculatedMemberProperty name='FORMAT_STRING' expression='1 + [FooBar]' />"
                + "</CalculatedMember>");
            throw new AssertionFailedError("expected error, got none");
        } catch (RuntimeException e) {
            final String msg = e.getMessage();
            if (!msg.equals("Mondrian Error:Named set in cube 'Sales' has bad formula")) {
                throw e;
            }
        }

        // should succeed if we switch the property to ignore invalid
        // members; the create will succeed and in the select, it will
        // return null for the member and therefore a 0 in the calculation
        MondrianProperties.instance().IgnoreInvalidMembers.set(true);
        salesCube.createCalculatedMember(
            "<CalculatedMember name='Profit4'"
            + "  dimension='Measures'"
            + "  formula='[Measures].[Store Sales]-[Measures].[Store Cost]+"
            + "     [Measures].[FooBar]'>"
            + "</CalculatedMember>");
        result = TestContext.instance().executeQuery(
            "select {[Measures].[Profit4]} on columns from Sales");
        s = result.getCell(new int[]{0}).getFormattedValue();
        Assert.assertEquals("339,610.90", s);
        MondrianProperties.instance().IgnoreInvalidMembers.set(false);
    }

    private Cube getSalesCube(String cubeName) {
        Cube[] cubes = getConnection().getSchema().getSchemaReader().getCubes();
        for (Cube cube : cubes) {
            if (cube.getName().equals(cubeName)) {
                return cube;
            }
        }
        return null;
    }

    public void testCalculatedMemberInCubeAndQuery() {
        // Profit is defined in the cube.
        // Profit Change is defined in the query.
        assertQueryReturns(
            "WITH MEMBER [Measures].[Profit Change]\n"
            + " AS '[Measures].[Profit] - ([Measures].[Profit], [Time].PrevMember)'\n"
            + "SELECT {[Measures].[Profit], [Measures].[Profit Change]} ON COLUMNS,\n"
            + " {[Time].[1997].[Q2].children} ON ROWS\n"
            + "FROM [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Profit]}\n"
            + "{[Measures].[Profit Change]}\n"
            + "Axis #2:\n"
            + "{[Time].[1997].[Q2].[4]}\n"
            + "{[Time].[1997].[Q2].[5]}\n"
            + "{[Time].[1997].[Q2].[6]}\n"
            + "Row #0: $25,766.55\n"
            + "Row #0: $-4,289.24\n"
            + "Row #1: $26,673.73\n"
            + "Row #1: $907.18\n"
            + "Row #2: $27,261.76\n"
            + "Row #2: $588.03\n");
    }

    public void testQueryCalculatedMemberOverridesCube() {
        // Profit is defined in the cube, and has a format string "$#,###".
        // We define it in a query to make sure that the format string in the
        // cube doesn't change.
        assertQueryReturns(
            "WITH MEMBER [Measures].[Profit]\n"
            + " AS '(Measures.[Store Sales] - Measures.[Store Cost])', FORMAT_STRING='#,###' \n"
            + "SELECT {[Measures].[Profit]} ON COLUMNS,\n"
            + " {[Time].[1997].[Q2]} ON ROWS\n"
            + "FROM [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Profit]}\n"
            + "Axis #2:\n"
            + "{[Time].[1997].[Q2]}\n"
            + "Row #0: 79,702\n");

        // Note that the Profit measure defined against the cube has
        // a format string preceded by "$".
        assertQueryReturns(
            "SELECT {[Measures].[Profit]} ON COLUMNS,\n"
            + " {[Time].[1997].[Q2]} ON ROWS\n"
            + "FROM [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Profit]}\n"
            + "Axis #2:\n"
            + "{[Time].[1997].[Q2]}\n"
            + "Row #0: $79,702.05\n");
    }

    public void _testWhole() {
        /*
         * "allmembers" tests compatibility with MSAS
         */

        executeQuery(
            "with\n"
            + "member [Measures].[Total Store Sales by Product Name] as\n"
            + "  'Sum([Product].[Product Name].members, [Measures].[Store Sales])'\n"
            + "\n"
            + "member [Measures].[Average Store Sales by Product Name] as\n"
            + "  'Avg([Product].[Product Name].allmembers, [Measures].[Store Sales])'\n"
            + "\n"
            + "member [Measures].[Number of Product Name members] as\n"
            + "  'Count([Product].[Product Name].members)'\n"
            + "\n"
            + "member [Measures].[Standard Deviation of Store Sales for Product Name] as\n"
            + "  'Stddev([Product].[Product Name].members, [Measures].[Store Sales])'\n"
            + "\n"
            + "member [Measures].[Variance between Store Sales and Store Cost] as\n"
            + "  '[Measures].[Store Sales] - [Measures].[Store Cost]'\n"
            + "\n"
            + "member [Measures].[% Variance between Store Sales and Store Cost] as\n"
            + "  'iif([Measures].[Store Cost] = 0, 1, [Measures].[Store Sales] / [Measures].[Store Cost])'\n"
            + ", format_string='Percent'\n"
            + "\n"
            + "member [Measures].[% Difference between Store Sales and Store Cost] as\n"
            + "  'iif([Measures].[Store Sales] = 0, -1, ([Measures].[Store Sales] - [Measures].[Store Cost]) / [Measures].[Store Sales])'\n"
            + ", format_string='Percent'\n"
            + "\n"
            + "member [Measures].[% Markup between Store Sales and Store Cost] as\n"
            + "  'iif([Measures].[Store Cost] = 0, 1, ([Measures].[Store Sales] - [Measures].[Store Cost]) / [Measures].[Store Cost])'\n"
            + ", format_string='Percent'\n"
            + "\n"
            + "member [Measures].[Growth of Store Sales since previous period] as\n"
            + "  '[Measures].[Store Sales] - ([Measures].[Store Sales], ParallelPeriod([Time].CurrentMember.level, 1))'\n"
            + "\n"
            + "member [Measures].[% Growth of Store Sales since previous period] as\n"
            + "  'iif(([Measures].[Store Sales], ParallelPeriod([Time].CurrentMember.level, 1)) = 0, 1, ([Measures].[Store Sales] - ([Measures].[Store Sales], ParallelPeriod([Time].CurrentMember.level, 1))) / ([Measures].[Store Sales], ParallelPeriod([Time].CurrentMember.level, 1)))'\n"
            + ", format_string='Percent'\n"
            + "\n"
            + "member [Measures].[Growth of Store Sales since previous year] as\n"
            + "  '[Measures].[Store Sales] - ([Measures].[Store Sales], ParallelPeriod([Time].[Year], 1))'\n"
            + "\n"
            + "member [Measures].[% Growth of Store Sales since previous year] as\n"
            + "  'iif(([Measures].[Store Sales], ParallelPeriod([Time].[Year], 1)) = 0, 1, ([Measures].[Store Sales] - ([Measures].[Store Sales], ParallelPeriod([Time].[Year], 1))) / ([Measures].[Store Sales], ParallelPeriod([Time].[Year], 1)))'\n"
            + ", format_string='Percent'\n"
            + "\n"
            + "member [Measures].[Store Sales as % of parent Store] as\n"
            + "  'iif(([Measures].[Store Sales], [Store].CurrentMember.Parent) = 0, 1, [Measures].[Store Sales] / ([Measures].[Store Sales], [Store].CurrentMember.Parent))'\n"
            + ", format_string='Percent'\n"
            + "\n"
            + "member [Measures].[Store Sales as % of all Store] as\n"
            + "  'iif(([Measures].[Store Sales], [Store].Members.Item(0)) = 0, 1, [Measures].[Store Sales] / ([Measures].[Store Sales], [Store].Members.Item(0)))'\n"
            + ", format_string='Percent'\n"
            + "\n"
            + "member [Measures].[Total Store Sales, period to date] as\n"
            + " 'sum(PeriodsToDate([Time].CurrentMember.Parent.Level), [Measures].[Store Sales])'\n"
            + "\n"
            + "member [Measures].[Total Store Sales, Quarter to date] as\n"
            + " 'sum(PeriodsToDate([Time].[Quarter]), [Measures].[Store Sales])'\n"
            + "\n"
            + "member [Measures].[Average Store Sales, period to date] as\n"
            + " 'avg(PeriodsToDate([Time].CurrentMember.Parent.Level), [Measures].[Store Sales])'\n"
            + "\n"
            + "member [Measures].[Average Store Sales, Quarter to date] as\n"
            + " 'avg(PeriodsToDate([Time].[Quarter]), [Measures].[Store Sales])'\n"
            + "\n"
            + "member [Measures].[Rolling Total of Store Sales over previous 3 periods] as\n"
            + " 'sum([Time].CurrentMember.Lag(2) : [Time].CurrentMember, [Measures].[Store Sales])'\n"
            + "\n"
            + "member [Measures].[Rolling Average of Store Sales over previous 3 periods] as\n"
            + " 'avg([Time].CurrentMember.Lag(2) : [Time].CurrentMember, [Measures].[Store Sales])'\n"
            + "\n"
            + "select\n"
            + " CrossJoin(\n"
            + "  {[Time].[1997], [Time].[1997].[Q2]},\n"
            + "  {[Store].[All Stores], \n"
            + "   [Store].[USA],\n"
            + "   [Store].[USA].[CA],\n"
            + "   [Store].[USA].[CA].[San Francisco]}) on columns,\n"
            + " AddCalculatedMembers([Measures].members) on rows\n"
            + " from Sales");

        // Repeat time-related measures with more time members.
        executeQuery(
            "with\n"
            + "member [Measures].[Total Store Sales, Quarter to date] as\n"
            + " 'sum(PeriodsToDate([Time].[Quarter]), [Measures].[Store Sales])'\n"
            + "\n"
            + "member [Measures].[Average Store Sales, period to date] as\n"
            + " 'avg(PeriodsToDate([Time].CurrentMember.Parent.Level), [Measures].[Store Sales])'\n"
            + "\n"
            + "member [Measures].[Average Store Sales, Quarter to date] as\n"
            + " 'avg(PeriodsToDate([Time].[Quarter]), [Measures].[Store Sales])'\n"
            + "\n"
            + "member [Measures].[Rolling Total of Store Sales over previous 3 periods] as\n"
            + " 'sum([Time].CurrentMember.Lag(2) : [Time].CurrentMember, [Measures].[Store Sales])'\n"
            + "\n"
            + "member [Measures].[Rolling Average of Store Sales over previous 3 periods] as\n"
            + " 'avg([Time].CurrentMember.Lag(2) : [Time].CurrentMember, [Measures].[Store Sales])'\n"
            + "\n"
            + "select\n"
            + " CrossJoin(\n"
            + "  {[Store].[USA].[CA],\n"
            + "   [Store].[USA].[CA].[San Francisco]},\n"
            + "  [Time].[Month].members) on columns,\n"
            + " AddCalculatedMembers([Measures].members) on rows\n"
            + " from Sales");
    }

    public void testCalculatedMemberCaption() {
        String mdx = "select {[Measures].[Profit Growth]} on columns from Sales";
        Result result = TestContext.instance().executeQuery(mdx);
        Axis axis0 = result.getAxes()[0];
        Position pos0 = axis0.getPositions().get(0);
        Member profGrowth = pos0.get(0);
        String caption = profGrowth.getCaption();
        Assert.assertEquals(caption, "Gewinn-Wachstum");
    }

    public void testCalcMemberIsSetFails() {
        // A member which is a set, and more important, cannot be converted to
        // a value, is an error.
        String queryString =
            "with member [Measures].[Foo] as ' Filter([Product].members, 1 <> 0) '"
            + "select {[Measures].[Foo]} on columns from [Sales]";
        String pattern = "Member expression 'Filter([Product].Members, (1.0 <> 0.0))' must not be a set";
        assertQueryThrows(queryString, pattern);

        // A tuple is OK, because it can be converted to a scalar expression.
        queryString =
            "with member [Measures].[Foo] as ' ([Measures].[Unit Sales], [Gender].[F]) '"
            + "select {[Measures].[Foo]} on columns from [Sales]";
        final Result result = executeQuery(queryString);
        Util.discard(result);

        // Level cannot be converted.
        assertExprThrows(
            "[Customers].[Country]",
            "Member expression '[Customers].[Country]' must not be a set");

        // Dimension can be converted.
        assertExprReturns("[Customers]", "266,773");

        // Member can be converted.
        assertExprReturns("[Customers].[USA]", "266,773");

        // Tuple can be converted.
        assertExprReturns(
            "([Customers].[USA], [Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer])",
            "1,683");

        // Set of tuples cannot be converted.
        assertExprThrows(
            "{([Customers].[USA], [Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer])}",
            "Member expression '{([Customers].[All Customers].[USA], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer])}' must not be a set");
        assertExprThrows(
            "{([Customers].[USA], [Product].[Food]),"
            + "([Customers].[USA], [Product].[Drink])}",
            "{([Customers].[All Customers].[USA], [Product].[All Products].[Food]), ([Customers].[All Customers].[USA], [Product].[All Products].[Drink])}' must not be a set");

        // Sets cannot be converted.
        assertExprThrows(
            "{[Product].[Food]}",
            "Member expression '{[Product].[All Products].[Food]}' must not be a set");
    }

    /**
     * Tests that calculated members can have brackets in their names.
     * (Bug 1251683.)
     */
    public void testBracketInCalcMemberName() {
        assertQueryReturns(
            "with member [Measures].[has a [bracket]] in it] as \n"
            + "' [Measures].CurrentMember.Name '\n"
            + "select {[Measures].[has a [bracket]] in it]} on columns\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[has a [bracket]] in it]}\n"
            + "Row #0: Unit Sales\n");
    }

    /**
     * Tests that IIf works OK even if its argument returns the NULL
     * value. (Bug 1418689.)
     */
    public void testNpeInIif() {
        assertQueryReturns(
            "WITH MEMBER [Measures].[Foo] AS ' 1 / [Measures].[Unit Sales] ',\n"
            + "  FORMAT_STRING=IIf([Measures].[Foo] < .3, \"|0.0|style=red\",\"0.0\")\n"
            + "SELECT {[Store].[USA].[WA].children} on columns\n"
            + "FROM Sales\n"
            + "WHERE ([Time].[1997].[Q4].[12],\n"
            + " [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth].[Portsmouth Imported Beer],\n"
            + " [Measures].[Foo])",
            "Axis #0:\n"
            + "{[Time].[1997].[Q4].[12], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth].[Portsmouth Imported Beer], [Measures].[Foo]}\n"
            + "Axis #1:\n"
            + "{[Store].[All Stores].[USA].[WA].[Bellingham]}\n"
            + "{[Store].[All Stores].[USA].[WA].[Bremerton]}\n"
            + "{[Store].[All Stores].[USA].[WA].[Seattle]}\n"
            + "{[Store].[All Stores].[USA].[WA].[Spokane]}\n"
            + "{[Store].[All Stores].[USA].[WA].[Tacoma]}\n"
            + "{[Store].[All Stores].[USA].[WA].[Walla Walla]}\n"
            + "{[Store].[All Stores].[USA].[WA].[Yakima]}\n"
            + "Row #0: Infinity\n"
            + "Row #0: Infinity\n"
            + "Row #0: 0.5\n"
            + "Row #0: Infinity\n"
            + "Row #0: |0.1|style=red\n"
            + "Row #0: Infinity\n"
            + "Row #0: |0.3|style=red\n");
    }

    /**
     * Tests that calculated members defined in the schema can have brackets in
     * their names. (Bug 1251683.)
     */
    public void testBracketInCubeCalcMemberName() {
        final String cubeName = "Sales_BracketInCubeCalcMemberName";
        String s =
            "<Cube name=\"" + cubeName + "\">\n"
            + "  <Table name=\"sales_fact_1997\"/>\n"
            + "  <Dimension name=\"Gender\" foreignKey=\"customer_id\">\n"
            + "    <Hierarchy hasAll=\"false\" primaryKey=\"customer_id\">\n"
            + "    <Table name=\"customer\"/>\n"
            + "      <Level name=\"Gender\" column=\"gender\" uniqueMembers=\"true\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\"\n"
            + "      formatString=\"Standard\" visible=\"false\"/>\n"
            + "  <CalculatedMember\n"
            + "      name=\"With a [bracket] inside it\"\n"
            + "      dimension=\"Measures\"\n"
            + "      visible=\"false\"\n"
            + "      formula=\"[Measures].[Unit Sales] * 10\">\n"
            + "    <CalculatedMemberProperty name=\"FORMAT_STRING\" value=\"$#,##0.00\"/>\n"
            + "  </CalculatedMember>\n"
            + "</Cube>";

        TestContext testContext = TestContext.create(
            null, s, null, null, null, null);
        testContext.assertQueryThrows(
            "select {[Measures].[With a [bracket] inside it]} on columns,\n"
            + " {[Gender].Members} on rows\n"
            + "from ["
            + cubeName
            + "]",
                "Syntax error at line 1, column 38, token 'inside'");

        testContext.assertQueryReturns(
            "select {[Measures].[With a [bracket]] inside it]} on columns,\n"
            + " {[Gender].Members} on rows\n"
            + "from ["
            + cubeName
            + "]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[With a [bracket]] inside it]}\n"
            + "Axis #2:\n"
            + "{[Gender].[F]}\n"
            + "{[Gender].[M]}\n"
            + "Row #0: $1,315,580.00\n"
            + "Row #1: $1,352,150.00\n");
    }

    public void testPropertyReferencesCalcMember() {
        assertQueryReturns(
            "with member [Measures].[Foo] as ' [Measures].[Unit Sales] * 2 ',"
            + " FORMAT_STRING=IIf([Measures].[Foo] < 600000, \"|#,##0|style=red\",\"#,##0\")  "
            + "select {[Measures].[Foo]} on columns "
            + "from [Sales]",

            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Foo]}\n"
            + "Row #0: |533,546|style=red\n");
    }

    public void testCalcMemberWithQuote() {
        // single-quote inside double-quoted string literal
        // MSAS does not allow this
        assertQueryThrows(
            "with member [Measures].[Foo] as ' \"quoted string with 'apostrophe' in it\" ' "
            + "select {[Measures].[Foo]} on columns "
            + "from [Sales]",
            "Syntax error at line 1, column 57, token 'apostrophe'");

        // Escaped single quote in double-quoted string literal inside
        // single-quoted member declaration.
        assertQueryReturns(
            "with member [Measures].[Foo] as ' \"quoted string with ''apostrophe'' in it\" ' "
            + "select {[Measures].[Foo]} on columns "
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Foo]}\n"
            + "Row #0: quoted string with 'apostrophe' in it\n");

        // escaped double-quote inside double-quoted string literal
        assertQueryReturns(
            "with member [Measures].[Foo] as ' \"quoted string with \"\"double-quote\"\" in it\" ' "
            + "select {[Measures].[Foo]} on columns "
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Foo]}\n"
            + "Row #0: quoted string with \"double-quote\" in it\n");

        // escaped double-quote inside double-quoted string literal
        assertQueryReturns(
            "with member [Measures].[Foo] as \"quoted string with 'apos' in it\" "
            + "select {[Measures].[Foo]} on columns "
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Foo]}\n"
            + "Row #0: quoted string with 'apos' in it\n");

        // Double-escaped single-quote
        // inside escaped single-quoted string literal
        // inside single-quoted formula.
        // MSAS does not allow this, but I think it should.
        assertQueryReturns(
            "with member [Measures].[Foo] as ' ''quoted string and ''''apos''''.'' ' "
            + "select {[Measures].[Foo]} on columns "
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Foo]}\n"
            + "Row #0: quoted string and 'apos'.\n");

        // Escaped single-quote
        // inside double-quoted string literal
        // inside single-quoted formula.
        assertQueryReturns(
            "with member [Measures].[Foo] as ' \"quoted string and ''apos''.\" ' "
            + "select {[Measures].[Foo]} on columns "
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Foo]}\n"
            + "Row #0: quoted string and 'apos'.\n");

        // single quote in format expression
        assertQueryReturns(
            "with member [Measures].[Colored Profit] as  ' [Measures].[Store Sales] - [Measures].[Store Cost] ', "
            + "  FORMAT_STRING = Iif([Measures].[Colored Profit] < 0, '|($#,##0.00)|style=red', '|$#,##0.00|style=green') "
            + "select {[Measures].[Colored Profit]} on columns,"
            + " {[Product].Children} on rows "
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Colored Profit]}\n"
            + "Axis #2:\n"
            + "{[Product].[All Products].[Drink]}\n"
            + "{[Product].[All Products].[Food]}\n"
            + "{[Product].[All Products].[Non-Consumable]}\n"
            + "Row #0: |$29,358.98|style=green\n"
            + "Row #1: |$245,764.87|style=green\n"
            + "Row #2: |$64,487.05|style=green\n");

        // double quote in format expression
        assertQueryReturns(
            "with member [Measures].[Colored Profit] as  ' [Measures].[Store Sales] - [Measures].[Store Cost] ', "
            + "  FORMAT_STRING = Iif([Measures].[Colored Profit] < 0, \"|($#,##0.00)|style=red\", \"|$#,##0.00|style=green\") "
            + "select {[Measures].[Colored Profit]} on columns,"
            + " {[Product].Children} on rows "
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Colored Profit]}\n"
            + "Axis #2:\n"
            + "{[Product].[All Products].[Drink]}\n"
            + "{[Product].[All Products].[Food]}\n"
            + "{[Product].[All Products].[Non-Consumable]}\n"
            + "Row #0: |$29,358.98|style=green\n"
            + "Row #1: |$245,764.87|style=green\n"
            + "Row #2: |$64,487.05|style=green\n");
    }

    /**
     * Testcase for bug <a href="http://jira.pentaho.com/browse/MONDRIAN-137">
     * MONDRIAN-137, "error if calc member in schema file contains single quotes"</a>.
     */
    public void testQuoteInCalcMember() {
        final String cubeName = "Sales_Bug1410383";
        String s =
                "<Cube name=\"" + cubeName + "\">\n"
                + "  <Table name=\"sales_fact_1997\"/>\n"
                + "  <Dimension name=\"Gender\" foreignKey=\"customer_id\">\n"
                + "    <Hierarchy hasAll=\"false\" primaryKey=\"customer_id\">\n"
                + "    <Table name=\"customer\"/>\n"
                + "      <Level name=\"Gender\" column=\"gender\" uniqueMembers=\"true\"/>\n"
                + "    </Hierarchy>\n"
                + "  </Dimension>\n"
                + "  <Measure name=\"Store Sales\" column=\"store_sales\" aggregator=\"sum\"\n"
                + "      formatString=\"Standard\" visible=\"false\"/>\n"
                + "  <Measure name=\"Store Cost\" column=\"store_cost\" aggregator=\"sum\"\n"
                + "      formatString=\"Standard\" visible=\"false\"/>\n"
                + "  <CalculatedMember\n"
                + "      name=\"Apos in dq\"\n"
                + "      dimension=\"Measures\"\n"
                + "      visible=\"false\"\n"
                + "      formula=\" &quot;an 'apos' in dq&quot; \" />\n"
                + "  <CalculatedMember\n"
                + "      name=\"Dq in dq\"\n"
                + "      dimension=\"Measures\"\n"
                + "      visible=\"false\"\n"
                + "      formula=\" &quot;a &quot;&quot;dq&quot;&quot; in dq&quot; \" />\n"
                + "  <CalculatedMember\n"
                + "      name=\"Apos in apos\"\n"
                + "      dimension=\"Measures\"\n"
                + "      visible=\"false\"\n"
                + "      formula=\" &apos;an &apos;&apos;apos&apos;&apos; in apos&apos; \" />\n"
                + "  <CalculatedMember\n"
                + "      name=\"Dq in apos\"\n"
                + "      dimension=\"Measures\"\n"
                + "      visible=\"false\"\n"
                + "      formula=\" &apos;a &quot;dq&quot; in apos&apos; \" />\n"
                + "  <CalculatedMember\n"
                + "      name=\"Colored Profit\"\n"
                + "      dimension=\"Measures\"\n"
                + "      visible=\"false\"\n"
                + "      formula=\" [Measures].[Store Sales] - [Measures].[Store Cost] \">\n"
                + "    <CalculatedMemberProperty name=\"FORMAT_STRING\" expression=\"Iif([Measures].[Colored Profit] &lt; 0, '|($#,##0.00)|style=red', '|$#,##0.00|style=green')\"/>\n"
                + "  </CalculatedMember>\n"
                + "</Cube>";

        final TestContext testContext = TestContext.create(
            null, s, null, null, null, null);
        testContext.assertQueryReturns(
            "select {[Measures].[Apos in dq], [Measures].[Dq in dq], [Measures].[Apos in apos], [Measures].[Dq in apos], [Measures].[Colored Profit]} on columns,\n"
            + " {[Gender].Members} on rows\n"
            + "from [" + cubeName + "]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Apos in dq]}\n"
            + "{[Measures].[Dq in dq]}\n"
            + "{[Measures].[Apos in apos]}\n"
            + "{[Measures].[Dq in apos]}\n"
            + "{[Measures].[Colored Profit]}\n"
            + "Axis #2:\n"
            + "{[Gender].[F]}\n"
            + "{[Gender].[M]}\n"
            + "Row #0: an 'apos' in dq\n"
            + "Row #0: a \"dq\" in dq\n"
            + "Row #0: an 'apos' in apos\n"
            + "Row #0: a \"dq\" in apos\n"
            + "Row #0: |$168,448.73|style=green\n"
            + "Row #1: an 'apos' in dq\n"
            + "Row #1: a \"dq\" in dq\n"
            + "Row #1: an 'apos' in apos\n"
            + "Row #1: a \"dq\" in apos\n"
            + "Row #1: |$171,162.17|style=green\n");
    }

    public void testChildrenOfCalcMembers() {
        assertQueryReturns(
            "with member [Time].[# Months Product Sold] as 'Count(Descendants([Time].LastSibling, [Time].[Month]), EXCLUDEEMPTY)'\n"
            + "select Crossjoin([Time].[# Months Product Sold].Children,\n"
            + "     [Store].[All Stores].Children) ON COLUMNS,\n"
            + "   [Product].[All Products].Children ON ROWS from [Sales] where [Measures].[Unit Sales]",
            "Axis #0:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #1:\n"
            + "Axis #2:\n"
            + "{[Product].[All Products].[Drink]}\n"
            + "{[Product].[All Products].[Food]}\n"
            + "{[Product].[All Products].[Non-Consumable]}\n");
    }

    public void testNonCharacterMembers() {
        assertQueryReturns(
            "with member [Has Coffee Bar].[Maybe] as \n"
            + "'SUM([Has Coffee Bar].members)' \n"
            + "SELECT {[Has Coffee Bar].[Maybe]} on rows, \n"
            + "{[Measures].[Store Sqft]} on columns \n"
            + "FROM [Store]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Sqft]}\n"
            + "Axis #2:\n"
            + "{[Has coffee bar].[Maybe]}\n"
            + "Row #0: 1,143,192\n");
     }

    public void testFormatString() {
        // Verify that
        // (a) a calculated member without a format string does not
        //     override the format string of a base measure
        //     ([Highly Profitable States] does not override [Store Sales])
        // and
        // (b) a calculated member with a format string does
        //     override the format string of a base measure
        //     ([Plain States] does override [Store Sales])
        // and
        // (c) the format string for conflicting calculated members
        //     is chosen according to solve order
        //     ([Plain States] overrides [Profit])
        assertQueryReturns(
            "WITH MEMBER [Customers].[Highly Profitable States]\n"
            + "AS 'SUM(FILTER([Customers].[USA].children,([Measures].[Profit] > 90000)))'\n"
            + "MEMBER [Customers].[Plain States]\n"
            + " AS 'SUM([Customers].[USA].children)', SOLVE_ORDER = 5, FORMAT_STRING='#,###'\n"
            + "SELECT {[Measures].[Store Sales], [Measures].[Profit]} ON COLUMNS,\n"
            + "UNION([Customers].[USA].children,{[Customers].[Highly Profitable States],[Customers].[Plain States]}) ON ROWS\n"
            + "FROM [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Sales]}\n"
            + "{[Measures].[Profit]}\n"
            + "Axis #2:\n"
            + "{[Customers].[All Customers].[USA].[CA]}\n"
            + "{[Customers].[All Customers].[USA].[OR]}\n"
            + "{[Customers].[All Customers].[USA].[WA]}\n"
            + "{[Customers].[Highly Profitable States]}\n"
            + "{[Customers].[Plain States]}\n"
            + "Row #0: 159,167.84\n"
            + "Row #0: $95,637.41\n"
            + "Row #1: 142,277.07\n"
            + "Row #1: $85,504.57\n"
            + "Row #2: 263,793.22\n"
            + "Row #2: $158,468.91\n"
            + "Row #3: 422,961.06\n"
            + "Row #3: $254,106.33\n"
            + "Row #4: 565,238\n"
            + "Row #4: 339,611\n");
    }

    /**
     * Testcase for <a href="http://jira.pentaho.com/browse/MONDRIAN-263">
     * bug MONDRIAN-263, Negative Solve Orders broken</a>.
     */
    public void testNegativeSolveOrder() {
        // Negative solve orders are OK.
        assertQueryReturns(
            "with member measures.blah as 'measures.[unit sales]', SOLVE_ORDER = -6 select {measures.[unit sales], measures.blah} on 0 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[blah]}\n"
            + "Row #0: 266,773\n"
            + "Row #0: 266,773\n");

        // Member with a negative solve order is trumped by a stored measure
        // (which has solve order 0), which in turn is trumped by a calc member
        // with a positive solve order.
        assertQueryReturns(
            "with member [Product].[Foo] as ' 1 ', SOLVE_ORDER = -6\n"
            + " member [Gender].[Bar] as ' 2 ', SOLVE_ORDER = 3\n"
            + "select {[Measures].[Unit Sales]} on 0,\n"
            + " {[Product].[Foo], [Product].[Drink]} *\n"
            + " {[Gender].[M], [Gender].[Bar]} on 1\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Foo], [Gender].[All Gender].[M]}\n"
            + "{[Product].[Foo], [Gender].[Bar]}\n"
            + "{[Product].[All Products].[Drink], [Gender].[All Gender].[M]}\n"
            + "{[Product].[All Products].[Drink], [Gender].[Bar]}\n"
            + "Row #0: 1\n"
            + "Row #1: 2\n"
            + "Row #2: 12,395\n"
            + "Row #3: 2\n");
    }

    public void testCalcMemberCustomFormatterInQuery() {
        // calc measure defined in query
        assertQueryReturns(
            "with member [Measures].[Foo] as ' [Measures].[Unit Sales] * 2 ',\n"
                + " CELL_FORMATTER='mondrian.test.FooBarCellFormatter' \n"
                + "select {[Measures].[Unit Sales], [Measures].[Foo]} on 0,\n"
                + " {[Store].Children} on rows\n"
                + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Foo]}\n"
            + "Axis #2:\n"
            + "{[Store].[All Stores].[Canada]}\n"
            + "{[Store].[All Stores].[Mexico]}\n"
            + "{[Store].[All Stores].[USA]}\n"
            + "Row #0: \n"
            + "Row #0: foo1.2345E-8bar\n"
            + "Row #1: \n"
            + "Row #1: foo1.2345E-8bar\n"
            + "Row #2: 266,773\n"
            + "Row #2: foo533546.0bar\n");
    }

    public void testCalcMemberCustomFormatterInQueryNegative() {
        assertQueryThrows(
            "with member [Measures].[Foo] as ' [Measures].[Unit Sales] * 2 ',\n"
            + " CELL_FORMATTER='mondrian.test.NonExistentCellFormatter' \n"
            + "select {[Measures].[Unit Sales], [Measures].[Foo]} on 0,\n"
            + " {[Store].Children} on rows\n"
            + "from [Sales]",
            "Failed to load formatter class 'mondrian.test.NonExistentCellFormatter' for member '[Measures].[Foo]'.");
    }

    public void testCalcMemberCustomFormatterInQueryNegative2() {
        String query = "with member [Measures].[Foo] as ' [Measures].[Unit Sales] * 2 ',\n"
            + " CELL_FORMATTER='java.lang.String' \n"
            + "select {[Measures].[Unit Sales], [Measures].[Foo]} on 0,\n"
            + " {[Store].Children} on rows\n"
            + "from [Sales]";
        assertQueryThrows(
            query,
            "Failed to load formatter class 'java.lang.String' for member '[Measures].[Foo]'.");
        assertQueryThrows(
            query,
            "java.lang.ClassCastException: java.lang.String");
    }

    public void testCalcMemberCustomFormatterInNonMeasureInQuery() {

        // CELL_FORMATTER is ignored for calc members which are not measures.
        //
        // We could change this behavior if it makes sense. In fact, we would
        // allow ALL properties to be inherited from the member with the
        // highest solve order. Need to check whether this is consistent with
        // the MDX spec. -- jhyde, 2007/9/5.
        assertQueryReturns(
            "with member [Store].[CA or OR] as ' Aggregate({[Store].[USA].[CA], [Store].[USA].[OR]}) ',\n"
                + " CELL_FORMATTER='mondrian.test.FooBarCellFormatter'\n"
                + "select {[Store].[USA], [Store].[CA or OR]} on columns\n"
                + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[All Stores].[USA]}\n"
            + "{[Store].[CA or OR]}\n"
            + "Row #0: 266,773\n"
            + "Row #0: 142,407\n");
    }

    public void testCalcMemberCustomFormatterInSchema() {
        // calc member defined in schema
        String cubeName = "Sales";
        TestContext testContext = TestContext.createSubstitutingCube(
            cubeName,
            null,
            "<CalculatedMember\n"
            + "    name=\"Profit Formatted\"\n"
            + "    dimension=\"Measures\"\n"
            + "    visible=\"false\"\n"
            + "    formula=\"[Measures].[Store Sales]-[Measures].[Store Cost]\">\n"
            + "  <CalculatedMemberProperty name=\"FORMAT_STRING\" value=\"$#,##0.00\"/>\n"
            + "  <CalculatedMemberProperty name=\"CELL_FORMATTER\" value=\"mondrian.test.FooBarCellFormatter\"/>\n"
            + "</CalculatedMember>\n");
        testContext.assertQueryReturns(
            "select {[Measures].[Unit Sales], [Measures].[Profit Formatted]} on 0,\n"
            + " {[Store].Children} on rows\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Profit Formatted]}\n"
            + "Axis #2:\n"
            + "{[Store].[All Stores].[Canada]}\n"
            + "{[Store].[All Stores].[Mexico]}\n"
            + "{[Store].[All Stores].[USA]}\n"
            + "Row #0: \n"
            + "Row #0: foo1.2345E-8bar\n"
            + "Row #1: \n"
            + "Row #1: foo1.2345E-8bar\n"
            + "Row #2: 266,773\n"
            + "Row #2: foo339610.89639999997bar\n");
    }

    public void testCalcMemberCustomFormatterInSchemaNegative() {
        // calc member defined in schema
        String cubeName = "Sales";
        TestContext testContext =
            TestContext.createSubstitutingCube(
                cubeName,
                null,
                "  <CalculatedMember\n"
                + "    name=\"Profit Formatted\"\n"
                + "    dimension=\"Measures\"\n"
                + "    visible=\"false\"\n"
                + "    formula=\"[Measures].[Store Sales]-[Measures].[Store Cost]\">\n"
                + "  <CalculatedMemberProperty name=\"FORMAT_STRING\" value=\"$#,##0.00\"/>\n"
                + "  <CalculatedMemberProperty name=\"CELL_FORMATTER\" value=\"mondrian.test.NonExistentCellFormatter\"/>\n"
                + "</CalculatedMember>\n");
        testContext.assertQueryThrows(
            "select {[Measures].[Unit Sales], [Measures].[Profit Formatted]} on 0,\n"
                + " {[Store].Children} on rows\n"
                + "from [Sales]",
            "Failed to load formatter class 'mondrian.test.NonExistentCellFormatter' for member '[Measures].[Profit Formatted]'.");
    }

    /**
     * Testcase for bug 1784617, "Using StrToTuple() in schema errors out".
     */
    public void testStrToSetInCubeCalcMember() {

        // calc member defined in schema
        String cubeName = "Sales";
        TestContext testContext =
            TestContext.createSubstitutingCube(
                cubeName,
                null,
                "<CalculatedMember\n"
                + "    name=\"My Tuple\"\n"
                + "    dimension=\"Measures\"\n"
                + "    visible=\"false\"\n"
                + "    formula=\"StrToTuple('([Gender].[M], [Marital Status].[S])', [Gender], [Marital Status])\"/>\n");
        String desiredResult =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[My Tuple]}\n"
            + "Row #0: 68,755\n";
        testContext.assertQueryReturns(
            "select {[Measures].[My Tuple]} on 0 from [Sales]",
            desiredResult);

        // same result if calc member is defined in query
        TestContext.instance().assertQueryReturns(
            "with member [Measures].[My Tuple] as\n"
                + " 'StrToTuple(\"([Gender].[M], [Marital Status].[S])\", [Gender], [Marital Status])'\n"
                + "select {[Measures].[My Tuple]} on 0 from [Sales]",
            desiredResult);
    }

    public void testCreateCalculatedMember() {
        // REVIEW: What is the purpose of this test?
        String query = "WITH MEMBER [Product].[Calculated Member] as 'AGGREGATE({})'\n"
            + "SELECT {[Measures].[Unit Sales]} on 0\n"
            + "FROM [Sales]\n"
            + "WHERE ([Product].[Calculated Member])";

        String derbySQL =
            "select \"product_class\".\"product_family\" from \"product\" as \"product\", \"product_class\" as \"product_class\" where \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\" and UPPER(\"product_class\".\"product_family\") = UPPER('Calculated Member') group by \"product_class\".\"product_family\" order by \"product_class\".\"product_family\" ASC";

        String mysqlSQL =
            "select `product_class`.`product_family` as `c0` from `product` as `product`, `product_class` as `product_class` where `product`.`product_class_id` = `product_class`.`product_class_id` and UPPER(`product_class`.`product_family`) = UPPER('Calculated Member') group by `product_class`.`product_family` order by ISNULL(`product_class`.`product_family`), `product_class`.`product_family` ASC";

        SqlPattern[] patterns = {
            new SqlPattern(Dialect.DatabaseProduct.DERBY, derbySQL, derbySQL),
            new SqlPattern(Dialect.DatabaseProduct.MYSQL, mysqlSQL, mysqlSQL)
        };

        assertQuerySqlOrNot(this.getTestContext(), query, patterns, true, true, true);
    }

    /**
     * Tests a calculated member which aggregates over a set which would seem
     * to include the calculated member (but does not).
     */
    public void testSetIncludesSelf() {
        assertQueryReturns(
            "with set [Top Products] as ' [Product].Children '\n"
            + "member [Product].[Top Product Total] as ' Aggregate([Top Products]) '\n"
            + "select {[Product].[Food], [Product].[Top Product Total]} on 0,"
            + " [Gender].Members on 1\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Product].[All Products].[Food]}\n"
            + "{[Product].[Top Product Total]}\n"
            + "Axis #2:\n"
            + "{[Gender].[All Gender]}\n"
            + "{[Gender].[All Gender].[F]}\n"
            + "{[Gender].[All Gender].[M]}\n"
            + "Row #0: 191,940\n"
            + "Row #0: 266,773\n"
            + "Row #1: 94,814\n"
            + "Row #1: 131,558\n"
            + "Row #2: 97,126\n"
            + "Row #2: 135,215\n");
    }

    /**
     * Test that if a filter is associated with input to a cal member with lower solve order;
     * the filter computation uses the context that contains the other cal members(those with
     * higher solve order).
     */
    public void testNegativeSolveOrderForCalMemberWithFilter() {
        assertQueryReturns(
            "With "
            + "Set [*NATIVE_CJ_SET] as 'NonEmptyCrossJoin([*BASE_MEMBERS_Education Level],[*BASE_MEMBERS_Product])' "
            + "Set [*METRIC_CJ_SET] as 'Filter([*NATIVE_CJ_SET],[Measures].[*Unit Sales_SEL~SUM] > 10000.0)' "
            + "Set [*BASE_MEMBERS_Measures] as '{[Measures].[*FORMATTED_MEASURE_0]}' "
            + "Set [*BASE_MEMBERS_Education Level] as '{[Education Level].[All Education Levels].[Bachelors Degree],[Education Level].[All Education Levels].[Graduate Degree]}' "
            + "Set [*NATIVE_MEMBERS_Education Level] as 'Generate([*NATIVE_CJ_SET], {[Education Level].CurrentMember})' "
            + "Set [*METRIC_MEMBERS_Education Level] as 'Generate([*METRIC_CJ_SET], {[Education Level].CurrentMember})' "
            + "Set [*BASE_MEMBERS_Product] as '{[Product].[All Products].[Food],[Product].[All Products].[Non-Consumable]}' "
            + "Set [*NATIVE_MEMBERS_Product] as 'Generate([*NATIVE_CJ_SET], {[Product].CurrentMember})' "
            + "Set [*METRIC_MEMBERS_Product] as 'Generate([*METRIC_CJ_SET], {[Product].CurrentMember})' "
            + "Member [Measures].[*Unit Sales_SEL~SUM] as '([Measures].[Unit Sales],[Education Level].CurrentMember,[Product].CurrentMember)', SOLVE_ORDER=200 "
            + "Member [Measures].[*FORMATTED_MEASURE_0] as '[Measures].[Unit Sales]', FORMAT_STRING = '#,##0', SOLVE_ORDER=300 "
            + "Member [Education Level].[*CTX_MEMBER_SEL~SUM] as 'Sum([*METRIC_MEMBERS_Education Level])', SOLVE_ORDER=-100 "
            + "Member [Product].[*CTX_MEMBER_SEL~SUM] as 'Sum(Filter([*METRIC_MEMBERS_Product], [Measures].[*Unit Sales_SEL~SUM] > 10000.0))', SOLVE_ORDER=-200 "
            + "Select "
            + "[*BASE_MEMBERS_Measures] on columns, "
            + "Non Empty Union("
            + "NonEmptyCrossJoin({[Education Level].[*CTX_MEMBER_SEL~SUM]},{[Product].[*CTX_MEMBER_SEL~SUM]}),"
            + "Generate([*METRIC_CJ_SET], {([Education Level].CurrentMember,[Product].CurrentMember)})) on rows "
            + "From [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[*FORMATTED_MEASURE_0]}\n"
            + "Axis #2:\n"
            + "{[Education Level].[*CTX_MEMBER_SEL~SUM], [Product].[*CTX_MEMBER_SEL~SUM]}\n"
            + "{[Education Level].[All Education Levels].[Bachelors Degree], [Product].[All Products].[Food]}\n"
            + "{[Education Level].[All Education Levels].[Bachelors Degree], [Product].[All Products].[Non-Consumable]}\n"
            + "{[Education Level].[All Education Levels].[Graduate Degree], [Product].[All Products].[Food]}\n"
            + "Row #0: 73,671\n"
            + "Row #1: 49,365\n"
            + "Row #2: 13,051\n"
            + "Row #3: 11,255\n");
    }

    /**
     * Test that if a filter is associated with input to a cal member with higher solve order;
     * the filter computation ignores the other cal members.
     */
    public void testNegativeSolveOrderForCalMemberWithFilters2() {
        assertQueryReturns(
            "With "
            + "Set [*NATIVE_CJ_SET] as 'NonEmptyCrossJoin([*BASE_MEMBERS_Education Level],[*BASE_MEMBERS_Product])' "
            + "Set [*METRIC_CJ_SET] as 'Filter([*NATIVE_CJ_SET],[Measures].[*Unit Sales_SEL~SUM] > 10000.0)' "
            + "Set [*BASE_MEMBERS_Measures] as '{[Measures].[*FORMATTED_MEASURE_0]}' "
            + "Set [*BASE_MEMBERS_Education Level] as '{[Education Level].[All Education Levels].[Bachelors Degree],[Education Level].[All Education Levels].[Graduate Degree]}' "
            + "Set [*NATIVE_MEMBERS_Education Level] as 'Generate([*NATIVE_CJ_SET], {[Education Level].CurrentMember})' "
            + "Set [*METRIC_MEMBERS_Education Level] as 'Generate([*METRIC_CJ_SET], {[Education Level].CurrentMember})' "
            + "Set [*BASE_MEMBERS_Product] as '{[Product].[All Products].[Food],[Product].[All Products].[Non-Consumable]}' "
            + "Set [*NATIVE_MEMBERS_Product] as 'Generate([*NATIVE_CJ_SET], {[Product].CurrentMember})' "
            + "Set [*METRIC_MEMBERS_Product] as 'Generate([*METRIC_CJ_SET], {[Product].CurrentMember})' "
            + "Member [Measures].[*Unit Sales_SEL~SUM] as '([Measures].[Unit Sales],[Education Level].CurrentMember,[Product].CurrentMember)', SOLVE_ORDER=200 "
            + "Member [Measures].[*FORMATTED_MEASURE_0] as '[Measures].[Unit Sales]', FORMAT_STRING = '#,##0', SOLVE_ORDER=300 "
            + "Member [Education Level].[*CTX_MEMBER_SEL~SUM] as 'Sum([*METRIC_MEMBERS_Education Level])', SOLVE_ORDER=-200 "
            + "Member [Product].[*CTX_MEMBER_SEL~SUM] as 'Sum(Filter([*METRIC_MEMBERS_Product], [Measures].[*Unit Sales_SEL~SUM] > 10000.0))', SOLVE_ORDER=-100 "
            + "Select "
            + "[*BASE_MEMBERS_Measures] on columns, "
            + "Non Empty Union("
            + "NonEmptyCrossJoin({[Education Level].[*CTX_MEMBER_SEL~SUM]},{[Product].[*CTX_MEMBER_SEL~SUM]}),"
            + "Generate([*METRIC_CJ_SET], {([Education Level].CurrentMember,[Product].CurrentMember)})) on rows "
            + "From [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[*FORMATTED_MEASURE_0]}\n"
            + "Axis #2:\n"
            + "{[Education Level].[*CTX_MEMBER_SEL~SUM], [Product].[*CTX_MEMBER_SEL~SUM]}\n"
            + "{[Education Level].[All Education Levels].[Bachelors Degree], [Product].[All Products].[Food]}\n"
            + "{[Education Level].[All Education Levels].[Bachelors Degree], [Product].[All Products].[Non-Consumable]}\n"
            + "{[Education Level].[All Education Levels].[Graduate Degree], [Product].[All Products].[Food]}\n"
            + "Row #0: 76,661\n"
            + "Row #1: 49,365\n"
            + "Row #2: 13,051\n"
            + "Row #3: 11,255\n");
    }


    /**
     * test case for bug #1801069, Issues with calculated members
     * verify that the calculated member [Product].[Food].[Test]
     * definition does not throw errors and returns expected
     * results.
     */
    public void testNonTopLevelCalculatedMember() {
        assertQueryReturns(
            "with member [Product].[Test] as '[Product].[Food]' "
            + "select {[Measures].[Unit Sales]} on columns, "
            + "{[Product].[Test]} on rows "
            + "from Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Test]}\n"
            + "Row #0: 191,940\n");

        assertQueryReturns(
            "with member [Product].[Food].[Test] as '[Product].[Food]' "
            + "select {[Measures].[Unit Sales]} on columns, "
            + "{[Product].[Food].[Test]} on rows "
            + "from Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[All Products].[Food].[Test]}\n"
            + "Row #0: 191,940\n");
    }

    /**
     * test case for bug #1801069, Issues with calculated members
     * verify that the calculated member [Product].[Test]
     * returns an empty children list vs. invalid behavior
     * of returning [All Products].Children
     */
    public void testCalculatedMemberChildren() {
        assertQueryReturns(
            "with member [Product].[Test] as '[Product].[Food]' "
            + "select {[Measures].[Unit Sales]} on columns, "
            + "[Product].[Test].children on rows "
            + "from Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n");
        assertQueryReturns(
            "with member [Product].[Food].[Test] as '[Product].[Food]' "
            + "select {[Measures].[Unit Sales]} on columns, "
            + "[Product].[Food].[Test].children on rows "
            + "from Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n");
    }

    public void testCalculatedMemberMSASCompatibility() {
        assertQueryReturns(
            "with "
            + "member gender.calculated as 'gender.m' "
            + "member  gender.[All Gender].calculated as 'gender.m' "
            + "member measures.countChildren as 'gender.calculated.children.Count' "
            + "member measures.parentIsAll as 'gender.calculated.Parent IS gender.[All Gender]' "
            + "member measures.levelOrdinal as 'gender.calculated.Level.Ordinal' "
            + "member measures.definedOnAllLevelOrdinal as 'gender.[all gender].calculated.Level.Ordinal' "
            + "member measures.definedOnAllLevelParentIsAll as 'gender.[all gender].calculated.Parent IS gender.[All Gender]' "
            + "member measures.definedOnAllLevelChildren as 'gender.[all gender].calculated.Children.Count' "
            + "member measures.definedOnAllLevelSiblings as 'gender.[all gender].calculated.Siblings.Count' "
            + "select { "
            + "  measures.[countChildren], " //   -- returns 0
            + "  measures.parentIsAll, " //   -- returns 0
            + "  measures.levelOrdinal, " //  -- returns 0
            + "  measures.definedOnAllLevelOrdinal, " //  -- returns 1
            + "  measures.definedOnAllLevelParentIsAll, " //   -- returns 1
            + "  measures.definedOnAllLevelChildren, " //   -- returns 0
            + "  measures.definedOnAllLevelSiblings " //   -- returns 2
            + "} on 0 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[countChildren]}\n"
            + "{[Measures].[parentIsAll]}\n"
            + "{[Measures].[levelOrdinal]}\n"
            + "{[Measures].[definedOnAllLevelOrdinal]}\n"
            + "{[Measures].[definedOnAllLevelParentIsAll]}\n"
            + "{[Measures].[definedOnAllLevelChildren]}\n"
            + "{[Measures].[definedOnAllLevelSiblings]}\n"
            + "Row #0: 0\n"
            + "Row #0: false\n"
            + "Row #0: 0\n"
            + "Row #0: 1\n"
            + "Row #0: true\n"
            + "Row #0: 0\n"
            + "Row #0: 2\n");
    }

    /**
     * Query that simulates a compound slicer by creating a calculated member
     * that aggregates over a set and places it in the WHERE clause.
     */
    public void testSimulatedCompoundSlicer() {
        assertQueryReturns(
            "with\n"
            + "  member [Measures].[Price per Unit] as\n"
            + "    [Measures].[Store Sales] / [Measures].[Unit Sales]\n"
            + "  set [Top Products] as\n"
            + "    TopCount(\n"
            + "      [Product].[Brand Name].Members,\n"
            + "      3,\n"
            + "      ([Measures].[Unit Sales], [Time].[1997].[Q3]))\n"
            + "  member [Product].[Top] as\n"
            + "    Aggregate([Top Products])\n"
            + "select {\n"
            + "  [Measures].[Unit Sales],\n"
            + "  [Measures].[Price per Unit]} on 0,\n"
            + " [Gender].Children * [Marital Status].Children on 1\n"
            + "from [Sales]\n"
            + "where ([Product].[Top], [Time].[1997].[Q3])",
            "Axis #0:\n"
            + "{[Product].[Top], [Time].[1997].[Q3]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Price per Unit]}\n"
            + "Axis #2:\n"
            + "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[M]}\n"
            + "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S]}\n"
            + "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M]}\n"
            + "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S]}\n"
            + "Row #0: 779\n"
            + "Row #0: 2.40\n"
            + "Row #1: 811\n"
            + "Row #1: 2.24\n"
            + "Row #2: 829\n"
            + "Row #2: 2.23\n"
            + "Row #3: 886\n"
            + "Row #3: 2.25\n");

        // Now the equivalent query, using a set in the slicer.
        assertQueryReturns(
            "with\n"
            + "  member [Measures].[Price per Unit] as\n"
            + "    [Measures].[Store Sales] / [Measures].[Unit Sales]\n"
            + "  set [Top Products] as\n"
            + "    TopCount(\n"
            + "      [Product].[Brand Name].Members,\n"
            + "      3,\n"
            + "      ([Measures].[Unit Sales], [Time].[1997].[Q3]))\n"
            + "select {\n"
            + "  [Measures].[Unit Sales],\n"
            + "  [Measures].[Price per Unit]} on 0,\n"
            + " [Gender].Children * [Marital Status].Children on 1\n"
            + "from [Sales]\n"
            + "where [Top Products] * [Time].[1997].[Q3]",
            "Axis #0:\n"
            + "{[Product].[All Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos], [Time].[1997].[Q3]}\n"
            + "{[Product].[All Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale], [Time].[1997].[Q3]}\n"
            + "{[Product].[All Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony], [Time].[1997].[Q3]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Price per Unit]}\n"
            + "Axis #2:\n"
            + "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[M]}\n"
            + "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S]}\n"
            + "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M]}\n"
            + "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S]}\n"
            + "Row #0: 779\n"
            + "Row #0: 2.40\n"
            + "Row #1: 811\n"
            + "Row #1: 2.24\n"
            + "Row #2: 829\n"
            + "Row #2: 2.23\n"
            + "Row #3: 886\n"
            + "Row #3: 2.25\n");
    }

    public void testCompoundSlicerOverTuples() {
        // reference query
        assertQueryReturns(
            "select [Measures].[Unit Sales] on 0,\n"
            + "    TopCount(\n"
            + "      [Product].[Product Category].Members\n"
            + "      * [Customers].[City].Members,\n"
            + "      10) on 1\n"
            + "from [Sales]\n"
            + "where [Time].[1997].[Q3]",
            "Axis #0:\n"
            + "{[Time].[1997].[Q3]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine], [Customers].[All Customers].[Canada].[BC].[Burnaby]}\n"
            + "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine], [Customers].[All Customers].[Canada].[BC].[Cliffside]}\n"
            + "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine], [Customers].[All Customers].[Canada].[BC].[Haney]}\n"
            + "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine], [Customers].[All Customers].[Canada].[BC].[Ladner]}\n"
            + "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine], [Customers].[All Customers].[Canada].[BC].[Langford]}\n"
            + "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine], [Customers].[All Customers].[Canada].[BC].[Langley]}\n"
            + "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine], [Customers].[All Customers].[Canada].[BC].[Metchosin]}\n"
            + "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine], [Customers].[All Customers].[Canada].[BC].[N. Vancouver]}\n"
            + "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine], [Customers].[All Customers].[Canada].[BC].[Newton]}\n"
            + "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine], [Customers].[All Customers].[Canada].[BC].[Oak Bay]}\n"
            + "Row #0: \n"
            + "Row #1: \n"
            + "Row #2: \n"
            + "Row #3: \n"
            + "Row #4: \n"
            + "Row #5: \n"
            + "Row #6: \n"
            + "Row #7: \n"
            + "Row #8: \n"
            + "Row #9: \n");

        // The actual query. Note that the set in the slicer has two dimensions.
        // This could not be expressed using calculated members and the
        // Aggregate function.
        assertQueryReturns(
            "with\n"
            + "  member [Measures].[Price per Unit] as\n"
            + "    [Measures].[Store Sales] / [Measures].[Unit Sales]\n"
            + "  set [Top Product Cities] as\n"
            + "    TopCount(\n"
            + "      [Product].[Product Category].Members\n"
            + "      * [Customers].[City].Members,\n"
            + "      3,\n"
            + "      ([Measures].[Unit Sales], [Time].[1997].[Q3]))\n"
            + "select {\n"
            + "  [Measures].[Unit Sales],\n"
            + "  [Measures].[Price per Unit]} on 0,\n"
            + " [Gender].Children * [Marital Status].Children on 1\n"
            + "from [Sales]\n"
            + "where [Top Product Cities] * [Time].[1997].[Q3]",
            "Axis #0:\n"
            + "{[Product].[All Products].[Food].[Snack Foods].[Snack Foods], [Customers].[All Customers].[USA].[WA].[Spokane], [Time].[1997].[Q3]}\n"
            + "{[Product].[All Products].[Food].[Produce].[Vegetables], [Customers].[All Customers].[USA].[WA].[Spokane], [Time].[1997].[Q3]}\n"
            + "{[Product].[All Products].[Food].[Snack Foods].[Snack Foods], [Customers].[All Customers].[USA].[WA].[Puyallup], [Time].[1997].[Q3]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Price per Unit]}\n"
            + "Axis #2:\n"
            + "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[M]}\n"
            + "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S]}\n"
            + "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M]}\n"
            + "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S]}\n"
            + "Row #0: 483\n"
            + "Row #0: 2.21\n"
            + "Row #1: 419\n"
            + "Row #1: 2.21\n"
            + "Row #2: 422\n"
            + "Row #2: 2.22\n"
            + "Row #3: 332\n"
            + "Row #3: 2.20\n");
    }
}

// End TestCalculatedMembers.java
