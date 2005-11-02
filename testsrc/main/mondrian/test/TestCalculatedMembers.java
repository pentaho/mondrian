/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 5 October, 2002
*/
package mondrian.test;

import junit.framework.Assert;
import junit.framework.AssertionFailedError;
import mondrian.olap.*;

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
        Assert.assertEquals("$339,610.90", s);
    }

    public void testCalculatedMemberInCubeViaApi() {
        Cube salesCube = getSalesCube("Sales");
        salesCube.createCalculatedMember(
            "<CalculatedMember name='Profit2'" +
            "  dimension='Measures'" +
            "  formula='[Measures].[Store Sales]-[Measures].[Store Cost]'/>");

        String s = executeExpr("[Measures].[Profit2]");
        Assert.assertEquals("339,610.90", s);

        // should fail if member of same name exists
        try {
            salesCube.createCalculatedMember(
                "<CalculatedMember name='Profit2'" +
                "  dimension='Measures'" +
                "  formula='[Measures].[Store Sales]-[Measures].[Store Cost]'/>");
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
        Cube salesCube = getSalesCube("Warehouse and Sales");
        salesCube.createCalculatedMember(
            "<CalculatedMember name='Profit With Spaces'" +
            "  dimension='Measures'" +
            "  formula='[Measures].[Store Sales]-[Measures].[Store Cost]'/>");

        Cell s = getTestContext("Warehouse and Sales").executeExprRaw("[Measures].[Profit With Spaces]");
        Assert.assertEquals("339,610.90", s.getFormattedValue());
    }

    public void testCalculatedMemberInCubeWithProps() {
        Cube salesCube = getSalesCube("Sales");

        // member with a property
        salesCube.createCalculatedMember(
            "<CalculatedMember name='Profit3'" +
            "  dimension='Measures'" +
            "  formula='[Measures].[Store Sales]-[Measures].[Store Cost]'>" +
            "    <CalculatedMemberProperty name='FORMAT_STRING' value='#'/>" +
            "</CalculatedMember>");

        // note that result uses format string
        Result result = TestContext.instance().executeQuery(
            "select {[Measures].[Profit3]} on columns from Sales");
        String s = result.getCell(new int[]{0}).getFormattedValue();
        Assert.assertEquals("339611", s);

        // should fail if member property has expr and value
        try {
            salesCube.createCalculatedMember(
                "<CalculatedMember name='Profit4'" +
                "  dimension='Measures'" +
                "  formula='[Measures].[Store Sales]-[Measures].[Store Cost]'>" +
                "    <CalculatedMemberProperty name='FORMAT_STRING' />" +
                "</CalculatedMember>");
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
                "<CalculatedMember name='Profit4'" +
                "  dimension='Measures'" +
                "  formula='[Measures].[Store Sales]-[Measures].[Store Cost]'>" +
                "    <CalculatedMemberProperty name='FORMAT_STRING' value='#' expression='\"#\"' />" +
                "</CalculatedMember>");
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
                "<CalculatedMember name='Profit4'" +
                "  dimension='Measures'" +
                "  formula='[Measures].[Store Sales]-[Measures].[Store Cost]'>" +
                "    <CalculatedMemberProperty name='FORMAT_STRING' expression='1 + [FooBar]' />" +
                "</CalculatedMember>");
            throw new AssertionFailedError("expected error, got none");
        } catch (RuntimeException e) {
            final String msg = e.getMessage();
            if (!msg.equals("Mondrian Error:Named set in cube 'Sales' has bad formula")) {
                throw e;
            }
        }
    }

    private Cube getSalesCube(String cubeName) {
        Cube[] cubes = getConnection().getSchema().getSchemaReader().getCubes();
        for (int i = 0; i < cubes.length; i++) {
            Cube cube = cubes[i];
            if (cube.getName().equals(cubeName)) {
                return cube;
            }
        }
        return null;
    }

    public void testCalculatedMemberInCubeAndQuery() {
        // Profit is defined in the cube.
        // Profit Change is defined in the query.
        assertQueryReturns("WITH MEMBER [Measures].[Profit Change]" + nl +
            " AS '[Measures].[Profit] - ([Measures].[Profit], [Time].PrevMember)'" + nl +
            "SELECT {[Measures].[Profit], [Measures].[Profit Change]} ON COLUMNS," + nl +
            " {[Time].[1997].[Q2].children} ON ROWS" + nl +
            "FROM [Sales]",
            "Axis #0:" + nl +
            "{}" + nl +
            "Axis #1:" + nl +
            "{[Measures].[Profit]}" + nl +
            "{[Measures].[Profit Change]}" + nl +
            "Axis #2:" + nl +
            "{[Time].[1997].[Q2].[4]}" + nl +
            "{[Time].[1997].[Q2].[5]}" + nl +
            "{[Time].[1997].[Q2].[6]}" + nl +
            "Row #0: $25,766.55" + nl +
            "Row #0: $-4,289.24" + nl +
            "Row #1: $26,673.73" + nl +
            "Row #1: $907.18" + nl +
            "Row #2: $27,261.76" + nl +
            "Row #2: $588.03" + nl);
    }

    public void testQueryCalculatedMemberOverridesCube() {
        // Profit is defined in the cube, and has a format string "$#,###".
        // We define it in a query to make sure that the format string in the
        // cube doesn't change.
        assertQueryReturns("WITH MEMBER [Measures].[Profit]" + nl +
            " AS '(Measures.[Store Sales] - Measures.[Store Cost])', FORMAT_STRING='#,###' " + nl +
            "SELECT {[Measures].[Profit]} ON COLUMNS," + nl +
            " {[Time].[1997].[Q2]} ON ROWS" + nl +
            "FROM [Sales]",
                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Profit]}" + nl +
                "Axis #2:" + nl +
                "{[Time].[1997].[Q2]}" + nl +
                "Row #0: 79,702" + nl);

        // Note that the Profit measure defined against the cube has
        // a format string preceded by "$".
        assertQueryReturns("SELECT {[Measures].[Profit]} ON COLUMNS," + nl +
                " {[Time].[1997].[Q2]} ON ROWS" + nl +
                "FROM [Sales]",
                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Profit]}" + nl +
                "Axis #2:" + nl +
                "{[Time].[1997].[Q2]}" + nl +
                "Row #0: $79,702.05" + nl);
    }

    public void _testWhole() {

        /*
         * "allmembers" tests compatibility with MSAS
         */

        executeQuery(
                "with" + nl +
                "member [Measures].[Total Store Sales by Product Name] as" + nl +
                "  'Sum([Product].[Product Name].members, [Measures].[Store Sales])'" + nl +
                "" + nl +
                "member [Measures].[Average Store Sales by Product Name] as" + nl +
                "  'Avg([Product].[Product Name].allmembers, [Measures].[Store Sales])'" + nl +
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
                "   [Store].[USA]," + nl +
                "   [Store].[USA].[CA]," + nl +
                "   [Store].[USA].[CA].[San Francisco]}) on columns," + nl +
                " AddCalculatedMembers([Measures].members) on rows" + nl +
                " from Sales");

        // Repeat time-related measures with more time members.
        executeQuery(
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
                "   [Store].[USA].[CA].[San Francisco]}," + nl +
                "  [Time].[Month].members) on columns," + nl +
                " AddCalculatedMembers([Measures].members) on rows" + nl +
                " from Sales");
    }

    public void testCalculatedMemberCaption() {
        String mdx = "select {[Measures].[Profit Growth]} on columns from Sales";
        Result result = TestContext.instance().executeQuery(mdx);
        Axis axis0 = result.getAxes()[0];
        Position pos0 = axis0.positions[0];
        Member profGrowth = pos0.members[0];
        String caption = profGrowth.getCaption();
        Assert.assertEquals(caption, "Gewinn-Wachstum");
    }

    public void testCalcMemberIsSetFails() {
        if (false) {
        // A member which is a set, and more important, cannot be converted to
        // a value, is an error.
        String queryString =
                "with member [Measures].[Foo] as ' Filter([Product].members, 1 <> 0) '" +
                "select {[Measures].[Foo]} on columns from [Sales]";
        String pattern = "Member expression 'Filter([Product].Members, (1.0 <> 0.0))' must not be a set";
        assertThrows(queryString, pattern);

        // A tuple is OK, because it can be converted to a scalar expression.
        queryString =
                "with member [Measures].[Foo] as ' ([Measures].[Unit Sales], [Gender].[F]) '" +
                "select {[Measures].[Foo]} on columns from [Sales]";
            final Result result = executeQuery(queryString);
        Util.discard(result);

        // Level cannot be converted.
        assertExprThrows("[Customers].[Country]",
                "Member expression '[Customers].[Country]' must not be a set");

        // Dimension can be converted.
        assertExprReturns("[Customers]", "266,773");

        // Member can be converted.
        assertExprReturns("[Customers].[USA]", "266,773");

        // Tuple can be converted.
        assertExprReturns("([Customers].[USA], [Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer])",
                "1,683");
        }
        // Set of tuples cannot be converted.
        assertExprThrows("{([Customers].[USA], [Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer])}",
                "Member expression '{([Customers].[All Customers].[USA], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer])}' must not be a set");
        assertExprThrows("{([Customers].[USA], [Product].[Food])," +
                "([Customers].[USA], [Product].[Drink])}",
                "{([Customers].[All Customers].[USA], [Product].[All Products].[Food]), ([Customers].[All Customers].[USA], [Product].[All Products].[Drink])}' must not be a set");

        // Sets cannot be converted.
        assertExprThrows("{[Product].[Food]}",
                "Member expression '{[Product].[All Products].[Food]}' must not be a set");
    }

    /**
     * Tests that calculated members can have brackets in their names.
     * (Bug 1251683.)
     */
    public void testBracketInCalcMemberName() {
        assertQueryReturns(
                fold(new String[] {
                    "with member [Measures].[has a [bracket]] in it] as ",
                    "' [Measures].CurrentMember.Name '",
                    "select {[Measures].[has a [bracket]] in it]} on columns",
                    "from [Sales]"}),
                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[has a [bracket]] in it]}" + nl +
                "Row #0: Unit Sales" + nl);
    }

    /**
     * Tests that calculated members defined in the schema can have brackets in
     * their names. (Bug 1251683.)
     */
    public void testBracketInCubeCalcMemberName() {
        Schema schema = getConnection().getSchema();
        final String cubeName = "Sales_BracketInCubeCalcMemberName";
        schema.createCube(fold(new String[] {
                "<Cube name=\"" + cubeName + "\">",
                "  <Table name=\"sales_fact_1997\"/>",
                "  <Dimension name=\"Gender\" foreignKey=\"customer_id\">",
                "    <Hierarchy hasAll=\"false\" primaryKey=\"customer_id\">",
                "    <Table name=\"customer\"/>",
                "      <Level name=\"Gender\" column=\"gender\" uniqueMembers=\"true\"/>",
                "    </Hierarchy>",
                "  </Dimension>",
                "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\"",
                "      formatString=\"Standard\" visible=\"false\"/>",
                "  <CalculatedMember",
                "      name=\"With a [bracket] in it\"",
                "      dimension=\"Measures\"",
                "      visible=\"false\"",
                "      formula=\"[Measures].[Unit Sales] * 10\">",
                "    <CalculatedMemberProperty name=\"FORMAT_STRING\" value=\"$#,##0.00\"/>",
                "  </CalculatedMember>",
                "</Cube>"}));

        getTestContext(cubeName).assertThrows(
                fold(new String[] {
                    "select {[Measures].[With a [bracket] in it]} on columns,",
                    " {[Gender].Members} on rows",
                    "from [" + cubeName + "]"}),
                "Syntax error at line 1, column 38, token 'in'");

        getTestContext(cubeName).assertQueryReturns(
                fold(new String[] {
                    "select {[Measures].[With a [bracket]] in it]} on columns,",
                    " {[Gender].Members} on rows",
                    "from [" + cubeName + "]"}),
                fold(new String[] {
                    "Axis #0:",
                    "{}",
                "Axis #1:",
                "{[Measures].[With a [bracket]] in it]}",
                "Axis #2:",
                "{[Gender].[F]}",
                "{[Gender].[M]}",
                "Row #0: $1,315,580.00",
                "Row #1: $1,352,150.00",
                ""}));
    }
}

// End CalculatedMembersTestCase.java
