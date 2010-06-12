/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2005-2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.test;

import mondrian.olap.*;
import mondrian.olap.type.*;
import mondrian.spi.UserDefinedFunction;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Unit-test for {@link UserDefinedFunction user-defined functions}.
 *
 * <p>TODO:
 * 1. test that function which does not return a name, description etc.
 *    gets a sensible error
 * 2. document UDFs
 *
 * @author jhyde
 * @since Apr 29, 2005
 * @version $Id$
 */
public class UdfTest extends FoodMartTestCase {

    public UdfTest() {
    }
    public UdfTest(String name) {
        super(name);
    }

    /**
     * Test context which uses the local FoodMart schema, and adds a "PlusOne"
     * user-defined function.
     */
    private final TestContext tc = TestContext.create(
        null,
        null,
        null,
        null,
        "<UserDefinedFunction name=\"PlusOne\" className=\""
        + PlusOneUdf.class.getName()
        + "\"/>\n",
        null);

    public TestContext getTestContext() {
        return tc;
    }

    public void testSanity() {
        // sanity check, make sure the schema is loading correctly
        assertQueryReturns(
            "SELECT {[Measures].[Store Sqft]} ON COLUMNS, {[Store Type]} ON ROWS FROM [Store]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Sqft]}\n"
            + "Axis #2:\n"
            + "{[Store Type].[All Store Types]}\n"
            + "Row #0: 571,596\n");
    }

    public void testFun() {
        assertQueryReturns(
            "WITH MEMBER [Measures].[Sqft Plus One] AS 'PlusOne([Measures].[Store Sqft])'\n"
            + "SELECT {[Measures].[Sqft Plus One]} ON COLUMNS, \n"
            + "  {[Store Type].children} ON ROWS \n"
            + "FROM [Store]",

            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Sqft Plus One]}\n"
            + "Axis #2:\n"
            + "{[Store Type].[Deluxe Supermarket]}\n"
            + "{[Store Type].[Gourmet Supermarket]}\n"
            + "{[Store Type].[HeadQuarters]}\n"
            + "{[Store Type].[Mid-Size Grocery]}\n"
            + "{[Store Type].[Small Grocery]}\n"
            + "{[Store Type].[Supermarket]}\n"
            + "Row #0: 146,046\n"
            + "Row #1: 47,448\n"
            + "Row #2: \n"
            + "Row #3: 109,344\n"
            + "Row #4: 75,282\n"
            + "Row #5: 193,481\n");
    }

    public void testLastNonEmpty() {
        assertQueryReturns(
            "WITH MEMBER [Measures].[Last Unit Sales] AS \n"
            + " '([Measures].[Unit Sales], \n"
            + "   LastNonEmpty(Descendants([Time].[Time]), [Measures].[Unit Sales]))'\n"
            + "SELECT {[Measures].[Last Unit Sales]} ON COLUMNS,\n"
            + " CrossJoin(\n"
            + "  {[Time].[1997], [Time].[1997].[Q1], [Time].[1997].[Q1].Children},\n"
            + "  {[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].children}) ON ROWS\n"
            + "FROM [Sales]\n"
            + "WHERE ([Store].[All Stores].[USA].[OR].[Portland].[Store 11])",
            "Axis #0:\n"
            + "{[Store].[USA].[OR].[Portland].[Store 11]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Last Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Time].[1997], [Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good]}\n"
            + "{[Time].[1997], [Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Pearl]}\n"
            + "{[Time].[1997], [Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth]}\n"
            + "{[Time].[1997], [Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Top Measure]}\n"
            + "{[Time].[1997], [Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Walrus]}\n"
            + "{[Time].[1997].[Q1], [Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good]}\n"
            + "{[Time].[1997].[Q1], [Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Pearl]}\n"
            + "{[Time].[1997].[Q1], [Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth]}\n"
            + "{[Time].[1997].[Q1], [Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Top Measure]}\n"
            + "{[Time].[1997].[Q1], [Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Walrus]}\n"
            + "{[Time].[1997].[Q1].[1], [Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good]}\n"
            + "{[Time].[1997].[Q1].[1], [Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Pearl]}\n"
            + "{[Time].[1997].[Q1].[1], [Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth]}\n"
            + "{[Time].[1997].[Q1].[1], [Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Top Measure]}\n"
            + "{[Time].[1997].[Q1].[1], [Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Walrus]}\n"
            + "{[Time].[1997].[Q1].[2], [Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good]}\n"
            + "{[Time].[1997].[Q1].[2], [Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Pearl]}\n"
            + "{[Time].[1997].[Q1].[2], [Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth]}\n"
            + "{[Time].[1997].[Q1].[2], [Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Top Measure]}\n"
            + "{[Time].[1997].[Q1].[2], [Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Walrus]}\n"
            + "{[Time].[1997].[Q1].[3], [Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good]}\n"
            + "{[Time].[1997].[Q1].[3], [Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Pearl]}\n"
            + "{[Time].[1997].[Q1].[3], [Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth]}\n"
            + "{[Time].[1997].[Q1].[3], [Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Top Measure]}\n"
            + "{[Time].[1997].[Q1].[3], [Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Walrus]}\n"
            + "Row #0: 2\n"
            + "Row #1: 7\n"
            + "Row #2: 6\n"
            + "Row #3: 7\n"
            + "Row #4: 4\n"
            + "Row #5: 3\n"
            + "Row #6: 4\n"
            + "Row #7: 3\n"
            + "Row #8: 4\n"
            + "Row #9: 2\n"
            + "Row #10: \n"
            + "Row #11: 4\n"
            + "Row #12: \n"
            + "Row #13: 2\n"
            + "Row #14: \n"
            + "Row #15: \n"
            + "Row #16: 2\n"
            + "Row #17: \n"
            + "Row #18: 4\n"
            + "Row #19: \n"
            + "Row #20: 3\n"
            + "Row #21: 4\n"
            + "Row #22: 3\n"
            + "Row #23: 4\n"
            + "Row #24: 2\n");
    }

    /**
     * Tests a performance issue with LastNonEmpty (bug 1533677). The naive
     * implementation of LastNonEmpty crawls backward one period at a time,
     * generates a cache miss, and the next iteration reads precisely one cell.
     * So the query soon exceeds the {@link MondrianProperties#MaxEvalDepth}
     * property.
     */
    public void testLastNonEmptyBig() {
        assertQueryReturns(
            "with\n"
            + "     member\n"
            + "     [Measures].[Last Sale] as ([Measures].[Unit Sales],\n"
            + "         LastNonEmpty(Descendants([Time].[Time].CurrentMember, [Time].[Month]),\n"
            + "         [Measures].[Unit Sales]))\n"
            + "select\n"
            + "     NON EMPTY {[Measures].[Last Sale]} ON columns,\n"
            + "     NON EMPTY Order([Store].[All Stores].Children,\n"
            + "         [Measures].[Last Sale], DESC) ON rows\n"
            + "from [Sales]\n"
            + "where [Time].[Time].LastSibling",
            "Axis #0:\n"
            + "{[Time].[1998]}\n"
            + "Axis #1:\n"
            + "Axis #2:\n");
    }

    public void testBadFun() {
        final TestContext tc = TestContext.create(
            null,
            null,
            null,
            null,
            "<UserDefinedFunction name=\"BadPlusOne\" className=\""
            + BadPlusOneUdf.class.getName()
            + "\"/>\n",
            null);
        try {
            tc.executeQuery("SELECT {} ON COLUMNS FROM [Sales]");
            fail("Expected exception");
        } catch (Exception e) {
            final String s = e.getMessage();
            assertEquals(
                "Mondrian Error:Internal error: Invalid "
                + "user-defined function 'BadPlusOne': return type is null", s);
        }
    }

    public void testGenericFun() {
        final TestContext tc = TestContext.create(
            null,
            null,
            null,
            null,
            "<UserDefinedFunction name=\"GenericPlusOne\" className=\""
            + PlusOrMinusOneUdf.class.getName()
            + "\"/>\n"
            + "<UserDefinedFunction name=\"GenericMinusOne\" className=\""
            + PlusOrMinusOneUdf.class.getName()
            + "\"/>\n",
            null);
        tc.assertExprReturns("GenericPlusOne(3)", "4");
        tc.assertExprReturns("GenericMinusOne(3)", "2");
    }

    public void testComplexFun() {
        assertQueryReturns(
            "WITH MEMBER [Measures].[InverseNormal] AS 'InverseNormal([Measures].[Grocery Sqft] / [Measures].[Store Sqft])', FORMAT_STRING = \"0.000\"\n"
            + "SELECT {[Measures].[InverseNormal]} ON COLUMNS, \n"
            + "  {[Store Type].children} ON ROWS \n"
            + "FROM [Store]",

            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[InverseNormal]}\n"
            + "Axis #2:\n"
            + "{[Store Type].[Deluxe Supermarket]}\n"
            + "{[Store Type].[Gourmet Supermarket]}\n"
            + "{[Store Type].[HeadQuarters]}\n"
            + "{[Store Type].[Mid-Size Grocery]}\n"
            + "{[Store Type].[Small Grocery]}\n"
            + "{[Store Type].[Supermarket]}\n"
            + "Row #0: 0.467\n"
            + "Row #1: 0.463\n"
            + "Row #2: \n"
            + "Row #3: 0.625\n"
            + "Row #4: 0.521\n"
            + "Row #5: 0.504\n");
    }

    public void testException() {
        Result result = executeQuery(
            "WITH MEMBER [Measures].[InverseNormal] "
            + " AS 'InverseNormal([Measures].[Store Sqft] / [Measures].[Grocery Sqft])',"
            + " FORMAT_STRING = \"0.000000\"\n"
            + "SELECT {[Measures].[InverseNormal]} ON COLUMNS, \n"
            + "  {[Store Type].children} ON ROWS \n"
            + "FROM [Store]");
        Axis rowAxis = result.getAxes()[0];
        assertTrue(rowAxis.getPositions().size() == 1);
        Axis colAxis = result.getAxes()[1];
        assertTrue(colAxis.getPositions().size() == 6);
        Cell cell = result.getCell(new int[]{0, 0});
        assertTrue(cell.isError());
        getTestContext().assertMatchesVerbose(
            Pattern.compile(
                ".*Invalid value for inverse normal distribution: 1.4708.*"),
            cell.getValue().toString());
        cell = result.getCell(new int[]{0, 5});
        assertTrue(cell.isError());
        getTestContext().assertMatchesVerbose(
            Pattern.compile(
                ".*Invalid value for inverse normal distribution: 1.4435.*"),
            cell.getValue().toString());
    }

    public void testCurrentDateString()
    {
        String actual = executeExpr("CurrentDateString(\"Ddd mmm dd yyyy\")");
        Date currDate = new Date();
        String dateString = currDate.toString();
        String expected =
            dateString.substring(0, 11)
            + dateString.substring(dateString.length() - 4);
        assertEquals(expected, actual);
    }

    public void testCurrentDateMemberBefore() {
        assertQueryReturns(
            "SELECT { CurrentDateMember([Time].[Time], "
            + "\"[Ti\\me]\\.[yyyy]\\.[Qq]\\.[m]\", BEFORE)} "
            + "ON COLUMNS FROM [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time].[1998].[Q4].[12]}\n"
            + "Row #0: \n");
    }

    public void testCurrentDateMemberBeforeUsingQuotes()
    {
        assertAxisReturns(
            MondrianProperties.instance().SsasCompatibleNaming.get()
            ? "CurrentDateMember([Time].[Time], "
            + "'\"[Time].[Time].[\"yyyy\"].[Q\"q\"].[\"m\"]\"', BEFORE)"
            : "CurrentDateMember([Time], "
            + "'\"[Time].[\"yyyy\"].[Q\"q\"].[\"m\"]\"', BEFORE)",
            "[Time].[1998].[Q4].[12]");
    }

    public void testCurrentDateMemberAfter()
    {
        // CurrentDateMember will return null member since the latest date in
        // FoodMart is from '98
        assertQueryReturns(
            "SELECT { CurrentDateMember([Time].[Time], "
            + "\"[Ti\\me]\\.[yyyy]\\.[Qq]\\.[m]\", AFTER)} "
            + "ON COLUMNS FROM [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n");
    }

    public void testCurrentDateMemberExact()
    {
        // CurrentDateMember will return null member since the latest date in
        // FoodMart is from '98; apply a function on the return value to
        // ensure null member instead of null is returned
        assertQueryReturns(
            "SELECT { CurrentDateMember([Time].[Time], "
            + "\"[Ti\\me]\\.[yyyy]\\.[Qq]\\.[m]\", EXACT).lag(1)} "
            + "ON COLUMNS FROM [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n");
    }

    public void testCurrentDateMemberNoFindArg()
    {
        // CurrentDateMember will return null member since the latest date in
        // FoodMart is from '98
        assertQueryReturns(
            "SELECT { CurrentDateMember([Time].[Time], "
            + "\"[Ti\\me]\\.[yyyy]\\.[Qq]\\.[m]\")} "
            + "ON COLUMNS FROM [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n");
    }

    public void testCurrentDateMemberHierarchy() {
        final String query =
            MondrianProperties.instance().SsasCompatibleNaming.get()
                ? "SELECT { CurrentDateMember([Time.Weekly], "
                  + "\"[Ti\\me\\.Weekl\\y]\\.[All Weekl\\y\\s]\\.[yyyy]\\.[ww]\", BEFORE)} "
                  + "ON COLUMNS FROM [Sales]"
                : "SELECT { CurrentDateMember([Time.Weekly], "
                  + "\"[Ti\\me\\.Weekl\\y]\\.[All Ti\\me\\.Weekl\\y\\s]\\.[yyyy]\\.[ww]\", BEFORE)} "
                  + "ON COLUMNS FROM [Sales]";
        assertQueryReturns(
            query,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time].[Weekly].[1998].[52]}\n"
            + "Row #0: \n");
    }

    public void testCurrentDateMemberHierarchyNullReturn() {
        // CurrentDateMember will return null member since the latest date in
        // FoodMart is from '98; note that first arg is a hierarchy rather
        // than a dimension
        assertQueryReturns(
            "SELECT { CurrentDateMember([Time.Weekly], "
            + "\"[Ti\\me]\\.[yyyy]\\.[Qq]\\.[m]\")} "
            + "ON COLUMNS FROM [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n");
    }

    public void testCurrentDateMemberRealAfter() {
        // omit formatting characters from the format so the current date
        // is hard-coded to actual value in the database so we can test the
        // after logic
        assertQueryReturns(
            "SELECT { CurrentDateMember([Time].[Time], "
            + "\"[Ti\\me]\\.[1996]\\.[Q4]\", after)} "
            + "ON COLUMNS FROM [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time].[1997].[Q1]}\n"
            + "Row #0: 66,291\n");
    }

    public void testCurrentDateMemberRealExact1() {
        // omit formatting characters from the format so the current date
        // is hard-coded to actual value in the database so we can test the
        // exact logic
        assertQueryReturns(
            "SELECT { CurrentDateMember([Time].[Time], "
            + "\"[Ti\\me]\\.[1997]\")} "
            + "ON COLUMNS FROM [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time].[1997]}\n"
            + "Row #0: 266,773\n");
    }

    public void testCurrentDateMemberRealExact2() {
        // omit formatting characters from the format so the current date
        // is hard-coded to actual value in the database so we can test the
        // exact logic
        assertQueryReturns(
            "SELECT { CurrentDateMember([Time].[Time], "
            + "\"[Ti\\me]\\.[1997]\\.[Q2]\\.[5]\")} "
            + "ON COLUMNS FROM [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time].[1997].[Q2].[5]}\n"
            + "Row #0: 21,081\n");
    }

    public void testCurrentDateMemberPrev() {
        // apply a function on the result of the UDF
        assertQueryReturns(
            "SELECT { CurrentDateMember([Time].[Time], "
            + "\"[Ti\\me]\\.[yyyy]\\.[Qq]\\.[m]\", BEFORE).PrevMember} "
            + "ON COLUMNS FROM [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time].[1998].[Q4].[11]}\n"
            + "Row #0: \n");
    }

    public void testCurrentDateLag() {
        // Also, try a different style of quoting, because single quote followed
        // by double quote (used in other examples) is difficult to read.
        assertQueryReturns(
            "SELECT\n"
            + "    { [Measures].[Unit Sales] } ON COLUMNS,\n"
            + "    { CurrentDateMember([Time].[Time], '[\"Time\"]\\.[yyyy]\\.[\"Q\"q]\\.[m]', BEFORE).Lag(3) : "
            + "      CurrentDateMember([Time].[Time], '[\"Time\"]\\.[yyyy]\\.[\"Q\"q]\\.[m]', BEFORE) } ON ROWS\n"
            + "FROM [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Time].[1998].[Q3].[9]}\n"
            + "{[Time].[1998].[Q4].[10]}\n"
            + "{[Time].[1998].[Q4].[11]}\n"
            + "{[Time].[1998].[Q4].[12]}\n"
            + "Row #0: \n"
            + "Row #1: \n"
            + "Row #2: \n"
            + "Row #3: \n");
    }

    public void testMatches() {
        assertQueryReturns(
            "SELECT {[Measures].[Org Salary]} ON COLUMNS, "
            + "Filter({[Employees].MEMBERS}, "
            + "[Employees].CurrentMember.Name MATCHES '(?i)sam.*') ON ROWS "
            + "FROM [HR]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Org Salary]}\n"
            + "Axis #2:\n"
            + "{[Employees].[Sheri Nowmer].[Derrick Whelply].[Beverly Baker].[Jacqueline Wyllie].[Ralph Mccoy].[Anne Tuck].[Samuel Johnson]}\n"
            + "{[Employees].[Sheri Nowmer].[Derrick Whelply].[Pedro Castillo].[Jose Bernard].[Mary Hunt].[Bonnie Bruno].[Sam Warren]}\n"
            + "{[Employees].[Sheri Nowmer].[Derrick Whelply].[Pedro Castillo].[Charles Macaluso].[Barbara Wallin].[Michael Suggs].[Sam Adair]}\n"
            + "{[Employees].[Sheri Nowmer].[Derrick Whelply].[Pedro Castillo].[Lois Wood].[Dell Gras].[Kristine Aldred].[Sam Zeller]}\n"
            + "{[Employees].[Sheri Nowmer].[Derrick Whelply].[Laurie Borges].[Cody Goldey].[Shanay Steelman].[Neal Hasty].[Sam Wheeler]}\n"
            + "{[Employees].[Sheri Nowmer].[Maya Gutierrez].[Brenda Blumberg].[Wayne Banack].[Samuel Agcaoili]}\n"
            + "{[Employees].[Sheri Nowmer].[Maya Gutierrez].[Jonathan Murraiin].[James Thompson].[Samantha Weller]}\n"
            + "Row #0: $40.62\n"
            + "Row #1: $40.31\n"
            + "Row #2: $75.60\n"
            + "Row #3: $40.35\n"
            + "Row #4: $47.52\n"
            + "Row #5: \n"
            + "Row #6: \n");
    }

    public void testNotMatches() {
        assertQueryReturns(
            "SELECT {[Measures].[Store Sales]} ON COLUMNS, "
            + "Filter({[Store Type].MEMBERS}, "
            + "[Store Type].CurrentMember.Name NOT MATCHES "
            + "'.*Grocery.*') ON ROWS "
            + "FROM [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Sales]}\n"
            + "Axis #2:\n"
            + "{[Store Type].[All Store Types]}\n"
            + "{[Store Type].[Deluxe Supermarket]}\n"
            + "{[Store Type].[Gourmet Supermarket]}\n"
            + "{[Store Type].[HeadQuarters]}\n"
            + "{[Store Type].[Supermarket]}\n"
            + "Row #0: 565,238.13\n"
            + "Row #1: 162,062.24\n"
            + "Row #2: 45,750.24\n"
            + "Row #3: \n"
            + "Row #4: 319,210.04\n");
    }

    public void testIn() {
        assertQueryReturns(
            "SELECT {[Measures].[Unit Sales]} ON COLUMNS, "
            + "FILTER([Product].[Product Family].MEMBERS, "
            + "[Product].[Product Family].CurrentMember IN "
            + "{[Product].[All Products].firstChild, "
            + "[Product].[All Products].lastChild}) ON ROWS "
            + "FROM [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Drink]}\n"
            + "{[Product].[Non-Consumable]}\n"
            + "Row #0: 24,597\n"
            + "Row #1: 50,236\n");
    }

    public void testNotIn() {
        assertQueryReturns(
            "SELECT {[Measures].[Unit Sales]} ON COLUMNS, "
            + "FILTER([Product].[Product Family].MEMBERS, "
            + "[Product].[Product Family].CurrentMember NOT IN "
            + "{[Product].[All Products].firstChild, "
            + "[Product].[All Products].lastChild}) ON ROWS "
            + "FROM [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Food]}\n"
            + "Row #0: 191,940\n");
    }

    public void testChildMemberIn() {
        assertQueryReturns(
            "SELECT {[Measures].[Store Sales]} ON COLUMNS, "
            + "{[Store].[Store Name].MEMBERS} ON ROWS "
            + "FROM [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Sales]}\n"
            + "Axis #2:\n"
            + "{[Store].[Canada].[BC].[Vancouver].[Store 19]}\n"
            + "{[Store].[Canada].[BC].[Victoria].[Store 20]}\n"
            + "{[Store].[Mexico].[DF].[Mexico City].[Store 9]}\n"
            + "{[Store].[Mexico].[DF].[San Andres].[Store 21]}\n"
            + "{[Store].[Mexico].[Guerrero].[Acapulco].[Store 1]}\n"
            + "{[Store].[Mexico].[Jalisco].[Guadalajara].[Store 5]}\n"
            + "{[Store].[Mexico].[Veracruz].[Orizaba].[Store 10]}\n"
            + "{[Store].[Mexico].[Yucatan].[Merida].[Store 8]}\n"
            + "{[Store].[Mexico].[Zacatecas].[Camacho].[Store 4]}\n"
            + "{[Store].[Mexico].[Zacatecas].[Hidalgo].[Store 12]}\n"
            + "{[Store].[Mexico].[Zacatecas].[Hidalgo].[Store 18]}\n"
            + "{[Store].[USA].[CA].[Alameda].[HQ]}\n"
            + "{[Store].[USA].[CA].[Beverly Hills].[Store 6]}\n"
            + "{[Store].[USA].[CA].[Los Angeles].[Store 7]}\n"
            + "{[Store].[USA].[CA].[San Diego].[Store 24]}\n"
            + "{[Store].[USA].[CA].[San Francisco].[Store 14]}\n"
            + "{[Store].[USA].[OR].[Portland].[Store 11]}\n"
            + "{[Store].[USA].[OR].[Salem].[Store 13]}\n"
            + "{[Store].[USA].[WA].[Bellingham].[Store 2]}\n"
            + "{[Store].[USA].[WA].[Bremerton].[Store 3]}\n"
            + "{[Store].[USA].[WA].[Seattle].[Store 15]}\n"
            + "{[Store].[USA].[WA].[Spokane].[Store 16]}\n"
            + "{[Store].[USA].[WA].[Tacoma].[Store 17]}\n"
            + "{[Store].[USA].[WA].[Walla Walla].[Store 22]}\n"
            + "{[Store].[USA].[WA].[Yakima].[Store 23]}\n"
            + "Row #0: \n"
            + "Row #1: \n"
            + "Row #2: \n"
            + "Row #3: \n"
            + "Row #4: \n"
            + "Row #5: \n"
            + "Row #6: \n"
            + "Row #7: \n"
            + "Row #8: \n"
            + "Row #9: \n"
            + "Row #10: \n"
            + "Row #11: \n"
            + "Row #12: 45,750.24\n"
            + "Row #13: 54,545.28\n"
            + "Row #14: 54,431.14\n"
            + "Row #15: 4,441.18\n"
            + "Row #16: 55,058.79\n"
            + "Row #17: 87,218.28\n"
            + "Row #18: 4,739.23\n"
            + "Row #19: 52,896.30\n"
            + "Row #20: 52,644.07\n"
            + "Row #21: 49,634.46\n"
            + "Row #22: 74,843.96\n"
            + "Row #23: 4,705.97\n"
            + "Row #24: 24,329.23\n");

        // test when the member arg is at a different level
        // from the set argument
        assertQueryReturns(
            "SELECT {[Measures].[Store Sales]} ON COLUMNS, "
            + "Filter({[Store].[Store Name].MEMBERS}, "
            + "[Store].[Store Name].CurrentMember IN "
            + "{[Store].[All Stores].[Mexico], "
            + "[Store].[All Stores].[USA]}) ON ROWS "
            + "FROM [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Sales]}\n"
            + "Axis #2:\n");
    }

    /**
     * Tests that the inferred return type is correct for a UDF whose return
     * type is not the same as would be guessed by the default implementation
     * of {@link mondrian.olap.fun.FunDefBase#getResultType}, which simply
     * guesses based on the type of the first argument.
     */
    public void testNonGuessableReturnType() {
        TestContext tc = TestContext.create(
            null,
            null,
            null,
            null,
            "<UserDefinedFunction name=\"StringMult\" className=\""
            + StringMultUdf.class.getName()
            + "\"/>\n",
            null);
        // The default implementation of getResultType would assume that
        // StringMult(int, string) returns an int, whereas it returns a string.
        tc.assertExprReturns(
            "StringMult(5, 'foo') || 'bar'", "foofoofoofoofoobar");
    }

    /**
     * Test case for the problem where a string expression gave a
     * ClassCastException because it was evaluating to a member, whereas the
     * member should have been evaluated to a scalar.
     */
    public void testUdfToString() {
        TestContext tc = TestContext.create(
            null,
            null,
            null,
            null,
            "<UserDefinedFunction name=\"StringMult\" className=\""
            + StringMultUdf.class.getName()
            + "\"/>\n",
            null);
        tc.assertQueryReturns(
            "with member [Measures].[ABC] as StringMult(1, 'A')\n"
            + "member [Measures].[Unit Sales Formatted] as\n"
            + "  [Measures].[Unit Sales],\n"
            + "  FORMAT_STRING = '#,###|color=' ||\n"
            + "      Iif([Measures].[ABC] = 'A', 'red', 'green')\n"
            + "select [Measures].[Unit Sales Formatted] on 0\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales Formatted]}\n"
            + "Row #0: 266,773|color=red\n");
    }

    /**
     * Tests a UDF whose return type is not the same as its first
     * parameter. The return type needs to have full dimensional information;
     * in this case, HierarchyType(dimension=Time, hierarchy=unknown).
     *
     * <p>Also tests applying a UDF to arguments of coercible type. In this
     * case, applies f(member,dimension) to args(member,hierarchy).
     */
    public void testAnotherMemberFun() {
        final TestContext tc = TestContext.create(
            null,
            null,
            null,
            null,
            "<UserDefinedFunction name=\"PlusOne\" className=\""
            + PlusOneUdf.class.getName() + "\"/>\n"
            + "<UserDefinedFunction name=\"AnotherMemberError\" className=\""
            + AnotherMemberErrorUdf.class.getName() + "\"/>",
            null);

        tc.assertQueryReturns(
            "WITH MEMBER [Measures].[Test] AS "
            + "'([Measures].[Store Sales],[Product].[Food],AnotherMemberError([Product].[Drink],[Time].[Time]))'\n"
            + "SELECT {[Measures].[Test]} ON COLUMNS, \n"
            + "  {[Customers].DefaultMember} ON ROWS \n"
            + "FROM [Sales]",

            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Test]}\n"
            + "Axis #2:\n"
            + "{[Customers].[All Customers]}\n"
            + "Row #0: 409,035.59\n");
    }


    public void testCachingCurrentDate() {
        assertQueryReturns(
            "SELECT {filter([Time].[Month].Members, "
            + "[Time].[Time].CurrentMember in {CurrentDateMember([Time].[Time], '[\"Time\"]\\.[yyyy]\\.[\"Q\"q]\\.[m]', BEFORE)})} ON COLUMNS "
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time].[1998].[Q4].[12]}\n"
            + "Row #0: \n");
    }

    /**
     * Test case for a UDF that returns a list.
     *
     * <p>Test case for bug
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-588">MONDRIAN-588,
     * "UDF returning List works under 2.4, fails under 3.1.1"</a>.
     *
     * <p>Also test case for bug
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-589">MONDRIAN-589,
     * "UDF expecting List gets anonymous
     * mondrian.rolap.RolapNamedSetEvaluator$1 instead"</a>.
     */
    public void testListUdf() {
        checkListUdf(ReverseFunction.class);
        checkListUdf(ReverseIterableFunction.class);
    }

    /**
     * Helper for {@link #testListUdf()}.
     *
     * @param functionClass Class that implements the "Reverse" function.
     */
    private void checkListUdf(
        final Class<? extends ReverseFunction> functionClass)
    {
        TestContext tc = TestContext.create(
            null,
            null,
            null,
            null,
            "<UserDefinedFunction name=\"Reverse\" className=\""
            + functionClass.getName()
            + "\"/>\n",
            null);
        final String expectedResult =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Gender].[M]}\n"
            + "{[Gender].[F]}\n"
            + "{[Gender].[All Gender]}\n"
            + "Row #0: 135,215\n"
            + "Row #0: 131,558\n"
            + "Row #0: 266,773\n";
        // UDF called directly in axis expression.
        tc.assertQueryReturns(
            "select Reverse([Gender].Members) on 0\n"
            + "from [Sales]",
            expectedResult);
        // UDF as calc set definition
        tc.assertQueryReturns(
            "with set [Foo] as Reverse([Gender].Members)\n"
            + "select [Foo] on 0\n"
            + "from [Sales]",
            expectedResult);
        // UDF applied to calc set -- exhibited MONDRIAN-589
        tc.assertQueryReturns(
            "with set [Foo] as [Gender].Members\n"
            + "select Reverse([Foo]) on 0\n"
            + "from [Sales]",
            expectedResult);
    }

    /**
     * Tests that a non-static function gives an error.
     */
    public void testNonStaticUdfFails() {
        if (Util.PreJdk15) {
            // Cannot detect non-static inner classes in JDK 1.4, because
            // such things are not supposed to exist.
            return;
        }
        TestContext tc = TestContext.create(
            null,
            null,
            null,
            null,
            "<UserDefinedFunction name=\"Reverse2\" className=\""
            + ReverseFunctionNotStatic.class.getName()
            + "\"/>\n",
            null);
        tc.assertQueryThrows(
            "select Reverse2([Gender].Members) on 0\n"
            + "from [Sales]",
            "Failed to load user-defined function 'Reverse2': class "
            + "'mondrian.test.UdfTest$ReverseFunctionNotStatic' must be public "
            + "and static");
    }

    /**
     * Tests a function that takes a member as argument. Want to make sure that
     * Mondrian leaves it as a member, does not try to evaluate it to a scalar
     * value.
     */
    public void testMemberUdfDoesNotEvaluateToScalar() {
        TestContext tc = TestContext.create(
            null,
            null,
            null,
            null,
            "<UserDefinedFunction name=\"MemberName\" className=\""
            + MemberNameFunction.class.getName()
            + "\"/>\n",
            null);
        tc.assertExprReturns(
            "MemberName([Gender].[F])",
            "F");
    }

    // ~ Inner classes --------------------------------------------------------


    /**
     * A simple user-defined function which adds one to its argument.
     */
    public static class PlusOneUdf implements UserDefinedFunction {
        public String getName() {
            return "PlusOne";
        }

        public String getDescription() {
            return "Returns its argument plus one";
        }

        public Syntax getSyntax() {
            return Syntax.Function;
        }

        public Type getReturnType(Type[] parameterTypes) {
            return new NumericType();
        }

        public Type[] getParameterTypes() {
            return new Type[] {new NumericType()};
        }

        public Object execute(Evaluator evaluator, Argument[] arguments) {
            final Object argValue = arguments[0].evaluateScalar(evaluator);
            if (argValue instanceof Number) {
                return ((Number) argValue).doubleValue() + 1.0;
            } else {
                // Argument might be a RuntimeException indicating that
                // the cache does not yet have the required cell value. The
                // function will be called again when the cache is loaded.
                return null;
            }
        }

        public String[] getReservedWords() {
            return null;
        }
    }

    /**
     * A simple user-defined function which adds one to its argument.
     */
    public static class BadPlusOneUdf extends PlusOneUdf {
        private final String name;

        public BadPlusOneUdf(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public Type getReturnType(Type[] parameterTypes) {
            // Will cause error.
            return null;
        }
    }

    /**
     * A user-defined function which, depending on its given name, either adds
     * one to, or subtracts one from, its argument.
     */
    public static class PlusOrMinusOneUdf implements UserDefinedFunction {
        private final String name;

        public PlusOrMinusOneUdf(String name) {
            if (!(name.equals("GenericPlusOne")
                  || name.equals("GenericMinusOne")))
            {
                throw new IllegalArgumentException();
            }
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public String getDescription() {
            return
                "A user-defined function which, depending on its given name, "
                + "either addsone to, or subtracts one from, its argument";
        }

        public Syntax getSyntax() {
            return Syntax.Function;
        }

        public Type getReturnType(Type[] parameterTypes) {
            return new NumericType();
        }

        public String[] getReservedWords() {
            return null;
        }

        public Type[] getParameterTypes() {
            return new Type[] {new NumericType()};
        }

        public Object execute(Evaluator evaluator, Argument[] arguments) {
            final Object argValue = arguments[0].evaluateScalar(evaluator);
            if (argValue instanceof Number) {
                return ((Number) argValue).doubleValue()
                   + (name.equals("GenericPlusOne") ? 1.0 : -1.0);
            } else {
                // Argument might be a RuntimeException indicating that
                // the cache does not yet have the required cell value. The
                // function will be called again when the cache is loaded.
                return null;
            }
        }
    }

    /**
     * The "TimesString" user-defined function. We wanted a function whose
     * actual return type (string) is not the same as the guessed return type
     * (integer).
     */
    public static class StringMultUdf implements UserDefinedFunction {
        public String getName() {
            return "StringMult";
        }

        public String getDescription() {
            return "Returns N copies of its string argument";
        }

        public Syntax getSyntax() {
            return Syntax.Function;
        }

        public Type getReturnType(Type[] parameterTypes) {
            return new StringType();
        }

        public Type[] getParameterTypes() {
            return new Type[] {
                new NumericType(), new StringType()
            };
        }

        public Object execute(Evaluator evaluator, Argument[] arguments) {
            final Object argValue = arguments[0].evaluateScalar(evaluator);
            int n;
            if (argValue instanceof Number) {
                n = ((Number) argValue).intValue();
            } else {
                // Argument might be a RuntimeException indicating that
                // the cache does not yet have the required cell value. The
                // function will be called again when the cache is loaded.
                return null;
            }
            String s;
            final Object argValue2 = arguments[1].evaluateScalar(evaluator);
            if (argValue2 instanceof String) {
                s = (String) argValue2;
            } else {
                return null;
            }
            if (n < 0) {
                return null;
            }
            StringBuilder buf = new StringBuilder(s.length() * n);
            for (int i = 0; i < n; i++) {
                buf.append(s);
            }
            return buf.toString();
        }

        public String[] getReservedWords() {
            return null;
        }
    }

    /**
     * A user-defined function which returns ignores its first parameter (a
     * member) and returns the default member from the second parameter (a
     * hierarchy).
     */
    public static class AnotherMemberErrorUdf implements UserDefinedFunction {
        public String getName() {
            return "AnotherMemberError";
        }

        public String getDescription() {
            return "Returns default member from hierarchy, "
                + "specified as a second parameter. "
                + "First parameter - any member from any hierarchy";
        }

        public Syntax getSyntax() {
            return Syntax.Function;
        }

        public Type getReturnType(Type[] parameterTypes) {
            HierarchyType hierType = (HierarchyType) parameterTypes[1];
            return MemberType.forType(hierType);
        }

        public Type[] getParameterTypes() {
            return new Type[] {
                // The first argument must be a member.
                MemberType.Unknown,
                // The second argument must be a hierarchy.
                HierarchyType.Unknown
            };
        }

        public Object execute(Evaluator evaluator, Argument[] arguments) {
            // Simply ignore first parameter
            Member member = (Member)arguments[0].evaluate(evaluator);
            Util.discard(member);
            Hierarchy hierarchy = (Hierarchy)arguments[1].evaluate(evaluator);
            return hierarchy.getDefaultMember();
        }

        public String[] getReservedWords() {
            return null;
        }
    }

    /**
     * Function that reverses a list of members.
     */
    public static class ReverseFunction implements UserDefinedFunction {
        public Object execute(Evaluator eval, Argument[] args) {
            // Note: must call Argument.evaluateList. If we call
            // Argument.evaluate we may get an Iterable.
            List<?> list = args[0].evaluateList(eval);
            // We do not need to copy before we reverse. The list is guaranteed
            // to be mutable.
            Collections.reverse(list);
            return list;
        }

        public String getDescription() {
            return "Reverses the order of a set";
        }

        public String getName() {
            return "Reverse";
        }

        public Type[] getParameterTypes() {
            return new Type[] {new SetType(MemberType.Unknown)};
        }

        public String[] getReservedWords() {
            return null;
        }

        public Type getReturnType(Type[] arg0) {
            return arg0[0];
        }

        public Syntax getSyntax() {
            return Syntax.Function;
        }
    }

    /**
     * Function that is non-static.
     */
    public class ReverseFunctionNotStatic extends ReverseFunction {
    }

    /**
     * Function that takes a set of members as argument, and returns a set of
     * members.
     */
    public static class ReverseIterableFunction extends ReverseFunction {
        public Object execute(Evaluator eval, Argument[] args) {
            // Note: must call Argument.evaluateList. If we call
            // Argument.evaluate we may get an Iterable.
            Iterable iterable = args[0].evaluateIterable(eval);
            List<Object> list = new ArrayList<Object>();
            for (Object o : iterable) {
                list.add(o);
            }
            Collections.reverse(list);
            return list;
        }
    }

    /**
     * Function that takes a member and returns a name.
     */
    public static class MemberNameFunction implements UserDefinedFunction {
        public Object execute(Evaluator eval, Argument[] args) {
            Member member = (Member) args[0].evaluate(eval);
            return member.getName();
        }

        public String getDescription() {
            return "Returns the name of a member";
        }

        public String getName() {
            return "MemberName";
        }

        public Type[] getParameterTypes() {
            return new Type[] {MemberType.Unknown};
        }

        public String[] getReservedWords() {
            return null;
        }

        public Type getReturnType(Type[] arg0) {
            return new StringType();
        }

        public Syntax getSyntax() {
            return Syntax.Function;
        }
    }
}

// End UdfTest.java
