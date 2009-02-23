/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2008 Julian Hyde
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
        null, null,
        "<UserDefinedFunction name=\"PlusOne\" className=\"" +
            PlusOneUdf.class.getName() + "\"/>" + nl,
        null);

    public TestContext getTestContext() {
        return tc;
    }

    public void testSanity() {
        // sanity check, make sure the schema is loading correctly
        assertQueryReturns(
                "SELECT {[Measures].[Store Sqft]} ON COLUMNS, {[Store Type]} ON ROWS FROM [Store]",
                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Store Sqft]}" + nl +
                "Axis #2:" + nl +
                "{[Store Type].[All Store Types]}" + nl +
                "Row #0: 571,596" + nl);
    }

    public void testFun() {
        assertQueryReturns(
                "WITH MEMBER [Measures].[Sqft Plus One] AS 'PlusOne([Measures].[Store Sqft])'" + nl +
                "SELECT {[Measures].[Sqft Plus One]} ON COLUMNS, " + nl +
                "  {[Store Type].children} ON ROWS " + nl +
                "FROM [Store]",

                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Sqft Plus One]}" + nl +
                "Axis #2:" + nl +
                "{[Store Type].[All Store Types].[Deluxe Supermarket]}" + nl +
                "{[Store Type].[All Store Types].[Gourmet Supermarket]}" + nl +
                "{[Store Type].[All Store Types].[HeadQuarters]}" + nl +
                "{[Store Type].[All Store Types].[Mid-Size Grocery]}" + nl +
                "{[Store Type].[All Store Types].[Small Grocery]}" + nl +
                "{[Store Type].[All Store Types].[Supermarket]}" + nl +
                "Row #0: 146,046" + nl +
                "Row #1: 47,448" + nl +
                "Row #2: " + nl +
                "Row #3: 109,344" + nl +
                "Row #4: 75,282" + nl +
                "Row #5: 193,481" + nl);
    }

    public void testLastNonEmpty() {
        assertQueryReturns(
                "WITH MEMBER [Measures].[Last Unit Sales] AS " + nl +
                " '([Measures].[Unit Sales], " + nl +
                "   LastNonEmpty(Descendants([Time]), [Measures].[Unit Sales]))'" + nl +
                "SELECT {[Measures].[Last Unit Sales]} ON COLUMNS," + nl +
                " CrossJoin(" + nl +
                "  {[Time].[1997], [Time].[1997].[Q1], [Time].[1997].[Q1].Children}," + nl +
                "  {[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].children}) ON ROWS" + nl +
                "FROM [Sales]" + nl +
                "WHERE ([Store].[All Stores].[USA].[OR].[Portland].[Store 11])",
                fold(
                    "Axis #0:\n" +
                    "{[Store].[All Stores].[USA].[OR].[Portland].[Store 11]}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Last Unit Sales]}\n" +
                    "Axis #2:\n" +
                    "{[Time].[1997], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good]}\n" +
                    "{[Time].[1997], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Pearl]}\n" +
                    "{[Time].[1997], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth]}\n" +
                    "{[Time].[1997], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Top Measure]}\n" +
                    "{[Time].[1997], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Walrus]}\n" +
                    "{[Time].[1997].[Q1], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good]}\n" +
                    "{[Time].[1997].[Q1], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Pearl]}\n" +
                    "{[Time].[1997].[Q1], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth]}\n" +
                    "{[Time].[1997].[Q1], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Top Measure]}\n" +
                    "{[Time].[1997].[Q1], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Walrus]}\n" +
                    "{[Time].[1997].[Q1].[1], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good]}\n" +
                    "{[Time].[1997].[Q1].[1], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Pearl]}\n" +
                    "{[Time].[1997].[Q1].[1], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth]}\n" +
                    "{[Time].[1997].[Q1].[1], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Top Measure]}\n" +
                    "{[Time].[1997].[Q1].[1], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Walrus]}\n" +
                    "{[Time].[1997].[Q1].[2], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good]}\n" +
                    "{[Time].[1997].[Q1].[2], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Pearl]}\n" +
                    "{[Time].[1997].[Q1].[2], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth]}\n" +
                    "{[Time].[1997].[Q1].[2], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Top Measure]}\n" +
                    "{[Time].[1997].[Q1].[2], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Walrus]}\n" +
                    "{[Time].[1997].[Q1].[3], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good]}\n" +
                    "{[Time].[1997].[Q1].[3], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Pearl]}\n" +
                    "{[Time].[1997].[Q1].[3], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth]}\n" +
                    "{[Time].[1997].[Q1].[3], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Top Measure]}\n" +
                    "{[Time].[1997].[Q1].[3], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Walrus]}\n" +
                    "Row #0: 2\n" +
                    "Row #1: 7\n" +
                    "Row #2: 6\n" +
                    "Row #3: 7\n" +
                    "Row #4: 4\n" +
                    "Row #5: 3\n" +
                    "Row #6: 4\n" +
                    "Row #7: 3\n" +
                    "Row #8: 4\n" +
                    "Row #9: 2\n" +
                    "Row #10: \n" +
                    "Row #11: 4\n" +
                    "Row #12: \n" +
                    "Row #13: 2\n" +
                    "Row #14: \n" +
                    "Row #15: \n" +
                    "Row #16: 2\n" +
                    "Row #17: \n" +
                    "Row #18: 4\n" +
                    "Row #19: \n" +
                    "Row #20: 3\n" +
                    "Row #21: 4\n" +
                    "Row #22: 3\n" +
                    "Row #23: 4\n" +
                    "Row #24: 2\n"));
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
            "with\n" +
            "     member\n" +
            "     [Measures].[Last Sale] as ([Measures].[Unit Sales],\n" +
            "         LastNonEmpty(Descendants([Time].CurrentMember, [Time].[Month]),\n" +
            "         [Measures].[Unit Sales]))\n" +
            "select\n" +
            "     NON EMPTY {[Measures].[Last Sale]} ON columns,\n" +
            "     NON EMPTY Order([Store].[All Stores].Children,\n" +
            "         [Measures].[Last Sale], DESC) ON rows\n" +
            "from [Sales]\n" +
            "where [Time].LastSibling",
            fold(
                "Axis #0:\n" +
                "{[Time].[1998]}\n" +
                "Axis #1:\n" +
                "Axis #2:\n"));
    }

    public void testBadFun() {
        final TestContext tc = TestContext.create(
            null, null, null, null,
            "<UserDefinedFunction name=\"BadPlusOne\" className=\"" +
                BadPlusOneUdf.class.getName() + "\"/>" + nl,
            null);
        try {
            tc.executeQuery("SELECT {} ON COLUMNS FROM [Sales]");
            fail("Expected exception");
        } catch (Exception e) {
            final String s = e.getMessage();
            assertEquals("Mondrian Error:Internal error: Invalid " +
                    "user-defined function 'BadPlusOne': return type is null",
                    s);
        }
    }

    public void testComplexFun() {
        assertQueryReturns(
                "WITH MEMBER [Measures].[InverseNormal] AS 'InverseNormal([Measures].[Grocery Sqft] / [Measures].[Store Sqft])', FORMAT_STRING = \"0.000\"" + nl +
                "SELECT {[Measures].[InverseNormal]} ON COLUMNS, " + nl +
                "  {[Store Type].children} ON ROWS " + nl +
                "FROM [Store]",

                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[InverseNormal]}" + nl +
                "Axis #2:" + nl +
                "{[Store Type].[All Store Types].[Deluxe Supermarket]}" + nl +
                "{[Store Type].[All Store Types].[Gourmet Supermarket]}" + nl +
                "{[Store Type].[All Store Types].[HeadQuarters]}" + nl +
                "{[Store Type].[All Store Types].[Mid-Size Grocery]}" + nl +
                "{[Store Type].[All Store Types].[Small Grocery]}" + nl +
                "{[Store Type].[All Store Types].[Supermarket]}" + nl +
                "Row #0: 0.467" + nl +
                "Row #1: 0.463" + nl +
                "Row #2: " + nl +
                "Row #3: 0.625" + nl +
                "Row #4: 0.521" + nl +
                "Row #5: 0.504" + nl);
    }

    public void testException() {
        Result result = executeQuery("WITH MEMBER [Measures].[InverseNormal] " +
                        " AS 'InverseNormal([Measures].[Store Sqft] / [Measures].[Grocery Sqft])'," +
                        " FORMAT_STRING = \"0.000000\"" + nl +
                        "SELECT {[Measures].[InverseNormal]} ON COLUMNS, " + nl +
                        "  {[Store Type].children} ON ROWS " + nl +
                        "FROM [Store]");
        Axis rowAxis = result.getAxes()[0];
        assertTrue(rowAxis.getPositions().size() == 1);
        Axis colAxis = result.getAxes()[1];
        assertTrue(colAxis.getPositions().size() == 6);
        Cell cell = result.getCell(new int[] {0, 0});
        assertTrue(cell.isError());
        getTestContext().assertMatchesVerbose(
                Pattern.compile(".*Invalid value for inverse normal distribution: 1.4708.*"),
                cell.getValue().toString());
        cell = result.getCell(new int[] {0, 5});
        assertTrue(cell.isError());
        getTestContext().assertMatchesVerbose(
                Pattern.compile(".*Invalid value for inverse normal distribution: 1.4435.*"),
                cell.getValue().toString());
    }

    public void testCurrentDateString()
    {
        String actual = executeExpr("CurrentDateString(\"Ddd mmm dd yyyy\")");
        Date currDate = new Date();
        String dateString = currDate.toString();
        String expected =
            dateString.substring(0, 11) +
            dateString.substring(dateString.length() - 4);
        assertEquals(expected, actual);
    }

    public void testCurrentDateMemberBefore()
    {
        assertQueryReturns(
            "SELECT { CurrentDateMember([Time], " +
            "\"[Ti\\me]\\.[yyyy]\\.[Qq]\\.[m]\", BEFORE)} " +
            "ON COLUMNS FROM [Sales]",
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Time].[1998].[Q4].[12]}\n" +
                "Row #0: \n"));
    }

    public void testCurrentDateMemberBeforeUsingQuotes()
    {
        assertAxisReturns(
            "CurrentDateMember([Time], '\"[Time].[\"yyyy\"].[Q\"q\"].[\"m\"]\"', BEFORE)",
            "[Time].[1998].[Q4].[12]");
    }

    public void testCurrentDateMemberAfter()
    {
        // CurrentDateMember will return null member since the latest date in
        // FoodMart is from '98
        assertQueryReturns(
            "SELECT { CurrentDateMember([Time], " +
            "\"[Ti\\me]\\.[yyyy]\\.[Qq]\\.[m]\", AFTER)} " +
            "ON COLUMNS FROM [Sales]",
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n"));
    }

    public void testCurrentDateMemberExact()
    {
        // CurrentDateMember will return null member since the latest date in
        // FoodMart is from '98; apply a function on the return value to
        // ensure null member instead of null is returned
        assertQueryReturns(
            "SELECT { CurrentDateMember([Time], " +
            "\"[Ti\\me]\\.[yyyy]\\.[Qq]\\.[m]\", EXACT).lag(1)} " +
            "ON COLUMNS FROM [Sales]",
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n"));
    }

    public void testCurrentDateMemberNoFindArg()
    {
        // CurrentDateMember will return null member since the latest date in
        // FoodMart is from '98
        assertQueryReturns(
            "SELECT { CurrentDateMember([Time], " +
            "\"[Ti\\me]\\.[yyyy]\\.[Qq]\\.[m]\")} " +
            "ON COLUMNS FROM [Sales]",
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n"));
    }

    public void testCurrentDateMemberHierarchy()
    {
        final String query =
            MondrianProperties.instance().SsasCompatibleNaming.get()
                ? "SELECT { CurrentDateMember([Time.Weekly], " +
                "\"[Ti\\me\\.Weekl\\y]\\.[All Weekl\\y\\s]\\.[yyyy]\\.[ww]\", BEFORE)} " +
                "ON COLUMNS FROM [Sales]"
                : "SELECT { CurrentDateMember([Time.Weekly], " +
                "\"[Ti\\me\\.Weekl\\y]\\.[All Ti\\me\\.Weekl\\y\\s]\\.[yyyy]\\.[ww]\", BEFORE)} " +
                "ON COLUMNS FROM [Sales]";
        assertQueryReturns(
            query,
            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Time].[Weekly].[All Weeklys].[1998].[52]}\n" +
                "Row #0: \n"));
    }

    public void testCurrentDateMemberHierarchyNullReturn()
    {
        // CurrentDateMember will return null member since the latest date in
        // FoodMart is from '98; note that first arg is a hierarchy rather
        // than a dimension
        assertQueryReturns(
            "SELECT { CurrentDateMember([Time.Weekly], " +
            "\"[Ti\\me]\\.[yyyy]\\.[Qq]\\.[m]\")} " +
            "ON COLUMNS FROM [Sales]",
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n"));
    }

    public void testCurrentDateMemberRealAfter()
    {
        // omit formatting characters from the format so the current date
        // is hard-coded to actual value in the database so we can test the
        // after logic
        assertQueryReturns(
            "SELECT { CurrentDateMember([Time], " +
            "\"[Ti\\me]\\.[1996]\\.[Q4]\", after)} " +
            "ON COLUMNS FROM [Sales]",
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Time].[1997].[Q1]}\n" +
                "Row #0: 66,291\n"));
    }

    public void testCurrentDateMemberRealExact1()
    {
        // omit formatting characters from the format so the current date
        // is hard-coded to actual value in the database so we can test the
        // exact logic
        assertQueryReturns(
            "SELECT { CurrentDateMember([Time], " +
            "\"[Ti\\me]\\.[1997]\")} " +
            "ON COLUMNS FROM [Sales]",
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Time].[1997]}\n" +
                "Row #0: 266,773\n"));
    }

    public void testCurrentDateMemberRealExact2()
    {
        // omit formatting characters from the format so the current date
        // is hard-coded to actual value in the database so we can test the
        // exact logic
        assertQueryReturns(
            "SELECT { CurrentDateMember([Time], " +
            "\"[Ti\\me]\\.[1997]\\.[Q2]\\.[5]\")} " +
            "ON COLUMNS FROM [Sales]",
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Time].[1997].[Q2].[5]}\n" +
                "Row #0: 21,081\n"));
    }

    public void testCurrentDateMemberPrev()
    {
        // apply a function on the result of the UDF
        assertQueryReturns(
            "SELECT { CurrentDateMember([Time], " +
            "\"[Ti\\me]\\.[yyyy]\\.[Qq]\\.[m]\", BEFORE).PrevMember} " +
            "ON COLUMNS FROM [Sales]",
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Time].[1998].[Q4].[11]}\n" +
                "Row #0: \n"));
    }

    public void testCurrentDateLag()
    {
        // Also, try a different style of quoting, because single quote followed
        // by double quote (used in other examples) is difficult to read.
        assertQueryReturns(
            "SELECT\n" +
                "    { [Measures].[Unit Sales] } ON COLUMNS,\n" +
                "    { CurrentDateMember([Time], '[\"Time\"]\\.[yyyy]\\.[\"Q\"q]\\.[m]', BEFORE).Lag(3) : " +
                "      CurrentDateMember([Time], '[\"Time\"]\\.[yyyy]\\.[\"Q\"q]\\.[m]', BEFORE) } ON ROWS\n" +
                "FROM [Sales]",
            fold(
                "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Unit Sales]}\n" +
                    "Axis #2:\n" +
                    "{[Time].[1998].[Q3].[9]}\n" +
                    "{[Time].[1998].[Q4].[10]}\n" +
                    "{[Time].[1998].[Q4].[11]}\n" +
                    "{[Time].[1998].[Q4].[12]}\n" +
                    "Row #0: \n" +
                    "Row #1: \n" +
                    "Row #2: \n" +
                    "Row #3: \n"));
    }

    public void testMatches()
    {
        assertQueryReturns(
            "SELECT {[Measures].[Org Salary]} ON COLUMNS, " +
                "Filter({[Employees].MEMBERS}, " +
                "[Employees].CurrentMember.Name MATCHES '(?i)sam.*') ON ROWS " +
                "FROM [HR]",
            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Org Salary]}\n" +
                "Axis #2:\n" +
                "{[Employees].[All Employees].[Sheri Nowmer].[Derrick Whelply].[Beverly Baker].[Jacqueline Wyllie].[Ralph Mccoy].[Anne Tuck].[Samuel Johnson]}\n" +
                "{[Employees].[All Employees].[Sheri Nowmer].[Derrick Whelply].[Pedro Castillo].[Jose Bernard].[Mary Hunt].[Bonnie Bruno].[Sam Warren]}\n" +
                "{[Employees].[All Employees].[Sheri Nowmer].[Derrick Whelply].[Pedro Castillo].[Charles Macaluso].[Barbara Wallin].[Michael Suggs].[Sam Adair]}\n" +
                "{[Employees].[All Employees].[Sheri Nowmer].[Derrick Whelply].[Pedro Castillo].[Lois Wood].[Dell Gras].[Kristine Aldred].[Sam Zeller]}\n" +
                "{[Employees].[All Employees].[Sheri Nowmer].[Derrick Whelply].[Laurie Borges].[Cody Goldey].[Shanay Steelman].[Neal Hasty].[Sam Wheeler]}\n" +
                "{[Employees].[All Employees].[Sheri Nowmer].[Maya Gutierrez].[Brenda Blumberg].[Wayne Banack].[Samuel Agcaoili]}\n" +
                "{[Employees].[All Employees].[Sheri Nowmer].[Maya Gutierrez].[Jonathan Murraiin].[James Thompson].[Samantha Weller]}\n" +
                "Row #0: $40.62\n" +
                "Row #1: $40.31\n" +
                "Row #2: $75.60\n" +
                "Row #3: $40.35\n" +
                "Row #4: $47.52\n" +
                "Row #5: \n" +
                "Row #6: \n"));
    }

    public void testNotMatches()
    {
        assertQueryReturns(
            "SELECT {[Measures].[Store Sales]} ON COLUMNS, " +
            "Filter({[Store Type].MEMBERS}, " +
            "[Store Type].CurrentMember.Name NOT MATCHES " +
            "'.*Grocery.*') ON ROWS " +
            "FROM [Sales]",
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Store Sales]}\n" +
                "Axis #2:\n" +
                "{[Store Type].[All Store Types]}\n" +
                "{[Store Type].[All Store Types].[Deluxe Supermarket]}\n" +
                "{[Store Type].[All Store Types].[Gourmet Supermarket]}\n" +
                "{[Store Type].[All Store Types].[HeadQuarters]}\n" +
                "{[Store Type].[All Store Types].[Supermarket]}\n" +
                "Row #0: 565,238.13\n" +
                "Row #1: 162,062.24\n" +
                "Row #2: 45,750.24\n" +
                "Row #3: \n" +
                "Row #4: 319,210.04\n"));
    }

    public void testIn()
    {
        assertQueryReturns(
            "SELECT {[Measures].[Unit Sales]} ON COLUMNS, " +
            "FILTER([Product].[Product Family].MEMBERS, " +
            "[Product].[Product Family].CurrentMember IN " +
            "{[Product].[All Products].firstChild, " +
            "[Product].[All Products].lastChild}) ON ROWS " +
            "FROM [Sales]",
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Unit Sales]}\n" +
                "Axis #2:\n" +
                "{[Product].[All Products].[Drink]}\n" +
                "{[Product].[All Products].[Non-Consumable]}\n" +
                "Row #0: 24,597\n" +
                "Row #1: 50,236\n"));
    }

    public void testNotIn()
    {
        assertQueryReturns(
            "SELECT {[Measures].[Unit Sales]} ON COLUMNS, " +
            "FILTER([Product].[Product Family].MEMBERS, " +
            "[Product].[Product Family].CurrentMember NOT IN " +
            "{[Product].[All Products].firstChild, " +
            "[Product].[All Products].lastChild}) ON ROWS " +
            "FROM [Sales]",
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Unit Sales]}\n" +
                "Axis #2:\n" +
                "{[Product].[All Products].[Food]}\n" +
                "Row #0: 191,940\n"));
    }

    public void testChildMemberIn()
    {
        assertQueryReturns(
            "SELECT {[Measures].[Store Sales]} ON COLUMNS, " +
            "{[Store].[Store Name].MEMBERS} ON ROWS " +
            "FROM [Sales]",
            fold(
               "Axis #0:\n" +
               "{}\n" +
               "Axis #1:\n" +
               "{[Measures].[Store Sales]}\n" +
               "Axis #2:\n" +
               "{[Store].[All Stores].[Canada].[BC].[Vancouver].[Store 19]}\n" +
               "{[Store].[All Stores].[Canada].[BC].[Victoria].[Store 20]}\n" +
               "{[Store].[All Stores].[Mexico].[DF].[Mexico City].[Store 9]}\n" +
               "{[Store].[All Stores].[Mexico].[DF].[San Andres].[Store 21]}\n" +
               "{[Store].[All Stores].[Mexico].[Guerrero].[Acapulco].[Store 1]}\n" +
               "{[Store].[All Stores].[Mexico].[Jalisco].[Guadalajara].[Store 5]}\n" +
               "{[Store].[All Stores].[Mexico].[Veracruz].[Orizaba].[Store 10]}\n" +
               "{[Store].[All Stores].[Mexico].[Yucatan].[Merida].[Store 8]}\n" +
               "{[Store].[All Stores].[Mexico].[Zacatecas].[Camacho].[Store 4]}\n" +
               "{[Store].[All Stores].[Mexico].[Zacatecas].[Hidalgo].[Store 12]}\n" +
               "{[Store].[All Stores].[Mexico].[Zacatecas].[Hidalgo].[Store 18]}\n" +
               "{[Store].[All Stores].[USA].[CA].[Alameda].[HQ]}\n" +
               "{[Store].[All Stores].[USA].[CA].[Beverly Hills].[Store 6]}\n" +
               "{[Store].[All Stores].[USA].[CA].[Los Angeles].[Store 7]}\n" +
               "{[Store].[All Stores].[USA].[CA].[San Diego].[Store 24]}\n" +
               "{[Store].[All Stores].[USA].[CA].[San Francisco].[Store 14]}\n" +
               "{[Store].[All Stores].[USA].[OR].[Portland].[Store 11]}\n" +
               "{[Store].[All Stores].[USA].[OR].[Salem].[Store 13]}\n" +
               "{[Store].[All Stores].[USA].[WA].[Bellingham].[Store 2]}\n" +
               "{[Store].[All Stores].[USA].[WA].[Bremerton].[Store 3]}\n" +
               "{[Store].[All Stores].[USA].[WA].[Seattle].[Store 15]}\n" +
               "{[Store].[All Stores].[USA].[WA].[Spokane].[Store 16]}\n" +
               "{[Store].[All Stores].[USA].[WA].[Tacoma].[Store 17]}\n" +
               "{[Store].[All Stores].[USA].[WA].[Walla Walla].[Store 22]}\n" +
               "{[Store].[All Stores].[USA].[WA].[Yakima].[Store 23]}\n" +
               "Row #0: \n" +
               "Row #1: \n" +
               "Row #2: \n" +
               "Row #3: \n" +
               "Row #4: \n" +
               "Row #5: \n" +
               "Row #6: \n" +
               "Row #7: \n" +
               "Row #8: \n" +
               "Row #9: \n" +
               "Row #10: \n" +
               "Row #11: \n" +
               "Row #12: 45,750.24\n" +
               "Row #13: 54,545.28\n" +
               "Row #14: 54,431.14\n" +
               "Row #15: 4,441.18\n" +
               "Row #16: 55,058.79\n" +
               "Row #17: 87,218.28\n" +
               "Row #18: 4,739.23\n" +
               "Row #19: 52,896.30\n" +
               "Row #20: 52,644.07\n" +
               "Row #21: 49,634.46\n" +
               "Row #22: 74,843.96\n" +
               "Row #23: 4,705.97\n" +
               "Row #24: 24,329.23\n"));

        // test when the member arg is at a different level
        // from the set argument
        assertQueryReturns(
            "SELECT {[Measures].[Store Sales]} ON COLUMNS, " +
            "Filter({[Store].[Store Name].MEMBERS}, " +
            "[Store].[Store Name].CurrentMember IN " +
            "{[Store].[All Stores].[Mexico], " +
            "[Store].[All Stores].[USA]}) ON ROWS " +
            "FROM [Sales]",
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Store Sales]}\n" +
                "Axis #2:\n"));
    }

    /**
     * Tests that the inferred return type is correct for a UDF whose return
     * type is not the same as would be guessed by the default implementation
     * of {@link mondrian.olap.fun.FunDefBase#getResultType}, which simply
     * guesses based on the type of the first argument.
     */
    public void testNonGuessableReturnType() {
        TestContext tc = TestContext.create(
            null, null, null, null,
            "<UserDefinedFunction name=\"StringMult\" className=\"" +
                StringMultUdf.class.getName() + "\"/>" + nl,
            null);
        // The default implementation of getResultType would assume that
        // StringMult(int, string) returns an int, whereas it returns a string.
        tc.assertExprReturns(
            "StringMult(5, 'foo') || 'bar'", "foofoofoofoofoobar");
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
            null, null, null, null,
            "<UserDefinedFunction name=\"PlusOne\" className=\"" +
                PlusOneUdf.class.getName() + "\"/>" + nl +
                "<UserDefinedFunction name=\"AnotherMemberError\" className=\"" +
                AnotherMemberErrorUdf.class.getName() + "\"/>",
            null);

        tc.assertQueryReturns(
                "WITH MEMBER [Measures].[Test] AS "+
                "'([Measures].[Store Sales],[Product].[Food],AnotherMemberError([Product].[Drink],[Time]))'" + nl +
                "SELECT {[Measures].[Test]} ON COLUMNS, " + nl +
                "  {[Customers].DefaultMember} ON ROWS " + nl +
                "FROM [Sales]",

                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Test]}" + nl +
                "Axis #2:" + nl +
                "{[Customers].[All Customers]}" + nl +
                "Row #0: 409,035.59" + nl);
    }


    public void testCachingCurrentDate() {
        assertQueryReturns(
            "SELECT {filter([Time].[Month].Members, " +
            "[Time].CurrentMember in {CurrentDateMember([Time], '[\"Time\"]\\.[yyyy]\\.[\"Q\"q]\\.[m]', BEFORE)})} ON COLUMNS " +
            "from [Sales]",
            "Axis #0:" + nl +
            "{}" + nl +
            "Axis #1:" + nl +
            "{[Time].[1998].[Q4].[12]}" + nl +
            "Row #0: " + nl);
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
            return "Returns default member from hierarchy, specified as a second parameter. "+
                "First parameter - any member from any hierarchy";
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
}

// End UdfTest.java
