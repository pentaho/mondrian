/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2003-2006 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import junit.framework.Assert;
import junit.framework.ComparisonFailure;
import mondrian.olap.*;
import mondrian.test.*;
import mondrian.resource.MondrianResource;
import mondrian.calc.*;

import org.eigenbase.xom.StringEscaper;

import java.io.*;
import java.util.List;

/**
 * <code>FunctionTest</code> tests the functions defined in
 * {@link BuiltinFunTable}.
 *
 * @author gjohnson
 * @version $Id$
 */
public class FunctionTest extends FoodMartTestCase {

    private static final String months =
            fold(new String[] {
                "[Time].[1997].[Q1].[1]",
                "[Time].[1997].[Q1].[2]",
                "[Time].[1997].[Q1].[3]",
                "[Time].[1997].[Q2].[4]",
                "[Time].[1997].[Q2].[5]",
                "[Time].[1997].[Q2].[6]",
                "[Time].[1997].[Q3].[7]",
                "[Time].[1997].[Q3].[8]",
                "[Time].[1997].[Q3].[9]",
                "[Time].[1997].[Q4].[10]",
                "[Time].[1997].[Q4].[11]",
                "[Time].[1997].[Q4].[12]"});

    private static final String quarters =
            fold(new String[] {
                "[Time].[1997].[Q1]",
                "[Time].[1997].[Q2]",
                "[Time].[1997].[Q3]",
                "[Time].[1997].[Q4]"});

    private static final String year1997 = "[Time].[1997]";

    private static final String hierarchized1997 =
            fold(new String[] {
                year1997,
                "[Time].[1997].[Q1]",
                "[Time].[1997].[Q1].[1]",
                "[Time].[1997].[Q1].[2]",
                "[Time].[1997].[Q1].[3]",
                "[Time].[1997].[Q2]",
                "[Time].[1997].[Q2].[4]",
                "[Time].[1997].[Q2].[5]",
                "[Time].[1997].[Q2].[6]",
                "[Time].[1997].[Q3]",
                "[Time].[1997].[Q3].[7]",
                "[Time].[1997].[Q3].[8]",
                "[Time].[1997].[Q3].[9]",
                "[Time].[1997].[Q4]",
                "[Time].[1997].[Q4].[10]",
                "[Time].[1997].[Q4].[11]",
                "[Time].[1997].[Q4].[12]"});

    private static final String[] AllDims = {
        "[Measures]",
        "[Store]",
        "[Store Size in SQFT]",
        "[Store Type]",
        "[Time]",
        "[Product]",
        "[Promotion Media]",
        "[Promotions]",
        "[Customers]",
        "[Education Level]",
        "[Gender]",
        "[Marital Status]",
        "[Yearly Income]"
    };

    private static String allDims() {
        return allDimsExcept(new String[0]);
    }

    /**
     * Generates a string containing all dimensions except those given.
     * Useful as an argument to {@link #assertExprDependsOn(String, String)}.
     */
    private static String allDimsExcept(String[] dims) {
        for (int i = 0; i < dims.length; i++) {
            assert contains(AllDims, dims[i]) : "unknown dimension " + dims[i];
        }
        StringBuffer buf = new StringBuffer("{");
        int j = 0;
        for (int i = 0; i < AllDims.length; i++) {
            if (!contains(dims, AllDims[i])) {
                if (j++ > 0) {
                    buf.append(", ");
                }
                buf.append(AllDims[i]);
            }
        }
        buf.append("}");
        return buf.toString();
    }

    private static final String NullNumericExpr = (" ([Measures].[Unit Sales]," +
            "   [Customers].[All Customers].[USA].[CA].[Bellflower], " +
            "   [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Imported Beer])");

    public void testNumericLiteral() {
        assertExprReturns("2", "2");
        if (false) {
            // The test is currently broken because the value 2.5 is formatted
            // as "2". TODO: better default format string
            assertExprReturns("2.5", "2.5");
        }
        assertExprReturns("-10.0", "-10");
        assertExprDependsOn("1.5", "{}");
    }

    public FunctionTest() {
    }
    public FunctionTest(String s) {
        super(s);
    }

    public void testStringLiteral() {
        // single-quoted string
        if (false) {
            // TODO: enhance parser so that you can include a quoted string
            //   inside a WITH MEMBER clause
            assertExprReturns("'foobar'", "foobar");
        }
        // double-quoted string
        assertExprReturns("\"foobar\"", "foobar");
        // literals don't depend on any dimensions
        assertExprDependsOn("\"foobar\"", "{}");
    }

    public void testDimensionHierarchy() {
        assertExprReturns("[Time].Dimension.Name", "Time");
    }

    public void testLevelDimension() {
        assertExprReturns("[Time].[Year].Dimension.UniqueName", "[Time]");
    }

    public void testMemberDimension() {
        assertExprReturns("[Time].[1997].[Q2].Dimension.UniqueName", "[Time]");
    }

    public void testDimensionsNumeric() {
        assertExprDependsOn("Dimensions(2).Name", "{}");
        assertMemberExprDependsOn("Dimensions(3).CurrentMember", allDims());
        assertExprReturns("Dimensions(2).Name", "Store Size in SQFT");
        // bug 1426134 -- Dimensions(0) throws 'Index '0' out of bounds'
        assertExprReturns("Dimensions(0).Name", "Measures");
        assertExprThrows("Dimensions(-1).Name", "Index '-1' out of bounds");
        assertExprThrows("Dimensions(100).Name", "Index '100' out of bounds");
    }

    public void testDimensionsString() {
        assertExprDependsOn("Dimensions(\"foo\").UniqueName", "{}");
        assertMemberExprDependsOn("Dimensions(\"foo\").CurrentMember", allDims());
        assertExprReturns("Dimensions(\"Store\").UniqueName", "[Store]");
    }

    public void testDimensionsDepends() {
        final String expression =
                "Crossjoin(" +
                "{Dimensions(\"Measures\").CurrentMember.Hierarchy.CurrentMember}, " +
                "{Dimensions(\"Product\")})";
        assertAxisReturns(
                expression,
                "{[Measures].[Unit Sales], [Product].[All Products]}");
        assertSetExprDependsOn(expression, allDims());
    }

    public void testTime() {
        assertExprReturns("[Time].[1997].[Q1].[1].Hierarchy.UniqueName", "[Time]");
    }

    public void testBasic9() {
        assertExprReturns("[Gender].[All Gender].[F].Hierarchy.UniqueName", "[Gender]");
    }

    public void testFirstInLevel9() {
        assertExprReturns("[Education Level].[All Education Levels].[Bachelors Degree].Hierarchy.UniqueName", "[Education Level]");
    }

    public void testHierarchyAll() {
        assertExprReturns("[Gender].[All Gender].Hierarchy.UniqueName", "[Gender]");
    }

    public void testNullMember() {
        // MSAS fails here, but Mondrian doesn't.
        assertExprReturns("[Gender].[All Gender].Parent.Level.UniqueName",
                "[Gender].[(All)]");

        // MSAS fails here, but Mondrian doesn't.
        assertExprReturns("[Gender].[All Gender].Parent.Hierarchy.UniqueName",
                "[Gender]");

        // MSAS fails here, but Mondrian doesn't.
        assertExprReturns("[Gender].[All Gender].Parent.Dimension.UniqueName",
                "[Gender]");

        // MSAS returns "" here.
        assertExprReturns("[Gender].[All Gender].Parent.UniqueName",
                "[Gender].[#Null]");

        // MSAS returns "" here.
        assertExprReturns("[Gender].[All Gender].Parent.Name",
                "#Null");

        // MSAS succeeds too
        assertExprReturns("[Gender].[All Gender].Parent.Children.Count",
                "0");
    }

    public void testMemberLevel() {
        assertExprReturns("[Time].[1997].[Q1].[1].Level.UniqueName", "[Time].[Month]");
    }

    public void testLevelsNumeric() {
        assertExprReturns("[Time].Levels(2).Name", "Month");
        assertExprReturns("[Time].Levels(0).Name", "Year");
        assertExprReturns("[Product].Levels(0).Name", "(All)");
    }

    public void testLevelsTooSmall() {
        assertExprThrows("[Time].Levels(-1).Name",
                "Index '-1' out of bounds");
    }

    public void testLevelsTooLarge() {
        assertExprThrows("[Time].Levels(8).Name",
                "Index '8' out of bounds");
    }

    public void testLevelsString() {
        assertExprReturns("Levels(\"[Time].[Year]\").UniqueName", "[Time].[Year]");
    }

    public void testLevelsStringFail() {
        assertExprThrows("Levels(\"nonexistent\").UniqueName",
                "Level 'nonexistent' not found");
    }

    public void testIsEmpty() {
        assertQueryReturns(
                fold(new String[] {
                    "WITH MEMBER [Measures].[Foo] AS 'Iif(IsEmpty([Measures].[Unit Sales]), 5, [Measures].[Unit Sales])'",
                    "SELECT {[Store].[USA].[WA].children} on columns",
                    "FROM Sales",
                    "WHERE ( [Time].[1997].[Q4].[12],",
                    " [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth].[Portsmouth Imported Beer],",
                    " [Measures].[Foo])"}),
                fold(new String[] {
                    "Axis #0:",
                    "{[Time].[1997].[Q4].[12], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth].[Portsmouth Imported Beer], [Measures].[Foo]}",
                    "Axis #1:",
                    "{[Store].[All Stores].[USA].[WA].[Bellingham]}",
                    "{[Store].[All Stores].[USA].[WA].[Bremerton]}",
                    "{[Store].[All Stores].[USA].[WA].[Seattle]}",
                    "{[Store].[All Stores].[USA].[WA].[Spokane]}",
                    "{[Store].[All Stores].[USA].[WA].[Tacoma]}",
                    "{[Store].[All Stores].[USA].[WA].[Walla Walla]}",
                    "{[Store].[All Stores].[USA].[WA].[Yakima]}",
                    "Row #0: 5",
                    "Row #0: 5",
                    "Row #0: 2",
                    "Row #0: 5",
                    "Row #0: 11",
                    "Row #0: 5",
                    "Row #0: 4" + nl}));
    }

    public void testAncestor() {
        Member member = executeSingletonAxis("Ancestor([Store].[USA].[CA].[Los Angeles],[Store Country])");
        Assert.assertEquals("USA", member.getName());

        assertAxisThrows("Ancestor([Store].[USA].[CA].[Los Angeles],[Promotions].[Promotion Name])",
                "Error while executing query");
    }

    public void testAncestorNumeric() {
        Member member = executeSingletonAxis("Ancestor([Store].[USA].[CA].[Los Angeles],1)");
        Assert.assertEquals("CA", member.getName());

        member = executeSingletonAxis("Ancestor([Store].[USA].[CA].[Los Angeles], 0)");
        Assert.assertEquals("Los Angeles", member.getName());

        final TestContext testContextRagged = getTestContext("[Sales Ragged]");
        member = testContextRagged.executeSingletonAxis("Ancestor([Store].[All Stores].[Vatican], 1)");
        Assert.assertEquals("All Stores", member.getName());

        member = testContextRagged.executeSingletonAxis("Ancestor([Store].[USA].[Washington], 1)");
        Assert.assertEquals("USA", member.getName());

        // complicated way to say "1".
        member = testContextRagged.executeSingletonAxis("Ancestor([Store].[USA].[Washington], 7 * 6 - 41)");
        Assert.assertEquals("USA", member.getName());

        member = testContextRagged.executeSingletonAxis("Ancestor([Store].[All Stores].[Vatican], 2)");
        Assert.assertNull("Ancestor at 2 must be null", member);

        member = testContextRagged.executeSingletonAxis("Ancestor([Store].[All Stores].[Vatican], -5)");
        Assert.assertNull("Ancestor at -5 must be null", member);

    }

    public void testAncestorHigher() {
        Member member = executeSingletonAxis("Ancestor([Store].[USA],[Store].[Store City])");
        Assert.assertNull(member); // MSOLAP returns null
    }

    public void testAncestorSameLevel() {
        Member member = executeSingletonAxis("Ancestor([Store].[Canada],[Store].[Store Country])");
        Assert.assertEquals("Canada", member.getName());
    }

    public void testAncestorWrongHierarchy() {
        // MSOLAP gives error "Formula error - dimensions are not
        // valid (they do not match) - in the Ancestor function"
        assertAxisThrows("Ancestor([Gender].[M],[Store].[Store Country])",
                "Error while executing query");
    }

    public void testAncestorAllLevel() {
        Member member = executeSingletonAxis("Ancestor([Store].[USA].[CA],[Store].Levels(0))");
        Assert.assertTrue(member.isAll());
    }

    public void testAncestorWithHiddenParent() {
        final TestContext testContext = getTestContext("[Sales Ragged]");
        Member member = testContext.executeSingletonAxis(
                "Ancestor([Store].[All Stores].[Israel].[Haifa], [Store].[Store Country])");

        assertNotNull("Member must not be null.", member);
        Assert.assertEquals("Israel", member.getName());
    }

    public void testAncestorDepends() {
        assertExprDependsOn("Ancestor([Store].CurrentMember, [Store].[Store Country]).Name",
                "{[Store]}");

        assertExprDependsOn("Ancestor([Store].[All Stores].[USA], [Store].CurrentMember.Level).Name",
                "{[Store]}");

        assertExprDependsOn("Ancestor([Store].[All Stores].[USA], [Store].[Store Country]).Name",
                "{}");

        assertExprDependsOn("Ancestor([Store].CurrentMember, 2+1).Name",
                "{[Store]}");
    }

    public void testOrdinal() {
        final TestContext testContext = getTestContext("Sales Ragged");
        Cell cell = testContext.executeExprRaw("[Store].[All Stores].[Vatican].ordinal");
        assertEquals("Vatican is at level 1.", 1, ((Number)cell.getValue()).intValue());

        cell = testContext.executeExprRaw("[Store].[All Stores].[USA].[Washington].ordinal");
        assertEquals("Washington is at level 3.", 3, ((Number) cell.getValue()).intValue());
    }

    public void testClosingPeriodNoArgs() {
        assertMemberExprDependsOn("ClosingPeriod()", "{[Time]}");
        // MSOLAP returns [1997].[Q4], because [Time].CurrentMember =
        // [1997].
        Member member = executeSingletonAxis("ClosingPeriod()");
        Assert.assertEquals("[Time].[1997].[Q4]", member.getUniqueName());
    }

    public void testClosingPeriodLevel() {
        assertMemberExprDependsOn("ClosingPeriod([Time].[Year])", "{[Time]}");
        assertMemberExprDependsOn("([Measures].[Unit Sales], ClosingPeriod([Time].[Month]))", "{[Time]}");

        Member member;

        member = executeSingletonAxis("ClosingPeriod([Year])");
        Assert.assertEquals("[Time].[1997]", member.getUniqueName());

        member = executeSingletonAxis("ClosingPeriod([Quarter])");
        Assert.assertEquals("[Time].[1997].[Q4]", member.getUniqueName());

        member = executeSingletonAxis("ClosingPeriod([Month])");
        Assert.assertEquals("[Time].[1997].[Q4].[12]", member.getUniqueName());

        assertQueryReturns(
                fold(new String[] {
                    "with member [Measures].[Closing Unit Sales] as '([Measures].[Unit Sales], ClosingPeriod([Time].[Month]))'",
                    "select non empty {[Measures].[Closing Unit Sales]} on columns,",
                    " {Descendants([Time].[1997])} on rows",
                    "from [Sales]"}),

                fold(new String[] {
                    "Axis #0:",
                    "{}",
                    "Axis #1:",
                    "{[Measures].[Closing Unit Sales]}",
                    "Axis #2:",
                    "{[Time].[1997]}",
                    "{[Time].[1997].[Q1]}",
                    "{[Time].[1997].[Q1].[1]}",
                    "{[Time].[1997].[Q1].[2]}",
                    "{[Time].[1997].[Q1].[3]}",
                    "{[Time].[1997].[Q2]}",
                    "{[Time].[1997].[Q2].[4]}",
                    "{[Time].[1997].[Q2].[5]}",
                    "{[Time].[1997].[Q2].[6]}",
                    "{[Time].[1997].[Q3]}",
                    "{[Time].[1997].[Q3].[7]}",
                    "{[Time].[1997].[Q3].[8]}",
                    "{[Time].[1997].[Q3].[9]}",
                    "{[Time].[1997].[Q4]}",
                    "{[Time].[1997].[Q4].[10]}",
                    "{[Time].[1997].[Q4].[11]}",
                    "{[Time].[1997].[Q4].[12]}",
                    "Row #0: 26,796",
                    "Row #1: 23,706",
                    "Row #2: 21,628",
                    "Row #3: 20,957",
                    "Row #4: 23,706",
                    "Row #5: 21,350",
                    "Row #6: 20,179",
                    "Row #7: 21,081",
                    "Row #8: 21,350",
                    "Row #9: 20,388",
                    "Row #10: 23,763",
                    "Row #11: 21,697",
                    "Row #12: 20,388",
                    "Row #13: 26,796",
                    "Row #14: 19,958",
                    "Row #15: 25,270",
                    "Row #16: 26,796" + nl}));

        assertQueryReturns(
                fold(new String[] {
                    "with member [Measures].[Closing Unit Sales] as '([Measures].[Unit Sales], ClosingPeriod([Time].[Month]))'",
                    "select {[Measures].[Unit Sales], [Measures].[Closing Unit Sales]} on columns,",
                    " {[Time].[1997], [Time].[1997].[Q1], [Time].[1997].[Q1].[1], [Time].[1997].[Q1].[3], [Time].[1997].[Q4].[12]} on rows",
                    "from [Sales]"}),
                fold(new String[] {
                    "Axis #0:",
                    "{}",
                    "Axis #1:",
                    "{[Measures].[Unit Sales]}",
                    "{[Measures].[Closing Unit Sales]}",
                    "Axis #2:",
                    "{[Time].[1997]}",
                    "{[Time].[1997].[Q1]}",
                    "{[Time].[1997].[Q1].[1]}",
                    "{[Time].[1997].[Q1].[3]}",
                    "{[Time].[1997].[Q4].[12]}",
                    "Row #0: 266,773",
                    "Row #0: 26,796",
                    "Row #1: 66,291",
                    "Row #1: 23,706",
                    "Row #2: 21,628",
                    "Row #2: 21,628",
                    "Row #3: 23,706",
                    "Row #3: 23,706",
                    "Row #4: 26,796",
                    "Row #4: 26,796" + nl}));
    }

    public void testClosingPeriodLevelNotInTimeFails() {
        assertAxisThrows("ClosingPeriod([Store].[Store City])",
                "The <level> and <member> arguments to ClosingPeriod must be from the same hierarchy. The level was from '[Store]' but the member was from '[Time]'");
    }

    public void testClosingPeriodMember() {
        if (false) {
            // This test is mistaken. Valid forms are ClosingPeriod(<level>)
            // and ClosingPeriod(<level>, <member>), but not
            // ClosingPeriod(<member>)
            Member member = executeSingletonAxis("ClosingPeriod([USA])");
            Assert.assertEquals("WA", member.getName());
        }
    }

    public void testClosingPeriodMemberLeaf() {

        Member member;
        if (false) {
            // This test is mistaken. Valid forms are ClosingPeriod(<level>)
            // and ClosingPeriod(<level>, <member>), but not
            // ClosingPeriod(<member>)
            member = executeSingletonAxis("ClosingPeriod([Time].[1997].[Q3].[8])");
            Assert.assertNull(member);
        } else {
            assertQueryReturns(
                    fold(new String[] {
                        "with member [Measures].[Foo] as ' ClosingPeriod().uniquename '",
                        "select {[Measures].[Foo]} on columns,",
                        "  {[Time].[1997],",
                        "   [Time].[1997].[Q2],",
                        "   [Time].[1997].[Q2].[4]} on rows",
                        "from Sales"}),
                    fold(new String[] {
                        "Axis #0:",
                        "{}",
                        "Axis #1:",
                        "{[Measures].[Foo]}",
                        "Axis #2:",
                        "{[Time].[1997]}",
                        "{[Time].[1997].[Q2]}",
                        "{[Time].[1997].[Q2].[4]}",
                        "Row #0: [Time].[1997].[Q4]",
                        "Row #1: [Time].[1997].[Q2].[6]",
                        "Row #2: [Time].[#Null]", // MSAS returns "" here.
                        ""}));
        }
    }

    public void testClosingPeriod() {
        assertMemberExprDependsOn(
                "ClosingPeriod([Time].[Month], [Time].CurrentMember)",
                "{[Time]}");

        assertExprDependsOn("(([Measures].[Store Sales], ClosingPeriod([Time].[Month], [Time].CurrentMember)) - ([Measures].[Store Cost], ClosingPeriod([Time].[Month], [Time].CurrentMember)))",
                allDimsExcept(new String[] {"[Measures]"}));

        assertMemberExprDependsOn(
                "ClosingPeriod([Time].[Month], [Time].[1997].[Q3])",
                "{}");

        assertAxisReturns("ClosingPeriod([Time].[Year], [Time].[1997].[Q3])",
                "");

        assertAxisReturns("ClosingPeriod([Time].[Quarter], [Time].[1997].[Q3])",
                "[Time].[1997].[Q3]");

        assertAxisReturns("ClosingPeriod([Time].[Month], [Time].[1997].[Q3])",
                "[Time].[1997].[Q3].[9]");

        assertAxisReturns("ClosingPeriod([Time].[Quarter], [Time].[1997])",
                "[Time].[1997].[Q4]");

        assertAxisReturns("ClosingPeriod([Time].[Year], [Time].[1997])",
                "[Time].[1997]");

        assertAxisReturns("ClosingPeriod([Time].[Month], [Time].[1997])",
                "[Time].[1997].[Q4].[12]");

        // leaf member

        assertAxisReturns("ClosingPeriod([Time].[Year], [Time].[1997].[Q3].[8])",
                "");

        assertAxisReturns("ClosingPeriod([Time].[Quarter], [Time].[1997].[Q3].[8])",
                "");

        assertAxisReturns("ClosingPeriod([Time].[Month], [Time].[1997].[Q3].[8])",
                "[Time].[1997].[Q3].[8]");

        // non-Time dimension

        assertAxisReturns("ClosingPeriod([Product].[Product Name], [Product].[All Products].[Drink])",
                "[Product].[All Products].[Drink].[Dairy].[Dairy].[Milk].[Gorilla].[Gorilla Whole Milk]");

        assertAxisReturns("ClosingPeriod([Product].[Product Family], [Product].[All Products].[Drink])",
                "[Product].[All Products].[Drink]");

        // 'all' level

        assertAxisReturns("ClosingPeriod([Product].[(All)], [Product].[All Products].[Drink])",
                "");

        // ragged
        getTestContext("[Sales Ragged]").assertAxisReturns(
                "ClosingPeriod([Store].[Store City], [Store].[All Stores].[Israel])",
                "[Store].[All Stores].[Israel].[Israel].[Tel Aviv]");

        // Default member is [Time].[1997].
        assertAxisReturns("ClosingPeriod([Time].[Month])",
                "[Time].[1997].[Q4].[12]");

        assertAxisReturns("ClosingPeriod()", "[Time].[1997].[Q4]");

        TestContext testContext = getTestContext("[Sales Ragged]");
        testContext.assertAxisReturns(
                "ClosingPeriod([Store].[Store State], [Store].[All Stores].[Israel])",
                "");

        testContext.assertAxisThrows(
                "ClosingPeriod([Time].[Year], [Store].[All Stores].[Israel])",
                "The <level> and <member> arguments to ClosingPeriod must be "
                + "from the same hierarchy. The level was from '[Time]' but "
                + "the member was from '[Store]'.");
    }

    public void testClosingPeriodBelow() {
        Member member = executeSingletonAxis("ClosingPeriod([Quarter],[1997].[Q3].[8])");
        Assert.assertNull(member);
    }


    public void testCousin1() {
        Member member = executeSingletonAxis("Cousin([1997].[Q4],[1998])");
        Assert.assertEquals("[Time].[1998].[Q4]", member.getUniqueName());
    }

    public void testCousin2() {
        Member member = executeSingletonAxis("Cousin([1997].[Q4].[12],[1998].[Q1])");
        Assert.assertEquals("[Time].[1998].[Q1].[3]", member.getUniqueName());
    }

    public void testCousinOverrun() {
        Member member = executeSingletonAxis("Cousin([Customers].[USA].[CA].[San Jose], [Customers].[USA].[OR])");
        // CA has more cities than OR
        Assert.assertNull(member);
    }

    public void testCousinThreeDown() {
        Member member = executeSingletonAxis("Cousin([Customers].[USA].[CA].[Berkeley].[Alma Shelton], [Customers].[Mexico])");
        // Alma Shelton is the 3rd child
        // of the 4th child (Berkeley)
        // of the 1st child (CA)
        // of USA
        Assert.assertEquals("[Customers].[All Customers].[Mexico].[DF].[Tixapan].[Albert Clouse]", member.getUniqueName());
    }

    public void testCousinSameLevel() {
        Member member = executeSingletonAxis("Cousin([Gender].[M], [Gender].[F])");
        Assert.assertEquals("F", member.getName());
    }

    public void testCousinHigherLevel() {
        Member member = executeSingletonAxis("Cousin([Time].[1997], [Time].[1998].[Q1])");
        Assert.assertNull(member);
    }

    public void testCousinWrongHierarchy() {
        assertAxisThrows("Cousin([Time].[1997], [Gender].[M])",
                MondrianResource.instance().CousinHierarchyMismatch.str("[Time].[1997]", "[Gender].[All Gender].[M]"));
    }

    public void testParent() {
        assertMemberExprDependsOn("[Gender].Parent", "{[Gender]}");
        assertMemberExprDependsOn("[Gender].[M].Parent", "{}");
        assertAxisReturns("{[Store].[USA].[CA].Parent}",
                "[Store].[All Stores].[USA]");
        // root member has null parent
        assertAxisReturns("{[Store].[All Stores].Parent}", "");
        // parent of null member is null
        assertAxisReturns("{[Store].[All Stores].Parent.Parent}", "");
    }

    public void testMembers() {
        // <Level>.members
        assertAxisReturns("{[Customers].[Country].Members}", fold(new String[] {
            "[Customers].[All Customers].[Canada]",
            "[Customers].[All Customers].[Mexico]",
            "[Customers].[All Customers].[USA]"
        }));

        // <Level>.members applied to 'all' level
        assertAxisReturns("{[Customers].[(All)].Members}",
                "[Customers].[All Customers]");

        // <Level>.members applied to measures dimension
        // Note -- no cube-level calculated members are present
        assertAxisReturns("{[Measures].[MeasuresLevel].Members}",
                fold(new String[] {
                    "[Measures].[Sales Count]",
                    "[Measures].[Store Cost]",
                    "[Measures].[Store Sales]",
                    "[Measures].[Unit Sales]",
                    "[Measures].[Customer Count]",
                }));

        // <Dimension>.members applied to Measures
        assertAxisReturns("{[Measures].Members}",
                fold(new String[] {
                    "[Measures].[Unit Sales]",
                    "[Measures].[Store Cost]",
                    "[Measures].[Store Sales]",
                    "[Measures].[Sales Count]",
                    "[Measures].[Customer Count]"}));

        // <Dimension>.members applied to a query with calc measures
        // Again, no calc measures are returned
        assertQueryReturns("with member [Measures].[Xxx] AS ' [Measures].[Unit Sales] '" +
                "select {[Measures].members} on columns from [Sales]",
                fold(new String[] {
                "Axis #0:",
                "{}",
                "Axis #1:",
                "{[Measures].[Unit Sales]}",
                "{[Measures].[Store Cost]}",
                "{[Measures].[Store Sales]}",
                "{[Measures].[Sales Count]}",
                "{[Measures].[Customer Count]}",
                "Row #0: 266,773",
                "Row #0: 225,627.23",
                "Row #0: 565,238.13",
                "Row #0: 86,837",
                "Row #0: 5,581",
                ""}));
    }

    public void testAllMembers() {
        // <Level>.allmembers
        assertAxisReturns("{[Customers].[Country].allmembers}",
                fold(new String[] {
                    "[Customers].[All Customers].[Canada]",
                    "[Customers].[All Customers].[Mexico]",
                    "[Customers].[All Customers].[USA]"
        }));

        // <Level>.allmembers applied to 'all' level
        assertAxisReturns("{[Customers].[(All)].allmembers}",
                "[Customers].[All Customers]");

        // <Level>.allmembers applied to measures dimension
        // Note -- cube-level calculated members ARE present
        assertAxisReturns("{[Measures].[MeasuresLevel].allmembers}",
                fold(new String[] {
                    "[Measures].[Sales Count]",
                    "[Measures].[Store Cost]",
                    "[Measures].[Store Sales]",
                    "[Measures].[Unit Sales]",
                    "[Measures].[Customer Count]",
                    "[Measures].[Profit]",
                    "[Measures].[Profit Growth]",
                    "[Measures].[Profit last Period]"}));

        // <Dimension>.allmembers applied to Measures
        assertAxisReturns("{[Measures].allmembers}",
                fold(new String[] {
                    "[Measures].[Unit Sales]",
                    "[Measures].[Store Cost]",
                    "[Measures].[Store Sales]",
                    "[Measures].[Sales Count]",
                    "[Measures].[Customer Count]",
                    "[Measures].[Profit]",
                    "[Measures].[Profit last Period]",
                    "[Measures].[Profit Growth]"}));

        // <Dimension>.allmembers applied to a query with calc measures
        // Calc measures are returned
        assertQueryReturns("with member [Measures].[Xxx] AS ' [Measures].[Unit Sales] '" +
                "select {[Measures].allmembers} on columns from [Sales]",
                fold(new String[] {
                    "Axis #0:",
                    "{}",
                    "Axis #1:",
                    "{[Measures].[Unit Sales]}",
                    "{[Measures].[Store Cost]}",
                    "{[Measures].[Store Sales]}",
                    "{[Measures].[Sales Count]}",
                    "{[Measures].[Customer Count]}",
                    "{[Measures].[Profit]}",
                    "{[Measures].[Profit last Period]}",
                    "{[Measures].[Profit Growth]}",
                    "{[Measures].[Xxx]}",
                    "Row #0: 266,773",
                    "Row #0: 225,627.23",
                    "Row #0: 565,238.13",
                    "Row #0: 86,837",
                    "Row #0: 5,581",
                    "Row #0: $339,610.90",
                    "Row #0: $339,610.90",
                    "Row #0: 0.0%",
                    "Row #0: 266,773",
                    ""}));

        // Calc measure members from schema and from query
        assertQueryReturns("WITH MEMBER [Measures].[Unit to Sales ratio] as '[Measures].[Unit Sales] / [Measures].[Store Sales]', FORMAT_STRING='0.0%' " +
                "SELECT {[Measures].AllMembers} ON COLUMNS," +
                "non empty({[Store].[Store State].Members}) ON ROWS " +
                "FROM Sales " +
                "WHERE ([1997].[Q1])",
                fold(new String[] {
                    "Axis #0:",
                    "{[Time].[1997].[Q1]}",
                    "Axis #1:",
                    "{[Measures].[Unit Sales]}",
                    "{[Measures].[Store Cost]}",
                    "{[Measures].[Store Sales]}",
                    "{[Measures].[Sales Count]}",
                    "{[Measures].[Customer Count]}",
                    "{[Measures].[Profit]}",
                    "{[Measures].[Profit last Period]}",
                    "{[Measures].[Profit Growth]}",
                    "{[Measures].[Unit to Sales ratio]}",
                    "Axis #2:",
                    "{[Store].[All Stores].[USA].[CA]}",
                    "{[Store].[All Stores].[USA].[OR]}",
                    "{[Store].[All Stores].[USA].[WA]}",
                    "Row #0: 16,890",
                    "Row #0: 14,431.09",
                    "Row #0: 36,175.20",
                    "Row #0: 5,498",
                    "Row #0: 1,110",
                    "Row #0: $21,744.11",
                    "Row #0: $21,744.11",
                    "Row #0: 0.0%",
                    "Row #0: 46.7%",
                    "Row #1: 19,287",
                    "Row #1: 16,081.07",
                    "Row #1: 40,170.29",
                    "Row #1: 6,184",
                    "Row #1: 767",
                    "Row #1: $24,089.22",
                    "Row #1: $24,089.22",
                    "Row #1: 0.0%",
                    "Row #1: 48.0%",
                    "Row #2: 30,114",
                    "Row #2: 25,240.08",
                    "Row #2: 63,282.86",
                    "Row #2: 9,906",
                    "Row #2: 1,104",
                    "Row #2: $38,042.78",
                    "Row #2: $38,042.78",
                    "Row #2: 0.0%",
                    "Row #2: 47.6%",
                    ""}));

        // Calc member in query and schema not seen
        assertQueryReturns("WITH MEMBER [Measures].[Unit to Sales ratio] as '[Measures].[Unit Sales] / [Measures].[Store Sales]', FORMAT_STRING='0.0%' " +
                "SELECT {[Measures].Members} ON COLUMNS," +
                        "non empty({[Store].[Store State].Members}) ON ROWS " +
                "FROM Sales " +
                "WHERE ([1997].[Q1])",
                fold(new String[] {
                "Axis #0:",
                "{[Time].[1997].[Q1]}",
                "Axis #1:",
                "{[Measures].[Unit Sales]}",
                "{[Measures].[Store Cost]}",
                "{[Measures].[Store Sales]}",
                "{[Measures].[Sales Count]}",
                "{[Measures].[Customer Count]}",
                "Axis #2:",
                "{[Store].[All Stores].[USA].[CA]}",
                "{[Store].[All Stores].[USA].[OR]}",
                "{[Store].[All Stores].[USA].[WA]}",
                "Row #0: 16,890",
                "Row #0: 14,431.09",
                "Row #0: 36,175.20",
                "Row #0: 5,498",
                "Row #0: 1,110",
                "Row #1: 19,287",
                "Row #1: 16,081.07",
                "Row #1: 40,170.29",
                "Row #1: 6,184",
                "Row #1: 767",
                "Row #2: 30,114",
                "Row #2: 25,240.08",
                "Row #2: 63,282.86",
                "Row #2: 9,906",
                "Row #2: 1,104",
                ""}));

        // Calc member in dimension based on level
        assertQueryReturns("WITH MEMBER [Store].[USA].[CA plus OR] AS 'AGGREGATE({[Store].[USA].[CA], [Store].[USA].[OR]})' " +
                "SELECT {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS," +
                        "non empty({[Store].[Store State].AllMembers}) ON ROWS " +
                "FROM Sales " +
                "WHERE ([1997].[Q1])",
                fold(new String[] {
                        "Axis #0:",
                        "{[Time].[1997].[Q1]}",
                        "Axis #1:",
                        "{[Measures].[Unit Sales]}",
                        "{[Measures].[Store Sales]}",
                        "Axis #2:",
                        "{[Store].[All Stores].[USA].[CA]}",
                        "{[Store].[All Stores].[USA].[OR]}",
                        "{[Store].[All Stores].[USA].[WA]}",
                        "{[Store].[All Stores].[USA].[CA plus OR]}",
                        "Row #0: 16,890",
                        "Row #0: 36,175.20",
                        "Row #1: 19,287",
                        "Row #1: 40,170.29",
                        "Row #2: 30,114",
                        "Row #2: 63,282.86",
                        "Row #3: 36,177",
                        "Row #3: 76,345.49",
                        ""}));

        // Calc member in dimension based on level not seen
        assertQueryReturns("WITH MEMBER [Store].[USA].[CA plus OR] AS 'AGGREGATE({[Store].[USA].[CA], [Store].[USA].[OR]})' " +
                "SELECT {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS," +
                        "non empty({[Store].[Store Country].AllMembers}) ON ROWS " +
                "FROM Sales " +
                "WHERE ([1997].[Q1])",
                fold(new String[] {
                        "Axis #0:",
                        "{[Time].[1997].[Q1]}",
                        "Axis #1:",
                        "{[Measures].[Unit Sales]}",
                        "{[Measures].[Store Sales]}",
                        "Axis #2:",
                        "{[Store].[All Stores].[USA]}",
                        "Row #0: 66,291",
                        "Row #0: 139,628.35",
                        ""}));
    }

    public void testAddCalculatedMembers() {
        //----------------------------------------------------
        // AddCalculatedMembers: Calc member in dimension based on level included
        //----------------------------------------------------
        assertQueryReturns("WITH MEMBER [Store].[USA].[CA plus OR] AS 'AGGREGATE({[Store].[USA].[CA], [Store].[USA].[OR]})' " +
                "SELECT {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS," +
                        "AddCalculatedMembers([Store].[USA].Children) ON ROWS " +
                "FROM Sales " +
                "WHERE ([1997].[Q1])",
                fold(new String[] {
                        "Axis #0:",
                        "{[Time].[1997].[Q1]}",
                        "Axis #1:",
                        "{[Measures].[Unit Sales]}",
                        "{[Measures].[Store Sales]}",
                        "Axis #2:",
                        "{[Store].[All Stores].[USA].[CA]}",
                        "{[Store].[All Stores].[USA].[OR]}",
                        "{[Store].[All Stores].[USA].[WA]}",
                        "{[Store].[All Stores].[USA].[CA plus OR]}",
                        "Row #0: 16,890",
                        "Row #0: 36,175.20",
                        "Row #1: 19,287",
                        "Row #1: 40,170.29",
                        "Row #2: 30,114",
                        "Row #2: 63,282.86",
                        "Row #3: 36,177",
                        "Row #3: 76,345.49",
                        ""}));
        //----------------------------------------------------
        //Calc member in dimension based on level included
        //Calc members in measures in schema included
        //----------------------------------------------------
        assertQueryReturns("WITH MEMBER [Store].[USA].[CA plus OR] AS 'AGGREGATE({[Store].[USA].[CA], [Store].[USA].[OR]})' " +
                "SELECT AddCalculatedMembers({[Measures].[Unit Sales], [Measures].[Store Sales]}) ON COLUMNS," +
                        "AddCalculatedMembers([Store].[USA].Children) ON ROWS " +
                "FROM Sales " +
                "WHERE ([1997].[Q1])",
                fold(new String[] {
                        "Axis #0:",
                        "{[Time].[1997].[Q1]}",
                        "Axis #1:",
                        "{[Measures].[Unit Sales]}",
                        "{[Measures].[Store Sales]}",
                        "{[Measures].[Profit]}",
                        "{[Measures].[Profit last Period]}",
                        "{[Measures].[Profit Growth]}",

                        "Axis #2:",
                        "{[Store].[All Stores].[USA].[CA]}",
                        "{[Store].[All Stores].[USA].[OR]}",
                        "{[Store].[All Stores].[USA].[WA]}",
                        "{[Store].[All Stores].[USA].[CA plus OR]}",
                        "Row #0: 16,890",
                        "Row #0: 36,175.20",
                        "Row #0: $21,744.11",
                        "Row #0: $21,744.11",
                        "Row #0: 0.0%",
                        "Row #1: 19,287",
                        "Row #1: 40,170.29",
                        "Row #1: $24,089.22",
                        "Row #1: $24,089.22",
                        "Row #1: 0.0%",
                        "Row #2: 30,114",
                        "Row #2: 63,282.86",
                        "Row #2: $38,042.78",
                        "Row #2: $38,042.78",
                        "Row #2: 0.0%",
                        "Row #3: 36,177",
                        "Row #3: 76,345.49",
                        "Row #3: $45,833.33",
                        "Row #3: $45,833.33",
                        "Row #3: 0.0%",
                        ""}));
        //----------------------------------------------------
        //Two dimensions
        //----------------------------------------------------
        assertQueryReturns("SELECT AddCalculatedMembers({[Measures].[Unit Sales], [Measures].[Store Sales]}) ON COLUMNS," +
                        "{([Store].[USA].[CA], [Gender].[F])} ON ROWS " +
                "FROM Sales " +
                "WHERE ([1997].[Q1])",
                fold(new String[] {
                        "Axis #0:",
                        "{[Time].[1997].[Q1]}",
                        "Axis #1:",
                        "{[Measures].[Unit Sales]}",
                        "{[Measures].[Store Sales]}",
                        "{[Measures].[Profit]}",
                        "{[Measures].[Profit last Period]}",
                        "{[Measures].[Profit Growth]}",

                        "Axis #2:",
                        "{[Store].[All Stores].[USA].[CA], [Gender].[All Gender].[F]}",
                        "Row #0: 8,218",
                        "Row #0: 17,928.37",
                        "Row #0: $10,771.98",
                        "Row #0: $10,771.98",
                        "Row #0: 0.0%",
                        ""}));
        //----------------------------------------------------
        //Should throw more than one dimension error
        //----------------------------------------------------

        assertAxisThrows("AddCalculatedMembers({([Store].[USA].[CA], [Gender].[F])})",
            "Only single dimension members allowed in set for AddCalculatedMembers");
    }

    public void testStripCalculatedMembers() {
        assertAxisReturns("StripCalculatedMembers({[Measures].AllMembers})",
                fold(new String[] {
                    "[Measures].[Unit Sales]",
                    "[Measures].[Store Cost]",
                    "[Measures].[Store Sales]",
                    "[Measures].[Sales Count]",
                    "[Measures].[Customer Count]"}));

        // applied to empty set
        assertAxisReturns("StripCalculatedMembers({[Gender].Parent})", "");

        assertSetExprDependsOn(
                "StripCalculatedMembers([Customers].CurrentMember.Children)",
                "{[Customers]}");

        //----------------------------------------------------
        //Calc members in dimension based on level stripped
        //Actual members in measures left alone
        //----------------------------------------------------
        assertQueryReturns("WITH MEMBER [Store].[USA].[CA plus OR] AS 'AGGREGATE({[Store].[USA].[CA], [Store].[USA].[OR]})' " +
                "SELECT StripCalculatedMembers({[Measures].[Unit Sales], [Measures].[Store Sales]}) ON COLUMNS," +
                        "StripCalculatedMembers(AddCalculatedMembers([Store].[USA].Children)) ON ROWS " +
                "FROM Sales " +
                "WHERE ([1997].[Q1])",
                fold(new String[] {
                        "Axis #0:",
                        "{[Time].[1997].[Q1]}",
                        "Axis #1:",
                        "{[Measures].[Unit Sales]}",
                        "{[Measures].[Store Sales]}",
                        "Axis #2:",
                        "{[Store].[All Stores].[USA].[CA]}",
                        "{[Store].[All Stores].[USA].[OR]}",
                        "{[Store].[All Stores].[USA].[WA]}",
                        "Row #0: 16,890",
                        "Row #0: 36,175.20",
                        "Row #1: 19,287",
                        "Row #1: 40,170.29",
                        "Row #2: 30,114",
                        "Row #2: 63,282.86",
                        ""}));
    }

    public void testCurrentMember() {
        // <Dimension>.CurrentMember
        assertAxisReturns("[Gender].CurrentMember", "[Gender].[All Gender]");
        // <Hierarchy>.CurrentMember
        assertAxisReturns("[Gender].Hierarchy.CurrentMember", "[Gender].[All Gender]");

        // <Level>.CurrentMember
        // MSAS doesn't allow this, but Mondrian does: it implicitly casts
        // level to hierarchy.
        assertAxisReturns("[Store Name].CurrentMember", "[Store].[All Stores]");
    }

    public void testCurrentMemberDepends() {
        assertMemberExprDependsOn("[Gender].CurrentMember", "{[Gender]}");

        assertExprDependsOn("[Gender].[M].Dimension.Name", "{}");
        // implcitit call to .CurrentMember when dimension is used as a member expression
        assertMemberExprDependsOn("[Gender].[M].Dimension", "{[Gender]}");

        assertMemberExprDependsOn("[Gender].[M].Dimension.CurrentMember", "{[Gender]}");
        assertMemberExprDependsOn("[Gender].[M].Dimension.CurrentMember.Parent", "{[Gender]}");

        // [Customers] is short for [Customers].CurrentMember, so
        // depends upon everything
        assertExprDependsOn("[Customers]", allDims());
    }

    public void testCurrentMemberFromSlicer() {
        Result result = executeQuery(
                fold(new String[] {
                    "with member [Measures].[Foo] as '[Gender].CurrentMember.Name'",
                    "select {[Measures].[Foo]} on columns",
                    "from Sales where ([Gender].[F])"}));
        Assert.assertEquals("F", result.getCell(new int[]{0}).getValue());
    }

    public void testCurrentMemberFromDefaultMember() {
        Result result = executeQuery(
                fold(new String[] {
                    "with member [Measures].[Foo] as '[Time].CurrentMember.Name'",
                    "select {[Measures].[Foo]} on columns",
                    "from Sales"}));
        Assert.assertEquals("1997", result.getCell(new int[]{0}).getValue());
    }

    public void testDefaultMember() {
        Result result = executeQuery(
            fold(new String[] {
                 "select {[Time.Weekly].DefaultMember} on columns",
                 "from Sales"
            }));
        Assert.assertEquals("1997", result.getAxes()[0].positions[0].members[0].getName());
    }

    public void testCurrentMemberFromAxis() {
        Result result = executeQuery(
                fold(new String[] {
                    "with member [Measures].[Foo] as '[Gender].CurrentMember.Name || [Marital Status].CurrentMember.Name'",
                    "select {[Measures].[Foo]} on columns,",
                    " CrossJoin({[Gender].children}, {[Marital Status].children}) on rows",
                    "from Sales"}));
        Assert.assertEquals("FM", result.getCell(new int[]{0, 0}).getValue());
    }

    /**
     * When evaluating a calculated member, MSOLAP regards that
     * calculated member as the current member of that dimension, so it
     * cycles in this case. But I disagree; it is the previous current
     * member, before the calculated member was expanded.
     */
    public void testCurrentMemberInCalcMember() {
        Result result = executeQuery(
                fold(new String[] {
                    "with member [Measures].[Foo] as '[Measures].CurrentMember.Name'",
                    "select {[Measures].[Foo]} on columns",
                    "from Sales"}));
        Assert.assertEquals("Unit Sales", result.getCell(new int[]{0}).getValue());
    }

    public void testDimensionDefaultMember() {
        Member member = executeSingletonAxis("[Measures].DefaultMember");
        Assert.assertEquals("Unit Sales", member.getName());
    }

    public void testDrilldownLevel() {
        // Expect all children of USA
        assertAxisReturns("DrilldownLevel({[Store].[USA]}, [Store].[Store Country])",
                fold(new String[] {
                    "[Store].[All Stores].[USA]",
                    "[Store].[All Stores].[USA].[CA]",
                    "[Store].[All Stores].[USA].[OR]",
                    "[Store].[All Stores].[USA].[WA]"}));

        // Expect same set, because [USA] is already drilled
        assertAxisReturns("DrilldownLevel({[Store].[USA], [Store].[USA].[CA]}, [Store].[Store Country])",
                fold(new String[] {
                    "[Store].[All Stores].[USA]",
                    "[Store].[All Stores].[USA].[CA]"}));

        // Expect drill, because [USA] isn't already drilled. You can't
        // drill down on [CA] and get to [USA]
        assertAxisReturns("DrilldownLevel({[Store].[USA].[CA],[Store].[USA]}, [Store].[Store Country])",
                fold(new String[] {
                    "[Store].[All Stores].[USA].[CA]",
                    "[Store].[All Stores].[USA]",
                    "[Store].[All Stores].[USA].[CA]",
                    "[Store].[All Stores].[USA].[OR]",
                    "[Store].[All Stores].[USA].[WA]"}));

        assertThrows("select DrilldownLevel({[Store].[USA].[CA],[Store].[USA]},, 0) on columns from [Sales]",
                "Syntax error");
    }


    public void testDrilldownMember() {

        // Expect all children of USA
        assertAxisReturns("DrilldownMember({[Store].[USA]}, {[Store].[USA]})",
                fold(new String[] {
                    "[Store].[All Stores].[USA]",
                    "[Store].[All Stores].[USA].[CA]",
                    "[Store].[All Stores].[USA].[OR]",
                    "[Store].[All Stores].[USA].[WA]"}));

        // Expect all children of USA.CA and USA.OR
        assertAxisReturns("DrilldownMember({[Store].[USA].[CA], [Store].[USA].[OR]}, "+
                "{[Store].[USA].[CA], [Store].[USA].[OR], [Store].[USA].[WA]})",
                fold(new String[] {
                    "[Store].[All Stores].[USA].[CA]",
                    "[Store].[All Stores].[USA].[CA].[Alameda]",
                    "[Store].[All Stores].[USA].[CA].[Beverly Hills]",
                    "[Store].[All Stores].[USA].[CA].[Los Angeles]",
                    "[Store].[All Stores].[USA].[CA].[San Diego]",
                    "[Store].[All Stores].[USA].[CA].[San Francisco]",
                    "[Store].[All Stores].[USA].[OR]",
                    "[Store].[All Stores].[USA].[OR].[Portland]",
                    "[Store].[All Stores].[USA].[OR].[Salem]"}));


        // Second set is empty
        assertAxisReturns("DrilldownMember({[Store].[USA]}, {})",
                "[Store].[All Stores].[USA]");

        // Drill down a leaf member
        assertAxisReturns("DrilldownMember({[Store].[All Stores].[USA].[CA].[San Francisco].[Store 14]}, "+
                "{[Store].[All Stores].[USA].[CA].[San Francisco].[Store 14]})",
                "[Store].[All Stores].[USA].[CA].[San Francisco].[Store 14]");

        // Complex case with option recursive
        assertAxisReturns("DrilldownMember({[Store].[All Stores].[USA]}, "+
                "{[Store].[All Stores].[USA], [Store].[All Stores].[USA].[CA], "+
                "[Store].[All Stores].[USA].[CA].[San Diego], [Store].[All Stores].[USA].[WA]}, "+
                "RECURSIVE)",
                fold(new String[] {
                    "[Store].[All Stores].[USA]",
                    "[Store].[All Stores].[USA].[CA]",
                    "[Store].[All Stores].[USA].[CA].[Alameda]",
                    "[Store].[All Stores].[USA].[CA].[Beverly Hills]",
                    "[Store].[All Stores].[USA].[CA].[Los Angeles]",
                    "[Store].[All Stores].[USA].[CA].[San Diego]",
                    "[Store].[All Stores].[USA].[CA].[San Diego].[Store 24]",
                    "[Store].[All Stores].[USA].[CA].[San Francisco]",
                    "[Store].[All Stores].[USA].[OR]",
                    "[Store].[All Stores].[USA].[WA]",
                    "[Store].[All Stores].[USA].[WA].[Bellingham]",
                    "[Store].[All Stores].[USA].[WA].[Bremerton]",
                    "[Store].[All Stores].[USA].[WA].[Seattle]",
                    "[Store].[All Stores].[USA].[WA].[Spokane]",
                    "[Store].[All Stores].[USA].[WA].[Tacoma]",
                    "[Store].[All Stores].[USA].[WA].[Walla Walla]",
                    "[Store].[All Stores].[USA].[WA].[Yakima]"}));

        // Sets of tuples
        assertAxisReturns("DrilldownMember({([Store Type].[Supermarket], [Store].[USA])}, {[Store].[USA]})",
                fold(new String[] {
                    "{[Store Type].[All Store Types].[Supermarket], [Store].[All Stores].[USA]}",
                    "{[Store Type].[All Store Types].[Supermarket], [Store].[All Stores].[USA].[CA]}",
                    "{[Store Type].[All Store Types].[Supermarket], [Store].[All Stores].[USA].[OR]}",
                    "{[Store Type].[All Store Types].[Supermarket], [Store].[All Stores].[USA].[WA]}"}));
    }


    public void testFirstChildFirstInLevel() {
        Member member = executeSingletonAxis("[Time].[1997].[Q4].FirstChild");
        Assert.assertEquals("10", member.getName());
    }

    public void testFirstChildAll() {
        Member member = executeSingletonAxis("[Gender].[All Gender].FirstChild");
        Assert.assertEquals("F", member.getName());
    }

    public void testFirstChildOfChildless() {
        Member member = executeSingletonAxis("[Gender].[All Gender].[F].FirstChild");
        Assert.assertNull(member);
    }

    public void testFirstSiblingFirstInLevel() {
        Member member = executeSingletonAxis("[Gender].[F].FirstSibling");
        Assert.assertEquals("F", member.getName());
    }

    public void testFirstSiblingLastInLevel() {
        Member member = executeSingletonAxis("[Time].[1997].[Q4].FirstSibling");
        Assert.assertEquals("Q1", member.getName());
    }

    public void testFirstSiblingAll() {
        Member member = executeSingletonAxis("[Gender].[All Gender].FirstSibling");
        Assert.assertTrue(member.isAll());
    }

    public void testFirstSiblingRoot() {
        // The [Measures] hierarchy does not have an 'all' member, so
        // [Unit Sales] does not have a parent.
        Member member = executeSingletonAxis("[Measures].[Store Sales].FirstSibling");
        Assert.assertEquals("Unit Sales", member.getName());
    }

    public void testFirstSiblingNull() {
        Member member = executeSingletonAxis("[Gender].[F].FirstChild.FirstSibling");
        Assert.assertNull(member);
    }

    public void testLag() {
        Member member = executeSingletonAxis("[Time].[1997].[Q4].[12].Lag(4)");
        Assert.assertEquals("8", member.getName());
    }

    public void testLagFirstInLevel() {
        Member member = executeSingletonAxis("[Gender].[F].Lag(1)");
        Assert.assertNull(member);
    }

    public void testLagAll() {
        Member member = executeSingletonAxis("[Gender].DefaultMember.Lag(2)");
        Assert.assertNull(member);
    }

    public void testLagRoot() {
        Member member = executeSingletonAxis("[Time].[1998].Lag(1)");
        Assert.assertEquals("1997", member.getName());
    }

    public void testLagRootTooFar() {
        Member member = executeSingletonAxis("[Time].[1998].Lag(2)");
        Assert.assertNull(member);
    }

    public void testLastChild() {
        Member member = executeSingletonAxis("[Gender].LastChild");
        Assert.assertEquals("M", member.getName());
    }

    public void testLastChildLastInLevel() {
        Member member = executeSingletonAxis("[Time].[1997].[Q4].LastChild");
        Assert.assertEquals("12", member.getName());
    }

    public void testLastChildAll() {
        Member member = executeSingletonAxis("[Gender].[All Gender].LastChild");
        Assert.assertEquals("M", member.getName());
    }

    public void testLastChildOfChildless() {
        Member member = executeSingletonAxis("[Gender].[M].LastChild");
        Assert.assertNull(member);
    }

    public void testLastSibling() {
        Member member = executeSingletonAxis("[Gender].[F].LastSibling");
        Assert.assertEquals("M", member.getName());
    }

    public void testLastSiblingFirstInLevel() {
        Member member = executeSingletonAxis("[Time].[1997].[Q1].LastSibling");
        Assert.assertEquals("Q4", member.getName());
    }

    public void testLastSiblingAll() {
        Member member = executeSingletonAxis("[Gender].[All Gender].LastSibling");
        Assert.assertTrue(member.isAll());
    }

    public void testLastSiblingRoot() {
        // The [Time] hierarchy does not have an 'all' member, so
        // [1997], [1998] do not have parents.
        Member member = executeSingletonAxis("[Time].[1998].LastSibling");
        Assert.assertEquals("1998", member.getName());
    }

    public void testLastSiblingNull() {
        Member member = executeSingletonAxis("[Gender].[F].FirstChild.LastSibling");
        Assert.assertNull(member);
    }


    public void testLead() {
        Member member = executeSingletonAxis("[Time].[1997].[Q2].[4].Lead(4)");
        Assert.assertEquals("8", member.getName());
    }

    public void testLeadNegative() {
        Member member = executeSingletonAxis("[Gender].[M].Lead(-1)");
        Assert.assertEquals("F", member.getName());
    }

    public void testLeadLastInLevel() {
        Member member = executeSingletonAxis("[Gender].[M].Lead(3)");
        Assert.assertNull(member);
    }

    public void testLeadNull() {
        Member member = executeSingletonAxis("[Gender].Parent.Lead(1)");
        Assert.assertNull(member);
    }

    public void testLeadZero() {
        Member member = executeSingletonAxis("[Gender].[F].Lead(0)");
        Assert.assertEquals("F", member.getName());
    }

    public void testBasic2() {
        Result result = executeQuery("select {[Gender].[F].NextMember} ON COLUMNS from Sales");
        Assert.assertTrue(result.getAxes()[0].positions[0].members[0].getName().equals("M"));
    }

    public void testFirstInLevel2() {
        Result result = executeQuery("select {[Gender].[M].NextMember} ON COLUMNS from Sales");
        Assert.assertTrue(result.getAxes()[0].positions.length == 0);
    }

    public void testAll2() {
        Result result = executeQuery("select {[Gender].PrevMember} ON COLUMNS from Sales");
        // previous to [Gender].[All] is null, so no members are returned
        Assert.assertTrue(result.getAxes()[0].positions.length == 0);
    }


    public void testBasic5() {
        Result result = executeQuery("select{ [Product].[All Products].[Drink].Parent} on columns from Sales");
        Assert.assertTrue(result.getAxes()[0].positions[0].members[0].getName().equals("All Products"));
    }

    public void testFirstInLevel5() {
        Result result = executeQuery("select {[Time].[1997].[Q2].[4].Parent} on columns,{[Gender].[M]} on rows from Sales");
        Assert.assertTrue(result.getAxes()[0].positions[0].members[0].getName().equals("Q2"));
    }

    public void testAll5() {
        Result result = executeQuery("select {[Time].[1997].[Q2].Parent} on columns,{[Gender].[M]} on rows from Sales");
        // previous to [Gender].[All] is null, so no members are returned
        Assert.assertTrue(result.getAxes()[0].positions[0].members[0].getName().equals("1997"));
    }


    public void testBasic() {
        Result result = executeQuery("select {[Gender].[M].PrevMember} ON COLUMNS from Sales");
        Assert.assertTrue(result.getAxes()[0].positions[0].members[0].getName().equals("F"));
    }

    public void testFirstInLevel() {
        Result result = executeQuery("select {[Gender].[F].PrevMember} ON COLUMNS from Sales");
        Assert.assertTrue(result.getAxes()[0].positions.length == 0);
    }

    public void testAll() {
        Result result = executeQuery("select {[Gender].PrevMember} ON COLUMNS from Sales");
        // previous to [Gender].[All] is null, so no members are returned
        Assert.assertTrue(result.getAxes()[0].positions.length == 0);
    }

    public void testAggregateDepends() {
        // Depends on everything except Measures, Gender
        assertExprDependsOn("([Measures].[Unit Sales], [Gender].[F])",
                allDimsExcept(new String[] {"[Measures]", "[Gender]"}));
        // Depends on everything except Customers, Measures, Gender
        assertExprDependsOn("Aggregate([Customers].Members, ([Measures].[Unit Sales], [Gender].[F]))",
                allDimsExcept(new String[] {"[Customers]", "[Measures]", "[Gender]"}));
        // Depends on everything except Customers
        assertExprDependsOn("Aggregate([Customers].Members)",
                allDimsExcept(new String[] {"[Customers]"}));
        // Depends on the current member of the Product dimension, even though
        // [Product].[All Products] is referenced from the expression.
        assertExprDependsOn("Aggregate(Filter([Customers].[City].Members, (([Measures].[Unit Sales] / ([Measures].[Unit Sales], [Product].[All Products])) > 0.1)))",
                allDimsExcept(new String[] {"[Customers]"}));
    }

    public static boolean contains(String[] a, String s) {
        for (int i = 0; i < a.length; i++) {
            if (a[i].equals(s)) {
                return true;
            }
        }
        return false;
    }

    public void testAggregate() {
        assertQueryReturns(
                fold(new String[] {
                    "WITH MEMBER [Store].[CA plus OR] AS 'AGGREGATE({[Store].[USA].[CA], [Store].[USA].[OR]})'",
                    "SELECT {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,",
                    "      {[Store].[USA].[CA], [Store].[USA].[OR], [Store].[CA plus OR]} ON ROWS",
                    "FROM Sales",
                    "WHERE ([1997].[Q1])"}),
                fold(new String[] {
                    "Axis #0:",
                    "{[Time].[1997].[Q1]}",
                    "Axis #1:",
                    "{[Measures].[Unit Sales]}",
                    "{[Measures].[Store Sales]}",
                    "Axis #2:",
                    "{[Store].[All Stores].[USA].[CA]}",
                    "{[Store].[All Stores].[USA].[OR]}",
                    "{[Store].[CA plus OR]}",
                    "Row #0: 16,890",
                    "Row #0: 36,175.20",
                    "Row #1: 19,287",
                    "Row #1: 40,170.29",
                    "Row #2: 36,177",
                    "Row #2: 76,345.49" + nl}));
    }

    public void testAggregate2() {
        assertQueryReturns(
                fold(new String[] {
                    "WITH",
                    "  MEMBER [Time].[1st Half Sales] AS 'Aggregate({Time.[1997].[Q1], Time.[1997].[Q2]})'",
                    "  MEMBER [Time].[2nd Half Sales] AS 'Aggregate({Time.[1997].[Q3], Time.[1997].[Q4]})'",
                    "  MEMBER [Time].[Difference] AS 'Time.[2nd Half Sales] - Time.[1st Half Sales]'",
                    "SELECT",
                    "   { [Store].[Store State].Members} ON COLUMNS,",
                    "   { Time.[1st Half Sales], Time.[2nd Half Sales], Time.[Difference]} ON ROWS",
                    "FROM Sales",
                    "WHERE [Measures].[Store Sales]"}),
                fold(new String[] {
                    "Axis #0:",
                    "{[Measures].[Store Sales]}",
                    "Axis #1:",
                    "{[Store].[All Stores].[Canada].[BC]}",
                    "{[Store].[All Stores].[Mexico].[DF]}",
                    "{[Store].[All Stores].[Mexico].[Guerrero]}",
                    "{[Store].[All Stores].[Mexico].[Jalisco]}",
                    "{[Store].[All Stores].[Mexico].[Veracruz]}",
                    "{[Store].[All Stores].[Mexico].[Yucatan]}",
                    "{[Store].[All Stores].[Mexico].[Zacatecas]}",
                    "{[Store].[All Stores].[USA].[CA]}",
                    "{[Store].[All Stores].[USA].[OR]}",
                    "{[Store].[All Stores].[USA].[WA]}",
                    "Axis #2:",
                    "{[Time].[1st Half Sales]}",
                    "{[Time].[2nd Half Sales]}",
                    "{[Time].[Difference]}",
                    "Row #0: (null)",
                    "Row #0: (null)",
                    "Row #0: (null)",
                    "Row #0: (null)",
                    "Row #0: (null)",
                    "Row #0: (null)",
                    "Row #0: (null)",
                    "Row #0: 74,571.95",
                    "Row #0: 71,943.17",
                    "Row #0: 125,779.50",
                    "Row #1: (null)",
                    "Row #1: (null)",
                    "Row #1: (null)",
                    "Row #1: (null)",
                    "Row #1: (null)",
                    "Row #1: (null)",
                    "Row #1: (null)",
                    "Row #1: 84,595.89",
                    "Row #1: 70,333.90",
                    "Row #1: 138,013.72",
                    "Row #2: (null)",
                    "Row #2: (null)",
                    "Row #2: (null)",
                    "Row #2: (null)",
                    "Row #2: (null)",
                    "Row #2: (null)",
                    "Row #2: (null)",
                    "Row #2: 10,023.94",
                    "Row #2: -1,609.27",
                    "Row #2: 12,234.22" + nl}));
    }

    public void testAggregate2AllMembers() {
        assertQueryReturns(
                fold(new String[] {
                    "WITH",
                    "  MEMBER [Time].[1st Half Sales] AS 'Aggregate({Time.[1997].[Q1], Time.[1997].[Q2]})'",
                    "  MEMBER [Time].[2nd Half Sales] AS 'Aggregate({Time.[1997].[Q3], Time.[1997].[Q4]})'",
                    "  MEMBER [Time].[Difference] AS 'Time.[2nd Half Sales] - Time.[1st Half Sales]'",
                    "SELECT",
                    "   { [Store].[Store State].AllMembers} ON COLUMNS,",
                    "   { Time.[1st Half Sales], Time.[2nd Half Sales], Time.[Difference]} ON ROWS",
                    "FROM Sales",
                    "WHERE [Measures].[Store Sales]"}),
                fold(new String[] {
                    "Axis #0:",
                    "{[Measures].[Store Sales]}",
                    "Axis #1:",
                    "{[Store].[All Stores].[Canada].[BC]}",
                    "{[Store].[All Stores].[Mexico].[DF]}",
                    "{[Store].[All Stores].[Mexico].[Guerrero]}",
                    "{[Store].[All Stores].[Mexico].[Jalisco]}",
                    "{[Store].[All Stores].[Mexico].[Veracruz]}",
                    "{[Store].[All Stores].[Mexico].[Yucatan]}",
                    "{[Store].[All Stores].[Mexico].[Zacatecas]}",
                    "{[Store].[All Stores].[USA].[CA]}",
                    "{[Store].[All Stores].[USA].[OR]}",
                    "{[Store].[All Stores].[USA].[WA]}",
                    "Axis #2:",
                    "{[Time].[1st Half Sales]}",
                    "{[Time].[2nd Half Sales]}",
                    "{[Time].[Difference]}",
                    "Row #0: (null)",
                    "Row #0: (null)",
                    "Row #0: (null)",
                    "Row #0: (null)",
                    "Row #0: (null)",
                    "Row #0: (null)",
                    "Row #0: (null)",
                    "Row #0: 74,571.95",
                    "Row #0: 71,943.17",
                    "Row #0: 125,779.50",
                    "Row #1: (null)",
                    "Row #1: (null)",
                    "Row #1: (null)",
                    "Row #1: (null)",
                    "Row #1: (null)",
                    "Row #1: (null)",
                    "Row #1: (null)",
                    "Row #1: 84,595.89",
                    "Row #1: 70,333.90",
                    "Row #1: 138,013.72",
                    "Row #2: (null)",
                    "Row #2: (null)",
                    "Row #2: (null)",
                    "Row #2: (null)",
                    "Row #2: (null)",
                    "Row #2: (null)",
                    "Row #2: (null)",
                    "Row #2: 10,023.94",
                    "Row #2: -1,609.27",
                    "Row #2: 12,234.22" + nl}));
    }


    public void testAggregateToSimulateCompoundSlicer() {
        assertQueryReturns(
                fold(new String[] {
                    "WITH MEMBER [Time].[1997 H1] as 'Aggregate({[Time].[1997].[Q1], [Time].[1997].[Q2]})'",
                    "  MEMBER [Education Level].[College or higher] as 'Aggregate({[Education Level].[Bachelors Degree], [Education Level].[Graduate Degree]})'",
                    "SELECT {[Measures].[Unit Sales], [Measures].[Store Sales]} on columns,",
                    "  {[Product].children} on rows",
                    "FROM [Sales]",
                    "WHERE ([Time].[1997 H1], [Education Level].[College or higher], [Gender].[F])"}),
                fold(new String[] {
                    "Axis #0:",
                    "{[Time].[1997 H1], [Education Level].[College or higher], [Gender].[All Gender].[F]}",
                    "Axis #1:",
                    "{[Measures].[Unit Sales]}",
                    "{[Measures].[Store Sales]}",
                    "Axis #2:",
                    "{[Product].[All Products].[Drink]}",
                    "{[Product].[All Products].[Food]}",
                    "{[Product].[All Products].[Non-Consumable]}",
                    "Row #0: 1,797",
                    "Row #0: 3,620.49",
                    "Row #1: 15,002",
                    "Row #1: 31,931.88",
                    "Row #2: 3,845",
                    "Row #2: 8,173.22" + nl}));
    }

    public void testAvg() {
        assertExprReturns("AVG({[Store].[All Stores].[USA].children},[Measures].[Store Sales])",
            "188,412.71");
    }
    //todo: testAvgWithNulls

    public void testCorrelation() {
        assertExprReturns("Correlation({[Store].[All Stores].[USA].children}, [Measures].[Unit Sales], [Measures].[Store Sales]) * 1000000", "999,906");
    }

    public void testCount() {
        assertExprDependsOn("count(Crossjoin([Store].[All Stores].[USA].Children, {[Gender].children}), INCLUDEEMPTY)",
                "{[Gender]}");

        assertExprDependsOn("count(Crossjoin([Store].[All Stores].[USA].Children, {[Gender].children}), EXCLUDEEMPTY)",
                allDimsExcept(new String[] {"[Store]"}));

        assertExprReturns("count({[Promotion Media].[Media Type].members})", "14");

        // applied to an empty set
        assertExprReturns("count({[Gender].Parent}, IncludeEmpty)", "0");
    }

    public void testCountExcludeEmpty() {
        assertExprDependsOn("count(Crossjoin([Store].[USA].Children, {[Gender].children}), EXCLUDEEMPTY)",
                allDimsExcept(new String[] {"[Store]"}));

        assertQueryReturns(
                fold(new String[] {
                    "with member [Measures].[Promo Count] as ",
                    " ' Count(Crossjoin({[Measures].[Unit Sales]},",
                    " {[Promotion Media].[Media Type].members}), EXCLUDEEMPTY)'",
                    "select {[Measures].[Unit Sales], [Measures].[Promo Count]} on columns,",
                    " {[Product].[Drink].[Beverages].[Carbonated Beverages].[Soda].children} on rows",
                    "from Sales"}),
                fold(new String[] {
                    "Axis #0:",
                    "{}",
                    "Axis #1:",
                    "{[Measures].[Unit Sales]}",
                    "{[Measures].[Promo Count]}",
                    "Axis #2:",
                    "{[Product].[All Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Excellent]}",
                    "{[Product].[All Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Fabulous]}",
                    "{[Product].[All Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Skinner]}",
                    "{[Product].[All Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Token]}",
                    "{[Product].[All Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Washington]}",
                    "Row #0: 738",
                    "Row #0: 14",
                    "Row #1: 632",
                    "Row #1: 13",
                    "Row #2: 655",
                    "Row #2: 14",
                    "Row #3: 735",
                    "Row #3: 14",
                    "Row #4: 647",
                    "Row #4: 12" + nl}));

        // applied to an empty set
        assertExprReturns("count({[Gender].Parent}, ExcludeEmpty)", "0");
    }

    //todo: testCountNull, testCountNoExp

    public void testCovariance() {
        assertExprReturns("Covariance({[Store].[All Stores].[USA].children}, [Measures].[Unit Sales], [Measures].[Store Sales])", "1,355,761,899");
    }

    public void testCovarianceN() {
        assertExprReturns("CovarianceN({[Store].[All Stores].[USA].children}, [Measures].[Unit Sales], [Measures].[Store Sales])", "2,033,642,849");
    }


    public void testIIfNumeric() {
        assertExprReturns("IIf(([Measures].[Unit Sales],[Product].[Drink].[Alcoholic Beverages].[Beer and Wine]) > 100, 45, 32)", "45");

        // Compare two members. The system needs to figure out that they are
        // both numeric, and use the right overloaded version of ">", otherwise
        // we'll get a ClassCastException at runtime.
        assertExprReturns("IIf([Measures].[Unit Sales] > [Measures].[Store Sales], 45, 32)",
            "32");
    }

    public void testMax() {
        assertExprReturns("MAX({[Store].[All Stores].[USA].children},[Measures].[Store Sales])", "263,793.22");
    }

    public void testMedian() {
        assertExprReturns("MEDIAN({[Store].[All Stores].[USA].children},[Measures].[Store Sales])", "159,167.84");
    }

    public void testMedian2() {
        assertQueryReturns(
                fold(new String[] {
                    "WITH",
                    "   MEMBER [Time].[1st Half Sales] AS 'Sum({[Time].[1997].[Q1], [Time].[1997].[Q2]})'",
                    "   MEMBER [Time].[2nd Half Sales] AS 'Sum({[Time].[1997].[Q3], [Time].[1997].[Q4]})'",
                    "   MEMBER [Time].[Median] AS 'Median(Time.Members)'",
                    "SELECT",
                    "   NON EMPTY { [Store].[Store Name].Members} ON COLUMNS,",
                    "   { [Time].[1st Half Sales], [Time].[2nd Half Sales], [Time].[Median]} ON ROWS",
                    "FROM Sales",
                    "WHERE [Measures].[Store Sales]"}),

                fold(new String[] {
                    "Axis #0:",
                    "{[Measures].[Store Sales]}",
                    "Axis #1:",
                    "{[Store].[All Stores].[USA].[CA].[Beverly Hills].[Store 6]}",
                    "{[Store].[All Stores].[USA].[CA].[Los Angeles].[Store 7]}",
                    "{[Store].[All Stores].[USA].[CA].[San Diego].[Store 24]}",
                    "{[Store].[All Stores].[USA].[CA].[San Francisco].[Store 14]}",
                    "{[Store].[All Stores].[USA].[OR].[Portland].[Store 11]}",
                    "{[Store].[All Stores].[USA].[OR].[Salem].[Store 13]}",
                    "{[Store].[All Stores].[USA].[WA].[Bellingham].[Store 2]}",
                    "{[Store].[All Stores].[USA].[WA].[Bremerton].[Store 3]}",
                    "{[Store].[All Stores].[USA].[WA].[Seattle].[Store 15]}",
                    "{[Store].[All Stores].[USA].[WA].[Spokane].[Store 16]}",
                    "{[Store].[All Stores].[USA].[WA].[Tacoma].[Store 17]}",
                    "{[Store].[All Stores].[USA].[WA].[Walla Walla].[Store 22]}",
                    "{[Store].[All Stores].[USA].[WA].[Yakima].[Store 23]}",
                    "Axis #2:",
                    "{[Time].[1st Half Sales]}",
                    "{[Time].[2nd Half Sales]}",
                    "{[Time].[Median]}",
                    "Row #0: 20,801.04",
                    "Row #0: 25,421.41",
                    "Row #0: 26,275.11",
                    "Row #0: 2,074.39",
                    "Row #0: 28,519.18",
                    "Row #0: 43,423.99",
                    "Row #0: 2,140.99",
                    "Row #0: 25,502.08",
                    "Row #0: 25,293.50",
                    "Row #0: 23,265.53",
                    "Row #0: 34,926.91",
                    "Row #0: 2,159.60",
                    "Row #0: 12,490.89",
                    "Row #1: 24,949.20",
                    "Row #1: 29,123.87",
                    "Row #1: 28,156.03",
                    "Row #1: 2,366.79",
                    "Row #1: 26,539.61",
                    "Row #1: 43,794.29",
                    "Row #1: 2,598.24",
                    "Row #1: 27,394.22",
                    "Row #1: 27,350.57",
                    "Row #1: 26,368.93",
                    "Row #1: 39,917.05",
                    "Row #1: 2,546.37",
                    "Row #1: 11,838.34",
                    "Row #2: 4,577.35",
                    "Row #2: 5,211.38",
                    "Row #2: 4,722.87",
                    "Row #2: 398.24",
                    "Row #2: 5,039.50",
                    "Row #2: 7,374.59",
                    "Row #2: 410.22",
                    "Row #2: 4,924.04",
                    "Row #2: 4,569.13",
                    "Row #2: 4,511.68",
                    "Row #2: 6,630.91",
                    "Row #2: 419.51",
                    "Row #2: 2,169.48",
                    ""}));
    }

    public void testMin() {
        assertExprReturns("MIN({[Store].[All Stores].[USA].children},[Measures].[Store Sales])", "142,277.07");
    }

    public void testMinTuple() {
        assertExprReturns("Min([Customers].[All Customers].[USA].Children, ([Measures].[Unit Sales], [Gender].[All Gender].[F]))", "33,036");
    }

    public void testStdev() {
        assertExprReturns("STDEV({[Store].[All Stores].[USA].children},[Measures].[Store Sales])", "65,825.45");
    }

    public void testStdevP() {
        assertExprReturns("STDEVP({[Store].[All Stores].[USA].children},[Measures].[Store Sales])", "53,746.26");
    }

    public void testSumNoExp() {
        assertExprReturns("SUM({[Promotion Media].[Media Type].members})", "266,773");
    }

    public void testValue() {
        // VALUE is usually a cell property, not a member property.
        // We allow it because MS documents it as a function, <Member>.VALUE.
        assertExprReturns("[Measures].[Store Sales].VALUE", "565,238.13");

        // Depends upon almost everything.
        assertExprDependsOn("[Measures].[Store Sales].VALUE",
                allDimsExcept(new String[] {"[Measures]"}));

        // We do not allow FORMATTED_VALUE.
        assertExprThrows("[Measures].[Store Sales].FORMATTED_VALUE",
                "MDX object '[Measures].[Store Sales].[FORMATTED_VALUE]' not found in cube 'Sales'");
        assertExprReturns("[Measures].[Store Sales].NAME", "Store Sales");
        // MS says that ID and KEY are standard member properties for
        // OLE DB for OLAP, but not for XML/A. We don't support them.
        assertExprThrows("[Measures].[Store Sales].ID",
                "MDX object '[Measures].[Store Sales].[ID]' not found in cube 'Sales'");
        assertExprThrows("[Measures].[Store Sales].KEY",
                "MDX object '[Measures].[Store Sales].[KEY]' not found in cube 'Sales'");
        assertExprReturns("[Measures].[Store Sales].CAPTION", "Store Sales");
    }

    public void testVar() {
        assertExprReturns("VAR({[Store].[All Stores].[USA].children},[Measures].[Store Sales])", "4,332,990,493.69");
    }

    public void testVarP() {
        assertExprReturns("VARP({[Store].[All Stores].[USA].children},[Measures].[Store Sales])", "2,888,660,329.13");
    }

    public void testAscendants() {
        assertAxisReturns("Ascendants([Store].[USA].[CA])",
                fold(new String[] {
                    "[Store].[All Stores].[USA].[CA]",
                    "[Store].[All Stores].[USA]",
                    "[Store].[All Stores]"}));
    }

    public void testAscendantsAll() {
        assertAxisReturns("Ascendants([Store].DefaultMember)",
                "[Store].[All Stores]");
    }

    public void testAscendantsNull() {
        assertAxisReturns("Ascendants([Gender].[F].PrevMember)",
                "");
    }

    public void testBottomCount() {
        assertAxisReturns("BottomCount({[Promotion Media].[Media Type].members}, 2, [Measures].[Unit Sales])",
                fold(new String[] {
                    "[Promotion Media].[All Media].[Radio]",
                    "[Promotion Media].[All Media].[Sunday Paper, Radio, TV]"}));
    }
    //todo: test unordered

    public void testBottomPercent() {
        assertAxisReturns("BottomPercent(Filter({[Store].[All Stores].[USA].[CA].Children, [Store].[All Stores].[USA].[OR].Children, [Store].[All Stores].[USA].[WA].Children}, ([Measures].[Unit Sales] > 0.0)), 100.0, [Measures].[Store Sales])",
                fold(new String[] {
                    "[Store].[All Stores].[USA].[CA].[San Francisco]",
                    "[Store].[All Stores].[USA].[WA].[Walla Walla]",
                    "[Store].[All Stores].[USA].[WA].[Bellingham]",
                    "[Store].[All Stores].[USA].[WA].[Yakima]",
                    "[Store].[All Stores].[USA].[CA].[Beverly Hills]",
                    "[Store].[All Stores].[USA].[WA].[Spokane]",
                    "[Store].[All Stores].[USA].[WA].[Seattle]",
                    "[Store].[All Stores].[USA].[WA].[Bremerton]",
                    "[Store].[All Stores].[USA].[CA].[San Diego]",
                    "[Store].[All Stores].[USA].[CA].[Los Angeles]",
                    "[Store].[All Stores].[USA].[OR].[Portland]",
                    "[Store].[All Stores].[USA].[WA].[Tacoma]",
                    "[Store].[All Stores].[USA].[OR].[Salem]"}));

        assertAxisReturns("BottomPercent({[Promotion Media].[Media Type].members}, 1, [Measures].[Unit Sales])",
                fold(new String[] {
                    "[Promotion Media].[All Media].[Radio]",
                    "[Promotion Media].[All Media].[Sunday Paper, Radio, TV]"}));
    }
    //todo: test precision

    public void testBottomSum() {
        assertAxisReturns("BottomSum({[Promotion Media].[Media Type].members}, 5000, [Measures].[Unit Sales])",
                fold(new String[] {
                    "[Promotion Media].[All Media].[Radio]",
                    "[Promotion Media].[All Media].[Sunday Paper, Radio, TV]"}));
    }

    public void testExceptEmpty() {
        // If left is empty, result is empty.
        assertAxisReturns("Except(Filter([Gender].Members, 1=0), {[Gender].[M]})",
                "");

        // If right is empty, result is left.
        assertAxisReturns("Except({[Gender].[M]}, Filter([Gender].Members, 1=0))",
                "[Gender].[All Gender].[M]");
    }

    /**
     * Tests that Except() successfully removes crossjoined tuples
     * from the axis results.  Previously, this would fail by returning
     * all tuples in the first argument to Except.  bug 1439627
     */
    public void testExceptCrossjoin() {
        assertAxisReturns(
                fold(new String[] {
                    "Except(CROSSJOIN({[Promotion Media].[All Media]},",
                    "                  [Product].[All Products].Children),",
                    "       CROSSJOIN({[Promotion Media].[All Media]},",
                    "                  {[Product].[All Products].[Drink]}))" }),
                fold(new String[] {
                    "{[Promotion Media].[All Media], [Product].[All Products].[Food]}",
                    "{[Promotion Media].[All Media], [Product].[All Products].[Non-Consumable]}" }));
    }

    /**
     * Tests that TopPercent() operates succesfully on a
     * axis of crossjoined tuples.  previously, this would
     * fail with a ClassCastException in FunUtil.java.  bug 1440306
     */
    public void testTopPercentCrossjoin() {
        assertAxisReturns(
                fold(new String[] {
                    "{TopPercent(Crossjoin([Product].[Product Department].members,",
                    "[Time].[1997].children),10,[Measures].[Store Sales])}" }),
                fold(new String[] {
                    "{[Product].[All Products].[Food].[Produce], [Time].[1997].[Q4]}",
                    "{[Product].[All Products].[Food].[Produce], [Time].[1997].[Q1]}",
                    "{[Product].[All Products].[Food].[Produce], [Time].[1997].[Q3]}" }));
    }

    public void testCrossjoinNested() {
        assertAxisReturns(
                fold(new String[] {
                    "  CrossJoin(",
                    "    CrossJoin(",
                    "      [Gender].members,",
                    "      [Marital Status].members),",
                    "   {[Store], [Store].children})"}),

                fold(new String[] {
                    "{[Gender].[All Gender], [Marital Status].[All Marital Status], [Store].[All Stores]}",
                    "{[Gender].[All Gender], [Marital Status].[All Marital Status], [Store].[All Stores].[Canada]}",
                    "{[Gender].[All Gender], [Marital Status].[All Marital Status], [Store].[All Stores].[Mexico]}",
                    "{[Gender].[All Gender], [Marital Status].[All Marital Status], [Store].[All Stores].[USA]}",
                    "{[Gender].[All Gender], [Marital Status].[All Marital Status].[M], [Store].[All Stores]}",
                    "{[Gender].[All Gender], [Marital Status].[All Marital Status].[M], [Store].[All Stores].[Canada]}",
                    "{[Gender].[All Gender], [Marital Status].[All Marital Status].[M], [Store].[All Stores].[Mexico]}",
                    "{[Gender].[All Gender], [Marital Status].[All Marital Status].[M], [Store].[All Stores].[USA]}",
                    "{[Gender].[All Gender], [Marital Status].[All Marital Status].[S], [Store].[All Stores]}",
                    "{[Gender].[All Gender], [Marital Status].[All Marital Status].[S], [Store].[All Stores].[Canada]}",
                    "{[Gender].[All Gender], [Marital Status].[All Marital Status].[S], [Store].[All Stores].[Mexico]}",
                    "{[Gender].[All Gender], [Marital Status].[All Marital Status].[S], [Store].[All Stores].[USA]}",
                    "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status], [Store].[All Stores]}",
                    "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status], [Store].[All Stores].[Canada]}",
                    "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status], [Store].[All Stores].[Mexico]}",
                    "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status], [Store].[All Stores].[USA]}",
                    "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[M], [Store].[All Stores]}",
                    "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[M], [Store].[All Stores].[Canada]}",
                    "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[M], [Store].[All Stores].[Mexico]}",
                    "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[M], [Store].[All Stores].[USA]}",
                    "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S], [Store].[All Stores]}",
                    "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S], [Store].[All Stores].[Canada]}",
                    "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S], [Store].[All Stores].[Mexico]}",
                    "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S], [Store].[All Stores].[USA]}",
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status], [Store].[All Stores]}",
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status], [Store].[All Stores].[Canada]}",
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status], [Store].[All Stores].[Mexico]}",
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status], [Store].[All Stores].[USA]}",
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M], [Store].[All Stores]}",
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M], [Store].[All Stores].[Canada]}",
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M], [Store].[All Stores].[Mexico]}",
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M], [Store].[All Stores].[USA]}",
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Store].[All Stores]}",
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Store].[All Stores].[Canada]}",
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Store].[All Stores].[Mexico]}",
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Store].[All Stores].[USA]}"}));
    }

    public void testCrossjoinSingletonTuples() {
        assertAxisReturns("CrossJoin({([Gender].[M])}, {([Marital Status].[S])})",
                "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S]}");
    }

    public void testCrossjoinSingletonTuplesNested() {
        assertAxisReturns(
                "CrossJoin({([Gender].[M])}, CrossJoin({([Marital Status].[S])}, [Store].children))",
                fold(new String[] {
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Store].[All Stores].[Canada]}",
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Store].[All Stores].[Mexico]}",
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Store].[All Stores].[USA]}"}));
    }

    public void testCrossjoinAsterisk() {
        assertAxisReturns("{[Gender].[M]} * {[Marital Status].[S]}",
                "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S]}");
    }

    public void testCrossjoinAsteriskAssoc() {
        assertAxisReturns("Order({[Gender].Children} * {[Marital Status].Children} * {[Time].[1997].[Q2].Children}," +
                "[Measures].[Unit Sales])",
                fold(new String[] {
                    "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[M], [Time].[1997].[Q2].[4]}",
                    "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[M], [Time].[1997].[Q2].[6]}",
                    "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[M], [Time].[1997].[Q2].[5]}",
                    "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S], [Time].[1997].[Q2].[4]}",
                    "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S], [Time].[1997].[Q2].[5]}",
                    "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S], [Time].[1997].[Q2].[6]}",
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M], [Time].[1997].[Q2].[4]}",
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M], [Time].[1997].[Q2].[5]}",
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M], [Time].[1997].[Q2].[6]}",
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Time].[1997].[Q2].[6]}",
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Time].[1997].[Q2].[4]}",
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Time].[1997].[Q2].[5]}"}));
    }

    public void testCrossjoinAsteriskInsideBraces() {
        assertAxisReturns("{[Gender].[M] * [Marital Status].[S] * [Time].[1997].[Q2].Children}",
                fold(new String[] {
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Time].[1997].[Q2].[4]}",
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Time].[1997].[Q2].[5]}",
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Time].[1997].[Q2].[6]}"}));
    }

    public void testCrossJoinAsteriskQuery() {
        assertQueryReturns(
                fold(new String[] {
                    "SELECT {[Measures].members * [1997].children} ON COLUMNS,",
                    " {[Store].[USA].children * [Position].[All Position].children} DIMENSION PROPERTIES [Store].[Store SQFT] ON ROWS",
                    "FROM [HR]"}),
                fold(new String[] {
                    "Axis #0:",
                    "{}",
                    "Axis #1:",
                    "{[Measures].[Org Salary], [Time].[1997].[Q1]}",
                    "{[Measures].[Org Salary], [Time].[1997].[Q2]}",
                    "{[Measures].[Org Salary], [Time].[1997].[Q3]}",
                    "{[Measures].[Org Salary], [Time].[1997].[Q4]}",
                    "{[Measures].[Count], [Time].[1997].[Q1]}",
                    "{[Measures].[Count], [Time].[1997].[Q2]}",
                    "{[Measures].[Count], [Time].[1997].[Q3]}",
                    "{[Measures].[Count], [Time].[1997].[Q4]}",
                    "{[Measures].[Number of Employees], [Time].[1997].[Q1]}",
                    "{[Measures].[Number of Employees], [Time].[1997].[Q2]}",
                    "{[Measures].[Number of Employees], [Time].[1997].[Q3]}",
                    "{[Measures].[Number of Employees], [Time].[1997].[Q4]}",
                    "Axis #2:",
                    "{[Store].[All Stores].[USA].[CA], [Position].[All Position].[Middle Management]}",
                    "{[Store].[All Stores].[USA].[CA], [Position].[All Position].[Senior Management]}",
                    "{[Store].[All Stores].[USA].[CA], [Position].[All Position].[Store Full Time Staf]}",
                    "{[Store].[All Stores].[USA].[CA], [Position].[All Position].[Store Management]}",
                    "{[Store].[All Stores].[USA].[CA], [Position].[All Position].[Store Temp Staff]}",
                    "{[Store].[All Stores].[USA].[OR], [Position].[All Position].[Middle Management]}",
                    "{[Store].[All Stores].[USA].[OR], [Position].[All Position].[Senior Management]}",
                    "{[Store].[All Stores].[USA].[OR], [Position].[All Position].[Store Full Time Staf]}",
                    "{[Store].[All Stores].[USA].[OR], [Position].[All Position].[Store Management]}",
                    "{[Store].[All Stores].[USA].[OR], [Position].[All Position].[Store Temp Staff]}",
                    "{[Store].[All Stores].[USA].[WA], [Position].[All Position].[Middle Management]}",
                    "{[Store].[All Stores].[USA].[WA], [Position].[All Position].[Senior Management]}",
                    "{[Store].[All Stores].[USA].[WA], [Position].[All Position].[Store Full Time Staf]}",
                    "{[Store].[All Stores].[USA].[WA], [Position].[All Position].[Store Management]}",
                    "{[Store].[All Stores].[USA].[WA], [Position].[All Position].[Store Temp Staff]}",
                    "Row #0: $275.40",
                    "Row #0: $275.40",
                    "Row #0: $275.40",
                    "Row #0: $275.40",
                    "Row #0: 27",
                    "Row #0: 27",
                    "Row #0: 27",
                    "Row #0: 27",
                    "Row #0: 9",
                    "Row #0: 9",
                    "Row #0: 9",
                    "Row #0: 9",
                    "Row #1: $837.00",
                    "Row #1: $837.00",
                    "Row #1: $837.00",
                    "Row #1: $837.00",
                    "Row #1: 24",
                    "Row #1: 24",
                    "Row #1: 24",
                    "Row #1: 24",
                    "Row #1: 8",
                    "Row #1: 8",
                    "Row #1: 8",
                    "Row #1: 8",
                    "Row #2: $1,728.45",
                    "Row #2: $1,727.02",
                    "Row #2: $1,727.72",
                    "Row #2: $1,726.55",
                    "Row #2: 357",
                    "Row #2: 357",
                    "Row #2: 357",
                    "Row #2: 357",
                    "Row #2: 119",
                    "Row #2: 119",
                    "Row #2: 119",
                    "Row #2: 119",
                    "Row #3: $473.04",
                    "Row #3: $473.04",
                    "Row #3: $473.04",
                    "Row #3: $473.04",
                    "Row #3: 51",
                    "Row #3: 51",
                    "Row #3: 51",
                    "Row #3: 51",
                    "Row #3: 17",
                    "Row #3: 17",
                    "Row #3: 17",
                    "Row #3: 17",
                    "Row #4: $401.35",
                    "Row #4: $405.73",
                    "Row #4: $400.61",
                    "Row #4: $402.31",
                    "Row #4: 120",
                    "Row #4: 120",
                    "Row #4: 120",
                    "Row #4: 120",
                    "Row #4: 40",
                    "Row #4: 40",
                    "Row #4: 40",
                    "Row #4: 40",
                    "Row #5: (null)",
                    "Row #5: (null)",
                    "Row #5: (null)",
                    "Row #5: (null)",
                    "Row #5: (null)",
                    "Row #5: (null)",
                    "Row #5: (null)",
                    "Row #5: (null)",
                    "Row #5: (null)",
                    "Row #5: (null)",
                    "Row #5: (null)",
                    "Row #5: (null)",
                    "Row #6: (null)",
                    "Row #6: (null)",
                    "Row #6: (null)",
                    "Row #6: (null)",
                    "Row #6: (null)",
                    "Row #6: (null)",
                    "Row #6: (null)",
                    "Row #6: (null)",
                    "Row #6: (null)",
                    "Row #6: (null)",
                    "Row #6: (null)",
                    "Row #6: (null)",
                    "Row #7: $1,343.62",
                    "Row #7: $1,342.61",
                    "Row #7: $1,342.57",
                    "Row #7: $1,343.65",
                    "Row #7: 279",
                    "Row #7: 279",
                    "Row #7: 279",
                    "Row #7: 279",
                    "Row #7: 93",
                    "Row #7: 93",
                    "Row #7: 93",
                    "Row #7: 93",
                    "Row #8: $286.74",
                    "Row #8: $286.74",
                    "Row #8: $286.74",
                    "Row #8: $286.74",
                    "Row #8: 30",
                    "Row #8: 30",
                    "Row #8: 30",
                    "Row #8: 30",
                    "Row #8: 10",
                    "Row #8: 10",
                    "Row #8: 10",
                    "Row #8: 10",
                    "Row #9: $333.20",
                    "Row #9: $332.65",
                    "Row #9: $331.28",
                    "Row #9: $332.43",
                    "Row #9: 99",
                    "Row #9: 99",
                    "Row #9: 99",
                    "Row #9: 99",
                    "Row #9: 33",
                    "Row #9: 33",
                    "Row #9: 33",
                    "Row #9: 33",
                    "Row #10: (null)",
                    "Row #10: (null)",
                    "Row #10: (null)",
                    "Row #10: (null)",
                    "Row #10: (null)",
                    "Row #10: (null)",
                    "Row #10: (null)",
                    "Row #10: (null)",
                    "Row #10: (null)",
                    "Row #10: (null)",
                    "Row #10: (null)",
                    "Row #10: (null)",
                    "Row #11: (null)",
                    "Row #11: (null)",
                    "Row #11: (null)",
                    "Row #11: (null)",
                    "Row #11: (null)",
                    "Row #11: (null)",
                    "Row #11: (null)",
                    "Row #11: (null)",
                    "Row #11: (null)",
                    "Row #11: (null)",
                    "Row #11: (null)",
                    "Row #11: (null)",
                    "Row #12: $2,768.60",
                    "Row #12: $2,769.18",
                    "Row #12: $2,766.78",
                    "Row #12: $2,769.50",
                    "Row #12: 579",
                    "Row #12: 579",
                    "Row #12: 579",
                    "Row #12: 579",
                    "Row #12: 193",
                    "Row #12: 193",
                    "Row #12: 193",
                    "Row #12: 193",
                    "Row #13: $736.29",
                    "Row #13: $736.29",
                    "Row #13: $736.29",
                    "Row #13: $736.29",
                    "Row #13: 81",
                    "Row #13: 81",
                    "Row #13: 81",
                    "Row #13: 81",
                    "Row #13: 27",
                    "Row #13: 27",
                    "Row #13: 27",
                    "Row #13: 27",
                    "Row #14: $674.70",
                    "Row #14: $674.54",
                    "Row #14: $676.25",
                    "Row #14: $676.48",
                    "Row #14: 201",
                    "Row #14: 201",
                    "Row #14: 201",
                    "Row #14: 201",
                    "Row #14: 67",
                    "Row #14: 67",
                    "Row #14: 67",
                    "Row #14: 67",
                    ""}));
    }

    public void testDescendantsM() {
        assertAxisReturns("Descendants([Time].[1997].[Q1])",
                fold(new String[] {
                    "[Time].[1997].[Q1]",
                    "[Time].[1997].[Q1].[1]",
                    "[Time].[1997].[Q1].[2]",
                    "[Time].[1997].[Q1].[3]"}));
    }

    public void testDescendantsDepends() {
        assertSetExprDependsOn("Descendants([Time].CurrentMember)",
                "{[Time]}");
    }

    /**
     * Asserts that an MDX set-valued expression depends upon a given list of
     * dimensions.
     */
    public void assertSetExprDependsOn(String expr, String dimList) {
        // Construct a query, and mine it for a parsed expression.
        // Use a fresh connection, because some tests define their own dims.
        final boolean fresh = true;
        final Connection connection =
                getTestContext().getFoodMartConnection(fresh);
        final String queryString =
                "SELECT {" + expr + "} ON COLUMNS FROM [Sales]";
        final Query query = connection.parseQuery(queryString);
        query.resolve();
        final Exp expression = query.getAxes()[0].getSet();

        // Build a list of the dimensions which the expression depends upon,
        // and check that it is as expected.
        checkDependsOn(query, expression, dimList, false);
    }

    /**
     * Asserts that an MDX member-valued depends upon a given list of
     * dimensions.
     */
    public void assertMemberExprDependsOn(String expr, String dimList) {
        assertSetExprDependsOn("{" + expr + "}", dimList);
    }

    /**
     * Asserts that an MDX expression depends upon a given list of dimensions.
     */
    public void assertExprDependsOn(String expr, String dimList) {
        // Construct a query, and mine it for a parsed expression.
        // Use a fresh connection, because some tests define their own dims.
        final boolean fresh = true;
        final Connection connection =
                getTestContext().getFoodMartConnection(fresh);
        final String queryString =
                "WITH MEMBER [Measures].[Foo] AS " +
                Util.singleQuoteString(expr) +
                " SELECT FROM [Sales]";
        final Query query = connection.parseQuery(queryString);
        query.resolve();
        final Formula formula = query.getFormulas()[0];
        final Exp expression = formula.getExpression();

        // Build a list of the dimensions which the expression depends upon,
        // and check that it is as expected.
        checkDependsOn(query, expression, dimList, true);
    }

    private void checkDependsOn(
            final Query query,
            final Exp expression,
            String expectedDimList,
            final boolean scalar) {
        final Calc calc = query.compileExpression(expression, scalar);
        final Dimension[] dimensions = query.getCube().getDimensions();
        StringBuffer buf = new StringBuffer("{");
        int dependCount = 0;
        for (int i = 0; i < dimensions.length; i++) {
            Dimension dimension = dimensions[i];
            if (calc.dependsOn(dimension)) {
                if (dependCount++ > 0) {
                    buf.append(", ");
                }
                buf.append(dimension.getUniqueName());
            }
        }
        buf.append("}");
        String actualDimList = buf.toString();
        assertEquals(expectedDimList, actualDimList);
    }

    public void testDescendantsML() {
        assertAxisReturns("Descendants([Time].[1997], [Time].[Month])",
                months);
    }

    public void testDescendantsMLSelf() {
        assertAxisReturns("Descendants([Time].[1997], [Time].[Quarter], SELF)",
                quarters);
    }

    public void testDescendantsMLLeaves() {
        assertAxisReturns("Descendants([Time].[1997], [Time].[Year], LEAVES)",
                "");
        assertAxisReturns("Descendants([Time].[1997], [Time].[Quarter], LEAVES)",
                "");
        assertAxisReturns("Descendants([Time].[1997], [Time].[Month], LEAVES)",
                months);

        assertAxisReturns("Descendants([Gender], [Gender].[Gender], leaves)",
                "[Gender].[All Gender].[F]" + nl +
                "[Gender].[All Gender].[M]");
    }

    public void testDescendantsMLLeavesRagged() {
        // no cities are at leaf level
        final TestContext raggedContext = getTestContext("[Sales Ragged]");
        raggedContext.assertAxisReturns(
                "Descendants([Store].[Israel], [Store].[Store City], leaves)",
                "");

        // all cities are leaves
        raggedContext.assertAxisReturns(
                "Descendants([Geography].[Israel], [Geography].[City], leaves)",
                "[Geography].[All Geographys].[Israel].[Israel].[Haifa]" + nl +
                "[Geography].[All Geographys].[Israel].[Israel].[Tel Aviv]");

        // No state is a leaf (not even Israel, which is both a country and a
        // a state, or Vatican, with is a country/state/city)
        raggedContext.assertAxisReturns(
                "Descendants([Geography], [Geography].[State], leaves)",
                "");

        // The Vatican is a nation with no children (they're all celibate,
        // you know).
        raggedContext.assertAxisReturns(
                "Descendants([Geography], [Geography].[Country], leaves)",
                "[Geography].[All Geographys].[Vatican]");
    }

    public void testDescendantsMNLeaves() {
        // leaves at depth 0 returns the member itself
        assertAxisReturns("Descendants([Time].[1997].[Q2].[4], 0, Leaves)",
                "[Time].[1997].[Q2].[4]");

        // leaves at depth > 0 returns the member itself
        assertAxisReturns("Descendants([Time].[1997].[Q2].[4], 100, Leaves)",
                "[Time].[1997].[Q2].[4]");

        // leaves at depth < 0 returns all descendants
        assertAxisReturns("Descendants([Time].[1997].[Q2], -1, Leaves)",
                fold(new String[] {
                    "[Time].[1997].[Q2].[4]",
                    "[Time].[1997].[Q2].[5]",
                    "[Time].[1997].[Q2].[6]"}));

        // leaves at depth 0 returns the member itself
        assertAxisReturns("Descendants([Time].[1997].[Q2], 0, Leaves)",
                fold(new String[] {
                    "[Time].[1997].[Q2].[4]",
                    "[Time].[1997].[Q2].[5]",
                    "[Time].[1997].[Q2].[6]"}));

        assertAxisReturns("Descendants([Time].[1997].[Q2], 3, Leaves)",
                fold(new String[] {
                    "[Time].[1997].[Q2].[4]",
                    "[Time].[1997].[Q2].[5]",
                    "[Time].[1997].[Q2].[6]"}));
    }

    public void testDescendantsMLSelfBefore() {
        assertAxisReturns("Descendants([Time].[1997], [Time].[Quarter], SELF_AND_BEFORE)",
                year1997 + nl + quarters);
    }

    public void testDescendantsMLSelfBeforeAfter() {
        assertAxisReturns("Descendants([Time].[1997], [Time].[Quarter], SELF_BEFORE_AFTER)",
                hierarchized1997);
    }

    public void testDescendantsMLBefore() {
        assertAxisReturns("Descendants([Time].[1997], [Time].[Quarter], BEFORE)",
                year1997);
    }

    public void testDescendantsMLBeforeAfter() {
        assertAxisReturns("Descendants([Time].[1997], [Time].[Quarter], BEFORE_AND_AFTER)",
                year1997 + nl + months);
    }

    public void testDescendantsMLAfter() {
        assertAxisReturns("Descendants([Time].[1997], [Time].[Quarter], AFTER)",
                months);
    }

    public void testDescendantsMLAfterEnd() {
        assertAxisReturns("Descendants([Time].[1997], [Time].[Month], AFTER)",
                "");
    }

    public void testDescendantsM0() {
        assertAxisReturns("Descendants([Time].[1997], 0)",
                year1997);
    }

    public void testDescendantsM2() {
        assertAxisReturns("Descendants([Time].[1997], 2)",
                months);
    }

    public void testDescendantsM2Self() {
        assertAxisReturns("Descendants([Time].[1997], 2, Self)",
                months);
    }

    public void testDescendantsM2Leaves() {
        assertAxisReturns("Descendants([Time].[1997], 2, Leaves)",
                months);
    }

    public void testDescendantsMFarLeaves() {
        assertAxisReturns("Descendants([Time].[1997], 10000, Leaves)",
                months);
    }

    public void testDescendantsMFarSelf() {
        assertAxisReturns("Descendants([Time].[1997], 10000, Self)",
                "");
    }

    public void testDescendantsMNY() {
        assertAxisReturns("Descendants([Time].[1997], 1, BEFORE_AND_AFTER)",
                year1997 + nl + months);
    }

    public void testDescendantsParentChild() {
        getTestContext("HR").assertAxisReturns(
                "Descendants([Employees], 2)",
                fold(new String[] {
                    "[Employees].[All Employees].[Sheri Nowmer].[Derrick Whelply]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Maya Gutierrez]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Roberta Damstra]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Rebecca Kanagaki]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Donna Arnold]"}));
    }

    public void testDescendantsParentChildBefore() {
        getTestContext("HR").assertAxisReturns(
                "Descendants([Employees], 2, BEFORE)",
                fold(new String[] {
                    "[Employees].[All Employees]",
                    "[Employees].[All Employees].[Sheri Nowmer]"}));
    }

    public void testDescendantsParentChildLeaves() {
        final TestContext testContext = getTestContext("HR");

        // leaves, restricted by level
        testContext.assertAxisReturns(
                "Descendants([Employees].[All Employees].[Sheri Nowmer].[Michael Spence], [Employees].[Employee Id], LEAVES)",
                fold(new String[] {
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[John Brooks]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Todd Logan]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Joshua Several]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[James Thomas]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Robert Vessa]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Bronson Jacobs]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Rebecca Barley]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Emilio Alvaro]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Becky Waters]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[A. Joyce Jarvis]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Ruby Sue Styles]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Lisa Roy]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Ingrid Burkhardt]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Todd Whitney]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Barbara Wisnewski]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Karren Burkhardt]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[John Long]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Edwin Olenzek]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Jessie Valerio]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Robert Ahlering]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Megan Burke]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Karel Bates]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[James Tran]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Shelley Crow]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Anne Sims]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Clarence Tatman]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Jan Nelsen]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Jeanie Glenn]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Peggy Smith]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Tish Duff]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Anita Lucero]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Stephen Burton]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Amy Consentino]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Stacie Mcanich]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Mary Browning]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Alexandra Wellington]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Cory Bacugalupi]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Stacy Rizzi]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Mike White]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Marty Simpson]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Robert Jones]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Raul Casts]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Bridget Browqett]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Kay Kartz]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Jeanette Cole]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Phyllis Huntsman]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Hannah Arakawa]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Wathalee Steuber]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Pamela Cox]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Helen Lutes]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Linda Ecoffey]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Katherine Swint]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Dianne Slattengren]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Ronald Heymsfield]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Steven Whitehead]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[William Sotelo]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Beth Stanley]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Jill Markwood]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Mildred Valentine]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Suzann Reams]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Audrey Wold]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Susan French]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Trish Pederson]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Eric Renn]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Elizabeth Catalano]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Eric Coleman]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Catherine Abel]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Emilo Miller]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Hazel Walker]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Dianne Collins].[Lawrence Hurkett].[Sara Pettengill].[Linda Blasingame]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Dianne Collins].[Lawrence Hurkett].[Sara Pettengill].[Jackie Blackwell]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Dianne Collins].[Lawrence Hurkett].[Sara Pettengill].[John Ortiz]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Dianne Collins].[Lawrence Hurkett].[Sara Pettengill].[Stacey Tearpak]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Dianne Collins].[Lawrence Hurkett].[Sara Pettengill].[Fannye Weber]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Dianne Collins].[Lawrence Hurkett].[Sara Pettengill].[Diane Kabbes]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Dianne Collins].[Lawrence Hurkett].[Sara Pettengill].[Brenda Heaney]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Dianne Collins].[Lawrence Hurkett].[Sara Pettengill].[Judith Karavites]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Dianne Collins].[Lawrence Hurkett].[Jauna Elson]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Dianne Collins].[Lawrence Hurkett].[Nancy Hirota]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Dianne Collins].[Lawrence Hurkett].[Marie Moya]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Dianne Collins].[Lawrence Hurkett].[Nicky Chesnut]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Dianne Collins].[Lawrence Hurkett].[Karen Hall]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Dianne Collins].[Lawrence Hurkett].[Greg Narberes]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Dianne Collins].[Lawrence Hurkett].[Anna Townsend]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Dianne Collins].[Lawrence Hurkett].[Carol Ann Rockne]"}));

        // leaves, restricted by depth
        testContext.assertAxisReturns(
                "Descendants([Employees], 1, LEAVES)", "");
        testContext.assertAxisReturns(
                "Descendants([Employees], 2, LEAVES)",
                fold(new String[] {
                    "[Employees].[All Employees].[Sheri Nowmer].[Roberta Damstra].[Jennifer Cooper]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Roberta Damstra].[Peggy Petty]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Roberta Damstra].[Jessica Olguin]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Roberta Damstra].[Phyllis Burchett]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Rebecca Kanagaki].[Juanita Sharp]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Rebecca Kanagaki].[Sandra Brunner]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz].[Ernest Staton]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz].[Rose Sims]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz].[Lauretta De Carlo]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz].[Mary Williams]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz].[Terri Burke]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz].[Audrey Osborn]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz].[Brian Binai]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz].[Concepcion Lozada]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Donna Arnold].[Howard Bechard]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Donna Arnold].[Doris Carter]"}));

        testContext.assertAxisReturns(
                "Descendants([Employees], 3, LEAVES)",
                fold(new String[] {
                    "[Employees].[All Employees].[Sheri Nowmer].[Roberta Damstra].[Jennifer Cooper]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Roberta Damstra].[Peggy Petty]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Roberta Damstra].[Jessica Olguin]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Roberta Damstra].[Phyllis Burchett]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Rebecca Kanagaki].[Juanita Sharp]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Rebecca Kanagaki].[Sandra Brunner]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz].[Ernest Staton]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz].[Rose Sims]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz].[Lauretta De Carlo]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz].[Mary Williams]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz].[Terri Burke]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz].[Audrey Osborn]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz].[Brian Binai]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz].[Concepcion Lozada]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Donna Arnold].[Howard Bechard]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Donna Arnold].[Doris Carter]"}));

        // note that depth is RELATIVE to the starting member
        testContext.assertAxisReturns(
                "Descendants([Employees].[All Employees].[Sheri Nowmer].[Roberta Damstra], 1, LEAVES)",
                fold(new String[] {
                    "[Employees].[All Employees].[Sheri Nowmer].[Roberta Damstra].[Jennifer Cooper]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Roberta Damstra].[Peggy Petty]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Roberta Damstra].[Jessica Olguin]",
                    "[Employees].[All Employees].[Sheri Nowmer].[Roberta Damstra].[Phyllis Burchett]"}));

        // Howard Bechard is a leaf member -- appears even at depth 0
        testContext.assertAxisReturns(
                "Descendants([Employees].[All Employees].[Sheri Nowmer].[Donna Arnold].[Howard Bechard], 0, LEAVES)",
                "[Employees].[All Employees].[Sheri Nowmer].[Donna Arnold].[Howard Bechard]");
        testContext.assertAxisReturns(
                "Descendants([Employees].[All Employees].[Sheri Nowmer].[Donna Arnold].[Howard Bechard], 1, LEAVES)",
                "[Employees].[All Employees].[Sheri Nowmer].[Donna Arnold].[Howard Bechard]");

        testContext.assertExprReturns("Count(Descendants([Employees], 2, LEAVES))", "16");
        testContext.assertExprReturns("Count(Descendants([Employees], 3, LEAVES))", "16");
        testContext.assertExprReturns("Count(Descendants([Employees], 4, LEAVES))", "63");
        testContext.assertExprReturns("Count(Descendants([Employees], 999, LEAVES))", "1,044");

        // negative depth acts like +infinity (per MSAS)
        // Run the test several times because we had a non-deterministic bug here.
        for (int i = 0; i < 100; ++i) {
            testContext.assertExprReturns("Count(Descendants([Employees], -1, LEAVES))", "1,044");
        }
    }

    public void testDescendantsSBA() {
        assertAxisReturns("Descendants([Time].[1997], 1, SELF_BEFORE_AFTER)",
                hierarchized1997);
    }

    public void testRange() {
        assertAxisReturns("[Time].[1997].[Q1].[2] : [Time].[1997].[Q2].[5]",
                fold(new String[] {
                    "[Time].[1997].[Q1].[2]",
                    "[Time].[1997].[Q1].[3]",
                    "[Time].[1997].[Q2].[4]",
                    "[Time].[1997].[Q2].[5]"})); // not parents
    }

    /**
     * Large dimensions use a different member reader, therefore need to
     * be tested separately.
     */
    public void testRangeLarge() {
        assertAxisReturns("[Customers].[USA].[CA].[San Francisco] : [Customers].[USA].[WA].[Bellingham]",
                fold(new String[] {
                    "[Customers].[All Customers].[USA].[CA].[San Francisco]",
                    "[Customers].[All Customers].[USA].[CA].[San Gabriel]",
                    "[Customers].[All Customers].[USA].[CA].[San Jose]",
                    "[Customers].[All Customers].[USA].[CA].[Santa Cruz]",
                    "[Customers].[All Customers].[USA].[CA].[Santa Monica]",
                    "[Customers].[All Customers].[USA].[CA].[Spring Valley]",
                    "[Customers].[All Customers].[USA].[CA].[Torrance]",
                    "[Customers].[All Customers].[USA].[CA].[West Covina]",
                    "[Customers].[All Customers].[USA].[CA].[Woodland Hills]",
                    "[Customers].[All Customers].[USA].[OR].[Albany]",
                    "[Customers].[All Customers].[USA].[OR].[Beaverton]",
                    "[Customers].[All Customers].[USA].[OR].[Corvallis]",
                    "[Customers].[All Customers].[USA].[OR].[Lake Oswego]",
                    "[Customers].[All Customers].[USA].[OR].[Lebanon]",
                    "[Customers].[All Customers].[USA].[OR].[Milwaukie]",
                    "[Customers].[All Customers].[USA].[OR].[Oregon City]",
                    "[Customers].[All Customers].[USA].[OR].[Portland]",
                    "[Customers].[All Customers].[USA].[OR].[Salem]",
                    "[Customers].[All Customers].[USA].[OR].[W. Linn]",
                    "[Customers].[All Customers].[USA].[OR].[Woodburn]",
                    "[Customers].[All Customers].[USA].[WA].[Anacortes]",
                    "[Customers].[All Customers].[USA].[WA].[Ballard]",
                    "[Customers].[All Customers].[USA].[WA].[Bellingham]"}));
    }

    public void testRangeStartEqualsEnd() {
        assertAxisReturns("[Time].[1997].[Q3].[7] : [Time].[1997].[Q3].[7]",
                "[Time].[1997].[Q3].[7]");
    }

    public void testRangeStartEqualsEndLarge() {
        assertAxisReturns("[Customers].[USA].[CA] : [Customers].[USA].[CA]",
                "[Customers].[All Customers].[USA].[CA]");
    }

    public void testRangeEndBeforeStart() {
        assertAxisReturns("[Time].[1997].[Q3].[7] : [Time].[1997].[Q2].[5]",
                fold(new String[] {
                    "[Time].[1997].[Q2].[5]",
                    "[Time].[1997].[Q2].[6]",
                    "[Time].[1997].[Q3].[7]"})); // same as if reversed
    }

    public void testRangeEndBeforeStartLarge() {
        assertAxisReturns("[Customers].[USA].[WA] : [Customers].[USA].[CA]",
                fold(new String[] {
                    "[Customers].[All Customers].[USA].[CA]",
                    "[Customers].[All Customers].[USA].[OR]",
                    "[Customers].[All Customers].[USA].[WA]"}));
    }

    public void testRangeBetweenDifferentLevelsIsError() {
        assertAxisThrows("[Time].[1997].[Q2] : [Time].[1997].[Q2].[5]",
                "Members must belong to the same level");
    }

    public void testRangeBoundedByAll() {
        assertAxisReturns("[Gender] : [Gender]",
                "[Gender].[All Gender]");
    }

    public void testRangeBoundedByAllLarge() {
        assertAxisReturns("[Customers].DefaultMember : [Customers]",
                "[Customers].[All Customers]");
    }

    public void testRangeBoundedByNull() {
        assertAxisReturns("[Gender].[F] : [Gender].[M].NextMember",
                "");
    }

    public void testRangeBoundedByNullLarge() {
        assertAxisReturns("[Customers].PrevMember : [Customers].[USA].[OR]",
                "");
    }

    public void testSetContainingLevelFails() {
        assertAxisThrows("[Store].[Store City]",
                "No function matches signature '{<Level>}'");
    }

    public void testBug715177() {
        assertQueryReturns(
                fold(new String[] {
                    "WITH MEMBER [Product].[All Products].[Non-Consumable].[Other] AS",
                    " 'Sum( Except( [Product].[Product Department].Members,",
                    "       TopCount( [Product].[Product Department].Members, 3 )),",
                    "       Measures.[Unit Sales] )'",
                    "SELECT",
                    "  { [Measures].[Unit Sales] } ON COLUMNS,",
                    "  { TopCount( [Product].[Product Department].Members,3 ),",
                    "              [Product].[All Products].[Non-Consumable].[Other] } ON ROWS",
                    "FROM [Sales]"}),
                fold(new String[] {
                    "Axis #0:",
                    "{}",
                    "Axis #1:",
                    "{[Measures].[Unit Sales]}",
                    "Axis #2:",
                    "{[Product].[All Products].[Drink].[Alcoholic Beverages]}",
                    "{[Product].[All Products].[Drink].[Beverages]}",
                    "{[Product].[All Products].[Drink].[Dairy]}",
                    "{[Product].[All Products].[Non-Consumable].[Other]}",
                    "Row #0: 6,838",
                    "Row #1: 13,573",
                    "Row #2: 4,186",
                    "Row #3: 242,176" + nl}));
    }

    public void testBug714707() {
        // Same issue as bug 715177 -- "children" returns immutable
        // list, which set operator must make mutable.
        assertAxisReturns(
                "{[Store].[USA].[CA].children, [Store].[USA]}",
                fold(new String[] {
                    "[Store].[All Stores].[USA].[CA].[Alameda]",
                    "[Store].[All Stores].[USA].[CA].[Beverly Hills]",
                    "[Store].[All Stores].[USA].[CA].[Los Angeles]",
                    "[Store].[All Stores].[USA].[CA].[San Diego]",
                    "[Store].[All Stores].[USA].[CA].[San Francisco]",
                    "[Store].[All Stores].[USA]"}));
    }

    public void testBug715177c() {
        assertAxisReturns("Order(TopCount({[Store].[USA].[CA].children}, [Measures].[Unit Sales], 2), [Measures].[Unit Sales])",
                fold(new String[] {
                    "[Store].[All Stores].[USA].[CA].[Alameda]",
                    "[Store].[All Stores].[USA].[CA].[San Francisco]",
                    "[Store].[All Stores].[USA].[CA].[Beverly Hills]",
                    "[Store].[All Stores].[USA].[CA].[San Diego]",
                    "[Store].[All Stores].[USA].[CA].[Los Angeles]"}));
    }

    public void testFormatFixed() {
        assertExprReturns("Format(12.2, \"#,##0.00\")", "12.20");
    }

    public void testFormatVariable() {
        assertExprReturns("Format(1234.5, \"#,#\" || \"#0.00\")", "1,234.50");
    }

    public void testIIf() {
        assertExprReturns("IIf(([Measures].[Unit Sales],[Product].[Drink].[Alcoholic Beverages].[Beer and Wine]) > 100, \"Yes\",\"No\")",
            "Yes");
    }

    public void testDimensionCaption() {
        assertExprReturns("[Time].[1997].Dimension.Caption", "Time");
    }

    public void testHierarchyCaption() {
        assertExprReturns("[Time].[1997].Hierarchy.Caption", "Time");
    }

    public void testLevelCaption() {
        assertExprReturns("[Time].[1997].Level.Caption", "Year");
    }

    public void testMemberCaption() {
        assertExprReturns("[Time].[1997].Caption", "1997");
    }


    public void testDimensionName() {
        assertExprReturns("[Time].[1997].Dimension.Name", "Time");
    }

    public void testHierarchyName() {
        assertExprReturns("[Time].[1997].Hierarchy.Name", "Time");
    }

    public void testLevelName() {
        assertExprReturns("[Time].[1997].Level.Name", "Year");
    }

    public void testMemberName() {
        assertExprReturns("[Time].[1997].Name", "1997");
        // dimension name
        assertExprReturns("[Store].Name", "Store");
        // member name
        assertExprReturns("[Store].DefaultMember.Name", "All Stores");
        // name of null member
        assertExprReturns("[Store].Parent.Name", "#Null");
    }


    public void testDimensionUniqueName() {
        assertExprReturns("[Gender].DefaultMember.Dimension.UniqueName", "[Gender]");
    }


    public void testHierarchyUniqueName() {
        assertExprReturns("[Gender].DefaultMember.Hierarchy.UniqueName", "[Gender]");
    }


    public void testLevelUniqueName() {
        assertExprReturns("[Gender].DefaultMember.Level.UniqueName", "[Gender].[(All)]");
    }


    public void testMemberUniqueName() {
        assertExprReturns("[Gender].DefaultMember.UniqueName", "[Gender].[All Gender]");
    }

    public void testMemberUniqueNameOfNull() {
        assertExprReturns("[Measures].[Unit Sales].FirstChild.UniqueName", "[Measures].[#Null]"); // MSOLAP gives "" here
    }

    public void testCoalesceEmptyDepends() {
        assertExprDependsOn("coalesceempty([Time].[1997], [Gender].[M])",
                allDims());
        assertExprDependsOn("coalesceempty(([Measures].[Unit Sales], [Time].[1997]), ([Measures].[Store Sales], [Time].[1997].[Q2]))",
                allDimsExcept(new String[] {"[Measures]", "[Time]"}));
    }

    public void testCoalesceEmpty() {
        // [DF] is all null and [WA] has numbers for 1997 but not for 1998.
        Result result = executeQuery(
                fold(new String[] {
                    "with",
                    "    member Measures.[Coal1] as 'coalesceempty(([Time].[1997], Measures.[Store Sales]), ([Time].[1998], Measures.[Store Sales]))'",
                    "    member Measures.[Coal2] as 'coalesceempty(([Time].[1997], Measures.[Unit Sales]), ([Time].[1998], Measures.[Unit Sales]))'",
                    "select ",
                    "    {Measures.[Coal1], Measures.[Coal2]} on columns,",
                    "    {[Store].[All Stores].[Mexico].[DF], [Store].[All Stores].[USA].[WA]} on rows",
                    "from ",
                    "    [Sales]"}));

        checkDataResults(
                new Double[][] {
                    {null, null},
                    {new Double(263793.22), new Double(124366)}
                },
                result,
                0.001);

        result = executeQuery(
                fold(new String[] {
                    "with",
                    "    member Measures.[Sales Per Customer] as 'Measures.[Sales Count] / Measures.[Customer Count]'",
                    "    member Measures.[Coal] as 'coalesceempty(([Measures].[Sales Per Customer], [Store].[All Stores].[Mexico].[DF]),",
                    "        Measures.[Sales Per Customer])'",
                    "select ",
                    "    {Measures.[Sales Per Customer], Measures.[Coal]} on columns,",
                    "    {[Store].[All Stores].[Mexico].[DF], [Store].[All Stores].[USA].[WA]} on rows",
                    "from ",
                    "    [Sales]",
                    "where",
                    "    ([Time].[1997].[Q2])"}));

        checkDataResults(new Double[][] {
            { null, null },
            { new Double(8.963), new Double(8.963) }
        },
                result,
                0.001);

        result = executeQuery(
                fold(new String[] {
                    "with",
                    "    member Measures.[Sales Per Customer] as 'Measures.[Sales Count] / Measures.[Customer Count]'",
                    "    member Measures.[Coal] as 'coalesceempty(([Measures].[Sales Per Customer], [Store].[All Stores].[Mexico].[DF]),",
                    "        ([Measures].[Sales Per Customer], [Store].[All Stores].[Mexico].[DF]),",
                    "        ([Measures].[Sales Per Customer], [Store].[All Stores].[Mexico].[DF]),",
                    "        ([Measures].[Sales Per Customer], [Store].[All Stores].[Mexico].[DF]),",
                    "        ([Measures].[Sales Per Customer], [Store].[All Stores].[Mexico].[DF]),",
                    "        ([Measures].[Sales Per Customer], [Store].[All Stores].[Mexico].[DF]),",
                    "        ([Measures].[Sales Per Customer], [Store].[All Stores].[Mexico].[DF]),",
                    "        ([Measures].[Sales Per Customer], [Store].[All Stores].[Mexico].[DF]),",
                    "        Measures.[Sales Per Customer])'",
                    "select ",
                    "    {Measures.[Sales Per Customer], Measures.[Coal]} on columns,",
                    "    {[Store].[All Stores].[Mexico].[DF], [Store].[All Stores].[USA].[WA]} on rows",
                    "from ",
                    "    [Sales]",
                    "where",
                    "    ([Time].[1997].[Q2])"}));

        checkDataResults(new Double[][] {
            { null, null },
            { new Double(8.963), new Double(8.963) }
        },
                result,
                0.001);
    }

    public void testBrokenContextBug() {
        Result result = executeQuery(
                fold(new String[] {
                    "with",
                    "    member Measures.[Sales Per Customer] as 'Measures.[Sales Count] / Measures.[Customer Count]'",
                    "    member Measures.[Coal] as 'coalesceempty(([Measures].[Sales Per Customer], [Store].[All Stores].[Mexico].[DF]),",
                    "        Measures.[Sales Per Customer])'",
                    "select ",
                    "    {Measures.[Coal]} on columns,",
                    "    {[Store].[All Stores].[USA].[WA]} on rows",
                    "from ",
                    "    [Sales]",
                    "where",
                    "    ([Time].[1997].[Q2])"}));

        checkDataResults(new Double[][] {{new Double(8.963)}}, result, 0.001);

    }

    /**
     * Tests the function <code>&lt;Set&gt;.Item(&lt;Integer&gt;)</code>.
     */
    public void testSetItemInt() {
        assertAxisReturns("{[Customers].[All Customers].[USA].[OR].[Lebanon].[Mary Frances Christian]}.Item(0)",
                "[Customers].[All Customers].[USA].[OR].[Lebanon].[Mary Frances Christian]");

        assertAxisReturns("{[Customers].[All Customers].[USA],"
                + "[Customers].[All Customers].[USA].[WA],"
                + "[Customers].[All Customers].[USA].[CA],"
                + "[Customers].[All Customers].[USA].[OR].[Lebanon].[Mary Frances Christian]}.Item(2)",
                "[Customers].[All Customers].[USA].[CA]");

        assertAxisReturns("{[Customers].[All Customers].[USA],"
                + "[Customers].[All Customers].[USA].[WA],"
                + "[Customers].[All Customers].[USA].[CA],"
                + "[Customers].[All Customers].[USA].[OR].[Lebanon].[Mary Frances Christian]}.Item(100 / 50 - 1)",
                "[Customers].[All Customers].[USA].[WA]");

        assertAxisReturns("{([Time].[1997].[Q1].[1], [Customers].[All Customers].[USA]),"
                + "([Time].[1997].[Q1].[2], [Customers].[All Customers].[USA].[WA]),"
                + "([Time].[1997].[Q1].[3], [Customers].[All Customers].[USA].[CA]),"
                + "([Time].[1997].[Q2].[4], [Customers].[All Customers].[USA].[OR].[Lebanon].[Mary Frances Christian])}"
                + ".Item(100 / 50 - 1)",
                "{[Time].[1997].[Q1].[2], [Customers].[All Customers].[USA].[WA]}");

        // given index out of bounds, item returns null
        assertAxisReturns("{[Customers].[All Customers].[USA],"
                + "[Customers].[All Customers].[USA].[WA],"
                + "[Customers].[All Customers].[USA].[CA],"
                + "[Customers].[All Customers].[USA].[OR].[Lebanon].[Mary Frances Christian]}.Item(-1)",
                "");

        // given index out of bounds, item returns null
        assertAxisReturns("{[Customers].[All Customers].[USA],"
                + "[Customers].[All Customers].[USA].[WA],"
                + "[Customers].[All Customers].[USA].[CA],"
                + "[Customers].[All Customers].[USA].[OR].[Lebanon].[Mary Frances Christian]}.Item(4)",
                "");
    }

    /**
     * Tests the function <code>&lt;Set&gt;.Item(&lt;String&gt; [,...])</code>.
     */
    public void testSetItemString() {
        assertAxisReturns("{[Gender].[M], [Gender].[F]}.Item(\"M\")",
                "[Gender].[All Gender].[M]");

        assertAxisReturns("{CrossJoin([Gender].Members, [Marital Status].Members)}.Item(\"M\", \"S\")",
                "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S]}");

        // MSAS fails with "duplicate dimensions across (independent) axes".
        // (That's a bug in MSAS.)
        assertAxisReturns("{CrossJoin([Gender].Members, [Marital Status].Members)}.Item(\"M\", \"M\")",
                "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M]}");

        // None found.
        assertAxisReturns("{[Gender].[M], [Gender].[F]}.Item(\"X\")",
                "");
        assertAxisReturns("{CrossJoin([Gender].Members, [Marital Status].Members)}.Item(\"M\", \"F\")",
                "");
        assertAxisReturns("CrossJoin([Gender].Members, [Marital Status].Members).Item(\"S\", \"M\")",
                "");

        assertAxisThrows("CrossJoin([Gender].Members, [Marital Status].Members).Item(\"M\")",
                "Argument count does not match set's cardinality 2");
    }

    public void testTuple() {
        assertExprReturns(
                "([Gender].[M], [Time].Children.Item(2), [Measures].[Unit Sales])",
                "33,249");
        // Calc calls MemberValue with 3 args -- more efficient than
        // constructing a tuple.
        assertExprCompilesTo(
                "([Gender].[M], [Time].Children.Item(2), [Measures].[Unit Sales])",
                "MemberValueCalc([Gender].[All Gender].[M], Item(Children(CurrentMember([Time])), 2.0), [Measures].[Unit Sales])");
    }

    public void testTupleItem() {
        assertAxisReturns(
                "([Time].[1997].[Q1].[1], [Customers].[All Customers].[USA].[OR], [Gender].[All Gender].[M]).item(2)",
                "[Gender].[All Gender].[M]");

        assertAxisReturns(
                "([Time].[1997].[Q1].[1], [Customers].[All Customers].[USA].[OR], [Gender].[All Gender].[M]).item(1)",
                "[Customers].[All Customers].[USA].[OR]");

        assertAxisReturns(
                "{[Time].[1997].[Q1].[1]}.item(0)",
                "[Time].[1997].[Q1].[1]");

        assertAxisReturns(
                "{[Time].[1997].[Q1].[1]}.Item(0).Item(0)",
                "[Time].[1997].[Q1].[1]");

        // given out of bounds index, item returns null
        assertAxisReturns(
                "([Time].[1997].[Q1].[1], [Customers].[All Customers].[USA].[OR], [Gender].[All Gender].[M]).item(-1)",
                "");

        // given out of bounds index, item returns null
        assertAxisReturns(
                "([Time].[1997].[Q1].[1], [Customers].[All Customers].[USA].[OR], [Gender].[All Gender].[M]).item(500)",
                "");

        // empty set
        assertExprReturns(
                "Filter([Gender].members, 1 = 0).Item(0)",
                "(null)");

        // empty set of unknown type
        assertExprReturns(
                "{}.Item(3)",
                "(null)");

        // past end of set
        assertExprReturns(
                "{[Gender].members}.Item(4)",
                "(null)");

        // negative index
        assertExprReturns(
                "{[Gender].members}.Item(-50)",
                "(null)");
    }

    public void testItemNull()
    {
        // In the following queries, MSAS returns 'Formula error - object type
        // is not valid - in an <object> base class. An error occurred during
        // attempt to get cell value'. This is because in MSAS, Item is a COM
        // function, and COM doesn't like null pointers.
        //
        // Mondrian represents null members as actual objects, so its behavior
        // is different.

        // MSAS returns error here.
        assertExprReturns(
                "Filter([Gender].members, 1 = 0).Item(0).Dimension.Name",
                "Gender");

        // MSAS returns error here.
        assertExprReturns(
                "Filter([Gender].members, 1 = 0).Item(0).Name",
                "#Null");

        // MSAS returns error here.
        assertExprReturns(
                "Filter([Gender].members, 1 = 0).Item(0).Parent",
                "(null)");
    }

    public void testTupleNull() {
        // if a tuple contains any null members, it evaluates to null
        assertQueryReturns(
                fold(new String[] {
                    "select {[Measures].[Unit Sales]} on columns,",
                    " { ([Gender].[M], [Store]),",
                    "   ([Gender].[F], [Store].parent),",
                    "   ([Gender].parent, [Store])} on rows",
                    "from [Sales]"}),
                fold(new String[] {
                    "Axis #0:",
                    "{}",
                    "Axis #1:",
                    "{[Measures].[Unit Sales]}",
                    "Axis #2:",
                    "{[Gender].[All Gender].[M], [Store].[All Stores]}",
                    "Row #0: 135,215" + nl}));

        // the set function eliminates tuples which are wholly or partially
        // null
        assertAxisReturns(
                fold(new String[] {
                    "([Gender].parent, [Marital Status]),", // part null
                    " ([Gender].[M], [Marital Status].parent),", // part null
                    " ([Gender].parent, [Marital Status].parent),", // wholly null
                    " ([Gender].[M], [Marital Status])", // not null
                }),
                "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status]}");

        // The tuple constructor returns a null tuple if one of its arguments
        // is null -- and the Item function returns null if the tuple is null.
        assertExprReturns(
                "([Gender].parent, [Marital Status]).Item(0).Name",
                "#Null");
        assertExprReturns(
                "([Gender].parent, [Marital Status]).Item(1).Name",
                "#Null");
    }

    private void checkDataResults(
            Double[][] expected, Result result, final double tolerance) {
        int[] coords = new int[2];

        for (int row = 0; row < expected.length; row++) {
            coords[1] = row;
            for (int col = 0; col < expected[0].length; col++) {
                coords[0] = col;

                Cell cell = result.getCell(coords);
                final Double expectedValue = expected[row][col];
                if (expectedValue == null) {
                    assertTrue("Expected null value", cell.isNull());
                } else if (cell.isNull()) {
                    fail(
                            "Cell at (" + row + ", " + col +
                            ") was null, but was expecting " +
                            expectedValue);
                } else {
                    assertEquals(
                            "Incorrect value returned at (" + row + ", " +
                            col + ")",
                            expectedValue.doubleValue(),
                            ((Number) cell.getValue()).doubleValue(),
                            tolerance);
                }
            }
        }
    }

    public void testLevelMemberExpressions() {
        // Should return Beverly Hills in California.
        assertAxisReturns("[Store].[Store City].[Beverly Hills]",
                "[Store].[All Stores].[USA].[CA].[Beverly Hills]");

        // There are two months named "1" in the time dimension: one for 1997 and one for 1998.
        // <Level>.<Member> should return the first one.
        assertAxisReturns("[Time].[Month].[1]", "[Time].[1997].[Q1].[1]");

        // Shouldn't be able to find a member named "Q1" on the month level.
        assertAxisThrows("[Time].[Month].[Q1]", "object '[Time].[Month].[Q1]' not found in cube");
    }

    public void testCaseTestMatch() {
        assertExprReturns("CASE WHEN 1=0 THEN \"first\" WHEN 1=1 THEN \"second\" WHEN 1=2 THEN \"third\" ELSE \"fourth\" END",
            "second");
    }

    public void testCaseTestMatchElse() {
        assertExprReturns("CASE WHEN 1=0 THEN \"first\" ELSE \"fourth\" END",
            "fourth");
    }

    public void testCaseTestMatchNoElse() {
        assertExprReturns("CASE WHEN 1=0 THEN \"first\" END",
            "(null)");
    }

    public void testCaseMatch() {
        assertExprReturns("CASE 2 WHEN 1 THEN \"first\" WHEN 2 THEN \"second\" WHEN 3 THEN \"third\" ELSE \"fourth\" END",
            "second");
    }

    public void testCaseMatchElse() {
        assertExprReturns("CASE 7 WHEN 1 THEN \"first\" ELSE \"fourth\" END",
            "fourth");
    }

    public void testCaseMatchNoElse() {
        assertExprReturns("CASE 8 WHEN 0 THEN \"first\" END",
            "(null)");
    }

    public void testCaseTypeMismatch() {
        // type mismatch between case and else
        assertAxisThrows("CASE 1 WHEN 1 THEN 2 ELSE \"foo\" END",
                "No function matches signature");
        // type mismatch between case and case
        assertAxisThrows("CASE 1 WHEN 1 THEN 2 WHEN 2 THEN \"foo\" ELSE 3 END",
                "No function matches signature");
        // type mismatch between value and case
        assertAxisThrows("CASE 1 WHEN \"foo\" THEN 2 ELSE 3 END",
                "No function matches signature");
        // non-boolean condition
        assertAxisThrows("CASE WHEN 1 = 2 THEN 3 WHEN 4 THEN 5 ELSE 6 END",
                "No function matches signature");
    }

    public void testPropertiesExpr() {
        assertExprReturns("[Store].[USA].[CA].[Beverly Hills].[Store 6].Properties(\"Store Type\")",
            "Gourmet Supermarket");
    }

    /**
     * Tests that non-existent property throws an error. *
     */
    public void testPropertiesNonExistent() {
        assertExprThrows("[Store].[USA].[CA].[Beverly Hills].[Store 6].Properties(\"Foo\")",
                "Property 'Foo' is not valid for");
    }

    public void testPropertiesFilter() {
        Result result = executeQuery(
                fold(new String[] {
                    "SELECT { [Store Sales] } ON COLUMNS,",
                    " TOPCOUNT( Filter( [Store].[Store Name].Members,",
                    "                   [Store].CurrentMember.Properties(\"Store Type\") = \"Supermarket\" ),",
                    "           10, [Store Sales]) ON ROWS",
                    "FROM [Sales]"}));
        Assert.assertEquals(8, result.getAxes()[1].positions.length);
    }

    public void testPropertyInCalculatedMember() {
        Result result = executeQuery(
                fold(new String[] {
                    "WITH MEMBER [Measures].[Store Sales per Sqft]",
                    "AS '[Measures].[Store Sales] / " +
                "  [Store].CurrentMember.Properties(\"Store Sqft\")'",
                    "SELECT ",
                    "  {[Measures].[Unit Sales], [Measures].[Store Sales per Sqft]} ON COLUMNS,",
                    "  {[Store].[Store Name].members} ON ROWS",
                    "FROM Sales"}));
        Member member;
        Cell cell;
        member = result.getAxes()[1].positions[18].members[0];
        Assert.assertEquals("[Store].[All Stores].[USA].[WA].[Bellingham].[Store 2]", member.getUniqueName());
        cell = result.getCell(new int[]{0, 18});
        Assert.assertEquals("2,237", cell.getFormattedValue());
        cell = result.getCell(new int[]{1, 18});
        Assert.assertEquals(".17", cell.getFormattedValue());
        member = result.getAxes()[1].positions[3].members[0];
        Assert.assertEquals("[Store].[All Stores].[Mexico].[DF].[San Andres].[Store 21]", member.getUniqueName());
        cell = result.getCell(new int[]{0, 3});
        Assert.assertEquals("(null)", cell.getFormattedValue());
        cell = result.getCell(new int[]{1, 3});
        Assert.assertEquals("(null)", cell.getFormattedValue());
    }

    public void testOpeningPeriod() {
        assertAxisReturns("OpeningPeriod([Time].[Month], [Time].[1997].[Q3])",
                "[Time].[1997].[Q3].[7]");

        assertAxisReturns("OpeningPeriod([Time].[Quarter], [Time].[1997])",
                "[Time].[1997].[Q1]");

        assertAxisReturns("OpeningPeriod([Time].[Year], [Time].[1997])",
                "[Time].[1997]");

        assertAxisReturns("OpeningPeriod([Time].[Month], [Time].[1997])",
                "[Time].[1997].[Q1].[1]");

        assertAxisReturns("OpeningPeriod([Product].[Product Name], [Product].[All Products].[Drink])",
                "[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Imported Beer]");

        getTestContext("[Sales Ragged]").assertAxisReturns(
                "OpeningPeriod([Store].[Store City], [Store].[All Stores].[Israel])",
                "[Store].[All Stores].[Israel].[Israel].[Haifa]");

        getTestContext("[Sales Ragged]").assertAxisReturns(
                "OpeningPeriod([Store].[Store State], [Store].[All Stores].[Israel])",
                "");

        // Default member is [Time].[1997].
        assertAxisReturns("OpeningPeriod([Time].[Month])",
                "[Time].[1997].[Q1].[1]");

        assertAxisReturns("OpeningPeriod()", "[Time].[1997].[Q1]");

        TestContext testContext = getTestContext("[Sales Ragged]");
        testContext.assertAxisThrows(
                "OpeningPeriod([Time].[Year], [Store].[All Stores].[Israel])",
                "The <level> and <member> arguments to OpeningPeriod must be "
                + "from the same hierarchy. The level was from '[Time]' but "
                + "the member was from '[Store]'.");

        assertAxisThrows("OpeningPeriod([Store].[Store City])",
                "The <level> and <member> arguments to OpeningPeriod must be "
                + "from the same hierarchy. The level was from '[Store]' but "
                + "the member was from '[Time]'.");
    }

    public void testLastPeriods() {
        assertAxisReturns("LastPeriods(0, [Time].[1998])",
                "");
        assertAxisReturns("LastPeriods(1, [Time].[1998])",
                "[Time].[1998]");
        assertAxisReturns("LastPeriods(-1, [Time].[1998])",
                "[Time].[1998]");
        assertAxisReturns("LastPeriods(2, [Time].[1998])",
                fold(new String[] {
                    "[Time].[1997]",
                    "[Time].[1998]"
                }));
        assertAxisReturns("LastPeriods(-2, [Time].[1997])",
                fold(new String[] {
                    "[Time].[1997]",
                    "[Time].[1998]"
                }));
        assertAxisReturns("LastPeriods(5000, [Time].[1998])",
                fold(new String[] {
                    "[Time].[1997]",
                    "[Time].[1998]"
                }));
        assertAxisReturns("LastPeriods(-5000, [Time].[1997])",
                fold(new String[] {
                    "[Time].[1997]",
                    "[Time].[1998]"
                }));
        assertAxisReturns("LastPeriods(2, [Time].[1998].[Q2])",
                fold(new String[] {
                    "[Time].[1998].[Q1]",
                    "[Time].[1998].[Q2]"
                }));
        assertAxisReturns("LastPeriods(4, [Time].[1998].[Q2])",
                fold(new String[] {
                    "[Time].[1997].[Q3]",
                    "[Time].[1997].[Q4]",
                    "[Time].[1998].[Q1]",
                    "[Time].[1998].[Q2]"
                }));
        assertAxisReturns("LastPeriods(-2, [Time].[1997].[Q2])",
                fold(new String[] {
                    "[Time].[1997].[Q2]",
                    "[Time].[1997].[Q3]"
                }));
        assertAxisReturns("LastPeriods(-4, [Time].[1997].[Q2])",
                fold(new String[] {
                    "[Time].[1997].[Q2]",
                    "[Time].[1997].[Q3]",
                    "[Time].[1997].[Q4]",
                    "[Time].[1998].[Q1]"
                }));
        assertAxisReturns("LastPeriods(5000, [Time].[1998].[Q2])",
                fold(new String[] {
                    "[Time].[1997].[Q1]",
                    "[Time].[1997].[Q2]",
                    "[Time].[1997].[Q3]",
                    "[Time].[1997].[Q4]",
                    "[Time].[1998].[Q1]",
                    "[Time].[1998].[Q2]"
                }));
        assertAxisReturns("LastPeriods(-5000, [Time].[1998].[Q2])",
                fold(new String[] {
                    "[Time].[1998].[Q2]",
                    "[Time].[1998].[Q3]",
                    "[Time].[1998].[Q4]"
                }));

        assertAxisReturns("LastPeriods(2, [Time].[1998].[Q2].[5])",
                fold(new String[] {
                    "[Time].[1998].[Q2].[4]",
                    "[Time].[1998].[Q2].[5]"
                }));
        assertAxisReturns("LastPeriods(12, [Time].[1998].[Q2].[5])",
                fold(new String[] {
                    "[Time].[1997].[Q2].[6]",
                    "[Time].[1997].[Q3].[7]",
                    "[Time].[1997].[Q3].[8]",
                    "[Time].[1997].[Q3].[9]",
                    "[Time].[1997].[Q4].[10]",
                    "[Time].[1997].[Q4].[11]",
                    "[Time].[1997].[Q4].[12]",
                    "[Time].[1998].[Q1].[1]",
                    "[Time].[1998].[Q1].[2]",
                    "[Time].[1998].[Q1].[3]",
                    "[Time].[1998].[Q2].[4]",
                    "[Time].[1998].[Q2].[5]"
                }));
        assertAxisReturns("LastPeriods(-2, [Time].[1998].[Q2].[4])",
                fold(new String[] {
                    "[Time].[1998].[Q2].[4]",
                    "[Time].[1998].[Q2].[5]"
                }));
        assertAxisReturns("LastPeriods(-12, [Time].[1997].[Q2].[6])",
                fold(new String[] {
                    "[Time].[1997].[Q2].[6]",
                    "[Time].[1997].[Q3].[7]",
                    "[Time].[1997].[Q3].[8]",
                    "[Time].[1997].[Q3].[9]",
                    "[Time].[1997].[Q4].[10]",
                    "[Time].[1997].[Q4].[11]",
                    "[Time].[1997].[Q4].[12]",
                    "[Time].[1998].[Q1].[1]",
                    "[Time].[1998].[Q1].[2]",
                    "[Time].[1998].[Q1].[3]",
                    "[Time].[1998].[Q2].[4]",
                    "[Time].[1998].[Q2].[5]"
                }));
        assertAxisReturns("LastPeriods(2, [Gender].[M])",
                fold(new String[] {
                    "[Gender].[All Gender].[F]",
                    "[Gender].[All Gender].[M]"
                }));
        assertAxisReturns("LastPeriods(-2, [Gender].[F])",
                fold(new String[] {
                    "[Gender].[All Gender].[F]",
                    "[Gender].[All Gender].[M]"
                }));
        assertAxisReturns("LastPeriods(2, [Gender])",
                fold(new String[] {
                    "[Gender].[All Gender]"
                }));
        assertAxisReturns("LastPeriods(2, [Gender].Parent)",
                "");
    }

    public void testParallelPeriod() {
        assertAxisReturns("parallelperiod([Time].[Quarter], 1, [Time].[1998].[Q1])",
                "[Time].[1997].[Q4]");

        assertAxisReturns("parallelperiod([Time].[Quarter], -1, [Time].[1997].[Q1])",
                "[Time].[1997].[Q2]");

        assertAxisReturns("parallelperiod([Time].[Year], 1, [Time].[1998].[Q1])",
                "[Time].[1997].[Q1]");

        assertAxisReturns("parallelperiod([Time].[Year], 1, [Time].[1998].[Q1].[1])",
                "[Time].[1997].[Q1].[1]");

        // No args, therefore finds parallel period to [Time].[1997], which
        // would be [Time].[1996], except that that doesn't exist, so null.
        assertAxisReturns("ParallelPeriod()", "");

        // Parallel period to [Time].[1997], which would be [Time].[1996],
        // except that that doesn't exist, so null.
        assertAxisReturns("ParallelPeriod([Time].[Year], 1, [Time].[1997])", "");

        // one parameter, level 2 above member
        assertQueryReturns(
                fold(new String[] {
                    "WITH MEMBER [Measures].[Foo] AS ",
                    " ' ParallelPeriod([Time].[Year]).UniqueName '",
                    "SELECT {[Measures].[Foo]} ON COLUMNS",
                    "FROM [Sales]",
                    "WHERE [Time].[1997].[Q3].[8]"}),
                fold(new String[] {
                    "Axis #0:",
                    "{[Time].[1997].[Q3].[8]}",
                    "Axis #1:",
                    "{[Measures].[Foo]}",
                    "Row #0: [Time].[#Null]" + nl}));

        // one parameter, level 1 above member
        assertQueryReturns(
                fold(new String[] {
                    "WITH MEMBER [Measures].[Foo] AS ",
                    " ' ParallelPeriod([Time].[Quarter]).UniqueName '",
                    "SELECT {[Measures].[Foo]} ON COLUMNS",
                    "FROM [Sales]",
                    "WHERE [Time].[1997].[Q3].[8]"}),
                fold(new String[] {
                    "Axis #0:",
                    "{[Time].[1997].[Q3].[8]}",
                    "Axis #1:",
                    "{[Measures].[Foo]}",
                    "Row #0: [Time].[1997].[Q2].[5]" + nl}));

        // one parameter, level same as member
        assertQueryReturns(
                fold(new String[] {
                    "WITH MEMBER [Measures].[Foo] AS ",
                    " ' ParallelPeriod([Time].[Month]).UniqueName '",
                    "SELECT {[Measures].[Foo]} ON COLUMNS",
                    "FROM [Sales]",
                    "WHERE [Time].[1997].[Q3].[8]"}),
                fold(new String[] {
                    "Axis #0:",
                    "{[Time].[1997].[Q3].[8]}",
                    "Axis #1:",
                    "{[Measures].[Foo]}",
                    "Row #0: [Time].[1997].[Q3].[7]" + nl}));

        //  one parameter, level below member
        assertQueryReturns(
                fold(new String[] {
                    "WITH MEMBER [Measures].[Foo] AS ",
                    " ' ParallelPeriod([Time].[Month]).UniqueName '",
                    "SELECT {[Measures].[Foo]} ON COLUMNS",
                    "FROM [Sales]",
                    "WHERE [Time].[1997].[Q3]"}),
                fold(new String[] {
                    "Axis #0:",
                    "{[Time].[1997].[Q3]}",
                    "Axis #1:",
                    "{[Measures].[Foo]}",
                    "Row #0: [Time].[#Null]" + nl}));
    }

    public void _testParallelPeriodThrowsException() {
        assertThrows("select {parallelperiod([Time].[Year], 1)} on columns "
                + "from [Sales] where ([Time].[1998].[Q1].[2])",
                "This should say something about Time appearing on two different axes (slicer an columns)");
    }

    public void testParallelPeriodDepends() {
        assertMemberExprDependsOn("ParallelPeriod([Time].[Quarter], 2.0)",
                "{[Time]}");
        assertMemberExprDependsOn("ParallelPeriod([Time].[Quarter], 2.0, [Time].[1997].[Q3])",
                "{}");
        assertMemberExprDependsOn("ParallelPeriod()",
                "{[Time]}");
        assertMemberExprDependsOn("ParallelPeriod([Product].[Food])",
                "{[Product]}");
        // [Gender].[M] is used here as a numeric expression!
        // The numeric expression DOES depend upon [Product].
        // The expression as a whole depends upon everything except [Gender].
        assertMemberExprDependsOn("ParallelPeriod([Product].[Product Family], [Gender].[M], [Product].[Food])",
                allDimsExcept(new String[] {"[Gender]"}));
        // As above
        assertMemberExprDependsOn("ParallelPeriod([Product].[Product Family], [Gender].[M])",
                allDimsExcept(new String[] {"[Gender]"}));
    }

    public void testParallelPeriodLevelLag() {
        assertQueryReturns(
                "with member [Measures].[Prev Unit Sales] as "
                + "        '([Measures].[Unit Sales], parallelperiod([Time].[Quarter], 2))' "
                + "select "
                + "    crossjoin({[Measures].[Unit Sales], [Measures].[Prev Unit Sales]}, {[Marital Status].[All Marital Status].children}) on columns, "
                + "    {[Time].[1997].[Q3]} on rows "
                + "from  "
                + "    [Sales] ",
                fold(new String[] {
                    "Axis #0:",
                    "{}",
                    "Axis #1:",
                    "{[Measures].[Unit Sales], [Marital Status].[All Marital Status].[M]}",
                    "{[Measures].[Unit Sales], [Marital Status].[All Marital Status].[S]}",
                    "{[Measures].[Prev Unit Sales], [Marital Status].[All Marital Status].[M]}",
                    "{[Measures].[Prev Unit Sales], [Marital Status].[All Marital Status].[S]}",
                    "Axis #2:",
                    "{[Time].[1997].[Q3]}",
                    "Row #0: 32,815",
                    "Row #0: 33,033",
                    "Row #0: 33,101",
                    "Row #0: 33,190" + nl}));

    }

    public void testParallelPeriodLevel() {
        assertQueryReturns("with "
                + "    member [Measures].[Prev Unit Sales] as "
                + "        '([Measures].[Unit Sales], parallelperiod([Time].[Quarter]))' "
                + "select "
                + "    crossjoin({[Measures].[Unit Sales], [Measures].[Prev Unit Sales]}, {[Marital Status].[All Marital Status].[M]}) on columns, "
                + "    {[Time].[1997].[Q3].[8]} on rows "
                + "from  "
                + "    [Sales]",
                fold(new String[] {
                    "Axis #0:",
                    "{}",
                    "Axis #1:",
                    "{[Measures].[Unit Sales], [Marital Status].[All Marital Status].[M]}",
                    "{[Measures].[Prev Unit Sales], [Marital Status].[All Marital Status].[M]}",
                    "Axis #2:",
                    "{[Time].[1997].[Q3].[8]}",
                    "Row #0: 10,957",
                    "Row #0: 10,280" + nl}));
    }

    public void testPlus() {
        assertExprDependsOn("1 + 2", "{}");
        assertExprDependsOn("([Measures].[Unit Sales], [Gender].[F]) + 2",
                allDimsExcept(new String[] {"[Measures]", "[Gender]"}));

        assertExprReturns("1+2", "3");
        assertExprReturns("5 + " + NullNumericExpr, "5"); // 5 + null --> 5
        assertExprReturns(NullNumericExpr + " + " + NullNumericExpr, "(null)");
        assertExprReturns(NullNumericExpr + " + 0", "0");
    }

    public void testMinus() {
        assertExprReturns("1-3", "-2");
        assertExprReturns("5 - " + NullNumericExpr, "5"); // 5 - null --> 5
        assertExprReturns(NullNumericExpr + " - - 2", "2");
        assertExprReturns(NullNumericExpr + " - " + NullNumericExpr, "(null)");
    }

    public void testMinus_bug1234759()
    {
        assertQueryReturns(
                fold(new String[] {
                    "WITH MEMBER [Customers].[USAMinusMexico]",
                    "AS '([Customers].[All Customers].[USA] - [Customers].[All Customers].[Mexico])'",
                    "SELECT {[Measures].[Unit Sales]} ON COLUMNS,",
                    "{[Customers].[All Customers].[USA], [Customers].[All Customers].[Mexico],",
                    "[Customers].[USAMinusMexico]} ON ROWS",
                    "FROM [Sales]"}),
                fold(new String[] {
                    "Axis #0:",
                    "{}",
                    "Axis #1:",
                    "{[Measures].[Unit Sales]}",
                    "Axis #2:",
                    "{[Customers].[All Customers].[USA]}",
                    "{[Customers].[All Customers].[Mexico]}",
                    "{[Customers].[USAMinusMexico]}",
                    "Row #0: 266,773",
                    "Row #1: (null)",
                    "Row #2: 266,773", // with bug 1234759, this was null
                    ""}));
    }

    public void testMinusAssociativity() {
        // right-associative would give 11-(7-5) = 9, which is wrong
        assertExprReturns("11-7-5", "-1");
    }

    public void testMultiply() {
        assertExprReturns("4*7", "28");
        assertExprReturns("5 * " + NullNumericExpr, "(null)"); // 5 * null --> null
        assertExprReturns(NullNumericExpr + " * - 2", "(null)");
        assertExprReturns(NullNumericExpr + " - " + NullNumericExpr, "(null)");
    }

    public void testMultiplyPrecedence() {
        assertExprReturns("3 + 4 * 5 + 6", "29");
        assertExprReturns("5 * 24 / 4 * 2", "60");
        assertExprReturns("48 / 4 / 2", "6");
    }

    /**
     * Bug 774807 caused expressions to be mistaken for the crossjoin
     * operator.
     */
    public void testMultiplyBug774807() {
        final String desiredResult = fold(new String[] {
            "Axis #0:",
            "{}",
            "Axis #1:",
            "{[Store].[All Stores]}",
            "Axis #2:",
            "{[Measures].[Store Sales]}",
            "{[Measures].[A]}",
            "Row #0: 565,238.13",
            "Row #1: 319,494,143,605.90" + nl});
        assertQueryReturns(
                fold(new String[] {
                    "WITH MEMBER [Measures].[A] AS",
                    " '([Measures].[Store Sales] * [Measures].[Store Sales])'",
                    "SELECT {[Store]} ON COLUMNS,",
                    " {[Measures].[Store Sales], [Measures].[A]} ON ROWS",
                    "FROM Sales"}),
                desiredResult);
        // as above, no parentheses
        assertQueryReturns(
                fold(new String[] {
                    "WITH MEMBER [Measures].[A] AS",
                    " '[Measures].[Store Sales] * [Measures].[Store Sales]'",
                    "SELECT {[Store]} ON COLUMNS,",
                    " {[Measures].[Store Sales], [Measures].[A]} ON ROWS",
                    "FROM Sales"}),
                desiredResult);
        // as above, plus 0
        assertQueryReturns(
                fold(new String[] {
                    "WITH MEMBER [Measures].[A] AS",
                    " '[Measures].[Store Sales] * [Measures].[Store Sales] + 0'",
                    "SELECT {[Store]} ON COLUMNS,",
                    " {[Measures].[Store Sales], [Measures].[A]} ON ROWS",
                    "FROM Sales"}),
                desiredResult);
    }

    public void testDivide() {
        assertExprReturns("10 / 5", "2");
        assertExprReturns("-2 / " + NullNumericExpr, "(null)");
        assertExprReturns(NullNumericExpr + " / - 2", "(null)");
        assertExprReturns(NullNumericExpr + " / " + NullNumericExpr, "(null)");
    }

    public void testDivideByZero() {
        assertExprReturns("-3 / (2 - 2)", "-Infinity");
    }

    public void testDividePrecedence() {
        assertExprReturns("24 / 4 / 2 * 10 - -1", "31");
    }

    public void testUnaryMinus() {
        assertExprReturns("-3", "-3");
    }

    public void testUnaryMinusMember() {
        assertExprReturns("- ([Measures].[Unit Sales],[Gender].[F])", "-131,558");
    }

    public void testUnaryMinusPrecedence() {
        assertExprReturns("1 - -10.5 * 2 -3", "19");
    }

    public void testStringConcat() {
        assertExprReturns(" \"foo\" || \"bar\"  ",
            "foobar");
    }

    public void testStringConcat2() {
        assertExprReturns(" \"foo\" || [Gender].[M].Name || \"\" ",
            "fooM");
    }

    public void testAnd() {
        assertBooleanExprReturns(" 1=1 AND 2=2 ", true);
    }

    public void testAnd2() {
        assertBooleanExprReturns(" 1=1 AND 2=0 ", false);
    }

    public void testOr() {
        assertBooleanExprReturns(" 1=0 OR 2=0 ", false);
    }

    public void testOr2() {
        assertBooleanExprReturns(" 1=0 OR 0=0 ", true);
    }

    public void testOrAssociativity1() {
        // Would give 'false' if OR were stronger than AND (wrong!)
        assertBooleanExprReturns(" 1=1 AND 1=0 OR 1=1 ", true);
    }

    public void testOrAssociativity2() {
        // Would give 'false' if OR were stronger than AND (wrong!)
        assertBooleanExprReturns(" 1=1 OR 1=0 AND 1=1 ", true);
    }

    public void testOrAssociativity3() {
        assertBooleanExprReturns(" (1=0 OR 1=1) AND 1=1 ", true);
    }

    public void testXor() {
        assertBooleanExprReturns(" 1=1 XOR 2=2 ", false);
    }

    public void testXorAssociativity() {
        // Would give 'false' if XOR were stronger than AND (wrong!)
        assertBooleanExprReturns(" 1 = 1 AND 1 = 1 XOR 1 = 0 ", true);
    }

    public void testNonEmptyCrossJoin() {
        // NonEmptyCrossJoin needs to evaluate measures to find out whether
        // cells are empty, so it implicitly depends upon all dimensions.
        assertSetExprDependsOn(
                "NonEmptyCrossJoin([Store].[USA].Children, [Gender].Children)",
                allDimsExcept(new String[] {"[Store]"}));

        assertAxisReturns("NonEmptyCrossJoin(" +
                "[Customers].[All Customers].[USA].[CA].Children, " +
                "[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].Children)",
                fold(new String[] {
                    "{[Customers].[All Customers].[USA].[CA].[Bellflower], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Light Beer]}",
                    "{[Customers].[All Customers].[USA].[CA].[Downey], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Imported Beer]}",
                    "{[Customers].[All Customers].[USA].[CA].[Glendale], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Imported Beer]}",
                    "{[Customers].[All Customers].[USA].[CA].[Glendale], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Light Beer]}",
                    "{[Customers].[All Customers].[USA].[CA].[Grossmont], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Light Beer]}",
                    "{[Customers].[All Customers].[USA].[CA].[Imperial Beach], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Light Beer]}",
                    "{[Customers].[All Customers].[USA].[CA].[La Jolla], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Imported Beer]}",
                    "{[Customers].[All Customers].[USA].[CA].[Lincoln Acres], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Imported Beer]}",
                    "{[Customers].[All Customers].[USA].[CA].[Lincoln Acres], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Light Beer]}",
                    "{[Customers].[All Customers].[USA].[CA].[Long Beach], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Light Beer]}",
                    "{[Customers].[All Customers].[USA].[CA].[Los Angeles], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Imported Beer]}",
                    "{[Customers].[All Customers].[USA].[CA].[Newport Beach], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Imported Beer]}",
                    "{[Customers].[All Customers].[USA].[CA].[Pomona], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Imported Beer]}",
                    "{[Customers].[All Customers].[USA].[CA].[Pomona], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Light Beer]}",
                    "{[Customers].[All Customers].[USA].[CA].[San Gabriel], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Light Beer]}",
                    "{[Customers].[All Customers].[USA].[CA].[West Covina], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Imported Beer]}",
                    "{[Customers].[All Customers].[USA].[CA].[West Covina], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Light Beer]}",
                    "{[Customers].[All Customers].[USA].[CA].[Woodland Hills], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Imported Beer]}"}));

        // empty set
        assertAxisReturns("NonEmptyCrossJoin({Gender.Parent}, {Store.Parent})", "");
        assertAxisReturns("NonEmptyCrossJoin({Store.Parent}, Gender.Children)", "");
        assertAxisReturns("NonEmptyCrossJoin(Store.Members, {})", "");

        // same dimension twice
        // todo: should throw
        if (false)
        assertAxisThrows("NonEmptyCrossJoin({Store.[USA]}, {Store.[USA].[CA]})", "xxx");
    }


    public void testNot() {
        assertBooleanExprReturns(" NOT 1=1 ", false);
    }

    public void testNotNot() {
        assertBooleanExprReturns(" NOT NOT 1=1 ", true);
    }

    public void testNotAssociativity() {
        assertBooleanExprReturns(" 1=1 AND NOT 1=1 OR NOT 1=1 AND 1=1 ", false);
    }

    public void testIsNull() {
        assertBooleanExprReturns(" Store.[All Stores] IS NULL ", false);
        assertBooleanExprReturns(" Store.[All Stores].parent IS NULL ", true);
    }

    public void testIsMember() {
        assertBooleanExprReturns(" Store.[USA].parent IS Store.[All Stores]", true);
        assertBooleanExprReturns(" [Store].[USA].[CA].parent IS [Store].[Mexico]", false);
    }

    // TODO: uncomment when tuple compare implemented
    public void _testIsTuple() {
        assertBooleanExprReturns(" (Store.[USA], Gender.[M]) IS (Store.[USA], Gender.[M])", true);
        assertBooleanExprReturns(" (Store.[USA], Gender.[M]) IS (Gender.[M], Store.[USA])", true);
        assertBooleanExprReturns(" (Store.[USA], Gender.[M]) IS (Store.[USA], Gender.[F])", false);
        assertBooleanExprReturns(" (Store.[USA], Gender.[M]) IS (Store.[USA])", false);
    }

    public void testIsLevel() {
        assertBooleanExprReturns(" Store.[USA].level IS Store.[Store Country] ", true);
        assertBooleanExprReturns(" Store.[USA].[CA].level IS Store.[Store Country] ", false);
    }

    public void testIsHierarchy() {
        assertBooleanExprReturns(" Store.[USA].hierarchy IS Store.[Mexico].hierarchy ", true);
        assertBooleanExprReturns(" Store.[USA].hierarchy IS Gender.[M].hierarchy ", false);
    }

    public void testIsDimension() {
        assertBooleanExprReturns(" Store.[USA].dimension IS Store ", true);
        assertBooleanExprReturns(" Gender.[M].dimension IS Store ", false);
    }

    public void testStringEquals() {
        assertBooleanExprReturns(" \"foo\" = \"bar\" ", false);
    }

    public void testStringEqualsAssociativity() {
        assertBooleanExprReturns(" \"foo\" = \"fo\" || \"o\" ", true);
    }

    public void testStringEqualsEmpty() {
        assertBooleanExprReturns(" \"\" = \"\" ", true);
    }

    public void testEq() {
        assertBooleanExprReturns(" 1.0 = 1 ", true);

        assertBooleanExprReturns("[Product].CurrentMember.Level.Ordinal = 2.0", false);
        checkNullOp("=");
    }

    public void testStringNe() {
        assertBooleanExprReturns(" \"foo\" <> \"bar\" ", true);
    }

    public void testNe() {
        assertBooleanExprReturns(" 2 <> 1.0 + 1.0 ", false);
        checkNullOp("<>");
    }

    public void testNeInfinity() {
        // Infinity does not equal itself
        assertBooleanExprReturns("(1 / 0) <> (1 / 0)", false);
    }

    public void testLt() {
        assertBooleanExprReturns(" 2 < 1.0 + 1.0 ", false);
        checkNullOp("<");
    }

    public void testLe() {
        assertBooleanExprReturns(" 2 <= 1.0 + 1.0 ", true);
        checkNullOp("<=");
    }

    public void testGt() {
        assertBooleanExprReturns(" 2 > 1.0 + 1.0 ", false);
        checkNullOp(">");
    }

    public void testGe() {
        assertBooleanExprReturns(" 2 > 1.0 + 1.0 ", false);
        checkNullOp(">=");
    }

    private void checkNullOp(final String op) {
        assertBooleanExprReturns(" 0 " + op + " " + NullNumericExpr, false);
        assertBooleanExprReturns(NullNumericExpr + " " + op + " 0", false);
        assertBooleanExprReturns(NullNumericExpr + " " + op + " " + NullNumericExpr, false);
    }

    public void testDistinctTwoMembers() {
        getTestContext("HR").assertAxisReturns(
                "Distinct({[Employees].[All Employees].[Sheri Nowmer].[Donna Arnold],"
                + "[Employees].[All Employees].[Sheri Nowmer].[Donna Arnold]})",
                "[Employees].[All Employees].[Sheri Nowmer].[Donna Arnold]");
    }

    public void testDistinctThreeMembers() {
        getTestContext("HR").assertAxisReturns(
                "Distinct({[Employees].[All Employees].[Sheri Nowmer].[Donna Arnold],"
                + "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz],"
                + "[Employees].[All Employees].[Sheri Nowmer].[Donna Arnold]})",
                fold(new String[] {
                    "[Employees].[All Employees].[Sheri Nowmer].[Donna Arnold]", "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz]"}));
    }

    public void testDistinctFourMembers() {
        getTestContext("HR").assertAxisReturns(
                "Distinct({[Employees].[All Employees].[Sheri Nowmer].[Donna Arnold],"
                + "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz],"
                + "[Employees].[All Employees].[Sheri Nowmer].[Donna Arnold],"
                + "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz]})",
                fold(new String[] {
                    "[Employees].[All Employees].[Sheri Nowmer].[Donna Arnold]", "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz]"}));
    }

    public void testDistinctTwoTuples() {
        getTestContext().assertAxisReturns(
                "Distinct({([Time].[1997],[Store].[All Stores].[Mexico]), "
                + "([Time].[1997], [Store].[All Stores].[Mexico])})",
                "{[Time].[1997], [Store].[All Stores].[Mexico]}");
    }

    public void testDistinctSomeTuples() {
        getTestContext().assertAxisReturns(
                "Distinct({([Time].[1997],[Store].[All Stores].[Mexico]), "
                + "crossjoin({[Time].[1997]},{[Store].[All Stores].children})})",
                fold(new String[] {
                    "{[Time].[1997], [Store].[All Stores].[Mexico]}", "{[Time].[1997], [Store].[All Stores].[Canada]}", "{[Time].[1997], [Store].[All Stores].[USA]}"}));
    }

    /**
     * Make sure that slicer is in force when expression is applied
     * on axis, E.g. select filter([Customers].members, [Unit Sales] > 100)
     * from sales where ([Time].[1998])
     */
    public void testFilterWithSlicer() {
        Result result = executeQuery(
                fold(new String[] {
                    "select {[Measures].[Unit Sales]} on columns,",
                    " filter([Customers].[USA].children,",
                    "        [Measures].[Unit Sales] > 20000) on rows",
                    "from Sales",
                    "where ([Time].[1997].[Q1])"}));
        Axis rows = result.getAxes()[1];
        // if slicer were ignored, there would be 3 rows
        Assert.assertEquals(1, rows.positions.length);
        Cell cell = result.getCell(new int[]{0, 0});
        Assert.assertEquals("30,114", cell.getFormattedValue());
    }

    public void testFilterCompound() {
        Result result = executeQuery(
                fold(new String[] {
                    "select {[Measures].[Unit Sales]} on columns,",
                    "  Filter(",
                    "    CrossJoin(",
                    "      [Gender].Children,",
                    "      [Customers].[USA].Children),",
                    "    [Measures].[Unit Sales] > 9500) on rows",
                    "from Sales",
                    "where ([Time].[1997].[Q1])"}));
        Position[] rows = result.getAxes()[1].positions;
        Assert.assertTrue(rows.length == 3);
        Assert.assertEquals("F", rows[0].members[0].getName());
        Assert.assertEquals("WA", rows[0].members[1].getName());
        Assert.assertEquals("M", rows[1].members[0].getName());
        Assert.assertEquals("OR", rows[1].members[1].getName());
        Assert.assertEquals("M", rows[2].members[0].getName());
        Assert.assertEquals("WA", rows[2].members[1].getName());
    }

    public void testGenerateDepends() {
        assertSetExprDependsOn(
                "Generate([Product].CurrentMember.Children, Crossjoin({[Product].CurrentMember}, Crossjoin([Store].[Store State].Members, [Store Type].Members)), ALL)",
                "{[Product]}");
        assertSetExprDependsOn(
                "Generate([Product].[All Products].Children, Crossjoin({[Product].CurrentMember}, Crossjoin([Store].[Store State].Members, [Store Type].Members)), ALL)",
                "{}");
        assertSetExprDependsOn(
                "Generate({[Store].[USA], [Store].[USA].[CA]}, {[Store].CurrentMember.Children})",
                "{}");
        assertSetExprDependsOn(
                "Generate({[Store].[USA], [Store].[USA].[CA]}, {[Gender].CurrentMember})",
                "{[Gender]}");
        assertSetExprDependsOn(
                "Generate({[Store].[USA], [Store].[USA].[CA]}, {[Gender].[M]})",
                "{}");
    }

    public void testGenerate() {
        assertAxisReturns("Generate({[Store].[USA], [Store].[USA].[CA]}, {[Store].CurrentMember.Children})",
                fold(new String[] {
                    "[Store].[All Stores].[USA].[CA]",
                    "[Store].[All Stores].[USA].[OR]",
                    "[Store].[All Stores].[USA].[WA]",
                    "[Store].[All Stores].[USA].[CA].[Alameda]",
                    "[Store].[All Stores].[USA].[CA].[Beverly Hills]",
                    "[Store].[All Stores].[USA].[CA].[Los Angeles]",
                    "[Store].[All Stores].[USA].[CA].[San Diego]",
                    "[Store].[All Stores].[USA].[CA].[San Francisco]"}));
    }

    public void testGenerateAll() {
        assertAxisReturns("Generate({[Store].[USA].[CA], [Store].[USA].[OR].[Portland]}," +
                " Ascendants([Store].CurrentMember)," +
                " ALL)",
                fold(new String[] {
                    "[Store].[All Stores].[USA].[CA]",
                    "[Store].[All Stores].[USA]",
                    "[Store].[All Stores]",
                    "[Store].[All Stores].[USA].[OR].[Portland]",
                    "[Store].[All Stores].[USA].[OR]",
                    "[Store].[All Stores].[USA]",
                    "[Store].[All Stores]"}));
    }

    public void testGenerateUnique() {
        assertAxisReturns("Generate({[Store].[USA].[CA], [Store].[USA].[OR].[Portland]}," +
                " Ascendants([Store].CurrentMember))",
                fold(new String[] {
                    "[Store].[All Stores].[USA].[CA]",
                    "[Store].[All Stores].[USA]",
                    "[Store].[All Stores]",
                    "[Store].[All Stores].[USA].[OR].[Portland]",
                    "[Store].[All Stores].[USA].[OR]"}));
    }

    public void testGenerateCrossJoin() {
        // Note that the different regions have different Top 2.
        assertAxisReturns(
                fold(new String[] {
                    "Generate({[Store].[USA].[CA], [Store].[USA].[CA].[San Francisco]},",
                    "  CrossJoin({[Store].CurrentMember},",
                    "    TopCount([Product].[Brand Name].members, ",
                    "    2,",
                    "    [Measures].[Unit Sales])))"}),
                fold(new String[] {
                    "{[Store].[All Stores].[USA].[CA], [Product].[All Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos]}",
                    "{[Store].[All Stores].[USA].[CA], [Product].[All Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale]}",
                    "{[Store].[All Stores].[USA].[CA].[San Francisco], [Product].[All Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony]}",
                    "{[Store].[All Stores].[USA].[CA].[San Francisco], [Product].[All Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top]}"}));
    }

    public void testHead() {
        assertAxisReturns("Head([Store].Children, 2)",
                fold(new String[] {
                    "[Store].[All Stores].[Canada]",
                    "[Store].[All Stores].[Mexico]"}));
    }

    public void testHeadNegative() {
        assertAxisReturns("Head([Store].Children, 2 - 3)",
                "");
    }

    public void testHeadDefault() {
        assertAxisReturns("Head([Store].Children)",
                "[Store].[All Stores].[Canada]");
    }

    public void testHeadOvershoot() {
        assertAxisReturns("Head([Store].Children, 2 + 2)",
                fold(new String[] {
                    "[Store].[All Stores].[Canada]",
                    "[Store].[All Stores].[Mexico]",
                    "[Store].[All Stores].[USA]"}));
    }

    public void testHeadEmpty() {
        assertAxisReturns("Head([Gender].[F].Children, 2)",
                "");

        assertAxisReturns("Head([Gender].[F].Children)",
                "");
    }

    public void testHierarchize() {
        assertAxisReturns(
                fold(new String[] {
                    "Hierarchize(",
                    "    {[Product].[All Products], " +
                "     [Product].[Food],",
                    "     [Product].[Drink],",
                    "     [Product].[Non-Consumable],",
                    "     [Product].[Food].[Eggs],",
                    "     [Product].[Drink].[Dairy]})"}),

                fold(new String[] {
                    "[Product].[All Products]",
                    "[Product].[All Products].[Drink]",
                    "[Product].[All Products].[Drink].[Dairy]",
                    "[Product].[All Products].[Food]",
                    "[Product].[All Products].[Food].[Eggs]",
                    "[Product].[All Products].[Non-Consumable]"}));
    }

    public void testHierarchizePost() {
        assertAxisReturns(
                fold(new String[] {
                    "Hierarchize(",
                    "    {[Product].[All Products], " +
                "     [Product].[Food],",
                    "     [Product].[Food].[Eggs],",
                    "     [Product].[Drink].[Dairy]},",
                    "  POST)"}),

                fold(new String[] {
                    "[Product].[All Products].[Drink].[Dairy]",
                    "[Product].[All Products].[Food].[Eggs]",
                    "[Product].[All Products].[Food]",
                    "[Product].[All Products]"}));
    }

    public void testHierarchizeCrossJoinPre() {
        assertAxisReturns(
                fold(new String[] {
                    "Hierarchize(",
                    "  CrossJoin(",
                    "    {[Product].[All Products], " +
                "     [Product].[Food],",
                    "     [Product].[Food].[Eggs],",
                    "     [Product].[Drink].[Dairy]},",
                    "    [Gender].MEMBERS),",
                    "  PRE)"}),

                fold(new String[] {
                    "{[Product].[All Products], [Gender].[All Gender]}",
                    "{[Product].[All Products], [Gender].[All Gender].[F]}",
                    "{[Product].[All Products], [Gender].[All Gender].[M]}",
                    "{[Product].[All Products].[Drink].[Dairy], [Gender].[All Gender]}",
                    "{[Product].[All Products].[Drink].[Dairy], [Gender].[All Gender].[F]}",
                    "{[Product].[All Products].[Drink].[Dairy], [Gender].[All Gender].[M]}",
                    "{[Product].[All Products].[Food], [Gender].[All Gender]}",
                    "{[Product].[All Products].[Food], [Gender].[All Gender].[F]}",
                    "{[Product].[All Products].[Food], [Gender].[All Gender].[M]}",
                    "{[Product].[All Products].[Food].[Eggs], [Gender].[All Gender]}",
                    "{[Product].[All Products].[Food].[Eggs], [Gender].[All Gender].[F]}",
                    "{[Product].[All Products].[Food].[Eggs], [Gender].[All Gender].[M]}"}));
    }

    public void testHierarchizeCrossJoinPost() {
        assertAxisReturns(
                fold(new String[] {
                    "Hierarchize(",
                    "  CrossJoin(",
                    "    {[Product].[All Products], " +
                "     [Product].[Food],",
                    "     [Product].[Food].[Eggs],",
                    "     [Product].[Drink].[Dairy]},",
                    "    [Gender].MEMBERS),",
                    "  POST)"}),

                fold(new String[] {
                    "{[Product].[All Products].[Drink].[Dairy], [Gender].[All Gender].[F]}",
                    "{[Product].[All Products].[Drink].[Dairy], [Gender].[All Gender].[M]}",
                    "{[Product].[All Products].[Drink].[Dairy], [Gender].[All Gender]}",
                    "{[Product].[All Products].[Food].[Eggs], [Gender].[All Gender].[F]}",
                    "{[Product].[All Products].[Food].[Eggs], [Gender].[All Gender].[M]}",
                    "{[Product].[All Products].[Food].[Eggs], [Gender].[All Gender]}",
                    "{[Product].[All Products].[Food], [Gender].[All Gender].[F]}",
                    "{[Product].[All Products].[Food], [Gender].[All Gender].[M]}",
                    "{[Product].[All Products].[Food], [Gender].[All Gender]}",
                    "{[Product].[All Products], [Gender].[All Gender].[F]}",
                    "{[Product].[All Products], [Gender].[All Gender].[M]}",
                    "{[Product].[All Products], [Gender].[All Gender]}"}));
    }

    /**
     * Tests that the Hierarchize function works correctly when applied to
     * a level whose ordering is determined by an 'ordinal' property.
     * TODO: fix this test (bug 1220787)
     */
    public void _testHierarchizeOrdinal() {
        final Connection connection =
                TestContext.instance().getFoodMartConnection(false);
        connection.getSchema().createCube(
                fold(new String[] {
                    "<Cube name=\"Sales_Hierarchize\">",
                    "  <Table name=\"sales_fact_1997\"/>",
                    "  <Dimension name=\"Time_Alphabetical\" type=\"TimeDimension\" foreignKey=\"time_id\">",
                    "    <Hierarchy hasAll=\"false\" primaryKey=\"time_id\">",
                    "      <Table name=\"time_by_day\"/>",
                    "      <Level name=\"Year\" column=\"the_year\" type=\"Numeric\" uniqueMembers=\"true\"",
                    "          levelType=\"TimeYears\"/>",
                    "      <Level name=\"Quarter\" column=\"quarter\" uniqueMembers=\"false\"",
                    "          levelType=\"TimeQuarters\"/>",
                    "      <Level name=\"Month\" column=\"month_of_year\" uniqueMembers=\"false\" type=\"Numeric\"",
                    "          ordinalColumn=\"the_month\"",
                    "          levelType=\"TimeMonths\"/>",
                    "    </Hierarchy>",
                    "  </Dimension>",
                    "",
                    "  <Dimension name=\"Month_Alphabetical\" type=\"TimeDimension\" foreignKey=\"time_id\">",
                    "    <Hierarchy hasAll=\"false\" primaryKey=\"time_id\">",
                    "      <Table name=\"time_by_day\"/>",
                    "      <Level name=\"Month\" column=\"month_of_year\" uniqueMembers=\"false\" type=\"Numeric\"",
                    "          ordinalColumn=\"the_month\"",
                    "          levelType=\"TimeMonths\"/>",
                    "    </Hierarchy>",
                    "  </Dimension>",
                    "",
                    "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\"",
                    "      formatString=\"Standard\"/>",
                    "</Cube>"}));

        // The [Time_Alphabetical] is ordered alphabetically by month
        getTestContext("[Sales_Hierarchize]").assertAxisReturns(
                "Hierarchize([Time_Alphabetical].members)",
                fold(new String[] {
                    "[Time_Alphabetical].[1997]",
                    "[Time_Alphabetical].[1997].[Q1]",
                    "[Time_Alphabetical].[1997].[Q1].[2]",
                    "[Time_Alphabetical].[1997].[Q1].[1]",
                    "[Time_Alphabetical].[1997].[Q1].[3]",
                    "[Time_Alphabetical].[1997].[Q2]",
                    "[Time_Alphabetical].[1997].[Q2].[4]",
                    "[Time_Alphabetical].[1997].[Q2].[6]",
                    "[Time_Alphabetical].[1997].[Q2].[5]",
                    "[Time_Alphabetical].[1997].[Q3]",
                    "[Time_Alphabetical].[1997].[Q3].[8]",
                    "[Time_Alphabetical].[1997].[Q3].[7]",
                    "[Time_Alphabetical].[1997].[Q3].[9]",
                    "[Time_Alphabetical].[1997].[Q4]",
                    "[Time_Alphabetical].[1997].[Q4].[12]",
                    "[Time_Alphabetical].[1997].[Q4].[11]",
                    "[Time_Alphabetical].[1997].[Q4].[10]",
                    "[Time_Alphabetical].[1998]",
                    "[Time_Alphabetical].[1998].[Q1]",
                    "[Time_Alphabetical].[1998].[Q1].[2]",
                    "[Time_Alphabetical].[1998].[Q1].[1]",
                    "[Time_Alphabetical].[1998].[Q1].[3]",
                    "[Time_Alphabetical].[1998].[Q2]",
                    "[Time_Alphabetical].[1998].[Q2].[4]",
                    "[Time_Alphabetical].[1998].[Q2].[6]",
                    "[Time_Alphabetical].[1998].[Q2].[5]",
                    "[Time_Alphabetical].[1998].[Q3]",
                    "[Time_Alphabetical].[1998].[Q3].[8]",
                    "[Time_Alphabetical].[1998].[Q3].[7]",
                    "[Time_Alphabetical].[1998].[Q3].[9]",
                    "[Time_Alphabetical].[1998].[Q4]",
                    "[Time_Alphabetical].[1998].[Q4].[12]",
                    "[Time_Alphabetical].[1998].[Q4].[11]",
                    "[Time_Alphabetical].[1998].[Q4].[10]"}));

        // The [Month_Alphabetical] is a single-level hierarchy ordered
        // alphabetically by month.
        getTestContext("[Sales_Hierarchize]").assertAxisReturns(
                "Hierarchize([Month_Alphabetical].members)",
                fold(new String[] {
                    "[Month_Alphabetical].[4]",
                    "[Month_Alphabetical].[8]",
                    "[Month_Alphabetical].[12]",
                    "[Month_Alphabetical].[2]",
                    "[Month_Alphabetical].[1]",
                    "[Month_Alphabetical].[7]",
                    "[Month_Alphabetical].[6]",
                    "[Month_Alphabetical].[3]",
                    "[Month_Alphabetical].[5]",
                    "[Month_Alphabetical].[11]",
                    "[Month_Alphabetical].[10]",
                    "[Month_Alphabetical].[9]"}));
    }

    public void testIntersect() {
        // Note: duplicates retained from left, not from right; and order is preserved.
        assertAxisReturns("Intersect({[Time].[1997].[Q2], [Time].[1997], [Time].[1997].[Q1], [Time].[1997].[Q2]}, " +
                "{[Time].[1998], [Time].[1997], [Time].[1997].[Q2], [Time].[1997]}, " +
                "ALL)",
                fold(new String[] {
                    "[Time].[1997].[Q2]",
                    "[Time].[1997]",
                    "[Time].[1997].[Q2]"}));
    }

    public void testIntersectRightEmpty() {
        assertAxisReturns("Intersect({[Time].[1997]}, {})",
                "");
    }

    public void testIntersectLeftEmpty() {
        assertAxisReturns("Intersect({}, {[Store].[USA].[CA]})",
                "");
    }

    public void testOrderDepends() {
        // Order(<Set>, <Value Expression>) depends upon everything
        // <Value Expression> depends upon, except the dimensions of <Set>.

        // Depends upon everything EXCEPT [Product], [Measures],
        // [Marital Status], [Gender].
        assertSetExprDependsOn(
                "Order(" +
                " Crossjoin([Gender].MEMBERS, [Product].MEMBERS)," +
                " ([Measures].[Unit Sales], [Marital Status].[S])," +
                " ASC)",
                allDimsExcept(new String[] {"[Product]", "[Measures]", "[Marital Status]", "[Gender]"}));

        // Depends upon everything EXCEPT [Product], [Measures],
        // [Marital Status]. Does depend upon [Gender].
        assertSetExprDependsOn(
                "Order(" +
                " Crossjoin({[Gender].CurrentMember}, [Product].MEMBERS)," +
                " ([Measures].[Unit Sales], [Marital Status].[S])," +
                " ASC)",
                allDimsExcept(new String[] {"[Product]", "[Measures]", "[Marital Status]"}));

        // Depends upon everything except [Measures].
        assertSetExprDependsOn(
                "Order(" +
                "  Crossjoin(" +
                "    [Gender].CurrentMember.Children, " +
                "    [Marital Status].CurrentMember.Children), " +
                "  [Measures].[Unit Sales], " +
                "  BDESC)",
                allDimsExcept(new String[] {"[Measures]"}));

        assertSetExprDependsOn(
                fold(new String[] {
                    "  Order(",
                    "    CrossJoin( ",
                    "      {[Product].[All Products].[Food].[Eggs],",
                    "       [Product].[All Products].[Food].[Seafood],",
                    "       [Product].[All Products].[Drink].[Alcoholic Beverages]},",
                    "      {[Store].[USA].[WA].[Seattle],",
                    "       [Store].[USA].[CA],",
                    "       [Store].[USA].[OR]}),",
                    "    ([Time].[1997].[Q1], [Measures].[Unit Sales]),",
                    "    ASC)"}),
                allDimsExcept(new String[] {"[Measures]", "[Store]", "[Product]", "[Time]"}));
    }

    public void testOrderCalc() {
        // [Measures].[Unit Sales] is a constant member, so it is evaluated in
        // a ContextCalc.
        assertAxisCompilesTo(
                "order([Product].children, [Measures].[Unit Sales])",
                "{}(Sublist(ContextCalc([Measures].[Unit Sales], Order(Children(CurrentMember([Product])), ValueCalc, ASC))))");

        // [Time].[1997] is constant, and is evaluated in a ContextCalc.
        // [Product].Parent is variable, and is evaluated inside the loop.
        assertAxisCompilesTo(
                "order([Product].children, ([Time].[1997], [Product].CurrentMember.Parent))",
                "{}(Sublist(ContextCalc([Time].[1997], Order(Children(CurrentMember([Product])), MemberValueCalc(Parent(CurrentMember([Product]))), ASC))))");

        // No ContextCalc this time. All members are non-variable.
        assertAxisCompilesTo(
                "order([Product].children, [Product].CurrentMember.Parent)",
                "{}(Sublist(Order(Children(CurrentMember([Product])), MemberValueCalc(Parent(CurrentMember([Product]))), ASC)))");

        // List expression is dependent on one of the constant calcs. It cannot
        // be pulled up, so [Gender].[M] is not in the ContextCalc.
        assertAxisCompilesTo(
                "order(filter([Product].children, [Measures].[Unit Sales] > 1000), ([Gender].[M], [Measures].[Store Sales]))",
                "{}(Sublist(ContextCalc([Measures].[Store Sales], Order(Filter(Children(CurrentMember([Product])), >(MemberValueCalc([Measures].[Unit Sales]), 1000.0)), MemberValueCalc([Gender].[All Gender].[M]), ASC))))");
    }

    public void testOrder() {
        assertQueryReturns(
                fold(new String[] {
                    "select {[Measures].[Unit Sales]} on columns,",
                    " order({",
                    "  [Product].[All Products].[Drink],",
                    "  [Product].[All Products].[Drink].[Beverages],",
                    "  [Product].[All Products].[Drink].[Dairy],",
                    "  [Product].[All Products].[Food],",
                    "  [Product].[All Products].[Food].[Baked Goods],",
                    "  [Product].[All Products].[Food].[Eggs],",
                    "  [Product].[All Products]},",
                    " [Measures].[Unit Sales]) on rows",
                    "from Sales"}),

                fold(new String[] {
                    "Axis #0:",
                    "{}",
                    "Axis #1:",
                    "{[Measures].[Unit Sales]}",
                    "Axis #2:",
                    "{[Product].[All Products]}",
                    "{[Product].[All Products].[Drink]}",
                    "{[Product].[All Products].[Drink].[Dairy]}",
                    "{[Product].[All Products].[Drink].[Beverages]}",
                    "{[Product].[All Products].[Food]}",
                    "{[Product].[All Products].[Food].[Eggs]}",
                    "{[Product].[All Products].[Food].[Baked Goods]}",
                    "Row #0: 266,773",
                    "Row #1: 24,597",
                    "Row #2: 4,186",
                    "Row #3: 13,573",
                    "Row #4: 191,940",
                    "Row #5: 4,132",
                    "Row #6: 7,870" + nl}));
    }

    public void testOrderParentsMissing() {
        // Paradoxically, [Alcoholic Beverages] comes before
        // [Eggs] even though it has a larger value, because
        // its parent [Drink] has a smaller value than [Food].
        assertQueryReturns(
                fold(new String[] {
                    "select {[Measures].[Unit Sales]} on columns," +
                " order({",
                    "  [Product].[All Products].[Drink].[Alcoholic Beverages],",
                    "  [Product].[All Products].[Food].[Eggs]},",
                    " [Measures].[Unit Sales], ASC) on rows",
                    "from Sales"}),

                fold(new String[] {
                    "Axis #0:",
                    "{}",
                    "Axis #1:",
                    "{[Measures].[Unit Sales]}",
                    "Axis #2:",
                    "{[Product].[All Products].[Drink].[Alcoholic Beverages]}",
                    "{[Product].[All Products].[Food].[Eggs]}",
                    "Row #0: 6,838",
                    "Row #1: 4,132" + nl}));
    }

    public void testOrderCrossJoinBreak() {
        assertQueryReturns(
                fold(new String[] {
                    "select {[Measures].[Unit Sales]} on columns,",
                    "  Order(",
                    "    CrossJoin(",
                    "      [Gender].children,",
                    "      [Marital Status].children),",
                    "    [Measures].[Unit Sales],",
                    "    BDESC) on rows",
                    "from Sales",
                    "where [Time].[1997].[Q1]"}),

                fold(new String[] {
                    "Axis #0:",
                    "{[Time].[1997].[Q1]}",
                    "Axis #1:",
                    "{[Measures].[Unit Sales]}",
                    "Axis #2:",
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S]}",
                    "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[M]}",
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M]}",
                    "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S]}",
                    "Row #0: 17,070",
                    "Row #1: 16,790",
                    "Row #2: 16,311",
                    "Row #3: 16,120" + nl}));
    }

    public void testOrderCrossJoin() {
        // Note:
        // 1. [Alcoholic Beverages] collates before [Eggs] and
        //    [Seafood] because its parent, [Drink], is less
        //    than [Food]
        // 2. [Seattle] generally sorts after [CA] and [OR]
        //    because invisible parent [WA] is greater.
        assertQueryReturns(
                fold(new String[] {
                    "select CrossJoin(",
                    "    {[Time].[1997],",
                    "     [Time].[1997].[Q1]},",
                    "    {[Measures].[Unit Sales]}) on columns,",
                    "  Order(",
                    "    CrossJoin( ",
                    "      {[Product].[All Products].[Food].[Eggs],",
                    "       [Product].[All Products].[Food].[Seafood],",
                    "       [Product].[All Products].[Drink].[Alcoholic Beverages]},",
                    "      {[Store].[USA].[WA].[Seattle],",
                    "       [Store].[USA].[CA],",
                    "       [Store].[USA].[OR]}),",
                    "    ([Time].[1997].[Q1], [Measures].[Unit Sales]),",
                    "    ASC) on rows",
                    "from Sales"}),

                fold(new String[] {
                    "Axis #0:",
                    "{}",
                    "Axis #1:",
                    "{[Time].[1997], [Measures].[Unit Sales]}",
                    "{[Time].[1997].[Q1], [Measures].[Unit Sales]}",
                    "Axis #2:",
                    "{[Product].[All Products].[Drink].[Alcoholic Beverages], [Store].[All Stores].[USA].[OR]}",
                    "{[Product].[All Products].[Drink].[Alcoholic Beverages], [Store].[All Stores].[USA].[CA]}",
                    "{[Product].[All Products].[Drink].[Alcoholic Beverages], [Store].[All Stores].[USA].[WA].[Seattle]}",
                    "{[Product].[All Products].[Food].[Seafood], [Store].[All Stores].[USA].[CA]}",
                    "{[Product].[All Products].[Food].[Seafood], [Store].[All Stores].[USA].[OR]}",
                    "{[Product].[All Products].[Food].[Seafood], [Store].[All Stores].[USA].[WA].[Seattle]}",
                    "{[Product].[All Products].[Food].[Eggs], [Store].[All Stores].[USA].[CA]}",
                    "{[Product].[All Products].[Food].[Eggs], [Store].[All Stores].[USA].[OR]}",
                    "{[Product].[All Products].[Food].[Eggs], [Store].[All Stores].[USA].[WA].[Seattle]}",
                    "Row #0: 1,680",
                    "Row #0: 393",
                    "Row #1: 1,936",
                    "Row #1: 431",
                    "Row #2: 635",
                    "Row #2: 142",
                    "Row #3: 441",
                    "Row #3: 91",
                    "Row #4: 451",
                    "Row #4: 107",
                    "Row #5: 217",
                    "Row #5: 44",
                    "Row #6: 1,116",
                    "Row #6: 240",
                    "Row #7: 1,119",
                    "Row #7: 251",
                    "Row #8: 373",
                    "Row #8: 57" + nl}));
    }

    public void testOrderHierarchicalDesc() {
        assertAxisReturns(
                fold(new String[] {
                    "Order(",
                    "    {[Product].[All Products], " +
                "     [Product].[Food],",
                    "     [Product].[Drink],",
                    "     [Product].[Non-Consumable],",
                    "     [Product].[Food].[Eggs],",
                    "     [Product].[Drink].[Dairy]},",
                    "  [Measures].[Unit Sales],",
                    "  DESC)"}),

                fold(new String[] {
                    "[Product].[All Products]",
                    "[Product].[All Products].[Food]",
                    "[Product].[All Products].[Food].[Eggs]",
                    "[Product].[All Products].[Non-Consumable]",
                    "[Product].[All Products].[Drink]",
                    "[Product].[All Products].[Drink].[Dairy]"}));
    }

    public void testOrderCrossJoinDesc() {
        assertAxisReturns(
                fold(new String[] {
                    "Order(",
                    "  CrossJoin(",
                    "    {[Gender].[M], [Gender].[F]},",
                    "    {[Product].[All Products], " +
                "     [Product].[Food],",
                    "     [Product].[Drink],",
                    "     [Product].[Non-Consumable],",
                    "     [Product].[Food].[Eggs],",
                    "     [Product].[Drink].[Dairy]}),",
                    "  [Measures].[Unit Sales],",
                    "  DESC)"}),

                fold(new String[] {
                    "{[Gender].[All Gender].[M], [Product].[All Products]}",
                    "{[Gender].[All Gender].[M], [Product].[All Products].[Food]}",
                    "{[Gender].[All Gender].[M], [Product].[All Products].[Food].[Eggs]}",
                    "{[Gender].[All Gender].[M], [Product].[All Products].[Non-Consumable]}",
                    "{[Gender].[All Gender].[M], [Product].[All Products].[Drink]}",
                    "{[Gender].[All Gender].[M], [Product].[All Products].[Drink].[Dairy]}",
                    "{[Gender].[All Gender].[F], [Product].[All Products]}",
                    "{[Gender].[All Gender].[F], [Product].[All Products].[Food]}",
                    "{[Gender].[All Gender].[F], [Product].[All Products].[Food].[Eggs]}",
                    "{[Gender].[All Gender].[F], [Product].[All Products].[Non-Consumable]}",
                    "{[Gender].[All Gender].[F], [Product].[All Products].[Drink]}",
                    "{[Gender].[All Gender].[F], [Product].[All Products].[Drink].[Dairy]}"}));
    }

    public void testOrderBug656802() {
        // Note:
        // 1. [Alcoholic Beverages] collates before [Eggs] and
        //    [Seafood] because its parent, [Drink], is less
        //    than [Food]
        // 2. [Seattle] generally sorts after [CA] and [OR]
        //    because invisible parent [WA] is greater.
        assertQueryReturns(
                fold(new String[] {
                    "select {[Measures].[Unit Sales], [Measures].[Store Cost], [Measures].[Store Sales]} ON columns, ",
                    "Order(",
                    "  ToggleDrillState(",
                    "    {([Promotion Media].[All Media], [Product].[All Products])},",
                    "    {[Product].[All Products]} ), ",
                    "  [Measures].[Unit Sales], DESC) ON rows ",
                    "from [Sales] where ([Time].[1997])"}),

                fold(new String[] {
                    "Axis #0:",
                    "{[Time].[1997]}",
                    "Axis #1:",
                    "{[Measures].[Unit Sales]}",
                    "{[Measures].[Store Cost]}",
                    "{[Measures].[Store Sales]}",
                    "Axis #2:",
                    "{[Promotion Media].[All Media], [Product].[All Products]}",
                    "{[Promotion Media].[All Media], [Product].[All Products].[Food]}",
                    "{[Promotion Media].[All Media], [Product].[All Products].[Non-Consumable]}",
                    "{[Promotion Media].[All Media], [Product].[All Products].[Drink]}",
                    "Row #0: 266,773",
                    "Row #0: 225,627.23",
                    "Row #0: 565,238.13",
                    "Row #1: 191,940",
                    "Row #1: 163,270.72",
                    "Row #1: 409,035.59",
                    "Row #2: 50,236",
                    "Row #2: 42,879.28",
                    "Row #2: 107,366.33",
                    "Row #3: 24,597",
                    "Row #3: 19,477.23",
                    "Row #3: 48,836.21" + nl}));
    }

    public void testOrderBug712702_Simplified() {
        assertQueryReturns(
                fold(new String[] {
                    "SELECT Order({[Time].[Year].members}, [Measures].[Unit Sales]) on columns",
                    "from [Sales]"}),
                fold(new String[] {
                    "Axis #0:",
                    "{}",
                    "Axis #1:",
                    "{[Time].[1998]}",
                    "{[Time].[1997]}",
                    "Row #0: (null)",
                    "Row #0: 266,773" + nl}));
    }

    // TODO: Fix NPE in SqlConstraintUtils.addContextConstraint, and re-enable
    public void _testOrderBug712702_Original() {
        assertQueryReturns(
                fold(new String[] {
                    "with member [Measures].[Average Unit Sales] as 'Avg(Descendants([Time].CurrentMember, [Time].[Month]), ",
                    "[Measures].[Unit Sales])' ",
                    "member [Measures].[Max Unit Sales] as 'Max(Descendants([Time].CurrentMember, [Time].[Month]), [Measures].[Unit Sales])' ",
                    "select {[Measures].[Average Unit Sales], [Measures].[Max Unit Sales], [Measures].[Unit Sales]} ON columns, ",
                    "  NON EMPTY Order(",
                    "    Crossjoin( ",
                    "      {[Store].[All Stores].[USA].[OR].[Portland],",
                    "       [Store].[All Stores].[USA].[OR].[Salem],",
                    "       [Store].[All Stores].[USA].[OR].[Salem].[Store 13],",
                    "       [Store].[All Stores].[USA].[CA].[San Francisco],",
                    "       [Store].[All Stores].[USA].[CA].[San Diego],",
                    "       [Store].[All Stores].[USA].[CA].[Beverly Hills],",
                    "       [Store].[All Stores].[USA].[CA].[Los Angeles],",
                    "       [Store].[All Stores].[USA].[WA].[Walla Walla],",
                    "       [Store].[All Stores].[USA].[WA].[Bellingham],",
                    "       [Store].[All Stores].[USA].[WA].[Yakima],",
                    "       [Store].[All Stores].[USA].[WA].[Spokane],",
                    "       [Store].[All Stores].[USA].[WA].[Seattle], ",
                    "       [Store].[All Stores].[USA].[WA].[Bremerton],",
                    "       [Store].[All Stores].[USA].[WA].[Tacoma]},",
                    "     [Time].[Year].Members), ",
                    "  [Measures].[Average Unit Sales], ASC) ON rows",
                    "from [Sales] "}),
                fold(new String[] {
                    "Axis #0:",
                    "{}",
                    "Axis #1:",
                    "{[Measures].[Average Unit Sales]}",
                    "{[Measures].[Max Unit Sales]}",
                    "{[Measures].[Unit Sales]}",
                    "Axis #2:",
                    "{[Store].[All Stores].[USA].[OR].[Portland], [Time].[1997]}",
                    "{[Store].[All Stores].[USA].[OR].[Salem], [Time].[1997]}",
                    "{[Store].[All Stores].[USA].[OR].[Salem].[Store 13], [Time].[1997]}",
                    "{[Store].[All Stores].[USA].[CA].[San Francisco], [Time].[1997]}",
                    "{[Store].[All Stores].[USA].[CA].[Beverly Hills], [Time].[1997]}",
                    "{[Store].[All Stores].[USA].[CA].[San Diego], [Time].[1997]}",
                    "{[Store].[All Stores].[USA].[CA].[Los Angeles], [Time].[1997]}",
                    "{[Store].[All Stores].[USA].[WA].[Walla Walla], [Time].[1997]}",
                    "{[Store].[All Stores].[USA].[WA].[Bellingham], [Time].[1997]}",
                    "{[Store].[All Stores].[USA].[WA].[Yakima], [Time].[1997]}",
                    "{[Store].[All Stores].[USA].[WA].[Spokane], [Time].[1997]}",
                    "{[Store].[All Stores].[USA].[WA].[Bremerton], [Time].[1997]}",
                    "{[Store].[All Stores].[USA].[WA].[Seattle], [Time].[1997]}",
                    "{[Store].[All Stores].[USA].[WA].[Tacoma], [Time].[1997]}",
                    "Row #0: 2,173",
                    "Row #0: 2,933",
                    "Row #0: 26,079",
                    "Row #1: 3,465",
                    "Row #1: 5,891",
                    "Row #1: 41,580",
                    "Row #2: 3,465",
                    "Row #2: 5,891",
                    "Row #2: 41,580",
                    "Row #3: 176",
                    "Row #3: 222",
                    "Row #3: 2,117",
                    "Row #4: 1,778",
                    "Row #4: 2,545",
                    "Row #4: 21,333",
                    "Row #5: 2,136",
                    "Row #5: 2,686",
                    "Row #5: 25,635",
                    "Row #6: 2,139",
                    "Row #6: 2,669",
                    "Row #6: 25,663",
                    "Row #7: 184",
                    "Row #7: 301",
                    "Row #7: 2,203",
                    "Row #8: 186",
                    "Row #8: 275",
                    "Row #8: 2,237",
                    "Row #9: 958",
                    "Row #9: 1,163",
                    "Row #9: 11,491",
                    "Row #10: 1,966",
                    "Row #10: 2,634",
                    "Row #10: 23,591",
                    "Row #11: 2,048",
                    "Row #11: 2,623",
                    "Row #11: 24,576",
                    "Row #12: 2,084",
                    "Row #12: 2,304",
                    "Row #12: 25,011",
                    "Row #13: 2,938",
                    "Row #13: 3,818",
                    "Row #13: 35,257" + nl}));
    }

    public void testSiblingsA() {
        assertAxisReturns("{[Time].[1997].Siblings}",
                fold(new String[] {
                    "[Time].[1997]",
                    "[Time].[1998]"}));
    }

    public void testSiblingsB() {
        assertAxisReturns("{[Store].Siblings}",
                "[Store].[All Stores]");
    }

    public void testSiblingsC() {
        assertAxisReturns("{[Store].[USA].[CA].Siblings}",
                fold(new String[] {
                    "[Store].[All Stores].[USA].[CA]",
                    "[Store].[All Stores].[USA].[OR]",
                    "[Store].[All Stores].[USA].[WA]"}));
    }

    public void testSiblingsD() {
        // The null member has no siblings -- not even itself
        assertAxisReturns("{[Gender].Parent.Siblings}", "");

        assertExprReturns("count ( [Gender].parent.siblings, includeempty )", "0");
    }

    public void testSubset() {
        assertAxisReturns("Subset([Promotion Media].Children, 7, 2)",
                fold(new String[] {
                    "[Promotion Media].[All Media].[Product Attachment]",
                    "[Promotion Media].[All Media].[Radio]"}));
    }

    public void testSubsetNegativeCount() {
        assertAxisReturns("Subset([Promotion Media].Children, 3, -1)",
                "");
    }

    public void testSubsetNegativeStart() {
        assertAxisReturns("Subset([Promotion Media].Children, -2, 4)",
                "");
    }

    public void testSubsetDefault() {
        assertAxisReturns("Subset([Promotion Media].Children, 11)",
                fold(new String[] {
                    "[Promotion Media].[All Media].[Sunday Paper, Radio]",
                    "[Promotion Media].[All Media].[Sunday Paper, Radio, TV]",
                    "[Promotion Media].[All Media].[TV]"}));
    }

    public void testSubsetOvershoot() {
        assertAxisReturns("Subset([Promotion Media].Children, 15)",
                "");
    }

    public void testSubsetEmpty() {
        assertAxisReturns("Subset([Gender].[F].Children, 1)",
                "");

        assertAxisReturns("Subset([Gender].[F].Children, 1, 3)",
                "");
    }

    public void testTail() {
        assertAxisReturns("Tail([Store].Children, 2)",
                fold(new String[] {
                    "[Store].[All Stores].[Mexico]",
                    "[Store].[All Stores].[USA]"}));
    }

    public void testTailNegative() {
        assertAxisReturns("Tail([Store].Children, 2 - 3)",
                "");
    }

    public void testTailDefault() {
        assertAxisReturns("Tail([Store].Children)",
                "[Store].[All Stores].[USA]");
    }

    public void testTailOvershoot() {
        assertAxisReturns("Tail([Store].Children, 2 + 2)",
                fold(new String[] {
                    "[Store].[All Stores].[Canada]",
                    "[Store].[All Stores].[Mexico]",
                    "[Store].[All Stores].[USA]"}));
    }

    public void testTailEmpty() {
        assertAxisReturns("Tail([Gender].[F].Children, 2)",
                "");

        assertAxisReturns("Tail([Gender].[F].Children)",
                "");
    }

    public void testToggleDrillState() {
        assertAxisReturns("ToggleDrillState({[Customers].[USA],[Customers].[Canada]},{[Customers].[USA],[Customers].[USA].[CA]})",
                fold(new String[] {
                    "[Customers].[All Customers].[USA]",
                    "[Customers].[All Customers].[USA].[CA]",
                    "[Customers].[All Customers].[USA].[OR]",
                    "[Customers].[All Customers].[USA].[WA]",
                    "[Customers].[All Customers].[Canada]"}));
    }

    public void testToggleDrillState2() {
        assertAxisReturns("ToggleDrillState([Product].[Product Department].members, {[Product].[All Products].[Food].[Snack Foods]})",
                fold(new String[] {
                    "[Product].[All Products].[Drink].[Alcoholic Beverages]",
                    "[Product].[All Products].[Drink].[Beverages]",
                    "[Product].[All Products].[Drink].[Dairy]",
                    "[Product].[All Products].[Food].[Baked Goods]",
                    "[Product].[All Products].[Food].[Baking Goods]",
                    "[Product].[All Products].[Food].[Breakfast Foods]",
                    "[Product].[All Products].[Food].[Canned Foods]",
                    "[Product].[All Products].[Food].[Canned Products]",
                    "[Product].[All Products].[Food].[Dairy]",
                    "[Product].[All Products].[Food].[Deli]",
                    "[Product].[All Products].[Food].[Eggs]",
                    "[Product].[All Products].[Food].[Frozen Foods]",
                    "[Product].[All Products].[Food].[Meat]",
                    "[Product].[All Products].[Food].[Produce]",
                    "[Product].[All Products].[Food].[Seafood]",
                    "[Product].[All Products].[Food].[Snack Foods]",
                    "[Product].[All Products].[Food].[Snack Foods].[Snack Foods]",
                    "[Product].[All Products].[Food].[Snacks]",
                    "[Product].[All Products].[Food].[Starchy Foods]",
                    "[Product].[All Products].[Non-Consumable].[Carousel]",
                    "[Product].[All Products].[Non-Consumable].[Checkout]",
                    "[Product].[All Products].[Non-Consumable].[Health and Hygiene]",
                    "[Product].[All Products].[Non-Consumable].[Household]",
                    "[Product].[All Products].[Non-Consumable].[Periodicals]"}));
    }

    public void testToggleDrillState3() {
        assertAxisReturns("ToggleDrillState(" +
                "{[Time].[1997].[Q1]," +
                " [Time].[1997].[Q2]," +
                " [Time].[1997].[Q2].[4]," +
                " [Time].[1997].[Q2].[6]," +
                " [Time].[1997].[Q3]}," +
                "{[Time].[1997].[Q2]})",
                fold(new String[] {
                    "[Time].[1997].[Q1]",
                    "[Time].[1997].[Q2]",
                    "[Time].[1997].[Q3]"}));
    }

    // bug 634860
    public void testToggleDrillStateTuple() {
        assertAxisReturns(
                fold(new String[] {
                    "ToggleDrillState(",
                    "{([Store].[All Stores].[USA].[CA]," +
                "  [Product].[All Products].[Drink].[Alcoholic Beverages]),",
                    " ([Store].[All Stores].[USA]," +
                "  [Product].[All Products].[Drink])},",
                    "{[Store].[All stores].[USA].[CA]})"}),
                fold(new String[] {
                    "{[Store].[All Stores].[USA].[CA], [Product].[All Products].[Drink].[Alcoholic Beverages]}",
                    "{[Store].[All Stores].[USA].[CA].[Alameda], [Product].[All Products].[Drink].[Alcoholic Beverages]}",
                    "{[Store].[All Stores].[USA].[CA].[Beverly Hills], [Product].[All Products].[Drink].[Alcoholic Beverages]}",
                    "{[Store].[All Stores].[USA].[CA].[Los Angeles], [Product].[All Products].[Drink].[Alcoholic Beverages]}",
                    "{[Store].[All Stores].[USA].[CA].[San Diego], [Product].[All Products].[Drink].[Alcoholic Beverages]}",
                    "{[Store].[All Stores].[USA].[CA].[San Francisco], [Product].[All Products].[Drink].[Alcoholic Beverages]}",
                    "{[Store].[All Stores].[USA], [Product].[All Products].[Drink]}"}));
    }

    public void testToggleDrillStateRecursive() {
        // We expect this to fail.
        assertThrows(
                fold(new String [] {
                    "Select ",
                    "    ToggleDrillState(",
                    "        {[Store].[All Stores].[USA]}, ",
                    "        {[Store].[All Stores].[USA]}, recursive) on Axis(0) ",
                    "from [Sales]",}),
                "'RECURSIVE' is not supported in ToggleDrillState.");
    }

    public void testTopCount() {
        assertAxisReturns("TopCount({[Promotion Media].[Media Type].members}, 2, [Measures].[Unit Sales])",
                fold(new String[] {
                    "[Promotion Media].[All Media].[No Media]",
                    "[Promotion Media].[All Media].[Daily Paper, Radio, TV]"}));
    }

    public void testTopCountTuple() {
        assertAxisReturns("TopCount([Customers].[Name].members,2,(Time.[1997].[Q1],[Measures].[Store Sales]))",
                fold(new String[] {
                    "[Customers].[All Customers].[USA].[WA].[Spokane].[Grace McLaughlin]",
                    "[Customers].[All Customers].[USA].[WA].[Spokane].[Matt Bellah]"}));
    }

    public void testTopCountEmpty() {
        assertAxisReturns("TopCount(Filter({[Promotion Media].[Media Type].members}, 1=0), 2, [Measures].[Unit Sales])",
                "");
    }

    public void testTopCountDepends() {
        checkTopBottomCountPercentDepends("TopCount");
        checkTopBottomCountPercentDepends("TopPercent");
        checkTopBottomCountPercentDepends("TopSum");
        checkTopBottomCountPercentDepends("BottomCount");
        checkTopBottomCountPercentDepends("BottomPercent");
        checkTopBottomCountPercentDepends("BottomSum");
    }

    private void checkTopBottomCountPercentDepends(String fun) {
        assertSetExprDependsOn(
                fun + "({[Promotion Media].[Media Type].members}, 2, [Measures].[Unit Sales])",
                allDimsExcept(new String[] {"[Measures]", "[Promotion Media]"}));

        if (fun.endsWith("Count")) {
            assertSetExprDependsOn(
                    fun + "({[Promotion Media].[Media Type].members}, 2)",
                    "{}");
        }
    }

    public void testTopPercent() {
        assertAxisReturns("TopPercent({[Promotion Media].[Media Type].members}, 70, [Measures].[Unit Sales])",
                "[Promotion Media].[All Media].[No Media]");
    }

    //todo: test precision

    public void testTopSum() {
        assertAxisReturns("TopSum({[Promotion Media].[Media Type].members}, 200000, [Measures].[Unit Sales])",
                fold(new String[] {
                    "[Promotion Media].[All Media].[No Media]",
                    "[Promotion Media].[All Media].[Daily Paper, Radio, TV]"}));
    }

    public void testTopSumEmpty() {
        assertAxisReturns("TopSum(Filter({[Promotion Media].[Media Type].members}, 1=0), 200000, [Measures].[Unit Sales])",
                "");
    }

    public void testUnionAll() {
        assertAxisReturns("Union({[Gender].[M]}, {[Gender].[F]}, ALL)",
                fold(new String[] {
                    "[Gender].[All Gender].[M]",
                    "[Gender].[All Gender].[F]"})); // order is preserved
    }

    public void testUnion() {
        assertAxisReturns("Union({[Store].[USA], [Store].[USA], [Store].[USA].[OR]}, {[Store].[USA].[CA], [Store].[USA]})",
                fold(new String[] {
                    "[Store].[All Stores].[USA]",
                    "[Store].[All Stores].[USA].[OR]",
                    "[Store].[All Stores].[USA].[CA]"}));
    }

    public void testUnionEmptyBoth() {
        assertAxisReturns("Union({}, {})",
                "");
    }

    public void testUnionEmptyRight() {
        assertAxisReturns("Union({[Gender].[M]}, {})",
                "[Gender].[All Gender].[M]");
    }

    public void testUnionTuple() {
        assertAxisReturns("Union({" +
                " ([Gender].[M], [Marital Status].[S])," +
                " ([Gender].[F], [Marital Status].[S])" +
                "}, {" +
                " ([Gender].[M], [Marital Status].[M])," +
                " ([Gender].[M], [Marital Status].[S])" +
                "})",

                fold(new String[] {
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S]}",
                    "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S]}",
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M]}"}));
    }

    public void testUnionTupleDistinct() {
        assertAxisReturns("Union({" +
                " ([Gender].[M], [Marital Status].[S])," +
                " ([Gender].[F], [Marital Status].[S])" +
                "}, {" +
                " ([Gender].[M], [Marital Status].[M])," +
                " ([Gender].[M], [Marital Status].[S])" +
                "}, Distinct)",

                fold(new String[] {
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S]}",
                    "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S]}",
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M]}"}));

    }

    public void testUnionQuery() {
        Result result = executeQuery(
                fold(new String[] {
                    "select {[Measures].[Unit Sales], [Measures].[Store Cost], [Measures].[Store Sales]} on columns,",
                    " Hierarchize( ",
                    "   Union(",
                    "     Crossjoin(",
                    "       Crossjoin([Gender].[All Gender].children,",
                    "                 [Marital Status].[All Marital Status].children ),",
                    "       Crossjoin([Customers].[All Customers].children,",
                    "                 [Product].[All Products].children ) ),",
                    "     Crossjoin( {([Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M] )},",
                    "       Crossjoin(",
                    "         [Customers].[All Customers].[USA].children,",
                    "         [Product].[All Products].children ) ) )) on rows",
                    "from Sales where ([Time].[1997])"}));
        final Axis rowsAxis = result.getAxes()[1];
        Assert.assertEquals(45, rowsAxis.positions.length);
    }

    public void testItemMember() {
        assertExprReturns("Descendants([Time].[1997], [Time].[Month]).Item(1).Item(0).UniqueName",
                "[Time].[1997].[Q1].[2]");

        // Access beyond the list yields the Null member.
        assertExprReturns("[Time].[1997].Children.Item(6).UniqueName", "[Time].[#Null]");
        assertExprReturns("[Time].[1997].Children.Item(-1).UniqueName", "[Time].[#Null]");
    }

    public void testItemTuple() {
        assertExprReturns("CrossJoin([Gender].[All Gender].children, " +
                "[Time].[1997].[Q2].children).Item(0).Item(1).UniqueName",
            "[Time].[1997].[Q2].[4]");
    }

    public void testStrToMember() {
        assertExprReturns("StrToMember(\"[Time].[1997].[Q2].[4]\").Name",
            "4");
    }

    // TODO: finish implementing StrToTuple
    public void _testStrToTuple() {
        assertExprReturns("StrToTuple(\"([Gender].[F], [Time].[1997].[Q2])\", [Gender], [Time])",
                "([Gender].[F], [Time].[1997].[Q2])");
    }

    // TODO: finish implementing StrToSet
    public void _testStrToSet() {
        assertAxisReturns("StrToSet(\"(" +
                "{([Gender].[F], [Time].[1997].[Q2]), " +
                " ([Gender].[M], [Time].[1997])}\"," +
                " [Gender]," +
                " [Time])",
                "{([Gender].[F], [Time].[1997].[Q2])," +
                " ([Gender].[M], [Time].[1997])}");
    }

    public void testYtd() {
        assertAxisReturns("Ytd()", "[Time].[1997]");
        assertAxisReturns("Ytd([Time].[1997].[Q3])",
                fold(new String[] {
                    "[Time].[1997].[Q1]",
                    "[Time].[1997].[Q2]",
                    "[Time].[1997].[Q3]"}));
        assertAxisReturns("Ytd([Time].[1997].[Q2].[4])",
                fold(new String[] {
                    "[Time].[1997].[Q1].[1]",
                    "[Time].[1997].[Q1].[2]",
                    "[Time].[1997].[Q1].[3]",
                    "[Time].[1997].[Q2].[4]"}));
        assertAxisThrows("Ytd([Store])",
                "Argument to function 'Ytd' must belong to Time hierarchy");
        assertSetExprDependsOn("Ytd()", "{[Time]}");
        assertSetExprDependsOn("Ytd([Time].[1997].[Q2])", "{}");
    }

    public void testQtd() {
        // zero args
        assertQueryReturns(
                fold(new String[] {
                    "with member [Measures].[Foo] as ' SetToStr(Qtd()) '",
                    "select {[Measures].[Foo]} on columns",
                    "from [Sales]",
                    "where [Time].[1997].[Q2].[5]"}),
                fold(new String[] {
                    "Axis #0:",
                    "{[Time].[1997].[Q2].[5]}",
                    "Axis #1:",
                    "{[Measures].[Foo]}",
                    "Row #0: {[Time].[1997].[Q2].[4], [Time].[1997].[Q2].[5]}" + nl}));

        // one arg, a month
        assertAxisReturns("Qtd([Time].[1997].[Q2].[5])",
                fold(new String[] {
                    "[Time].[1997].[Q2].[4]",
                    "[Time].[1997].[Q2].[5]"}));


        // one arg, a quarter
        assertAxisReturns("Qtd([Time].[1997].[Q2])",
                "[Time].[1997].[Q2]");

        // one arg, a year
        assertAxisReturns("Qtd([Time].[1997])",
                "");

        assertAxisThrows("Qtd([Store])",
                "Argument to function 'Qtd' must belong to Time hierarchy");
    }

    public void testMtd() {
        // zero args
        assertQueryReturns(
                fold(new String[] {
                    "with member [Measures].[Foo] as ' SetToStr(Mtd()) '",
                    "select {[Measures].[Foo]} on columns",
                    "from [Sales]",
                    "where [Time].[1997].[Q2].[5]"}),
                fold(new String[] {
                    "Axis #0:",
                    "{[Time].[1997].[Q2].[5]}",
                    "Axis #1:",
                    "{[Measures].[Foo]}",
                    "Row #0: {[Time].[1997].[Q2].[5]}" + nl}));

        // one arg, a month
        assertAxisReturns("Mtd([Time].[1997].[Q2].[5])",
                "[Time].[1997].[Q2].[5]");

        // one arg, a quarter
        assertAxisReturns("Mtd([Time].[1997].[Q2])",
                "");

        // one arg, a year
        assertAxisReturns("Mtd([Time].[1997])",
                "");

        assertAxisThrows("Mtd([Store])",
                "Argument to function 'Mtd' must belong to Time hierarchy");
    }

    public void testPeriodsToDate() {
        assertSetExprDependsOn("PeriodsToDate()", "{[Time]}");
        assertSetExprDependsOn("PeriodsToDate([Time].[Year])", "{[Time]}");
        assertSetExprDependsOn("PeriodsToDate([Time].[Year], [Time].[1997].[Q2].[5])", "{}");

        assertAxisThrows(
                "PeriodsToDate([Product].[Product Family])",
                "Argument to function 'PeriodsToDate' must belong to Time hierarchy.");

        // two args
        assertAxisReturns(
                "PeriodsToDate([Time].[Quarter], [Time].[1997].[Q2].[5])",
                fold(new String[] {
                    "[Time].[1997].[Q2].[4]",
                    "[Time].[1997].[Q2].[5]"}));

        // equivalent to above
        assertAxisReturns(
                "TopCount(" +
                "  Descendants(" +
                "    Ancestor(" +
                "      [Time].[1997].[Q2].[5], [Time].[Quarter])," +
                "    [Time].[1997].[Q2].[5].Level)," +
                "  1).Item(0) : [Time].[1997].[Q2].[5]",
                fold(new String[] {
                    "[Time].[1997].[Q2].[4]",
                    "[Time].[1997].[Q2].[5]"}));

        // one arg
        assertQueryReturns(
                fold(new String[] {
                    "with member [Measures].[Foo] as ' SetToStr(PeriodsToDate([Time].[Quarter])) '",
                    "select {[Measures].[Foo]} on columns",
                    "from [Sales]",
                    "where [Time].[1997].[Q2].[5]"}),
                fold(new String[] {
                    "Axis #0:",
                    "{[Time].[1997].[Q2].[5]}",
                    "Axis #1:",
                    "{[Measures].[Foo]}",
                    "Row #0: {[Time].[1997].[Q2].[4], [Time].[1997].[Q2].[5]}" + nl}));

        // zero args
        assertQueryReturns(
                fold(new String[] {
                    "with member [Measures].[Foo] as ' SetToStr(PeriodsToDate()) '",
                    "select {[Measures].[Foo]} on columns",
                    "from [Sales]",
                    "where [Time].[1997].[Q2].[5]"}),
                fold(new String[] {
                    "Axis #0:",
                    "{[Time].[1997].[Q2].[5]}",
                    "Axis #1:",
                    "{[Measures].[Foo]}",
                    "Row #0: {[Time].[1997].[Q2].[4], [Time].[1997].[Q2].[5]}" + nl}));

        // zero args, evaluated at a member which is at the top level.
        // The default level is the level above the current member -- so
        // choosing a member at the highest level might trip up the
        // implementation.
        assertQueryReturns(
                fold(new String[] {
                    "with member [Measures].[Foo] as ' SetToStr(PeriodsToDate()) '",
                    "select {[Measures].[Foo]} on columns",
                    "from [Sales]",
                    "where [Time].[1997]"}),
                fold(new String[] {
                    "Axis #0:",
                    "{[Time].[1997]}",
                    "Axis #1:",
                    "{[Measures].[Foo]}",
                    "Row #0: {}" + nl}));
    }

    public void testSetToStr() {
        assertExprReturns("SetToStr([Time].children)",
                "{[Time].[1997].[Q1], [Time].[1997].[Q2], [Time].[1997].[Q3], [Time].[1997].[Q4]}");

        // Now, applied to tuples
        assertExprReturns("SetToStr({CrossJoin([Marital Status].children, {[Gender].[M]})})",
                "{" +
                "([Marital Status].[All Marital Status].[M]," +
                " [Gender].[All Gender].[M]), " +
                "([Marital Status].[All Marital Status].[S]," +
                " [Gender].[All Gender].[M])" +
                "}");
    }

    public void testTupleToStr() {
        // Applied to a dimension (which becomes a member)
        assertExprReturns("TupleToStr([Time])",
                "[Time].[1997]");

        // Applied to a member
        assertExprReturns("TupleToStr([Store].[USA].[OR])",
                "[Store].[All Stores].[USA].[OR]");

        // Applied to a member (extra set of parens)
        assertExprReturns("TupleToStr(([Store].[USA].[OR]))",
                "[Store].[All Stores].[USA].[OR]");

        // Now, applied to a tuple
        assertExprReturns("TupleToStr(([Marital Status], [Gender].[M]))",
                "([Marital Status].[All Marital Status], [Gender].[All Gender].[M])");

        // Applied to a tuple containing a null member
        assertExprReturns("TupleToStr(([Marital Status], [Gender].Parent))",
                "");

        // Applied to a null member
        assertExprReturns("TupleToStr([Marital Status].Parent)",
                "");
    }

    /**
     * Executes a scalar expression, and asserts that the result is as
     * expected. For example, <code>assertExprReturns("1 + 2", "3")</code>
     * should succeed.
     */
    public void assertExprReturns(String expr, String expected) {
        String actual = executeExpr(expr);
        assertEquals(expected, actual);
    }

    /**
     * Executes a scalar expression, and asserts that the result is within
     * delta of the expected result.
     */
    public void assertExprReturns(
            String expr, double expected, double delta) {
        String actual = executeExpr(expr);

        try {
            Assert.assertEquals(null,
                    expected,
                    Double.parseDouble(actual),
                    delta);
        } catch (NumberFormatException ex) {
            String msg = "Actual value \"" +
                    actual +
                    "\" is not a double.";
            throw new ComparisonFailure(
                    msg, Double.toString(expected), actual);
        }
    }

    /**
     * Compiles a scalar expression, and asserts that the program looks as
     * expected.
     */
    public void assertExprCompilesTo(
            String expr, String expectedCalc) {
        final String actualCalc = getTestContext().compileExpression(expr, true);
        final int expDeps =
                MondrianProperties.instance().TestExpDependencies.get();
        if (expDeps > 0) {
            // Don't bother checking the compiled output if we are also
            // testing dependencies. The compiled code will have extra
            // 'DependencyTestingCalc' instances embedded in it.
            return;
        }
        assertEquals(actualCalc, expectedCalc);
    }

    /**
     * Compiles a set expression, and asserts that the program looks as
     * expected.
     */
    public void assertAxisCompilesTo(
            String expr, String expectedCalc) {
        final String actualCalc = getTestContext().compileExpression(expr, false);
        final int expDeps =
                MondrianProperties.instance().TestExpDependencies.get();
        if (expDeps > 0) {
            // Don't bother checking the compiled output if we are also
            // testing dependencies. The compiled code will have extra
            // 'DependencyTestingCalc' instances embedded in it.
            return;
        }
        assertEquals(expectedCalc, actualCalc);
    }

    /**
     * Tests the <code>Rank(member, set)</code> MDX function.
     */
    public void testRank() {
        // Member within set
        assertExprReturns("Rank([Store].[USA].[CA], " +
                "{[Store].[USA].[OR]," +
                " [Store].[USA].[CA]," +
                " [Store].[USA]})",
                "2");
        // Member not in set
        assertExprReturns("Rank([Store].[USA].[WA], " +
                "{[Store].[USA].[OR]," +
                " [Store].[USA].[CA]," +
                " [Store].[USA]})",
                "0");
        // Member not in empty set
        assertExprReturns("Rank([Store].[USA].[WA], {})",
                "0");
        // Null member not in set returns null.
        assertExprReturns("Rank([Store].Parent, " +
                "{[Store].[USA].[OR]," +
                " [Store].[USA].[CA]," +
                " [Store].[USA]})",
                "(null)");
        // Null member in empty set. (MSAS returns an error "Formula error -
        // dimension count is not valid - in the Rank function" but I think
        // null is the correct behavior.)
        assertExprReturns("Rank([Gender].Parent, {})",
                "(null)");
        // Member occurs twice in set -- pick first
        assertExprReturns(
                fold(new String[] {
                    "Rank([Store].[USA].[WA], ",
                    "{[Store].[USA].[WA]," +
                " [Store].[USA].[CA]," +
                " [Store].[USA]," +
                " [Store].[USA].[WA]})"}),
                "1");
        // Tuple not in set
        assertExprReturns(
                fold(new String[] {
                    "Rank(([Gender].[F], [Marital Status].[M]), ",
                    "{([Gender].[F], [Marital Status].[S]),",
                    " ([Gender].[M], [Marital Status].[S]),",
                    " ([Gender].[M], [Marital Status].[M])})"}),
                "0");
        // Tuple in set
        assertExprReturns(
                fold(new String[] {
                    "Rank(([Gender].[F], [Marital Status].[M]), ",
                    "{([Gender].[F], [Marital Status].[S]),",
                    " ([Gender].[M], [Marital Status].[S]),",
                    " ([Gender].[F], [Marital Status].[M])})"}),
                "3");
        // Tuple not in empty set
        assertExprReturns(
                fold(new String[] {
                    "Rank(([Gender].[F], [Marital Status].[M]), ",
                    "{})"}),
                "0");
        // Partially null tuple in set, returns null
        assertExprReturns(
                fold(new String[] {
                    "Rank(([Gender].[F], [Marital Status].Parent), ",
                    "{([Gender].[F], [Marital Status].[S]),",
                    " ([Gender].[M], [Marital Status].[S]),",
                    " ([Gender].[F], [Marital Status].[M])})"}),
                "(null)");
    }

    public void testRankWithExpr() {
        assertQueryReturns(
                fold(new String[] {
                    "with member [Measures].[Sibling Rank] as ' Rank([Product].CurrentMember, [Product].CurrentMember.Siblings) '",
                    "  member [Measures].[Sales Rank] as ' Rank([Product].CurrentMember, Order([Product].Parent.Children, [Measures].[Unit Sales], DESC)) '",
                    "  member [Measures].[Sales Rank2] as ' Rank([Product].CurrentMember, [Product].Parent.Children, [Measures].[Unit Sales]) '",
                    "select {[Measures].[Unit Sales], [Measures].[Sales Rank], [Measures].[Sales Rank2]} on columns,",
                    " {[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].children} on rows",
                    "from [Sales]",
                    "WHERE ( [Store].[All Stores].[USA].[OR].[Portland].[Store 11], [Time].[1997].[Q2].[6])"}),
                fold(new String[] {
                    "Axis #0:",
                    "{[Store].[All Stores].[USA].[OR].[Portland].[Store 11], [Time].[1997].[Q2].[6]}",
                    "Axis #1:",
                    "{[Measures].[Unit Sales]}",
                    "{[Measures].[Sales Rank]}",
                    "{[Measures].[Sales Rank2]}",
                    "Axis #2:",
                    "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good]}",
                    "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Pearl]}",
                    "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth]}",
                    "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Top Measure]}",
                    "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Walrus]}",
                    "Row #0: 5",
                    "Row #0: 1",
                    "Row #0: 1",
                    "Row #1: (null)",
                    "Row #1: 5",
                    "Row #1: 5",
                    "Row #2: 3",
                    "Row #2: 3",
                    "Row #2: 3",
                    "Row #3: 5",
                    "Row #3: 2",
                    "Row #3: 1",
                    "Row #4: 3",
                    "Row #4: 4",
                    "Row #4: 3" + nl}));
    }

    public void testRankWithExpr2() {
        // Data: Unit Sales
        // All gender 266,733
        // F          131,558
        // M          135,215
        assertExprReturns(
                "Rank([Gender].[All Gender]," +
                " {[Gender].Members}," +
                " [Measures].[Unit Sales])",
                "1");
        assertExprReturns(
                "Rank([Gender].[F]," +
                " {[Gender].Members}," +
                " [Measures].[Unit Sales])",
                "3");
        assertExprReturns(
                "Rank([Gender].[M]," +
                " {[Gender].Members}," +
                " [Measures].[Unit Sales])",
                "2");
        // Null member. Expression evaluates to null, therefore value does
        // not appear in the list of values, therefore the rank is null.
        assertExprReturns(
                "Rank([Gender].[All Gender].Parent," +
                " {[Gender].Members}," +
                " [Measures].[Unit Sales])",
                "(null)");
        // Empty set. Value never appears in the set, therefore rank is null.
        assertExprReturns(
                "Rank([Gender].[M]," +
                " {}," +
                " [Measures].[Unit Sales])",
                "(null)");
        // Member is not in set
        assertExprReturns(
                "Rank([Gender].[M]," +
                " {[Gender].[All Gender], [Gender].[F]})",
                "0");
        // Even though M is not in the set, its value lies between [All Gender]
        // and [F].
        assertExprReturns("Rank([Gender].[M]," +
                " {[Gender].[All Gender], [Gender].[F]}," +
                " [Measures].[Unit Sales])",
                "2");
        // Expr evaluates to null for some values of set.
        assertExprReturns(
                "Rank([Product].[Non-Consumable].[Household]," +
                " {[Product].[Food], [Product].[All Products], [Product].[Drink].[Dairy]}," +
                " [Product].CurrentMember.Parent)",
                "2");
        // Expr evaluates to null for all values in the set.
        assertExprReturns("Rank([Gender].[M]," +
                " {[Gender].[All Gender], [Gender].[F]}," +
                " [Marital Status].[All Marital Status].Parent)",
                "1");
    }

    /**
     * Tests a RANK function which is so large that we need to use caching
     * in order to execute it efficiently.
     */
    public void testRankHuge() {
        // If caching is disabled, don't even try -- it will take too long.
        if (!MondrianProperties.instance().EnableExpCache.get()) {
            return;
        }

        String query = fold(new String[] {
            "WITH ",
            "  MEMBER [Measures].[Rank among products] ",
            "    AS ' Rank([Product].CurrentMember, Order([Product].members, [Measures].[Unit Sales], BDESC)) '",
            "SELECT CrossJoin(",
            "  [Gender].members,",
            "  {[Measures].[Unit Sales],",
            "   [Measures].[Rank among products]}) ON COLUMNS,",
//                "  {[Product], [Product].[All Products].[Non-Consumable].[Periodicals].[Magazines].[Sports Magazines].[Robust].[Robust Monthly Sports Magazine]} ON ROWS",
            "  {[Product].members} ON ROWS",
            "FROM [Sales]"});
        checkRankHuge(query, false);
    }

    /**
     * As {@link #testRankHuge()}, but for the 3-argument form of the
     * <code>RANK</code> function.
     *
     * <p>Disabled by jhyde, 2006/2/14. Bug 1431316 logged.
     */
    public void _testRank3Huge() {
        // If caching is disabled, don't even try -- it will take too long.
        if (!MondrianProperties.instance().EnableExpCache.get()) {
            return;
        }

        String query = fold(new String[] {
            "WITH ",
            "  MEMBER [Measures].[Rank among products] ",
            "    AS ' Rank([Product].CurrentMember, [Product].members, [Measures].[Unit Sales]) '",
            "SELECT CrossJoin(",
            "  [Gender].members,",
            "  {[Measures].[Unit Sales],",
            "   [Measures].[Rank among products]}) ON COLUMNS,",
                "  {[Product], [Product].[All Products].[Non-Consumable].[Periodicals].[Magazines].[Sports Magazines].[Robust].[Robust Monthly Sports Magazine]} ON ROWS",
//            "  {[Product].members} ON ROWS",
            "FROM [Sales]"});
        checkRankHuge(query, true);
    }

    private void checkRankHuge(String query, boolean rank3) {
        final Result result = getTestContext().executeQuery(query);
        final Axis[] axes = result.getAxes();
        final Axis rowsAxis = axes[1];
        final int rowCount = rowsAxis.positions.length;
        assertEquals(2256, rowCount);
        // [All Products], [All Gender], [Rank]
        Cell cell = result.getCell(new int[] {1, 0});
        assertEquals("1", cell.getFormattedValue());
        // [Robust Monthly Sports Magazine]
        Member member = rowsAxis.positions[rowCount - 1].members[0];
        assertEquals("Robust Monthly Sports Magazine", member.getName());
        // [Robust Monthly Sports Magazine], [All Gender], [Rank]
        cell = result.getCell(new int[] {0, rowCount - 1});
        assertEquals("152", cell.getFormattedValue());
        cell = result.getCell(new int[] {1, rowCount - 1});
        assertEquals(rank3 ? "1,854" : "1,871", cell.getFormattedValue());
        // [Robust Monthly Sports Magazine], [Gender].[F], [Rank]
        cell = result.getCell(new int[] {2, rowCount - 1});
        assertEquals("90", cell.getFormattedValue());
        cell = result.getCell(new int[] {3, rowCount - 1});
        assertEquals(rank3 ? "1,119" : "1,150", cell.getFormattedValue());
        // [Robust Monthly Sports Magazine], [Gender].[M], [Rank]
        cell = result.getCell(new int[] {4, rowCount - 1});
        assertEquals("62", cell.getFormattedValue());
        cell = result.getCell(new int[] {5, rowCount - 1});
        assertEquals(rank3 ? "2,131" : "2,147", cell.getFormattedValue());
    }

    public void testLinRegPointQuarter() {
        String query = fold(new String[] {
            "WITH MEMBER [Measures].[Test] as ",
            "  'LinRegPoint(",
            "    Rank(Time.CurrentMember, Time.CurrentMember.Level.Members),",
            "    Descendants([Time].[1997], [Time].[Quarter]), ",
            "[Measures].[Store Sales], ",
            "    Rank(Time.CurrentMember, Time.CurrentMember.Level.Members))' ",
            "SELECT ",
            "{[Measures].[Test],[Measures].[Store Sales]} ON ROWS, ",
            "{[Time].[1997].Children} ON COLUMNS ",
            "FROM Sales"});
        String expected = fold(new String[] {
            "Axis #0:",
            "{}",
            "Axis #1:",
            "{[Time].[1997].[Q1]}",
            "{[Time].[1997].[Q2]}",
            "{[Time].[1997].[Q3]}",
            "{[Time].[1997].[Q4]}",
            "Axis #2:",
            "{[Measures].[Test]}",
            "{[Measures].[Store Sales]}",
            "Row #0: 134,299.22",
            "Row #0: 138,972.76",
            "Row #0: 143,646.30",
            "Row #0: 148,319.85",
            "Row #1: 139,628.35",
            "Row #1: 132,666.27",
            "Row #1: 140,271.89",
            "Row #1: 152,671.62" + nl});

        assertQueryReturns(query, expected);
    }

    /**
     * Tests all of the linear regression functions, as suggested by
     * <a href="http://support.microsoft.com/kb/q307276/">a Microsoft knowledge
     * base article</a>.
     */
    public void _testLinRegAll() {
        // We have not implemented the LastPeriods function, so we use
        //   [Time].CurrentMember.Lag(9) : [Time].CurrentMember
        // is equivalent to
        //   LastPeriods(10)
        String query = fold(new String[] {
            "WITH MEMBER ",
            "[Measures].[Intercept] AS ",
            "  'LinRegIntercept([Time].CurrentMember.Lag(10) : [Time].CurrentMember, [Measures].[Unit Sales], [Measures].[Store Sales])' ",
            "MEMBER [Measures].[Regression Slope] AS",
            "  'LinRegSlope([Time].CurrentMember.Lag(9) : [Time].CurrentMember,[Measures].[Unit Sales],[Measures].[Store Sales]) '",
            "MEMBER [Measures].[Predict] AS",
            "  'LinRegPoint([Measures].[Unit Sales],[Time].CurrentMember.Lag(9) : [Time].CurrentMember,[Measures].[Unit Sales],[Measures].[Store Sales])',",
            "  FORMAT_STRING = 'Standard' ",
            "MEMBER [Measures].[Predict Formula] AS",
            "  '([Measures].[Regression Slope] * [Measures].[Unit Sales]) + [Measures].[Intercept]',",
            "  FORMAT_STRING='Standard'",
            "MEMBER [Measures].[Good Fit] AS",
            "  'LinRegR2([Time].CurrentMember.Lag(9) : [Time].CurrentMember, [Measures].[Unit Sales],[Measures].[Store Sales])',",
            "  FORMAT_STRING='#,#.00'",
            "MEMBER [Measures].[Variance] AS",
            "  'LinRegVariance([Time].CurrentMember.Lag(9) : [Time].CurrentMember,[Measures].[Unit Sales],[Measures].[Store Sales])'",
            "SELECT ",
            "  {[Measures].[Store Sales], ",
            "   [Measures].[Intercept], ",
            "   [Measures].[Regression Slope], ",
            "   [Measures].[Predict], ",
            "   [Measures].[Predict Formula], ",
            "   [Measures].[Good Fit], ",
            "   [Measures].[Variance] } ON COLUMNS, ",
            "  Descendants([Time].[1997], [Time].[Month]) ON ROWS",
            "FROM Sales"});

        String expected = fold(new String[] {
            "Axis #0:",
            "{}",
            "Axis #1:",
            "{[Measures].[Store Sales]}",
            "{[Measures].[Intercept]}",
            "{[Measures].[Regression Slope]}",
            "{[Measures].[Predict]}",
            "{[Measures].[Predict Formula]}",
            "{[Measures].[Good Fit]}",
            "{[Measures].[Variance]}",
            "Axis #2:",
            "{[Time].[1997].[Q1].[1]}",
            "{[Time].[1997].[Q1].[2]}",
            "{[Time].[1997].[Q1].[3]}",
            "{[Time].[1997].[Q2].[4]}",
            "{[Time].[1997].[Q2].[5]}",
            "{[Time].[1997].[Q2].[6]}",
            "{[Time].[1997].[Q3].[7]}",
            "{[Time].[1997].[Q3].[8]}",
            "{[Time].[1997].[Q3].[9]}",
            "{[Time].[1997].[Q4].[10]}",
            "{[Time].[1997].[Q4].[11]}",
            "{[Time].[1997].[Q4].[12]}",
            "Row #0: 45,539.69",
            "Row #0: 68711.40",
            "Row #0: -1.033",
            "Row #0: 46,350.26",
            "Row #0: 46.350.26",
            "Row #0: -1.#INF",
            "Row #0: 5.17E-08",
            "...",
            "Row #11: 15343.67" + nl});

        assertQueryReturns(query, expected);
    }

    public void testLinRegPointMonth() {
        String query = fold(new String[] {
            "WITH MEMBER ",
            "[Measures].[Test] as ",
            "  'LinRegPoint(",
            "    Rank(Time.CurrentMember, Time.CurrentMember.Level.Members),",
            "    Descendants([Time].[1997], [Time].[Month]), ",
            "    [Measures].[Store Sales], ",
            "    Rank(Time.CurrentMember, Time.CurrentMember.Level.Members)",
            "  )' ",
            "SELECT ",
            "  {[Measures].[Test],[Measures].[Store Sales]} ON ROWS, ",
            "  Descendants([Time].[1997], [Time].[Month]) ON COLUMNS ",
            "FROM Sales"});

        String expected = fold(new String[] {
            "Axis #0:",
            "{}",
            "Axis #1:",
            "{[Time].[1997].[Q1].[1]}",
            "{[Time].[1997].[Q1].[2]}",
            "{[Time].[1997].[Q1].[3]}",
            "{[Time].[1997].[Q2].[4]}",
            "{[Time].[1997].[Q2].[5]}",
            "{[Time].[1997].[Q2].[6]}",
            "{[Time].[1997].[Q3].[7]}",
            "{[Time].[1997].[Q3].[8]}",
            "{[Time].[1997].[Q3].[9]}",
            "{[Time].[1997].[Q4].[10]}",
            "{[Time].[1997].[Q4].[11]}",
            "{[Time].[1997].[Q4].[12]}",
            "Axis #2:",
            "{[Measures].[Test]}",
            "{[Measures].[Store Sales]}",
            "Row #0: 43,824.36",
            "Row #0: 44,420.51",
            "Row #0: 45,016.66",
            "Row #0: 45,612.81",
            "Row #0: 46,208.95",
            "Row #0: 46,805.10",
            "Row #0: 47,401.25",
            "Row #0: 47,997.40",
            "Row #0: 48,593.55",
            "Row #0: 49,189.70",
            "Row #0: 49,785.85",
            "Row #0: 50,382.00",
            "Row #1: 45,539.69",
            "Row #1: 44,058.79",
            "Row #1: 50,029.87",
            "Row #1: 42,878.25",
            "Row #1: 44,456.29",
            "Row #1: 45,331.73",
            "Row #1: 50,246.88",
            "Row #1: 46,199.04",
            "Row #1: 43,825.97",
            "Row #1: 42,342.27",
            "Row #1: 53,363.71",
            "Row #1: 56,965.64" + nl});

        assertQueryReturns(query, expected);
    }

    public void testLinRegIntercept() {
        assertExprReturns("LinRegIntercept([Time].[Month].members," +
                " [Measures].[Unit Sales], [Measures].[Store Sales])",
                -126.65,
                0.50);

/*
-1#IND missing data
*/
/*
1#INF division by zero
*/
/*
The following table shows query return values from using different
FORMAT_STRING's in an expression involving 'division by zero' (tested on
Intel platforms):

+===========================+=====================+
| Format Strings            | Query Return Values |
+===========================+=====================+
| FORMAT_STRING="           | 1.#INF              |
+===========================+=====================+
| FORMAT_STRING='Standard'  | 1.#J                |
+===========================+=====================+
| FORMAT_STRING='Fixed'     | 1.#J                |
+===========================+=====================+
| FORMAT_STRING='Percent'   | 1#I.NF%             |
+===========================+=====================+
| FORMAT_STRING='Scientific'| 1.JE+00             |
+===========================+=====================+
*/

/*
Mondrian can not return "missing data" value -1.#IND
// empty set
assertExprReturns("LinRegIntercept({[Time].Parent}," +
" [Measures].[Unit Sales], [Measures].[Store Sales])",
"-1.#IND"); // MSAS returns -1.#IND (whatever that means)
*/


/*
// first expr constant
assertExprReturns("LinRegIntercept([Time].[Month].members," +
" 7, [Measures].[Store Sales])",
"$7.00");
*/
        // format does not add '$'
        assertExprReturns("LinRegIntercept([Time].[Month].members," +
                " 7, [Measures].[Store Sales])",
                7.00, 0.01);

/*
Mondrian can not return "missing data" value -1.#IND
// second expr constant
assertExprReturns("LinRegIntercept([Time].[Month].members," +
" [Measures].[Unit Sales], 4)",
"-1.#IND"); // MSAS returns -1.#IND (whatever that means)
*/
    }

    public void testLinRegSlope() {
        assertExprReturns("LinRegSlope([Time].[Month].members," +
                " [Measures].[Unit Sales], [Measures].[Store Sales])",
                0.4746,
                0.50);

/*
Mondrian can not return "missing data" value -1.#IND
// empty set
assertExprReturns("LinRegSlope({[Time].Parent}," +
" [Measures].[Unit Sales], [Measures].[Store Sales])",
"-1.#IND"); // MSAS returns -1.#IND (whatever that means)
*/

/*
// first expr constant
assertExprReturns("LinRegSlope([Time].[Month].members," +
" 7, [Measures].[Store Sales])",
"$7.00");
^^^^
copy and paste error
*/
        assertExprReturns("LinRegSlope([Time].[Month].members," +
                " 7, [Measures].[Store Sales])",
                0.00,
                0.01);

/*
Mondrian can not return "missing data" value -1.#IND
// second expr constant
assertExprReturns("LinRegSlope([Time].[Month].members," +
" [Measures].[Unit Sales], 4)",
"-1.#IND"); // MSAS returns -1.#IND (whatever that means)
*/
    }

    public void testLinRegPoint() {
/*
NOTE: mdx does not parse
assertExprReturns("LinRegPoint([Measures].[Unit Sales]," +
" [Time].CurrentMember[Time].[Month].members," +
" [Measures].[Unit Sales], [Measures].[Store Sales])",
"0.4746");
*/

/*
Mondrian can not return "missing data" value -1.#IND

// empty set
assertExprReturns("LinRegPoint([Measures].[Unit Sales]," +
" {[Time].Parent}," +
" [Measures].[Unit Sales], [Measures].[Store Sales])",
"-1.#IND"); // MSAS returns -1.#IND (whatever that means)
*/

/*
Expected value is wrong
// zeroth expr constant
assertExprReturns("LinRegPoint(-1," +
" [Time].[Month].members," +
" 7, [Measures].[Store Sales])",
"-127.124");
*/

/*
// first expr constant
assertExprReturns("LinRegPoint([Measures].[Unit Sales]," +
" [Time].[Month].members," +
" 7, [Measures].[Store Sales])",
"$7.00");
*/
        // format does not add '$'
        assertExprReturns("LinRegPoint([Measures].[Unit Sales]," +
                " [Time].[Month].members," +
                " 7, [Measures].[Store Sales])",
                7.00, 0.01);

/*
Mondrian can not return "missing data" value -1.#IND
// second expr constant
assertExprReturns("LinRegPoint([Measures].[Unit Sales]," +
" [Time].[Month].members," +
" [Measures].[Unit Sales], 4)",
"-1.#IND"); // MSAS returns -1.#IND (whatever that means)
*/
    }

    public void _testLinRegR2() {
/*
Why would R2 equal the slope
assertExprReturns("LinRegR2([Time].[Month].members," +
" [Measures].[Unit Sales], [Measures].[Store Sales])",
"0.4746");
*/

/*
Mondrian can not return "missing data" value -1.#IND
// empty set
assertExprReturns("LinRegR2({[Time].Parent}," +
" [Measures].[Unit Sales], [Measures].[Store Sales])",
"-1.#IND"); // MSAS returns -1.#IND (whatever that means)
*/

        // first expr constant
        assertExprReturns("LinRegR2([Time].[Month].members," +
                " 7, [Measures].[Store Sales])",
                "$7.00");

/*
Mondrian can not return "missing data" value -1.#IND
// second expr constant
assertExprReturns("LinRegR2([Time].[Month].members," +
" [Measures].[Unit Sales], 4)",
"-1.#IND"); // MSAS returns -1.#IND (whatever that means)
*/
    }

    public void _testLinRegVariance() {
        assertExprReturns("LinRegVariance([Time].[Month].members," +
                " [Measures].[Unit Sales], [Measures].[Store Sales])",
                "0.4746");

        // empty set
        assertExprReturns("LinRegVariance({[Time].Parent}," +
                " [Measures].[Unit Sales], [Measures].[Store Sales])",
                "-1.#IND"); // MSAS returns -1.#IND (whatever that means)

        // first expr constant
        assertExprReturns("LinRegVariance([Time].[Month].members," +
                " 7, [Measures].[Store Sales])",
                "$7.00");

        // second expr constant
        assertExprReturns("LinRegVariance([Time].[Month].members," +
                " [Measures].[Unit Sales], 4)",
                "-1.#IND"); // MSAS returns -1.#IND (whatever that means)
    }

    public void testVisualTotalsBasic() {
        assertQueryReturns(
                "select {[Measures].[Unit Sales]} on columns, " +
                "{VisualTotals(" +
                "    {[Product].[All Products].[Food].[Baked Goods].[Bread]," +
                "     [Product].[All Products].[Food].[Baked Goods].[Bread].[Bagels]," +
                "     [Product].[All Products].[Food].[Baked Goods].[Bread].[Muffins]}," +
                "     \"**Subtotal - *\")} on rows " +
                "from [Sales]",

                // note that Subtotal - Bread only includes 2 displayed children
                fold(new String[] {
                    "Axis #0:",
                    "{}",
                    "Axis #1:",
                    "{[Measures].[Unit Sales]}",
                    "Axis #2:",
                    "{[Product].[All Products].[Food].[Baked Goods].[*Subtotal - Bread]}",
                    "{[Product].[All Products].[Food].[Baked Goods].[Bread].[Bagels]}",
                    "{[Product].[All Products].[Food].[Baked Goods].[Bread].[Muffins]}",
                    "Row #0: 4,312",
                    "Row #1: 815",
                    "Row #2: 3,497",
                    ""}));
    }

    public void testVisualTotalsConsecutively() {
        assertQueryReturns(
                "select {[Measures].[Unit Sales]} on columns, " +
                "{VisualTotals(" +
                "    {[Product].[All Products].[Food].[Baked Goods].[Bread]," +
                "     [Product].[All Products].[Food].[Baked Goods].[Bread].[Bagels]," +
                "     [Product].[All Products].[Food].[Baked Goods].[Bread].[Bagels]," +
                "     [Product].[All Products].[Food].[Baked Goods].[Bread].[Bagels].[Colony]," +
                "     [Product].[All Products].[Food].[Baked Goods].[Bread].[Bagels]," +
                "     [Product].[All Products].[Food].[Baked Goods].[Bread].[Muffins]}," +
                "     \"**Subtotal - *\")} on rows " +
                "from [Sales]",

                // Note that [Bagels] occurs 3 times, but only once does it
                // become a subtotal. Note that the subtotal does not include
                // the following [Bagels] member.
                fold(new String[] {
                    "Axis #0:",
                    "{}",
                    "Axis #1:",
                    "{[Measures].[Unit Sales]}",
                    "Axis #2:",
                    "{[Product].[All Products].[Food].[Baked Goods].[*Subtotal - Bread]}",
                    "{[Product].[All Products].[Food].[Baked Goods].[Bread].[Bagels]}",
                    "{[Product].[All Products].[Food].[Baked Goods].[Bread].[*Subtotal - Bagels]}",
                    "{[Product].[All Products].[Food].[Baked Goods].[Bread].[Bagels].[Colony]}",
                    "{[Product].[All Products].[Food].[Baked Goods].[Bread].[Bagels]}",
                    "{[Product].[All Products].[Food].[Baked Goods].[Bread].[Muffins]}",
                    "Row #0: 5,290",
                    "Row #1: 815",
                    "Row #2: 163",
                    "Row #3: 163",
                    "Row #4: 815",
                    "Row #5: 3,497",
                    ""}));
    }

    public void testVisualTotalsNoPattern() {
        assertAxisReturns(
                "VisualTotals(" +
                "    {[Product].[All Products].[Food].[Baked Goods].[Bread]," +
                "     [Product].[All Products].[Food].[Baked Goods].[Bread].[Bagels]," +
                "     [Product].[All Products].[Food].[Baked Goods].[Bread].[Muffins]})",

                // Note that the [Bread] visual member is just called [Bread].
                fold(new String[] {
                    "[Product].[All Products].[Food].[Baked Goods].[Bread]",
                    "[Product].[All Products].[Food].[Baked Goods].[Bread].[Bagels]",
                    "[Product].[All Products].[Food].[Baked Goods].[Bread].[Muffins]"}));
    }

    public void testVisualTotalsWithFilter() {
        assertQueryReturns(
                "select {[Measures].[Unit Sales]} on columns, " +
                "{Filter(" +
                "    VisualTotals(" +
                "        {[Product].[All Products].[Food].[Baked Goods].[Bread]," +
                "         [Product].[All Products].[Food].[Baked Goods].[Bread].[Bagels]," +
                "         [Product].[All Products].[Food].[Baked Goods].[Bread].[Muffins]}," +
                "        \"**Subtotal - *\")," +
                "[Measures].[Unit Sales] > 3400)} on rows " +
                "from [Sales]",

                // Note that [*Subtotal - Bread] still contains the
                // contribution of [Bagels] 815, which was filtered out.
                fold(new String[] {
                    "Axis #0:",
                    "{}",
                    "Axis #1:",
                    "{[Measures].[Unit Sales]}",
                    "Axis #2:",
                    "{[Product].[All Products].[Food].[Baked Goods].[*Subtotal - Bread]}",
                    "{[Product].[All Products].[Food].[Baked Goods].[Bread].[Muffins]}",
                    "Row #0: 4,312",
                    "Row #1: 3,497",
                    ""}));
    }

    public void testVisualTotalsNested() {
        assertQueryReturns(
                "select {[Measures].[Unit Sales]} on columns, " +
                "{VisualTotals(" +
                "    Filter(" +
                "        VisualTotals(" +
                "            {[Product].[All Products].[Food].[Baked Goods].[Bread]," +
                "             [Product].[All Products].[Food].[Baked Goods].[Bread].[Bagels]," +
                "             [Product].[All Products].[Food].[Baked Goods].[Bread].[Muffins]}," +
                "            \"**Subtotal - *\")," +
                "    [Measures].[Unit Sales] > 3400)," +
                "    \"Second total - *\")} on rows " +
                "from [Sales]",

                // Yields the same -- no extra total.
                fold(new String[] {
                    "Axis #0:",
                    "{}",
                    "Axis #1:",
                    "{[Measures].[Unit Sales]}",
                    "Axis #2:",
                    "{[Product].[All Products].[Food].[Baked Goods].[*Subtotal - Bread]}",
                    "{[Product].[All Products].[Food].[Baked Goods].[Bread].[Muffins]}",
                    "Row #0: 4,312",
                    "Row #1: 3,497",
                    ""}));
    }

    public void testVisualTotalsFilterInside() {
        assertQueryReturns(
                "select {[Measures].[Unit Sales]} on columns, " +
                "{VisualTotals(" +
                "    Filter(" +
                "        {[Product].[All Products].[Food].[Baked Goods].[Bread]," +
                "         [Product].[All Products].[Food].[Baked Goods].[Bread].[Bagels]," +
                "         [Product].[All Products].[Food].[Baked Goods].[Bread].[Muffins]}," +
                "        [Measures].[Unit Sales] > 3400)," +
                "    \"**Subtotal - *\")} on rows " +
                "from [Sales]",

                fold(new String[] {
                    "Axis #0:",
                    "{}",
                    "Axis #1:",
                    "{[Measures].[Unit Sales]}",
                    "Axis #2:",
                    "{[Product].[All Products].[Food].[Baked Goods].[*Subtotal - Bread]}",
                    "{[Product].[All Products].[Food].[Baked Goods].[Bread].[Muffins]}",
                    "Row #0: 3,497",
                    "Row #1: 3,497",
                    ""}));
    }

    public void testVisualTotalsOutOfOrder() {
        assertQueryReturns(
                "select {[Measures].[Unit Sales]} on columns, " +
                "{VisualTotals(" +
                "    {[Product].[All Products].[Food].[Baked Goods].[Bread].[Bagels]," +
                "     [Product].[All Products].[Food].[Baked Goods].[Bread]," +
                "     [Product].[All Products].[Food].[Baked Goods].[Bread].[Muffins]}," +
                "    \"**Subtotal - *\")} on rows " +
                "from [Sales]",

                // Note that [*Subtotal - Bread] 3497 does not include 815 for
                // bagels.
                fold(new String[] {
                    "Axis #0:",
                    "{}",
                    "Axis #1:",
                    "{[Measures].[Unit Sales]}",
                    "Axis #2:",
                    "{[Product].[All Products].[Food].[Baked Goods].[Bread].[Bagels]}",
                    "{[Product].[All Products].[Food].[Baked Goods].[*Subtotal - Bread]}",
                    "{[Product].[All Products].[Food].[Baked Goods].[Bread].[Muffins]}",
                    "Row #0: 815",
                    "Row #1: 3,497",
                    "Row #2: 3,497",
                    ""}));
    }

    public void testVisualTotalsGrandparentsAndOutOfOrder() {
        assertQueryReturns(
                "select {[Measures].[Unit Sales]} on columns, " +
                "{VisualTotals(" +
                "    {[Product].[All Products].[Food]," +
                "     [Product].[All Products].[Food].[Baked Goods].[Bread]," +
                "     [Product].[All Products].[Food].[Baked Goods].[Bread].[Bagels]," +
                "     [Product].[All Products].[Food].[Frozen Foods].[Breakfast Foods]," +
                "     [Product].[All Products].[Food].[Frozen Foods].[Breakfast Foods].[Pancake Mix].[Golden]," +
                "     [Product].[All Products].[Food].[Frozen Foods].[Breakfast Foods].[Pancake Mix].[Big Time]," +
                "     [Product].[All Products].[Food].[Baked Goods].[Bread].[Muffins]}," +
                "    \"**Subtotal - *\")} on rows " +
                "from [Sales]",

                // Note:
                // [*Subtotal - Food]  = 4513 = 815 + 311 + 3497
                // [*Subtotal - Bread] = 815, does not include muffins
                // [*Subtotal - Breakfast Foods] = 311 = 110 + 201, includes grandchildren
                fold(new String[] {
                    "Axis #0:",
                    "{}",
                    "Axis #1:",
                    "{[Measures].[Unit Sales]}",
                    "Axis #2:",
                    "{[Product].[All Products].[*Subtotal - Food]}",
                    "{[Product].[All Products].[Food].[Baked Goods].[*Subtotal - Bread]}",
                    "{[Product].[All Products].[Food].[Baked Goods].[Bread].[Bagels]}",
                    "{[Product].[All Products].[Food].[Frozen Foods].[*Subtotal - Breakfast Foods]}",
                    "{[Product].[All Products].[Food].[Frozen Foods].[Breakfast Foods].[Pancake Mix].[Golden]}",
                    "{[Product].[All Products].[Food].[Frozen Foods].[Breakfast Foods].[Pancake Mix].[Big Time]}",
                    "{[Product].[All Products].[Food].[Baked Goods].[Bread].[Muffins]}",
                    "Row #0: 4,623",
                    "Row #1: 815",
                    "Row #2: 815",
                    "Row #3: 311",
                    "Row #4: 110",
                    "Row #5: 201",
                    "Row #6: 3,497",
                    ""}));
    }

    public void testVisualTotalsCrossjoin() {
        assertAxisThrows("VisualTotals(Crossjoin([Gender].Members, [Store].children))",
                "Argument to 'VisualTotals' function must be a set of members; got set of tuples.");
    }

    public void testCalculatedChild() {
    	// Construct calculated children with the same name for both [Drink] and [Non-Consumable]
    	// Then, create a metric to select the calculated child based on current product member
        assertQueryReturns(fold(new String[] {
                "with",
                " member [Product].[All Products].[Drink].[Calculated Child] as '[Product].[All Products].[Drink].[Alcoholic Beverages]'",
                " member [Product].[All Products].[Non-Consumable].[Calculated Child] as '[Product].[All Products].[Non-Consumable].[Carousel]'",
                " member [Measures].[Unit Sales CC] as '([Measures].[Unit Sales],[Product].currentmember.CalculatedChild(\"Calculated Child\"))'",
                " select non empty {[Measures].[Unit Sales CC]} on columns,", 
                " non empty {[Product].[All Products].[Drink], [Product].[All Products].[Non-Consumable]} on rows",
                " from [Sales]"}),
                
                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Unit Sales CC]}" + nl +
                "Axis #2:" + nl +
                "{[Product].[All Products].[Drink]}" + nl + "{[Product].[All Products].[Non-Consumable]}" + nl +
                "Row #0: 6,838" + nl +  // Calculated child for [Drink]
                "Row #1: 841" + nl); // Calculated child for [Non-Consumable]
        Member member = executeSingletonAxis("[Product].[All Products].CalculatedChild(\"foobar\")");
        Assert.assertEquals(member, null);
    }
    
    public void testCalculatedChildUsingItem() {
    	// Construct calculated children with the same name for both [Drink] and [Non-Consumable]
    	// Then, create a metric to select the first calculated child 
        assertQueryReturns(fold(new String[] {
                "with",
                " member [Product].[All Products].[Drink].[Calculated Child] as '[Product].[All Products].[Drink].[Alcoholic Beverages]'",
                " member [Product].[All Products].[Non-Consumable].[Calculated Child] as '[Product].[All Products].[Non-Consumable].[Carousel]'",
                " member [Measures].[Unit Sales CC] as '([Measures].[Unit Sales],AddCalculatedMembers([Product].currentmember.children).Item(\"Calculated Child\"))'",
                " select non empty {[Measures].[Unit Sales CC]} on columns,", 
                " non empty {[Product].[All Products].[Drink], [Product].[All Products].[Non-Consumable]} on rows",
                " from [Sales]"}),
                
                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Unit Sales CC]}" + nl +
                "Axis #2:" + nl +
                "{[Product].[All Products].[Drink]}" + nl + "{[Product].[All Products].[Non-Consumable]}" + nl +
                "Row #0: 6,838" + nl + 
                "Row #1: 6,838" + nl); // Note: For [Non-Consumable], the calculated child for [Drink] was selected!
        Member member = executeSingletonAxis("[Product].[All Products].CalculatedChild(\"foobar\")");
        Assert.assertEquals(member, null);
    }
    
    public void testCalculatedChildOnMemberWithNoChildren() {
        Member member = executeSingletonAxis("[Measures].[Store Sales].CalculatedChild(\"foobar\")");
        Assert.assertEquals(member, null);
    }
    
    public void testCalculatedChildOnNullMember() {
        Member member = executeSingletonAxis("[Measures].[Store Sales].parent.CalculatedChild(\"foobar\")");
        Assert.assertEquals(member, null);
    }

    /**
     * Tests {@link mondrian.olap.FunTable#getFunInfoList()}, but more
     * importantly, generates an HTML table of all implemented functions into
     * a file called "functions.html". You can manually include that table
     * in the <a href="http://mondrian.sourceforge.net/mdx.html">MDX
     * specification</a>.
     */
    public void testDumpFunctions() throws IOException {
        final List funInfoList = BuiltinFunTable.instance().getFunInfoList();
        final File file = new File("functions.html");
        final FileOutputStream os = new FileOutputStream(file);
        final PrintWriter pw = new PrintWriter(os);
        pw.println("<table border='1'>");
        pw.println("<tr>");
        pw.println("<th>Name</th>");
        pw.println("<th>Description</th>");
        pw.println("</tr>");
        for (int i = 0; i < funInfoList.size(); i++) {
            FunInfo funInfo = (FunInfo) funInfoList.get(i);
            pw.println("<tr>");
            pw.print("  <td valign=top>");
            printHtml(pw, funInfo.getName());
            pw.println("</td>");
            pw.print("  <td>");
            if (funInfo.getDescription() != null) {
                printHtml(pw, funInfo.getDescription());
            }
            final String[] signatures = funInfo.getSignatures();
            if (signatures != null) {
                pw.println("    <p><b>Syntax</b></p>");
                for (int j = 0; j < signatures.length; j++) {
                    String signature = signatures[j];
                    pw.print("    <code>");
                    printHtml(pw, signature);
                    pw.println("</code><br/>");
                }
            }
            pw.println("  </td>");
            pw.println("</tr>");
        }
        pw.println("</table>");
        pw.close();
    }

    private static void printHtml(PrintWriter pw, String s) {
        final String escaped = StringEscaper.htmlEscaper.escapeString(s);
        pw.print(escaped);
    }

}

// End FunctionTest.java
