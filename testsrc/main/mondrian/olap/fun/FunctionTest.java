/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2003-2005 Julian Hyde
// Copyright (C) 2005-2014 Pentaho and others
// All Rights Reserved.
*/
package mondrian.olap.fun;

import mondrian.olap.*;
import mondrian.resource.MondrianResource;
import mondrian.test.FoodMartTestCase;
import mondrian.test.TestContext;
import mondrian.udf.*;
import mondrian.util.Bug;

import junit.framework.Assert;
import junit.framework.ComparisonFailure;

import org.apache.log4j.Logger;

import org.eigenbase.xom.StringEscaper;

import java.io.*;
import java.util.*;

/**
 * <code>FunctionTest</code> tests the functions defined in
 * {@link BuiltinFunTable}.
 *
 * @author gjohnson
 */
public class FunctionTest extends FoodMartTestCase {

    private static final Logger LOGGER = Logger.getLogger(FunctionTest.class);

    public static final boolean FILTER_SNOWFLAKE = Util.deprecated(true, false);

    private static final String months =
        "[Time].[Time].[1997].[Q1].[1]\n"
        + "[Time].[Time].[1997].[Q1].[2]\n"
        + "[Time].[Time].[1997].[Q1].[3]\n"
        + "[Time].[Time].[1997].[Q2].[4]\n"
        + "[Time].[Time].[1997].[Q2].[5]\n"
        + "[Time].[Time].[1997].[Q2].[6]\n"
        + "[Time].[Time].[1997].[Q3].[7]\n"
        + "[Time].[Time].[1997].[Q3].[8]\n"
        + "[Time].[Time].[1997].[Q3].[9]\n"
        + "[Time].[Time].[1997].[Q4].[10]\n"
        + "[Time].[Time].[1997].[Q4].[11]\n"
        + "[Time].[Time].[1997].[Q4].[12]";

    private static final String quarters =
        "[Time].[Time].[1997].[Q1]\n"
        + "[Time].[Time].[1997].[Q2]\n"
        + "[Time].[Time].[1997].[Q3]\n"
        + "[Time].[Time].[1997].[Q4]";

    private static final String year1997 = "[Time].[Time].[1997]";

    private static final String hierarchized1997 =
        year1997
        + "\n"
        + "[Time].[Time].[1997].[Q1]\n"
        + "[Time].[Time].[1997].[Q1].[1]\n"
        + "[Time].[Time].[1997].[Q1].[2]\n"
        + "[Time].[Time].[1997].[Q1].[3]\n"
        + "[Time].[Time].[1997].[Q2]\n"
        + "[Time].[Time].[1997].[Q2].[4]\n"
        + "[Time].[Time].[1997].[Q2].[5]\n"
        + "[Time].[Time].[1997].[Q2].[6]\n"
        + "[Time].[Time].[1997].[Q3]\n"
        + "[Time].[Time].[1997].[Q3].[7]\n"
        + "[Time].[Time].[1997].[Q3].[8]\n"
        + "[Time].[Time].[1997].[Q3].[9]\n"
        + "[Time].[Time].[1997].[Q4]\n"
        + "[Time].[Time].[1997].[Q4].[10]\n"
        + "[Time].[Time].[1997].[Q4].[11]\n"
        + "[Time].[Time].[1997].[Q4].[12]";

    private static final String NullNumericExpr =
        " ([Measures].[Unit Sales],"
        + "   [Customers].[All Customers].[USA].[CA].[Bellflower], "
        + "   [Product].[Products].[Drink].[Alcoholic Beverages]."
        + "[Beer and Wine].[Beer].[Good].[Good Imported Beer])";

    private static final String TimeWeekly =
        "[Time].[Weekly]";

    // ~ Constructors ----------------------------------------------------------

    /**
     * Creates a FunctionTest.
     */
    public FunctionTest() {
    }

    /**
     * Creates a FuncionTest with an explicit name.
     *
     * @param s Test name
     */
    public FunctionTest(String s) {
        super(s);
    }

    // ~ Methods ---------------------------------------------------------------

    // ~ Test methods ----------------------------------------------------------

    /**
     * Tests that Integeer.MIN_VALUE(-2147483648) does not cause NPE.
     */
    public void testParallelPeriodMinValue() {
        executeQuery(
            "with "
            + "member [measures].[foo] as "
            + "'([Measures].[unit sales],"
            + "ParallelPeriod([Time].[Quarter], -2147483648))' "
            + "select "
            + "[measures].[foo] on columns, "
            + "[time].[1997].children on rows "
            + "from [sales]");
    }

    /**
     * Tests that Integeer.MIN_VALUE(-2147483648) in Lag is handled correctly.
     */
    public void testLagMinValue() {
        executeQuery(
            "with "
            + "member [measures].[foo] as "
            + "'([Measures].[unit sales], [Time].[1997].[Q1].Lag(-2147483648))' "
            + "select "
            + "[measures].[foo] on columns, "
            + "[time].[1997].children on rows "
            + "from [sales]");
    }

    /**
     * Tests that ParallelPeriod with Aggregate function works
     */
    public void testParallelPeriodWithSlicer() {
        if (!Bug.BaseStarKeyColumnFixed) {
            return;
        }
        assertQueryReturns(
            "With "
            + "Set [*NATIVE_CJ_SET] as 'NonEmptyCrossJoin([*BASE_MEMBERS_Time],[*BASE_MEMBERS_Product])' "
            + "Set [*BASE_MEMBERS_Measures] as '{[Measures].[*FORMATTED_MEASURE_0], [Measures].[*FORMATTED_MEASURE_1]}' "
            + "Set [*BASE_MEMBERS_Time] as '{[Time].[1997].[Q2].[6]}' "
            + "Set [*NATIVE_MEMBERS_Time] as 'Generate([*NATIVE_CJ_SET], {[Time].[Time].CurrentMember})' "
            + "Set [*BASE_MEMBERS_Product] as '{[Product].[Products].[Drink],[Product].[Products].[Food]}' "
            + "Set [*NATIVE_MEMBERS_Product] as 'Generate([*NATIVE_CJ_SET], {[Product].CurrentMember})' "
            + "Member [Measures].[*FORMATTED_MEASURE_0] as '[Measures].[Customer Count]', FORMAT_STRING = '#,##0', SOLVE_ORDER=400 "
            + "Member [Measures].[*FORMATTED_MEASURE_1] as '([Measures].[Customer Count], ParallelPeriod([Time].[Quarter], 1, [Time].[Time].currentMember))', FORMAT_STRING = '#,##0', SOLVE_ORDER=-200 "
            + "Member [Product].[*FILTER_MEMBER] as 'Aggregate ([*NATIVE_MEMBERS_Product])', SOLVE_ORDER=-300 "
            + "Select "
            + "[*BASE_MEMBERS_Measures] on columns, Non Empty Generate([*NATIVE_CJ_SET], {([Time].[Time].CurrentMember)}) on rows "
            + "From [Sales] "
            + "Where ([Product].[*FILTER_MEMBER])",
            "Axis #0:\n"
            + "{[Product].[Products].[*FILTER_MEMBER]}\n"
            + "Axis #1:\n"
            + "{[Measures].[*FORMATTED_MEASURE_0]}\n"
            + "{[Measures].[*FORMATTED_MEASURE_1]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[1997].[Q2].[6]}\n"
            + "Row #0: 1,314\n"
            + "Row #0: 1,447\n");
    }

    public void testParallelperiodOnLevelsString() {
        assertQueryReturns(
            "with member Measures.[Prev Unit Sales] as 'parallelperiod(Levels(\"[Time].[Month]\"))'\n"
            + "select {[Measures].[Unit Sales], Measures.[Prev Unit Sales]} ON COLUMNS,\n"
            + "[Gender].members ON ROWS\n"
            + "from [Sales]\n"
            + "where [Time].[1997].[Q2].[5]",
            "Axis #0:\n"
            + "{[Time].[Time].[1997].[Q2].[5]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Prev Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Gender].[All Gender]}\n"
            + "{[Customer].[Gender].[F]}\n"
            + "{[Customer].[Gender].[M]}\n"
            + "Row #0: 21,081\n"
            + "Row #0: 20,179\n"
            + "Row #1: 10,536\n"
            + "Row #1: 9,990\n"
            + "Row #2: 10,545\n"
            + "Row #2: 10,189\n");
    }

    public void testParallelperiodOnStrToMember() {
        assertQueryReturns(
            "with member Measures.[Prev Unit Sales] as 'parallelperiod(strToMember(\"[Time].[1997].[Q2]\"))'\n"
            + "select {[Measures].[Unit Sales], Measures.[Prev Unit Sales]} ON COLUMNS,\n"
            + "[Gender].members ON ROWS\n"
            + "from [Sales]\n"
            + "where [Time].[1997].[Q2].[5]",
            "Axis #0:\n"
            + "{[Time].[Time].[1997].[Q2].[5]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Prev Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Gender].[All Gender]}\n"
            + "{[Customer].[Gender].[F]}\n"
            + "{[Customer].[Gender].[M]}\n"
            + "Row #0: 21,081\n"
            + "Row #0: 20,957\n"
            + "Row #1: 10,536\n"
            + "Row #1: 10,266\n"
            + "Row #2: 10,545\n"
            + "Row #2: 10,691\n");

        assertQueryThrows(
            "with member Measures.[Prev Unit Sales] as 'parallelperiod"
            + "(strToMember(\"[Time].[Quarter]\"))'\n"
            + "select {[Measures].[Unit Sales], Measures.[Prev Unit Sales]} ON COLUMNS,\n"
            + "[Gender].members ON ROWS\n"
            + "from [Sales]\n"
            + "where [Time].[1997].[Q2].[5]",
            "Member '[Time].[Quarter]' not found");
    }

    public void testNumericLiteral() {
        assertExprReturns("2", "2");
        if (false) {
            // The test is currently broken because the value 2.5 is formatted
            // as "2". TODO: better default format string
            assertExprReturns("2.5", "2.5");
        }
        assertExprReturns("-10.0", "-10");
        getTestContext().assertExprDependsOn(
            "1.5", Collections.<String>emptySet());
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
        getTestContext().assertExprDependsOn(
            "\"foobar\"", Collections.<String>emptySet());
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
        getTestContext().assertExprDependsOn(
            "Dimensions(2).Name", Collections.<String>emptySet());
        getTestContext().assertMemberExprDependsOn(
            "Dimensions(3).CurrentMember",
            TestContext.allHiers());
        assertExprReturns("Dimensions(2).Name", "Store Size in SQFT");
        // bug 1426134 -- Dimensions(0) throws 'Index '0' out of bounds'
        assertExprReturns("Dimensions(0).Name", "Measures");
        assertExprThrows("Dimensions(-1).Name", "Index '-1' out of bounds");
        assertExprThrows("Dimensions(100).Name", "Index '100' out of bounds");
        // Since Dimensions returns a Hierarchy, can apply CurrentMember.
        assertAxisReturns(
            "Dimensions(3).CurrentMember",
            "[Store].[Store Type].[All Store Types]");
    }

    public void testDimensionsString() {
        getTestContext().assertExprDependsOn(
            "Dimensions(\"foo\").UniqueName",
            Collections.<String>emptySet());
        getTestContext().assertMemberExprDependsOn(
            "Dimensions(\"foo\").CurrentMember", TestContext.allHiers());
        assertExprReturns(
            "Dimensions(\"Stores\").UniqueName", "[Store].[Stores]");
        // Since Dimensions returns a Hierarchy, can apply Children.
        assertAxisReturns(
            "Dimensions(\"Stores\").Children",
            "[Store].[Stores].[Canada]\n"
            + "[Store].[Stores].[Mexico]\n"
            + "[Store].[Stores].[USA]");
    }

    public void testDimensionsDepends() {
        final String expression =
            "Crossjoin("
            + "{Dimensions(\"Measures\").CurrentMember.Hierarchy.CurrentMember}, "
            + "{Dimensions(\"Product\")})";
        assertAxisReturns(
            expression,
            "{[Measures].[Unit Sales], [Product].[Products].[All Products]}");
        getTestContext().assertSetExprDependsOn(
            expression, TestContext.allHiers());
    }

    public void testTime() {
        assertExprReturns(
            "[Time].[1997].[Q1].[1].Hierarchy.UniqueName", "[Time].[Time]");
    }

    public void testBasic9() {
        assertExprReturns(
            "[Gender].[All Gender].[F].Hierarchy.UniqueName",
            "[Customer].[Gender]");
    }

    public void testFirstInLevel9() {
        assertExprReturns(
            "[Education Level].[All Education Levels].[Bachelors Degree].Hierarchy.UniqueName",
            "[Customer].[Education Level]");
    }

    public void testHierarchyAll() {
        assertExprReturns(
            "[Gender].[All Gender].Hierarchy.UniqueName",
            "[Customer].[Gender]");
    }

    public void testNullMember() {
        // MSAS fails here, but Mondrian doesn't.
        assertExprReturns(
            "[Gender].[All Gender].Parent.Level.UniqueName",
            "[Customer].[Gender].[(All)]");

        // MSAS fails here, but Mondrian doesn't.
        assertExprReturns(
            "[Gender].[All Gender].Parent.Hierarchy.UniqueName",
            "[Customer].[Gender]");

        // MSAS fails here, but Mondrian doesn't.
        assertExprReturns(
            "[Gender].[All Gender].Parent.Dimension.UniqueName", "[Customer]");

        // MSAS succeeds too
        assertExprReturns(
            "[Gender].[All Gender].Parent.Children.Count", "0");

        if (isDefaultNullMemberRepresentation()) {
            // MSAS returns "" here.
            assertExprReturns(
                "[Gender].[All Gender].Parent.UniqueName",
                "[Customer].[Gender].[#null]");

            // MSAS returns "" here.
            assertExprReturns(
                "[Gender].[All Gender].Parent.Name", "#null");
        }
    }

    /**
     * Tests use of NULL literal to generate a null cell value.
     * Testcase is from bug 1440344.
     */
    public void testNullValue() {
        assertQueryReturns(
            "with member [Measures].[X] as 'IIF([Measures].[Store Sales]>10000,[Measures].[Store Sales],Null)'\n"
            + "select\n"
            + "{[Measures].[X]} on columns,\n"
            + "{[Product].[Product Department].members} on rows\n"
            + "from Sales",
            FILTER_SNOWFLAKE
                ? "Axis #0:\n"
                  + "{}\n"
                  + "Axis #1:\n"
                  + "{[Measures].[X]}\n"
                  + "Axis #2:\n"
                  + "{[Product].[Products].[Drink].[Alcoholic Beverages]}\n"
                  + "{[Product].[Products].[Drink].[Beverages]}\n"
                  + "{[Product].[Products].[Drink].[Dairy]}\n"
                  + "{[Product].[Products].[Food].[Baked Goods]}\n"
                  + "{[Product].[Products].[Food].[Baking Goods]}\n"
                  + "{[Product].[Products].[Food].[Breakfast Foods]}\n"
                  + "{[Product].[Products].[Food].[Canned Foods]}\n"
                  + "{[Product].[Products].[Food].[Canned Products]}\n"
                  + "{[Product].[Products].[Food].[Dairy]}\n"
                  + "{[Product].[Products].[Food].[Deli]}\n"
                  + "{[Product].[Products].[Food].[Eggs]}\n"
                  + "{[Product].[Products].[Food].[Frozen Foods]}\n"
                  + "{[Product].[Products].[Food].[Meat]}\n"
                  + "{[Product].[Products].[Food].[Produce]}\n"
                  + "{[Product].[Products].[Food].[Seafood]}\n"
                  + "{[Product].[Products].[Food].[Snack Foods]}\n"
                  + "{[Product].[Products].[Food].[Snacks]}\n"
                  + "{[Product].[Products].[Food].[Starchy Foods]}\n"
                  + "{[Product].[Products].[Non-Consumable].[Carousel]}\n"
                  + "{[Product].[Products].[Non-Consumable].[Checkout]}\n"
                  + "{[Product].[Products].[Non-Consumable].[Health and Hygiene]}\n"
                  + "{[Product].[Products].[Non-Consumable].[Household]}\n"
                  + "{[Product].[Products].[Non-Consumable].[Periodicals]}\n"
                  + "Row #0: 14,029.08\n"
                  + "Row #1: 27,748.53\n"
                  + "Row #2: \n"
                  + "Row #3: 16,455.43\n"
                  + "Row #4: 38,670.41\n"
                  + "Row #5: \n"
                  + "Row #6: 39,774.34\n"
                  + "Row #7: \n"
                  + "Row #8: 30,508.85\n"
                  + "Row #9: 25,318.93\n"
                  + "Row #10: \n"
                  + "Row #11: 55,207.50\n"
                  + "Row #12: \n"
                  + "Row #13: 82,248.42\n"
                  + "Row #14: \n"
                  + "Row #15: 67,609.82\n"
                  + "Row #16: 14,550.05\n"
                  + "Row #17: 11,756.07\n"
                  + "Row #18: \n"
                  + "Row #19: \n"
                  + "Row #20: 32,571.86\n"
                  + "Row #21: 60,469.89\n"
                  + "Row #22: \n"
                : "Axis #0:\n"
                  + "{}\n"
                  + "Axis #1:\n"
                  + "{[Measures].[X]}\n"
                  + "Axis #2:\n"
                  + "{[Product].[Products].[Drink].[Alcoholic Beverages]}\n"
                  + "{[Product].[Products].[Drink].[Baking Goods]}\n"
                  + "{[Product].[Products].[Drink].[Beverages]}\n"
                  + "{[Product].[Products].[Drink].[Dairy]}\n"
                  + "{[Product].[Products].[Food].[Baked Goods]}\n"
                  + "{[Product].[Products].[Food].[Baking Goods]}\n"
                  + "{[Product].[Products].[Food].[Breakfast Foods]}\n"
                  + "{[Product].[Products].[Food].[Canned Foods]}\n"
                  + "{[Product].[Products].[Food].[Canned Products]}\n"
                  + "{[Product].[Products].[Food].[Dairy]}\n"
                  + "{[Product].[Products].[Food].[Deli]}\n"
                  + "{[Product].[Products].[Food].[Eggs]}\n"
                  + "{[Product].[Products].[Food].[Frozen Foods]}\n"
                  + "{[Product].[Products].[Food].[Meat]}\n"
                  + "{[Product].[Products].[Food].[Packaged Foods]}\n"
                  + "{[Product].[Products].[Food].[Produce]}\n"
                  + "{[Product].[Products].[Food].[Seafood]}\n"
                  + "{[Product].[Products].[Food].[Snack Foods]}\n"
                  + "{[Product].[Products].[Food].[Snacks]}\n"
                  + "{[Product].[Products].[Food].[Starchy Foods]}\n"
                  + "{[Product].[Products].[Non-Consumable].[Carousel]}\n"
                  + "{[Product].[Products].[Non-Consumable].[Checkout]}\n"
                  + "{[Product].[Products].[Non-Consumable].[Health and Hygiene]}\n"
                  + "{[Product].[Products].[Non-Consumable].[Household]}\n"
                  + "{[Product].[Products].[Non-Consumable].[Periodicals]}\n"
                  + "Row #0: 14,029.08\n"
                  + "Row #1: \n"
                  + "Row #2: 27,748.53\n"
                  + "Row #3: \n"
                  + "Row #4: 16,455.43\n"
                  + "Row #5: 38,670.41\n"
                  + "Row #6: \n"
                  + "Row #7: 39,774.34\n"
                  + "Row #8: \n"
                  + "Row #9: 30,508.85\n"
                  + "Row #10: 25,318.93\n"
                  + "Row #11: \n"
                  + "Row #12: 55,207.50\n"
                  + "Row #13: \n"
                  + "Row #14: \n"
                  + "Row #15: 82,248.42\n"
                  + "Row #16: \n"
                  + "Row #17: 67,609.82\n"
                  + "Row #18: 14,550.05\n"
                  + "Row #19: 11,756.07\n"
                  + "Row #20: \n"
                  + "Row #21: \n"
                  + "Row #22: 32,571.86\n"
                  + "Row #23: 60,469.89\n"
                  + "Row #24: \n");
    }

    public void testNullInMultiplication() {
        assertExprReturns("NULL*1", "");
        assertExprReturns("1*NULL", "");
        assertExprReturns("NULL*NULL", "");
    }

    public void testNullInAddition() {
        assertExprReturns("1+NULL", "1");
        assertExprReturns("NULL+1", "1");
    }

    public void testNullInSubtraction() {
        assertExprReturns("1-NULL", "1");
        assertExprReturns("NULL-1", "-1");
    }

    public void testMemberLevel() {
        assertExprReturns(
            "[Time].[1997].[Q1].[1].Level.UniqueName",
            "[Time].[Time].[Month]");
    }

    public void testLevelsNumeric() {
        assertExprReturns("[Time].[Time].Levels(2).Name", "Month");
        assertExprReturns("[Time].[Time].Levels(0).Name", "Year");
        assertExprReturns("[Product].Levels(0).Name", "(All)");
    }

    public void testLevelsTooSmall() {
        assertExprThrows(
            "[Time].[Time].Levels(-1).Name", "Index '-1' out of bounds");
    }

    public void testLevelsTooLarge() {
        assertExprThrows(
            "[Time].[Time].Levels(8).Name", "Index '8' out of bounds");
    }

    public void testHierarchyLevelsString() {
        assertExprReturns(
            "[Time].[Time].Levels(\"Year\").UniqueName",
            "[Time].[Time].[Year]");
    }

    public void testHierarchyLevelsStringFail() {
        assertExprThrows(
            "[Time].[Time].Levels(\"nonexistent\").UniqueName",
            "Level 'nonexistent' not found in hierarchy '[Time].[Time]'");
    }

    public void testLevelsString() {
        assertExprReturns(
            "Levels(\"[Time].[Year]\").UniqueName",
            "[Time].[Time].[Year]");
    }

    public void testLevelsStringFail() {
        assertExprThrows(
            "Levels(\"nonexistent\").UniqueName",
            "Level 'nonexistent' not found");
    }

    public void testIsEmptyQuery() {
        String desiredResult =
            "Axis #0:\n"
            + "{[Time].[Time].[1997].[Q4].[12], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth].[Portsmouth Imported Beer], [Measures].[Foo]}\n"
            + "Axis #1:\n"
            + "{[Store].[Stores].[USA].[WA].[Bellingham]}\n"
            + "{[Store].[Stores].[USA].[WA].[Bremerton]}\n"
            + "{[Store].[Stores].[USA].[WA].[Seattle]}\n"
            + "{[Store].[Stores].[USA].[WA].[Spokane]}\n"
            + "{[Store].[Stores].[USA].[WA].[Tacoma]}\n"
            + "{[Store].[Stores].[USA].[WA].[Walla Walla]}\n"
            + "{[Store].[Stores].[USA].[WA].[Yakima]}\n"
            + "Row #0: 5\n"
            + "Row #0: 5\n"
            + "Row #0: 2\n"
            + "Row #0: 5\n"
            + "Row #0: 11\n"
            + "Row #0: 5\n"
            + "Row #0: 4\n";

        assertQueryReturns(
            "WITH MEMBER [Measures].[Foo] AS 'Iif(IsEmpty([Measures].[Unit Sales]), 5, [Measures].[Unit Sales])'\n"
            + "SELECT {[Store].[USA].[WA].children} on columns\n"
            + "FROM Sales\n"
            + "WHERE ([Time].[1997].[Q4].[12],\n"
            + " [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth].[Portsmouth Imported Beer],\n"
            + " [Measures].[Foo])",
            desiredResult);

        assertQueryReturns(
            "WITH MEMBER [Measures].[Foo] AS 'Iif([Measures].[Unit Sales] IS EMPTY, 5, [Measures].[Unit Sales])'\n"
            + "SELECT {[Store].[USA].[WA].children} on columns\n"
            + "FROM Sales\n"
            + "WHERE ([Time].[1997].[Q4].[12],\n"
            + " [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and "
            + "Wine].[Beer].[Portsmouth].[Portsmouth Imported Beer],\n"
            + " [Measures].[Foo])",
            desiredResult);

        assertQueryReturns(
            "WITH MEMBER [Measures].[Foo] AS 'Iif([Measures].[Bar] IS EMPTY, "
            + "1, [Measures].[Bar])'\n"
            + "MEMBER [Measures].[Bar] AS 'CAST(\"42\" AS INTEGER)'\n"
            + "SELECT {[Measures].[Unit Sales], [Measures].[Foo]} on columns\n"
            + "FROM Sales\n"
            + "WHERE ([Time].[1998].[Q4].[12])",
            "Axis #0:\n"
            + "{[Time].[Time].[1998].[Q4].[12]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Foo]}\n"
            + "Row #0: \n"
            + "Row #0: 42\n");
    }

    public void testIsEmptyWithAggregate() {
        assertQueryReturns(
            "WITH MEMBER [gender].[foo] AS 'isEmpty(Aggregate({[Gender].m}))' "
            + "SELECT {Gender.foo} on 0 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Customer].[Gender].[foo]}\n"
            + "Row #0: false\n");
    }

    public void testIsEmpty()
    {
        assertBooleanExprReturns("[Gender].[All Gender].Parent IS NULL", true);

        // Any functions that return a member from parameters that
        // include a member and that member is NULL also give a NULL.
        // Not a runtime exception.
        assertBooleanExprReturns(
            "[Gender].CurrentMember.Parent.NextMember IS NULL",
            true);

        if (!Bug.BugMondrian207Fixed) {
            return;
        }

        // When resolving a tuple's value in the cube, if there is
        // at least one NULL member in the tuple should return a
        // NULL cell value.
        assertBooleanExprReturns(
            "IsEmpty(([Time].currentMember.Parent, [Measures].[Unit Sales]))",
            false);
        assertBooleanExprReturns(
            "IsEmpty(([Time].currentMember, [Measures].[Unit Sales]))",
            false);

        // EMPTY refers to a genuine cell value that exists in the cube space,
        // and has no NULL members in the tuple,
        // but has no fact data at that crossing,
        // so it evaluates to EMPTY as a cell value.
        assertBooleanExprReturns(
            "IsEmpty(\n"
            + " ([Time].[1997].[Q4].[12],\n"
            + "  [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth].[Portsmouth Imported Beer],\n"
            + "  [Store].[All Stores].[USA].[WA].[Bellingham]))", true);
        assertBooleanExprReturns(
            "IsEmpty(\n"
            + " ([Time].[1997].[Q4].[11],\n"
            + "  [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth].[Portsmouth Imported Beer],\n"
            + "  [Store].[All Stores].[USA].[WA].[Bellingham]))", false);

        // The empty set is neither EMPTY nor NULL.
        // should give 0 as a result, not NULL and not EMPTY.
        assertQueryReturns(
            "WITH SET [empty set] AS '{}'\n"
            + " MEMBER [Measures].[Set Size] AS 'Count([empty set])'\n"
            + " MEMBER [Measures].[Set Size Is Empty] AS 'CASE WHEN IsEmpty([Measures].[Set Size]) THEN 1 ELSE 0 END '\n"
            + "SELECT [Measures].[Set Size] on columns", "");

        assertQueryReturns(
            "WITH SET [empty set] AS '{}'\n"
            + "WITH MEMBER [Measures].[Set Size] AS 'Count([empty set])'\n"
            + "SELECT [Measures].[Set Size] on columns", "");

        // Run time errors are BAD things.  They should not occur
        // in almost all cases.  In fact there should be no
        // logically formed MDX that generates them.  An ERROR
        // value in a cell though is perfectly legal - e.g. a
        // divide by 0.
        // E.g.
        String foo =
            "WITH [Measures].[Ratio This Period to Previous] as\n"
            + "'([Measures].[Sales],[Time].CurrentMember/([Measures].[Sales],[Time].CurrentMember.PrevMember)'\n"
            + "SELECT [Measures].[Ratio This Period to Previous] ON COLUMNS,\n"
            + "[Time].Members ON ROWS\n"
            + "FROM ...";

        // For the [Time].[All Time] row as well as the first
        // year, first month etc, the PrevMember will evaluate to
        // NULL, the tuple will evaluate to NULL and the division
        // will implicitly convert the NULL to 0 and then evaluate
        // to an ERROR value due to a divide by 0.

        // This leads to another point: NULL and EMPTY values get
        // implicitly converted to 0 when treated as numeric
        // values for division and multiplication but for addition
        // and subtraction, NULL is treated as NULL (5+NULL yields
        // NULL).
        // I have no idea about how EMPTY works.  I.e. is does
        // 5+EMPTY yield 5 or EMPTY or NULL or what?
        // E.g.
        String foo2 =
            "WITH MEMBER [Measures].[5 plus empty] AS\n"
            + "'5+([Product].[Products].[Ski boots],[Geography].[All Geography].[Hawaii])'\n"
            + "SELECT [Measures].[5 plus empty] ON COLUMNS\n"
            + "FROM ...";
        // Does this yield EMPTY, 5, NULL or ERROR?

        // Lastly, IS NULL and IS EMPTY are both legal and
        // distinct.  <<Object>> IS {<<Object>> | NULL}  and
        // <<Value>> IS EMPTY.
        // E.g.
        // a)  [Time].CurrentMember.Parent IS [Time].[Year].[2004]
        // is also a perfectly legal expression and better than
        // [Time].CurrentMember.Parent.Name="2004".
        // b) ([Measures].[Sales],[Time].FirstSibling) IS EMPTY is
        // a legal expression.


        // Microsoft's site says that the EMPTY value participates in 3 value
        // logic e.g. TRUE AND EMPTY gives EMPTY, FALSE AND EMPTY gives FALSE.
        // todo: test for this
    }

    public void testQueryWithoutValidMeasure() {
        assertQueryReturns(
            "with\n"
            + "member measures.[without VM] as ' [measures].[unit sales] '\n"
            + "select {measures.[without VM] } on 0,\n"
            + "[Warehouse].[Country].members on 1 from [warehouse and sales]\n",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[without VM]}\n"
            + "Axis #2:\n"
            + "{[Warehouse].[Warehouses].[Canada]}\n"
            + "{[Warehouse].[Warehouses].[Mexico]}\n"
            + "{[Warehouse].[Warehouses].[USA]}\n"
            + "Row #0: \n"
            + "Row #1: \n"
            + "Row #2: \n");
    }

    /** Tests the <code>ValidMeasure</code> function. */
    public void testValidMeasure() {
        assertQueryReturns(
            "with\n"
            + "member measures.[with VM] as 'validmeasure([measures].[unit sales])'\n"
            + "select { measures.[with VM]} on 0,\n"
            + "[Warehouse].[Country].members on 1 from [warehouse and sales]\n",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[with VM]}\n"
            + "Axis #2:\n"
            + "{[Warehouse].[Warehouses].[Canada]}\n"
            + "{[Warehouse].[Warehouses].[Mexico]}\n"
            + "{[Warehouse].[Warehouses].[USA]}\n"
            + "Row #0: 266,773\n"
            + "Row #1: 266,773\n"
            + "Row #2: 266,773\n");
    }

    public void _testValidMeasureNonEmpty() {
        // Note that [with VM2] is NULL where it needs to be - and therefore
        // does not prevent NON EMPTY from eliminating empty rows.
        assertQueryReturns(
            "with set [Foo] as ' Crossjoin({[Time].Children}, {[Measures].[Warehouse Sales]}) '\n"
            + " member [Measures].[with VM] as 'ValidMeasure([Measures].[Unit Sales])'\n"
            + " member [Measures].[with VM2] as 'Iif(Count(Filter([Foo], not isempty([Measures].CurrentMember))) > 0, ValidMeasure([Measures].[Unit Sales]), NULL)'\n"
            + "select NON EMPTY Crossjoin({[Time].Children}, {[Measures].[with VM2], [Measures].[Warehouse Sales]}) ON COLUMNS,\n"
            + "  NON EMPTY {[Warehouse].[All Warehouses].[USA].[WA].Children} ON ROWS\n"
            + "from [Warehouse and Sales]\n"
            + "where [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Light Beer]",
            "Axis #0:\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Light Beer]}\n"
            + "Axis #1:\n"
            + "{[Time].[Time].[1997].[Q1], [Measures].[with VM2]}\n"
            + "{[Time].[Time].[1997].[Q1], [Measures].[Warehouse Sales]}\n"
            + "{[Time].[Time].[1997].[Q2], [Measures].[with VM2]}\n"
            + "{[Time].[Time].[1997].[Q2], [Measures].[Warehouse Sales]}\n"
            + "{[Time].[Time].[1997].[Q3], [Measures].[with VM2]}\n"
            + "{[Time].[Time].[1997].[Q4], [Measures].[with VM2]}\n"
            + "Axis #2:\n"
            + "{[Warehouse].[Warehouses].[USA].[WA].[Seattle]}\n"
            + "{[Warehouse].[Warehouses].[USA].[WA].[Tacoma]}\n"
            + "{[Warehouse].[Warehouses].[USA].[WA].[Yakima]}\n"
            + "Row #0: 26\n"
            + "Row #0: 34.793\n"
            + "Row #0: 25\n"
            + "Row #0: \n"
            + "Row #0: 36\n"
            + "Row #0: 28\n"
            + "Row #1: 26\n"
            + "Row #1: \n"
            + "Row #1: 25\n"
            + "Row #1: 64.615\n"
            + "Row #1: 36\n"
            + "Row #1: 28\n"
            + "Row #2: 26\n"
            + "Row #2: 79.657\n"
            + "Row #2: 25\n"
            + "Row #2: \n"
            + "Row #2: 36\n"
            + "Row #2: 28\n");
    }

    public void testValidMeasureTupleHasAnotherMember() {
        assertQueryReturns(
            "with\n"
            + "member measures.[with VM] as 'validmeasure(([measures].[unit sales],[customers].[all customers]))'\n"
            + "select { measures.[with VM]} on 0,\n"
            + "[Warehouse].[Country].members on 1 from [warehouse and sales]\n",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[with VM]}\n"
            + "Axis #2:\n"
            + "{[Warehouse].[Warehouses].[Canada]}\n"
            + "{[Warehouse].[Warehouses].[Mexico]}\n"
            + "{[Warehouse].[Warehouses].[USA]}\n"
            + "Row #0: 266,773\n"
            + "Row #1: 266,773\n"
            + "Row #2: 266,773\n");
    }

    public void testValidMeasureDepends() {
        Set<String> s12 = TestContext.allHiersExcept("[Measures]");
        getTestContext().assertExprDependsOn(
            "ValidMeasure([Measures].[Unit Sales])", s12);

        Set<String> s11 =
            TestContext.allHiersExcept(
                "[Measures]", "[Time].[Time]");
        getTestContext().assertExprDependsOn(
            "ValidMeasure(([Measures].[Unit Sales], [Time].[1997].[Q1]))", s11);

        Set<String> s1 = TestContext.allHiersExcept("[Measures]");
        getTestContext().assertExprDependsOn(
            "ValidMeasure(([Measures].[Unit Sales], "
            + "[Time].[Time].CurrentMember.Parent))",
            s1);
    }

    public void testValidMeasureNonVirtualCube() {
        // verify ValidMeasure used outside of a virtual cube
        // is effectively a no-op.
        assertQueryReturns(
            "with member measures.vm as 'ValidMeasure(measures.[Store Sales])'"
            + " select measures.[vm] on 0 from Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[vm]}\n"
            + "Row #0: 565,238.13\n");
        assertQueryReturns(
            "with member measures.vm as 'ValidMeasure((gender.f, measures.[Store Sales]))'"
            + " select measures.[vm] on 0 from Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[vm]}\n"
            + "Row #0: 280,226.21\n");
    }

    /**
     * This is a test for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-2109">MONDRIAN-2109</a>
     *
     * <p>We can't allow calculated members in ValidMeasure so a proper message
     * must be returned.
     */
    public void testValidMeasureCalculatedMemberMeasure() {
        // Check for failure.
        assertQueryThrows(
            "with member measures.calc as 'measures.[Warehouse sales]' \n"
            + "member measures.vm as 'ValidMeasure(measures.calc)' \n"
            + "select from [warehouse and sales]\n"
            + "where (measures.vm ,gender.f) \n",
            "The function ValidMeasure cannot be used with the measure '[Measures].[calc]' because it is a calculated member.");
        // Check the working version
        assertQueryReturns(
            "with \n"
            + "member measures.vm as 'ValidMeasure(measures.[warehouse sales])' \n"
            + "select from [warehouse and sales] where (measures.vm, gender.f) \n",
            "Axis #0:\n"
            + "{[Measures].[vm], [Customer].[Gender].[F]}\n"
            + "196,770.888");
    }

    public void testAncestor() {
        Member member =
            executeSingletonAxis(
                "Ancestor([Store].[USA].[CA].[Los Angeles],[Store Country])");
        assertEquals("USA", member.getName());

        assertAxisThrows(
            "Ancestor([Store].[USA].[CA].[Los Angeles],[Promotions].[Promotion Name])",
            "Error while executing query");
    }

    public void testAncestorNumeric() {
        Member member =
            executeSingletonAxis(
                "Ancestor([Store].[USA].[CA].[Los Angeles],1)");
        assertEquals("CA", member.getName());

        member =
            executeSingletonAxis(
                "Ancestor([Store].[USA].[CA].[Los Angeles], 0)");
        assertEquals("Los Angeles", member.getName());
    }

    public void testAncestorNumericRagged() {
        if (!Bug.CubeRaggedFeature) {
            return;
        }
        final TestContext testContextRagged =
            getTestContext().legacy().withCube("[Sales Ragged]");
        Member member =
            testContextRagged.executeSingletonAxis(
                "Ancestor([Store].[All Stores].[Vatican], 1)");
        assertEquals("All Stores", member.getName());

        member =
            testContextRagged.executeSingletonAxis(
                "Ancestor([Store].[USA].[Washington], 1)");
        assertEquals("USA", member.getName());

        // complicated way to say "1".
        member =
            testContextRagged.executeSingletonAxis(
                "Ancestor([Store].[USA].[Washington], 7 * 6 - 41)");
        assertEquals("USA", member.getName());

        member =
            testContextRagged.executeSingletonAxis(
                "Ancestor([Store].[All Stores].[Vatican], 2)");
        Assert.assertNull("Ancestor at 2 must be null", member);

        member =
            testContextRagged.executeSingletonAxis(
                "Ancestor([Store].[All Stores].[Vatican], -5)");
        Assert.assertNull("Ancestor at -5 must be null", member);
    }

    public void testAncestorHigher() {
        Member member =
            executeSingletonAxis(
                "Ancestor([Store].[USA],[Store].[Store City])");
        Assert.assertNull(member); // MSOLAP returns null
    }

    public void testAncestorSameLevel() {
        Member member =
            executeSingletonAxis(
                "Ancestor([Store].[Canada],[Store].[Store Country])");
        assertEquals("Canada", member.getName());
    }

    public void testAncestorWrongHierarchy() {
        // MSOLAP gives error "Formula error - dimensions are not
        // valid (they do not match) - in the Ancestor function"
        assertAxisThrows(
            "Ancestor([Gender].[M],[Store].[Store Country])",
            "Error while executing query");
    }

    public void testAncestorAllLevel() {
        Member member =
            executeSingletonAxis(
                "Ancestor([Store].[USA].[CA],[Stores].Levels(0))");
        Assert.assertTrue(member.isAll());
    }

    public void testAncestorWithHiddenParent() {
        if (!Bug.CubeRaggedFeature) {
            return;
        }
        final TestContext testContext =
            getTestContext().legacy().withCube("[Sales Ragged]");
        Member member =
            testContext.executeSingletonAxis(
                "Ancestor([Store].[All Stores].[Israel].[Haifa], "
                + "[Store].[Store Country])");

        assertNotNull("Member must not be null.", member);
        assertEquals("Israel", member.getName());
    }

    public void testAncestorDepends() {
        getTestContext().assertExprDependsOn(
            "Ancestor([Stores].CurrentMember, [Store].[Store Country]).Name",
            setOf("[Store].[Stores]"));

        getTestContext().assertExprDependsOn(
            "Ancestor([Store].[All Stores].[USA], "
            + "[Stores].CURRENTMEMBER.Level).Name",
            setOf("[Store].[Stores]"));

        getTestContext().assertExprDependsOn(
            "Ancestor([Store].[All Stores].[USA], "
            + "[Store].[Store Country]).Name",
            Collections.<String>emptySet());

        getTestContext().assertExprDependsOn(
            "Ancestor([Stores].CURRENTMEMBER, 2+1).Name",
            setOf("[Store].[Stores]"));
    }

    private Set<String> setOf(String... strings) {
        return new LinkedHashSet<String>(Arrays.asList(strings));
    }

    public void testOrdinal() {
        if (!Bug.CubeRaggedFeature) {
            return;
        }
        final TestContext testContext =
            getTestContext().withSalesRagged();

        Cell cell =
            testContext.executeExprRaw(
                "[Store].[All Stores].[Vatican].ordinal");
        assertEquals(
            "Vatican is at level 1.",
            1,
            ((Number)cell.getValue()).intValue());

        cell = testContext.executeExprRaw(
            "[Store].[All Stores].[USA].[Washington].ordinal");
        assertEquals(
            "Washington is at level 3.",
            3,
            ((Number) cell.getValue()).intValue());
    }

    public void testClosingPeriodNoArgs() {
        getTestContext().assertMemberExprDependsOn(
            "ClosingPeriod()", setOf("[Time].[Time]"));
        // MSOLAP returns [1997].[Q4], because [Time].CurrentMember =
        // [1997].
        Member member = executeSingletonAxis("ClosingPeriod()");
        assertEquals("[Time].[Time].[1997].[Q4]", member.getUniqueName());
    }

    public void testClosingPeriodLevel() {
        getTestContext().assertMemberExprDependsOn(
            "ClosingPeriod([Time].[Year])", setOf("[Time].[Time]"));
        getTestContext().assertMemberExprDependsOn(
            "([Measures].[Unit Sales], ClosingPeriod([Time].[Month]))",
            setOf("[Time].[Time]"));

        Member member;

        member = executeSingletonAxis("ClosingPeriod([Year])");
        assertEquals("[Time].[Time].[1997]", member.getUniqueName());

        member = executeSingletonAxis("ClosingPeriod([Quarter])");
        assertEquals("[Time].[Time].[1997].[Q4]", member.getUniqueName());

        member = executeSingletonAxis("ClosingPeriod([Month])");
        assertEquals("[Time].[Time].[1997].[Q4].[12]", member.getUniqueName());

        assertQueryReturns(
            "with member [Measures].[Closing Unit Sales] as "
            + "'([Measures].[Unit Sales], ClosingPeriod([Time].[Month]))'\n"
            + "select non empty {[Measures].[Closing Unit Sales]} on columns,\n"
            + " {Descendants([Time].[1997])} on rows\n"
            + "from [Sales]",

            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Closing Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[1997]}\n"
            + "{[Time].[Time].[1997].[Q1]}\n"
            + "{[Time].[Time].[1997].[Q1].[1]}\n"
            + "{[Time].[Time].[1997].[Q1].[2]}\n"
            + "{[Time].[Time].[1997].[Q1].[3]}\n"
            + "{[Time].[Time].[1997].[Q2]}\n"
            + "{[Time].[Time].[1997].[Q2].[4]}\n"
            + "{[Time].[Time].[1997].[Q2].[5]}\n"
            + "{[Time].[Time].[1997].[Q2].[6]}\n"
            + "{[Time].[Time].[1997].[Q3]}\n"
            + "{[Time].[Time].[1997].[Q3].[7]}\n"
            + "{[Time].[Time].[1997].[Q3].[8]}\n"
            + "{[Time].[Time].[1997].[Q3].[9]}\n"
            + "{[Time].[Time].[1997].[Q4]}\n"
            + "{[Time].[Time].[1997].[Q4].[10]}\n"
            + "{[Time].[Time].[1997].[Q4].[11]}\n"
            + "{[Time].[Time].[1997].[Q4].[12]}\n"
            + "Row #0: 26,796\n"
            + "Row #1: 23,706\n"
            + "Row #2: 21,628\n"
            + "Row #3: 20,957\n"
            + "Row #4: 23,706\n"
            + "Row #5: 21,350\n"
            + "Row #6: 20,179\n"
            + "Row #7: 21,081\n"
            + "Row #8: 21,350\n"
            + "Row #9: 20,388\n"
            + "Row #10: 23,763\n"
            + "Row #11: 21,697\n"
            + "Row #12: 20,388\n"
            + "Row #13: 26,796\n"
            + "Row #14: 19,958\n"
            + "Row #15: 25,270\n"
            + "Row #16: 26,796\n");

        assertQueryReturns(
            "with member [Measures].[Closing Unit Sales] as '([Measures].[Unit Sales], ClosingPeriod([Time].[Month]))'\n"
            + "select {[Measures].[Unit Sales], [Measures].[Closing Unit Sales]} on columns,\n"
            + " {[Time].[1997], [Time].[1997].[Q1], [Time].[1997].[Q1].[1], [Time].[1997].[Q1].[3], [Time].[1997].[Q4].[12]} on rows\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Closing Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[1997]}\n"
            + "{[Time].[Time].[1997].[Q1]}\n"
            + "{[Time].[Time].[1997].[Q1].[1]}\n"
            + "{[Time].[Time].[1997].[Q1].[3]}\n"
            + "{[Time].[Time].[1997].[Q4].[12]}\n"
            + "Row #0: 266,773\n"
            + "Row #0: 26,796\n"
            + "Row #1: 66,291\n"
            + "Row #1: 23,706\n"
            + "Row #2: 21,628\n"
            + "Row #2: 21,628\n"
            + "Row #3: 23,706\n"
            + "Row #3: 23,706\n"
            + "Row #4: 26,796\n"
            + "Row #4: 26,796\n");
    }

    public void testClosingPeriodLevelNotInTimeFails() {
        assertAxisThrows(
            "ClosingPeriod([Store].[Store City])",
            "The <level> and <member> arguments to ClosingPeriod must be from "
            + "the same hierarchy. The level was from '[Store].[Stores]' but "
            + "the member was from '[Time].[Time]'");
    }

    public void testClosingPeriodMember() {
        if (false) {
            // This test is mistaken. Valid forms are ClosingPeriod(<level>)
            // and ClosingPeriod(<level>, <member>), but not
            // ClosingPeriod(<member>)
            Member member = executeSingletonAxis("ClosingPeriod([USA])");
            assertEquals("WA", member.getName());
        }
    }

    public void testClosingPeriodMemberLeaf() {
        Member member;
        if (false) {
            // This test is mistaken. Valid forms are ClosingPeriod(<level>)
            // and ClosingPeriod(<level>, <member>), but not
            // ClosingPeriod(<member>)
            member = executeSingletonAxis(
                "ClosingPeriod([Time].[1997].[Q3].[8])");
            Assert.assertNull(member);
        } else if (isDefaultNullMemberRepresentation()) {
            assertQueryReturns(
                "with member [Measures].[Foo] as ClosingPeriod().uniquename\n"
                + "select {[Measures].[Foo]} on columns,\n"
                + "  {[Time].[1997],\n"
                + "   [Time].[1997].[Q2],\n"
                + "   [Time].[1997].[Q2].[4]} on rows\n"
                + "from Sales",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Measures].[Foo]}\n"
                + "Axis #2:\n"
                + "{[Time].[Time].[1997]}\n"
                + "{[Time].[Time].[1997].[Q2]}\n"
                + "{[Time].[Time].[1997].[Q2].[4]}\n"
                + "Row #0: [Time].[Time].[1997].[Q4]\n"
                + "Row #1: [Time].[Time].[1997].[Q2].[6]\n"
                + "Row #2: [Time].[Time].[#null]\n"
                // MSAS returns "" here.
                + "");
        }
    }

    public void testClosingPeriod() {
        getTestContext().assertMemberExprDependsOn(
            "ClosingPeriod([Time].[Month], [Time].[Time].CurrentMember)",
            setOf("[Time].[Time]"));

        Set<String> s1 = TestContext.allHiersExcept("[Measures]");
        getTestContext().assertExprDependsOn(
            "(([Measures].[Store Sales],"
            + " ClosingPeriod([Time].[Month], [Time].[Time].CurrentMember)) - "
            + "([Measures].[Store Cost],"
            + " ClosingPeriod([Time].[Month], [Time].[Time].CurrentMember)))",
            s1);

        getTestContext().assertMemberExprDependsOn(
            "ClosingPeriod([Time].[Month], [Time].[1997].[Q3])",
            Collections.<String>emptySet());

        assertAxisReturns(
            "ClosingPeriod([Time].[Year], [Time].[1997].[Q3])", "");

        assertAxisReturns(
            "ClosingPeriod([Time].[Quarter], [Time].[1997].[Q3])",
            "[Time].[Time].[1997].[Q3]");

        assertAxisReturns(
            "ClosingPeriod([Time].[Month], [Time].[1997].[Q3])",
            "[Time].[Time].[1997].[Q3].[9]");

        assertAxisReturns(
            "ClosingPeriod([Time].[Quarter], [Time].[1997])",
            "[Time].[Time].[1997].[Q4]");

        assertAxisReturns(
            "ClosingPeriod([Time].[Year], [Time].[1997])",
            "[Time].[Time].[1997]");

        assertAxisReturns(
            "ClosingPeriod([Time].[Month], [Time].[1997])",
            "[Time].[Time].[1997].[Q4].[12]");

        // leaf member

        assertAxisReturns(
            "ClosingPeriod([Time].[Year], [Time].[1997].[Q3].[8])", "");

        assertAxisReturns(
            "ClosingPeriod([Time].[Quarter], [Time].[1997].[Q3].[8])", "");

        assertAxisReturns(
            "ClosingPeriod([Time].[Month], [Time].[1997].[Q3].[8])",
            "[Time].[Time].[1997].[Q3].[8]");

        // non-Time dimension

        assertAxisReturns(
            "ClosingPeriod([Product].[Product Name], [Product].[Products].[Drink])",
            "[Product].[Products].[Drink].[Dairy].[Dairy].[Milk].[Gorilla].[Gorilla Whole Milk]");

        assertAxisReturns(
            "ClosingPeriod([Product].[Product Family], [Product].[Products].[Drink])",
            "[Product].[Products].[Drink]");

        // 'all' level

        assertAxisReturns(
            "ClosingPeriod([Product].[(All)], [Product].[Products].[Drink])",
            "");

        // Default member is [Time].[1997].
        assertAxisReturns(
            "ClosingPeriod([Time].[Month])", "[Time].[Time].[1997].[Q4].[12]");

        assertAxisReturns("ClosingPeriod()", "[Time].[Time].[1997].[Q4]");
    }

    public void testClosingPeriodRagged() {
        if (!Bug.CubeRaggedFeature) {
            return;
        }

        TestContext testContext =
            getTestContext().legacy().withCube("[Sales Ragged]");
        testContext.assertAxisReturns(
            "ClosingPeriod([Store].[Store City], [Store].[All Stores].[Israel])",
            "[Store].[Store].[Israel].[Israel].[Tel Aviv]");

        testContext.assertAxisReturns(
            "ClosingPeriod([Store].[Store State], [Store].[All Stores].[Israel])",
            "");

        testContext.assertAxisThrows(
            "ClosingPeriod([Time].[Year], [Store].[All Stores].[Israel])",
            "The <level> and <member> arguments to ClosingPeriod must be "
            + "from the same hierarchy. The level was from '[Time].[Time]' but "
            + "the member was from '[Store].[Store]'.");
    }

    public void testClosingPeriodBelow() {
        Member member = executeSingletonAxis(
            "ClosingPeriod([Quarter],[1997].[Q3].[8])");
        Assert.assertNull(member);
    }


    public void testCousin1() {
        Member member = executeSingletonAxis("Cousin([1997].[Q4],[1998])");
        assertEquals("[Time].[Time].[1998].[Q4]", member.getUniqueName());
    }

    public void testCousin2() {
        Member member = executeSingletonAxis(
            "Cousin([1997].[Q4].[12],[1998].[Q1])");
        assertEquals("[Time].[Time].[1998].[Q1].[3]", member.getUniqueName());
    }

    public void testCousinOverrun() {
        Member member = executeSingletonAxis(
            "Cousin([Customers].[USA].[CA].[San Jose],"
            + " [Customers].[USA].[OR])");
        // CA has more cities than OR
        Assert.assertNull(member);
    }

    public void testCousinThreeDown() {
        Member member =
            executeSingletonAxis(
                "Cousin([Customers].[USA].[CA].[Berkeley].[Barbara Combs],"
                + " [Customers].[Mexico])");
        // Barbara Combs is the 6th child
        // of the 4th child (Berkeley)
        // of the 1st child (CA)
        // of USA
        // Annmarie Hill is the 6th child
        // of the 4th child (Tixapan)
        // of the 1st child (DF)
        // of Mexico
        assertEquals(
            "[Customer].[Customers].[Mexico].[DF].[Tixapan].[Annmarie Hill]",
            member.getUniqueName());
    }

    public void testCousinSameLevel() {
        Member member =
            executeSingletonAxis("Cousin([Gender].[M], [Gender].[F])");
        assertEquals("F", member.getName());
    }

    public void testCousinHigherLevel() {
        Member member =
            executeSingletonAxis("Cousin([Time].[1997], [Time].[1998].[Q1])");
        Assert.assertNull(member);
    }

    public void testCousinWrongHierarchy() {
        assertAxisThrows(
            "Cousin([Time].[1997], [Gender].[M])",
            MondrianResource.instance().CousinHierarchyMismatch.str(
                "[Time].[Time].[1997]",
                "[Customer].[Gender].[M]"));
    }

    public void testParent() {
        getTestContext().assertMemberExprDependsOn(
            "[Gender].Parent",
            setOf("[Customer].[Gender]"));
        getTestContext().assertMemberExprDependsOn(
            "[Gender].[M].Parent",
            Collections.<String>emptySet());
        assertAxisReturns(
            "{[Store].[USA].[CA].Parent}", "[Store].[Stores].[USA]");
        // root member has null parent
        assertAxisReturns("{[Store].[All Stores].Parent}", "");
        // parent of null member is null
        assertAxisReturns("{[Store].[All Stores].Parent.Parent}", "");
    }

    public void testParentPC() {
        final TestContext testContext = getTestContext().withCube("HR");
        testContext.assertAxisReturns(
            "[Employees].Parent",
            "");
        testContext.assertAxisReturns(
            "[Employees].[Sheri Nowmer].Parent",
            "[Employee].[Employees].[All Employees]");
        testContext.assertAxisReturns(
            "[Employees].[Sheri Nowmer].[Derrick Whelply].Parent",
            "[Employee].[Employees].[Sheri Nowmer]");
        testContext.assertAxisReturns(
            "[Employees].Members.Item(3)",
            "[Employee].[Employees].[Sheri Nowmer].[Derrick Whelply].[Beverly Baker]");
        testContext.assertAxisReturns(
            "[Employees].Members.Item(3).Parent",
            "[Employee].[Employees].[Sheri Nowmer].[Derrick Whelply]");
        testContext.assertAxisReturns(
            "[Employees].AllMembers.Item(3).Parent",
            "[Employee].[Employees].[Sheri Nowmer].[Derrick Whelply]");

        // Ascendants(<Member>) applied to parent-child hierarchy accessed via
        // <Level>.Members
        testContext.assertAxisReturns(
            "Ascendants([Employees].Members.Item(73))",
            "[Employee].[Employees].[Sheri Nowmer].[Derrick Whelply].[Beverly Baker].[Jacqueline Wyllie].[Ralph Mccoy].[Bertha Jameson].[James Bailey]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Derrick Whelply].[Beverly Baker].[Jacqueline Wyllie].[Ralph Mccoy].[Bertha Jameson]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Derrick Whelply].[Beverly Baker].[Jacqueline Wyllie].[Ralph Mccoy]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Derrick Whelply].[Beverly Baker].[Jacqueline Wyllie]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Derrick Whelply].[Beverly Baker]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Derrick Whelply]\n"
            + "[Employee].[Employees].[Sheri Nowmer]\n"
            + "[Employee].[Employees].[All Employees]");
    }

    public void testMembers() {
        // <Level>.members
        assertAxisReturns(
            "{[Customers].[Country].Members}",
            "[Customer].[Customers].[Canada]\n"
            + "[Customer].[Customers].[Mexico]\n"
            + "[Customer].[Customers].[USA]");

        // <Level>.members applied to 'all' level
        assertAxisReturns(
            "{[Customers].[(All)].Members}",
            "[Customer].[Customers].[All Customers]");

        // <Level>.members applied to measures dimension
        // Note -- no cube-level calculated members are present
        assertAxisReturns(
            "{[Measures].[MeasuresLevel].Members}",
            "[Measures].[Unit Sales]\n"
            + "[Measures].[Store Cost]\n"
            + "[Measures].[Store Sales]\n"
            + "[Measures].[Sales Count]\n"
            + "[Measures].[Customer Count]\n"
            + "[Measures].[Promotion Sales]");

        // <Dimension>.members applied to Measures
        assertAxisReturns(
            "{[Measures].Members}",
            "[Measures].[Unit Sales]\n"
            + "[Measures].[Store Cost]\n"
            + "[Measures].[Store Sales]\n"
            + "[Measures].[Sales Count]\n"
            + "[Measures].[Customer Count]\n"
            + "[Measures].[Promotion Sales]");

        // <Dimension>.members applied to a query with calc measures
        // Again, no calc measures are returned
        switch (TestContext.instance().getDialect().getDatabaseProduct()) {
        case INFOBRIGHT:
            // Skip this test on Infobright, because [Promotion Sales] is
            // defined wrong.
            break;
        default:
            assertQueryReturns(
                "with member [Measures].[Xxx] AS ' [Measures].[Unit Sales] '"
                + "select {[Measures].members} on columns from [Sales]",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Measures].[Unit Sales]}\n"
                + "{[Measures].[Store Cost]}\n"
                + "{[Measures].[Store Sales]}\n"
                + "{[Measures].[Sales Count]}\n"
                + "{[Measures].[Customer Count]}\n"
                + "{[Measures].[Promotion Sales]}\n"
                + "Row #0: 266,773\n"
                + "Row #0: 225,627.23\n"
                + "Row #0: 565,238.13\n"
                + "Row #0: 86,837\n"
                + "Row #0: 5,581\n"
                + "Row #0: 151,211.21\n");
        }

        // <Level>.members applied to a query with calc measures
        // Again, no calc measures are returned
        switch (TestContext.instance().getDialect().getDatabaseProduct()) {
        case INFOBRIGHT:
            // Skip this test on Infobright, because [Promotion Sales] is
            // defined wrong.
            break;
        default:
            assertQueryReturns(
                "with member [Measures].[Xxx] AS ' [Measures].[Unit Sales] '"
                + "select {[Measures].[Measures].members} on columns from [Sales]",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Measures].[Unit Sales]}\n"
                + "{[Measures].[Store Cost]}\n"
                + "{[Measures].[Store Sales]}\n"
                + "{[Measures].[Sales Count]}\n"
                + "{[Measures].[Customer Count]}\n"
                + "{[Measures].[Promotion Sales]}\n"
                + "Row #0: 266,773\n"
                + "Row #0: 225,627.23\n"
                + "Row #0: 565,238.13\n"
                + "Row #0: 86,837\n"
                + "Row #0: 5,581\n"
                + "Row #0: 151,211.21\n");
        }
    }

    public void testHierarchyMembers() {
        assertAxisReturns(
            "Head({[Time.Weekly].Members}, 10)",
            "[Time].[Weekly].[All Weeklys]\n"
            + "[Time].[Weekly].[1997]\n"
            + "[Time].[Weekly].[1997].[1]\n"
            + "[Time].[Weekly].[1997].[1].[15]\n"
            + "[Time].[Weekly].[1997].[1].[16]\n"
            + "[Time].[Weekly].[1997].[1].[17]\n"
            + "[Time].[Weekly].[1997].[1].[18]\n"
            + "[Time].[Weekly].[1997].[1].[19]\n"
            + "[Time].[Weekly].[1997].[1].[20]\n"
            + "[Time].[Weekly].[1997].[2]");
        assertAxisReturns(
            "Tail({[Time.Weekly].Members}, 5)",
            "[Time].[Weekly].[1998].[51].[3]\n"
            + "[Time].[Weekly].[1998].[51].[4]\n"
            + "[Time].[Weekly].[1998].[51].[5]\n"
            + "[Time].[Weekly].[1998].[52]\n"
            + "[Time].[Weekly].[1998].[52].[6]");
    }

    public void testAllMembers() {
        // <Level>.allmembers
        assertAxisReturns(
            "{[Customers].[Country].allmembers}",
            "[Customer].[Customers].[Canada]\n"
            + "[Customer].[Customers].[Mexico]\n"
            + "[Customer].[Customers].[USA]");

        // <Level>.allmembers applied to 'all' level
        assertAxisReturns(
            "{[Customers].[(All)].allmembers}",
            "[Customer].[Customers].[All Customers]");

        // <Level>.allmembers applied to measures dimension
        // Note -- cube-level calculated members ARE present
        getTestContext().withCube("Warehouse and Sales").assertAxisReturns(
            "{[Measures].[MeasuresLevel].allmembers}",
            "[Measures].[Unit Sales]\n"
            + "[Measures].[Store Cost]\n"
            + "[Measures].[Store Sales]\n"
            + "[Measures].[Sales Count]\n"
            + "[Measures].[Customer Count]\n"
            + "[Measures].[Promotion Sales]\n"
            + "[Measures].[Fact Count]\n"
            + "[Measures].[Store Invoice]\n"
            + "[Measures].[Supply Time]\n"
            + "[Measures].[Warehouse Cost]\n"
            + "[Measures].[Warehouse Sales]\n"
            + "[Measures].[Units Shipped]\n"
            + "[Measures].[Units Ordered]\n"
            + "[Measures].[Warehouse Profit]\n"
            + "[Measures].[Profit]\n"
            + "[Measures].[Profit last Period]\n"
            + "[Measures].[Profit Growth]\n"
            + "[Measures].[Average Warehouse Sale]\n"
            + "[Measures].[Profit Per Unit Shipped]");

        // <Dimension>.allmembers applied to Measures
        assertAxisReturns(
            "{[Measures].allmembers}",
            "[Measures].[Unit Sales]\n"
            + "[Measures].[Store Cost]\n"
            + "[Measures].[Store Sales]\n"
            + "[Measures].[Sales Count]\n"
            + "[Measures].[Customer Count]\n"
            + "[Measures].[Promotion Sales]\n"
            + "[Measures].[Profit]\n"
            + "[Measures].[Profit last Period]\n"
            + "[Measures].[Profit Growth]");

        // <Dimension>.allmembers applied to a query with calc measures
        // Calc measures are returned
        switch (TestContext.instance().getDialect().getDatabaseProduct()) {
        case INFOBRIGHT:
            // Skip this test on Infobright, because [Promotion Sales] is
            // defined wrong.
            break;
        default:
            assertQueryReturns(
                "with member [Measures].[Xxx] AS ' [Measures].[Unit Sales] '"
                + "select {[Measures].allmembers} on columns from [Sales]",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Measures].[Unit Sales]}\n"
                + "{[Measures].[Store Cost]}\n"
                + "{[Measures].[Store Sales]}\n"
                + "{[Measures].[Sales Count]}\n"
                + "{[Measures].[Customer Count]}\n"
                + "{[Measures].[Promotion Sales]}\n"
                + "{[Measures].[Profit]}\n"
                + "{[Measures].[Profit last Period]}\n"
                + "{[Measures].[Profit Growth]}\n"
                + "{[Measures].[Xxx]}\n"
                + "Row #0: 266,773\n"
                + "Row #0: 225,627.23\n"
                + "Row #0: 565,238.13\n"
                + "Row #0: 86,837\n"
                + "Row #0: 5,581\n"
                + "Row #0: 151,211.21\n"
                + "Row #0: $339,610.90\n"
                + "Row #0: $339,610.90\n"
                + "Row #0: 0.0%\n"
                + "Row #0: 266,773\n");
        }

        // Calc measure members from schema and from query
        switch (TestContext.instance().getDialect().getDatabaseProduct()) {
        case INFOBRIGHT:
            // Skip this test on Infobright, because [Promotion Sales] is
            // defined wrong.
            break;
        default:
            assertQueryReturns(
                "WITH MEMBER [Measures].[Unit to Sales ratio] as\n"
                + " '[Measures].[Unit Sales] / [Measures].[Store Sales]', FORMAT_STRING='0.0%' "
                + "SELECT {[Measures].AllMembers} ON COLUMNS,"
                + "non empty({[Store].[Store State].Members}) ON ROWS "
                + "FROM Sales "
                + "WHERE ([1997].[Q1])",
                "Axis #0:\n"
                + "{[Time].[Time].[1997].[Q1]}\n"
                + "Axis #1:\n"
                + "{[Measures].[Unit Sales]}\n"
                + "{[Measures].[Store Cost]}\n"
                + "{[Measures].[Store Sales]}\n"
                + "{[Measures].[Sales Count]}\n"
                + "{[Measures].[Customer Count]}\n"
                + "{[Measures].[Promotion Sales]}\n"
                + "{[Measures].[Profit]}\n"
                + "{[Measures].[Profit last Period]}\n"
                + "{[Measures].[Profit Growth]}\n"
                + "{[Measures].[Unit to Sales ratio]}\n"
                + "Axis #2:\n"
                + "{[Store].[Stores].[USA].[CA]}\n"
                + "{[Store].[Stores].[USA].[OR]}\n"
                + "{[Store].[Stores].[USA].[WA]}\n"
                + "Row #0: 16,890\n"
                + "Row #0: 14,431.09\n"
                + "Row #0: 36,175.20\n"
                + "Row #0: 5,498\n"
                + "Row #0: 1,110\n"
                + "Row #0: 14,447.16\n"
                + "Row #0: $21,744.11\n"
                + "Row #0: $21,744.11\n"
                + "Row #0: 0.0%\n"
                + "Row #0: 46.7%\n"
                + "Row #1: 19,287\n"
                + "Row #1: 16,081.07\n"
                + "Row #1: 40,170.29\n"
                + "Row #1: 6,184\n"
                + "Row #1: 767\n"
                + "Row #1: 10,829.64\n"
                + "Row #1: $24,089.22\n"
                + "Row #1: $24,089.22\n"
                + "Row #1: 0.0%\n"
                + "Row #1: 48.0%\n"
                + "Row #2: 30,114\n"
                + "Row #2: 25,240.08\n"
                + "Row #2: 63,282.86\n"
                + "Row #2: 9,906\n"
                + "Row #2: 1,104\n"
                + "Row #2: 18,459.60\n"
                + "Row #2: $38,042.78\n"
                + "Row #2: $38,042.78\n"
                + "Row #2: 0.0%\n"
                + "Row #2: 47.6%\n");
        }

        // Calc member in query and schema not seen
        switch (TestContext.instance().getDialect().getDatabaseProduct()) {
        case INFOBRIGHT:
            // Skip this test on Infobright, because [Promotion Sales] is
            // defined wrong.
            break;
        default:
            assertQueryReturns(
                "WITH MEMBER [Measures].[Unit to Sales ratio] as '[Measures].[Unit Sales] / [Measures].[Store Sales]', FORMAT_STRING='0.0%' "
                + "SELECT {[Measures].AllMembers} ON COLUMNS,"
                + "non empty({[Store].[Store State].Members}) ON ROWS "
                + "FROM Sales "
                + "WHERE ([1997].[Q1])",
                "Axis #0:\n"
                + "{[Time].[Time].[1997].[Q1]}\n"
                + "Axis #1:\n"
                + "{[Measures].[Unit Sales]}\n"
                + "{[Measures].[Store Cost]}\n"
                + "{[Measures].[Store Sales]}\n"
                + "{[Measures].[Sales Count]}\n"
                + "{[Measures].[Customer Count]}\n"
                + "{[Measures].[Promotion Sales]}\n"
                + "{[Measures].[Profit]}\n"
                + "{[Measures].[Profit last Period]}\n"
                + "{[Measures].[Profit Growth]}\n"
                + "{[Measures].[Unit to Sales ratio]}\n"
                + "Axis #2:\n"
                + "{[Store].[Stores].[USA].[CA]}\n"
                + "{[Store].[Stores].[USA].[OR]}\n"
                + "{[Store].[Stores].[USA].[WA]}\n"
                + "Row #0: 16,890\n"
                + "Row #0: 14,431.09\n"
                + "Row #0: 36,175.20\n"
                + "Row #0: 5,498\n"
                + "Row #0: 1,110\n"
                + "Row #0: 14,447.16\n"
                + "Row #0: $21,744.11\n"
                + "Row #0: $21,744.11\n"
                + "Row #0: 0.0%\n"
                + "Row #0: 46.7%\n"
                + "Row #1: 19,287\n"
                + "Row #1: 16,081.07\n"
                + "Row #1: 40,170.29\n"
                + "Row #1: 6,184\n"
                + "Row #1: 767\n"
                + "Row #1: 10,829.64\n"
                + "Row #1: $24,089.22\n"
                + "Row #1: $24,089.22\n"
                + "Row #1: 0.0%\n"
                + "Row #1: 48.0%\n"
                + "Row #2: 30,114\n"
                + "Row #2: 25,240.08\n"
                + "Row #2: 63,282.86\n"
                + "Row #2: 9,906\n"
                + "Row #2: 1,104\n"
                + "Row #2: 18,459.60\n"
                + "Row #2: $38,042.78\n"
                + "Row #2: $38,042.78\n"
                + "Row #2: 0.0%\n"
                + "Row #2: 47.6%\n");
        }

        // Calc member in query and schema not seen
        switch (TestContext.instance().getDialect().getDatabaseProduct()) {
        case INFOBRIGHT:
            // Skip this test on Infobright, because [Promotion Sales] is
            // defined wrong.
            break;
        default:
            assertQueryReturns(
                "WITH MEMBER [Measures].[Unit to Sales ratio] as '[Measures].[Unit Sales] / [Measures].[Store Sales]', FORMAT_STRING='0.0%' "
                + "SELECT {[Measures].Members} ON COLUMNS,"
                + "non empty({[Store].[Store State].Members}) ON ROWS "
                + "FROM Sales "
                + "WHERE ([1997].[Q1])",
                "Axis #0:\n"
                + "{[Time].[Time].[1997].[Q1]}\n"
                + "Axis #1:\n"
                + "{[Measures].[Unit Sales]}\n"
                + "{[Measures].[Store Cost]}\n"
                + "{[Measures].[Store Sales]}\n"
                + "{[Measures].[Sales Count]}\n"
                + "{[Measures].[Customer Count]}\n"
                + "{[Measures].[Promotion Sales]}\n"
                + "Axis #2:\n"
                + "{[Store].[Stores].[USA].[CA]}\n"
                + "{[Store].[Stores].[USA].[OR]}\n"
                + "{[Store].[Stores].[USA].[WA]}\n"
                + "Row #0: 16,890\n"
                + "Row #0: 14,431.09\n"
                + "Row #0: 36,175.20\n"
                + "Row #0: 5,498\n"
                + "Row #0: 1,110\n"
                + "Row #0: 14,447.16\n"
                + "Row #1: 19,287\n"
                + "Row #1: 16,081.07\n"
                + "Row #1: 40,170.29\n"
                + "Row #1: 6,184\n"
                + "Row #1: 767\n"
                + "Row #1: 10,829.64\n"
                + "Row #2: 30,114\n"
                + "Row #2: 25,240.08\n"
                + "Row #2: 63,282.86\n"
                + "Row #2: 9,906\n"
                + "Row #2: 1,104\n"
                + "Row #2: 18,459.60\n");
        }

        // Calc member in dimension based on level
        assertQueryReturns(
            "WITH MEMBER [Store].[Stores].[USA].[CA plus OR] AS 'AGGREGATE({[Store].[USA].[CA], [Store].[USA].[OR]})' "
            + "SELECT {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,"
            + "non empty({[Store].[Store State].AllMembers}) ON ROWS "
            + "FROM Sales "
            + "WHERE ([1997].[Q1])",
            "Axis #0:\n"
            + "{[Time].[Time].[1997].[Q1]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Store Sales]}\n"
            + "Axis #2:\n"
            + "{[Store].[Stores].[USA].[CA]}\n"
            + "{[Store].[Stores].[USA].[OR]}\n"
            + "{[Store].[Stores].[USA].[WA]}\n"
            + "{[Store].[Stores].[USA].[CA plus OR]}\n"
            + "Row #0: 16,890\n"
            + "Row #0: 36,175.20\n"
            + "Row #1: 19,287\n"
            + "Row #1: 40,170.29\n"
            + "Row #2: 30,114\n"
            + "Row #2: 63,282.86\n"
            + "Row #3: 36,177\n"
            + "Row #3: 76,345.49\n");

        // Calc member in dimension based on level not seen
        assertQueryReturns(
            "WITH MEMBER [Store].[Stores].[USA].[CA plus OR] AS 'AGGREGATE({[Store].[USA].[CA], [Store].[USA].[OR]})' "
            + "SELECT {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,"
            + "non empty({[Store].[Store Country].AllMembers}) ON ROWS "
            + "FROM Sales "
            + "WHERE ([1997].[Q1])",
            "Axis #0:\n"
            + "{[Time].[Time].[1997].[Q1]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Store Sales]}\n"
            + "Axis #2:\n"
            + "{[Store].[Stores].[USA]}\n"
            + "Row #0: 66,291\n"
            + "Row #0: 139,628.35\n");
    }

    public void testAddCalculatedMembers() {
        //----------------------------------------------------
        // AddCalculatedMembers: Calc member in dimension based on level
        // included
        //----------------------------------------------------
        assertQueryReturns(
            "WITH MEMBER [Store].[Stores].[USA].[CA plus OR] AS 'AGGREGATE({[Store].[USA].[CA], [Store].[USA].[OR]})' "
            + "SELECT {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,"
            + "AddCalculatedMembers([Store].[USA].Children) ON ROWS "
            + "FROM Sales "
            + "WHERE ([1997].[Q1])",
            "Axis #0:\n"
            + "{[Time].[Time].[1997].[Q1]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Store Sales]}\n"
            + "Axis #2:\n"
            + "{[Store].[Stores].[USA].[CA]}\n"
            + "{[Store].[Stores].[USA].[OR]}\n"
            + "{[Store].[Stores].[USA].[WA]}\n"
            + "{[Store].[Stores].[USA].[CA plus OR]}\n"
            + "Row #0: 16,890\n"
            + "Row #0: 36,175.20\n"
            + "Row #1: 19,287\n"
            + "Row #1: 40,170.29\n"
            + "Row #2: 30,114\n"
            + "Row #2: 63,282.86\n"
            + "Row #3: 36,177\n"
            + "Row #3: 76,345.49\n");
        //----------------------------------------------------
        // Calc member in dimension based on level included
        // Calc members in measures in schema included
        //----------------------------------------------------
        assertQueryReturns(
            "WITH MEMBER [Store].[Stores].[USA].[CA plus OR] AS 'AGGREGATE({[Store].[USA].[CA], [Store].[Stores].[USA].[OR]})' "
            + "SELECT AddCalculatedMembers({[Measures].[Unit Sales], [Measures].[Store Sales]}) ON COLUMNS,"
            + "AddCalculatedMembers([Store].[USA].Children) ON ROWS "
            + "FROM Sales "
            + "WHERE ([1997].[Q1])",
            "Axis #0:\n"
            + "{[Time].[Time].[1997].[Q1]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Store Sales]}\n"
            + "{[Measures].[Profit]}\n"
            + "{[Measures].[Profit last Period]}\n"
            + "{[Measures].[Profit Growth]}\n"
            + "Axis #2:\n"
            + "{[Store].[Stores].[USA].[CA]}\n"
            + "{[Store].[Stores].[USA].[OR]}\n"
            + "{[Store].[Stores].[USA].[WA]}\n"
            + "{[Store].[Stores].[USA].[CA plus OR]}\n"
            + "Row #0: 16,890\n"
            + "Row #0: 36,175.20\n"
            + "Row #0: $21,744.11\n"
            + "Row #0: $21,744.11\n"
            + "Row #0: 0.0%\n"
            + "Row #1: 19,287\n"
            + "Row #1: 40,170.29\n"
            + "Row #1: $24,089.22\n"
            + "Row #1: $24,089.22\n"
            + "Row #1: 0.0%\n"
            + "Row #2: 30,114\n"
            + "Row #2: 63,282.86\n"
            + "Row #2: $38,042.78\n"
            + "Row #2: $38,042.78\n"
            + "Row #2: 0.0%\n"
            + "Row #3: 36,177\n"
            + "Row #3: 76,345.49\n"
            + "Row #3: $45,833.33\n"
            + "Row #3: $45,833.33\n"
            + "Row #3: 0.0%\n");
        //----------------------------------------------------
        // Two dimensions
        //----------------------------------------------------
        assertQueryReturns(
            "SELECT AddCalculatedMembers({[Measures].[Unit Sales], [Measures].[Store Sales]}) ON COLUMNS,"
            + "{([Store].[Stores].[USA].[CA], [Gender].[F])} ON ROWS "
            + "FROM Sales "
            + "WHERE ([1997].[Q1])",
            "Axis #0:\n"
            + "{[Time].[Time].[1997].[Q1]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Store Sales]}\n"
            + "{[Measures].[Profit]}\n"
            + "{[Measures].[Profit last Period]}\n"
            + "{[Measures].[Profit Growth]}\n"
            + "Axis #2:\n"
            + "{[Store].[Stores].[USA].[CA], [Customer].[Gender].[F]}\n"
            + "Row #0: 8,218\n"
            + "Row #0: 17,928.37\n"
            + "Row #0: $10,771.98\n"
            + "Row #0: $10,771.98\n"
            + "Row #0: 0.0%\n");
        // ---------------------------------------------------
        // Should throw more than one dimension error
        // ---------------------------------------------------
        assertAxisThrows(
            "AddCalculatedMembers({([Store].[USA].[CA], [Gender].[F])})",
            "Only single dimension members allowed in set for "
            + "AddCalculatedMembers");
    }

    public void testStripCalculatedMembers() {
        assertAxisReturns(
            "StripCalculatedMembers({[Measures].AllMembers})",
            "[Measures].[Unit Sales]\n"
            + "[Measures].[Store Cost]\n"
            + "[Measures].[Store Sales]\n"
            + "[Measures].[Sales Count]\n"
            + "[Measures].[Customer Count]\n"
            + "[Measures].[Promotion Sales]");

        // applied to empty set
        assertAxisReturns("StripCalculatedMembers({[Gender].Parent})", "");

        getTestContext().assertSetExprDependsOn(
            "StripCalculatedMembers([Customers].CurrentMember.Children)",
            setOf("[Customer].[Customers]"));

        // ---------------------------------------------------
        // Calc members in dimension based on level stripped
        // Actual members in measures left alone
        // ---------------------------------------------------
        assertQueryReturns(
            "WITH MEMBER [Store].[Stores].[USA].[CA plus OR] AS "
            + "'AGGREGATE({[Store].[USA].[CA], [Store].[USA].[OR]})' "
            + "SELECT StripCalculatedMembers({[Measures].[Unit Sales], "
            + "[Measures].[Store Sales]}) ON COLUMNS,"
            + "StripCalculatedMembers("
            + "AddCalculatedMembers([Store].[USA].Children)) ON ROWS "
            + "FROM Sales "
            + "WHERE ([1997].[Q1])",
            "Axis #0:\n"
            + "{[Time].[Time].[1997].[Q1]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Store Sales]}\n"
            + "Axis #2:\n"
            + "{[Store].[Stores].[USA].[CA]}\n"
            + "{[Store].[Stores].[USA].[OR]}\n"
            + "{[Store].[Stores].[USA].[WA]}\n"
            + "Row #0: 16,890\n"
            + "Row #0: 36,175.20\n"
            + "Row #1: 19,287\n"
            + "Row #1: 40,170.29\n"
            + "Row #2: 30,114\n"
            + "Row #2: 63,282.86\n");
    }

    public void testCurrentMember() {
        // <Dimension>.CurrentMember
        assertAxisThrows(
            "[Customer].CurrentMember",
            "The 'Customer' dimension contains more than one hierarchy, therefore the hierarchy must be explicitly specified.");
        // <Hierarchy>.CurrentMember
        assertAxisReturns(
            "[Gender].Hierarchy.CurrentMember",
            "[Customer].[Gender].[All Gender]");

        // <Level>.CurrentMember
        // MSAS doesn't allow this, but Mondrian does: it implicitly casts
        // level to hierarchy.
        assertAxisReturns(
            "[Store Name].CurrentMember",
            "[Store].[Stores].[All Stores]");
    }

    public void testCurrentMemberDepends() {
        getTestContext().assertMemberExprDependsOn(
            "[Gender].CurrentMember",
            setOf("[Customer].[Gender]"));

        getTestContext().assertExprDependsOn(
            "[Gender].[M].Dimension.Name", Collections.<String>emptySet());

        // implicit call to .CurrentMember when dimension is used as a member
        // expression
        getTestContext().assertAxisThrows(
            "{[Gender].[M].Dimension}",
            "The 'Customer' dimension contains more than one hierarchy, therefore the hierarchy must be explicitly specified.");

        // [Customers] is short for [Customer].[Customers].CurrentMember, so
        // depends upon everything
        getTestContext().assertExprDependsOn(
            "[Customers]", TestContext.allHiers());
    }

    public void testCurrentMemberFromSlicer() {
        Result result = executeQuery(
            "with member [Measures].[Foo] as '[Gender].CurrentMember.Name'\n"
            + "select {[Measures].[Foo]} on columns\n"
            + "from Sales where ([Gender].[F])");
        assertEquals("F", result.getCell(new int[]{0}).getValue());
    }

    public void testCurrentMemberFromDefaultMember() {
        Result result = executeQuery(
            "with member [Measures].[Foo] as"
            + " '[Time].[Time].CurrentMember.Name'\n"
            + "select {[Measures].[Foo]} on columns\n"
            + "from Sales");
        assertEquals("1997", result.getCell(new int[]{0}).getValue());
    }

    public void testCurrentMemberMultiHierarchy() {
        final String hierarchyName = "Weekly";
        final String queryString =
            "with member [Measures].[Foo] as\n"
            + " 'IIf(([Time].[Time].CurrentMember.Hierarchy.Name = \""
            + hierarchyName
            + "\"), \n"
            + "[Measures].[Unit Sales], \n"
            + "- [Measures].[Unit Sales])'\n"
            + "select {[Measures].[Unit Sales], [Measures].[Foo]} ON COLUMNS,\n"
            + "  {[Product].[Food].[Dairy]} ON ROWS\n"
            + "from [Sales]";
        Result result =
            executeQuery(
                queryString + " where [Time].[1997]");
        final int[] coords = {1, 0};
        assertEquals(
            "-12,885",
            result.getCell(coords).getFormattedValue());

        // As above, but context provided on rows axis as opposed to slicer.
        final String queryString1 =
            "with member [Measures].[Foo] as\n"
            + " 'IIf(([Time].[Time].CurrentMember.Hierarchy.Name = \""
            + hierarchyName
            + "\"), \n"
            + "[Measures].[Unit Sales], \n"
            + "- [Measures].[Unit Sales])'\n"
            + "select {[Measures].[Unit Sales], [Measures].[Foo]} ON COLUMNS,";

        final String queryString2 =
            "from [Sales]\n"
            + "  where [Product].[Food].[Dairy] ";

        result =
            executeQuery(
                queryString1 + " {[Time].[1997]} ON ROWS " + queryString2);
        assertEquals(
            "-12,885",
            result.getCell(coords).getFormattedValue());

        result =
            executeQuery(
                queryString + " where [Time.Weekly].[1997]");
        assertEquals(
            "-12,885",
            result.getCell(coords).getFormattedValue());

        result =
            executeQuery(
                queryString1 + " {[Time.Weekly].[1997]} ON ROWS "
                + queryString2);
        assertEquals(
            "-12,885",
            result.getCell(coords).getFormattedValue());
    }

    public void testDefaultMember() {
        // [Time] has no default member and no all, so the default member is
        // the first member of the first level.
        if (false) {
        Result result =
            executeQuery(
                "select {[Time].[Time].DefaultMember} on columns\n"
                + "from Sales");
        assertEquals(
            "1997",
            result.getAxes()[0].getPositions().get(0).get(0).getName());

        // [Time].[Weekly] has an all member and no explicit default.
        result =
            executeQuery(
                "select {[Time.Weekly].DefaultMember} on columns\n"
                + "from Sales");
        assertEquals(
            "All Weeklys",
            result.getAxes()[0].getPositions().get(0).get(0).getName());
        }

        final String memberUname =
            "[Time2].[Weekly].[1997].[23]";

        if (!Bug.ModifiedSchema) {
            return;
        }
        TestContext testContext = TestContext.instance().legacy()
        .createSubstitutingCube(
            "Sales",
            "  <Dimension name=\"Time2\" type=\"TimeDimension\" foreignKey=\"time_id\">\n"
            + "    <Hierarchy hasAll=\"false\" primaryKey=\"time_id\">\n"
            + "      <Table name=\"time_by_day\"/>\n"
            + "      <Level name=\"Year\" column=\"the_year\" type=\"Numeric\" uniqueMembers=\"true\"\n"
            + "          levelType=\"TimeYears\"/>\n"
            + "      <Level name=\"Quarter\" column=\"quarter\" uniqueMembers=\"false\"\n"
            + "          levelType=\"TimeQuarters\"/>\n"
            + "      <Level name=\"Month\" column=\"month_of_year\" uniqueMembers=\"false\" type=\"Numeric\"\n"
            + "          levelType=\"TimeMonths\"/>\n"
            + "    </Hierarchy>\n"
            + "    <Hierarchy hasAll=\"true\" name=\"Weekly\" primaryKey=\"time_id\"\n"
            + "          defaultMember=\""
            + memberUname
            + "\">\n"
            + "      <Table name=\"time_by_day\"/>\n"
            + "      <Level name=\"Year\" column=\"the_year\" type=\"Numeric\" uniqueMembers=\"true\"\n"
            + "          levelType=\"TimeYears\"/>\n"
            + "      <Level name=\"Week\" column=\"week_of_year\" type=\"Numeric\" uniqueMembers=\"false\"\n"
            + "          levelType=\"TimeWeeks\"/>\n"
            + "      <Level name=\"Day\" column=\"day_of_month\" uniqueMembers=\"false\" type=\"Numeric\"\n"
            + "          levelType=\"TimeDays\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>");

        // In this variant of the schema, Time2.Weekly has an explicit default
        // member.
        Result result =
            testContext.executeQuery(
                "select {[Time2.Weekly].DefaultMember} on columns\n"
                + "from Sales");
        assertEquals(
            "23",
            result.getAxes()[0].getPositions().get(0).get(0).getName());
    }

    public void testCurrentMemberFromAxis() {
        Result result = executeQuery(
            "with member [Measures].[Foo] as"
            + " '[Gender].CurrentMember.Name"
            + " || [Marital Status].CurrentMember.Name'\n"
            + "select {[Measures].[Foo]} on columns,\n"
            + " CrossJoin({[Gender].children},"
            + "           {[Marital Status].children}) on rows\n"
            + "from Sales");
        assertEquals("FM", result.getCell(new int[]{0, 0}).getValue());
    }

    /**
     * When evaluating a calculated member, MSOLAP regards that
     * calculated member as the current member of that dimension, so it
     * cycles in this case. But I disagree; it is the previous current
     * member, before the calculated member was expanded.
     */
    public void testCurrentMemberInCalcMember() {
        Result result = executeQuery(
            "with member [Measures].[Foo] as '[Measures].CurrentMember.Name'\n"
            + "select {[Measures].[Foo]} on columns\n"
            + "from Sales");
        assertEquals(
            "Unit Sales", result.getCell(new int[]{0}).getValue());
    }

    /**
     * Tests NamedSet.CurrentOrdinal combined with the Order function.
     */
    public void testNamedSetCurrentOrdinalWithOrder() {
        // The <Named Set>.CurrentOrdinal only works correctly when named sets
        // are evaluated as iterables, and JDK 1.4 only supports lists.
        if (Util.Retrowoven) {
            return;
        }
        assertQueryReturns(
            "with set [Time Regular] as [Time].[Time].Members\n"
            + " set [Time Reversed] as"
            + " Order([Time Regular], [Time Regular].CurrentOrdinal, BDESC)\n"
            + "select [Time Reversed] on 0\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time].[Time].[1998].[Q4].[12]}\n"
            + "{[Time].[Time].[1998].[Q4].[11]}\n"
            + "{[Time].[Time].[1998].[Q4].[10]}\n"
            + "{[Time].[Time].[1998].[Q4]}\n"
            + "{[Time].[Time].[1998].[Q3].[9]}\n"
            + "{[Time].[Time].[1998].[Q3].[8]}\n"
            + "{[Time].[Time].[1998].[Q3].[7]}\n"
            + "{[Time].[Time].[1998].[Q3]}\n"
            + "{[Time].[Time].[1998].[Q2].[6]}\n"
            + "{[Time].[Time].[1998].[Q2].[5]}\n"
            + "{[Time].[Time].[1998].[Q2].[4]}\n"
            + "{[Time].[Time].[1998].[Q2]}\n"
            + "{[Time].[Time].[1998].[Q1].[3]}\n"
            + "{[Time].[Time].[1998].[Q1].[2]}\n"
            + "{[Time].[Time].[1998].[Q1].[1]}\n"
            + "{[Time].[Time].[1998].[Q1]}\n"
            + "{[Time].[Time].[1998]}\n"
            + "{[Time].[Time].[1997].[Q4].[12]}\n"
            + "{[Time].[Time].[1997].[Q4].[11]}\n"
            + "{[Time].[Time].[1997].[Q4].[10]}\n"
            + "{[Time].[Time].[1997].[Q4]}\n"
            + "{[Time].[Time].[1997].[Q3].[9]}\n"
            + "{[Time].[Time].[1997].[Q3].[8]}\n"
            + "{[Time].[Time].[1997].[Q3].[7]}\n"
            + "{[Time].[Time].[1997].[Q3]}\n"
            + "{[Time].[Time].[1997].[Q2].[6]}\n"
            + "{[Time].[Time].[1997].[Q2].[5]}\n"
            + "{[Time].[Time].[1997].[Q2].[4]}\n"
            + "{[Time].[Time].[1997].[Q2]}\n"
            + "{[Time].[Time].[1997].[Q1].[3]}\n"
            + "{[Time].[Time].[1997].[Q1].[2]}\n"
            + "{[Time].[Time].[1997].[Q1].[1]}\n"
            + "{[Time].[Time].[1997].[Q1]}\n"
            + "{[Time].[Time].[1997]}\n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: 26,796\n"
            + "Row #0: 25,270\n"
            + "Row #0: 19,958\n"
            + "Row #0: 72,024\n"
            + "Row #0: 20,388\n"
            + "Row #0: 21,697\n"
            + "Row #0: 23,763\n"
            + "Row #0: 65,848\n"
            + "Row #0: 21,350\n"
            + "Row #0: 21,081\n"
            + "Row #0: 20,179\n"
            + "Row #0: 62,610\n"
            + "Row #0: 23,706\n"
            + "Row #0: 20,957\n"
            + "Row #0: 21,628\n"
            + "Row #0: 66,291\n"
            + "Row #0: 266,773\n");
    }

    /**
     * Tests NamedSet.CurrentOrdinal combined with the Generate function.
     */
    public void testNamedSetCurrentOrdinalWithGenerate() {
        // The <Named Set>.CurrentOrdinal only works correctly when named sets
        // are evaluated as iterables, and JDK 1.4 only supports lists.
        if (Util.Retrowoven) {
            return;
        }
        assertQueryReturns(
            " with set [Time Regular] as [Time].[Time].Members\n"
            + "set [Every Other Time] as\n"
            + "  Generate(\n"
            + "    [Time Regular],\n"
            + "    {[Time].[Time].Members.Item(\n"
            + "      [Time Regular].CurrentOrdinal * 2)})\n"
            + "select [Every Other Time] on 0\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time].[Time].[1997]}\n"
            + "{[Time].[Time].[1997].[Q1].[1]}\n"
            + "{[Time].[Time].[1997].[Q1].[3]}\n"
            + "{[Time].[Time].[1997].[Q2].[4]}\n"
            + "{[Time].[Time].[1997].[Q2].[6]}\n"
            + "{[Time].[Time].[1997].[Q3].[7]}\n"
            + "{[Time].[Time].[1997].[Q3].[9]}\n"
            + "{[Time].[Time].[1997].[Q4].[10]}\n"
            + "{[Time].[Time].[1997].[Q4].[12]}\n"
            + "{[Time].[Time].[1998].[Q1]}\n"
            + "{[Time].[Time].[1998].[Q1].[2]}\n"
            + "{[Time].[Time].[1998].[Q2]}\n"
            + "{[Time].[Time].[1998].[Q2].[5]}\n"
            + "{[Time].[Time].[1998].[Q3]}\n"
            + "{[Time].[Time].[1998].[Q3].[8]}\n"
            + "{[Time].[Time].[1998].[Q4]}\n"
            + "{[Time].[Time].[1998].[Q4].[11]}\n"
            + "Row #0: 266,773\n"
            + "Row #0: 21,628\n"
            + "Row #0: 23,706\n"
            + "Row #0: 20,179\n"
            + "Row #0: 21,350\n"
            + "Row #0: 23,763\n"
            + "Row #0: 20,388\n"
            + "Row #0: 19,958\n"
            + "Row #0: 26,796\n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n");
    }

    public void testNamedSetCurrentOrdinalWithFilter() {
        // The <Named Set>.CurrentOrdinal only works correctly when named sets
        // are evaluated as iterables, and JDK 1.4 only supports lists.
        if (Util.Retrowoven) {
            return;
        }
        assertQueryReturns(
            "with set [Time Regular] as [Time].[Time].Members\n"
            + " set [Time Subset] as "
            + "   Filter([Time Regular], [Time Regular].CurrentOrdinal = 3"
            + "                       or [Time Regular].CurrentOrdinal = 5)\n"
            + "select [Time Subset] on 0\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time].[Time].[1997].[Q1].[2]}\n"
            + "{[Time].[Time].[1997].[Q2]}\n"
            + "Row #0: 20,957\n"
            + "Row #0: 62,610\n");
    }

    public void testNamedSetCurrentOrdinalWithCrossjoin() {
        // TODO:
    }

    public void testNamedSetCurrentOrdinalWithNonNamedSetFails() {
        // a named set wrapped in {...} is not a named set, so CurrentOrdinal
        // fails
        assertQueryThrows(
            "with set [Time Members] as [Time].Members\n"
            + "member [Measures].[Foo] as ' {[Time Members]}.CurrentOrdinal '\n"
            + "select {[Measures].[Unit Sales], [Measures].[Foo]} on 0,\n"
            + " {[Product].Children} on 1\n"
            + "from [Sales]",
            "Not a named set");

        // as above for Current function
        assertQueryThrows(
            "with set [Time Members] as [Time].Members\n"
            + "member [Measures].[Foo] as ' {[Time Members]}.Current.Name '\n"
            + "select {[Measures].[Unit Sales], [Measures].[Foo]} on 0,\n"
            + " {[Product].Children} on 1\n"
            + "from [Sales]",
            "Not a named set");

        // a set expression is not a named set, so CurrentOrdinal fails
        assertQueryThrows(
            "with member [Measures].[Foo] as\n"
            + " ' Head([Time].Members, 5).CurrentOrdinal '\n"
            + "select {[Measures].[Unit Sales], [Measures].[Foo]} on 0,\n"
            + " {[Product].Children} on 1\n"
            + "from [Sales]",
            "Not a named set");

        // as above for Current function
        assertQueryThrows(
            "with member [Measures].[Foo] as\n"
            + " ' Crossjoin([Time].Members, [Gender].Members).Current.Name '\n"
            + "select {[Measures].[Unit Sales], [Measures].[Foo]} on 0,\n"
            + " {[Product].Children} on 1\n"
            + "from [Sales]",
            "Not a named set");
    }

    public void testDimensionDefaultMember() {
        Member member = executeSingletonAxis("[Measures].DefaultMember");
        assertEquals("Unit Sales", member.getName());
    }

    public void testDrilldownLevel() {
        // Expect all children of USA
        assertAxisReturns(
            "DrilldownLevel({[Store].[USA]}, [Store].[Store Country])",
            "[Store].[Stores].[USA]\n"
            + "[Store].[Stores].[USA].[CA]\n"
            + "[Store].[Stores].[USA].[OR]\n"
            + "[Store].[Stores].[USA].[WA]");

        // Expect same set, because [USA] is already drilled
        assertAxisReturns(
            "DrilldownLevel({[Store].[USA], [Store].[USA].[CA]}, [Store].[Store Country])",
            "[Store].[Stores].[USA]\n"
            + "[Store].[Stores].[USA].[CA]");

        // Expect drill, because [USA] isn't already drilled. You can't
        // drill down on [CA] and get to [USA]
        assertAxisReturns(
            "DrilldownLevel({[Store].[USA].[CA],[Store].[USA]}, [Store].[Store Country])",
            "[Store].[Stores].[USA].[CA]\n"
            + "[Store].[Stores].[USA]\n"
            + "[Store].[Stores].[USA].[CA]\n"
            + "[Store].[Stores].[USA].[OR]\n"
            + "[Store].[Stores].[USA].[WA]");

        assertAxisReturns(
            "DrilldownLevel({[Store].[USA].[CA],[Store].[USA]},, 0)",
            "[Store].[Stores].[USA].[CA]\n"
            + "[Store].[Stores].[USA].[CA].[Alameda]\n"
            + "[Store].[Stores].[USA].[CA].[Beverly Hills]\n"
            + "[Store].[Stores].[USA].[CA].[Los Angeles]\n"
            + "[Store].[Stores].[USA].[CA].[San Diego]\n"
            + "[Store].[Stores].[USA].[CA].[San Francisco]\n"
            + "[Store].[Stores].[USA]\n"
            + "[Store].[Stores].[USA].[CA]\n"
            + "[Store].[Stores].[USA].[OR]\n"
            + "[Store].[Stores].[USA].[WA]");

        assertAxisReturns(
            "DrilldownLevel({[Store].[USA].[CA],[Store].[USA]} * {[Gender].Members},, 0)",
            "{[Store].[Stores].[USA].[CA], [Customer].[Gender].[All Gender]}\n"
            + "{[Store].[Stores].[USA].[CA].[Alameda], [Customer].[Gender].[All Gender]}\n"
            + "{[Store].[Stores].[USA].[CA].[Beverly Hills], [Customer].[Gender].[All Gender]}\n"
            + "{[Store].[Stores].[USA].[CA].[Los Angeles], [Customer].[Gender].[All Gender]}\n"
            + "{[Store].[Stores].[USA].[CA].[San Diego], [Customer].[Gender].[All Gender]}\n"
            + "{[Store].[Stores].[USA].[CA].[San Francisco], [Customer].[Gender].[All Gender]}\n"
            + "{[Store].[Stores].[USA].[CA], [Customer].[Gender].[F]}\n"
            + "{[Store].[Stores].[USA].[CA].[Alameda], [Customer].[Gender].[F]}\n"
            + "{[Store].[Stores].[USA].[CA].[Beverly Hills], [Customer].[Gender].[F]}\n"
            + "{[Store].[Stores].[USA].[CA].[Los Angeles], [Customer].[Gender].[F]}\n"
            + "{[Store].[Stores].[USA].[CA].[San Diego], [Customer].[Gender].[F]}\n"
            + "{[Store].[Stores].[USA].[CA].[San Francisco], [Customer].[Gender].[F]}\n"
            + "{[Store].[Stores].[USA].[CA], [Customer].[Gender].[M]}\n"
            + "{[Store].[Stores].[USA].[CA].[Alameda], [Customer].[Gender].[M]}\n"
            + "{[Store].[Stores].[USA].[CA].[Beverly Hills], [Customer].[Gender].[M]}\n"
            + "{[Store].[Stores].[USA].[CA].[Los Angeles], [Customer].[Gender].[M]}\n"
            + "{[Store].[Stores].[USA].[CA].[San Diego], [Customer].[Gender].[M]}\n"
            + "{[Store].[Stores].[USA].[CA].[San Francisco], [Customer].[Gender].[M]}\n"
            + "{[Store].[Stores].[USA], [Customer].[Gender].[All Gender]}\n"
            + "{[Store].[Stores].[USA].[CA], [Customer].[Gender].[All Gender]}\n"
            + "{[Store].[Stores].[USA].[OR], [Customer].[Gender].[All Gender]}\n"
            + "{[Store].[Stores].[USA].[WA], [Customer].[Gender].[All Gender]}\n"
            + "{[Store].[Stores].[USA], [Customer].[Gender].[F]}\n"
            + "{[Store].[Stores].[USA].[CA], [Customer].[Gender].[F]}\n"
            + "{[Store].[Stores].[USA].[OR], [Customer].[Gender].[F]}\n"
            + "{[Store].[Stores].[USA].[WA], [Customer].[Gender].[F]}\n"
            + "{[Store].[Stores].[USA], [Customer].[Gender].[M]}\n"
            + "{[Store].[Stores].[USA].[CA], [Customer].[Gender].[M]}\n"
            + "{[Store].[Stores].[USA].[OR], [Customer].[Gender].[M]}\n"
            + "{[Store].[Stores].[USA].[WA], [Customer].[Gender].[M]}");

        assertAxisReturns(
            "DrilldownLevel({[Store].[USA].[CA],[Store].[USA]} * {[Gender].Members},, 1)",
            "{[Store].[Stores].[USA].[CA], [Customer].[Gender].[All Gender]}\n"
            + "{[Store].[Stores].[USA].[CA], [Customer].[Gender].[F]}\n"
            + "{[Store].[Stores].[USA].[CA], [Customer].[Gender].[M]}\n"
            + "{[Store].[Stores].[USA].[CA], [Customer].[Gender].[F]}\n"
            + "{[Store].[Stores].[USA].[CA], [Customer].[Gender].[M]}\n"
            + "{[Store].[Stores].[USA], [Customer].[Gender].[All Gender]}\n"
            + "{[Store].[Stores].[USA], [Customer].[Gender].[F]}\n"
            + "{[Store].[Stores].[USA], [Customer].[Gender].[M]}\n"
            + "{[Store].[Stores].[USA], [Customer].[Gender].[F]}\n"
            + "{[Store].[Stores].[USA], [Customer].[Gender].[M]}");
    }

    public void testDrilldownLevelTop() {
        // <set>, <n>, <level>
        assertAxisReturns(
            "DrilldownLevelTop({[Store].[USA]}, 2, [Store].[Store Country])",
            "[Store].[Stores].[USA]\n"
            + "[Store].[Stores].[USA].[WA]\n"
            + "[Store].[Stores].[USA].[CA]");

        // similarly DrilldownLevelBottom
        assertAxisReturns(
            "DrilldownLevelBottom({[Store].[USA]}, 2, [Store].[Store Country])",
            "[Store].[Stores].[USA]\n"
            + "[Store].[Stores].[USA].[OR]\n"
            + "[Store].[Stores].[USA].[CA]");

        // <set>, <n>
        assertAxisReturns(
            "DrilldownLevelTop({[Store].[USA]}, 2)",
            "[Store].[Stores].[USA]\n"
            + "[Store].[Stores].[USA].[WA]\n"
            + "[Store].[Stores].[USA].[CA]");

        // <n> greater than number of children
        assertAxisReturns(
            "DrilldownLevelTop({[Store].[USA], [Store].[Canada]}, 4)",
            "[Store].[Stores].[USA]\n"
            + "[Store].[Stores].[USA].[WA]\n"
            + "[Store].[Stores].[USA].[CA]\n"
            + "[Store].[Stores].[USA].[OR]\n"
            + "[Store].[Stores].[Canada]\n"
            + "[Store].[Stores].[Canada].[BC]");

        // <n> negative
        assertAxisReturns(
            "DrilldownLevelTop({[Store].[USA]}, 2 - 3)",
            "[Store].[Stores].[USA]");

        // <n> zero
        assertAxisReturns(
            "DrilldownLevelTop({[Store].[USA]}, 2 - 2)",
            "[Store].[Stores].[USA]");

        // <n> null
        assertAxisReturns(
            "DrilldownLevelTop({[Store].[USA]}, null)",
            "[Store].[Stores].[USA]");

        // mixed bag, no level, all expanded
        assertAxisReturns(
            "DrilldownLevelTop({[Store].[USA], "
            + "[Store].[Stores].[USA].[CA].[San Francisco], "
            + "[Store].[Stores].[All Stores], "
            + "[Store].[Stores].[Canada].[BC]}, "
            + "2)",
            "[Store].[Stores].[USA]\n"
            + "[Store].[Stores].[USA].[WA]\n"
            + "[Store].[Stores].[USA].[CA]\n"
            + "[Store].[Stores].[USA].[CA].[San Francisco]\n"
            + "[Store].[Stores].[USA].[CA].[San Francisco].[Store 14]\n"
            + "[Store].[Stores].[All Stores]\n"
            + "[Store].[Stores].[USA]\n"
            + "[Store].[Stores].[Canada]\n"
            + "[Store].[Stores].[Canada].[BC]\n"
            + "[Store].[Stores].[Canada].[BC].[Vancouver]\n"
            + "[Store].[Stores].[Canada].[BC].[Victoria]");

        // mixed bag, only specified level expanded
        assertAxisReturns(
            "DrilldownLevelTop({[Store].[USA], "
            + "[Store].[USA].[CA].[San Francisco], "
            + "[Store].[All Stores], "
            + "[Store].[Canada].[BC]}, 2, [Store].[Store City])",
            "[Store].[Stores].[USA]\n"
            + "[Store].[Stores].[USA].[CA].[San Francisco]\n"
            + "[Store].[Stores].[USA].[CA].[San Francisco].[Store 14]\n"
            + "[Store].[Stores].[All Stores]\n"
            + "[Store].[Stores].[Canada].[BC]");

        // bad level
        assertAxisThrows(
            "DrilldownLevelTop({[Store].[USA]}, 2, [Customers].[Country])",
            "Level '[Customer].[Customers].[Country]' not compatible with "
            + "member '[Store].[Stores].[USA]'");
    }

    public void testDrilldownMemberEmptyExpr() {
        // no level, with expression
        assertAxisReturns(
            "DrilldownLevelTop({[Store].[USA]}, 2, , [Measures].[Unit Sales])",
            "[Store].[Stores].[USA]\n"
            + "[Store].[Stores].[USA].[WA]\n"
            + "[Store].[Stores].[USA].[CA]");

        // reverse expression
        assertAxisReturns(
            "DrilldownLevelTop("
            + "{[Store].[USA]}, 2, , - [Measures].[Unit Sales])",
            "[Store].[Stores].[USA]\n"
            + "[Store].[Stores].[USA].[OR]\n"
            + "[Store].[Stores].[USA].[CA]");
    }

    public void testDrilldownMember() {
        // Expect all children of USA
        assertAxisReturns(
            "DrilldownMember({[Store].[USA]}, {[Store].[USA]})",
            "[Store].[Stores].[USA]\n"
            + "[Store].[Stores].[USA].[CA]\n"
            + "[Store].[Stores].[USA].[OR]\n"
            + "[Store].[Stores].[USA].[WA]");

        // Expect all children of USA.CA and USA.OR
        assertAxisReturns(
            "DrilldownMember({[Store].[USA].[CA], [Store].[USA].[OR]}, "
            + "{[Store].[USA].[CA], [Store].[USA].[OR], [Store].[USA].[WA]})",
            "[Store].[Stores].[USA].[CA]\n"
            + "[Store].[Stores].[USA].[CA].[Alameda]\n"
            + "[Store].[Stores].[USA].[CA].[Beverly Hills]\n"
            + "[Store].[Stores].[USA].[CA].[Los Angeles]\n"
            + "[Store].[Stores].[USA].[CA].[San Diego]\n"
            + "[Store].[Stores].[USA].[CA].[San Francisco]\n"
            + "[Store].[Stores].[USA].[OR]\n"
            + "[Store].[Stores].[USA].[OR].[Portland]\n"
            + "[Store].[Stores].[USA].[OR].[Salem]");

        // Second set is empty
        assertAxisReturns(
            "DrilldownMember({[Store].[USA]}, {})",
            "[Store].[Stores].[USA]");

        // Drill down a leaf member
        assertAxisReturns(
            "DrilldownMember({[Store].[All Stores].[USA].[CA].[San Francisco].[Store 14]}, "
            + "{[Store].[USA].[CA].[San Francisco].[Store 14]})",
            "[Store].[Stores].[USA].[CA].[San Francisco].[Store 14]");

        // Complex case with option recursive
        assertAxisReturns(
            "DrilldownMember({[Store].[All Stores].[USA]}, "
            + "{[Store].[All Stores].[USA], [Store].[All Stores].[USA].[CA], "
            + "[Store].[All Stores].[USA].[CA].[San Diego], [Store].[All Stores].[USA].[WA]}, "
            + "RECURSIVE)",
            "[Store].[Stores].[USA]\n"
            + "[Store].[Stores].[USA].[CA]\n"
            + "[Store].[Stores].[USA].[CA].[Alameda]\n"
            + "[Store].[Stores].[USA].[CA].[Beverly Hills]\n"
            + "[Store].[Stores].[USA].[CA].[Los Angeles]\n"
            + "[Store].[Stores].[USA].[CA].[San Diego]\n"
            + "[Store].[Stores].[USA].[CA].[San Diego].[Store 24]\n"
            + "[Store].[Stores].[USA].[CA].[San Francisco]\n"
            + "[Store].[Stores].[USA].[OR]\n"
            + "[Store].[Stores].[USA].[WA]\n"
            + "[Store].[Stores].[USA].[WA].[Bellingham]\n"
            + "[Store].[Stores].[USA].[WA].[Bremerton]\n"
            + "[Store].[Stores].[USA].[WA].[Seattle]\n"
            + "[Store].[Stores].[USA].[WA].[Spokane]\n"
            + "[Store].[Stores].[USA].[WA].[Tacoma]\n"
            + "[Store].[Stores].[USA].[WA].[Walla Walla]\n"
            + "[Store].[Stores].[USA].[WA].[Yakima]");

        // Sets of tuples
        assertAxisReturns(
            "DrilldownMember({([Store Type].[Supermarket], [Store].[USA])}, {[Store].[USA]})",
            "{[Store].[Store Type].[Supermarket], [Store].[Stores].[USA]}\n"
            + "{[Store].[Store Type].[Supermarket], [Store].[Stores].[USA].[CA]}\n"
            + "{[Store].[Store Type].[Supermarket], [Store].[Stores].[USA].[OR]}\n"
            + "{[Store].[Store Type].[Supermarket], [Store].[Stores].[USA].[WA]}");
    }


    public void testFirstChildFirstInLevel() {
        Member member = executeSingletonAxis("[Time].[1997].[Q4].FirstChild");
        assertEquals("10", member.getName());
    }

    public void testFirstChildAll() {
        Member member =
            executeSingletonAxis("[Gender].[All Gender].FirstChild");
        assertEquals("F", member.getName());
    }

    public void testFirstChildOfChildless() {
        Member member =
            executeSingletonAxis("[Gender].[All Gender].[F].FirstChild");
        Assert.assertNull(member);
    }

    public void testFirstSiblingFirstInLevel() {
        Member member = executeSingletonAxis("[Gender].[F].FirstSibling");
        assertEquals("F", member.getName());
    }

    public void testFirstSiblingLastInLevel() {
        Member member =
            executeSingletonAxis("[Time].[1997].[Q4].FirstSibling");
        assertEquals("Q1", member.getName());
    }

    public void testFirstSiblingAll() {
        Member member =
            executeSingletonAxis("[Gender].[All Gender].FirstSibling");
        Assert.assertTrue(member.isAll());
    }

    public void testFirstSiblingRoot() {
        // The [Measures] hierarchy does not have an 'all' member, so
        // [Unit Sales] does not have a parent.
        Member member =
            executeSingletonAxis("[Measures].[Store Sales].FirstSibling");
        assertEquals("Unit Sales", member.getName());
    }

    public void testFirstSiblingNull() {
        Member member =
            executeSingletonAxis("[Gender].[F].FirstChild.FirstSibling");
        Assert.assertNull(member);
    }

    public void testLag() {
        Member member = executeSingletonAxis("[Time].[1997].[Q4].[12].Lag(4)");
        assertEquals("8", member.getName());
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
        assertEquals("1997", member.getName());
    }

    public void testLagRootTooFar() {
        Member member = executeSingletonAxis("[Time].[1998].Lag(2)");
        Assert.assertNull(member);
    }

    public void testLastChild() {
        Member member = executeSingletonAxis("[Gender].LastChild");
        assertEquals("M", member.getName());
    }

    public void testLastChildLastInLevel() {
        Member member = executeSingletonAxis("[Time].[1997].[Q4].LastChild");
        assertEquals("12", member.getName());
    }

    public void testLastChildAll() {
        Member member = executeSingletonAxis("[Gender].[All Gender].LastChild");
        assertEquals("M", member.getName());
    }

    public void testLastChildOfChildless() {
        Member member = executeSingletonAxis("[Gender].[M].LastChild");
        Assert.assertNull(member);
    }

    public void testLastSibling() {
        Member member = executeSingletonAxis("[Gender].[F].LastSibling");
        assertEquals("M", member.getName());
    }

    public void testLastSiblingFirstInLevel() {
        Member member = executeSingletonAxis("[Time].[1997].[Q1].LastSibling");
        assertEquals("Q4", member.getName());
    }

    public void testLastSiblingAll() {
        Member member =
            executeSingletonAxis("[Gender].[All Gender].LastSibling");
        Assert.assertTrue(member.isAll());
    }

    public void testLastSiblingRoot() {
        // The [Time] hierarchy does not have an 'all' member, so
        // [1997], [1998] do not have parents.
        Member member = executeSingletonAxis("[Time].[1998].LastSibling");
        assertEquals("1998", member.getName());
    }

    public void testLastSiblingNull() {
        Member member =
            executeSingletonAxis("[Gender].[F].FirstChild.LastSibling");
        Assert.assertNull(member);
    }


    public void testLead() {
        Member member = executeSingletonAxis("[Time].[1997].[Q2].[4].Lead(4)");
        assertEquals("8", member.getName());
    }

    public void testLeadNegative() {
        Member member = executeSingletonAxis("[Gender].[M].Lead(-1)");
        assertEquals("F", member.getName());
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
        assertEquals("F", member.getName());
    }

    public void testBasic2() {
        Result result =
            executeQuery(
                "select {[Gender].[F].NextMember} ON COLUMNS from Sales");
        assertEquals(
            "M",
            result.getAxes()[0].getPositions().get(0).get(0).getName());
    }

    public void testFirstInLevel2() {
        Result result =
            executeQuery(
                "select {[Gender].[M].NextMember} ON COLUMNS from Sales");
        assertEquals(0, result.getAxes()[0].getPositions().size());
    }

    public void testAll2() {
        Result result =
            executeQuery("select {[Gender].PrevMember} ON COLUMNS from Sales");
        // previous to [Gender].[All] is null, so no members are returned
        assertEquals(0, result.getAxes()[0].getPositions().size());
    }


    public void testBasic5() {
        Result result =
            executeQuery(
                "select{ [Product].[Products].[Drink].Parent} on columns "
                + "from Sales");
        assertEquals(
            "All Products",
            result.getAxes()[0].getPositions().get(0).get(0).getName());
    }

    public void testFirstInLevel5() {
        Result result =
            executeQuery(
                "select {[Time].[1997].[Q2].[4].Parent} on columns,"
                + "{[Customer].[Gender].[M]} on rows from Sales");
        assertEquals(
            "Q2",
            result.getAxes()[0].getPositions().get(0).get(0).getName());
    }

    public void testAll5() {
        Result result =
            executeQuery(
                "select {[Time].[1997].[Q2].Parent} on columns,"
                + "{[Customer].[Gender].[M]} on rows from Sales");
        // previous to [Gender].[All] is null, so no members are returned
        assertEquals(
            "1997",
            result.getAxes()[0].getPositions().get(0).get(0).getName());
    }

    public void testBasic() {
        Result result =
            executeQuery(
                "select {[Gender].[M].PrevMember} ON COLUMNS from Sales");
        assertEquals(
            "F",
            result.getAxes()[0].getPositions().get(0).get(0).getName());
    }

    public void testFirstInLevel() {
        Result result =
            executeQuery(
                "select {[Gender].[F].PrevMember} ON COLUMNS from Sales");
        assertEquals(0, result.getAxes()[0].getPositions().size());
    }

    public void testAll() {
        Result result =
            executeQuery("select {[Gender].PrevMember} ON COLUMNS from Sales");
        // previous to [Gender].[All] is null, so no members are returned
        assertEquals(0, result.getAxes()[0].getPositions().size());
    }

    public void testAggregateDepends() {
        // Depends on everything except Measures, Gender
        Set<String> s12 = TestContext.allHiersExcept(
            "[Measures]",
            "[Customer].[Gender]");
        getTestContext().assertExprDependsOn(
            "([Measures].[Unit Sales], [Gender].[F])", s12);
        // Depends on everything except Customers, Measures, Gender
        Set<String> s13 = TestContext.allHiersExcept(
            "[Customer].[Customers]", "[Customer].[Gender]");
        getTestContext().assertExprDependsOn(
            "Aggregate([Customers].Members, ([Measures].[Unit Sales], [Gender].[F]))",
            s13);
        // Depends on everything except Customers
        Set<String> s11 = TestContext.allHiersExcept("[Customer].[Customers]");
        getTestContext().assertExprDependsOn(
            "Aggregate([Customers].Members)",
            s11);
        // Depends on the current member of the Product dimension, even though
        // [Product].[All Products] is referenced from the expression.
        Set<String> s1 = TestContext.allHiersExcept("[Customer].[Customers]");
        getTestContext().assertExprDependsOn(
            "Aggregate(Filter([Customers].[City].Members, (([Measures].[Unit Sales] / ([Measures].[Unit Sales], [Product].[All Products])) > 0.1)))",
            s1);
    }

    public void testAggregate() {
        assertQueryReturns(
            "WITH MEMBER [Store].[Stores].[CA plus OR] AS 'AGGREGATE({[Store].[USA].[CA], [Store].[USA].[OR]})'\n"
            + "SELECT {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,\n"
            + "      {[Store].[USA].[CA], [Store].[USA].[OR], [Store].[CA plus OR]} ON ROWS\n"
            + "FROM Sales\n"
            + "WHERE ([1997].[Q1])",
            "Axis #0:\n"
            + "{[Time].[Time].[1997].[Q1]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Store Sales]}\n"
            + "Axis #2:\n"
            + "{[Store].[Stores].[USA].[CA]}\n"
            + "{[Store].[Stores].[USA].[OR]}\n"
            + "{[Store].[Stores].[CA plus OR]}\n"
            + "Row #0: 16,890\n"
            + "Row #0: 36,175.20\n"
            + "Row #1: 19,287\n"
            + "Row #1: 40,170.29\n"
            + "Row #2: 36,177\n"
            + "Row #2: 76,345.49\n");
    }

    public void testAggregate2() {
        assertQueryReturns(
            "WITH\n"
            + "  Member [Time].[Time].[1st Half Sales] AS 'Aggregate({Time.[1997].[Q1], Time.[1997].[Q2]})'\n"
            + "  Member [Time].[Time].[2nd Half Sales] AS 'Aggregate({Time.[1997].[Q3], Time.[1997].[Q4]})'\n"
            + "  Member [Time].[Time].[Difference] AS 'Time.[2nd Half Sales] - Time.[1st Half Sales]'\n"
            + "SELECT\n"
            + "   { [Store].[Store State].Members} ON COLUMNS,\n"
            + "   { Time.[1st Half Sales], Time.[2nd Half Sales], Time.[Difference]} ON ROWS\n"
            + "FROM Sales\n"
            + "WHERE [Measures].[Store Sales]",
            "Axis #0:\n"
            + "{[Measures].[Store Sales]}\n"
            + "Axis #1:\n"
            + "{[Store].[Stores].[Canada].[BC]}\n"
            + "{[Store].[Stores].[Mexico].[DF]}\n"
            + "{[Store].[Stores].[Mexico].[Guerrero]}\n"
            + "{[Store].[Stores].[Mexico].[Jalisco]}\n"
            + "{[Store].[Stores].[Mexico].[Veracruz]}\n"
            + "{[Store].[Stores].[Mexico].[Yucatan]}\n"
            + "{[Store].[Stores].[Mexico].[Zacatecas]}\n"
            + "{[Store].[Stores].[USA].[CA]}\n"
            + "{[Store].[Stores].[USA].[OR]}\n"
            + "{[Store].[Stores].[USA].[WA]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[1st Half Sales]}\n"
            + "{[Time].[Time].[2nd Half Sales]}\n"
            + "{[Time].[Time].[Difference]}\n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: 74,571.95\n"
            + "Row #0: 71,943.17\n"
            + "Row #0: 125,779.50\n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: 84,595.89\n"
            + "Row #1: 70,333.90\n"
            + "Row #1: 138,013.72\n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: 10,023.94\n"
            + "Row #2: -1,609.27\n"
            + "Row #2: 12,234.22\n");
    }

    public void testAggregateWithIIF() {
        assertQueryReturns(
            "with member store.[Stores].foo as 'iif(3>1,"
            + "aggregate({[Store].[All Stores].[USA].[OR]}),"
            + "aggregate({[Store].[All Stores].[USA].[CA]}))' "
            + "select {store.foo} on 0 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[Stores].[foo]}\n"
            + "Row #0: 67,659\n");
    }


    public void testAggregate2AllMembers() {
        assertQueryReturns(
            "WITH\n"
            + "  Member [Time].[Time].[1st Half Sales] AS 'Aggregate({Time.[1997].[Q1], Time.[1997].[Q2]})'\n"
            + "  Member [Time].[Time].[2nd Half Sales] AS 'Aggregate({Time.[1997].[Q3], Time.[1997].[Q4]})'\n"
            + "  Member [Time].[Time].[Difference] AS 'Time.[2nd Half Sales] - Time.[1st Half Sales]'\n"
            + "SELECT\n"
            + "   { [Store].[Store State].AllMembers} ON COLUMNS,\n"
            + "   { Time.[1st Half Sales], Time.[2nd Half Sales], Time.[Difference]} ON ROWS\n"
            + "FROM Sales\n"
            + "WHERE [Measures].[Store Sales]",
            "Axis #0:\n"
            + "{[Measures].[Store Sales]}\n"
            + "Axis #1:\n"
            + "{[Store].[Stores].[Canada].[BC]}\n"
            + "{[Store].[Stores].[Mexico].[DF]}\n"
            + "{[Store].[Stores].[Mexico].[Guerrero]}\n"
            + "{[Store].[Stores].[Mexico].[Jalisco]}\n"
            + "{[Store].[Stores].[Mexico].[Veracruz]}\n"
            + "{[Store].[Stores].[Mexico].[Yucatan]}\n"
            + "{[Store].[Stores].[Mexico].[Zacatecas]}\n"
            + "{[Store].[Stores].[USA].[CA]}\n"
            + "{[Store].[Stores].[USA].[OR]}\n"
            + "{[Store].[Stores].[USA].[WA]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[1st Half Sales]}\n"
            + "{[Time].[Time].[2nd Half Sales]}\n"
            + "{[Time].[Time].[Difference]}\n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: 74,571.95\n"
            + "Row #0: 71,943.17\n"
            + "Row #0: 125,779.50\n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: 84,595.89\n"
            + "Row #1: 70,333.90\n"
            + "Row #1: 138,013.72\n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: 10,023.94\n"
            + "Row #2: -1,609.27\n"
            + "Row #2: 12,234.22\n");
    }


    public void testAggregateToSimulateCompoundSlicer() {
        assertQueryReturns(
            "WITH MEMBER [Time].[Time].[1997 H1] as 'Aggregate({[Time].[1997].[Q1], [Time].[1997].[Q2]})'\n"
            + "  MEMBER [Education Level].[College or higher] as 'Aggregate({[Education Level].[Bachelors Degree], [Education Level].[Graduate Degree]})'\n"
            + "SELECT {[Measures].[Unit Sales], [Measures].[Store Sales]} on columns,\n"
            + "  {[Product].children} on rows\n"
            + "FROM [Sales]\n"
            + "WHERE ([Time].[1997 H1], [Education Level].[College or higher], [Gender].[F])",
            "Axis #0:\n"
            + "{[Time].[Time].[1997 H1], [Customer].[Education Level].[College or higher], [Customer].[Gender].[F]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Store Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Products].[Drink]}\n"
            + "{[Product].[Products].[Food]}\n"
            + "{[Product].[Products].[Non-Consumable]}\n"
            + "Row #0: 1,797\n"
            + "Row #0: 3,620.49\n"
            + "Row #1: 15,002\n"
            + "Row #1: 31,931.88\n"
            + "Row #2: 3,845\n"
            + "Row #2: 8,173.22\n");
    }

    /**
     * Tests behavior where CurrentMember occurs in calculated members and
     * that member is a set.
     *
     * <p>Mosha discusses this behavior in the article
     * <a href="http://www.mosha.com/msolap/articles/mdxmultiselectcalcs.htm">
     * Multiselect friendly MDX calculations</a>.
     *
     * <p>Mondrian's behavior is consistent with MSAS 2K: it returns zeroes.
     * SSAS 2005 returns an error, which can be fixed by reformulating the
     * calculated members.
     *
     * @see mondrian.rolap.FastBatchingCellReaderTest#testAggregateDistinctCount()
     */
    public void testMultiselectCalculations() {
        assertQueryReturns(
            "WITH\n"
            + "MEMBER [Measures].[Declining Stores Count] AS\n"
            + " ' Count(Filter(Descendants(Stores.CurrentMember, Store.[Store Name]), [Store Sales] < ([Store Sales],Time.Time.PrevMember))) '\n"
            + " MEMBER \n"
            + "  [Stores].[XL_QZX] AS 'Aggregate ({ [Store].[All Stores].[USA].[WA] , [Store].[All Stores].[USA].[CA] })' \n"
            + "SELECT \n"
            + "  NON EMPTY HIERARCHIZE(AddCalculatedMembers({DrillDownLevel({[Product].[All Products]})})) \n"
            + "    DIMENSION PROPERTIES PARENT_UNIQUE_NAME ON COLUMNS \n"
            + "FROM [Sales] \n"
            + "WHERE ([Measures].[Declining Stores Count], [Time].[1998].[Q3], [Store].[XL_QZX])",
            "Axis #0:\n"
            + "{[Measures].[Declining Stores Count], [Time].[Time].[1998].[Q3], [Store].[Stores].[XL_QZX]}\n"
            + "Axis #1:\n"
            + "{[Product].[Products].[All Products]}\n"
            + "{[Product].[Products].[Drink]}\n"
            + "{[Product].[Products].[Food]}\n"
            + "{[Product].[Products].[Non-Consumable]}\n"
            + "Row #0: .00\n"
            + "Row #0: .00\n"
            + "Row #0: .00\n"
            + "Row #0: .00\n");
    }

    public void testAvg() {
        assertExprReturns(
            "AVG({[Store].[All Stores].[USA].children},[Measures].[Store Sales])",
            "188,412.71");
    }

    // todo: testAvgWithNulls

    public void testCorrelation() {
        assertExprReturns(
            "Correlation({[Store].[All Stores].[USA].children}, [Measures].[Unit Sales], [Measures].[Store Sales]) * 1000000",
            "999,906");
    }

    public void testCount() {
        getTestContext().assertExprDependsOn(
            "count(Crossjoin([Store].[All Stores].[USA].Children, {[Gender].children}), INCLUDEEMPTY)",
            setOf("[Customer].[Gender]"));

        Set<String> s1 = TestContext.allHiersExcept("[Store].[Stores]");
        getTestContext().assertExprDependsOn(
            "count(Crossjoin([Store].[All Stores].[USA].Children, "
            + "{[Gender].children}), EXCLUDEEMPTY)",
            s1);

        assertExprReturns(
            "count({[Promotion].[Media Type].[Media Type].members})", "14");

        // applied to an empty set
        assertExprReturns("count({[Gender].Parent}, IncludeEmpty)", "0");
    }

    public void testCountExcludeEmpty() {
        Set<String> s1 = TestContext.allHiersExcept("[Store].[Stores]");
        getTestContext().assertExprDependsOn(
            "count(Crossjoin([Store].[USA].Children, {[Gender].children}), EXCLUDEEMPTY)",
            s1);

        assertQueryReturns(
            "with member [Measures].[Promo Count] as \n"
            + " ' Count(Crossjoin({[Measures].[Unit Sales]},\n"
            + " {[Promotion].[Media Type].[Media Type].members}), EXCLUDEEMPTY)'\n"
            + "select {[Measures].[Unit Sales], [Measures].[Promo Count]} on columns,\n"
            + " {[Product].[Drink].[Beverages].[Carbonated Beverages].[Soda].children} on rows\n"
            + "from Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Promo Count]}\n"
            + "Axis #2:\n"
            + "{[Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Excellent]}\n"
            + "{[Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Fabulous]}\n"
            + "{[Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Skinner]}\n"
            + "{[Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Token]}\n"
            + "{[Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Washington]}\n"
            + "Row #0: 738\n"
            + "Row #0: 14\n"
            + "Row #1: 632\n"
            + "Row #1: 13\n"
            + "Row #2: 655\n"
            + "Row #2: 14\n"
            + "Row #3: 735\n"
            + "Row #3: 14\n"
            + "Row #4: 647\n"
            + "Row #4: 12\n");

        // applied to an empty set
        assertExprReturns("count({[Gender].Parent}, ExcludeEmpty)", "0");
    }

    /**
     * Tests that the 'null' value is regarded as empty, even if the underlying
     * cell has fact table rows.
     *
     * <p>For a fuller test case, see
     * {@link mondrian.xmla.XmlaCognosTest#testCognosMDXSuiteConvertedAdventureWorksToFoodMart_015()}
     */
    public void testCountExcludeEmptyNull() {
        assertQueryReturns(
            "WITH MEMBER [Measures].[Foo] AS\n"
            + "    Iif("
            + TestContext.hierarchyName("Time", "Time")
            + ".CurrentMember.Name = 'Q2', 1, NULL)\n"
            + "  MEMBER [Measures].[Bar] AS\n"
            + "    Iif("
            + TestContext.hierarchyName("Time", "Time")
            + ".CurrentMember.Name = 'Q2', 1, 0)\n"
            + "  Member [Time].[Time].[CountExc] AS\n"
            + "    Count([Time].[1997].Children, EXCLUDEEMPTY),\n"
            + "    SOLVE_ORDER = 2\n"
            + "  Member [Time].[Time].[CountInc] AS\n"
            + "    Count([Time].[1997].Children, INCLUDEEMPTY),\n"
            + "    SOLVE_ORDER = 2\n"
            + "SELECT {[Measures].[Foo],\n"
            + "   [Measures].[Bar],\n"
            + "   [Measures].[Unit Sales]} ON 0,\n"
            + "  {[Time].[1997].Children,\n"
            + "   [Time].[CountExc],\n"
            + "   [Time].[CountInc]} ON 1\n"
            + "FROM [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Foo]}\n"
            + "{[Measures].[Bar]}\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[1997].[Q1]}\n"
            + "{[Time].[Time].[1997].[Q2]}\n"
            + "{[Time].[Time].[1997].[Q3]}\n"
            + "{[Time].[Time].[1997].[Q4]}\n"
            + "{[Time].[Time].[CountExc]}\n"
            + "{[Time].[Time].[CountInc]}\n"
            + "Row #0: \n"
            + "Row #0: 0\n"
            + "Row #0: 66,291\n"
            + "Row #1: 1\n"
            + "Row #1: 1\n"
            + "Row #1: 62,610\n"
            + "Row #2: \n"
            + "Row #2: 0\n"
            + "Row #2: 65,848\n"
            + "Row #3: \n"
            + "Row #3: 0\n"
            + "Row #3: 72,024\n"
            + "Row #4: 1\n"
            + "Row #4: 4\n"
            + "Row #4: 4\n"
            + "Row #5: 4\n"
            + "Row #5: 4\n"
            + "Row #5: 4\n");
    }

    /**
     * Testcase for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-710">
     * bug MONDRIAN-710, "Count with ExcludeEmpty throws an exception when the
     * cube does not have a factCountMeasure"</a>.
     */
    public void testCountExcludeEmptyOnCubeWithNoCountFacts() {
        assertQueryReturns(
            "WITH "
            + "  MEMBER [Measures].[count] AS '"
            + "    COUNT([Store Type].[Store Type].MEMBERS, EXCLUDEEMPTY)'"
            + " SELECT "
            + "  {[Measures].[count]} ON AXIS(0)"
            + " FROM [Warehouse]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[count]}\n"
            + "Row #0: 5\n");
    }

    public void testCountExcludeEmptyOnVirtualCubeWithNoCountFacts() {
        assertQueryReturns(
            "WITH "
            + "  MEMBER [Measures].[count] AS '"
            + "    COUNT([Stores].MEMBERS, EXCLUDEEMPTY)'"
            + " SELECT "
            + "  {[Measures].[count]} ON AXIS(0)"
            + " FROM [Warehouse and Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[count]}\n"
            + "Row #0: 31\n");
    }

    // todo: testCountNull, testCountNoExp

    public void testCovariance() {
        assertExprReturns(
            "Covariance({[Store].[All Stores].[USA].children}, [Measures].[Unit Sales], [Measures].[Store Sales])",
            "1,355,761,899");
    }

    public void testCovarianceN() {
        assertExprReturns(
            "CovarianceN({[Store].[All Stores].[USA].children}, [Measures].[Unit Sales], [Measures].[Store Sales])",
            "2,033,642,849");
    }

    public void testIIfNumeric() {
        assertExprReturns(
            "IIf(([Measures].[Unit Sales],[Product].[Drink].[Alcoholic Beverages].[Beer and Wine]) > 100, 45, 32)",
            "45");

        // Compare two members. The system needs to figure out that they are
        // both numeric, and use the right overloaded version of ">", otherwise
        // we'll get a ClassCastException at runtime.
        assertExprReturns(
            "IIf([Measures].[Unit Sales] > [Measures].[Store Sales], 45, 32)",
            "32");
    }

    public void testMax() {
        assertExprReturns(
            "MAX({[Store].[All Stores].[USA].children},[Measures].[Store Sales])",
            "263,793.22");
    }

    public void testMaxNegative() {
        // Bug 1771928, "Max() works incorrectly with negative values"
        assertQueryReturns(
            "with \n"
            + "  member [Customers].[Neg] as '-1'\n"
            + "  member [Customers].[Min] as 'Min({[Customers].[Neg]})'\n"
            + "  member [Customers].[Max] as 'Max({[Customers].[Neg]})'\n"
            + "select {[Customers].[Neg],[Customers].[Min],[Customers].[Max]} on 0\n"
            + "from Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Customer].[Customers].[Neg]}\n"
            + "{[Customer].[Customers].[Min]}\n"
            + "{[Customer].[Customers].[Max]}\n"
            + "Row #0: -1\n"
            + "Row #0: -1\n"
            + "Row #0: -1\n");
    }

    public void testMedian() {
        assertExprReturns(
            "MEDIAN({[Store].[All Stores].[USA].children},"
            + "[Measures].[Store Sales])",
            "159,167.84");
        // single value
        assertExprReturns(
            "MEDIAN({[Store].[All Stores].[USA]}, [Measures].[Store Sales])",
            "565,238.13");
    }

    public void testMedian2() {
        assertQueryReturns(
            "WITH\n"
            + "   Member [Time].[Time].[1st Half Sales] AS 'Sum({[Time].[1997].[Q1], [Time].[1997].[Q2]})'\n"
            + "   Member [Time].[Time].[2nd Half Sales] AS 'Sum({[Time].[1997].[Q3], [Time].[1997].[Q4]})'\n"
            + "   Member [Time].[Time].[Median] AS 'Median(Time.[Time].Members)'\n"
            + "SELECT\n"
            + "   NON EMPTY { [Store].[Store Name].Members} ON COLUMNS,\n"
            + "   { [Time].[1st Half Sales], [Time].[2nd Half Sales], [Time].[Median]} ON ROWS\n"
            + "FROM Sales\n"
            + "WHERE [Measures].[Store Sales]",

            "Axis #0:\n"
            + "{[Measures].[Store Sales]}\n"
            + "Axis #1:\n"
            + "{[Store].[Stores].[USA].[CA].[Beverly Hills].[Store 6]}\n"
            + "{[Store].[Stores].[USA].[CA].[Los Angeles].[Store 7]}\n"
            + "{[Store].[Stores].[USA].[CA].[San Diego].[Store 24]}\n"
            + "{[Store].[Stores].[USA].[CA].[San Francisco].[Store 14]}\n"
            + "{[Store].[Stores].[USA].[OR].[Portland].[Store 11]}\n"
            + "{[Store].[Stores].[USA].[OR].[Salem].[Store 13]}\n"
            + "{[Store].[Stores].[USA].[WA].[Bellingham].[Store 2]}\n"
            + "{[Store].[Stores].[USA].[WA].[Bremerton].[Store 3]}\n"
            + "{[Store].[Stores].[USA].[WA].[Seattle].[Store 15]}\n"
            + "{[Store].[Stores].[USA].[WA].[Spokane].[Store 16]}\n"
            + "{[Store].[Stores].[USA].[WA].[Tacoma].[Store 17]}\n"
            + "{[Store].[Stores].[USA].[WA].[Walla Walla].[Store 22]}\n"
            + "{[Store].[Stores].[USA].[WA].[Yakima].[Store 23]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[1st Half Sales]}\n"
            + "{[Time].[Time].[2nd Half Sales]}\n"
            + "{[Time].[Time].[Median]}\n"
            + "Row #0: 20,801.04\n"
            + "Row #0: 25,421.41\n"
            + "Row #0: 26,275.11\n"
            + "Row #0: 2,074.39\n"
            + "Row #0: 28,519.18\n"
            + "Row #0: 43,423.99\n"
            + "Row #0: 2,140.99\n"
            + "Row #0: 25,502.08\n"
            + "Row #0: 25,293.50\n"
            + "Row #0: 23,265.53\n"
            + "Row #0: 34,926.91\n"
            + "Row #0: 2,159.60\n"
            + "Row #0: 12,490.89\n"
            + "Row #1: 24,949.20\n"
            + "Row #1: 29,123.87\n"
            + "Row #1: 28,156.03\n"
            + "Row #1: 2,366.79\n"
            + "Row #1: 26,539.61\n"
            + "Row #1: 43,794.29\n"
            + "Row #1: 2,598.24\n"
            + "Row #1: 27,394.22\n"
            + "Row #1: 27,350.57\n"
            + "Row #1: 26,368.93\n"
            + "Row #1: 39,917.05\n"
            + "Row #1: 2,546.37\n"
            + "Row #1: 11,838.34\n"
            + "Row #2: 4,577.35\n"
            + "Row #2: 5,211.38\n"
            + "Row #2: 4,722.87\n"
            + "Row #2: 398.24\n"
            + "Row #2: 5,039.50\n"
            + "Row #2: 7,374.59\n"
            + "Row #2: 410.22\n"
            + "Row #2: 4,924.04\n"
            + "Row #2: 4,569.13\n"
            + "Row #2: 4,511.68\n"
            + "Row #2: 6,630.91\n"
            + "Row #2: 419.51\n"
            + "Row #2: 2,169.48\n");
    }

    public void testPercentile() {
        // same result as median
        assertExprReturns(
            "Percentile({[Store].[All Stores].[USA].children}, [Measures].[Store Sales], 50)",
            "159,167.84");
        // same result as min
        assertExprReturns(
            "Percentile({[Store].[All Stores].[USA].children}, [Measures].[Store Sales], 0)",
            "142,277.07");
        // same result as max
        assertExprReturns(
            "Percentile({[Store].[All Stores].[USA].children}, [Measures].[Store Sales], 100)",
            "263,793.22");

        // check some real percentile cases
        assertExprReturns(
            "Percentile({[Store].[All Stores].[USA].[WA].children}, [Measures].[Store Sales], 50)",
            "49,634.46");
        // lets return the second element of the 7 children 4,739.23
        assertExprReturns(
            "Percentile({[Store].[All Stores].[USA].[WA].children}, [Measures].[Store Sales], 100/7*2)",
            "4,739.23");
        assertExprReturns(
            "Percentile({[Store].[All Stores].[USA].[WA].children}, [Measures].[Store Sales], 95)",
            "67,162.28");
    }

    /**
     * Testcase for bug
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1045">MONDRIAN-1045,
     * "When I use the Percentile function it cracks when there's only
     * 1 register"</a>.
     */
    public void testPercentileBugMondrian1045() {
        assertExprReturns(
            "Percentile({[Store].[All Stores].[USA]}, [Measures].[Store Sales], 50)",
            "565,238.13");
        assertExprReturns(
            "Percentile({[Store].[All Stores].[USA]}, [Measures].[Store Sales], 40)",
            "565,238.13");
        assertExprReturns(
            "Percentile({[Store].[All Stores].[USA]}, [Measures].[Store Sales], 95)",
            "565,238.13");
    }

    public void testMin() {
        assertExprReturns(
            "MIN({[Store].[All Stores].[USA].children},[Measures].[Store Sales])",
            "142,277.07");
    }

    public void testMinTuple() {
        assertExprReturns(
            "Min([Customers].[All Customers].[USA].Children, ([Measures].[Unit Sales], [Gender].[All Gender].[F]))",
            "33,036");
    }

    public void testStdev() {
        assertExprReturns(
            "STDEV({[Store].[All Stores].[USA].children},[Measures].[Store Sales])",
            "65,825.45");
    }

    public void testStdevP() {
        assertExprReturns(
            "STDEVP({[Store].[All Stores].[USA].children},[Measures].[Store Sales])",
            "53,746.26");
    }

    public void testSumNoExp() {
        assertExprReturns(
            "SUM({[Promotion].[Media Type].[Media Type].members})", "266,773");
    }

    public void testValue() {
        // VALUE is usually a cell property, not a member property.
        // We allow it because MS documents it as a function, <Member>.VALUE.
        assertExprReturns("[Measures].[Store Sales].VALUE", "565,238.13");

        // Depends upon almost everything.
        Set<String> s1 = TestContext.allHiersExcept("[Measures]");
        getTestContext().assertExprDependsOn(
            "[Measures].[Store Sales].VALUE", s1);

        // We do not allow FORMATTED_VALUE.
        assertExprThrows(
            "[Measures].[Store Sales].FORMATTED_VALUE",
            "MDX object '[Measures].[Store Sales].FORMATTED_VALUE' not found in cube 'Sales'");

        assertExprReturns("[Measures].[Store Sales].NAME", "Store Sales");
        // MS says that ID and KEY are standard member properties for
        // OLE DB for OLAP, but not for XML/A. We don't support them.
        assertExprThrows(
            "[Measures].[Store Sales].ID",
            "MDX object '[Measures].[Store Sales].ID' not found in cube 'Sales'");

        // Error for KEY is slightly different than for ID. It doesn't matter
        // very much.
        //
        // The error is different because KEY is registered as a Mondrian
        // builtin property, but ID isn't. KEY cannot be evaluated in
        // "<MEMBER>.KEY" syntax because there is not function defined. For
        // other builtin properties, such as NAME, CAPTION there is a builtin
        // function.
        assertExprThrows(
            "[Measures].[Store Sales].KEY",
            "No function matches signature '<Member>.KEY'");

        assertExprReturns("[Measures].[Store Sales].CAPTION", "Store Sales");
    }

    public void testVar() {
        assertExprReturns(
            "VAR({[Store].[All Stores].[USA].children},[Measures].[Store Sales])",
            "4,332,990,493.69");
    }

    public void testVarP() {
        assertExprReturns(
            "VARP({[Store].[All Stores].[USA].children},[Measures].[Store Sales])",
            "2,888,660,329.13");
    }

    /**
     * Tests the AS operator, that gives an expression an alias.
     */
    public void testAs() {
        assertAxisReturns(
            "Filter([Customers].Children as t,\n"
            + "t.Current.Name = 'USA')",
            "[Customer].[Customers].[USA]");

        // 'AS' and the ':' operator have similar precedence, so it's worth
        // checking that they play nice.
        assertQueryReturns(
            "select\n"
            + "  filter(\n"
            + "    [Time].[1997].[Q1].[2] : [Time].[1997].[Q3].[9] as t,"
            + "    mod(t.CurrentOrdinal, 2) = 0) on 0\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time].[Time].[1997].[Q1].[2]}\n"
            + "{[Time].[Time].[1997].[Q2].[4]}\n"
            + "{[Time].[Time].[1997].[Q2].[6]}\n"
            + "{[Time].[Time].[1997].[Q3].[8]}\n"
            + "Row #0: 20,957\n"
            + "Row #0: 20,179\n"
            + "Row #0: 21,350\n"
            + "Row #0: 21,697\n");

        // AS member fails on SSAS with "The CHILDREN function expects a member
        // expression for the 0 argument. A tuple set expression was used."
        assertQueryThrows(
            "select\n"
            + " {([Time].[1997].[Q1] as t).Children, \n"
            + "  t.Parent } on 0 \n"
            + "from [Sales]",
            "No function matches signature '<Set>.Children'");

        // Set of members. OK.
        assertQueryReturns(
            "select Measures.[Unit Sales] on 0, \n"
            + "  {[Time].[1997].Children as t, \n"
            + "   Descendants(t, [Time].[Month])} on 1 \n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[1997].[Q1]}\n"
            + "{[Time].[Time].[1997].[Q2]}\n"
            + "{[Time].[Time].[1997].[Q3]}\n"
            + "{[Time].[Time].[1997].[Q4]}\n"
            + "{[Time].[Time].[1997].[Q1].[1]}\n"
            + "{[Time].[Time].[1997].[Q1].[2]}\n"
            + "{[Time].[Time].[1997].[Q1].[3]}\n"
            + "{[Time].[Time].[1997].[Q2].[4]}\n"
            + "{[Time].[Time].[1997].[Q2].[5]}\n"
            + "{[Time].[Time].[1997].[Q2].[6]}\n"
            + "{[Time].[Time].[1997].[Q3].[7]}\n"
            + "{[Time].[Time].[1997].[Q3].[8]}\n"
            + "{[Time].[Time].[1997].[Q3].[9]}\n"
            + "{[Time].[Time].[1997].[Q4].[10]}\n"
            + "{[Time].[Time].[1997].[Q4].[11]}\n"
            + "{[Time].[Time].[1997].[Q4].[12]}\n"
            + "Row #0: 66,291\n"
            + "Row #1: 62,610\n"
            + "Row #2: 65,848\n"
            + "Row #3: 72,024\n"
            + "Row #4: 21,628\n"
            + "Row #5: 20,957\n"
            + "Row #6: 23,706\n"
            + "Row #7: 20,179\n"
            + "Row #8: 21,081\n"
            + "Row #9: 21,350\n"
            + "Row #10: 23,763\n"
            + "Row #11: 21,697\n"
            + "Row #12: 20,388\n"
            + "Row #13: 19,958\n"
            + "Row #14: 25,270\n"
            + "Row #15: 26,796\n");

        // Alias a member. Implicitly becomes set. OK.
        assertQueryReturns(
            "select Measures.[Unit Sales] on 0,\n"
            + "  {[Time].[1997] as t,\n"
            + "   Descendants(t, [Time].[Month])} on 1\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[1997]}\n"
            + "{[Time].[Time].[1997].[Q1].[1]}\n"
            + "{[Time].[Time].[1997].[Q1].[2]}\n"
            + "{[Time].[Time].[1997].[Q1].[3]}\n"
            + "{[Time].[Time].[1997].[Q2].[4]}\n"
            + "{[Time].[Time].[1997].[Q2].[5]}\n"
            + "{[Time].[Time].[1997].[Q2].[6]}\n"
            + "{[Time].[Time].[1997].[Q3].[7]}\n"
            + "{[Time].[Time].[1997].[Q3].[8]}\n"
            + "{[Time].[Time].[1997].[Q3].[9]}\n"
            + "{[Time].[Time].[1997].[Q4].[10]}\n"
            + "{[Time].[Time].[1997].[Q4].[11]}\n"
            + "{[Time].[Time].[1997].[Q4].[12]}\n"
            + "Row #0: 266,773\n"
            + "Row #1: 21,628\n"
            + "Row #2: 20,957\n"
            + "Row #3: 23,706\n"
            + "Row #4: 20,179\n"
            + "Row #5: 21,081\n"
            + "Row #6: 21,350\n"
            + "Row #7: 23,763\n"
            + "Row #8: 21,697\n"
            + "Row #9: 20,388\n"
            + "Row #10: 19,958\n"
            + "Row #11: 25,270\n"
            + "Row #12: 26,796\n");

        // Alias a tuple. Implicitly becomes set. The error confirms that the
        // named set's type is a set of tuples. SSAS gives error "Descendants
        // function expects a member or set ..."
        assertQueryThrows(
            "select Measures.[Unit Sales] on 0,\n"
            + "  {([Time].[1997], [Customers].[USA].[CA]) as t,\n"
            + "   Descendants(t, [Time].[Month])} on 1\n"
            + "from [Sales]",
            "Argument to Descendants function must be a member or set of members, not a set of tuples");
    }

    public void testAs2() {
        // Named set and alias with same name (t) and a second alias (t2).
        // Reference to t from within descendants resolves to alias, of type
        // [Time], because it is nearer.
        final String result =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales], [Customer].[Gender].[F]}\n"
            + "{[Measures].[Unit Sales], [Customer].[Gender].[M]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[1997].[Q1]}\n"
            + "{[Time].[Time].[1997].[Q2]}\n"
            + "{[Time].[Time].[1997].[Q3]}\n"
            + "{[Time].[Time].[1997].[Q4]}\n"
            + "{[Time].[Time].[1997].[Q1].[1]}\n"
            + "{[Time].[Time].[1997].[Q2].[6]}\n"
            + "{[Time].[Time].[1997].[Q4].[11]}\n"
            + "Row #0: 32,910\n"
            + "Row #0: 33,381\n"
            + "Row #1: 30,992\n"
            + "Row #1: 31,618\n"
            + "Row #2: 32,599\n"
            + "Row #2: 33,249\n"
            + "Row #3: 35,057\n"
            + "Row #3: 36,967\n"
            + "Row #4: 10,932\n"
            + "Row #4: 10,696\n"
            + "Row #5: 10,466\n"
            + "Row #5: 10,884\n"
            + "Row #6: 12,320\n"
            + "Row #6: 12,950\n";
        assertQueryReturns(
            "with set t as [Gender].Children\n"
            + "select\n"
            + "  Measures.[Unit Sales] * t on 0,\n"
            + "  {\n"
            + "    [Time].[1997].Children as t,\n"
            + "    Filter(\n"
            + "      Descendants(t, [Time].[Month]) as t2,\n"
            + "      Mod(t2.CurrentOrdinal, 5) = 0)\n"
            + "  } on 1\n"
            + "from [Sales]",
            result);

        // Two aliases with same name. OK.
        assertQueryReturns(
            "select\n"
            + "  Measures.[Unit Sales] * [Gender].Children as t on 0,\n"
            + "  {[Time].[1997].Children as t,\n"
            + "    Filter(\n"
            + "      Descendants(t, [Time].[Month]) as t2,\n"
            + "      Mod(t2.CurrentOrdinal, 5) = 0)\n"
            + "  } on 1\n"
            + "from [Sales]",
            result);

        // Bug MONDRIAN-648 causes 'AS' to have lower precedence than '*'.
        if (Bug.BugMondrian648Fixed) {
            // Note that 'as' has higher precedence than '*'.
            assertQueryReturns(
                "select\n"
                + "  Measures.[Unit Sales] * [Gender].Members as t on 0,\n"
                + "  {t} on 1\n"
                + "from [Sales]",
                "xxxxx");
        }

        // Reference to hierarchy on other axis.
        // On SSAS 2005, finds t, and gives error,
        // "The Gender hierarchy already appears in the Axis0 axis."
        // On Mondrian, cannot find t. FIXME.
        assertQueryThrows(
            "select\n"
            + "  Measures.[Unit Sales] * ([Gender].Members as t) on 0,\n"
            + "  {t} on 1\n"
            + "from [Sales]",
            "MDX object 't' not found in cube 'Sales'");

        // As above, with parentheses. Tuple valued.
        // On SSAS 2005, finds t, and gives error,
        // "The Measures hierarchy already appears in the Axis0 axis."
        // On Mondrian, cannot find t. FIXME.
        assertQueryThrows(
            "select\n"
            + "  (Measures.[Unit Sales] * [Gender].Members) as t on 0,\n"
            + "  {t} on 1\n"
            + "from [Sales]",
            "MDX object 't' not found in cube 'Sales'");

        // Calculated set, CurrentMember
        assertQueryReturns(
            "select Measures.[Unit Sales] on 0,\n"
            + "  filter(\n"
            + "    (Time.Month.Members * Gender.Members) as s,\n"
            + "    (s.Current.Item(0).Parent, [Marital Status].[S], [Gender].[F]) > 17000) on 1\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[1997].[Q4].[10], [Customer].[Gender].[All Gender]}\n"
            + "{[Time].[Time].[1997].[Q4].[10], [Customer].[Gender].[F]}\n"
            + "{[Time].[Time].[1997].[Q4].[10], [Customer].[Gender].[M]}\n"
            + "{[Time].[Time].[1997].[Q4].[11], [Customer].[Gender].[All Gender]}\n"
            + "{[Time].[Time].[1997].[Q4].[11], [Customer].[Gender].[F]}\n"
            + "{[Time].[Time].[1997].[Q4].[11], [Customer].[Gender].[M]}\n"
            + "{[Time].[Time].[1997].[Q4].[12], [Customer].[Gender].[All Gender]}\n"
            + "{[Time].[Time].[1997].[Q4].[12], [Customer].[Gender].[F]}\n"
            + "{[Time].[Time].[1997].[Q4].[12], [Customer].[Gender].[M]}\n"
            + "Row #0: 19,958\n"
            + "Row #1: 9,506\n"
            + "Row #2: 10,452\n"
            + "Row #3: 25,270\n"
            + "Row #4: 12,320\n"
            + "Row #5: 12,950\n"
            + "Row #6: 26,796\n"
            + "Row #7: 13,231\n"
            + "Row #8: 13,565\n");

        // As above, but don't override [Gender] in filter condition. Note that
        // the filter condition is evaluated in the context created by the
        // filter set. So, only items with [All Gender] pass the filter.
        assertQueryReturns(
            "select Measures.[Unit Sales] on 0,\n"
            + "  filter(\n"
            + "    (Time.Month.Members * Gender.Members) as s,\n"
            + "    (s.Current.Item(0).Parent, [Marital Status].[S]) > 35000) on 1\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[1997].[Q4].[10], [Customer].[Gender].[All Gender]}\n"
            + "{[Time].[Time].[1997].[Q4].[11], [Customer].[Gender].[All Gender]}\n"
            + "{[Time].[Time].[1997].[Q4].[12], [Customer].[Gender].[All Gender]}\n"
            + "Row #0: 19,958\n"
            + "Row #1: 25,270\n"
            + "Row #2: 26,796\n");

        // Multiple definitions of alias within same axis
        assertQueryReturns(
            "select Measures.[Unit Sales] on 0,\n"
            + "  generate(\n"
            + "    [Marital Status].Children as s,\n"
            + "    filter(\n"
            + "      (Time.Month.Members * Gender.Members) as s,\n"
            + "      (s.Current.Item(0).Parent, [Marital Status].[S], [Gender].[F]) > 17000),\n"
            + "    ALL) on 1\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[1997].[Q4].[10], [Customer].[Gender].[All Gender]}\n"
            + "{[Time].[Time].[1997].[Q4].[10], [Customer].[Gender].[F]}\n"
            + "{[Time].[Time].[1997].[Q4].[10], [Customer].[Gender].[M]}\n"
            + "{[Time].[Time].[1997].[Q4].[11], [Customer].[Gender].[All Gender]}\n"
            + "{[Time].[Time].[1997].[Q4].[11], [Customer].[Gender].[F]}\n"
            + "{[Time].[Time].[1997].[Q4].[11], [Customer].[Gender].[M]}\n"
            + "{[Time].[Time].[1997].[Q4].[12], [Customer].[Gender].[All Gender]}\n"
            + "{[Time].[Time].[1997].[Q4].[12], [Customer].[Gender].[F]}\n"
            + "{[Time].[Time].[1997].[Q4].[12], [Customer].[Gender].[M]}\n"
            + "{[Time].[Time].[1997].[Q4].[10], [Customer].[Gender].[All Gender]}\n"
            + "{[Time].[Time].[1997].[Q4].[10], [Customer].[Gender].[F]}\n"
            + "{[Time].[Time].[1997].[Q4].[10], [Customer].[Gender].[M]}\n"
            + "{[Time].[Time].[1997].[Q4].[11], [Customer].[Gender].[All Gender]}\n"
            + "{[Time].[Time].[1997].[Q4].[11], [Customer].[Gender].[F]}\n"
            + "{[Time].[Time].[1997].[Q4].[11], [Customer].[Gender].[M]}\n"
            + "{[Time].[Time].[1997].[Q4].[12], [Customer].[Gender].[All Gender]}\n"
            + "{[Time].[Time].[1997].[Q4].[12], [Customer].[Gender].[F]}\n"
            + "{[Time].[Time].[1997].[Q4].[12], [Customer].[Gender].[M]}\n"
            + "Row #0: 19,958\n"
            + "Row #1: 9,506\n"
            + "Row #2: 10,452\n"
            + "Row #3: 25,270\n"
            + "Row #4: 12,320\n"
            + "Row #5: 12,950\n"
            + "Row #6: 26,796\n"
            + "Row #7: 13,231\n"
            + "Row #8: 13,565\n"
            + "Row #9: 19,958\n"
            + "Row #10: 9,506\n"
            + "Row #11: 10,452\n"
            + "Row #12: 25,270\n"
            + "Row #13: 12,320\n"
            + "Row #14: 12,950\n"
            + "Row #15: 26,796\n"
            + "Row #16: 13,231\n"
            + "Row #17: 13,565\n");

        // Multiple definitions of alias within same axis.
        //
        // On SSAS 2005, gives error, "The CURRENT function cannot be called in
        // current context because the 'x' set is not in scope". SSAS 2005 gives
        // same error even if set does not exist.
        assertQueryThrows(
            "with member Measures.Foo as 'x.Current.Name'\n"
            + "select\n"
            + "  {Measures.[Unit Sales], Measures.Foo} on 0,\n"
            + "  generate(\n"
            + "    [Marital Status].\n"
            + "    Children as x,\n"
            + "    filter(\n"
            + "      Gender.Members as x,\n"
            + "      (x.Current, [Marital Status].[S]) > 50000),\n"
            + "    ALL) on 1\n"
            + "from [Sales]",
            "MDX object 'x' not found in cube 'Sales'");

        // As above, but set is not out of scope; it does not exist; but error
        // should be the same.
        assertQueryThrows(
            "with member Measures.Foo as 'z.Current.Name'\n"
            + "select\n"
            + "  {Measures.[Unit Sales], Measures.Foo} on 0,\n"
            + "  generate(\n"
            + "    [Marital Status].\n"
            + "    Children as s,\n"
            + "    filter(\n"
            + "      Gender.Members as s,\n"
            + "      (s.Current, [Marital Status].[S]) > 50000),\n"
            + "    ALL) on 1\n"
            + "from [Sales]",
            "MDX object 'z' not found in cube 'Sales'");

        // 'set AS string' is invalid
        assertQueryThrows(
            "select Measures.[Unit Sales] on 0,\n"
            + "  filter(\n"
            + "    (Time.Month.Members * Gender.Members) as 'foo',\n"
            + "    (s.Current.Item(0).Parent, [Marital Status].[S]) > 50000) on 1\n"
            + "from [Sales]",
            "Syntax error at line 3, column 46, token ''foo''");

        // 'set AS numeric' is invalid
        assertQueryThrows(
            "select Measures.[Unit Sales] on 0,\n"
            + "  filter(\n"
            + "    (Time.Month.Members * Gender.Members) as 1234,\n"
            + "    (s.Current.Item(0).Parent, [Marital Status].[S]) > 50000) on 1\n"
            + "from [Sales]",
            "Syntax error at line 3, column 46, token '1234'");

        // 'numeric AS identifier' is invalid
        assertQueryThrows(
            "select Measures.[Unit Sales] on 0,\n"
            + "  filter(\n"
            + "    123 * 456 as s,\n"
            + "    (s.Current.Item(0).Parent, [Marital Status].[S]) > 50000) on 1\n"
            + "from [Sales]",
            "No function matches signature '<Numeric Expression> AS <Set>'");
    }

    public void testAscendants() {
        assertAxisReturns(
            "Ascendants([Store].[USA].[CA])",
            "[Store].[Stores].[USA].[CA]\n"
            + "[Store].[Stores].[USA]\n"
            + "[Store].[Stores].[All Stores]");
    }

    public void testAscendantsAll() {
        assertAxisReturns(
            "Ascendants([Store].[Stores].DefaultMember)",
            "[Store].[Stores].[All Stores]");
    }

    public void testAscendantsNull() {
        assertAxisReturns(
            "Ascendants([Gender].[F].PrevMember)", "");
    }

    public void testBottomCount() {
        assertAxisReturns(
            "BottomCount({[Promotion].[Media Type].members}, 2, [Measures].[Unit Sales])",
            "[Promotion].[Media Type].[Radio]\n"
            + "[Promotion].[Media Type].[Sunday Paper, Radio, TV]");
    }

    // todo: test unordered

    public void testBottomPercent() {
        assertAxisReturns(
            "BottomPercent(Filter({[Store].[All Stores].[USA].[CA].Children, [Store].[All Stores].[USA].[OR].Children, [Store].[All Stores].[USA].[WA].Children}, ([Measures].[Unit Sales] > 0.0)), 100.0, [Measures].[Store Sales])",
            "[Store].[Stores].[USA].[CA].[San Francisco]\n"
            + "[Store].[Stores].[USA].[WA].[Walla Walla]\n"
            + "[Store].[Stores].[USA].[WA].[Bellingham]\n"
            + "[Store].[Stores].[USA].[WA].[Yakima]\n"
            + "[Store].[Stores].[USA].[CA].[Beverly Hills]\n"
            + "[Store].[Stores].[USA].[WA].[Spokane]\n"
            + "[Store].[Stores].[USA].[WA].[Seattle]\n"
            + "[Store].[Stores].[USA].[WA].[Bremerton]\n"
            + "[Store].[Stores].[USA].[CA].[San Diego]\n"
            + "[Store].[Stores].[USA].[CA].[Los Angeles]\n"
            + "[Store].[Stores].[USA].[OR].[Portland]\n"
            + "[Store].[Stores].[USA].[WA].[Tacoma]\n"
            + "[Store].[Stores].[USA].[OR].[Salem]");

        assertAxisReturns(
            "BottomPercent({[Promotion].[Media Type].[Media Type].members}, 1, [Measures].[Unit Sales])",
            "[Promotion].[Media Type].[Radio]\n"
            + "[Promotion].[Media Type].[Sunday Paper, Radio, TV]");
    }

    // todo: test precision

    public void testBottomSum() {
        assertAxisReturns(
            "BottomSum({[Promotion].[Media Type].members}, 5000, [Measures].[Unit Sales])",
            "[Promotion].[Media Type].[Radio]\n"
            + "[Promotion].[Media Type].[Sunday Paper, Radio, TV]");
    }

    public void testExceptEmpty() {
        // If left is empty, result is empty.
        assertAxisReturns(
            "Except(Filter([Gender].Members, 1=0), {[Gender].[M]})", "");

        // If right is empty, result is left.
        assertAxisReturns(
            "Except({[Gender].[M]}, Filter([Gender].Members, 1=0))",
            "[Customer].[Gender].[M]");
    }

    /**
     * Tests that Except() successfully removes crossjoined tuples
     * from the axis results.  Previously, this would fail by returning
     * all tuples in the first argument to Except.  bug 1439627
     */
    public void testExceptCrossjoin() {
        assertAxisReturns(
            "Except(CROSSJOIN({[Promotion].[Media Type].[All Media]},\n"
            + "                  [Product].[Products].Children),\n"
            + "       CROSSJOIN({[Promotion].[Media Type].[All Media]},\n"
            + "                  {[Product].[Products].[Drink]}))",
            "{[Promotion].[Media Type].[All Media], [Product].[Products].[Food]}\n"
            + "{[Promotion].[Media Type].[All Media], [Product].[Products].[Non-Consumable]}");
    }

    public void testExtract() {
        assertAxisReturns(
            "Extract(\n"
            + "Crossjoin({[Gender].[F], [Gender].[M]},\n"
            + "          {[Marital Status].Members}),\n"
            + "[Gender])",
            "[Customer].[Gender].[F]\n"
            + "[Customer].[Gender].[M]");

        // Extract(<set>) with no dimensions is not valid
        assertAxisThrows(
            "Extract(Crossjoin({[Gender].[F], [Gender].[M]}, {[Marital Status].Members}))",
            "No function matches signature 'Extract(<Set>)'");

        // Extract applied to non-constant dimension should fail
        assertAxisThrows(
            "Extract(Crossjoin([Gender].Members, [Store].[Stores].Children), [Store].[Stores].Hierarchy.Dimension)",
            "not a constant hierarchy: [Store].[Stores].Hierarchy.Dimension");

        // Extract applied to non-constant hierarchy should fail
        assertAxisThrows(
            "Extract(Crossjoin([Gender].Members, [Store].[Stores].Children), [Store].Hierarchy)",
            "not a constant hierarchy: [Store].Hierarchy");

        // Extract applied to set of members is OK (if silly). Duplicates are
        // removed, as always.
        assertAxisReturns(
            "Extract({[Gender].[M], [Gender].Members}, [Gender])",
            "[Customer].[Gender].[M]\n"
            + "[Customer].[Gender].[All Gender]\n"
            + "[Customer].[Gender].[F]");

        // Extract of hierarchy not in set fails
        assertAxisThrows(
            "Extract(Crossjoin([Gender].Members, [Store].[Stores].Children), [Marital Status])",
            "hierarchy [Customer].[Marital Status] is not a hierarchy of the expression Crossjoin([Customer].[Gender].Members, [Store].[Stores].Children)");

        // Extract applied to empty set returns empty set
        assertAxisReturns(
            "Extract(Crossjoin({[Gender].Parent}, [Store].[Stores].Children), [Store])",
            "");

        // Extract applied to asymmetric set
        assertAxisReturns(
            "Extract(\n"
            + "{([Gender].[M], [Marital Status].[M]),\n"
            + " ([Gender].[F], [Marital Status].[M]),\n"
            + " ([Gender].[M], [Marital Status].[S])},\n"
            + "[Customer].[Gender])",
            "[Customer].[Gender].[M]\n"
            + "[Customer].[Gender].[F]");

        // Extract applied to asymmetric set (other side)
        assertAxisReturns(
            "Extract(\n"
            + "{([Gender].[M], [Marital Status].[M]),\n"
            + " ([Gender].[F], [Marital Status].[M]),\n"
            + " ([Gender].[M], [Marital Status].[S])},\n"
            + "[Marital Status])",
            "[Customer].[Marital Status].[M]\n"
            + "[Customer].[Marital Status].[S]");

        // Extract more than one hierarchy
        assertAxisReturns(
            "Extract(\n"
            + "[Gender].Children * [Marital Status].Children * [Time].[1997].Children * [Store].[USA].Children,\n"
            + "[Time], [Marital Status])",
            "{[Time].[Time].[1997].[Q1], [Customer].[Marital Status].[M]}\n"
            + "{[Time].[Time].[1997].[Q2], [Customer].[Marital Status].[M]}\n"
            + "{[Time].[Time].[1997].[Q3], [Customer].[Marital Status].[M]}\n"
            + "{[Time].[Time].[1997].[Q4], [Customer].[Marital Status].[M]}\n"
            + "{[Time].[Time].[1997].[Q1], [Customer].[Marital Status].[S]}\n"
            + "{[Time].[Time].[1997].[Q2], [Customer].[Marital Status].[S]}\n"
            + "{[Time].[Time].[1997].[Q3], [Customer].[Marital Status].[S]}\n"
            + "{[Time].[Time].[1997].[Q4], [Customer].[Marital Status].[S]}");

        // Extract duplicate hierarchies fails
        assertAxisThrows(
            "Extract(\n"
            + "{([Gender].[M], [Marital Status].[M]),\n"
            + " ([Gender].[F], [Marital Status].[M]),\n"
            + " ([Gender].[M], [Marital Status].[S])},\n"
            + "[Gender], [Gender])",
            "hierarchy [Customer].[Gender] is extracted more than once");
    }

    /**
     * Tests that TopPercent() operates succesfully on a
     * axis of crossjoined tuples.  previously, this would
     * fail with a ClassCastException in FunUtil.java.  bug 1440306
     */
    public void testTopPercentCrossjoin() {
        assertAxisReturns(
            "{TopPercent(Crossjoin([Product].[Product Department].members,\n"
            + "[Time].[1997].children),10,[Measures].[Store Sales])}",
            "{[Product].[Products].[Food].[Produce], [Time].[Time].[1997].[Q4]}\n"
            + "{[Product].[Products].[Food].[Produce], [Time].[Time].[1997].[Q1]}\n"
            + "{[Product].[Products].[Food].[Produce], [Time].[Time].[1997].[Q3]}");
    }

    public void testCrossjoinNested() {
        assertAxisReturns(
            "  CrossJoin(\n"
            + "    CrossJoin(\n"
            + "      [Gender].members,\n"
            + "      [Marital Status].members),\n"
            + "   {[Store].[Stores], [Store].[Stores].children})",

            "{[Customer].[Gender].[All Gender], [Customer].[Marital Status].[All Marital Status], [Store].[Stores].[All Stores]}\n"
            + "{[Customer].[Gender].[All Gender], [Customer].[Marital Status].[All Marital Status], [Store].[Stores].[Canada]}\n"
            + "{[Customer].[Gender].[All Gender], [Customer].[Marital Status].[All Marital Status], [Store].[Stores].[Mexico]}\n"
            + "{[Customer].[Gender].[All Gender], [Customer].[Marital Status].[All Marital Status], [Store].[Stores].[USA]}\n"
            + "{[Customer].[Gender].[All Gender], [Customer].[Marital Status].[M], [Store].[Stores].[All Stores]}\n"
            + "{[Customer].[Gender].[All Gender], [Customer].[Marital Status].[M], [Store].[Stores].[Canada]}\n"
            + "{[Customer].[Gender].[All Gender], [Customer].[Marital Status].[M], [Store].[Stores].[Mexico]}\n"
            + "{[Customer].[Gender].[All Gender], [Customer].[Marital Status].[M], [Store].[Stores].[USA]}\n"
            + "{[Customer].[Gender].[All Gender], [Customer].[Marital Status].[S], [Store].[Stores].[All Stores]}\n"
            + "{[Customer].[Gender].[All Gender], [Customer].[Marital Status].[S], [Store].[Stores].[Canada]}\n"
            + "{[Customer].[Gender].[All Gender], [Customer].[Marital Status].[S], [Store].[Stores].[Mexico]}\n"
            + "{[Customer].[Gender].[All Gender], [Customer].[Marital Status].[S], [Store].[Stores].[USA]}\n"
            + "{[Customer].[Gender].[F], [Customer].[Marital Status].[All Marital Status], [Store].[Stores].[All Stores]}\n"
            + "{[Customer].[Gender].[F], [Customer].[Marital Status].[All Marital Status], [Store].[Stores].[Canada]}\n"
            + "{[Customer].[Gender].[F], [Customer].[Marital Status].[All Marital Status], [Store].[Stores].[Mexico]}\n"
            + "{[Customer].[Gender].[F], [Customer].[Marital Status].[All Marital Status], [Store].[Stores].[USA]}\n"
            + "{[Customer].[Gender].[F], [Customer].[Marital Status].[M], [Store].[Stores].[All Stores]}\n"
            + "{[Customer].[Gender].[F], [Customer].[Marital Status].[M], [Store].[Stores].[Canada]}\n"
            + "{[Customer].[Gender].[F], [Customer].[Marital Status].[M], [Store].[Stores].[Mexico]}\n"
            + "{[Customer].[Gender].[F], [Customer].[Marital Status].[M], [Store].[Stores].[USA]}\n"
            + "{[Customer].[Gender].[F], [Customer].[Marital Status].[S], [Store].[Stores].[All Stores]}\n"
            + "{[Customer].[Gender].[F], [Customer].[Marital Status].[S], [Store].[Stores].[Canada]}\n"
            + "{[Customer].[Gender].[F], [Customer].[Marital Status].[S], [Store].[Stores].[Mexico]}\n"
            + "{[Customer].[Gender].[F], [Customer].[Marital Status].[S], [Store].[Stores].[USA]}\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[All Marital Status], [Store].[Stores].[All Stores]}\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[All Marital Status], [Store].[Stores].[Canada]}\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[All Marital Status], [Store].[Stores].[Mexico]}\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[All Marital Status], [Store].[Stores].[USA]}\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[M], [Store].[Stores].[All Stores]}\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[M], [Store].[Stores].[Canada]}\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[M], [Store].[Stores].[Mexico]}\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[M], [Store].[Stores].[USA]}\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[S], [Store].[Stores].[All Stores]}\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[S], [Store].[Stores].[Canada]}\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[S], [Store].[Stores].[Mexico]}\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[S], [Store].[Stores].[USA]}");
    }

    public void testCrossjoinSingletonTuples() {
        assertAxisReturns(
            "CrossJoin({([Gender].[M])}, {([Marital Status].[S])})",
            "{[Customer].[Gender].[M], [Customer].[Marital Status].[S]}");
    }

    public void testCrossjoinSingletonTuplesNested() {
        assertAxisReturns(
            "CrossJoin({([Gender].[M])}, CrossJoin({([Marital Status].[S])}, [Store].[Stores].children))",
            "{[Customer].[Gender].[M], [Customer].[Marital Status].[S], [Store].[Stores].[Canada]}\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[S], [Store].[Stores].[Mexico]}\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[S], [Store].[Stores].[USA]}");
    }

    public void testCrossjoinAsterisk() {
        assertAxisReturns(
            "{[Gender].[M]} * {[Marital Status].[S]}",
            "{[Customer].[Gender].[M], [Customer].[Marital Status].[S]}");
    }

    public void testCrossjoinAsteriskTuple() {
        assertQueryReturns(
            "select {[Measures].[Unit Sales]} ON COLUMNS, "
            + "NON EMPTY [Store].[All Stores] "
            + " * ([Product].[All Products], [Gender]) "
            + " * [Customers].[All Customers] ON ROWS "
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Store].[Stores].[All Stores], [Product].[Products].[All Products], [Customer].[Gender].[All Gender], [Customer].[Customers].[All Customers]}\n"
            + "Row #0: 266,773\n");
    }

    public void testCrossjoinAsteriskAssoc() {
        assertAxisReturns(
            "Order({[Gender].Children} * {[Marital Status].Children} * {[Time].[1997].[Q2].Children},"
            + "[Measures].[Unit Sales])",
            "{[Customer].[Gender].[F], [Customer].[Marital Status].[M], [Time].[Time].[1997].[Q2].[4]}\n"
            + "{[Customer].[Gender].[F], [Customer].[Marital Status].[M], [Time].[Time].[1997].[Q2].[6]}\n"
            + "{[Customer].[Gender].[F], [Customer].[Marital Status].[M], [Time].[Time].[1997].[Q2].[5]}\n"
            + "{[Customer].[Gender].[F], [Customer].[Marital Status].[S], [Time].[Time].[1997].[Q2].[4]}\n"
            + "{[Customer].[Gender].[F], [Customer].[Marital Status].[S], [Time].[Time].[1997].[Q2].[5]}\n"
            + "{[Customer].[Gender].[F], [Customer].[Marital Status].[S], [Time].[Time].[1997].[Q2].[6]}\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[M], [Time].[Time].[1997].[Q2].[4]}\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[M], [Time].[Time].[1997].[Q2].[5]}\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[M], [Time].[Time].[1997].[Q2].[6]}\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[S], [Time].[Time].[1997].[Q2].[6]}\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[S], [Time].[Time].[1997].[Q2].[4]}\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[S], [Time].[Time].[1997].[Q2].[5]}");
    }

    public void testCrossjoinAsteriskInsideBraces() {
        assertAxisReturns(
            "{[Gender].[M] * [Marital Status].[S] * [Time].[1997].[Q2].Children}",
            "{[Customer].[Gender].[M], [Customer].[Marital Status].[S], [Time].[Time].[1997].[Q2].[4]}\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[S], [Time].[Time].[1997].[Q2].[5]}\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[S], [Time].[Time].[1997].[Q2].[6]}");
    }

    public void testCrossJoinAsteriskQuery() {
        assertQueriesReturnSimilarResults(
            "SELECT  {[Measures].members * [Time].[1997].children} ON COLUMNS,\n"
            + " NON EMPTY {Head([Department].[All Departments].children,2) * [Position].[All Position].children} ON ROWS\n"
            + "FROM [HR]",
            "SELECT crossjoin([Measures].members, [Time].[1997].children) ON COLUMNS,\n"
            + " NON EMPTY Crossjoin(Head([Department].[All Departments].children,2), [Position].[All Position].children) ON ROWS\n"
            + "FROM [HR]", getTestContext());
        assertQueryReturns(
            "SELECT  {[Measures].members * [Time].[1997].children} ON COLUMNS,\n"
            + " NON EMPTY {Head([Department].[All Departments].children,2) * [Position].[All Position].children} ON ROWS\n"
            + "FROM [HR]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Org Salary], [Time].[Time].[1997].[Q1]}\n"
            + "{[Measures].[Org Salary], [Time].[Time].[1997].[Q2]}\n"
            + "{[Measures].[Org Salary], [Time].[Time].[1997].[Q3]}\n"
            + "{[Measures].[Org Salary], [Time].[Time].[1997].[Q4]}\n"
            + "{[Measures].[Count], [Time].[Time].[1997].[Q1]}\n"
            + "{[Measures].[Count], [Time].[Time].[1997].[Q2]}\n"
            + "{[Measures].[Count], [Time].[Time].[1997].[Q3]}\n"
            + "{[Measures].[Count], [Time].[Time].[1997].[Q4]}\n"
            + "{[Measures].[Number of Employees], [Time].[Time].[1997].[Q1]}\n"
            + "{[Measures].[Number of Employees], [Time].[Time].[1997].[Q2]}\n"
            + "{[Measures].[Number of Employees], [Time].[Time].[1997].[Q3]}\n"
            + "{[Measures].[Number of Employees], [Time].[Time].[1997].[Q4]}\n"
            + "Axis #2:\n"
            + "{[Department].[Department].[1], [Employee].[Position].[Senior Management]}\n"
            + "{[Department].[Department].[2], [Employee].[Position].[Middle Management]}\n"
            + "{[Department].[Department].[2], [Employee].[Position].[Senior Management]}\n"
            + "Row #0: $594.00\n"
            + "Row #0: $594.00\n"
            + "Row #0: $594.00\n"
            + "Row #0: $594.00\n"
            + "Row #0: 15\n"
            + "Row #0: 15\n"
            + "Row #0: 15\n"
            + "Row #0: 15\n"
            + "Row #0: 5\n"
            + "Row #0: 5\n"
            + "Row #0: 5\n"
            + "Row #0: 5\n"
            + "Row #1: $39.69\n"
            + "Row #1: $39.69\n"
            + "Row #1: $39.69\n"
            + "Row #1: $39.69\n"
            + "Row #1: 6\n"
            + "Row #1: 6\n"
            + "Row #1: 6\n"
            + "Row #1: 6\n"
            + "Row #1: 2\n"
            + "Row #1: 2\n"
            + "Row #1: 2\n"
            + "Row #1: 2\n"
            + "Row #2: $67.50\n"
            + "Row #2: $67.50\n"
            + "Row #2: $67.50\n"
            + "Row #2: $67.50\n"
            + "Row #2: 3\n"
            + "Row #2: 3\n"
            + "Row #2: 3\n"
            + "Row #2: 3\n"
            + "Row #2: 1\n"
            + "Row #2: 1\n"
            + "Row #2: 1\n"
            + "Row #2: 1\n");
    }

    public void testChildren() {
        // Yes, baking goods is correct. there are no products in that product
        // class, but we should not generate a join to filter a snowflake
        // table. It is a difference in behavior with FoodMart.
        assertAxisReturns(
            "[Product].[Drink].Children",
            "[Product].[Products].[Drink].[Alcoholic Beverages]\n"
            + (FILTER_SNOWFLAKE
                ? ""
                : "[Product].[Products].[Drink].[Baking Goods]\n")
            + "[Product].[Products].[Drink].[Beverages]\n"
            + "[Product].[Products].[Drink].[Dairy]");
    }

    /**
     * Testcase for bug 1889745, "StackOverflowError while resolving
     * crossjoin". The problem occurs when a calculated member that references
     * itself is referenced in a crossjoin.
     */
    public void testCrossjoinResolve() {
        assertQueryReturns(
            "with\n"
            + "member [Measures].[Filtered Unit Sales] as\n"
            + " 'IIf((([Measures].[Unit Sales] > 50000.0)\n"
            + "      OR ([Product].CurrentMember.Level.UniqueName <>\n"
            + "          \"[Product].[Products].[Product Family]\")),\n"
            + "      IIf(((Count([Product].CurrentMember.Children) = 0.0)),\n"
            + "          [Measures].[Unit Sales],\n"
            + "          Sum([Product].CurrentMember.Children,\n"
            + "              [Measures].[Filtered Unit Sales])),\n"
            + "      NULL)'\n"
            + "select NON EMPTY {crossjoin({[Measures].[Filtered Unit Sales]},\n"
            + "{[Gender].[M], [Gender].[F]})} ON COLUMNS,\n"
            + "NON EMPTY {[Product].[All Products]} ON ROWS\n"
            + "from [Sales]\n"
            + "where [Time].[1997]",
            "Axis #0:\n"
            + "{[Time].[Time].[1997]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Filtered Unit Sales], [Customer].[Gender].[M]}\n"
            + "{[Measures].[Filtered Unit Sales], [Customer].[Gender].[F]}\n"
            + "Axis #2:\n"
            + "{[Product].[Products].[All Products]}\n"
            + "Row #0: 97,126\n"
            + "Row #0: 94,814\n");
    }

    /**
     * Test case for bug 1911832, "Exception converting immutable list to array
     * in JDK 1.5".
     */
    public void testCrossjoinOrder() {
        assertQueryReturns(
            "WITH\n"
            + "\n"
            + "SET [S1] AS 'CROSSJOIN({[Time].[1997]}, {[Gender].[Gender].MEMBERS})'\n"
            + "\n"
            + "SELECT CROSSJOIN(ORDER([S1], [Measures].[Unit Sales], BDESC),\n"
            + "{[Measures].[Unit Sales]}) ON AXIS(0)\n"
            + "FROM [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time].[Time].[1997], [Customer].[Gender].[M], [Measures].[Unit Sales]}\n"
            + "{[Time].[Time].[1997], [Customer].[Gender].[F], [Measures].[Unit Sales]}\n"
            + "Row #0: 135,215\n"
            + "Row #0: 131,558\n");
    }

    public void testCrossjoinDupHierarchyFails() {
        assertQueryThrows(
            "select [Measures].[Unit Sales] ON COLUMNS,\n"
            + " CrossJoin({[Time].[Quarter].[Q1]}, {[Time].[Month].[5]}) ON ROWS\n"
            + "from [Sales]",
            "Tuple contains more than one member of hierarchy '[Time].[Time]'.");

        // now with Item, for kicks
        assertQueryThrows(
            "select [Measures].[Unit Sales] ON COLUMNS,\n"
            + " CrossJoin({[Time].[Quarter].[Q1]}, {[Time].[Month].[5]}).Item(0) ON ROWS\n"
            + "from [Sales]",
            "Tuple contains more than one member of hierarchy '[Time].[Time]'.");

        // same query using explicit tuple
        assertQueryThrows(
            "select [Measures].[Unit Sales] ON COLUMNS,\n"
            + " ([Time].[Quarter].[Q1], [Time].[Month].[5]) ON ROWS\n"
            + "from [Sales]",
            "Tuple contains more than one member of hierarchy '[Time].[Time]'.");
    }

    /**
     * Tests cases of different hierarchies in the same dimension.
     * (Compare to {@link #testCrossjoinDupHierarchyFails()}). Not an error.
     */
    public void testCrossjoinDupDimensionOk() {
        final String expectedResult =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[1997].[Q1], [Time].[Weekly].[1997].[10]}\n"
            + "Row #0: 4,395\n";
        final String timeWeekly = TestContext.hierarchyName("Time", "Weekly");
        assertQueryReturns(
            "select [Measures].[Unit Sales] ON COLUMNS,\n"
            + " CrossJoin({[Time].[Quarter].[Q1]}, {"
            + timeWeekly + ".[1997].[10]}) ON ROWS\n"
            + "from [Sales]",
            expectedResult);

        // now with Item, for kicks
        assertQueryReturns(
            "select [Measures].[Unit Sales] ON COLUMNS,\n"
            + " CrossJoin({[Time].[Quarter].[Q1]}, {"
            + timeWeekly + ".[1997].[10]}).Item(0) ON ROWS\n"
            + "from [Sales]",
            expectedResult);

        // same query using explicit tuple
        assertQueryReturns(
            "select [Measures].[Unit Sales] ON COLUMNS,\n"
            + " ([Time].[Quarter].[Q1], "
            + timeWeekly + ".[1997].[10]) ON ROWS\n"
            + "from [Sales]",
            expectedResult);
    }

    public void testDescendantsM() {
        assertAxisReturns(
            "Descendants([Time].[1997].[Q1])",
            "[Time].[Time].[1997].[Q1]\n"
            + "[Time].[Time].[1997].[Q1].[1]\n"
            + "[Time].[Time].[1997].[Q1].[2]\n"
            + "[Time].[Time].[1997].[Q1].[3]");
    }

    public void testDescendantsDepends() {
        getTestContext().assertSetExprDependsOn(
            "Descendants([Time].[Time].CurrentMember)",
            setOf("[Time].[Time]"));
    }

    public void testDescendantsML() {
        assertAxisReturns(
            "Descendants([Time].[1997], [Time].[Month])",
            months);
    }

    public void testDescendantsMLSelf() {
        assertAxisReturns(
            "Descendants([Time].[1997], [Time].[Quarter], SELF)",
            quarters);
    }

    public void testDescendantsMLLeaves() {
        assertAxisReturns(
            "Descendants([Time].[1997], [Time].[Year], LEAVES)",
            "");
        assertAxisReturns(
            "Descendants([Time].[1997], [Time].[Quarter], LEAVES)",
            "");
        assertAxisReturns(
            "Descendants([Time].[1997], [Time].[Month], LEAVES)",
            months);

        assertAxisReturns(
            "Descendants([Gender], [Gender].[Gender], leaves)",
            "[Customer].[Gender].[F]\n"
            + "[Customer].[Gender].[M]");
    }

    public void testDescendantsMLLeavesRagged() {
        if (!Bug.CubeRaggedFeature) {
            return;
        }
        // no cities are at leaf level
        final TestContext raggedContext =
            getTestContext().withSalesRagged();
        raggedContext.assertAxisReturns(
            "Descendants([Store].[Israel], [Store].[Store City], leaves)",
            "");

        // all cities are leaves
        raggedContext.assertAxisReturns(
            "Descendants([Geography].[Israel], [Geography].[City], leaves)",
            "[Geography].[Geographies].[Israel].[Israel].[Haifa]\n"
            + "[Geography].[Geographies].[Israel].[Israel].[Tel Aviv]");

        // No state is a leaf (not even Israel, which is both a country and a
        // a state, or Vatican, with is a country/state/city)
        raggedContext.assertAxisReturns(
            "Descendants([Geography], [Geography].[State], leaves)",
            "");

        // The Vatican is a nation with no children (they're all celibate,
        // you know).
        raggedContext.assertAxisReturns(
            "Descendants([Geography], [Geography].[Country], leaves)",
            "[Geography].[Geographies].[Vatican]");
    }

    public void testDescendantsMNLeaves() {
        // leaves at depth 0 returns the member itself
        assertAxisReturns(
            "Descendants([Time].[1997].[Q2].[4], 0, Leaves)",
            "[Time].[Time].[1997].[Q2].[4]");

        // leaves at depth > 0 returns the member itself
        assertAxisReturns(
            "Descendants([Time].[1997].[Q2].[4], 100, Leaves)",
            "[Time].[Time].[1997].[Q2].[4]");

        // leaves at depth < 0 returns all descendants
        assertAxisReturns(
            "Descendants([Time].[1997].[Q2], -1, Leaves)",
            "[Time].[Time].[1997].[Q2].[4]\n"
            + "[Time].[Time].[1997].[Q2].[5]\n"
            + "[Time].[Time].[1997].[Q2].[6]");

        // leaves at depth 0 returns the member itself
        assertAxisReturns(
            "Descendants([Time].[1997].[Q2], 0, Leaves)",
            "[Time].[Time].[1997].[Q2].[4]\n"
            + "[Time].[Time].[1997].[Q2].[5]\n"
            + "[Time].[Time].[1997].[Q2].[6]");

        assertAxisReturns(
            "Descendants([Time].[1997].[Q2], 3, Leaves)",
            "[Time].[Time].[1997].[Q2].[4]\n"
            + "[Time].[Time].[1997].[Q2].[5]\n"
            + "[Time].[Time].[1997].[Q2].[6]");
    }

    public void testDescendantsMLSelfBefore() {
        assertAxisReturns(
            "Descendants([Time].[1997], [Time].[Quarter], SELF_AND_BEFORE)",
            year1997
            + "\n"
            + quarters);
    }

    public void testDescendantsMLSelfBeforeAfter() {
        assertAxisReturns(
            "Descendants([Time].[1997], [Time].[Quarter], SELF_BEFORE_AFTER)",
            hierarchized1997);
    }

    public void testDescendantsMLBefore() {
        assertAxisReturns(
            "Descendants([Time].[1997], [Time].[Quarter], BEFORE)", year1997);
    }

    public void testDescendantsMLBeforeAfter() {
        assertAxisReturns(
            "Descendants([Time].[1997], [Time].[Quarter], BEFORE_AND_AFTER)",
            year1997
            + "\n"
            + months);
    }

    public void testDescendantsMLAfter() {
        assertAxisReturns(
            "Descendants([Time].[1997], [Time].[Quarter], AFTER)", months);
    }

    public void testDescendantsMLAfterEnd() {
        assertAxisReturns(
            "Descendants([Time].[1997], [Time].[Month], AFTER)", "");
    }

    public void testDescendantsM0() {
        assertAxisReturns(
            "Descendants([Time].[1997], 0)", year1997);
    }

    public void testDescendantsM2() {
        assertAxisReturns(
            "Descendants([Time].[1997], 2)", months);
    }

    public void testDescendantsM2Self() {
        assertAxisReturns(
            "Descendants([Time].[1997], 2, Self)", months);
    }

    public void testDescendantsM2Leaves() {
        assertAxisReturns(
            "Descendants([Time].[1997], 2, Leaves)", months);
    }

    public void testDescendantsMFarLeaves() {
        assertAxisReturns(
            "Descendants([Time].[1997], 10000, Leaves)", months);
    }

    public void testDescendantsMEmptyLeaves() {
        assertAxisReturns(
            "Descendants([Time].[1997], , Leaves)",
            months);
    }

    public void testDescendantsMEmptyLeavesFail() {
        assertAxisThrows(
            "Descendants([Time].[1997],)",
            "No function matches signature 'Descendants(<Member>, <Empty>)");
    }

    public void testDescendantsMEmptyLeavesFail2() {
        assertAxisThrows(
            "Descendants([Time].[1997], , AFTER)",
            "depth must be specified unless DESC_FLAG is LEAVES");
    }

    public void testDescendantsMFarSelf() {
        assertAxisReturns(
            "Descendants([Time].[1997], 10000, Self)",
            "");
    }

    public void testDescendantsMNY() {
        assertAxisReturns(
            "Descendants([Time].[1997], 1, BEFORE_AND_AFTER)",
            year1997
            + "\n"
            + months);
    }

    public void testDescendants2ndHier() {
        assertAxisReturns(
            "Descendants([Time.Weekly].[1997].[10], [Time.Weekly].[Day])",
            "[Time].[Weekly].[1997].[10].[23]\n"
            + "[Time].[Weekly].[1997].[10].[24]\n"
            + "[Time].[Weekly].[1997].[10].[25]\n"
            + "[Time].[Weekly].[1997].[10].[26]\n"
            + "[Time].[Weekly].[1997].[10].[27]\n"
            + "[Time].[Weekly].[1997].[10].[28]\n"
            + "[Time].[Weekly].[1997].[10].[1]");
    }

    public void testDescendantsParentChild() {
        getTestContext().withCube("HR").assertAxisReturns(
            "Descendants([Employees], 2)",
            "[Employee].[Employees].[Sheri Nowmer].[Derrick Whelply]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Maya Gutierrez]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Roberta Damstra]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Rebecca Kanagaki]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Darren Stanz]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Donna Arnold]");
    }

    public void testDescendantsParentChildBefore() {
        getTestContext().withCube("HR").assertAxisReturns(
            "Descendants([Employees], 2, BEFORE)",
            "[Employee].[Employees].[All Employees]\n"
            + "[Employee].[Employees].[Sheri Nowmer]");
    }

    public void testDescendantsParentChildLeaves() {
        final TestContext testContext = getTestContext().withCube("HR");
        if (Bug.avoidSlowTestOnLucidDB(testContext.getDialect())) {
            return;
        }

        // leaves, restricted by level
        testContext.assertAxisReturns(
            "Descendants([Employees].[All Employees].[Sheri Nowmer].[Michael Spence], [Employees].[Employee Id], LEAVES)",
            "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[John Brooks]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Todd Logan]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Joshua Several]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[James Thomas]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Robert Vessa]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Bronson Jacobs]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Rebecca Barley]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Emilio Alvaro]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Becky Waters]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[A. Joyce Jarvis]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Ruby Sue Styles]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Lisa Roy]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Ingrid Burkhardt]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Todd Whitney]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Barbara Wisnewski]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Karren Burkhardt]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[John Long]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Edwin Olenzek]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Jessie Valerio]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Robert Ahlering]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Megan Burke]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Karel Bates]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[James Tran]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Shelley Crow]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Anne Sims]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Clarence Tatman]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Jan Nelsen]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Jeanie Glenn]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Peggy Smith]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Tish Duff]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Anita Lucero]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Stephen Burton]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Amy Consentino]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Stacie Mcanich]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Mary Browning]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Alexandra Wellington]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Cory Bacugalupi]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Stacy Rizzi]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Mike White]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Marty Simpson]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Robert Jones]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Raul Casts]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Bridget Browqett]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Kay Kartz]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Jeanette Cole]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Phyllis Huntsman]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Hannah Arakawa]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Wathalee Steuber]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Pamela Cox]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Helen Lutes]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Linda Ecoffey]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Katherine Swint]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Dianne Slattengren]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Ronald Heymsfield]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Steven Whitehead]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[William Sotelo]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Beth Stanley]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Jill Markwood]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Mildred Valentine]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Suzann Reams]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Audrey Wold]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Susan French]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Trish Pederson]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Eric Renn]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Elizabeth Catalano]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Eric Coleman]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Catherine Abel]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Emilo Miller]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Hazel Walker]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Dianne Collins].[Lawrence Hurkett].[Sara Pettengill].[Linda Blasingame]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Dianne Collins].[Lawrence Hurkett].[Sara Pettengill].[Jackie Blackwell]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Dianne Collins].[Lawrence Hurkett].[Sara Pettengill].[John Ortiz]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Dianne Collins].[Lawrence Hurkett].[Sara Pettengill].[Stacey Tearpak]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Dianne Collins].[Lawrence Hurkett].[Sara Pettengill].[Fannye Weber]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Dianne Collins].[Lawrence Hurkett].[Sara Pettengill].[Diane Kabbes]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Dianne Collins].[Lawrence Hurkett].[Sara Pettengill].[Brenda Heaney]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Dianne Collins].[Lawrence Hurkett].[Sara Pettengill].[Judith Karavites]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Dianne Collins].[Lawrence Hurkett].[Jauna Elson]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Dianne Collins].[Lawrence Hurkett].[Nancy Hirota]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Dianne Collins].[Lawrence Hurkett].[Marie Moya]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Dianne Collins].[Lawrence Hurkett].[Nicky Chesnut]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Dianne Collins].[Lawrence Hurkett].[Karen Hall]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Dianne Collins].[Lawrence Hurkett].[Greg Narberes]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Dianne Collins].[Lawrence Hurkett].[Anna Townsend]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Michael Spence].[Dianne Collins].[Lawrence Hurkett].[Carol Ann Rockne]");

        // leaves, restricted by depth
        testContext.assertAxisReturns(
            "Descendants([Employees], 1, LEAVES)", "");
        testContext.assertAxisReturns(
            "Descendants([Employees], 2, LEAVES)",
            "[Employee].[Employees].[Sheri Nowmer].[Roberta Damstra].[Jennifer Cooper]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Roberta Damstra].[Peggy Petty]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Roberta Damstra].[Jessica Olguin]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Roberta Damstra].[Phyllis Burchett]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Rebecca Kanagaki].[Juanita Sharp]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Rebecca Kanagaki].[Sandra Brunner]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Darren Stanz].[Ernest Staton]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Darren Stanz].[Rose Sims]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Darren Stanz].[Lauretta De Carlo]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Darren Stanz].[Mary Williams]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Darren Stanz].[Terri Burke]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Darren Stanz].[Audrey Osborn]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Darren Stanz].[Brian Binai]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Darren Stanz].[Concepcion Lozada]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Donna Arnold].[Howard Bechard]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Donna Arnold].[Doris Carter]");

        testContext.assertAxisReturns(
            "Descendants([Employees], 3, LEAVES)",
            "[Employee].[Employees].[Sheri Nowmer].[Roberta Damstra].[Jennifer Cooper]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Roberta Damstra].[Peggy Petty]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Roberta Damstra].[Jessica Olguin]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Roberta Damstra].[Phyllis Burchett]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Rebecca Kanagaki].[Juanita Sharp]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Rebecca Kanagaki].[Sandra Brunner]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Darren Stanz].[Ernest Staton]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Darren Stanz].[Rose Sims]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Darren Stanz].[Lauretta De Carlo]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Darren Stanz].[Mary Williams]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Darren Stanz].[Terri Burke]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Darren Stanz].[Audrey Osborn]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Darren Stanz].[Brian Binai]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Darren Stanz].[Concepcion Lozada]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Donna Arnold].[Howard Bechard]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Donna Arnold].[Doris Carter]");

        // note that depth is RELATIVE to the starting member
        testContext.assertAxisReturns(
            "Descendants([Employees].[Sheri Nowmer].[Roberta Damstra], 1, LEAVES)",
            "[Employee].[Employees].[Sheri Nowmer].[Roberta Damstra].[Jennifer Cooper]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Roberta Damstra].[Peggy Petty]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Roberta Damstra].[Jessica Olguin]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Roberta Damstra].[Phyllis Burchett]");

        // Howard Bechard is a leaf member -- appears even at depth 0
        testContext.assertAxisReturns(
            "Descendants([Employees].[All Employees].[Sheri Nowmer].[Donna Arnold].[Howard Bechard], 0, LEAVES)",
            "[Employee].[Employees].[Sheri Nowmer].[Donna Arnold].[Howard Bechard]");
        testContext.assertAxisReturns(
            "Descendants([Employees].[All Employees].[Sheri Nowmer].[Donna Arnold].[Howard Bechard], 1, LEAVES)",
            "[Employee].[Employees].[Sheri Nowmer].[Donna Arnold].[Howard Bechard]");

        testContext.assertExprReturns(
            "Count(Descendants([Employees], 2, LEAVES))", "16");
        testContext.assertExprReturns(
            "Count(Descendants([Employees], 3, LEAVES))", "16");
        testContext.assertExprReturns(
            "Count(Descendants([Employees], 4, LEAVES))", "63");
        testContext.assertExprReturns(
            "Count(Descendants([Employees], 999, LEAVES))", "1,044");

        // Negative depth acts like +infinity (per MSAS).  Run the test several
        // times because we had a non-deterministic bug here.
        for (int i = 0; i < 100; ++i) {
            testContext.assertExprReturns(
                "Count(Descendants([Employees], -1, LEAVES))", "1,044");
        }
    }

    public void testDescendantsSBA() {
        assertAxisReturns(
            "Descendants([Time].[1997], 1, SELF_BEFORE_AFTER)",
            hierarchized1997);
    }

    public void testDescendantsSet() {
        assertAxisReturns(
            "Descendants({[Time].[1997].[Q4], [Time].[1997].[Q2]}, 1)",
            "[Time].[Time].[1997].[Q4].[10]\n"
            + "[Time].[Time].[1997].[Q4].[11]\n"
            + "[Time].[Time].[1997].[Q4].[12]\n"
            + "[Time].[Time].[1997].[Q2].[4]\n"
            + "[Time].[Time].[1997].[Q2].[5]\n"
            + "[Time].[Time].[1997].[Q2].[6]");
        assertAxisReturns(
            "Descendants({[Time].[1997]}, [Time].[Month], LEAVES)",
            months);
    }

    public void testDescendantsSetEmpty() {
        assertAxisThrows(
            "Descendants({}, 1)",
            "Cannot deduce type of set");
        assertAxisReturns(
            "Descendants(Filter({[Time].[Time].Members}, 1=0), 1)",
            "");
    }

    public void testRange() {
        assertAxisReturns(
            "[Time].[1997].[Q1].[2] : [Time].[1997].[Q2].[5]",
            "[Time].[Time].[1997].[Q1].[2]\n"
            + "[Time].[Time].[1997].[Q1].[3]\n"
            + "[Time].[Time].[1997].[Q2].[4]\n"
            + "[Time].[Time].[1997].[Q2].[5]"); // not parents

        // testcase for bug XXXXX: braces required
        assertQueryReturns(
            "with set [Set1] as '[Product].[Drink]:[Product].[Food]' \n"
            + "\n"
            + "select [Set1] on columns, {[Measures].defaultMember} on rows \n"
            + "\n"
            + "from Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Product].[Products].[Drink]}\n"
            + "{[Product].[Products].[Food]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Row #0: 24,597\n"
            + "Row #0: 191,940\n");
    }

    /**
     * tests that a null passed in returns an empty set in range function
     */
    public void testNullRange() {
        assertAxisReturns(
            "[Time].[1997].[Q1].[2] : NULL", //[Time].[1997].[Q2].[5]
            ""); // Empty Set
    }

    /**
     * tests that an exception is thrown if both parameters in a range function
     * are null.
     */
    public void testTwoNullRange() {
        assertAxisThrows(
            "NULL : NULL",
            "Mondrian Error:Failed to parse query 'select {NULL : NULL} on columns from Sales'");
    }

    /**
     * Large dimensions use a different member reader, therefore need to
     * be tested separately.
     */
    public void testRangeLarge() {
        assertAxisReturns(
            "[Customers].[USA].[CA].[San Francisco] : [Customers].[USA].[WA].[Bellingham]",
            "[Customer].[Customers].[USA].[CA].[San Francisco]\n"
            + "[Customer].[Customers].[USA].[CA].[San Gabriel]\n"
            + "[Customer].[Customers].[USA].[CA].[San Jose]\n"
            + "[Customer].[Customers].[USA].[CA].[Santa Cruz]\n"
            + "[Customer].[Customers].[USA].[CA].[Santa Monica]\n"
            + "[Customer].[Customers].[USA].[CA].[Spring Valley]\n"
            + "[Customer].[Customers].[USA].[CA].[Torrance]\n"
            + "[Customer].[Customers].[USA].[CA].[West Covina]\n"
            + "[Customer].[Customers].[USA].[CA].[Woodland Hills]\n"
            + "[Customer].[Customers].[USA].[OR].[Albany]\n"
            + "[Customer].[Customers].[USA].[OR].[Beaverton]\n"
            + "[Customer].[Customers].[USA].[OR].[Corvallis]\n"
            + "[Customer].[Customers].[USA].[OR].[Lake Oswego]\n"
            + "[Customer].[Customers].[USA].[OR].[Lebanon]\n"
            + "[Customer].[Customers].[USA].[OR].[Milwaukie]\n"
            + "[Customer].[Customers].[USA].[OR].[Oregon City]\n"
            + "[Customer].[Customers].[USA].[OR].[Portland]\n"
            + "[Customer].[Customers].[USA].[OR].[Salem]\n"
            + "[Customer].[Customers].[USA].[OR].[W. Linn]\n"
            + "[Customer].[Customers].[USA].[OR].[Woodburn]\n"
            + "[Customer].[Customers].[USA].[WA].[Anacortes]\n"
            + "[Customer].[Customers].[USA].[WA].[Ballard]\n"
            + "[Customer].[Customers].[USA].[WA].[Bellingham]");
    }

    public void testRangeStartEqualsEnd() {
        assertAxisReturns(
            "[Time].[1997].[Q3].[7] : [Time].[1997].[Q3].[7]",
            "[Time].[Time].[1997].[Q3].[7]");
    }

    public void testRangeStartEqualsEndLarge() {
        assertAxisReturns(
            "[Customers].[USA].[CA] : [Customers].[USA].[CA]",
            "[Customer].[Customers].[USA].[CA]");
    }

    public void testRangeEndBeforeStart() {
        assertAxisReturns(
            "[Time].[1997].[Q3].[7] : [Time].[1997].[Q2].[5]",
            "[Time].[Time].[1997].[Q2].[5]\n"
            + "[Time].[Time].[1997].[Q2].[6]\n"
            + "[Time].[Time].[1997].[Q3].[7]"); // same as if reversed
    }

    public void testRangeEndBeforeStartLarge() {
        assertAxisReturns(
            "[Customers].[USA].[WA] : [Customers].[USA].[CA]",
            "[Customer].[Customers].[USA].[CA]\n"
            + "[Customer].[Customers].[USA].[OR]\n"
            + "[Customer].[Customers].[USA].[WA]");
    }

    public void testRangeBetweenDifferentLevelsIsError() {
        assertAxisThrows(
            "[Time].[1997].[Q2] : [Time].[1997].[Q2].[5]",
            "Members must belong to the same level");
    }

    public void testRangeBoundedByAll() {
        assertAxisReturns(
            "[Gender] : [Gender]",
            "[Customer].[Gender].[All Gender]");
    }

    public void testRangeBoundedByAllLarge() {
        assertAxisReturns(
            "[Customers].DefaultMember : [Customers]",
            "[Customer].[Customers].[All Customers]");
    }

    public void testRangeBoundedByNull() {
        assertAxisReturns(
            "[Gender].[F] : [Gender].[M].NextMember",
            "");
    }

    public void testRangeBoundedByNullLarge() {
        assertAxisReturns(
            "[Customers].PrevMember : [Customers].[USA].[OR]",
            "");
    }

    public void testSetContainingLevelFails() {
        assertAxisThrows(
            "[Store].[Store City]",
            "No function matches signature '{<Level>}'");
    }

    public void testBug715177() {
        assertQueryReturns(
            "WITH MEMBER [Product].[Non-Consumable].[Other] AS\n"
            + " 'Sum(Except( [Product].[Product Department].Members,\n"
            + "       TopCount([Product].[Product Department].Members, 3)),\n"
            + "       Measures.[Unit Sales])'\n"
            + "SELECT\n"
            + "  { [Measures].[Unit Sales] } ON COLUMNS,\n"
            + "  { TopCount([Product].[Product Department].Members,3),\n"
            + "              [Product].[Non-Consumable].[Other] } ON ROWS\n"
            + "FROM [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages]}\n"
            + "{[Product].[Products].[Drink].[Beverages]}\n"
            + "{[Product].[Products].[Drink].[Dairy]}\n"
            + "{[Product].[Products].[Non-Consumable].[Other]}\n"
            + "Row #0: 6,838\n"
            + "Row #1: 13,573\n"
            + "Row #2: 4,186\n"
            + "Row #3: 242,176\n");
    }

    public void testBug714707() {
        // Same issue as bug 715177 -- "children" returns immutable
        // list, which set operator must make mutable.
        assertAxisReturns(
            "{[Store].[USA].[CA].children, [Store].[USA]}",
            "[Store].[Stores].[USA].[CA].[Alameda]\n"
            + "[Store].[Stores].[USA].[CA].[Beverly Hills]\n"
            + "[Store].[Stores].[USA].[CA].[Los Angeles]\n"
            + "[Store].[Stores].[USA].[CA].[San Diego]\n"
            + "[Store].[Stores].[USA].[CA].[San Francisco]\n"
            + "[Store].[Stores].[USA]");
    }

    public void testBug715177c() {
        assertAxisReturns(
            "Order(TopCount({[Store].[USA].[CA].children},"
            + " [Measures].[Unit Sales], 2), [Measures].[Unit Sales])",
            "[Store].[Stores].[USA].[CA].[Alameda]\n"
            + "[Store].[Stores].[USA].[CA].[San Francisco]\n"
            + "[Store].[Stores].[USA].[CA].[Beverly Hills]\n"
            + "[Store].[Stores].[USA].[CA].[San Diego]\n"
            + "[Store].[Stores].[USA].[CA].[Los Angeles]");
    }

    public void testFormatFixed() {
        assertExprReturns(
            "Format(12.2, \"#,##0.00\")",
            "12.20");
    }

    public void testFormatVariable() {
        assertExprReturns(
            "Format(1234.5, \"#,#\" || \"#0.00\")",
            "1,234.50");
    }

    public void testFormatMember() {
        assertExprReturns(
            "Format([Store].[USA].[CA], \"#,#\" || \"#0.00\")",
            "74,748.00");
    }

    public void testIIf() {
        assertExprReturns(
            "IIf(([Measures].[Unit Sales],[Product].[Drink].[Alcoholic Beverages].[Beer and Wine]) > 100, \"Yes\",\"No\")",
            "Yes");
    }

    public void testIIfWithNullAndNumber() {
        assertExprReturns(
            "IIf(([Measures].[Unit Sales],[Product].[Drink].[Alcoholic Beverages].[Beer and Wine]) > 100, null,20)",
            "");
        assertExprReturns(
            "IIf(([Measures].[Unit Sales],[Product].[Drink].[Alcoholic Beverages].[Beer and Wine]) > 100, 20,null)",
            "20");
    }

    public void testIIfWithStringAndNull()
    {
        assertExprReturns(
            "IIf(([Measures].[Unit Sales],[Product].[Drink].[Alcoholic Beverages].[Beer and Wine]) > 100, null,\"foo\")",
            "");
        assertExprReturns(
            "IIf(([Measures].[Unit Sales],[Product].[Drink].[Alcoholic Beverages].[Beer and Wine]) > 100, \"foo\",null)",
            "foo");
    }

    public void testIsEmptyWithNull()
    {
        assertExprReturns(
            "iif (isempty(null), \"is empty\", \"not is empty\")",
            "is empty");
        assertExprReturns("iif (isempty(null), 1, 2)", "1");
    }

    public void testIIfMember() {
        assertAxisReturns(
            "IIf(1 > 2,[Store].[USA],[Store].[Canada].[BC])",
            "[Store].[Stores].[Canada].[BC]");
    }

    public void testIIfLevel() {
        assertExprReturns(
            "IIf(1 > 2, [Store].[Store Country],[Store].[Store City]).Name",
            "Store City");
    }

    public void testIIfHierarchy() {
        assertExprReturns(
            "IIf(1 > 2, [Time], [Store]).Name",
            "Store");

        // Call Iif(<Logical>, <Dimension>, <Hierarchy>). Argument #3, the
        // hierarchy [Time.Weekly] is implicitly converted to
        // the dimension [Time] to match argument #2 which is a dimension.
        assertExprReturns(
            "IIf(1 > 2, [Time], [Time.Weekly]).Name",
            "Time");
    }

    public void testIIfDimension() {
        assertExprReturns(
            "IIf(1 > 2, [Store], [Time]).Name",
            "Time");
    }

    public void testIIfSet() {
        assertAxisReturns(
            "IIf(1 > 2, {[Store].[USA], [Store].[USA].[CA]}, {[Store].[Mexico], [Store].[USA].[OR]})",
            "[Store].[Stores].[Mexico]\n"
            + "[Store].[Stores].[USA].[OR]");
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
        assertExprReturns("[Store].[Stores].DefaultMember.Name", "All Stores");
        assertExprReturns("[Stores].DefaultMember.Name", "All Stores");
        assertExprThrows(
            "[Store].DefaultMember.Name",
            "The 'Store' dimension contains more than one hierarchy, "
            + "therefore "
            + "the hierarchy must be explicitly specified.");
        if (isDefaultNullMemberRepresentation()) {
            // name of null member
            assertExprReturns("[Stores].Parent.Name", "#null");
        }
    }

    public void testDimensionUniqueName() {
        assertExprReturns(
            "[Gender].DefaultMember.Dimension.UniqueName",
            "[Customer]");
    }

    public void testHierarchyUniqueName() {
        assertExprReturns(
            "[Gender].DefaultMember.Hierarchy.UniqueName",
            "[Customer].[Gender]");
    }

    public void testLevelUniqueName() {
        assertExprReturns(
            "[Gender].DefaultMember.Level.UniqueName",
            "[Customer].[Gender].[(All)]");
    }

    public void testMemberUniqueName() {
        assertExprReturns(
            "[Gender].DefaultMember.UniqueName",
            "[Customer].[Gender].[All Gender]");
    }

    public void testMemberUniqueNameOfNull() {
        if (isDefaultNullMemberRepresentation()) {
            assertExprReturns(
                "[Measures].[Unit Sales].FirstChild.UniqueName",
                "[Measures].[#null]"); // MSOLAP gives "" here
        }
    }

    public void testCoalesceEmptyDepends() {
        getTestContext().assertExprDependsOn(
            "coalesceempty([Time].[1997], [Gender].[M])",
            TestContext.allHiers());
        Set<String> s1 =
            TestContext.allHiersExcept(
                "[Measures]", "[Time].[Time]");
        getTestContext().assertExprDependsOn(
            "coalesceempty(([Measures].[Unit Sales], [Time].[1997]),"
            + " ([Measures].[Store Sales], [Time].[1997].[Q2]))",
            s1);
    }

    public void testCoalesceEmpty() {
        // [DF] is all null and [WA] has numbers for 1997 but not for 1998.
        Result result = executeQuery(
            "with\n"
            + "    member Measures.[Coal1] as 'coalesceempty(([Time].[1997], Measures.[Store Sales]), ([Time].[1998], Measures.[Store Sales]))'\n"
            + "    member Measures.[Coal2] as 'coalesceempty(([Time].[1997], Measures.[Unit Sales]), ([Time].[1998], Measures.[Unit Sales]))'\n"
            + "select \n"
            + "    {Measures.[Coal1], Measures.[Coal2]} on columns,\n"
            + "    {[Store].[All Stores].[Mexico].[DF], [Store].[All Stores].[USA].[WA]} on rows\n"
            + "from \n"
            + "    [Sales]");

        checkDataResults(
            new Double[][]{
                new Double[]{null, null},
                new Double[]{new Double(263793.22), new Double(124366)}
            },
            result,
            0.001);

        result = executeQuery(
            "with\n"
            + "    member Measures.[Sales Per Customer] as 'Measures.[Sales Count] / Measures.[Customer Count]'\n"
            + "    member Measures.[Coal] as 'coalesceempty(([Measures].[Sales Per Customer], [Store].[All Stores].[Mexico].[DF]),\n"
            + "        Measures.[Sales Per Customer])'\n"
            + "select \n"
            + "    {Measures.[Sales Per Customer], Measures.[Coal]} on columns,\n"
            + "    {[Store].[All Stores].[Mexico].[DF], [Store].[All Stores].[USA].[WA]} on rows\n"
            + "from \n"
            + "    [Sales]\n"
            + "where\n"
            + "    ([Time].[1997].[Q2])");

        checkDataResults(
            new Double[][]{
                new Double[]{null, null},
                new Double[]{new Double(8.963), new Double(8.963)}
            },
            result,
            0.001);

        result = executeQuery(
            "with\n"
            + "    member Measures.[Sales Per Customer] as 'Measures.[Sales Count] / Measures.[Customer Count]'\n"
            + "    member Measures.[Coal] as 'coalesceempty(([Measures].[Sales Per Customer], [Store].[All Stores].[Mexico].[DF]),\n"
            + "        ([Measures].[Sales Per Customer], [Store].[All Stores].[Mexico].[DF]),\n"
            + "        ([Measures].[Sales Per Customer], [Store].[All Stores].[Mexico].[DF]),\n"
            + "        ([Measures].[Sales Per Customer], [Store].[All Stores].[Mexico].[DF]),\n"
            + "        ([Measures].[Sales Per Customer], [Store].[All Stores].[Mexico].[DF]),\n"
            + "        ([Measures].[Sales Per Customer], [Store].[All Stores].[Mexico].[DF]),\n"
            + "        ([Measures].[Sales Per Customer], [Store].[All Stores].[Mexico].[DF]),\n"
            + "        ([Measures].[Sales Per Customer], [Store].[All Stores].[Mexico].[DF]),\n"
            + "        Measures.[Sales Per Customer])'\n"
            + "select \n"
            + "    {Measures.[Sales Per Customer], Measures.[Coal]} on columns,\n"
            + "    {[Store].[All Stores].[Mexico].[DF], [Store].[All Stores].[USA].[WA]} on rows\n"
            + "from \n"
            + "    [Sales]\n"
            + "where\n"
            + "    ([Time].[1997].[Q2])");

        checkDataResults(
            new Double[][]{
                new Double[]{null, null},
                new Double[]{new Double(8.963), new Double(8.963)}
            },
            result,
            0.001);
    }

    public void testBrokenContextBug() {
        Result result = executeQuery(
            "with\n"
            + "    member Measures.[Sales Per Customer] as 'Measures.[Sales Count] / Measures.[Customer Count]'\n"
            + "    member Measures.[Coal] as 'coalesceempty(([Measures].[Sales Per Customer], [Store].[All Stores].[Mexico].[DF]),\n"
            + "        Measures.[Sales Per Customer])'\n"
            + "select \n"
            + "    {Measures.[Coal]} on columns,\n"
            + "    {[Store].[All Stores].[USA].[WA]} on rows\n"
            + "from \n"
            + "    [Sales]\n"
            + "where\n"
            + "    ([Time].[1997].[Q2])");

        checkDataResults(new Double[][]{{new Double(8.963)}}, result, 0.001);
    }

    /**
     * Tests the function <code>&lt;Set&gt;.Item(&lt;Integer&gt;)</code>.
     */
    public void testSetItemInt() {
        assertAxisReturns(
            "{[Customers].[All Customers].[USA].[OR].[Lebanon].[Mary Frances Christian]}.Item(0)",
            "[Customer].[Customers].[USA].[OR].[Lebanon].[Mary Frances Christian]");

        assertAxisReturns(
            "{[Customers].[All Customers].[USA],"
            + "[Customers].[All Customers].[USA].[WA],"
            + "[Customers].[All Customers].[USA].[CA],"
            + "[Customers].[All Customers].[USA].[OR].[Lebanon].[Mary Frances Christian]}.Item(2)",
            "[Customer].[Customers].[USA].[CA]");

        assertAxisReturns(
            "{[Customers].[All Customers].[USA],"
            + "[Customers].[All Customers].[USA].[WA],"
            + "[Customers].[All Customers].[USA].[CA],"
            + "[Customers].[All Customers].[USA].[OR].[Lebanon].[Mary Frances Christian]}.Item(100 / 50 - 1)",
            "[Customer].[Customers].[USA].[WA]");

        assertAxisReturns(
            "{([Time].[1997].[Q1].[1], [Customers].[All Customers].[USA]),"
            + "([Time].[1997].[Q1].[2], [Customers].[All Customers].[USA].[WA]),"
            + "([Time].[1997].[Q1].[3], [Customers].[All Customers].[USA].[CA]),"
            + "([Time].[1997].[Q2].[4], [Customers].[All Customers].[USA].[OR].[Lebanon].[Mary Frances Christian])}"
            + ".Item(100 / 50 - 1)",
            "{[Time].[Time].[1997].[Q1].[2], [Customer].[Customers].[USA].[WA]}");

        // given index out of bounds, item returns null
        assertAxisReturns(
            "{[Customers].[All Customers].[USA],"
            + "[Customers].[All Customers].[USA].[WA],"
            + "[Customers].[All Customers].[USA].[CA],"
            + "[Customers].[All Customers].[USA].[OR].[Lebanon].[Mary Frances Christian]}.Item(-1)",
            "");

        // given index out of bounds, item returns null
        assertAxisReturns(
            "{[Customers].[All Customers].[USA],"
            + "[Customers].[All Customers].[USA].[WA],"
            + "[Customers].[All Customers].[USA].[CA],"
            + "[Customers].[All Customers].[USA].[OR].[Lebanon].[Mary Frances Christian]}.Item(4)",
            "");
    }

    /**
     * Tests the function <code>&lt;Set&gt;.Item(&lt;String&gt; [,...])</code>.
     */
    public void testSetItemString() {
        assertAxisReturns(
            "{[Gender].[M], [Gender].[F]}.Item(\"M\")",
            "[Customer].[Gender].[M]");

        assertAxisReturns(
            "{CrossJoin([Gender].Members, [Marital Status].Members)}.Item(\"M\", \"S\")",
            "{[Customer].[Gender].[M], [Customer].[Marital Status].[S]}");

        // MSAS fails with "duplicate dimensions across (independent) axes".
        // (That's a bug in MSAS.)
        assertAxisReturns(
            "{CrossJoin([Gender].Members, [Marital Status].Members)}.Item(\"M\", \"M\")",
            "{[Customer].[Gender].[M], [Customer].[Marital Status].[M]}");

        // None found.
        assertAxisReturns(
            "{[Gender].[M], [Gender].[F]}.Item(\"X\")", "");
        assertAxisReturns(
            "{CrossJoin([Gender].Members, [Marital Status].Members)}.Item(\"M\", \"F\")",
            "");
        assertAxisReturns(
            "CrossJoin([Gender].Members, [Marital Status].Members).Item(\"S\", \"M\")",
            "");

        assertAxisThrows(
            "CrossJoin([Gender].Members, [Marital Status].Members).Item(\"M\")",
            "Argument count does not match set's cardinality 2");
    }

    public void testTuple() {
        assertExprReturns(
            "([Gender].[M], "
            + "[Time].[Time].Children.Item(2), "
            + "[Measures].[Unit Sales])",
            "33,249");
        // Calc calls MemberValue with 3 args -- more efficient than
        // constructing a tuple.
        assertExprCompilesTo(
            "([Gender].[M], [Time].[Time].Children.Item(2), [Measures].[Unit Sales])",
            "MemberArrayValueCalc(name=MemberArrayValueCalc, class=class mondrian.calc.impl.MemberArrayValueCalc, type=SCALAR, resultStyle=VALUE)\n"
            + "    Literal(name=Literal, class=class mondrian.calc.impl.ConstantCalc, type=MemberType<member=[Customer].[Gender].[M]>, resultStyle=VALUE_NOT_NULL, value=[Customer].[Gender].[M])\n"
            + "    Item(name=Item, class=class mondrian.olap.fun.SetItemFunDef$5, type=MemberType<hierarchy=[Time].[Time]>, resultStyle=VALUE)\n"
            + "        Children(name=Children, class=class mondrian.olap.fun.BuiltinFunTable$22$1, type=SetType<MemberType<hierarchy=[Time].[Time]>>, resultStyle=LIST)\n"
            + "            CurrentMemberFixed(hierarchy=[Time].[Time], name=CurrentMemberFixed, class=class mondrian.olap.fun.HierarchyCurrentMemberFunDef$FixedCalcImpl, type=MemberType<hierarchy=[Time].[Time]>, resultStyle=VALUE)\n"
            + "        Literal(name=Literal, class=class mondrian.calc.impl.ConstantCalc, type=DecimalType(0), resultStyle=VALUE_NOT_NULL, value=2)\n"
            + "    Literal(name=Literal, class=class mondrian.calc.impl.ConstantCalc, type=MemberType<member=[Measures].[Unit Sales]>, resultStyle=VALUE_NOT_NULL, value=[Measures].[Unit Sales])\n");
    }

    /**
     * Tests whether the tuple operator can be applied to arguments of various
     * types. See bug 1491699
     * "ClassCastException in mondrian.calc.impl.GenericCalc.evaluat".
     */
    public void testTupleArgTypes() {
        // can coerce dimensions (if they have a unique hierarchy) and
        // hierarchies to members
        assertExprReturns(
            "([Gender], [Time].[Time])",
            "266,773");

        // can coerce hierarchy to member
        assertExprReturns(
            "([Gender].[M], " + TimeWeekly + ")", "135,215");

        // cannot coerce level to member
        assertAxisThrows(
            "{([Gender].[M], [Store].[Store City])}",
            "No function matches signature '(<Member>, <Level>)'");

        // coerce args (hierarchy, member, member, dimension)
        assertAxisReturns(
            "{([Time.Weekly], [Measures].[Store Sales], [Marital Status].[M], [Promotion].[Media Type])}",
            "{[Time].[Weekly].[All Weeklys], [Measures].[Store Sales], [Customer].[Marital Status].[M], [Promotion].[Media Type].[All Media]}");

        // usage of different hierarchies in the [Time] dimension
        assertAxisReturns(
            "{([Time.Weekly], [Measures].[Store Sales], [Marital Status].[M], [Time].[Time])}",
            "{[Time].[Weekly].[All Weeklys], [Measures].[Store Sales], [Customer].[Marital Status].[M], [Time].[Time].[1997]}");

        // two usages of the [Time].[Weekly] hierarchy
        assertAxisThrows(
            "{([Time].[Weekly], [Measures].[Store Sales], [Marital Status].[M], [Time].[Weekly])}",
            "Tuple contains more than one member of hierarchy '[Time].[Weekly]'.");

        // cannot coerce integer to member
        assertAxisThrows(
            "{([Gender].[M], 123)}",
            "No function matches signature '(<Member>, <Numeric Expression>)'");
    }

    public void testTupleItem() {
        assertAxisReturns(
            "([Time].[1997].[Q1].[1], [Customers].[All Customers].[USA].[OR], [Gender].[All Gender].[M]).item(2)",
            "[Customer].[Gender].[M]");

        assertAxisReturns(
            "([Time].[1997].[Q1].[1], [Customers].[All Customers].[USA].[OR], [Gender].[All Gender].[M]).item(1)",
            "[Customer].[Customers].[USA].[OR]");

        assertAxisReturns(
            "{[Time].[1997].[Q1].[1]}.item(0)",
            "[Time].[Time].[1997].[Q1].[1]");

        assertAxisReturns(
            "{[Time].[1997].[Q1].[1]}.Item(0).Item(0)",
            "[Time].[Time].[1997].[Q1].[1]");

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
            "");

        // empty set of unknown type
        assertExprReturns(
            "{}.Item(3)",
            "");

        // past end of set
        assertExprReturns(
            "{[Gender].members}.Item(4)",
            "");

        // negative index
        assertExprReturns(
            "{[Gender].members}.Item(-50)",
            "");
    }

    public void testTupleAppliedToUnknownHierarchy() {
        // manifestation of bug 1735821
        assertQueryReturns(
            "with \n"
            + "member [Product].[Test] as '([Product].[Food],Dimensions(0).defaultMember)' \n"
            + "select \n"
            + "{[Product].[Test], [Product].[Food]} on columns, \n"
            + "{[Measures].[Store Sales]} on rows \n"
            + "from Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Product].[Products].[Test]}\n"
            + "{[Product].[Products].[Food]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Store Sales]}\n"
            + "Row #0: 191,940.00\n"
            + "Row #0: 409,035.59\n");
    }

    public void testTupleDepends()
    {
        getTestContext().assertMemberExprDependsOn(
            "([Store].[USA], [Gender].[F])", Collections.<String>emptySet());

        getTestContext().assertMemberExprDependsOn(
            "([Store].[USA], [Gender])", setOf("[Customer].[Gender]"));

        // in a scalar context, the expression depends on everything except
        // the explicitly stated dimensions
        getTestContext().assertExprDependsOn(
            "([Store].[USA], [Gender])",
            TestContext.allHiersExcept("[Store].[Stores]"));

        // The result should be all dims except [Gender], but there's a small
        // bug in MemberValueCalc.dependsOn where we escalate 'might depend' to
        // 'depends' and we return that it depends on all dimensions.
        getTestContext().assertExprDependsOn(
            "(Dimensions('Store').CurrentMember, [Gender].[F])",
            TestContext.allHiers());
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
            "Customer");

        // MSAS returns error here.
        assertExprReturns(
            "Filter([Gender].members, 1 = 0).Item(0).Parent",
            "");
        assertExprReturns(
            "(Filter([Stores].MEMBERS, 0 = 0).Item(0).Item(0),"
            + "Filter([Stores].MEMBERS, 0 = 0).Item(0).Item(0))",
            "266,773");

        if (isDefaultNullMemberRepresentation()) {
            // MSAS returns error here.
            assertExprReturns(
                "Filter([Gender].members, 1 = 0).Item(0).Name",
                "#null");
        }
    }

    public void testTupleNull() {
        // if a tuple contains any null members, it evaluates to null
        assertQueryReturns(
            "select {[Measures].[Unit Sales]} on columns,\n"
            + " { ([Gender].[M], [Stores]),\n"
            + "   ([Gender].[F], [Stores].parent),\n"
            + "   ([Gender].parent, [Stores])} on rows\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Gender].[M], [Store].[Stores].[All Stores]}\n"
            + "Row #0: 135,215\n");

        // the set function eliminates tuples which are wholly or partially
        // null
        assertAxisReturns(
            "([Gender].parent, [Marital Status]),\n" // part null
            + " ([Gender].[M], [Marital Status].parent),\n" // part null
            + " ([Gender].parent, [Marital Status].parent),\n" // wholly null
            + " ([Gender].[M], [Marital Status])", // not null
            "{[Customer].[Gender].[M], [Customer].[Marital Status].[All Marital Status]}");

        if (isDefaultNullMemberRepresentation()) {
            // The tuple constructor returns a null tuple if one of its
            // arguments is null -- and the Item function returns null if the
            // tuple is null.
            assertExprReturns(
                "([Gender].parent, [Marital Status]).Item(0).Name",
                "#null");
            assertExprReturns(
                "([Gender].parent, [Marital Status]).Item(1).Name",
                "#null");
        }
    }

    private void checkDataResults(
        Double[][] expected,
        Result result,
        final double tolerance)
    {
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
                        "Cell at (" + row + ", " + col
                        + ") was null, but was expecting "
                        + expectedValue);
                } else {
                    assertEquals(
                        "Incorrect value returned at ("
                        + row + ", " + col + ")",
                        expectedValue,
                        ((Number) cell.getValue()).doubleValue(),
                        tolerance);
                }
            }
        }
    }

    public void testLevelMemberExpressions() {
        // Should return Beverly Hills in California.
        assertAxisReturns(
            "[Store].[Store City].[Beverly Hills]",
            "[Store].[Stores].[USA].[CA].[Beverly Hills]");

        // There are two months named "1" in the time dimension: one
        // for 1997 and one for 1998.  <Level>.<Member> should return
        // the first one.
        assertAxisReturns(
            "[Time].[Month].[1]",
            "[Time].[Time].[1997].[Q1].[1]");

        // Shouldn't be able to find a member named "Q1" on the month level.
        assertAxisThrows(
            "[Time].[Month].[Q1]",
            "MDX object '[Time].[Month].[Q1]' not found in cube");
    }

    public void testCaseTestMatch() {
        assertExprReturns(
            "CASE WHEN 1=0 THEN \"first\" WHEN 1=1 THEN \"second\" WHEN 1=2 THEN \"third\" ELSE \"fourth\" END",
            "second");
    }

    public void testCaseTestMatchElse() {
        assertExprReturns(
            "CASE WHEN 1=0 THEN \"first\" ELSE \"fourth\" END",
            "fourth");
    }

    public void testCaseTestMatchNoElse() {
        assertExprReturns(
            "CASE WHEN 1=0 THEN \"first\" END",
            "");
    }

    /**
     * Testcase for bug 1799391, "Case Test function throws class cast
     * exception"
     */
    public void testCaseTestReturnsMemberBug1799391() {
        assertQueryReturns(
            "WITH\n"
            + " MEMBER [Product].[CaseTest] AS\n"
            + " 'CASE\n"
            + " WHEN [Gender].CurrentMember IS [Gender].[M] THEN [Gender].[F]\n"
            + " ELSE [Gender].[F]\n"
            + " END'\n"
            + "                \n"
            + "SELECT {[Product].[CaseTest]} ON 0, {[Gender].[M]} ON 1 FROM Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Product].[Products].[CaseTest]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Gender].[M]}\n"
            + "Row #0: 131,558\n");

        assertAxisReturns(
            "CASE WHEN 1+1 = 2 THEN [Gender].[F] ELSE [Gender].[F].Parent END",
            "[Customer].[Gender].[F]");

        // try case match for good measure
        assertAxisReturns(
            "CASE 1 WHEN 2 THEN [Gender].[F] ELSE [Gender].[F].Parent END",
            "[Customer].[Gender].[All Gender]");
    }

    public void testCaseMatch() {
        assertExprReturns(
            "CASE 2 WHEN 1 THEN \"first\" WHEN 2 THEN \"second\" WHEN 3 THEN \"third\" ELSE \"fourth\" END",
            "second");
    }

    public void testCaseMatchElse() {
        assertExprReturns(
            "CASE 7 WHEN 1 THEN \"first\" ELSE \"fourth\" END",
            "fourth");
    }

    public void testCaseMatchNoElse() {
        assertExprReturns(
            "CASE 8 WHEN 0 THEN \"first\" END",
            "");
    }

    public void testCaseTypeMismatch() {
        // type mismatch between case and else
        assertAxisThrows(
            "CASE 1 WHEN 1 THEN 2 ELSE \"foo\" END",
            "No function matches signature");
        // type mismatch between case and case
        assertAxisThrows(
            "CASE 1 WHEN 1 THEN 2 WHEN 2 THEN \"foo\" ELSE 3 END",
            "No function matches signature");
        // type mismatch between value and case
        assertAxisThrows(
            "CASE 1 WHEN \"foo\" THEN 2 ELSE 3 END",
            "No function matches signature");
        // non-boolean condition
        assertAxisThrows(
            "CASE WHEN 1 = 2 THEN 3 WHEN 4 THEN 5 ELSE 6 END",
            "No function matches signature");
    }

    /**
     * Testcase for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-853">
     * bug MONDRIAN-853, "When using CASE WHEN in a CalculatedMember values are
     * not returned the way expected"</a>.
     */
    public void testCaseTuple() {
        // The case in the bug, simplified. With the bug, returns a member array
        // "[Lmondrian.olap.Member;@151b0a5". Type deduction should realize
        // that the result is a scalar, therefore a tuple (represented by a
        // member array) needs to be evaluated to a scalar. I think that if we
        // get the type deduction right, the MDX exp compiler will handle the
        // rest.
        if (false)
        assertExprReturns(
            "case 1 when 0 then 1.5\n"
            + " else ([Gender].[M], [Measures].[Unit Sales]) end",
            "135,215");

        // "case when" variant always worked
        assertExprReturns(
            "case when 1=0 then 1.5\n"
            + " else ([Gender].[M], [Measures].[Unit Sales]) end",
            "135,215");

        // case 2: cannot deduce type (tuple x) vs. (tuple y). Should be able
        // to deduce that the result type is tuple-type<member-type<Gender>,
        // member-type<Measures>>.
        if (false)
        assertExprReturns(
            "case when 1=0 then ([Gender].[M], [Measures].[Store Sales])\n"
            + " else ([Gender].[M], [Measures].[Unit Sales]) end",
            "xxx");

        // case 3: mixture of member & tuple. Should be able to deduce that
        // result type is an expression.
        if (false)
        assertExprReturns(
            "case when 1=0 then ([Measures].[Store Sales])\n"
            + " else ([Gender].[M], [Measures].[Unit Sales]) end",
            "xxx");
    }

    public void testPropertiesExpr() {
        assertExprReturns(
            "[Store].[USA].[CA].[Beverly Hills].[Store 6].Properties(\"Store Type\")",
            "Gourmet Supermarket");
    }

    /**
     * Test case for bug
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1227">MONDRIAN-1227,
     * "Properties function does not implicitly convert dimension to member; has
     * documentation typos"</a>.
     */
    public void testPropertiesOnDimension() {
        // [Store] is a dimension. When called with a property like FirstChild,
        // it is implicitly converted to a member.
        // Store in sales cube now has two hierarchies, so using sales ragged.
        getTestContext().withSalesRagged()
            .assertAxisReturns(
                "[Store].FirstChild",
                "[Store].[Stores].[Canada]");

        // The same should happen with the <Member>.Properties(<String>)
        // function; now the bug is fixed, it does. Dimension is implicitly
        // converted to member.
        getTestContext().withSalesRagged().assertExprReturns(
            "[Store].Properties('MEMBER_UNIQUE_NAME')",
            "[Store].[Stores].[All Stores]");

        // Hierarchy is implicitly converted to member.
        getTestContext().withSalesRagged().assertExprReturns(
            "[Store].[USA].Hierarchy.Properties('MEMBER_UNIQUE_NAME')",
            "[Store].[Stores].[All Stores]");
    }

    /**
     * Tests that non-existent property throws an error. *
     */
    public void testPropertiesNonExistent() {
        assertExprThrows(
            "[Store].[USA].[CA].[Beverly Hills].[Store 6].Properties(\"Foo\")",
            "Property 'Foo' is not valid for");
    }

    public void testPropertiesFilter() {
        Result result = executeQuery(
            "SELECT { [Store Sales] } ON COLUMNS,\n"
            + " TOPCOUNT(Filter( [Store].[Store Name].Members,\n"
            + "                   [Stores].CURRENTMEMBER.Properties(\"Store Type\") = \"Supermarket\"),\n"
            + "           10, [Store Sales]) ON ROWS\n"
            + "FROM [Sales]");
        assertEquals(8, result.getAxes()[1].getPositions().size());
    }

    public void testPropertyInCalculatedMember() {
        Result result = executeQuery(
            "WITH MEMBER [Measures].[Store Sales per Sqft]\n"
            + "AS '[Measures].[Store Sales] / "
            + "  [Stores].CURRENTMEMBER.Properties(\"Store Sqft\")'\n"
            + "SELECT \n"
            + "  {[Measures].[Unit Sales], [Measures].[Store Sales per Sqft]} ON COLUMNS,\n"
            + "  {[Store].[Store Name].members} ON ROWS\n"
            + "FROM Sales");
        Member member;
        Cell cell;
        member = result.getAxes()[1].getPositions().get(18).get(0);
        assertEquals(
            "[Store].[Stores].[USA].[WA].[Bellingham].[Store 2]",
            member.getUniqueName());
        cell = result.getCell(new int[]{0, 18});
        assertEquals("2,237", cell.getFormattedValue());
        cell = result.getCell(new int[]{1, 18});
        assertEquals(".17", cell.getFormattedValue());
        member = result.getAxes()[1].getPositions().get(3).get(0);
        assertEquals(
            "[Store].[Stores].[Mexico].[DF].[San Andres].[Store 21]",
            member.getUniqueName());
        cell = result.getCell(new int[]{0, 3});
        assertEquals("", cell.getFormattedValue());
        cell = result.getCell(new int[]{1, 3});
        assertEquals("", cell.getFormattedValue());
    }

    public void testOpeningPeriod() {
        assertAxisReturns(
            "OpeningPeriod([Time].[Month], [Time].[1997].[Q3])",
            "[Time].[Time].[1997].[Q3].[7]");

        assertAxisReturns(
            "OpeningPeriod([Time].[Quarter], [Time].[1997])",
            "[Time].[Time].[1997].[Q1]");

        assertAxisReturns(
            "OpeningPeriod([Time].[Year], [Time].[1997])",
            "[Time].[Time].[1997]");

        assertAxisReturns(
            "OpeningPeriod([Time].[Month], [Time].[1997])",
            "[Time].[Time].[1997].[Q1].[1]");

        assertAxisReturns(
            "OpeningPeriod([Product].[Product Name], [Product].[Products].[Drink])",
            "[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Imported Beer]");

        // Default member is [Time].[1997].
        assertAxisReturns(
            "OpeningPeriod([Time].[Month])", "[Time].[Time].[1997].[Q1].[1]");

        assertAxisReturns("OpeningPeriod()", "[Time].[Time].[1997].[Q1]");

        assertAxisThrows(
            "OpeningPeriod([Store].[Store City])",
            "The <level> and <member> arguments to OpeningPeriod must be "
            + "from the same hierarchy. The level was from '[Store].[Stores]' "
            + "but the member was from '[Time].[Time]'.");
    }

    public void testOpeningPeriodRagged() {
        if (!Bug.CubeRaggedFeature) {
            return;
        }
        final TestContext raggedContext =
            getTestContext().legacy().withCube("[Sales Ragged]");
        raggedContext.assertAxisReturns(
            "OpeningPeriod([Store].[Store City], [Store].[All Stores].[Israel])",
            "[Store].[Store].[Israel].[Israel].[Haifa]");

        raggedContext.assertAxisReturns(
            "OpeningPeriod([Store].[Store State], [Store].[All Stores].[Israel])",
            "");

        raggedContext.assertAxisThrows(
            "OpeningPeriod([Time].[Year], [Store].[All Stores].[Israel])",
            "The <level> and <member> arguments to OpeningPeriod must be "
            + "from the same hierarchy. The level was from '[Time].[Time]' but "
            + "the member was from '[Store].[Store]'.");
    }

    /**
     * This tests new NULL functionality exception throwing
     *
     */
    public void testOpeningPeriodNull() {
        assertAxisThrows(
            "OpeningPeriod([Time].[Month], NULL)",
            "Mondrian Error:Failed to parse query 'select {OpeningPeriod([Time].[Month], NULL)} on columns from Sales'");
    }

    public void testLastPeriods() {
        assertAxisReturns(
            "LastPeriods(0, [Time].[1998])", "");
        assertAxisReturns(
            "LastPeriods(1, [Time].[1998])", "[Time].[Time].[1998]");
        assertAxisReturns(
            "LastPeriods(-1, [Time].[1998])", "[Time].[Time].[1998]");
        assertAxisReturns(
            "LastPeriods(2, [Time].[1998])",
            "[Time].[Time].[1997]\n"
            + "[Time].[Time].[1998]");
        assertAxisReturns(
            "LastPeriods(-2, [Time].[1997])",
            "[Time].[Time].[1997]\n"
            + "[Time].[Time].[1998]");
        assertAxisReturns(
            "LastPeriods(5000, [Time].[1998])",
            "[Time].[Time].[1997]\n"
            + "[Time].[Time].[1998]");
        assertAxisReturns(
            "LastPeriods(-5000, [Time].[1997])",
            "[Time].[Time].[1997]\n"
            + "[Time].[Time].[1998]");
        assertAxisReturns(
            "LastPeriods(2, [Time].[1998].[Q2])",
            "[Time].[Time].[1998].[Q1]\n"
            + "[Time].[Time].[1998].[Q2]");
        assertAxisReturns(
            "LastPeriods(4, [Time].[1998].[Q2])",
            "[Time].[Time].[1997].[Q3]\n"
            + "[Time].[Time].[1997].[Q4]\n"
            + "[Time].[Time].[1998].[Q1]\n"
            + "[Time].[Time].[1998].[Q2]");
        assertAxisReturns(
            "LastPeriods(-2, [Time].[1997].[Q2])",
            "[Time].[Time].[1997].[Q2]\n"
            + "[Time].[Time].[1997].[Q3]");
        assertAxisReturns(
            "LastPeriods(-4, [Time].[1997].[Q2])",
            "[Time].[Time].[1997].[Q2]\n"
            + "[Time].[Time].[1997].[Q3]\n"
            + "[Time].[Time].[1997].[Q4]\n"
            + "[Time].[Time].[1998].[Q1]");
        assertAxisReturns(
            "LastPeriods(5000, [Time].[1998].[Q2])",
            "[Time].[Time].[1997].[Q1]\n"
            + "[Time].[Time].[1997].[Q2]\n"
            + "[Time].[Time].[1997].[Q3]\n"
            + "[Time].[Time].[1997].[Q4]\n"
            + "[Time].[Time].[1998].[Q1]\n"
            + "[Time].[Time].[1998].[Q2]");
        assertAxisReturns(
            "LastPeriods(-5000, [Time].[1998].[Q2])",
            "[Time].[Time].[1998].[Q2]\n"
            + "[Time].[Time].[1998].[Q3]\n"
            + "[Time].[Time].[1998].[Q4]");

        assertAxisReturns(
            "LastPeriods(2, [Time].[1998].[Q2].[5])",
            "[Time].[Time].[1998].[Q2].[4]\n"
            + "[Time].[Time].[1998].[Q2].[5]");
        assertAxisReturns(
            "LastPeriods(12, [Time].[1998].[Q2].[5])",
            "[Time].[Time].[1997].[Q2].[6]\n"
            + "[Time].[Time].[1997].[Q3].[7]\n"
            + "[Time].[Time].[1997].[Q3].[8]\n"
            + "[Time].[Time].[1997].[Q3].[9]\n"
            + "[Time].[Time].[1997].[Q4].[10]\n"
            + "[Time].[Time].[1997].[Q4].[11]\n"
            + "[Time].[Time].[1997].[Q4].[12]\n"
            + "[Time].[Time].[1998].[Q1].[1]\n"
            + "[Time].[Time].[1998].[Q1].[2]\n"
            + "[Time].[Time].[1998].[Q1].[3]\n"
            + "[Time].[Time].[1998].[Q2].[4]\n"
            + "[Time].[Time].[1998].[Q2].[5]");
        assertAxisReturns(
            "LastPeriods(-2, [Time].[1998].[Q2].[4])",
            "[Time].[Time].[1998].[Q2].[4]\n"
            + "[Time].[Time].[1998].[Q2].[5]");
        assertAxisReturns(
            "LastPeriods(-12, [Time].[1997].[Q2].[6])",
            "[Time].[Time].[1997].[Q2].[6]\n"
            + "[Time].[Time].[1997].[Q3].[7]\n"
            + "[Time].[Time].[1997].[Q3].[8]\n"
            + "[Time].[Time].[1997].[Q3].[9]\n"
            + "[Time].[Time].[1997].[Q4].[10]\n"
            + "[Time].[Time].[1997].[Q4].[11]\n"
            + "[Time].[Time].[1997].[Q4].[12]\n"
            + "[Time].[Time].[1998].[Q1].[1]\n"
            + "[Time].[Time].[1998].[Q1].[2]\n"
            + "[Time].[Time].[1998].[Q1].[3]\n"
            + "[Time].[Time].[1998].[Q2].[4]\n"
            + "[Time].[Time].[1998].[Q2].[5]");
        assertAxisReturns(
            "LastPeriods(2, [Gender].[M])",
            "[Customer].[Gender].[F]\n"
            + "[Customer].[Gender].[M]");
        assertAxisReturns(
            "LastPeriods(-2, [Gender].[F])",
            "[Customer].[Gender].[F]\n"
            + "[Customer].[Gender].[M]");
        assertAxisReturns(
            "LastPeriods(2, [Gender])",
            "[Customer].[Gender].[All Gender]");
        assertAxisReturns(
            "LastPeriods(2, [Gender].Parent)", "");
    }

    public void testParallelPeriod() {
        assertAxisReturns(
            "parallelperiod([Time].[Quarter], 1, [Time].[1998].[Q1])",
            "[Time].[Time].[1997].[Q4]");

        assertAxisReturns(
            "parallelperiod([Time].[Quarter], -1, [Time].[1997].[Q1])",
            "[Time].[Time].[1997].[Q2]");

        assertAxisReturns(
            "parallelperiod([Time].[Year], 1, [Time].[1998].[Q1])",
            "[Time].[Time].[1997].[Q1]");

        assertAxisReturns(
            "parallelperiod([Time].[Year], 1, [Time].[1998].[Q1].[1])",
            "[Time].[Time].[1997].[Q1].[1]");

        // No args, therefore finds parallel period to [Time].[1997], which
        // would be [Time].[1996], except that that doesn't exist, so null.
        assertAxisReturns("ParallelPeriod()", "");

        // Parallel period to [Time].[1997], which would be [Time].[1996],
        // except that that doesn't exist, so null.
        assertAxisReturns(
            "ParallelPeriod([Time].[Year], 1, [Time].[1997])", "");

        // one parameter, level 2 above member
        if (isDefaultNullMemberRepresentation()) {
            assertQueryReturns(
                "WITH MEMBER [Measures].[Foo] AS \n"
                + " ' ParallelPeriod([Time].[Year]).UniqueName '\n"
                + "SELECT {[Measures].[Foo]} ON COLUMNS\n"
                + "FROM [Sales]\n"
                + "WHERE [Time].[1997].[Q3].[8]",
                "Axis #0:\n"
                + "{[Time].[Time].[1997].[Q3].[8]}\n"
                + "Axis #1:\n"
                + "{[Measures].[Foo]}\n"
                + "Row #0: [Time].[Time].[#null]\n");
        }

        // one parameter, level 1 above member
        assertQueryReturns(
            "WITH MEMBER [Measures].[Foo] AS \n"
            + " ' ParallelPeriod([Time].[Quarter]).UniqueName '\n"
            + "SELECT {[Measures].[Foo]} ON COLUMNS\n"
            + "FROM [Sales]\n"
            + "WHERE [Time].[1997].[Q3].[8]",
            "Axis #0:\n"
            + "{[Time].[Time].[1997].[Q3].[8]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Foo]}\n"
            + "Row #0: [Time].[Time].[1997].[Q2].[5]\n");

        // one parameter, level same as member
        assertQueryReturns(
            "WITH MEMBER [Measures].[Foo] AS \n"
            + " ' ParallelPeriod([Time].[Month]).UniqueName '\n"
            + "SELECT {[Measures].[Foo]} ON COLUMNS\n"
            + "FROM [Sales]\n"
            + "WHERE [Time].[1997].[Q3].[8]",
            "Axis #0:\n"
            + "{[Time].[Time].[1997].[Q3].[8]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Foo]}\n"
            + "Row #0: [Time].[Time].[1997].[Q3].[7]\n");

        //  one parameter, level below member
        if (isDefaultNullMemberRepresentation()) {
            assertQueryReturns(
                "WITH MEMBER [Measures].[Foo] AS \n"
                + " ' ParallelPeriod([Time].[Month]).UniqueName '\n"
                + "SELECT {[Measures].[Foo]} ON COLUMNS\n"
                + "FROM [Sales]\n"
                + "WHERE [Time].[1997].[Q3]",
                "Axis #0:\n"
                + "{[Time].[Time].[1997].[Q3]}\n"
                + "Axis #1:\n"
                + "{[Measures].[Foo]}\n"
                + "Row #0: [Time].[Time].[#null]\n");
        }
    }

    public void _testParallelPeriodThrowsException() {
        assertQueryThrows(
            "select {parallelperiod([Time].[Year], 1)} on columns "
            + "from [Sales] where ([Time].[1998].[Q1].[2])",
            "This should say something about Time appearing on two different axes (slicer an columns)");
    }

    public void testParallelPeriodDepends() {
        getTestContext().assertMemberExprDependsOn(
            "ParallelPeriod([Time].[Quarter], 2.0)", setOf("[Time].[Time]"));
        getTestContext().assertMemberExprDependsOn(
            "ParallelPeriod([Time].[Quarter], 2.0, [Time].[1997].[Q3])",
            Collections.<String>emptySet());
        getTestContext().assertMemberExprDependsOn(
            "ParallelPeriod()",
            setOf("[Time].[Time]"));
        getTestContext().assertMemberExprDependsOn(
            "ParallelPeriod([Product].[Food])", setOf("[Product].[Products]"));
        // [Gender].[M] is used here as a numeric expression!
        // The numeric expression DOES depend upon [Product].
        // The expression as a whole depends upon everything except [Gender].
        Set<String> s1 = TestContext.allHiersExcept("[Customer].[Gender]");
        getTestContext().assertMemberExprDependsOn(
            "ParallelPeriod([Product].[Product Family], [Gender].[M], [Product].[Food])",
            s1);
        // As above
        Set<String> s11 = TestContext.allHiersExcept("[Customer].[Gender]");
        getTestContext().assertMemberExprDependsOn(
            "ParallelPeriod([Product].[Product Family], [Gender].[M])", s11);
        getTestContext().assertSetExprDependsOn(
            "parallelperiod([Time].[Time].CurrentMember)",
            setOf("[Time].[Time]"));
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
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales], [Customer].[Marital Status].[M]}\n"
            + "{[Measures].[Unit Sales], [Customer].[Marital Status].[S]}\n"
            + "{[Measures].[Prev Unit Sales], [Customer].[Marital Status].[M]}\n"
            + "{[Measures].[Prev Unit Sales], [Customer].[Marital Status].[S]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[1997].[Q3]}\n"
            + "Row #0: 32,815\n"
            + "Row #0: 33,033\n"
            + "Row #0: 33,101\n"
            + "Row #0: 33,190\n");
    }

    public void testParallelPeriodLevel() {
        assertQueryReturns(
            "with "
            + "    member [Measures].[Prev Unit Sales] as "
            + "        '([Measures].[Unit Sales], parallelperiod([Time].[Quarter]))' "
            + "select "
            + "    crossjoin({[Measures].[Unit Sales], [Measures].[Prev Unit Sales]}, {[Marital Status].[All Marital Status].[M]}) on columns, "
            + "    {[Time].[1997].[Q3].[8]} on rows "
            + "from  "
            + "    [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales], [Customer].[Marital Status].[M]}\n"
            + "{[Measures].[Prev Unit Sales], [Customer].[Marital Status].[M]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[1997].[Q3].[8]}\n"
            + "Row #0: 10,957\n"
            + "Row #0: 10,280\n");
    }

    public void testPlus() {
        getTestContext().assertExprDependsOn(
            "1 + 2", Collections.<String>emptySet());
        Set<String> s1 =
            TestContext.allHiersExcept("[Measures]", "[Customer].[Gender]");
        getTestContext().assertExprDependsOn(
            "([Measures].[Unit Sales], [Gender].[F]) + 2", s1);

        assertExprReturns("1+2", "3");
        assertExprReturns("5 + " + NullNumericExpr, "5"); // 5 + null --> 5
        assertExprReturns(NullNumericExpr + " + " + NullNumericExpr, "");
        assertExprReturns(NullNumericExpr + " + 0", "0");
    }

    public void testMinus() {
        assertExprReturns("1-3", "-2");
        assertExprReturns("5 - " + NullNumericExpr, "5"); // 5 - null --> 5
        assertExprReturns(NullNumericExpr + " - - 2", "2");
        assertExprReturns(NullNumericExpr + " - " + NullNumericExpr, "");
    }

    public void testMinus_bug1234759()
    {
        assertQueryReturns(
            "WITH MEMBER [Customers].[USAMinusMexico]\n"
            + "AS '([Customers].[All Customers].[USA] - [Customers].[All Customers].[Mexico])'\n"
            + "SELECT {[Measures].[Unit Sales]} ON COLUMNS,\n"
            + "{[Customers].[All Customers].[USA], [Customers].[All Customers].[Mexico],\n"
            + "[Customers].[USAMinusMexico]} ON ROWS\n"
            + "FROM [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Customers].[USA]}\n"
            + "{[Customer].[Customers].[Mexico]}\n"
            + "{[Customer].[Customers].[USAMinusMexico]}\n"
            + "Row #0: 266,773\n"
            + "Row #1: \n"
            + "Row #2: 266,773\n"
            // with bug 1234759, this was null
            + "");
    }

    public void testMinusAssociativity() {
        // right-associative would give 11-(7-5) = 9, which is wrong
        assertExprReturns("11-7-5", "-1");
    }

    public void testMultiply() {
        assertExprReturns("4*7", "28");
        assertExprReturns("5 * " + NullNumericExpr, ""); // 5 * null --> null
        assertExprReturns(NullNumericExpr + " * - 2", "");
        assertExprReturns(NullNumericExpr + " - " + NullNumericExpr, "");
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
        final String desiredResult =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[Stores].[All Stores]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Store Sales]}\n"
            + "{[Measures].[A]}\n"
            + "Row #0: 565,238.13\n"
            + "Row #1: 319,494,143,605.90\n";
        assertQueryReturns(
            "WITH MEMBER [Measures].[A] AS\n"
            + " '([Measures].[Store Sales] * [Measures].[Store Sales])'\n"
            + "SELECT {[Stores]} ON COLUMNS,\n"
            + " {[Measures].[Store Sales], [Measures].[A]} ON ROWS\n"
            + "FROM Sales",
            desiredResult);
        // as above, no parentheses
        assertQueryReturns(
            "WITH MEMBER [Measures].[A] AS\n"
            + " '[Measures].[Store Sales] * [Measures].[Store Sales]'\n"
            + "SELECT {[Stores]} ON COLUMNS,\n"
            + " {[Measures].[Store Sales], [Measures].[A]} ON ROWS\n"
            + "FROM Sales",
            desiredResult);
        // as above, plus 0
        assertQueryReturns(
            "WITH MEMBER [Measures].[A] AS\n"
            + " '[Measures].[Store Sales] * [Measures].[Store Sales] + 0'\n"
            + "SELECT {[Stores]} ON COLUMNS,\n"
            + " {[Measures].[Store Sales], [Measures].[A]} ON ROWS\n"
            + "FROM Sales",
            desiredResult);
    }

    public void testDivide() {
        assertExprReturns("10 / 5", "2");
        assertExprReturns(NullNumericExpr + " / - 2", "");
        assertExprReturns(NullNumericExpr + " / " + NullNumericExpr, "");

        // default behavior
        propSaver.set(propSaver.props.NullDenominatorProducesNull, false);

        assertExprReturns("-2 / " + NullNumericExpr, "Infinity");
        assertExprReturns("0 / 0", "NaN");
        assertExprReturns("-3 / (2 - 2)", "-Infinity");

        assertExprReturns("NULL/1", "");
        assertExprReturns("NULL/NULL", "");
        assertExprReturns("1/NULL", "Infinity");

        // when NullOrZeroDenominatorProducesNull is set to true
        propSaver.set(propSaver.props.NullDenominatorProducesNull, true);

        assertExprReturns("-2 / " + NullNumericExpr, "");
        assertExprReturns("0 / 0", "NaN");
        assertExprReturns("-3 / (2 - 2)", "-Infinity");

        assertExprReturns("NULL/1", "");
        assertExprReturns("NULL/NULL", "");
        assertExprReturns("1/NULL", "");
    }

    public void testDividePrecedence() {
        assertExprReturns("24 / 4 / 2 * 10 - -1", "31");
    }

    public void testMod() {
        // the following tests are consistent with excel xp

        assertExprReturns("mod(11, 3)", "2");
        assertExprReturns("mod(-12, 3)", "0");

        // can handle non-ints, using the formula MOD(n, d) = n - d * INT(n / d)
        assertExprReturns("mod(7.2, 3)", 1.2, 0.0001);
        assertExprReturns("mod(7.2, 3.2)", .8, 0.0001);
        assertExprReturns("mod(7.2, -3.2)", -2.4, 0.0001);

        // per Excel doc "sign of result is same as divisor"
        assertExprReturns("mod(3, 2)", "1");
        assertExprReturns("mod(-3, 2)", "1");
        assertExprReturns("mod(3, -2)", "-1");
        assertExprReturns("mod(-3, -2)", "-1");

        assertExprThrows(
            "mod(4, 0)",
            "java.lang.ArithmeticException: / by zero");
        assertExprThrows(
            "mod(0, 0)",
            "java.lang.ArithmeticException: / by zero");
    }

    public void testUnaryMinus() {
        assertExprReturns("-3", "-3");
    }

    public void testUnaryMinusMember() {
        assertExprReturns(
            "- ([Measures].[Unit Sales],[Gender].[F])",
            "-131,558");
    }

    public void testUnaryMinusPrecedence() {
        assertExprReturns("1 - -10.5 * 2 -3", "19");
    }

    public void testNegativeZero() {
        assertExprReturns("-0.0", "0");
    }

    public void testNegativeZero1() {
        assertExprReturns("-(0.0)", "0");
    }

    public void testNegativeZeroSubtract() {
        assertExprReturns("-0.0 - 0.0", "0");
    }

    public void testNegativeZeroMultiply() {
        assertExprReturns("-1 * 0", "0");
    }

    public void testNegativeZeroDivide() {
        assertExprReturns("-0.0 / 2", "0");
    }

    public void testString() {
        // The String(Integer,Char) function requires us to implicitly cast a
        // string to a char.
        assertQueryReturns(
            "with member measures.x as 'String(3, \"yahoo\")'\n"
            + "select measures.x on 0 from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[x]}\n"
            + "Row #0: yyy\n");
        // String is converted to char by taking first character
        assertExprReturns("String(3, \"yahoo\")", "yyy"); // SSAS agrees
        // Integer is converted to char by converting to string and taking first
        // character
        if (Bug.Ssas2005Compatible) {
            // SSAS2005 can implicitly convert an integer (32) to a string, and
            // then to a char by taking the first character. Mondrian requires
            // an explicit cast.
            assertExprReturns("String(3, 32)", "333");
            assertExprReturns("String(8, -5)", "--------");
        } else {
            assertExprReturns("String(3, Cast(32 as string))", "333");
            assertExprReturns("String(8, Cast(-5 as string))", "--------");
        }
        // Error if length<0
        assertExprReturns("String(0, 'x')", ""); // SSAS agrees
        assertExprThrows(
            "String(-1, 'x')", "NegativeArraySizeException"); // SSAS agrees
        assertExprThrows(
            "String(-200, 'x')", "NegativeArraySizeException"); // SSAS agrees
    }

    public void testStringConcat() {
        assertExprReturns(
            " \"foo\" || \"bar\"  ",
            "foobar");
    }

    public void testStringConcat2() {
        assertExprReturns(
            " \"foo\" || [Gender].[M].Name || \"\" ",
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
        Set<String> s1 = TestContext.allHiersExcept("[Store].[Stores]");
        getTestContext().assertSetExprDependsOn(
            "NonEmptyCrossJoin([Store].[USA].Children, [Gender].Children)", s1);

        assertAxisReturns(
            "NonEmptyCrossJoin("
            + "[Customers].[All Customers].[USA].[CA].Children, "
            + "[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].Children)",
            "{[Customer].[Customers].[USA].[CA].[Bellflower], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Light Beer]}\n"
            + "{[Customer].[Customers].[USA].[CA].[Downey], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Imported Beer]}\n"
            + "{[Customer].[Customers].[USA].[CA].[Glendale], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Imported Beer]}\n"
            + "{[Customer].[Customers].[USA].[CA].[Glendale], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Light Beer]}\n"
            + "{[Customer].[Customers].[USA].[CA].[Grossmont], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Light Beer]}\n"
            + "{[Customer].[Customers].[USA].[CA].[Imperial Beach], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Light Beer]}\n"
            + "{[Customer].[Customers].[USA].[CA].[La Jolla], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Imported Beer]}\n"
            + "{[Customer].[Customers].[USA].[CA].[Lincoln Acres], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Imported Beer]}\n"
            + "{[Customer].[Customers].[USA].[CA].[Lincoln Acres], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Light Beer]}\n"
            + "{[Customer].[Customers].[USA].[CA].[Long Beach], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Light Beer]}\n"
            + "{[Customer].[Customers].[USA].[CA].[Los Angeles], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Imported Beer]}\n"
            + "{[Customer].[Customers].[USA].[CA].[Newport Beach], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Imported Beer]}\n"
            + "{[Customer].[Customers].[USA].[CA].[Pomona], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Imported Beer]}\n"
            + "{[Customer].[Customers].[USA].[CA].[Pomona], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Light Beer]}\n"
            + "{[Customer].[Customers].[USA].[CA].[San Gabriel], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Light Beer]}\n"
            + "{[Customer].[Customers].[USA].[CA].[West Covina], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Imported Beer]}\n"
            + "{[Customer].[Customers].[USA].[CA].[West Covina], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Light Beer]}\n"
            + "{[Customer].[Customers].[USA].[CA].[Woodland Hills], [Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Imported Beer]}");

        // empty set
        assertAxisReturns(
            "NonEmptyCrossJoin({Gender.Parent}, {Stores.Parent})", "");
        assertAxisReturns(
            "NonEmptyCrossJoin({Stores.Parent}, Gender.Children)", "");
        assertAxisReturns("NonEmptyCrossJoin(Stores.Members, {})", "");

        // same dimension twice
        assertAxisThrows(
            "NonEmptyCrossJoin({Store.[USA]}, {Store.[USA].[CA]})",
            "Tuple contains more than one member of hierarchy "
            + "'[Store].[Stores]'.");
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
        assertBooleanExprReturns(
            " Store.[USA].parent IS Store.[All Stores]", true);
        assertBooleanExprReturns(
            " [Store].[USA].[CA].parent IS [Store].[Mexico]", false);
    }

    public void testIsString() {
        assertExprThrows(
            " [Store].[USA].Name IS \"USA\" ",
            "No function matches signature '<String> IS <String>'");
    }

    public void testIsNumeric() {
        assertExprThrows(
            " [Store].[USA].Level.Ordinal IS 25 ",
            "No function matches signature '<Numeric Expression> IS <Numeric Expression>'");
    }

    public void testIsTuple() {
        assertBooleanExprReturns(
            " (Store.[USA], Gender.[M]) IS (Store.[USA], Gender.[M])", true);
        assertBooleanExprReturns(
            " (Store.[USA], Gender.[M]) IS (Gender.[M], Store.[USA])", true);
        assertBooleanExprReturns(
            " (Store.[USA], Gender.[M]) IS (Gender.[M], Store.[USA]) "
            + "OR [Gender] IS NULL",
            true);
        assertBooleanExprReturns(
            " (Store.[USA], Gender.[M]) IS (Gender.[M], Store.[USA]) "
            + "AND [Gender] IS NULL",
            false);
        assertBooleanExprReturns(
            " (Store.[USA], Gender.[M]) IS (Store.[USA], Gender.[F])",
            false);
        assertBooleanExprReturns(
            " (Store.[USA], Gender.[M]) IS (Store.[USA])",
            false);
        assertBooleanExprReturns(
            " (Store.[USA], Gender.[M]) IS Store.[USA]",
            false);
    }

    public void testIsLevel() {
        assertBooleanExprReturns(
            " Store.[USA].level IS Store.[Store Country] ", true);
        assertBooleanExprReturns(
            " Store.[USA].[CA].level IS Store.[Store Country] ", false);
    }

    public void testIsHierarchy() {
        assertBooleanExprReturns(
            " Store.[USA].hierarchy IS Store.[Mexico].hierarchy ", true);
        assertBooleanExprReturns(
            " Store.[USA].hierarchy IS Gender.[M].hierarchy ", false);
    }

    public void testIsDimension() {
        assertBooleanExprReturns(" Store.[USA].dimension IS Store ", true);
        assertBooleanExprReturns(" Gender.[M].dimension IS Store ", false);
    }

    public void testIsWithMismatchedArguments() {
        // Hierarchy is Member
        assertBooleanExprReturns("Store.Stores IS Store.Stores.USA", false);
        // Hierarchy is Level
        assertBooleanExprReturns(
            "Store.Stores IS Store.Stores.[Store Country]",
            true);
        assertBooleanExprReturns(
            "Store.Stores IS Store.Stores.[Store City]",
            true); // correct?
        assertBooleanExprReturns(
            "Product.Products IS Store.Stores.[Store City]",
            false);
        // Level is Hierarchy
        assertBooleanExprReturns(
            "Store.Stores.[Store Country] IS Store.Stores",
            true);
        // Dimension is Hierarchy
        assertBooleanExprReturns(
            "Store IS Store.Stores",
            true);
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

        assertBooleanExprReturns(
            "[Product].CurrentMember.Level.Ordinal = 2.0", false);
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
        assertBooleanExprReturns(
            NullNumericExpr + " " + op + " " + NullNumericExpr, false);
    }

    public void testDistinctTwoMembers() {
        getTestContext().withCube("HR").assertAxisReturns(
            "Distinct({[Employees].[All Employees].[Sheri Nowmer].[Donna Arnold],"
            + "[Employee].[Employees].[Sheri Nowmer].[Donna Arnold]})",
            "[Employee].[Employees].[Sheri Nowmer].[Donna Arnold]");
    }

    public void testDistinctThreeMembers() {
        getTestContext().withCube("HR").assertAxisReturns(
            "Distinct({[Employees].[All Employees].[Sheri Nowmer].[Donna Arnold],"
            + "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz],"
            + "[Employees].[All Employees].[Sheri Nowmer].[Donna Arnold]})",
            "[Employee].[Employees].[Sheri Nowmer].[Donna Arnold]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Darren Stanz]");
    }

    public void testDistinctFourMembers() {
        getTestContext().withCube("HR").assertAxisReturns(
            "Distinct({[Employees].[All Employees].[Sheri Nowmer].[Donna Arnold],"
            + "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz],"
            + "[Employees].[All Employees].[Sheri Nowmer].[Donna Arnold],"
            + "[Employee].[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz]})",
            "[Employee].[Employees].[Sheri Nowmer].[Donna Arnold]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Darren Stanz]");
    }

    public void testDistinctTwoTuples() {
        getTestContext().assertAxisReturns(
            "Distinct({([Time].[1997],[Store].[All Stores].[Mexico]), "
            + "([Time].[1997], [Store].[All Stores].[Mexico])})",
            "{[Time].[Time].[1997], [Store].[Stores].[Mexico]}");
    }

    public void testDistinctSomeTuples() {
        getTestContext().assertAxisReturns(
            "Distinct({([Time].[1997],[Store].[All Stores].[Mexico]), "
            + "crossjoin({[Time].[1997]},{[Store].[All Stores].children})})",
            "{[Time].[Time].[1997], [Store].[Stores].[Mexico]}\n"
            + "{[Time].[Time].[1997], [Store].[Stores].[Canada]}\n"
            + "{[Time].[Time].[1997], [Store].[Stores].[USA]}");
    }

    /**
     * Make sure that slicer is in force when expression is applied
     * on axis, E.g. select filter([Customers].members, [Unit Sales] > 100)
     * from sales where ([Time].[1998])
     */
    public void testFilterWithSlicer() {
        Result result = executeQuery(
            "select {[Measures].[Unit Sales]} on columns,\n"
            + " filter([Customers].[USA].children,\n"
            + "        [Measures].[Unit Sales] > 20000) on rows\n"
            + "from Sales\n"
            + "where ([Time].[1997].[Q1])");
        Axis rows = result.getAxes()[1];
        // if slicer were ignored, there would be 3 rows
        assertEquals(1, rows.getPositions().size());
        Cell cell = result.getCell(new int[]{0, 0});
        assertEquals("30,114", cell.getFormattedValue());
    }

    public void testFilterCompound() {
        Result result = executeQuery(
            "select {[Measures].[Unit Sales]} on columns,\n"
            + "  Filter(\n"
            + "    CrossJoin(\n"
            + "      [Gender].Children,\n"
            + "      [Customers].[USA].Children),\n"
            + "    [Measures].[Unit Sales] > 9500) on rows\n"
            + "from Sales\n"
            + "where ([Time].[1997].[Q1])");
        List<Position> rows = result.getAxes()[1].getPositions();
        assertEquals(3, rows.size());
        assertEquals("F", rows.get(0).get(0).getName());
        assertEquals("WA", rows.get(0).get(1).getName());
        assertEquals("M", rows.get(1).get(0).getName());
        assertEquals("OR", rows.get(1).get(1).getName());
        assertEquals("M", rows.get(2).get(0).getName());
        assertEquals("WA", rows.get(2).get(1).getName());
    }

    public void testGenerateDepends() {
        getTestContext().assertSetExprDependsOn(
            "Generate([Product].CurrentMember.Children, Crossjoin({[Product].CurrentMember}, Crossjoin([Store].[Store State].Members, [Store Type].Members)), ALL)",
            setOf("[Product].[Products]"));
        getTestContext().assertSetExprDependsOn(
            "Generate([Product].[Products].[All Products].Children, Crossjoin({[Product].[Products].CurrentMember}, Crossjoin([Store].[Store State].Members, [Store Type].Members)), ALL)",
            Collections.<String>emptySet());
        getTestContext().assertSetExprDependsOn(
            "Generate({[Store].[USA], [Store].[USA].[CA]}, {[Stores].CURRENTMEMBER.Children})",
            Collections.<String>emptySet());
        getTestContext().assertSetExprDependsOn(
            "Generate({[Store].[USA], [Store].[USA].[CA]}, {[Gender].CurrentMember})",
            setOf("[Customer].[Gender]"));
        getTestContext().assertSetExprDependsOn(
            "Generate({[Store].[USA], [Store].[USA].[CA]}, {[Gender].[M]})",
            Collections.<String>emptySet());
    }

    public void testGenerate() {
        assertAxisReturns(
            "Generate({[Store].[USA], [Store].[USA].[CA]}, {[Stores].CURRENTMEMBER.Children})",
            "[Store].[Stores].[USA].[CA]\n"
            + "[Store].[Stores].[USA].[OR]\n"
            + "[Store].[Stores].[USA].[WA]\n"
            + "[Store].[Stores].[USA].[CA].[Alameda]\n"
            + "[Store].[Stores].[USA].[CA].[Beverly Hills]\n"
            + "[Store].[Stores].[USA].[CA].[Los Angeles]\n"
            + "[Store].[Stores].[USA].[CA].[San Diego]\n"
            + "[Store].[Stores].[USA].[CA].[San Francisco]");
    }

    public void testGenerateNonSet() {
        // SSAS implicitly converts arg #2 to a set
        assertAxisReturns(
            "Generate({[Store].[USA], [Store].[USA].[CA]}, [Stores].PrevMember, ALL)",
            "[Store].[Stores].[Mexico]\n"
            + "[Store].[Stores].[Mexico].[Zacatecas]");

        // SSAS implicitly converts arg #1 to a set
        assertAxisReturns(
            "Generate([Store].[USA], [Stores].PrevMember, ALL)",
            "[Store].[Stores].[Mexico]");
    }

    public void testGenerateAll() {
        assertAxisReturns(
            "Generate({[Store].[USA].[CA], [Store].[USA].[OR].[Portland]},"
            + " Ascendants([Stores].CURRENTMEMBER),"
            + " ALL)",
            "[Store].[Stores].[USA].[CA]\n"
            + "[Store].[Stores].[USA]\n"
            + "[Store].[Stores].[All Stores]\n"
            + "[Store].[Stores].[USA].[OR].[Portland]\n"
            + "[Store].[Stores].[USA].[OR]\n"
            + "[Store].[Stores].[USA]\n"
            + "[Store].[Stores].[All Stores]");
    }

    public void testGenerateUnique() {
        assertAxisReturns(
            "Generate({[Store].[USA].[CA], [Store].[USA].[OR].[Portland]},"
            + " Ascendants([Stores].CURRENTMEMBER))",
            "[Store].[Stores].[USA].[CA]\n"
            + "[Store].[Stores].[USA]\n"
            + "[Store].[Stores].[All Stores]\n"
            + "[Store].[Stores].[USA].[OR].[Portland]\n"
            + "[Store].[Stores].[USA].[OR]");
    }

    public void testGenerateUniqueTuple() {
        assertAxisReturns(
            "Generate({([Store].[USA].[CA],[Product].[All Products]), "
            + "([Store].[USA].[CA],[Product].[All Products])},"
            + "{([Stores].CURRENTMEMBER, [Product].CurrentMember)})",
            "{[Store].[Stores].[USA].[CA], [Product].[Products].[All Products]}");
    }

    public void testGenerateCrossJoin() {
        // Note that the different regions have different Top 2.
        assertAxisReturns(
            "Generate({[Store].[USA].[CA], [Store].[USA].[CA].[San Francisco]},\n"
            + "  CrossJoin({[Stores].CURRENTMEMBER},\n"
            + "    TopCount([Product].[Brand Name].members, \n"
            + "    2,\n"
            + "    [Measures].[Unit Sales])))",
            "{[Store].[Stores].[USA].[CA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos]}\n"
            + "{[Store].[Stores].[USA].[CA], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale]}\n"
            + "{[Store].[Stores].[USA].[CA].[San Francisco], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony]}\n"
            + "{[Store].[Stores].[USA].[CA].[San Francisco], [Product].[Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top]}");
    }

    public void testGenerateString() {
        assertExprReturns(
            "Generate({Time.[1997], Time.[1998]},"
            + " Time.[Time].CurrentMember.Name)",
            "19971998");
        assertExprReturns(
            "Generate({Time.[1997], Time.[1998]},"
            + " Time.[Time].CurrentMember.Name, \" and \")",
            "1997 and 1998");
    }

    public void testHead() {
        assertAxisReturns(
            "Head([Store].[Stores].Children, 2)",
            "[Store].[Stores].[Canada]\n"
            + "[Store].[Stores].[Mexico]");
    }

    public void testHeadNegative() {
        assertAxisReturns(
            "Head([Store].[Stores].Children, 2 - 3)",
            "");
    }

    public void testHeadDefault() {
        assertAxisReturns(
            "Head([Store].[Stores].Children)",
            "[Store].[Stores].[Canada]");
    }

    public void testHeadOvershoot() {
        assertAxisReturns(
            "Head([Store].[Stores].Children, 2 + 2)",
            "[Store].[Stores].[Canada]\n"
            + "[Store].[Stores].[Mexico]\n"
            + "[Store].[Stores].[USA]");
    }

    public void testHeadEmpty() {
        assertAxisReturns(
            "Head([Gender].[F].Children, 2)",
            "");

        assertAxisReturns(
            "Head([Gender].[F].Children)",
            "");
    }

    /**
     * Test case for bug 2488492, "Union between calc mem and head function
     * throws exception"
     */
    public void testHeadBug() {
        assertQueryReturns(
            "SELECT\n"
            + "                        UNION(\n"
            + "                            {([Customers].CURRENTMEMBER)},\n"
            + "                            HEAD(\n"
            + "                                {([Customers].CURRENTMEMBER)},\n"
            + "                                IIF(\n"
            + "                                    COUNT(\n"
            + "                                        FILTER(\n"
            + "                                            DESCENDANTS(\n"
            + "                                                [Customers].CURRENTMEMBER,\n"
            + "                                                [Customers].[Country]),\n"
            + "                                            [Measures].[Unit Sales] >= 66),\n"
            + "                                        INCLUDEEMPTY)> 0,\n"
            + "                                    1,\n"
            + "                                    0)),\n"
            + "                            ALL)\n"
            + "    ON AXIS(0)\n"
            + "FROM\n"
            + "    [Sales]\n",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Customer].[Customers].[All Customers]}\n"
            + "{[Customer].[Customers].[All Customers]}\n"
            + "Row #0: 266,773\n"
            + "Row #0: 266,773\n");

        assertQueryReturns(
            "WITH\n"
            + "    MEMBER\n"
            + "        [Customers].[COG_OQP_INT_t2]AS '1',\n"
            + "        SOLVE_ORDER = 65535\n"
            + "SELECT\n"
            + "                        UNION(\n"
            + "                            {([Customers].[COG_OQP_INT_t2])},\n"
            + "                            HEAD(\n"
            + "                                {([Customers].CURRENTMEMBER)},\n"
            + "                                IIF(\n"
            + "                                    COUNT(\n"
            + "                                        FILTER(\n"
            + "                                            DESCENDANTS(\n"
            + "                                                [Customers].CURRENTMEMBER,\n"
            + "                                                [Customers].[Country]),\n"
            + "                                            [Measures].[Unit Sales]>= 66),\n"
            + "                                        INCLUDEEMPTY)> 0,\n"
            + "                                    1,\n"
            + "                                    0)),\n"
            + "                            ALL)\n"
            + "    ON AXIS(0)\n"
            + "FROM\n"
            + "    [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Customer].[Customers].[COG_OQP_INT_t2]}\n"
            + "{[Customer].[Customers].[All Customers]}\n"
            + "Row #0: 1\n"
            + "Row #0: 266,773\n");

        // More minimal test case. Also demonstrates similar problem with Tail.
        assertAxisReturns(
            "Union(\n"
            + "  Union(\n"
            + "    Tail([Customers].[USA].[CA].Children, 2),\n"
            + "    Head([Customers].[USA].[WA].Children, 2),\n"
            + "    ALL),\n"
            + "  Tail([Customers].[USA].[OR].Children, 2),"
            + "  ALL)",
            "[Customer].[Customers].[USA].[CA].[West Covina]\n"
            + "[Customer].[Customers].[USA].[CA].[Woodland Hills]\n"
            + "[Customer].[Customers].[USA].[WA].[Anacortes]\n"
            + "[Customer].[Customers].[USA].[WA].[Ballard]\n"
            + "[Customer].[Customers].[USA].[OR].[W. Linn]\n"
            + "[Customer].[Customers].[USA].[OR].[Woodburn]");
    }

    public void testHierarchize() {
        assertAxisReturns(
            "Hierarchize(\n"
            + "    {[Product].[All Products], "
            + "     [Product].[Food],\n"
            + "     [Product].[Drink],\n"
            + "     [Product].[Non-Consumable],\n"
            + "     [Product].[Food].[Eggs],\n"
            + "     [Product].[Drink].[Dairy]})",

            "[Product].[Products].[All Products]\n"
            + "[Product].[Products].[Drink]\n"
            + "[Product].[Products].[Drink].[Dairy]\n"
            + "[Product].[Products].[Food]\n"
            + "[Product].[Products].[Food].[Eggs]\n"
            + "[Product].[Products].[Non-Consumable]");
    }

    public void testHierarchizePost() {
        assertAxisReturns(
            "Hierarchize(\n"
            + "    {[Product].[All Products], "
            + "     [Product].[Food],\n"
            + "     [Product].[Food].[Eggs],\n"
            + "     [Product].[Drink].[Dairy]},\n"
            + "  POST)",

            "[Product].[Products].[Drink].[Dairy]\n"
            + "[Product].[Products].[Food].[Eggs]\n"
            + "[Product].[Products].[Food]\n"
            + "[Product].[Products].[All Products]");
    }

    public void testHierarchizePC() {
        getTestContext().withCube("HR").assertAxisReturns(
            "Hierarchize(\n"
            + "   { Subset([Employees].Members, 90, 10),\n"
            + "     Head([Employees].Members, 5) })",
            "[Employee].[Employees].[All Employees]\n"
            + "[Employee].[Employees].[Sheri Nowmer]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Derrick Whelply]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Derrick Whelply].[Beverly Baker]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Derrick Whelply].[Beverly Baker].[Shauna Wyro]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Derrick Whelply].[Pedro Castillo].[Lin Conley].[Paul Tays].[Cheryl Thorton].[Leopoldo Renfro]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Derrick Whelply].[Pedro Castillo].[Lin Conley].[Paul Tays].[Cheryl Thorton].[Donna Brockett]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Derrick Whelply].[Pedro Castillo].[Lin Conley].[Paul Tays].[Cheryl Thorton].[Laurie Anderson]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Derrick Whelply].[Pedro Castillo].[Lin Conley].[Paul Tays].[Cheryl Thorton].[Louis Gomez]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Derrick Whelply].[Pedro Castillo].[Lin Conley].[Paul Tays].[Cheryl Thorton].[Melvin Glass]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Derrick Whelply].[Pedro Castillo].[Lin Conley].[Paul Tays].[Cheryl Thorton].[Kristin Cohen]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Derrick Whelply].[Pedro Castillo].[Lin Conley].[Paul Tays].[Cheryl Thorton].[Susan Kharman]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Derrick Whelply].[Pedro Castillo].[Lin Conley].[Paul Tays].[Cheryl Thorton].[Gordon Kirschner]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Derrick Whelply].[Pedro Castillo].[Lin Conley].[Paul Tays].[Cheryl Thorton].[Geneva Kouba]\n"
            + "[Employee].[Employees].[Sheri Nowmer].[Derrick Whelply].[Pedro Castillo].[Lin Conley].[Paul Tays].[Cheryl Thorton].[Tricia Clark]");
    }

    public void testHierarchizeCrossJoinPre() {
        assertAxisReturns(
            "Hierarchize(\n"
            + "  CrossJoin(\n"
            + "    {[Product].[All Products], "
            + "     [Product].[Food],\n"
            + "     [Product].[Food].[Eggs],\n"
            + "     [Product].[Drink].[Dairy]},\n"
            + "    [Gender].MEMBERS),\n"
            + "  PRE)",

            "{[Product].[Products].[All Products], [Customer].[Gender].[All Gender]}\n"
            + "{[Product].[Products].[All Products], [Customer].[Gender].[F]}\n"
            + "{[Product].[Products].[All Products], [Customer].[Gender].[M]}\n"
            + "{[Product].[Products].[Drink].[Dairy], [Customer].[Gender].[All Gender]}\n"
            + "{[Product].[Products].[Drink].[Dairy], [Customer].[Gender].[F]}\n"
            + "{[Product].[Products].[Drink].[Dairy], [Customer].[Gender].[M]}\n"
            + "{[Product].[Products].[Food], [Customer].[Gender].[All Gender]}\n"
            + "{[Product].[Products].[Food], [Customer].[Gender].[F]}\n"
            + "{[Product].[Products].[Food], [Customer].[Gender].[M]}\n"
            + "{[Product].[Products].[Food].[Eggs], [Customer].[Gender].[All Gender]}\n"
            + "{[Product].[Products].[Food].[Eggs], [Customer].[Gender].[F]}\n"
            + "{[Product].[Products].[Food].[Eggs], [Customer].[Gender].[M]}");
    }

    public void testHierarchizeCrossJoinPost() {
        assertAxisReturns(
            "Hierarchize(\n"
            + "  CrossJoin(\n"
            + "    {[Product].[All Products], "
            + "     [Product].[Food],\n"
            + "     [Product].[Food].[Eggs],\n"
            + "     [Product].[Drink].[Dairy]},\n"
            + "    [Gender].MEMBERS),\n"
            + "  POST)",

            "{[Product].[Products].[Drink].[Dairy], [Customer].[Gender].[F]}\n"
            + "{[Product].[Products].[Drink].[Dairy], [Customer].[Gender].[M]}\n"
            + "{[Product].[Products].[Drink].[Dairy], [Customer].[Gender].[All Gender]}\n"
            + "{[Product].[Products].[Food].[Eggs], [Customer].[Gender].[F]}\n"
            + "{[Product].[Products].[Food].[Eggs], [Customer].[Gender].[M]}\n"
            + "{[Product].[Products].[Food].[Eggs], [Customer].[Gender].[All Gender]}\n"
            + "{[Product].[Products].[Food], [Customer].[Gender].[F]}\n"
            + "{[Product].[Products].[Food], [Customer].[Gender].[M]}\n"
            + "{[Product].[Products].[Food], [Customer].[Gender].[All Gender]}\n"
            + "{[Product].[Products].[All Products], [Customer].[Gender].[F]}\n"
            + "{[Product].[Products].[All Products], [Customer].[Gender].[M]}\n"
            + "{[Product].[Products].[All Products], [Customer].[Gender].[All Gender]}");
    }

    /**
     * Tests that the {@code Hierarchize} function works correctly when applied
     * to a level whose ordering is determined by an 'ordinal' property.
     *
     * <p>Test case for bug
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-96">MONDRIAN-96,
     * "NPE if both axes are empty"</a>.</p>
     */
    public void testHierarchizeOrdinal() {
        final String cubeName = "Sales_Hierarchize";
        final TestContext testContext = TestContext.instance().create(
            null,
            "<Cube name='" + cubeName + "'>\n"
            + "  <Dimensions>\n"
            + "    <Dimension name='Time_Alphabetical' table='time_by_day' key='Time Id' type='TIME'>\n"
            + "      <Attributes>\n"
            + "        <Attribute name='Year' keyColumn='the_year' levelType='TimeYears'/>\n"
            + "        <Attribute name='Quarter' levelType='TimeQuarters'>\n"
            + "            <Key>\n"
            + "                <Column name='the_year'/>\n"
            + "                <Column name='quarter'/>\n"
            + "            </Key>\n"
            + "            <Name>\n"
            + "                <Column name='quarter'/>\n"
            + "            </Name>\n"
            + "        </Attribute>\n"
            + "        <Attribute name='Month Name' levelType='TimeMonths'>\n"
            + "            <Key>\n"
            + "                <Column name='the_year'/>\n"
            + "                <Column name='month_of_year'/>\n"
            + "            </Key>\n"
            + "            <Name>\n"
            + "                <Column name='month_of_year'/>\n"
            + "            </Name>\n"
            + "            <OrderBy>\n"
            // + "                <Column name='the_year'/>\n"
            // + "                <Column name='quarter'/>\n"
            + "                <Column name='the_month'/>\n"
            + "            </OrderBy>\n"
            + "        </Attribute>\n"
            + "        <Attribute name='Time Id' keyColumn='time_id'/>\n"
            + "      </Attributes>\n"
            + "      <Hierarchies>\n"
            + "        <Hierarchy name='Time_Alphabetical' hasAll='false'>\n"
            + "          <Level attribute='Year'/>\n"
            + "          <Level attribute='Quarter'/>\n"
            + "          <Level attribute='Month Name'/>\n"
            + "        </Hierarchy>\n"
            + "        <Hierarchy name='Month_Alphabetical' hasAll='false'>\n"
            + "          <Level attribute='Month Name'/>\n"
            + "        </Hierarchy>\n"
            + "      </Hierarchies>\n"
            + "    </Dimension>\n"
            + "  </Dimensions>\n"
            + "  <MeasureGroups>\n"
            + "    <MeasureGroup name='sales' table='sales_fact_1997'>\n"
            + "      <Measures>\n"
            + "        <Measure name='Unit Sales' column='unit_sales' aggregator='sum' formatString='Standard' visible='false'/>\n"
            + "      </Measures>\n"
            + "      <DimensionLinks>\n"
            + "        <ForeignKeyLink dimension='Time_Alphabetical' foreignKeyColumn='time_id'/>\n"
            + "      </DimensionLinks>\n"
            + "    </MeasureGroup>\n"
            + "  </MeasureGroups>\n"
            + "</Cube>",
            null,
            null,
            null,
            null)
            .withCube(cubeName);

        if (!Bug.BugMondrian1173Fixed) {
            testContext.assertSimpleQuery();
            return;
        }
            // The [Time_Alphabetical] is ordered alphabetically by month
        if (!Bug.CubeRaggedFeature) {
            testContext.assertAxisReturns(
                "Head([Time_Alphabetical].[Time_Alphabetical].members, 7)",
                "[Time_Alphabetical].[Time_Alphabetical].[1997]\n"
                + "[Time_Alphabetical].[Time_Alphabetical].[1997].[Q1]\n"
                + "[Time_Alphabetical].[Time_Alphabetical].[1997].[Q1].[2]\n"
                + "[Time_Alphabetical].[Time_Alphabetical].[1997].[Q1].[1]\n"
                + "[Time_Alphabetical].[Time_Alphabetical].[1997].[Q1].[3]\n"
                + "[Time_Alphabetical].[Time_Alphabetical].[1997].[Q2]\n"
                + "[Time_Alphabetical].[Time_Alphabetical].[1997].[Q2].[4]");

        testContext.assertAxisReturns(
            "Hierarchize([Time_Alphabetical].[Time_Alphabetical].members)",
                "[Time_Alphabetical].[Time_Alphabetical].[1997]\n"
                + "[Time_Alphabetical].[Time_Alphabetical].[1997].[Q1]\n"
                + "[Time_Alphabetical].[Time_Alphabetical].[1997].[Q1].[2]\n"
                + "[Time_Alphabetical].[Time_Alphabetical].[1997].[Q1].[1]\n"
                + "[Time_Alphabetical].[Time_Alphabetical].[1997].[Q1].[3]\n"
                + "[Time_Alphabetical].[Time_Alphabetical].[1997].[Q2]\n"
                + "[Time_Alphabetical].[Time_Alphabetical].[1997].[Q2].[4]\n"
                + "[Time_Alphabetical].[Time_Alphabetical].[1997].[Q2].[6]\n"
                + "[Time_Alphabetical].[Time_Alphabetical].[1997].[Q2].[5]\n"
                + "[Time_Alphabetical].[Time_Alphabetical].[1997].[Q3]\n"
                + "[Time_Alphabetical].[Time_Alphabetical].[1997].[Q3].[8]\n"
                + "[Time_Alphabetical].[Time_Alphabetical].[1997].[Q3].[7]\n"
                + "[Time_Alphabetical].[Time_Alphabetical].[1997].[Q3].[9]\n"
                + "[Time_Alphabetical].[Time_Alphabetical].[1997].[Q4]\n"
                + "[Time_Alphabetical].[Time_Alphabetical].[1997].[Q4].[12]\n"
                + "[Time_Alphabetical].[Time_Alphabetical].[1997].[Q4].[11]\n"
                + "[Time_Alphabetical].[Time_Alphabetical].[1997].[Q4].[10]\n"
                + "[Time_Alphabetical].[Time_Alphabetical].[1998]\n"
                + "[Time_Alphabetical].[Time_Alphabetical].[1998].[Q1]\n"
                + "[Time_Alphabetical].[Time_Alphabetical].[1998].[Q1].[2]\n"
                + "[Time_Alphabetical].[Time_Alphabetical].[1998].[Q1].[1]\n"
                + "[Time_Alphabetical].[Time_Alphabetical].[1998].[Q1].[3]\n"
                + "[Time_Alphabetical].[Time_Alphabetical].[1998].[Q2]\n"
                + "[Time_Alphabetical].[Time_Alphabetical].[1998].[Q2].[4]\n"
                + "[Time_Alphabetical].[Time_Alphabetical].[1998].[Q2].[6]\n"
                + "[Time_Alphabetical].[Time_Alphabetical].[1998].[Q2].[5]\n"
                + "[Time_Alphabetical].[Time_Alphabetical].[1998].[Q3]\n"
                + "[Time_Alphabetical].[Time_Alphabetical].[1998].[Q3].[8]\n"
                + "[Time_Alphabetical].[Time_Alphabetical].[1998].[Q3].[7]\n"
                + "[Time_Alphabetical].[Time_Alphabetical].[1998].[Q3].[9]\n"
                + "[Time_Alphabetical].[Time_Alphabetical].[1998].[Q4]\n"
                + "[Time_Alphabetical].[Time_Alphabetical].[1998].[Q4].[12]\n"
                + "[Time_Alphabetical].[Time_Alphabetical].[1998].[Q4].[11]\n"
                + "[Time_Alphabetical].[Time_Alphabetical].[1998].[Q4].[10]");
        }

        // The [Month_Alphabetical] is a single-level hierarchy ordered
        // alphabetically by month.
        testContext.assertAxisReturns(
            "Hierarchize([Month_Alphabetical].members)",
            "[Time_Alphabetical].[Month_Alphabetical].[4]\n"
            + "[Time_Alphabetical].[Month_Alphabetical].[4]\n"
            + "[Time_Alphabetical].[Month_Alphabetical].[8]\n"
            + "[Time_Alphabetical].[Month_Alphabetical].[8]\n"
            + "[Time_Alphabetical].[Month_Alphabetical].[12]\n"
            + "[Time_Alphabetical].[Month_Alphabetical].[12]\n"
            + "[Time_Alphabetical].[Month_Alphabetical].[2]\n"
            + "[Time_Alphabetical].[Month_Alphabetical].[2]\n"
            + "[Time_Alphabetical].[Month_Alphabetical].[1]\n"
            + "[Time_Alphabetical].[Month_Alphabetical].[1]\n"
            + "[Time_Alphabetical].[Month_Alphabetical].[7]\n"
            + "[Time_Alphabetical].[Month_Alphabetical].[7]\n"
            + "[Time_Alphabetical].[Month_Alphabetical].[6]\n"
            + "[Time_Alphabetical].[Month_Alphabetical].[6]\n"
            + "[Time_Alphabetical].[Month_Alphabetical].[3]\n"
            + "[Time_Alphabetical].[Month_Alphabetical].[3]\n"
            + "[Time_Alphabetical].[Month_Alphabetical].[5]\n"
            + "[Time_Alphabetical].[Month_Alphabetical].[5]\n"
            + "[Time_Alphabetical].[Month_Alphabetical].[11]\n"
            + "[Time_Alphabetical].[Month_Alphabetical].[11]\n"
            + "[Time_Alphabetical].[Month_Alphabetical].[10]\n"
            + "[Time_Alphabetical].[Month_Alphabetical].[10]\n"
            + "[Time_Alphabetical].[Month_Alphabetical].[9]\n"
            + "[Time_Alphabetical].[Month_Alphabetical].[9]");
    }

    public void testIntersectAll() {
        // Note: duplicates retained from left, not from right; and order is
        // preserved.
        assertAxisReturns(
            "Intersect({[Time].[1997].[Q2], [Time].[1997], [Time].[1997].[Q1], [Time].[1997].[Q2]}, "
            + "{[Time].[1998], [Time].[1997], [Time].[1997].[Q2], [Time].[1997]}, "
            + "ALL)",
            "[Time].[Time].[1997].[Q2]\n"
            + "[Time].[Time].[1997]\n"
            + "[Time].[Time].[1997].[Q2]");
    }

    public void testIntersect() {
        // Duplicates not preserved. Output in order that first duplicate
        // occurred.
        assertAxisReturns(
            "Intersect(\n"
            + "  {[Time].[1997].[Q2], [Time].[1997], [Time].[1997].[Q1], [Time].[1997].[Q2]}, "
            + "{[Time].[1998], [Time].[1997], [Time].[1997].[Q2], [Time].[1997]})",
            "[Time].[Time].[1997].[Q2]\n"
            + "[Time].[Time].[1997]");
    }

    public void testIntersectTuples() {
        assertAxisReturns(
            "Intersect(\n"
            + "  {([Time].[1997].[Q2], [Gender].[M]),\n"
            + "   ([Time].[1997], [Gender].[F]),\n"
            + "   ([Time].[1997].[Q1], [Gender].[M]),\n"
            + "   ([Time].[1997].[Q2], [Gender].[M])},\n"
            + "  {([Time].[1998], [Gender].[F]),\n"
            + "   ([Time].[1997], [Gender].[F]),\n"
            + "   ([Time].[1997].[Q2], [Gender].[M]),\n"
            + "   ([Time].[1997], [Gender])})",
            "{[Time].[Time].[1997].[Q2], [Customer].[Gender].[M]}\n"
            + "{[Time].[Time].[1997], [Customer].[Gender].[F]}");
    }

    public void testIntersectRightEmpty() {
        assertAxisReturns(
            "Intersect({[Time].[1997]}, {})",
            "");
    }

    public void testIntersectLeftEmpty() {
        assertAxisReturns(
            "Intersect({}, {[Store].[USA].[CA]})",
            "");
    }

    public void testOrderDepends() {
        // Order(<Set>, <Value Expression>) depends upon everything
        // <Value Expression> depends upon, except the dimensions of <Set>.

        // Depends upon everything EXCEPT [Product], [Measures],
        // [Marital Status], [Gender].
        Set<String> s11 = TestContext.allHiersExcept(
            "[Product].[Products]",
            "[Measures]",
            "[Customer].[Marital Status]",
            "[Customer].[Gender]");
        getTestContext().assertSetExprDependsOn(
            "Order("
            + " Crossjoin([Gender].MEMBERS, [Product].MEMBERS),"
            + " ([Measures].[Unit Sales], [Marital Status].[S]),"
            + " ASC)",
            s11);

        // Depends upon everything EXCEPT [Product], [Measures],
        // [Marital Status]. Does depend upon [Gender].
        Set<String> s12 = TestContext.allHiersExcept(
            "[Product].[Products]",
            "[Measures]",
            "[Customer].[Marital Status]");
        getTestContext().assertSetExprDependsOn(
            "Order("
            + " Crossjoin({[Gender].CurrentMember}, [Product].MEMBERS),"
            + " ([Measures].[Unit Sales], [Marital Status].[S]),"
            + " ASC)",
            s12);

        // Depends upon everything except [Measures].
        Set<String> s13 = TestContext.allHiersExcept("[Measures]");
        getTestContext().assertSetExprDependsOn(
            "Order("
            + "  Crossjoin("
            + "    [Gender].CurrentMember.Children, "
            + "    [Marital Status].CurrentMember.Children), "
            + "  [Measures].[Unit Sales], "
            + "  BDESC)",
            s13);

        Set<String> s1 = TestContext.allHiersExcept(
            "[Measures]", "[Store].[Stores]", "[Product].[Products]",
            "[Time].[Time]");
        getTestContext().assertSetExprDependsOn(
            "  Order(\n"
            + "    CrossJoin(\n"
            + "      {[Product].[Products].[Food].[Eggs],\n"
            + "       [Product].[Products].[Food].[Seafood],\n"
            + "       [Product].[Products].[Drink].[Alcoholic Beverages]},\n"
            + "      {[Store].[USA].[WA].[Seattle],\n"
            + "       [Store].[USA].[CA],\n"
            + "       [Store].[USA].[OR]}),\n"
            + "    ([Time].[1997].[Q1], [Measures].[Unit Sales]),\n"
            + "    ASC)",
            s1);
    }

    public void testOrderCalc() {
        if (Util.Retrowoven) {
            // If retrowoven, we don't use Iterable, so plans are different.
            return;
        }
        // [Measures].[Unit Sales] is a constant member, so it is evaluated in
        // a ContextCalc.
        assertAxisCompilesTo(
            "order([Product].children, [Measures].[Unit Sales])",
            "ContextCalc(name=ContextCalc, class=class mondrian.olap.fun.OrderFunDef$ContextCalc, type=SetType<MemberType<hierarchy=[Product].[Products]>>, resultStyle=MUTABLE_LIST)\n"
            + "    Literal(name=Literal, class=class mondrian.calc.impl.ConstantCalc, type=MemberType<member=[Measures].[Unit Sales]>, resultStyle=VALUE_NOT_NULL, value=[Measures].[Unit Sales])\n"
            + "    CalcImpl(name=CalcImpl, class=class mondrian.olap.fun.OrderFunDef$CalcImpl, type=SetType<MemberType<hierarchy=[Product].[Products]>>, resultStyle=MUTABLE_LIST, direction=ASC)\n"
            + "        Children(name=Children, class=class mondrian.olap.fun.BuiltinFunTable$22$1, type=SetType<MemberType<hierarchy=[Product].[Products]>>, resultStyle=LIST)\n"
            + "            CurrentMemberFixed(hierarchy=[Product].[Products], name=CurrentMemberFixed, class=class mondrian.olap.fun.HierarchyCurrentMemberFunDef$FixedCalcImpl, type=MemberType<hierarchy=[Product].[Products]>, resultStyle=VALUE)\n"
            + "        ValueCalc(name=ValueCalc, class=class mondrian.calc.impl.ValueCalc, type=SCALAR, resultStyle=VALUE)\n");

        // [Time].[1997] is constant, and is evaluated in a ContextCalc.
        // [Product].Parent is variable, and is evaluated inside the loop.
        assertAxisCompilesTo(
            "order([Product].children,"
            + " ([Time].[1997], [Product].CurrentMember.Parent))",
            "ContextCalc(name=ContextCalc, class=class mondrian.olap.fun.OrderFunDef$ContextCalc, type=SetType<MemberType<hierarchy=[Product].[Products]>>, resultStyle=MUTABLE_LIST)\n"
            + "    Literal(name=Literal, class=class mondrian.calc.impl.ConstantCalc, type=MemberType<member=[Time].[Time].[1997]>, resultStyle=VALUE_NOT_NULL, value=[Time].[Time].[1997])\n"
            + "    CalcImpl(name=CalcImpl, class=class mondrian.olap.fun.OrderFunDef$CalcImpl, type=SetType<MemberType<hierarchy=[Product].[Products]>>, resultStyle=MUTABLE_LIST, direction=ASC)\n"
            + "        Children(name=Children, class=class mondrian.olap.fun.BuiltinFunTable$22$1, type=SetType<MemberType<hierarchy=[Product].[Products]>>, resultStyle=LIST)\n"
            + "            CurrentMemberFixed(hierarchy=[Product].[Products], name=CurrentMemberFixed, class=class mondrian.olap.fun.HierarchyCurrentMemberFunDef$FixedCalcImpl, type=MemberType<hierarchy=[Product].[Products]>, resultStyle=VALUE)\n"
            + "        MemberValueCalc(name=MemberValueCalc, class=class mondrian.calc.impl.MemberValueCalc, type=SCALAR, resultStyle=VALUE)\n"
            + "            Parent(name=Parent, class=class mondrian.olap.fun.BuiltinFunTable$15$1, type=MemberType<hierarchy=[Product].[Products]>, resultStyle=VALUE)\n"
            + "                CurrentMemberFixed(hierarchy=[Product].[Products], name=CurrentMemberFixed, class=class mondrian.olap.fun.HierarchyCurrentMemberFunDef$FixedCalcImpl, type=MemberType<hierarchy=[Product].[Products]>, resultStyle=VALUE)\n");

        // No ContextCalc this time. All members are non-variable.
        assertAxisCompilesTo(
            "order([Product].children, [Product].CurrentMember.Parent)",
            "CalcImpl(name=CalcImpl, class=class mondrian.olap.fun.OrderFunDef$CalcImpl, type=SetType<MemberType<hierarchy=[Product].[Products]>>, resultStyle=MUTABLE_LIST, direction=ASC)\n"
            + "    Children(name=Children, class=class mondrian.olap.fun.BuiltinFunTable$22$1, type=SetType<MemberType<hierarchy=[Product].[Products]>>, resultStyle=LIST)\n"
            + "        CurrentMemberFixed(hierarchy=[Product].[Products], name=CurrentMemberFixed, class=class mondrian.olap.fun.HierarchyCurrentMemberFunDef$FixedCalcImpl, type=MemberType<hierarchy=[Product].[Products]>, resultStyle=VALUE)\n"
            + "    MemberValueCalc(name=MemberValueCalc, class=class mondrian.calc.impl.MemberValueCalc, type=SCALAR, resultStyle=VALUE)\n"
            + "        Parent(name=Parent, class=class mondrian.olap.fun.BuiltinFunTable$15$1, type=MemberType<hierarchy=[Product].[Products]>, resultStyle=VALUE)\n"
            + "            CurrentMemberFixed(hierarchy=[Product].[Products], name=CurrentMemberFixed, class=class mondrian.olap.fun.HierarchyCurrentMemberFunDef$FixedCalcImpl, type=MemberType<hierarchy=[Product].[Products]>, resultStyle=VALUE)\n");

        // List expression is dependent on one of the constant calcs. It cannot
        // be pulled up, so [Gender].[M] is not in the ContextCalc.
        // Note that there is no CopyListCalc - because Filter creates its own
        // mutable copy.
        // Under JDK 1.4, needs an extra converter from list to iterator,
        // because JDK 1.4 doesn't support the ITERABLE result style.
        assertAxisCompilesTo(
            "order(filter([Product].children, [Measures].[Unit Sales] > 1000), "
            + "([Gender].[M], [Measures].[Store Sales]))",
            Util.Retrowoven
                ? ""
                  + "ContextCalc(name=ContextCalc, class=class mondrian.olap.fun.OrderFunDef$ContextCalc, type=SetType<MemberType<hierarchy=[Product]>>, resultStyle=MUTABLE_LIST)\n"
                  + "    Literal(name=Literal, class=class mondrian.calc.impl.ConstantCalc, type=MemberType<member=[Measures].[Store Sales]>, resultStyle=VALUE_NOT_NULL, value=[Measures].[Store Sales])\n"
                  + "    MemberCalcImpl(name=MemberCalcImpl, class=class mondrian.olap.fun.OrderFunDef$MemberCalcImpl, type=SetType<MemberType<hierarchy=[Product]>>, resultStyle=MUTABLE_LIST, direction=ASC)\n"
                  + "        MemberListIterCalc(name=MemberListIterCalc, class=class mondrian.calc.impl.AbstractExpCompiler$MemberListIterCalc, type=SetType<MemberType<hierarchy=[Product]>>, resultStyle=ITERABLE)\n"
                  + "            ImmutableMemberListCalc(name=ImmutableMemberListCalc, class=class mondrian.olap.fun.FilterFunDef$ImmutableMemberListCalc, type=SetType<MemberType<hierarchy=[Product]>>, resultStyle=MUTABLE_LIST)\n"
                  + "                Children(name=Children, class=class mondrian.olap.fun.BuiltinFunTable$22$1, type=SetType<MemberType<hierarchy=[Product]>>, resultStyle=LIST)\n"
                  + "                    CurrentMemberFixed(hierarchy=[Product], name=CurrentMemberFixed, class=class mondrian.olap.fun.HierarchyCurrentMemberFunDef$FixedCalcImpl, type=MemberType<hierarchy=[Product]>, resultStyle=VALUE)\n"
                  + "                >(name=>, class=class mondrian.olap.fun.BuiltinFunTable$63$1, type=BOOLEAN, resultStyle=VALUE)\n"
                  + "                    MemberValueCalc(name=MemberValueCalc, class=class mondrian.calc.impl.MemberValueCalc, type=SCALAR, resultStyle=VALUE)\n"
                  + "                        Literal(name=Literal, class=class mondrian.calc.impl.ConstantCalc, type=MemberType<member=[Measures].[Unit Sales]>, resultStyle=VALUE_NOT_NULL, value=[Measures].[Unit Sales])\n"
                  + "                    Literal(name=Literal, class=class mondrian.calc.impl.ConstantCalc, type=NUMERIC, resultStyle=VALUE_NOT_NULL, value=1000.0)\n"
                  + "        MemberValueCalc(name=MemberValueCalc, class=class mondrian.calc.impl.MemberValueCalc, type=SCALAR, resultStyle=VALUE)\n"
                  + "            Literal(name=Literal, class=class mondrian.calc.impl.ConstantCalc, type=MemberType<member=[Gender].[M]>, resultStyle=VALUE_NOT_NULL, value=[Gender].[M])\n"
                : ""
                  + "ContextCalc(name=ContextCalc, class=class mondrian.olap.fun.OrderFunDef$ContextCalc, type=SetType<MemberType<hierarchy=[Product].[Products]>>, resultStyle=MUTABLE_LIST)\n"
                  + "    Literal(name=Literal, class=class mondrian.calc.impl.ConstantCalc, type=MemberType<member=[Measures].[Store Sales]>, resultStyle=VALUE_NOT_NULL, value=[Measures].[Store Sales])\n"
                  + "    CalcImpl(name=CalcImpl, class=class mondrian.olap.fun.OrderFunDef$CalcImpl, type=SetType<MemberType<hierarchy=[Product].[Products]>>, resultStyle=MUTABLE_LIST, direction=ASC)\n"
                  + "        ImmutableIterCalc(name=ImmutableIterCalc, class=class mondrian.olap.fun.FilterFunDef$ImmutableIterCalc, type=SetType<MemberType<hierarchy=[Product].[Products]>>, resultStyle=ITERABLE)\n"
                  + "            Children(name=Children, class=class mondrian.olap.fun.BuiltinFunTable$22$1, type=SetType<MemberType<hierarchy=[Product].[Products]>>, resultStyle=LIST)\n"
                  + "                CurrentMemberFixed(hierarchy=[Product].[Products], name=CurrentMemberFixed, class=class mondrian.olap.fun.HierarchyCurrentMemberFunDef$FixedCalcImpl, type=MemberType<hierarchy=[Product].[Products]>, resultStyle=VALUE)\n"
                  + "            >(name=>, class=class mondrian.olap.fun.BuiltinFunTable$63$1, type=BOOLEAN, resultStyle=VALUE)\n"
                  + "                MemberValueCalc(name=MemberValueCalc, class=class mondrian.calc.impl.MemberValueCalc, type=SCALAR, resultStyle=VALUE)\n"
                  + "                    Literal(name=Literal, class=class mondrian.calc.impl.ConstantCalc, type=MemberType<member=[Measures].[Unit Sales]>, resultStyle=VALUE_NOT_NULL, value=[Measures].[Unit Sales])\n"
                  + "                Literal(name=Literal, class=class mondrian.calc.impl.ConstantCalc, type=NUMERIC, resultStyle=VALUE_NOT_NULL, value=1000.0)\n"
                  + "        MemberValueCalc(name=MemberValueCalc, class=class mondrian.calc.impl.MemberValueCalc, type=SCALAR, resultStyle=VALUE)\n"
                  + "            Literal(name=Literal, class=class mondrian.calc.impl.ConstantCalc, type=MemberType<member=[Customer].[Gender].[M]>, resultStyle=VALUE_NOT_NULL, value=[Customer].[Gender].[M])\n");
    }

    /**
     * Verifies that the order function works with a defined member.
     * See this forum post for additional information:
     * http://forums.pentaho.com/showthread.php?p=179473#post179473
     */
    public void testOrderWithMember() {
        assertQueryReturns(
            "with member [Measures].[Product Name Length] as "
            + "'LEN([Product].CurrentMember.Name)'\n"
            + "select {[Measures].[Product Name Length]} ON COLUMNS,\n"
            + "Order([Product].[Products].Children, "
            + "[Measures].[Product Name Length], BASC) ON ROWS\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Product Name Length]}\n"
            + "Axis #2:\n"
            + "{[Product].[Products].[Food]}\n"
            + "{[Product].[Products].[Drink]}\n"
            + "{[Product].[Products].[Non-Consumable]}\n"
            + "Row #0: 4\n"
            + "Row #1: 5\n"
            + "Row #2: 14\n");
    }

    /**
     * Test case for bug
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-332">MONDRIAN-332,
     * "Potential MDX Order Non Empty Problem"</a>
     */
    public void testOrderNonEmpty() {
        assertQueryReturns(
            "select NON EMPTY [Gender].Members ON COLUMNS,\n"
            + "NON EMPTY Order([Product].[Products].[Drink].Children,\n"
            + "[Gender].[All Gender].[F], ASC) ON ROWS\n"
            + "from [Sales]\n"
            + "where ([Customers].[All Customers].[USA].[CA].[San Francisco],\n"
            + " [Time].[1997])",

            "Axis #0:\n"
            + "{[Customer].[Customers].[USA].[CA].[San Francisco], [Time].[Time].[1997]}\n"
            + "Axis #1:\n"
            + "{[Customer].[Gender].[All Gender]}\n"
            + "{[Customer].[Gender].[F]}\n"
            + "{[Customer].[Gender].[M]}\n"
            + "Axis #2:\n"
            + "{[Product].[Products].[Drink].[Beverages]}\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages]}\n"
            + "Row #0: 2\n"
            + "Row #0: \n"
            + "Row #0: 2\n"
            + "Row #1: 4\n"
            + "Row #1: 2\n"
            + "Row #1: 2\n");
    }

    public void testOrder() {
        assertQueryReturns(
            "select {[Measures].[Unit Sales]} on columns,\n"
            + " order({\n"
            + "  [Product].[Products].[Drink],\n"
            + "  [Product].[Products].[Drink].[Beverages],\n"
            + "  [Product].[Products].[Drink].[Dairy],\n"
            + "  [Product].[Products].[Food],\n"
            + "  [Product].[Products].[Food].[Baked Goods],\n"
            + "  [Product].[Products].[Food].[Eggs],\n"
            + "  [Product].[All Products]},\n"
            + " [Measures].[Unit Sales]) on rows\n"
            + "from Sales",

            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Products].[All Products]}\n"
            + "{[Product].[Products].[Drink]}\n"
            + "{[Product].[Products].[Drink].[Dairy]}\n"
            + "{[Product].[Products].[Drink].[Beverages]}\n"
            + "{[Product].[Products].[Food]}\n"
            + "{[Product].[Products].[Food].[Eggs]}\n"
            + "{[Product].[Products].[Food].[Baked Goods]}\n"
            + "Row #0: 266,773\n"
            + "Row #1: 24,597\n"
            + "Row #2: 4,186\n"
            + "Row #3: 13,573\n"
            + "Row #4: 191,940\n"
            + "Row #5: 4,132\n"
            + "Row #6: 7,870\n");
    }

    public void testOrderParentsMissing() {
        // Paradoxically, [Alcoholic Beverages] comes before
        // [Eggs] even though it has a larger value, because
        // its parent [Drink] has a smaller value than [Food].
        assertQueryReturns(
            "select {[Measures].[Unit Sales]} on columns,"
            + " order({\n"
            + "  [Product].[Products].[Drink].[Alcoholic Beverages],\n"
            + "  [Product].[Products].[Food].[Eggs]},\n"
            + " [Measures].[Unit Sales], ASC) on rows\n"
            + "from Sales",

            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages]}\n"
            + "{[Product].[Products].[Food].[Eggs]}\n"
            + "Row #0: 6,838\n"
            + "Row #1: 4,132\n");
    }

    public void testOrderCrossJoinBreak() {
        assertQueryReturns(
            "select {[Measures].[Unit Sales]} on columns,\n"
            + "  Order(\n"
            + "    CrossJoin(\n"
            + "      [Gender].children,\n"
            + "      [Marital Status].children),\n"
            + "    [Measures].[Unit Sales],\n"
            + "    BDESC) on rows\n"
            + "from Sales\n"
            + "where [Time].[1997].[Q1]",

            "Axis #0:\n"
            + "{[Time].[Time].[1997].[Q1]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[S]}\n"
            + "{[Customer].[Gender].[F], [Customer].[Marital Status].[M]}\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[M]}\n"
            + "{[Customer].[Gender].[F], [Customer].[Marital Status].[S]}\n"
            + "Row #0: 17,070\n"
            + "Row #1: 16,790\n"
            + "Row #2: 16,311\n"
            + "Row #3: 16,120\n");
    }

    public void testOrderCrossJoin() {
        // Note:
        // 1. [Alcoholic Beverages] collates before [Eggs] and
        //    [Seafood] because its parent, [Drink], is less
        //    than [Food]
        // 2. [Seattle] generally sorts after [CA] and [OR]
        //    because invisible parent [WA] is greater.
        assertQueryReturns(
            "select CrossJoin(\n"
            + "    {[Time].[1997],\n"
            + "     [Time].[1997].[Q1]},\n"
            + "    {[Measures].[Unit Sales]}) on columns,\n"
            + "  Order(\n"
            + "    CrossJoin(\n"
            + "      {[Product].[Products].[Food].[Eggs],\n"
            + "       [Product].[Products].[Food].[Seafood],\n"
            + "       [Product].[Products].[Drink].[Alcoholic Beverages]},\n"
            + "      {[Store].[USA].[WA].[Seattle],\n"
            + "       [Store].[USA].[CA],\n"
            + "       [Store].[USA].[OR]}),\n"
            + "    ([Time].[1997].[Q1], [Measures].[Unit Sales]),\n"
            + "    ASC) on rows\n"
            + "from Sales",

            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time].[Time].[1997], [Measures].[Unit Sales]}\n"
            + "{[Time].[Time].[1997].[Q1], [Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages], [Store].[Stores].[USA].[OR]}\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages], [Store].[Stores].[USA].[CA]}\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages], [Store].[Stores].[USA].[WA].[Seattle]}\n"
            + "{[Product].[Products].[Food].[Seafood], [Store].[Stores].[USA].[CA]}\n"
            + "{[Product].[Products].[Food].[Seafood], [Store].[Stores].[USA].[OR]}\n"
            + "{[Product].[Products].[Food].[Seafood], [Store].[Stores].[USA].[WA].[Seattle]}\n"
            + "{[Product].[Products].[Food].[Eggs], [Store].[Stores].[USA].[CA]}\n"
            + "{[Product].[Products].[Food].[Eggs], [Store].[Stores].[USA].[OR]}\n"
            + "{[Product].[Products].[Food].[Eggs], [Store].[Stores].[USA].[WA].[Seattle]}\n"
            + "Row #0: 1,680\n"
            + "Row #0: 393\n"
            + "Row #1: 1,936\n"
            + "Row #1: 431\n"
            + "Row #2: 635\n"
            + "Row #2: 142\n"
            + "Row #3: 441\n"
            + "Row #3: 91\n"
            + "Row #4: 451\n"
            + "Row #4: 107\n"
            + "Row #5: 217\n"
            + "Row #5: 44\n"
            + "Row #6: 1,116\n"
            + "Row #6: 240\n"
            + "Row #7: 1,119\n"
            + "Row #7: 251\n"
            + "Row #8: 373\n"
            + "Row #8: 57\n");
    }

    public void testOrderHierarchicalDesc() {
        assertAxisReturns(
            "Order(\n"
            + "    {[Product].[All Products], "
            + "     [Product].[Food],\n"
            + "     [Product].[Drink],\n"
            + "     [Product].[Non-Consumable],\n"
            + "     [Product].[Food].[Eggs],\n"
            + "     [Product].[Drink].[Dairy]},\n"
            + "  [Measures].[Unit Sales],\n"
            + "  DESC)",

            "[Product].[Products].[All Products]\n"
            + "[Product].[Products].[Food]\n"
            + "[Product].[Products].[Food].[Eggs]\n"
            + "[Product].[Products].[Non-Consumable]\n"
            + "[Product].[Products].[Drink]\n"
            + "[Product].[Products].[Drink].[Dairy]");
    }

    public void testOrderCrossJoinDesc() {
        assertAxisReturns(
            "Order(\n"
            + "  CrossJoin(\n"
            + "    {[Gender].[M], [Gender].[F]},\n"
            + "    {[Product].[All Products], "
            + "     [Product].[Food],\n"
            + "     [Product].[Drink],\n"
            + "     [Product].[Non-Consumable],\n"
            + "     [Product].[Food].[Eggs],\n"
            + "     [Product].[Drink].[Dairy]}),\n"
            + "  [Measures].[Unit Sales],\n"
            + "  DESC)",

            "{[Customer].[Gender].[M], [Product].[Products].[All Products]}\n"
            + "{[Customer].[Gender].[M], [Product].[Products].[Food]}\n"
            + "{[Customer].[Gender].[M], [Product].[Products].[Food].[Eggs]}\n"
            + "{[Customer].[Gender].[M], [Product].[Products].[Non-Consumable]}\n"
            + "{[Customer].[Gender].[M], [Product].[Products].[Drink]}\n"
            + "{[Customer].[Gender].[M], [Product].[Products].[Drink].[Dairy]}\n"
            + "{[Customer].[Gender].[F], [Product].[Products].[All Products]}\n"
            + "{[Customer].[Gender].[F], [Product].[Products].[Food]}\n"
            + "{[Customer].[Gender].[F], [Product].[Products].[Food].[Eggs]}\n"
            + "{[Customer].[Gender].[F], [Product].[Products].[Non-Consumable]}\n"
            + "{[Customer].[Gender].[F], [Product].[Products].[Drink]}\n"
            + "{[Customer].[Gender].[F], [Product].[Products].[Drink].[Dairy]}");
    }

    public void testOrderBug656802() {
        // Note:
        // 1. [Alcoholic Beverages] collates before [Eggs] and
        //    [Seafood] because its parent, [Drink], is less
        //    than [Food]
        // 2. [Seattle] generally sorts after [CA] and [OR]
        //    because invisible parent [WA] is greater.
        assertQueryReturns(
            "select {[Measures].[Unit Sales], [Measures].[Store Cost], [Measures].[Store Sales]} ON columns, \n"
            + "Order(\n"
            + "  ToggleDrillState(\n"
            + "    {([Promotion].[Media Type].[All Media], [Product].[All Products])},\n"
            + "    {[Product].[All Products]}), \n"
            + "  [Measures].[Unit Sales], DESC) ON rows \n"
            + "from [Sales] where ([Time].[1997])",

            "Axis #0:\n"
            + "{[Time].[Time].[1997]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Store Cost]}\n"
            + "{[Measures].[Store Sales]}\n"
            + "Axis #2:\n"
            + "{[Promotion].[Media Type].[All Media], [Product].[Products].[All Products]}\n"
            + "{[Promotion].[Media Type].[All Media], [Product].[Products].[Food]}\n"
            + "{[Promotion].[Media Type].[All Media], [Product].[Products].[Non-Consumable]}\n"
            + "{[Promotion].[Media Type].[All Media], [Product].[Products].[Drink]}\n"
            + "Row #0: 266,773\n"
            + "Row #0: 225,627.23\n"
            + "Row #0: 565,238.13\n"
            + "Row #1: 191,940\n"
            + "Row #1: 163,270.72\n"
            + "Row #1: 409,035.59\n"
            + "Row #2: 50,236\n"
            + "Row #2: 42,879.28\n"
            + "Row #2: 107,366.33\n"
            + "Row #3: 24,597\n"
            + "Row #3: 19,477.23\n"
            + "Row #3: 48,836.21\n");
    }

    public void testOrderBug712702_Simplified() {
        assertQueryReturns(
            "SELECT Order({[Time].[Year].members}, [Measures].[Unit Sales]) on columns\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time].[Time].[1998]}\n"
            + "{[Time].[Time].[1997]}\n"
            + "Row #0: \n"
            + "Row #0: 266,773\n");
    }

    public void testOrderBug712702_Original() {
        assertQueryReturns(
            "with member [Measures].[Average Unit Sales] as 'Avg(Descendants([Time].[Time].CurrentMember, [Time].[Month]), \n"
            + "[Measures].[Unit Sales])' \n"
            + "member [Measures].[Max Unit Sales] as 'Max(Descendants([Time].[Time].CurrentMember, [Time].[Month]), [Measures].[Unit Sales])' \n"
            + "select {[Measures].[Average Unit Sales], [Measures].[Max Unit Sales], [Measures].[Unit Sales]} ON columns, \n"
            + "  NON EMPTY Order(\n"
            + "    Crossjoin(\n"
            + "      {[Store].[USA].[OR].[Portland],\n"
            + "       [Store].[USA].[OR].[Salem],\n"
            + "       [Store].[USA].[OR].[Salem].[Store 13],\n"
            + "       [Store].[USA].[CA].[San Francisco],\n"
            + "       [Store].[USA].[CA].[San Diego],\n"
            + "       [Store].[USA].[CA].[Beverly Hills],\n"
            + "       [Store].[USA].[CA].[Los Angeles],\n"
            + "       [Store].[USA].[WA].[Walla Walla],\n"
            + "       [Store].[USA].[WA].[Bellingham],\n"
            + "       [Store].[USA].[WA].[Yakima],\n"
            + "       [Store].[USA].[WA].[Spokane],\n"
            + "       [Store].[USA].[WA].[Seattle], \n"
            + "       [Store].[USA].[WA].[Bremerton],\n"
            + "       [Store].[USA].[WA].[Tacoma]},\n"
            + "     [Time].[Year].Members), \n"
            + "  [Measures].[Average Unit Sales], ASC) ON rows\n"
            + "from [Sales] ",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Average Unit Sales]}\n"
            + "{[Measures].[Max Unit Sales]}\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Store].[Stores].[USA].[OR].[Portland], [Time].[Time].[1997]}\n"
            + "{[Store].[Stores].[USA].[OR].[Salem], [Time].[Time].[1997]}\n"
            + "{[Store].[Stores].[USA].[OR].[Salem].[Store 13], [Time].[Time].[1997]}\n"
            + "{[Store].[Stores].[USA].[CA].[San Francisco], [Time].[Time].[1997]}\n"
            + "{[Store].[Stores].[USA].[CA].[Beverly Hills], [Time].[Time].[1997]}\n"
            + "{[Store].[Stores].[USA].[CA].[San Diego], [Time].[Time].[1997]}\n"
            + "{[Store].[Stores].[USA].[CA].[Los Angeles], [Time].[Time].[1997]}\n"
            + "{[Store].[Stores].[USA].[WA].[Walla Walla], [Time].[Time].[1997]}\n"
            + "{[Store].[Stores].[USA].[WA].[Bellingham], [Time].[Time].[1997]}\n"
            + "{[Store].[Stores].[USA].[WA].[Yakima], [Time].[Time].[1997]}\n"
            + "{[Store].[Stores].[USA].[WA].[Spokane], [Time].[Time].[1997]}\n"
            + "{[Store].[Stores].[USA].[WA].[Bremerton], [Time].[Time].[1997]}\n"
            + "{[Store].[Stores].[USA].[WA].[Seattle], [Time].[Time].[1997]}\n"
            + "{[Store].[Stores].[USA].[WA].[Tacoma], [Time].[Time].[1997]}\n"
            + "Row #0: 2,173\n"
            + "Row #0: 2,933\n"
            + "Row #0: 26,079\n"
            + "Row #1: 3,465\n"
            + "Row #1: 5,891\n"
            + "Row #1: 41,580\n"
            + "Row #2: 3,465\n"
            + "Row #2: 5,891\n"
            + "Row #2: 41,580\n"
            + "Row #3: 176\n"
            + "Row #3: 222\n"
            + "Row #3: 2,117\n"
            + "Row #4: 1,778\n"
            + "Row #4: 2,545\n"
            + "Row #4: 21,333\n"
            + "Row #5: 2,136\n"
            + "Row #5: 2,686\n"
            + "Row #5: 25,635\n"
            + "Row #6: 2,139\n"
            + "Row #6: 2,669\n"
            + "Row #6: 25,663\n"
            + "Row #7: 184\n"
            + "Row #7: 301\n"
            + "Row #7: 2,203\n"
            + "Row #8: 186\n"
            + "Row #8: 275\n"
            + "Row #8: 2,237\n"
            + "Row #9: 958\n"
            + "Row #9: 1,163\n"
            + "Row #9: 11,491\n"
            + "Row #10: 1,966\n"
            + "Row #10: 2,634\n"
            + "Row #10: 23,591\n"
            + "Row #11: 2,048\n"
            + "Row #11: 2,623\n"
            + "Row #11: 24,576\n"
            + "Row #12: 2,084\n"
            + "Row #12: 2,304\n"
            + "Row #12: 25,011\n"
            + "Row #13: 2,938\n"
            + "Row #13: 3,818\n"
            + "Row #13: 35,257\n");
    }

    public void testOrderEmpty() {
        assertQueryReturns(
            "select \n"
            + "  Order("
            + "    {},"
            + "    [Customers].currentMember, BDESC) \n"
            + "on 0 from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n");
    }

    public void testOrderOne() {
        assertQueryReturns(
            "select \n"
            + "  Order("
            + "    {[Customers].[All Customers].[USA].[CA].[Woodland Hills].[Abel Young]},"
            + "    [Customers].currentMember, BDESC) \n"
            + "on 0 from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Customer].[Customers].[USA].[CA].[Woodland Hills].[Abel Young]}\n"
            + "Row #0: 75\n");
    }

    public void testOrderKeyEmpty() {
        assertQueryReturns(
            "select \n"
            + "  Order("
            + "    {},"
            + "    [Customers].currentMember.OrderKey, BDESC) \n"
            + "on 0 from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n");
    }

    public void testOrderKeyOne() {
        assertQueryReturns(
            "select \n"
            + "  Order("
            + "    {[Customers].[All Customers].[USA].[CA].[Woodland Hills].[Abel Young]},"
            + "    [Customers].currentMember.OrderKey, BDESC) \n"
            + "on 0 from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Customer].[Customers].[USA].[CA].[Woodland Hills].[Abel Young]}\n"
            + "Row #0: 75\n");
    }

    public void testOrderDesc() {
        // based on olap4j's OlapTest.testSortDimension
        assertQueryReturns(
            "SELECT\n"
            + "{[Measures].[Store Sales]} ON COLUMNS,\n"
            + "{Order(\n"
            + "  {{[Product].[Drink], [Product].[Drink].Children}},\n"
            + "  [Product].CurrentMember.Name,\n"
            + "  DESC)} ON ROWS\n"
            + "FROM [Sales]\n"
            + "WHERE {[Time].[1997].[Q3].[7]}",
            "Axis #0:\n"
            + "{[Time].[Time].[1997].[Q3].[7]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Products].[Drink]}\n"
            + "{[Product].[Products].[Drink].[Dairy]}\n"
            + "{[Product].[Products].[Drink].[Beverages]}\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages]}\n"
            + "Row #0: 4,409.58\n"
            + "Row #1: 629.69\n"
            + "Row #2: 2,477.02\n"
            + "Row #3: 1,302.87\n");
    }

    public void testOrderMemberMemberValueExpNew() {
        if (!Bug.BugMondrian1173Fixed) {
            return;
        }
        propSaver.set(propSaver.props.CompareSiblingsByOrderKey, true);
        // Use a fresh connection to make sure bad member ordinals haven't
        // been assigned by previous tests.
        final TestContext context = getTestContext().withFreshConnection();
        try {
            context.assertQueryReturns(
                "select \n"
                + "  Order("
                + "    {[Customers].[All Customers].[USA].[CA].[Woodland Hills].[Abel Young],"
                + "     [Customers].[All Customers].[USA].[CA].[Santa Monica].[Adeline Chun]},"
                + "    [Customers].currentMember.OrderKey, BDESC) \n"
                + "on 0 from [Sales]",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Customer].[Customers].[USA].[CA].[Santa Monica].[Adeline Chun]}\n"
                + "{[Customer].[Customers].[USA].[CA].[Woodland Hills].[Abel Young]}\n"
                + "Row #0: 33\n"
                + "Row #0: 75\n");
        } finally {
            if (context != null) {
                context.close();
            }
        }
    }

    public void testOrderMemberMemberValueExpNew1() {
        // sort by default measure
        propSaver.set(propSaver.props.CompareSiblingsByOrderKey, true);
        // Use a fresh connection to make sure bad member ordinals haven't
        // been assigned by previous tests.
        final TestContext context = getTestContext().withFreshConnection();
        try {
            context.assertQueryReturns(
                "select \n"
                + "  Order("
                + "    {[Customers].[All Customers].[USA].[CA].[Woodland Hills].[Abel Young],"
                + "     [Customers].[All Customers].[USA].[CA].[Santa Monica].[Adeline Chun]},"
                + "    [Customers].currentMember, BDESC) \n"
                + "on 0 from [Sales]",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Customer].[Customers].[USA].[CA].[Woodland Hills].[Abel Young]}\n"
                + "{[Customer].[Customers].[USA].[CA].[Santa Monica].[Adeline Chun]}\n"
                + "Row #0: 75\n"
                + "Row #0: 33\n");
        } finally {
            context.close();
        }
    }

    public void testOrderMemberDefaultFlag1() {
        // flags not specified default to ASC - sort by default measure
        assertQueryReturns(
            "with \n"
            + "  Member [Measures].[Zero] as '0' \n"
            + "select \n"
            + "  Order("
            + "    {[Customers].[All Customers].[USA].[CA].[Woodland Hills].[Abel Young],"
            + "     [Customers].[All Customers].[USA].[CA].[Santa Monica].[Adeline Chun]},"
            + "    [Customers].currentMember.OrderKey) \n"
            + "on 0 from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Customer].[Customers].[USA].[CA].[Santa Monica].[Adeline Chun]}\n"
            + "{[Customer].[Customers].[USA].[CA].[Woodland Hills].[Abel Young]}\n"
            + "Row #0: 33\n"
            + "Row #0: 75\n");
    }

    public void testOrderMemberDefaultFlag2() {
        // flags not specified default to ASC
        assertQueryReturns(
            "with \n"
            + "  Member [Measures].[Zero] as '0' \n"
            + "select \n"
            + "  Order("
            + "    {[Customers].[All Customers].[USA].[CA].[Woodland Hills].[Abel Young],"
            + "     [Customers].[All Customers].[USA].[CA].[Santa Monica].[Adeline Chun]},"
            + "    [Measures].[Store Cost]) \n"
            + "on 0 from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Customer].[Customers].[USA].[CA].[Woodland Hills].[Abel Young]}\n"
            + "{[Customer].[Customers].[USA].[CA].[Santa Monica].[Adeline Chun]}\n"
            + "Row #0: 75\n"
            + "Row #0: 33\n");
    }

    public void testOrderMemberMemberValueExpHierarchy() {
        // Santa Monica and Woodland Hills both don't have orderkey
        // members are sorted by the order of their keys
        assertQueryReturns(
            "select \n"
            + "  Order("
            + "    {[Customers].[All Customers].[USA].[CA].[Woodland Hills].[Abel Young],"
            + "     [Customers].[All Customers].[USA].[CA].[Santa Monica].[Adeline Chun]},"
            + "    [Customers].currentMember.OrderKey, DESC) \n"
            + "on 0 from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Customer].[Customers].[USA].[CA].[Woodland Hills].[Abel Young]}\n"
            + "{[Customer].[Customers].[USA].[CA].[Santa Monica].[Adeline Chun]}\n"
            + "Row #0: 75\n"
            + "Row #0: 33\n");
    }

    public void testOrderMemberMultiKeysMemberValueExp1() {
        // sort by unit sales and then customer id (Adeline = 6442, Abe = 570)
        assertQueryReturns(
            "select \n"
            + "  Order("
            + "    {[Customers].[USA].[WA].[Issaquah].[Abe Tramel],"
            + "     [Customers].[All Customers].[USA].[CA].[Woodland Hills].[Abel Young],"
            + "     [Customers].[All Customers].[USA].[CA].[Santa Monica].[Adeline Chun]},"
            + "    [Measures].[Unit Sales], BDESC, [Customers].currentMember.OrderKey, BDESC) \n"
            + "on 0 from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Customer].[Customers].[USA].[CA].[Woodland Hills].[Abel Young]}\n"
            + "{[Customer].[Customers].[USA].[CA].[Santa Monica].[Adeline Chun]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Issaquah].[Abe Tramel]}\n"
            + "Row #0: 75\n"
            + "Row #0: 33\n"
            + "Row #0: 33\n");
    }

    public void testOrderMemberMultiKeysMemberValueExp2() {
        if (!Bug.BugMondrian1173Fixed) {
            return;
        }
        propSaver.set(propSaver.props.CompareSiblingsByOrderKey, true);
        // Use a fresh connection to make sure bad member ordinals haven't
        // been assigned by previous tests.
        final TestContext context = getTestContext().withFreshConnection();
        try {
            context.assertQueryReturns(
                "select \n"
                + "  Order("
                + "    {[Customers].[USA].[WA].[Issaquah].[Abe Tramel],"
                + "     [Customers].[All Customers].[USA].[CA].[Woodland Hills].[Abel Young],"
                + "     [Customers].[All Customers].[USA].[CA].[Santa Monica].[Adeline Chun]},"
                + "    [Customers].currentMember.Parent.Parent.OrderKey, BASC, [Customers].currentMember.OrderKey, BDESC) \n"
                + "on 0 from [Sales]",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Customer].[Customers].[USA].[CA].[Santa Monica].[Adeline Chun]}\n"
                + "{[Customer].[Customers].[USA].[CA].[Woodland Hills].[Abel Young]}\n"
                + "{[Customer].[Customers].[USA].[WA].[Issaquah].[Abe Tramel]}\n"
                + "Row #0: 33\n"
                + "Row #0: 75\n"
                + "Row #0: 33\n");
        } finally {
            context.close();
        }
    }

    public void testOrderMemberMultiKeysMemberValueExpDepends() {
        // should preserve order of Abe and Adeline (note second key is [Time])
        assertQueryReturns(
            "select \n"
            + "  Order("
            + "    {[Customers].[USA].[WA].[Issaquah].[Abe Tramel],"
            + "     [Customers].[All Customers].[USA].[CA].[Woodland Hills].[Abel Young],"
            + "     [Customers].[All Customers].[USA].[CA].[Santa Monica].[Adeline Chun]},"
            + "    [Measures].[Unit Sales], BDESC, [Time].[Time].currentMember, BDESC) \n"
            + "on 0 from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Customer].[Customers].[USA].[CA].[Woodland Hills].[Abel Young]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Issaquah].[Abe Tramel]}\n"
            + "{[Customer].[Customers].[USA].[CA].[Santa Monica].[Adeline Chun]}\n"
            + "Row #0: 75\n"
            + "Row #0: 33\n"
            + "Row #0: 33\n");
    }

    public void testOrderTupleSingleKeysNew() {
        if (!Bug.BugMondrian1173Fixed) {
            return;
        }
        propSaver.set(propSaver.props.CompareSiblingsByOrderKey, true);
        // Use a fresh connection to make sure bad member ordinals haven't
        // been assigned by previous tests.
        final TestContext context = getTestContext().withFreshConnection();
        try {
            context.assertQueryReturns(
                "with \n"
                + "  set [NECJ] as \n"
                + "    'NonEmptyCrossJoin( \n"
                + "    {[Customers].[USA].[WA].[Issaquah].[Abe Tramel],"
                + "     [Customers].[All Customers].[USA].[CA].[Woodland Hills].[Abel Young],"
                + "     [Customers].[All Customers].[USA].[CA].[Santa Monica].[Adeline Chun]},"
                + "    {[Store].[USA].[WA].[Seattle],\n"
                + "     [Store].[USA].[CA],\n"
                + "     [Store].[USA].[OR]})'\n"
                + "select \n"
                + " Order([NECJ], [Customers].currentMember.OrderKey, BDESC) \n"
                + "on 0 from [Sales]",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Customer].[Customers].[USA].[CA].[Santa Monica].[Adeline Chun], [Store].[Stores].[USA].[CA]}\n"
                + "{[Customer].[Customers].[USA].[CA].[Woodland Hills].[Abel Young], [Store].[Stores].[USA].[CA]}\n"
                + "{[Customer].[Customers].[USA].[WA].[Issaquah].[Abe Tramel], [Store].[Stores].[USA].[WA].[Seattle]}\n"
                + "Row #0: 33\n"
                + "Row #0: 75\n"
                + "Row #0: 33\n");
        } finally {
            context.close();
        }
    }

    public void testOrderTupleSingleKeysNew1() {
        propSaver.set(propSaver.props.CompareSiblingsByOrderKey, true);
        // Use a fresh connection to make sure bad member ordinals haven't
        // been assigned by previous tests.
        final TestContext context = getTestContext().withFreshConnection();
        try {
            context.assertQueryReturns(
                "with \n"
                + "  set [NECJ] as \n"
                + "    'NonEmptyCrossJoin( \n"
                + "    {[Customers].[USA].[WA].[Issaquah].[Abe Tramel],"
                + "     [Customers].[All Customers].[USA].[CA].[Woodland Hills].[Abel Young],"
                + "     [Customers].[All Customers].[USA].[CA].[Santa Monica].[Adeline Chun]},"
                + "    {[Store].[USA].[WA].[Seattle],\n"
                + "     [Store].[USA].[CA],\n"
                + "     [Store].[USA].[OR]})'\n"
                + "select \n"
                + " Order([NECJ], [Stores].CURRENTMEMBER.OrderKey, DESC) \n"
                + "on 0 from [Sales]",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Customer].[Customers].[USA].[WA].[Issaquah].[Abe Tramel], [Store].[Stores].[USA].[WA].[Seattle]}\n"
                + "{[Customer].[Customers].[USA].[CA].[Woodland Hills].[Abel Young], [Store].[Stores].[USA].[CA]}\n"
                + "{[Customer].[Customers].[USA].[CA].[Santa Monica].[Adeline Chun], [Store].[Stores].[USA].[CA]}\n"
                + "Row #0: 33\n"
                + "Row #0: 75\n"
                + "Row #0: 33\n");
        } finally {
            context.close();
        }
    }

    public void testOrderTupleMultiKeys1() {
        assertQueryReturns(
            "with \n"
            + "  set [NECJ] as \n"
            + "    'NonEmptyCrossJoin( \n"
            + "    {[Store].[USA].[CA],\n"
            + "     [Store].[USA].[WA]},\n"
            + "    {[Customers].[USA].[WA].[Issaquah].[Abe Tramel],"
            + "     [Customers].[All Customers].[USA].[CA].[Woodland Hills].[Abel Young],"
            + "     [Customers].[All Customers].[USA].[CA].[Santa Monica].[Adeline Chun]})' \n"
            + "select \n"
            + " Order([NECJ], [Stores].CURRENTMEMBER.OrderKey, BDESC, [Measures].[Unit Sales], BDESC) \n"
            + "on 0 from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[Stores].[USA].[WA], [Customer].[Customers].[USA].[WA].[Issaquah].[Abe Tramel]}\n"
            + "{[Store].[Stores].[USA].[CA], [Customer].[Customers].[USA].[CA].[Woodland Hills].[Abel Young]}\n"
            + "{[Store].[Stores].[USA].[CA], [Customer].[Customers].[USA].[CA].[Santa Monica].[Adeline Chun]}\n"
            + "Row #0: 33\n"
            + "Row #0: 75\n"
            + "Row #0: 33\n");
    }

    public void testOrderTupleMultiKeys2() {
        assertQueryReturns(
            "with \n"
            + "  set [NECJ] as \n"
            + "    'NonEmptyCrossJoin( \n"
            + "    {[Store].[USA].[CA],\n"
            + "     [Store].[USA].[WA]},\n"
            + "    {[Customers].[USA].[WA].[Issaquah].[Abe Tramel],"
            + "     [Customers].[All Customers].[USA].[CA].[Woodland Hills].[Abel Young],"
            + "     [Customers].[All Customers].[USA].[CA].[Santa Monica].[Adeline Chun]})' \n"
            + "select \n"
            + " Order([NECJ], [Measures].[Unit Sales], BDESC, Ancestor([Customers].currentMember, [Customers].[Name]).OrderKey, BDESC) \n"
            + "on 0 from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[Stores].[USA].[CA], [Customer].[Customers].[USA].[CA].[Woodland Hills].[Abel Young]}\n"
            + "{[Store].[Stores].[USA].[CA], [Customer].[Customers].[USA].[CA].[Santa Monica].[Adeline Chun]}\n"
            + "{[Store].[Stores].[USA].[WA], [Customer].[Customers].[USA].[WA].[Issaquah].[Abe Tramel]}\n"
            + "Row #0: 75\n"
            + "Row #0: 33\n"
            + "Row #0: 33\n");
    }

    public void testOrderTupleMultiKeys3() {
        if (!Bug.BaseStarKeyColumnFixed) {
            return;
        }
        // WA unit sales is greater than CA unit sales
        // Santa Monica unit sales (2660) is greater that Woodland hills (2516)
        assertQueryReturns(
            "with \n"
            + "  set [NECJ] as \n"
            + "    'NonEmptyCrossJoin( \n"
            + "    {[Store].[USA].[CA],\n"
            + "     [Store].[USA].[WA]},\n"
            + "    {[Customers].[USA].[WA].[Issaquah].[Abe Tramel],"
            + "     [Customers].[All Customers].[USA].[CA].[Woodland Hills].[Abel Young],"
            + "     [Customers].[All Customers].[USA].[CA].[Santa Monica].[Adeline Chun]})' \n"
            + "select \n"
            + " Order([NECJ], [Measures].[Unit Sales], DESC, Ancestor([Customers].currentMember, [Customers].[Name]), BDESC) \n"
            + "on 0 from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[Stores].[USA].[WA], [Customer].[Customers].[USA].[WA].[Issaquah].[Abe Tramel]}\n"
            + "{[Store].[Stores].[USA].[CA], [Customer].[Customers].[USA].[CA].[Santa Monica].[Adeline Chun]}\n"
            + "{[Store].[Stores].[USA].[CA], [Customer].[Customers].[USA].[CA].[Woodland Hills].[Abel Young]}\n"
            + "Row #0: 33\n"
            + "Row #0: 33\n"
            + "Row #0: 75\n");
    }

    public void testOrderTupleMultiKeyswithVCube() {
        if (!Bug.ModifiedSchema) {
            return;
        }
        // WA unit sales is greater than CA unit sales
        propSaver.set(propSaver.props.CompareSiblingsByOrderKey, true);

        // Use a fresh connection to make sure bad member ordinals haven't
        // been assigned by previous tests.
        // a non-sense cube just to test ordering by order key
        TestContext context = TestContext.instance().legacy().create(
            null,
            null,
            "<VirtualCube name=\"Sales vs HR\">\n"
            + "<VirtualCubeDimension cubeName=\"Sales\" name=\"Customers\"/>\n"
            + "<VirtualCubeDimension cubeName=\"HR\" name=\"Position\"/>\n"
            + "<VirtualCubeMeasure cubeName=\"HR\" name=\"[Measures].[Org Salary]\"/>\n"
            + "</VirtualCube>",
            null, null, null);

        context.assertQueryReturns(
            "with \n"
            + "  set [CJ] as \n"
            + "    'CrossJoin( \n"
            + "    {[Position].[Store Management].children},\n"
            + "    {[Customers].[USA].[WA].[Issaquah].[Abe Tramel],"
            + "     [Customers].[All Customers].[USA].[CA].[Woodland Hills].[Abel Young],"
            + "     [Customers].[All Customers].[USA].[CA].[Santa Monica].[Adeline Chun]})' \n"
            + "select \n"
            + "  [Measures].[Org Salary] on columns, \n"
            + "  Order([CJ], [Position].currentMember.OrderKey, BASC, Ancestor([Customers].currentMember, [Customers].[Name]).OrderKey, BDESC) \n"
            + "on rows \n"
            + "from [Sales vs HR]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Org Salary]}\n"
            + "Axis #2:\n"
            + "{[Employee].[Position].[Store Management].[Store Manager], [Customer].[Customers].[USA].[CA].[Santa Monica].[Adeline Chun]}\n"
            + "{[Employee].[Position].[Store Management].[Store Manager], [Customer].[Customers].[USA].[CA].[Woodland Hills].[Abel Young]}\n"
            + "{[Employee].[Position].[Store Management].[Store Manager], [Customer].[Customers].[USA].[WA].[Issaquah].[Abe Tramel]}\n"
            + "{[Employee].[Position].[Store Management].[Store Assistant Manager], [Customer].[Customers].[USA].[CA].[Santa Monica].[Adeline Chun]}\n"
            + "{[Employee].[Position].[Store Management].[Store Assistant Manager], [Customer].[Customers].[USA].[CA].[Woodland Hills].[Abel Young]}\n"
            + "{[Employee].[Position].[Store Management].[Store Assistant Manager], [Customer].[Customers].[USA].[WA].[Issaquah].[Abe Tramel]}\n"
            + "{[Employee].[Position].[Store Management].[Store Shift Supervisor], [Customer].[Customers].[USA].[CA].[Santa Monica].[Adeline Chun]}\n"
            + "{[Employee].[Position].[Store Management].[Store Shift Supervisor], [Customer].[Customers].[USA].[CA].[Woodland Hills].[Abel Young]}\n"
            + "{[Employee].[Position].[Store Management].[Store Shift Supervisor], [Customer].[Customers].[USA].[WA].[Issaquah].[Abe Tramel]}\n"
            + "Row #0: \n"
            + "Row #1: \n"
            + "Row #2: \n"
            + "Row #3: \n"
            + "Row #4: \n"
            + "Row #5: \n"
            + "Row #6: \n"
            + "Row #7: \n"
            + "Row #8: \n");
    }

    public void testOrderConstant1() {
        // sort by customerId (Abel = 7851, Adeline = 6442, Abe = 570)
        getTestContext()
            .withSubstitution(
                new Util.Function1<String, String>() {
                    public String apply(String param) {
                        return param.replace(
                            "<Attribute name='Name' keyColumn='customer_id' nameColumn='full_name' orderByColumn='full_name' hasHierarchy='false'/>",
                            "<Attribute name='Name' keyColumn='customer_id' nameColumn='full_name' orderByColumn='customer_id' hasHierarchy='false'/>");
                    }
                }
            ).assertQueryReturns(
                "select \n"
                + "  Order("
                + "    {[Customers].[USA].[WA].[Issaquah].[Abe Tramel],"
                + "     [Customers].[All Customers].[USA].[CA].[Woodland Hills].[Abel Young],"
                + "     [Customers].[All Customers].[USA].[CA].[Santa Monica].[Adeline Chun]},"
                + "    [Customers].[USA].OrderKey, BDESC, [Customers].currentMember.OrderKey, BASC) \n"
                + "on 0 from [Sales]",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Customer].[Customers].[USA].[WA].[Issaquah].[Abe Tramel]}\n"
                + "{[Customer].[Customers].[USA].[CA].[Santa Monica].[Adeline Chun]}\n"
                + "{[Customer].[Customers].[USA].[CA].[Woodland Hills].[Abel Young]}\n"
                + "Row #0: 33\n"
                + "Row #0: 33\n"
                + "Row #0: 75\n");
    }

    public void testOrderDifferentDim() {
        assertQueryReturns(
            "select \n"
            + "  Order("
            + "    {[Customers].[USA].[WA].[Issaquah].[Abe Tramel],"
            + "     [Customers].[All Customers].[USA].[CA].[Woodland Hills].[Abel Young],"
            + "     [Customers].[All Customers].[USA].[CA].[Santa Monica].[Adeline Chun]},"
            + "    [Product].currentMember.OrderKey, BDESC, [Gender].currentMember.OrderKey, BDESC) \n"
            + "on 0 from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Customer].[Customers].[USA].[WA].[Issaquah].[Abe Tramel]}\n"
            + "{[Customer].[Customers].[USA].[CA].[Woodland Hills].[Abel Young]}\n"
            + "{[Customer].[Customers].[USA].[CA].[Santa Monica].[Adeline Chun]}\n"
            + "Row #0: 33\n"
            + "Row #0: 75\n"
            + "Row #0: 33\n");
    }

    public void testUnorder() {
        assertAxisReturns(
            "Unorder([Gender].members)",
            "[Customer].[Gender].[All Gender]\n"
            + "[Customer].[Gender].[F]\n"
            + "[Customer].[Gender].[M]");
        assertAxisReturns(
            "Unorder(Order([Gender].members, -[Measures].[Unit Sales]))",
            "[Customer].[Gender].[All Gender]\n"
            + "[Customer].[Gender].[M]\n"
            + "[Customer].[Gender].[F]");
        assertAxisReturns(
            "Unorder(Crossjoin([Gender].members, [Marital Status].Children))",
            "{[Customer].[Gender].[All Gender], [Customer].[Marital Status].[M]}\n"
            + "{[Customer].[Gender].[All Gender], [Customer].[Marital Status].[S]}\n"
            + "{[Customer].[Gender].[F], [Customer].[Marital Status].[M]}\n"
            + "{[Customer].[Gender].[F], [Customer].[Marital Status].[S]}\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[M]}\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[S]}");

        // implicitly convert member to set
        assertAxisReturns(
            "Unorder([Gender].[M])",
            "[Customer].[Gender].[M]");

        assertAxisThrows(
            "Unorder(1 + 3)",
            "No function matches signature 'Unorder(<Numeric Expression>)'");
        assertAxisThrows(
            "Unorder([Gender].[M], 1 + 3)",
            "No function matches signature 'Unorder(<Member>, <Numeric Expression>)'");
        assertQueryReturns(
            "select {[Measures].[Store Sales], [Measures].[Unit Sales]} on 0,\n"
            + "  Unorder([Gender].Members) on 1\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Sales]}\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Gender].[All Gender]}\n"
            + "{[Customer].[Gender].[F]}\n"
            + "{[Customer].[Gender].[M]}\n"
            + "Row #0: 565,238.13\n"
            + "Row #0: 266,773\n"
            + "Row #1: 280,226.21\n"
            + "Row #1: 131,558\n"
            + "Row #2: 285,011.92\n"
            + "Row #2: 135,215\n");
    }

    public void testSiblingsA() {
        assertAxisReturns(
            "{[Time].[1997].Siblings}",
            "[Time].[Time].[1997]\n"
            + "[Time].[Time].[1998]");
    }

    public void testSiblingsB() {
        assertAxisReturns(
            "{[Stores].Siblings}",
            "[Store].[Stores].[All Stores]");
    }

    public void testSiblingsC() {
        assertAxisReturns(
            "{[Store].[USA].[CA].Siblings}",
            "[Store].[Stores].[USA].[CA]\n"
            + "[Store].[Stores].[USA].[OR]\n"
            + "[Store].[Stores].[USA].[WA]");
    }

    public void testSiblingsD() {
        // The null member has no siblings -- not even itself
        assertAxisReturns("{[Gender].Parent.Siblings}", "");

        assertExprReturns(
            "count ([Gender].parent.siblings, includeempty)", "0");
    }

    public void testSubset() {
        assertAxisReturns(
            "Subset([Promotion].[Media Type].Children, 7, 2)",
            "[Promotion].[Media Type].[Product Attachment]\n"
            + "[Promotion].[Media Type].[Radio]");
    }

    public void testSubsetNegativeCount() {
        assertAxisReturns(
            "Subset([Promotion].[Media Type].Children, 3, -1)",
            "");
    }

    public void testSubsetNegativeStart() {
        assertAxisReturns(
            "Subset([Promotion].[Media Type].Children, -2, 4)",
            "");
    }

    public void testSubsetDefault() {
        assertAxisReturns(
            "Subset([Promotion].[Media Type].Children, 11)",
            "[Promotion].[Media Type].[Sunday Paper, Radio]\n"
            + "[Promotion].[Media Type].[Sunday Paper, Radio, TV]\n"
            + "[Promotion].[Media Type].[TV]");
    }

    public void testSubsetOvershoot() {
        assertAxisReturns(
            "Subset([Promotion].[Media Type].Children, 15)",
            "");
    }

    public void testSubsetEmpty() {
        assertAxisReturns(
            "Subset([Gender].[F].Children, 1)",
            "");

        assertAxisReturns(
            "Subset([Gender].[F].Children, 1, 3)",
            "");
    }

    public void testTail() {
        assertAxisReturns(
            "Tail([Stores].Children, 2)",
            "[Store].[Stores].[Mexico]\n"
            + "[Store].[Stores].[USA]");
    }

    public void testTailNegative() {
        assertAxisReturns(
            "Tail([Stores].Children, 2 - 3)",
            "");
    }

    public void testTailDefault() {
        assertAxisReturns(
            "Tail([Stores].Children)",
            "[Store].[Stores].[USA]");
    }

    public void testTailOvershoot() {
        assertAxisReturns(
            "Tail([Stores].Children, 2 + 2)",
            "[Store].[Stores].[Canada]\n"
            + "[Store].[Stores].[Mexico]\n"
            + "[Store].[Stores].[USA]");
    }

    public void testTailEmpty() {
        assertAxisReturns(
            "Tail([Gender].[F].Children, 2)",
            "");

        assertAxisReturns(
            "Tail([Gender].[F].Children)",
            "");
    }

    public void testToggleDrillState() {
        assertAxisReturns(
            "ToggleDrillState({[Customers].[USA],[Customers].[Canada]},"
            + "{[Customers].[USA],[Customers].[USA].[CA]})",
            "[Customer].[Customers].[USA]\n"
            + "[Customer].[Customers].[USA].[CA]\n"
            + "[Customer].[Customers].[USA].[OR]\n"
            + "[Customer].[Customers].[USA].[WA]\n"
            + "[Customer].[Customers].[Canada]");
    }

    public void testToggleDrillState2() {
        assertAxisReturns(
            "ToggleDrillState([Product].[Product Department].members, "
            + "{[Product].[Products].[Food].[Snack Foods]})",
            "[Product].[Products].[Drink].[Alcoholic Beverages]\n"
            + "[Product].[Products].[Drink].[Beverages]\n"
            + "[Product].[Products].[Drink].[Dairy]\n"
            + "[Product].[Products].[Food].[Baked Goods]\n"
            + "[Product].[Products].[Food].[Baking Goods]\n"
            + "[Product].[Products].[Food].[Breakfast Foods]\n"
            + "[Product].[Products].[Food].[Canned Foods]\n"
            + "[Product].[Products].[Food].[Canned Products]\n"
            + "[Product].[Products].[Food].[Dairy]\n"
            + "[Product].[Products].[Food].[Deli]\n"
            + "[Product].[Products].[Food].[Eggs]\n"
            + "[Product].[Products].[Food].[Frozen Foods]\n"
            + "[Product].[Products].[Food].[Meat]\n"
            + "[Product].[Products].[Food].[Produce]\n"
            + "[Product].[Products].[Food].[Seafood]\n"
            + "[Product].[Products].[Food].[Snack Foods]\n"
            + "[Product].[Products].[Food].[Snack Foods].[Snack Foods]\n"
            + "[Product].[Products].[Food].[Snacks]\n"
            + "[Product].[Products].[Food].[Starchy Foods]\n"
            + "[Product].[Products].[Non-Consumable].[Carousel]\n"
            + "[Product].[Products].[Non-Consumable].[Checkout]\n"
            + "[Product].[Products].[Non-Consumable].[Health and Hygiene]\n"
            + "[Product].[Products].[Non-Consumable].[Household]\n"
            + "[Product].[Products].[Non-Consumable].[Periodicals]");
    }

    public void testToggleDrillState3() {
        assertAxisReturns(
            "ToggleDrillState("
            + "{[Time].[1997].[Q1],"
            + " [Time].[1997].[Q2],"
            + " [Time].[1997].[Q2].[4],"
            + " [Time].[1997].[Q2].[6],"
            + " [Time].[1997].[Q3]},"
            + "{[Time].[1997].[Q2]})",
            "[Time].[Time].[1997].[Q1]\n"
            + "[Time].[Time].[1997].[Q2]\n"
            + "[Time].[Time].[1997].[Q3]");
    }

    // bug 634860
    public void testToggleDrillStateTuple() {
        assertAxisReturns(
            "ToggleDrillState(\n"
            + "{([Store].[USA].[CA],"
            + "  [Product].[Products].[Drink].[Alcoholic Beverages]),\n"
            + " ([Store].[USA],"
            + "  [Product].[Products].[Drink])},\n"
            + "{[Store].[All stores].[USA].[CA]})",
            "{[Store].[Stores].[USA].[CA], [Product].[Products].[Drink].[Alcoholic Beverages]}\n"
            + "{[Store].[Stores].[USA].[CA].[Alameda], [Product].[Products].[Drink].[Alcoholic Beverages]}\n"
            + "{[Store].[Stores].[USA].[CA].[Beverly Hills], [Product].[Products].[Drink].[Alcoholic Beverages]}\n"
            + "{[Store].[Stores].[USA].[CA].[Los Angeles], [Product].[Products].[Drink].[Alcoholic Beverages]}\n"
            + "{[Store].[Stores].[USA].[CA].[San Diego], [Product].[Products].[Drink].[Alcoholic Beverages]}\n"
            + "{[Store].[Stores].[USA].[CA].[San Francisco], [Product].[Products].[Drink].[Alcoholic Beverages]}\n"
            + "{[Store].[Stores].[USA], [Product].[Products].[Drink]}");
    }

    public void testToggleDrillStateRecursive() {
        // We expect this to fail.
        assertQueryThrows(
            "Select \n"
            + "    ToggleDrillState(\n"
            + "        {[Store].[USA]}, \n"
            + "        {[Store].[USA]}, recursive) on Axis(0) \n"
            + "from [Sales]\n",
            "'RECURSIVE' is not supported in ToggleDrillState.");
    }

    public void testTopCount() {
        assertAxisReturns(
            "TopCount({[Promotion].[Media Type].[Media Type].members}, 2, [Measures].[Unit Sales])",
            "[Promotion].[Media Type].[No Media]\n"
            + "[Promotion].[Media Type].[Daily Paper, Radio, TV]");
    }

    public void testTopCountTuple() {
        assertAxisReturns(
            "TopCount([Customers].[Name].members,2,(Time.[1997].[Q1],[Measures].[Store Sales]))",
            "[Customer].[Customers].[USA].[WA].[Spokane].[Grace McLaughlin]\n"
            + "[Customer].[Customers].[USA].[WA].[Spokane].[Matt Bellah]");
    }

    public void testTopCountEmpty() {
        assertAxisReturns(
            "TopCount(Filter({[Promotion].[Media Type].members}, 1=0), 2, [Measures].[Unit Sales])",
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
        Set<String> s1 =
            TestContext.allHiersExcept(
                "[Measures]",
                "[Promotion].[Media Type]");
        getTestContext().assertSetExprDependsOn(
            fun
            + "({[Promotion].[Media Type].members}, "
            + "2, [Measures].[Unit Sales])",
            s1);

        if (fun.endsWith("Count")) {
            getTestContext().assertSetExprDependsOn(
                fun + "({[Promotion].[Media Type].members}, 2)",
                Collections.<String>emptySet());
        }
    }

    /**
     * Tests TopCount applied to a large result set.
     *
     * <p>Before optimizing (see FunUtil.partialSort), on a 2-core 32-bit 2.4GHz
     * machine, the 1st query took 14.5 secs, the 2nd query took 5.0 secs.
     * After optimizing, who knows?
     */
    public void testTopCountHuge() {
        final String query =
            "SELECT [Measures].[Store Sales] ON 0,\n"
            + "TopCount([Time].[Month].members * "
            + "[Customers].[Name].members, 3, [Measures].[Store Sales]) ON 1\n"
            + "FROM [Sales]";
        final String desiredResult =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Sales]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[1997].[Q1].[3], [Customer].[Customers].[USA].[WA].[Spokane].[George Todero]}\n"
            + "{[Time].[Time].[1997].[Q3].[7], [Customer].[Customers].[USA].[WA].[Spokane].[James Horvat]}\n"
            + "{[Time].[Time].[1997].[Q4].[11], [Customer].[Customers].[USA].[WA].[Olympia].[Charles Stanley]}\n"
            + "Row #0: 234.83\n"
            + "Row #1: 199.46\n"
            + "Row #2: 191.90\n";
        long now = System.currentTimeMillis();
        assertQueryReturns(query, desiredResult);
        LOGGER.info("first query took " + (System.currentTimeMillis() - now));
        now = System.currentTimeMillis();
        assertQueryReturns(query, desiredResult);
        LOGGER.info("second query took " + (System.currentTimeMillis() - now));
    }

    public void testTopPercent() {
        assertAxisReturns(
            "TopPercent({[Promotion].[Media Type].[Media Type].members}, 70, [Measures].[Unit Sales])",
            "[Promotion].[Media Type].[No Media]");
    }

    // todo: test precision

    public void testTopSum() {
        assertAxisReturns(
            "TopSum({[Promotion].[Media Type].[Media Type].members}, 200000, [Measures].[Unit Sales])",
            "[Promotion].[Media Type].[No Media]\n"
            + "[Promotion].[Media Type].[Daily Paper, Radio, TV]");
    }

    public void testTopSumEmpty() {
        assertAxisReturns(
            "TopSum(Filter({[Promotion].[Media Type].[Media Type].members}, 1=0), "
            + "200000, [Measures].[Unit Sales])",
            "");
    }

    public void testUnionAll() {
        assertAxisReturns(
            "Union({[Gender].[M]}, {[Gender].[F]}, ALL)",
            "[Customer].[Gender].[M]\n"
            + "[Customer].[Gender].[F]"); // order is preserved
    }

    public void testUnionAllTuple() {
        // With the bug, the last 8 rows are repeated.
        assertQueryReturns(
            "with \n"
            + "set [Set1] as 'Crossjoin({[Time].[1997].[Q1]:[Time].[1997].[Q4]},{[Store].[USA].[CA]:[Store].[USA].[OR]})'\n"
            + "set [Set2] as 'Crossjoin({[Time].[1997].[Q2]:[Time].[1997].[Q3]},{[Store].[Mexico].[DF]:[Store].[Mexico].[Veracruz]})'\n"
            + "select \n"
            + "{[Measures].[Unit Sales]} ON COLUMNS,\n"
            + "Union([Set1], [Set2], ALL) ON ROWS\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[1997].[Q1], [Store].[Stores].[USA].[CA]}\n"
            + "{[Time].[Time].[1997].[Q1], [Store].[Stores].[USA].[OR]}\n"
            + "{[Time].[Time].[1997].[Q2], [Store].[Stores].[USA].[CA]}\n"
            + "{[Time].[Time].[1997].[Q2], [Store].[Stores].[USA].[OR]}\n"
            + "{[Time].[Time].[1997].[Q3], [Store].[Stores].[USA].[CA]}\n"
            + "{[Time].[Time].[1997].[Q3], [Store].[Stores].[USA].[OR]}\n"
            + "{[Time].[Time].[1997].[Q4], [Store].[Stores].[USA].[CA]}\n"
            + "{[Time].[Time].[1997].[Q4], [Store].[Stores].[USA].[OR]}\n"
            + "{[Time].[Time].[1997].[Q2], [Store].[Stores].[Mexico].[DF]}\n"
            + "{[Time].[Time].[1997].[Q2], [Store].[Stores].[Mexico].[Guerrero]}\n"
            + "{[Time].[Time].[1997].[Q2], [Store].[Stores].[Mexico].[Jalisco]}\n"
            + "{[Time].[Time].[1997].[Q2], [Store].[Stores].[Mexico].[Veracruz]}\n"
            + "{[Time].[Time].[1997].[Q3], [Store].[Stores].[Mexico].[DF]}\n"
            + "{[Time].[Time].[1997].[Q3], [Store].[Stores].[Mexico].[Guerrero]}\n"
            + "{[Time].[Time].[1997].[Q3], [Store].[Stores].[Mexico].[Jalisco]}\n"
            + "{[Time].[Time].[1997].[Q3], [Store].[Stores].[Mexico].[Veracruz]}\n"
            + "Row #0: 16,890\n"
            + "Row #1: 19,287\n"
            + "Row #2: 18,052\n"
            + "Row #3: 15,079\n"
            + "Row #4: 18,370\n"
            + "Row #5: 16,940\n"
            + "Row #6: 21,436\n"
            + "Row #7: 16,353\n"
            + "Row #8: \n"
            + "Row #9: \n"
            + "Row #10: \n"
            + "Row #11: \n"
            + "Row #12: \n"
            + "Row #13: \n"
            + "Row #14: \n"
            + "Row #15: \n");
    }

    public void testUnion() {
        assertAxisReturns(
            "Union({[Store].[USA], [Store].[USA], [Store].[USA].[OR]}, "
            + "{[Store].[USA].[CA], [Store].[USA]})",
            "[Store].[Stores].[USA]\n"
            + "[Store].[Stores].[USA].[OR]\n"
            + "[Store].[Stores].[USA].[CA]");
    }

    public void testUnionEmptyBoth() {
        assertAxisReturns(
            "Union({}, {})",
            "");
    }

    public void testUnionEmptyRight() {
        assertAxisReturns(
            "Union({[Gender].[M]}, {})",
            "[Customer].[Gender].[M]");
    }

    public void testUnionTuple() {
        assertAxisReturns(
            "Union({"
            + " ([Gender].[M], [Marital Status].[S]),"
            + " ([Gender].[F], [Marital Status].[S])"
            + "}, {"
            + " ([Gender].[M], [Marital Status].[M]),"
            + " ([Gender].[M], [Marital Status].[S])"
            + "})",

            "{[Customer].[Gender].[M], [Customer].[Marital Status].[S]}\n"
            + "{[Customer].[Gender].[F], [Customer].[Marital Status].[S]}\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[M]}");
    }

    public void testUnionTupleDistinct() {
        assertAxisReturns(
            "Union({"
            + " ([Gender].[M], [Marital Status].[S]),"
            + " ([Gender].[F], [Marital Status].[S])"
            + "}, {"
            + " ([Gender].[M], [Marital Status].[M]),"
            + " ([Gender].[M], [Marital Status].[S])"
            + "}, Distinct)",

            "{[Customer].[Gender].[M], [Customer].[Marital Status].[S]}\n"
            + "{[Customer].[Gender].[F], [Customer].[Marital Status].[S]}\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[M]}");
    }

    public void testUnionQuery() {
        Result result = executeQuery(
            "select {[Measures].[Unit Sales], "
            + "[Measures].[Store Cost], "
            + "[Measures].[Store Sales]} on columns,\n"
            + " Hierarchize(\n"
            + "   Union(\n"
            + "     Crossjoin(\n"
            + "       Crossjoin([Gender].[All Gender].children,\n"
            + "                 [Marital Status].[All Marital Status].children),\n"
            + "       Crossjoin([Customers].[All Customers].children,\n"
            + "                 [Product].[Products].children) ),\n"
            + "     Crossjoin({([Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M])},\n"
            + "       Crossjoin(\n"
            + "         [Customers].[All Customers].[USA].children,\n"
            + "         [Product].[Products].children) ) )) on rows\n"
            + "from Sales where ([Time].[1997])");
        final Axis rowsAxis = result.getAxes()[1];
        assertEquals(45, rowsAxis.getPositions().size());
    }

    public void testItemMember() {
        assertExprReturns(
            "Descendants([Time].[1997], [Time].[Month]).Item(1).Item(0).UniqueName",
            "[Time].[Time].[1997].[Q1].[2]");

        // Access beyond the list yields the Null member.
        if (isDefaultNullMemberRepresentation()) {
            assertExprReturns(
                "[Time].[1997].Children.Item(6).UniqueName",
                "[Time].[Time].[#null]");
            assertExprReturns(
                "[Time].[1997].Children.Item(-1).UniqueName",
                "[Time].[Time].[#null]");
        }
    }

    public void testItemTuple() {
        assertExprReturns(
            "CrossJoin([Gender].[All Gender].children, "
            + "[Time].[1997].[Q2].children).Item(0).Item(1).UniqueName",
            "[Time].[Time].[1997].[Q2].[4]");
    }

    public void testStrToMember() {
        assertExprReturns(
            "StrToMember(\"[Time].[1997].[Q2].[4]\").Name",
            "4");
    }

    public void testStrToMemberUniqueName() {
        assertExprReturns(
            "StrToMember(\"[Store].[USA].[CA]\").Name",
            "CA");
    }

    public void testStrToMemberFullyQualifiedName() {
        assertExprReturns(
            "StrToMember(\"[Store].[All Stores].[USA].[CA]\").Name",
            "CA");
    }

    public void testStrToMemberNull() {
        // SSAS 2005 gives "#Error An MDX expression was expected. An empty
        // expression was specified."
        assertExprThrows(
            "StrToMember(null).Name",
            "An MDX expression was expected. An empty expression was specified");
        assertExprThrows(
            "StrToSet(null, [Gender]).Count",
            "An MDX expression was expected. An empty expression was specified");
        assertExprThrows(
            "StrToTuple(null, [Gender]).Name",
            "An MDX expression was expected. An empty expression was specified");
    }

    /**
     * Testcase for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-560">
     * bug MONDRIAN-560, "StrToMember function doesn't use IgnoreInvalidMembers
     * option"</a>.
     */
    public void testStrToMemberIgnoreInvalidMembers() {
        propSaver.set(propSaver.props.IgnoreInvalidMembersDuringQuery, true);

        // [Product].[Drugs] is invalid, becomes null member, and is dropped
        // from list
        assertQueryReturns(
            "select \n"
            + "  {[Product].[Food],\n"
            + "    StrToMember(\"[Product].[Drugs]\")} on columns,\n"
            + "  {[Measures].[Unit Sales]} on rows\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Product].[Products].[Food]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Row #0: 191,940\n");

        // Hierarchy is inferred from leading edge
        assertExprReturns(
            "StrToMember(\"[Marital Status].[Separated]\").Hierarchy.Name",
            "Marital Status");

        // Null member is returned
        assertExprReturns(
            "StrToMember(\"[Marital Status].[Separated]\").Name",
            "#null");

        // Use longest valid prefix, so get [Time].[Weekly] rather than just
        // [Time].
        final String timeWeekly = TestContext.hierarchyName("Time", "Weekly");
        assertExprReturns(
            "StrToMember(\"" + timeWeekly
            + ".[1996].[Q1]\").Hierarchy.UniqueName",
            timeWeekly);

        // If hierarchy is invalid, throw an error even though
        // IgnoreInvalidMembersDuringQuery is set.
        assertExprThrows(
            "StrToMember(\"[Unknown Hierarchy].[Invalid].[Member]\").Name",
            "MDX object '[Unknown Hierarchy].[Invalid].[Member]' not found in cube 'Sales'");
        assertExprThrows(
            "StrToMember(\"[Unknown Hierarchy].[Invalid]\").Name",
            "MDX object '[Unknown Hierarchy].[Invalid]' not found in cube 'Sales'");
        assertExprThrows(
            "StrToMember(\"[Unknown Hierarchy]\").Name",
            "MDX object '[Unknown Hierarchy]' not found in cube 'Sales'");

        assertAxisThrows(
            "StrToMember(\"\")",
            "MDX object '' not found in cube 'Sales'");

        propSaver.set(propSaver.props.IgnoreInvalidMembersDuringQuery, false);
        assertQueryThrows(
            "select \n"
            + "  {[Product].[Food],\n"
            + "    StrToMember(\"[Product].[Drugs]\")} on columns,\n"
            + "  {[Measures].[Unit Sales]} on rows\n"
            + "from [Sales]",
            "Member '[Product].[Drugs]' not found");
        assertExprThrows(
            "StrToMember(\"[Marital Status].[Separated]\").Hierarchy.Name",
            "Member '[Marital Status].[Separated]' not found");
    }

    public void testStrToTuple() {
        // single dimension yields member
        assertAxisReturns(
            "{StrToTuple(\"[Time].[1997].[Q2]\", [Time])}",
            "[Time].[Time].[1997].[Q2]");

        // multiple dimensions yield tuple
        assertAxisReturns(
            "{StrToTuple(\"([Gender].[F], [Time].[1997].[Q2])\", [Gender], [Time])}",
            "{[Customer].[Gender].[F], [Time].[Time].[1997].[Q2]}");

        // todo: test for garbage at end of string
    }

    public void testStrToTupleIgnoreInvalidMembers() {
        propSaver.set(propSaver.props.IgnoreInvalidMembersDuringQuery, true);

        // If any member is invalid, the whole tuple is null.
        assertAxisReturns(
            "StrToTuple(\"([Gender].[M], [Marital Status].[Separated])\","
            + " [Gender], [Marital Status])",
            "");
    }

    public void testStrToTupleDuHierarchiesFails() {
        assertAxisThrows(
            "{StrToTuple(\"([Gender].[F], [Time].[1997].[Q2], [Gender].[M])\", [Gender], [Time], [Gender])}",
                "Tuple contains more than one member of hierarchy '[Customer].[Gender]'.");
    }

    public void testStrToTupleDupHierInSameDimensions() {
        assertAxisThrows(
            "{StrToTuple("
            + "\"([Gender].[F], "
            + "[Time].[1997].[Q2], "
            + "[Time].[Weekly].[1997].[10])\","
            + " [Gender], "
            + TestContext.hierarchyName("Time", "Weekly")
            + ", [Gender])}",
            "Tuple contains more than one member of hierarchy '[Customer].[Gender]'.");
    }

    public void testStrToTupleDepends() {
        getTestContext().assertMemberExprDependsOn(
            "StrToTuple(\"[Time].[1997].[Q2]\", [Time])",
            Collections.<String>emptySet());

        // converted to scalar, depends set is larger
        getTestContext().assertExprDependsOn(
            "StrToTuple(\"[Time].[1997].[Q2]\", [Time])",
            TestContext.allHiersExcept("[Time].[Time]"));

        getTestContext().assertMemberExprDependsOn(
            "StrToTuple(\"[Time].[1997].[Q2], [Gender].[F]\", [Time], [Gender])",
            Collections.<String>emptySet());

        getTestContext().assertExprDependsOn(
            "StrToTuple(\"[Time].[1997].[Q2], [Gender].[F]\", [Time], [Gender])",
            TestContext.allHiersExcept("[Time].[Time]", "[Customer].[Gender]"));
    }

    public void testStrToSet() {
        // TODO: handle text after '}'
        // TODO: handle string which ends too soon
        // TODO: handle spaces before first '{'
        // TODO: test spaces before unbracketed names,
        //       e.g. "{Gender. M, Gender. F   }".

        assertAxisReturns(
            "StrToSet("
            + " \"{[Gender].[F], [Gender].[M]}\","
            + " [Gender])",
            "[Customer].[Gender].[F]\n"
            + "[Customer].[Gender].[M]");

        assertAxisThrows(
            "StrToSet("
            + " \"{[Gender].[F], [Time].[1997]}\","
            + " [Gender])",
            "member is of wrong hierarchy");

        // whitespace ok
        assertAxisReturns(
            "StrToSet("
            + " \"  {   [Gender] .  [F]  ,[Gender].[M] }  \","
            + " [Gender])",
            "[Customer].[Gender].[F]\n"
            + "[Customer].[Gender].[M]");

        // tuples
        assertAxisReturns(
            "StrToSet("
            + "\""
            + "{"
            + " ([Gender].[F], [Time].[1997].[Q2]), "
            + " ([Gender].[M], [Time].[1997])"
            + "}"
            + "\","
            + " [Gender],"
            + " [Time])",
            "{[Customer].[Gender].[F], [Time].[Time].[1997].[Q2]}\n"
            + "{[Customer].[Gender].[M], [Time].[Time].[1997]}");

        // matches unique name
        assertAxisReturns(
            "StrToSet("
            + "\""
            + "{"
            + " [Store].[USA].[CA], "
            + " [Store].[All Stores].[USA].OR,"
            + " [Store].[All Stores]. [USA] . [WA]"
            + "}"
            + "\","
            + " [Store])",
            "[Store].[Stores].[USA].[CA]\n"
            + "[Store].[Stores].[USA].[OR]\n"
            + "[Store].[Stores].[USA].[WA]");
    }

    public void testStrToSetDupDimensionsFails() {
        assertAxisThrows(
            "StrToSet("
            + "\""
            + "{"
            + " ([Gender].[F], [Time].[1997].[Q2], [Gender].[F]), "
            + " ([Gender].[M], [Time].[1997], [Gender].[F])"
            + "}"
            + "\","
            + " [Gender],"
            + " [Time],"
            + " [Gender])",
            "Tuple contains more than one member of hierarchy '[Customer].[Gender]'.");
    }

    public void testStrToSetIgnoreInvalidMembers() {
        propSaver.set(propSaver.props.IgnoreInvalidMembersDuringQuery, true);
        assertAxisReturns(
            "StrToSet("
            + "\""
            + "{"
            + " [Product].[Food],"
            + " [Product].[Food].[You wouldn't like],"
            + " [Product].[Drink].[You would like],"
            + " [Product].[Drink].[Dairy]"
            + "}"
            + "\","
            + " [Product])",
            "[Product].[Products].[Food]\n"
            + "[Product].[Products].[Drink].[Dairy]");

        assertAxisReturns(
            "StrToSet("
            + "\""
            + "{"
            + " ([Gender].[M], [Product].[Food]),"
            + " ([Gender].[F], [Product].[Food].[You wouldn't like]),"
            + " ([Gender].[M], [Product].[Drink].[You would like]),"
            + " ([Gender].[F], [Product].[Drink].[Dairy])"
            + "}"
            + "\","
            + " [Gender], [Product])",
            "{[Customer].[Gender].[M], [Product].[Products].[Food]}\n"
            + "{[Customer].[Gender].[F], [Product].[Products].[Drink].[Dairy]}");
    }

    public void testYtd() {
        assertAxisReturns(
            "Ytd()",
            "[Time].[Time].[1997]");
        assertAxisReturns(
            "Ytd([Time].[1997].[Q3])",
            "[Time].[Time].[1997].[Q1]\n"
            + "[Time].[Time].[1997].[Q2]\n"
            + "[Time].[Time].[1997].[Q3]");
        assertAxisReturns(
            "Ytd([Time].[1997].[Q2].[4])",
            "[Time].[Time].[1997].[Q1].[1]\n"
            + "[Time].[Time].[1997].[Q1].[2]\n"
            + "[Time].[Time].[1997].[Q1].[3]\n"
            + "[Time].[Time].[1997].[Q2].[4]");
        assertAxisThrows(
            "Ytd([Store])",
            "Argument to function 'Ytd' must belong to Time hierarchy");
        getTestContext().assertSetExprDependsOn(
            "Ytd()",
            setOf("[Time].[Time]", "[Time].[Weekly]"));
        getTestContext().assertSetExprDependsOn(
            "Ytd([Time].[1997].[Q2])",
            Collections.<String>emptySet());
    }

    /**
     * Testcase for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-458">
     * bug MONDRIAN-458, "error deducing type of Ytd/Qtd/Mtd functions within
     * Generate"</a>.
     */
    public void testGeneratePlusXtd() {
        assertAxisReturns(
            "generate(\n"
            + "  {[Time].[1997].[Q1].[2], [Time].[1997].[Q3].[7]},\n"
            + " {Ytd( [Time].[Time].currentMember)})",
            "[Time].[Time].[1997].[Q1].[1]\n"
            + "[Time].[Time].[1997].[Q1].[2]\n"
            + "[Time].[Time].[1997].[Q1].[3]\n"
            + "[Time].[Time].[1997].[Q2].[4]\n"
            + "[Time].[Time].[1997].[Q2].[5]\n"
            + "[Time].[Time].[1997].[Q2].[6]\n"
            + "[Time].[Time].[1997].[Q3].[7]");
        assertAxisReturns(
            "generate(\n"
            + "  {[Time].[1997].[Q1].[2], [Time].[1997].[Q3].[7]},\n"
            + " {Ytd( [Time].[Time].currentMember)}, ALL)",
            "[Time].[Time].[1997].[Q1].[1]\n"
            + "[Time].[Time].[1997].[Q1].[2]\n"
            + "[Time].[Time].[1997].[Q1].[1]\n"
            + "[Time].[Time].[1997].[Q1].[2]\n"
            + "[Time].[Time].[1997].[Q1].[3]\n"
            + "[Time].[Time].[1997].[Q2].[4]\n"
            + "[Time].[Time].[1997].[Q2].[5]\n"
            + "[Time].[Time].[1997].[Q2].[6]\n"
            + "[Time].[Time].[1997].[Q3].[7]");
        assertExprReturns(
            "count(generate({[Time].[1997].[Q4].[11]},"
            + " {Qtd( [Time].[Time].currentMember)}))",
            2, 0);
        assertExprReturns(
            "count(generate({[Time].[1997].[Q4].[11]},"
            + " {Mtd( [Time].[Time].currentMember)}))",
            1, 0);
    }

    public void testQtd() {
        // zero args
        assertQueryReturns(
            "with member [Measures].[Foo] as ' SetToStr(Qtd()) '\n"
            + "select {[Measures].[Foo]} on columns\n"
            + "from [Sales]\n"
            + "where [Time].[1997].[Q2].[5]",
            "Axis #0:\n"
            + "{[Time].[Time].[1997].[Q2].[5]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Foo]}\n"
            + "Row #0: {[Time].[Time].[1997].[Q2].[4], [Time].[Time].[1997].[Q2].[5]}\n");

        // one arg, a month
        assertAxisReturns(
            "Qtd([Time].[1997].[Q2].[5])",
            "[Time].[Time].[1997].[Q2].[4]\n"
            + "[Time].[Time].[1997].[Q2].[5]");

        // one arg, a quarter
        assertAxisReturns(
            "Qtd([Time].[1997].[Q2])",
            "[Time].[Time].[1997].[Q2]");

        // one arg, a year
        assertAxisReturns(
            "Qtd([Time].[1997])",
            "");

        assertAxisThrows(
            "Qtd([Store])",
            "Argument to function 'Qtd' must belong to Time hierarchy");
    }

    public void testMtd() {
        // zero args
        assertQueryReturns(
            "with member [Measures].[Foo] as ' SetToStr(Mtd()) '\n"
            + "select {[Measures].[Foo]} on columns\n"
            + "from [Sales]\n"
            + "where [Time].[1997].[Q2].[5]",
            "Axis #0:\n"
            + "{[Time].[Time].[1997].[Q2].[5]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Foo]}\n"
            + "Row #0: {[Time].[Time].[1997].[Q2].[5]}\n");

        // one arg, a month
        assertAxisReturns(
            "Mtd([Time].[1997].[Q2].[5])",
            "[Time].[Time].[1997].[Q2].[5]");

        // one arg, a quarter
        assertAxisReturns(
            "Mtd([Time].[1997].[Q2])",
            "");

        // one arg, a year
        assertAxisReturns(
            "Mtd([Time].[1997])",
            "");

        assertAxisThrows(
            "Mtd([Store])",
            "Argument to function 'Mtd' must belong to Time hierarchy");
    }

    public void testPeriodsToDate() {
        getTestContext().assertSetExprDependsOn(
            "PeriodsToDate()", setOf("[Time].[Time]"));
        getTestContext().assertSetExprDependsOn(
            "PeriodsToDate([Time].[Year])",
            setOf("[Time].[Time]"));
        getTestContext().assertSetExprDependsOn(
            "PeriodsToDate([Time].[Year], [Time].[1997].[Q2].[5])",
            Collections.<String>emptySet());

        // two args
        assertAxisReturns(
            "PeriodsToDate([Time].[Quarter], [Time].[1997].[Q2].[5])",
            "[Time].[Time].[1997].[Q2].[4]\n"
            + "[Time].[Time].[1997].[Q2].[5]");

        // equivalent to above
        assertAxisReturns(
            "TopCount("
            + "  Descendants("
            + "    Ancestor("
            + "      [Time].[1997].[Q2].[5], [Time].[Quarter]),"
            + "    [Time].[1997].[Q2].[5].Level),"
            + "  1).Item(0) : [Time].[1997].[Q2].[5]",
            "[Time].[Time].[1997].[Q2].[4]\n"
            + "[Time].[Time].[1997].[Q2].[5]");

        // one arg
        assertQueryReturns(
            "with member [Measures].[Foo] as ' SetToStr(PeriodsToDate([Time].[Quarter])) '\n"
            + "select {[Measures].[Foo]} on columns\n"
            + "from [Sales]\n"
            + "where [Time].[1997].[Q2].[5]",
            "Axis #0:\n"
            + "{[Time].[Time].[1997].[Q2].[5]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Foo]}\n"
            + "Row #0: {[Time].[Time].[1997].[Q2].[4], [Time].[Time].[1997].[Q2].[5]}\n");

        // zero args
        assertQueryReturns(
            "with member [Measures].[Foo] as ' SetToStr(PeriodsToDate()) '\n"
            + "select {[Measures].[Foo]} on columns\n"
            + "from [Sales]\n"
            + "where [Time].[1997].[Q2].[5]",
            "Axis #0:\n"
            + "{[Time].[Time].[1997].[Q2].[5]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Foo]}\n"
            + "Row #0: {[Time].[Time].[1997].[Q2].[4], [Time].[Time].[1997].[Q2].[5]}\n");

        // zero args, evaluated at a member which is at the top level.
        // The default level is the level above the current member -- so
        // choosing a member at the highest level might trip up the
        // implementation.
        assertQueryReturns(
            "with member [Measures].[Foo] as ' SetToStr(PeriodsToDate()) '\n"
            + "select {[Measures].[Foo]} on columns\n"
            + "from [Sales]\n"
            + "where [Time].[1997]",
            "Axis #0:\n"
            + "{[Time].[Time].[1997]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Foo]}\n"
            + "Row #0: {}\n");

        // Testcase for bug 1598379, which caused NPE because the args[0].type
        // knew its dimension but not its hierarchy.
        assertQueryReturns(
            "with member [Measures].[Position] as\n"
            + " 'Sum("
            + "PeriodsToDate([Time].[Time].Levels(0),"
            + " [Time].[Time].CurrentMember), "
            + "[Measures].[Store Sales])'\n"
            + "select {[Time].[1997],\n"
            + " [Time].[1997].[Q1],\n"
            + " [Time].[1997].[Q1].[1],\n"
            + " [Time].[1997].[Q1].[2],\n"
            + " [Time].[1997].[Q1].[3]} ON COLUMNS,\n"
            + "{[Measures].[Store Sales], [Measures].[Position] } ON ROWS\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time].[Time].[1997]}\n"
            + "{[Time].[Time].[1997].[Q1]}\n"
            + "{[Time].[Time].[1997].[Q1].[1]}\n"
            + "{[Time].[Time].[1997].[Q1].[2]}\n"
            + "{[Time].[Time].[1997].[Q1].[3]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Store Sales]}\n"
            + "{[Measures].[Position]}\n"
            + "Row #0: 565,238.13\n"
            + "Row #0: 139,628.35\n"
            + "Row #0: 45,539.69\n"
            + "Row #0: 44,058.79\n"
            + "Row #0: 50,029.87\n"
            + "Row #1: 565,238.13\n"
            + "Row #1: 139,628.35\n"
            + "Row #1: 45,539.69\n"
            + "Row #1: 89,598.48\n"
            + "Row #1: 139,628.35\n");

        assertQueryReturns(
            "select\n"
            + "{[Measures].[Unit Sales]} on columns,\n"
            + "periodstodate(\n"
            + "    [Product].[Product Category],\n"
            + "    [Product].[Food].[Baked Goods].[Bread].[Muffins]) on rows\n"
            + "from [Sales]\n"
            + "",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Products].[Food].[Baked Goods].[Bread].[Bagels]}\n"
            + "{[Product].[Products].[Food].[Baked Goods].[Bread].[Muffins]}\n"
            + "Row #0: 815\n"
            + "Row #1: 3,497\n"
            + "");

        // TODO: enable
        if (false) {
            assertExprThrows(
                "Sum(PeriodsToDate([Time.Weekly].[Year], [Time].CurrentMember), [Measures].[Unit Sales])",
                "wrong dimension");
        }
    }

    public void testSetToStr() {
        assertExprReturns(
            "SetToStr([Time].[Time].children)",
            "{[Time].[Time].[1997].[Q1],"
            + " [Time].[Time].[1997].[Q2],"
            + " [Time].[Time].[1997].[Q3],"
            + " [Time].[Time].[1997].[Q4]}");

        // Now, applied to tuples
        assertExprReturns(
            "SetToStr({CrossJoin([Marital Status].children, {[Gender].[M]})})",
            "{"
            + "([Customer].[Marital Status].[M], [Customer].[Gender].[M]), "
            + "([Customer].[Marital Status].[S], [Customer].[Gender].[M])"
            + "}");
    }

    public void testTupleToStr() {
        // Applied to a dimension (which becomes a member)
        assertExprReturns(
            "TupleToStr([Product])",
            "[Product].[Products].[All Products]");

        // Applied to a dimension (invalid because has no default hierarchy)
        assertExprThrows(
            "TupleToStr([Time])",
            "The 'Time' dimension contains more than one hierarchy, "
            + "therefore the hierarchy must be explicitly specified.");

        // Applied to a hierarchy
        assertExprReturns(
            "TupleToStr([Time].[Time])",
            "[Time].[Time].[1997]");

        // Applied to a member
        assertExprReturns(
            "TupleToStr([Store].[USA].[OR])",
            "[Store].[Stores].[USA].[OR]");

        // Applied to a member (extra set of parens)
        assertExprReturns(
            "TupleToStr(([Store].[USA].[OR]))",
            "[Store].[Stores].[USA].[OR]");

        // Now, applied to a tuple
        assertExprReturns(
            "TupleToStr(([Marital Status], [Gender].[M]))",
            "([Customer].[Marital Status].[All Marital Status], [Customer].[Gender].[M])");

        // Applied to a tuple containing a null member
        assertExprReturns(
            "TupleToStr(([Marital Status], [Gender].Parent))",
            "");

        // Applied to a null member
        assertExprReturns(
            "TupleToStr([Marital Status].Parent)",
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
     *
     * @param expr MDX scalar expression
     * @param expected Expected value
     * @param delta Maximum allowed deviation from expected value
     */
    public void assertExprReturns(
        String expr, double expected, double delta)
    {
        Object value = getTestContext().executeExprRaw(expr).getValue();

        try {
            double actual = ((Number) value).doubleValue();
            if (Double.isNaN(expected) && Double.isNaN(actual)) {
                return;
            }
            assertEquals(
                null,
                expected,
                actual,
                delta);
        } catch (ClassCastException ex) {
            String msg = "Actual value \"" + value + "\" is not a number.";
            throw new ComparisonFailure(
                msg, Double.toString(expected), String.valueOf(value));
        }
    }

    /**
     * Compiles a scalar expression, and asserts that the program looks as
     * expected.
     */
    public void assertExprCompilesTo(
        String expr,
        String expectedCalc)
    {
        final String actualCalc =
            getTestContext().compileExpression(expr, true);
        final int expDeps =
            MondrianProperties.instance().TestExpDependencies.get();
        if (expDeps > 0) {
            // Don't bother checking the compiled output if we are also
            // testing dependencies. The compiled code will have extra
            // 'DependencyTestingCalc' instances embedded in it.
            return;
        }
        TestContext.assertEqualsVerbose(expectedCalc, actualCalc);
    }

    /**
     * Compiles a set expression, and asserts that the program looks as
     * expected.
     */
    public void assertAxisCompilesTo(
        String expr,
        String expectedCalc)
    {
        final String actualCalc =
            getTestContext().compileExpression(expr, false);
        final int expDeps =
            MondrianProperties.instance().TestExpDependencies.get();
        if (expDeps > 0) {
            // Don't bother checking the compiled output if we are also
            // testing dependencies. The compiled code will have extra
            // 'DependencyTestingCalc' instances embedded in it.
            return;
        }
        TestContext.assertEqualsVerbose(expectedCalc, actualCalc);
    }

    /**
     * Tests the <code>Rank(member, set)</code> MDX function.
     */
    public void testRank() {
        // Member within set
        assertExprReturns(
            "Rank([Store].[USA].[CA], "
            + "{[Store].[USA].[OR],"
            + " [Store].[USA].[CA],"
            + " [Store].[USA]})", "2");
        // Member not in set
        assertExprReturns(
            "Rank([Store].[USA].[WA], "
            + "{[Store].[USA].[OR],"
            + " [Store].[USA].[CA],"
            + " [Store].[USA]})", "0");
        // Member not in empty set
        assertExprReturns(
            "Rank([Store].[USA].[WA], {})", "0");
        // Null member not in set returns null.
        assertExprReturns(
            "Rank([Store].[Stores].Parent, "
            + "{[Store].[USA].[OR],"
            + " [Store].[USA].[CA],"
            + " [Store].[USA]})",
            "");
        // Null member in empty set. (MSAS returns an error "Formula error -
        // dimension count is not valid - in the Rank function" but I think
        // null is the correct behavior.)
        assertExprReturns(
            "Rank([Gender].Parent, {})", "");
        // Member occurs twice in set -- pick first
        assertExprReturns(
            "Rank([Store].[USA].[WA], \n"
            + "{[Store].[USA].[WA],"
            + " [Store].[USA].[CA],"
            + " [Store].[USA],"
            + " [Store].[USA].[WA]})", "1");
        // Tuple not in set
        assertExprReturns(
            "Rank(([Gender].[F], [Marital Status].[M]), \n"
            + "{([Gender].[F], [Marital Status].[S]),\n"
            + " ([Gender].[M], [Marital Status].[S]),\n"
            + " ([Gender].[M], [Marital Status].[M])})", "0");
        // Tuple in set
        assertExprReturns(
            "Rank(([Gender].[F], [Marital Status].[M]), \n"
            + "{([Gender].[F], [Marital Status].[S]),\n"
            + " ([Gender].[M], [Marital Status].[S]),\n"
            + " ([Gender].[F], [Marital Status].[M])})", "3");
        // Tuple not in empty set
        assertExprReturns(
            "Rank(([Gender].[F], [Marital Status].[M]), \n"
            + "{})", "0");
        // Partially null tuple in set, returns null
        assertExprReturns(
            "Rank(([Gender].[F], [Marital Status].Parent), \n"
            + "{([Gender].[F], [Marital Status].[S]),\n"
            + " ([Gender].[M], [Marital Status].[S]),\n"
            + " ([Gender].[F], [Marital Status].[M])})", "");
    }

    public void testRankWithExpr() {
        // Note that [Good] and [Top Measure] have the same [Unit Sales]
        // value (5), but [Good] ranks 1 and [Top Measure] ranks 2. Even though
        // they are sorted descending on unit sales, they remain in their
        // natural order (member name) because MDX sorts are stable.
        assertQueryReturns(
            "with member [Measures].[Sibling Rank] as ' Rank([Product].CurrentMember, [Product].CurrentMember.Siblings) '\n"
            + "  member [Measures].[Sales Rank] as ' Rank([Product].CurrentMember, Order([Product].Parent.Children, [Measures].[Unit Sales], DESC)) '\n"
            + "  member [Measures].[Sales Rank2] as ' Rank([Product].CurrentMember, [Product].Parent.Children, [Measures].[Unit Sales]) '\n"
            + "select {[Measures].[Unit Sales], [Measures].[Sales Rank], [Measures].[Sales Rank2]} on columns,\n"
            + " {[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].children} on rows\n"
            + "from [Sales]\n"
            + "WHERE ([Store].[USA].[OR].[Portland].[Store 11], [Time].[1997].[Q2].[6])",
            "Axis #0:\n"
            + "{[Store].[Stores].[USA].[OR].[Portland].[Store 11], [Time].[Time].[1997].[Q2].[6]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Sales Rank]}\n"
            + "{[Measures].[Sales Rank2]}\n"
            + "Axis #2:\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good]}\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Pearl]}\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth]}\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Top Measure]}\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Walrus]}\n"
            + "Row #0: 5\n"
            + "Row #0: 1\n"
            + "Row #0: 1\n"
            + "Row #1: \n"
            + "Row #1: 5\n"
            + "Row #1: 5\n"
            + "Row #2: 3\n"
            + "Row #2: 3\n"
            + "Row #2: 3\n"
            + "Row #3: 5\n"
            + "Row #3: 2\n"
            + "Row #3: 1\n"
            + "Row #4: 3\n"
            + "Row #4: 4\n"
            + "Row #4: 3\n");
    }

    public void testRankMembersWithTiedExpr() {
        assertQueryReturns(
            "with "
            + " Set [Beers] as {[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].children} "
            + "  member [Measures].[Sales Rank] as ' Rank([Product].CurrentMember, [Beers], [Measures].[Unit Sales]) '\n"
            + "select {[Measures].[Unit Sales], [Measures].[Sales Rank]} on columns,\n"
            + " Generate([Beers], {[Product].CurrentMember}) on rows\n"
            + "from [Sales]\n"
            + "WHERE ([Store].[USA].[OR].[Portland].[Store 11], [Time].[1997].[Q2].[6])",
            "Axis #0:\n"
            + "{[Store].[Stores].[USA].[OR].[Portland].[Store 11], [Time].[Time].[1997].[Q2].[6]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Sales Rank]}\n"
            + "Axis #2:\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good]}\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Pearl]}\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth]}\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Top Measure]}\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Walrus]}\n"
            + "Row #0: 5\n"
            + "Row #0: 1\n"
            + "Row #1: \n"
            + "Row #1: 5\n"
            + "Row #2: 3\n"
            + "Row #2: 3\n"
            + "Row #3: 5\n"
            + "Row #3: 1\n"
            + "Row #4: 3\n"
            + "Row #4: 3\n");
    }

    public void testRankTuplesWithTiedExpr() {
        assertQueryReturns(
            "with "
            + " Set [Beers for Store] as 'NonEmptyCrossJoin("
            + "[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].children, "
            + "{[Store].[USA].[OR].[Portland].[Store 11]})' "
            + "  member [Measures].[Sales Rank] as ' Rank(([Product].CurrentMember,[Stores].CURRENTMEMBER), [Beers for Store], [Measures].[Unit Sales]) '\n"
            + "select {[Measures].[Unit Sales], [Measures].[Sales Rank]} on columns,\n"
            + " Generate([Beers for Store], {([Product].CurrentMember, [Stores].CURRENTMEMBER)}) on rows\n"
            + "from [Sales]\n"
            + "WHERE ([Time].[1997].[Q2].[6])",
            "Axis #0:\n"
            + "{[Time].[Time].[1997].[Q2].[6]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Sales Rank]}\n"
            + "Axis #2:\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good], [Store].[Stores].[USA].[OR].[Portland].[Store 11]}\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth], [Store].[Stores].[USA].[OR].[Portland].[Store 11]}\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Top Measure], [Store].[Stores].[USA].[OR].[Portland].[Store 11]}\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Walrus], [Store].[Stores].[USA].[OR].[Portland].[Store 11]}\n"
            + "Row #0: 5\n"
            + "Row #0: 1\n"
            + "Row #1: 3\n"
            + "Row #1: 3\n"
            + "Row #2: 5\n"
            + "Row #2: 1\n"
            + "Row #3: 3\n"
            + "Row #3: 3\n");
    }

    public void testRankWithExpr2() {
        // Data: Unit Sales
        // All gender 266,733
        // F          131,558
        // M          135,215
        assertExprReturns(
            "Rank([Gender].[All Gender],"
            + " {[Gender].Members},"
            + " [Measures].[Unit Sales])", "1");
        assertExprReturns(
            "Rank([Gender].[F],"
            + " {[Gender].Members},"
            + " [Measures].[Unit Sales])", "3");
        assertExprReturns(
            "Rank([Gender].[M],"
            + " {[Gender].Members},"
            + " [Measures].[Unit Sales])", "2");
        // Null member. Expression evaluates to null, therefore value does
        // not appear in the list of values, therefore the rank is null.
        assertExprReturns(
            "Rank([Gender].[All Gender].Parent,"
            + " {[Gender].Members},"
            + " [Measures].[Unit Sales])", "");
        // Empty set. Value would appear after all elements in the empty set,
        // therefore rank is 1.
        // Note that SSAS gives error 'The first argument to the Rank function,
        // a tuple expression, should reference the same hierachies as the
        // second argument, a set expression'. I think that's because it can't
        // deduce a type for '{}'. SSAS's problem, not Mondrian's. :)
        assertExprReturns(
            "Rank([Gender].[M],"
            + " {},"
            + " [Measures].[Unit Sales])",
            "1");
        // As above, but SSAS can type-check this.
        assertExprReturns(
            "Rank([Gender].[M],"
            + " Filter(Gender.Members, 1 = 0),"
            + " [Measures].[Unit Sales])",
            "1");
        // Member is not in set
        assertExprReturns(
            "Rank([Gender].[M],"
            + " {[Gender].[All Gender], [Gender].[F]})",
            "0");
        // Even though M is not in the set, its value lies between [All Gender]
        // and [F].
        assertExprReturns(
            "Rank([Gender].[M],"
            + " {[Gender].[All Gender], [Gender].[F]},"
            + " [Measures].[Unit Sales])",
            "2");
        // Expr evaluates to null for some values of set.
        assertExprReturns(
            "Rank([Product].[Non-Consumable].[Household],"
            + " {[Product].[Food], [Product].[All Products], [Product].[Drink].[Dairy]},"
            + " [Product].CurrentMember.Parent)",
            "2");
        // Expr evaluates to null for all values in the set.
        assertExprReturns(
            "Rank([Gender].[M],"
            + " {[Gender].[All Gender], [Gender].[F]},"
            + " [Marital Status].[All Marital Status].Parent)",
            "1");
    }

    /**
     * Tests the 3-arg version of the RANK function with a value
     * which returns null within a set of nulls.
     */
    public void testRankWithNulls() {
        assertQueryReturns(
            "with member [Measures].[X] as "
            + "'iif([Measures].[Store Sales]=777,"
            + "[Measures].[Store Sales],Null)'\n"
            + "member [Measures].[Y] as 'Rank([Gender].[M],"
            + "{[Measures].[X],[Measures].[X],[Measures].[X]},"
            + " [Marital Status].[All Marital Status].Parent)'"
            + "select {[Measures].[Y]} on columns from Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Y]}\n"
            + "Row #0: 1\n");
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

        checkRankHuge(
            "WITH \n"
            + "  MEMBER [Measures].[Rank among products] \n"
            + "    AS ' Rank([Product].CurrentMember, "
            + "            Order([Product].members, "
            + "            [Measures].[Unit Sales], BDESC)) '\n"
            + "SELECT CrossJoin(\n"
            + "  [Gender].members,\n"
            + "  {[Measures].[Unit Sales],\n"
            + "   [Measures].[Rank among products]}) ON COLUMNS,\n"
            // + "  {[Product], [Product].[Products].[Non-Consumable].
            // [Periodicals].[Magazines].[Sports Magazines].[Robust].
            // [Robust Monthly Sports Magazine]} ON ROWS\n"
            + "  {[Product].members} ON ROWS\n"
            + "FROM [Sales]",
            false);
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

        checkRankHuge(
            "WITH \n"
            + "  MEMBER [Measures].[Rank among products] \n"
            + "    AS ' Rank([Product].CurrentMember, [Product].members, [Measures].[Unit Sales]) '\n"
            + "SELECT CrossJoin(\n"
            + "  [Gender].members,\n"
            + "  {[Measures].[Unit Sales],\n"
            + "   [Measures].[Rank among products]}) ON COLUMNS,\n"
            + "  {[Product],"
            + "   [Product].[Products].[Non-Consumable].[Periodicals]"
            + ".[Magazines].[Sports Magazines].[Robust]"
            + ".[Robust Monthly Sports Magazine]} ON ROWS\n"
            // + "  {[Product].members} ON ROWS\n"
            + "FROM [Sales]",
            true);
    }

    private void checkRankHuge(String query, boolean rank3) {
        final Result result = getTestContext().executeQuery(query);
        final Axis[] axes = result.getAxes();
        final Axis rowsAxis = axes[1];
        final int rowCount = rowsAxis.getPositions().size();
        assertEquals(FILTER_SNOWFLAKE ? 2256 : 2266, rowCount);
        // [All Products], [All Gender], [Rank]
        Cell cell = result.getCell(new int[] {1, 0});
        assertEquals("1", cell.getFormattedValue());
        // [Robust Monthly Sports Magazine]
        Member member = rowsAxis.getPositions().get(rowCount - 1).get(0);
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
        assertQueryReturns(
            "WITH MEMBER [Measures].[Test] as \n"
            + "  'LinRegPoint(\n"
            + "    Rank(Time.[Time].CurrentMember, Time.[Time].CurrentMember.Level.Members),\n"
            + "    Descendants([Time].[1997], [Time].[Quarter]), \n"
            + "[Measures].[Store Sales], \n"
            + "    Rank(Time.[Time].CurrentMember, Time.[Time].CurrentMember.Level.Members))' \n"
            + "SELECT \n"
            + "{[Measures].[Test],[Measures].[Store Sales]} ON ROWS, \n"
            + "{[Time].[1997].Children} ON COLUMNS \n"
            + "FROM Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time].[Time].[1997].[Q1]}\n"
            + "{[Time].[Time].[1997].[Q2]}\n"
            + "{[Time].[Time].[1997].[Q3]}\n"
            + "{[Time].[Time].[1997].[Q4]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Test]}\n"
            + "{[Measures].[Store Sales]}\n"
            + "Row #0: 134,299.22\n"
            + "Row #0: 138,972.76\n"
            + "Row #0: 143,646.30\n"
            + "Row #0: 148,319.85\n"
            + "Row #1: 139,628.35\n"
            + "Row #1: 132,666.27\n"
            + "Row #1: 140,271.89\n"
            + "Row #1: 152,671.62\n");
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
        assertQueryReturns(
            "WITH MEMBER \n"
            + "[Measures].[Intercept] AS \n"
            + "  'LinRegIntercept([Time].CurrentMember.Lag(10) : [Time].CurrentMember, [Measures].[Unit Sales], [Measures].[Store Sales])' \n"
            + "MEMBER [Measures].[Regression Slope] AS\n"
            + "  'LinRegSlope([Time].CurrentMember.Lag(9) : [Time].CurrentMember,[Measures].[Unit Sales],[Measures].[Store Sales]) '\n"
            + "MEMBER [Measures].[Predict] AS\n"
            + "  'LinRegPoint([Measures].[Unit Sales],[Time].CurrentMember.Lag(9) : [Time].CurrentMember,[Measures].[Unit Sales],[Measures].[Store Sales])',\n"
            + "  FORMAT_STRING = 'Standard' \n"
            + "MEMBER [Measures].[Predict Formula] AS\n"
            + "  '([Measures].[Regression Slope] * [Measures].[Unit Sales]) + [Measures].[Intercept]',\n"
            + "  FORMAT_STRING='Standard'\n"
            + "MEMBER [Measures].[Good Fit] AS\n"
            + "  'LinRegR2([Time].CurrentMember.Lag(9) : [Time].CurrentMember, [Measures].[Unit Sales],[Measures].[Store Sales])',\n"
            + "  FORMAT_STRING='#,#.00'\n"
            + "MEMBER [Measures].[Variance] AS\n"
            + "  'LinRegVariance([Time].CurrentMember.Lag(9) : [Time].CurrentMember,[Measures].[Unit Sales],[Measures].[Store Sales])'\n"
            + "SELECT \n"
            + "  {[Measures].[Store Sales], \n"
            + "   [Measures].[Intercept], \n"
            + "   [Measures].[Regression Slope], \n"
            + "   [Measures].[Predict], \n"
            + "   [Measures].[Predict Formula], \n"
            + "   [Measures].[Good Fit], \n"
            + "   [Measures].[Variance] } ON COLUMNS, \n"
            + "  Descendants([Time].[1997], [Time].[Month]) ON ROWS\n"
            + "FROM Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Sales]}\n"
            + "{[Measures].[Intercept]}\n"
            + "{[Measures].[Regression Slope]}\n"
            + "{[Measures].[Predict]}\n"
            + "{[Measures].[Predict Formula]}\n"
            + "{[Measures].[Good Fit]}\n"
            + "{[Measures].[Variance]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[1997].[Q1].[1]}\n"
            + "{[Time].[Time].[1997].[Q1].[2]}\n"
            + "{[Time].[Time].[1997].[Q1].[3]}\n"
            + "{[Time].[Time].[1997].[Q2].[4]}\n"
            + "{[Time].[Time].[1997].[Q2].[5]}\n"
            + "{[Time].[Time].[1997].[Q2].[6]}\n"
            + "{[Time].[Time].[1997].[Q3].[7]}\n"
            + "{[Time].[Time].[1997].[Q3].[8]}\n"
            + "{[Time].[Time].[1997].[Q3].[9]}\n"
            + "{[Time].[Time].[1997].[Q4].[10]}\n"
            + "{[Time].[Time].[1997].[Q4].[11]}\n"
            + "{[Time].[Time].[1997].[Q4].[12]}\n"
            + "Row #0: 45,539.69\n"
            + "Row #0: 68711.40\n"
            + "Row #0: -1.033\n"
            + "Row #0: 46,350.26\n"
            + "Row #0: 46.350.26\n"
            + "Row #0: -1.#INF\n"
            + "Row #0: 5.17E-08\n"
            + "...\n"
            + "Row #11: 15343.67\n");
    }

    public void testLinRegPointMonth() {
        assertQueryReturns(
            "WITH MEMBER \n"
            + "[Measures].[Test] as \n"
            + "  'LinRegPoint(\n"
            + "    Rank(Time.[Time].CurrentMember, Time.[Time].CurrentMember.Level.Members),\n"
            + "    Descendants([Time].[1997], [Time].[Month]), \n"
            + "    [Measures].[Store Sales], \n"
            + "    Rank(Time.[Time].CurrentMember, Time.[Time].CurrentMember.Level.Members)\n"
            + " )' \n"
            + "SELECT \n"
            + "  {[Measures].[Test],[Measures].[Store Sales]} ON ROWS, \n"
            + "  Descendants([Time].[1997], [Time].[Month]) ON COLUMNS \n"
            + "FROM Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time].[Time].[1997].[Q1].[1]}\n"
            + "{[Time].[Time].[1997].[Q1].[2]}\n"
            + "{[Time].[Time].[1997].[Q1].[3]}\n"
            + "{[Time].[Time].[1997].[Q2].[4]}\n"
            + "{[Time].[Time].[1997].[Q2].[5]}\n"
            + "{[Time].[Time].[1997].[Q2].[6]}\n"
            + "{[Time].[Time].[1997].[Q3].[7]}\n"
            + "{[Time].[Time].[1997].[Q3].[8]}\n"
            + "{[Time].[Time].[1997].[Q3].[9]}\n"
            + "{[Time].[Time].[1997].[Q4].[10]}\n"
            + "{[Time].[Time].[1997].[Q4].[11]}\n"
            + "{[Time].[Time].[1997].[Q4].[12]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Test]}\n"
            + "{[Measures].[Store Sales]}\n"
            + "Row #0: 43,824.36\n"
            + "Row #0: 44,420.51\n"
            + "Row #0: 45,016.66\n"
            + "Row #0: 45,612.81\n"
            + "Row #0: 46,208.95\n"
            + "Row #0: 46,805.10\n"
            + "Row #0: 47,401.25\n"
            + "Row #0: 47,997.40\n"
            + "Row #0: 48,593.55\n"
            + "Row #0: 49,189.70\n"
            + "Row #0: 49,785.85\n"
            + "Row #0: 50,382.00\n"
            + "Row #1: 45,539.69\n"
            + "Row #1: 44,058.79\n"
            + "Row #1: 50,029.87\n"
            + "Row #1: 42,878.25\n"
            + "Row #1: 44,456.29\n"
            + "Row #1: 45,331.73\n"
            + "Row #1: 50,246.88\n"
            + "Row #1: 46,199.04\n"
            + "Row #1: 43,825.97\n"
            + "Row #1: 42,342.27\n"
            + "Row #1: 53,363.71\n"
            + "Row #1: 56,965.64\n");
    }

    public void testLinRegIntercept() {
        assertExprReturns(
            "LinRegIntercept([Time].[Month].members,"
            + " [Measures].[Unit Sales], [Measures].[Store Sales])",
            -126.65,
            0.50);

// -1#IND missing data
//
// 1#INF division by zero
//
// The following table shows query return values from using different
// FORMAT_STRING's in an expression involving 'division by zero' (tested on
// Intel platforms):
//
// +===========================+=====================+
// | Format Strings            | Query Return Values |
// +===========================+=====================+
// | FORMAT_STRING="           | 1.#INF              |
// +===========================+=====================+
// | FORMAT_STRING='Standard'  | 1.#J                |
// +===========================+=====================+
// | FORMAT_STRING='Fixed'     | 1.#J                |
// +===========================+=====================+
// | FORMAT_STRING='Percent'   | 1#I.NF%             |
// +===========================+=====================+
// | FORMAT_STRING='Scientific'| 1.JE+00             |
// +===========================+=====================+

        // Mondrian can not return "missing data" value -1.#IND
        // empty set
        if (false) {
            assertExprReturns(
                "LinRegIntercept({[Time].Parent},"
                + " [Measures].[Unit Sales], [Measures].[Store Sales])",
                "-1.#IND"); // MSAS returns -1.#IND (whatever that means)
        }

        // first expr constant
        if (false) {
            assertExprReturns(
                "LinRegIntercept([Time].[Month].members,"
                + " 7, [Measures].[Store Sales])",
                "$7.00");
        }

        // format does not add '$'
        assertExprReturns(
            "LinRegIntercept([Time].[Month].members,"
            + " 7, [Measures].[Store Sales])",
            7.00,
            0.01);

        // Mondrian can not return "missing data" value -1.#IND
        // second expr constant
        if (false) {
            assertExprReturns(
                "LinRegIntercept([Time].[Month].members,"
                + " [Measures].[Unit Sales], 4)",
                "-1.#IND"); // MSAS returns -1.#IND (whatever that means)
        }
    }

    public void testLinRegSlope() {
        assertExprReturns(
            "LinRegSlope([Time].[Month].members,"
            + " [Measures].[Unit Sales], [Measures].[Store Sales])",
            0.4746,
            0.50);

        // Mondrian can not return "missing data" value -1.#IND
        // empty set
        if (false) {
            assertExprReturns(
                "LinRegSlope({[Time].Parent},"
                + " [Measures].[Unit Sales], [Measures].[Store Sales])",
                "-1.#IND"); // MSAS returns -1.#IND (whatever that means)
        }

        // first expr constant
        if (false) {
            assertExprReturns(
                "LinRegSlope([Time].[Month].members,"
                + " 7, [Measures].[Store Sales])",
                "$7.00");
        }
        // ^^^^
        // copy and paste error

        assertExprReturns(
            "LinRegSlope([Time].[Month].members,"
            + " 7, [Measures].[Store Sales])",
            0.00,
            0.01);

        // Mondrian can not return "missing data" value -1.#IND
        // second expr constant
        if (false) {
            assertExprReturns(
                "LinRegSlope([Time].[Month].members,"
                + " [Measures].[Unit Sales], 4)",
                "-1.#IND"); // MSAS returns -1.#IND (whatever that means)
        }
    }

    public void testLinRegPoint() {
        // NOTE: mdx does not parse
        if (false) {
            assertExprReturns(
                "LinRegPoint([Measures].[Unit Sales],"
                + " [Time].CurrentMember[Time].[Month].members,"
                + " [Measures].[Unit Sales], [Measures].[Store Sales])",
                "0.4746");
        }

        // Mondrian can not return "missing data" value -1.#IND
        // empty set
        if (false) {
            assertExprReturns(
                "LinRegPoint([Measures].[Unit Sales],"
                + " {[Time].Parent},"
                + " [Measures].[Unit Sales], [Measures].[Store Sales])",
                "-1.#IND"); // MSAS returns -1.#IND (whatever that means)
        }

        // Expected value is wrong
        // zeroth expr constant
        if (false) {
            assertExprReturns(
                "LinRegPoint(-1,"
                + " [Time].[Month].members,"
                + " 7, [Measures].[Store Sales])", "-127.124");
        }

        // first expr constant
        if (false) {
            assertExprReturns(
                "LinRegPoint([Measures].[Unit Sales],"
                + " [Time].[Month].members,"
                + " 7, [Measures].[Store Sales])", "$7.00");
        }

        // format does not add '$'
        assertExprReturns(
            "LinRegPoint([Measures].[Unit Sales],"
            + " [Time].[Month].members,"
            + " 7, [Measures].[Store Sales])",
            7.00,
            0.01);

        // Mondrian can not return "missing data" value -1.#IND
        // second expr constant
        if (false) {
            assertExprReturns(
                "LinRegPoint([Measures].[Unit Sales],"
                + " [Time].[Month].members,"
                + " [Measures].[Unit Sales], 4)",
                "-1.#IND"); // MSAS returns -1.#IND (whatever that means)
        }
    }

    public void _testLinRegR2() {
        // Why would R2 equal the slope
        if (false) {
            assertExprReturns(
                "LinRegR2([Time].[Month].members,"
                + " [Measures].[Unit Sales], [Measures].[Store Sales])",
                "0.4746");
        }

        // Mondrian can not return "missing data" value -1.#IND
        // empty set
        if (false) {
            assertExprReturns(
                "LinRegR2({[Time].Parent},"
                + " [Measures].[Unit Sales], [Measures].[Store Sales])",
                "-1.#IND"); // MSAS returns -1.#IND (whatever that means)
        }

        // first expr constant
        assertExprReturns(
            "LinRegR2([Time].[Month].members,"
            + " 7, [Measures].[Store Sales])",
            "$7.00");

        // Mondrian can not return "missing data" value -1.#IND
        // second expr constant
        if (false) {
            assertExprReturns(
                "LinRegR2([Time].[Month].members,"
                + " [Measures].[Unit Sales], 4)",
                "-1.#IND"); // MSAS returns -1.#IND (whatever that means)
        }
    }

    public void _testLinRegVariance() {
        assertExprReturns(
            "LinRegVariance([Time].[Month].members,"
            + " [Measures].[Unit Sales], [Measures].[Store Sales])",
            "0.4746");

        // empty set
        assertExprReturns(
            "LinRegVariance({[Time].Parent},"
            + " [Measures].[Unit Sales], [Measures].[Store Sales])",
            "-1.#IND"); // MSAS returns -1.#IND (whatever that means)

        // first expr constant
        assertExprReturns(
            "LinRegVariance([Time].[Month].members,"
            + " 7, [Measures].[Store Sales])",
            "$7.00");

        // second expr constant
        assertExprReturns(
            "LinRegVariance([Time].[Month].members,"
            + " [Measures].[Unit Sales], 4)",
            "-1.#IND"); // MSAS returns -1.#IND (whatever that means)
    }

    public void testVisualTotalsBasic() {
        assertQueryReturns(
            "select {[Measures].[Unit Sales]} on columns, "
            + "{VisualTotals("
            + "    {[Product].[Products].[Food].[Baked Goods].[Bread],"
            + "     [Product].[Products].[Food].[Baked Goods].[Bread].[Bagels],"
            + "     [Product].[Products].[Food].[Baked Goods].[Bread].[Muffins]},"
            + "     \"**Subtotal - *\")} on rows "
            + "from [Sales]",

            // note that Subtotal - Bread only includes 2 displayed children
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Products].[Food].[Baked Goods].[*Subtotal - Bread]}\n"
            + "{[Product].[Products].[Food].[Baked Goods].[Bread].[Bagels]}\n"
            + "{[Product].[Products].[Food].[Baked Goods].[Bread].[Muffins]}\n"
            + "Row #0: 4,312\n"
            + "Row #1: 815\n"
            + "Row #2: 3,497\n");
    }

    public void testVisualTotalsConsecutively() {
        assertQueryReturns(
            "select {[Measures].[Unit Sales]} on columns, "
            + "{VisualTotals("
            + "    {[Product].[Products].[Food].[Baked Goods].[Bread],"
            + "     [Product].[Products].[Food].[Baked Goods].[Bread].[Bagels],"
            + "     [Product].[Products].[Food].[Baked Goods].[Bread].[Bagels],"
            + "     [Product].[Products].[Food].[Baked Goods].[Bread].[Bagels].[Colony],"
            + "     [Product].[Products].[Food].[Baked Goods].[Bread].[Bagels],"
            + "     [Product].[Products].[Food].[Baked Goods].[Bread].[Muffins]},"
            + "     \"**Subtotal - *\")} on rows "
            + "from [Sales]",

            // Note that [Bagels] occurs 3 times, but only once does it
            // become a subtotal. Note that the subtotal does not include
            // the following [Bagels] member.
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Products].[Food].[Baked Goods].[*Subtotal - Bread]}\n"
            + "{[Product].[Products].[Food].[Baked Goods].[Bread].[Bagels]}\n"
            + "{[Product].[Products].[Food].[Baked Goods].[Bread].[*Subtotal - Bagels]}\n"
            + "{[Product].[Products].[Food].[Baked Goods].[Bread].[Bagels].[Colony]}\n"
            + "{[Product].[Products].[Food].[Baked Goods].[Bread].[Bagels]}\n"
            + "{[Product].[Products].[Food].[Baked Goods].[Bread].[Muffins]}\n"
            + "Row #0: 5,290\n"
            + "Row #1: 815\n"
            + "Row #2: 163\n"
            + "Row #3: 163\n"
            + "Row #4: 815\n"
            + "Row #5: 3,497\n");
    }

    public void testVisualTotalsNoPattern() {
        assertAxisReturns(
            "VisualTotals("
            + "    {[Product].[All Products].[Food].[Baked Goods].[Bread],"
            + "     [Product].[All Products].[Food].[Baked Goods].[Bread].[Bagels],"
            + "     [Product].[All Products].[Food].[Baked Goods].[Bread].[Muffins]})",

            // Note that the [Bread] visual member is just called [Bread].
            "[Product].[Products].[Food].[Baked Goods].[Bread]\n"
            + "[Product].[Products].[Food].[Baked Goods].[Bread].[Bagels]\n"
            + "[Product].[Products].[Food].[Baked Goods].[Bread].[Muffins]");
    }

    public void testVisualTotalsWithFilter() {
        assertQueryReturns(
            "select {[Measures].[Unit Sales]} on columns, "
            + "{Filter("
            + "    VisualTotals("
            + "        {[Product].[Products].[Food].[Baked Goods].[Bread],"
            + "         [Product].[Products].[Food].[Baked Goods].[Bread].[Bagels],"
            + "         [Product].[Products].[Food].[Baked Goods].[Bread].[Muffins]},"
            + "        \"**Subtotal - *\"),"
            + "[Measures].[Unit Sales] > 3400)} on rows "
            + "from [Sales]",

            // Note that [*Subtotal - Bread] still contains the
            // contribution of [Bagels] 815, which was filtered out.
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Products].[Food].[Baked Goods].[*Subtotal - Bread]}\n"
            + "{[Product].[Products].[Food].[Baked Goods].[Bread].[Muffins]}\n"
            + "Row #0: 4,312\n"
            + "Row #1: 3,497\n");
    }

    public void testVisualTotalsNested() {
        assertQueryReturns(
            "select {[Measures].[Unit Sales]} on columns, "
            + "{VisualTotals("
            + "    Filter("
            + "        VisualTotals("
            + "            {[Product].[Products].[Food].[Baked Goods].[Bread],"
            + "             [Product].[Products].[Food].[Baked Goods].[Bread].[Bagels],"
            + "             [Product].[Products].[Food].[Baked Goods].[Bread].[Muffins]},"
            + "            \"**Subtotal - *\"),"
            + "    [Measures].[Unit Sales] > 3400),"
            + "    \"Second total - *\")} on rows "
            + "from [Sales]",

            // Yields the same -- no extra total.
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Products].[Food].[Baked Goods].[*Subtotal - Bread]}\n"
            + "{[Product].[Products].[Food].[Baked Goods].[Bread].[Muffins]}\n"
            + "Row #0: 4,312\n"
            + "Row #1: 3,497\n");
    }

    public void testVisualTotalsFilterInside() {
        assertQueryReturns(
            "select {[Measures].[Unit Sales]} on columns, "
            + "{VisualTotals("
            + "    Filter("
            + "        {[Product].[Products].[Food].[Baked Goods].[Bread],"
            + "         [Product].[Products].[Food].[Baked Goods].[Bread].[Bagels],"
            + "         [Product].[Products].[Food].[Baked Goods].[Bread].[Muffins]},"
            + "        [Measures].[Unit Sales] > 3400),"
            + "    \"**Subtotal - *\")} on rows "
            + "from [Sales]",

            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Products].[Food].[Baked Goods].[*Subtotal - Bread]}\n"
            + "{[Product].[Products].[Food].[Baked Goods].[Bread].[Muffins]}\n"
            + "Row #0: 3,497\n"
            + "Row #1: 3,497\n");
    }

    public void testVisualTotalsOutOfOrder() {
        assertQueryReturns(
            "select {[Measures].[Unit Sales]} on columns, "
            + "{VisualTotals("
            + "    {[Product].[Products].[Food].[Baked Goods].[Bread].[Bagels],"
            + "     [Product].[Products].[Food].[Baked Goods].[Bread],"
            + "     [Product].[Products].[Food].[Baked Goods].[Bread].[Muffins]},"
            + "    \"**Subtotal - *\")} on rows "
            + "from [Sales]",

            // Note that [*Subtotal - Bread] 3497 does not include 815 for
            // bagels.
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Products].[Food].[Baked Goods].[Bread].[Bagels]}\n"
            + "{[Product].[Products].[Food].[Baked Goods].[*Subtotal - Bread]}\n"
            + "{[Product].[Products].[Food].[Baked Goods].[Bread].[Muffins]}\n"
            + "Row #0: 815\n"
            + "Row #1: 3,497\n"
            + "Row #2: 3,497\n");
    }

    public void testVisualTotalsGrandparentsAndOutOfOrder() {
        assertQueryReturns(
            "select {[Measures].[Unit Sales]} on columns, "
            + "{VisualTotals("
            + "    {[Product].[Products].[Food],"
            + "     [Product].[Products].[Food].[Baked Goods].[Bread],"
            + "     [Product].[Products].[Food].[Baked Goods].[Bread].[Bagels],"
            + "     [Product].[Products].[Food].[Frozen Foods].[Breakfast Foods],"
            + "     [Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Pancake Mix].[Golden],"
            + "     [Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Pancake Mix].[Big Time],"
            + "     [Product].[Products].[Food].[Baked Goods].[Bread].[Muffins]},"
            + "    \"**Subtotal - *\")} on rows "
            + "from [Sales]",

            // Note:
            // [*Subtotal - Food]  = 4513 = 815 + 311 + 3497
            // [*Subtotal - Bread] = 815, does not include muffins
            // [*Subtotal - Breakfast Foods] = 311 = 110 + 201, includes
            //     grandchildren
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Products].[*Subtotal - Food]}\n"
            + "{[Product].[Products].[Food].[Baked Goods].[*Subtotal - Bread]}\n"
            + "{[Product].[Products].[Food].[Baked Goods].[Bread].[Bagels]}\n"
            + "{[Product].[Products].[Food].[Frozen Foods].[*Subtotal - Breakfast Foods]}\n"
            + "{[Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Pancake Mix].[Golden]}\n"
            + "{[Product].[Products].[Food].[Frozen Foods].[Breakfast Foods].[Pancake Mix].[Big Time]}\n"
            + "{[Product].[Products].[Food].[Baked Goods].[Bread].[Muffins]}\n"
            + "Row #0: 4,623\n"
            + "Row #1: 815\n"
            + "Row #2: 815\n"
            + "Row #3: 311\n"
            + "Row #4: 110\n"
            + "Row #5: 201\n"
            + "Row #6: 3,497\n");
    }

    public void testVisualTotalsCrossjoin() {
        assertAxisThrows(
            "VisualTotals(Crossjoin([Gender].Members, [Store].[Stores].children))",
            "Argument to 'VisualTotals' function must be a set of members; got set of tuples.");
    }

    /**
     * Test case for bug
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-615">MONDRIAN-615</a>,
     * "VisualTotals doesn't work for the all member".
     */
    public void testVisualTotalsAll() {
        if (!Bug.VisualTotalsFixed) {
            return;
        }
        final String query =
            "SELECT \n"
            + "  {[Measures].[Unit Sales]} ON 0, \n"
            + "  VisualTotals(\n"
            + "    {[Customers].[All Customers],\n"
            + "     [Customers].[USA],\n"
            + "     [Customers].[USA].[CA],\n"
            + "     [Customers].[USA].[OR]}) ON 1\n"
            + "FROM [Sales]";
        assertQueryReturns(
            query,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Customers].[All Customers]}\n"
            + "{[Customer].[Customers].[USA]}\n"
            + "{[Customer].[Customers].[USA].[CA]}\n"
            + "{[Customer].[Customers].[USA].[OR]}\n"
            + "Row #0: 142,407\n"
            + "Row #1: 142,407\n"
            + "Row #2: 74,748\n"
            + "Row #3: 67,659\n");

        // Check captions
        final Result result = getTestContext().executeQuery(query);
        final List<Position> positionList = result.getAxes()[1].getPositions();
        assertEquals("All Customers", positionList.get(0).get(0).getCaption());
        assertEquals("USA", positionList.get(1).get(0).getCaption());
        assertEquals("CA", positionList.get(2).get(0).getCaption());
    }

    /**
     * Test case involving a named set and query pivoted. Suggested in
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-615">MONDRIAN-615</a>,
     * "VisualTotals doesn't work for the all member".
     */
    public void testVisualTotalsWithNamedSetAndPivot() {
        if (!Bug.VisualTotalsFixed) {
            return;
        }
        assertQueryReturns(
            "WITH SET [CA_OR] AS\n"
            + "    VisualTotals(\n"
            + "        {[Customers].[All Customers],\n"
            + "         [Customers].[USA],\n"
            + "         [Customers].[USA].[CA],\n"
            + "         [Customers].[USA].[OR]})\n"
            + "SELECT \n"
            + "    Drilldownlevel({[Time].[1997]}) ON 0, \n"
            + "    [CA_OR] ON 1 \n"
            + "FROM [Sales] ",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time].[Time].[1997]}\n"
            + "{[Time].[Time].[1997].[Q1]}\n"
            + "{[Time].[Time].[1997].[Q2]}\n"
            + "{[Time].[Time].[1997].[Q3]}\n"
            + "{[Time].[Time].[1997].[Q4]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Customers].[All Customers]}\n"
            + "{[Customer].[Customers].[USA]}\n"
            + "{[Customer].[Customers].[USA].[CA]}\n"
            + "{[Customer].[Customers].[USA].[OR]}\n"
            + "Row #0: 142,407\n"
            + "Row #0: 36,177\n"
            + "Row #0: 33,131\n"
            + "Row #0: 35,310\n"
            + "Row #0: 37,789\n"
            + "Row #1: 142,407\n"
            + "Row #1: 36,177\n"
            + "Row #1: 33,131\n"
            + "Row #1: 35,310\n"
            + "Row #1: 37,789\n"
            + "Row #2: 74,748\n"
            + "Row #2: 16,890\n"
            + "Row #2: 18,052\n"
            + "Row #2: 18,370\n"
            + "Row #2: 21,436\n"
            + "Row #3: 67,659\n"
            + "Row #3: 19,287\n"
            + "Row #3: 15,079\n"
            + "Row #3: 16,940\n"
            + "Row #3: 16,353\n");

        // same query, swap axes
        assertQueryReturns(
            "WITH SET [CA_OR] AS\n"
            + "    VisualTotals(\n"
            + "        {[Customers].[All Customers],\n"
            + "         [Customers].[USA],\n"
            + "         [Customers].[USA].[CA],\n"
            + "         [Customers].[USA].[OR]})\n"
            + "SELECT \n"
            + "    [CA_OR] ON 0,\n"
            + "    Drilldownlevel({[Time].[1997]}) ON 1\n"
            + "FROM [Sales] ",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Customer].[Customers].[All Customers]}\n"
            + "{[Customer].[Customers].[USA]}\n"
            + "{[Customer].[Customers].[USA].[CA]}\n"
            + "{[Customer].[Customers].[USA].[OR]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[1997]}\n"
            + "{[Time].[Time].[1997].[Q1]}\n"
            + "{[Time].[Time].[1997].[Q2]}\n"
            + "{[Time].[Time].[1997].[Q3]}\n"
            + "{[Time].[Time].[1997].[Q4]}\n"
            + "Row #0: 142,407\n"
            + "Row #0: 142,407\n"
            + "Row #0: 74,748\n"
            + "Row #0: 67,659\n"
            + "Row #1: 36,177\n"
            + "Row #1: 36,177\n"
            + "Row #1: 16,890\n"
            + "Row #1: 19,287\n"
            + "Row #2: 33,131\n"
            + "Row #2: 33,131\n"
            + "Row #2: 18,052\n"
            + "Row #2: 15,079\n"
            + "Row #3: 35,310\n"
            + "Row #3: 35,310\n"
            + "Row #3: 18,370\n"
            + "Row #3: 16,940\n"
            + "Row #4: 37,789\n"
            + "Row #4: 37,789\n"
            + "Row #4: 21,436\n"
            + "Row #4: 16,353\n");
    }

    /**
     * Tests that members generated by VisualTotals have correct identity.
     *
     * <p>Testcase for <a href="http://jira.pentaho.com/browse/MONDRIAN-295">
     * bug MONDRIAN-295, "Query generated by Excel 2007 gives incorrect
     * results"</a>.
     */
    public void testVisualTotalsIntersect() {
        if (!Bug.VisualTotalsFixed) {
            return;
        }
        assertQueryReturns(
            "WITH\n"
            + "SET [XL_Row_Dim_0] AS 'VisualTotals(Distinct(Hierarchize({Ascendants([Customers].[All Customers].[USA]), Descendants([Customers].[All Customers].[USA])})))' \n"
            + "SELECT \n"
            + "NON EMPTY Hierarchize({[Time].[Year].members}) ON COLUMNS , \n"
            + "NON EMPTY Hierarchize(Intersect({DrilldownLevel({[Customers].[All Customers]})}, [XL_Row_Dim_0])) ON ROWS \n"
            + "FROM [Sales] \n"
            + "WHERE ([Measures].[Store Sales])",
            "Axis #0:\n"
            + "{[Measures].[Store Sales]}\n"
            + "Axis #1:\n"
            + "{[Time].[Time].[1997]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Customers].[All Customers]}\n"
            + "{[Customer].[Customers].[USA]}\n"
            + "Row #0: 565,238.13\n"
            + "Row #1: 565,238.13\n");
    }

    /**
     * <p>Testcase for <a href="http://jira.pentaho.com/browse/MONDRIAN-668">
     * bug MONDRIAN-668, "Intersect should return any VisualTotals members in
     * right-hand set"</a>.
     */
    public void testVisualTotalsWithNamedSetAndPivotSameAxis() {
        if (!Bug.VisualTotalsFixed) {
            return;
        }
        assertQueryReturns(
            "WITH SET [XL_Row_Dim_0] AS\n"
            + " VisualTotals(\n"
            + "   Distinct(\n"
            + "     Hierarchize(\n"
            + "       {Ascendants([Store].[USA].[CA]),\n"
            + "        Descendants([Store].[USA].[CA])})))\n"
            + "select NON EMPTY \n"
            + "  Hierarchize(\n"
            + "    Intersect(\n"
            + "      {DrilldownLevel({[Store].[USA]})},\n"
            + "      [XL_Row_Dim_0])) ON COLUMNS\n"
            + "from [Sales] "
            + "where [Measures].[Sales count]\n",
            "Axis #0:\n"
            + "{[Measures].[Sales Count]}\n"
            + "Axis #1:\n"
            + "{[Store].[Stores].[USA]}\n"
            + "{[Store].[Stores].[USA].[CA]}\n"
            + "Row #0: 24,442\n"
            + "Row #0: 24,442\n");

        // now with tuples
        assertQueryReturns(
            "WITH SET [XL_Row_Dim_0] AS\n"
            + " VisualTotals(\n"
            + "   Distinct(\n"
            + "     Hierarchize(\n"
            + "       {Ascendants([Store].[USA].[CA]),\n"
            + "        Descendants([Store].[USA].[CA])})))\n"
            + "select NON EMPTY \n"
            + "  Hierarchize(\n"
            + "    Intersect(\n"
            + "     [Marital Status].[M]\n"
            + "     * {DrilldownLevel({[Store].[USA]})}\n"
            + "     * [Gender].[F],\n"
            + "     [Marital Status].[M]\n"
            + "     * [XL_Row_Dim_0]\n"
            + "     * [Gender].[F])) ON COLUMNS\n"
            + "from [Sales] "
            + "where [Measures].[Sales count]\n",
            "Axis #0:\n"
            + "{[Measures].[Sales Count]}\n"
            + "Axis #1:\n"
            + "{[Customer].[Marital Status].[M], [Store].[Stores].[USA], [Customer].[Gender].[F]}\n"
            + "{[Customer].[Marital Status].[M], [Store].[Stores].[USA].[CA], [Customer].[Gender].[F]}\n"
            + "Row #0: 6,054\n"
            + "Row #0: 6,054\n");
    }

    /**
     * <p>Testcase for <a href="http://jira.pentaho.com/browse/MONDRIAN-682">
     * bug MONDRIAN-682, "VisualTotals + Distinct-count measure gives wrong
     * results"</a>.
     */
    public void testVisualTotalsDistinctCountMeasure() {
        if (!Bug.VisualTotalsFixed) {
            return;
        }
        // distinct measure
        assertQueryReturns(
            "WITH SET [XL_Row_Dim_0] AS\n"
            + " VisualTotals(\n"
            + "   Distinct(\n"
            + "     Hierarchize(\n"
            + "       {Ascendants([Store].[USA].[CA]),\n"
            + "        Descendants([Store].[USA].[CA])})))\n"
            + "select NON EMPTY \n"
            + "  Hierarchize(\n"
            + "    Intersect(\n"
            + "      {DrilldownLevel({[Store].[All Stores]})},\n"
            + "      [XL_Row_Dim_0])) ON COLUMNS\n"
            + "from [HR] "
            + "where [Measures].[Number of Employees]\n",
            "Axis #0:\n"
            + "{[Measures].[Number of Employees]}\n"
            + "Axis #1:\n"
            + "{[Store].[Stores].[All Stores]}\n"
            + "{[Store].[Stores].[USA]}\n"
            + "Row #0: 193\n"
            + "Row #0: 193\n");

        // distinct measure
        assertQueryReturns(
            "WITH SET [XL_Row_Dim_0] AS\n"
            + " VisualTotals(\n"
            + "   Distinct(\n"
            + "     Hierarchize(\n"
            + "       {Ascendants([Store].[USA].[CA].[Beverly Hills]),\n"
            + "        Descendants([Store].[USA].[CA].[Beverly Hills]),\n"
            + "        Ascendants([Store].[USA].[CA].[Los Angeles]),\n"
            + "        Descendants([Store].[USA].[CA].[Los Angeles])})))"
            + "select NON EMPTY \n"
            + "  Hierarchize(\n"
            + "    Intersect(\n"
            + "      {DrilldownLevel({[Store].[All Stores]})},\n"
            + "      [XL_Row_Dim_0])) ON COLUMNS\n"
            + "from [HR] "
            + "where [Measures].[Number of Employees]\n",
            "Axis #0:\n"
            + "{[Measures].[Number of Employees]}\n"
            + "Axis #1:\n"
            + "{[Store].[Stores].[All Stores]}\n"
            + "{[Store].[Stores].[USA]}\n"
            + "Row #0: 110\n"
            + "Row #0: 110\n");

        // distinct measure on columns
        assertQueryReturns(
            "WITH SET [XL_Row_Dim_0] AS\n"
            + " VisualTotals(\n"
            + "   Distinct(\n"
            + "     Hierarchize(\n"
            + "       {Ascendants([Store].[USA].[CA]),\n"
            + "        Descendants([Store].[USA].[CA])})))\n"
            + "select {[Measures].[Count], [Measures].[Number of Employees]} on COLUMNS,"
            + " NON EMPTY \n"
            + "  Hierarchize(\n"
            + "    Intersect(\n"
            + "      {DrilldownLevel({[Store].[All Stores]})},\n"
            + "      [XL_Row_Dim_0])) ON ROWS\n"
            + "from [HR] ",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Count]}\n"
            + "{[Measures].[Number of Employees]}\n"
            + "Axis #2:\n"
            + "{[Store].[Stores].[All Stores]}\n"
            + "{[Store].[Stores].[USA]}\n"
            + "Row #0: 2,316\n"
            + "Row #0: 193\n"
            + "Row #1: 2,316\n"
            + "Row #1: 193\n");

        // distinct measure with tuples
        assertQueryReturns(
            "WITH SET [XL_Row_Dim_0] AS\n"
            + " VisualTotals(\n"
            + "   Distinct(\n"
            + "     Hierarchize(\n"
            + "       {Ascendants([Store].[USA].[CA]),\n"
            + "        Descendants([Store].[USA].[CA])})))\n"
            + "select NON EMPTY \n"
            + "  Hierarchize(\n"
            + "    Intersect(\n"
            + "     [Marital Status].[M]\n"
            + "     * {DrilldownLevel({[Store].[USA]})}\n"
            + "     * [Gender].[F],\n"
            + "     [Marital Status].[M]\n"
            + "     * [XL_Row_Dim_0]\n"
            + "     * [Gender].[F])) ON COLUMNS\n"
            + "from [Sales] "
            + "where [Measures].[Customer count]\n",
            "Axis #0:\n"
            + "{[Measures].[Customer Count]}\n"
            + "Axis #1:\n"
            + "{[Customer].[Marital Status].[M], [Store].[Stores].[USA], [Customer].[Gender].[F]}\n"
            + "{[Customer].[Marital Status].[M], [Store].[Stores].[USA].[CA], [Customer].[Gender].[F]}\n"
            + "Row #0: 654\n"
            + "Row #0: 654\n");
    }

    /**
     * <p>Testcase for <a href="http://jira.pentaho.com/browse/MONDRIAN-761">
     * bug MONDRIAN-761, "VisualTotalMember cannot be cast to
     * RolapCubeMember"</a>.
     */
    public void testVisualTotalsClassCast() {
        if (!Bug.VisualTotalsFixed) {
            return;
        }
        assertQueryReturns(
            "WITH  SET [XL_Row_Dim_0] AS\n"
            + " VisualTotals(\n"
            + "   Distinct(\n"
            + "     Hierarchize(\n"
            + "       {Ascendants([Store].[USA].[WA].[Yakima]), \n"
            + "        Descendants([Store].[USA].[WA].[Yakima]), \n"
            + "        Ascendants([Store].[USA].[WA].[Walla Walla]), \n"
            + "        Descendants([Store].[USA].[WA].[Walla Walla]), \n"
            + "        Ascendants([Store].[USA].[WA].[Tacoma]), \n"
            + "        Descendants([Store].[USA].[WA].[Tacoma]), \n"
            + "        Ascendants([Store].[USA].[WA].[Spokane]), \n"
            + "        Descendants([Store].[USA].[WA].[Spokane]), \n"
            + "        Ascendants([Store].[USA].[WA].[Seattle]), \n"
            + "        Descendants([Store].[USA].[WA].[Seattle]), \n"
            + "        Ascendants([Store].[USA].[WA].[Bremerton]), \n"
            + "        Descendants([Store].[USA].[WA].[Bremerton]), \n"
            + "        Ascendants([Store].[USA].[OR]), \n"
            + "        Descendants([Store].[USA].[OR])}))) \n"
            + " SELECT NON EMPTY \n"
            + " Hierarchize(\n"
            + "   Intersect(\n"
            + "     DrilldownMember(\n"
            + "       {{DrilldownMember(\n"
            + "         {{DrilldownMember(\n"
            + "           {{DrilldownLevel(\n"
            + "             {[Store].[All Stores]})}},\n"
            + "           {[Store].[USA]})}},\n"
            + "         {[Store].[USA].[WA]})}},\n"
            + "       {[Store].[USA].[WA].[Bremerton]}),\n"
            + "       [XL_Row_Dim_0]))\n"
            + "DIMENSION PROPERTIES \n"
            + "  PARENT_UNIQUE_NAME, \n"
            + "  [Store].[Store Name].[Store Type],\n"
            + "  [Store].[Store Name].[Store Manager],\n"
            + "  [Store].[Store Name].[Store Sqft],\n"
            + "  [Store].[Store Name].[Grocery Sqft],\n"
            + "  [Store].[Store Name].[Frozen Sqft],\n"
            + "  [Store].[Store Name].[Meat Sqft],\n"
            + "  [Store].[Store Name].[Has coffee bar],\n"
            + "  [Store].[Store Name].[Street address] ON COLUMNS \n"
            + "FROM [HR]\n"
            + "WHERE \n"
            + "  ([Measures].[Number of Employees])\n"
            + "CELL PROPERTIES\n"
            + "  VALUE,\n"
            + "  FORMAT_STRING,\n"
            + "  LANGUAGE,\n"
            + "  BACK_COLOR,\n"
            + "  FORE_COLOR,\n"
            + "  FONT_FLAGS",
            "Axis #0:\n"
            + "{[Measures].[Number of Employees]}\n"
            + "Axis #1:\n"
            + "{[Store].[Stores].[All Stores]}\n"
            + "{[Store].[Stores].[USA]}\n"
            + "{[Store].[Stores].[USA].[OR]}\n"
            + "{[Store].[Stores].[USA].[WA]}\n"
            + "{[Store].[Stores].[USA].[WA].[Bremerton]}\n"
            + "{[Store].[Stores].[USA].[WA].[Bremerton].[Store 3]}\n"
            + "{[Store].[Stores].[USA].[WA].[Seattle]}\n"
            + "{[Store].[Stores].[USA].[WA].[Spokane]}\n"
            + "{[Store].[Stores].[USA].[WA].[Tacoma]}\n"
            + "{[Store].[Stores].[USA].[WA].[Walla Walla]}\n"
            + "{[Store].[Stores].[USA].[WA].[Yakima]}\n"
            + "Row #0: 419\n"
            + "Row #0: 419\n"
            + "Row #0: 136\n"
            + "Row #0: 283\n"
            + "Row #0: 62\n"
            + "Row #0: 62\n"
            + "Row #0: 62\n"
            + "Row #0: 62\n"
            + "Row #0: 74\n"
            + "Row #0: 4\n"
            + "Row #0: 19\n");
    }

    /**
     * <p>Testcase for <a href="http://jira.pentaho.com/browse/MONDRIAN-678">
     * bug MONDRIAN-678, "VisualTotals gives UnsupportedOperationException
     * calling getOrdinal"</a>. Key difference from previous test is that there
     * are multiple hierarchies in Named set.
     */
    public void testVisualTotalsWithNamedSetOfTuples() {
        if (!Bug.VisualTotalsFixed) {
            return;
        }
        assertQueryReturns(
            "WITH SET [XL_Row_Dim_0] AS\n"
            + " VisualTotals(\n"
            + "   Distinct(\n"
            + "     Hierarchize(\n"
            + "       {Ascendants([Customers].[All Customers].[USA].[CA].[Beverly Hills].[Ari Tweten]),\n"
            + "        Descendants([Customers].[All Customers].[USA].[CA].[Beverly Hills].[Ari Tweten]),\n"
            + "        Ascendants([Customers].[All Customers].[Mexico]),\n"
            + "        Descendants([Customers].[All Customers].[Mexico])})))\n"
            + "select NON EMPTY \n"
            + "  Hierarchize(\n"
            + "    Intersect(\n"
            + "      (DrilldownMember(\n"
            + "        {{DrilldownMember(\n"
            + "          {{DrilldownLevel(\n"
            + "            {[Customers].[All Customers]})}},\n"
            + "          {[Customers].[All Customers].[USA]})}},\n"
            + "        {[Customers].[All Customers].[USA].[CA]})),\n"
            + "        [XL_Row_Dim_0])) ON COLUMNS\n"
            + "from [Sales]\n"
            + "where [Measures].[Sales count]\n",
            "Axis #0:\n"
            + "{[Measures].[Sales Count]}\n"
            + "Axis #1:\n"
            + "{[Customer].[Customers].[All Customers]}\n"
            + "{[Customer].[Customers].[USA]}\n"
            + "{[Customer].[Customers].[USA].[CA]}\n"
            + "{[Customer].[Customers].[USA].[CA].[Beverly Hills]}\n"
            + "Row #0: 4\n"
            + "Row #0: 4\n"
            + "Row #0: 4\n"
            + "Row #0: 4\n");
    }

    public void testVisualTotalsLevel() {
        Result result = getTestContext().executeQuery(
            "select {[Measures].[Unit Sales]} on columns,\n"
            + "{[Product].[Products].[All Products],\n"
            + " [Product].[Products].[Food].[Baked Goods].[Bread],\n"
            + " VisualTotals(\n"
            + "    {[Product].[Products].[Food].[Baked Goods].[Bread],\n"
            + "     [Product].[Products].[Food].[Baked Goods].[Bread].[Bagels],\n"
            + "     [Product].[Products].[Food].[Baked Goods].[Bread].[Muffins]},\n"
            + "     \"**Subtotal - *\")} on rows\n"
            + "from [Sales]");
        final List<Position> rowPos = result.getAxes()[1].getPositions();
        final Member member0 = rowPos.get(0).get(0);
        assertEquals("All Products", member0.getName());
        assertEquals("(All)", member0.getLevel().getName());
        final Member member1 = rowPos.get(1).get(0);
        assertEquals("Bread", member1.getName());
        assertEquals("Product Category", member1.getLevel().getName());
        final Member member2 = rowPos.get(2).get(0);
        assertEquals("*Subtotal - Bread", member2.getName());
        assertEquals("Product Category", member2.getLevel().getName());
        final Member member3 = rowPos.get(3).get(0);
        assertEquals("Bagels", member3.getName());
        assertEquals("Product Subcategory", member3.getLevel().getName());
        final Member member4 = rowPos.get(4).get(0);
        assertEquals("Muffins", member4.getName());
        assertEquals("Product Subcategory", member4.getLevel().getName());
    }

    /**
     * Testcase for bug <a href="http://jira.pentaho.com/browse/MONDRIAN-749">
     * MONDRIAN-749, "Cannot use visual totals members in calculations"</a>.
     *
     * <p>The bug is not currently fixed, so it is a negative test case. Row #2
     * cell #1 contains an exception, but should be "**Subtotal - Bread :
     * Product Subcategory".
     */
    public void testVisualTotalsMemberInCalculation() {
        getTestContext().assertQueryReturns(
            "with member [Measures].[Foo] as\n"
            + " [Product].CurrentMember.Name || ' : ' || [Product].Level.Name\n"
            + "select {[Measures].[Unit Sales], [Measures].[Foo]} on columns,\n"
            + "{[Product].[All Products],\n"
            + " [Product].[Products].[Food].[Baked Goods].[Bread],\n"
            + " VisualTotals(\n"
            + "    {[Product].[Products].[Food].[Baked Goods].[Bread],\n"
            + "     [Product].[Products].[Food].[Baked Goods].[Bread].[Bagels],\n"
            + "     [Product].[Products].[Food].[Baked Goods].[Bread].[Muffins]},\n"
            + "     \"**Subtotal - *\")} on rows\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Foo]}\n"
            + "Axis #2:\n"
            + "{[Product].[Products].[All Products]}\n"
            + "{[Product].[Products].[Food].[Baked Goods].[Bread]}\n"
            + "{[Product].[Products].[Food].[Baked Goods].[*Subtotal - Bread]}\n"
            + "{[Product].[Products].[Food].[Baked Goods].[Bread].[Bagels]}\n"
            + "{[Product].[Products].[Food].[Baked Goods].[Bread].[Muffins]}\n"
            + "Row #0: 266,773\n"
            + "Row #0: All Products : (All)\n"
            + "Row #1: 7,870\n"
            + "Row #1: Bread : Product Category\n"
            + "Row #2: 4,312\n"
            + "Row #2: #ERR: mondrian.olap.fun.MondrianEvaluationException: Could not find an aggregator in the current evaluation context\n"
            + "Row #3: 815\n"
            + "Row #3: Bagels : Product Subcategory\n"
            + "Row #4: 3,497\n"
            + "Row #4: Muffins : Product Subcategory\n");
    }

    public void testCalculatedChild() {
        // Construct calculated children with the same name for both [Drink] and
        // [Non-Consumable].  Then, create a metric to select the calculated
        // child based on current product member.
        assertQueryReturns(
            "with\n"
            + " member [Product].[Products].[Drink].[Calculated Child] as '[Product].[Products].[Drink].[Alcoholic Beverages]'\n"
            + " member [Product].[Products].[Non-Consumable].[Calculated Child] as '[Product].[Products].[Non-Consumable].[Carousel]'\n"
            + " member [Measures].[Unit Sales CC] as '([Measures].[Unit Sales],[Product].currentmember.CalculatedChild(\"Calculated Child\"))'\n"
            + " select non empty {[Measures].[Unit Sales CC]} on columns,\n"
            + " non empty {[Product].[Drink], [Product].[Non-Consumable]} on rows\n"
            + " from [Sales]",

            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales CC]}\n"
            + "Axis #2:\n"
            + "{[Product].[Products].[Drink]}\n"
            + "{[Product].[Products].[Non-Consumable]}\n"
            + "Row #0: 6,838\n" // Calculated child for [Drink]
            + "Row #1: 841\n"); // Calculated child for [Non-Consumable]
        Member member = executeSingletonAxis(
            "[Product].[Products].CalculatedChild(\"foobar\")");
        assertEquals(member, null);
    }

    public void testCalculatedChildUsingItem() {
        // Construct calculated children with the same name for both [Drink] and
        // [Non-Consumable].  Then, create a metric to select the first
        // calculated child.
        assertQueryReturns(
            "with\n"
            + " member [Product].[Products].[Drink].[Calculated Child] as '[Product].[Products].[Drink].[Alcoholic Beverages]'\n"
            + " member [Product].[Products].[Non-Consumable].[Calculated Child] as '[Product].[Products].[Non-Consumable].[Carousel]'\n"
            + " member [Measures].[Unit Sales CC] as '([Measures].[Unit Sales],AddCalculatedMembers([Product].currentmember.children).Item(\"Calculated Child\"))'\n"
            + " select non empty {[Measures].[Unit Sales CC]} on columns,\n"
            + " non empty {[Product].[Drink], [Product].[Non-Consumable]} on rows\n"
            + " from [Sales]",

            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales CC]}\n"
            + "Axis #2:\n"
            + "{[Product].[Products].[Drink]}\n"
            + "{[Product].[Products].[Non-Consumable]}\n"
            + "Row #0: 6,838\n"
            // Note: For [Non-Consumable], the calculated child for [Drink] was
            // selected!
            + "Row #1: 6,838\n");
        Member member = executeSingletonAxis(
            "[Product].[Products].CalculatedChild(\"foobar\")");
        assertEquals(member, null);
    }

    public void testCalculatedChildOnMemberWithNoChildren() {
        Member member =
            executeSingletonAxis(
                "[Measures].[Store Sales].CalculatedChild(\"foobar\")");
        assertEquals(member, null);
    }

    public void testCalculatedChildOnNullMember() {
        Member member =
            executeSingletonAxis(
                "[Measures].[Store Sales].parent.CalculatedChild(\"foobar\")");
        assertEquals(member, null);
    }

    public void testCast() {
        // NOTE: Some of these tests fail with 'cannot convert ...', and they
        // probably shouldn't. Feel free to fix the conversion.
        // -- jhyde, 2006/9/3

        // From double to integer.  MONDRIAN-1631
        Cell cell = getTestContext().executeExprRaw("Cast(1.4 As Integer)");
        assertEquals(
            "Cast to Integer resulted in wrong datatype\n"
            + cell.getValue().getClass().toString(),
            Integer.class, cell.getValue().getClass());
        assertEquals(cell.getValue(), 1);

        // From integer
        // To integer (trivial)
        assertExprReturns("0 + Cast(1 + 2 AS Integer)", "3");
        // To String
        assertExprReturns("'' || Cast(1 + 2 AS String)", "3.0");
        // To Boolean
        assertExprReturns("1=1 AND Cast(1 + 2 AS Boolean)", "true");
        assertExprReturns("1=1 AND Cast(1 - 1 AS Boolean)", "false");


        // From boolean
        // To String
        assertExprReturns("'' || Cast((1 = 1 AND 1 = 2) AS String)", "false");

        // This case demonstrates the relative precedence of 'AS' in 'CAST'
        // and 'AS' for creating inline named sets. See also bug MONDRIAN-648.
        Util.discard(Bug.BugMondrian648Fixed);
        assertExprReturns(
            "'xxx' || Cast(1 = 1 AND 1 = 2 AS String)",
            "xxxfalse");

        // To boolean (trivial)
        assertExprReturns(
            "1=1 AND Cast((1 = 1 AND 1 = 2) AS Boolean)",
            "false");

        assertExprReturns(
            "1=1 OR Cast(1 = 1 AND 1 = 2 AS Boolean)",
            "true");

        // From null : should not throw exceptions since RolapResult.executeBody
        // can receive NULL values when the cell value is not loaded yet, so
        // should return null instead.
        // To Integer : Expect to return NULL

        // Expect to return NULL
        assertExprReturns("0 * Cast(NULL AS Integer)", "");

        // To Numeric : Expect to return NULL
        // Expect to return NULL
        assertExprReturns("0 * Cast(NULL AS Numeric)", "");

        // To String : Expect to return "null"
        assertExprReturns("'' || Cast(NULL AS String)", "null");

        // To Boolean : Expect to return NULL, but since FunUtil.BooleanNull
        // does not implement three-valued boolean logic yet, this will return
        // false
        assertExprReturns("1=1 AND Cast(NULL AS Boolean)", "false");

        // Double is not allowed as a type
        assertExprThrows(
            "Cast(1 AS Double)",
            "Unknown type 'Double'; values are NUMERIC, STRING, BOOLEAN");

        // An integer constant is not allowed as a type
        assertExprThrows(
            "Cast(1 AS 5)",
            "Syntax error at line 1, column 11, token '5'");

        assertExprReturns("Cast('tr' || 'ue' AS boolean)", "true");
    }

    /**
     * Testcase for bug <a href="http://jira.pentaho.com/browse/MONDRIAN-524">
     * MONDRIAN-524, "VB functions: expected primitive type, got
     * java.lang.Object"</a>.
     */
    public void testCastBug524() {
        assertExprReturns(
            "Cast(Int([Measures].[Store Sales] / 3600) as String)",
            "157");
    }

    /**
     * Tests {@link mondrian.olap.FunTable#getFunInfoList()}, but more
     * importantly, generates an HTML table of all implemented functions into
     * a file called "functions.html". You can manually include that table
     * in the <a href="{@docRoot}/../mdx.html">MDX
     * specification</a>.
     */
    public void testDumpFunctions() throws IOException {
        final List<FunInfo> funInfoList = new ArrayList<FunInfo>();
        funInfoList.addAll(BuiltinFunTable.instance().getFunInfoList());

        // Add some UDFs.
        funInfoList.add(
            new FunInfo(
                new UdfResolver(
                    new UdfResolver.ClassUdfFactory(
                        CurrentDateMemberExactUdf.class,
                        null))));
        funInfoList.add(
            new FunInfo(
                new UdfResolver(
                    new UdfResolver.ClassUdfFactory(
                        CurrentDateMemberUdf.class,
                        null))));
        funInfoList.add(
            new FunInfo(
                new UdfResolver(
                    new UdfResolver.ClassUdfFactory(
                        CurrentDateStringUdf.class,
                        null))));
        Collections.sort(funInfoList);

        final File file = new File("functions.html");
        final FileOutputStream os = new FileOutputStream(file);
        final PrintWriter pw = new PrintWriter(os);
        pw.println("<table border='1'>");
        pw.println("<tr>");
        pw.println("<td><b>Name</b></td>");
        pw.println("<td><b>Description</b></td>");
        pw.println("</tr>");
        for (FunInfo funInfo : funInfoList) {
            pw.println("<tr>");
            pw.print("  <td valign=top><code>");
            printHtml(pw, funInfo.getName());
            pw.println("</code></td>");
            pw.print("  <td>");
            if (funInfo.getDescription() != null) {
                printHtml(pw, funInfo.getDescription());
            }
            pw.println();
            final String[] signatures = funInfo.getSignatures();
            if (signatures != null) {
                pw.println("    <h1>Syntax</h1>");
                for (int j = 0; j < signatures.length; j++) {
                    if (j > 0) {
                        pw.println("<br/>");
                    }
                    String signature = signatures[j];
                    pw.print("    ");
                    printHtml(pw, signature);
                }
                pw.println();
            }
            pw.println("  </td>");
            pw.println("</tr>");
        }
        pw.println("</table>");
        pw.close();
    }

    public void testComplexOrExpr()
    {
        switch (TestContext.instance().getDialect().getDatabaseProduct()) {
        case INFOBRIGHT:
            // Skip this test on Infobright, because [Promotion Sales] is
            // defined wrong.
            return;
        }

        // make sure all aggregates referenced in the OR expression are
        // processed in a single load request by setting the eval depth to
        // a value smaller than the number of measures
        propSaver.set(propSaver.props.MaxEvalDepth, 3);
        assertQueryReturns(
            "with set [*NATIVE_CJ_SET] as '[Store].[Store Country].members' "
            + "set [*GENERATED_MEMBERS_Measures] as "
            + "    '{[Measures].[Unit Sales], [Measures].[Store Cost], "
            + "    [Measures].[Sales Count], [Measures].[Customer Count], "
            + "    [Measures].[Promotion Sales]}' "
            + "set [*GENERATED_MEMBERS] as "
            + "    'Generate([*NATIVE_CJ_SET], {[Stores].CURRENTMEMBER})' "
            + "member [Store].[Stores].[*SUBTOTAL_MEMBER_SEL~SUM] as 'Sum([*GENERATED_MEMBERS])' "
            + "select [*GENERATED_MEMBERS_Measures] ON COLUMNS, "
            + "NON EMPTY "
            + "    Filter("
            + "        Generate("
            + "        [*NATIVE_CJ_SET], "
            + "        {[Stores].CURRENTMEMBER}), "
            + "        (((((NOT IsEmpty([Measures].[Unit Sales])) OR "
            + "            (NOT IsEmpty([Measures].[Store Cost]))) OR "
            + "            (NOT IsEmpty([Measures].[Sales Count]))) OR "
            + "            (NOT IsEmpty([Measures].[Customer Count]))) OR "
            + "            (NOT IsEmpty([Measures].[Promotion Sales])))) "
            + "on rows "
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Store Cost]}\n"
            + "{[Measures].[Sales Count]}\n"
            + "{[Measures].[Customer Count]}\n"
            + "{[Measures].[Promotion Sales]}\n"
            + "Axis #2:\n"
            + "{[Store].[Stores].[USA]}\n"
            + "Row #0: 266,773\n"
            + "Row #0: 225,627.23\n"
            + "Row #0: 86,837\n"
            + "Row #0: 5,581\n"
            + "Row #0: 151,211.21\n");
    }

    public void testLeftFunctionWithValidArguments() {
        assertQueryReturns(
            "select filter([Stores].MEMBERS,"
            + "Left([Stores].CURRENTMEMBER.Name, 4)=\"Bell\") on 0 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[Stores].[USA].[WA].[Bellingham]}\n"
            + "Row #0: 2,237\n");
    }

    public void testLeftFunctionWithLengthValueZero() {
        assertQueryReturns(
            "select filter([Stores].MEMBERS,"
            + "Left([Stores].CURRENTMEMBER.Name, 0)=\"\" And "
            + "[Stores].CURRENTMEMBER.Name = \"Bellingham\") on 0 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[Stores].[USA].[WA].[Bellingham]}\n"
            + "Row #0: 2,237\n");
    }

    public void testLeftFunctionWithLengthValueEqualToStringLength() {
        assertQueryReturns(
            "select filter([Stores].MEMBERS,"
            + "Left([Stores].CURRENTMEMBER.Name, 10)=\"Bellingham\") "
            + "on 0 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[Stores].[USA].[WA].[Bellingham]}\n"
            + "Row #0: 2,237\n");
    }

    public void testLeftFunctionWithLengthMoreThanStringLength() {
        assertQueryReturns(
            "select filter([Stores].MEMBERS,"
            + "Left([Stores].CURRENTMEMBER.Name, 20)=\"Bellingham\") "
            + "on 0 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[Stores].[USA].[WA].[Bellingham]}\n"
            + "Row #0: 2,237\n");
    }

    public void testLeftFunctionWithZeroLengthString() {
        assertQueryReturns(
            "select filter([Stores].MEMBERS,Left(\"\", 20)=\"\" "
            + "And [Stores].CURRENTMEMBER.Name = \"Bellingham\") "
            + "on 0 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[Stores].[USA].[WA].[Bellingham]}\n"
            + "Row #0: 2,237\n");
    }

    public void testLeftFunctionWithNegativeLength() {
        assertQueryThrows(
            "select filter([Stores].MEMBERS,"
            + "Left([Stores].CURRENTMEMBER.Name, -20)=\"Bellingham\") "
            + "on 0 from sales",
            Util.IBM_JVM
                ? "StringIndexOutOfBoundsException: null"
                : "StringIndexOutOfBoundsException: String index out of range: "
                  + "-20");
    }

    public void testMidFunctionWithValidArguments() {
        assertQueryReturns(
            "select filter([Stores].MEMBERS,"
            + "[Stores].CURRENTMEMBER.Name = \"Bellingham\""
            + "And Mid(\"Bellingham\", 4, 6) = \"lingha\")"
            + "on 0 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[Stores].[USA].[WA].[Bellingham]}\n"
            + "Row #0: 2,237\n");
    }

    public void testMidFunctionWithZeroLengthStringArgument() {
        assertQueryReturns(
            "select filter([Stores].MEMBERS,"
            + "[Stores].CURRENTMEMBER.Name = \"Bellingham\""
            + "And Mid(\"\", 4, 6) = \"\")"
            + "on 0 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[Stores].[USA].[WA].[Bellingham]}\n"
            + "Row #0: 2,237\n");
    }

    public void testMidFunctionWithLengthArgumentLargerThanStringLength() {
        assertQueryReturns(
            "select filter([Stores].MEMBERS,"
            + "[Stores].CURRENTMEMBER.Name = \"Bellingham\""
            + "And Mid(\"Bellingham\", 4, 20) = \"lingham\")"
            + "on 0 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[Stores].[USA].[WA].[Bellingham]}\n"
            + "Row #0: 2,237\n");
    }

    public void testMidFunctionWithStartIndexGreaterThanStringLength() {
        assertQueryReturns(
            "select filter([Stores].MEMBERS,"
            + "[Stores].CURRENTMEMBER.Name = \"Bellingham\""
            + "And Mid(\"Bellingham\", 20, 2) = \"\")"
            + "on 0 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[Stores].[USA].[WA].[Bellingham]}\n"
            + "Row #0: 2,237\n");
    }

    public void testMidFunctionWithStartIndexZeroFails() {
        // Note: SSAS 2005 treats start<=0 as 1, therefore gives different
        // result for this query. We favor the VBA spec over SSAS 2005.
        if (Bug.Ssas2005Compatible) {
            assertQueryReturns(
                "select filter([Stores].MEMBERS,"
                + "[Stores].CURRENTMEMBER.Name = \"Bellingham\""
                + "And Mid(\"Bellingham\", 0, 2) = \"Be\")"
                + "on 0 from sales",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Store].[Stores].[USA].[WA].[Bellingham]}\n"
                + "Row #0: 2,237\n");
        } else {
            assertQueryThrows(
                "select filter([Stores].MEMBERS,"
                + "[Stores].CURRENTMEMBER.Name = \"Bellingham\""
                + "And Mid(\"Bellingham\", 0, 2) = \"Be\")"
                + "on 0 from sales",
                "Invalid parameter. Start parameter of Mid function must be "
                + "positive");
        }
    }

    public void testMidFunctionWithStartIndexOne() {
        assertQueryReturns(
            "select filter([Stores].MEMBERS,"
            + "[Stores].CURRENTMEMBER.Name = \"Bellingham\""
            + "And Mid(\"Bellingham\", 1, 2) = \"Be\")"
            + "on 0 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[Stores].[USA].[WA].[Bellingham]}\n"
            + "Row #0: 2,237\n");
    }

    public void testMidFunctionWithNegativeStartIndex() {
        assertQueryThrows(
            "select filter([Stores].MEMBERS,"
            + "[Stores].CURRENTMEMBER.Name = \"Bellingham\""
            + "And Mid(\"Bellingham\", -20, 2) = \"\")"
            + "on 0 from sales",
            "Invalid parameter. "
            + "Start parameter of Mid function must be positive");
    }

    public void testMidFunctionWithNegativeLength() {
        assertQueryThrows(
            "select filter([Stores].MEMBERS,"
            + "[Stores].CURRENTMEMBER.Name = \"Bellingham\""
            + "And Mid(\"Bellingham\", 2, -2) = \"\")"
            + "on 0 from sales",
            "Invalid parameter. "
            + "Length parameter of Mid function must be non-negative");
    }

    public void testMidFunctionWithoutLength() {
        assertQueryReturns(
            "select filter([Stores].MEMBERS,"
            + "[Stores].CURRENTMEMBER.Name = \"Bellingham\""
            + "And Mid(\"Bellingham\", 2) = \"ellingham\")"
            + "on 0 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[Stores].[USA].[WA].[Bellingham]}\n"
            + "Row #0: 2,237\n");
    }

    public void testLenFunctionWithNonEmptyString() {
        assertQueryReturns(
            "select filter([Stores].MEMBERS, "
            + "Len([Stores].CURRENTMEMBER.Name) = 3) on 0 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[Stores].[USA]}\n"
            + "Row #0: 266,773\n");
    }

    public void testLenFunctionWithAnEmptyString() {
        assertQueryReturns(
            "select filter([Stores].MEMBERS,Len(\"\")=0 "
            + "And [Stores].CURRENTMEMBER.Name = \"Bellingham\") "
            + "on 0 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[Stores].[USA].[WA].[Bellingham]}\n"
            + "Row #0: 2,237\n");
    }

    public void testLenFunctionWithNullString() {
        // SSAS2005 returns 0
        assertQueryReturns(
            "with member [Measures].[Foo] as ' NULL '\n"
            + " member [Measures].[Bar] as ' len([Measures].[Foo]) '\n"
            + "select [Measures].[Bar] on 0\n"
            + "from [Warehouse and Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Bar]}\n"
            + "Row #0: 0\n");
        // same, but inline
        assertExprReturns("len(null)", 0, 0);
    }

    public void testUCaseWithNonEmptyString() {
        assertQueryReturns(
            "select filter([Stores].MEMBERS, "
            + " UCase([Stores].CURRENTMEMBER.Name) = \"BELLINGHAM\") "
            + "on 0 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[Stores].[USA].[WA].[Bellingham]}\n"
            + "Row #0: 2,237\n");
    }

    public void testUCaseWithEmptyString() {
        assertQueryReturns(
            "select filter([Stores].MEMBERS, "
            + " UCase(\"\") = \"\" "
            + "And [Stores].CURRENTMEMBER.Name = \"Bellingham\") "
            + "on 0 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[Stores].[USA].[WA].[Bellingham]}\n"
            + "Row #0: 2,237\n");
    }

    public void testInStrFunctionWithValidArguments() {
        assertQueryReturns(
            "select filter([Stores].MEMBERS,InStr(\"Bellingham\", \"ingha\")=5 "
            + "And [Stores].CURRENTMEMBER.Name = \"Bellingham\") "
            + "on 0 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[Stores].[USA].[WA].[Bellingham]}\n"
            + "Row #0: 2,237\n");
    }

    public void
        testIifFWithBooleanBooleanAndNumericParameterForReturningTruePart()
    {
        assertQueryReturns(
            "SELECT Filter(Stores.allmembers, "
            + "iif(measures.profit < 400000,"
            + "[stores].currentMember.NAME = \"USA\", 0)) on 0 FROM SALES",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[Stores].[USA]}\n"
            + "Row #0: 266,773\n");
    }

    public void
        testIifWithBooleanBooleanAndNumericParameterForReturningFalsePart()
    {
        assertQueryReturns(
            "SELECT Filter([Store].[USA].[CA].[Beverly Hills].children, "
            + "iif(measures.profit > 400000,"
            + "[stores].currentMember.NAME = \"USA\", 1)) on 0 FROM SALES",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[Stores].[USA].[CA].[Beverly Hills].[Store 6]}\n"
            + "Row #0: 21,333\n");
    }

    public void testIIFWithBooleanBooleanAndNumericParameterForReturningZero() {
        assertQueryReturns(
            "SELECT Filter(Stores.allmembers, "
            + "iif(measures.profit > 400000,"
            + "[stores].currentMember.NAME = \"USA\", 0)) on 0 FROM SALES",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n");
    }

    public void testInStrFunctionWithEmptyString1() {
        assertQueryReturns(
            "select filter([Stores].MEMBERS,InStr(\"\", \"ingha\")=0 "
            + "And [Stores].CURRENTMEMBER.Name = \"Bellingham\") "
            + "on 0 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[Stores].[USA].[WA].[Bellingham]}\n"
            + "Row #0: 2,237\n");
    }

    public void testInStrFunctionWithEmptyString2() {
        assertQueryReturns(
            "select filter([Stores].MEMBERS,InStr(\"Bellingham\", \"\")=1 "
            + "And [Stores].CURRENTMEMBER.Name = \"Bellingham\") "
            + "on 0 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[Stores].[USA].[WA].[Bellingham]}\n"
            + "Row #0: 2,237\n");
    }

    public void testGetCaptionUsingMemberDotCaption() {
        assertQueryReturns(
            "SELECT Filter(Stores.allmembers, "
            + "[Stores].CURRENTMEMBER.caption = \"USA\") on 0 FROM SALES",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[Stores].[USA]}\n"
            + "Row #0: 266,773\n");
    }

    private static void printHtml(PrintWriter pw, String s) {
        final String escaped = StringEscaper.htmlEscaper.escapeString(s);
        pw.print(escaped);
    }

    public void testCache() {
        // test various data types: integer, string, member, set, tuple
        assertExprReturns("Cache(1 + 2)", "3");
        assertExprReturns("Cache('foo' || 'bar')", "foobar");
        assertAxisReturns(
            "[Gender].Children",
            "[Customer].[Gender].[F]\n"
            + "[Customer].[Gender].[M]");
        assertAxisReturns(
            "([Gender].[M], [Marital Status].[S].PrevMember)",
            "{[Customer].[Gender].[M], [Customer].[Marital Status].[M]}");

        // inside another expression
        assertAxisReturns(
            "Order(Cache([Gender].Children), Cache(([Measures].[Unit Sales], [Time].[1997].[Q1])), BDESC)",
            "[Customer].[Gender].[M]\n"
            + "[Customer].[Gender].[F]");

        // doesn't work with multiple args
        assertExprThrows(
            "Cache(1, 2)",
            "No function matches signature 'Cache(<Numeric Expression>, <Numeric Expression>)'");
    }

    // The following methods test VBA functions. They don't test all of them,
    // because the raw methods are tested in VbaTest, but they test the core
    // functionalities like error handling and operator overloading.

    public void testVbaBasic() {
        // Exp is a simple function: one arg.
        assertExprReturns("exp(0)", "1");
        assertExprReturns("exp(1)", Math.E, 0.00000001);
        assertExprReturns("exp(-2)", 1d / (Math.E * Math.E), 0.00000001);

        // If any arg is null, result is null.
        assertExprReturns("exp(cast(null as numeric))", "");
    }

    // Test a VBA function with variable number of args.
    public void testVbaOverloading() {
        assertExprReturns("replace('xyzxyz', 'xy', 'a')", "azaz");
        assertExprReturns("replace('xyzxyz', 'xy', 'a', 2)", "xyzaz");
        assertExprReturns("replace('xyzxyz', 'xy', 'a', 1, 1)", "azxyz");
    }

    // Test VBA exception handling
    public void testVbaExceptions() {
        assertExprThrows(
            "right(\"abc\", -4)",
            Util.IBM_JVM
                ? "StringIndexOutOfBoundsException: null"
                : "StringIndexOutOfBoundsException: "
                  + "String index out of range: -4");
    }

    public void testVbaDateTime() {
        // function which returns date
        assertExprReturns(
            "Format(DateSerial(2006, 4, 29), \"Long Date\")",
            "Saturday, April 29, 2006");
        // function with date parameter
        assertExprReturns("Year(DateSerial(2006, 4, 29))", "2,006");
    }

    public void testExcelPi() {
        // The PI function is defined in the Excel class.
        assertExprReturns("Pi()", "3");
    }

    public void testExcelPower() {
        assertExprReturns("Power(8, 0.333333)", 2.0, 0.01);
        assertExprReturns("Power(-2, 0.5)", Double.NaN, 0.001);
    }

    // Comment from the bug: the reason for this is that in AbstractExpCompiler
    // in the compileInteger method we are casting an IntegerCalc into a
    // DoubleCalc and there is no check for IntegerCalc in the NumericType
    // conditional path.
    public void testBug1881739() {
        assertExprReturns("LEFT(\"TEST\", LEN(\"TEST\"))", "TEST");
    }

    /**
     * Testcase for bug <a href="http://jira.pentaho.com/browse/MONDRIAN-296">
     * MONDRIAN-296, "Cube getTimeDimension use when Cube has no Time
     * dimension"</a>.
     */
    public void testCubeTimeDimensionFails() {
        if (!Bug.CubeStoreFeature) {
            return;
        }
        assertQueryThrows(
            "select LastPeriods(1) on columns from [Store]",
            "'LastPeriods', no time dimension");
        assertQueryThrows(
            "select OpeningPeriod() on columns from [Store]",
            "'OpeningPeriod', no time dimension");
        assertQueryThrows(
            "select OpeningPeriod([Store Type]) on columns from [Store]",
            "'OpeningPeriod', no time dimension");
        assertQueryThrows(
            "select ClosingPeriod() on columns from [Store]",
            "'ClosingPeriod', no time dimension");
        assertQueryThrows(
            "select ClosingPeriod([Store Type]) on columns from [Store]",
            "'ClosingPeriod', no time dimension");
        assertQueryThrows(
            "select ParallelPeriod() on columns from [Store]",
            "'ParallelPeriod', no time dimension");
        assertQueryThrows(
            "select PeriodsToDate() on columns from [Store]",
            "'PeriodsToDate', no time dimension");
        assertQueryThrows(
            "select Mtd() on columns from [Store]",
            "'Mtd', no time dimension");
    }

    public void testFilterEmpty() {
        // Unlike "Descendants(<set>, ...)", we do not need to know the precise
        // type of the set, therefore it is OK if the set is empty.
        assertAxisReturns(
            "Filter({}, 1=0)",
            "");
        assertAxisReturns(
            "Filter({[Time].[Time].Children}, 1=0)",
            "");
    }

    public void testFilterCalcSlicer() {
        assertQueryReturns(
            "with member [Time].[Time].[Date Range] as \n"
            + "'Aggregate({[Time].[1997].[Q1]:[Time].[1997].[Q3]})'\n"
            + "select\n"
            + "{[Measures].[Unit Sales],[Measures].[Store Cost],\n"
            + "[Measures].[Store Sales]} ON columns,\n"
            + "NON EMPTY Filter ([Store].[Store State].members,\n"
            + "[Measures].[Store Cost] > 75000) ON rows\n"
            + "from [Sales] where [Time].[Date Range]",
            "Axis #0:\n"
            + "{[Time].[Time].[Date Range]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Store Cost]}\n"
            + "{[Measures].[Store Sales]}\n"
            + "Axis #2:\n"
            + "{[Store].[Stores].[USA].[WA]}\n"
            + "Row #0: 90,131\n"
            + "Row #0: 76,151.59\n"
            + "Row #0: 190,776.88\n");
        assertQueryReturns(
            "with member [Time].[Time].[Date Range] as \n"
            + "'Aggregate({[Time].[1997].[Q1]:[Time].[1997].[Q3]})'\n"
            + "select\n"
            + "{[Measures].[Unit Sales],[Measures].[Store Cost],\n"
            + "[Measures].[Store Sales]} ON columns,\n"
            + "NON EMPTY Order (Filter ([Store].[Store State].members,\n"
            + "[Measures].[Store Cost] > 100),[Measures].[Store Cost], DESC) ON rows\n"
            + "from [Sales] where [Time].[Date Range]",
            "Axis #0:\n"
            + "{[Time].[Time].[Date Range]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Store Cost]}\n"
            + "{[Measures].[Store Sales]}\n"
            + "Axis #2:\n"
            + "{[Store].[Stores].[USA].[WA]}\n"
            + "{[Store].[Stores].[USA].[CA]}\n"
            + "{[Store].[Stores].[USA].[OR]}\n"
            + "Row #0: 90,131\n"
            + "Row #0: 76,151.59\n"
            + "Row #0: 190,776.88\n"
            + "Row #1: 53,312\n"
            + "Row #1: 45,435.93\n"
            + "Row #1: 113,966.00\n"
            + "Row #2: 51,306\n"
            + "Row #2: 43,033.82\n"
            + "Row #2: 107,823.63\n");
    }

    public void testExistsMembersAll() {
        assertQueryReturns(
            "select exists(\n"
            + "  {[Customers].[All Customers],\n"
            + "   [Customers].[Country].Members,\n"
            + "   [Customers].[State Province].[CA],\n"
            + "   [Customers].[Canada].[BC].[Richmond]},\n"
            + "  {[Customers].[All Customers]})\n"
            + "on 0 from Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Customer].[Customers].[All Customers]}\n"
            + "{[Customer].[Customers].[Canada]}\n"
            + "{[Customer].[Customers].[Mexico]}\n"
            + "{[Customer].[Customers].[USA]}\n"
            + "{[Customer].[Customers].[USA].[CA]}\n"
            + "{[Customer].[Customers].[Canada].[BC].[Richmond]}\n"
            + "Row #0: 266,773\n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: 266,773\n"
            + "Row #0: 74,748\n"
            + "Row #0: \n");
    }

    public void testExistsMembersLevel2() {
        assertQueryReturns(
            "select exists(\n"
            + "  {[Customers].[All Customers],\n"
            + "   [Customers].[Country].Members,\n"
            + "   [Customers].[State Province].[CA],\n"
            + "   [Customers].[Canada].[BC].[Richmond]},\n"
            + "  {[Customers].[Country].[USA]})\n"
            + "on 0 from Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Customer].[Customers].[All Customers]}\n"
            + "{[Customer].[Customers].[USA]}\n"
            + "{[Customer].[Customers].[USA].[CA]}\n"
            + "Row #0: 266,773\n"
            + "Row #0: 266,773\n"
            + "Row #0: 74,748\n");
    }

    public void testExistsWithImplicitAllMember() {
        // the tuple in the second arg in this case should implicitly
        // contain [Customers].[All Customers], so the whole tuple list
        // from the first arg should be returned.
        assertQueryReturns(
            "select non empty exists(\n"
            + "  {[Customers].[All Customers],\n"
            + "   [Customers].[All Customers].Children,\n"
            + "   [Customers].[State Province].Members},\n"
            + "  {[Product].Members})\n"
            + "on 0 from Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Customer].[Customers].[All Customers]}\n"
            + "{[Customer].[Customers].[USA]}\n"
            + "{[Customer].[Customers].[USA].[CA]}\n"
            + "{[Customer].[Customers].[USA].[OR]}\n"
            + "{[Customer].[Customers].[USA].[WA]}\n"
            + "Row #0: 266,773\n"
            + "Row #0: 266,773\n"
            + "Row #0: 74,748\n"
            + "Row #0: 67,659\n"
            + "Row #0: 124,366\n");

        assertQueryReturns(
            "select exists( "
            + "[Customers].[USA].[CA], (Store.[USA], Gender.[F])) "
            + "on 0 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Customer].[Customers].[USA].[CA]}\n"
            + "Row #0: 74,748\n");
    }

    public void testExistsWithMultipleHierarchies() {
        // tests queries w/ a multi-hierarchy dim in either or both args.
        assertQueryReturns(
            "select exists( "
            + "crossjoin( time.[1997], {[Time.Weekly].[1997].[16]}), "
            + " { Gender.F } ) on 0 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time].[Time].[1997], [Time].[Weekly].[1997].[16]}\n"
            + "Row #0: 3,839\n");

        assertQueryReturns(
            "select exists( "
            + "time.[1997].[Q1], {[Time.Weekly].[1997].[4]}) "
            + " on 0 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time].[Time].[1997].[Q1]}\n"
            + "Row #0: 66,291\n");

        assertQueryReturns(
            "select exists( "
            + "{ Gender.F }, "
            + "crossjoin( time.[1997], {[Time.Weekly].[1997].[16]})  ) "
            + "on 0 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Customer].[Gender].[F]}\n"
            + "Row #0: 131,558\n");

        assertQueryReturns(
            "select exists( "
            + "{ time.[1998] }, "
            + "crossjoin( time.[1997], {[Time.Weekly].[1997].[16]})  ) "
            + "on 0 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n");
    }


    public void testExistsWithDefaultNonAllMember() {
        // default mem for Time is 1997

        // non-all default on right side.
        assertQueryReturns(
            "select exists( [Time].[1998].[Q1], Gender.[All Gender]) on 0 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n");

        // switching to an explicit member on the hierarchy chain should return
        // 1998.Q1
        assertQueryReturns(
            "select exists( [Time].[1998].[Q1], ([Time].[1998], Gender.[All Gender])) on 0 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time].[Time].[1998].[Q1]}\n"
            + "Row #0: \n");


        // non-all default on left side
        assertQueryReturns(
            "select exists( "
            + "Gender.[All Gender], (Gender.[F], [Time].[1998].[Q1])) "
            + "on 0 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n");

        assertQueryReturns(
            "select exists( "
            + "(Time.[1998].[Q1].[1], Gender.[All Gender]), (Gender.[F], [Time].[1998].[Q1])) "
            + "on 0 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time].[Time].[1998].[Q1].[1], [Customer].[Gender].[All Gender]}\n"
            + "Row #0: \n");
    }


    public void testExistsMembers2Hierarchies() {
        assertQueryReturns(
            "select exists(\n"
            + "  {[Customers].[All Customers],\n"
            + "   [Customers].[All Customers].Children,\n"
            + "   [Customers].[State Province].Members,\n"
            + "   [Customers].[Country].[Canada],\n"
            + "   [Customers].[Country].[Mexico]},\n"
            + "  {[Customers].[Country].[USA],\n"
            + "   [Customers].[State Province].[Veracruz]})\n"
            + "on 0 from Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Customer].[Customers].[All Customers]}\n"
            + "{[Customer].[Customers].[Mexico]}\n"
            + "{[Customer].[Customers].[USA]}\n"
            + "{[Customer].[Customers].[Mexico].[Veracruz]}\n"
            + "{[Customer].[Customers].[USA].[CA]}\n"
            + "{[Customer].[Customers].[USA].[OR]}\n"
            + "{[Customer].[Customers].[USA].[WA]}\n"
            + "{[Customer].[Customers].[Mexico]}\n"
            + "Row #0: 266,773\n"
            + "Row #0: \n"
            + "Row #0: 266,773\n"
            + "Row #0: \n"
            + "Row #0: 74,748\n"
            + "Row #0: 67,659\n"
            + "Row #0: 124,366\n"
            + "Row #0: \n");
    }

    public void testExistsTuplesAll() {
        assertQueryReturns(
            "select exists(\n"
            + "  crossjoin({[Product].[All Products]},{[Customers].[All Customers]}),\n"
            + "  {[Customers].[All Customers]})\n"
            + "on 0 from Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Product].[Products].[All Products], [Customer].[Customers].[All Customers]}\n"
            + "Row #0: 266,773\n");
    }

    public void testExistsTuplesLevel2() {
        assertQueryReturns(
            "select exists(\n"
            + "  crossjoin({[Product].[All Products]},{[Customers].[All Customers].Children}),\n"
            + "  {[Customers].[All Customers].[USA]})\n"
            + "on 0 from Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Product].[Products].[All Products], [Customer].[Customers].[USA]}\n"
            + "Row #0: 266,773\n");
    }

    public void testExistsTuplesLevel23() {
        assertQueryReturns(
            "select exists(\n"
            + "  crossjoin({[Customers].[State Province].Members}, {[Product].[All Products]}),\n"
            + "  {[Customers].[All Customers].[USA]})\n"
            + "on 0 from Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Customer].[Customers].[USA].[CA], [Product].[Products].[All Products]}\n"
            + "{[Customer].[Customers].[USA].[OR], [Product].[Products].[All Products]}\n"
            + "{[Customer].[Customers].[USA].[WA], [Product].[Products].[All Products]}\n"
            + "Row #0: 74,748\n"
            + "Row #0: 67,659\n"
            + "Row #0: 124,366\n");
    }

    public void testExistsTuples2Dim() {
        assertQueryReturns(
            "select exists(\n"
            + "  crossjoin({[Customers].[State Province].Members}, {[Product].[Product Family].Members}),\n"
            + "  {([Product].[Product Department].[Dairy],[Customers].[All Customers].[USA])})\n"
            + "on 0 from Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Customer].[Customers].[USA].[CA], [Product].[Products].[Drink]}\n"
            + "{[Customer].[Customers].[USA].[OR], [Product].[Products].[Drink]}\n"
            + "{[Customer].[Customers].[USA].[WA], [Product].[Products].[Drink]}\n"
            + "Row #0: 7,102\n"
            + "Row #0: 6,106\n"
            + "Row #0: 11,389\n");
    }

    public void testExistsTuplesDiffDim() {
        assertQueryReturns(
            "select exists(\n"
            + "  crossjoin(\n"
            + "    crossjoin({[Customers].[State Province].Members},\n"
            + "              {[Time].[Year].[1997]}), \n"
            + "    {[Product].[Product Family].Members}),\n"
            + "  {([Product].[Product Department].[Dairy],\n"
            + "    [Promotions].[All Promotions], \n"
            + "    [Customers].[All Customers].[USA])})\n"
            + "on 0 from Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Customer].[Customers].[USA].[CA], [Time].[Time].[1997], [Product].[Products].[Drink]}\n"
            + "{[Customer].[Customers].[USA].[OR], [Time].[Time].[1997], [Product].[Products].[Drink]}\n"
            + "{[Customer].[Customers].[USA].[WA], [Time].[Time].[1997], [Product].[Products].[Drink]}\n"
            + "Row #0: 7,102\n"
            + "Row #0: 6,106\n"
            + "Row #0: 11,389\n");
    }

    /**
     * Executes a query that has a complex parse tree. Goal is to find
     * algorithmic complexity bugs in the validator which would make the query
     * run extremely slowly.
     */
    public void testComplexQuery() {
        final String expected =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Gender].[All Gender]}\n"
            + "{[Customer].[Gender].[F]}\n"
            + "{[Customer].[Gender].[M]}\n"
            + "Row #0: 266,773\n"
            + "Row #1: 131,558\n"
            + "Row #2: 135,215\n";

        // hand written case
        assertQueryReturns(
            "select\n"
            + "   [Measures].[Unit Sales] on 0,\n"
            + "   Distinct({\n"
            + "     [Gender],\n"
            + "     Tail(\n"
            + "       Head({\n"
            + "         [Gender],\n"
            + "         [Gender].[F],\n"
            + "         [Gender].[M]},\n"
            + "         2),\n"
            + "       1),\n"
            + "     Tail(\n"
            + "       Head({\n"
            + "         [Gender],\n"
            + "         [Gender].[F],\n"
            + "         [Gender].[M]},\n"
            + "         2),\n"
            + "       1),\n"
            + "     [Gender].[M]}) on 1\n"
            + "from [Sales]", expected);

        // generated equivalent
        StringBuilder buf = new StringBuilder();
        buf.append(
            "select\n"
            + "   [Measures].[Unit Sales] on 0,\n");
        generateComplex(buf, "   ", 0, 7, 3);
        buf.append(
            " on 1\n"
            + "from [Sales]");
        if (false) {
            System.out.println(buf.toString().length() + ": " + buf.toString());
        }
        assertQueryReturns(buf.toString(), expected);
    }

    /**
     * Recursive routine to generate a complex MDX expression.
     *
     * @param buf String builder
     * @param indent Indent
     * @param depth Current depth
     * @param depthLimit Max recursion depth
     * @param breadth Number of iterations at each depth
     */
    private void generateComplex(
        StringBuilder buf,
        String indent,
        int depth,
        int depthLimit,
        int breadth)
    {
        buf.append(indent + "Distinct({\n");
        buf.append(indent + "  [Gender],\n");
        for (int i = 0; i < breadth; i++) {
            if (depth < depthLimit) {
                buf.append(indent + "  Tail(\n");
                buf.append(indent + "    Head({\n");
                generateComplex(
                    buf,
                    indent + "      ",
                    depth + 1,
                    depthLimit,
                    breadth);
                buf.append("},\n");
                buf.append(indent + "      2),\n");
                buf.append(indent + "    1),\n");
            } else {
                buf.append(indent + "  [Gender].[F],\n");
            }
        }
        buf.append(indent + "  [Gender].[M]})");
    }

    /**
     * Testcase for bug <a href="http://jira.pentaho.com/browse/MONDRIAN-1050">
     * MONDRIAN-1050, "MDX Order function fails when using DateTime expression
     * for ordering"</a>.
     */
    public void testDateParameter() throws Exception {
        executeQuery(
            "SELECT {[Measures].[Unit Sales]} ON COLUMNS, Order([Gender].Members, Now(), ASC) ON ROWS FROM [Sales]");
    }

    /**
     * Testcase for bug <a href="http://jira.pentaho.com/browse/MONDRIAN-1043">
     * MONDRIAN-1043, "Hierarchize with Except sort set members differently than
     * in Mondrian 3.2.1"</a>.
     *
     * <p>This test makes sure that
     * Hierarchize and Except can be used within each other and that the
     * sort order is maintained.</p>
     */
    public void testHierarchizeExcept() throws Exception {
        final String[] mdxA =
            new String[] {
                "SELECT {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS, Hierarchize(Except({[Customers].[USA].Children, [Customers].[USA].[CA].Children}, [Customers].[USA].[CA])) ON ROWS FROM [Sales]",
                "SELECT {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS, Except(Hierarchize({[Customers].[USA].Children, [Customers].[USA].[CA].Children}), [Customers].[USA].[CA]) ON ROWS FROM [Sales] "
        };
        for (String mdx : mdxA) {
            assertQueryReturns(
                mdx,
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Measures].[Unit Sales]}\n"
                + "{[Measures].[Store Sales]}\n"
                + "Axis #2:\n"
                + "{[Customer].[Customers].[USA].[CA].[Altadena]}\n"
                + "{[Customer].[Customers].[USA].[CA].[Arcadia]}\n"
                + "{[Customer].[Customers].[USA].[CA].[Bellflower]}\n"
                + "{[Customer].[Customers].[USA].[CA].[Berkeley]}\n"
                + "{[Customer].[Customers].[USA].[CA].[Beverly Hills]}\n"
                + "{[Customer].[Customers].[USA].[CA].[Burbank]}\n"
                + "{[Customer].[Customers].[USA].[CA].[Burlingame]}\n"
                + "{[Customer].[Customers].[USA].[CA].[Chula Vista]}\n"
                + "{[Customer].[Customers].[USA].[CA].[Colma]}\n"
                + "{[Customer].[Customers].[USA].[CA].[Concord]}\n"
                + "{[Customer].[Customers].[USA].[CA].[Coronado]}\n"
                + "{[Customer].[Customers].[USA].[CA].[Daly City]}\n"
                + "{[Customer].[Customers].[USA].[CA].[Downey]}\n"
                + "{[Customer].[Customers].[USA].[CA].[El Cajon]}\n"
                + "{[Customer].[Customers].[USA].[CA].[Fremont]}\n"
                + "{[Customer].[Customers].[USA].[CA].[Glendale]}\n"
                + "{[Customer].[Customers].[USA].[CA].[Grossmont]}\n"
                + "{[Customer].[Customers].[USA].[CA].[Imperial Beach]}\n"
                + "{[Customer].[Customers].[USA].[CA].[La Jolla]}\n"
                + "{[Customer].[Customers].[USA].[CA].[La Mesa]}\n"
                + "{[Customer].[Customers].[USA].[CA].[Lakewood]}\n"
                + "{[Customer].[Customers].[USA].[CA].[Lemon Grove]}\n"
                + "{[Customer].[Customers].[USA].[CA].[Lincoln Acres]}\n"
                + "{[Customer].[Customers].[USA].[CA].[Long Beach]}\n"
                + "{[Customer].[Customers].[USA].[CA].[Los Angeles]}\n"
                + "{[Customer].[Customers].[USA].[CA].[Mill Valley]}\n"
                + "{[Customer].[Customers].[USA].[CA].[National City]}\n"
                + "{[Customer].[Customers].[USA].[CA].[Newport Beach]}\n"
                + "{[Customer].[Customers].[USA].[CA].[Novato]}\n"
                + "{[Customer].[Customers].[USA].[CA].[Oakland]}\n"
                + "{[Customer].[Customers].[USA].[CA].[Palo Alto]}\n"
                + "{[Customer].[Customers].[USA].[CA].[Pomona]}\n"
                + "{[Customer].[Customers].[USA].[CA].[Redwood City]}\n"
                + "{[Customer].[Customers].[USA].[CA].[Richmond]}\n"
                + "{[Customer].[Customers].[USA].[CA].[San Carlos]}\n"
                + "{[Customer].[Customers].[USA].[CA].[San Diego]}\n"
                + "{[Customer].[Customers].[USA].[CA].[San Francisco]}\n"
                + "{[Customer].[Customers].[USA].[CA].[San Gabriel]}\n"
                + "{[Customer].[Customers].[USA].[CA].[San Jose]}\n"
                + "{[Customer].[Customers].[USA].[CA].[Santa Cruz]}\n"
                + "{[Customer].[Customers].[USA].[CA].[Santa Monica]}\n"
                + "{[Customer].[Customers].[USA].[CA].[Spring Valley]}\n"
                + "{[Customer].[Customers].[USA].[CA].[Torrance]}\n"
                + "{[Customer].[Customers].[USA].[CA].[West Covina]}\n"
                + "{[Customer].[Customers].[USA].[CA].[Woodland Hills]}\n"
                + "{[Customer].[Customers].[USA].[OR]}\n"
                + "{[Customer].[Customers].[USA].[WA]}\n"
                + "Row #0: 2,574\n"
                + "Row #0: 5,585.59\n"
                + "Row #1: 2,440\n"
                + "Row #1: 5,136.59\n"
                + "Row #2: 3,106\n"
                + "Row #2: 6,633.97\n"
                + "Row #3: 136\n"
                + "Row #3: 320.17\n"
                + "Row #4: 2,907\n"
                + "Row #4: 6,194.37\n"
                + "Row #5: 3,086\n"
                + "Row #5: 6,577.33\n"
                + "Row #6: 198\n"
                + "Row #6: 407.38\n"
                + "Row #7: 2,999\n"
                + "Row #7: 6,284.30\n"
                + "Row #8: 129\n"
                + "Row #8: 287.78\n"
                + "Row #9: 105\n"
                + "Row #9: 219.77\n"
                + "Row #10: 2,391\n"
                + "Row #10: 5,051.15\n"
                + "Row #11: 129\n"
                + "Row #11: 271.60\n"
                + "Row #12: 3,440\n"
                + "Row #12: 7,367.06\n"
                + "Row #13: 2,543\n"
                + "Row #13: 5,460.42\n"
                + "Row #14: 163\n"
                + "Row #14: 350.22\n"
                + "Row #15: 3,284\n"
                + "Row #15: 7,082.91\n"
                + "Row #16: 2,131\n"
                + "Row #16: 4,458.60\n"
                + "Row #17: 1,616\n"
                + "Row #17: 3,409.34\n"
                + "Row #18: 1,938\n"
                + "Row #18: 4,081.37\n"
                + "Row #19: 1,834\n"
                + "Row #19: 3,908.26\n"
                + "Row #20: 2,487\n"
                + "Row #20: 5,174.12\n"
                + "Row #21: 2,651\n"
                + "Row #21: 5,636.82\n"
                + "Row #22: 2,176\n"
                + "Row #22: 4,691.94\n"
                + "Row #23: 2,973\n"
                + "Row #23: 6,422.37\n"
                + "Row #24: 2,009\n"
                + "Row #24: 4,312.99\n"
                + "Row #25: 58\n"
                + "Row #25: 109.36\n"
                + "Row #26: 2,031\n"
                + "Row #26: 4,237.46\n"
                + "Row #27: 3,098\n"
                + "Row #27: 6,696.06\n"
                + "Row #28: 163\n"
                + "Row #28: 335.98\n"
                + "Row #29: 70\n"
                + "Row #29: 145.90\n"
                + "Row #30: 133\n"
                + "Row #30: 272.08\n"
                + "Row #31: 2,712\n"
                + "Row #31: 5,595.62\n"
                + "Row #32: 144\n"
                + "Row #32: 312.43\n"
                + "Row #33: 110\n"
                + "Row #33: 212.45\n"
                + "Row #34: 145\n"
                + "Row #34: 289.80\n"
                + "Row #35: 1,535\n"
                + "Row #35: 3,348.69\n"
                + "Row #36: 88\n"
                + "Row #36: 195.28\n"
                + "Row #37: 2,631\n"
                + "Row #37: 5,663.60\n"
                + "Row #38: 161\n"
                + "Row #38: 343.20\n"
                + "Row #39: 185\n"
                + "Row #39: 367.78\n"
                + "Row #40: 2,660\n"
                + "Row #40: 5,739.63\n"
                + "Row #41: 1,790\n"
                + "Row #41: 3,862.79\n"
                + "Row #42: 2,570\n"
                + "Row #42: 5,405.02\n"
                + "Row #43: 2,503\n"
                + "Row #43: 5,302.08\n"
                + "Row #44: 2,516\n"
                + "Row #44: 5,406.21\n"
                + "Row #45: 67,659\n"
                + "Row #45: 142,277.07\n"
                + "Row #46: 124,366\n"
                + "Row #46: 263,793.22\n");
        }
    }

    /**
     * "Could not find column for <agg table column> in header.." error<br>
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1359">MONDRIAN-1359</a>
     */
    public void testMondrian1359() {
        executeQuery(
            "WITH MEMBER [Store].[Stores].[CA plus OR] AS 'AGGREGATE({[Store].[USA].[CA], [Store].[USA].[OR]})'\n"
            + "SELECT {[Measures].[Unit Sales]} ON COLUMNS,\n"
            + "      {[Store].[USA].[CA], [Store].[USA].[OR], [Store].[CA plus OR]} ON ROWS\n"
            + "FROM Sales\n"
            + "WHERE ([1997].[Q1])");
        executeQuery(
            "SELECT {[Measures].AllMembers} ON COLUMNS,"
            + "{[Store].[USA].[CA], [Store].[USA].[WA]} ON ROWS "
            + "FROM Sales "
            + "WHERE ([1997].[Q1])");
        executeQuery(
            "SELECT {[Measures].[Unit Sales]} ON COLUMNS,\n"
            + "{[Store].[USA].[CA]} ON ROWS "
            + "FROM Sales ");
    }
}

// End FunctionTest.java
