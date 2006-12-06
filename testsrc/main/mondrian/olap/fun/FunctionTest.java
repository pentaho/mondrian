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
import mondrian.udf.CurrentDateMemberExactUdf;
import mondrian.udf.CurrentDateMemberUdf;
import mondrian.udf.CurrentDateStringUdf;
import mondrian.util.Bug;

import org.eigenbase.xom.StringEscaper;

import java.io.*;
import java.util.List;
import java.util.Collections;
import java.util.ArrayList;

/**
 * <code>FunctionTest</code> tests the functions defined in
 * {@link BuiltinFunTable}.
 *
 * @author gjohnson
 * @version $Id$
 */
public class FunctionTest extends FoodMartTestCase {

    private static final String months =
        fold("[Time].[1997].[Q1].[1]\n" +
            "[Time].[1997].[Q1].[2]\n" +
            "[Time].[1997].[Q1].[3]\n" +
            "[Time].[1997].[Q2].[4]\n" +
            "[Time].[1997].[Q2].[5]\n" +
            "[Time].[1997].[Q2].[6]\n" +
            "[Time].[1997].[Q3].[7]\n" +
            "[Time].[1997].[Q3].[8]\n" +
            "[Time].[1997].[Q3].[9]\n" +
            "[Time].[1997].[Q4].[10]\n" +
            "[Time].[1997].[Q4].[11]\n" +
            "[Time].[1997].[Q4].[12]");

    private static final String quarters =
        fold("[Time].[1997].[Q1]\n" +
            "[Time].[1997].[Q2]\n" +
            "[Time].[1997].[Q3]\n" +
            "[Time].[1997].[Q4]");

    private static final String year1997 = "[Time].[1997]";

    private static final String hierarchized1997 =
        fold(year1997 + "\n" +
            "[Time].[1997].[Q1]\n" +
            "[Time].[1997].[Q1].[1]\n" +
            "[Time].[1997].[Q1].[2]\n" +
            "[Time].[1997].[Q1].[3]\n" +
            "[Time].[1997].[Q2]\n" +
            "[Time].[1997].[Q2].[4]\n" +
            "[Time].[1997].[Q2].[5]\n" +
            "[Time].[1997].[Q2].[6]\n" +
            "[Time].[1997].[Q3]\n" +
            "[Time].[1997].[Q3].[7]\n" +
            "[Time].[1997].[Q3].[8]\n" +
            "[Time].[1997].[Q3].[9]\n" +
            "[Time].[1997].[Q4]\n" +
            "[Time].[1997].[Q4].[10]\n" +
            "[Time].[1997].[Q4].[11]\n" +
            "[Time].[1997].[Q4].[12]");

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
        StringBuilder buf = new StringBuilder("{");
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

    /**
     * Tests use of NULL literal to generate a null cell value.
     * Testcase is from bug 1440344.
     */
    public void testNullValue() {
        assertQueryReturns("with member [Measures].[X] as 'IIF([Measures].[Store Sales]>10000,[Measures].[Store Sales],Null)'\n" +
            "select\n" +
            "{[Measures].[X]} on columns,\n" +
            "{[Product].[Product Department].members} on rows\n" +
            "from Sales",
            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[X]}\n" +
                "Axis #2:\n" +
                "{[Product].[All Products].[Drink].[Alcoholic Beverages]}\n" +
                "{[Product].[All Products].[Drink].[Beverages]}\n" +
                "{[Product].[All Products].[Drink].[Dairy]}\n" +
                "{[Product].[All Products].[Food].[Baked Goods]}\n" +
                "{[Product].[All Products].[Food].[Baking Goods]}\n" +
                "{[Product].[All Products].[Food].[Breakfast Foods]}\n" +
                "{[Product].[All Products].[Food].[Canned Foods]}\n" +
                "{[Product].[All Products].[Food].[Canned Products]}\n" +
                "{[Product].[All Products].[Food].[Dairy]}\n" +
                "{[Product].[All Products].[Food].[Deli]}\n" +
                "{[Product].[All Products].[Food].[Eggs]}\n" +
                "{[Product].[All Products].[Food].[Frozen Foods]}\n" +
                "{[Product].[All Products].[Food].[Meat]}\n" +
                "{[Product].[All Products].[Food].[Produce]}\n" +
                "{[Product].[All Products].[Food].[Seafood]}\n" +
                "{[Product].[All Products].[Food].[Snack Foods]}\n" +
                "{[Product].[All Products].[Food].[Snacks]}\n" +
                "{[Product].[All Products].[Food].[Starchy Foods]}\n" +
                "{[Product].[All Products].[Non-Consumable].[Carousel]}\n" +
                "{[Product].[All Products].[Non-Consumable].[Checkout]}\n" +
                "{[Product].[All Products].[Non-Consumable].[Health and Hygiene]}\n" +
                "{[Product].[All Products].[Non-Consumable].[Household]}\n" +
                "{[Product].[All Products].[Non-Consumable].[Periodicals]}\n" +
                "Row #0: 14,029.08\n" +
                "Row #1: 27,748.53\n" +
                "Row #2: \n" +
                "Row #3: 16,455.43\n" +
                "Row #4: 38,670.41\n" +
                "Row #5: \n" +
                "Row #6: 39,774.34\n" +
                "Row #7: \n" +
                "Row #8: 30,508.85\n" +
                "Row #9: 25,318.93\n" +
                "Row #10: \n" +
                "Row #11: 55,207.50\n" +
                "Row #12: \n" +
                "Row #13: 82,248.42\n" +
                "Row #14: \n" +
                "Row #15: 67,609.82\n" +
                "Row #16: 14,550.05\n" +
                "Row #17: 11,756.07\n" +
                "Row #18: \n" +
                "Row #19: \n" +
                "Row #20: 32,571.86\n" +
                "Row #21: 60,469.89\n" +
                "Row #22: \n"));
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

    public void testIsEmptyQuery() {
        String desiredResult = fold("Axis #0:\n" +
            "{[Time].[1997].[Q4].[12], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth].[Portsmouth Imported Beer], [Measures].[Foo]}\n" +
            "Axis #1:\n" +
            "{[Store].[All Stores].[USA].[WA].[Bellingham]}\n" +
            "{[Store].[All Stores].[USA].[WA].[Bremerton]}\n" +
            "{[Store].[All Stores].[USA].[WA].[Seattle]}\n" +
            "{[Store].[All Stores].[USA].[WA].[Spokane]}\n" +
            "{[Store].[All Stores].[USA].[WA].[Tacoma]}\n" +
            "{[Store].[All Stores].[USA].[WA].[Walla Walla]}\n" +
            "{[Store].[All Stores].[USA].[WA].[Yakima]}\n" +
            "Row #0: 5\n" +
            "Row #0: 5\n" +
            "Row #0: 2\n" +
            "Row #0: 5\n" +
            "Row #0: 11\n" +
            "Row #0: 5\n" +
            "Row #0: 4\n");
        
        assertQueryReturns(
            "WITH MEMBER [Measures].[Foo] AS 'Iif(IsEmpty([Measures].[Unit Sales]), 5, [Measures].[Unit Sales])'\n" +
            "SELECT {[Store].[USA].[WA].children} on columns\n" +
            "FROM Sales\n" +
            "WHERE ( [Time].[1997].[Q4].[12],\n" +
            " [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth].[Portsmouth Imported Beer],\n" +
            " [Measures].[Foo])",
            desiredResult);

        assertQueryReturns(
            "WITH MEMBER [Measures].[Foo] AS 'Iif([Measures].[Unit Sales] IS EMPTY, 5, [Measures].[Unit Sales])'\n" +
            "SELECT {[Store].[USA].[WA].children} on columns\n" +
            "FROM Sales\n" +
            "WHERE ( [Time].[1997].[Q4].[12],\n" +
            " [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth].[Portsmouth Imported Beer],\n" +
            " [Measures].[Foo])",
            desiredResult);
    }

    public void testIsEmpty()
    {
        assertBooleanExprReturns("[Gender].[All Gender].Parent IS NULL", true);

        // Any functions that return a member from parameters that
        // include a member and that member is NULL also give a NULL.
        // Not a runtime exception.
        assertBooleanExprReturns("[Gender].CurrentMember.Parent.NextMember IS NULL", true);

        if (!Bug.Bug1530543Fixed) return;

        // When resolving a tuple's value in the cube, if there is
        // at least one NULL member in the tuple should return a
        // NULL cell value.
        assertBooleanExprReturns("IsEmpty( ([Time].currentMember.Parent, [Measures].[Unit Sales]) )", false);
        assertBooleanExprReturns("IsEmpty( ([Time].currentMember, [Measures].[Unit Sales]) )", false);

        // EMPTY refers to a genuine cell value that exists in the cube space,
        // and has no NULL members in the tuple,
        // but has no fact data at that crossing,
        // so it evaluates to EMPTY as a cell value.
        assertBooleanExprReturns("IsEmpty(\n" +
            " ([Time].[1997].[Q4].[12],\n" +
            "  [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth].[Portsmouth Imported Beer],\n" +
            "  [Store].[All Stores].[USA].[WA].[Bellingham]) )",
            true);
        assertBooleanExprReturns("IsEmpty(\n" +
            " ([Time].[1997].[Q4].[11],\n" +
            "  [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth].[Portsmouth Imported Beer],\n" +
            "  [Store].[All Stores].[USA].[WA].[Bellingham]) )",
            false);

        // The empty set is neither EMPTY nor NULL.
        // should give 0 as a result, not NULL and not EMPTY.
        assertQueryReturns(
            "WITH SET [empty set] AS '{}'\n" +
                " MEMBER [Measures].[Set Size] AS 'Count([empty set])'\n" +
                " MEMBER [Measures].[Set Size Is Empty] AS 'CASE WHEN IsEmpty([Measures].[Set Size]) THEN 1 ELSE 0 END '\n" +
                "SELECT [Measures].[Set Size] on columns",
            "");

        assertQueryReturns(
            "WITH SET [empty set] AS '{}'\n" +
                "WITH MEMBER [Measures].[Set Size] AS 'Count([empty set])'\n" +
                "SELECT [Measures].[Set Size] on columns",
            "");

        // Run time errors are BAD things.  They should not occur
        // in almost all cases.  In fact there should be no
        // logically formed MDX that generates them.  An ERROR
        // value in a cell though is perfectly legal - e.g. a
        // divide by 0.
        // E.g.
        String foo = "WITH [Measures].[Ratio This Period to Previous] as\n" +
            "'([Measures].[Sales],[Time].CurrentMember/([Measures].[Sales],[Time].CurrentMember.PrevMember)'\n" +
            "SELECT [Measures].[Ratio This Period to Previous] ON COLUMNS,\n" +
            "[Time].Members ON ROWS\n" +
            "FROM ...";

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
        String foo2 = "WITH MEMBER [Measures].[5 plus empty] AS\n" +
            "'5+([Product].[All Products].[Ski boots],[Geography].[All Geography].[Hawaii])'\n" +
            "SELECT [Measures].[5 plus empty] ON COLUMNS\n" +
            "FROM ...";
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
        String query = "with\n" +
            "member measures.[without VM] as ' [measures].[unit sales] '\n" +
            "select {measures.[without VM] } on 0,\n" +
            "[Warehouse].[Country].members on 1 from [warehouse and sales]\n";
        String expectedResult = "Axis #0:\n" +
            "{}\n" +
            "Axis #1:\n" +
            "{[Measures].[without VM]}\n" +
            "Axis #2:\n" +
            "{[Warehouse].[All Warehouses].[Canada]}\n" +
            "{[Warehouse].[All Warehouses].[Mexico]}\n" +
            "{[Warehouse].[All Warehouses].[USA]}\n" +
            "Row #0: \n" +
            "Row #1: \n" +
            "Row #2: \n";
        assertQueryReturns(query, fold(expectedResult));
    }

    /** Tests the <code>ValidMeasure</code> function. */
    public void testValidMeasure() {
        String query = "with\n" +
            "member measures.[with VM] as 'validmeasure( [measures].[unit sales] )'\n" +
            "select { measures.[with VM]} on 0,\n" +
            "[Warehouse].[Country].members on 1 from [warehouse and sales]\n";
        String expectedResult = "Axis #0:\n" +
            "{}\n" +
            "Axis #1:\n" +
            "{[Measures].[with VM]}\n" +
            "Axis #2:\n" +
            "{[Warehouse].[All Warehouses].[Canada]}\n" +
            "{[Warehouse].[All Warehouses].[Mexico]}\n" +
            "{[Warehouse].[All Warehouses].[USA]}\n" +
            "Row #0: 266,773\n" +
            "Row #1: 266,773\n" +
            "Row #2: 266,773\n";
        assertQueryReturns(query, fold(expectedResult));
    }

    public void testValidMeasureTupleHasAnotherMember() {
        String query = "with\n" +
            "member measures.[with VM] as 'validmeasure(( [measures].[unit sales],[customers].[all customers]))'\n" +
            "select { measures.[with VM]} on 0,\n" +
            "[Warehouse].[Country].members on 1 from [warehouse and sales]\n";
        String expectedResult = "Axis #0:\n" +
            "{}\n" +
            "Axis #1:\n" +
            "{[Measures].[with VM]}\n" +
            "Axis #2:\n" +
            "{[Warehouse].[All Warehouses].[Canada]}\n" +
            "{[Warehouse].[All Warehouses].[Mexico]}\n" +
            "{[Warehouse].[All Warehouses].[USA]}\n" +
            "Row #0: 266,773\n" +
            "Row #1: 266,773\n" +
            "Row #2: 266,773\n";
        assertQueryReturns(query, fold(expectedResult));
    }

    public void testValidMeasureDepends() {
        assertExprDependsOn("ValidMeasure([Measures].[Unit Sales])",
            allDimsExcept(new String[] {"[Measures]"}));

        assertExprDependsOn("ValidMeasure(([Measures].[Unit Sales], [Time].[1997].[Q1]))",
            allDimsExcept(new String[] {"[Measures]", "[Time]"}));

        assertExprDependsOn("ValidMeasure(([Measures].[Unit Sales], [Time].CurrentMember.Parent))",
            allDimsExcept(new String[] {"[Measures]"}));
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
                    "with member [Measures].[Closing Unit Sales] as '([Measures].[Unit Sales], ClosingPeriod([Time].[Month]))'\n" +
                    "select non empty {[Measures].[Closing Unit Sales]} on columns,\n" +
                    " {Descendants([Time].[1997])} on rows\n" +
                    "from [Sales]",

                fold(
                    "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Closing Unit Sales]}\n" +
                    "Axis #2:\n" +
                    "{[Time].[1997]}\n" +
                    "{[Time].[1997].[Q1]}\n" +
                    "{[Time].[1997].[Q1].[1]}\n" +
                    "{[Time].[1997].[Q1].[2]}\n" +
                    "{[Time].[1997].[Q1].[3]}\n" +
                    "{[Time].[1997].[Q2]}\n" +
                    "{[Time].[1997].[Q2].[4]}\n" +
                    "{[Time].[1997].[Q2].[5]}\n" +
                    "{[Time].[1997].[Q2].[6]}\n" +
                    "{[Time].[1997].[Q3]}\n" +
                    "{[Time].[1997].[Q3].[7]}\n" +
                    "{[Time].[1997].[Q3].[8]}\n" +
                    "{[Time].[1997].[Q3].[9]}\n" +
                    "{[Time].[1997].[Q4]}\n" +
                    "{[Time].[1997].[Q4].[10]}\n" +
                    "{[Time].[1997].[Q4].[11]}\n" +
                    "{[Time].[1997].[Q4].[12]}\n" +
                    "Row #0: 26,796\n" +
                    "Row #1: 23,706\n" +
                    "Row #2: 21,628\n" +
                    "Row #3: 20,957\n" +
                    "Row #4: 23,706\n" +
                    "Row #5: 21,350\n" +
                    "Row #6: 20,179\n" +
                    "Row #7: 21,081\n" +
                    "Row #8: 21,350\n" +
                    "Row #9: 20,388\n" +
                    "Row #10: 23,763\n" +
                    "Row #11: 21,697\n" +
                    "Row #12: 20,388\n" +
                    "Row #13: 26,796\n" +
                    "Row #14: 19,958\n" +
                    "Row #15: 25,270\n" +
                    "Row #16: 26,796\n"));

        assertQueryReturns(
                    "with member [Measures].[Closing Unit Sales] as '([Measures].[Unit Sales], ClosingPeriod([Time].[Month]))'\n" +
                    "select {[Measures].[Unit Sales], [Measures].[Closing Unit Sales]} on columns,\n" +
                    " {[Time].[1997], [Time].[1997].[Q1], [Time].[1997].[Q1].[1], [Time].[1997].[Q1].[3], [Time].[1997].[Q4].[12]} on rows\n" +
                    "from [Sales]",
                fold(
                    "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Unit Sales]}\n" +
                    "{[Measures].[Closing Unit Sales]}\n" +
                    "Axis #2:\n" +
                    "{[Time].[1997]}\n" +
                    "{[Time].[1997].[Q1]}\n" +
                    "{[Time].[1997].[Q1].[1]}\n" +
                    "{[Time].[1997].[Q1].[3]}\n" +
                    "{[Time].[1997].[Q4].[12]}\n" +
                    "Row #0: 266,773\n" +
                    "Row #0: 26,796\n" +
                    "Row #1: 66,291\n" +
                    "Row #1: 23,706\n" +
                    "Row #2: 21,628\n" +
                    "Row #2: 21,628\n" +
                    "Row #3: 23,706\n" +
                    "Row #3: 23,706\n" +
                    "Row #4: 26,796\n" +
                    "Row #4: 26,796\n"));
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
                        "with member [Measures].[Foo] as ' ClosingPeriod().uniquename '\n" +
                        "select {[Measures].[Foo]} on columns,\n" +
                        "  {[Time].[1997],\n" +
                        "   [Time].[1997].[Q2],\n" +
                        "   [Time].[1997].[Q2].[4]} on rows\n" +
                        "from Sales",
                    fold(
                        "Axis #0:\n" +
                        "{}\n" +
                        "Axis #1:\n" +
                        "{[Measures].[Foo]}\n" +
                        "Axis #2:\n" +
                        "{[Time].[1997]}\n" +
                        "{[Time].[1997].[Q2]}\n" +
                        "{[Time].[1997].[Q2].[4]}\n" +
                        "Row #0: [Time].[1997].[Q4]\n" +
                        "Row #1: [Time].[1997].[Q2].[6]\n" +
                        "Row #2: [Time].[#Null]\n" + // MSAS returns "" here.
                        ""));
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
        assertAxisReturns("{[Customers].[Country].Members}", fold(
            "[Customers].[All Customers].[Canada]\n" +
            "[Customers].[All Customers].[Mexico]\n" +
            "[Customers].[All Customers].[USA]"
        ));

        // <Level>.members applied to 'all' level
        assertAxisReturns("{[Customers].[(All)].Members}",
                "[Customers].[All Customers]");

        // <Level>.members applied to measures dimension
        // Note -- no cube-level calculated members are present
        assertAxisReturns("{[Measures].[MeasuresLevel].Members}",
                fold(
                    "[Measures].[Unit Sales]\n" +
                    "[Measures].[Store Cost]\n" +
                    "[Measures].[Store Sales]\n" +
                    "[Measures].[Sales Count]\n" +
                    "[Measures].[Customer Count]\n" +
                    "[Measures].[Promotion Sales]"
                ));

        // <Dimension>.members applied to Measures
        assertAxisReturns("{[Measures].Members}",
                fold(
                    "[Measures].[Unit Sales]\n" +
                    "[Measures].[Store Cost]\n" +
                    "[Measures].[Store Sales]\n" +
                    "[Measures].[Sales Count]\n" +
                    "[Measures].[Customer Count]\n" +
                    "[Measures].[Promotion Sales]"));

        // <Dimension>.members applied to a query with calc measures
        // Again, no calc measures are returned
        assertQueryReturns("with member [Measures].[Xxx] AS ' [Measures].[Unit Sales] '" +
                "select {[Measures].members} on columns from [Sales]",
                fold(
                    "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Unit Sales]}\n" +
                    "{[Measures].[Store Cost]}\n" +
                    "{[Measures].[Store Sales]}\n" +
                    "{[Measures].[Sales Count]}\n" +
                    "{[Measures].[Customer Count]}\n" +
                    "{[Measures].[Promotion Sales]}\n" +
                    "Row #0: 266,773\n" +
                    "Row #0: 225,627.23\n" +
                    "Row #0: 565,238.13\n" +
                    "Row #0: 86,837\n" +
                    "Row #0: 5,581\n" +
                    "Row #0: 151,211.21\n"));
    }

    public void testAllMembers() {
        // <Level>.allmembers
        assertAxisReturns("{[Customers].[Country].allmembers}",
                fold(
                    "[Customers].[All Customers].[Canada]\n" +
                    "[Customers].[All Customers].[Mexico]\n" +
                    "[Customers].[All Customers].[USA]"
        ));

        // <Level>.allmembers applied to 'all' level
        assertAxisReturns("{[Customers].[(All)].allmembers}",
                "[Customers].[All Customers]");

        // <Level>.allmembers applied to measures dimension
        // Note -- cube-level calculated members ARE present
        assertAxisReturns("{[Measures].[MeasuresLevel].allmembers}",
                fold(
                    "[Measures].[Unit Sales]\n" +
                    "[Measures].[Store Cost]\n" +
                    "[Measures].[Store Sales]\n" +
                    "[Measures].[Sales Count]\n" +
                    "[Measures].[Customer Count]\n" +
                    "[Measures].[Promotion Sales]\n" +
                    "[Measures].[Profit]\n" +
                    "[Measures].[Profit last Period]\n" +
                    "[Measures].[Profit Growth]"));

        // <Dimension>.allmembers applied to Measures
        assertAxisReturns("{[Measures].allmembers}",
                fold(
                    "[Measures].[Unit Sales]\n" +
                    "[Measures].[Store Cost]\n" +
                    "[Measures].[Store Sales]\n" +
                    "[Measures].[Sales Count]\n" +
                    "[Measures].[Customer Count]\n" +
                    "[Measures].[Promotion Sales]\n" +
                    "[Measures].[Profit]\n" +
                    "[Measures].[Profit last Period]\n" +
                    "[Measures].[Profit Growth]"));

        // <Dimension>.allmembers applied to a query with calc measures
        // Calc measures are returned
        assertQueryReturns("with member [Measures].[Xxx] AS ' [Measures].[Unit Sales] '" +
                "select {[Measures].allmembers} on columns from [Sales]",
                fold(
                    "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Unit Sales]}\n" +
                    "{[Measures].[Store Cost]}\n" +
                    "{[Measures].[Store Sales]}\n" +
                    "{[Measures].[Sales Count]}\n" +
                    "{[Measures].[Customer Count]}\n" +
                    "{[Measures].[Promotion Sales]}\n" +
                    "{[Measures].[Profit]}\n" +
                    "{[Measures].[Profit last Period]}\n" +
                    "{[Measures].[Profit Growth]}\n" +
                    "{[Measures].[Xxx]}\n" +
                    "Row #0: 266,773\n" +
                    "Row #0: 225,627.23\n" +
                    "Row #0: 565,238.13\n" +
                    "Row #0: 86,837\n" +
                    "Row #0: 5,581\n" +
                    "Row #0: 151,211.21\n" +
                    "Row #0: $339,610.90\n" +
                    "Row #0: $339,610.90\n" +
                    "Row #0: 0.0%\n" +
                    "Row #0: 266,773\n"));

        // Calc measure members from schema and from query
        assertQueryReturns("WITH MEMBER [Measures].[Unit to Sales ratio] as '[Measures].[Unit Sales] / [Measures].[Store Sales]', FORMAT_STRING='0.0%' " +
                "SELECT {[Measures].AllMembers} ON COLUMNS," +
                "non empty({[Store].[Store State].Members}) ON ROWS " +
                "FROM Sales " +
                "WHERE ([1997].[Q1])",
                fold(
                    "Axis #0:\n" +
                    "{[Time].[1997].[Q1]}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Unit Sales]}\n" +
                    "{[Measures].[Store Cost]}\n" +
                    "{[Measures].[Store Sales]}\n" +
                    "{[Measures].[Sales Count]}\n" +
                    "{[Measures].[Customer Count]}\n" +
                    "{[Measures].[Promotion Sales]}\n" +
                    "{[Measures].[Profit]}\n" +
                    "{[Measures].[Profit last Period]}\n" +
                    "{[Measures].[Profit Growth]}\n" +
                    "{[Measures].[Unit to Sales ratio]}\n" +
                    "Axis #2:\n" +
                    "{[Store].[All Stores].[USA].[CA]}\n" +
                    "{[Store].[All Stores].[USA].[OR]}\n" +
                    "{[Store].[All Stores].[USA].[WA]}\n" +
                    "Row #0: 16,890\n" +
                    "Row #0: 14,431.09\n" +
                    "Row #0: 36,175.20\n" +
                    "Row #0: 5,498\n" +
                    "Row #0: 1,110\n" +
                    "Row #0: 14,447.16\n" +
                    "Row #0: $21,744.11\n" +
                    "Row #0: $21,744.11\n" +
                    "Row #0: 0.0%\n" +
                    "Row #0: 46.7%\n" +
                    "Row #1: 19,287\n" +
                    "Row #1: 16,081.07\n" +
                    "Row #1: 40,170.29\n" +
                    "Row #1: 6,184\n" +
                    "Row #1: 767\n" +
                    "Row #1: 10,829.64\n" +
                    "Row #1: $24,089.22\n" +
                    "Row #1: $24,089.22\n" +
                    "Row #1: 0.0%\n" +
                    "Row #1: 48.0%\n" +
                    "Row #2: 30,114\n" +
                    "Row #2: 25,240.08\n" +
                    "Row #2: 63,282.86\n" +
                    "Row #2: 9,906\n" +
                    "Row #2: 1,104\n" +
                    "Row #2: 18,459.60\n" +
                    "Row #2: $38,042.78\n" +
                    "Row #2: $38,042.78\n" +
                    "Row #2: 0.0%\n" +
                    "Row #2: 47.6%\n"));

        // Calc member in query and schema not seen
        assertQueryReturns("WITH MEMBER [Measures].[Unit to Sales ratio] as '[Measures].[Unit Sales] / [Measures].[Store Sales]', FORMAT_STRING='0.0%' " +
                "SELECT {[Measures].AllMembers} ON COLUMNS," +
                "non empty({[Store].[Store State].Members}) ON ROWS " +
                "FROM Sales " +
                "WHERE ([1997].[Q1])",
                fold(
                    "Axis #0:\n" +
                    "{[Time].[1997].[Q1]}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Unit Sales]}\n" +
                    "{[Measures].[Store Cost]}\n" +
                    "{[Measures].[Store Sales]}\n" +
                    "{[Measures].[Sales Count]}\n" +
                    "{[Measures].[Customer Count]}\n" +
                    "{[Measures].[Promotion Sales]}\n" +
                    "{[Measures].[Profit]}\n" +
                    "{[Measures].[Profit last Period]}\n" +
                    "{[Measures].[Profit Growth]}\n" +
                    "{[Measures].[Unit to Sales ratio]}\n" +
                    "Axis #2:\n" +
                    "{[Store].[All Stores].[USA].[CA]}\n" +
                    "{[Store].[All Stores].[USA].[OR]}\n" +
                    "{[Store].[All Stores].[USA].[WA]}\n" +
                    "Row #0: 16,890\n" +
                    "Row #0: 14,431.09\n" +
                    "Row #0: 36,175.20\n" +
                    "Row #0: 5,498\n" +
                    "Row #0: 1,110\n" +
                    "Row #0: 14,447.16\n" +
                    "Row #0: $21,744.11\n" +
                    "Row #0: $21,744.11\n" +
                    "Row #0: 0.0%\n" +
                    "Row #0: 46.7%\n" +
                    "Row #1: 19,287\n" +
                    "Row #1: 16,081.07\n" +
                    "Row #1: 40,170.29\n" +
                    "Row #1: 6,184\n" +
                    "Row #1: 767\n" +
                    "Row #1: 10,829.64\n" +
                    "Row #1: $24,089.22\n" +
                    "Row #1: $24,089.22\n" +
                    "Row #1: 0.0%\n" +
                    "Row #1: 48.0%\n" +
                    "Row #2: 30,114\n" +
                    "Row #2: 25,240.08\n" +
                    "Row #2: 63,282.86\n" +
                    "Row #2: 9,906\n" +
                    "Row #2: 1,104\n" +
                    "Row #2: 18,459.60\n" +
                    "Row #2: $38,042.78\n" +
                    "Row #2: $38,042.78\n" +
                    "Row #2: 0.0%\n" +
                    "Row #2: 47.6%\n"));

        // Calc member in query and schema not seen
        assertQueryReturns("WITH MEMBER [Measures].[Unit to Sales ratio] as '[Measures].[Unit Sales] / [Measures].[Store Sales]', FORMAT_STRING='0.0%' " +
                "SELECT {[Measures].Members} ON COLUMNS," +
                        "non empty({[Store].[Store State].Members}) ON ROWS " +
                "FROM Sales " +
                "WHERE ([1997].[Q1])",
                fold(
                "Axis #0:\n" +
                "{[Time].[1997].[Q1]}\n" +
                "Axis #1:\n" +
                "{[Measures].[Unit Sales]}\n" +
                "{[Measures].[Store Cost]}\n" +
                "{[Measures].[Store Sales]}\n" +
                "{[Measures].[Sales Count]}\n" +
                "{[Measures].[Customer Count]}\n" +
                "{[Measures].[Promotion Sales]}\n" +
                "Axis #2:\n" +
                "{[Store].[All Stores].[USA].[CA]}\n" +
                "{[Store].[All Stores].[USA].[OR]}\n" +
                "{[Store].[All Stores].[USA].[WA]}\n" +
                "Row #0: 16,890\n" +
                "Row #0: 14,431.09\n" +
                "Row #0: 36,175.20\n" +
                "Row #0: 5,498\n" +
                "Row #0: 1,110\n" +
                "Row #0: 14,447.16\n" +
                "Row #1: 19,287\n" +
                "Row #1: 16,081.07\n" +
                "Row #1: 40,170.29\n" +
                "Row #1: 6,184\n" +
                "Row #1: 767\n" +
                "Row #1: 10,829.64\n" +
                "Row #2: 30,114\n" +
                "Row #2: 25,240.08\n" +
                "Row #2: 63,282.86\n" +
                "Row #2: 9,906\n" +
                "Row #2: 1,104\n" +
                "Row #2: 18,459.60\n"));

        // Calc member in dimension based on level
        assertQueryReturns("WITH MEMBER [Store].[USA].[CA plus OR] AS 'AGGREGATE({[Store].[USA].[CA], [Store].[USA].[OR]})' " +
                "SELECT {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS," +
                        "non empty({[Store].[Store State].AllMembers}) ON ROWS " +
                "FROM Sales " +
                "WHERE ([1997].[Q1])",
                fold(
                        "Axis #0:\n" +
                        "{[Time].[1997].[Q1]}\n" +
                        "Axis #1:\n" +
                        "{[Measures].[Unit Sales]}\n" +
                        "{[Measures].[Store Sales]}\n" +
                        "Axis #2:\n" +
                        "{[Store].[All Stores].[USA].[CA]}\n" +
                        "{[Store].[All Stores].[USA].[OR]}\n" +
                        "{[Store].[All Stores].[USA].[WA]}\n" +
                        "{[Store].[All Stores].[USA].[CA plus OR]}\n" +
                        "Row #0: 16,890\n" +
                        "Row #0: 36,175.20\n" +
                        "Row #1: 19,287\n" +
                        "Row #1: 40,170.29\n" +
                        "Row #2: 30,114\n" +
                        "Row #2: 63,282.86\n" +
                        "Row #3: 36,177\n" +
                        "Row #3: 76,345.49\n"));

        // Calc member in dimension based on level not seen
        assertQueryReturns("WITH MEMBER [Store].[USA].[CA plus OR] AS 'AGGREGATE({[Store].[USA].[CA], [Store].[USA].[OR]})' " +
                "SELECT {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS," +
                        "non empty({[Store].[Store Country].AllMembers}) ON ROWS " +
                "FROM Sales " +
                "WHERE ([1997].[Q1])",
                fold(
                        "Axis #0:\n" +
                        "{[Time].[1997].[Q1]}\n" +
                        "Axis #1:\n" +
                        "{[Measures].[Unit Sales]}\n" +
                        "{[Measures].[Store Sales]}\n" +
                        "Axis #2:\n" +
                        "{[Store].[All Stores].[USA]}\n" +
                        "Row #0: 66,291\n" +
                        "Row #0: 139,628.35\n"));
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
                fold(
                        "Axis #0:\n" +
                        "{[Time].[1997].[Q1]}\n" +
                        "Axis #1:\n" +
                        "{[Measures].[Unit Sales]}\n" +
                        "{[Measures].[Store Sales]}\n" +
                        "Axis #2:\n" +
                        "{[Store].[All Stores].[USA].[CA]}\n" +
                        "{[Store].[All Stores].[USA].[OR]}\n" +
                        "{[Store].[All Stores].[USA].[WA]}\n" +
                        "{[Store].[All Stores].[USA].[CA plus OR]}\n" +
                        "Row #0: 16,890\n" +
                        "Row #0: 36,175.20\n" +
                        "Row #1: 19,287\n" +
                        "Row #1: 40,170.29\n" +
                        "Row #2: 30,114\n" +
                        "Row #2: 63,282.86\n" +
                        "Row #3: 36,177\n" +
                        "Row #3: 76,345.49\n"));
        //----------------------------------------------------
        //Calc member in dimension based on level included
        //Calc members in measures in schema included
        //----------------------------------------------------
        assertQueryReturns("WITH MEMBER [Store].[USA].[CA plus OR] AS 'AGGREGATE({[Store].[USA].[CA], [Store].[USA].[OR]})' " +
                "SELECT AddCalculatedMembers({[Measures].[Unit Sales], [Measures].[Store Sales]}) ON COLUMNS," +
                        "AddCalculatedMembers([Store].[USA].Children) ON ROWS " +
                "FROM Sales " +
                "WHERE ([1997].[Q1])",
                fold(
                        "Axis #0:\n" +
                        "{[Time].[1997].[Q1]}\n" +
                        "Axis #1:\n" +
                        "{[Measures].[Unit Sales]}\n" +
                        "{[Measures].[Store Sales]}\n" +
                        "{[Measures].[Profit]}\n" +
                        "{[Measures].[Profit last Period]}\n" +
                        "{[Measures].[Profit Growth]}\n" +

                        "Axis #2:\n" +
                        "{[Store].[All Stores].[USA].[CA]}\n" +
                        "{[Store].[All Stores].[USA].[OR]}\n" +
                        "{[Store].[All Stores].[USA].[WA]}\n" +
                        "{[Store].[All Stores].[USA].[CA plus OR]}\n" +
                        "Row #0: 16,890\n" +
                        "Row #0: 36,175.20\n" +
                        "Row #0: $21,744.11\n" +
                        "Row #0: $21,744.11\n" +
                        "Row #0: 0.0%\n" +
                        "Row #1: 19,287\n" +
                        "Row #1: 40,170.29\n" +
                        "Row #1: $24,089.22\n" +
                        "Row #1: $24,089.22\n" +
                        "Row #1: 0.0%\n" +
                        "Row #2: 30,114\n" +
                        "Row #2: 63,282.86\n" +
                        "Row #2: $38,042.78\n" +
                        "Row #2: $38,042.78\n" +
                        "Row #2: 0.0%\n" +
                        "Row #3: 36,177\n" +
                        "Row #3: 76,345.49\n" +
                        "Row #3: $45,833.33\n" +
                        "Row #3: $45,833.33\n" +
                        "Row #3: 0.0%\n"));
        //----------------------------------------------------
        //Two dimensions
        //----------------------------------------------------
        assertQueryReturns("SELECT AddCalculatedMembers({[Measures].[Unit Sales], [Measures].[Store Sales]}) ON COLUMNS," +
                        "{([Store].[USA].[CA], [Gender].[F])} ON ROWS " +
                "FROM Sales " +
                "WHERE ([1997].[Q1])",
                fold(
                        "Axis #0:\n" +
                        "{[Time].[1997].[Q1]}\n" +
                        "Axis #1:\n" +
                        "{[Measures].[Unit Sales]}\n" +
                        "{[Measures].[Store Sales]}\n" +
                        "{[Measures].[Profit]}\n" +
                        "{[Measures].[Profit last Period]}\n" +
                        "{[Measures].[Profit Growth]}\n" +

                        "Axis #2:\n" +
                        "{[Store].[All Stores].[USA].[CA], [Gender].[All Gender].[F]}\n" +
                        "Row #0: 8,218\n" +
                        "Row #0: 17,928.37\n" +
                        "Row #0: $10,771.98\n" +
                        "Row #0: $10,771.98\n" +
                        "Row #0: 0.0%\n"));
        //----------------------------------------------------
        //Should throw more than one dimension error
        //----------------------------------------------------

        assertAxisThrows("AddCalculatedMembers({([Store].[USA].[CA], [Gender].[F])})",
            "Only single dimension members allowed in set for AddCalculatedMembers");
    }

    public void testStripCalculatedMembers() {
        assertAxisReturns("StripCalculatedMembers({[Measures].AllMembers})",
                fold(
                    "[Measures].[Unit Sales]\n" +
                    "[Measures].[Store Cost]\n" +
                    "[Measures].[Store Sales]\n" +
                    "[Measures].[Sales Count]\n" +
                    "[Measures].[Customer Count]\n" +
                    "[Measures].[Promotion Sales]"));

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
                fold(
                        "Axis #0:\n" +
                        "{[Time].[1997].[Q1]}\n" +
                        "Axis #1:\n" +
                        "{[Measures].[Unit Sales]}\n" +
                        "{[Measures].[Store Sales]}\n" +
                        "Axis #2:\n" +
                        "{[Store].[All Stores].[USA].[CA]}\n" +
                        "{[Store].[All Stores].[USA].[OR]}\n" +
                        "{[Store].[All Stores].[USA].[WA]}\n" +
                        "Row #0: 16,890\n" +
                        "Row #0: 36,175.20\n" +
                        "Row #1: 19,287\n" +
                        "Row #1: 40,170.29\n" +
                        "Row #2: 30,114\n" +
                        "Row #2: 63,282.86\n"));
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
                    "with member [Measures].[Foo] as '[Gender].CurrentMember.Name'\n" +
                    "select {[Measures].[Foo]} on columns\n" +
                    "from Sales where ([Gender].[F])");
        Assert.assertEquals("F", result.getCell(new int[]{0}).getValue());
    }

    public void testCurrentMemberFromDefaultMember() {
        Result result = executeQuery(
                    "with member [Measures].[Foo] as '[Time].CurrentMember.Name'\n" +
                    "select {[Measures].[Foo]} on columns\n" +
                    "from Sales");
        Assert.assertEquals("1997", result.getCell(new int[]{0}).getValue());
    }

    public void testCurrentMemberMultiHierarchy() {
        final String queryString =
            "with member [Measures].[Foo] as\n" +
            " 'IIf(([Time].CurrentMember.Hierarchy.Name = \"Time.Weekly\"), \n" +
            "[Measures].[Unit Sales], \n" +
            "- [Measures].[Unit Sales])'\n" +
            "select {[Measures].[Unit Sales], [Measures].[Foo]} ON COLUMNS,\n" +
            "  {[Product].[Food].[Dairy]} ON ROWS\n" +
            "from [Sales]";
        Result result = executeQuery(
                queryString + " where [Time].[1997]");
        final int[] coords = {1, 0};
        Assert.assertEquals("-12,885", result.getCell(coords).getFormattedValue());

        result = executeQuery(
                queryString + " where [Time.Weekly].[1997]");
        Assert.assertEquals("12,885", result.getCell(coords).getFormattedValue());
    }
    public void testCurrentMemberMultiHierarchy2() {
        final String queryString1 =
            "with member [Measures].[Foo] as\n" +
            " 'IIf(([Time].CurrentMember.Hierarchy.Name = \"Time.Weekly\"), \n" +
            "[Measures].[Unit Sales], \n" +
            "- [Measures].[Unit Sales])'\n" +
            "select {[Measures].[Unit Sales], [Measures].[Foo]} ON COLUMNS,";

        final String queryString2 =
            "from [Sales]\n" +
            "  where [Product].[Food].[Dairy] ";

        Result result = executeQuery(
                queryString1 + " {[Time].[1997]} ON ROWS " + queryString2);
        final int[] coords = {1, 0};
        Assert.assertEquals("-12,885", result.getCell(coords).getFormattedValue());

        result = executeQuery(
                queryString1 + " {[Time.Weekly].[1997]} ON ROWS " + queryString2);
        Assert.assertEquals("12,885", result.getCell(coords).getFormattedValue());
    }

    public void testDefaultMember() {
        Result result = executeQuery(
                 "select {[Time.Weekly].DefaultMember} on columns\n" +
                 "from Sales");
        Assert.assertEquals("1997", result.getAxes()[0].positions[0].members[0].getName());
    }

    public void testCurrentMemberFromAxis() {
        Result result = executeQuery(
                    "with member [Measures].[Foo] as '[Gender].CurrentMember.Name || [Marital Status].CurrentMember.Name'\n" +
                    "select {[Measures].[Foo]} on columns,\n" +
                    " CrossJoin({[Gender].children}, {[Marital Status].children}) on rows\n" +
                    "from Sales");
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
                    "with member [Measures].[Foo] as '[Measures].CurrentMember.Name'\n" +
                    "select {[Measures].[Foo]} on columns\n" +
                    "from Sales");
        Assert.assertEquals("Unit Sales", result.getCell(new int[]{0}).getValue());
    }

    public void testDimensionDefaultMember() {
        Member member = executeSingletonAxis("[Measures].DefaultMember");
        Assert.assertEquals("Unit Sales", member.getName());
    }

    public void testDrilldownLevel() {
        // Expect all children of USA
        assertAxisReturns("DrilldownLevel({[Store].[USA]}, [Store].[Store Country])",
                fold(
                    "[Store].[All Stores].[USA]\n" +
                    "[Store].[All Stores].[USA].[CA]\n" +
                    "[Store].[All Stores].[USA].[OR]\n" +
                    "[Store].[All Stores].[USA].[WA]"));

        // Expect same set, because [USA] is already drilled
        assertAxisReturns("DrilldownLevel({[Store].[USA], [Store].[USA].[CA]}, [Store].[Store Country])",
                fold(
                    "[Store].[All Stores].[USA]\n" +
                    "[Store].[All Stores].[USA].[CA]"));

        // Expect drill, because [USA] isn't already drilled. You can't
        // drill down on [CA] and get to [USA]
        assertAxisReturns("DrilldownLevel({[Store].[USA].[CA],[Store].[USA]}, [Store].[Store Country])",
                fold(
                    "[Store].[All Stores].[USA].[CA]\n" +
                    "[Store].[All Stores].[USA]\n" +
                    "[Store].[All Stores].[USA].[CA]\n" +
                    "[Store].[All Stores].[USA].[OR]\n" +
                    "[Store].[All Stores].[USA].[WA]"));

        assertThrows("select DrilldownLevel({[Store].[USA].[CA],[Store].[USA]},, 0) on columns from [Sales]",
                "Syntax error");
    }


    public void testDrilldownMember() {

        // Expect all children of USA
        assertAxisReturns("DrilldownMember({[Store].[USA]}, {[Store].[USA]})",
                fold(
                    "[Store].[All Stores].[USA]\n" +
                    "[Store].[All Stores].[USA].[CA]\n" +
                    "[Store].[All Stores].[USA].[OR]\n" +
                    "[Store].[All Stores].[USA].[WA]"));

        // Expect all children of USA.CA and USA.OR
        assertAxisReturns("DrilldownMember({[Store].[USA].[CA], [Store].[USA].[OR]}, "+
                "{[Store].[USA].[CA], [Store].[USA].[OR], [Store].[USA].[WA]})",
                fold(
                    "[Store].[All Stores].[USA].[CA]\n" +
                    "[Store].[All Stores].[USA].[CA].[Alameda]\n" +
                    "[Store].[All Stores].[USA].[CA].[Beverly Hills]\n" +
                    "[Store].[All Stores].[USA].[CA].[Los Angeles]\n" +
                    "[Store].[All Stores].[USA].[CA].[San Diego]\n" +
                    "[Store].[All Stores].[USA].[CA].[San Francisco]\n" +
                    "[Store].[All Stores].[USA].[OR]\n" +
                    "[Store].[All Stores].[USA].[OR].[Portland]\n" +
                    "[Store].[All Stores].[USA].[OR].[Salem]"));


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
                fold(
                    "[Store].[All Stores].[USA]\n" +
                    "[Store].[All Stores].[USA].[CA]\n" +
                    "[Store].[All Stores].[USA].[CA].[Alameda]\n" +
                    "[Store].[All Stores].[USA].[CA].[Beverly Hills]\n" +
                    "[Store].[All Stores].[USA].[CA].[Los Angeles]\n" +
                    "[Store].[All Stores].[USA].[CA].[San Diego]\n" +
                    "[Store].[All Stores].[USA].[CA].[San Diego].[Store 24]\n" +
                    "[Store].[All Stores].[USA].[CA].[San Francisco]\n" +
                    "[Store].[All Stores].[USA].[OR]\n" +
                    "[Store].[All Stores].[USA].[WA]\n" +
                    "[Store].[All Stores].[USA].[WA].[Bellingham]\n" +
                    "[Store].[All Stores].[USA].[WA].[Bremerton]\n" +
                    "[Store].[All Stores].[USA].[WA].[Seattle]\n" +
                    "[Store].[All Stores].[USA].[WA].[Spokane]\n" +
                    "[Store].[All Stores].[USA].[WA].[Tacoma]\n" +
                    "[Store].[All Stores].[USA].[WA].[Walla Walla]\n" +
                    "[Store].[All Stores].[USA].[WA].[Yakima]"));

        // Sets of tuples
        assertAxisReturns("DrilldownMember({([Store Type].[Supermarket], [Store].[USA])}, {[Store].[USA]})",
                fold(
                    "{[Store Type].[All Store Types].[Supermarket], [Store].[All Stores].[USA]}\n" +
                    "{[Store Type].[All Store Types].[Supermarket], [Store].[All Stores].[USA].[CA]}\n" +
                    "{[Store Type].[All Store Types].[Supermarket], [Store].[All Stores].[USA].[OR]}\n" +
                    "{[Store Type].[All Store Types].[Supermarket], [Store].[All Stores].[USA].[WA]}"));
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
                    "WITH MEMBER [Store].[CA plus OR] AS 'AGGREGATE({[Store].[USA].[CA], [Store].[USA].[OR]})'\n" +
                    "SELECT {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,\n" +
                    "      {[Store].[USA].[CA], [Store].[USA].[OR], [Store].[CA plus OR]} ON ROWS\n" +
                    "FROM Sales\n" +
                    "WHERE ([1997].[Q1])",
                fold(
                    "Axis #0:\n" +
                    "{[Time].[1997].[Q1]}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Unit Sales]}\n" +
                    "{[Measures].[Store Sales]}\n" +
                    "Axis #2:\n" +
                    "{[Store].[All Stores].[USA].[CA]}\n" +
                    "{[Store].[All Stores].[USA].[OR]}\n" +
                    "{[Store].[CA plus OR]}\n" +
                    "Row #0: 16,890\n" +
                    "Row #0: 36,175.20\n" +
                    "Row #1: 19,287\n" +
                    "Row #1: 40,170.29\n" +
                    "Row #2: 36,177\n" +
                    "Row #2: 76,345.49\n"));
    }

    public void testAggregate2() {
        assertQueryReturns(
                    "WITH\n" +
                    "  MEMBER [Time].[1st Half Sales] AS 'Aggregate({Time.[1997].[Q1], Time.[1997].[Q2]})'\n" +
                    "  MEMBER [Time].[2nd Half Sales] AS 'Aggregate({Time.[1997].[Q3], Time.[1997].[Q4]})'\n" +
                    "  MEMBER [Time].[Difference] AS 'Time.[2nd Half Sales] - Time.[1st Half Sales]'\n" +
                    "SELECT\n" +
                    "   { [Store].[Store State].Members} ON COLUMNS,\n" +
                    "   { Time.[1st Half Sales], Time.[2nd Half Sales], Time.[Difference]} ON ROWS\n" +
                    "FROM Sales\n" +
                    "WHERE [Measures].[Store Sales]",
                fold(
                    "Axis #0:\n" +
                    "{[Measures].[Store Sales]}\n" +
                    "Axis #1:\n" +
                    "{[Store].[All Stores].[Canada].[BC]}\n" +
                    "{[Store].[All Stores].[Mexico].[DF]}\n" +
                    "{[Store].[All Stores].[Mexico].[Guerrero]}\n" +
                    "{[Store].[All Stores].[Mexico].[Jalisco]}\n" +
                    "{[Store].[All Stores].[Mexico].[Veracruz]}\n" +
                    "{[Store].[All Stores].[Mexico].[Yucatan]}\n" +
                    "{[Store].[All Stores].[Mexico].[Zacatecas]}\n" +
                    "{[Store].[All Stores].[USA].[CA]}\n" +
                    "{[Store].[All Stores].[USA].[OR]}\n" +
                    "{[Store].[All Stores].[USA].[WA]}\n" +
                    "Axis #2:\n" +
                    "{[Time].[1st Half Sales]}\n" +
                    "{[Time].[2nd Half Sales]}\n" +
                    "{[Time].[Difference]}\n" +
                    "Row #0: \n" +
                    "Row #0: \n" +
                    "Row #0: \n" +
                    "Row #0: \n" +
                    "Row #0: \n" +
                    "Row #0: \n" +
                    "Row #0: \n" +
                    "Row #0: 74,571.95\n" +
                    "Row #0: 71,943.17\n" +
                    "Row #0: 125,779.50\n" +
                    "Row #1: \n" +
                    "Row #1: \n" +
                    "Row #1: \n" +
                    "Row #1: \n" +
                    "Row #1: \n" +
                    "Row #1: \n" +
                    "Row #1: \n" +
                    "Row #1: 84,595.89\n" +
                    "Row #1: 70,333.90\n" +
                    "Row #1: 138,013.72\n" +
                    "Row #2: \n" +
                    "Row #2: \n" +
                    "Row #2: \n" +
                    "Row #2: \n" +
                    "Row #2: \n" +
                    "Row #2: \n" +
                    "Row #2: \n" +
                    "Row #2: 10,023.94\n" +
                    "Row #2: -1,609.27\n" +
                    "Row #2: 12,234.22\n"));
    }

    public void testAggregate2AllMembers() {
        assertQueryReturns(
                    "WITH\n" +
                    "  MEMBER [Time].[1st Half Sales] AS 'Aggregate({Time.[1997].[Q1], Time.[1997].[Q2]})'\n" +
                    "  MEMBER [Time].[2nd Half Sales] AS 'Aggregate({Time.[1997].[Q3], Time.[1997].[Q4]})'\n" +
                    "  MEMBER [Time].[Difference] AS 'Time.[2nd Half Sales] - Time.[1st Half Sales]'\n" +
                    "SELECT\n" +
                    "   { [Store].[Store State].AllMembers} ON COLUMNS,\n" +
                    "   { Time.[1st Half Sales], Time.[2nd Half Sales], Time.[Difference]} ON ROWS\n" +
                    "FROM Sales\n" +
                    "WHERE [Measures].[Store Sales]",
                fold(
                    "Axis #0:\n" +
                    "{[Measures].[Store Sales]}\n" +
                    "Axis #1:\n" +
                    "{[Store].[All Stores].[Canada].[BC]}\n" +
                    "{[Store].[All Stores].[Mexico].[DF]}\n" +
                    "{[Store].[All Stores].[Mexico].[Guerrero]}\n" +
                    "{[Store].[All Stores].[Mexico].[Jalisco]}\n" +
                    "{[Store].[All Stores].[Mexico].[Veracruz]}\n" +
                    "{[Store].[All Stores].[Mexico].[Yucatan]}\n" +
                    "{[Store].[All Stores].[Mexico].[Zacatecas]}\n" +
                    "{[Store].[All Stores].[USA].[CA]}\n" +
                    "{[Store].[All Stores].[USA].[OR]}\n" +
                    "{[Store].[All Stores].[USA].[WA]}\n" +
                    "Axis #2:\n" +
                    "{[Time].[1st Half Sales]}\n" +
                    "{[Time].[2nd Half Sales]}\n" +
                    "{[Time].[Difference]}\n" +
                    "Row #0: \n" +
                    "Row #0: \n" +
                    "Row #0: \n" +
                    "Row #0: \n" +
                    "Row #0: \n" +
                    "Row #0: \n" +
                    "Row #0: \n" +
                    "Row #0: 74,571.95\n" +
                    "Row #0: 71,943.17\n" +
                    "Row #0: 125,779.50\n" +
                    "Row #1: \n" +
                    "Row #1: \n" +
                    "Row #1: \n" +
                    "Row #1: \n" +
                    "Row #1: \n" +
                    "Row #1: \n" +
                    "Row #1: \n" +
                    "Row #1: 84,595.89\n" +
                    "Row #1: 70,333.90\n" +
                    "Row #1: 138,013.72\n" +
                    "Row #2: \n" +
                    "Row #2: \n" +
                    "Row #2: \n" +
                    "Row #2: \n" +
                    "Row #2: \n" +
                    "Row #2: \n" +
                    "Row #2: \n" +
                    "Row #2: 10,023.94\n" +
                    "Row #2: -1,609.27\n" +
                    "Row #2: 12,234.22\n"));
    }


    public void testAggregateToSimulateCompoundSlicer() {
        assertQueryReturns(
                    "WITH MEMBER [Time].[1997 H1] as 'Aggregate({[Time].[1997].[Q1], [Time].[1997].[Q2]})'\n" +
                    "  MEMBER [Education Level].[College or higher] as 'Aggregate({[Education Level].[Bachelors Degree], [Education Level].[Graduate Degree]})'\n" +
                    "SELECT {[Measures].[Unit Sales], [Measures].[Store Sales]} on columns,\n" +
                    "  {[Product].children} on rows\n" +
                    "FROM [Sales]\n" +
                    "WHERE ([Time].[1997 H1], [Education Level].[College or higher], [Gender].[F])",
                fold(
                    "Axis #0:\n" +
                    "{[Time].[1997 H1], [Education Level].[College or higher], [Gender].[All Gender].[F]}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Unit Sales]}\n" +
                    "{[Measures].[Store Sales]}\n" +
                    "Axis #2:\n" +
                    "{[Product].[All Products].[Drink]}\n" +
                    "{[Product].[All Products].[Food]}\n" +
                    "{[Product].[All Products].[Non-Consumable]}\n" +
                    "Row #0: 1,797\n" +
                    "Row #0: 3,620.49\n" +
                    "Row #1: 15,002\n" +
                    "Row #1: 31,931.88\n" +
                    "Row #2: 3,845\n" +
                    "Row #2: 8,173.22\n"));
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
                    "with member [Measures].[Promo Count] as \n" +
                    " ' Count(Crossjoin({[Measures].[Unit Sales]},\n" +
                    " {[Promotion Media].[Media Type].members}), EXCLUDEEMPTY)'\n" +
                    "select {[Measures].[Unit Sales], [Measures].[Promo Count]} on columns,\n" +
                    " {[Product].[Drink].[Beverages].[Carbonated Beverages].[Soda].children} on rows\n" +
                    "from Sales",
                fold(
                    "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Unit Sales]}\n" +
                    "{[Measures].[Promo Count]}\n" +
                    "Axis #2:\n" +
                    "{[Product].[All Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Excellent]}\n" +
                    "{[Product].[All Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Fabulous]}\n" +
                    "{[Product].[All Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Skinner]}\n" +
                    "{[Product].[All Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Token]}\n" +
                    "{[Product].[All Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Washington]}\n" +
                    "Row #0: 738\n" +
                    "Row #0: 14\n" +
                    "Row #1: 632\n" +
                    "Row #1: 13\n" +
                    "Row #2: 655\n" +
                    "Row #2: 14\n" +
                    "Row #3: 735\n" +
                    "Row #3: 14\n" +
                    "Row #4: 647\n" +
                    "Row #4: 12\n"));

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
                    "WITH\n" +
                    "   MEMBER [Time].[1st Half Sales] AS 'Sum({[Time].[1997].[Q1], [Time].[1997].[Q2]})'\n" +
                    "   MEMBER [Time].[2nd Half Sales] AS 'Sum({[Time].[1997].[Q3], [Time].[1997].[Q4]})'\n" +
                    "   MEMBER [Time].[Median] AS 'Median(Time.Members)'\n" +
                    "SELECT\n" +
                    "   NON EMPTY { [Store].[Store Name].Members} ON COLUMNS,\n" +
                    "   { [Time].[1st Half Sales], [Time].[2nd Half Sales], [Time].[Median]} ON ROWS\n" +
                    "FROM Sales\n" +
                    "WHERE [Measures].[Store Sales]",

                fold(
                    "Axis #0:\n" +
                    "{[Measures].[Store Sales]}\n" +
                    "Axis #1:\n" +
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
                    "Axis #2:\n" +
                    "{[Time].[1st Half Sales]}\n" +
                    "{[Time].[2nd Half Sales]}\n" +
                    "{[Time].[Median]}\n" +
                    "Row #0: 20,801.04\n" +
                    "Row #0: 25,421.41\n" +
                    "Row #0: 26,275.11\n" +
                    "Row #0: 2,074.39\n" +
                    "Row #0: 28,519.18\n" +
                    "Row #0: 43,423.99\n" +
                    "Row #0: 2,140.99\n" +
                    "Row #0: 25,502.08\n" +
                    "Row #0: 25,293.50\n" +
                    "Row #0: 23,265.53\n" +
                    "Row #0: 34,926.91\n" +
                    "Row #0: 2,159.60\n" +
                    "Row #0: 12,490.89\n" +
                    "Row #1: 24,949.20\n" +
                    "Row #1: 29,123.87\n" +
                    "Row #1: 28,156.03\n" +
                    "Row #1: 2,366.79\n" +
                    "Row #1: 26,539.61\n" +
                    "Row #1: 43,794.29\n" +
                    "Row #1: 2,598.24\n" +
                    "Row #1: 27,394.22\n" +
                    "Row #1: 27,350.57\n" +
                    "Row #1: 26,368.93\n" +
                    "Row #1: 39,917.05\n" +
                    "Row #1: 2,546.37\n" +
                    "Row #1: 11,838.34\n" +
                    "Row #2: 4,577.35\n" +
                    "Row #2: 5,211.38\n" +
                    "Row #2: 4,722.87\n" +
                    "Row #2: 398.24\n" +
                    "Row #2: 5,039.50\n" +
                    "Row #2: 7,374.59\n" +
                    "Row #2: 410.22\n" +
                    "Row #2: 4,924.04\n" +
                    "Row #2: 4,569.13\n" +
                    "Row #2: 4,511.68\n" +
                    "Row #2: 6,630.91\n" +
                    "Row #2: 419.51\n" +
                    "Row #2: 2,169.48\n"));
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
                fold(
                    "[Store].[All Stores].[USA].[CA]\n" +
                    "[Store].[All Stores].[USA]\n" +
                    "[Store].[All Stores]"));
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
                fold(
                    "[Promotion Media].[All Media].[Radio]\n" +
                    "[Promotion Media].[All Media].[Sunday Paper, Radio, TV]"));
    }
    //todo: test unordered

    public void testBottomPercent() {
        assertAxisReturns("BottomPercent(Filter({[Store].[All Stores].[USA].[CA].Children, [Store].[All Stores].[USA].[OR].Children, [Store].[All Stores].[USA].[WA].Children}, ([Measures].[Unit Sales] > 0.0)), 100.0, [Measures].[Store Sales])",
                fold(
                    "[Store].[All Stores].[USA].[CA].[San Francisco]\n" +
                    "[Store].[All Stores].[USA].[WA].[Walla Walla]\n" +
                    "[Store].[All Stores].[USA].[WA].[Bellingham]\n" +
                    "[Store].[All Stores].[USA].[WA].[Yakima]\n" +
                    "[Store].[All Stores].[USA].[CA].[Beverly Hills]\n" +
                    "[Store].[All Stores].[USA].[WA].[Spokane]\n" +
                    "[Store].[All Stores].[USA].[WA].[Seattle]\n" +
                    "[Store].[All Stores].[USA].[WA].[Bremerton]\n" +
                    "[Store].[All Stores].[USA].[CA].[San Diego]\n" +
                    "[Store].[All Stores].[USA].[CA].[Los Angeles]\n" +
                    "[Store].[All Stores].[USA].[OR].[Portland]\n" +
                    "[Store].[All Stores].[USA].[WA].[Tacoma]\n" +
                    "[Store].[All Stores].[USA].[OR].[Salem]"));

        assertAxisReturns("BottomPercent({[Promotion Media].[Media Type].members}, 1, [Measures].[Unit Sales])",
                fold(
                    "[Promotion Media].[All Media].[Radio]\n" +
                    "[Promotion Media].[All Media].[Sunday Paper, Radio, TV]"));
    }
    //todo: test precision

    public void testBottomSum() {
        assertAxisReturns("BottomSum({[Promotion Media].[Media Type].members}, 5000, [Measures].[Unit Sales])",
                fold(
                    "[Promotion Media].[All Media].[Radio]\n" +
                    "[Promotion Media].[All Media].[Sunday Paper, Radio, TV]"));
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
                fold(
                    "Except(CROSSJOIN({[Promotion Media].[All Media]},\n" +
                    "                  [Product].[All Products].Children),\n" +
                    "       CROSSJOIN({[Promotion Media].[All Media]},\n" +
                    "                  {[Product].[All Products].[Drink]}))" ),
                fold(
                    "{[Promotion Media].[All Media], [Product].[All Products].[Food]}\n" +
                    "{[Promotion Media].[All Media], [Product].[All Products].[Non-Consumable]}" ));
    }

    /**
     * Tests that TopPercent() operates succesfully on a
     * axis of crossjoined tuples.  previously, this would
     * fail with a ClassCastException in FunUtil.java.  bug 1440306
     */
    public void testTopPercentCrossjoin() {
        assertAxisReturns(
                fold(
                    "{TopPercent(Crossjoin([Product].[Product Department].members,\n" +
                    "[Time].[1997].children),10,[Measures].[Store Sales])}" ),
                fold(
                    "{[Product].[All Products].[Food].[Produce], [Time].[1997].[Q4]}\n" +
                    "{[Product].[All Products].[Food].[Produce], [Time].[1997].[Q1]}\n" +
                    "{[Product].[All Products].[Food].[Produce], [Time].[1997].[Q3]}" ));
    }

    public void testCrossjoinNested() {
        assertAxisReturns(
                fold(
                    "  CrossJoin(\n" +
                    "    CrossJoin(\n" +
                    "      [Gender].members,\n" +
                    "      [Marital Status].members),\n" +
                    "   {[Store], [Store].children})"),

                fold(
                    "{[Gender].[All Gender], [Marital Status].[All Marital Status], [Store].[All Stores]}\n" +
                    "{[Gender].[All Gender], [Marital Status].[All Marital Status], [Store].[All Stores].[Canada]}\n" +
                    "{[Gender].[All Gender], [Marital Status].[All Marital Status], [Store].[All Stores].[Mexico]}\n" +
                    "{[Gender].[All Gender], [Marital Status].[All Marital Status], [Store].[All Stores].[USA]}\n" +
                    "{[Gender].[All Gender], [Marital Status].[All Marital Status].[M], [Store].[All Stores]}\n" +
                    "{[Gender].[All Gender], [Marital Status].[All Marital Status].[M], [Store].[All Stores].[Canada]}\n" +
                    "{[Gender].[All Gender], [Marital Status].[All Marital Status].[M], [Store].[All Stores].[Mexico]}\n" +
                    "{[Gender].[All Gender], [Marital Status].[All Marital Status].[M], [Store].[All Stores].[USA]}\n" +
                    "{[Gender].[All Gender], [Marital Status].[All Marital Status].[S], [Store].[All Stores]}\n" +
                    "{[Gender].[All Gender], [Marital Status].[All Marital Status].[S], [Store].[All Stores].[Canada]}\n" +
                    "{[Gender].[All Gender], [Marital Status].[All Marital Status].[S], [Store].[All Stores].[Mexico]}\n" +
                    "{[Gender].[All Gender], [Marital Status].[All Marital Status].[S], [Store].[All Stores].[USA]}\n" +
                    "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status], [Store].[All Stores]}\n" +
                    "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status], [Store].[All Stores].[Canada]}\n" +
                    "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status], [Store].[All Stores].[Mexico]}\n" +
                    "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status], [Store].[All Stores].[USA]}\n" +
                    "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[M], [Store].[All Stores]}\n" +
                    "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[M], [Store].[All Stores].[Canada]}\n" +
                    "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[M], [Store].[All Stores].[Mexico]}\n" +
                    "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[M], [Store].[All Stores].[USA]}\n" +
                    "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S], [Store].[All Stores]}\n" +
                    "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S], [Store].[All Stores].[Canada]}\n" +
                    "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S], [Store].[All Stores].[Mexico]}\n" +
                    "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S], [Store].[All Stores].[USA]}\n" +
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status], [Store].[All Stores]}\n" +
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status], [Store].[All Stores].[Canada]}\n" +
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status], [Store].[All Stores].[Mexico]}\n" +
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status], [Store].[All Stores].[USA]}\n" +
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M], [Store].[All Stores]}\n" +
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M], [Store].[All Stores].[Canada]}\n" +
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M], [Store].[All Stores].[Mexico]}\n" +
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M], [Store].[All Stores].[USA]}\n" +
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Store].[All Stores]}\n" +
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Store].[All Stores].[Canada]}\n" +
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Store].[All Stores].[Mexico]}\n" +
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Store].[All Stores].[USA]}"));
    }

    public void testCrossjoinSingletonTuples() {
        assertAxisReturns("CrossJoin({([Gender].[M])}, {([Marital Status].[S])})",
                "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S]}");
    }

    public void testCrossjoinSingletonTuplesNested() {
        assertAxisReturns(
                "CrossJoin({([Gender].[M])}, CrossJoin({([Marital Status].[S])}, [Store].children))",
                fold(
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Store].[All Stores].[Canada]}\n" +
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Store].[All Stores].[Mexico]}\n" +
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Store].[All Stores].[USA]}"));
    }

    public void testCrossjoinAsterisk() {
        assertAxisReturns("{[Gender].[M]} * {[Marital Status].[S]}",
                "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S]}");
    }

    public void testCrossjoinAsteriskAssoc() {
        assertAxisReturns("Order({[Gender].Children} * {[Marital Status].Children} * {[Time].[1997].[Q2].Children}," +
                "[Measures].[Unit Sales])",
                fold(
                    "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[M], [Time].[1997].[Q2].[4]}\n" +
                    "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[M], [Time].[1997].[Q2].[6]}\n" +
                    "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[M], [Time].[1997].[Q2].[5]}\n" +
                    "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S], [Time].[1997].[Q2].[4]}\n" +
                    "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S], [Time].[1997].[Q2].[5]}\n" +
                    "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S], [Time].[1997].[Q2].[6]}\n" +
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M], [Time].[1997].[Q2].[4]}\n" +
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M], [Time].[1997].[Q2].[5]}\n" +
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M], [Time].[1997].[Q2].[6]}\n" +
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Time].[1997].[Q2].[6]}\n" +
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Time].[1997].[Q2].[4]}\n" +
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Time].[1997].[Q2].[5]}"));
    }

    public void testCrossjoinAsteriskInsideBraces() {
        assertAxisReturns("{[Gender].[M] * [Marital Status].[S] * [Time].[1997].[Q2].Children}",
                fold(
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Time].[1997].[Q2].[4]}\n" +
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Time].[1997].[Q2].[5]}\n" +
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Time].[1997].[Q2].[6]}"));
    }

    public void testCrossJoinAsteriskQuery() {
        assertQueryReturns(
                    "SELECT {[Measures].members * [1997].children} ON COLUMNS,\n" +
                    " {[Store].[USA].children * [Position].[All Position].children} DIMENSION PROPERTIES [Store].[Store SQFT] ON ROWS\n" +
                    "FROM [HR]",
                fold(
                    "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Org Salary], [Time].[1997].[Q1]}\n" +
                    "{[Measures].[Org Salary], [Time].[1997].[Q2]}\n" +
                    "{[Measures].[Org Salary], [Time].[1997].[Q3]}\n" +
                    "{[Measures].[Org Salary], [Time].[1997].[Q4]}\n" +
                    "{[Measures].[Count], [Time].[1997].[Q1]}\n" +
                    "{[Measures].[Count], [Time].[1997].[Q2]}\n" +
                    "{[Measures].[Count], [Time].[1997].[Q3]}\n" +
                    "{[Measures].[Count], [Time].[1997].[Q4]}\n" +
                    "{[Measures].[Number of Employees], [Time].[1997].[Q1]}\n" +
                    "{[Measures].[Number of Employees], [Time].[1997].[Q2]}\n" +
                    "{[Measures].[Number of Employees], [Time].[1997].[Q3]}\n" +
                    "{[Measures].[Number of Employees], [Time].[1997].[Q4]}\n" +
                    "Axis #2:\n" +
                    "{[Store].[All Stores].[USA].[CA], [Position].[All Position].[Middle Management]}\n" +
                    "{[Store].[All Stores].[USA].[CA], [Position].[All Position].[Senior Management]}\n" +
                    "{[Store].[All Stores].[USA].[CA], [Position].[All Position].[Store Full Time Staf]}\n" +
                    "{[Store].[All Stores].[USA].[CA], [Position].[All Position].[Store Management]}\n" +
                    "{[Store].[All Stores].[USA].[CA], [Position].[All Position].[Store Temp Staff]}\n" +
                    "{[Store].[All Stores].[USA].[OR], [Position].[All Position].[Middle Management]}\n" +
                    "{[Store].[All Stores].[USA].[OR], [Position].[All Position].[Senior Management]}\n" +
                    "{[Store].[All Stores].[USA].[OR], [Position].[All Position].[Store Full Time Staf]}\n" +
                    "{[Store].[All Stores].[USA].[OR], [Position].[All Position].[Store Management]}\n" +
                    "{[Store].[All Stores].[USA].[OR], [Position].[All Position].[Store Temp Staff]}\n" +
                    "{[Store].[All Stores].[USA].[WA], [Position].[All Position].[Middle Management]}\n" +
                    "{[Store].[All Stores].[USA].[WA], [Position].[All Position].[Senior Management]}\n" +
                    "{[Store].[All Stores].[USA].[WA], [Position].[All Position].[Store Full Time Staf]}\n" +
                    "{[Store].[All Stores].[USA].[WA], [Position].[All Position].[Store Management]}\n" +
                    "{[Store].[All Stores].[USA].[WA], [Position].[All Position].[Store Temp Staff]}\n" +
                    "Row #0: $275.40\n" +
                    "Row #0: $275.40\n" +
                    "Row #0: $275.40\n" +
                    "Row #0: $275.40\n" +
                    "Row #0: 27\n" +
                    "Row #0: 27\n" +
                    "Row #0: 27\n" +
                    "Row #0: 27\n" +
                    "Row #0: 9\n" +
                    "Row #0: 9\n" +
                    "Row #0: 9\n" +
                    "Row #0: 9\n" +
                    "Row #1: $837.00\n" +
                    "Row #1: $837.00\n" +
                    "Row #1: $837.00\n" +
                    "Row #1: $837.00\n" +
                    "Row #1: 24\n" +
                    "Row #1: 24\n" +
                    "Row #1: 24\n" +
                    "Row #1: 24\n" +
                    "Row #1: 8\n" +
                    "Row #1: 8\n" +
                    "Row #1: 8\n" +
                    "Row #1: 8\n" +
                    "Row #2: $1,728.45\n" +
                    "Row #2: $1,727.02\n" +
                    "Row #2: $1,727.72\n" +
                    "Row #2: $1,726.55\n" +
                    "Row #2: 357\n" +
                    "Row #2: 357\n" +
                    "Row #2: 357\n" +
                    "Row #2: 357\n" +
                    "Row #2: 119\n" +
                    "Row #2: 119\n" +
                    "Row #2: 119\n" +
                    "Row #2: 119\n" +
                    "Row #3: $473.04\n" +
                    "Row #3: $473.04\n" +
                    "Row #3: $473.04\n" +
                    "Row #3: $473.04\n" +
                    "Row #3: 51\n" +
                    "Row #3: 51\n" +
                    "Row #3: 51\n" +
                    "Row #3: 51\n" +
                    "Row #3: 17\n" +
                    "Row #3: 17\n" +
                    "Row #3: 17\n" +
                    "Row #3: 17\n" +
                    "Row #4: $401.35\n" +
                    "Row #4: $405.73\n" +
                    "Row #4: $400.61\n" +
                    "Row #4: $402.31\n" +
                    "Row #4: 120\n" +
                    "Row #4: 120\n" +
                    "Row #4: 120\n" +
                    "Row #4: 120\n" +
                    "Row #4: 40\n" +
                    "Row #4: 40\n" +
                    "Row #4: 40\n" +
                    "Row #4: 40\n" +
                    "Row #5: \n" +
                    "Row #5: \n" +
                    "Row #5: \n" +
                    "Row #5: \n" +
                    "Row #5: \n" +
                    "Row #5: \n" +
                    "Row #5: \n" +
                    "Row #5: \n" +
                    "Row #5: \n" +
                    "Row #5: \n" +
                    "Row #5: \n" +
                    "Row #5: \n" +
                    "Row #6: \n" +
                    "Row #6: \n" +
                    "Row #6: \n" +
                    "Row #6: \n" +
                    "Row #6: \n" +
                    "Row #6: \n" +
                    "Row #6: \n" +
                    "Row #6: \n" +
                    "Row #6: \n" +
                    "Row #6: \n" +
                    "Row #6: \n" +
                    "Row #6: \n" +
                    "Row #7: $1,343.62\n" +
                    "Row #7: $1,342.61\n" +
                    "Row #7: $1,342.57\n" +
                    "Row #7: $1,343.65\n" +
                    "Row #7: 279\n" +
                    "Row #7: 279\n" +
                    "Row #7: 279\n" +
                    "Row #7: 279\n" +
                    "Row #7: 93\n" +
                    "Row #7: 93\n" +
                    "Row #7: 93\n" +
                    "Row #7: 93\n" +
                    "Row #8: $286.74\n" +
                    "Row #8: $286.74\n" +
                    "Row #8: $286.74\n" +
                    "Row #8: $286.74\n" +
                    "Row #8: 30\n" +
                    "Row #8: 30\n" +
                    "Row #8: 30\n" +
                    "Row #8: 30\n" +
                    "Row #8: 10\n" +
                    "Row #8: 10\n" +
                    "Row #8: 10\n" +
                    "Row #8: 10\n" +
                    "Row #9: $333.20\n" +
                    "Row #9: $332.65\n" +
                    "Row #9: $331.28\n" +
                    "Row #9: $332.43\n" +
                    "Row #9: 99\n" +
                    "Row #9: 99\n" +
                    "Row #9: 99\n" +
                    "Row #9: 99\n" +
                    "Row #9: 33\n" +
                    "Row #9: 33\n" +
                    "Row #9: 33\n" +
                    "Row #9: 33\n" +
                    "Row #10: \n" +
                    "Row #10: \n" +
                    "Row #10: \n" +
                    "Row #10: \n" +
                    "Row #10: \n" +
                    "Row #10: \n" +
                    "Row #10: \n" +
                    "Row #10: \n" +
                    "Row #10: \n" +
                    "Row #10: \n" +
                    "Row #10: \n" +
                    "Row #10: \n" +
                    "Row #11: \n" +
                    "Row #11: \n" +
                    "Row #11: \n" +
                    "Row #11: \n" +
                    "Row #11: \n" +
                    "Row #11: \n" +
                    "Row #11: \n" +
                    "Row #11: \n" +
                    "Row #11: \n" +
                    "Row #11: \n" +
                    "Row #11: \n" +
                    "Row #11: \n" +
                    "Row #12: $2,768.60\n" +
                    "Row #12: $2,769.18\n" +
                    "Row #12: $2,766.78\n" +
                    "Row #12: $2,769.50\n" +
                    "Row #12: 579\n" +
                    "Row #12: 579\n" +
                    "Row #12: 579\n" +
                    "Row #12: 579\n" +
                    "Row #12: 193\n" +
                    "Row #12: 193\n" +
                    "Row #12: 193\n" +
                    "Row #12: 193\n" +
                    "Row #13: $736.29\n" +
                    "Row #13: $736.29\n" +
                    "Row #13: $736.29\n" +
                    "Row #13: $736.29\n" +
                    "Row #13: 81\n" +
                    "Row #13: 81\n" +
                    "Row #13: 81\n" +
                    "Row #13: 81\n" +
                    "Row #13: 27\n" +
                    "Row #13: 27\n" +
                    "Row #13: 27\n" +
                    "Row #13: 27\n" +
                    "Row #14: $674.70\n" +
                    "Row #14: $674.54\n" +
                    "Row #14: $676.25\n" +
                    "Row #14: $676.48\n" +
                    "Row #14: 201\n" +
                    "Row #14: 201\n" +
                    "Row #14: 201\n" +
                    "Row #14: 201\n" +
                    "Row #14: 67\n" +
                    "Row #14: 67\n" +
                    "Row #14: 67\n" +
                    "Row #14: 67\n"));
    }

    public void testDescendantsM() {
        assertAxisReturns("Descendants([Time].[1997].[Q1])",
                fold(
                    "[Time].[1997].[Q1]\n" +
                    "[Time].[1997].[Q1].[1]\n" +
                    "[Time].[1997].[Q1].[2]\n" +
                    "[Time].[1997].[Q1].[3]"));
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
        StringBuilder buf = new StringBuilder("{");
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
            fold("[Gender].[All Gender].[F]\n" +
                "[Gender].[All Gender].[M]"));
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
            fold("[Geography].[All Geographys].[Israel].[Israel].[Haifa]\n" +
                "[Geography].[All Geographys].[Israel].[Israel].[Tel Aviv]"));

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
                fold(
                    "[Time].[1997].[Q2].[4]\n" +
                    "[Time].[1997].[Q2].[5]\n" +
                    "[Time].[1997].[Q2].[6]"));

        // leaves at depth 0 returns the member itself
        assertAxisReturns("Descendants([Time].[1997].[Q2], 0, Leaves)",
                fold(
                    "[Time].[1997].[Q2].[4]\n" +
                    "[Time].[1997].[Q2].[5]\n" +
                    "[Time].[1997].[Q2].[6]"));

        assertAxisReturns("Descendants([Time].[1997].[Q2], 3, Leaves)",
                fold(
                    "[Time].[1997].[Q2].[4]\n" +
                    "[Time].[1997].[Q2].[5]\n" +
                    "[Time].[1997].[Q2].[6]"));
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
                fold(
                    "[Employees].[All Employees].[Sheri Nowmer].[Derrick Whelply]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Maya Gutierrez]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Roberta Damstra]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Rebecca Kanagaki]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Donna Arnold]"));
    }

    public void testDescendantsParentChildBefore() {
        getTestContext("HR").assertAxisReturns(
                "Descendants([Employees], 2, BEFORE)",
                fold(
                    "[Employees].[All Employees]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer]"));
    }

    public void testDescendantsParentChildLeaves() {
        final TestContext testContext = getTestContext("HR");

        // leaves, restricted by level
        testContext.assertAxisReturns(
                "Descendants([Employees].[All Employees].[Sheri Nowmer].[Michael Spence], [Employees].[Employee Id], LEAVES)",
                fold(
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[John Brooks]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Todd Logan]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Joshua Several]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[James Thomas]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Robert Vessa]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Bronson Jacobs]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Rebecca Barley]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Emilio Alvaro]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Becky Waters]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[A. Joyce Jarvis]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Ruby Sue Styles]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Lisa Roy]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Ingrid Burkhardt]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Todd Whitney]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Barbara Wisnewski]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Karren Burkhardt]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[John Long]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Edwin Olenzek]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Jessie Valerio]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Robert Ahlering]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Megan Burke]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Mary Sandidge].[Karel Bates]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[James Tran]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Shelley Crow]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Anne Sims]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Clarence Tatman]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Jan Nelsen]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Jeanie Glenn]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Peggy Smith]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Tish Duff]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Anita Lucero]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Stephen Burton]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Amy Consentino]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Stacie Mcanich]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Mary Browning]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Alexandra Wellington]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Cory Bacugalupi]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Stacy Rizzi]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Mike White]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Marty Simpson]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Robert Jones]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Raul Casts]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Bridget Browqett]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Monk Skonnard].[Kay Kartz]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Jeanette Cole]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Phyllis Huntsman]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Hannah Arakawa]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Wathalee Steuber]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Pamela Cox]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Helen Lutes]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Linda Ecoffey]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Katherine Swint]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Dianne Slattengren]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Ronald Heymsfield]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Steven Whitehead]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[William Sotelo]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Beth Stanley]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Jill Markwood]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Mildred Valentine]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Suzann Reams]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Audrey Wold]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Susan French]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Trish Pederson]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Eric Renn]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Elizabeth Catalano]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Christopher Beck].[Eric Coleman]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Catherine Abel]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Emilo Miller]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Daniel Wolter].[Michael John Troyer].[Hazel Walker]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Dianne Collins].[Lawrence Hurkett].[Sara Pettengill].[Linda Blasingame]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Dianne Collins].[Lawrence Hurkett].[Sara Pettengill].[Jackie Blackwell]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Dianne Collins].[Lawrence Hurkett].[Sara Pettengill].[John Ortiz]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Dianne Collins].[Lawrence Hurkett].[Sara Pettengill].[Stacey Tearpak]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Dianne Collins].[Lawrence Hurkett].[Sara Pettengill].[Fannye Weber]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Dianne Collins].[Lawrence Hurkett].[Sara Pettengill].[Diane Kabbes]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Dianne Collins].[Lawrence Hurkett].[Sara Pettengill].[Brenda Heaney]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Dianne Collins].[Lawrence Hurkett].[Sara Pettengill].[Judith Karavites]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Dianne Collins].[Lawrence Hurkett].[Jauna Elson]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Dianne Collins].[Lawrence Hurkett].[Nancy Hirota]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Dianne Collins].[Lawrence Hurkett].[Marie Moya]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Dianne Collins].[Lawrence Hurkett].[Nicky Chesnut]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Dianne Collins].[Lawrence Hurkett].[Karen Hall]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Dianne Collins].[Lawrence Hurkett].[Greg Narberes]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Dianne Collins].[Lawrence Hurkett].[Anna Townsend]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence].[Dianne Collins].[Lawrence Hurkett].[Carol Ann Rockne]"));

        // leaves, restricted by depth
        testContext.assertAxisReturns(
                "Descendants([Employees], 1, LEAVES)", "");
        testContext.assertAxisReturns(
                "Descendants([Employees], 2, LEAVES)",
                fold(
                    "[Employees].[All Employees].[Sheri Nowmer].[Roberta Damstra].[Jennifer Cooper]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Roberta Damstra].[Peggy Petty]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Roberta Damstra].[Jessica Olguin]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Roberta Damstra].[Phyllis Burchett]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Rebecca Kanagaki].[Juanita Sharp]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Rebecca Kanagaki].[Sandra Brunner]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz].[Ernest Staton]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz].[Rose Sims]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz].[Lauretta De Carlo]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz].[Mary Williams]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz].[Terri Burke]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz].[Audrey Osborn]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz].[Brian Binai]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz].[Concepcion Lozada]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Donna Arnold].[Howard Bechard]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Donna Arnold].[Doris Carter]"));

        testContext.assertAxisReturns(
                "Descendants([Employees], 3, LEAVES)",
                fold(
                    "[Employees].[All Employees].[Sheri Nowmer].[Roberta Damstra].[Jennifer Cooper]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Roberta Damstra].[Peggy Petty]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Roberta Damstra].[Jessica Olguin]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Roberta Damstra].[Phyllis Burchett]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Rebecca Kanagaki].[Juanita Sharp]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Rebecca Kanagaki].[Sandra Brunner]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz].[Ernest Staton]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz].[Rose Sims]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz].[Lauretta De Carlo]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz].[Mary Williams]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz].[Terri Burke]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz].[Audrey Osborn]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz].[Brian Binai]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz].[Concepcion Lozada]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Donna Arnold].[Howard Bechard]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Donna Arnold].[Doris Carter]"));

        // note that depth is RELATIVE to the starting member
        testContext.assertAxisReturns(
                "Descendants([Employees].[All Employees].[Sheri Nowmer].[Roberta Damstra], 1, LEAVES)",
                fold(
                    "[Employees].[All Employees].[Sheri Nowmer].[Roberta Damstra].[Jennifer Cooper]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Roberta Damstra].[Peggy Petty]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Roberta Damstra].[Jessica Olguin]\n" +
                    "[Employees].[All Employees].[Sheri Nowmer].[Roberta Damstra].[Phyllis Burchett]"));

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
                fold(
                    "[Time].[1997].[Q1].[2]\n" +
                    "[Time].[1997].[Q1].[3]\n" +
                    "[Time].[1997].[Q2].[4]\n" +
                    "[Time].[1997].[Q2].[5]")); // not parents
    }

    /**
     * Large dimensions use a different member reader, therefore need to
     * be tested separately.
     */
    public void testRangeLarge() {
        assertAxisReturns("[Customers].[USA].[CA].[San Francisco] : [Customers].[USA].[WA].[Bellingham]",
                fold(
                    "[Customers].[All Customers].[USA].[CA].[San Francisco]\n" +
                    "[Customers].[All Customers].[USA].[CA].[San Gabriel]\n" +
                    "[Customers].[All Customers].[USA].[CA].[San Jose]\n" +
                    "[Customers].[All Customers].[USA].[CA].[Santa Cruz]\n" +
                    "[Customers].[All Customers].[USA].[CA].[Santa Monica]\n" +
                    "[Customers].[All Customers].[USA].[CA].[Spring Valley]\n" +
                    "[Customers].[All Customers].[USA].[CA].[Torrance]\n" +
                    "[Customers].[All Customers].[USA].[CA].[West Covina]\n" +
                    "[Customers].[All Customers].[USA].[CA].[Woodland Hills]\n" +
                    "[Customers].[All Customers].[USA].[OR].[Albany]\n" +
                    "[Customers].[All Customers].[USA].[OR].[Beaverton]\n" +
                    "[Customers].[All Customers].[USA].[OR].[Corvallis]\n" +
                    "[Customers].[All Customers].[USA].[OR].[Lake Oswego]\n" +
                    "[Customers].[All Customers].[USA].[OR].[Lebanon]\n" +
                    "[Customers].[All Customers].[USA].[OR].[Milwaukie]\n" +
                    "[Customers].[All Customers].[USA].[OR].[Oregon City]\n" +
                    "[Customers].[All Customers].[USA].[OR].[Portland]\n" +
                    "[Customers].[All Customers].[USA].[OR].[Salem]\n" +
                    "[Customers].[All Customers].[USA].[OR].[W. Linn]\n" +
                    "[Customers].[All Customers].[USA].[OR].[Woodburn]\n" +
                    "[Customers].[All Customers].[USA].[WA].[Anacortes]\n" +
                    "[Customers].[All Customers].[USA].[WA].[Ballard]\n" +
                    "[Customers].[All Customers].[USA].[WA].[Bellingham]"));
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
                fold(
                    "[Time].[1997].[Q2].[5]\n" +
                    "[Time].[1997].[Q2].[6]\n" +
                    "[Time].[1997].[Q3].[7]")); // same as if reversed
    }

    public void testRangeEndBeforeStartLarge() {
        assertAxisReturns("[Customers].[USA].[WA] : [Customers].[USA].[CA]",
                fold(
                    "[Customers].[All Customers].[USA].[CA]\n" +
                    "[Customers].[All Customers].[USA].[OR]\n" +
                    "[Customers].[All Customers].[USA].[WA]"));
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
                    "WITH MEMBER [Product].[All Products].[Non-Consumable].[Other] AS\n" +
                    " 'Sum( Except( [Product].[Product Department].Members,\n" +
                    "       TopCount( [Product].[Product Department].Members, 3 )),\n" +
                    "       Measures.[Unit Sales] )'\n" +
                    "SELECT\n" +
                    "  { [Measures].[Unit Sales] } ON COLUMNS,\n" +
                    "  { TopCount( [Product].[Product Department].Members,3 ),\n" +
                    "              [Product].[All Products].[Non-Consumable].[Other] } ON ROWS\n" +
                    "FROM [Sales]",
                fold(
                    "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Unit Sales]}\n" +
                    "Axis #2:\n" +
                    "{[Product].[All Products].[Drink].[Alcoholic Beverages]}\n" +
                    "{[Product].[All Products].[Drink].[Beverages]}\n" +
                    "{[Product].[All Products].[Drink].[Dairy]}\n" +
                    "{[Product].[All Products].[Non-Consumable].[Other]}\n" +
                    "Row #0: 6,838\n" +
                    "Row #1: 13,573\n" +
                    "Row #2: 4,186\n" +
                    "Row #3: 242,176\n"));
    }

    public void testBug714707() {
        // Same issue as bug 715177 -- "children" returns immutable
        // list, which set operator must make mutable.
        assertAxisReturns(
                "{[Store].[USA].[CA].children, [Store].[USA]}",
                fold(
                    "[Store].[All Stores].[USA].[CA].[Alameda]\n" +
                    "[Store].[All Stores].[USA].[CA].[Beverly Hills]\n" +
                    "[Store].[All Stores].[USA].[CA].[Los Angeles]\n" +
                    "[Store].[All Stores].[USA].[CA].[San Diego]\n" +
                    "[Store].[All Stores].[USA].[CA].[San Francisco]\n" +
                    "[Store].[All Stores].[USA]"));
    }

    public void testBug715177c() {
        assertAxisReturns("Order(TopCount({[Store].[USA].[CA].children}, [Measures].[Unit Sales], 2), [Measures].[Unit Sales])",
                fold(
                    "[Store].[All Stores].[USA].[CA].[Alameda]\n" +
                    "[Store].[All Stores].[USA].[CA].[San Francisco]\n" +
                    "[Store].[All Stores].[USA].[CA].[Beverly Hills]\n" +
                    "[Store].[All Stores].[USA].[CA].[San Diego]\n" +
                    "[Store].[All Stores].[USA].[CA].[Los Angeles]"));
    }

    public void testFormatFixed() {
        assertExprReturns("Format(12.2, \"#,##0.00\")", "12.20");
    }

    public void testFormatVariable() {
        assertExprReturns("Format(1234.5, \"#,#\" || \"#0.00\")", "1,234.50");
    }

    public void testFormatMember() {
        assertExprReturns("Format([Store].[USA].[CA], \"#,#\" || \"#0.00\")", "74,748.00");
    }

    public void testIIf() {
        assertExprReturns("IIf(([Measures].[Unit Sales],[Product].[Drink].[Alcoholic Beverages].[Beer and Wine]) > 100, \"Yes\",\"No\")",
            "Yes");
    }

    public void testIIfWithNullAndNumber()
    {
        assertExprReturns("IIf(([Measures].[Unit Sales],[Product].[Drink].[Alcoholic Beverages].[Beer and Wine]) > 100, null,20)",
                "");
        assertExprReturns("IIf(([Measures].[Unit Sales],[Product].[Drink].[Alcoholic Beverages].[Beer and Wine]) > 100, 20,null)",
                "20");
    }

    public void testIIfWithStringAndNull()
    {
        assertExprThrows("IIf(([Measures].[Unit Sales],[Product].[Drink].[Alcoholic Beverages].[Beer and Wine]) > 100, null,\"foo\")",
                "Failed to parse");
        assertExprThrows("IIf(([Measures].[Unit Sales],[Product].[Drink].[Alcoholic Beverages].[Beer and Wine]) > 100, \"foo\",null)",
                "Failed to parse");
    }

    public void testIsEmptyWithNull()
    {
        assertExprReturns("iif (isempty(null), \"is empty\", \"not is empty\")", "is empty");
        assertExprReturns("iif (isempty(null), 1, 2)", "1");
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
                    "with\n" +
                    "    member Measures.[Coal1] as 'coalesceempty(([Time].[1997], Measures.[Store Sales]), ([Time].[1998], Measures.[Store Sales]))'\n" +
                    "    member Measures.[Coal2] as 'coalesceempty(([Time].[1997], Measures.[Unit Sales]), ([Time].[1998], Measures.[Unit Sales]))'\n" +
                    "select \n" +
                    "    {Measures.[Coal1], Measures.[Coal2]} on columns,\n" +
                    "    {[Store].[All Stores].[Mexico].[DF], [Store].[All Stores].[USA].[WA]} on rows\n" +
                    "from \n" +
                    "    [Sales]");

        checkDataResults(
                new Double[][] {
                    {null, null},
                    {new Double(263793.22), new Double(124366)}
                },
                result,
                0.001);

        result = executeQuery(
                    "with\n" +
                    "    member Measures.[Sales Per Customer] as 'Measures.[Sales Count] / Measures.[Customer Count]'\n" +
                    "    member Measures.[Coal] as 'coalesceempty(([Measures].[Sales Per Customer], [Store].[All Stores].[Mexico].[DF]),\n" +
                    "        Measures.[Sales Per Customer])'\n" +
                    "select \n" +
                    "    {Measures.[Sales Per Customer], Measures.[Coal]} on columns,\n" +
                    "    {[Store].[All Stores].[Mexico].[DF], [Store].[All Stores].[USA].[WA]} on rows\n" +
                    "from \n" +
                    "    [Sales]\n" +
                    "where\n" +
                    "    ([Time].[1997].[Q2])");

        checkDataResults(new Double[][] {
            { null, null },
            { new Double(8.963), new Double(8.963) }
        },
                result,
                0.001);

        result = executeQuery(
                    "with\n" +
                    "    member Measures.[Sales Per Customer] as 'Measures.[Sales Count] / Measures.[Customer Count]'\n" +
                    "    member Measures.[Coal] as 'coalesceempty(([Measures].[Sales Per Customer], [Store].[All Stores].[Mexico].[DF]),\n" +
                    "        ([Measures].[Sales Per Customer], [Store].[All Stores].[Mexico].[DF]),\n" +
                    "        ([Measures].[Sales Per Customer], [Store].[All Stores].[Mexico].[DF]),\n" +
                    "        ([Measures].[Sales Per Customer], [Store].[All Stores].[Mexico].[DF]),\n" +
                    "        ([Measures].[Sales Per Customer], [Store].[All Stores].[Mexico].[DF]),\n" +
                    "        ([Measures].[Sales Per Customer], [Store].[All Stores].[Mexico].[DF]),\n" +
                    "        ([Measures].[Sales Per Customer], [Store].[All Stores].[Mexico].[DF]),\n" +
                    "        ([Measures].[Sales Per Customer], [Store].[All Stores].[Mexico].[DF]),\n" +
                    "        Measures.[Sales Per Customer])'\n" +
                    "select \n" +
                    "    {Measures.[Sales Per Customer], Measures.[Coal]} on columns,\n" +
                    "    {[Store].[All Stores].[Mexico].[DF], [Store].[All Stores].[USA].[WA]} on rows\n" +
                    "from \n" +
                    "    [Sales]\n" +
                    "where\n" +
                    "    ([Time].[1997].[Q2])");

        checkDataResults(new Double[][] {
            { null, null },
            { new Double(8.963), new Double(8.963) }
        },
                result,
                0.001);
    }

    public void testBrokenContextBug() {
        Result result = executeQuery(
                    "with\n" +
                    "    member Measures.[Sales Per Customer] as 'Measures.[Sales Count] / Measures.[Customer Count]'\n" +
                    "    member Measures.[Coal] as 'coalesceempty(([Measures].[Sales Per Customer], [Store].[All Stores].[Mexico].[DF]),\n" +
                    "        Measures.[Sales Per Customer])'\n" +
                    "select \n" +
                    "    {Measures.[Coal]} on columns,\n" +
                    "    {[Store].[All Stores].[USA].[WA]} on rows\n" +
                    "from \n" +
                    "    [Sales]\n" +
                    "where\n" +
                    "    ([Time].[1997].[Q2])");

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

    /**
     * Tests whether the tuple operator can be applied to arguments of various
     * types. See bug 1491699
     * "ClassCastException in mondrian.calc.impl.GenericCalc.evaluat".
     */
    public void testTupleArgTypes() {
        // can coerce dims to members
        assertExprReturns(
                "([Gender], [Time])",
                "266,773");

        // can coerce hierarchy to member
        assertExprReturns(
                "([Gender].[M], [Time.Weekly])",
                "135,215");

        // cannot coerce level to member
        assertAxisThrows(
                "{([Gender].[M], [Store].[Store City])}",
                "No function matches signature '(<Member>, <Level>)'");

        // coerce args (hierarchy, member, member, dimension)
        assertAxisReturns(
                "{([Time.Weekly], [Measures].[Store Sales], [Marital Status].[M], [Promotion Media])}",
                "{[Time.Weekly].[All Time.Weeklys].[1997], [Measures].[Store Sales], [Marital Status].[All Marital Status].[M], [Promotion Media].[All Media]}");

        // two usages of the [Time] dimension
        assertAxisThrows(
                "{([Time.Weekly], [Measures].[Store Sales], [Marital Status].[M], [Time])}",
                "Tuple contains more than one member of dimension '[Time]'.");

        // cannot coerce integer to member
        assertAxisThrows(
                "{([Gender].[M], 123)}",
                "No function matches signature '(<Member>, <Numeric Expression>)'");
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
                "");
    }

    public void testTupleNull() {
        // if a tuple contains any null members, it evaluates to null
        assertQueryReturns(
                    "select {[Measures].[Unit Sales]} on columns,\n" +
                    " { ([Gender].[M], [Store]),\n" +
                    "   ([Gender].[F], [Store].parent),\n" +
                    "   ([Gender].parent, [Store])} on rows\n" +
                    "from [Sales]",
                fold(
                    "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Unit Sales]}\n" +
                    "Axis #2:\n" +
                    "{[Gender].[All Gender].[M], [Store].[All Stores]}\n" +
                    "Row #0: 135,215\n"));

        // the set function eliminates tuples which are wholly or partially
        // null
        assertAxisReturns(
            "([Gender].parent, [Marital Status]),\n" + // part null
            " ([Gender].[M], [Marital Status].parent),\n" + // part null
            " ([Gender].parent, [Marital Status].parent),\n" + // wholly null
            " ([Gender].[M], [Marital Status])", // not null
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
        assertAxisThrows("[Time].[Month].[Q1]",
                "MDX object '[Time].[Month].[Q1]' not found in cube");
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
            "");
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
            "");
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
                    "SELECT { [Store Sales] } ON COLUMNS,\n" +
                    " TOPCOUNT( Filter( [Store].[Store Name].Members,\n" +
                    "                   [Store].CurrentMember.Properties(\"Store Type\") = \"Supermarket\" ),\n" +
                    "           10, [Store Sales]) ON ROWS\n" +
                    "FROM [Sales]");
        Assert.assertEquals(8, result.getAxes()[1].positions.length);
    }

    public void testPropertyInCalculatedMember() {
        Result result = executeQuery(
                    "WITH MEMBER [Measures].[Store Sales per Sqft]\n" +
                    "AS '[Measures].[Store Sales] / " +
                "  [Store].CurrentMember.Properties(\"Store Sqft\")'\n" +
                    "SELECT \n" +
                    "  {[Measures].[Unit Sales], [Measures].[Store Sales per Sqft]} ON COLUMNS,\n" +
                    "  {[Store].[Store Name].members} ON ROWS\n" +
                    "FROM Sales");
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
        Assert.assertEquals("", cell.getFormattedValue());
        cell = result.getCell(new int[]{1, 3});
        Assert.assertEquals("", cell.getFormattedValue());
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
                fold(
                    "[Time].[1997]\n" +
                    "[Time].[1998]"
                ));
        assertAxisReturns("LastPeriods(-2, [Time].[1997])",
                fold(
                    "[Time].[1997]\n" +
                    "[Time].[1998]"
                ));
        assertAxisReturns("LastPeriods(5000, [Time].[1998])",
                fold(
                    "[Time].[1997]\n" +
                    "[Time].[1998]"
                ));
        assertAxisReturns("LastPeriods(-5000, [Time].[1997])",
                fold(
                    "[Time].[1997]\n" +
                    "[Time].[1998]"
                ));
        assertAxisReturns("LastPeriods(2, [Time].[1998].[Q2])",
                fold(
                    "[Time].[1998].[Q1]\n" +
                    "[Time].[1998].[Q2]"
                ));
        assertAxisReturns("LastPeriods(4, [Time].[1998].[Q2])",
                fold(
                    "[Time].[1997].[Q3]\n" +
                    "[Time].[1997].[Q4]\n" +
                    "[Time].[1998].[Q1]\n" +
                    "[Time].[1998].[Q2]"
                ));
        assertAxisReturns("LastPeriods(-2, [Time].[1997].[Q2])",
                fold(
                    "[Time].[1997].[Q2]\n" +
                    "[Time].[1997].[Q3]"
                ));
        assertAxisReturns("LastPeriods(-4, [Time].[1997].[Q2])",
                fold(
                    "[Time].[1997].[Q2]\n" +
                    "[Time].[1997].[Q3]\n" +
                    "[Time].[1997].[Q4]\n" +
                    "[Time].[1998].[Q1]"
                ));
        assertAxisReturns("LastPeriods(5000, [Time].[1998].[Q2])",
                fold(
                    "[Time].[1997].[Q1]\n" +
                    "[Time].[1997].[Q2]\n" +
                    "[Time].[1997].[Q3]\n" +
                    "[Time].[1997].[Q4]\n" +
                    "[Time].[1998].[Q1]\n" +
                    "[Time].[1998].[Q2]"
                ));
        assertAxisReturns("LastPeriods(-5000, [Time].[1998].[Q2])",
                fold(
                    "[Time].[1998].[Q2]\n" +
                    "[Time].[1998].[Q3]\n" +
                    "[Time].[1998].[Q4]"
                ));

        assertAxisReturns("LastPeriods(2, [Time].[1998].[Q2].[5])",
                fold(
                    "[Time].[1998].[Q2].[4]\n" +
                    "[Time].[1998].[Q2].[5]"
                ));
        assertAxisReturns("LastPeriods(12, [Time].[1998].[Q2].[5])",
                fold(
                    "[Time].[1997].[Q2].[6]\n" +
                    "[Time].[1997].[Q3].[7]\n" +
                    "[Time].[1997].[Q3].[8]\n" +
                    "[Time].[1997].[Q3].[9]\n" +
                    "[Time].[1997].[Q4].[10]\n" +
                    "[Time].[1997].[Q4].[11]\n" +
                    "[Time].[1997].[Q4].[12]\n" +
                    "[Time].[1998].[Q1].[1]\n" +
                    "[Time].[1998].[Q1].[2]\n" +
                    "[Time].[1998].[Q1].[3]\n" +
                    "[Time].[1998].[Q2].[4]\n" +
                    "[Time].[1998].[Q2].[5]"));
        assertAxisReturns("LastPeriods(-2, [Time].[1998].[Q2].[4])",
                fold(
                    "[Time].[1998].[Q2].[4]\n" +
                    "[Time].[1998].[Q2].[5]"));
        assertAxisReturns("LastPeriods(-12, [Time].[1997].[Q2].[6])",
                fold(
                    "[Time].[1997].[Q2].[6]\n" +
                    "[Time].[1997].[Q3].[7]\n" +
                    "[Time].[1997].[Q3].[8]\n" +
                    "[Time].[1997].[Q3].[9]\n" +
                    "[Time].[1997].[Q4].[10]\n" +
                    "[Time].[1997].[Q4].[11]\n" +
                    "[Time].[1997].[Q4].[12]\n" +
                    "[Time].[1998].[Q1].[1]\n" +
                    "[Time].[1998].[Q1].[2]\n" +
                    "[Time].[1998].[Q1].[3]\n" +
                    "[Time].[1998].[Q2].[4]\n" +
                    "[Time].[1998].[Q2].[5]"));
        assertAxisReturns("LastPeriods(2, [Gender].[M])",
                fold(
                    "[Gender].[All Gender].[F]\n" +
                    "[Gender].[All Gender].[M]"));
        assertAxisReturns("LastPeriods(-2, [Gender].[F])",
            fold(
                "[Gender].[All Gender].[F]\n" +
                "[Gender].[All Gender].[M]"));
        assertAxisReturns("LastPeriods(2, [Gender])",
            "[Gender].[All Gender]");
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
                    "WITH MEMBER [Measures].[Foo] AS \n" +
                    " ' ParallelPeriod([Time].[Year]).UniqueName '\n" +
                    "SELECT {[Measures].[Foo]} ON COLUMNS\n" +
                    "FROM [Sales]\n" +
                    "WHERE [Time].[1997].[Q3].[8]",
                fold(
                    "Axis #0:\n" +
                    "{[Time].[1997].[Q3].[8]}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Foo]}\n" +
                    "Row #0: [Time].[#Null]\n"));

        // one parameter, level 1 above member
        assertQueryReturns(
                    "WITH MEMBER [Measures].[Foo] AS \n" +
                    " ' ParallelPeriod([Time].[Quarter]).UniqueName '\n" +
                    "SELECT {[Measures].[Foo]} ON COLUMNS\n" +
                    "FROM [Sales]\n" +
                    "WHERE [Time].[1997].[Q3].[8]",
                fold(
                    "Axis #0:\n" +
                    "{[Time].[1997].[Q3].[8]}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Foo]}\n" +
                    "Row #0: [Time].[1997].[Q2].[5]\n"));

        // one parameter, level same as member
        assertQueryReturns(
                    "WITH MEMBER [Measures].[Foo] AS \n" +
                    " ' ParallelPeriod([Time].[Month]).UniqueName '\n" +
                    "SELECT {[Measures].[Foo]} ON COLUMNS\n" +
                    "FROM [Sales]\n" +
                    "WHERE [Time].[1997].[Q3].[8]",
                fold(
                    "Axis #0:\n" +
                    "{[Time].[1997].[Q3].[8]}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Foo]}\n" +
                    "Row #0: [Time].[1997].[Q3].[7]\n"));

        //  one parameter, level below member
        assertQueryReturns(
                    "WITH MEMBER [Measures].[Foo] AS \n" +
                    " ' ParallelPeriod([Time].[Month]).UniqueName '\n" +
                    "SELECT {[Measures].[Foo]} ON COLUMNS\n" +
                    "FROM [Sales]\n" +
                    "WHERE [Time].[1997].[Q3]",
                fold(
                    "Axis #0:\n" +
                    "{[Time].[1997].[Q3]}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Foo]}\n" +
                    "Row #0: [Time].[#Null]\n"));
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
                fold(
                    "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Unit Sales], [Marital Status].[All Marital Status].[M]}\n" +
                    "{[Measures].[Unit Sales], [Marital Status].[All Marital Status].[S]}\n" +
                    "{[Measures].[Prev Unit Sales], [Marital Status].[All Marital Status].[M]}\n" +
                    "{[Measures].[Prev Unit Sales], [Marital Status].[All Marital Status].[S]}\n" +
                    "Axis #2:\n" +
                    "{[Time].[1997].[Q3]}\n" +
                    "Row #0: 32,815\n" +
                    "Row #0: 33,033\n" +
                    "Row #0: 33,101\n" +
                    "Row #0: 33,190\n"));

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
                fold(
                    "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Unit Sales], [Marital Status].[All Marital Status].[M]}\n" +
                    "{[Measures].[Prev Unit Sales], [Marital Status].[All Marital Status].[M]}\n" +
                    "Axis #2:\n" +
                    "{[Time].[1997].[Q3].[8]}\n" +
                    "Row #0: 10,957\n" +
                    "Row #0: 10,280\n"));
    }

    public void testPlus() {
        assertExprDependsOn("1 + 2", "{}");
        assertExprDependsOn("([Measures].[Unit Sales], [Gender].[F]) + 2",
                allDimsExcept(new String[] {"[Measures]", "[Gender]"}));

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
                    "WITH MEMBER [Customers].[USAMinusMexico]\n" +
                    "AS '([Customers].[All Customers].[USA] - [Customers].[All Customers].[Mexico])'\n" +
                    "SELECT {[Measures].[Unit Sales]} ON COLUMNS,\n" +
                    "{[Customers].[All Customers].[USA], [Customers].[All Customers].[Mexico],\n" +
                    "[Customers].[USAMinusMexico]} ON ROWS\n" +
                    "FROM [Sales]",
                fold(
                    "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Unit Sales]}\n" +
                    "Axis #2:\n" +
                    "{[Customers].[All Customers].[USA]}\n" +
                    "{[Customers].[All Customers].[Mexico]}\n" +
                    "{[Customers].[USAMinusMexico]}\n" +
                    "Row #0: 266,773\n" +
                    "Row #1: \n" +
                    "Row #2: 266,773\n" + // with bug 1234759, this was null
                    ""));
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
        final String desiredResult = fold(
            "Axis #0:\n" +
            "{}\n" +
            "Axis #1:\n" +
            "{[Store].[All Stores]}\n" +
            "Axis #2:\n" +
            "{[Measures].[Store Sales]}\n" +
            "{[Measures].[A]}\n" +
            "Row #0: 565,238.13\n" +
            "Row #1: 319,494,143,605.90\n");
        assertQueryReturns(
                    "WITH MEMBER [Measures].[A] AS\n" +
                    " '([Measures].[Store Sales] * [Measures].[Store Sales])'\n" +
                    "SELECT {[Store]} ON COLUMNS,\n" +
                    " {[Measures].[Store Sales], [Measures].[A]} ON ROWS\n" +
                    "FROM Sales",
                desiredResult);
        // as above, no parentheses
        assertQueryReturns(
                    "WITH MEMBER [Measures].[A] AS\n" +
                    " '[Measures].[Store Sales] * [Measures].[Store Sales]'\n" +
                    "SELECT {[Store]} ON COLUMNS,\n" +
                    " {[Measures].[Store Sales], [Measures].[A]} ON ROWS\n" +
                    "FROM Sales",
                desiredResult);
        // as above, plus 0
        assertQueryReturns(
                    "WITH MEMBER [Measures].[A] AS\n" +
                    " '[Measures].[Store Sales] * [Measures].[Store Sales] + 0'\n" +
                    "SELECT {[Store]} ON COLUMNS,\n" +
                    " {[Measures].[Store Sales], [Measures].[A]} ON ROWS\n" +
                    "FROM Sales",
                desiredResult);
    }

    public void testDivide() {
        assertExprReturns("10 / 5", "2");
        assertExprReturns("-2 / " + NullNumericExpr, "");
        assertExprReturns(NullNumericExpr + " / - 2", "");
        assertExprReturns(NullNumericExpr + " / " + NullNumericExpr, "");
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
                fold(
                    "{[Customers].[All Customers].[USA].[CA].[Bellflower], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Light Beer]}\n" +
                    "{[Customers].[All Customers].[USA].[CA].[Downey], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Imported Beer]}\n" +
                    "{[Customers].[All Customers].[USA].[CA].[Glendale], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Imported Beer]}\n" +
                    "{[Customers].[All Customers].[USA].[CA].[Glendale], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Light Beer]}\n" +
                    "{[Customers].[All Customers].[USA].[CA].[Grossmont], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Light Beer]}\n" +
                    "{[Customers].[All Customers].[USA].[CA].[Imperial Beach], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Light Beer]}\n" +
                    "{[Customers].[All Customers].[USA].[CA].[La Jolla], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Imported Beer]}\n" +
                    "{[Customers].[All Customers].[USA].[CA].[Lincoln Acres], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Imported Beer]}\n" +
                    "{[Customers].[All Customers].[USA].[CA].[Lincoln Acres], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Light Beer]}\n" +
                    "{[Customers].[All Customers].[USA].[CA].[Long Beach], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Light Beer]}\n" +
                    "{[Customers].[All Customers].[USA].[CA].[Los Angeles], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Imported Beer]}\n" +
                    "{[Customers].[All Customers].[USA].[CA].[Newport Beach], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Imported Beer]}\n" +
                    "{[Customers].[All Customers].[USA].[CA].[Pomona], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Imported Beer]}\n" +
                    "{[Customers].[All Customers].[USA].[CA].[Pomona], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Light Beer]}\n" +
                    "{[Customers].[All Customers].[USA].[CA].[San Gabriel], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Light Beer]}\n" +
                    "{[Customers].[All Customers].[USA].[CA].[West Covina], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Imported Beer]}\n" +
                    "{[Customers].[All Customers].[USA].[CA].[West Covina], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Light Beer]}\n" +
                    "{[Customers].[All Customers].[USA].[CA].[Woodland Hills], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good].[Good Imported Beer]}"));

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

    public void testIsString() {
        assertExprThrows(" [Store].[USA].Name IS \"USA\" ",
            "No function matches signature '<String> IS <String>'");
    }

    public void testIsNumeric() {
        assertExprThrows(" [Store].[USA].Level.Ordinal IS 25 ",
            "No function matches signature '<Numeric Expression> IS <Numeric Expression>'");
    }

    public void testIsTuple() {
        assertBooleanExprReturns(" (Store.[USA], Gender.[M]) IS (Store.[USA], Gender.[M])", true);
        assertBooleanExprReturns(" (Store.[USA], Gender.[M]) IS (Gender.[M], Store.[USA])", true);
        assertBooleanExprReturns(" (Store.[USA], Gender.[M]) IS (Gender.[M], Store.[USA]) OR [Gender] IS NULL", true);
        assertBooleanExprReturns(" (Store.[USA], Gender.[M]) IS (Gender.[M], Store.[USA]) AND [Gender] IS NULL", false);
        assertBooleanExprReturns(" (Store.[USA], Gender.[M]) IS (Store.[USA], Gender.[F])", false);
        assertBooleanExprReturns(" (Store.[USA], Gender.[M]) IS (Store.[USA])", false);
        assertBooleanExprReturns(" (Store.[USA], Gender.[M]) IS Store.[USA]", false);
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
                fold(
                    "[Employees].[All Employees].[Sheri Nowmer].[Donna Arnold]\n" + "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz]"));
    }

    public void testDistinctFourMembers() {
        getTestContext("HR").assertAxisReturns(
                "Distinct({[Employees].[All Employees].[Sheri Nowmer].[Donna Arnold],"
                + "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz],"
                + "[Employees].[All Employees].[Sheri Nowmer].[Donna Arnold],"
                + "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz]})",
                fold(
                    "[Employees].[All Employees].[Sheri Nowmer].[Donna Arnold]\n" + "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz]"));
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
                fold(
                    "{[Time].[1997], [Store].[All Stores].[Mexico]}\n" + "{[Time].[1997], [Store].[All Stores].[Canada]}\n" + "{[Time].[1997], [Store].[All Stores].[USA]}"));
    }

    /**
     * Make sure that slicer is in force when expression is applied
     * on axis, E.g. select filter([Customers].members, [Unit Sales] > 100)
     * from sales where ([Time].[1998])
     */
    public void testFilterWithSlicer() {
        Result result = executeQuery(
                    "select {[Measures].[Unit Sales]} on columns,\n" +
                    " filter([Customers].[USA].children,\n" +
                    "        [Measures].[Unit Sales] > 20000) on rows\n" +
                    "from Sales\n" +
                    "where ([Time].[1997].[Q1])");
        Axis rows = result.getAxes()[1];
        // if slicer were ignored, there would be 3 rows
        Assert.assertEquals(1, rows.positions.length);
        Cell cell = result.getCell(new int[]{0, 0});
        Assert.assertEquals("30,114", cell.getFormattedValue());
    }

    public void testFilterCompound() {
        Result result = executeQuery(
                    "select {[Measures].[Unit Sales]} on columns,\n" +
                    "  Filter(\n" +
                    "    CrossJoin(\n" +
                    "      [Gender].Children,\n" +
                    "      [Customers].[USA].Children),\n" +
                    "    [Measures].[Unit Sales] > 9500) on rows\n" +
                    "from Sales\n" +
                    "where ([Time].[1997].[Q1])");
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
                fold(
                    "[Store].[All Stores].[USA].[CA]\n" +
                    "[Store].[All Stores].[USA].[OR]\n" +
                    "[Store].[All Stores].[USA].[WA]\n" +
                    "[Store].[All Stores].[USA].[CA].[Alameda]\n" +
                    "[Store].[All Stores].[USA].[CA].[Beverly Hills]\n" +
                    "[Store].[All Stores].[USA].[CA].[Los Angeles]\n" +
                    "[Store].[All Stores].[USA].[CA].[San Diego]\n" +
                    "[Store].[All Stores].[USA].[CA].[San Francisco]"));
    }

    public void testGenerateAll() {
        assertAxisReturns("Generate({[Store].[USA].[CA], [Store].[USA].[OR].[Portland]}," +
                " Ascendants([Store].CurrentMember)," +
                " ALL)",
                fold(
                    "[Store].[All Stores].[USA].[CA]\n" +
                    "[Store].[All Stores].[USA]\n" +
                    "[Store].[All Stores]\n" +
                    "[Store].[All Stores].[USA].[OR].[Portland]\n" +
                    "[Store].[All Stores].[USA].[OR]\n" +
                    "[Store].[All Stores].[USA]\n" +
                    "[Store].[All Stores]"));
    }

    public void testGenerateUnique() {
        assertAxisReturns("Generate({[Store].[USA].[CA], [Store].[USA].[OR].[Portland]}," +
                " Ascendants([Store].CurrentMember))",
                fold(
                    "[Store].[All Stores].[USA].[CA]\n" +
                    "[Store].[All Stores].[USA]\n" +
                    "[Store].[All Stores]\n" +
                    "[Store].[All Stores].[USA].[OR].[Portland]\n" +
                    "[Store].[All Stores].[USA].[OR]"));
    }

    public void testGenerateCrossJoin() {
        // Note that the different regions have different Top 2.
        assertAxisReturns(
                fold(
                    "Generate({[Store].[USA].[CA], [Store].[USA].[CA].[San Francisco]},\n" +
                    "  CrossJoin({[Store].CurrentMember},\n" +
                    "    TopCount([Product].[Brand Name].members, \n" +
                    "    2,\n" +
                    "    [Measures].[Unit Sales])))"),
                fold(
                    "{[Store].[All Stores].[USA].[CA], [Product].[All Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos]}\n" +
                    "{[Store].[All Stores].[USA].[CA], [Product].[All Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale]}\n" +
                    "{[Store].[All Stores].[USA].[CA].[San Francisco], [Product].[All Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony]}\n" +
                    "{[Store].[All Stores].[USA].[CA].[San Francisco], [Product].[All Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top]}"));
    }

    public void testHead() {
        assertAxisReturns("Head([Store].Children, 2)",
                fold(
                    "[Store].[All Stores].[Canada]\n" +
                    "[Store].[All Stores].[Mexico]"));
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
                fold(
                    "[Store].[All Stores].[Canada]\n" +
                    "[Store].[All Stores].[Mexico]\n" +
                    "[Store].[All Stores].[USA]"));
    }

    public void testHeadEmpty() {
        assertAxisReturns("Head([Gender].[F].Children, 2)",
                "");

        assertAxisReturns("Head([Gender].[F].Children)",
                "");
    }

    public void testHierarchize() {
        assertAxisReturns(
                fold(
                    "Hierarchize(\n" +
                    "    {[Product].[All Products], " +
                "     [Product].[Food],\n" +
                    "     [Product].[Drink],\n" +
                    "     [Product].[Non-Consumable],\n" +
                    "     [Product].[Food].[Eggs],\n" +
                    "     [Product].[Drink].[Dairy]})"),

                fold(
                    "[Product].[All Products]\n" +
                    "[Product].[All Products].[Drink]\n" +
                    "[Product].[All Products].[Drink].[Dairy]\n" +
                    "[Product].[All Products].[Food]\n" +
                    "[Product].[All Products].[Food].[Eggs]\n" +
                    "[Product].[All Products].[Non-Consumable]"));
    }

    public void testHierarchizePost() {
        assertAxisReturns(
                fold(
                    "Hierarchize(\n" +
                    "    {[Product].[All Products], " +
                "     [Product].[Food],\n" +
                    "     [Product].[Food].[Eggs],\n" +
                    "     [Product].[Drink].[Dairy]},\n" +
                    "  POST)"),

                fold(
                    "[Product].[All Products].[Drink].[Dairy]\n" +
                    "[Product].[All Products].[Food].[Eggs]\n" +
                    "[Product].[All Products].[Food]\n" +
                    "[Product].[All Products]"));
    }

    public void testHierarchizeCrossJoinPre() {
        assertAxisReturns(
                fold(
                    "Hierarchize(\n" +
                    "  CrossJoin(\n" +
                    "    {[Product].[All Products], " +
                "     [Product].[Food],\n" +
                    "     [Product].[Food].[Eggs],\n" +
                    "     [Product].[Drink].[Dairy]},\n" +
                    "    [Gender].MEMBERS),\n" +
                    "  PRE)"),

                fold(
                    "{[Product].[All Products], [Gender].[All Gender]}\n" +
                    "{[Product].[All Products], [Gender].[All Gender].[F]}\n" +
                    "{[Product].[All Products], [Gender].[All Gender].[M]}\n" +
                    "{[Product].[All Products].[Drink].[Dairy], [Gender].[All Gender]}\n" +
                    "{[Product].[All Products].[Drink].[Dairy], [Gender].[All Gender].[F]}\n" +
                    "{[Product].[All Products].[Drink].[Dairy], [Gender].[All Gender].[M]}\n" +
                    "{[Product].[All Products].[Food], [Gender].[All Gender]}\n" +
                    "{[Product].[All Products].[Food], [Gender].[All Gender].[F]}\n" +
                    "{[Product].[All Products].[Food], [Gender].[All Gender].[M]}\n" +
                    "{[Product].[All Products].[Food].[Eggs], [Gender].[All Gender]}\n" +
                    "{[Product].[All Products].[Food].[Eggs], [Gender].[All Gender].[F]}\n" +
                    "{[Product].[All Products].[Food].[Eggs], [Gender].[All Gender].[M]}"));
    }

    public void testHierarchizeCrossJoinPost() {
        assertAxisReturns(
                fold(
                    "Hierarchize(\n" +
                    "  CrossJoin(\n" +
                    "    {[Product].[All Products], " +
                "     [Product].[Food],\n" +
                    "     [Product].[Food].[Eggs],\n" +
                    "     [Product].[Drink].[Dairy]},\n" +
                    "    [Gender].MEMBERS),\n" +
                    "  POST)"),

                fold(
                    "{[Product].[All Products].[Drink].[Dairy], [Gender].[All Gender].[F]}\n" +
                    "{[Product].[All Products].[Drink].[Dairy], [Gender].[All Gender].[M]}\n" +
                    "{[Product].[All Products].[Drink].[Dairy], [Gender].[All Gender]}\n" +
                    "{[Product].[All Products].[Food].[Eggs], [Gender].[All Gender].[F]}\n" +
                    "{[Product].[All Products].[Food].[Eggs], [Gender].[All Gender].[M]}\n" +
                    "{[Product].[All Products].[Food].[Eggs], [Gender].[All Gender]}\n" +
                    "{[Product].[All Products].[Food], [Gender].[All Gender].[F]}\n" +
                    "{[Product].[All Products].[Food], [Gender].[All Gender].[M]}\n" +
                    "{[Product].[All Products].[Food], [Gender].[All Gender]}\n" +
                    "{[Product].[All Products], [Gender].[All Gender].[F]}\n" +
                    "{[Product].[All Products], [Gender].[All Gender].[M]}\n" +
                    "{[Product].[All Products], [Gender].[All Gender]}"));
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
                fold(
                    "<Cube name=\"Sales_Hierarchize\">\n" +
                    "  <Table name=\"sales_fact_1997\"/>\n" +
                    "  <Dimension name=\"Time_Alphabetical\" type=\"TimeDimension\" foreignKey=\"time_id\">\n" +
                    "    <Hierarchy hasAll=\"false\" primaryKey=\"time_id\">\n" +
                    "      <Table name=\"time_by_day\"/>\n" +
                    "      <Level name=\"Year\" column=\"the_year\" type=\"Numeric\" uniqueMembers=\"true\"\n" +
                    "          levelType=\"TimeYears\"/>\n" +
                    "      <Level name=\"Quarter\" column=\"quarter\" uniqueMembers=\"false\"\n" +
                    "          levelType=\"TimeQuarters\"/>\n" +
                    "      <Level name=\"Month\" column=\"month_of_year\" uniqueMembers=\"false\" type=\"Numeric\"\n" +
                    "          ordinalColumn=\"the_month\"\n" +
                    "          levelType=\"TimeMonths\"/>\n" +
                    "    </Hierarchy>\n" +
                    "  </Dimension>\n" +
                    "\n" +
                    "  <Dimension name=\"Month_Alphabetical\" type=\"TimeDimension\" foreignKey=\"time_id\">\n" +
                    "    <Hierarchy hasAll=\"false\" primaryKey=\"time_id\">\n" +
                    "      <Table name=\"time_by_day\"/>\n" +
                    "      <Level name=\"Month\" column=\"month_of_year\" uniqueMembers=\"false\" type=\"Numeric\"\n" +
                    "          ordinalColumn=\"the_month\"\n" +
                    "          levelType=\"TimeMonths\"/>\n" +
                    "    </Hierarchy>\n" +
                    "  </Dimension>\n" +
                    "\n" +
                    "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\"\n" +
                    "      formatString=\"Standard\"/>\n" +
                    "</Cube>"));

        // The [Time_Alphabetical] is ordered alphabetically by month
        getTestContext("[Sales_Hierarchize]").assertAxisReturns(
                "Hierarchize([Time_Alphabetical].members)",
                fold(
                    "[Time_Alphabetical].[1997]\n" +
                    "[Time_Alphabetical].[1997].[Q1]\n" +
                    "[Time_Alphabetical].[1997].[Q1].[2]\n" +
                    "[Time_Alphabetical].[1997].[Q1].[1]\n" +
                    "[Time_Alphabetical].[1997].[Q1].[3]\n" +
                    "[Time_Alphabetical].[1997].[Q2]\n" +
                    "[Time_Alphabetical].[1997].[Q2].[4]\n" +
                    "[Time_Alphabetical].[1997].[Q2].[6]\n" +
                    "[Time_Alphabetical].[1997].[Q2].[5]\n" +
                    "[Time_Alphabetical].[1997].[Q3]\n" +
                    "[Time_Alphabetical].[1997].[Q3].[8]\n" +
                    "[Time_Alphabetical].[1997].[Q3].[7]\n" +
                    "[Time_Alphabetical].[1997].[Q3].[9]\n" +
                    "[Time_Alphabetical].[1997].[Q4]\n" +
                    "[Time_Alphabetical].[1997].[Q4].[12]\n" +
                    "[Time_Alphabetical].[1997].[Q4].[11]\n" +
                    "[Time_Alphabetical].[1997].[Q4].[10]\n" +
                    "[Time_Alphabetical].[1998]\n" +
                    "[Time_Alphabetical].[1998].[Q1]\n" +
                    "[Time_Alphabetical].[1998].[Q1].[2]\n" +
                    "[Time_Alphabetical].[1998].[Q1].[1]\n" +
                    "[Time_Alphabetical].[1998].[Q1].[3]\n" +
                    "[Time_Alphabetical].[1998].[Q2]\n" +
                    "[Time_Alphabetical].[1998].[Q2].[4]\n" +
                    "[Time_Alphabetical].[1998].[Q2].[6]\n" +
                    "[Time_Alphabetical].[1998].[Q2].[5]\n" +
                    "[Time_Alphabetical].[1998].[Q3]\n" +
                    "[Time_Alphabetical].[1998].[Q3].[8]\n" +
                    "[Time_Alphabetical].[1998].[Q3].[7]\n" +
                    "[Time_Alphabetical].[1998].[Q3].[9]\n" +
                    "[Time_Alphabetical].[1998].[Q4]\n" +
                    "[Time_Alphabetical].[1998].[Q4].[12]\n" +
                    "[Time_Alphabetical].[1998].[Q4].[11]\n" +
                    "[Time_Alphabetical].[1998].[Q4].[10]"));

        // The [Month_Alphabetical] is a single-level hierarchy ordered
        // alphabetically by month.
        getTestContext("[Sales_Hierarchize]").assertAxisReturns(
                "Hierarchize([Month_Alphabetical].members)",
                fold(
                    "[Month_Alphabetical].[4]\n" +
                    "[Month_Alphabetical].[8]\n" +
                    "[Month_Alphabetical].[12]\n" +
                    "[Month_Alphabetical].[2]\n" +
                    "[Month_Alphabetical].[1]\n" +
                    "[Month_Alphabetical].[7]\n" +
                    "[Month_Alphabetical].[6]\n" +
                    "[Month_Alphabetical].[3]\n" +
                    "[Month_Alphabetical].[5]\n" +
                    "[Month_Alphabetical].[11]\n" +
                    "[Month_Alphabetical].[10]\n" +
                    "[Month_Alphabetical].[9]"));
    }

    public void testIntersect() {
        // Note: duplicates retained from left, not from right; and order is preserved.
        assertAxisReturns("Intersect({[Time].[1997].[Q2], [Time].[1997], [Time].[1997].[Q1], [Time].[1997].[Q2]}, " +
                "{[Time].[1998], [Time].[1997], [Time].[1997].[Q2], [Time].[1997]}, " +
                "ALL)",
                fold(
                    "[Time].[1997].[Q2]\n" +
                    "[Time].[1997]\n" +
                    "[Time].[1997].[Q2]"));
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
                fold(
                    "  Order(\n" +
                    "    CrossJoin( \n" +
                    "      {[Product].[All Products].[Food].[Eggs],\n" +
                    "       [Product].[All Products].[Food].[Seafood],\n" +
                    "       [Product].[All Products].[Drink].[Alcoholic Beverages]},\n" +
                    "      {[Store].[USA].[WA].[Seattle],\n" +
                    "       [Store].[USA].[CA],\n" +
                    "       [Store].[USA].[OR]}),\n" +
                    "    ([Time].[1997].[Q1], [Measures].[Unit Sales]),\n" +
                    "    ASC)"),
                allDimsExcept(new String[] {"[Measures]", "[Store]", "[Product]", "[Time]"}));
    }

    public void testOrderCalc() {
        // [Measures].[Unit Sales] is a constant member, so it is evaluated in
        // a ContextCalc.
        // Note that a CopyListCalc is required because Children returns an
        // immutable list, and Order wants to sortMembers it.
        assertAxisCompilesTo(
                "order([Product].children, [Measures].[Unit Sales])",
                "{}(Sublist(ContextCalc([Measures].[Unit Sales], Order(CopyListCalc(Children(CurrentMember([Product]))), ValueCalc, ASC))))");

        // [Time].[1997] is constant, and is evaluated in a ContextCalc.
        // [Product].Parent is variable, and is evaluated inside the loop.
        assertAxisCompilesTo(
                "order([Product].children, ([Time].[1997], [Product].CurrentMember.Parent))",
                "{}(Sublist(ContextCalc([Time].[1997], Order(CopyListCalc(Children(CurrentMember([Product]))), MemberValueCalc(Parent(CurrentMember([Product]))), ASC))))");

        // No ContextCalc this time. All members are non-variable.
        assertAxisCompilesTo(
                "order([Product].children, [Product].CurrentMember.Parent)",
                "{}(Sublist(Order(CopyListCalc(Children(CurrentMember([Product]))), MemberValueCalc(Parent(CurrentMember([Product]))), ASC)))");

        // List expression is dependent on one of the constant calcs. It cannot
        // be pulled up, so [Gender].[M] is not in the ContextCalc.
        // Note that there is no CopyListCalc - because Filter creates its own
        // mutable copy.
        assertAxisCompilesTo(
                "order(filter([Product].children, [Measures].[Unit Sales] > 1000), ([Gender].[M], [Measures].[Store Sales]))",
                "{}(Sublist(ContextCalc([Measures].[Store Sales], Order(Filter(Children(CurrentMember([Product])), >(MemberValueCalc([Measures].[Unit Sales]), 1000.0)), MemberValueCalc([Gender].[All Gender].[M]), ASC))))");
    }

    public void testOrder() {
        assertQueryReturns(
                    "select {[Measures].[Unit Sales]} on columns,\n" +
                    " order({\n" +
                    "  [Product].[All Products].[Drink],\n" +
                    "  [Product].[All Products].[Drink].[Beverages],\n" +
                    "  [Product].[All Products].[Drink].[Dairy],\n" +
                    "  [Product].[All Products].[Food],\n" +
                    "  [Product].[All Products].[Food].[Baked Goods],\n" +
                    "  [Product].[All Products].[Food].[Eggs],\n" +
                    "  [Product].[All Products]},\n" +
                    " [Measures].[Unit Sales]) on rows\n" +
                    "from Sales",

                fold(
                    "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Unit Sales]}\n" +
                    "Axis #2:\n" +
                    "{[Product].[All Products]}\n" +
                    "{[Product].[All Products].[Drink]}\n" +
                    "{[Product].[All Products].[Drink].[Dairy]}\n" +
                    "{[Product].[All Products].[Drink].[Beverages]}\n" +
                    "{[Product].[All Products].[Food]}\n" +
                    "{[Product].[All Products].[Food].[Eggs]}\n" +
                    "{[Product].[All Products].[Food].[Baked Goods]}\n" +
                    "Row #0: 266,773\n" +
                    "Row #1: 24,597\n" +
                    "Row #2: 4,186\n" +
                    "Row #3: 13,573\n" +
                    "Row #4: 191,940\n" +
                    "Row #5: 4,132\n" +
                    "Row #6: 7,870\n"));
    }

    public void testOrderParentsMissing() {
        // Paradoxically, [Alcoholic Beverages] comes before
        // [Eggs] even though it has a larger value, because
        // its parent [Drink] has a smaller value than [Food].
        assertQueryReturns(
                    "select {[Measures].[Unit Sales]} on columns," +
                " order({\n" +
                    "  [Product].[All Products].[Drink].[Alcoholic Beverages],\n" +
                    "  [Product].[All Products].[Food].[Eggs]},\n" +
                    " [Measures].[Unit Sales], ASC) on rows\n" +
                    "from Sales",

                fold(
                    "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Unit Sales]}\n" +
                    "Axis #2:\n" +
                    "{[Product].[All Products].[Drink].[Alcoholic Beverages]}\n" +
                    "{[Product].[All Products].[Food].[Eggs]}\n" +
                    "Row #0: 6,838\n" +
                    "Row #1: 4,132\n"));
    }

    public void testOrderCrossJoinBreak() {
        assertQueryReturns(
                    "select {[Measures].[Unit Sales]} on columns,\n" +
                    "  Order(\n" +
                    "    CrossJoin(\n" +
                    "      [Gender].children,\n" +
                    "      [Marital Status].children),\n" +
                    "    [Measures].[Unit Sales],\n" +
                    "    BDESC) on rows\n" +
                    "from Sales\n" +
                    "where [Time].[1997].[Q1]",

                fold(
                    "Axis #0:\n" +
                    "{[Time].[1997].[Q1]}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Unit Sales]}\n" +
                    "Axis #2:\n" +
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S]}\n" +
                    "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[M]}\n" +
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M]}\n" +
                    "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S]}\n" +
                    "Row #0: 17,070\n" +
                    "Row #1: 16,790\n" +
                    "Row #2: 16,311\n" +
                    "Row #3: 16,120\n"));
    }

    public void testOrderCrossJoin() {
        // Note:
        // 1. [Alcoholic Beverages] collates before [Eggs] and
        //    [Seafood] because its parent, [Drink], is less
        //    than [Food]
        // 2. [Seattle] generally sorts after [CA] and [OR]
        //    because invisible parent [WA] is greater.
        assertQueryReturns(
                    "select CrossJoin(\n" +
                    "    {[Time].[1997],\n" +
                    "     [Time].[1997].[Q1]},\n" +
                    "    {[Measures].[Unit Sales]}) on columns,\n" +
                    "  Order(\n" +
                    "    CrossJoin( \n" +
                    "      {[Product].[All Products].[Food].[Eggs],\n" +
                    "       [Product].[All Products].[Food].[Seafood],\n" +
                    "       [Product].[All Products].[Drink].[Alcoholic Beverages]},\n" +
                    "      {[Store].[USA].[WA].[Seattle],\n" +
                    "       [Store].[USA].[CA],\n" +
                    "       [Store].[USA].[OR]}),\n" +
                    "    ([Time].[1997].[Q1], [Measures].[Unit Sales]),\n" +
                    "    ASC) on rows\n" +
                    "from Sales",

                fold(
                    "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Time].[1997], [Measures].[Unit Sales]}\n" +
                    "{[Time].[1997].[Q1], [Measures].[Unit Sales]}\n" +
                    "Axis #2:\n" +
                    "{[Product].[All Products].[Drink].[Alcoholic Beverages], [Store].[All Stores].[USA].[OR]}\n" +
                    "{[Product].[All Products].[Drink].[Alcoholic Beverages], [Store].[All Stores].[USA].[CA]}\n" +
                    "{[Product].[All Products].[Drink].[Alcoholic Beverages], [Store].[All Stores].[USA].[WA].[Seattle]}\n" +
                    "{[Product].[All Products].[Food].[Seafood], [Store].[All Stores].[USA].[CA]}\n" +
                    "{[Product].[All Products].[Food].[Seafood], [Store].[All Stores].[USA].[OR]}\n" +
                    "{[Product].[All Products].[Food].[Seafood], [Store].[All Stores].[USA].[WA].[Seattle]}\n" +
                    "{[Product].[All Products].[Food].[Eggs], [Store].[All Stores].[USA].[CA]}\n" +
                    "{[Product].[All Products].[Food].[Eggs], [Store].[All Stores].[USA].[OR]}\n" +
                    "{[Product].[All Products].[Food].[Eggs], [Store].[All Stores].[USA].[WA].[Seattle]}\n" +
                    "Row #0: 1,680\n" +
                    "Row #0: 393\n" +
                    "Row #1: 1,936\n" +
                    "Row #1: 431\n" +
                    "Row #2: 635\n" +
                    "Row #2: 142\n" +
                    "Row #3: 441\n" +
                    "Row #3: 91\n" +
                    "Row #4: 451\n" +
                    "Row #4: 107\n" +
                    "Row #5: 217\n" +
                    "Row #5: 44\n" +
                    "Row #6: 1,116\n" +
                    "Row #6: 240\n" +
                    "Row #7: 1,119\n" +
                    "Row #7: 251\n" +
                    "Row #8: 373\n" +
                    "Row #8: 57\n"));
    }

    public void testOrderHierarchicalDesc() {
        assertAxisReturns(
                fold(
                    "Order(\n" +
                    "    {[Product].[All Products], " +
                "     [Product].[Food],\n" +
                    "     [Product].[Drink],\n" +
                    "     [Product].[Non-Consumable],\n" +
                    "     [Product].[Food].[Eggs],\n" +
                    "     [Product].[Drink].[Dairy]},\n" +
                    "  [Measures].[Unit Sales],\n" +
                    "  DESC)"),

                fold(
                    "[Product].[All Products]\n" +
                    "[Product].[All Products].[Food]\n" +
                    "[Product].[All Products].[Food].[Eggs]\n" +
                    "[Product].[All Products].[Non-Consumable]\n" +
                    "[Product].[All Products].[Drink]\n" +
                    "[Product].[All Products].[Drink].[Dairy]"));
    }

    public void testOrderCrossJoinDesc() {
        assertAxisReturns(
                fold(
                    "Order(\n" +
                    "  CrossJoin(\n" +
                    "    {[Gender].[M], [Gender].[F]},\n" +
                    "    {[Product].[All Products], " +
                "     [Product].[Food],\n" +
                    "     [Product].[Drink],\n" +
                    "     [Product].[Non-Consumable],\n" +
                    "     [Product].[Food].[Eggs],\n" +
                    "     [Product].[Drink].[Dairy]}),\n" +
                    "  [Measures].[Unit Sales],\n" +
                    "  DESC)"),

                fold(
                    "{[Gender].[All Gender].[M], [Product].[All Products]}\n" +
                    "{[Gender].[All Gender].[M], [Product].[All Products].[Food]}\n" +
                    "{[Gender].[All Gender].[M], [Product].[All Products].[Food].[Eggs]}\n" +
                    "{[Gender].[All Gender].[M], [Product].[All Products].[Non-Consumable]}\n" +
                    "{[Gender].[All Gender].[M], [Product].[All Products].[Drink]}\n" +
                    "{[Gender].[All Gender].[M], [Product].[All Products].[Drink].[Dairy]}\n" +
                    "{[Gender].[All Gender].[F], [Product].[All Products]}\n" +
                    "{[Gender].[All Gender].[F], [Product].[All Products].[Food]}\n" +
                    "{[Gender].[All Gender].[F], [Product].[All Products].[Food].[Eggs]}\n" +
                    "{[Gender].[All Gender].[F], [Product].[All Products].[Non-Consumable]}\n" +
                    "{[Gender].[All Gender].[F], [Product].[All Products].[Drink]}\n" +
                    "{[Gender].[All Gender].[F], [Product].[All Products].[Drink].[Dairy]}"));
    }

    public void testOrderBug656802() {
        // Note:
        // 1. [Alcoholic Beverages] collates before [Eggs] and
        //    [Seafood] because its parent, [Drink], is less
        //    than [Food]
        // 2. [Seattle] generally sorts after [CA] and [OR]
        //    because invisible parent [WA] is greater.
        assertQueryReturns(
                    "select {[Measures].[Unit Sales], [Measures].[Store Cost], [Measures].[Store Sales]} ON columns, \n" +
                    "Order(\n" +
                    "  ToggleDrillState(\n" +
                    "    {([Promotion Media].[All Media], [Product].[All Products])},\n" +
                    "    {[Product].[All Products]} ), \n" +
                    "  [Measures].[Unit Sales], DESC) ON rows \n" +
                    "from [Sales] where ([Time].[1997])",

                fold(
                    "Axis #0:\n" +
                    "{[Time].[1997]}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Unit Sales]}\n" +
                    "{[Measures].[Store Cost]}\n" +
                    "{[Measures].[Store Sales]}\n" +
                    "Axis #2:\n" +
                    "{[Promotion Media].[All Media], [Product].[All Products]}\n" +
                    "{[Promotion Media].[All Media], [Product].[All Products].[Food]}\n" +
                    "{[Promotion Media].[All Media], [Product].[All Products].[Non-Consumable]}\n" +
                    "{[Promotion Media].[All Media], [Product].[All Products].[Drink]}\n" +
                    "Row #0: 266,773\n" +
                    "Row #0: 225,627.23\n" +
                    "Row #0: 565,238.13\n" +
                    "Row #1: 191,940\n" +
                    "Row #1: 163,270.72\n" +
                    "Row #1: 409,035.59\n" +
                    "Row #2: 50,236\n" +
                    "Row #2: 42,879.28\n" +
                    "Row #2: 107,366.33\n" +
                    "Row #3: 24,597\n" +
                    "Row #3: 19,477.23\n" +
                    "Row #3: 48,836.21\n"));
    }

    public void testOrderBug712702_Simplified() {
        assertQueryReturns(
                    "SELECT Order({[Time].[Year].members}, [Measures].[Unit Sales]) on columns\n" +
                    "from [Sales]",
                fold(
                    "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Time].[1998]}\n" +
                    "{[Time].[1997]}\n" +
                    "Row #0: \n" +
                    "Row #0: 266,773\n"));
    }

    public void testOrderBug712702_Original() {
        assertQueryReturns(
                    "with member [Measures].[Average Unit Sales] as 'Avg(Descendants([Time].CurrentMember, [Time].[Month]), \n" +
                    "[Measures].[Unit Sales])' \n" +
                    "member [Measures].[Max Unit Sales] as 'Max(Descendants([Time].CurrentMember, [Time].[Month]), [Measures].[Unit Sales])' \n" +
                    "select {[Measures].[Average Unit Sales], [Measures].[Max Unit Sales], [Measures].[Unit Sales]} ON columns, \n" +
                    "  NON EMPTY Order(\n" +
                    "    Crossjoin( \n" +
                    "      {[Store].[All Stores].[USA].[OR].[Portland],\n" +
                    "       [Store].[All Stores].[USA].[OR].[Salem],\n" +
                    "       [Store].[All Stores].[USA].[OR].[Salem].[Store 13],\n" +
                    "       [Store].[All Stores].[USA].[CA].[San Francisco],\n" +
                    "       [Store].[All Stores].[USA].[CA].[San Diego],\n" +
                    "       [Store].[All Stores].[USA].[CA].[Beverly Hills],\n" +
                    "       [Store].[All Stores].[USA].[CA].[Los Angeles],\n" +
                    "       [Store].[All Stores].[USA].[WA].[Walla Walla],\n" +
                    "       [Store].[All Stores].[USA].[WA].[Bellingham],\n" +
                    "       [Store].[All Stores].[USA].[WA].[Yakima],\n" +
                    "       [Store].[All Stores].[USA].[WA].[Spokane],\n" +
                    "       [Store].[All Stores].[USA].[WA].[Seattle], \n" +
                    "       [Store].[All Stores].[USA].[WA].[Bremerton],\n" +
                    "       [Store].[All Stores].[USA].[WA].[Tacoma]},\n" +
                    "     [Time].[Year].Members), \n" +
                    "  [Measures].[Average Unit Sales], ASC) ON rows\n" +
                    "from [Sales] ",
                fold(
                    "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Average Unit Sales]}\n" +
                    "{[Measures].[Max Unit Sales]}\n" +
                    "{[Measures].[Unit Sales]}\n" +
                    "Axis #2:\n" +
                    "{[Store].[All Stores].[USA].[OR].[Portland], [Time].[1997]}\n" +
                    "{[Store].[All Stores].[USA].[OR].[Salem], [Time].[1997]}\n" +
                    "{[Store].[All Stores].[USA].[OR].[Salem].[Store 13], [Time].[1997]}\n" +
                    "{[Store].[All Stores].[USA].[CA].[San Francisco], [Time].[1997]}\n" +
                    "{[Store].[All Stores].[USA].[CA].[Beverly Hills], [Time].[1997]}\n" +
                    "{[Store].[All Stores].[USA].[CA].[San Diego], [Time].[1997]}\n" +
                    "{[Store].[All Stores].[USA].[CA].[Los Angeles], [Time].[1997]}\n" +
                    "{[Store].[All Stores].[USA].[WA].[Walla Walla], [Time].[1997]}\n" +
                    "{[Store].[All Stores].[USA].[WA].[Bellingham], [Time].[1997]}\n" +
                    "{[Store].[All Stores].[USA].[WA].[Yakima], [Time].[1997]}\n" +
                    "{[Store].[All Stores].[USA].[WA].[Spokane], [Time].[1997]}\n" +
                    "{[Store].[All Stores].[USA].[WA].[Bremerton], [Time].[1997]}\n" +
                    "{[Store].[All Stores].[USA].[WA].[Seattle], [Time].[1997]}\n" +
                    "{[Store].[All Stores].[USA].[WA].[Tacoma], [Time].[1997]}\n" +
                    "Row #0: 2,173\n" +
                    "Row #0: 2,933\n" +
                    "Row #0: 26,079\n" +
                    "Row #1: 3,465\n" +
                    "Row #1: 5,891\n" +
                    "Row #1: 41,580\n" +
                    "Row #2: 3,465\n" +
                    "Row #2: 5,891\n" +
                    "Row #2: 41,580\n" +
                    "Row #3: 176\n" +
                    "Row #3: 222\n" +
                    "Row #3: 2,117\n" +
                    "Row #4: 1,778\n" +
                    "Row #4: 2,545\n" +
                    "Row #4: 21,333\n" +
                    "Row #5: 2,136\n" +
                    "Row #5: 2,686\n" +
                    "Row #5: 25,635\n" +
                    "Row #6: 2,139\n" +
                    "Row #6: 2,669\n" +
                    "Row #6: 25,663\n" +
                    "Row #7: 184\n" +
                    "Row #7: 301\n" +
                    "Row #7: 2,203\n" +
                    "Row #8: 186\n" +
                    "Row #8: 275\n" +
                    "Row #8: 2,237\n" +
                    "Row #9: 958\n" +
                    "Row #9: 1,163\n" +
                    "Row #9: 11,491\n" +
                    "Row #10: 1,966\n" +
                    "Row #10: 2,634\n" +
                    "Row #10: 23,591\n" +
                    "Row #11: 2,048\n" +
                    "Row #11: 2,623\n" +
                    "Row #11: 24,576\n" +
                    "Row #12: 2,084\n" +
                    "Row #12: 2,304\n" +
                    "Row #12: 25,011\n" +
                    "Row #13: 2,938\n" +
                    "Row #13: 3,818\n" +
                    "Row #13: 35,257\n"));
    }

    public void testSiblingsA() {
        assertAxisReturns("{[Time].[1997].Siblings}",
                fold(
                    "[Time].[1997]\n" +
                    "[Time].[1998]"));
    }

    public void testSiblingsB() {
        assertAxisReturns("{[Store].Siblings}",
                "[Store].[All Stores]");
    }

    public void testSiblingsC() {
        assertAxisReturns("{[Store].[USA].[CA].Siblings}",
                fold(
                    "[Store].[All Stores].[USA].[CA]\n" +
                    "[Store].[All Stores].[USA].[OR]\n" +
                    "[Store].[All Stores].[USA].[WA]"));
    }

    public void testSiblingsD() {
        // The null member has no siblings -- not even itself
        assertAxisReturns("{[Gender].Parent.Siblings}", "");

        assertExprReturns("count ( [Gender].parent.siblings, includeempty )", "0");
    }

    public void testSubset() {
        assertAxisReturns("Subset([Promotion Media].Children, 7, 2)",
                fold(
                    "[Promotion Media].[All Media].[Product Attachment]\n" +
                    "[Promotion Media].[All Media].[Radio]"));
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
                fold(
                    "[Promotion Media].[All Media].[Sunday Paper, Radio]\n" +
                    "[Promotion Media].[All Media].[Sunday Paper, Radio, TV]\n" +
                    "[Promotion Media].[All Media].[TV]"));
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
                fold(
                    "[Store].[All Stores].[Mexico]\n" +
                    "[Store].[All Stores].[USA]"));
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
                fold(
                    "[Store].[All Stores].[Canada]\n" +
                    "[Store].[All Stores].[Mexico]\n" +
                    "[Store].[All Stores].[USA]"));
    }

    public void testTailEmpty() {
        assertAxisReturns("Tail([Gender].[F].Children, 2)",
                "");

        assertAxisReturns("Tail([Gender].[F].Children)",
                "");
    }

    public void testToggleDrillState() {
        assertAxisReturns("ToggleDrillState({[Customers].[USA],[Customers].[Canada]},{[Customers].[USA],[Customers].[USA].[CA]})",
                fold(
                    "[Customers].[All Customers].[USA]\n" +
                    "[Customers].[All Customers].[USA].[CA]\n" +
                    "[Customers].[All Customers].[USA].[OR]\n" +
                    "[Customers].[All Customers].[USA].[WA]\n" +
                    "[Customers].[All Customers].[Canada]"));
    }

    public void testToggleDrillState2() {
        assertAxisReturns("ToggleDrillState([Product].[Product Department].members, {[Product].[All Products].[Food].[Snack Foods]})",
                fold(
                    "[Product].[All Products].[Drink].[Alcoholic Beverages]\n" +
                    "[Product].[All Products].[Drink].[Beverages]\n" +
                    "[Product].[All Products].[Drink].[Dairy]\n" +
                    "[Product].[All Products].[Food].[Baked Goods]\n" +
                    "[Product].[All Products].[Food].[Baking Goods]\n" +
                    "[Product].[All Products].[Food].[Breakfast Foods]\n" +
                    "[Product].[All Products].[Food].[Canned Foods]\n" +
                    "[Product].[All Products].[Food].[Canned Products]\n" +
                    "[Product].[All Products].[Food].[Dairy]\n" +
                    "[Product].[All Products].[Food].[Deli]\n" +
                    "[Product].[All Products].[Food].[Eggs]\n" +
                    "[Product].[All Products].[Food].[Frozen Foods]\n" +
                    "[Product].[All Products].[Food].[Meat]\n" +
                    "[Product].[All Products].[Food].[Produce]\n" +
                    "[Product].[All Products].[Food].[Seafood]\n" +
                    "[Product].[All Products].[Food].[Snack Foods]\n" +
                    "[Product].[All Products].[Food].[Snack Foods].[Snack Foods]\n" +
                    "[Product].[All Products].[Food].[Snacks]\n" +
                    "[Product].[All Products].[Food].[Starchy Foods]\n" +
                    "[Product].[All Products].[Non-Consumable].[Carousel]\n" +
                    "[Product].[All Products].[Non-Consumable].[Checkout]\n" +
                    "[Product].[All Products].[Non-Consumable].[Health and Hygiene]\n" +
                    "[Product].[All Products].[Non-Consumable].[Household]\n" +
                    "[Product].[All Products].[Non-Consumable].[Periodicals]"));
    }

    public void testToggleDrillState3() {
        assertAxisReturns("ToggleDrillState(" +
                "{[Time].[1997].[Q1]," +
                " [Time].[1997].[Q2]," +
                " [Time].[1997].[Q2].[4]," +
                " [Time].[1997].[Q2].[6]," +
                " [Time].[1997].[Q3]}," +
                "{[Time].[1997].[Q2]})",
                fold(
                    "[Time].[1997].[Q1]\n" +
                    "[Time].[1997].[Q2]\n" +
                    "[Time].[1997].[Q3]"));
    }

    // bug 634860
    public void testToggleDrillStateTuple() {
        assertAxisReturns(
            fold("ToggleDrillState(\n" +
                "{([Store].[All Stores].[USA].[CA]," +
                "  [Product].[All Products].[Drink].[Alcoholic Beverages]),\n" +
                " ([Store].[All Stores].[USA]," +
                "  [Product].[All Products].[Drink])},\n" +
                "{[Store].[All stores].[USA].[CA]})"),
            fold("{[Store].[All Stores].[USA].[CA], [Product].[All Products].[Drink].[Alcoholic Beverages]}\n" +
                "{[Store].[All Stores].[USA].[CA].[Alameda], [Product].[All Products].[Drink].[Alcoholic Beverages]}\n" +
                "{[Store].[All Stores].[USA].[CA].[Beverly Hills], [Product].[All Products].[Drink].[Alcoholic Beverages]}\n" +
                "{[Store].[All Stores].[USA].[CA].[Los Angeles], [Product].[All Products].[Drink].[Alcoholic Beverages]}\n" +
                "{[Store].[All Stores].[USA].[CA].[San Diego], [Product].[All Products].[Drink].[Alcoholic Beverages]}\n" +
                "{[Store].[All Stores].[USA].[CA].[San Francisco], [Product].[All Products].[Drink].[Alcoholic Beverages]}\n" +
                "{[Store].[All Stores].[USA], [Product].[All Products].[Drink]}"));
    }

    public void testToggleDrillStateRecursive() {
        // We expect this to fail.
        assertThrows(
            "Select \n" +
            "    ToggleDrillState(\n" +
            "        {[Store].[All Stores].[USA]}, \n" +
            "        {[Store].[All Stores].[USA]}, recursive) on Axis(0) \n" +
            "from [Sales]\n",
            "'RECURSIVE' is not supported in ToggleDrillState.");
    }

    public void testTopCount() {
        assertAxisReturns("TopCount({[Promotion Media].[Media Type].members}, 2, [Measures].[Unit Sales])",
                fold(
                    "[Promotion Media].[All Media].[No Media]\n" +
                    "[Promotion Media].[All Media].[Daily Paper, Radio, TV]"));
    }

    public void testTopCountTuple() {
        assertAxisReturns("TopCount([Customers].[Name].members,2,(Time.[1997].[Q1],[Measures].[Store Sales]))",
                fold(
                    "[Customers].[All Customers].[USA].[WA].[Spokane].[Grace McLaughlin]\n" +
                    "[Customers].[All Customers].[USA].[WA].[Spokane].[Matt Bellah]"));
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
                fold(
                    "[Promotion Media].[All Media].[No Media]\n" +
                    "[Promotion Media].[All Media].[Daily Paper, Radio, TV]"));
    }

    public void testTopSumEmpty() {
        assertAxisReturns("TopSum(Filter({[Promotion Media].[Media Type].members}, 1=0), 200000, [Measures].[Unit Sales])",
                "");
    }

    public void testUnionAll() {
        assertAxisReturns("Union({[Gender].[M]}, {[Gender].[F]}, ALL)",
                fold(
                    "[Gender].[All Gender].[M]\n" +
                    "[Gender].[All Gender].[F]")); // order is preserved
    }

    public void testUnionAllTuple() {
        // With the bug, the last 8 rows are repeated.
        assertQueryReturns("with \n" +
            "set [Set1] as 'Crossjoin({[Time].[1997].[Q1]:[Time].[1997].[Q4]},{[Store].[USA].[CA]:[Store].[USA].[OR]})'\n" +
            "set [Set2] as 'Crossjoin({[Time].[1997].[Q2]:[Time].[1997].[Q3]},{[Store].[Mexico].[DF]:[Store].[Mexico].[Veracruz]})'\n" +
            "select \n" +
            "{[Measures].[Unit Sales]} ON COLUMNS,\n" +
            "Union([Set1], [Set2], ALL) ON ROWS\n" +
            "from [Sales]",
            fold("Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Unit Sales]}\n" +
                    "Axis #2:\n" +
                    "{[Time].[1997].[Q1], [Store].[All Stores].[USA].[CA]}\n" +
                    "{[Time].[1997].[Q1], [Store].[All Stores].[USA].[OR]}\n" +
                    "{[Time].[1997].[Q2], [Store].[All Stores].[USA].[CA]}\n" +
                    "{[Time].[1997].[Q2], [Store].[All Stores].[USA].[OR]}\n" +
                    "{[Time].[1997].[Q3], [Store].[All Stores].[USA].[CA]}\n" +
                    "{[Time].[1997].[Q3], [Store].[All Stores].[USA].[OR]}\n" +
                    "{[Time].[1997].[Q4], [Store].[All Stores].[USA].[CA]}\n" +
                    "{[Time].[1997].[Q4], [Store].[All Stores].[USA].[OR]}\n" +
                    "{[Time].[1997].[Q2], [Store].[All Stores].[Mexico].[DF]}\n" +
                    "{[Time].[1997].[Q2], [Store].[All Stores].[Mexico].[Guerrero]}\n" +
                    "{[Time].[1997].[Q2], [Store].[All Stores].[Mexico].[Jalisco]}\n" +
                    "{[Time].[1997].[Q2], [Store].[All Stores].[Mexico].[Veracruz]}\n" +
                    "{[Time].[1997].[Q3], [Store].[All Stores].[Mexico].[DF]}\n" +
                    "{[Time].[1997].[Q3], [Store].[All Stores].[Mexico].[Guerrero]}\n" +
                    "{[Time].[1997].[Q3], [Store].[All Stores].[Mexico].[Jalisco]}\n" +
                    "{[Time].[1997].[Q3], [Store].[All Stores].[Mexico].[Veracruz]}\n" +
                    "Row #0: 16,890\n" +
                    "Row #1: 19,287\n" +
                    "Row #2: 18,052\n" +
                    "Row #3: 15,079\n" +
                    "Row #4: 18,370\n" +
                    "Row #5: 16,940\n" +
                    "Row #6: 21,436\n" +
                    "Row #7: 16,353\n" +
                    "Row #8: \n" +
                    "Row #9: \n" +
                    "Row #10: \n" +
                    "Row #11: \n" +
                    "Row #12: \n" +
                    "Row #13: \n" +
                    "Row #14: \n" +
                    "Row #15: \n"));
    }

    public void testUnion() {
        assertAxisReturns("Union({[Store].[USA], [Store].[USA], [Store].[USA].[OR]}, {[Store].[USA].[CA], [Store].[USA]})",
                fold(
                    "[Store].[All Stores].[USA]\n" +
                    "[Store].[All Stores].[USA].[OR]\n" +
                    "[Store].[All Stores].[USA].[CA]"));
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

                fold(
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S]}\n" +
                    "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S]}\n" +
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M]}"));
    }

    public void testUnionTupleDistinct() {
        assertAxisReturns("Union({" +
                " ([Gender].[M], [Marital Status].[S])," +
                " ([Gender].[F], [Marital Status].[S])" +
                "}, {" +
                " ([Gender].[M], [Marital Status].[M])," +
                " ([Gender].[M], [Marital Status].[S])" +
                "}, Distinct)",

                fold(
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S]}\n" +
                    "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S]}\n" +
                    "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M]}"));

    }

    public void testUnionQuery() {
        Result result = executeQuery(
                    "select {[Measures].[Unit Sales], [Measures].[Store Cost], [Measures].[Store Sales]} on columns,\n" +
                    " Hierarchize( \n" +
                    "   Union(\n" +
                    "     Crossjoin(\n" +
                    "       Crossjoin([Gender].[All Gender].children,\n" +
                    "                 [Marital Status].[All Marital Status].children ),\n" +
                    "       Crossjoin([Customers].[All Customers].children,\n" +
                    "                 [Product].[All Products].children ) ),\n" +
                    "     Crossjoin( {([Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M] )},\n" +
                    "       Crossjoin(\n" +
                    "         [Customers].[All Customers].[USA].children,\n" +
                    "         [Product].[All Products].children ) ) )) on rows\n" +
                    "from Sales where ([Time].[1997])");
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
                fold(
                    "[Time].[1997].[Q1]\n" +
                    "[Time].[1997].[Q2]\n" +
                    "[Time].[1997].[Q3]"));
        assertAxisReturns("Ytd([Time].[1997].[Q2].[4])",
                fold(
                    "[Time].[1997].[Q1].[1]\n" +
                    "[Time].[1997].[Q1].[2]\n" +
                    "[Time].[1997].[Q1].[3]\n" +
                    "[Time].[1997].[Q2].[4]"));
        assertAxisThrows("Ytd([Store])",
                "Argument to function 'Ytd' must belong to Time hierarchy");
        assertSetExprDependsOn("Ytd()", "{[Time]}");
        assertSetExprDependsOn("Ytd([Time].[1997].[Q2])", "{}");
    }

    public void testQtd() {
        // zero args
        assertQueryReturns(
                    "with member [Measures].[Foo] as ' SetToStr(Qtd()) '\n" +
                    "select {[Measures].[Foo]} on columns\n" +
                    "from [Sales]\n" +
                    "where [Time].[1997].[Q2].[5]",
                fold(
                    "Axis #0:\n" +
                    "{[Time].[1997].[Q2].[5]}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Foo]}\n" +
                    "Row #0: {[Time].[1997].[Q2].[4], [Time].[1997].[Q2].[5]}\n"));

        // one arg, a month
        assertAxisReturns("Qtd([Time].[1997].[Q2].[5])",
                fold(
                    "[Time].[1997].[Q2].[4]\n" +
                    "[Time].[1997].[Q2].[5]"));


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
                    "with member [Measures].[Foo] as ' SetToStr(Mtd()) '\n" +
                    "select {[Measures].[Foo]} on columns\n" +
                    "from [Sales]\n" +
                    "where [Time].[1997].[Q2].[5]",
                fold(
                    "Axis #0:\n" +
                    "{[Time].[1997].[Q2].[5]}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Foo]}\n" +
                    "Row #0: {[Time].[1997].[Q2].[5]}\n"));

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
                fold(
                    "[Time].[1997].[Q2].[4]\n" +
                    "[Time].[1997].[Q2].[5]"));

        // equivalent to above
        assertAxisReturns(
                "TopCount(" +
                "  Descendants(" +
                "    Ancestor(" +
                "      [Time].[1997].[Q2].[5], [Time].[Quarter])," +
                "    [Time].[1997].[Q2].[5].Level)," +
                "  1).Item(0) : [Time].[1997].[Q2].[5]",
                fold(
                    "[Time].[1997].[Q2].[4]\n" +
                    "[Time].[1997].[Q2].[5]"));

        // one arg
        assertQueryReturns(
                    "with member [Measures].[Foo] as ' SetToStr(PeriodsToDate([Time].[Quarter])) '\n" +
                    "select {[Measures].[Foo]} on columns\n" +
                    "from [Sales]\n" +
                    "where [Time].[1997].[Q2].[5]",
                fold(
                    "Axis #0:\n" +
                    "{[Time].[1997].[Q2].[5]}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Foo]}\n" +
                    "Row #0: {[Time].[1997].[Q2].[4], [Time].[1997].[Q2].[5]}\n"));

        // zero args
        assertQueryReturns(
                    "with member [Measures].[Foo] as ' SetToStr(PeriodsToDate()) '\n" +
                    "select {[Measures].[Foo]} on columns\n" +
                    "from [Sales]\n" +
                    "where [Time].[1997].[Q2].[5]",
                fold(
                    "Axis #0:\n" +
                    "{[Time].[1997].[Q2].[5]}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Foo]}\n" +
                    "Row #0: {[Time].[1997].[Q2].[4], [Time].[1997].[Q2].[5]}\n"));

        // zero args, evaluated at a member which is at the top level.
        // The default level is the level above the current member -- so
        // choosing a member at the highest level might trip up the
        // implementation.
        assertQueryReturns(
                    "with member [Measures].[Foo] as ' SetToStr(PeriodsToDate()) '\n" +
                    "select {[Measures].[Foo]} on columns\n" +
                    "from [Sales]\n" +
                    "where [Time].[1997]",
                fold(
                    "Axis #0:\n" +
                    "{[Time].[1997]}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Foo]}\n" +
                    "Row #0: {}\n"));

        // Testcase for bug 1598379, which caused NPE because the args[0].type
        // knew its dimension but not its hierarchy.
        assertQueryReturns("with member [Measures].[Position] as\n" +
            " 'Sum(PeriodsToDate([Time].Levels(0), [Time].CurrentMember), [Measures].[Store Sales])'\n" +
            "select {[Time].[1997],\n" +
            " [Time].[1997].[Q1],\n" +
            " [Time].[1997].[Q1].[1],\n" +
            " [Time].[1997].[Q1].[2],\n" +
            " [Time].[1997].[Q1].[3]} ON COLUMNS,\n" +
            "{[Measures].[Store Sales], [Measures].[Position] } ON ROWS\n" +
            "from [Sales]",
            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Time].[1997]}\n" +
                "{[Time].[1997].[Q1]}\n" +
                "{[Time].[1997].[Q1].[1]}\n" +
                "{[Time].[1997].[Q1].[2]}\n" +
                "{[Time].[1997].[Q1].[3]}\n" +
                "Axis #2:\n" +
                "{[Measures].[Store Sales]}\n" +
                "{[Measures].[Position]}\n" +
                "Row #0: 565,238.13\n" +
                "Row #0: 139,628.35\n" +
                "Row #0: 45,539.69\n" +
                "Row #0: 44,058.79\n" +
                "Row #0: 50,029.87\n" +
                "Row #1: 565,238.13\n" +
                "Row #1: 139,628.35\n" +
                "Row #1: 45,539.69\n" +
                "Row #1: 89,598.48\n" +
                "Row #1: 139,628.35\n"));
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
                "");
        // Null member in empty set. (MSAS returns an error "Formula error -
        // dimension count is not valid - in the Rank function" but I think
        // null is the correct behavior.)
        assertExprReturns("Rank([Gender].Parent, {})",
                "");
        // Member occurs twice in set -- pick first
        assertExprReturns(
                fold(
                    "Rank([Store].[USA].[WA], \n" +
                    "{[Store].[USA].[WA]," +
                " [Store].[USA].[CA]," +
                " [Store].[USA]," +
                " [Store].[USA].[WA]})"),
                "1");
        // Tuple not in set
        assertExprReturns(
                fold(
                    "Rank(([Gender].[F], [Marital Status].[M]), \n" +
                    "{([Gender].[F], [Marital Status].[S]),\n" +
                    " ([Gender].[M], [Marital Status].[S]),\n" +
                    " ([Gender].[M], [Marital Status].[M])})"),
                "0");
        // Tuple in set
        assertExprReturns(
                fold(
                    "Rank(([Gender].[F], [Marital Status].[M]), \n" +
                    "{([Gender].[F], [Marital Status].[S]),\n" +
                    " ([Gender].[M], [Marital Status].[S]),\n" +
                    " ([Gender].[F], [Marital Status].[M])})"),
                "3");
        // Tuple not in empty set
        assertExprReturns(
                fold(
                    "Rank(([Gender].[F], [Marital Status].[M]), \n" +
                    "{})"),
                "0");
        // Partially null tuple in set, returns null
        assertExprReturns(
                fold(
                    "Rank(([Gender].[F], [Marital Status].Parent), \n" +
                    "{([Gender].[F], [Marital Status].[S]),\n" +
                    " ([Gender].[M], [Marital Status].[S]),\n" +
                    " ([Gender].[F], [Marital Status].[M])})"),
                "");
    }

    public void testRankWithExpr() {
        assertQueryReturns(
                    "with member [Measures].[Sibling Rank] as ' Rank([Product].CurrentMember, [Product].CurrentMember.Siblings) '\n" +
                    "  member [Measures].[Sales Rank] as ' Rank([Product].CurrentMember, Order([Product].Parent.Children, [Measures].[Unit Sales], DESC)) '\n" +
                    "  member [Measures].[Sales Rank2] as ' Rank([Product].CurrentMember, [Product].Parent.Children, [Measures].[Unit Sales]) '\n" +
                    "select {[Measures].[Unit Sales], [Measures].[Sales Rank], [Measures].[Sales Rank2]} on columns,\n" +
                    " {[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].children} on rows\n" +
                    "from [Sales]\n" +
                    "WHERE ( [Store].[All Stores].[USA].[OR].[Portland].[Store 11], [Time].[1997].[Q2].[6])",
                fold(
                    "Axis #0:\n" +
                    "{[Store].[All Stores].[USA].[OR].[Portland].[Store 11], [Time].[1997].[Q2].[6]}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Unit Sales]}\n" +
                    "{[Measures].[Sales Rank]}\n" +
                    "{[Measures].[Sales Rank2]}\n" +
                    "Axis #2:\n" +
                    "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good]}\n" +
                    "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Pearl]}\n" +
                    "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth]}\n" +
                    "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Top Measure]}\n" +
                    "{[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Walrus]}\n" +
                    "Row #0: 5\n" +
                    "Row #0: 1\n" +
                    "Row #0: 1\n" +
                    "Row #1: \n" +
                    "Row #1: 5\n" +
                    "Row #1: 5\n" +
                    "Row #2: 3\n" +
                    "Row #2: 3\n" +
                    "Row #2: 3\n" +
                    "Row #3: 5\n" +
                    "Row #3: 2\n" +
                    "Row #3: 1\n" +
                    "Row #4: 3\n" +
                    "Row #4: 4\n" +
                    "Row #4: 3\n"));
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
                "");
        // Empty set. Value never appears in the set, therefore rank is null.
        assertExprReturns(
                "Rank([Gender].[M]," +
                " {}," +
                " [Measures].[Unit Sales])",
                "");
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
     * Tests the 3-arg version of the RANK function with a value
     * which returns null within a set of nulls.
     */
    public void testRankWithNulls() {
        assertQueryReturns(
            "with member [Measures].[X] as " +
            "'iif([Measures].[Store Sales]=777," +
            "[Measures].[Store Sales],Null)'\n" +
            "member [Measures].[Y] as 'Rank([Gender].[M]," +
            "{[Measures].[X],[Measures].[X],[Measures].[X]}," +
            " [Marital Status].[All Marital Status].Parent)'" +
            "select {[Measures].[Y]} on rows from Sales",
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Y]}\n" +
                "Row #0: 1\n"));
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

        String query =
            "WITH \n" +
            "  MEMBER [Measures].[Rank among products] \n" +
            "    AS ' Rank([Product].CurrentMember, Order([Product].members, [Measures].[Unit Sales], BDESC)) '\n" +
            "SELECT CrossJoin(\n" +
            "  [Gender].members,\n" +
            "  {[Measures].[Unit Sales],\n" +
            "   [Measures].[Rank among products]}) ON COLUMNS,\n" +
//                "  {[Product], [Product].[All Products].[Non-Consumable].[Periodicals].[Magazines].[Sports Magazines].[Robust].[Robust Monthly Sports Magazine]} ON ROWS\n" +
            "  {[Product].members} ON ROWS\n" +
            "FROM [Sales]";
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

        String query =
            "WITH \n" +
            "  MEMBER [Measures].[Rank among products] \n" +
            "    AS ' Rank([Product].CurrentMember, [Product].members, [Measures].[Unit Sales]) '\n" +
            "SELECT CrossJoin(\n" +
            "  [Gender].members,\n" +
            "  {[Measures].[Unit Sales],\n" +
            "   [Measures].[Rank among products]}) ON COLUMNS,\n" +
                "  {[Product], [Product].[All Products].[Non-Consumable].[Periodicals].[Magazines].[Sports Magazines].[Robust].[Robust Monthly Sports Magazine]} ON ROWS\n" +
//            "  {[Product].members} ON ROWS\n" +
            "FROM [Sales]";
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
        String query =
            "WITH MEMBER [Measures].[Test] as \n" +
            "  'LinRegPoint(\n" +
            "    Rank(Time.CurrentMember, Time.CurrentMember.Level.Members),\n" +
            "    Descendants([Time].[1997], [Time].[Quarter]), \n" +
            "[Measures].[Store Sales], \n" +
            "    Rank(Time.CurrentMember, Time.CurrentMember.Level.Members))' \n" +
            "SELECT \n" +
            "{[Measures].[Test],[Measures].[Store Sales]} ON ROWS, \n" +
            "{[Time].[1997].Children} ON COLUMNS \n" +
            "FROM Sales";
        String expected = fold(
            "Axis #0:\n" +
            "{}\n" +
            "Axis #1:\n" +
            "{[Time].[1997].[Q1]}\n" +
            "{[Time].[1997].[Q2]}\n" +
            "{[Time].[1997].[Q3]}\n" +
            "{[Time].[1997].[Q4]}\n" +
            "Axis #2:\n" +
            "{[Measures].[Test]}\n" +
            "{[Measures].[Store Sales]}\n" +
            "Row #0: 134,299.22\n" +
            "Row #0: 138,972.76\n" +
            "Row #0: 143,646.30\n" +
            "Row #0: 148,319.85\n" +
            "Row #1: 139,628.35\n" +
            "Row #1: 132,666.27\n" +
            "Row #1: 140,271.89\n" +
            "Row #1: 152,671.62\n");

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
        String query =
            "WITH MEMBER \n" +
            "[Measures].[Intercept] AS \n" +
            "  'LinRegIntercept([Time].CurrentMember.Lag(10) : [Time].CurrentMember, [Measures].[Unit Sales], [Measures].[Store Sales])' \n" +
            "MEMBER [Measures].[Regression Slope] AS\n" +
            "  'LinRegSlope([Time].CurrentMember.Lag(9) : [Time].CurrentMember,[Measures].[Unit Sales],[Measures].[Store Sales]) '\n" +
            "MEMBER [Measures].[Predict] AS\n" +
            "  'LinRegPoint([Measures].[Unit Sales],[Time].CurrentMember.Lag(9) : [Time].CurrentMember,[Measures].[Unit Sales],[Measures].[Store Sales])',\n" +
            "  FORMAT_STRING = 'Standard' \n" +
            "MEMBER [Measures].[Predict Formula] AS\n" +
            "  '([Measures].[Regression Slope] * [Measures].[Unit Sales]) + [Measures].[Intercept]',\n" +
            "  FORMAT_STRING='Standard'\n" +
            "MEMBER [Measures].[Good Fit] AS\n" +
            "  'LinRegR2([Time].CurrentMember.Lag(9) : [Time].CurrentMember, [Measures].[Unit Sales],[Measures].[Store Sales])',\n" +
            "  FORMAT_STRING='#,#.00'\n" +
            "MEMBER [Measures].[Variance] AS\n" +
            "  'LinRegVariance([Time].CurrentMember.Lag(9) : [Time].CurrentMember,[Measures].[Unit Sales],[Measures].[Store Sales])'\n" +
            "SELECT \n" +
            "  {[Measures].[Store Sales], \n" +
            "   [Measures].[Intercept], \n" +
            "   [Measures].[Regression Slope], \n" +
            "   [Measures].[Predict], \n" +
            "   [Measures].[Predict Formula], \n" +
            "   [Measures].[Good Fit], \n" +
            "   [Measures].[Variance] } ON COLUMNS, \n" +
            "  Descendants([Time].[1997], [Time].[Month]) ON ROWS\n" +
            "FROM Sales";

        String expected = fold(
            "Axis #0:\n" +
            "{}\n" +
            "Axis #1:\n" +
            "{[Measures].[Store Sales]}\n" +
            "{[Measures].[Intercept]}\n" +
            "{[Measures].[Regression Slope]}\n" +
            "{[Measures].[Predict]}\n" +
            "{[Measures].[Predict Formula]}\n" +
            "{[Measures].[Good Fit]}\n" +
            "{[Measures].[Variance]}\n" +
            "Axis #2:\n" +
            "{[Time].[1997].[Q1].[1]}\n" +
            "{[Time].[1997].[Q1].[2]}\n" +
            "{[Time].[1997].[Q1].[3]}\n" +
            "{[Time].[1997].[Q2].[4]}\n" +
            "{[Time].[1997].[Q2].[5]}\n" +
            "{[Time].[1997].[Q2].[6]}\n" +
            "{[Time].[1997].[Q3].[7]}\n" +
            "{[Time].[1997].[Q3].[8]}\n" +
            "{[Time].[1997].[Q3].[9]}\n" +
            "{[Time].[1997].[Q4].[10]}\n" +
            "{[Time].[1997].[Q4].[11]}\n" +
            "{[Time].[1997].[Q4].[12]}\n" +
            "Row #0: 45,539.69\n" +
            "Row #0: 68711.40\n" +
            "Row #0: -1.033\n" +
            "Row #0: 46,350.26\n" +
            "Row #0: 46.350.26\n" +
            "Row #0: -1.#INF\n" +
            "Row #0: 5.17E-08\n" +
            "...\n" +
            "Row #11: 15343.67\n");

        assertQueryReturns(query, expected);
    }

    public void testLinRegPointMonth() {
        String query =
            "WITH MEMBER \n" +
            "[Measures].[Test] as \n" +
            "  'LinRegPoint(\n" +
            "    Rank(Time.CurrentMember, Time.CurrentMember.Level.Members),\n" +
            "    Descendants([Time].[1997], [Time].[Month]), \n" +
            "    [Measures].[Store Sales], \n" +
            "    Rank(Time.CurrentMember, Time.CurrentMember.Level.Members)\n" +
            "  )' \n" +
            "SELECT \n" +
            "  {[Measures].[Test],[Measures].[Store Sales]} ON ROWS, \n" +
            "  Descendants([Time].[1997], [Time].[Month]) ON COLUMNS \n" +
            "FROM Sales";

        String expected = fold(
            "Axis #0:\n" +
            "{}\n" +
            "Axis #1:\n" +
            "{[Time].[1997].[Q1].[1]}\n" +
            "{[Time].[1997].[Q1].[2]}\n" +
            "{[Time].[1997].[Q1].[3]}\n" +
            "{[Time].[1997].[Q2].[4]}\n" +
            "{[Time].[1997].[Q2].[5]}\n" +
            "{[Time].[1997].[Q2].[6]}\n" +
            "{[Time].[1997].[Q3].[7]}\n" +
            "{[Time].[1997].[Q3].[8]}\n" +
            "{[Time].[1997].[Q3].[9]}\n" +
            "{[Time].[1997].[Q4].[10]}\n" +
            "{[Time].[1997].[Q4].[11]}\n" +
            "{[Time].[1997].[Q4].[12]}\n" +
            "Axis #2:\n" +
            "{[Measures].[Test]}\n" +
            "{[Measures].[Store Sales]}\n" +
            "Row #0: 43,824.36\n" +
            "Row #0: 44,420.51\n" +
            "Row #0: 45,016.66\n" +
            "Row #0: 45,612.81\n" +
            "Row #0: 46,208.95\n" +
            "Row #0: 46,805.10\n" +
            "Row #0: 47,401.25\n" +
            "Row #0: 47,997.40\n" +
            "Row #0: 48,593.55\n" +
            "Row #0: 49,189.70\n" +
            "Row #0: 49,785.85\n" +
            "Row #0: 50,382.00\n" +
            "Row #1: 45,539.69\n" +
            "Row #1: 44,058.79\n" +
            "Row #1: 50,029.87\n" +
            "Row #1: 42,878.25\n" +
            "Row #1: 44,456.29\n" +
            "Row #1: 45,331.73\n" +
            "Row #1: 50,246.88\n" +
            "Row #1: 46,199.04\n" +
            "Row #1: 43,825.97\n" +
            "Row #1: 42,342.27\n" +
            "Row #1: 53,363.71\n" +
            "Row #1: 56,965.64\n");

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
                fold(
                    "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Unit Sales]}\n" +
                    "Axis #2:\n" +
                    "{[Product].[All Products].[Food].[Baked Goods].[*Subtotal - Bread]}\n" +
                    "{[Product].[All Products].[Food].[Baked Goods].[Bread].[Bagels]}\n" +
                    "{[Product].[All Products].[Food].[Baked Goods].[Bread].[Muffins]}\n" +
                    "Row #0: 4,312\n" +
                    "Row #1: 815\n" +
                    "Row #2: 3,497\n"));
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
                fold(
                    "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Unit Sales]}\n" +
                    "Axis #2:\n" +
                    "{[Product].[All Products].[Food].[Baked Goods].[*Subtotal - Bread]}\n" +
                    "{[Product].[All Products].[Food].[Baked Goods].[Bread].[Bagels]}\n" +
                    "{[Product].[All Products].[Food].[Baked Goods].[Bread].[*Subtotal - Bagels]}\n" +
                    "{[Product].[All Products].[Food].[Baked Goods].[Bread].[Bagels].[Colony]}\n" +
                    "{[Product].[All Products].[Food].[Baked Goods].[Bread].[Bagels]}\n" +
                    "{[Product].[All Products].[Food].[Baked Goods].[Bread].[Muffins]}\n" +
                    "Row #0: 5,290\n" +
                    "Row #1: 815\n" +
                    "Row #2: 163\n" +
                    "Row #3: 163\n" +
                    "Row #4: 815\n" +
                    "Row #5: 3,497\n"));
    }

    public void testVisualTotalsNoPattern() {
        assertAxisReturns(
                "VisualTotals(" +
                "    {[Product].[All Products].[Food].[Baked Goods].[Bread]," +
                "     [Product].[All Products].[Food].[Baked Goods].[Bread].[Bagels]," +
                "     [Product].[All Products].[Food].[Baked Goods].[Bread].[Muffins]})",

                // Note that the [Bread] visual member is just called [Bread].
                fold(
                    "[Product].[All Products].[Food].[Baked Goods].[Bread]\n" +
                    "[Product].[All Products].[Food].[Baked Goods].[Bread].[Bagels]\n" +
                    "[Product].[All Products].[Food].[Baked Goods].[Bread].[Muffins]"));
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
                fold(
                    "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Unit Sales]}\n" +
                    "Axis #2:\n" +
                    "{[Product].[All Products].[Food].[Baked Goods].[*Subtotal - Bread]}\n" +
                    "{[Product].[All Products].[Food].[Baked Goods].[Bread].[Muffins]}\n" +
                    "Row #0: 4,312\n" +
                    "Row #1: 3,497\n"));
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
                fold(
                    "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Unit Sales]}\n" +
                    "Axis #2:\n" +
                    "{[Product].[All Products].[Food].[Baked Goods].[*Subtotal - Bread]}\n" +
                    "{[Product].[All Products].[Food].[Baked Goods].[Bread].[Muffins]}\n" +
                    "Row #0: 4,312\n" +
                    "Row #1: 3,497\n"));
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

                fold(
                    "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Unit Sales]}\n" +
                    "Axis #2:\n" +
                    "{[Product].[All Products].[Food].[Baked Goods].[*Subtotal - Bread]}\n" +
                    "{[Product].[All Products].[Food].[Baked Goods].[Bread].[Muffins]}\n" +
                    "Row #0: 3,497\n" +
                    "Row #1: 3,497\n"));
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
                fold(
                    "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Unit Sales]}\n" +
                    "Axis #2:\n" +
                    "{[Product].[All Products].[Food].[Baked Goods].[Bread].[Bagels]}\n" +
                    "{[Product].[All Products].[Food].[Baked Goods].[*Subtotal - Bread]}\n" +
                    "{[Product].[All Products].[Food].[Baked Goods].[Bread].[Muffins]}\n" +
                    "Row #0: 815\n" +
                    "Row #1: 3,497\n" +
                    "Row #2: 3,497\n"));
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
                fold(
                    "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Unit Sales]}\n" +
                    "Axis #2:\n" +
                    "{[Product].[All Products].[*Subtotal - Food]}\n" +
                    "{[Product].[All Products].[Food].[Baked Goods].[*Subtotal - Bread]}\n" +
                    "{[Product].[All Products].[Food].[Baked Goods].[Bread].[Bagels]}\n" +
                    "{[Product].[All Products].[Food].[Frozen Foods].[*Subtotal - Breakfast Foods]}\n" +
                    "{[Product].[All Products].[Food].[Frozen Foods].[Breakfast Foods].[Pancake Mix].[Golden]}\n" +
                    "{[Product].[All Products].[Food].[Frozen Foods].[Breakfast Foods].[Pancake Mix].[Big Time]}\n" +
                    "{[Product].[All Products].[Food].[Baked Goods].[Bread].[Muffins]}\n" +
                    "Row #0: 4,623\n" +
                    "Row #1: 815\n" +
                    "Row #2: 815\n" +
                    "Row #3: 311\n" +
                    "Row #4: 110\n" +
                    "Row #5: 201\n" +
                    "Row #6: 3,497\n"));
    }

    public void testVisualTotalsCrossjoin() {
        assertAxisThrows("VisualTotals(Crossjoin([Gender].Members, [Store].children))",
                "Argument to 'VisualTotals' function must be a set of members; got set of tuples.");
    }

    public void testCalculatedChild() {
        // Construct calculated children with the same name for both [Drink] and [Non-Consumable]
        // Then, create a metric to select the calculated child based on current product member
        assertQueryReturns(
            "with\n" +
            " member [Product].[All Products].[Drink].[Calculated Child] as '[Product].[All Products].[Drink].[Alcoholic Beverages]'\n" +
            " member [Product].[All Products].[Non-Consumable].[Calculated Child] as '[Product].[All Products].[Non-Consumable].[Carousel]'\n" +
            " member [Measures].[Unit Sales CC] as '([Measures].[Unit Sales],[Product].currentmember.CalculatedChild(\"Calculated Child\"))'\n" +
            " select non empty {[Measures].[Unit Sales CC]} on columns,\n" +
            " non empty {[Product].[All Products].[Drink], [Product].[All Products].[Non-Consumable]} on rows\n" +
            " from [Sales]",

            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Unit Sales CC]}\n" +
                "Axis #2:\n" +
                "{[Product].[All Products].[Drink]}\n" +
                "{[Product].[All Products].[Non-Consumable]}\n" +
                "Row #0: 6,838\n" +  // Calculated child for [Drink]
                "Row #1: 841\n")); // Calculated child for [Non-Consumable]
        Member member = executeSingletonAxis("[Product].[All Products].CalculatedChild(\"foobar\")");
        Assert.assertEquals(member, null);
    }

    public void testCalculatedChildUsingItem() {
        // Construct calculated children with the same name for both [Drink] and [Non-Consumable]
        // Then, create a metric to select the first calculated child
        assertQueryReturns(
            "with\n" +
            " member [Product].[All Products].[Drink].[Calculated Child] as '[Product].[All Products].[Drink].[Alcoholic Beverages]'\n" +
            " member [Product].[All Products].[Non-Consumable].[Calculated Child] as '[Product].[All Products].[Non-Consumable].[Carousel]'\n" +
            " member [Measures].[Unit Sales CC] as '([Measures].[Unit Sales],AddCalculatedMembers([Product].currentmember.children).Item(\"Calculated Child\"))'\n" +
            " select non empty {[Measures].[Unit Sales CC]} on columns,\n" +
            " non empty {[Product].[All Products].[Drink], [Product].[All Products].[Non-Consumable]} on rows\n" +
            " from [Sales]",

            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Unit Sales CC]}\n" +
                "Axis #2:\n" +
                "{[Product].[All Products].[Drink]}\n" +
                "{[Product].[All Products].[Non-Consumable]}\n" +
                "Row #0: 6,838\n" +
                "Row #1: 6,838\n")); // Note: For [Non-Consumable], the calculated child for [Drink] was selected!
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

    public void testCast() {
        // NOTE: Some of these tests fail with 'cannot convert ...', and they
        // probably shouldn't. Feel free to fix the conversion.
        // -- jhyde, 2006/9/3

        // From integer
        // To integer (trivial)
        assertExprReturns("0 + Cast(1 + 2 AS Integer)", "3");
        // To String
        assertExprReturns("'' || Cast(1 + 2 AS String)", "3.0");
        // To Boolean (not possible)
        assertExprThrows("1=1 AND Cast(1 + 2 AS Boolean)",
            "cannot convert value '3.0' to targetType 'BOOLEAN'");

        // From boolean
        // To String
        assertExprReturns("'' || Cast(1 = 1 AND 1 = 2 AS String)", "false");
        // To boolean (trivial)
        assertExprReturns("1=1 AND Cast(1 = 1 AND 1 = 2 AS Boolean)", "false");

        // From null
        // To Integer
        assertExprThrows("0 + Cast(NULL AS Integer)",
            "cannot convert value 'null' to targetType 'DECIMAL(0)'");
        // To Numeric
        assertExprThrows("0 + Cast(NULL AS Numeric)",
            "cannot convert value 'null' to targetType 'NUMERIC'");
        // To String
        assertExprReturns("'' || Cast(NULL AS String)", "null");
        // To Boolean
        assertExprThrows("1=1 AND Cast(NULL AS Boolean)",
            "cannot convert value 'null' to targetType 'BOOLEAN'");

        // Double is not allowed as a type
        assertExprThrows("Cast(1 AS Double)",
            "Unknown type 'Double'; values are NUMERIC, STRING, BOOLEAN");

        // An integer constant is not allowed as a type
        assertExprThrows("Cast(1 AS 5)",
            "Syntax error at line 1, column 11, token '5.0'");

        assertExprReturns("Cast('tr' || 'ue' AS boolean)", "true");
    }

    /**
     * Tests {@link mondrian.olap.FunTable#getFunInfoList()}, but more
     * importantly, generates an HTML table of all implemented functions into
     * a file called "functions.html". You can manually include that table
     * in the <a href="{@docRoot}/../mdx.html">MDX
     * specification</a>.
     */
    public void testDumpFunctions() throws IOException {
        final List funInfoList = new ArrayList();
        funInfoList.addAll(BuiltinFunTable.instance().getFunInfoList());

        // Add some UDFs.
        funInfoList.add(new FunInfo(new UdfResolver(new CurrentDateMemberExactUdf())));
        funInfoList.add(new FunInfo(new UdfResolver(new CurrentDateMemberUdf())));
        funInfoList.add(new FunInfo(new UdfResolver(new CurrentDateStringUdf())));
        Collections.sort(funInfoList);

        final File file = new File("functions.html");
        final FileOutputStream os = new FileOutputStream(file);
        final PrintWriter pw = new PrintWriter(os);
        pw.println("<table border='1'>");
        pw.println("<tr>");
        pw.println("<td><b>Name</b></td>");
        pw.println("<td><b>Description</b></td>");
        pw.println("</tr>");
        for (int i = 0; i < funInfoList.size(); i++) {
            FunInfo funInfo = (FunInfo) funInfoList.get(i);
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

    private static void printHtml(PrintWriter pw, String s) {
        final String escaped = StringEscaper.htmlEscaper.escapeString(s);
        pw.print(escaped);
    }

}

// End FunctionTest.java
