/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2003-2004 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import junit.framework.Assert;
import mondrian.olap.*;
import mondrian.test.FoodMartTestCase;

/**
 * <code>FunctionTest</code> tests the functions defined in
 * {@link BuiltinFunTable}.
 *
 * @author gjohnson
 * @version $Id$
 */
public class FunctionTest extends FoodMartTestCase {
    private static final String months = "[Time].[1997].[Q1].[1]" + nl +
            "[Time].[1997].[Q1].[2]" + nl +
            "[Time].[1997].[Q1].[3]" + nl +
            "[Time].[1997].[Q2].[4]" + nl +
            "[Time].[1997].[Q2].[5]" + nl +
            "[Time].[1997].[Q2].[6]" + nl +
            "[Time].[1997].[Q3].[7]" + nl +
            "[Time].[1997].[Q3].[8]" + nl +
            "[Time].[1997].[Q3].[9]" + nl +
            "[Time].[1997].[Q4].[10]" + nl +
            "[Time].[1997].[Q4].[11]" + nl +
            "[Time].[1997].[Q4].[12]";
    private static final String quarters = "[Time].[1997].[Q1]" + nl +
            "[Time].[1997].[Q2]" + nl +
            "[Time].[1997].[Q3]" + nl +
            "[Time].[1997].[Q4]";
    private static final String year1997 = "[Time].[1997]";

    private static final String hierarchized1997 = year1997 + nl +
                    "[Time].[1997].[Q1]" + nl +
                    "[Time].[1997].[Q1].[1]" + nl +
                    "[Time].[1997].[Q1].[2]" + nl +
                    "[Time].[1997].[Q1].[3]" + nl +
                    "[Time].[1997].[Q2]" + nl +
                    "[Time].[1997].[Q2].[4]" + nl +
                    "[Time].[1997].[Q2].[5]" + nl +
                    "[Time].[1997].[Q2].[6]" + nl +
                    "[Time].[1997].[Q3]" + nl +
                    "[Time].[1997].[Q3].[7]" + nl +
                    "[Time].[1997].[Q3].[8]" + nl +
                    "[Time].[1997].[Q3].[9]" + nl +
                    "[Time].[1997].[Q4]" + nl +
                    "[Time].[1997].[Q4].[10]" + nl +
                    "[Time].[1997].[Q4].[11]" + nl +
                    "[Time].[1997].[Q4].[12]";

    public void testDimensionHierarchy() {
        String s = executeExpr("[Time].Dimension.Name");
        Assert.assertEquals("Time", s);
    }

    public void testLevelDimension() {
        String s = executeExpr("[Time].[Year].Dimension");
        Assert.assertEquals("[Time]", s);
    }

    public void testMemberDimension() {
        String s = executeExpr("[Time].[1997].[Q2].Dimension");
        Assert.assertEquals("[Time]", s);
    }

    public void testDimensionsNumeric() {
        String s = executeExpr("Dimensions(2).Name");
        Assert.assertEquals("Store", s);
    }

    public void testDimensionsString() {
        String s = executeExpr("Dimensions(\"Store\").UniqueName");
        Assert.assertEquals("[Store]", s);
    }

    public void testTime() {
        String s = executeExpr("[Time].[1997].[Q1].[1].Hierarchy");
        Assert.assertEquals("[Time]", s);
    }

    public void testBasic9() {
        String s = executeExpr("[Gender].[All Gender].[F].Hierarchy");
        Assert.assertEquals("[Gender]", s);
    }

    public void testFirstInLevel9() {
        String s = executeExpr("[Education Level].[All Education Levels].[Bachelors Degree].Hierarchy");
        Assert.assertEquals("[Education Level]", s);
    }

    public void testHierarchyAll() {
        String s = executeExpr("[Gender].[All Gender].Hierarchy");
        Assert.assertEquals("[Gender]", s);
    }

    public void testHierarchyNull() {
        String s = executeExpr("[Gender].[All Gender].Parent.Hierarchy");
        Assert.assertEquals("[Gender]", s); // MSOLAP gives "#ERR"
    }

    public void testMemberLevel() {
        String s = executeExpr("[Time].[1997].[Q1].[1].Level.UniqueName");
        Assert.assertEquals("[Time].[Month]", s);
    }

    public void testLevelsNumeric() {
        String s = executeExpr("[Time].Levels(2).Name");
        Assert.assertEquals("Month", s);
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
        String s = executeExpr("Levels(\"[Time].[Year]\").UniqueName");
        Assert.assertEquals("[Time].[Year]", s);
    }

    public void testLevelsStringFail() {
        assertExprThrows("Levels(\"nonexistent\").UniqueName",
                "Level 'nonexistent' not found");
    }

    public void testIsEmpty() {
        runQueryCheckResult("WITH MEMBER [Measures].[Foo] AS 'Iif(IsEmpty([Measures].[Unit Sales]), 5, [Measures].[Unit Sales])'"
                + nl +
                "SELECT {[Store].[USA].[WA].children} on columns" + nl +
                "FROM Sales" + nl +
                "WHERE ( [Time].[1997].[Q4].[12]," + nl
                +
                " [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth].[Portsmouth Imported Beer],"
                + nl +
                " [Measures].[Foo])",
                "Axis #0:" + nl
                +
                "{[Time].[1997].[Q4].[12], [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth].[Portsmouth Imported Beer], [Measures].[Foo]}"
                + nl +
                "Axis #1:" + nl +
                "{[Store].[All Stores].[USA].[WA].[Bellingham]}" + nl +
                "{[Store].[All Stores].[USA].[WA].[Bremerton]}" + nl +
                "{[Store].[All Stores].[USA].[WA].[Seattle]}" + nl +
                "{[Store].[All Stores].[USA].[WA].[Spokane]}" + nl +
                "{[Store].[All Stores].[USA].[WA].[Tacoma]}" + nl +
                "{[Store].[All Stores].[USA].[WA].[Walla Walla]}" + nl +
                "{[Store].[All Stores].[USA].[WA].[Yakima]}" + nl +
                "Row #0: 5" + nl +
                "Row #0: 5" + nl +
                "Row #0: 2" + nl +
                "Row #0: 5" + nl +
                "Row #0: 11" + nl +
                "Row #0: 5" + nl +
                "Row #0: 4" + nl);
    }

    public void testAncestor() {
        Member member = executeAxis("Ancestor([Store].[USA].[CA].[Los Angeles],[Store Country])");
        Assert.assertEquals("USA", member.getName());

        assertAxisThrows("Ancestor([Store].[USA].[CA].[Los Angeles],[Promotions].[Promotion Name])",
                "Error while executing query");
    }

    public void testAncestorNumeric() {
        Member member = executeAxis("Ancestor([Store].[USA].[CA].[Los Angeles],1)");
        Assert.assertEquals("CA", member.getName());

        member = executeAxis("Ancestor([Store].[USA].[CA].[Los Angeles], 0)");
        Assert.assertEquals("Los Angeles", member.getName());

        member = executeAxis("[Sales Ragged]", "Ancestor([Store].[All Stores].[Vatican], 1)");
        Assert.assertEquals("All Stores", member.getName());

        member = executeAxis("[Sales Ragged]", "Ancestor([Store].[USA].[Washington], 1)");
        Assert.assertEquals("USA", member.getName());

        // complicated way to say "1".
        member = executeAxis("[Sales Ragged]", "Ancestor([Store].[USA].[Washington], 7 * 6 - 41)");
        Assert.assertEquals("USA", member.getName());

        member = executeAxis("[Sales Ragged]", "Ancestor([Store].[All Stores].[Vatican], 2)");
        Assert.assertNull("Ancestor at 2 must be null", member);

        member = executeAxis("[Sales Ragged]", "Ancestor([Store].[All Stores].[Vatican], -5)");
        Assert.assertNull("Ancestor at -5 must be null", member);

    }

    public void testAncestorHigher() {
        Member member = executeAxis("Ancestor([Store].[USA],[Store].[Store City])");
        Assert.assertNull(member); // MSOLAP returns null
    }

    public void testAncestorSameLevel() {
        Member member = executeAxis("Ancestor([Store].[Canada],[Store].[Store Country])");
        Assert.assertEquals("Canada", member.getName());
    }

    public void testAncestorWrongHierarchy() {
        // MSOLAP gives error "Formula error - dimensions are not
        // valid (they do not match) - in the Ancestor function"
        assertAxisThrows("Ancestor([Gender].[M],[Store].[Store Country])",
                "Error while executing query");
    }

    public void testAncestorAllLevel() {
        Member member = executeAxis("Ancestor([Store].[USA].[CA],[Store].Levels(0))");
        Assert.assertTrue(member.isAll());
    }

    public void testAncestorWithHiddenParent() throws Exception {
        Member member = executeAxis("[Sales Ragged]", "Ancestor([Store].[All Stores].[Israel].[Haifa], [Store].[Store Country])");

        assertNotNull("Member must not be null.", member);
        Assert.assertEquals("Israel", member.getName());
    }

    public void testOrdinal() throws Exception {
        Cell cell = executeExprRaw("[Sales Ragged]", "[Store].[All Stores].[Vatican].ordinal");

        assertEquals("Vatican is at level 1.", 1, ((Number)cell.getValue()).intValue());

        cell = executeExprRaw("[Sales Ragged]", "[Store].[All Stores].[USA].[Washington].ordinal");

        assertEquals("Washington is at level 3.", 3, ((Number) cell.getValue()).intValue());
    }

    public void testClosingPeriodNoArgs() {
        // MSOLAP returns [1997].[Q4], because [Time].CurrentMember =
        // [1997].
        Member member = executeAxis("ClosingPeriod()");
        Assert.assertEquals("[Time].[1997].[Q4]", member.getUniqueName());
    }

    public void testClosingPeriodLevel() {
        Member member = executeAxis("ClosingPeriod([Month])");
        Assert.assertEquals("[Time].[1997].[Q4].[12]", member.getUniqueName());
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
            Member member = executeAxis("ClosingPeriod([USA])");
            Assert.assertEquals("WA", member.getName());
        }
    }

    public void testClosingPeriodMemberLeaf() {

        Member member;
        if (false) {
            // This test is mistaken. Valid forms are ClosingPeriod(<level>)
            // and ClosingPeriod(<level>, <member>), but not
            // ClosingPeriod(<member>)
            member = executeAxis("ClosingPeriod([Time].[1997].[Q3].[8])");
            Assert.assertNull(member);
        } else {
            runQueryCheckResult(
                "with member [Measures].[Foo] as ' ClosingPeriod().uniquename '" + nl +
                "select {[Measures].[Foo]} on columns," + nl +
                "  {[Time].[1997]," + nl +
                "   [Time].[1997].[Q2]," + nl +
                "   [Time].[1997].[Q2].[4]} on rows" + nl +
                "from Sales",
                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Foo]}" + nl +
                "Axis #2:" + nl +
                "{[Time].[1997]}" + nl +
                "{[Time].[1997].[Q2]}" + nl +
                "{[Time].[1997].[Q2].[4]}" + nl +
                "Row #0: [Time].[1997].[Q4]" + nl +
                "Row #1: [Time].[1997].[Q2].[6]" + nl +
                "Row #2: [Time].[#Null]" + nl);
        }
    }

    public void testClosingPeriod() {
        assertAxisReturns("ClosingPeriod([Time].[Month], [Time].[1997].[Q3])",
                "[Time].[1997].[Q3].[9]");

        assertAxisReturns("ClosingPeriod([Time].[Quarter], [Time].[1997])",
                "[Time].[1997].[Q4]");

        assertAxisReturns("ClosingPeriod([Time].[Year], [Time].[1997])",
                "[Time].[1997]");

        assertAxisReturns("ClosingPeriod([Time].[Month], [Time].[1997])",
                "[Time].[1997].[Q4].[12]");

        assertAxisReturns("ClosingPeriod([Product].[Product Name], [Product].[All Products].[Drink])",
                "[Product].[All Products].[Drink].[Dairy].[Dairy].[Milk].[Gorilla].[Gorilla Whole Milk]");

        assertAxisReturns("[Sales Ragged]", "ClosingPeriod([Store].[Store City], [Store].[All Stores].[Israel])",
                "[Store].[All Stores].[Israel].[Israel].[Tel Aviv]");

        // Default member is [Time].[1997].
        assertAxisReturns("ClosingPeriod([Time].[Month])",
                "[Time].[1997].[Q4].[12]");

        assertAxisReturns("ClosingPeriod()", "[Time].[1997].[Q4]");

        assertAxisReturns("[Sales Ragged]", "ClosingPeriod([Store].[Store State], [Store].[All Stores].[Israel])",
                "");

        assertAxisThrows("[Sales Ragged]", "ClosingPeriod([Time].[Year], [Store].[All Stores].[Israel])",
                "The <level> and <member> arguments to ClosingPeriod must be "
                + "from the same hierarchy. The level was from '[Time]' but "
                + "the member was from '[Store]'.");
    }

    public void testClosingPeriodBelow() {
        Member member = executeAxis("ClosingPeriod([Quarter],[1997].[Q3].[8])");
        Assert.assertNull(member);
    }


    public void testCousin1() {
        Member member = executeAxis("Cousin([1997].[Q4],[1998])");
        Assert.assertEquals("[Time].[1998].[Q4]", member.getUniqueName());
    }

    public void testCousin2() {
        Member member = executeAxis("Cousin([1997].[Q4].[12],[1998].[Q1])");
        Assert.assertEquals("[Time].[1998].[Q1].[3]", member.getUniqueName());
    }

    public void testCousinOverrun() {
        Member member = executeAxis("Cousin([Customers].[USA].[CA].[San Jose], [Customers].[USA].[OR])");
        // CA has more cities than OR
        Assert.assertNull(member);
    }

    public void testCousinThreeDown() {
        Member member = executeAxis("Cousin([Customers].[USA].[CA].[Berkeley].[Alma Shelton], [Customers].[Mexico])");
        // Alma Shelton is the 3rd child
        // of the 4th child (Berkeley)
        // of the 1st child (CA)
        // of USA
        Assert.assertEquals("[Customers].[All Customers].[Mexico].[DF].[Tixapan].[Albert Clouse]", member.getUniqueName());
    }

    public void testCousinSameLevel() {
        Member member = executeAxis("Cousin([Gender].[M], [Gender].[F])");
        Assert.assertEquals("F", member.getName());
    }

    public void testCousinHigherLevel() {
        Member member = executeAxis("Cousin([Time].[1997], [Time].[1998].[Q1])");
        Assert.assertNull(member);
    }

    public void testCousinWrongHierarchy() {
        assertAxisThrows("Cousin([Time].[1997], [Gender].[M])",
                MondrianResource.instance().getCousinHierarchyMismatch("[Time].[1997]", "[Gender].[All Gender].[M]"));
    }

    public void testCurrentMemberFromSlicer() {
        Result result = runQuery("with member [Measures].[Foo] as '[Gender].CurrentMember.Name'" + nl +
                "select {[Measures].[Foo]} on columns" + nl +
                "from Sales where ([Gender].[F])");
        Assert.assertEquals("F", result.getCell(new int[]{0}).getValue());
    }

    public void testCurrentMemberFromDefaultMember() {
        Result result = runQuery("with member [Measures].[Foo] as '[Time].CurrentMember.Name'" + nl +
                "select {[Measures].[Foo]} on columns" + nl +
                "from Sales");
        Assert.assertEquals("1997", result.getCell(new int[]{0}).getValue());
    }

    public void testCurrentMemberFromAxis() {
        Result result = runQuery("with member [Measures].[Foo] as '[Gender].CurrentMember.Name || [Marital Status].CurrentMember.Name'"
                + nl +
                "select {[Measures].[Foo]} on columns," + nl +
                " CrossJoin({[Gender].children}, {[Marital Status].children}) on rows" + nl +
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
        Result result = runQuery("with member [Measures].[Foo] as '[Measures].CurrentMember.Name'" + nl +
                "select {[Measures].[Foo]} on columns" + nl +
                "from Sales");
        Assert.assertEquals("Unit Sales", result.getCell(new int[]{0}).getValue());
    }

    public void testDimensionDefaultMember() {
        Member member = executeAxis("[Measures].DefaultMember");
        Assert.assertEquals("Unit Sales", member.getName());
    }

    public void testDrilldownLevel() throws Exception {
        // Expect all children of USA
        assertAxisReturns("DrilldownLevel({[Store].[USA]}, [Store].[Store Country])",
                "[Store].[All Stores].[USA]" + nl +
                "[Store].[All Stores].[USA].[CA]" + nl +
                "[Store].[All Stores].[USA].[OR]" + nl +
                "[Store].[All Stores].[USA].[WA]");

        // Expect same set, because [USA] is already drilled
        assertAxisReturns("DrilldownLevel({[Store].[USA], [Store].[USA].[CA]}, [Store].[Store Country])",
                "[Store].[All Stores].[USA]" + nl +
                "[Store].[All Stores].[USA].[CA]");

        // Expect drill, because [USA] isn't already drilled. You can't
        // drill down on [CA] and get to [USA]
        assertAxisReturns("DrilldownLevel({[Store].[USA].[CA],[Store].[USA]}, [Store].[Store Country])",
                "[Store].[All Stores].[USA].[CA]" + nl +
                "[Store].[All Stores].[USA]" + nl +
                "[Store].[All Stores].[USA].[CA]" + nl +
                "[Store].[All Stores].[USA].[OR]" + nl +
                "[Store].[All Stores].[USA].[WA]");

        assertThrows("select DrilldownLevel({[Store].[USA].[CA],[Store].[USA]}, , 0) on columns from [Sales]",
                "Syntax error");
    }


    public void testDrilldownMember() throws Exception {
        
        // Expect all children of USA
        assertAxisReturns("DrilldownMember({[Store].[USA]}, {[Store].[USA]})",
                "[Store].[All Stores].[USA]" + nl +
                "[Store].[All Stores].[USA].[CA]" + nl +
                "[Store].[All Stores].[USA].[OR]" + nl +
                "[Store].[All Stores].[USA].[WA]");
                
        // Expect all children of USA.CA and USA.OR        
        assertAxisReturns("DrilldownMember({[Store].[USA].[CA], [Store].[USA].[OR]}, "+
        	"{[Store].[USA].[CA], [Store].[USA].[OR], [Store].[USA].[WA]})",
                "[Store].[All Stores].[USA].[CA]" + nl +
                "[Store].[All Stores].[USA].[CA].[Alameda]" + nl +
                "[Store].[All Stores].[USA].[CA].[Beverly Hills]" + nl +
                "[Store].[All Stores].[USA].[CA].[Los Angeles]" + nl +
                "[Store].[All Stores].[USA].[CA].[San Diego]" + nl +
                "[Store].[All Stores].[USA].[CA].[San Francisco]" + nl +
                "[Store].[All Stores].[USA].[OR]" + nl +
                "[Store].[All Stores].[USA].[OR].[Portland]" + nl +
                "[Store].[All Stores].[USA].[OR].[Salem]");

        
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
        		"[Store].[All Stores].[USA]" + nl +
                "[Store].[All Stores].[USA].[CA]" + nl +
                "[Store].[All Stores].[USA].[CA].[Alameda]" + nl +
                "[Store].[All Stores].[USA].[CA].[Beverly Hills]" + nl +
                "[Store].[All Stores].[USA].[CA].[Los Angeles]" + nl +
                "[Store].[All Stores].[USA].[CA].[San Diego]" + nl +
                "[Store].[All Stores].[USA].[CA].[San Diego].[Store 24]" + nl +
                "[Store].[All Stores].[USA].[CA].[San Francisco]" + nl +
                "[Store].[All Stores].[USA].[OR]" + nl +
                "[Store].[All Stores].[USA].[WA]" + nl +
                "[Store].[All Stores].[USA].[WA].[Bellingham]" + nl +
                "[Store].[All Stores].[USA].[WA].[Bremerton]" + nl +
                "[Store].[All Stores].[USA].[WA].[Seattle]" + nl +
                "[Store].[All Stores].[USA].[WA].[Spokane]" + nl +
                "[Store].[All Stores].[USA].[WA].[Tacoma]" + nl +
                "[Store].[All Stores].[USA].[WA].[Walla Walla]" + nl +
                "[Store].[All Stores].[USA].[WA].[Yakima]");
  		
        // Sets of tuples
        assertAxisReturns("DrilldownMember({([Store Type].[Supermarket], [Store].[USA])}, {[Store].[USA]})",
                "{[Store Type].[All Store Types].[Supermarket], [Store].[All Stores].[USA]}" + nl +
                "{[Store Type].[All Store Types].[Supermarket], [Store].[All Stores].[USA].[CA]}" + nl +
                "{[Store Type].[All Store Types].[Supermarket], [Store].[All Stores].[USA].[OR]}" + nl +
                "{[Store Type].[All Store Types].[Supermarket], [Store].[All Stores].[USA].[WA]}");
    }


    public void testFirstChildFirstInLevel() {
        Member member = executeAxis("[Time].[1997].[Q4].FirstChild");
        Assert.assertEquals("10", member.getName());
    }

    public void testFirstChildAll() {
        Member member = executeAxis("[Gender].[All Gender].FirstChild");
        Assert.assertEquals("F", member.getName());
    }

    public void testFirstChildOfChildless() {
        Member member = executeAxis("[Gender].[All Gender].[F].FirstChild");
        Assert.assertNull(member);
    }

    public void testFirstSiblingFirstInLevel() {
        Member member = executeAxis("[Gender].[F].FirstSibling");
        Assert.assertEquals("F", member.getName());
    }

    public void testFirstSiblingLastInLevel() {
        Member member = executeAxis("[Time].[1997].[Q4].FirstSibling");
        Assert.assertEquals("Q1", member.getName());
    }

    public void testFirstSiblingAll() {
        Member member = executeAxis("[Gender].[All Gender].FirstSibling");
        Assert.assertTrue(member.isAll());
    }

    public void testFirstSiblingRoot() {
        // The [Measures] hierarchy does not have an 'all' member, so
        // [Unit Sales] does not have a parent.
        Member member = executeAxis("[Measures].[Store Sales].FirstSibling");
        Assert.assertEquals("Unit Sales", member.getName());
    }

    public void testFirstSiblingNull() {
        Member member = executeAxis("[Gender].[F].FirstChild.FirstSibling");
        Assert.assertNull(member);
    }

    public void testLag() {
        Member member = executeAxis("[Time].[1997].[Q4].[12].Lag(4)");
        Assert.assertEquals("8", member.getName());
    }

    public void testLagFirstInLevel() {
        Member member = executeAxis("[Gender].[F].Lag(1)");
        Assert.assertNull(member);
    }

    public void testLagAll() {
        Member member = executeAxis("[Gender].DefaultMember.Lag(2)");
        Assert.assertNull(member);
    }

    public void testLagRoot() {
        Member member = executeAxis("[Time].[1998].Lag(1)");
        Assert.assertEquals("1997", member.getName());
    }

    public void testLagRootTooFar() {
        Member member = executeAxis("[Time].[1998].Lag(2)");
        Assert.assertNull(member);
    }

    public void testLastChild() {
        Member member = executeAxis("[Gender].LastChild");
        Assert.assertEquals("M", member.getName());
    }

    public void testLastChildLastInLevel() {
        Member member = executeAxis("[Time].[1997].[Q4].LastChild");
        Assert.assertEquals("12", member.getName());
    }

    public void testLastChildAll() {
        Member member = executeAxis("[Gender].[All Gender].LastChild");
        Assert.assertEquals("M", member.getName());
    }

    public void testLastChildOfChildless() {
        Member member = executeAxis("[Gender].[M].LastChild");
        Assert.assertNull(member);
    }

    public void testLastSibling() {
        Member member = executeAxis("[Gender].[F].LastSibling");
        Assert.assertEquals("M", member.getName());
    }

    public void testLastSiblingFirstInLevel() {
        Member member = executeAxis("[Time].[1997].[Q1].LastSibling");
        Assert.assertEquals("Q4", member.getName());
    }

    public void testLastSiblingAll() {
        Member member = executeAxis("[Gender].[All Gender].LastSibling");
        Assert.assertTrue(member.isAll());
    }

    public void testLastSiblingRoot() {
        // The [Time] hierarchy does not have an 'all' member, so
        // [1997], [1998] do not have parents.
        Member member = executeAxis("[Time].[1998].LastSibling");
        Assert.assertEquals("1998", member.getName());
    }

    public void testLastSiblingNull() {
        Member member = executeAxis("[Gender].[F].FirstChild.LastSibling");
        Assert.assertNull(member);
    }


    public void testLead() {
        Member member = executeAxis("[Time].[1997].[Q2].[4].Lead(4)");
        Assert.assertEquals("8", member.getName());
    }

    public void testLeadNegative() {
        Member member = executeAxis("[Gender].[M].Lead(-1)");
        Assert.assertEquals("F", member.getName());
    }

    public void testLeadLastInLevel() {
        Member member = executeAxis("[Gender].[M].Lead(3)");
        Assert.assertNull(member);
    }


    public void testBasic2() {
        Result result = runQuery("select {[Gender].[F].NextMember} ON COLUMNS from Sales");
        Assert.assertTrue(result.getAxes()[0].positions[0].members[0].getName().equals("M"));
    }

    public void testFirstInLevel2() {
        Result result = runQuery("select {[Gender].[M].NextMember} ON COLUMNS from Sales");
        Assert.assertTrue(result.getAxes()[0].positions.length == 0);
    }

    public void testAll2() {
        Result result = runQuery("select {[Gender].PrevMember} ON COLUMNS from Sales");
        // previous to [Gender].[All] is null, so no members are returned
        Assert.assertTrue(result.getAxes()[0].positions.length == 0);
    }


    public void testBasic5() {
        Result result = runQuery("select{ [Product].[All Products].[Drink].Parent} on columns from Sales");
        Assert.assertTrue(result.getAxes()[0].positions[0].members[0].getName().equals("All Products"));
    }

    public void testFirstInLevel5() {
        Result result = runQuery("select {[Time].[1997].[Q2].[4].Parent} on columns,{[Gender].[M]} on rows from Sales");
        Assert.assertTrue(result.getAxes()[0].positions[0].members[0].getName().equals("Q2"));
    }

    public void testAll5() {
        Result result = runQuery("select {[Time].[1997].[Q2].Parent} on columns,{[Gender].[M]} on rows from Sales");
        // previous to [Gender].[All] is null, so no members are returned
        Assert.assertTrue(result.getAxes()[0].positions[0].members[0].getName().equals("1997"));
    }


    public void testBasic() {
        Result result = runQuery("select {[Gender].[M].PrevMember} ON COLUMNS from Sales");
        Assert.assertTrue(result.getAxes()[0].positions[0].members[0].getName().equals("F"));
    }

    public void testFirstInLevel() {
        Result result = runQuery("select {[Gender].[F].PrevMember} ON COLUMNS from Sales");
        Assert.assertTrue(result.getAxes()[0].positions.length == 0);
    }

    public void testAll() {
        Result result = runQuery("select {[Gender].PrevMember} ON COLUMNS from Sales");
        // previous to [Gender].[All] is null, so no members are returned
        Assert.assertTrue(result.getAxes()[0].positions.length == 0);
    }

    public void testAggregate() {
        runQueryCheckResult("WITH MEMBER [Store].[CA plus OR] AS 'AGGREGATE({[Store].[USA].[CA], [Store].[USA].[OR]})'"
                + nl +
                "SELECT {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS," + nl +
                "      {[Store].[USA].[CA], [Store].[USA].[OR], [Store].[CA plus OR]} ON ROWS" + nl +
                "FROM Sales" + nl +
                "WHERE ([1997].[Q1])",
                "Axis #0:" + nl +
                "{[Time].[1997].[Q1]}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Unit Sales]}" + nl +
                "{[Measures].[Store Sales]}" + nl +
                "Axis #2:" + nl +
                "{[Store].[All Stores].[USA].[CA]}" + nl +
                "{[Store].[All Stores].[USA].[OR]}" + nl +
                "{[Store].[CA plus OR]}" + nl +
                "Row #0: 16,890" + nl +
                "Row #0: 36,175.20" + nl +
                "Row #1: 19,287" + nl +
                "Row #1: 40,170.29" + nl +
                "Row #2: 36,177" + nl +
                "Row #2: 76,345.49" + nl);
    }

    public void testAggregate2() {
        runQueryCheckResult("WITH" + nl +
                "  MEMBER [Time].[1st Half Sales] AS 'Aggregate({Time.[1997].[Q1], Time.[1997].[Q2]})'" + nl +
                "  MEMBER [Time].[2nd Half Sales] AS 'Aggregate({Time.[1997].[Q3], Time.[1997].[Q4]})'" + nl +
                "  MEMBER [Time].[Difference] AS 'Time.[2nd Half Sales] - Time.[1st Half Sales]'" + nl +
                "SELECT" + nl +
                "   { [Store].[Store State].Members} ON COLUMNS," + nl +
                "   { Time.[1st Half Sales], Time.[2nd Half Sales], Time.[Difference]} ON ROWS" + nl +
                "FROM Sales" + nl +
                "WHERE [Measures].[Store Sales]",
                "Axis #0:" + nl +
                "{[Measures].[Store Sales]}" + nl +
                "Axis #1:" + nl +
                "{[Store].[All Stores].[Canada].[BC]}" + nl +
                "{[Store].[All Stores].[Mexico].[DF]}" + nl +
                "{[Store].[All Stores].[Mexico].[Guerrero]}" + nl +
                "{[Store].[All Stores].[Mexico].[Jalisco]}" + nl +
                "{[Store].[All Stores].[Mexico].[Veracruz]}" + nl +
                "{[Store].[All Stores].[Mexico].[Yucatan]}" + nl +
                "{[Store].[All Stores].[Mexico].[Zacatecas]}" + nl +
                "{[Store].[All Stores].[USA].[CA]}" + nl +
                "{[Store].[All Stores].[USA].[OR]}" + nl +
                "{[Store].[All Stores].[USA].[WA]}" + nl +
                "Axis #2:" + nl +
                "{[Time].[1st Half Sales]}" + nl +
                "{[Time].[2nd Half Sales]}" + nl +
                "{[Time].[Difference]}" + nl +
                "Row #0: (null)" + nl +
                "Row #0: (null)" + nl +
                "Row #0: (null)" + nl +
                "Row #0: (null)" + nl +
                "Row #0: (null)" + nl +
                "Row #0: (null)" + nl +
                "Row #0: (null)" + nl +
                "Row #0: 74,571.95" + nl +
                "Row #0: 71,943.17" + nl +
                "Row #0: 125,779.50" + nl +
                "Row #1: (null)" + nl +
                "Row #1: (null)" + nl +
                "Row #1: (null)" + nl +
                "Row #1: (null)" + nl +
                "Row #1: (null)" + nl +
                "Row #1: (null)" + nl +
                "Row #1: (null)" + nl +
                "Row #1: 84,595.89" + nl +
                "Row #1: 70,333.90" + nl +
                "Row #1: 138,013.72" + nl +
                "Row #2: (null)" + nl +
                "Row #2: (null)" + nl +
                "Row #2: (null)" + nl +
                "Row #2: (null)" + nl +
                "Row #2: (null)" + nl +
                "Row #2: (null)" + nl +
                "Row #2: (null)" + nl +
                "Row #2: 10,023.94" + nl +
                "Row #2: -1,609.27" + nl +
                "Row #2: 12,234.22" + nl);
    }

    public void testAggregateToSimulateCompoundSlicer() {
        runQueryCheckResult("WITH MEMBER [Time].[1997 H1] as 'Aggregate({[Time].[1997].[Q1], [Time].[1997].[Q2]})'"
                + nl
                +
                "  MEMBER [Education Level].[College or higher] as 'Aggregate({[Education Level].[Bachelors Degree], [Education Level].[Graduate Degree]})'"
                + nl +
                "SELECT {[Measures].[Unit Sales], [Measures].[Store Sales]} on columns," + nl +
                "  {[Product].children} on rows" + nl +
                "FROM [Sales]" + nl +
                "WHERE ([Time].[1997 H1], [Education Level].[College or higher], [Gender].[F])",
                "Axis #0:" + nl +
                "{[Time].[1997 H1], [Education Level].[College or higher], [Gender].[All Gender].[F]}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Unit Sales]}" + nl +
                "{[Measures].[Store Sales]}" + nl +
                "Axis #2:" + nl +
                "{[Product].[All Products].[Drink]}" + nl +
                "{[Product].[All Products].[Food]}" + nl +
                "{[Product].[All Products].[Non-Consumable]}" + nl +
                "Row #0: 1,797" + nl +
                "Row #0: 3,620.49" + nl +
                "Row #1: 15,002" + nl +
                "Row #1: 31,931.88" + nl +
                "Row #2: 3,845" + nl +
                "Row #2: 8,173.22" + nl);
    }

    public void testAvg() {
        String result = executeExpr("AVG({[Store].[All Stores].[USA].children},[Measures].[Store Sales])");
        Assert.assertEquals("188,412.71", result);
    }
    //todo: testAvgWithNulls

    public void testCorrelation() {
        String result = executeExpr("Correlation({[Store].[All Stores].[USA].children}, [Measures].[Unit Sales], [Measures].[Store Sales]) * 1000000");
        Assert.assertEquals("999,906", result);
    }

    public void testCount() {
        String result = executeExpr("count({[Promotion Media].[Media Type].members})");
        Assert.assertEquals("14", result);
    }

    public void testCountExcludeEmpty() {
        runQueryCheckResult("with member [Measures].[Promo Count] as " + nl +
                " ' Count(Crossjoin({[Measures].[Unit Sales]}," + nl +
                " {[Promotion Media].[Media Type].members}), EXCLUDEEMPTY)'" + nl +
                "select {[Measures].[Unit Sales], [Measures].[Promo Count]} on columns," + nl +
                " {[Product].[Drink].[Beverages].[Carbonated Beverages].[Soda].children} on rows" + nl +
                "from Sales",
                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Unit Sales]}" + nl +
                "{[Measures].[Promo Count]}" + nl +
                "Axis #2:" + nl +
                "{[Product].[All Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Excellent]}" + nl +
                "{[Product].[All Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Fabulous]}" + nl +
                "{[Product].[All Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Skinner]}" + nl +
                "{[Product].[All Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Token]}" + nl +
                "{[Product].[All Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Washington]}" + nl +
                "Row #0: 738" + nl +
                "Row #0: 14" + nl +
                "Row #1: 632" + nl +
                "Row #1: 13" + nl +
                "Row #2: 655" + nl +
                "Row #2: 14" + nl +
                "Row #3: 735" + nl +
                "Row #3: 14" + nl +
                "Row #4: 647" + nl +
                "Row #4: 12" + nl);
    }
    //todo: testCountNull, testCountNoExp

    public void testCovariance() {
        String result = executeExpr("Covariance({[Store].[All Stores].[USA].children}, [Measures].[Unit Sales], [Measures].[Store Sales])");
        Assert.assertEquals("1,355,761,899", result);
    }

    public void testCovarianceN() {
        String result = executeExpr("CovarianceN({[Store].[All Stores].[USA].children}, [Measures].[Unit Sales], [Measures].[Store Sales])");
        Assert.assertEquals("2,033,642,849", result);
    }


    public void testIIfNumeric() {
        String s = executeExpr("IIf(([Measures].[Unit Sales],[Product].[Drink].[Alcoholic Beverages].[Beer and Wine]) > 100, 45, 32)");
        Assert.assertEquals("45", s);
    }

    public void testMax() {
        String result = executeExpr("MAX({[Store].[All Stores].[USA].children},[Measures].[Store Sales])");
        Assert.assertEquals("263,793.22", result);
    }

    public void testMedian() {
        String result = executeExpr("MEDIAN({[Store].[All Stores].[USA].children},[Measures].[Store Sales])");
        Assert.assertEquals("159,167.84", result);
    }

    public void testMedian2() {
        runQueryCheckResult("WITH" + nl +
                "   MEMBER [Time].[1st Half Sales] AS 'Sum({[Time].[1997].[Q1], [Time].[1997].[Q2]})'" + nl +
                "   MEMBER [Time].[2nd Half Sales] AS 'Sum({[Time].[1997].[Q3], [Time].[1997].[Q4]})'" + nl +
                "   MEMBER [Time].[Median] AS 'Median(Time.Members)'" + nl +
                "SELECT" + nl +
                "   NON EMPTY { [Store].[Store Name].Members} ON COLUMNS," + nl +
                "   { [Time].[1st Half Sales], [Time].[2nd Half Sales], [Time].[Median]} ON ROWS" + nl +
                "FROM Sales" + nl +
                "WHERE [Measures].[Store Sales]",

                "Axis #0:" + nl +
                "{[Measures].[Store Sales]}" + nl +
                "Axis #1:" + nl +
                "{[Store].[All Stores].[USA].[CA].[Beverly Hills].[Store 6]}" + nl +
                "{[Store].[All Stores].[USA].[CA].[Los Angeles].[Store 7]}" + nl +
                "{[Store].[All Stores].[USA].[CA].[San Diego].[Store 24]}" + nl +
                "{[Store].[All Stores].[USA].[CA].[San Francisco].[Store 14]}" + nl +
                "{[Store].[All Stores].[USA].[OR].[Portland].[Store 11]}" + nl +
                "{[Store].[All Stores].[USA].[OR].[Salem].[Store 13]}" + nl +
                "{[Store].[All Stores].[USA].[WA].[Bellingham].[Store 2]}" + nl +
                "{[Store].[All Stores].[USA].[WA].[Bremerton].[Store 3]}" + nl +
                "{[Store].[All Stores].[USA].[WA].[Seattle].[Store 15]}" + nl +
                "{[Store].[All Stores].[USA].[WA].[Spokane].[Store 16]}" + nl +
                "{[Store].[All Stores].[USA].[WA].[Tacoma].[Store 17]}" + nl +
                "{[Store].[All Stores].[USA].[WA].[Walla Walla].[Store 22]}" + nl +
                "{[Store].[All Stores].[USA].[WA].[Yakima].[Store 23]}" + nl +
                "Axis #2:" + nl +
                "{[Time].[1st Half Sales]}" + nl +
                "{[Time].[2nd Half Sales]}" + nl +
                "{[Time].[Median]}" + nl +
                "Row #0: 20,801.04" + nl +
                "Row #0: 25,421.41" + nl +
                "Row #0: 26,275.11" + nl +
                "Row #0: 2,074.39" + nl +
                "Row #0: 28,519.18" + nl +
                "Row #0: 43,423.99" + nl +
                "Row #0: 2,140.99" + nl +
                "Row #0: 25,502.08" + nl +
                "Row #0: 25,293.50" + nl +
                "Row #0: 23,265.53" + nl +
                "Row #0: 34,926.91" + nl +
                "Row #0: 2,159.60" + nl +
                "Row #0: 12,490.89" + nl +
                "Row #1: 24,949.20" + nl +
                "Row #1: 29,123.87" + nl +
                "Row #1: 28,156.03" + nl +
                "Row #1: 2,366.79" + nl +
                "Row #1: 26,539.61" + nl +
                "Row #1: 43,794.29" + nl +
                "Row #1: 2,598.24" + nl +
                "Row #1: 27,394.22" + nl +
                "Row #1: 27,350.57" + nl +
                "Row #1: 26,368.93" + nl +
                "Row #1: 39,917.05" + nl +
                "Row #1: 2,546.37" + nl +
                "Row #1: 11,838.34" + nl +
                "Row #2: 4,577.35" + nl +
                "Row #2: 5,211.38" + nl +
                "Row #2: 4,722.87" + nl +
                "Row #2: 398.24" + nl +
                "Row #2: 5,039.50" + nl +
                "Row #2: 7,374.59" + nl +
                "Row #2: 410.22" + nl +
                "Row #2: 4,924.04" + nl +
                "Row #2: 4,569.13" + nl +
                "Row #2: 4,511.68" + nl +
                "Row #2: 6,630.91" + nl +
                "Row #2: 419.51" + nl +
                "Row #2: 2,169.48" + nl);
    }

    public void testMin() {
        String result = executeExpr("MIN({[Store].[All Stores].[USA].children},[Measures].[Store Sales])");
        Assert.assertEquals("142,277.07", result);
    }

    public void testMinTupel() {
        String result = executeExpr("Min([Customers].[All Customers].[USA].Children, ([Measures].[Unit Sales], [Gender].[All Gender].[F]))");
        Assert.assertEquals("33,036", result);
    }
    public void testStdev() {
        String result = executeExpr("STDEV({[Store].[All Stores].[USA].children},[Measures].[Store Sales])");
        Assert.assertEquals("65,825.45", result);
    }

    public void testStdevP() {
        String result = executeExpr("STDEVP({[Store].[All Stores].[USA].children},[Measures].[Store Sales])");
        Assert.assertEquals("53,746.26", result);
    }

    public void testSumNoExp() {
        String result = executeExpr("SUM({[Promotion Media].[Media Type].members})");
        Assert.assertEquals("266,773", result);
    }

    public void testVar() {
        String result = executeExpr("VAR({[Store].[All Stores].[USA].children},[Measures].[Store Sales])");
        Assert.assertEquals("4,332,990,493.69", result);
    }

    public void testVarP() {
        String result = executeExpr("VARP({[Store].[All Stores].[USA].children},[Measures].[Store Sales])");
        Assert.assertEquals("2,888,660,329.13", result);
    }


    public void testAscendants() {
        assertAxisReturns("Ascendants([Store].[USA].[CA])",
                "[Store].[All Stores].[USA].[CA]" + nl +
                "[Store].[All Stores].[USA]" + nl +
                "[Store].[All Stores]");
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
                "[Promotion Media].[All Media].[Radio]" + nl +
                "[Promotion Media].[All Media].[Sunday Paper, Radio, TV]");
    }
    //todo: test unordered

    public void testBottomPercent() {
        assertAxisReturns("BottomPercent({[Promotion Media].[Media Type].members}, 1, [Measures].[Unit Sales])",
                "[Promotion Media].[All Media].[Radio]" + nl +
                "[Promotion Media].[All Media].[Sunday Paper, Radio, TV]");
    }
    //todo: test precision

    public void testBottomSum() {
        assertAxisReturns("BottomSum({[Promotion Media].[Media Type].members}, 5000, [Measures].[Unit Sales])",
                "[Promotion Media].[All Media].[Radio]" + nl +
                "[Promotion Media].[All Media].[Sunday Paper, Radio, TV]");
    }

    public void testCrossjoinNested() {
        assertAxisReturns("  CrossJoin(" + nl +
                "    CrossJoin(" + nl +
                "      [Gender].members," + nl +
                "      [Marital Status].members)," + nl +
                "   {[Store], [Store].children})",

                "{[Gender].[All Gender], [Marital Status].[All Marital Status], [Store].[All Stores]}" + nl +
                "{[Gender].[All Gender], [Marital Status].[All Marital Status], [Store].[All Stores].[Canada]}" + nl +
                "{[Gender].[All Gender], [Marital Status].[All Marital Status], [Store].[All Stores].[Mexico]}" + nl +
                "{[Gender].[All Gender], [Marital Status].[All Marital Status], [Store].[All Stores].[USA]}" + nl +
                "{[Gender].[All Gender], [Marital Status].[All Marital Status].[M], [Store].[All Stores]}" + nl +
                "{[Gender].[All Gender], [Marital Status].[All Marital Status].[M], [Store].[All Stores].[Canada]}"
                + nl +
                "{[Gender].[All Gender], [Marital Status].[All Marital Status].[M], [Store].[All Stores].[Mexico]}"
                + nl +
                "{[Gender].[All Gender], [Marital Status].[All Marital Status].[M], [Store].[All Stores].[USA]}" + nl +
                "{[Gender].[All Gender], [Marital Status].[All Marital Status].[S], [Store].[All Stores]}" + nl +
                "{[Gender].[All Gender], [Marital Status].[All Marital Status].[S], [Store].[All Stores].[Canada]}"
                + nl +
                "{[Gender].[All Gender], [Marital Status].[All Marital Status].[S], [Store].[All Stores].[Mexico]}"
                + nl +
                "{[Gender].[All Gender], [Marital Status].[All Marital Status].[S], [Store].[All Stores].[USA]}" + nl +
                "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status], [Store].[All Stores]}" + nl +
                "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status], [Store].[All Stores].[Canada]}"
                + nl +
                "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status], [Store].[All Stores].[Mexico]}"
                + nl +
                "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status], [Store].[All Stores].[USA]}" + nl +
                "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[M], [Store].[All Stores]}" + nl
                +
                "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[M], [Store].[All Stores].[Canada]}"
                + nl
                +
                "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[M], [Store].[All Stores].[Mexico]}"
                + nl +
                "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[M], [Store].[All Stores].[USA]}"
                + nl +
                "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S], [Store].[All Stores]}" + nl
                +
                "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S], [Store].[All Stores].[Canada]}"
                + nl
                +
                "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S], [Store].[All Stores].[Mexico]}"
                + nl +
                "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S], [Store].[All Stores].[USA]}"
                + nl +
                "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status], [Store].[All Stores]}" + nl +
                "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status], [Store].[All Stores].[Canada]}"
                + nl +
                "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status], [Store].[All Stores].[Mexico]}"
                + nl +
                "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status], [Store].[All Stores].[USA]}" + nl +
                "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M], [Store].[All Stores]}" + nl
                +
                "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M], [Store].[All Stores].[Canada]}"
                + nl
                +
                "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M], [Store].[All Stores].[Mexico]}"
                + nl +
                "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M], [Store].[All Stores].[USA]}"
                + nl +
                "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Store].[All Stores]}" + nl
                +
                "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Store].[All Stores].[Canada]}"
                + nl
                +
                "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Store].[All Stores].[Mexico]}"
                + nl +
                "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Store].[All Stores].[USA]}");
    }

    public void testCrossjoinSingletonTuples() {
        assertAxisReturns("CrossJoin({([Gender].[M])}, {([Marital Status].[S])})",
                "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S]}");
    }

    public void testCrossjoinSingletonTuplesNested() {
        assertAxisReturns("CrossJoin({([Gender].[M])}, CrossJoin({([Marital Status].[S])}, [Store].children))",
                "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Store].[All Stores].[Canada]}"
                + nl
                +
                "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Store].[All Stores].[Mexico]}"
                + nl +
                "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Store].[All Stores].[USA]}");
    }

    public void testCrossjoinAsterisk() {
        assertAxisReturns("{[Gender].[M]} * {[Marital Status].[S]}",
                "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S]}");
    }

    public void testCrossjoinAsteriskAssoc() {
        assertAxisReturns("Order({[Gender].Children} * {[Marital Status].Children} * {[Time].[1997].[Q2].Children}," +
                "[Measures].[Unit Sales])",
                "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[M], [Time].[1997].[Q2].[4]}" + nl +
                "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[M], [Time].[1997].[Q2].[6]}" + nl +
                "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[M], [Time].[1997].[Q2].[5]}" + nl +
                "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S], [Time].[1997].[Q2].[4]}" + nl +
                "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S], [Time].[1997].[Q2].[5]}" + nl +
                "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S], [Time].[1997].[Q2].[6]}" + nl +
                "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M], [Time].[1997].[Q2].[4]}" + nl +
                "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M], [Time].[1997].[Q2].[5]}" + nl +
                "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M], [Time].[1997].[Q2].[6]}" + nl +
                "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Time].[1997].[Q2].[6]}" + nl +
                "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Time].[1997].[Q2].[4]}" + nl +
                "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Time].[1997].[Q2].[5]}");
    }

    public void testCrossjoinAsteriskInsideBraces() {
        assertAxisReturns("{[Gender].[M] * [Marital Status].[S] * [Time].[1997].[Q2].Children}",
                "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Time].[1997].[Q2].[4]}" + nl +
                "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Time].[1997].[Q2].[5]}" + nl +
                "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], [Time].[1997].[Q2].[6]}");
    }

    public void testCrossJoinAsteriskQuery() {
        runQueryCheckResult("SELECT {[Measures].members * [1997].children} ON COLUMNS," + nl +
                " {[Store].[USA].children * [Position].[All Position].children} DIMENSION PROPERTIES [Store].[Store SQFT] ON ROWS" + nl +
                "FROM [HR]",
                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Org Salary], [Time].[1997].[Q1]}" + nl +
                "{[Measures].[Org Salary], [Time].[1997].[Q2]}" + nl +
                "{[Measures].[Org Salary], [Time].[1997].[Q3]}" + nl +
                "{[Measures].[Org Salary], [Time].[1997].[Q4]}" + nl +
                "{[Measures].[Count], [Time].[1997].[Q1]}" + nl +
                "{[Measures].[Count], [Time].[1997].[Q2]}" + nl +
                "{[Measures].[Count], [Time].[1997].[Q3]}" + nl +
                "{[Measures].[Count], [Time].[1997].[Q4]}" + nl +
                "{[Measures].[Number of Employees], [Time].[1997].[Q1]}" + nl +
                "{[Measures].[Number of Employees], [Time].[1997].[Q2]}" + nl +
                "{[Measures].[Number of Employees], [Time].[1997].[Q3]}" + nl +
                "{[Measures].[Number of Employees], [Time].[1997].[Q4]}" + nl +
                "Axis #2:" + nl +
                "{[Store].[All Stores].[USA].[CA], [Position].[All Position].[Middle Management]}" + nl +
                "{[Store].[All Stores].[USA].[CA], [Position].[All Position].[Senior Management]}" + nl +
                "{[Store].[All Stores].[USA].[CA], [Position].[All Position].[Store Full Time Staf]}" + nl +
                "{[Store].[All Stores].[USA].[CA], [Position].[All Position].[Store Management]}" + nl +
                "{[Store].[All Stores].[USA].[CA], [Position].[All Position].[Store Temp Staff]}" + nl +
                "{[Store].[All Stores].[USA].[OR], [Position].[All Position].[Middle Management]}" + nl +
                "{[Store].[All Stores].[USA].[OR], [Position].[All Position].[Senior Management]}" + nl +
                "{[Store].[All Stores].[USA].[OR], [Position].[All Position].[Store Full Time Staf]}" + nl +
                "{[Store].[All Stores].[USA].[OR], [Position].[All Position].[Store Management]}" + nl +
                "{[Store].[All Stores].[USA].[OR], [Position].[All Position].[Store Temp Staff]}" + nl +
                "{[Store].[All Stores].[USA].[WA], [Position].[All Position].[Middle Management]}" + nl +
                "{[Store].[All Stores].[USA].[WA], [Position].[All Position].[Senior Management]}" + nl +
                "{[Store].[All Stores].[USA].[WA], [Position].[All Position].[Store Full Time Staf]}" + nl +
                "{[Store].[All Stores].[USA].[WA], [Position].[All Position].[Store Management]}" + nl +
                "{[Store].[All Stores].[USA].[WA], [Position].[All Position].[Store Temp Staff]}" + nl +
                "Row #0: $275.40" + nl +
                "Row #0: $275.40" + nl +
                "Row #0: $275.40" + nl +
                "Row #0: $275.40" + nl +
                "Row #0: 27" + nl +
                "Row #0: 27" + nl +
                "Row #0: 27" + nl +
                "Row #0: 27" + nl +
                "Row #0: 9" + nl +
                "Row #0: 9" + nl +
                "Row #0: 9" + nl +
                "Row #0: 9" + nl +
                "Row #1: $837.00" + nl +
                "Row #1: $837.00" + nl +
                "Row #1: $837.00" + nl +
                "Row #1: $837.00" + nl +
                "Row #1: 24" + nl +
                "Row #1: 24" + nl +
                "Row #1: 24" + nl +
                "Row #1: 24" + nl +
                "Row #1: 8" + nl +
                "Row #1: 8" + nl +
                "Row #1: 8" + nl +
                "Row #1: 8" + nl +
                "Row #2: $1,728.45" + nl +
                "Row #2: $1,727.02" + nl +
                "Row #2: $1,727.72" + nl +
                "Row #2: $1,726.55" + nl +
                "Row #2: 357" + nl +
                "Row #2: 357" + nl +
                "Row #2: 357" + nl +
                "Row #2: 357" + nl +
                "Row #2: 119" + nl +
                "Row #2: 119" + nl +
                "Row #2: 119" + nl +
                "Row #2: 119" + nl +
                "Row #3: $473.04" + nl +
                "Row #3: $473.04" + nl +
                "Row #3: $473.04" + nl +
                "Row #3: $473.04" + nl +
                "Row #3: 51" + nl +
                "Row #3: 51" + nl +
                "Row #3: 51" + nl +
                "Row #3: 51" + nl +
                "Row #3: 17" + nl +
                "Row #3: 17" + nl +
                "Row #3: 17" + nl +
                "Row #3: 17" + nl +
                "Row #4: $401.35" + nl +
                "Row #4: $405.73" + nl +
                "Row #4: $400.61" + nl +
                "Row #4: $402.31" + nl +
                "Row #4: 120" + nl +
                "Row #4: 120" + nl +
                "Row #4: 120" + nl +
                "Row #4: 120" + nl +
                "Row #4: 40" + nl +
                "Row #4: 40" + nl +
                "Row #4: 40" + nl +
                "Row #4: 40" + nl +
                "Row #5: (null)" + nl +
                "Row #5: (null)" + nl +
                "Row #5: (null)" + nl +
                "Row #5: (null)" + nl +
                "Row #5: (null)" + nl +
                "Row #5: (null)" + nl +
                "Row #5: (null)" + nl +
                "Row #5: (null)" + nl +
                "Row #5: (null)" + nl +
                "Row #5: (null)" + nl +
                "Row #5: (null)" + nl +
                "Row #5: (null)" + nl +
                "Row #6: (null)" + nl +
                "Row #6: (null)" + nl +
                "Row #6: (null)" + nl +
                "Row #6: (null)" + nl +
                "Row #6: (null)" + nl +
                "Row #6: (null)" + nl +
                "Row #6: (null)" + nl +
                "Row #6: (null)" + nl +
                "Row #6: (null)" + nl +
                "Row #6: (null)" + nl +
                "Row #6: (null)" + nl +
                "Row #6: (null)" + nl +
                "Row #7: $1,343.62" + nl +
                "Row #7: $1,342.61" + nl +
                "Row #7: $1,342.57" + nl +
                "Row #7: $1,343.65" + nl +
                "Row #7: 279" + nl +
                "Row #7: 279" + nl +
                "Row #7: 279" + nl +
                "Row #7: 279" + nl +
                "Row #7: 93" + nl +
                "Row #7: 93" + nl +
                "Row #7: 93" + nl +
                "Row #7: 93" + nl +
                "Row #8: $286.74" + nl +
                "Row #8: $286.74" + nl +
                "Row #8: $286.74" + nl +
                "Row #8: $286.74" + nl +
                "Row #8: 30" + nl +
                "Row #8: 30" + nl +
                "Row #8: 30" + nl +
                "Row #8: 30" + nl +
                "Row #8: 10" + nl +
                "Row #8: 10" + nl +
                "Row #8: 10" + nl +
                "Row #8: 10" + nl +
                "Row #9: $333.20" + nl +
                "Row #9: $332.65" + nl +
                "Row #9: $331.28" + nl +
                "Row #9: $332.43" + nl +
                "Row #9: 99" + nl +
                "Row #9: 99" + nl +
                "Row #9: 99" + nl +
                "Row #9: 99" + nl +
                "Row #9: 33" + nl +
                "Row #9: 33" + nl +
                "Row #9: 33" + nl +
                "Row #9: 33" + nl +
                "Row #10: (null)" + nl +
                "Row #10: (null)" + nl +
                "Row #10: (null)" + nl +
                "Row #10: (null)" + nl +
                "Row #10: (null)" + nl +
                "Row #10: (null)" + nl +
                "Row #10: (null)" + nl +
                "Row #10: (null)" + nl +
                "Row #10: (null)" + nl +
                "Row #10: (null)" + nl +
                "Row #10: (null)" + nl +
                "Row #10: (null)" + nl +
                "Row #11: (null)" + nl +
                "Row #11: (null)" + nl +
                "Row #11: (null)" + nl +
                "Row #11: (null)" + nl +
                "Row #11: (null)" + nl +
                "Row #11: (null)" + nl +
                "Row #11: (null)" + nl +
                "Row #11: (null)" + nl +
                "Row #11: (null)" + nl +
                "Row #11: (null)" + nl +
                "Row #11: (null)" + nl +
                "Row #11: (null)" + nl +
                "Row #12: $2,768.60" + nl +
                "Row #12: $2,769.18" + nl +
                "Row #12: $2,766.78" + nl +
                "Row #12: $2,769.50" + nl +
                "Row #12: 579" + nl +
                "Row #12: 579" + nl +
                "Row #12: 579" + nl +
                "Row #12: 579" + nl +
                "Row #12: 193" + nl +
                "Row #12: 193" + nl +
                "Row #12: 193" + nl +
                "Row #12: 193" + nl +
                "Row #13: $736.29" + nl +
                "Row #13: $736.29" + nl +
                "Row #13: $736.29" + nl +
                "Row #13: $736.29" + nl +
                "Row #13: 81" + nl +
                "Row #13: 81" + nl +
                "Row #13: 81" + nl +
                "Row #13: 81" + nl +
                "Row #13: 27" + nl +
                "Row #13: 27" + nl +
                "Row #13: 27" + nl +
                "Row #13: 27" + nl +
                "Row #14: $674.70" + nl +
                "Row #14: $674.54" + nl +
                "Row #14: $676.25" + nl +
                "Row #14: $676.48" + nl +
                "Row #14: 201" + nl +
                "Row #14: 201" + nl +
                "Row #14: 201" + nl +
                "Row #14: 201" + nl +
                "Row #14: 67" + nl +
                "Row #14: 67" + nl +
                "Row #14: 67" + nl +
                "Row #14: 67" + nl);
    }

    public void testDescendantsM() {
        assertAxisReturns("Descendants([Time].[1997].[Q1])",
                "[Time].[1997].[Q1]" + nl +
                "[Time].[1997].[Q1].[1]" + nl +
                "[Time].[1997].[Q1].[2]" + nl +
                "[Time].[1997].[Q1].[3]");
    }

    public void testDescendantsML() {
        assertAxisReturns("Descendants([Time].[1997], [Time].[Month])",
                months);
    }

    public void testDescendantsMLSelf() {
        assertAxisReturns("Descendants([Time].[1997], [Time].[Quarter], SELF)",
                quarters);
    }

    public void _testDescendantsMLLeaves1() {
        assertAxisReturns("Descendants([Time].[1997], [Time].[Quarter], leaves)",
                quarters);

        assertAxisReturns("[Sales Ragged]", "Descendants([Store].[Israel], [Store].[Store City], leaves)",
                "[Store].[All Stores].[Israel].[Israel].[Haifa]" + nl +
                "[Store].[All Stores].[Israel].[Israel].[Tel Aviv]");
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

    public void _testDescendantsM2Leaves() {
        assertAxisReturns("Descendants([Time].[1997], 2, Leaves)",
                months);
    }

    public void _testDescendantsMFarLeaves() {
        assertAxisReturns("Descendants([Time].[1997], 10000, Leaves)",
                months);
    }

    public void testLeavesNotSupported() throws Exception {
        assertThrows("select Descendants([Time].[1998], 1, Leaves) "
                + "on columns from [Sales]",
                "LEAVES option in Descendants function is not supported.");
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
        assertAxisReturns("HR", "Descendants([Employees], 2)",
                "[Employees].[All Employees].[Sheri Nowmer].[Derrick Whelply]" + nl +
                "[Employees].[All Employees].[Sheri Nowmer].[Michael Spence]" + nl +
                "[Employees].[All Employees].[Sheri Nowmer].[Maya Gutierrez]" + nl +
                "[Employees].[All Employees].[Sheri Nowmer].[Roberta Damstra]" + nl +
                "[Employees].[All Employees].[Sheri Nowmer].[Rebecca Kanagaki]" + nl +
                "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz]" + nl +
                "[Employees].[All Employees].[Sheri Nowmer].[Donna Arnold]");
    }

    public void testDescendantsParentChildBefore() {
        assertAxisReturns("HR", "Descendants([Employees], 2, BEFORE)",
                "[Employees].[All Employees]" + nl +
                "[Employees].[All Employees].[Sheri Nowmer]");
    }

    public void testDescendantsSBA() throws Exception {
        assertAxisReturns("Descendants([Time].[1997], 1, SELF_BEFORE_AFTER)",
                hierarchized1997);
    }

    public void testRange() {
        assertAxisReturns("[Time].[1997].[Q1].[2] : [Time].[1997].[Q2].[5]",
                "[Time].[1997].[Q1].[2]" + nl +
                "[Time].[1997].[Q1].[3]" + nl +
                "[Time].[1997].[Q2].[4]" + nl +
                "[Time].[1997].[Q2].[5]"); // not parents
    }

    /**
     * Large dimensions use a different member reader, therefore need to
     * be tested separately.
     */
    public void testRangeLarge() {
        assertAxisReturns("[Customers].[USA].[CA].[San Francisco] : [Customers].[USA].[WA].[Bellingham]",
                "[Customers].[All Customers].[USA].[CA].[San Francisco]" + nl +
                "[Customers].[All Customers].[USA].[CA].[San Gabriel]" + nl +
                "[Customers].[All Customers].[USA].[CA].[San Jose]" + nl +
                "[Customers].[All Customers].[USA].[CA].[Santa Cruz]" + nl +
                "[Customers].[All Customers].[USA].[CA].[Santa Monica]" + nl +
                "[Customers].[All Customers].[USA].[CA].[Spring Valley]" + nl +
                "[Customers].[All Customers].[USA].[CA].[Torrance]" + nl +
                "[Customers].[All Customers].[USA].[CA].[West Covina]" + nl +
                "[Customers].[All Customers].[USA].[CA].[Woodland Hills]" + nl +
                "[Customers].[All Customers].[USA].[OR].[Albany]" + nl +
                "[Customers].[All Customers].[USA].[OR].[Beaverton]" + nl +
                "[Customers].[All Customers].[USA].[OR].[Corvallis]" + nl +
                "[Customers].[All Customers].[USA].[OR].[Lake Oswego]" + nl +
                "[Customers].[All Customers].[USA].[OR].[Lebanon]" + nl +
                "[Customers].[All Customers].[USA].[OR].[Milwaukie]" + nl +
                "[Customers].[All Customers].[USA].[OR].[Oregon City]" + nl +
                "[Customers].[All Customers].[USA].[OR].[Portland]" + nl +
                "[Customers].[All Customers].[USA].[OR].[Salem]" + nl +
                "[Customers].[All Customers].[USA].[OR].[W. Linn]" + nl +
                "[Customers].[All Customers].[USA].[OR].[Woodburn]" + nl +
                "[Customers].[All Customers].[USA].[WA].[Anacortes]" + nl +
                "[Customers].[All Customers].[USA].[WA].[Ballard]" + nl +
                "[Customers].[All Customers].[USA].[WA].[Bellingham]");
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
                "[Time].[1997].[Q2].[5]" + nl +
                "[Time].[1997].[Q2].[6]" + nl +
                "[Time].[1997].[Q3].[7]"); // same as if reversed
    }

    public void testRangeEndBeforeStartLarge() {
        assertAxisReturns("[Customers].[USA].[WA] : [Customers].[USA].[CA]",
                "[Customers].[All Customers].[USA].[CA]" + nl +
                "[Customers].[All Customers].[USA].[OR]" + nl +
                "[Customers].[All Customers].[USA].[WA]");
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
        runQueryCheckResult("WITH MEMBER [Product].[All Products].[Non-Consumable].[Other] AS" + nl +
                " 'Sum( Except( [Product].[Product Department].Members," + nl +
                "       TopCount( [Product].[Product Department].Members, 3 ))," + nl +
                "       Measures.[Unit Sales] )'" + nl +
                "SELECT" + nl +
                "  { [Measures].[Unit Sales] } ON COLUMNS ," + nl +
                "  { TopCount( [Product].[Product Department].Members,3 )," + nl +
                "              [Product].[All Products].[Non-Consumable].[Other] } ON ROWS" + nl +
                "FROM [Sales]",
                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Unit Sales]}" + nl +
                "Axis #2:" + nl +
                "{[Product].[All Products].[Drink].[Alcoholic Beverages]}" + nl +
                "{[Product].[All Products].[Drink].[Beverages]}" + nl +
                "{[Product].[All Products].[Drink].[Dairy]}" + nl +
                "{[Product].[All Products].[Non-Consumable].[Other]}" + nl +
                "Row #0: 6,838" + nl +
                "Row #1: 13,573" + nl +
                "Row #2: 4,186" + nl +
                "Row #3: 242,176" + nl);
    }

    public void testBug714707() {
        // Same issue as bug 715177 -- "children" returns immutable
        // list, which set operator must make mutable.
        assertAxisReturns("{[Store].[USA].[CA].children, [Store].[USA]}",
                "[Store].[All Stores].[USA].[CA].[Alameda]" + nl +
                "[Store].[All Stores].[USA].[CA].[Beverly Hills]" + nl +
                "[Store].[All Stores].[USA].[CA].[Los Angeles]" + nl +
                "[Store].[All Stores].[USA].[CA].[San Diego]" + nl +
                "[Store].[All Stores].[USA].[CA].[San Francisco]" + nl +
                "[Store].[All Stores].[USA]");
    }

    // todo: Fix this test
    public void todo_testBug715177c() {
        assertAxisReturns("Order(TopCount({[Store].[USA].[CA].children}, [Measures].[Unit Sales], 2), [Measures].[Unit Sales])",
                "foo");
    }

    public void testFormatFixed() {
        String s = executeExpr("Format(12.2, \"#,##0.00\")");
        Assert.assertEquals("12.20", s);
    }

    public void testFormatVariable() {
        String s = executeExpr("Format(1234.5, \"#,#\" || \"#0.00\")");
        Assert.assertEquals("1,234.50", s);
    }


    public void testIIf() {
        String s = executeExpr("IIf(([Measures].[Unit Sales],[Product].[Drink].[Alcoholic Beverages].[Beer and Wine]) > 100, \"Yes\",\"No\")");
        Assert.assertEquals("Yes", s);
    }

    
    public void testDimensionCaption() {
        String s = executeExpr("[Time].[1997].Dimension.Caption");
        Assert.assertEquals("Time", s);
    }

    public void testHierarchyCaption() {
        String s = executeExpr("[Time].[1997].Hierarchy.Caption");
        Assert.assertEquals("Time", s);
    }

    public void testLevelCaption() {
        String s = executeExpr("[Time].[1997].Level.Caption");
        Assert.assertEquals("Year", s);
    }

    public void testMemberCaption() {
        String s = executeExpr("[Time].[1997].Caption");
        Assert.assertEquals("1997", s);
    }

    
    public void testDimensionName() {
        String s = executeExpr("[Time].[1997].Dimension.Name");
        Assert.assertEquals("Time", s);
    }

    public void testHierarchyName() {
        String s = executeExpr("[Time].[1997].Hierarchy.Name");
        Assert.assertEquals("Time", s);
    }
    
    public void testLevelName() {
        String s = executeExpr("[Time].[1997].Level.Name");
        Assert.assertEquals("Year", s);
    }
    
    public void testMemberName() {
        String s = executeExpr("[Time].[1997].Name");
        Assert.assertEquals("1997", s);
    }

    
    public void testDimensionUniqueName() {
        String s = executeExpr("[Gender].DefaultMember.Dimension.UniqueName");
        Assert.assertEquals("[Gender]", s);
    }


    public void testHierarchyUniqueName() {
        String s = executeExpr("[Gender].DefaultMember.Hierarchy.UniqueName");
        Assert.assertEquals("[Gender]", s);
    }


    public void testLevelUniqueName() {
        String s = executeExpr("[Gender].DefaultMember.Level.UniqueName");
        Assert.assertEquals("[Gender].[(All)]", s);
    }


    public void testMemberUniqueName() {
        String s = executeExpr("[Gender].DefaultMember.UniqueName");
        Assert.assertEquals("[Gender].[All Gender]", s);
    }

    public void testMemberUniqueNameOfNull() {
        String s = executeExpr("[Measures].[Unit Sales].FirstChild.UniqueName");
        Assert.assertEquals("[Measures].[#Null]", s); // MSOLAP gives "" here
    }

    public void testCoalesceEmpty() throws Exception {
        // [DF] is all null and [WA] has numbers for 1997 but not for 1998.
        Result result = execute("with\n"
                + "    member Measures.[Coal1] as 'coalesceempty(([Time].[1997], Measures.[Store Sales]), ([Time].[1998], Measures.[Store Sales]))'\n"
                + "    member Measures.[Coal2] as 'coalesceempty(([Time].[1997], Measures.[Unit Sales]), ([Time].[1998], Measures.[Unit Sales]))'\n"
                + "select \n"
                + "    {Measures.[Coal1], Measures.[Coal2]} on columns,\n"
                + "    {[Store].[All Stores].[Mexico].[DF], [Store].[All Stores].[USA].[WA]} on rows\n"
                + "from \n"
                + "    [Sales]");

        Cell cell;

        checkDataResults(
                new Double[][] {
                    {null, null},
                    {new Double(263793.22), new Double(124366)}
                },
                result,
                0.001);

        result = execute("with\n"
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

        checkDataResults(new Double[][] {
                    { null, null },
                    { new Double(8.963), new Double(8.963) }
                },
                result,
                0.001);

        result = execute("with\n"
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

        checkDataResults(new Double[][] {
                    { null, null },
                    { new Double(8.963), new Double(8.963) }
                },
                result,
                0.001);
    }

    public void testBrokenContextBug() throws Exception {
        Result result = execute("with\n"
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

        checkDataResults(new Double[][] {{new Double(8.963)}}, result, 0.001);

    }

    public void testSetItem() throws Exception {
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

        assertAxisThrows("{[Customers].[All Customers].[USA],"
                + "[Customers].[All Customers].[USA].[WA],"
                + "[Customers].[All Customers].[USA].[CA],"
                + "[Customers].[All Customers].[USA].[OR].[Lebanon].[Mary Frances Christian]}.Item(-1)",
                "Expected a number between 0 and 3, but was -1.");

        assertAxisThrows("{[Customers].[All Customers].[USA],"
                + "[Customers].[All Customers].[USA].[WA],"
                + "[Customers].[All Customers].[USA].[CA],"
                + "[Customers].[All Customers].[USA].[OR].[Lebanon].[Mary Frances Christian]}.Item(4)",
                "Expected a number between 0 and 3, but was 4.");
    }

    public void testTupleItem() throws Exception {
        assertAxisReturns(""
                + "([Time].[1997].[Q1].[1], [Customers].[All Customers].[USA].[OR], [Gender].[All Gender].[M]).item(2)",
                "[Gender].[All Gender].[M]");

        assertAxisReturns(""
                + "([Time].[1997].[Q1].[1], [Customers].[All Customers].[USA].[OR], [Gender].[All Gender].[M]).item(1)",
                "[Customers].[All Customers].[USA].[OR]");

        assertAxisReturns(""
                + "{[Time].[1997].[Q1].[1]}.item(0)",
                "[Time].[1997].[Q1].[1]");

        assertAxisReturns(""
                + "{[Time].[1997].[Q1].[1]}.Item(0).Item(0)",
                "[Time].[1997].[Q1].[1]");

        assertAxisThrows(""
                + "([Time].[1997].[Q1].[1], [Customers].[All Customers].[USA].[OR], [Gender].[All Gender].[M]).item(-1)",
                "Expected a number between 0 and 2, but was -1.");

        assertAxisThrows(""
                + "([Time].[1997].[Q1].[1], [Customers].[All Customers].[USA].[OR], [Gender].[All Gender].[M]).item(500)",
                "Expected a number between 0 and 2, but was 500.");

    }

    private void checkDataResults(Double[][] expected, Result result, final double tolerance) {
        Cell cell;
        int[] coords = new int[2];

        for (int rowIdx = 0; rowIdx < expected.length; rowIdx++) {
            coords[1] = rowIdx;
            for (int colIdx = 0; colIdx < expected[0].length; colIdx++) {
                coords[0] = colIdx;

                cell = result.getCell(coords);

                if (expected[rowIdx][colIdx] == null) {
                    assertTrue("Expected null value", cell.isNull());
                }
                else if (cell.isNull()) {
                    fail("Cell at (" + rowIdx + ", " + colIdx + ") was null, but was expecting " + expected[rowIdx][colIdx]);
                }
                else {
                    assertEquals("Incorrect value returned at (" + rowIdx + ", " + colIdx + ")",
                            expected[rowIdx][colIdx].doubleValue(),
                            ((Number)cell.getValue()).doubleValue(), tolerance);
                }
            }
        }
    }

    public void testLevelMemberExpressions() throws Exception {
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
        String s = executeExpr("CASE WHEN 1=0 THEN \"first\" WHEN 1=1 THEN \"second\" WHEN 1=2 THEN \"third\" ELSE \"fourth\" END");
        Assert.assertEquals("second", s);
    }

    public void testCaseTestMatchElse() {
        String s = executeExpr("CASE WHEN 1=0 THEN \"first\" ELSE \"fourth\" END");
        Assert.assertEquals("fourth", s);
    }

    public void testCaseTestMatchNoElse() {
        String s = executeExpr("CASE WHEN 1=0 THEN \"first\" END");
        Assert.assertEquals("(null)", s);
    }

    public void testCaseMatch() {
        String s = executeExpr("CASE 2 WHEN 1 THEN \"first\" WHEN 2 THEN \"second\" WHEN 3 THEN \"third\" ELSE \"fourth\" END");
        Assert.assertEquals("second", s);
    }

    public void testCaseMatchElse() {
        String s = executeExpr("CASE 7 WHEN 1 THEN \"first\" ELSE \"fourth\" END");
        Assert.assertEquals("fourth", s);
    }

    public void testCaseMatchNoElse() {
        String s = executeExpr("CASE 8 WHEN 0 THEN \"first\" END");
        Assert.assertEquals("(null)", s);
    }


    public void testPropertiesExpr() {
        String s = executeExpr("[Store].[USA].[CA].[Beverly Hills].[Store 6].Properties(\"Store Type\")");
        Assert.assertEquals("Gourmet Supermarket", s);
    }

    /**
     * Tests that non-existent property throws an error. *
     */
    public void testPropertiesNonExistent() {
        assertExprThrows("[Store].[USA].[CA].[Beverly Hills].[Store 6].Properties(\"Foo\")",
                "Property 'Foo' is not valid for");
    }

    public void testPropertiesFilter() {
        Result result = execute("SELECT { [Store Sales] } ON COLUMNS," + nl +
                " TOPCOUNT( Filter( [Store].[Store Name].Members," + nl +
                "                   [Store].CurrentMember.Properties(\"Store Type\") = \"Supermarket\" )," + nl +
                "           10, [Store Sales]) ON ROWS" + nl +
                "FROM [Sales]");
        Assert.assertEquals(8, result.getAxes()[1].positions.length);
    }

    public void testPropertyInCalculatedMember() {
        Result result = execute("WITH MEMBER [Measures].[Store Sales per Sqft]" + nl +
                "AS '[Measures].[Store Sales] / " +
                "  [Store].CurrentMember.Properties(\"Store Sqft\")'" + nl +
                "SELECT " + nl +
                "  {[Measures].[Unit Sales], [Measures].[Store Sales per Sqft]} ON COLUMNS," + nl +
                "  {[Store].[Store Name].members} ON ROWS" + nl +
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
        Assert.assertEquals("(null)", cell.getFormattedValue());
        cell = result.getCell(new int[]{1, 3});
        Assert.assertEquals("(null)", cell.getFormattedValue());
    }

    public void testOpeningPeriod() throws Exception {
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

        assertAxisReturns("[Sales Ragged]",
                "OpeningPeriod([Store].[Store City], [Store].[All Stores].[Israel])",
                "[Store].[All Stores].[Israel].[Israel].[Haifa]");

        assertAxisReturns("[Sales Ragged]",
                "OpeningPeriod([Store].[Store State], [Store].[All Stores].[Israel])",
                "");

        // Default member is [Time].[1997].
        assertAxisReturns("OpeningPeriod([Time].[Month])",
                "[Time].[1997].[Q1].[1]");

        assertAxisReturns("OpeningPeriod()", "[Time].[1997].[Q1]");

        assertAxisThrows("[Sales Ragged]",
                "OpeningPeriod([Time].[Year], [Store].[All Stores].[Israel])",
                "The <level> and <member> arguments to OpeningPeriod must be "
                + "from the same hierarchy. The level was from '[Time]' but "
                + "the member was from '[Store]'.");

        assertAxisThrows("OpeningPeriod([Store].[Store City])",
                "The <level> and <member> arguments to OpeningPeriod must be "
                + "from the same hierarchy. The level was from '[Store]' but "
                + "the member was from '[Time]'.");
    }

    public void testParallelPeriod() throws Exception {
        assertAxisReturns("parallelperiod([Time].[Quarter], 1, [Time].[1998].[Q1])",
                "[Time].[1997].[Q4]");

        assertAxisReturns("parallelperiod([Time].[Quarter], -1, [Time].[1997].[Q1])",
                "[Time].[1997].[Q2]");

        assertAxisReturns("parallelperiod([Time].[Year], 1, [Time].[1998].[Q1])",
                "[Time].[1997].[Q1]");

        assertAxisReturns("parallelperiod([Time].[Year], 1, [Time].[1998].[Q1].[1])",
                "[Time].[1997].[Q1].[1]");
    }

    public void _testParallelPeriodThrowsException() throws Exception {
        assertThrows("select {parallelperiod([Time].[Year], 1)} on columns "
                + "from [Sales] where ([Time].[1998].[Q1].[2])",
                "This should say something about Time appearing on two different axes (slicer an columns)");
    }

    public void testParallelPeriodLevelLag() throws Exception {
        runQueryCheckResult(
                  "with member [Measures].[Prev Unit Sales] as "
                + "        '([Measures].[Unit Sales], parallelperiod([Time].[Quarter], 2))' "
                + "select "
                + "    crossjoin({[Measures].[Unit Sales], [Measures].[Prev Unit Sales]}, {[Marital Status].[All Marital Status].children}) on columns, "
                + "    {[Time].[1997].[Q3]} on rows "
                + "from  "
                + "    [Sales] ",
                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Unit Sales], [Marital Status].[All Marital Status].[M]}" + nl +
                "{[Measures].[Unit Sales], [Marital Status].[All Marital Status].[S]}" + nl +
                "{[Measures].[Prev Unit Sales], [Marital Status].[All Marital Status].[M]}" + nl +
                "{[Measures].[Prev Unit Sales], [Marital Status].[All Marital Status].[S]}" + nl +
                "Axis #2:" + nl +
                "{[Time].[1997].[Q3]}" + nl +
                "Row #0: 32,815" + nl +
                "Row #0: 33,033" + nl +
                "Row #0: 33,101" + nl +
                "Row #0: 33,190" + nl);

    }

    public void testParallelPeriodLevel() throws Exception {
        runQueryCheckResult("with "
                + "    member [Measures].[Prev Unit Sales] as "
                + "        '([Measures].[Unit Sales], parallelperiod([Time].[Quarter]))' "
                + "select "
                + "    crossjoin({[Measures].[Unit Sales], [Measures].[Prev Unit Sales]}, {[Marital Status].[All Marital Status].[M]}) on columns, "
                + "    {[Time].[1997].[Q3].[8]} on rows "
                + "from  "
                + "    [Sales]",
                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Unit Sales], [Marital Status].[All Marital Status].[M]}" + nl +
                "{[Measures].[Prev Unit Sales], [Marital Status].[All Marital Status].[M]}" + nl +
                "Axis #2:" + nl +
                "{[Time].[1997].[Q3].[8]}" + nl +
                "Row #0: 10,957" + nl +
                "Row #0: 10,280" + nl);
    }

    public void testPlus() {
        String s = executeExpr("1+2");
        Assert.assertEquals("3", s);
    }

    public void testMinus() {
        String s = executeExpr("1-3");
        Assert.assertEquals("-2", s);
    }

    public void testMinusAssociativity() {
        String s = executeExpr("11-7-5");
        // right-associative would give 11-(7-5) = 9, which is wrong
        Assert.assertEquals("-1", s);
    }

    public void testMultiply() {
        String s = executeExpr("4*7");
        Assert.assertEquals("28", s);
    }

    public void testMultiplyPrecedence() {
        String s = executeExpr("3 + 4 * 5 + 6");
        Assert.assertEquals("29", s);
    }

    /**
     * Bug 774807 caused expressions to be mistaken for the crossjoin
     * operator.
     */
    public void testMultiplyBug774807() {
        final String desiredResult = "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Store].[All Stores]}" + nl +
                "Axis #2:" + nl +
                "{[Measures].[Store Sales]}" + nl +
                "{[Measures].[A]}" + nl +
                "Row #0: 565,238.13" + nl +
                "Row #1: 319,494,143,605.90" + nl;
        runQueryCheckResult("WITH MEMBER [Measures].[A] AS" + nl +
                " '([Measures].[Store Sales] * [Measures].[Store Sales])'" + nl +
                "SELECT {[Store]} ON COLUMNS," + nl +
                " {[Measures].[Store Sales], [Measures].[A]} ON ROWS" + nl +
                "FROM Sales",
                desiredResult);
        // as above, no parentheses
        runQueryCheckResult("WITH MEMBER [Measures].[A] AS" + nl +
                " '[Measures].[Store Sales] * [Measures].[Store Sales]'" + nl +
                "SELECT {[Store]} ON COLUMNS," + nl +
                " {[Measures].[Store Sales], [Measures].[A]} ON ROWS" + nl +
                "FROM Sales",
                desiredResult);
        // as above, plus 0
        runQueryCheckResult("WITH MEMBER [Measures].[A] AS" + nl +
                " '[Measures].[Store Sales] * [Measures].[Store Sales] + 0'" + nl +
                "SELECT {[Store]} ON COLUMNS," + nl +
                " {[Measures].[Store Sales], [Measures].[A]} ON ROWS" + nl +
                "FROM Sales",
                desiredResult);
    }

    public void testDivide() {
        String s = executeExpr("10 / 5");
        Assert.assertEquals("2", s);
    }

    public void testDivideByZero() {
        String s = executeExpr("-3 / (2 - 2)");
        Assert.assertEquals("-Infinity", s);
    }

    public void testDividePrecedence() {
        String s = executeExpr("24 / 4 / 2 * 10 - -1");
        Assert.assertEquals("31", s);
    }

    public void testUnaryMinus() {
        String s = executeExpr("-3");
        Assert.assertEquals("-3", s);
    }

    public void testUnaryMinusMember() {
        String s = executeExpr("- ([Measures].[Unit Sales],[Gender].[F])");
        Assert.assertEquals("-131,558", s);
    }

    public void testUnaryMinusPrecedence() {
        String s = executeExpr("1 - -10.5 * 2 -3");
        Assert.assertEquals("19", s);
    }

    public void testStringConcat() {
        String s = executeExpr(" \"foo\" || \"bar\"  ");
        Assert.assertEquals("foobar", s);
    }

    public void testStringConcat2() {
        String s = executeExpr(" \"foo\" || [Gender].[M].Name || \"\" ");
        Assert.assertEquals("fooM", s);
    }

    public void testAnd() {
        String s = executeBooleanExpr(" 1=1 AND 2=2 ");
        Assert.assertEquals("true", s);
    }

    public void testAnd2() {
        String s = executeBooleanExpr(" 1=1 AND 2=0 ");
        Assert.assertEquals("false", s);
    }

    public void testOr() {
        String s = executeBooleanExpr(" 1=0 OR 2=0 ");
        Assert.assertEquals("false", s);
    }

    public void testOr2() {
        String s = executeBooleanExpr(" 1=0 OR 0=0 ");
        Assert.assertEquals("true", s);
    }

    public void testOrAssociativity1() {
        String s = executeBooleanExpr(" 1=1 AND 1=0 OR 1=1 ");
        // Would give 'false' if OR were stronger than AND (wrong!)
        Assert.assertEquals("true", s);
    }

    public void testOrAssociativity2() {
        String s = executeBooleanExpr(" 1=1 OR 1=0 AND 1=1 ");
        // Would give 'false' if OR were stronger than AND (wrong!)
        Assert.assertEquals("true", s);
    }

    public void testOrAssociativity3() {
        String s = executeBooleanExpr(" (1=0 OR 1=1) AND 1=1 ");
        Assert.assertEquals("true", s);
    }

    public void testXor() {
        String s = executeBooleanExpr(" 1=1 XOR 2=2 ");
        Assert.assertEquals("false", s);
    }

    public void testXorAssociativity() {
        // Would give 'false' if XOR were stronger than AND (wrong!)
        String s = executeBooleanExpr(" 1 = 1 AND 1 = 1 XOR 1 = 0 ");
        Assert.assertEquals("true", s);
    }

    public void testNot() {
        String s = executeBooleanExpr(" NOT 1=1 ");
        Assert.assertEquals("false", s);
    }

    public void testNotNot() {
        String s = executeBooleanExpr(" NOT NOT 1=1 ");
        Assert.assertEquals("true", s);
    }

    public void testNotAssociativity() {
        String s = executeBooleanExpr(" 1=1 AND NOT 1=1 OR NOT 1=1 AND 1=1 ");
        Assert.assertEquals("false", s);
    }

    public void testStringEquals() {
        String s = executeBooleanExpr(" \"foo\" = \"bar\" ");
        Assert.assertEquals("false", s);
    }

    public void testStringEqualsAssociativity() {
        String s = executeBooleanExpr(" \"foo\" = \"fo\" || \"o\" ");
        Assert.assertEquals("true", s);
    }

    public void testStringEqualsEmpty() {
        String s = executeBooleanExpr(" \"\" = \"\" ");
        Assert.assertEquals("true", s);
    }
    public void testEq() {
        String s = executeBooleanExpr(" 1.0 = 1 ");
        Assert.assertEquals("true", s);
    }
    public void testStringNe() {
        String s = executeBooleanExpr(" \"foo\" <> \"bar\" ");
        Assert.assertEquals("true", s);
    }

    public void testNe() {
        String s = executeBooleanExpr(" 2 <> 1.0 + 1.0 ");
        Assert.assertEquals("false", s);
    }

    public void testNeInfinity() {
        String s = executeBooleanExpr("(1 / 0) <> (1 / 0)");
        // Infinity does not equal itself
        Assert.assertEquals("false", s);
    }

    public void testLt() {
        String s = executeBooleanExpr(" 2 < 1.0 + 1.0 ");
        Assert.assertEquals("false", s);
    }

    public void testLe() {
        String s = executeBooleanExpr(" 2 <= 1.0 + 1.0 ");
        Assert.assertEquals("true", s);
    }

    public void testGt() {
        String s = executeBooleanExpr(" 2 > 1.0 + 1.0 ");
        Assert.assertEquals("false", s);
    }

    public void testGe() {
        String s = executeBooleanExpr(" 2 > 1.0 + 1.0 ");
        Assert.assertEquals("false", s);
    }


    public void testDistinctTwoMembers() {
        assertAxisReturns("HR", "Distinct({[Employees].[All Employees].[Sheri Nowmer].[Donna Arnold],"
                + "[Employees].[All Employees].[Sheri Nowmer].[Donna Arnold]})",
                "[Employees].[All Employees].[Sheri Nowmer].[Donna Arnold]");
    }

    public void testDistinctThreeMembers() {
        assertAxisReturns("HR", "Distinct({[Employees].[All Employees].[Sheri Nowmer].[Donna Arnold],"
                + "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz],"
                + "[Employees].[All Employees].[Sheri Nowmer].[Donna Arnold]})",
                "[Employees].[All Employees].[Sheri Nowmer].[Donna Arnold]" + nl
                + "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz]");
    }

    public void testDistinctFourMembers() {
        assertAxisReturns("HR", "Distinct({[Employees].[All Employees].[Sheri Nowmer].[Donna Arnold],"
                + "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz],"
                + "[Employees].[All Employees].[Sheri Nowmer].[Donna Arnold],"
                + "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz]})",
                "[Employees].[All Employees].[Sheri Nowmer].[Donna Arnold]" + nl
                + "[Employees].[All Employees].[Sheri Nowmer].[Darren Stanz]");
    }

    public void testDistinctTwoTuples() {
        assertAxisReturns("Sales", "Distinct({([Time].[1997],[Store].[All Stores].[Mexico]), "
                + "([Time].[1997], [Store].[All Stores].[Mexico])})",
                "{[Time].[1997], [Store].[All Stores].[Mexico]}");
    }

    public void testDistinctSomeTuples() {
        assertAxisReturns("Sales", "Distinct({([Time].[1997],[Store].[All Stores].[Mexico]), "
                + "crossjoin({[Time].[1997]},{[Store].[All Stores].children})})",
                "{[Time].[1997], [Store].[All Stores].[Mexico]}" + nl
                + "{[Time].[1997], [Store].[All Stores].[Canada]}" + nl
                + "{[Time].[1997], [Store].[All Stores].[USA]}");
    }

    /**
     * Make sure that slicer is in force when expression is applied
     * on axis, E.g. select filter([Customers].members, [Unit Sales] > 100)
     * from sales where ([Time].[1998])
     */
    public void testFilterWithSlicer() {
        Result result = execute("select {[Measures].[Unit Sales]} on columns," + nl +
                " filter([Customers].[USA].children," + nl +
                "        [Measures].[Unit Sales] > 20000) on rows" + nl +
                "from Sales" + nl +
                "where ([Time].[1997].[Q1])");
        Axis rows = result.getAxes()[1];
        // if slicer were ignored, there would be 3 rows
        Assert.assertEquals(1, rows.positions.length);
        Cell cell = result.getCell(new int[]{0, 0});
        Assert.assertEquals("30,114", cell.getFormattedValue());
    }

    public void testFilterCompound() {
        Result result = execute("select {[Measures].[Unit Sales]} on columns," + nl +
                "  Filter(" + nl +
                "    CrossJoin(" + nl +
                "      [Gender].Children," + nl +
                "      [Customers].[USA].Children)," + nl +
                "    [Measures].[Unit Sales] > 9500) on rows" + nl +
                "from Sales" + nl +
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

    public void testGenerate() {
        assertAxisReturns("Generate({[Store].[USA], [Store].[USA].[CA]}, {[Store].CurrentMember.Children})",
                "[Store].[All Stores].[USA].[CA]" + nl +
                "[Store].[All Stores].[USA].[OR]" + nl +
                "[Store].[All Stores].[USA].[WA]" + nl +
                "[Store].[All Stores].[USA].[CA].[Alameda]" + nl +
                "[Store].[All Stores].[USA].[CA].[Beverly Hills]" + nl +
                "[Store].[All Stores].[USA].[CA].[Los Angeles]" + nl +
                "[Store].[All Stores].[USA].[CA].[San Diego]" + nl +
                "[Store].[All Stores].[USA].[CA].[San Francisco]");
    }

    public void testGenerateAll() {
        assertAxisReturns("Generate({[Store].[USA].[CA], [Store].[USA].[OR].[Portland]}," +
                " Ascendants([Store].CurrentMember)," +
                " ALL)",
                "[Store].[All Stores].[USA].[CA]" + nl +
                "[Store].[All Stores].[USA]" + nl +
                "[Store].[All Stores]" + nl +
                "[Store].[All Stores].[USA].[OR].[Portland]" + nl +
                "[Store].[All Stores].[USA].[OR]" + nl +
                "[Store].[All Stores].[USA]" + nl +
                "[Store].[All Stores]");
    }

    public void testGenerateUnique() {
        assertAxisReturns("Generate({[Store].[USA].[CA], [Store].[USA].[OR].[Portland]}," +
                " Ascendants([Store].CurrentMember))",
                "[Store].[All Stores].[USA].[CA]" + nl +
                "[Store].[All Stores].[USA]" + nl +
                "[Store].[All Stores]" + nl +
                "[Store].[All Stores].[USA].[OR].[Portland]" + nl +
                "[Store].[All Stores].[USA].[OR]");
    }

    public void testGenerateCrossJoin() {
        // Note that the different regions have different Top 2.
        assertAxisReturns("Generate({[Store].[USA].[CA], [Store].[USA].[CA].[San Francisco]}," + nl +
                "  CrossJoin({[Store].CurrentMember}," + nl +
                "    TopCount([Product].[Brand Name].members, " + nl +
                "    2," + nl +
                "    [Measures].[Unit Sales])))",
                "{[Store].[All Stores].[USA].[CA], [Product].[All Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Hermanos]}" + nl +
                "{[Store].[All Stores].[USA].[CA], [Product].[All Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale]}" + nl +
                "{[Store].[All Stores].[USA].[CA].[San Francisco], [Product].[All Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Ebony]}" + nl +
                "{[Store].[All Stores].[USA].[CA].[San Francisco], [Product].[All Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[High Top]}");
    }

    public void testHead() {
        assertAxisReturns("Head([Store].Children, 2)",
                "[Store].[All Stores].[Canada]" + nl +
                "[Store].[All Stores].[Mexico]");
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
                "[Store].[All Stores].[Canada]" + nl +
                "[Store].[All Stores].[Mexico]" + nl +
                "[Store].[All Stores].[USA]");
    }

    public void testHeadEmpty() {
        assertAxisReturns("Head([Gender].[F].Children, 2)",
                "");
    }

    public void testHierarchize() {
        assertAxisReturns("Hierarchize(" + nl +
                "    {[Product].[All Products], " +
                "     [Product].[Food]," + nl +
                "     [Product].[Drink]," + nl +
                "     [Product].[Non-Consumable]," + nl +
                "     [Product].[Food].[Eggs]," + nl +
                "     [Product].[Drink].[Dairy]})",

                "[Product].[All Products]" + nl +
                "[Product].[All Products].[Drink]" + nl +
                "[Product].[All Products].[Drink].[Dairy]" + nl +
                "[Product].[All Products].[Food]" + nl +
                "[Product].[All Products].[Food].[Eggs]" + nl +
                "[Product].[All Products].[Non-Consumable]");
    }

    public void testHierarchizePost() {
        assertAxisReturns("Hierarchize(" + nl +
                "    {[Product].[All Products], " +
                "     [Product].[Food]," + nl +
                "     [Product].[Food].[Eggs]," + nl +
                "     [Product].[Drink].[Dairy]}," + nl +
                "  POST)",

                "[Product].[All Products].[Drink].[Dairy]" + nl +
                "[Product].[All Products].[Food].[Eggs]" + nl +
                "[Product].[All Products].[Food]" + nl +
                "[Product].[All Products]");
    }

    public void testHierarchizeCrossJoinPre() {
        assertAxisReturns("Hierarchize(" + nl +
                "  CrossJoin(" + nl +
                "    {[Product].[All Products], " +
                "     [Product].[Food]," + nl +
                "     [Product].[Food].[Eggs]," + nl +
                "     [Product].[Drink].[Dairy]}," + nl +
                "    [Gender].MEMBERS)," + nl +
                "  PRE)",

                "{[Product].[All Products], [Gender].[All Gender]}" + nl +
                "{[Product].[All Products], [Gender].[All Gender].[F]}" + nl +
                "{[Product].[All Products], [Gender].[All Gender].[M]}" + nl +
                "{[Product].[All Products].[Drink].[Dairy], [Gender].[All Gender]}" + nl +
                "{[Product].[All Products].[Drink].[Dairy], [Gender].[All Gender].[F]}" + nl +
                "{[Product].[All Products].[Drink].[Dairy], [Gender].[All Gender].[M]}" + nl +
                "{[Product].[All Products].[Food], [Gender].[All Gender]}" + nl +
                "{[Product].[All Products].[Food], [Gender].[All Gender].[F]}" + nl +
                "{[Product].[All Products].[Food], [Gender].[All Gender].[M]}" + nl +
                "{[Product].[All Products].[Food].[Eggs], [Gender].[All Gender]}" + nl +
                "{[Product].[All Products].[Food].[Eggs], [Gender].[All Gender].[F]}" + nl +
                "{[Product].[All Products].[Food].[Eggs], [Gender].[All Gender].[M]}");
    }

    public void testHierarchizeCrossJoinPost() {
        assertAxisReturns("Hierarchize(" + nl +
                "  CrossJoin(" + nl +
                "    {[Product].[All Products], " +
                "     [Product].[Food]," + nl +
                "     [Product].[Food].[Eggs]," + nl +
                "     [Product].[Drink].[Dairy]}," + nl +
                "    [Gender].MEMBERS)," + nl +
                "  POST)",

                "{[Product].[All Products].[Drink].[Dairy], [Gender].[All Gender].[F]}" + nl +
                "{[Product].[All Products].[Drink].[Dairy], [Gender].[All Gender].[M]}" + nl +
                "{[Product].[All Products].[Drink].[Dairy], [Gender].[All Gender]}" + nl +
                "{[Product].[All Products].[Food].[Eggs], [Gender].[All Gender].[F]}" + nl +
                "{[Product].[All Products].[Food].[Eggs], [Gender].[All Gender].[M]}" + nl +
                "{[Product].[All Products].[Food].[Eggs], [Gender].[All Gender]}" + nl +
                "{[Product].[All Products].[Food], [Gender].[All Gender].[F]}" + nl +
                "{[Product].[All Products].[Food], [Gender].[All Gender].[M]}" + nl +
                "{[Product].[All Products].[Food], [Gender].[All Gender]}" + nl +
                "{[Product].[All Products], [Gender].[All Gender].[F]}" + nl +
                "{[Product].[All Products], [Gender].[All Gender].[M]}" + nl +
                "{[Product].[All Products], [Gender].[All Gender]}");
    }

    public void testIntersect() {
        // Note: duplicates retained from left, not from right; and order is preserved.
        assertAxisReturns("Intersect({[Time].[1997].[Q2], [Time].[1997], [Time].[1997].[Q1], [Time].[1997].[Q2]}, " +
                "{[Time].[1998], [Time].[1997], [Time].[1997].[Q2], [Time].[1997]}, " +
                "ALL)",
                "[Time].[1997].[Q2]" + nl +
                "[Time].[1997]" + nl +
                "[Time].[1997].[Q2]");
    }

    public void testIntersectRightEmpty() {
        assertAxisReturns("Intersect({[Time].[1997]}, {})",
                "");
    }

    public void testIntersectLeftEmpty() {
        assertAxisReturns("Intersect({}, {[Store].[USA].[CA]})",
                "");
    }

    public void testOrder() {
        runQueryCheckResult("select {[Measures].[Unit Sales]} on columns," + nl +
                " order({" + nl +
                "  [Product].[All Products].[Drink]," + nl +
                "  [Product].[All Products].[Drink].[Beverages]," + nl +
                "  [Product].[All Products].[Drink].[Dairy]," + nl +
                "  [Product].[All Products].[Food]," + nl +
                "  [Product].[All Products].[Food].[Baked Goods]," + nl +
                "  [Product].[All Products].[Food].[Eggs]," + nl +
                "  [Product].[All Products]}," + nl +
                " [Measures].[Unit Sales]) on rows" + nl +
                "from Sales",

                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Unit Sales]}" + nl +
                "Axis #2:" + nl +
                "{[Product].[All Products]}" + nl +
                "{[Product].[All Products].[Drink]}" + nl +
                "{[Product].[All Products].[Drink].[Dairy]}" + nl +
                "{[Product].[All Products].[Drink].[Beverages]}" + nl +
                "{[Product].[All Products].[Food]}" + nl +
                "{[Product].[All Products].[Food].[Eggs]}" + nl +
                "{[Product].[All Products].[Food].[Baked Goods]}" + nl +
                "Row #0: 266,773" + nl +
                "Row #1: 24,597" + nl +
                "Row #2: 4,186" + nl +
                "Row #3: 13,573" + nl +
                "Row #4: 191,940" + nl +
                "Row #5: 4,132" + nl +
                "Row #6: 7,870" + nl);
    }

    public void testOrderParentsMissing() {
        // Paradoxically, [Alcoholic Beverages] comes before
        // [Eggs] even though it has a larger value, because
        // its parent [Drink] has a smaller value than [Food].
        runQueryCheckResult("select {[Measures].[Unit Sales]} on columns," +
                " order({" + nl +
                "  [Product].[All Products].[Drink].[Alcoholic Beverages]," + nl +
                "  [Product].[All Products].[Food].[Eggs]}," + nl +
                " [Measures].[Unit Sales], ASC) on rows" + nl +
                "from Sales",

                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Unit Sales]}" + nl +
                "Axis #2:" + nl +
                "{[Product].[All Products].[Drink].[Alcoholic Beverages]}" + nl +
                "{[Product].[All Products].[Food].[Eggs]}" + nl +
                "Row #0: 6,838" + nl +
                "Row #1: 4,132" + nl);
    }

    public void testOrderCrossJoinBreak() {
        runQueryCheckResult("select {[Measures].[Unit Sales]} on columns," + nl +
                "  Order(" + nl +
                "    CrossJoin(" + nl +
                "      [Gender].children," + nl +
                "      [Marital Status].children)," + nl +
                "    [Measures].[Unit Sales]," + nl +
                "    BDESC) on rows" + nl +
                "from Sales" + nl +
                "where [Time].[1997].[Q1]",

                "Axis #0:" + nl +
                "{[Time].[1997].[Q1]}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Unit Sales]}" + nl +
                "Axis #2:" + nl +
                "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S]}" + nl +
                "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[M]}" + nl +
                "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M]}" + nl +
                "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S]}" + nl +
                "Row #0: 17,070" + nl +
                "Row #1: 16,790" + nl +
                "Row #2: 16,311" + nl +
                "Row #3: 16,120" + nl);
    }

    public void testOrderCrossJoin() {
        // Note:
        // 1. [Alcoholic Beverages] collates before [Eggs] and
        //    [Seafood] because its parent, [Drink], is less
        //    than [Food]
        // 2. [Seattle] generally sorts after [CA] and [OR]
        //    because invisible parent [WA] is greater.
        runQueryCheckResult("select CrossJoin(" + nl +
                "    {[Time].[1997]," + nl +
                "     [Time].[1997].[Q1]}," + nl +
                "    {[Measures].[Unit Sales]}) on columns," + nl +
                "  Order(" + nl +
                "    CrossJoin( " + nl +
                "      {[Product].[All Products].[Food].[Eggs]," + nl +
                "       [Product].[All Products].[Food].[Seafood]," + nl +
                "       [Product].[All Products].[Drink].[Alcoholic Beverages]}," + nl +
                "      {[Store].[USA].[WA].[Seattle]," + nl +
                "       [Store].[USA].[CA]," + nl +
                "       [Store].[USA].[OR]})," + nl +
                "    ([Time].[1997].[Q1], [Measures].[Unit Sales])," + nl +
                "    ASC) on rows" + nl +
                "from Sales",

                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Time].[1997], [Measures].[Unit Sales]}" + nl +
                "{[Time].[1997].[Q1], [Measures].[Unit Sales]}" + nl +
                "Axis #2:" + nl +
                "{[Product].[All Products].[Drink].[Alcoholic Beverages], [Store].[All Stores].[USA].[OR]}" + nl +
                "{[Product].[All Products].[Drink].[Alcoholic Beverages], [Store].[All Stores].[USA].[CA]}" + nl +
                "{[Product].[All Products].[Drink].[Alcoholic Beverages], [Store].[All Stores].[USA].[WA].[Seattle]}" + nl +
                "{[Product].[All Products].[Food].[Seafood], [Store].[All Stores].[USA].[CA]}" + nl +
                "{[Product].[All Products].[Food].[Seafood], [Store].[All Stores].[USA].[OR]}" + nl +
                "{[Product].[All Products].[Food].[Seafood], [Store].[All Stores].[USA].[WA].[Seattle]}" + nl +
                "{[Product].[All Products].[Food].[Eggs], [Store].[All Stores].[USA].[CA]}" + nl +
                "{[Product].[All Products].[Food].[Eggs], [Store].[All Stores].[USA].[OR]}" + nl +
                "{[Product].[All Products].[Food].[Eggs], [Store].[All Stores].[USA].[WA].[Seattle]}" + nl +
                "Row #0: 1,680" + nl +
                "Row #0: 393" + nl +
                "Row #1: 1,936" + nl +
                "Row #1: 431" + nl +
                "Row #2: 635" + nl +
                "Row #2: 142" + nl +
                "Row #3: 441" + nl +
                "Row #3: 91" + nl +
                "Row #4: 451" + nl +
                "Row #4: 107" + nl +
                "Row #5: 217" + nl +
                "Row #5: 44" + nl +
                "Row #6: 1,116" + nl +
                "Row #6: 240" + nl +
                "Row #7: 1,119" + nl +
                "Row #7: 251" + nl +
                "Row #8: 373" + nl +
                "Row #8: 57" + nl);
    }

    public void testOrderHierarchicalDesc() {
        assertAxisReturns("Order(" + nl +
                "    {[Product].[All Products], " +
                "     [Product].[Food]," + nl +
                "     [Product].[Drink]," + nl +
                "     [Product].[Non-Consumable]," + nl +
                "     [Product].[Food].[Eggs]," + nl +
                "     [Product].[Drink].[Dairy]}," + nl +
                "  [Measures].[Unit Sales]," + nl +
                "  DESC)",

                "[Product].[All Products]" + nl +
                "[Product].[All Products].[Food]" + nl +
                "[Product].[All Products].[Food].[Eggs]" + nl +
                "[Product].[All Products].[Non-Consumable]" + nl +
                "[Product].[All Products].[Drink]" + nl +
                "[Product].[All Products].[Drink].[Dairy]");
    }

    public void testOrderCrossJoinDesc() {
        assertAxisReturns("Order(" + nl +
                "  CrossJoin(" + nl +
                "    {[Gender].[M], [Gender].[F]}," + nl +
                "    {[Product].[All Products], " +
                "     [Product].[Food]," + nl +
                "     [Product].[Drink]," + nl +
                "     [Product].[Non-Consumable]," + nl +
                "     [Product].[Food].[Eggs]," + nl +
                "     [Product].[Drink].[Dairy]})," + nl +
                "  [Measures].[Unit Sales]," + nl +
                "  DESC)",

                "{[Gender].[All Gender].[M], [Product].[All Products]}" + nl +
                "{[Gender].[All Gender].[M], [Product].[All Products].[Food]}" + nl +
                "{[Gender].[All Gender].[M], [Product].[All Products].[Food].[Eggs]}" + nl +
                "{[Gender].[All Gender].[M], [Product].[All Products].[Non-Consumable]}" + nl +
                "{[Gender].[All Gender].[M], [Product].[All Products].[Drink]}" + nl +
                "{[Gender].[All Gender].[M], [Product].[All Products].[Drink].[Dairy]}" + nl +
                "{[Gender].[All Gender].[F], [Product].[All Products]}" + nl +
                "{[Gender].[All Gender].[F], [Product].[All Products].[Food]}" + nl +
                "{[Gender].[All Gender].[F], [Product].[All Products].[Food].[Eggs]}" + nl +
                "{[Gender].[All Gender].[F], [Product].[All Products].[Non-Consumable]}" + nl +
                "{[Gender].[All Gender].[F], [Product].[All Products].[Drink]}" + nl +
                "{[Gender].[All Gender].[F], [Product].[All Products].[Drink].[Dairy]}");
    }

    public void testOrderBug656802() {
        // Note:
        // 1. [Alcoholic Beverages] collates before [Eggs] and
        //    [Seafood] because its parent, [Drink], is less
        //    than [Food]
        // 2. [Seattle] generally sorts after [CA] and [OR]
        //    because invisible parent [WA] is greater.
        runQueryCheckResult("select {[Measures].[Unit Sales], [Measures].[Store Cost], [Measures].[Store Sales]} ON columns, " + nl +
                "Order(" + nl +
                "  ToggleDrillState(" + nl +
                "    {([Promotion Media].[All Media], [Product].[All Products])}," + nl +
                "    {[Product].[All Products]} ), " + nl +
                "  [Measures].[Unit Sales], DESC) ON rows " + nl +
                "from [Sales] where ([Time].[1997])",

                "Axis #0:" + nl +
                "{[Time].[1997]}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Unit Sales]}" + nl +
                "{[Measures].[Store Cost]}" + nl +
                "{[Measures].[Store Sales]}" + nl +
                "Axis #2:" + nl +
                "{[Promotion Media].[All Media], [Product].[All Products]}" + nl +
                "{[Promotion Media].[All Media], [Product].[All Products].[Food]}" + nl +
                "{[Promotion Media].[All Media], [Product].[All Products].[Non-Consumable]}" + nl +
                "{[Promotion Media].[All Media], [Product].[All Products].[Drink]}" + nl +
                "Row #0: 266,773" + nl +
                "Row #0: 225,627.23" + nl +
                "Row #0: 565,238.13" + nl +
                "Row #1: 191,940" + nl +
                "Row #1: 163,270.72" + nl +
                "Row #1: 409,035.59" + nl +
                "Row #2: 50,236" + nl +
                "Row #2: 42,879.28" + nl +
                "Row #2: 107,366.33" + nl +
                "Row #3: 24,597" + nl +
                "Row #3: 19,477.23" + nl +
                "Row #3: 48,836.21" + nl);
    }

    public void testOrderBug712702_Simplified() {
        runQueryCheckResult("SELECT Order({[Time].[Year].members}, [Measures].[Unit Sales]) on columns" + nl +
                "from [Sales]",
                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Time].[1998]}" + nl +
                "{[Time].[1997]}" + nl +
                "Row #0: (null)" + nl +
                "Row #0: 266,773" + nl);
    }

    public void testOrderBug712702_Original() {
        runQueryCheckResult("with member [Measures].[Average Unit Sales] as 'Avg(Descendants([Time].CurrentMember, [Time].[Month]), " + nl +
                "[Measures].[Unit Sales])' " + nl +
                "member [Measures].[Max Unit Sales] as 'Max(Descendants([Time].CurrentMember, [Time].[Month]), [Measures].[Unit Sales])' " + nl +
                "select {[Measures].[Average Unit Sales], [Measures].[Max Unit Sales], [Measures].[Unit Sales]} ON columns, " + nl +
                "  NON EMPTY Order(" + nl +
                "    Crossjoin( " + nl +
                "      {[Store].[All Stores].[USA].[OR].[Portland]," + nl +
                "       [Store].[All Stores].[USA].[OR].[Salem]," + nl +
                "       [Store].[All Stores].[USA].[OR].[Salem].[Store 13]," + nl +
                "       [Store].[All Stores].[USA].[CA].[San Francisco]," + nl +
                "       [Store].[All Stores].[USA].[CA].[San Diego]," + nl +
                "       [Store].[All Stores].[USA].[CA].[Beverly Hills]," + nl +
                "       [Store].[All Stores].[USA].[CA].[Los Angeles]," + nl +
                "       [Store].[All Stores].[USA].[WA].[Walla Walla]," + nl +
                "       [Store].[All Stores].[USA].[WA].[Bellingham]," + nl +
                "       [Store].[All Stores].[USA].[WA].[Yakima]," + nl +
                "       [Store].[All Stores].[USA].[WA].[Spokane]," + nl +
                "       [Store].[All Stores].[USA].[WA].[Seattle], " + nl +
                "       [Store].[All Stores].[USA].[WA].[Bremerton]," + nl +
                "       [Store].[All Stores].[USA].[WA].[Tacoma]}," + nl +
                "     [Time].[Year].Members), " + nl +
                "  [Measures].[Average Unit Sales], ASC) ON rows" + nl +
                "from [Sales] ",
                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Average Unit Sales]}" + nl +
                "{[Measures].[Max Unit Sales]}" + nl +
                "{[Measures].[Unit Sales]}" + nl +
                "Axis #2:" + nl +
                "{[Store].[All Stores].[USA].[OR].[Portland], [Time].[1997]}" + nl +
                "{[Store].[All Stores].[USA].[OR].[Salem], [Time].[1997]}" + nl +
                "{[Store].[All Stores].[USA].[OR].[Salem].[Store 13], [Time].[1997]}" + nl +
                "{[Store].[All Stores].[USA].[CA].[San Francisco], [Time].[1997]}" + nl +
                "{[Store].[All Stores].[USA].[CA].[Beverly Hills], [Time].[1997]}" + nl +
                "{[Store].[All Stores].[USA].[CA].[San Diego], [Time].[1997]}" + nl +
                "{[Store].[All Stores].[USA].[CA].[Los Angeles], [Time].[1997]}" + nl +
                "{[Store].[All Stores].[USA].[WA].[Walla Walla], [Time].[1997]}" + nl +
                "{[Store].[All Stores].[USA].[WA].[Bellingham], [Time].[1997]}" + nl +
                "{[Store].[All Stores].[USA].[WA].[Yakima], [Time].[1997]}" + nl +
                "{[Store].[All Stores].[USA].[WA].[Spokane], [Time].[1997]}" + nl +
                "{[Store].[All Stores].[USA].[WA].[Bremerton], [Time].[1997]}" + nl +
                "{[Store].[All Stores].[USA].[WA].[Seattle], [Time].[1997]}" + nl +
                "{[Store].[All Stores].[USA].[WA].[Tacoma], [Time].[1997]}" + nl +
                "Row #0: 2,173" + nl +
                "Row #0: 2,933" + nl +
                "Row #0: 26,079" + nl +
                "Row #1: 3,465" + nl +
                "Row #1: 5,891" + nl +
                "Row #1: 41,580" + nl +
                "Row #2: 3,465" + nl +
                "Row #2: 5,891" + nl +
                "Row #2: 41,580" + nl +
                "Row #3: 176" + nl +
                "Row #3: 222" + nl +
                "Row #3: 2,117" + nl +
                "Row #4: 1,778" + nl +
                "Row #4: 2,545" + nl +
                "Row #4: 21,333" + nl +
                "Row #5: 2,136" + nl +
                "Row #5: 2,686" + nl +
                "Row #5: 25,635" + nl +
                "Row #6: 2,139" + nl +
                "Row #6: 2,669" + nl +
                "Row #6: 25,663" + nl +
                "Row #7: 184" + nl +
                "Row #7: 301" + nl +
                "Row #7: 2,203" + nl +
                "Row #8: 186" + nl +
                "Row #8: 275" + nl +
                "Row #8: 2,237" + nl +
                "Row #9: 958" + nl +
                "Row #9: 1,163" + nl +
                "Row #9: 11,491" + nl +
                "Row #10: 1,966" + nl +
                "Row #10: 2,634" + nl +
                "Row #10: 23,591" + nl +
                "Row #11: 2,048" + nl +
                "Row #11: 2,623" + nl +
                "Row #11: 24,576" + nl +
                "Row #12: 2,084" + nl +
                "Row #12: 2,304" + nl +
                "Row #12: 25,011" + nl +
                "Row #13: 2,938" + nl +
                "Row #13: 3,818" + nl +
                "Row #13: 35,257" + nl);
    }

    public void testSiblingsA() {
        assertAxisReturns("{[Time].[1997].Siblings}",
                "[Time].[1997]" + nl +
                "[Time].[1998]");
    }

    public void testSiblingsB() {
        assertAxisReturns("{[Store].Siblings}",
                "[Store].[All Stores]");
    }

    public void testSiblingsC() {
        assertAxisReturns("{[Store].[USA].[CA].Siblings}",
                "[Store].[All Stores].[USA].[CA]" + nl +
                "[Store].[All Stores].[USA].[OR]" + nl +
                "[Store].[All Stores].[USA].[WA]");
    }

    public void testSubset() {
        assertAxisReturns("Subset([Promotion Media].Children, 7, 2)",
                "[Promotion Media].[All Media].[Product Attachment]" + nl +
                "[Promotion Media].[All Media].[Radio]");
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
                "[Promotion Media].[All Media].[Sunday Paper, Radio]" + nl +
                "[Promotion Media].[All Media].[Sunday Paper, Radio, TV]" + nl +
                "[Promotion Media].[All Media].[TV]");
    }

    public void testSubsetOvershoot() {
        assertAxisReturns("Subset([Promotion Media].Children, 15)",
                "");
    }

    public void testSubsetEmpty() {
        assertAxisReturns("Subset([Gender].[F].Children, 1)",
                "");
    }

    public void testTail() {
        assertAxisReturns("Tail([Store].Children, 2)",
                "[Store].[All Stores].[Mexico]" + nl +
                "[Store].[All Stores].[USA]");
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
                "[Store].[All Stores].[Canada]" + nl +
                "[Store].[All Stores].[Mexico]" + nl +
                "[Store].[All Stores].[USA]");
    }

    public void testTailEmpty() {
        assertAxisReturns("Tail([Gender].[F].Children, 2)",
                "");
    }

    public void testToggleDrillState() {
        assertAxisReturns("ToggleDrillState({[Customers].[USA],[Customers].[Canada]},{[Customers].[USA],[Customers].[USA].[CA]})",
                "[Customers].[All Customers].[USA]" + nl +
                "[Customers].[All Customers].[USA].[CA]" + nl +
                "[Customers].[All Customers].[USA].[OR]" + nl +
                "[Customers].[All Customers].[USA].[WA]" + nl +
                "[Customers].[All Customers].[Canada]");
    }

    public void testToggleDrillState2() {
        assertAxisReturns("ToggleDrillState([Product].[Product Department].members, {[Product].[All Products].[Food].[Snack Foods]})",
                "[Product].[All Products].[Drink].[Alcoholic Beverages]" + nl +
                "[Product].[All Products].[Drink].[Beverages]" + nl +
                "[Product].[All Products].[Drink].[Dairy]" + nl +
                "[Product].[All Products].[Food].[Baked Goods]" + nl +
                "[Product].[All Products].[Food].[Baking Goods]" + nl +
                "[Product].[All Products].[Food].[Breakfast Foods]" + nl +
                "[Product].[All Products].[Food].[Canned Foods]" + nl +
                "[Product].[All Products].[Food].[Canned Products]" + nl +
                "[Product].[All Products].[Food].[Dairy]" + nl +
                "[Product].[All Products].[Food].[Deli]" + nl +
                "[Product].[All Products].[Food].[Eggs]" + nl +
                "[Product].[All Products].[Food].[Frozen Foods]" + nl +
                "[Product].[All Products].[Food].[Meat]" + nl +
                "[Product].[All Products].[Food].[Produce]" + nl +
                "[Product].[All Products].[Food].[Seafood]" + nl +
                "[Product].[All Products].[Food].[Snack Foods]" + nl +
                "[Product].[All Products].[Food].[Snack Foods].[Snack Foods]" + nl +
                "[Product].[All Products].[Food].[Snacks]" + nl +
                "[Product].[All Products].[Food].[Starchy Foods]" + nl +
                "[Product].[All Products].[Non-Consumable].[Carousel]" + nl +
                "[Product].[All Products].[Non-Consumable].[Checkout]" + nl +
                "[Product].[All Products].[Non-Consumable].[Health and Hygiene]" + nl +
                "[Product].[All Products].[Non-Consumable].[Household]" + nl +
                "[Product].[All Products].[Non-Consumable].[Periodicals]");
    }

    public void testToggleDrillState3() {
        assertAxisReturns("ToggleDrillState(" +
                "{[Time].[1997].[Q1]," +
                " [Time].[1997].[Q2]," +
                " [Time].[1997].[Q2].[4]," +
                " [Time].[1997].[Q2].[6]," +
                " [Time].[1997].[Q3]}," +
                "{[Time].[1997].[Q2]})",
                "[Time].[1997].[Q1]" + nl +
                "[Time].[1997].[Q2]" + nl +
                "[Time].[1997].[Q3]");
    }

    // bug 634860
    public void testToggleDrillStateTuple() {
        assertAxisReturns("ToggleDrillState(" + nl +
                "{([Store].[All Stores].[USA].[CA]," +
                "  [Product].[All Products].[Drink].[Alcoholic Beverages])," + nl +
                " ([Store].[All Stores].[USA]," +
                "  [Product].[All Products].[Drink])}," + nl +
                "{[Store].[All stores].[USA].[CA]})",
                "{[Store].[All Stores].[USA].[CA], [Product].[All Products].[Drink].[Alcoholic Beverages]}" + nl +
                "{[Store].[All Stores].[USA].[CA].[Alameda], [Product].[All Products].[Drink].[Alcoholic Beverages]}" + nl +
                "{[Store].[All Stores].[USA].[CA].[Beverly Hills], [Product].[All Products].[Drink].[Alcoholic Beverages]}" + nl +
                "{[Store].[All Stores].[USA].[CA].[Los Angeles], [Product].[All Products].[Drink].[Alcoholic Beverages]}" + nl +
                "{[Store].[All Stores].[USA].[CA].[San Diego], [Product].[All Products].[Drink].[Alcoholic Beverages]}" + nl +
                "{[Store].[All Stores].[USA].[CA].[San Francisco], [Product].[All Products].[Drink].[Alcoholic Beverages]}" + nl +
                "{[Store].[All Stores].[USA], [Product].[All Products].[Drink]}");
    }

    public void testToggleDrillStateRecursive() throws Exception {
        // We expect this to fail.
        assertThrows("Select \n"
                + "    ToggleDrillState(\n"
                + "        {[Store].[All Stores].[USA]}, \n"
                + "        {[Store].[All Stores].[USA]}, recursive) on Axis(0) \n"
                + "from [Sales]",
                "'RECURSIVE' is not supported in ToggleDrillState.");
    }

    public void testTopCount() {
        assertAxisReturns("TopCount({[Promotion Media].[Media Type].members}, 2, [Measures].[Unit Sales])",
                "[Promotion Media].[All Media].[No Media]" + nl +
                "[Promotion Media].[All Media].[Daily Paper, Radio, TV]");
    }

    public void testTopCountTuple() {
        assertAxisReturns("TopCount([Customers].[Name].members,2,(Time.[1997].[Q1],[Measures].[Store Sales]))",
                "[Customers].[All Customers].[USA].[WA].[Spokane].[Grace McLaughlin]" + nl +
                "[Customers].[All Customers].[USA].[WA].[Spokane].[Matt Bellah]");
    }

    public void testTopPercent() {
        assertAxisReturns("TopPercent({[Promotion Media].[Media Type].members}, 70, [Measures].[Unit Sales])",
                "[Promotion Media].[All Media].[No Media]");
    }

    //todo: test precision

    public void testTopSum() {
        assertAxisReturns("TopSum({[Promotion Media].[Media Type].members}, 200000, [Measures].[Unit Sales])",
                "[Promotion Media].[All Media].[No Media]" + nl +
                "[Promotion Media].[All Media].[Daily Paper, Radio, TV]");
    }

    public void testUnionAll() {
        assertAxisReturns("Union({[Gender].[M]}, {[Gender].[F]}, ALL)",
                "[Gender].[All Gender].[M]" + nl +
                "[Gender].[All Gender].[F]"); // order is preserved
    }

    public void testUnion() {
        assertAxisReturns("Union({[Store].[USA], [Store].[USA], [Store].[USA].[OR]}, {[Store].[USA].[CA], [Store].[USA]})",
                "[Store].[All Stores].[USA]" + nl +
                "[Store].[All Stores].[USA].[OR]" + nl +
                "[Store].[All Stores].[USA].[CA]");
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

                "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S]}" + nl +
                "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S]}" + nl +
                "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M]}");
    }

    public void testUnionTupleDistinct() {
        assertAxisReturns("Union({" +
                " ([Gender].[M], [Marital Status].[S])," +
                " ([Gender].[F], [Marital Status].[S])" +
                "}, {" +
                " ([Gender].[M], [Marital Status].[M])," +
                " ([Gender].[M], [Marital Status].[S])" +
                "}, Distinct)",

                "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S]}"
                + nl
                +
                "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S]}"
                + nl
                +
                "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M]}");

    }

    public void testUnionQuery() {
        Result result = runQuery("select {[Measures].[Unit Sales], [Measures].[Store Cost], [Measures].[Store Sales]} on columns," + nl +
                " Hierarchize( " + nl +
                "   Union(" + nl +
                "     Crossjoin(" + nl +
                "       Crossjoin([Gender].[All Gender].children," + nl +
                "                 [Marital Status].[All Marital Status].children )," + nl +
                "       Crossjoin([Customers].[All Customers].children," + nl +
                "                 [Product].[All Products].children ) ) ," + nl +
                "     Crossjoin( {([Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M] )}," + nl +
                "       Crossjoin(" + nl +
                "         [Customers].[All Customers].[USA].children," + nl +
                "         [Product].[All Products].children ) ) )) on rows" + nl +
                "from Sales where ([Time].[1997])");
        final Axis rowsAxis = result.getAxes()[1];
        Assert.assertEquals(45, rowsAxis.positions.length);
    }
    
    public void testItemMember() {
        String s = executeExpr("Descendants([Time].[1997], [Time].[Month]).Item(1).Item(0).UniqueName");
        Assert.assertEquals("[Time].[1997].[Q1].[2]", s);
    }

    public void testItemTuple() {
        String s = executeExpr("CrossJoin([Gender].[All Gender].children, " +
        		"[Time].[1997].[Q2].children).Item(0).Item(1).UniqueName");
        Assert.assertEquals("[Time].[1997].[Q2].[4]", s);
    }
    
    public void testStrToMember() {
        String s = executeExpr("StrToMember(\"[Time].[1997].[Q2].[4]\").Name");
        Assert.assertEquals("4", s);
    }

}
