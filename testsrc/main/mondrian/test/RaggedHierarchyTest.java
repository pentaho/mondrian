/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2004-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.test;

/**
 * <code>RaggedHierarchyTest</code> tests ragged hierarchies.
 * <p>
 * I have disabled some tests by prefixing the tests name with "dont_".
 *
 * @author jhyde
 * @since Apr 19, 2004
 * @version $Id$
 **/
public class RaggedHierarchyTest extends FoodMartTestCase {
    private void assertRaggedReturns(String expression, String expected) {
        getTestContext("[Sales Ragged]").assertAxisReturns(expression, expected);
    }

    // ~ The tests ------------------------------------------------------------

    public void testChildrenOfRoot() {
        assertRaggedReturns("[Store].children",
                "[Store].[All Stores].[Canada]" + nl +
                "[Store].[All Stores].[Israel]" + nl +
                "[Store].[All Stores].[Mexico]" + nl +
                "[Store].[All Stores].[USA]" + nl +
                "[Store].[All Stores].[Vatican]");
    }
    public void testChildrenOfUSA() {
        assertRaggedReturns("[Store].[USA].children",
                "[Store].[All Stores].[USA].[CA]" + nl +
                "[Store].[All Stores].[USA].[OR]" + nl +
                "[Store].[All Stores].[USA].[USA].[Washington]" + nl +
                "[Store].[All Stores].[USA].[WA]");
    }
    // Israel has one real child, which is hidden, and which has children
    // Haifa and Tel Aviv
    public void testChildrenOfIsrael() {
        assertRaggedReturns("[Store].[Israel].children",
                "[Store].[All Stores].[Israel].[Israel].[Haifa]" + nl +
                "[Store].[All Stores].[Israel].[Israel].[Tel Aviv]");
    }

    // disabled: (1) does not work with SmartMemberReader and (2) test returns [null] member
    // Vatican's descendants at the province and city level are hidden
    public void dont_testChildrenOfVatican() {
        assertRaggedReturns("[Store].[Vatican].children",
                "[Store].[All Stores].[Vatican].[Vatican].[null].[Store 17]");
    }
    public void testParentOfHaifa() {
        assertRaggedReturns("[Store].[Israel].[Haifa].Parent",
                "[Store].[All Stores].[Israel]");
    }
    public void testParentOfVatican() {
        assertRaggedReturns("[Store].[Vatican].Parent",
                "[Store].[All Stores]");
    }
    // PrevMember must return something at the same level -- a city
    public void testPrevMemberOfHaifa() {
        assertRaggedReturns("[Store].[Israel].[Haifa].PrevMember",
                "[Store].[All Stores].[Canada].[BC].[Victoria]");
    }
    // PrevMember must return something at the same level -- a city
    public void testNextMemberOfTelAviv() {
        assertRaggedReturns("[Store].[Israel].[Tel Aviv].NextMember",
                "[Store].[All Stores].[Mexico].[DF].[Mexico City]");
    }
    public void testNextMemberOfBC() {
        // The next state after BC is Israel, but it's hidden
        assertRaggedReturns("[Store].[All Stores].[Canada].[BC].NextMember",
                "[Store].[All Stores].[Mexico].[DF]");
    }
    public void testLead() {
        assertRaggedReturns("[Store].[All Stores].[Mexico].[DF].Lead(1)",
                "[Store].[All Stores].[Mexico].[Guerrero]");
        assertRaggedReturns("[Store].[All Stores].[Mexico].[DF].Lead(0)",
                "[Store].[All Stores].[Mexico].[DF]");
        // Israel is immediately before Mexico, but is hidden
        assertRaggedReturns("[Store].[All Stores].[Mexico].[DF].Lead(-1)",
                "[Store].[All Stores].[Canada].[BC]");
        assertRaggedReturns("[Store].[All Stores].[Mexico].[DF].Lag(1)",
                "[Store].[All Stores].[Canada].[BC]");
        // Fall off the edge of the world
        assertRaggedReturns("[Store].[All Stores].[Mexico].[DF].Lead(-2)",
                "");
        assertRaggedReturns("[Store].[All Stores].[Mexico].[DF].Lead(-543)",
                "");
    }

    // disabled: (1) does not work with SmartMemberReader and (2) test returns [null] member
    public void dont_testDescendantsOfVatican() {
        assertRaggedReturns("Descendants([Store].[Vatican])",
                "[Store].[All Stores].[Vatican]" + nl +
                "[Store].[All Stores].[Vatican].[Vatican].[null].[Store 17]");
    }
    // The only child of Vatican at state level is hidden
    public void testDescendantsOfVaticanAtStateLevel() {
        assertRaggedReturns("Descendants([Store].[Vatican], [Store].[Store State])",
                "");
    }
    public void testDescendantsOfRootAtCity() {
        assertRaggedReturns("Descendants([Store], [Store City])",
                "[Store].[All Stores].[Canada].[BC].[Vancouver]" + nl +
                "[Store].[All Stores].[Canada].[BC].[Victoria]" + nl +
                "[Store].[All Stores].[Israel].[Israel].[Haifa]" + nl +
                "[Store].[All Stores].[Israel].[Israel].[Tel Aviv]" + nl +
                "[Store].[All Stores].[Mexico].[DF].[Mexico City]" + nl +
                "[Store].[All Stores].[Mexico].[DF].[San Andres]" + nl +
                "[Store].[All Stores].[Mexico].[Guerrero].[Acapulco]" + nl +
                "[Store].[All Stores].[Mexico].[Jalisco].[Guadalajara]" + nl +
                "[Store].[All Stores].[Mexico].[Veracruz].[Orizaba]" + nl +
                "[Store].[All Stores].[Mexico].[Yucatan].[Merida]" + nl +
                "[Store].[All Stores].[Mexico].[Zacatecas].[Camacho]" + nl +
                "[Store].[All Stores].[Mexico].[Zacatecas].[Hidalgo]" + nl +
                "[Store].[All Stores].[USA].[CA].[Alameda]" + nl +
                "[Store].[All Stores].[USA].[CA].[Beverly Hills]" + nl +
                "[Store].[All Stores].[USA].[CA].[Los Angeles]" + nl +
                "[Store].[All Stores].[USA].[CA].[San Francisco]" + nl +
                "[Store].[All Stores].[USA].[OR].[Portland]" + nl +
                "[Store].[All Stores].[USA].[OR].[Salem]" + nl +
                "[Store].[All Stores].[USA].[USA].[Washington]" + nl +
                "[Store].[All Stores].[USA].[WA].[Bellingham]" + nl +
                "[Store].[All Stores].[USA].[WA].[Bremerton]" + nl +
                "[Store].[All Stores].[USA].[WA].[Seattle]" + nl +
                "[Store].[All Stores].[USA].[WA].[Spokane]");
    }
    // no ancestor at the State level
    public void testAncestorOfHaifa() {
        assertRaggedReturns(
                "Ancestor([Store].[Israel].[Haifa], [Store].[Store State])",
                "");
    }
    public void testHierarchize() {
        // Haifa and Tel Aviv should appear directly after Israel
        // Vatican should have no children
        // Washington should appear after WA
        assertRaggedReturns(
                "Hierarchize(Descendants([Store], [Store].[Store City], SELF_AND_BEFORE))",
                "[Store].[All Stores]" + nl +
                "[Store].[All Stores].[Canada]" + nl +
                "[Store].[All Stores].[Canada].[BC]" + nl +
                "[Store].[All Stores].[Canada].[BC].[Vancouver]" + nl +
                "[Store].[All Stores].[Canada].[BC].[Victoria]" + nl +
                "[Store].[All Stores].[Israel]" + nl +
                "[Store].[All Stores].[Israel].[Israel].[Haifa]" + nl +
                "[Store].[All Stores].[Israel].[Israel].[Tel Aviv]" + nl +
                "[Store].[All Stores].[Mexico]" + nl +
                "[Store].[All Stores].[Mexico].[DF]" + nl +
                "[Store].[All Stores].[Mexico].[DF].[Mexico City]" + nl +
                "[Store].[All Stores].[Mexico].[DF].[San Andres]" + nl +
                "[Store].[All Stores].[Mexico].[Guerrero]" + nl +
                "[Store].[All Stores].[Mexico].[Guerrero].[Acapulco]" + nl +
                "[Store].[All Stores].[Mexico].[Jalisco]" + nl +
                "[Store].[All Stores].[Mexico].[Jalisco].[Guadalajara]" + nl +
                "[Store].[All Stores].[Mexico].[Veracruz]" + nl +
                "[Store].[All Stores].[Mexico].[Veracruz].[Orizaba]" + nl +
                "[Store].[All Stores].[Mexico].[Yucatan]" + nl +
                "[Store].[All Stores].[Mexico].[Yucatan].[Merida]" + nl +
                "[Store].[All Stores].[Mexico].[Zacatecas]" + nl +
                "[Store].[All Stores].[Mexico].[Zacatecas].[Camacho]" + nl +
                "[Store].[All Stores].[Mexico].[Zacatecas].[Hidalgo]" + nl +
                "[Store].[All Stores].[USA]" + nl +
                "[Store].[All Stores].[USA].[CA]" + nl +
                "[Store].[All Stores].[USA].[CA].[Alameda]" + nl +
                "[Store].[All Stores].[USA].[CA].[Beverly Hills]" + nl +
                "[Store].[All Stores].[USA].[CA].[Los Angeles]" + nl +
                "[Store].[All Stores].[USA].[CA].[San Francisco]" + nl +
                "[Store].[All Stores].[USA].[OR]" + nl +
                "[Store].[All Stores].[USA].[OR].[Portland]" + nl +
                "[Store].[All Stores].[USA].[OR].[Salem]" + nl +
                "[Store].[All Stores].[USA].[USA].[Washington]" + nl +
                "[Store].[All Stores].[USA].[WA]" + nl +
                "[Store].[All Stores].[USA].[WA].[Bellingham]" + nl +
                "[Store].[All Stores].[USA].[WA].[Bremerton]" + nl +
                "[Store].[All Stores].[USA].[WA].[Seattle]" + nl +
                "[Store].[All Stores].[USA].[WA].[Spokane]" + nl +
                "[Store].[All Stores].[Vatican]");
    }

    /**
     * Make sure that the numbers are right! 
     *
     * <p>The Vatican is the tricky case,
     * because one of the columns is null, so the SQL generator might get
     * confused.
     */
    // disabled: (1) does not work with SmartMemberReader and (2) test returns [null] member
    public void dont_testMeasuresVatican() {
        assertQueryReturns(
                "SELECT {[Measures].[Unit Sales]} ON COLUMNS," + nl +
                " {Descendants([Store].[Vatican])} ON ROWS" + nl +
                "FROM [Sales Ragged]",
                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Unit Sales]}" + nl +
                "Axis #2:" + nl +
                "{[Store].[All Stores].[Vatican]}" + nl +
                "{[Store].[All Stores].[Vatican].[Vatican].[null].[Store 17]}" + nl +
                "Row #0: 35,257" + nl +
                "Row #1: 35,257" + nl);
    }
    // Make sure that the numbers are right!
    /** disabled: (1) does not work with SmartMemberReader and (2) test returns [null] member ? */
    public void dont_testMeasures() {
        assertQueryReturns(
                "SELECT {[Measures].[Unit Sales]} ON COLUMNS," + nl +
                " NON EMPTY {Descendants([Store])} ON ROWS" + nl +
                "FROM [Sales Ragged]",
                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Unit Sales]}" + nl +
                "Axis #2:" + nl +
                "{[Store].[All Stores]}" + nl +
                "{[Store].[All Stores].[Israel]}" + nl +
                "{[Store].[All Stores].[Israel].[Israel].[Haifa]}" + nl +
                "{[Store].[All Stores].[Israel].[Israel].[Haifa].[Store 22]}" + nl +
                "{[Store].[All Stores].[Israel].[Israel].[Tel Aviv]}" + nl +
                "{[Store].[All Stores].[Israel].[Israel].[Tel Aviv].[Store 23]}" + nl +
                "{[Store].[All Stores].[USA]}" + nl +
                "{[Store].[All Stores].[USA].[CA]}" + nl +
                "{[Store].[All Stores].[USA].[CA].[Beverly Hills]}" + nl +
                "{[Store].[All Stores].[USA].[CA].[Beverly Hills].[Store 6]}" + nl +
                "{[Store].[All Stores].[USA].[CA].[Los Angeles]}" + nl +
                "{[Store].[All Stores].[USA].[CA].[Los Angeles].[Store 7]}" + nl +
                "{[Store].[All Stores].[USA].[CA].[San Francisco]}" + nl +
                "{[Store].[All Stores].[USA].[CA].[San Francisco].[Store 14]}" + nl +
                "{[Store].[All Stores].[USA].[OR]}" + nl +
                "{[Store].[All Stores].[USA].[OR].[Portland]}" + nl +
                "{[Store].[All Stores].[USA].[OR].[Portland].[Store 11]}" + nl +
                "{[Store].[All Stores].[USA].[OR].[Salem]}" + nl +
                "{[Store].[All Stores].[USA].[OR].[Salem].[Store 13]}" + nl +
                "{[Store].[All Stores].[USA].[USA].[Washington]}" + nl +
                "{[Store].[All Stores].[USA].[USA].[Washington].[Store 24]}" + nl +
                "{[Store].[All Stores].[USA].[WA]}" + nl +
                "{[Store].[All Stores].[USA].[WA].[Bellingham]}" + nl +
                "{[Store].[All Stores].[USA].[WA].[Bellingham].[Store 2]}" + nl +
                "{[Store].[All Stores].[USA].[WA].[Bremerton]}" + nl +
                "{[Store].[All Stores].[USA].[WA].[Bremerton].[Store 3]}" + nl +
                "{[Store].[All Stores].[USA].[WA].[Seattle]}" + nl +
                "{[Store].[All Stores].[USA].[WA].[Seattle].[Store 15]}" + nl +
                "{[Store].[All Stores].[USA].[WA].[Spokane]}" + nl +
                "{[Store].[All Stores].[USA].[WA].[Spokane].[Store 16]}" + nl +
                "{[Store].[All Stores].[Vatican]}" + nl +
                "{[Store].[All Stores].[Vatican].[Vatican].[null].[Store 17]}" + nl +
                "Row #0: 266,773" + nl +
                "Row #1: 13,694" + nl +
                "Row #2: 2,203" + nl +
                "Row #3: 2,203" + nl +
                "Row #4: 11,491" + nl +
                "Row #5: 11,491" + nl +
                "Row #6: 217,822" + nl +
                "Row #7: 49,113" + nl +
                "Row #8: 21,333" + nl +
                "Row #9: 21,333" + nl +
                "Row #10: 25,663" + nl +
                "Row #11: 25,663" + nl +
                "Row #12: 2,117" + nl +
                "Row #13: 2,117" + nl +
                "Row #14: 67,659" + nl +
                "Row #15: 26,079" + nl +
                "Row #16: 26,079" + nl +
                "Row #17: 41,580" + nl +
                "Row #18: 41,580" + nl +
                "Row #19: 25,635" + nl +
                "Row #20: 25,635" + nl +
                "Row #21: 75,415" + nl +
                "Row #22: 2,237" + nl +
                "Row #23: 2,237" + nl +
                "Row #24: 24,576" + nl +
                "Row #25: 24,576" + nl +
                "Row #26: 25,011" + nl +
                "Row #27: 25,011" + nl +
                "Row #28: 23,591" + nl +
                "Row #29: 23,591" + nl +
                "Row #30: 35,257" + nl +
                "Row #31: 35,257" + nl);
    }
}

// End RaggedHierarchyTest.java
