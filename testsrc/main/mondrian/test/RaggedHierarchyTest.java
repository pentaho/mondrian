/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2004-2004 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.test;

/**
 * <code>RaggedHierarchyTest</code> tests ragged hierarchies.
 *
 * @author jhyde
 * @since Apr 19, 2004
 * @version $Id$
 **/
public class RaggedHierarchyTest extends FoodMartTestCase {
    private void assertRaggedReturns(String expression, String expected) {
        assertAxisReturns("[Sales Ragged]", expression, expected);
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
    // Vatican's child is 2 deep
    public void testChildrenOfVatican() {
        assertRaggedReturns("[Store].[Vatican].children",
                "[Store].[All Stores].[Vatican].[Vatican].[Vatican]");
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
    // The next state after BC is Israel, but it's hidden
    public void _testNextMemberOfBC() {
        assertRaggedReturns("[Store].[All Stores].[Canada].[BC].NextMember",
                "?");
    }
    public void testDescendantsOfVatican() {
        assertRaggedReturns("Descendants([Store].[Vatican])",
                "[Store].[All Stores].[Vatican]" + nl +
                "[Store].[All Stores].[Vatican].[Vatican].[Vatican]" + nl +
                "[Store].[All Stores].[Vatican].[Vatican].[Vatican].[Store 17]");
    }
    // The only child of Vatican at state level is hidden
    public void testDescendantsOfVaticanAtStateLevel() {
        assertRaggedReturns("Descendants([Store].[Vatican], [Store].[Store State])",
                "");
    }
    public void _testDescendantsOfRoot() {
        assertRaggedReturns("Descendants([Store])",
                "?");
    }
    // no ancestor at the State level
    public void testAncestorOfHaifa() {
        assertRaggedReturns(
                "Ancestor([Store].[Israel].[Haifa], [Store].[Store State])",
                "");
    }
    // disabled until Descendants is fixed
    public void _testHierarchize() {
        // Haifa and Tel Aviv should appear directly after Israel
        // Vatican should have no children
        // Washington should appear after WA
        assertRaggedReturns(
                "Hierarchize(Descendants([Store], [Store].[Store City], SELF_AND_BEFORE))",
                "?");
    }
    // Make sure that the numbers are right!
    public void _testMeasures() {
        runQueryCheckResult(
                "SELECT {[Measures].[Unit Sales]} ON COLUMNS," + nl +
                " {Descendants([Store])} ON ROWS" + nl +
                "FROM [Sales Ragged]",
                "Axis #0:" + nl +
                "{}" + nl +
                "Axis #1:" + nl +
                "{[Measures].[Unit Sales]}" + nl +
                "Axis #2:" + nl +
                "{[Store].[All Stores]}" + nl +
                "{[Store].[All Stores].[Canada]}" + nl +
                "{[Store].[All Stores].[Israel]}" + nl +
                "{[Store].[All Stores].[Mexico]}" + nl +
                "{[Store].[All Stores].[USA]}" + nl +
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
                "{[Store].[All Stores].[Canada].[BC].[Vancouver]}" + nl +
                "{[Store].[All Stores].[Canada].[BC].[Victoria]}" + nl +
                "{[Store].[All Stores].[Israel].[Haifa]}" + nl +
                "{[Store].[All Stores].[Israel].[Tel Aviv]}" + nl +
                "{[Store].[All Stores].[Mexico].[DF].[Mexico City]}" + nl +
                "{[Store].[All Stores].[Mexico].[DF].[San Andres]}" + nl +
                "{[Store].[All Stores].[Mexico].[Guerrero].[Acapulco]}" + nl +
                "{[Store].[All Stores].[Mexico].[Jalisco].[Guadalajara]}" + nl +
                "{[Store].[All Stores].[Mexico].[Veracruz].[Orizaba]}" + nl +
                "{[Store].[All Stores].[Mexico].[Yucatan].[Merida]}" + nl +
                "{[Store].[All Stores].[Mexico].[Zacatecas].[Camacho]}" + nl +
                "{[Store].[All Stores].[Mexico].[Zacatecas].[Hidalgo]}" + nl +
                "{[Store].[All Stores].[USA].[CA].[Alameda]}" + nl +
                "{[Store].[All Stores].[USA].[CA].[Beverly Hills]}" + nl +
                "{[Store].[All Stores].[USA].[CA].[Los Angeles]}" + nl +
                "{[Store].[All Stores].[USA].[CA].[San Diego]}" + nl +
                "{[Store].[All Stores].[USA].[CA].[San Francisco]}" + nl +
                "{[Store].[All Stores].[USA].[OR].[Portland]}" + nl +
                "{[Store].[All Stores].[USA].[OR].[Salem]}" + nl +
                "{[Store].[All Stores].[USA].[WA].[Bellingham]}" + nl +
                "{[Store].[All Stores].[USA].[WA].[Bremerton]}" + nl +
                "{[Store].[All Stores].[USA].[WA].[Seattle]}" + nl +
                "{[Store].[All Stores].[USA].[WA].[Spokane]}" + nl +
                "{[Store].[All Stores].[USA].[WA].[Tacoma]}" + nl +
                "{[Store].[All Stores].[USA].[WA].[Walla Walla]}" + nl +
                "{[Store].[All Stores].[USA].[WA].[Yakima]}" + nl +
                "{[Store].[All Stores].[Vatican]]}" + nl +
                "{[Store].[All Stores].[Canada].[BC].[Vancouver].[Store 19]}" + nl +
                "{[Store].[All Stores].[Canada].[BC].[Victoria].[Store 20]}" + nl +
                "{[Store].[All Stores].[Israel].[Haifa].[Store 22]}" + nl +
                "{[Store].[All Stores].[Israel].[Tel Aviv].[Store 23]}" + nl +
                "{[Store].[All Stores].[Mexico].[DF].[Mexico City].[Store 9]}" + nl +
                "{[Store].[All Stores].[Mexico].[DF].[San Andres].[Store 21]}" + nl +
                "{[Store].[All Stores].[Mexico].[Guerrero].[Acapulco].[Store 1]}" + nl +
                "{[Store].[All Stores].[Mexico].[Jalisco].[Guadalajara].[Store 5]}" + nl +
                "{[Store].[All Stores].[Mexico].[Veracruz].[Orizaba].[Store 10]}" + nl +
                "{[Store].[All Stores].[Mexico].[Yucatan].[Merida].[Store 8]}" + nl +
                "{[Store].[All Stores].[Mexico].[Zacatecas].[Camacho].[Store 4]}" + nl +
                "{[Store].[All Stores].[Mexico].[Zacatecas].[Hidalgo].[Store 12]}" + nl +
                "{[Store].[All Stores].[Mexico].[Zacatecas].[Hidalgo].[Store 18]}" + nl +
                "{[Store].[All Stores].[USA].[CA].[Alameda].[HQ]}" + nl +
                "{[Store].[All Stores].[USA].[CA].[Beverly Hills].[Store 6]}" + nl +
                "{[Store].[All Stores].[USA].[CA].[Los Angeles].[Store 7]}" + nl +
                "{[Store].[All Stores].[USA].[Washington].[Store 24]}" + nl +
                "{[Store].[All Stores].[USA].[CA].[San Francisco].[Store 14]}" + nl +
                "{[Store].[All Stores].[USA].[OR].[Portland].[Store 11]}" + nl +
                "{[Store].[All Stores].[USA].[OR].[Salem].[Store 13]}" + nl +
                "{[Store].[All Stores].[USA].[WA].[Bellingham].[Store 2]}" + nl +
                "{[Store].[All Stores].[USA].[WA].[Bremerton].[Store 3]}" + nl +
                "{[Store].[All Stores].[USA].[WA].[Seattle].[Store 15]}" + nl +
                "{[Store].[All Stores].[USA].[Spokane].[Store 16]}" + nl +
                "{[Store].[All Stores].[Vatican].[Store 17]}" + nl +
                "Row #0: 266,773" + nl +
                "Row #1: (null)" + nl +
                "Row #2: (null)" + nl +
                "Row #3: 266,773" + nl +
                "Row #4: (null)" + nl +
                "Row #5: (null)" + nl +
                "Row #6: (null)" + nl +
                "Row #7: (null)" + nl +
                "Row #8: (null)" + nl +
                "Row #9: (null)" + nl +
                "Row #10: (null)" + nl +
                "Row #11: 74,748" + nl +
                "Row #12: 67,659" + nl +
                "Row #13: 124,366" + nl +
                "Row #14: (null)" + nl +
                "Row #15: (null)" + nl +
                "Row #16: (null)" + nl +
                "Row #17: (null)" + nl +
                "Row #18: (null)" + nl +
                "Row #19: (null)" + nl +
                "Row #20: (null)" + nl +
                "Row #21: (null)" + nl +
                "Row #22: (null)" + nl +
                "Row #23: (null)" + nl +
                "Row #24: (null)" + nl +
                "Row #25: 21,333" + nl +
                "Row #26: 25,663" + nl +
                "Row #27: 25,635" + nl +
                "Row #28: 2,117" + nl +
                "Row #29: 26,079" + nl +
                "Row #30: 41,580" + nl +
                "Row #31: 2,237" + nl +
                "Row #32: 24,576" + nl +
                "Row #33: 25,011" + nl +
                "Row #34: 23,591" + nl +
                "Row #35: 35,257" + nl +
                "Row #36: 2,203" + nl +
                "Row #37: 11,491" + nl +
                "Row #38: (null)" + nl +
                "Row #39: (null)" + nl +
                "Row #40: (null)" + nl +
                "Row #41: (null)" + nl +
                "Row #42: (null)" + nl +
                "Row #43: (null)" + nl +
                "Row #44: (null)" + nl +
                "Row #45: (null)" + nl +
                "Row #46: (null)" + nl +
                "Row #47: (null)" + nl +
                "Row #48: (null)" + nl +
                "Row #49: (null)" + nl +
                "Row #50: 21,333" + nl +
                "Row #51: 25,663" + nl +
                "Row #52: 25,635" + nl +
                "Row #53: 2,117" + nl +
                "Row #54: 26,079" + nl +
                "Row #55: 41,580" + nl +
                "Row #56: 2,237" + nl +
                "Row #57: 24,576" + nl +
                "Row #58: 25,011" + nl +
                "Row #59: 23,591" + nl +
                "Row #60: 35,257" + nl +
                "Row #61: 2,203" + nl +
                "Row #62: 11,491" + nl);
    }
}

// End RaggedHierarchyTest.java
