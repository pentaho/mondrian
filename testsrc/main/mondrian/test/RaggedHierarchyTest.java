/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2004-2009 Julian Hyde
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
 */
public class RaggedHierarchyTest extends FoodMartTestCase {
    private void assertRaggedReturns(String expression, String expected) {
        getTestContext().withCube("[Sales Ragged]")
            .assertAxisReturns(expression, expected);
    }

    // ~ The tests ------------------------------------------------------------

    public void testChildrenOfRoot() {
        assertRaggedReturns(
            "[Store].children",
            "[Store].[All Stores].[Canada]\n"
            + "[Store].[All Stores].[Israel]\n"
            + "[Store].[All Stores].[Mexico]\n"
            + "[Store].[All Stores].[USA]\n"
            + "[Store].[All Stores].[Vatican]");
    }

    public void testChildrenOfUSA() {
        assertRaggedReturns(
            "[Store].[USA].children",
            "[Store].[All Stores].[USA].[CA]\n"
            + "[Store].[All Stores].[USA].[OR]\n"
            + "[Store].[All Stores].[USA].[USA].[Washington]\n"
            + "[Store].[All Stores].[USA].[WA]");
    }

    // Israel has one real child, which is hidden, and which has children
    // Haifa and Tel Aviv
    public void testChildrenOfIsrael() {
        assertRaggedReturns(
            "[Store].[Israel].children",
            "[Store].[All Stores].[Israel].[Israel].[Haifa]\n"
            + "[Store].[All Stores].[Israel].[Israel].[Tel Aviv]");
    }

    // disabled: (1) does not work with SmartMemberReader and
    // (2) test returns [null] member
    // Vatican's descendants at the province and city level are hidden
    public void dont_testChildrenOfVatican() {
        assertRaggedReturns(
            "[Store].[Vatican].children",
            "[Store].[All Stores].[Vatican].[Vatican].[null].[Store 17]");
    }

    public void testParentOfHaifa() {
        assertRaggedReturns(
            "[Store].[Israel].[Haifa].Parent", "[Store].[All Stores].[Israel]");
    }

    public void testParentOfVatican() {
        assertRaggedReturns(
            "[Store].[Vatican].Parent", "[Store].[All Stores]");
    }

    // PrevMember must return something at the same level -- a city
    public void testPrevMemberOfHaifa() {
        assertRaggedReturns(
            "[Store].[Israel].[Haifa].PrevMember",
            "[Store].[All Stores].[Canada].[BC].[Victoria]");
    }

    // PrevMember must return something at the same level -- a city
    public void testNextMemberOfTelAviv() {
        assertRaggedReturns(
            "[Store].[Israel].[Tel Aviv].NextMember",
            "[Store].[All Stores].[Mexico].[DF].[Mexico City]");
    }

    public void testNextMemberOfBC() {
        // The next state after BC is Israel, but it's hidden
        assertRaggedReturns(
            "[Store].[All Stores].[Canada].[BC].NextMember",
            "[Store].[All Stores].[Mexico].[DF]");
    }

    public void testLead() {
        assertRaggedReturns(
            "[Store].[All Stores].[Mexico].[DF].Lead(1)",
            "[Store].[All Stores].[Mexico].[Guerrero]");
        assertRaggedReturns(
            "[Store].[All Stores].[Mexico].[DF].Lead(0)",
            "[Store].[All Stores].[Mexico].[DF]");
        // Israel is immediately before Mexico, but is hidden
        assertRaggedReturns(
            "[Store].[All Stores].[Mexico].[DF].Lead(-1)",
            "[Store].[All Stores].[Canada].[BC]");
        assertRaggedReturns(
            "[Store].[All Stores].[Mexico].[DF].Lag(1)",
            "[Store].[All Stores].[Canada].[BC]");
        // Fall off the edge of the world
        assertRaggedReturns(
            "[Store].[All Stores].[Mexico].[DF].Lead(-2)", "");
        assertRaggedReturns(
            "[Store].[All Stores].[Mexico].[DF].Lead(-543)", "");
    }

    // disabled: (1) does not work with SmartMemberReader and (2) test returns
    // [null] member
    public void dont_testDescendantsOfVatican() {
        assertRaggedReturns(
            "Descendants([Store].[Vatican])",
            "[Store].[All Stores].[Vatican]\n"
            + "[Store].[All Stores].[Vatican].[Vatican].[null].[Store 17]");
    }

    // The only child of Vatican at state level is hidden
    public void testDescendantsOfVaticanAtStateLevel() {
        assertRaggedReturns(
            "Descendants([Store].[Vatican], [Store].[Store State])",
            "");
    }

    public void testDescendantsOfRootAtCity() {
        assertRaggedReturns(
            "Descendants([Store], [Store City])",
            "[Store].[All Stores].[Canada].[BC].[Vancouver]\n"
            + "[Store].[All Stores].[Canada].[BC].[Victoria]\n"
            + "[Store].[All Stores].[Israel].[Israel].[Haifa]\n"
            + "[Store].[All Stores].[Israel].[Israel].[Tel Aviv]\n"
            + "[Store].[All Stores].[Mexico].[DF].[Mexico City]\n"
            + "[Store].[All Stores].[Mexico].[DF].[San Andres]\n"
            + "[Store].[All Stores].[Mexico].[Guerrero].[Acapulco]\n"
            + "[Store].[All Stores].[Mexico].[Jalisco].[Guadalajara]\n"
            + "[Store].[All Stores].[Mexico].[Veracruz].[Orizaba]\n"
            + "[Store].[All Stores].[Mexico].[Yucatan].[Merida]\n"
            + "[Store].[All Stores].[Mexico].[Zacatecas].[Camacho]\n"
            + "[Store].[All Stores].[Mexico].[Zacatecas].[Hidalgo]\n"
            + "[Store].[All Stores].[USA].[CA].[Alameda]\n"
            + "[Store].[All Stores].[USA].[CA].[Beverly Hills]\n"
            + "[Store].[All Stores].[USA].[CA].[Los Angeles]\n"
            + "[Store].[All Stores].[USA].[CA].[San Francisco]\n"
            + "[Store].[All Stores].[USA].[OR].[Portland]\n"
            + "[Store].[All Stores].[USA].[OR].[Salem]\n"
            + "[Store].[All Stores].[USA].[USA].[Washington]\n"
            + "[Store].[All Stores].[USA].[WA].[Bellingham]\n"
            + "[Store].[All Stores].[USA].[WA].[Bremerton]\n"
            + "[Store].[All Stores].[USA].[WA].[Seattle]\n"
            + "[Store].[All Stores].[USA].[WA].[Spokane]");
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
            "[Store].[All Stores]\n"
            + "[Store].[All Stores].[Canada]\n"
            + "[Store].[All Stores].[Canada].[BC]\n"
            + "[Store].[All Stores].[Canada].[BC].[Vancouver]\n"
            + "[Store].[All Stores].[Canada].[BC].[Victoria]\n"
            + "[Store].[All Stores].[Israel]\n"
            + "[Store].[All Stores].[Israel].[Israel].[Haifa]\n"
            + "[Store].[All Stores].[Israel].[Israel].[Tel Aviv]\n"
            + "[Store].[All Stores].[Mexico]\n"
            + "[Store].[All Stores].[Mexico].[DF]\n"
            + "[Store].[All Stores].[Mexico].[DF].[Mexico City]\n"
            + "[Store].[All Stores].[Mexico].[DF].[San Andres]\n"
            + "[Store].[All Stores].[Mexico].[Guerrero]\n"
            + "[Store].[All Stores].[Mexico].[Guerrero].[Acapulco]\n"
            + "[Store].[All Stores].[Mexico].[Jalisco]\n"
            + "[Store].[All Stores].[Mexico].[Jalisco].[Guadalajara]\n"
            + "[Store].[All Stores].[Mexico].[Veracruz]\n"
            + "[Store].[All Stores].[Mexico].[Veracruz].[Orizaba]\n"
            + "[Store].[All Stores].[Mexico].[Yucatan]\n"
            + "[Store].[All Stores].[Mexico].[Yucatan].[Merida]\n"
            + "[Store].[All Stores].[Mexico].[Zacatecas]\n"
            + "[Store].[All Stores].[Mexico].[Zacatecas].[Camacho]\n"
            + "[Store].[All Stores].[Mexico].[Zacatecas].[Hidalgo]\n"
            + "[Store].[All Stores].[USA]\n"
            + "[Store].[All Stores].[USA].[CA]\n"
            + "[Store].[All Stores].[USA].[CA].[Alameda]\n"
            + "[Store].[All Stores].[USA].[CA].[Beverly Hills]\n"
            + "[Store].[All Stores].[USA].[CA].[Los Angeles]\n"
            + "[Store].[All Stores].[USA].[CA].[San Francisco]\n"
            + "[Store].[All Stores].[USA].[OR]\n"
            + "[Store].[All Stores].[USA].[OR].[Portland]\n"
            + "[Store].[All Stores].[USA].[OR].[Salem]\n"
            + "[Store].[All Stores].[USA].[USA].[Washington]\n"
            + "[Store].[All Stores].[USA].[WA]\n"
            + "[Store].[All Stores].[USA].[WA].[Bellingham]\n"
            + "[Store].[All Stores].[USA].[WA].[Bremerton]\n"
            + "[Store].[All Stores].[USA].[WA].[Seattle]\n"
            + "[Store].[All Stores].[USA].[WA].[Spokane]\n"
            + "[Store].[All Stores].[Vatican]");
    }

    /**
     * Make sure that the numbers are right!
     *
     * <p>The Vatican is the tricky case,
     * because one of the columns is null, so the SQL generator might get
     * confused.
     */
    // disabled: (1) does not work with SmartMemberReader and (2) test returns
    // [null] member
    public void dont_testMeasuresVatican() {
        assertQueryReturns(
            "SELECT {[Measures].[Unit Sales]} ON COLUMNS,\n"
            + " {Descendants([Store].[Vatican])} ON ROWS\n"
            + "FROM [Sales Ragged]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Store].[All Stores].[Vatican]}\n"
            + "{[Store].[All Stores].[Vatican].[Vatican].[null].[Store 17]}\n"
            + "Row #0: 35,257\n"
            + "Row #1: 35,257\n");
    }

    // Make sure that the numbers are right!
    /**
     * disabled: (1) does not work with SmartMemberReader and (2) test returns
     * [null] member?
     */
    public void dont_testMeasures() {
        assertQueryReturns(
            "SELECT {[Measures].[Unit Sales]} ON COLUMNS,\n"
            + " NON EMPTY {Descendants([Store])} ON ROWS\n"
            + "FROM [Sales Ragged]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Store].[All Stores]}\n"
            + "{[Store].[All Stores].[Israel]}\n"
            + "{[Store].[All Stores].[Israel].[Israel].[Haifa]}\n"
            + "{[Store].[All Stores].[Israel].[Israel].[Haifa].[Store 22]}\n"
            + "{[Store].[All Stores].[Israel].[Israel].[Tel Aviv]}\n"
            + "{[Store].[All Stores].[Israel].[Israel].[Tel Aviv].[Store 23]}\n"
            + "{[Store].[All Stores].[USA]}\n"
            + "{[Store].[All Stores].[USA].[CA]}\n"
            + "{[Store].[All Stores].[USA].[CA].[Beverly Hills]}\n"
            + "{[Store].[All Stores].[USA].[CA].[Beverly Hills].[Store 6]}\n"
            + "{[Store].[All Stores].[USA].[CA].[Los Angeles]}\n"
            + "{[Store].[All Stores].[USA].[CA].[Los Angeles].[Store 7]}\n"
            + "{[Store].[All Stores].[USA].[CA].[San Francisco]}\n"
            + "{[Store].[All Stores].[USA].[CA].[San Francisco].[Store 14]}\n"
            + "{[Store].[All Stores].[USA].[OR]}\n"
            + "{[Store].[All Stores].[USA].[OR].[Portland]}\n"
            + "{[Store].[All Stores].[USA].[OR].[Portland].[Store 11]}\n"
            + "{[Store].[All Stores].[USA].[OR].[Salem]}\n"
            + "{[Store].[All Stores].[USA].[OR].[Salem].[Store 13]}\n"
            + "{[Store].[All Stores].[USA].[USA].[Washington]}\n"
            + "{[Store].[All Stores].[USA].[USA].[Washington].[Store 24]}\n"
            + "{[Store].[All Stores].[USA].[WA]}\n"
            + "{[Store].[All Stores].[USA].[WA].[Bellingham]}\n"
            + "{[Store].[All Stores].[USA].[WA].[Bellingham].[Store 2]}\n"
            + "{[Store].[All Stores].[USA].[WA].[Bremerton]}\n"
            + "{[Store].[All Stores].[USA].[WA].[Bremerton].[Store 3]}\n"
            + "{[Store].[All Stores].[USA].[WA].[Seattle]}\n"
            + "{[Store].[All Stores].[USA].[WA].[Seattle].[Store 15]}\n"
            + "{[Store].[All Stores].[USA].[WA].[Spokane]}\n"
            + "{[Store].[All Stores].[USA].[WA].[Spokane].[Store 16]}\n"
            + "{[Store].[All Stores].[Vatican]}\n"
            + "{[Store].[All Stores].[Vatican].[Vatican].[null].[Store 17]}\n"
            + "Row #0: 266,773\n"
            + "Row #1: 13,694\n"
            + "Row #2: 2,203\n"
            + "Row #3: 2,203\n"
            + "Row #4: 11,491\n"
            + "Row #5: 11,491\n"
            + "Row #6: 217,822\n"
            + "Row #7: 49,113\n"
            + "Row #8: 21,333\n"
            + "Row #9: 21,333\n"
            + "Row #10: 25,663\n"
            + "Row #11: 25,663\n"
            + "Row #12: 2,117\n"
            + "Row #13: 2,117\n"
            + "Row #14: 67,659\n"
            + "Row #15: 26,079\n"
            + "Row #16: 26,079\n"
            + "Row #17: 41,580\n"
            + "Row #18: 41,580\n"
            + "Row #19: 25,635\n"
            + "Row #20: 25,635\n"
            + "Row #21: 75,415\n"
            + "Row #22: 2,237\n"
            + "Row #23: 2,237\n"
            + "Row #24: 24,576\n"
            + "Row #25: 24,576\n"
            + "Row #26: 25,011\n"
            + "Row #27: 25,011\n"
            + "Row #28: 23,591\n"
            + "Row #29: 23,591\n"
            + "Row #30: 35,257\n"
            + "Row #31: 35,257\n");
    }
}

// End RaggedHierarchyTest.java
