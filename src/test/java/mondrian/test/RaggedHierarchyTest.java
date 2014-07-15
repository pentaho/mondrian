/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 Julian Hyde
// Copyright (C) 2005-2014 Pentaho
// All Rights Reserved.
*/
package mondrian.test;


/**
 * Unit test for ragged hierarchies.
 *
 * <p>Some tests are disabled by prefixing the tests name with "dont_".</p>
 *
 * @author jhyde
 * @since Apr 19, 2004
 */
public class RaggedHierarchyTest extends FoodMartTestCase {
    @Override
    public TestContext getTestContext() {
        return super.getTestContext().legacy().withCube("Sales Ragged");
    }

    // ~ The tests ------------------------------------------------------------

    public void testChildrenOfRoot() {
        assertAxisReturns(
            "[Store].[Store].children",
            "[Store].[Store].[Canada]\n"
            + "[Store].[Store].[Israel]\n"
            + "[Store].[Store].[Mexico]\n"
            + "[Store].[Store].[USA]\n"
            + "[Store].[Store].[Vatican]");
    }

    public void testChildrenOfUSA() {
        assertAxisReturns(
            "[Store].[USA].children",
            "[Store].[Store].[USA].[CA]\n"
            + "[Store].[Store].[USA].[OR]\n"
            + "[Store].[Store].[USA].[USA].[Washington]\n"
            + "[Store].[Store].[USA].[WA]");
    }

    // Israel has one real child, which is hidden, and which has children
    // Haifa and Tel Aviv
    public void testChildrenOfIsrael() {
        assertAxisReturns(
            "[Store].[Israel].children",
            "[Store].[Store].[Israel].[Israel].[Haifa]\n"
            + "[Store].[Store].[Israel].[Israel].[Tel Aviv]");
    }

    // disabled: (1) does not work with SmartMemberReader and
    // (2) test returns [null] member
    // Vatican's descendants at the province and city level are hidden
    public void testChildrenOfVatican() {
        assertAxisReturns(
            "[Store].[Vatican].children",
            "[Store].[Store].[Vatican].[Vatican].[#null].[Store 17]");
    }

    public void testParentOfHaifa() {
        assertAxisReturns(
            "[Store].[Israel].[Haifa].Parent",
            "[Store].[Store].[Israel]");
    }

    public void testParentOfVatican() {
        assertAxisReturns(
            "[Store].[Vatican].Parent",
            "[Store].[Store].[All Stores]");
    }

    // PrevMember must return something at the same level -- a city
    public void testPrevMemberOfHaifa() {
        assertAxisReturns(
            "[Store].[Israel].[Haifa].PrevMember",
            "[Store].[Store].[Canada].[BC].[Victoria]");
    }

    // PrevMember must return something at the same level -- a city
    public void testNextMemberOfTelAviv() {
        assertAxisReturns(
            "[Store].[Israel].[Tel Aviv].NextMember",
            "[Store].[Store].[Mexico].[DF].[Mexico City]");
    }

    public void testNextMemberOfBC() {
        // The next state after BC is Israel, but it's hidden
        assertAxisReturns(
            "[Store].[Canada].[BC].NextMember",
            "[Store].[Store].[Mexico].[DF]");
    }

    public void testLead() {
        assertAxisReturns(
            "[Store].[Mexico].[DF].Lead(1)",
            "[Store].[Store].[Mexico].[Guerrero]");
        assertAxisReturns(
            "[Store].[Mexico].[DF].Lead(0)",
            "[Store].[Store].[Mexico].[DF]");
        // Israel is immediately before Mexico, but is hidden
        assertAxisReturns(
            "[Store].[Mexico].[DF].Lead(-1)",
            "[Store].[Store].[Canada].[BC]");
        assertAxisReturns(
            "[Store].[Mexico].[DF].Lag(1)",
            "[Store].[Store].[Canada].[BC]");
        // Fall off the edge of the world
        assertAxisReturns(
            "[Store].[Mexico].[DF].Lead(-2)", "");
        assertAxisReturns(
            "[Store].[Mexico].[DF].Lead(-543)", "");
    }

    // disabled: (1) does not work with SmartMemberReader and (2) test returns
    // [null] member
    public void testDescendantsOfVatican() {
        assertAxisReturns(
            "Descendants([Store].[Vatican])",
            "[Store].[Store].[Vatican]\n"
            + "[Store].[Store].[Vatican].[Vatican].[#null].[Store 17]");
    }

    // The only child of Vatican at state level is hidden
    public void testDescendantsOfVaticanAtStateLevel() {
        assertAxisReturns(
            "Descendants([Store].[Vatican], [Store].[Store State])",
            "");
    }

    public void testDescendantsOfRootAtCity() {
        assertAxisReturns(
            "Descendants([Store], [Store City])",
            "[Store].[Store].[Canada].[BC].[Vancouver]\n"
            + "[Store].[Store].[Canada].[BC].[Victoria]\n"
            + "[Store].[Store].[Israel].[Israel].[Haifa]\n"
            + "[Store].[Store].[Israel].[Israel].[Tel Aviv]\n"
            + "[Store].[Store].[Mexico].[DF].[Mexico City]\n"
            + "[Store].[Store].[Mexico].[DF].[San Andres]\n"
            + "[Store].[Store].[Mexico].[Guerrero].[Acapulco]\n"
            + "[Store].[Store].[Mexico].[Jalisco].[Guadalajara]\n"
            + "[Store].[Store].[Mexico].[Veracruz].[Orizaba]\n"
            + "[Store].[Store].[Mexico].[Yucatan].[Merida]\n"
            + "[Store].[Store].[Mexico].[Zacatecas].[Camacho]\n"
            + "[Store].[Store].[Mexico].[Zacatecas].[Hidalgo]\n"
            + "[Store].[Store].[USA].[CA].[Alameda]\n"
            + "[Store].[Store].[USA].[CA].[Beverly Hills]\n"
            + "[Store].[Store].[USA].[CA].[Los Angeles]\n"
            + "[Store].[Store].[USA].[CA].[San Francisco]\n"
            + "[Store].[Store].[USA].[OR].[Portland]\n"
            + "[Store].[Store].[USA].[OR].[Salem]\n"
            + "[Store].[Store].[USA].[USA].[Washington]\n"
            + "[Store].[Store].[USA].[WA].[Bellingham]\n"
            + "[Store].[Store].[USA].[WA].[Bremerton]\n"
            + "[Store].[Store].[USA].[WA].[Seattle]\n"
            + "[Store].[Store].[USA].[WA].[Spokane]");
    }

    // no ancestor at the State level
    public void testAncestorOfHaifa() {
        assertAxisReturns(
            "Ancestor([Store].[Israel].[Haifa], [Store].[Store State])",
            "");
    }

    public void testHierarchize() {
        // Haifa and Tel Aviv should appear directly after Israel
        // Vatican should have no children
        // Washington should appear after WA
        assertAxisReturns(
            "Hierarchize(Descendants([Store], [Store].[Store City], SELF_AND_BEFORE))",
            "[Store].[Store].[All Stores]\n"
            + "[Store].[Store].[Canada]\n"
            + "[Store].[Store].[Canada].[BC]\n"
            + "[Store].[Store].[Canada].[BC].[Vancouver]\n"
            + "[Store].[Store].[Canada].[BC].[Victoria]\n"
            + "[Store].[Store].[Israel]\n"
            + "[Store].[Store].[Israel].[Israel].[Haifa]\n"
            + "[Store].[Store].[Israel].[Israel].[Tel Aviv]\n"
            + "[Store].[Store].[Mexico]\n"
            + "[Store].[Store].[Mexico].[DF]\n"
            + "[Store].[Store].[Mexico].[DF].[Mexico City]\n"
            + "[Store].[Store].[Mexico].[DF].[San Andres]\n"
            + "[Store].[Store].[Mexico].[Guerrero]\n"
            + "[Store].[Store].[Mexico].[Guerrero].[Acapulco]\n"
            + "[Store].[Store].[Mexico].[Jalisco]\n"
            + "[Store].[Store].[Mexico].[Jalisco].[Guadalajara]\n"
            + "[Store].[Store].[Mexico].[Veracruz]\n"
            + "[Store].[Store].[Mexico].[Veracruz].[Orizaba]\n"
            + "[Store].[Store].[Mexico].[Yucatan]\n"
            + "[Store].[Store].[Mexico].[Yucatan].[Merida]\n"
            + "[Store].[Store].[Mexico].[Zacatecas]\n"
            + "[Store].[Store].[Mexico].[Zacatecas].[Camacho]\n"
            + "[Store].[Store].[Mexico].[Zacatecas].[Hidalgo]\n"
            + "[Store].[Store].[USA]\n"
            + "[Store].[Store].[USA].[CA]\n"
            + "[Store].[Store].[USA].[CA].[Alameda]\n"
            + "[Store].[Store].[USA].[CA].[Beverly Hills]\n"
            + "[Store].[Store].[USA].[CA].[Los Angeles]\n"
            + "[Store].[Store].[USA].[CA].[San Francisco]\n"
            + "[Store].[Store].[USA].[OR]\n"
            + "[Store].[Store].[USA].[OR].[Portland]\n"
            + "[Store].[Store].[USA].[OR].[Salem]\n"
            + "[Store].[Store].[USA].[USA].[Washington]\n"
            + "[Store].[Store].[USA].[WA]\n"
            + "[Store].[Store].[USA].[WA].[Bellingham]\n"
            + "[Store].[Store].[USA].[WA].[Bremerton]\n"
            + "[Store].[Store].[USA].[WA].[Seattle]\n"
            + "[Store].[Store].[USA].[WA].[Spokane]\n"
            + "[Store].[Store].[Vatican]");
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
    public void testMeasuresVatican() {
        assertQueryReturns(
            "SELECT {[Measures].[Unit Sales]} ON COLUMNS,\n"
            + " {Descendants([Store].[Vatican])} ON ROWS\n"
            + "FROM [Sales Ragged]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Store].[Store].[Vatican]}\n"
            + "{[Store].[Store].[Vatican].[Vatican].[#null].[Store 17]}\n"
            + "Row #0: 35,257\n"
            + "Row #1: 35,257\n");
    }

    // Make sure that the numbers are right!
    /**
     * disabled: (1) does not work with SmartMemberReader and (2) test returns
     * [null] member?
     */
    public void testMeasures() {
        assertQueryReturns(
            "SELECT {[Measures].[Unit Sales]} ON COLUMNS,\n"
            + " NON EMPTY {Descendants([Store])} ON ROWS\n"
            + "FROM [Sales Ragged]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Store].[Store].[All Stores]}\n"
            + "{[Store].[Store].[Israel]}\n"
            + "{[Store].[Store].[Israel].[Israel].[Haifa]}\n"
            + "{[Store].[Store].[Israel].[Israel].[Haifa].[Store 22]}\n"
            + "{[Store].[Store].[Israel].[Israel].[Tel Aviv]}\n"
            + "{[Store].[Store].[Israel].[Israel].[Tel Aviv].[Store 23]}\n"
            + "{[Store].[Store].[USA]}\n"
            + "{[Store].[Store].[USA].[CA]}\n"
            + "{[Store].[Store].[USA].[CA].[Beverly Hills]}\n"
            + "{[Store].[Store].[USA].[CA].[Beverly Hills].[Store 6]}\n"
            + "{[Store].[Store].[USA].[CA].[Los Angeles]}\n"
            + "{[Store].[Store].[USA].[CA].[Los Angeles].[Store 7]}\n"
            + "{[Store].[Store].[USA].[CA].[San Francisco]}\n"
            + "{[Store].[Store].[USA].[CA].[San Francisco].[Store 14]}\n"
            + "{[Store].[Store].[USA].[OR]}\n"
            + "{[Store].[Store].[USA].[OR].[Portland]}\n"
            + "{[Store].[Store].[USA].[OR].[Portland].[Store 11]}\n"
            + "{[Store].[Store].[USA].[OR].[Salem]}\n"
            + "{[Store].[Store].[USA].[OR].[Salem].[Store 13]}\n"
            + "{[Store].[Store].[USA].[USA].[Washington]}\n"
            + "{[Store].[Store].[USA].[USA].[Washington].[Store 24]}\n"
            + "{[Store].[Store].[USA].[WA]}\n"
            + "{[Store].[Store].[USA].[WA].[Bellingham]}\n"
            + "{[Store].[Store].[USA].[WA].[Bellingham].[Store 2]}\n"
            + "{[Store].[Store].[USA].[WA].[Bremerton]}\n"
            + "{[Store].[Store].[USA].[WA].[Bremerton].[Store 3]}\n"
            + "{[Store].[Store].[USA].[WA].[Seattle]}\n"
            + "{[Store].[Store].[USA].[WA].[Seattle].[Store 15]}\n"
            + "{[Store].[Store].[USA].[WA].[Spokane]}\n"
            + "{[Store].[Store].[USA].[WA].[Spokane].[Store 16]}\n"
            + "{[Store].[Store].[Vatican]}\n"
            + "{[Store].[Store].[Vatican].[Vatican].[#null].[Store 17]}\n"
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

    /**
     * Test case for bug
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-628">MONDRIAN-628</a>,
     * "ClassCastException in Mondrian for query using Sales Ragged cube".
     *
     * <p>Cause was that ancestor yielded a null member, which was a RolapMember
     * but Order required it to be a RolapCubeMember.
     */
    public void testNullMember() {
        assertQueryReturns(
            "With \n"
            + " Set [*NATIVE_CJ_SET] as '[*BASE_MEMBERS_Geography]' \n"
            + " Set [*SORTED_ROW_AXIS] as 'Order([*CJ_ROW_AXIS],Ancestor([Geography].CurrentMember, [Geography].[Country]).OrderKey,BASC,Ancestor([Geography].CurrentMember, [Geography].[State]).OrderKey,BASC,[Geography].CurrentMember.OrderKey,BASC)' \n"
            + " Set [*BASE_MEMBERS_Geography] as '[Geography].[City].Members' \n"
            + " Set [*NATIVE_MEMBERS_Geography] as 'Generate([*NATIVE_CJ_SET], {[Geography].CurrentMember})' \n"
            + " Set [*BASE_MEMBERS_Measures] as '{[Measures].[*ZERO]}' \n"
            + " Set [*CJ_ROW_AXIS] as 'Generate([*NATIVE_CJ_SET], {([Geography].currentMember)})' \n"
            + " Set [*CJ_COL_AXIS] as '[*NATIVE_CJ_SET]' \n"
            + " Member [Measures].[*ZERO] as '0', SOLVE_ORDER=0 \n"
            + " Select \n"
            + " [*BASE_MEMBERS_Measures] on columns, \n"
            + " [*SORTED_ROW_AXIS] on rows \n"
            + " From [Sales Ragged]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[*ZERO]}\n"
            + "Axis #2:\n"
            + "{[Geography].[Geography].[Canada].[BC].[Vancouver]}\n"
            + "{[Geography].[Geography].[Canada].[BC].[Victoria]}\n"
            + "{[Geography].[Geography].[Israel].[Israel].[Haifa]}\n"
            + "{[Geography].[Geography].[Israel].[Israel].[Tel Aviv]}\n"
            + "{[Geography].[Geography].[Mexico].[DF].[Mexico City]}\n"
            + "{[Geography].[Geography].[Mexico].[DF].[San Andres]}\n"
            + "{[Geography].[Geography].[Mexico].[Guerrero].[Acapulco]}\n"
            + "{[Geography].[Geography].[Mexico].[Jalisco].[Guadalajara]}\n"
            + "{[Geography].[Geography].[Mexico].[Veracruz].[Orizaba]}\n"
            + "{[Geography].[Geography].[Mexico].[Yucatan].[Merida]}\n"
            + "{[Geography].[Geography].[Mexico].[Zacatecas].[Camacho]}\n"
            + "{[Geography].[Geography].[Mexico].[Zacatecas].[Hidalgo]}\n"
            + "{[Geography].[Geography].[USA].[USA].[Washington]}\n"
            + "{[Geography].[Geography].[USA].[CA].[Alameda]}\n"
            + "{[Geography].[Geography].[USA].[CA].[Beverly Hills]}\n"
            + "{[Geography].[Geography].[USA].[CA].[Los Angeles]}\n"
            + "{[Geography].[Geography].[USA].[CA].[San Francisco]}\n"
            + "{[Geography].[Geography].[USA].[OR].[Portland]}\n"
            + "{[Geography].[Geography].[USA].[OR].[Salem]}\n"
            + "{[Geography].[Geography].[USA].[WA].[Bellingham]}\n"
            + "{[Geography].[Geography].[USA].[WA].[Bremerton]}\n"
            + "{[Geography].[Geography].[USA].[WA].[Seattle]}\n"
            + "{[Geography].[Geography].[USA].[WA].[Spokane]}\n"
            + "Row #0: 0\n"
            + "Row #1: 0\n"
            + "Row #2: 0\n"
            + "Row #3: 0\n"
            + "Row #4: 0\n"
            + "Row #5: 0\n"
            + "Row #6: 0\n"
            + "Row #7: 0\n"
            + "Row #8: 0\n"
            + "Row #9: 0\n"
            + "Row #10: 0\n"
            + "Row #11: 0\n"
            + "Row #12: 0\n"
            + "Row #13: 0\n"
            + "Row #14: 0\n"
            + "Row #15: 0\n"
            + "Row #16: 0\n"
            + "Row #17: 0\n"
            + "Row #18: 0\n"
            + "Row #19: 0\n"
            + "Row #20: 0\n"
            + "Row #21: 0\n"
            + "Row #22: 0\n");
    }

    /**
     * Unit test for <a href="http://jira.pentaho.com/browse/MONDRIAN-642">
     * MONDRIAN-642, "Treat members that have only whitespace as blanks"</a>.
     */
    public void testHideIfBlankHidesWhitespace() {
        switch (getTestContext().getDialect().getDatabaseProduct()) {
        case MYSQL:
        case ORACLE:
            break;
        default:
            return;
        }
        final TestContext testContext =
            TestContext.instance().legacy().createSubstitutingCube(
                "Sales",
                "<Dimension name=\"Gender4\" foreignKey=\"customer_id\">\n"
                + "    <Hierarchy hasAll=\"true\" allMemberName=\"All Gender\" primaryKey=\"customer_id\">\n"
                + "      <Table name=\"customer\"/>\n"
                + "      <Level name=\"Gender\" column=\"gender\" uniqueMembers=\"true\" hideMemberIf=\"IfBlankName\">\n"
                + "         <NameExpression> "
                + " <SQL dialect='oracle'> "
                + "case \"gender\" "
                + "when 'F' then ' ' "
                + "when 'M' then 'M' "
                + " end "
                + "</SQL> "
                + " <SQL dialect='mysql'> "
                + "case `gender` "
                + "when 'F' then ' ' "
                + "when 'M' then 'M' "
                + " end "
                + "</SQL> "
                + "</NameExpression>  "
                + "      </Level>"
                + "    </Hierarchy>\n"
                + "  </Dimension>");
        testContext.assertQueryReturns(
            " select {[Gender4].[Gender].members} "
            + "on COLUMNS "
            + "from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Gender4].[Gender4].[M]}\n"
            + "Row #0: 135,215\n");
    }

    public void testNativeFilterWithHideMemberIfBlankOnLeaf() throws Exception {
        TestContext testContext =
            TestContext.instance().withSalesRagged()
                .remove("<Level attribute='Store Name' hideMemberIf='Never'/>");

        testContext.assertQueryReturns(
            "SELECT\n"
            + "[Measures].[Unit Sales] ON COLUMNS\n"
            + ",FILTER([Store].[Store City].MEMBERS, NOT ISEMPTY ([Measures].[Unit Sales])) ON ROWS\n"
            + "FROM [Sales Ragged]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Store].[Stores].[Israel].[Israel].[Haifa]}\n"
            + "{[Store].[Stores].[Israel].[Israel].[Tel Aviv]}\n"
            + "{[Store].[Stores].[USA].[CA].[Beverly Hills]}\n"
            + "{[Store].[Stores].[USA].[CA].[Los Angeles]}\n"
            + "{[Store].[Stores].[USA].[CA].[San Francisco]}\n"
            + "{[Store].[Stores].[USA].[OR].[Portland]}\n"
            + "{[Store].[Stores].[USA].[OR].[Salem]}\n"
            + "{[Store].[Stores].[USA].[USA].[Washington]}\n"
            + "{[Store].[Stores].[USA].[WA].[Bellingham]}\n"
            + "{[Store].[Stores].[USA].[WA].[Bremerton]}\n"
            + "{[Store].[Stores].[USA].[WA].[Seattle]}\n"
            + "{[Store].[Stores].[USA].[WA].[Spokane]}\n"
            + "Row #0: 2,203\n"
            + "Row #1: 11,491\n"
            + "Row #2: 21,333\n"
            + "Row #3: 25,663\n"
            + "Row #4: 2,117\n"
            + "Row #5: 26,079\n"
            + "Row #6: 41,580\n"
            + "Row #7: 25,635\n"
            + "Row #8: 2,237\n"
            + "Row #9: 24,576\n"
            + "Row #10: 25,011\n"
            + "Row #11: 23,591\n");
    }

    public void testNativeCJWithHideMemberIfBlankOnLeaf() throws Exception {
        TestContext testContext = TestContext.instance().withSalesRagged()
            .remove("<Level attribute='Store Name' hideMemberIf='Never'/>");

        testContext.assertQueryReturns(
            "SELECT\n"
            + "[Measures].[Unit Sales] ON COLUMNS\n"
            + ",non empty Crossjoin([Product].[Products].[Product Family].members, [Store].[Store City].MEMBERS) ON ROWS\n"
            + "FROM [Sales Ragged]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Products].[Drink], [Store].[Stores].[Israel].[Israel].[Haifa]}\n"
            + "{[Product].[Products].[Drink], [Store].[Stores].[Israel].[Israel].[Tel Aviv]}\n"
            + "{[Product].[Products].[Drink], [Store].[Stores].[USA].[CA].[Beverly Hills]}\n"
            + "{[Product].[Products].[Drink], [Store].[Stores].[USA].[CA].[Los Angeles]}\n"
            + "{[Product].[Products].[Drink], [Store].[Stores].[USA].[CA].[San Francisco]}\n"
            + "{[Product].[Products].[Drink], [Store].[Stores].[USA].[OR].[Portland]}\n"
            + "{[Product].[Products].[Drink], [Store].[Stores].[USA].[OR].[Salem]}\n"
            + "{[Product].[Products].[Drink], [Store].[Stores].[USA].[USA].[Washington]}\n"
            + "{[Product].[Products].[Drink], [Store].[Stores].[USA].[WA].[Bellingham]}\n"
            + "{[Product].[Products].[Drink], [Store].[Stores].[USA].[WA].[Bremerton]}\n"
            + "{[Product].[Products].[Drink], [Store].[Stores].[USA].[WA].[Seattle]}\n"
            + "{[Product].[Products].[Drink], [Store].[Stores].[USA].[WA].[Spokane]}\n"
            + "{[Product].[Products].[Food], [Store].[Stores].[Israel].[Israel].[Haifa]}\n"
            + "{[Product].[Products].[Food], [Store].[Stores].[Israel].[Israel].[Tel Aviv]}\n"
            + "{[Product].[Products].[Food], [Store].[Stores].[USA].[CA].[Beverly Hills]}\n"
            + "{[Product].[Products].[Food], [Store].[Stores].[USA].[CA].[Los Angeles]}\n"
            + "{[Product].[Products].[Food], [Store].[Stores].[USA].[CA].[San Francisco]}\n"
            + "{[Product].[Products].[Food], [Store].[Stores].[USA].[OR].[Portland]}\n"
            + "{[Product].[Products].[Food], [Store].[Stores].[USA].[OR].[Salem]}\n"
            + "{[Product].[Products].[Food], [Store].[Stores].[USA].[USA].[Washington]}\n"
            + "{[Product].[Products].[Food], [Store].[Stores].[USA].[WA].[Bellingham]}\n"
            + "{[Product].[Products].[Food], [Store].[Stores].[USA].[WA].[Bremerton]}\n"
            + "{[Product].[Products].[Food], [Store].[Stores].[USA].[WA].[Seattle]}\n"
            + "{[Product].[Products].[Food], [Store].[Stores].[USA].[WA].[Spokane]}\n"
            + "{[Product].[Products].[Non-Consumable], [Store].[Stores].[Israel].[Israel].[Haifa]}\n"
            + "{[Product].[Products].[Non-Consumable], [Store].[Stores].[Israel].[Israel].[Tel Aviv]}\n"
            + "{[Product].[Products].[Non-Consumable], [Store].[Stores].[USA].[CA].[Beverly Hills]}\n"
            + "{[Product].[Products].[Non-Consumable], [Store].[Stores].[USA].[CA].[Los Angeles]}\n"
            + "{[Product].[Products].[Non-Consumable], [Store].[Stores].[USA].[CA].[San Francisco]}\n"
            + "{[Product].[Products].[Non-Consumable], [Store].[Stores].[USA].[OR].[Portland]}\n"
            + "{[Product].[Products].[Non-Consumable], [Store].[Stores].[USA].[OR].[Salem]}\n"
            + "{[Product].[Products].[Non-Consumable], [Store].[Stores].[USA].[USA].[Washington]}\n"
            + "{[Product].[Products].[Non-Consumable], [Store].[Stores].[USA].[WA].[Bellingham]}\n"
            + "{[Product].[Products].[Non-Consumable], [Store].[Stores].[USA].[WA].[Bremerton]}\n"
            + "{[Product].[Products].[Non-Consumable], [Store].[Stores].[USA].[WA].[Seattle]}\n"
            + "{[Product].[Products].[Non-Consumable], [Store].[Stores].[USA].[WA].[Spokane]}\n"
            + "Row #0: 191\n"
            + "Row #1: 1,159\n"
            + "Row #2: 1,945\n"
            + "Row #3: 2,422\n"
            + "Row #4: 175\n"
            + "Row #5: 2,371\n"
            + "Row #6: 3,735\n"
            + "Row #7: 2,560\n"
            + "Row #8: 208\n"
            + "Row #9: 2,288\n"
            + "Row #10: 2,213\n"
            + "Row #11: 2,238\n"
            + "Row #12: 1,622\n"
            + "Row #13: 8,192\n"
            + "Row #14: 15,438\n"
            + "Row #15: 18,294\n"
            + "Row #16: 1,555\n"
            + "Row #17: 18,632\n"
            + "Row #18: 29,905\n"
            + "Row #19: 18,369\n"
            + "Row #20: 1,587\n"
            + "Row #21: 17,809\n"
            + "Row #22: 18,159\n"
            + "Row #23: 16,925\n"
            + "Row #24: 390\n"
            + "Row #25: 2,140\n"
            + "Row #26: 3,950\n"
            + "Row #27: 4,947\n"
            + "Row #28: 387\n"
            + "Row #29: 5,076\n"
            + "Row #30: 7,940\n"
            + "Row #31: 4,706\n"
            + "Row #32: 442\n"
            + "Row #33: 4,479\n"
            + "Row #34: 4,639\n"
            + "Row #35: 4,428\n");
    }
}

// End RaggedHierarchyTest.java
