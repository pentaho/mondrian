/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2003-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Feb 14, 2003
*/
package mondrian.test;

/**
 * <code>ClearViewTest</code> is a test case which tests complex
 * queries against the FoodMart database.
 *
 * @author Richard Emberson
 * @since Jan 25, 2007
 * @version $Id$
 */
public class ClearViewTest extends FoodMartTestCase {
    static final String EmptyResult = fold(
        "Axis #0:\n" +
            "{}\n" +
            "Axis #1:\n" +
            "Axis #2:\n");

    public ClearViewTest() {
        super();
    }
    public ClearViewTest(String name) {
        super(name);
    }

    /**
     * Complex query involving Head(Crossjoin(...)), which was the testcase for
     * bug 1642828.
     */
    public void testHeadCrossjoinComplex() {
        assertQueryReturns(
            "with set [*NATIVE_CJ_SET] as \n" +
                "'NonEmptyCrossJoin([*BASE_MEMBERS_Product], [*BASE_MEMBERS_Education Level])' \n" +
                "set [*GENERATED_MEMBERS_Measures] as \n" +
                "'{[Measures].[Unit Sales], [Measures].[Store Cost], [Measures].[*SUMMARY_METRIC_0]}' \n" +
                "set [*BASE_MEMBERS_Product] as  \n" +
                "'[Product].[Product Family].Members' \n" +
                "set [*GENERATED_MEMBERS_Product] as  \n" +
                "'Generate([*NATIVE_CJ_SET], {[Product].CurrentMember})' \n" +
                "set [*BASE_MEMBERS_Education Level] as \n" +
                "'[Education Level].[Education Level].Members' \n" +
                "set [*GENERATED_MEMBERS_Education Level] as \n" +
                "'Generate([*NATIVE_CJ_SET], {[Education Level].CurrentMember})' \n" +
                "member [Measures].[*SUMMARY_METRIC_0] as \n" +
                "'Sum(Head(Crossjoin({[Product].CurrentMember}, [*GENERATED_MEMBERS_Education Level]), Rank(([Product].CurrentMember, [Education Level].CurrentMember), Crossjoin({[Product].CurrentMember}, [*GENERATED_MEMBERS_Education Level]))), [Measures].[Unit Sales])' \n" +
                "member [Education Level].[*SUBTOTAL_MEMBER_SEL~SUM] as \n" +
                "'Sum([*GENERATED_MEMBERS_Education Level])' \n" +
                "member [Product].[*SUBTOTAL_MEMBER_SEL~SUM] as \n" +
                "'Sum([*GENERATED_MEMBERS_Product])' \n" +
                "select [*GENERATED_MEMBERS_Measures] ON COLUMNS, \n" +
                "NON EMPTY Union(NonEmptyCrossJoin({[Product].[*SUBTOTAL_MEMBER_SEL~SUM]}, \n" +
                "{[Education Level].[*SUBTOTAL_MEMBER_SEL~SUM]}), \n" +
                "Union(NonEmptyCrossJoin([*GENERATED_MEMBERS_Product], \n" +
                "{[Education Level].[*SUBTOTAL_MEMBER_SEL~SUM]}), \n" +
                "Generate([*NATIVE_CJ_SET], \n" +
                "{([Product].CurrentMember, [Education Level].CurrentMember)}))) ON ROWS \n" +
                "from [Sales]\n",
            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Unit Sales]}\n" +
                "{[Measures].[Store Cost]}\n" +
                "{[Measures].[*SUMMARY_METRIC_0]}\n" +
                "Axis #2:\n" +
                "{[Product].[*SUBTOTAL_MEMBER_SEL~SUM], [Education Level].[*SUBTOTAL_MEMBER_SEL~SUM]}\n" +
                "{[Product].[All Products].[Drink], [Education Level].[*SUBTOTAL_MEMBER_SEL~SUM]}\n" +
                "{[Product].[All Products].[Food], [Education Level].[*SUBTOTAL_MEMBER_SEL~SUM]}\n" +
                "{[Product].[All Products].[Non-Consumable], [Education Level].[*SUBTOTAL_MEMBER_SEL~SUM]}\n" +
                "{[Product].[All Products].[Drink], [Education Level].[All Education Levels].[Bachelors Degree]}\n" +
                "{[Product].[All Products].[Drink], [Education Level].[All Education Levels].[Graduate Degree]}\n" +
                "{[Product].[All Products].[Drink], [Education Level].[All Education Levels].[High School Degree]}\n" +
                "{[Product].[All Products].[Drink], [Education Level].[All Education Levels].[Partial College]}\n" +
                "{[Product].[All Products].[Drink], [Education Level].[All Education Levels].[Partial High School]}\n" +
                "{[Product].[All Products].[Food], [Education Level].[All Education Levels].[Bachelors Degree]}\n" +
                "{[Product].[All Products].[Food], [Education Level].[All Education Levels].[Graduate Degree]}\n" +
                "{[Product].[All Products].[Food], [Education Level].[All Education Levels].[High School Degree]}\n" +
                "{[Product].[All Products].[Food], [Education Level].[All Education Levels].[Partial College]}\n" +
                "{[Product].[All Products].[Food], [Education Level].[All Education Levels].[Partial High School]}\n" +
                "{[Product].[All Products].[Non-Consumable], [Education Level].[All Education Levels].[Bachelors Degree]}\n" +
                "{[Product].[All Products].[Non-Consumable], [Education Level].[All Education Levels].[Graduate Degree]}\n" +
                "{[Product].[All Products].[Non-Consumable], [Education Level].[All Education Levels].[High School Degree]}\n" +
                "{[Product].[All Products].[Non-Consumable], [Education Level].[All Education Levels].[Partial College]}\n" +
                "{[Product].[All Products].[Non-Consumable], [Education Level].[All Education Levels].[Partial High School]}\n" +
                "Row #0: 266,773\n" +
                "Row #0: 225,627.23\n" +
                "Row #0: \n" +
                "Row #1: 24,597\n" +
                "Row #1: 19,477.23\n" +
                "Row #1: \n" +
                "Row #2: 191,940\n" +
                "Row #2: 163,270.72\n" +
                "Row #2: \n" +
                "Row #3: 50,236\n" +
                "Row #3: 42,879.28\n" +
                "Row #3: \n" +
                "Row #4: 6,423\n" +
                "Row #4: 5,003.03\n" +
                "Row #4: 6,423\n" +
                "Row #5: 1,325\n" +
                "Row #5: 1,090.96\n" +
                "Row #5: 7,748\n" +
                "Row #6: 7,226\n" +
                "Row #6: 5,744.83\n" +
                "Row #6: 14,974\n" +
                "Row #7: 2,164\n" +
                "Row #7: 1,760.69\n" +
                "Row #7: 17,138\n" +
                "Row #8: 7,459\n" +
                "Row #8: 5,877.72\n" +
                "Row #8: 24,597\n" +
                "Row #9: 49,365\n" +
                "Row #9: 41,895.43\n" +
                "Row #9: 49,365\n" +
                "Row #10: 11,255\n" +
                "Row #10: 9,508.55\n" +
                "Row #10: 60,620\n" +
                "Row #11: 56,509\n" +
                "Row #11: 48,233.90\n" +
                "Row #11: 117,129\n" +
                "Row #12: 17,859\n" +
                "Row #12: 15,290.15\n" +
                "Row #12: 134,988\n" +
                "Row #13: 56,952\n" +
                "Row #13: 48,342.70\n" +
                "Row #13: 191,940\n" +
                "Row #14: 13,051\n" +
                "Row #14: 11,154.60\n" +
                "Row #14: 13,051\n" +
                "Row #15: 2,990\n" +
                "Row #15: 2,560.96\n" +
                "Row #15: 16,041\n" +
                "Row #16: 14,929\n" +
                "Row #16: 12,662.10\n" +
                "Row #16: 30,970\n" +
                "Row #17: 4,522\n" +
                "Row #17: 3,863.13\n" +
                "Row #17: 35,492\n" +
                "Row #18: 14,744\n" +
                "Row #18: 12,638.49\n" +
                "Row #18: 50,236\n"));
    }


    /**
     * Simplified version of {@link #testHeadCrossjoinComplex()}.
     */
    public void testHeadCrossjoin() {
        assertAxisReturns(
            "Head(Crossjoin([Gender].Members, [Marital Status].Members), 4)",
            fold("{[Gender].[All Gender], [Marital Status].[All Marital Status]}\n" +
                "{[Gender].[All Gender], [Marital Status].[All Marital Status].[M]}\n" +
                "{[Gender].[All Gender], [Marital Status].[All Marital Status].[S]}\n" +
                "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status]}"));
    }

    /**
     * Tests a bug with incorrect reuse of a named set which can't be
     * computed directly against a cold cache.
     */
    public void testLer4260() {
        assertQueryReturns(
            "With Set [*BMEL] as \n" +
            "'[Education Level].[Education Level].Members' \n" +
            "Member [Measures].[*TBM] as \n" +
            "'Rank([Education Level].CurrentMember, \n" +
            "Order([*BMEL],([Measures].[Unit Sales]),BDESC))' \n" +
            "Set [*SM_RSUM_SET_0] as \n" +
            "'Filter([*BMEL],[Measures].[*TBM] <= 3)'\n" +
            "select [*SM_RSUM_SET_0] on rows, \n" +
            "{[Measures].[*TBM], [Measures].[Unit Sales]} on columns \n" +
            "From [Sales]",
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[*TBM]}\n" +
                "{[Measures].[Unit Sales]}\n" +
                "Axis #2:\n" +
                "{[Education Level].[All Education Levels].[Bachelors Degree]}\n" +
                "{[Education Level].[All Education Levels].[High School Degree]}\n" +
                "{[Education Level].[All Education Levels].[Partial High School]}\n" +
                "Row #0: 3\n" +
                "Row #0: 68,839\n" +
                "Row #1: 2\n" +
                "Row #1: 78,664\n" +
                "Row #2: 1\n" +
                "Row #2: 79,155\n"));
    }
}

// End ClearViewTest.java
