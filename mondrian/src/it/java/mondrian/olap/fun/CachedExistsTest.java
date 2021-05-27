/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2002-2021 Hitachi Vantara.  All rights reserved.
*/
package mondrian.olap.fun;

import mondrian.test.FoodMartTestCase;
import mondrian.test.TestContext;

/**
 * Tests the CachedExists function.
 *
 * @author Benny Chow
 */
public class CachedExistsTest extends FoodMartTestCase {

  public CachedExistsTest() {
    super();
  }

  public CachedExistsTest( String name ) {
    super( name );
  }

  public void testEducationLevelSubtotals() {
    String query =
        "WITH "
            + "SET [*NATIVE_CJ_SET] AS 'NONEMPTYCROSSJOIN([*BASE_MEMBERS__Education Level_],[*BASE_MEMBERS__Product_])' "
            + "SET [*SORTED_ROW_AXIS] AS 'ORDER([*CJ_ROW_AXIS],[Education Level].CURRENTMEMBER.ORDERKEY,BASC,[Measures].[*SORTED_MEASURE],BDESC)' "
            + "SET [*BASE_MEMBERS__Education Level_] AS '{[Education Level].[All Education Levels].[Bachelors Degree],[Education Level].[All Education Levels].[Graduate Degree]}' "
            + "SET [*BASE_MEMBERS__Measures_] AS '{[Measures].[*FORMATTED_MEASURE_0]}' "
            + "SET [*BASE_MEMBERS__Product_] AS '{[Product].[All Products].[Drink],[Product].[All Products].[Food]}' "
            + "SET [*CJ_ROW_AXIS] AS 'GENERATE([*NATIVE_CJ_SET], {([Education Level].CURRENTMEMBER,[Product].CURRENTMEMBER)})' "
            + "MEMBER [Measures].[*FORMATTED_MEASURE_0] AS '[Measures].[Unit Sales]', FORMAT_STRING = 'Standard', SOLVE_ORDER=500 "
            + "MEMBER [Measures].[*SORTED_MEASURE] AS '([Measures].[*FORMATTED_MEASURE_0])', SOLVE_ORDER=400 "
            + "MEMBER [Product].[*TOTAL_MEMBER_SEL~SUM] AS 'SUM(CachedExists([*CJ_ROW_AXIS], ([Education Level].CURRENTMEMBER), \"*CJ_ROW_AXIS\"))', SOLVE_ORDER=99 "
            + "SELECT " + "[*BASE_MEMBERS__Measures_] ON COLUMNS " + ", NON EMPTY "
            + "UNION(CROSSJOIN(GENERATE([*CJ_ROW_AXIS], {([Education Level].CURRENTMEMBER)}),{[Product].[*TOTAL_MEMBER_SEL~SUM]}),[*SORTED_ROW_AXIS]) ON ROWS "
            + "FROM [Sales]";
    String expected =
        "Axis #0:\n" + "{}\n" + "Axis #1:\n" + "{[Measures].[*FORMATTED_MEASURE_0]}\n" + "Axis #2:\n"
            + "{[Education Level].[Bachelors Degree], [Product].[*TOTAL_MEMBER_SEL~SUM]}\n"
            + "{[Education Level].[Graduate Degree], [Product].[*TOTAL_MEMBER_SEL~SUM]}\n"
            + "{[Education Level].[Bachelors Degree], [Product].[Food]}\n"
            + "{[Education Level].[Bachelors Degree], [Product].[Drink]}\n"
            + "{[Education Level].[Graduate Degree], [Product].[Food]}\n"
            + "{[Education Level].[Graduate Degree], [Product].[Drink]}\n" + "Row #0: 55,788\n" + "Row #1: 12,580\n"
            + "Row #2: 49,365\n" + "Row #3: 6,423\n" + "Row #4: 11,255\n" + "Row #5: 1,325\n";
    assertQueryReturns( query, expected );
  }

  public void testProductFamilySubtotals() {
    String query =
        "WITH\r\n"
            + "SET [*NATIVE_CJ_SET] AS 'FILTER(FILTER([Product].[Product Department].MEMBERS,ANCESTOR([Product].CURRENTMEMBER, [Product].[Product Family]) IN {[Product].[All Products].[Drink],[Product].[All Products].[Non-Consumable]}), NOT ISEMPTY ([Measures].[Unit Sales]))'\r\n"
            + "SET [*SORTED_ROW_AXIS] AS 'ORDER([*CJ_ROW_AXIS],ANCESTOR([Product].CURRENTMEMBER, [Product].[Product Family]).ORDERKEY,BASC,[Product].CURRENTMEMBER.ORDERKEY,BASC)'\r\n"
            + "SET [*BASE_MEMBERS__Measures_] AS '{[Measures].[*FORMATTED_MEASURE_0]}'\r\n"
            + "SET [*BASE_MEMBERS__Product_] AS 'FILTER([Product].[Product Department].MEMBERS,ANCESTOR([Product].CURRENTMEMBER, [Product].[Product Family]) IN {[Product].[All Products].[Drink],[Product].[All Products].[Non-Consumable]})'\r\n"
            + "SET [*CJ_ROW_AXIS] AS 'GENERATE([*NATIVE_CJ_SET], {([Product].CURRENTMEMBER)})'\r\n"
            + "MEMBER [Measures].[*FORMATTED_MEASURE_0] AS '[Measures].[Unit Sales]', FORMAT_STRING = 'Standard', SOLVE_ORDER=500\r\n"
            + "MEMBER [Product].[All Products].[Drink].[*TOTAL_MEMBER_SEL~SUM] AS 'SUM(CachedExists([*CJ_ROW_AXIS], ([Product].[All Products].[Drink]), \"*CJ_ROW_AXIS\"))', SOLVE_ORDER=100\r\n"
            + "MEMBER [Product].[All Products].[Non-Consumable].[*TOTAL_MEMBER_SEL~SUM] AS 'SUM(CachedExists([*CJ_ROW_AXIS], ([Product].[All Products].[Non-Consumable]), \"*CJ_ROW_AXIS\"))', SOLVE_ORDER=100\r\n"
            + "SELECT\r\n" + "[*BASE_MEMBERS__Measures_] ON COLUMNS\r\n" + ", NON EMPTY\r\n"
            + "UNION({[Product].[All Products].[Drink].[*TOTAL_MEMBER_SEL~SUM], [Product].[All Products].[Non-Consumable].[*TOTAL_MEMBER_SEL~SUM]},[*SORTED_ROW_AXIS]) ON ROWS\r\n"
            + "FROM [Sales]";
    String expected =
        "Axis #0:\n" + "{}\n" + "Axis #1:\n" + "{[Measures].[*FORMATTED_MEASURE_0]}\n" + "Axis #2:\n"
            + "{[Product].[Drink].[*TOTAL_MEMBER_SEL~SUM]}\n"
            + "{[Product].[Non-Consumable].[*TOTAL_MEMBER_SEL~SUM]}\n" + "{[Product].[Drink].[Alcoholic Beverages]}\n"
            + "{[Product].[Drink].[Beverages]}\n" + "{[Product].[Drink].[Dairy]}\n"
            + "{[Product].[Non-Consumable].[Carousel]}\n" + "{[Product].[Non-Consumable].[Checkout]}\n"
            + "{[Product].[Non-Consumable].[Health and Hygiene]}\n" + "{[Product].[Non-Consumable].[Household]}\n"
            + "{[Product].[Non-Consumable].[Periodicals]}\n" + "Row #0: 24,597\n" + "Row #1: 50,236\n"
            + "Row #2: 6,838\n" + "Row #3: 13,573\n" + "Row #4: 4,186\n" + "Row #5: 841\n" + "Row #6: 1,779\n"
            + "Row #7: 16,284\n" + "Row #8: 27,038\n" + "Row #9: 4,294\n";
    assertQueryReturns( query, expected );
  }

  public void testProductFamilyProductDepartmentSubtotals() {
    String query =
        "WITH\r\n"
            + "SET [*NATIVE_CJ_SET] AS 'NONEMPTYCROSSJOIN([*BASE_MEMBERS__Product_],[*BASE_MEMBERS__Gender_])'\r\n"
            + "SET [*SORTED_ROW_AXIS] AS 'ORDER([*CJ_ROW_AXIS],ANCESTOR([Product].CURRENTMEMBER, [Product].[Product Family]).ORDERKEY,BASC,[Product].CURRENTMEMBER.ORDERKEY,BASC,[Gender].CURRENTMEMBER.ORDERKEY,BASC)'\r\n"
            + "SET [*BASE_MEMBERS__Measures_] AS '{[Measures].[*FORMATTED_MEASURE_0]}'\r\n"
            + "SET [*BASE_MEMBERS__Gender_] AS '[Gender].[Gender].MEMBERS'\r\n"
            + "SET [*BASE_MEMBERS__Product_] AS 'FILTER([Product].[Product Department].MEMBERS,(ANCESTOR([Product].CURRENTMEMBER, [Product].[Product Family]) IN {[Product].[All Products].[Drink],[Product].[All Products].[Non-Consumable]}) AND ([Product].CURRENTMEMBER IN {[Product].[All Products].[Drink].[Beverages],[Product].[All Products].[Drink].[Dairy],[Product].[All Products].[Non-Consumable].[Periodicals]}))'\r\n"
            + "SET [*CJ_ROW_AXIS] AS 'GENERATE([*NATIVE_CJ_SET], {([Product].CURRENTMEMBER,[Gender].CURRENTMEMBER)})'\r\n"
            + "MEMBER [Gender].[*DEFAULT_MEMBER] AS '[Gender].DEFAULTMEMBER', SOLVE_ORDER=-400\r\n"
            + "MEMBER [Gender].[*TOTAL_MEMBER_SEL~SUM] AS 'SUM(CachedExists([*CJ_ROW_AXIS], ([Product].CURRENTMEMBER), \"*CJ_ROW_AXIS\"))', SOLVE_ORDER=99\r\n"
            + "MEMBER [Measures].[*FORMATTED_MEASURE_0] AS '[Measures].[Unit Sales]', FORMAT_STRING = 'Standard', SOLVE_ORDER=500\r\n"
            + "MEMBER [Product].[All Products].[Drink].[*TOTAL_MEMBER_SEL~SUM] AS 'SUM(CachedExists([*CJ_ROW_AXIS], ([Product].[All Products].[Drink]), \"*CJ_ROW_AXIS\"))', SOLVE_ORDER=100\r\n"
            + "MEMBER [Product].[All Products].[Non-Consumable].[*TOTAL_MEMBER_SEL~SUM] AS 'SUM(CachedExists([*CJ_ROW_AXIS], ([Product].[All Products].[Non-Consumable]), \"*CJ_ROW_AXIS\"))', SOLVE_ORDER=100\r\n"
            + "SELECT\r\n" + "[*BASE_MEMBERS__Measures_] ON COLUMNS\r\n" + ", NON EMPTY\r\n"
            + "UNION(CROSSJOIN(GENERATE([*CJ_ROW_AXIS], {([Product].CURRENTMEMBER)}),{[Gender].[*TOTAL_MEMBER_SEL~SUM]}),UNION(CROSSJOIN({[Product].[All Products].[Drink].[*TOTAL_MEMBER_SEL~SUM], [Product].[All Products].[Non-Consumable].[*TOTAL_MEMBER_SEL~SUM]},{([Gender].[*DEFAULT_MEMBER])}),[*SORTED_ROW_AXIS])) ON ROWS\r\n"
            + "FROM [Sales]";
    String expected =
        "Axis #0:\n" + "{}\n" + "Axis #1:\n" + "{[Measures].[*FORMATTED_MEASURE_0]}\n" + "Axis #2:\n"
            + "{[Product].[Drink].[Beverages], [Gender].[*TOTAL_MEMBER_SEL~SUM]}\n"
            + "{[Product].[Drink].[Dairy], [Gender].[*TOTAL_MEMBER_SEL~SUM]}\n"
            + "{[Product].[Non-Consumable].[Periodicals], [Gender].[*TOTAL_MEMBER_SEL~SUM]}\n"
            + "{[Product].[Drink].[*TOTAL_MEMBER_SEL~SUM], [Gender].[*DEFAULT_MEMBER]}\n"
            + "{[Product].[Non-Consumable].[*TOTAL_MEMBER_SEL~SUM], [Gender].[*DEFAULT_MEMBER]}\n"
            + "{[Product].[Drink].[Beverages], [Gender].[F]}\n" + "{[Product].[Drink].[Beverages], [Gender].[M]}\n"
            + "{[Product].[Drink].[Dairy], [Gender].[F]}\n" + "{[Product].[Drink].[Dairy], [Gender].[M]}\n"
            + "{[Product].[Non-Consumable].[Periodicals], [Gender].[F]}\n"
            + "{[Product].[Non-Consumable].[Periodicals], [Gender].[M]}\n" + "Row #0: 13,573\n" + "Row #1: 4,186\n"
            + "Row #2: 4,294\n" + "Row #3: 17,759\n" + "Row #4: 4,294\n" + "Row #5: 6,776\n" + "Row #6: 6,797\n"
            + "Row #7: 1,987\n" + "Row #8: 2,199\n" + "Row #9: 2,168\n" + "Row #10: 2,126\n";
    assertQueryReturns( query, expected );
  }

  public void testRowColumSubtotals() {
    String query =
        "WITH\r\n"
            + "SET [*NATIVE_CJ_SET] AS 'NONEMPTYCROSSJOIN([*BASE_MEMBERS__Time_],NONEMPTYCROSSJOIN([*BASE_MEMBERS__Product_],[*BASE_MEMBERS__Gender_]))'\r\n"
            + "SET [*SORTED_ROW_AXIS] AS 'ORDER([*CJ_ROW_AXIS],[Product].CURRENTMEMBER.ORDERKEY,BASC,[Gender].CURRENTMEMBER.ORDERKEY,BASC)'\r\n"
            + "SET [*SORTED_COL_AXIS] AS 'ORDER([*CJ_COL_AXIS],ANCESTOR([Time].CURRENTMEMBER, [Time].[Year]).ORDERKEY,BASC,[Time].CURRENTMEMBER.ORDERKEY,BASC,[Measures].CURRENTMEMBER.ORDERKEY,BASC)'\r\n"
            + "SET [*BASE_MEMBERS__Measures_] AS '{[Measures].[*FORMATTED_MEASURE_0]}'\r\n"
            + "SET [*BASE_MEMBERS__Gender_] AS '[Gender].[Gender].MEMBERS'\r\n"
            + "SET [*CJ_COL_AXIS] AS 'GENERATE([*NATIVE_CJ_SET], {([Time].CURRENTMEMBER)})'\r\n"
            + "SET [*BASE_MEMBERS__Product_] AS '{[Product].[All Products].[Drink],[Product].[All Products].[Non-Consumable]}'\r\n"
            + "SET [*BASE_MEMBERS__Time_] AS '{[Time].[1997].[Q1],[Time].[1997].[Q2]}'\r\n"
            + "SET [*CJ_ROW_AXIS] AS 'GENERATE([*NATIVE_CJ_SET], {([Product].CURRENTMEMBER,[Gender].CURRENTMEMBER)})'\r\n"
            + "MEMBER [Gender].[*TOTAL_MEMBER_SEL~SUM] AS 'SUM(CachedExists([*CJ_ROW_AXIS], ([Product].CURRENTMEMBER), \"*CJ_ROW_AXIS\"))', SOLVE_ORDER=99\r\n"
            + "MEMBER [Measures].[*FORMATTED_MEASURE_0] AS '[Measures].[Unit Sales]', FORMAT_STRING = 'Standard', SOLVE_ORDER=500\r\n"
            + "MEMBER [Time].[1997].[*TOTAL_MEMBER_SEL~SUM] AS 'SUM(CachedExists([*CJ_COL_AXIS], ([Time].[1997]), \"*CJ_COL_AXIS\"))', SOLVE_ORDER=98\r\n"
            + "SELECT\r\n"
            + "UNION(CROSSJOIN({[Time].[1997].[*TOTAL_MEMBER_SEL~SUM]},[*BASE_MEMBERS__Measures_]),CROSSJOIN([*SORTED_COL_AXIS],[*BASE_MEMBERS__Measures_])) ON COLUMNS\r\n"
            + ", NON EMPTY\r\n"
            + "UNION(CROSSJOIN(GENERATE([*CJ_ROW_AXIS], {([Product].CURRENTMEMBER)}),{[Gender].[*TOTAL_MEMBER_SEL~SUM]}),[*SORTED_ROW_AXIS]) ON ROWS\r\n"
            + "FROM [Sales]";
    String expected =
        "Axis #0:\n" + "{}\n" + "Axis #1:\n"
            + "{[Time].[1997].[*TOTAL_MEMBER_SEL~SUM], [Measures].[*FORMATTED_MEASURE_0]}\n"
            + "{[Time].[1997].[Q1], [Measures].[*FORMATTED_MEASURE_0]}\n"
            + "{[Time].[1997].[Q2], [Measures].[*FORMATTED_MEASURE_0]}\n" + "Axis #2:\n"
            + "{[Product].[Drink], [Gender].[*TOTAL_MEMBER_SEL~SUM]}\n"
            + "{[Product].[Non-Consumable], [Gender].[*TOTAL_MEMBER_SEL~SUM]}\n"
            + "{[Product].[Drink], [Gender].[F]}\n" + "{[Product].[Drink], [Gender].[M]}\n"
            + "{[Product].[Non-Consumable], [Gender].[F]}\n" + "{[Product].[Non-Consumable], [Gender].[M]}\n"
            + "Row #0: 11,871\n" + "Row #0: 5,976\n" + "Row #0: 5,895\n" + "Row #1: 24,396\n" + "Row #1: 12,506\n"
            + "Row #1: 11,890\n" + "Row #2: 5,806\n" + "Row #2: 2,934\n" + "Row #2: 2,872\n" + "Row #3: 6,065\n"
            + "Row #3: 3,042\n" + "Row #3: 3,023\n" + "Row #4: 11,997\n" + "Row #4: 6,144\n" + "Row #4: 5,853\n"
            + "Row #5: 12,399\n" + "Row #5: 6,362\n" + "Row #5: 6,037\n";
    assertQueryReturns( query, expected );
  }

  public void testProductFamilyDisplayMember() {
    String query =
        "WITH\r\n" + 
        "SET [*NATIVE_CJ_SET] AS 'NONEMPTYCROSSJOIN([*BASE_MEMBERS__Product_],[*BASE_MEMBERS__Gender_])'\r\n" + 
        "SET [*SORTED_ROW_AXIS] AS 'ORDER([*CJ_ROW_AXIS],ANCESTOR([Product].CURRENTMEMBER, [Product].[Product Family]).ORDERKEY,BASC,[Gender].CURRENTMEMBER.ORDERKEY,BASC)'\r\n" + 
        "SET [*NATIVE_MEMBERS__Product_] AS 'GENERATE([*NATIVE_CJ_SET], {[Product].CURRENTMEMBER})'\r\n" + 
        "SET [*BASE_MEMBERS__Measures_] AS '{[Measures].[*FORMATTED_MEASURE_0]}'\r\n" + 
        "SET [*BASE_MEMBERS__Gender_] AS '[Gender].[Gender].MEMBERS'\r\n" + 
        "SET [*BASE_MEMBERS__Product_] AS 'FILTER([Product].[Product Category].MEMBERS,(ANCESTOR([Product].CURRENTMEMBER, [Product].[Product Family]) IN {[Product].[All Products].[Drink],[Product].[All Products].[Non-Consumable]}) AND ([Product].CURRENTMEMBER IN {[Product].[All Products].[Non-Consumable].[Household].[Candles],[Product].[All Products].[Drink].[Dairy].[Dairy],[Product].[All Products].[Non-Consumable].[Periodicals].[Magazines],[Product].[All Products].[Drink].[Beverages].[Pure Juice Beverages]}))'\r\n" + 
        "SET [*CJ_ROW_AXIS] AS 'GENERATE([*NATIVE_CJ_SET], {(ANCESTOR([Product].CURRENTMEMBER, [Product].[Product Family]).CALCULATEDCHILD(\"*DISPLAY_MEMBER\"),[Gender].CURRENTMEMBER)})'\r\n" + 
        "MEMBER [Gender].[*TOTAL_MEMBER_SEL~SUM] AS 'SUM(CachedExists([*CJ_ROW_AXIS], ([Product].CURRENTMEMBER), \"*CJ_ROW_AXIS\" ))', SOLVE_ORDER=99\r\n" + 
        "MEMBER [Measures].[*FORMATTED_MEASURE_0] AS '[Measures].[Unit Sales]', FORMAT_STRING = 'Standard', SOLVE_ORDER=500\r\n" + 
        "MEMBER [Product].[Drink].[*DISPLAY_MEMBER] AS 'AGGREGATE (FILTER([*NATIVE_MEMBERS__Product_],ANCESTOR([Product].CURRENTMEMBER, [Product].[Product Family]) IS [Product].[All Products].[Drink]))', SOLVE_ORDER=-100\r\n" + 
        "MEMBER [Product].[Non-Consumable].[*DISPLAY_MEMBER] AS 'AGGREGATE (FILTER([*NATIVE_MEMBERS__Product_],ANCESTOR([Product].CURRENTMEMBER, [Product].[Product Family]) IS [Product].[All Products].[Non-Consumable]))', SOLVE_ORDER=-100\r\n" + 
        "SELECT\r\n" + 
        "[*BASE_MEMBERS__Measures_] ON COLUMNS\r\n" + 
        ", NON EMPTY\r\n" + 
        "UNION(CROSSJOIN(GENERATE([*CJ_ROW_AXIS], {(ANCESTOR([Product].CURRENTMEMBER, [Product].[Product Family]))}),{[Gender].[*TOTAL_MEMBER_SEL~SUM]}),[*SORTED_ROW_AXIS]) ON ROWS\r\n" + 
        "FROM [Sales]";
    String expected =
        "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[*FORMATTED_MEASURE_0]}\n"
            + "Axis #2:\n"
            + "{[Product].[Drink], [Gender].[*TOTAL_MEMBER_SEL~SUM]}\n"
            + "{[Product].[Non-Consumable], [Gender].[*TOTAL_MEMBER_SEL~SUM]}\n"
            + "{[Product].[Drink].[*DISPLAY_MEMBER], [Gender].[F]}\n"
            + "{[Product].[Drink].[*DISPLAY_MEMBER], [Gender].[M]}\n"
            + "{[Product].[Non-Consumable].[*DISPLAY_MEMBER], [Gender].[F]}\n"
            + "{[Product].[Non-Consumable].[*DISPLAY_MEMBER], [Gender].[M]}\n"
            + "Row #0: 7,582\n"
            + "Row #1: 5,109\n"
            + "Row #2: 3,690\n"
            + "Row #3: 3,892\n"
            + "Row #4: 2,607\n"
            + "Row #5: 2,502\n";
    assertQueryReturns( query, expected );
  }
  
  public void testTop10Customers() {
    String query =
        "WITH\r\n" + 
        "SET [*NATIVE_CJ_SET] AS 'NONEMPTYCROSSJOIN([*BASE_MEMBERS__Customers_],NONEMPTYCROSSJOIN([*BASE_MEMBERS__Product_],[*BASE_MEMBERS__Store_]))'\r\n" + 
        "SET [*METRIC_CJ_SET] AS 'FILTER([*NATIVE_CJ_SET],[Measures].[*TOP_Unit Sales_SEL~SUM] <= 10)'\r\n" + 
        "SET [*SORTED_ROW_AXIS] AS 'ORDER([*CJ_ROW_AXIS],[Customers].CURRENTMEMBER.ORDERKEY,BASC,ANCESTOR([Customers].CURRENTMEMBER,[Customers].[City]).ORDERKEY,BASC,[Product].CURRENTMEMBER.ORDERKEY,BASC,[Measures].[*SORTED_MEASURE],BDESC)'\r\n" + 
        "SET [*BASE_MEMBERS__Measures_] AS '{[Measures].[*FORMATTED_MEASURE_0]}'\r\n" + 
        "SET [*BASE_MEMBERS__Store_] AS '[Store].[Store Country].MEMBERS'\r\n" + 
        "SET [*BASE_MEMBERS__Customers_] AS '[Customers].[Name].MEMBERS'\r\n" + 
        "SET [*TOP_SET] AS 'ORDER(GENERATE([*NATIVE_CJ_SET],{[Customers].CURRENTMEMBER}),([Measures].[Unit Sales],[Education Level].[*TOPBOTTOM_CTX_SET_SUM]),BDESC)'\r\n" + 
        "SET [*BASE_MEMBERS__Product_] AS '{[Product].[All Products].[Drink],[Product].[All Products].[Food]}'\r\n" + 
        "SET [*CJ_ROW_AXIS] AS 'GENERATE([*METRIC_CJ_SET], {([Customers].CURRENTMEMBER,[Product].CURRENTMEMBER,[Store].CURRENTMEMBER)})'\r\n" + 
        "MEMBER [Education Level].[*TOPBOTTOM_CTX_SET_SUM] AS 'SUM(CachedExists([*NATIVE_CJ_SET],([Customers].CURRENTMEMBER), \"*NATIVE_CJ_SET\"))', SOLVE_ORDER=100\r\n" + 
        "MEMBER [Measures].[*FORMATTED_MEASURE_0] AS '[Measures].[Unit Sales]', FORMAT_STRING = 'Standard', SOLVE_ORDER=500\r\n" + 
        "MEMBER [Measures].[*SORTED_MEASURE] AS '([Measures].[*FORMATTED_MEASURE_0])', SOLVE_ORDER=400\r\n" + 
        "MEMBER [Measures].[*TOP_Unit Sales_SEL~SUM] AS 'RANK([Customers].CURRENTMEMBER,[*TOP_SET])', SOLVE_ORDER=400\r\n" + 
        "SELECT\r\n" + 
        "[*BASE_MEMBERS__Measures_] ON COLUMNS\r\n" + 
        ", NON EMPTY\r\n" + 
        "[*SORTED_ROW_AXIS] ON ROWS\r\n" + 
        "FROM [Sales]";
    String expected =
        "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[*FORMATTED_MEASURE_0]}\n"
            + "Axis #2:\n"
            + "{[Customers].[USA].[WA].[Spokane].[Joann Mramor], [Product].[Drink], [Store].[USA]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[Joann Mramor], [Product].[Food], [Store].[USA]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[Jack Zucconi], [Product].[Drink], [Store].[USA]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[Jack Zucconi], [Product].[Food], [Store].[USA]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[Mary Francis Benigar], [Product].[Drink], [Store].[USA]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[Mary Francis Benigar], [Product].[Food], [Store].[USA]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[Kristin Miller], [Product].[Drink], [Store].[USA]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[Kristin Miller], [Product].[Food], [Store].[USA]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[James Horvat], [Product].[Drink], [Store].[USA]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[James Horvat], [Product].[Food], [Store].[USA]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[Frank Darrell], [Product].[Drink], [Store].[USA]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[Frank Darrell], [Product].[Food], [Store].[USA]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[Ida Rodriguez], [Product].[Drink], [Store].[USA]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[Ida Rodriguez], [Product].[Food], [Store].[USA]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[Matt Bellah], [Product].[Drink], [Store].[USA]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[Matt Bellah], [Product].[Food], [Store].[USA]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[Emily Barela], [Product].[Drink], [Store].[USA]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[Emily Barela], [Product].[Food], [Store].[USA]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[Wildon Cameron], [Product].[Drink], [Store].[USA]}\n"
            + "{[Customers].[USA].[WA].[Spokane].[Wildon Cameron], [Product].[Food], [Store].[USA]}\n"
            + "Row #0: 57\n"
            + "Row #1: 267\n"
            + "Row #2: 26\n"
            + "Row #3: 279\n"
            + "Row #4: 37\n"
            + "Row #5: 390\n"
            + "Row #6: 16\n"
            + "Row #7: 294\n"
            + "Row #8: 40\n"
            + "Row #9: 344\n"
            + "Row #10: 49\n"
            + "Row #11: 252\n"
            + "Row #12: 38\n"
            + "Row #13: 319\n"
            + "Row #14: 36\n"
            + "Row #15: 273\n"
            + "Row #16: 26\n"
            + "Row #17: 291\n"
            + "Row #18: 47\n"
            + "Row #19: 319\n";
    assertQueryReturns( query, expected );
  }
  
  public void testTop1CustomersWithColumnLevel() {
    String query =
        "WITH\n"
            + "SET [*NATIVE_CJ_SET] AS 'NONEMPTYCROSSJOIN([*BASE_MEMBERS__Time_],NONEMPTYCROSSJOIN([*BASE_MEMBERS__Product_],NONEMPTYCROSSJOIN([*BASE_MEMBERS__Education Level_],[*BASE_MEMBERS__Customers_])))'\n"
            + "SET [*METRIC_CJ_SET] AS 'FILTER([*NATIVE_CJ_SET],[Measures].[*TOP_Unit Sales_SEL~SUM] <= 1)'\n"
            + "SET [*SORTED_ROW_AXIS] AS 'ORDER([*CJ_ROW_AXIS],[Product].CURRENTMEMBER.ORDERKEY,BASC,[Education Level].CURRENTMEMBER.ORDERKEY,BASC,[Measures].[*SORTED_MEASURE],BDESC)'\n"
            + "SET [*NATIVE_MEMBERS__Time_] AS 'GENERATE([*NATIVE_CJ_SET], {[Time].CURRENTMEMBER})'\n"
            + "SET [*SORTED_COL_AXIS] AS 'ORDER([*CJ_COL_AXIS],[Time].CURRENTMEMBER.ORDERKEY,BASC,[Measures].CURRENTMEMBER.ORDERKEY,BASC)'\n"
            + "SET [*BASE_MEMBERS__Education Level_] AS '{[Education Level].[All Education Levels].[Bachelors Degree]}'\n"
            + "SET [*BASE_MEMBERS__Measures_] AS '{[Measures].[*FORMATTED_MEASURE_0]}'\n"
            + "SET [*BASE_MEMBERS__Customers_] AS '[Customers].[Name].MEMBERS'\n"
            + "SET [*CJ_COL_AXIS] AS 'GENERATE([*METRIC_CJ_SET], {([Time].CURRENTMEMBER)})'\n"
            + "SET [*BASE_MEMBERS__Product_] AS '{[Product].[All Products].[Drink]}'\n"
            + "SET [*BASE_MEMBERS__Time_] AS '[Time].[Year].MEMBERS'\n"
            + "SET [*CJ_ROW_AXIS] AS 'GENERATE([*METRIC_CJ_SET], {([Product].CURRENTMEMBER,[Education Level].CURRENTMEMBER,[Customers].CURRENTMEMBER)})'\n"
            + "MEMBER [Measures].[*FORMATTED_MEASURE_0] AS '[Measures].[Unit Sales]', FORMAT_STRING = 'Standard', SOLVE_ORDER=500\n"
            + "MEMBER [Measures].[*SORTED_MEASURE] AS '([Measures].[*FORMATTED_MEASURE_0],[Time].[*CTX_MEMBER_SEL~SUM])', SOLVE_ORDER=400\n"
            + "MEMBER [Measures].[*TOP_Unit Sales_SEL~SUM] AS 'RANK([Customers].CURRENTMEMBER,ORDER(GENERATE(CACHEDEXISTS([*NATIVE_CJ_SET],([Product].CURRENTMEMBER, [Education Level].CURRENTMEMBER),\"[*NATIVE_CJ_SET]\"),{[Customers].CURRENTMEMBER}),([Measures].[Unit Sales],[Product].CURRENTMEMBER,[Education Level].CURRENTMEMBER,[Time].[*CTX_MEMBER_SEL~SUM]),BDESC))', SOLVE_ORDER=400\n"
            + "MEMBER [Time].[*CTX_MEMBER_SEL~SUM] AS 'SUM([*NATIVE_MEMBERS__Time_])', SOLVE_ORDER=97\n" + "SELECT\n"
            + "CROSSJOIN([*SORTED_COL_AXIS],[*BASE_MEMBERS__Measures_]) ON COLUMNS\n" + ", NON EMPTY\n"
            + "[*SORTED_ROW_AXIS] ON ROWS\n" + "FROM [Sales]";
    String expected =
        "Axis #0:\n" + "{}\n" + "Axis #1:\n" + "{[Time].[1997], [Measures].[*FORMATTED_MEASURE_0]}\n" + "Axis #2:\n"
            + "{[Product].[Drink], [Education Level].[Bachelors Degree], [Customers].[USA].[WA].[Spokane].[Wildon Cameron]}\n"
            + "Row #0: 47\n";
    assertQueryReturns( query, expected );
  }
  
  public void testMondrian2704() {
    TestContext testContext = getTestContext().create(
        null,
        "<Cube name=\"Alternate Sales\">\n"
            + "  <Table name=\"sales_fact_1997\"/>\n"
            + "<Dimension name=\"Time\" type=\"TimeDimension\" foreignKey=\"time_id\">\n" + 
            "    <Hierarchy name=\"Time\" hasAll=\"true\" primaryKey=\"time_id\">\n" + 
            "      <Table name=\"time_by_day\"/>\n" + 
            "      <Level name=\"Year\" column=\"the_year\" type=\"Numeric\" uniqueMembers=\"true\"\n" + 
            "          levelType=\"TimeYears\"/>\n" + 
            "    </Hierarchy>\n" + 
            "    <Hierarchy hasAll=\"true\" name=\"Weekly\" primaryKey=\"time_id\">\n" + 
            "      <Table name=\"time_by_day\"/>\n" + 
            "      <Level name=\"Year\" column=\"the_year\" type=\"Numeric\" uniqueMembers=\"true\"\n" + 
            "          levelType=\"TimeYears\"/>\n" + 
            "    </Hierarchy>\n" + 
            "    <Hierarchy hasAll=\"true\" name=\"Weekly2\" primaryKey=\"time_id\">\n" + 
            "      <Table name=\"time_by_day\"/>\n" + 
            "      <Level name=\"Year\" column=\"the_year\" type=\"Numeric\" uniqueMembers=\"true\"\n" + 
            "          levelType=\"TimeYears\"/>\n" + 
            "    </Hierarchy>\n" + 
            "  </Dimension>"
            + "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\" formatString=\"Standard\"/>\n"
            + "</Cube>",
        null,
        null,
        null,
        null);
    
    
    // Verifies second arg of CachedExists uses a tuple type
    testContext.assertQueryReturns(
        "WITH\n" + 
        "SET [*NATIVE_CJ_SET] AS 'NONEMPTYCROSSJOIN([*BASE_MEMBERS__Time_],[*BASE_MEMBERS__Time.Weekly_])'\n" + 
        "SET [*SORTED_ROW_AXIS] AS 'ORDER([*CJ_ROW_AXIS],[Time].CURRENTMEMBER.ORDERKEY,BASC,[Time.Weekly].CURRENTMEMBER.ORDERKEY,BASC)'\n" + 
        "SET [*BASE_MEMBERS__Measures_] AS '{[Measures].[*FORMATTED_MEASURE_0]}'\n" + 
        "SET [*BASE_MEMBERS__Time.Weekly_] AS '[Time.Weekly].[Year].MEMBERS'\n" + 
        "SET [*BASE_MEMBERS__Time_] AS '[Time].[Year].MEMBERS'\n" + 
        "SET [*CJ_ROW_AXIS] AS 'GENERATE([*NATIVE_CJ_SET], {([Time].CURRENTMEMBER,[Time.Weekly].CURRENTMEMBER)})'\n" + 
        "MEMBER [Measures].[*FORMATTED_MEASURE_0] AS '[Measures].[Unit Sales]', FORMAT_STRING = 'Standard', SOLVE_ORDER=500\n" + 
        "MEMBER [Time.Weekly].[*DEFAULT_MEMBER] AS '[Time.Weekly].DEFAULTMEMBER', SOLVE_ORDER=-400\n" + 
        "MEMBER [Time.Weekly].[*TOTAL_MEMBER_SEL~SUM] AS 'SUM(CACHEDEXISTS([*CJ_ROW_AXIS],([Time].CURRENTMEMBER),\"[*CJ_ROW_AXIS]\"))', SOLVE_ORDER=99\n" + 
        "MEMBER [Time].[*TOTAL_MEMBER_SEL~SUM] AS 'SUM([*CJ_ROW_AXIS])', SOLVE_ORDER=100\n" + 
        "SELECT\n" + 
        "[*BASE_MEMBERS__Measures_] ON COLUMNS\n" + 
        ", NON EMPTY\n" + 
        "UNION(CROSSJOIN({[Time].[*TOTAL_MEMBER_SEL~SUM]},{([Time.Weekly].[*DEFAULT_MEMBER])}),UNION(CROSSJOIN(GENERATE([*CJ_ROW_AXIS], {([Time].CURRENTMEMBER)}),{[Time.Weekly].[*TOTAL_MEMBER_SEL~SUM]}),[*SORTED_ROW_AXIS])) ON ROWS\n" + 
        "FROM [Alternate Sales]",
        "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[*FORMATTED_MEASURE_0]}\n"
            + "Axis #2:\n"
            + "{[Time].[*TOTAL_MEMBER_SEL~SUM], [Time].[Weekly].[*DEFAULT_MEMBER]}\n"
            + "{[Time].[1997], [Time].[Weekly].[*TOTAL_MEMBER_SEL~SUM]}\n"
            + "{[Time].[1997], [Time].[Weekly].[1997]}\n"
            + "Row #0: 266,773\n"
            + "Row #1: 266,773\n"
            + "Row #2: 266,773\n");
    
    // Verified second arg of CachedExists uses a member type
    testContext.assertQueryReturns(
        "WITH\n" + 
        "SET [*NATIVE_CJ_SET] AS 'NONEMPTYCROSSJOIN([*BASE_MEMBERS__Time.Weekly_],NONEMPTYCROSSJOIN([*BASE_MEMBERS__Time_],[*BASE_MEMBERS__Time.Weekly2_]))'\n" + 
        "SET [*SORTED_ROW_AXIS] AS 'ORDER([*CJ_ROW_AXIS],[Time.Weekly].CURRENTMEMBER.ORDERKEY,BASC,[Time].CURRENTMEMBER.ORDERKEY,BASC,[Time.Weekly2].CURRENTMEMBER.ORDERKEY,BASC)'\n" + 
        "SET [*BASE_MEMBERS__Measures_] AS '{[Measures].[*FORMATTED_MEASURE_0]}'\n" + 
        "SET [*BASE_MEMBERS__Time.Weekly2_] AS '[Time.Weekly2].[Year].MEMBERS'\n" + 
        "SET [*BASE_MEMBERS__Time.Weekly_] AS '[Time.Weekly].[Year].MEMBERS'\n" + 
        "SET [*BASE_MEMBERS__Time_] AS '[Time].[Year].MEMBERS'\n" + 
        "SET [*CJ_ROW_AXIS] AS 'GENERATE([*NATIVE_CJ_SET], {([Time.Weekly].CURRENTMEMBER,[Time].CURRENTMEMBER,[Time.Weekly2].CURRENTMEMBER)})'\n" + 
        "MEMBER [Measures].[*FORMATTED_MEASURE_0] AS '[Measures].[Unit Sales]', FORMAT_STRING = 'Standard', SOLVE_ORDER=500\n" + 
        "MEMBER [Time.Weekly2].[*TOTAL_MEMBER_SEL~SUM] AS 'SUM(CACHEDEXISTS([*CJ_ROW_AXIS],([Time.Weekly].CURRENTMEMBER, [Time].CURRENTMEMBER),\"[*CJ_ROW_AXIS]\"))', SOLVE_ORDER=98\n" + 
        "SELECT\n" + 
        "[*BASE_MEMBERS__Measures_] ON COLUMNS\n" + 
        ", NON EMPTY\n" + 
        "UNION(CROSSJOIN(GENERATE([*CJ_ROW_AXIS], {([Time.Weekly].CURRENTMEMBER,[Time].CURRENTMEMBER)}),{[Time.Weekly2].[*TOTAL_MEMBER_SEL~SUM]}),[*SORTED_ROW_AXIS]) ON ROWS\n" + 
        "FROM [Alternate Sales]",
        "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[*FORMATTED_MEASURE_0]}\n"
            + "Axis #2:\n"
            + "{[Time].[Weekly].[1997], [Time].[1997], [Time.Weekly2].[*TOTAL_MEMBER_SEL~SUM]}\n"
            + "{[Time].[Weekly].[1997], [Time].[1997], [Time.Weekly2].[1997]}\n"
            + "Row #0: 266,773\n"
            + "Row #1: 266,773\n");
  }
}

