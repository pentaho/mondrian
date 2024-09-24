/*
 * This software is subject to the terms of the Eclipse Public License v1.0
 * Agreement, available at the following URL:
 * http://www.eclipse.org/legal/epl-v10.html.
 * You must accept the terms of that agreement to use this software.
 *
 * Copyright (c) 2002-2020 Hitachi Vantara..  All rights reserved.
 */

package mondrian.olap.fun;

import mondrian.olap.MondrianProperties;
import mondrian.test.FoodMartTestCase;

/**
 * <code>SortTest</code> tests the collation order of positive and negative
 * infinity, and {@link Double#NaN}.
 *
 * @author jhyde
 * @since Sep 21, 2006
 */
public class SortTest extends FoodMartTestCase {
  public void testFoo() {
    // Check that each value compares according to its position in the total
    // order. For example, NaN compares greater than
    // Double.NEGATIVE_INFINITY, -34.5, -0.001, 0, 0.00000567, 1, 3.14;
    // equal to NaN; and less than Double.POSITIVE_INFINITY.
    double[] values = {
      Double.NEGATIVE_INFINITY,
      FunUtil.DoubleNull,
      -34.5,
      -0.001,
      0,
      0.00000567,
      1,
      3.14,
      Double.NaN,
      Double.POSITIVE_INFINITY,
    };
    for ( int i = 0; i < values.length; i++ ) {
      for ( int j = 0; j < values.length; j++ ) {
        int expected = Integer.compare( i, j );
        assertEquals(
          "values[" + i + "]=" + values[ i ] + ", values[" + j
            + "]=" + values[ j ],
          expected,
          FunUtil.compareValues( values[ i ], values[ j ] ) );
      }
    }
  }

  public void testOrderDesc() {
    // In MSAS, NULLs collate last (or almost last, along with +inf and
    // NaN) whereas in Mondrian NULLs collate least (that is, before -inf).
    assertQueryReturns(
      "with"
        + "   member [Measures].[Foo] as '\n"
        + "      Iif([Promotion Media].CurrentMember IS [Promotion Media].[TV], 1.0 / 0.0,\n"
        + "         Iif([Promotion Media].CurrentMember IS [Promotion Media].[Radio], -1.0 / 0.0,\n"
        + "            Iif([Promotion Media].CurrentMember IS [Promotion Media].[Bulk Mail], 0.0 / 0.0,\n"
        + "               Iif([Promotion Media].CurrentMember IS [Promotion Media].[Daily Paper], NULL,\n"
        + "       [Measures].[Unit Sales])))) '\n"
        + "select \n"
        + "    {[Measures].[Foo]} on columns, \n"
        + "    order(except([Promotion Media].[Media Type].members,{[Promotion Media].[Media Type].[No Media]}),"
        + "[Measures].[Foo],DESC) on rows\n"
        + "from Sales",
      "Axis #0:\n"
        + "{}\n"
        + "Axis #1:\n"
        + "{[Measures].[Foo]}\n"
        + "Axis #2:\n"
        + "{[Promotion Media].[TV]}\n"
        + "{[Promotion Media].[Bulk Mail]}\n"
        + "{[Promotion Media].[Daily Paper, Radio, TV]}\n"
        + "{[Promotion Media].[Product Attachment]}\n"
        + "{[Promotion Media].[Daily Paper, Radio]}\n"
        + "{[Promotion Media].[Cash Register Handout]}\n"
        + "{[Promotion Media].[Sunday Paper, Radio]}\n"
        + "{[Promotion Media].[Street Handout]}\n"
        + "{[Promotion Media].[Sunday Paper]}\n"
        + "{[Promotion Media].[In-Store Coupon]}\n"
        + "{[Promotion Media].[Sunday Paper, Radio, TV]}\n"
        + "{[Promotion Media].[Radio]}\n"
        + "{[Promotion Media].[Daily Paper]}\n"
        + "Row #0: Infinity\n"
        + "Row #1: NaN\n"
        + "Row #2: 9,513\n"
        + "Row #3: 7,544\n"
        + "Row #4: 6,891\n"
        + "Row #5: 6,697\n"
        + "Row #6: 5,945\n"
        + "Row #7: 5,753\n"
        + "Row #8: 4,339\n"
        + "Row #9: 3,798\n"
        + "Row #10: 2,726\n"
        + "Row #11: -Infinity\n"
        + "Row #12: \n" );
  }

  public void testOrderAndRank() {
    assertQueryReturns(
      "with "
        + "   member [Measures].[Foo] as '\n"
        + "      Iif([Promotion Media].CurrentMember IS [Promotion Media].[TV], 1.0 / 0.0,\n"
        + "         Iif([Promotion Media].CurrentMember IS [Promotion Media].[Radio], -1.0 / 0.0,\n"
        + "            Iif([Promotion Media].CurrentMember IS [Promotion Media].[Bulk Mail], 0.0 / 0.0,\n"
        + "               Iif([Promotion Media].CurrentMember IS [Promotion Media].[Daily Paper], NULL,\n"
        + "                  [Measures].[Unit Sales])))) '\n"
        + "   member [Measures].[R] as '\n"
        + "      Rank([Promotion Media].CurrentMember, [Promotion Media].Members, [Measures].[Foo]) '\n"
        + "select\n"
        + "    {[Measures].[Foo], [Measures].[R]} on columns, \n"
        + "    order([Promotion Media].[Media Type].members,[Measures].[Foo]) on rows\n"
        + "from Sales",
      "Axis #0:\n"
        + "{}\n"
        + "Axis #1:\n"
        + "{[Measures].[Foo]}\n"
        + "{[Measures].[R]}\n"
        + "Axis #2:\n"
        + "{[Promotion Media].[Daily Paper]}\n"
        + "{[Promotion Media].[Radio]}\n"
        + "{[Promotion Media].[Sunday Paper, Radio, TV]}\n"
        + "{[Promotion Media].[In-Store Coupon]}\n"
        + "{[Promotion Media].[Sunday Paper]}\n"
        + "{[Promotion Media].[Street Handout]}\n"
        + "{[Promotion Media].[Sunday Paper, Radio]}\n"
        + "{[Promotion Media].[Cash Register Handout]}\n"
        + "{[Promotion Media].[Daily Paper, Radio]}\n"
        + "{[Promotion Media].[Product Attachment]}\n"
        + "{[Promotion Media].[Daily Paper, Radio, TV]}\n"
        + "{[Promotion Media].[No Media]}\n"
        + "{[Promotion Media].[Bulk Mail]}\n"
        + "{[Promotion Media].[TV]}\n"
        + "Row #0: \n"
        + "Row #0: 15\n"
        + "Row #1: -Infinity\n"
        + "Row #1: 14\n"
        + "Row #2: 2,726\n"
        + "Row #2: 13\n"
        + "Row #3: 3,798\n"
        + "Row #3: 12\n"
        + "Row #4: 4,339\n"
        + "Row #4: 11\n"
        + "Row #5: 5,753\n"
        + "Row #5: 10\n"
        + "Row #6: 5,945\n"
        + "Row #6: 9\n"
        + "Row #7: 6,697\n"
        + "Row #7: 8\n"
        + "Row #8: 6,891\n"
        + "Row #8: 7\n"
        + "Row #9: 7,544\n"
        + "Row #9: 6\n"
        + "Row #10: 9,513\n"
        + "Row #10: 5\n"
        + "Row #11: 195,448\n"
        + "Row #11: 4\n"
        + "Row #12: NaN\n"
        + "Row #12: 2\n"
        + "Row #13: Infinity\n"
        + "Row #13: 1\n" );
  }


  public void testListTuplesExceedsCellEvalLimit() {
    // cell eval performed within the sort, so cycles to retrieve all cells.
    propSaver.set( MondrianProperties.instance().CellBatchSize, 2 );
    assertAxisReturns(
      "ORDER(GENERATE(CROSSJOIN({[Customers].[USA].[WA].Children},{[Product].[Food]}),\n"
        + "{([Customers].CURRENTMEMBER,[Product].CURRENTMEMBER)}), [Measures].[Store Sales], BASC, [Customers]"
        + ".CURRENTMEMBER.ORDERKEY,BASC)",
      "{[Customers].[USA].[WA].[Sedro Woolley], [Product].[Food]}\n"
        + "{[Customers].[USA].[WA].[Anacortes], [Product].[Food]}\n"
        + "{[Customers].[USA].[WA].[Bellingham], [Product].[Food]}\n"
        + "{[Customers].[USA].[WA].[Seattle], [Product].[Food]}\n"
        + "{[Customers].[USA].[WA].[Issaquah], [Product].[Food]}\n"
        + "{[Customers].[USA].[WA].[Redmond], [Product].[Food]}\n"
        + "{[Customers].[USA].[WA].[Marysville], [Product].[Food]}\n"
        + "{[Customers].[USA].[WA].[Edmonds], [Product].[Food]}\n"
        + "{[Customers].[USA].[WA].[Renton], [Product].[Food]}\n"
        + "{[Customers].[USA].[WA].[Kirkland], [Product].[Food]}\n"
        + "{[Customers].[USA].[WA].[Walla Walla], [Product].[Food]}\n"
        + "{[Customers].[USA].[WA].[Lynnwood], [Product].[Food]}\n"
        + "{[Customers].[USA].[WA].[Ballard], [Product].[Food]}\n"
        + "{[Customers].[USA].[WA].[Everett], [Product].[Food]}\n"
        + "{[Customers].[USA].[WA].[Burien], [Product].[Food]}\n"
        + "{[Customers].[USA].[WA].[Tacoma], [Product].[Food]}\n"
        + "{[Customers].[USA].[WA].[Yakima], [Product].[Food]}\n"
        + "{[Customers].[USA].[WA].[Puyallup], [Product].[Food]}\n"
        + "{[Customers].[USA].[WA].[Bremerton], [Product].[Food]}\n"
        + "{[Customers].[USA].[WA].[Olympia], [Product].[Food]}\n"
        + "{[Customers].[USA].[WA].[Port Orchard], [Product].[Food]}\n"
        + "{[Customers].[USA].[WA].[Spokane], [Product].[Food]}" );

  }


  public void testNonBreakingAscendingComparator() {
    // more than one non-breaking sortkey, where first is ascending
    assertAxisReturns(
      "ORDER(GENERATE(CROSSJOIN({[Customers].[USA].[WA].Children},{[Product].[Food]}),\n"
        + "{([Customers].CURRENTMEMBER,[Product].CURRENTMEMBER)}), [Measures].[Unit Sales], DESC, [Measures].[Store "
        + "Sales], ASC)",
      "{[Customers].[USA].[WA].[Spokane], [Product].[Food]}\n"
        + "{[Customers].[USA].[WA].[Olympia], [Product].[Food]}\n"
        + "{[Customers].[USA].[WA].[Port Orchard], [Product].[Food]}\n"
        + "{[Customers].[USA].[WA].[Bremerton], [Product].[Food]}\n"
        + "{[Customers].[USA].[WA].[Puyallup], [Product].[Food]}\n"
        + "{[Customers].[USA].[WA].[Yakima], [Product].[Food]}\n"
        + "{[Customers].[USA].[WA].[Tacoma], [Product].[Food]}\n"
        + "{[Customers].[USA].[WA].[Burien], [Product].[Food]}\n"
        + "{[Customers].[USA].[WA].[Everett], [Product].[Food]}\n"
        + "{[Customers].[USA].[WA].[Ballard], [Product].[Food]}\n"
        + "{[Customers].[USA].[WA].[Kirkland], [Product].[Food]}\n"
        + "{[Customers].[USA].[WA].[Marysville], [Product].[Food]}\n"
        + "{[Customers].[USA].[WA].[Renton], [Product].[Food]}\n"
        + "{[Customers].[USA].[WA].[Walla Walla], [Product].[Food]}\n"
        + "{[Customers].[USA].[WA].[Lynnwood], [Product].[Food]}\n"
        + "{[Customers].[USA].[WA].[Redmond], [Product].[Food]}\n"
        + "{[Customers].[USA].[WA].[Issaquah], [Product].[Food]}\n"
        + "{[Customers].[USA].[WA].[Edmonds], [Product].[Food]}\n"
        + "{[Customers].[USA].[WA].[Seattle], [Product].[Food]}\n"
        + "{[Customers].[USA].[WA].[Anacortes], [Product].[Food]}\n"
        + "{[Customers].[USA].[WA].[Bellingham], [Product].[Food]}\n"
        + "{[Customers].[USA].[WA].[Sedro Woolley], [Product].[Food]}" );

  }


  public void testMultiLevelBrkSort() {
    // first 2 sort keys depend on Customers hierarchy only.
    // 3rd requires both Customer and Product
    assertQueryReturns( "WITH\n"
      + "SET [*NATIVE_CJ_SET] AS 'NONEMPTYCROSSJOIN([*BASE_MEMBERS__Customers_],[*BASE_MEMBERS__Product_])'\n"
      + "SET [*SORTED_ROW_AXIS] AS 'ORDER([*CJ_ROW_AXIS],[Customers].CURRENTMEMBER.ORDERKEY,BASC,ANCESTOR"
      + "([Customers].CURRENTMEMBER,[Customers].[City]).ORDERKEY,BASC,[Measures].[*SORTED_MEASURE],BASC)'\n"
      + "SET [*BASE_MEMBERS__Measures_] AS '{[Measures].[*FORMATTED_MEASURE_0],[Measures]"
      + ".[*FORMATTED_MEASURE_1]}'\n"
      + "SET [*BASE_MEMBERS__Customers_] AS '[Customers].[Name].MEMBERS'\n"
      + "SET [*BASE_MEMBERS__Product_] AS '[Product].[Product Name].MEMBERS'\n"
      + "SET [*CJ_ROW_AXIS] AS 'GENERATE([*NATIVE_CJ_SET], {([Customers].CURRENTMEMBER,[Product].CURRENTMEMBER)})"
      + "'\n"
      + "MEMBER [Measures].[*FORMATTED_MEASURE_0] AS '[Measures].[Unit Sales]', FORMAT_STRING = 'Standard', "
      + "SOLVE_ORDER=500\n"
      + "MEMBER [Measures].[*FORMATTED_MEASURE_1] AS '[Measures].[Store Sales]', FORMAT_STRING = '#,###', "
      + "SOLVE_ORDER=500\n"
      + "MEMBER [Measures].[*SORTED_MEASURE] AS '([Measures].[*FORMATTED_MEASURE_1])', SOLVE_ORDER=400\n"
      + "SELECT\n"
      + "[*BASE_MEMBERS__Measures_] ON COLUMNS\n"
      + ", NON EMPTY\n"
      + "HEAD([*SORTED_ROW_AXIS],5) ON ROWS\n"
      + "FROM [Sales]", "Axis #0:\n"
      + "{}\n"
      + "Axis #1:\n"
      + "{[Measures].[*FORMATTED_MEASURE_0]}\n"
      + "{[Measures].[*FORMATTED_MEASURE_1]}\n"
      + "Axis #2:\n"
      + "{[Customers].[USA].[WA].[Issaquah].[Jeanne Derry], [Product].[Food].[Starchy Foods].[Starchy Foods].[Pasta]"
      + ".[Monarch].[Monarch Spaghetti]}\n"
      + "{[Customers].[USA].[WA].[Issaquah].[Jeanne Derry], [Product].[Food].[Deli].[Side Dishes].[Deli Salads]"
      + ".[Lake].[Lake Low Fat Cole Slaw]}\n"
      + "{[Customers].[USA].[WA].[Issaquah].[Jeanne Derry], [Product].[Food].[Frozen Foods].[Breakfast Foods]"
      + ".[Waffles].[Big Time].[Big Time Low Fat Waffles]}\n"
      + "{[Customers].[USA].[WA].[Issaquah].[Jeanne Derry], [Product].[Food].[Baking Goods].[Baking Goods].[Sugar]"
      + ".[Super].[Super Brown Sugar]}\n"
      + "{[Customers].[USA].[WA].[Issaquah].[Jeanne Derry], [Product].[Non-Consumable].[Health and Hygiene].[Bathroom"
      + " Products].[Mouthwash].[Faux Products].[Faux Products Laundry Detergent]}\n"
      + "Row #0: 2\n"
      + "Row #0: 3\n"
      + "Row #1: 3\n"
      + "Row #1: 3\n"
      + "Row #2: 3\n"
      + "Row #2: 3\n"
      + "Row #3: 3\n"
      + "Row #3: 4\n"
      + "Row #4: 2\n"
      + "Row #4: 4\n" );
  }

  public void testAttributesWithShowsRowsColumnsWithMeasureData() {
    // Sort on Attributes with Shows rows/columns with measure data.   Most common use case.
    assertQueryReturns( "WITH\n"
      + "SET [*NATIVE_CJ_SET_WITH_SLICER] AS 'NONEMPTYCROSSJOIN([*BASE_MEMBERS__Store_],NONEMPTYCROSSJOIN"
      + "([*BASE_MEMBERS__Education Level_],NONEMPTYCROSSJOIN([*BASE_MEMBERS__Product_],NONEMPTYCROSSJOIN"
      + "([*BASE_MEMBERS__Yearly Income_],[*BASE_MEMBERS__Store Type_]))))'\n"
      + "SET [*NATIVE_CJ_SET] AS 'GENERATE([*NATIVE_CJ_SET_WITH_SLICER], {([Store].CURRENTMEMBER,[Education Level]"
      + ".CURRENTMEMBER,[Product].CURRENTMEMBER,[Yearly Income].CURRENTMEMBER)})'\n"
      + "SET [*BASE_MEMBERS__Store Type_] AS '{[Store Type].[All Store Types].[HeadQuarters],[Store Type].[All Store "
      + "Types].[Mid-Size Grocery],[Store Type].[All Store Types].[Small Grocery]}'\n"
      + "SET [*SORTED_ROW_AXIS] AS 'ORDER([*CJ_ROW_AXIS],[Education Level].CURRENTMEMBER.ORDERKEY,BASC,[Product]"
      + ".CURRENTMEMBER.ORDERKEY,BASC,ANCESTOR([Product].CURRENTMEMBER,[Product].[Product Family]).ORDERKEY,BASC,"
      + "[Yearly Income].CURRENTMEMBER.ORDERKEY,BASC)'\n"
      + "SET [*SORTED_COL_AXIS] AS 'ORDER([*CJ_COL_AXIS],[Store].CURRENTMEMBER.ORDERKEY,BASC,[Measures].CURRENTMEMBER"
      + ".ORDERKEY,BASC)'\n"
      + "SET [*BASE_MEMBERS__Education Level_] AS '[Education Level].[Education Level].MEMBERS'\n"
      + "SET [*BASE_MEMBERS__Measures_] AS '{[Measures].[*FORMATTED_MEASURE_0]}'\n"
      + "SET [*BASE_MEMBERS__Store_] AS '[Store].[Store Country].MEMBERS'\n"
      + "SET [*BASE_MEMBERS__Yearly Income_] AS '[Yearly Income].[Yearly Income].MEMBERS'\n"
      + "SET [*CJ_SLICER_AXIS] AS 'GENERATE([*NATIVE_CJ_SET_WITH_SLICER], {([Store Type].CURRENTMEMBER)})'\n"
      + "SET [*CJ_COL_AXIS] AS 'GENERATE([*NATIVE_CJ_SET], {([Store].CURRENTMEMBER)})'\n"
      + "SET [*BASE_MEMBERS__Product_] AS '[Product].[Product Department].MEMBERS'\n"
      + "SET [*CJ_ROW_AXIS] AS 'GENERATE([*NATIVE_CJ_SET], {([Education Level].CURRENTMEMBER,[Product].CURRENTMEMBER,"
      + "[Yearly Income].CURRENTMEMBER)})'\n"
      + "MEMBER [Measures].[*FORMATTED_MEASURE_0] AS '[Measures].[Unit Sales]', FORMAT_STRING = 'Standard', "
      + "SOLVE_ORDER=500\n"
      + "SELECT\n"
      + "CROSSJOIN([*SORTED_COL_AXIS],[*BASE_MEMBERS__Measures_]) ON COLUMNS\n"
      + ", NON EMPTY\n"
      + "{HEAD([*SORTED_ROW_AXIS],5), TAIL([*SORTED_ROW_AXIS],5)} ON ROWS\n"
      + "FROM [Sales]\n"
      + "WHERE ([*CJ_SLICER_AXIS])\n", "Axis #0:\n"
      + "{[Store Type].[Mid-Size Grocery]}\n"
      + "{[Store Type].[Small Grocery]}\n"
      + "Axis #1:\n"
      + "{[Store].[USA], [Measures].[*FORMATTED_MEASURE_0]}\n"
      + "Axis #2:\n"
      + "{[Education Level].[Bachelors Degree], [Product].[Drink].[Alcoholic Beverages], [Yearly Income].[$110K - "
      + "$130K]}\n"
      + "{[Education Level].[Bachelors Degree], [Product].[Drink].[Alcoholic Beverages], [Yearly Income].[$130K - "
      + "$150K]}\n"
      + "{[Education Level].[Bachelors Degree], [Product].[Drink].[Alcoholic Beverages], [Yearly Income].[$150K +]}\n"
      + "{[Education Level].[Bachelors Degree], [Product].[Drink].[Alcoholic Beverages], [Yearly Income].[$50K - "
      + "$70K]}\n"
      + "{[Education Level].[Bachelors Degree], [Product].[Drink].[Alcoholic Beverages], [Yearly Income].[$70K - "
      + "$90K]}\n"
      + "{[Education Level].[Partial High School], [Product].[Food].[Starchy Foods], [Yearly Income].[$130K - $150K]}\n"
      + "{[Education Level].[Partial High School], [Product].[Food].[Starchy Foods], [Yearly Income].[$150K +]}\n"
      + "{[Education Level].[Partial High School], [Product].[Food].[Starchy Foods], [Yearly Income].[$30K - $50K]}\n"
      + "{[Education Level].[Partial High School], [Product].[Food].[Starchy Foods], [Yearly Income].[$50K - $70K]}\n"
      + "{[Education Level].[Partial High School], [Product].[Food].[Starchy Foods], [Yearly Income].[$90K - $110K]}\n"
      + "Row #0: 15\n"
      + "Row #1: 8\n"
      + "Row #2: 13\n"
      + "Row #3: 75\n"
      + "Row #4: 43\n"
      + "Row #5: 5\n"
      + "Row #6: 4\n"
      + "Row #7: 9\n"
      + "Row #8: 2\n"
      + "Row #9: 3\n" );
  }

  public void testSortOnMeasureWithShowRowsColumnsWithMeasureData() {
    assertQueryReturns( "WITH\n"
      + "SET [*NATIVE_CJ_SET_WITH_SLICER] AS 'NONEMPTYCROSSJOIN([*BASE_MEMBERS__Education Level_],NONEMPTYCROSSJOIN"
      + "([*BASE_MEMBERS__Product_],NONEMPTYCROSSJOIN([*BASE_MEMBERS__Yearly Income_],NONEMPTYCROSSJOIN"
      + "([*BASE_MEMBERS__Store_],[*BASE_MEMBERS__Store Type_]))))'\n"
      + "SET [*NATIVE_CJ_SET] AS 'GENERATE([*NATIVE_CJ_SET_WITH_SLICER], {([Education Level].CURRENTMEMBER,[Product]"
      + ".CURRENTMEMBER,[Yearly Income].CURRENTMEMBER,[Store].CURRENTMEMBER)})'\n"
      + "SET [*BASE_MEMBERS__Store Type_] AS '{[Store Type].[All Store Types].[HeadQuarters],[Store Type].[All Store "
      + "Types].[Mid-Size Grocery],[Store Type].[All Store Types].[Small Grocery]}'\n"
      + "SET [*SORTED_ROW_AXIS] AS 'ORDER([*CJ_ROW_AXIS],[Education Level].CURRENTMEMBER.ORDERKEY,BASC,[Product]"
      + ".CURRENTMEMBER.ORDERKEY,BASC,ANCESTOR([Product].CURRENTMEMBER,[Product].[Product Family]).ORDERKEY,BASC,"
      + "[Yearly Income].CURRENTMEMBER.ORDERKEY,BASC,[Measures].[*SORTED_MEASURE],BASC)'\n"
      + "SET [*BASE_MEMBERS__Education Level_] AS '[Education Level].[Education Level].MEMBERS'\n"
      + "SET [*BASE_MEMBERS__Measures_] AS '{[Measures].[*FORMATTED_MEASURE_0]}'\n"
      + "SET [*BASE_MEMBERS__Store_] AS '[Store].[Store Country].MEMBERS'\n"
      + "SET [*BASE_MEMBERS__Yearly Income_] AS '[Yearly Income].[Yearly Income].MEMBERS'\n"
      + "SET [*CJ_SLICER_AXIS] AS 'GENERATE([*NATIVE_CJ_SET_WITH_SLICER], {([Store Type].CURRENTMEMBER)})'\n"
      + "SET [*BASE_MEMBERS__Product_] AS '[Product].[Product Department].MEMBERS'\n"
      + "SET [*CJ_ROW_AXIS] AS 'GENERATE([*NATIVE_CJ_SET], {([Education Level].CURRENTMEMBER,[Product].CURRENTMEMBER,"
      + "[Yearly Income].CURRENTMEMBER,[Store].CURRENTMEMBER)})'\n"
      + "MEMBER [Measures].[*FORMATTED_MEASURE_0] AS '[Measures].[Unit Sales]', FORMAT_STRING = 'Standard', "
      + "SOLVE_ORDER=500\n"
      + "MEMBER [Measures].[*SORTED_MEASURE] AS '([Measures].[*FORMATTED_MEASURE_0])', SOLVE_ORDER=400\n"
      + "SELECT\n"
      + "[*BASE_MEMBERS__Measures_] ON COLUMNS\n"
      + ", NON EMPTY\n"
      + "{HEAD([*SORTED_ROW_AXIS],5), TAIL([*SORTED_ROW_AXIS],5)} ON ROWS\n"
      + "FROM [Sales]\n"
      + "WHERE ([*CJ_SLICER_AXIS])\n", "Axis #0:\n"
      + "{[Store Type].[Mid-Size Grocery]}\n"
      + "{[Store Type].[Small Grocery]}\n"
      + "Axis #1:\n"
      + "{[Measures].[*FORMATTED_MEASURE_0]}\n"
      + "Axis #2:\n"
      + "{[Education Level].[Bachelors Degree], [Product].[Drink].[Alcoholic Beverages], [Yearly Income].[$110K - "
      + "$130K], [Store].[USA]}\n"
      + "{[Education Level].[Bachelors Degree], [Product].[Drink].[Alcoholic Beverages], [Yearly Income].[$130K - "
      + "$150K], [Store].[USA]}\n"
      + "{[Education Level].[Bachelors Degree], [Product].[Drink].[Alcoholic Beverages], [Yearly Income].[$150K +], "
      + "[Store].[USA]}\n"
      + "{[Education Level].[Bachelors Degree], [Product].[Drink].[Alcoholic Beverages], [Yearly Income].[$50K - "
      + "$70K], [Store].[USA]}\n"
      + "{[Education Level].[Bachelors Degree], [Product].[Drink].[Alcoholic Beverages], [Yearly Income].[$70K - "
      + "$90K], [Store].[USA]}\n"
      + "{[Education Level].[Partial High School], [Product].[Food].[Starchy Foods], [Yearly Income].[$130K - $150K],"
      + " [Store].[USA]}\n"
      + "{[Education Level].[Partial High School], [Product].[Food].[Starchy Foods], [Yearly Income].[$150K +], "
      + "[Store].[USA]}\n"
      + "{[Education Level].[Partial High School], [Product].[Food].[Starchy Foods], [Yearly Income].[$30K - $50K], "
      + "[Store].[USA]}\n"
      + "{[Education Level].[Partial High School], [Product].[Food].[Starchy Foods], [Yearly Income].[$50K - $70K], "
      + "[Store].[USA]}\n"
      + "{[Education Level].[Partial High School], [Product].[Food].[Starchy Foods], [Yearly Income].[$90K - $110K], "
      + "[Store].[USA]}\n"
      + "Row #0: 15\n"
      + "Row #1: 8\n"
      + "Row #2: 13\n"
      + "Row #3: 75\n"
      + "Row #4: 43\n"
      + "Row #5: 5\n"
      + "Row #6: 4\n"
      + "Row #7: 9\n"
      + "Row #8: 2\n"
      + "Row #9: 3\n" );
  }

  public void testSortOnAttributesWithShowsRowsColumnsWithMeasureAndCalculatedMeasureData() {
    assertQueryReturns( "WITH\n"
      + "SET [*NATIVE_CJ_SET_WITH_SLICER] AS '[*BASE_MEMBERS__Store Type_]'\n"
      + "SET [*NATIVE_CJ_SET] AS 'CROSSJOIN([*BASE_MEMBERS__Education Level_],CROSSJOIN([*BASE_MEMBERS__Product_],"
      + "[*BASE_MEMBERS__Yearly Income_]))'\n"
      + "SET [*BASE_MEMBERS__Store Type_] AS '{[Store Type].[All Store Types].[HeadQuarters],[Store Type].[All Store "
      + "Types].[Mid-Size Grocery],[Store Type].[All Store Types].[Small Grocery]}'\n"
      + "SET [*SORTED_ROW_AXIS] AS 'ORDER([*CJ_ROW_AXIS],[Education Level].CURRENTMEMBER.ORDERKEY,BASC,[Product]"
      + ".CURRENTMEMBER.ORDERKEY,BASC,ANCESTOR([Product].CURRENTMEMBER,[Product].[Product Family]).ORDERKEY,BASC,"
      + "[Yearly Income].CURRENTMEMBER.ORDERKEY,BASC)'\n"
      + "SET [*BASE_MEMBERS__Education Level_] AS '[Education Level].[Education Level].MEMBERS'\n"
      + "SET [*BASE_MEMBERS__Measures_] AS '{[Measures].[*FORMATTED_MEASURE_0]}'\n"
      + "SET [*BASE_MEMBERS__Yearly Income_] AS '[Yearly Income].[Yearly Income].MEMBERS'\n"
      + "SET [*CJ_SLICER_AXIS] AS 'GENERATE([*NATIVE_CJ_SET_WITH_SLICER], {([Store Type].CURRENTMEMBER)})'\n"
      + "SET [*BASE_MEMBERS__Product_] AS '[Product].[Product Department].MEMBERS'\n"
      + "SET [*CJ_ROW_AXIS] AS 'GENERATE([*NATIVE_CJ_SET], {([Education Level].CURRENTMEMBER,[Product].CURRENTMEMBER,"
      + "[Yearly Income].CURRENTMEMBER)})'\n"
      + "MEMBER [Measures].[*FORMATTED_MEASURE_0] AS '[Measures].[Unit Sales]', FORMAT_STRING = 'Standard', "
      + "SOLVE_ORDER=500\n"
      + "SELECT\n"
      + "[*BASE_MEMBERS__Measures_] ON COLUMNS\n"
      + ", NON EMPTY\n"
      + "{HEAD([*SORTED_ROW_AXIS],5), TAIL([*SORTED_ROW_AXIS],5)} ON ROWS\n"
      + "FROM [Sales]\n"
      + "WHERE ([*CJ_SLICER_AXIS])\n", "Axis #0:\n"
      + "{[Store Type].[HeadQuarters]}\n"
      + "{[Store Type].[Mid-Size Grocery]}\n"
      + "{[Store Type].[Small Grocery]}\n"
      + "Axis #1:\n"
      + "{[Measures].[*FORMATTED_MEASURE_0]}\n"
      + "Axis #2:\n"
      + "{[Education Level].[Bachelors Degree], [Product].[Drink].[Alcoholic Beverages], [Yearly Income].[$110K - "
      + "$130K]}\n"
      + "{[Education Level].[Bachelors Degree], [Product].[Drink].[Alcoholic Beverages], [Yearly Income].[$130K - "
      + "$150K]}\n"
      + "{[Education Level].[Bachelors Degree], [Product].[Drink].[Alcoholic Beverages], [Yearly Income].[$150K +]}\n"
      + "{[Education Level].[Partial High School], [Product].[Food].[Starchy Foods], [Yearly Income].[$150K +]}\n"
      + "{[Education Level].[Partial High School], [Product].[Food].[Starchy Foods], [Yearly Income].[$30K - $50K]}\n"
      + "{[Education Level].[Partial High School], [Product].[Food].[Starchy Foods], [Yearly Income].[$50K - $70K]}\n"
      + "{[Education Level].[Partial High School], [Product].[Food].[Starchy Foods], [Yearly Income].[$90K - $110K]}\n"
      + "Row #0: 15\n"
      + "Row #1: 8\n"
      + "Row #2: 13\n"
      + "Row #3: 4\n"
      + "Row #4: 9\n"
      + "Row #5: 2\n"
      + "Row #6: 3\n" );
  }

  public void testSortOnMeasureWithShowsRowsColumnsWithShowAllEvenBlank() {
    assertQueryReturns( "WITH\n"
      + "SET [*NATIVE_CJ_SET_WITH_SLICER] AS '[*BASE_MEMBERS__Store Type_]'\n"
      + "SET [*NATIVE_CJ_SET] AS 'CROSSJOIN([*BASE_MEMBERS__Education Level_],CROSSJOIN([*BASE_MEMBERS__Product_],"
      + "[*BASE_MEMBERS__Yearly Income_]))'\n"
      + "SET [*BASE_MEMBERS__Store Type_] AS '{[Store Type].[All Store Types].[HeadQuarters],[Store Type].[All Store "
      + "Types].[Mid-Size Grocery],[Store Type].[All Store Types].[Small Grocery]}'\n"
      + "SET [*SORTED_ROW_AXIS] AS 'ORDER([*CJ_ROW_AXIS],[Education Level].CURRENTMEMBER.ORDERKEY,BASC,[Product]"
      + ".CURRENTMEMBER.ORDERKEY,BASC,ANCESTOR([Product].CURRENTMEMBER,[Product].[Product Family]).ORDERKEY,BASC,"
      + "[Measures].[*SORTED_MEASURE],BASC)'\n"
      + "SET [*BASE_MEMBERS__Education Level_] AS '[Education Level].[Education Level].MEMBERS'\n"
      + "SET [*BASE_MEMBERS__Measures_] AS '{[Measures].[*FORMATTED_MEASURE_0]}'\n"
      + "SET [*BASE_MEMBERS__Yearly Income_] AS '[Yearly Income].[Yearly Income].MEMBERS'\n"
      + "SET [*CJ_SLICER_AXIS] AS 'GENERATE([*NATIVE_CJ_SET_WITH_SLICER], {([Store Type].CURRENTMEMBER)})'\n"
      + "SET [*BASE_MEMBERS__Product_] AS '[Product].[Product Department].MEMBERS'\n"
      + "SET [*CJ_ROW_AXIS] AS 'GENERATE([*NATIVE_CJ_SET], {([Education Level].CURRENTMEMBER,[Product].CURRENTMEMBER,"
      + "[Yearly Income].CURRENTMEMBER)})'\n"
      + "MEMBER [Measures].[*FORMATTED_MEASURE_0] AS '[Measures].[Unit Sales]', FORMAT_STRING = 'Standard', "
      + "SOLVE_ORDER=500\n"
      + "MEMBER [Measures].[*SORTED_MEASURE] AS '([Measures].[*FORMATTED_MEASURE_0])', SOLVE_ORDER=400\n"
      + "SELECT\n"
      + "[*BASE_MEMBERS__Measures_] ON COLUMNS,\n"
      + "{HEAD([*SORTED_ROW_AXIS],5), TAIL([*SORTED_ROW_AXIS],5)} ON ROWS\n"
      + "FROM [Sales]\n"
      + "WHERE ([*CJ_SLICER_AXIS])\n", "Axis #0:\n"
      + "{[Store Type].[HeadQuarters]}\n"
      + "{[Store Type].[Mid-Size Grocery]}\n"
      + "{[Store Type].[Small Grocery]}\n"
      + "Axis #1:\n"
      + "{[Measures].[*FORMATTED_MEASURE_0]}\n"
      + "Axis #2:\n"
      + "{[Education Level].[Bachelors Degree], [Product].[Drink].[Alcoholic Beverages], [Yearly Income].[$10K - "
      + "$30K]}\n"
      + "{[Education Level].[Bachelors Degree], [Product].[Drink].[Alcoholic Beverages], [Yearly Income].[$30K - "
      + "$50K]}\n"
      + "{[Education Level].[Bachelors Degree], [Product].[Drink].[Alcoholic Beverages], [Yearly Income].[$90K - "
      + "$110K]}\n"
      + "{[Education Level].[Bachelors Degree], [Product].[Drink].[Alcoholic Beverages], [Yearly Income].[$130K - "
      + "$150K]}\n"
      + "{[Education Level].[Bachelors Degree], [Product].[Drink].[Alcoholic Beverages], [Yearly Income].[$150K +]}\n"
      + "{[Education Level].[Partial High School], [Product].[Food].[Starchy Foods], [Yearly Income].[$150K +]}\n"
      + "{[Education Level].[Partial High School], [Product].[Food].[Starchy Foods], [Yearly Income].[$130K - $150K]}\n"
      + "{[Education Level].[Partial High School], [Product].[Food].[Starchy Foods], [Yearly Income].[$30K - $50K]}\n"
      + "{[Education Level].[Partial High School], [Product].[Food].[Starchy Foods], [Yearly Income].[$110K - $130K]}\n"
      + "{[Education Level].[Partial High School], [Product].[Food].[Starchy Foods], [Yearly Income].[$10K - $30K]}\n"
      + "Row #0: \n"
      + "Row #1: \n"
      + "Row #2: \n"
      + "Row #3: 8\n"
      + "Row #4: 13\n"
      + "Row #5: 4\n"
      + "Row #6: 5\n"
      + "Row #7: 9\n"
      + "Row #8: 10\n"
      + "Row #9: 68\n" );
  }

}

// End SortTest.java
