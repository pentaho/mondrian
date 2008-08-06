/*
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2008 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
*/

package mondrian.test;

import mondrian.olap.MondrianProperties;
/**
 * Runs specified set of MDX queries concurrently.
 * This Class is not added to the Main test suite.
 * Purpose of this test is to simulate Concurrent access to Aggregation and data
 * load. Simulation will be more effective if we run this single test again and
 * again with a fresh connection.
 *
 * @author Thiyagu,Ajit
 * @version $Id$
 */
public class ConcurrentMdxTest extends FoodMartTestCase {
    private MondrianProperties props;

    static final QueryAndResult[] mdxQueries = new QueryAndResult[]{
            new QueryAndResult("select {[Measures].[Sales Count]} on 0 from [Sales] ",
                    "Axis #0:\n" +
                            "{}\n" +
                            "Axis #1:\n" +
                            "{[Measures].[Sales Count]}\n" +
                            "Row #0: 86,837\n"),

            new QueryAndResult("select {[Measures].[Store Cost]} on 0 from [Sales] ",
                    "Axis #0:\n" +
                            "{}\n" +
                            "Axis #1:\n" +
                            "{[Measures].[Store Cost]}\n" +
                            "Row #0: 225,627.23\n"),

            new QueryAndResult("select {[Measures].[Sales Count], " +
                    "[Measures].[Store Invoice]} on 0 from [Warehouse and Sales]",
                    "Axis #0:\n" +
                            "{}\n" +
                            "Axis #1:\n" +
                            "{[Measures].[Sales Count]}\n" +
                            "{[Measures].[Store Invoice]}\n" +
                            "Row #0: 86,837\n" +
                            "Row #0: 102,278.409\n"),

            new QueryAndResult("select {[Measures].[Sales Count], " +
                    "[Measures].[Store Invoice]} on 0 from [Warehouse and Sales]",
                    "Axis #0:\n" +
                            "{}\n" +
                            "Axis #1:\n" +
                            "{[Measures].[Sales Count]}\n" +
                            "{[Measures].[Store Invoice]}\n" +
                            "Row #0: 86,837\n" +
                            "Row #0: 102,278.409\n"),

            new QueryAndResult(
                    "select {[Measures].[Sales Count], " +
                            "[Measures].[Store Invoice]} on 0, " +
                            "{[Time].[1997],[Time].[1998]} on 1 " +
                            "from [Warehouse and Sales]",
                            "Axis #0:\n" +
                                    "{}\n" +
                                    "Axis #1:\n" +
                                    "{[Measures].[Sales Count]}\n" +
                                    "{[Measures].[Store Invoice]}\n" +
                                    "Axis #2:\n" +
                                    "{[Time].[1997]}\n" +
                                    "{[Time].[1998]}\n" +
                                    "Row #0: 86,837\n" +
                                    "Row #0: 102,278.409\n" +
                                    "Row #1: \n" +
                                    "Row #1: \n"),

            new QueryAndResult(
                    "select {[Measures].[Sales Count], " +
                            "[Measures].[Store Invoice]} on 0, " +
                            "{([Gender].[M],[Time].[1997])} on 1 " +
                            "from [Warehouse and Sales]",
                    "Axis #0:\n" +
                            "{}\n" +
                            "Axis #1:\n" +
                            "{[Measures].[Sales Count]}\n" +
                            "{[Measures].[Store Invoice]}\n" +
                            "Axis #2:\n" +
                            "{[Gender].[All Gender].[M], [Time].[1997]}\n" +
                            "Row #0: 44,006\n" +
                            "Row #0: \n"),

            new QueryAndResult(
                    "select {[Measures].[Sales Count], " +
                            "[Measures].[Store Invoice]} on 0, " +
                            "{([Gender].[F],[Time].[1997])} on 1 " +
                            "from [Warehouse and Sales]",
                    "Axis #0:\n" +
                            "{}\n" +
                            "Axis #1:\n" +
                            "{[Measures].[Sales Count]}\n" +
                            "{[Measures].[Store Invoice]}\n" +
                            "Axis #2:\n" +
                            "{[Gender].[All Gender].[F], [Time].[1997]}\n" +
                            "Row #0: 42,831\n" +
                            "Row #0: \n"),

            new QueryAndResult("select {[Measures].[Store Cost], " +
                    "[Measures].[Supply Time]} on 0 from [Warehouse and Sales]",
                    "Axis #0:\n" +
                            "{}\n" +
                            "Axis #1:\n" +
                            "{[Measures].[Store Cost]}\n" +
                            "{[Measures].[Supply Time]}\n" +
                            "Row #0: 225,627.23\n" +
                            "Row #0: 10,425\n"),

            new QueryAndResult("select {[Measures].[Store Sales], " +
                    "[Measures].[Units Ordered]} on 0 from [Warehouse and Sales]",
                    "Axis #0:\n" +
                            "{}\n" +
                            "Axis #1:\n" +
                            "{[Measures].[Store Sales]}\n" +
                            "{[Measures].[Units Ordered]}\n" +
                            "Row #0: 565,238.13\n" +
                            "Row #0: 227238.0\n"),

            new QueryAndResult("select {[Measures].[Unit Sales], " +
                    "[Measures].[Units Ordered]} on 0 from [Warehouse and Sales]",
                    "Axis #0:\n" +
                            "{}\n" +
                            "Axis #1:\n" +
                            "{[Measures].[Unit Sales]}\n" +
                            "{[Measures].[Units Ordered]}\n" +
                            "Row #0: 266,773\n" +
                            "Row #0: 227238.0\n"),

            new QueryAndResult("select {[Measures].[Profit], " +
                    "[Measures].[Units Shipped]} on 0 from [Warehouse and Sales]",
                    "Axis #0:\n" +
                            "{}\n" +
                            "Axis #1:\n" +
                            "{[Measures].[Profit]}\n" +
                            "{[Measures].[Units Shipped]}\n" +
                            "Row #0: $339,610.90\n" +
                            "Row #0: 207726.0\n"),

            new QueryAndResult("select {[Measures].[Unit Sales]} on columns" + nl +
                    " from Sales", "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Unit Sales]}\n" +
                    "Row #0: 266,773\n"),

            new QueryAndResult("select " + nl +
                    "{[Measures].[Unit Sales]} on columns," + nl +
                    "order(except([Promotion Media].[Media Type].members," +
                    "{[Promotion Media].[Media Type].[No Media]})," +
                    "[Measures].[Unit Sales],DESC) on rows" + nl +
                    "from Sales", "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Unit Sales]}\n" +
                    "Axis #2:\n" +
                    "{[Promotion Media].[All Media].[Daily Paper, Radio, TV]}\n" +
                    "{[Promotion Media].[All Media].[Daily Paper]}\n" +
                    "{[Promotion Media].[All Media].[Product Attachment]}\n" +
                    "{[Promotion Media].[All Media].[Daily Paper, Radio]}\n" +
                    "{[Promotion Media].[All Media].[Cash Register Handout]}\n" +
                    "{[Promotion Media].[All Media].[Sunday Paper, Radio]}\n" +
                    "{[Promotion Media].[All Media].[Street Handout]}\n" +
                    "{[Promotion Media].[All Media].[Sunday Paper]}\n" +
                    "{[Promotion Media].[All Media].[Bulk Mail]}\n" +
                    "{[Promotion Media].[All Media].[In-Store Coupon]}\n" +
                    "{[Promotion Media].[All Media].[TV]}\n" +
                    "{[Promotion Media].[All Media].[Sunday Paper, Radio, TV]}\n" +
                    "{[Promotion Media].[All Media].[Radio]}\n" +
                    "Row #0: 9,513\n" +
                    "Row #1: 7,738\n" +
                    "Row #2: 7,544\n" +
                    "Row #3: 6,891\n" +
                    "Row #4: 6,697\n" +
                    "Row #5: 5,945\n" +
                    "Row #6: 5,753\n" +
                    "Row #7: 4,339\n" +
                    "Row #8: 4,320\n" +
                    "Row #9: 3,798\n" +
                    "Row #10: 3,607\n" +
                    "Row #11: 2,726\n" +
                    "Row #12: 2,454\n"),

            new QueryAndResult("select" + nl +
                    "{ [Measures].[Units Shipped], [Measures].[Units Ordered] }" +
                    " on columns," + nl +
                    "NON EMPTY [Store].[Store Name].members on rows" + nl +
                    "from Warehouse", "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Units Shipped]}\n" +
                    "{[Measures].[Units Ordered]}\n" +
                    "Axis #2:\n" +
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
                    "Row #0: 10759.0\n" +
                    "Row #0: 11699.0\n" +
                    "Row #1: 24587.0\n" +
                    "Row #1: 26463.0\n" +
                    "Row #2: 23835.0\n" +
                    "Row #2: 26270.0\n" +
                    "Row #3: 1696.0\n" +
                    "Row #3: 1875.0\n" +
                    "Row #4: 8515.0\n" +
                    "Row #4: 9109.0\n" +
                    "Row #5: 32393.0\n" +
                    "Row #5: 35797.0\n" +
                    "Row #6: 2348.0\n" +
                    "Row #6: 2454.0\n" +
                    "Row #7: 22734.0\n" +
                    "Row #7: 24610.0\n" +
                    "Row #8: 24110.0\n" +
                    "Row #8: 26703.0\n" +
                    "Row #9: 11889.0\n" +
                    "Row #9: 12828.0\n" +
                    "Row #10: 32411.0\n" +
                    "Row #10: 35930.0\n" +
                    "Row #11: 1860.0\n" +
                    "Row #11: 2074.0\n" +
                    "Row #12: 10589.0\n" +
                    "Row #12: 11426.0\n"),

            new QueryAndResult("with member [Measures].[Store Sales Last Period] as" +
                    " '([Measures].[Store Sales], Time.PrevMember)'" + nl +
                    "select" + nl +
                    " {[Measures].[Store Sales Last Period]} on columns," + nl +
                    " {TopCount([Product].[Product Department].members,5," +
                    " [Measures].[Store Sales Last Period])} on rows" + nl +
                    "from Sales" + nl +
                    "where ([Time].[1998])", "Axis #0:\n" +
                    "{[Time].[1998]}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Store Sales Last Period]}\n" +
                    "Axis #2:\n" +
                    "{[Product].[All Products].[Food].[Produce]}\n" +
                    "{[Product].[All Products].[Food].[Snack Foods]}\n" +
                    "{[Product].[All Products].[Non-Consumable].[Household]}\n" +
                    "{[Product].[All Products].[Food].[Frozen Foods]}\n" +
                    "{[Product].[All Products].[Food].[Canned Foods]}\n" +
                    "Row #0: 82,248.42\n" +
                    "Row #1: 67,609.82\n" +
                    "Row #2: 60,469.89\n" +
                    "Row #3: 55,207.50\n" +
                    "Row #4: 39,774.34\n"),

            new QueryAndResult("with member [Measures].[Total Store Sales] as" +
                    "'Sum(YTD(),[Measures].[Store Sales])'" + nl +
                    "select" + nl +
                    "{[Measures].[Total Store Sales]} on columns," + nl +
                    "{TopCount([Product].[Product Department].members,5," +
                    "[Measures].[Total Store Sales])} on rows" + nl +
                    "from Sales" + nl +
                    "where ([Time].[1997].[Q2].[4])", "Axis #0:\n" +
                    "{[Time].[1997].[Q2].[4]}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Total Store Sales]}\n" +
                    "Axis #2:\n" +
                    "{[Product].[All Products].[Food].[Produce]}\n" +
                    "{[Product].[All Products].[Food].[Snack Foods]}\n" +
                    "{[Product].[All Products].[Non-Consumable].[Household]}\n" +
                    "{[Product].[All Products].[Food].[Frozen Foods]}\n" +
                    "{[Product].[All Products].[Food].[Canned Foods]}\n" +
                    "Row #0: 26,526.67\n" +
                    "Row #1: 21,897.10\n" +
                    "Row #2: 19,980.90\n" +
                    "Row #3: 17,882.63\n" +
                    "Row #4: 12,963.23\n"),

            new QueryAndResult("with member [Measures].[Store Profit Rate] as" +
                    "'([Measures].[Store Sales]-[Measures].[Store Cost])/" +
                    "[Measures].[Store Cost]', format = '#.00%'" + nl +
                    "select" + nl +
                    "    {[Measures].[Store Cost],[Measures].[Store Sales]," +
                    "[Measures].[Store Profit Rate]} on columns," + nl +
                    "    Order([Product].[Product Department].members, " +
                    "[Measures].[Store Profit Rate], BDESC) on rows" + nl +
                    "from Sales" + nl +
                    "where ([Time].[1997])", "Axis #0:\n" +
                    "{[Time].[1997]}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Store Cost]}\n" +
                    "{[Measures].[Store Sales]}\n" +
                    "{[Measures].[Store Profit Rate]}\n" +
                    "Axis #2:\n" +
                    "{[Product].[All Products].[Food].[Breakfast Foods]}\n" +
                    "{[Product].[All Products].[Non-Consumable].[Carousel]}\n" +
                    "{[Product].[All Products].[Food].[Canned Products]}\n" +
                    "{[Product].[All Products].[Food].[Baking Goods]}\n" +
                    "{[Product].[All Products].[Drink].[Alcoholic Beverages]}\n" +
                    "{[Product].[All Products].[Non-Consumable].[Health and Hygiene]}\n" +
                    "{[Product].[All Products].[Food].[Snack Foods]}\n" +
                    "{[Product].[All Products].[Food].[Baked Goods]}\n" +
                    "{[Product].[All Products].[Drink].[Beverages]}\n" +
                    "{[Product].[All Products].[Food].[Frozen Foods]}\n" +
                    "{[Product].[All Products].[Non-Consumable].[Periodicals]}\n" +
                    "{[Product].[All Products].[Food].[Produce]}\n" +
                    "{[Product].[All Products].[Food].[Seafood]}\n" +
                    "{[Product].[All Products].[Food].[Deli]}\n" +
                    "{[Product].[All Products].[Food].[Meat]}\n" +
                    "{[Product].[All Products].[Food].[Canned Foods]}\n" +
                    "{[Product].[All Products].[Non-Consumable].[Household]}\n" +
                    "{[Product].[All Products].[Food].[Starchy Foods]}\n" +
                    "{[Product].[All Products].[Food].[Eggs]}\n" +
                    "{[Product].[All Products].[Food].[Snacks]}\n" +
                    "{[Product].[All Products].[Food].[Dairy]}\n" +
                    "{[Product].[All Products].[Drink].[Dairy]}\n" +
                    "{[Product].[All Products].[Non-Consumable].[Checkout]}\n" +
                    "Row #0: 2,756.80\n" +
                    "Row #0: 6,941.46\n" +
                    "Row #0: 151.79%\n" +
                    "Row #1: 595.97\n" +
                    "Row #1: 1,500.11\n" +
                    "Row #1: 151.71%\n" +
                    "Row #2: 1,317.13\n" +
                    "Row #2: 3,314.52\n" +
                    "Row #2: 151.65%\n" +
                    "Row #3: 15,370.61\n" +
                    "Row #3: 38,670.41\n" +
                    "Row #3: 151.59%\n" +
                    "Row #4: 5,576.79\n" +
                    "Row #4: 14,029.08\n" +
                    "Row #4: 151.56%\n" +
                    "Row #5: 12,972.99\n" +
                    "Row #5: 32,571.86\n" +
                    "Row #5: 151.07%\n" +
                    "Row #6: 26,963.34\n" +
                    "Row #6: 67,609.82\n" +
                    "Row #6: 150.75%\n" +
                    "Row #7: 6,564.09\n" +
                    "Row #7: 16,455.43\n" +
                    "Row #7: 150.69%\n" +
                    "Row #8: 11,069.53\n" +
                    "Row #8: 27,748.53\n" +
                    "Row #8: 150.67%\n" +
                    "Row #9: 22,030.66\n" +
                    "Row #9: 55,207.50\n" +
                    "Row #9: 150.59%\n" +
                    "Row #10: 3,614.55\n" +
                    "Row #10: 9,056.76\n" +
                    "Row #10: 150.56%\n" +
                    "Row #11: 32,831.33\n" +
                    "Row #11: 82,248.42\n" +
                    "Row #11: 150.52%\n" +
                    "Row #12: 1,520.70\n" +
                    "Row #12: 3,809.14\n" +
                    "Row #12: 150.49%\n" +
                    "Row #13: 10,108.87\n" +
                    "Row #13: 25,318.93\n" +
                    "Row #13: 150.46%\n" +
                    "Row #14: 1,465.42\n" +
                    "Row #14: 3,669.89\n" +
                    "Row #14: 150.43%\n" +
                    "Row #15: 15,894.53\n" +
                    "Row #15: 39,774.34\n" +
                    "Row #15: 150.24%\n" +
                    "Row #16: 24,170.73\n" +
                    "Row #16: 60,469.89\n" +
                    "Row #16: 150.18%\n" +
                    "Row #17: 4,705.91\n" +
                    "Row #17: 11,756.07\n" +
                    "Row #17: 149.82%\n" +
                    "Row #18: 3,684.90\n" +
                    "Row #18: 9,200.76\n" +
                    "Row #18: 149.69%\n" +
                    "Row #19: 5,827.58\n" +
                    "Row #19: 14,550.05\n" +
                    "Row #19: 149.68%\n" +
                    "Row #20: 12,228.85\n" +
                    "Row #20: 30,508.85\n" +
                    "Row #20: 149.48%\n" +
                    "Row #21: 2,830.92\n" +
                    "Row #21: 7,058.60\n" +
                    "Row #21: 149.34%\n" +
                    "Row #22: 1,525.04\n" +
                    "Row #22: 3,767.71\n" +
                    "Row #22: 147.06%\n"),

            new QueryAndResult("with" + nl +
                    "   member [Product].[All Products].[Drink].[Percent of Alcoholic Drinks]" +
                    " as '[Product].[All Products].[Drink].[Alcoholic Beverages]/" +
                    "[Product].[All Products].[Drink]', format = '#.00%'" + nl +
                    "select" + nl +
                    "   { [Product].[All Products].[Drink].[Percent of Alcoholic Drinks] }" +
                    " on columns," + nl +
                    "   order([Customers].[All Customers].[USA].[WA].Children," +
                    " [Product].[All Products].[Drink].[Percent of Alcoholic Drinks],BDESC )" +
                    " on rows" + nl +
                    "from Sales" + nl +
                    "where ( [Measures].[Unit Sales] )", "Axis #0:\n" +
                    "{[Measures].[Unit Sales]}\n" +
                    "Axis #1:\n" +
                    "{[Product].[All Products].[Drink].[Percent of Alcoholic Drinks]}\n" +
                    "Axis #2:\n" +
                    "{[Customers].[All Customers].[USA].[WA].[Seattle]}\n" +
                    "{[Customers].[All Customers].[USA].[WA].[Kirkland]}\n" +
                    "{[Customers].[All Customers].[USA].[WA].[Marysville]}\n" +
                    "{[Customers].[All Customers].[USA].[WA].[Anacortes]}\n" +
                    "{[Customers].[All Customers].[USA].[WA].[Olympia]}\n" +
                    "{[Customers].[All Customers].[USA].[WA].[Ballard]}\n" +
                    "{[Customers].[All Customers].[USA].[WA].[Bremerton]}\n" +
                    "{[Customers].[All Customers].[USA].[WA].[Puyallup]}\n" +
                    "{[Customers].[All Customers].[USA].[WA].[Yakima]}\n" +
                    "{[Customers].[All Customers].[USA].[WA].[Tacoma]}\n" +
                    "{[Customers].[All Customers].[USA].[WA].[Everett]}\n" +
                    "{[Customers].[All Customers].[USA].[WA].[Renton]}\n" +
                    "{[Customers].[All Customers].[USA].[WA].[Issaquah]}\n" +
                    "{[Customers].[All Customers].[USA].[WA].[Bellingham]}\n" +
                    "{[Customers].[All Customers].[USA].[WA].[Port Orchard]}\n" +
                    "{[Customers].[All Customers].[USA].[WA].[Redmond]}\n" +
                    "{[Customers].[All Customers].[USA].[WA].[Spokane]}\n" +
                    "{[Customers].[All Customers].[USA].[WA].[Burien]}\n" +
                    "{[Customers].[All Customers].[USA].[WA].[Lynnwood]}\n" +
                    "{[Customers].[All Customers].[USA].[WA].[Walla Walla]}\n" +
                    "{[Customers].[All Customers].[USA].[WA].[Edmonds]}\n" +
                    "{[Customers].[All Customers].[USA].[WA].[Sedro Woolley]}\n" +
                    "Row #0: 44.05%\n" +
                    "Row #1: 34.41%\n" +
                    "Row #2: 34.20%\n" +
                    "Row #3: 32.93%\n" +
                    "Row #4: 31.05%\n" +
                    "Row #5: 30.84%\n" +
                    "Row #6: 30.69%\n" +
                    "Row #7: 29.81%\n" +
                    "Row #8: 28.82%\n" +
                    "Row #9: 28.70%\n" +
                    "Row #10: 28.37%\n" +
                    "Row #11: 26.67%\n" +
                    "Row #12: 26.60%\n" +
                    "Row #13: 26.47%\n" +
                    "Row #14: 26.42%\n" +
                    "Row #15: 26.28%\n" +
                    "Row #16: 25.96%\n" +
                    "Row #17: 24.70%\n" +
                    "Row #18: 21.89%\n" +
                    "Row #19: 21.47%\n" +
                    "Row #20: 17.47%\n" +
                    "Row #21: 13.79%\n"),

            new QueryAndResult("with member [Measures].[Accumulated Sales] as " +
                    "'Sum(YTD(),[Measures].[Store Sales])'" + nl +
                    "select" + nl +
                    "    {[Measures].[Store Sales],[Measures].[Accumulated Sales]}" +
                    " on columns," + nl +
                    "    {Descendants([Time].[1997],[Time].[Month])} on rows" + nl +
                    "from Sales", "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Store Sales]}\n" +
                    "{[Measures].[Accumulated Sales]}\n" +
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
                    "Row #0: 45,539.69\n" +
                    "Row #1: 44,058.79\n" +
                    "Row #1: 89,598.48\n" +
                    "Row #2: 50,029.87\n" +
                    "Row #2: 139,628.35\n" +
                    "Row #3: 42,878.25\n" +
                    "Row #3: 182,506.60\n" +
                    "Row #4: 44,456.29\n" +
                    "Row #4: 226,962.89\n" +
                    "Row #5: 45,331.73\n" +
                    "Row #5: 272,294.62\n" +
                    "Row #6: 50,246.88\n" +
                    "Row #6: 322,541.50\n" +
                    "Row #7: 46,199.04\n" +
                    "Row #7: 368,740.54\n" +
                    "Row #8: 43,825.97\n" +
                    "Row #8: 412,566.51\n" +
                    "Row #9: 42,342.27\n" +
                    "Row #9: 454,908.78\n" +
                    "Row #10: 53,363.71\n" +
                    "Row #10: 508,272.49\n" +
                    "Row #11: 56,965.64\n" +
                    "Row #11: 565,238.13\n"),

            new QueryAndResult("select" + nl +
                    " {[Measures].[Unit Sales]} on columns," + nl +
                    " [Gender].members on rows" + nl +
                    "from Sales", "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Unit Sales]}\n" +
                    "Axis #2:\n" +
                    "{[Gender].[All Gender]}\n" +
                    "{[Gender].[All Gender].[F]}\n" +
                    "{[Gender].[All Gender].[M]}\n" +
                    "Row #0: 266,773\n" +
                    "Row #1: 131,558\n" +
                    "Row #2: 135,215\n"),

            new QueryAndResult("WITH" + nl +
                    "   MEMBER [Measures].[StoreType] AS " + nl +
                    "   '[Store].CurrentMember.Properties(\"Store Type\")'," + nl +
                    "   SOLVE_ORDER = 2" + nl +
                    "   MEMBER [Measures].[ProfitPct] AS " + nl +
                    "   '((Measures.[Store Sales] - Measures.[Store Cost]) /" +
                    " Measures.[Store Sales])'," + nl +
                    "   SOLVE_ORDER = 1, FORMAT_STRING = 'Percent'" + nl +
                    "SELECT non empty" + nl +
                    "   { [Store].[Store Name].Members} ON COLUMNS," + nl +
                    "   { [Measures].[Store Sales], [Measures].[Store Cost]," +
                    " [Measures].[StoreType]," + nl +
                    "   [Measures].[ProfitPct] } ON ROWS" + nl +
                    "FROM Sales", "Axis #0:\n" +
                    "{}\n" +
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
                    "{[Measures].[Store Sales]}\n" +
                    "{[Measures].[Store Cost]}\n" +
                    "{[Measures].[StoreType]}\n" +
                    "{[Measures].[ProfitPct]}\n" +
                    "Row #0: 45,750.24\n" +
                    "Row #0: 54,545.28\n" +
                    "Row #0: 54,431.14\n" +
                    "Row #0: 4,441.18\n" +
                    "Row #0: 55,058.79\n" +
                    "Row #0: 87,218.28\n" +
                    "Row #0: 4,739.23\n" +
                    "Row #0: 52,896.30\n" +
                    "Row #0: 52,644.07\n" +
                    "Row #0: 49,634.46\n" +
                    "Row #0: 74,843.96\n" +
                    "Row #0: 4,705.97\n" +
                    "Row #0: 24,329.23\n" +
                    "Row #1: 18,266.44\n" +
                    "Row #1: 21,771.54\n" +
                    "Row #1: 21,713.53\n" +
                    "Row #1: 1,778.92\n" +
                    "Row #1: 21,948.94\n" +
                    "Row #1: 34,823.56\n" +
                    "Row #1: 1,896.62\n" +
                    "Row #1: 21,121.96\n" +
                    "Row #1: 20,956.80\n" +
                    "Row #1: 19,795.49\n" +
                    "Row #1: 29,959.28\n" +
                    "Row #1: 1,880.34\n" +
                    "Row #1: 9,713.81\n" +
                    "Row #2: Gourmet Supermarket\n" +
                    "Row #2: Supermarket\n" +
                    "Row #2: Supermarket\n" +
                    "Row #2: Small Grocery\n" +
                    "Row #2: Supermarket\n" +
                    "Row #2: Deluxe Supermarket\n" +
                    "Row #2: Small Grocery\n" +
                    "Row #2: Supermarket\n" +
                    "Row #2: Supermarket\n" +
                    "Row #2: Supermarket\n" +
                    "Row #2: Deluxe Supermarket\n" +
                    "Row #2: Small Grocery\n" +
                    "Row #2: Mid-Size Grocery\n" +
                    "Row #3: 60.07%\n" +
                    "Row #3: 60.09%\n" +
                    "Row #3: 60.11%\n" +
                    "Row #3: 59.94%\n" +
                    "Row #3: 60.14%\n" +
                    "Row #3: 60.07%\n" +
                    "Row #3: 59.98%\n" +
                    "Row #3: 60.07%\n" +
                    "Row #3: 60.19%\n" +
                    "Row #3: 60.12%\n" +
                    "Row #3: 59.97%\n" +
                    "Row #3: 60.04%\n" +
                    "Row #3: 60.07%\n"),

            new QueryAndResult("WITH" + nl +
                    "   MEMBER [Product].[All Products].[Drink].[Alcoholic Beverages]" +
                    ".[Beer and Wine].[BigSeller] AS" + nl +
                    "  'IIf([Product].[All Products].[Drink].[Alcoholic Beverages]" +
                    ".[Beer and Wine] > 100, \"Yes\",\"No\")'" + nl +
                    "SELECT" + nl +
                    "   {[Product].[All Products].[Drink].[Alcoholic Beverages]" +
                    ".[Beer and Wine].[BigSeller]} ON COLUMNS," + nl +
                    "   {Store.[Store Name].Members} ON ROWS" + nl +
                    "FROM Sales", "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Product].[All Products].[Drink].[Alcoholic Beverages]" +
                    ".[Beer and Wine].[BigSeller]}\n" +
                    "Axis #2:\n" +
                    "{[Store].[All Stores].[Canada].[BC].[Vancouver].[Store 19]}\n" +
                    "{[Store].[All Stores].[Canada].[BC].[Victoria].[Store 20]}\n" +
                    "{[Store].[All Stores].[Mexico].[DF].[Mexico City].[Store 9]}\n" +
                    "{[Store].[All Stores].[Mexico].[DF].[San Andres].[Store 21]}\n" +
                    "{[Store].[All Stores].[Mexico].[Guerrero].[Acapulco].[Store 1]}\n" +
                    "{[Store].[All Stores].[Mexico].[Jalisco].[Guadalajara].[Store 5]}\n" +
                    "{[Store].[All Stores].[Mexico].[Veracruz].[Orizaba].[Store 10]}\n" +
                    "{[Store].[All Stores].[Mexico].[Yucatan].[Merida].[Store 8]}\n" +
                    "{[Store].[All Stores].[Mexico].[Zacatecas].[Camacho].[Store 4]}\n" +
                    "{[Store].[All Stores].[Mexico].[Zacatecas].[Hidalgo].[Store 12]}\n" +
                    "{[Store].[All Stores].[Mexico].[Zacatecas].[Hidalgo].[Store 18]}\n" +
                    "{[Store].[All Stores].[USA].[CA].[Alameda].[HQ]}\n" +
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
                    "Row #0: No\n" +
                    "Row #1: No\n" +
                    "Row #2: No\n" +
                    "Row #3: No\n" +
                    "Row #4: No\n" +
                    "Row #5: No\n" +
                    "Row #6: No\n" +
                    "Row #7: No\n" +
                    "Row #8: No\n" +
                    "Row #9: No\n" +
                    "Row #10: No\n" +
                    "Row #11: No\n" +
                    "Row #12: Yes\n" +
                    "Row #13: Yes\n" +
                    "Row #14: Yes\n" +
                    "Row #15: No\n" +
                    "Row #16: Yes\n" +
                    "Row #17: Yes\n" +
                    "Row #18: No\n" +
                    "Row #19: Yes\n" +
                    "Row #20: Yes\n" +
                    "Row #21: Yes\n" +
                    "Row #22: Yes\n" +
                    "Row #23: No\n" +
                    "Row #24: Yes\n"),

            new QueryAndResult("WITH" + nl +
                    "   MEMBER [Measures].[ProfitPct] AS " + nl +
                    "   '((Measures.[Store Sales] - Measures.[Store Cost]) /" +
                    " Measures.[Store Sales])'," + nl +
                    "   SOLVE_ORDER = 1, FORMAT_STRING = 'Percent'" + nl +
                    "   MEMBER [Measures].[ProfitValue] AS " + nl +
                    "   '[Measures].[Store Sales] * [Measures].[ProfitPct]'," + nl +
                    "   SOLVE_ORDER = 2, FORMAT_STRING = 'Currency'" + nl +
                    "SELECT non empty " + nl +
                    "   { [Store].[Store Name].Members} ON COLUMNS," + nl +
                    "   { [Measures].[Store Sales], [Measures].[Store Cost]," +
                    " [Measures].[ProfitValue]," + nl +
                    "   [Measures].[ProfitPct] } ON ROWS" + nl +
                    "FROM Sales", "Axis #0:\n" +
                    "{}\n" +
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
                    "{[Measures].[Store Sales]}\n" +
                    "{[Measures].[Store Cost]}\n" +
                    "{[Measures].[ProfitValue]}\n" +
                    "{[Measures].[ProfitPct]}\n" +
                    "Row #0: 45,750.24\n" +
                    "Row #0: 54,545.28\n" +
                    "Row #0: 54,431.14\n" +
                    "Row #0: 4,441.18\n" +
                    "Row #0: 55,058.79\n" +
                    "Row #0: 87,218.28\n" +
                    "Row #0: 4,739.23\n" +
                    "Row #0: 52,896.30\n" +
                    "Row #0: 52,644.07\n" +
                    "Row #0: 49,634.46\n" +
                    "Row #0: 74,843.96\n" +
                    "Row #0: 4,705.97\n" +
                    "Row #0: 24,329.23\n" +
                    "Row #1: 18,266.44\n" +
                    "Row #1: 21,771.54\n" +
                    "Row #1: 21,713.53\n" +
                    "Row #1: 1,778.92\n" +
                    "Row #1: 21,948.94\n" +
                    "Row #1: 34,823.56\n" +
                    "Row #1: 1,896.62\n" +
                    "Row #1: 21,121.96\n" +
                    "Row #1: 20,956.80\n" +
                    "Row #1: 19,795.49\n" +
                    "Row #1: 29,959.28\n" +
                    "Row #1: 1,880.34\n" +
                    "Row #1: 9,713.81\n" +
                    "Row #2: $27,483.80\n" +
                    "Row #2: $32,773.74\n" +
                    "Row #2: $32,717.61\n" +
                    "Row #2: $2,662.26\n" +
                    "Row #2: $33,109.85\n" +
                    "Row #2: $52,394.72\n" +
                    "Row #2: $2,842.61\n" +
                    "Row #2: $31,774.34\n" +
                    "Row #2: $31,687.27\n" +
                    "Row #2: $29,838.97\n" +
                    "Row #2: $44,884.68\n" +
                    "Row #2: $2,825.63\n" +
                    "Row #2: $14,615.42\n" +
                    "Row #3: 60.07%\n" +
                    "Row #3: 60.09%\n" +
                    "Row #3: 60.11%\n" +
                    "Row #3: 59.94%\n" +
                    "Row #3: 60.14%\n" +
                    "Row #3: 60.07%\n" +
                    "Row #3: 59.98%\n" +
                    "Row #3: 60.07%\n" +
                    "Row #3: 60.19%\n" +
                    "Row #3: 60.12%\n" +
                    "Row #3: 59.97%\n" +
                    "Row #3: 60.04%\n" +
                    "Row #3: 60.07%\n"),

            new QueryAndResult("WITH MEMBER MEASURES.ProfitPercent AS" + nl +
                    "     '([Measures].[Store Sales]-[Measures].[Store Cost])/" +
                    "([Measures].[Store Cost])'," + nl +
                    " FORMAT_STRING = '#.00%', SOLVE_ORDER = 1" + nl +
                    " MEMBER [Time].[First Half 97] AS  '[Time].[1997].[Q1] +" +
                    " [Time].[1997].[Q2]'" + nl +
                    " MEMBER [Time].[Second Half 97] AS '[Time].[1997].[Q3] +" +
                    " [Time].[1997].[Q4]'" + nl +
                    " SELECT {[Time].[First Half 97]," + nl +
                    "     [Time].[Second Half 97]," + nl +
                    "     [Time].[1997].CHILDREN} ON COLUMNS," + nl +
                    " {[Store].[Store Country].[USA].CHILDREN} ON ROWS" + nl +
                    " FROM [Sales]" + nl +
                    " WHERE ([Measures].[ProfitPercent])", "Axis #0:\n" +
                    "{[Measures].[ProfitPercent]}\n" +
                    "Axis #1:\n" +
                    "{[Time].[First Half 97]}\n" +
                    "{[Time].[Second Half 97]}\n" +
                    "{[Time].[1997].[Q1]}\n" +
                    "{[Time].[1997].[Q2]}\n" +
                    "{[Time].[1997].[Q3]}\n" +
                    "{[Time].[1997].[Q4]}\n" +
                    "Axis #2:\n" +
                    "{[Store].[All Stores].[USA].[CA]}\n" +
                    "{[Store].[All Stores].[USA].[OR]}\n" +
                    "{[Store].[All Stores].[USA].[WA]}\n" +
                    "Row #0: 150.55%\n" +
                    "Row #0: 150.53%\n" +
                    "Row #0: 150.68%\n" +
                    "Row #0: 150.44%\n" +
                    "Row #0: 151.35%\n" +
                    "Row #0: 149.81%\n" +
                    "Row #1: 150.15%\n" +
                    "Row #1: 151.08%\n" +
                    "Row #1: 149.80%\n" +
                    "Row #1: 150.60%\n" +
                    "Row #1: 151.37%\n" +
                    "Row #1: 150.78%\n" +
                    "Row #2: 150.59%\n" +
                    "Row #2: 150.34%\n" +
                    "Row #2: 150.72%\n" +
                    "Row #2: 150.45%\n" +
                    "Row #2: 150.39%\n" +
                    "Row #2: 150.29%\n"),

            new QueryAndResult("with member [Measures].[Accumulated Sales] as" +
                    " 'Sum(YTD(),[Measures].[Store Sales])'" + nl +
                    "select" + nl +
                    "    {[Measures].[Store Sales],[Measures].[Accumulated Sales]}" +
                    " on columns," + nl +
                    "    {Descendants([Time].[1997],[Time].[Month])} on rows" + nl +
                    "from [Warehouse and Sales]", "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Store Sales]}\n" +
                    "{[Measures].[Accumulated Sales]}\n" +
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
                    "Row #0: 45,539.69\n" +
                    "Row #1: 44,058.79\n" +
                    "Row #1: 89,598.48\n" +
                    "Row #2: 50,029.87\n" +
                    "Row #2: 139,628.35\n" +
                    "Row #3: 42,878.25\n" +
                    "Row #3: 182,506.60\n" +
                    "Row #4: 44,456.29\n" +
                    "Row #4: 226,962.89\n" +
                    "Row #5: 45,331.73\n" +
                    "Row #5: 272,294.62\n" +
                    "Row #6: 50,246.88\n" +
                    "Row #6: 322,541.50\n" +
                    "Row #7: 46,199.04\n" +
                    "Row #7: 368,740.54\n" +
                    "Row #8: 43,825.97\n" +
                    "Row #8: 412,566.51\n" +
                    "Row #9: 42,342.27\n" +
                    "Row #9: 454,908.78\n" +
                    "Row #10: 53,363.71\n" +
                    "Row #10: 508,272.49\n" +
                    "Row #11: 56,965.64\n" +
                    "Row #11: 565,238.13\n"),

//        // Virtual cube. Note that Unit Sales is independent of Warehouse.
            new QueryAndResult("select non empty CrossJoin(\r\n" +
                    "  {[Warehouse].DefaultMember, [Warehouse].[USA].children}," + nl +
                    " {[Measures].[Unit Sales], [Measures].[Units Shipped]}) on" +
                    " columns," + nl +
                    " [Time].children on rows" + nl +
                    "from [Warehouse and Sales]", "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Warehouse].[All Warehouses], [Measures].[Unit Sales]}\n" +
                    "{[Warehouse].[All Warehouses], [Measures].[Units Shipped]}\n" +
                    "{[Warehouse].[All Warehouses].[USA].[CA], [Measures]" +
                    ".[Units Shipped]}\n" +
                    "{[Warehouse].[All Warehouses].[USA].[OR], [Measures]" +
                    ".[Units Shipped]}\n" +
                    "{[Warehouse].[All Warehouses].[USA].[WA], [Measures]" +
                    ".[Units Shipped]}\n" +
                    "Axis #2:\n" +
                    "{[Time].[1997].[Q1]}\n" +
                    "{[Time].[1997].[Q2]}\n" +
                    "{[Time].[1997].[Q3]}\n" +
                    "{[Time].[1997].[Q4]}\n" +
                    "Row #0: 66,291\n" +
                    "Row #0: 50951.0\n" +
                    "Row #0: 8539.0\n" +
                    "Row #0: 7994.0\n" +
                    "Row #0: 34418.0\n" +
                    "Row #1: 62,610\n" +
                    "Row #1: 49187.0\n" +
                    "Row #1: 15726.0\n" +
                    "Row #1: 7575.0\n" +
                    "Row #1: 25886.0\n" +
                    "Row #2: 65,848\n" +
                    "Row #2: 57789.0\n" +
                    "Row #2: 20821.0\n" +
                    "Row #2: 8673.0\n" +
                    "Row #2: 28295.0\n" +
                    "Row #3: 72,024\n" +
                    "Row #3: 49799.0\n" +
                    "Row #3: 15791.0\n" +
                    "Row #3: 16666.0\n" +
                    "Row #3: 17342.0\n"),

            // should allow dimension to be used as shorthand for member
            new QueryAndResult("select {[Measures].[Unit Sales]} on columns," + nl +
                    " {[Store], [Store].children} on rows" + nl +
                    "from [Sales]", "Axis #0:\n" +
                    "{}\n" +
                    "Axis #1:\n" +
                    "{[Measures].[Unit Sales]}\n" +
                    "Axis #2:\n" +
                    "{[Store].[All Stores]}\n" +
                    "{[Store].[All Stores].[Canada]}\n" +
                    "{[Store].[All Stores].[Mexico]}\n" +
                    "{[Store].[All Stores].[USA]}\n" +
                    "Row #0: 266,773\n" +
                    "Row #1: \n" +
                    "Row #2: \n" +
                    "Row #3: 266,773\n"),

            // crossjoins on rows and columns, and a slicer
            new QueryAndResult("select" + nl +
                    "  CrossJoin(" + nl +
                    "    {[Measures].[Unit Sales], [Measures].[Store Sales]}," + nl +
                    "    {[Time].[1997].[Q2].children}) on columns, " + nl +
                    "  CrossJoin(" + nl +
                    "    CrossJoin(" + nl +
                    "      [Gender].members," + nl +
                    "      [Marital Status].members)," + nl +
                    "   {[Store], [Store].children}) on rows" + nl +
                    "from [Sales]" + nl +
                    "where (" + nl +
                    " [Product].[Food]," + nl +
                    " [Education Level].[High School Degree]," + nl +
                    " [Promotions].DefaultMember)",
                    "Axis #0:\n" +
                            "{[Product].[All Products].[Food], [Education Level]" +
                            ".[All Education Levels].[High School Degree], " +
                            "[Promotions].[All Promotions]}\n" +
                            "Axis #1:\n" +
                            "{[Measures].[Unit Sales], [Time].[1997].[Q2].[4]}\n" +
                            "{[Measures].[Unit Sales], [Time].[1997].[Q2].[5]}\n" +
                            "{[Measures].[Unit Sales], [Time].[1997].[Q2].[6]}\n" +
                            "{[Measures].[Store Sales], [Time].[1997].[Q2].[4]}\n" +
                            "{[Measures].[Store Sales], [Time].[1997].[Q2].[5]}\n" +
                            "{[Measures].[Store Sales], [Time].[1997].[Q2].[6]}\n" +
                            "Axis #2:\n" +
                            "{[Gender].[All Gender], [Marital Status].[All Marital Status], " +
                            "[Store].[All Stores]}\n" +
                            "{[Gender].[All Gender], [Marital Status].[All Marital Status], " +
                            "[Store].[All Stores].[Canada]}\n" +
                            "{[Gender].[All Gender], [Marital Status].[All Marital Status], " +
                            "[Store].[All Stores].[Mexico]}\n" +
                            "{[Gender].[All Gender], [Marital Status].[All Marital Status], " +
                            "[Store].[All Stores].[USA]}\n" +
                            "{[Gender].[All Gender], [Marital Status].[All Marital Status].[M], " +
                            "[Store].[All Stores]}\n" +
                            "{[Gender].[All Gender], [Marital Status].[All Marital Status].[M], " +
                            "[Store].[All Stores].[Canada]}\n" +
                            "{[Gender].[All Gender], [Marital Status].[All Marital Status].[M], " +
                            "[Store].[All Stores].[Mexico]}\n" +
                            "{[Gender].[All Gender], [Marital Status].[All Marital Status].[M], " +
                            "[Store].[All Stores].[USA]}\n" +
                            "{[Gender].[All Gender], [Marital Status].[All Marital Status].[S], " +
                            "[Store].[All Stores]}\n" +
                            "{[Gender].[All Gender], [Marital Status].[All Marital Status].[S], " +
                            "[Store].[All Stores].[Canada]}\n" +
                            "{[Gender].[All Gender], [Marital Status].[All Marital Status].[S], " +
                            "[Store].[All Stores].[Mexico]}\n" +
                            "{[Gender].[All Gender], [Marital Status].[All Marital Status].[S], " +
                            "[Store].[All Stores].[USA]}\n" +
                            "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status], " +
                            "[Store].[All Stores]}\n" +
                            "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status], " +
                            "[Store].[All Stores].[Canada]}\n" +
                            "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status], " +
                            "[Store].[All Stores].[Mexico]}\n" +
                            "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status], " +
                            "[Store].[All Stores].[USA]}\n" +
                            "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[M], " +
                            "[Store].[All Stores]}\n" +
                            "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[M], " +
                            "[Store].[All Stores].[Canada]}\n" +
                            "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[M], " +
                            "[Store].[All Stores].[Mexico]}\n" +
                            "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[M], " +
                            "[Store].[All Stores].[USA]}\n" +
                            "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S], " +
                            "[Store].[All Stores]}\n" +
                            "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S], " +
                            "[Store].[All Stores].[Canada]}\n" +
                            "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S], " +
                            "[Store].[All Stores].[Mexico]}\n" +
                            "{[Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S], " +
                            "[Store].[All Stores].[USA]}\n" +
                            "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status], " +
                            "[Store].[All Stores]}\n" +
                            "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status], " +
                            "[Store].[All Stores].[Canada]}\n" +
                            "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status], " +
                            "[Store].[All Stores].[Mexico]}\n" +
                            "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status], " +
                            "[Store].[All Stores].[USA]}\n" +
                            "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M], " +
                            "[Store].[All Stores]}\n" +
                            "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M], " +
                            "[Store].[All Stores].[Canada]}\n" +
                            "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M], " +
                            "[Store].[All Stores].[Mexico]}\n" +
                            "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[M], " +
                            "[Store].[All Stores].[USA]}\n" +
                            "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], " +
                            "[Store].[All Stores]}\n" +
                            "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], " +
                            "[Store].[All Stores].[Canada]}\n" +
                            "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], " +
                            "[Store].[All Stores].[Mexico]}\n" +
                            "{[Gender].[All Gender].[M], [Marital Status].[All Marital Status].[S], " +
                            "[Store].[All Stores].[USA]}\n" +
                            "Row #0: 4,284\n" +
                            "Row #0: 3,972\n" +
                            "Row #0: 4,476\n" +
                            "Row #0: 9,014.60\n" +
                            "Row #0: 8,595.99\n" +
                            "Row #0: 9,480.21\n" +
                            "Row #1: \n" +
                            "Row #1: \n" +
                            "Row #1: \n" +
                            "Row #1: \n" +
                            "Row #1: \n" +
                            "Row #1: \n" +
                            "Row #2: \n" +
                            "Row #2: \n" +
                            "Row #2: \n" +
                            "Row #2: \n" +
                            "Row #2: \n" +
                            "Row #2: \n" +
                            "Row #3: 4,284\n" +
                            "Row #3: 3,972\n" +
                            "Row #3: 4,476\n" +
                            "Row #3: 9,014.60\n" +
                            "Row #3: 8,595.99\n" +
                            "Row #3: 9,480.21\n" +
                            "Row #4: 1,942\n" +
                            "Row #4: 1,843\n" +
                            "Row #4: 2,128\n" +
                            "Row #4: 4,133.87\n" +
                            "Row #4: 4,007.53\n" +
                            "Row #4: 4,541.97\n" +
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
                            "Row #7: 1,942\n" +
                            "Row #7: 1,843\n" +
                            "Row #7: 2,128\n" +
                            "Row #7: 4,133.87\n" +
                            "Row #7: 4,007.53\n" +
                            "Row #7: 4,541.97\n" +
                            "Row #8: 2,342\n" +
                            "Row #8: 2,129\n" +
                            "Row #8: 2,348\n" +
                            "Row #8: 4,880.73\n" +
                            "Row #8: 4,588.46\n" +
                            "Row #8: 4,938.24\n" +
                            "Row #9: \n" +
                            "Row #9: \n" +
                            "Row #9: \n" +
                            "Row #9: \n" +
                            "Row #9: \n" +
                            "Row #9: \n" +
                            "Row #10: \n" +
                            "Row #10: \n" +
                            "Row #10: \n" +
                            "Row #10: \n" +
                            "Row #10: \n" +
                            "Row #10: \n" +
                            "Row #11: 2,342\n" +
                            "Row #11: 2,129\n" +
                            "Row #11: 2,348\n" +
                            "Row #11: 4,880.73\n" +
                            "Row #11: 4,588.46\n" +
                            "Row #11: 4,938.24\n" +
                            "Row #12: 2,093\n" +
                            "Row #12: 1,981\n" +
                            "Row #12: 1,918\n" +
                            "Row #12: 4,380.67\n" +
                            "Row #12: 4,338.31\n" +
                            "Row #12: 4,130.34\n" +
                            "Row #13: \n" +
                            "Row #13: \n" +
                            "Row #13: \n" +
                            "Row #13: \n" +
                            "Row #13: \n" +
                            "Row #13: \n" +
                            "Row #14: \n" +
                            "Row #14: \n" +
                            "Row #14: \n" +
                            "Row #14: \n" +
                            "Row #14: \n" +
                            "Row #14: \n" +
                            "Row #15: 2,093\n" +
                            "Row #15: 1,981\n" +
                            "Row #15: 1,918\n" +
                            "Row #15: 4,380.67\n" +
                            "Row #15: 4,338.31\n" +
                            "Row #15: 4,130.34\n" +
                            "Row #16: 901\n" +
                            "Row #16: 942\n" +
                            "Row #16: 837\n" +
                            "Row #16: 1,905.00\n" +
                            "Row #16: 2,069.44\n" +
                            "Row #16: 1,865.44\n" +
                            "Row #17: \n" +
                            "Row #17: \n" +
                            "Row #17: \n" +
                            "Row #17: \n" +
                            "Row #17: \n" +
                            "Row #17: \n" +
                            "Row #18: \n" +
                            "Row #18: \n" +
                            "Row #18: \n" +
                            "Row #18: \n" +
                            "Row #18: \n" +
                            "Row #18: \n" +
                            "Row #19: 901\n" +
                            "Row #19: 942\n" +
                            "Row #19: 837\n" +
                            "Row #19: 1,905.00\n" +
                            "Row #19: 2,069.44\n" +
                            "Row #19: 1,865.44\n" +
                            "Row #20: 1,192\n" +
                            "Row #20: 1,039\n" +
                            "Row #20: 1,081\n" +
                            "Row #20: 2,475.67\n" +
                            "Row #20: 2,268.87\n" +
                            "Row #20: 2,264.90\n" +
                            "Row #21: \n" +
                            "Row #21: \n" +
                            "Row #21: \n" +
                            "Row #21: \n" +
                            "Row #21: \n" +
                            "Row #21: \n" +
                            "Row #22: \n" +
                            "Row #22: \n" +
                            "Row #22: \n" +
                            "Row #22: \n" +
                            "Row #22: \n" +
                            "Row #22: \n" +
                            "Row #23: 1,192\n" +
                            "Row #23: 1,039\n" +
                            "Row #23: 1,081\n" +
                            "Row #23: 2,475.67\n" +
                            "Row #23: 2,268.87\n" +
                            "Row #23: 2,264.90\n" +
                            "Row #24: 2,191\n" +
                            "Row #24: 1,991\n" +
                            "Row #24: 2,558\n" +
                            "Row #24: 4,633.93\n" +
                            "Row #24: 4,257.68\n" +
                            "Row #24: 5,349.87\n" +
                            "Row #25: \n" +
                            "Row #25: \n" +
                            "Row #25: \n" +
                            "Row #25: \n" +
                            "Row #25: \n" +
                            "Row #25: \n" +
                            "Row #26: \n" +
                            "Row #26: \n" +
                            "Row #26: \n" +
                            "Row #26: \n" +
                            "Row #26: \n" +
                            "Row #26: \n" +
                            "Row #27: 2,191\n" +
                            "Row #27: 1,991\n" +
                            "Row #27: 2,558\n" +
                            "Row #27: 4,633.93\n" +
                            "Row #27: 4,257.68\n" +
                            "Row #27: 5,349.87\n" +
                            "Row #28: 1,041\n" +
                            "Row #28: 901\n" +
                            "Row #28: 1,291\n" +
                            "Row #28: 2,228.87\n" +
                            "Row #28: 1,938.09\n" +
                            "Row #28: 2,676.53\n" +
                            "Row #29: \n" +
                            "Row #29: \n" +
                            "Row #29: \n" +
                            "Row #29: \n" +
                            "Row #29: \n" +
                            "Row #29: \n" +
                            "Row #30: \n" +
                            "Row #30: \n" +
                            "Row #30: \n" +
                            "Row #30: \n" +
                            "Row #30: \n" +
                            "Row #30: \n" +
                            "Row #31: 1,041\n" +
                            "Row #31: 901\n" +
                            "Row #31: 1,291\n" +
                            "Row #31: 2,228.87\n" +
                            "Row #31: 1,938.09\n" +
                            "Row #31: 2,676.53\n" +
                            "Row #32: 1,150\n" +
                            "Row #32: 1,090\n" +
                            "Row #32: 1,267\n" +
                            "Row #32: 2,405.06\n" +
                            "Row #32: 2,319.59\n" +
                            "Row #32: 2,673.34\n" +
                            "Row #33: \n" +
                            "Row #33: \n" +
                            "Row #33: \n" +
                            "Row #33: \n" +
                            "Row #33: \n" +
                            "Row #33: \n" +
                            "Row #34: \n" +
                            "Row #34: \n" +
                            "Row #34: \n" +
                            "Row #34: \n" +
                            "Row #34: \n" +
                            "Row #34: \n" +
                            "Row #35: 1,150\n" +
                            "Row #35: 1,090\n" +
                            "Row #35: 1,267\n" +
                            "Row #35: 2,405.06\n" +
                            "Row #35: 2,319.59\n" +
                            "Row #35: 2,673.34\n"),

            new QueryAndResult("select from [Warehouse and Sales]",
                    "Axis #0:\n" +
                            "{}\n" +
                            "565,238.13"),

    };

    public void testConcurrentValidatingQueriesInRandomOrder() {
        props.DisableCaching.set(false);
        props.UseAggregates.set(false);
        props.ReadAggregates.set(false);

        FoodMartTestCase.QueryAndResult[] singleQuery = {mdxQueries[0]};
        assertTrue(ConcurrentValidatingQueryRunner.runTest(1, 1, false, true,
                singleQuery).size() == 0);
        //ensures same global aggregation is used by 2 or more threads and
        // all of them load the same segment.
        FoodMartTestCase.QueryAndResult[] singleQueryFor2Threads = {mdxQueries[1]};
        assertTrue(ConcurrentValidatingQueryRunner.runTest(2, 5, false, true,
                singleQueryFor2Threads).size() == 0);

        assertTrue(ConcurrentValidatingQueryRunner.runTest(10, 45, true, true,
                mdxQueries).size() == 0);
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    protected void setUp() throws Exception {
        super.setUp();
        props = MondrianProperties.instance();
    }
}
// End ConcurrentMdxTest.java