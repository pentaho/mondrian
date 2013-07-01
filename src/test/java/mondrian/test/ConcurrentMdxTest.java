/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde
// Copyright (C) 2005-2013 Pentaho and others
// All Rights Reserved.
*/
package mondrian.test;

import mondrian.olap.*;

import org.apache.log4j.Logger;

import org.olap4j.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Runs specified set of MDX queries concurrently.
 * This Class is not added to the Main test suite.
 * Purpose of this test is to simulate Concurrent access to Aggregation and data
 * load. Simulation will be more effective if we run this single test again and
 * again with a fresh connection.
 *
 * @author Thiyagu,Ajit
 */
public class ConcurrentMdxTest extends FoodMartTestCase {
    private static final Logger LOGGER =
        Logger.getLogger(FoodMartTestCase.class);

    static final QueryAndResult[] mdxQueries = {
        new QueryAndResult(
            "select {[Measures].[Sales Count]} on 0 from [Sales] ",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Sales Count]}\n"
            + "Row #0: 86,837\n"),

        new QueryAndResult(
            "select {[Measures].[Store Cost]} on 0 from [Sales] ",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Cost]}\n"
            + "Row #0: 225,627.23\n"),

        new QueryAndResult(
            "select {[Measures].[Sales Count], "
            + "[Measures].[Store Invoice]} on 0 from [Warehouse and Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Sales Count]}\n"
            + "{[Measures].[Store Invoice]}\n"
            + "Row #0: 86,837\n"
            + "Row #0: 102,278.409\n"),

        new QueryAndResult(
            "select {[Measures].[Sales Count], "
            + "[Measures].[Store Invoice]} on 0 from [Warehouse and Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Sales Count]}\n"
            + "{[Measures].[Store Invoice]}\n"
            + "Row #0: 86,837\n"
            + "Row #0: 102,278.409\n"),

        new QueryAndResult(
            "select {[Measures].[Sales Count], "
            + "[Measures].[Store Invoice]} on 0, "
            + "{[Time].[Time].[1997],[Time].[Time].[1998]} on 1 "
            + "from [Warehouse and Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Sales Count]}\n"
            + "{[Measures].[Store Invoice]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[1997]}\n"
            + "{[Time].[Time].[1998]}\n"
            + "Row #0: 86,837\n"
            + "Row #0: 102,278.409\n"
            + "Row #1: \n"
            + "Row #1: \n"),

        new QueryAndResult(
            "select {[Measures].[Sales Count], "
            + "[Measures].[Store Invoice]} on 0, "
            + "{([Gender].[Gender].[M],[Time].[Time].[1997])} on 1 "
            + "from [Warehouse and Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Sales Count]}\n"
            + "{[Measures].[Store Invoice]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Gender].[M], [Time].[Time].[1997]}\n"
            + "Row #0: 44,006\n"
            + "Row #0: \n"),

        new QueryAndResult(
            "select {[Measures].[Sales Count], "
            + "[Measures].[Store Invoice]} on 0, "
            + "{([Gender].[Gender].[F],[Time].[Time].[1997])} on 1 "
            + "from [Warehouse and Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Sales Count]}\n"
            + "{[Measures].[Store Invoice]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Gender].[F], [Time].[Time].[1997]}\n"
            + "Row #0: 42,831\n"
            + "Row #0: \n"),

        new QueryAndResult(
            "select {[Measures].[Store Cost], "
            + "[Measures].[Supply Time]} on 0 from [Warehouse and Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Cost]}\n"
            + "{[Measures].[Supply Time]}\n"
            + "Row #0: 225,627.23\n"
            + "Row #0: 10,425\n"),

        new QueryAndResult(
            "select {[Measures].[Store Sales], "
            + "[Measures].[Units Ordered]} on 0 from [Warehouse and Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Sales]}\n"
            + "{[Measures].[Units Ordered]}\n"
            + "Row #0: 565,238.13\n"
            + "Row #0: 227238.0\n"),

        new QueryAndResult(
            "select {[Measures].[Unit Sales], "
            + "[Measures].[Units Ordered]} on 0 from [Warehouse and Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Units Ordered]}\n"
            + "Row #0: 266,773\n"
            + "Row #0: 227238.0\n"),

        new QueryAndResult(
            "select {[Measures].[Profit], "
            + "[Measures].[Units Shipped]} on 0 from [Warehouse and Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Profit]}\n"
            + "{[Measures].[Units Shipped]}\n"
            + "Row #0: $339,610.90\n"
            + "Row #0: 207726.0\n"),

        new QueryAndResult(
            "select {[Measures].[Unit Sales]} on columns\n" + " from Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Row #0: 266,773\n"),

        new QueryAndResult(
            "select \n"
            + "{[Measures].[Unit Sales]} on columns,\n"
            + "order(except([Promotion].[Media Type].members,"
            + "{[Promotion].[Media Type].[No Media], [Promotion].[Media Type].[All Media]}),"
            + "[Measures].[Unit Sales],DESC) on rows\n"
            + "from Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Promotion].[Media Type].[Daily Paper, Radio, TV]}\n"
            + "{[Promotion].[Media Type].[Daily Paper]}\n"
            + "{[Promotion].[Media Type].[Product Attachment]}\n"
            + "{[Promotion].[Media Type].[Daily Paper, Radio]}\n"
            + "{[Promotion].[Media Type].[Cash Register Handout]}\n"
            + "{[Promotion].[Media Type].[Sunday Paper, Radio]}\n"
            + "{[Promotion].[Media Type].[Street Handout]}\n"
            + "{[Promotion].[Media Type].[Sunday Paper]}\n"
            + "{[Promotion].[Media Type].[Bulk Mail]}\n"
            + "{[Promotion].[Media Type].[In-Store Coupon]}\n"
            + "{[Promotion].[Media Type].[TV]}\n"
            + "{[Promotion].[Media Type].[Sunday Paper, Radio, TV]}\n"
            + "{[Promotion].[Media Type].[Radio]}\n"
            + "Row #0: 9,513\n"
            + "Row #1: 7,738\n"
            + "Row #2: 7,544\n"
            + "Row #3: 6,891\n"
            + "Row #4: 6,697\n"
            + "Row #5: 5,945\n"
            + "Row #6: 5,753\n"
            + "Row #7: 4,339\n"
            + "Row #8: 4,320\n"
            + "Row #9: 3,798\n"
            + "Row #10: 3,607\n"
            + "Row #11: 2,726\n"
            + "Row #12: 2,454\n"),

        new QueryAndResult(
            "select\n"
            + "{ [Measures].[Units Shipped], [Measures].[Units Ordered] }"
            + " on columns,\n"
            + "NON EMPTY [Store].[Store Name].members on rows\n"
            + "from Warehouse",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Units Shipped]}\n"
            + "{[Measures].[Units Ordered]}\n"
            + "Axis #2:\n"
            + "{[Store].[Stores].[USA].[CA].[Beverly Hills].[Store 6]}\n"
            + "{[Store].[Stores].[USA].[CA].[Los Angeles].[Store 7]}\n"
            + "{[Store].[Stores].[USA].[CA].[San Diego].[Store 24]}\n"
            + "{[Store].[Stores].[USA].[CA].[San Francisco].[Store 14]}\n"
            + "{[Store].[Stores].[USA].[OR].[Portland].[Store 11]}\n"
            + "{[Store].[Stores].[USA].[OR].[Salem].[Store 13]}\n"
            + "{[Store].[Stores].[USA].[WA].[Bellingham].[Store 2]}\n"
            + "{[Store].[Stores].[USA].[WA].[Bremerton].[Store 3]}\n"
            + "{[Store].[Stores].[USA].[WA].[Seattle].[Store 15]}\n"
            + "{[Store].[Stores].[USA].[WA].[Spokane].[Store 16]}\n"
            + "{[Store].[Stores].[USA].[WA].[Tacoma].[Store 17]}\n"
            + "{[Store].[Stores].[USA].[WA].[Walla Walla].[Store 22]}\n"
            + "{[Store].[Stores].[USA].[WA].[Yakima].[Store 23]}\n"
            + "Row #0: 10759.0\n"
            + "Row #0: 11699.0\n"
            + "Row #1: 24587.0\n"
            + "Row #1: 26463.0\n"
            + "Row #2: 23835.0\n"
            + "Row #2: 26270.0\n"
            + "Row #3: 1696.0\n"
            + "Row #3: 1875.0\n"
            + "Row #4: 8515.0\n"
            + "Row #4: 9109.0\n"
            + "Row #5: 32393.0\n"
            + "Row #5: 35797.0\n"
            + "Row #6: 2348.0\n"
            + "Row #6: 2454.0\n"
            + "Row #7: 22734.0\n"
            + "Row #7: 24610.0\n"
            + "Row #8: 24110.0\n"
            + "Row #8: 26703.0\n"
            + "Row #9: 11889.0\n"
            + "Row #9: 12828.0\n"
            + "Row #10: 32411.0\n"
            + "Row #10: 35930.0\n"
            + "Row #11: 1860.0\n"
            + "Row #11: 2074.0\n"
            + "Row #12: 10589.0\n"
            + "Row #12: 11426.0\n"),

        new QueryAndResult(
            "with member [Measures].[Store Sales Last Period] as"
            + " '([Measures].[Store Sales], Time.[Time].PrevMember)'\n"
            + "select\n"
            + " {[Measures].[Store Sales Last Period]} on columns,\n"
            + " {TopCount([Product].[Product Department].members,5,"
            + " [Measures].[Store Sales Last Period])} on rows\n"
            + "from Sales\n"
            + "where ([Time].[Time].[1998])",
            "Axis #0:\n"
            + "{[Time].[Time].[1998]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Sales Last Period]}\n"
            + "Axis #2:\n"
            + "{[Product].[Products].[Food].[Produce]}\n"
            + "{[Product].[Products].[Food].[Snack Foods]}\n"
            + "{[Product].[Products].[Non-Consumable].[Household]}\n"
            + "{[Product].[Products].[Food].[Frozen Foods]}\n"
            + "{[Product].[Products].[Food].[Canned Foods]}\n"
            + "Row #0: 82,248.42\n"
            + "Row #1: 67,609.82\n"
            + "Row #2: 60,469.89\n"
            + "Row #3: 55,207.50\n"
            + "Row #4: 39,774.34\n"),

        new QueryAndResult(
            "with member [Measures].[Total Store Sales] as"
            + "'Sum(YTD(),[Measures].[Store Sales])'\n"
            + "select\n"
            + "{[Measures].[Total Store Sales]} on columns,\n"
            + "{TopCount([Product].[Product Department].members,5,"
            + "[Measures].[Total Store Sales])} on rows\n"
            + "from Sales\n"
            + "where ([Time].[1997].[Q2].[4])",
            "Axis #0:\n"
            + "{[Time].[Time].[1997].[Q2].[4]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Total Store Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Products].[Food].[Produce]}\n"
            + "{[Product].[Products].[Food].[Snack Foods]}\n"
            + "{[Product].[Products].[Non-Consumable].[Household]}\n"
            + "{[Product].[Products].[Food].[Frozen Foods]}\n"
            + "{[Product].[Products].[Food].[Canned Foods]}\n"
            + "Row #0: 26,526.67\n"
            + "Row #1: 21,897.10\n"
            + "Row #2: 19,980.90\n"
            + "Row #3: 17,882.63\n"
            + "Row #4: 12,963.23\n"),

        new QueryAndResult(
            "with member [Measures].[Store Profit Rate] as"
            + "'([Measures].[Store Sales]-[Measures].[Store Cost])/"
            + "[Measures].[Store Cost]', format = '#.00%'\n"
            + "select\n"
            + "    {[Measures].[Store Cost],[Measures].[Store Sales],"
            + "[Measures].[Store Profit Rate]} on columns,\n"
            + "    Order([Product].[Product Department].members, "
            + "[Measures].[Store Profit Rate], BDESC) on rows\n"
            + "from Sales\n"
            + "where ([Time].[1997])",
            "Axis #0:\n"
            + "{[Time].[Time].[1997]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Cost]}\n"
            + "{[Measures].[Store Sales]}\n"
            + "{[Measures].[Store Profit Rate]}\n"
            + "Axis #2:\n"
            + "{[Product].[Products].[Food].[Breakfast Foods]}\n"
            + "{[Product].[Products].[Non-Consumable].[Carousel]}\n"
            + "{[Product].[Products].[Food].[Canned Products]}\n"
            + "{[Product].[Products].[Food].[Baking Goods]}\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages]}\n"
            + "{[Product].[Products].[Non-Consumable].[Health and Hygiene]}\n"
            + "{[Product].[Products].[Food].[Snack Foods]}\n"
            + "{[Product].[Products].[Food].[Baked Goods]}\n"
            + "{[Product].[Products].[Drink].[Beverages]}\n"
            + "{[Product].[Products].[Food].[Frozen Foods]}\n"
            + "{[Product].[Products].[Non-Consumable].[Periodicals]}\n"
            + "{[Product].[Products].[Food].[Produce]}\n"
            + "{[Product].[Products].[Food].[Seafood]}\n"
            + "{[Product].[Products].[Food].[Deli]}\n"
            + "{[Product].[Products].[Food].[Meat]}\n"
            + "{[Product].[Products].[Food].[Canned Foods]}\n"
            + "{[Product].[Products].[Non-Consumable].[Household]}\n"
            + "{[Product].[Products].[Food].[Starchy Foods]}\n"
            + "{[Product].[Products].[Food].[Eggs]}\n"
            + "{[Product].[Products].[Food].[Snacks]}\n"
            + "{[Product].[Products].[Food].[Dairy]}\n"
            + "{[Product].[Products].[Drink].[Dairy]}\n"
            + "{[Product].[Products].[Non-Consumable].[Checkout]}\n"
            + "Row #0: 2,756.80\n"
            + "Row #0: 6,941.46\n"
            + "Row #0: 151.79%\n"
            + "Row #1: 595.97\n"
            + "Row #1: 1,500.11\n"
            + "Row #1: 151.71%\n"
            + "Row #2: 1,317.13\n"
            + "Row #2: 3,314.52\n"
            + "Row #2: 151.65%\n"
            + "Row #3: 15,370.61\n"
            + "Row #3: 38,670.41\n"
            + "Row #3: 151.59%\n"
            + "Row #4: 5,576.79\n"
            + "Row #4: 14,029.08\n"
            + "Row #4: 151.56%\n"
            + "Row #5: 12,972.99\n"
            + "Row #5: 32,571.86\n"
            + "Row #5: 151.07%\n"
            + "Row #6: 26,963.34\n"
            + "Row #6: 67,609.82\n"
            + "Row #6: 150.75%\n"
            + "Row #7: 6,564.09\n"
            + "Row #7: 16,455.43\n"
            + "Row #7: 150.69%\n"
            + "Row #8: 11,069.53\n"
            + "Row #8: 27,748.53\n"
            + "Row #8: 150.67%\n"
            + "Row #9: 22,030.66\n"
            + "Row #9: 55,207.50\n"
            + "Row #9: 150.59%\n"
            + "Row #10: 3,614.55\n"
            + "Row #10: 9,056.76\n"
            + "Row #10: 150.56%\n"
            + "Row #11: 32,831.33\n"
            + "Row #11: 82,248.42\n"
            + "Row #11: 150.52%\n"
            + "Row #12: 1,520.70\n"
            + "Row #12: 3,809.14\n"
            + "Row #12: 150.49%\n"
            + "Row #13: 10,108.87\n"
            + "Row #13: 25,318.93\n"
            + "Row #13: 150.46%\n"
            + "Row #14: 1,465.42\n"
            + "Row #14: 3,669.89\n"
            + "Row #14: 150.43%\n"
            + "Row #15: 15,894.53\n"
            + "Row #15: 39,774.34\n"
            + "Row #15: 150.24%\n"
            + "Row #16: 24,170.73\n"
            + "Row #16: 60,469.89\n"
            + "Row #16: 150.18%\n"
            + "Row #17: 4,705.91\n"
            + "Row #17: 11,756.07\n"
            + "Row #17: 149.82%\n"
            + "Row #18: 3,684.90\n"
            + "Row #18: 9,200.76\n"
            + "Row #18: 149.69%\n"
            + "Row #19: 5,827.58\n"
            + "Row #19: 14,550.05\n"
            + "Row #19: 149.68%\n"
            + "Row #20: 12,228.85\n"
            + "Row #20: 30,508.85\n"
            + "Row #20: 149.48%\n"
            + "Row #21: 2,830.92\n"
            + "Row #21: 7,058.60\n"
            + "Row #21: 149.34%\n"
            + "Row #22: 1,525.04\n"
            + "Row #22: 3,767.71\n"
            + "Row #22: 147.06%\n"),

        new QueryAndResult(
            "with\n"
            + "   member [Product].[All Products].[Drink].[Percent of Alcoholic Drinks]"
            + " as '[Product].[All Products].[Drink].[Alcoholic Beverages]/"
            + "[Product].[Products].[Drink]', format = '#.00%'\n"
            + "select\n"
            + "   { [Product].[Drink].[Percent of Alcoholic Drinks] }"
            + " on columns,\n"
            + "   order([Customers].[All Customers].[USA].[WA].Children,"
            + " [Product].[Drink].[Percent of Alcoholic Drinks],BDESC)"
            + " on rows\n"
            + "from Sales\n"
            + "where ([Measures].[Unit Sales])",
            "Axis #0:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #1:\n"
            + "{[Product].[Products].[Drink].[Percent of Alcoholic Drinks]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Customers].[USA].[WA].[Seattle]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Kirkland]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Marysville]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Anacortes]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Olympia]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Ballard]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Bremerton]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Puyallup]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Yakima]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Tacoma]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Everett]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Renton]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Issaquah]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Bellingham]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Port Orchard]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Redmond]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Spokane]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Burien]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Lynnwood]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Walla Walla]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Edmonds]}\n"
            + "{[Customer].[Customers].[USA].[WA].[Sedro Woolley]}\n"
            + "Row #0: 44.05%\n"
            + "Row #1: 34.41%\n"
            + "Row #2: 34.20%\n"
            + "Row #3: 32.93%\n"
            + "Row #4: 31.05%\n"
            + "Row #5: 30.84%\n"
            + "Row #6: 30.69%\n"
            + "Row #7: 29.81%\n"
            + "Row #8: 28.82%\n"
            + "Row #9: 28.70%\n"
            + "Row #10: 28.37%\n"
            + "Row #11: 26.67%\n"
            + "Row #12: 26.60%\n"
            + "Row #13: 26.47%\n"
            + "Row #14: 26.42%\n"
            + "Row #15: 26.28%\n"
            + "Row #16: 25.96%\n"
            + "Row #17: 24.70%\n"
            + "Row #18: 21.89%\n"
            + "Row #19: 21.47%\n"
            + "Row #20: 17.47%\n"
            + "Row #21: 13.79%\n"),

        new QueryAndResult(
            "with member [Measures].[Accumulated Sales] as "
            + "'Sum(YTD(),[Measures].[Store Sales])'\n"
            + "select\n"
            + "    {[Measures].[Store Sales],[Measures].[Accumulated Sales]}"
            + " on columns,\n"
            + "    {Descendants([Time].[1997],[Time].[Month])} on rows\n"
            + "from Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Sales]}\n"
            + "{[Measures].[Accumulated Sales]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[1997].[Q1].[1]}\n"
            + "{[Time].[Time].[1997].[Q1].[2]}\n"
            + "{[Time].[Time].[1997].[Q1].[3]}\n"
            + "{[Time].[Time].[1997].[Q2].[4]}\n"
            + "{[Time].[Time].[1997].[Q2].[5]}\n"
            + "{[Time].[Time].[1997].[Q2].[6]}\n"
            + "{[Time].[Time].[1997].[Q3].[7]}\n"
            + "{[Time].[Time].[1997].[Q3].[8]}\n"
            + "{[Time].[Time].[1997].[Q3].[9]}\n"
            + "{[Time].[Time].[1997].[Q4].[10]}\n"
            + "{[Time].[Time].[1997].[Q4].[11]}\n"
            + "{[Time].[Time].[1997].[Q4].[12]}\n"
            + "Row #0: 45,539.69\n"
            + "Row #0: 45,539.69\n"
            + "Row #1: 44,058.79\n"
            + "Row #1: 89,598.48\n"
            + "Row #2: 50,029.87\n"
            + "Row #2: 139,628.35\n"
            + "Row #3: 42,878.25\n"
            + "Row #3: 182,506.60\n"
            + "Row #4: 44,456.29\n"
            + "Row #4: 226,962.89\n"
            + "Row #5: 45,331.73\n"
            + "Row #5: 272,294.62\n"
            + "Row #6: 50,246.88\n"
            + "Row #6: 322,541.50\n"
            + "Row #7: 46,199.04\n"
            + "Row #7: 368,740.54\n"
            + "Row #8: 43,825.97\n"
            + "Row #8: 412,566.51\n"
            + "Row #9: 42,342.27\n"
            + "Row #9: 454,908.78\n"
            + "Row #10: 53,363.71\n"
            + "Row #10: 508,272.49\n"
            + "Row #11: 56,965.64\n"
            + "Row #11: 565,238.13\n"),

        new QueryAndResult(
            "select\n"
            + " {[Measures].[Unit Sales]} on columns,\n"
            + " [Gender].members on rows\n"
            + "from Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Gender].[All Gender]}\n"
            + "{[Customer].[Gender].[F]}\n"
            + "{[Customer].[Gender].[M]}\n"
            + "Row #0: 266,773\n"
            + "Row #1: 131,558\n"
            + "Row #2: 135,215\n"),

        new QueryAndResult(
            "WITH\n"
            + "   MEMBER [Measures].[StoreType] AS \n"
            + "   '[Store].[Store Name].CurrentMember.Properties(\"Store Type\")',\n"
            + "   SOLVE_ORDER = 2\n"
            + "   MEMBER [Measures].[ProfitPct] AS \n"
            + "   '((Measures.[Store Sales] - Measures.[Store Cost]) /"
            + " Measures.[Store Sales])',\n"
            + "   SOLVE_ORDER = 1, FORMAT_STRING = 'Percent'\n"
            + "SELECT non empty\n"
            + "   { [Store].[Store Name].Members} ON COLUMNS,\n"
            + "   { [Measures].[Store Sales], [Measures].[Store Cost],"
            + " [Measures].[StoreType],\n"
            + "   [Measures].[ProfitPct] } ON ROWS\n"
            + "FROM Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[Stores].[USA].[CA].[Beverly Hills].[Store 6]}\n"
            + "{[Store].[Stores].[USA].[CA].[Los Angeles].[Store 7]}\n"
            + "{[Store].[Stores].[USA].[CA].[San Diego].[Store 24]}\n"
            + "{[Store].[Stores].[USA].[CA].[San Francisco].[Store 14]}\n"
            + "{[Store].[Stores].[USA].[OR].[Portland].[Store 11]}\n"
            + "{[Store].[Stores].[USA].[OR].[Salem].[Store 13]}\n"
            + "{[Store].[Stores].[USA].[WA].[Bellingham].[Store 2]}\n"
            + "{[Store].[Stores].[USA].[WA].[Bremerton].[Store 3]}\n"
            + "{[Store].[Stores].[USA].[WA].[Seattle].[Store 15]}\n"
            + "{[Store].[Stores].[USA].[WA].[Spokane].[Store 16]}\n"
            + "{[Store].[Stores].[USA].[WA].[Tacoma].[Store 17]}\n"
            + "{[Store].[Stores].[USA].[WA].[Walla Walla].[Store 22]}\n"
            + "{[Store].[Stores].[USA].[WA].[Yakima].[Store 23]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Store Sales]}\n"
            + "{[Measures].[Store Cost]}\n"
            + "{[Measures].[StoreType]}\n"
            + "{[Measures].[ProfitPct]}\n"
            + "Row #0: 45,750.24\n"
            + "Row #0: 54,545.28\n"
            + "Row #0: 54,431.14\n"
            + "Row #0: 4,441.18\n"
            + "Row #0: 55,058.79\n"
            + "Row #0: 87,218.28\n"
            + "Row #0: 4,739.23\n"
            + "Row #0: 52,896.30\n"
            + "Row #0: 52,644.07\n"
            + "Row #0: 49,634.46\n"
            + "Row #0: 74,843.96\n"
            + "Row #0: 4,705.97\n"
            + "Row #0: 24,329.23\n"
            + "Row #1: 18,266.44\n"
            + "Row #1: 21,771.54\n"
            + "Row #1: 21,713.53\n"
            + "Row #1: 1,778.92\n"
            + "Row #1: 21,948.94\n"
            + "Row #1: 34,823.56\n"
            + "Row #1: 1,896.62\n"
            + "Row #1: 21,121.96\n"
            + "Row #1: 20,956.80\n"
            + "Row #1: 19,795.49\n"
            + "Row #1: 29,959.28\n"
            + "Row #1: 1,880.34\n"
            + "Row #1: 9,713.81\n"
            + "Row #2: Gourmet Supermarket\n"
            + "Row #2: Supermarket\n"
            + "Row #2: Supermarket\n"
            + "Row #2: Small Grocery\n"
            + "Row #2: Supermarket\n"
            + "Row #2: Deluxe Supermarket\n"
            + "Row #2: Small Grocery\n"
            + "Row #2: Supermarket\n"
            + "Row #2: Supermarket\n"
            + "Row #2: Supermarket\n"
            + "Row #2: Deluxe Supermarket\n"
            + "Row #2: Small Grocery\n"
            + "Row #2: Mid-Size Grocery\n"
            + "Row #3: 60.07%\n"
            + "Row #3: 60.09%\n"
            + "Row #3: 60.11%\n"
            + "Row #3: 59.94%\n"
            + "Row #3: 60.14%\n"
            + "Row #3: 60.07%\n"
            + "Row #3: 59.98%\n"
            + "Row #3: 60.07%\n"
            + "Row #3: 60.19%\n"
            + "Row #3: 60.12%\n"
            + "Row #3: 59.97%\n"
            + "Row #3: 60.04%\n"
            + "Row #3: 60.07%\n"),

        new QueryAndResult(
            "WITH\n"
            + "   MEMBER [Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[BigSeller] AS\n"
            + "  'IIf([Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine] > 100, \"Yes\",\"No\")'\n"
            + "SELECT\n"
            + "   {[Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[BigSeller]} ON COLUMNS,\n"
            + "   {Store.[Store Name].Members} ON ROWS\n"
            + "FROM Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[BigSeller]}\n"
            + "Axis #2:\n"
            + "{[Store].[Stores].[Canada].[BC].[Vancouver].[Store 19]}\n"
            + "{[Store].[Stores].[Canada].[BC].[Victoria].[Store 20]}\n"
            + "{[Store].[Stores].[Mexico].[DF].[Mexico City].[Store 9]}\n"
            + "{[Store].[Stores].[Mexico].[DF].[San Andres].[Store 21]}\n"
            + "{[Store].[Stores].[Mexico].[Guerrero].[Acapulco].[Store 1]}\n"
            + "{[Store].[Stores].[Mexico].[Jalisco].[Guadalajara].[Store 5]}\n"
            + "{[Store].[Stores].[Mexico].[Veracruz].[Orizaba].[Store 10]}\n"
            + "{[Store].[Stores].[Mexico].[Yucatan].[Merida].[Store 8]}\n"
            + "{[Store].[Stores].[Mexico].[Zacatecas].[Camacho].[Store 4]}\n"
            + "{[Store].[Stores].[Mexico].[Zacatecas].[Hidalgo].[Store 12]}\n"
            + "{[Store].[Stores].[Mexico].[Zacatecas].[Hidalgo].[Store 18]}\n"
            + "{[Store].[Stores].[USA].[CA].[Alameda].[HQ]}\n"
            + "{[Store].[Stores].[USA].[CA].[Beverly Hills].[Store 6]}\n"
            + "{[Store].[Stores].[USA].[CA].[Los Angeles].[Store 7]}\n"
            + "{[Store].[Stores].[USA].[CA].[San Diego].[Store 24]}\n"
            + "{[Store].[Stores].[USA].[CA].[San Francisco].[Store 14]}\n"
            + "{[Store].[Stores].[USA].[OR].[Portland].[Store 11]}\n"
            + "{[Store].[Stores].[USA].[OR].[Salem].[Store 13]}\n"
            + "{[Store].[Stores].[USA].[WA].[Bellingham].[Store 2]}\n"
            + "{[Store].[Stores].[USA].[WA].[Bremerton].[Store 3]}\n"
            + "{[Store].[Stores].[USA].[WA].[Seattle].[Store 15]}\n"
            + "{[Store].[Stores].[USA].[WA].[Spokane].[Store 16]}\n"
            + "{[Store].[Stores].[USA].[WA].[Tacoma].[Store 17]}\n"
            + "{[Store].[Stores].[USA].[WA].[Walla Walla].[Store 22]}\n"
            + "{[Store].[Stores].[USA].[WA].[Yakima].[Store 23]}\n"
            + "Row #0: No\n"
            + "Row #1: No\n"
            + "Row #2: No\n"
            + "Row #3: No\n"
            + "Row #4: No\n"
            + "Row #5: No\n"
            + "Row #6: No\n"
            + "Row #7: No\n"
            + "Row #8: No\n"
            + "Row #9: No\n"
            + "Row #10: No\n"
            + "Row #11: No\n"
            + "Row #12: Yes\n"
            + "Row #13: Yes\n"
            + "Row #14: Yes\n"
            + "Row #15: No\n"
            + "Row #16: Yes\n"
            + "Row #17: Yes\n"
            + "Row #18: No\n"
            + "Row #19: Yes\n"
            + "Row #20: Yes\n"
            + "Row #21: Yes\n"
            + "Row #22: Yes\n"
            + "Row #23: No\n"
            + "Row #24: Yes\n"),

        new QueryAndResult(
            "WITH\n"
            + "   MEMBER [Measures].[ProfitPct] AS \n"
            + "   '((Measures.[Store Sales] - Measures.[Store Cost]) /"
            + " Measures.[Store Sales])',\n"
            + "   SOLVE_ORDER = 1, FORMAT_STRING = 'Percent'\n"
            + "   MEMBER [Measures].[ProfitValue] AS \n"
            + "   '[Measures].[Store Sales] * [Measures].[ProfitPct]',\n"
            + "   SOLVE_ORDER = 2, FORMAT_STRING = 'Currency'\n"
            + "SELECT non empty \n"
            + "   { [Store].[Store Name].Members} ON COLUMNS,\n"
            + "   { [Measures].[Store Sales], [Measures].[Store Cost],"
            + " [Measures].[ProfitValue],\n"
            + "   [Measures].[ProfitPct] } ON ROWS\n"
            + "FROM Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[Stores].[USA].[CA].[Beverly Hills].[Store 6]}\n"
            + "{[Store].[Stores].[USA].[CA].[Los Angeles].[Store 7]}\n"
            + "{[Store].[Stores].[USA].[CA].[San Diego].[Store 24]}\n"
            + "{[Store].[Stores].[USA].[CA].[San Francisco].[Store 14]}\n"
            + "{[Store].[Stores].[USA].[OR].[Portland].[Store 11]}\n"
            + "{[Store].[Stores].[USA].[OR].[Salem].[Store 13]}\n"
            + "{[Store].[Stores].[USA].[WA].[Bellingham].[Store 2]}\n"
            + "{[Store].[Stores].[USA].[WA].[Bremerton].[Store 3]}\n"
            + "{[Store].[Stores].[USA].[WA].[Seattle].[Store 15]}\n"
            + "{[Store].[Stores].[USA].[WA].[Spokane].[Store 16]}\n"
            + "{[Store].[Stores].[USA].[WA].[Tacoma].[Store 17]}\n"
            + "{[Store].[Stores].[USA].[WA].[Walla Walla].[Store 22]}\n"
            + "{[Store].[Stores].[USA].[WA].[Yakima].[Store 23]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Store Sales]}\n"
            + "{[Measures].[Store Cost]}\n"
            + "{[Measures].[ProfitValue]}\n"
            + "{[Measures].[ProfitPct]}\n"
            + "Row #0: 45,750.24\n"
            + "Row #0: 54,545.28\n"
            + "Row #0: 54,431.14\n"
            + "Row #0: 4,441.18\n"
            + "Row #0: 55,058.79\n"
            + "Row #0: 87,218.28\n"
            + "Row #0: 4,739.23\n"
            + "Row #0: 52,896.30\n"
            + "Row #0: 52,644.07\n"
            + "Row #0: 49,634.46\n"
            + "Row #0: 74,843.96\n"
            + "Row #0: 4,705.97\n"
            + "Row #0: 24,329.23\n"
            + "Row #1: 18,266.44\n"
            + "Row #1: 21,771.54\n"
            + "Row #1: 21,713.53\n"
            + "Row #1: 1,778.92\n"
            + "Row #1: 21,948.94\n"
            + "Row #1: 34,823.56\n"
            + "Row #1: 1,896.62\n"
            + "Row #1: 21,121.96\n"
            + "Row #1: 20,956.80\n"
            + "Row #1: 19,795.49\n"
            + "Row #1: 29,959.28\n"
            + "Row #1: 1,880.34\n"
            + "Row #1: 9,713.81\n"
            + "Row #2: $27,483.80\n"
            + "Row #2: $32,773.74\n"
            + "Row #2: $32,717.61\n"
            + "Row #2: $2,662.26\n"
            + "Row #2: $33,109.85\n"
            + "Row #2: $52,394.72\n"
            + "Row #2: $2,842.61\n"
            + "Row #2: $31,774.34\n"
            + "Row #2: $31,687.27\n"
            + "Row #2: $29,838.97\n"
            + "Row #2: $44,884.68\n"
            + "Row #2: $2,825.63\n"
            + "Row #2: $14,615.42\n"
            + "Row #3: 60.07%\n"
            + "Row #3: 60.09%\n"
            + "Row #3: 60.11%\n"
            + "Row #3: 59.94%\n"
            + "Row #3: 60.14%\n"
            + "Row #3: 60.07%\n"
            + "Row #3: 59.98%\n"
            + "Row #3: 60.07%\n"
            + "Row #3: 60.19%\n"
            + "Row #3: 60.12%\n"
            + "Row #3: 59.97%\n"
            + "Row #3: 60.04%\n"
            + "Row #3: 60.07%\n"),

        new QueryAndResult(
            "WITH MEMBER MEASURES.ProfitPercent AS\n"
            + "     '([Measures].[Store Sales]-[Measures].[Store Cost])/"
            + "([Measures].[Store Cost])',\n"
            + " FORMAT_STRING = '#.00%', SOLVE_ORDER = 1\n"
            + " Member [Time].[Time].[First Half 97] AS  '[Time].[Time].[1997].[Q1] +"
            + " [Time].[Time].[1997].[Q2]'\n"
            + " Member [Time].[Time].[Second Half 97] AS '[Time].[Time].[1997].[Q3] +"
            + " [Time].[Time].[1997].[Q4]'\n"
            + " SELECT {[Time].[First Half 97],\n"
            + "     [Time].[Time].[Second Half 97],\n"
            + "     [Time].[Time].[1997].CHILDREN} ON COLUMNS,\n"
            + " {[Store].[Store Country].[USA].CHILDREN} ON ROWS\n"
            + " FROM [Sales]\n"
            + " WHERE ([Measures].[ProfitPercent])",
            "Axis #0:\n"
            + "{[Measures].[ProfitPercent]}\n"
            + "Axis #1:\n"
            + "{[Time].[Time].[First Half 97]}\n"
            + "{[Time].[Time].[Second Half 97]}\n"
            + "{[Time].[Time].[1997].[Q1]}\n"
            + "{[Time].[Time].[1997].[Q2]}\n"
            + "{[Time].[Time].[1997].[Q3]}\n"
            + "{[Time].[Time].[1997].[Q4]}\n"
            + "Axis #2:\n"
            + "{[Store].[Stores].[USA].[CA]}\n"
            + "{[Store].[Stores].[USA].[OR]}\n"
            + "{[Store].[Stores].[USA].[WA]}\n"
            + "Row #0: 150.55%\n"
            + "Row #0: 150.53%\n"
            + "Row #0: 150.68%\n"
            + "Row #0: 150.44%\n"
            + "Row #0: 151.35%\n"
            + "Row #0: 149.81%\n"
            + "Row #1: 150.15%\n"
            + "Row #1: 151.08%\n"
            + "Row #1: 149.80%\n"
            + "Row #1: 150.60%\n"
            + "Row #1: 151.37%\n"
            + "Row #1: 150.78%\n"
            + "Row #2: 150.59%\n"
            + "Row #2: 150.34%\n"
            + "Row #2: 150.72%\n"
            + "Row #2: 150.45%\n"
            + "Row #2: 150.39%\n"
            + "Row #2: 150.29%\n"),

        new QueryAndResult(
            "with member [Measures].[Accumulated Sales] as"
            + " 'Sum(YTD(),[Measures].[Store Sales])'\n"
            + "select\n"
            + "    {[Measures].[Store Sales],[Measures].[Accumulated Sales]}"
            + " on columns,\n"
            + "    {Descendants([Time].[Time].[1997],[Time].[Month])} on rows\n"
            + "from [Warehouse and Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Sales]}\n"
            + "{[Measures].[Accumulated Sales]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[1997].[Q1].[1]}\n"
            + "{[Time].[Time].[1997].[Q1].[2]}\n"
            + "{[Time].[Time].[1997].[Q1].[3]}\n"
            + "{[Time].[Time].[1997].[Q2].[4]}\n"
            + "{[Time].[Time].[1997].[Q2].[5]}\n"
            + "{[Time].[Time].[1997].[Q2].[6]}\n"
            + "{[Time].[Time].[1997].[Q3].[7]}\n"
            + "{[Time].[Time].[1997].[Q3].[8]}\n"
            + "{[Time].[Time].[1997].[Q3].[9]}\n"
            + "{[Time].[Time].[1997].[Q4].[10]}\n"
            + "{[Time].[Time].[1997].[Q4].[11]}\n"
            + "{[Time].[Time].[1997].[Q4].[12]}\n"
            + "Row #0: 45,539.69\n"
            + "Row #0: 45,539.69\n"
            + "Row #1: 44,058.79\n"
            + "Row #1: 89,598.48\n"
            + "Row #2: 50,029.87\n"
            + "Row #2: 139,628.35\n"
            + "Row #3: 42,878.25\n"
            + "Row #3: 182,506.60\n"
            + "Row #4: 44,456.29\n"
            + "Row #4: 226,962.89\n"
            + "Row #5: 45,331.73\n"
            + "Row #5: 272,294.62\n"
            + "Row #6: 50,246.88\n"
            + "Row #6: 322,541.50\n"
            + "Row #7: 46,199.04\n"
            + "Row #7: 368,740.54\n"
            + "Row #8: 43,825.97\n"
            + "Row #8: 412,566.51\n"
            + "Row #9: 42,342.27\n"
            + "Row #9: 454,908.78\n"
            + "Row #10: 53,363.71\n"
            + "Row #10: 508,272.49\n"
            + "Row #11: 56,965.64\n"
            + "Row #11: 565,238.13\n"),

        // Virtual cube. Note that Unit Sales is independent of Warehouse.
        new QueryAndResult(
            "select non empty CrossJoin(\r\n"
            + "  {[Warehouse].DefaultMember, [Warehouse].[USA].children},\n"
            + " {[Measures].[Unit Sales], [Measures].[Units Shipped]}) on"
            + " columns,\n"
            + " [Time].[Time].children on rows\n"
            + "from [Warehouse and Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Warehouse].[Warehouses].[All Warehouses], [Measures].[Unit Sales]}\n"
            + "{[Warehouse].[Warehouses].[All Warehouses], [Measures].[Units Shipped]}\n"
            + "{[Warehouse].[Warehouses].[USA].[CA], [Measures].[Units Shipped]}\n"
            + "{[Warehouse].[Warehouses].[USA].[OR], [Measures].[Units Shipped]}\n"
            + "{[Warehouse].[Warehouses].[USA].[WA], [Measures].[Units Shipped]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[1997].[Q1]}\n"
            + "{[Time].[Time].[1997].[Q2]}\n"
            + "{[Time].[Time].[1997].[Q3]}\n"
            + "{[Time].[Time].[1997].[Q4]}\n"
            + "Row #0: 66,291\n"
            + "Row #0: 50951.0\n"
            + "Row #0: 8539.0\n"
            + "Row #0: 7994.0\n"
            + "Row #0: 34418.0\n"
            + "Row #1: 62,610\n"
            + "Row #1: 49187.0\n"
            + "Row #1: 15726.0\n"
            + "Row #1: 7575.0\n"
            + "Row #1: 25886.0\n"
            + "Row #2: 65,848\n"
            + "Row #2: 57789.0\n"
            + "Row #2: 20821.0\n"
            + "Row #2: 8673.0\n"
            + "Row #2: 28295.0\n"
            + "Row #3: 72,024\n"
            + "Row #3: 49799.0\n"
            + "Row #3: 15791.0\n"
            + "Row #3: 16666.0\n"
            + "Row #3: 17342.0\n"),

        // should allow dimension to be used as shorthand for member
        new QueryAndResult(
            "select {[Measures].[Unit Sales]} on columns,\n"
            + " {[Store].[Stores], [Store].[Stores].children} on rows\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Store].[Stores].[All Stores]}\n"
            + "{[Store].[Stores].[Canada]}\n"
            + "{[Store].[Stores].[Mexico]}\n"
            + "{[Store].[Stores].[USA]}\n"
            + "Row #0: 266,773\n"
            + "Row #1: \n"
            + "Row #2: \n"
            + "Row #3: 266,773\n"),

        // crossjoins on rows and columns, and a slicer
        new QueryAndResult(
            "select\n"
            + "  CrossJoin(\n"
            + "    {[Measures].[Unit Sales], [Measures].[Store Sales]},\n"
            + "    {[Time].[Time].[1997].[Q2].children}) on columns, \n"
            + "  CrossJoin(\n"
            + "    CrossJoin(\n"
            + "      [Gender].members,\n"
            + "      [Marital Status].members),\n"
            + "   {[Store].[Stores], [Store].[Stores].children}) on rows\n"
            + "from [Sales]\n"
            + "where (\n"
            + " [Product].[Food],\n"
            + " [Education Level].[High School Degree],\n"
            + " [Promotions].DefaultMember)",
            "Axis #0:\n"
            + "{[Product].[Products].[Food], [Customer].[Education Level].[High School Degree], "
            + "[Promotion].[Promotions].[All Promotions]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales], [Time].[Time].[1997].[Q2].[4]}\n"
            + "{[Measures].[Unit Sales], [Time].[Time].[1997].[Q2].[5]}\n"
            + "{[Measures].[Unit Sales], [Time].[Time].[1997].[Q2].[6]}\n"
            + "{[Measures].[Store Sales], [Time].[Time].[1997].[Q2].[4]}\n"
            + "{[Measures].[Store Sales], [Time].[Time].[1997].[Q2].[5]}\n"
            + "{[Measures].[Store Sales], [Time].[Time].[1997].[Q2].[6]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Gender].[All Gender], [Customer].[Marital Status].[All Marital Status], [Store].[Stores].[All Stores]}\n"
            + "{[Customer].[Gender].[All Gender], [Customer].[Marital Status].[All Marital Status], [Store].[Stores].[Canada]}\n"
            + "{[Customer].[Gender].[All Gender], [Customer].[Marital Status].[All Marital Status], [Store].[Stores].[Mexico]}\n"
            + "{[Customer].[Gender].[All Gender], [Customer].[Marital Status].[All Marital Status], [Store].[Stores].[USA]}\n"
            + "{[Customer].[Gender].[All Gender], [Customer].[Marital Status].[M], [Store].[Stores].[All Stores]}\n"
            + "{[Customer].[Gender].[All Gender], [Customer].[Marital Status].[M], [Store].[Stores].[Canada]}\n"
            + "{[Customer].[Gender].[All Gender], [Customer].[Marital Status].[M], [Store].[Stores].[Mexico]}\n"
            + "{[Customer].[Gender].[All Gender], [Customer].[Marital Status].[M], [Store].[Stores].[USA]}\n"
            + "{[Customer].[Gender].[All Gender], [Customer].[Marital Status].[S], [Store].[Stores].[All Stores]}\n"
            + "{[Customer].[Gender].[All Gender], [Customer].[Marital Status].[S], [Store].[Stores].[Canada]}\n"
            + "{[Customer].[Gender].[All Gender], [Customer].[Marital Status].[S], [Store].[Stores].[Mexico]}\n"
            + "{[Customer].[Gender].[All Gender], [Customer].[Marital Status].[S], [Store].[Stores].[USA]}\n"
            + "{[Customer].[Gender].[F], [Customer].[Marital Status].[All Marital Status], [Store].[Stores].[All Stores]}\n"
            + "{[Customer].[Gender].[F], [Customer].[Marital Status].[All Marital Status], [Store].[Stores].[Canada]}\n"
            + "{[Customer].[Gender].[F], [Customer].[Marital Status].[All Marital Status], [Store].[Stores].[Mexico]}\n"
            + "{[Customer].[Gender].[F], [Customer].[Marital Status].[All Marital Status], [Store].[Stores].[USA]}\n"
            + "{[Customer].[Gender].[F], [Customer].[Marital Status].[M], [Store].[Stores].[All Stores]}\n"
            + "{[Customer].[Gender].[F], [Customer].[Marital Status].[M], [Store].[Stores].[Canada]}\n"
            + "{[Customer].[Gender].[F], [Customer].[Marital Status].[M], [Store].[Stores].[Mexico]}\n"
            + "{[Customer].[Gender].[F], [Customer].[Marital Status].[M], [Store].[Stores].[USA]}\n"
            + "{[Customer].[Gender].[F], [Customer].[Marital Status].[S], [Store].[Stores].[All Stores]}\n"
            + "{[Customer].[Gender].[F], [Customer].[Marital Status].[S], [Store].[Stores].[Canada]}\n"
            + "{[Customer].[Gender].[F], [Customer].[Marital Status].[S], [Store].[Stores].[Mexico]}\n"
            + "{[Customer].[Gender].[F], [Customer].[Marital Status].[S], [Store].[Stores].[USA]}\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[All Marital Status], [Store].[Stores].[All Stores]}\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[All Marital Status], [Store].[Stores].[Canada]}\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[All Marital Status], [Store].[Stores].[Mexico]}\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[All Marital Status], [Store].[Stores].[USA]}\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[M], [Store].[Stores].[All Stores]}\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[M], [Store].[Stores].[Canada]}\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[M], [Store].[Stores].[Mexico]}\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[M], [Store].[Stores].[USA]}\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[S], [Store].[Stores].[All Stores]}\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[S], [Store].[Stores].[Canada]}\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[S], [Store].[Stores].[Mexico]}\n"
            + "{[Customer].[Gender].[M], [Customer].[Marital Status].[S], [Store].[Stores].[USA]}\n"
            + "Row #0: 4,284\n"
            + "Row #0: 3,972\n"
            + "Row #0: 4,476\n"
            + "Row #0: 9,014.60\n"
            + "Row #0: 8,595.99\n"
            + "Row #0: 9,480.21\n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #3: 4,284\n"
            + "Row #3: 3,972\n"
            + "Row #3: 4,476\n"
            + "Row #3: 9,014.60\n"
            + "Row #3: 8,595.99\n"
            + "Row #3: 9,480.21\n"
            + "Row #4: 1,942\n"
            + "Row #4: 1,843\n"
            + "Row #4: 2,128\n"
            + "Row #4: 4,133.87\n"
            + "Row #4: 4,007.53\n"
            + "Row #4: 4,541.97\n"
            + "Row #5: \n"
            + "Row #5: \n"
            + "Row #5: \n"
            + "Row #5: \n"
            + "Row #5: \n"
            + "Row #5: \n"
            + "Row #6: \n"
            + "Row #6: \n"
            + "Row #6: \n"
            + "Row #6: \n"
            + "Row #6: \n"
            + "Row #6: \n"
            + "Row #7: 1,942\n"
            + "Row #7: 1,843\n"
            + "Row #7: 2,128\n"
            + "Row #7: 4,133.87\n"
            + "Row #7: 4,007.53\n"
            + "Row #7: 4,541.97\n"
            + "Row #8: 2,342\n"
            + "Row #8: 2,129\n"
            + "Row #8: 2,348\n"
            + "Row #8: 4,880.73\n"
            + "Row #8: 4,588.46\n"
            + "Row #8: 4,938.24\n"
            + "Row #9: \n"
            + "Row #9: \n"
            + "Row #9: \n"
            + "Row #9: \n"
            + "Row #9: \n"
            + "Row #9: \n"
            + "Row #10: \n"
            + "Row #10: \n"
            + "Row #10: \n"
            + "Row #10: \n"
            + "Row #10: \n"
            + "Row #10: \n"
            + "Row #11: 2,342\n"
            + "Row #11: 2,129\n"
            + "Row #11: 2,348\n"
            + "Row #11: 4,880.73\n"
            + "Row #11: 4,588.46\n"
            + "Row #11: 4,938.24\n"
            + "Row #12: 2,093\n"
            + "Row #12: 1,981\n"
            + "Row #12: 1,918\n"
            + "Row #12: 4,380.67\n"
            + "Row #12: 4,338.31\n"
            + "Row #12: 4,130.34\n"
            + "Row #13: \n"
            + "Row #13: \n"
            + "Row #13: \n"
            + "Row #13: \n"
            + "Row #13: \n"
            + "Row #13: \n"
            + "Row #14: \n"
            + "Row #14: \n"
            + "Row #14: \n"
            + "Row #14: \n"
            + "Row #14: \n"
            + "Row #14: \n"
            + "Row #15: 2,093\n"
            + "Row #15: 1,981\n"
            + "Row #15: 1,918\n"
            + "Row #15: 4,380.67\n"
            + "Row #15: 4,338.31\n"
            + "Row #15: 4,130.34\n"
            + "Row #16: 901\n"
            + "Row #16: 942\n"
            + "Row #16: 837\n"
            + "Row #16: 1,905.00\n"
            + "Row #16: 2,069.44\n"
            + "Row #16: 1,865.44\n"
            + "Row #17: \n"
            + "Row #17: \n"
            + "Row #17: \n"
            + "Row #17: \n"
            + "Row #17: \n"
            + "Row #17: \n"
            + "Row #18: \n"
            + "Row #18: \n"
            + "Row #18: \n"
            + "Row #18: \n"
            + "Row #18: \n"
            + "Row #18: \n"
            + "Row #19: 901\n"
            + "Row #19: 942\n"
            + "Row #19: 837\n"
            + "Row #19: 1,905.00\n"
            + "Row #19: 2,069.44\n"
            + "Row #19: 1,865.44\n"
            + "Row #20: 1,192\n"
            + "Row #20: 1,039\n"
            + "Row #20: 1,081\n"
            + "Row #20: 2,475.67\n"
            + "Row #20: 2,268.87\n"
            + "Row #20: 2,264.90\n"
            + "Row #21: \n"
            + "Row #21: \n"
            + "Row #21: \n"
            + "Row #21: \n"
            + "Row #21: \n"
            + "Row #21: \n"
            + "Row #22: \n"
            + "Row #22: \n"
            + "Row #22: \n"
            + "Row #22: \n"
            + "Row #22: \n"
            + "Row #22: \n"
            + "Row #23: 1,192\n"
            + "Row #23: 1,039\n"
            + "Row #23: 1,081\n"
            + "Row #23: 2,475.67\n"
            + "Row #23: 2,268.87\n"
            + "Row #23: 2,264.90\n"
            + "Row #24: 2,191\n"
            + "Row #24: 1,991\n"
            + "Row #24: 2,558\n"
            + "Row #24: 4,633.93\n"
            + "Row #24: 4,257.68\n"
            + "Row #24: 5,349.87\n"
            + "Row #25: \n"
            + "Row #25: \n"
            + "Row #25: \n"
            + "Row #25: \n"
            + "Row #25: \n"
            + "Row #25: \n"
            + "Row #26: \n"
            + "Row #26: \n"
            + "Row #26: \n"
            + "Row #26: \n"
            + "Row #26: \n"
            + "Row #26: \n"
            + "Row #27: 2,191\n"
            + "Row #27: 1,991\n"
            + "Row #27: 2,558\n"
            + "Row #27: 4,633.93\n"
            + "Row #27: 4,257.68\n"
            + "Row #27: 5,349.87\n"
            + "Row #28: 1,041\n"
            + "Row #28: 901\n"
            + "Row #28: 1,291\n"
            + "Row #28: 2,228.87\n"
            + "Row #28: 1,938.09\n"
            + "Row #28: 2,676.53\n"
            + "Row #29: \n"
            + "Row #29: \n"
            + "Row #29: \n"
            + "Row #29: \n"
            + "Row #29: \n"
            + "Row #29: \n"
            + "Row #30: \n"
            + "Row #30: \n"
            + "Row #30: \n"
            + "Row #30: \n"
            + "Row #30: \n"
            + "Row #30: \n"
            + "Row #31: 1,041\n"
            + "Row #31: 901\n"
            + "Row #31: 1,291\n"
            + "Row #31: 2,228.87\n"
            + "Row #31: 1,938.09\n"
            + "Row #31: 2,676.53\n"
            + "Row #32: 1,150\n"
            + "Row #32: 1,090\n"
            + "Row #32: 1,267\n"
            + "Row #32: 2,405.06\n"
            + "Row #32: 2,319.59\n"
            + "Row #32: 2,673.34\n"
            + "Row #33: \n"
            + "Row #33: \n"
            + "Row #33: \n"
            + "Row #33: \n"
            + "Row #33: \n"
            + "Row #33: \n"
            + "Row #34: \n"
            + "Row #34: \n"
            + "Row #34: \n"
            + "Row #34: \n"
            + "Row #34: \n"
            + "Row #34: \n"
            + "Row #35: 1,150\n"
            + "Row #35: 1,090\n"
            + "Row #35: 1,267\n"
            + "Row #35: 2,405.06\n"
            + "Row #35: 2,319.59\n"
            + "Row #35: 2,673.34\n"),

        new QueryAndResult(
            "select from [Warehouse and Sales] where [Measures].[Store Sales]",
            "Axis #0:\n"
            + "{[Measures].[Store Sales]}\n"
            + "565,238.13"),
    };
    private int count = 0;

    public void testConcurrentValidatingQueriesInRandomOrder() {
        propSaver.set(propSaver.props.DisableCaching, false);
        propSaver.set(propSaver.props.UseAggregates, false);
        propSaver.set(propSaver.props.ReadAggregates, false);

        FoodMartTestCase.QueryAndResult[] singleQuery = {mdxQueries[0]};
        assertTrue(
            ConcurrentValidatingQueryRunner.runTest(
                1, 1, false, true, singleQuery)
            .size() == 0);
        // ensures same global aggregation is used by 2 or more threads and
        // all of them load the same segment.
        FoodMartTestCase.QueryAndResult[] singleQueryFor2Threads = {
            mdxQueries[1]
        };
        assertTrue(
            ConcurrentValidatingQueryRunner.runTest(
                2, 5, false, true, singleQueryFor2Threads)
            .size() == 0);

        assertTrue(
            ConcurrentValidatingQueryRunner.runTest(
                10, 45, true, true, mdxQueries)
            .size() == 0);
    }

    public void testFlushingDoesNotCauseDeadlock() throws Exception {
        // Create a seeded deterministic random generator.
        final long seed = new Random().nextLong();
        LOGGER.debug("Test seed: " + seed);
        final Random random = new Random(seed);

        final List<OlapStatement> statements = new ArrayList<OlapStatement>();
        ExecutorService executorService =
            Executors.newFixedThreadPool(
                propSaver.props.RolapConnectionShepherdNbThreads.get() - 2);

        for (int i = 0; i < 700; i++) {
            for (final QueryAndResult mdxQuery : mdxQueries) {
                executorService.submit(
                    new Runnable() { public void run() {
                        OlapStatement statement = null;
                        try {
                            // Throttle a bit (randomly)
                            Thread.sleep(random.nextInt(50));
                            OlapConnection connection =
                                getTestContext().getOlap4jConnection();
                            statement = connection.createStatement();
                            statements.add(statement);
                            statement.executeOlapQuery(mdxQuery.query);
                        } catch (Exception e) {
                            e.printStackTrace();
                        } finally {
                            logStatus();
                        }
                    }
                });
            }
        }

        randomlyFlush(statements, random);

        executorService.shutdown();

        boolean finished = executorService.awaitTermination(
            45, TimeUnit.SECONDS);
        assertTrue(finished);
    }

    private synchronized void logStatus() {
        if (count % 100 == 0) {
            LOGGER.debug(count);
            System.out.println(count);
        }
        count++;
    }

    private void randomlyFlush(List<OlapStatement> statements, Random r) {
        try {
            // Let the system boot up and start processing queries.
            Thread.sleep(1000);
            for (int i = 0; i < 20; i++) {
                // Wait between 0 and 5 seconds.
                Thread.sleep(1000 * r.nextInt(5));
                try {
                    if (statements.size() > 0) {
                        OlapStatement olapStatement = statements.get(
                            r.nextInt(statements.size()));
                        LOGGER.debug("flushing");
                        System.out.println("flushing");
                        flushSchema(
                            olapStatement.getConnection().unwrap(
                                Connection.class));
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void flushSchema(Connection connection) {
        CacheControl cacheControl =
            connection.getCacheControl(null);

        Cube salesCube = connection.getSchema().lookupCube("Sales", true);
        CacheControl.CellRegion measuresRegion =
            cacheControl.createMeasuresRegion(salesCube);
        cacheControl.flush(measuresRegion);

        Cube whsalesCube =
            connection.getSchema().lookupCube("Warehouse and Sales", true);
        measuresRegion =
            cacheControl.createMeasuresRegion(whsalesCube);
        cacheControl.flush(measuresRegion);
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    protected void setUp() throws Exception {
        super.setUp();
    }
}

// End ConcurrentMdxTest.java
