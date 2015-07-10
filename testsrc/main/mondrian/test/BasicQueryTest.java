/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2003-2005 Julian Hyde
// Copyright (C) 2005-2014 Pentaho
// All Rights Reserved.
//
// jhyde, Feb 14, 2003
*/
package mondrian.test;

import mondrian.calc.ResultStyle;
import mondrian.olap.Axis;
import mondrian.olap.Cell;
import mondrian.olap.Connection;
import mondrian.olap.*;
import mondrian.olap.Position;
import mondrian.olap.type.NumericType;
import mondrian.olap.type.Type;
import mondrian.rolap.RolapConnection;
import mondrian.rolap.RolapSchema;
import mondrian.rolap.RolapUtil;
import mondrian.server.Execution;
import mondrian.spi.*;
import mondrian.spi.impl.JdbcStatisticsProvider;
import mondrian.spi.impl.SqlStatisticsProvider;
import mondrian.util.Bug;

import junit.framework.Assert;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;

import org.eigenbase.util.property.StringProperty;

import org.olap4j.*;
import org.olap4j.layout.RectangularCellSetFormatter;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <code>BasicQueryTest</code> is a test case which tests simple queries
 * against the FoodMart database.
 *
 * @author jhyde
 * @since Feb 14, 2003
 */
public class BasicQueryTest extends FoodMartTestCase {
    static final String EmptyResult =
        "Axis #0:\n"
        + "{}\n"
        + "Axis #1:\n"
        + "Axis #2:\n";

    private static final String timeWeekly =
        TestContext.hierarchyName("Time", "Weekly");

    private MondrianProperties props = MondrianProperties.instance();

    public BasicQueryTest() {
        super();
    }

    public BasicQueryTest(String name) {
        super(name);
    }

    private static final QueryAndResult[] sampleQueries = {
        // 0
        new QueryAndResult(
            "select {[Measures].[Unit Sales]} on columns\n" + " from Sales",

            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Row #0: 266,773\n"),

        // 1
        new QueryAndResult(
            "select\n"
            + "    {[Measures].[Unit Sales]} on columns,\n"
            + "    order(except([Promotion Media].[Media Type].members,{[Promotion Media].[Media Type].[No Media]}),[Measures].[Unit Sales],DESC) on rows\n"
            + "from Sales ",

            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Promotion Media].[Daily Paper, Radio, TV]}\n"
            + "{[Promotion Media].[Daily Paper]}\n"
            + "{[Promotion Media].[Product Attachment]}\n"
            + "{[Promotion Media].[Daily Paper, Radio]}\n"
            + "{[Promotion Media].[Cash Register Handout]}\n"
            + "{[Promotion Media].[Sunday Paper, Radio]}\n"
            + "{[Promotion Media].[Street Handout]}\n"
            + "{[Promotion Media].[Sunday Paper]}\n"
            + "{[Promotion Media].[Bulk Mail]}\n"
            + "{[Promotion Media].[In-Store Coupon]}\n"
            + "{[Promotion Media].[TV]}\n"
            + "{[Promotion Media].[Sunday Paper, Radio, TV]}\n"
            + "{[Promotion Media].[Radio]}\n"
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

        // 2
        new QueryAndResult(
            "select\n"
            + "    { [Measures].[Units Shipped], [Measures].[Units Ordered] } on columns,\n"
            + "    NON EMPTY [Store].[Store Name].members on rows\n"
            + "from Warehouse",

            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Units Shipped]}\n"
            + "{[Measures].[Units Ordered]}\n"
            + "Axis #2:\n"
            + "{[Store].[USA].[CA].[Beverly Hills].[Store 6]}\n"
            + "{[Store].[USA].[CA].[Los Angeles].[Store 7]}\n"
            + "{[Store].[USA].[CA].[San Diego].[Store 24]}\n"
            + "{[Store].[USA].[CA].[San Francisco].[Store 14]}\n"
            + "{[Store].[USA].[OR].[Portland].[Store 11]}\n"
            + "{[Store].[USA].[OR].[Salem].[Store 13]}\n"
            + "{[Store].[USA].[WA].[Bellingham].[Store 2]}\n"
            + "{[Store].[USA].[WA].[Bremerton].[Store 3]}\n"
            + "{[Store].[USA].[WA].[Seattle].[Store 15]}\n"
            + "{[Store].[USA].[WA].[Spokane].[Store 16]}\n"
            + "{[Store].[USA].[WA].[Tacoma].[Store 17]}\n"
            + "{[Store].[USA].[WA].[Walla Walla].[Store 22]}\n"
            + "{[Store].[USA].[WA].[Yakima].[Store 23]}\n"
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

        // 3
        new QueryAndResult(
            "with member [Measures].[Store Sales Last Period] as "
            + "    '([Measures].[Store Sales], Time.[Time].PrevMember)',\n"
            + "    format='#,###.00'\n"
            + "select\n"
            + "    {[Measures].[Store Sales Last Period]} on columns,\n"
            + "    {TopCount([Product].[Product Department].members,5, [Measures].[Store Sales Last Period])} on rows\n"
            + "from Sales\n"
            + "where ([Time].[1998])",

            "Axis #0:\n"
            + "{[Time].[1998]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Sales Last Period]}\n"
            + "Axis #2:\n"
            + "{[Product].[Food].[Produce]}\n"
            + "{[Product].[Food].[Snack Foods]}\n"
            + "{[Product].[Non-Consumable].[Household]}\n"
            + "{[Product].[Food].[Frozen Foods]}\n"
            + "{[Product].[Food].[Canned Foods]}\n"
            + "Row #0: 82,248.42\n"
            + "Row #1: 67,609.82\n"
            + "Row #2: 60,469.89\n"
            + "Row #3: 55,207.50\n"
            + "Row #4: 39,774.34\n"),

        // 4
        new QueryAndResult(
            "with member [Measures].[Total Store Sales] as 'Sum(YTD(),[Measures].[Store Sales])', format_string='#.00'\n"
            + "select\n"
            + "    {[Measures].[Total Store Sales]} on columns,\n"
            + "    {TopCount([Product].[Product Department].members,5, [Measures].[Total Store Sales])} on rows\n"
            + "from Sales\n"
            + "where ([Time].[1997].[Q2].[4])",

            "Axis #0:\n"
            + "{[Time].[1997].[Q2].[4]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Total Store Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Food].[Produce]}\n"
            + "{[Product].[Food].[Snack Foods]}\n"
            + "{[Product].[Non-Consumable].[Household]}\n"
            + "{[Product].[Food].[Frozen Foods]}\n"
            + "{[Product].[Food].[Canned Foods]}\n"
            + "Row #0: 26526.67\n"
            + "Row #1: 21897.10\n"
            + "Row #2: 19980.90\n"
            + "Row #3: 17882.63\n"
            + "Row #4: 12963.23\n"),

        // 5
        new QueryAndResult(
            "with member [Measures].[Store Profit Rate] as '([Measures].[Store Sales]-[Measures].[Store Cost])/[Measures].[Store Cost]', format = '#.00%'\n"
            + "select\n"
            + "    {[Measures].[Store Cost],[Measures].[Store Sales],[Measures].[Store Profit Rate]} on columns,\n"
            + "    Order([Product].[Product Department].members, [Measures].[Store Profit Rate], BDESC) on rows\n"
            + "from Sales\n"
            + "where ([Time].[1997])",

            "Axis #0:\n"
            + "{[Time].[1997]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Cost]}\n"
            + "{[Measures].[Store Sales]}\n"
            + "{[Measures].[Store Profit Rate]}\n"
            + "Axis #2:\n"
            + "{[Product].[Food].[Breakfast Foods]}\n"
            + "{[Product].[Non-Consumable].[Carousel]}\n"
            + "{[Product].[Food].[Canned Products]}\n"
            + "{[Product].[Food].[Baking Goods]}\n"
            + "{[Product].[Drink].[Alcoholic Beverages]}\n"
            + "{[Product].[Non-Consumable].[Health and Hygiene]}\n"
            + "{[Product].[Food].[Snack Foods]}\n"
            + "{[Product].[Food].[Baked Goods]}\n"
            + "{[Product].[Drink].[Beverages]}\n"
            + "{[Product].[Food].[Frozen Foods]}\n"
            + "{[Product].[Non-Consumable].[Periodicals]}\n"
            + "{[Product].[Food].[Produce]}\n"
            + "{[Product].[Food].[Seafood]}\n"
            + "{[Product].[Food].[Deli]}\n"
            + "{[Product].[Food].[Meat]}\n"
            + "{[Product].[Food].[Canned Foods]}\n"
            + "{[Product].[Non-Consumable].[Household]}\n"
            + "{[Product].[Food].[Starchy Foods]}\n"
            + "{[Product].[Food].[Eggs]}\n"
            + "{[Product].[Food].[Snacks]}\n"
            + "{[Product].[Food].[Dairy]}\n"
            + "{[Product].[Drink].[Dairy]}\n"
            + "{[Product].[Non-Consumable].[Checkout]}\n"
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

        // 6
        new QueryAndResult(
            "with\n"
            + "   member [Product].[All Products].[Drink].[Percent of Alcoholic Drinks] as '[Product].[All Products].[Drink].[Alcoholic Beverages]/[Product].[All Products].[Drink]',\n"
            + "       format_string = '#.00%'\n"
            + "select\n"
            + "   { [Product].[All Products].[Drink].[Percent of Alcoholic Drinks] } on columns,\n"
            + "   order([Customers].[All Customers].[USA].[WA].Children, [Product].[All Products].[Drink].[Percent of Alcoholic Drinks],BDESC) on rows\n"
            + "from Sales\n"
            + "where ([Measures].[Unit Sales])",

            "Axis #0:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #1:\n"
            + "{[Product].[Drink].[Percent of Alcoholic Drinks]}\n"
            + "Axis #2:\n"
            + "{[Customers].[USA].[WA].[Seattle]}\n"
            + "{[Customers].[USA].[WA].[Kirkland]}\n"
            + "{[Customers].[USA].[WA].[Marysville]}\n"
            + "{[Customers].[USA].[WA].[Anacortes]}\n"
            + "{[Customers].[USA].[WA].[Olympia]}\n"
            + "{[Customers].[USA].[WA].[Ballard]}\n"
            + "{[Customers].[USA].[WA].[Bremerton]}\n"
            + "{[Customers].[USA].[WA].[Puyallup]}\n"
            + "{[Customers].[USA].[WA].[Yakima]}\n"
            + "{[Customers].[USA].[WA].[Tacoma]}\n"
            + "{[Customers].[USA].[WA].[Everett]}\n"
            + "{[Customers].[USA].[WA].[Renton]}\n"
            + "{[Customers].[USA].[WA].[Issaquah]}\n"
            + "{[Customers].[USA].[WA].[Bellingham]}\n"
            + "{[Customers].[USA].[WA].[Port Orchard]}\n"
            + "{[Customers].[USA].[WA].[Redmond]}\n"
            + "{[Customers].[USA].[WA].[Spokane]}\n"
            + "{[Customers].[USA].[WA].[Burien]}\n"
            + "{[Customers].[USA].[WA].[Lynnwood]}\n"
            + "{[Customers].[USA].[WA].[Walla Walla]}\n"
            + "{[Customers].[USA].[WA].[Edmonds]}\n"
            + "{[Customers].[USA].[WA].[Sedro Woolley]}\n"
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

        // 7
        new QueryAndResult(
            "with member [Measures].[Accumulated Sales] as 'Sum(YTD(),[Measures].[Store Sales])'\n"
            + "select\n"
            + "    {[Measures].[Store Sales],[Measures].[Accumulated Sales]} on columns,\n"
            + "    {Descendants([Time].[1997],[Time].[Month])} on rows\n"
            + "from Sales",

            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Sales]}\n"
            + "{[Measures].[Accumulated Sales]}\n"
            + "Axis #2:\n"
            + "{[Time].[1997].[Q1].[1]}\n"
            + "{[Time].[1997].[Q1].[2]}\n"
            + "{[Time].[1997].[Q1].[3]}\n"
            + "{[Time].[1997].[Q2].[4]}\n"
            + "{[Time].[1997].[Q2].[5]}\n"
            + "{[Time].[1997].[Q2].[6]}\n"
            + "{[Time].[1997].[Q3].[7]}\n"
            + "{[Time].[1997].[Q3].[8]}\n"
            + "{[Time].[1997].[Q3].[9]}\n"
            + "{[Time].[1997].[Q4].[10]}\n"
            + "{[Time].[1997].[Q4].[11]}\n"
            + "{[Time].[1997].[Q4].[12]}\n"
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

        // 8
        new QueryAndResult(
            "select {[Measures].[Promotion Sales]} on columns\n"
            + " from Sales",

            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Promotion Sales]}\n"
            + "Row #0: 151,211.21\n"),
    };

    public void testSample0() {
        assertQueryReturns(sampleQueries[0].query, sampleQueries[0].result);
    }

    public void testSample1() {
        assertQueryReturns(sampleQueries[1].query, sampleQueries[1].result);
    }

    public void testSample2() {
        assertQueryReturns(sampleQueries[2].query, sampleQueries[2].result);
    }

    public void testSample3() {
        assertQueryReturns(sampleQueries[3].query, sampleQueries[3].result);
    }

    public void testSample4() {
        assertQueryReturns(sampleQueries[4].query, sampleQueries[4].result);
    }

    public void testSample5() {
        assertQueryReturns(sampleQueries[5].query, sampleQueries[5].result);
    }

    public void testSample5Snowflake() {
        propSaver.set(
            MondrianProperties.instance().FilterChildlessSnowflakeMembers,
            false);
        final TestContext context = getTestContext().withFreshConnection();
        try {
            context.assertQueryReturns(
                sampleQueries[5].query,
                "Axis #0:\n"
                + "{[Time].[1997]}\n"
                + "Axis #1:\n"
                + "{[Measures].[Store Cost]}\n"
                + "{[Measures].[Store Sales]}\n"
                + "{[Measures].[Store Profit Rate]}\n"
                + "Axis #2:\n"
                + "{[Product].[Food].[Breakfast Foods]}\n"
                + "{[Product].[Non-Consumable].[Carousel]}\n"
                + "{[Product].[Food].[Canned Products]}\n"
                + "{[Product].[Food].[Baking Goods]}\n"
                + "{[Product].[Drink].[Alcoholic Beverages]}\n"
                + "{[Product].[Non-Consumable].[Health and Hygiene]}\n"
                + "{[Product].[Food].[Snack Foods]}\n"
                + "{[Product].[Food].[Baked Goods]}\n"
                + "{[Product].[Drink].[Beverages]}\n"
                + "{[Product].[Food].[Frozen Foods]}\n"
                + "{[Product].[Non-Consumable].[Periodicals]}\n"
                + "{[Product].[Food].[Produce]}\n"
                + "{[Product].[Food].[Seafood]}\n"
                + "{[Product].[Food].[Deli]}\n"
                + "{[Product].[Food].[Meat]}\n"
                + "{[Product].[Food].[Canned Foods]}\n"
                + "{[Product].[Non-Consumable].[Household]}\n"
                + "{[Product].[Food].[Starchy Foods]}\n"
                + "{[Product].[Food].[Eggs]}\n"
                + "{[Product].[Food].[Snacks]}\n"
                + "{[Product].[Food].[Dairy]}\n"
                + "{[Product].[Drink].[Dairy]}\n"
                + "{[Product].[Non-Consumable].[Checkout]}\n"
                + "{[Product].[Drink].[Baking Goods]}\n"
                + "{[Product].[Food].[Packaged Foods]}\n"
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
                + "Row #22: 147.06%\n"
                + "Row #23: \n"
                + "Row #23: \n"
                + "Row #23: \n"
                + "Row #24: \n"
                + "Row #24: \n"
                + "Row #24: \n");
        } finally {
            context.close();
        }
    }

    public void testSample6() {
        assertQueryReturns(sampleQueries[6].query, sampleQueries[6].result);
    }

    public void testSample7() {
        assertQueryReturns(sampleQueries[7].query, sampleQueries[7].result);
    }

    public void testSample8() {
        if (TestContext.instance().getDialect().getDatabaseProduct()
            == Dialect.DatabaseProduct.INFOBRIGHT)
        {
            // Skip this test on Infobright, because [Promotion Sales] is
            // defined wrong.
            return;
        }
        assertQueryReturns(sampleQueries[8].query, sampleQueries[8].result);
    }

    public void testGoodComments() {
        assertQueryReturns(
            "SELECT {} ON ROWS, {} ON COLUMNS FROM [Sales]/* trailing comment*/",
            EmptyResult);

        String[] comments = {
            "-- a basic comment\n",

            "// another basic comment\n",

            "/* yet another basic comment */",

            "-- a more complicated comment test\n",

            "-- to make it more intesting, -- we'll nest this comment\n",

            "-- also, \"we can put a string in the comment\"\n",

            "-- also, 'even a single quote string'\n",

            "---- and, the comment delimiter is looong\n",

            "/*\n"
            + " * next, how about a comment block?\n"
            + " * with several lines.\n"
            + " * also, \"we can put a string in the comment\"\n"
            + " * also, 'even a single quote string'\n"
            + " * also, -- another style comment is happy\n"
            + " */\n",


            "/* a simple /* nested comment, only needs to be closed once */",

            "/*\n" + " * a multiline /* nested comment\n" + "*/",

            "/**\n"
            + " * a multiline\n"
            + " * /* multiline\n"
            + " *  * nested comment\n"
            + "*/",

            "-- single-line comment containing /* multiline */ comment\n",

            "/* multi-line comment containing -- single-line comment */",
        };


        List<String> allCommentList = new ArrayList<String>();
        for (String comment : comments) {
            allCommentList.add(comment);
            if (comment.indexOf("\n") >= 0) {
                allCommentList.add(comment.replaceAll("\n", "\r\n"));
                allCommentList.add(comment.replaceAll("\n", "\n\r"));
                allCommentList.add(comment.replaceAll("\n", " \n \n "));
            }
        }
        allCommentList.add("");
        final String[] allComments =
            allCommentList.toArray(new String[allCommentList.size()]);

        // The last element of the array is the concatenation of all other
        // comments.
        StringBuilder buf = new StringBuilder();
        for (String allComment : allComments) {
            buf.append(allComment);
        }
        allComments[allComments.length - 1] = buf.toString();

        // Comment at start of query.
        for (String comment : allComments) {
            assertQueryReturns(
                comment + "SELECT {} ON ROWS, {} ON COLUMNS FROM [Sales]",
                EmptyResult);
        }

        // Comment after SELECT.
        for (String comment : allComments) {
            assertQueryReturns(
                "SELECT" + comment + "{} ON ROWS, {} ON COLUMNS FROM [Sales]",
                EmptyResult);
        }

        // Comment within braces.
        for (String comment : allComments) {
            assertQueryReturns(
                "SELECT {" + comment + "} ON ROWS, {} ON COLUMNS FROM [Sales]",
                EmptyResult);
        }

        // Comment after axis name.
        for (String comment : allComments) {
            assertQueryReturns(
                "SELECT {} ON ROWS" + comment + ", {} ON COLUMNS FROM [Sales]",
                EmptyResult);
        }

        // Comment before slicer.
        for (String comment : allComments) {
            assertQueryReturns(
                "SELECT {} ON ROWS, {} ON COLUMNS FROM [Sales] WHERE"
                + comment
                + "([Gender].[F])",
                "Axis #0:\n"
                + "{[Gender].[F]}\n"
                + "Axis #1:\n"
                + "Axis #2:\n");
        }

        // Comment after query.
        for (String comment : allComments) {
            assertQueryReturns(
                "SELECT {} ON ROWS, {} ON COLUMNS FROM [Sales]" + comment,
                EmptyResult);
        }


        assertQueryReturns(
            "-- a comment test with carriage returns at the end of the lines\r\n"
            + "-- first, more than one single-line comment in a row\r\n"
            + "-- and, to make it more intesting, -- we'll nest this comment\r\n"
            + "-- also, \"we can put a string in the comment\"\r\n"
            + "-- also, 'even a single quote string'\r\n"
            + "---- and, the comment delimiter is looong\r\n"
            + "/*\r\n"
            + " * next, now about a comment block?\r\n"
            + " * with several lines.\r\n"
            + " * also, \"we can put a string in the comment\"\r\n"
            + " * also, 'even a single quote comment'\r\n"
            + " * also, -- another style comment is heppy\r\n"
            + " * also, // another style comment is heppy\r\n"
            + " */\r\n"
            + "SELECT {} ON ROWS, {} ON COLUMNS FROM [Sales]\r",
            EmptyResult);

        assertQueryReturns(
            "-- an entire select statement commented out\n"
            + "-- SELECT {} ON ROWS, {} ON COLUMNS FROM [Sales];\n"
            + "/*SELECT {} ON ROWS, {} ON COLUMNS FROM [Sales];*/\n"
            + "// SELECT {} ON ROWS, {} ON COLUMNS FROM [Sales];\n"
            + "SELECT {} ON ROWS, {} ON COLUMNS FROM [Sales]",
            EmptyResult);

        assertQueryReturns(
            "// now for some comments in a larger command\n"
            + "with // create calculate measure [Product].[All Products].[Drink].[Percent of Alcoholic Drinks]\n"
            + "   member [Product].[All Products].[Drink].[Percent of Alcoholic Drinks]/*the measure name*/as '                        // begin the definition of the measure next\n"
            + "       [Product]./****this is crazy****/[All Products].[Drink].[Alcoholic Beverages]/[Product].[All Products].[Drink]',  // divide number of alcoholic drinks by total # of drinks\n"
            + "       format_string = '#.00%'  // a custom format for our measure\n"
            + "select\n"
            + "   { [Product]/**** still crazy ****/.[All Products].[Drink].[Percent of Alcoholic Drinks] } on columns,\n"
            + "   order(/****do not put a comment inside square brackets****/[Customers].[All Customers].[USA].[WA].Children, [Product].[All Products].[Drink].[Percent of Alcoholic Drinks],BDESC) on rows\n"
            + "from Sales\n"
            + "where ([Measures].[Unit Sales] /****,[Time].[1997]****/) -- a comment at the end of the command",

            "Axis #0:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #1:\n"
            + "{[Product].[Drink].[Percent of Alcoholic Drinks]}\n"
            + "Axis #2:\n"
            + "{[Customers].[USA].[WA].[Seattle]}\n"
            + "{[Customers].[USA].[WA].[Kirkland]}\n"
            + "{[Customers].[USA].[WA].[Marysville]}\n"
            + "{[Customers].[USA].[WA].[Anacortes]}\n"
            + "{[Customers].[USA].[WA].[Olympia]}\n"
            + "{[Customers].[USA].[WA].[Ballard]}\n"
            + "{[Customers].[USA].[WA].[Bremerton]}\n"
            + "{[Customers].[USA].[WA].[Puyallup]}\n"
            + "{[Customers].[USA].[WA].[Yakima]}\n"
            + "{[Customers].[USA].[WA].[Tacoma]}\n"
            + "{[Customers].[USA].[WA].[Everett]}\n"
            + "{[Customers].[USA].[WA].[Renton]}\n"
            + "{[Customers].[USA].[WA].[Issaquah]}\n"
            + "{[Customers].[USA].[WA].[Bellingham]}\n"
            + "{[Customers].[USA].[WA].[Port Orchard]}\n"
            + "{[Customers].[USA].[WA].[Redmond]}\n"
            + "{[Customers].[USA].[WA].[Spokane]}\n"
            + "{[Customers].[USA].[WA].[Burien]}\n"
            + "{[Customers].[USA].[WA].[Lynnwood]}\n"
            + "{[Customers].[USA].[WA].[Walla Walla]}\n"
            + "{[Customers].[USA].[WA].[Edmonds]}\n"
            + "{[Customers].[USA].[WA].[Sedro Woolley]}\n"
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
            + "Row #21: 13.79%\n");
    }

    public void testBadComments() {
        // Comments cannot appear inside identifiers.
        assertQueryThrows(
            "SELECT {[Measures].[Unit Sales]} ON COLUMNS,\n"
            + " {[Gender].MEMBERS} ON ROWS\n"
            + "FROM [Sales]\n"
            + "WHERE {[/***an illegal comment****/Marital Status].[S]}",
            "Failed to parse query");

        // Nested comments only need to be closed once.
        assertQueryThrows(
            "/* a simple /* nested */ comment */\n"
            + "SELECT {} ON ROWS, {} ON COLUMNS FROM [Sales]",
            "Failed to parse query");

        // We support \r as a line-end delimiter.
        assertQueryReturns(
            "SELECT {} ON COLUMNS -- comment terminated by CR only\r, {} ON ROWS FROM [Sales]",
            EmptyResult);
    }

    /**
     * Tests that a query whose axes are empty works; bug
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-52">MONDRIAN-52</a>.
     */
    public void testBothAxesEmpty() {
        assertQueryReturns(
            "SELECT {} ON ROWS, {} ON COLUMNS FROM [Sales]",
            EmptyResult);

        // expression which evaluates to empty set
        assertQueryReturns(
            "SELECT Filter({[Gender].MEMBERS}, 1 = 0) ON COLUMNS, \n"
            + "{} ON ROWS\n"
            + "FROM [Sales]",
            EmptyResult);

        // with slicer
        assertQueryReturns(
            "SELECT {} ON ROWS, {} ON COLUMNS \n"
            + "FROM [Sales] WHERE ([Gender].[F])",
            "Axis #0:\n"
            + "{[Gender].[F]}\n"
            + "Axis #1:\n"
            + "Axis #2:\n");
    }

    /**
     * Used to test that a slicer with multiple values gives an error; bug
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-96">MONDRIAN-96</a>.
     * But now compound slicers are valid.
     */
    public void testCompoundSlicer() {
        // two tuples
        assertQueryReturns(
            "SELECT {[Measures].[Unit Sales]} ON COLUMNS,\n"
            + " {[Gender].MEMBERS} ON ROWS\n"
            + "FROM [Sales]\n"
            + "WHERE {([Marital Status].[S]),\n"
            + "       ([Marital Status].[M])}",
            "Axis #0:\n"
            + "{[Marital Status].[S]}\n"
            + "{[Marital Status].[M]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Gender].[All Gender]}\n"
            + "{[Gender].[F]}\n"
            + "{[Gender].[M]}\n"
            + "Row #0: 266,773\n"
            + "Row #1: 131,558\n"
            + "Row #2: 135,215\n");

        // set with incompatible members
        assertQueryThrows(
            "SELECT {[Measures].[Unit Sales]} ON COLUMNS,\n"
            + " {[Gender].MEMBERS} ON ROWS\n"
            + "FROM [Sales]\n"
            + "WHERE {[Marital Status].[S],\n"
            + "       [Product]}",
            "All arguments to function '{}' must have same hierarchy.");

        // expression which evaluates to a set with zero members used to be an
        // error - now it's ok; cells are null because they are aggregating over
        // nothing
        assertQueryReturns(
            "SELECT {[Measures].[Unit Sales]} ON COLUMNS,\n"
            + " {[Gender].MEMBERS} ON ROWS\n"
            + "FROM [Sales]\n"
            + "WHERE Filter({[Marital Status].MEMBERS}, 1 = 0)",
            "Axis #0:\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Gender].[All Gender]}\n"
            + "{[Gender].[F]}\n"
            + "{[Gender].[M]}\n"
            + "Row #0: \n"
            + "Row #1: \n"
            + "Row #2: \n");

        // expression which evaluates to a not-null member is ok
        assertQueryReturns(
            "SELECT {[Measures].[Unit Sales]} ON COLUMNS,\n"
            + " {[Gender].MEMBERS} ON ROWS\n"
            + "FROM [Sales]\n"
            + "WHERE ({Filter({[Marital Status].MEMBERS}, [Measures].[Unit Sales] = 266773)}.Item(0))",
            "Axis #0:\n"
            + "{[Marital Status].[All Marital Status]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Gender].[All Gender]}\n"
            + "{[Gender].[F]}\n"
            + "{[Gender].[M]}\n"
            + "Row #0: 266,773\n"
            + "Row #1: 131,558\n"
            + "Row #2: 135,215\n");

        // Expression which evaluates to a null member used to be an error; now
        // it is an unsatisfiable condition, so cells come out empty.
        // Confirmed with SSAS 2005.
        assertQueryReturns(
            "SELECT {[Measures].[Unit Sales]} ON COLUMNS,\n"
            + " {[Gender].MEMBERS} ON ROWS\n"
            + "FROM [Sales]\n"
            + "WHERE [Marital Status].Parent",
            "Axis #0:\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Gender].[All Gender]}\n"
            + "{[Gender].[F]}\n"
            + "{[Gender].[M]}\n"
            + "Row #0: \n"
            + "Row #1: \n"
            + "Row #2: \n");

        // expression which evaluates to a set with one member is ok
        assertQueryReturns(
            "SELECT {[Measures].[Unit Sales]} ON COLUMNS,\n"
            + " {[Gender].MEMBERS} ON ROWS\n"
            + "FROM [Sales]\n"
            + "WHERE Filter({[Marital Status].MEMBERS}, [Measures].[Unit Sales] = 266773)",
            "Axis #0:\n"
            + "{[Marital Status].[All Marital Status]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Gender].[All Gender]}\n"
            + "{[Gender].[F]}\n"
            + "{[Gender].[M]}\n"
            + "Row #0: 266,773\n"
            + "Row #1: 131,558\n"
            + "Row #2: 135,215\n");

        // Slicer expression which evaluates to three tuples is ok now we
        // support compound slicers. But the results must not double-count if,
        // as in this case, a member and its parent both appear in the list.
        // In that respect, an MDX WHERE clause behaves more like a SQL WHERE
        // clause (applying the condition to each cell) than the MDX Aggregate
        // function (summing over all cells that pass).
        assertQueryReturns(
            "SELECT {[Measures].[Unit Sales]} ON COLUMNS,\n"
            + " {[Gender].MEMBERS} ON ROWS\n"
            + "FROM [Sales]\n"
            + "WHERE Filter({[Marital Status].MEMBERS}, [Measures].[Unit Sales] <= 266773)",
            "Axis #0:\n"
            + "{[Marital Status].[All Marital Status]}\n"
            + "{[Marital Status].[M]}\n"
            + "{[Marital Status].[S]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Gender].[All Gender]}\n"
            + "{[Gender].[F]}\n"
            + "{[Gender].[M]}\n"
            + "Row #0: 266,773\n"
            + "Row #1: 131,558\n"
            + "Row #2: 135,215\n");

        // set with incompatible members
        assertQueryThrows(
            "SELECT {[Measures].[Unit Sales]} ON COLUMNS,\n"
            + " {[Gender].MEMBERS} ON ROWS\n"
            + "FROM [Sales]\n"
            + "WHERE {[Marital Status].[S],\n"
            + "       [Product]}",
            "All arguments to function '{}' must have same hierarchy.");

        // two members of same dimension in columns and rows
        assertQueryThrows(
            "SELECT CrossJoin(\n"
            + "  {[Measures].[Unit Sales]},\n"
            + "  {[Gender].[M]}) ON COLUMNS,\n"
            + " {[Gender].MEMBERS} ON ROWS\n"
            + "FROM [Sales]",
            "Hierarchy '[Gender]' appears in more than one independent axis.");

        // two members of same dimension in rows and filter
        assertQueryThrows(
            "SELECT {[Measures].[Unit Sales]} ON COLUMNS,\n"
            + " {[Gender].MEMBERS} ON ROWS\n"
            + "FROM [Sales]"
            + "WHERE ([Marital Status].[S], [Gender].[F])",
            "Hierarchy '[Gender]' appears in more than one independent axis.");

        // members of different hierarchies of the same dimension in rows and
        // filter
        assertQueryReturns(
            "SELECT {[Measures].[Unit Sales]} ON COLUMNS,\n"
            + " {[Time].[1997].Children} ON ROWS\n"
            + "FROM [Sales]"
            + "WHERE ([Marital Status].[S], " + timeWeekly + ".[1997].[20])",
            "Axis #0:\n"
            + "{[Marital Status].[S], [Time].[Weekly].[1997].[20]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Time].[1997].[Q1]}\n"
            + "{[Time].[1997].[Q2]}\n"
            + "{[Time].[1997].[Q3]}\n"
            + "{[Time].[1997].[Q4]}\n"
            + "Row #0: \n"
            + "Row #1: 3,523\n"
            + "Row #2: \n"
            + "Row #3: \n");

        // two members of same dimension in slicer tuple
        assertQueryThrows(
            "SELECT {[Measures].[Unit Sales]} ON COLUMNS,\n"
            + " {[Gender].MEMBERS} ON ROWS\n"
            + "FROM [Sales]"
            + "WHERE ([Marital Status].[S], [Marital Status].[M])",
            "Tuple contains more than one member of hierarchy '[Marital Status]'.");

        // two members of different hierarchies of the same dimension in the
        // slicer tuple
        assertQueryReturns(
            "SELECT {[Measures].[Unit Sales]} ON COLUMNS,\n"
            + " {[Gender].MEMBERS} ON ROWS\n"
            + "FROM [Sales]"
            + "WHERE ([Time].[1997].[Q1], " + timeWeekly + ".[1997].[4])",
            "Axis #0:\n"
            + "{[Time].[1997].[Q1], [Time].[Weekly].[1997].[4]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Gender].[All Gender]}\n"
            + "{[Gender].[F]}\n"
            + "{[Gender].[M]}\n"
            + "Row #0: 4,908\n"
            + "Row #1: 2,354\n"
            + "Row #2: 2,554\n");

        // testcase for bug MONDRIAN-68, "Member appears in slicer and other
        // axis should be illegal"
        assertQueryThrows(
            "select\n"
            + "{[Measures].[Unit Sales]} on columns,\n"
            + "{([Product].[All Products], [Time].[1997])} ON rows\n"
            + "from Sales\n"
            + "where ([Time].[1997])",
            "Hierarchy '[Time]' appears in more than one independent axis.");

        // different hierarchies of same dimension on slicer and other axis
        assertQueryReturns(
            "select\n"
            + "{[Measures].[Unit Sales]} on columns,\n"
            + "{([Product].[All Products], "
            + timeWeekly + ".[1997])} ON rows\n"
            + "from Sales\n"
            + "where ([Time].[1997])",
            "Axis #0:\n"
            + "{[Time].[1997]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[All Products], [Time].[Weekly].[1997]}\n"
            + "Row #0: 266,773\n");
    }

    /**
     * Test case for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-814">MONDRIAN-814,
     * "MDX with specific where clause doesn't work" </a>. This test case
     * was as close as I could get to the original test case on the foodmart
     * data set, but it did not reproduce the bug.
     */
    public void testCompoundSlicerNonEmpty() {
        // With MONDRIAN-814, cell totals would be about a factor of 4 smaller,
        // and the number of rows returned would be the same (1220) if 21, 22
        // and 23 were removed from the slicer.
        final String mdx =
            "select non empty [Measures].[Sales Count] on 0,\n"
            + " non empty hierarchize(\n"
            + "  Crossjoin(\n"
            + "    Union(\n"
            + "     [Gender].CurrentMember,\n"
            + "     [Gender].Children),\n"
            + "    Union(\n"
            + "     [Product].CurrentMember,\n"
            + "     [Product].[Brand Name].Members))) on 1\n"
            + "from [Sales]\n"
            + "where { " + timeWeekly + ".[1997].[20]"
            + "      , " + timeWeekly + ".[1997].[21]"
            + "      , " + timeWeekly + ".[1997].[22]"
            + "      , " + timeWeekly + ".[1997].[23]"
            + " }";
        if (false) {
            // Output too large to check in.
            assertQueryReturns(mdx, "xxxx");
        }
        Result result = getTestContext().executeQuery(mdx);
        assertEquals(1477, result.getAxes()[1].getPositions().size());
        assertEquals(
            "5,896", result.getCell(new int[] {0, 0}).getFormattedValue());
    }

    public void testEmptyTupleSlicerFails() {
        assertQueryThrows(
            "select [Measures].[Unit Sales] on 0,\n"
            + "[Product].Children on 1\n"
            + "from [Warehouse and Sales]\n"
            + "where ()",
            "Syntax error at line 4, column 8, token ')'");
    }

    /**
     * Requires the use of a sparse segment, because the product dimension
     * has 6 atttributes, the product of whose cardinalities is ~8M. If we
     * use a dense segment, we run out of memory trying to allocate a huge
     * array.
     */
    public void testBigQuery() {
        Result result = executeQuery(
            "SELECT {[Measures].[Unit Sales]} on columns,\n"
            + " {[Product].members} on rows\n"
            + "from Sales");
        final int rowCount = result.getAxes()[1].getPositions().size();
        assertEquals(
            MondrianProperties.instance().FilterChildlessSnowflakeMembers.get()
                ? 2256
                : 2266,
            rowCount);
        assertEquals(
            "152",
            result.getCell(
                new int[]{
                    0, rowCount - 1
                }).getFormattedValue());
    }

    /**
     * Unit test for the {@link Cell#getContextMember(mondrian.olap.Hierarchy)}
     * method.
     */
    public void testGetContext() {
        if (!MondrianProperties.instance().SsasCompatibleNaming.get()) {
            return;
        }
        Result result =
            getTestContext().executeQuery(
                "select [Gender].Members on 0,\n"
                + "[Time].[Weekly].[1997].[6].Children on 1\n"
                + "from [Sales]\n"
                + "where [Marital Status].[S]");
        final Cell cell = result.getCell(new int[]{0, 0});
        final Map<String, Hierarchy> hierarchyMap =
            new HashMap<String, Hierarchy>();
        for (Dimension dimension : result.getQuery().getCube().getDimensions())
        {
            for (Hierarchy hierarchy : dimension.getHierarchies()) {
                hierarchyMap.put(hierarchy.getUniqueName(), hierarchy);
            }
        }
        assertEquals(
            "[Measures].[Unit Sales]",
            cell.getContextMember(hierarchyMap.get("[Measures]"))
                .getUniqueName());
        assertEquals(
            "[Time].[1997]",
            cell.getContextMember(hierarchyMap.get("[Time]"))
                .getUniqueName());
        assertEquals(
            "[Time].[Weekly].[1997].[6].[1]",
            cell.getContextMember(hierarchyMap.get("[Time].[Weekly]"))
                .getUniqueName());
        assertEquals(
            "[Gender].[All Gender]",
            cell.getContextMember(hierarchyMap.get("[Gender]"))
                .getUniqueName());
        assertEquals(
            "[Marital Status].[S]",
            cell.getContextMember(hierarchyMap.get("[Marital Status]"))
                .getUniqueName());
    }

    public void testNonEmpty1() {
        assertSize(
            "select\n"
            + "  NON EMPTY CrossJoin({[Product].[All Products].[Drink].Children},\n"
            + "    {[Customers].[All Customers].[USA].[WA].[Bellingham]}) on rows,\n"
            + "  CrossJoin(\n"
            + "    {[Measures].[Unit Sales], [Measures].[Store Sales]},\n"
            + "    { [Promotion Media].[All Media].[Radio],\n"
            + "      [Promotion Media].[All Media].[TV],\n"
            + "      [Promotion Media].[All Media].[Sunday Paper],\n"
            + "      [Promotion Media].[All Media].[Street Handout] }\n"
            + "   ) on columns\n"
            + "from Sales\n"
            + "where ([Time].[1997])",
            8, 2);
    }

    public void testNonEmpty2() {
        assertSize(
            "select\n"
            + "  NON EMPTY CrossJoin(\n"
            + "    {[Product].[All Products].Children},\n"
            + "    {[Customers].[All Customers].[USA].[WA].[Bellingham]}) on rows,\n"
            + "  NON EMPTY CrossJoin(\n"
            + "    {[Measures].[Unit Sales]},\n"
            + "    { [Promotion Media].[All Media].[Cash Register Handout],\n"
            + "      [Promotion Media].[All Media].[Sunday Paper],\n"
            + "      [Promotion Media].[All Media].[Street Handout] }\n"
            + "   ) on columns\n"
            + "from Sales\n"
            + "where ([Time].[1997])",
            2, 2);
    }

    public void testOneDimensionalQueryWithTupleAsSlicer() {
        Result result = executeQuery(
            "select\n"
            + "  [Product].[All Products].[Drink].children on columns\n"
            + "from Sales\n"
            + "where ([Measures].[Unit Sales], [Promotion Media].[All Media].[Street Handout], [Time].[1997])");
        assertTrue(result.getAxes().length == 1);
        assertTrue(result.getAxes()[0].getPositions().size() == 3);
        assertTrue(result.getSlicerAxis().getPositions().size() == 1);
        assertTrue(result.getSlicerAxis().getPositions().get(0).size() == 3);
    }

    public void testSlicerIsEvaluatedBeforeAxes() {
        // about 10 products exceeded 20000 units in 1997, only 2 for Q1
        assertSize(
            "SELECT {[Measures].[Unit Sales]} on columns,\n"
            + " filter({[Product].members}, [Measures].[Unit Sales] > 20000) on rows\n"
            + "FROM Sales\n"
            + "WHERE [Time].[1997].[Q1]", 1, 2);
    }

    public void testSlicerWithCalculatedMembers() {
        assertSize(
            "WITH Member [Time].[Time].[1997].[H1] as ' Aggregate({[Time].[1997].[Q1], [Time].[1997].[Q2]})' \n"
            + "  MEMBER [Measures].[Store Margin] as '[Measures].[Store Sales] - [Measures].[Store Cost]'\n"
            + "SELECT {[Gender].members} on columns,\n"
            + " filter({[Product].members}, [Gender].[F] > 10000) on rows\n"
            + "FROM Sales\n"
            + "WHERE ([Time].[1997].[H1], [Measures].[Store Margin])",
            3,
            6);
    }

    public void _testEver() {
        assertQueryReturns(
            "select\n"
            + " {[Measures].[Unit Sales], [Measures].[Ever]} on columns,\n"
            + " [Gender].members on rows\n"
            + "from Sales", "xxx");
    }

    public void _testDairy() {
        assertQueryReturns(
            "with\n"
            + "  member [Product].[Non dairy] as '[Product].[All Products] - [Product].[Food].[Dairy]'\n"
            + "  member [Measures].[Dairy ever] as 'sum([Time].members, ([Measures].[Unit Sales],[Product].[Food].[Dairy]))'\n"
            + "  set [Customers who never bought dairy] as 'filter([Customers].members, [Measures].[Dairy ever] = 0)'\n"
            + "select\n"
            + " {[Measures].[Unit Sales], [Measures].[Dairy ever]}  on columns,\n"
            + "  [Customers who never bought dairy] on rows\n"
            + "from Sales", "xxx");
    }

    public void testSolveOrder() {
        assertQueryReturns(
            "WITH\n"
            + "   MEMBER [Measures].[StoreType] AS \n"
            + "   '[Store].CurrentMember.Properties(\"Store Type\")',\n"
            + "   SOLVE_ORDER = 2\n"
            + "   MEMBER [Measures].[ProfitPct] AS \n"
            + "   '(Measures.[Store Sales] - Measures.[Store Cost]) / Measures.[Store Sales]',\n"
            + "   SOLVE_ORDER = 1, FORMAT_STRING = '##.00%'\n"
            + "SELECT\n"
            + "   { Descendants([Store].[USA], [Store].[Store Name])} ON COLUMNS,\n"
            + "   { [Measures].[Store Sales], [Measures].[Store Cost], [Measures].[StoreType],\n"
            + "   [Measures].[ProfitPct] } ON ROWS\n"
            + "FROM Sales",

            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[USA].[CA].[Alameda].[HQ]}\n"
            + "{[Store].[USA].[CA].[Beverly Hills].[Store 6]}\n"
            + "{[Store].[USA].[CA].[Los Angeles].[Store 7]}\n"
            + "{[Store].[USA].[CA].[San Diego].[Store 24]}\n"
            + "{[Store].[USA].[CA].[San Francisco].[Store 14]}\n"
            + "{[Store].[USA].[OR].[Portland].[Store 11]}\n"
            + "{[Store].[USA].[OR].[Salem].[Store 13]}\n"
            + "{[Store].[USA].[WA].[Bellingham].[Store 2]}\n"
            + "{[Store].[USA].[WA].[Bremerton].[Store 3]}\n"
            + "{[Store].[USA].[WA].[Seattle].[Store 15]}\n"
            + "{[Store].[USA].[WA].[Spokane].[Store 16]}\n"
            + "{[Store].[USA].[WA].[Tacoma].[Store 17]}\n"
            + "{[Store].[USA].[WA].[Walla Walla].[Store 22]}\n"
            + "{[Store].[USA].[WA].[Yakima].[Store 23]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Store Sales]}\n"
            + "{[Measures].[Store Cost]}\n"
            + "{[Measures].[StoreType]}\n"
            + "{[Measures].[ProfitPct]}\n"
            + "Row #0: \n"
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
            + "Row #1: \n"
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
            + "Row #2: HeadQuarters\n"
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
            + "Row #3: \n"
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
            + "Row #3: 60.07%\n");
    }

    public void testSolveOrderNonMeasure() {
        assertQueryReturns(
            "WITH\n"
            + "   MEMBER [Product].[ProdCalc] as '1', SOLVE_ORDER=1\n"
            + "   MEMBER [Measures].[MeasuresCalc] as '2', SOLVE_ORDER=2\n"
            + "   Member [Time].[Time].[1997].[TimeCalc] as '3', SOLVE_ORDER=3\n"
            + "SELECT\n"
            + "   { [Product].[ProdCalc] } ON columns,\n"
            + "   {([Time].[1997].[TimeCalc],\n"
            + "     [Measures].[MeasuresCalc])} ON rows\n"
            + "FROM Sales",

            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Product].[ProdCalc]}\n"
            + "Axis #2:\n"
            + "{[Time].[1997].[TimeCalc], [Measures].[MeasuresCalc]}\n"
            + "Row #0: 3\n");
    }

    public void testSolveOrderNonMeasure2() {
        assertQueryReturns(
            "WITH\n"
            + "   MEMBER [Store].[StoreCalc] as '0', SOLVE_ORDER=0\n"
            + "   MEMBER [Product].[ProdCalc] as '1', SOLVE_ORDER=1\n"
            + "SELECT\n"
            + "   { [Product].[ProdCalc] } ON columns,\n"
            + "   { [Store].[StoreCalc] } ON rows\n"
            + "FROM Sales",

            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Product].[ProdCalc]}\n"
            + "Axis #2:\n"
            + "{[Store].[StoreCalc]}\n"
            + "Row #0: 1\n");
    }

    /**
     * Test what happens when the solve orders are the same. According to
     * http://msdn.microsoft.com/library/en-us/olapdmad/agmdxadvanced_6jn7.asp
     * if solve orders are the same then the dimension specified first
     * when defining the cube wins.
     *
     * <p>In the first test, the answer should be 1 because Promotions
     * comes before Customers in the FoodMart.xml schema.
     */
    public void testSolveOrderAmbiguous1() {
        assertQueryReturns(
            "WITH\n"
            + "   MEMBER [Promotions].[Calc] AS '1'\n"
            + "   MEMBER [Customers].[Calc] AS '2'\n"
            + "SELECT\n"
            + "   { [Promotions].[Calc] } ON COLUMNS,\n"
            + "   {  [Customers].[Calc] } ON ROWS\n"
            + "FROM Sales",

            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Promotions].[Calc]}\n"
            + "Axis #2:\n"
            + "{[Customers].[Calc]}\n"
            + "Row #0: 1\n");
    }

    /**
     * In the second test, the answer should be 2 because Product comes before
     * Promotions in the FoodMart.xml schema.
     */
    public void testSolveOrderAmbiguous2() {
        assertQueryReturns(
            "WITH\n"
            + "   MEMBER [Promotions].[Calc] AS '1'\n"
            + "   MEMBER [Product].[Calc] AS '2'\n"
            + "SELECT\n"
            + "   { [Promotions].[Calc] } ON COLUMNS,\n"
            + "   { [Product].[Calc] } ON ROWS\n"
            + "FROM Sales",

            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Promotions].[Calc]}\n"
            + "Axis #2:\n"
            + "{[Product].[Calc]}\n"
            + "Row #0: 2\n");
    }

    public void testCalculatedMemberWhichIsNotAMeasure() {
        assertQueryReturns(
            "WITH MEMBER [Product].[BigSeller] AS\n"
            + "  'IIf([Product].[Drink].[Alcoholic Beverages].[Beer and Wine] > 100, \"Yes\",\"No\")'\n"
            + "SELECT {[Product].[BigSeller],[Product].children} ON COLUMNS,\n"
            + "   {[Store].[USA].[CA].children} ON ROWS\n"
            + "FROM Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Product].[BigSeller]}\n"
            + "{[Product].[Drink]}\n"
            + "{[Product].[Food]}\n"
            + "{[Product].[Non-Consumable]}\n"
            + "Axis #2:\n"
            + "{[Store].[USA].[CA].[Alameda]}\n"
            + "{[Store].[USA].[CA].[Beverly Hills]}\n"
            + "{[Store].[USA].[CA].[Los Angeles]}\n"
            + "{[Store].[USA].[CA].[San Diego]}\n"
            + "{[Store].[USA].[CA].[San Francisco]}\n"
            + "Row #0: No\n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #1: Yes\n"
            + "Row #1: 1,945\n"
            + "Row #1: 15,438\n"
            + "Row #1: 3,950\n"
            + "Row #2: Yes\n"
            + "Row #2: 2,422\n"
            + "Row #2: 18,294\n"
            + "Row #2: 4,947\n"
            + "Row #3: Yes\n"
            + "Row #3: 2,560\n"
            + "Row #3: 18,369\n"
            + "Row #3: 4,706\n"
            + "Row #4: No\n"
            + "Row #4: 175\n"
            + "Row #4: 1,555\n"
            + "Row #4: 387\n");
    }

    public void testMultipleCalculatedMembersWhichAreNotMeasures() {
        assertQueryReturns(
            "WITH\n"
            + "  MEMBER [Store].[x] AS '1'\n"
            + "  MEMBER [Product].[x] AS '1'\n"
            + "SELECT {[Store].[x]} ON COLUMNS\n"
            + "FROM Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[x]}\n"
            + "Row #0: 1\n");
    }

    /**
     * Testcase for bug <a href="http://jira.pentaho.com/browse/MONDRIAN-77">
     * MONDRIAN-77, "Calculated member name conflict"</a>.
     *
     * <p>There used to be something wrong with non-measure calculated members
     * where the ordering of the WITH MEMBER would determine whether or not
     * the member would be found in the cube. This test would fail but the
     * previous one would work ok.
     */
    public void testMultipleCalculatedMembersWhichAreNotMeasures2() {
        assertQueryReturns(
            "WITH\n"
            + "  MEMBER [Product].[x] AS '1'\n"
            + "  MEMBER [Store].[x] AS '1'\n"
            + "SELECT {[Store].[x]} ON COLUMNS\n"
            + "FROM Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[x]}\n"
            + "Row #0: 1\n");
    }

    /**
     * This one had the same problem. It wouldn't find the
     * [Store].[x] member because it has the same leaf
     * name as [Product].[x]. (See MONDRIAN-77.)
     */
    public void testMultipleCalculatedMembersWhichAreNotMeasures3() {
        assertQueryReturns(
            "WITH\n"
            + "  MEMBER [Product].[x] AS '1'\n"
            + "  MEMBER [Store].[x] AS '1'\n"
            + "SELECT {[Store].[x]} ON COLUMNS\n"
            + "FROM Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[x]}\n"
            + "Row #0: 1\n");
    }

    public void testConstantString() {
        String s = executeExpr(" \"a string\" ");
        assertEquals("a string", s);
    }

    public void testConstantNumber() {
        String s = executeExpr(" 1234 ");
        assertEquals("1,234", s);
    }

    public void testCyclicalCalculatedMembers() {
        Util.discard(
            executeQuery(
                "WITH\n"
                + "   MEMBER [Product].[X] AS '[Product].[Y]'\n"
                + "   MEMBER [Product].[Y] AS '[Product].[X]'\n"
                + "SELECT\n"
                + "   {[Product].[X]} ON COLUMNS,\n"
                + "   {Store.[Store Name].Members} ON ROWS\n"
                + "FROM Sales"));
    }

    /**
     * Disabled test. It used throw an 'infinite loop' error (which is what
     * Plato does). But now we revert to the context of the default member when
     * calculating calculated members (we used to stay in the context of the
     * calculated member), and we get a result.
     */
    public void testCycle() {
        if (false) {
            assertExprThrows("[Time].[1997].[Q4]", "infinite loop");
        } else {
            String s = executeExpr("[Time].[1997].[Q4]");
            assertEquals("72,024", s);
        }
    }

    public void testHalfYears() {
        Util.discard(
            executeQuery(
                "WITH MEMBER [Measures].[ProfitPercent] AS\n"
                + "     '([Measures].[Store Sales]-[Measures].[Store Cost])/([Measures].[Store Cost])',\n"
                + " FORMAT_STRING = '#.00%', SOLVE_ORDER = 1\n"
                + " Member [Time].[Time].[1997].[First Half] AS  '[Time].[1997].[Q1] + [Time].[1997].[Q2]'\n"
                + " Member [Time].[Time].[1997].[Second Half] AS '[Time].[1997].[Q3] + [Time].[1997].[Q4]'\n"
                + " SELECT {[Time].[1997].[First Half],\n"
                + "     [Time].[1997].[Second Half],\n"
                + "     [Time].[1997].CHILDREN} ON COLUMNS,\n"
                + " {[Store].[Store Country].[USA].CHILDREN} ON ROWS\n"
                + " FROM [Sales]\n"
                + " WHERE ([Measures].[ProfitPercent])"));
    }

    public void _testHalfYearsTrickyCase() {
        Util.discard(
            executeQuery(
                "WITH MEMBER MEASURES.ProfitPercent AS\n"
                + "     '([Measures].[Store Sales]-[Measures].[Store Cost])/([Measures].[Store Cost])',\n"
                + " FORMAT_STRING = '#.00%', SOLVE_ORDER = 1\n"
                + " Member [Time].[Time].[First Half 97] AS  '[Time].[1997].[Q1] + [Time].[1997].[Q2]'\n"
                + " Member [Time].[Time].[Second Half 97] AS '[Time].[1997].[Q3] + [Time].[1997].[Q4]'\n"
                + " SELECT {[Time].[First Half 97],\n"
                + "     [Time].[Second Half 97],\n"
                + "     [Time].[1997].CHILDREN} ON COLUMNS,\n"
                + " {[Store].[Store Country].[USA].CHILDREN} ON ROWS\n"
                + " FROM [Sales]\n"
                + " WHERE (MEASURES.ProfitPercent)"));
    }

    public void testAsSample7ButUsingVirtualCube() {
        Util.discard(
            executeQuery(
                "with member [Measures].[Accumulated Sales] as 'Sum(YTD(),[Measures].[Store Sales])'\n"
                + "select\n"
                + "    {[Measures].[Store Sales],[Measures].[Accumulated Sales]} on columns,\n"
                + "    {Descendants([Time].[1997],[Time].[Month])} on rows\n"
                + "from [Warehouse and Sales]"));
    }

    public void testVirtualCube() {
        assertQueryReturns(
            // Note that Unit Sales is independent of Warehouse.
            "select CrossJoin(\n"
            + "  {[Warehouse].DefaultMember, [Warehouse].[USA].children},\n"
            + "  {[Measures].[Unit Sales], [Measures].[Store Sales], [Measures].[Units Shipped]}) on columns,\n"
            + " [Time].[Time].children on rows\n"
            + "from [Warehouse and Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Warehouse].[All Warehouses], [Measures].[Unit Sales]}\n"
            + "{[Warehouse].[All Warehouses], [Measures].[Store Sales]}\n"
            + "{[Warehouse].[All Warehouses], [Measures].[Units Shipped]}\n"
            + "{[Warehouse].[USA].[CA], [Measures].[Unit Sales]}\n"
            + "{[Warehouse].[USA].[CA], [Measures].[Store Sales]}\n"
            + "{[Warehouse].[USA].[CA], [Measures].[Units Shipped]}\n"
            + "{[Warehouse].[USA].[OR], [Measures].[Unit Sales]}\n"
            + "{[Warehouse].[USA].[OR], [Measures].[Store Sales]}\n"
            + "{[Warehouse].[USA].[OR], [Measures].[Units Shipped]}\n"
            + "{[Warehouse].[USA].[WA], [Measures].[Unit Sales]}\n"
            + "{[Warehouse].[USA].[WA], [Measures].[Store Sales]}\n"
            + "{[Warehouse].[USA].[WA], [Measures].[Units Shipped]}\n"
            + "Axis #2:\n"
            + "{[Time].[1997].[Q1]}\n"
            + "{[Time].[1997].[Q2]}\n"
            + "{[Time].[1997].[Q3]}\n"
            + "{[Time].[1997].[Q4]}\n"
            + "Row #0: 66,291\n"
            + "Row #0: 139,628.35\n"
            + "Row #0: 50951.0\n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: 8539.0\n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: 7994.0\n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: 34418.0\n"
            + "Row #1: 62,610\n"
            + "Row #1: 132,666.27\n"
            + "Row #1: 49187.0\n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: 15726.0\n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: 7575.0\n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: 25886.0\n"
            + "Row #2: 65,848\n"
            + "Row #2: 140,271.89\n"
            + "Row #2: 57789.0\n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: 20821.0\n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: 8673.0\n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: 28295.0\n"
            + "Row #3: 72,024\n"
            + "Row #3: 152,671.62\n"
            + "Row #3: 49799.0\n"
            + "Row #3: \n"
            + "Row #3: \n"
            + "Row #3: 15791.0\n"
            + "Row #3: \n"
            + "Row #3: \n"
            + "Row #3: 16666.0\n"
            + "Row #3: \n"
            + "Row #3: \n"
            + "Row #3: 17342.0\n");
    }

    public void testUseDimensionAsShorthandForMember() {
        Util.discard(
            executeQuery(
                "select {[Measures].[Unit Sales]} on columns,\n"
                + " {[Store], [Store].children} on rows\n"
                + "from [Sales]"));
    }

    public void _testMembersFunction() {
        Util.discard(
            executeQuery(
                "select {[Measures].[Unit Sales]} on columns,\n"
                + " {[Customers].members(0)} on rows\n"
                + "from [Sales]"));
    }

    public void _testProduct2() {
        final Axis axis = getTestContext().executeAxis("{[Product2].members}");
        System.out.println(TestContext.toString(axis.getPositions()));
    }

    private static final List<QueryAndResult> taglibQueries = Arrays.asList(
        // 0
        new QueryAndResult(
            "select\n"
            + "  {[Measures].[Unit Sales], [Measures].[Store Cost], [Measures].[Store Sales]} on columns,\n"
            + "  CrossJoin(\n"
            + "    { [Promotion Media].[All Media].[Radio],\n"
            + "      [Promotion Media].[All Media].[TV],\n"
            + "      [Promotion Media].[All Media].[Sunday Paper],\n"
            + "      [Promotion Media].[All Media].[Street Handout] },\n"
            + "    [Product].[All Products].[Drink].children) on rows\n"
            + "from Sales\n"
            + "where ([Time].[1997])",

            "Axis #0:\n"
            + "{[Time].[1997]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Store Cost]}\n"
            + "{[Measures].[Store Sales]}\n"
            + "Axis #2:\n"
            + "{[Promotion Media].[Radio], [Product].[Drink].[Alcoholic Beverages]}\n"
            + "{[Promotion Media].[Radio], [Product].[Drink].[Beverages]}\n"
            + "{[Promotion Media].[Radio], [Product].[Drink].[Dairy]}\n"
            + "{[Promotion Media].[TV], [Product].[Drink].[Alcoholic Beverages]}\n"
            + "{[Promotion Media].[TV], [Product].[Drink].[Beverages]}\n"
            + "{[Promotion Media].[TV], [Product].[Drink].[Dairy]}\n"
            + "{[Promotion Media].[Sunday Paper], [Product].[Drink].[Alcoholic Beverages]}\n"
            + "{[Promotion Media].[Sunday Paper], [Product].[Drink].[Beverages]}\n"
            + "{[Promotion Media].[Sunday Paper], [Product].[Drink].[Dairy]}\n"
            + "{[Promotion Media].[Street Handout], [Product].[Drink].[Alcoholic Beverages]}\n"
            + "{[Promotion Media].[Street Handout], [Product].[Drink].[Beverages]}\n"
            + "{[Promotion Media].[Street Handout], [Product].[Drink].[Dairy]}\n"
            + "Row #0: 75\n"
            + "Row #0: 70.40\n"
            + "Row #0: 168.62\n"
            + "Row #1: 97\n"
            + "Row #1: 75.70\n"
            + "Row #1: 186.03\n"
            + "Row #2: 54\n"
            + "Row #2: 36.75\n"
            + "Row #2: 89.03\n"
            + "Row #3: 76\n"
            + "Row #3: 70.99\n"
            + "Row #3: 182.38\n"
            + "Row #4: 188\n"
            + "Row #4: 167.00\n"
            + "Row #4: 419.14\n"
            + "Row #5: 68\n"
            + "Row #5: 45.19\n"
            + "Row #5: 119.55\n"
            + "Row #6: 148\n"
            + "Row #6: 128.97\n"
            + "Row #6: 316.88\n"
            + "Row #7: 197\n"
            + "Row #7: 161.81\n"
            + "Row #7: 399.58\n"
            + "Row #8: 85\n"
            + "Row #8: 54.75\n"
            + "Row #8: 140.27\n"
            + "Row #9: 158\n"
            + "Row #9: 121.14\n"
            + "Row #9: 294.55\n"
            + "Row #10: 270\n"
            + "Row #10: 201.28\n"
            + "Row #10: 520.55\n"
            + "Row #11: 84\n"
            + "Row #11: 50.26\n"
            + "Row #11: 128.32\n"),

        // 1
        new QueryAndResult(
            "select\n"
            + "  [Product].[All Products].[Drink].children on rows,\n"
            + "  CrossJoin(\n"
            + "    {[Measures].[Unit Sales], [Measures].[Store Sales]},\n"
            + "    { [Promotion Media].[All Media].[Radio],\n"
            + "      [Promotion Media].[All Media].[TV],\n"
            + "      [Promotion Media].[All Media].[Sunday Paper],\n"
            + "      [Promotion Media].[All Media].[Street Handout] }\n"
            + "   ) on columns\n"
            + "from Sales\n"
            + "where ([Time].[1997])",

            "Axis #0:\n"
            + "{[Time].[1997]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales], [Promotion Media].[Radio]}\n"
            + "{[Measures].[Unit Sales], [Promotion Media].[TV]}\n"
            + "{[Measures].[Unit Sales], [Promotion Media].[Sunday Paper]}\n"
            + "{[Measures].[Unit Sales], [Promotion Media].[Street Handout]}\n"
            + "{[Measures].[Store Sales], [Promotion Media].[Radio]}\n"
            + "{[Measures].[Store Sales], [Promotion Media].[TV]}\n"
            + "{[Measures].[Store Sales], [Promotion Media].[Sunday Paper]}\n"
            + "{[Measures].[Store Sales], [Promotion Media].[Street Handout]}\n"
            + "Axis #2:\n"
            + "{[Product].[Drink].[Alcoholic Beverages]}\n"
            + "{[Product].[Drink].[Beverages]}\n"
            + "{[Product].[Drink].[Dairy]}\n"
            + "Row #0: 75\n"
            + "Row #0: 76\n"
            + "Row #0: 148\n"
            + "Row #0: 158\n"
            + "Row #0: 168.62\n"
            + "Row #0: 182.38\n"
            + "Row #0: 316.88\n"
            + "Row #0: 294.55\n"
            + "Row #1: 97\n"
            + "Row #1: 188\n"
            + "Row #1: 197\n"
            + "Row #1: 270\n"
            + "Row #1: 186.03\n"
            + "Row #1: 419.14\n"
            + "Row #1: 399.58\n"
            + "Row #1: 520.55\n"
            + "Row #2: 54\n"
            + "Row #2: 68\n"
            + "Row #2: 85\n"
            + "Row #2: 84\n"
            + "Row #2: 89.03\n"
            + "Row #2: 119.55\n"
            + "Row #2: 140.27\n"
            + "Row #2: 128.32\n"),

        // 2
        new QueryAndResult(
            "select\n"
            + "  {[Measures].[Unit Sales], [Measures].[Store Sales]} on columns,\n"
            + "  Order([Product].[Product Department].members, [Measures].[Store Sales], DESC) on rows\n"
            + "from Sales",

            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Store Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Food].[Produce]}\n"
            + "{[Product].[Food].[Snack Foods]}\n"
            + "{[Product].[Food].[Frozen Foods]}\n"
            + "{[Product].[Food].[Canned Foods]}\n"
            + "{[Product].[Food].[Baking Goods]}\n"
            + "{[Product].[Food].[Dairy]}\n"
            + "{[Product].[Food].[Deli]}\n"
            + "{[Product].[Food].[Baked Goods]}\n"
            + "{[Product].[Food].[Snacks]}\n"
            + "{[Product].[Food].[Starchy Foods]}\n"
            + "{[Product].[Food].[Eggs]}\n"
            + "{[Product].[Food].[Breakfast Foods]}\n"
            + "{[Product].[Food].[Seafood]}\n"
            + "{[Product].[Food].[Meat]}\n"
            + "{[Product].[Food].[Canned Products]}\n"
            + "{[Product].[Non-Consumable].[Household]}\n"
            + "{[Product].[Non-Consumable].[Health and Hygiene]}\n"
            + "{[Product].[Non-Consumable].[Periodicals]}\n"
            + "{[Product].[Non-Consumable].[Checkout]}\n"
            + "{[Product].[Non-Consumable].[Carousel]}\n"
            + "{[Product].[Drink].[Beverages]}\n"
            + "{[Product].[Drink].[Alcoholic Beverages]}\n"
            + "{[Product].[Drink].[Dairy]}\n"
            + "Row #0: 37,792\n"
            + "Row #0: 82,248.42\n"
            + "Row #1: 30,545\n"
            + "Row #1: 67,609.82\n"
            + "Row #2: 26,655\n"
            + "Row #2: 55,207.50\n"
            + "Row #3: 19,026\n"
            + "Row #3: 39,774.34\n"
            + "Row #4: 20,245\n"
            + "Row #4: 38,670.41\n"
            + "Row #5: 12,885\n"
            + "Row #5: 30,508.85\n"
            + "Row #6: 12,037\n"
            + "Row #6: 25,318.93\n"
            + "Row #7: 7,870\n"
            + "Row #7: 16,455.43\n"
            + "Row #8: 6,884\n"
            + "Row #8: 14,550.05\n"
            + "Row #9: 5,262\n"
            + "Row #9: 11,756.07\n"
            + "Row #10: 4,132\n"
            + "Row #10: 9,200.76\n"
            + "Row #11: 3,317\n"
            + "Row #11: 6,941.46\n"
            + "Row #12: 1,764\n"
            + "Row #12: 3,809.14\n"
            + "Row #13: 1,714\n"
            + "Row #13: 3,669.89\n"
            + "Row #14: 1,812\n"
            + "Row #14: 3,314.52\n"
            + "Row #15: 27,038\n"
            + "Row #15: 60,469.89\n"
            + "Row #16: 16,284\n"
            + "Row #16: 32,571.86\n"
            + "Row #17: 4,294\n"
            + "Row #17: 9,056.76\n"
            + "Row #18: 1,779\n"
            + "Row #18: 3,767.71\n"
            + "Row #19: 841\n"
            + "Row #19: 1,500.11\n"
            + "Row #20: 13,573\n"
            + "Row #20: 27,748.53\n"
            + "Row #21: 6,838\n"
            + "Row #21: 14,029.08\n"
            + "Row #22: 4,186\n"
            + "Row #22: 7,058.60\n"),

        // 3
        new QueryAndResult(
            "select\n"
            + "  [Product].[All Products].[Drink].children on columns\n"
            + "from Sales\n"
            + "where ([Measures].[Unit Sales], [Promotion Media].[All Media].[Street Handout], [Time].[1997])",

            "Axis #0:\n"
            + "{[Measures].[Unit Sales], [Promotion Media].[Street Handout], [Time].[1997]}\n"
            + "Axis #1:\n"
            + "{[Product].[Drink].[Alcoholic Beverages]}\n"
            + "{[Product].[Drink].[Beverages]}\n"
            + "{[Product].[Drink].[Dairy]}\n"
            + "Row #0: 158\n"
            + "Row #0: 270\n"
            + "Row #0: 84\n"),

        // 4
        new QueryAndResult(
            "select\n"
            + "  NON EMPTY CrossJoin([Product].[All Products].[Drink].children, [Customers].[All Customers].[USA].[WA].Children) on rows,\n"
            + "  CrossJoin(\n"
            + "    {[Measures].[Unit Sales], [Measures].[Store Sales]},\n"
            + "    { [Promotion Media].[All Media].[Radio],\n"
            + "      [Promotion Media].[All Media].[TV],\n"
            + "      [Promotion Media].[All Media].[Sunday Paper],\n"
            + "      [Promotion Media].[All Media].[Street Handout] }\n"
            + "   ) on columns\n"
            + "from Sales\n"
            + "where ([Time].[1997])",

            "Axis #0:\n"
            + "{[Time].[1997]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales], [Promotion Media].[Radio]}\n"
            + "{[Measures].[Unit Sales], [Promotion Media].[TV]}\n"
            + "{[Measures].[Unit Sales], [Promotion Media].[Sunday Paper]}\n"
            + "{[Measures].[Unit Sales], [Promotion Media].[Street Handout]}\n"
            + "{[Measures].[Store Sales], [Promotion Media].[Radio]}\n"
            + "{[Measures].[Store Sales], [Promotion Media].[TV]}\n"
            + "{[Measures].[Store Sales], [Promotion Media].[Sunday Paper]}\n"
            + "{[Measures].[Store Sales], [Promotion Media].[Street Handout]}\n"
            + "Axis #2:\n"
            + "{[Product].[Drink].[Alcoholic Beverages], [Customers].[USA].[WA].[Anacortes]}\n"
            + "{[Product].[Drink].[Alcoholic Beverages], [Customers].[USA].[WA].[Ballard]}\n"
            + "{[Product].[Drink].[Alcoholic Beverages], [Customers].[USA].[WA].[Bellingham]}\n"
            + "{[Product].[Drink].[Alcoholic Beverages], [Customers].[USA].[WA].[Bremerton]}\n"
            + "{[Product].[Drink].[Alcoholic Beverages], [Customers].[USA].[WA].[Burien]}\n"
            + "{[Product].[Drink].[Alcoholic Beverages], [Customers].[USA].[WA].[Everett]}\n"
            + "{[Product].[Drink].[Alcoholic Beverages], [Customers].[USA].[WA].[Issaquah]}\n"
            + "{[Product].[Drink].[Alcoholic Beverages], [Customers].[USA].[WA].[Kirkland]}\n"
            + "{[Product].[Drink].[Alcoholic Beverages], [Customers].[USA].[WA].[Lynnwood]}\n"
            + "{[Product].[Drink].[Alcoholic Beverages], [Customers].[USA].[WA].[Marysville]}\n"
            + "{[Product].[Drink].[Alcoholic Beverages], [Customers].[USA].[WA].[Olympia]}\n"
            + "{[Product].[Drink].[Alcoholic Beverages], [Customers].[USA].[WA].[Port Orchard]}\n"
            + "{[Product].[Drink].[Alcoholic Beverages], [Customers].[USA].[WA].[Puyallup]}\n"
            + "{[Product].[Drink].[Alcoholic Beverages], [Customers].[USA].[WA].[Redmond]}\n"
            + "{[Product].[Drink].[Alcoholic Beverages], [Customers].[USA].[WA].[Renton]}\n"
            + "{[Product].[Drink].[Alcoholic Beverages], [Customers].[USA].[WA].[Seattle]}\n"
            + "{[Product].[Drink].[Alcoholic Beverages], [Customers].[USA].[WA].[Spokane]}\n"
            + "{[Product].[Drink].[Alcoholic Beverages], [Customers].[USA].[WA].[Tacoma]}\n"
            + "{[Product].[Drink].[Alcoholic Beverages], [Customers].[USA].[WA].[Yakima]}\n"
            + "{[Product].[Drink].[Beverages], [Customers].[USA].[WA].[Anacortes]}\n"
            + "{[Product].[Drink].[Beverages], [Customers].[USA].[WA].[Ballard]}\n"
            + "{[Product].[Drink].[Beverages], [Customers].[USA].[WA].[Bremerton]}\n"
            + "{[Product].[Drink].[Beverages], [Customers].[USA].[WA].[Burien]}\n"
            + "{[Product].[Drink].[Beverages], [Customers].[USA].[WA].[Edmonds]}\n"
            + "{[Product].[Drink].[Beverages], [Customers].[USA].[WA].[Everett]}\n"
            + "{[Product].[Drink].[Beverages], [Customers].[USA].[WA].[Issaquah]}\n"
            + "{[Product].[Drink].[Beverages], [Customers].[USA].[WA].[Kirkland]}\n"
            + "{[Product].[Drink].[Beverages], [Customers].[USA].[WA].[Lynnwood]}\n"
            + "{[Product].[Drink].[Beverages], [Customers].[USA].[WA].[Marysville]}\n"
            + "{[Product].[Drink].[Beverages], [Customers].[USA].[WA].[Olympia]}\n"
            + "{[Product].[Drink].[Beverages], [Customers].[USA].[WA].[Port Orchard]}\n"
            + "{[Product].[Drink].[Beverages], [Customers].[USA].[WA].[Puyallup]}\n"
            + "{[Product].[Drink].[Beverages], [Customers].[USA].[WA].[Redmond]}\n"
            + "{[Product].[Drink].[Beverages], [Customers].[USA].[WA].[Seattle]}\n"
            + "{[Product].[Drink].[Beverages], [Customers].[USA].[WA].[Sedro Woolley]}\n"
            + "{[Product].[Drink].[Beverages], [Customers].[USA].[WA].[Spokane]}\n"
            + "{[Product].[Drink].[Beverages], [Customers].[USA].[WA].[Tacoma]}\n"
            + "{[Product].[Drink].[Beverages], [Customers].[USA].[WA].[Walla Walla]}\n"
            + "{[Product].[Drink].[Beverages], [Customers].[USA].[WA].[Yakima]}\n"
            + "{[Product].[Drink].[Dairy], [Customers].[USA].[WA].[Ballard]}\n"
            + "{[Product].[Drink].[Dairy], [Customers].[USA].[WA].[Bellingham]}\n"
            + "{[Product].[Drink].[Dairy], [Customers].[USA].[WA].[Bremerton]}\n"
            + "{[Product].[Drink].[Dairy], [Customers].[USA].[WA].[Burien]}\n"
            + "{[Product].[Drink].[Dairy], [Customers].[USA].[WA].[Everett]}\n"
            + "{[Product].[Drink].[Dairy], [Customers].[USA].[WA].[Issaquah]}\n"
            + "{[Product].[Drink].[Dairy], [Customers].[USA].[WA].[Kirkland]}\n"
            + "{[Product].[Drink].[Dairy], [Customers].[USA].[WA].[Lynnwood]}\n"
            + "{[Product].[Drink].[Dairy], [Customers].[USA].[WA].[Marysville]}\n"
            + "{[Product].[Drink].[Dairy], [Customers].[USA].[WA].[Olympia]}\n"
            + "{[Product].[Drink].[Dairy], [Customers].[USA].[WA].[Port Orchard]}\n"
            + "{[Product].[Drink].[Dairy], [Customers].[USA].[WA].[Puyallup]}\n"
            + "{[Product].[Drink].[Dairy], [Customers].[USA].[WA].[Redmond]}\n"
            + "{[Product].[Drink].[Dairy], [Customers].[USA].[WA].[Renton]}\n"
            + "{[Product].[Drink].[Dairy], [Customers].[USA].[WA].[Seattle]}\n"
            + "{[Product].[Drink].[Dairy], [Customers].[USA].[WA].[Spokane]}\n"
            + "{[Product].[Drink].[Dairy], [Customers].[USA].[WA].[Tacoma]}\n"
            + "{[Product].[Drink].[Dairy], [Customers].[USA].[WA].[Yakima]}\n"
            + "Row #0: \n"
            + "Row #0: 2\n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: 1.14\n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #1: 4\n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: 4\n"
            + "Row #1: 10.40\n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: 2.16\n"
            + "Row #2: \n"
            + "Row #2: 1\n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: 2.37\n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #3: \n"
            + "Row #3: \n"
            + "Row #3: 24\n"
            + "Row #3: \n"
            + "Row #3: \n"
            + "Row #3: \n"
            + "Row #3: 46.09\n"
            + "Row #3: \n"
            + "Row #4: 3\n"
            + "Row #4: \n"
            + "Row #4: \n"
            + "Row #4: 8\n"
            + "Row #4: 2.10\n"
            + "Row #4: \n"
            + "Row #4: \n"
            + "Row #4: 9.63\n"
            + "Row #5: 6\n"
            + "Row #5: \n"
            + "Row #5: \n"
            + "Row #5: 5\n"
            + "Row #5: 8.06\n"
            + "Row #5: \n"
            + "Row #5: \n"
            + "Row #5: 6.21\n"
            + "Row #6: 3\n"
            + "Row #6: \n"
            + "Row #6: \n"
            + "Row #6: 7\n"
            + "Row #6: 7.80\n"
            + "Row #6: \n"
            + "Row #6: \n"
            + "Row #6: 15.00\n"
            + "Row #7: 14\n"
            + "Row #7: \n"
            + "Row #7: \n"
            + "Row #7: \n"
            + "Row #7: 36.10\n"
            + "Row #7: \n"
            + "Row #7: \n"
            + "Row #7: \n"
            + "Row #8: 3\n"
            + "Row #8: \n"
            + "Row #8: \n"
            + "Row #8: 16\n"
            + "Row #8: 10.29\n"
            + "Row #8: \n"
            + "Row #8: \n"
            + "Row #8: 32.20\n"
            + "Row #9: 3\n"
            + "Row #9: \n"
            + "Row #9: \n"
            + "Row #9: \n"
            + "Row #9: 10.56\n"
            + "Row #9: \n"
            + "Row #9: \n"
            + "Row #9: \n"
            + "Row #10: \n"
            + "Row #10: \n"
            + "Row #10: 15\n"
            + "Row #10: 11\n"
            + "Row #10: \n"
            + "Row #10: \n"
            + "Row #10: 34.79\n"
            + "Row #10: 15.67\n"
            + "Row #11: \n"
            + "Row #11: \n"
            + "Row #11: 7\n"
            + "Row #11: \n"
            + "Row #11: \n"
            + "Row #11: \n"
            + "Row #11: 17.44\n"
            + "Row #11: \n"
            + "Row #12: \n"
            + "Row #12: \n"
            + "Row #12: 22\n"
            + "Row #12: 9\n"
            + "Row #12: \n"
            + "Row #12: \n"
            + "Row #12: 32.35\n"
            + "Row #12: 17.43\n"
            + "Row #13: 7\n"
            + "Row #13: \n"
            + "Row #13: \n"
            + "Row #13: 4\n"
            + "Row #13: 4.77\n"
            + "Row #13: \n"
            + "Row #13: \n"
            + "Row #13: 15.16\n"
            + "Row #14: 4\n"
            + "Row #14: \n"
            + "Row #14: \n"
            + "Row #14: 4\n"
            + "Row #14: 3.64\n"
            + "Row #14: \n"
            + "Row #14: \n"
            + "Row #14: 9.64\n"
            + "Row #15: 2\n"
            + "Row #15: \n"
            + "Row #15: \n"
            + "Row #15: 7\n"
            + "Row #15: 6.86\n"
            + "Row #15: \n"
            + "Row #15: \n"
            + "Row #15: 8.38\n"
            + "Row #16: \n"
            + "Row #16: \n"
            + "Row #16: \n"
            + "Row #16: 28\n"
            + "Row #16: \n"
            + "Row #16: \n"
            + "Row #16: \n"
            + "Row #16: 61.98\n"
            + "Row #17: \n"
            + "Row #17: \n"
            + "Row #17: 3\n"
            + "Row #17: 4\n"
            + "Row #17: \n"
            + "Row #17: \n"
            + "Row #17: 10.56\n"
            + "Row #17: 8.96\n"
            + "Row #18: 6\n"
            + "Row #18: \n"
            + "Row #18: \n"
            + "Row #18: 3\n"
            + "Row #18: 7.16\n"
            + "Row #18: \n"
            + "Row #18: \n"
            + "Row #18: 8.10\n"
            + "Row #19: 7\n"
            + "Row #19: \n"
            + "Row #19: \n"
            + "Row #19: \n"
            + "Row #19: 15.63\n"
            + "Row #19: \n"
            + "Row #19: \n"
            + "Row #19: \n"
            + "Row #20: 3\n"
            + "Row #20: \n"
            + "Row #20: \n"
            + "Row #20: 13\n"
            + "Row #20: 6.96\n"
            + "Row #20: \n"
            + "Row #20: \n"
            + "Row #20: 12.22\n"
            + "Row #21: \n"
            + "Row #21: \n"
            + "Row #21: 16\n"
            + "Row #21: \n"
            + "Row #21: \n"
            + "Row #21: \n"
            + "Row #21: 45.08\n"
            + "Row #21: \n"
            + "Row #22: 3\n"
            + "Row #22: \n"
            + "Row #22: \n"
            + "Row #22: 18\n"
            + "Row #22: 6.39\n"
            + "Row #22: \n"
            + "Row #22: \n"
            + "Row #22: 21.08\n"
            + "Row #23: \n"
            + "Row #23: \n"
            + "Row #23: \n"
            + "Row #23: 21\n"
            + "Row #23: \n"
            + "Row #23: \n"
            + "Row #23: \n"
            + "Row #23: 33.22\n"
            + "Row #24: \n"
            + "Row #24: \n"
            + "Row #24: \n"
            + "Row #24: 9\n"
            + "Row #24: \n"
            + "Row #24: \n"
            + "Row #24: \n"
            + "Row #24: 22.65\n"
            + "Row #25: 2\n"
            + "Row #25: \n"
            + "Row #25: \n"
            + "Row #25: 9\n"
            + "Row #25: 6.80\n"
            + "Row #25: \n"
            + "Row #25: \n"
            + "Row #25: 18.90\n"
            + "Row #26: 3\n"
            + "Row #26: \n"
            + "Row #26: \n"
            + "Row #26: 9\n"
            + "Row #26: 1.50\n"
            + "Row #26: \n"
            + "Row #26: \n"
            + "Row #26: 23.01\n"
            + "Row #27: \n"
            + "Row #27: \n"
            + "Row #27: \n"
            + "Row #27: 22\n"
            + "Row #27: \n"
            + "Row #27: \n"
            + "Row #27: \n"
            + "Row #27: 50.71\n"
            + "Row #28: 4\n"
            + "Row #28: \n"
            + "Row #28: \n"
            + "Row #28: \n"
            + "Row #28: 5.16\n"
            + "Row #28: \n"
            + "Row #28: \n"
            + "Row #28: \n"
            + "Row #29: \n"
            + "Row #29: \n"
            + "Row #29: 20\n"
            + "Row #29: 14\n"
            + "Row #29: \n"
            + "Row #29: \n"
            + "Row #29: 48.02\n"
            + "Row #29: 28.80\n"
            + "Row #30: \n"
            + "Row #30: \n"
            + "Row #30: 14\n"
            + "Row #30: \n"
            + "Row #30: \n"
            + "Row #30: \n"
            + "Row #30: 19.96\n"
            + "Row #30: \n"
            + "Row #31: \n"
            + "Row #31: \n"
            + "Row #31: 10\n"
            + "Row #31: 40\n"
            + "Row #31: \n"
            + "Row #31: \n"
            + "Row #31: 26.36\n"
            + "Row #31: 74.49\n"
            + "Row #32: 6\n"
            + "Row #32: \n"
            + "Row #32: \n"
            + "Row #32: \n"
            + "Row #32: 17.01\n"
            + "Row #32: \n"
            + "Row #32: \n"
            + "Row #32: \n"
            + "Row #33: 4\n"
            + "Row #33: \n"
            + "Row #33: \n"
            + "Row #33: \n"
            + "Row #33: 2.80\n"
            + "Row #33: \n"
            + "Row #33: \n"
            + "Row #33: \n"
            + "Row #34: 4\n"
            + "Row #34: \n"
            + "Row #34: \n"
            + "Row #34: \n"
            + "Row #34: 7.98\n"
            + "Row #34: \n"
            + "Row #34: \n"
            + "Row #34: \n"
            + "Row #35: \n"
            + "Row #35: \n"
            + "Row #35: \n"
            + "Row #35: 46\n"
            + "Row #35: \n"
            + "Row #35: \n"
            + "Row #35: \n"
            + "Row #35: 81.71\n"
            + "Row #36: \n"
            + "Row #36: \n"
            + "Row #36: 21\n"
            + "Row #36: 6\n"
            + "Row #36: \n"
            + "Row #36: \n"
            + "Row #36: 37.93\n"
            + "Row #36: 14.73\n"
            + "Row #37: \n"
            + "Row #37: \n"
            + "Row #37: 3\n"
            + "Row #37: \n"
            + "Row #37: \n"
            + "Row #37: \n"
            + "Row #37: 7.92\n"
            + "Row #37: \n"
            + "Row #38: 25\n"
            + "Row #38: \n"
            + "Row #38: \n"
            + "Row #38: 3\n"
            + "Row #38: 51.65\n"
            + "Row #38: \n"
            + "Row #38: \n"
            + "Row #38: 2.34\n"
            + "Row #39: 3\n"
            + "Row #39: \n"
            + "Row #39: \n"
            + "Row #39: 4\n"
            + "Row #39: 4.47\n"
            + "Row #39: \n"
            + "Row #39: \n"
            + "Row #39: 9.20\n"
            + "Row #40: \n"
            + "Row #40: 1\n"
            + "Row #40: \n"
            + "Row #40: \n"
            + "Row #40: \n"
            + "Row #40: 1.47\n"
            + "Row #40: \n"
            + "Row #40: \n"
            + "Row #41: \n"
            + "Row #41: \n"
            + "Row #41: 15\n"
            + "Row #41: \n"
            + "Row #41: \n"
            + "Row #41: \n"
            + "Row #41: 18.88\n"
            + "Row #41: \n"
            + "Row #42: \n"
            + "Row #42: \n"
            + "Row #42: \n"
            + "Row #42: 3\n"
            + "Row #42: \n"
            + "Row #42: \n"
            + "Row #42: \n"
            + "Row #42: 3.75\n"
            + "Row #43: 9\n"
            + "Row #43: \n"
            + "Row #43: \n"
            + "Row #43: 10\n"
            + "Row #43: 31.41\n"
            + "Row #43: \n"
            + "Row #43: \n"
            + "Row #43: 15.12\n"
            + "Row #44: 3\n"
            + "Row #44: \n"
            + "Row #44: \n"
            + "Row #44: 3\n"
            + "Row #44: 7.41\n"
            + "Row #44: \n"
            + "Row #44: \n"
            + "Row #44: 2.55\n"
            + "Row #45: 3\n"
            + "Row #45: \n"
            + "Row #45: \n"
            + "Row #45: \n"
            + "Row #45: 1.71\n"
            + "Row #45: \n"
            + "Row #45: \n"
            + "Row #45: \n"
            + "Row #46: \n"
            + "Row #46: \n"
            + "Row #46: \n"
            + "Row #46: 7\n"
            + "Row #46: \n"
            + "Row #46: \n"
            + "Row #46: \n"
            + "Row #46: 11.86\n"
            + "Row #47: \n"
            + "Row #47: \n"
            + "Row #47: \n"
            + "Row #47: 3\n"
            + "Row #47: \n"
            + "Row #47: \n"
            + "Row #47: \n"
            + "Row #47: 2.76\n"
            + "Row #48: \n"
            + "Row #48: \n"
            + "Row #48: 4\n"
            + "Row #48: 5\n"
            + "Row #48: \n"
            + "Row #48: \n"
            + "Row #48: 4.50\n"
            + "Row #48: 7.27\n"
            + "Row #49: \n"
            + "Row #49: \n"
            + "Row #49: 7\n"
            + "Row #49: \n"
            + "Row #49: \n"
            + "Row #49: \n"
            + "Row #49: 10.01\n"
            + "Row #49: \n"
            + "Row #50: \n"
            + "Row #50: \n"
            + "Row #50: 5\n"
            + "Row #50: 4\n"
            + "Row #50: \n"
            + "Row #50: \n"
            + "Row #50: 12.88\n"
            + "Row #50: 5.28\n"
            + "Row #51: 2\n"
            + "Row #51: \n"
            + "Row #51: \n"
            + "Row #51: \n"
            + "Row #51: 2.64\n"
            + "Row #51: \n"
            + "Row #51: \n"
            + "Row #51: \n"
            + "Row #52: \n"
            + "Row #52: \n"
            + "Row #52: \n"
            + "Row #52: 5\n"
            + "Row #52: \n"
            + "Row #52: \n"
            + "Row #52: \n"
            + "Row #52: 12.34\n"
            + "Row #53: \n"
            + "Row #53: \n"
            + "Row #53: \n"
            + "Row #53: 5\n"
            + "Row #53: \n"
            + "Row #53: \n"
            + "Row #53: \n"
            + "Row #53: 3.41\n"
            + "Row #54: \n"
            + "Row #54: \n"
            + "Row #54: \n"
            + "Row #54: 4\n"
            + "Row #54: \n"
            + "Row #54: \n"
            + "Row #54: \n"
            + "Row #54: 2.44\n"
            + "Row #55: \n"
            + "Row #55: \n"
            + "Row #55: 2\n"
            + "Row #55: \n"
            + "Row #55: \n"
            + "Row #55: \n"
            + "Row #55: 6.92\n"
            + "Row #55: \n"
            + "Row #56: 13\n"
            + "Row #56: \n"
            + "Row #56: \n"
            + "Row #56: 7\n"
            + "Row #56: 23.69\n"
            + "Row #56: \n"
            + "Row #56: \n"
            + "Row #56: 7.07\n"),

        // 5
        new QueryAndResult(
            "select from Sales\n"
            + "where ([Measures].[Store Sales], [Time].[1997], [Promotion Media].[All Media].[TV])",

            "Axis #0:\n"
            + "{[Measures].[Store Sales], [Time].[1997], [Promotion Media].[TV]}\n"
            + "7,786.21"));

    public void testTaglib0() {
        assertQueryReturns(
            taglibQueries.get(0).query, taglibQueries.get(0).result);
    }

    public void testTaglib1() {
        assertQueryReturns(
            taglibQueries.get(1).query, taglibQueries.get(1).result);
    }

    public void testTaglib2() {
        assertQueryReturns(
            taglibQueries.get(2).query, taglibQueries.get(2).result);
    }

    public void testTaglib3() {
        assertQueryReturns(
            taglibQueries.get(3).query, taglibQueries.get(3).result);
    }

    public void testTaglib4() {
        assertQueryReturns(
            taglibQueries.get(4).query, taglibQueries.get(4).result);
    }

    public void testTaglib5() {
        assertQueryReturns(
            taglibQueries.get(5).query, taglibQueries.get(5).result);
    }

    public void testCellValue() {
        Result result = executeQuery(
            "select {[Measures].[Unit Sales],[Measures].[Store Sales]} on columns,\n"
            + " {[Gender].[M]} on rows\n"
            + "from Sales");
        Cell cell = result.getCell(new int[]{0, 0});
        Object value = cell.getValue();
        assertTrue(value instanceof Number);
        assertEquals(135215, ((Number) value).intValue());
        cell = result.getCell(new int[]{1, 0});
        value = cell.getValue();
        assertTrue(value instanceof Number);
        // Plato give 285011.12, Oracle gives 285011, MySQL gives 285964 (bug!)
        assertEquals(285011, ((Number) value).intValue());
    }

    public void testDynamicFormat() {
        assertQueryReturns(
            "with member [Measures].[USales] as [Measures].[Unit Sales],\n"
            + "  format_string = iif([Measures].[Unit Sales] > 50000, \"\\<b\\>#.00\\<\\/b\\>\", \"\\<i\\>#.00\\<\\/i\\>\")\n"
            + "select \n"
            + "  {[Measures].[USales]} on columns,\n"
            + "  {[Store Type].members} on rows\n"
            + "from Sales",

            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[USales]}\n"
            + "Axis #2:\n"
            + "{[Store Type].[All Store Types]}\n"
            + "{[Store Type].[Deluxe Supermarket]}\n"
            + "{[Store Type].[Gourmet Supermarket]}\n"
            + "{[Store Type].[HeadQuarters]}\n"
            + "{[Store Type].[Mid-Size Grocery]}\n"
            + "{[Store Type].[Small Grocery]}\n"
            + "{[Store Type].[Supermarket]}\n"
            + "Row #0: <b>266773.00</b>\n"
            + "Row #1: <b>76837.00</b>\n"
            + "Row #2: <i>21333.00</i>\n"
            + "Row #3: \n"
            + "Row #4: <i>11491.00</i>\n"
            + "Row #5: <i>6557.00</i>\n"
            + "Row #6: <b>150555.00</b>\n");
    }

    public void testFormatOfNulls() {
        assertQueryReturns(
            "with member [Measures]._Foo as '([Measures].[Store Sales])',\n"
            + " format_string = '$#,##0.00;($#,##0.00);ZERO;NULL;Nil'\n"
            + "select\n"
            + " {[Measures].[_Foo]} on columns,\n"
            + " {[Customers].[Country].members} on rows\n"
            + "from Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[_Foo]}\n"
            + "Axis #2:\n"
            + "{[Customers].[Canada]}\n"
            + "{[Customers].[Mexico]}\n"
            + "{[Customers].[USA]}\n"
            + "Row #0: NULL\n"
            + "Row #1: NULL\n"
            + "Row #2: $565,238.13\n");

        // explicit null value
        assertQueryReturns(
            "with member [Measures].[Foo] as null,\n"
            + " format_string = 'a;b;c;d'\n"
            + "select {[Measures].[Foo]} on columns\n"
            + "from Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Foo]}\n"
            + "Row #0: d\n");
    }

    /**
     * Test case for bug
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-434">MONDRIAN-434</a>,
     * "Small negative numbers cause exceptions w 2-section format".
     */
    public void testFormatOfNil() {
        assertQueryReturns(
            "with member measures.formatTest as '0.000001',\n"
            + " FORMAT_STRING='#.##;(#.##)' \n"
            + "select { measures.formatTest } on 0 from sales ",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[formatTest]}\n"
            + "Row #0: .\n");
    }

    /**
     * If a measure (in this case, <code>[Measures].[Sales Count]</code>)
     * occurs only within a format expression, bug
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-14">MONDRIAN-14</a>.
     * causes an internal
     * error ("value not found") when the cell's formatted value is retrieved.
     */
    public void testBugMondrian14() {
        assertQueryReturns(
            "with member [Measures].[USales] as '[Measures].[Unit Sales]',\n"
            + " format_string = iif([Measures].[Sales Count] > 30, \"#.00 good\",\"#.00 bad\")\n"
            + "select {[Measures].[USales], [Measures].[Store Cost], [Measures].[Store Sales]} ON columns,\n"
            + " Crossjoin({[Promotion Media].[All Media].[Radio], [Promotion Media].[All Media].[TV], [Promotion Media]. [All Media].[Sunday Paper], [Promotion Media].[All Media].[Street Handout]}, [Product].[All Products].[Drink].Children) ON rows\n"
            + "from [Sales] where ([Time].[1997])",

            "Axis #0:\n"
            + "{[Time].[1997]}\n"
            + "Axis #1:\n"
            + "{[Measures].[USales]}\n"
            + "{[Measures].[Store Cost]}\n"
            + "{[Measures].[Store Sales]}\n"
            + "Axis #2:\n"
            + "{[Promotion Media].[Radio], [Product].[Drink].[Alcoholic Beverages]}\n"
            + "{[Promotion Media].[Radio], [Product].[Drink].[Beverages]}\n"
            + "{[Promotion Media].[Radio], [Product].[Drink].[Dairy]}\n"
            + "{[Promotion Media].[TV], [Product].[Drink].[Alcoholic Beverages]}\n"
            + "{[Promotion Media].[TV], [Product].[Drink].[Beverages]}\n"
            + "{[Promotion Media].[TV], [Product].[Drink].[Dairy]}\n"
            + "{[Promotion Media].[Sunday Paper], [Product].[Drink].[Alcoholic Beverages]}\n"
            + "{[Promotion Media].[Sunday Paper], [Product].[Drink].[Beverages]}\n"
            + "{[Promotion Media].[Sunday Paper], [Product].[Drink].[Dairy]}\n"
            + "{[Promotion Media].[Street Handout], [Product].[Drink].[Alcoholic Beverages]}\n"
            + "{[Promotion Media].[Street Handout], [Product].[Drink].[Beverages]}\n"
            + "{[Promotion Media].[Street Handout], [Product].[Drink].[Dairy]}\n"
            + "Row #0: 75.00 bad\n"
            + "Row #0: 70.40\n"
            + "Row #0: 168.62\n"
            + "Row #1: 97.00 good\n"
            + "Row #1: 75.70\n"
            + "Row #1: 186.03\n"
            + "Row #2: 54.00 bad\n"
            + "Row #2: 36.75\n"
            + "Row #2: 89.03\n"
            + "Row #3: 76.00 bad\n"
            + "Row #3: 70.99\n"
            + "Row #3: 182.38\n"
            + "Row #4: 188.00 good\n"
            + "Row #4: 167.00\n"
            + "Row #4: 419.14\n"
            + "Row #5: 68.00 bad\n"
            + "Row #5: 45.19\n"
            + "Row #5: 119.55\n"
            + "Row #6: 148.00 good\n"
            + "Row #6: 128.97\n"
            + "Row #6: 316.88\n"
            + "Row #7: 197.00 good\n"
            + "Row #7: 161.81\n"
            + "Row #7: 399.58\n"
            + "Row #8: 85.00 bad\n"
            + "Row #8: 54.75\n"
            + "Row #8: 140.27\n"
            + "Row #9: 158.00 good\n"
            + "Row #9: 121.14\n"
            + "Row #9: 294.55\n"
            + "Row #10: 270.00 good\n"
            + "Row #10: 201.28\n"
            + "Row #10: 520.55\n"
            + "Row #11: 84.00 bad\n"
            + "Row #11: 50.26\n"
            + "Row #11: 128.32\n");
    }

    /**
     * This bug causes all of the format strings to be the same, because the
     * required expression [Measures].[Unit Sales] is not in the cache; bug
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-34">MONDRIAN-34</a>.
     */
    public void testBugMondrian34() {
        assertQueryReturns(
            "with member [Measures].[xxx] as '[Measures].[Store Sales]',\n"
            + " format_string = IIf([Measures].[Unit Sales] > 100000, \"AAA######.00\",\"BBB###.00\")\n"
            + "select {[Measures].[xxx]} ON columns,\n"
            + " {[Product].children} ON rows\n"
            + "from [Sales] where [Time].[1997]",

            "Axis #0:\n"
            + "{[Time].[1997]}\n"
            + "Axis #1:\n"
            + "{[Measures].[xxx]}\n"
            + "Axis #2:\n"
            + "{[Product].[Drink]}\n"
            + "{[Product].[Food]}\n"
            + "{[Product].[Non-Consumable]}\n"
            + "Row #0: BBB48836.21\n"
            + "Row #1: AAA409035.59\n"
            + "Row #2: BBB107366.33\n");
    }

    /**
     * Tuple as slicer causes {@link ClassCastException}; bug
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-36">MONDRIAN-36</a>.
     */
    public void testBugMondrian36() {
        assertQueryReturns(
            "select {[Measures].[Unit Sales]} ON columns,\n"
            + " {[Gender].Children} ON rows\n"
            + "from [Sales]\n"
            + "where ([Time].[1997], [Customers])",
            "Axis #0:\n"
            + "{[Time].[1997], [Customers].[All Customers]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Gender].[F]}\n"
            + "{[Gender].[M]}\n"
            + "Row #0: 131,558\n"
            + "Row #1: 135,215\n");
    }

    /**
     * Query with distinct-count measure and no other measures gives
     * {@link ArrayIndexOutOfBoundsException};
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-46">MONDRIAN-46</a>.
     */
    public void testBugMondrian46() {
        TestContext.instance().flushSchemaCache();
        assertQueryReturns(
            "select {[Measures].[Customer Count]} ON columns,\n"
            + "  {([Promotion Media].[All Media], [Product].[All Products])} ON rows\n"
            + "from [Sales]\n"
            + "where [Time].[1997]",
            "Axis #0:\n"
            + "{[Time].[1997]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Customer Count]}\n"
            + "Axis #2:\n"
            + "{[Promotion Media].[All Media], [Product].[All Products]}\n"
            + "Row #0: 5,581\n");
    }

    /**
     * Tests that the "Store" cube is working.
     *
     * <p>The [Fact Count] measure, which is implicitly created because the cube
     * definition does not include an explicit count measure, is flagged 'not
     * visible' but is still correctly returned from [Measures].Members.
     */
    public void testStoreCube() {
        assertQueryReturns(
            "select {[Measures].members} on columns,\n"
            + " {[Store Type].members} on rows\n"
            + "from [Store]"
            + "where [Store].[USA].[CA]",

            "Axis #0:\n"
            + "{[Store].[USA].[CA]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Sqft]}\n"
            + "{[Measures].[Grocery Sqft]}\n"
            + "{[Measures].[Fact Count]}\n"
            + "Axis #2:\n"
            + "{[Store Type].[All Store Types]}\n"
            + "{[Store Type].[Deluxe Supermarket]}\n"
            + "{[Store Type].[Gourmet Supermarket]}\n"
            + "{[Store Type].[HeadQuarters]}\n"
            + "{[Store Type].[Mid-Size Grocery]}\n"
            + "{[Store Type].[Small Grocery]}\n"
            + "{[Store Type].[Supermarket]}\n"
            + "Row #0: 69,764\n"
            + "Row #0: 44,868\n"
            + "Row #0: 5\n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #2: 23,688\n"
            + "Row #2: 15,337\n"
            + "Row #2: 1\n"
            + "Row #3: \n"
            + "Row #3: \n"
            + "Row #3: 1\n"
            + "Row #4: \n"
            + "Row #4: \n"
            + "Row #4: \n"
            + "Row #5: 22,478\n"
            + "Row #5: 15,321\n"
            + "Row #5: 1\n"
            + "Row #6: 23,598\n"
            + "Row #6: 14,210\n"
            + "Row #6: 2\n");
    }

    public void testSchemaLevelTableIsBad() {
        // todo: <Level table="nonexistentTable">
    }

    public void testSchemaLevelTableInAnotherHierarchy() {
        // todo:
        // <Cube>
        // <Hierarchy name="h1"><Table name="t1"/></Hierarchy>
        // <Hierarchy name="h2">
        //   <Table name="t2"/>
        //   <Level tableName="t1"/>
        // </Hierarchy>
        // </Cube>
    }

    public void testSchemaLevelWithViewSpecifiesTable() {
        // todo:
        // <Hierarchy>
        //  <View><SQL dialect="generic">select * from emp</SQL></View>
        //  <Level tableName="emp"/>
        // </hierarchy>
        // Should get error that tablename is not allowed
    }

    public void testSchemaLevelOrdinalInOtherTable() {
        // todo:
        // Hierarchy is based upon a join.
        // Level's name expression is in a different table than its ordinal.
    }

    public void testSchemaTopLevelNotUnique() {
        // todo:
        // Should get error if the top level of a hierarchy does not have
        // uniqueNames="true"
    }

    /**
     * Test case for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-8">MONDRIAN-8,
     * "Problem getting children in hierarchy based on join."</a>.
     * It happens when getting the children of a member crosses a table
     * boundary.
     */
    public void testBugMondrian8() {
        // minimal test case
        assertQueryReturns(
            "select {[Measures].[Unit Sales]} ON columns,\n"
            + "{[Product].[All Products].[Drink].[Beverages].[Drinks].[Flavored Drinks].children} ON rows\n"
            + "from [Sales]",

            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Drink].[Beverages].[Drinks].[Flavored Drinks].[Excellent]}\n"
            + "{[Product].[Drink].[Beverages].[Drinks].[Flavored Drinks].[Fabulous]}\n"
            + "{[Product].[Drink].[Beverages].[Drinks].[Flavored Drinks].[Skinner]}\n"
            + "{[Product].[Drink].[Beverages].[Drinks].[Flavored Drinks].[Token]}\n"
            + "{[Product].[Drink].[Beverages].[Drinks].[Flavored Drinks].[Washington]}\n"
            + "Row #0: 468\n"
            + "Row #1: 469\n"
            + "Row #2: 506\n"
            + "Row #3: 466\n"
            + "Row #4: 560\n");

        // shorter test case
        executeQuery(
            "select {[Measures].[Unit Sales], [Measures].[Store Cost], [Measures].[Store Sales]} ON columns,"
            + "ToggleDrillState({"
            + "([Promotion Media].[All Media].[Radio], [Product].[All Products].[Drink].[Beverages].[Drinks].[Flavored Drinks])"
            + "}, {[Product].[All Products].[Drink].[Beverages].[Drinks].[Flavored Drinks]}) ON rows "
            + "from [Sales] where ([Time].[1997])");
    }

    /**
     * The bug happened when a cell which was in cache was compared with a cell
     * which was not in cache. The compare method could not deal with the
     * {@link RuntimeException} which indicates that the cell is not in cache.
     */
    public void testBug636687() {
        executeQuery(
            "select {[Measures].[Unit Sales], [Measures].[Store Cost],[Measures].[Store Sales]} ON columns, "
            + "Order("
            + "{([Store].[All Stores].[USA].[CA], [Product].[All Products].[Drink].[Alcoholic Beverages]), "
            + "([Store].[All Stores].[USA].[CA], [Product].[All Products].[Drink].[Beverages]), "
            + "Crossjoin({[Store].[All Stores].[USA].[CA].Children}, {[Product].[All Products].[Drink].[Beverages]}), "
            + "([Store].[All Stores].[USA].[CA], [Product].[All Products].[Drink].[Dairy]), "
            + "([Store].[All Stores].[USA].[OR], [Product].[All Products].[Drink].[Alcoholic Beverages]), "
            + "([Store].[All Stores].[USA].[OR], [Product].[All Products].[Drink].[Beverages]), "
            + "([Store].[All Stores].[USA].[OR], [Product].[All Products].[Drink].[Dairy]), "
            + "([Store].[All Stores].[USA].[WA], [Product].[All Products].[Drink].[Alcoholic Beverages]), "
            + "([Store].[All Stores].[USA].[WA], [Product].[All Products].[Drink].[Beverages]), "
            + "([Store].[All Stores].[USA].[WA], [Product].[All Products].[Drink].[Dairy])}, "
            + "[Measures].[Store Cost], BDESC) ON rows "
            + "from [Sales] "
            + "where ([Time].[1997])");
        executeQuery(
            "select {[Measures].[Unit Sales], [Measures].[Store Cost], [Measures].[Store Sales]} ON columns, "
            + "Order("
            + "{([Store].[All Stores].[USA].[WA], [Product].[All Products].[Drink].[Beverages]), "
            + "([Store].[All Stores].[USA].[CA], [Product].[All Products].[Drink].[Beverages]), "
            + "([Store].[All Stores].[USA].[OR], [Product].[All Products].[Drink].[Beverages]), "
            + "([Store].[All Stores].[USA].[WA], [Product].[All Products].[Drink].[Alcoholic Beverages]), "
            + "([Store].[All Stores].[USA].[CA], [Product].[All Products].[Drink].[Alcoholic Beverages]), "
            + "([Store].[All Stores].[USA].[OR], [Product].[All Products].[Drink].[Alcoholic Beverages]), "
            + "([Store].[All Stores].[USA].[WA], [Product].[All Products].[Drink].[Dairy]), "
            + "([Store].[All Stores].[USA].[CA].[San Diego], [Product].[All Products].[Drink].[Beverages]), "
            + "([Store].[All Stores].[USA].[CA].[Los Angeles], [Product].[All Products].[Drink].[Beverages]), "
            + "Crossjoin({[Store].[All Stores].[USA].[CA].[Los Angeles]}, {[Product].[All Products].[Drink].[Beverages].Children}), "
            + "([Store].[All Stores].[USA].[CA].[Beverly Hills], [Product].[All Products].[Drink].[Beverages]), "
            + "([Store].[All Stores].[USA].[CA], [Product].[All Products].[Drink].[Dairy]), "
            + "([Store].[All Stores].[USA].[OR], [Product].[All Products].[Drink].[Dairy]), "
            + "([Store].[All Stores].[USA].[CA].[San Francisco], [Product].[All Products].[Drink].[Beverages])}, "
            + "[Measures].[Store Cost], BDESC) ON rows "
            + "from [Sales] "
            + "where ([Time].[1997])");
    }

    /**
     * Bug 769114: Internal error ("not found") when executing
     * Order(TopCount).
     */
    public void testBug769114() {
        assertQueryReturns(
            "select {[Measures].[Unit Sales], [Measures].[Store Cost], [Measures].[Store Sales]} ON columns,\n"
            + " Order(TopCount({[Product].[Product Category].Members}, 10.0, [Measures].[Unit Sales]), [Measures].[Store Sales], ASC) ON rows\n"
            + "from [Sales]\n"
            + "where [Time].[1997]",
            "Axis #0:\n"
            + "{[Time].[1997]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Store Cost]}\n"
            + "{[Measures].[Store Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Food].[Baked Goods].[Bread]}\n"
            + "{[Product].[Food].[Deli].[Meat]}\n"
            + "{[Product].[Food].[Dairy].[Dairy]}\n"
            + "{[Product].[Food].[Baking Goods].[Baking Goods]}\n"
            + "{[Product].[Food].[Baking Goods].[Jams and Jellies]}\n"
            + "{[Product].[Food].[Canned Foods].[Canned Soup]}\n"
            + "{[Product].[Food].[Frozen Foods].[Vegetables]}\n"
            + "{[Product].[Food].[Snack Foods].[Snack Foods]}\n"
            + "{[Product].[Food].[Produce].[Fruit]}\n"
            + "{[Product].[Food].[Produce].[Vegetables]}\n"
            + "Row #0: 7,870\n"
            + "Row #0: 6,564.09\n"
            + "Row #0: 16,455.43\n"
            + "Row #1: 9,433\n"
            + "Row #1: 8,215.81\n"
            + "Row #1: 20,616.29\n"
            + "Row #2: 12,885\n"
            + "Row #2: 12,228.85\n"
            + "Row #2: 30,508.85\n"
            + "Row #3: 8,357\n"
            + "Row #3: 6,123.32\n"
            + "Row #3: 15,446.69\n"
            + "Row #4: 11,888\n"
            + "Row #4: 9,247.29\n"
            + "Row #4: 23,223.72\n"
            + "Row #5: 8,006\n"
            + "Row #5: 6,408.29\n"
            + "Row #5: 15,966.10\n"
            + "Row #6: 6,984\n"
            + "Row #6: 5,885.05\n"
            + "Row #6: 14,769.82\n"
            + "Row #7: 30,545\n"
            + "Row #7: 26,963.34\n"
            + "Row #7: 67,609.82\n"
            + "Row #8: 11,767\n"
            + "Row #8: 10,312.77\n"
            + "Row #8: 25,816.13\n"
            + "Row #9: 20,739\n"
            + "Row #9: 18,048.81\n"
            + "Row #9: 45,185.41\n");
    }

    /**
     * Bug 793616: Deeply nested UNION function takes forever to validate.
     * (Problem was that each argument of a function was validated twice, hence
     * the validation time was <code>O(2 ^ depth)</code>.)
     */
    public void _testBug793616() {
        if (props.TestExpDependencies.get() > 0) {
            // Don't run this test if dependency-checking is enabled.
            // Dependency checking will hugely slow down evaluation, and give
            // the false impression that the validation performance bug has
            // returned.
            return;
        }
        final long start = System.currentTimeMillis();
        Connection connection = getTestContext().getConnection();
        final String queryString =
            "select {[Measures].[Unit Sales],\n"
            + " [Measures].[Store Cost],\n"
            + " [Measures].[Store Sales]} ON columns,\n"
            + "Hierarchize(Union(Union(Union(Union(Union(Union(Union(Union(Union(Union(Union(Union(Union(Union(Union(Union(Union(Union(Union(Union\n"
            + "({([Gender].[All Gender],\n"
            + " [Marital Status].[All Marital Status],\n"
            + " [Customers].[All Customers],\n"
            + " [Product].[All Products])},\n"
            + " Crossjoin ([Gender].[All Gender].Children,\n"
            + " {([Marital Status].[All Marital Status],\n"
            + " [Customers].[All Customers],\n"
            + " [Product].[All Products])})),\n"
            + " Crossjoin(Crossjoin({[Gender].[All Gender].[F]},\n"
            + " [Marital Status].[All Marital Status].Children),\n"
            + " {([Customers].[All Customers],\n"
            + " [Product].[All Products])})),\n"
            + " Crossjoin(Crossjoin({([Gender].[All Gender].[F],\n"
            + " [Marital Status].[All Marital Status].[M])},\n"
            + " [Customers].[All Customers].Children),\n"
            + " {[Product].[All Products]})),\n"
            + " Crossjoin(Crossjoin({([Gender].[All Gender].[F],\n"
            + " [Marital Status].[All Marital Status].[M])},\n"
            + " [Customers].[All Customers].[USA].Children),\n"
            + " {[Product].[All Products]})),\n"
            + " Crossjoin ({([Gender].[All Gender].[F], [Marital Status].[All Marital Status].[M], [Customers].[All Customers].[USA].[CA])},\n"
            + "   [Product].[All Products].Children)),\n"
            + " Crossjoin({([Gender].[All Gender].[F], [Marital Status].[All Marital Status].[M], [Customers].[All Customers].[USA].[OR])},\n"
            + "   [Product].[All Products].Children)),\n"
            + " Crossjoin({([Gender].[All Gender].[F], [Marital Status].[All Marital Status].[M], [Customers].[All Customers].[USA].[WA])},\n"
            + "   [Product].[All Products].Children)),\n"
            + " Crossjoin ({([Gender].[All Gender].[F], [Marital Status].[All Marital Status].[M], [Customers].[All Customers].[USA])},\n"
            + "   [Product].[All Products].Children)),\n"
            + " Crossjoin(\n"
            + "   Crossjoin({([Gender].[All Gender].[F], [Marital Status].[All Marital Status].[S])}, [Customers].[All Customers].Children),\n"
            + "   {[Product].[All Products]})),\n"
            + " Crossjoin(Crossjoin({([Gender].[All Gender].[F],\n"
            + " [Marital Status].[All Marital Status].[S])},\n"
            + " [Customers].[All Customers].[USA].Children),\n"
            + " {[Product].[All Products]})),\n"
            + " Crossjoin ({([Gender].[All Gender].[F],\n"
            + " [Marital Status].[All Marital Status].[S],\n"
            + " [Customers].[All Customers].[USA].[CA])},\n"
            + " [Product].[All Products].Children)),\n"
            + " Crossjoin({([Gender].[All Gender].[F],\n"
            + " [Marital Status].[All Marital Status].[S],\n"
            + " [Customers].[All Customers].[USA].[OR])},\n"
            + " [Product].[All Products].Children)),\n"
            + " Crossjoin({([Gender].[All Gender].[F],\n"
            + " [Marital Status].[All Marital Status].[S],\n"
            + " [Customers].[All Customers].[USA].[WA])},\n"
            + " [Product].[All Products].Children)),\n"
            + " Crossjoin ({([Gender].[All Gender].[F],\n"
            + " [Marital Status].[All Marital Status].[S],\n"
            + " [Customers].[All Customers].[USA])},\n"
            + " [Product].[All Products].Children)),\n"
            + " Crossjoin(Crossjoin({([Gender].[All Gender].[F],\n"
            + " [Marital Status].[All Marital Status])},\n"
            + " [Customers].[All Customers].Children),\n"
            + " {[Product].[All Products]})),\n"
            + " Crossjoin(Crossjoin({([Gender].[All Gender].[F],\n"
            + " [Marital Status].[All Marital Status])},\n"
            + " [Customers].[All Customers].[USA].Children),\n"
            + " {[Product].[All Products]})),\n"
            + " Crossjoin ({([Gender].[All Gender].[F],\n"
            + " [Marital Status].[All Marital Status],\n"
            + " [Customers].[All Customers].[USA].[CA])},\n"
            + " [Product].[All Products].Children)),\n"
            + " Crossjoin({([Gender].[All Gender].[F],\n"
            + " [Marital Status].[All Marital Status],\n"
            + " [Customers].[All Customers].[USA].[OR])},\n"
            + " [Product].[All Products].Children)),\n"
            + " Crossjoin({([Gender].[All Gender].[F],\n"
            + " [Marital Status].[All Marital Status],\n"
            + " [Customers].[All Customers].[USA].[WA])},\n"
            + " [Product].[All Products].Children)),\n"
            + " Crossjoin ({([Gender].[All Gender].[F],\n"
            + " [Marital Status].[All Marital Status],\n"
            + " [Customers].[All Customers].[USA])},\n"
            + " [Product].[All Products].Children))) ON rows  from [Sales]  where [Time].[1997]";
        Query query = connection.parseQuery(queryString);
        // If this call took longer than 10 seconds, the performance bug has
        // probably resurfaced again.
        final long afterParseMillis = System.currentTimeMillis();
        final long afterParseNonDbMillis =
            afterParseMillis - Util.dbTimeMillis();
        final long parseMillis = afterParseMillis - start;
        assertTrue(
            "performance problem: parse took " + parseMillis + " milliseconds",
            parseMillis <= 10000);

        Result result = connection.execute(query);
        assertEquals(59, result.getAxes()[1].getPositions().size());

        // If this call took longer than 10 seconds,
        // or 2 seconds exclusing db access,
        // the performance bug has
        // probably resurfaced again.
        final long afterExecMillis = System.currentTimeMillis();
        final long afterExecNonDbMillis = afterExecMillis - Util.dbTimeMillis();
        final long execNonDbMillis =
            afterExecNonDbMillis - afterParseNonDbMillis;
        final long execMillis = (afterExecMillis - afterParseMillis);
        assertTrue(
            "performance problem: execute took "
            + execMillis
            + " milliseconds, "
            + execNonDbMillis
            + " milliseconds excluding db",
            execNonDbMillis <= 2000 && execMillis <= 30000);
    }

    public void testCatalogHierarchyBasedOnView() {
        // Don't run this test if aggregates are enabled: two levels mapped to
        // the "gender" column confuse the agg engine.
        if (props.ReadAggregates.get()) {
            return;
        }
        TestContext testContext = TestContext.instance().createSubstitutingCube(
            "Sales",
            "<Dimension name=\"Gender2\" foreignKey=\"customer_id\">\n"
            + "  <Hierarchy hasAll=\"true\" allMemberName=\"All Gender\" primaryKey=\"customer_id\">\n"
            + "    <View alias=\"gender2\">\n"
            + "      <SQL dialect=\"generic\">\n"
            + "        <![CDATA[SELECT * FROM customer]]>\n"
            + "      </SQL>\n"
            + "      <SQL dialect=\"oracle\">\n"
            + "        <![CDATA[SELECT * FROM \"customer\"]]>\n"
            + "      </SQL>\n"
            + "      <SQL dialect=\"hsqldb\">\n"
            + "        <![CDATA[SELECT * FROM \"customer\"]]>\n"
            + "      </SQL>\n"
            + "      <SQL dialect=\"derby\">\n"
            + "        <![CDATA[SELECT * FROM \"customer\"]]>\n"
            + "      </SQL>\n"
            + "      <SQL dialect=\"luciddb\">\n"
            + "        <![CDATA[SELECT * FROM \"customer\"]]>\n"
            + "      </SQL>\n"
            + "      <SQL dialect=\"db2\">\n"
            + "        <![CDATA[SELECT * FROM \"customer\"]]>\n"
            + "      </SQL>\n"
            + "      <SQL dialect=\"neoview\">\n"
            + "        <![CDATA[SELECT * FROM \"customer\"]]>\n"
            + "      </SQL>\n"
            + "      <SQL dialect=\"netezza\">\n"
            + "        <![CDATA[SELECT * FROM \"customer\"]]>\n"
            + "      </SQL>\n"
            + "    </View>\n"
            + "    <Level name=\"Gender\" column=\"gender\" uniqueMembers=\"true\"/>\n"
            + "  </Hierarchy>\n"
            + "</Dimension>",
            null);
        if (!testContext.getDialect().allowsFromQuery()) {
            return;
        }
        testContext.assertAxisReturns(
            "[Gender2].members",
            "[Gender2].[All Gender]\n"
            + "[Gender2].[F]\n"
            + "[Gender2].[M]");
    }


    public void testMemberSameNameAsLevel() throws SQLException {
        // http://jira.pentaho.com/browse/ANALYZER-1618
        // Tests the case where the Level name matches the name of a member
        // in the level.  We were failing to resolve such members.
        // In this test the "Product Family" level has been renamed "Drink"
        TestContext testContext = TestContext.instance().createSubstitutingCube(
            "Sales",
            "   <Dimension name=\"ProdAmbiguousLevelName\" foreignKey=\"product_id\">\n"
            + "    <Hierarchy hasAll=\"true\" primaryKey=\"product_id\" primaryKeyTable=\"product\">\n"
            + "      <Join leftKey=\"product_class_id\" rightKey=\"product_class_id\">\n"
            + "        <Table name=\"product\"/>\n"
            + "        <Table name=\"product_class\"/>\n"
            + "      </Join>\n"
            + "\n"
            + "      <Level name=\"Drink\" table=\"product_class\" column=\"product_family\"\n"
            + "          uniqueMembers=\"true\"/>\n"
            + "      <Level name=\"Beverages\" table=\"product_class\" column=\"product_department\"\n"
            + "          uniqueMembers=\"false\"/>\n"
            + "      <Level name=\"Product Category\" table=\"product_class\" column=\"product_category\"\n"
            + "          uniqueMembers=\"false\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n",
            null);

        // These two references should resolve
        // to the same member whether used in the WITH block or on an axis
        String[] alternateReferences = {
            "ProdAmbiguousLevelName.Drink.Drink.calc",
            "ProdAmbiguousLevelName.[All ProdAmbiguousLevelNames].Drink.calc"
        };
        for (String withMemberName : alternateReferences) {
            for (String queryMemberName : alternateReferences) {
                testContext.assertQueryReturns(
                    "with member " + withMemberName + " as '1' "
                    + " select " + queryMemberName + " on 0 from sales",
                    "Axis #0:\n"
                    + "{}\n"
                    + "Axis #1:\n"
                    + "{[ProdAmbiguousLevelName].[Drink].[Drink].[calc]}\n"
                    + "Row #0: 1\n");
            }
        }
    }

    /**
     * Run a query against a large hierarchy, to make sure that we can generate
     * joins correctly. This probably won't work in MySQL.
     */
    public void testCatalogHierarchyBasedOnView2() {
        // Don't run this test if aggregates are enabled: two levels mapped to
        // the "gender" column confuse the agg engine.
        if (props.ReadAggregates.get()) {
            return;
        }
        if (getTestContext().getDialect().allowsFromQuery()) {
            return;
        }
        TestContext testContext = TestContext.instance().createSubstitutingCube(
            "Sales",
            "<Dimension name=\"ProductView\" foreignKey=\"product_id\">\n"
            + "   <Hierarchy hasAll=\"true\" primaryKey=\"product_id\" primaryKeyTable=\"productView\">\n"
            + "       <View alias=\"productView\">\n"
            + "           <SQL dialect=\"db2\"><![CDATA[\n"
            + "SELECT *\n"
            + "FROM \"product\", \"product_class\"\n"
            + "WHERE \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\"\n"
            + "]]>\n"
            + "           </SQL>\n"
            + "           <SQL dialect=\"mssql\"><![CDATA[\n"
            + "SELECT \"product\".\"product_id\",\n"
            + "\"product\".\"brand_name\",\n"
            + "\"product\".\"product_name\",\n"
            + "\"product\".\"SKU\",\n"
            + "\"product\".\"SRP\",\n"
            + "\"product\".\"gross_weight\",\n"
            + "\"product\".\"net_weight\",\n"
            + "\"product\".\"recyclable_package\",\n"
            + "\"product\".\"low_fat\",\n"
            + "\"product\".\"units_per_case\",\n"
            + "\"product\".\"cases_per_pallet\",\n"
            + "\"product\".\"shelf_width\",\n"
            + "\"product\".\"shelf_height\",\n"
            + "\"product\".\"shelf_depth\",\n"
            + "\"product_class\".\"product_class_id\",\n"
            + "\"product_class\".\"product_subcategory\",\n"
            + "\"product_class\".\"product_category\",\n"
            + "\"product_class\".\"product_department\",\n"
            + "\"product_class\".\"product_family\"\n"
            + "FROM \"product\" inner join \"product_class\"\n"
            + "ON \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\"\n"
            + "]]>\n"
            + "           </SQL>\n"
            + "           <SQL dialect=\"mysql\"><![CDATA[\n"
            + "SELECT `product`.`product_id`,\n"
            + "`product`.`brand_name`,\n"
            + "`product`.`product_name`,\n"
            + "`product`.`SKU`,\n"
            + "`product`.`SRP`,\n"
            + "`product`.`gross_weight`,\n"
            + "`product`.`net_weight`,\n"
            + "`product`.`recyclable_package`,\n"
            + "`product`.`low_fat`,\n"
            + "`product`.`units_per_case`,\n"
            + "`product`.`cases_per_pallet`,\n"
            + "`product`.`shelf_width`,\n"
            + "`product`.`shelf_height`,\n"
            + "`product`.`shelf_depth`,\n"
            + "`product_class`.`product_class_id`,\n"
            + "`product_class`.`product_family`,\n"
            + "`product_class`.`product_department`,\n"
            + "`product_class`.`product_category`,\n"
            + "`product_class`.`product_subcategory` \n"
            + "FROM `product`, `product_class`\n"
            + "WHERE `product`.`product_class_id` = `product_class`.`product_class_id`\n"
            + "]]>\n"
            + "           </SQL>\n"
            + "           <SQL dialect=\"generic\"><![CDATA[\n"
            + "SELECT *\n"
            + "FROM \"product\", \"product_class\"\n"
            + "WHERE \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\"\n"
            + "]]>\n"
            + "           </SQL>\n"
            + "       </View>\n"
            + "       <Level name=\"Product Family\" column=\"product_family\" uniqueMembers=\"true\"/>\n"
            + "       <Level name=\"Product Department\" column=\"product_department\" uniqueMembers=\"false\"/>\n"
            + "       <Level name=\"Product Category\" column=\"product_category\" uniqueMembers=\"false\"/>\n"
            + "       <Level name=\"Product Subcategory\" column=\"product_subcategory\" uniqueMembers=\"false\"/>\n"
            + "       <Level name=\"Brand Name\" column=\"brand_name\" uniqueMembers=\"false\"/>\n"
            + "       <Level name=\"Product Name\" column=\"product_name\" uniqueMembers=\"true\"/>\n"
            + "   </Hierarchy>\n"
            + "</Dimension>");
        testContext.assertQueryReturns(
            "select {[Measures].[Unit Sales]} on columns,\n"
            + " {[ProductView].[Drink].[Beverages].children} on rows\n"
            + "from Sales",

            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[ProductView].[Drink].[Beverages].[Carbonated Beverages]}\n"
            + "{[ProductView].[Drink].[Beverages].[Drinks]}\n"
            + "{[ProductView].[Drink].[Beverages].[Hot Beverages]}\n"
            + "{[ProductView].[Drink].[Beverages].[Pure Juice Beverages]}\n"
            + "Row #0: 3,407\n"
            + "Row #1: 2,469\n"
            + "Row #2: 4,301\n"
            + "Row #3: 3,396\n");
    }

    public void testCountDistinct() {
        assertQueryReturns(
            "select {[Measures].[Unit Sales], [Measures].[Customer Count]} on columns,\n"
            + " {[Gender].members} on rows\n"
            + "from Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Customer Count]}\n"
            + "Axis #2:\n"
            + "{[Gender].[All Gender]}\n"
            + "{[Gender].[F]}\n"
            + "{[Gender].[M]}\n"
            + "Row #0: 266,773\n"
            + "Row #0: 5,581\n"
            + "Row #1: 131,558\n"
            + "Row #1: 2,755\n"
            + "Row #2: 135,215\n"
            + "Row #2: 2,826\n");
    }

    /**
     * Turn off aggregate caching and run query with both use of aggregate
     * tables on and off - should result in the same answer.
     * Note that if the "mondrian.rolap.aggregates.Read" property is not true,
     * then no aggregate tables is be read in any event.
     */
    public void testCountDistinctAgg() {
        boolean use_agg_orig = props.UseAggregates.get();

        // turn off caching
        propSaver.set(props.DisableCaching, true);

        assertQueryReturns(
            "select {[Measures].[Unit Sales], [Measures].[Customer Count]} on rows,\n"
            + "NON EMPTY {[Time].[1997].[Q1].[1]} ON COLUMNS\n"
            + "from Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time].[1997].[Q1].[1]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Customer Count]}\n"
            + "Row #0: 21,628\n"
            + "Row #1: 1,396\n");

        if (use_agg_orig) {
            propSaver.set(props.UseAggregates, false);
        } else {
            propSaver.set(props.UseAggregates, true);
        }

        assertQueryReturns(
            "select {[Measures].[Unit Sales], [Measures].[Customer Count]} on rows,\n"
            + "NON EMPTY {[Time].[1997].[Q1].[1]} ON COLUMNS\n"
            + "from Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time].[1997].[Q1].[1]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Customer Count]}\n"
            + "Row #0: 21,628\n"
            + "Row #1: 1,396\n");
    }

    /**
     *
     * There are cross database order issues in this test.
     *
     * MySQL and Access show the rows as:
     *
     * [Store Size in SQFT].[All Store Size in SQFTs]
     * [Store Size in SQFT].[null]
     * [Store Size in SQFT].[<each distinct store
     * size>]
     *
     * Postgres shows:
     *
     * [Store Size in SQFT].[All Store Size in SQFTs]
     * [Store Size in SQFT].[<each distinct store
     * size>]
     * [Store Size in SQFT].[null]
     *
     * The test failure is due to some inherent differences in the way
     * Postgres orders NULLs in a result set,
     * compared with MySQL and Access.
     *
     * From the MySQL 4.X manual:
     *
     * When doing an ORDER BY, NULL values are presented first if you do
     * ORDER BY ... ASC and last if you do ORDER BY ... DESC.
     *
     * From the Postgres 8.0 manual:
     *
     * The null value sorts higher than any other value. In other words,
     * with ascending sort order, null values sort at the end, and with
     * descending sort order, null values sort at the beginning.
     *
     * Oracle also sorts nulls high by default.
     *
     * So, this test has expected results that vary depending on whether
     * the database is being used sorts nulls high or low.
     *
     */
    public void testMemberWithNullKey() {
        if (!isDefaultNullMemberRepresentation()) {
            return;
        }
        Result result = executeQuery(
            "select {[Measures].[Unit Sales]} on columns,\n"
            + "{[Store Size in SQFT].members} on rows\n"
            + "from Sales");
        String resultString = TestContext.toString(result);
        resultString =
            Pattern.compile("\\.0\\]").matcher(resultString).replaceAll("]");

        // The members function hierarchizes its results, so nulls should
        // sort high, regardless of DBMS. Note that Oracle's driver says that
        // NULLs sort low, but it's lying.
        final boolean nullsSortHigh = false;
        int row = 0;

        final String expected =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Store Size in SQFT].[All Store Size in SQFTs]}\n"

            // null is at the start in order under DBMSs that sort null low
            + (!nullsSortHigh
                ? "{[Store Size in SQFT].[#null]}\n"
                : "")
            + "{[Store Size in SQFT].[20319]}\n"
            + "{[Store Size in SQFT].[21215]}\n"
            + "{[Store Size in SQFT].[22478]}\n"
            + "{[Store Size in SQFT].[23112]}\n"
            + "{[Store Size in SQFT].[23593]}\n"
            + "{[Store Size in SQFT].[23598]}\n"
            + "{[Store Size in SQFT].[23688]}\n"
            + "{[Store Size in SQFT].[23759]}\n"
            + "{[Store Size in SQFT].[24597]}\n"
            + "{[Store Size in SQFT].[27694]}\n"
            + "{[Store Size in SQFT].[28206]}\n"
            + "{[Store Size in SQFT].[30268]}\n"
            + "{[Store Size in SQFT].[30584]}\n"
            + "{[Store Size in SQFT].[30797]}\n"
            + "{[Store Size in SQFT].[33858]}\n"
            + "{[Store Size in SQFT].[34452]}\n"
            + "{[Store Size in SQFT].[34791]}\n"
            + "{[Store Size in SQFT].[36509]}\n"
            + "{[Store Size in SQFT].[38382]}\n"
            + "{[Store Size in SQFT].[39696]}\n"

            // null is at the end in order for DBMSs that sort nulls high
            + (nullsSortHigh
               ? "{[Store Size in SQFT].[#null]}\n"
               : "")
            + "Row #" + row++ + ": 266,773\n"
            + (!nullsSortHigh ? "Row #" + row++ + ": 39,329\n" : "")
            + "Row #" + row++ + ": 26,079\n"
            + "Row #" + row++ + ": 25,011\n"
            + "Row #" + row++ + ": 2,117\n"
            + "Row #" + row++ + ": \n"
            + "Row #" + row++ + ": \n"
            + "Row #" + row++ + ": 25,663\n"
            + "Row #" + row++ + ": 21,333\n"
            + "Row #" + row++ + ": \n"
            + "Row #" + row++ + ": \n"
            + "Row #" + row++ + ": 41,580\n"
            + "Row #" + row++ + ": 2,237\n"
            + "Row #" + row++ + ": 23,591\n"
            + "Row #" + row++ + ": \n"
            + "Row #" + row++ + ": \n"
            + "Row #" + row++ + ": 35,257\n"
            + "Row #" + row++ + ": \n"
            + "Row #" + row++ + ": \n"
            + "Row #" + row++ + ": \n"
            + "Row #" + row++ + ": \n"
            + "Row #" + row++ + ": 24,576\n"
            + (nullsSortHigh ? "Row #" + row++ + ": 39,329\n" : "");
        TestContext.assertEqualsVerbose(expected, resultString);
    }

    /**
     * Test case for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-977">MONDRIAN-977,
     * "NPE in Query with Crossjoin Descendants of Unknown Member"</a>.
     */
    public void testCrossjoinWithDescendantsAndUnknownMember() {
        propSaver.set(
            MondrianProperties.instance().IgnoreInvalidMembersDuringQuery,
            true);
        assertQueryReturns(
            "select {[Measures].[Unit Sales]} on columns,\n"
            + "NON EMPTY CrossJoin(\n"
            + " Descendants([Product].[All Products], [Product].[Product Family]),\n"
            + " Descendants([Store].[All Stores].[Foo], [Store].[Store State])) on rows\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n");
    }

    /**
     * Slicer contains <code>[Promotion Media].[Daily Paper]</code>, but
     * filter expression is in terms of <code>[Promotion Media].[Radio]</code>.
     */
    public void testSlicerOverride() {
        assertQueryReturns(
            "with member [Measures].[Radio Unit Sales] as \n"
            + " '([Measures].[Unit Sales], [Promotion Media].[Radio])'\n"
            + "select {[Measures].[Unit Sales], [Measures].[Radio Unit Sales]} on columns,\n"
            + " filter([Product].[Product Department].members, [Promotion Media].[Radio] > 50) on rows\n"
            + "from Sales\n"
            + "where ([Promotion Media].[Daily Paper], [Time].[1997].[Q1])",
            "Axis #0:\n"
            + "{[Promotion Media].[Daily Paper], [Time].[1997].[Q1]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Radio Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Food].[Produce]}\n"
            + "{[Product].[Food].[Snack Foods]}\n"
            + "Row #0: 692\n"
            + "Row #0: 87\n"
            + "Row #1: 447\n"
            + "Row #1: 63\n");
    }

    public void testMembersOfLargeDimensionTheHardWay() {
        // Avoid this test if memory is scarce.
        if (Bug.avoidMemoryOverflow(TestContext.instance().getDialect())) {
            return;
        }

        final Connection connection =
            TestContext.instance().getConnection();
        String queryString =
            "select {[Measures].[Unit Sales]} on columns,\n"
            + "{[Customers].members} on rows\n"
            + "from Sales";
        Query query = connection.parseQuery(queryString);
        Result result = connection.execute(query);
        assertEquals(10407, result.getAxes()[1].getPositions().size());
    }

    public void testUnparse() {
        Connection connection = getConnection();
        Query query = connection.parseQuery(
            "with member [Measures].[Rendite] as \n"
            + " '(([Measures].[Store Sales] - [Measures].[Store Cost])) / [Measures].[Store Cost]',\n"
            + " format_string = iif(([Measures].[Store Sales] - [Measures].[Store Cost]) / [Measures].[Store Cost] * 100 > \n"
            + "     Parameter (\"UpperLimit\", NUMERIC, 151, \"Obere Grenze\"), \n"
            + "   \"|#.00%|arrow='up'\",\n"
            + "   iif(([Measures].[Store Sales] - [Measures].[Store Cost]) / [Measures].[Store Cost] * 100 < \n"
            + "       Parameter(\"LowerLimit\", NUMERIC, 150, \"Untere Grenze\"),\n"
            + "     \"|#.00%|arrow='down'\",\n"
            + "     \"|#.00%|arrow='right'\"))\n"
            + "select {[Measures].members} on columns\n"
            + "from Sales");
        final String s = Util.unparse(query);
        // Parentheses are added to reflect operator precedence, but that's ok.
        // Note that the doubled parentheses in line #2 of the query have been
        // reduced to a single level.
        TestContext.assertEqualsVerbose(
            "with member [Measures].[Rendite] as '(([Measures].[Store Sales] - [Measures].[Store Cost]) / [Measures].[Store Cost])', "
            + "format_string = IIf((((([Measures].[Store Sales] - [Measures].[Store Cost]) / [Measures].[Store Cost]) * 100) > Parameter(\"UpperLimit\", NUMERIC, 151, \"Obere Grenze\")), "
            + "\"|#.00%|arrow='up'\", "
            + "IIf((((([Measures].[Store Sales] - [Measures].[Store Cost]) / [Measures].[Store Cost]) * 100) < Parameter(\"LowerLimit\", NUMERIC, 150, \"Untere Grenze\")), "
            + "\"|#.00%|arrow='down'\", \"|#.00%|arrow='right'\"))\n"
            + "select {[Measures].Members} ON COLUMNS\n"
            + "from [Sales]\n",
            s);
    }

    public void testUnparse2() {
        Connection connection = getConnection();
        Query query = connection.parseQuery(
            "with member [Measures].[Foo] as '1', "
            + "format_string='##0.00', "
            + "funny=IIf(1=1,\"x\"\"y\",\"foo\") "
            + "select {[Measures].[Foo]} on columns from Sales");
        final String s = query.toString();
        // The "format_string" property, a string literal, is now delimited by
        // double-quotes. This won't work in MSOLAP, but for Mondrian it's
        // consistent with the fact that property values are expressions,
        // not enclosed in single-quotes.
        TestContext.assertEqualsVerbose(
            "with member [Measures].[Foo] as '1', "
            + "format_string = \"##0.00\", "
            + "funny = IIf((1 = 1), \"x\"\"y\", \"foo\")\n"
            + "select {[Measures].[Foo]} ON COLUMNS\n"
            + "from [Sales]\n",
            s);
    }

    /**
     * Basically, the LookupCube function can evaluate a single MDX statement
     * against a cube other than the cube currently indicated by query context
     * to retrieve a single string or numeric result.
     *
     * <p>For example, the Budget cube in the FoodMart 2000 database contains
     * budget information that can be displayed by store. The Sales cube in the
     * FoodMart 2000 database contains sales information that can be displayed
     * by store. Since no virtual cube exists in the FoodMart 2000 database that
     * joins the Sales and Budget cubes together, comparing the two sets of
     * figures would be difficult at best.
     *
     * <p><b>Note<b> In many situations a virtual cube can be used to integrate
     * data from multiple cubes, which will often provide a simpler and more
     * efficient solution than the LookupCube function. This example uses the
     * LookupCube function for purposes of illustration.
     *
     * <p>The following MDX query, however, uses the LookupCube function to
     * retrieve unit sales information for each store from the Sales cube,
     * presenting it side by side with the budget information from the Budget
     * cube.
     */
    public void _testLookupCube() {
        assertQueryReturns(
            "WITH MEMBER Measures.[Store Unit Sales] AS \n"
            + " 'LookupCube(\"Sales\", \"(\" + MemberToStr(Store.CurrentMember) + \", Measures.[Unit Sales])\")'\n"
            + "SELECT\n"
            + " {Measures.Amount, Measures.[Store Unit Sales]} ON COLUMNS,\n"
            + " Store.CA.CHILDREN ON ROWS\n"
            + "FROM Budget", "");
    }

    /**
     * <p>Basket analysis is a topic better suited to data mining discussions,
     * but some basic forms of basket analysis can be handled through the use of
     * MDX queries.
     *
     * <p>For example, one method of basket analysis groups customers based on
     * qualification. In the following example, a qualified customer is one who
     * has more than $10,000 in store sales or more than 10 unit sales. The
     * following table illustrates such a report, run against the Sales cube in
     * FoodMart 2000 with qualified customers grouped by the Country and State
     * Province levels of the Customers dimension. The count and store sales
     * total of qualified customers is represented by the Qualified Count and
     * Qualified Sales columns, respectively.
     *
     * <p>To accomplish this basic form of basket analysis, the following MDX
     * query constructs two calculated members. The first calculated member uses
     * the MDX Count, Filter, and Descendants functions to create the Qualified
     * Count column, while the second calculated member uses the MDX Sum,
     * Filter, and Descendants functions to create the Qualified Sales column.
     *
     * <p>The key to this MDX query is the use of Filter and Descendants
     * together to screen out non-qualified customers. Once screened out, the
     * Sum and Count MDX functions can then be used to provide aggregation data
     * only on qualified customers.
     */
    public void testBasketAnalysis() {
        assertQueryReturns(
            "WITH MEMBER [Measures].[Qualified Count] AS\n"
            + " 'COUNT(FILTER(DESCENDANTS(Customers.CURRENTMEMBER, [Customers].[Name]),\n"
            + "               ([Measures].[Store Sales]) > 10000 OR ([Measures].[Unit Sales]) > 10))'\n"
            + "MEMBER [Measures].[Qualified Sales] AS\n"
            + " 'SUM(FILTER(DESCENDANTS(Customers.CURRENTMEMBER, [Customers].[Name]),\n"
            + "             ([Measures].[Store Sales]) > 10000 OR ([Measures].[Unit Sales]) > 10),\n"
            + "      ([Measures].[Store Sales]))'\n"
            + "SELECT {[Measures].[Qualified Count], [Measures].[Qualified Sales]} ON COLUMNS,\n"
            + "  DESCENDANTS([Customers].[All Customers], [State Province], SELF_AND_BEFORE) ON ROWS\n"
            + "FROM Sales",

            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Qualified Count]}\n"
            + "{[Measures].[Qualified Sales]}\n"
            + "Axis #2:\n"
            + "{[Customers].[All Customers]}\n"
            + "{[Customers].[Canada]}\n"
            + "{[Customers].[Canada].[BC]}\n"
            + "{[Customers].[Mexico]}\n"
            + "{[Customers].[Mexico].[DF]}\n"
            + "{[Customers].[Mexico].[Guerrero]}\n"
            + "{[Customers].[Mexico].[Jalisco]}\n"
            + "{[Customers].[Mexico].[Mexico]}\n"
            + "{[Customers].[Mexico].[Oaxaca]}\n"
            + "{[Customers].[Mexico].[Sinaloa]}\n"
            + "{[Customers].[Mexico].[Veracruz]}\n"
            + "{[Customers].[Mexico].[Yucatan]}\n"
            + "{[Customers].[Mexico].[Zacatecas]}\n"
            + "{[Customers].[USA]}\n"
            + "{[Customers].[USA].[CA]}\n"
            + "{[Customers].[USA].[OR]}\n"
            + "{[Customers].[USA].[WA]}\n"
            + "Row #0: 4,719.00\n"
            + "Row #0: 553,587.77\n"
            + "Row #1: .00\n"
            + "Row #1: \n"
            + "Row #2: .00\n"
            + "Row #2: \n"
            + "Row #3: .00\n"
            + "Row #3: \n"
            + "Row #4: .00\n"
            + "Row #4: \n"
            + "Row #5: .00\n"
            + "Row #5: \n"
            + "Row #6: .00\n"
            + "Row #6: \n"
            + "Row #7: .00\n"
            + "Row #7: \n"
            + "Row #8: .00\n"
            + "Row #8: \n"
            + "Row #9: .00\n"
            + "Row #9: \n"
            + "Row #10: .00\n"
            + "Row #10: \n"
            + "Row #11: .00\n"
            + "Row #11: \n"
            + "Row #12: .00\n"
            + "Row #12: \n"
            + "Row #13: 4,719.00\n"
            + "Row #13: 553,587.77\n"
            + "Row #14: 2,149.00\n"
            + "Row #14: 151,509.69\n"
            + "Row #15: 1,008.00\n"
            + "Row #15: 141,899.84\n"
            + "Row #16: 1,562.00\n"
            + "Row #16: 260,178.24\n");
    }

    /**
     * Flushes the cache then runs {@link #testBasketAnalysis}, because this
     * test has been known to fail when run standalone.
     */
    public void testBasketAnalysisAfterFlush() {
        TestContext.instance().flushSchemaCache();
        testBasketAnalysis();
    }

    /**
     * <b>How Can I Perform Complex String Comparisons?</b>
     *
     * <p>MDX can handle basic string comparisons, but does not include complex
     * string comparison and manipulation functions, for example, for finding
     * substrings in strings or for supporting case-insensitive string
     * comparisons. However, since MDX can take advantage of external function
     * libraries, this question is easily resolved using string manipulation
     * and comparison functions from the Microsoft Visual Basic for
     * Applications (VBA) external function library.
     *
     * <p>For example, you want to report the unit sales of all fruit-based
     * products -- not only the sales of fruit, but canned fruit, fruit snacks,
     * fruit juices, and so on. By using the LCase and InStr VBA functions, the
     * following results are easily accomplished in a single MDX query, without
     * complex set construction or explicit member names within the query.
     *
     * <p>The following MDX query demonstrates how to achieve the results
     * displayed in the previous table. For each member in the Product
     * dimension, the name of the member is converted to lowercase using the
     * LCase VBA function. Then, the InStr VBA function is used to discover
     * whether or not the name contains the word "fruit". This information is
     * used to then construct a set, using the Filter MDX function, from only
     * those members from the Product dimension that contain the substring
     * "fruit" in their names.
     */
    public void testStringComparisons() {
        assertQueryReturns(
            "SELECT {Measures.[Unit Sales]} ON COLUMNS,\n"
            + "  FILTER([Product].[Product Name].MEMBERS,\n"
            + "         INSTR(LCASE([Product].CURRENTMEMBER.NAME), \"fruit\") <> 0) ON ROWS \n"
            + "FROM Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Food].[Canned Products].[Fruit].[Canned Fruit].[Applause].[Applause Canned Mixed Fruit]}\n"
            + "{[Product].[Food].[Canned Products].[Fruit].[Canned Fruit].[Big City].[Big City Canned Mixed Fruit]}\n"
            + "{[Product].[Food].[Canned Products].[Fruit].[Canned Fruit].[Green Ribbon].[Green Ribbon Canned Mixed Fruit]}\n"
            + "{[Product].[Food].[Canned Products].[Fruit].[Canned Fruit].[Swell].[Swell Canned Mixed Fruit]}\n"
            + "{[Product].[Food].[Canned Products].[Fruit].[Canned Fruit].[Toucan].[Toucan Canned Mixed Fruit]}\n"
            + "{[Product].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Best Choice].[Best Choice Apple Fruit Roll]}\n"
            + "{[Product].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Best Choice].[Best Choice Grape Fruit Roll]}\n"
            + "{[Product].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Best Choice].[Best Choice Raspberry Fruit Roll]}\n"
            + "{[Product].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Best Choice].[Best Choice Strawberry Fruit Roll]}\n"
            + "{[Product].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Fast].[Fast Apple Fruit Roll]}\n"
            + "{[Product].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Fast].[Fast Grape Fruit Roll]}\n"
            + "{[Product].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Fast].[Fast Raspberry Fruit Roll]}\n"
            + "{[Product].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Fast].[Fast Strawberry Fruit Roll]}\n"
            + "{[Product].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Fort West].[Fort West Apple Fruit Roll]}\n"
            + "{[Product].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Fort West].[Fort West Grape Fruit Roll]}\n"
            + "{[Product].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Fort West].[Fort West Raspberry Fruit Roll]}\n"
            + "{[Product].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Fort West].[Fort West Strawberry Fruit Roll]}\n"
            + "{[Product].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Horatio].[Horatio Apple Fruit Roll]}\n"
            + "{[Product].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Horatio].[Horatio Grape Fruit Roll]}\n"
            + "{[Product].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Horatio].[Horatio Raspberry Fruit Roll]}\n"
            + "{[Product].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Horatio].[Horatio Strawberry Fruit Roll]}\n"
            + "{[Product].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Nationeel].[Nationeel Apple Fruit Roll]}\n"
            + "{[Product].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Nationeel].[Nationeel Grape Fruit Roll]}\n"
            + "{[Product].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Nationeel].[Nationeel Raspberry Fruit Roll]}\n"
            + "{[Product].[Food].[Snack Foods].[Snack Foods].[Dried Fruit].[Nationeel].[Nationeel Strawberry Fruit Roll]}\n"
            + "Row #0: 205\n"
            + "Row #1: 204\n"
            + "Row #2: 142\n"
            + "Row #3: 204\n"
            + "Row #4: 187\n"
            + "Row #5: 174\n"
            + "Row #6: 114\n"
            + "Row #7: 110\n"
            + "Row #8: 150\n"
            + "Row #9: 149\n"
            + "Row #10: 173\n"
            + "Row #11: 163\n"
            + "Row #12: 154\n"
            + "Row #13: 181\n"
            + "Row #14: 178\n"
            + "Row #15: 210\n"
            + "Row #16: 189\n"
            + "Row #17: 177\n"
            + "Row #18: 191\n"
            + "Row #19: 149\n"
            + "Row #20: 169\n"
            + "Row #21: 185\n"
            + "Row #22: 216\n"
            + "Row #23: 167\n"
            + "Row #24: 138\n");
    }

    /**
     * Test case for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-539">MONDRIAN-539,
     * "Problem with the MID function getting last character in a string."</a>.
     */
    public void testMid() {
        assertQueryReturns(
            "with\n"
            + "member measures.x as 'Mid(\"yahoo\",5, 1)'\n"
            + "select {measures.x} ON COLUMNS from [Sales] ",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[x]}\n"
            + "Row #0: o\n");
    }

    /**
     * <b>How Can I Show Percentages as Measures?</b>
     *
     * <p>Another common business question easily answered through MDX is the
     * display of percent values created as available measures.
     *
     * <p>For example, the Sales cube in the FoodMart 2000 database contains
     * unit sales for each store in a given city, state, and country, organized
     * along the Sales dimension. A report is requested to show, for
     * California, the percentage of total unit sales attained by each city
     * with a store. The results are illustrated in the following table.
     *
     * <p>Because the parent of a member is typically another, aggregated
     * member in a regular dimension, this is easily achieved by the
     * construction of a calculated member, as demonstrated in the following MDX
     * query, using the CurrentMember and Parent MDX functions.
     */
    public void testPercentagesAsMeasures() {
        assertQueryReturns(
            // todo: "Store.[USA].[CA]" should be "Store.CA"
            "WITH MEMBER Measures.[Unit Sales Percent] AS\n"
            + "  '((Store.CURRENTMEMBER, Measures.[Unit Sales]) /\n"
            + "    (Store.CURRENTMEMBER.PARENT, Measures.[Unit Sales])) ',\n"
            + "  FORMAT_STRING = 'Percent'\n"
            + "SELECT {Measures.[Unit Sales], Measures.[Unit Sales Percent]} ON COLUMNS,\n"
            + "  ORDER(DESCENDANTS(Store.[USA].[CA], Store.[Store City], SELF), \n"
            + "        [Measures].[Unit Sales], ASC) ON ROWS\n"
            + "FROM Sales",

            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "{[Measures].[Unit Sales Percent]}\n"
            + "Axis #2:\n"
            + "{[Store].[USA].[CA].[Alameda]}\n"
            + "{[Store].[USA].[CA].[San Francisco]}\n"
            + "{[Store].[USA].[CA].[Beverly Hills]}\n"
            + "{[Store].[USA].[CA].[San Diego]}\n"
            + "{[Store].[USA].[CA].[Los Angeles]}\n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #1: 2,117\n"
            + "Row #1: 2.83%\n"
            + "Row #2: 21,333\n"
            + "Row #2: 28.54%\n"
            + "Row #3: 25,635\n"
            + "Row #3: 34.30%\n"
            + "Row #4: 25,663\n"
            + "Row #4: 34.33%\n");
    }

    /**
     * <b>How Can I Show Cumulative Sums as Measures?</b>
     *
     * <p>Another common business request, cumulative sums, is useful for
     * business reporting purposes. However, since aggregations are handled in a
     * hierarchical fashion, cumulative sums present some unique challenges in
     * Analysis Services.
     *
     * <p>The best way to create a cumulative sum is as a calculated measure in
     * MDX, using the Rank, Head, Order, and Sum MDX functions together.
     *
     * <p>For example, the following table illustrates a report that shows two
     * views of employee count in all stores and cities in California, sorted
     * by employee count. The first column shows the aggregated counts for each
     * store and city, while the second column shows aggregated counts for each
     * store, but cumulative counts for each city.
     *
     * <p>The cumulative number of employees for San Diego represents the value
     * of both Los Angeles and San Diego, the value for Beverly Hills represents
     * the cumulative total of Los Angeles, San Diego, and Beverly Hills, and so
     * on.
     *
     * <p>Since the members within the state of California have been ordered
     * from highest to lowest number of employees, this form of cumulative sum
     * measure provides a form of pareto analysis within each state.
     *
     * <p>To support this, the Order function is first used to reorder members
     * accordingly for both the Rank and Head functions. Once reordered, the
     * Rank function is used to supply the ranking of each tuple within the
     * reordered set of members, progressing as each member in the Store
     * dimension is examined. The value is then used to determine the number of
     * tuples to retrieve from the set of reordered members using the Head
     * function. Finally, the retrieved members are then added together using
     * the Sum function to obtain a cumulative sum. The following MDX query
     * demonstrates how all of this works in concert to provide cumulative
     * sums.
     *
     * <p>As an aside, a named set cannot be used in this situation to replace
     * the duplicate Order function calls. Named sets are evaluated once, when
     * a query is parsed -- since the set can change based on the fact that the
     * set can be different for each store member because the set is evaluated
     * for the children of multiple parents, the set does not change with
     * respect to its use in the Sum function. Since the named set is only
     * evaluated once, it would not satisfy the needs of this query.
     */
    public void _testCumlativeSums() {
        assertQueryReturns(
            // todo: "[Store].[USA].[CA]" should be "Store.CA"; implement "AS"
            "WITH MEMBER Measures.[Cumulative No of Employees] AS\n"
            + "  'SUM(HEAD(ORDER({[Store].Siblings}, [Measures].[Number of Employees], BDESC) AS OrderedSiblings,\n"
            + "            RANK([Store], OrderedSiblings)),\n"
            + "       [Measures].[Number of Employees])'\n"
            + "SELECT {[Measures].[Number of Employees], [Measures].[Cumulative No of Employees]} ON COLUMNS,\n"
            + "  ORDER(DESCENDANTS([Store].[USA].[CA], [Store State], AFTER), \n"
            + "        [Measures].[Number of Employees], BDESC) ON ROWS\n"
            + "FROM HR",
            "");
    }

    /**
     * <b>How Can I Implement a Logical AND or OR Condition in a WHERE
     * Clause?</b>
     *
     * <p>For SQL users, the use of AND and OR logical operators in the WHERE
     * clause of a SQL statement is an essential tool for constructing business
     * queries. However, the WHERE clause of an MDX statement serves a
     * slightly different purpose, and understanding how the WHERE clause is
     * used in MDX can assist in constructing such business queries.
     *
     * <p>The WHERE clause in MDX is used to further restrict the results of
     * an MDX query, in effect providing another dimension on which the results
     * of the query are further sliced. As such, only expressions that resolve
     * to a single tuple are allowed. The WHERE clause implicitly supports a
     * logical AND operation involving members across different dimensions, by
     * including the members as part of a tuple. To support logical AND
     * operations involving members within a single dimensions, as well as
     * logical OR operations, a calculated member needs to be defined in
     * addition to the use of the WHERE clause.
     *
     * <p>For example, the following MDX query illustrates the use of a
     * calculated member to support a logical OR. The query returns unit sales
     * by quarter and year for all food and drink related products sold in 1997,
     * run against the Sales cube in the FoodMart 2000 database.
     *
     * <p>The calculated member simply adds the values of the Unit Sales
     * measure for the Food and the Drink levels of the Product dimension
     * together. The WHERE clause is then used to restrict return of
     * information only to the calculated member, effectively implementing a
     * logical OR to return information for all time periods that contain unit
     * sales values for either food, drink, or both types of products.
     *
     * <p>You can use the Aggregate function in similar situations where all
     * measures are not aggregated by summing. To return the same results in the
     * above example using the Aggregate function, replace the definition for
     * the calculated member with this definition:
     *
     * <blockquote>
     * <code>'Aggregate({[Product].[Food], [Product].[Drink]})'</code>
     * </blockquote>
     */
    public void testLogicalOps() {
        assertQueryReturns(
            "WITH MEMBER [Product].[Food OR Drink] AS\n"
            + "  '([Product].[Food], Measures.[Unit Sales]) + ([Product].[Drink], Measures.[Unit Sales])'\n"
            + "SELECT {Measures.[Unit Sales]} ON COLUMNS,\n"
            + "  DESCENDANTS(Time.[1997], [Quarter], SELF_AND_BEFORE) ON ROWS\n"
            + "FROM Sales\n"
            + "WHERE [Product].[Food OR Drink]",

            "Axis #0:\n"
            + "{[Product].[Food OR Drink]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Time].[1997]}\n"
            + "{[Time].[1997].[Q1]}\n"
            + "{[Time].[1997].[Q2]}\n"
            + "{[Time].[1997].[Q3]}\n"
            + "{[Time].[1997].[Q4]}\n"
            + "Row #0: 216,537\n"
            + "Row #1: 53,785\n"
            + "Row #2: 50,720\n"
            + "Row #3: 53,505\n"
            + "Row #4: 58,527\n");
    }

    /**
     * <p>A logical AND, by contrast, can be supported by using two different
     * techniques. If the members used to construct the logical AND reside on
     * different dimensions, all that is required is a WHERE clause that uses
     * a tuple representing all involved members. The following MDX query uses a
     * WHERE clause that effectively restricts the query to retrieve unit
     * sales for drink products in the USA, shown by quarter and year for 1997.
     */
    public void testLogicalAnd() {
        assertQueryReturns(
            "SELECT {Measures.[Unit Sales]} ON COLUMNS,\n"
            + "  DESCENDANTS([Time].[1997], [Quarter], SELF_AND_BEFORE) ON ROWS\n"
            + "FROM Sales\n"
            + "WHERE ([Product].[Drink], [Store].USA)",

            "Axis #0:\n"
            + "{[Product].[Drink], [Store].[USA]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Time].[1997]}\n"
            + "{[Time].[1997].[Q1]}\n"
            + "{[Time].[1997].[Q2]}\n"
            + "{[Time].[1997].[Q3]}\n"
            + "{[Time].[1997].[Q4]}\n"
            + "Row #0: 24,597\n"
            + "Row #1: 5,976\n"
            + "Row #2: 5,895\n"
            + "Row #3: 6,065\n"
            + "Row #4: 6,661\n");
    }

    /**
     * <p>The WHERE clause in the previous MDX query effectively provides a
     * logical AND operator, in which all unit sales for 1997 are returned only
     * for drink products and only for those sold in stores in the USA.
     *
     * <p>If the members used to construct the logical AND condition reside on
     * the same dimension, you can use a calculated member or a named set to
     * filter out the unwanted members, as demonstrated in the following MDX
     * query.
     *
     * <p>The named set, [Good AND Pearl Stores], restricts the displayed unit
     * sales totals only to those stores that have sold both Good products and
     * Pearl products.
     */
    public void _testSet() {
        assertQueryReturns(
            "WITH SET [Good AND Pearl Stores] AS\n"
            + "  'FILTER(Store.Members,\n"
            + "          ([Product].[Good], Measures.[Unit Sales]) > 0 AND \n"
            + "          ([Product].[Pearl], Measures.[Unit Sales]) > 0)'\n"
            + "SELECT DESCENDANTS([Time].[1997], [Quarter], SELF_AND_BEFORE) ON COLUMNS,\n"
            + "  [Good AND Pearl Stores] ON ROWS\n"
            + "FROM Sales",
            "");
    }

    /**
     * <b>How Can I Use Custom Member Properties in MDX?</b>
     *
     * <p>Member properties are a good way of adding secondary business
     * information to members in a dimension. However, getting that information
     * out can be confusing -- member properties are not readily apparent in a
     * typical MDX query.
     *
     * <p>Member properties can be retrieved in one of two ways. The easiest
     * and most used method of retrieving member properties is to use the
     * DIMENSION PROPERTIES MDX statement when constructing an axis in an MDX
     * query.
     *
     * <p>For example, a member property in the Store dimension in the FoodMart
     * 2000 database details the total square feet for each store. The following
     * MDX query can retrieve this member property as part of the returned
     * cellset.
     */
    public void testCustomMemberProperties() {
        assertQueryReturns(
            "SELECT {[Measures].[Units Shipped], [Measures].[Units Ordered]} ON COLUMNS,\n"
            + "  NON EMPTY [Store].[Store Name].MEMBERS\n"
            + "    DIMENSION PROPERTIES [Store].[Store Name].[Store Sqft] ON ROWS\n"
            + "FROM Warehouse",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Units Shipped]}\n"
            + "{[Measures].[Units Ordered]}\n"
            + "Axis #2:\n"
            + "{[Store].[USA].[CA].[Beverly Hills].[Store 6]}\n"
            + "{[Store].[USA].[CA].[Los Angeles].[Store 7]}\n"
            + "{[Store].[USA].[CA].[San Diego].[Store 24]}\n"
            + "{[Store].[USA].[CA].[San Francisco].[Store 14]}\n"
            + "{[Store].[USA].[OR].[Portland].[Store 11]}\n"
            + "{[Store].[USA].[OR].[Salem].[Store 13]}\n"
            + "{[Store].[USA].[WA].[Bellingham].[Store 2]}\n"
            + "{[Store].[USA].[WA].[Bremerton].[Store 3]}\n"
            + "{[Store].[USA].[WA].[Seattle].[Store 15]}\n"
            + "{[Store].[USA].[WA].[Spokane].[Store 16]}\n"
            + "{[Store].[USA].[WA].[Tacoma].[Store 17]}\n"
            + "{[Store].[USA].[WA].[Walla Walla].[Store 22]}\n"
            + "{[Store].[USA].[WA].[Yakima].[Store 23]}\n"
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
            + "Row #12: 11426.0\n");
    }

    /**
     * <p>The drawback to using the DIMENSION PROPERTIES statement is that,
     * for most client applications, the member property is not readily
     * apparent. If the previous MDX query is executed in the MDX sample
     * application shipped with SQL Server 2000 Analysis Services, for example,
     * you must double-click the name of the member in the grid to open the
     * Member Properties dialog box, which displays all of the member properties
     * shipped as part of the cellset, including the [Store].[Store Name].[Store
     * Sqft] member property.
     *
     * <p>The other method of retrieving member properties involves the creation
     * of a calculated member based on the member property. The following MDX
     * query brings back the total square feet for each store as a measure,
     * included in the COLUMNS axis.
     *
     * <p>The [Store SqFt] measure is constructed with the Properties MDX
     * function to retrieve the [Store SQFT] member property for each member in
     * the Store dimension. The benefit to this technique is that the calculated
     * member is readily apparent and easily accessible in client applications
     * that do not support member properties.
     */
    public void _testMemberPropertyAsCalcMember() {
        assertQueryReturns(
            // todo: implement <member>.PROPERTIES
            "WITH MEMBER Measures.[Store SqFt] AS '[Store].CURRENTMEMBER.PROPERTIES(\"Store SQFT\")'\n"
            + "SELECT { [Measures].[Store SQFT], [Measures].[Units Shipped], [Measures].[Units Ordered] }  ON COLUMNS,\n"
            + "  [Store].[Store Name].MEMBERS ON ROWS\n"
            + "FROM Warehouse",
            "");
    }

    /**
     * <b>How Can I Drill Down More Than One Level Deep, or Skip Levels When
     * Drilling Down?</b>
     *
     * <p>Drilling down is an essential ability for most OLAP products, and
     * Analysis Services is no exception. Several functions exist that support
     * drilling up and down the hierarchy of dimensions within a cube.
     * Typically, drilling up and down the hierarchy is done one level at a
     * time; think of this functionality as a zoom feature for OLAP data.
     *
     * <p>There are times, though, when the need to drill down more than one
     * level at the same time, or even skip levels when displaying information
     * about multiple levels, exists for a business scenario.
     *
     * <p>For example, you would like to show report results from a query of
     * the Sales cube in the FoodMart 2000 sample database showing sales totals
     * for individual cities and the subtotals for each country, as shown in the
     * following table.
     *
     * <p>The Customers dimension, however, has Country, State Province, and
     * City levels. In order to show the above report, you would have to show
     * the Country level and then drill down two levels to show the City
     * level, skipping the State Province level entirely.
     *
     * <p>However, the MDX ToggleDrillState and DrillDownMember functions
     * provide drill down functionality only one level below a specified set. To
     * drill down more than one level below a specified set, you need to use a
     * combination of MDX functions, including Descendants, Generate, and
     * Except. This technique essentially constructs a large set that includes
     * all levels between both upper and lower desired levels, then uses a
     * smaller set representing the undesired level or levels to remove the
     * appropriate members from the larger set.
     *
     * <p>The MDX Descendants function is used to construct a set consisting of
     * the descendants of each member in the Customers dimension. The
     * descendants are determined using the MDX Descendants function, with the
     * descendants of the City level and the level above, the State Province
     * level, for each member of the Customers dimension being added to the
     * set.
     *
     * <p>The MDX Generate function now creates a set consisting of all members
     * at the Country level as well as the members of the set generated by the
     * MDX Descendants function. Then, the MDX Except function is used to
     * exclude all members at the State Province level, so the returned set
     * contains members at the Country and City levels.
     *
     * <p>Note, however, that the previous MDX query will still order the
     * members according to their hierarchy. Although the returned set contains
     * members at the Country and City levels, the Country, State Province,
     * and City levels determine the order of the members.
     */
    public void _testDrillingDownMoreThanOneLevel() {
        assertQueryReturns(
            // todo: implement "GENERATE"
            "SELECT  {[Measures].[Unit Sales]} ON COLUMNS,\n"
            + "  EXCEPT(GENERATE([Customers].[Country].MEMBERS,\n"
            + "                  {DESCENDANTS([Customers].CURRENTMEMBER, [Customers].[City], SELF_AND_BEFORE)}),\n"
            + "         {[Customers].[State Province].MEMBERS}) ON ROWS\n"
            + "FROM Sales", "");
    }

    /**
     * <b>How Do I Get the Topmost Members of a Level Broken Out by an Ancestor
     * Level?</b>
     *
     * <p>This type of MDX query is common when only the facts for the lowest
     * level of a dimension within a cube are needed, but information about
     * other levels within the same dimension may also be required to satisfy a
     * specific business scenario.
     *
     * <p>For example, a report that shows the unit sales for the store with
     * the highest unit sales from each country is needed for marketing
     * purposes. The following table provides an example of this report, run
     * against the Sales cube in the FoodMart 2000 sample database.
     *
     * <p>This looks simple enough, but the Country Name column provides
     * unexpected difficulty. The values for the Store Country column are taken
     * from the Store Country level of the Store dimension, so the Store
     * Country column is constructed as a calculated member as part of the MDX
     * query, using the MDX Ancestor and Name functions to return the country
     * names for each store.
     *
     * <p>A combination of the MDX Generate, TopCount, and Descendants
     * functions are used to create a set containing the top stores in unit
     * sales for each country.
     */
    public void _testTopmost() {
        assertQueryReturns(
            // todo: implement "GENERATE"
            "WITH MEMBER Measures.[Country Name] AS \n"
            + "  'Ancestor(Store.CurrentMember, [Store Country]).Name'\n"
            + "SELECT {Measures.[Country Name], Measures.[Unit Sales]} ON COLUMNS,\n"
            + "  GENERATE([Store Country].MEMBERS, \n"
            + "    TOPCOUNT(DESCENDANTS([Store].CURRENTMEMBER, [Store].[Store Name]),\n"
            + "      1, [Measures].[Unit Sales])) ON ROWS\n"
            + "FROM Sales",
            "");
    }

    /**
     * <p>The MDX Descendants function is used to construct a set consisting of
     * only those members at the Store Name level in the Store dimension. Then,
     * the MDX TopCount function is used to return only the topmost store based
     * on the Unit Sales measure. The MDX Generate function then constructs a
     * set based on the topmost stores, following the hierarchy of the Store
     * dimension.
     *
     * <p>Alternate techniques, such as using the MDX Crossjoin function, may
     * not provide the desired results because non-related joins can occur.
     * Since the Store Country and Store Name levels are within the same
     * dimension, they cannot be cross-joined. Another dimension that provides
     * the same regional hierarchy structure, such as the Customers dimension,
     * can be employed with the Crossjoin function. But, using this technique
     * can cause non-related joins and return unexpected results.
     *
     * <p>For example, the following MDX query uses the Crossjoin function to
     * attempt to return the same desired results.
     *
     * <p>However, some unexpected surprises occur because the topmost member
     * in the Store dimension is cross-joined with all of the children of the
     * Customers dimension, as shown in the following table.
     *
     * <p>In this instance, the use of a calculated member to provide store
     * country names is easier to understand and debug than attempting to
     * cross-join across unrelated members
     */
    public void testTopmost2() {
        assertQueryReturns(
            "SELECT {Measures.[Unit Sales]} ON COLUMNS,\n"
            + "  CROSSJOIN(Customers.CHILDREN,\n"
            + "    TOPCOUNT(DESCENDANTS([Store].CURRENTMEMBER, [Store].[Store Name]),\n"
            + "             1, [Measures].[Unit Sales])) ON ROWS\n"
            + "FROM Sales",

            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Customers].[Canada], [Store].[USA].[OR].[Salem].[Store 13]}\n"
            + "{[Customers].[Mexico], [Store].[USA].[OR].[Salem].[Store 13]}\n"
            + "{[Customers].[USA], [Store].[USA].[OR].[Salem].[Store 13]}\n"
            + "Row #0: \n"
            + "Row #1: \n"
            + "Row #2: 41,580\n");
    }

    /**
     * <b>How Can I Rank or Reorder Members?</b>
     *
     * <p>One of the issues commonly encountered in business scenarios is the
     * need to rank the members of a dimension according to their corresponding
     * measure values. The Order MDX function allows you to order a set based on
     * a string or numeric expression evaluated against the members of a set.
     * Combined with other MDX functions, the Order function can support
     * several different types of ranking.
     *
     * <p>For example, the Sales cube in the FoodMart 2000 database can be used
     * to show unit sales for each store. However, the business scenario
     * requires a report that ranks the stores from highest to lowest unit
     * sales, individually, of nonconsumable products.
     *
     * <p>Because of the requirement that stores be sorted individually, the
     * hierarchy must be broken (in other words, ignored) for the purpose of
     * ranking the stores. The Order function is capable of sorting within the
     * hierarchy, based on the topmost level represented in the set to be
     * sorted, or, by breaking the hierarchy, sorting all of the members of the
     * set as if they existed on the same level, with the same parent.
     *
     * <p>The following MDX query illustrates the use of the Order function to
     * rank the members according to unit sales.
     */
    public void testRank() {
        assertQueryReturns(
            "SELECT {[Measures].[Unit Sales]} ON COLUMNS, \n"
            + "  ORDER([Store].[Store Name].MEMBERS, (Measures.[Unit Sales]), BDESC) ON ROWS\n"
            + "FROM Sales\n"
            + "WHERE [Product].[Non-Consumable]",
            "Axis #0:\n"
            + "{[Product].[Non-Consumable]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Store].[USA].[OR].[Salem].[Store 13]}\n"
            + "{[Store].[USA].[WA].[Tacoma].[Store 17]}\n"
            + "{[Store].[USA].[OR].[Portland].[Store 11]}\n"
            + "{[Store].[USA].[CA].[Los Angeles].[Store 7]}\n"
            + "{[Store].[USA].[CA].[San Diego].[Store 24]}\n"
            + "{[Store].[USA].[WA].[Seattle].[Store 15]}\n"
            + "{[Store].[USA].[WA].[Bremerton].[Store 3]}\n"
            + "{[Store].[USA].[WA].[Spokane].[Store 16]}\n"
            + "{[Store].[USA].[CA].[Beverly Hills].[Store 6]}\n"
            + "{[Store].[USA].[WA].[Yakima].[Store 23]}\n"
            + "{[Store].[USA].[WA].[Bellingham].[Store 2]}\n"
            + "{[Store].[USA].[WA].[Walla Walla].[Store 22]}\n"
            + "{[Store].[USA].[CA].[San Francisco].[Store 14]}\n"
            + "{[Store].[Canada].[BC].[Vancouver].[Store 19]}\n"
            + "{[Store].[Canada].[BC].[Victoria].[Store 20]}\n"
            + "{[Store].[Mexico].[DF].[Mexico City].[Store 9]}\n"
            + "{[Store].[Mexico].[DF].[San Andres].[Store 21]}\n"
            + "{[Store].[Mexico].[Guerrero].[Acapulco].[Store 1]}\n"
            + "{[Store].[Mexico].[Jalisco].[Guadalajara].[Store 5]}\n"
            + "{[Store].[Mexico].[Veracruz].[Orizaba].[Store 10]}\n"
            + "{[Store].[Mexico].[Yucatan].[Merida].[Store 8]}\n"
            + "{[Store].[Mexico].[Zacatecas].[Camacho].[Store 4]}\n"
            + "{[Store].[Mexico].[Zacatecas].[Hidalgo].[Store 12]}\n"
            + "{[Store].[Mexico].[Zacatecas].[Hidalgo].[Store 18]}\n"
            + "{[Store].[USA].[CA].[Alameda].[HQ]}\n"
            + "Row #0: 7,940\n"
            + "Row #1: 6,712\n"
            + "Row #2: 5,076\n"
            + "Row #3: 4,947\n"
            + "Row #4: 4,706\n"
            + "Row #5: 4,639\n"
            + "Row #6: 4,479\n"
            + "Row #7: 4,428\n"
            + "Row #8: 3,950\n"
            + "Row #9: 2,140\n"
            + "Row #10: 442\n"
            + "Row #11: 390\n"
            + "Row #12: 387\n"
            + "Row #13: \n"
            + "Row #14: \n"
            + "Row #15: \n"
            + "Row #16: \n"
            + "Row #17: \n"
            + "Row #18: \n"
            + "Row #19: \n"
            + "Row #20: \n"
            + "Row #21: \n"
            + "Row #22: \n"
            + "Row #23: \n"
            + "Row #24: \n");
    }

    /**
     * <b>How Can I Use Different Calculations for Different Levels in a
     * Dimension?</b>
     *
     * <p>This type of MDX query frequently occurs when different aggregations
     * are needed at different levels in a dimension. One easy way to support
     * such functionality is through the use of a calculated measure, created as
     * part of the query, which uses the MDX Descendants function in conjunction
     * with one of the MDX aggregation functions to provide results.
     *
     * <p>For example, the Warehouse cube in the FoodMart 2000 database
     * supplies the [Units Ordered] measure, aggregated through the Sum
     * function. But, you would also like to see the average number of units
     * ordered per store. The following table demonstrates the desired results.
     *
     * <p>By using the following MDX query, the desired results can be
     * achieved. The calculated measure, [Average Units Ordered], supplies the
     * average number of ordered units per store by using the Avg,
     * CurrentMember, and Descendants MDX functions.
     */
    public void testDifferentCalculationsForDifferentLevels() {
        assertQueryReturns(
            "WITH MEMBER Measures.[Average Units Ordered] AS\n"
            + "  'AVG(DESCENDANTS([Store].CURRENTMEMBER, [Store].[Store Name]), [Measures].[Units Ordered])',\n"
            + "  FORMAT_STRING='#.00'\n"
            + "SELECT {[Measures].[Units ordered], Measures.[Average Units Ordered]} ON COLUMNS,\n"
            + "  [Store].[Store State].MEMBERS ON ROWS\n"
            + "FROM Warehouse",

            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Units Ordered]}\n"
            + "{[Measures].[Average Units Ordered]}\n"
            + "Axis #2:\n"
            + "{[Store].[Canada].[BC]}\n"
            + "{[Store].[Mexico].[DF]}\n"
            + "{[Store].[Mexico].[Guerrero]}\n"
            + "{[Store].[Mexico].[Jalisco]}\n"
            + "{[Store].[Mexico].[Veracruz]}\n"
            + "{[Store].[Mexico].[Yucatan]}\n"
            + "{[Store].[Mexico].[Zacatecas]}\n"
            + "{[Store].[USA].[CA]}\n"
            + "{[Store].[USA].[OR]}\n"
            + "{[Store].[USA].[WA]}\n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #3: \n"
            + "Row #3: \n"
            + "Row #4: \n"
            + "Row #4: \n"
            + "Row #5: \n"
            + "Row #5: \n"
            + "Row #6: \n"
            + "Row #6: \n"
            + "Row #7: 66307.0\n"
            + "Row #7: 16576.75\n"
            + "Row #8: 44906.0\n"
            + "Row #8: 22453.00\n"
            + "Row #9: 116025.0\n"
            + "Row #9: 16575.00\n");
    }

    /**
     * <p>This calculated measure is more powerful than it seems; if, for
     * example, you then want to see the average number of units ordered for
     * beer products in all of the stores in the California area, the following
     * MDX query can be executed with the same calculated measure.
     */
    public void testDifferentCalculations2() {
        assertQueryReturns(
            // todo: "[Store].[USA].[CA]" should be "[Store].CA",
            //  "[Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer]"
            // should be "[Product].[Beer]"
            "WITH MEMBER Measures.[Average Units Ordered] AS\n"
            + "  'AVG(DESCENDANTS([Store].CURRENTMEMBER, [Store].[Store Name]), [Measures].[Units Ordered])'\n"
            + "SELECT {[Measures].[Units ordered], Measures.[Average Units Ordered]} ON COLUMNS,\n"
            + "  [Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].CHILDREN ON ROWS\n"
            + "FROM Warehouse\n"
            + "WHERE [Store].[USA].[CA]",

            "Axis #0:\n"
            + "{[Store].[USA].[CA]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Units Ordered]}\n"
            + "{[Measures].[Average Units Ordered]}\n"
            + "Axis #2:\n"
            + "{[Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Good]}\n"
            + "{[Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Pearl]}\n"
            + "{[Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Portsmouth]}\n"
            + "{[Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Top Measure]}\n"
            + "{[Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Beer].[Walrus]}\n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #1: 151.0\n"
            + "Row #1: 75.5\n"
            + "Row #2: 95.0\n"
            + "Row #2: 95.0\n"
            + "Row #3: \n"
            + "Row #3: \n"
            + "Row #4: 211.0\n"
            + "Row #4: 105.5\n");
    }

    /**
     * <b>How Can I Use Different Calculations for Different Dimensions?</b>
     *
     * <p>Each measure in a cube uses the same aggregation function across all
     * dimensions. However, there are times where a different aggregation
     * function may be needed to represent a measure for reporting purposes. Two
     * basic cases involve aggregating a single dimension using a different
     * aggregation function than the one used for other dimensions.<ul>
     *
     * <li>Aggregating minimums, maximums, or averages along a time
     * dimension</li>
     *
     * <li>Aggregating opening and closing period values along a time
     * dimension</li></ul>
     *
     * <p>The first case involves some knowledge of the behavior of the time
     * dimension specified in the cube. For instance, to create a calculated
     * measure that contains the average, along a time dimension, of measures
     * aggregated as sums along other dimensions, the average of the aggregated
     * measures must be taken over the set of averaging time periods,
     * constructed through the use of the Descendants MDX function. Minimum and
     * maximum values are more easily calculated through the use of the Min and
     * Max MDX functions, also combined with the Descendants function.
     *
     * <p>For example, the Warehouse cube in the FoodMart 2000 database
     * contains information on ordered and shipped inventory; from it, a report
     * is requested to show the average number of units shipped, by product, to
     * each store. Information on units shipped is added on a monthly basis, so
     * the aggregated measure [Units Shipped] is divided by the count of
     * descendants, at the Month level, of the current member in the Time
     * dimension. This calculation provides a measure representing the average
     * number of units shipped per month, as demonstrated in the following MDX
     * query.
     */
    public void _testDifferentCalculationsForDifferentDimensions() {
        assertQueryReturns(
            // todo: implement "NONEMPTYCROSSJOIN"
            "WITH MEMBER [Measures].[Avg Units Shipped] AS\n"
            + "  '[Measures].[Units Shipped] / \n"
            + "    COUNT(DESCENDANTS([Time].CURRENTMEMBER, [Time].[Month], SELF))'\n"
            + "SELECT {Measures.[Units Shipped], Measures.[Avg Units Shipped]} ON COLUMNS,\n"
            + "NONEMPTYCROSSJOIN(Store.CA.Children, Product.MEMBERS) ON ROWS\n"
            + "FROM Warehouse",
            "");
    }

    /**
     * <p>The second case is easier to resolve, because MDX provides the
     * OpeningPeriod and ClosingPeriod MDX functions specifically to support
     * opening and closing period values.
     *
     * <p>For example, the Warehouse cube in the FoodMart 2000 database
     * contains information on ordered and shipped inventory; from it, a report
     * is requested to show on-hand inventory at the end of every month. Because
     * the inventory on hand should equal ordered inventory minus shipped
     * inventory, the ClosingPeriod MDX function can be used to create a
     * calculated measure to supply the value of inventory on hand, as
     * demonstrated in the following MDX query.
     */
    public void _testDifferentCalculationsForDifferentDimensions2() {
        assertQueryReturns(
            "WITH MEMBER Measures.[Closing Balance] AS\n"
            + "  '([Measures].[Units Ordered], \n"
            + "    CLOSINGPERIOD([Time].[Month], [Time].CURRENTMEMBER)) -\n"
            + "   ([Measures].[Units Shipped], \n"
            + "    CLOSINGPERIOD([Time].[Month], [Time].CURRENTMEMBER))'\n"
            + "SELECT {[Measures].[Closing Balance]} ON COLUMNS,\n"
            + "  Product.MEMBERS ON ROWS\n"
            + "FROM Warehouse",
            "");
    }

    /**
     * <b>How Can I Use Date Ranges in MDX?</b>
     *
     * <p>Date ranges are a frequently encountered problem. Business questions
     * use ranges of dates, but OLAP objects provide aggregated information in
     * date levels.
     *
     * <p>Using the technique described here, you can establish date ranges in
     * MDX queries at the level of granularity provided by a time dimension.
     * Date ranges cannot be established below the granularity of the dimension
     * without additional information. For example, if the lowest level of a
     * time dimension represents months, you will not be able to establish a
     * two-week date range without other information. Member properties can be
     * added to supply specific dates for members; using such member properties,
     * you can take advantage of the date and time functions provided by VBA and
     * Excel external function libraries to establish date ranges.
     *
     * <p>The easiest way to specify a static date range is by using the colon
     * (:) operator. This operator creates a naturally ordered set, using the
     * members specified on either side of the operator as the endpoints for the
     * ordered set. For example, to specify the first six months of 1998 from
     * the Time dimension in FoodMart 2000, the MDX syntax would resemble:
     *
     * <blockquote><pre>[Time].[1998].[1]:[Time].[1998].[6]</pre></blockquote>
     *
     * <p>For example, the Sales cube uses a time dimension that supports Year,
     * Quarter, and Month levels. To add a six-month and nine-month total, two
     * calculated members are created in the following MDX query.
     */
    public void _testDateRange() {
        assertQueryReturns(
            // todo: implement "AddCalculatedMembers"
            "WITH Member [Time].[Time].[1997].[Six Month] AS\n"
            + "  'SUM([Time].[1]:[Time].[6])'\n"
            + "Member [Time].[Time].[1997].[Nine Month] AS\n"
            + "  'SUM([Time].[1]:[Time].[9])'\n"
            + "SELECT AddCalculatedMembers([Time].[1997].Children) ON COLUMNS,\n"
            + "  [Product].Children ON ROWS\n"
            + "FROM Sales",
            "");
    }

    /**
     * <b>How Can I Use Rolling Date Ranges in MDX?</b>
     *
     * <p>There are several techniques that can be used in MDX to support
     * rolling date ranges. All of these techniques tend to fall into two
     * groups. The first group involves the use of relative hierarchical
     * functions to construct named sets or calculated members, and the second
     * group involves the use of absolute date functions from external function
     * libraries to construct named sets or calculated members. Both groups are
     * applicable in different business scenarios.
     *
     * <p>In the first group of techniques, typically a named set is
     * constructed which contains a number of periods from a time dimension. For
     * example, the following table illustrates a 12-month rolling period, in
     * which the figures for unit sales of the previous 12 months are shown.
     *
     * <p>The following MDX query accomplishes this by using a number of MDX
     * functions, including LastPeriods, Tail, Filter, Members, and Item, to
     * construct a named set containing only those members across all other
     * dimensions that share data with the time dimension at the Month level.
     * The example assumes that there is at least one measure, such as [Unit
     * Sales], with a value greater than zero in the current period. The Filter
     * function creates a set of months with unit sales greater than zero, while
     * the Tail function returns the last month in this set, the current month.
     * The LastPeriods function, finally, is then used to retrieve the last 12
     * periods at this level, including the current period.
     */
    public void _testRolling() {
        assertQueryReturns(
            "WITH SET Rolling12 AS\n"
            + "  'LASTPERIODS(12, TAIL(FILTER([Time].[Month].MEMBERS, \n"
            + "    ([Customers].[All Customers], \n"
            + "    [Education Level].[All Education Level],\n"
            + "    [Gender].[All Gender],\n"
            + "    [Marital Status].[All Marital Status],\n"
            + "    [Product].[All Products], \n"
            + "    [Promotion Media].[All Media],\n"
            + "    [Promotions].[All Promotions],\n"
            + "    [Store].[All Stores],\n"
            + "    [Store Size in SQFT].[All Store Size in SQFT],\n"
            + "    [Store Type].[All Store Type],\n"
            + "    [Yearly Income].[All Yearly Income],\n"
            + "    Measures.[Unit Sales]) >0),\n"
            + "  1).ITEM(0).ITEM(0))'\n"
            + "SELECT {[Measures].[Unit Sales]} ON COLUMNS, \n"
            + "  Rolling12 ON ROWS\n"
            + "FROM Sales",
            "");
    }

    /**
     * <b>How Can I Use Different Calculations for Different Time Periods?</b>
     *
     * <p>A few techniques can be used, depending on the structure of the cube
     * being queried, to support different calculations for members depending
     * on the time period. The following example includes the MDX IIf function,
     * and is easy to use but difficult to maintain. This example works well for
     * ad hoc queries, but is not the ideal technique for client applications in
     * a production environment.
     *
     * <p>For example, the following table illustrates a standard and dynamic
     * forecast of warehouse sales, from the Warehouse cube in the FoodMart 2000
     * database, for drink products. The standard forecast is double the
     * warehouse sales of the previous year, while the dynamic forecast varies
     * from month to month -- the forecast for January is 120 percent of
     * previous sales, while the forecast for July is 260 percent of previous
     * sales.
     *
     * <p>The most flexible way of handling this type of report is the use of
     * nested MDX IIf functions to return a multiplier to be used on the members
     * of the Products dimension, at the Drinks level. The following MDX query
     * demonstrates this technique.
     */
    public void testDifferentCalcsForDifferentTimePeriods() {
        assertQueryReturns(
            // note: "[Product].[Drink Forecast - Standard]"
            // was "[Drink Forecast - Standard]"
            "WITH MEMBER [Product].[Drink Forecast - Standard] AS\n"
            + "  '[Product].[All Products].[Drink] * 2'\n"
            + "MEMBER [Product].[Drink Forecast - Dynamic] AS \n"
            + "  '[Product].[All Products].[Drink] * \n"
            + "   IIF([Time].[Time].CurrentMember.Name = \"1\", 1.2,\n"
            + "     IIF([Time].[Time].CurrentMember.Name = \"2\", 1.3,\n"
            + "       IIF([Time].[Time].CurrentMember.Name = \"3\", 1.4,\n"
            + "         IIF([Time].[Time].CurrentMember.Name = \"4\", 1.6,\n"
            + "           IIF([Time].[Time].CurrentMember.Name = \"5\", 2.1,\n"
            + "             IIF([Time].[Time].CurrentMember.Name = \"6\", 2.4,\n"
            + "               IIF([Time].[Time].CurrentMember.Name = \"7\", 2.6,\n"
            + "                 IIF([Time].[Time].CurrentMember.Name = \"8\", 2.3,\n"
            + "                   IIF([Time].[Time].CurrentMember.Name = \"9\", 1.9,\n"
            + "                     IIF([Time].[Time].CurrentMember.Name = \"10\", 1.5,\n"
            + "                       IIF([Time].[Time].CurrentMember.Name = \"11\", 1.4,\n"
            + "                         IIF([Time].[Time].CurrentMember.Name = \"12\", 1.2, 1.0))))))))))))'\n"
            + "SELECT DESCENDANTS(Time.[1997], [Month], SELF) ON COLUMNS, \n"
            + "  {[Product].CHILDREN, [Product].[Drink Forecast - Standard], [Product].[Drink Forecast - Dynamic]} ON ROWS\n"
            + "FROM Warehouse",

            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time].[1997].[Q1].[1]}\n"
            + "{[Time].[1997].[Q1].[2]}\n"
            + "{[Time].[1997].[Q1].[3]}\n"
            + "{[Time].[1997].[Q2].[4]}\n"
            + "{[Time].[1997].[Q2].[5]}\n"
            + "{[Time].[1997].[Q2].[6]}\n"
            + "{[Time].[1997].[Q3].[7]}\n"
            + "{[Time].[1997].[Q3].[8]}\n"
            + "{[Time].[1997].[Q3].[9]}\n"
            + "{[Time].[1997].[Q4].[10]}\n"
            + "{[Time].[1997].[Q4].[11]}\n"
            + "{[Time].[1997].[Q4].[12]}\n"
            + "Axis #2:\n"
            + "{[Product].[Drink]}\n"
            + "{[Product].[Food]}\n"
            + "{[Product].[Non-Consumable]}\n"
            + "{[Product].[Drink Forecast - Standard]}\n"
            + "{[Product].[Drink Forecast - Dynamic]}\n"
            + "Row #0: 881.847\n"
            + "Row #0: 579.051\n"
            + "Row #0: 476.292\n"
            + "Row #0: 618.722\n"
            + "Row #0: 778.886\n"
            + "Row #0: 636.935\n"
            + "Row #0: 937.842\n"
            + "Row #0: 767.332\n"
            + "Row #0: 920.707\n"
            + "Row #0: 1,007.764\n"
            + "Row #0: 820.808\n"
            + "Row #0: 792.167\n"
            + "Row #1: 8,383.446\n"
            + "Row #1: 4,851.406\n"
            + "Row #1: 5,353.188\n"
            + "Row #1: 6,061.829\n"
            + "Row #1: 6,039.282\n"
            + "Row #1: 5,259.242\n"
            + "Row #1: 6,902.01\n"
            + "Row #1: 5,790.772\n"
            + "Row #1: 8,167.053\n"
            + "Row #1: 6,188.732\n"
            + "Row #1: 5,344.845\n"
            + "Row #1: 5,025.744\n"
            + "Row #2: 2,040.396\n"
            + "Row #2: 1,269.816\n"
            + "Row #2: 1,460.686\n"
            + "Row #2: 1,696.757\n"
            + "Row #2: 1,397.035\n"
            + "Row #2: 1,578.136\n"
            + "Row #2: 1,671.046\n"
            + "Row #2: 1,609.447\n"
            + "Row #2: 2,059.617\n"
            + "Row #2: 1,617.493\n"
            + "Row #2: 1,909.713\n"
            + "Row #2: 1,382.364\n"
            + "Row #3: 1,763.693\n"
            + "Row #3: 1,158.102\n"
            + "Row #3: 952.584\n"
            + "Row #3: 1,237.444\n"
            + "Row #3: 1,557.773\n"
            + "Row #3: 1,273.87\n"
            + "Row #3: 1,875.685\n"
            + "Row #3: 1,534.665\n"
            + "Row #3: 1,841.414\n"
            + "Row #3: 2,015.528\n"
            + "Row #3: 1,641.615\n"
            + "Row #3: 1,584.334\n"
            + "Row #4: 1,058.216\n"
            + "Row #4: 752.766\n"
            + "Row #4: 666.809\n"
            + "Row #4: 989.955\n"
            + "Row #4: 1,635.661\n"
            + "Row #4: 1,528.644\n"
            + "Row #4: 2,438.39\n"
            + "Row #4: 1,764.865\n"
            + "Row #4: 1,749.343\n"
            + "Row #4: 1,511.646\n"
            + "Row #4: 1,149.13\n"
            + "Row #4: 950.601\n");
    }

    /**
     * <p>Other techniques, such as the addition of member properties to the
     * Time or Product dimensions to support such calculations, are not as
     * flexible but are much more efficient. The primary drawback to using such
     * techniques is that the calculations are not easily altered for
     * speculative analysis purposes. For client applications, however, where
     * the calculations are static or slowly changing, using a member property
     * is an excellent way of supplying such functionality to clients while
     * keeping maintenance of calculation variables at the server level. The
     * same MDX query, for example, could be rewritten to use a member property
     * named [Dynamic Forecast Multiplier] as shown in the following MDX query.
     */
    public void _testDc4dtp2() {
        assertQueryReturns(
            "WITH MEMBER [Product].[Drink Forecast - Standard] AS\n"
            + "  '[Product].[All Products].[Drink] * 2'\n"
            + "MEMBER [Product].[Drink Forecast - Dynamic] AS \n"
            + "  '[Product].[All Products].[Drink] * \n"
            + "   [Time].CURRENTMEMBER.PROPERTIES(\"Dynamic Forecast Multiplier\")'\n"
            + "SELECT DESCENDANTS(Time.[1997], [Month], SELF) ON COLUMNS, \n"
            + "  {[Product].CHILDREN, [Drink Forecast - Standard], [Drink Forecast - Dynamic]} ON ROWS\n"
            + "FROM Warehouse",
            "");
    }

    public void _testWarehouseProfit() {
        assertQueryReturns(
            "select \n"
            + "{[Measures].[Warehouse Cost], [Measures].[Warehouse Sales], [Measures].[Warehouse Profit]}\n"
            + " ON COLUMNS from [Warehouse]",
            "");
    }

    /**
     * <b>How Can I Compare Time Periods in MDX?</b>
     *
     * <p>To answer such a common business question, MDX provides a number of
     * functions specifically designed to navigate and aggregate information
     * across time periods. For example, year-to-date (YTD) totals are directly
     * supported through the YTD function in MDX. In combination with the MDX
     * ParallelPeriod function, you can create calculated members to support
     * direct comparison of totals across time periods.
     *
     * <p>For example, the following table represents a comparison of YTD unit
     * sales between 1997 and 1998, run against the Sales cube in the FoodMart
     * 2000 database.
     *
     * <p>The following MDX query uses three calculated members to illustrate
     * how to use the YTD and ParallelPeriod functions in combination to compare
     * time periods.
     */
    public void _testYtdGrowth() {
        assertQueryReturns(
            // todo: implement "ParallelPeriod"
            "WITH MEMBER [Measures].[YTD Unit Sales] AS\n"
            + "  'COALESCEEMPTY(SUM(YTD(), [Measures].[Unit Sales]), 0)'\n"
            + "MEMBER [Measures].[Previous YTD Unit Sales] AS\n"
            + "  '(Measures.[YTD Unit Sales], PARALLELPERIOD([Time].[Year]))'\n"
            + "MEMBER [Measures].[YTD Growth] AS\n"
            + "  '[Measures].[YTD Unit Sales] - ([Measures].[Previous YTD Unit Sales])'\n"
            + "SELECT {[Time].[1998]} ON COLUMNS,\n"
            + "  {[Measures].[YTD Unit Sales], [Measures].[Previous YTD Unit Sales], [Measures].[YTD Growth]} ON ROWS\n"
            + "FROM Sales ", "");
    }


    /**
     * Disabled; takes a quite long time.
     */
    public void dont_testParallelMutliple() {
        for (int i = 0; i < 5; i++) {
            runParallelQueries(1, 1, false);
            runParallelQueries(3, 2, false);
            runParallelQueries(4, 6, true);
            runParallelQueries(6, 10, false);
        }
    }

    public void dont_testParallelNot() {
        runParallelQueries(1, 1, false);
    }

    public void dont_testParallelSomewhat() {
        runParallelQueries(3, 2, false);
    }

    public void dont_testParallelFlushCache() {
        runParallelQueries(4, 6, true);
    }

    public void dont_testParallelVery() {
        runParallelQueries(6, 10, false);
    }

    private void runParallelQueries(
        final int threadCount,
        final int iterationCount,
        final boolean flush)
    {
        // 10 minute per query
        long timeoutMs = (long) threadCount * iterationCount * 600 * 1000;
        final int[] executeCount = new int[] {0};
        final List<QueryAndResult> queries = new ArrayList<QueryAndResult>();
        queries.addAll(Arrays.asList(sampleQueries));
        queries.addAll(taglibQueries);
        TestCaseForker threaded =
            new TestCaseForker(
                this, timeoutMs, threadCount,
                new ChooseRunnable() {
                    public void run(int i) {
                        for (int j = 0; j < iterationCount; j++) {
                            int queryIndex = (i * 2 + j) % queries.size();
                            try {
                                QueryAndResult query = queries.get(queryIndex);
                                assertQueryReturns(query.query, query.result);
                                if (flush && i == 0) {
                                    TestContext.instance().flushSchemaCache();
                                }
                                synchronized (executeCount) {
                                    executeCount[0]++;
                                }
                            } catch (Throwable e) {
                                e.printStackTrace();
                                throw Util.newInternal(
                                    e,
                                    "Thread #"
                                    + i
                                    + " failed while executing query #"
                                    + queryIndex);
                            }
                        }
                    }
                });
        threaded.run();
        assertEquals(
            "number of executions",
            threadCount * iterationCount,
            executeCount[0]);
    }

    /**
     * Makes sure that the expression <code>
     *
     * [Measures].[Unit Sales] / ([Measures].[Unit Sales], [Product].[All
     * Products])
     *
     * </code> depends on the current member of the Product dimension, although
     * [Product].[All Products] is referenced from the expression.
     */
    public void testDependsOn() {
        assertQueryReturns(
            "with member [Customers].[my] as \n"
            + "  'Aggregate(Filter([Customers].[City].Members, (([Measures].[Unit Sales] / ([Measures].[Unit Sales], [Product].[All Products])) > 0.1)))' \n"
            + "select  \n"
            + "  {[Measures].[Unit Sales]} ON columns, \n"
            + "  {[Product].[All Products].[Food].[Deli], [Product].[All Products].[Food].[Frozen Foods]} ON rows \n"
            + "from [Sales] \n"
            + "where ([Customers].[my], [Time].[1997])\n",

            "Axis #0:\n"
            + "{[Customers].[my], [Time].[1997]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Food].[Deli]}\n"
            + "{[Product].[Food].[Frozen Foods]}\n"
            + "Row #0: 13\n"
            + "Row #1: 15,111\n");
    }

    /**
     * Testcase for bug 1755778, "CrossJoin / Filter query returns null row in
     * result set"
     *
     * @throws Exception on error
     */
    public void testFilterWithCrossJoin() throws Exception {
        String queryWithFilter =
            "WITH SET [#DataSet#] AS 'Filter(Crossjoin({[Store].[All Stores]}, {[Customers].[All Customers]}), "
            + "[Measures].[Unit Sales] > 5)' "
            + "MEMBER [Customers].[#GT#] as 'Aggregate({[#DataSet#]})' "
            + "MEMBER [Store].[#GT#] as 'Aggregate({[#DataSet#]})' "
            + "SET [#GrandTotalSet#] as 'Crossjoin({[Store].[#GT#]}, {[Customers].[#GT#]})' "
            + "SELECT {[Measures].[Unit Sales]} "
            + "on columns, Union([#GrandTotalSet#], Hierarchize({[#DataSet#]})) on rows FROM [Sales]";

        String queryWithoutFilter =
            "WITH SET [#DataSet#] AS 'Crossjoin({[Store].[All Stores]}, {[Customers].[All Customers]})' "
            + "SET [#GrandTotalSet#] as 'Crossjoin({[Store].[All Stores]}, {[Customers].[All Customers]})' "
            + "SELECT {[Measures].[Unit Sales]} on columns, Union([#GrandTotalSet#], Hierarchize({[#DataSet#]})) "
            + "on rows FROM [Sales]";


        String wrongResultWithFilter =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Store].[#GT#], [Customers].[#GT#]}\n"
            + "Row #0: \n";

        String expectedResultWithFilter =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Store].[#GT#], [Customers].[#GT#]}\n"
            + "{[Store].[All Stores], [Customers].[All Customers]}\n"
            + "Row #0: 266,773\n"
            + "Row #1: 266,773\n";

        String expectedResultWithoutFilter =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Store].[All Stores], [Customers].[All Customers]}\n"
            + "Row #0: 266,773\n";

        // With bug 1755778, the following test below fails because it returns
        // only row that have a null value (see "wrongResultWithFilter").
        // It should return the "expectedResultWithFilter" value.
        assertQueryReturns(queryWithFilter, expectedResultWithFilter);

        // To see the test case return the correct result comment out the line
        // above and uncomment out the lines below following. If a similar
        // query without the filter is executed (queryWithoutFilter) prior to
        // running the query with the filter then the correct result set is
        // returned
        assertQueryReturns(
            queryWithoutFilter, expectedResultWithoutFilter);
        assertQueryReturns(
            queryWithFilter, expectedResultWithFilter);
    }

    /**
     * This resulted in {@link OutOfMemoryError} when the
     * BatchingCellReader did not know the values for the tuples that
     * were used in filters.
     */
    public void testFilteredCrossJoin() {
        TestContext.instance().flushSchemaCache();
        Result result = executeQuery(
            "select {[Measures].[Store Sales]} on columns,\n"
            + "  NON EMPTY Crossjoin(\n"
            + "    Filter([Customers].[Name].Members,\n"
            + "      (([Measures].[Store Sales],\n"
            + "        [Store].[All Stores].[USA].[CA].[San Francisco].[Store 14],\n"
            + "        [Time].[1997].[Q1].[1]) > 5.0)),\n"
            + "    Filter([Product].[Product Name].Members,\n"
            + "      (([Measures].[Store Sales],\n"
            + "        [Store].[All Stores].[USA].[CA].[San Francisco].[Store 14],\n"
            + "        [Time].[1997].[Q1].[1]) > 5.0))\n"
            + " ) ON rows\n"
            + "from [Sales]\n"
            + "where (\n"
            + "  [Store].[All Stores].[USA].[CA].[San Francisco].[Store 14],\n"
            + "  [Time].[1997].[Q1].[1]\n"
            + ")\n");
        // ok if no OutOfMemoryError occurs
        Axis a = result.getAxes()[1];
        assertEquals(12, a.getPositions().size());
    }

    /**
     * Tests a query with a CrossJoin so large that we run out of memory unless
     * we can push down evaluation to SQL.
     */
    public void testNonEmptyCrossJoin() {
        if (!props.EnableNativeCrossJoin.get()) {
            // If we try to evaluate the crossjoin in memory we run out of
            // memory.
            return;
        }
        TestContext.instance().flushSchemaCache();
        Result result = executeQuery(
            "select {[Measures].[Store Sales]} on columns,\n"
            + "  NON EMPTY Crossjoin(\n"
            + "    [Customers].[Name].Members,\n"
            + "    [Product].[Product Name].Members\n"
            + " ) ON rows\n"
            + "from [Sales]\n"
            + "where (\n"
            + "  [Store].[All Stores].[USA].[CA].[San Francisco].[Store 14],\n"
            + "  [Time].[1997].[Q1].[1]\n"
            + ")\n");
        // ok if no OutOfMemoryError occurs
        Axis a = result.getAxes()[1];
        assertEquals(67, a.getPositions().size());
    }

    /**
     * NonEmptyCrossJoin() is not the same as NON EMPTY CrossJoin()
     * because it's evaluated independently of the other axes.
     * (see http://blogs.msdn.com/bi_systems/articles/162841.aspx)
     */
    public void testNonEmptyNonEmptyCrossJoin1() {
        TestContext.instance().flushSchemaCache();
        Result result = executeQuery(
            "select {[Education Level].[All Education Levels].[Graduate Degree]} on columns,\n"
            + "   CrossJoin(\n"
            + "      {[Store Type].[Store Type].members},\n"
            + "      {[Promotions].[Promotion Name].members})\n"
            + "   on rows\n"
            + "from Sales\n"
            + "where ([Customers].[All Customers].[USA].[WA].[Anacortes])\n");
        Axis a = result.getAxes()[1];
        assertEquals(306, a.getPositions().size());
    }

    public void testNonEmptyNonEmptyCrossJoin2() {
        TestContext.instance().flushSchemaCache();
        Result result = executeQuery(
            "select {[Education Level].[All Education Levels].[Graduate Degree]} on columns,\n"
            + "   NonEmptyCrossJoin(\n"
            + "      {[Store Type].[Store Type].members},\n"
            + "      {[Promotions].[Promotion Name].members})\n"
            + "   on rows\n"
            + "from Sales\n"
            + "where ([Customers].[All Customers].[USA].[WA].[Anacortes])\n");
        Axis a = result.getAxes()[1];
        assertEquals(10, a.getPositions().size());
    }

    public void testNonEmptyNonEmptyCrossJoin3() {
        TestContext.instance().flushSchemaCache();
        Result result = executeQuery(
            "select {[Education Level].[All Education Levels].[Graduate Degree]} on columns,\n"
            + "   Non Empty CrossJoin(\n"
            + "      {[Store Type].[Store Type].members},\n"
            + "      {[Promotions].[Promotion Name].members})\n"
            + "   on rows\n"
            + "from Sales\n"
            + "where ([Customers].[All Customers].[USA].[WA].[Anacortes])\n");
        Axis a = result.getAxes()[1];
        assertEquals(1, a.getPositions().size());
    }

    public void testNonEmptyNonEmptyCrossJoin4() {
        TestContext.instance().flushSchemaCache();
        Result result = executeQuery(
            "select {[Education Level].[All Education Levels].[Graduate Degree]} on columns,\n"
            + "   Non Empty NonEmptyCrossJoin(\n"
            + "      {[Store Type].[Store Type].members},\n"
            + "      {[Promotions].[Promotion Name].members})\n"
            + "   on rows\n"
            + "from Sales\n"
            + "where ([Customers].[All Customers].[USA].[WA].[Anacortes])\n");
        Axis a = result.getAxes()[1];
        assertEquals(1, a.getPositions().size());
    }

    /**
     * description of this testcase:
     * A calculated member is created on the time.month level.
     * On Hierarchize this member is compared to a month.
     * The month has a numeric key, while the calculated members
     * key type is string.
     * No exeception must be thrown.
     */
    public void testHierDifferentKeyClass() {
        Result result = executeQuery(
            "with member [Time].[Time].[1997].[Q1].[xxx] as\n"
            + "'Aggregate({[Time].[1997].[Q1].[1], [Time].[1997].[Q1].[2]})'\n"
            + "select {[Measures].[Unit Sales], [Measures].[Store Cost],\n"
            + "[Measures].[Store Sales]} ON columns,\n"
            + "Hierarchize(Union(Union({[Time].[1997], [Time].[1998],\n"
            + "[Time].[1997].[Q1].[xxx]}, [Time].[1997].Children),\n"
            + "[Time].[1997].[Q1].Children)) ON rows from [Sales]");
        Axis a = result.getAxes()[1];
        assertEquals(10, a.getPositions().size());
    }


    /**
     * Bug #1005995 - many totals of various dimensions
     */
    public void testOverlappingCalculatedMembers() {
        assertQueryReturns(
            "WITH MEMBER [Store].[Total] AS 'SUM([Store].[Store Country].MEMBERS)' "
            + "MEMBER [Store Type].[Total] AS 'SUM([Store Type].[Store Type].MEMBERS)' "
            + "MEMBER [Gender].[Total] AS 'SUM([Gender].[Gender].MEMBERS)' "
            + "MEMBER [Measures].[x] AS '[Measures].[Store Sales]' "
            + "SELECT {[Measures].[x]} ON COLUMNS , "
            + "{ ([Store].[Total], [Store Type].[Total], [Gender].[Total]) } ON ROWS "
            + "FROM Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[x]}\n"
            + "Axis #2:\n"
            + "{[Store].[Total], [Store Type].[Total], [Gender].[Total]}\n"
            + "Row #0: 565,238.13\n");
    }

    /**
     * the following query raised a classcast exception because
     * an empty property evaluated as "NullMember"
     * note: Store "HQ" does not have a "Store Manager"
     */
    public void testEmptyProperty() {
        assertQueryReturns(
            "select     {[Measures].[Unit Sales]} on columns, "
            + "filter([Store].[Store Name].members,"
            + "[Store].currentmember.properties(\"Store Manager\")=\"Smith\") on rows"
            + " from Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Store].[USA].[WA].[Bellingham].[Store 2]}\n"
            + "Row #0: 2,237\n");
    }

    /**
     * This test modifies the Sales cube to contain both the regular usage
     * of the [Store] shared dimension, and another usage called [Other Store]
     * which is connected to the [Unit Sales] column
     */
    public void _testCubeWhichUsesSameSharedDimTwice() {
        // Create a second usage of the "Store" shared dimension called "Other
        // Store". Attach it to the "unit_sales" column (which has values [1,
        // 6] whereas store has values [1, 24].
        TestContext testContext = TestContext.instance().createSubstitutingCube(
            "Sales",
            "<DimensionUsage name=\"Other Store\" source=\"Store\" foreignKey=\"unit_sales\" />");
        Axis axis = testContext.executeAxis("[Other Store].members");
        assertEquals(63, axis.getPositions().size());

        axis = testContext.executeAxis("[Store].members");
        assertEquals(63, axis.getPositions().size());

        final String q1 =
            "select {[Measures].[Unit Sales]} on columns,\n"
            + " NON EMPTY {[Other Store].members} on rows\n"
            + "from [Sales]";
        testContext.assertQueryReturns(
            q1,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Other Store].[All Other Stores]}\n"
            + "{[Other Store].[Mexico]}\n"
            + "{[Other Store].[USA]}\n"
            + "{[Other Store].[Mexico].[Guerrero]}\n"
            + "{[Other Store].[Mexico].[Jalisco]}\n"
            + "{[Other Store].[Mexico].[Zacatecas]}\n"
            + "{[Other Store].[USA].[CA]}\n"
            + "{[Other Store].[USA].[WA]}\n"
            + "{[Other Store].[Mexico].[Guerrero].[Acapulco]}\n"
            + "{[Other Store].[Mexico].[Jalisco].[Guadalajara]}\n"
            + "{[Other Store].[Mexico].[Zacatecas].[Camacho]}\n"
            + "{[Other Store].[USA].[CA].[Beverly Hills]}\n"
            + "{[Other Store].[USA].[WA].[Bellingham]}\n"
            + "{[Other Store].[USA].[WA].[Bremerton]}\n"
            + "{[Other Store].[Mexico].[Guerrero].[Acapulco].[Store 1]}\n"
            + "{[Other Store].[Mexico].[Jalisco].[Guadalajara].[Store 5]}\n"
            + "{[Other Store].[Mexico].[Zacatecas].[Camacho].[Store 4]}\n"
            + "{[Other Store].[USA].[CA].[Beverly Hills].[Store 6]}\n"
            + "{[Other Store].[USA].[WA].[Bellingham].[Store 2]}\n"
            + "{[Other Store].[USA].[WA].[Bremerton].[Store 3]}\n"
            + "Row #0: 266,773\n"
            + "Row #1: 110,822\n"
            + "Row #2: 155,951\n"
            + "Row #3: 1,827\n"
            + "Row #4: 14,915\n"
            + "Row #5: 94,080\n"
            + "Row #6: 222\n"
            + "Row #7: 155,729\n"
            + "Row #8: 1,827\n"
            + "Row #9: 14,915\n"
            + "Row #10: 94,080\n"
            + "Row #11: 222\n"
            + "Row #12: 39,362\n"
            + "Row #13: 116,367\n"
            + "Row #14: 1,827\n"
            + "Row #15: 14,915\n"
            + "Row #16: 94,080\n"
            + "Row #17: 222\n"
            + "Row #18: 39,362\n"
            + "Row #19: 116,367\n");

        final String q2 =
            "select {[Measures].[Unit Sales]} on columns,\n"
            + " CrossJoin(\n"
            + "  {[Store].[USA], [Store].[USA].[CA], [Store].[USA].[OR].[Portland]}, \n"
            + "  {[Other Store].[USA], [Other Store].[USA].[CA], [Other Store].[USA].[OR].[Portland]}) on rows\n"
            + "from [Sales]";
        testContext.assertQueryReturns(
            q2,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Store].[USA], [Other Store].[USA]}\n"
            + "{[Store].[USA], [Other Store].[USA].[CA]}\n"
            + "{[Store].[USA], [Other Store].[USA].[OR].[Portland]}\n"
            + "{[Store].[USA].[CA], [Other Store].[USA]}\n"
            + "{[Store].[USA].[CA], [Other Store].[USA].[CA]}\n"
            + "{[Store].[USA].[CA], [Other Store].[USA].[OR].[Portland]}\n"
            + "{[Store].[USA].[OR].[Portland], [Other Store].[USA]}\n"
            + "{[Store].[USA].[OR].[Portland], [Other Store].[USA].[CA]}\n"
            + "{[Store].[USA].[OR].[Portland], [Other Store].[USA].[OR].[Portland]}\n"
            + "Row #0: 155,951\n"
            + "Row #1: 222\n"
            + "Row #2: \n"
            + "Row #3: 43,730\n"
            + "Row #4: 66\n"
            + "Row #5: \n"
            + "Row #6: 15,134\n"
            + "Row #7: 24\n"
            + "Row #8: \n");

        Result result = executeQuery(q2);
        final Cell cell = result.getCell(new int[] {0, 0});
        String sql = cell.getDrillThroughSQL(false);
        // the following replacement is for databases in ANSI mode
        //  using '"' to quote identifiers
        sql = sql.replace('"', '`');

        String tableQualifier = "as ";
        final Dialect dialect = getTestContext().getDialect();
        if (dialect.getDatabaseProduct() == Dialect.DatabaseProduct.ORACLE) {
            // " + tableQualifier + "
            tableQualifier = "";
        }
        assertEquals(
            "select `store`.`store_country` as `Store Country`,"
            + " `time_by_day`.`the_year` as `Year`,"
            + " `store_1`.`store_country` as `x0`,"
            + " `sales_fact_1997`.`unit_sales` as `Unit Sales` "
            + "from `store` " + tableQualifier + "`store`,"
            + " `sales_fact_1997` " + tableQualifier + "`sales_fact_1997`,"
            + " `time_by_day` " + tableQualifier + "`time_by_day`,"
            + " `store` " + tableQualifier + "`store_1` "
            + "where `sales_fact_1997`.`store_id` = `store`.`store_id`"
            + " and `store`.`store_country` = 'USA'"
            + " and `sales_fact_1997`.`time_id` = `time_by_day`.`time_id`"
            + " and `time_by_day`.`the_year` = 1997"
            + " and `sales_fact_1997`.`unit_sales` = `store_1`.`store_id`"
            + " and `store_1`.`store_country` = 'USA'",
            sql);
    }

    public void testMemberVisibility() {
        String cubeName = "Sales_MemberVis";
        final TestContext testContext = TestContext.instance().create(
            null,
            "<Cube name=\""
            + cubeName
            + "\">\n"
            + "  <Table name=\"sales_fact_1997\"/>\n"
            + "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\"\n"
            + "      formatString=\"Standard\" visible=\"false\"/>\n"
            + "  <Measure name=\"Store Cost\" column=\"store_cost\" aggregator=\"sum\"\n"
            + "      formatString=\"#,###.00\"/>\n"
            + "  <Measure name=\"Store Sales\" column=\"store_sales\" aggregator=\"sum\"\n"
            + "      formatString=\"#,###.00\"/>\n"
            + "  <Measure name=\"Sales Count\" column=\"product_id\" aggregator=\"count\"\n"
            + "      formatString=\"#,###\"/>\n"
            + "  <Measure name=\"Customer Count\" column=\"customer_id\"\n"
            + "      aggregator=\"distinct-count\" formatString=\"#,###\"/>\n"
            + "  <CalculatedMember\n"
            + "      name=\"Profit\"\n"
            + "      dimension=\"Measures\"\n"
            + "      visible=\"false\"\n"
            + "      formula=\"[Measures].[Store Sales]-[Measures].[Store Cost]\">\n"
            + "    <CalculatedMemberProperty name=\"FORMAT_STRING\" value=\"$#,##0.00\"/>\n"
            + "  </CalculatedMember>\n"
            + "</Cube>", null, null, null, null);
        SchemaReader scr = testContext.getConnection().getSchema().lookupCube(
            cubeName, true).getSchemaReader(null);
        Member member = scr.getMemberByUniqueName(
            Id.Segment.toList(
                "Measures", "Unit Sales"), true);
        Object visible = member.getPropertyValue(Property.VISIBLE.name);
        assertEquals(Boolean.FALSE, visible);

        member = scr.getMemberByUniqueName(
            Id.Segment.toList(
                "Measures", "Store Cost"), true);
        visible = member.getPropertyValue(Property.VISIBLE.name);
        assertEquals(Boolean.TRUE, visible);

        member = scr.getMemberByUniqueName(
            Id.Segment.toList(
                "Measures", "Profit"), true);
        visible = member.getPropertyValue(Property.VISIBLE.name);
        assertEquals(Boolean.FALSE, visible);
    }

    public void testAllMemberCaption() {
        TestContext testContext = TestContext.instance()
        .createSubstitutingCube(
            "Sales",
            "<Dimension name=\"Gender3\" foreignKey=\"customer_id\">\n"
            + "  <Hierarchy hasAll=\"true\" allMemberName=\"All Gender\"\n"
            + " allMemberCaption=\"Frauen und Maenner\" primaryKey=\"customer_id\">\n"
            + "  <Table name=\"customer\"/>\n"
            + "    <Level name=\"Gender\" column=\"gender\" uniqueMembers=\"true\"/>\n"
            + "  </Hierarchy>\n"
            + "</Dimension>");
        String mdx = "select {[Gender3].[All Gender]} on columns from Sales";
        Result result = testContext.executeQuery(mdx);
        Axis axis0 = result.getAxes()[0];
        Position pos0 = axis0.getPositions().get(0);
        Member allGender = pos0.get(0);
        String caption = allGender.getCaption();
        Assert.assertEquals(caption, "Frauen und Maenner");
    }

    public void testAllLevelName() {
        TestContext testContext = TestContext.instance()
        .createSubstitutingCube(
            "Sales",
            "<Dimension name=\"Gender4\" foreignKey=\"customer_id\">\n"
            + "  <Hierarchy hasAll=\"true\" allMemberName=\"All Gender\"\n"
            + " allLevelName=\"GenderLevel\" primaryKey=\"customer_id\">\n"
            + "  <Table name=\"customer\"/>\n"
            + "    <Level name=\"Gender\" column=\"gender\" uniqueMembers=\"true\"/>\n"
            + "  </Hierarchy>\n"
            + "</Dimension>");
        String mdx = "select {[Gender4].[All Gender]} on columns from Sales";
        Result result = testContext.executeQuery(mdx);
        Axis axis0 = result.getAxes()[0];
        Position pos0 = axis0.getPositions().get(0);
        Member allGender = pos0.get(0);
        String caption = allGender.getLevel().getName();
        Assert.assertEquals(caption, "GenderLevel");
    }

    /**
     * Bug 1250080 caused a dimension with no 'all' member to be constrained
     * twice.
     */
    public void testDimWithoutAll() {
        // Create a test context with a new ""Sales_DimWithoutAll" cube, and
        // which evaluates expressions against that cube.
        final String schema = TestContext.instance().getSchema(
            null,
            "<Cube name=\"Sales_DimWithoutAll\">\n"
            + "  <Table name=\"sales_fact_1997\"/>\n"
            + "  <Dimension name=\"Product\" foreignKey=\"product_id\">\n"
            + "    <Hierarchy hasAll=\"false\" primaryKey=\"product_id\" "
            + "primaryKeyTable=\"product\">\n"
            + "      <Join leftKey=\"product_class_id\" "
            + "rightKey=\"product_class_id\">\n"
            + "        <Table name=\"product\"/>\n"
            + "        <Table name=\"product_class\"/>\n"
            + "      </Join>\n"
            + "      <Level name=\"Product Family\" table=\"product_class\" "
            + "column=\"product_family\"\n"
            + "          uniqueMembers=\"true\"/>\n"
            + "      <Level name=\"Product Department\" "
            + "table=\"product_class\" column=\"product_department\"\n"
            + "          uniqueMembers=\"false\"/>\n"
            + "      <Level name=\"Product Category\" table=\"product_class\""
            + " column=\"product_category\"\n"
            + "          uniqueMembers=\"false\"/>\n"
            + "      <Level name=\"Product Subcategory\" "
            + "table=\"product_class\" column=\"product_subcategory\"\n"
            + "          uniqueMembers=\"false\"/>\n"
            + "      <Level name=\"Brand Name\" table=\"product\" "
            + "column=\"brand_name\" uniqueMembers=\"false\"/>\n"
            + "      <Level name=\"Product Name\" table=\"product\" "
            + "column=\"product_name\"\n"
            + "          uniqueMembers=\"true\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Dimension name=\"Gender\" foreignKey=\"customer_id\">\n"
            + "    <Hierarchy hasAll=\"false\" primaryKey=\"customer_id\">\n"
            + "    <Table name=\"customer\"/>\n"
            + "      <Level name=\"Gender\" column=\"gender\" "
            + "uniqueMembers=\"true\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>"
            + "  <Measure name=\"Unit Sales\" column=\"unit_sales\" "
            + "aggregator=\"sum\"\n"
            + "      formatString=\"Standard\" visible=\"false\"/>\n"
            + "  <Measure name=\"Store Cost\" column=\"store_cost\" aggregator=\"sum\"\n"
            + "      formatString=\"#,###.00\"/>\n"
            + "</Cube>",
            null,
            null,
            null,
            null);
        TestContext testContext =
            TestContext.instance()
                .withSchema(schema)
                .withCube("Sales_DimWithoutAll");
        // the default member of the Gender dimension is the first member
        testContext.assertExprReturns("[Gender].CurrentMember.Name", "F");
        testContext.assertExprReturns("[Product].CurrentMember.Name", "Drink");
        // There is no all member.
        testContext.assertExprThrows(
            "([Gender].[All Gender], [Measures].[Unit Sales])",
            "MDX object '[Gender].[All Gender]' not found in cube 'Sales_DimWithoutAll'");
        testContext.assertExprThrows(
            "([Gender].[All Genders], [Measures].[Unit Sales])",
            "MDX object '[Gender].[All Genders]' not found in cube 'Sales_DimWithoutAll'");
        // evaluated in the default context: [Product].[Drink], [Gender].[F]
        testContext.assertExprReturns("[Measures].[Unit Sales]", "12,202");
        // evaluated in the same context: [Product].[Drink], [Gender].[F]
        testContext.assertExprReturns(
            "([Gender].[F], [Measures].[Unit Sales])", "12,202");
        // evaluated at in the context: [Product].[Drink], [Gender].[M]
        testContext.assertExprReturns(
            "([Gender].[M], [Measures].[Unit Sales])", "12,395");
        // evaluated in the context:
        // [Product].[Food].[Canned Foods], [Gender].[F]
        testContext.assertExprReturns(
            "([Product].[Food].[Canned Foods], [Measures].[Unit Sales])",
            "9,407");
        testContext.assertExprReturns(
            "([Product].[Food].[Dairy], [Measures].[Unit Sales])", "6,513");
        testContext.assertExprReturns(
            "([Product].[Drink].[Dairy], [Measures].[Unit Sales])", "1,987");
    }

    /**
     * If an axis expression is a member, implicitly convert it to a set.
     */
    public void testMemberOnAxis() {
        assertQueryReturns(
            "select [Measures].[Sales Count] on 0, non empty [Store].[Store State].members on 1 from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Sales Count]}\n"
            + "Axis #2:\n"
            + "{[Store].[USA].[CA]}\n"
            + "{[Store].[USA].[OR]}\n"
            + "{[Store].[USA].[WA]}\n"
            + "Row #0: 24,442\n"
            + "Row #1: 21,611\n"
            + "Row #2: 40,784\n");
    }

    public void testScalarOnAxisFails() {
        assertQueryThrows(
            "select [Measures].[Sales Count] + 1 on 0, non empty [Store].[Store State].members on 1 from [Sales]",
            "Axis 'COLUMNS' expression is not a set");
    }

    /**
     * It is illegal for a query to have the same dimension on more than
     * one axis.
     */
    public void testSameDimOnTwoAxesFails() {
        assertQueryThrows(
            "select {[Measures].[Unit Sales]} on columns,\n"
            + " {[Measures].[Store Sales]} on rows\n"
            + "from [Sales]",
            "Hierarchy '[Measures]' appears in more than one independent axis");

        // as part of a crossjoin
        assertQueryThrows(
            "select {[Measures].[Unit Sales]} on columns,\n"
            + " CrossJoin({[Product].members},"
            + "           {[Measures].[Store Sales]}) on rows\n"
            + "from [Sales]",
            "Hierarchy '[Measures]' appears in more than one independent axis");

        // as part of a tuple
        assertQueryThrows(
            "select CrossJoin(\n"
            + "    {[Product].children},\n"
            + "    {[Measures].[Unit Sales]}) on columns,\n"
            + "    {([Product],\n"
            + "      [Store].CurrentMember)} on rows\n"
            + "from [Sales]",
            "Hierarchy '[Product]' appears in more than one independent axis");

        // clash between columns and slicer
        assertQueryThrows(
            "select {[Measures].[Unit Sales]} on columns,\n"
            + " {[Store].Members} on rows\n"
            + "from [Sales]\n"
            + "where ([Time].[1997].[Q1], [Measures].[Store Sales])",
            "Hierarchy '[Measures]' appears in more than one independent axis");

        // within aggregate is OK
        executeQuery(
            "with member [Measures].[West Coast Total] as "
            + " ' Aggregate({[Store].[USA].[CA], [Store].[USA].[OR], [Store].[USA].[WA]}) ' \n"
            + "select "
            + "   {[Measures].[Store Sales], \n"
            + "    [Measures].[Unit Sales]} on Columns,\n"
            + " CrossJoin(\n"
            + "   {[Product].children},\n"
            + "   {[Store].children}) on Rows\n"
            + "from [Sales]");
    }

    public void _testSetArgToTupleFails() {
        assertQueryThrows(
            "select CrossJoin(\n"
            + "    {[Product].children},\n"
            + "    {[Measures].[Unit Sales]}) on columns,\n"
            + "    {([Product],\n"
            + "      [Store].members)} on rows\n"
            + "from [Sales]",
            "Dimension '[Product]' appears in more than one independent axis");
    }

    public void _badArgsToTupleFails() {
        // clash within slicer
        assertQueryThrows(
            "select {[Measures].[Unit Sales]} on columns,\n"
            + " {[Store].Members} on rows\n"
            + "from [Sales]\n"
            + "where ([Time].[1997].[Q1], [Product], [Time].[1997].[Q2])",
            "Dimension '[Time]' more than once in same tuple");

        // ditto
        assertQueryThrows(
            "select {[Measures].[Unit Sales]} on columns,\n"
            + " CrossJoin({[Time].[1997].[Q1],\n"
            + "           {[Product]},\n"
            + "           {[Time].[1997].[Q2]}) on rows\n"
            + "from [Sales]",
            "Dimension '[Time]' more than once in same tuple");
    }

    public void testNullMember() {
        if (isDefaultNullMemberRepresentation()) {
            assertQueryReturns(
                "SELECT \n"
                + "{[Measures].[Store Cost]} ON columns, \n"
                + "{[Store Size in SQFT].[All Store Size in SQFTs].[#null]} ON rows \n"
                + "FROM [Sales] \n"
                + "WHERE [Time].[1997]",
                "Axis #0:\n"
                + "{[Time].[1997]}\n"
                + "Axis #1:\n"
                + "{[Measures].[Store Cost]}\n"
                + "Axis #2:\n"
                + "{[Store Size in SQFT].[#null]}\n"
                + "Row #0: 33,307.69\n");
        }
    }

    public void testNullMemberWithOneNonNull() {
        if (isDefaultNullMemberRepresentation()) {
            assertQueryReturns(
                "SELECT \n"
                + "{[Measures].[Store Cost]} ON columns, \n"
                + "{[Store Size in SQFT].[All Store Size in SQFTs].[#null],"
                + "[Store Size in SQFT].[ALL Store Size in SQFTs].[39696]} ON rows \n"
                + "FROM [Sales] \n"
                + "WHERE [Time].[1997]",
                "Axis #0:\n"
                + "{[Time].[1997]}\n"
                + "Axis #1:\n"
                + "{[Measures].[Store Cost]}\n"
                + "Axis #2:\n"
                + "{[Store Size in SQFT].[#null]}\n"
                + "{[Store Size in SQFT].[39696]}\n"
                + "Row #0: 33,307.69\n"
                + "Row #1: 21,121.96\n");
        }
    }

    /**
     * Tests whether the agg mgr behaves correctly if a cell request causes
     * a column to be constrained multiple times. This happens if two levels
     * map to the same column via the same join-path. If the constraints are
     * inconsistent, no data will be returned.
     */
    public void testMultipleConstraintsOnSameColumn() {
        final String cubeName = "Sales_withCities";
        final TestContext testContext = TestContext.instance().create(
            null,
            "<Cube name=\"" + cubeName + "\">\n"
            + "  <Table name=\"sales_fact_1997\"/>\n"
            + "  <DimensionUsage name=\"Time\" source=\"Time\" foreignKey=\"time_id\"/>\n"
            + "  <Dimension name=\"Cities\" foreignKey=\"customer_id\">\n"
            + "    <Hierarchy hasAll=\"true\" allMemberName=\"All Cities\" primaryKey=\"customer_id\">\n"
            + "      <Table name=\"customer\"/>\n"
            + "      <Level name=\"City\" column=\"city\" uniqueMembers=\"false\"/> \n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Dimension name=\"Customers\" foreignKey=\"customer_id\">\n"
            + "    <Hierarchy hasAll=\"true\" allMemberName=\"All Customers\" primaryKey=\"customer_id\">\n"
            + "      <Table name=\"customer\"/>\n"
            + "      <Level name=\"Country\" column=\"country\" uniqueMembers=\"true\"/>\n"
            + "      <Level name=\"State Province\" column=\"state_province\" uniqueMembers=\"true\"/>\n"
            + "      <Level name=\"City\" column=\"city\" uniqueMembers=\"false\"/>\n"
            + "      <Level name=\"Name\" column=\"fullname\" uniqueMembers=\"true\">\n"
            + "        <Property name=\"Gender\" column=\"gender\"/>\n"
            + "        <Property name=\"Marital Status\" column=\"marital_status\"/>\n"
            + "        <Property name=\"Education\" column=\"education\"/>\n"
            + "        <Property name=\"Yearly Income\" column=\"yearly_income\"/>\n"
            + "      </Level>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "  <Dimension name=\"Gender\" foreignKey=\"customer_id\">\n"
            + "    <Hierarchy hasAll=\"true\" primaryKey=\"customer_id\">\n"
            + "    <Table name=\"customer\"/>\n"
            + "      <Level name=\"Gender\" column=\"gender\" uniqueMembers=\"true\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>"
            + "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\"\n"
            + "      formatString=\"Standard\" visible=\"false\"/>\n"
            + "  <Measure name=\"Store Sales\" column=\"store_sales\" aggregator=\"sum\"\n"
            + "      formatString=\"#,###.00\"/>\n"
            + "</Cube>",
            null,
            null,
            null,
            null);

        testContext.assertQueryReturns(
            "select {\n"
            + " [Customers].[All Customers].[USA],\n"
            + " [Customers].[All Customers].[USA].[OR],\n"
            + " [Customers].[All Customers].[USA].[CA],\n"
            + " [Customers].[All Customers].[USA].[CA].[Altadena],\n"
            + " [Customers].[All Customers].[USA].[CA].[Burbank],\n"
            + " [Customers].[All Customers].[USA].[CA].[Burbank].[Alma Son]} ON COLUMNS\n"
            + "from ["
            + cubeName
            + "] \n"
            + "where ([Cities].[All Cities].[Burbank], [Measures].[Store Sales])",
            "Axis #0:\n"
            + "{[Cities].[Burbank], [Measures].[Store Sales]}\n"
            + "Axis #1:\n"
            + "{[Customers].[USA]}\n"
            + "{[Customers].[USA].[OR]}\n"
            + "{[Customers].[USA].[CA]}\n"
            + "{[Customers].[USA].[CA].[Altadena]}\n"
            + "{[Customers].[USA].[CA].[Burbank]}\n"
            + "{[Customers].[USA].[CA].[Burbank].[Alma Son]}\n"
            + "Row #0: 6,577.33\n"
            + "Row #0: \n"
            + "Row #0: 6,577.33\n"
            + "Row #0: \n"
            + "Row #0: 6,577.33\n"
            + "Row #0: 36.50\n");
    }

    public void testOverrideDimension() {
        assertQueryReturns(
            "with member  [Gender].[test] as '\n"
            + "  aggregate(\n"
            + "  filter (crossjoin( [Gender].[Gender].members, [Time].[Time].members), \n"
            + "      [time].[Time].CurrentMember = [Time].[1997].[Q1]   AND\n"
            + "[measures].[unit sales] > 50) )\n"
            + "'\n"
            + "select \n"
            + "  { [time].[year].members } on 0,\n"
            + "  { [gender].[test] }\n"
            + " on 1  \n"
            + "from [sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time].[1997]}\n"
            + "{[Time].[1998]}\n"
            + "Axis #2:\n"
            + "{[Gender].[test]}\n"
            + "Row #0: 66,291\n"
            + "Row #0: 66,291\n");
    }

    public void testBadMeasure1() {
        final TestContext testContext = TestContext.instance().create(
            null,
            "<Cube name=\"SalesWithBadMeasure\">\n"
            + "  <Table name=\"sales_fact_1997\"/>\n"
            + "  <DimensionUsage name=\"Time\" source=\"Time\" foreignKey=\"time_id\"/>\n"
            + "  <Measure name=\"Bad Measure\" aggregator=\"sum\"\n"
            + "      formatString=\"Standard\"/>\n"
            + "</Cube>",
            null,
            null,
            null,
            null);
        Throwable throwable = null;
        try {
            testContext.assertSimpleQuery();
        } catch (Throwable e) {
            throwable = e;
        }
        // neither a source column or source expression specified
        TestContext.checkThrowable(
            throwable,
            "must contain either a source column or a source expression, but not both");
    }

    public void testBadMeasure2() {
        final TestContext testContext = TestContext.instance().create(
            null,
            "<Cube name=\"SalesWithBadMeasure2\">\n"
            + "  <Table name=\"sales_fact_1997\"/>\n"
            + "  <DimensionUsage name=\"Time\" source=\"Time\" foreignKey=\"time_id\"/>\n"
            + "  <Measure name=\"Bad Measure\" column=\"unit_sales\" aggregator=\"sum\"\n"
            + "      formatString=\"Standard\">\n"
            + "    <MeasureExpression>\n"
            + "       <SQL dialect=\"generic\">\n"
            + "         unit_sales\n"
            + "       </SQL>\n"
            + "    </MeasureExpression>\n"
            + "  </Measure>\n"
            + "</Cube>",
            null,
            null,
            null,
            null);
        Throwable throwable = null;
        try {
            testContext.assertSimpleQuery();
        } catch (Throwable e) {
            throwable = e;
        }
        // both a source column and source expression specified
        TestContext.checkThrowable(
            throwable,
            "must contain either a source column or a source expression, but not both");
    }

    public void testInvalidMembersInQuery() {
        String mdx =
            "select {[Measures].[Unit Sales]} on columns,\n"
            + " {[Time].[1997].[Q1], [Time].[1997].[QTOO]} on rows\n"
            + "from [Sales]";

        String mdx2 =
            "select {[Measures].[Unit Sales]} on columns,\n"
            + "nonemptycrossjoin(\n"
            + "{[Time].[1997].[Q1], [Time].[1997].[QTOO]},\n"
            + "[Customers].[All Customers].[USA].children) on rows\n"
            + "from [Sales]";

        String mdx3 =
            "select {[Measures].[Unit Sales]} on columns\n"
            + "from [Sales]\n"
            + "where ([Time].[1997].[QTOO])";

        // By default, reference to invalid member should cause
        // query failure.
        assertQueryThrows(
            mdx, "MDX object '[Time].[1997].[QTOO]' not found in cube 'Sales'");

        assertQueryThrows(
            mdx3,
            "MDX object '[Time].[1997].[QTOO]' not found in cube 'Sales'");

        // Now set property

        propSaver.set(
            props.IgnoreInvalidMembersDuringQuery,
            true);

        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Time].[1997].[Q1]}\n"
            + "Row #0: 66,291\n");

        // Illegal member in slicer
        assertQueryReturns(
            mdx3,
            "Axis #0:\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Row #0: \n");

        // Verify that invalid members in query do NOT prevent
        // usage of native NECJ (LER-5165).

        propSaver.set(
            props.AlertNativeEvaluationUnsupported,
            "ERROR");

        assertQueryReturns(
            mdx2,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Time].[1997].[Q1], [Customers].[USA].[CA]}\n"
            + "{[Time].[1997].[Q1], [Customers].[USA].[OR]}\n"
            + "{[Time].[1997].[Q1], [Customers].[USA].[WA]}\n"
            + "Row #0: 16,890\n"
            + "Row #1: 19,287\n"
            + "Row #2: 30,114\n");
    }

    public void testMemberOrdinalCaching() {
        propSaver.set(props.CompareSiblingsByOrderKey, true);
        // Use a fresh connection to make sure bad member ordinals haven't
        // been assigned by previous tests.
        final TestContext context = getTestContext().withFreshConnection();
        try {
            tryMemberOrdinalCaching(context);
        } finally {
            context.close();
        }
    }

    private void tryMemberOrdinalCaching(TestContext context) {
        // NOTE jvs 20-Feb-2007: If you change the calculated measure
        // definition below from zero to
        // [Customers].[Name].currentmember.Properties(\"MEMBER_ORDINAL\"), you
        // can see that the absolute ordinals returned are incorrect due to bug
        // 1660383 (http://tinyurl.com/3xb56f).  For now, this test just
        // verifies that the member sorting is correct when using relative
        // order key rather than absolute ordinal value.  If absolute ordinals
        // get fixed, replace zero with the MEMBER_ORDINAL property.

        context.assertQueryReturns(
            "with member [Measures].[o] as 0\n"
            + "set necj as nonemptycrossjoin(\n"
            + "[Store].[Store State].members, [Customers].[Name].members)\n"
            + "select tail(necj,5) on rows,\n"
            + "{[Measures].[o]} on columns\n"
            + "from [Sales]\n",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[o]}\n"
            + "Axis #2:\n"
            + "{[Store].[USA].[WA], [Customers].[USA].[WA].[Yakima].[Tracy Meyer]}\n"
            + "{[Store].[USA].[WA], [Customers].[USA].[WA].[Yakima].[Vanessa Thompson]}\n"
            + "{[Store].[USA].[WA], [Customers].[USA].[WA].[Yakima].[Velma Lykes]}\n"
            + "{[Store].[USA].[WA], [Customers].[USA].[WA].[Yakima].[William Battaglia]}\n"
            + "{[Store].[USA].[WA], [Customers].[USA].[WA].[Yakima].[Wilma Fink]}\n"
            + "Row #0: 0\n"
            + "Row #1: 0\n"
            + "Row #2: 0\n"
            + "Row #3: 0\n"
            + "Row #4: 0\n");

        // The query above primed the cache with bad absolute ordinals;
        // verify that this doesn't interfere with subsequent queries.

        context.assertQueryReturns(
            "with member [Measures].[o] as 0\n"
            + "select tail([Customers].[Name].members, 5)\n"
            + "on rows,\n"
            + "{[Measures].[o]} on columns\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[o]}\n"
            + "Axis #2:\n"
            + "{[Customers].[USA].[WA].[Yakima].[Tracy Meyer]}\n"
            + "{[Customers].[USA].[WA].[Yakima].[Vanessa Thompson]}\n"
            + "{[Customers].[USA].[WA].[Yakima].[Velma Lykes]}\n"
            + "{[Customers].[USA].[WA].[Yakima].[William Battaglia]}\n"
            + "{[Customers].[USA].[WA].[Yakima].[Wilma Fink]}\n"
            + "Row #0: 0\n"
            + "Row #1: 0\n"
            + "Row #2: 0\n"
            + "Row #3: 0\n"
            + "Row #4: 0\n");
    }

    public void testCancel()
    {
        // the cancel is issued after 2 seconds so the test query needs to
        // run for at least that long; it will because the query references
        // a Udf that has a 1 ms sleep in it; and there are enough rows
        // in the result that the Udf should execute > 2000 times
        String query =
            "WITH \n"
            + "  MEMBER [Measures].[Sleepy] \n"
            + "    AS 'SleepUdf([Measures].[Unit Sales])' \n"
            + "SELECT {[Measures].[Sleepy]} ON COLUMNS,\n"
            + "  {[Product].members} ON ROWS\n"
            + "FROM [Sales]";

        executeAndCancel(query, 2000);
    }


    private void executeAndCancel(String queryString, int waitMillis)
    {
        final TestContext tc = TestContext.instance().create(
            null,
            null,
            null,
            null,
            "<UserDefinedFunction name=\"SleepUdf\" className=\""
            + SleepUdf.class.getName()
            + "\"/>",
            null);
        Connection connection = tc.getConnection();

        final Query query = connection.parseQuery(queryString);
        final Throwable[] throwables = {null};
        if (waitMillis == 0) {
            // cancel immediately
            try {
                query.getStatement().cancel();
            } catch (Exception e) {
                throwables[0] = e;
            }
        } else {
            // Schedule timer to cancel after waitMillis
            Timer timer = new Timer(true);
            TimerTask task = new TimerTask()
            {
                public void run()
                {
                    Thread thread = Thread.currentThread();
                    thread.setName("CancelThread");
                    try {
                        query.getStatement().cancel();
                    } catch (Exception e) {
                        throwables[0] = e;
                    }
                }
            };
            timer.schedule(task, waitMillis);
        }
        if (throwables[0] != null) {
            Assert.fail(
                "Cancel request failed:  "
                + throwables[0]);
        }
        Throwable throwable = null;
        try {
            connection.execute(query);
        } catch (Throwable ex) {
            throwable = ex;
        }
        if (throwables[0] != null) {
            Assert.fail(
                "Cancel request failed:  "
                + throwables[0]);
        }
        TestContext.checkThrowable(throwable, "canceled");
    }

    /**
     * Test case for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-2161">
     * MONDRIAN-2161
     * </a>.<br>
     * Tests cancelation after executing sql for readTuples
     */
    public void testCancelSqlFetchReadTuples() throws Exception {
        // 512 rows
        final int cancelInterval = 50;
        final String query =
            "SELECT {[Measures].[Unit Sales]} ON COLUMNS,\n"
            + "  {[Product].members} ON ROWS\n"
            + "FROM [Sales]";
        propSaver.set(props.CancelPhaseInterval, cancelInterval);
        final String triggerSql = "product_name";

        Long rows = executeAndCancelAtSqlFetch(
            query, triggerSql,
            "SqlTupleReader.readTuples [[Product].[Product Name]]");
         Assert.assertEquals(
             "Query not aborted at first interval",
             new Long(cancelInterval), rows);
    }

    /**
     * Test case for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-2161">
     * MONDRIAN-2161
     * </a>.<br>
     * Tests cancelation after executing sql for SegmentLoader
     */
    public void testCancelSqlFetchSegmentLoad() throws Exception {
          // 512 rows
          final int cancelInterval = 101;
          propSaver.set(props.CancelPhaseInterval, cancelInterval);
          // this will avoid spamming output with cache failures, but should
          // also work without side effects with cache enabled
          propSaver.set(props.DisableCaching, true);
          final String query =
              "SELECT {[Measures].[Unit Sales]} ON COLUMNS,\n"
              + "  {[Product].members} ON ROWS\n"
              + "FROM [Sales]";
          final String triggerSql = "product_name";

        Long rows = executeAndCancelAtSqlFetch(
            query, triggerSql,
            "Segment.load");
         Assert.assertEquals(
             "Query not aborted at first interval",
             new Long(cancelInterval), rows);
    }

    /**
     * Test case for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-2161">
     * MONDRIAN-2161
     * </a>.<br>
     * Tests cancelation after executing sql for getMemberChildren
     */
    public void testCancelSqlFetchMemberChildren() throws Exception {
        // 106 rows
        final int cancelInterval = 33;
        final String query =
            "SELECT {[Measures].[Unit Sales]} ON COLUMNS,\n"
            + "  {[Customers].[Mexico].[Guerrero].[Acapulco].Children} ON ROWS\n"
            + "FROM [Sales]";
        propSaver.set(props.CancelPhaseInterval, cancelInterval);

        Long rows = executeAndCancelAtSqlFetch(
            query, "customer_id", "SqlMemberSource.getMemberChildren");
        Assert.assertEquals(
            "Query not aborted at first interval",
            new Long(cancelInterval), rows);
    }

    private Long executeAndCancelAtSqlFetch(
        final String query, final String triggerSql, final String component)
        throws Exception
    {
        // avoid cache to ensure sql executes
        TestContext context = getTestContext().withFreshConnection();
        context.flushSchemaCache();

        RolapConnection conn =  (RolapConnection) context.getConnection();
        final mondrian.server.Statement stmt = conn.getInternalStatement();
        // use the logger to block and trigger cancelation at the right time
        Logger sqlLog = RolapUtil.SQL_LOGGER;
        propSaver.set(sqlLog, org.apache.log4j.Level.DEBUG);
        final Execution exec = new Execution(stmt, 50000);
        final CountDownLatch okToGo = new CountDownLatch(1);
        SqlCancelingAppender canceler =
            new SqlCancelingAppender(component, triggerSql, exec, okToGo);
        stmt.setQuery(conn.parseQuery(query));
        sqlLog.addAppender(canceler);
        try {
            conn.execute(exec);
            Assert.fail("Query not canceled.");
        } catch (QueryCanceledException e) {
            // 5 sec just in case it all goes wrong
            if (!okToGo.await(5, TimeUnit.SECONDS)) {
                Assert.fail("Timeout reading sql statement end from log.");
            }
            return canceler.rows;
        } finally {
            sqlLog.removeAppender(canceler);
            context.close();
        }
        return null;
    }

    /**
     * Listens to sql log for a specific query, cancels mdx when it's
     * executed and reads number of fetched rows.
     */
    private static class SqlCancelingAppender extends AppenderSkeleton {
        private String trigger;
        private Pattern stmtNbrGetter, stmtCanceler, stmtRowCounter;
        private int state = 0;
        Long rows = null;
        Execution exec;
        CountDownLatch latch;

        SqlCancelingAppender(
            String comp, String trigger, Execution exec, CountDownLatch latch)
        {
            super();
            this.trigger = trigger;
            this.latch = latch;
            // capture stalked statement's number
            stmtNbrGetter = Pattern.compile(
                "^([0-9]*):\\s*"
                    + Pattern.quote(comp) + "\\s*: executing sql ");
            this.exec = exec;
        }

        @Override
        protected synchronized void append(LoggingEvent event) {
            String msg = event.getMessage().toString();
            if (state == 0
                && msg.contains(trigger))
            {
                Matcher matcher = stmtNbrGetter.matcher(msg);
                if (matcher.find()) {
                    // get sql statement number
                    String stmt = matcher.group(1);
                    // log entry to trigger cancel
                    stmtCanceler =
                        Pattern.compile("^" + stmt + ":\\s*,\\s*exec\\s");
                    // log entry to find fetched rows
                    stmtRowCounter = Pattern.compile(
                        "^" + stmt + ":\\s*,[^,]*,\\s+([0-9]*)\\s+rows\\s*$");
                    state = 1;
                }
            } else if (state == 1) {
                if (stmtCanceler.matcher(msg).find()) {
                    // sql in fetch phase
                    // cancel the mdx query
                    exec.cancel();
                    state = 2;
                }
            } else if (state == 2) {
                Matcher matcher = stmtRowCounter.matcher(msg);
                if (matcher.find()) {
                    rows = new Long(matcher.group(1));
                    // invalidate
                    state++;
                    // release test
                    latch.countDown();
                }
            }
        }

        @Override
        public void close() {
        }

        @Override
        public boolean requiresLayout() {
            return false;
        }
    }

    public void testQueryTimeout() {
        // timeout is issued after 2 seconds so the test query needs to
        // run for at least that long; it will because the query references
        // a Udf that has a 1 ms sleep in it; and there are enough rows
        // in the result that the Udf should execute > 2000 times
        final TestContext tc = TestContext.instance().create(
            null,
            null,
            null,
            null,
            "<UserDefinedFunction name=\"SleepUdf\" className=\""
            + SleepUdf.class.getName()
            + "\"/>",
            null);

        String query =
            "WITH\n"
            + "  MEMBER [Measures].[Sleepy]\n"
            + "    AS 'SleepUdf([Measures].[Unit Sales])'\n"
            + "SELECT {[Measures].[Sleepy]} ON COLUMNS,\n"
            + "  {[Product].members} ON ROWS\n"
            + "FROM [Sales]";
        Throwable throwable = null;
        propSaver.set(props.QueryTimeout, 2);
        try {
            tc.executeQuery(query);
        } catch (Throwable ex) {
            throwable = ex;
        }
        TestContext.checkThrowable(
            throwable, "Query timeout of 2 seconds reached");
    }

    public void testFormatInheritance() {
        assertQueryReturns(
            "with member measures.foo as 'measures.bar' "
            + "member measures.bar as "
            + "'measures.profit' select {measures.foo} on 0 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[foo]}\n"
            + "Row #0: $339,610.90\n");
    }

    public void testFormatInheritanceWithIIF() {
        assertQueryReturns(
            "with member measures.foo as 'measures.bar' "
            + "member measures.bar as "
            + "'iif(not isempty(measures.profit),measures.profit,null)' "
            + "select from sales where measures.foo",
            "Axis #0:\n"
            + "{[Measures].[foo]}\n"
            + "$339,610.90");
    }

    /**
     * For a calulated member picks up the format of first member that has a
     * format.  In this particular case foo will use profit's format, i.e
     * neither [unit sales] nor [customer count] format is used.
     */
    public void testFormatInheritanceWorksWithFirstFormatItFinds() {
        assertQueryReturns(
            "with member measures.foo as 'measures.bar' "
            + "member measures.bar as "
            + "'iif(measures.profit>3000,measures.[unit sales],measures.[Customer Count])' "
            + "select {[Store].[All Stores].[USA].[WA].children} on 0 "
            + "from sales where measures.foo",
            "Axis #0:\n"
            + "{[Measures].[foo]}\n"
            + "Axis #1:\n"
            + "{[Store].[USA].[WA].[Bellingham]}\n"
            + "{[Store].[USA].[WA].[Bremerton]}\n"
            + "{[Store].[USA].[WA].[Seattle]}\n"
            + "{[Store].[USA].[WA].[Spokane]}\n"
            + "{[Store].[USA].[WA].[Tacoma]}\n"
            + "{[Store].[USA].[WA].[Walla Walla]}\n"
            + "{[Store].[USA].[WA].[Yakima]}\n"
            + "Row #0: $190.00\n"
            + "Row #0: $24,576.00\n"
            + "Row #0: $25,011.00\n"
            + "Row #0: $23,591.00\n"
            + "Row #0: $35,257.00\n"
            + "Row #0: $96.00\n"
            + "Row #0: $11,491.00\n");
    }

    /**
     * Test format string values. Previously, a bug meant that string values
     * were printed as is, never passed through the format string.
     */
    public void testFormatStringAppliedToStringValue() {
        // "23" as an integer value
        assertQueryReturns(
            "with member [Measures].[Test] as '23', FORMAT_STRING = '|<|arrow=\"up\"'\n"
            + "select [Measures].[Test] on 0\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Test]}\n"
            + "Row #0: |23|arrow=up\n");
        // "23" as a string value: converted to lower case
        assertQueryReturns(
            "with member [Measures].[Test] as '\"23\"', FORMAT_STRING = '|<|arrow=\"up\"'\n"
            + "select [Measures].[Test] on 0\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Test]}\n"
            + "Row #0: |23|arrow=up\n");
        // string value "Foo Bar" -- converted to lower case
        assertQueryReturns(
            "with member [Measures].[Test] as '\"Foo \" || \"Bar\"', FORMAT_STRING = '|<|arrow=\"up\"'\n"
            + "select [Measures].[Test] on 0\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Test]}\n"
            + "Row #0: |foo bar|arrow=up\n");
    }

    /**
     * This tests a fix for bug #1603653
     */
    public void testAvgCastProblem() {
        assertQueryReturns(
            "with member measures.bar as "
            + "'iif(measures.profit>3000,min([Education Level].[Education Level].Members),min([Education Level].[Education Level].Members))' "
            + "select {[Store].[All Stores].[USA].[WA].children} on 0 "
            + "from sales where measures.bar",
            "Axis #0:\n"
            + "{[Measures].[bar]}\n"
            + "Axis #1:\n"
            + "{[Store].[USA].[WA].[Bellingham]}\n"
            + "{[Store].[USA].[WA].[Bremerton]}\n"
            + "{[Store].[USA].[WA].[Seattle]}\n"
            + "{[Store].[USA].[WA].[Spokane]}\n"
            + "{[Store].[USA].[WA].[Tacoma]}\n"
            + "{[Store].[USA].[WA].[Walla Walla]}\n"
            + "{[Store].[USA].[WA].[Yakima]}\n"
            + "Row #0: $95.00\n"
            + "Row #0: $1,835.00\n"
            + "Row #0: $1,277.00\n"
            + "Row #0: $1,434.00\n"
            + "Row #0: $1,084.00\n"
            + "Row #0: $129.00\n"
            + "Row #0: $958.00\n");
    }

    /**
     * Test format inheritance to pickup format from second measure when the
     * first does not have one.
     */
    public void testFormatInheritanceUseSecondIfFirstHasNoFormat() {
        assertQueryReturns(
            "with member measures.foo as 'measures.bar+measures.blah'"
            + " member measures.bar as '10'"
            + " member measures.blah as '20',format_string='$##.###.00' "
            + "select from sales where measures.foo",
            "Axis #0:\n"
            + "{[Measures].[foo]}\n"
            + "$30.00");
    }

    /**
     * Tests format inheritance with complex expression to assert that the
     * format of the first member that has a valid format is used.
     */
    public void testFormatInheritanceUseFirstValid() {
        assertQueryReturns(
            "with member measures.foo as '13+31*measures.[Unit Sales]/"
            + "iif(measures.profit>0,measures.profit,measures.[Customer Count])'"
            + " select {[Store].[All Stores].[USA].[CA].children} on 0 "
            + "from sales where measures.foo",
            "Axis #0:\n"
            + "{[Measures].[foo]}\n"
            + "Axis #1:\n"
            + "{[Store].[USA].[CA].[Alameda]}\n"
            + "{[Store].[USA].[CA].[Beverly Hills]}\n"
            + "{[Store].[USA].[CA].[Los Angeles]}\n"
            + "{[Store].[USA].[CA].[San Diego]}\n"
            + "{[Store].[USA].[CA].[San Francisco]}\n"
            + "Row #0: 13\n"
            + "Row #0: 37\n"
            + "Row #0: 37\n"
            + "Row #0: 37\n"
            + "Row #0: 38\n");
    }

    public void testQueryIterationLimit() {
        // Query will need to iterate 4*3 times to compute aggregates,
        // so set iteration limit to 11
        String queryString =
            "With Set [*NATIVE_CJ_SET] as "
            + "'NonEmptyCrossJoin([*BASE_MEMBERS_Dates], [*BASE_MEMBERS_Stores])' "
            + "Set [*BASE_MEMBERS_Dates] as '{[Time].[1997].[Q1], [Time].[1997].[Q2], [Time].[1997].[Q3], [Time].[1997].[Q4]}' "
            + "Set [*GENERATED_MEMBERS_Dates] as 'Generate([*NATIVE_CJ_SET], {[Time].[Time].CurrentMember})' "
            + "Set [*GENERATED_MEMBERS_Measures] as '{[Measures].[*SUMMARY_METRIC_0]}' "
            + "Set [*BASE_MEMBERS_Stores] as '{[Store].[USA].[CA], [Store].[USA].[WA], [Store].[USA].[OR]}' "
            + "Set [*GENERATED_MEMBERS_Stores] as 'Generate([*NATIVE_CJ_SET], {[Store].CurrentMember})' "
            + "Member [Time].[Time].[*SM_CTX_SEL] as 'Aggregate([*GENERATED_MEMBERS_Dates])' "
            + "Member [Measures].[*SUMMARY_METRIC_0] as '[Measures].[Unit Sales]/([Measures].[Unit Sales],[Time].[*SM_CTX_SEL])' "
            + "Member [Time].[Time].[*SUBTOTAL_MEMBER_SEL~SUM] as 'sum([*GENERATED_MEMBERS_Dates])' "
            + "Member [Store].[*SUBTOTAL_MEMBER_SEL~SUM] as 'sum([*GENERATED_MEMBERS_Stores])' "
            + "select crossjoin({[Time].[*SUBTOTAL_MEMBER_SEL~SUM]}, {[Store].[*SUBTOTAL_MEMBER_SEL~SUM]}) "
            + "on columns from [Sales]";

        propSaver.set(props.IterationLimit, 11);

        Throwable throwable = null;
        try {
            Connection connection = getConnection();
            Query query = connection.parseQuery(queryString);
            query.setResultStyle(ResultStyle.LIST);
            connection.execute(query);
        } catch (Throwable ex) {
            throwable = ex;
        }

        TestContext.checkThrowable(
            throwable, "Number of iterations exceeded limit of 11");

        // make sure the query runs without the limit set
        propSaver.reset();
        executeQuery(queryString);
    }

    public void testGetCaptionUsingMemberDotCaption() {
        assertQueryReturns(
            "SELECT Filter(Store.allmembers, "
            + "[store].currentMember.caption = \"USA\") on 0 FROM SALES",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[USA]}\n"
            + "Row #0: 266,773\n");
    }

    public void testGetCaptionUsingMemberDotPropertiesCaption() {
        assertQueryReturns(
            "SELECT Filter(Store.allmembers, "
            + "[store].currentMember.properties(\"caption\") = \"USA\") "
            + "on 0 FROM SALES",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[USA]}\n"
            + "Row #0: 266,773\n");
    }

    public void testDefaultMeasureInCube() {
        TestContext testContext = TestContext.instance().create(
            null,
            "<Cube name=\"DefaultMeasureTesting\" defaultMeasure=\"Supply Time\">\n"
            + "  <Table name=\"inventory_fact_1997\"/>\n"
            + "  <DimensionUsage name=\"Store\" source=\"Store\" "
            + "foreignKey=\"store_id\"/>\n"
            + "  <DimensionUsage name=\"Store Type\" source=\"Store Type\" "
            + "foreignKey=\"store_id\"/>\n"
            + "  <Measure name=\"Store Invoice\" column=\"store_invoice\" "
            + "aggregator=\"sum\"/>\n"
            + "  <Measure name=\"Supply Time\" column=\"supply_time\" "
            + "aggregator=\"sum\"/>\n"
            + "  <Measure name=\"Warehouse Cost\" column=\"warehouse_cost\" "
            + "aggregator=\"sum\"/>\n"
            + "</Cube>",
            null,
            null,
            null,
            null);
        String queryWithoutFilter =
            "select store.members on 0 from "
            + "DefaultMeasureTesting";
        String queryWithDeflaultMeasureFilter =
            "select store.members on 0 "
            + "from DefaultMeasureTesting where [measures].[Supply Time]";
        assertQueriesReturnSimilarResults(
            queryWithoutFilter, queryWithDeflaultMeasureFilter, testContext);
    }

    public void testDefaultMeasureInCubeForIncorrectMeasureName() {
        TestContext testContext = TestContext.instance().create(
            null,
            "<Cube name=\"DefaultMeasureTesting\" defaultMeasure=\"Supply Time Error\">\n"
            + "  <Table name=\"inventory_fact_1997\"/>\n"
            + "  <DimensionUsage name=\"Store\" source=\"Store\" "
            + "foreignKey=\"store_id\"/>\n"
            + "  <DimensionUsage name=\"Store Type\" source=\"Store Type\" "
            + "foreignKey=\"store_id\"/>\n"
            + "  <Measure name=\"Store Invoice\" column=\"store_invoice\" "
            + "aggregator=\"sum\"/>\n"
            + "  <Measure name=\"Supply Time\" column=\"supply_time\" "
            + "aggregator=\"sum\"/>\n"
            + "  <Measure name=\"Warehouse Cost\" column=\"warehouse_cost\" "
            + "aggregator=\"sum\"/>\n"
            + "</Cube>",
            null,
            null,
            null,
            null);
        String queryWithoutFilter =
            "select store.members on 0 from "
            + "DefaultMeasureTesting";
        String queryWithFirstMeasure =
            "select store.members on 0 "
            + "from DefaultMeasureTesting where [measures].[Store Invoice]";
        assertQueriesReturnSimilarResults(
            queryWithoutFilter, queryWithFirstMeasure, testContext);
    }

    public void testDefaultMeasureInCubeForCaseSensitivity() {
        TestContext testContext = TestContext.instance().create(
            null,
            "<Cube name=\"DefaultMeasureTesting\" defaultMeasure=\"SUPPLY TIME\">\n"
            + "  <Table name=\"inventory_fact_1997\"/>\n"
            + "  <DimensionUsage name=\"Store\" source=\"Store\" "
            + "foreignKey=\"store_id\"/>\n"
            + "  <DimensionUsage name=\"Store Type\" source=\"Store Type\" "
            + "foreignKey=\"store_id\"/>\n"
            + "  <Measure name=\"Store Invoice\" column=\"store_invoice\" "
            + "aggregator=\"sum\"/>\n"
            + "  <Measure name=\"Supply Time\" column=\"supply_time\" "
            + "aggregator=\"sum\"/>\n"
            + "  <Measure name=\"Warehouse Cost\" column=\"warehouse_cost\" "
            + "aggregator=\"sum\"/>\n"
            + "</Cube>",
            null,
            null,
            null,
            null);
        String queryWithoutFilter =
            "select store.members on 0 from "
            + "DefaultMeasureTesting";
        String queryWithFirstMeasure =
            "select store.members on 0 "
            + "from DefaultMeasureTesting where [measures].[Store Invoice]";
        String queryWithDefaultMeasureFilter =
            "select store.members on 0 "
            + "from DefaultMeasureTesting where [measures].[Supply Time]";
        if (props.CaseSensitive.get()) {
            assertQueriesReturnSimilarResults(
                queryWithoutFilter, queryWithFirstMeasure, testContext);
        } else {
            assertQueriesReturnSimilarResults(
                queryWithoutFilter, queryWithDefaultMeasureFilter, testContext);
        }
    }

    /**
     * This tests for bug #1706434,
     * the ability to convert numeric types to logical (boolean) types.
     */
    public void testNumericToLogicalConversion() {
        assertQueryReturns(
            "select "
            + "{[Measures].[Unit Sales]} on columns, "
            + "Filter(Descendants("
            + "[Product].[Food].[Baked Goods].[Bread]), "
            + "Count([Product].currentMember.children)) on Rows "
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Food].[Baked Goods].[Bread]}\n"
            + "{[Product].[Food].[Baked Goods].[Bread].[Bagels]}\n"
            + "{[Product].[Food].[Baked Goods].[Bread].[Bagels].[Colony]}\n"
            + "{[Product].[Food].[Baked Goods].[Bread].[Bagels].[Fantastic]}\n"
            + "{[Product].[Food].[Baked Goods].[Bread].[Bagels].[Great]}\n"
            + "{[Product].[Food].[Baked Goods].[Bread].[Bagels].[Modell]}\n"
            + "{[Product].[Food].[Baked Goods].[Bread].[Bagels].[Sphinx]}\n"
            + "{[Product].[Food].[Baked Goods].[Bread].[Muffins]}\n"
            + "{[Product].[Food].[Baked Goods].[Bread].[Muffins].[Colony]}\n"
            + "{[Product].[Food].[Baked Goods].[Bread].[Muffins].[Fantastic]}\n"
            + "{[Product].[Food].[Baked Goods].[Bread].[Muffins].[Great]}\n"
            + "{[Product].[Food].[Baked Goods].[Bread].[Muffins].[Modell]}\n"
            + "{[Product].[Food].[Baked Goods].[Bread].[Muffins].[Sphinx]}\n"
            + "{[Product].[Food].[Baked Goods].[Bread].[Sliced Bread]}\n"
            + "{[Product].[Food].[Baked Goods].[Bread].[Sliced Bread].[Colony]}\n"
            + "{[Product].[Food].[Baked Goods].[Bread].[Sliced Bread].[Fantastic]}\n"
            + "{[Product].[Food].[Baked Goods].[Bread].[Sliced Bread].[Great]}\n"
            + "{[Product].[Food].[Baked Goods].[Bread].[Sliced Bread].[Modell]}\n"
            + "{[Product].[Food].[Baked Goods].[Bread].[Sliced Bread].[Sphinx]}\n"
            + "Row #0: 7,870\n"
            + "Row #1: 815\n"
            + "Row #2: 163\n"
            + "Row #3: 160\n"
            + "Row #4: 145\n"
            + "Row #5: 165\n"
            + "Row #6: 182\n"
            + "Row #7: 3,497\n"
            + "Row #8: 740\n"
            + "Row #9: 798\n"
            + "Row #10: 605\n"
            + "Row #11: 719\n"
            + "Row #12: 635\n"
            + "Row #13: 3,558\n"
            + "Row #14: 737\n"
            + "Row #15: 815\n"
            + "Row #16: 638\n"
            + "Row #17: 653\n"
            + "Row #18: 715\n");
    }

    public void testRollupQuery() {
        assertQueryReturns(
            "SELECT {[Product].[Product Department].MEMBERS} ON AXIS(0),\n"
            + "{{[Gender].[Gender].MEMBERS}, {[Gender].[All Gender]}} ON AXIS(1)\n"
            + "FROM [Sales 2] WHERE {[Measures].[Unit Sales]}",
            "Axis #0:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #1:\n"
            + "{[Product].[Drink].[Alcoholic Beverages]}\n"
            + "{[Product].[Drink].[Beverages]}\n"
            + "{[Product].[Drink].[Dairy]}\n"
            + "{[Product].[Food].[Baked Goods]}\n"
            + "{[Product].[Food].[Baking Goods]}\n"
            + "{[Product].[Food].[Breakfast Foods]}\n"
            + "{[Product].[Food].[Canned Foods]}\n"
            + "{[Product].[Food].[Canned Products]}\n"
            + "{[Product].[Food].[Dairy]}\n"
            + "{[Product].[Food].[Deli]}\n"
            + "{[Product].[Food].[Eggs]}\n"
            + "{[Product].[Food].[Frozen Foods]}\n"
            + "{[Product].[Food].[Meat]}\n"
            + "{[Product].[Food].[Produce]}\n"
            + "{[Product].[Food].[Seafood]}\n"
            + "{[Product].[Food].[Snack Foods]}\n"
            + "{[Product].[Food].[Snacks]}\n"
            + "{[Product].[Food].[Starchy Foods]}\n"
            + "{[Product].[Non-Consumable].[Carousel]}\n"
            + "{[Product].[Non-Consumable].[Checkout]}\n"
            + "{[Product].[Non-Consumable].[Health and Hygiene]}\n"
            + "{[Product].[Non-Consumable].[Household]}\n"
            + "{[Product].[Non-Consumable].[Periodicals]}\n"
            + "Axis #2:\n"
            + "{[Gender].[F]}\n"
            + "{[Gender].[M]}\n"
            + "{[Gender].[All Gender]}\n"
            + "Row #0: 3,439\n"
            + "Row #0: 6,776\n"
            + "Row #0: 1,987\n"
            + "Row #0: 3,771\n"
            + "Row #0: 9,841\n"
            + "Row #0: 1,821\n"
            + "Row #0: 9,407\n"
            + "Row #0: 867\n"
            + "Row #0: 6,513\n"
            + "Row #0: 5,990\n"
            + "Row #0: 2,001\n"
            + "Row #0: 13,011\n"
            + "Row #0: 841\n"
            + "Row #0: 18,713\n"
            + "Row #0: 947\n"
            + "Row #0: 14,936\n"
            + "Row #0: 3,459\n"
            + "Row #0: 2,696\n"
            + "Row #0: 368\n"
            + "Row #0: 887\n"
            + "Row #0: 7,841\n"
            + "Row #0: 13,278\n"
            + "Row #0: 2,168\n"
            + "Row #1: 3,399\n"
            + "Row #1: 6,797\n"
            + "Row #1: 2,199\n"
            + "Row #1: 4,099\n"
            + "Row #1: 10,404\n"
            + "Row #1: 1,496\n"
            + "Row #1: 9,619\n"
            + "Row #1: 945\n"
            + "Row #1: 6,372\n"
            + "Row #1: 6,047\n"
            + "Row #1: 2,131\n"
            + "Row #1: 13,644\n"
            + "Row #1: 873\n"
            + "Row #1: 19,079\n"
            + "Row #1: 817\n"
            + "Row #1: 15,609\n"
            + "Row #1: 3,425\n"
            + "Row #1: 2,566\n"
            + "Row #1: 473\n"
            + "Row #1: 892\n"
            + "Row #1: 8,443\n"
            + "Row #1: 13,760\n"
            + "Row #1: 2,126\n"
            + "Row #2: 6,838\n"
            + "Row #2: 13,573\n"
            + "Row #2: 4,186\n"
            + "Row #2: 7,870\n"
            + "Row #2: 20,245\n"
            + "Row #2: 3,317\n"
            + "Row #2: 19,026\n"
            + "Row #2: 1,812\n"
            + "Row #2: 12,885\n"
            + "Row #2: 12,037\n"
            + "Row #2: 4,132\n"
            + "Row #2: 26,655\n"
            + "Row #2: 1,714\n"
            + "Row #2: 37,792\n"
            + "Row #2: 1,764\n"
            + "Row #2: 30,545\n"
            + "Row #2: 6,884\n"
            + "Row #2: 5,262\n"
            + "Row #2: 841\n"
            + "Row #2: 1,779\n"
            + "Row #2: 16,284\n"
            + "Row #2: 27,038\n"
            + "Row #2: 4,294\n");
    }

    /**
     * Tests for bug #1630754. In Mondrian 2.2.2 the SqlTupleReader.readTuples
     * method would create a SQL having an in-clause with more that 1000
     * entities under some circumstances. This exceeded the limit for Oracle
     * resulting in an ORA-01795 error.
     */
    public void testBug1630754() {
        // In order to reproduce this bug a dimension with 2 levels with more
        // than 1000 member each was necessary. The customer_id column has more
        // than 1000 distinct members so it was used for this test.
        TestContext testContext = TestContext.instance()
        .createSubstitutingCube(
            "Sales",
            "  <Dimension name=\"Customer_2\" foreignKey=\"customer_id\">\n"
            + "    <Hierarchy hasAll=\"true\" "
            + "allMemberName=\"All Customers\" "
            + "primaryKey=\"customer_id\" "
            + " >\n"
            + "      <Table name=\"customer\"/>\n"
            + "      <Level name=\"Name1\" column=\"customer_id\" uniqueMembers=\"true\"/>"
            + "      <Level name=\"Name2\" column=\"customer_id\" uniqueMembers=\"true\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>");

        Result result = testContext.executeQuery(
            "WITH SET [#DataSet#] AS "
            + "   'NonEmptyCrossjoin({Descendants([Customer_2].[All Customers], 2)}, "
            + "   {[Product].[All Products]})' "
            + "SELECT {[Measures].[Unit Sales], [Measures].[Store Sales]} on columns, "
            + "Hierarchize({[#DataSet#]}) on rows FROM [Sales]");

        final int rowCount = result.getAxes()[1].getPositions().size();
        assertEquals(5581, rowCount);
    }

    /**
     * Tests a query which uses filter and crossjoin. This query caused
     * problems when the retrowoven version of mondrian was used in jdk1.5,
     * specifically a {@link ClassCastException} trying to cast a {@link List}
     * to a {@link Iterable}.
     */
    public void testNonEmptyCrossjoinFilter() {
        String desiredResult =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[All Products], [Time].[1997].[Q2].[5]}\n"
            + "Row #0: 21,081\n";

        assertQueryReturns(
            "select NON EMPTY {[Measures].[Unit Sales]} ON COLUMNS,\n"
            + "NON EMPTY Crossjoin("
            + "  {Product.[All Products]},\n"
            + "  Filter("
            + "    Descendants(Time.[Time], [Time].[Month]), "
            + "    Time.[Time].CurrentMember.Name = '5')) ON ROWS\n"
            + "from [Sales] ",
            desiredResult);

        assertQueryReturns(
            "select NON EMPTY {[Measures].[Unit Sales]} ON COLUMNS,\n"
            + "NON EMPTY Filter("
            + "  Crossjoin("
            + "    {Product.[All Products]},\n"
            + "    Descendants(Time.[Time], [Time].[Month])),"
            + "  Time.[Time].CurrentMember.Name = '5') ON ROWS\n"
            + "from [Sales] ",
            desiredResult);
    }

    public void testDuplicateAxisFails() {
        assertQueryThrows(
            "select [Gender].Members on columns,"
            + " [Measures].Members on columns "
            + "from [Sales]",
            "Duplicate axis name 'COLUMNS'.");
    }

    public void testInvalidAxisFails() {
        assertQueryThrows(
            "select [Gender].Members on 0,"
            + " [Measures].Members on 10 "
            + "from [Sales]",
            "Axis numbers specified in a query must be sequentially specified,"
            + " and cannot contain gaps. Axis 1 (ROWS) is missing.");

        assertQueryThrows(
            "select [Gender].Members on columns,"
            + " [Measures].Members on foobar\n"
            + "from [Sales]",
            "Syntax error at line 1, column 59, token 'foobar'");

        assertQueryThrows(
            "select [Gender].Members on columns,"
            + " [Measures].Members on slicer\n"
            + "from [Sales]",
            "Syntax error at line 1, column 59, token 'slicer'");

        assertQueryThrows(
            "select [Gender].Members on columns,"
            + " [Measures].Members on filter\n"
            + "from [Sales]",
            "Syntax error at line 1, column 59, token 'filter'");
    }

    /**
     * Tests various ways to sum the properties of the descendants of a member,
     * inspired by forum post
     * <a href="http://forums.pentaho.com/showthread.php?p=177135">summing
     * properties</a>.
     */
    public void testSummingProperties() {
        final String expected =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[USA]}\n"
            + "{[Store].[USA].[CA]}\n"
            + "Axis #2:\n"
            + "{[Gender].[F]}\n"
            + "{[Gender].[M]}\n"
            + "Row #0: 131,558\n"
            + "Row #0: 36,759\n"
            + "Row #1: 135,215\n"
            + "Row #1: 37,989\n";

        assertQueryReturns(
            "with member [Measures].[Sum Sqft] as '"
            + "sum("
            + "  Descendants([Store].CurrentMember, [Store].Levels(5)),"
            + "  [Store].CurrentMember.Properties(\"Store Sqft\")) '\n"
            + "select {[Store].[USA], [Store].[USA].[CA]} on 0,\n"
            + " [Gender].Children on 1\n"
            + "from [Sales]",
            expected);

        // same query, except get level by name not ordinal, should give same
        // result
        assertQueryReturns(
            "with member [Measures].[Sum Sqft] as '"
            + "sum("
            + "  Descendants([Store].CurrentMember, [Store].Levels(\"Store Name\")),"
            + "  [Store].CurrentMember.Properties(\"Store Sqft\")) '\n"
            + "select {[Store].[USA], [Store].[USA].[CA]} on 0,\n"
            + " [Gender].Children on 1\n"
            + "from [Sales]",
            expected);

        // same query, except level is hard-coded; same result again
        assertQueryReturns(
            "with member [Measures].[Sum Sqft] as '"
            + "sum("
            + "  Descendants([Store].CurrentMember, [Store].[Store Name]),"
            + "  [Store].CurrentMember.Properties(\"Store Sqft\")) '\n"
            + "select {[Store].[USA], [Store].[USA].[CA]} on 0,\n"
            + " [Gender].Children on 1\n"
            + "from [Sales]",
            expected);


        // same query, except using the level-less form of the DESCENDANTS
        // function; same result again
        assertQueryReturns(
            "with member [Measures].[Sum Sqft] as '"
            + "sum("
            + "  Descendants([Store].CurrentMember, , LEAVES),"
            + "  [Store].CurrentMember.Properties(\"Store Sqft\")) '\n"
            + "select {[Store].[USA], [Store].[USA].[CA]} on 0,\n"
            + " [Gender].Children on 1\n"
            + "from [Sales]",
            expected);
    }

    public void testIifWithTupleFirstAndMemberNextWithMeasure() {
        assertQueryReturns(
            "WITH\n"
            + "MEMBER [Gender].agg "
            + "AS 'IIF(1=1, ([Gender].[All Gender],measures.[unit sales]),"
            + "([Gender].[All Gender]))', SOLVE_ORDER = 4 "
            + "SELECT {[Measures].[unit sales]} ON 0, "
            + "{{[Gender].[Gender].MEMBERS},{([Gender].agg)}} on 1 FROM sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Gender].[F]}\n"
            + "{[Gender].[M]}\n"
            + "{[Gender].[agg]}\n"
            + "Row #0: 131,558\n"
            + "Row #1: 135,215\n"
            + "Row #2: 266,773\n");
    }

    public void testIifWithMemberFirstAndTupleNextWithMeasure() {
        assertQueryReturns(
            "WITH\n"
            + "MEMBER [Gender].agg "
            + "AS 'IIF(1=1, ([Gender].[All Gender]),"
            + "([Gender].[All Gender],measures.[unit sales]))', SOLVE_ORDER = 4 "
            + "SELECT {[Measures].[unit sales]} ON 0, "
            + "{{[Gender].[Gender].MEMBERS},{([Gender].agg)}} on 1 FROM sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Gender].[F]}\n"
            + "{[Gender].[M]}\n"
            + "{[Gender].[agg]}\n"
            + "Row #0: 131,558\n"
            + "Row #1: 135,215\n"
            + "Row #2: 266,773\n");
    }

    public void testIifWithMemberFirstAndTupleNextWithoutMeasure() {
        assertQueryReturns(
            "WITH\n"
            + "MEMBER [Gender].agg "
            + "AS 'IIF(1=1, ([Gender].[All Gender]),"
            + "([Gender].[All Gender],[Time].[1997]))', SOLVE_ORDER = 4 "
            + "SELECT {[Measures].[unit sales]} ON 0, "
            + "{{[Gender].[Gender].MEMBERS},{([Gender].agg)}} on 1 FROM sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Gender].[F]}\n"
            + "{[Gender].[M]}\n"
            + "{[Gender].[agg]}\n"
            + "Row #0: 131,558\n"
            + "Row #1: 135,215\n"
            + "Row #2: 266,773\n");
    }

    public void testIifWithTupleFirstAndMemberNextWithoutMeasure() {
        assertQueryReturns(
            "WITH\n"
            + "MEMBER [Gender].agg "
            + "AS 'IIF(1=1, "
            + "([Store].[All Stores].[USA], [Gender].[All Gender]), "
            + "([Gender].[All Gender]))', "
            + "SOLVE_ORDER = 4 "
            + "SELECT {[Measures].[unit sales]} ON 0, "
            + "{([Gender].agg)} on 1 FROM sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Gender].[agg]}\n"
            + "Row #0: 266,773\n");
    }

    public void testIifWithTuplesOfUnequalSizes() {
        assertQueryReturns(
            "WITH\n"
            + "MEMBER [Gender].agg "
            + "AS 'IIF(Measures.currentMember is [Measures].[Unit Sales], "
            + "([Store].[All Stores],[Gender].[All Gender],measures.[unit sales]),"
            + "([Store].[All Stores],[Gender].[All Gender]))', SOLVE_ORDER = 4 "
            + "SELECT {[Measures].[unit sales]} ON 0, "
            + "{{[Gender].[Gender].MEMBERS},{([Gender].agg)}} on 1 FROM sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Gender].[F]}\n"
            + "{[Gender].[M]}\n"
            + "{[Gender].[agg]}\n"
            + "Row #0: 131,558\n"
            + "Row #1: 135,215\n"
            + "Row #2: 266,773\n");
    }

    public void testIifWithTuplesOfUnequalSizesAndOrder() {
        assertQueryReturns(
            "WITH\n"
            + "MEMBER [Gender].agg "
            + "AS 'IIF(Measures.currentMember is [Measures].[Unit Sales], "
            + "([Store].[All Stores],[Gender].[M],measures.[unit sales]),"
            + "([Gender].[M],[Store].[All Stores]))', SOLVE_ORDER = 4 "
            + "SELECT {[Measures].[unit sales]} ON 0, "
            + "{{[Gender].[Gender].MEMBERS},{([Gender].agg)}} on 1 FROM sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Gender].[F]}\n"
            + "{[Gender].[M]}\n"
            + "{[Gender].[agg]}\n"
            + "Row #0: 131,558\n"
            + "Row #1: 135,215\n"
            + "Row #2: 135,215\n");
    }

    public void testEmptyAggregationListDueToFilterDoesNotThrowException() {
        propSaver.set(props.IgnoreMeasureForNonJoiningDimension, true);
        assertQueryReturns(
            "WITH \n"
            + "MEMBER [GENDER].[AGG] "
            + "AS 'AGGREGATE(FILTER([S1], (NOT ISEMPTY([MEASURES].[STORE SALES]))))' "
            + "SET [S1] "
            + "AS 'CROSSJOIN({[GENDER].[GENDER].MEMBERS},{[STORE].[CANADA].CHILDREN})' "
            + "SELECT\n"
            + "{[MEASURES].[STORE SALES]} ON COLUMNS,\n"
            + "{[GENDER].[AGG]} ON ROWS\n"
            + "FROM [WAREHOUSE AND SALES]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Sales]}\n"
            + "Axis #2:\n"
            + "{[Gender].[AGG]}\n"
            + "Row #0: \n");
    }

    /**
     * Testcase for Pentaho bug
     * <a href="http://jira.pentaho.com/browse/BISERVER-1323">BISERVER-1323</a>,
     * empty SQL query generated when crossjoining more than two sets each
     * containing just the 'all' member.
     */
    public void testEmptySqlBug() {
        final String expectedResult =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Store].[All Stores], [Product].[All Products], [Customers].[All Customers]}\n"
            + "Row #0: 266,773\n";
        assertQueryReturns(
            "select {[Measures].[Unit Sales]} ON COLUMNS, "
            + "NON EMPTY Crossjoin({[Store].[All Stores]}"
            + ", Crossjoin({[Product].[All Products]}, {[Customers].[All Customers]})) ON ROWS "
            + "from [Sales]",
            expectedResult);
        // without NON EMPTY
        assertQueryReturns(
            "select {[Measures].[Unit Sales]} ON COLUMNS, "
            + "  Crossjoin({[Store].[All Stores]}"
            + ", Crossjoin({[Product].[All Products]}, {[Customers].[All Customers]})) ON ROWS "
            + "from [Sales]",
            expectedResult);
        // using * operator
        assertQueryReturns(
            "select {[Measures].[Unit Sales]} ON COLUMNS, "
            + "NON EMPTY [Store].[All Stores] "
            + " * [Product].[All Products]"
            + " * [Customers].[All Customers] ON ROWS "
            + "from [Sales]",
            expectedResult);
        // combining tuple
        assertQueryReturns(
            "select {[Measures].[Unit Sales]} ON COLUMNS, "
            + "NON EMPTY [Store].[All Stores] "
            + " * {([Product].[All Products],"
            + "     [Customers].[All Customers])} ON ROWS "
            + "from [Sales]",
            expectedResult);
        // combining two members with tuple
        final String expectedResult4 =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Store].[All Stores], [Product].[All Products], [Customers].[All Customers], [Gender].[All Gender]}\n"
            + "Row #0: 266,773\n";
        assertQueryReturns(
            "select {[Measures].[Unit Sales]} ON COLUMNS, "
            + "NON EMPTY [Store].[All Stores] "
            + " * [Product].[All Products]"
            + " * {([Customers].[All Customers], [Gender].[All Gender])} ON ROWS "
            + "from [Sales]",
            expectedResult4);
        assertQueryReturns(
            "select {[Measures].[Unit Sales]} ON COLUMNS, "
            + "NON EMPTY [Store].[All Stores] "
            + " * {([Product].[All Products], [Customers].[All Customers])}"
            + " * [Gender].[All Gender] ON ROWS "
            + "from [Sales]",
            expectedResult4);
        assertQueryReturns(
            "select {[Measures].[Unit Sales]} ON COLUMNS, "
            + "NON EMPTY {([Store].[All Stores], [Product].[All Products])}"
            + " * [Customers].[All Customers]"
            + " * [Gender].[All Gender] ON ROWS "
            + "from [Sales]",
            expectedResult4);
    }

    /**
     * Tests bug <a href="http://jira.pentaho.com/browse/MONDRIAN-7">MONDRIAN-7,
     * "Heterogeneous axis gives wrong results"</a>. The bug is a misnomer;
     * heterogeneous axes should give an error.
     */
    public void testHeterogeneousAxis() {
        // SSAS2005 gives error:
        //   Query (1, 8) Two sets specified in the  function have different
        //   dimensionality.
        assertQueryThrows(
            "select {[Measures].[Unit Sales], [Gender].Members} on 0,\n"
            + " [Store].[USA].Children on 1\n"
            + "from [Sales]",
            "All arguments to function '{}' must have same hierarchy.");
        assertQueryThrows(
            "select {[Marital Status].Members, [Gender].Members} on 0,\n"
            + " [Store].[USA].Children on 1\n"
            + "from [Sales]",
            "All arguments to function '{}' must have same hierarchy.");
    }

    /**
     * Tests hierarchies of the same dimension on different axes.
     */
    public void testHierarchiesOfSameDimensionOnDifferentAxes() {
        if (!MondrianProperties.instance().SsasCompatibleNaming.get()) {
            return;
        }
        assertQueryReturns(
            "select [Time].[Year].Members on columns,\n"
            + "[Time].[Weekly].[1997].[6].Children on rows\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time].[1997]}\n"
            + "{[Time].[1998]}\n"
            + "Axis #2:\n"
            + "{[Time].[Weekly].[1997].[6].[1]}\n"
            + "{[Time].[Weekly].[1997].[6].[26]}\n"
            + "{[Time].[Weekly].[1997].[6].[27]}\n"
            + "{[Time].[Weekly].[1997].[6].[28]}\n"
            + "{[Time].[Weekly].[1997].[6].[29]}\n"
            + "{[Time].[Weekly].[1997].[6].[30]}\n"
            + "{[Time].[Weekly].[1997].[6].[31]}\n"
            + "Row #0: 404\n"
            + "Row #0: \n"
            + "Row #1: 593\n"
            + "Row #1: \n"
            + "Row #2: 422\n"
            + "Row #2: \n"
            + "Row #3: 382\n"
            + "Row #3: \n"
            + "Row #4: 731\n"
            + "Row #4: \n"
            + "Row #5: \n"
            + "Row #5: \n"
            + "Row #6: \n"
            + "Row #6: \n");
    }

    /**
     * Test case for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1432">MONDRIAN-1432,
     * "ArrayIndexOutOfBoundsException in
     * DenseObjectSegmentDataset.getObject"</a>.
     */
    public void testMondrian1432() {
        TestContext testContext = getTestContext().createSubstitutingCube(
            "Sales",
            null,
            "<Measure name='zero' aggregator='sum'>\n"
            + "  <MeasureExpression>\n"
            + "  <SQL dialect='generic'>\n"
            + "    0"
            + "  </SQL></MeasureExpression></Measure>",
            null, null);
        testContext.assertQueryReturns(
            "select "
            + "Crossjoin([Gender].[Gender].Members, [Measures].[zero]) ON COLUMNS\n"
            + "from [Sales] "
            + "  \n",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Gender].[F], [Measures].[zero]}\n"
            + "{[Gender].[M], [Measures].[zero]}\n"
            + "Row #0: 0\n"
            + "Row #0: 0\n");
        testContext.assertQueryReturns(
            "select [Measures].[zero] ON COLUMNS,\n"
            + "  {[Gender].[All Gender]}  ON ROWS\n"
            + "from [Sales] "
            + " ",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[zero]}\n"
            + "Axis #2:\n"
            + "{[Gender].[All Gender]}\n"
            + "Row #0: 0\n");
    }

    public void testMondrian1432_ZeroAxisSegment() {
        TestContext testContext = getTestContext().create(
            null,
            "<Cube name='FooBarZerOneAnything'>\n"
            + "  <Table name='sales_fact_1997'/>\n"
            + "  <Dimension name='Gender' foreignKey='customer_id'>\n"
            + "    <Hierarchy hasAll='true' allMemberName='All Gender' primaryKey='customer_id'>\n"
            + "      <Table name='customer'/>\n"
            + "      <Level name='Gender' column='gender' uniqueMembers='true'/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>"
            + "<Measure name='zero' aggregator='sum'>\n"
            + "  <MeasureExpression>\n"
            + "  <SQL dialect='generic'>\n"
            + "    0"
            + "  </SQL></MeasureExpression></Measure>"
            + "</Cube>",
            null, null, null, null);

        testContext.assertQueryReturns(
            "select "
            + "Crossjoin([Gender].[Gender].Members, [Measures].[zero]) ON COLUMNS\n"
            + "from [FooBarZerOneAnything] ",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Gender].[F], [Measures].[zero]}\n"
            + "{[Gender].[M], [Measures].[zero]}\n"
            + "Row #0: 0\n"
            + "Row #0: 0\n");
        testContext.assertQueryReturns(
            "select [Measures].[zero] ON COLUMNS,\n"
            + "  {[Gender].[All Gender]}  ON ROWS\n"
            + "from [FooBarZerOneAnything] ",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[zero]}\n"
            + "Axis #2:\n"
            + "{[Gender].[All Gender]}\n"
            + "Row #0: 0\n");
    }

    /**
     * A simple user-defined function which adds one to its argument, but
     * sleeps 1 ms before doing so.
     */
    public static class SleepUdf implements UserDefinedFunction {
        public String getName() {
            return "SleepUdf";
        }

        public String getDescription() {
            return "Returns its argument plus one but sleeps 1 ms first";
        }

        public Syntax getSyntax() {
            return Syntax.Function;
        }

        public Type getReturnType(Type[] parameterTypes) {
            return new NumericType();
        }

        public Type[] getParameterTypes() {
            return new Type[] {new NumericType()};
        }

        public Object execute(Evaluator evaluator, Argument[] arguments) {
            final Object argValue = arguments[0].evaluateScalar(evaluator);
            if (argValue instanceof Number) {
                try {
                    Thread.sleep(1);
                } catch (Exception ex) {
                    return null;
                }
                return ((Number) argValue).doubleValue() + 1;
            } else {
                // Argument might be a RuntimeException indicating that
                // the cache does not yet have the required cell value. The
                // function will be called again when the cache is loaded.
                return null;
            }
        }

        public String[] getReservedWords() {
            return null;
        }
    }

    public static class CountConcurrentUdf implements UserDefinedFunction {
        private static AtomicInteger count = new AtomicInteger();
        public String getName() {
            return "CountConcurrentUdf";
        }

        public String getDescription() {
            return "Counts the current number of threads using this thing.";
        }

        public Syntax getSyntax() {
            return Syntax.Function;
        }

        public Type getReturnType(Type[] parameterTypes) {
            return new NumericType();
        }

        public Type[] getParameterTypes() {
            return new Type[] {};
        }

        public Object execute(Evaluator evaluator, Argument[] arguments) {
            try {
                count.incrementAndGet();
                Thread.sleep(10000);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            } finally {
                count.decrementAndGet();
            }
            throw new Error("Leaving.");
        }

        static int getCount() {
            return count.get();
        }

        public String[] getReservedWords() {
            return null;
        }
    }

    /**
     * This unit test would cause connection leaks without a fix for bug
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-571">MONDRIAN-571,
     * "HighCardSqlTupleReader does not close SQL Connections"</a>.
     * It would be better if there was a way to verify that no leaks occurred in
     * the data source.
     */
    public void testHighCardSqlTupleReaderLeakingConnections() {
        assertQueryReturns(
            "WITH MEMBER [Measures].[NegativeSales] AS '- [Measures].[Store Sales]' "
            + "MEMBER [Product].[SameName] AS 'Aggregate(Filter("
            + "[Product].[Product Name].members,([Measures].[Store Sales] > 0)))' "
            + "MEMBER [Measures].[SameName] AS "
            + "'([Measures].[Store Sales],[Product].[SameName])' "
            + "select {[Measures].[Store Sales], [Measures].[NegativeSales], "
            + "[Measures].[SameName]} ON COLUMNS, "
            + "[Store].[Store Country].members ON ROWS "
            + "from [Sales] "
            + "where [Time].[1997]",
            "Axis #0:\n"
            + "{[Time].[1997]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Sales]}\n"
            + "{[Measures].[NegativeSales]}\n"
            + "{[Measures].[SameName]}\n"
            + "Axis #2:\n"
            + "{[Store].[Canada]}\n"
            + "{[Store].[Mexico]}\n"
            + "{[Store].[USA]}\n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #2: 565,238.13\n"
            + "Row #2: -565,238.13\n"
            + "Row #2: 565,238.13\n");
    }

     public void testZeroValuesAreNotTreatedAsNull() {
        String mdx =
            "select"
            + "  {"
            + "    ("
            + "      [Product].[All Products].[Food].[Produce].[Vegetables].[Fresh Vegetables].[Tell Tale].[Tell Tale Tomatos],"
            + "      [Warehouse].[All Warehouses].[USA].[WA].[Seattle].[Quality Warehousing and Trucking],"
            + "      [Store].[All Stores].[USA].[WA].[Seattle].[Store 15],"
            + "      [Time.Weekly].[All Time.Weeklys].[1997].[24].[3]"
            + "  )"
            + "  }"
            + "  on 0,"
            + "  [Measures].[units shipped] on 1"
            + " from warehouse";
        assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Product].[Food].[Produce].[Vegetables].[Fresh Vegetables]"
            + ".[Tell Tale].[Tell Tale Tomatos], "
            + "[Warehouse].[USA].[WA].[Seattle].[Quality Warehousing and "
            + "Trucking], "
            + "[Store].[USA].[WA].[Seattle].[Store 15], "
            + "[Time].[Weekly].[1997].[24].[3]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Units Shipped]}\n"
            + "Row #0: .0\n");
    }

    public void testDirectMemberReferenceOnDimensionWithCalculationsDefined() {
        TestContext testContext = TestContext.instance().createSubstitutingCube(
            "Sales",
            null,
            "<CalculatedMember dimension=\"Gender\" visible=\"true\" name=\"last\">"
            + "<Formula>([Gender].LastChild)</Formula>"
            + "</CalculatedMember>");
        testContext.assertQueryReturns(
            "select {[Gender].[M]} on 0 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Gender].[M]}\n"
            + "Row #0: 135,215\n");
    }

    public void testExplain() throws SQLException {
        OlapConnection connection =
            TestContext.instance().getOlap4jConnection();
        final OlapStatement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery(
            "explain plan for\n"
            + "select [Measures].[Unit Sales] on 0,\n"
            + "  Filter([Product].Children, [Measures].[Unit Sales] > 100) on 1\n"
            + "from [Sales]");
        assertTrue(resultSet.next());
        assertEquals(1, resultSet.getMetaData().getColumnCount());
        assertEquals("PLAN", resultSet.getMetaData().getColumnName(1));
        assertEquals(Types.VARCHAR, resultSet.getMetaData().getColumnType(1));
        String s = resultSet.getString(1);
        TestContext.assertEqualsVerbose(
            "Axis (COLUMNS):\n"
            + "SetListCalc(name=SetListCalc, class=class mondrian.olap.fun.SetFunDef$SetListCalc, type=SetType<MemberType<member=[Measures].[Unit Sales]>>, resultStyle=MUTABLE_LIST)\n"
            + "    2(name=2, class=class mondrian.olap.fun.SetFunDef$SetListCalc$2, type=MemberType<member=[Measures].[Unit Sales]>, resultStyle=VALUE)\n"
            + "        Literal(name=Literal, class=class mondrian.calc.impl.ConstantCalc, type=MemberType<member=[Measures].[Unit Sales]>, resultStyle=VALUE_NOT_NULL, value=[Measures].[Unit Sales])\n"
            + "\n"
            + "Axis (ROWS):\n"
            + "ImmutableIterCalc(name=ImmutableIterCalc, class=class mondrian.olap.fun.FilterFunDef$ImmutableIterCalc, type=SetType<MemberType<hierarchy=[Product]>>, resultStyle=ITERABLE)\n"
            + "    Children(name=Children, class=class mondrian.olap.fun.BuiltinFunTable$22$1, type=SetType<MemberType<hierarchy=[Product]>>, resultStyle=LIST)\n"
            + "        CurrentMemberFixed(hierarchy=[Product], name=CurrentMemberFixed, class=class mondrian.olap.fun.HierarchyCurrentMemberFunDef$FixedCalcImpl, type=MemberType<hierarchy=[Product]>, resultStyle=VALUE)\n"
            + "    >(name=>, class=class mondrian.olap.fun.BuiltinFunTable$63$1, type=BOOLEAN, resultStyle=VALUE)\n"
            + "        MemberValueCalc(name=MemberValueCalc, class=class mondrian.calc.impl.MemberValueCalc, type=SCALAR, resultStyle=VALUE)\n"
            + "            Literal(name=Literal, class=class mondrian.calc.impl.ConstantCalc, type=MemberType<member=[Measures].[Unit Sales]>, resultStyle=VALUE_NOT_NULL, value=[Measures].[Unit Sales])\n"
            + "        Literal(name=Literal, class=class mondrian.calc.impl.ConstantCalc, type=NUMERIC, resultStyle=VALUE_NOT_NULL, value=100.0)\n"
            + "\n",
            s);
    }

    public void testExplainComplex() throws SQLException {
        OlapConnection connection =
            TestContext.instance().getOlap4jConnection();
        final OlapStatement statement = connection.createStatement();
        final String mdx =
            "with member [Time].[Time].[1997].[H1] as\n"
            + "    Aggregate({[Time].[1997].[Q1], [Time].[1997].[Q2]})\n"
            + "  member [Measures].[Store Margin] as\n"
            + "    [Measures].[Store Sales] - [Measures].[Store Cost],\n"
            + "      format_string =\n"
            + "        iif(\n"
            + "          [Measures].[Unit Sales] > 50000,\n"
            + "          \"\\<b\\>#.00\\<\\/b\\>\",\n"
            + "           \"\\<i\\>#.00\\<\\/i\\>\")\n"
            + "  set [Hi Val Products] as\n"
            + "    Filter(\n"
            + "      Descendants([Product].[Drink], , LEAVES),\n"
            + "     [Measures].[Unit Sales] > 100)\n"
            + "select\n"
            + "  {[Measures].[Unit Sales], [Measures].[Store Margin]} on 0,\n"
            + "  [Hi Val Products] * [Marital Status].Members on 1\n"
            + "from [Sales]\n"
            + "where [Gender].[F]";

        // Plan before execution.
        final ResultSet resultSet =
            statement.executeQuery("explain plan for\n" + mdx);
        assertTrue(resultSet.next());
        String s = resultSet.getString(1);
        TestContext.assertEqualsVerbose(
            "Axis (FILTER):\n"
            + "SetListCalc(name=SetListCalc, class=class mondrian.olap.fun.SetFunDef$SetListCalc, type=SetType<MemberType<member=[Gender].[F]>>, resultStyle=MUTABLE_LIST)\n"
            + "    ()(name=(), class=class mondrian.olap.fun.SetFunDef$SetListCalc$2, type=MemberType<member=[Gender].[F]>, resultStyle=VALUE)\n"
            + "        Literal(name=Literal, class=class mondrian.calc.impl.ConstantCalc, type=MemberType<member=[Gender].[F]>, resultStyle=VALUE_NOT_NULL, value=[Gender].[F])\n"
            + "\n"
            + "Axis (COLUMNS):\n"
            + "SetListCalc(name=SetListCalc, class=class mondrian.olap.fun.SetFunDef$SetListCalc, type=SetType<MemberType<member=[Measures].[Unit Sales]>>, resultStyle=MUTABLE_LIST)\n"
            + "    2(name=2, class=class mondrian.olap.fun.SetFunDef$SetListCalc$2, type=MemberType<member=[Measures].[Unit Sales]>, resultStyle=VALUE)\n"
            + "        Literal(name=Literal, class=class mondrian.calc.impl.ConstantCalc, type=MemberType<member=[Measures].[Unit Sales]>, resultStyle=VALUE_NOT_NULL, value=[Measures].[Unit Sales])\n"
            + "    2(name=2, class=class mondrian.olap.fun.SetFunDef$SetListCalc$2, type=MemberType<member=[Measures].[Store Margin]>, resultStyle=VALUE)\n"
            + "        Literal(name=Literal, class=class mondrian.calc.impl.ConstantCalc, type=MemberType<member=[Measures].[Store Margin]>, resultStyle=VALUE_NOT_NULL, value=[Measures].[Store Margin])\n"
            + "\n"
            + "Axis (ROWS):\n"
            + "CrossJoinIterCalc(name=CrossJoinIterCalc, class=class mondrian.olap.fun.CrossJoinFunDef$CrossJoinIterCalc, type=SetType<TupleType<MemberType<member=[Product].[Drink]>, MemberType<hierarchy=[Marital Status]>>>, resultStyle=ITERABLE)\n"
            + "    1(name=1, class=class mondrian.mdx.NamedSetExpr$1, type=SetType<MemberType<member=[Product].[Drink]>>, resultStyle=ITERABLE)\n"
            + "    Members(name=Members, class=class mondrian.olap.fun.BuiltinFunTable$27$1, type=SetType<MemberType<hierarchy=[Marital Status]>>, resultStyle=MUTABLE_LIST)\n"
            + "        Literal(name=Literal, class=class mondrian.calc.impl.ConstantCalc, type=HierarchyType<hierarchy=[Marital Status]>, resultStyle=VALUE_NOT_NULL, value=[Marital Status])\n"
            + "\n",
            s);

        // Plan after execution, including profiling.
        final String[] strings = {null, null};
        ((mondrian.server.Statement) statement).enableProfiling(
            new ProfileHandler() {
                public void explain(String plan, QueryTiming timing) {
                    strings[0] = plan;
                    strings[1] = String.valueOf(timing);
                }
            }
        );
        final CellSet cellSet = statement.executeOlapQuery(mdx);
        new RectangularCellSetFormatter(true).format(
            cellSet, new PrintWriter(new StringWriter()));
        cellSet.close();
        final String actual =
            strings[0].replaceAll(
                "callMillis=[0-9]+",
                "callMillis=nnn")
            .replaceAll(
                "[0-9]+ms",
                "nnnms");
        TestContext.assertEqualsVerbose(
            "Axis (FILTER):\n"
            + "SetListCalc(name=SetListCalc, class=class mondrian.olap.fun.SetFunDef$SetListCalc, type=SetType<MemberType<member=[Gender].[F]>>, resultStyle=MUTABLE_LIST, callCount=2, callMillis=nnn, elementCount=2, elementSquaredCount=2)\n"
            + "    ()(name=(), class=class mondrian.olap.fun.SetFunDef$SetListCalc$2, type=MemberType<member=[Gender].[F]>, resultStyle=VALUE)\n"
            + "        Literal(name=Literal, class=class mondrian.calc.impl.ConstantCalc, type=MemberType<member=[Gender].[F]>, resultStyle=VALUE_NOT_NULL, value=[Gender].[F], callCount=2, callMillis=nnn)\n"
            + "\n"
            + "Axis (COLUMNS):\n"
            + "SetListCalc(name=SetListCalc, class=class mondrian.olap.fun.SetFunDef$SetListCalc, type=SetType<MemberType<member=[Measures].[Unit Sales]>>, resultStyle=MUTABLE_LIST, callCount=2, callMillis=nnn, elementCount=4, elementSquaredCount=8)\n"
            + "    2(name=2, class=class mondrian.olap.fun.SetFunDef$SetListCalc$2, type=MemberType<member=[Measures].[Unit Sales]>, resultStyle=VALUE)\n"
            + "        Literal(name=Literal, class=class mondrian.calc.impl.ConstantCalc, type=MemberType<member=[Measures].[Unit Sales]>, resultStyle=VALUE_NOT_NULL, value=[Measures].[Unit Sales], callCount=2, callMillis=nnn)\n"
            + "    2(name=2, class=class mondrian.olap.fun.SetFunDef$SetListCalc$2, type=MemberType<member=[Measures].[Store Margin]>, resultStyle=VALUE)\n"
            + "        Literal(name=Literal, class=class mondrian.calc.impl.ConstantCalc, type=MemberType<member=[Measures].[Store Margin]>, resultStyle=VALUE_NOT_NULL, value=[Measures].[Store Margin], callCount=2, callMillis=nnn)\n"
            + "\n"
            + "Axis (ROWS):\n"
            + "CrossJoinIterCalc(name=CrossJoinIterCalc, class=class mondrian.olap.fun.CrossJoinFunDef$CrossJoinIterCalc, type=SetType<TupleType<MemberType<member=[Product].[Drink]>, MemberType<hierarchy=[Marital Status]>>>, resultStyle=ITERABLE, callCount=2, callMillis=nnn, elementCount=0, elementSquaredCount=0)\n"
            + "    1(name=1, class=class mondrian.mdx.NamedSetExpr$1, type=SetType<MemberType<member=[Product].[Drink]>>, resultStyle=ITERABLE)\n"
            + "    Members(name=Members, class=class mondrian.olap.fun.BuiltinFunTable$27$1, type=SetType<MemberType<hierarchy=[Marital Status]>>, resultStyle=MUTABLE_LIST)\n"
            + "        Literal(name=Literal, class=class mondrian.calc.impl.ConstantCalc, type=HierarchyType<hierarchy=[Marital Status]>, resultStyle=VALUE_NOT_NULL, value=[Marital Status], callCount=2, callMillis=nnn)\n"
            + "\n",
            actual);

        assertTrue(
            strings[1],
            strings[1].contains(
                "SqlStatement-SqlTupleReader.readTuples [[Product].[Product "
                + "Category]] invoked 1 times for total of "));
    }

    public void testExplainInvalid() throws SQLException {
        OlapConnection connection =
            TestContext.instance().getOlap4jConnection();
        final OlapStatement statement = connection.createStatement();
        try {
            final ResultSet resultSet = statement.executeQuery(
                "select\n"
                + "  {[Measures].[Unit Sales], [Measures].[Store Margin]} on 0,\n"
                + "  [Hi Val Products] * [Marital Status].Members on 1\n"
                + "from [Sales]\n"
                + "where [Gender].[F]");
            fail("expected error, got " + resultSet);
        } catch (SQLException e) {
            TestContext.checkThrowable(
                e,
                "MDX object '[Measures].[Store Margin]' not found in cube 'Sales'");
        }
    }

    /**
     * This is a test for MONDRIAN-1014. Executing a statement
     * twice concurrently would fail because the statement wasn't
     * cleaning up properly its execution context.
     */
    public void testConcurrentStatementRun() throws Exception {
        final OlapConnection olapConnection =
            TestContext.instance().getOlap4jConnection();

        final String mdxQuery =
            "select {TopCount([Customers].Members, 10, [Measures].[Unit Sales])} on columns from [Sales]";

        final ExecutorService es =
            Executors.newCachedThreadPool(
                new ThreadFactory() {
                    public Thread newThread(Runnable r) {
                        final Thread thread =
                            Executors.defaultThreadFactory().newThread(r);
                        thread.setName(
                            "mondrian.test.BasicQueryTest.testConcurrentStatementRun");
                        thread.setDaemon(true);
                        return thread;
                    }
                });


        final OlapStatement stmt = olapConnection.createStatement();

        es.submit(
            new Callable<CellSet>() {
                public CellSet call() throws Exception {
                    return stmt.executeOlapQuery(mdxQuery);
                }
            });

        // Give some time to the first query so it enters a "running" state.
        Thread.sleep(100);

        es.submit(
            new Callable<CellSet>() {
                public CellSet call() throws Exception {
                    return stmt.executeOlapQuery(mdxQuery);
                }
            }).get();

        es.shutdownNow();
    }

    public void testRollup() {
        switch (2) {
        case 0:
        assertQueryReturns(
            "select [Gender].Children * [Product].Children on 0\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Gender].[F], [Product].[Drink]}\n"
            + "{[Gender].[F], [Product].[Food]}\n"
            + "{[Gender].[F], [Product].[Non-Consumable]}\n"
            + "{[Gender].[M], [Product].[Drink]}\n"
            + "{[Gender].[M], [Product].[Food]}\n"
            + "{[Gender].[M], [Product].[Non-Consumable]}\n"
            + "Row #0: 12,202\n"
            + "Row #0: 94,814\n"
            + "Row #0: 24,542\n"
            + "Row #0: 12,395\n"
            + "Row #0: 97,126\n"
            + "Row #0: 25,694\n");
        // now, should be able to answer this one by rolling up gender
        assertQueryReturns(
            "select [Product].Children on 0\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Product].[Drink]}\n"
            + "{[Product].[Food]}\n"
            + "{[Product].[Non-Consumable]}\n"
            + "Row #0: 24,597\n"
            + "Row #0: 191,940\n"
            + "Row #0: 50,236\n");
            break;
        case 1:
            assertQueryReturns(
                "select [Gender].[M] * [Product].Children on 0\n"
                + "from [Sales]",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Gender].[M], [Product].[Drink]}\n"
                + "{[Gender].[M], [Product].[Food]}\n"
                + "{[Gender].[M], [Product].[Non-Consumable]}\n"
                + "Row #0: 12,395\n"
                + "Row #0: 97,126\n"
                + "Row #0: 25,694\n");
            assertQueryReturns(
                "select [Gender].[F] * [Product].Children on 0\n"
                + "from [Sales]",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Gender].[F], [Product].[Drink]}\n"
                + "{[Gender].[F], [Product].[Food]}\n"
                + "{[Gender].[F], [Product].[Non-Consumable]}\n"
                + "Row #0: 12,202\n"
                + "Row #0: 94,814\n"
                + "Row #0: 24,542\n");
        // now, should be able to answer this one by rolling up gender
        assertQueryReturns(
            "select [Product].Children on 0\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Product].[Drink]}\n"
            + "{[Product].[Food]}\n"
            + "{[Product].[Non-Consumable]}\n"
            + "Row #0: 24,597\n"
            + "Row #0: 191,940\n"
            + "Row #0: 50,236\n");
            break;
        case 2:
            String[] genders = {"M", "F"};
            String[] states = {"USA", "Canada", "Mexico"};
            for (String state : states) {
                for (String gender : genders) {
                    getTestContext().executeQuery(
                        "select [Gender].[" + gender
                        + "] * [Store].[" + state
                        + "] * [Product].Children on 0\n"
                        + "from [Sales]");
                }
            }
        // now, should be able to answer this one by rolling up gender
        assertQueryReturns(
            "select [Product].Children on 0\n"
            + "from [Sales]",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Product].[Drink]}\n"
            + "{[Product].[Food]}\n"
            + "{[Product].[Non-Consumable]}\n"
            + "Row #0: 24,597\n"
            + "Row #0: 191,940\n"
            + "Row #0: 50,236\n");
            break;
        case 3:
            // Test case for MONDRIAN-1021.
            // First, read {Mexico}.
            // Now, query {USA, Canada, Mexico}. Should just read {USA, Canada}.
            break;
        case 4:
        }
    }

    /**
     * Unit test for {@link StatisticsProvider} and implementations
     * {@link JdbcStatisticsProvider} and
     * {@link SqlStatisticsProvider}.
     */
    public void testStatistics() {
        final String product =
            getTestContext().getDialect().getDatabaseProduct().name();
        final String dialectClassName =
            getTestContext().getDialect().getClass().getName();

        propSaver.set(
            new StringProperty(
                MondrianProperties.instance(),
                MondrianProperties.instance().StatisticsProviders.getPath()
                + "."
                + product,
                null),
            MyJdbcStatisticsProvider.class.getName()
            + ","
            + SqlStatisticsProvider.class.getName());
        final TestContext testContext = getTestContext().withFreshConnection();
        try {
            testContext.assertSimpleQuery();
            // bypass dialect cache and always get a fresh dialect instance
            // with our custom providers
            Dialect dialect =
                DialectManager.createDialect(
                    testContext.getConnection().getDataSource(),
                    null,
                    dialectClassName);
            final List<StatisticsProvider> statisticsProviders =
                dialect.getStatisticsProviders();
            assertEquals(2, statisticsProviders.size());
            assertTrue(
                statisticsProviders.get(0) instanceof MyJdbcStatisticsProvider);
            assertTrue(
                statisticsProviders.get(1) instanceof SqlStatisticsProvider);

            for (StatisticsProvider statisticsProvider : statisticsProviders) {
                int rowCount =
                    statisticsProvider.getTableCardinality(
                        dialect,
                        testContext.getConnection().getDataSource(),
                        null,
                        null,
                        "customer",
                        new Execution(
                            ((RolapSchema)
                                testContext.getConnection().getSchema())
                                .getInternalConnection()
                                .getInternalStatement(),
                            0));
                if (statisticsProvider instanceof SqlStatisticsProvider) {
                    assertTrue(
                        "Row count estimate: " + rowCount + " (actual 10281)",
                        rowCount > 10000 && rowCount < 15000);
                }

                int valueCount =
                    statisticsProvider.getColumnCardinality(
                        dialect,
                        testContext.getConnection().getDataSource(),
                        null,
                        null,
                        "customer",
                        "gender",
                        new Execution(
                            ((RolapSchema)
                                testContext.getConnection().getSchema())
                                .getInternalConnection().getInternalStatement(),
                            0));
                assertTrue(
                    "Value count estimate: " + valueCount + " (actual 2)",
                    statisticsProvider instanceof JdbcStatisticsProvider
                        ? valueCount == -1
                        : valueCount == 2);
            }
        } finally {
            testContext.close();
        }
    }

    public void testResultLimit() throws Exception {
        propSaver.set(
            MondrianProperties.instance().ResultLimit,
            1000);
        assertAxisThrows(
            "CrossJoin([Product].[Brand Name].Members, [Gender].[Gender].Members)",
            "Mondrian Error:Number of cell results to be read exceeded limit of (1,000)");
        propSaver.set(
            MondrianProperties.instance().ResultLimit,
            5000);
        executeQuery(
            "select CrossJoin([Product].[Brand Name].Members, [Gender].[Gender].Members) on columns from [Sales]");
    }

    /**
     * This is a test for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1161">
     * MONDRIAN-1161</a>. It verifies that two queries can run
     * at the same time.
     */
    public void testConcurrentStatementRun_2() throws Exception {
        // timeout is issued after 2 seconds so the test query needs to
        // run for at least that long; it will because the query references
        // a Udf that has a 1 ms sleep in it; and there are enough rows
        // in the result that the Udf should execute > 2000 times
        final TestContext tc = TestContext.instance().create(
            null,
            null,
            null,
            null,
            "<UserDefinedFunction name='CountConcurrentUdf' className='"
            + CountConcurrentUdf.class.getName()
            + "'/>",
            null);

        final String query =
            "WITH\n"
            + "  MEMBER [Measures].[CountyThigny]\n"
            + "    AS 'CountConcurrentUdf()'\n"
            + "SELECT {[Measures].[CountyThigny]} ON COLUMNS,\n"
            + "  {[Product].members} ON ROWS\n"
            + "FROM [Sales]";

        final ExecutorService es =
            Executors.newCachedThreadPool(
                new ThreadFactory() {
                    public Thread newThread(Runnable r) {
                        final Thread thread =
                            Executors.defaultThreadFactory().newThread(r);
                        thread.setName(
                            "mondrian.test.BasicQueryTest.testConcurrentStatementRun_2");
                        thread.setDaemon(true);
                        return thread;
                    }
                });

        // Submit a query twice.
        Future<Result> task1 = es.submit(
            new Callable<Result>() {
                public Result call() throws Exception {
                    return tc.executeQuery(query);
                }
            });
        Future<Result> task2 = es.submit(
            new Callable<Result>() {
                public Result call() throws Exception {
                    return tc.executeQuery(query);
                }
            });

        // Let the backend run a bit
        Thread.sleep(5000);

        // There should be 2 queries running.
        try {
            assertEquals(2, CountConcurrentUdf.getCount());
        } finally {
            // cleanup and leave.
            task1.cancel(true);
            task2.cancel(true);
            es.shutdownNow();
        }
    }

    /**
     * Test for MONDRIAN-1560
     * Verifies that various references to a member resolve
     * correctly when case.sensitive=false
     */
    public void testCaseInsensitiveResolution() {
        propSaver.set(MondrianProperties.instance().CaseSensitive, false);
        String [] equivalentMemberNames =
            {
             "gender.gender.F",
             "gender.gender.f",
             "gender.[All gender].F",
             "gender.[All gender].f"
            };
        for (String memberName : equivalentMemberNames) {
            assertQueryReturns(
                "select " + memberName + " on 0 from sales",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Gender].[F]}\n"
                + "Row #0: 131,558\n");
        }
        // also verify case.sensitive=true is honored
        propSaver.set(MondrianProperties.instance().CaseSensitive, true);
        String [] wrongCase =
            {
                "gender.gender.f",
                "gender.[All gender].f"
            };
        for (String memberName : wrongCase) {
            assertExprThrows(
                "select " + memberName + " on 0 from sales",
                "Failed to parse query");
        }
        propSaver.set(MondrianProperties.instance().CaseSensitive, false);
    }

    /**
     * Dummy statistics provider for
     * {@link mondrian.test.BasicQueryTest#testStatistics()}.
     */
    public static class MyJdbcStatisticsProvider
        extends JdbcStatisticsProvider
    {
    }

    /**
     * Test for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1506">MONDRIAN-1506</a>
     *
     * <p>This is a test for a concurrency problem in the old Query API.
     * It also makes sure that we do not leak a future to a segment
     * which we are about to cancel.
     *
     * <p>It is disabled by default because it can make the JVM crash
     * pretty hard if it is run as part of the full test suite. Something to
     * do with stack heap problems. Probably because the test has to run on
     * multiple threads at the same time.
     */
    public void testMondrian1506() throws Exception {
        // First test. Run two queries in parallel. Cancel one.
        // The exception should appear on thread 1 and thread 2
        // should succeed.
        runMondrian1506(
            new Mondrian1506Lambda() {
                public void run(
                    ExecutorService exec,
                    Query q1,
                    AtomicBoolean fail,
                    AtomicBoolean success,
                    Runnable r1,
                    Runnable r2) throws Exception
                {
                    getTestContext().flushSchemaCache();

                    // Run the first query
                    exec.submit(r1);

                    // Wait a bit
                    Thread.sleep(500);

                    // Run the second immediatly
                    Future<?> f2 = exec.submit(r2);

                    // Wait a bit
                    Thread.sleep(500);

                    // Cancel the first
                    q1.cancel();

                    // Wait a bit for the cancelation of q1 to propagate.
                    Thread.sleep(3000);

                    // Make sure the first query didn't have time to finish.
                    assertTrue(fail.get());

                    // Wait for q2 to finish
                    while (!f2.isDone()) {
                        Thread.sleep(1000);
                    }

                    // Make sure the second query worked.
                    assertTrue(success.get());
                }
            });

        // Wait a bit to clean up stuff
        Thread.currentThread().sleep(1000);

        // Second test. Launch one query. Cancel and start another one right
        // after the call to cancel() returns. Same result as test 1.
        runMondrian1506(
            new Mondrian1506Lambda() {
                public void run(
                    ExecutorService exec,
                    Query q1,
                    AtomicBoolean fail,
                    AtomicBoolean success,
                    Runnable r1,
                    Runnable r2) throws Exception
                {
                    getTestContext().flushSchemaCache();

                    // Run the first query
                    exec.submit(r1);

                    // Wait a bit
                    Thread.sleep(500);

                    // Cancel the first
                    q1.cancel();

                    // Run the second immediatly
                    Future<?> f2 = exec.submit(r2);

                    // Wait a bit for the cancelation of q1 to propagate.
                    Thread.sleep(1000);

                    // Make sure the first query didn't have time to finish.
                    assertTrue(fail.get());

                    // Wait for q2 to finish
                    while (!f2.isDone()) {
                        Thread.sleep(1000);
                    }

                    // Make sure the second query worked.
                    assertTrue(success.get());
                }
            });
    }

    private interface Mondrian1506Lambda {
        void run(
            final ExecutorService exec,
            final Query q1,
            final AtomicBoolean fail,
            final AtomicBoolean success,
            final Runnable r1,
            final Runnable r2) throws Exception;
    }

    private void runMondrian1506(Mondrian1506Lambda lambda) throws Exception {
        if (getTestContext().getDialect().getDatabaseProduct()
            != Dialect.DatabaseProduct.MYSQL)
        {
            // This only works on MySQL because of Sleep()
            return;
        }
        final TestContext context =
            getTestContext().withSchema(
                "<Schema name=\"Foo\">\n"
                + "  <Cube name=\"Bar\">\n"
                + "    <Table name=\"warehouse\">\n"
                + "      <SQL>sleep(0.1) = 0</SQL>\n"
                + "    </Table>   \n"
                + " <Dimension name=\"Dim\">\n"
                + "   <Hierarchy hasAll=\"true\">\n"
                + "     <Level name=\"Level\" column=\"warehouse_id\"/>\n"
                + "      </Hierarchy>\n"
                + " </Dimension>\n"
                + " <Measure name=\"Measure\" aggregator=\"sum\">\n"
                + "   <MeasureExpression>\n"
                + "     <SQL>1</SQL>\n"
                + "   </MeasureExpression>\n"
                + " </Measure>\n"
                + "  </Cube>\n"
                + "</Schema>\n");

        final String mdx =
            "select {[Measures].[Measure]} on columns from [Bar]";

        // A service to execute stuff in the background.
        final ExecutorService exec =
            Util.getExecutorService(
                2,
                2,
                1000,
                "BasicQueryTest.testMondrian1506",
                new RejectedExecutionHandler() {
                    public void rejectedExecution(
                        Runnable r,
                        ThreadPoolExecutor executor)
                    {
                        throw new RuntimeException();
                    }
            });

        // We are testing the old Query API.
        final Connection connection =
            context.getConnection();
        final Query q1 =
            connection.parseQuery(mdx);
        final Query q2 =
            connection.parseQuery(mdx);

        // Some flags to test.
        final AtomicBoolean fail =
            new AtomicBoolean(false);
        final AtomicBoolean success =
            new AtomicBoolean(false);

        final Runnable r1 =
            new Runnable() {
                public void run() {
                    try {
                        connection.execute(q1);
                    } catch (Throwable t) {
                        fail.set(true);
                    }
                }
            };
        final Runnable r2 =
            new Runnable() {
                public void run() {
                    try {
                        connection.execute(q2);
                        success.set(true);
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                }
            };
        try {
            lambda.run(exec, q1, fail, success, r1, r2);
        } finally {
            exec.shutdownNow();
        }
    }

    /**
     * This is a test for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1605">MONDRIAN-1605</a>
     *
     * <p>When a dense object has only null values, it threw a AIOOBE
     * because the offset resolved to 0 and was used to fetch data directly out
     * of the array.
     */
    public void testArrayIndexOutOfBoundsWithEmptySegment() {
        TestContext testContext =
            getTestContext().createSubstitutingCube(
                "Sales",
                null,
                "<Measure name='zero' aggregator='sum'>\n"
                + " <MeasureExpression>\n"
                + " <SQL dialect='generic'>\n"
                + " NULL"
                + " </SQL>"
                + " <SQL dialect='vertica'>\n"
                + " NULL::FLOAT"
                + " </SQL>"
                + "</MeasureExpression></Measure>",
                null, null);
        testContext.executeQuery(
            "select "
            + "Crossjoin([Gender].[Gender].Members, [Measures].[zero]) ON COLUMNS\n"
            + "from [Sales] "
            + " \n");

        // Some DBs return 0 when we ask for null. Like Oracle.
        final String returnedValue;
        switch (getTestContext().getDialect().getDatabaseProduct()) {
            case ORACLE:
                returnedValue = "0";
                break;
            default:
                returnedValue = "";
        }

        testContext.assertQueryReturns(
            "select [Measures].[zero] ON COLUMNS,\n"
            + " {[Gender].[All Gender]} ON ROWS\n"
            + "from [Sales] "
            + " ",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[zero]}\n"
            + "Axis #2:\n"
            + "{[Gender].[All Gender]}\n"
            + "Row #0: " + returnedValue + "\n");
    }

    /**
     * This test is disabled by default because we can't set the max number
     * of SQL threads dynamically. The test still works when run standalone.
     */
    public void _testSqlPoolAndQueue() throws Exception {
        // We use 10 SQL threads and the query needs about 30-ish.
        // If the bug exists, it'll fail.
        propSaver.set(
            propSaver.properties.SegmentCacheManagerNumberSqlThreads, 10);
        final String mdx =
            "with set [*NATIVE_CJ_SET] as 'NonEmptyCrossJoin([*BASE_MEMBERS__Promotion Media_], NonEmptyCrossJoin([*BASE_MEMBERS__Customers_], NonEmptyCrossJoin([*BASE_MEMBERS__Yearly Income_], NonEmptyCrossJoin([*BASE_MEMBERS__Gender_], NonEmptyCrossJoin([*BASE_MEMBERS__Education Level_], NonEmptyCrossJoin([*BASE_MEMBERS__Store Type_], [*BASE_MEMBERS__Promotions_]))))))'\n"
            + "  set [*BASE_MEMBERS__Gender_] as '[Gender].[Gender].Members'\n"
            + "  set [*SORTED_COL_AXIS] as 'Order([*CJ_COL_AXIS], [Promotion Media].CurrentMember.OrderKey, BASC)'\n"
            + "  set [*BASE_MEMBERS__Promotions_] as '{[Promotions].[No Promotion]}'\n"
            + "  set [*NATIVE_MEMBERS__Promotions_] as 'Generate([*NATIVE_CJ_SET], {[Promotions].CurrentMember})'\n"
            + "  set [*BASE_MEMBERS__Customers_] as '{[Customers].[USA].[WA].[Spokane], [Customers].[USA].[WA].[Olympia], [Customers].[USA].[WA].[Port Orchard], [Customers].[USA].[WA].[Bremerton], [Customers].[USA].[WA].[Puyallup], [Customers].[USA].[WA].[Yakima], [Customers].[USA].[WA].[Tacoma], [Customers].[USA].[WA].[Burien]}'\n"
            + "  set [*BASE_MEMBERS__Yearly Income_] as '[Yearly Income].[Yearly Income].Members'\n"
            + "  set [*SORTED_ROW_AXIS] as 'Order([*CJ_ROW_AXIS], [Customers].CurrentMember.OrderKey, BASC, Ancestor([Customers].CurrentMember, [Customers].[State Province]).OrderKey, BASC, [Yearly Income].CurrentMember.OrderKey, BASC, [Gender].CurrentMember.OrderKey, BASC, [Education Level].CurrentMember.OrderKey, BASC)'\n"
            + "  set [*NATIVE_MEMBERS__Store Type_] as 'Generate([*NATIVE_CJ_SET], {[Store Type].CurrentMember})'\n"
            + "  set [*CJ_COL_AXIS] as 'Generate([*NATIVE_CJ_SET], {[Promotion Media].CurrentMember})'\n"
            + "  set [*BASE_MEMBERS__Store Type_] as '{[Store Type].[Supermarket]}'\n"
            + "  set [*BASE_MEMBERS__Measures_] as '{[Measures].[Customer Count]}'\n"
            + "  set [*BASE_MEMBERS__Education Level_] as '{[Education Level].[Bachelors Degree], [Education Level].[Graduate Degree]}'\n"
            + "  set [*CJ_SLICER_AXIS] as 'Generate([*NATIVE_CJ_SET], {([Store Type].CurrentMember, [Promotions].CurrentMember)})'\n"
            + "  set [*CJ_ROW_AXIS] as 'Generate([*NATIVE_CJ_SET], {([Customers].CurrentMember, [Yearly Income].CurrentMember, [Gender].CurrentMember, [Education Level].CurrentMember)})'\n"
            + "  set [*BASE_MEMBERS__Promotion Media_] as '[Promotion Media].[Media Type].Members'\n"
            + "  member [Promotion Media].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate(Generate([*NATIVE_CJ_SET], {[Promotion Media].CurrentMember}))', SOLVE_ORDER = (- 104)\n"
            + "  member [Promotion Media].[*TOTAL_MEMBER_SEL~SUM] as 'Sum(Generate([*NATIVE_CJ_SET], {[Promotion Media].CurrentMember}))', SOLVE_ORDER = 96\n"
            + "  member [Yearly Income].[*DEFAULT_MEMBER] as '[Yearly Income].DefaultMember', SOLVE_ORDER = (- 400)\n"
            + "  member [Yearly Income].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate(Generate(Exists([*NATIVE_CJ_SET], {[Customers].CurrentMember}), {([Customers].CurrentMember, [Yearly Income].CurrentMember, [Gender].CurrentMember, [Education Level].CurrentMember)}))', SOLVE_ORDER = (- 101)\n"
            + "  member [Yearly Income].[*TOTAL_MEMBER_SEL~SUM] as 'Sum(Generate(Exists([*NATIVE_CJ_SET], {[Customers].CurrentMember}), {([Customers].CurrentMember, [Yearly Income].CurrentMember, [Gender].CurrentMember, [Education Level].CurrentMember)}))', SOLVE_ORDER = 99\n"
            + "  member [Gender].[*DEFAULT_MEMBER] as '[Gender].DefaultMember', SOLVE_ORDER = (- 400)\n"
            + "  member [Gender].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate(Generate(Exists([*NATIVE_CJ_SET], {([Customers].CurrentMember, [Yearly Income].CurrentMember)}), {([Customers].CurrentMember, [Yearly Income].CurrentMember, [Gender].CurrentMember, [Education Level].CurrentMember)}))', SOLVE_ORDER = (- 102)\n"
            + "  member [Gender].[*TOTAL_MEMBER_SEL~SUM] as 'Sum(Generate(Exists([*NATIVE_CJ_SET], {([Customers].CurrentMember, [Yearly Income].CurrentMember)}), {([Customers].CurrentMember, [Yearly Income].CurrentMember, [Gender].CurrentMember, [Education Level].CurrentMember)}))', SOLVE_ORDER = 98\n"
            + "  member [Education Level].[*DEFAULT_MEMBER] as '[Education Level].DefaultMember', SOLVE_ORDER = (- 400)\n"
            + "  member [Customers].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate(Generate([*NATIVE_CJ_SET], {([Customers].CurrentMember, [Yearly Income].CurrentMember, [Gender].CurrentMember, [Education Level].CurrentMember)}))', SOLVE_ORDER = (- 100)\n"
            + "  member [Customers].[*TOTAL_MEMBER_SEL~SUM] as 'Sum(Generate([*NATIVE_CJ_SET], {([Customers].CurrentMember, [Yearly Income].CurrentMember, [Gender].CurrentMember, [Education Level].CurrentMember)}))', SOLVE_ORDER = 100\n"
            + "select Union(Crossjoin({[Promotion Media].[*TOTAL_MEMBER_SEL~AGG]}, [*BASE_MEMBERS__Measures_]), Union(Crossjoin({[Promotion Media].[*TOTAL_MEMBER_SEL~SUM]}, [*BASE_MEMBERS__Measures_]), Crossjoin([*SORTED_COL_AXIS], [*BASE_MEMBERS__Measures_]))) ON COLUMNS,\n"
            + "  NON EMPTY Union(Crossjoin({[Customers].[*TOTAL_MEMBER_SEL~AGG]}, {([Yearly Income].[*DEFAULT_MEMBER], [Gender].[*DEFAULT_MEMBER], [Education Level].[*DEFAULT_MEMBER])}), Union(Crossjoin(Crossjoin(Generate([*NATIVE_CJ_SET], {([Customers].CurrentMember, [Yearly Income].CurrentMember)}), {[Gender].[*TOTAL_MEMBER_SEL~AGG]}), {[Education Level].[*DEFAULT_MEMBER]}), Union(Crossjoin(Crossjoin(Generate([*NATIVE_CJ_SET], {[Customers].CurrentMember}), {[Yearly Income].[*TOTAL_MEMBER_SEL~AGG]}), {([Gender].[*DEFAULT_MEMBER], [Education Level].[*DEFAULT_MEMBER])}), Union(Crossjoin({[Customers].[*TOTAL_MEMBER_SEL~SUM]}, {([Yearly Income].[*DEFAULT_MEMBER], [Gender].[*DEFAULT_MEMBER], [Education Level].[*DEFAULT_MEMBER])}), Union(Crossjoin(Crossjoin(Generate([*NATIVE_CJ_SET], {([Customers].CurrentMember, [Yearly Income].CurrentMember)}), {[Gender].[*TOTAL_MEMBER_SEL~SUM]}), {[Education Level].[*DEFAULT_MEMBER]}), Union(Crossjoin(Crossjoin(Generate([*NATIVE_CJ_SET], {[Customers].CurrentMember}), {[Yearly Income].[*TOTAL_MEMBER_SEL~SUM]}), {([Gender].[*DEFAULT_MEMBER], [Education Level].[*DEFAULT_MEMBER])}), [*SORTED_ROW_AXIS])))))) ON ROWS\n"
            + "from [Sales]\n"
            + "where [*CJ_SLICER_AXIS]\n";
        executeQuery(mdx);
    }

    /**
     * Test case for <a href="http://jira.pentaho.com/browse/MONDRIAN-1925">
     * MONDRIAN-1925: NameExpression within snowflake dimension causes exception
     * </a>
     */
    public void testNameExpressionSnowflake() {
        Dialect dialect = getTestContext().getDialect();
        TestContext testContext =
            getTestContext().createSubstitutingCube(
                "Sales",
                "<Dimension foreignKey=\"product_id\" type=\"StandardDimension\" visible=\"true\" highCardinality=\"false\" name=\"Example\">\n"
                + "  <Hierarchy name=\"Example Hierarchy\" visible=\"true\" hasAll=\"true\" allMemberName=\"All\" allMemberCaption=\"All\" primaryKey=\"product_id\" primaryKeyTable=\"product\">\n"
                + "    <Join leftKey=\"product_class_id\" rightKey=\"product_class_id\">\n"
                + "      <Table name=\"product\">\n"
                + "      </Table>\n"
                + "         <Table name=\"product_class\">\n"
                + "      </Table>\n"
                + "    </Join>\n"
                + "    <Level name=\"IsZero\" visible=\"true\" table=\"product\" column=\"product_id\" type=\"Integer\" uniqueMembers=\"false\" levelType=\"Regular\" hideMemberIf=\"Never\">\n"
                + "      <NameExpression>\n"
                + "        <SQL dialect=\"generic\">\n"
                + "          <![CDATA[case when " + dialect.quoteIdentifier("product","product_id") + "=0 then 'Zero' else 'Non-Zero' end]]>\n"
                + "        </SQL>\n"
                + "      </NameExpression>\n"
                + "    </Level>\n"
                + "    <Level name=\"SubCat\" visible=\"true\" table=\"product_class\" column=\"product_class_id\" type=\"String\" uniqueMembers=\"false\" levelType=\"Regular\" hideMemberIf=\"Never\">\n"
                + "      <NameExpression>\n"
                + "        <SQL dialect=\"generic\">\n"
                + "          <![CDATA[" + dialect.quoteIdentifier("product_class","product_subcategory") + "]]>\n"
                + "        </SQL>\n"
                + "      </NameExpression>\n"
                + "    </Level>\n"
                + "    <Level name=\"ProductName\" visible=\"true\" table=\"product\" column=\"product_id\" type=\"Integer\" uniqueMembers=\"false\" levelType=\"Regular\" hideMemberIf=\"Never\">\n"
                + "      <NameExpression>\n"
                + "        <SQL dialect=\"generic\">\n"
                + "          <![CDATA["+ dialect.quoteIdentifier("product","product_name") + "]]>\n"
                + "        </SQL>\n"
                + "      </NameExpression>\n"
                + "    </Level>\n"
                + "  </Hierarchy>\n"
                + "</Dimension>\n",
                null,
                null, null);
        testContext.assertAxisReturns(
            "[Example.Example Hierarchy].[Non-Zero]",
            "[Example.Example Hierarchy].[Non-Zero]");
        testContext.assertAxisReturns(
            "[Example.Example Hierarchy].[Non-Zero].Children",
            "[Example.Example Hierarchy].[Non-Zero].[Juice]");
        testContext.assertAxisReturns(
            "[Example.Example Hierarchy].[Non-Zero].[Juice].Children",
            "[Example.Example Hierarchy].[Non-Zero].[Juice].[Washington Berry Juice]");
    }
}

// End BasicQueryTest.java
