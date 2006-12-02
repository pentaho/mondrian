/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2006 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.test;

import mondrian.olap.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.io.PrintStream;
import java.text.MessageFormat;

/**
 * Thread which runs an MDX query and checks it against an expected result.
 * It is used for concurrency testing.
 */
public class QueryRunner extends Thread {
    private long mRunTime;
    private long mStartTime;
    private long mStopTime;
    private List<Exception> mExceptions = new ArrayList<Exception>();
    private int mMyId;
    private int mRunCount;
    private int mSuccessCount;
    private boolean mRandomQueries;

    private static final char nl = Character.LINE_SEPARATOR;

    static final String[] Queries = new String[]{
        // #0
        "select {[Measures].[Unit Sales]} on columns" + nl +
        " from Sales",

        // mdx sample #1
        "select" + nl +
        "    {[Measures].[Unit Sales]} on columns," + nl +
        "    order(except([Promotion Media].[Media Type].members,{[Promotion Media].[Media Type].[No Media]}),[Measures].[Unit Sales],DESC) on rows" + nl +
        "from Sales",

        // mdx sample #2
        "select" + nl +
        "    { [Measures].[Units Shipped], [Measures].[Units Ordered] } on columns," + nl +
        "    NON EMPTY [Store].[Store Name].members on rows" + nl +
        "from Warehouse",

        // mdx sample #3
        "with member [Measures].[Store Sales Last Period] as '([Measures].[Store Sales], Time.PrevMember)'" + nl +
        "select" + nl +
        "    {[Measures].[Store Sales Last Period]} on columns," + nl +
        "    {TopCount([Product].[Product Department].members,5, [Measures].[Store Sales Last Period])} on rows" + nl +
        "from Sales" + nl +
        "where ([Time].[1998])",

        // mdx sample #4
        "with member [Measures].[Total Store Sales] as 'Sum(YTD(),[Measures].[Store Sales])'" + nl +
        "select" + nl +
        "    {[Measures].[Total Store Sales]} on columns," + nl +
        "    {TopCount([Product].[Product Department].members,5, [Measures].[Total Store Sales])} on rows" + nl +
        "from Sales" + nl +
        "where ([Time].[1997].[Q2].[4])",

        // mdx sample #5
        "with member [Measures].[Store Profit Rate] as '([Measures].[Store Sales]-[Measures].[Store Cost])/[Measures].[Store Cost]', format = '#.00%'" + nl +
        "select" + nl +
        "    {[Measures].[Store Cost],[Measures].[Store Sales],[Measures].[Store Profit Rate]} on columns," + nl +
        "    Order([Product].[Product Department].members, [Measures].[Store Profit Rate], BDESC) on rows" + nl +
        "from Sales" + nl +
        "where ([Time].[1997])",

        // mdx sample #6
        "with" + nl +
        "   member [Product].[All Products].[Drink].[Percent of Alcoholic Drinks] as '[Product].[All Products].[Drink].[Alcoholic Beverages]/[Product].[All Products].[Drink]', format = '#.00%'" + nl +
        "select" + nl +
        "   { [Product].[All Products].[Drink].[Percent of Alcoholic Drinks] } on columns," + nl +
        "   order([Customers].[All Customers].[USA].[WA].Children, [Product].[All Products].[Drink].[Percent of Alcoholic Drinks],BDESC ) on rows" + nl +
        "from Sales" + nl +
        "where ( [Measures].[Unit Sales] )",

        // mdx sample #7
        "with member [Measures].[Accumulated Sales] as 'Sum(YTD(),[Measures].[Store Sales])'" + nl +
        "select" + nl +
        "    {[Measures].[Store Sales],[Measures].[Accumulated Sales]} on columns," + nl +
        "    {Descendants([Time].[1997],[Time].[Month])} on rows" + nl +
        "from Sales",

        // #8
        "select" + nl +
        " {[Measures].[Unit Sales]} on columns," + nl +
        " [Gender].members on rows" + nl +
        "from Sales",

//        // #9
//        "with" + nl +
//        "  member [Product].[Non dairy] as '[Product].[All Products] - [Product].[Food].[Dairy]'" + nl +
//        "  member [Measures].[Dairy ever] as 'sum([Time].members, ([Measures].[Unit Sales],[Product].[Food].[Dairy]))'" + nl +
//        "  set [Customers who never bought dairy] as 'filter([Customers].members, [Measures].[Dairy ever] = 0)'" + nl +
//        "select" + nl +
//        " {[Measures].[Unit Sales], [Measures].[Dairy ever]}  on columns," + nl +
//        "  [Customers who never bought dairy] on rows" + nl +
//        "from Sales\r\n",

        // #10
        "select {[Has bought dairy].members} on columns," + nl +
        " {[Customers].[USA]} on rows" + nl +
        "from Sales" + nl +
        "where ([Measures].[Unit Sales])",

        // #11
        "WITH" + nl +
        "   MEMBER [Measures].[StoreType] AS " + nl +
        "   '[Store].CurrentMember.Properties(\"Store Type\")'," + nl +
        "   SOLVE_ORDER = 2" + nl +
        "   MEMBER [Measures].[ProfitPct] AS " + nl +
        "   '((Measures.[Store Sales] - Measures.[Store Cost]) / Measures.[Store Sales])'," + nl +
        "   SOLVE_ORDER = 1, FORMAT_STRING = 'Percent'" + nl +
        "SELECT" + nl +
        "   { [Store].[Store Name].Members} ON COLUMNS," + nl +
        "   { [Measures].[Store Sales], [Measures].[Store Cost], [Measures].[StoreType]," + nl +
        "   [Measures].[ProfitPct] } ON ROWS" + nl +
        "FROM Sales",

        // #12
        "WITH" + nl +
        "   MEMBER [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[BigSeller] AS" + nl +
        "  'IIf([Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine] > 100, \"Yes\",\"No\")'" + nl +
        "SELECT" + nl +
        "   {[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[BigSeller]} ON COLUMNS," + nl +
        "   {Store.[Store Name].Members} ON ROWS" + nl +
        "FROM Sales",

        // #13
        "WITH" + nl +
        "   MEMBER [Measures].[ProfitPct] AS " + nl +
        "   '((Measures.[Store Sales] - Measures.[Store Cost]) / Measures.[Store Sales])'," + nl +
        "   SOLVE_ORDER = 1, FORMAT_STRING = 'Percent'" + nl +
        "   MEMBER [Measures].[ProfitValue] AS " + nl +
        "   '[Measures].[Store Sales] * [Measures].[ProfitPct]'," + nl +
        "   SOLVE_ORDER = 2, FORMAT_STRING = 'Currency'" + nl +
        "SELECT" + nl +
        "   { [Store].[Store Name].Members} ON COLUMNS," + nl +
        "   { [Measures].[Store Sales], [Measures].[Store Cost], [Measures].[ProfitValue]," + nl +
        "   [Measures].[ProfitPct] } ON ROWS" + nl +
        "FROM Sales",

        // #14: cyclical calculated members
        "WITH" + nl +
        "   MEMBER [Product].[X] AS '[Product].[Y]'" + nl +
        "   MEMBER [Product].[Y] AS '[Product].[X]'" + nl +
        "SELECT" + nl +
        "   {[Product].[X]} ON COLUMNS," + nl +
        "   {Store.[Store Name].Members} ON ROWS" + nl +
        "FROM Sales",

        // #15
        "WITH MEMBER MEASURES.ProfitPercent AS" + nl +
        "     '([Measures].[Store Sales]-[Measures].[Store Cost])/([Measures].[Store Cost])'," + nl +
        " FORMAT_STRING = '#.00%', SOLVE_ORDER = 1" + nl +
        " MEMBER [Time].[First Half 97] AS  '[Time].[1997].[Q1] + [Time].[1997].[Q2]'" + nl +
        " MEMBER [Time].[Second Half 97] AS '[Time].[1997].[Q3] + [Time].[1997].[Q4]'" + nl +
        " SELECT {[Time].[First Half 97]," + nl +
        "     [Time].[Second Half 97]," + nl +
        "     [Time].[1997].CHILDREN} ON COLUMNS," + nl +
        " {[Store].[Store Country].[USA].CHILDREN} ON ROWS" + nl +
        " FROM [Sales]" + nl +
        " WHERE ([Measures].[ProfitPercent])",

        // #16 (= mdx sample #7, but uses virtual cube)
        "with member [Measures].[Accumulated Sales] as 'Sum(YTD(),[Measures].[Store Sales])'" + nl +
        "select" + nl +
        "    {[Measures].[Store Sales],[Measures].[Accumulated Sales]} on columns," + nl +
        "    {Descendants([Time].[1997],[Time].[Month])} on rows" + nl +
        "from [Warehouse and Sales]",

        // #17 Virtual cube. Note that Unit Sales is independent of Warehouse.
        "select CrossJoin(\r\n" +
        "  {[Warehouse].DefaultMember, [Warehouse].[USA].children}," + nl +
        "  {[Measures].[Unit Sales], [Measures].[Units Shipped]}) on columns," + nl +
        " [Time].children on rows" + nl +
        "from [Warehouse and Sales]",

        // #18 bug: should allow dimension to be used as shorthand for member
        "select {[Measures].[Unit Sales]} on columns," + nl +
        " {[Store], [Store].children} on rows" + nl +
        "from [Sales]",

        // #19 bug: should allow 'members(n)' (and do it efficiently)
        "select {[Measures].[Unit Sales]} on columns," + nl +
        " {[Customers].members} on rows" + nl +
        "from [Sales]",

        // #20 crossjoins on rows and columns, and a slicer
        "select" + nl +
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
    };

    public QueryRunner(int id, int numSeconds, boolean useRandomQuery) {
        mRunTime = numSeconds * 1000;
        mMyId = id;
        mRandomQueries = useRandomQuery;
    }

    public void run() {
        mStartTime = System.currentTimeMillis();

        try {
            Connection cxn = DriverManager.getConnection(StandAlone.ConnectionString, null, false);
            int queryIndex = -1;

            while (System.currentTimeMillis() - mStartTime < mRunTime) {
                try {
                    if (mRandomQueries) {
                        queryIndex = (int)(Math.random() * Queries.length);
                    }
                    else {
                        queryIndex = mRunCount % Queries.length;
                    }

                    mRunCount++;

                    Query query = cxn.parseQuery(Queries[queryIndex]);
                    cxn.execute(query);
                    mSuccessCount++;
                } catch (Exception e) {
                    mExceptions.add(new Exception("Exception occurred on iteration " + queryIndex, e));
                }
            }
            mStopTime = System.currentTimeMillis();
        }
        catch (Exception e) {
            mExceptions.add(e);
        }
    }

    public void report(PrintStream out) {
        String message = MessageFormat.format(
            "Thread {0} ran {1} queries, {2} successfully in {3} milliseconds",
            mMyId, mRunCount, mSuccessCount, mStopTime - mStartTime);

        out.println(message);

        for (Exception throwable : mExceptions) {
            throwable.printStackTrace(out);
        }
    }

    private static void runTest(int numThreads, int seconds, boolean randomQueries) {
        QueryRunner[] runners = new QueryRunner[numThreads];

        for (int idx = 0; idx < runners.length; idx++) {
            runners[idx] = new QueryRunner(idx, seconds, randomQueries);
        }

        for (int idx = 0; idx < runners.length; idx++) {
            runners[idx].start();
        }

        for (int idx = 0; idx < runners.length; idx++) {
            try {
                runners[idx].join();
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        for (int idx = 0; idx < runners.length; idx++) {
            runners[idx].report(System.out);
        }
    }

    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("usage: QueryRunner numThreads seconds randomQueries");

            return;
        }
        else {
            runTest(
                Integer.parseInt(args[0]),
                Integer.parseInt(args[1]),
                Boolean.valueOf(args[2]));
        }
    }
}

// End QueryRunner.java
