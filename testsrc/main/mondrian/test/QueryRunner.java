/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 Julian Hyde
// Copyright (C) 2005-2011 Pentaho and others
// All Rights Reserved.
*/
package mondrian.test;

import mondrian.olap.*;

import java.io.PrintStream;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

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

    static final String[] Queries = new String[]{
        // #0
        "select {[Measures].[Unit Sales]} on columns\n"
        + " from Sales",

        // mdx sample #1
        "select\n"
        + "    {[Measures].[Unit Sales]} on columns,\n"
        + "    order(except([Promotion].[Media Type].members,{[Promotion].[Media Type].[No Media]}),[Measures].[Unit Sales],DESC) on rows\n"
        + "from Sales",

        // mdx sample #2
        "select\n"
        + "    { [Measures].[Units Shipped], [Measures].[Units Ordered] } on columns,\n"
        + "    NON EMPTY [Store].[Store Name].members on rows\n"
        + "from Warehouse",

        // mdx sample #3
        "with member [Measures].[Store Sales Last Period] as '([Measures].[Store Sales], Time.PrevMember)'\n"
        + "select\n"
        + "    {[Measures].[Store Sales Last Period]} on columns,\n"
        + "    {TopCount([Product].[Product Department].members,5, [Measures].[Store Sales Last Period])} on rows\n"
        + "from Sales\n"
        + "where ([Time].[1998])",

        // mdx sample #4
        "with member [Measures].[Total Store Sales] as 'Sum(YTD(),[Measures].[Store Sales])'\n"
        + "select\n"
        + "    {[Measures].[Total Store Sales]} on columns,\n"
        + "    {TopCount([Product].[Product Department].members,5, [Measures].[Total Store Sales])} on rows\n"
        + "from Sales\n"
        + "where ([Time].[1997].[Q2].[4])",

        // mdx sample #5
        "with member [Measures].[Store Profit Rate] as '([Measures].[Store Sales]-[Measures].[Store Cost])/[Measures].[Store Cost]', format = '#.00%'\n"
        + "select\n"
        + "    {[Measures].[Store Cost],[Measures].[Store Sales],[Measures].[Store Profit Rate]} on columns,\n"
        + "    Order([Product].[Product Department].members, [Measures].[Store Profit Rate], BDESC) on rows\n"
        + "from Sales\n"
        + "where ([Time].[1997])",

        // mdx sample #6
        "with\n"
        + "   member [Product].[All Products].[Drink].[Percent of Alcoholic Drinks] as '[Product].[All Products].[Drink].[Alcoholic Beverages]/[Product].[All Products].[Drink]', format = '#.00%'\n"
        + "select\n"
        + "   { [Product].[All Products].[Drink].[Percent of Alcoholic Drinks] } on columns,\n"
        + "   order([Customers].[All Customers].[USA].[WA].Children, [Product].[All Products].[Drink].[Percent of Alcoholic Drinks],BDESC) on rows\n"
        + "from Sales\n"
        + "where ([Measures].[Unit Sales])",

        // mdx sample #7
        "with member [Measures].[Accumulated Sales] as 'Sum(YTD(),[Measures].[Store Sales])'\n"
        + "select\n"
        + "    {[Measures].[Store Sales],[Measures].[Accumulated Sales]} on columns,\n"
        + "    {Descendants([Time].[1997],[Time].[Month])} on rows\n"
        + "from Sales",

        // #8
        "select\n"
        + " {[Measures].[Unit Sales]} on columns,\n"
        + " [Gender].members on rows\n"
        + "from Sales",

//        // #9
//        "with\n"
//        + "  member [Product].[Non dairy] as"
//        + " '[Product].[All Products] - [Product].[Food].[Dairy]'\n"
//        + "  member [Measures].[Dairy ever] as"
//        + " 'sum([Time].members, "
//        + "([Measures].[Unit Sales],[Product].[Food].[Dairy]))'\n"
//        + "  set [Customers who never bought dairy] as"
//        + " 'filter([Customers].members, [Measures].[Dairy ever] = 0)'\n"
//        + "select\n"
//        + " {[Measures].[Unit Sales], [Measures].[Dairy ever]}  on columns,\n"
//        + "  [Customers who never bought dairy] on rows\n"
//        + "from Sales\r\n",

        // #10
        "select {[Has bought dairy].members} on columns,\n"
        + " {[Customers].[USA]} on rows\n"
        + "from Sales\n"
        + "where ([Measures].[Unit Sales])",

        // #11
        "WITH\n"
        + "   MEMBER [Measures].[StoreType] AS \n"
        + "   '[Store].CurrentMember.Properties(\"Store Type\")',\n"
        + "   SOLVE_ORDER = 2\n"
        + "   MEMBER [Measures].[ProfitPct] AS \n"
        + "   '((Measures.[Store Sales] - Measures.[Store Cost]) / Measures.[Store Sales])',\n"
        + "   SOLVE_ORDER = 1, FORMAT_STRING = 'Percent'\n"
        + "SELECT\n"
        + "   { [Store].[Store Name].Members} ON COLUMNS,\n"
        + "   { [Measures].[Store Sales], [Measures].[Store Cost], [Measures].[StoreType],\n"
        + "   [Measures].[ProfitPct] } ON ROWS\n"
        + "FROM Sales",

        // #12
        "WITH\n"
        + "   MEMBER [Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[BigSeller] AS\n"
        + "  'IIf([Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine] > 100, \"Yes\",\"No\")'\n"
        + "SELECT\n"
        + "   {[Product].[All Products].[Drink].[Alcoholic Beverages].[Beer and Wine].[BigSeller]} ON COLUMNS,\n"
        + "   {Store.[Store Name].Members} ON ROWS\n"
        + "FROM Sales",

        // #13
        "WITH\n"
        + "   MEMBER [Measures].[ProfitPct] AS \n"
        + "   '((Measures.[Store Sales] - Measures.[Store Cost]) / Measures.[Store Sales])',\n"
        + "   SOLVE_ORDER = 1, FORMAT_STRING = 'Percent'\n"
        + "   MEMBER [Measures].[ProfitValue] AS \n"
        + "   '[Measures].[Store Sales] * [Measures].[ProfitPct]',\n"
        + "   SOLVE_ORDER = 2, FORMAT_STRING = 'Currency'\n"
        + "SELECT\n"
        + "   { [Store].[Store Name].Members} ON COLUMNS,\n"
        + "   { [Measures].[Store Sales], [Measures].[Store Cost], [Measures].[ProfitValue],\n"
        + "   [Measures].[ProfitPct] } ON ROWS\n"
        + "FROM Sales",

        // #14: cyclical calculated members
        "WITH\n"
        + "   MEMBER [Product].[X] AS '[Product].[Y]'\n"
        + "   MEMBER [Product].[Y] AS '[Product].[X]'\n"
        + "SELECT\n"
        + "   {[Product].[X]} ON COLUMNS,\n"
        + "   {Store.[Store Name].Members} ON ROWS\n"
        + "FROM Sales",

        // #15
        "WITH MEMBER MEASURES.ProfitPercent AS\n"
        + "     '([Measures].[Store Sales]-[Measures].[Store Cost])/([Measures].[Store Cost])',\n"
        + " FORMAT_STRING = '#.00%', SOLVE_ORDER = 1\n"
        + " MEMBER [Time].[Time].[First Half 97] AS  '[Time].[1997].[Q1] + [Time].[1997].[Q2]'\n"
        + " MEMBER [Time].[Time].[Second Half 97] AS '[Time].[1997].[Q3] + [Time].[1997].[Q4]'\n"
        + " SELECT {[Time].[First Half 97],\n"
        + "     [Time].[Second Half 97],\n"
        + "     [Time].[1997].CHILDREN} ON COLUMNS,\n"
        + " {[Store].[Store Country].[USA].CHILDREN} ON ROWS\n"
        + " FROM [Sales]\n"
        + " WHERE ([Measures].[ProfitPercent])",

        // #16 (= mdx sample #7, but uses virtual cube)
        "with member [Measures].[Accumulated Sales] as 'Sum(YTD(),[Measures].[Store Sales])'\n"
        + "select\n"
        + "    {[Measures].[Store Sales],[Measures].[Accumulated Sales]} on columns,\n"
        + "    {Descendants([Time].[1997],[Time].[Month])} on rows\n"
        + "from [Warehouse and Sales]",

        // #17 Virtual cube. Note that Unit Sales is independent of Warehouse.
        "select CrossJoin(\r\n"
        + "  {[Warehouse].DefaultMember, [Warehouse].[USA].children},\n"
        + "  {[Measures].[Unit Sales], [Measures].[Units Shipped]}) on columns,\n"
        + " [Time].children on rows\n"
        + "from [Warehouse and Sales]",

        // #18 bug: should allow dimension to be used as shorthand for member
        "select {[Measures].[Unit Sales]} on columns,\n"
        + " {[Store], [Store].[Stores].children} on rows\n"
        + "from [Sales]",

        // #19 bug: should allow 'members(n)' (and do it efficiently)
        "select {[Measures].[Unit Sales]} on columns,\n"
        + " {[Customers].members} on rows\n"
        + "from [Sales]",

        // #20 crossjoins on rows and columns, and a slicer
        "select\n"
        + "  CrossJoin(\n"
        + "    {[Measures].[Unit Sales], [Measures].[Store Sales]},\n"
        + "    {[Time].[1997].[Q2].children}) on columns, \n"
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
    };

    public QueryRunner(int id, int numSeconds, boolean useRandomQuery) {
        mRunTime = (long) numSeconds * 1000;
        mMyId = id;
        mRandomQueries = useRandomQuery;
    }

    public void run() {
        mStartTime = System.currentTimeMillis();

        try {
            Connection cxn =
                DriverManager.getConnection(
                    StandAlone.ConnectionString, null);
            int queryIndex = -1;

            while (System.currentTimeMillis() - mStartTime < mRunTime) {
                try {
                    if (mRandomQueries) {
                        queryIndex = (int)(Math.random() * Queries.length);
                    } else {
                        queryIndex = mRunCount % Queries.length;
                    }

                    mRunCount++;

                    Query query = cxn.parseQuery(Queries[queryIndex]);
                    cxn.execute(query);
                    mSuccessCount++;
                } catch (Exception e) {
                    mExceptions.add(
                        new Exception(
                            "Exception occurred on iteration " + queryIndex,
                            e));
                }
            }
            mStopTime = System.currentTimeMillis();
        } catch (Exception e) {
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

    private static void runTest(
        int numThreads, int seconds, boolean randomQueries)
    {
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
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        for (int idx = 0; idx < runners.length; idx++) {
            runners[idx].report(System.out);
        }
    }

    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println(
                "usage: QueryRunner numThreads seconds randomQueries");
        } else {
            runTest(
                Integer.parseInt(args[0]),
                Integer.parseInt(args[1]),
                Boolean.valueOf(args[2]));
        }
    }
}

// End QueryRunner.java
