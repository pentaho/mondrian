/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde
// Copyright (C) 2005-2009 Pentaho and others
// All Rights Reserved.
*/
package mondrian.test;

import mondrian.olap.*;

import java.io.PrintStream;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class to run set of MDX queries in multiple threads and
 * validate the results.
 * Queries are run against the FoodMart database.
 *
 * @author Thiyagu, Ajit
 */

public class ConcurrentValidatingQueryRunner extends Thread {
    private long mRunTime;
    private long mStartTime;
    private long mStopTime;
    private volatile List<Throwable> mExceptions = new ArrayList<Throwable>();
    private String threadName;
    private int mRunCount;
    private int mSuccessCount;
    private boolean mRandomQueries;
    // if mRandomCacheFlush is true, toss a unfair coin,
    // if the result of the coin toss is favorable, flush a random region
    // of cache
    private boolean mRandomCacheFlush;
    // a real number from 0 to 1 inclusive represents the bias of the coin
    // 0.5 is a fair coin
    private double mRandomFlushFrequency = 0.5;
    private ConcurrentMdxTest concurrentMdxTest = new ConcurrentMdxTest();

    private FoodMartTestCase.QueryAndResult[] mdxQueries;

    // mutex to isolate sections that run MDX and sections that flush cache
    // tests fail intermittenly if this mutex is removed
    private static Object lock = new Object();

    /**
     * Runs concurrent queries without flushing cache. This constructor
     * provides backward compatibilty for usage in {@link ConcurrentMdxTest}.
     *
     * @param numSeconds Running time
     * @param useRandomQuery If set to <code>true</code>, the runner will
     *        pick a random query from the set. If set to <code>false</code>,
     *        the runner will circle through queries sequentially
     * @param queriesAndResults The array of pairs of query and expected result
     */
    public ConcurrentValidatingQueryRunner(
        int numSeconds,
        boolean useRandomQuery,
        FoodMartTestCase.QueryAndResult[] queriesAndResults)
    {
        this.mdxQueries = queriesAndResults;
        mRunTime = numSeconds * 1000;
        mRandomQueries = useRandomQuery;
        mRandomCacheFlush = false;
    }

    /**
     * Runs concurrent queries with random cache flush.
     *
     * @param numSeconds Running time
     * @param useRandomQuery If set to <code>true</code>, the runner will
     *        pick a random query from the set. If set to <code>false</code>,
     *        the runner will circle through queries sequentially
     * @param randomCacheFlush If set to <code>true</code>, the runner will
     *        do a coin toss before running the query. If the result of the
     *        experiment is favorable, runner will flush a random region
     *        of aggregation cache
     * @param queriesAndResults The array of pairs of query and expected result
     */
    public ConcurrentValidatingQueryRunner(
        int numSeconds,
        boolean useRandomQuery,
        boolean randomCacheFlush,
        FoodMartTestCase.QueryAndResult[] queriesAndResults)
    {
        this.mdxQueries = queriesAndResults;
        mRunTime = numSeconds * 1000;
        mRandomQueries = useRandomQuery;
        mRandomCacheFlush = randomCacheFlush;
    }

    /**
     * Runs a number of queries until time expires. For each iteration,
     * if cache is to be flushed, do it before running the query.
     */
    public void run() {
        mStartTime = System.currentTimeMillis();
        threadName = Thread.currentThread().getName();
        try {
            int queryIndex = -1;

            while (System.currentTimeMillis() - mStartTime < mRunTime) {
                try {
                    if (mRandomQueries) {
                        queryIndex =
                            (int) (Math.random() * mdxQueries.length);
                    } else {
                        queryIndex = mRunCount %
                            mdxQueries.length;
                    }

                    mRunCount++;

                    synchronized (lock) {
                        // flush a random region of cache
                        if (mRandomCacheFlush
                            && (Math.random() < mRandomFlushFrequency))
                        {
                            flushRandomRegionOfCache();
                        }

                        // flush the whole schema
                        if (mRandomCacheFlush
                            && (Math.random() < mRandomFlushFrequency))
                        {
                            flushSchema();
                        }
                    }
                    synchronized (lock) {
                        concurrentMdxTest.assertQueryReturns(
                            mdxQueries[queryIndex].query,
                            mdxQueries[queryIndex].result);
                        mSuccessCount++;
                    }
                } catch (Exception e) {
                    mExceptions.add(
                        new Exception(
                            "Exception occurred in iteration " + mRunCount
                            + " of thread " + Thread.currentThread().getName(),
                            e));
                }
            }
            mStopTime = System.currentTimeMillis();
        } catch (Exception e) {
            mExceptions.add(e);
        } catch (Error e) {
            mExceptions.add(e);
        }
    }

    /**
     * Prints result of this test run.
     *
     * @param out Output stream
     */
    private void report(PrintStream out) {
        String message = MessageFormat.format(
            " {0} ran {1} queries, {2} successfully in {3} milliseconds",
            threadName,
            mRunCount,
            mSuccessCount,
            mStopTime - mStartTime);

        out.println(message);

        for (Object throwable : mExceptions) {
            if (throwable instanceof Exception) {
                ((Exception) throwable).printStackTrace(out);
            } else {
                System.out.println(throwable);
            }
        }
    }

    /**
     * Creates and runs concurrent threads of tests without flushing cache.
     * This method provides backward compatibilty for usage in
     * {@link ConcurrentMdxTest}.
     *
     * @param numThreads Number of concurrent threads
     * @param runTimeInSeconds Running Time
     * @param randomQueries Whether to pick queries in random or in sequence
     * @param printReport Whether to print report
     * @param queriesAndResults Array of pairs of query and expected result
     * @return The list of failures
     */
    static List<Throwable> runTest(
        int numThreads,
        int runTimeInSeconds,
        boolean randomQueries,
        boolean printReport,
        FoodMartTestCase.QueryAndResult[] queriesAndResults)
    {
        return runTest(
            numThreads,
            runTimeInSeconds,
            randomQueries,
            false,
            printReport,
            queriesAndResults);
    }

    /**
     * Creates and runs concurrent threads of tests with random cache flush.
     *
     * @param numThreads Number of concurrent threads
     * @param runTimeInSeconds Running Time
     * @param randomQueries Whether to pick queries in random or in sequence
     * @param randomCacheFlush Whether to flush cache before running queries
     * @param printReport Whether to print report
     * @param queriesAndResults Array of pairs of query and expected result
     * @return The list of failures
     */
    static List<Throwable> runTest(
        int numThreads,
        int runTimeInSeconds,
        boolean randomQueries,
        boolean randomCacheFlush,
        boolean printReport,
        FoodMartTestCase.QueryAndResult[] queriesAndResults)
    {
        ConcurrentValidatingQueryRunner[] runners =
            new ConcurrentValidatingQueryRunner[numThreads];
        List<Throwable> allExceptions = new ArrayList<Throwable>();

        for (int idx = 0; idx < runners.length; idx++) {
            runners[idx] = new ConcurrentValidatingQueryRunner(
                runTimeInSeconds,
                randomQueries,
                randomCacheFlush,
                queriesAndResults);
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
            allExceptions.addAll(runners[idx].mExceptions);
            if (printReport) {
                runners[idx].report(System.out);
            }
        }
        return allExceptions;
    }

    /**
     * Flushes the whole schema.
     *
     */
    private void flushSchema() {
        Connection connection =
            concurrentMdxTest.getConnection();
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

    /**
     * Flushes a random region of cache. This is not truly random yet; the
     * method pick one of the three US states to be flushed.
     */
    private void flushRandomRegionOfCache() {
        // todo: more dimensions for randomizing

        Connection connection =
            concurrentMdxTest.getConnection();
        CacheControl cacheControl =
            connection.getCacheControl(null);

        // Lookup members
        Cube salesCube =
            connection.getSchema().lookupCube(
                "Sales", true);
        SchemaReader schemaReader =
            salesCube.getSchemaReader(null);

        CacheControl.CellRegion measuresRegion =
            cacheControl.createMeasuresRegion(
                salesCube);

        try {
            String[] tsegments =
                new String[] {"Time", "1997"};
            Id tid = new Id(Id.Segment.toList(tsegments));

            Member memberTime97 =
                schemaReader.getMemberByUniqueName(tid.getSegments(), false);
            CacheControl.CellRegion regionTime97 =
                cacheControl.createMemberRegion(
                    memberTime97, true);

            String[] states = {"CA", "OR", "WA"};
            int idx = (int) (Math.random() * states.length);

            String[] ssegments =
                new String[] {"Customers", "All Customers", "USA", states[idx]};
            Id sid = new Id(Id.Segment.toList(ssegments));

            Member memberCustomerState =
                schemaReader.getMemberByUniqueName(sid.getSegments(), false);
            CacheControl.CellRegion regionCustomerState =
                cacheControl.createMemberRegion(
                    memberCustomerState, true);

            CacheControl.CellRegion region97State =
                cacheControl.createCrossjoinRegion(
                    measuresRegion,
                    regionTime97,
                    regionCustomerState);

            cacheControl.flush(region97State);
        } catch (Exception e) {
            // do nothing when a wrong region was picked
            // don't throw exception
        }
    }
}

// End ConcurrentValidatingQueryRunner.java
