/*
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
*/
package mondrian.test;

import mondrian.test.ConcurrentMdxTest;

import java.util.List;
import java.util.ArrayList;
import java.io.PrintStream;
import java.text.MessageFormat;

/**
 * Utility class to run set of MDX queries in multiple threads and
 * validate the results.
 * Queries are run against the FoodMart database.
 *
 * @author Thiyagu, Ajit
 * @version $Id$
 */

public class ConcurrentValidatingQueryRunner extends Thread {
    private long mRunTime;
    private long mStartTime;
    private long mStopTime;
    private volatile List mExceptions = new ArrayList();
    private String threadName;
    private int mRunCount;
    private int mSuccessCount;
    private boolean mRandomQueries;
    private ConcurrentMdxTest concurrentMdxTest = new ConcurrentMdxTest();

    private FoodMartTestCase.QueryAndResult[] mdxQueries;

    public ConcurrentValidatingQueryRunner(int numSeconds,
                                           boolean useRandomQuery,
                                           FoodMartTestCase.QueryAndResult[] queriesAndResults) {
        this.mdxQueries = queriesAndResults;
        mRunTime = numSeconds * 1000;
        mRandomQueries = useRandomQuery;
    }

    public void run() {
        mStartTime = System.currentTimeMillis();
        threadName = Thread.currentThread().getName();
        try {

            int queryIndex = -1;

            while (System.currentTimeMillis() - mStartTime < mRunTime) {
                try {
                    if (mRandomQueries) {
                        queryIndex = (int) (Math.random() *
                                mdxQueries.length);
                    } else {
                        queryIndex = mRunCount %
                                mdxQueries.length;
                    }

                    mRunCount++;
                    concurrentMdxTest.assertQueryReturns(
                            mdxQueries[queryIndex].query,
                            mdxQueries[queryIndex].result);

                    mSuccessCount++;
                } catch (Exception e) {
                    mExceptions.add(
                            new Exception("Exception occurred in iteration " +
                                   mRunCount + " of thread " +
                                    Thread.currentThread().getName(), e));
                }
            }
            mStopTime = System.currentTimeMillis();
        }
        catch (Exception e) {
            mExceptions.add(e);
        }
        catch (Error e) {
            mExceptions.add(e);
        }
    }

    private void report(PrintStream out) {
        String message = MessageFormat.format(
                " {0} ran {1} queries, {2} successfully in {3} milliseconds",
                threadName, mRunCount, mSuccessCount, mStopTime - mStartTime);

        out.println(message);

        for (Object throwable : mExceptions) {
            if (throwable instanceof Exception) {
                ((Exception) throwable).printStackTrace(out);
            } else {
                System.out.println(throwable);
            }
        }
    }

    static List<Exception> runTest(int numThreads, int runTimeInSeconds,
                                          boolean randomQueries,
                                          boolean printReport,
                                          FoodMartTestCase.QueryAndResult[] queriesAndResults) {
        ConcurrentValidatingQueryRunner[] runners =
                new ConcurrentValidatingQueryRunner[numThreads];
        List<Exception> allExceptions = new ArrayList<Exception>();

        for (int idx = 0; idx < runners.length; idx++) {
            runners[idx] = new ConcurrentValidatingQueryRunner(runTimeInSeconds,
                                                               randomQueries,
                                                               queriesAndResults);
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
            allExceptions.addAll(runners[idx].mExceptions);
            if (printReport) {
                runners[idx].report(System.out);
            }
        }
        return allExceptions;
    }
}

