/*
// Modified from junit's TestRunner class. Original code is covered by
// the junit license and modifications are covered as follows:
//
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 SAS Institute, Inc.
// Copyright (C) 2005-2009 Pentaho and others
// All Rights Reserved.
//
// sasebb, 14 December, 2004
*/
package mondrian.test;

import junit.framework.Test;
import junit.framework.TestResult;
import junit.runner.*;

import java.io.PrintStream;
import java.util.Timer;
import java.util.TimerTask;

public class MondrianTestRunner extends BaseTestRunner {
    private MondrianResultPrinter fPrinter;
    private int fIterations = 1;
    private int fVUsers = 1;
    private int fTimeLimit = 0; // seconds

    public static final int SUCCESS_EXIT = 0;
    public static final int FAILURE_EXIT = 1;
    public static final int EXCEPTION_EXIT = 2;

    private String stopReason = "Normal termination.";

    /**
     * Constructs a TestRunner.
     */
    public MondrianTestRunner() {
        this(System.out);
   }

    /**
     * Constructs a TestRunner using the given stream for all the output
     */
    public MondrianTestRunner(PrintStream writer) {
        this(new MondrianResultPrinter(writer));
    }

    /**
     * Constructs a TestRunner using the given ResultPrinter all the output
     */
    public MondrianTestRunner(MondrianResultPrinter printer) {
        fPrinter = printer;
    }

    /**
     * Always use the StandardTestSuiteLoader. Overridden from
     * BaseTestRunner.
     */
    public TestSuiteLoader getLoader() {
        return new StandardTestSuiteLoader();
    }

    public void testFailed(int status, Test test, Throwable t) {
    }
    public void testStarted(String testName) {
    }
    public void testEnded(String testName) {
    }
    /**
     * Creates the TestResult to be used for the test run.
     */
    protected TestResult createTestResult() {
        return new TestResult();
    }

    public TestResult doRun(final Test suite) {
        final TestResult result = createTestResult();
       result.addListener(fPrinter);
        /*
        // uncomment this block to get a list of the single tests with time used
        final long[] longa = new long[1];
       result.addListener(new TestListener() {
         public void addError(Test arg0, Throwable arg1) {
               // do nothing
           }

           public void addFailure(Test arg0, AssertionFailedError arg1) {
              // do nothing
           }

           public void endTest(Test arg0) {
                if (arg0 instanceof TestCase) {
                 long longb = System.currentTimeMillis() - longa[0];
                 System.out.println(
                     "endTest " + ((TestCase)arg0).getName() + " " + longb
                     + " ms");
              }
           }

           public void startTest(Test arg0) {
              if (arg0 instanceof TestCase) {
                 longa[0] = System.currentTimeMillis();
                  System.out.println("startTest " + ((TestCase)arg0).getName());
              }
           }
       }
      );
        */

        final long startTime = System.currentTimeMillis();
        // Set up a timit limit if specified
        if (getTimeLimit() > 0) {
            Timer timer = new Timer();
            timer.schedule(
                new TimerTask() {
                    public void run() {
                        setStopReason(
                            "Test stopped because the time limit expired.");
                        result.stop();
                    }
                },
                1000L * (long)getTimeLimit());
        }

        // Start a new thread for each virtual user
        Thread threads[] = new Thread[getVUsers()];
        for (int i = 0; i < getVUsers(); i++) {
            threads[i] =
                new Thread(
                    new Runnable() {
                        public void run() {
                            for (int j = 0;
                                getIterations() == 0 || j < getIterations();
                                j++)
                            {
                                suite.run(result);
                                if (!result.wasSuccessful()) {
                                    setStopReason(
                                        "Test stopped due to errors.");
                                    result.stop();
                                }
                                if (result.shouldStop()) {
                                    break;
                                }
                            }
                        }
                    },
                    "Test thread " + i);
            threads[i].start();
        }
        System.out.println("All " + getVUsers() + " thread(s) started.");

        // wait for all threads to finish
        for (int i = 0; i < getVUsers(); i++) {
            try {
                threads[i].join();
            } catch (InterruptedException e) {
                System.out.println(
                    "Thread: " + threads[i].getName() + " interrupted: "
                    + e.getMessage());
            }
        }

        // print timer results and any exceptions
        long runTime = System.currentTimeMillis() - startTime;
        fPrinter.print(result, runTime);
        fPrinter.getWriter().println(getStopReason());

        return result;
    }

    protected void runFailed(String message) {
        System.err.println(message);
        System.exit(FAILURE_EXIT);
    }

    public void setPrinter(MondrianResultPrinter printer) {
        fPrinter = printer;
    }

    public void setIterations(int fIterations) {
        this.fIterations = fIterations;
    }

    public int getIterations() {
        return fIterations;
    }

    public void setVUsers(int fVUsers) {
        this.fVUsers = fVUsers;
    }

    public int getVUsers() {
        return fVUsers;
    }

    public void setTimeLimit(int fTimeLimit) {
        this.fTimeLimit = fTimeLimit;
    }

    public int getTimeLimit() {
        return fTimeLimit;
    }

    private void setStopReason(String stopReason) {
        this.stopReason = stopReason;
    }

    private String getStopReason() {
        return stopReason;
    }

}

// End MondrianTestRunner.java
