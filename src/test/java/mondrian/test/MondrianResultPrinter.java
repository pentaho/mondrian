/*
// Modified from junit's ResultPrinter class. Original code is covered by
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

import junit.framework.*;
import junit.runner.BaseTestRunner;

import java.io.PrintStream;
import java.text.NumberFormat;
import java.util.Enumeration;

public class MondrianResultPrinter implements TestListener {
    PrintStream fWriter;
    int fStarted = 0;

    public MondrianResultPrinter(PrintStream writer) {
        fWriter = writer;
    }

    /* API for use by textui.TestRunner
     */

    synchronized void print(TestResult result, long runTime) {
        printHeader();
        printErrors(result);
        printFailures(result);
        printFooter(runTime, result);
    }

    void printWaitPrompt() {
        getWriter().println();
        getWriter().println("<RETURN> to continue");
    }

    /* Internal methods
     */

    protected void printHeader() {
        getWriter().println();
    }

    protected void printErrors(TestResult result) {
        printDefects(result.errors(), result.errorCount(), "error");
    }

    protected void printFailures(TestResult result) {
        printDefects(result.failures(), result.failureCount(), "failure");
    }

    protected void printDefects(Enumeration booBoos, int count, String type) {
        if (count == 0) {
            return;
        }
        if (count == 1) {
            getWriter().println("There was " + count + " " + type + ":");
        } else {
            getWriter().println("There were " + count + " " + type + "s:");
        }
        for (int i = 1; booBoos.hasMoreElements(); i++) {
            printDefect((TestFailure) booBoos.nextElement(), i);
        }
    }

    // only public for testing purposes
    public void printDefect(TestFailure booBoo, int count) {
        printDefectHeader(booBoo, count);
        printDefectTrace(booBoo);
    }

    protected void printDefectHeader(TestFailure booBoo, int count) {
        // I feel like making this a println, then adding a line
        // giving the throwable a chance to print something before we
        // get to the stack trace.
        getWriter().print(count + ") " + booBoo.failedTest());
    }

    protected void printDefectTrace(TestFailure booBoo) {
        getWriter().print(BaseTestRunner.getFilteredTrace(booBoo.trace()));
    }

    protected void printFooter(long runTime, TestResult result) {
        if (result.wasSuccessful()) {
            getWriter().println();
            getWriter().print("OK");
            getWriter().println(
                " (" + result.runCount() + " test"
                + (result.runCount() == 1 ? "" : "s") + ")");
        } else {
            getWriter().println();
            getWriter().println("FAILURES!!!");
            getWriter().println(
                "Tests run: " + result.runCount()
                + ",  Failures: " + result.failureCount()
                + ",  Errors: " + result.errorCount());
        }
        getWriter().println();
        getWriter().println("Time: " + elapsedTimeAsString(runTime));
    }


    /**
     * Returns the formatted string of the elapsed time.
     * Duplicated from BaseTestRunner. Fix it.
     */
    protected String elapsedTimeAsString(long runTime) {
        return NumberFormat.getInstance().format((double)runTime / 1000);
    }

    public PrintStream getWriter() {
        return fWriter;
    }
    /**
     * @see junit.framework.TestListener#addError(Test, Throwable)
     */
    public void addError(Test test, Throwable t) {
        getWriter().print("E");
    }

    /**
     * @see junit.framework.TestListener#addFailure(Test, AssertionFailedError)
     */
    public void addFailure(Test test, AssertionFailedError t) {
        getWriter().print("F");
    }

    /**
     * @see junit.framework.TestListener#endTest(Test)
     */
    public void endTest(Test test) {
    }

    /**
     * @see junit.framework.TestListener#startTest(Test)
     */
    public void startTest(Test test) {
        if (fStarted % 40 == 0) {
            getWriter().print("\n[" + fStarted + "] ");
        }
        getWriter().print(".");
        fStarted++;
    }

}

// End MondrianResultPrinter.java
