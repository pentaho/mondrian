/*
// $Id$
// Modified from junit's TestRunner class. Original code is covered by
// the junit license and modifications are covered as follows:
//
// This software is subject to the terms of the Common Public License
// and Eclipse Plublic License agreements, available at the following URLs:
//    http://www.opensource.org/licenses/cpl1.0.php.
//    http://www.opensource.org/licenses/eclipse-1.0.php.
//
// Copyright (C) 2004 SAS Institute, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// sasebb, 14 December, 2004
*/
package mondrian.test;

import java.io.PrintStream;
import java.util.Enumeration;
import java.util.regex.Pattern;

import junit.framework.*;
import junit.runner.*;

public class MondrianTestRunner extends BaseTestRunner {
	private MondrianResultPrinter fPrinter;
	private int fIterations = 1;
	private int fVUsers = 1;

	public static final int SUCCESS_EXIT = 0;
	public static final int FAILURE_EXIT = 1;
	public static final int EXCEPTION_EXIT = 2;

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
		long startTime = System.currentTimeMillis();

		// Start a new thread for each virtual user
		Thread threads[] = new Thread[getVUsers()];
		for (int i = 0; i < getVUsers(); i++) {
			threads[i] = new Thread(new Runnable() {
				public void run() {
					//System.out.println("Thread: " + Thread.currentThread().getName());
					for (int j = 0; j < getIterations(); j++) {
						suite.run(result);
					}
				}

			}, "Test thread " + i);
			threads[i].start();
		}
		System.out.println("All " + getVUsers() + " thread(s) started.");

		// wait for all threads to finish
		for (int i = 0; i < getVUsers(); i++) {
			try {
				threads[i].join();
			} catch (InterruptedException e) {
				System.out.println("Thread: " + threads[i].getName() + " interrupted: "
						+ e.getMessage());
			}
		}

		long endTime = System.currentTimeMillis();
		long runTime = endTime - startTime;
		fPrinter.print(result, runTime);

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

}