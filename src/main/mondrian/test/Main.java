/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 1998-2003 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 21 January, 1999
*/

package mondrian.test;
import junit.framework.*;
import junit.textui.TestRunner;
import mondrian.olap.MondrianProperties;
import mondrian.olap.Util;
import mondrian.olap.fun.BuiltinFunTable;
import mondrian.rolap.CachePool;
import mondrian.rolap.RolapUtil;
import mondrian.rolap.agg.TestAggregationManager;
import mondrian.util.Schedule;

import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Enumeration;
import java.util.regex.Pattern;

/**
 * <code>Main</code> is the main test suite for Mondrian.
 **/
public class Main {
	static public void main(String[] args) {
        new Main().runSafe(args);
    }

    private void runSafe(String[] args) {
        try {
            run(args);
        } catch (Exception e) {
            PrintWriter pw = new PrintWriter(System.out);
            pw.println("mondrian.test.Main received exception:");
            String[] errors = Util.convertStackToString(e);
            for (int i = 0; i < errors.length; i++) {
                pw.println(errors[i]);
            }
            pw.flush();
        }
    }

	/**
	 * Creates and runs the root test suite.
	 */
	void run(String[] args) throws Exception {
		Test test = suite();
        if (false) {
            new MondrianHarness().run(test, new MondrianListener());
        } else {
            TestRunner.run(test);
        }
	}

	/**
	 * Creates a TestSuite to test the whole of mondrian. Methods with the
	 * signature <code>public static Test suite()</code> are recognized
	 * automatically by JUnit test-harnesses; see {@link TestSuite}.
	 */
	public static Test suite() throws Exception {
		RolapUtil.checkTracing();
		MondrianProperties properties = MondrianProperties.instance();
		String testName = properties.getTestName(),
			testClass = properties.getTestClass();
		TestSuite suite = new TestSuite();
		if (testClass != null) {
			Class clazz = Class.forName(testClass);
			if (Test.class.isAssignableFrom(clazz)) {
				// e.g. testClass = "mondrian.test.FoodMartTestCase",
				// the name of a class which extends TestCase. We will invoke
				// every method which starts with 'test'. (If "testName" is set,
				// we'll filter this list later.)
				suite.addTestSuite(clazz);
			} else {
				// e.g. testClass = "mondrian.olap.fun.BuiltinFunTable". Class
				// does not implement Test, so look for a 'public [static]
				// Test suite()' method.
				Method method = clazz.getMethod("suite", new Class[0]);
				Object target;
				if (Modifier.isStatic(method.getModifiers())) {
					target = null;
				} else {
					target = clazz.newInstance();
				}
				Object o = method.invoke(target, new Object[0]);
				suite.addTest((Test) o);
			}
		} else {
			suite.addTestSuite(BasicQueryTest.class);
			suite.addTest(BuiltinFunTable.suite());
			suite.addTestSuite(Schedule.ScheduleTestCase.class);
			suite.addTest(Util.suite());
			suite.addTest(CachePool.suite());
			suite.addTestSuite(TestAggregationManager.class);
			suite.addTestSuite(ParameterTest.class);
			suite.addTestSuite(AccessControlTest.class);
			//suite.addTestSuite(ParentChildHierarchyTest.class);
		}
		if (testName != null) {
			// Filter the suite, so that only tests whose names match
			// "testName" (in its entirety) will be run.
			Pattern testPattern = Pattern.compile(testName);
			suite = copySuite(suite, testPattern);
		}
		return suite;
	}

	private static TestSuite copySuite(TestSuite suite, Pattern testPattern) {
		TestSuite newSuite = new TestSuite();
		Enumeration tests = suite.tests();
		while (tests.hasMoreElements()) {
			Test test = (Test) tests.nextElement();
			if (test instanceof TestCase) {
				TestCase testCase = (TestCase) test;
				final String testName = testCase.getName();
				if (testPattern.matcher(testName).matches()) {
					newSuite.addTest(test);
				}
			} else if (test instanceof TestSuite) {
				TestSuite subSuite = copySuite((TestSuite) test, testPattern);
				if (subSuite.countTestCases() > 0) {
					newSuite.addTest(subSuite);
				}
			} else {
				// some other kind of test
				newSuite.addTest(test);
			}
		}
		return newSuite;
	}

	/**
	 * Call <code><i>className</i>.suite()</code> and add the
	 * resulting {@link Test}.
	 */
	static void addSuite(TestSuite suite, String className) throws Exception {
		Class clazz = Class.forName(className);
		Method method = clazz.getMethod("suite", new Class[0]);
		Object o = method.invoke(null, new Object[0]);
		suite.addTest((Test) o);
	}
}

/**
 * <code>MondrianHarness</code> is a simple harness for JUnit tests.
 */
class MondrianHarness {
	/**
	 * Runs a test.
	 */
	void run(Test test, TestListener listener) {
		TestResult result = new TestResult();
		result.addListener(listener);
		test.run(result);
	}
}

/**
 * <code>MondrianListener</code> is a simple listener.
 */
class MondrianListener implements TestListener {
	PrintWriter pw;
	MondrianListener() {
		this.pw = TestContext.instance().getWriter();
	}
	private void report(Test test, String s, Throwable throwable) {
		pw.print(s + " in test '" + test + "': ");
		String[] msgs = Util.convertStackToString(throwable);
		for (int i = 0; i < msgs.length; i++) {
			String msg = msgs[i];
			pw.println(msg);
		}
	}
	public void addError(Test test, Throwable throwable) {
		report(test, "Error", throwable);
	}
	public void addFailure(Test test, AssertionFailedError error) {
		report(test, "Failure", error);
	}
	public void endTest(Test test) {
		pw.println(test + " succeeded.");
	}
	public void startTest(Test test) {
	}
}

// End Main.java
