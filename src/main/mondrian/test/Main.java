/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 1998-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 21 January, 1999
*/

package mondrian.test;
import junit.framework.*;
import junit.textui.TestRunner;
import mondrian.olap.Util;
import mondrian.olap.fun.BuiltinFunTable;
import mondrian.util.Schedule;

import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Properties;

/**
 * <code>Main</code> is the main test suite for Mondrian.
 **/
public class Main extends TestCase {
	static public void main(String[] args) {
        new Main().runSafe(args);
    }

    Main() {
		super("mondrian");
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
		Properties properties = Util.getProperties();
		String testName = properties.getProperty("mondrian.test.Name"),
			testClass = properties.getProperty("mondrian.test.Class"),
			testSuite = properties.getProperty("mondrian.test.Suite");
		if (testClass != null) {
			Class clazz = Class.forName(testClass);
			if (testName != null) {
				// e.g. testName = "testUseDimensionAsShorthandForMember",
				// testClass = "mondrian.test.FoodMartTestCase"
				Constructor constructor = clazz.getConstructor(
						new Class[] {String.class});
				Object o = constructor.newInstance(
						new Object[] {testName});
				return (Test) o;
			} else {
				// e.g. testClass = "mondrian.test.FoodMartTestCase",
				// the name of a class which extends TestCase. We will invoke
				// every method which starts with 'test'.
				TestSuite suite = new TestSuite();
				suite.addTestSuite(clazz);
				return suite;
			}
		}
		if (testSuite != null) {
			// e.g. testSuite = "mondrian.olap.fun.BuiltinFunTable". Class does
			// not necessarily implement Test. We call its 'public [static]
			// Test suite()' method.
			Class clazz = Class.forName(testSuite);
			Method method = clazz.getMethod("suite", new Class[0]);
			Object target;
			if (Modifier.isStatic(method.getModifiers())) {
				target = null;
			} else {
				target = clazz.newInstance();
			}
			Object o = method.invoke(target, new Object[0]);
			return (Test) o;
		}
		TestSuite suite = new TestSuite();
		suite.addTestSuite(FoodMartTestCase.class);
		suite.addTest(BuiltinFunTable.instance().suite());
		suite.addTestSuite(Schedule.ScheduleTestCase.class);
		return suite;
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
