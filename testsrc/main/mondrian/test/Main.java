/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 1998-2002 Kana Software, Inc.
// Copyright (C) 2001-2006 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 21 January, 1999
*/

package mondrian.test;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Enumeration;
import java.util.regex.Pattern;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestResult;
import junit.framework.TestSuite;
import mondrian.olap.MondrianProperties;
import mondrian.olap.ParserTest;
import mondrian.olap.Util;
import mondrian.olap.UtilTestCase;
import mondrian.olap.fun.FunctionTest;
import mondrian.olap.HierarchyBugTest;
import mondrian.rolap.*;
import mondrian.test.comp.ResultComparatorTest;
import mondrian.util.ScheduleTest;
import mondrian.util.FormatTest;
import mondrian.xmla.XmlaBasicTest;
import mondrian.xmla.XmlaExcel2000Test;
import mondrian.xmla.XmlaExcelXPTest;
import mondrian.xmla.test.XmlaTest;

import org.apache.log4j.Logger;

/**
 * Main test suite for Mondrian.
 *
 * <p>The {@link #suite()} method returns a suite which contains all other
 * Mondrian tests.
 *
 * @author jhyde
 * @version $Id$
 */
public class Main extends TestSuite {
    private static final Logger logger = Logger.getLogger(Main.class);
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
            System.exit(1);
        }
    }

    /**
     * Creates and runs the root test suite.
     */
    void run(String[] args) throws Exception {
        final MondrianProperties properties = MondrianProperties.instance();
        Test test = suite();
        if (properties.Warmup.get()) {
            System.out.println("Starting warmup run...");
            MondrianTestRunner runner = new MondrianTestRunner();
            TestResult tres = runner.doRun(test);
            if (!tres.wasSuccessful()) {
                System.out.println("Warmup run failed. Regular tests will not be run.");
                System.exit(1);
            }
            System.out.println("Warmup run complete. Starting regular run...");
        }
        MondrianTestRunner runner = new MondrianTestRunner();
        runner.setIterations(properties.Iterations.get());
        System.out.println("Iterations=" + properties.Iterations.get());
        runner.setVUsers(properties.VUsers.get());
        runner.setTimeLimit(properties.TimeLimit.get());
        TestResult tres = runner.doRun(test);
        if (!tres.wasSuccessful())
            System.exit(1);
    }

    /**
     * Creates a TestSuite to test the whole of mondrian. Methods with the
     * signature <code>public static Test suite()</code> are recognized
     * automatically by JUnit test-harnesses; see {@link TestSuite}.
     */
    public static Test suite() throws Exception {
        RolapUtil.checkTracing();
        MondrianProperties properties = MondrianProperties.instance();
        String testName = properties.TestName.get();
        String testClass = properties.TestClass.get();

        System.out.println("testName: " + testName);
        System.out.println("testClass: " + testClass);

        TestSuite suite = new TestSuite();
        if (testClass != null && !testClass.equals("")) {
            Class clazz = Class.forName(testClass);

            // use addTestSuite only if the class has test methods
            // Allows you to run individual queries with ResultComparatorTest

            boolean matchTestMethods = false;
            if (Test.class.isAssignableFrom(clazz)) {
                Method[] methods = clazz.getMethods();
                for (int i = 0; i < methods.length && !matchTestMethods; i++) {
                    matchTestMethods = methods[i].getName().startsWith("test");
                }
            }

            if (matchTestMethods) {
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
            suite.addTestSuite(RolapConnectionTest.class);
            suite.addTest(ResultComparatorTest.suite());
            suite.addTestSuite(DrillThroughTest.class);
            suite.addTestSuite(BasicQueryTest.class);
            suite.addTestSuite(FunctionTest.class);
            suite.addTestSuite(HierarchyBugTest.class);
            suite.addTestSuite(ScheduleTest.class);
            suite.addTestSuite(UtilTestCase.class);
            if (isRunOnce()) suite.addTestSuite(TestAggregationManager.class);
            suite.addTestSuite(VirtualCubeTest.class);
            suite.addTestSuite(ParameterTest.class);
            suite.addTestSuite(AccessControlTest.class);
            suite.addTestSuite(ParserTest.class);
            suite.addTestSuite(ParentChildHierarchyTest.class);
            suite.addTestSuite(XmlaBasicTest.class);
            suite.addTestSuite(XmlaExcel2000Test.class);
            suite.addTestSuite(XmlaExcelXPTest.class);
            suite.addTest(XmlaTest.suite());
            if (isRunOnce()) suite.addTestSuite(TestCalculatedMembers.class);
            suite.addTestSuite(RaggedHierarchyTest.class);
            suite.addTestSuite(InlineTableTest.class);
            suite.addTestSuite(CompatibilityTest.class);
            suite.addTestSuite(CaptionTest.class);
            suite.addTestSuite(UdfTest.class);
            suite.addTestSuite(NamedSetTest.class);
            suite.addTestSuite(PropertiesTest.class);
            suite.addTestSuite(MultipleHierarchyTest.class);
            suite.addTestSuite(I18nTest.class);
            suite.addTestSuite(FormatTest.class);
            suite.addTestSuite(CmdRunnerTest.class);

            boolean testNonEmpty = isRunOnce();
            if (!MondrianProperties.instance().EnableNativeNonEmpty.get())
                testNonEmpty = false;
            if (!MondrianProperties.instance().EnableNativeCrossJoin.get())
                testNonEmpty = false;
            if (testNonEmpty)
              suite.addTestSuite(NonEmptyTest.class);
            else
                logger.warn("skipping NonEmptyTests");
        }
        if (testName != null && !testName.equals("")) {
            // Filter the suite, so that only tests whose names match
            // "testName" (in its entirety) will be run.
            Pattern testPattern = Pattern.compile(testName);
            suite = copySuite(suite, testPattern);
        }
        return suite;
    }

    /**
     * Check to see if the tests are running one user, one iteration.
     * Some tests are not thread safe so have to be skipped if this is not true.
     */
    private static boolean isRunOnce() {
        final MondrianProperties properties = MondrianProperties.instance();
        return !properties.Warmup.get() &&
                properties.VUsers.get() == 1 &&
                properties.Iterations.get() == 1;
    }

    /**
     * Make a copy of a suite, filtering certain tests.
     */
    private static TestSuite copySuite(TestSuite suite, Pattern testPattern) {
        TestSuite newSuite = new TestSuite();
        Enumeration tests = suite.tests();
        while (tests.hasMoreElements()) {
            Test test = (Test) tests.nextElement();
            if (test instanceof TestCase) {
                TestCase testCase = (TestCase) test;
                final String testName = testCase.getName();
                if (testPattern == null || testPattern.matcher(testName).matches()) {
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

// End Main.java
