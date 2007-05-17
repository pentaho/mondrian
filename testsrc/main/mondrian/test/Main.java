/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 1998-2002 Kana Software, Inc.
// Copyright (C) 2001-2007 Julian Hyde and others
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
import mondrian.olap.fun.SortTest;
import mondrian.olap.fun.CrossJoinTest;
import mondrian.olap.fun.MemberHelperTest;
import mondrian.olap.HierarchyBugTest;
import mondrian.olap.QueryTest;
import mondrian.olap.CellPropertyTest;
import mondrian.rolap.*;
import mondrian.rolap.aggmatcher.*;
import mondrian.test.comp.ResultComparatorTest;
import mondrian.udf.*;
import mondrian.util.*;
import mondrian.xmla.XmlaBasicTest;
import mondrian.xmla.XmlaExcel2000Test;
import mondrian.xmla.XmlaExcelXPTest;
import mondrian.xmla.XmlaErrorTest;
import mondrian.xmla.XmlaCognosTest;
import mondrian.xmla.impl.DynamicDatasourceXmlaServletTest;
import mondrian.xmla.test.XmlaTest;
import mondrian.test.clearview.*;
import mondrian.calc.impl.ConstantCalcTest;

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

    /**
     * Entry point to run test suite from the command line.
     *
     * @param args Command-line parameters.
     */
    public static void main(String[] args) {
        new Main().runSafe(args);
    }

    /**
     * Runs the suite, catching any exceptions and printing their stack trace.
     *
     * @param args Command-line arguments
     */
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
        System.out.println("java.version: " + System.getProperty("java.version"));

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
                Object o = method.invoke(target);
                suite.addTest((Test) o);
            }
        } else {
            suite.addTestSuite(MemoryMonitorTest.class);
            suite.addTestSuite(ObjectPoolTest.class);
            suite.addTestSuite(RolapConnectionTest.class);
            suite.addTest(ResultComparatorTest.suite());
            suite.addTestSuite(DrillThroughTest.class);
            suite.addTestSuite(BasicQueryTest.class);
            suite.addTest(CVBasicTest.suite());
            suite.addTest(GrandTotalTest.suite());
            suite.addTest(MetricFilterTest.suite());
            suite.addTest(MiscTest.suite());
            suite.addTest(PredicateFilterTest.suite());
            suite.addTest(SubTotalTest.suite());
            suite.addTest(SummaryMetricPercentTest.suite());
            suite.addTest(SummaryTest.suite());
            suite.addTest(TopBottomTest.suite());
            suite.addTestSuite(CacheControlTest.class);
            suite.addTestSuite(FunctionTest.class);
            suite.addTestSuite(HierarchyBugTest.class);
            suite.addTestSuite(ScheduleTest.class);
            suite.addTestSuite(UtilTestCase.class);
            suite.addTestSuite(SortTest.class);
            if (isRunOnce()) suite.addTestSuite(TestAggregationManager.class);
            suite.addTestSuite(VirtualCubeTest.class);
            suite.addTestSuite(ParameterTest.class);
            suite.addTestSuite(AccessControlTest.class);
            suite.addTestSuite(ParserTest.class);
            suite.addTestSuite(ParentChildHierarchyTest.class);
            suite.addTestSuite(XmlaBasicTest.class);
            suite.addTestSuite(XmlaErrorTest.class);
            suite.addTestSuite(XmlaExcel2000Test.class);
            suite.addTestSuite(XmlaExcelXPTest.class);
            suite.addTestSuite(XmlaCognosTest.class);
            suite.addTestSuite(DynamicDatasourceXmlaServletTest.class);
            suite.addTest(XmlaTest.suite());
            if (isRunOnce()) suite.addTestSuite(TestCalculatedMembers.class);
            suite.addTestSuite(RaggedHierarchyTest.class);
            suite.addTestSuite(NonEmptyPropertyForAllAxisTest.class);
            suite.addTestSuite(InlineTableTest.class);
            suite.addTestSuite(CompatibilityTest.class);
            suite.addTestSuite(CaptionTest.class);
            suite.addTestSuite(UdfTest.class);
            suite.addTestSuite(NullValueTest.class);
            suite.addTestSuite(NamedSetTest.class);
            suite.addTestSuite(PropertiesTest.class);
            suite.addTestSuite(MultipleHierarchyTest.class);
            suite.addTestSuite(I18nTest.class);
            suite.addTestSuite(FormatTest.class);
            suite.addTestSuite(ParallelTest.class);
            suite.addTestSuite(SchemaTest.class);
            suite.addTestSuite(CmdRunnerTest.class);
            suite.addTestSuite(BUG_1541077.class);
            suite.addTestSuite(DataSourceChangeListenerTest.class);
            suite.addTestSuite(ModulosTest.class);
            suite.addTestSuite(PrimeFinderTest.class);
            suite.addTestSuite(CellKeyTest.class);
            suite.addTestSuite(RolapAxisTest.class);
            suite.addTestSuite(MemberHelperTest.class);
            suite.addTestSuite(CrossJoinTest.class);
            suite.addTestSuite(RolapResultTest.class);
            suite.addTestSuite(ConstantCalcTest.class);
            suite.addTestSuite(SharedDimensionTest.class);
            suite.addTestSuite(CellPropertyTest.class);
            suite.addTestSuite(QueryTest.class);

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
        Method method = clazz.getMethod("suite");
        Object o = method.invoke(null);
        suite.addTest((Test) o);
    }
}

// End Main.java
