/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 1998-2002 Kana Software, Inc.
// Copyright (C) 2001-2008 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 21 January, 1999
*/

package mondrian.test;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.regex.Pattern;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestResult;
import junit.framework.TestSuite;
import mondrian.olap.fun.*;
import mondrian.olap.fun.vba.VbaTest;
import mondrian.olap.*;
import mondrian.olap.type.TypeTest;
import mondrian.rolap.*;
import mondrian.rolap.sql.SqlQueryTest;
import mondrian.test.comp.ResultComparatorTest;
import mondrian.udf.*;
import mondrian.util.*;
import mondrian.xmla.*;
import mondrian.xmla.impl.DynamicDatasourceXmlaServletTest;
import mondrian.xmla.test.XmlaTest;
import mondrian.test.clearview.*;
import mondrian.calc.impl.ConstantCalcTest;
import mondrian.rolap.agg.AggregationOnDistinctCountMeasuresTest;
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
    /*
     * Scratch area to store information on the emerging test suite.
     */
    private static Map<TestSuite, String> testSuiteInfo = 
        new HashMap<TestSuite, String>();
    
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
            for (String error : errors) {
                pw.println(error);
            }
            pw.flush();
            System.exit(1);
        }
    }

    /**
     * Creates and runs the root test suite.
     *
     * @param args Command-line arguments
     * @throws Exception on error
     */
    private void run(String[] args) throws Exception {
        final MondrianProperties properties = MondrianProperties.instance();
        Test test = suite();
        if (args.length == 1 && args[0].equals("-l")) {
            /*
             * Only lists the tests to run if invoking ant test-nobuild next.
             */
            return;
        }
        
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
     *
     * @return test suite
     * @throws Exception on error
     */
    private static Test suite() throws Exception {
        RolapUtil.checkTracing();
        MondrianProperties properties = MondrianProperties.instance();
        String testName = properties.TestName.get();
        String testClass = properties.TestClass.get();

        System.out.println("testName: " + testName);
        System.out.println("testClass: " + testClass);
        System.out.println("java.version: " + System.getProperty("java.version"));

        TestSuite suite = new TestSuite();
        if (testClass != null && !testClass.equals("")) {
            //noinspection unchecked
            Class<? extends TestCase> clazz =
                (Class<? extends TestCase>) Class.forName(testClass);

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
                addTest(suite, clazz);
            } else {
                // e.g. testClass = "mondrian.olap.fun.BuiltinFunTable". Class
                // does not implement Test, so look for a 'public [static]
                // Test suite()' method.
                Method method = clazz.getMethod("suite", new Class[0]);
                TestCase target;
                if (Modifier.isStatic(method.getModifiers())) {
                    target = null;
                } else {
                    target = clazz.newInstance();
                }
                Object o = method.invoke(target);
                addTest(suite, (Test) o, clazz.getName() + method.getName());
            }
        } else {
            addTest(suite, IndexedValuesTest.class);
            addTest(suite, MemoryMonitorTest.class);
            addTest(suite, ObjectPoolTest.class);
            addTest(suite, RolapConnectionTest.class);
            addTest(suite, DialectTest.class);
            addTest(suite, ResultComparatorTest.class, "suite");
            addTest(suite, DrillThroughTest.class);
            addTest(suite, BasicQueryTest.class);
            addTest(suite, CVBasicTest.class, "suite");
            addTest(suite, GrandTotalTest.class, "suite");
            addTest(suite, MetricFilterTest.class, "suite");
            addTest(suite, MiscTest.class, "suite");
            addTest(suite, PredicateFilterTest.class, "suite");
            addTest(suite, SubTotalTest.class, "suite");
            addTest(suite, SummaryMetricPercentTest.class, "suite");
            addTest(suite, SummaryTest.class, "suite");
            addTest(suite, TopBottomTest.class, "suite");
            addTest(suite, CacheControlTest.class);
            addTest(suite, FunctionTest.class);
            addTest(suite, VbaTest.class);
            addTest(suite, HierarchyBugTest.class);
            addTest(suite, ScheduleTest.class);
            addTest(suite, UtilTestCase.class);
            addTest(suite, SortTest.class);
            if (isRunOnce()) addTest(suite, TestAggregationManager.class);
            addTest(suite, VirtualCubeTest.class);
            addTest(suite, ParameterTest.class);
            addTest(suite, AccessControlTest.class);
            addTest(suite, ParserTest.class);
            addTest(suite, ParentChildHierarchyTest.class);
            addTest(suite, XmlaBasicTest.class);
            addTest(suite, XmlaErrorTest.class);
            addTest(suite, XmlaExcel2000Test.class);
            addTest(suite, XmlaExcelXPTest.class);
            addTest(suite, XmlaCognosTest.class);
            addTest(suite, XmlaTabularTest.class);
            addTest(suite, XmlaTests.class);
            addTest(suite, DynamicDatasourceXmlaServletTest.class);
            addTest(suite, XmlaTest.class, "suite");
            if (isRunOnce()) addTest(suite, TestCalculatedMembers.class);
            addTest(suite, RaggedHierarchyTest.class);
            addTest(suite, NonEmptyPropertyForAllAxisTest.class);
            addTest(suite, InlineTableTest.class);
            addTest(suite, CompatibilityTest.class);
            addTest(suite, CaptionTest.class);
            addTest(suite, UdfTest.class);
            addTest(suite, NullValueTest.class);
            addTest(suite, NamedSetTest.class);
            addTest(suite, PropertiesTest.class);
            addTest(suite, MultipleHierarchyTest.class);
            addTest(suite, I18nTest.class);
            addTest(suite, FormatTest.class);
            addTest(suite, ParallelTest.class);
            addTest(suite, SchemaTest.class);
            // GroupingSetQueryTest must be run before any test derived from
            // CsvDBTestCase
            addTest(suite, GroupingSetQueryTest.class);
            addTest(suite, CmdRunnerTest.class);
            addTest(suite, DataSourceChangeListenerTest.class);
            addTest(suite, ModulosTest.class);
            addTest(suite, PrimeFinderTest.class);
            addTest(suite, CellKeyTest.class);
            addTest(suite, RolapAxisTest.class);
            addTest(suite, MemberHelperTest.class);
            addTest(suite, CrossJoinTest.class);
            addTest(suite, RolapResultTest.class);
            addTest(suite, ConstantCalcTest.class);
            addTest(suite, SharedDimensionTest.class);
            addTest(suite, CellPropertyTest.class);
            addTest(suite, QueryTest.class);
            addTest(suite, RolapSchemaReaderTest.class);
            addTest(suite, RolapCubeTest.class);
            addTest(suite, NullMemberRepresentationTest.class);
            addTest(suite, IgnoreUnrelatedDimensionsTest.class);
            addTest(suite, IgnoreMeasureForNonJoiningDimensionInAggregationTest.class);
            addTest(suite, SetFunDefTest.class);
            addTest(suite, AggregationOnDistinctCountMeasuresTest.class);
            addTest(suite, BitKeyTest.class);
            addTest(suite, TypeTest.class);

            boolean testNonEmpty = isRunOnce();
            if (!MondrianProperties.instance().EnableNativeNonEmpty.get()) {
                testNonEmpty = false;
            }
            if (!MondrianProperties.instance().EnableNativeCrossJoin.get()) {
                testNonEmpty = false;
            }
            if (testNonEmpty) {
                addTest(suite, NonEmptyTest.class);
            } else {
                logger.warn("skipping NonEmptyTests");
            }

            addTest(suite, FastBatchingCellReaderTest.class);
            addTest(suite, SqlQueryTest.class);

            if (MondrianProperties.instance().EnableNativeCrossJoin.get()) {
                addTest(suite, BatchedFillTest.class, "suite");
            } else {
                logger.warn("skipping BatchedFillTests");
            }
        }

        if (testName != null && !testName.equals("")) {
            // Filter the suite,  so that only tests whose names match
            // "testName" (in its entirety) will be run.
            Pattern testPattern = Pattern.compile(testName);
            suite = copySuite(suite,  testPattern);
        }
        
        String testInfo = testSuiteInfo.get(suite);
        
        if (testInfo != null && testInfo.length() > 0) {
            System.out.println(testInfo);
        } else {
            System.out.println("No tests to run. Check mondrian.properties setting.");
        }
        
        System.out.flush();
        return suite;
    }

    /**
     * Checks to see if the tests are running one user, one iteration.
     * Some tests are not thread safe so have to be skipped if this is not true.
     *
     * @return whether the tests are run with one user, one iteration
     */
    private static boolean isRunOnce() {
        final MondrianProperties properties = MondrianProperties.instance();
        return !properties.Warmup.get() &&
                properties.VUsers.get() == 1 &&
                properties.Iterations.get() == 1;
    }

    /**
     * Makes a copy of a suite, filtering certain tests.
     *
     * @param suite Test suite
     * @param testPattern Regular expression of name of tests to include
     * @return copy of test suite
     * @throws Exception on error
     */
    private static TestSuite copySuite(TestSuite suite, Pattern testPattern) 
        throws Exception
    {
        TestSuite newSuite = new TestSuite(suite.getName());
        Enumeration tests = suite.tests();
        while (tests.hasMoreElements()) {
            Test test = (Test) tests.nextElement();
            if (test instanceof TestCase) {
                TestCase testCase = (TestCase) test;
                final String testName = testCase.getName();
                if (testPattern == null || testPattern.matcher(testName).matches()) {
                    addTest(newSuite, test, suite.getName() + testName);
                }
            } else if (test instanceof TestSuite) {
                TestSuite subSuite = copySuite((TestSuite) test, testPattern);
                if (subSuite.countTestCases() > 0) {
                   addTest(newSuite, subSuite, subSuite.getName());
                }
            } else {
                // some other kind of test
                addTest(newSuite, test, " ");
            }
        }
        return newSuite;
    }
    
    private static void addTest(
        TestSuite suite,
        Class<? extends TestCase> testClass) throws Exception
    {
        int startTestCount = suite.countTestCases();
        suite.addTestSuite(testClass);
        int endTestCount = suite.countTestCases();
        printTestInfo(suite, testClass.getName(), startTestCount, endTestCount);
    }

    private static void addTest(
        TestSuite suite,
        Class<? extends TestCase> testClass,
        String testMethod) throws Exception
    {
        Method method = testClass.getMethod(testMethod);
        Object o = method.invoke(null);
        int startTestCount = suite.countTestCases();        
        suite.addTest((Test) o);
        int endTestCount = suite.countTestCases();
        printTestInfo(suite, testClass.getName(), startTestCount, endTestCount);
    }
    
    private static void addTest(
        TestSuite suite,
        Test tests,
        String testClassName) throws Exception
    {
        int startTestCount = suite.countTestCases();        
        suite.addTest(tests);
        int endTestCount = suite.countTestCases();
        printTestInfo(suite, testClassName, startTestCount, endTestCount);
    }
    
    private static void printTestInfo(
        TestSuite suite, String testClassName, int startCount, int endCount) {
        String testInfo = testSuiteInfo.get(suite);
        String newTestInfo = 
            "[" + startCount + " - " + endCount + "] : " + testClassName + "\n";
        if (testInfo == null) {
            testInfo = newTestInfo;            
        } else {
            testInfo += newTestInfo;                        
        }
        testSuiteInfo.put(suite, testInfo);
    }
}

// End Main.java
