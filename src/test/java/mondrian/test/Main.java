/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 1998-2005 Julian Hyde
// Copyright (C) 2005-2014 Pentaho and others
// All Rights Reserved.
//
// jhyde, 21 January, 1999
*/
package mondrian.test;

import mondrian.calc.impl.ConstantCalcTest;
import mondrian.olap.*;
import mondrian.olap.fun.*;
import mondrian.olap.fun.vba.ExcelTest;
import mondrian.olap.fun.vba.VbaTest;
import mondrian.olap.type.TypeTest;
import mondrian.rolap.*;
import mondrian.rolap.agg.*;
import mondrian.rolap.aggmatcher.*;
import mondrian.rolap.sql.SelectNotInGroupByTest;
import mondrian.rolap.sql.SqlQueryTest;
import mondrian.test.build.CodeComplianceTest;
import mondrian.test.clearview.*;
import mondrian.test.comp.ResultComparatorTest;
import mondrian.udf.CurrentDateMemberUdfTest;
import mondrian.udf.NullValueTest;
import mondrian.util.*;
import mondrian.xmla.*;
import mondrian.xmla.impl.DynamicDatasourceXmlaServletTest;
import mondrian.xmla.test.XmlaTest;

import junit.framework.Test;
import junit.framework.*;

import org.apache.log4j.Logger;

import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;

/**
 * Main test suite for Mondrian.
 *
 * <p>The {@link #suite()} method returns a suite which contains all other
 * Mondrian tests.
 *
 * @author jhyde
 */
public class Main extends TestSuite {
    private static final Logger logger = Logger.getLogger(Main.class);

    /** Scratch area to store information on the emerging test suite. */
    private static Map<TestSuite, String> testSuiteInfo =
        new HashMap<TestSuite, String>();

    private static final boolean RUN_OPTIONAL_TESTS = false;

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
            // Only lists the tests to run if invoking ant test-nobuild next.
            return;
        }

        if (properties.Warmup.get()) {
            System.out.println("Starting warmup run...");
            MondrianTestRunner runner = new MondrianTestRunner();
            TestResult tres = runner.doRun(test);
            if (!tres.wasSuccessful()) {
                System.out.println(
                    "Warmup run failed. Regular tests will not be run.");
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
        if (!tres.wasSuccessful()) {
            System.exit(1);
        }
    }

    /**
     * Creates a TestSuite to test the whole of mondrian. Methods with the
     * signature <code>public static Test suite()</code> are recognized
     * automatically by JUnit test-harnesses; see {@link TestSuite}.
     *
     * @return test suite
     * @throws Exception on error
     */
    public static Test suite() throws Exception {
        MondrianProperties properties = MondrianProperties.instance();
        final String testName = properties.TestName.get();
        String testClass = properties.TestClass.get();

        System.out.println("testName: " + testName);
        System.out.println("testClass: " + testClass);
        System.out.println(
            "java.version: " + System.getProperty("java.version"));

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
            if (RUN_OPTIONAL_TESTS) {
                addTest(suite, SegmentLoaderTest.class); // 2f, 1e as of 13571
                addTest(suite, AggGenTest.class); // passes
                addTest(suite, DefaultRuleTest.class); // passes
                addTest(suite, SelectNotInGroupByTest.class);
                addTest(suite, CVConcurrentMdxTest.class);
                addTest(suite, CacheHitTest.class);
                addTest(suite, ConcurrentMdxTest.class);
                addTest(suite, MemHungryTest.class, "suite");
                addTest(suite, MultiDimTest.class, "suite");
                addTest(suite, MultiDimVCTest.class, "suite");
                addTest(suite, MultiLevelTest.class, "suite");
                addTest(suite, MultiLevelVCTest.class, "suite");
                addTest(suite, PartialCacheTest.class, "suite");
                addTest(suite, PartialCacheVCTest.class, "suite");
                addTest(suite, QueryAllTest.class, "suite");
                addTest(suite, QueryAllVCTest.class, "suite");
                addTest(suite, Base64Test.class);
                return suite;
            }
            addTest(suite, SegmentBuilderTest.class);
            addTest(suite, NativeFilterMatchingTest.class);
            addTest(suite, RolapConnectionTest.class);
            addTest(suite, FilteredIterableTest.class);
            addTest(suite, IndexedValuesTest.class);
            addTest(suite, MemoryMonitorTest.class);
            addTest(suite, ObjectPoolTest.class);
            addTest(suite, Ssas2005CompatibilityTest.class);
            addTest(suite, DialectTest.class);
            addTest(suite, ResultComparatorTest.class, "suite");
            addTest(suite, DrillThroughTest.class);
            addTest(suite, ScenarioTest.class);
            addTest(suite, BasicQueryTest.class);
            addTest(suite, SegmentCacheTest.class);
            addTest(suite, CVBasicTest.class, "suite");
            addTest(suite, GrandTotalTest.class, "suite");
            addTest(suite, HangerDimensionClearViewTest.class, "suite");
            addTest(suite, MetricFilterTest.class, "suite");
            addTest(suite, MiscTest.class, "suite");
            addTest(suite, PredicateFilterTest.class, "suite");
            addTest(suite, SubTotalTest.class, "suite");
            addTest(suite, SummaryMetricPercentTest.class, "suite");
            addTest(suite, SummaryTest.class, "suite");
            addTest(suite, TopBottomTest.class, "suite");
            addTest(suite, OrderTest.class, "suite");
            addTest(suite, CacheControlTest.class);
            addTest(suite, MemberCacheControlTest.class);
            addTest(suite, FunctionTest.class);
            addTest(suite, CurrentDateMemberUdfTest.class);
            addTest(suite, PartialSortTest.class);
            addTest(suite, VbaTest.class);
            addTest(suite, ExcelTest.class);
            addTest(suite, HierarchyBugTest.class);
            addTest(suite, ScheduleTest.class);
            addTest(suite, UtilTestCase.class);
            addTest(suite, PartiallyOrderedSetTest.class);
            addTest(suite, ConcatenableListTest.class);
            addTest(suite, Olap4jTest.class);
            addTest(suite, SortTest.class);
            if (isRunOnce()) {
                addTest(suite, TestAggregationManager.class);
            }
            addTest(suite, VirtualCubeTest.class);
            addTest(suite, ParameterTest.class);
            addTest(suite, AccessControlTest.class);
            addTest(suite, ParserTest.class);
            addTest(suite, CustomizedParserTest.class);
            addTest(suite, SolveOrderScopeIsolationTest.class);
            addTest(suite, ParentChildHierarchyTest.class);
            addTest(suite, ClosureSqlTest.class);
            addTest(suite, Olap4jTckTest.class, "suite");
            addTest(suite, MondrianServerTest.class);
            addTest(suite, XmlaBasicTest.class);
            addTest(suite, XmlaMetaDataConstraintsTest.class);
            addTest(suite, XmlaErrorTest.class);
            addTest(suite, XmlaExcel2000Test.class);
            addTest(suite, XmlaExcelXPTest.class);
            addTest(suite, XmlaExcel2007Test.class);
            addTest(suite, XmlaCognosTest.class);
            addTest(suite, XmlaTabularTest.class);
            addTest(suite, XmlaTests.class);
            addTest(suite, DynamicDatasourceXmlaServletTest.class);
            addTest(suite, XmlaTest.class, "suite");
            if (isRunOnce()) {
                addTest(suite, TestCalculatedMembers.class);
            }
            addTest(suite, CompoundSlicerTest.class);
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
            addTest(suite, SchemaVersionTest.class);
            addTest(suite, SchemaTest.class);
            addTest(suite, HangerDimensionTest.class);
            addTest(suite, DateTableBuilderTest.class);
            addTest(suite, PerformanceTest.class);
            // GroupingSetQueryTest must be run before any test derived from
            // CsvDBTestCase
            addTest(suite, GroupingSetQueryTest.class);
            addTest(suite, CmdRunnerTest.class);
            addTest(suite, ModulosTest.class);
            addTest(suite, PrimeFinderTest.class);
            addTest(suite, CellKeyTest.class);
            addTest(suite, RolapAxisTest.class);
            addTest(suite, CrossJoinTest.class);
            if (Bug.BugMondrian503Fixed) {
                addTest(suite, RolapResultTest.class);
            }
            addTest(suite, ConstantCalcTest.class);
            addTest(suite, SharedDimensionTest.class);
            addTest(suite, CellPropertyTest.class);
            addTest(suite, QueryTest.class);
            addTest(suite, RolapSchemaReaderTest.class);
            addTest(suite, RolapSchemaTest.class);
            addTest(suite, RolapCubeTest.class);
            addTest(suite, NullMemberRepresentationTest.class);
            addTest(suite, IgnoreUnrelatedDimensionsTest.class);
            addTest(
                suite,
                IgnoreMeasureForNonJoiningDimensionInAggregationTest.class);
            addTest(suite, SetFunDefTest.class);
            addTest(suite, VisualTotalsTest.class);
            addTest(suite, AggregationOnDistinctCountMeasuresTest.class);
            addTest(suite, NonCollapsedAggTest.class);
            addTest(suite, BitKeyTest.class);
            addTest(suite, TypeTest.class);
            addTest(suite, SteelWheelsSchemaTest.class);
            addTest(suite, MultipleColsInTupleAggTest.class);
            addTest(suite, DynamicSchemaProcessorTest.class);
            addTest(suite, MonitorTest.class);
            addTest(suite, BlockingHashMapTest.class);
            addTest(suite, CodeComplianceTest.class);

            boolean testNonEmpty = isRunOnce();
            if (!MondrianProperties.instance().EnableNativeNonEmpty.get()) {
                testNonEmpty = false;
            }
            if (!MondrianProperties.instance().EnableNativeCrossJoin.get()) {
                testNonEmpty = false;
            }
            if (testNonEmpty) {
                addTest(suite, NonEmptyTest.class);
                addTest(suite, FilterTest.class);
                if (Bug.BugMondrian1315Fixed) {
                    addTest(suite, NativizeSetFunDefTest.class);
                }
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

            // Must be the last test.
            addTest(suite, TerminatorTest.class);
        }

        if (testName != null && !testName.equals("")) {
            // Filter the suite,  so that only tests whose names match
            // "testName" (in its entirety) will be run.
            suite = TestContext.copySuite(
                suite, TestContext.patternPredicate(testName));
        }

        String testInfo = testSuiteInfo.get(suite);

        if (testInfo != null && testInfo.length() > 0) {
            System.out.println(testInfo);
        } else {
            System.out.println(
                "No tests to run. Check mondrian.properties setting.");
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
        return !properties.Warmup.get()
            && properties.VUsers.get() == 1
            && properties.Iterations.get() == 1;
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
        Class<? extends TestCase> testClass,
        Util.Predicate1<Test> predicate)
    {
        final TestSuite tempSuite = new TestSuite();
        tempSuite.addTestSuite(testClass);

        int startTestCount = suite.countTestCases();
        TestContext.copyTests(suite, tempSuite, predicate);
        int endTestCount = suite.countTestCases();
        printTestInfo(suite, testClass.getName(), startTestCount, endTestCount);
    }

    private static void addTest(
        TestSuite suite,
        Test tests,
        String testClassName)
    {
        int startTestCount = suite.countTestCases();
        suite.addTest(tests);
        int endTestCount = suite.countTestCases();
        printTestInfo(suite, testClassName, startTestCount, endTestCount);
    }

    private static void printTestInfo(
        TestSuite suite, String testClassName, int startCount, int endCount)
    {
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

    /**
     * Test that executes last. It can be used to check invariants.
     */
    public static class TerminatorTest extends TestCase {
        public void testSqlStatementExecuteMatchesClose() {
            // Number of successful calls to SqlStatement.execute
            // should match number of calls to SqlStatement.close
            // (excluding calls to close where close has already been called).
            // If there is a mismatch, try debugging by adding SqlStatement.id
            // values to a Set<Long>.
            assertEquals(
                "SqlStatement instances still open: "
                + Counters.SQL_STATEMENT_EXECUTING_IDS,
                Counters.SQL_STATEMENT_EXECUTE_COUNT.get(),
                Counters.SQL_STATEMENT_CLOSE_COUNT.get());
        }
    }
}

// End Main.java
