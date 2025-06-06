/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/

package mondrian.test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import mondrian.calc.impl.ConstantCalcTest;
import mondrian.olap.CellPropertyTest;
import mondrian.olap.CustomizedParserTest;
import mondrian.olap.HierarchyBugTest;
import mondrian.olap.IdBatchResolverTest;
import mondrian.olap.MondrianProperties;
import mondrian.olap.NullMemberRepresentationTest;
import mondrian.olap.ParserTest;
import mondrian.olap.QueryTest;
import mondrian.olap.UtilTestCase;
import mondrian.olap.fun.CachedExistsTest;
import mondrian.olap.fun.CrossJoinTest;
import mondrian.olap.fun.FunctionTest;
import mondrian.olap.fun.IifFunDefTest;
import mondrian.olap.fun.NativizeSetFunDefTest;
import mondrian.olap.fun.PropertiesFunctionTest;
import mondrian.olap.fun.SetFunDefTest;
import mondrian.olap.fun.SortTest;
import mondrian.olap.fun.UnionFunDefTest;
import mondrian.olap.fun.ValidMeasureFunDefTest;
import mondrian.olap.fun.VisualTotalsTest;
import mondrian.olap.fun.sort.PartialSortTest;
import mondrian.olap.fun.sort.SorterTest;
import mondrian.olap.fun.vba.ExcelTest;
import mondrian.olap.fun.vba.VbaTest;
import mondrian.olap.type.TypeTest;
import mondrian.olap4j.XmlaExtraTest;
import mondrian.rolap.BitKeyTest;
import mondrian.rolap.CacheControlTest;
import mondrian.rolap.CancellationTest;
import mondrian.rolap.CellKeyTest;
import mondrian.rolap.DataSourceChangeListenerTest;
import mondrian.rolap.FastBatchingCellReaderTest;
import mondrian.rolap.FilterTest;
import mondrian.rolap.GroupingSetQueryTest;
import mondrian.rolap.HighDimensionsTest;
import mondrian.rolap.IndexedValuesTest;
import mondrian.rolap.MemberCacheControlTest;
import mondrian.rolap.MemberCacheHelperTest;
import mondrian.rolap.ModulosTest;
import mondrian.rolap.NativeEvalVirtualCubeTest;
import mondrian.rolap.NativeFilterAgainstAggTableTest;
import mondrian.rolap.NativeFilterMatchingTest;
import mondrian.rolap.NonEmptyPropertyForAllAxisTest;
import mondrian.rolap.NonEmptyTest;
import mondrian.rolap.NumberSqlCompilerTest;
import mondrian.rolap.OrderKeyOneToOneCheckTest;
import mondrian.rolap.RestrictedMemberReaderTest;
import mondrian.rolap.RolapAxisTest;
import mondrian.rolap.RolapConnectionTest;
import mondrian.rolap.RolapCubeDimensionTest;
import mondrian.rolap.RolapCubeHierarchyTest;
import mondrian.rolap.RolapCubeTest;
import mondrian.rolap.RolapEvaluatorTest;
import mondrian.rolap.RolapMemberBaseTest;
import mondrian.rolap.RolapNativeSqlInjectionTest;
import mondrian.rolap.RolapNativeTopCountTest;
import mondrian.rolap.RolapNativeTopCountVersusNonNativeTest;
import mondrian.rolap.RolapResultTest;
import mondrian.rolap.RolapSchemaPoolConcurrencyTest;
import mondrian.rolap.RolapSchemaPoolTest;
import mondrian.rolap.RolapSchemaReaderTest;
import mondrian.rolap.RolapSchemaTest;
import mondrian.rolap.RolapStarTest;
import mondrian.rolap.RolapUtilTest;
import mondrian.rolap.SharedDimensionTest;
import mondrian.rolap.SqlConstraintUtilsTest;
import mondrian.rolap.SqlMemberSourceTest;
import mondrian.rolap.SqlStatementTest;
import mondrian.rolap.TestAggregationManager;
import mondrian.rolap.TopCountNativeEvaluatorTest;
import mondrian.rolap.TopCountWithTwoParamsVersusHeadTest;
import mondrian.rolap.VirtualCubeTest;
import mondrian.rolap.agg.AggregationOnDistinctCountMeasuresTest;
import mondrian.rolap.agg.AggregationOnInvalidRoleTest;
import mondrian.rolap.agg.AggregationOnInvalidRoleWhenNotIgnoringTest;
import mondrian.rolap.agg.DenseDoubleSegmentBodyTest;
import mondrian.rolap.agg.DenseIntSegmentBodyTest;
import mondrian.rolap.agg.GroupingSetsListTest;
import mondrian.rolap.agg.SegmentBuilderTest;
import mondrian.rolap.agg.SegmentCacheTest;
import mondrian.rolap.agg.SegmentLoaderTest;
import mondrian.rolap.aggmatcher.AggGenTest;
import mondrian.rolap.aggmatcher.AggSchemaScanTest;
import mondrian.rolap.aggmatcher.AggregationOverAggTableTest;
import mondrian.rolap.aggmatcher.DefaultRecognizerTest;
import mondrian.rolap.aggmatcher.DefaultRuleTest;
import mondrian.rolap.aggmatcher.ExplicitRecognizerTest;
import mondrian.rolap.aggmatcher.MultipleColsInTupleAggTest;
import mondrian.rolap.aggmatcher.NonCollapsedAggTest;
import mondrian.rolap.aggmatcher.SpeciesNonCollapsedAggTest;
import mondrian.rolap.aggmatcher.UsagePrefixTest;
import mondrian.rolap.cache.SegmentCacheIndexImplTest;
import mondrian.rolap.format.DefaultFormatterTest;
import mondrian.rolap.format.FormatterCreateContextTest;
import mondrian.rolap.format.FormatterFactoryTest;
import mondrian.rolap.sql.CodeSetTest;
import mondrian.rolap.sql.CrossJoinArgFactoryTest;
import mondrian.rolap.sql.EffectiveMemberCacheTest;
import mondrian.rolap.sql.SelectNotInGroupByTest;
import mondrian.rolap.sql.SqlQueryTest;
import mondrian.server.FileRepositoryTest;
import mondrian.spi.DialectUtilTest;
import mondrian.spi.impl.ImpalaDialectTest;
import mondrian.spi.impl.JdbcDialectImplTest;
import mondrian.spi.impl.MonetDbDialectTest;
import mondrian.spi.impl.MySqlDialectTest;
import mondrian.spi.impl.OracleDialectTest;
import mondrian.spi.impl.PostgreSqlDialectTest;
import mondrian.spi.impl.SybaseDialectTest;
import mondrian.test.build.CodeComplianceTest;
import mondrian.test.clearview.BatchedFillTest;
import mondrian.test.clearview.CVBasicTest;
import mondrian.test.clearview.GrandTotalTest;
import mondrian.test.clearview.HangerDimensionTest;
import mondrian.test.clearview.MemHungryTest;
import mondrian.test.clearview.MetricFilterTest;
import mondrian.test.clearview.MiscTest;
import mondrian.test.clearview.MultiDimTest;
import mondrian.test.clearview.MultiDimVCTest;
import mondrian.test.clearview.MultiLevelTest;
import mondrian.test.clearview.MultiLevelVCTest;
import mondrian.test.clearview.OrderTest;
import mondrian.test.clearview.PartialCacheTest;
import mondrian.test.clearview.PartialCacheVCTest;
import mondrian.test.clearview.PredicateFilterTest;
import mondrian.test.clearview.QueryAllTest;
import mondrian.test.clearview.QueryAllVCTest;
import mondrian.test.clearview.SubTotalTest;
import mondrian.test.clearview.SummaryMetricPercentTest;
import mondrian.test.clearview.SummaryTest;
import mondrian.test.clearview.TopBottomTest;
import mondrian.test.comp.ResultComparatorTest;
import mondrian.tui.NamespaceContextImplTest;
import mondrian.tui.XmlUtilTest;
import mondrian.udf.CurrentDateMemberUdfTest;
import mondrian.udf.NullValueTest;
import mondrian.util.Base64Test;
import mondrian.util.BlockingHashMapTest;
import mondrian.util.Bug;
import mondrian.util.CancellationCheckerTest;
import mondrian.util.ConcatenableListTest;
import mondrian.util.Counters;
import mondrian.util.ExpiringReferenceTest;
import mondrian.util.FilteredIterableTest;
import mondrian.util.FormatTest;
import mondrian.util.MemoryMonitorTest;
import mondrian.util.ObjectPoolTest;
import mondrian.util.PartiallyOrderedSetTest;
import mondrian.util.PrimeFinderTest;
import mondrian.util.ScheduleTest;
import mondrian.xmla.XmlaBasicTest;
import mondrian.xmla.XmlaCognosTest;
import mondrian.xmla.XmlaDimensionPropertiesTest;
import mondrian.xmla.XmlaErrorTest;
import mondrian.xmla.XmlaExcel2000Test;
import mondrian.xmla.XmlaExcel2007Test;
import mondrian.xmla.XmlaExcelXPTest;
import mondrian.xmla.XmlaMetaDataConstraintsTest;
import mondrian.xmla.XmlaTabularTest;
import mondrian.xmla.XmlaTests;
import mondrian.xmla.impl.DynamicDatasourceXmlaServletTest;
import mondrian.xmla.test.XmlaTest;

/**
 * Main test suite for Mondrian.
 *
 * <p>The {@link #suite()} method returns a suite which contains all other
 * Mondrian tests.
 *
 * @author jhyde
 */
public class Main extends TestSuite {
  private static final Logger logger = LogManager.getLogger( Main.class );
  /**
   * Scratch area to store information on the emerging test suite.
   */
  private static Map<TestSuite, String> testSuiteInfo =
    new HashMap<TestSuite, String>();

  private static final boolean RUN_OPTIONAL_TESTS = false;

  /**
   * Creates a TestSuite to test the whole of mondrian. Methods with the signature <code>public static Test
   * suite()</code> are recognized automatically by JUnit test-harnesses; see {@link TestSuite}.
   *
   * @return test suite
   * @throws Exception on error
   */
  public static Test suite() throws Exception {
    MondrianProperties properties = MondrianProperties.instance();
    String testName = properties.TestName.get();
    String testClass = properties.TestClass.get();

    System.out.println( "testName: " + testName );
    System.out.println( "testClass: " + testClass );
    System.out.println(
      "java.version: " + System.getProperty( "java.version" ) );

    TestSuite suite = new TestSuite();
    if ( testClass != null && !testClass.equals( "" ) ) {
      //noinspection unchecked
      Class<? extends TestCase> clazz =
        (Class<? extends TestCase>) Class.forName( testClass );

      // use addTestSuite only if the class has test methods
      // Allows you to run individual queries with ResultComparatorTest

      boolean matchTestMethods = false;
      if ( Test.class.isAssignableFrom( clazz ) ) {
        Method[] methods = clazz.getMethods();
        for ( int i = 0; i < methods.length && !matchTestMethods; i++ ) {
          matchTestMethods = methods[ i ].getName().startsWith( "test" );
        }
      }

      if ( matchTestMethods ) {
        // e.g. testClass = "mondrian.test.FoodMartTestCase",
        // the name of a class which extends TestCase. We will invoke
        // every method which starts with 'test'. (If "testName" is set,
        // we'll filter this list later.)
        addTest( suite, clazz );
      } else {
        // e.g. testClass = "mondrian.olap.fun.BuiltinFunTable". Class
        // does not implement Test, so look for a 'public [static]
        // Test suite()' method.
        Method method = clazz.getMethod( "suite", new Class[ 0 ] );
        TestCase target;
        if ( Modifier.isStatic( method.getModifiers() ) ) {
          target = null;
        } else {
          target = clazz.newInstance();
        }
        Object o = method.invoke( target );
        addTest( suite, (Test) o, clazz.getName() + method.getName() );
      }
    } else {
      if ( RUN_OPTIONAL_TESTS ) {
        addTest( suite, SegmentLoaderTest.class ); // 2f, 1e as of 13571
        addTest( suite, AggGenTest.class ); // passes
        addTest( suite, DefaultRuleTest.class ); // passes
        addTest( suite, SelectNotInGroupByTest.class );
        addTest( suite, CVConcurrentMdxTest.class );
        addTest( suite, CacheHitTest.class );
        addTest( suite, ConcurrentMdxTest.class );
        addTest( suite, MemHungryTest.class, "suite" );
        addTest( suite, MultiDimTest.class, "suite" );
        addTest( suite, MultiDimVCTest.class, "suite" );
        addTest( suite, MultiLevelTest.class, "suite" );
        addTest( suite, MultiLevelVCTest.class, "suite" );
        addTest( suite, PartialCacheTest.class, "suite" );
        addTest( suite, PartialCacheVCTest.class, "suite" );
        addTest( suite, QueryAllTest.class, "suite" );
        addTest( suite, QueryAllVCTest.class, "suite" );
        addTest( suite, Base64Test.class );
        addTest( suite, CodeComplianceTest.class );
        return suite;
      }
      addTest( suite, SqlMemberSourceTest.class );
      addTest( suite, SqlConstraintUtilsTest.class );
      addTest( suite, IifFunDefTest.class );
      addTest( suite, GroupingSetsListTest.class );
      addTest( suite, PropertiesFunctionTest.class );
      addTest( suite, SegmentBuilderTest.class );
      addTest( suite, DenseDoubleSegmentBodyTest.class );
      addTest( suite, DenseIntSegmentBodyTest.class );
      addTest( suite, NativeFilterMatchingTest.class );
      addTest( suite, NativeFilterAgainstAggTableTest.class );
      addTest( suite, RolapConnectionTest.class );
      addTest( suite, FilteredIterableTest.class );
      addTest( suite, HighDimensionsTest.class );
      addTest( suite, IndexedValuesTest.class );
      addTest( suite, MemoryMonitorTest.class );
      addTest( suite, ObjectPoolTest.class );
      addTest( suite, Ssas2005CompatibilityTest.OldBehaviorTest.class );
      addTest( suite, DialectTest.class );
      addTest( suite, ResultComparatorTest.class, "suite" );
      addTest( suite, DrillThroughTest.class );
      addTest( suite, DrillThroughFieldListTest.class );
      addTest( suite, DrillThroughExcludeFilterTest.class );
      addTest( suite, ScenarioTest.class );
      addTest( suite, BasicQueryTest.class );
      addTest( suite, SegmentCacheTest.class );
      addTest( suite, SegmentCacheIndexImplTest.class );
      addTest( suite, CVBasicTest.class, "suite" );
      addTest( suite, GrandTotalTest.class, "suite" );
      addTest( suite, HangerDimensionTest.class, "suite" );
      addTest( suite, MetricFilterTest.class, "suite" );
      addTest( suite, MiscTest.class, "suite" );
      addTest( suite, PredicateFilterTest.class, "suite" );
      addTest( suite, SubTotalTest.class, "suite" );
      addTest( suite, SummaryMetricPercentTest.class, "suite" );
      addTest( suite, SummaryTest.class, "suite" );
      addTest( suite, TopBottomTest.class, "suite" );
      addTest( suite, OrderTest.class, "suite" );
      addTest( suite, CacheControlTest.class );
      addTest( suite, MemberCacheControlTest.class );
      addTest( suite, FunctionTest.class );
      addTest( suite, CurrentDateMemberUdfTest.class );
      addTest( suite, PartialSortTest.class );
      addTest( suite, VbaTest.class );
      addTest( suite, ExcelTest.class );
      addTest( suite, HierarchyBugTest.class );
      addTest( suite, ScheduleTest.class );
      addTest( suite, UtilTestCase.class );
      addTest( suite, CancellationCheckerTest.class );
      addTest( suite, PartiallyOrderedSetTest.class );
      addTest( suite, ConcatenableListTest.class );
      addTest( suite, ExpiringReferenceTest.class );
      addTest( suite, Olap4jTest.class );
      addTest( suite, SortTest.class );
      addTest( suite, SorterTest.class );
      if ( isRunOnce() ) {
        addTest( suite, TestAggregationManager.class );
      }
      addTest( suite, VirtualCubeTest.class );
      addTest( suite, ParameterTest.class );
      addTest( suite, AccessControlTest.class );
      addTest( suite, ParserTest.class );
      addTest( suite, CustomizedParserTest.class );
      addTest( suite, SolveOrderScopeIsolationTest.class );
      addTest( suite, ParentChildHierarchyTest.class );
      addTest( suite, Olap4jTckTest.class, "suite" );
      addTest( suite, MondrianServerTest.class );
      addTest( suite, XmlaBasicTest.class );
      addTest( suite, XmlaMetaDataConstraintsTest.class );
      addTest( suite, XmlaErrorTest.class );
      addTest( suite, XmlaExcel2000Test.class );
      addTest( suite, XmlaExcelXPTest.class );
      addTest( suite, XmlaExcel2007Test.class );
      addTest( suite, XmlaCognosTest.class );
      addTest( suite, XmlaTabularTest.class );
      addTest( suite, XmlaTests.class );
      addTest( suite, DynamicDatasourceXmlaServletTest.class );
      addTest( suite, XmlaTest.class, "suite" );
      addTest( suite, XmlaDimensionPropertiesTest.class );
      if ( isRunOnce() ) {
        addTest( suite, TestCalculatedMembers.class );
      }
      addTest( suite, CompoundSlicerTest.class );
      addTest( suite, RaggedHierarchyTest.class );
      addTest( suite, NonEmptyPropertyForAllAxisTest.class );
      addTest( suite, InlineTableTest.class );
      addTest( suite, CompatibilityTest.class );
      addTest( suite, CaptionTest.class );
      addTest( suite, UdfTest.class );
      addTest( suite, UnicodeSpecialCharactersTest.class);
      addTest( suite, NullValueTest.class );
      addTest( suite, NamedSetTest.class );
      addTest( suite, NativeSetEvaluationTest.class );
      addTest( suite, PropertiesTest.class );
      addTest( suite, MultipleHierarchyTest.class );
      addTest( suite, I18nTest.class );
      addTest( suite, FormatTest.class );
      addTest( suite, ParallelTest.class );
      addTest( suite, SchemaVersionTest.class );
      addTest( suite, SchemaTest.class );
      addTest( suite, DefaultRecognizerTest.class );
      addTest( suite, PerformanceTest.class );
      // GroupingSetQueryTest must be run before any test derived from
      // CsvDBTestCase
      addTest( suite, GroupingSetQueryTest.class );
      addTest( suite, CmdRunnerTest.class );
      addTest( suite, DataSourceChangeListenerTest.class );
      addTest( suite, ModulosTest.class );
      addTest( suite, PrimeFinderTest.class );
      addTest( suite, CellKeyTest.class );
      addTest( suite, RolapAxisTest.class );
      addTest( suite, CrossJoinTest.class );
      if ( Bug.BugMondrian503Fixed ) {
        addTest( suite, RolapResultTest.class );
      }
      addTest( suite, ConstantCalcTest.class );
      addTest( suite, SharedDimensionTest.class );
      addTest( suite, CellPropertyTest.class );
      addTest( suite, QueryTest.class );
      addTest( suite, RolapSchemaReaderTest.class );
      addTest( suite, RolapCubeTest.class );
      addTest( suite, NumberSqlCompilerTest.class );
      addTest( suite, RolapEvaluatorTest.class );
      addTest( suite, RolapNativeSqlInjectionTest.class );
      addTest( suite, RolapNativeTopCountTest.class );
      addTest( suite, RolapNativeTopCountVersusNonNativeTest.class );
      addTest( suite, TopCountNativeEvaluatorTest.class );
      addTest( suite, TopCountWithTwoParamsVersusHeadTest.class );
      addTest( suite, RolapStarTest.class );
      addTest( suite, RolapSchemaTest.class );
      addTest( suite, RolapSchemaPoolTest.class );
      addTest( suite, RolapSchemaPoolConcurrencyTest.class );
      addTest( suite, NullMemberRepresentationTest.class );
      addTest( suite, IgnoreUnrelatedDimensionsTest.class );
      addTest(
        suite,
        IgnoreMeasureForNonJoiningDimensionInAggregationTest.class );
      addTest( suite, SetFunDefTest.class );
      addTest( suite, VisualTotalsTest.class );
      addTest( suite, AggregationOnDistinctCountMeasuresTest.class );
      addTest( suite, AggregationOnInvalidRoleTest.class );
      addTest( suite, AggregationOnInvalidRoleWhenNotIgnoringTest.class );
      addTest( suite, NonCollapsedAggTest.class );
      addTest( suite, SpeciesNonCollapsedAggTest.class );
      addTest( suite, UsagePrefixTest.class );
      addTest( suite, BitKeyTest.class );
      addTest( suite, TypeTest.class );
      addTest( suite, SteelWheelsSchemaTest.class );
      addTest( suite, MultipleColsInTupleAggTest.class );
      addTest( suite, DynamicSchemaProcessorTest.class );
      addTest( suite, LocalizingDynamicSchemaProcessorTest.class );
      addTest( suite, MonitorTest.class );
      addTest( suite, DeadlockTest.class );

      addTest( suite, BlockingHashMapTest.class );
      addTest( suite, FileRepositoryTest.class );
      addTest( suite, XmlaExtraTest.class );
      addTest( suite, CrossJoinArgFactoryTest.class );
      addTest( suite, UnionFunDefTest.class );
      addTest( suite, JdbcDialectImplTest.class );
      addTest( suite, ImpalaDialectTest.class );
      addTest( suite, SybaseDialectTest.class );
      addTest( suite, PostgreSqlDialectTest.class );
      addTest( suite, OracleDialectTest.class );
      addTest( suite, MySqlDialectTest.class );
      addTest( suite, MonetDbDialectTest.class );
      addTest( suite, DialectUtilTest.class );
      addTest( suite, IdBatchResolverTest.class );
      addTest( suite, MemberCacheHelperTest.class );
      addTest( suite, EffectiveMemberCacheTest.class );
      addTest( suite, SqlStatementTest.class );
      addTest( suite, ValidMeasureFunDefTest.class );

      boolean testNonEmpty = isRunOnce();
      if ( !MondrianProperties.instance().EnableNativeNonEmpty.get() ) {
        testNonEmpty = false;
      }
      if ( !MondrianProperties.instance().EnableNativeCrossJoin.get() ) {
        testNonEmpty = false;
      }
      if ( testNonEmpty ) {
        addTest( suite, NonEmptyTest.class );
        addTest( suite, FilterTest.class );
        addTest( suite, NativizeSetFunDefTest.class );
      } else {
        logger.warn( "skipping NonEmptyTests" );
      }

      addTest( suite, FastBatchingCellReaderTest.class );
      addTest( suite, SqlQueryTest.class );
      addTest( suite, CodeSetTest.class );
      addTest( suite, ExplicitRecognizerTest.class );
      addTest( suite, AggregationOverAggTableTest.class );
      addTest( suite, XmlUtilTest.class );
      addTest( suite, NativeEvalVirtualCubeTest.class );
      addTest( suite, NamespaceContextImplTest.class );
      addTest( suite, CancellationTest.class );

      if ( MondrianProperties.instance().EnableNativeCrossJoin.get() ) {
        addTest( suite, BatchedFillTest.class, "suite" );
      } else {
        logger.warn( "skipping BatchedFillTests" );
      }

      addTest( suite, RolapMemberBaseTest.class );
      addTest( suite, DefaultFormatterTest.class );
      addTest( suite, FormatterCreateContextTest.class );
      addTest( suite, FormatterFactoryTest.class );
      addTest( suite, OrderKeyOneToOneCheckTest.class );
      addTest( suite, RestrictedMemberReaderTest.class );

      addTest( suite, RolapCubeHierarchyTest.class );
      addTest( suite, RolapCubeDimensionTest.class );
      addTest( suite, RolapUtilTest.class );
      addTest( suite, AggSchemaScanTest.class );
      addTest( suite, MdcUtilTest.class );
      addTest( suite, CachedExistsTest.class );
      addTest( suite, ExplainPlanTest.class );

      // Must be the last test.
      addTest( suite, TerminatorTest.class );
    }

    if ( testName != null && !testName.equals( "" ) ) {
      // Filter the suite,  so that only tests whose names match
      // "testName" (in its entirety) will be run.
      Pattern testPattern = Pattern.compile( testName );
      suite = copySuite( suite, testPattern );
    }

    String testInfo = testSuiteInfo.get( suite );

    if ( testInfo != null && testInfo.length() > 0 ) {
      System.out.println( testInfo );
    } else {
      System.out.println(
        "No tests to run. Check mondrian.properties setting." );
    }

    System.out.flush();
    return suite;
  }

  /**
   * Checks to see if the tests are running one user, one iteration. Some tests are not thread safe so have to be
   * skipped if this is not true.
   *
   * @return whether the tests are run with one user, one iteration
   */
  private static boolean isRunOnce() {
    final MondrianProperties properties = MondrianProperties.instance();
    return !properties.Warmup.get()
      && properties.VUsers.get() == 1
      && properties.Iterations.get() == 1;
  }

  /**
   * Makes a copy of a suite, filtering certain tests.
   *
   * @param suite       Test suite
   * @param testPattern Regular expression of name of tests to include
   * @return copy of test suite
   * @throws Exception on error
   */
  private static TestSuite copySuite( TestSuite suite, Pattern testPattern )
    throws Exception {
    TestSuite newSuite = new TestSuite( suite.getName() );
    Enumeration<?> tests = suite.tests();
    while ( tests.hasMoreElements() ) {
      Test test = (Test) tests.nextElement();
      if ( test instanceof TestCase ) {
        TestCase testCase = (TestCase) test;
        final String testName = testCase.getName();
        if ( testPattern == null
          || testPattern.matcher( testName ).matches() ) {
          addTest( newSuite, test, suite.getName() + testName );
        }
      } else if ( test instanceof TestSuite ) {
        TestSuite subSuite = copySuite( (TestSuite) test, testPattern );
        if ( subSuite.countTestCases() > 0 ) {
          addTest( newSuite, subSuite, subSuite.getName() );
        }
      } else {
        // some other kind of test
        addTest( newSuite, test, " " );
      }
    }
    return newSuite;
  }

  private static void addTest(
    TestSuite suite,
    Class<? extends TestCase> testClass ) throws Exception {
    int startTestCount = suite.countTestCases();
    suite.addTestSuite( testClass );
    int endTestCount = suite.countTestCases();
    printTestInfo( suite, testClass.getName(), startTestCount, endTestCount );
  }

  private static void addTest(
    TestSuite suite,
    Class<? extends TestCase> testClass,
    String testMethod ) throws Exception {
    Method method = testClass.getMethod( testMethod );
    Object o = method.invoke( null );
    int startTestCount = suite.countTestCases();
    suite.addTest( (Test) o );
    int endTestCount = suite.countTestCases();
    printTestInfo( suite, testClass.getName(), startTestCount, endTestCount );
  }

  private static void addTest(
    TestSuite suite,
    Test tests,
    String testClassName ) throws Exception {
    int startTestCount = suite.countTestCases();
    suite.addTest( tests );
    int endTestCount = suite.countTestCases();
    printTestInfo( suite, testClassName, startTestCount, endTestCount );
  }

  private static void printTestInfo(
    TestSuite suite, String testClassName, int startCount, int endCount ) {
    String testInfo = testSuiteInfo.get( suite );
    String newTestInfo =
      "[" + startCount + " - " + endCount + "] : " + testClassName + "\n";
    if ( testInfo == null ) {
      testInfo = newTestInfo;
    } else {
      testInfo += newTestInfo;
    }
    testSuiteInfo.put( suite, testInfo );
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
        Counters.SQL_STATEMENT_CLOSE_COUNT.get() );
    }
  }
}

// End Main.java
