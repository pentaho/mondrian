/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 1998-2002 Kana Software, Inc.
// Copyright (C) 2001-2009 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
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
import mondrian.olap.CacheControl;
import mondrian.olap.Connection;
import mondrian.olap.Cube;
import mondrian.olap.MondrianProperties;
import mondrian.olap.Util;
import mondrian.olap.UtilTestCase;
import mondrian.rolap.*;
import mondrian.rolap.aggmatcher.*;
import mondrian.udf.*;
import mondrian.util.*;
import mondrian.test.clearview.*;
import mondrian.rolap.agg.AggregationManager;
import mondrian.rolap.agg.CountingAggregationManager;

import org.apache.log4j.Logger;

/**
 * The <code>CacheHitTest</code> class contains test suites that return
 * hit ratio of aggregation cache for various sequences of MDX queries.
 * This test should be run with mondrian property
 * <code>mondrian.rolap.agg.enableCacheHitCounters=true</code>
 * This is not run as part of Main test suite as it only reports
 * ratios for further investigations.
 *
 * @author kvu
 * @version $Id$
 */
public class CacheHitTest extends FoodMartTestCase {

    /**
     * Runs a set of small MDX queries that targets a small region
     * of aggregation cache sequentially. All queries reference
     * the relational Sales cube.
     *
     * @throws Exception
     */
    public void testSmallSetSequential() throws Exception {
        TestSuite suite = new TestSuite();
        suite.addTest(PartialCacheTest.suite());
        suite.addTest(MultiLevelTest.suite());
        suite.addTest(MultiDimTest.suite());
        suite.addTest(QueryAllTest.suite());

        System.out.println("== " + this.getName() + " ==");
        runTestSuiteInOrder(suite, 50);
        clearCache("Sales");
    }

    /**
     * Runs a set of small MDX queries that targets a small region
     * of aggregation cache in random order. All queries reference
     * the relational Sales cube.
     *
     * @throws Exception
     */
    public void testSmallSetRandom() throws Exception {
        TestSuite suite = new TestSuite();
        suite.addTest(PartialCacheTest.suite());
        suite.addTest(MultiLevelTest.suite());
        suite.addTest(MultiDimTest.suite());
        suite.addTest(QueryAllTest.suite());

        System.out.println("== " + this.getName() + " ==");
        runRandomSuite(suite, 200);
        clearCache("Sales");
    }

    /**
     * Runs a set of small MDX queries that targets a small region
     * of aggregation cache sequentially. All queries reference
     * the virtual Warehouse and Sales cube.
     *
     * @throws Exception
     */
    public void testSmallSetVCSequential() throws Exception {
        TestSuite suite = new TestSuite();
        suite.addTest(PartialCacheVCTest.suite());
        suite.addTest(MultiLevelVCTest.suite());
        suite.addTest(MultiDimVCTest.suite());
        suite.addTest(QueryAllVCTest.suite());

        System.out.println("== " + this.getName() + " ==");
        runTestSuiteInOrder(suite, 50);
        clearCache("Warehouse and Sales");
    }

    /**
     * Runs a set of small MDX queries that targets a small region
     * of aggregation cache in random order. All queries reference
     * the virtual Warehouse and Sales cube.
     *
     * @throws Exception
     */
    public void testSmallSetVCRandom() throws Exception {
        TestSuite suite = new TestSuite();
        suite.addTest(PartialCacheVCTest.suite());
        suite.addTest(MultiLevelVCTest.suite());
        suite.addTest(MultiDimVCTest.suite());
        suite.addTest(QueryAllVCTest.suite());

        System.out.println("== " + this.getName() + " ==");
        runRandomSuite(suite, 200);
        clearCache("Warehouse and Sales");
    }

    /**
     * Runs a set of bigger MDX queries that requires more memory
     * and targets a bigger region of cache in random order.
     * Queries reference to Sales cube as well as
     * Warehouse and Sales cube.
     *
     * @throws Exception
     */
    public void testBigSetRandom() throws Exception {
        TestSuite suite = new TestSuite();
        suite.addTest(MemHungryTest.suite());
        suite.addTest(PartialCacheTest.suite());
        suite.addTest(MultiLevelTest.suite());
        suite.addTest(MultiDimTest.suite());
        suite.addTest(QueryAllTest.suite());
        suite.addTest(PartialCacheVCTest.suite());
        suite.addTest(MultiLevelVCTest.suite());
        suite.addTest(MultiDimVCTest.suite());
        suite.addTest(QueryAllVCTest.suite());
        suite.addTest(CVBasicTest.suite());
        suite.addTest(GrandTotalTest.suite());
        suite.addTest(MetricFilterTest.suite());
        suite.addTest(MiscTest.suite());
        suite.addTest(PredicateFilterTest.suite());
        suite.addTest(SubTotalTest.suite());
        suite.addTest(SummaryMetricPercentTest.suite());
        suite.addTest(SummaryTest.suite());
        suite.addTest(TopBottomTest.suite());

        System.out.println("== " + this.getName() + " ==");
        runRandomSuite(suite, 200);
        clearCache("Sales");
        clearCache("Warehouse and Sales");
    }

    /**
     * Loops <code>n</code> times, each time run a random test case
     * in the test <code>suite</code>
     *
     * @param suite the suite of test cases
     * @param n number of times
     * @throws Exception
     */
    public void runRandomSuite(TestSuite suite, int n)
        throws Exception
    {
        TestResult tres = new TestResult();
        boolean origCacheHitCounters =
            MondrianProperties.instance().EnableCacheHitCounters.get();
        MondrianProperties.instance().EnableCacheHitCounters.set(true);

        AggregationManager aggMngr = AggregationManager.instance();
        assert aggMngr instanceof CountingAggregationManager;

        ((CountingAggregationManager) aggMngr).resetCounters();

        for (int i = 0; i < n; i++) {
            int suiteIdx = (int) (Math.random() * suite.testCount());
            TestSuite test = (TestSuite) suite.testAt(suiteIdx);
            int testIdx = (int) (Math.random() * test.testCount());
            test.testAt(testIdx).run(tres);
        }
        report((CountingAggregationManager) aggMngr);
        MondrianProperties.instance().EnableCacheHitCounters.set(
            origCacheHitCounters);
    }

    /**
     * Loops <code>numIte</code> times, each time run all child test
     * suite in the <code>suite</code>
     *
     * @param suite the suite of test suites
     * @param numIter number of iterations
     * @throws Exception
     */
    public void runTestSuiteInOrder(TestSuite suite, int numIter)
        throws Exception
    {
        TestResult tres = new TestResult();
        boolean origCacheHitCounters =
            MondrianProperties.instance().EnableCacheHitCounters.get();
        MondrianProperties.instance().EnableCacheHitCounters.set(true);

        AggregationManager aggMngr = AggregationManager.instance();
        assert aggMngr instanceof CountingAggregationManager;

        ((CountingAggregationManager) aggMngr).resetCounters();

        for (int i = 0; i < numIter; i++) {
            TestSuite test = (TestSuite) suite.testAt(i % suite.testCount());
            for (int j = 0; j < test.testCount(); j++) {
                test.testAt(j).run(tres);
            }
        }
        report((CountingAggregationManager) aggMngr);
        MondrianProperties.instance().EnableCacheHitCounters.set(
            origCacheHitCounters);
    }

    /**
     * Prints cache hit ratio
     *
     */
    public void report(CountingAggregationManager am) {
        System.out.println(
            "Number of requests: " + am.getRequestCount());
        System.out.println(
            "Number of misses: " + am.getMissCount());
        System.out.println(
            "Hit ratio ---> " + am.getHitRatio());
    }

    /**
     * Clears aggregation cache
     *
     * @param cube
     */
    public void clearCache(String cube) {
        final TestContext testContext = getTestContext();
        final CacheControl cacheControl =
            testContext.getConnection().getCacheControl(null);

        // Flush the entire cache.
        final Connection connection = testContext.getConnection();
        final Cube salesCube = connection.getSchema().lookupCube(cube, true);
        final CacheControl.CellRegion measuresRegion =
            cacheControl.createMeasuresRegion(salesCube);
        cacheControl.flush(measuresRegion);
    }
}

// End CacheHitTest.java
