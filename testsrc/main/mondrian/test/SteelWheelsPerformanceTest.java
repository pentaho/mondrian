/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2009-2012 Pentaho
// All Rights Reserved.
*/
package mondrian.test;

import junit.framework.TestCase;

import org.apache.log4j.Logger;

/**
 * Those performance tests use the steel wheels schema
 * and are not meant to be run as part of the CI test suite.
 * They must be deactivated by default.
 * @author LBoudreau
 */
public class SteelWheelsPerformanceTest extends TestCase {
    /**
     * Certain tests are enabled only if logging is enabled.
     */
    private static final Logger LOGGER =
        Logger.getLogger(SteelWheelsPerformanceTest.class);

    public SteelWheelsPerformanceTest(String name) {
        super(name);
    }

    /**
     * Returns the test context. Override this method if you wish to use a
     * different source for your FoodMart connection.
     */
    public TestContext getTestContext() {
        return TestContext.instance().with(TestContext.DataSet.STEELWHEELS);
    }

    /**
     * This test execute a specially crafted query with
     * tons of filters and sort to test the performance
     * of some bug fixes before/after.
     */
    public void testComplexFilters() throws Exception {
        if (!LOGGER.isDebugEnabled()) {
            return;
        }
        final String query =
            "with set [*NATIVE_CJ_SET] as 'NonEmptyCrossJoin([*BASE_MEMBERS_Product], NonEmptyCrossJoin([*BASE_MEMBERS_Markets], NonEmptyCrossJoin([*BASE_MEMBERS_Customers], NonEmptyCrossJoin([*BASE_MEMBERS_Time], [*BASE_MEMBERS_Order Status]))))'\n"
            + "  set [*METRIC_CJ_SET] as 'Filter(Filter([*NATIVE_CJ_SET], (([Measures].[*Sales_SEL~AGG] > 0.0) AND ([Measures].[*Quantity_SEL~AGG] > 0.0))), (NOT IsEmpty([Measures].[Quantity])))'\n"
            + "  set [*SORTED_ROW_AXIS] as 'Order([*CJ_ROW_AXIS], Ancestor([Markets].CurrentMember, [Markets].[Territory]).OrderKey, BASC, ([Product].[*TOTAL_MEMBER_SEL~AGG], [Measures].[*FORMATTED_MEASURE_0]), BDESC)'\n"
            + "  set [*SORTED_COL_AXIS] as 'Order([*CJ_COL_AXIS], Ancestor([Product].CurrentMember, [Product].[Line]).OrderKey, BDESC)'\n"
            + "  set [*METRIC_MEMBERS_Order Status] as 'Generate([*METRIC_CJ_SET], {[Order Status].CurrentMember})'\n"
            + "  set [*METRIC_MEMBERS_Markets] as 'Generate([*METRIC_CJ_SET], {[Markets].CurrentMember})'\n"
            + "  set [*NATIVE_MEMBERS_Order Status] as 'Generate([*NATIVE_CJ_SET], {[Order Status].CurrentMember})'\n"
            + "  set [*BASE_MEMBERS_Product] as '{[Product].[Vintage Cars].[Motor City Art Classics].[1911 Ford Town Car], [Product].[Planes].[Motor City Art Classics].[America West Airlines B757-200], [Product].[Ships].[Unimax Art Galleries].[HMS Bounty], [Product].[Planes].[Gearbox Collectibles].[P-51-D Mustang]}'\n"
            + "  set [*BASE_MEMBERS_Customers] as '[Customers].[Customer].Members'\n"
            + "  set [*NATIVE_MEMBERS_Customers] as 'Generate([*NATIVE_CJ_SET], {[Customers].CurrentMember})'\n"
            + "  set [*BASE_MEMBERS_Measures] as '{[Measures].[*FORMATTED_MEASURE_0]}'\n"
            + "  set [*BASE_MEMBERS_Time] as 'Filter([Time].[Months].Members, (NOT ([Time].CurrentMember IN {[Time].[2004].[QTR1].[Mar]})))'\n"
            + "  set [*METRIC_MEMBERS_Time] as 'Generate([*METRIC_CJ_SET], {[Time].CurrentMember})'\n"
            + "  set [*CJ_COL_AXIS] as 'Generate([*METRIC_CJ_SET], {Ancestor([Product].CurrentMember, [Product].[Line]).CalculatedChild(\"*DISPLAY_MEMBER\")})'\n"
            + "  set [*NATIVE_MEMBERS_Product] as 'Generate([*NATIVE_CJ_SET], {[Product].CurrentMember})'\n"
            + "  set [*BASE_MEMBERS_Markets] as 'Filter([Markets].[Country].Members, (([Markets].CurrentMember IN {[Markets].[EMEA].[Denmark], [Markets].[#null].[Germany], [Markets].[Japan].[Japan], [Markets].[Japan].[Philippines], [Markets].[#null].[South Africa], [Markets].[EMEA].[Spain], [Markets].[NA].[USA]}) AND (NOT (Ancestor([Markets].CurrentMember, [Markets].[Territory]) IN {[Markets].[#null]}))))'\n"
            + "  set [*NATIVE_MEMBERS_Markets] as 'Generate([*NATIVE_CJ_SET], {[Markets].CurrentMember})'\n"
            + "  set [*BASE_MEMBERS_Order Status] as '{[Order Status].[In Process], [Order Status].[Resolved], [Order Status].[Shipped]}'\n"
            + "  set [*NATIVE_MEMBERS_Time] as 'Generate([*NATIVE_CJ_SET], {[Time].CurrentMember})'\n"
            + "  set [*METRIC_MEMBERS_Product] as 'Generate([*METRIC_CJ_SET], {[Product].CurrentMember})'\n"
            + "  set [*CJ_ROW_AXIS] as 'Generate([*METRIC_CJ_SET], {(Ancestor([Markets].CurrentMember, [Markets].[Territory]).CalculatedChild(\"*DISPLAY_MEMBER\"), [Customers].CurrentMember)})'\n"
            + "  member [Product].[Ships].[*DISPLAY_MEMBER] as 'Aggregate(Filter([*METRIC_MEMBERS_Product], (Ancestor([Product].CurrentMember, [Product].[Line]) IS [Product].[Ships])))', SOLVE_ORDER = (- 102.0)\n"
            + "  member [Product].[Vintage Cars].[*CTX_MEMBER_SEL~AGG] as 'Aggregate(Filter([*NATIVE_MEMBERS_Product], (Ancestor([Product].CurrentMember, [Product].[Line]) IS [Product].[Vintage Cars])))', SOLVE_ORDER = (- 102.0)\n"
            + "  member [Markets].[Japan].[*DISPLAY_MEMBER] as 'Aggregate(Filter([*METRIC_MEMBERS_Markets], (Ancestor([Markets].CurrentMember, [Markets].[Territory]) IS [Markets].[Japan])))', SOLVE_ORDER = (- 100.0)\n"
            + "  member [Order Status].[*SLICER_MEMBER] as 'Aggregate([*METRIC_MEMBERS_Order Status])', SOLVE_ORDER = (- 400.0)\n"
            + "  member [Markets].[*CTX_MEMBER_SEL~AGG] as 'Aggregate([*NATIVE_MEMBERS_Markets])', SOLVE_ORDER = (- 100.0)\n"
            + "  member [Order Status].[*CTX_MEMBER_SEL~AGG] as 'Aggregate([*NATIVE_MEMBERS_Order Status])', SOLVE_ORDER = (- 403.0)\n"
            + "  member [Product].[Planes].[*DISPLAY_MEMBER] as 'Aggregate(Filter([*METRIC_MEMBERS_Product], (Ancestor([Product].CurrentMember, [Product].[Line]) IS [Product].[Planes])))', SOLVE_ORDER = (- 102.0)\n"
            + "  member [Measures].[*FORMATTED_MEASURE_0] as '[Measures].[Quantity]', FORMAT_STRING = \"#,###\", SOLVE_ORDER = 400.0\n"
            + "  member [Customers].[*CTX_MEMBER_SEL~AGG] as 'Aggregate([*NATIVE_MEMBERS_Customers])', SOLVE_ORDER = (- 101.0)\n"
            + "  member [Measures].[*Sales_SEL~AGG] as '([Measures].[Sales], Ancestor([Product].CurrentMember, [Product].[Line]).CalculatedChild(\"*CTX_MEMBER_SEL~AGG\"), [Markets].[*CTX_MEMBER_SEL~AGG], [Customers].[*CTX_MEMBER_SEL~AGG], [Time].[*CTX_MEMBER_SEL~AGG], [Order Status].[*CTX_MEMBER_SEL~AGG])', SOLVE_ORDER = 300.0\n"
            + "  member [Product].[Vintage Cars].[*DISPLAY_MEMBER] as 'Aggregate(Filter([*METRIC_MEMBERS_Product], (Ancestor([Product].CurrentMember, [Product].[Line]) IS [Product].[Vintage Cars])))', SOLVE_ORDER = (- 102.0)\n"
            + "  member [Time].[Time].[*SLICER_MEMBER] as 'Aggregate([*METRIC_MEMBERS_Time])', SOLVE_ORDER = (- 400.0)\n"
            + "  member [Markets].[EMEA].[*DISPLAY_MEMBER] as 'Aggregate(Filter([*METRIC_MEMBERS_Markets], (Ancestor([Markets].CurrentMember, [Markets].[Territory]) IS [Markets].[EMEA])))', SOLVE_ORDER = (- 100.0)\n"
            + "  member [Product].[*TOTAL_MEMBER_SEL~AGG] as 'Aggregate(Generate([*METRIC_CJ_SET], {[Product].CurrentMember}))', SOLVE_ORDER = (- 102.0)\n"
            + "  member [Measures].[*Quantity_SEL~AGG] as '([Measures].[Quantity], Ancestor([Product].CurrentMember, [Product].[Line]).CalculatedChild(\"*CTX_MEMBER_SEL~AGG\"), [Markets].[*CTX_MEMBER_SEL~AGG], [Customers].[*CTX_MEMBER_SEL~AGG], [Time].[*CTX_MEMBER_SEL~AGG], [Order Status].[*CTX_MEMBER_SEL~AGG])', SOLVE_ORDER = 300.0\n"
            + "  member [Markets].[NA].[*DISPLAY_MEMBER] as 'Aggregate(Filter([*METRIC_MEMBERS_Markets], (Ancestor([Markets].CurrentMember, [Markets].[Territory]) IS [Markets].[NA])))', SOLVE_ORDER = (- 100.0)\n"
            + "  member [Time].[Time].[*CTX_MEMBER_SEL~AGG] as 'Aggregate([*NATIVE_MEMBERS_Time])', SOLVE_ORDER = (- 402.0)\n"
            + "  member [Product].[Ships].[*CTX_MEMBER_SEL~AGG] as 'Aggregate(Filter([*NATIVE_MEMBERS_Product], (Ancestor([Product].CurrentMember, [Product].[Line]) IS [Product].[Ships])))', SOLVE_ORDER = (- 102.0)\n"
            + "  member [Product].[Planes].[*CTX_MEMBER_SEL~AGG] as 'Aggregate(Filter([*NATIVE_MEMBERS_Product], (Ancestor([Product].CurrentMember, [Product].[Line]) IS [Product].[Planes])))', SOLVE_ORDER = (- 102.0)\n"
            + "select Union(Crossjoin({[Product].[*TOTAL_MEMBER_SEL~AGG]}, [*BASE_MEMBERS_Measures]), Crossjoin([*SORTED_COL_AXIS], [*BASE_MEMBERS_Measures])) ON COLUMNS,\n"
            + "  [*SORTED_ROW_AXIS] ON ROWS\n"
            + "from [SteelWheelsSales]\n"
            + "where ([Time].[*SLICER_MEMBER], [Order Status].[*SLICER_MEMBER])\n";
        long start = System.currentTimeMillis();
        getTestContext().executeQuery(query);
        printDuration("Complex filters query performance", start);
    }

    private void printDuration(String desc, long t0) {
        final long t1 = System.currentTimeMillis();
        final long duration = t1 - t0;
        LOGGER.debug(desc + " took " + duration + " millis");
    }
}

// End SteelWheelsPerformanceTest.java
