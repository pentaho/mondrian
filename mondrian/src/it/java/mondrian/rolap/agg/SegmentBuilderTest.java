/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2002-2016 Pentaho Corporation..  All rights reserved.
*/
package mondrian.rolap.agg;

import mondrian.olap.*;
import mondrian.rolap.*;
import mondrian.spi.*;
import mondrian.test.*;
import mondrian.util.*;

import java.util.*;

import static mondrian.util.Pair.of;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static java.util.Collections.singleton;

/**
 * <p>Test for <code>SegmentBuilder</code>.</p>
 *
 * @author mcampbell
 */
public class SegmentBuilderTest extends BatchTestCase {

    public static final double MOCK_CELL_VALUE = 123.123;

    protected void setUp() throws Exception {
        super.setUp();
        propSaver.set(
            MondrianProperties.instance().EnableInMemoryRollup,
            true);
        propSaver.set(MondrianProperties.instance().EnableNativeNonEmpty, true);
        propSaver.set(
            MondrianProperties.instance().SparseSegmentDensityThreshold, .5);
        propSaver.set(
            MondrianProperties.instance().SparseSegmentCountThreshold, 1000);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        RolapUtil.setHook(null);
    }

    public void testRollupWithNullAxisVals() {
        // Perform two rollups.  One with two columns each containing 3 values.
        // The second with two columns containing 2 values + null.
        // The rolled up values should be equal in the two cases.
        Pair<SegmentHeader, SegmentBody> rollupNoNulls = SegmentBuilder.rollup(
            makeSegmentMap(
                new String[] {"col1", "col2"}, null, 3, 9, true,
                new boolean[] {false, false}),// each axis sets null axis flag=F
            Collections.singleton("col2"),
            null, RolapAggregator.Sum, Dialect.Datatype.Numeric);
        Pair<SegmentHeader, SegmentBody> rollupWithNullMembers =
            SegmentBuilder.rollup(
                makeSegmentMap(
                    new String[] {"col1", "col2"}, null, 2, 9, true,
                    // each axis sets null axis flag=T
                    new boolean[] {true, true}),
                Collections.singleton("col2"),
                null, RolapAggregator.Sum, Dialect.Datatype.Numeric);
        assertArraysAreEqual(
            (double[]) rollupNoNulls.getValue().getValueArray(),
            (double[]) rollupWithNullMembers.getValue().getValueArray());
        assertTrue(
            "Rolled up column should have nullAxisFlag set.",
            rollupWithNullMembers.getValue().getNullAxisFlags().length == 1
                && rollupWithNullMembers.getValue().getNullAxisFlags()[0]);
        assertEquals(
            "col2",
            rollupWithNullMembers.getKey().getConstrainedColumns()
                .get(0).columnExpression);
    }

    public void testRollupWithMixOfNullAxisValues() {
        // constructed segment has 3 columns:
        //    2 values in the first
        //    2 values + null in the second and third
        //  = 18 values
        Pair<SegmentHeader, SegmentBody> rollup = SegmentBuilder.rollup(
            makeSegmentMap(
                new String[] {"col1", "col2", "col3"}, null, 2, 18, true,
                // col2 & col3 have nullAxisFlag=T
                new boolean[] {false, true, true}),
            Collections.singleton("col2"),
            null, RolapAggregator.Sum, Dialect.Datatype.Numeric);

        // expected value is 6 * MOCK_CELL_VALUE for each of 3 column values,
        // since each of the 18 cells are being rolled up to 3 buckets
        double expectedVal = 6 * MOCK_CELL_VALUE;
        assertArraysAreEqual(
            new double[] { expectedVal, expectedVal, expectedVal },
            (double[]) rollup.getValue().getValueArray());
        assertTrue(
            "Rolled up column should have nullAxisFlag set.",
            rollup.getValue().getNullAxisFlags().length == 1
                && rollup.getValue().getNullAxisFlags()[0]);
        assertEquals(
            "col2",
            rollup.getKey().getConstrainedColumns()
                .get(0).columnExpression);
    }

    public void testRollup2ColumnsWithMixOfNullAxisValues() {
        // constructed segment has 3 columns:
        //    2 values in the first
        //    2 values + null in the second and third
        //  = 18 values
        Pair<SegmentHeader, SegmentBody> rollup = SegmentBuilder.rollup(
            makeSegmentMap(
                new String[] {"col1", "col2", "col3"}, null, 2, 12, true,
                // col2 & col3 have nullAxisFlag=T
                new boolean[] {false, true, false}),
            new HashSet<String>(Arrays.asList("col1", "col2")),
            null, RolapAggregator.Sum, Dialect.Datatype.Numeric);

        // expected value is 2 * MOCK_CELL_VALUE for each of 3 column values,
        // since each of the 12 cells are being rolled up to 3 * 2 buckets
        double expectedVal = 2 * MOCK_CELL_VALUE;
        assertArraysAreEqual(
            new double[] {expectedVal, expectedVal, expectedVal,
                expectedVal, expectedVal, expectedVal},
            (double[]) rollup.getValue().getValueArray());
        assertTrue(
            "Rolled up column should have nullAxisFlag set to false for "
            + "the first column, true for second column.",
            rollup.getValue().getNullAxisFlags().length == 2
                && !rollup.getValue().getNullAxisFlags()[0]
                && rollup.getValue().getNullAxisFlags()[1]);
        assertEquals(
            "col1",
            rollup.getKey().getConstrainedColumns()
                .get(0).columnExpression);
        assertEquals(
            "col2",
            rollup.getKey().getConstrainedColumns()
                .get(1).columnExpression);
    }

    public void testMultiSegRollupWithMixOfNullAxisValues() {
        // rolls up 2 segments.
        // Segment 1 has 3 columns:
        //    2 values in the first
        //    1 values + null in the second
        //    2 vals + null in the third
        //  = 12 values
        //  Segment 2 has the same 3 columns, difft values for 3rd column.
        //
        //  none of the columns are wildcarded.
        final Map<SegmentHeader, SegmentBody> map = makeSegmentMap(
            new String[]{"col1", "col2", "col3"},
            new String[][]{{"col1A", "col1B"}, {"col2A"}, {"col3A", "col3B"}},
            -1, 12, false,
            // col2 & col3 have nullAxisFlag=T
            new boolean[]{false, true, true});
        map.putAll(
            makeSegmentMap(
                new String[]{"col1", "col2", "col3"},
                new String[][]{{"col1A", "col1B"}, {"col2A"}, {"col3C",
                    "col3D"}},
                -1, 8, false,
                // col3 has nullAxisFlag=T
                new boolean[]{false, true, false}));
        Pair<SegmentHeader, SegmentBody> rollup = SegmentBuilder.rollup(
            map,
            Collections.singleton("col2"),
            null, RolapAggregator.Sum, Dialect.Datatype.Numeric);
        // expected value is 10 * MOCK_CELL_VALUE for each of 2 column values,
        // since the 20 cells across 2 segments are being rolled up to 2 buckets
        double expectedVal = 10 * MOCK_CELL_VALUE;
        assertArraysAreEqual(
            new double[]{ expectedVal, expectedVal},
            (double[]) rollup.getValue().getValueArray());
        assertTrue(
            "Rolled up column should have nullAxisFlag set to true for "
            + "a single column.",
            rollup.getValue().getNullAxisFlags().length == 1
                && rollup.getValue().getNullAxisFlags()[0]);
        assertEquals(
            "col2",
            rollup.getKey().getConstrainedColumns()
                .get(0).columnExpression);
    }

    private void assertArraysAreEqual(double[] expected, double[] actual) {
        assertTrue(
            "Expected double array:  "
                + Arrays.toString(expected)
                + ", but got "
                + Arrays.toString(actual),
            doubleArraysEqual(actual, expected));
    }

    private boolean doubleArraysEqual(
        double[] valueArray, double[] expectedVal)
    {
        if (valueArray.length != expectedVal.length) {
            return false;
        }
        double within = 0.00000001;
        for (int i = 0; i < valueArray.length; i++) {
            if (Math.abs(valueArray[i] - expectedVal[i]) > within) {
                return false;
            }
        }
        return true;
    }

    public void testNullMemberOffset() {
        // verifies that presence of a null member does not cause
        // offsets to be incorrect for a Segment rollup.
        // First query loads the cache with a segment that can fulfill the
        // second query.
        executeQuery(
            "select [Store Size in SQFT].[Store Sqft].members * "
            + "gender.gender.members  on 0 from sales");
        assertQueryReturns(
            "select non empty [Store Size in SQFT].[Store Sqft].members on 0"
            + "from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store Size in SQFT].[#null]}\n"
            + "{[Store Size in SQFT].[20319]}\n"
            + "{[Store Size in SQFT].[21215]}\n"
            + "{[Store Size in SQFT].[22478]}\n"
            + "{[Store Size in SQFT].[23598]}\n"
            + "{[Store Size in SQFT].[23688]}\n"
            + "{[Store Size in SQFT].[27694]}\n"
            + "{[Store Size in SQFT].[28206]}\n"
            + "{[Store Size in SQFT].[30268]}\n"
            + "{[Store Size in SQFT].[33858]}\n"
            + "{[Store Size in SQFT].[39696]}\n"
            + "Row #0: 39,329\n"
            + "Row #0: 26,079\n"
            + "Row #0: 25,011\n"
            + "Row #0: 2,117\n"
            + "Row #0: 25,663\n"
            + "Row #0: 21,333\n"
            + "Row #0: 41,580\n"
            + "Row #0: 2,237\n"
            + "Row #0: 23,591\n"
            + "Row #0: 35,257\n"
            + "Row #0: 24,576\n");
    }

    public void testNullMemberOffset2ColRollup() {
        // verifies that presence of a null member does not cause
        // offsets to be incorrect for a Segment rollup involving 2
        // columns.  This tests a case where
        // SegmentBuilder.computeAxisMultipliers needs to factor in
        // the null axis flag.
        executeQuery(
            "select [Store Size in SQFT].[Store Sqft].members * "
            + "[store].[store state].members * time.[quarter].members on 0"
            + " from sales where [Product].[Food].[Produce].[Vegetables].[Fresh Vegetables]");
        assertQueryReturns(
            "select non empty [Store Size in SQFT].[Store Sqft].members "
            + " * [store].[store state].members  on 0"
            + "from sales where [Product].[Food].[Produce].[Vegetables].[Fresh Vegetables]",
            "Axis #0:\n"
            + "{[Product].[Food].[Produce].[Vegetables].[Fresh Vegetables]}\n"
            + "Axis #1:\n"
            + "{[Store Size in SQFT].[#null], [Store].[USA].[CA]}\n"
            + "{[Store Size in SQFT].[#null], [Store].[USA].[WA]}\n"
            + "{[Store Size in SQFT].[20319], [Store].[USA].[OR]}\n"
            + "{[Store Size in SQFT].[21215], [Store].[USA].[WA]}\n"
            + "{[Store Size in SQFT].[22478], [Store].[USA].[CA]}\n"
            + "{[Store Size in SQFT].[23598], [Store].[USA].[CA]}\n"
            + "{[Store Size in SQFT].[23688], [Store].[USA].[CA]}\n"
            + "{[Store Size in SQFT].[27694], [Store].[USA].[OR]}\n"
            + "{[Store Size in SQFT].[28206], [Store].[USA].[WA]}\n"
            + "{[Store Size in SQFT].[30268], [Store].[USA].[WA]}\n"
            + "{[Store Size in SQFT].[33858], [Store].[USA].[WA]}\n"
            + "{[Store Size in SQFT].[39696], [Store].[USA].[WA]}\n"
            + "Row #0: 1,967\n"
            + "Row #0: 947\n"
            + "Row #0: 2,065\n"
            + "Row #0: 1,827\n"
            + "Row #0: 165\n"
            + "Row #0: 2,109\n"
            + "Row #0: 1,665\n"
            + "Row #0: 3,382\n"
            + "Row #0: 162\n"
            + "Row #0: 1,875\n"
            + "Row #0: 2,668\n"
            + "Row #0: 1,907\n");
    }

    public void testSegmentBodyIterator() {
        // checks that cell key coordinates are generated correctly
        // when a null member is present.
        List<Pair<SortedSet<Comparable>, Boolean>> axes =
            new ArrayList<Pair<SortedSet<Comparable>, Boolean>>();
        axes.add(new Pair<SortedSet<Comparable>, Boolean>(
            new TreeSet<Comparable>(
                Arrays.asList("foo1", "bar1")), true)); // nullAxisFlag=T
        axes.add(new Pair<SortedSet<Comparable>, Boolean>(
            new TreeSet<Comparable>(
                Arrays.asList("foo2", "bar2", "baz3")), false));
        SegmentBody testBody = new DenseIntSegmentBody(
            new BitSet(), new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9},
            axes);
        Map valueMap = testBody.getValueMap();
        assertEquals(
            "{(0, 0)=1, "
            + "(0, 1)=2, "
            + "(0, 2)=3, "
            + "(1, 0)=4, "
            + "(1, 1)=5, "
            + "(1, 2)=6, "
            + "(2, 0)=7, "
            + "(2, 1)=8, "
            + "(2, 2)=9}",
            valueMap.toString());
    }

    public void testSparseRollup() {
        // functional test for a case that causes OOM if rollup creates
        // a dense segment.
        // This takes several seconds to run

        if (PerformanceTest.LOGGER.isDebugEnabled()) {
            // load the cache with a segment for the subsequent rollup
            executeQuery(
                "select NON EMPTY Crossjoin([Store Type].[Store Type].members, "
                + "CrossJoin([Promotion Media].[Media Type].members, "
                + " Crossjoin([Promotions].[Promotion Name].members, "
                + "Crossjoin([Store].[Store Name].Members, "
                + "Crossjoin( [product].[product name].members, "
                + "Crossjoin( [Customers].[Name].members,  "
                + "[Gender].[Gender].members)))))) on 1, "
                + "{ measures.[unit sales] } on 0 from [Sales]");

            executeQuery(
                "select NON EMPTY Crossjoin([Store Type].[Store Type].members, "
                + "CrossJoin([Promotion Media].[Media Type].members, "
                + "Crossjoin([Promotions].[Promotion Name].members, "
                + "Crossjoin([Store].[Store Name].Members, "
                + "Crossjoin( [product].[product name].members, "
                + "[Customers].[Name].members))))) on 1, "
                + "{ measures.[unit sales] } on 0 from [Sales]");
            // second query will throw OOM if .rollup() attempts
            // to create a dense segment
        }
    }

    public void testRollupWithIntOverflowPossibility() {
        // rolling up a segment that would cause int overflow if
        // rolled up to a dense segment
        // MONDRIAN-1377

        // make a source segment w/ 3 cols, 47K vals each,
        // target segment has 2 of the 3 cols.
        // count of possible values will exceed Integer.MAX_VALUE
        Pair<SegmentHeader, SegmentBody> rollup =
            SegmentBuilder.rollup(
                makeSegmentMap(
                    new String[] {"col1", "col2", "col3"},
                    null, 47000, 4, false, null),
                new HashSet<String>(Arrays.asList("col1", "col2")),
                null, RolapAggregator.Sum, Dialect.Datatype.Numeric);
        assertTrue(rollup.right instanceof SparseSegmentBody);
    }

    public void testRollupWithOOMPossibility() {
        // rolling up a segment that would cause OOM if
        // rolled up to a dense segment
        // MONDRIAN-1377

        // make a source segment w/ 3 cols, 44K vals each,
        // target segment has 2 of the 3 cols.
        Pair<SegmentHeader, SegmentBody> rollup =
            SegmentBuilder.rollup(
                makeSegmentMap(
                    new String[] {"col1", "col2", "col3"},
                    null, 44000, 4, false, null),
                new HashSet<String>(Arrays.asList("col1", "col2")),
                null, RolapAggregator.Sum, Dialect.Datatype.Numeric);
        assertTrue(rollup.right instanceof SparseSegmentBody);
    }

    public void testRollupShouldBeDense() {
        // Fewer than 1000 column values in rolled up segment.
        Pair<SegmentHeader, SegmentBody> rollup =
            SegmentBuilder.rollup(
                makeSegmentMap(
                    new String[] {"col1", "col2", "col3"},
                    null, 10, 15, false, null),
                new HashSet<String>(Arrays.asList("col1", "col2")),
                null, RolapAggregator.Sum, Dialect.Datatype.Numeric);
        assertTrue(rollup.right instanceof DenseDoubleSegmentBody);

        // greater than 1K col vals, above density ratio
        rollup =
            SegmentBuilder.rollup(
                makeSegmentMap(
                    new String[] {"col1", "col2", "col3", "col4"},
                    null, 11, 10000, false, null),
                    // 1331 possible intersections (11*3)
                new HashSet<String>(Arrays.asList("col1", "col2", "col3")),
                null, RolapAggregator.Sum, Dialect.Datatype.Numeric);
        assertTrue(rollup.right instanceof DenseDoubleSegmentBody);
    }

    public void testRollupWithDenseIntBody() {
      //
      //  We have the following data:
      //
      //           1 _ _
      //    col2   1 2 _
      //           1 _ 1
      //            col1
      //   So, after rolling it up with the SUM function, we expect to get
      //
      //           3 2 1
      //            col1
      //
      String[][] colValues = dummyColumnValues(2, 3);
      int[] values = {1, 1, 1, 0, 2, 0, 1};

      BitSet nulls = new BitSet();
      for (int i = 0; i < values.length; i++) {
        if (values[i] == 0) {
          nulls.set(i);
        }
      }

      List<Pair<SortedSet<Comparable>, Boolean>> axes =
          new ArrayList<Pair<SortedSet<Comparable>, Boolean>>();
      List<SegmentColumn> segmentColumns = new ArrayList<SegmentColumn>();
      for (int i = 0; i < colValues.length; i++) {
        axes.add(of(toSortedSet(colValues[i]), false));
        segmentColumns.add(new SegmentColumn(
            "col" + (i + 1),
            colValues[i].length,
            toSortedSet(colValues[i])));
      }
      SegmentHeader header = makeDummySegmentHeader(segmentColumns);
      SegmentBody body = new DenseIntSegmentBody(nulls, values, axes);
      Map<SegmentHeader, SegmentBody> segmentsMap = singletonMap(header, body);

      Pair<SegmentHeader, SegmentBody> rollup =
          SegmentBuilder.rollup(
              segmentsMap, singleton("col1"),
              null, RolapAggregator.Sum, Dialect.Datatype.Numeric);

      double[] result = (double[])rollup.right.getValueArray();
      double[] expected = {3, 2, 1};
      assertEquals(expected.length, result.length);
      for (int i = 0; i < expected.length; i++) {
        double exp = expected[i];
        double act = result[i];
        assertTrue(
            String.format("%d %f %f", i, exp, act),
            Math.abs(exp - act) < 1e-6);
      }
    }

    public void testOverlappingSegments() {
        // MONDRIAN-2107
        // The segments created by the first 2 queries below overlap on
        //  [1997].[Q1].[1].  The rollup of these two segments should not
        // doubly-add that cell.
        // Also, these two segments have predicates optimized for 'quarter'
        // since 3 out of 4 quarters are present.  This means the
        //  header.getValues() will be null.  This has the potential
        // to cause issues with rollup since one segment body will have
        // 3 values for quarter, the other segment body will have a different
        // set of values.
        getTestContext().flushSchemaCache();
        TestContext context = getTestContext().withFreshConnection();

        context.executeQuery(
            "select "
            + "{[Time].[1997].[Q1].[1], [Time].[1997].[Q1].[2], [Time].[1997].[Q1].[3], "
            + "[Time].[1997].[Q2].[4], [Time].[1997].[Q2].[5], [Time].[1997].[Q2].[6],"
            + "[Time].[1997].[Q3].[7]} on 0 from sales");
        context.executeQuery(
            "select "
            + "{[Time].[1997].[Q1].[1], [Time].[1997].[Q3].[8], [Time].[1997].[Q3].[9], "
            + "[Time].[1997].[Q4].[10], [Time].[1997].[Q4].[11], [Time].[1997].[Q4].[12],"
            + "[Time].[1998].[Q1].[1], [Time].[1998].[Q3].[8], [Time].[1998].[Q3].[9], "
            + "[Time].[1998].[Q4].[10], [Time].[1998].[Q4].[11], [Time].[1998].[Q4].[12]}"
            + "on 0 from sales");

        RolapUtil.setHook(
            new RolapUtil.ExecuteQueryHook()
        {
            public void onExecuteQuery(String sql) {
                //  We shouldn't see a sum of unit_sales in SQL if using rollup.
                assertFalse(
                    "Expected cells to be pulled from cache",
                    sql.matches(".*sum\\([^ ]+unit_sales.*"));
            }
        });
        context.assertQueryReturns(
            "select [Time].[1997].children on 0 from sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Time].[1997].[Q1]}\n"
            + "{[Time].[1997].[Q2]}\n"
            + "{[Time].[1997].[Q3]}\n"
            + "{[Time].[1997].[Q4]}\n"
            + "Row #0: 66,291\n"
            + "Row #0: 62,610\n"
            + "Row #0: 65,848\n"
            + "Row #0: 72,024\n");
    }

    public void testNonOverlappingRollupWithUnconstrainedColumn() {
        // MONDRIAN-2107
        // The two segments loaded by the 1st 2 queries will have predicates
        // optimized for Name.  Prior to the fix for 2107 this would
        // result in roughly half of the customers having empty results
        // for the 3rd query, since the values of only one of the two
        // segments would be loaded into the AxisInfo.
        getTestContext().flushSchemaCache();
        TestContext context = getTestContext().withFreshConnection();
        final String query = "select customers.[name].members on 0 from sales";
        propSaver.set(propSaver.properties.EnableInMemoryRollup, false);
        Result result = context.executeQuery(query);

        getTestContext().flushSchemaCache();
        context = getTestContext().withFreshConnection();
        propSaver.set(propSaver.properties.EnableInMemoryRollup, true);
        context.executeQuery(
            "select "
            + "{[customers].[name].members} on 0 from sales where gender.f");
        context.executeQuery(
            "select "
            + "{[customers].[name].members} on 0 from sales where gender.m");

        RolapUtil.setHook(
            new RolapUtil.ExecuteQueryHook()
        {
            public void onExecuteQuery(String sql) {
                //  We shouldn't see a sum of unit_sales in SQL if using rollup.
                assertFalse(
                    "Expected cells to be pulled from cache",
                    sql.matches(".*sum\\([^ ]+unit_sales.*"));
            }
        });

        context.assertQueryReturns(
            query,
            context.toString(result));
    }

    public void testNonOverlappingRollupWithUnconstrainedColumnAndHasNull() {
        // MONDRIAN-2107
        // Creates 10 segments, one for each city, with various sets
        // of [Store Sqft].  Some contain NULL, some do not.
        // Results from rollup should match results from a query not pulling
        // from cache.
        String[] states = {"[Canada].BC", "[USA].CA", "[Mexico].DF",
            "[Mexico].Guerrero", "[Mexico].Jalisco", "[USA].[OR]",
            "[Mexico].Veracruz", "[USA].WA", "[Mexico].Yucatan",
            "[Mexico].Zacatecas"};
        getTestContext().flushSchemaCache();
        TestContext context = getTestContext().withFreshConnection();
        final String query =
            "select [Store Size in SQFT].[Store Sqft].members on 0 from sales";

        Result result = context.executeQuery(query);
        getTestContext().flushSchemaCache();
        context = getTestContext().withFreshConnection();
        for (String state : states) {
            context.executeQuery(
                String.format(
                    "select "
                    + "{[Store Size in SQFT].[Store Sqft].members} on 0 "
                    + "from sales where store.%s", state));
        }
        RolapUtil.setHook(
            new RolapUtil.ExecuteQueryHook()
        {
            public void onExecuteQuery(String sql) {
                //  We shouldn't see a sum of unit_sales in SQL if using rollup.
                assertFalse(
                    "Expected cell to be pulled from cache",
                    sql.matches(".*sum\\([^ ]+unit_sales.*"));
            }
        });

        context.assertQueryReturns(
            query,
            context.toString(result));
    }

    public void testBadRollupCausesGreaterThan12Iterations() {
        // http://jira.pentaho.com/browse/MONDRIAN-1729
        // The first two queries populate the cache with segments
        // capable of being rolled up to fulfill the 3rd query.
        // MONDRIAN-1729 involved the rollup being invalid, causing
        // an infinite loop.
        getTestContext().flushSchemaCache();
        TestContext context = getTestContext().withFreshConnection();

        context.executeQuery(
            "select "
            + "{[Time].[1998].[Q1].[2],[Time].[1998].[Q1].[3],"
            + "[Time].[1998].[Q2].[4],[Time].[1998].[Q2].[5],"
            + "[Time].[1998].[Q2].[5],[Time].[1998].[Q2].[6],"
            + "[Time].[1998].[Q3].[7]} on 0 from sales");

        context.executeQuery(
            "select "
            + "{[Time].[1997].[Q1].[1], [Time].[1997].[Q3].[8], [Time].[1997].[Q3].[9], "
            + "[Time].[1997].[Q4].[10], [Time].[1997].[Q4].[11], [Time].[1997].[Q4].[12],"
            + "[Time].[1998].[Q1].[1], [Time].[1998].[Q3].[8], [Time].[1998].[Q3].[9], "
            + "[Time].[1998].[Q4].[10], [Time].[1998].[Q4].[11], [Time].[1998].[Q4].[12]}"
            + "on 0 from sales");

        context.executeQuery("select [Time].[1998].[Q1] on 0 from sales");
    }

    public void testSameRollupRegardlessOfSegmentOrderWithEmptySegmentBody() {
        // http://jira.pentaho.com/browse/MONDRIAN-1729
        // rollup of segments {A, B} should produce the same resulting segment
        // regardless of whether rollup processes them in the order A,B or B,A.
        // MONDRIAN-1729 involved a case where the rollup segment was invalid
        // if processed in a particular order.
        // This tests a wildcarded segment (on year) rolled up w/ a seg
        // containing a single val.
        // The resulting segment contains only empty results (for 1998)
        runRollupTest(
            // queries to populate the cache with segments which will be rolled
            // up
            new String[]{
                "select "
                + "{[Time].[1998].[Q1].[2],[Time].[1998].[Q1].[3],"
                + "[Time].[1998].[Q2].[4],[Time].[1998].[Q2].[5],"
                + "[Time].[1998].[Q2].[5],[Time].[1998].[Q2].[6],"
                + "[Time].[1998].[Q3].[7]} on 0 from sales",
                "select "
                + "{[Time].[1997].[Q1].[1], [Time].[1997].[Q3].[8], [Time].[1997].[Q3].[9], "
                + "[Time].[1997].[Q4].[10], [Time].[1997].[Q4].[11], [Time].[1997].[Q4].[12],"
                + "[Time].[1998].[Q1].[1], [Time].[1998].[Q3].[8], [Time].[1998].[Q3].[9], "
                + "[Time].[1998].[Q4].[10], [Time].[1998].[Q4].[11], [Time].[1998].[Q4].[12]}"
                + "on 0 from sales"},
            new String[] {
                "af45d690c892633e5755b06d623ba50a849357832d2157954303a76b4d9564ac",
                    // ^^^
                    //    {time_by_day.the_year=('1998')}
                    //    {time_by_day.quarter=('Q1','Q2','Q3')}
                    //    {time_by_day.month_of_year=('2','3','4','5','6','7')}]
                "0ceeeddc5d670cf385ea5bfe88811dee2f4b2ec43e1272d0258a9cd7c4d02ceb"
                    // ^^^
                    // {time_by_day.the_year=(*)}
                    // {time_by_day.quarter=('Q1','Q3','Q4')}
                    // {time_by_day.month_of_year=('1','8','9','10','11','12')}]
            },
            new String[]{
                // rollup columns
                "time_by_day.quarter",
                "time_by_day.the_year"
            },
            // expected header of the rolled up segment
            "*Segment Header\n"
            + "Schema:[FoodMart]\n"
            + "Checksum:[7b4af973b0d21f364b0a746f5565cb03]\n"
            + "Cube:[Sales]\n"
            + "Measure:[Unit Sales]\n"
            + "Axes:[\n"
            + "    {time_by_day.quarter=('Q1','Q3')}\n"
            + "    {time_by_day.the_year=('1998')}]\n"
            + "Excluded Regions:[]\n"
            + "Compound Predicates:[]\n"
            + "ID:[5eca71e4224b8c407e4f0c9ba0082bba0b36455304a675263eee942847cbfc06]\n");
    }

    public void testSameRollupRegardlessOfSegmentOrderWithData() {
        // http://jira.pentaho.com/browse/MONDRIAN-1729
        // Tests a wildcarded segment rolled up w/ a seg containing a single
        // val.  Both segments are associated w/ non empty results.
        runRollupTest(
            new String[]{
                "select {{[Product].[Drink].[Alcoholic Beverages]},\n"
                + "{[Product].[Drink].[Beverages]},\n"
                + "{[Product].[Food].[Baked Goods]},\n"
                + "{[Product].[Non-Consumable].[Periodicals]}}\n on 0 from sales",
                "select "
                + "\n"
                + "{[Product].[Drink].[Dairy]}"
                + "on 0 from sales"},
            new String[] {
              "71cf49a1d6f0c2da433442927e05ea5c571f52fce132081f499aba34b6dbb054",
                // ^^^
                //    {time_by_day.the_year=('1997')}
                //    {product_class.product_family=(*)}
                //    {product_class.product_department=('Alcoholic Beverages',
                //     'Baked Goods','Beverages','Periodicals')}]
              "5bc7ead7b284809f6203df971c09c7c797d727050ae46dbd9cc04d59edc9538a"
                // ^^^
                //    {time_by_day.the_year=('1997')}
                //    {product_class.product_family=('Drink')}
                //    {product_class.product_department=('Dairy')}]
            },
            new String[]{
                "product_class.product_family"
            },
            "*Segment Header\n"
            + "Schema:[FoodMart]\n"
            + "Checksum:[7b4af973b0d21f364b0a746f5565cb03]\n"
            + "Cube:[Sales]\n"
            + "Measure:[Unit Sales]\n"
            + "Axes:[\n"
            + "    {product_class.product_family=('Drink')}]\n"
            + "Excluded Regions:[]\n"
            + "Compound Predicates:[]\n"
            + "ID:[360a78a6503ae823650bbfd528bd3014e77d09c2cc0815f5eac90e8d7d46a474]\n");
    }

    public void testSameRollupRegardlessOfSegmentOrderNoWildcards() {
        // http://jira.pentaho.com/browse/MONDRIAN-1729
        // Tests 2 segments, each w/ no wildcarded values.
        runRollupTest(
            new String[]{
                "select {{[Product].[Drink].[Alcoholic Beverages]},\n"
                + "{[Product].[Drink].[Beverages]},\n"
                + "{[Product].[Non-Consumable].[Periodicals]}}\n on 0 from sales",
                "select "
                + "\n"
                + "{[Product].[Drink].[Dairy]}"
                + "on 0 from sales"},
            new String[] {
                "63b78b31fae7a3229f284be9a2fc8d5ff47f1241b6a5972b6e6adf41f48cdb5a",
                    // ^^^
                    //    {time_by_day.the_year=('1997')}
                    // {product_class.product_family=('Drink','Non-Consumable')}
                    // {product_class.product_department=('Alcoholic Beverages',
                    //  'Beverages','Periodicals')}]
                "5bc7ead7b284809f6203df971c09c7c797d727050ae46dbd9cc04d59edc9538a"
                    // ^^^
                    //    {time_by_day.the_year=('1997')}
                    //    {product_class.product_family=('Drink')}
                    //    {product_class.product_department=('Dairy')}]
            },
            new String[]{
                "product_class.product_family"
            },
            "*Segment Header\n"
            + "Schema:[FoodMart]\n"
            + "Checksum:[7b4af973b0d21f364b0a746f5565cb03]\n"
            + "Cube:[Sales]\n"
            + "Measure:[Unit Sales]\n"
            + "Axes:[\n"
            + "    {product_class.product_family=('Drink')}]\n"
            + "Excluded Regions:[]\n"
            + "Compound Predicates:[]\n"
            + "ID:[360a78a6503ae823650bbfd528bd3014e77d09c2cc0815f5eac90e8d7d46a474]\n");
    }

    public void testSameRollupRegardlessOfSegmentOrderThreeSegs() {
        // http://jira.pentaho.com/browse/MONDRIAN-1729
        // Tests 3 segments, each w/ no wildcarded values.
        runRollupTest(
            new String[]{
                "select {{[Product].[Drink].[Alcoholic Beverages]},\n"
                + "{[Product].[Non-Consumable].[Periodicals]}}\n on 0 from sales",
                "select "
                + "\n"
                + "{[Product].[Drink].[Dairy]}"
                + "on 0 from sales",
                " select "
                + "{[Product].[Drink].[Beverages]} on 0 from sales"},
            new String[] {
                "e546620837ba618128e2ffbb53f2a32867725f68f53467e4cc081e94ffc7809b",
                    // ^^^
                    //    {time_by_day.the_year=('1997')}
                    //    {product_class.product_family=('Drink')}
                    //    {product_class.product_department=('Dairy')}]
                "4c6158aa74d34b4941f79c699f39a866eb5d4aaacf27f4319839aaff5486e886",
                    // ^^^
                    //    {time_by_day.the_year=('1997')}
                    //    {product_class.product_family=('Drink')}
                    //    {product_class.product_department=('Beverages')}]
                "5bc7ead7b284809f6203df971c09c7c797d727050ae46dbd9cc04d59edc9538a"
                    // ^^^
                    //    {time_by_day.the_year=('1997')}
                    //    {product_class.product_family=
                    //                              ('Drink','Non-Consumable')}
                    //    {product_class.product_department=
                    //                  ('Alcoholic Beverages','Periodicals')}]
            },
            new String[]{
                "product_class.product_family"
            },
            "*Segment Header\n"
            + "Schema:[FoodMart]\n"
            + "Checksum:[7b4af973b0d21f364b0a746f5565cb03]\n"
            + "Cube:[Sales]\n"
            + "Measure:[Unit Sales]\n"
            + "Axes:[\n"
            + "    {product_class.product_family=('Drink')}]\n"
            + "Excluded Regions:[]\n"
            + "Compound Predicates:[]\n"
            + "ID:[360a78a6503ae823650bbfd528bd3014e77d09c2cc0815f5eac90e8d7d46a474]\n");
    }


    public void testSegmentCreationForBoolean_True() {
        doTestSegmentCreationForBoolean(true);
    }

    public void testSegmentCreationForBoolean_False() {
        doTestSegmentCreationForBoolean(false);
    }

    private void doTestSegmentCreationForBoolean(boolean value) {
        Dialect.DatabaseProduct db =
            getTestContext().getDialect().getDatabaseProduct();
        if (db == Dialect.DatabaseProduct.ORACLE) {
            // Oracle does not support boolean type
            return;
        }

        final String queryToBeCached = String.format(
            "SELECT NON EMPTY [Store].[Store Country].members on COLUMNS "
            + "FROM [Store] "
            + "WHERE [Has coffee bar].[%b]", value);
        executeQuery(queryToBeCached);

        final String query = ""
            + "SELECT NON EMPTY "
            + "CROSSJOIN([Store].[Store Country].members, [Has coffee bar].[has coffee bar].members) ON COLUMNS "
            + "FROM [Store]";

        final String expected = ""
            + "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Store].[Canada], [Has coffee bar].[true]}\n"
            + "{[Store].[Mexico], [Has coffee bar].[false]}\n"
            + "{[Store].[Mexico], [Has coffee bar].[true]}\n"
            + "{[Store].[USA], [Has coffee bar].[false]}\n"
            + "{[Store].[USA], [Has coffee bar].[true]}\n"
            + "Row #0: 57,564\n"
            + "Row #0: 133,275\n"
            + "Row #0: 109,737\n"
            + "Row #0: 113,881\n"
            + "Row #0: 157,139\n";

        assertQueryReturns(query, expected);
    }


    /**
     * Loads the cache with the results of the queries
     * in cachePopulatingQueries, and then attempts to rollup all
     * cached segments based on the keepColumns array, checking
     * against expectedHeader.  Rolls up loading segment
     * in both forward and reverse order and verifies
     * same results both ways.
     * @return the rolled up SegmentHeader/SegmentBody pair
     */
    private Pair<SegmentHeader, SegmentBody> runRollupTest(
        String[] cachePopulatingQueries,
        String[] segmentIdsToRollup,
        String[] keepColumns,
        String expectedHeader)
    {
        propSaver.set(
            MondrianProperties.instance().OptimizePredicates,
            false);
        TestContext context = loadCacheWithQueries(cachePopulatingQueries);
        Map<SegmentHeader, SegmentBody> map = getReversibleTestMap(
            context, Order.FORWARD, segmentIdsToRollup);
        Set<String> keepColumnsSet = new HashSet<String>();
        keepColumnsSet.addAll(Arrays.asList(keepColumns));
        Pair<SegmentHeader, SegmentBody> rolledForward = SegmentBuilder.rollup(
            map,
            keepColumnsSet,
                   // bitkey does not factor into rollup logic, so it's safe to
                   // use a dummy
            BitKey.Factory.makeBitKey(new BitSet()),
            RolapAggregator.Sum,
            Dialect.Datatype.Numeric);
        // Now try reversing the order the segments are retrieved
        context = loadCacheWithQueries(cachePopulatingQueries);
        map = getReversibleTestMap(context, Order.REVERSE, segmentIdsToRollup);
        Pair<SegmentHeader, SegmentBody> rolledReverse = SegmentBuilder.rollup(
            map,
            keepColumnsSet,
            BitKey.Factory.makeBitKey(new BitSet()),
            RolapAggregator.Sum,
            Dialect.Datatype.Numeric);
        assertEquals(expectedHeader, rolledForward.getKey().toString());
        // the header of the rolled up segment should be the same
        // regardless of the order the segments were processed
        assertEquals(rolledForward.getKey(), rolledReverse.getKey());
        assertEquals(
            rolledForward.getValue().getValueMap().size(),
            rolledReverse.getValue().getValueMap().size());
        propSaver.reset();
        return rolledForward;
    }

    private TestContext loadCacheWithQueries(String [] queries) {
        getTestContext().flushSchemaCache();
        TestContext context = getTestContext().withFreshConnection();
        for (String query : queries) {
            context.executeQuery(query);
        }
        return context;
    }

    enum Order {
        FORWARD, REVERSE
    }
    /**
     * Creates a Map<SegmentHeader,SegmentBody> based on the set of
     * segments currently in the cache.  The Map overrides the entrySet()
     * method to provide an ordered set of elements based
     * on Header.getUniqueID(), ordered according to the order param.
     * @param context  The test context
     * @param order  The order to sort the elements returned by entrySet(),
     *               FORWARD or REVERSE
     * @param segmentIdsToInclude  The IDs of currently cached segments to
     *                             include in the map.
     */
    private Map<SegmentHeader, SegmentBody> getReversibleTestMap(
        TestContext context, final Order order, String[] segmentIdsToInclude)
    {
        SegmentCache cache = MondrianServer.forConnection(
            context.getConnection()).getAggregationManager()
            .cacheMgr.compositeCache;

        List<SegmentHeader> headers = cache.getSegmentHeaders();
        Map<SegmentHeader, SegmentBody> testMap =
            new HashMap<SegmentHeader, SegmentBody>() {
            public Set<Map.Entry<SegmentHeader, SegmentBody>> entrySet() {
                List<Map.Entry<SegmentHeader, SegmentBody>> list =
                    new ArrayList<Map.Entry<SegmentHeader, SegmentBody>>();
                list.addAll(super.entrySet());
                Collections.sort(
                    list,
                    new Comparator<Map.Entry<SegmentHeader, SegmentBody>>() {
                        public int compare(
                            Map.Entry<SegmentHeader, SegmentBody> o1,
                            Map.Entry<SegmentHeader, SegmentBody> o2)
                        {
                            int ret = o1.getKey().getUniqueID().compareTo(
                                o2.getKey().getUniqueID());
                            return order == Order.REVERSE ? -ret : ret;
                        }
                    });
                LinkedHashSet<Map.Entry<SegmentHeader, SegmentBody>>
                    orderedSet =
                    new LinkedHashSet<Map.Entry<SegmentHeader, SegmentBody>>();
                orderedSet.addAll(list);
                return orderedSet;
            }
            public Set<SegmentHeader> keySet() {
                List<SegmentHeader> list = new ArrayList<SegmentHeader>();
                list.addAll(super.keySet());
                Collections.sort(
                    list,
                    new Comparator<SegmentHeader>() {
                        public int compare(
                            SegmentHeader o1,
                            SegmentHeader o2)
                        {
                            int ret = o1.getUniqueID().compareTo(
                                o2.getUniqueID());
                            return order == Order.REVERSE ? ret : -ret;
                        }
                    });
                LinkedHashSet<SegmentHeader>
                    orderedSet =
                    new LinkedHashSet<SegmentHeader>();
                orderedSet.addAll(list);
                return orderedSet;
            }
        };
        for (SegmentHeader header : headers) {
            for (String segmentId : segmentIdsToInclude) {
                if (header.getUniqueID().toString().equals(segmentId)) {
                    testMap.put(header, cache.get(header));
                }
            }
        }
        assertFalse(String.format(
            "SegmentMap is empty. No segmentIds matched test parameters. "
            + "Full segment cache: %s", headers), testMap.isEmpty());
        return testMap;
    }


    /**
     * Creates a rough segment map for testing purposes, containing
     * the array of column names passed in, with numValsPerCol dummy
     * values placed per column.  Populates the cells in the body with
     * numPopulatedCells dummy values placed in the first N places of the
     * values array.
     */
    private Map<SegmentHeader, SegmentBody> makeSegmentMap(
        String[] colNames, String[][] colVals,
        int numValsPerCol, int numPopulatedCells,
        boolean wildcardCols, boolean[] nullAxisFlags)
    {
        if (colVals == null) {
            colVals = dummyColumnValues(colNames.length, numValsPerCol);
        }

        Pair<SegmentHeader, SegmentBody> headerBody = makeDummyHeaderBodyPair(
            colNames,
            colVals,
            numPopulatedCells, wildcardCols, nullAxisFlags);
        Map<SegmentHeader, SegmentBody> map =
            new HashMap<SegmentHeader, SegmentBody>();
        map.put(headerBody.left, headerBody.right);

        return map;
    }

    private Pair<SegmentHeader, SegmentBody> makeDummyHeaderBodyPair(
        String[] colExps, String[][] colVals, int numCellVals,
        boolean wildcardCols, boolean[] nullAxisFlags)
    {
        final List<SegmentColumn> constrainedColumns =
            new ArrayList<SegmentColumn>();

        final List<Pair<SortedSet<Comparable>, Boolean>> axes =
            new ArrayList<Pair<SortedSet<Comparable>, Boolean>>();
        for (int i = 0; i < colVals.length; i++) {
            String colExp = colExps[i];
            SortedSet<Comparable> headerVals = null;
            SortedSet<Comparable> vals = toSortedSet(colVals[i]);
            if (!wildcardCols) {
                headerVals = vals;
            }
            boolean nullAxisFlag = nullAxisFlags != null && nullAxisFlags[i];
            constrainedColumns.add(
                new SegmentColumn(
                    colExp,
                    colVals[i].length,
                    headerVals));
            axes.add(Pair.of(vals, nullAxisFlag));
        }

        Object [] cells = new Object[numCellVals];
        for (int i = 0; i < numCellVals; i++) {
            cells[i] = MOCK_CELL_VALUE; // assign a non-null val
        }
        return Pair.<SegmentHeader, SegmentBody>of(
            makeDummySegmentHeader(constrainedColumns),
            new DenseObjectSegmentBody(
                cells,
                axes));
    }

    private SegmentHeader makeDummySegmentHeader(
        List<SegmentColumn> constrainedColumns)
    {
        return new SegmentHeader(
            "dummySchemaName",
            new ByteString(new byte[0]),
            "dummyCubeName",
            "dummyMeasureName",
            constrainedColumns,
            Collections.<String>emptyList(),
            "dummyFactTable",
            BitKey.Factory.makeBitKey(3),
            Collections.<SegmentColumn>emptyList());
    }

    private String [][] dummyColumnValues(int cols, int numVals) {
        String [][] dummyColVals = new String[cols][numVals];
        for (int i = 0; i < cols; i++) {
            for (int j = 0; j < numVals; j++) {
                dummyColVals[i][j] = "c" + i + "v" + j;
            }
        }
        return dummyColVals;
    }

  private static SortedSet<Comparable> toSortedSet(Comparable... comparables) {
    List<Comparable> list = asList(comparables);
    return new TreeSet<Comparable>(list);
  }

  public void testRollupWithNonUniqueColumns() {
      Pair<SegmentHeader, SegmentBody> rollup =
          SegmentBuilder.rollup(
              makeSegmentMap(
                  new String[] {"col1", "col2", "col3", "col2"},
                  new String[][] {{"0.0"}, {"0.0"}, {"0.0"}, {"0.0"}},
                  10, 15, false, null),
              new HashSet<String>(Arrays.asList("col1", "col2")),
              null, RolapAggregator.Sum, Dialect.Datatype.Numeric);
      assertEquals(3, rollup.left.getConstrainedColumns().size());
  }

}

// End SegmentBuilderTest.java
