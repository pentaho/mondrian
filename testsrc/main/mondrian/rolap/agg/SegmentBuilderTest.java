/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/
package mondrian.rolap.agg;

import mondrian.olap.*;
import mondrian.rolap.*;
import mondrian.spi.*;
import mondrian.test.*;
import mondrian.util.ByteString;
import mondrian.util.Pair;

import java.util.*;

/**
 * <p>Test for <code>SegmentBuilder</code>.</p>
 *
 * @author mcampbell
 */
public class SegmentBuilderTest extends BatchTestCase {

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
                    new String[] {"col1", "col2", "col3"}, 47000, 4),
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
                    new String[] {"col1", "col2", "col3"}, 44000, 4),
                new HashSet<String>(Arrays.asList("col1", "col2")),
                null, RolapAggregator.Sum, Dialect.Datatype.Numeric);
        assertTrue(rollup.right instanceof SparseSegmentBody);
    }

    public void testRollupShouldBeDense() {
        // Fewer than 1000 column values in rolled up segment.
        Pair<SegmentHeader, SegmentBody> rollup =
            SegmentBuilder.rollup(
                makeSegmentMap(
                    new String[] {"col1", "col2", "col3"}, 10, 15),
                new HashSet<String>(Arrays.asList("col1", "col2")),
                null, RolapAggregator.Sum, Dialect.Datatype.Numeric);
        assertTrue(rollup.right instanceof DenseDoubleSegmentBody);

        // greater than 1K col vals, above density ratio
        rollup =
            SegmentBuilder.rollup(
                makeSegmentMap(
                    new String[] {"col1", "col2", "col3", "col4"},
                    11, 10000),   // 1331 possible intersections (11*3)
                new HashSet<String>(Arrays.asList("col1", "col2", "col3")),
                null, RolapAggregator.Sum, Dialect.Datatype.Numeric);
        assertTrue(rollup.right instanceof DenseDoubleSegmentBody);
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
                "26f2dc1bd8a454424158c348d0e3c64e3c20e00fa807d56442b8a580728e9953",
                "b76369e0fb17e67bc777aa1e5d2d2d81627992488eb70bb45432ecb6ff6752c9"
            },
            new String[]{
                // rollup columns
                "time_by_day.quarter",
                "time_by_day.the_year"
            },
            // expected header of the rolled up segment
            "*Segment Header\n"
            + "Schema:[FoodMart]\n"
            + "Checksum:[9cca66327439577753dd5c3144ab59b5]\n"
            + "Cube:[Sales]\n"
            + "Measure:[Unit Sales]\n"
            + "Axes:[\n"
            + "    {time_by_day.the_year=('1998')}\n"
            + "    {time_by_day.quarter=('Q1','Q3')}]\n"
            + "Excluded Regions:[]\n"
            + "Compound Predicates:[]\n"
            + "ID:[3be3399cef4616f7718611775b3409c88a931b4f4642a8bcaa81740c884c3d3b]\n");
    }

    public void testSameRollupRegardlessOfSegmentOrderWithData() {
        // http://jira.pentaho.com/browse/MONDRIAN-1729
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
              "464a73cb907ccee680acd79a3b03181ddc6b8c2d9cf8b75ea81fcc6d634d8d57",
              "38fec44b20434448aa8c6191ac34ca96157e6db5bb50586d4dc4a55d7385a0f0"
            },
            new String[]{
                "product_class.product_family"
            },
            "*Segment Header\n"
            + "Schema:[FoodMart]\n"
            + "Checksum:[9cca66327439577753dd5c3144ab59b5]\n"
            + "Cube:[Sales]\n"
            + "Measure:[Unit Sales]\n"
            + "Axes:[\n"
            + "    {product_class.product_family=('Drink')}]\n"
            + "Excluded Regions:[]\n"
            + "Compound Predicates:[]\n"
            + "ID:[0c3fbbc2a97f4183b7761326ceb76a"
            + "760ebc20550b4f686d129ece11e0dc1e49]\n");
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
        };
        for (SegmentHeader header : headers) {
            for (String segmentId : segmentIdsToInclude) {
                if (header.getUniqueID().toString().equals(segmentId)) {
                    testMap.put(header, cache.get(header));
                }
            }
        }
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
        String[] colNames, int numValsPerCol, int numPopulatedCells)
    {
        Pair<SegmentHeader, SegmentBody> headerBody = makeDummyHeaderBodyPair(
            colNames,
            dummyColumnValues(colNames.length, numValsPerCol),
            numPopulatedCells);
        Map<SegmentHeader, SegmentBody> map =
            new HashMap<SegmentHeader, SegmentBody>();
        map.put(headerBody.left, headerBody.right);

        return map;
    }

    private Pair<SegmentHeader, SegmentBody> makeDummyHeaderBodyPair(
        String[] colExps, String[][] colVals, int numCellVals)
    {
        final List<SegmentColumn> constrainedColumns =
            new ArrayList<SegmentColumn>();

        final List<Pair<SortedSet<Comparable>, Boolean>> axes =
            new ArrayList<Pair<SortedSet<Comparable>, Boolean>>();
        for (int i = 0; i < colVals.length; i++) {
            String colExp = colExps[i];
            SortedSet<Comparable> vals =
                new TreeSet<Comparable>(Arrays.<Comparable>asList(colVals[i]));
            constrainedColumns.add(
                new SegmentColumn(
                    colExp,
                    colVals[i].length,
                    vals));
            axes.add(Pair.of(vals, Boolean.FALSE));
        }

        Object [] cells = new Object[numCellVals];
        for (int i = 0; i < numCellVals; i++) {
            cells[i] = 123.123; // assign a non-null val
        }
        return Pair.<SegmentHeader, SegmentBody>of(
            new SegmentHeader(
                "dummySchemaName",
                new ByteString(new byte[]{}),
                "dummyCubeName",
                "dummyMeasureName",
                constrainedColumns,
                Collections.<String>emptyList(),
                "dummyFactTable",
                BitKey.Factory.makeBitKey(3),
                Collections.<SegmentColumn>emptyList()),
            new DenseObjectSegmentBody(
                cells,
                axes));
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



}

// End SegmentBuilderTest.java
