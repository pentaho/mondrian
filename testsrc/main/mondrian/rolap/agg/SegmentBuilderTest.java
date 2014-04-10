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
                "78a261a0d167093d12cc2f279d1a8a19cc9d7050ad004ef8fee38ca45cc08101",
                    /* ^^^
                        {time_by_day.the_year=('1998')}
                        {time_by_day.quarter=('Q1','Q2','Q3')}
                        {time_by_day.month_of_year=('2','3','4','5','6','7')}]
                     */
                "c99bd7b377b7181a9438adf47834fd2f3b5cb086614c587bd2dd9a94396a541d"
                    /* ^^^
                        {time_by_day.the_year=(*)}
                        {time_by_day.quarter=('Q1','Q3','Q4')}
                        {time_by_day.month_of_year=('1','8','9','10','11','12')}]
                     */
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
            + "    {time_by_day.the_year=('1998')}\n"
            + "    {time_by_day.quarter=('Q1','Q3')}]\n"
            + "Excluded Regions:[]\n"
            + "Compound Predicates:[]\n"
            + "ID:[685502fc54fc3d05e4438ee812b26799a05e25fbbdd6c75b0133d62480bcbd98]\n");
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
              "6fa249f861b66fef01fc8ad41e116d1a1316563c440e7c8df8fd5457713e3fc6",
                /* ^^^
                    {time_by_day.the_year=('1997')}
                    {product_class.product_family=(*)}
                    {product_class.product_department=('Alcoholic Beverages',
                     'Baked Goods','Beverages','Periodicals')}]

                 */
              "28998f37f3a12b6b566e5542c1de63ad89c7c024cded868476745f4a2eb2724e"
                /* ^^^
                    {time_by_day.the_year=('1997')}
                    {product_class.product_family=('Drink')}
                    {product_class.product_department=('Dairy')}]
                 */
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
                "348d8b84a4322bce4abaa6f22fffb43beeaa005bb84eee2fe95c52e8467c9e17",
                    /* ^^^
                        {time_by_day.the_year=('1997')}
                        {product_class.product_family=('Drink','Non-Consumable')}
                        {product_class.product_department=('Alcoholic Beverages',
                         'Beverages','Periodicals')}]
                     */
                "28998f37f3a12b6b566e5542c1de63ad89c7c024cded868476745f4a2eb2724e"
                    /* ^^^
                        {time_by_day.the_year=('1997')}
                        {product_class.product_family=('Drink')}
                        {product_class.product_department=('Dairy')}]
                     */
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
                "28998f37f3a12b6b566e5542c1de63ad89c7c024cded868476745f4a2eb2724e",
                    /* ^^^
                        {time_by_day.the_year=('1997')}
                        {product_class.product_family=('Drink')}
                        {product_class.product_department=('Dairy')}]

                     */
                "c5e912e9af50f82339c8bc2e3806e0b6d1e4e2029c3b1ae9ebef504d0799037b",
                    /* ^^^
                        {time_by_day.the_year=('1997')}
                        {product_class.product_family=('Drink')}
                        {product_class.product_department=('Beverages')}]

                     */
                "bdf9f996de66edb6477d4ae016a1a4a6508c324c8641833e03bb2b869dc83d77"
                    /* ^^^
                        {time_by_day.the_year=('1997')}
                        {product_class.product_family=('Drink','Non-Consumable')}
                        {product_class.product_department=('Alcoholic Beverages','Periodicals')}]

                     */
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
        assertFalse(String.format("SegmentMap is empty. No segmentIds matched test parameters. Full segment cache: %s", headers), testMap.isEmpty());
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
