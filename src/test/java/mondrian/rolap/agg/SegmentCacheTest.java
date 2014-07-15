/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2011-2013 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap.agg;

import mondrian.olap.CacheControl;
import mondrian.olap.Cube;
import mondrian.olap.MondrianServer;
import mondrian.spi.SegmentCache;
import mondrian.spi.SegmentHeader;
import mondrian.test.BasicQueryTest;

import java.util.ArrayList;
import java.util.List;

/**
 * Test suite that runs the {@link BasicQueryTest} but with the
 * {@link MockSegmentCache} active.
 *
 * @author LBoudreau
 */
public class SegmentCacheTest extends BasicQueryTest {
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        getTestContext().getConnection().getCacheControl(null)
            .flushSchemaCache();
    }

    public void testCompoundPredicatesCollision() {
        String query =
            "SELECT [Gender].[All Gender] ON 0, [MEASURES].[CUSTOMER COUNT] ON 1 FROM SALES";
        String query2 =
            "WITH MEMBER GENDER.X AS 'AGGREGATE({[GENDER].[GENDER].members} * "
            + "{[STORE].[ALL STORES].[USA].[CA]})', solve_order=100 "
            + "SELECT GENDER.X ON 0, [MEASURES].[CUSTOMER COUNT] ON 1 FROM SALES";
        String result =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Customer].[Gender].[All Gender]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Customer Count]}\n"
            + "Row #0: 5,581\n";
        String result2 =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Customer].[Gender].[X]}\n"
            + "Axis #2:\n"
            + "{[Measures].[Customer Count]}\n"
            + "Row #0: 2,716\n";
        assertQueryReturns(query, result);
        assertQueryReturns(query2, result2);
    }

    public void testSegmentCacheEvents() throws Exception {
        SegmentCache mockCache = new MockSegmentCache();
        SegmentCacheWorker testWorker =
            new SegmentCacheWorker(mockCache, null);

        // Flush the cache before we start. Wait a second for the cache
        // flush to propagate.
        final CacheControl cc =
            getTestContext().getConnection().getCacheControl(null);
        Cube salesCube = getCube("Sales");
        cc.flush(cc.createMeasuresRegion(salesCube));
        Thread.sleep(1000);

        MondrianServer.forConnection(getTestContext().getConnection())
            .getAggregationManager().cacheMgr.segmentCacheWorkers
            .add(testWorker);

        final List<SegmentHeader> createdHeaders =
            new ArrayList<SegmentHeader>();
        final List<SegmentHeader> deletedHeaders =
            new ArrayList<SegmentHeader>();
        final SegmentCache.SegmentCacheListener listener =
            new SegmentCache.SegmentCacheListener() {
                public void handle(SegmentCacheEvent e) {
                    switch (e.getEventType()) {
                    case ENTRY_CREATED:
                        createdHeaders.add(e.getSource());
                        break;
                    case ENTRY_DELETED:
                        deletedHeaders.add(e.getSource());
                        break;
                    default:
                        throw new UnsupportedOperationException();
                    }
                }
            };

        try {
            // Register our custom listener.
            MondrianServer
                .forConnection(getTestContext().getConnection())
                .getAggregationManager().cacheMgr.compositeCache
                .addListener(listener);
            // Now execute a query and check the events
            executeQuery(
                "select {[Measures].[Unit Sales]} on columns from [Sales]");
            // Wait for propagation.
            Thread.sleep(2000);
            assertEquals(2, createdHeaders.size());
            assertEquals(0, deletedHeaders.size());
            assertEquals("Sales", createdHeaders.get(0).cubeName);
            assertEquals("FoodMart", createdHeaders.get(0).schemaName);
            assertEquals("Unit Sales", createdHeaders.get(0).measureName);
            createdHeaders.clear();
            deletedHeaders.clear();

            // Now flush the segment and check the events.
            cc.flush(cc.createMeasuresRegion(salesCube));

            // Wait for propagation.
            Thread.sleep(2000);
            assertEquals(0, createdHeaders.size());
            assertEquals(2, deletedHeaders.size());
            assertEquals("Sales", deletedHeaders.get(0).cubeName);
            assertEquals("FoodMart", deletedHeaders.get(0).schemaName);
            assertEquals("Unit Sales", deletedHeaders.get(0).measureName);
        } finally {
            MondrianServer
                .forConnection(getTestContext().getConnection())
                .getAggregationManager().cacheMgr.compositeCache
                .removeListener(listener);
            MondrianServer.forConnection(getTestContext().getConnection())
                .getAggregationManager().cacheMgr.segmentCacheWorkers
                .remove(testWorker);
        }
    }

    private Cube getCube(String cubeName) {
        for (Cube cube
            : getConnection().getSchemaReader().withLocus().getCubes())
        {
            if (cube.getName().equals(cubeName)) {
                return cube;
            }
        }
        return null;
    }
}

// End SegmentCacheTest.java
