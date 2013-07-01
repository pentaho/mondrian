/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2011-2012 Pentaho and others
// All Rights Reserved.
*/
package mondrian.test;

import mondrian.olap.*;
import mondrian.server.monitor.Monitor;
import mondrian.server.monitor.ServerInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Cunning tests to discover whether the cache manager is working to spec
 * and is thread-safe.
 *
 * @author Julian Hyde
 */
public class CacheTest extends FoodMartTestCase {
    /**
     * Tests that if N queries are executed at the same time, only one segment
     * request will be sent. The query that arrives second should see that there
     * is a pending segment in the aggregation manager, and should wait for
     * that.
     *
     * <p>If the test fails, look at segmentCreateViaSqlCount. If it has
     * increased by more than one between before and after, the clients have not
     * managed to share work. If it has not increased, the cache was probably
     * not flushed correctly.</p>
     */
    public void testNQueriesWaitingForSameSegmentRepeat()
        throws ExecutionException, InterruptedException
    {
        final int parallel = 10;
        final ThreadPoolExecutor executor =
            new ThreadPoolExecutor(
                2, parallel, 1, TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(parallel * 2));
        final int repeatCount = 20;
        for (int i = 0; i < repeatCount; i++) {
            checkNQueriesWaitingForSameSegment(
                executor, parallel, "iteration #" + i + " of " + repeatCount);
        }
        executor.shutdown();
    }

    private void checkNQueriesWaitingForSameSegment(
        ThreadPoolExecutor executor,
        int parallel,
        String iteration)
        throws InterruptedException, ExecutionException
    {
        final MondrianServer server =
            MondrianServer.forConnection(getConnection());
        final CacheControl cacheControl =
            getConnection().getCacheControl(null);
        cacheControl.flush(
            cacheControl.createMeasuresRegion(
                getCubeWithName(
                    "Sales",
                    getConnection().getSchema().getCubes())));
        Thread.sleep(2000); // wait for flush to propagate
        final Monitor monitor = server.getMonitor();
        final ServerInfo serverBefore = monitor.getServer();

        final List<Future<Boolean>> futures = new ArrayList<Future<Boolean>>();
        for (int i = 0; i < parallel; i++) {
            Callable<Boolean> runnable = new Callable<Boolean>() {
                public Boolean call() {
                    assertQueryReturns(
                        "select [Gender].Children * [Product].Children on 0\n"
                        + "from [Sales]",
                        "Axis #0:\n"
                        + "{}\n"
                        + "Axis #1:\n"
                        + "{[Gender].[F], [Product].[Drink]}\n"
                        + "{[Gender].[F], [Product].[Food]}\n"
                        + "{[Gender].[F], [Product].[Non-Consumable]}\n"
                        + "{[Gender].[M], [Product].[Drink]}\n"
                        + "{[Gender].[M], [Product].[Food]}\n"
                        + "{[Gender].[M], [Product].[Non-Consumable]}\n"
                        + "Row #0: 12,202\n"
                        + "Row #0: 94,814\n"
                        + "Row #0: 24,542\n"
                        + "Row #0: 12,395\n"
                        + "Row #0: 97,126\n"
                        + "Row #0: 25,694\n");
                    return true;
                }
            };
            futures.add(executor.submit(runnable));
        }
        for (Future<Boolean> future : futures) {
            assertTrue(future.get() == Boolean.TRUE);
        }
        final ServerInfo serverAfter = monitor.getServer();
        final String beforeAfter =
            "before: " + serverBefore + "\n"
            + "after: " + serverAfter + "\n"
            + iteration;
        assertTrue(
            beforeAfter,
            serverAfter.segmentCreateCount
            == serverBefore.segmentCreateCount + 1
            && serverAfter.segmentCreateViaSqlCount
               == serverBefore.segmentCreateViaSqlCount + 1);
    }

    private Cube getCubeWithName(String cubeName, Cube[] cubes) {
        for (Cube cube : cubes) {
            if (cubeName.equals(cube.getName())) {
                return cube;
            }
        }
        return null;
    }
}

// End CacheTest.java
