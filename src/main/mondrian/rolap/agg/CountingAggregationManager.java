/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2002-2009 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
*/

package mondrian.rolap.agg;

import mondrian.olap.Util;
import mondrian.rolap.RolapStar;
import mondrian.rolap.RolapAggregationManager.PinSet;

/**
 * This class adds to {@link AggregationManager} counters for
 * aggregation cache hit and miss. It should only be used for testing
 * purpose due to potential performance regression by the
 * introduction of synchronized blocks.
 *
 * @author Khanh Vu
 * @version $Id$
 */
public class CountingAggregationManager extends AggregationManager {
    private SynchronizedCounter requestCount;
    private SynchronizedCounter missCount;

    CountingAggregationManager() {
        super();
        requestCount = new SynchronizedCounter();
        missCount = new SynchronizedCounter();
    }

    /**
     * Calls super method and sets counters.
     */
    public Object getCellFromCache(CellRequest request) {
        requestCount.increment();
        Object obj = super.getCellFromCache(request);
        if (obj == null) {
            missCount.increment();
        }
        return obj;
    }

    /**
     * Calls super method and sets counters.
     */
    public Object getCellFromCache(CellRequest request, PinSet pinSet) {
        requestCount.increment();
        Object obj = super.getCellFromCache(request, pinSet);
        if (obj == null) {
            missCount.increment();
        }
        return obj;
    }

    /**
     * Returns total number of cache requests.
     *
     * @return an integer represents value of request counter
     */
    public int getRequestCount() {
        return requestCount.value();
    }

    /**
     * Returns number of cache misses.
     *
     * @return an integer represents value of cache miss counter
     */
    public int getMissCount() {
        return missCount.value();
    }

    /**
     * Returns the cache hit ratio.
     *
     * @return a double value represent hit ratio
     */
    public double getHitRatio() {
        return ((double) requestCount.value() - missCount.value()) /
            requestCount.value();
    }

    /**
     * Resets both counters to zero
     *
     */
    public void resetCounters() {
        requestCount.reset();
        missCount.reset();
    }

    private class SynchronizedCounter {
        private int c = 0;

        public synchronized void increment() {
            c++;
        }

        public synchronized void reset() {
            c = 0;
        }

        public synchronized int value() {
            return c;
        }
    }
}

// End CountingAggregationManager.java
