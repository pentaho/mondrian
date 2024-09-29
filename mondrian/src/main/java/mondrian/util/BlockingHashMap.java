/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


package mondrian.util;

import mondrian.olap.Util;

import java.util.concurrent.ConcurrentHashMap;

/**
 * A limited Map implementation which supports waiting for a value
 * to be available when calling get(). Intended for use with
 * producer/consumer queues, where a producer thread puts a value into
 * the collection with a separate thread waiting to get that value.
 * Currently used by the Actor implementations in
 * <code>SegmentCacheManager</code> and <code>MonitorImpl</code>.
 *
 * <p><b>Thread safety</b>. BlockingHashMap is thread safe.  The class
 * delegates all get and put operations to a ConcurrentHashMap. </p>
 *
 * @param <K> request (key) type
 * @param <V> response (value) type
 */
public class BlockingHashMap<K, V> {
    private final ConcurrentHashMap<K, SlotFuture<V>> map;

    /**
     * Creates a BlockingHashMap with given capacity.
     *
     * @param capacity Capacity
     */
    public BlockingHashMap(int capacity) {
        map = new ConcurrentHashMap<K, SlotFuture<V>>(capacity);
    }

    /**
     * Places a (request, response) pair onto the map.
     *
     * @param k key
     * @param v value
     */
    public void put(K k, V v) {
        map.putIfAbsent(k, new SlotFuture<V>());
        map.get(k).put(v);
    }

    /**
     * Retrieves the response from the map matching the given key,
     * blocking until it is received.
     *
     * @param k key
     * @return value
     * @throws InterruptedException if interrupted while waiting
     */
    public V get(K k) throws InterruptedException {
        map.putIfAbsent(k, new SlotFuture<V>());
        V v = Util.safeGet(
            map.get(k),
            "Waiting to retrieve a value from BlockingHashMap.");
        map.remove(k);
        return v;
    }
}
