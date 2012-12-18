/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2006-2012 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap.cache;

import mondrian.util.Pair;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.*;

/**
 * A map with soft references that is cleaned up in regular intervals.
 * <p>
 * There is no contains(key) method because it makes no sense - after
 * contains() returns true, the garbage collector may remove
 * the value that was contained. Instead the code should call get() and
 * keep a reference to the value to prevent garbage collection.
 *
 * @author av
 * @since Nov 3, 2005
 */
public class SoftSmartCache<K, V> extends SmartCacheImpl<K, V> {

    private final Map<K, CacheReference> cache =
        new HashMap<K, CacheReference>();

    private final ReferenceQueue<V> queue = new ReferenceQueue<V>();

    /**
     * an entry in the cache that contains the key for
     * the cache map to remove the entry when its value
     * has been garbage collected
     *
     * @author rk
     * @since Nov 7, 2005
     */
    class CacheReference extends SoftReference<V> {
        K key;

        public CacheReference(K key, V value) {
            super(value, queue);
            this.key = key;
        }

        public String toString() {
            return String.valueOf(get());
        }
    }

    public V putImpl(K key, V value) {
        // remove garbage collected entries from cache
        CacheReference ref;
        while ((ref = (CacheReference) queue.poll()) != null) {
            cache.remove(ref.key);
        }

        // put new entry into cache
        ref = new CacheReference(key, value);
        ref = cache.put(key, ref);
        if (ref != null) {
            return ref.get();
        }
        return null;
    }

    public V getImpl(K key) {
        CacheReference ref = cache.get(key);
        if (ref == null) {
            return null;
        }
        V value = ref.get();
        if (value == null) {
            cache.remove(key);
        }
        return value;
    }

    public V removeImpl(K key) {
        final CacheReference ref = cache.remove(key);
        return ref == null ? null : ref.get();
    }

    public void clearImpl() {
        cache.clear();
    }

    public int sizeImpl() {
        return cache.size();
    }

    public Iterator<Map.Entry<K, V>> iteratorImpl() {
        final Iterator<Map.Entry<K, CacheReference>> cacheIterator =
            cache.entrySet().iterator();
        return new Iterator<Map.Entry<K, V>>() {
            private Map.Entry<K, V> entry;

            public boolean hasNext() {
                if (entry != null) {
                    return true;
                }
                while (cacheIterator.hasNext()) {
                    Map.Entry<K, CacheReference> cacheEntry =
                        cacheIterator.next();
                    // skip over entries that have been garbage collected
                    final V value = cacheEntry.getValue().get();
                    if (value != null) {
                        // Would use AbstractMap.SimpleEntry but it's not public
                        // until JDK 1.6.
                        entry = new Pair<K, V>(cacheEntry.getKey(), value);
                        return true;
                    }
                }
                return false;
            }

            public Map.Entry<K, V> next() {
                Map.Entry<K, V> entry = this.entry;
                this.entry = null;
                return entry;
            }

            public void remove() {
                cacheIterator.remove();
            }
        };
    }
}

// End SoftSmartCache.java

