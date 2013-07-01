/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2006-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap.cache;

import org.apache.commons.collections.map.ReferenceMap;

import java.util.*;

/**
 * An implementation of {@link SmartCacheImpl} which uses a
 * {@link ReferenceMap} as a backing object. Both the key
 * and the value are soft references, because of their
 * cyclic nature.
 *
 * <p>This class does not enforce any synchronization, because
 * this is handled by {@link SmartCacheImpl}.
 *
 * @author av, lboudreau
 * @since Nov 3, 2005
 */
public class SoftSmartCache<K, V> extends SmartCacheImpl<K, V> {

    @SuppressWarnings("unchecked")
    private final Map<K, V> cache =
        new ReferenceMap(ReferenceMap.SOFT, ReferenceMap.SOFT);

    public V putImpl(K key, V value) {
        // Null values are the same as a 'remove'
        // Convert the operation because ReferenceMap doesn't
        // like null values.
        if (value == null) {
            return cache.remove(key);
        } else {
            return cache.put(key, value);
        }
    }

    public V getImpl(K key) {
        return cache.get(key);
    }

    public V removeImpl(K key) {
        return cache.remove(key);
    }

    public void clearImpl() {
        cache.clear();
    }

    public int sizeImpl() {
        return cache.size();
    }

    public Iterator<Map.Entry<K, V>> iteratorImpl() {
        return cache.entrySet().iterator();
    }
}

// End SoftSmartCache.java

