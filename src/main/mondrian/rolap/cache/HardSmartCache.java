/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2006-2012 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap.cache;

import java.util.*;

/**
 * An implementation of {@link SmartCache} that uses hard
 * references. Used for testing.
 */
public class HardSmartCache <K, V> extends SmartCacheImpl<K, V> {
    Map<K, V> cache = new HashMap<K, V>();

    public V putImpl(K key, V value) {
        return cache.put(key, value);
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

// End HardSmartCache.java
