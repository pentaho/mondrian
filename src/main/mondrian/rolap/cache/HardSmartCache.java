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
public class HardSmartCache <K, V> implements SmartCache <K, V> {
    Map<K, V> cache = new HashMap<K, V>();

    public V put(K key, V value) {
        return cache.put(key, value);
    }

    public V get(K key) {
        return cache.get(key);
    }

    public V remove(K key) {
        return cache.remove(key);
    }

    public void clear() {
        cache.clear();
    }

    public int size() {
        return cache.size();
    }

    public Iterator<Map.Entry<K, V>> iterator() {
        return cache.entrySet().iterator();
    }
}

// End HardSmartCache.java
