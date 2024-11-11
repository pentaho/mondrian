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
