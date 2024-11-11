/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/

package mondrian.rolap;

import mondrian.rolap.cache.*;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * Uses a SmartCache to store a collection of values.
 * Supplements put operations with an "addToEntry", which
 * supports incrementally adding to the collection associated
 * with key.
 */
public class SmartIncrementalCache<K, V extends Collection> {
    SmartCache<K, V> cache;

    public SmartIncrementalCache() {
        cache = new SoftSmartCache<K, V>();
    }

    public V put(final K  key, final V value) {
        return cache.put(key, value);
    }

    public V get(K key) {
        return cache.get(key);
    }

    public void clear() {
        cache.clear();
    }

    public void addToEntry(final K key, final V value) {
        cache
            .execute(
                new SmartCache.SmartCacheTask
                    <K, V>() {
                    public void execute(Iterator<Map.Entry<K, V>> iterator) {
                        // iterator is ignored,
                        // we're updating a single entry and
                        // we have the key.
                        if (cache.get(key) == null) {
                            cache.put(key, value);
                        } else {
                            cache.get(key).addAll(value);
                        }
                    } });
    }

    SmartCache<K, V> getCache() {
        return cache;
    }

    void setCache(SmartCache<K, V> cache) {
        this.cache = cache;
    }

}
// End SmartIncrementalCache.java
