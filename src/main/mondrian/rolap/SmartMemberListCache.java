/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2006-2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.rolap.cache.SmartCache;
import mondrian.rolap.cache.SoftSmartCache;
import mondrian.rolap.sql.SqlConstraint;

/**
 * Uses a {@link mondrian.rolap.cache.SmartCache} to store lists of members,
 * where the key depends on a {@link mondrian.rolap.sql.SqlConstraint}.
 *
 * <p>Example 1:
 *
 * <pre>
 *   select ...
 *   [Customer].[Name].members on rows
 *   ...
 * </pre>
 *
 * <p>Example 2:
 * <pre>
 *   select ...
 *   NON EMPTY [Customer].[Name].members on rows
 *   ...
 *   WHERE ([Store#14], [Product].[Product#1])
 * </pre>
 *
 * <p>The first set, <em>all</em> customers are computed, in the second only
 * those, who have bought Product#1 in Store#14. We want to put both results
 * into the cache. Then the key for the cache entry is the Level that the
 * members belong to <em>plus</em> the costraint that restricted the amount of
 * members fetched. For Level.Members the key consists of the Level and the
 * cacheKey of the {@link mondrian.rolap.sql.SqlConstraint}.
 *
 * @see mondrian.rolap.sql.SqlConstraint#getCacheKey
 *
 * @author av
 * @since Nov 21, 2005
 * @version $Id$
 */
public class SmartMemberListCache <K, V> {
    SmartCache<Key2<K, Object>, V> cache;

    /**
     * a HashMap key that consists of two components.
     */
    static class Key2 <T1, T2> {
        T1 o1;
        T2 o2;

        public Key2(T1 o1, T2 o2) {
            this.o1 = o1;
            this.o2 = o2;
        }

        public boolean equals(Object obj) {
            if (!(obj instanceof Key2)) {
                return false;
            }
            Key2 that = (Key2) obj;
            return equals(this.o1, that.o1) && equals(this.o2, that.o2);
        }

        private boolean equals(Object o1, Object o2) {
            return o1 == null ? o2 == null : o1.equals(o2);
        }

        public int hashCode() {
            int c = 1;
            if (o1 != null) {
                c = o1.hashCode();
            }
            if (o2 != null) {
                c = 31 * c + o2.hashCode();
            }
            return c;
        }

        public String toString() {
            return "key(" + o1 + "," + o2 + ")";
        }
    }

    public SmartMemberListCache() {
        cache = new SoftSmartCache<Key2<K, Object>, V>();
    }

    public Object put(K key, SqlConstraint constraint, V value) {
        Object cacheKey = constraint.getCacheKey();
        if (cacheKey == null) {
            return null;
        }
        Key2<K, Object> key2 = new Key2<K, Object>(key, cacheKey);
        return cache.put(key2, value);
    }

    public V get(K key, SqlConstraint constraint) {
        Key2<K, Object> key2 =
            new Key2<K, Object>(key, constraint.getCacheKey());
        return cache.get(key2);
    }

    public void clear() {
        cache.clear();
    }

    SmartCache<Key2<K, Object>, V> getCache() {
        return cache;
    }

    void setCache(SmartCache<Key2<K, Object>, V> cache) {
        this.cache = cache;
    }
}

// End SmartMemberListCache.java
