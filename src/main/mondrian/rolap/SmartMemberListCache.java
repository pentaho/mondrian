/*
//This software is subject to the terms of the Common Public License
//Agreement, available at the following URL:
//http://www.opensource.org/licenses/cpl.html.
//Copyright (C) 2004-2005 TONBELLER AG
//All Rights Reserved.
//You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import java.util.List;

import mondrian.rolap.cache.SmartCache;
import mondrian.rolap.cache.SoftSmartCache;
import mondrian.rolap.sql.SqlConstraint;

/**
 * uses a {@link mondrian.rolap.cache.SmartCache} to store lists of members, where the key
 * depends on a {@link mondrian.rolap.sql.SqlConstraint}.
 * <p>
 * Example 1
 * <pre>
 *   select ...
 *   [Customer].[Name].members on rows
 *   ...
 * </pre>
 * Example 2
 * <pre>
 *   select ...
 *   NON EMPTY [Customer].[Name].members on rows
 *   ...
 *   WHERE ([Store#14], [Product].[Product#1])
 * </pre>
 * 
 * The first set, <em>all</em> customers are computed, in the second only those, who
 * have bought Product#1 in Store#14. We want to put both results into the cache. Then the
 * key for the cache entry is the Level that the members belong to <em>plus</em> the
 * costraint that restricted the amount of members fetched. For Level.Members the key
 * consists of the Level and the cacheKey of the {@link mondrian.rolap.sql.SqlConstraint}
 * @see @link mondrian.rolap.sql.SqlConstraint#getCacheKey() 
 * 
 * @author av
 * @since Nov 21, 2005
 */
public class SmartMemberListCache {
    SmartCache cache;

    /**
     * a HashMap key that consists of two components.
     */
    static class Key2 {
        Object o1, o2;

        public Key2(Object o1, Object o2) {
            this.o1 = o1;
            this.o2 = o2;
        }

        public boolean equals(Object obj) {
            if (!(obj instanceof Key2))
                return false;
            Key2 that = (Key2) obj;
            return equals(this.o1, that.o1) && equals(this.o2, that.o2);
        }

        private boolean equals(Object o1, Object o2) {
            return o1 == null ? o2 == null : o1.equals(o2);
        }

        public int hashCode() {
            int c = 1;
            if (o1 != null)
                c = o1.hashCode();
            if (o2 != null)
                c = 31 * c + o2.hashCode();
            return c;
        }

        public String toString() {
            return "key(" + o1 + "," + o2 + ")";
        }

    }

    public SmartMemberListCache() {
        cache = new SoftSmartCache();
    }

    public Object put(Object key, SqlConstraint constraint, List value) {
        Object cacheKey = constraint.getCacheKey();
        if (cacheKey == null)
            return null;
        key = new Key2(key, cacheKey);
        return cache.put(key, value);
    }

    public List get(Object key, SqlConstraint constraint) {
        key = new Key2(key, constraint.getCacheKey());
        return (List) cache.get(key);
    }

    public void clear() {
        cache.clear();
    }

    SmartCache getCache() {
        return cache;
    }

    void setCache(SmartCache cache) {
        this.cache = cache;
    }

}
