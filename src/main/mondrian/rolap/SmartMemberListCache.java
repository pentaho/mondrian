package mondrian.rolap;

import java.util.List;

import mondrian.rolap.cache.SmartCache;
import mondrian.rolap.cache.SoftSmartCache;
import mondrian.rolap.sql.SqlConstraint;

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
