
package mondrian.spi;

import java.util.Set;

public interface CacheAdapter<K, V> {

    V get(K key);

    
    void put(K key, V value);

    void invalidate(K key);

    void invalidateAll(Set<K> keys);

    void invalidateAll();

}
