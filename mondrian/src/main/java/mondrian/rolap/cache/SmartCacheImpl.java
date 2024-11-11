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


package mondrian.rolap.cache;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.locks.*;

/**
 * A base implementation of the {@link SmartCache} interface which
 * enforces synchronization with a ReadWrite lock.
 */
public abstract class SmartCacheImpl<K, V>
    implements SmartCache<K, V>
{
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Must provide an iterator on the contents of the cache.
     * It does not need to be thread safe because we will handle
     * that in {@link SmartCacheImpl}.
     */
    protected abstract Iterator<Entry<K, V>> iteratorImpl();

    protected abstract V putImpl(K key, V value);
    protected abstract V getImpl(K key);
    protected abstract V removeImpl(K key);
    protected abstract void clearImpl();
    protected abstract int sizeImpl();

    public V put(K key, V value) {
        lock.writeLock().lock();
        try {
            return putImpl(key, value);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public V get(K key) {
        lock.readLock().lock();
        try {
            return getImpl(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    public V remove(K key) {
        lock.writeLock().lock();
        try {
            return removeImpl(key);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void clear() {
        lock.writeLock().lock();
        try {
            clearImpl();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public int size() {
        lock.readLock().lock();
        try {
            return sizeImpl();
        } finally {
            lock.readLock().unlock();
        }
    }

    public void execute(SmartCache.SmartCacheTask<K, V> task) {
        lock.writeLock().lock();
        try {
            task.execute(iteratorImpl());
        } finally {
            lock.writeLock().unlock();
        }
    }
}
// End SmartCacheImpl.java
