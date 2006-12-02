/*
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2005 TONBELLER AG
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap.cache;

/**
 * Defines a cache API. Implementations exist for hard and soft references.
 * 
 * @author av
 * @since Nov 21, 2005
 * @version $Id$
 */
public interface SmartCache <K, V> {
    /**
     * Places a key/value pair into the queue.
     *
     * @param key Key
     * @param value Value
     * @return the previous value of <code>key</code> or null
     */
    V put(K key, V value);

    V get(K key);

    void clear();

    int size();

}

// End SmartCache.java
