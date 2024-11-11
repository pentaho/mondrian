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

/**
 * A <code>CachePool</code> manages the objects in a collection of
 * caches.
 *
 * @author av
 */
public class CachePool {

    /** The singleton. */
    private static CachePool instance = new CachePool();

    /** Returns the singleton. */
    public static CachePool instance() {
        return instance;
    }

    private CachePool() {
    }
}

// End CachePool.java
