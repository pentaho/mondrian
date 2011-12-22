/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2004-2009 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap.cache;

import mondrian.rolap.RolapSchema;

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
