/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 Julian Hyde
// Copyright (C) 2005-2009 Pentaho and others
// All Rights Reserved.
*/

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
