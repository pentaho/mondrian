/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
// Copyright (C) 2011-2011 Julian Hyde
// All Rights Reserved.
*/
package mondrian.server.monitor;

/**
 * Event concerning the cell cache.
 *
 * @version $Id$
 */
public abstract class CellCacheEvent extends Event {

    /**
     * Creates a CellCacheEvent.
     *
     * @param timestamp Timestamp
     */
    public CellCacheEvent(
        long timestamp)
    {
        super(timestamp);
    }
}

// End CellCacheEvent.java
