/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
// Copyright (C) 2011-2012 Julian Hyde
// All Rights Reserved.
*/
package mondrian.server.monitor;

import mondrian.server.Locus;

/**
 * Event concerning the cell cache.
 *
 * @version $Id$
 */
public abstract class CellCacheEvent extends ExecutionEvent {

    /**
     * Creates a CellCacheEvent.
     *
     * @param timestamp Timestamp
     * @param locus Locus
     */
    public CellCacheEvent(long timestamp, Locus locus) {
        super(timestamp, locus);
    }
}

// End CellCacheEvent.java
