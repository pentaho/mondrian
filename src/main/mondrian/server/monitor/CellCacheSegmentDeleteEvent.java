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
 * Deletion of a segment from the cell cache.
 *
 * @version $Id$
 */
public class CellCacheSegmentDeleteEvent extends CellCacheEvent {

    public final int coordinateCount;

    /**
     * Creates a CellCacheSegmentCreateEvent.
     *
     * @param timestamp Timestamp
     * @param locus Locus
     * @param coordinateCount Number of coordinates of segment header
     */
    public CellCacheSegmentDeleteEvent(
        long timestamp,
        Locus locus,
        int coordinateCount)
    {
        super(timestamp, locus);
        this.coordinateCount = coordinateCount;
    }

    public <T> T accept(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}

// End CellCacheSegmentDeleteEvent.java
