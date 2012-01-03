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
 * Creation of a segment in the cell cache.
 *
 * @version $Id$
 */
public class CellCacheSegmentCreateEvent extends CellCacheEvent {

    public final int coordinateCount;
    public final int actualCellCount;
    public final Source source;

    /**
     * Creates a CellCacheSegmentCreateEvent.
     *
     * @param timestamp Timestamp
     * @param locus Locus
     * @param coordinateCount Number of coordinates of segment header
     * @param actualCellCount Number of cells in body (or 0 if body not yet
     *     present)
     * @param source Source of segment
     */
    public CellCacheSegmentCreateEvent(
        long timestamp,
        Locus locus,
        int coordinateCount,
        int actualCellCount,
        Source source)
    {
        super(timestamp, locus);
        this.coordinateCount = coordinateCount;
        this.actualCellCount = actualCellCount;
        this.source = source;
    }

    public <T> T accept(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    /**
     * Enumeration of sources of a cell cache segment.
     */
    public enum Source {
        /**
         * A segment that is placed into the cache by an external cache.
         *
         * <p>Some caches (e.g. memcached) never generate this kind of
         * event.</p>
         *
         * <p>In infinispan, one scenario that causes this kind of event is as
         * follows. A user issues an MDX query against a different Mondrian node
         * in the same Infinispan cluster. To resolve missing cells, that node
         * issues a SQL statement to load a segment. Infinispan propagates that
         * segment to its peers, and each peer is notified that an "external
         * segment" is now in the cache.</p>
         */
        EXTERNAL,

        /**
         * A segment that has been loaded in response to a user query,
         * and populated by generating and executing a SQL statement.
         */
        SQL,

        /**
         * a segment that has been loaded in response to a user query,
         * and populated by rolling up existing cache segments.
         */
        ROLLUP,
    }
}

// End CellCacheSegmentCreateEvent.java
