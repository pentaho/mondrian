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

package mondrian.server.monitor;

/**
 * Deletion of a segment from the cell cache.
 */
public class CellCacheSegmentDeleteEvent extends CellCacheEvent {

    public final int coordinateCount;

    /**
     * Creates a CellCacheSegmentCreateEvent.
     *
     * @param timestamp Timestamp
     * @param serverId ID of the server from which the event originates.
     * @param connectionId ID of the connection from which the event
     * originates.
     * @param statementId ID of the statement from which the event originates.
     * @param executionId ID of the execution from which the event originates.
     * @param coordinateCount Number of coordinates of segment header
     * @param source Source of segment
     */
    public CellCacheSegmentDeleteEvent(
        long timestamp,
        int serverId,
        int connectionId,
        long statementId,
        long executionId,
        int coordinateCount,
        Source source)
    {
        super(
            timestamp, serverId, connectionId,
            statementId, executionId, source);
        this.coordinateCount = coordinateCount;
    }

    public <T> T accept(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}

// End CellCacheSegmentDeleteEvent.java
