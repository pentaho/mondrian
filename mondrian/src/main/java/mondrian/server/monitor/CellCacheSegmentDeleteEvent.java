/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

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
