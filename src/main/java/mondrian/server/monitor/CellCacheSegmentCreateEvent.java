/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2011-2012 Pentaho
// All Rights Reserved.
*/
package mondrian.server.monitor;

/**
 * Creation of a segment in the cell cache.
 */
public class CellCacheSegmentCreateEvent extends CellCacheEvent {

    public final int coordinateCount;
    public final int actualCellCount;

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
     * @param actualCellCount Number of cells in body (or 0 if body not yet
     *     present)
     * @param source Source of segment
     */
    public CellCacheSegmentCreateEvent(
        long timestamp,
        int serverId,
        int connectionId,
        long statementId,
        long executionId,
        int coordinateCount,
        int actualCellCount,
        Source source)
    {
        super(
            timestamp, serverId, connectionId,
            statementId, executionId, source);
        this.coordinateCount = coordinateCount;
        this.actualCellCount = actualCellCount;
    }

    public <T> T accept(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}

// End CellCacheSegmentCreateEvent.java
