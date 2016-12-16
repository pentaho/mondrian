/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.server.monitor;

import mondrian.server.Execution;

/**
 * Event concerning the execution of an MDX statement.
 */
public class ExecutionEndEvent extends ExecutionEvent {
    public final int phaseCount;
    public final Execution.State state;
    public final int cellCacheHitCount;
    public final int cellCacheMissCount;
    public final int cellCachePendingCount;

    /**
     * Creates an ExecutionEndEvent.
     *
     * @param timestamp Timestamp
     * @param serverId Server id
     * @param connectionId Connection id
     * @param statementId Statement id
     * @param executionId Execution id
     * @param phaseCount Number of execution phases (trips to DBMS to populate
     *   cache)
     * @param state State; indicates reason why execution terminated
     * @param cellCacheHitCount Number of cell requests for which cell was
     *   already in cache
     * @param cellCacheMissCount Number of cell requests for which cell was
     *   not in cache
     * @param cellCachePendingCount Number of cell requests for which cell was
     */
    public ExecutionEndEvent(
        long timestamp,
        int serverId,
        int connectionId,
        long statementId,
        long executionId,
        int phaseCount,
        Execution.State state,
        int cellCacheHitCount,
        int cellCacheMissCount,
        int cellCachePendingCount)
    {
        super(timestamp, serverId, connectionId, statementId, executionId);
        this.phaseCount = phaseCount;
        this.state = state;
        this.cellCacheHitCount = cellCacheHitCount;
        this.cellCacheMissCount = cellCacheMissCount;
        this.cellCachePendingCount = cellCachePendingCount;
    }

    @Override
    public String toString() {
        return "ExecutionEndEvent(" + executionId + ")";
    }

    public <T> T accept(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}

// End ExecutionEndEvent.java
