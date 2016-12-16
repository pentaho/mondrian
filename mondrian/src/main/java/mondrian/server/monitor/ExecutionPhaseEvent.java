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
 * Event signalling the start of a phase of executing an MDX statement.
 *
 * <p>A phase begins when Mondrian has tried to execute a statement and has
 * determined that it needs cell values in order to give the complete, correct
 * result. It generates one or more SQL statements to load those cells, and
 * starts a new phase. Most MDX statements can be completed in 3 or fewer
 * phases.</p>
 */
public class ExecutionPhaseEvent extends ExecutionEvent {
    public final int phase;
    public final int hitCount;
    public final int missCount;
    public final int pendingCount;

    /**
     * Creates an ExecutionPhaseEvent.
     *
     * @param timestamp Timestamp
     * @param serverId Server id
     * @param connectionId Connection id
     * @param statementId Statement id
     * @param executionId Execution id
     * @param phase Phase
     * @param hitCount Cache hits this phase
     * @param missCount Cache misses this phase
     * @param pendingCount Cache entries hit but not ready this phase
     */
    public ExecutionPhaseEvent(
        long timestamp,
        int serverId,
        int connectionId,
        long statementId,
        long executionId,
        int phase,
        int hitCount,
        int missCount,
        int pendingCount)
    {
        super(timestamp, serverId, connectionId, statementId, executionId);
        this.phase = phase;
        this.hitCount = hitCount;
        this.missCount = missCount;
        this.pendingCount = pendingCount;
    }

    @Override
    public String toString() {
        return "ExecutionPhaseEvent(" + executionId + ", " + phase + ")";
    }

    public <T> T accept(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}

// End ExecutionPhaseEvent.java
