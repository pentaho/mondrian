/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2011-2011 Pentaho
// All Rights Reserved.
*/
package mondrian.server.monitor;

/**
 * Event signalling the start of executing an MDX statement.
 */
public class ExecutionStartEvent extends ExecutionEvent {
    public final String mdx;

    /**
     * Creates an ExecutionStartEvent.
     *
     * @param timestamp Timestamp
     * @param serverId Server id
     * @param connectionId Connection id
     * @param statementId Statement id
     * @param executionId Execution id
     * @param mdx MDX string
     */
    public ExecutionStartEvent(
        long timestamp,
        int serverId,
        int connectionId,
        long statementId,
        long executionId,
        String mdx)
    {
        super(timestamp, serverId, connectionId, statementId, executionId);
        this.mdx = mdx;
    }

    @Override
    public String toString() {
        return "ExecutionStartEvent(" + executionId + ")";
    }

    public <T> T accept(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}

// End ExecutionStartEvent.java
