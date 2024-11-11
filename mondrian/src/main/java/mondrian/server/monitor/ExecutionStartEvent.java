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
