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
 * Event created just after a statement has been created.
 */
public class StatementEndEvent extends StatementEvent {
    /**
     * Creates a StatementStartEvent.
     *
     * @param timestamp Timestamp
     * @param serverId Server id
     * @param connectionId Connection id
     * @param statementId Statement id
     */
    public StatementEndEvent(
        long timestamp,
        int serverId,
        int connectionId,
        long statementId)
    {
        super(timestamp, serverId, connectionId, statementId);
    }

    public String toString() {
        return "StatementEndEvent(" + statementId + ")";
    }

    public <T> T accept(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}

// End StatementEndEvent.java
