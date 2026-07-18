/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/



package mondrian.server.monitor;

/**
 * Event created just after a statement has been created.
 */
public class StatementStartEvent extends StatementEvent {
    /**
     * Creates a StatementStartEvent.
     *
     * @param timestamp Timestamp
     * @param serverId Server id
     * @param connectionId Connection id
     * @param statementId Statement id
     */
    public StatementStartEvent(
        long timestamp,
        int serverId,
        int connectionId,
        long statementId)
    {
        super(timestamp, serverId, connectionId, statementId);
    }

    public String toString() {
        return "StatementStartEvent(" + statementId + ")";
    }

    public <T> T accept(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}

// End StatementStartEvent.java
