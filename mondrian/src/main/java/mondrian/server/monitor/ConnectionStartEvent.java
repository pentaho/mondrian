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
 * Event indicating that a connection has been created.
 */
public class ConnectionStartEvent extends ConnectionEvent {
    /**
     * Creates a ConnectionStartEvent.
     *
     * @param timestamp Timestamp
     * @param serverId Server id
     * @param connectionId Connection id
     */
    public ConnectionStartEvent(
        long timestamp,
        int serverId,
        int connectionId)
    {
        super(timestamp, serverId, connectionId);
    }

    public String toString() {
        return "ConnectionStartEvent(" + connectionId + ")";
    }

    public <T> T accept(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}

// End ConnectionStartEvent.java
