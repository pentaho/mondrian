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
