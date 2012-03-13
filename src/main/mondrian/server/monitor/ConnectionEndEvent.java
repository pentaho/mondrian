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
 * Event indicating that a connection has been closed.
 */
public class ConnectionEndEvent extends ConnectionEvent {
    /**
     * Creates a ConnectionEndEvent.
     *
     * @param timestamp Timestamp
     * @param serverId Server id
     * @param connectionId Connection id
     */
    public ConnectionEndEvent(
        long timestamp,
        int serverId,
        int connectionId)
    {
        super(timestamp, serverId, connectionId);
    }

    public String toString() {
        return "ConnectionEndEvent(" + connectionId + ")";
    }

    public <T> T accept(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}

// End ConnectionEndEvent.java
