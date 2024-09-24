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
