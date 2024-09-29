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
 * Event concerning a connection.
 */
public abstract class ConnectionEvent extends Event {
    /**
     * Server identifier; corresponds to
     * {@link mondrian.olap.MondrianServer#getId()}.
     */
    public final int serverId;

    /**
     * Connection identifier. To retrieve the connection, call
     * {@link mondrian.olap.MondrianServer#getConnection(int)}
     */
    public final int connectionId;

    /**
     * Creates a ConnectionEvent.
     *
     * @param timestamp Timestamp
     * @param serverId Server id
     * @param connectionId Connection id
     */
    public ConnectionEvent(
        long timestamp,
        int serverId,
        int connectionId)
    {
        super(timestamp);
        this.serverId = serverId;
        this.connectionId = connectionId;
    }
}

// End ConnectionEvent.java
