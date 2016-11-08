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
