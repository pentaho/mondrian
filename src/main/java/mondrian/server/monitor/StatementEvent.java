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
 * Event concerning an MDX statement.
 */
public abstract class StatementEvent extends Event {
    /**
     * Identifier of the server.
     */
    public final int serverId;

    /**
     * Identifier of the connection.
     */
    public final int connectionId;

    /**
     * Identifier of the statement. Unique for the lifetime of the JVM.
     */
    public final long statementId;

    /**
     * Creates a StatementEvent.
     *
     * @param timestamp Timestamp
     * @param serverId Server id
     * @param connectionId Connection id
     * @param statementId Statement id
     */
    public StatementEvent(
        long timestamp,
        int serverId,
        int connectionId,
        long statementId)
    {
        super(timestamp);
        this.serverId = serverId;
        this.connectionId = connectionId;
        this.statementId = statementId;
    }
}

// End StatementEvent.java
