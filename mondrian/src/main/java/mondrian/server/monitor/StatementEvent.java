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
