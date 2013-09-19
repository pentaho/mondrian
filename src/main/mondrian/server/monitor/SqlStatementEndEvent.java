/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.server.monitor;

import mondrian.server.Locus;

/**
 * Event created when Mondrian finishes executing an SQL statement.
 */
public class SqlStatementEndEvent extends SqlStatementEvent {
    public final long rowFetchCount;
    public final boolean canceled;
    public final Throwable throwable;

    /**
     * Creates a SqlStatementEndEvent.
     *
     * @param timestamp Timestamp
     * @param sqlStatementId SQL statement id
     * @param locus Locus of event
     * @param sql SQL
     * @param purpose Why Mondrian is executing this statement
     * @param rowFetchCount Number of rows fetched
     * @param canceled Whether statement was canceled
     * @param throwable Throwable, or null if there was no error
     */
    public SqlStatementEndEvent(
        long timestamp,
        long sqlStatementId,
        Locus locus,
        String sql,
        Purpose purpose,
        long rowFetchCount,
        boolean canceled,
        Throwable throwable)
    {
        super(timestamp, sqlStatementId, locus, sql, purpose);
        this.rowFetchCount = rowFetchCount;
        this.canceled = canceled;
        this.throwable = throwable;
    }

    public String toString() {
        return "SqlStatementEndEvent(" + sqlStatementId + ")";
    }

    public <T> T accept(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}

// End SqlStatementEndEvent.java
