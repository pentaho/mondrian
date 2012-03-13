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

import mondrian.server.Locus;

/**
 * Event created just before Mondrian starts to execute a SQL statement.
 */
public class SqlStatementStartEvent extends SqlStatementEvent {
    public final int cellRequestCount;

    /**
     * Creates a SqlStatementStartEvent.
     *
     * @param timestamp Timestamp
     * @param sqlStatementId SQL Statement id
     * @param locus Locus of event
     * @param sql SQL
     * @param purpose Why Mondrian is executing this statement
     * @param cellRequestCount Number of missed cells that led to this request
     */
    public SqlStatementStartEvent(
        long timestamp,
        long sqlStatementId,
        Locus locus,
        String sql,
        Purpose purpose,
        int cellRequestCount)
    {
        super(timestamp, sqlStatementId, locus, sql, purpose);
        this.cellRequestCount = cellRequestCount;
    }

    public String toString() {
        return "SqlStatementStartEvent(" + sqlStatementId + ")";
    }

    public <T> T accept(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}

// End SqlStatementStartEvent.java
