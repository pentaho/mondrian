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
 * Event created just after Mondrian has executed a SQL statement.
 */
public class SqlStatementExecuteEvent extends SqlStatementEvent {
    public final long executeNanos;

    /**
     * Creates a SqlStatementExecuteEvent.
     *
     * @param timestamp Timestamp
     * @param statementId Statement id
     * @param locus Locus of event
     * @param sql SQL
     * @param purpose Why Mondrian is executing this statement
     * @param executeNanos Execution time
     */
    public SqlStatementExecuteEvent(
        long timestamp,
        long statementId,
        Locus locus,
        String sql,
        Purpose purpose,
        long executeNanos)
    {
        super(timestamp, statementId, locus, sql, purpose);
        this.executeNanos = executeNanos;
    }

    public String toString() {
        return "SqlStatementExecuteEvent(" + sqlStatementId + ")";
    }

    public <T> T accept(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}

// End SqlStatementExecuteEvent.java
