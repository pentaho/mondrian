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
