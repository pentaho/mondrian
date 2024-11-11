/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


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
