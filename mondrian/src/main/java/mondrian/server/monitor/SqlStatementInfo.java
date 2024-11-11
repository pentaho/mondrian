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

/**
 * Information about an SQL statement submitted by Mondrian to a back-end
 * database.
 *
 * @see StatementInfo
 */
public class SqlStatementInfo extends Info {
    public final long sqlStatementId;
    public final String sql;

    public SqlStatementInfo(
        String stack,
        long sqlStatementId,
        String sql)
    {
        super(stack);
        this.sqlStatementId = sqlStatementId;
        this.sql = sql;
    }

    public long getSqlStatementId() {
        return sqlStatementId;
    }

    public String getSql() {
        return sql;
    }

}

// End SqlStatementInfo.java
