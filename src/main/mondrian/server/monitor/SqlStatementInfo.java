/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
// Copyright (C) 2011-2011 Julian Hyde
// All Rights Reserved.
*/
package mondrian.server.monitor;

/**
 * Information about an SQL statement submitted by Mondrian to a back-end
 * database.
 *
 * @see StatementInfo
 *
 * @version $Id$
 */
public class SqlStatementInfo extends Info {
    public final long sqlStatementId;

    public SqlStatementInfo(long sqlStatementId) {
        this.sqlStatementId = sqlStatementId;
    }
}

// End SqlStatementInfo.java
