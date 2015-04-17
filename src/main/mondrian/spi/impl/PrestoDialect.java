/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2015-2015 Pentaho Corporation..  All rights reserved.
*/

package mondrian.spi.impl;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

import mondrian.rolap.SqlStatement;
import mondrian.rolap.SqlStatement.Type;

public class PrestoDialect extends JdbcDialectImpl {

    public static final JdbcDialectFactory FACTORY =
        new JdbcDialectFactory(
            PrestoDialect.class,
            DatabaseProduct.PRESTO);

    public PrestoDialect(Connection connection) throws SQLException {
        super(connection);
    }

    @Override
    public Type getType(
        ResultSetMetaData metaData,
        int columnIndex)
        throws SQLException
    {
        // Presto's driver doesn't like getInt on a boolean.
        // Treat it as an Object.
        switch (metaData.getColumnType(columnIndex + 1)) {
        case Types.BOOLEAN:
            return SqlStatement.Type.OBJECT;
        default:
            return super.getType(metaData, columnIndex);
        }
    }
}

// End PrestoDialect.java
