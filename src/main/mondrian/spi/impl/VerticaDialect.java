/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2008-2014 Pentaho
// All Rights Reserved.
*/
package mondrian.spi.impl;

import mondrian.rolap.SqlStatement;

import java.sql.*;
import java.util.List;

/**
 * Implementation of {@link mondrian.spi.Dialect} for the Vertica database.
 *
 * @author Pedro Alves
 * @since Sept 11, 2009
 */
public class VerticaDialect extends JdbcDialectImpl {

    public static final JdbcDialectFactory FACTORY =
        new JdbcDialectFactory(
            VerticaDialect.class,
            DatabaseProduct.VERTICA);

    /**
     * Creates a VerticaDialect.
     *
     * @param connection Connection
     */
    public VerticaDialect(Connection connection) throws SQLException {
        super(connection);
    }

    public boolean requiresAliasForFromQuery() {
        return true;
    }

    public boolean allowsFromQuery() {
        return true;
    }

    @Override
    public DatabaseProduct getDatabaseProduct() {
        return DatabaseProduct.VERTICA;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) {
        return false;
    }

    public String generateInline(
        List<String> columnNames,
        List<String> columnTypes,
        List<String[]> valueList)
    {
        return generateInlineGeneric(
            columnNames, columnTypes, valueList, null, false);
    }

    public SqlStatement.Type getType(
        ResultSetMetaData metaData, int columnIndex)
        throws SQLException
    {
        // BIGINT->LONG should be the general rule, not just for Vertica,
        // see MONDRIAN-1890
        return metaData.getColumnType(columnIndex + 1) == Types.BIGINT
            ? SqlStatement.Type.LONG : super.getType(metaData, columnIndex);
    }

}

// End VerticaDialect.java
