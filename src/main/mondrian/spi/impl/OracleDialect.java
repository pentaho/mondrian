/*
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2008-2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.spi.impl;

import java.util.List;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Implementation of {@link mondrian.spi.Dialect} for the Oracle database.
 *
 * @author jhyde
 * @version $Id$
 * @since Nov 23, 2008
 */
public class OracleDialect extends JdbcDialectImpl {

    public static final JdbcDialectFactory FACTORY =
        new JdbcDialectFactory(
            OracleDialect.class,
            DatabaseProduct.ORACLE);

    /**
     * Creates an OracleDialect.
     *
     * @param connection Connection
     */
    public OracleDialect(Connection connection) throws SQLException {
        super(connection);
    }

    public boolean allowsAs() {
        return false;
    }

    public String generateInline(
        List<String> columnNames,
        List<String> columnTypes,
        List<String[]> valueList)
    {
        return generateInlineGeneric(
            columnNames, columnTypes, valueList,
            " from dual", false);
    }

    public boolean supportsGroupingSets() {
        return true;
    }
}

// End OracleDialect.java
