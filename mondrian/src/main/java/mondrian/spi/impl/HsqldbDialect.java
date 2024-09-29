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


package mondrian.spi.impl;

import mondrian.olap.Util;

import java.sql.*;
import java.util.List;

/**
 * Implementation of {@link mondrian.spi.Dialect} for the Hsqldb database.
 *
 * @author wgorman
 * @since Aug 20, 2009
 */
public class HsqldbDialect extends JdbcDialectImpl {

    public static final JdbcDialectFactory FACTORY =
        new JdbcDialectFactory(
            HsqldbDialect.class,
            DatabaseProduct.HSQLDB);

    /**
     * Creates a FirebirdDialect.
     *
     * @param connection Connection
     */
    public HsqldbDialect(Connection connection) throws SQLException {
        super(connection);
    }

    protected void quoteDateLiteral(
        StringBuilder buf,
        String value,
        Date date)
    {
        // Hsqldb accepts '2008-01-23' but not SQL:2003 format.
        Util.singleQuoteString(value, buf);
    }

    public String generateInline(
        List<String> columnNames,
        List<String> columnTypes,
        List<String[]> valueList)
    {
        // Fall back to using the FoodMart 'days' table, because
        // HQLDB's SQL has no way to generate values not from a table.
        // (Same as Access.)
        return generateInlineGeneric(
            columnNames, columnTypes, valueList,
            " from \"days\" where \"day\" = 1", false);
    }
}

// End HsqldbDialect.java
