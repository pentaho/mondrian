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
 * Implementation of {@link mondrian.spi.Dialect} for the Apache Derby database.
 *
 * @author jhyde
 * @since Nov 23, 2008
 */
public class DerbyDialect extends JdbcDialectImpl {

    public static final JdbcDialectFactory FACTORY =
        new JdbcDialectFactory(
            DerbyDialect.class,
            DatabaseProduct.DERBY);

    /**
     * Creates a DerbyDialect.
     *
     * @param connection Connection
     */
    public DerbyDialect(Connection connection) throws SQLException {
        super(connection);
    }

    protected void quoteDateLiteral(
        StringBuilder buf,
        String value,
        Date date)
    {
        // Derby accepts DATE('2008-01-23') but not SQL:2003 format.
        buf.append("DATE(");
        Util.singleQuoteString(value, buf);
        buf.append(")");
    }

    public boolean requiresAliasForFromQuery() {
        return true;
    }

    public boolean allowsMultipleCountDistinct() {
        // Derby allows at most one distinct-count per query.
        return false;
    }

    public String generateInline(
        List<String> columnNames,
        List<String> columnTypes,
        List<String[]> valueList)
    {
        return generateInlineForAnsi(
            "t", columnNames, columnTypes, valueList, true);
    }

    public boolean supportsGroupByExpressions() {
        return false;
    }
}

// End DerbyDialect.java
