/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2009-2009 Pentaho and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.spi.impl;

import mondrian.olap.Util;

import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.util.List;

/**
 * Implementation of {@link mondrian.spi.Dialect} for the Hsqldb database.
 *
 * @author wgorman
 * @version $Id
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

    @Override
    public NullCollation getNullCollation() {
        return NullCollation.NEGINF;
    }

    @Override
    public String generateOrderItem(
        String expr,
        boolean nullable,
        boolean ascending)
    {
        if (nullable && ascending) {
            return "CASE WHEN " + expr + " IS NULL THEN 1 ELSE 0 END, "
               + expr + " ASC";
        } else {
            return super.generateOrderItem(expr, nullable, ascending);
        }
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
