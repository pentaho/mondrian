/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2008-2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.spi.impl;

import mondrian.olap.Util;

import java.util.List;
import java.sql.*;

/**
 * Implementation of {@link mondrian.spi.Dialect} for the Apache Derby database.
 *
 * @author jhyde
 * @version $Id$
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

    public String generateOrderItem(
        String expr,
        boolean nullable,
        boolean ascending)
    {
        if (nullable && !ascending) {
            return "CASE WHEN " + expr + " IS NULL THEN 1 ELSE 0 END, "
               + expr + " DESC";
        } else {
            return super.generateOrderItem(expr, nullable, ascending);
        }
    }
}

// End DerbyDialect.java
