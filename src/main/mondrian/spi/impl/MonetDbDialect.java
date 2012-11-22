/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2008-2010 Pentaho
// All Rights Reserved.
*/
package mondrian.spi.impl;

import mondrian.olap.Util;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Implementation of {@link mondrian.spi.Dialect} for the MonetDB database.
 *
 * @author pstoellberger
 * @since Nov 10, 2012
 */
public class MonetDbDialect extends JdbcDialectImpl {

    public static final JdbcDialectFactory FACTORY =
        new JdbcDialectFactory(
            MonetDbDialect.class,
            DatabaseProduct.MONETDB);

    /**
     * Creates a MonetDbDialect.
     *
     * @param connection Connection
     *
     * @throws java.sql.SQLException on error
     */
    public MonetDbDialect(Connection connection) throws SQLException {
        super(connection);
    }

    @Override
    public boolean allowsMultipleDistinctSqlMeasures() {
        return false;
    }

    @Override
    public boolean allowsCountDistinct() {
        return false;
    }

    @Override
    public boolean requiresAliasForFromQuery() {
        return true;
    }

    @Override
    public boolean allowsCompoundCountDistinct() {
        return false;
    }

    @Override
    public boolean supportsGroupByExpressions() {
        return false;
    }

    @Override
    public void quoteStringLiteral(StringBuilder buf, String s) {
        // Go beyond Util.singleQuoteString; also quote backslash, like MySQL.
        buf.append('\'');
        String s0 = Util.replace(s, "'", "''");
        String s1 = Util.replace(s0, "\\", "\\\\");
        buf.append(s1);
        buf.append('\'');
    }
}

// End MonetDbDialect.java
