/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2008-2009 Millersoft
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.spi.impl;

import java.sql.Connection;
import java.sql.SQLException;

import mondrian.olap.Util;

/**
 * Implementation of {@link mondrian.spi.Dialect} for the GreenplumSQL database.
 *
 * @author Millersoft
 * @version $Id$
 * @since Dec 23, 2009
 */
public class GreenplumDialect extends PostgreSqlDialect {

    /**
     * Creates a GreenplumDialect.
     *
     * @param connection Connection
     */
    public GreenplumDialect(Connection connection) throws SQLException {
        super(connection);
    }


    public static final JdbcDialectFactory FACTORY =
        new JdbcDialectFactory(
            GreenplumDialect.class,
            // While we're choosing dialects, this still looks like a Postgres
            // connection.
            DatabaseProduct.POSTGRESQL)
        {
            protected boolean acceptsConnection(Connection connection) {
                try {
                    return super.acceptsConnection(connection)
                       && isGreenplum(connection.getMetaData());
                } catch (SQLException e) {
                    throw Util.newError(
                        e, "Error while instantiating dialect");
                }
            }
        };

    public boolean supportsGroupingSets() {
        return true;
    }

    public boolean allowsCountDistinct() {
        return false;
    }

    public boolean allowsFromQuery() {
        return true;
    }

    public DatabaseProduct getDatabaseProduct() {
        return DatabaseProduct.GREENPLUM;
    }

    public boolean allowsRegularExpressionInWhereClause() {
        // Support for regexp was added in GP 3.2+
        if (productVersion.compareTo("3.2") >= 0) {
            return true;
        } else {
            return false;
        }
    }

    public String generateRegularExpression(String source, String javaRegExp) {
        final StringBuilder sb = new StringBuilder();
        sb.append(source);
        sb.append(" ~ ");
        quoteStringLiteral(sb, javaRegExp);
        return sb.toString();
    }
}

// End GreenplumDialect.java
