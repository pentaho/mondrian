/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2008-2011 Pentaho
// All Rights Reserved.
*/
package mondrian.spi.impl;

import org.apache.log4j.Logger;

import java.sql.*;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Implementation of {@link mondrian.spi.Dialect} for the PostgreSQL database.
 *
 * @author jhyde
 * @since Nov 23, 2008
 */
public class PostgreSqlDialect extends JdbcDialectImpl {
    private static final Logger LOGGER =
        Logger.getLogger(PostgreSqlDialect.class);

    public static final JdbcDialectFactory FACTORY =
        new JdbcDialectFactory(
            PostgreSqlDialect.class,
            DatabaseProduct.POSTGRESQL)
        {
            protected boolean acceptsConnection(Connection connection) {
                // Greenplum looks a lot like Postgres. If this is a
                // Greenplum connection, yield to the Greenplum dialect.
                return super.acceptsConnection(connection)
                    && !isDatabase(DatabaseProduct.GREENPLUM, connection)
                    && !isDatabase(DatabaseProduct.NETEZZA, connection)
                    && !isDatabase(DatabaseProduct.REDSHIFT, connection);
            }
        };

    /**
     * Creates a PostgreSqlDialect.
     *
     * @param connection Connection
     */
    public PostgreSqlDialect(Connection connection) throws SQLException {
        super(connection);
    }

    public boolean requiresAliasForFromQuery() {
        return true;
    }

    @Override
    protected String generateOrderByNulls(
        String expr,
        boolean ascending,
        boolean collateNullsLast)
    {
        // Support for "ORDER BY ... NULLS LAST" was introduced in Postgres 8.3.
        if (productVersion.compareTo("8.3") >= 0) {
            return
                generateOrderByNullsAnsi(
                    expr,
                    ascending,
                    collateNullsLast);
        } else {
            return
                super.generateOrderByNulls(
                    expr,
                    ascending,
                    collateNullsLast);
        }
    }

    public DatabaseProduct getDatabaseProduct() {
        return DatabaseProduct.POSTGRESQL;
    }

    @Override
    public boolean allowsRegularExpressionInWhereClause() {
        return true;
    }

    public String generateRegularExpression(String source, String javaRegex) {
        try {
            Pattern.compile(javaRegex);
        } catch (PatternSyntaxException e) {
            // Not a valid Java regex. Too risky to continue.
            return null;
        }
        javaRegex = javaRegex.replace("\\Q", "");
        javaRegex = javaRegex.replace("\\E", "");
        final StringBuilder sb = new StringBuilder();
        sb.append("cast(");
        sb.append(source);
        sb.append(" as text) ~ ");
        quoteStringLiteral(sb, javaRegex);
        return sb.toString();
    }
}

// End PostgreSqlDialect.java
