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

import mondrian.olap.Util;

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
                try {
                    // Greenplum looks a lot like Postgres. If this is a
                    // Greenplum connection, yield to the Greenplum dialect.
                    return super.acceptsConnection(connection)
                        && !isGreenplum(connection.getMetaData())
                        && !isNetezza(connection.getMetaData());
                } catch (SQLException e) {
                    throw Util.newError(
                        e, "Error while instantiating dialect");
                }
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

    /**
     * Detects whether this database is Netezza.
     *
     * @param databaseMetaData Database metadata
     *
     * @return Whether this is a Netezza database
     */
    public static boolean isNetezza(
        DatabaseMetaData databaseMetaData)
    {
        Statement statement = null;
        ResultSet resultSet = null;

        try {
            // Quick and dirty check first.
            if (databaseMetaData.getDatabaseProductName()
                .toLowerCase().contains("netezza"))
            {
                LOGGER.info("Using NETEZZA dialect");
                return true;
            }

            // Let's try using version().
            statement = databaseMetaData.getConnection().createStatement();
            resultSet = statement.executeQuery("select version()");
            if (resultSet.next()) {
                String version = resultSet.getString(1);
                LOGGER.info("Version=" + version);
                if (version != null
                    && version.toLowerCase().indexOf("netezza") != -1)
                {
                    LOGGER.info("Using NETEZZA dialect");
                    return true;
                }
            }
            LOGGER.info("NOT Using NETEZZA dialect");
            return false;
        } catch (SQLException e) {
            throw Util.newInternal(
                e,
                "while running query to detect Netezza database");
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    // ignore
                }
            }
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    // ignore
                }
            }
        }
    }

    /**
     * Detects whether this database is Greenplum.
     *
     * <p>Greenplum uses the Postgres driver and appears to be a Postgres
     * instance. The key difference is the presence of 'greenplum' in 'select
     * version()'.
     *
     * @param databaseMetaData Database metadata
     *
     * @return Whether this is a Greenplum database
     */
    public static boolean isGreenplum(
        DatabaseMetaData databaseMetaData)
    {
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            statement = databaseMetaData.getConnection().createStatement();
            resultSet = statement.executeQuery("select version()");
            if (resultSet.next()) {
                String version = resultSet.getString(1);
                LOGGER.info("Version=" + version);
                if (version != null
                    && version.toLowerCase().indexOf("greenplum") != -1)
                {
                    LOGGER.info("Using GREENPLUM dialect");
                    return true;
                }
            }
            LOGGER.info("Using POSTGRES dialect");
            return false;
        } catch (SQLException e) {
            throw Util.newInternal(
                e,
                "while running query to detect Greenplum database");
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    // ignore
                }
            }
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    // ignore
                }
            }
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
        sb.append(source);
        sb.append(" ~ ");
        quoteStringLiteral(sb, javaRegex);
        return sb.toString();
    }
}

// End PostgreSqlDialect.java
