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
import org.apache.log4j.Logger;

import java.sql.*;

/**
 * Implementation of {@link mondrian.spi.Dialect} for the PostgreSQL database.
 *
 * @author jhyde
 * @version $Id$
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
                        && !isGreenplum(connection.getMetaData());
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
    protected String generateOrderByNullsLast(String expr, boolean ascending) {
        // Support for "ORDER BY ... NULLS LAST" was introduced in Postgres 8.3.
        if (productVersion.compareTo("8.3") >= 0) {
            return generateOrderByNullsLastAnsi(expr, ascending);
        } else {
            return super.generateOrderByNullsLast(expr, ascending);
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
        try {
            statement = databaseMetaData.getConnection().createStatement();
            final ResultSet resultSet =
                statement.executeQuery("select version()");

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
}

// End PostgreSqlDialect.java
