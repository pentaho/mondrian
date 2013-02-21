/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2008-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.spi.impl;

import mondrian.olap.Util;
import mondrian.rolap.SqlStatement;

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
    public static final JdbcDialectFactory FACTORY =
        new JdbcDialectFactory(
            PostgreSqlDialect.class,
            DatabaseProduct.POSTGRESQL)
        {
            protected boolean acceptsConnection(Connection connection) {
                try {
                    // Other databases use the Postgres JDBC driver
                    // Yield to the sub-dialects if identified.
                    return super.acceptsConnection(connection)
                        && !isGreenplum(connection.getMetaData())
                        && !isNetezza(connection.getMetaData())
                        && !isRedshift(connection.getMetaData());
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
     * Detects whether the database is the desired product
     *
     * @param databaseProduct the Product to detect for
     * @param databaseMetaData Database metadata
     *
     * @return Whether this is the requested database product
     */
    protected static boolean isDatabase(
        DatabaseProduct databaseProduct, DatabaseMetaData databaseMetaData)
    {
        Statement statement = null;
        ResultSet resultSet = null;

        String dbProduct = databaseProduct.name().toLowerCase();

        try {
            // Quick and dirty check first.
            if (databaseMetaData.getDatabaseProductName()
                .toLowerCase().contains(dbProduct))
            {
                LOGGER.info("Using " + databaseProduct.name() + " dialect");
                return true;
            }

            // Let's try using version().
            statement = databaseMetaData.getConnection().createStatement();
            resultSet = statement.executeQuery("select version()");
            if (resultSet.next()) {
                String version = resultSet.getString(1);
                LOGGER.info("Version=" + version);
                if (version != null) {
                    if (version.toLowerCase().contains(dbProduct)) {
                        LOGGER.info(
                            "Using " + databaseProduct.name() + " dialect");
                        return true;
                    }
                }
            }
            LOGGER.info("NOT Using " + databaseProduct.name() + " dialect");
            return false;
        } catch (SQLException e) {
            throw Util.newInternal(
                e,
                "while running query to detect Postgresql derivative database");
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
     * Detects whether this database is Netezza.
     *
     * @param databaseMetaData Database metadata
     *
     * @return Whether this is a Netezza database
     */
    public static boolean isNetezza(DatabaseMetaData databaseMetaData) {
        return isDatabase(DatabaseProduct.NETEZZA, databaseMetaData);
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
    public static boolean isGreenplum(DatabaseMetaData databaseMetaData) {
        try {
            // Mock connection used to create dialects during testing does not
            // support executing statements.
            final String driverName = databaseMetaData.getDriverName();
            if (driverName.startsWith("Mondrian fake dialect")) {
                return driverName.equals("Mondrian fake dialect for Greenplum");
            }
        } catch (SQLException e) {
            LOGGER.error(
                "Failed to access databaseMetaData for Mock object test");
        }

        return isDatabase(DatabaseProduct.GREENPLUM, databaseMetaData);
    }

    /**
     * Detects whether this database is Redshift.
     *
     * <p>Redshift uses the Postgres driver and appears to be a Postgres
     * instance. The key difference is the presence of 'redshift' in 'select
     * version()'.
     *
     * @param databaseMetaData Database metadata
     *
     * @return Whether this is a Redshift database
     */
    public static boolean isRedshift(DatabaseMetaData databaseMetaData) {
        return isDatabase(DatabaseProduct.REDSHIFT, databaseMetaData);
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

    @Override
    public SqlStatement.Type getType(
        ResultSetMetaData metaData, int columnIndex)
        throws SQLException
    {
        final int precision = metaData.getPrecision(columnIndex + 1);
        final int scale = metaData.getScale(columnIndex + 1);
        final int columnType = metaData.getColumnType(columnIndex + 1);
        final String columnName = metaData.getColumnName(columnIndex + 1);

        //  TODO - Do we need the check for "m"??
        if (columnType == Types.NUMERIC
            && scale == 0 && precision == 0
            && columnName.startsWith("m"))
        {
            // In Greenplum  NUMBER/NUMERIC w/ no precision or
            // scale means floating point.
            logTypeInfo(metaData, columnIndex, SqlStatement.Type.OBJECT);
            return SqlStatement.Type.OBJECT; // TODO - can this be DOUBLE?
        }
        return super.getType(metaData, columnIndex);
    }

}

// End PostgreSqlDialect.java
