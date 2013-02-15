/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2011-2011 Pentaho and others
// All Rights Reserved.
*/
package mondrian.spi.impl;

import mondrian.olap.Util;

import org.apache.log4j.Logger;

import java.sql.*;
import java.sql.Date;
import java.util.*;

/**
 * Implementation of {@link mondrian.spi.Dialect} for the Hive database.
 *
 * @author Hongwei Fu
 * @since Jan 10, 2011
 */
public class HiveDialect extends JdbcDialectImpl {
    private static final Logger LOGGER =
            Logger.getLogger(HiveDialect.class);

    private static final int MAX_COLUMN_NAME_LENGTH = 128;

    public static final JdbcDialectFactory FACTORY =
        new JdbcDialectFactory(
            HiveDialect.class,
            DatabaseProduct.HIVE)
        {
            protected boolean acceptsConnection(Connection connection) {
                try {
                    return super.acceptsConnection(connection)
                        && !isImpala(connection.getMetaData());
                } catch (SQLException e) {
                    throw Util.newError(
                        e, "Error while instantiating dialect");
                }
            }
        };

    /**
     * Creates a HiveDialect.
     *
     * @param connection Connection
     *
     * @throws SQLException on error
     */
    public HiveDialect(Connection connection) throws SQLException {
        super(connection);
    }

    /**
     * Detects whether the database is the desired product
     *
     * @param  databaseProduct the Product to detect for
     * @param databaseMetaData Database metadata
     *
     * @return Whether this is the requested database product
     */
    protected static boolean isDatabase(
        DatabaseProduct databaseProduct,
        DatabaseMetaData databaseMetaData)
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
     * Detects whether this database is Impala.
     *
     * @param databaseMetaData Database metadata
     *
     * @return Whether this is a Impala database
     */
    public static boolean isImpala(
        DatabaseMetaData databaseMetaData)
    {
        return isDatabase(DatabaseProduct.IMPALA, databaseMetaData);
    }

    protected String deduceIdentifierQuoteString(
        DatabaseMetaData databaseMetaData)
    {
        try {
            final String quoteIdentifierString =
                databaseMetaData.getIdentifierQuoteString();
            return "".equals(quoteIdentifierString)
                // quoting not supported
                ? null
                : quoteIdentifierString;
        } catch (SQLException e) {
            // Not supported by HiveDatabaseMetaData; do nothing if catch an
            // Exception
            return "`";
        }
    }

    protected Set<List<Integer>> deduceSupportedResultSetStyles(
        DatabaseMetaData databaseMetaData)
    {
        // Hive don't support this, so just return an empty set.
        return Collections.emptySet();
    }

    protected boolean deduceReadOnly(DatabaseMetaData databaseMetaData) {
        try {
            return databaseMetaData.isReadOnly();
        } catch (SQLException e) {
            // Hive is read only (as of release 0.7)
            return true;
        }
    }

    protected int deduceMaxColumnNameLength(DatabaseMetaData databaseMetaData) {
        try {
            return databaseMetaData.getMaxColumnNameLength();
        } catch (SQLException e) {
            return MAX_COLUMN_NAME_LENGTH;
        }
    }

    public boolean allowsCompoundCountDistinct() {
        return true;
    }

    public boolean requiresAliasForFromQuery() {
        return true;
    }

    public boolean requiresOrderByAlias() {
        return true;
    }

    public boolean requiresUnionOrderByExprToBeInSelectClause() {
        return false;
    }

    public String generateInline(
        List<String> columnNames,
        List<String> columnTypes,
        List<String[]> valueList)
    {
        return "select * from ("
            + generateInlineGeneric(
                columnNames, columnTypes, valueList, " from dual", false)
            + ") x limit " + valueList.size();
    }

    protected void quoteDateLiteral(
        StringBuilder buf,
        String value,
        Date date)
    {
        // Hive doesn't support Date type; treat date as a string '2008-01-23'
        Util.singleQuoteString(value, buf);
    }

    @Override
    protected String generateOrderByNulls(
        String expr,
        boolean ascending,
        boolean collateNullsLast)
    {
        // In Hive, Null values are worth negative infinity.
        if (collateNullsLast) {
            if (ascending) {
                return "ISNULL(" + expr + ") ASC, " + expr + " ASC";
            } else {
                return expr + " DESC";
            }
        } else {
            if (ascending) {
                return expr + " ASC";
            } else {
                return "ISNULL(" + expr + ") DESC, " + expr + " DESC";
            }
        }
    }

    public boolean allowsAs() {
        return false;
    }

    @Override
    public boolean allowsJoinOn() {
        return true;
    }
}

// End HiveDialect.java
