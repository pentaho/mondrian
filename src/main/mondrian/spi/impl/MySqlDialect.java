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
 * Implementation of {@link mondrian.spi.Dialect} for the MySQL database.
 *
 * @author jhyde
 * @version $Id$
 * @since Nov 23, 2008
 */
public class MySqlDialect extends JdbcDialectImpl {

    public static final JdbcDialectFactory FACTORY =
        new JdbcDialectFactory(
            MySqlDialect.class,
            DatabaseProduct.MYSQL)
        {
            protected boolean acceptsConnection(Connection connection) {
                try {
                    // Infobright looks a lot like MySQL. If this is an
                    // Infobright connection, yield to the Infobright dialect.
                    return super.acceptsConnection(connection)
                        && !isInfobright(connection.getMetaData());
                } catch (SQLException e) {
                    throw Util.newError(
                        e, "Error while instantiating dialect");
                }
            }
        };

    /**
     * Creates a MySqlDialect.
     *
     * @param connection Connection
     */
    public MySqlDialect(Connection connection) throws SQLException {
        super(connection);
    }

    /**
     * Detects whether this database is Infobright.
     *
     * <p>Infobright uses the MySQL driver and appears to be a MySQL instance.
     * The only difference is the presence of the BRIGHTHOUSE engine.
     *
     * @param databaseMetaData Database metadata
     *
     * @return Whether this is Infobright
     */
    public static boolean isInfobright(
        DatabaseMetaData databaseMetaData)
    {
        Statement statement = null;
        try {
            String productVersion =
                databaseMetaData.getDatabaseProductVersion();
            if (productVersion.compareTo("5.1") >= 0) {
                statement = databaseMetaData.getConnection().createStatement();
                final ResultSet resultSet =
                    statement.executeQuery(
                        "select * from INFORMATION_SCHEMA.engines "
                            + "where ENGINE = 'BRIGHTHOUSE'");
                if (resultSet.next()) {
                    return true;
                }
            }
            return false;
        } catch (SQLException e) {
            throw Util.newInternal(
                e,
                "while running query to detect Brighthouse engine");
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

    @Override
    protected String deduceProductName(DatabaseMetaData databaseMetaData) {
        final String productName = super.deduceProductName(databaseMetaData);
        if (isInfobright(databaseMetaData)) {
            return "MySQL (Infobright)";
        }
        return productName;
    }

    protected String deduceIdentifierQuoteString(
        DatabaseMetaData databaseMetaData)
    {
        String quoteIdentifierString =
            super.deduceIdentifierQuoteString(databaseMetaData);

        if (quoteIdentifierString == null) {
            // mm.mysql.2.0.4 driver lies. We know better.
            quoteIdentifierString = "`";
        }
        return quoteIdentifierString;
    }

    protected boolean deduceSupportsSelectNotInGroupBy(Connection connection)
        throws SQLException
    {
        boolean supported = false;
        String sqlmode = getCurrentSqlMode(connection);
        if (sqlmode == null) {
            supported = true;
        } else {
            if (!sqlmode.contains("ONLY_FULL_GROUP_BY")) {
                supported = true;
            }
        }
        return supported;
    }

    private String getCurrentSqlMode(Connection connection)
        throws SQLException
    {
        return getSqlMode(connection, true);
    }

    private String getSqlMode(Connection connection, boolean session)
        throws SQLException
    {
        String scope = session ? "SESSION" : "GLOBAL";
        String sqlmode = null;
        Statement s = connection.createStatement();
        if (s.execute("SELECT @@" + scope + ".sql_mode")) {
            ResultSet rs = s.getResultSet();
            rs.next();
            sqlmode = rs.getString(1);
        }
        s.close();
        return sqlmode;
    }

    public boolean requiresAliasForFromQuery() {
        return true;
    }

    public boolean allowsFromQuery() {
        // MySQL before 4.0 does not allow FROM
        // subqueries in the FROM clause.
        return productVersion.compareTo("4.") >= 0;
    }

    public boolean allowsCompoundCountDistinct() {
        return true;
    }

    public String generateInline(
        List<String> columnNames,
        List<String> columnTypes,
        List<String[]> valueList)
    {
        return generateInlineGeneric(
            columnNames, columnTypes, valueList, null, false);
    }

    public NullCollation getNullCollation() {
        return NullCollation.NEGINF;
    }

    public String generateOrderItem(
        String expr,
        boolean nullable,
        boolean ascending)
    {
        if (nullable && ascending) {
            return "ISNULL(" + expr + "), " + expr + " ASC";
        } else {
            return super.generateOrderItem(expr, nullable, ascending);
        }
    }

    public boolean requiresOrderByAlias() {
        return true;
    }

    public boolean supportsMultiValueInExpr() {
        return true;
    }
}

// End MySqlDialect.java
