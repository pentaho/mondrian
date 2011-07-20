/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2011-2011 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.spi.impl;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.SQLException;
import java.util.*;

import mondrian.olap.Util;

/**
 * Implementation of {@link mondrian.spi.Dialect} for the Hive database.
 *
 * @author Hongwei Fu
 * @version $Id: //open/mondrian/src/main/mondrian/spi/impl/HiveDialect.java
 * @since Jan 10, 2011
 */
public class HiveDialect extends JdbcDialectImpl {
    private static final int MAX_COLUMN_NAME_LENGTH = 128;

    public static final JdbcDialectFactory FACTORY =
        new JdbcDialectFactory(
            HiveDialect.class,
            DatabaseProduct.HIVE)
        {
            protected boolean acceptsConnection(Connection connection) {
                return super.acceptsConnection(connection);
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
