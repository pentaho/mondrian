/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2013-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.spi.impl;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * Dialect for Cloudera's Impala DB.
 *
 * @author cboyden
 * @since 2/11/13
 */
public class ImpalaDialect extends HiveDialect {
    /**
     * Creates an ImpalaDialect.
     *
     * @param connection Connection
     * @throws java.sql.SQLException on error
     */
    public ImpalaDialect(Connection connection) throws SQLException {
        super(connection);
    }

    public static final JdbcDialectFactory FACTORY =
        new JdbcDialectFactory(
            ImpalaDialect.class,
            DatabaseProduct.HIVE)
        {
            protected boolean acceptsConnection(Connection connection) {
                return super.acceptsConnection(connection)
                    && isDatabase(DatabaseProduct.IMPALA, connection);
            }
        };

    @Override
    public DatabaseProduct getDatabaseProduct() {
        return DatabaseProduct.IMPALA;
    }

    @Override
    protected String generateOrderByNulls(
        String expr,
        boolean ascending,
        boolean collateNullsLast)
    {
        if (ascending) {
            return expr + " ASC";
        } else {
            return expr + " DESC";
        }
    }


    @Override
    public String generateOrderItem(
        String expr,
        boolean nullable,
        boolean ascending,
        boolean collateNullsLast)
    {
        String ret = null;

        if (nullable && collateNullsLast) {
            ret = "CASE WHEN " + expr + " IS NULL THEN 1 ELSE 0 END, ";
        } else {
            ret = "CASE WHEN " + expr + " IS NULL THEN 0 ELSE 1 END, ";
        }

        if (ascending) {
            ret += expr + " ASC";
        } else {
            ret += expr + " DESC";
        }

        return ret;
    }

    @Override
    public boolean allowsMultipleCountDistinct() {
        return false;
    }

    @Override
    public boolean allowsCompoundCountDistinct() {
        return true;
    }

    @Override
    public boolean requiresOrderByAlias() {
        return false;
    }

    @Override
    public boolean requiresAliasForFromQuery() {
        return true;
    }

    @Override
    public boolean supportsGroupByExpressions() {
        return false;
    }

    @Override
    public boolean allowsSelectNotInGroupBy() {
        return false;
    }

    @Override
    public String generateInline(
        List<String> columnNames,
        List<String> columnTypes,
        List<String[]> valueList)
    {
        // TODO: fix this, when Impala has the necessary features. See bug
        // http://jira.pentaho.com/browse/MONDRIAN-1512.
        return "";
    }

    public boolean allowsJoinOn() {
        return false;
    }

    @Override
    public void quoteStringLiteral(
        StringBuilder buf,
        String value)
    {
        // REVIEW: Are Impala's rules for string literals so very different
        // from the standard? Or from Hive's?
        String quote = "\'";
        String s0 = value;

        if (value.contains("'")) {
            quote = "\"";
        }

        if (value.contains(quote)) {
            s0 = value.replaceAll(quote, "\\\\" + quote);
        }

        buf.append(quote);

        buf.append(s0);

        buf.append(quote);
    }
}
// End ImpalaDialect.java
