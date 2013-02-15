/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2013 Pentaho
// All Rights Reserved.
*/
package mondrian.spi.impl;

import mondrian.olap.Util;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * User: cboyden
 * Date: 2/11/13
 */
public class ImpalaDialect extends HiveDialect {
    /**
     * Creates a HiveDialect.
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
                try {
                    return super.acceptsConnection(connection)
                            && isImpala(connection.getMetaData());
                } catch (SQLException e) {
                    throw Util.newError(
                            e, "Error while instantiating dialect");
                }
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

        if(ascending) {
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
        return "select * from ("
                + generateInlineGeneric(
                columnNames, columnTypes, valueList, " from customer", false)
                + ") x limit " + valueList.size();
    }

    @Override
    public void quoteStringLiteral(
            StringBuilder buf,
            String value)
    {
        String quote = "\'";
        String s0 = value;

        if(value.contains("'")) {
            quote = "\"";
        }

        if(value.contains(quote)) {
            s0 = value.replaceAll(quote, "\\\\" + quote);
        }

        buf.append(quote);

        buf.append(s0);

        buf.append(quote);
    }

}
