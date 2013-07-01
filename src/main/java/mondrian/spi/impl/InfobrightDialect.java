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

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Implementation of {@link mondrian.spi.Dialect} for the Infobright database.
 *
 * @author jhyde
 * @since Nov 23, 2008
 */
public class InfobrightDialect extends MySqlDialect {

    public static final JdbcDialectFactory FACTORY =
        new JdbcDialectFactory(
            InfobrightDialect.class,
            // While we're choosing dialects, this still looks like a MySQL
            // connection.
            DatabaseProduct.MYSQL)
        {
            protected boolean acceptsConnection(Connection connection) {
                try {
                    return super.acceptsConnection(connection)
                        && isInfobright(connection.getMetaData());
                } catch (SQLException e) {
                    throw Util.newError(
                        e, "Error while instantiating dialect");
                }
            }
        };

    /**
     * Creates an InfobrightDialect.
     *
     * @param connection Connection
     */
    public InfobrightDialect(Connection connection) throws SQLException {
        super(connection);
    }

    public DatabaseProduct getDatabaseProduct() {
        return DatabaseProduct.INFOBRIGHT;
    }

    public boolean allowsCompoundCountDistinct() {
        return false;
    }

    @Override
    public String generateOrderItem(
        String expr,
        boolean nullable,
        boolean ascending,
        boolean collateNullsLast)
    {
        // Like MySQL, Infobright collates NULL values as negative-infinity
        // (first in ASC, last in DESC). But we can't generate ISNULL to
        // correct the NULL ordering, as we do for MySQL, because Infobright
        // does not support this function.
        if (ascending) {
            return expr + " ASC";
        } else {
            return expr + " DESC";
        }
    }

    public boolean supportsGroupByExpressions() {
        return false;
    }

    public boolean requiresGroupByAlias() {
        return true;
    }

    public boolean allowsOrderByAlias() {
        return false;
    }

    public boolean requiresOrderByAlias() {
        // Actually, Infobright doesn't ALLOW aliases to be used in the ORDER BY
        // clause, let alone REQUIRE them. Infobright doesn't allow expressions
        // in the ORDER BY clause, so returning true gives the right effect.
        return true;
    }

    public boolean supportsMultiValueInExpr() {
        // Infobright supports multi-value IN by falling through to MySQL,
        // which is very slow (see for example
        // PredicateFilterTest.testFilterAtSameLevel) so we pretend that it
        // does not.
        return false;
    }
}

// End InfobrightDialect.java
