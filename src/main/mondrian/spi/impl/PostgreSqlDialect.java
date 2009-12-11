/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2008-2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.spi.impl;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Implementation of {@link mondrian.spi.Dialect} for the PostgreSQL database.
 *
 * @author jhyde
 * @version $Id$
 * @since Nov 23, 2008
 */
public class PostgreSqlDialect extends JdbcDialectImpl {

    public static final JdbcDialectFactory FACTORY =
        new JdbcDialectFactory(
            PostgreSqlDialect.class,
            DatabaseProduct.POSTGRESQL);

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
}

// End PostgreSqlDialect.java
