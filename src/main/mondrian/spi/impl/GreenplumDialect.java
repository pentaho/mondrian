/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2008-2009 Millersoft
// Copyright (C) 2011-2013 Pentaho and others
// All Rights Reserved.
*/

package mondrian.spi.impl;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Implementation of {@link mondrian.spi.Dialect} for the GreenplumSQL database.
 *
 * @author Millersoft
 * @since Dec 23, 2009
 */
public class GreenplumDialect extends PostgreSqlDialect {

    /**
     * Creates a GreenplumDialect.
     *
     * @param connection Connection
     */
    public GreenplumDialect(Connection connection) throws SQLException {
        super(connection);
    }


    public static final JdbcDialectFactory FACTORY =
        new JdbcDialectFactory(
            GreenplumDialect.class,
            // While we're choosing dialects, this still looks like a Postgres
            // connection.
            DatabaseProduct.POSTGRESQL)
        {
            protected boolean acceptsConnection(Connection connection) {
                return super.acceptsConnection(connection)
                   && isDatabase(DatabaseProduct.GREENPLUM, connection);
            }
        };

    public boolean supportsGroupingSets() {
        return true;
    }

    public boolean requiresGroupByAlias() {
        return true;
    }

    public boolean requiresAliasForFromQuery() {
        return false;
    }

    public boolean allowsCountDistinct() {
        return true;
    }

    public boolean allowsFromQuery() {
        return false;
    }

    public DatabaseProduct getDatabaseProduct() {
        return DatabaseProduct.GREENPLUM;
    }

    public String generateCountExpression(String exp) {
        return caseWhenElse(exp + " ISNULL", "'0'", "TEXT(" + exp + ")");
    }

    public boolean allowsRegularExpressionInWhereClause() {
        // Support for regexp was added in GP 3.2+
        if (productVersion.compareTo("3.2") >= 0) {
            return true;
        } else {
            return false;
        }
    }
}

// End GreenplumDialect.java
