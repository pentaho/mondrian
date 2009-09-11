/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.spi.impl;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Implementation of {@link mondrian.spi.Dialect} for the Vertica database.
 *
 * @author Pedro Alves
 * @version $Id$
 * @since Sept 11, 2009
 */
public class VerticaDialect extends JdbcDialectImpl {

    public static final JdbcDialectFactory FACTORY =
        new JdbcDialectFactory(
            VerticaDialect.class,
            DatabaseProduct.VERTICA);

    /**
     * Creates a VerticaDialect.
     *
     * @param connection Connection
     */
    public VerticaDialect(Connection connection) throws SQLException {
        super(connection);
    }

    public boolean requiresAliasForFromQuery() {
        return true;
    }

    @Override
    public String generateOrderItem(
            String expr,
            boolean nullable,
            boolean ascending)
    {
        if (ascending) {
            return expr + " ASC";
        } else {
            return expr + " DESC";
        }
    }

}

// End VerticaDialect.java
