/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2009 Pentaho
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.spi.impl;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Implementation of {@link mondrian.spi.Dialect} for the Hsqldb database.
 *
 * @author wgorman
 * @version $Id
 * @since Aug 20, 2009
 */
public class HsqldbDialect extends JdbcDialectImpl {

    public static final JdbcDialectFactory FACTORY =
        new JdbcDialectFactory(
                HsqldbDialect.class,
            DatabaseProduct.HSQLDB);

    /**
     * Creates a FirebirdDialect.
     *
     * @param connection Connection
     */
    public HsqldbDialect(Connection connection) throws SQLException {
        super(connection);
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

// End HsqldbDialect.java
