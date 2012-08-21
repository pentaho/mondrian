/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2008-2009 Jaspersoft
// All Rights Reserved.
*/
package mondrian.spi.impl;

import java.sql.Connection;
import java.sql.SQLException;

import mondrian.olap.Util;
import mondrian.spi.Dialect.DatabaseProduct;

/**
 * Implementation of {@link mondrian.spi.Dialect} for the Netezza database.
 *
 * @author swood
 * @since April 17, 2009
 */
public class NetezzaDialect extends PostgreSqlDialect {

    public static final JdbcDialectFactory FACTORY =
        new JdbcDialectFactory(
            NetezzaDialect.class,
            // While we're choosing dialects, this still looks like a Postgres
            // connection.
            DatabaseProduct.POSTGRESQL)
        {
            protected boolean acceptsConnection(Connection connection) {
                try {
                    return super.acceptsConnection(connection)
                       && isNetezza(connection.getMetaData());
                } catch (SQLException e) {
                    throw Util.newError(
                        e, "Error while instantiating dialect");
                }
            }
        };

    /**
     * Creates a NetezzaDialect.
     *
     * @param connection Connection
     */
    public NetezzaDialect(Connection connection) throws SQLException {
        super(connection);
    }

    @Override
    public DatabaseProduct getDatabaseProduct() {
        return DatabaseProduct.NETEZZA;
    }
}
// End NetezzaDialect.java