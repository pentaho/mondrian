/*
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2008-2009 Jaspersoft
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/

package mondrian.spi.impl;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Implementation of {@link mondrian.spi.Dialect} for the Netezza database.
 *
 * @author swood
 * @version $Id$
 * @since April 17, 2009
 */
public class NetezzaDialect extends PostgreSqlDialect {

    public static final JdbcDialectFactory FACTORY =
        new JdbcDialectFactory(
            NetezzaDialect.class,
            DatabaseProduct.NETEZZA);

    /**
     * Creates a NetezzaDialect.
     *
     * @param connection Connection
     */
    public NetezzaDialect(Connection connection) throws SQLException {
        super(connection);
    }

}
// End NetezzaDialect.java