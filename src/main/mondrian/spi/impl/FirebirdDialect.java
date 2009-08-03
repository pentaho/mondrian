/*
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2008-2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.spi.impl;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Implementation of {@link mondrian.spi.Dialect} for the Firebird database.
 *
 * @author jhyde
 * @version $Id$
 * @since Nov 23, 2008
 */
public class FirebirdDialect extends JdbcDialectImpl {

    public static final JdbcDialectFactory FACTORY =
        new JdbcDialectFactory(
            FirebirdDialect.class,
            DatabaseProduct.FIREBIRD);

    /**
     * Creates a FirebirdDialect.
     *
     * @param connection Connection
     */
    public FirebirdDialect(Connection connection) throws SQLException {
        super(connection);
    }

    public boolean allowsAs() {
        return false;
    }
}

// End FirebirdDialect.java
