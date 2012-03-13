/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2008-2009 Pentaho
// All Rights Reserved.
*/
package mondrian.spi.impl;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Implementation of {@link mondrian.spi.Dialect} for the Interbase database.
 *
 * @author jhyde
 * @since Nov 23, 2008
 */
public class InterbaseDialect extends JdbcDialectImpl {

    public static final JdbcDialectFactory FACTORY =
        new JdbcDialectFactory(
            InterbaseDialect.class,
            DatabaseProduct.INTERBASE);

    /**
     * Creates an InterbaseDialect.
     *
     * @param connection Connection
     */
    public InterbaseDialect(Connection connection) throws SQLException {
        super(connection);
    }

    public boolean allowsAs() {
        return false;
    }

    public boolean allowsFromQuery() {
        return false;
    }
}

// End InterbaseDialect.java
