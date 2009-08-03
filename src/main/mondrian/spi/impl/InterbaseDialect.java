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
 * Implementation of {@link mondrian.spi.Dialect} for the Interbase database.
 *
 * @author jhyde
 * @version $Id$
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
