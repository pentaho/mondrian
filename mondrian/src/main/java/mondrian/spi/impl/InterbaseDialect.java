/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


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
