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
 * Implementation of {@link mondrian.spi.Dialect} for the Firebird database.
 *
 * @author jhyde
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

    @Override
    public String generateOrderByNulls(
        String expr,
        boolean ascending,
        boolean collateNullsLast)
    {
        return generateOrderByNullsAnsi(expr, ascending, collateNullsLast);
    }
}

// End FirebirdDialect.java
