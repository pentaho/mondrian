/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/

package mondrian.spi.impl;

import mondrian.olap.Util;

import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;

/**
 * Implementation of {@link mondrian.spi.Dialect} for the Sybase database.
 *
 * @author jhyde
 * @since Nov 23, 2008
 */
public class SybaseDialect extends JdbcDialectImpl {

    public static final JdbcDialectFactory FACTORY =
        new JdbcDialectFactory(
            SybaseDialect.class,
            DatabaseProduct.SYBASE);

    /**
     * Creates a SybaseDialect.
     *
     * @param connection Connection
     */
    public SybaseDialect(Connection connection) throws SQLException {
        super(connection);
    }

    public boolean allowsAs() {
        return false;
    }

    public boolean allowsFromQuery() {
        return false;
    }

    public boolean requiresAliasForFromQuery() {
        return true;
    }

    @Override
    protected void quoteDateLiteral(StringBuilder buf, String value, Date date)
    {
        Util.singleQuoteString(value, buf);
    }
}

// End SybaseDialect.java
