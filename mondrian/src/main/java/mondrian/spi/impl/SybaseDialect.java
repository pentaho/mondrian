/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2002-2017 Hitachi Vantara.
// All rights reserved.
*/
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
