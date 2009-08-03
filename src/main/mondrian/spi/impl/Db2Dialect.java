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
 * Implementation of {@link mondrian.spi.Dialect} for the IBM DB2 database.
 *
 * @see mondrian.spi.impl.Db2OldAs400Dialect
 *
 * @author jhyde
 * @version $Id$
 * @since Nov 23, 2008
 */
public class Db2Dialect extends JdbcDialectImpl {

    public static final JdbcDialectFactory FACTORY =
        new JdbcDialectFactory(
            Db2Dialect.class,
            DatabaseProduct.DB2);

    /**
     * Creates a Db2Dialect.
     *
     * @param connection Connection
     */
    public Db2Dialect(Connection connection) throws SQLException {
        super(connection);
    }

    public String toUpper(String expr) {
        return "UCASE(" + expr + ")";
    }

    public boolean supportsGroupingSets() {
        return true;
    }

    public boolean requiresOrderByAlias() {
        return true;
    }
}

// End Db2Dialect.java

