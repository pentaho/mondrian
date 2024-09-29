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
 * Implementation of {@link mondrian.spi.Dialect} for old versions of the IBM
 * DB2/AS400 database. Modern versions of DB2/AS400 use
 * {@link mondrian.spi.impl.Db2Dialect}.
 *
 * @see mondrian.spi.impl.Db2Dialect
 *
 * @author jhyde
 * @since Nov 23, 2008
 */
public class Db2OldAs400Dialect extends Db2Dialect {

    public static final JdbcDialectFactory FACTORY =
        new JdbcDialectFactory(
            Db2OldAs400Dialect.class,
            DatabaseProduct.DB2_OLD_AS400);

    /**
     * Creates a Db2OldAs400Dialect.
     *
     * @param connection Connection
     */
    public Db2OldAs400Dialect(Connection connection) throws SQLException {
        super(connection);
    }

    public boolean allowsFromQuery() {
        // Older versions of AS400 do not allow FROM
        // subqueries in the FROM clause.
        return false;
    }
}

// End Db2OldAs400Dialect.java
