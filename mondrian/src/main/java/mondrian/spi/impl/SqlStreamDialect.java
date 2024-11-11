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

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Implementation of {@link mondrian.spi.Dialect} for the SQLstream streaming
 * SQL system.
 *
 * @author jhyde
 * @since Mar 23, 2009
 */
public class SqlStreamDialect extends LucidDbDialect {

    public static final JdbcDialectFactory FACTORY =
        new JdbcDialectFactory(
            SqlStreamDialect.class,
            DatabaseProduct.SQLSTREAM);

    /**
     * Creates a SqlStreamDialect.
     *
     * @param connection Connection
     */
    public SqlStreamDialect(Connection connection) throws SQLException {
        super(connection);
    }
}

// End SqlStreamDialect.java
