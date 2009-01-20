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
 * Implementation of {@link mondrian.spi.Dialect} for the LucidDB database.
 *
 * @author jhyde
 * @version $Id$
 * @since Nov 23, 2008
 */
public class LucidDbDialect extends JdbcDialectImpl {

    public static final JdbcDialectFactory FACTORY =
        new JdbcDialectFactory(
            LucidDbDialect.class,
            DatabaseProduct.LUCIDDB);

    /**
     * Creates a LucidDbDialect.
     *
     * @param connection Connection
     */
    public LucidDbDialect(Connection connection) throws SQLException {
        super(connection);
    }

    public boolean allowsMultipleDistinctSqlMeasures() {
        return false;
    }

    public boolean needsExponent(Object value, String valueString) {
        return value instanceof Double && !valueString.contains("E");
    }

    public boolean supportsUnlimitedValueList() {
        return true;
    }

    public boolean supportsMultiValueInExpr() {
        return true;
    }
}

// End LucidDbDialect.java
