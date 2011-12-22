/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2008-2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.spi.impl;

import java.util.List;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Implementation of {@link mondrian.spi.Dialect} for the Ingres database.
 *
 * @author jhyde
 * @version $Id$
 * @since Nov 23, 2008
 */
public class IngresDialect extends JdbcDialectImpl {

    public static final JdbcDialectFactory FACTORY =
        new JdbcDialectFactory(
            IngresDialect.class,
            DatabaseProduct.INGRES);

    /**
     * Creates an IngresDialect.
     *
     * @param connection Connection
     */
    public IngresDialect(Connection connection) throws SQLException {
        super(connection);
    }

    public String generateInline(
        List<String> columnNames,
        List<String> columnTypes,
        List<String[]> valueList)
    {
        return generateInlineGeneric(
            columnNames, columnTypes, valueList, null, false);
    }

    public boolean requiresOrderByAlias() {
        return true;
    }
}

// End IngresDialect.java
